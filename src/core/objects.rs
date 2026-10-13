//! Content-addressed objects: verified reads and installs, immutable sizes,
//! local availability, the shared (sendable) inventory, per-object error state
//! and per-object locks. Also owns the object side tables (§9), their
//! backfills and the startup object check (phase 4 of `Store::open`).
//!
//! # Indexes (WP1.8)
//!
//! * `object_availability(hash, size, state, mtime_ns, verified_ms)`: whether
//!   the bytes of `hash` are installed in `objects/` (`AVAILABLE`), absent
//!   (`MISSING`) or known corrupt (`CORRUPT`). It is written after every
//!   install (`put_object`, `put_stream`, `register_received`), on quarantine
//!   renames and verification failures, and heals on read paths
//!   (`open_object`, `object_available_checked`). It is derived: a lost row
//!   only costs a `stat` (rowless objects fall back to the legacy check) or a
//!   re-hash at the next startup.
//! * `event_objects(hash, event, shared)` and `staged_objects(hash, event)`:
//!   which committed / staged events reference which objects. Both are kept by
//!   SQLite triggers, so writes by older binaries are covered too.
//! * `object_error_state(hash, code, class, attempts, retry_at_ms, last_ms)`:
//!   per-object failure backoff. The positional `object_errors` table and its
//!   writers are unchanged.
//!
//! # Startup markers
//!
//! A row is *trusted* when its size and mtime still match the file, it was
//! verified, and it is not racy (verified at least `RACY_NS` after the file's
//! mtime, so a same-size rewrite within one timestamp tick cannot hide). Every
//! object this store installs gets its mtime stamped to `STAMP_SECS` before the
//! install rename, which makes it trusted at once; any later write changes the
//! mtime. `VerifyMode::Changed` hashes only untrusted objects, `Full` hashes
//! everything, `Lazy` hashes nothing (rows stay unverified for `scrub_step`).
use super::*;
use crate::model::errors::{ErrorClass, ErrorWire};
use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, HashSet},
    fs::OpenOptions,
    io::{Seek, Write},
    sync::{Arc, Mutex, Weak},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// `object_availability.state`: no installed bytes.
pub const MISSING: i64 = 0;
/// `object_availability.state`: installed, size-consistent bytes.
pub const AVAILABLE: i64 = 2;
/// `object_availability.state`: the bytes failed verification (quarantined).
pub const CORRUPT: i64 = 3;
/// The mtime (2001-01-01T00:00:00Z) stamped on every object this store installs.
pub const STAMP_SECS: u64 = 978_307_200;
const STAMP_NS: i64 = STAMP_SECS as i64 * 1_000_000_000;
/// A marker is racy when it was recorded within this long of the file's mtime:
/// a same-size rewrite in the same timestamp tick would be invisible to `stat`.
/// Larger than any common mtime granularity (FAT: 2 s).
const RACY_NS: i64 = 3_000_000_000;
/// Rows per backfill / startup-check transaction.
const CHUNK: usize = 10_000;
/// `config('schema:objects')`.
const SCHEMA_VERSION: &str = "1";
/// One-time markers (§5.3).
const AVAILABILITY_MARKER: &str = "availability_v2";
const EVENT_OBJECTS_MARKER: &str = "event_objects_v1";

/// Object-module cache (interior mutability; derived and disposable).
#[derive(Default)]
pub(crate) struct Cache {
    /// Whole-object SHA-256 verifications performed through this handle.
    hashed: Cell<u64>,
    /// The last content scan (owned by `core::status`).
    pub(crate) content: RefCell<Option<super::status::ContentCache>>,
}

/// One enumeration of `objects/`: what is physically installed. Reused while
/// the directory's mtime is unchanged and the listing is not racy (taken at
/// least one timestamp tick after the directory's last change), since every
/// create, delete or rename of an entry changes the directory's mtime. Shared
/// by every handle of a store in this process (bulk lanes open short-lived
/// handles), so an unchanged store is listed once.
pub(crate) struct Presence {
    /// Distinct per listing, process-wide.
    pub(crate) generation: u64,
    dir_mtime_ns: i64,
    trusted: bool,
    pub(crate) files: HashMap<String, (u64, i64)>,
}
impl Presence {
    /// The legacy availability rule: a regular file whose length matches the
    /// known size (any length when the size is unknown).
    pub(crate) fn has(&self, object: &str, size: Option<i64>) -> bool {
        self.files
            .get(object)
            .is_some_and(|&(len, _)| size.is_none_or(|size| size == len as i64))
    }
}
/// Directory mtimes: sub-second resolution means a fine-grained filesystem
/// (NTFS, ext4: ticks of 16 ms or less); whole seconds a coarse one (FAT).
fn directory_racy_ns(dir_mtime_ns: i64) -> i64 {
    if dir_mtime_ns % 1_000_000_000 == 0 {
        RACY_NS
    } else {
        100_000_000
    }
}
/// The latest listing per store root (at most `MAX_LISTINGS` roots).
static LISTINGS: Mutex<BTreeMap<PathBuf, Arc<Presence>>> = Mutex::new(BTreeMap::new());
static LISTING_GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
const MAX_LISTINGS: usize = 64;
fn listings() -> std::sync::MutexGuard<'static, BTreeMap<PathBuf, Arc<Presence>>> {
    LISTINGS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
}

/// Startup verification of referenced, locally present objects.
/// `Full` re-hashes every object; `Changed` (the default) trusts objects whose
/// recorded markers still match and hashes the others once; `Lazy` skips
/// hashing (untrusted objects stay unverified for the background scrubber).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum VerifyMode {
    Full,
    #[default]
    Changed,
    Lazy,
}

/// Result of one `Store::scrub_step`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScrubOutcome {
    /// Objects whose bytes matched their hash.
    pub verified: u64,
    /// Objects found corrupt and quarantined.
    pub corrupt: u64,
    /// Bytes hashed.
    pub bytes: u64,
}

const SCHEMA: &str = "SAVEPOINT objects_schema;
    CREATE TABLE IF NOT EXISTS object_availability(hash TEXT PRIMARY KEY,size INTEGER NOT NULL,state INTEGER NOT NULL,mtime_ns INTEGER NOT NULL,verified_ms INTEGER NOT NULL);
    CREATE INDEX IF NOT EXISTS object_availability_by_state ON object_availability(state,verified_ms);
    CREATE TABLE IF NOT EXISTS event_objects(hash TEXT NOT NULL,event TEXT NOT NULL,shared INTEGER NOT NULL,PRIMARY KEY(hash,event));
    CREATE INDEX IF NOT EXISTS event_objects_by_event ON event_objects(event);
    CREATE TABLE IF NOT EXISTS staged_objects(hash TEXT NOT NULL,event TEXT NOT NULL,PRIMARY KEY(hash,event));
    CREATE INDEX IF NOT EXISTS staged_objects_by_event ON staged_objects(event);
    CREATE TABLE IF NOT EXISTS object_error_state(hash TEXT PRIMARY KEY,code TEXT NOT NULL,class TEXT NOT NULL,attempts INTEGER NOT NULL,retry_at_ms INTEGER NOT NULL,last_ms INTEGER NOT NULL);
    CREATE TRIGGER IF NOT EXISTS event_objects_insert AFTER INSERT ON events BEGIN
        INSERT OR IGNORE INTO event_objects(hash,event,shared)
            SELECT json_extract(c.value,'$.value'),NEW.id,coalesce(json_extract(NEW.payload,'$.branch.shared'),0)
            FROM json_each(CASE WHEN json_valid(NEW.payload) THEN NEW.payload ELSE '{}' END,'$.changes') c
            WHERE json_extract(c.value,'$.field')='content' AND json_type(c.value,'$.value')='text';
    END;
    CREATE TRIGGER IF NOT EXISTS event_objects_delete AFTER DELETE ON events BEGIN
        DELETE FROM event_objects WHERE event=OLD.id;
    END;
    CREATE TRIGGER IF NOT EXISTS event_objects_update AFTER UPDATE OF id,payload ON events BEGIN
        DELETE FROM event_objects WHERE event=OLD.id;
        INSERT OR IGNORE INTO event_objects(hash,event,shared)
            SELECT json_extract(c.value,'$.value'),NEW.id,coalesce(json_extract(NEW.payload,'$.branch.shared'),0)
            FROM json_each(CASE WHEN json_valid(NEW.payload) THEN NEW.payload ELSE '{}' END,'$.changes') c
            WHERE json_extract(c.value,'$.field')='content' AND json_type(c.value,'$.value')='text';
    END;
    CREATE TRIGGER IF NOT EXISTS staged_objects_insert AFTER INSERT ON incoming_events BEGIN
        INSERT OR IGNORE INTO staged_objects(hash,event)
            SELECT json_extract(c.value,'$.value'),NEW.id
            FROM json_each(CASE WHEN json_valid(NEW.payload) THEN NEW.payload ELSE '{}' END,'$.changes') c
            WHERE json_extract(c.value,'$.field')='content' AND json_type(c.value,'$.value')='text';
    END;
    CREATE TRIGGER IF NOT EXISTS staged_objects_delete AFTER DELETE ON incoming_events BEGIN
        DELETE FROM staged_objects WHERE event=OLD.id;
    END;
    CREATE TRIGGER IF NOT EXISTS staged_objects_update AFTER UPDATE OF id,payload ON incoming_events BEGIN
        DELETE FROM staged_objects WHERE event=OLD.id;
        INSERT OR IGNORE INTO staged_objects(hash,event)
            SELECT json_extract(c.value,'$.value'),NEW.id
            FROM json_each(CASE WHEN json_valid(NEW.payload) THEN NEW.payload ELSE '{}' END,'$.changes') c
            WHERE json_extract(c.value,'$.field')='content' AND json_type(c.value,'$.value')='text';
    END;
    RELEASE objects_schema;";

/// Backfill statements: `(source table, cursor key, statement over rowid range ?1 < rowid <= ?2)`.
const BACKFILLS: [(&str, &str, &str); 2] = [
    (
        "events",
        "event_objects_v1:events",
        "INSERT OR IGNORE INTO event_objects(hash,event,shared)
            SELECT json_extract(c.value,'$.value'),e.id,coalesce(json_extract(e.payload,'$.branch.shared'),0)
            FROM events e,json_each(CASE WHEN json_valid(e.payload) THEN e.payload ELSE '{}' END,'$.changes') c
            WHERE e.rowid>?1 AND e.rowid<=?2
              AND json_extract(c.value,'$.field')='content' AND json_type(c.value,'$.value')='text'",
    ),
    (
        "incoming_events",
        "event_objects_v1:incoming",
        "INSERT OR IGNORE INTO staged_objects(hash,event)
            SELECT json_extract(c.value,'$.value'),e.id
            FROM incoming_events e,json_each(CASE WHEN json_valid(e.payload) THEN e.payload ELSE '{}' END,'$.changes') c
            WHERE e.rowid>?1 AND e.rowid<=?2
              AND json_extract(c.value,'$.field')='content' AND json_type(c.value,'$.value')='text'",
    ),
];

fn config_value(db: &Connection, key: &str) -> Result<Option<String>> {
    Ok(db
        .query_row("SELECT value FROM config WHERE key=?", [key], |r| r.get(0))
        .optional()?)
}
fn set_config(db: &Connection, key: &str, value: &str) -> Result<()> {
    db.execute(
        "INSERT INTO config(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        [key, value],
    )?;
    Ok(())
}

/// Phase 3 (objects): the §9 side tables and triggers, in their own savepoint,
/// then the one-time chunked, resumable `event_objects_v1` backfill.
pub(super) fn ensure_schema(store: &mut Store) -> Result<()> {
    store.db.execute_batch(SCHEMA)?;
    if config_value(&store.db, "schema:objects")?.as_deref() != Some(SCHEMA_VERSION) {
        set_config(&store.db, "schema:objects", SCHEMA_VERSION)?;
    }
    if config_value(&store.db, EVENT_OBJECTS_MARKER)?.is_some() {
        return Ok(());
    }
    // Triggers already cover every row written from now on; the backfill
    // covers older rows, `CHUNK` rows per transaction, resuming from a cursor.
    for (table, cursor_key, statement) in BACKFILLS {
        let mut cursor: i64 = config_value(&store.db, cursor_key)?
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        loop {
            let upper: Option<i64> = store.db.query_row(
                &format!(
                    "SELECT max(rowid) FROM (SELECT rowid FROM {table} WHERE rowid>? ORDER BY rowid LIMIT {CHUNK})"
                ),
                [cursor],
                |r| r.get(0),
            )?;
            let Some(upper) = upper else { break };
            let tx = store.db.savepoint()?;
            tx.execute(statement, params![cursor, upper])?;
            set_config(&tx, cursor_key, &upper.to_string())?;
            tx.commit()?;
            cursor = upper;
            crate::startup::progress("indexing object references");
        }
    }
    let tx = store.db.savepoint()?;
    set_config(&tx, EVENT_OBJECTS_MARKER, "1")?;
    for (_, cursor_key, _) in BACKFILLS {
        tx.execute("DELETE FROM config WHERE key=?", [cursor_key])?;
    }
    tx.commit()?;
    Ok(())
}

/// One `object_availability` row.
#[derive(Debug, Clone, Copy)]
struct Row {
    size: i64,
    state: i64,
    mtime_ns: i64,
    verified_ms: i64,
}
impl Row {
    /// Verified, not racy, and the file still has the recorded size and mtime.
    fn trusted(&self, len: u64, mtime_ns: i64) -> bool {
        self.state == AVAILABLE
            && self.size == len as i64
            && self.mtime_ns == mtime_ns
            && self.verified_ms > 0
            && i128::from(self.verified_ms) * 1_000_000 - i128::from(self.mtime_ns)
                >= i128::from(RACY_NS)
    }
}

pub(crate) fn mtime_ns(metadata: &fs::Metadata) -> i64 {
    metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
}

/// Stamp `STAMP_SECS` on a file opened for writing; returns the mtime the file
/// now has (the stamp, or its current mtime when stamping is unsupported).
pub(crate) fn stamp(file: &fs::File) -> Result<i64> {
    let stamp = UNIX_EPOCH + Duration::from_secs(STAMP_SECS);
    if file.set_modified(stamp).is_ok() {
        return Ok(STAMP_NS);
    }
    Ok(mtime_ns(&file.metadata()?))
}

/// Regular files in `objects/` named like an object id: `hash → (len, mtime_ns)`.
/// One directory enumeration instead of one `stat` per object.
fn scan_objects(root: &Path) -> Result<HashMap<String, (u64, i64)>> {
    let mut present = HashMap::new();
    for entry in fs::read_dir(root.join("objects"))? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if valid_object(name).is_err() || !entry.file_type()?.is_file() {
            continue;
        }
        let metadata = entry.metadata()?;
        present.insert(name.to_owned(), (metadata.len(), mtime_ns(&metadata)));
    }
    Ok(present)
}

/// Phase 4 of `Store::open`: reference/size bookkeeping and verification of
/// stored bytes; corrupt content is quarantined, the journal is kept.
///
/// After the one-time pass (`availability_v2`), a clean restart enumerates
/// `objects/` once and hashes nothing: every candidate (referenced, staged or
/// indexed object) is compared with its marker. Deleted objects are marked
/// missing and queued for download; changed ones are hashed, and quarantined
/// if corrupt. Each `CHUNK` candidates commit in their own savepoint.
pub(super) fn startup_object_check(store: &mut Store, verify: VerifyMode) -> Result<()> {
    let upgraded = config_value(&store.db, AVAILABILITY_MARKER)?.is_some();
    // Very old states (no `availability_v1`) also publish verified shared objects.
    let legacy = config_value(&store.db, "availability_v1")?.is_none();
    if !upgraded {
        one_time_bookkeeping(store)?;
    }
    // Lazy cannot grant the legacy publication, which requires verification.
    let verify = if legacy && verify == VerifyMode::Lazy {
        VerifyMode::Changed
    } else {
        verify
    };
    let rows: HashMap<String, Row> = store
        .db
        .prepare("SELECT hash,size,state,mtime_ns,verified_ms FROM object_availability")?
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                Row {
                    size: r.get(1)?,
                    state: r.get(2)?,
                    mtime_ns: r.get(3)?,
                    verified_ms: r.get(4)?,
                },
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;
    let mut candidates: BTreeSet<String> = store
        .db
        .prepare("SELECT hash FROM event_objects UNION SELECT hash FROM staged_objects")?
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    candidates.extend(rows.keys().cloned());
    let shared: HashSet<String> = if legacy {
        store
            .db
            .prepare("SELECT DISTINCT hash FROM event_objects WHERE shared=1")?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?
    } else {
        HashSet::new()
    };
    crate::startup::progress("verifying available objects");
    let present = scan_objects(&store.root)?;
    let candidates: Vec<String> = candidates
        .into_iter()
        .filter(|h| valid_object(h).is_ok())
        .collect();
    for chunk in candidates.chunks(CHUNK) {
        store.db.execute_batch("SAVEPOINT object_check")?;
        let result = (|| -> Result<()> {
            for object in chunk {
                crate::startup::progress("verifying available objects");
                check_object(store, object, rows.get(object), present.get(object), verify)
                    .and_then(|ok| {
                        if ok && legacy && shared.contains(object) {
                            store.db.execute(
                                "INSERT OR IGNORE INTO published_objects VALUES(?)",
                                [object],
                            )?;
                        }
                        Ok(())
                    })?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => store.db.execute_batch("RELEASE object_check")?,
            Err(error) => {
                let _ = store
                    .db
                    .execute_batch("ROLLBACK TO object_check; RELEASE object_check");
                return Err(error);
            }
        }
    }
    let tx = store.db.savepoint()?;
    set_config(&tx, AVAILABILITY_MARKER, "1")?;
    tx.execute(
        "INSERT OR IGNORE INTO config VALUES('availability_v1','1')",
        [],
    )?;
    tx.commit()?;
    Ok(())
}

/// The legacy per-open bookkeeping (`object_references`, `object_sizes` from
/// every event), now run once per upgrade. Commits maintain both afterwards
/// (in this and older binaries alike).
fn one_time_bookkeeping(store: &mut Store) -> Result<()> {
    crate::startup::progress("indexing object references");
    let tx = store.db.savepoint()?;
    tx.execute(
        "INSERT OR IGNORE INTO object_references SELECT DISTINCT hash FROM event_objects",
        [],
    )?;
    tx.commit()?;
    let sizes: Vec<(String, i64)> = store
        .db
        .prepare(
            "SELECT j.key,j.value FROM events e,json_each(CASE WHEN json_valid(e.payload) THEN e.payload ELSE '{}' END,'$.object_sizes') j",
        )?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    for chunk in sizes.chunks(CHUNK) {
        let tx = store.db.savepoint()?;
        for (object, size) in chunk {
            let size = u64::try_from(*size).context("FILE_TOO_LARGE")?;
            remember_size(&tx, object, size)?;
        }
        tx.commit()?;
        crate::startup::progress("indexing object references");
    }
    Ok(())
}

/// `Store::remember_object_size` on any connection (transactions included).
fn remember_size(db: &Connection, object: &str, size: u64) -> Result<()> {
    valid_object(object)?;
    ensure!(size <= MAX_CONTENT, "FILE_TOO_LARGE");
    let old: Option<u64> = db
        .query_row(
            "SELECT size FROM object_sizes WHERE hash=?",
            [object],
            |r| r.get(0),
        )
        .optional()?;
    ensure!(old.is_none_or(|old| old == size), "OBJECT_SIZE_CHANGED");
    db.execute(
        "INSERT OR IGNORE INTO object_sizes VALUES(?,?)",
        params![object, size],
    )?;
    Ok(())
}

/// One startup candidate. Returns whether the object is present and good.
fn check_object(
    store: &Store,
    object: &str,
    row: Option<&Row>,
    present: Option<&(u64, i64)>,
    verify: VerifyMode,
) -> Result<bool> {
    let Some(&(len, mtime)) = present else {
        match row {
            Some(row) if row.state == AVAILABLE => {
                // Deleted behind our back: missing, and wanted again.
                store.set_availability(object, row.size, MISSING, 0, 0)?;
                store.queue_request(object)?;
            }
            Some(_) => {}
            None => {
                let size = known_size(&store.db, object)?.map_or(-1, |s| s as i64);
                store.set_availability(object, size, MISSING, 0, 0)?;
            }
        }
        return Ok(false);
    };
    if verify != VerifyMode::Full && row.is_some_and(|row| row.trusted(len, mtime)) {
        return Ok(true);
    }
    if verify == VerifyMode::Lazy {
        let expected = known_size(&store.db, object)?;
        if expected.is_some_and(|size| size != len) {
            store.quarantine(object, &anyhow::anyhow!("CORRUPT_OBJECT: {object}"))?;
            return Ok(false);
        }
        // Present and size-consistent, but unverified: the scrubber hashes it.
        store.set_availability(object, len as i64, AVAILABLE, mtime, 0)?;
        return Ok(true);
    }
    match store.verify_object(object) {
        Ok(()) => {
            store.remember_object_size(object, len)?;
            // `mtime` was read before hashing: a write during or after the hash
            // changes it, and a racy marker is never trusted.
            store.set_availability(object, len as i64, AVAILABLE, mtime, time() as i64)?;
            store
                .db
                .execute("DELETE FROM object_error_state WHERE hash=?", [object])?;
            Ok(true)
        }
        Err(error) => {
            store.quarantine(object, &error)?;
            Ok(false)
        }
    }
}

/// The recorded size descriptor of `object`, without touching the filesystem.
fn known_size(db: &Connection, object: &str) -> Result<Option<u64>> {
    Ok(db
        .query_row(
            "SELECT size FROM object_sizes WHERE hash=?",
            [object],
            |r| r.get(0),
        )
        .optional()?)
}

impl Store {
    /// Whole-object SHA-256 verifications performed through this handle (a
    /// clean `Changed` startup performs none).
    pub fn object_hashes(&self) -> u64 {
        self.caches.objects.hashed.get()
    }
    /// What `objects/` holds right now: one directory `stat`, plus one
    /// enumeration when the directory changed since the cached listing.
    pub(crate) fn presence(&self) -> Result<Arc<Presence>> {
        let dir = self.root.join("objects");
        let dir_mtime_ns = mtime_ns(&fs::metadata(&dir)?);
        if let Some(presence) = listings().get(&self.root)
            && presence.trusted
            && presence.dir_mtime_ns == dir_mtime_ns
        {
            return Ok(presence.clone());
        }
        let taken = now_ns();
        let files = scan_objects(&self.root)?;
        let after = mtime_ns(&fs::metadata(&dir)?);
        let presence = Arc::new(Presence {
            generation: LISTING_GENERATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1,
            dir_mtime_ns,
            trusted: after == dir_mtime_ns
                && taken.saturating_sub(dir_mtime_ns) >= directory_racy_ns(dir_mtime_ns),
            files,
        });
        let mut cached = listings();
        if cached.len() >= MAX_LISTINGS && !cached.contains_key(&self.root) {
            cached.pop_first();
        }
        cached.insert(self.root.clone(), presence.clone());
        Ok(presence)
    }
    pub(crate) fn count_hash(&self) {
        let cache = &self.caches.objects.hashed;
        cache.set(cache.get() + 1);
    }
    fn availability_row(&self, object: &str) -> Result<Option<Row>> {
        Ok(self
            .db
            .prepare_cached(
                "SELECT size,state,mtime_ns,verified_ms FROM object_availability WHERE hash=?",
            )?
            .query_row([object], |r| {
                Ok(Row {
                    size: r.get(0)?,
                    state: r.get(1)?,
                    mtime_ns: r.get(2)?,
                    verified_ms: r.get(3)?,
                })
            })
            .optional()?)
    }
    /// Upsert one availability row (joins the caller's transaction, if any).
    pub(crate) fn set_availability(
        &self,
        object: &str,
        size: i64,
        state: i64,
        mtime_ns: i64,
        verified_ms: i64,
    ) -> Result<()> {
        self.db
            .prepare_cached(
                "INSERT INTO object_availability(hash,size,state,mtime_ns,verified_ms) VALUES(?,?,?,?,?)
                 ON CONFLICT(hash) DO UPDATE SET size=excluded.size,state=excluded.state,
                 mtime_ns=excluded.mtime_ns,verified_ms=excluded.verified_ms",
            )?
            .execute(params![object, size, state, mtime_ns, verified_ms])?;
        Ok(())
    }
    /// An install (or verified dedupe) of `object`: available, clear its backoff.
    /// Best effort: the index is derived, and a lost row heals.
    pub(crate) fn note_available(&self, object: &str, size: u64, mtime_ns: i64, verified_ms: u64) {
        let _ = self.in_savepoint("object_available", || {
            let fresh = self.availability_row(object)?.is_some_and(|row| {
                row.state == AVAILABLE
                    && row.size == size as i64
                    && row.mtime_ns == mtime_ns
                    && row.verified_ms > 0
            });
            if !fresh {
                self.set_availability(
                    object,
                    size as i64,
                    AVAILABLE,
                    mtime_ns,
                    verified_ms as i64,
                )?;
            }
            self.db
                .prepare_cached("DELETE FROM object_error_state WHERE hash=?")?
                .execute([object])?;
            Ok(())
        });
    }
    /// A read found no file: an `AVAILABLE` row becomes `MISSING` (best effort).
    fn note_missing(&self, object: &str) {
        let _ = self.db.execute(
            "UPDATE object_availability SET state=?1,verified_ms=0 WHERE hash=?2 AND state=?3",
            params![MISSING, object, AVAILABLE],
        );
    }
    /// A verification failed: the row becomes `CORRUPT` (best effort).
    fn note_corrupt(&self, object: &str, size: u64) {
        let _ = self.db.execute(
            "INSERT INTO object_availability(hash,size,state,mtime_ns,verified_ms) VALUES(?1,?2,?3,0,0)
             ON CONFLICT(hash) DO UPDATE SET state=?3,verified_ms=0 WHERE object_availability.state<>?3",
            params![object, size as i64, CORRUPT],
        );
    }
    /// Run `f` inside a savepoint on this (shared) connection.
    pub(crate) fn in_savepoint<T>(&self, name: &str, f: impl FnOnce() -> Result<T>) -> Result<T> {
        self.db.execute_batch(&format!("SAVEPOINT {name}"))?;
        match f() {
            Ok(value) => {
                if let Err(error) = self.db.execute_batch(&format!("RELEASE {name}")) {
                    let _ = self
                        .db
                        .execute_batch(&format!("ROLLBACK TO {name}; RELEASE {name}"));
                    return Err(error.into());
                }
                Ok(value)
            }
            Err(error) => {
                let _ = self
                    .db
                    .execute_batch(&format!("ROLLBACK TO {name}; RELEASE {name}"));
                Err(error)
            }
        }
    }
    /// Rename corrupt bytes aside (`<hash>.corrupt-<id>`), record the legacy
    /// `object_errors` row and the backoff, mark the object `CORRUPT` and queue
    /// it for download again. The journal is kept.
    pub(crate) fn quarantine(&self, object: &str, error: &anyhow::Error) -> Result<()> {
        let path = self.root.join("objects").join(object);
        let size = fs::metadata(&path).map_or(-1, |m| m.len() as i64);
        if path.is_file() {
            fs::rename(&path, path.with_extension(format!("corrupt-{}", id())))?;
        }
        self.db.execute("INSERT INTO object_errors VALUES(?,?,?) ON CONFLICT(hash) DO UPDATE SET error=excluded.error,at=excluded.at", params![object, format!("{error:#}"), time()])?;
        self.set_availability(object, size, CORRUPT, 0, time() as i64)?;
        self.record_object_failure(object, &ErrorWire::coded("CORRUPT_OBJECT", object))?;
        self.queue_request(object)?;
        Ok(())
    }
    /// Queue a download of a committed-referenced object (no-op otherwise).
    pub(crate) fn queue_request(&self, object: &str) -> Result<()> {
        if self
            .db
            .prepare_cached("SELECT EXISTS(SELECT 1 FROM event_objects WHERE hash=?)")?
            .query_row([object], |r| r.get::<_, bool>(0))?
        {
            self.db.prepare_cached("INSERT INTO object_requests VALUES(?,?) ON CONFLICT(hash) DO UPDATE SET requested_ms=excluded.requested_ms")?
                .execute(params![object, time()])?;
        }
        Ok(())
    }

    pub fn read_object(&self, object: &str) -> Result<Vec<u8>> {
        ensure!(
            self.object_size(object)? <= MAX_FILE as u64,
            "OBJECT_REQUIRES_STREAMING"
        );
        let mut file = self.open_object(object)?;
        ensure!(
            file.metadata()?.len() <= MAX_FILE as u64,
            "OBJECT_REQUIRES_STREAMING"
        );
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(bytes)
    }
    pub fn object_size(&self, object: &str) -> Result<u64> {
        self.object_size_if_known(object)?
            .with_context(|| format!("CONTENT_SIZE_UNKNOWN: {object}"))
    }
    /// Legacy journals may reference unavailable bytes without a size descriptor.
    /// Preserve that distinction rather than blocking metadata or inventing a size.
    pub fn object_size_if_known(&self, object: &str) -> Result<Option<u64>> {
        valid_object(object)?;
        if let Some(size) = known_size(&self.db, object)? {
            return Ok(Some(size));
        }
        match fs::metadata(self.root.join("objects").join(object)) {
            Ok(metadata) => Ok(Some(metadata.len())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(object_error(error)),
        }
    }
    pub fn remember_object_size(&self, object: &str, size: u64) -> Result<()> {
        remember_size(&self.db, object, size)
    }
    /// Point lookup in the availability index (no filesystem access). Objects
    /// the index has never seen fall back to the legacy `stat` check.
    pub fn object_available(&self, object: &str) -> bool {
        if valid_object(object).is_err() {
            return false;
        }
        match self.availability_row(object) {
            Ok(Some(row)) => row.state == AVAILABLE,
            Ok(None) | Err(_) => self.object_present(object),
        }
    }
    /// The legacy check: a regular file whose length matches the known size.
    fn object_present(&self, object: &str) -> bool {
        valid_object(object).is_ok()
            && fs::metadata(self.root.join("objects").join(object)).is_ok_and(|m| {
                m.is_file() && self.object_size(object).is_ok_and(|size| size == m.len())
            })
    }
    /// `stat` the object and heal its index row to what the filesystem shows:
    /// a size-consistent file is available (unverified unless its marker still
    /// matches), a missing file is missing, a size mismatch is corrupt.
    pub fn object_available_checked(&self, object: &str) -> bool {
        if valid_object(object).is_err() {
            return false;
        }
        let row = self.availability_row(object).ok().flatten();
        let path = self.root.join("objects").join(object);
        let metadata = fs::metadata(&path).ok().filter(fs::Metadata::is_file);
        let Some(metadata) = metadata else {
            if row.is_some_and(|row| row.state == AVAILABLE) {
                self.note_missing(object);
            }
            return false;
        };
        let (len, mtime) = (metadata.len(), mtime_ns(&metadata));
        let expected = known_size(&self.db, object).ok().flatten();
        if expected.is_some_and(|size| size != len) {
            if row.is_none_or(|row| row.state != CORRUPT) {
                self.note_corrupt(object, len);
            }
            return false;
        }
        let fresh = row.is_some_and(|row| {
            row.state == AVAILABLE && row.size == len as i64 && row.mtime_ns == mtime
        });
        if !fresh {
            let _ = self.set_availability(object, len as i64, AVAILABLE, mtime, 0);
        }
        true
    }
    /// Index-trusting availability for demand bookkeeping: a row that says
    /// available is confirmed with one `stat` (and healed); a row that says
    /// missing is believed (a wrong "missing" only causes a verified re-transfer).
    pub(crate) fn object_available_for_demand(&self, object: &str) -> bool {
        match self.availability_row(object) {
            Ok(Some(row)) if row.state != AVAILABLE => false,
            Ok(Some(_)) => self.object_available_checked(object),
            Ok(None) | Err(_) => self.object_present(object),
        }
    }
    /// Shareable: published by local authorship or received through a verified
    /// transfer (availability is separate).
    pub(crate) fn object_shareable(&self, object: &str) -> Result<bool> {
        Ok(self
            .db
            .prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM published_objects WHERE hash=?1)
                 OR EXISTS(SELECT 1 FROM received_shared_objects WHERE hash=?1)",
            )?
            .query_row([object], |r| r.get(0))?)
    }
    /// Referenced by a committed **shared** event.
    pub fn objects_referenced_committed(&self, object: &str) -> Result<bool> {
        Ok(self
            .db
            .prepare_cached("SELECT EXISTS(SELECT 1 FROM event_objects WHERE hash=? AND shared=1)")?
            .query_row([object], |r| r.get(0))?)
    }
    /// Referenced by a validly staged event (an inbox row that is neither
    /// quarantined nor blocked; staged events are always shared).
    pub fn objects_referenced_staged(&self, object: &str) -> Result<bool> {
        Ok(self
            .db
            .prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM staged_objects s JOIN incoming_events i ON i.id=s.event
                 WHERE s.hash=? AND i.error IS NULL)",
            )?
            .query_row([object], |r| r.get(0))?)
    }
    /// Record a per-object failure with an explicit retry time. Attempts count
    /// up until the object becomes available (or is registered).
    pub fn record_object_error(
        &self,
        object: &str,
        error: &ErrorWire,
        retry_at_ms: u64,
    ) -> Result<()> {
        self.db
            .prepare_cached(
                "INSERT INTO object_error_state(hash,code,class,attempts,retry_at_ms,last_ms) VALUES(?,?,?,1,?,?)
                 ON CONFLICT(hash) DO UPDATE SET code=excluded.code,class=excluded.class,
                 attempts=object_error_state.attempts+1,retry_at_ms=excluded.retry_at_ms,last_ms=excluded.last_ms",
            )
            .and_then(|mut statement| {
                statement.execute(params![
                    object,
                    error.code,
                    error.class.as_str(),
                    retry_at_ms as i64,
                    time() as i64
                ])
            })?;
        Ok(())
    }
    /// Record a failure with the §8 object backoff (30 s → 1 h by attempt).
    /// Contention is never recorded. Returns the retry time.
    pub fn record_object_failure(&self, object: &str, error: &ErrorWire) -> Result<u64> {
        let now = time();
        if error.class == ErrorClass::Contention {
            return Ok(now);
        }
        let attempts: u32 = self
            .db
            .prepare_cached("SELECT attempts FROM object_error_state WHERE hash=?")?
            .query_row([object], |r| r.get(0))
            .optional()?
            .unwrap_or(0);
        let delay = crate::util::backoff::Backoff::objects().nominal(attempts.saturating_add(1));
        let retry_at = now.saturating_add(delay.as_millis() as u64);
        self.record_object_error(object, error, retry_at)?;
        Ok(retry_at)
    }
    /// The retry time of `object` while it is inside its failure backoff.
    pub fn object_backoff(&self, object: &str) -> Result<Option<u64>> {
        let retry_at: Option<i64> = self
            .db
            .prepare_cached("SELECT retry_at_ms FROM object_error_state WHERE hash=?")?
            .query_row([object], |r| r.get(0))
            .optional()?;
        Ok(retry_at
            .map(|at| at.max(0) as u64)
            .filter(|at| *at > time()))
    }
    /// Every object currently inside its failure backoff.
    pub(crate) fn objects_in_backoff(&self) -> Result<HashSet<String>> {
        Ok(self
            .db
            .prepare_cached("SELECT hash FROM object_error_state WHERE retry_at_ms>?")?
            .query_map([time() as i64], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }
    /// Download state of one object (a `stat`, for a handful of objects).
    pub fn object_state(&self, object: &str) -> Result<Value> {
        let available = self.object_present(object)
            && self
                .availability_row(object)?
                .is_none_or(|row| row.state != CORRUPT);
        let size = self.object_size_if_known(object)?;
        let partial = fs::metadata(self.root.join("incoming-objects").join(object))
            .ok()
            .map(|m| m.len());
        let requested: bool = self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM object_requests WHERE hash=?)",
            [object],
            |r| r.get(0),
        )?;
        let error: Option<String> = self
            .db
            .query_row(
                "SELECT error FROM object_errors WHERE hash=?",
                [object],
                |r| r.get(0),
            )
            .optional()?;
        let error = match error {
            Some(error) => Some(error),
            None => self
                .db
                .query_row(
                    "SELECT code FROM object_error_state WHERE hash=?",
                    [object],
                    |r| r.get(0),
                )
                .optional()?,
        };
        let state = if available {
            "available"
        } else if error.is_some() {
            "retrying"
        } else if size.is_some() && partial == size {
            "verifying"
        } else if partial.is_some() {
            "downloading"
        } else if requested {
            "queued"
        } else {
            "missing"
        };
        Ok(
            json!({"object":object,"size":size,"size_known":size.is_some(),"state":state,"downloaded":partial.unwrap_or(0),"error":if available { None } else { error }}),
        )
    }
    /// Open and fully verify an object. Read paths heal the index: a missing
    /// file marks an available row missing, a hash mismatch marks it corrupt.
    pub fn open_object(&self, object: &str) -> Result<fs::File> {
        valid_object(object)?;
        let mut file = match fs::File::open(self.root.join("objects").join(object)) {
            Ok(file) => file,
            Err(error) => {
                if error.kind() == std::io::ErrorKind::NotFound {
                    self.note_missing(object);
                }
                return Err(object_error(error));
            }
        };
        let len = file.metadata()?.len();
        ensure!(len <= MAX_CONTENT, "FILE_TOO_LARGE");
        self.count_hash();
        if stream_hash(&mut file)? != object {
            self.note_corrupt(object, len);
            bail!("CORRUPT_OBJECT: {object}");
        }
        file.rewind()?;
        Ok(file)
    }
    pub fn verify_object(&self, object: &str) -> Result<()> {
        self.open_object(object).map(|_| ())
    }
    /// Verify existing bytes (a dedupe) and record them as available, with the
    /// mtime read before hashing.
    fn verify_existing(&self, object: &str) -> Result<()> {
        let path = self.root.join("objects").join(object);
        let before = fs::metadata(&path).ok();
        self.verify_object(object)?;
        if let Some(metadata) = before {
            self.note_available(object, metadata.len(), mtime_ns(&metadata), time());
        }
        Ok(())
    }
    pub fn put_stream(&self, source: &mut impl Read) -> Result<String> {
        let temp = self.root.join("objects").join(format!("{}.part", id()));
        let result = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)?;
            let mut digest = Sha256::new();
            let mut buffer = [0; 64 * 1024];
            let mut size = 0u64;
            loop {
                let count = source.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                size += count as u64;
                ensure!(size <= MAX_CONTENT, "FILE_TOO_LARGE");
                digest.update(&buffer[..count]);
                file.write_all(&buffer[..count])?;
            }
            file.sync_all()?;
            let mtime = stamp(&file)?;
            drop(file);
            fault("object_flushed");
            let object = hex::encode(digest.finalize());
            let dest = self.root.join("objects").join(&object);
            if dest.exists() {
                self.verify_existing(&object)?;
                fs::remove_file(&temp)?;
                fault("object_installed");
            } else {
                install_object(&temp, &dest)?;
                fault("object_installed");
                self.note_available(&object, size, mtime, time());
            }
            Ok(object)
        })();
        if result.is_err() {
            let _ = fs::remove_file(temp);
        }
        result
    }
    pub fn put_object(&self, bytes: &[u8]) -> Result<String> {
        ensure!(bytes.len() <= MAX_FILE, "FILE_TOO_LARGE");
        let object = hash(bytes);
        let dest = self.root.join("objects").join(&object);
        if dest.exists() {
            self.verify_existing(&object)?;
            return Ok(object);
        }
        let temp = self.root.join("objects").join(format!("{}.part", id()));
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        let mtime = stamp(&f)?;
        drop(f);
        fault("object_flushed");
        // MoveFileExW WRITE_THROUGH below supplies the Windows rename barrier.
        install_object(&temp, &dest)?;
        fault("object_installed");
        self.note_available(&object, bytes.len() as u64, mtime, time());
        Ok(object)
    }
    /// Shareable and available objects: one SQL join over the grants, the
    /// index and the size descriptors, confirmed against the cached listing of
    /// `objects/` (never one `stat` per object), so bytes deleted behind the
    /// store's back are never advertised. Known-corrupt bytes are excluded.
    pub fn shared_object_inventory(&self) -> Result<BTreeSet<String>> {
        let presence = self.presence()?;
        let mut statement = self.db.prepare_cached(
            "SELECT p.hash,a.state,z.size FROM (SELECT hash FROM published_objects UNION SELECT hash FROM received_shared_objects) p
             LEFT JOIN object_availability a ON a.hash=p.hash LEFT JOIN object_sizes z ON z.hash=p.hash",
        )?;
        let mut rows = statement.query([])?;
        let mut inventory = BTreeSet::new();
        while let Some(row) = rows.next()? {
            let object = row.get_ref(0)?.as_str()?;
            if row.get::<_, Option<i64>>(1)? != Some(CORRUPT) && presence.has(object, row.get(2)?) {
                inventory.insert(object.to_owned());
            }
        }
        Ok(inventory)
    }
    /// Physically available (installed, size-consistent, not known corrupt)
    /// according to the cached listing; for loops over many objects.
    pub(crate) fn available_in(&self, presence: &Presence, object: &str) -> Result<bool> {
        let row = self.availability_row(object)?;
        Ok(row.is_none_or(|row| row.state != CORRUPT)
            && presence.has(object, known_size(&self.db, object)?.map(|s| s as i64)))
    }
    /// Verify up to `budget_bytes` of available objects (at least one object
    /// when the budget is positive): unverified rows first, then racy ones,
    /// then the least recently verified. Corrupt bytes are quarantined.
    pub fn scrub_step(&self, budget_bytes: u64) -> Result<ScrubOutcome> {
        let mut outcome = ScrubOutcome::default();
        if budget_bytes == 0 {
            return Ok(outcome);
        }
        let candidates: Vec<String> = self
            .db
            .prepare_cached(
                "SELECT hash FROM object_availability WHERE state=?1
                 ORDER BY CASE WHEN verified_ms=0 THEN 0 WHEN verified_ms*1000000-mtime_ns<?2 THEN 1 ELSE 2 END,
                 verified_ms,hash LIMIT 256",
            )?
            .query_map(params![AVAILABLE, RACY_NS], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        for object in candidates {
            if outcome.bytes >= budget_bytes {
                break;
            }
            let path = self.root.join("objects").join(&object);
            let Some(metadata) = fs::metadata(&path).ok().filter(fs::Metadata::is_file) else {
                self.note_missing(&object);
                self.queue_request(&object)?;
                continue;
            };
            let (len, mtime) = (metadata.len(), mtime_ns(&metadata));
            outcome.bytes = outcome.bytes.saturating_add(len);
            match self.verify_object(&object) {
                Ok(()) => {
                    self.set_availability(&object, len as i64, AVAILABLE, mtime, time() as i64)?;
                    outcome.verified += 1;
                }
                Err(error) if crate::model::errors::code_of(&error) == "CORRUPT_OBJECT" => {
                    self.quarantine(&object, &error)?;
                    outcome.corrupt += 1;
                }
                // Missing (healed by `open_object`) or unreadable right now: next step.
                Err(_) => {}
            }
        }
        Ok(outcome)
    }
}

/// Per-object mutual exclusion for concurrent writers of one spool/install
/// (invariant 19). Entries are dropped once no holder remains.
#[derive(Default)]
pub struct ObjectLocks(Mutex<BTreeMap<String, Weak<Mutex<()>>>>);
impl ObjectLocks {
    pub fn for_object(&self, object: &str) -> Arc<Mutex<()>> {
        let mut locks = self.0.lock().unwrap();
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(object).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(object.to_owned(), Arc::downgrade(&lock));
        lock
    }
}

#[cfg(windows)]
pub(crate) fn install_object(temp: &Path, dest: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn MoveFileExW(old: *const u16, new: *const u16, flags: u32) -> i32;
    }
    let old: Vec<_> = temp.as_os_str().encode_wide().chain(Some(0)).collect();
    let new: Vec<_> = dest.as_os_str().encode_wide().chain(Some(0)).collect();
    if unsafe { MoveFileExW(old.as_ptr(), new.as_ptr(), 8) } == 0 {
        bail!(std::io::Error::last_os_error());
    }
    Ok(())
}
#[cfg(not(windows))]
pub(crate) fn install_object(temp: &Path, dest: &Path) -> Result<()> {
    fs::rename(temp, dest)?;
    fs::File::open(dest.parent().unwrap())?.sync_all()?;
    Ok(())
}
