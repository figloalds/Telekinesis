//! Disposable startup snapshots. The semantic journal remains authoritative.
use super::*;
use std::borrow::Cow;

// Bump when projection/validation semantics change, even without a SQL migration.
const FORMAT: u32 = 1;

/// The shortest journal distance between two periodic seals of a branch.
pub const MIN_SEAL_INTERVAL: i64 = 128;
/// The longest: it also bounds a startup's replay after the last seal
/// (`try_resume_snapshot` refuses a longer tail).
pub const MAX_SEAL_INTERVAL: i64 = 1024;
/// Sealed payload bytes per event of interval (see [`seal_interval`]).
const SEAL_BYTES_PER_EVENT: i64 = 4096;

/// The periodic seal interval of a branch whose last sealed payload has
/// `saved_bytes` bytes (CCR-WP2.4-3, applied by WP3.INT). A seal serializes,
/// hashes and stores the whole projection (O(entries)), so a fixed interval
/// made seals the dominant cost of a large linear catch-up (at 10K files,
/// 78 seals of 60-250 ms each took 15 s of a 36 s activation). The interval
/// grows with the payload, `saved_bytes / 4 KiB` events (each event pays for
/// about 4 KiB of sealing), from [`MIN_SEAL_INTERVAL`] (every payload under
/// 512 KiB, unchanged) to [`MAX_SEAL_INTERVAL`] (from 4 MiB). No seal yet:
/// the minimum. The snapshot stays disposable (the journal is
/// authoritative); a longer interval only lengthens a startup's replay, at
/// most `MAX_SEAL_INTERVAL` events.
pub fn seal_interval(saved_bytes: Option<i64>) -> i64 {
    saved_bytes.map_or(MIN_SEAL_INTERVAL, |bytes| {
        (bytes / SEAL_BYTES_PER_EVENT).clamp(MIN_SEAL_INTERVAL, MAX_SEAL_INTERVAL)
    })
}

/// The sealed cursor of a branch and its payload's length, which
/// [`seal_interval`] reads (`octet_length` reads no payload). Every reader
/// of the seal cadence uses this query: the decision (`maybe_persist`) and
/// its mirrors (the fast commit's `seal_due`, the activator's forecast).
pub const SEALED_SQL: &str =
    "SELECT cursor,octet_length(payload) FROM startup_snapshots WHERE branch=?1";

/// The sealed payload. `projection` is borrowed when the sealing commit holds
/// it (the JSON is the same either way).
#[derive(Serialize, Deserialize)]
struct Snapshot<'a> {
    format: u32,
    repo: String,
    device: String,
    branch: Branch,
    cursor: i64,
    journal_hash: String,
    projection: Cow<'a, Projection>,
    conflicts: Vec<Conflict>,
    frontier: Vec<String>,
}

pub(super) fn initialize(db: &Connection) -> Result<()> {
    // Triggers also cover writes by older executables and repair tools. Sequence
    // numbers are local acceptance order, never CRDT version precedence.
    db.execute_batch("SAVEPOINT snapshot_schema;
        CREATE TABLE IF NOT EXISTS startup_snapshots(branch TEXT PRIMARY KEY,cursor INTEGER NOT NULL,payload TEXT NOT NULL,checksum TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS journal_sequence(seq INTEGER PRIMARY KEY AUTOINCREMENT,event TEXT NOT NULL UNIQUE);
        INSERT INTO journal_sequence(event) SELECT id FROM events e WHERE NOT EXISTS(SELECT 1 FROM journal_sequence s WHERE s.event=e.id) ORDER BY rowid;
        CREATE TRIGGER IF NOT EXISTS snapshot_event_insert AFTER INSERT ON events BEGIN
            INSERT INTO journal_sequence(event) VALUES(NEW.id);
        END;
        CREATE TRIGGER IF NOT EXISTS snapshot_event_delete AFTER DELETE ON events BEGIN
            DELETE FROM startup_snapshots;
            DELETE FROM journal_sequence WHERE event=OLD.id;
        END;
        CREATE TRIGGER IF NOT EXISTS snapshot_event_update AFTER UPDATE ON events BEGIN
            DELETE FROM startup_snapshots;
            UPDATE journal_sequence SET event=NEW.id WHERE event=OLD.id;
        END;
        RELEASE snapshot_schema;")?;
    Ok(())
}

pub(super) fn load_projection(db: &Connection, branch: &str) -> Result<Projection> {
    let payload: String = db.query_row(
        "SELECT payload FROM current_state WHERE branch=?",
        [branch],
        |r| r.get(0),
    )?;
    let mut p: Projection = serde_json::from_str(&payload)?;
    let mut statement = db.prepare("SELECT entry,heads FROM current_parts WHERE branch=?")?;
    let rows = statement.query_map([branch], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (entry, heads) = row?;
        let entry: Entry = serde_json::from_str(&entry)?;
        p.entries.insert(entry.id.clone(), entry);
        let heads: BTreeMap<String, Vec<Version>> = serde_json::from_str(&heads)?;
        p.heads.extend(heads);
    }
    Ok(p)
}

fn cursor(db: &Connection, branch: &str) -> Result<i64> {
    Ok(db.query_row("SELECT coalesce(max(s.seq),0) FROM journal_sequence s JOIN events e ON e.id=s.event WHERE e.branch=?", [branch], |r| r.get(0))?)
}

fn journal_hash(db: &Connection, branch: &str, cursor: i64) -> Result<String> {
    let mut digest = Sha256::new();
    let mut statement = db.prepare("SELECT s.seq,e.id,e.payload FROM journal_sequence s JOIN events e ON e.id=s.event WHERE e.branch=? AND s.seq<=? ORDER BY s.seq")?;
    let mut rows = statement.query(params![branch, cursor])?;
    while let Some(row) = rows.next()? {
        let seq: i64 = row.get(0)?;
        digest.update(seq.to_le_bytes());
        for column in [1, 2] {
            let value: String = row.get(column)?;
            digest.update((value.len() as u64).to_le_bytes());
            digest.update(value.as_bytes());
        }
        crate::startup::progress("checking snapshot journal");
    }
    Ok(hex::encode(digest.finalize()))
}

/// Seals `branch` when `full` (a slow-path commit) or when the journal moved
/// [`seal_interval`] events past its last seal. A commit that holds the branch's
/// whole projection as it has just stored it passes it as `known`: the seal
/// serializes that instead of reading the ~7 MB (at 5K entries) back and
/// parsing it (WP1.INT fix round 1, CCR-WP1.INT-4). The payload is the same.
pub(super) fn maybe_persist(
    db: &Connection,
    branch: &str,
    full: bool,
    known: Option<&Projection>,
) -> Result<()> {
    let saved: Option<(i64, Option<i64>)> = db
        .prepare_cached(SEALED_SQL)?
        .query_row([branch], |r| Ok((r.get(0)?, r.get(1)?)))
        .optional()?;
    // O(1) on each commit. The branch-specific cursor/digest is computed only
    // when sealing; activity on another branch may merely seal this one sooner.
    let tip: i64 = db.query_row(
        "SELECT coalesce(max(seq),0) FROM journal_sequence",
        [],
        |r| r.get(0),
    )?;
    if full || saved.is_none_or(|(saved, bytes)| tip - saved >= seal_interval(bytes)) {
        persist_with(db, branch, known)?;
    }
    Ok(())
}

pub(super) fn persist(db: &Connection, branch: &str) -> Result<()> {
    persist_with(db, branch, None)
}

/// Seals `branch`; its projection is `known` when given, else the stored one.
fn persist_with(db: &Connection, branch: &str, known: Option<&Projection>) -> Result<()> {
    let cursor = cursor(db, branch)?;
    let projection = match known {
        Some(projection) => Cow::Borrowed(projection),
        None => Cow::Owned(load_projection(db, branch)?),
    };
    ensure!(projection.pending.is_empty(), "INCOMPLETE_SNAPSHOT");
    let mut conflicts = Vec::new();
    let mut statement =
        db.prepare("SELECT payload,resolved_by FROM conflicts WHERE branch=? ORDER BY id")?;
    let mut rows = statement.query([branch])?;
    while let Some(row) = rows.next()? {
        let mut conflict: Conflict = serde_json::from_str(&row.get::<_, String>(0)?)?;
        conflict.resolved_by = row.get(1)?;
        conflicts.push(conflict);
    }
    let snapshot = Snapshot {
        format: FORMAT,
        repo: db.query_row("SELECT value FROM config WHERE key='repo'", [], |r| {
            r.get(0)
        })?,
        device: db.query_row("SELECT value FROM config WHERE key='device'", [], |r| {
            r.get(0)
        })?,
        branch: db.query_row(
            "SELECT id,name,shared FROM branches WHERE id=?",
            [branch],
            |r| {
                Ok(Branch {
                    id: r.get(0)?,
                    name: r.get(1)?,
                    shared: r.get(2)?,
                })
            },
        )?,
        cursor,
        journal_hash: journal_hash(db, branch, cursor)?,
        projection,
        conflicts,
        frontier: db
            .prepare("SELECT event FROM event_frontier WHERE branch=? ORDER BY event")?
            .query_map([branch], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?,
    };
    let payload = serde_json::to_string(&snapshot)?;
    fault("before_startup_snapshot");
    db.execute("INSERT INTO startup_snapshots VALUES(?,?,?,?) ON CONFLICT(branch) DO UPDATE SET cursor=excluded.cursor,payload=excluded.payload,checksum=excluded.checksum", params![branch, cursor, payload, hash(payload.as_bytes())])?;
    fault("startup_snapshot_written");
    Ok(())
}

impl Store {
    pub(super) fn resume_snapshot(&mut self, branch: &Branch) -> Result<bool> {
        // Restoring the base and applying the suffix is atomic. In particular,
        // an unsupported concurrent/namespace event must leave no partial view.
        self.db.execute_batch("SAVEPOINT startup_replay")?;
        let result = self.try_resume_snapshot(branch);
        self.view_cache.borrow_mut().take();
        match result {
            Ok(true) => {
                self.db.execute_batch("RELEASE startup_replay")?;
                Ok(true)
            }
            result => {
                self.db
                    .execute_batch("ROLLBACK TO startup_replay; RELEASE startup_replay")?;
                if let Err(error) = result {
                    eprintln!("Startup snapshot fallback for {}: {error:#}", branch.id);
                }
                Ok(false)
            }
        }
    }

    fn try_resume_snapshot(&mut self, branch: &Branch) -> Result<bool> {
        let saved: Option<(i64, String, String)> = self
            .db
            .query_row(
                "SELECT cursor,payload,checksum FROM startup_snapshots WHERE branch=?",
                [&branch.id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let Some((saved_cursor, payload, checksum)) = saved else {
            return Ok(false);
        };
        crate::startup::progress("loading startup snapshot");
        ensure!(hash(payload.as_bytes()) == checksum, "SNAPSHOT_CHECKSUM");
        let snapshot: Snapshot = serde_json::from_str(&payload)?;
        ensure!(
            snapshot.format == FORMAT
                && snapshot.repo == self.repo
                && snapshot.device == self.device
                && snapshot.branch == *branch,
            "SNAPSHOT_VERSION_OR_IDENTITY"
        );
        ensure!(
            snapshot.cursor == saved_cursor
                && saved_cursor >= 0
                && saved_cursor <= cursor(&self.db, &branch.id)?,
            "SNAPSHOT_CURSOR"
        );
        ensure!(
            snapshot.journal_hash == journal_hash(&self.db, &branch.id, saved_cursor)?,
            "SNAPSHOT_JOURNAL_CHANGED"
        );
        ensure!(
            snapshot.projection.pending.is_empty(),
            "INCOMPLETE_SNAPSHOT"
        );
        let tail: Vec<(i64, String)> = self.db.prepare("SELECT s.seq,e.payload FROM journal_sequence s JOIN events e ON e.id=s.event WHERE e.branch=? AND s.seq>? ORDER BY s.seq")?
            .query_map(params![branch.id, saved_cursor], |r| Ok((r.get(0)?, r.get(1)?)))?.collect::<rusqlite::Result<_>>()?;
        // Bound startup work even when an older writer never consolidated.
        if tail.len() > MAX_SEAL_INTERVAL as usize {
            return Ok(false);
        }
        self.db
            .execute("DELETE FROM current_parts WHERE branch=?", [&branch.id])?;
        self.db.execute("INSERT INTO current_state VALUES(?,?) ON CONFLICT(branch) DO UPDATE SET payload=excluded.payload", params![branch.id, serde_json::to_string(&snapshot.projection)?])?;
        self.db
            .execute("DELETE FROM conflicts WHERE branch=?", [&branch.id])?;
        for c in &snapshot.conflicts {
            self.db.execute(
                "INSERT INTO conflicts VALUES(?,?,?,?)",
                params![c.id, branch.id, serde_json::to_string(c)?, c.resolved_by],
            )?;
        }
        self.db
            .execute("DELETE FROM event_frontier WHERE branch=?", [&branch.id])?;
        for id in &snapshot.frontier {
            self.db.execute(
                "INSERT INTO event_frontier VALUES(?,?)",
                params![branch.id, id],
            )?;
        }
        self.view_cache.borrow_mut().take();
        for (seq, payload) in &tail {
            crate::startup::progress("replaying snapshot suffix");
            let event: Event = serde_json::from_str(payload)?;
            ensure!(
                event.repo == self.repo && event.branch == *branch,
                "SNAPSHOT_EVENT_IDENTITY"
            );
            // A cursor orders acceptance, not causality. Legacy journals with a
            // prerequisite later in acceptance order require full reconstruction.
            for parent in event
                .dependencies
                .iter()
                .chain(event.changes.iter().flat_map(|c| c.parents.iter()))
            {
                let parent_seq: Option<i64> = self
                    .db
                    .query_row(
                        "SELECT seq FROM journal_sequence WHERE event=?",
                        [parent],
                        |r| r.get(0),
                    )
                    .optional()?;
                ensure!(
                    parent_seq.is_some_and(|parent| parent < *seq),
                    "SNAPSHOT_NON_TOPOLOGICAL_SUFFIX"
                );
            }
            // Reuse only proven incremental rules. Conflicting edits, reviews
            // and directory changes that suppress descendants use full replay.
            let mut delta = self.content_successor(&event)?;
            // Namespace successor helpers consult the active branch's review
            // history. Other branches conservatively use full reconstruction.
            if delta.is_none() && event.branch.id == self.active {
                delta = self.creation_successor(&event)?;
            }
            if delta.is_none() && event.branch.id == self.active {
                delta = self.namespace_successor(&event)?;
            }
            let Some(delta) = delta else { return Ok(false) };
            for (entity, entry) in &delta.entries {
                let heads: BTreeMap<_, _> = delta
                    .heads
                    .iter()
                    .filter(|(key, _)| key.rsplit_once('/').is_some_and(|(id, _)| id == entity))
                    .collect();
                self.db.execute("INSERT INTO current_parts VALUES(?,?,?,?) ON CONFLICT(branch,entity) DO UPDATE SET entry=excluded.entry,heads=excluded.heads", params![branch.id, entity, serde_json::to_string(entry)?, serde_json::to_string(&heads)?])?;
            }
            for parent in event
                .dependencies
                .iter()
                .chain(event.changes.iter().flat_map(|c| c.parents.iter()))
            {
                self.db.execute(
                    "DELETE FROM event_frontier WHERE branch=? AND event=?",
                    params![branch.id, parent],
                )?;
            }
            self.db.execute(
                "INSERT INTO event_frontier VALUES(?,?)",
                params![branch.id, event.id],
            )?;
            fault("startup_suffix_applied");
        }
        if !tail.is_empty() {
            persist(&self.db, &branch.id)?;
        }
        Ok(true)
    }
}
