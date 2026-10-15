//! Object transfer between replicas: bundles, resumable parts, spools and
//! verified registration of received objects.
//!
//! Receiving is per object (WP1.8): `receive_parts_outcomes` returns one
//! `PartOutcome` per part, so one bad object never discards the objects that
//! completed in the same page. The legacy all-or-nothing entry points stay as
//! wrappers.
//!
//! # Objects v3 (WP3.2, §6.4)
//!
//! `write_object_items` is the worker half of `objects.write`: binary parts
//! appended to `incoming-objects/<hash>` spools whose committed durable offset
//! lives in `spool_state` (invariant 2), whole-object verification before
//! install (invariant 1), and one registration transaction per call after the
//! installs (invariant 3). The legacy page receiver (`receive_part`) keeps
//! its semantics and records its spools that span parts in `spool_state` too,
//! so one spool never has two notions of its durable length.
use super::objects::{AVAILABLE, STAMP_SECS, rename_object, stamp, sync_dir};
use super::*;
use crate::model::errors::ErrorWire;
pub use crate::model::protocol::objects::{
    ObjectOutcome, ReportAction, WantItem, WantPriority, WriteItem, WriteResult,
};
use std::{
    fs::OpenOptions,
    io::{Seek, SeekFrom, Write},
};

/// Most parts one page carries. Below the receivers' 10,000-part bound
/// (`bulk_sync`), whatever the object sizes (fixes the tiny-object page cliff).
pub const MAX_PAGE_PARTS: usize = 4096;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bundle {
    pub repo: String,
    pub branches: Vec<Branch>,
    pub events: Vec<Event>,
    pub objects: BTreeMap<String, String>,
}
/// A sequential, bounded transfer segment. Whole-object hash verification is
/// required before it enters the available shared-object inventory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectPart {
    pub offset: u64,
    pub total: u64,
    pub hex: String,
}

/// What receiving one part did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartOutcome {
    /// Verified and installed (not yet registered: see `register_received`).
    Completed { object: String, size: u64 },
    /// Appended to the spool, which is now `offset` bytes long (durable).
    Appended { object: String, offset: u64 },
    /// Nothing to do: already shareable and available, or a part beyond the
    /// spool (a stale resume cursor; the sender rewinds on the next page).
    Skipped { object: String },
    /// This object failed; other parts are unaffected.
    Failed { object: String, error: ErrorWire },
}
impl PartOutcome {
    pub fn object(&self) -> &str {
        match self {
            PartOutcome::Completed { object, .. }
            | PartOutcome::Appended { object, .. }
            | PartOutcome::Skipped { object }
            | PartOutcome::Failed { object, .. } => object,
        }
    }
}

/// `(object, size)` of completed objects, as `register_received` takes them.
pub type Completed = Vec<(String, u64)>;
/// `(object, error)` of failed objects.
pub type Failures = Vec<(String, ErrorWire)>;

/// Completed objects (for `register_received`) and failures of `outcomes`.
pub fn split_outcomes(outcomes: &[PartOutcome]) -> (Completed, Failures) {
    let mut completed = vec![];
    let mut failed = vec![];
    for outcome in outcomes {
        match outcome {
            PartOutcome::Completed { object, size } => completed.push((object.clone(), *size)),
            PartOutcome::Failed { object, error } => failed.push((object.clone(), error.clone())),
            _ => {}
        }
    }
    (completed, failed)
}

/// Exact JSON bytes of one `"<object>":{"offset":o,"total":t,"hex":""}` map
/// entry plus its separator, excluding the hex digits themselves.
fn part_overhead(object: &str, offset: u64, total: u64) -> usize {
    object.len() + 3 + r#"{"offset":,"total":,"hex":""}"#.len() + 1 + digits(offset) + digits(total)
}
fn digits(value: u64) -> usize {
    value.checked_ilog10().map_or(1, |log| log as usize + 1)
}

impl Store {
    pub fn partial_objects(&self) -> Result<BTreeMap<String, u64>> {
        let mut partial = BTreeMap::new();
        let folder = self.root.join("incoming-objects");
        if folder.is_dir() {
            for entry in fs::read_dir(folder)? {
                let entry = entry?;
                let object = entry.file_name().to_string_lossy().into_owned();
                if valid_object(&object).is_ok() && entry.file_type()?.is_file() {
                    partial.insert(object, entry.metadata()?.len());
                }
            }
        }
        Ok(partial)
    }
    pub fn peer_partial_objects(&self, peer: &str) -> Result<BTreeMap<String, u64>> {
        let payload: Option<String> = self
            .db
            .query_row(
                "SELECT value FROM config WHERE key=?",
                [format!("peer-partials:{peer}")],
                |r| r.get(0),
            )
            .optional()?;
        payload
            .map(|p| serde_json::from_str(&p).map_err(Into::into))
            .transpose()
            .map(|p| p.unwrap_or_default())
    }
    pub fn remember_peer_partials(
        &self,
        peer: &str,
        offsets: &BTreeMap<String, u64>,
    ) -> Result<()> {
        ensure!(offsets.len() <= 10000, "PARTIAL_INVENTORY_TOO_LARGE");
        for (object, offset) in offsets {
            valid_object(object)?;
            ensure!(*offset <= MAX_CONTENT, "FILE_TOO_LARGE");
        }
        self.db.execute(
            "INSERT INTO config VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![
                format!("peer-partials:{peer}"),
                serde_json::to_string(offsets)?
            ],
        )?;
        Ok(())
    }
    /// Objects of committed shared events that `peer` has acknowledged.
    pub(crate) fn objects_of_events_known_by(&self, peer: &str) -> Result<BTreeSet<String>> {
        Ok(self
            .db
            .prepare_cached(
                "SELECT DISTINCT o.hash FROM peer_known k JOIN event_objects o ON o.event=k.event
                 WHERE k.peer=? AND o.shared=1",
            )?
            .query_map([peer], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }
    pub fn shared_parts(
        &self,
        page: &Bundle,
        known: &BTreeSet<String>,
        offsets: &BTreeMap<String, u64>,
        budget: usize,
    ) -> Result<BTreeMap<String, ObjectPart>> {
        self.transfer_parts(
            page,
            known,
            offsets,
            budget,
            &self.shared_object_inventory()?,
            &BTreeSet::new(),
        )
    }
    /// Object parts for a peer. An object that cannot be read is skipped (and
    /// put into its failure backoff); it never fails the page.
    pub fn transfer_parts(
        &self,
        page: &Bundle,
        known: &BTreeSet<String>,
        offsets: &BTreeMap<String, u64>,
        budget: usize,
        permitted: &BTreeSet<String>,
        wanted: &BTreeSet<String>,
    ) -> Result<BTreeMap<String, ObjectPart>> {
        let mut failures = vec![];
        let parts = self.transfer_parts_collect(
            page,
            known,
            offsets,
            budget,
            permitted,
            wanted,
            &mut failures,
        )?;
        self.note_send_failures(&failures);
        Ok(parts)
    }
    /// Heal the index and back off objects that could not be sent. Best effort;
    /// call outside any read-only snapshot transaction.
    pub(crate) fn note_send_failures(&self, failures: &[(String, ErrorWire)]) {
        for (object, error) in failures {
            self.object_available_checked(object);
            let _ = self.record_object_failure(object, error);
        }
    }
    /// `transfer_parts` without any write: unreadable objects are returned in
    /// `failures` (for callers reading inside a snapshot transaction).
    /// Objects in their failure backoff are not sent. At most `MAX_PAGE_PARTS`
    /// parts; every part is charged its exact JSON size against `budget`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn transfer_parts_collect(
        &self,
        page: &Bundle,
        known: &BTreeSet<String>,
        offsets: &BTreeMap<String, u64>,
        budget: usize,
        permitted: &BTreeSet<String>,
        wanted: &BTreeSet<String>,
        failures: &mut Vec<(String, ErrorWire)>,
    ) -> Result<BTreeMap<String, ObjectPart>> {
        let mut parts = BTreeMap::new();
        let mut used = serde_json::to_vec(page)?.len() + 1024;
        let backoff = self.objects_in_backoff()?;
        let mut candidates: Vec<_> = self
            .shared_object_inventory()?
            .into_iter()
            .filter(|h| permitted.contains(h) && !backoff.contains(h))
            .collect();
        candidates.sort_by_key(|h| (!wanted.contains(h), h.clone()));
        for object in candidates {
            if parts.len() >= MAX_PAGE_PARTS {
                break;
            }
            if known.contains(&object) || page.objects.contains_key(&object) {
                continue;
            }
            let total = match self.object_size(&object) {
                Ok(total) => total,
                Err(error) => {
                    failures.push((object, ErrorWire::from_error(&error)));
                    continue;
                }
            };
            let advertised = offsets.get(&object).copied().unwrap_or(0);
            ensure!(advertised <= total, "INVALID_OBJECT_OFFSET");
            // A crash may leave a complete spool before verification/install.
            // Replay its final byte to trigger finalization without restarting.
            let offset = advertised.min(total.saturating_sub(1));
            let overhead = part_overhead(&object, offset, total);
            let count = ((budget.saturating_sub(used + overhead)) / 2)
                .min(4 * IO_CHUNK)
                .min((total - offset) as usize);
            if count == 0 && total != 0 {
                break;
            }
            // Immutable CAS was verified at installation/open; the receiver
            // independently verifies its entire assembled object before ACK.
            let read = (|| -> Result<Vec<u8>> {
                let mut file = fs::File::open(self.root.join("objects").join(&object))
                    .map_err(object_error)?;
                file.seek(SeekFrom::Start(offset))?;
                let mut bytes = vec![0; count];
                file.read_exact(&mut bytes).map_err(object_error)?;
                Ok(bytes)
            })();
            let bytes = match read {
                Ok(bytes) => bytes,
                Err(error) => {
                    failures.push((object, ErrorWire::from_error(&error)));
                    continue;
                }
            };
            used += count * 2 + overhead;
            parts.insert(
                object,
                ObjectPart {
                    offset,
                    total,
                    hex: hex::encode(bytes),
                },
            );
        }
        Ok(parts)
    }
    /// Receive parts on this connection and register the completed objects,
    /// even when another part failed. Fails with the first per-object error
    /// (after recording every failure's backoff), like the legacy page.
    pub fn receive_parts(
        &mut self,
        parts: BTreeMap<String, ObjectPart>,
        events: &[Event],
        allowed: &BTreeSet<String>,
    ) -> Result<()> {
        let outcomes = self.receive_parts_outcomes(parts, events, allowed, None)?;
        let (completed, failed) = split_outcomes(&outcomes);
        self.register_received(&completed)?;
        for (object, error) in &failed {
            let _ = self.record_object_failure(object, error);
        }
        match failed.into_iter().next() {
            Some((_, error)) => Err(error.into_anyhow()),
            None => Ok(()),
        }
    }
    /// Registration after install (invariant 3): sizes, shareable grant, index,
    /// and the end of the object's demand and error records. One transaction.
    pub fn register_received(&self, completed: &[(String, u64)]) -> Result<()> {
        if completed.is_empty() {
            return Ok(());
        }
        self.in_savepoint("register_received", || self.register_rows(completed))
    }
    /// File I/O, apart from the spool rows (`receive_part`): the caller
    /// registers completed objects on its serialized metadata connection. This
    /// avoids SQLite snapshot-upgrade races.
    pub fn receive_parts_data(
        &self,
        parts: BTreeMap<String, ObjectPart>,
        events: &[Event],
        allowed: &BTreeSet<String>,
    ) -> Result<Vec<(String, u64)>> {
        self.receive_parts_data_locked(parts, events, allowed, None)
    }
    /// Legacy all-or-nothing wrapper of `receive_parts_outcomes`: the completed
    /// objects, or the first per-object error.
    pub(crate) fn receive_parts_data_locked(
        &self,
        parts: BTreeMap<String, ObjectPart>,
        events: &[Event],
        allowed: &BTreeSet<String>,
        locks: Option<&ObjectLocks>,
    ) -> Result<Vec<(String, u64)>> {
        let outcomes = self.receive_parts_outcomes(parts, events, allowed, locks)?;
        let (completed, failed) = split_outcomes(&outcomes);
        match failed.into_iter().next() {
            Some((_, error)) => Err(error.into_anyhow()),
            None => Ok(completed),
        }
    }
    /// Receive each part independently (file I/O; the only database writes
    /// are index heals and the `spool_state` row of a spool that spans parts,
    /// see `receive_part`). Page-level checks (`UNAUTHORIZED_EVENT`) fail the
    /// call; every per-object problem becomes `PartOutcome::Failed` for that
    /// object only.
    ///
    /// An object is authorized when a committed shared event, or one of
    /// `events`, references it. After taking the object lock, an object that
    /// is already shareable and available is skipped without re-hashing; a
    /// physically present but private (unshared) object is transferred and
    /// verified again, then registered by the caller (I4).
    pub fn receive_parts_outcomes(
        &self,
        parts: BTreeMap<String, ObjectPart>,
        events: &[Event],
        allowed: &BTreeSet<String>,
        locks: Option<&ObjectLocks>,
    ) -> Result<Vec<PartOutcome>> {
        self.receive_parts_until(parts, events, allowed, locks, None)
    }
    /// `receive_parts_outcomes` with a time budget: once `deadline` passes,
    /// the remaining parts are `Skipped` untouched (at least one part is always
    /// received). Installing an object costs a few fsyncs, so a page of
    /// thousands of tiny objects would otherwise outlive the caller's RPC
    /// deadline; skipped objects stay out of the reply's inventory and the
    /// sender offers them again on the next page.
    pub fn receive_parts_until(
        &self,
        parts: BTreeMap<String, ObjectPart>,
        events: &[Event],
        allowed: &BTreeSet<String>,
        locks: Option<&ObjectLocks>,
        deadline: Option<std::time::Instant>,
    ) -> Result<Vec<PartOutcome>> {
        ensure!(
            events
                .iter()
                .all(|e| e.repo == self.repo && e.branch.shared && allowed.contains(&e.device)),
            "UNAUTHORIZED_EVENT"
        );
        let page_objects: BTreeSet<String> = events.iter().flat_map(Event::objects).collect();
        let mut outcomes = Vec::with_capacity(parts.len());
        for (object, part) in parts {
            if !outcomes.is_empty() && deadline.is_some_and(|d| std::time::Instant::now() >= d) {
                outcomes.push(PartOutcome::Skipped { object });
                continue;
            }
            outcomes.push(
                match self.receive_part(&object, &part, &page_objects, locks) {
                    Ok(outcome) => outcome,
                    Err(error) => PartOutcome::Failed {
                        error: ErrorWire::from_error(&error),
                        object,
                    },
                },
            );
        }
        Ok(outcomes)
    }
    fn receive_part(
        &self,
        object: &str,
        part: &ObjectPart,
        page_objects: &BTreeSet<String>,
        locks: Option<&ObjectLocks>,
    ) -> Result<PartOutcome> {
        valid_object(object)?;
        ensure!(
            page_objects.contains(object) || self.objects_referenced_committed(object)?,
            "UNAUTHORIZED_OBJECT"
        );
        let lock = locks.map(|locks| locks.for_object(object));
        let _guard = lock.as_ref().map(|lock| lock.lock().unwrap());
        let folder = self.root.join("incoming-objects");
        let path = folder.join(object);
        let skipped = || PartOutcome::Skipped {
            object: object.to_owned(),
        };
        // Re-checked under the object lock: another lane may have just finished.
        if self.object_shareable(object)? && self.object_available_for_demand(object) {
            // Duplicate part: no re-hash. Drop an orphaned spool, if any (a
            // row without its spool is harmless: every reader clamps it to
            // the file, and the next open drops it).
            if fs::remove_file(&path).is_ok() {
                let _ = self.delete_spool_row(object);
            }
            return Ok(skipped());
        }
        ensure!(
            part.total <= MAX_CONTENT && part.hex.len() <= 8 * IO_CHUNK,
            "INVALID_OBJECT_PART"
        );
        if let Ok(size) = self.object_size(object) {
            ensure!(size == part.total, "OBJECT_SIZE_CHANGED");
        }
        let bytes = hex::decode(&part.hex).context("INVALID_OBJECT_PART")?;
        ensure!(
            (!bytes.is_empty() || part.total == 0 && part.offset == 0)
                && part
                    .offset
                    .checked_add(bytes.len() as u64)
                    .is_some_and(|end| end <= part.total),
            "INVALID_OBJECT_PART"
        );
        let existing = OpenOptions::new().read(true).write(true).open(&path);
        // Whether this spool may have a `spool_state` row (it existed before
        // this part); a spool this part creates has none.
        let mut row = existing.is_ok();
        let mut file = match existing {
            Ok(file) => file,
            // A peer may retain a pre-crash resume cursor. Never create a spool
            // that would start with a gap; the next page rewinds to offset 0.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && part.offset > 0 => {
                return Ok(skipped());
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir_all(&folder)?;
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&path)?
            }
            Err(error) => return Err(object_error(error)),
        };
        let mut length = file.metadata()?.len();
        if length > part.total {
            file.set_len(0)?;
            file.sync_all()?;
            length = 0;
            let _ = self.delete_spool_row(object);
        }
        // Return the current inventory without accepting a gap.
        if part.offset > length {
            return Ok(skipped());
        }
        let mut synced = false;
        if part.offset < length {
            // Concurrent peers or a changed pool may use different chunk
            // boundaries. Verify the durable overlap, then append its suffix.
            let overlap = bytes.len().min((length - part.offset) as usize);
            file.seek(SeekFrom::Start(part.offset))?;
            let mut old = vec![0; overlap];
            file.read_exact(&mut old)?;
            if old != bytes[..overlap] {
                file.set_len(0)?;
                file.sync_all()?;
                let _ = self.delete_spool_row(object);
                bail!("OBJECT_PART_CHANGED");
            }
            if overlap < bytes.len() {
                file.seek(SeekFrom::Start(length))?;
                file.write_all(&bytes[overlap..])?;
                file.sync_all()?;
                synced = true;
                length += (bytes.len() - overlap) as u64;
            }
        } else {
            file.seek(SeekFrom::Start(length))?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            synced = true;
            length += bytes.len() as u64;
        }
        if synced && length != part.total {
            // Each append is fsynced before it is reported (the legacy rule);
            // the row keeps the v3 path and the startup reconciliation in step.
            // Best effort: a missing row only costs a re-transfer. One
            // autocommit per part that leaves its object incomplete; an
            // object received in one part (every tiny one) costs none: it is
            // completed, or its spool dropped, right below.
            row = self.upsert_spool(object, length, Some(part.total)).is_ok() || row;
        }
        if length != part.total {
            return Ok(PartOutcome::Appended {
                object: object.to_owned(),
                offset: length,
            });
        }
        // Complete. A spool finished by an earlier call (or process) is
        // flushed here; one this call appended to is already durable.
        if !synced {
            file.sync_all()?;
        }
        file.rewind()?;
        self.count_hash();
        if stream_hash(&mut file)? != object {
            drop(file);
            fs::remove_file(&path)?;
            if row {
                let _ = self.delete_spool_row(object);
            }
            bail!("CORRUPT_OBJECT: {object}");
        }
        stamp(&file)?;
        drop(file);
        let dest = self.root.join("objects").join(object);
        if dest.exists() && self.verify_object(object).is_ok() {
            // Present already (for example private bytes): keep the verified copy.
            fs::remove_file(&path)?;
        } else {
            if dest.exists() {
                // Corrupt local bytes: set them aside, install the verified spool.
                fs::rename(&dest, dest.with_extension(format!("corrupt-{}", id())))?;
            }
            install_object(&path, &dest)?;
            #[cfg(unix)]
            fs::File::open(&folder)?.sync_all()?;
        }
        if row {
            let _ = self.delete_spool_row(object);
        }
        // A crash here leaves an installed, unregistered object. It is not
        // shareable, so it stays wanted and is safely transferred again (I4).
        fault("object_installed");
        Ok(PartOutcome::Completed {
            object: object.to_owned(),
            size: part.total,
        })
    }
}

// ---------------------------------------------------------------------------
// Objects v3, worker side (WP3.2, §6.4)

/// Most items one `objects.write` call may carry.
pub const MAX_WRITE_ITEMS: usize = 4096;
/// `config` marker: legacy spools (written before `spool_state` existed) were
/// adopted at their file length, once (§6.4).
pub const SPOOLS_MARKER: &str = "spools_v1";
/// Set once the directory fsync of a batch of installs failed: from then on
/// this process installs each object with its own durable rename.
static DIR_SYNC_FAILED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// What phase C did with one complete spool.
enum Finish {
    /// The bytes did not hash to the object: the spool was dropped.
    Corrupt,
    /// In `objects/`: `barrier` when the call's directory fsync must still
    /// make it durable before registration.
    Installed { barrier: bool },
}

/// One `spool_state` row: the committed durable offset of a spool, and the
/// object's total size when known (`-1` in the table otherwise).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpoolState {
    pub durable: u64,
    pub total: Option<u64>,
}

/// The spool of `object` (`incoming-objects/<hash>`).
fn spool_path(root: &Path, object: &str) -> PathBuf {
    root.join("incoming-objects").join(object)
}

/// A per-item failure (the other items of the call are unaffected).
fn failed(hash: &str, error: ErrorWire, durable_offset: u64) -> WriteResult {
    WriteResult {
        hash: hash.to_owned(),
        outcome: ObjectOutcome::Failed(error),
        durable_offset,
    }
}

fn result(hash: &str, outcome: ObjectOutcome, durable_offset: u64) -> WriteResult {
    WriteResult {
        hash: hash.to_owned(),
        outcome,
        durable_offset,
    }
}

/// Static checks of one write item: a canonical id, bounded sizes, a range
/// inside the object, `last` exactly when the range ends the object, and no
/// empty part except the final one.
fn check_item(item: &WriteItem) -> std::result::Result<(), ErrorWire> {
    if !super::objects::canonical_object(&item.hash) {
        return Err(ErrorWire::coded("INVALID_OBJECT_ID", &item.hash));
    }
    let end = item.offset.checked_add(item.len);
    let valid = item.total <= MAX_CONTENT
        && end.is_some_and(|end| end <= item.total)
        && item.last == (end == Some(item.total))
        && (item.len > 0 || item.last);
    if !valid {
        return Err(ErrorWire::coded(
            "INVALID_OBJECT_PART",
            format!(
                "{}: offset {} + len {} of total {} (last: {})",
                item.hash, item.offset, item.len, item.total, item.last
            ),
        ));
    }
    Ok(())
}

/// SHA-256 of everything in `file` (from its start), in reads of up to 1 MiB
/// (the buffer sized for `len`, the expected length).
fn hash_from_start(file: &mut fs::File, len: u64) -> Result<String> {
    file.rewind()?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0; len.clamp(1, IO_CHUNK as u64) as usize];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(hex::encode(digest.finalize()))
}

/// What `objects.write` checks about one object (`Store::write_facts`).
struct WriteFacts {
    referenced: bool,
    shareable: bool,
    size: Option<u64>,
    row: Option<SpoolState>,
}

/// What phase A (append and fsync) left for one object of a write call.
struct Group {
    hash: String,
    total: u64,
    /// The spool's bytes on disk after this call's appends (fsynced).
    durable: u64,
    /// The committed offset before this call (0 without a row).
    committed: u64,
    /// Items answered once the durable offset is committed (or the object
    /// registered): their indices in the call.
    pending: Vec<usize>,
    /// The spool holds all `total` bytes: verify, install, register.
    complete: bool,
    /// The spool must be recorded at `durable` (it changed, or its row did).
    record: bool,
    /// Every item already has its result; nothing to commit.
    settled: bool,
    file: Option<fs::File>,
}

/// Startup spool reconciliation (§6.4; phase 4 of `Store::open`, after the
/// object check). Nothing is registered here (I4).
///
/// * Once per store (`spools_v1`), a legacy spool without a row adopts its
///   file length (the legacy code fsynced every append before advertising
///   it). Afterwards a spool without a row has durable offset 0.
/// * A spool longer than its recorded durable offset is truncated to it: the
///   bytes past it were written, maybe fsynced, but never advertised (a crash
///   between the fsync and the commit, or before the fsync).
/// * A row whose spool is gone, or a spool shorter than its row, records what
///   is there (the whole-object hash decides at completion).
/// * The spool of an object that is already shareable and available, or whose
///   length exceeds the object's known size, is dropped.
pub(super) fn reconcile_spools(store: &Store) -> Result<()> {
    let folder = store.root.join("incoming-objects");
    let mut files = BTreeMap::new();
    if folder.is_dir() {
        for entry in fs::read_dir(&folder)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if !super::objects::canonical_object(name) || !entry.file_type()?.is_file() {
                continue;
            }
            files.insert(name.to_owned(), entry.metadata()?.len());
        }
    }
    let rows: BTreeMap<String, SpoolState> = store
        .db
        .prepare("SELECT hash,durable,total FROM spool_state")?
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                SpoolState {
                    durable: r.get::<_, i64>(1)?.max(0) as u64,
                    total: u64::try_from(r.get::<_, i64>(2)?).ok(),
                },
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;
    let adopt = store
        .db
        .query_row(
            "SELECT value FROM config WHERE key=?",
            [SPOOLS_MARKER],
            |r| r.get::<_, String>(0),
        )
        .optional()?
        .is_none();
    store.in_savepoint("reconcile_spools", || {
        for object in rows.keys() {
            if !files.contains_key(object) {
                store.delete_spool_row(object)?;
            }
        }
        for (object, &len) in &files {
            let path = folder.join(object);
            let reconciled = (|| -> Result<()> {
                let known = super::objects::known_size(&store.db, object)?;
                let row = rows.get(object);
                let durable = match row {
                    Some(row) if row.total.is_some_and(|t| known.is_some_and(|k| k != t)) => 0,
                    Some(row) => row.durable.min(len),
                    None if adopt => len,
                    None => 0,
                };
                let finished = store.object_shareable(object)? && store.object_available(object);
                if finished || durable == 0 || known.is_some_and(|size| durable > size) {
                    fs::remove_file(&path)?;
                    store.delete_spool_row(object)?;
                    return Ok(());
                }
                if len > durable {
                    let file = OpenOptions::new().write(true).open(&path)?;
                    file.set_len(durable)?;
                    file.sync_all()?;
                }
                if row.map(|row| row.durable) != Some(durable) {
                    store.upsert_spool(object, durable, row.and_then(|row| row.total).or(known))?;
                }
                Ok(())
            })();
            // One spool that cannot be reconciled (a file another program
            // holds open) never stops the store from opening: the write path
            // cuts every spool back to its recorded offset under the object
            // lock before it appends, and a row-less spool has no durable bytes.
            if let Err(error) = reconciled {
                let wire = ErrorWire::from_error(&error);
                crate::util::log::event!(warn, (wire.code.as_str()), {
                    "site": "store.reconcile_spools",
                    "object": object,
                    "message": wire.message,
                });
            }
        }
        store.db.execute(
            "INSERT INTO config(key,value) VALUES(?,'1') ON CONFLICT(key) DO NOTHING",
            [SPOOLS_MARKER],
        )?;
        Ok(())
    })
}

impl Store {
    /// Run `f` in a write transaction: `BEGIN IMMEDIATE` on a connection in
    /// autocommit (the writer slot is taken first, waiting out other writers
    /// with the busy timeout; a deferred transaction that reads before it
    /// writes fails at once with `SQLITE_BUSY` when another connection
    /// committed meanwhile, the WAL snapshot-upgrade race), else a savepoint
    /// inside the caller's transaction.
    pub(crate) fn write_transaction<T>(
        &self,
        name: &str,
        f: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        if !self.db.is_autocommit() {
            return self.in_savepoint(name, f);
        }
        self.db.execute_batch("BEGIN IMMEDIATE")?;
        match f() {
            Ok(value) => match self.db.execute_batch("COMMIT") {
                Ok(()) => Ok(value),
                Err(error) => {
                    let _ = self.db.execute_batch("ROLLBACK");
                    Err(error.into())
                }
            },
            Err(error) => {
                let _ = self.db.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }
    /// The committed spool state of `object`, if it has a row.
    pub fn spool_state(&self, object: &str) -> Result<Option<SpoolState>> {
        Ok(self
            .db
            .prepare_cached("SELECT durable,total FROM spool_state WHERE hash=?")?
            .query_row([object], |r| {
                Ok(SpoolState {
                    durable: r.get::<_, i64>(0)?.max(0) as u64,
                    total: u64::try_from(r.get::<_, i64>(1)?).ok(),
                })
            })
            .optional()?)
    }
    /// Upsert one `spool_state` row (joins the caller's transaction).
    pub(crate) fn upsert_spool(
        &self,
        object: &str,
        durable: u64,
        total: Option<u64>,
    ) -> Result<()> {
        self.db
            .prepare_cached(
                "INSERT INTO spool_state(hash,durable,total,updated_ms) VALUES(?,?,?,?)
                 ON CONFLICT(hash) DO UPDATE SET durable=excluded.durable,total=excluded.total,
                 updated_ms=excluded.updated_ms",
            )?
            .execute(params![
                object,
                durable as i64,
                total.map_or(-1, |t| t as i64),
                time() as i64
            ])?;
        Ok(())
    }
    pub(crate) fn delete_spool_row(&self, object: &str) -> Result<()> {
        self.db
            .prepare_cached("DELETE FROM spool_state WHERE hash=?")?
            .execute([object])?;
        Ok(())
    }
    /// The sizes `object` may have: its committed size descriptor when there
    /// is one, else every size validly staged events claim for it (empty:
    /// nothing is known, any size up to `MAX_CONTENT` may complete it; the
    /// whole-object hash decides).
    pub fn expected_object_sizes(&self, object: &str) -> Result<BTreeSet<u64>> {
        if let Some(size) = super::objects::known_size(&self.db, object)? {
            return Ok(BTreeSet::from([size]));
        }
        let path = format!("$.object_sizes.\"{object}\"");
        Ok(self
            .db
            .prepare_cached(
                "SELECT DISTINCT json_extract(i.payload,?2) FROM staged_objects s
                 JOIN incoming_events i ON i.id=s.event
                 WHERE s.hash=?1 AND i.error IS NULL AND json_valid(i.payload)",
            )?
            .query_map(params![object, path], |r| r.get::<_, Option<i64>>(0))?
            .filter_map(|size| match size {
                Ok(Some(size)) => u64::try_from(size).ok().map(Ok),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<rusqlite::Result<_>>()?)
    }
    /// Whether this replica wants the bytes of `object` (§6.4): referenced by
    /// a committed shared event or a validly staged one, and not already
    /// shareable and available. A physically present but private copy is
    /// wanted: only a verified transfer makes it shareable (I4).
    pub fn object_wanted(&self, object: &str) -> Result<bool> {
        let referenced =
            self.objects_referenced_committed(object)? || self.objects_referenced_staged(object)?;
        Ok(referenced
            && !(self.object_shareable(object)? && self.object_available_for_demand(object)))
    }

    /// Objects v3 `objects.write` (§6.4): append each item's bytes to its
    /// object's spool, durably, and verify, install and register the objects
    /// that complete. `body` is the items' bytes, concatenated in item order.
    /// One result per item, in item order; one object's failure never affects
    /// another's. Invariants 1–3, literally:
    ///
    /// 1. **Phase A, append and fsync** (per object, under its lock from
    ///    `locks`, all locks taken in hash order and held to the end of the
    ///    call). The spool is first cut back to its committed durable offset.
    ///    An append must start at that offset (`RESUME_OFFSET_AHEAD`
    ///    otherwise); bytes before it are compared byte for byte with the
    ///    spool, and a mismatch resets the spool (`OBJECT_PART_CHANGED`). A
    ///    spool that changed is fsynced once. The appends and fsyncs of
    ///    concurrent calls take turns (the spool I/O gate of `ObjectLocks`).
    /// 2. **Phase B, commit**: one transaction records the new durable offset
    ///    of every incomplete spool. Only after it commits is an offset
    ///    advertised (`Progressed{durable_offset}`, invariant 2).
    /// 3. **Phase C, verify and install** each complete object: a full re-read
    ///    SHA-256 of the spool; a mismatch drops the spool and answers
    ///    `CorruptAtSource` (nothing is installed, invariant 1). A match
    ///    (stamped before its data fsync) is renamed into `objects/`, and one
    ///    fsync of `objects/` makes the call's renames durable before any of
    ///    them is registered ("rename plus directory fsync"; on Windows
    ///    `FlushFileBuffers` on the directory). If that directory fsync ever
    ///    fails, the call's renamed objects are answered `OBJECT_IO_ERROR`
    ///    (not registered, still wanted) and this process installs each
    ///    object with its own durable rename from then on (`MoveFileExW`
    ///    WRITE_THROUGH, or rename plus directory fsync).
    /// 4. **Phase D, registration** of every object installed by this call in
    ///    one transaction (sizes, `received_shared_objects`, requests and error
    ///    state cleared, availability, spool rows dropped). A crash between
    ///    install and registration leaves an object that is not shareable,
    ///    so it is still wanted and transferred again (invariant 3).
    ///
    /// Authorization: an object that is not wanted is `Unsolicited` (or
    /// `Skipped` when it is already shareable and available); a size that
    /// differs from the committed (or every staged) size is `SizeMismatch`.
    /// Leases are the caller's (`objects_rpc`).
    pub fn write_object_items(
        &self,
        items: &[WriteItem],
        body: &[u8],
        locks: &ObjectLocks,
    ) -> Result<Vec<WriteResult>> {
        ensure!(
            items.len() <= MAX_WRITE_ITEMS,
            "INVALID_OBJECT_PART: {} items in one write (at most {MAX_WRITE_ITEMS})",
            items.len()
        );
        let described = items
            .iter()
            .try_fold(0u64, |sum, item| sum.checked_add(item.len))
            .context("INVALID_OBJECT_PART: item lengths overflow")?;
        ensure!(
            described == body.len() as u64,
            "INVALID_OBJECT_PART: the items describe {described} bytes, the body has {}",
            body.len()
        );
        let mut slices = Vec::with_capacity(items.len());
        let mut at = 0usize;
        for item in items {
            let len = item.len as usize;
            slices.push(&body[at..at + len]);
            at += len;
        }
        let mut results: Vec<Option<WriteResult>> = vec![None; items.len()];
        let mut by_object: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
        for (index, item) in items.iter().enumerate() {
            match check_item(item) {
                Ok(()) => by_object.entry(item.hash.as_str()).or_default().push(index),
                Err(error) => results[index] = Some(failed(&item.hash, error, 0)),
            }
        }
        // Locks in hash order (the map's order): two calls never wait on each
        // other in a cycle; the legacy receive path holds one lock at a time.
        let held: Vec<_> = by_object.keys().map(|o| locks.for_object(o)).collect();
        let _guards: Vec<_> = held
            .iter()
            .map(|lock| {
                lock.lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
            })
            .collect();

        // Phase A: append and fsync (the spool I/O gate held: concurrent
        // calls take turns at the disk, see `ObjectLocks`).
        let mut groups = Vec::with_capacity(by_object.len());
        let io = locks.spool_io();
        for (object, indices) in &by_object {
            match self.append_group(object, indices, items, &slices, &mut results) {
                Ok(group) => groups.push(group),
                Err(error) => {
                    let wire = ErrorWire::from_error(&error);
                    let committed = self
                        .spool_state(object)
                        .ok()
                        .flatten()
                        .map_or(0, |s| s.durable);
                    for &index in indices {
                        results[index]
                            .get_or_insert_with(|| failed(object, wire.clone(), committed));
                    }
                }
            }
        }
        drop(io);
        // A crash here leaves fsynced bytes past the recorded offset: never
        // advertised, cut back at the next open (invariant 2).
        fault("object_spool_synced");

        // Phase B: record the durable offsets, then advertise them.
        let incomplete: Vec<usize> = (0..groups.len())
            .filter(|&g| !groups[g].settled && !groups[g].complete)
            .collect();
        let recorded = if incomplete.iter().any(|&g| groups[g].record) {
            self.write_transaction("spool_commit", || {
                for &g in &incomplete {
                    let group = &groups[g];
                    if !group.record {
                        continue;
                    }
                    if group.durable == 0 {
                        self.delete_spool_row(&group.hash)?;
                    } else {
                        self.upsert_spool(&group.hash, group.durable, Some(group.total))?;
                    }
                }
                Ok(())
            })
        } else {
            Ok(())
        };
        for &g in &incomplete {
            let group = &groups[g];
            for &index in &group.pending {
                results[index] = Some(match &recorded {
                    Ok(()) => result(
                        &group.hash,
                        ObjectOutcome::Progressed {
                            durable_offset: group.durable,
                        },
                        group.durable,
                    ),
                    Err(error) => {
                        failed(&group.hash, ErrorWire::from_error(error), group.committed)
                    }
                });
            }
        }

        // Phase C: verify and install complete objects: renames into
        // `objects/`, made durable together by one directory fsync before
        // any is registered ("rename plus directory fsync", §6.4).
        let batched = !DIR_SYNC_FAILED.load(std::sync::atomic::Ordering::Relaxed);
        let mut installed: Vec<(String, u64)> = vec![];
        let mut awaiting: Vec<usize> = vec![];
        let mut dropped: Vec<String> = vec![];
        let mut unfinished: Vec<(String, u64)> = vec![];
        for (g, group) in groups.iter_mut().enumerate() {
            if group.settled || !group.complete {
                continue;
            }
            match self.finish_group(group, locks, batched) {
                Ok(Finish::Installed { barrier }) => {
                    installed.push((group.hash.clone(), group.total));
                    if barrier {
                        awaiting.push(g);
                    }
                }
                Ok(Finish::Corrupt) => {
                    dropped.push(group.hash.clone());
                    for &index in &group.pending {
                        results[index] =
                            Some(result(&group.hash, ObjectOutcome::CorruptAtSource, 0));
                    }
                }
                Err(error) => {
                    // The complete spool stays; the next write finalizes it.
                    let wire = ErrorWire::from_error(&error);
                    unfinished.push((group.hash.clone(), group.total));
                    for &index in &group.pending {
                        results[index] = Some(failed(&group.hash, wire.clone(), group.committed));
                    }
                }
            }
        }
        if !awaiting.is_empty() {
            let synced = {
                let _io = locks.spool_io();
                sync_dir(&self.root.join("objects"))
            };
            if let Err(error) = synced {
                // Not known durable: not registered. They stay wanted, and the
                // next transfer verifies the present copy and registers it.
                DIR_SYNC_FAILED.store(true, std::sync::atomic::Ordering::Relaxed);
                let wire =
                    ErrorWire::coded("OBJECT_IO_ERROR", format!("objects/ fsync: {error:#}"));
                for &g in &awaiting {
                    let group = &groups[g];
                    installed.retain(|(hash, _)| *hash != group.hash);
                    dropped.push(group.hash.clone());
                    for &index in &group.pending {
                        results[index] = Some(failed(&group.hash, wire.clone(), 0));
                    }
                }
            }
        }
        // A crash here leaves installed, unregistered objects: not shareable,
        // so still wanted and transferred again (invariant 3).
        if !installed.is_empty() {
            fault("object_installed");
        }

        // Phase D: one registration transaction for the call.
        if !installed.is_empty() || !dropped.is_empty() || !unfinished.is_empty() {
            let batch = self.write_transaction("register_objects", || {
                self.register_rows(&installed)?;
                for (object, _) in &installed {
                    self.delete_spool_row(object)?;
                }
                for object in &dropped {
                    self.delete_spool_row(object)?;
                }
                for (object, total) in &unfinished {
                    self.upsert_spool(object, *total, Some(*total))?;
                }
                Ok(())
            });
            let mut registered: BTreeMap<String, Result<(), ErrorWire>> = BTreeMap::new();
            match batch {
                Ok(()) => {
                    let cache = &self.caches.objects.registrations;
                    cache.set(cache.get() + u64::from(!installed.is_empty()));
                    for (object, _) in &installed {
                        registered.insert(object.clone(), Ok(()));
                    }
                }
                Err(_) => {
                    // One object's registration (say a size descriptor that
                    // changed meanwhile) must not undo the others': each one
                    // alone.
                    for (object, size) in &installed {
                        let one = self.write_transaction("register_object", || {
                            self.register_rows(&[(object.clone(), *size)])?;
                            self.delete_spool_row(object)
                        });
                        if one.is_ok() {
                            let cache = &self.caches.objects.registrations;
                            cache.set(cache.get() + 1);
                        }
                        registered
                            .insert(object.clone(), one.map_err(|e| ErrorWire::from_error(&e)));
                    }
                    for object in &dropped {
                        let _ = self.delete_spool_row(object);
                    }
                    for (object, total) in &unfinished {
                        let _ = self.upsert_spool(object, *total, Some(*total));
                    }
                }
            }
            for group in &groups {
                let Some(outcome) = registered.get(&group.hash) else {
                    continue;
                };
                for &index in &group.pending {
                    results[index] = Some(match outcome {
                        Ok(()) => result(&group.hash, ObjectOutcome::Completed, group.total),
                        // Installed but not registered: still wanted (I4).
                        Err(error) => failed(&group.hash, error.clone(), 0),
                    });
                }
            }
        }
        Ok(results
            .into_iter()
            .zip(items)
            .map(|(result, item)| {
                result.unwrap_or_else(|| {
                    failed(
                        &item.hash,
                        ErrorWire::coded("OBJECT_IO_ERROR", "the write did not answer this item"),
                        0,
                    )
                })
            })
            .collect())
    }

    /// Phase A of `write_object_items` for one object's items (its lock held).
    fn append_group(
        &self,
        object: &str,
        indices: &[usize],
        items: &[WriteItem],
        slices: &[&[u8]],
        results: &mut [Option<WriteResult>],
    ) -> Result<Group> {
        let total = items[indices[0]].total;
        let mut group = Group {
            hash: object.to_owned(),
            total,
            durable: 0,
            committed: 0,
            pending: vec![],
            complete: false,
            record: false,
            settled: true,
            file: None,
        };
        let mut settle = |outcome: ObjectOutcome, offset: u64| {
            for &index in indices {
                results[index].get_or_insert_with(|| result(object, outcome.clone(), offset));
            }
        };
        if indices.iter().any(|&index| items[index].total != total) {
            settle(
                ObjectOutcome::Failed(ErrorWire::coded(
                    "INVALID_OBJECT_PART",
                    format!("{object}: items of one write disagree on its total"),
                )),
                0,
            );
            return Ok(group);
        }
        let path = spool_path(&self.root, object);
        let facts = self.write_facts(object)?;
        if facts.shareable && self.object_available_for_demand(object) {
            // Already ours: drop an orphaned spool, nothing to transfer. Its
            // row goes in phase B's transaction, not in an autocommit under
            // the spool I/O gate.
            let _ = fs::remove_file(&path);
            settle(ObjectOutcome::Skipped, total);
            group.record = facts.row.is_some();
            group.settled = !group.record;
            return Ok(group);
        }
        if !facts.referenced {
            settle(ObjectOutcome::Unsolicited, 0);
            return Ok(group);
        }
        let expected = match facts.size {
            Some(size) => BTreeSet::from([size]),
            None => self.expected_object_sizes(object)?,
        };
        if !expected.is_empty() && !expected.contains(&total) {
            settle(ObjectOutcome::SizeMismatch, 0);
            return Ok(group);
        }
        let row = facts.row;
        // Without a row the spool has no durable bytes: it is (re)created on
        // the first append, never opened (one create per new object).
        let mut file = match row {
            Some(_) => match OpenOptions::new().read(true).write(true).open(&path) {
                Ok(file) => Some(file),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(object_error(error)),
            },
            None => None,
        };
        let length = match &file {
            Some(file) => file.metadata()?.len(),
            None => 0,
        };
        group.committed = row.map_or(0, |row| row.durable);
        let mut durable = match row {
            // A spool of another size (an earlier, refused claim) restarts.
            Some(row) if row.total.is_some_and(|t| t != total) => 0,
            Some(row) => row.durable.min(length),
            None => 0,
        };
        if durable > total {
            durable = 0;
        }
        let mut changed = false;
        if length > durable {
            // Bytes past the committed offset were never advertised.
            if let Some(file) = &file {
                file.set_len(durable)?;
            }
            changed = true;
        }
        for &index in indices {
            let item = &items[index];
            let bytes = slices[index];
            if item.offset > durable {
                results[index] = Some(failed(
                    object,
                    ErrorWire::coded(
                        "RESUME_OFFSET_AHEAD",
                        format!("{object}: part at {} but durable at {durable}", item.offset),
                    ),
                    durable,
                ));
                continue;
            }
            let end = item.offset + item.len;
            let overlap = end.min(durable) - item.offset;
            if overlap > 0 {
                let file = file.as_mut().context("OBJECT_IO_ERROR: spool missing")?;
                file.seek(SeekFrom::Start(item.offset))?;
                let mut old = vec![0; overlap as usize];
                file.read_exact(&mut old)?;
                if old != bytes[..overlap as usize] {
                    file.set_len(0)?;
                    durable = 0;
                    changed = true;
                    results[index] = Some(failed(
                        object,
                        ErrorWire::coded(
                            "OBJECT_PART_CHANGED",
                            format!("{object}: bytes at {} differ from the spool", item.offset),
                        ),
                        0,
                    ));
                    continue;
                }
            }
            if end > durable || file.is_none() {
                if file.is_none() {
                    file = Some(self.create_spool(&path)?);
                }
                let file = file.as_mut().context("OBJECT_IO_ERROR: spool missing")?;
                file.seek(SeekFrom::Start(durable))?;
                file.write_all(&bytes[(durable - item.offset) as usize..])
                    .map_err(object_error)?;
                durable = durable.max(end);
                changed = true;
            }
            group.pending.push(index);
        }
        group.durable = durable;
        group.complete = durable == total && !group.pending.is_empty();
        if let Some(file) = &file {
            if group.complete {
                // Stamped before the flush that makes the bytes durable: the
                // installed object's availability marker is trusted at once.
                stamp(file)?;
            }
            if changed {
                file.sync_all().map_err(object_error)?;
                let syncs = &self.caches.objects.spool_syncs;
                syncs.set(syncs.get() + 1);
            }
        }
        // The row this spool needs: none at offset 0.
        let needed = (durable > 0).then_some((durable, Some(total)));
        group.record = row.map(|row| (row.durable, row.total)) != needed;
        group.settled = group.pending.is_empty() && !group.record;
        group.file = file;
        Ok(group)
    }

    /// A new, empty spool at `path` (creating `incoming-objects` on demand).
    fn create_spool(&self, path: &Path) -> Result<fs::File> {
        let open = || {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
        };
        match open() {
            Ok(file) => Ok(file),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir_all(self.root.join("incoming-objects"))?;
                Ok(open()?)
            }
            Err(error) => Err(object_error(error)),
        }
    }

    /// What `objects.write` checks about `object`, in one query: referenced by
    /// a committed shared or a validly staged event, shareable, its committed
    /// size descriptor and its spool row.
    fn write_facts(&self, object: &str) -> Result<WriteFacts> {
        Ok(self
            .db
            .prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM event_objects WHERE hash=?1 AND shared=1)
                     OR EXISTS(SELECT 1 FROM staged_objects s JOIN incoming_events i ON i.id=s.event
                               WHERE s.hash=?1 AND i.error IS NULL),
                     EXISTS(SELECT 1 FROM published_objects WHERE hash=?1)
                     OR EXISTS(SELECT 1 FROM received_shared_objects WHERE hash=?1),
                     (SELECT size FROM object_sizes WHERE hash=?1),
                     (SELECT durable FROM spool_state WHERE hash=?1),
                     (SELECT total FROM spool_state WHERE hash=?1)",
            )?
            .query_row([object], |r| {
                let durable: Option<i64> = r.get(3)?;
                let total: Option<i64> = r.get(4)?;
                Ok(WriteFacts {
                    referenced: r.get(0)?,
                    shareable: r.get(1)?,
                    size: r
                        .get::<_, Option<i64>>(2)?
                        .and_then(|s| u64::try_from(s).ok()),
                    row: durable.map(|durable| SpoolState {
                        durable: durable.max(0) as u64,
                        total: total.and_then(|t| u64::try_from(t).ok()),
                    }),
                })
            })?)
    }

    /// Phase C of `write_object_items` for one complete spool (its lock held):
    /// a whole-object hash, then the install. `batched`: the install is a
    /// plain rename that the call's directory fsync makes durable (a verified
    /// copy already present is covered by it too); otherwise a durable rename
    /// of its own (`install_object`). The install takes the spool I/O gate;
    /// the hash runs outside it.
    fn finish_group(
        &self,
        group: &mut Group,
        locks: &ObjectLocks,
        batched: bool,
    ) -> Result<Finish> {
        let object = group.hash.clone();
        let path = spool_path(&self.root, &object);
        let mut file = match group.file.take() {
            Some(file) => file,
            None => OpenOptions::new().read(true).write(true).open(&path)?,
        };
        self.count_hash();
        if hash_from_start(&mut file, group.total)? != object {
            drop(file);
            fs::remove_file(&path)?;
            return Ok(Finish::Corrupt);
        }
        drop(file);
        let dest = self.root.join("objects").join(&object);
        if dest.exists() && self.verify_object(&object).is_ok() {
            // A verified copy is present already (for example private bytes):
            // keep it; this transfer still proves the source's copy (I4).
            fs::remove_file(&path)?;
        } else {
            let _io = locks.spool_io();
            if dest.exists() {
                fs::rename(&dest, dest.with_extension(format!("corrupt-{}", id())))?;
            }
            if batched {
                rename_object(&path, &dest)?;
            } else {
                install_object(&path, &dest)?;
            }
        }
        Ok(Finish::Installed { barrier: batched })
    }

    /// The rows of `register_received`, on the caller's transaction.
    fn register_rows(&self, completed: &[(String, u64)]) -> Result<()> {
        let now = time() as i64;
        let stamp_ns = STAMP_SECS as i64 * 1_000_000_000;
        for (object, size) in completed {
            self.remember_object_size(object, *size)?;
            self.db
                .prepare_cached("INSERT OR IGNORE INTO received_shared_objects VALUES(?)")?
                .execute([object])?;
            self.db
                .prepare_cached("DELETE FROM object_requests WHERE hash=?")?
                .execute([object])?;
            self.db
                .prepare_cached("DELETE FROM object_errors WHERE hash=?")?
                .execute([object])?;
            self.db
                .prepare_cached("DELETE FROM object_error_state WHERE hash=?")?
                .execute([object])?;
            self.set_availability(object, *size as i64, AVAILABLE, stamp_ns, now)?;
        }
        Ok(())
    }
}
