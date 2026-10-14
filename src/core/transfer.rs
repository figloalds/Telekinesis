//! Object transfer between replicas: bundles, resumable parts, spools and
//! verified registration of received objects.
//!
//! Receiving is per object (WP1.8): `receive_parts_outcomes` returns one
//! `PartOutcome` per part, so one bad object never discards the objects that
//! completed in the same page. The legacy all-or-nothing entry points stay as
//! wrappers.
use super::objects::{AVAILABLE, STAMP_SECS, stamp};
use super::*;
use crate::model::errors::ErrorWire;
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
        let now = time() as i64;
        // Every object we install carries the stamp (see `objects`); a missed
        // stamp only makes the next startup re-hash it.
        let stamp_ns = STAMP_SECS as i64 * 1_000_000_000;
        self.in_savepoint("register_received", || {
            for (object, size) in completed {
                self.remember_object_size(object, *size)?;
                self.db.execute(
                    "INSERT OR IGNORE INTO received_shared_objects VALUES(?)",
                    [object],
                )?;
                self.db
                    .execute("DELETE FROM object_requests WHERE hash=?", [object])?;
                self.db
                    .execute("DELETE FROM object_errors WHERE hash=?", [object])?;
                self.db
                    .execute("DELETE FROM object_error_state WHERE hash=?", [object])?;
                self.set_availability(object, *size as i64, AVAILABLE, stamp_ns, now)?;
            }
            Ok(())
        })
    }
    /// File I/O only: the caller registers completed objects on its serialized
    /// metadata connection. This avoids SQLite snapshot-upgrade races.
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
    /// Receive each part independently (file I/O; the only writes are index
    /// heals). Page-level checks (`UNAUTHORIZED_EVENT`) fail the call; every
    /// per-object problem becomes `PartOutcome::Failed` for that object only.
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
            // Duplicate part: no re-hash. Drop an orphaned spool, if any.
            let _ = fs::remove_file(&path);
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
        // A crash here leaves an installed, unregistered object. It is not
        // shareable, so it stays wanted and is safely transferred again (I4).
        fault("object_installed");
        Ok(PartOutcome::Completed {
            object: object.to_owned(),
            size: part.total,
        })
    }
}
