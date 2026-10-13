//! Object transfer between replicas: bundles, resumable parts, spools and
//! verified registration of received objects.
use super::*;
use std::{
    fs::OpenOptions,
    io::{Seek, SeekFrom, Write},
};

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
    pub fn transfer_parts(
        &self,
        page: &Bundle,
        known: &BTreeSet<String>,
        offsets: &BTreeMap<String, u64>,
        budget: usize,
        permitted: &BTreeSet<String>,
        wanted: &BTreeSet<String>,
    ) -> Result<BTreeMap<String, ObjectPart>> {
        let mut parts = BTreeMap::new();
        let mut used = serde_json::to_vec(page)?.len() + 1024;
        let mut candidates: Vec<_> = self
            .shared_object_inventory()?
            .intersection(permitted)
            .cloned()
            .collect();
        candidates.sort_by_key(|h| (!wanted.contains(h), h.clone()));
        for object in candidates {
            if known.contains(&object) || page.objects.contains_key(&object) {
                continue;
            }
            let total = self.object_size(&object)?;
            let advertised = offsets.get(&object).copied().unwrap_or(0);
            ensure!(advertised <= total, "INVALID_OBJECT_OFFSET");
            // A crash may leave a complete spool before verification/install.
            // Replay its final byte to trigger finalization without restarting.
            let offset = advertised.min(total.saturating_sub(1));
            let count = ((budget.saturating_sub(used + 256)) / 2)
                .min(4 * IO_CHUNK)
                .min((total - offset) as usize);
            if count == 0 && total != 0 {
                break;
            }
            // Immutable CAS was verified at installation/open; the receiver
            // independently verifies its entire assembled object before ACK.
            let mut file =
                fs::File::open(self.root.join("objects").join(&object)).map_err(object_error)?;
            file.seek(SeekFrom::Start(offset))?;
            let mut bytes = vec![0; count];
            file.read_exact(&mut bytes)?;
            used += count * 2 + 256;
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
    pub fn receive_parts(
        &mut self,
        parts: BTreeMap<String, ObjectPart>,
        events: &[Event],
        allowed: &BTreeSet<String>,
    ) -> Result<()> {
        let completed = self.receive_parts_data(parts, events, allowed)?;
        self.register_received(&completed)
    }
    pub fn register_received(&self, completed: &[(String, u64)]) -> Result<()> {
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
        }
        Ok(())
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
    pub(crate) fn receive_parts_data_locked(
        &self,
        parts: BTreeMap<String, ObjectPart>,
        events: &[Event],
        allowed: &BTreeSet<String>,
        locks: Option<&ObjectLocks>,
    ) -> Result<Vec<(String, u64)>> {
        let mut completed = vec![];
        ensure!(
            events
                .iter()
                .all(|e| e.repo == self.repo && e.branch.shared && allowed.contains(&e.device)),
            "UNAUTHORIZED_EVENT"
        );
        if parts.is_empty() {
            return Ok(completed);
        }
        let authorized: BTreeSet<_> = self
            .events()?
            .iter()
            .filter(|e| e.branch.shared)
            .chain(events.iter())
            .flat_map(Event::objects)
            .collect();
        // One availability snapshot per page, not one full CAS scan per object.
        // Part keys are unique; completed objects are registered by the caller.
        let available = self.shared_object_inventory()?;
        for (object, part) in parts {
            valid_object(&object)?;
            ensure!(authorized.contains(&object), "UNAUTHORIZED_OBJECT");
            let lock = locks.map(|locks| locks.for_object(&object));
            let _guard = lock.as_ref().map(|lock| lock.lock().unwrap());
            if available.contains(&object) {
                self.verify_object(&object)?;
                continue;
            }
            ensure!(
                part.total <= MAX_CONTENT && part.hex.len() <= 8 * IO_CHUNK,
                "INVALID_OBJECT_PART"
            );
            if let Ok(size) = self.object_size(&object) {
                ensure!(size == part.total, "OBJECT_SIZE_CHANGED");
            }
            let bytes = hex::decode(&part.hex)?;
            ensure!(
                (!bytes.is_empty() || part.total == 0 && part.offset == 0)
                    && part
                        .offset
                        .checked_add(bytes.len() as u64)
                        .is_some_and(|end| end <= part.total),
                "INVALID_OBJECT_PART"
            );
            let folder = self.root.join("incoming-objects");
            fs::create_dir_all(&folder)?;
            let path = folder.join(&object);
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)?;
            let mut length = file.metadata()?.len();
            if length > part.total {
                file.set_len(0)?;
                file.sync_all()?;
                length = 0;
            }
            // A peer may retain a pre-crash resume cursor. Return the current
            // inventory without accepting a gap; the next bulk page rewinds.
            if part.offset > length {
                continue;
            }
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
                }
            } else {
                file.seek(SeekFrom::Start(length))?;
                file.write_all(&bytes)?;
                file.sync_all()?;
            }
            if file.metadata()?.len() == part.total {
                file.sync_all()?;
                file.rewind()?;
                if stream_hash(&mut file)? != object {
                    drop(file);
                    fs::remove_file(path)?;
                    bail!("CORRUPT_OBJECT");
                }
                drop(file);
                let dest = self.root.join("objects").join(&object);
                if dest.exists() {
                    self.verify_object(&object)?;
                    fs::remove_file(&path)?;
                } else {
                    install_object(&path, &dest)?;
                    #[cfg(unix)]
                    fs::File::open(&folder)?.sync_all()?;
                }
                completed.push((object, part.total));
            }
        }
        Ok(completed)
    }
}
