//! Mutable host files. The byte path never opens SQLite or computes a digest.
use super::local::{LocalContent, Pin};
use crate::model::content::{CHUNK_SIZE, FileContentRef};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};
use uuid::Uuid;

pub const DIRTY_PAGE: u64 = 4096;
pub const WRITEBACK_DELAY: Duration = Duration::from_secs(1);
pub const PENDING_BUDGET: u64 = 64 * 1024 * 1024;
pub(crate) fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Basis {
    pub entity: String,
    pub branch: String,
    pub version: String,
    pub anchor: String,
    pub content: FileContentRef,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScratchRecord {
    pub id: String,
    pub operation: String,
    pub basis: Basis,
    pub modified_ns: u128,
    pub hydrated: BTreeSet<u64>,
    /// Immutable ranges remain visible only below this boundary after truncate.
    /// Old sidecars use their original basis length.
    #[serde(default)]
    pub base_visible_len: Option<u64>,
}
#[derive(Default)]
pub struct DirtyBitmap {
    bits: Vec<u64>,
}
impl DirtyBitmap {
    pub fn mark(&mut self, offset: u64, len: u64) {
        if len == 0 {
            return;
        }
        let first = offset / DIRTY_PAGE;
        let last = (offset + len - 1) / DIRTY_PAGE;
        self.bits
            .resize(self.bits.len().max((last / 64 + 1) as usize), 0);
        for page in first..=last {
            self.bits[(page / 64) as usize] |= 1 << (page % 64);
        }
    }
    pub fn intersects(&self, offset: u64, len: u64) -> bool {
        if len == 0 {
            return false;
        }
        (offset / DIRTY_PAGE..=(offset + len - 1) / DIRTY_PAGE).any(|page| {
            self.bits
                .get((page / 64) as usize)
                .is_some_and(|bits| bits & (1 << (page % 64)) != 0)
        })
    }
    pub fn clear(&mut self) {
        self.bits.clear();
    }
}

pub struct Scratch {
    pub record: ScratchRecord,
    pub path: PathBuf,
    pub file: Option<File>,
    pub length: u64,
    pub dirty: DirtyBitmap,
    pub changed: bool,
    pub(crate) revision: u64,
    /// Client-visible write time, fixed at the write/explicit timestamp boundary.
    pub modified_time: Option<u64>,
    pub(crate) explicit_modified_time: bool,
    pub(crate) automatic_time_current: bool,
    /// A native data cache still owns bytes that have not reached this file.
    pub writers: usize,
    pub handles: usize,
    pub closed_at: Option<Instant>,
    pub idle_at: Instant,
    pub hydrated: BTreeSet<u64>,
    /// A failed marker must be retried even if the next operation touches a
    /// different range. Avoid rediscovering this with a whole-draft scan on
    /// every small write to an already fully recorded draft.
    pub(crate) hydration_pending: bool,
    pub base_visible_len: u64,
    pub pin: Pin,
}
impl Scratch {
    pub(crate) fn automatic_modified_time(&mut self) -> u64 {
        let modified = if self.automatic_time_current && !self.explicit_modified_time {
            self.modified_time
                .unwrap_or_else(|| crate::core::Times::default().modified)
        } else {
            crate::core::Times::default().modified
        };
        if self.modified_time != Some(modified) {
            self.revision = self.revision.wrapping_add(1);
        }
        self.modified_time = Some(modified);
        self.explicit_modified_time = false;
        self.automatic_time_current = true;
        modified
    }
    pub fn file(&self) -> Result<&File> {
        self.file.as_ref().context("SCRATCH_NOT_MATERIALIZED")
    }
    pub(crate) fn materialize(&mut self) -> Result<bool> {
        self.materialize_len(self.length)
    }
    fn materialize_len(&mut self, length: u64) -> Result<bool> {
        if self.file.is_some() {
            return Ok(false);
        }
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&self.path)?;
        file.set_len(length)?;
        self.record.modified_ns = modified_ns(&file)?;
        self.create_record()?;
        self.file = Some(file);
        Ok(true)
    }
    pub fn len(&self) -> Result<u64> {
        Ok(self.length)
    }
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }
    pub fn persist_record(&self) -> Result<()> {
        self.write_record(false)
    }
    fn create_record(&self) -> Result<()> {
        // No caller can edit this scratch until initialization returns. A
        // partial initial record is quarantined on recovery; later updates
        // still use atomic replacement.
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.path.with_extension("json"))?;
        file.write_all(&serde_json::to_vec(&self.record)?)?;
        Ok(())
    }
    fn write_record(&self, durable: bool) -> Result<()> {
        let path = self.path.with_extension("json");
        let temp = self.path.with_extension("json.tmp");
        let mut file = File::create(&temp)?;
        file.write_all(&serde_json::to_vec(&self.record)?)?;
        if durable {
            file.sync_all()?;
        }
        // Replace atomically on both supported platforms.
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStrExt;
            unsafe extern "system" {
                fn MoveFileExW(old: *const u16, new: *const u16, flags: u32) -> i32;
            }
            let old: Vec<_> = temp.as_os_str().encode_wide().chain(Some(0)).collect();
            let new: Vec<_> = path.as_os_str().encode_wide().chain(Some(0)).collect();
            ensure!(
                unsafe { MoveFileExW(old.as_ptr(), new.as_ptr(), 1 | if durable { 8 } else { 0 }) }
                    != 0,
                "SCRATCH_RECORD_REPLACE: {}",
                std::io::Error::last_os_error()
            );
        }
        #[cfg(unix)]
        {
            fs::rename(&temp, &path)?;
            if durable {
                File::open(path.parent().unwrap())?.sync_all()?;
            }
        }
        Ok(())
    }
    /// Fetch only chunks touched by this operation. Entire overwritten chunks
    /// need no base data; partial writes must preserve the rest of their chunk.
    fn hydrate(
        &mut self,
        content: &LocalContent,
        offset: u64,
        len: u64,
        overwrite: bool,
    ) -> Result<()> {
        if len == 0 {
            return Ok(());
        }
        let initial = self.materialize()?;
        let file_len = self.len()?;
        for index in offset / CHUNK_SIZE..=(offset + len - 1) / CHUNK_SIZE {
            if self.hydrated.contains(&index) {
                continue;
            }
            let start = index * CHUNK_SIZE;
            if overwrite && offset <= start && offset + len >= (start + CHUNK_SIZE).min(file_len) {
                self.hydrated.insert(index);
                self.hydration_pending |= start < self.base_visible_len;
                continue;
            }
            let count = self
                .base_visible_len
                .saturating_sub(start)
                .min(CHUNK_SIZE)
                .min(file_len.saturating_sub(start));
            if count > 0 {
                let mut bytes = vec![0; count as usize];
                content.read_range(&self.record.basis.content, start, &mut bytes)?;
                write_at(self.file()?, &bytes, start)?;
            }
            self.hydrated.insert(index);
            self.hydration_pending |= start < self.base_visible_len;
        }
        // Publish every newly materialized base range before accepting edits,
        // including ranges left by a previous failed hydration attempt.
        if self.hydration_pending {
            self.persist_hydration(!initial)?;
        }
        Ok(())
    }
    fn persist_hydration(&mut self, durable: bool) -> Result<()> {
        if durable {
            self.file()?.sync_all()?;
        }
        let previous = std::mem::replace(&mut self.record.hydrated, self.hydrated.clone());
        if let Err(error) = self.write_record(durable) {
            self.record.hydrated = previous;
            return Err(error);
        }
        self.hydration_pending = false;
        Ok(())
    }
    fn persist_visible_boundary(&mut self) -> Result<()> {
        if self.record.base_visible_len == Some(self.base_visible_len) {
            return Ok(());
        }
        // Publish a shrink before any later re-extension can make the old
        // immutable tail visible again after a crash. Keep the old record
        // value on failure so the next extending operation must retry.
        let previous = self.record.base_visible_len;
        self.record.base_visible_len = Some(self.base_visible_len);
        if let Err(error) = self.persist_hydration(true) {
            self.record.base_visible_len = previous;
            return Err(error);
        }
        Ok(())
    }
    pub fn read(&mut self, content: &LocalContent, offset: u64, out: &mut [u8]) -> Result<usize> {
        let len = self.len()?.saturating_sub(offset).min(out.len() as u64) as usize;
        let mut done = 0;
        while done < len {
            let start = offset + done as u64;
            let from_scratch = self.hydrated.contains(&(start / CHUNK_SIZE));
            let limit = offset + len as u64;
            let mut end = ((start / CHUNK_SIZE + 1) * CHUNK_SIZE).min(limit);
            while end < limit && self.hydrated.contains(&(end / CHUNK_SIZE)) == from_scratch {
                end = (end + CHUNK_SIZE).min(limit);
            }
            let count = (end - start) as usize;
            let target = &mut out[done..done + count];
            if from_scratch {
                super::local::read_exact_at(self.file()?, target, start)?;
            } else {
                let base_count = self
                    .base_visible_len
                    .saturating_sub(start)
                    .min(count as u64) as usize;
                if base_count != 0 {
                    content.read_range(
                        &self.record.basis.content,
                        start,
                        &mut target[..base_count],
                    )?;
                }
                target[base_count..].fill(0);
            }
            done += count;
        }
        Ok(len)
    }
    pub fn write(
        &mut self,
        content: &LocalContent,
        offset: u64,
        bytes: &[u8],
        append: bool,
        constrained: bool,
    ) -> Result<usize> {
        let old_len = self.len()?;
        let offset = if append { old_len } else { offset };
        let count = if constrained {
            old_len.saturating_sub(offset).min(bytes.len() as u64) as usize
        } else {
            bytes.len()
        };
        if count == 0 {
            return Ok(0);
        }
        let end = offset.checked_add(count as u64).context("FILE_TOO_LARGE")?;
        ensure!(
            end <= crate::model::content::MAX_FILE_SIZE,
            "FILE_TOO_LARGE"
        );
        if end > old_len {
            self.persist_visible_boundary()?;
        }
        self.hydrate(content, offset, count as u64, true)?;
        write_at(self.file()?, &bytes[..count], offset)?;
        self.length = self.length.max(end);
        self.dirty.mark(offset, count as u64);
        self.changed = true;
        self.revision = self.revision.wrapping_add(1);
        // Write replies populate the kernel's cached FileInfo. Cleanup must
        // retain that same automatic time, including when file fsync sealed
        // the bytes before Cleanup. Explicit SetFileTime still wins over late
        // paging delivery; a new writer starts a new automatic-time boundary.
        if constrained {
            if !self.explicit_modified_time {
                // An active writer can deliver another application write at
                // every paging callback. After its Cleanup, later cache
                // delivery keeps the already published close boundary.
                if self.writers != 0 {
                    self.automatic_time_current = false;
                }
                self.automatic_modified_time();
            }
        } else {
            self.modified_time = Some(crate::core::Times::default().modified);
            self.explicit_modified_time = false;
            self.automatic_time_current = true;
        }
        self.closed_at = if self.writers == 0 {
            Some(Instant::now())
        } else {
            None
        };
        Ok(count)
    }
    pub fn truncate(&mut self, size: u64) -> Result<()> {
        ensure!(
            size <= crate::model::content::MAX_FILE_SIZE,
            "FILE_TOO_LARGE"
        );
        let old = self.len()?;
        if old == size {
            return self.persist_visible_boundary();
        }
        if size > old {
            self.persist_visible_boundary()?;
        }
        // Unhydrated retained bytes still come from the immutable prefix.
        // A first shrink must not allocate/copy the old discarded length.
        self.materialize_len(old.min(size))?;
        self.file()?.set_len(size)?;
        self.length = size;
        self.base_visible_len = self.base_visible_len.min(size);
        self.hydrated.retain(|index| *index * CHUNK_SIZE < size);
        self.dirty.mark(old.min(size), old.abs_diff(size));
        self.changed = true;
        self.revision = self.revision.wrapping_add(1);
        self.modified_time = Some(crate::core::Times::default().modified);
        self.explicit_modified_time = false;
        self.automatic_time_current = true;
        self.closed_at = None;
        self.persist_visible_boundary()
    }
    pub fn set_modified_time(&mut self, time: u64) -> Result<()> {
        const UNIX: u64 = 116_444_736_000_000_000;
        let ticks = time.abs_diff(UNIX);
        let delta = Duration::new(ticks / 10_000_000, ((ticks % 10_000_000) * 100) as u32);
        let time_on_host = if time >= UNIX {
            std::time::UNIX_EPOCH.checked_add(delta)
        } else {
            std::time::UNIX_EPOCH.checked_sub(delta)
        }
        .context("INVALID_MODIFIED_TIME")?;
        if let Some(file) = &self.file {
            file.set_times(std::fs::FileTimes::new().set_modified(time_on_host))?;
        }
        if self.modified_time != Some(time) {
            self.revision = self.revision.wrapping_add(1);
        }
        self.modified_time = Some(time);
        self.explicit_modified_time = true;
        self.automatic_time_current = false;
        Ok(())
    }
    pub fn hydrate_chunk(&mut self, content: &LocalContent, index: u64) -> Result<()> {
        self.hydrate(
            content,
            index * CHUNK_SIZE,
            self.len()?
                .saturating_sub(index * CHUNK_SIZE)
                .min(CHUNK_SIZE),
            false,
        )
    }
}

type ScratchFiles = BTreeMap<(String, String, String), Arc<Mutex<Scratch>>>;
type LatestFiles = BTreeMap<(String, String), Weak<Mutex<Scratch>>>;
type BoundFiles = BTreeMap<(String, String, String), Weak<Mutex<Scratch>>>;
type OpenGates = BTreeMap<(String, String), Weak<Mutex<()>>>;
type EntityFiles = BTreeMap<(String, String), BTreeMap<String, Weak<Mutex<Scratch>>>>;
pub struct WorkingLayer {
    root: PathBuf,
    content: LocalContent,
    files: Mutex<ScratchFiles>,
    opening: Mutex<OpenGates>,
    latest: Mutex<LatestFiles>,
    bindings: Mutex<BoundFiles>,
    entities: Mutex<EntityFiles>,
    pending: Mutex<BTreeMap<String, Weak<Mutex<Scratch>>>>,
    sealing: Mutex<()>,
}
impl WorkingLayer {
    pub fn new(root: &Path, content: LocalContent) -> Result<Self> {
        let root = root.join("scratch");
        fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            content,
            files: Mutex::new(BTreeMap::new()),
            opening: Mutex::new(BTreeMap::new()),
            latest: Mutex::new(BTreeMap::new()),
            bindings: Mutex::new(BTreeMap::new()),
            entities: Mutex::new(BTreeMap::new()),
            pending: Mutex::new(BTreeMap::new()),
            sealing: Mutex::new(()),
        })
    }
    pub(crate) fn seal_guard(&self) -> std::sync::MutexGuard<'_, ()> {
        lock(&self.sealing)
    }
    pub fn open(&self, basis: Basis, writable: bool) -> Result<Arc<Mutex<Scratch>>> {
        self.open_impl(basis, writable, false, true)?
            .context("SCRATCH_NOT_CREATED")
    }
    pub fn open_bound(&self, basis: Basis, writable: bool) -> Result<Arc<Mutex<Scratch>>> {
        self.open_impl(basis, writable, true, true)?
            .context("SCRATCH_NOT_CREATED")
    }
    /// Attach an existing draft for an attribute-only native open, without
    /// materializing bytes or fetching a missing manifest just to stat a file.
    pub fn existing(&self, basis: Basis) -> Result<Option<Arc<Mutex<Scratch>>>> {
        self.open_impl(basis, false, false, false)
    }
    fn open_impl(
        &self,
        basis: Basis,
        writable: bool,
        bound: bool,
        create_missing: bool,
    ) -> Result<Option<Arc<Mutex<Scratch>>>> {
        let key = (
            basis.branch.clone(),
            basis.entity.clone(),
            basis.version.clone(),
        );
        let entity_key = (basis.branch.clone(), basis.entity.clone());
        // Serialize initialization only for this entity. Manifest hydration and
        // host file creation must not hold the index used by every other file.
        let opening = {
            let mut gates = lock(&self.opening);
            if let Some(gate) = gates.get(&entity_key).and_then(Weak::upgrade) {
                gate
            } else {
                let gate = Arc::new(Mutex::new(()));
                gates.insert(entity_key.clone(), Arc::downgrade(&gate));
                gate
            }
        };
        let _opening = lock(&opening);
        let files = lock(&self.files);
        let existing = bound
            .then(|| lock(&self.bindings).get(&key).and_then(Weak::upgrade))
            .flatten()
            .or_else(|| {
                lock(&self.latest)
                    .get(&entity_key)
                    .and_then(Weak::upgrade)
                    .filter(|file| {
                        let s = lock(file);
                        s.record.basis.anchor == basis.anchor
                            && s.record.basis.content == basis.content
                    })
            });
        if let Some(existing) = existing {
            let mut scratch = lock(&existing);
            if writable && scratch.writers == 0 {
                scratch.explicit_modified_time = false;
                scratch.automatic_time_current = false;
            }
            scratch.handles += 1;
            scratch.writers += usize::from(writable);
            scratch.closed_at = None;
            lock(&self.bindings).insert(key, Arc::downgrade(&existing));
            return Ok(Some(existing.clone()));
        }
        drop(files);
        if !create_missing {
            return Ok(None);
        }
        let operation = Uuid::new_v4().to_string();
        let path = self.root.join(format!("{operation}.scratch"));
        let pin = self.content.pin(&basis.content)?;
        let hydrated = BTreeSet::new();
        let scratch = Scratch {
            record: ScratchRecord {
                id: operation.clone(),
                operation,
                basis: basis.clone(),
                modified_ns: 0,
                hydrated: hydrated.clone(),
                base_visible_len: Some(basis.content.total_len),
            },
            path,
            file: None,
            length: basis.content.total_len,
            dirty: DirtyBitmap::default(),
            changed: false,
            revision: 0,
            modified_time: None,
            explicit_modified_time: false,
            automatic_time_current: false,
            writers: usize::from(writable),
            handles: 1,
            closed_at: None,
            idle_at: Instant::now(),
            hydrated,
            hydration_pending: false,
            base_visible_len: basis.content.total_len,
            pin,
        };
        let scratch = Arc::new(Mutex::new(scratch));
        lock(&self.bindings).insert(key.clone(), Arc::downgrade(&scratch));
        lock(&self.files).insert(key, scratch.clone());
        lock(&self.latest).insert(entity_key, Arc::downgrade(&scratch));
        self.index_entity(&scratch);
        Ok(Some(scratch))
    }
    pub fn close(&self, scratch: &Arc<Mutex<Scratch>>, writable: bool) {
        self.cleanup(scratch, writable);
        self.release(scratch);
    }
    pub fn cleanup(&self, scratch: &Arc<Mutex<Scratch>>, writable: bool) {
        {
            let mut state = lock(scratch);
            if writable {
                state.writers = state.writers.saturating_sub(1);
            }
            if state.writers == 0 && state.changed {
                state.closed_at = Some(Instant::now());
            }
        }
        self.schedule(scratch);
    }
    pub fn schedule(&self, scratch: &Arc<Mutex<Scratch>>) {
        let id = {
            let state = lock(scratch);
            if state.writers != 0 || !state.changed {
                return;
            }
            state.record.id.clone()
        };
        lock(&self.pending).insert(id, Arc::downgrade(scratch));
    }
    pub fn release(&self, scratch: &Arc<Mutex<Scratch>>) {
        let mut scratch = lock(scratch);
        scratch.handles = scratch.handles.saturating_sub(1);
        scratch.idle_at = Instant::now();
    }
    pub fn pending(&self, force: bool) -> Vec<Arc<Mutex<Scratch>>> {
        if force {
            return lock(&self.files)
                .values()
                .filter(|f| lock(f).changed)
                .cloned()
                .collect();
        }
        let mut pending = lock(&self.pending);
        pending.retain(|_, weak| {
            weak.upgrade().is_some_and(|file| {
                let s = lock(&file);
                s.changed && s.writers == 0
            })
        });
        let files: Vec<_> = pending.values().filter_map(Weak::upgrade).collect();
        let bytes: u64 = files.iter().map(|file| lock(file).length).sum();
        files
            .into_iter()
            .filter(|file| {
                let s = lock(file);
                bytes > PENDING_BUDGET
                    || s.closed_at.is_some_and(|t| t.elapsed() >= WRITEBACK_DELAY)
            })
            .collect()
    }
    /// Every changed scratch, open writers included. Used after a native
    /// volume flush issued by another process delivered their cached pages.
    pub fn materialized_dirty(&self) -> Vec<Arc<Mutex<Scratch>>> {
        lock(&self.files)
            .values()
            .filter(|f| lock(f).changed)
            .cloned()
            .collect()
    }
    /// A file fsync must visit all drafts of that entity, including old local
    /// versions, without locking every unrelated file in a large working tree.
    pub fn dirty_for_entity(&self, branch: &str, entity: &str) -> Vec<Arc<Mutex<Scratch>>> {
        let candidates: Vec<_> = lock(&self.entities)
            .get(&(branch.into(), entity.into()))
            .into_iter()
            .flat_map(|files| files.values())
            .filter_map(Weak::upgrade)
            .collect();
        candidates
            .into_iter()
            .filter(|file| {
                let scratch = lock(file);
                // Atomic replacement can move a draft to another entity.
                scratch.changed
                    && scratch.record.basis.branch == branch
                    && scratch.record.basis.entity == entity
            })
            .collect()
    }
    pub fn reap_clean(&self) -> Result<()> {
        lock(&self.opening).retain(|_, gate| gate.strong_count() != 0);
        lock(&self.bindings).retain(|_, file| file.strong_count() != 0);
        lock(&self.latest).retain(|_, file| file.strong_count() != 0);
        lock(&self.entities).retain(|_, files| {
            files.retain(|_, file| file.strong_count() != 0);
            !files.is_empty()
        });
        let mut files = lock(&self.files);
        let pressure = files.len() > 4096;
        let keys: Vec<_> = files
            .iter()
            .filter(|(_, file)| {
                let s = lock(file);
                s.handles == 0
                    && !s.changed
                    && (pressure || s.idle_at.elapsed() >= Duration::from_secs(30))
            })
            .map(|(k, _)| k.clone())
            .take(256)
            .collect();
        let mut retired = Vec::new();
        for key in keys {
            retired.push(files.remove(&key).unwrap());
        }
        drop(files);
        for scratch in retired {
            let s = lock(&scratch);
            if s.file.is_some() {
                fs::remove_file(&s.path)?;
                fs::remove_file(s.path.with_extension("json"))?;
            }
        }
        Ok(())
    }
    pub fn records(&self) -> Result<Vec<(PathBuf, ScratchRecord)>> {
        let mut records = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "json") {
                let record: ScratchRecord = match serde_json::from_slice(&fs::read(&path)?) {
                    Ok(record) => record,
                    Err(_) => {
                        self.quarantine(&path.with_extension("scratch"))?;
                        continue;
                    }
                };
                ensure!(
                    Uuid::parse_str(&record.operation).is_ok()
                        && Uuid::parse_str(&record.id).is_ok()
                        && path.file_stem().and_then(|s| s.to_str()) == Some(record.id.as_str()),
                    "INVALID_SCRATCH_RECORD"
                );
                records.push((path.with_extension("scratch"), record));
            }
        }
        Ok(records)
    }
    pub(crate) fn quarantine(&self, scratch: &Path) -> Result<()> {
        let destination = self.root.join("quarantine");
        fs::create_dir_all(&destination)?;
        for path in [scratch.to_owned(), scratch.with_extension("json")] {
            if path.exists() {
                let name = path.file_name().context("INVALID_SCRATCH_PATH")?;
                fs::rename(&path, destination.join(name))?;
            }
        }
        Ok(())
    }
    pub fn content(&self) -> &LocalContent {
        &self.content
    }
    pub fn track(&self, scratch: &Arc<Mutex<Scratch>>) {
        let basis = lock(scratch).record.basis.clone();
        lock(&self.latest).insert((basis.branch, basis.entity), Arc::downgrade(scratch));
        self.index_entity(scratch);
    }
    fn index_entity(&self, scratch: &Arc<Mutex<Scratch>>) {
        let (key, id) = {
            let state = lock(scratch);
            (
                (
                    state.record.basis.branch.clone(),
                    state.record.basis.entity.clone(),
                ),
                state.record.id.clone(),
            )
        };
        lock(&self.entities)
            .entry(key)
            .or_default()
            .insert(id, Arc::downgrade(scratch));
    }
    pub fn latest_file(&self, branch: &str, entity: &str) -> Option<Arc<Mutex<Scratch>>> {
        lock(&self.latest)
            .get(&(branch.into(), entity.into()))
            .and_then(Weak::upgrade)
    }
    pub fn visible_info(
        &self,
        branch: &str,
        entity: &str,
        anchor: &str,
    ) -> Option<(u64, Option<u64>)> {
        let file = self.latest_file(branch, entity)?;
        let state = lock(&file);
        (state.record.basis.anchor == anchor).then_some((state.length, state.modified_time))
    }
    pub fn bound_len(&self, basis: &Basis) -> Option<u64> {
        let key = (
            basis.branch.clone(),
            basis.entity.clone(),
            basis.version.clone(),
        );
        let file = lock(&self.bindings).get(&key).and_then(Weak::upgrade)?;
        Some(lock(&file).length)
    }
}
fn write_at(file: &File, mut bytes: &[u8], mut offset: u64) -> std::io::Result<()> {
    while !bytes.is_empty() {
        #[cfg(windows)]
        let count = {
            use std::os::windows::fs::FileExt;
            file.seek_write(bytes, offset)?
        };
        #[cfg(unix)]
        let count = {
            use std::os::unix::fs::FileExt;
            file.write_at(bytes, offset)?
        };
        if count == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        bytes = &bytes[count..];
        offset += count as u64;
    }
    Ok(())
}
pub fn modified_ns(file: &File) -> Result<u128> {
    Ok(file
        .metadata()?
        .modified()?
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos())
}
pub fn file_write_time(file: &File) -> Result<u64> {
    const UNIX: u64 = 116_444_736_000_000_000;
    match file
        .metadata()?
        .modified()?
        .duration_since(std::time::UNIX_EPOCH)
    {
        Ok(delta) => UNIX.checked_add(u64::try_from(delta.as_nanos() / 100)?),
        Err(before) => UNIX.checked_sub(u64::try_from(before.duration().as_nanos() / 100)?),
    }
    .context("INVALID_MODIFIED_TIME")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    #[ignore = "opt-in backing-file write scaling diagnostic; no filesystem mount"]
    fn fully_hydrated_random_write_cost() -> Result<()> {
        use crate::content::local::{Kind, Manifest};
        use crate::model::content::ContentKind;
        for chunks in [64, 16_384] {
            let root = tempfile::tempdir()?;
            let content = LocalContent::open(root.path())?;
            let zero = content
                .packs
                .put(Kind::Chunk, &vec![0; CHUNK_SIZE as usize])?;
            let len = chunks * CHUNK_SIZE;
            let manifest = Manifest {
                len,
                chunks: vec![zero; chunks as usize],
            };
            let digest = content.packs.put(Kind::Manifest, &manifest.encode()?)?;
            let basis = Basis {
                entity: "write-scaling".into(),
                branch: "main".into(),
                version: "v1".into(),
                anchor: "v1".into(),
                content: FileContentRef::new(ContentKind::Manifest, digest, len)?,
            };
            let layer = WorkingLayer::new(root.path(), content.clone())?;
            let file = layer.open(basis, true)?;
            let mut scratch = lock(&file);
            // A fully materialized zero-filled draft with a matching durable
            // marker, equivalent to an earlier completed full-file write/seal.
            scratch.materialize()?;
            write_at(scratch.file()?, &[0], len - 1)?;
            scratch.hydrated = (0..chunks).collect();
            scratch.persist_hydration(true)?;
            let payload = [43; DIRTY_PAGE as usize];
            let mut random = 7_u64;
            let started = Instant::now();
            let mut last = 0;
            for _ in 0..10_000 {
                random = random
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                last = (random % (len / DIRTY_PAGE)) * DIRTY_PAGE;
                scratch.write(&content, last, &payload, false, false)?;
            }
            let elapsed = started.elapsed();
            let mut actual = [0; DIRTY_PAGE as usize];
            scratch.read(&content, last, &mut actual)?;
            assert_eq!(actual, payload);
            eprintln!(
                "TKFS_HYDRATION_BENCH {}",
                serde_json::json!({
                    "bytes":len,"chunks":chunks,"writes":10000,"ms":elapsed.as_secs_f64()*1000.0
                })
            );
        }
        Ok(())
    }

    #[test]
    fn failed_recovery_record_writes_do_not_authorize_untracked_edits() -> Result<()> {
        let root = tempfile::tempdir()?;
        let content = LocalContent::open(root.path())?;
        let basis = Basis {
            entity: "failed-record".into(),
            branch: "main".into(),
            version: "v1".into(),
            anchor: "v1".into(),
            content: content.import(b"base bytes")?,
        };
        let layer = WorkingLayer::new(root.path(), content.clone())?;
        let file = layer.open(basis, true)?;
        let mut scratch = lock(&file);
        let sidecar = scratch.path.with_extension("json");
        fs::create_dir(&sidecar)?;
        assert!(scratch.write(&content, 0, b"edit", false, false).is_err());
        assert!(scratch.file.is_none());
        assert!(scratch.write(&content, 0, b"edit", false, false).is_err());
        assert!(!scratch.changed);
        // Retain this failed initialization; exercise marker replacement with
        // a separate draft so retry never bypasses an incomplete initial record.
        drop(scratch);
        let mut other_basis = lock(&file).record.basis.clone();
        other_basis.entity = "failed-hydration".into();
        let other = layer.open(other_basis, true)?;
        let mut scratch = lock(&other);
        let temporary = scratch.path.with_extension("json.tmp");
        fs::create_dir(&temporary)?;
        assert!(scratch.write(&content, 0, b"edit", false, false).is_err());
        assert!(!scratch.changed);
        assert!(scratch.record.hydrated.is_empty());
        assert!(scratch.hydrated.contains(&0));
        fs::remove_dir(&temporary)?;
        scratch.write(&content, 0, b"edit", false, false)?;
        let saved: ScratchRecord =
            serde_json::from_slice(&fs::read(scratch.path.with_extension("json"))?)?;
        assert!(saved.hydrated.contains(&0));
        let mut bytes = [0; 10];
        scratch.read(&content, 0, &mut bytes)?;
        assert_eq!(&bytes, b"edit bytes");
        Ok(())
    }

    #[test]
    fn failed_hydration_marker_blocks_unrelated_range_edits_until_recorded() -> Result<()> {
        let root = tempfile::tempdir()?;
        let content = LocalContent::open(root.path())?;
        let base = vec![37; 2 * CHUNK_SIZE as usize];
        let layer = WorkingLayer::new(root.path(), content.clone())?;
        let file = layer.open(
            Basis {
                entity: "marker-retry-range".into(),
                branch: "main".into(),
                version: "v1".into(),
                anchor: "v1".into(),
                content: content.import(&base)?,
            },
            true,
        )?;
        let mut scratch = lock(&file);
        let temporary = scratch.path.with_extension("json.tmp");
        fs::create_dir(&temporary)?;
        assert!(
            scratch
                .write(&content, 0, b"blocked", false, false)
                .is_err()
        );
        // Even an operation outside the immutable base must not bypass the
        // failed marker from an earlier range. No edit has been accepted yet.
        assert!(
            scratch
                .write(&content, 2 * CHUNK_SIZE, b"later", false, false)
                .is_err()
        );
        assert!(!scratch.changed);
        assert!(scratch.record.hydrated.is_empty());
        fs::remove_dir(&temporary)?;
        scratch.write(&content, 2 * CHUNK_SIZE, b"later", false, false)?;
        let record: ScratchRecord =
            serde_json::from_slice(&fs::read(scratch.path.with_extension("json"))?)?;
        assert!(record.hydrated.contains(&0));
        let mut actual = vec![0; base.len() + 5];
        scratch.read(&content, 0, &mut actual)?;
        assert_eq!(&actual[..base.len()], &base);
        assert_eq!(&actual[base.len()..], b"later");
        Ok(())
    }

    #[test]
    fn opens_and_reads_do_not_materialize_immutable_content() -> Result<()> {
        let root = tempfile::tempdir()?;
        let content = LocalContent::open(root.path())?;
        let bytes = vec![37; (CHUNK_SIZE * 2 + 11) as usize];
        let basis = Basis {
            entity: "lazy".into(),
            branch: "main".into(),
            version: "v1".into(),
            anchor: "v1".into(),
            content: content.import(&bytes)?,
        };
        let layer = WorkingLayer::new(root.path(), content.clone())?;
        let reader = layer.open(basis.clone(), false)?;
        let writer = layer.open(basis, true)?;
        assert!(Arc::ptr_eq(&reader, &writer));
        assert!(!lock(&reader).path.exists(), "open created a host scratch");
        let mut out = vec![0; bytes.len()];
        lock(&reader).read(&content, 0, &mut out)?;
        assert_eq!(out, bytes);
        assert!(!lock(&reader).path.exists(), "read copied immutable bytes");
        assert!(layer.records()?.is_empty());
        // The cache can write through the reader first. Both handles must see
        // the same overlay, including untouched chunks and partial boundaries.
        lock(&reader).write(&content, CHUNK_SIZE - 2, b"paging", false, true)?;
        let mut expected = bytes;
        expected[(CHUNK_SIZE - 2) as usize..(CHUNK_SIZE + 4) as usize].copy_from_slice(b"paging");
        lock(&writer).read(&content, 0, &mut out)?;
        assert_eq!(out, expected);
        assert_eq!(layer.records()?.len(), 1);
        assert!(!lock(&reader).hydrated.contains(&2));
        layer.close(&reader, false);
        layer.close(&writer, true);
        Ok(())
    }

    #[test]
    fn concurrent_opens_initialize_one_shared_draft() -> Result<()> {
        let root = tempfile::tempdir()?;
        let content = LocalContent::open(root.path())?;
        let basis = Basis {
            entity: "shared".into(),
            branch: "main".into(),
            version: "v1".into(),
            anchor: "v1".into(),
            content: content.import(b"shared bytes")?,
        };
        let layer = Arc::new(WorkingLayer::new(root.path(), content)?);
        let ready = Arc::new(std::sync::Barrier::new(8));
        let workers: Vec<_> = (0..8)
            .map(|index| {
                let layer = layer.clone();
                let basis = basis.clone();
                let ready = ready.clone();
                std::thread::spawn(move || {
                    ready.wait();
                    if index % 2 == 0 {
                        layer.open(basis, true)
                    } else {
                        layer.open_bound(basis, true)
                    }
                })
            })
            .collect();
        let files: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Result<_>>()?;
        assert!(files.iter().all(|file| Arc::ptr_eq(file, &files[0])));
        assert_eq!(lock(&files[0]).handles, 8);
        assert_eq!(lock(&files[0]).writers, 8);
        assert!(layer.records()?.is_empty());
        for file in files {
            layer.close(&file, true);
        }
        layer.reap_clean()?;
        assert!(lock(&layer.opening).is_empty());
        Ok(())
    }

    #[test]
    fn a_remote_open_does_not_hold_the_scratch_index_while_fetching() -> Result<()> {
        struct WaitingFetcher {
            entered: std::sync::mpsc::SyncSender<()>,
            resume: Mutex<std::sync::mpsc::Receiver<()>>,
        }
        impl super::super::local::ObjectFetcher for WaitingFetcher {
            fn fetch(
                &self,
                _objects: &[(crate::model::content::Digest, super::super::local::Kind)],
            ) -> Result<()> {
                self.entered.send(())?;
                lock(&self.resume).recv_timeout(Duration::from_secs(5))?;
                anyhow::bail!("TEST_REMOTE_UNAVAILABLE")
            }
        }
        let root = tempfile::tempdir()?;
        let content = LocalContent::open(root.path())?;
        let local = Basis {
            entity: "local".into(),
            branch: "main".into(),
            version: "v1".into(),
            anchor: "v1".into(),
            content: content.import(b"local")?,
        };
        let mut remote = local.clone();
        remote.entity = "remote".into();
        remote.content = FileContentRef::new(
            crate::model::content::ContentKind::Manifest,
            crate::model::content::Digest::bytes(b"missing manifest"),
            CHUNK_SIZE + 1,
        )?;
        let (entered, waiting) = std::sync::mpsc::sync_channel(1);
        let (resume, resumed) = std::sync::mpsc::channel();
        content.set_fetcher(Arc::new(WaitingFetcher {
            entered,
            resume: Mutex::new(resumed),
        }));
        let layer = Arc::new(WorkingLayer::new(root.path(), content)?);
        let slow_layer = layer.clone();
        let slow = std::thread::spawn(move || slow_layer.open(remote, false));
        let fetch_started = waiting.recv_timeout(Duration::from_secs(2));
        let (send, receive) = std::sync::mpsc::channel();
        let fast_layer = layer.clone();
        let fast = std::thread::spawn(move || {
            send.send(fast_layer.open(local, false)).unwrap();
        });
        let ready = receive.recv_timeout(Duration::from_secs(1));
        resume.send(())?;
        let remote_failed = slow.join().unwrap().is_err();
        fast.join().unwrap();
        fetch_started?;
        assert!(remote_failed);
        assert!(
            ready.is_ok(),
            "unrelated local open waited for remote hydration"
        );
        ready.unwrap()?;
        Ok(())
    }

    #[test]
    fn entity_flush_finds_old_and_retargeted_drafts_without_locking_other_entities() -> Result<()> {
        let root = tempfile::tempdir()?;
        let content = LocalContent::open(root.path())?;
        let basis = Basis {
            entity: "target".into(),
            branch: "main".into(),
            version: "v1".into(),
            anchor: "v1".into(),
            content: content.import(b"base")?,
        };
        let layer = Arc::new(WorkingLayer::new(root.path(), content.clone())?);
        let old = layer.open(basis.clone(), true)?;
        lock(&old).write(&content, 0, b"mine", false, false)?;
        let mut next = basis.clone();
        next.version = "v2".into();
        next.anchor = "v2".into();
        next.content = content.import(b"peer")?;
        let current = layer.open(next, true)?;
        lock(&current).write(&content, 0, b"next", false, false)?;
        let mut other = basis;
        other.entity = "unrelated".into();
        let unrelated = layer.open(other, true)?;
        let held = lock(&unrelated);
        let (send, receive) = std::sync::mpsc::channel();
        let worker_layer = layer.clone();
        let worker = std::thread::spawn(move || {
            send.send(worker_layer.dirty_for_entity("main", "target"))
                .unwrap();
        });
        let result = receive.recv_timeout(Duration::from_secs(2));
        // Always release and join, including when a regression scans and blocks.
        drop(held);
        worker.join().unwrap();
        let files = result?;
        assert_eq!(files.len(), 2);
        assert!(files.iter().any(|f| Arc::ptr_eq(f, &old)));
        assert!(files.iter().any(|f| Arc::ptr_eq(f, &current)));

        lock(&current).record.basis.entity = "replacement".into();
        layer.track(&current);
        let files = layer.dirty_for_entity("main", "target");
        assert_eq!(files.len(), 1);
        assert!(Arc::ptr_eq(&files[0], &old));
        let files = layer.dirty_for_entity("main", "replacement");
        assert_eq!(files.len(), 1);
        assert!(Arc::ptr_eq(&files[0], &current));
        assert!(
            layer
                .dirty_for_entity("other-branch", "replacement")
                .is_empty()
        );
        Ok(())
    }
    #[test]
    fn scratch_passthrough_append_truncate_and_version_isolation() -> Result<()> {
        let root = tempfile::tempdir()?;
        let content = LocalContent::open(root.path())?;
        let reference = content.import(b"abcdef")?;
        let layer = WorkingLayer::new(root.path(), content.clone())?;
        let basis = Basis {
            entity: "file".into(),
            branch: "main".into(),
            version: "v1".into(),
            anchor: "v1".into(),
            content: reference,
        };
        let first = layer.open(basis.clone(), true)?;
        {
            let mut s = lock(&first);
            s.write(&content, 2, b"XY", false, false)?;
            s.write(&content, 0, b"!", true, false)?;
        }
        let second = layer.open(basis.clone(), false)?;
        assert!(Arc::ptr_eq(&first, &second));
        let mut new_basis = basis;
        new_basis.version = "v2".into();
        new_basis.content = content.import(b"remote")?;
        let remote = layer.open(new_basis, false)?;
        let mut bytes = [0; 7];
        assert_eq!(lock(&first).read(&content, 0, &mut bytes)?, 7);
        assert_eq!(&bytes, b"abXYef!");
        let mut other = [0; 6];
        lock(&remote).read(&content, 0, &mut other)?;
        assert_eq!(&other, b"remote");
        lock(&first).truncate(2)?;
        lock(&first).truncate(7)?;
        lock(&first).read(&content, 0, &mut bytes)?;
        assert_eq!(&bytes, b"ab\0\0\0\0\0");
        layer.close(&first, true);
        layer.close(&second, false);
        assert_eq!(layer.pending(true).len(), 1);
        Ok(())
    }
}
