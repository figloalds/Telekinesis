//! Mutable host files. The byte path never opens SQLite or computes a digest.
use super::local::{LocalContent, Pin};
use crate::model::content::{CHUNK_SIZE, FileContentRef};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Seek, SeekFrom, Write},
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
    pub file: File,
    pub length: u64,
    pub dirty: DirtyBitmap,
    pub changed: bool,
    /// A native data cache still owns bytes that have not reached this file.
    pub writers: usize,
    pub handles: usize,
    pub closed_at: Option<Instant>,
    pub idle_at: Instant,
    pub hydrated: BTreeSet<u64>,
    pub base_visible_len: u64,
    pub pin: Pin,
}
impl Scratch {
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
        let file_len = self.len()?;
        for index in offset / CHUNK_SIZE..=(offset + len - 1) / CHUNK_SIZE {
            if self.hydrated.contains(&index) {
                continue;
            }
            let start = index * CHUNK_SIZE;
            if overwrite && offset <= start && offset + len >= (start + CHUNK_SIZE).min(file_len) {
                self.hydrated.insert(index);
                if start < self.base_visible_len {
                    self.persist_hydration()?;
                }
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
                self.file.seek(SeekFrom::Start(start))?;
                self.file.write_all(&bytes)?;
            }
            self.hydrated.insert(index);
            if start < self.base_visible_len {
                self.persist_hydration()?;
            }
        }
        Ok(())
    }
    fn persist_hydration(&mut self) -> Result<()> {
        // Only demand hydration needs a recovery marker. Ordinary local writes
        // have no sidecar writes, hashing, or database work.
        self.file.sync_all()?;
        self.record.hydrated = self.hydrated.clone();
        self.write_record(true)
    }
    pub fn read(&mut self, content: &LocalContent, offset: u64, out: &mut [u8]) -> Result<usize> {
        let len = self.len()?.saturating_sub(offset).min(out.len() as u64) as usize;
        self.hydrate(content, offset, len as u64, false)?;
        super::local::read_exact_at(&self.file, &mut out[..len], offset)?;
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
        self.hydrate(content, offset, count as u64, true)?;
        write_at(&self.file, &bytes[..count], offset)?;
        self.length = self.length.max(end);
        self.dirty.mark(offset, count as u64);
        self.changed = true;
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
            return Ok(());
        }
        ensure!(
            self.hydrated.len() as u64 >= old.min(self.base_visible_len).div_ceil(CHUNK_SIZE),
            "TRUNCATE_REQUIRES_HYDRATION"
        );
        self.file.set_len(size)?;
        self.length = size;
        self.base_visible_len = self.base_visible_len.min(size);
        self.hydrated.retain(|index| *index * CHUNK_SIZE < size);
        self.dirty.mark(old.min(size), old.abs_diff(size));
        self.changed = true;
        self.closed_at = None;
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
pub struct WorkingLayer {
    root: PathBuf,
    content: LocalContent,
    files: Mutex<ScratchFiles>,
    latest: Mutex<LatestFiles>,
    bindings: Mutex<BoundFiles>,
    pending: Mutex<BTreeMap<String, Weak<Mutex<Scratch>>>>,
}
impl WorkingLayer {
    pub fn new(root: &Path, content: LocalContent) -> Result<Self> {
        let root = root.join("scratch");
        fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            content,
            files: Mutex::new(BTreeMap::new()),
            latest: Mutex::new(BTreeMap::new()),
            bindings: Mutex::new(BTreeMap::new()),
            pending: Mutex::new(BTreeMap::new()),
        })
    }
    pub fn open(&self, basis: Basis, writable: bool) -> Result<Arc<Mutex<Scratch>>> {
        self.open_impl(basis, writable, false)
    }
    pub fn open_bound(&self, basis: Basis, writable: bool) -> Result<Arc<Mutex<Scratch>>> {
        self.open_impl(basis, writable, true)
    }
    fn open_impl(&self, basis: Basis, writable: bool, bound: bool) -> Result<Arc<Mutex<Scratch>>> {
        let mut files = lock(&self.files);
        let key = (
            basis.branch.clone(),
            basis.entity.clone(),
            basis.version.clone(),
        );
        let entity_key = (basis.branch.clone(), basis.entity.clone());
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
            scratch.handles += 1;
            scratch.writers += usize::from(writable);
            scratch.closed_at = None;
            lock(&self.bindings).insert(key, Arc::downgrade(&existing));
            return Ok(existing.clone());
        }
        let operation = Uuid::new_v4().to_string();
        let path = self.root.join(format!("{operation}.scratch"));
        let pin = self.content.pin(&basis.content)?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        file.set_len(basis.content.total_len)?;
        let mut hydrated = BTreeSet::new();
        for (index, digest) in self.content.chunks(&basis.content)?.into_iter().enumerate() {
            if let Ok(bytes) = self.content.packs.get(digest) {
                file.seek(SeekFrom::Start(index as u64 * CHUNK_SIZE))?;
                file.write_all(&bytes)?;
                hydrated.insert(index as u64);
            }
        }
        let scratch = Scratch {
            record: ScratchRecord {
                id: operation.clone(),
                operation,
                basis: basis.clone(),
                modified_ns: modified_ns(&file)?,
                hydrated: hydrated.clone(),
            },
            path,
            file,
            length: basis.content.total_len,
            dirty: DirtyBitmap::default(),
            changed: false,
            writers: usize::from(writable),
            handles: 1,
            closed_at: None,
            idle_at: Instant::now(),
            hydrated,
            base_visible_len: basis.content.total_len,
            pin,
        };
        scratch.create_record()?;
        let scratch = Arc::new(Mutex::new(scratch));
        lock(&self.bindings).insert(key.clone(), Arc::downgrade(&scratch));
        files.insert(key, scratch.clone());
        lock(&self.latest).insert(entity_key, Arc::downgrade(&scratch));
        Ok(scratch)
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
    pub fn reap_clean(&self) -> Result<()> {
        lock(&self.bindings).retain(|_, file| file.strong_count() != 0);
        lock(&self.latest).retain(|_, file| file.strong_count() != 0);
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
            fs::remove_file(&s.path)?;
            fs::remove_file(s.path.with_extension("json"))?;
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
    }
    pub fn visible_len(&self, branch: &str, entity: &str, anchor: &str) -> Option<u64> {
        let file = lock(&self.latest)
            .get(&(branch.into(), entity.into()))
            .and_then(Weak::upgrade)?;
        let state = lock(&file);
        (state.record.basis.anchor == anchor).then_some(state.length)
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
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos())
}

#[cfg(test)]
mod tests {
    use super::*;
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
