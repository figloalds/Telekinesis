//! Filesystem operations share metadata only at namespace/save boundaries.
use crate::{
    content::{seal, working::*},
    core::*,
};
use anyhow::{Context, Result, ensure};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fs::File,
    path::Path,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

type Invalidate = Arc<dyn Fn(&str, u32) -> Result<()> + Send + Sync>;
static OPEN_COUNTS: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];
static OPEN_NANOS: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];
static NAMESPACE_HITS: AtomicU64 = AtomicU64::new(0);
static NAMESPACE_MISSES: AtomicU64 = AtomicU64::new(0);
const NAMESPACE_CACHE_LIMIT: usize = 65_536;
const NAMESPACE_CACHE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone)]
struct NamespaceEntry {
    epoch: u64,
    entry: Entry,
    branch: String,
    version: Option<String>,
    anchor: Option<String>,
}
impl NamespaceEntry {
    fn cache_bytes(&self, path: &str) -> usize {
        // Include both owned pathname keys and the entry's owned strings,
        // plus space for the map/queue keys and their bookkeeping.
        std::mem::size_of::<Self>()
            + 64
            + 2 * path.len()
            + self.entry.id.len()
            + self.entry.parent.len()
            + self.entry.name.len()
            + self.branch.len()
            + self.version.as_ref().map_or(0, String::len)
            + self.anchor.as_ref().map_or(0, String::len)
    }
    fn basis(&self) -> Result<Option<Basis>> {
        if self.entry.kind == EntryKind::Directory {
            return Ok(None);
        }
        Ok(Some(Basis {
            entity: self.entry.id.clone(),
            branch: self.branch.clone(),
            version: self.version.clone().context("VERSION_MISSING")?,
            anchor: self.anchor.clone().context("VERSION_MISSING")?,
            content: self.entry.content.clone().context("IS_A_DIRECTORY")?,
        }))
    }
}
#[derive(Default)]
struct NamespaceCache {
    entries: HashMap<String, NamespaceEntry>,
    order: VecDeque<String>,
    bytes: usize,
}
impl NamespaceCache {
    fn insert(&mut self, path: &str, value: NamespaceEntry) {
        let bytes = value.cache_bytes(path);
        if bytes > NAMESPACE_CACHE_BYTES {
            return;
        }
        if !self.entries.contains_key(path) {
            self.order.push_back(path.into());
        }
        if let Some(previous) = self.entries.insert(path.into(), value) {
            self.bytes -= previous.cache_bytes(path);
        }
        self.bytes += bytes;
        while self.entries.len() > NAMESPACE_CACHE_LIMIT || self.bytes > NAMESPACE_CACHE_BYTES {
            let Some(old) = self.order.pop_front() else {
                break;
            };
            if let Some(previous) = self.entries.remove(&old) {
                self.bytes -= previous.cache_bytes(&old);
            }
        }
    }
}
pub fn namespace_stats() -> serde_json::Value {
    serde_json::json!({"hits":NAMESPACE_HITS.load(Ordering::Relaxed),"misses":NAMESPACE_MISSES.load(Ordering::Relaxed),"limit":NAMESPACE_CACHE_LIMIT,"byte_budget":NAMESPACE_CACHE_BYTES})
}
fn open_timing(stage: usize, started: std::time::Instant) {
    OPEN_COUNTS[stage].fetch_add(1, Ordering::Relaxed);
    OPEN_NANOS[stage].fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
}

pub fn open_stats() -> serde_json::Value {
    serde_json::Value::Array(
        ["metadata_wait", "metadata_total", "scratch", "handle"]
            .into_iter()
            .enumerate()
            .map(|(i, stage)| {
                serde_json::json!({"stage":stage,
            "calls":OPEN_COUNTS[i].load(Ordering::Relaxed),
            "ms":OPEN_NANOS[i].load(Ordering::Relaxed) as f64/1e6})
            })
            .collect(),
    )
}
pub struct Handle {
    pub entry: Mutex<Entry>,
    pub scratch: Option<Arc<Mutex<Scratch>>>,
    pub writable: bool,
    closed: AtomicBool,
    pub delete: AtomicBool,
    pub directory: Mutex<Option<Arc<Vec<Entry>>>>,
}
pub struct Engine {
    pub store: Mutex<Store>,
    pub working: WorkingLayer,
    namespace_epoch: Arc<AtomicU64>,
    namespace_cache: Mutex<NamespaceCache>,
    handles: RwLock<BTreeMap<u64, Arc<Handle>>>,
    next: AtomicU64,
    invalidate: RwLock<Option<Invalidate>>,
    notifications: Mutex<BTreeMap<String, u32>>,
    pub stopping: AtomicBool,
    pub error: Mutex<Option<String>>,
    shutdown_error: Mutex<Option<String>>,
    _owner: File,
    pub last_applied: Mutex<BTreeMap<String, u128>>,
    publication: Mutex<()>,
    faults: Mutex<std::collections::BTreeSet<String>>,
}
impl Engine {
    pub fn open(root: &Path, repo: Option<&str>) -> Result<Arc<Self>> {
        std::fs::create_dir_all(root)?;
        let owner = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("owner.lock"))?;
        #[cfg(windows)]
        owner.try_lock().context("STORE_ALREADY_OPEN")?;
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            ensure!(
                unsafe { libc::flock(owner.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
                "STORE_ALREADY_OPEN: {}",
                std::io::Error::last_os_error()
            );
        }
        let store = Store::open(root, repo)?;
        let working = WorkingLayer::new(root, store.content.clone())?;
        let store = Mutex::new(store);
        seal::recover(&store, &working)?;
        let namespace_epoch = lock(&store).namespace_epoch.clone();
        let engine = Arc::new(Self {
            store,
            working,
            namespace_epoch,
            namespace_cache: Mutex::new(NamespaceCache::default()),
            handles: RwLock::new(BTreeMap::new()),
            next: AtomicU64::new(1),
            invalidate: RwLock::new(None),
            notifications: Mutex::new(BTreeMap::new()),
            stopping: AtomicBool::new(false),
            error: Mutex::new(None),
            shutdown_error: Mutex::new(None),
            _owner: owner,
            last_applied: Mutex::new(BTreeMap::new()),
            publication: Mutex::new(()),
            faults: Mutex::new(Default::default()),
        });
        let weak = Arc::downgrade(&engine);
        std::thread::Builder::new()
            .name("tkfs-sealer".into())
            .spawn(move || {
                let mut next_seal = std::time::Instant::now();
                let mut next_reap = std::time::Instant::now();
                loop {
                    std::thread::sleep(Duration::from_millis(5));
                    let Some(engine) = weak.upgrade() else {
                        break;
                    };
                    if engine.stopping.load(Ordering::Acquire) {
                        break;
                    }
                    if let Err(error) = lock(&engine.store).commit_due() {
                        *lock(&engine.error) = Some(format!("{error:#}"));
                    }
                    if std::time::Instant::now() < next_seal {
                        continue;
                    }
                    next_seal = std::time::Instant::now() + Duration::from_millis(50);
                    if let Err(error) = engine.retry_notifications() {
                        *lock(&engine.error) = Some(format!("{error:#}"));
                    }
                    let files = engine.working.pending(false);
                    let _publication = lock(&engine.publication);
                    let result = seal::seal_batch(&engine.store, &engine.working, &files)
                        .and_then(|stats| engine.queue_sealed(&files, stats.entities));
                    drop(_publication);
                    if let Err(error) = result {
                        *lock(&engine.error) = Some(format!("{error:#}"));
                    }
                    if std::time::Instant::now() >= next_reap {
                        next_reap = std::time::Instant::now() + Duration::from_secs(1);
                        if let Err(error) = engine.working.reap_clean() {
                            *lock(&engine.error) = Some(format!("{error:#}"));
                        }
                    }
                }
            })?;
        Ok(engine)
    }
    pub fn set_invalidator(&self, callback: Invalidate) {
        *self.invalidate.write().unwrap() = Some(callback);
    }
    pub fn invalidate_paths(&self, paths: impl IntoIterator<Item = String>) -> Result<()> {
        lock(&self.notifications).extend(paths.into_iter().map(|path| (path, 3)));
        self.retry_notifications()
    }
    fn retry_notifications(&self) -> Result<()> {
        let Some(callback) = self.invalidate.read().unwrap().clone() else {
            return Ok(());
        };
        let pending = std::mem::take(&mut *lock(&self.notifications));
        let mut result = Ok(());
        for (path, action) in pending {
            if let Err(error) = callback(&path, action) {
                lock(&self.notifications).entry(path).or_insert(action);
                result = Err(error);
            }
        }
        result
    }
    fn namespace_entry(
        &self,
        path: &str,
        timing: Option<std::time::Instant>,
    ) -> Result<NamespaceEntry> {
        let epoch = self.namespace_epoch.load(Ordering::Acquire);
        let cached = lock(&self.namespace_cache)
            .entries
            .get(path)
            .filter(|value| value.epoch == epoch)
            .cloned();
        if let Some(cached) = cached
            && self.namespace_epoch.load(Ordering::Acquire) == epoch
        {
            NAMESPACE_HITS.fetch_add(1, Ordering::Relaxed);
            if let Some(started) = timing {
                open_timing(0, started);
            }
            return Ok(cached);
        }
        NAMESPACE_MISSES.fetch_add(1, Ordering::Relaxed);
        let value = {
            let store = lock(&self.store);
            if let Some(started) = timing {
                open_timing(0, started);
            }
            let entry = store.lookup(path)?;
            NamespaceEntry {
                epoch: store.namespace_epoch.load(Ordering::Acquire),
                branch: store.active.clone(),
                version: store.projection().visible.get(&entry.id).cloned(),
                anchor: store.projection().anchors.get(&entry.id).cloned(),
                entry,
            }
        };
        // Cache only successful lookups and owned entry values, never a clone
        // of the projection maps or a handle into the served namespace. Raw
        // pathname keys can miss on aliases; authoritative lookup validates
        // them before insertion. Epoch tags invalidate every alias on change.
        lock(&self.namespace_cache).insert(path, value.clone());
        Ok(value)
    }
    pub fn lookup(&self, path: &str) -> Result<Entry> {
        let NamespaceEntry {
            mut entry,
            branch,
            anchor,
            ..
        } = self.namespace_entry(path, None)?;
        if let Some(anchor) = anchor
            && let Some((len, modified)) = self.working.visible_info(&branch, &entry.id, &anchor)
        {
            entry.working_len = Some(len);
            if let Some(modified) = modified {
                entry.times.modified = modified;
            }
        }
        Ok(entry)
    }
    pub fn open_file(
        &self,
        path: &str,
        create: bool,
        directory: bool,
        writable: bool,
    ) -> Result<u64> {
        self.open_file_with_attributes(path, create, directory, writable, None)
    }
    pub fn open_file_with_attributes(
        &self,
        path: &str,
        create: bool,
        directory: bool,
        writable: bool,
        attributes: Option<u32>,
    ) -> Result<u64> {
        self.open_native_file(path, create, directory, writable, attributes, false)
    }
    pub fn open_native_file(
        &self,
        path: &str,
        create: bool,
        directory: bool,
        writable: bool,
        attributes: Option<u32>,
        metadata_only: bool,
    ) -> Result<u64> {
        ensure!(!metadata_only || !writable, "INVALID_METADATA_OPEN");
        ensure!(!self.stopping.load(Ordering::Acquire), "STOPPING");
        let started = std::time::Instant::now();
        let (entry, basis) = if !create && !writable {
            let value = self.namespace_entry(path, Some(started))?;
            let basis = value.basis()?;
            (value.entry, basis)
        } else {
            let mut store = lock(&self.store);
            open_timing(0, started);
            let e = if create {
                store.create_with_attributes(
                    path,
                    if directory {
                        EntryKind::Directory
                    } else {
                        EntryKind::File
                    },
                    attributes,
                )?
            } else {
                store.lookup(path)?
            };
            let basis = if e.kind == EntryKind::File {
                Some(store.basis(&e.id)?)
            } else {
                None
            };
            (e, basis)
        };
        ensure!(
            !directory || entry.kind == EntryKind::Directory,
            "NOT_A_DIRECTORY"
        );
        open_timing(1, started);
        self.insert_handle(entry, basis, writable, false, metadata_only)
    }
    pub fn open_bound(&self, entry: Entry, basis: Basis, writable: bool) -> Result<u64> {
        ensure!(!self.stopping.load(Ordering::Acquire), "STOPPING");
        ensure!(
            entry.kind == EntryKind::File
                && entry.id == basis.entity
                && entry.content.as_ref() == Some(&basis.content),
            "INVALID_BOUND_OPEN"
        );
        self.insert_handle(entry, Some(basis), writable, true, false)
    }
    fn insert_handle(
        &self,
        entry: Entry,
        basis: Option<Basis>,
        writable: bool,
        bound: bool,
        metadata_only: bool,
    ) -> Result<u64> {
        let started = std::time::Instant::now();
        let scratch = basis
            .map(|basis| {
                if metadata_only {
                    self.working.existing(basis)
                } else if bound {
                    self.working.open_bound(basis, writable).map(Some)
                } else {
                    self.working.open(basis, writable).map(Some)
                }
            })
            .transpose()?
            .flatten();
        open_timing(2, started);
        let started = std::time::Instant::now();
        let token = self.next.fetch_add(1, Ordering::Relaxed);
        self.handles.write().unwrap().insert(
            token,
            Arc::new(Handle {
                entry: Mutex::new(entry),
                scratch,
                writable,
                closed: AtomicBool::new(false),
                delete: AtomicBool::new(false),
                directory: Mutex::new(None),
            }),
        );
        open_timing(3, started);
        Ok(token)
    }
    pub fn handle(&self, token: u64) -> Result<Arc<Handle>> {
        self.handles
            .read()
            .unwrap()
            .get(&token)
            .cloned()
            .context("INVALID_HANDLE")
    }
    pub fn writable_tokens(&self) -> Vec<u64> {
        self.handles
            .read()
            .unwrap()
            .iter()
            .filter(|(_, h)| h.writable && !h.closed.load(Ordering::Acquire))
            .map(|(token, _)| *token)
            .collect()
    }
    pub fn info(&self, token: u64) -> Result<(Entry, u64)> {
        let h = self.handle(token)?;
        let mut e = lock(&h.entry).clone();
        let len = if let Some(s) = &h.scratch {
            let state = lock(s);
            if let Some(modified) = state.modified_time {
                e.times.modified = modified;
            }
            state.len()?
        } else {
            e.content.as_ref().map_or(0, |content| content.total_len)
        };
        Ok((e, len))
    }
    pub fn read(&self, token: u64, offset: u64, out: &mut [u8]) -> Result<usize> {
        let h = self.handle(token)?;
        let s = h.scratch.as_ref().context("IS_A_DIRECTORY")?;
        lock(s).read(self.working.content(), offset, out)
    }
    /// Windows shares cached pages across opens of a pathname. After notify
    /// purges those pages, paging reads must populate them from the visible head.
    pub fn read_visible(&self, token: u64, offset: u64, out: &mut [u8]) -> Result<usize> {
        let h = self.handle(token)?;
        let original = h.scratch.as_ref();
        let bound = lock(&h.entry).clone();
        let basis = {
            let store = lock(&self.store);
            let current = Self::cached_entry(&store, &bound)?;
            if current.deleted || !store.projection().entries.contains_key(&current.id) {
                None
            } else {
                Some(store.basis(&current.id)?)
            }
        };
        let Some(basis) = basis else {
            return self.read(token, offset, out);
        };
        if let Some(original) = original
            && lock(original).record.basis.anchor == basis.anchor
        {
            return lock(original).read(self.working.content(), offset, out);
        }
        let visible = self.working.open(basis, false)?;
        let result = lock(&visible).read(self.working.content(), offset, out);
        self.working.cleanup(&visible, false);
        self.working.release(&visible);
        result
    }
    pub fn info_visible(&self, token: u64) -> Result<(Entry, u64)> {
        let h = self.handle(token)?;
        let bound = lock(&h.entry).clone();
        let (mut entry, branch, anchor) = {
            let store = lock(&self.store);
            let entry = Self::cached_entry(&store, &bound)?;
            let anchor = store.projection().anchors.get(&entry.id).cloned();
            (entry, store.active.clone(), anchor)
        };
        if entry.deleted || (anchor.is_none() && h.scratch.is_some()) {
            return self.info(token);
        }
        let len = if let Some((len, modified)) =
            anchor.and_then(|anchor| self.working.visible_info(&branch, &entry.id, &anchor))
        {
            if let Some(modified) = modified {
                entry.times.modified = modified;
            }
            len
        } else {
            entry.content.as_ref().map_or(0, |c| c.total_len)
        };
        entry.working_len = Some(len);
        Ok((entry, len))
    }
    fn cached_entry(store: &Store, bound: &Entry) -> Result<Entry> {
        if bound.id == ROOT {
            return Ok(Entry::root());
        }
        // A live entity may have been renamed using another handle. Keep its
        // identity even if the old name is subsequently reused by a new file.
        if let Ok(entry) = store.entry(&bound.id)
            && !entry.deleted
            && store.projection().entries.contains_key(&entry.id)
        {
            return Ok(entry);
        }
        // A Windows cache file object can outlive a remote delete/recreate of
        // this name. Its old user context must refill from the current pathname.
        if let Ok(parent) = store.projection().path(&bound.parent) {
            let path = format!("{parent}/{}", bound.name);
            if let Ok(entry) = store.lookup(&path) {
                return Ok(entry);
            }
        }
        Ok(store.entry(&bound.id).unwrap_or_else(|_| bound.clone()))
    }
    pub fn write(
        &self,
        token: u64,
        offset: u64,
        bytes: &[u8],
        append: bool,
        constrained: bool,
    ) -> Result<usize> {
        let h = self.handle(token)?;
        // Paging (constrained) writes come through whichever file object the
        // Cache Manager initialised caching with, possibly a read-only handle.
        // The kernel checked access at Create; refusing them strands dirty pages.
        ensure!(h.writable || constrained, "ACCESS_DENIED");
        let s = h.scratch.as_ref().context("IS_A_DIRECTORY")?;
        let count = lock(s).write(self.working.content(), offset, bytes, append, constrained)?;
        if h.closed.load(Ordering::Acquire) {
            self.working.schedule(s);
        }
        Ok(count)
    }
    pub fn write_native(
        &self,
        token: u64,
        offset: u64,
        bytes: &[u8],
        append: bool,
        constrained: bool,
    ) -> Result<usize> {
        if !constrained {
            return self.write(token, offset, bytes, append, false);
        }
        let h = self.handle(token)?;
        let original = h.scratch.as_ref();
        let bound = lock(&h.entry).clone();
        let basis = {
            let store = lock(&self.store);
            let current = Self::cached_entry(&store, &bound)?;
            store.basis(&current.id)?
        };
        if let Some(original) = original {
            let scratch = lock(original);
            // Keep a local edit's original parent when a remote content event
            // arrives. A clean old cache reader, however, is no longer the
            // write destination after a pathname's cache has been refilled.
            if scratch.record.basis.entity == basis.entity
                && ((scratch.record.basis.branch == basis.branch
                    && scratch.record.basis.anchor == basis.anchor)
                    || scratch.changed
                    || scratch.writers != 0)
            {
                drop(scratch);
                return self.write(token, offset, bytes, append, true);
            }
        }
        // Windows can retain the old reader's file object across remote
        // delete/recreate. Paging writes must follow the same visible identity
        // as paging reads and FileInfo, not its deleted entity's old scratch.
        let visible = self.working.open(basis, false)?;
        let result = lock(&visible).write(self.working.content(), offset, bytes, append, true);
        self.working.cleanup(&visible, false);
        self.working.release(&visible);
        result
    }
    pub fn truncate(&self, token: u64, size: u64, allocation: bool) -> Result<()> {
        let h = self.handle(token)?;
        ensure!(h.writable, "ACCESS_DENIED");
        let mut s = lock(h.scratch.as_ref().context("IS_A_DIRECTORY")?);
        if allocation && size >= s.len()? {
            return Ok(());
        }
        s.truncate(size)
    }
    pub fn flush(&self, token: Option<u64>) -> Result<()> {
        self.flush_impl(token, false)
    }
    /// The native volume flush has already delivered cached pages to scratch.
    pub fn flush_native_volume(&self) -> Result<()> {
        self.flush_impl(None, true)
    }
    pub fn flush_native_file(&self, token: u64) -> Result<()> {
        self.flush_impl(Some(token), true)
    }
    /// Lifecycle fault seams: `flush` fails once, `flush-persistent` also
    /// fails final retries so VM checks can exercise scratch recovery.
    pub fn inject_fault(&self, name: &str) {
        lock(&self.faults).insert(name.to_owned());
    }
    fn fault(&self, name: &str) -> Result<()> {
        let mut faults = lock(&self.faults);
        ensure!(
            !faults.contains(&format!("{name}-persistent")) && !faults.remove(name),
            "FAULT_INJECTED: {name}"
        );
        Ok(())
    }
    fn flush_impl(&self, token: Option<u64>, native_drained: bool) -> Result<()> {
        self.fault("flush")?;
        let _publication = lock(&self.publication);
        let files = if let Some(token) = token {
            let handle = self.handle(token)?;
            if native_drained {
                // A file Flush can use the same retained cache context as a
                // paging Write. Seal the visible entity's drafts, including a
                // writer whose context differs from this read-only context.
                let bound = lock(&handle.entry).clone();
                let (entity, branch) = {
                    let store = lock(&self.store);
                    (Self::cached_entry(&store, &bound)?.id, store.active.clone())
                };
                let mut files = self.working.dirty_for_entity(&branch, &entity);
                // Preserve the passed handle's private draft as well (for
                // example an edit opened before a branch checkout). Following
                // the cache's current pathname must not weaken file fsync.
                files.extend(handle.scratch.clone());
                files
            } else {
                handle.scratch.clone().into_iter().collect()
            }
        } else if native_drained {
            self.working.materialized_dirty()
        } else {
            self.working.pending(true)
        };
        let stats = seal::seal_batch(&self.store, &self.working, &files)?;
        self.queue_sealed(&files, stats.entities)?;
        // Also covers namespace-only fsyncs with no dirty content event.
        lock(&self.store).sync_metadata()?;
        *lock(&self.error) = None;
        Ok(())
    }
    fn queue_sealed(&self, files: &[Arc<Mutex<Scratch>>], count: usize) -> Result<()> {
        #[cfg(target_os = "linux")]
        if count != 0 {
            let entities: Vec<_> = files.iter().map(|s| lock(s).record.basis.clone()).collect();
            let paths = {
                let store = lock(&self.store);
                entities
                    .iter()
                    .filter(|b| b.branch == store.active)
                    .filter_map(|b| store.projection().path(&b.entity).ok())
                    .collect::<Vec<_>>()
            };
            lock(&self.notifications).extend(paths.into_iter().map(|path| (path, 3)));
        }
        #[cfg(not(target_os = "linux"))]
        let _ = (files, count);
        Ok(())
    }
    pub fn cleanup(&self, token: u64, delete: bool) -> Result<()> {
        let h = self.handle(token)?;
        if delete || h.delete.load(Ordering::Acquire) {
            let entity = lock(&h.entry).id.clone();
            let mut store = lock(&self.store);
            if !store.entry(&entity)?.deleted {
                store.delete(&entity)?;
            }
        }
        if !h.closed.swap(true, Ordering::AcqRel)
            && let Some(s) = &h.scratch
        {
            self.working.cleanup(s, h.writable);
        }
        Ok(())
    }
    pub fn cleanup_native(&self, token: u64, delete: bool, update_write_time: bool) -> Result<()> {
        let timestamp = (|| -> Result<()> {
            if update_write_time {
                let handle = self.handle(token)?;
                let bound = lock(&handle.entry).clone();
                let entry = { Self::cached_entry(&lock(&self.store), &bound)? };
                if entry.kind == EntryKind::File && !entry.deleted {
                    let (branch, anchor) = {
                        let store = lock(&self.store);
                        (
                            store.active.clone(),
                            store.projection().anchors.get(&entry.id).cloned(),
                        )
                    };
                    let draft = self.working.latest_file(&branch, &entry.id);
                    let updated = if let Some(draft) = draft {
                        let mut state = lock(&draft);
                        let mut store = lock(&self.store);
                        if store.active == branch
                            && anchor.as_ref() == Some(&state.record.basis.anchor)
                        {
                            let modified = state.automatic_modified_time();
                            if !state.changed {
                                let entry = store.entry(&entry.id)?;
                                if entry.times.modified != modified {
                                    let mut times = entry.times;
                                    times.modified = modified;
                                    store.set_times(&entry.id, times, None, None)?;
                                }
                            }
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    if !updated {
                        self.set_metadata(
                            &entry.id,
                            [None, None, Some(Times::default().modified), None],
                            None,
                            None,
                        )?;
                    }
                    // Nested paging-write replies do not refresh WinFsp's
                    // cached FileInfo. Cleanup also has no FileInfo response.
                    // Queue notification for the background thread after this
                    // callback releases the kernel's per-file locks.
                    let path = { lock(&self.store).projection().path(&entry.id)? };
                    lock(&self.notifications).insert(path, 3);
                }
            }
            Ok(())
        })();
        // Cleanup has no error reply in the native API. Retire the writer even
        // if its timestamp failed, and retain that failure for final shutdown.
        let cleanup = self.cleanup(token, delete);
        let result = timestamp.and(cleanup);
        if let Err(error) = &result {
            *lock(&self.shutdown_error) = Some(format!("native cleanup: {error:#}"));
        }
        result
    }
    pub fn close(&self, token: u64) -> Result<()> {
        self.cleanup(token, false)?;
        // Scratch release can wait for a batched seal. Do not hold the global
        // handle map across that wait and stall every other file callback.
        let handle = self.handles.write().unwrap().remove(&token);
        if let Some(handle) = handle
            && let Some(scratch) = &handle.scratch
        {
            self.working.release(scratch);
        }
        Ok(())
    }
    pub fn children(&self, token: u64, restart: bool) -> Result<Arc<Vec<Entry>>> {
        let h = self.handle(token)?;
        let mut cache = lock(&h.directory);
        if restart || cache.is_none() {
            let id = lock(&h.entry).id.clone();
            let (mut entries, branch, anchors) = {
                let store = lock(&self.store);
                (
                    store.projection().children(&id),
                    store.active.clone(),
                    store.projection().anchors.clone(),
                )
            };
            for entry in &mut entries {
                if let Some(anchor) = anchors.get(&entry.id)
                    && let Some((len, modified)) =
                        self.working.visible_info(&branch, &entry.id, anchor)
                {
                    entry.working_len = Some(len);
                    if let Some(modified) = modified {
                        entry.times.modified = modified;
                    }
                }
            }
            *cache = Some(Arc::new(entries));
        }
        Ok(cache.as_ref().unwrap().clone())
    }
    pub fn rename(&self, token: u64, target: &str, replace: bool) -> Result<()> {
        let h = self.handle(token)?;
        // A delete/attribute-only native handle may have opened before the
        // writer created a draft. Preserve atomic save folding without making
        // every stat/open materialize a new scratch file.
        let temporary = if replace && h.scratch.is_none() && lock(&h.entry).kind == EntryKind::File
        {
            let basis = { lock(&self.store).basis(&lock(&h.entry).id)? };
            self.working.existing(basis)?
        } else {
            None
        };
        let result = self.rename_with_scratch(
            &h,
            h.scratch.as_ref().or(temporary.as_ref()),
            target,
            replace,
        );
        if let Some(scratch) = temporary {
            self.working.close(&scratch, false);
        }
        result
    }
    fn rename_with_scratch(
        &self,
        h: &Handle,
        scratch: Option<&Arc<Mutex<Scratch>>>,
        target: &str,
        replace: bool,
    ) -> Result<()> {
        let entity = lock(&h.entry).id.clone();
        if replace && let Some(scratch) = scratch {
            let destination = {
                let store = lock(&self.store);
                store
                    .lookup(target)
                    .ok()
                    .filter(|e| e.id != entity && e.kind == EntryKind::File)
            };
            if let Some(destination) = destination
                && lock(scratch).changed
            {
                let _publication = lock(&self.publication);
                seal::replace_file(&self.store, &self.working, scratch, &destination.id)?;
                self.working.track(scratch);
                *lock(&h.entry) = lock(&self.store).entry(&destination.id)?;
                return Ok(());
            }
        }
        let mut store = lock(&self.store);
        store.rename(&entity, target, replace)?;
        *lock(&h.entry) = store.entry(&entity)?;
        Ok(())
    }
    pub fn collect(&self) -> Result<usize> {
        self.flush(None)?;
        let _publication = lock(&self.publication);
        let store = lock(&self.store);
        let live = store.live_objects(32)?;
        store.content.packs.collect(&live, 0.7)
    }
    pub fn basic(&self, token: u64, attributes: u32, times: [u64; 4]) -> Result<()> {
        let h = self.handle(token)?;
        let entity = lock(&h.entry).id.clone();
        let updates = times.map(|time| (time != 0 && time != u64::MAX).then_some(time));
        let entry = self.set_metadata(
            &entity,
            updates,
            None,
            (attributes != u32::MAX).then_some(attributes),
        )?;
        *lock(&h.entry) = entry;
        Ok(())
    }
    /// Apply explicit timestamps at their call boundary, before a later seal.
    /// Lock the draft before metadata, matching the sealer's lock order.
    pub fn set_metadata(
        &self,
        entity: &str,
        updates: [Option<u64>; 4],
        mode: Option<u32>,
        attributes: Option<u32>,
    ) -> Result<Entry> {
        let _sealing = updates[2].is_some().then(|| self.working.seal_guard());
        self.set_metadata_inner(entity, updates, mode, attributes)
    }
    fn set_metadata_inner(
        &self,
        entity: &str,
        updates: [Option<u64>; 4],
        mode: Option<u32>,
        attributes: Option<u32>,
    ) -> Result<Entry> {
        let branch = { lock(&self.store).active.clone() };
        let file = updates[2].and_then(|_| self.working.latest_file(&branch, entity));
        let mut scratch = file.as_ref().map(|file| lock(file));
        let mut store = lock(&self.store);
        let mut e = store.entry(entity)?;
        let original = e.clone();
        for (old, new) in [
            &mut e.times.created,
            &mut e.times.accessed,
            &mut e.times.modified,
            &mut e.times.changed,
        ]
        .into_iter()
        .zip(updates)
        {
            if let Some(new) = new {
                *old = new;
            }
        }
        if let Some(scratch) = scratch.as_mut()
            && store.active == branch
            && store.projection().anchors.get(entity) == Some(&scratch.record.basis.anchor)
            && let Some(modified) = updates[2]
        {
            scratch.set_modified_time(modified)?;
        }
        if original.times == e.times
            && mode.is_none_or(|mode| mode == original.mode)
            && attributes.is_none_or(|attributes| attributes == original.attributes)
        {
            return Ok(e);
        }
        store.set_times(entity, e.times, mode, attributes)?;
        store.entry(entity)
    }
    pub fn apply_remote(&self, events: &[Event]) -> Result<Vec<String>> {
        let mut store = lock(&self.store);
        let mut before = BTreeMap::new();
        for event in events {
            if let Ok(path) = store.projection().path(&event.entity) {
                before.insert(event.entity.clone(), path);
            }
        }
        let applied = store.apply(events)?;
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        for id in &applied {
            if let Some(event) = store.version(id)? {
                lock(&self.last_applied).insert(event.entity.clone(), stamp);
                if event.branch == store.active {
                    let mut notifications = lock(&self.notifications);
                    if let Some(path) = before.get(&event.entity) {
                        notifications.insert(path.clone(), 3);
                    }
                    if let Ok(path) = store.projection().path(&event.entity) {
                        notifications.insert(path, if event.state.deleted { 2 } else { 3 });
                    }
                }
            }
        }
        drop(store);
        if let Err(error) = self.retry_notifications() {
            *lock(&self.error) = Some(format!("{error:#}"));
        }
        Ok(applied)
    }
    /// Publish the stop request only after remembering a failed control flush.
    /// The daemon's final flush may succeed, but cannot erase that failure.
    pub fn request_shutdown(&self) -> Result<()> {
        let result = self.flush(None);
        if let Err(error) = &result {
            let error = format!("{error:#}");
            *lock(&self.shutdown_error) = Some(error.clone());
            *lock(&self.error) = Some(error);
        }
        self.stopping.store(true, Ordering::Release);
        result
    }
    pub fn shutdown(&self) -> Result<()> {
        self.stopping.store(true, Ordering::Release);
        let final_result = self.flush(None).and_then(|()| self.working.reap_clean());
        let mut errors = Vec::new();
        if let Some(error) = lock(&self.shutdown_error).clone() {
            errors.push(error);
        }
        if let Err(error) = final_result {
            errors.push(format!("{error:#}"));
        }
        if !errors.is_empty() {
            let error = errors.join("; ");
            *lock(&self.error) = Some(error.clone());
            anyhow::bail!("{error}");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed_cached_file(engine: &Engine, path: &str, bytes: &[u8]) -> Result<()> {
        let handle = engine.open_file(path, true, false, true)?;
        engine.write(handle, 0, bytes, false, false)?;
        engine.flush(Some(handle))?;
        engine.close(handle)
    }

    #[test]
    fn stable_metadata_open_uses_cache_without_waiting_for_store_or_cloning_maps() -> Result<()> {
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), None)?;
        seed_cached_file(&engine, "file", b"cached")?;
        engine.lookup("file")?;
        let held = lock(&engine.store);
        assert_eq!(Arc::strong_count(&held.projection().entries), 1);
        let (send, receive) = std::sync::mpsc::channel();
        let reader = engine.clone();
        let thread = std::thread::spawn(move || {
            let result = reader
                .open_native_file("file", false, false, false, None, true)
                .and_then(|token| {
                    let result = reader.info(token);
                    reader.close(token)?;
                    result
                });
            send.send(result).unwrap();
        });
        let result = receive.recv_timeout(Duration::from_secs(1));
        drop(held);
        thread.join().unwrap();
        let (_, len) = result.context("cached stat waited for the metadata Store lock")??;
        assert_eq!(len, 6);
        engine.shutdown()?;
        Ok(())
    }

    #[test]
    fn metadata_cache_invalidates_aliases_on_rename_delete_and_recreate() -> Result<()> {
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), None)?;
        seed_cached_file(&engine, "file", b"old")?;
        let original = engine.lookup("file")?.id;
        assert_eq!(engine.lookup("/FILE")?.id, original);
        let rename = engine.open_file("file", false, false, false)?;
        engine.rename(rename, "renamed", false)?;
        engine.close(rename)?;
        assert!(engine.lookup("file").is_err());
        assert!(engine.lookup("/FILE").is_err());
        assert_eq!(engine.lookup("renamed")?.id, original);
        lock(&engine.store).delete(&original)?;
        assert!(engine.lookup("renamed").is_err());
        seed_cached_file(&engine, "file", b"replacement")?;
        let replacement = engine.lookup("/FILE")?;
        assert_ne!(replacement.id, original);
        assert_eq!(replacement.working_len, Some(11));
        assert!(
            engine
                .open_native_file("file", false, true, false, None, true)
                .is_err()
        );
        engine.shutdown()?;
        Ok(())
    }

    #[test]
    fn cached_metadata_keeps_unsealed_growth_and_explicit_times_visible() -> Result<()> {
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), None)?;
        seed_cached_file(&engine, "file", b"old")?;
        let entry = engine.lookup("file")?;
        let writer = engine.open_file("file", false, false, true)?;
        engine.write(writer, 3, b" extension", false, false)?;
        assert_eq!(engine.lookup("file")?.working_len, Some(13));
        let metadata = engine.open_native_file("file", false, false, false, None, true)?;
        assert_eq!(engine.info(metadata)?.1, 13);
        let time = 116_444_736_042_000_000;
        engine.set_metadata(&entry.id, [None, None, Some(time), None], None, None)?;
        assert_eq!(engine.lookup("file")?.times.modified, time);
        engine.close(metadata)?;
        engine.close(writer)?;
        engine.shutdown()?;
        Ok(())
    }

    #[test]
    fn metadata_cache_follows_checkout_when_both_views_are_already_loaded() -> Result<()> {
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), None)?;
        seed_cached_file(&engine, "file", b"main")?;
        let other = {
            let mut store = lock(&engine.store);
            let snapshot = store.checkpoint()?;
            store.fork(&snapshot, "other")?
        };
        lock(&engine.store).checkout(&other)?;
        let writer = engine.open_file("file", false, false, true)?;
        engine.truncate(writer, 0, false)?;
        engine.write(writer, 0, b"other branch", false, false)?;
        engine.flush(Some(writer))?;
        engine.close(writer)?;
        assert_eq!(engine.lookup("file")?.working_len, Some(12));
        lock(&engine.store).checkout(MAIN)?;
        assert_eq!(engine.lookup("file")?.working_len, Some(4));
        let metadata = engine.open_native_file("file", false, false, false, None, true)?;
        assert_eq!(engine.info(metadata)?.1, 4);
        engine.close(metadata)?;
        engine.shutdown()?;
        Ok(())
    }

    #[test]
    fn close_waiting_for_one_scratch_does_not_block_other_handles() -> Result<()> {
        let root = tempfile::tempdir()?;
        let engine = Engine::open(root.path(), None)?;
        let closing = engine.open_file("closing", true, false, true)?;
        let other = engine.open_file("other", true, false, true)?;
        engine.cleanup(closing, false)?;
        let scratch = engine.handle(closing)?.scratch.clone().unwrap();
        let held = lock(&scratch);
        let closer = engine.clone();
        let close = std::thread::spawn(move || closer.close(closing));
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let reached_release = loop {
            match engine.handles.try_read() {
                Ok(handles) if !handles.contains_key(&closing) => break true,
                Err(std::sync::TryLockError::WouldBlock) => break true,
                _ if std::time::Instant::now() >= deadline => break false,
                _ => std::thread::yield_now(),
            }
        };
        let (send, receive) = std::sync::mpsc::channel();
        let reader = engine.clone();
        let probe = std::thread::spawn(move || send.send(reader.handle(other).is_ok()).unwrap());
        let accessible = receive.recv_timeout(Duration::from_secs(1));
        // Always release/join, even when a regression holds the handle-map lock.
        drop(held);
        close.join().unwrap()?;
        probe.join().unwrap();
        engine.close(other)?;
        engine.shutdown()?;
        assert!(reached_release, "close did not reach scratch release");
        assert_eq!(accessible.ok(), Some(true));
        Ok(())
    }
}
