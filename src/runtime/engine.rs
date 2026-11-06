//! Filesystem operations share metadata only at namespace/save boundaries.
use crate::util::namespace::NamespaceChanges;
use crate::{
    content::{Kind, seal, working::*},
    core::*,
    model::content::{ContentKind, Digest},
    util::log::event,
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
/// How far ahead of a sequential reader its chunks are asked for.
pub const READ_AHEAD: u64 = 16 * 1024 * 1024;
static OPEN_COUNTS: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];
static OPEN_NANOS: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];
static NAMESPACE_HITS: AtomicU64 = AtomicU64::new(0);
static NAMESPACE_MISSES: AtomicU64 = AtomicU64::new(0);
/// Handle tokens, unique across every engine of the process. The embedded host
/// resolves a folder's current engine on each call, so a token kept across a
/// worker restart must be `INVALID_HANDLE` there, not another file's handle.
static NEXT_TOKEN: AtomicU64 = AtomicU64::new(1);
const NAMESPACE_CACHE_LIMIT: usize = 65_536;
const NAMESPACE_CACHE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone)]
struct NamespaceEntry {
    epoch: u64,
    entry: Entry,
    branch: String,
    version: Option<String>,
    anchor: Option<String>,
    ancestors: Option<Arc<[String]>>,
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
            + self.ancestors.as_ref().map_or(0, |ancestors| {
                32 + ancestors
                    .iter()
                    .map(|id| std::mem::size_of::<String>() + id.len())
                    .sum::<usize>()
            })
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
/// Where a handle's read-ahead stands: see [`Engine::read_sequential`].
#[derive(Default)]
struct ReadAhead {
    /// Where the last window starts; `None` until the first read.
    from: Option<u64>,
    /// What this handle holds hinted.
    hinted: Vec<(Digest, Kind)>,
}
pub struct Handle {
    pub entry: Mutex<Entry>,
    pub scratch: Option<Arc<Mutex<Scratch>>>,
    pub writable: bool,
    closed: AtomicBool,
    pub delete: AtomicBool,
    pub directory: Mutex<Option<Arc<Vec<Entry>>>>,
    read_ahead: Mutex<ReadAhead>,
}
pub struct Engine {
    pub store: Mutex<Store>,
    pub working: WorkingLayer,
    namespace_changes: Arc<NamespaceChanges>,
    namespace_cache: Mutex<NamespaceCache>,
    handles: RwLock<BTreeMap<u64, Arc<Handle>>>,
    invalidate: RwLock<Option<Invalidate>>,
    notifications: Mutex<BTreeMap<String, u32>>,
    pub stopping: AtomicBool,
    pub error: Mutex<Option<String>>,
    shutdown_error: Mutex<Option<String>>,
    _owner: File,
    pub last_applied: Mutex<BTreeMap<String, u128>>,
    pub peer_status: Mutex<BTreeMap<String, serde_json::Value>>,
    publication: Mutex<()>,
    faults: Mutex<std::collections::BTreeSet<String>>,
}
/// How often a draft that could not seal at the start is tried again.
const WAITING_DRAFT_RETRY: Duration = Duration::from_secs(5);
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
        let namespace_changes = lock(&store).namespace_changes.clone();
        let engine = Arc::new(Self {
            store,
            working,
            namespace_changes,
            namespace_cache: Mutex::new(NamespaceCache::default()),
            handles: RwLock::new(BTreeMap::new()),
            invalidate: RwLock::new(None),
            notifications: Mutex::new(BTreeMap::new()),
            stopping: AtomicBool::new(false),
            error: Mutex::new(None),
            shutdown_error: Mutex::new(None),
            _owner: owner,
            last_applied: Mutex::new(BTreeMap::new()),
            peer_status: Mutex::new(BTreeMap::new()),
            publication: Mutex::new(()),
            faults: Mutex::new(Default::default()),
        });
        let weak = Arc::downgrade(&engine);
        std::thread::Builder::new()
            .name("tkfs-sealer".into())
            .spawn(move || {
                let mut next_seal = std::time::Instant::now();
                let mut next_reap = std::time::Instant::now();
                let mut next_retry = std::time::Instant::now();
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
                    if std::time::Instant::now() >= next_retry {
                        next_retry = std::time::Instant::now() + WAITING_DRAFT_RETRY;
                        if let Err(error) = engine.retry_waiting_drafts() {
                            *lock(&engine.error) = Some(format!("{error:#}"));
                        }
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
    /// Drafts [`seal::recover`] could not seal at the start. This asks again for what they need,
    /// and what it does not wait for: a hint lasts longer than [`WAITING_DRAFT_RETRY`].
    fn retry_waiting_drafts(&self) -> Result<()> {
        let _publication = lock(&self.publication);
        let (sealed, failure) = seal::retry_waiting(&self.store, &self.working);
        drop(_publication);
        if !sealed.is_empty() {
            let paths: Vec<_> = {
                let store = lock(&self.store);
                sealed
                    .iter()
                    .filter_map(|entity| store.projection().path(entity).ok())
                    .collect()
            };
            self.invalidate_paths(paths)?;
        }
        failure.map_or(Ok(()), Err)
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
        let epoch = self.namespace_changes.epoch();
        let cached = lock(&self.namespace_cache)
            .entries
            .get_mut(path)
            .and_then(|value| {
                if self.namespace_changes.unchanged(
                    value.epoch,
                    epoch,
                    &value.entry.id,
                    value.ancestors.as_deref(),
                ) {
                    value.epoch = epoch;
                    Some(value.clone())
                } else {
                    None
                }
            });
        if let Some(cached) = cached
            && self.namespace_changes.epoch() == epoch
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
                epoch: store.namespace_changes.epoch(),
                branch: store.active.clone(),
                version: store.projection().visible.get(&entry.id).cloned(),
                anchor: store.projection().anchors.get(&entry.id).cloned(),
                ancestors: Self::namespace_ancestors(store.projection(), &entry),
                entry,
            }
        };
        // Cache only successful lookups and owned entry values, never a clone
        // of the projection maps or a handle into the served namespace. Raw
        // pathname keys can miss on aliases; authoritative lookup validates
        // them before insertion. Entity/ancestor IDs invalidate all aliases;
        // global records and history gaps force an authoritative lookup.
        lock(&self.namespace_cache).insert(path, value.clone());
        Ok(value)
    }
    fn namespace_ancestors(projection: &Projection, entry: &Entry) -> Option<Arc<[String]>> {
        let mut ids = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        let mut current = entry;
        while current.id != ROOT {
            if !seen.insert(current.id.as_str()) {
                return None;
            }
            ids.push(current.parent.clone());
            current = projection.entries.get(&current.parent)?;
        }
        Some(ids.into())
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
    /// Opens a file for writing, to cut it to nothing: the caller calls `truncate(token, 0, false)`
    /// before anything else it does with the handle but `close`. Until then the file stays as it
    /// was, whatever becomes of the handle. With the old contents going none of them is asked
    /// for, so this works with no peer to ask on a file whose manifest has never been here.
    pub fn open_to_replace(&self, path: &str) -> Result<u64> {
        ensure!(!self.stopping.load(Ordering::Acquire), "STOPPING");
        let (entry, basis) = {
            let store = lock(&self.store);
            let entry = store.lookup(path)?;
            let basis = store.basis(&entry.id)?;
            (entry, basis)
        };
        let scratch = self.working.open_to_replace(basis)?;
        Ok(self.add_handle(entry, Some(scratch), true))
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
        let token = self.add_handle(entry, scratch, writable);
        open_timing(3, started);
        Ok(token)
    }
    fn add_handle(
        &self,
        entry: Entry,
        scratch: Option<Arc<Mutex<Scratch>>>,
        writable: bool,
    ) -> u64 {
        let token = NEXT_TOKEN.fetch_add(1, Ordering::Relaxed);
        self.handles.write().unwrap().insert(
            token,
            Arc::new(Handle {
                entry: Mutex::new(entry),
                scratch,
                writable,
                closed: AtomicBool::new(false),
                delete: AtomicBool::new(false),
                directory: Mutex::new(None),
                read_ahead: Mutex::new(ReadAhead::default()),
            }),
        );
        token
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
    /// [`Self::read`] for a reader going through a file in order, which is how a
    /// phone reads. It also asks for the [`READ_AHEAD`] bytes after the read, again
    /// once the reader has passed half of that window, or left it. A read asks only
    /// for what it touches, and each such ask costs a wake-up of the worker's pump:
    /// this way one wake-up brings a window. It never fetches a whole large file for
    /// a reader that stops, and closing the handle takes back what it asked for.
    pub fn read_sequential(&self, token: u64, offset: u64, out: &mut [u8]) -> Result<usize> {
        let count = self.read(token, offset, out)?;
        if count > 0
            && let Err(error) = self.read_ahead(token, offset + count as u64)
        {
            // The read stands. The next one that needs anything says what is wrong.
            event!(warn, READ_AHEAD_FAILED, {"error": format!("{error:#}")});
        }
        Ok(count)
    }
    fn read_ahead(&self, token: u64, end: u64) -> Result<()> {
        let h = self.handle(token)?;
        let Some(scratch) = &h.scratch else {
            return Ok(());
        };
        let mut ahead = lock(&h.read_ahead);
        if ahead
            .from
            .is_some_and(|from| (from..from + READ_AHEAD / 2).contains(&end))
        {
            return Ok(());
        }
        let (reference, visible) = {
            let scratch = lock(scratch);
            (
                scratch.record.basis.content.clone(),
                scratch.base_visible_len,
            )
        };
        let content = self.working.content();
        content.unhint(&std::mem::take(&mut ahead.hinted));
        let len = READ_AHEAD.min(visible.saturating_sub(end));
        ahead.hinted = content.hint_range(&reference, end, len)?;
        ahead.from = Some(end);
        Ok(())
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
    /// Whether fault `name` (or `name-persistent`) is injected; a one-shot
    /// fault is consumed.
    pub fn take_fault(&self, name: &str) -> bool {
        self.fault(name).is_err()
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
        self.flush_locked(token, native_drained)
    }
    fn flush_locked(&self, token: Option<u64>, native_drained: bool) -> Result<()> {
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
    /// Include pending local saves in the alternatives before choosing a head.
    /// A seal after resolution would otherwise reopen the conflict immediately.
    pub fn resolve(&self, entity: &str, selected: &str) -> Result<Event> {
        self.fault("flush")?;
        let publication = lock(&self.publication);
        self.flush_locked(None, false)?;
        let (event, durability) = {
            let mut store = lock(&self.store);
            let previous = store.projection().path(entity)?;
            let event = store.resolve(entity, selected)?;
            let current = store.projection().path(entity)?;
            let mut notifications = lock(&self.notifications);
            notifications.insert(previous, 3);
            notifications.insert(current, if event.state.deleted { 2 } else { 3 });
            drop(notifications);
            (event, store.sync_metadata())
        };
        drop(publication);
        // Commit and notification have independent outcomes. Keep failed
        // invalidations queued rather than reporting a committed choice as lost.
        if let Err(error) = self.retry_notifications() {
            *lock(&self.error) = Some(format!("{error:#}"));
        }
        if let Err(error) = durability {
            *lock(&self.error) = Some(format!("{error:#}"));
            return Err(error);
        }
        Ok(event)
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
        let deletion = (|| -> Result<()> {
            if delete || h.delete.load(Ordering::Acquire) {
                let entity = lock(&h.entry).id.clone();
                let mut store = lock(&self.store);
                if !store.entry(&entity)?.deleted {
                    store.delete(&entity)?;
                }
            }
            Ok(())
        })();
        // Cleanup retires a writer even if its namespace mutation failed.
        // Otherwise its dirty scratch can never enter the delayed seal queue.
        if !h.closed.swap(true, Ordering::AcqRel)
            && let Some(s) = &h.scratch
        {
            self.working.cleanup(s, h.writable);
        }
        deletion
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
        let cleanup = self.cleanup(token, false);
        // Scratch release can wait for a batched seal. Do not hold the global
        // handle map across that wait and stall every other file callback.
        let handle = self.handles.write().unwrap().remove(&token);
        if let Some(handle) = handle {
            self.working
                .content()
                .unhint(&lock(&handle.read_ahead).hinted);
            if let Some(scratch) = &handle.scratch {
                self.working.release(scratch);
            }
        }
        cleanup
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
                // The target's own unsealed draft predates this replacement.
                // Seal it first so it becomes the replacement's parent; a later
                // write-back would fork the entity against its stale basis.
                let branch = lock(scratch).record.basis.branch.clone();
                let replaced = self.working.dirty_for_entity(&branch, &destination.id);
                let stats = seal::seal_batch(&self.store, &self.working, &replaced)?;
                self.queue_sealed(&replaced, stats.entities)?;
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
                        // A conflicting incoming deletion can remain an
                        // unselected alternative. Notify the projected state.
                        let deleted = store.entry(&event.entity)?.deleted;
                        notifications.insert(path, if deleted { 2 } else { 3 });
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

/// What the head of a file needs a peer to hold before the peer has all of it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeadClosure {
    /// Some head of the entity is an event of this device. A head only a peer wrote is the peer's
    /// to hold, whatever this device once wrote under it.
    pub written_here: bool,
    /// The entity is deleted: there are no bytes to confirm.
    pub deleted: bool,
    /// The manifest and chunks of each head written here, without repeats. When a manifest is not
    /// in the local packs its chunks are unknown, so only the manifest is listed.
    pub digests: Vec<Digest>,
}

impl Engine {
    /// This device's own events with a sequence after `after`, oldest first, at most `limit`.
    pub fn own_events_after(&self, after: u64, limit: usize) -> Result<Vec<(u64, Event)>> {
        Ok(self
            .own_events_between(after, u64::MAX, limit, usize::MAX)?
            .events)
    }

    /// This device's own events with a sequence in `after..=upto`, oldest first, at most `limit`
    /// of them, from at most `pages` pages of the journal. The store is locked one page at a time,
    /// so a long journal does not stall the sealer, the feed or the provider. `reached` is how far
    /// the journal was read: the position to resume from.
    pub fn own_events_between(
        &self,
        after: u64,
        upto: u64,
        limit: usize,
        pages: usize,
    ) -> Result<OwnEvents> {
        const PAGE: usize = 1024;
        let mut found = OwnEvents {
            events: Vec::new(),
            reached: after,
        };
        for _ in 0..pages {
            let store = lock(&self.store);
            let page = store.events_after(found.reached, PAGE)?;
            let full = page.len() == PAGE;
            for (seq, event) in page {
                if seq > upto {
                    found.reached = upto;
                    return Ok(found);
                }
                found.reached = seq;
                if event.author == store.device {
                    found.events.push((seq, event));
                    if found.events.len() == limit {
                        return Ok(found);
                    }
                }
            }
            if !full {
                found.reached = found.reached.max(upto.min(store.feed_position()?));
                return Ok(found);
            }
        }
        Ok(found)
    }

    /// The journal's latest sequence after making what it holds durable. The metadata store keeps
    /// recent events in an open transaction that a kill takes back, and their sequences are then
    /// used again. A position taken before this may name events that never were.
    pub fn durable_position(&self) -> Result<u64> {
        let mut store = lock(&self.store);
        store.sync_metadata()?;
        store.feed_position()
    }

    /// Whether the journal holds an event of `entity` at `seq`: false for a sequence that was taken
    /// back, or that is another entity's now.
    pub fn has_event(&self, seq: u64, entity: &str) -> Result<bool> {
        let store = lock(&self.store);
        Ok(store
            .events_after(seq.saturating_sub(1), 1)?
            .first()
            .is_some_and(|(at, event)| *at == seq && event.entity == entity))
    }

    /// The bytes a peer must hold for `entity`'s current head. It never fetches: what the local
    /// packs do not have is not asked for from anyone.
    pub fn head_closure(&self, entity: &str) -> Result<HeadClosure> {
        let (deleted, heads) = {
            let store = lock(&self.store);
            let projection = store.projection();
            let versions = projection
                .heads
                .get(entity)
                .filter(|versions| !versions.is_empty())
                .with_context(|| format!("NOT_FOUND: no head for {entity}"))?;
            let mut heads = Vec::new();
            for version in versions {
                let head = store
                    .version(version)?
                    .with_context(|| format!("NOT_FOUND: head {version} of {entity}"))?;
                let bytes_here = content_written_here(&store, &head)?;
                heads.push((head, bytes_here));
            }
            let deleted = projection.entries.get(entity).is_some_and(|e| e.deleted);
            (deleted, heads)
        };
        let content = self.working.content();
        let device = lock(&self.store).device.clone();
        let mut closure = HeadClosure {
            deleted,
            ..HeadClosure::default()
        };
        for (head, bytes_here) in &heads {
            closure.written_here |= head.author == device || *bytes_here;
            // A deleted head has no bytes to hold, and one that only a peer's bytes are under is
            // the peer's to hold. A live head beside a deleted one still needs its own.
            let Some(reference) = head
                .state
                .content
                .as_ref()
                .filter(|_| *bytes_here && !head.state.deleted)
            else {
                continue;
            };
            reference.validate()?;
            closure.digests.push(reference.digest);
            if reference.kind == ContentKind::Manifest
                && content.packs.contains(reference.digest)?
            {
                closure.digests.extend(content.chunks(reference)?);
            }
        }
        let mut seen = std::collections::BTreeSet::new();
        closure.digests.retain(|digest| seen.insert(*digest));
        Ok(closure)
    }
}

/// How many events [`content_written_here`] reads before it gives up and says yes.
const ORIGIN_WALK_LIMIT: usize = 4096;

/// Whether this device wrote the bytes `head` holds: the event that introduced its content is
/// found by walking parents while the content stays the same (a rename, a times change or a peer's
/// metadata edit over the same bytes does not change who wrote them). A parent that is not here,
/// or a walk too long to finish, says yes: bytes this device may have written are never skipped.
fn content_written_here(store: &Store, head: &Event) -> Result<bool> {
    let mut pending = vec![head.clone()];
    let mut seen = std::collections::BTreeSet::new();
    while let Some(event) = pending.pop() {
        if !seen.insert(event.id.clone()) {
            continue;
        }
        if seen.len() > ORIGIN_WALK_LIMIT {
            return Ok(true);
        }
        let mut introduced = true;
        for parent in &event.parents {
            match store.version(parent)? {
                Some(parent) if parent.state.content == head.state.content => {
                    introduced = false;
                    pending.push(parent);
                }
                Some(_) => {}
                None => return Ok(true),
            }
        }
        if introduced && event.author == store.device {
            return Ok(true);
        }
    }
    Ok(false)
}

/// What [`Engine::own_events_between`] read.
pub struct OwnEvents {
    pub events: Vec<(u64, Event)>,
    pub reached: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        content::{Kind, Manifest, ObjectFetcher},
        model::content::{CHUNK_SIZE, ContentKind, Digest, FileContentRef},
    };

    fn seed_cached_file(engine: &Engine, path: &str, bytes: &[u8]) -> Result<()> {
        let handle = engine.open_file(path, true, false, true)?;
        engine.write(handle, 0, bytes, false, false)?;
        engine.flush(Some(handle))?;
        engine.close(handle)
    }

    /// An event of another device on top of `entity`'s head, as a feed would bring it.
    fn peer_edit(engine: &Engine, entity: &str, change: impl FnOnce(&mut Entry)) -> Result<Event> {
        let mut event = {
            let mut store = lock(&engine.store);
            let mut state = store.entry(entity)?;
            change(&mut state);
            let heads = store.projection().heads[entity].iter().cloned().collect();
            store.event(MAIN, state, heads, Operation::Content)
        };
        event.author = "a peer".into();
        engine.apply_remote(std::slice::from_ref(&event))?;
        Ok(event)
    }

    #[test]
    fn own_events_are_this_devices_in_order_and_after_a_sequence() -> Result<()> {
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), None)?;
        seed_cached_file(&engine, "first", b"1")?;
        let first = engine.lookup("first")?.id;
        peer_edit(&engine, &first, |state| state.attributes = 0x20)?;
        seed_cached_file(&engine, "second", b"2")?;

        let own = engine.own_events_after(0, 100)?;
        assert!(own.len() >= 2);
        assert!(own.windows(2).all(|pair| pair[0].0 < pair[1].0));
        assert!(own.iter().all(|(_, event)| event.author != "a peer"));
        let all = lock(&engine.store).events_after(0, 100)?;
        assert_eq!(
            all.len(),
            own.len() + 1,
            "exactly the peer's event is left out"
        );

        let (cut, _) = own[0];
        assert_eq!(engine.own_events_after(cut, 100)?, own[1..]);
        assert_eq!(engine.own_events_after(0, 1)?, own[..1]);
        assert!(
            engine
                .own_events_after(own.last().unwrap().0, 100)?
                .is_empty()
        );
        engine.shutdown()?;
        Ok(())
    }

    #[test]
    fn a_head_closure_lists_the_manifest_and_every_chunk_without_repeats() -> Result<()> {
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), None)?;
        let bytes: Vec<u8> = (0..2 * CHUNK_SIZE as usize + 10)
            .map(|index| (index / 7 % 251) as u8)
            .collect();
        seed_cached_file(&engine, "big", &bytes)?;
        seed_cached_file(&engine, "small", b"tiny")?;

        let big = engine.lookup("big")?;
        let reference = big.content.clone().unwrap();
        let chunks = engine.working.content().chunks(&reference)?;
        assert_eq!(chunks.len(), 3);
        let closure = engine.head_closure(&big.id)?;
        assert!(closure.written_here && !closure.deleted);
        let mut expected = vec![reference.digest];
        expected.extend(chunks);
        assert_eq!(closure.digests, expected);

        let small = engine.lookup("small")?;
        let closure = engine.head_closure(&small.id)?;
        assert_eq!(closure.digests, vec![small.content.unwrap().digest]);
        engine.shutdown()?;
        Ok(())
    }

    #[test]
    fn a_head_closure_of_a_deleted_file_or_a_peers_head_needs_no_bytes() -> Result<()> {
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), None)?;
        seed_cached_file(&engine, "gone", b"gone")?;
        seed_cached_file(&engine, "theirs", b"mine first")?;
        let gone = engine.lookup("gone")?.id;
        lock(&engine.store).delete(&gone)?;
        let closure = engine.head_closure(&gone)?;
        assert!(closure.deleted && closure.written_here && closure.digests.is_empty());

        let theirs = engine.lookup("theirs")?;
        let other = engine.working.content().import(b"the peer's bytes")?;
        peer_edit(&engine, &theirs.id, |state| state.content = Some(other))?;
        let closure = engine.head_closure(&theirs.id)?;
        assert_eq!(closure, HeadClosure::default());

        assert!(engine.head_closure("no such entity").is_err());
        engine.shutdown()?;
        Ok(())
    }

    #[test]
    fn a_peers_metadata_event_over_bytes_written_here_still_needs_them() -> Result<()> {
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), None)?;
        seed_cached_file(&engine, "mine", b"bytes only this device has")?;
        let id = engine.lookup("mine")?.id;
        let before = engine.head_closure(&id)?;
        assert_eq!(before.digests.len(), 1);
        // The PC renames it: its event carries this device's content, and only the name changed.
        let renamed = peer_edit(&engine, &id, |state| state.name = "renamed".into())?;
        assert_eq!(renamed.state.content, engine.lookup("renamed")?.content);
        let after = engine.head_closure(&id)?;
        assert!(after.written_here);
        assert_eq!(after.digests, before.digests);
        // And once more, a second metadata event on top of that one.
        peer_edit(&engine, &id, |state| state.attributes = 0x20)?;
        assert_eq!(engine.head_closure(&id)?.digests, before.digests);
        engine.shutdown()?;
        Ok(())
    }

    #[test]
    fn a_peers_metadata_event_over_a_peers_bytes_needs_none() -> Result<()> {
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), None)?;
        seed_cached_file(&engine, "file", b"mine first")?;
        let id = engine.lookup("file")?.id;
        let theirs = engine.working.content().import(b"the peer wrote these")?;
        peer_edit(&engine, &id, |state| state.content = Some(theirs))?;
        peer_edit(&engine, &id, |state| state.name = "renamed".into())?;
        let closure = engine.head_closure(&id)?;
        assert!(closure.digests.is_empty() && !closure.written_here);
        // Then this device renames it: the bytes are still the peer's.
        let token = engine.open_native_file("renamed", false, false, false, None, true)?;
        engine.rename(token, "again", false)?;
        engine.close(token)?;
        let closure = engine.head_closure(&id)?;
        assert!(closure.written_here && closure.digests.is_empty());
        engine.shutdown()?;
        Ok(())
    }

    #[test]
    fn a_live_head_beside_a_peers_delete_still_needs_its_bytes() -> Result<()> {
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), None)?;
        seed_cached_file(&engine, "file", b"first version")?;
        let id = engine.lookup("file")?.id;
        let first = lock(&engine.store).projection().heads[&id]
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        // A draft is open on it when the PC's delete of the version it was made from arrives; the
        // draft then seals beside the delete, which stays what shows: two heads.
        let writer = engine.open_file("file", false, false, true)?;
        engine.truncate(writer, 0, false)?;
        engine.write(writer, 0, b"an edit the PC never saw", false, false)?;
        let mut deletion = {
            let mut store = lock(&engine.store);
            let mut state = store.entry(&id)?;
            state.deleted = true;
            store.event(MAIN, state, first, Operation::Delete)
        };
        deletion.author = "a peer".into();
        engine.apply_remote(std::slice::from_ref(&deletion))?;
        engine.flush(Some(writer))?;
        engine.close(writer)?;
        assert_eq!(lock(&engine.store).projection().heads[&id].len(), 2);
        let closure = engine.head_closure(&id)?;
        // The delete is what shows, as in the review's case.
        assert!(closure.deleted, "the test needs the delete to be visible");
        assert!(closure.written_here);
        assert!(
            !closure.digests.is_empty(),
            "the live head's bytes exist only here: {closure:?}"
        );
        engine.shutdown()?;
        Ok(())
    }

    #[test]
    fn own_events_are_read_a_page_at_a_time_and_never_past_a_position() -> Result<()> {
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), None)?;
        {
            let mut store = lock(&engine.store);
            for index in 0..2100 {
                store.create(&format!("d{index}"), EntryKind::Directory)?;
            }
        }
        let first = engine.own_events_between(0, u64::MAX, usize::MAX, 1)?;
        assert_eq!((first.events.len(), first.reached), (1024, 1024));
        let second = engine.own_events_between(first.reached, u64::MAX, usize::MAX, 1)?;
        assert_eq!(second.reached, 2048);
        let third = engine.own_events_between(second.reached, u64::MAX, usize::MAX, 1)?;
        assert_eq!((third.events.len(), third.reached), (52, 2100));
        let capped = engine.own_events_between(0, 10, usize::MAX, usize::MAX)?;
        assert_eq!((capped.events.len(), capped.reached), (10, 10));
        let limited = engine.own_events_between(0, u64::MAX, 3, usize::MAX)?;
        assert_eq!((limited.events.len(), limited.reached), (3, 3));
        engine.shutdown()?;
        Ok(())
    }

    #[test]
    fn the_durable_position_and_a_sequences_event_are_known() -> Result<()> {
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), None)?;
        seed_cached_file(&engine, "file", b"x")?;
        let id = engine.lookup("file")?.id;
        let top = engine.durable_position()?;
        assert!(lock(&engine.store).metadata_is_durable());
        assert_eq!(top, lock(&engine.store).feed_position()?);
        let (seq, _) = *engine.own_events_after(0, 100)?.last().unwrap();
        assert!(engine.has_event(seq, &id)?);
        assert!(!engine.has_event(seq, "another entity")?);
        assert!(!engine.has_event(top + 50, &id)?);
        engine.shutdown()?;
        Ok(())
    }

    /// A head the phone has only by name: the manifest is not here, so nothing more is known.
    #[test]
    fn a_head_closure_of_a_manifest_not_held_lists_only_the_manifest() -> Result<()> {
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), None)?;
        seed_cached_file(&engine, "file", b"x")?;
        let entity = engine.lookup("file")?.id;
        let absent = FileContentRef::new(
            ContentKind::Manifest,
            Digest::bytes(b"never here"),
            4 * CHUNK_SIZE,
        )?;
        {
            let mut store = lock(&engine.store);
            let mut state = store.entry(&entity)?;
            state.content = Some(absent.clone());
            let heads = store.projection().heads[&entity].iter().cloned().collect();
            let event = store.event(MAIN, state, heads, Operation::Content);
            store.apply(&[event])?;
        }
        let closure = engine.head_closure(&entity)?;
        assert_eq!(closure.digests, vec![absent.digest]);
        engine.shutdown()?;
        Ok(())
    }

    /// The embedded host resolves a folder's current engine on every call, so a
    /// token kept across a worker restart reaches an engine that never issued it.
    #[test]
    fn two_engines_never_share_a_token() -> Result<()> {
        let (first_home, second_home) = (tempfile::tempdir()?, tempfile::tempdir()?);
        let first = Engine::open(first_home.path(), None)?;
        let second = Engine::open(second_home.path(), None)?;
        seed_cached_file(&first, "file", b"first")?;
        seed_cached_file(&second, "file", b"second")?;
        let open = |engine: &Engine| -> Result<Vec<u64>> {
            (0..8)
                .map(|_| engine.open_file("file", false, false, false))
                .collect()
        };
        let (from_first, from_second) = (open(&first)?, open(&second)?);
        assert!(
            from_first.iter().all(|token| !from_second.contains(token)),
            "{from_first:?} and {from_second:?} share a token"
        );

        let stale = from_first[0];
        let mut bytes = [0u8; 16];
        let refused = second.read(stale, 0, &mut bytes).unwrap_err();
        assert_eq!(crate::model::errors::code_of(&refused), "INVALID_HANDLE");
        let refused = second.close(stale).unwrap_err();
        assert_eq!(crate::model::errors::code_of(&refused), "INVALID_HANDLE");
        assert_eq!(second.read(from_second[0], 0, &mut bytes)?, 6);
        assert_eq!(&bytes[..6], b"second");

        for token in from_first {
            first.close(token)?;
        }
        for token in from_second {
            second.close(token)?;
        }
        first.shutdown()?;
        second.shutdown()?;
        Ok(())
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
    fn git_lock_changes_preserve_unrelated_cached_lookups() -> Result<()> {
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), None)?;
        for path in ["sources", ".git"] {
            let directory = engine.open_file(path, true, true, false)?;
            engine.close(directory)?;
        }
        seed_cached_file(&engine, "sources/file", b"cached")?;
        let original = engine.lookup("/SOURCES/FILE")?.id;
        seed_cached_file(&engine, ".git/index.lock", b"")?;
        let lock_id = engine.lookup(".git/index.lock")?.id;
        lock(&engine.store).delete(&lock_id)?;
        let held = lock(&engine.store);
        let (send, receive) = std::sync::mpsc::channel();
        let reader = engine.clone();
        let thread = std::thread::spawn(move || {
            send.send(reader.lookup("/SOURCES/FILE")).unwrap();
        });
        let result = receive.recv_timeout(Duration::from_secs(1));
        drop(held);
        thread.join().unwrap();
        engine.shutdown()?;
        assert_eq!(
            result
                .context("index.lock invalidated an unrelated cached path")??
                .id,
            original
        );
        Ok(())
    }

    #[test]
    fn metadata_cache_invalidates_descendants_after_ancestor_move() -> Result<()> {
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), None)?;
        {
            let mut store = lock(&engine.store);
            store.create("left", EntryKind::Directory)?;
            store.create("right", EntryKind::Directory)?;
            store.create("left/nested", EntryKind::Directory)?;
        }
        seed_cached_file(&engine, "left/nested/file", b"nested")?;
        let original = engine.lookup("\\LEFT\\NESTED\\FILE")?.id;
        let directory = engine.lookup("left/nested")?.id;
        lock(&engine.store).rename(&directory, "right/moved", false)?;
        assert!(engine.lookup("\\LEFT\\NESTED\\FILE").is_err());
        assert_eq!(engine.lookup("/RIGHT/MOVED/FILE")?.id, original);
        let top = engine.lookup("right")?.id;
        lock(&engine.store).rename(&top, "renamed", false)?;
        assert!(engine.lookup("/RIGHT/MOVED/FILE").is_err());
        assert_eq!(engine.lookup("renamed/moved/file")?.id, original);
        engine.shutdown()?;
        Ok(())
    }

    #[test]
    fn metadata_cache_follows_remote_name_collision_winner_and_delete() -> Result<()> {
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), None)?;
        lock(&engine.store).create("directory", EntryKind::Directory)?;
        seed_cached_file(&engine, "directory/file", b"original")?;
        let original = engine.lookup("/DIRECTORY/FILE")?;
        let remote = {
            let mut store = lock(&engine.store);
            let mut state = store.entry(&original.id)?;
            state.id = "00000000-0000-0000-0000-000000000000".into();
            state.name = "FILE".into();
            store.event(MAIN, state, vec![], Operation::Create)
        };
        engine.apply_remote(std::slice::from_ref(&remote))?;
        assert_eq!(engine.lookup("/DIRECTORY/FILE")?.id, remote.entity);
        let deletion = {
            let mut store = lock(&engine.store);
            let mut state = remote.state.clone();
            state.deleted = true;
            store.event(MAIN, state, vec![remote.id], Operation::Delete)
        };
        engine.apply_remote(&[deletion])?;
        assert_eq!(engine.lookup("/DIRECTORY/FILE")?.id, original.id);
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

    /// Records what it is told to hint, call by call, and what is taken back.
    #[derive(Default)]
    struct HintLog {
        hinted: Mutex<Vec<Vec<Digest>>>,
        taken_back: Mutex<Vec<Digest>>,
    }
    impl ObjectFetcher for HintLog {
        fn fetch(&self, _: &[(Digest, Kind)]) -> Result<()> {
            anyhow::bail!("OBJECT_MISSING")
        }
        fn hint(&self, objects: &[(Digest, Kind)]) {
            lock(&self.hinted).push(objects.iter().map(|(digest, _)| *digest).collect());
        }
        fn unhint(&self, objects: &[(Digest, Kind)]) {
            lock(&self.taken_back).extend(objects.iter().map(|(digest, _)| *digest));
        }
    }

    /// A read handle on a file of `chunks` chunks of which the first `held` are
    /// here, with the digests of them all and a log of what it hints.
    fn read_ahead_handle(
        engine: &Engine,
        chunks: u64,
        held: u64,
    ) -> Result<(u64, Vec<Digest>, Arc<HintLog>)> {
        let content = engine.working.content();
        let token = engine.open_file("big", true, false, true)?;
        engine.flush(Some(token))?;
        engine.close(token)?;
        let mut digests = Vec::new();
        for index in 0..chunks {
            let mut bytes = vec![0; CHUNK_SIZE as usize];
            bytes[..8].copy_from_slice(&index.to_le_bytes());
            digests.push(if index < held {
                content.packs.put(Kind::Chunk, &bytes)?
            } else {
                Digest::bytes(&bytes)
            });
        }
        let manifest = Manifest {
            len: chunks * CHUNK_SIZE,
            chunks: digests.clone(),
        };
        let reference = FileContentRef::new(
            ContentKind::Manifest,
            content.packs.put(Kind::Manifest, &manifest.encode()?)?,
            manifest.len,
        )?;
        let log = Arc::new(HintLog::default());
        content.set_fetcher(log.clone());
        let mut entry = engine.lookup("big")?;
        entry.content = Some(reference.clone());
        let basis = Basis {
            entity: entry.id.clone(),
            branch: "read-ahead".into(),
            version: "v".into(),
            anchor: "v".into(),
            content: reference,
        };
        Ok((engine.open_bound(entry, basis, false)?, digests, log))
    }

    #[test]
    fn a_plain_read_hints_nothing() -> Result<()> {
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), None)?;
        let (token, _, log) = read_ahead_handle(&engine, 8, 2)?;
        let mut bytes = vec![0; 1024];
        assert_eq!(engine.read(token, 0, &mut bytes)?, 1024);
        engine.close(token)?;
        assert!(lock(&log.hinted).is_empty());
        assert!(lock(&log.taken_back).is_empty());
        engine.shutdown()?;
        Ok(())
    }

    #[test]
    fn read_ahead_follows_the_reader_and_ends_with_the_handle() -> Result<()> {
        let window = (READ_AHEAD / CHUNK_SIZE) as usize;
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), None)?;
        // Held up to 3/4 of a window in: the reader goes that far without a fetch.
        let held = 3 * window / 4;
        let (token, digests, log) = read_ahead_handle(&engine, 2 * window as u64, held as u64)?;
        let hints = || lock(&log.hinted).clone();
        let mut bytes = vec![0; CHUNK_SIZE as usize];

        // The first read asks for the window after it, without what is here.
        engine.read_sequential(token, 0, &mut bytes)?;
        assert_eq!(hints(), vec![digests[held..=window].to_vec()]);
        assert!(lock(&log.taken_back).is_empty());

        // Short of half that window, nothing more.
        let half = READ_AHEAD / 2;
        engine.read_sequential(token, half - CHUNK_SIZE, &mut bytes)?;
        assert_eq!(hints().len(), 1);

        // Past it, the window moves, and the last one is taken back first.
        engine.read_sequential(token, half + CHUNK_SIZE, &mut bytes)?;
        let moved = hints();
        assert_eq!(moved.len(), 2);
        let from = window / 2 + 2;
        assert_eq!(moved[1], digests[held..from + window]);
        assert_eq!(*lock(&log.taken_back), moved[0]);

        // A jump back is a new place to read from.
        engine.read_sequential(token, 0, &mut bytes)?;
        assert_eq!(hints().len(), 3);
        assert_eq!(hints()[2], digests[held..=window]);

        // The handle's close takes back what it still holds.
        lock(&log.taken_back).clear();
        engine.close(token)?;
        assert_eq!(*lock(&log.taken_back), hints()[2]);
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
