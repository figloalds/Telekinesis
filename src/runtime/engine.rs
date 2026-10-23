//! Filesystem operations share metadata only at namespace/save boundaries.
use crate::{
    content::{seal, working::*},
    core::*,
};
use anyhow::{Context, Result, ensure};
use std::{
    collections::BTreeMap,
    fs::File,
    path::Path,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

type Invalidate = Arc<dyn Fn(&str, u32) -> Result<()> + Send + Sync>;
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
    handles: RwLock<BTreeMap<u64, Arc<Handle>>>,
    next: AtomicU64,
    invalidate: RwLock<Option<Invalidate>>,
    notifications: Mutex<BTreeMap<String, u32>>,
    pub stopping: AtomicBool,
    pub error: Mutex<Option<String>>,
    _owner: File,
    pub last_applied: Mutex<BTreeMap<String, u128>>,
    publication: Mutex<()>,
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
        let engine = Arc::new(Self {
            store,
            working,
            handles: RwLock::new(BTreeMap::new()),
            next: AtomicU64::new(1),
            invalidate: RwLock::new(None),
            notifications: Mutex::new(BTreeMap::new()),
            stopping: AtomicBool::new(false),
            error: Mutex::new(None),
            _owner: owner,
            last_applied: Mutex::new(BTreeMap::new()),
            publication: Mutex::new(()),
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
                    let files = engine.working.pending(false);
                    let _publication = lock(&engine.publication);
                    let result =
                        seal::seal_batch(&engine.store, &engine.working, &files).map(|_| ());
                    drop(_publication);
                    if let Err(error) = result {
                        *lock(&engine.error) = Some(format!("{error:#}"));
                    }
                    if let Err(error) = engine.retry_notifications() {
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
    pub fn lookup(&self, path: &str) -> Result<Entry> {
        let (mut entry, branch, anchor) = {
            let store = lock(&self.store);
            let e = store.lookup(path)?;
            let anchor = store.projection().anchors.get(&e.id).cloned();
            (e, store.active.clone(), anchor)
        };
        if let Some(anchor) = anchor {
            entry.working_len = self.working.visible_len(&branch, &entry.id, &anchor);
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
        ensure!(!self.stopping.load(Ordering::Acquire), "STOPPING");
        let (entry, basis) = {
            let mut store = lock(&self.store);
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
            ensure!(
                !directory || e.kind == EntryKind::Directory,
                "NOT_A_DIRECTORY"
            );
            let basis = if e.kind == EntryKind::File {
                Some(store.basis(&e.id)?)
            } else {
                None
            };
            store.flush_metadata()?;
            (e, basis)
        };
        let scratch = basis
            .map(|basis| self.working.open(basis, writable))
            .transpose()?;
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
    pub fn info(&self, token: u64) -> Result<(Entry, u64)> {
        let h = self.handle(token)?;
        let e = lock(&h.entry).clone();
        let len = if let Some(s) = &h.scratch {
            lock(s).len()?
        } else {
            0
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
        let original = h.scratch.as_ref().context("IS_A_DIRECTORY")?;
        let entity = lock(&h.entry).id.clone();
        let basis = {
            let store = lock(&self.store);
            store.basis(&entity)?
        };
        if lock(original).record.basis.anchor == basis.anchor {
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
        let entity = lock(&h.entry).id.clone();
        let (mut entry, branch, anchor) = {
            let store = lock(&self.store);
            let entry = store.entry(&entity)?;
            let anchor = store.projection().anchors.get(&entity).cloned();
            (entry, store.active.clone(), anchor)
        };
        let len = anchor
            .and_then(|anchor| self.working.visible_len(&branch, &entity, &anchor))
            .unwrap_or_else(|| entry.content.as_ref().map_or(0, |c| c.total_len));
        entry.working_len = Some(len);
        Ok((entry, len))
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
        ensure!(h.writable, "ACCESS_DENIED");
        let s = h.scratch.as_ref().context("IS_A_DIRECTORY")?;
        let count = lock(s).write(self.working.content(), offset, bytes, append, constrained)?;
        if h.closed.load(Ordering::Acquire) {
            self.working.schedule(s);
        }
        Ok(count)
    }
    pub fn truncate(&self, token: u64, size: u64, allocation: bool) -> Result<()> {
        let h = self.handle(token)?;
        ensure!(h.writable, "ACCESS_DENIED");
        let mut s = lock(h.scratch.as_ref().context("IS_A_DIRECTORY")?);
        if allocation && size >= s.len()? {
            return Ok(());
        }
        let count = s
            .len()?
            .min(s.base_visible_len)
            .div_ceil(crate::model::content::CHUNK_SIZE);
        for index in 0..count {
            s.hydrate_chunk(self.working.content(), index)?;
        }
        s.truncate(size)
    }
    pub fn flush(&self, token: Option<u64>) -> Result<()> {
        let _publication = lock(&self.publication);
        let files = if let Some(token) = token {
            self.handle(token)?.scratch.clone().into_iter().collect()
        } else {
            self.working.pending(true)
        };
        seal::seal_batch(&self.store, &self.working, &files)?;
        // Also covers namespace-only fsyncs with no dirty content event.
        {
            let mut store = lock(&self.store);
            store.flush_metadata()?;
            store.db.execute_batch("PRAGMA wal_checkpoint(FULL);")?;
        }
        *lock(&self.error) = None;
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
    pub fn close(&self, token: u64) -> Result<()> {
        self.cleanup(token, false)?;
        if let Some(handle) = self.handles.write().unwrap().remove(&token)
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
                if let Some(anchor) = anchors.get(&entry.id) {
                    entry.working_len = self.working.visible_len(&branch, &entry.id, anchor);
                }
            }
            *cache = Some(Arc::new(entries));
        }
        Ok(cache.as_ref().unwrap().clone())
    }
    pub fn rename(&self, token: u64, target: &str, replace: bool) -> Result<()> {
        let h = self.handle(token)?;
        let entity = lock(&h.entry).id.clone();
        if replace && let Some(scratch) = &h.scratch {
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
        let mut store = lock(&self.store);
        let mut e = store.entry(&entity)?;
        let original = e.clone();
        for (old, new) in [
            &mut e.times.created,
            &mut e.times.accessed,
            &mut e.times.modified,
            &mut e.times.changed,
        ]
        .into_iter()
        .zip(times)
        {
            if new != 0 && new != u64::MAX {
                *old = new;
            }
        }
        if original.times == e.times
            && (attributes == u32::MAX || attributes == original.attributes)
        {
            return Ok(());
        }
        store.set_times(
            &entity,
            e.times,
            None,
            (attributes != u32::MAX).then_some(attributes),
        )?;
        *lock(&h.entry) = store.entry(&entity)?;
        Ok(())
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
    pub fn shutdown(&self) -> Result<()> {
        self.stopping.store(true, Ordering::Release);
        self.flush(None)?;
        self.working.reap_clean()?;
        Ok(())
    }
}
