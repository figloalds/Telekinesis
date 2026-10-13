//! Engine state: open sessions and handles, pending-save recovery, and the
//! filesystem/session operations used by the mount adapters and peer receive.
use super::notify::namespace_notifications;
use crate::core::*;
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

pub struct Session {
    pub entry: Entry,
    pub bytes: crate::staging::Staged,
    pub content_base: Option<String>,
    pub alive_base: Option<String>,
    pub dirty: bool,
    pub references: usize,
    pub preserve_times: u8,
}
pub struct Handle {
    pub entity: String,
    pub generation: u64,
    pub cleaned: bool,
    pub writable: bool,
    pub time_update_disabled: u8,
    directory: Option<Vec<(Entry, usize)>>,
}
#[derive(Serialize, Deserialize)]
struct PendingSave {
    branch: String,
    entry: Entry,
    stage: String,
    content_base: Option<String>,
    alive_base: Option<String>,
    preserve_times: u8,
}
pub struct Engine {
    pub store: Store,
    pub sessions: BTreeMap<String, Session>,
    pub handles: BTreeMap<u64, Handle>,
    pub next_handle: u64,
    pub discovery: Vec<u8>,
    pub health: Option<String>,
    pub observation: Arc<Mutex<Value>>,
    recovery_health: bool,
    pub switching: bool,
    pub mounted: bool,
    pub last_sync: Option<Value>,
    pub transfers: Arc<crate::bulk_sync::ObjectLocks>,
    pub sync_peers: BTreeSet<String>,
    pub peer_sync: BTreeMap<String, Value>,
    pub stopping: Arc<std::sync::atomic::AtomicBool>,
    pub notifications: Vec<(String, u32)>,
    pub notification_error: Option<String>,
}
impl Engine {
    pub fn new(store: Store) -> Self {
        let mut engine = Self {
            store,
            sessions: BTreeMap::new(),
            handles: BTreeMap::new(),
            next_handle: 1,
            discovery: vec![],
            health: None,
            observation: Arc::new(Mutex::new(Value::Null)),
            recovery_health: false,
            switching: false,
            mounted: false,
            last_sync: None,
            transfers: Arc::new(crate::bulk_sync::ObjectLocks::default()),
            sync_peers: BTreeSet::new(),
            peer_sync: BTreeMap::new(),
            stopping: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            notifications: vec![],
            notification_error: None,
        };
        if let Err(error) = engine.load_pending() {
            engine.health = Some(format!("RECOVERY_REQUIRED: {error:#}"));
        }
        engine.publish_observation();
        engine
    }
    pub fn publish_observation(&self) {
        let branch = self.store.branch(&self.store.active).ok();
        *self.observation.lock().unwrap() = json!({"repo":self.store.repo,"device":self.store.device,
            "branch":branch,"generation":self.store.generation,"mounted":self.mounted,
            "open_handles":self.handles.len(),"unflushed_files":self.sessions.values().filter(|s|s.dirty).count(),
            "health":self.health,"sampled_at_ms":time(),"caught_up":false});
    }
    fn load_pending(&mut self) -> Result<()> {
        self.store.db.execute_batch("CREATE TABLE IF NOT EXISTS pending_saves(entity TEXT PRIMARY KEY,payload TEXT NOT NULL)")?;
        let records = self
            .store
            .db
            .prepare("SELECT payload FROM pending_saves")?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for record in records {
            crate::startup::progress("recovering pending saves");
            let pending: PendingSave = serde_json::from_str(&record)?;
            ensure!(
                pending.branch == self.store.active,
                "RECOVERY_BRANCH_MISMATCH"
            );
            let bytes = crate::staging::Staged::recover(&self.store.root, &pending.stage)?;
            self.sessions.insert(
                pending.entry.id.clone(),
                Session {
                    entry: pending.entry,
                    bytes,
                    content_base: pending.content_base,
                    alive_base: pending.alive_base,
                    preserve_times: pending.preserve_times,
                    dirty: true,
                    references: 0,
                },
            );
        }
        if !self.sessions.is_empty() {
            self.health = Some("PENDING_SAVES: recovery in progress".into());
            self.recovery_health = true;
        }
        Ok(())
    }
    pub(crate) fn retain_pending(&mut self, entity: &str, error: &anyhow::Error) -> Result<()> {
        self.recovery_health = self.health.is_none() || self.recovery_health;
        if self.recovery_health {
            self.health = Some(format!("PENDING_SAVE: {error:#}"));
        }
        let session = self.sessions.get_mut(entity).context("INVALID_HANDLE")?;
        if !session.dirty {
            return Ok(());
        }
        let stage = session.bytes.retain(&self.store.root)?;
        let record = PendingSave {
            branch: self.store.active.clone(),
            entry: session.entry.clone(),
            stage,
            content_base: session.content_base.clone(),
            alive_base: session.alive_base.clone(),
            preserve_times: session.preserve_times,
        };
        self.store.db.execute("INSERT INTO pending_saves VALUES(?,?) ON CONFLICT(entity) DO UPDATE SET payload=excluded.payload",
            rusqlite::params![entity,serde_json::to_string(&record)?])?;
        Ok(())
    }
    /// Persist every dirty session into `pending_saves` without committing it,
    /// so a force detach or a poison exit never loses unsaved bytes
    /// (invariant 8). Sessions stay in memory and dirty; a later flush or
    /// `retry_pending` commits them and clears the row. Every dirty session is
    /// attempted even if an earlier one fails; the first failure is returned.
    pub fn retain_all_dirty(&mut self) -> Result<usize> {
        let dirty: Vec<_> = self
            .sessions
            .iter()
            .filter(|(_, s)| s.dirty)
            .map(|(id, _)| id.clone())
            .collect();
        let reason = anyhow::anyhow!("DIRTY_SESSION_RETAINED");
        let mut retained = 0;
        let mut failure = None;
        for entity in &dirty {
            match self.retain_pending(entity, &reason) {
                Ok(()) => retained += 1,
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        match failure {
            Some(error) => Err(error.context(format!(
                "PENDING_SAVE_PERSIST_FAILED: {} of {} dirty sessions not retained",
                dirty.len() - retained,
                dirty.len()
            ))),
            None => Ok(retained),
        }
    }
    fn flush_entity(&mut self, entity: &str) -> Result<()> {
        let session = self.sessions.get_mut(entity).context("INVALID_HANDLE")?;
        if !session.dirty {
            return Ok(());
        }
        let pending: bool = self.store.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM pending_saves WHERE entity=?)",
            [entity],
            |r| r.get(0),
        )?;
        if pending {
            self.store.db.execute_batch("SAVEPOINT staged_save")?;
        }
        let result = (|| {
            let revision = match &mut session.bytes {
                crate::staging::Staged::Memory(bytes) => self.store.write_with_base_and_times(
                    entity,
                    session.content_base.clone(),
                    session.alive_base.clone(),
                    bytes,
                    session.preserve_times,
                )?,
                staged => {
                    let mut source = staged.reader()?;
                    self.store.write_stream_with_base_and_times(
                        entity,
                        session.content_base.clone(),
                        session.alive_base.clone(),
                        &mut source,
                        session.preserve_times,
                    )?
                }
            };
            if pending {
                self.store
                    .db
                    .execute("DELETE FROM pending_saves WHERE entity=?", [entity])?;
                self.store.db.execute_batch("RELEASE staged_save")?;
            }
            Ok(revision)
        })();
        match result {
            Ok(revision) => {
                session.content_base = Some(revision.clone());
                session.alive_base = Some(revision);
                session.dirty = false;
                session.bytes.release();
                session.entry = self.store.entry(entity)?;
                if self.recovery_health && !self.sessions.values().any(|s| s.dirty) {
                    self.health = None;
                    self.recovery_health = false;
                }
                Ok(())
            }
            Err(error) => {
                if pending {
                    let _ = self
                        .store
                        .db
                        .execute_batch("ROLLBACK TO staged_save; RELEASE staged_save");
                }
                self.retain_pending(entity, &error)
                    .context("PENDING_SAVE_PERSIST_FAILED")?;
                Err(error)
            }
        }
    }
    pub fn retry_pending(&mut self) -> Result<()> {
        let entities: Vec<_> = self
            .sessions
            .iter()
            .filter(|(_, s)| s.dirty && s.references == 0)
            .map(|(id, _)| id.clone())
            .collect();
        for entity in entities {
            self.flush_entity(&entity)?;
            self.sessions.remove(&entity);
        }
        Ok(())
    }
    pub fn open(
        &mut self,
        path: &str,
        create: Option<&str>,
        writable: bool,
    ) -> Result<(u64, Entry)> {
        self.open_with_attributes(path, create, writable, None)
    }
    pub fn open_with_attributes(
        &mut self,
        path: &str,
        create: Option<&str>,
        writable: bool,
        attributes: Option<u32>,
    ) -> Result<(u64, Entry)> {
        ensure!(!self.switching, "BUSY_VIEW");
        let special = path
            .replace('\\', "/")
            .trim_matches('/')
            .eq_ignore_ascii_case(".tkfs-runtime.json");
        let entry = if special {
            ensure!(create.is_none() && !writable, "PERMISSION_DENIED");
            Entry {
                id: "control".into(),
                name: ".tkfs-runtime.json".into(),
                kind: "file".into(),
                parent: ROOT.into(),
                alive: true,
                ..Default::default()
            }
        } else if let Some(kind) = create {
            self.store.create_with_attributes(path, kind, attributes)?
        } else {
            self.store.lookup(path)?
        };
        ensure!(
            create.is_some()
                || !writable
                || entry.kind == "directory"
                || entry.basic_info().attributes & 1 == 0,
            "ACCESS_DENIED: readonly file"
        );
        if !self.sessions.contains_key(&entry.id) {
            let bytes = if special {
                crate::staging::Staged::memory(self.discovery.clone())
            } else {
                crate::staging::Staged::from_object(&self.store, entry.content.as_deref())?
            };
            self.sessions.insert(
                entry.id.clone(),
                Session {
                    content_base: entry.revisions.get("content").cloned(),
                    alive_base: entry.revisions.get("alive").cloned(),
                    entry: entry.clone(),
                    bytes,
                    dirty: false,
                    references: 0,
                    preserve_times: 0,
                },
            );
        }
        let session = self.sessions.get_mut(&entry.id).unwrap();
        session.references += 1;
        let h = self.next_handle;
        self.next_handle += 1;
        self.handles.insert(
            h,
            Handle {
                entity: entry.id.clone(),
                generation: self.store.generation,
                cleaned: false,
                writable,
                time_update_disabled: 0,
                directory: None,
            },
        );
        Ok((h, entry))
    }
    pub fn session(&self, h: u64) -> Result<&Session> {
        let handle = self.handles.get(&h).context("INVALID_HANDLE")?;
        ensure!(handle.generation == self.store.generation, "STALE_VIEW");
        self.sessions.get(&handle.entity).context("INVALID_HANDLE")
    }
    /// One directory cut per enumeration, retained through marker continuations.
    /// A rewind sees later mutations; close releases the cursor. Generation
    /// validation applies even to an already populated cursor.
    pub fn directory_entry(
        &mut self,
        h: u64,
        ordinal: usize,
        restart: bool,
    ) -> Result<Option<(Entry, usize)>> {
        let directory = self.session(h)?.entry.clone();
        ensure!(directory.kind == "directory", "NOT_DIRECTORY");
        if restart || self.handles[&h].directory.is_none() {
            let mut children = self.store.children(&directory.id)?;
            children.sort_by_key(|entry| entry.name.to_lowercase());
            let mut parent = self
                .store
                .entry(&directory.parent)
                .or_else(|_| self.store.lookup(""))?;
            parent.name = "..".into();
            let mut directory = directory;
            directory.name = ".".into();
            let mut cut = Vec::with_capacity(children.len() + 2);
            cut.push((directory, 0));
            cut.push((parent, 0));
            for entry in children {
                let size = entry
                    .content
                    .as_ref()
                    .map(|object| self.store.object_size(object))
                    .transpose()?
                    .unwrap_or(0);
                cut.push((entry, usize::try_from(size)?));
            }
            self.handles.get_mut(&h).unwrap().directory = Some(cut);
        }
        Ok(self.handles[&h]
            .directory
            .as_ref()
            .unwrap()
            .get(ordinal)
            .cloned())
    }
    pub fn write(
        &mut self,
        h: u64,
        offset: u64,
        bytes: &[u8],
        append: bool,
        constrained: bool,
    ) -> Result<usize> {
        let handle = self.handles.get(&h).context("INVALID_HANDLE")?;
        ensure!(
            handle.writable && !handle.cleaned && handle.generation == self.store.generation,
            "STALE_VIEW_OR_ACCESS_DENIED"
        );
        let session = self.sessions.get_mut(&handle.entity).unwrap();
        ensure!(
            session.entry.kind == "file" && session.entry.id != "control",
            "IS_DIRECTORY"
        );
        let start = if append {
            session.bytes.len()
        } else {
            usize::try_from(offset)?
        };
        let mut count = bytes.len();
        if constrained {
            count = count.min(session.bytes.len().saturating_sub(start));
        }
        if count == 0 {
            return Ok(0);
        }
        let end = start.checked_add(count).context("FILE_TOO_LARGE")?;
        ensure!(end as u64 <= MAX_CONTENT, "FILE_TOO_LARGE");
        if end > session.bytes.len() {
            session.bytes.resize(&self.store.root, end)?;
        }
        session
            .bytes
            .write_at(&self.store.root, start, &bytes[..count])?;
        session.preserve_times = handle.time_update_disabled;
        session.dirty = true;
        Ok(count)
    }
    pub fn truncate(&mut self, h: u64, size: u64, allocation: bool) -> Result<()> {
        let handle = self.handles.get(&h).context("INVALID_HANDLE")?;
        ensure!(handle.writable && !handle.cleaned, "PERMISSION_DENIED");
        let session = self.sessions.get_mut(&handle.entity).unwrap();
        ensure!(session.entry.kind == "file", "IS_DIRECTORY");
        ensure!(size <= MAX_CONTENT, "FILE_TOO_LARGE");
        if !allocation || size < (session.bytes.len() as u64) {
            session.bytes.resize(&self.store.root, size as usize)?;
            session.preserve_times = handle.time_update_disabled;
            session.dirty = true;
        }
        Ok(())
    }
    pub fn flush(&mut self, h: u64) -> Result<()> {
        if h == 0 {
            let entities: Vec<_> = self.sessions.keys().cloned().collect();
            for entity in entities {
                self.flush_entity(&entity)?;
            }
            // A volume flush can commit a retained closed session before the
            // periodic retry. Retire it before another view can reuse its ID.
            self.sessions
                .retain(|_, session| session.references > 0 || session.dirty);
            return Ok(());
        }
        let entity = self
            .handles
            .get(&h)
            .context("INVALID_HANDLE")?
            .entity
            .clone();
        self.flush_entity(&entity)
    }
    /// Metadata-only durability never publishes an unrelated dirty content buffer.
    pub fn set_basic(&mut self, h: u64, attributes: u32, times: [u64; 4]) -> Result<()> {
        let entry = self.session(h)?.entry.clone();
        ensure!(entry.id != "control", "PERMISSION_DENIED");
        ensure!(entry.id != ROOT, "UNSUPPORTED_FS_OPERATION: root metadata");
        let current = self.store.entry(&entry.id)?;
        let mut basic = current.basic_info();
        if attributes != u32::MAX {
            basic.attributes = BasicInfo::attributes(&entry.kind, attributes)?;
        }
        let mut disabled = self.handles[&h].time_update_disabled;
        for (i, value) in times.into_iter().enumerate() {
            let bit = if i == 0 { 0 } else { 1 << (i - 1) };
            match value {
                0 => {}
                u64::MAX if i != 0 => disabled |= bit,
                v if v == u64::MAX - 1 && i != 0 => disabled &= !bit,
                v if v <= i64::MAX as u64 => match i {
                    0 => basic.creation_time = v,
                    1 => basic.access_time = v,
                    2 => basic.write_time = v,
                    _ => basic.change_time = v,
                },
                _ => bail!("INVALID_TIMESTAMP"),
            }
        }
        self.store.set_basic(&entry.id, basic)?;
        self.handles.get_mut(&h).unwrap().time_update_disabled = disabled;
        let s = self.sessions.get_mut(&entry.id).unwrap();
        s.entry.basic = Some(basic);
        if s.dirty {
            if times[2] != 0 {
                s.preserve_times |= 2;
            }
            if times[3] != 0 {
                s.preserve_times |= 4;
            }
        }
        Ok(())
    }
    pub fn cleanup(&mut self, h: u64, delete: bool) -> Result<()> {
        self.flush(h)?;
        if delete {
            let entity = self
                .handles
                .get(&h)
                .context("INVALID_HANDLE")?
                .entity
                .clone();
            self.store.delete(&entity)?;
        }
        self.handles.get_mut(&h).context("INVALID_HANDLE")?.cleaned = true;
        Ok(())
    }
    pub fn close(&mut self, h: u64) -> Result<()> {
        let result = self.flush(h);
        if let Some(handle) = self.handles.remove(&h) {
            let s = self.sessions.get_mut(&handle.entity).unwrap();
            s.references -= 1;
            if s.references == 0 && !s.dirty {
                self.sessions.remove(&handle.entity);
            }
        }
        result
    }
    pub fn quiet(&self) -> Result<()> {
        ensure!(
            self.handles.is_empty()
                && self.health.is_none()
                && !self.sessions.values().any(|s| s.dirty),
            "BUSY_VIEW: {} open contexts, {} unsaved files; {}",
            self.handles.len(),
            self.sessions.values().filter(|s| s.dirty).count(),
            self.health.as_deref().unwrap_or("healthy")
        );
        Ok(())
    }
    pub fn receive_peer(
        &mut self,
        bundle: Bundle,
        allowed: &BTreeSet<String>,
    ) -> Result<Vec<String>> {
        self.receive_peer_budget(bundle, allowed, None)
    }
    pub(crate) fn receive_peer_budget(
        &mut self,
        bundle: Bundle,
        allowed: &BTreeSet<String>,
        budget: Option<usize>,
    ) -> Result<Vec<String>> {
        let before = self.store.projection(&self.store.active)?;
        let accepted = self.store.receive_with_budget(bundle, allowed, budget)?;
        let after = self.store.projection(&self.store.active)?;
        self.notifications
            .extend(namespace_notifications(&before, &after));
        // Existing writer and mapping bases are deliberately pinned. Even a
        // clean writer may have kernel-buffered writes not yet delivered here.
        let writers: BTreeSet<_> = self
            .handles
            .values()
            .filter(|h| h.writable)
            .map(|h| h.entity.clone())
            .collect();
        for (entity, session) in &mut self.sessions {
            if let Some(entry) = after.entries.get(entity).filter(|e| e.alive) {
                // Metadata follows the canonical register without rebasing a
                // writer's pinned content buffer or existence/content ancestry.
                session.entry.basic = entry.basic;
            }
            if !session.dirty
                && !writers.contains(entity)
                && let Some(entry) = after.entries.get(entity).filter(|e| e.alive)
            {
                session.bytes =
                    crate::staging::Staged::from_object(&self.store, entry.content.as_deref())?;
                session.content_base = entry.revisions.get("content").cloned();
                session.alive_base = entry.revisions.get("alive").cloned();
            }
        }
        Ok(accepted)
    }
}
