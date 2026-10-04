//! One owner process, shared local write staging, control RPC and encrypted peers.
use crate::core::*;
use aes_gcm::{
    Aes256Gcm, KeyInit, Nonce,
    aead::{Aead, Payload},
};
use anyhow::{Context, Result, bail, ensure};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
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
    pub peer: Option<PeerConfig>,
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
            peer: None,
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
    fn retain_pending(&mut self, entity: &str, error: &anyhow::Error) -> Result<()> {
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
        let before = self.store.projection(&self.store.active)?;
        let accepted = self.store.receive(bundle, allowed)?;
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
    pub fn control(&mut self, request: &str, payload: &Value) -> Result<Value> {
        if let Some(value) = self.store.receipt(request, payload)? {
            return Ok(value);
        }
        if let Some(g) = payload["generation"].as_u64() {
            ensure!(g == self.store.generation, "STALE_VIEW");
        }
        let op = payload["op"].as_str().context("INVALID_REQUEST")?;
        let text =
            |key: &str| -> Result<&str> { payload[key].as_str().context("MISSING_ARGUMENT") };
        match op {
            "status" => {
                let mut s = self.store.status()?;
                s["open_handles"] = json!(self.handles.len());
                s["unflushed_files"] = json!(self.sessions.values().filter(|s| s.dirty).count());
                s["health"] = json!(self.health);
                s["notification_error"] = json!(self.notification_error);
                s["pending_notifications"] = json!(self.notifications.len());
                s["last_sync"] = json!(self.last_sync);
                s["caught_up"] = json!(
                    self.last_sync
                        .as_ref()
                        .is_some_and(|v| v["caught_up"] == true)
                        && s["outgoing_unacknowledged"] == 0
                        && s["incoming_pending"] == 0
                        && s["pending_causal_events"] == json!([])
                        && s["unflushed_files"] == 0
                        && self.health.is_none()
                        && if let Some(peer) = &self.peer {
                            self.store.shared_event_inventory()?
                                == self.store.known_by_peer(&peer.peer)?
                        } else {
                            false
                        }
                );
                s["upload_state"] = json!(if !self.store.branch(&self.store.active)?.shared {
                    "local-only"
                } else if self.peer.is_none() {
                    "peer-not-configured"
                } else if s["outgoing_unacknowledged"] != 0 {
                    "queued"
                } else {
                    "peer-acknowledged"
                });
                return Ok(s);
            }
            "branches" => return Ok(json!(self.store.branches()?)),
            "conflicts" => return Ok(json!(self.store.conflicts()?)),
            "history" => return self.store.history(),
            "bucket-export" => {
                bail!("BUCKET_EXPORT_REQUIRES_PATH_VALIDATION");
            }
            "state" => return Ok(json!(self.store.projection(&self.store.active)?)),
            "cat-object" => {
                return Ok(json!({"hex":hex::encode(self.store.read_object(text("object")?)?)}));
            }
            "events" => return Ok(json!(self.store.events()?)),
            _ => {}
        }
        self.quiet()?;
        let before = self.store.projection(&self.store.active)?;
        // Semantic mutation and retry receipt share an outer savepoint. All inner
        // journal/current-state/outbox changes commit together with the receipt.
        self.store.db.execute_batch("SAVEPOINT control_request")?;
        let old_active = self.store.active.clone();
        let old_generation = self.store.generation;
        let result = (|| -> Result<Value> {
            let result = match op {
                "branch" => json!(self.store.fork(text("name")?, false)?),
                "publish" => {
                    ensure!(
                        payload["current_state_only"] == true,
                        "CURRENT_STATE_PUBLICATION_REQUIRES_EXPLICIT_FLAG"
                    );
                    json!(self.store.fork(text("name")?, true)?)
                }
                "checkout" => {
                    ensure!(!self.mounted, "MOUNT_REBIND_REQUIRED");
                    self.store.checkout(text("name")?)?;
                    json!({"branch":self.store.active,"generation":self.store.generation})
                }
                "checkpoint" => json!({"checkpoint":self.store.checkpoint(text("message")?)?}),
                "resolve" => {
                    let conflicts: Vec<String> =
                        serde_json::from_value(payload["conflicts"].clone())?;
                    json!({"revision":self.store.resolve(&conflicts,text("revision")?)?})
                }
                "recover" => {
                    let conflicts: Vec<String> =
                        serde_json::from_value(payload["conflicts"].clone())?;
                    json!({"revision":self.store.recover(text("entity")?,text("target")?,&conflicts)?})
                }
                "restore" => json!(self.store.restore(text("checkpoint")?, text("name")?)?),
                // Import is intentionally RPC: the CLI never opens SQLite or CAS.
                "import" => {
                    let path = text("path")?;
                    let bytes = hex::decode(text("hex")?)?;
                    let e = match self.store.lookup(path) {
                        Ok(e) => e,
                        Err(_) => self.store.create(path, "file")?,
                    };
                    json!({"revision":self.store.write_revision(&e.id,e.revisions.get("content").cloned(),&bytes)?})
                }
                "mkdir" => json!(self.store.create(text("path")?, "directory")?),
                "rename" => {
                    let e = self.store.lookup(text("path")?)?;
                    self.store
                        .rename(&e.id, text("target")?, payload["replace"] == true)?;
                    json!({"entity":e.id})
                }
                "delete" => {
                    let e = self.store.lookup(text("path")?)?;
                    self.store.delete(&e.id)?;
                    json!({"entity":e.id})
                }
                _ => bail!("UNKNOWN_OPERATION"),
            };
            self.store.db.execute(
                "INSERT INTO receipts VALUES(?,?,?)",
                rusqlite::params![
                    request,
                    hash(&serde_json::to_vec(payload)?),
                    serde_json::to_string(&result)?
                ],
            )?;
            Ok(result)
        })();
        if result.is_ok() {
            self.store.db.execute_batch("RELEASE control_request")?;
            fault("receipt_committed");
            if op != "checkout" {
                self.notifications.extend(namespace_notifications(
                    &before,
                    &self.store.projection(&self.store.active)?,
                ));
            } else {
                self.notifications.clear();
                self.notification_error = None;
            }
        } else {
            self.store
                .db
                .execute_batch("ROLLBACK TO control_request; RELEASE control_request")?;
            self.store.active = old_active;
            self.store.generation = old_generation;
        }
        result
    }
}
pub type Shared = Arc<Mutex<Engine>>;
pub const MAX_FRAME: usize = 64 * 1024 * 1024;
/// Format 5 includes durable basic metadata and portable POSIX permissions.
/// Never negotiate down or acknowledge events a peer cannot project.
pub use crate::core::PEER_FORMAT;
pub fn send_frame(stream: &mut impl Write, bytes: &[u8]) -> Result<()> {
    ensure!(bytes.len() <= MAX_FRAME, "FRAME_TOO_LARGE");
    stream.write_all(&(bytes.len() as u32).to_be_bytes())?;
    stream.write_all(bytes)?;
    Ok(())
}
pub fn read_frame(stream: &mut impl Read) -> Result<Vec<u8>> {
    let mut len = [0; 4];
    stream.read_exact(&mut len)?;
    let count = u32::from_be_bytes(len) as usize;
    ensure!(count <= MAX_FRAME, "FRAME_TOO_LARGE");
    let mut bytes = vec![0; count];
    stream.read_exact(&mut bytes)?;
    Ok(bytes)
}
pub fn stream(address: &str) -> Result<TcpStream> {
    let mut addresses = std::net::ToSocketAddrs::to_socket_addrs(address)?;
    let s = TcpStream::connect_timeout(
        &addresses.next().context("INVALID_ADDRESS")?,
        Duration::from_secs(3),
    )?;
    s.set_read_timeout(Some(Duration::from_secs(15)))?;
    s.set_write_timeout(Some(Duration::from_secs(15)))?;
    Ok(s)
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Discovery {
    pub format: u32,
    pub address: String,
    pub token: String,
    pub state: String,
    pub mount: Option<String>,
    pub repo: String,
    pub device: String,
}
pub fn rpc(info: &Discovery, request: &str, payload: Value) -> Result<Value> {
    #[cfg(windows)]
    let response: Value = crate::local_ipc::call_timeout(
        info.address
            .strip_prefix("pipe:")
            .context("WINDOWS_CONTROL_REQUIRES_OWNER_PIPE")?,
        &json!({"token":info.token,"request":request,"payload":payload}),
        20000,
    )?;
    #[cfg(not(windows))]
    let response = {
        #[cfg(target_os = "linux")]
        let mut s = unix_control_stream(&info.address)?;
        #[cfg(not(any(windows, target_os = "linux")))]
        let mut s = stream(&info.address)?;
        send_frame(
            &mut s,
            &serde_json::to_vec(&json!({"token":info.token,"request":request,"payload":payload}))?,
        )?;
        let response: Value = serde_json::from_slice(&read_frame(&mut s)?)?;
        response
    };
    if response["ok"] != true {
        bail!("{}", response["error"].as_str().unwrap_or("RPC_ERROR"));
    }
    Ok(response["result"].clone())
}
pub fn start_rpc(engine: Shared, state: &Path, mount: Option<String>) -> Result<Discovery> {
    #[cfg(windows)]
    crate::local_ipc::protect_directory(state)?;
    #[cfg(not(any(windows, target_os = "linux")))]
    let listener = TcpListener::bind("127.0.0.1:0")?;
    #[cfg(windows)]
    let pipe_name = format!("tkfs-worker-{}", id());
    #[cfg(windows)]
    let mut listener = crate::local_ipc::Listener::bind(&pipe_name)?;
    #[cfg(target_os = "linux")]
    let listener = bind_unix_control(state)?;
    let recovery = engine.clone();
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(Duration::from_secs(1));
            if let Ok(mut engine) = recovery.try_lock() {
                let _ = engine.retry_pending();
                engine.publish_observation();
            }
            deliver_notifications(&recovery);
        }
    });
    let info = {
        let e = engine.lock().unwrap();
        Discovery {
            format: 1,
            #[cfg(not(any(windows, target_os = "linux")))]
            address: listener.local_addr()?.to_string(),
            #[cfg(windows)]
            address: format!("pipe:{pipe_name}"),
            #[cfg(target_os = "linux")]
            address: format!("unix:{}", state.join("control.sock").display()),
            token: id(),
            state: state.to_string_lossy().into_owned(),
            mount,
            repo: e.store.repo.clone(),
            device: e.store.device.clone(),
        }
    };
    engine.lock().unwrap().discovery = serde_json::to_vec(&info)?;
    std::fs::write(
        state.join("runtime.json"),
        serde_json::to_vec_pretty(&info)?,
    )?;
    let token = info.token.clone();
    let observation = engine.lock().unwrap().observation.clone();
    std::thread::spawn(move || {
        #[cfg(windows)]
        loop {
            match listener.receive::<Value>() {
                Ok(Some(request)) => {
                    let result = dispatch_rpc(&engine, &observation, &token, &request);
                    deliver_notifications(&engine);
                    let reply = match result {
                        Ok(value) => json!({"ok":true,"result":value}),
                        Err(error) => json!({"ok":false,"error":format!("{error:#}")}),
                    };
                    let _ = listener.reply(&reply);
                }
                Ok(None) => {}
                Err(error) => eprintln!("owner pipe receive: {error:#}"),
            }
        }
        #[cfg(not(windows))]
        for incoming in listener.incoming() {
            match incoming {
                Ok(mut s) => {
                    let result = (|| -> Result<Value> {
                        #[cfg(target_os = "linux")]
                        authorize_unix(&s)?;
                        s.set_read_timeout(Some(Duration::from_secs(10)))?;
                        s.set_write_timeout(Some(Duration::from_secs(15)))?;
                        let r: Value = serde_json::from_slice(&read_frame(&mut s)?)?;
                        dispatch_rpc(&engine, &observation, &token, &r)
                    })();
                    deliver_notifications(&engine);
                    let reply = match result {
                        Ok(value) => json!({"ok":true,"result":value}),
                        Err(e) => json!({"ok":false,"error":format!("{e:#}")}),
                    };
                    let _ = send_frame(&mut s, &serde_json::to_vec(&reply).unwrap());
                    #[cfg(target_os = "linux")]
                    if reply["result"]["stopped"] == true {
                        std::process::exit(0);
                    }
                }
                Err(e) => eprintln!("rpc accept: {e}"),
            }
        }
    });
    Ok(info)
}

fn dispatch_rpc(
    engine: &Shared,
    observation: &Arc<Mutex<Value>>,
    token: &str,
    r: &Value,
) -> Result<Value> {
    ensure!(r["token"] == token, "PERMISSION_DENIED");
    let request = r["request"].as_str().context("INVALID_REQUEST_ID")?;
    let payload = &r["payload"];
    if payload["op"]
        .as_str()
        .is_some_and(|op| op.starts_with("published-"))
    {
        return crate::paired_sync::worker_control(&mut engine.lock().unwrap(), payload);
    }
    if payload["op"] == "status" {
        if let Ok(mut e) = engine.try_lock() {
            return e.control(request, payload);
        }
        let mut status = observation.lock().unwrap().clone();
        status["busy"] = json!(true);
        status["stale"] = json!(true);
        return Ok(status);
    }
    if payload["op"] == "sync" {
        let config = engine
            .lock()
            .unwrap()
            .peer
            .clone()
            .context("PEER_NOT_CONFIGURED")?;
        let value = sync_once(engine, &config)?;
        return Ok(value);
    }
    #[cfg(any(windows, target_os = "linux"))]
    if payload["op"] == "checkout" && engine.lock().unwrap().mounted {
        return crate::mount::checkout(engine, request, payload);
    }
    #[cfg(target_os = "linux")]
    if payload["op"] == "stop" {
        if engine.lock().unwrap().mounted {
            crate::mount::stop(engine)?;
        } else {
            engine.lock().unwrap().quiet()?;
        }
        return Ok(json!({"stopped":true}));
    }
    if payload["op"] == "bucket-export" {
        return export_bucket(engine, payload);
    }
    engine.lock().unwrap().control(request, payload)
}
#[derive(Clone)]
pub struct PeerConfig {
    pub listen: String,
    pub address: String,
    pub peer: String,
    pub key: [u8; 32],
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PeerMessage {
    format: u32,
    sender: String,
    receiver: String,
    request: String,
    reply_to: Option<String>,
    known: BTreeSet<String>,
    known_objects: BTreeSet<String>,
    #[serde(default)]
    partial_objects: BTreeMap<String, u64>,
    #[serde(default)]
    object_parts: BTreeMap<String, ObjectPart>,
    pending_events: usize,
    bundle: Option<Bundle>,
    ack: Vec<String>,
}
pub fn encrypt(key: &[u8; 32], bytes: &[u8]) -> Result<Vec<u8>> {
    let mut nonce = [0; 12];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    let cipher = Aes256Gcm::new_from_slice(key).unwrap();
    let mut out = nonce.to_vec();
    out.extend(
        cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: bytes,
                    aad: b"tkfs-peer-v1",
                },
            )
            .map_err(|_| anyhow::anyhow!("ENCRYPTION_FAILED"))?,
    );
    Ok(out)
}
pub fn decrypt(key: &[u8; 32], bytes: &[u8]) -> Result<Vec<u8>> {
    ensure!(bytes.len() >= 28, "INVALID_ENVELOPE");
    Aes256Gcm::new_from_slice(key)
        .unwrap()
        .decrypt(
            Nonce::from_slice(&bytes[..12]),
            Payload {
                msg: &bytes[12..],
                aad: b"tkfs-peer-v1",
            },
        )
        .map_err(|_| anyhow::anyhow!("AUTHENTICATION_FAILED"))
}
fn check_peer(e: &Engine, c: &PeerConfig, message: &PeerMessage) -> Result<()> {
    ensure!(
        message.format == PEER_FORMAT,
        "INCOMPATIBLE_PEER_FORMAT: both peers require the metadata-capable build"
    );
    ensure!(
        message.sender == c.peer && message.receiver == e.store.device,
        "UNAUTHORIZED_DEVICE"
    );
    ensure!(
        message
            .ack
            .iter()
            .all(|event| message.known.contains(event)),
        "INVALID_PEER_ACK"
    );
    Ok(())
}
pub fn start_peer(engine: Shared, config: PeerConfig) -> Result<()> {
    let listener = TcpListener::bind(&config.listen)?;
    engine.lock().unwrap().peer = Some(config.clone());
    let server = engine.clone();
    let sc = config.clone();
    std::thread::spawn(move || {
        for mut s in listener.incoming().flatten() {
            let result = (|| -> Result<PeerMessage> {
                s.set_read_timeout(Some(Duration::from_secs(15)))?;
                s.set_write_timeout(Some(Duration::from_secs(15)))?;
                let message: PeerMessage =
                    serde_json::from_slice(&decrypt(&sc.key, &read_frame(&mut s)?)?)?;
                let mut e = server.lock().unwrap();
                check_peer(&e, &sc, &message)?;
                ensure!(message.reply_to.is_none(), "INVALID_PEER_REQUEST");
                let allowed = BTreeSet::from([e.store.device.clone(), sc.peer.clone()]);
                let ack = if let Some(bundle) = message.bundle {
                    e.store
                        .receive_parts(message.object_parts, &bundle.events, &allowed)?;
                    e.receive_peer(bundle, &allowed)?
                } else {
                    vec![]
                };
                e.store.acknowledge(&message.ack)?;
                e.store.remember_peer_inventory(&sc.peer, &message.known)?;
                e.store
                    .remember_peer_objects(&sc.peer, &message.known_objects)?;
                e.store
                    .remember_peer_partials(&sc.peer, &message.partial_objects)?;
                let bundle = e.store.shared_page(
                    &message.known,
                    &message.known_objects,
                    MAX_FRAME - 12 * 1024 * 1024,
                )?;
                let object_parts = e.store.shared_parts(
                    &bundle,
                    &message.known_objects,
                    &message.partial_objects,
                    MAX_FRAME - 8 * 1024 * 1024,
                )?;
                Ok(PeerMessage {
                    format: PEER_FORMAT,
                    sender: e.store.device.clone(),
                    receiver: sc.peer.clone(),
                    request: id(),
                    reply_to: Some(message.request),
                    known: e.store.shared_event_inventory()?,
                    known_objects: e.store.shared_object_inventory()?,
                    partial_objects: e.store.partial_objects()?,
                    object_parts,
                    pending_events: e.store.db.query_row(
                        "SELECT count(*) FROM incoming_events",
                        [],
                        |r| r.get(0),
                    )?,
                    bundle: Some(bundle),
                    ack,
                })
            })();
            deliver_notifications(&server);
            match result {
                Ok(r) => {
                    if let Ok(bytes) = serde_json::to_vec(&r).and_then(|bytes| {
                        encrypt(&sc.key, &bytes).map_err(serde::ser::Error::custom)
                    }) {
                        let _ = send_frame(&mut s, &bytes);
                    }
                }
                Err(e) => eprintln!("peer receive: {e:#}"),
            }
        }
    });
    std::thread::spawn(move || {
        loop {
            let _ = sync_once(&engine, &config);
            std::thread::sleep(Duration::from_secs(1));
        }
    });
    Ok(())
}
pub fn sync_once(engine: &Shared, c: &PeerConfig) -> Result<Value> {
    let result = exchange_once(engine, c);
    engine.lock().unwrap().last_sync = Some(match &result {
        Ok(value) => value.clone(),
        Err(error) => json!({"caught_up":false,"error":format!("{error:#}")}),
    });
    deliver_notifications(engine);
    result
}
fn exchange_once(engine: &Shared, c: &PeerConfig) -> Result<Value> {
    let request = id();
    let message = {
        let e = engine.lock().unwrap();
        let known = e.store.known_by_peer(&c.peer)?;
        let known_objects = e.store.known_objects_by_peer(&c.peer)?;
        let offsets = e.store.peer_partial_objects(&c.peer)?;
        let bundle = e
            .store
            .shared_page(&known, &known_objects, MAX_FRAME - 12 * 1024 * 1024)?;
        let object_parts = e.store.shared_parts(
            &bundle,
            &known_objects,
            &offsets,
            MAX_FRAME - 8 * 1024 * 1024,
        )?;
        PeerMessage {
            format: PEER_FORMAT,
            sender: e.store.device.clone(),
            receiver: c.peer.clone(),
            request: request.clone(),
            reply_to: None,
            known: e.store.shared_event_inventory()?,
            known_objects: e.store.shared_object_inventory()?,
            partial_objects: e.store.partial_objects()?,
            object_parts,
            pending_events: e.store.db.query_row(
                "SELECT count(*) FROM incoming_events",
                [],
                |r| r.get(0),
            )?,
            bundle: Some(bundle),
            ack: vec![],
        }
    };
    // No SQLite or runtime mutex is held while connecting or transferring.
    let mut s = stream(&c.address)?;
    send_frame(&mut s, &encrypt(&c.key, &serde_json::to_vec(&message)?)?)?;
    let response: PeerMessage = serde_json::from_slice(&decrypt(&c.key, &read_frame(&mut s)?)?)?;
    let mut e = engine.lock().unwrap();
    check_peer(&e, c, &response)?;
    ensure!(
        response.reply_to.as_ref() == Some(&request),
        "UNBOUND_PEER_RESPONSE"
    );
    let allowed = BTreeSet::from([e.store.device.clone(), c.peer.clone()]);
    if let Some(bundle) = response.bundle {
        e.store
            .receive_parts(response.object_parts, &bundle.events, &allowed)?;
        e.receive_peer(bundle, &allowed)?;
    }
    fault("peer_ack_received");
    e.store.acknowledge(&response.ack)?;
    e.store.remember_peer_inventory(&c.peer, &response.known)?;
    e.store
        .remember_peer_objects(&c.peer, &response.known_objects)?;
    e.store
        .remember_peer_partials(&c.peer, &response.partial_objects)?;
    let local = e.store.shared_event_inventory()?;
    let pending: usize = e
        .store
        .db
        .query_row("SELECT count(*) FROM incoming_events", [], |r| r.get(0))?;
    let result = json!({"caught_up_at":time(),"caught_up":local==response.known&&pending==0&&response.pending_events==0&&e.store.outgoing_unacknowledged()?==0&&e.store.projection(&e.store.active)?.pending.is_empty()&&e.health.is_none()&&!e.sessions.values().any(|s|s.dirty),"peer":c.peer,"durability":"peer SQLite+verified objects"});
    Ok(result)
}

fn visible_paths(p: &Projection) -> BTreeMap<String, Entry> {
    let mut result = BTreeMap::new();
    for e in p.entries.values().filter(|e| e.alive) {
        let mut parts = vec![e.name.clone()];
        let mut parent = e.parent.clone();
        let mut seen = BTreeSet::new();
        while parent != ROOT && seen.insert(parent.clone()) {
            let Some(dir) = p.entries.get(&parent) else {
                break;
            };
            parts.push(dir.name.clone());
            parent = dir.parent.clone();
        }
        parts.reverse();
        result.insert(format!("\\{}", parts.join("\\")), e.clone());
    }
    result
}
pub fn namespace_notifications(before: &Projection, after: &Projection) -> Vec<(String, u32)> {
    let a = visible_paths(before);
    let b = visible_paths(after);
    let mut out = vec![];
    for (path, old) in &a {
        match b.get(path) {
            None => out.push((path.clone(), 2)),
            Some(new) if old.id != new.id => {
                out.push((path.clone(), 2));
                out.push((path.clone(), 1));
            }
            Some(new)
                if old.content != new.content
                    || old.modified_ms != new.modified_ms
                    || old.basic_info() != new.basic_info() =>
            {
                out.push((path.clone(), 3))
            }
            _ => {}
        }
    }
    for path in b.keys() {
        if !a.contains_key(path) {
            out.push((path.clone(), 1));
        }
    }
    out
}
pub fn deliver_notifications(engine: &Shared) {
    #[cfg(windows)]
    {
        let pending = {
            let Ok(mut e) = engine.try_lock() else {
                return;
            };
            if !e.mounted || e.switching {
                return;
            }
            std::mem::take(&mut e.notifications)
        };
        if pending.is_empty() {
            return;
        }
        let mut retry = vec![];
        let mut error = None;
        for (path, action) in pending {
            if let Err(e) = crate::mount::notify(&path, action) {
                error = Some(format!("{e:#}"));
                retry.push((path, action));
            }
        }
        let mut e = engine.lock().unwrap();
        e.notifications.extend(retry);
        e.notification_error = error;
    }
    #[cfg(target_os = "linux")]
    {
        // The Linux adapter uses zero entry/attribute TTLs and direct file I/O.
        if let Ok(mut e) = engine.try_lock() {
            e.notifications.clear();
            e.notification_error = None;
        }
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = engine;
    }
}
fn export_bucket(engine: &Shared, payload: &Value) -> Result<Value> {
    use crate::backend::{DirectoryBackend, ObjectBackend};
    let requested = std::path::absolute(Path::new(
        payload["directory"].as_str().context("MISSING_DIRECTORY")?,
    ))?;
    let (state, mount) = {
        let e = engine.lock().unwrap();
        let info: Discovery = serde_json::from_slice(&e.discovery)?;
        (e.store.root.clone(), info.mount)
    };
    let normalized = |p: &std::path::Path| {
        p.to_string_lossy()
            .trim_start_matches("\\\\?\\")
            .replace('/', "\\")
            .trim_end_matches('\\')
            .to_uppercase()
    };
    let overlaps = |a: &std::path::Path, b: &std::path::Path| {
        let a = normalized(a);
        let b = normalized(b);
        a == b || a.starts_with(&(b.clone() + "\\")) || b.starts_with(&(a + "\\"))
    };
    ensure!(
        !overlaps(&requested, &state),
        "BUCKET_MUST_BE_OUTSIDE_STATE"
    );
    if let Some(mount) = &mount {
        ensure!(
            !overlaps(&requested, Path::new(mount)),
            "BUCKET_MUST_BE_OUTSIDE_MOUNT"
        );
    }
    let mut ancestor = requested.as_path();
    let mut tail = vec![];
    while !ancestor.exists() {
        tail.push(
            ancestor
                .file_name()
                .context("INVALID_DIRECTORY")?
                .to_owned(),
        );
        ancestor = ancestor.parent().context("INVALID_DIRECTORY")?;
    }
    let mut destination =
        std::fs::canonicalize(ancestor).context("BUCKET_REQUIRES_NATIVE_DIRECTORY")?;
    for name in tail.into_iter().rev() {
        destination.push(name);
    }
    let state = std::fs::canonicalize(state)?;
    ensure!(
        !destination.starts_with(&state) && !state.starts_with(&destination),
        "BUCKET_MUST_BE_OUTSIDE_STATE"
    );
    if let Some(mount) = mount {
        let mount = Path::new(&mount);
        let mount = std::fs::canonicalize(mount.parent().context("FOLDER_MOUNT_REQUIRED")?)?
            .join(mount.file_name().context("FOLDER_MOUNT_REQUIRED")?);
        ensure!(
            !destination.starts_with(&mount) && !mount.starts_with(&destination),
            "BUCKET_MUST_BE_OUTSIDE_MOUNT"
        );
    }
    let backend = DirectoryBackend::new(&destination)?;
    // Capture only authorized immutable data under the lock; backend I/O outside.
    let (repo, branches, events, objects) = {
        let e = engine.lock().unwrap();
        let events: Vec<_> = e
            .store
            .events()?
            .into_iter()
            .filter(|e| e.branch.shared)
            .collect();
        let objects: BTreeSet<_> = events.iter().flat_map(Event::objects).collect();
        (
            e.store.repo.clone(),
            e.store
                .branches()?
                .into_iter()
                .filter(|b| b.shared)
                .collect::<Vec<_>>(),
            events,
            objects,
        )
    };
    for object in &objects {
        let mut file = {
            let e = engine.lock().unwrap();
            e.store.open_object(object)?
        };
        ensure!(
            backend.put_stream(&mut file)? == *object,
            "BACKEND_HASH_MISMATCH"
        );
    }
    let manifest = json!({"format":"tkfs-shared-export-1","repo":repo,"branches":branches,"events":events,"objects":objects});
    let hash = backend.put_verified(&serde_json::to_vec(&manifest)?)?;
    Ok(
        json!({"backend":"local-directory-test-bucket","manifest":hash,"objects":objects.len(),"remote_validated":false}),
    )
}

#[cfg(target_os = "linux")]
fn authorize_unix(stream: &std::os::unix::net::UnixStream) -> Result<()> {
    use std::os::fd::AsRawFd;
    let mut credentials: libc::ucred = unsafe { std::mem::zeroed() };
    let mut size = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    ensure!(
        unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut credentials as *mut libc::ucred).cast(),
                &mut size,
            )
        } == 0,
        "LOCAL_IPC_CREDENTIALS"
    );
    ensure!(
        credentials.uid == unsafe { libc::geteuid() },
        "LOCAL_IPC_OWNER_MISMATCH"
    );
    Ok(())
}
#[cfg(target_os = "linux")]
fn unix_control_stream(address: &str) -> Result<std::os::unix::net::UnixStream> {
    let stream = std::os::unix::net::UnixStream::connect(
        address
            .strip_prefix("unix:")
            .context("LINUX_CONTROL_REQUIRES_UNIX_SOCKET")?,
    )?;
    authorize_unix(&stream)?;
    stream.set_read_timeout(Some(Duration::from_secs(15)))?;
    stream.set_write_timeout(Some(Duration::from_secs(15)))?;
    Ok(stream)
}
#[cfg(target_os = "linux")]
fn bind_unix_control(state: &Path) -> Result<std::os::unix::net::UnixListener> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
    let path = state.join("control.sock");
    if let Ok(metadata) = std::fs::symlink_metadata(&path) {
        ensure!(
            metadata.file_type().is_socket() && metadata.uid() == unsafe { libc::geteuid() },
            "UNSAFE_STALE_CONTROL_SOCKET"
        );
        std::fs::remove_file(&path)?;
    }
    let listener = std::os::unix::net::UnixListener::bind(&path)?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}
