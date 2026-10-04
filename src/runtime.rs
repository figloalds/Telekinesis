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
    pub bytes: Vec<u8>,
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
}
pub struct Engine {
    pub store: Store,
    pub sessions: BTreeMap<String, Session>,
    pub handles: BTreeMap<u64, Handle>,
    pub next_handle: u64,
    pub discovery: Vec<u8>,
    pub health: Option<String>,
    pub switching: bool,
    pub mounted: bool,
    pub last_sync: Option<Value>,
    pub peer: Option<PeerConfig>,
    pub notifications: Vec<(String, u32)>,
    pub notification_error: Option<String>,
}
impl Engine {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            sessions: BTreeMap::new(),
            handles: BTreeMap::new(),
            next_handle: 1,
            discovery: vec![],
            health: None,
            switching: false,
            mounted: false,
            last_sync: None,
            peer: None,
            notifications: vec![],
            notification_error: None,
        }
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
                self.discovery.clone()
            } else {
                entry
                    .content
                    .as_ref()
                    .map(|h| self.store.read_object(h))
                    .transpose()?
                    .unwrap_or_default()
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
            },
        );
        Ok((h, entry))
    }
    pub fn session(&self, h: u64) -> Result<&Session> {
        let handle = self.handles.get(&h).context("INVALID_HANDLE")?;
        ensure!(handle.generation == self.store.generation, "STALE_VIEW");
        self.sessions.get(&handle.entity).context("INVALID_HANDLE")
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
        ensure!(end <= MAX_FILE, "FILE_TOO_LARGE");
        if end > session.bytes.len() {
            session.bytes.resize(end, 0);
        }
        session.bytes[start..end].copy_from_slice(&bytes[..count]);
        session.preserve_times = handle.time_update_disabled;
        session.dirty = true;
        Ok(count)
    }
    pub fn truncate(&mut self, h: u64, size: u64, allocation: bool) -> Result<()> {
        let handle = self.handles.get(&h).context("INVALID_HANDLE")?;
        ensure!(handle.writable && !handle.cleaned, "PERMISSION_DENIED");
        let session = self.sessions.get_mut(&handle.entity).unwrap();
        ensure!(session.entry.kind == "file", "IS_DIRECTORY");
        ensure!(size <= MAX_FILE as u64, "FILE_TOO_LARGE");
        if !allocation || size < (session.bytes.len() as u64) {
            session.bytes.resize(size as usize, 0);
            session.preserve_times = handle.time_update_disabled;
            session.dirty = true;
        }
        Ok(())
    }
    pub fn flush(&mut self, h: u64) -> Result<()> {
        if h == 0 {
            let handles: Vec<_> = self.handles.keys().copied().collect();
            for h in handles {
                self.flush(h)?;
            }
            return Ok(());
        }
        let entity = self
            .handles
            .get(&h)
            .context("INVALID_HANDLE")?
            .entity
            .clone();
        let session = self.sessions.get_mut(&entity).unwrap();
        if session.dirty {
            let rev = self.store.write_with_base_and_times(
                &entity,
                session.content_base.clone(),
                session.alive_base.clone(),
                &session.bytes,
                session.preserve_times,
            )?;
            session.content_base = Some(rev.clone());
            session.alive_base = Some(rev);
            session.dirty = false;
            session.entry = self.store.projection(&self.store.active)?.entries[&entity].clone();
        }
        Ok(())
    }
    /// Metadata-only durability never publishes an unrelated dirty content buffer.
    pub fn set_basic(&mut self, h: u64, attributes: u32, times: [u64; 4]) -> Result<()> {
        let entry = self.session(h)?.entry.clone();
        ensure!(entry.id != "control", "PERMISSION_DENIED");
        ensure!(entry.id != ROOT, "UNSUPPORTED_FS_OPERATION: root metadata");
        let current = self
            .store
            .projection(&self.store.active)?
            .entries
            .get(&entry.id)
            .cloned()
            .context("FILE_NOT_FOUND")?;
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
        self.flush(h)?;
        if let Some(handle) = self.handles.remove(&h) {
            let s = self.sessions.get_mut(&handle.entity).unwrap();
            s.references -= 1;
            if s.references == 0 {
                self.sessions.remove(&handle.entity);
            }
        }
        Ok(())
    }
    pub fn quiet(&self) -> Result<()> {
        ensure!(
            self.handles.is_empty() && self.health.is_none(),
            "BUSY_VIEW: {} open contexts",
            self.handles.len()
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
                session.bytes = entry
                    .content
                    .as_ref()
                    .map(|h| self.store.read_object(h))
                    .transpose()?
                    .unwrap_or_default();
                session.entry = entry.clone();
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
/// Format 3 requires the durable basic metadata register. Never negotiate down:
/// a format-2 peer must not acknowledge or cache events it cannot project.
pub use crate::core::PEER_FORMAT;
pub fn send_frame(stream: &mut TcpStream, bytes: &[u8]) -> Result<()> {
    ensure!(bytes.len() <= MAX_FRAME, "FRAME_TOO_LARGE");
    stream.write_all(&(bytes.len() as u32).to_be_bytes())?;
    stream.write_all(bytes)?;
    Ok(())
}
pub fn read_frame(stream: &mut TcpStream) -> Result<Vec<u8>> {
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
    let mut s = stream(&info.address)?;
    send_frame(
        &mut s,
        &serde_json::to_vec(&json!({"token":info.token,"request":request,"payload":payload}))?,
    )?;
    let response: Value = serde_json::from_slice(&read_frame(&mut s)?)?;
    if response["ok"] != true {
        bail!("{}", response["error"].as_str().unwrap_or("RPC_ERROR"));
    }
    Ok(response["result"].clone())
}
pub fn start_rpc(engine: Shared, state: &Path, mount: Option<String>) -> Result<Discovery> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let info = {
        let e = engine.lock().unwrap();
        Discovery {
            format: 1,
            address: listener.local_addr()?.to_string(),
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
    std::thread::spawn(move || {
        for incoming in listener.incoming() {
            match incoming {
                Ok(mut s) => {
                    let result = (|| -> Result<Value> {
                        s.set_read_timeout(Some(Duration::from_secs(10)))?;
                        s.set_write_timeout(Some(Duration::from_secs(15)))?;
                        let r: Value = serde_json::from_slice(&read_frame(&mut s)?)?;
                        ensure!(r["token"] == token, "PERMISSION_DENIED");
                        let request = r["request"].as_str().context("INVALID_REQUEST_ID")?;
                        let payload = &r["payload"];
                        if payload["op"] == "sync" {
                            let config = engine
                                .lock()
                                .unwrap()
                                .peer
                                .clone()
                                .context("PEER_NOT_CONFIGURED")?;
                            let value = sync_once(&engine, &config)?;
                            return Ok(value);
                        }
                        #[cfg(windows)]
                        if payload["op"] == "checkout" && engine.lock().unwrap().mounted {
                            return crate::mount::checkout(&engine, request, payload);
                        }
                        if payload["op"] == "bucket-export" {
                            return export_bucket(&engine, payload);
                        }
                        engine.lock().unwrap().control(request, payload)
                    })();
                    deliver_notifications(&engine);
                    let reply = match result {
                        Ok(value) => json!({"ok":true,"result":value}),
                        Err(e) => json!({"ok":false,"error":format!("{e:#}")}),
                    };
                    let _ = send_frame(&mut s, &serde_json::to_vec(&reply).unwrap());
                }
                Err(e) => eprintln!("rpc accept: {e}"),
            }
        }
    });
    Ok(info)
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
                    e.receive_peer(bundle, &allowed)?
                } else {
                    vec![]
                };
                e.store.acknowledge(&message.ack)?;
                e.store.remember_peer_inventory(&sc.peer, &message.known)?;
                e.store
                    .remember_peer_objects(&sc.peer, &message.known_objects)?;
                Ok(PeerMessage {
                    format: PEER_FORMAT,
                    sender: e.store.device.clone(),
                    receiver: sc.peer.clone(),
                    request: id(),
                    reply_to: Some(message.request),
                    known: e.store.shared_event_inventory()?,
                    known_objects: e.store.shared_object_inventory()?,
                    pending_events: e.store.db.query_row(
                        "SELECT count(*) FROM incoming_events",
                        [],
                        |r| r.get(0),
                    )?,
                    bundle: Some(e.store.shared_page(
                        &message.known,
                        &message.known_objects,
                        MAX_FRAME - 8 * 1024 * 1024,
                    )?),
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
        PeerMessage {
            format: PEER_FORMAT,
            sender: e.store.device.clone(),
            receiver: c.peer.clone(),
            request: request.clone(),
            reply_to: None,
            known: e.store.shared_event_inventory()?,
            known_objects: e.store.shared_object_inventory()?,
            pending_events: e.store.db.query_row(
                "SELECT count(*) FROM incoming_events",
                [],
                |r| r.get(0),
            )?,
            bundle: Some(e.store.shared_page(
                &e.store.known_by_peer(&c.peer)?,
                &e.store.known_objects_by_peer(&c.peer)?,
                MAX_FRAME - 8 * 1024 * 1024,
            )?),
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
        e.receive_peer(bundle, &allowed)?;
    }
    fault("peer_ack_received");
    e.store.acknowledge(&response.ack)?;
    e.store.remember_peer_inventory(&c.peer, &response.known)?;
    e.store
        .remember_peer_objects(&c.peer, &response.known_objects)?;
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
            let mut e = engine.lock().unwrap();
            if !e.mounted || e.switching {
                return;
            }
            std::mem::take(&mut e.notifications)
        };
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
    #[cfg(not(windows))]
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
    let bundle = engine
        .lock()
        .unwrap()
        .store
        .shared_bundle(&BTreeSet::new())?;
    for (object, bytes) in &bundle.objects {
        ensure!(
            backend.put_verified(&hex::decode(bytes)?)? == *object,
            "BACKEND_HASH_MISMATCH"
        );
    }
    let objects: Vec<_> = bundle.objects.keys().cloned().collect();
    let manifest = json!({"format":"tkfs-shared-export-1","repo":bundle.repo,"branches":bundle.branches,"events":bundle.events,"objects":objects});
    let hash = backend.put_verified(&serde_json::to_vec(&manifest)?)?;
    Ok(
        json!({"backend":"local-directory-test-bucket","manifest":hash,"objects":objects.len(),"remote_validated":false}),
    )
}
