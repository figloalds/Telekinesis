//! Durable semantic journal and deterministic multi-value registers.
//! Receipt order is never used to choose a version. Ordinary edits parent only
//! their actual visible/base revision; only explicit review can parent competitors.
//!
//! The root keeps the data model, helpers, `Store`, its view cache, opening and
//! recovery phases, branches/events/projection access, rebuilds and every local
//! filesystem or branch operation. Per-concern `impl Store` blocks live in the
//! submodules; `pub use` keeps every historical `tkfs::core::*` path.
use crate::backend::ObjectBackend;
use anyhow::{Context, Result, bail, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    cell::{Ref, RefCell},
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Read,
    path::{Path, PathBuf},
};
use unicode_normalization::UnicodeNormalization;
use uuid::Uuid;

pub mod commit;
pub mod demand;
pub mod derive;
pub mod inbox;
pub mod objects;
pub mod sharing;
mod snapshot;
pub mod status;
pub mod transfer;
pub mod verify;

use derive::{activated_ids, persist_projection};
use inbox::{validate_direct_links, validate_known_links};

pub use derive::project;
pub(crate) use objects::install_object;
pub use objects::{ObjectLocks, VerifyMode};
pub use transfer::{Bundle, ObjectPart};
pub use verify::DerivedDiff;

pub const ROOT: &str = "root";
pub const MAX_FILE: usize = 16 * 1024 * 1024;
/// Streaming file ceiling; buffered object APIs retain their 16 MiB bound.
pub const MAX_CONTENT: u64 = 16 * 1024 * 1024 * 1024;
pub const IO_CHUNK: usize = 1024 * 1024;
pub const MAX_EVENT: usize = 8 * 1024 * 1024;
pub const MAX_NAMESPACE_CUTS: usize = 4096;
pub const FILETIME_EPOCH: u64 = 116_444_736_000_000_000;
pub const BASIC_ATTRIBUTES: u32 = 0x1 | 0x2 | 0x4 | 0x10 | 0x20 | 0x80;
/// Format 6 acknowledges metadata independently and carries content descriptors.
pub const PEER_FORMAT: u32 = 6;
pub fn filetime(ms: u64) -> u64 {
    ms.saturating_mul(10_000).saturating_add(FILETIME_EPOCH)
}
/// One bounded atomic metadata register, separate from content revision clocks.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BasicInfo {
    /// Portable permission bits; Windows retains these without interpreting execute.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub posix_mode: Option<u32>,
    pub attributes: u32,
    pub creation_time: u64,
    pub access_time: u64,
    pub write_time: u64,
    pub change_time: u64,
}
impl BasicInfo {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.posix_mode.is_none_or(|mode| mode & !0o777 == 0),
            "UNSUPPORTED_POSIX_MODE"
        );
        ensure!(
            self.attributes & !BASIC_ATTRIBUTES == 0,
            "UNSUPPORTED_ATTRIBUTES"
        );
        ensure!(
            self.attributes & 0x80 == 0 || self.attributes == 0x80,
            "INVALID_ATTRIBUTES"
        );
        ensure!(
            [
                self.creation_time,
                self.access_time,
                self.write_time,
                self.change_time
            ]
            .iter()
            .all(|v| *v <= i64::MAX as u64),
            "INVALID_TIMESTAMP"
        );
        Ok(())
    }
    pub fn attributes(kind: &str, attributes: u32) -> Result<u32> {
        ensure!(
            attributes & !BASIC_ATTRIBUTES == 0,
            "UNSUPPORTED_ATTRIBUTES"
        );
        // DIRECTORY is an immutable type fact; NORMAL is the absence of flags.
        let attributes = attributes & !(0x10 | 0x80);
        Ok(if kind == "directory" {
            attributes | 0x10
        } else if attributes == 0 {
            0x80
        } else {
            attributes
        })
    }
}
#[derive(Debug)]
struct CausalPrerequisitesPending;
impl std::fmt::Display for CausalPrerequisitesPending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CAUSAL_PREREQUISITES_PENDING")
    }
}
impl std::error::Error for CausalPrerequisitesPending {}
pub fn id() -> String {
    Uuid::new_v4().to_string()
}
pub fn hash(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
pub(crate) fn valid_object(object: &str) -> Result<()> {
    ensure!(
        object.len() == 64 && object.bytes().all(|c| c.is_ascii_hexdigit()),
        "INVALID_OBJECT_ID"
    );
    Ok(())
}
fn object_error(error: std::io::Error) -> anyhow::Error {
    let label = if error.kind() == std::io::ErrorKind::NotFound {
        "OFFLINE_OBJECT_MISSING"
    } else {
        "OBJECT_IO_ERROR"
    };
    anyhow::Error::new(error).context(label)
}
pub(crate) fn stream_hash(reader: &mut impl Read) -> Result<String> {
    let mut digest = Sha256::new();
    let mut bytes = [0; 64 * 1024];
    loop {
        let count = reader.read(&mut bytes)?;
        if count == 0 {
            break;
        }
        digest.update(&bytes[..count]);
        crate::startup::progress("verifying objects");
    }
    Ok(hex::encode(digest.finalize()))
}
pub fn time() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Branch {
    pub id: String,
    pub name: String,
    pub shared: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Change {
    pub entity: String,
    pub field: String,
    pub parents: Vec<String>,
    pub value: Value,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Event {
    pub id: String,
    pub repo: String,
    pub branch: Branch,
    pub device: String,
    pub created_ms: u64,
    pub changes: Vec<Change>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub reviewed: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub object_sizes: BTreeMap<String, u64>,
}
impl Event {
    pub fn objects(&self) -> BTreeSet<String> {
        self.changes
            .iter()
            .filter(|c| c.field == "content")
            .filter_map(|c| c.value.as_str().map(str::to_owned))
            .collect()
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Version {
    pub revision: String,
    pub device: String,
    pub created_ms: u64,
    pub parents: Vec<String>,
    pub value: Value,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Conflict {
    pub id: String,
    pub branch: String,
    pub entity: String,
    pub field: String,
    pub reason: String,
    pub alternatives: Vec<Version>,
    pub bases: Vec<Version>,
    pub resolved_by: Option<String>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Entry {
    pub id: String,
    pub kind: String,
    pub parent: String,
    pub name: String,
    pub content: Option<String>,
    pub alive: bool,
    pub revisions: BTreeMap<String, String>,
    pub modified_ms: u64,
    #[serde(default)]
    pub created_ms: u64,
    #[serde(default)]
    pub basic: Option<BasicInfo>,
}
impl Entry {
    pub fn basic_info(&self) -> BasicInfo {
        let mut basic = self.basic.unwrap_or(BasicInfo {
            posix_mode: None,
            attributes: if self.kind == "directory" { 0x10 } else { 0x20 },
            creation_time: filetime(self.created_ms),
            access_time: filetime(self.created_ms),
            write_time: filetime(self.modified_ms),
            change_time: filetime(self.modified_ms),
        });
        // The register cannot change the projected entry's immutable type.
        basic.attributes &= !(0x10 | 0x80);
        if self.kind == "directory" {
            basic.attributes |= 0x10;
        } else if basic.attributes == 0 {
            basic.attributes = 0x80;
        }
        basic
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Projection {
    pub entries: BTreeMap<String, Entry>,
    pub heads: BTreeMap<String, Vec<Version>>,
    pub conflicts: Vec<Conflict>,
    pub pending: Vec<String>,
}
pub fn register(entity: &str, field: &str) -> String {
    format!("{entity}/{field}")
}
pub fn name_key(name: &str) -> Result<String> {
    ensure!(
        !name.eq_ignore_ascii_case(".tkfs-runtime.json"),
        "INVALID_NAME: reserved runtime discovery path"
    );
    ensure!(
        !name.is_empty() && name.encode_utf16().count() <= 255,
        "INVALID_NAME"
    );
    ensure!(
        !name.ends_with(['.', ' ']) && !name.chars().any(|c| c < ' ' || "<>:\"/\\|?*".contains(c)),
        "INVALID_NAME"
    );
    let upper = name.split('.').next().unwrap().to_uppercase();
    ensure!(
        !["CON", "PRN", "AUX", "NUL"].contains(&upper.as_str())
            && !(upper.len() == 4
                && (upper.starts_with("COM") || upper.starts_with("LPT"))
                && upper.ends_with(['1', '2', '3', '4', '5', '6', '7', '8', '9'])),
        "INVALID_NAME"
    );
    Ok(name
        .nfc()
        .flat_map(char::to_lowercase)
        .collect::<String>()
        .nfc()
        .collect())
}

/// How `Store::open_existing_with` runs the recovery phases of `Store::open`.
/// Wave 0: every `VerifyMode` verifies like `Full`, and activation is never
/// deferred (`startup_drain` still activates staged events).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OpenOptions {
    /// Leave staged incoming events to a background activator (classify only).
    pub defer_activation: bool,
    /// Startup verification of referenced, locally present objects.
    pub verify: VerifyMode,
}

pub struct Store {
    pub db: Connection,
    pub root: PathBuf,
    pub repo: String,
    pub device: String,
    pub active: String,
    pub generation: u64,
    view_cache: RefCell<Option<ViewCache>>,
    /// Module-owned derived caches; see `StoreCaches`.
    #[allow(dead_code)]
    pub(crate) caches: StoreCaches,
}
/// One projection view. It is fresh only while all of these are unchanged:
/// the branch, this connection's own `total_changes`, `PRAGMA data_version`
/// (bumped only by commits of *other* connections, so secondary handles see
/// engine commits) and whether a transaction is open.
struct ViewCache {
    branch: String,
    changes: u64,
    data_version: i64,
    transactional: bool,
    projection: Projection,
    slots: BTreeMap<(String, String), String>,
}
/// The only `Store` field the revamp adds. Each module owns its `Cache` type and
/// chooses its own interior mutability; caches are derived and disposable.
#[derive(Default)]
#[allow(dead_code)]
pub(crate) struct StoreCaches {
    pub(crate) objects: objects::Cache,
    pub(crate) inbox: inbox::Cache,
    pub(crate) commit: commit::Cache,
    pub(crate) derive: derive::Cache,
    pub(crate) sharing: sharing::Cache,
}
impl Store {
    /// Initialize an operation-owned new store with identities chosen by its caller.
    /// Repeated initialization verifies the same identities; it never replaces them.
    pub fn initialize(root: &Path, repo: &str, device: &str) -> Result<Self> {
        ensure!(
            !repo.is_empty() && !device.is_empty(),
            "INVALID_STORE_IDENTITY"
        );
        fs::create_dir_all(root)?;
        let mut db = Connection::open(root.join("metadata.sqlite"))?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS config(key TEXT PRIMARY KEY,value TEXT NOT NULL);")?;
        let tx = db.transaction()?;
        for (key, expected) in [("repo", repo), ("device", device)] {
            let actual: Option<String> = tx
                .query_row("SELECT value FROM config WHERE key=?", [key], |r| r.get(0))
                .optional()?;
            if let Some(actual) = actual {
                ensure!(actual == expected, "STORE_IDENTITY_MISMATCH");
            } else {
                tx.execute("INSERT INTO config VALUES(?,?)", [key, expected])?;
            }
        }
        tx.commit()?;
        drop(db);
        Self::open(root, Some(repo))
    }

    /// Validate before invoking the legacy schema/recovery path. Missing metadata
    /// or identities must never become a newly initialized healthy replica.
    pub fn open_existing(root: &Path) -> Result<Self> {
        Self::open_existing_with(root, OpenOptions::default())
    }
    /// `open_existing` with explicit recovery options (see `OpenOptions`).
    pub fn open_existing_with(root: &Path, options: OpenOptions) -> Result<Self> {
        ensure!(
            root.join("objects").is_dir() && root.join("metadata.sqlite").is_file(),
            "STATE_NOT_INITIALIZED"
        );
        let db = Connection::open_with_flags(
            root.join("metadata.sqlite"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        for key in ["repo", "device", "active", "generation"] {
            let value: String = db
                .query_row("SELECT value FROM config WHERE key=?", [key], |r| r.get(0))
                .context("INVALID_STORE_METADATA")?;
            ensure!(!value.is_empty(), "INVALID_STORE_METADATA");
        }
        let valid: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM branches WHERE id=(SELECT value FROM config WHERE key='active'))", [], |r| r.get(0))?;
        ensure!(valid, "INVALID_SELECTED_BRANCH");
        drop(db);
        Self::open_with(root, None, options)
    }

    pub fn open(root: &Path, repo: Option<&str>) -> Result<Self> {
        Self::open_with(root, repo, OpenOptions::default())
    }
    /// Recovery as named phases, in their historical order. Every phase runs
    /// on the opening thread, so startup progress stays truthful (invariant 28),
    /// and the RPC listener can only open after all of them (invariant 24).
    fn open_with(root: &Path, repo: Option<&str>, options: OpenOptions) -> Result<Self> {
        let db = Self::open_schema(root)?;
        snapshot::initialize(&db)?;
        let mut store = Self::open_identity(db, root, repo)?;
        objects::ensure_schema(&mut store)?;
        inbox::ensure_schema(&mut store)?;
        sharing::ensure_schema(&mut store)?;
        objects::startup_object_check(&mut store, options.verify)?;
        crate::startup::progress("validating journal");
        store.evacuate_unvalidated_journal()?;
        crate::startup::progress("rebuilding frontiers");
        store.rebuild_frontiers()?;
        store.resume_or_rebuild()?;
        inbox::startup_drain(&mut store, options.defer_activation)?;
        Ok(store)
    }
    /// Phase 1: the legacy schema. Every statement is idempotent.
    fn open_schema(root: &Path) -> Result<Connection> {
        fs::create_dir_all(root.join("objects"))?;
        let db = Connection::open(root.join("metadata.sqlite"))?;
        db.busy_timeout(std::time::Duration::from_secs(5))?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS config(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS branches(id TEXT PRIMARY KEY, name TEXT NOT NULL, shared INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY, branch TEXT NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS current_state(branch TEXT PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS current_parts(branch TEXT NOT NULL,entity TEXT NOT NULL,entry TEXT NOT NULL,heads TEXT NOT NULL,PRIMARY KEY(branch,entity));
            CREATE TABLE IF NOT EXISTS conflicts(id TEXT PRIMARY KEY, branch TEXT NOT NULL, payload TEXT NOT NULL, resolved_by TEXT);
            CREATE TABLE IF NOT EXISTS outbox(event TEXT PRIMARY KEY, acknowledged INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS receipts(id TEXT PRIMARY KEY, payload_hash TEXT NOT NULL, result TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS checkpoints(id TEXT PRIMARY KEY, branch TEXT NOT NULL, object TEXT NOT NULL, message TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS peer_known(peer TEXT NOT NULL,event TEXT NOT NULL,PRIMARY KEY(peer,event));
            CREATE TABLE IF NOT EXISTS received_shared_objects(hash TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS incoming_events(id TEXT PRIMARY KEY,payload TEXT NOT NULL,error TEXT);
            CREATE TABLE IF NOT EXISTS peer_objects(peer TEXT NOT NULL,hash TEXT NOT NULL,PRIMARY KEY(peer,hash));")?;
        db.execute_batch("CREATE TABLE IF NOT EXISTS event_frontier(branch TEXT NOT NULL,event TEXT NOT NULL,PRIMARY KEY(branch,event));")?;
        db.execute_batch("CREATE TABLE IF NOT EXISTS object_sizes(hash TEXT PRIMARY KEY,size INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS published_objects(hash TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS object_requests(hash TEXT PRIMARY KEY,requested_ms INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS object_references(hash TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS object_errors(hash TEXT PRIMARY KEY,error TEXT NOT NULL,at INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS peer_requests(peer TEXT NOT NULL,hash TEXT NOT NULL,PRIMARY KEY(peer,hash));")?;
        Ok(db)
    }
    /// Phase 2 (after `snapshot::initialize`): legacy branch-table migration,
    /// store identity and the in-memory handle.
    fn open_identity(db: Connection, root: &Path, repo: Option<&str>) -> Result<Self> {
        let schema: String = db.query_row(
            "SELECT sql FROM sqlite_master WHERE name='branches'",
            [],
            |r| r.get(0),
        )?;
        if schema.contains("UNIQUE") {
            db.execute_batch("BEGIN IMMEDIATE; ALTER TABLE branches RENAME TO branches_legacy; CREATE TABLE branches(id TEXT PRIMARY KEY,name TEXT NOT NULL,shared INTEGER NOT NULL); INSERT INTO branches SELECT * FROM branches_legacy; DROP TABLE branches_legacy; COMMIT;")?;
        }
        let get = |key: &str| -> Result<Option<String>> {
            Ok(db
                .query_row("SELECT value FROM config WHERE key=?", [key], |r| r.get(0))
                .optional()?)
        };
        let repo_id = get("repo")?.unwrap_or_else(|| repo.map(str::to_owned).unwrap_or_else(id));
        if let Some(r) = repo {
            ensure!(r == repo_id, "REPOSITORY_MISMATCH");
        }
        let device = get("device")?.unwrap_or_else(id);
        let main = hash(format!("tkfs-v1/{repo_id}/main").as_bytes());
        let active = get("active")?.unwrap_or_else(|| main.clone());
        let generation: u64 = get("generation")?.unwrap_or_else(|| "0".into()).parse()?;
        db.execute(
            "INSERT OR IGNORE INTO branches VALUES(?, 'main', 1)",
            [&main],
        )?;
        for (k, v) in [
            ("repo", repo_id.as_str()),
            ("device", device.as_str()),
            ("active", active.as_str()),
            ("generation", &generation.to_string()),
        ] {
            db.execute("INSERT OR IGNORE INTO config VALUES(?,?)", [k, v])?;
        }
        Ok(Self {
            db,
            root: root.to_owned(),
            repo: repo_id,
            device,
            active,
            generation,
            view_cache: RefCell::new(None),
            caches: StoreCaches::default(),
        })
    }
    /// Phase 7: resume each branch from its startup snapshot, else rebuild it.
    fn resume_or_rebuild(&mut self) -> Result<()> {
        for branch in self.branches()? {
            if !self.resume_snapshot(&branch)? {
                crate::startup::progress("reconstructing snapshot");
                self.rebuild(&branch.id)?;
            }
        }
        Ok(())
    }
    /// Separate connection for bulk I/O. No replay, projection rebuild or engine
    /// mutex; accepted metadata remains owned by the worker's semantic engine.
    pub fn transfer_view(&self) -> Result<Self> {
        Self::open_transfer(&self.root, &self.repo, &self.device)
    }
    pub(crate) fn open_transfer(root: &Path, repo: &str, device: &str) -> Result<Self> {
        let db = Connection::open(root.join("metadata.sqlite"))?;
        db.busy_timeout(std::time::Duration::from_secs(5))?;
        db.execute_batch("PRAGMA synchronous=FULL;")?;
        let active = db.query_row("SELECT value FROM config WHERE key='active'", [], |row| {
            row.get(0)
        })?;
        Ok(Self {
            db,
            root: root.to_owned(),
            repo: repo.to_owned(),
            device: device.to_owned(),
            active,
            generation: 0,
            view_cache: RefCell::new(None),
            caches: StoreCaches::default(),
        })
    }
    /// Public secondary handle: an alias of the bulk transfer view. It runs no
    /// schema work, recovery or replay; its view cache follows other
    /// connections' commits through `PRAGMA data_version`.
    pub fn open_secondary(root: &Path, repo: &str, device: &str) -> Result<Store> {
        Self::open_transfer(root, repo, device)
    }
    pub fn branches(&self) -> Result<Vec<Branch>> {
        Ok(self
            .db
            .prepare("SELECT id,name,shared FROM branches ORDER BY name,id")?
            .query_map([], |r| {
                Ok(Branch {
                    id: r.get(0)?,
                    name: r.get(1)?,
                    shared: r.get(2)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?)
    }
    pub fn branch(&self, name_or_id: &str) -> Result<Branch> {
        let branches = self.branches()?;
        if let Some(b) = branches.iter().find(|b| b.id == name_or_id) {
            return Ok(b.clone());
        }
        let matching: Vec<_> = branches
            .into_iter()
            .filter(|b| b.name == name_or_id)
            .collect();
        ensure!(
            matching.len() <= 1,
            "AMBIGUOUS_BRANCH: use stable branch ID"
        );
        matching.into_iter().next().context("BRANCH_NOT_FOUND")
    }
    pub fn events(&self) -> Result<Vec<Event>> {
        let payloads = self
            .db
            .prepare("SELECT payload FROM events ORDER BY id")?
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        payloads
            .into_iter()
            .map(|s| Ok(serde_json::from_str(&s)?))
            .collect()
    }
    pub fn projection(&self, branch: &str) -> Result<Projection> {
        Ok(self.view(branch)?.projection.clone())
    }
    /// Changes only when another connection commits; a connection's own
    /// commits leave it unchanged (those are covered by `total_changes`).
    fn data_version(&self) -> Result<i64> {
        Ok(self
            .db
            .prepare_cached("PRAGMA data_version")?
            .query_row([], |r| r.get(0))?)
    }
    fn view(&self, branch: &str) -> Result<Ref<'_, ViewCache>> {
        let changes = self.db.total_changes();
        let data_version = self.data_version()?;
        let transactional = !self.db.is_autocommit();
        let fresh = self.view_cache.borrow().as_ref().is_some_and(|v| {
            v.branch == branch
                && v.changes == changes
                && v.data_version == data_version
                && v.transactional == transactional
        });
        if !fresh {
            let projection = snapshot::load_projection(&self.db, branch)?;
            let slots = projection
                .entries
                .values()
                .filter(|e| e.alive)
                .map(|e| Ok(((e.parent.clone(), name_key(&e.name)?), e.id.clone())))
                .collect::<Result<_>>()?;
            *self.view_cache.borrow_mut() = Some(ViewCache {
                branch: branch.into(),
                changes,
                data_version,
                transactional,
                projection,
                slots,
            });
        }
        Ok(Ref::map(self.view_cache.borrow(), |v| v.as_ref().unwrap()))
    }
    pub fn entry(&self, entity: &str) -> Result<Entry> {
        self.view(&self.active)?
            .projection
            .entries
            .get(entity)
            .cloned()
            .context("FILE_NOT_FOUND")
    }
    fn rebuild(&mut self, branch: &str) -> Result<()> {
        let events: Vec<_> = self
            .events()?
            .into_iter()
            .filter(|e| e.branch.id == branch)
            .collect();
        let projection = project(branch, &events)?;
        let tx = self.db.savepoint()?;
        persist_projection(&tx, branch, &projection, &events, None)?;
        snapshot::persist(&tx, branch)?;
        tx.commit()?;
        Ok(())
    }
    fn rebuild_frontiers(&mut self) -> Result<()> {
        let events = self.events()?;
        let referenced: BTreeSet<_> = events
            .iter()
            .flat_map(|e| {
                e.dependencies
                    .iter()
                    .chain(e.changes.iter().flat_map(|c| c.parents.iter()))
            })
            .collect();
        let tx = self.db.savepoint()?;
        tx.execute("DELETE FROM event_frontier", [])?;
        for e in events.iter().filter(|e| !referenced.contains(&e.id)) {
            tx.execute(
                "INSERT INTO event_frontier VALUES(?,?)",
                params![e.branch.id, e.id],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
    fn evacuate_unvalidated_journal(&mut self) -> Result<()> {
        // Upgrade older PoC journals that acknowledged incomplete events. Keep
        // their payloads/objects inspectable in the durable inbox, but remove
        // them from accepted inventories, local dependency frontiers and outbox.
        let events = self.events()?;
        let known: BTreeMap<_, _> = events.iter().map(|e| (e.id.clone(), e.clone())).collect();
        let mut rejected: BTreeMap<String, String> = BTreeMap::new();
        for e in &events {
            crate::startup::progress("validating journal");
            if let Err(error) = validate_direct_links(e, &known, &BTreeSet::new()) {
                rejected.insert(e.id.clone(), format!("{error:#}"));
            }
        }
        let candidates: Vec<_> = events
            .iter()
            .filter(|e| !rejected.contains_key(&e.id))
            .cloned()
            .collect();
        let valid = activated_ids(&candidates);
        // Only legacy unresolved nodes need cycle/ancestor traversal. Ordinary
        // long accepted chains validate once per direct edge, not per ancestor.
        for e in events.iter().filter(|e| !valid.contains(&e.id)) {
            if let Err(error) = validate_known_links(e, &known, &rejected.keys().cloned().collect())
            {
                rejected.insert(e.id.clone(), format!("{error:#}"));
            }
        }
        let deferred: Vec<_> = events.iter().filter(|e| !valid.contains(&e.id)).collect();
        if deferred.is_empty() {
            return Ok(());
        }
        let tx = self.db.savepoint()?;
        let mut branches = BTreeSet::new();
        for e in deferred {
            let payload = serde_json::to_string(e)?;
            let old: Option<String> = tx
                .query_row(
                    "SELECT payload FROM incoming_events WHERE id=?",
                    [&e.id],
                    |r| r.get(0),
                )
                .optional()?;
            ensure!(old.is_none_or(|old| old == payload), "EVENT_ID_REUSED");
            tx.execute("INSERT INTO incoming_events(id,payload,error) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET error=excluded.error", params![e.id,payload,rejected.get(&e.id)])?;
            tx.execute("DELETE FROM outbox WHERE event=?", [&e.id])?;
            tx.execute("DELETE FROM peer_known WHERE event=?", [&e.id])?;
            tx.execute("DELETE FROM events WHERE id=?", [&e.id])?;
            branches.insert(e.branch.id.clone());
        }
        // Rebuild valid history/reviews in this SAME transaction. In particular,
        // a cut-budget failure must roll back migration and preserve old records.
        // Invalid event evidence remains in incoming_events and immutable CAS.
        for branch in branches {
            let accepted: Vec<_> = events
                .iter()
                .filter(|e| e.branch.id == branch && valid.contains(&e.id))
                .cloned()
                .collect();
            let projection = project(&branch, &accepted)?;
            tx.execute("DELETE FROM conflicts WHERE branch=?", [&branch])?;
            persist_projection(&tx, &branch, &projection, &accepted, None)?;
        }
        tx.commit()?;
        Ok(())
    }
    pub fn children(&self, parent: &str) -> Result<Vec<Entry>> {
        let view = self.view(&self.active)?;
        // Keep the original projection-based enumeration for conflicted cuts;
        // do not make the lookup index an additional choice of alternatives.
        if !view.projection.conflicts.is_empty() {
            return Ok(view
                .projection
                .entries
                .values()
                .filter(|entry| entry.alive && entry.parent == parent)
                .cloned()
                .collect());
        }
        // The existing live namespace index groups each parent's names. Avoid
        // walking unrelated entries for every directory enumeration ordinal.
        Ok(view
            .slots
            .range((parent.to_owned(), String::new())..)
            .take_while(|((p, _), _)| p == parent)
            .map(|(_, entity)| view.projection.entries[entity].clone())
            .collect())
    }
    pub fn event(&self, branch: &Branch, changes: Vec<Change>) -> Event {
        // Order observed namespace prerequisites without claiming an old writer
        // reviewed other file revisions. Registers still use their actual bases.
        let dependencies = self
            .db
            .prepare("SELECT event FROM event_frontier WHERE branch=? ORDER BY event")
            .and_then(|mut statement| {
                statement
                    .query_map([&branch.id], |r| r.get(0))?
                    .collect::<rusqlite::Result<Vec<String>>>()
            })
            .expect("CAUSAL_FRONTIER_UNAVAILABLE");
        let object_sizes = changes
            .iter()
            .filter(|c| c.field == "content")
            .filter_map(|c| c.value.as_str())
            .filter_map(|h| self.object_size(h).ok().map(|size| (h.to_owned(), size)))
            .collect();
        Event {
            object_sizes,
            id: id(),
            repo: self.repo.clone(),
            branch: branch.clone(),
            device: self.device.clone(),
            created_ms: time(),
            changes,
            dependencies,
            reviewed: vec![],
        }
    }
    pub fn current_change(&self, entity: &str, field: &str, value: Value) -> Result<Change> {
        let view = self.view(&self.active)?;
        let parents = view
            .projection
            .entries
            .get(entity)
            .and_then(|e| e.revisions.get(field))
            .cloned()
            .into_iter()
            .collect();
        Ok(Change {
            entity: entity.into(),
            field: field.into(),
            parents,
            value,
        })
    }
    pub fn lookup(&self, path: &str) -> Result<Entry> {
        let view = self.view(&self.active)?;
        let mut entry = Entry {
            id: ROOT.into(),
            kind: "directory".into(),
            alive: true,
            ..Default::default()
        };
        for component in path.split(['\\', '/']).filter(|s| !s.is_empty()) {
            let key = (entry.id, name_key(component)?);
            let entity = view.slots.get(&key).context("FILE_NOT_FOUND")?;
            entry = view.projection.entries[entity].clone();
        }
        Ok(entry)
    }
    pub fn create(&mut self, path: &str, kind: &str) -> Result<Entry> {
        self.create_with_attributes(path, kind, None)
    }
    pub fn create_with_attributes(
        &mut self,
        path: &str,
        kind: &str,
        attributes: Option<u32>,
    ) -> Result<Entry> {
        let (parent, name) = split_path(path)?;
        let dir = self.lookup(&parent)?;
        ensure!(dir.kind == "directory", "NOT_DIRECTORY");
        ensure!(self.lookup(path).is_err(), "NAME_COLLISION");
        let entity = id();
        let mut changes = vec![
            Change {
                entity: entity.clone(),
                field: "kind".into(),
                parents: vec![],
                value: json!(kind),
            },
            Change {
                entity: entity.clone(),
                field: "location".into(),
                parents: vec![],
                value: json!({"parent":dir.id,"name":name}),
            },
            Change {
                entity: entity.clone(),
                field: "alive".into(),
                parents: vec![],
                value: json!(true),
            },
        ];
        if kind == "file" {
            changes.push(Change {
                entity: entity.clone(),
                field: "content".into(),
                parents: vec![],
                value: json!(self.put_object(b"")?),
            });
        }
        let branch = self.branch(&self.active)?;
        let mut event = self.event(&branch, changes);
        if let Some(attributes) = attributes {
            let now = filetime(event.created_ms);
            let basic = BasicInfo {
                posix_mode: None,
                attributes: BasicInfo::attributes(kind, attributes)?,
                creation_time: now,
                access_time: now,
                write_time: now,
                change_time: now,
            };
            event.changes.push(Change {
                entity,
                field: "basic".into(),
                parents: vec![],
                value: json!(basic),
            });
        }
        self.commit(event, true)?;
        self.lookup(path)
    }
    pub fn set_basic(&mut self, entity: &str, basic: BasicInfo) -> Result<()> {
        basic.validate()?;
        let entry = self.entry(entity)?;
        ensure!(
            basic.attributes == BasicInfo::attributes(&entry.kind, basic.attributes)?,
            "INVALID_ATTRIBUTES"
        );
        if entry.basic_info() == basic {
            return Ok(());
        }
        let change = self.current_change(entity, "basic", json!(basic))?;
        self.commit(self.event(&self.branch(&self.active)?, vec![change]), true)
    }
    pub fn write_revision(
        &mut self,
        entity: &str,
        base: Option<String>,
        bytes: &[u8],
    ) -> Result<String> {
        // An edit also asserts existence. Parent only the existence revision the
        // writer observed: callers needing an old base use write_with_base.
        let alive = self.current_change(entity, "alive", json!(true))?;
        self.write_with_parents(entity, base, alive.parents, bytes, 0)
    }
    pub fn write_with_base(
        &mut self,
        entity: &str,
        content_base: Option<String>,
        alive_base: Option<String>,
        bytes: &[u8],
    ) -> Result<String> {
        self.write_with_base_and_times(entity, content_base, alive_base, bytes, 0)
    }
    pub fn write_with_base_and_times(
        &mut self,
        entity: &str,
        content_base: Option<String>,
        alive_base: Option<String>,
        bytes: &[u8],
        preserve_times: u8,
    ) -> Result<String> {
        self.write_with_parents(
            entity,
            content_base,
            alive_base.into_iter().collect(),
            bytes,
            preserve_times,
        )
    }
    fn write_with_parents(
        &mut self,
        entity: &str,
        content_base: Option<String>,
        alive_parents: Vec<String>,
        bytes: &[u8],
        preserve_times: u8,
    ) -> Result<String> {
        let object = self.put_object(bytes)?;
        self.write_object_with_parents(entity, content_base, alive_parents, object, preserve_times)
    }
    pub fn write_stream_with_base_and_times(
        &mut self,
        entity: &str,
        content_base: Option<String>,
        alive_base: Option<String>,
        source: &mut impl Read,
        preserve_times: u8,
    ) -> Result<String> {
        let object = self.put_stream(source)?;
        self.write_object_with_parents(
            entity,
            content_base,
            alive_base.into_iter().collect(),
            object,
            preserve_times,
        )
    }
    fn write_object_with_parents(
        &mut self,
        entity: &str,
        content_base: Option<String>,
        alive_parents: Vec<String>,
        object: String,
        preserve_times: u8,
    ) -> Result<String> {
        let b = self.branch(&self.active)?;
        let mut event = self.event(
            &b,
            vec![
                Change {
                    entity: entity.into(),
                    field: "content".into(),
                    parents: content_base.into_iter().collect(),
                    value: json!(object),
                },
                Change {
                    entity: entity.into(),
                    field: "alive".into(),
                    parents: alive_parents,
                    value: json!(true),
                },
            ],
        );
        let entry = self.entry(entity)?;
        if entry.basic.is_some() || preserve_times != 0 {
            let mut basic = entry.basic_info();
            if preserve_times & 2 == 0 {
                basic.write_time = filetime(event.created_ms);
            }
            if preserve_times & 4 == 0 {
                basic.change_time = filetime(event.created_ms);
            }
            basic.attributes = BasicInfo::attributes(&entry.kind, basic.attributes | 0x20)?;
            event
                .changes
                .push(self.current_change(entity, "basic", json!(basic))?);
        }
        let rev = event.id.clone();
        self.commit(event, true)?;
        Ok(rev)
    }
    pub fn rename(&mut self, entity: &str, target: &str, replace: bool) -> Result<()> {
        let (parent, name) = split_path(target)?;
        let dir = self.lookup(&parent)?;
        ensure!(dir.kind == "directory", "NOT_DIRECTORY");
        let p = self.projection(&self.active)?;
        let source = p.entries.get(entity).context("FILE_NOT_FOUND")?;
        let mut ancestor = dir.id.clone();
        while ancestor != ROOT {
            ensure!(ancestor != entity, "DIRECTORY_CYCLE");
            ancestor = p
                .entries
                .get(&ancestor)
                .context("FILE_NOT_FOUND")?
                .parent
                .clone();
        }
        let mut changes =
            vec![self.current_change(entity, "location", json!({"parent":dir.id,"name":name}))?];
        if let Ok(existing) = self.lookup(target)
            && existing.id != entity
        {
            ensure!(
                replace && existing.kind == "file" && source.kind == "file",
                "NAME_COLLISION"
            );
            changes.push(self.current_change(&existing.id, "alive", json!(false))?);
        }
        let b = self.branch(&self.active)?;
        self.commit(self.event(&b, changes), true)
    }
    pub fn delete(&mut self, entity: &str) -> Result<()> {
        let p = self.projection(&self.active)?;
        ensure!(
            !p.entries.values().any(|e| e.alive && e.parent == entity),
            "DIRECTORY_NOT_EMPTY"
        );
        let b = self.branch(&self.active)?;
        self.commit(
            self.event(
                &b,
                vec![self.current_change(entity, "alive", json!(false))?],
            ),
            true,
        )
    }
    /// Fork/publish only the currently visible cut. Neither source event IDs nor
    /// private conflict alternatives/ancestry appear in the resulting root event.
    pub fn fork(&mut self, name: &str, shared: bool) -> Result<Branch> {
        name_key(name)?;
        ensure!(
            !self
                .branches()?
                .iter()
                .any(|b| b.name == name && b.shared == shared),
            "BRANCH_EXISTS"
        );
        let source = self.projection(&self.active)?;
        ensure!(source.pending.is_empty(), "INCOMPLETE_CAUSAL_HISTORY");
        let b = Branch {
            id: id(),
            name: name.into(),
            shared,
        };
        let mut changes = vec![];
        for entry in source.entries.values().filter(|e| e.alive) {
            for (field, value) in [
                ("kind", json!(entry.kind)),
                ("location", json!({"parent":entry.parent,"name":entry.name})),
                ("alive", json!(true)),
                ("basic", json!(entry.basic_info())),
            ] {
                changes.push(Change {
                    entity: entry.id.clone(),
                    field: field.into(),
                    parents: vec![],
                    value,
                });
            }
            if let Some(h) = &entry.content {
                changes.push(Change {
                    entity: entry.id.clone(),
                    field: "content".into(),
                    parents: vec![],
                    value: json!(h),
                });
            }
        }
        if changes.is_empty() {
            self.db.execute(
                "INSERT INTO branches VALUES(?,?,?)",
                params![b.id, b.name, b.shared],
            )?;
            self.rebuild(&b.id)?;
        } else {
            self.commit(self.event(&b, changes), true)?;
        }
        Ok(b)
    }
    pub fn checkout(&mut self, branch: &str) -> Result<()> {
        let b = self.branch(branch)?;
        let p = self.projection(&b.id)?;
        ensure!(p.pending.is_empty(), "INCOMPLETE_CAUSAL_HISTORY");
        let generation = self.generation + 1;
        let tx = self.db.savepoint()?;
        tx.execute("UPDATE config SET value=? WHERE key='active'", [&b.id])?;
        tx.execute(
            "UPDATE config SET value=? WHERE key='generation'",
            [generation.to_string()],
        )?;
        tx.commit()?;
        self.active = b.id;
        self.generation = generation;
        Ok(())
    }
    pub fn conflicts(&self) -> Result<Vec<Conflict>> {
        let mut s = self
            .db
            .prepare("SELECT payload,resolved_by FROM conflicts WHERE branch=? ORDER BY id")?;
        let rows = s.query_map([&self.active], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
        })?;
        rows.map(|r| {
            let (s, resolved) = r?;
            let mut c: Conflict = serde_json::from_str(&s)?;
            c.resolved_by = resolved;
            Ok(c)
        })
        .collect()
    }
    pub fn resolve(&mut self, conflicts: &[String], selected: &str) -> Result<String> {
        ensure!(!conflicts.is_empty(), "EXPLICIT_REVIEW_REQUIRED");
        let all = self.conflicts()?;
        let mut groups: BTreeMap<String, Vec<&Conflict>> = BTreeMap::new();
        for requested in conflicts {
            let c = all
                .iter()
                .find(|c| &c.id == requested && c.resolved_by.is_none())
                .context("CONFLICT_NOT_FOUND")?;
            ensure!(
                ["content", "alive", "location", "kind", "basic"].contains(&c.field.as_str()),
                "NAMESPACE_RESOLUTION_REQUIRES_RENAME_OR_DELETE"
            );
            groups
                .entry(register(&c.entity, &c.field))
                .or_default()
                .push(c);
        }
        let mut changes = vec![];
        for group in groups.values() {
            let c = group[0];
            let alternatives: Vec<_> = group.iter().flat_map(|c| c.alternatives.iter()).collect();
            let value = alternatives
                .iter()
                .find(|v| v.revision == selected)
                .context("REVISION_NOT_IN_REVIEW")?
                .value
                .clone();
            let parents = alternatives
                .iter()
                .map(|v| v.revision.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            changes.push(Change {
                entity: c.entity.clone(),
                field: c.field.clone(),
                parents,
                value,
            });
        }
        let b = self.branch(&self.active)?;
        let mut event = self.event(&b, changes);
        event.reviewed = conflicts.to_vec();
        let revision = event.id.clone();
        self.commit(event, true)?;
        Ok(revision)
    }
    /// Move an explicitly reviewed displaced namespace entry to a free path.
    /// Only reviewed location revisions are parented; unseen competitors remain.
    pub fn recover(&mut self, entity: &str, target: &str, reviewed: &[String]) -> Result<String> {
        ensure!(!reviewed.is_empty(), "EXPLICIT_REVIEW_REQUIRED");
        let p = self.projection(&self.active)?;
        let entry = p.entries.get(entity).context("ENTRY_NOT_FOUND")?;
        ensure!(
            p.heads[&register(entity, "alive")]
                .last()
                .is_some_and(|v| v.value == true),
            "EXPLICIT_EXISTENCE_RESOLUTION_REQUIRED"
        );
        let conflicts = self.conflicts()?;
        let events = self.events()?;
        let mut parents = BTreeSet::new();
        for requested in reviewed {
            let c = conflicts
                .iter()
                .find(|c| &c.id == requested && c.resolved_by.is_none())
                .context("CONFLICT_NOT_FOUND")?;
            ensure!(
                (c.field == "namespace" || c.field == "location")
                    && c.entity.split(',').any(|e| e == entity),
                "REVIEW_SCOPE_MISMATCH"
            );
            for v in &c.alternatives {
                if events.iter().find(|e| e.id == v.revision).is_some_and(|e| {
                    e.changes.iter().any(|ch| {
                        ch.entity == entity && ch.field == "location" && ch.value == v.value
                    })
                }) {
                    parents.insert(v.revision.clone());
                }
            }
        }
        ensure!(!parents.is_empty(), "REVIEW_HAS_NO_ENTRY_LOCATION");
        let (parent, name) = split_path(target)?;
        let dir = self.lookup(&parent)?;
        ensure!(dir.kind == "directory", "NOT_DIRECTORY");
        ensure!(self.lookup(target).is_err(), "NAME_COLLISION");
        let branch = self.branch(&self.active)?;
        let mut event = self.event(
            &branch,
            vec![Change {
                entity: entity.into(),
                field: "location".into(),
                parents: parents.into_iter().collect(),
                value: json!({"parent":dir.id,"name":name}),
            }],
        );
        event.reviewed = reviewed.to_vec();
        let revision = event.id.clone();
        let mut candidate: Vec<_> = events
            .into_iter()
            .filter(|e| e.branch.id == self.active)
            .collect();
        candidate.push(event.clone());
        let projected = project(&self.active, &candidate)?;
        ensure!(
            lookup(&projected, target).is_ok_and(|e| e.id == entry.id),
            "RECOVERY_NOT_VISIBLE: review remaining location competitors"
        );
        self.commit(event, true)?;
        Ok(revision)
    }
    pub fn history(&self) -> Result<Value> {
        let records = self
            .db
            .prepare("SELECT id,branch,object,message FROM checkpoints ORDER BY rowid")?
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(json!(records.into_iter().map(|(id,branch,object,message)|json!({"id":id,"branch":branch,"object":object,"message":message})).collect::<Vec<_>>()))
    }
    /// Explicit export; all authorization remains rooted in shared events.
    pub fn export_shared_objects(&self, backend: &dyn ObjectBackend) -> Result<Value> {
        let events: Vec<_> = self
            .events()?
            .into_iter()
            .filter(|e| e.branch.shared)
            .collect();
        let objects: BTreeSet<_> = events.iter().flat_map(Event::objects).collect();
        for object in &objects {
            ensure!(
                backend.put_stream(&mut self.open_object(object)?)? == *object,
                "BACKEND_HASH_MISMATCH"
            );
        }
        let branches: Vec<_> = self.branches()?.into_iter().filter(|b| b.shared).collect();
        let manifest = json!({"format":"tkfs-shared-export-1","repo":self.repo,"branches":branches,"events":events,"objects":objects});
        let manifest_object = backend.put_verified(&serde_json::to_vec(&manifest)?)?;
        Ok(
            json!({"backend":"local-directory-test-bucket","manifest":manifest_object,"objects":objects.len(),"events":manifest["events"].as_array().unwrap().len(),"remote_validated":false}),
        )
    }
    /// Restore creates a fresh PRIVATE branch; it never rewinds shared history.
    pub fn restore(&mut self, checkpoint: &str, name: &str) -> Result<Branch> {
        name_key(name)?;
        ensure!(
            !self.branches()?.iter().any(|b| b.name == name),
            "BRANCH_EXISTS"
        );
        let object: String = self
            .db
            .query_row(
                "SELECT object FROM checkpoints WHERE id=?",
                [checkpoint],
                |r| r.get(0),
            )
            .context("CHECKPOINT_NOT_FOUND")?;
        let snapshot: Value = serde_json::from_slice(&self.read_object(&object)?)?;
        ensure!(
            ["tkfs-snapshot-1", "tkfs-snapshot-2"]
                .iter()
                .any(|f| snapshot["format"] == *f),
            "INVALID_SNAPSHOT"
        );
        let p: Projection = serde_json::from_value(snapshot["state"].clone())?;
        let branch = Branch {
            id: id(),
            name: name.into(),
            shared: false,
        };
        let mut changes = vec![];
        for entry in p.entries.values().filter(|e| e.alive) {
            for (field, value) in [
                ("kind", json!(entry.kind)),
                ("alive", json!(true)),
                ("location", json!({"parent":entry.parent,"name":entry.name})),
                ("basic", json!(entry.basic_info())),
            ] {
                changes.push(Change {
                    entity: entry.id.clone(),
                    field: field.into(),
                    parents: vec![],
                    value,
                });
            }
            if let Some(h) = &entry.content {
                changes.push(Change {
                    entity: entry.id.clone(),
                    field: "content".into(),
                    parents: vec![],
                    value: json!(h),
                });
            }
        }
        if changes.is_empty() {
            self.db.execute(
                "INSERT INTO branches VALUES(?,?,0)",
                params![branch.id, branch.name],
            )?;
            self.rebuild(&branch.id)?;
        } else {
            self.commit(self.event(&branch, changes), true)?;
        }
        Ok(branch)
    }
    pub fn checkpoint(&mut self, message: &str) -> Result<String> {
        let snapshot = self.projection(&self.active)?;
        ensure!(snapshot.pending.is_empty(), "INCOMPLETE_CAUSAL_HISTORY");
        let bytes = serde_json::to_vec(
            &json!({"format":"tkfs-snapshot-2","branch":self.active,"state":snapshot}),
        )?;
        let object = self.put_object(&bytes)?;
        let checkpoint = id();
        self.db.execute(
            "INSERT INTO checkpoints VALUES(?,?,?,?)",
            params![checkpoint, self.active, object, message],
        )?;
        Ok(checkpoint)
    }
    pub fn receipt(&self, request: &str, payload: &Value) -> Result<Option<Value>> {
        let old: Option<(String, String)> = self
            .db
            .query_row(
                "SELECT payload_hash,result FROM receipts WHERE id=?",
                [request],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if let Some((h, result)) = old {
            ensure!(
                h == hash(&serde_json::to_vec(payload)?),
                "REQUEST_ID_REUSED"
            );
            Ok(Some(serde_json::from_str(&result)?))
        } else {
            Ok(None)
        }
    }
}
pub fn lookup(p: &Projection, path: &str) -> Result<Entry> {
    let mut entry = Entry {
        id: ROOT.into(),
        kind: "directory".into(),
        alive: true,
        ..Default::default()
    };
    for component in path.split(['\\', '/']).filter(|s| !s.is_empty()) {
        let key = name_key(component)?;
        entry = p
            .entries
            .values()
            .find(|e| {
                e.alive && e.parent == entry.id && name_key(&e.name).ok().as_ref() == Some(&key)
            })
            .cloned()
            .context("FILE_NOT_FOUND")?;
    }
    Ok(entry)
}
pub fn split_path(path: &str) -> Result<(String, String)> {
    let path = path.replace('\\', "/");
    let path = path.trim_end_matches('/');
    let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
    name_key(name)?;
    Ok((parent.into(), name.into()))
}
pub fn fault(point: &str) {
    if std::env::var("TKFS_FAULT").as_deref() == Ok(point) {
        std::process::exit(86);
    }
}
