//! Durable semantic journal and deterministic multi-value registers.
//! Receipt order is never used to choose a version. Ordinary edits parent only
//! their actual visible/base revision; only explicit review can parent competitors.
use crate::backend::ObjectBackend;
use anyhow::{Context, Result, bail, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
use unicode_normalization::UnicodeNormalization;
use uuid::Uuid;

pub const ROOT: &str = "root";
pub const MAX_FILE: usize = 16 * 1024 * 1024;
pub const MAX_EVENT: usize = 8 * 1024 * 1024;
pub const MAX_NAMESPACE_CUTS: usize = 4096;
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

pub struct Store {
    pub db: Connection,
    pub root: PathBuf,
    pub repo: String,
    pub device: String,
    pub active: String,
    pub generation: u64,
}
impl Store {
    pub fn open(root: &Path, repo: Option<&str>) -> Result<Self> {
        fs::create_dir_all(root.join("objects"))?;
        let db = Connection::open(root.join("metadata.sqlite"))?;
        db.busy_timeout(std::time::Duration::from_secs(5))?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS config(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS branches(id TEXT PRIMARY KEY, name TEXT NOT NULL, shared INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY, branch TEXT NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS current_state(branch TEXT PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS conflicts(id TEXT PRIMARY KEY, branch TEXT NOT NULL, payload TEXT NOT NULL, resolved_by TEXT);
            CREATE TABLE IF NOT EXISTS outbox(event TEXT PRIMARY KEY, acknowledged INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS receipts(id TEXT PRIMARY KEY, payload_hash TEXT NOT NULL, result TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS checkpoints(id TEXT PRIMARY KEY, branch TEXT NOT NULL, object TEXT NOT NULL, message TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS peer_known(peer TEXT NOT NULL,event TEXT NOT NULL,PRIMARY KEY(peer,event));
            CREATE TABLE IF NOT EXISTS received_shared_objects(hash TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS incoming_events(id TEXT PRIMARY KEY,payload TEXT NOT NULL,error TEXT);
            CREATE TABLE IF NOT EXISTS peer_objects(peer TEXT NOT NULL,hash TEXT NOT NULL,PRIMARY KEY(peer,hash));")?;
        db.execute_batch("CREATE TABLE IF NOT EXISTS event_frontier(branch TEXT NOT NULL,event TEXT NOT NULL,PRIMARY KEY(branch,event));")?;
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
        let mut store = Self {
            db,
            root: root.to_owned(),
            repo: repo_id,
            device,
            active,
            generation,
        };
        // Recompute from the journal and verify every referenced object before
        // serving anything, including alternatives that lost the visible tie-break.
        for event in store.events()? {
            for object in event.objects() {
                store.read_object(&object)?;
            }
        }
        store.evacuate_unvalidated_journal()?;
        store.rebuild_frontiers()?;
        for branch in store.branches()? {
            store.rebuild(&branch.id)?;
        }
        store.drain_incoming()?;
        Ok(store)
    }
    pub fn read_object(&self, object: &str) -> Result<Vec<u8>> {
        ensure!(
            object.len() == 64 && object.bytes().all(|c| c.is_ascii_hexdigit()),
            "INVALID_OBJECT_ID"
        );
        let bytes =
            fs::read(self.root.join("objects").join(object)).context("OFFLINE_OBJECT_MISSING")?;
        ensure!(hash(&bytes) == object, "CORRUPT_OBJECT: {object}");
        Ok(bytes)
    }
    pub fn put_object(&self, bytes: &[u8]) -> Result<String> {
        ensure!(bytes.len() <= MAX_FILE, "FILE_TOO_LARGE");
        let object = hash(bytes);
        let dest = self.root.join("objects").join(&object);
        if dest.exists() {
            self.read_object(&object)?;
            return Ok(object);
        }
        let temp = self.root.join("objects").join(format!("{}.part", id()));
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        drop(f);
        fault("object_flushed");
        // MoveFileExW WRITE_THROUGH below supplies the Windows rename barrier.
        install_object(&temp, &dest)?;
        fault("object_installed");
        Ok(object)
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
        let payload: String = self.db.query_row(
            "SELECT payload FROM current_state WHERE branch=?",
            [branch],
            |r| r.get(0),
        )?;
        Ok(serde_json::from_str(&payload)?)
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
    pub fn commit(&mut self, event: Event, local: bool) -> Result<()> {
        if local {
            return self.commit_ready(event, true);
        }
        // Trusted core callers supply the actual local bytes explicitly. The
        // network path uses receive directly and never promotes global CAS hashes.
        let objects = event
            .objects()
            .into_iter()
            .map(|h| Ok((h.clone(), hex::encode(self.read_object(&h)?))))
            .collect::<Result<_>>()?;
        let revision = event.id.clone();
        let device = event.device.clone();
        self.receive(
            Bundle {
                repo: self.repo.clone(),
                branches: vec![],
                events: vec![event],
                objects,
            },
            &BTreeSet::from([device]),
        )?;
        let error: Option<String> = self
            .db
            .query_row(
                "SELECT error FROM incoming_events WHERE id=?",
                [&revision],
                |r| r.get(0),
            )
            .optional()?
            .flatten();
        if let Some(error) = error {
            bail!("{error}");
        }
        Ok(())
    }
    fn commit_ready(&mut self, event: Event, local: bool) -> Result<()> {
        ensure!(event.repo == self.repo, "REPOSITORY_MISMATCH");
        ensure!(
            event.id.len() <= 128 && !event.id.is_empty() && event.device.len() <= 128,
            "INVALID_EVENT"
        );
        ensure!(
            !event.changes.is_empty() && event.changes.len() <= 10000,
            "INVALID_EVENT"
        );
        ensure!(local || event.branch.shared, "PRIVATE_EVENT_REJECTED");
        let payload = serde_json::to_string(&event)?;
        ensure!(payload.len() <= MAX_EVENT, "EVENT_METADATA_TOO_LARGE");
        name_key(&event.branch.name)?;
        ensure!(
            !event.branch.id.is_empty() && event.branch.id.len() <= 128,
            "INVALID_BRANCH_ID"
        );
        let existing: Option<String> = self
            .db
            .query_row("SELECT payload FROM events WHERE id=?", [&event.id], |r| {
                r.get(0)
            })
            .optional()?;
        if let Some(old) = existing {
            ensure!(old == payload, "EVENT_ID_REUSED");
            return Ok(());
        }
        for object in event.objects() {
            self.read_object(&object)?;
        }
        let mut seen = BTreeSet::new();
        for change in &event.changes {
            ensure!(
                seen.insert(register(&change.entity, &change.field)),
                "DUPLICATE_REGISTER"
            );
            ensure!(
                change.entity != ROOT && !change.entity.is_empty() && change.entity.len() <= 128,
                "INVALID_ENTITY"
            );
            ensure!(
                ["kind", "location", "content", "alive"].contains(&change.field.as_str()),
                "INVALID_FIELD"
            );
            ensure!(!change.parents.contains(&event.id), "CAUSAL_CYCLE");
            match change.field.as_str() {
                "location" => {
                    name_key(change.value["name"].as_str().context("INVALID_LOCATION")?)?;
                    ensure!(
                        change.value["parent"].as_str().is_some(),
                        "INVALID_LOCATION"
                    );
                }
                "kind" => ensure!(
                    [json!("file"), json!("directory")].contains(&change.value),
                    "INVALID_KIND"
                ),
                "alive" => ensure!(change.value.is_boolean(), "INVALID_ALIVE"),
                "content" => ensure!(change.value.is_string(), "INVALID_CONTENT"),
                _ => {}
            }
        }
        if let Ok(b) = self.branch(&event.branch.id) {
            ensure!(b == event.branch, "BRANCH_DESCRIPTOR_CHANGED");
        }
        let fast = self.content_successor(&event)?;
        let events = if fast.is_none() {
            let mut events: Vec<_> = self
                .events()?
                .into_iter()
                .filter(|e| e.branch.id == event.branch.id)
                .collect();
            events.push(event.clone());
            events
        } else {
            vec![]
        };
        let projection = if let Some(p) = &fast {
            p.clone()
        } else {
            project(&event.branch.id, &events)?
        };
        if !projection.pending.is_empty() {
            return Err(CausalPrerequisitesPending.into());
        }
        fault("before_sql");
        let tx = self.db.savepoint()?;
        tx.execute(
            "INSERT OR IGNORE INTO branches VALUES(?,?,?)",
            params![event.branch.id, event.branch.name, event.branch.shared],
        )?;
        tx.execute(
            "INSERT INTO events VALUES(?,?,?)",
            params![event.id, event.branch.id, payload],
        )?;
        if event.branch.shared {
            tx.execute("INSERT INTO outbox(event) VALUES(?)", [&event.id])?;
        }
        if fast.is_some() {
            tx.execute(
                "UPDATE current_state SET payload=? WHERE branch=?",
                params![serde_json::to_string(&projection)?, event.branch.id],
            )?;
        } else {
            persist_projection(&tx, &event.branch.id, &projection, &events, Some(&event.id))?;
        }
        for parent in event
            .dependencies
            .iter()
            .chain(event.changes.iter().flat_map(|c| c.parents.iter()))
        {
            tx.execute(
                "DELETE FROM event_frontier WHERE branch=? AND event=?",
                params![event.branch.id, parent],
            )?;
        }
        tx.execute(
            "INSERT INTO event_frontier VALUES(?,?)",
            params![event.branch.id, event.id],
        )?;
        tx.commit()?;
        fault("sql_committed");
        Ok(())
    }
    fn content_successor(&self, event: &Event) -> Result<Option<Projection>> {
        if !event.reviewed.is_empty()
            || !event.changes.iter().any(|c| c.field == "content")
            || event.changes.iter().any(|c| {
                !["content", "alive"].contains(&c.field.as_str())
                    || (c.field == "alive" && c.value != true)
            })
        {
            return Ok(None);
        }
        let Ok(mut p) = self.projection(&event.branch.id) else {
            return Ok(None);
        };
        if !p.pending.is_empty() {
            return Ok(None);
        }
        for change in &event.changes {
            let Some(entry) = p.entries.get(&change.entity) else {
                return Ok(None);
            };
            let Some(heads) = p.heads.get(&register(&change.entity, &change.field)) else {
                return Ok(None);
            };
            if entry.kind != "file"
                || !entry.alive
                || heads.len() != 1
                || change.parents != vec![heads[0].revision.clone()]
            {
                return Ok(None);
            }
        }
        // Frontier dependencies are checked against accepted journal rows only.
        // A missing or cross-branch prerequisite falls back to full validation.
        for dependency in &event.dependencies {
            let branch: Option<String> = self
                .db
                .query_row("SELECT branch FROM events WHERE id=?", [dependency], |r| {
                    r.get(0)
                })
                .optional()?;
            if branch.as_deref() != Some(&event.branch.id) {
                return Ok(None);
            }
        }
        for change in &event.changes {
            p.heads.insert(
                register(&change.entity, &change.field),
                vec![Version {
                    revision: event.id.clone(),
                    device: event.device.clone(),
                    created_ms: event.created_ms,
                    parents: change.parents.clone(),
                    value: change.value.clone(),
                }],
            );
            let entry = p.entries.get_mut(&change.entity).unwrap();
            entry
                .revisions
                .insert(change.field.clone(), event.id.clone());
            if change.field == "content" {
                entry.content = change.value.as_str().map(str::to_owned);
            }
        }
        for entity in event
            .changes
            .iter()
            .map(|c| &c.entity)
            .collect::<BTreeSet<_>>()
        {
            let modified = ["kind", "location", "alive", "content"]
                .iter()
                .filter_map(|field| {
                    p.heads
                        .get(&register(entity, field))
                        .and_then(|heads| heads.last())
                })
                .map(|v| v.created_ms)
                .max()
                .unwrap_or(0);
            p.entries.get_mut(entity).unwrap().modified_ms = modified;
        }
        // Each changed register has one head and this event parents it. All old
        // versions are ancestors, so no new historical pair/namespace conflict
        // is possible. Existing durable records remain untouched.
        Ok(Some(p))
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
        Event {
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
        let p = self.projection(&self.active)?;
        let parents = p
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
        lookup(&self.projection(&self.active)?, path)
    }
    pub fn create(&mut self, path: &str, kind: &str) -> Result<Entry> {
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
        self.commit(self.event(&branch, changes), true)?;
        self.lookup(path)
    }
    pub fn write_revision(
        &mut self,
        entity: &str,
        base: Option<String>,
        bytes: &[u8],
    ) -> Result<String> {
        let object = self.put_object(bytes)?;
        let branch = self.branch(&self.active)?;
        // An edit also asserts existence. Parent only the existence revision the
        // writer observed: callers needing an old base use write_with_base.
        let alive = self.current_change(entity, "alive", json!(true))?;
        let event = self.event(
            &branch,
            vec![
                Change {
                    entity: entity.into(),
                    field: "content".into(),
                    parents: base.into_iter().collect(),
                    value: json!(object),
                },
                alive,
            ],
        );
        let rev = event.id.clone();
        self.commit(event, true)?;
        Ok(rev)
    }
    pub fn write_with_base(
        &mut self,
        entity: &str,
        content_base: Option<String>,
        alive_base: Option<String>,
        bytes: &[u8],
    ) -> Result<String> {
        let object = self.put_object(bytes)?;
        let b = self.branch(&self.active)?;
        let event = self.event(
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
                    parents: alive_base.into_iter().collect(),
                    value: json!(true),
                },
            ],
        );
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
        for e in p.entries.values().filter(|e| e.alive) {
            if let Some(h) = &e.content {
                self.read_object(h)?;
            }
        }
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
                ["content", "alive", "location", "kind"].contains(&c.field.as_str()),
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
    pub fn known_by_peer(&self, peer: &str) -> Result<BTreeSet<String>> {
        Ok(self
            .db
            .prepare("SELECT event FROM peer_known WHERE peer=?")?
            .query_map([peer], |r| r.get(0))?
            .collect::<std::result::Result<_, _>>()?)
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
        let bundle = self.shared_bundle(&BTreeSet::new())?;
        for (object, encoded) in &bundle.objects {
            ensure!(
                backend.put_verified(&hex::decode(encoded)?)? == *object,
                "BACKEND_HASH_MISMATCH"
            );
        }
        let objects: Vec<_> = bundle.objects.keys().cloned().collect();
        let manifest = json!({"format":"tkfs-shared-export-1","repo":self.repo,"branches":bundle.branches,"events":bundle.events,"objects":objects});
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
        ensure!(snapshot["format"] == "tkfs-snapshot-1", "INVALID_SNAPSHOT");
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
            ] {
                changes.push(Change {
                    entity: entry.id.clone(),
                    field: field.into(),
                    parents: vec![],
                    value,
                });
            }
            if let Some(h) = &entry.content {
                self.read_object(h)?;
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
    pub fn remember_peer_inventory(&mut self, peer: &str, known: &BTreeSet<String>) -> Result<()> {
        let tx = self.db.savepoint()?;
        tx.execute("DELETE FROM peer_known WHERE peer=?", [peer])?;
        for event in known {
            tx.execute("INSERT INTO peer_known VALUES(?,?)", params![peer, event])?;
        }
        tx.commit()?;
        Ok(())
    }
    pub fn checkpoint(&mut self, message: &str) -> Result<String> {
        let snapshot = self.projection(&self.active)?;
        ensure!(snapshot.pending.is_empty(), "INCOMPLETE_CAUSAL_HISTORY");
        let bytes = serde_json::to_vec(
            &json!({"format":"tkfs-snapshot-1","branch":self.active,"state":snapshot}),
        )?;
        let object = self.put_object(&bytes)?;
        let checkpoint = id();
        self.db.execute(
            "INSERT INTO checkpoints VALUES(?,?,?,?)",
            params![checkpoint, self.active, object, message],
        )?;
        Ok(checkpoint)
    }
    pub fn shared_bundle(&self, known: &BTreeSet<String>) -> Result<Bundle> {
        let events: Vec<_> = self
            .events()?
            .into_iter()
            .filter(|e| e.branch.shared && !known.contains(&e.id))
            .collect();
        // Authorization starts with shared semantic events, never the object dir.
        let hashes: BTreeSet<_> = events.iter().flat_map(Event::objects).collect();
        let objects = hashes
            .into_iter()
            .map(|h| Ok((h.clone(), hex::encode(self.read_object(&h)?))))
            .collect::<Result<_>>()?;
        Ok(Bundle {
            repo: self.repo.clone(),
            branches: self.branches()?.into_iter().filter(|b| b.shared).collect(),
            events,
            objects,
        })
    }
    pub fn receive(
        &mut self,
        bundle: Bundle,
        allowed_devices: &BTreeSet<String>,
    ) -> Result<Vec<String>> {
        ensure!(bundle.repo == self.repo, "REPOSITORY_MISMATCH");
        for b in &bundle.branches {
            ensure!(b.shared, "PRIVATE_BRANCH_REJECTED");
        }
        for e in &bundle.events {
            ensure!(
                e.branch.shared && allowed_devices.contains(&e.device),
                "UNAUTHORIZED_EVENT"
            );
        }
        let authorized: BTreeSet<_> = bundle.events.iter().flat_map(Event::objects).collect();
        for (h, data) in bundle.objects {
            ensure!(authorized.contains(&h), "UNAUTHORIZED_OBJECT");
            let bytes = hex::decode(data)?;
            ensure!(hash(&bytes) == h, "CORRUPT_OBJECT");
            self.put_object(&bytes)?;
            // Only actual verified inbound bytes grant reuse authorization. A
            // manifest/hash alone cannot relabel private-only local CAS content.
            self.db.execute(
                "INSERT OR IGNORE INTO received_shared_objects VALUES(?)",
                [h],
            )?;
        }
        for e in bundle.events {
            let payload = serde_json::to_string(&e)?;
            let old: Option<String> = self
                .db
                .query_row(
                    "SELECT payload FROM incoming_events WHERE id=?",
                    [&e.id],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(old) = old {
                ensure!(old == payload, "EVENT_ID_REUSED");
            } else {
                self.db.execute(
                    "INSERT INTO incoming_events(id,payload) VALUES(?,?)",
                    params![e.id, payload],
                )?;
            }
        }
        let accepted = self.drain_incoming()?;
        for b in bundle.branches {
            if let Ok(existing) = self.branch(&b.id) {
                ensure!(existing == b, "BRANCH_DESCRIPTOR_CHANGED");
            } else {
                let pending = self
                    .db
                    .prepare("SELECT payload FROM incoming_events")?
                    .query_map([], |r| r.get::<_, String>(0))?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                if pending.iter().any(|payload| {
                    serde_json::from_str::<Event>(payload).is_ok_and(|e| e.branch.id == b.id)
                }) {
                    continue;
                }
                self.db
                    .execute("INSERT INTO branches VALUES(?,?,1)", params![b.id, b.name])?;
                self.rebuild(&b.id)?;
            }
        }
        Ok(accepted)
    }
    fn drain_incoming(&mut self) -> Result<Vec<String>> {
        if !self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM incoming_events WHERE error IS NULL)",
            [],
            |r| r.get::<_, bool>(0),
        )? {
            return Ok(vec![]);
        }
        // One received frame is acknowledged only after activation completes.
        // Batch its validated mutations/receipts behind one SQLite durability
        // barrier; nested per-event savepoints still isolate semantic failures.
        // All referenced objects were installed durably before entering here.
        let name = format!("incoming_{}", id().replace('-', ""));
        self.db.execute_batch(&format!("SAVEPOINT {name}"))?;
        let result = self.drain_incoming_inner();
        match result {
            Ok(accepted) => {
                if let Err(error) = self.db.execute_batch(&format!("RELEASE {name}")) {
                    let _ = self
                        .db
                        .execute_batch(&format!("ROLLBACK TO {name}; RELEASE {name}"));
                    return Err(error.into());
                }
                Ok(accepted)
            }
            Err(error) => {
                self.db
                    .execute_batch(&format!("ROLLBACK TO {name}; RELEASE {name}"))?;
                Err(error)
            }
        }
    }
    fn drain_incoming_inner(&mut self) -> Result<Vec<String>> {
        let mut accepted = vec![];
        loop {
            let staged: Vec<(String, Option<String>)> = self
                .db
                .prepare("SELECT payload,error FROM incoming_events ORDER BY id")?
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect::<std::result::Result<_, _>>()?;
            let staged: Vec<(Event, Option<String>)> = staged
                .into_iter()
                .map(|(payload, error)| Ok((serde_json::from_str(&payload)?, error)))
                .collect::<Result<_>>()?;
            let mut known: BTreeMap<_, _> = self
                .events()?
                .into_iter()
                .map(|e| (e.id.clone(), e))
                .collect();
            let mut committed: BTreeSet<_> = known.keys().cloned().collect();
            for (e, _) in &staged {
                known.entry(e.id.clone()).or_insert_with(|| e.clone());
            }
            let quarantined: BTreeSet<_> = staged
                .iter()
                .filter(|(e, error)| error.is_some() && !committed.contains(&e.id))
                .map(|(e, _)| e.id.clone())
                .collect();
            let authorized = self.shared_object_inventory()?;
            let mut progress = false;
            for (e, error) in &staged {
                if error.is_some() {
                    continue;
                }
                let links = validate_incoming_links(e, &known, &quarantined, &committed);
                let result = match links {
                    Err(error) => Err(error),
                    Ok(()) if !e.objects().iter().all(|h| authorized.contains(h)) => continue,
                    Ok(())
                        if !e
                            .dependencies
                            .iter()
                            .chain(e.changes.iter().flat_map(|c| c.parents.iter()))
                            .all(|p| committed.contains(p)) =>
                    {
                        continue;
                    }
                    Ok(()) => self.commit_ready(e.clone(), false),
                };
                match result {
                    Ok(()) => {
                        self.db
                            .execute("DELETE FROM incoming_events WHERE id=?", [&e.id])?;
                        accepted.push(e.id.clone());
                        committed.insert(e.id.clone());
                        progress = true;
                    }
                    Err(error) if error.downcast_ref::<CausalPrerequisitesPending>().is_some() => {}
                    Err(error) => {
                        self.db.execute(
                            "UPDATE incoming_events SET error=? WHERE id=?",
                            params![format!("{error:#}"), e.id],
                        )?;
                        progress = true;
                    }
                }
            }
            if !progress {
                break;
            }
        }
        Ok(accepted)
    }
    pub fn shared_object_inventory(&self) -> Result<BTreeSet<String>> {
        let mut inventory: BTreeSet<_> = self
            .events()?
            .iter()
            .filter(|e| e.branch.shared)
            .flat_map(Event::objects)
            .collect();
        inventory.extend(
            self.db
                .prepare("SELECT hash FROM received_shared_objects")?
                .query_map([], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?,
        );
        Ok(inventory)
    }
    pub fn known_objects_by_peer(&self, peer: &str) -> Result<BTreeSet<String>> {
        Ok(self
            .db
            .prepare("SELECT hash FROM peer_objects WHERE peer=?")?
            .query_map([peer], |r| r.get(0))?
            .collect::<std::result::Result<_, _>>()?)
    }
    pub fn remember_peer_objects(&mut self, peer: &str, objects: &BTreeSet<String>) -> Result<()> {
        let tx = self.db.savepoint()?;
        tx.execute("DELETE FROM peer_objects WHERE peer=?", [peer])?;
        for h in objects {
            tx.execute("INSERT INTO peer_objects VALUES(?,?)", params![peer, h])?;
        }
        tx.commit()?;
        Ok(())
    }
    /// Manifests and objects can cross separately. A large atomic root remains
    /// invisible in incoming_events until every hash has verified provenance.
    pub fn shared_page(
        &self,
        known: &BTreeSet<String>,
        known_objects: &BTreeSet<String>,
        budget: usize,
    ) -> Result<Bundle> {
        let branches: Vec<_> = self.branches()?.into_iter().filter(|b| b.shared).collect();
        let mut used = serde_json::to_vec(&branches)?.len() + 1024;
        let mut events = vec![];
        for e in self
            .events()?
            .into_iter()
            .filter(|e| e.branch.shared && !known.contains(&e.id))
        {
            let size = serde_json::to_vec(&e)?.len() + 1;
            ensure!(size < budget / 3, "EVENT_METADATA_TOO_LARGE");
            if used + size > budget / 3 {
                break;
            }
            used += size;
            events.push(e);
        }
        let hashes: BTreeSet<_> = events.iter().flat_map(Event::objects).collect();
        let mut objects = BTreeMap::new();
        for h in hashes.into_iter().filter(|h| !known_objects.contains(h)) {
            let bytes = self.read_object(&h)?;
            let size = bytes.len() * 2 + h.len() + 8;
            if used + size > budget {
                continue;
            }
            used += size;
            objects.insert(h, hex::encode(bytes));
        }
        let bundle = Bundle {
            repo: self.repo.clone(),
            branches,
            events,
            objects,
        };
        ensure!(
            serde_json::to_vec(&bundle)?.len() <= budget,
            "BUNDLE_TOO_LARGE"
        );
        Ok(bundle)
    }
    pub fn acknowledge(&self, events: &[String]) -> Result<()> {
        for rev in events {
            self.db
                .execute("UPDATE outbox SET acknowledged=1 WHERE event=?", [rev])?;
        }
        Ok(())
    }
    pub fn status(&self) -> Result<Value> {
        let p = self.projection(&self.active)?;
        let outgoing: i64 = self.db.query_row(
            "SELECT count(*) FROM outbox WHERE acknowledged=0",
            [],
            |r| r.get(0),
        )?;
        Ok(
            json!({"repo":self.repo,"device":self.device,"branch":self.branch(&self.active)?,"generation":self.generation,
            "local_durability":"sqlite-full+verified-objects","outgoing_unacknowledged":outgoing,"incoming_pending":self.db.query_row("SELECT count(*) FROM incoming_events",[],|r|r.get::<_,i64>(0))?,"incoming_quarantined":self.db.query_row("SELECT count(*) FROM incoming_events WHERE error IS NOT NULL",[],|r|r.get::<_,i64>(0))?,"peer_protocol":2,
            "caught_up":"unknown-until-peer-roundtrip","pending_causal_events":p.pending,"unresolved_conflicts":self.conflicts()?.iter().filter(|c|c.resolved_by.is_none()).count()}),
        )
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bundle {
    pub repo: String,
    pub branches: Vec<Branch>,
    pub events: Vec<Event>,
    pub objects: BTreeMap<String, String>,
}

fn persist_projection(
    tx: &Connection,
    branch: &str,
    p: &Projection,
    events: &[Event],
    new_event: Option<&str>,
) -> Result<()> {
    tx.execute("INSERT INTO current_state VALUES(?,?) ON CONFLICT(branch) DO UPDATE SET payload=excluded.payload",params![branch,serde_json::to_string(p)?])?;
    // Derive historical concurrent pairs from the full activated DAG too, so a
    // fast-forward delivered before a competitor cannot hide the earlier pair.
    let mut history: BTreeMap<String, Vec<Version>> = BTreeMap::new();
    for e in events.iter().filter(|e| !p.pending.contains(&e.id)) {
        for c in &e.changes {
            history
                .entry(register(&c.entity, &c.field))
                .or_default()
                .push(Version {
                    revision: e.id.clone(),
                    device: e.device.clone(),
                    created_ms: e.created_ms,
                    parents: c.parents.clone(),
                    value: c.value.clone(),
                });
        }
    }
    let mut conflicts = p.conflicts.clone();
    if let Some(event) = events.iter().find(|e| Some(e.id.as_str()) == new_event) {
        for reviewed in &event.reviewed {
            let payload: Option<String> = tx
                .query_row(
                    "SELECT payload FROM conflicts WHERE id=? AND branch=?",
                    params![reviewed, branch],
                    |r| r.get(0),
                )
                .optional()?;
            if let Some(payload) = payload {
                conflicts.push(serde_json::from_str(&payload)?);
            }
        }
    }
    // Namespace collisions also have a deterministic historical basis. A later
    // rename arriving first must not erase evidence of concurrent same-name
    // placement. Sequential temp replacement is ordered by event context.
    let activated: Vec<_> = events
        .iter()
        .filter(|e| !p.pending.contains(&e.id))
        .collect();
    let mut locations = vec![];
    for e in &activated {
        for c in &e.changes {
            if c.field == "location" {
                locations.push((*e, c));
            }
        }
    }
    for (i, (a, ac)) in locations.iter().enumerate() {
        for (b, bc) in &locations[i + 1..] {
            if ac.entity == bc.entity
                || ac.value["parent"] != bc.value["parent"]
                || name_key(ac.value["name"].as_str().unwrap())?
                    != name_key(bc.value["name"].as_str().unwrap())?
            {
                continue;
            }
            if event_ancestors(a, events).contains(&b.id)
                || event_ancestors(b, events).contains(&a.id)
            {
                continue;
            }
            let mut entities = [ac.entity.clone(), bc.entity.clone()];
            entities.sort();
            let alternatives = [(*a, *ac), (*b, *bc)]
                .into_iter()
                .map(|(e, c)| Version {
                    revision: e.id.clone(),
                    device: e.device.clone(),
                    created_ms: e.created_ms,
                    parents: c.parents.clone(),
                    value: c.value.clone(),
                })
                .collect();
            conflicts.push(make_conflict(
                branch,
                &entities.join(","),
                "namespace",
                "same name / file-directory collision",
                alternatives,
                vec![],
            ));
        }
    }
    // Explicit reviews carry their observed cut. Recover namespace records from
    // it even when resolution was delivered before its prerequisites.
    for e in &activated {
        if !e.reviewed.is_empty() {
            let ancestors = event_ancestors(e, events);
            let cut: Vec<_> = events
                .iter()
                .filter(|prior| ancestors.contains(&prior.id))
                .cloned()
                .collect();
            conflicts.extend(project(branch, &cut)?.conflicts);
        }
    }
    // Every namespace causal cut is represented by an antichain of maximal
    // namespace-changing events. Include arbitrary-width concurrency, not just
    // pairs; content-only events add no new namespace projection. The explicit
    // budget refuses a commit before acknowledgement rather than losing history.
    conflicts.extend(namespace_history(branch, events)?);
    for (key, versions) in &history {
        let candidate = new_event.and_then(|id| versions.iter().position(|v| v.revision == id));
        if new_event.is_some() && candidate.is_none() {
            continue;
        }
        // The overwhelmingly common single-parent chain has no historical
        // concurrency. Detect it in O(revisions), without enumerating pairs.
        if linear_versions(versions) {
            continue;
        }
        let reach = VersionReach::new(versions);
        let (entity, field) = key.rsplit_once('/').unwrap();
        // Appending a validated event cannot change old/old register ancestry.
        // Keep their durable records and derive only new/old pairs on append.
        // Startup/repair still derives the entire historical set from the DAG.
        let pairs: Box<dyn Iterator<Item = (usize, usize)>> = if let Some(i) = candidate {
            Box::new(
                (0..versions.len())
                    .filter(move |&j| j != i)
                    .map(move |j| (i, j)),
            )
        } else {
            Box::new(
                (0..versions.len()).flat_map(|i| ((i + 1)..versions.len()).map(move |j| (i, j))),
            )
        };
        for (i, j) in pairs {
            let a = &versions[i];
            let b = &versions[j];
            if a.value != b.value && !reach.has(i, j) && !reach.has(j, i) {
                let mut bases: Vec<_> = versions
                    .iter()
                    .enumerate()
                    .filter(|(k, _)| reach.has(i, *k) && reach.has(j, *k))
                    .map(|(_, v)| v)
                    .cloned()
                    .collect();
                bases.sort_by(|a, b| a.revision.cmp(&b.revision));
                conflicts.push(make_conflict(
                    branch,
                    entity,
                    field,
                    "concurrent revisions",
                    vec![a.clone(), b.clone()],
                    bases,
                ));
            }
        }
    }
    let conflicts: Vec<_> = conflicts
        .into_iter()
        .map(|c| (c.id.clone(), c))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect();
    let namespace_ids: BTreeSet<_> = conflicts
        .iter()
        .filter(|c| c.field == "namespace")
        .map(|c| c.id.clone())
        .collect();
    let old: Vec<(String, String)> = tx
        .prepare("SELECT id,payload FROM conflicts WHERE branch=? AND json_extract(payload,'$.field')='namespace'")?
        .query_map([branch], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<std::result::Result<_, _>>()?;
    for (id, payload) in old {
        let c: Conflict = serde_json::from_str(&payload)?;
        if c.field == "namespace" && !namespace_ids.contains(&id) {
            tx.execute("DELETE FROM conflicts WHERE id=?", [id])?;
        }
    }
    {
        let mut insert=tx.prepare_cached("INSERT INTO conflicts(id,branch,payload) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload WHERE conflicts.payload<>excluded.payload")?;
        for c in &conflicts {
            insert.execute(params![c.id, branch, serde_json::to_string(c)?])?;
        }
    }
    for e in events {
        if !p.pending.contains(&e.id) && new_event.is_none_or(|id| id == e.id) {
            for reviewed in &e.reviewed {
                let c = conflicts
                    .iter()
                    .find(|c| &c.id == reviewed)
                    .context("INVALID_CONFLICT_REVIEW")?;
                let valid_review = if c.field == "namespace" {
                    e.changes.iter().any(|change| {
                        change.field == "location"
                            && c.entity.split(',').any(|entity| entity == change.entity)
                            && c.alternatives
                                .iter()
                                .any(|v| change.parents.contains(&v.revision))
                            && c.alternatives
                                .iter()
                                .filter(|v| {
                                    events
                                        .iter()
                                        .find(|prior| prior.id == v.revision)
                                        .is_some_and(|prior| {
                                            prior.changes.iter().any(|ch| {
                                                ch.entity == change.entity
                                                    && ch.field == "location"
                                                    && ch.value == v.value
                                            })
                                        })
                                })
                                .all(|v| change.parents.contains(&v.revision))
                    })
                } else {
                    e.changes.iter().any(|change| {
                        change.entity == c.entity
                            && change.field == c.field
                            && c.alternatives
                                .iter()
                                .all(|v| change.parents.contains(&v.revision))
                    })
                };
                ensure!(valid_review, "INVALID_CONFLICT_REVIEW");
                // Concurrent review events converge on a stable receipt as well.
                tx.execute("UPDATE conflicts SET resolved_by=CASE WHEN resolved_by IS NULL OR resolved_by < ? THEN ? ELSE resolved_by END WHERE id=? AND branch=?",params![e.id,e.id,reviewed,branch])?;
            }
        }
    }
    Ok(())
}
fn ancestors(version: &Version, versions: &[Version]) -> BTreeSet<String> {
    let index: BTreeMap<_, _> = versions.iter().map(|v| (v.revision.as_str(), v)).collect();
    let mut found = BTreeSet::new();
    let mut todo = version.parents.clone();
    while let Some(revision) = todo.pop() {
        if found.insert(revision.clone())
            && let Some(v) = index.get(revision.as_str())
        {
            todo.extend(v.parents.iter().cloned());
        }
    }
    found
}
fn event_ancestors(event: &Event, events: &[Event]) -> BTreeSet<String> {
    let index: BTreeMap<_, _> = events.iter().map(|e| (e.id.as_str(), e)).collect();
    let mut seen = BTreeSet::new();
    let mut todo = event.dependencies.clone();
    todo.extend(event.changes.iter().flat_map(|c| c.parents.iter().cloned()));
    while let Some(revision) = todo.pop() {
        if seen.insert(revision.clone())
            && let Some(e) = index.get(revision.as_str())
        {
            todo.extend(e.dependencies.iter().cloned());
            todo.extend(e.changes.iter().flat_map(|c| c.parents.iter().cloned()));
        }
    }
    seen
}
fn linear_versions(versions: &[Version]) -> bool {
    let mut children = BTreeSet::new();
    let mut roots = 0;
    for v in versions {
        if v.parents.is_empty() {
            roots += 1;
        }
        if v.parents.len() > 1 || v.parents.iter().any(|p| !children.insert(p)) {
            return false;
        }
    }
    roots == 1
}
struct VersionReach {
    rows: Vec<Vec<u64>>,
}
impl VersionReach {
    fn new(versions: &[Version]) -> Self {
        let index: BTreeMap<_, _> = versions
            .iter()
            .enumerate()
            .map(|(i, v)| (v.revision.as_str(), i))
            .collect();
        let mut children = vec![vec![]; versions.len()];
        let mut degree = vec![0; versions.len()];
        let mut parents = vec![vec![]; versions.len()];
        for (i, v) in versions.iter().enumerate() {
            for parent in &v.parents {
                if let Some(&j) = index.get(parent.as_str()) {
                    parents[i].push(j);
                    children[j].push(i);
                    degree[i] += 1;
                }
            }
        }
        let mut ready: Vec<_> = (0..versions.len()).filter(|&i| degree[i] == 0).collect();
        let mut rows = vec![vec![0; versions.len().div_ceil(64)]; versions.len()];
        while let Some(i) = ready.pop() {
            for &parent in &parents[i] {
                for word in 0..rows[i].len() {
                    rows[i][word] |= rows[parent][word];
                }
                rows[i][parent / 64] |= 1 << (parent % 64);
            }
            for &child in &children[i] {
                degree[child] -= 1;
                if degree[child] == 0 {
                    ready.push(child);
                }
            }
        }
        Self { rows }
    }
    fn has(&self, child: usize, parent: usize) -> bool {
        self.rows[child][parent / 64] & (1 << (parent % 64)) != 0
    }
}
fn validate_known_links(
    event: &Event,
    known: &BTreeMap<String, Event>,
    quarantined: &BTreeSet<String>,
) -> Result<()> {
    validate_incoming_links(event, known, quarantined, &BTreeSet::new())
}
fn validate_incoming_links(
    event: &Event,
    known: &BTreeMap<String, Event>,
    quarantined: &BTreeSet<String>,
    committed: &BTreeSet<String>,
) -> Result<()> {
    validate_direct_links(event, known, quarantined)?;
    let mut visited = BTreeSet::new();
    let mut todo = event.dependencies.clone();
    todo.extend(event.changes.iter().flat_map(|c| c.parents.iter().cloned()));
    while let Some(revision) = todo.pop() {
        ensure!(revision != event.id, "CAUSAL_CYCLE");
        if committed.contains(&revision) {
            continue;
        }
        if visited.insert(revision.clone())
            && let Some(parent) = known.get(&revision)
        {
            todo.extend(parent.dependencies.iter().cloned());
            todo.extend(
                parent
                    .changes
                    .iter()
                    .flat_map(|c| c.parents.iter().cloned()),
            );
        }
    }
    Ok(())
}
fn validate_direct_links(
    event: &Event,
    known: &BTreeMap<String, Event>,
    quarantined: &BTreeSet<String>,
) -> Result<()> {
    for dependency in &event.dependencies {
        ensure!(
            !quarantined.contains(dependency),
            "QUARANTINED_CAUSAL_ANCESTOR: {dependency}"
        );
        if let Some(parent) = known.get(dependency) {
            ensure!(
                parent.branch == event.branch,
                "INVALID_CAUSAL_DEPENDENCY: {dependency}"
            );
        }
    }
    for change in &event.changes {
        for revision in &change.parents {
            ensure!(
                !quarantined.contains(revision),
                "QUARANTINED_CAUSAL_ANCESTOR: {revision}"
            );
            if let Some(parent) = known.get(revision) {
                ensure!(
                    parent.branch == event.branch
                        && parent
                            .changes
                            .iter()
                            .any(|c| c.entity == change.entity && c.field == change.field),
                    "INVALID_CAUSAL_PARENT: {revision}"
                );
            }
        }
    }
    Ok(())
}
fn activated_ids(events: &[Event]) -> BTreeSet<String> {
    let mut degrees = BTreeMap::new();
    let mut children: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    let mut ready = BTreeSet::new();
    for e in events {
        let parents: BTreeSet<_> = e
            .dependencies
            .iter()
            .chain(e.changes.iter().flat_map(|c| c.parents.iter()))
            .collect();
        degrees.insert(e.id.as_str(), parents.len());
        if parents.is_empty() {
            ready.insert(e.id.as_str());
        }
        for parent in parents {
            children
                .entry(parent.as_str())
                .or_default()
                .push(e.id.as_str());
        }
    }
    let mut active = BTreeSet::new();
    while let Some(id) = ready.pop_first() {
        active.insert(id.to_owned());
        if let Some(children) = children.get(id) {
            for child in children {
                let degree = degrees.get_mut(child).unwrap();
                *degree -= 1;
                if *degree == 0 {
                    ready.insert(*child);
                }
            }
        }
    }
    active
}
fn namespace_history(branch: &str, events: &[Event]) -> Result<Vec<Conflict>> {
    let graph = FactGraph::events(events);
    let cones = NamespaceCones::new(events)?;
    let mut alive: BTreeMap<&str, Vec<(usize, &Change)>> = BTreeMap::new();
    for (i, e) in events.iter().enumerate() {
        for c in &e.changes {
            if c.field == "alive" {
                alive.entry(&c.entity).or_default().push((i, c));
            }
        }
    }
    let mut conflicts = BTreeMap::new();
    for members in cones.members() {
        let mut relevant = BTreeSet::new();
        for (i, e) in events.iter().enumerate() {
            if e.changes.iter().any(|c| {
                members.contains(&c.entity)
                    && (["kind", "location"].contains(&c.field.as_str())
                        || (c.field == "alive"
                            && (c.value == false
                                || c.parents.iter().any(|parent| {
                                    graph.index.get(parent.as_str()).is_some_and(|&j| {
                                        events[j].changes.iter().any(|p| {
                                            p.entity == c.entity
                                                && p.field == "alive"
                                                && p.value == false
                                        })
                                    })
                                }))))
            }) {
                relevant.insert(i);
            }
        }
        let primary: Vec<_> = relevant
            .iter()
            .map(|&i| (graph.closure(i, false), graph.closure(i, true)))
            .collect();
        // Pure TRUE presence assertions cannot change namespace when there is
        // no tombstone. If tombstones exist, retain rank extrema per unbranched
        // TRUE segment and topology/false-revision causal relationship signature.
        // This preserves possible boolean winners without counting every save.
        for entity in &members {
            let Some(versions) = alive.get(entity.as_str()) else {
                continue;
            };
            let false_ids: Vec<_> = versions
                .iter()
                .filter(|(_, c)| c.value == false)
                .map(|(i, _)| *i)
                .collect();
            if false_ids.is_empty() {
                continue;
            }
            let register = FactGraph::register(events, versions);
            let false_context: Vec<_> = false_ids
                .iter()
                .map(|&i| (register.closure(i, false), register.closure(i, true)))
                .collect();
            let segments = register.segments();
            let mut groups: BTreeMap<(usize, Vec<u8>), (usize, usize)> = BTreeMap::new();
            for &(i, c) in versions {
                if c.value != true || relevant.contains(&i) {
                    continue;
                }
                let signature = primary
                    .iter()
                    .chain(&false_context)
                    .map(|(anc, desc)| {
                        if anc.contains(&i) {
                            1
                        } else if desc.contains(&i) {
                            2
                        } else {
                            0
                        }
                    })
                    .collect();
                let range = groups.entry((segments[i], signature)).or_insert((i, i));
                let rank = |j: usize| (&events[j].device, &events[j].id);
                if rank(i) < rank(range.0) {
                    range.0 = i;
                }
                if rank(i) > rank(range.1) {
                    range.1 = i;
                }
            }
            for (min, max) in groups.values() {
                relevant.extend([*min, *max]);
            }
        }
        let relevant: Vec<_> = relevant.into_iter().collect();
        let ancestors: Vec<_> = relevant.iter().map(|&i| graph.closure(i, false)).collect();
        let mut antichains: Vec<Vec<usize>> = vec![vec![]];
        let mut competing_cuts = 0;
        for (i, &event) in relevant.iter().enumerate() {
            let before = antichains.len();
            for index in 0..before {
                if !antichains[index].iter().all(|&j| {
                    !ancestors[i].contains(&relevant[j]) && !ancestors[j].contains(&event)
                }) {
                    continue;
                }
                let mut selected = antichains[index].clone();
                selected.push(i);
                if selected.len() > 1 {
                    competing_cuts += 1;
                    ensure!(
                        competing_cuts <= MAX_NAMESPACE_CUTS,
                        "NAMESPACE_HISTORY_LIMIT: more than {MAX_NAMESPACE_CUTS} competing topology cuts in one dependency cone; commit refused without discarding conflicts"
                    );
                }
                let mut ids = BTreeSet::new();
                for &j in &selected {
                    ids.extend(&ancestors[j]);
                    ids.insert(relevant[j]);
                }
                let cut: Vec<_> = ids.into_iter().map(|j| events[j].clone()).collect();
                for c in project(branch, &cut)?.conflicts.into_iter().filter(|c| {
                    c.field == "namespace" && c.entity.split(',').any(|e| members.contains(e))
                }) {
                    conflicts.insert(c.id.clone(), c);
                }
                antichains.push(selected);
            }
        }
    }
    Ok(conflicts.into_values().collect())
}
struct FactGraph {
    index: BTreeMap<String, usize>,
    parents: Vec<Vec<usize>>,
    children: Vec<Vec<usize>>,
}
impl FactGraph {
    fn events(events: &[Event]) -> Self {
        let index: BTreeMap<_, _> = events
            .iter()
            .enumerate()
            .map(|(i, e)| (e.id.clone(), i))
            .collect();
        let parents: Vec<_> = events
            .iter()
            .map(|e| {
                e.dependencies
                    .iter()
                    .chain(e.changes.iter().flat_map(|c| c.parents.iter()))
                    .filter_map(|p| index.get(p).copied())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect()
            })
            .collect();
        Self::from_parents(index, parents)
    }
    fn register(events: &[Event], versions: &[(usize, &Change)]) -> Self {
        let index: BTreeMap<_, _> = versions
            .iter()
            .map(|(i, _)| (events[*i].id.clone(), *i))
            .collect();
        let mut parents = vec![vec![]; events.len()];
        for &(i, c) in versions {
            parents[i] = c
                .parents
                .iter()
                .filter_map(|p| index.get(p).copied())
                .collect();
        }
        let graph = Self::from_parents(index.clone(), parents);
        // Explicit review can list many ancestors as parents. Such redundant
        // edges are evidence, not new branches in an unchanged-TRUE segment.
        // Reduce only this analysis graph; immutable event parents stay intact.
        let mut degree: Vec<_> = graph.parents.iter().map(Vec::len).collect();
        let mut ready: Vec<_> = (0..degree.len()).filter(|&i| degree[i] == 0).collect();
        let mut rank = vec![0; degree.len()];
        let mut sequence = 0;
        while let Some(i) = ready.pop() {
            rank[i] = sequence;
            sequence += 1;
            for &child in &graph.children[i] {
                degree[child] -= 1;
                if degree[child] == 0 {
                    ready.push(child);
                }
            }
        }
        let mut reduced = graph.parents.clone();
        for ps in &mut reduced {
            if ps.len() < 2 {
                continue;
            }
            ps.sort_by_key(|&p| std::cmp::Reverse(rank[p]));
            let mut covered = BTreeSet::new();
            ps.retain(|&p| {
                if covered.contains(&p) {
                    false
                } else {
                    covered.extend(graph.closure(p, false));
                    true
                }
            });
        }
        Self::from_parents(index, reduced)
    }
    fn from_parents(index: BTreeMap<String, usize>, parents: Vec<Vec<usize>>) -> Self {
        let mut children = vec![vec![]; parents.len()];
        for (i, ps) in parents.iter().enumerate() {
            for &p in ps {
                children[p].push(i);
            }
        }
        Self {
            index,
            parents,
            children,
        }
    }
    fn closure(&self, id: usize, forward: bool) -> BTreeSet<usize> {
        let edges = if forward {
            &self.children
        } else {
            &self.parents
        };
        let mut found = BTreeSet::new();
        let mut todo = edges[id].clone();
        while let Some(i) = todo.pop() {
            if found.insert(i) {
                todo.extend(&edges[i]);
            }
        }
        found
    }
    fn segments(&self) -> Vec<usize> {
        let mut degree: Vec<_> = self.parents.iter().map(Vec::len).collect();
        let mut ready: Vec<_> = (0..degree.len()).filter(|&i| degree[i] == 0).collect();
        let mut segment: Vec<_> = (0..degree.len()).collect();
        while let Some(i) = ready.pop() {
            if self.parents[i].len() == 1 && self.children[self.parents[i][0]].len() == 1 {
                segment[i] = segment[self.parents[i][0]];
            }
            for &child in &self.children[i] {
                degree[child] -= 1;
                if degree[child] == 0 {
                    ready.push(child);
                }
            }
        }
        segment
    }
}
struct NamespaceCones {
    parents: BTreeMap<String, BTreeSet<String>>,
    entity_slots: BTreeMap<String, BTreeSet<(String, String)>>,
    slots: BTreeMap<(String, String), BTreeSet<String>>,
}
impl NamespaceCones {
    fn new(events: &[Event]) -> Result<Self> {
        let mut result = Self {
            parents: BTreeMap::new(),
            entity_slots: BTreeMap::new(),
            slots: BTreeMap::new(),
        };
        for e in events {
            for c in &e.changes {
                result.parents.entry(c.entity.clone()).or_default();
                if c.field == "location" {
                    let parent = c.value["parent"].as_str().unwrap();
                    if parent != ROOT {
                        result
                            .parents
                            .entry(c.entity.clone())
                            .or_default()
                            .insert(parent.to_owned());
                        result.parents.entry(parent.to_owned()).or_default();
                    }
                    let key = (
                        parent.to_owned(),
                        name_key(c.value["name"].as_str().unwrap())?,
                    );
                    result
                        .entity_slots
                        .entry(c.entity.clone())
                        .or_default()
                        .insert(key.clone());
                    result
                        .slots
                        .entry(key)
                        .or_default()
                        .insert(c.entity.clone());
                }
            }
        }
        Ok(result)
    }
    fn members(&self) -> BTreeSet<BTreeSet<String>> {
        let mut cones = BTreeSet::new();
        for entity in self.parents.keys() {
            let mut members = BTreeSet::new();
            let mut visited_slots = BTreeSet::new();
            let mut pending = vec![entity.clone()];
            while let Some(next) = pending.pop() {
                if !members.insert(next.clone()) {
                    continue;
                }
                pending.extend(self.parents[&next].iter().cloned());
                if let Some(slots) = self.entity_slots.get(&next) {
                    for slot in slots {
                        if visited_slots.insert(slot.clone()) {
                            pending.extend(self.slots[slot].iter().cloned());
                        }
                    }
                }
            }
            // Ancestors and slot competitors can affect this entry's visibility.
            // Sharing an ancestor alone does not make sibling histories interact.
            cones.insert(members);
        }
        cones
    }
}

/// Complete topological activation followed by per-register ancestry. Unknown
/// parents keep the entire atomic event pending; child-before-parent is durable.
pub fn project(branch: &str, events: &[Event]) -> Result<Projection> {
    let by_id: BTreeMap<_, _> = events.iter().map(|e| (e.id.clone(), e)).collect();
    for e in events {
        for c in &e.changes {
            for parent in &c.parents {
                if let Some(parent) = by_id.get(parent) {
                    ensure!(
                        parent
                            .changes
                            .iter()
                            .any(|pc| pc.entity == c.entity && pc.field == c.field),
                        "INVALID_CAUSAL_PARENT"
                    );
                }
            }
        }
    }
    let active = activated_ids(events);
    // A known closed dependency cycle is malformed, rather than permanently pending.
    let pending: Vec<_> = by_id
        .keys()
        .filter(|e| !active.contains(*e))
        .cloned()
        .collect();
    if !pending.is_empty() {
        let has_missing = events.iter().any(|e| {
            e.dependencies.iter().any(|p| !by_id.contains_key(p))
                || e.changes
                    .iter()
                    .any(|c| c.parents.iter().any(|p| !by_id.contains_key(p)))
        });
        ensure!(has_missing, "CAUSAL_CYCLE");
    }
    let mut registers: BTreeMap<String, Vec<Version>> = BTreeMap::new();
    for e in events.iter().filter(|e| active.contains(&e.id)) {
        for c in &e.changes {
            registers
                .entry(register(&c.entity, &c.field))
                .or_default()
                .push(Version {
                    revision: e.id.clone(),
                    device: e.device.clone(),
                    created_ms: e.created_ms,
                    parents: c.parents.clone(),
                    value: c.value.clone(),
                });
        }
    }
    let mut p = Projection {
        pending,
        ..Default::default()
    };
    for (key, mut versions) in registers {
        // All ancestors are dominated before the stable concurrent rank is used.
        let dominated: BTreeSet<_> = versions
            .iter()
            .flat_map(|v| v.parents.iter().cloned())
            .collect();
        let mut heads: Vec<_> = versions
            .iter()
            .filter(|v| !dominated.contains(&v.revision))
            .cloned()
            .collect();
        heads.sort_by(|a, b| (&a.device, &a.revision).cmp(&(&b.device, &b.revision)));
        let winner = heads.last().unwrap();
        let (entity, field) = key.rsplit_once('/').unwrap();
        let entry = p.entries.entry(entity.into()).or_insert_with(|| Entry {
            id: entity.into(),
            ..Default::default()
        });
        entry
            .revisions
            .insert(field.into(), winner.revision.clone());
        entry.modified_ms = entry.modified_ms.max(winner.created_ms);
        match field {
            "kind" => entry.kind = winner.value.as_str().unwrap().into(),
            "location" => {
                entry.parent = winner.value["parent"].as_str().unwrap().into();
                entry.name = winner.value["name"].as_str().unwrap().into();
            }
            "alive" => entry.alive = winner.value.as_bool().unwrap(),
            "content" => entry.content = winner.value.as_str().map(str::to_owned),
            _ => {}
        }
        if heads.len() > 1
            && heads
                .iter()
                .map(|v| serde_json::to_string(&v.value).unwrap())
                .collect::<BTreeSet<_>>()
                .len()
                > 1
        {
            versions.sort_by(|a, b| a.revision.cmp(&b.revision));
            // Pair records have stable identity even when a third competitor is
            // learned later. No delivery-dependent subset record is persisted.
            for (i, a) in heads.iter().enumerate() {
                for b in &heads[i + 1..] {
                    if a.value != b.value {
                        let aa = ancestors(a, &versions);
                        let ba = ancestors(b, &versions);
                        let bases = versions
                            .iter()
                            .filter(|v| aa.contains(&v.revision) && ba.contains(&v.revision))
                            .cloned()
                            .collect();
                        p.conflicts.push(make_conflict(
                            branch,
                            entity,
                            field,
                            "concurrent revisions",
                            vec![a.clone(), b.clone()],
                            bases,
                        ));
                    }
                }
            }
        }
        p.heads.insert(key, heads);
    }
    // Make a deterministic, acyclic visible namespace, retaining suppressed
    // entities and their complete registers in conflicts/current-state.
    let ids: Vec<_> = p.entries.keys().cloned().collect();
    let mut slots: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    for entity in &ids {
        let e = &p.entries[entity];
        if e.alive {
            slots
                .entry((e.parent.clone(), name_key(&e.name)?))
                .or_default()
                .push(entity.clone());
        }
    }
    for members in slots.values().filter(|m| m.len() > 1) {
        let mut alternatives = vec![];
        for entity in members {
            alternatives.extend(p.heads[&register(entity, "location")].clone());
        }
        let winner = members.iter().max().unwrap();
        for entity in members {
            if entity != winner {
                p.entries.get_mut(entity).unwrap().alive = false;
            }
        }
        for (i, a) in members.iter().enumerate() {
            for b in &members[i + 1..] {
                let pair = [
                    p.heads[&register(a, "location")].clone(),
                    p.heads[&register(b, "location")].clone(),
                ]
                .concat();
                p.conflicts.push(make_conflict(
                    branch,
                    &format!("{a},{b}"),
                    "namespace",
                    "same name / file-directory collision",
                    pair,
                    vec![],
                ));
            }
        }
    }
    for entity in &ids {
        let mut cursor = entity.clone();
        let mut visited = BTreeSet::new();
        let mut invalid = false;
        while cursor != ROOT {
            if !visited.insert(cursor.clone()) {
                invalid = true;
                break;
            }
            if let Some(e) = p.entries.get(&cursor) {
                if !e.alive || (cursor != *entity && e.kind != "directory") {
                    invalid = true;
                    break;
                }
                cursor = e.parent.clone();
            } else {
                invalid = true;
                break;
            }
        }
        if invalid && p.entries[entity].alive {
            p.entries.get_mut(entity).unwrap().alive = false;
            let alternatives = p
                .heads
                .get(&register(entity, "location"))
                .cloned()
                .unwrap_or_default();
            p.conflicts.push(make_conflict(
                branch,
                entity,
                "namespace",
                "deleted/missing parent or directory cycle",
                alternatives,
                vec![],
            ));
        }
    }
    p.conflicts.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(p)
}
fn make_conflict(
    branch: &str,
    entity: &str,
    field: &str,
    reason: &str,
    mut alternatives: Vec<Version>,
    bases: Vec<Version>,
) -> Conflict {
    alternatives.sort_by(|a, b| a.revision.cmp(&b.revision));
    let revs: Vec<_> = alternatives.iter().map(|v| &v.revision).collect();
    let conflict_id = hash(&serde_json::to_vec(&(branch, entity, field, reason, revs)).unwrap());
    Conflict {
        id: conflict_id,
        branch: branch.into(),
        entity: entity.into(),
        field: field.into(),
        reason: reason.into(),
        alternatives,
        bases,
        resolved_by: None,
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

#[cfg(windows)]
pub(crate) fn install_object(temp: &Path, dest: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn MoveFileExW(old: *const u16, new: *const u16, flags: u32) -> i32;
    }
    let old: Vec<_> = temp.as_os_str().encode_wide().chain(Some(0)).collect();
    let new: Vec<_> = dest.as_os_str().encode_wide().chain(Some(0)).collect();
    if unsafe { MoveFileExW(old.as_ptr(), new.as_ptr(), 8) } == 0 {
        bail!(std::io::Error::last_os_error());
    }
    Ok(())
}
#[cfg(not(windows))]
pub(crate) fn install_object(temp: &Path, dest: &Path) -> Result<()> {
    fs::rename(temp, dest)?;
    fs::File::open(dest.parent().unwrap())?.sync_all()?;
    Ok(())
}
