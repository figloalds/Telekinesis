//! Authoritative version journal and cached current namespace. No cone replay.
use crate::content::local::{Kind as ObjectKind, LocalContent};
use crate::content::working::Basis;
use crate::model::content::{Digest, FileContentRef};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
};
use unicode_normalization::UnicodeNormalization;
use uuid::Uuid;

pub const ROOT: &str = "root";
pub const MAIN: &str = "main";
pub fn id() -> String {
    Uuid::new_v4().to_string()
}
pub fn now() -> u64 {
    crate::util::time::now_ms()
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    File,
    Directory,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Times {
    pub created: u64,
    pub accessed: u64,
    pub modified: u64,
    pub changed: u64,
}
impl Default for Times {
    fn default() -> Self {
        let t = now().saturating_mul(10_000) + 116_444_736_000_000_000;
        Self {
            created: t,
            accessed: t,
            modified: t,
            changed: t,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Entry {
    pub id: String,
    pub parent: String,
    pub name: String,
    pub kind: EntryKind,
    pub content: Option<FileContentRef>,
    pub times: Times,
    pub mode: u32,
    pub attributes: u32,
    pub deleted: bool,
    #[serde(skip)]
    pub working_len: Option<u64>,
}
impl Entry {
    pub fn root() -> Self {
        Self {
            id: ROOT.into(),
            parent: String::new(),
            name: String::new(),
            kind: EntryKind::Directory,
            content: None,
            times: Times {
                created: 0,
                accessed: 0,
                modified: 0,
                changed: 0,
            },
            mode: 0o755,
            attributes: 0x10,
            deleted: false,
            working_len: None,
        }
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Create,
    Content,
    Rename,
    Delete,
    Mkdir,
    Times,
    Resolve,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Event {
    pub id: String,
    pub entity: String,
    pub parents: Vec<String>,
    pub operation: Operation,
    pub state: Entry,
    pub branch: String,
    pub author: String,
    pub clock: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub id: String,
    pub root: Digest,
    pub frontier: BTreeMap<String, Vec<String>>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TreeEntry {
    pub name: String,
    pub entity: String,
    pub kind: EntryKind,
    pub content: Option<FileContentRef>,
    pub tree: Option<Digest>,
    pub times: Times,
    pub mode: u32,
    pub attributes: u32,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Tree {
    pub entries: Vec<TreeEntry>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Conflict {
    pub entity: String,
    pub branch: String,
    pub visible: String,
    pub heads: Vec<Event>,
    pub resolved: bool,
}
impl Tree {
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut b = b"TKTR\x01".to_vec();
        b.extend(serde_json::to_vec(self)?);
        Ok(b)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(bytes.starts_with(b"TKTR\x01"), "INVALID_TREE");
        let tree: Self = serde_json::from_slice(&bytes[5..])?;
        tree.validate()?;
        Ok(tree)
    }
    fn validate(&self) -> Result<()> {
        let mut last = None;
        for e in &self.entries {
            let key = name_key(&e.name)?;
            ensure!(last.as_ref().is_none_or(|old| old < &key), "UNSORTED_TREE");
            last = Some(key);
            ensure!(!e.entity.is_empty() && e.entity != ROOT, "INVALID_ENTITY");
            ensure!(
                match e.kind {
                    EntryKind::File => e.content.is_some() && e.tree.is_none(),
                    EntryKind::Directory => e.content.is_none() && e.tree.is_some(),
                },
                "INVALID_TREE_ENTRY"
            );
            if let Some(content) = &e.content {
                content.validate()?;
            }
        }
        Ok(())
    }
}
#[derive(Clone, Default)]
pub struct Projection {
    pub entries: Arc<BTreeMap<String, Entry>>,
    pub heads: Arc<BTreeMap<String, BTreeSet<String>>>,
    pub visible: Arc<BTreeMap<String, String>>,
    pub anchors: Arc<BTreeMap<String, String>>,
    children: Arc<BTreeMap<String, BTreeMap<String, String>>>,
    slots: Arc<BTreeMap<(String, String), BTreeSet<String>>>,
    trees: BTreeMap<String, Digest>,
    dirty: BTreeSet<String>,
}
impl Projection {
    fn empty() -> Self {
        let mut p = Self::default();
        Arc::make_mut(&mut p.entries).insert(ROOT.into(), Entry::root());
        p.dirty.insert(ROOT.into());
        p
    }
    fn index(&mut self, entry: Entry) -> Result<()> {
        if let Some(old) = self.entries.get(&entry.id) {
            if !old.deleted {
                let key = name_key(&old.name)?;
                let slot = (old.parent.clone(), key.clone());
                let slots = Arc::make_mut(&mut self.slots);
                if let Some(members) = slots.get_mut(&slot) {
                    members.remove(&old.id);
                    if members.is_empty() {
                        slots.remove(&slot);
                    }
                }
                if let Some(children) = Arc::make_mut(&mut self.children).get_mut(&old.parent) {
                    if let Some(winner) = slots.get(&slot).and_then(|s| s.first()) {
                        children.insert(key, winner.clone());
                    } else {
                        children.remove(&key);
                    }
                }
            }
            self.dirty.insert(old.parent.clone());
        }
        if !entry.deleted && entry.id != ROOT {
            let children = Arc::make_mut(&mut self.children)
                .entry(entry.parent.clone())
                .or_default();
            let key = name_key(&entry.name)?;
            let members = Arc::make_mut(&mut self.slots)
                .entry((entry.parent.clone(), key.clone()))
                .or_default();
            members.insert(entry.id.clone());
            children.insert(key, members.first().unwrap().clone());
            self.dirty.insert(entry.parent.clone());
        }
        Arc::make_mut(&mut self.entries).insert(entry.id.clone(), entry);
        Ok(())
    }
    pub fn lookup(&self, path: &str) -> Result<Entry> {
        let mut entry = self.entries.get(ROOT).unwrap();
        for name in components(path)? {
            ensure!(
                entry.kind == EntryKind::Directory && !entry.deleted,
                "NOT_A_DIRECTORY"
            );
            let entity = self
                .children
                .get(&entry.id)
                .and_then(|c| c.get(&name_key(name).ok()?))
                .context("NOT_FOUND")?;
            entry = self.entries.get(entity).context("NOT_FOUND")?;
        }
        ensure!(!entry.deleted, "NOT_FOUND");
        Ok(entry.clone())
    }
    pub fn children(&self, entity: &str) -> Vec<Entry> {
        self.children
            .get(entity)
            .into_iter()
            .flat_map(|c| c.values())
            .filter_map(|id| self.entries.get(id))
            .filter(|e| !e.deleted)
            .cloned()
            .collect()
    }
    pub fn path(&self, entity: &str) -> Result<String> {
        let mut names = Vec::new();
        let mut current = entity;
        let mut seen = BTreeSet::new();
        while current != ROOT {
            ensure!(seen.insert(current), "DIRECTORY_CYCLE");
            let e = self.entries.get(current).context("NOT_FOUND")?;
            names.push(e.name.as_str());
            current = &e.parent;
        }
        names.reverse();
        Ok(format!("/{}", names.join("/")))
    }
}
fn components(path: &str) -> Result<Vec<&str>> {
    let components: Vec<_> = path.split(['/', '\\']).filter(|s| !s.is_empty()).collect();
    for name in &components {
        name_key(name)?;
    }
    Ok(components)
}
pub fn name_key(name: &str) -> Result<String> {
    ensure!(
        !name.is_empty()
            && name != "."
            && name != ".."
            && !name.contains(['/', '\\', '\0'])
            && name.encode_utf16().count() <= 255,
        "INVALID_NAME"
    );
    Ok(name.nfc().flat_map(char::to_lowercase).collect())
}

pub struct Store {
    pub root: PathBuf,
    pub device: String,
    pub repo: String,
    pub active: String,
    pub content: LocalContent,
    pub(crate) db: Connection,
    views: BTreeMap<String, Projection>,
    clock: u64,
    pending_since: Option<std::time::Instant>,
}
const SCHEMA:&str="
CREATE TABLE config(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE events(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,branch TEXT NOT NULL,entity TEXT NOT NULL,event BLOB NOT NULL);
CREATE INDEX entity_versions ON events(entity,seq);
CREATE TABLE waiting(id TEXT PRIMARY KEY,event BLOB NOT NULL);
CREATE TABLE branches(id TEXT PRIMARY KEY,name TEXT UNIQUE NOT NULL,snapshot TEXT);
CREATE TABLE current(branch TEXT NOT NULL,entity TEXT NOT NULL,state BLOB NOT NULL,visible TEXT NOT NULL,PRIMARY KEY(branch,entity));
CREATE TABLE heads(branch TEXT NOT NULL,entity TEXT NOT NULL,version TEXT NOT NULL,PRIMARY KEY(branch,entity,version));
CREATE TABLE snapshots(id TEXT PRIMARY KEY,root BLOB NOT NULL,frontier BLOB NOT NULL);
CREATE TABLE seal_receipts(operation TEXT PRIMARY KEY,event TEXT NOT NULL,modified_ns TEXT NOT NULL);
CREATE TABLE conflict_history(branch TEXT NOT NULL,entity TEXT NOT NULL,version TEXT NOT NULL,PRIMARY KEY(branch,entity,version));
PRAGMA application_id=1414223440; PRAGMA user_version=1;
";
impl Store {
    pub fn open(root: &Path, repo: Option<&str>) -> Result<Self> {
        std::fs::create_dir_all(root)?;
        let content = LocalContent::open(root)?;
        let db = Connection::open(root.join("metadata.sqlite"))?;
        db.busy_timeout(std::time::Duration::from_secs(5))?;
        db.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON;",
        )?;
        let app: i64 = db.query_row("PRAGMA application_id", [], |r| r.get(0))?;
        if app == 0 {
            let count: i64 =
                db.query_row("SELECT count(*) FROM sqlite_schema", [], |r| r.get(0))?;
            ensure!(count == 0, "UNSUPPORTED_METADATA_FORMAT");
            let tx = db.unchecked_transaction()?;
            tx.execute_batch(SCHEMA)?;
            for (key, value) in [
                ("device", id()),
                ("repo", repo.map(str::to_owned).unwrap_or_else(id)),
                ("active", MAIN.into()),
                ("clock", "0".into()),
            ] {
                tx.execute("INSERT INTO config VALUES(?1,?2)", params![key, value])?;
            }
            tx.execute("INSERT INTO branches VALUES(?1,?1,NULL)", [MAIN])?;
            tx.commit()?;
        } else {
            ensure!(
                app == 1414223440
                    && db.query_row("PRAGMA user_version", [], |r| r.get::<_, u32>(0))? == 1,
                "UNSUPPORTED_METADATA_FORMAT"
            );
        }
        let config = |key: &str| -> Result<String> {
            Ok(db.query_row("SELECT value FROM config WHERE key=?1", [key], |r| r.get(0))?)
        };
        let device = config("device")?;
        let repository = config("repo")?;
        let active = config("active")?;
        if let Some(repo) = repo {
            ensure!(repo == repository, "REPOSITORY_MISMATCH");
        }
        let mut store = Self {
            root: root.to_owned(),
            device,
            repo: repository,
            active,
            content,
            db,
            views: BTreeMap::new(),
            clock: 0,
            pending_since: None,
        };
        store.load_view(&store.active.clone())?;
        store.clock = store
            .db
            .query_row("SELECT value FROM config WHERE key='clock'", [], |r| {
                r.get::<_, String>(0)
            })?
            .parse()?;
        Ok(store)
    }
    fn load_tree(
        &self,
        digest: Digest,
        parent: &str,
        projection: &mut Projection,
        depth: usize,
    ) -> Result<()> {
        ensure!(depth < 256, "TREE_TOO_DEEP");
        let tree = Tree::decode(&self.content.packs.get(digest)?)?;
        projection.trees.insert(parent.into(), digest);
        for e in tree.entries {
            ensure!(
                !projection.entries.contains_key(&e.entity),
                "DUPLICATE_TREE_ENTITY"
            );
            let entry = Entry {
                id: e.entity.clone(),
                parent: parent.into(),
                name: e.name,
                kind: e.kind,
                content: e.content,
                times: e.times,
                mode: e.mode,
                attributes: e.attributes,
                deleted: false,
                working_len: None,
            };
            projection.index(entry)?;
            if let Some(child) = e.tree {
                self.load_tree(child, &e.entity, projection, depth + 1)?;
            }
        }
        Ok(())
    }
    fn load_view(&mut self, branch: &str) -> Result<()> {
        if self.views.contains_key(branch) {
            return Ok(());
        }
        let snapshot: Option<String> = self
            .db
            .query_row("SELECT snapshot FROM branches WHERE id=?1", [branch], |r| {
                r.get(0)
            })
            .context("BRANCH_NOT_FOUND")?;
        let mut projection = Projection::empty();
        if let Some(snapshot) = snapshot {
            let snapshot = self.snapshot(&snapshot)?;
            self.load_tree(snapshot.root, ROOT, &mut projection, 0)?;
            projection.heads = Arc::new(
                snapshot
                    .frontier
                    .iter()
                    .map(|(k, v)| (k.clone(), v.iter().cloned().collect()))
                    .collect(),
            );
            projection.visible = Arc::new(
                snapshot
                    .frontier
                    .iter()
                    .filter_map(|(k, v)| v.first().map(|v| (k.clone(), v.clone())))
                    .collect(),
            );
        }
        let rows = {
            let mut q = self
                .db
                .prepare("SELECT entity,state,visible FROM current WHERE branch=?1")?;
            q.query_map([branch], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        };
        for (entity, state, visible) in rows {
            projection.index(serde_json::from_slice(&state)?)?;
            Arc::make_mut(&mut projection.visible).insert(entity, visible);
        }
        let rows = {
            let mut q = self
                .db
                .prepare("SELECT entity,version FROM heads WHERE branch=?1")?;
            q.query_map([branch], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut overlaid = BTreeSet::new();
        for (entity, version) in rows {
            let heads = Arc::make_mut(&mut projection.heads);
            if overlaid.insert(entity.clone()) {
                heads.insert(entity.clone(), BTreeSet::new());
            }
            heads.entry(entity).or_default().insert(version);
        }
        for (entity, visible) in &*projection.visible {
            Arc::make_mut(&mut projection.anchors)
                .insert(entity.clone(), self.content_anchor(visible)?);
        }
        self.views.insert(branch.into(), projection);
        Ok(())
    }
    pub fn projection(&self) -> &Projection {
        &self.views[&self.active]
    }
    pub fn lookup(&self, path: &str) -> Result<Entry> {
        self.projection().lookup(path)
    }
    pub fn entry(&self, entity: &str) -> Result<Entry> {
        self.projection()
            .entries
            .get(entity)
            .cloned()
            .context("NOT_FOUND")
    }
    pub fn basis(&self, entity: &str) -> Result<Basis> {
        let entry = self.entry(entity)?;
        Ok(Basis {
            entity: entity.into(),
            branch: self.active.clone(),
            version: self
                .projection()
                .visible
                .get(entity)
                .cloned()
                .context("VERSION_MISSING")?,
            anchor: self
                .projection()
                .anchors
                .get(entity)
                .cloned()
                .context("VERSION_MISSING")?,
            content: entry.content.context("IS_A_DIRECTORY")?,
        })
    }
    pub fn content_anchor(&self, version: &str) -> Result<String> {
        let mut current = self.version(version)?.context("VERSION_MISSING")?;
        for _ in 0..4096 {
            if current.author != self.device
                || !matches!(
                    current.operation,
                    Operation::Times | Operation::Rename | Operation::Delete
                )
                || current.parents.len() != 1
            {
                return Ok(current.id);
            }
            let parent = self
                .version(&current.parents[0])?
                .context("VERSION_MISSING")?;
            if current.state.content != parent.state.content {
                return Ok(current.id);
            }
            current = parent;
        }
        anyhow::bail!("METADATA_CHAIN_TOO_DEEP")
    }
    pub fn event(
        &mut self,
        branch: &str,
        state: Entry,
        parents: Vec<String>,
        operation: Operation,
    ) -> Event {
        self.clock += 1;
        Event {
            id: id(),
            entity: state.id.clone(),
            parents,
            operation,
            state,
            branch: branch.into(),
            author: self.device.clone(),
            clock: self.clock,
        }
    }
    fn validate_event(&self, event: &Event) -> Result<bool> {
        ensure!(
            Uuid::parse_str(&event.id).is_ok()
                && event.entity == event.state.id
                && event.entity != ROOT
                && !event.author.is_empty()
                && event.clock > 0,
            "INVALID_EVENT"
        );
        name_key(&event.state.name)?;
        ensure!(
            event.state.mode & !0o7777 == 0 && event.parents.len() <= 1024,
            "INVALID_EVENT"
        );
        match event.state.kind {
            EntryKind::File => event
                .state
                .content
                .as_ref()
                .context("CONTENT_REQUIRED")?
                .validate()?,
            EntryKind::Directory => ensure!(event.state.content.is_none(), "DIRECTORY_HAS_CONTENT"),
        }
        ensure!(
            (event.operation != Operation::Delete || event.state.deleted)
                && (!event.state.deleted
                    || matches!(event.operation, Operation::Delete | Operation::Resolve)),
            "INVALID_DELETION"
        );
        let mut parents = BTreeSet::new();
        for parent in &event.parents {
            ensure!(parents.insert(parent), "DUPLICATE_PARENT");
            let Some(prior) = self.version(parent)? else {
                return Ok(false);
            };
            ensure!(
                prior.entity == event.entity && prior.clock < event.clock,
                "INVALID_PARENT"
            );
        }
        ensure!(
            !event.parents.is_empty()
                || matches!(event.operation, Operation::Create | Operation::Mkdir),
            "VERSION_PARENT_REQUIRED"
        );
        Ok(true)
    }
    pub fn version(&self, id: &str) -> Result<Option<Event>> {
        let value: Option<Vec<u8>> = self
            .db
            .query_row("SELECT event FROM events WHERE id=?1", [id], |r| r.get(0))
            .optional()?;
        value.map(|v| Ok(serde_json::from_slice(&v)?)).transpose()
    }
    /// One metadata transaction per batch. Projection changes become visible
    /// only after commit, and missing parents remain in the bounded inbox.
    pub fn apply(&mut self, events: &[Event]) -> Result<Vec<String>> {
        self.apply_batch(events, &[])
    }
    pub fn apply_batch(
        &mut self,
        events: &[Event],
        receipts: &[(String, String, u128)],
    ) -> Result<Vec<String>> {
        let mut applied = Vec::new();
        let mut pending: Vec<Event> = events.to_vec();
        {
            let mut q = self.db.prepare("SELECT event FROM waiting LIMIT 4096")?;
            for row in q.query_map([], |r| r.get::<_, Vec<u8>>(0))? {
                pending.push(serde_json::from_slice(&row?)?);
            }
        }
        // Validate against earlier events in the same batch through a temporary
        // transaction. All projections are rebuilt from per-entity deltas below.
        for event in events {
            let exists: bool = self.db.query_row(
                "SELECT EXISTS(SELECT 1 FROM branches WHERE id=?1)",
                [&event.branch],
                |r| r.get(0),
            )?;
            if exists {
                self.load_view(&event.branch)?;
            }
        }
        let mut affected = BTreeSet::new();
        if !receipts.is_empty() {
            self.flush_metadata()?;
            self.db.execute_batch("PRAGMA synchronous=FULL;")?;
        }
        if self.db.is_autocommit() {
            self.db.execute_batch("BEGIN IMMEDIATE;")?;
            self.pending_since = Some(std::time::Instant::now());
        }
        self.db.execute_batch("SAVEPOINT apply_events;")?;
        let tx = &self.db;
        let result = (|| -> Result<()> {
            let mut progress = true;
            while progress {
                progress = false;
                let mut remain = Vec::new();
                for event in pending.drain(..) {
                    if let Some(existing) = self.version(&event.id)? {
                        ensure!(existing == event, "EVENT_ID_COLLISION");
                        tx.execute("DELETE FROM waiting WHERE id=?1", [&event.id])?;
                        continue;
                    }
                    if !self.validate_event(&event)? {
                        remain.push(event);
                        continue;
                    }
                    let exists: bool = tx.query_row(
                        "SELECT EXISTS(SELECT 1 FROM branches WHERE id=?1)",
                        [&event.branch],
                        |r| r.get(0),
                    )?;
                    if !exists {
                        tx.execute("INSERT INTO branches VALUES(?1,?1,NULL)", [&event.branch])?;
                    }
                    // Seed inherited heads when a branch receives its first edit.
                    let inherited = self
                        .views
                        .get(&event.branch)
                        .and_then(|p| p.heads.get(&event.entity));
                    let explicit: bool = tx.query_row(
                        "SELECT EXISTS(SELECT 1 FROM heads WHERE branch=?1 AND entity=?2)",
                        params![event.branch, event.entity],
                        |r| r.get(0),
                    )?;
                    if !explicit && let Some(heads) = inherited {
                        for head in heads {
                            tx.execute(
                                "INSERT OR IGNORE INTO heads VALUES(?1,?2,?3)",
                                params![event.branch, event.entity, head],
                            )?;
                        }
                    }
                    for parent in &event.parents {
                        tx.execute(
                            "DELETE FROM heads WHERE branch=?1 AND entity=?2 AND version=?3",
                            params![event.branch, event.entity, parent],
                        )?;
                    }
                    tx.execute(
                        "INSERT INTO events(id,branch,entity,event) VALUES(?1,?2,?3,?4)",
                        params![
                            event.id,
                            event.branch,
                            event.entity,
                            serde_json::to_vec(&event)?
                        ],
                    )?;
                    tx.execute(
                        "INSERT INTO heads VALUES(?1,?2,?3)",
                        params![event.branch, event.entity, event.id],
                    )?;
                    let heads: usize = tx.query_row(
                        "SELECT count(*) FROM heads WHERE branch=?1 AND entity=?2",
                        params![event.branch, event.entity],
                        |r| r.get(0),
                    )?;
                    let old: Option<String> = tx
                        .query_row(
                            "SELECT visible FROM current WHERE branch=?1 AND entity=?2",
                            params![event.branch, event.entity],
                            |r| r.get(0),
                        )
                        .optional()?;
                    let candidate = old.or_else(|| {
                        self.views
                            .get(&event.branch)
                            .and_then(|p| p.visible.get(&event.entity).cloned())
                    });
                    let visible = if heads > 1 {
                        candidate
                            .filter(|old| !event.parents.contains(old))
                            .unwrap_or_else(|| event.id.clone())
                    } else {
                        event.id.clone()
                    };
                    let state = if visible == event.id {
                        event.state.clone()
                    } else {
                        self.version(&visible)?
                            .context("VISIBLE_VERSION_MISSING")?
                            .state
                    };
                    tx.execute(
                        "INSERT OR REPLACE INTO current VALUES(?1,?2,?3,?4)",
                        params![
                            event.branch,
                            event.entity,
                            serde_json::to_vec(&state)?,
                            visible
                        ],
                    )?;
                    if heads > 1 {
                        tx.execute("INSERT OR IGNORE INTO conflict_history SELECT branch,entity,version FROM heads WHERE branch=?1 AND entity=?2",params![event.branch,event.entity])?;
                    }
                    tx.execute("DELETE FROM waiting WHERE id=?1", [&event.id])?;
                    affected.insert((event.branch.clone(), event.entity.clone()));
                    applied.push(event.id.clone());
                    progress = true;
                }
                pending = remain;
            }
            ensure!(pending.len() <= 4096, "INBOX_FULL");
            for event in &pending {
                tx.execute(
                    "INSERT OR IGNORE INTO waiting VALUES(?1,?2)",
                    params![event.id, serde_json::to_vec(event)?],
                )?;
            }
            Ok(())
        })();
        if let Err(error) = result {
            tx.execute_batch("ROLLBACK TO apply_events; RELEASE apply_events;")?;
            if !receipts.is_empty() {
                tx.execute_batch("ROLLBACK;PRAGMA synchronous=NORMAL;")?;
                self.pending_since = None;
            }
            return Err(error);
        }
        let clock = events
            .iter()
            .map(|e| e.clock)
            .max()
            .unwrap_or(0)
            .max(self.clock);
        tx.execute(
            "UPDATE config SET value=?1 WHERE key='clock'",
            [clock.to_string()],
        )?;
        for (operation, event, stamp) in receipts {
            tx.execute(
                "INSERT OR IGNORE INTO seal_receipts VALUES(?1,?2,?3)",
                params![operation, event, stamp.to_string()],
            )?;
        }
        tx.execute_batch("RELEASE apply_events;")?;
        if !receipts.is_empty() {
            self.flush_metadata()?;
            self.db.execute_batch("PRAGMA synchronous=NORMAL;")?;
        } else if self
            .pending_since
            .is_some_and(|t| t.elapsed() >= std::time::Duration::from_millis(5))
        {
            self.flush_metadata()?;
        }
        for (branch, entity) in affected {
            if !self.views.contains_key(&branch) {
                self.load_view(&branch)?;
                continue;
            }
            let (state, visible): (Vec<u8>, String) = self.db.query_row(
                "SELECT state,visible FROM current WHERE branch=?1 AND entity=?2",
                params![branch, entity],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            let heads = {
                let mut q = self
                    .db
                    .prepare("SELECT version FROM heads WHERE branch=?1 AND entity=?2")?;
                q.query_map(params![branch, entity], |r| r.get::<_, String>(0))?
                    .collect::<rusqlite::Result<BTreeSet<_>>>()?
            };
            let anchor = self.content_anchor(&visible)?;
            let p = self.views.get_mut(&branch).unwrap();
            p.index(serde_json::from_slice(&state)?)?;
            Arc::make_mut(&mut p.visible).insert(entity.clone(), visible);
            Arc::make_mut(&mut p.anchors).insert(entity.clone(), anchor);
            Arc::make_mut(&mut p.heads).insert(entity, heads);
        }
        if let Some(clock) = events.iter().map(|e| e.clock).max() {
            self.clock = self.clock.max(clock);
        }
        Ok(applied)
    }
    pub fn flush_metadata(&mut self) -> Result<()> {
        if !self.db.is_autocommit() {
            self.db.execute_batch("COMMIT;")?;
        }
        self.pending_since = None;
        Ok(())
    }
    pub fn commit_due(&mut self) -> Result<()> {
        if self
            .pending_since
            .is_some_and(|t| t.elapsed() >= std::time::Duration::from_millis(5))
        {
            self.flush_metadata()?;
        }
        Ok(())
    }
    fn local(&mut self, state: Entry, operation: Operation) -> Result<Event> {
        let parents = self
            .projection()
            .visible
            .get(&state.id)
            .cloned()
            .into_iter()
            .collect();
        let event = self.event(&self.active.clone(), state, parents, operation);
        self.apply(std::slice::from_ref(&event))?;
        Ok(event)
    }
    pub fn create(&mut self, path: &str, kind: EntryKind) -> Result<Entry> {
        self.create_with_attributes(path, kind, None)
    }
    pub fn create_with_attributes(
        &mut self,
        path: &str,
        kind: EntryKind,
        attributes: Option<u32>,
    ) -> Result<Entry> {
        let names = components(path)?;
        let name = names.last().context("INVALID_NAME")?;
        ensure!(self.lookup(path).is_err(), "ALREADY_EXISTS");
        let parent = self.lookup(&names[..names.len() - 1].join("/"))?;
        ensure!(parent.kind == EntryKind::Directory, "NOT_A_DIRECTORY");
        let content = if kind == EntryKind::File {
            let content = self.content.import(&[])?;
            self.content.packs.flush()?;
            Some(content)
        } else {
            None
        };
        let entry = Entry {
            id: id(),
            parent: parent.id,
            name: name.to_string(),
            kind,
            content,
            times: Times::default(),
            mode: if kind == EntryKind::Directory {
                0o755
            } else {
                0o644
            },
            attributes: attributes.unwrap_or(if kind == EntryKind::Directory {
                0x10
            } else {
                0x80
            }) | if kind == EntryKind::Directory {
                0x10
            } else {
                0
            },
            deleted: false,
            working_len: None,
        };
        self.local(
            entry.clone(),
            if kind == EntryKind::Directory {
                Operation::Mkdir
            } else {
                Operation::Create
            },
        )?;
        Ok(entry)
    }
    pub fn rename(&mut self, entity: &str, target: &str, replace: bool) -> Result<()> {
        let mut entry = self.entry(entity)?;
        ensure!(entity != ROOT, "ROOT_IMMUTABLE");
        let names = components(target)?;
        let name = names.last().context("INVALID_NAME")?;
        let parent = self.lookup(&names[..names.len() - 1].join("/"))?;
        ensure!(parent.kind == EntryKind::Directory, "NOT_A_DIRECTORY");
        let mut ancestor = parent.id.as_str();
        while ancestor != ROOT {
            ensure!(ancestor != entity, "DIRECTORY_CYCLE");
            ancestor = &self
                .projection()
                .entries
                .get(ancestor)
                .context("NOT_FOUND")?
                .parent;
        }
        let mut events = Vec::new();
        if let Ok(existing) = self.lookup(target)
            && existing.id != entity
        {
            ensure!(replace && existing.kind == entry.kind, "ALREADY_EXISTS");
            ensure!(
                existing.kind != EntryKind::Directory
                    || self.projection().children(&existing.id).is_empty(),
                "DIRECTORY_NOT_EMPTY"
            );
            let mut dead = existing.clone();
            dead.deleted = true;
            events.push(self.event(
                &self.active.clone(),
                dead,
                vec![self.projection().visible[&existing.id].clone()],
                Operation::Delete,
            ));
        }
        entry.parent = parent.id;
        entry.name = name.to_string();
        entry.times.changed = Times::default().changed;
        let event = self.event(
            &self.active.clone(),
            entry,
            vec![self.projection().visible[entity].clone()],
            Operation::Rename,
        );
        events.push(event);
        self.apply(&events)?;
        Ok(())
    }
    pub fn delete(&mut self, entity: &str) -> Result<()> {
        let mut e = self.entry(entity)?;
        ensure!(entity != ROOT, "ROOT_IMMUTABLE");
        ensure!(
            e.kind != EntryKind::Directory || self.projection().children(entity).is_empty(),
            "DIRECTORY_NOT_EMPTY"
        );
        e.deleted = true;
        self.local(e, Operation::Delete)?;
        Ok(())
    }
    pub fn set_times(
        &mut self,
        entity: &str,
        times: Times,
        mode: Option<u32>,
        attributes: Option<u32>,
    ) -> Result<()> {
        let mut e = self.entry(entity)?;
        e.times = times;
        if let Some(mode) = mode {
            e.mode = mode;
        }
        if let Some(a) = attributes {
            e.attributes = (a & !0x10)
                | if e.kind == EntryKind::Directory {
                    0x10
                } else {
                    0
                };
        }
        self.local(e, Operation::Times)?;
        Ok(())
    }
    pub fn receipt(&self, operation: &str) -> Result<Option<(String, u128)>> {
        let row: Option<(String, String)> = self
            .db
            .query_row(
                "SELECT event,modified_ns FROM seal_receipts WHERE operation=?1",
                [operation],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        row.map(|(e, t)| Ok((e, t.parse()?))).transpose()
    }
    fn tree_for(
        content: &LocalContent,
        p: &mut Projection,
        entity: &str,
        seen: &mut BTreeSet<String>,
    ) -> Result<Digest> {
        ensure!(seen.insert(entity.into()), "DIRECTORY_CYCLE");
        if !p.dirty.contains(entity)
            && let Some(digest) = p.trees.get(entity)
        {
            seen.remove(entity);
            return Ok(*digest);
        }
        let mut entries = Vec::new();
        for e in p.children(entity) {
            let tree = if e.kind == EntryKind::Directory {
                Some(Self::tree_for(content, p, &e.id, seen)?)
            } else {
                None
            };
            entries.push(TreeEntry {
                name: e.name,
                entity: e.id,
                kind: e.kind,
                content: e.content,
                tree,
                times: e.times,
                mode: e.mode,
                attributes: e.attributes,
            });
        }
        entries.sort_by_key(|e| name_key(&e.name).unwrap());
        let digest = content
            .packs
            .put(ObjectKind::Tree, &Tree { entries }.encode()?)?;
        p.trees.insert(entity.into(), digest);
        p.dirty.remove(entity);
        seen.remove(entity);
        Ok(digest)
    }
    pub fn checkpoint(&mut self) -> Result<Snapshot> {
        let p = self.views.get_mut(&self.active).unwrap();
        let dirty: Vec<_> = p.dirty.iter().cloned().collect();
        for entity in dirty {
            let mut at = entity;
            for _ in 0..256 {
                if at == ROOT || at.is_empty() {
                    break;
                }
                p.dirty.insert(at.clone());
                at = p
                    .entries
                    .get(&at)
                    .map(|e| e.parent.clone())
                    .unwrap_or_default();
            }
            p.dirty.insert(ROOT.into());
        }
        let root = Self::tree_for(&self.content, p, ROOT, &mut BTreeSet::new())?;
        self.content.packs.flush()?;
        let snapshot = Snapshot {
            id: id(),
            root,
            frontier: p
                .heads
                .iter()
                .map(|(e, heads)| {
                    let visible = p.visible.get(e);
                    let mut heads: Vec<_> = heads.iter().cloned().collect();
                    heads.sort_by_key(|h| Some(h) != visible);
                    (e.clone(), heads)
                })
                .collect(),
        };
        self.db.execute(
            "INSERT INTO snapshots VALUES(?1,?2,?3)",
            params![
                snapshot.id,
                &root.0[..],
                serde_json::to_vec(&snapshot.frontier)?
            ],
        )?;
        Ok(snapshot)
    }
    pub fn snapshot(&self, id: &str) -> Result<Snapshot> {
        let (root, frontier): (Vec<u8>, Vec<u8>) = self.db.query_row(
            "SELECT root,frontier FROM snapshots WHERE id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        Ok(Snapshot {
            id: id.into(),
            root: Digest(root.as_slice().try_into()?),
            frontier: serde_json::from_slice(&frontier)?,
        })
    }
    pub fn fork(&mut self, snapshot: &Snapshot, name: &str) -> Result<String> {
        ensure!(!name.is_empty(), "INVALID_BRANCH");
        let branch = id();
        self.db.execute(
            "INSERT INTO branches VALUES(?1,?2,?3)",
            params![branch, name, snapshot.id],
        )?;
        Ok(branch)
    }
    pub fn checkout(&mut self, branch: &str) -> Result<()> {
        let branch: String = self.db.query_row(
            "SELECT id FROM branches WHERE id=?1 OR name=?1",
            [branch],
            |r| r.get(0),
        )?;
        self.load_view(&branch)?;
        self.db
            .execute("UPDATE config SET value=?1 WHERE key='active'", [&branch])?;
        self.active = branch;
        Ok(())
    }
    pub fn events_after(&self, after: u64, limit: usize) -> Result<Vec<(u64, Event)>> {
        ensure!(limit <= 4096, "FEED_BATCH_TOO_LARGE");
        let mut q = self
            .db
            .prepare("SELECT seq,event FROM events WHERE seq>?1 ORDER BY seq LIMIT ?2")?;
        let rows = q
            .query_map(params![after, limit], |r| {
                Ok((r.get::<_, u64>(0)?, r.get::<_, Vec<u8>>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.into_iter()
            .map(|(seq, event)| Ok((seq, serde_json::from_slice(&event)?)))
            .collect()
    }
    pub fn conflicts(&self, include_resolved: bool) -> Result<Vec<Conflict>> {
        let mut versions: BTreeMap<String, BTreeSet<String>> = self
            .projection()
            .heads
            .iter()
            .filter(|(_, heads)| heads.len() > 1)
            .map(|(e, h)| (e.clone(), h.clone()))
            .collect();
        if include_resolved {
            let mut q = self
                .db
                .prepare("SELECT entity,version FROM conflict_history WHERE branch=?1")?;
            for row in q.query_map([&self.active], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })? {
                let (e, v) = row?;
                versions.entry(e).or_default().insert(v);
            }
        }
        versions
            .into_iter()
            .map(|(entity, heads)| {
                let resolved = self
                    .projection()
                    .heads
                    .get(&entity)
                    .is_none_or(|h| h.len() < 2);
                let visible = self
                    .projection()
                    .visible
                    .get(&entity)
                    .cloned()
                    .unwrap_or_default();
                let heads = heads
                    .iter()
                    .map(|h| self.version(h)?.context("CONFLICT_VERSION_MISSING"))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Conflict {
                    entity,
                    branch: self.active.clone(),
                    visible,
                    heads,
                    resolved,
                })
            })
            .collect()
    }
    pub fn resolve(&mut self, entity: &str, selected: &str) -> Result<Event> {
        let heads = self
            .projection()
            .heads
            .get(entity)
            .context("CONFLICT_NOT_FOUND")?
            .clone();
        ensure!(
            heads.len() > 1 && heads.contains(selected),
            "INVALID_RESOLUTION"
        );
        let state = self.version(selected)?.context("VERSION_MISSING")?.state;
        let event = self.event(
            &self.active.clone(),
            state,
            heads.into_iter().collect(),
            Operation::Resolve,
        );
        self.apply(std::slice::from_ref(&event))?;
        Ok(event)
    }
    /// Snapshot history, every current entity head, and reviewed conflict
    /// alternatives are retention roots. The pack layer adds active handle pins.
    pub fn live_objects(&self, history_depth: usize) -> Result<BTreeSet<Digest>> {
        let mut pending = Vec::new();
        for view in self.views.values() {
            for heads in view.heads.values() {
                for head in heads {
                    if let Some(event) = self.version(head)?
                        && let Some(content) = event.state.content
                    {
                        pending.push((
                            content.digest,
                            if content.kind == crate::model::content::ContentKind::Direct {
                                ObjectKind::Chunk
                            } else {
                                ObjectKind::Manifest
                            },
                        ));
                    }
                }
            }
        }
        let mut q = self
            .db
            .prepare("SELECT root FROM snapshots ORDER BY rowid DESC LIMIT ?1")?;
        for row in q.query_map([history_depth], |r| r.get::<_, Vec<u8>>(0))? {
            let digest = row?;
            pending.push((Digest(digest.as_slice().try_into()?), ObjectKind::Tree));
        }
        // Branch bases cannot age out merely because another branch checkpoints.
        let mut q = self
            .db
            .prepare("SELECT s.root FROM snapshots s JOIN branches b ON b.snapshot=s.id")?;
        for row in q.query_map([], |r| r.get::<_, Vec<u8>>(0))? {
            let digest = row?;
            pending.push((Digest(digest.as_slice().try_into()?), ObjectKind::Tree));
        }
        let mut q = self.db.prepare(
            "SELECT DISTINCT version FROM conflict_history UNION SELECT version FROM heads",
        )?;
        for row in q.query_map([], |r| r.get::<_, String>(0))? {
            if let Some(event) = self.version(&row?)?
                && let Some(c) = event.state.content
            {
                pending.push((
                    c.digest,
                    if c.kind == crate::model::content::ContentKind::Direct {
                        ObjectKind::Chunk
                    } else {
                        ObjectKind::Manifest
                    },
                ));
            }
        }
        let mut live = BTreeSet::new();
        while let Some((digest, kind)) = pending.pop() {
            if !live.insert(digest) {
                continue;
            }
            if !self.content.packs.contains(digest)? {
                continue;
            }
            let bytes = self.content.packs.get(digest)?;
            match kind {
                ObjectKind::Chunk => {}
                ObjectKind::Manifest => {
                    for chunk in crate::content::local::Manifest::decode(&bytes)?.chunks {
                        pending.push((chunk, ObjectKind::Chunk));
                    }
                }
                ObjectKind::Tree => {
                    for entry in Tree::decode(&bytes)?.entries {
                        if let Some(tree) = entry.tree {
                            pending.push((tree, ObjectKind::Tree));
                        }
                        if let Some(c) = entry.content {
                            pending.push((
                                c.digest,
                                if c.kind == crate::model::content::ContentKind::Direct {
                                    ObjectKind::Chunk
                                } else {
                                    ObjectKind::Manifest
                                },
                            ));
                        }
                    }
                }
            }
        }
        Ok(live)
    }
}
impl Drop for Store {
    fn drop(&mut self) {
        let _ = self.flush_metadata();
    }
}
