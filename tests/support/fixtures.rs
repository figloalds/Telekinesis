//! Seeded, in-process fixtures built through the public `tkfs::core::Store` API.
//!
//! Determinism: operation sequences, names and content bytes depend only on the
//! inputs (and `seed`). Event and entity ids are random UUIDs by design, so
//! callers compare operation logs, names and content hashes, never ids.
//!
//! Bulk metadata uses the public current commit path inside one outer
//! SAVEPOINT. It does not synthesize projection rows, legacy object catalogs or
//! root-admission effects. Small direct-data fixture bytes are installed outside
//! that transaction; normal immutable readers still verify them.
use anyhow::{Context, Result, ensure};
use rand::{Rng, RngCore, SeedableRng, rngs::StdRng};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Read,
    path::Path,
};
use tkfs::{
    core::{Bundle, Change, Event, ROOT, Store, hash, id, time},
    model::content::{CHUNK_SIZE, ContentKind, Digest, FileContentRef},
    runtime::Engine,
};

/// Bulk fixtures above this many creates use `direct_creates`.
pub const DIRECT_ROWS: usize = 1000;

/// Deterministic pseudo-random bytes for `seed`.
pub fn seeded_bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut bytes = vec![0; len];
    StdRng::seed_from_u64(seed).fill_bytes(&mut bytes);
    bytes
}

/// A `Read` source of `remaining` PRNG bytes (for streamed large objects).
pub struct PrngReader {
    rng: StdRng,
    remaining: u64,
}
impl PrngReader {
    pub fn new(seed: u64, len: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            remaining: len,
        }
    }
}
impl Read for PrngReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = buf.len().min(self.remaining as usize);
        self.rng.fill_bytes(&mut buf[..n]);
        self.remaining -= n as u64;
        Ok(n)
    }
}

/// Run `f` inside one SAVEPOINT on `store` (one durable commit for many events).
pub fn batched<T>(store: &mut Store, f: impl FnOnce(&mut Store) -> Result<T>) -> Result<T> {
    let name = format!("fixture_{}", id().replace('-', ""));
    store.db.execute_batch(&format!("SAVEPOINT {name}"))?;
    match f(store) {
        Ok(value) => {
            store.db.execute_batch(&format!("RELEASE {name}"))?;
            Ok(value)
        }
        Err(error) => {
            let _ = store
                .db
                .execute_batch(&format!("ROLLBACK TO {name}; RELEASE {name}"));
            Err(error)
        }
    }
}

/// Every device that authored an event in `stores` (the `allowed` set for receive).
pub fn devices<'a>(stores: impl IntoIterator<Item = &'a Store>) -> BTreeSet<String> {
    stores.into_iter().map(|s| s.device.clone()).collect()
}

/// Committed shared event ids of `store`.
pub fn shared_ids(store: &Store) -> Result<BTreeSet<String>> {
    store.shared_event_inventory()
}

/// Shared events of `from` that `to` has not committed, in journal order, with
/// the available objects they reference.
pub fn missing_bundle(from: &Store, to: &Store) -> Result<Bundle> {
    from.shared_bundle(&to.shared_event_inventory()?)
}

/// Deliver everything `from` has that `to` lacks (one `Store::receive`).
pub fn sync_into(from: &Store, to: &mut Store, allowed: &BTreeSet<String>) -> Result<Vec<String>> {
    let bundle = missing_bundle(from, to)?;
    if bundle.events.is_empty() {
        return Ok(vec![]);
    }
    to.receive(bundle, allowed)
}

/// Sort events so every parent (dependency or change parent) precedes its
/// children; ties keep their input order (Kahn's algorithm, O(N log N)).
/// Parents outside `events` are ignored; events in a cycle keep input order at the end.
pub fn causal_sort(events: &mut Vec<Event>) {
    let index: BTreeMap<String, usize> = events
        .iter()
        .enumerate()
        .map(|(i, e)| (e.id.clone(), i))
        .collect();
    let mut degree = vec![0usize; events.len()];
    let mut children: Vec<Vec<usize>> = vec![vec![]; events.len()];
    for (i, e) in events.iter().enumerate() {
        let parents: BTreeSet<usize> = e
            .dependencies
            .iter()
            .chain(e.changes.iter().flat_map(|c| c.parents.iter()))
            .filter_map(|p| index.get(p).copied())
            .filter(|p| *p != i)
            .collect();
        degree[i] = parents.len();
        for p in parents {
            children[p].push(i);
        }
    }
    let mut ready: BTreeSet<usize> = (0..events.len()).filter(|i| degree[*i] == 0).collect();
    let mut order = Vec::with_capacity(events.len());
    while let Some(i) = ready.pop_first() {
        order.push(i);
        for &c in &children[i] {
            degree[c] -= 1;
            if degree[c] == 0 {
                ready.insert(c);
            }
        }
    }
    let placed: BTreeSet<usize> = order.iter().copied().collect();
    order.extend((0..events.len()).filter(|i| !placed.contains(i)));
    let mut slots: Vec<Option<Event>> = std::mem::take(events).into_iter().map(Some).collect();
    events.extend(order.into_iter().map(|i| slots[i].take().unwrap()));
}

/// Split `bundle` into causally ordered pages of `events_per_page` events
/// (objects follow their events).
pub fn pages(bundle: &Bundle, events_per_page: usize) -> Vec<Bundle> {
    let mut events = bundle.events.clone();
    causal_sort(&mut events);
    events
        .chunks(events_per_page.max(1))
        .map(|events| {
            let wanted: BTreeSet<String> = events.iter().flat_map(Event::objects).collect();
            Bundle {
                repo: bundle.repo.clone(),
                branches: bundle.branches.clone(),
                events: events.to_vec(),
                objects: bundle
                    .objects
                    .iter()
                    .filter(|(h, _)| wanted.contains(*h))
                    .map(|(h, v)| (h.clone(), v.clone()))
                    .collect(),
            }
        })
        .collect()
}

/// `n` linear creates on `store`: files `lin-000000.txt`… at the root, one event
/// each (local fast path), committed in one batch. Returns the new event ids in order.
pub fn linear_history(store: &mut Store, n: usize) -> Result<Vec<String>> {
    linear_history_named(store, "lin", n)
}

/// `linear_history` with a name prefix (to append more history later).
pub fn linear_history_named(store: &mut Store, prefix: &str, n: usize) -> Result<Vec<String>> {
    let before = store.shared_event_inventory()?;
    if n > DIRECT_ROWS {
        let empty = hash(b"");
        let files: Vec<NewFile> = (0..n - 1)
            .map(|i| NewFile {
                parent: ROOT.into(),
                name: format!("{prefix}-{i:06}.txt"),
                content: empty.clone(),
                size: 0,
            })
            .collect();
        direct_creates(store, &files)?;
        // The last create runs the real commit path and seals the snapshot.
        store.create(&format!("{prefix}-{:06}.txt", n - 1), "file")?;
    } else {
        batched(store, |s| {
            for i in 0..n {
                s.create(&format!("{prefix}-{i:06}.txt"), "file")?;
            }
            Ok(())
        })?;
    }
    journal_order(store, &before)
}

/// One file create for `direct_creates`.
pub struct NewFile {
    /// Parent directory entity id (`ROOT` for the root).
    pub parent: String,
    pub name: String,
    /// Digest of one direct data blob (not a whole-file/manifest conversion).
    pub content: String,
    pub size: u64,
}

/// Commit one current descriptor-valued creation per file through the public
/// metadata path. The Core owns projection/seals, root handoff and save-owner
/// generations. No byte work occurs inside the batch; descriptors name direct
/// fixture data already installed by the caller (empty content needs no blob).
pub fn direct_creates(store: &mut Store, files: &[NewFile]) -> Result<Vec<String>> {
    let branch = store.branch(&store.active.clone())?;
    let device = store.device.clone();
    let repo = store.repo.clone();
    let projection = store.projection(&store.active)?;
    let mut parent_paths = BTreeMap::new();
    for parent in files
        .iter()
        .map(|file| &file.parent)
        .collect::<BTreeSet<_>>()
    {
        let mut current = parent.as_str();
        let mut parts = vec![];
        let mut seen = BTreeSet::new();
        while current != ROOT {
            ensure!(seen.insert(current), "INVALID_PARENT");
            let entry = projection.entries.get(current).context("INVALID_PARENT")?;
            parts.push(entry.name.as_str());
            current = &entry.parent;
        }
        parts.reverse();
        let path = parts.join("/");
        let directory = store.lookup(&path)?;
        ensure!(
            directory.id == *parent && directory.kind == "directory",
            "INVALID_PARENT"
        );
        parent_paths.insert(parent.clone(), path);
    }
    let mut previous: Vec<String> = store
        .db
        .prepare("SELECT event FROM event_frontier WHERE branch=? ORDER BY event")?
        .query_map([&branch.id], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    let start_ms = time();
    batched(store, |s| {
        let mut ids = Vec::with_capacity(files.len());
        for (i, file) in files.iter().enumerate() {
            let parent = &parent_paths[&file.parent];
            let path = if parent.is_empty() {
                file.name.clone()
            } else {
                format!("{parent}/{}", file.name)
            };
            ensure!(s.lookup(&path).is_err(), "NAME_COLLISION");
            let reference = FileContentRef::new(
                ContentKind::Direct,
                Digest::parse(&file.content)?,
                file.size,
            )?;
            let entity = id();
            let event_id = id();
            let event = Event {
                object_sizes: BTreeMap::new(),
                id: event_id.clone(),
                repo: repo.clone(),
                branch: branch.clone(),
                device: device.clone(),
                created_ms: start_ms + i as u64,
                changes: vec![
                    change(&entity, "kind", json!("file")),
                    change(
                        &entity,
                        "location",
                        json!({"parent":file.parent,"name":file.name}),
                    ),
                    change(&entity, "alive", json!(true)),
                    change(&entity, "content", json!(reference)),
                ],
                dependencies: previous.clone(),
                reviewed: vec![],
            };
            s.commit(event, true)?;
            previous = vec![event_id.clone()];
            ids.push(event_id);
        }
        Ok(ids)
    })
}

/// Committed event ids in commit (rowid) order, excluding `skip`.
pub fn journal_order(store: &Store, skip: &BTreeSet<String>) -> Result<Vec<String>> {
    let ids = store
        .db
        .prepare("SELECT id FROM events ORDER BY rowid")?
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(ids.into_iter().filter(|id| !skip.contains(id)).collect())
}

/// Summary of `concurrent_histories`.
///
/// Determinism contract (same `(stores.len(), ops, seed)` → equal values):
/// `log`, `syncs` and `history`. `history` is read back from the stores, so it
/// also proves the stores committed the same operations. The projected
/// outcome (which concurrent rename or write wins, conflict counts, visible
/// names) is **not** part of the contract: the core breaks those ties by event
/// and entity ids, which are random UUIDs, so it differs between runs of the
/// same seed (measured: 29 of 30 seeds; equal across replicas of one run).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConcurrentSummary {
    /// The intended operation sequence: `replica:op:target[:content]` (deterministic per seed).
    pub log: Vec<String>,
    /// Operations the store refused (state-dependent; not part of the determinism contract).
    pub refused: usize,
    /// Pairwise one-way syncs performed.
    pub syncs: usize,
    /// `canonical_history` of the converged replicas (identical on every replica).
    pub history: Vec<String>,
}

/// The committed shared events of `store` without ids, sorted: each event as
/// `author|entity.field=value,...`, where `author` is the index of its device in
/// `devices`, entities are named by the name they were created with, a location
/// is `parent-name/name` (`/` for the root) and `basic` values (timestamps) are
/// omitted. Equal for two runs of a seeded fixture that committed the same
/// operations in the same causal structure, whatever ids they drew.
pub fn canonical_history(store: &Store, devices: &[String]) -> Result<Vec<String>> {
    let events: Vec<Event> = store
        .events()?
        .into_iter()
        .filter(|e| e.branch.shared)
        .collect();
    let mut created: BTreeMap<String, String> = BTreeMap::new();
    for e in &events {
        let new: BTreeSet<&str> = e
            .changes
            .iter()
            .filter(|c| c.field == "kind")
            .map(|c| c.entity.as_str())
            .collect();
        for c in &e.changes {
            if c.field == "location" && new.contains(c.entity.as_str()) {
                let name = c.value["name"].as_str().unwrap_or_default();
                created.insert(c.entity.clone(), name.to_owned());
            }
        }
    }
    let name = |entity: &str| -> String {
        if entity == ROOT {
            "/".into()
        } else {
            created.get(entity).cloned().unwrap_or_else(|| "?".into())
        }
    };
    let mut out: Vec<String> = events
        .iter()
        .map(|e| {
            let author = devices
                .iter()
                .position(|d| *d == e.device)
                .map_or_else(|| "?".to_owned(), |i| i.to_string());
            let mut changes: Vec<String> = e
                .changes
                .iter()
                .map(|c| {
                    let value = match c.field.as_str() {
                        "location" => format!(
                            "{}/{}",
                            name(c.value["parent"].as_str().unwrap_or_default()),
                            c.value["name"].as_str().unwrap_or_default()
                        ),
                        "basic" => "-".into(),
                        _ => c.value.to_string(),
                    };
                    format!("{}.{}={value}", name(&c.entity), c.field)
                })
                .collect();
            changes.sort();
            format!("{author}|{}", changes.join(","))
        })
        .collect();
    out.sort();
    Ok(out)
}

/// `ops` random namespace and content operations spread over `stores` (all of the
/// same repository, distinct devices), with seeded one-way syncs between random
/// pairs, followed by full convergence. Operations only target entities a
/// replica has seen (tracked by the fixture, not by store state), names are
/// unique, directories are never deleted and only files move, so the intent
/// log is a pure function of `(stores.len(), ops, seed)`.
pub fn concurrent_histories(
    stores: &mut [Store],
    ops: usize,
    seed: u64,
) -> Result<ConcurrentSummary> {
    ensure!(stores.len() >= 2, "CONCURRENT_HISTORIES_NEEDS_TWO_STORES");
    let mut rng = StdRng::seed_from_u64(seed);
    let allowed = devices(stores.iter());
    let n = stores.len();
    // logical entity name -> (kind, creator replica); per-replica visibility.
    let mut entities: Vec<(String, &'static str)> = vec![];
    let mut ids: Vec<BTreeMap<String, String>> = vec![BTreeMap::new(); n];
    let mut seen: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); n];
    let mut summary = ConcurrentSummary::default();
    for op in 0..ops {
        let r = rng.gen_range(0..n);
        let visible: Vec<usize> = seen[r].iter().copied().collect();
        let files: Vec<usize> = visible
            .iter()
            .copied()
            .filter(|e| entities[*e].1 == "file")
            .collect();
        let dirs: Vec<usize> = visible
            .iter()
            .copied()
            .filter(|e| entities[*e].1 == "directory")
            .collect();
        let choice = rng.gen_range(0..100);
        let entry = if choice < 25 || files.is_empty() {
            let kind = if choice % 4 == 0 { "directory" } else { "file" };
            let name = format!("r{r}-op{op}");
            let parent = if !dirs.is_empty() && rng.gen_bool(0.5) {
                Some(dirs[rng.gen_range(0..dirs.len())])
            } else {
                None
            };
            let path = match parent {
                Some(d) => format!("{}/{name}", entities[d].0),
                None => name.clone(),
            };
            entities.push((path.clone(), kind));
            let index = entities.len() - 1;
            seen[r].insert(index);
            let result = stores[r].create(&path, kind);
            match result {
                Ok(entry) => {
                    ids[r].insert(path.clone(), entry.id.clone());
                }
                Err(_) => summary.refused += 1,
            }
            format!("{r}:create-{kind}:{path}")
        } else if choice < 60 {
            let f = files[rng.gen_range(0..files.len())];
            let content = rng.r#gen::<u64>();
            let bytes = format!("{f}:{content:016x}").into_bytes();
            match entity_id(&ids, &entities[f].0) {
                Some(entity) => {
                    let base = stores[r]
                        .projection(&stores[r].active.clone())?
                        .entries
                        .get(&entity)
                        .and_then(|e| e.revisions.get("content").cloned());
                    if stores[r].write_revision(&entity, base, &bytes).is_err() {
                        summary.refused += 1;
                    }
                }
                None => summary.refused += 1,
            }
            format!("{r}:write:{}:{}", entities[f].0, hash(&bytes))
        } else if choice < 85 {
            let f = files[rng.gen_range(0..files.len())];
            let target_dir = if !dirs.is_empty() && rng.gen_bool(0.5) {
                Some(dirs[rng.gen_range(0..dirs.len())])
            } else {
                None
            };
            let name = format!("mv{op}");
            let target = match target_dir {
                Some(d) => format!("{}/{name}", entities[d].0),
                None => name,
            };
            match entity_id(&ids, &entities[f].0) {
                Some(entity) => {
                    if stores[r].rename(&entity, &target, false).is_err() {
                        summary.refused += 1;
                    }
                }
                None => summary.refused += 1,
            }
            format!("{r}:rename:{}->{target}", entities[f].0)
        } else {
            let f = files[rng.gen_range(0..files.len())];
            match entity_id(&ids, &entities[f].0) {
                Some(entity) => {
                    if stores[r].delete(&entity).is_err() {
                        summary.refused += 1;
                    }
                }
                None => summary.refused += 1,
            }
            format!("{r}:delete:{}", entities[f].0)
        };
        summary.log.push(entry);
        if rng.gen_bool(0.3) {
            let from = rng.gen_range(0..n);
            let to = (from + rng.gen_range(1..n)) % n;
            sync_pair(stores, from, to, &allowed)?;
            let known = seen[from].clone();
            seen[to].extend(known);
            summary.syncs += 1;
        }
    }
    converge(stores, &allowed)?;
    let replica_devices: Vec<String> = stores.iter().map(|s| s.device.clone()).collect();
    summary.history = canonical_history(&stores[0], &replica_devices)?;
    for other in &stores[1..] {
        ensure!(
            canonical_history(other, &replica_devices)? == summary.history,
            "FIXTURE_HISTORIES_DIFFER"
        );
    }
    Ok(summary)
}

/// Entity id for a logical name: whichever replica created it (ids are global).
fn entity_id(ids: &[BTreeMap<String, String>], name: &str) -> Option<String> {
    ids.iter().find_map(|m| m.get(name).cloned())
}

/// One-way sync `stores[from]` → `stores[to]`.
pub fn sync_pair(
    stores: &mut [Store],
    from: usize,
    to: usize,
    allowed: &BTreeSet<String>,
) -> Result<()> {
    let bundle = missing_bundle(&stores[from], &stores[to])?;
    if !bundle.events.is_empty() {
        stores[to].receive(bundle, allowed)?;
    }
    Ok(())
}

/// Full-mesh syncs until every store has the same committed shared events.
pub fn converge(stores: &mut [Store], allowed: &BTreeSet<String>) -> Result<()> {
    for _ in 0..8 {
        for from in 0..stores.len() {
            for to in 0..stores.len() {
                if from != to {
                    sync_pair(stores, from, to, allowed)?;
                }
            }
        }
        let first = stores[0].shared_event_inventory()?;
        let mut equal = true;
        for s in stores.iter().skip(1) {
            equal &= s.shared_event_inventory()? == first;
        }
        if equal {
            return Ok(());
        }
    }
    anyhow::bail!("FIXTURE_DID_NOT_CONVERGE")
}

/// The state built by `two_way_backlog`.
pub struct Backlog {
    /// A's `n` creates that B has not seen, in journal order.
    pub bundle: Bundle,
    /// Devices of A and B.
    pub allowed: BTreeSet<String>,
    /// B's one unseen local edit.
    pub local_edit: String,
}

/// A authors `n` creates while B authors one unseen local edit (the two-way
/// activation case of RC1/M1). A and B must be stores of the same repository.
/// On return both share a seed history; A's `n` creates are in `bundle`, not yet
/// delivered to B, and B's edit is not delivered to A.
pub fn two_way_backlog(a: &mut Store, b: &mut Store, n: usize) -> Result<Backlog> {
    ensure!(a.repo == b.repo, "REPOSITORY_MISMATCH");
    let allowed = devices([&*a, &*b]);
    let shared = a.create("backlog-shared.txt", "file")?;
    a.write_revision(
        &shared.id,
        shared.revisions.get("content").cloned(),
        b"seed",
    )?;
    sync_into(a, b, &allowed)?;
    let entry = b.lookup("backlog-shared.txt")?;
    let local_edit = b.write_revision(
        &entry.id,
        entry.revisions.get("content").cloned(),
        b"unseen local edit on B",
    )?;
    let before = b.shared_event_inventory()?;
    linear_history_named(a, "backlog", n)?;
    let mut bundle = a.shared_bundle(&before)?;
    causal_sort(&mut bundle.events);
    ensure!(bundle.events.len() == n, "BACKLOG_SIZE");
    Ok(Backlog {
        bundle,
        allowed,
        local_edit,
    })
}

/// Author `n` linear creates as a sibling replica of `store` and stage them in
/// `store`'s inbox (`incoming_events`) without activating them, using the same
/// named-column INSERT as `Store::receive`. They activate on the next open (startup
/// drain) or receive. Returns the staged ids in causal order.
pub fn staged_inbox(store: &mut Store, n: usize) -> Result<Vec<String>> {
    let temp = tempfile::tempdir()?;
    let mut author = Store::open(temp.path(), Some(&store.repo))?;
    let allowed = devices([&*store, &author]);
    sync_into(store, &mut author, &allowed)?;
    let before = author.shared_event_inventory()?;
    linear_history_named(&mut author, "staged", n)?;
    let mut events: Vec<Event> = author
        .events()?
        .into_iter()
        .filter(|e| !before.contains(&e.id))
        .collect();
    causal_sort(&mut events);
    let ids: Vec<String> = events.iter().map(|e| e.id.clone()).collect();
    batched(store, |s| {
        for e in &events {
            s.db.execute(
                "INSERT INTO incoming_events(id,payload) VALUES(?,?)",
                rusqlite::params![e.id, serde_json::to_string(e)?],
            )?;
        }
        Ok(())
    })?;
    Ok(ids)
}

/// `count` files of `size` seeded bytes (`objects/obj-000000.bin`…), unique per
/// index. Returns their content hashes in index order.
pub fn small_objects(engine: &mut Engine, count: usize, size: usize) -> Result<Vec<String>> {
    small_objects_in(&mut engine.store, "objects", count, size, 0)
}

/// `small_objects` on a store: direct blobs installed outside the metadata
/// batch, then current descriptor-valued creates through public Core commits.
/// Fixture-only byte installation skips per-file fsync.
pub fn small_objects_in(
    store: &mut Store,
    dir: &str,
    count: usize,
    size: usize,
    seed: u64,
) -> Result<Vec<String>> {
    ensure!(
        size as u64 <= CHUNK_SIZE,
        "small_objects requires direct data; use the streamed large fixture for manifests"
    );
    if store.lookup(dir).is_err() {
        store.create(dir, "directory")?;
    }
    let parent = store.lookup(dir)?.id;
    let objects = store.root.join("objects");
    let mut hashes = Vec::with_capacity(count);
    let mut contents = Vec::with_capacity(count);
    for i in 0..count {
        let mut bytes = seeded_bytes(seed.wrapping_mul(1_000_003).wrapping_add(i as u64), size);
        // Unique even for tiny sizes.
        let tag = (i as u64).to_le_bytes();
        let n = tag.len().min(bytes.len());
        bytes[..n].copy_from_slice(&tag[..n]);
        let object = hash(&bytes);
        if i + 1 < count {
            let path = objects.join(&object);
            if !path.exists() {
                fs::write(&path, &bytes)?;
            }
        }
        hashes.push(object);
        contents.push(bytes);
    }
    let name = |i: usize| format!("obj-{i:06}.bin");
    let files: Vec<NewFile> = (0..count.saturating_sub(1))
        .map(|i| NewFile {
            parent: parent.clone(),
            name: name(i),
            content: hashes[i].clone(),
            size: size as u64,
        })
        .collect();
    direct_creates(store, &files)?;
    if count > 0 {
        // The last file goes through the real commit path (and seals the snapshot).
        let last = count - 1;
        let e = store.create(&format!("{dir}/{}", name(last)), "file")?;
        store.write_revision(&e.id, e.revisions.get("content").cloned(), &contents[last])?;
    }
    Ok(hashes)
}

fn change(entity: &str, field: &str, value: Value) -> Change {
    Change {
        entity: entity.into(),
        field: field.into(),
        parents: vec![],
        value,
    }
}

/// One file `large-<bytes>.bin` whose content is streamed from a PRNG (never held
/// in memory). Returns its content hash.
pub fn large_object(engine: &mut Engine, bytes: u64) -> Result<String> {
    large_object_in(&mut engine.store, &format!("large-{bytes}.bin"), bytes, 7)
}

pub fn large_object_in(store: &mut Store, path: &str, bytes: u64, seed: u64) -> Result<String> {
    let entry = match store.lookup(path) {
        Ok(entry) => entry,
        Err(_) => store.create(path, "file")?,
    };
    let mut source = PrngReader::new(seed, bytes);
    store.write_stream_with_base_and_times(
        &entry.id,
        entry.revisions.get("content").cloned(),
        entry.revisions.get("alive").cloned(),
        &mut source,
        bytes,
        0,
    )?;
    store
        .lookup(path)?
        .content_ref
        .map(|content| content.digest.to_string())
        .context("LARGE_OBJECT_CONTENT_MISSING")
}

/// Close `store`, copy its directory to `dst` and give the copy a fresh device
/// id: a second replica that has already received and activated the same
/// history (avoids replaying a large remote history through the slow path
/// during setup). The copy's per-replica rows are those of a receiver after its
/// first round trip with the author (`as_receiver`). Returns
/// `(original reopened, clone)`.
pub fn clone_replica(store: Store, dst: &Path) -> Result<(Store, Store)> {
    let root = store.root.clone();
    let repo = store.repo.clone();
    let author = store.device.clone();
    store.db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    drop(store);
    copy_dir(&root, dst)?;
    for name in ["runtime.json", "worker.json", "owner.lock"] {
        let _ = fs::remove_file(dst.join(name));
    }
    restamp_device(dst, &id())?;
    as_receiver(dst, &author)?;
    let original = Store::open(&root, Some(&repo))?;
    let clone = Store::open(dst, Some(&repo))?;
    Ok((original, clone))
}

/// `clone_replica`, then remove the clone's object bytes (except the empty
/// object) and its availability rows for them: a replica that has committed
/// the whole history but holds none of its content, as if it had received the
/// metadata only. Avoids activating a large remote history through the slow
/// path during setup.
pub fn metadata_replica(store: Store, dst: &Path) -> Result<(Store, Store)> {
    let (original, clone) = clone_replica(store, dst)?;
    let repo = clone.repo.clone();
    drop(clone);
    let empty = hash(b"");
    for entry in fs::read_dir(dst.join("objects"))? {
        let entry = entry?;
        if entry.file_name().to_string_lossy() != empty {
            fs::remove_file(entry.path())?;
        }
    }
    // Only verified inbound bytes are recorded as received; object references
    // and sizes come from the events and stay.
    let db = rusqlite::Connection::open(dst.join("metadata.sqlite"))?;
    db.execute(
        "DELETE FROM received_shared_objects WHERE hash<>?",
        [&empty],
    )?;
    db.execute("DELETE FROM published_objects WHERE hash<>?", [&empty])?;
    drop(db);
    let clone = Store::open(dst, Some(&repo))?;
    Ok((original, clone))
}

/// Turn a copied (closed) state of `author` into what a receiver of that whole
/// history holds after its first round trip with `author` (core.rs `commit`,
/// `receive`, `remember_peer_inventory`):
/// - the author's publication grants (`published_objects`, written only for
///   local authorship) become `received_shared_objects` (verified inbound bytes);
/// - every shared event stays in the `outbox` (a receiver queues received
///   shared events too) and is acknowledged, because the author holds them all;
/// - the author's knowledge of its own peers (`peer_known`, `peer_objects`,
///   `peer_requests`), its object requests and request receipts are dropped;
///   `peer_known` records that `author` holds every shared event.
pub fn as_receiver(state: &Path, author: &str) -> Result<()> {
    let mut db = rusqlite::Connection::open(state.join("metadata.sqlite"))?;
    let tx = db.transaction()?;
    tx.execute_batch(
        "INSERT OR IGNORE INTO received_shared_objects SELECT hash FROM published_objects;
         DELETE FROM published_objects;
         UPDATE outbox SET acknowledged=1;
         DELETE FROM peer_known;
         DELETE FROM peer_objects;
         DELETE FROM peer_requests;
         DELETE FROM object_requests;
         DELETE FROM receipts;",
    )?;
    tx.execute(
        "INSERT INTO peer_known SELECT ?, e.id FROM events e JOIN branches b ON b.id=e.branch \
         WHERE b.shared=1",
        [author],
    )?;
    tx.commit()?;
    Ok(())
}

/// Recursive directory copy (files and directories only).
pub fn copy_dir(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let target = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else {
            fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

/// A remote-device event with arbitrary changes on `store`'s main branch,
/// parented by nothing (`dependencies` empty). Used to inject invalid events.
pub fn foreign_event(store: &Store, device: &str, changes: Vec<Change>) -> Result<Event> {
    let branch = store.branch(&store.active.clone())?;
    let mut event = store.event(&branch, changes);
    event.device = device.to_owned();
    event.dependencies.clear();
    Ok(event)
}

/// A new-entity create whose `kind` is invalid: `commit` refuses it with
/// `INVALID_KIND`, so a receiver quarantines it.
pub fn invalid_kind_changes(name: &str) -> Vec<Change> {
    let entity = id();
    vec![
        change(&entity, "kind", json!("symlink")),
        change(&entity, "location", json!({"parent": ROOT, "name": name})),
        change(&entity, "alive", json!(true)),
    ]
}

/// The legacy (format 6) `SyncPage` JSON for a worker `published-exchange`,
/// carrying `events` from `sender` to `receiver` with no objects.
pub fn legacy_page(
    repo: &str,
    sender: &str,
    receiver: &str,
    events: &[Event],
    known: &BTreeSet<String>,
) -> Value {
    json!({
        "format": 6, "repo": repo, "sender": sender, "receiver": receiver,
        "request": id(), "reply_to": null, "known": known,
        "known_objects": [], "partial_objects": {}, "object_parts": {},
        "bundle": {"repo": repo, "branches": [], "events": events, "objects": {}},
        "ack": [], "pending_events": 0, "object_sizes": {}, "wanted_objects": []
    })
}

/// A legacy `SyncRoute` JSON.
pub fn legacy_route(peer: &str, repo: &str, local: &str, remote: &str) -> Value {
    json!({"peer": peer, "repo": repo, "local_replica": local, "remote_replica": remote})
}

/// Shared events of `from` that `to` lacks, causally sorted, **without object
/// bytes** (safe for very large objects).
pub fn metadata_bundle(from: &Store, to: &Store) -> Result<Bundle> {
    let known = to.shared_event_inventory()?;
    let mut events: Vec<Event> = from
        .events()?
        .into_iter()
        .filter(|e| e.branch.shared && !known.contains(&e.id))
        .collect();
    causal_sort(&mut events);
    Ok(Bundle {
        repo: from.repo.clone(),
        branches: from.branches()?.into_iter().filter(|b| b.shared).collect(),
        events,
        objects: BTreeMap::new(),
    })
}

/// Outcome of `deliver_paced`.
#[derive(Debug, Clone, Default)]
pub struct Delivery {
    pub delivered: usize,
    pub total_ms: f64,
    /// Per-event milliseconds of each `receive` call.
    pub per_event_ms: Vec<f64>,
    pub timed_out: bool,
}
impl Delivery {
    pub fn mean_per_event_ms(&self) -> f64 {
        self.total_ms / self.delivered.max(1) as f64
    }
}

/// Deliver `bundle` to `store` with `Store::receive` in causal pages, stopping
/// at `deadline`. Pages hold 64 events while activation is cheap and shrink to
/// one event once an event costs more than 50 ms, so a slow baseline overruns
/// the deadline by at most one event.
pub fn deliver_paced(
    store: &mut Store,
    bundle: &Bundle,
    allowed: &BTreeSet<String>,
    deadline: std::time::Instant,
) -> Result<Delivery> {
    let mut events = bundle.events.clone();
    causal_sort(&mut events);
    let mut out = Delivery::default();
    let mut index = 0;
    let mut page = 64;
    while index < events.len() {
        if std::time::Instant::now() >= deadline {
            out.timed_out = true;
            break;
        }
        let end = (index + page).min(events.len());
        let slice = events[index..end].to_vec();
        let wanted: BTreeSet<String> = slice.iter().flat_map(Event::objects).collect();
        let part = Bundle {
            repo: bundle.repo.clone(),
            branches: bundle.branches.clone(),
            events: slice,
            objects: bundle
                .objects
                .iter()
                .filter(|(h, _)| wanted.contains(*h))
                .map(|(h, v)| (h.clone(), v.clone()))
                .collect(),
        };
        let started = std::time::Instant::now();
        store.receive(part, allowed)?;
        let ms = started.elapsed().as_secs_f64() * 1000.0;
        let n = end - index;
        out.delivered += n;
        out.total_ms += ms;
        let per = ms / n as f64;
        out.per_event_ms.push(per);
        if per > 50.0 {
            page = 1;
        }
        index = end;
    }
    Ok(out)
}

/// Copy a closed state directory `src` over `dst` and stamp `device` as the
/// copy's device id (turns `dst` into an already-synchronized replica, with
/// receiver-side rows, see `as_receiver`). Both directories must belong to
/// stopped folders whose workers have exited (`Node::stop_folder_exited`).
pub fn clone_into(src: &Path, dst: &Path, device: &str) -> Result<()> {
    let author: String = rusqlite::Connection::open(src.join("metadata.sqlite"))?.query_row(
        "SELECT value FROM config WHERE key='device'",
        [],
        |r| r.get(0),
    )?;
    if dst.exists() {
        fs::remove_dir_all(dst)?;
    }
    copy_dir(src, dst)?;
    for name in ["runtime.json", "worker.json", "owner.lock", "worker.log"] {
        let _ = fs::remove_file(dst.join(name));
    }
    restamp_device(dst, device)?;
    as_receiver(dst, &author)
}

/// Give a copied (closed) state directory the device id `device`: the `config`
/// row and every startup snapshot, whose payload names the device and whose
/// checksum covers the payload. Without the snapshot update the copy would fail
/// `SNAPSHOT_VERSION_OR_IDENTITY` on every open and rebuild its whole history
/// (O(L²) at 6298d0b: hours at 50K events).
pub fn restamp_device(state: &Path, device: &str) -> Result<()> {
    let db = rusqlite::Connection::open(state.join("metadata.sqlite"))?;
    db.execute("UPDATE config SET value=? WHERE key='device'", [device])?;
    let rows: Vec<(String, String)> = db
        .prepare("SELECT branch,payload FROM startup_snapshots")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    for (branch, payload) in rows {
        let mut snapshot: Value = serde_json::from_str(&payload)?;
        snapshot["device"] = json!(device);
        let payload = serde_json::to_string(&snapshot)?;
        db.execute(
            "UPDATE startup_snapshots SET payload=?,checksum=? WHERE branch=?",
            rusqlite::params![payload, hash(payload.as_bytes()), branch],
        )?;
    }
    Ok(())
}

/// One event creating `files` files with `name_len`-character names: a large
/// metadata payload (roughly `files × (name_len + 330)` bytes).
pub fn large_event(store: &mut Store, dir: &str, files: usize, name_len: usize) -> Result<String> {
    if store.lookup(dir).is_err() {
        store.create(dir, "directory")?;
    }
    let parent = store.lookup(dir)?.id;
    let empty = hash(b"");
    let mut changes = vec![];
    for i in 0..files {
        let entity = id();
        let name = format!("{}-{i:06}", "n".repeat(name_len.saturating_sub(7)));
        changes.extend([
            change(&entity, "kind", json!("file")),
            change(&entity, "location", json!({"parent": parent, "name": name})),
            change(&entity, "alive", json!(true)),
            change(&entity, "content", json!(empty)),
        ]);
    }
    let branch = store.branch(&store.active.clone())?;
    let event = store.event(&branch, changes);
    let event_id = event.id.clone();
    store.commit(event, true)?;
    Ok(event_id)
}
