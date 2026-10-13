//! WP1.7: append-mode conflict derivation is exact (REVAMP_PLAN §5.2).
//!
//! - The fuzzer: 2–4 replicas make random creates, mkdirs, renames, directory
//!   moves (concurrent cycles included), deletes, resurrections, content
//!   saves (`alive:true`), stale-base saves, metadata changes, temp
//!   replacements, resolutions and recoveries, and exchange events in random
//!   delivery orders (subsets, shuffles, duplicates, children before parents)
//!   with random restarts. After every committed local operation, delivery
//!   page and restart the oracle (`verify_derived_state`, a from-scratch rebuild) must be
//!   empty, which also proves the history still rebuilds (refusal parity
//!   with startup repair). Finally every replica converges to identical rows.
//!   Seeds run in append mode (append's own output feeds later appends) and,
//!   every third seed, in verify mode (each commit compared with full mode).
//!   50 seeds in debug; `fuzz_300_seeds_release` is the qualification run.
//! - Differential: one event corpus delivered in the same random order to an
//!   append-mode and a full-mode receiver must leave identical rows after
//!   every delivery.
//! - Refusal parity at `MAX_NAMESPACE_CUTS`, append usage on the workloads
//!   the package exists for, and the oracle's excess-state detection.
use rand::{Rng, SeedableRng, rngs::StdRng, seq::SliceRandom};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use tempfile::TempDir;
use tkfs::core::{
    derive::{
        ConflictMode, DeriveCounters, set_thread_conflict_mode, thread_derive_counters,
        thread_fallback_reasons, thread_last_fallback,
    },
    *,
};

const REPO: &str = "derive-oracle";
const NAMES: &[&str] = &["a", "A", "b", "c.txt", "C.TXT", "d"];

/// Sets the calling thread's conflict mode; restores the previous one on drop.
struct ModeGuard(Option<ConflictMode>);
impl ModeGuard {
    fn new(mode: ConflictMode) -> Self {
        Self(set_thread_conflict_mode(Some(mode)))
    }
}
impl Drop for ModeGuard {
    fn drop(&mut self) {
        set_thread_conflict_mode(self.0);
    }
}

fn counters_since(start: DeriveCounters) -> DeriveCounters {
    let now = thread_derive_counters();
    DeriveCounters {
        rebuilds: now.rebuilds - start.rebuilds,
        full: now.full - start.full,
        append: now.append - start.append,
        fallbacks: now.fallbacks - start.fallbacks,
        verified: now.verified - start.verified,
        restricted: now.restricted - start.restricted,
        commit_micros: now.commit_micros - start.commit_micros,
    }
}

/// One branch's conflict rows: id -> (payload without its ignored
/// `resolved_by` field, `resolved_by` column).
fn conflict_rows(s: &Store) -> BTreeMap<String, (Conflict, Option<String>)> {
    let mut statement =
        s.db.prepare("SELECT id,payload,resolved_by FROM conflicts")
            .unwrap();
    statement
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
            ))
        })
        .unwrap()
        .map(|row| {
            let (id, payload, resolved) = row.unwrap();
            let mut conflict: Conflict = serde_json::from_str(&payload).unwrap();
            conflict.resolved_by = None;
            (id, (conflict, resolved))
        })
        .collect()
}

fn quarantined(s: &Store) -> Vec<(String, String)> {
    s.db.prepare("SELECT id,error FROM incoming_events WHERE error IS NOT NULL ORDER BY id")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

fn staged_ids(s: &Store) -> BTreeSet<String> {
    s.db.prepare("SELECT id FROM incoming_events")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

fn bundle(events: Vec<Event>) -> Bundle {
    Bundle {
        repo: REPO.into(),
        branches: vec![],
        events,
        objects: BTreeMap::new(),
    }
}

/// Whether full mode also refuses the quarantined event `id`: retried, in
/// full mode, on a consistent copy of the store.
fn full_mode_refuses(s: &Store, id: &str) -> bool {
    let dir = tempfile::tempdir().unwrap();
    let copy = dir.path().join("metadata.sqlite");
    s.db.execute("VACUUM INTO ?", [copy.to_str().unwrap()])
        .unwrap();
    let _mode = ModeGuard::new(ConflictMode::Full);
    let mut copy = Store::open(dir.path(), Some(REPO)).unwrap();
    copy.db
        .execute("UPDATE incoming_events SET error=NULL WHERE id=?", [id])
        .unwrap();
    copy.receive(bundle(vec![]), &BTreeSet::new()).unwrap();
    let error: Option<String> = copy
        .db
        .query_row("SELECT error FROM incoming_events WHERE id=?", [id], |r| {
            r.get(0)
        })
        .unwrap_or(None);
    error.is_some_and(|e| e.contains("NAMESPACE_HISTORY_LIMIT"))
}

/// The visible path of an entry, if it and every ancestor are visible.
fn path_of(p: &Projection, id: &str) -> Option<String> {
    let mut parts = vec![];
    let mut cursor = id;
    while cursor != ROOT {
        let e = p.entries.get(cursor).filter(|e| e.alive)?;
        parts.push(e.name.clone());
        cursor = &e.parent;
        if parts.len() > 64 {
            return None;
        }
    }
    parts.reverse();
    Some(parts.join("/"))
}

fn join(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.into()
    } else {
        format!("{dir}/{name}")
    }
}

struct Replica {
    dir: TempDir,
    store: Option<Store>,
}
impl Replica {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Some(Store::open(dir.path(), Some(REPO)).unwrap());
        Self { dir, store }
    }
    fn store(&mut self) -> &mut Store {
        self.store.as_mut().unwrap()
    }
    fn restart(&mut self) {
        self.store = None;
        self.store = Some(Store::open(self.dir.path(), Some(REPO)).unwrap());
    }
}

struct Fuzz {
    seed: u64,
    mode: ConflictMode,
    rng: StdRng,
    replicas: Vec<Replica>,
    allowed: BTreeSet<String>,
    log: Vec<String>,
    serial: usize,
    refusals: usize,
    /// Successful operations by kind, and final conflict shapes.
    stats: BTreeMap<String, usize>,
    /// Refusals already checked against full mode (`replica/event`).
    parity_checked: BTreeSet<String>,
    stress: bool,
}

impl Fuzz {
    fn new(seed: u64, mode: ConflictMode) -> Self {
        Self::with(seed, mode, false)
    }

    /// `stress`: four replicas that sync rarely, so concurrent namespace
    /// chains pile up towards `MAX_NAMESPACE_CUTS` (refusals are checked
    /// against full mode).
    fn with(seed: u64, mode: ConflictMode, stress: bool) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let count = if stress { 4 } else { rng.gen_range(2..=4) };
        let mut replicas: Vec<_> = (0..count).map(|_| Replica::new()).collect();
        let allowed = replicas
            .iter_mut()
            .map(|r| r.store().device.clone())
            .collect();
        Self {
            seed,
            mode,
            rng,
            replicas,
            allowed,
            log: vec![],
            serial: 0,
            refusals: 0,
            stats: BTreeMap::new(),
            parity_checked: BTreeSet::new(),
            stress,
        }
    }

    #[track_caller]
    fn fail(&self, what: impl std::fmt::Display) -> ! {
        let tail: Vec<_> = self.log.iter().rev().take(40).rev().cloned().collect();
        panic!(
            "seed {} ({} mode): {what}\nlast steps:\n  {}",
            self.seed,
            self.mode.as_str(),
            tail.join("\n  ")
        );
    }

    /// The oracle is empty, and nothing was quarantined except a budget
    /// refusal (which is checked for parity in verify mode).
    fn check(&mut self, r: usize, after: &str) {
        let started = std::time::Instant::now();
        let diff = self.replicas[r].store().verify_derived_state();
        *self.stats.entry("oracle_ms".into()).or_default() +=
            started.elapsed().as_millis() as usize;
        let diff = match diff {
            Ok(diff) => diff,
            Err(error) => self.fail(format!(
                "replica {r} after {after}: rebuild refused: {error:#}"
            )),
        };
        if !diff.is_empty() {
            self.fail(format!("replica {r} after {after}: oracle {diff:#?}"));
        }
        // A refusal is legitimate only if full mode refuses the same event
        // (checked on a copy of the store); a descendant of a refused event
        // waits on it. Anything else is a failure, reported with every row.
        let rows: BTreeMap<String, String> =
            quarantined(self.replicas[r].store()).into_iter().collect();
        for (id, error) in &rows {
            if error.contains("NAMESPACE_HISTORY_LIMIT") {
                if self.parity_checked.insert(format!("{r}/{id}")) {
                    if !full_mode_refuses(self.replicas[r].store(), id) {
                        self.fail(format!(
                            "replica {r} after {after}: {id} refused ({error}) but full mode accepts it"
                        ));
                    }
                    *self.stats.entry("refused_deliveries".into()).or_default() += 1;
                }
                continue;
            }
            if error
                .strip_prefix("QUARANTINED_CAUSAL_ANCESTOR: ")
                .is_some_and(|ancestor| rows.contains_key(ancestor))
            {
                continue;
            }
            self.fail(format!(
                "replica {r} after {after}: {id} quarantined: {error}
all quarantined rows: {rows:#?}"
            ));
        }
    }

    fn step(&mut self) {
        let r = self.rng.gen_range(0..self.replicas.len());
        let local = if self.stress { 79 } else { 54 };
        match self.rng.gen_range(0..100) {
            n if n <= local => self.local(r),
            55..=89 => {
                let mut to = self.rng.gen_range(0..self.replicas.len() - 1);
                if to >= r {
                    to += 1;
                }
                self.deliver(r, to, false);
            }
            90..=94 => {
                self.log.push(format!("restart {r}"));
                self.replicas[r].restart();
                self.check(r, "restart");
            }
            _ => {
                let to = (r + 1) % self.replicas.len();
                self.deliver(r, to, true);
            }
        }
    }

    fn local(&mut self, r: usize) {
        let op = self.rng.gen_range(0..100);
        self.serial += 1;
        let serial = self.serial;
        let pick = |rng: &mut StdRng, n: usize| rng.gen_range(0..n.max(1));
        let s = self.replicas[r].store.as_mut().unwrap();
        let p = s.projection(&s.active).unwrap();
        let visible: Vec<(String, String)> = p
            .entries
            .keys()
            .filter_map(|id| path_of(&p, id).map(|path| (id.clone(), path)))
            .collect();
        let mut dirs: Vec<String> = visible
            .iter()
            .filter(|(id, _)| p.entries[id].kind == "directory")
            .map(|(_, path)| path.clone())
            .collect();
        dirs.push(String::new());
        let dir = dirs[pick(&mut self.rng, dirs.len())].clone();
        let name = NAMES[pick(&mut self.rng, NAMES.len())];
        let target = visible.get(pick(&mut self.rng, visible.len())).cloned();
        let files: Vec<&Entry> = p.entries.values().filter(|e| e.kind == "file").collect();
        let file = files
            .get(pick(&mut self.rng, files.len()))
            .map(|e| (*e).clone());
        let (what, result, expected): (String, anyhow::Result<()>, &[&str]) = match op {
            0..=24 => {
                let kind = if self.rng.gen_bool(0.6) {
                    "file"
                } else {
                    "directory"
                };
                let path = join(&dir, name);
                (
                    format!("create {kind} {path}"),
                    s.create(&path, kind).map(|_| ()),
                    &["NAME_COLLISION", "NOT_DIRECTORY", "FILE_NOT_FOUND"],
                )
            }
            25..=44 => {
                let Some((id, path)) = target else { return };
                let to = join(&dir, name);
                let replace = self.rng.gen_bool(0.3);
                (
                    format!("rename {path} -> {to} replace={replace}"),
                    s.rename(&id, &to, replace),
                    &[
                        "NAME_COLLISION",
                        "DIRECTORY_CYCLE",
                        "NOT_DIRECTORY",
                        "FILE_NOT_FOUND",
                    ],
                )
            }
            45..=54 => {
                let Some((id, path)) = target else { return };
                (
                    format!("delete {path}"),
                    s.delete(&id),
                    &["DIRECTORY_NOT_EMPTY"],
                )
            }
            55..=69 => {
                // A save; on a deleted file it resurrects it.
                let Some(file) = file else { return };
                (
                    format!("write {} alive={}", file.id, file.alive),
                    s.write_revision(
                        &file.id,
                        file.revisions.get("content").cloned(),
                        format!("bytes {serial}").as_bytes(),
                    )
                    .map(|_| ()),
                    &[],
                )
            }
            70..=74 => {
                // A save on a stale base: concurrent versions on one replica.
                let Some(file) = file else { return };
                let history: Vec<Event> = s
                    .events()
                    .unwrap()
                    .into_iter()
                    .filter(|e| e.changes.iter().any(|c| c.entity == file.id))
                    .collect();
                let base = |field: &str, rng: &mut StdRng| {
                    let versions: Vec<_> = history
                        .iter()
                        .filter(|e| {
                            e.changes
                                .iter()
                                .any(|c| c.entity == file.id && c.field == field)
                        })
                        .map(|e| e.id.clone())
                        .collect();
                    versions.choose(rng).cloned()
                };
                let content = base("content", &mut self.rng);
                let alive = base("alive", &mut self.rng);
                (
                    format!("stale write {}", file.id),
                    s.write_with_base(
                        &file.id,
                        content,
                        alive,
                        format!("stale {serial}").as_bytes(),
                    )
                    .map(|_| ()),
                    &[],
                )
            }
            75..=79 => {
                let Some((id, path)) = target else { return };
                let entry = &p.entries[&id];
                let mut basic = entry.basic_info();
                basic.attributes =
                    BasicInfo::attributes(&entry.kind, basic.attributes ^ 0x1).unwrap();
                (format!("set_basic {path}"), s.set_basic(&id, basic), &[])
            }
            80..=89 => {
                let open: Vec<Conflict> = s
                    .conflicts()
                    .unwrap()
                    .into_iter()
                    .filter(|c| c.resolved_by.is_none() && c.field != "namespace")
                    .collect();
                let Some(c) = open.choose(&mut self.rng).cloned() else {
                    return;
                };
                let selected = c
                    .alternatives
                    .choose(&mut self.rng)
                    .unwrap()
                    .revision
                    .clone();
                (
                    format!("resolve {} {}/{} -> {selected}", c.id, c.entity, c.field),
                    s.resolve(std::slice::from_ref(&c.id), &selected)
                        .map(|_| ()),
                    &["CONFLICT_NOT_FOUND", "REVISION_NOT_IN_REVIEW"],
                )
            }
            _ => {
                let open: Vec<Conflict> = s
                    .conflicts()
                    .unwrap()
                    .into_iter()
                    .filter(|c| c.resolved_by.is_none() && c.field == "namespace")
                    .collect();
                let Some(c) = open.choose(&mut self.rng).cloned() else {
                    return;
                };
                let entities: Vec<&str> = c.entity.split(',').collect();
                let entity = entities.choose(&mut self.rng).unwrap().to_string();
                let to = format!("recovered-{}-{serial}", self.seed);
                (
                    format!("recover {entity} -> {to} reviewing {}", c.id),
                    s.recover(&entity, &to, std::slice::from_ref(&c.id))
                        .map(|_| ()),
                    &[
                        "EXPLICIT_EXISTENCE_RESOLUTION_REQUIRED",
                        "REVIEW_HAS_NO_ENTRY_LOCATION",
                        "RECOVERY_NOT_VISIBLE",
                        "REVIEW_SCOPE_MISMATCH",
                        "ENTRY_NOT_FOUND",
                        "CONFLICT_NOT_FOUND",
                    ],
                )
            }
        };
        match result {
            Ok(()) => {
                let kind = what.split(' ').next().unwrap_or("").to_owned();
                *self.stats.entry(kind).or_default() += 1;
                self.log.push(format!("[{r}] {what}"));
            }
            Err(error) => {
                let text = format!("{error:#}");
                if text.contains("NAMESPACE_HISTORY_LIMIT") {
                    self.refusals += 1;
                } else if !expected.iter().any(|code| text.contains(code)) {
                    self.log.push(format!("[{r}] {what}"));
                    self.fail(format!("replica {r}: {what}: unexpected error {text}"));
                }
                self.log.push(format!("[{r}] {what}: {text}"));
                // A refused operation commits nothing (its savepoint rolled back).
                return;
            }
        }
        self.check(r, "local operation");
    }

    /// Deliver some (or all) of `from`'s shared events that `to` lacks, plus a
    /// few duplicates, shuffled, in random-size pages.
    fn deliver(&mut self, from: usize, to: usize, everything: bool) {
        let source: Vec<Event> = self.replicas[from]
            .store()
            .events()
            .unwrap()
            .into_iter()
            .filter(|e| e.branch.shared)
            .collect();
        let target = self.replicas[to].store();
        let committed: BTreeSet<String> =
            target.events().unwrap().into_iter().map(|e| e.id).collect();
        let staged = staged_ids(target);
        let (mut known, missing): (Vec<Event>, Vec<Event>) = source
            .into_iter()
            .partition(|e| committed.contains(&e.id) || staged.contains(&e.id));
        let mut events = missing;
        if !everything && !events.is_empty() && self.rng.gen_bool(0.7) {
            events.shuffle(&mut self.rng);
            let keep = self.rng.gen_range(1..=events.len());
            events.truncate(keep);
        }
        known.shuffle(&mut self.rng);
        let duplicates = self.rng.gen_range(0..=2usize).min(known.len());
        events.extend(known.into_iter().take(duplicates));
        if events.is_empty() {
            return;
        }
        events.shuffle(&mut self.rng);
        while !events.is_empty() {
            let take = self.rng.gen_range(1..=events.len().min(4));
            let page: Vec<Event> = events.drain(..take).collect();
            let ids: Vec<_> = page.iter().map(|e| e.id.clone()).collect();
            self.log
                .push(format!("deliver {from}->{to} {}", ids.join(" ")));
            let allowed = self.allowed.clone();
            if let Err(error) = self.replicas[to].store().receive(bundle(page), &allowed) {
                self.fail(format!("deliver {from}->{to}: {error:#}"));
            }
            self.check(to, "delivery");
        }
    }

    /// Exchange everything until every replica holds the same events, then
    /// require identical projections and conflict rows everywhere.
    fn converge(&mut self) {
        for _ in 0..4 {
            for from in 0..self.replicas.len() {
                for to in 0..self.replicas.len() {
                    if from != to {
                        self.deliver(from, to, true);
                    }
                }
            }
        }
        // Replicas that refused an event (a legitimate, full-mode-checked
        // budget refusal) hold different histories; all the others agree.
        let mut reference: Option<(BTreeSet<String>, Projection, Vec<Conflict>)> = None;
        for r in 0..self.replicas.len() {
            let s = self.replicas[r].store();
            let events: BTreeSet<String> = s.events().unwrap().into_iter().map(|e| e.id).collect();
            let state = (
                events,
                s.projection(&s.active).unwrap(),
                s.conflicts().unwrap(),
            );
            let staged = staged_ids(s);
            let refused = quarantined(s);
            if !staged.is_empty() && refused.is_empty() {
                self.fail(format!("replica {r} did not drain: {staged:?}"));
            }
            match &reference {
                _ if !refused.is_empty() => {}
                None => reference = Some(state),
                Some(reference) => {
                    if reference.0 != state.0 {
                        self.fail(format!("replica {r} holds different events"));
                    }
                    if reference.1 != state.1 {
                        self.fail(format!("replica {r} projects differently"));
                    }
                    if reference.2 != state.2 {
                        self.fail(format!(
                            "replica {r} has different conflict rows:\n{:#?}\nvs\n{:#?}",
                            reference.2, state.2
                        ));
                    }
                }
            }
        }
        let s = self.replicas[0].store();
        *self.stats.entry("events".into()).or_default() += s.events().unwrap().len();
        for c in s.conflicts().unwrap() {
            let kind = match (c.field.as_str(), c.reason.as_str()) {
                ("namespace", reason) if reason.contains("cycle") => "conflict:parent-or-cycle",
                ("namespace", _) => "conflict:namespace",
                _ => "conflict:register",
            };
            *self.stats.entry(kind.into()).or_default() += 1;
            if c.resolved_by.is_some() {
                *self.stats.entry("conflict:resolved".into()).or_default() += 1;
            }
        }
    }
}

fn run_seed(
    seed: u64,
    steps: std::ops::RangeInclusive<usize>,
    stress: bool,
) -> (DeriveCounters, usize, BTreeMap<String, usize>) {
    let mode = if seed.is_multiple_of(3) {
        ConflictMode::Verify
    } else {
        ConflictMode::Append
    };
    let _mode = ModeGuard::new(mode);
    let start = thread_derive_counters();
    let mut fuzz = Fuzz::with(seed, mode, stress);
    let steps = fuzz.rng.gen_range(steps);
    for _ in 0..steps {
        fuzz.step();
    }
    fuzz.converge();
    (counters_since(start), fuzz.refusals, fuzz.stats)
}

fn run_seeds(
    seeds: impl Iterator<Item = u64>,
    steps: std::ops::RangeInclusive<usize>,
    stress: bool,
) {
    let mut total = DeriveCounters::default();
    let mut refusals = 0;
    let mut stats: BTreeMap<String, usize> = BTreeMap::new();
    for seed in seeds {
        let started = std::time::Instant::now();
        let (c, r, seed_stats) = run_seed(seed, steps.clone(), stress);
        eprintln!(
            "DERIVE_SEED {seed}: {} ms, {} append, {} verified, {} fallbacks, {} refused deliveries",
            started.elapsed().as_millis(),
            c.append,
            c.verified,
            c.fallbacks,
            seed_stats.get("refused_deliveries").copied().unwrap_or(0)
        );
        for (k, v) in seed_stats {
            *stats.entry(k).or_default() += v;
        }
        total.append += c.append;
        total.fallbacks += c.fallbacks;
        total.verified += c.verified;
        total.full += c.full;
        total.rebuilds += c.rebuilds;
        total.restricted += c.restricted;
        total.commit_micros += c.commit_micros;
        refusals += r;
    }
    println!(
        "DERIVE_FUZZ {}",
        json!({"counters": total, "refusals": refusals, "stats": stats,
            "fallback_reasons": thread_fallback_reasons()})
    );
    // The fuzzer must actually exercise append mode, and verify mode must
    // have compared real commits.
    assert!(total.append > 0, "{total:?}");
    assert!(total.verified > 0, "{total:?}");
    assert!(total.restricted > 0, "{total:?}");
}

/// Steps per seed in the debug suite (the oracle runs after every one, and a
/// rebuild is quadratic in history, so debug histories stay short).
const DEBUG_STEPS: std::ops::RangeInclusive<usize> = 50..=90;

#[test]
fn fuzz_seeds_00_09() {
    run_seeds(0..10, DEBUG_STEPS, false);
}
#[test]
fn fuzz_seeds_10_19() {
    run_seeds(10..20, DEBUG_STEPS, false);
}
#[test]
fn fuzz_seeds_20_29() {
    run_seeds(20..30, DEBUG_STEPS, false);
}
#[test]
fn fuzz_seeds_30_39() {
    run_seeds(30..40, DEBUG_STEPS, false);
}
#[test]
fn fuzz_seeds_40_49() {
    run_seeds(40..50, DEBUG_STEPS, false);
}

/// Qualification: `cargo test --release --test derive_oracle -- --ignored`.
/// `TKFS_DERIVE_SEEDS` (default 300) and `TKFS_DERIVE_SEED_START` (default
/// 1000, disjoint from the debug seeds) select the seeds.
#[test]
#[ignore = "WP1.7 release qualification (300 seeds)"]
fn fuzz_300_seeds_release() {
    let env = |key: &str, default: u64| {
        std::env::var(key)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    let start = env("TKFS_DERIVE_SEED_START", 1000);
    let count = env("TKFS_DERIVE_SEEDS", 300);
    run_seeds(start..start + count, 100..=180, false);
}

/// Stress: four rarely-syncing replicas over long runs, so receivers meet
/// `MAX_NAMESPACE_CUTS`; every refusal must be a full-mode refusal too.
/// `TKFS_DERIVE_STRESS_START` (default 5000), `TKFS_DERIVE_STRESS_SEEDS`
/// (default 40).
#[test]
#[ignore = "WP1.7 release stress (namespace budget refusals)"]
fn fuzz_stress_release() {
    let env = |key: &str, default: u64| {
        std::env::var(key)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    let start = env("TKFS_DERIVE_STRESS_START", 5000);
    let count = env("TKFS_DERIVE_STRESS_SEEDS", 40);
    run_seeds(start..start + count, 200..=320, true);
}

/// Authors make a corpus with rich concurrency (occasional partial syncs);
/// its union is then delivered, in the same random pages, to an append-mode
/// and a full-mode receiver, which must agree row for row after every page,
/// across restarts at the same points.
#[test]
fn append_and_full_receivers_store_identical_rows() {
    let mut appended = 0;
    for seed in 0..8u64 {
        let _mode = ModeGuard::new(ConflictMode::Append);
        let mut fuzz = Fuzz::new(500 + seed, ConflictMode::Append);
        for _ in 0..50 {
            match fuzz.rng.gen_range(0..10) {
                0..=7 => {
                    let r = fuzz.rng.gen_range(0..fuzz.replicas.len());
                    fuzz.local(r);
                }
                _ => {
                    let from = fuzz.rng.gen_range(0..fuzz.replicas.len());
                    let to = (from + 1) % fuzz.replicas.len();
                    fuzz.deliver(from, to, false);
                }
            }
        }
        let mut corpus: BTreeMap<String, Event> = BTreeMap::new();
        for replica in &mut fuzz.replicas {
            for e in replica.store().events().unwrap() {
                if e.branch.shared {
                    corpus.insert(e.id.clone(), e);
                }
            }
        }
        let allowed = fuzz.allowed.clone();
        for order in 0..3 {
            let mut rng = StdRng::seed_from_u64(seed * 100 + order);
            let mut events: Vec<Event> = corpus.values().cloned().collect();
            events.shuffle(&mut rng);
            let mut append = Replica::new();
            let mut full = Replica::new();
            let start = thread_derive_counters();
            while !events.is_empty() {
                let take = rng.gen_range(1..=events.len().min(5));
                let page: Vec<Event> = events.drain(..take).collect();
                {
                    let _mode = ModeGuard::new(ConflictMode::Append);
                    append
                        .store()
                        .receive(bundle(page.clone()), &allowed)
                        .unwrap();
                }
                {
                    let _mode = ModeGuard::new(ConflictMode::Full);
                    full.store().receive(bundle(page), &allowed).unwrap();
                }
                if rng.gen_bool(0.1) {
                    append.restart();
                    full.restart();
                }
                let (a, f) = (append.store(), full.store());
                assert_eq!(
                    conflict_rows(a),
                    conflict_rows(f),
                    "seed {seed} order {order}"
                );
                assert_eq!(
                    a.projection(&a.active).unwrap(),
                    f.projection(&f.active).unwrap()
                );
                assert_eq!(quarantined(a), quarantined(f));
                assert_eq!(staged_ids(a), staged_ids(f));
            }
            appended += counters_since(start).append;
            for receiver in [&mut append, &mut full] {
                let diff = receiver.store().verify_derived_state().unwrap();
                assert!(diff.is_empty(), "seed {seed} order {order}: {diff:#?}");
            }
        }
    }
    assert!(appended > 50, "append mode used for {appended} commits");
}

/// 13 directories concurrently renamed into one slot of `anchor`: the 13th
/// crosses `MAX_NAMESPACE_CUTS` (8,178 > 4,096 competing cuts). Then a
/// concurrent rename of an already colliding directory into the same slot
/// (an old, touched cone whose members do not change) crosses it as well.
/// Both are refused at the same event, with the same error and no ACK, in
/// every mode; with 8 renames the same late rename is accepted identically.
#[test]
fn history_limit_refused_at_the_same_event_in_every_mode() {
    let source = Replica::new();
    let mut source = source;
    let s = source.store();
    let anchor = s.create("anchor", "directory").unwrap();
    let dirs: Vec<Entry> = (0..13)
        .map(|i| {
            s.create(&format!("anchor/dir-{i:02}"), "directory")
                .unwrap()
        })
        .collect();
    let branch = s.branch(&s.active).unwrap();
    let creates: Vec<Event> = s.events().unwrap();
    let mut moves: Vec<Event> = dirs
        .iter()
        .enumerate()
        .map(|(i, dir)| {
            let mut event = s.event(
                &branch,
                vec![
                    s.current_change(
                        &dir.id,
                        "location",
                        json!({"parent": anchor.id, "name": "collision"}),
                    )
                    .unwrap(),
                ],
            );
            event.id = format!("concurrent-{i:02}");
            event
        })
        .collect();
    // Concurrent with every move: dir-00 into the same slot, case folded.
    let mut late = s.event(
        &branch,
        vec![
            s.current_change(
                &dirs[0].id,
                "location",
                json!({"parent": anchor.id, "name": "COLLISION"}),
            )
            .unwrap(),
        ],
    );
    late.id = "late-rename".into();
    let allowed = BTreeSet::from([s.device.clone()]);

    // (mode, moves delivered) -> per delivery (acked ids, quarantine), rows.
    type Outcome = (
        Vec<(Vec<String>, Vec<(String, String)>)>,
        BTreeMap<String, (Conflict, Option<String>)>,
    );
    let run = |mode: ConflictMode, count: usize| -> Outcome {
        let _mode = ModeGuard::new(mode);
        let mut receiver = Replica::new();
        let r = receiver.store();
        let mut deliveries = vec![];
        r.receive(bundle(creates.clone()), &allowed).unwrap();
        for event in moves[..count].iter().chain([&late]) {
            let acked = r.receive(bundle(vec![event.clone()]), &allowed).unwrap();
            deliveries.push((acked, quarantined(r)));
        }
        let diff = r.verify_derived_state().unwrap();
        assert!(diff.is_empty(), "{mode:?}: {diff:#?}");
        (deliveries, conflict_rows(r))
    };
    for count in [13, 8] {
        let full = run(ConflictMode::Full, count);
        for mode in [ConflictMode::Append, ConflictMode::Verify] {
            assert_eq!(run(mode, count), full, "{mode:?} with {count} renames");
        }
        let (deliveries, rows) = full;
        let refused: Vec<&String> = deliveries
            .last()
            .unwrap()
            .1
            .iter()
            .filter(|(_, error)| error.contains("NAMESPACE_HISTORY_LIMIT"))
            .map(|(id, _)| id)
            .collect();
        if count == 13 {
            // The 13th rename and the late rename are refused and never acked.
            assert_eq!(refused, vec!["concurrent-12", "late-rename"]);
            for (acked, _) in &deliveries[12..] {
                assert!(acked.is_empty(), "{acked:?}");
            }
            assert!(deliveries[11].0 == vec!["concurrent-11".to_string()]);
        } else {
            assert!(refused.is_empty());
            assert_eq!(deliveries.last().unwrap().0, vec![late.id.clone()]);
            assert!(rows.values().any(|(c, _)| c.field == "namespace"
                && c.alternatives.iter().any(|v| v.revision == late.id)));
        }
    }
    moves.clear();
}

/// The workloads append mode exists for take the append path, never the
/// fallback: a two-way backlog (A's creates while B has one unseen edit),
/// directory creation plus moves into it, deletes, renames in place and
/// content saves on the receiving side, all with the oracle empty.
#[test]
fn common_remote_workloads_take_the_append_path() {
    let _mode = ModeGuard::new(ConflictMode::Append);
    let mut a = Replica::new();
    let mut b = Replica::new();
    let allowed: BTreeSet<String> = [a.store().device.clone(), b.store().device.clone()].into();
    let shared = a.store().create("shared.txt", "file").unwrap();
    for i in 0..20 {
        a.store()
            .create(&format!("lin-{i:02}.txt"), "file")
            .unwrap();
    }
    let seed = a.store().shared_bundle(&BTreeSet::new()).unwrap();
    b.store().receive(seed, &allowed).unwrap();
    // B's unseen local edit makes every one of A's events a slow-path commit.
    let local = b.store().lookup("shared.txt").unwrap();
    b.store()
        .write_revision(
            &shared.id,
            local.revisions.get("content").cloned(),
            b"b edit",
        )
        .unwrap();
    let before = b.store().shared_event_inventory().unwrap();
    {
        let a = a.store();
        for i in 0..10 {
            a.create(&format!("backlog-{i:02}.txt"), "file").unwrap();
        }
        for i in 0..5 {
            a.create(&format!("dir-{i}"), "directory").unwrap();
            let file = a.lookup(&format!("lin-{i:02}.txt")).unwrap();
            a.rename(&file.id, &format!("dir-{i}/moved.txt"), false)
                .unwrap();
        }
        let file = a.lookup("lin-10.txt").unwrap();
        a.rename(&file.id, "renamed.txt", false).unwrap();
        let file = a.lookup("lin-11.txt").unwrap();
        a.delete(&file.id).unwrap();
        let file = a.lookup("lin-12.txt").unwrap();
        a.write_revision(&file.id, file.revisions.get("content").cloned(), b"a edit")
            .unwrap();
    }
    let mut events = a.store().shared_bundle(&before).unwrap().events;
    events.sort_by_key(|e| e.created_ms);
    let start = thread_derive_counters();
    for event in events {
        let b = b.store();
        b.receive(bundle(vec![event]), &allowed).unwrap();
        let diff = b.verify_derived_state().unwrap();
        assert!(diff.is_empty(), "{diff:#?}");
    }
    let used = counters_since(start);
    assert_eq!(used.fallbacks, 0, "{:?} {:?}", used, thread_last_fallback());
    assert!(used.append >= 22, "{used:?}");
    assert!(b.store().lookup("dir-4/moved.txt").is_ok());
    assert!(b.store().lookup("renamed.txt").is_ok());
    assert!(b.store().lookup("lin-11.txt").is_err());
}

/// The only precondition failure honest histories produce: a slow-path TRUE
/// presence assertion that extends a compressed TRUE segment past its rank
/// extreme displaces an old witness (so full mode would stop enumerating the
/// antichains through it). Append mode counts the fallback and derives in
/// full; the oracle stays empty and verify mode agrees.
///
/// `f` is created, then concurrently deleted (`t`) and saved twice (`w1`,
/// `w2`, alive TRUE, not depending on `t`): one TRUE group whose witnesses are
/// the rank extremes `w1` and `w2`. `w3` (higher rank) extends the segment.
#[test]
fn fallback_when_a_presence_witness_is_displaced() {
    let crafted = |id: &str, device: &str, deps: &[&str], changes: Vec<Change>| Event {
        id: id.into(),
        repo: REPO.into(),
        branch: Branch {
            id: hash(format!("tkfs-v1/{REPO}/main").as_bytes()),
            name: "main".into(),
            shared: true,
        },
        device: device.into(),
        created_ms: 1,
        changes,
        dependencies: deps.iter().map(|d| d.to_string()).collect(),
        reviewed: vec![],
        object_sizes: BTreeMap::new(),
    };
    let change = |field: &str, parents: &[&str], value| Change {
        entity: "f".into(),
        field: field.into(),
        parents: parents.iter().map(|p| p.to_string()).collect(),
        value,
    };
    let content = |n: u8| json!(hash(&[n]));
    let events = vec![
        crafted(
            "seed",
            "dev-a",
            &[],
            vec![
                change("kind", &[], json!("file")),
                change("location", &[], json!({"parent": ROOT, "name": "f.txt"})),
                change("alive", &[], json!(true)),
                change("content", &[], content(0)),
            ],
        ),
        crafted(
            "t",
            "dev-a",
            &["seed"],
            vec![change("alive", &["seed"], json!(false))],
        ),
        crafted(
            "w1",
            "dev-b",
            &["seed"],
            vec![
                change("alive", &["seed"], json!(true)),
                change("content", &["seed"], content(1)),
            ],
        ),
        crafted(
            "w2",
            "dev-b",
            &["w1"],
            vec![
                change("alive", &["w1"], json!(true)),
                change("content", &["w1"], content(2)),
            ],
        ),
        crafted(
            "w3",
            "dev-b",
            &["w2"],
            vec![
                change("alive", &["w2"], json!(true)),
                change("content", &["w2"], content(3)),
            ],
        ),
    ];
    let allowed: BTreeSet<String> = ["dev-a".to_string(), "dev-b".to_string()].into();
    let mut rows = vec![];
    for mode in [
        ConflictMode::Append,
        ConflictMode::Verify,
        ConflictMode::Full,
    ] {
        let _mode = ModeGuard::new(mode);
        let mut receiver = Replica::new();
        let r = receiver.store();
        for event in &events {
            let start = thread_derive_counters();
            let acked = r.receive(bundle(vec![event.clone()]), &allowed).unwrap();
            assert_eq!(acked, vec![event.id.clone()], "{mode:?}");
            let used = counters_since(start);
            let diff = r.verify_derived_state().unwrap();
            assert!(diff.is_empty(), "{mode:?} after {}: {diff:#?}", event.id);
            match (mode, event.id.as_str()) {
                (ConflictMode::Full, _) => assert_eq!(used.full, 1, "{used:?}"),
                (_, "w3") => {
                    assert_eq!(used.fallbacks, 1, "{used:?}");
                    assert_eq!(
                        thread_last_fallback(),
                        Some("a touched cone lost a relevant event")
                    );
                }
                (ConflictMode::Append, _) => assert_eq!(used.append, 1, "{used:?}"),
                _ => assert_eq!(used.verified, 1, "{used:?}"),
            }
        }
        assert!(r.conflicts().unwrap().iter().any(|c| c.field == "alive"));
        rows.push(conflict_rows(r));
    }
    assert!(rows.windows(2).all(|w| w[0] == w[1]));
}

/// The oracle starts its rebuild from no conflict rows, so excess derived
/// state is reported: a fabricated register record, an inflated or invented
/// `resolved_by`, a stale namespace record. It also works inside a caller's
/// transaction and leaves it open.
#[test]
fn oracle_reports_excess_derived_state() {
    let fixture = || {
        let mut a = Replica::new();
        let mut b = Replica::new();
        let allowed: BTreeSet<String> = [a.store().device.clone(), b.store().device.clone()].into();
        a.store().create("shared.txt", "file").unwrap();
        let known = b.store().shared_event_inventory().unwrap();
        let page = a.store().shared_bundle(&known).unwrap();
        b.store().receive(page, &allowed).unwrap();
        for (r, bytes) in [(&mut a, b"from a" as &[u8]), (&mut b, b"from b")] {
            let e = r.store().lookup("shared.txt").unwrap();
            r.store()
                .write_revision(&e.id, e.revisions.get("content").cloned(), bytes)
                .unwrap();
        }
        a.store().create("same.txt", "file").unwrap();
        b.store().create("same.txt", "file").unwrap();
        for from in [0, 1] {
            let (source, target) = if from == 0 {
                (&mut a, &mut b)
            } else {
                (&mut b, &mut a)
            };
            let known = target.store().shared_event_inventory().unwrap();
            let page = source.store().shared_bundle(&known).unwrap();
            target.store().receive(page, &allowed).unwrap();
        }
        let content = a
            .store()
            .conflicts()
            .unwrap()
            .into_iter()
            .find(|c| c.field == "content")
            .unwrap();
        let selected = content.alternatives[0].revision.clone();
        a.store()
            .resolve(std::slice::from_ref(&content.id), &selected)
            .unwrap();
        let diff = a.store().verify_derived_state().unwrap();
        assert!(diff.is_empty(), "{diff:#?}");
        (a, b)
    };

    // A fabricated register record.
    let (mut a, _b) = fixture();
    let s = a.store();
    let mut fake = s
        .conflicts()
        .unwrap()
        .into_iter()
        .find(|c| c.field == "content")
        .unwrap();
    fake.id = hash(b"fabricated register record");
    fake.resolved_by = None;
    s.db.execute(
        "INSERT INTO conflicts(id,branch,payload) VALUES(?,?,?)",
        rusqlite::params![fake.id, s.active, serde_json::to_string(&fake).unwrap()],
    )
    .unwrap();
    let diff = s.verify_derived_state().unwrap();
    assert_eq!(
        diff.conflicts_extra,
        vec![format!("{}/{}", s.active, fake.id)]
    );
    assert!(diff.conflicts_missing.is_empty() && diff.resolved_by_mismatch.is_empty());

    // An inflated receipt (larger than any review sets) and an invented one.
    let (mut a, _b) = fixture();
    let s = a.store();
    let rows = s.conflicts().unwrap();
    let resolved = rows.iter().find(|c| c.resolved_by.is_some()).unwrap();
    let open = rows.iter().find(|c| c.resolved_by.is_none()).unwrap();
    for (id, value) in [(&resolved.id, "zzzz-inflated"), (&open.id, "invented")] {
        s.db.execute(
            "UPDATE conflicts SET resolved_by=? WHERE id=?",
            rusqlite::params![value, id],
        )
        .unwrap();
    }
    let diff = s.verify_derived_state().unwrap();
    assert_eq!(diff.resolved_by_mismatch.len(), 2, "{diff:#?}");
    assert!(diff.conflicts_extra.is_empty() && diff.conflicts_missing.is_empty());

    // A payload altered in place (same id) is reported, not repaired.
    let (mut a, _b) = fixture();
    let s = a.store();
    let mut altered = s
        .conflicts()
        .unwrap()
        .into_iter()
        .find(|c| c.field == "namespace")
        .unwrap();
    altered.reason = "tampered".into();
    s.db.execute(
        "UPDATE conflicts SET payload=? WHERE id=?",
        rusqlite::params![serde_json::to_string(&altered).unwrap(), altered.id],
    )
    .unwrap();
    let diff = s.verify_derived_state().unwrap();
    assert_eq!(
        diff.conflicts_extra,
        vec![format!("{}/{}: payload differs", s.active, altered.id)]
    );
    assert_eq!(
        s.conflicts()
            .unwrap()
            .iter()
            .find(|c| c.id == altered.id)
            .unwrap()
            .reason,
        "tampered"
    );

    // Inside a caller's transaction the oracle nests and leaves it open.
    let (mut a, _b) = fixture();
    let s = a.store();
    s.db.execute_batch("BEGIN").unwrap();
    assert!(s.verify_derived_state().unwrap().is_empty());
    assert!(!s.db.is_autocommit());
    s.db.execute_batch("ROLLBACK").unwrap();
    assert!(s.verify_derived_state().unwrap().is_empty());
    assert!(s.db.is_autocommit());
}
