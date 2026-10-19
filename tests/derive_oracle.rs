//! WP1.7: append-mode conflict derivation is exact (REVAMP_PLAN §5.2).
//!
//! - The fuzzer: 2–4 replicas make random creates, mkdirs, renames, directory
//!   moves (concurrent cycles included), deletes, resurrections, content
//!   saves (`alive:true`), stale-base saves, metadata changes, temp
//!   replacements, resolutions and recoveries, and exchange events in random
//!   delivery orders (subsets, shuffles, duplicates, children before parents)
//!   with random restarts. Every commit is checked inside its transaction
//!   (`set_thread_commit_checks`): the branch's conflict rows equal a
//!   derivation from no rows, and a refused commit is refused by full mode
//!   with the same error (local operations and deliveries alike). After
//!   every local operation, delivery page and restart the oracle (with every
//!   row re-derived from no rows, `verify_derived_state_strict`) must be
//!   empty, which also proves the history still rebuilds (refusal parity
//!   with startup repair). Finally every replica converges to identical rows.
//!   Seeds run in append mode (append's own output feeds later appends) and,
//!   every third seed, in verify mode (each commit compared with full mode).
//!   50 seeds in debug; `fuzz_300_seeds_release` is the qualification run.
//! - Differential: one event corpus delivered in the same random order to an
//!   append-mode and a full-mode receiver must leave identical rows after
//!   every delivery.
//! - Refusal parity at `MAX_NAMESPACE_CUTS`, append usage on the workloads
//!   the package exists for, the fallbacks of crafted histories, and the
//!   oracle's excess-state detection.
use rand::{Rng, SeedableRng, rngs::StdRng, seq::SliceRandom};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use tempfile::TempDir;
use tkfs::core::{
    derive::{
        ConflictMode, DeriveCounters, set_thread_commit_checks, set_thread_conflict_mode,
        thread_derive_counters, thread_fallback_reasons, thread_last_fallback,
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

/// Checks every commit of the calling thread against the reference;
/// restores the previous setting on drop.
struct ChecksGuard(bool);
impl ChecksGuard {
    fn new() -> Self {
        Self(set_thread_commit_checks(true))
    }
}
impl Drop for ChecksGuard {
    fn drop(&mut self) {
        set_thread_commit_checks(self.0);
    }
}

/// The oracle with every conflict row re-derived from no rows (the adapter
/// for CCR-WP1.7-1).
fn oracle(s: &mut Store) -> anyhow::Result<DerivedDiff> {
    s.verify_derived_state_strict()
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
        checked: now.checked - start.checked,
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
/// full mode, on a consistent copy of the store. The row is re-queued with
/// `Store::revalidate` (WP1.6's inbox state machine: clearing `error` alone
/// leaves the row `quarantined`, never retried, so every refusal read as
/// "full mode accepts it"; WP1.INT fix round 1).
fn full_mode_refuses(s: &Store, id: &str) -> bool {
    let dir = tempfile::tempdir().unwrap();
    let copy = dir.path().join("metadata.sqlite");
    s.db.execute("VACUUM INTO ?", [copy.to_str().unwrap()])
        .unwrap();
    let _mode = ModeGuard::new(ConflictMode::Full);
    let mut copy = Store::open(dir.path(), Some(REPO)).unwrap();
    // The row itself, plus any row blocked on it.
    assert!(
        copy.revalidate(Some(&[id.to_owned()])).unwrap() >= 1,
        "{id}"
    );
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
    /// refusal that full mode also makes (or a descendant of one).
    fn check(&mut self, r: usize, after: &str) {
        let started = std::time::Instant::now();
        let diff = oracle(self.replicas[r].store());
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
            // A failed commit check quotes the refusal it compared.
            if error.contains("CONFLICT_MODE_MISMATCH") {
                self.fail(format!("replica {r} after {after}: {id}: {error}"));
            }
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
                if text.contains("CONFLICT_MODE_MISMATCH") {
                    self.log.push(format!("[{r}] {what}"));
                    self.fail(format!("replica {r}: {what}: {text}"));
                }
                if text.contains("NAMESPACE_HISTORY_LIMIT") {
                    // The commit check made full mode refuse it too.
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
    let _checks = ChecksGuard::new();
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
            "DERIVE_SEED {seed}: {} ms, {} append, {} verified, {} fallbacks, {} checked, {} refused deliveries",
            started.elapsed().as_millis(),
            c.append,
            c.verified,
            c.fallbacks,
            c.checked,
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
        total.checked += c.checked;
        refusals += r;
    }
    println!(
        "DERIVE_FUZZ {}",
        json!({"counters": total, "refusals": refusals, "stats": stats,
            "fallback_reasons": thread_fallback_reasons()})
    );
    // The fuzzer must actually exercise append mode, verify mode must have
    // compared real commits, and every slow-path commit must have been
    // checked (a refused one is checked but not counted by mode).
    assert!(total.append > 0, "{total:?}");
    assert!(total.verified > 0, "{total:?}");
    assert!(total.restricted > 0, "{total:?}");
    assert!(
        total.checked >= total.append + total.fallbacks + total.verified + total.full,
        "{total:?}"
    );
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
                let diff = oracle(receiver.store()).unwrap();
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
        let diff = oracle(r).unwrap();
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

/// CCR-WP4.4-5 (WP4.INT): a content successor (a save committed on the fast
/// path: content plus `alive = true`, one head per register, not the whole
/// frontier) that is concurrent with a cone's relevant events and saves an
/// entity with a tombstone adds a TRUE-presence witness to that cone. Here
/// `f.txt`'s cone holds the historical slot `anchor/collision` of its parent,
/// into which twelve other directories move concurrently: 4,083 competing
/// cuts, within `MAX_NAMESPACE_CUTS`. A save of `f.txt` concurrent with all
/// twelve would make 8,178. Full replay refuses it, so the fast path must
/// too, before its ACK: before the fix it was accepted and the next rebuild
/// of the branch failed (`NAMESPACE_HISTORY_LIMIT`), as the release stress
/// fuzz found.
#[test]
fn a_content_successor_that_tips_a_cone_over_the_cut_limit_is_refused() {
    let mut source = Replica::new();
    let s = source.store();
    let anchor = s.create("anchor", "directory").unwrap();
    let home = s.create("anchor/collision", "directory").unwrap();
    let file = s.create("anchor/collision/f.txt", "file").unwrap();
    s.rename(&home.id, "anchor/home", false).unwrap();
    let dirs: Vec<Entry> = (0..12)
        .map(|i| {
            s.create(&format!("anchor/dir-{i:02}"), "directory")
                .unwrap()
        })
        .collect();
    s.delete(&file.id).unwrap();
    let branch = s.branch(&s.active).unwrap();
    let history: Vec<Event> = s.events().unwrap();
    // The undelete: alive TRUE parented by the tombstone (a primary change).
    let mut undelete = s.event(
        &branch,
        vec![s.current_change(&file.id, "alive", json!(true)).unwrap()],
    );
    undelete.id = "undelete".into();
    let moves: Vec<Event> = dirs
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
            event.dependencies = vec![undelete.id.clone()];
            event
        })
        .collect();
    // The save: concurrent with every move, one head per register.
    let mut save = s.event(
        &branch,
        vec![
            s.current_change(&file.id, "content", json!(hash(b"saved")))
                .unwrap(),
            Change {
                entity: file.id.clone(),
                field: "alive".into(),
                parents: vec![undelete.id.clone()],
                value: json!(true),
            },
        ],
    );
    save.id = "late-save".into();
    save.dependencies = vec![undelete.id.clone()];
    let allowed = BTreeSet::from([s.device.clone()]);
    for mode in [ConflictMode::Append, ConflictMode::Full] {
        let _mode = ModeGuard::new(mode);
        let mut receiver = Replica::new();
        let r = receiver.store();
        r.receive(bundle(history.clone()), &allowed).unwrap();
        r.receive(bundle(vec![undelete.clone()]), &allowed).unwrap();
        for event in &moves {
            let acked = r.receive(bundle(vec![event.clone()]), &allowed).unwrap();
            assert_eq!(
                acked,
                vec![event.id.clone()],
                "{mode:?}: {:?}",
                quarantined(r)
            );
        }
        // The save meets the content successor's conditions.
        let entry = r.lookup("anchor/home/f.txt").unwrap();
        assert!(entry.alive && entry.revisions["alive"] == undelete.id);
        let acked = r.receive(bundle(vec![save.clone()]), &allowed).unwrap();
        assert!(acked.is_empty(), "{mode:?}: the save was acknowledged");
        let refused = quarantined(r);
        assert!(
            refused
                .iter()
                .any(|(id, error)| id == "late-save" && error.contains("NAMESPACE_HISTORY_LIMIT")),
            "{mode:?}: {refused:?}"
        );
        let diff = oracle(r).unwrap();
        assert!(
            diff.is_empty(),
            "{mode:?}: the history no longer rebuilds: {diff:#?}"
        );
        assert!(
            full_mode_refuses(r, "late-save"),
            "{mode:?}: refusal parity"
        );
    }
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
        let diff = oracle(b).unwrap();
        assert!(diff.is_empty(), "{diff:#?}");
    }
    let used = counters_since(start);
    assert_eq!(used.fallbacks, 0, "{:?} {:?}", used, thread_last_fallback());
    assert!(used.append >= 22, "{used:?}");
    assert!(b.store().lookup("dir-4/moved.txt").is_ok());
    assert!(b.store().lookup("renamed.txt").is_ok());
    assert!(b.store().lookup("lin-11.txt").is_err());
}

/// WP5.INT (the WP5.1 review's minor): a slow commit that does not append
/// (full mode, or a declined append) leaves the handle's view where it is,
/// so it loads the stored projection no more often than an appending one
/// (before: full mode took the view out, dropped it unused and then loaded
/// it again for its delta, one extra O(entries) load per slow commit).
#[test]
fn slow_commits_that_do_not_append_keep_the_view() {
    let loads = |mode: ConflictMode| -> (u64, usize) {
        let _mode = ModeGuard::new(mode);
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
        // B's unseen local edit makes every one of A's events a slow commit.
        let local = b.store().lookup("shared.txt").unwrap();
        b.store()
            .write_revision(
                &shared.id,
                local.revisions.get("content").cloned(),
                b"b edit",
            )
            .unwrap();
        let before = b.store().shared_event_inventory().unwrap();
        for i in 0..12 {
            a.store()
                .create(&format!("backlog-{i:02}.txt"), "file")
                .unwrap();
        }
        let mut events = a.store().shared_bundle(&before).unwrap().events;
        events.sort_by_key(|e| e.created_ms);
        let count = events.len();
        // The first delivery warms the view; the rest are measured.
        let mut events = events.into_iter();
        b.store()
            .receive(bundle(vec![events.next().unwrap()]), &allowed)
            .unwrap();
        let start = b.store().view_loads();
        for event in events {
            b.store().receive(bundle(vec![event]), &allowed).unwrap();
        }
        let loads = b.store().view_loads() - start;
        let diff = oracle(b.store()).unwrap();
        assert!(diff.is_empty(), "{mode:?}: {diff:#?}");
        (loads, count - 1)
    };
    let (append, measured) = loads(ConflictMode::Append);
    let (full, _) = loads(ConflictMode::Full);
    assert!(measured >= 10, "{measured} slow deliveries");
    assert!(
        full <= append,
        "full mode loaded the view {full} times for {measured} slow commits, append mode {append}"
    );
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
            let paths = r.commit_path_counters();
            let acked = r.receive(bundle(vec![event.clone()]), &allowed).unwrap();
            assert_eq!(acked, vec![event.id.clone()], "{mode:?}");
            let used = counters_since(start);
            let fast = r.commit_path_counters().fast - paths.fast;
            let diff = oracle(r).unwrap();
            assert!(diff.is_empty(), "{mode:?} after {}: {diff:#?}", event.id);
            // WP1.INT: since WP1.6, remote events may commit through a
            // successor (§5.1 remote fast paths), which derives nothing. The
            // event under test (`w3`) must still take the slow path.
            if fast == 1 {
                assert_ne!(event.id, "w3", "{mode:?}: w3 took a fast path");
                assert_eq!(
                    used.full + used.append + used.fallbacks + used.verified,
                    0,
                    "{mode:?} {used:?}"
                );
                continue;
            }
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

/// The current projection can hold a namespace record that no cut of the
/// namespace history derives (TRUE presence compression keeps rank extrema,
/// not a presence chain's head); full mode deletes it when an event takes it
/// out of the projection. Append mode cannot delete, so it must fall back
/// there ("a current namespace record is not derived from history"); rows
/// stay identical in every mode and the oracle stays empty.
///
/// `f` and `y` compete for slot `x`. `f`'s presence chain `e1 -> a5 -> c1`
/// (saves on three devices, concurrent with `f`'s tombstone `b1`) is one
/// compressed group with witnesses `a5` and `e1`; `y` is deleted (`a3`),
/// moved into `x` (`a4`) and saved on a stale base (`d1`, after `a5` and
/// `b1`). Both are alive in the whole history, so `f,y` is recorded, but in
/// every cut where `y` is alive in `x`, `f`'s head is `a5`, which loses to
/// `b1`. `c2` moves `y` out of `x`.
#[test]
fn fallback_when_a_current_record_is_not_derived_from_history() {
    let crafted = |id: &str, deps: &[&str], changes: Vec<Change>| Event {
        id: id.into(),
        repo: REPO.into(),
        branch: Branch {
            id: hash(format!("tkfs-v1/{REPO}/main").as_bytes()),
            name: "main".into(),
            shared: true,
        },
        device: format!("dev-{}", &id[..1]),
        created_ms: 1,
        changes,
        dependencies: deps.iter().map(|d| d.to_string()).collect(),
        reviewed: vec![],
        object_sizes: BTreeMap::new(),
    };
    let change = |entity: &str, field: &str, parents: &[&str], value| Change {
        entity: entity.into(),
        field: field.into(),
        parents: parents.iter().map(|p| p.to_string()).collect(),
        value,
    };
    let place = |name: &str| json!({"parent": ROOT, "name": name});
    let events = vec![
        crafted(
            "a1",
            &[],
            vec![
                change("f", "kind", &[], json!("file")),
                change("f", "location", &[], place("x")),
                change("f", "alive", &[], json!(true)),
            ],
        ),
        crafted(
            "a2",
            &["a1"],
            vec![
                change("y", "kind", &[], json!("file")),
                change("y", "location", &[], place("z")),
                change("y", "alive", &[], json!(true)),
            ],
        ),
        crafted(
            "a3",
            &["a2"],
            vec![change("y", "alive", &["a2"], json!(false))],
        ),
        crafted(
            "a4",
            &["a3"],
            vec![change("y", "location", &["a2"], place("x"))],
        ),
        crafted(
            "e1",
            &["a4"],
            vec![change("f", "alive", &["a1"], json!(true))],
        ),
        crafted(
            "a5",
            &["e1"],
            vec![change("f", "alive", &["e1"], json!(true))],
        ),
        crafted(
            "b1",
            &["a4"],
            vec![change("f", "alive", &["a1"], json!(false))],
        ),
        crafted(
            "c1",
            &["a5"],
            vec![change("f", "alive", &["a5"], json!(true))],
        ),
        crafted(
            "d1",
            &["a5", "b1"],
            vec![change("y", "alive", &["a2"], json!(true))],
        ),
        crafted(
            "c2",
            &["c1"],
            vec![change("y", "location", &["a4"], place("w"))],
        ),
    ];
    let allowed: BTreeSet<String> = ["a", "b", "c", "d", "e"]
        .into_iter()
        .map(|d| format!("dev-{d}"))
        .collect();
    let collision = |r: &Store| {
        r.conflicts()
            .unwrap()
            .iter()
            .any(|c| c.field == "namespace" && c.entity == "f,y")
    };
    let mut rows = vec![];
    for mode in [
        ConflictMode::Append,
        ConflictMode::Verify,
        ConflictMode::Full,
    ] {
        let _mode = ModeGuard::new(mode);
        let _checks = ChecksGuard::new();
        let mut receiver = Replica::new();
        let r = receiver.store();
        for event in &events {
            let start = thread_derive_counters();
            let acked = r.receive(bundle(vec![event.clone()]), &allowed).unwrap();
            assert_eq!(
                acked,
                vec![event.id.clone()],
                "{mode:?}: {:?}",
                quarantined(r)
            );
            let used = counters_since(start);
            let diff = oracle(r).unwrap();
            assert!(diff.is_empty(), "{mode:?} after {}: {diff:#?}", event.id);
            if event.id == "d1" {
                assert!(collision(r), "{mode:?}: f,y recorded");
            }
            if event.id == "c2" {
                assert!(!collision(r), "{mode:?}: f,y deleted");
                match mode {
                    ConflictMode::Full => assert_eq!(used.full, 1, "{used:?}"),
                    _ => {
                        assert_eq!(used.fallbacks, 1, "{used:?}");
                        assert_eq!(
                            thread_last_fallback(),
                            Some("a current namespace record is not derived from history")
                        );
                    }
                }
            }
        }
        rows.push(conflict_rows(r));
    }
    assert!(rows.windows(2).all(|w| w[0] == w[1]));
}

/// WP5.1 review (round 1): a collision record's entity is its pair joined by
/// a comma, so an entity id with a comma makes it ambiguous (`a,b` with `c`,
/// and `a` with `b,c`, are both `a,b,c`). `check_commit` accepts such ids,
/// so a crafted history can have them. Here `a,b` and `c` collide in `x`
/// (`a2`), `c` moves out (`a3`: the current record must go), back in
/// (`a4`), and `a,b` is deleted (`a5`). Every delivery commits in every mode,
/// with and without the commit checks; after each the oracle is empty, the
/// stored projection is `project`'s, and every configuration stores the
/// same rows.
#[test]
fn entity_ids_with_a_comma_commit_as_the_reference() {
    let branch = hash(format!("tkfs-v1/{REPO}/main").as_bytes());
    let crafted = |id: &str, deps: &[&str], changes: Vec<Change>| Event {
        id: id.into(),
        repo: REPO.into(),
        branch: Branch {
            id: branch.clone(),
            name: "main".into(),
            shared: true,
        },
        device: format!("dev-{}", &id[..1]),
        created_ms: 1,
        changes,
        dependencies: deps.iter().map(|d| d.to_string()).collect(),
        reviewed: vec![],
        object_sizes: BTreeMap::new(),
    };
    let change = |entity: &str, field: &str, parents: &[&str], value| Change {
        entity: entity.into(),
        field: field.into(),
        parents: parents.iter().map(|p| p.to_string()).collect(),
        value,
    };
    let place = |name: &str| json!({"parent": ROOT, "name": name});
    let events = vec![
        crafted(
            "a1",
            &[],
            vec![
                change("a,b", "kind", &[], json!("file")),
                change("a,b", "location", &[], place("x")),
                change("a,b", "alive", &[], json!(true)),
            ],
        ),
        crafted(
            "a2",
            &["a1"],
            vec![
                change("c", "kind", &[], json!("file")),
                change("c", "location", &[], place("x")),
                change("c", "alive", &[], json!(true)),
            ],
        ),
        crafted(
            "a3",
            &["a2"],
            vec![change("c", "location", &["a2"], place("y"))],
        ),
        crafted(
            "a4",
            &["a3"],
            vec![change("c", "location", &["a3"], place("x"))],
        ),
        crafted(
            "a5",
            &["a4"],
            vec![change("a,b", "alive", &["a1"], json!(false))],
        ),
    ];
    let allowed: BTreeSet<String> = ["dev-a".to_string()].into();
    // Whether the current projection records the collision of `a,b` and `c`.
    let collided = |p: &Projection| {
        p.conflicts
            .iter()
            .any(|c| c.field == "namespace" && c.entity == "a,b,c")
    };
    let mut rows = vec![];
    for (mode, checks) in [
        (ConflictMode::Append, true),
        (ConflictMode::Append, false),
        (ConflictMode::Verify, true),
        (ConflictMode::Full, true),
        (ConflictMode::Full, false),
    ] {
        let _mode = ModeGuard::new(mode);
        let previous = set_thread_commit_checks(checks);
        let mut receiver = Replica::new();
        let r = receiver.store();
        for event in &events {
            let acked = r.receive(bundle(vec![event.clone()]), &allowed).unwrap();
            assert_eq!(
                acked,
                vec![event.id.clone()],
                "{mode:?} checks {checks}: {:?}",
                quarantined(r)
            );
            let diff = oracle(r).unwrap();
            assert!(
                diff.is_empty(),
                "{mode:?} checks {checks} after {}: {diff:#?}",
                event.id
            );
            let stored = r.projection(&branch).unwrap();
            assert_eq!(
                stored,
                project(&branch, &r.events().unwrap()).unwrap(),
                "{mode:?} checks {checks} after {}",
                event.id
            );
            let expected = matches!(event.id.as_str(), "a2" | "a4");
            assert_eq!(
                collided(&stored),
                expected,
                "{mode:?} checks {checks} after {}: {:?}",
                event.id,
                stored.conflicts
            );
        }
        set_thread_commit_checks(previous);
        rows.push(conflict_rows(r));
    }
    assert!(rows.windows(2).all(|w| w[0] == w[1]));
}

/// A crafted history for `crafted_histories_with_comma_ids_commit_as_the_reference`:
/// `len` events over directories `d1`..`d3`, `d,0` and files `f1`..`f4`,
/// `f,0`, each changing one to three entities (so several relocations per
/// event, directory moves and cycles), with concurrent and stale parents.
fn comma_history(rng: &mut StdRng, len: usize) -> Vec<Event> {
    const ENTITIES: &[&str] = &["d1", "d2", "d3", "d,0", "f1", "f2", "f3", "f4", "f,0"];
    const PARENTS: &[&str] = &["d1", "d2", "d3", "d,0"];
    let branch = Branch {
        id: hash(format!("tkfs-v1/{REPO}/main").as_bytes()),
        name: "main".into(),
        shared: true,
    };
    let mut versions: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    let mut created: BTreeSet<&str> = BTreeSet::new();
    let mut events: Vec<Event> = vec![];
    for n in 0..len {
        let id = format!("{:04x}-{n:03}", rng.gen_range(0..0x10000));
        let mut chosen = ENTITIES.to_vec();
        chosen.shuffle(rng);
        chosen.truncate(rng.gen_range(1..=3));
        let mut changes = vec![];
        for entity in chosen {
            let kind = if entity.starts_with('d') {
                "directory"
            } else {
                "file"
            };
            let place = |rng: &mut StdRng| {
                let parent = if rng.gen_bool(0.4) {
                    ROOT
                } else {
                    PARENTS[rng.gen_range(0..PARENTS.len())]
                };
                let name = ["a", "A", "b"][rng.gen_range(0..3)];
                json!({"parent": parent, "name": name})
            };
            let mut fields: Vec<(&str, serde_json::Value)> = vec![];
            if created.insert(entity) {
                fields.push(("kind", json!(kind)));
                fields.push(("location", place(rng)));
                fields.push(("alive", json!(true)));
            } else {
                match rng.gen_range(0..3) {
                    0 => fields.push(("location", place(rng))),
                    1 => fields.push(("alive", json!(rng.gen_bool(0.6)))),
                    _ => {
                        fields.push(("location", place(rng)));
                        fields.push(("alive", json!(rng.gen_bool(0.7))));
                    }
                }
            }
            for (field, value) in fields {
                let key = (entity.to_string(), field.to_string());
                let known = versions.get(&key).cloned().unwrap_or_default();
                // The latest version, sometimes an older one, or none
                // (concurrent).
                let parents = if known.is_empty() || rng.gen_bool(0.12) {
                    vec![]
                } else if rng.gen_bool(0.7) {
                    vec![known.last().unwrap().clone()]
                } else {
                    vec![known[rng.gen_range(0..known.len())].clone()]
                };
                changes.push(Change {
                    entity: entity.into(),
                    field: field.into(),
                    parents,
                    value,
                });
                versions.entry(key).or_default().push(id.clone());
            }
        }
        let mut dependencies = vec![];
        if !events.is_empty() && rng.gen_bool(0.5) {
            let back = rng.gen_range(1..=events.len().min(4));
            dependencies.push(events[events.len() - back].id.clone());
        }
        events.push(Event {
            id,
            repo: REPO.into(),
            branch: branch.clone(),
            device: format!("dev-{}", n % 3),
            created_ms: 1000 + rng.gen_range(0..50),
            changes,
            dependencies,
            reviewed: vec![],
            object_sizes: BTreeMap::new(),
        });
    }
    events
}

/// WP5.1 review (round 1): random crafted histories whose entity ids include
/// commas (`comma_history`), received one event at a time:
///
/// - under the commit checks, no commit differs from the reference (no
///   `CONFLICT_MODE_MISMATCH` in the inbox);
/// - without them, the oracle is empty after every delivery (the stored
///   projection is `project`'s and every row a rebuild's);
/// - a full-mode receiver quarantines the same events and stores the same
///   rows.
///
/// `TKFS_DERIVE_COMMA_SEEDS` (default 8).
#[test]
fn crafted_histories_with_comma_ids_commit_as_the_reference() {
    let seeds: u64 = std::env::var("TKFS_DERIVE_COMMA_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    let allowed: BTreeSet<String> = (0..3).map(|d| format!("dev-{d}")).collect();
    let mut acked = 0;
    for seed in 0..seeds {
        let mut rng = StdRng::seed_from_u64(51_000 + seed);
        let events = comma_history(&mut rng, 16);
        let mut outcomes = vec![];
        for (mode, checks) in [
            (ConflictMode::Append, true),
            (ConflictMode::Append, false),
            (ConflictMode::Full, false),
        ] {
            let _mode = ModeGuard::new(mode);
            let previous = set_thread_commit_checks(checks);
            let mut receiver = Replica::new();
            let r = receiver.store();
            for event in &events {
                if let Ok(ids) = r.receive(bundle(vec![event.clone()]), &allowed) {
                    acked += ids.len();
                }
                if !checks {
                    let diff = oracle(r).unwrap();
                    assert!(
                        diff.is_empty(),
                        "seed {seed} {mode:?} after {}: {diff:#?}",
                        event.id
                    );
                }
            }
            let refused = quarantined(r);
            for (id, error) in &refused {
                assert!(
                    !error.contains("CONFLICT_MODE_MISMATCH"),
                    "seed {seed} {mode:?} checks {checks}: {id}: {error}"
                );
            }
            set_thread_commit_checks(previous);
            outcomes.push((refused, conflict_rows(r)));
        }
        assert!(
            outcomes.windows(2).all(|w| w[0] == w[1]),
            "seed {seed}: the receivers differ"
        );
    }
    println!("{acked} events committed");
    assert!(acked > 0);
}

/// The oracle adapter (`verify_derived_state_strict`, CCR-WP1.7-1) starts
/// its rebuild from no conflict rows, so excess derived state is reported: a
/// fabricated register record, an inflated or invented `resolved_by`, a
/// stale namespace record. It also works inside a caller's transaction and
/// leaves it open.
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
        let diff = oracle(a.store()).unwrap();
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
    let diff = oracle(s).unwrap();
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
    let diff = oracle(s).unwrap();
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
    let diff = oracle(s).unwrap();
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
    assert!(oracle(s).unwrap().is_empty());
    assert!(!s.db.is_autocommit());
    s.db.execute_batch("ROLLBACK").unwrap();
    assert!(oracle(s).unwrap().is_empty());
    assert!(s.db.is_autocommit());
}
