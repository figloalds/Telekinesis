//! Qualification scenarios for WP5.1 (concurrent-edit activation), REVAMP_PLAN
//! §14.5b. Floors in `tests/qualification-floors.json` (WP5.INT).
//!
//! - (a) `conflict_storm`: two daemons paired both ways through fault proxies,
//!   both sides editing one shared name pool, an operation every 50–400 ms per
//!   side for `TKFS_WP51_MINUTES` (default 120); the replicas converge within
//!   `TKFS_WP51_SETTLE_MINUTES` (default 30) and both oracles are empty.
//! - (b) `fast_two_way_editing`: the same rate with 10 % of the names shared;
//!   the activation backlog has no upward trend over the second half of the
//!   edit phase, and the replicas converge.
//! - (c) `event_cost_5k_1k`: CPU per concurrent namespace event on a store with
//!   5K events and at least 1K open conflicts.
//! - (d) `rebuild_9k_conflicts`: a full rebuild (startup reconstruction and
//!   the oracle) of a store with at least 9K open conflicts.
//!
//! (c) and (d) build their stores with a store-level conflict storm
//! (`storm::Storm`: two replicas editing a small shared name pool, exchanging
//! every few operations). `TKFS_WP51_FIXTURE=<dir>` reuses (or, when absent,
//! saves) the built stores, so one fixture can be measured with several
//! builds; `TKFS_WP51_KEEP_FIXTURE=1` saves it.
//!
//! `cargo test --release --test qual_wp5_1_concurrent -- --ignored --test-threads=1 --nocapture`
mod support;

use serde_json::json;
use std::{
    path::{Path, PathBuf},
    time::Instant,
};
use support::*;
use tkfs::core::{
    Store,
    derive::{DeriveCounters, thread_derive_counters, thread_fallback_reasons},
};

const FILE: &str = "qual_wp5_1_concurrent";

/// The process's CPU time (user + kernel), ms.
fn cpu_ms() -> f64 {
    process_cpu_ms(std::process::id()).unwrap_or(f64::NAN)
}

fn since(start: DeriveCounters) -> serde_json::Value {
    let now = thread_derive_counters();
    json!({
        "append": now.append - start.append,
        "fallbacks": now.fallbacks - start.fallbacks,
        "full": now.full - start.full,
        "verified": now.verified - start.verified,
        "rebuilds": now.rebuilds - start.rebuilds,
        "derive_ms": (now.commit_micros - start.commit_micros) as f64 / 1000.0,
    })
}

mod storm {
    use super::support::*;
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use std::{collections::BTreeSet, path::Path};
    use tkfs::core::{Store, id};

    /// Unresolved conflict rows of `s` (all branches).
    pub fn unresolved(s: &Store) -> i64 {
        s.db.query_row(
            "SELECT count(*) FROM conflicts WHERE resolved_by IS NULL",
            [],
            |r| r.get(0),
        )
        .unwrap()
    }

    pub fn events(s: &Store) -> i64 {
        s.db.query_row("SELECT count(*) FROM events", [], |r| r.get(0))
            .unwrap()
    }

    /// What one editing operation may do.
    #[derive(Clone, Copy, PartialEq, Eq)]
    pub enum Ops {
        /// Saves, creates, renames, deletes and directories (the soak's mix).
        All,
        /// Creates, renames and deletes only.
        Namespace,
    }

    /// Two replicas editing a shared name pool (`names` names in each of
    /// four directories), each exchanging with the other with probability
    /// `sync_p` per round of one operation per side.
    pub struct Storm {
        pub a: Store,
        pub b: Store,
        pub allowed: BTreeSet<String>,
        rng: StdRng,
        names: u32,
        sync_p: f64,
        serial: u64,
    }

    impl Storm {
        pub fn create(root: &Path, seed: u64, names: u32, sync_p: f64) -> Self {
            let repo = id();
            let a = Store::open(&root.join("a"), Some(&repo)).unwrap();
            let b = Store::open(&root.join("b"), Some(&repo)).unwrap();
            Self::with(a, b, seed, names, sync_p)
        }

        pub fn open(root: &Path, seed: u64, names: u32, sync_p: f64) -> Self {
            let a = Store::open_existing(&root.join("a")).unwrap();
            let b = Store::open_existing(&root.join("b")).unwrap();
            Self::with(a, b, seed, names, sync_p)
        }

        fn with(a: Store, b: Store, seed: u64, names: u32, sync_p: f64) -> Self {
            let allowed = devices([&a, &b]);
            Self {
                a,
                b,
                allowed,
                rng: StdRng::seed_from_u64(seed),
                names,
                sync_p,
                serial: 0,
            }
        }

        /// One operation on replica `side` (0: A, 1: B). Refused operations
        /// (a missing name, an occupied target) change nothing.
        pub fn op(&mut self, side: usize, ops: Ops) {
            self.serial += 1;
            let serial = self.serial;
            let names = self.names;
            let rng = &mut self.rng;
            let s = if side == 0 { &mut self.a } else { &mut self.b };
            let dirs = ["", "docs/", "src/", "media/"];
            let dir = dirs[rng.gen_range(0..dirs.len())];
            let name = format!("{dir}f{:03}.dat", rng.gen_range(0..names));
            let op = match ops {
                Ops::All => rng.gen_range(0..100),
                Ops::Namespace => rng.gen_range(65..84),
            };
            let _ = match op {
                0..=64 => (|| -> anyhow::Result<()> {
                    let e = match s.lookup(&name) {
                        Ok(e) => e,
                        Err(_) => s.create(&name, "file")?,
                    };
                    s.write_revision(
                        &e.id,
                        e.revisions.get("content").cloned(),
                        format!("{side} {serial}").as_bytes(),
                    )?;
                    Ok(())
                })(),
                65..=72 => (|| -> anyhow::Result<()> {
                    let target = format!(
                        "{}f{:03}.dat",
                        dirs[rng.gen_range(0..dirs.len())],
                        rng.gen_range(0..names)
                    );
                    let e = s.lookup(&name)?;
                    s.rename(&e.id, &target, false)
                })(),
                73..=80 => (|| -> anyhow::Result<()> {
                    let e = s.lookup(&name)?;
                    s.delete(&e.id)
                })(),
                81..=83 => {
                    let path = dir.trim_end_matches('/');
                    if path.is_empty() {
                        s.create(&name, "file").map(|_| ())
                    } else {
                        s.create(path, "directory").map(|_| ())
                    }
                }
                _ => Ok(()),
            };
        }

        /// Deliver what `from` has that the other lacks; returns the events
        /// delivered.
        pub fn sync(&mut self, from: usize) -> usize {
            let (src, dst) = if from == 0 {
                (&self.a, &mut self.b)
            } else {
                (&self.b, &mut self.a)
            };
            let bundle = missing_bundle(src, dst).unwrap();
            let n = bundle.events.len();
            if n > 0 {
                dst.receive(bundle, &self.allowed).unwrap();
            }
            n
        }

        /// One round: an operation per side, then (with `sync_p`) an exchange.
        pub fn round(&mut self) {
            self.op(0, Ops::All);
            self.op(1, Ops::All);
            if self.rng.gen_bool(self.sync_p) {
                self.sync(0);
                self.sync(1);
            }
        }

        /// Rounds until `done`; `report` every 500 events of A.
        pub fn run_until(&mut self, mut done: impl FnMut(&Storm) -> bool) {
            let mut reported = 0;
            let started = std::time::Instant::now();
            while !done(self) {
                self.round();
                let n = events(&self.a);
                if n >= reported + 500 {
                    reported = n;
                    eprintln!(
                        "STORM events={n} unresolved={} elapsed_s={:.1}",
                        unresolved(&self.a),
                        started.elapsed().as_secs_f64()
                    );
                }
            }
        }

        /// Exchange until both replicas hold the same events.
        pub fn converge(&mut self) {
            for _ in 0..8 {
                let moved = self.sync(0) + self.sync(1);
                if moved == 0 {
                    return;
                }
            }
            panic!("the storm replicas did not converge");
        }
    }
}

/// The fixture directory for `name`: reused when `TKFS_WP51_FIXTURE` names
/// one that holds it, else built by `build` in `work` (and saved there when
/// `TKFS_WP51_FIXTURE` is set). Returns the directory holding `a` and `b`.
fn fixture(name: &str, work: &Path, build: impl FnOnce(&Path)) -> PathBuf {
    let saved = std::env::var("TKFS_WP51_FIXTURE")
        .ok()
        .filter(|v| !v.is_empty())
        .map(|dir| PathBuf::from(dir).join(name));
    let root = work.join(name);
    if let Some(saved) = &saved
        && saved.join("a").join("metadata.sqlite").is_file()
    {
        copy_dir(saved, &root).unwrap();
        return root;
    }
    build(&root);
    if let Some(saved) = &saved {
        copy_dir(&root, saved).unwrap();
    }
    root
}

/// (c) CPU per concurrent namespace event at 5K events and 1K open conflicts.
/// A store-level storm builds the history (both replicas converged), B makes
/// three unseen local edits on the shared pool, A makes 60 namespace
/// operations (creates, renames, deletes) on it, and B receives them one per
/// page: each is concurrent with B's edits, so each takes the slow path.
#[test]
#[ignore = "WP5.1"]
fn event_cost_5k_1k() {
    let mut s = Scenario::with_budget(FILE, "event_cost_5k_1k", budget_or(1800));
    let work = tempfile::tempdir().unwrap();
    let root = fixture("event_cost_5k_1k", work.path(), |root| {
        let mut storm = storm::Storm::create(root, 51, 40, 0.15);
        storm.run_until(|st| storm::events(&st.a) >= 5000 && storm::unresolved(&st.a) >= 1000);
        storm.converge();
    });
    let mut storm = storm::Storm::open(&root, 52, 40, 0.15);
    let (events, unresolved) = (storm::events(&storm.b), storm::unresolved(&storm.b));
    s.check(events >= 5000, format!("{events} events"));
    s.check(unresolved >= 1000, format!("{unresolved} open conflicts"));
    // Three unseen local edits on B (refused operations commit nothing).
    let seen = storm.b.shared_event_inventory().unwrap().len();
    while storm.b.shared_event_inventory().unwrap().len() < seen + 3 {
        storm.op(1, storm::Ops::Namespace);
    }
    let before = storm.a.shared_event_inventory().unwrap();
    while storm.a.shared_bundle(&before).unwrap().events.len() < 60 {
        storm.op(0, storm::Ops::Namespace);
    }
    let mut bundle = storm.a.shared_bundle(&before).unwrap();
    causal_sort(&mut bundle.events);
    let start = thread_derive_counters();
    let slow_before = storm.b.commit_path_counters().slow;
    s.measure_from_now();
    let mut cpu = vec![];
    let mut wall = vec![];
    for event in bundle.events.iter().take(60) {
        let referenced = event.objects();
        let page = tkfs::core::Bundle {
            repo: bundle.repo.clone(),
            branches: vec![],
            events: vec![event.clone()],
            objects: bundle
                .objects
                .iter()
                .filter(|(hash, _)| referenced.contains(*hash))
                .map(|(hash, bytes)| (hash.clone(), bytes.clone()))
                .collect(),
        };
        let (c0, t0) = (cpu_ms(), Instant::now());
        storm.b.receive(page, &storm.allowed).unwrap();
        cpu.push(cpu_ms() - c0);
        wall.push(t0.elapsed().as_secs_f64() * 1000.0);
        if s.over_budget() {
            s.timeout(format!("{} of 60 events in budget", cpu.len()));
            break;
        }
    }
    let slow = storm.b.commit_path_counters().slow - slow_before;
    let summary = summarize(&cpu);
    s.metric_with(
        "cpu_ms_per_event",
        cpu.iter().sum::<f64>() / cpu.len().max(1) as f64,
        "ms",
        json!({"events": events, "open_conflicts": unresolved, "delivered": cpu.len(),
            "slow_commits": slow, "cpu": summary.json(), "wall": summarize(&wall).json(),
            "derivations": since(start), "fallback_reasons": thread_fallback_reasons()}),
    );
    s.metric(
        "wall_ms_per_event",
        wall.iter().sum::<f64>() / wall.len().max(1) as f64,
        "ms",
    );
    s.check(
        slow as usize * 2 >= cpu.len(),
        format!("{slow} of {} slow commits", cpu.len()),
    );
    if std::env::var("TKFS_WP51_PHASES").is_ok() {
        let mut phases: Vec<_> = tkfs::core::derive::thread_phase_micros()
            .into_iter()
            .map(|(k, (calls, micros))| (k, calls, micros / 1000))
            .collect();
        phases.sort_by(|a, b| b.2.cmp(&a.2));
        eprintln!("PHASES {phases:?}");
    }
    let (diff, oracle_ms) = time_ms(|| storm.b.verify_derived_state().unwrap());
    s.metric("oracle_ms", oracle_ms, "ms");
    s.check(diff.is_empty(), format!("oracle empty: {diff:?}"));
    s.finish();
}

/// (d) A full rebuild of a store with at least 9K open conflicts: the
/// startup reconstruction (the snapshot is gone, as after damage or a
/// downgrade) and the oracle (`verify_derived_state`, a rebuild inside a
/// rolled-back transaction).
#[test]
#[ignore = "WP5.1"]
fn rebuild_9k_conflicts() {
    let mut s = Scenario::with_budget(FILE, "rebuild_9k_conflicts", budget_or(3600));
    let work = tempfile::tempdir().unwrap();
    let root = fixture("rebuild_9k_conflicts", work.path(), |root| {
        let mut storm = storm::Storm::create(root, 91, 24, 0.12);
        storm.run_until(|st| storm::unresolved(&st.b) >= 9000);
        storm.converge();
    });
    let b = root.join("b");
    let (events, unresolved) = {
        let store = Store::open_existing(&b).unwrap();
        (storm::events(&store), storm::unresolved(&store))
    };
    s.check(unresolved >= 9000, format!("{unresolved} open conflicts"));
    s.measure_from_now();
    // Startup reconstruction: no startup snapshot.
    rusqlite::Connection::open(b.join("metadata.sqlite"))
        .unwrap()
        .execute_batch("DELETE FROM startup_snapshots")
        .unwrap();
    let start = thread_derive_counters();
    let (store, open_ms) = time_ms(|| Store::open_existing(&b).unwrap());
    let mut store = store;
    s.metric_with(
        "reconstruct_ms",
        open_ms,
        "ms",
        json!({"events": events, "open_conflicts": unresolved, "derivations": since(start)}),
    );
    s.check(
        store.startup_resumes() == 0,
        "the open reconstructed (no snapshot)",
    );
    let (diff, oracle_ms) = time_ms(|| store.verify_derived_state().unwrap());
    s.metric("oracle_ms", oracle_ms, "ms");
    s.check(diff.is_empty(), format!("oracle empty: {diff:?}"));
    s.finish();
}
// ---------------------------------------------------------------------------
// Daemon workloads (a) and (b).

mod heavy {
    use super::support::*;
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use serde_json::{Value, json};
    use std::{
        collections::BTreeSet,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };
    use tkfs::core::Store;

    pub const FILE: &str = "qual_wp5_1_concurrent";

    pub fn env_u64(key: &str, default: u64) -> u64 {
        std::env::var(key)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    /// Edits through one side's worker until `stop` (the soak's editor).
    struct Editor {
        edits: AtomicU64,
        refused: AtomicU64,
        last_error: Mutex<Option<String>>,
    }

    impl Editor {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                edits: AtomicU64::new(0),
                refused: AtomicU64::new(0),
                last_error: Mutex::new(None),
            })
        }

        fn run(
            self: Arc<Self>,
            folder: Value,
            seed: u64,
            side: &'static str,
            shared_percent: u32,
            pause_ms: (u64, u64),
            stop: Arc<AtomicBool>,
        ) {
            let mut rng = StdRng::seed_from_u64(seed);
            let dirs = ["", "docs/", "src/", "media/"];
            while !stop.load(Ordering::Relaxed) {
                let dir = dirs[rng.gen_range(0..dirs.len())];
                let pool = if rng.gen_range(0..100) < shared_percent {
                    "f"
                } else {
                    side
                };
                let name = format!("{dir}{pool}{:03}.dat", rng.gen_range(0..160));
                let op = rng.gen_range(0..100);
                let result = match op {
                    0..=64 => {
                        let size = match rng.gen_range(0..100) {
                            0..=79 => rng.gen_range(16..16 * 1024),
                            80..=97 => rng.gen_range(16 * 1024..512 * 1024),
                            _ => rng.gen_range(1024 * 1024..4 * 1024 * 1024),
                        };
                        let mut bytes = vec![0u8; size];
                        rng.fill(&mut bytes[..]);
                        try_rpc(
                            &folder,
                            json!({"op":"import","path":name,"hex":hex::encode(&bytes)}),
                        )
                    }
                    65..=72 => {
                        let target = format!(
                            "{}{pool}{:03}.dat",
                            dirs[rng.gen_range(0..dirs.len())],
                            rng.gen_range(0..160)
                        );
                        try_rpc(
                            &folder,
                            json!({"op":"rename","path":name,"target":target,"replace":false}),
                        )
                    }
                    73..=80 => try_rpc(&folder, json!({"op":"delete","path":name})),
                    81..=83 => {
                        let path = dir.trim_end_matches('/');
                        if path.is_empty() {
                            continue;
                        }
                        try_rpc(&folder, json!({"op":"mkdir","path":path}))
                    }
                    _ => try_rpc(&folder, json!({"op":"status"})),
                };
                match result {
                    Ok(_) => {
                        self.edits.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(error) => {
                        self.refused.fetch_add(1, Ordering::Relaxed);
                        *self.last_error.lock().unwrap() = Some(describe_error(&error));
                    }
                }
                thread::sleep(Duration::from_millis(
                    rng.gen_range(pause_ms.0..=pause_ms.1),
                ));
            }
        }
    }

    /// One fault phase of both links (the soak's cycle).
    fn phase(proxies: &[&FaultProxy], index: u64, rng: &mut StdRng) -> String {
        for proxy in proxies {
            proxy.clear();
        }
        match index % 9 {
            0 => "healthy".into(),
            1 => {
                let ms = [5, 25, 50, 100][rng.gen_range(0..4)];
                for proxy in proxies {
                    proxy.set_latency(Duration::from_millis(ms));
                }
                format!("latency {ms} ms one way")
            }
            2 => {
                let rate = [256 * 1024, 1024 * 1024, 4 * 1024 * 1024, 20 * 1024 * 1024]
                    [rng.gen_range(0..4)];
                for proxy in proxies {
                    proxy.set_bandwidth(rate);
                    proxy.set_latency(Duration::from_millis(10));
                }
                format!("bandwidth {} KiB/s", rate / 1024)
            }
            3 => {
                let s = rng.gen_range(3..20);
                for proxy in proxies {
                    proxy.stall_for(Duration::from_secs(s));
                }
                format!("stall {s} s")
            }
            4 => {
                for proxy in proxies {
                    proxy.reset_all();
                }
                "reset".into()
            }
            5 => {
                for proxy in proxies {
                    proxy.reset_after(rng.gen_range(64 * 1024..4 * 1024 * 1024));
                    proxy.set_latency(Duration::from_millis(15));
                }
                "reset after some bytes".into()
            }
            6 => {
                for proxy in proxies {
                    proxy.blackhole(true);
                }
                "blackhole".into()
            }
            7 => {
                for proxy in proxies {
                    proxy.refuse(true);
                    proxy.reset_all();
                }
                "refuse new connections".into()
            }
            _ => {
                for proxy in proxies {
                    proxy.stall_after(
                        rng.gen_range(64 * 1024..2 * 1024 * 1024),
                        Duration::from_secs(5),
                    );
                    proxy.set_latency(Duration::from_millis(rng.gen_range(1..60)));
                }
                "stall after some bytes, jittery latency".into()
            }
        }
    }

    fn folder_status(node: &Node, state: &str) -> Value {
        node.raw_call(json!({"op":"status"}))
            .ok()
            .and_then(|status| {
                status["folders"]
                    .as_array()
                    .and_then(|folders| folders.iter().find(|f| f["state"] == state).cloned())
            })
            .unwrap_or(Value::Null)
    }

    fn peers_ready(node: &Node) -> bool {
        node.raw_call(json!({"op":"status"})).is_ok_and(|status| {
            status["peers"].as_array().is_some_and(|peers| {
                !peers.is_empty()
                    && peers.iter().all(|peer| {
                        peer["lanes"].as_array().is_some_and(|lanes| {
                            lanes
                                .iter()
                                .filter(|lane| lane["direction"] == "outbound")
                                .all(|lane| lane["state"] == "ready")
                        })
                    })
            })
        })
    }

    /// Staged rows still to activate (ready, waiting, retry, unclassified).
    fn backlog(status: &Value) -> u64 {
        ["ready", "waiting", "retry", "unclassified"]
            .iter()
            .map(|state| status["activation"]["inbox"][state].as_u64().unwrap_or(0))
            .sum()
    }

    fn brief(status: &Value) -> Value {
        json!({
            "sync": status["sync"]["state"],
            "events": status["journal"]["events"],
            "unacknowledged": status["journal"]["outgoing_unacknowledged"],
            "conflicts": status["journal"]["unresolved_conflicts"],
            "backlog": backlog(status),
            "inbox": status["activation"]["inbox"],
            "activated": status["activation"]["counters"]["activated"],
            "slow": status["activation"]["counters"]["slow"],
            "stale": status["activation"]["counters"]["stale"],
            "complete": status["content"]["complete"],
        })
    }

    fn settled(status: &Value) -> bool {
        status["sync"]["state"] == "up_to_date"
            && status["journal"]["outgoing_unacknowledged"] == 0
            && status["content"]["complete"] == true
            && [
                "ready",
                "waiting",
                "retry",
                "blocked",
                "quarantined",
                "unclassified",
            ]
            .iter()
            .all(|state| status["activation"]["inbox"][state].as_u64().unwrap_or(0) == 0)
    }

    /// The summed CPU time of a folder's worker processes (ms).
    fn worker_cpu(node: &Node, folder: &Value) -> Option<f64> {
        let pids = node.worker_pids(folder);
        if pids.is_empty() {
            return None;
        }
        pids.iter().map(|pid| process_cpu_ms(*pid)).sum()
    }

    /// The least-squares slope of `points` (x, y), per unit of x.
    pub fn slope(points: &[(f64, f64)]) -> f64 {
        let n = points.len() as f64;
        if points.len() < 2 {
            return 0.0;
        }
        let mx = points.iter().map(|p| p.0).sum::<f64>() / n;
        let my = points.iter().map(|p| p.1).sum::<f64>() / n;
        let sxx: f64 = points.iter().map(|p| (p.0 - mx).powi(2)).sum();
        let sxy: f64 = points.iter().map(|p| (p.0 - mx) * (p.1 - my)).sum();
        if sxx == 0.0 { 0.0 } else { sxy / sxx }
    }

    /// One heavy workload: two daemons paired both ways through fault
    /// proxies, both sides editing for `TKFS_WP51_MINUTES` (default 120) at
    /// one operation every 50–400 ms each, `shared_percent` of the
    /// operations on the shared name pool; then a settle of
    /// `TKFS_WP51_SETTLE_MINUTES` (default 30). Per-minute samples of both
    /// workers: journal events, unresolved conflicts, the activation backlog,
    /// worker CPU. `trend`: also require no upward backlog trend over the
    /// second half of the edit phase (the last hour of two).
    pub fn run(name: &str, shared_default: u64, trend: bool) {
        let minutes = env_u64("TKFS_WP51_MINUTES", 120);
        let settle_minutes = env_u64("TKFS_WP51_SETTLE_MINUTES", 30);
        let duration = Duration::from_secs(60 * minutes);
        let settle = Duration::from_secs(60 * settle_minutes);
        let shared_percent = env_u64("TKFS_WP51_SHARED_PERCENT", shared_default).min(100) as u32;
        let pause_min = env_u64("TKFS_WP51_PAUSE_MS_MIN", 50);
        let pause_ms = (
            pause_min,
            env_u64("TKFS_WP51_PAUSE_MS_MAX", 400).max(pause_min),
        );
        let faults = env_u64("TKFS_WP51_FAULTS", 1) != 0;
        let mut s =
            Scenario::with_budget(FILE, name, duration + settle + Duration::from_secs(1800));
        let config = || {
            let config = NodeConfig::default().interval(200).connections(3);
            match std::env::var("TKFS_DERIVE_PHASES") {
                Ok(value) => config.env("TKFS_DERIVE_PHASES", &value),
                Err(_) => config,
            }
        };
        let a = Node::start(config());
        let b = Node::start(config());
        let pa = a.create("Heavy", true);
        for i in 0..20 {
            write_file(&pa, &format!("f{i:03}.dat"), format!("seed {i}").as_bytes());
        }
        let to_a = FaultProxy::start(&a.endpoint());
        let to_b = FaultProxy::start(&b.endpoint());
        b.pair_endpoint(&to_a.endpoint(), &a.code(), Some(3));
        a.pair_endpoint(&to_b.endpoint(), &b.code(), Some(3));
        let pb = b.receive_from(&to_a.endpoint(), &pa);
        let (state_a, state_b) = (state_id(&pa), state_id(&pb));
        b.wait_for("the first sync", Duration::from_secs(300), || {
            has_file(&pb, "f019.dat", b"seed 19")
        });
        s.measure_from_now();

        let stop = Arc::new(AtomicBool::new(false));
        let editors = [Editor::new(), Editor::new()];
        let edit_threads: Vec<_> = [(&pa, 1u64, "a"), (&pb, 2u64, "b")]
            .into_iter()
            .zip(editors.iter())
            .map(|((folder, seed, side), editor)| {
                let (editor, folder, stop) = (editor.clone(), folder.clone(), stop.clone());
                thread::spawn(move || {
                    editor.run(folder, seed, side, shared_percent, pause_ms, stop)
                })
            })
            .collect();

        let mut rng = StdRng::seed_from_u64(7);
        let started = Instant::now();
        let mut phases = Vec::new();
        let mut samples: Vec<Value> = Vec::new();
        let mut next_sample = Instant::now();
        let mut index = 0u64;
        let sample = |samples: &mut Vec<Value>, at_s: u64| {
            let (sa, sb) = (folder_status(&a, &state_a), folder_status(&b, &state_b));
            let record = json!({
                "at_s": at_s,
                "a": brief(&sa),
                "b": brief(&sb),
                "cpu_ms": [worker_cpu(&a, &pa), worker_cpu(&b, &pb)],
            });
            eprintln!("WP51_SAMPLE {record}");
            samples.push(record);
        };
        while started.elapsed() < duration {
            let label = if faults {
                phase(&[&to_a, &to_b], index, &mut rng)
            } else {
                "healthy".into()
            };
            let length = Duration::from_secs(rng.gen_range(20..60));
            phases.push(json!({"at_s": started.elapsed().as_secs(), "phase": label}));
            let until = Instant::now() + length;
            while Instant::now() < until && started.elapsed() < duration {
                if Instant::now() >= next_sample {
                    next_sample += Duration::from_secs(60);
                    sample(&mut samples, started.elapsed().as_secs());
                }
                thread::sleep(Duration::from_millis(500));
            }
            index += 1;
        }
        stop.store(true, Ordering::Relaxed);
        for handle in edit_threads {
            handle.join().unwrap();
        }
        to_a.clear();
        to_b.clear();
        let edited = started.elapsed();
        sample(&mut samples, edited.as_secs());

        // Settle: up to date on both sides, every lane ready, equal feeds.
        let settling = Instant::now();
        let mut next_settle_sample = Instant::now() + Duration::from_secs(60);
        let converged = wait_until(settle, || {
            if Instant::now() >= next_settle_sample {
                next_settle_sample += Duration::from_secs(60);
                eprintln!(
                    "WP51_SETTLE {}",
                    json!({"at_s": settling.elapsed().as_secs(),
                        "a": brief(&folder_status(&a, &state_a)),
                        "b": brief(&folder_status(&b, &state_b))})
                );
            }
            settled(&folder_status(&a, &state_a))
                && settled(&folder_status(&b, &state_b))
                && peers_ready(&a)
                && peers_ready(&b)
                && {
                    let (sa, sb) = (folder_status(&a, &state_a), folder_status(&b, &state_b));
                    sa["journal"]["shared_events"] == sb["journal"]["shared_events"]
                }
        });
        let settle_s = settling.elapsed().as_secs_f64();
        let final_a = folder_status(&a, &state_a);
        let final_b = folder_status(&b, &state_b);
        let cpu_end = [worker_cpu(&a, &pa), worker_cpu(&b, &pb)];
        s.check(
            converged,
            format!(
                "both folders up to date, settled and ready within {settle:?}: A {} / B {}",
                brief(&final_a),
                brief(&final_b)
            ),
        );

        // CPU per activated event and the backlog trend, from the samples.
        let first = samples.first().cloned().unwrap_or(Value::Null);
        let last = samples.last().cloned().unwrap_or(Value::Null);
        let mut cpu_per_event = vec![];
        for (side, i) in [("a", 0), ("b", 1)] {
            let cpu = last["cpu_ms"][i].as_f64().zip(first["cpu_ms"][i].as_f64());
            let events = last[side]["activated"]
                .as_u64()
                .zip(first[side]["activated"].as_u64());
            if let (Some((c1, c0)), Some((e1, e0))) = (cpu, events)
                && e1 > e0
            {
                cpu_per_event.push((c1 - c0) / (e1 - e0) as f64);
            }
        }
        let half = edited.as_secs() / 2;
        let mut trends = vec![];
        let mut peaks = vec![];
        for side in ["a", "b"] {
            let points: Vec<(f64, f64)> = samples
                .iter()
                .filter(|v| v["at_s"].as_u64().is_some_and(|t| t >= half))
                .filter_map(|v| Some((v["at_s"].as_f64()? / 60.0, v[side]["backlog"].as_f64()?)))
                .collect();
            trends.push(slope(&points));
            peaks.push(points.iter().map(|p| p.1).fold(0.0, f64::max));
        }
        s.metric_with(
            "cpu_ms_per_activated_event",
            cpu_per_event.iter().copied().fold(0.0, f64::max),
            "ms",
            json!({"per_side": cpu_per_event}),
        );
        let trend_max = trends.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        s.metric_with(
            "backlog_trend_per_min",
            trend_max,
            "events/min",
            json!({"per_side": trends, "peak_second_half": peaks}),
        );
        if trend {
            // A backlog that keeps up oscillates with the faults; one that
            // falls behind grows steadily (WP4.INT: 140 to 1.5K events over
            // an hour, about 25 events/min).
            s.check(
                trend_max <= 2.0,
                format!("no upward backlog trend over the second half: {trends:?} events/min"),
            );
        }

        // Equal feeds, equal projections, empty oracles: on the stopped stores.
        a.stop_folder_exited(&pa, STOP_EXIT_TIMEOUT).unwrap();
        b.stop_folder_exited(&pb, STOP_EXIT_TIMEOUT).unwrap();
        let mut journals = Vec::new();
        let mut oracle_ms = vec![];
        for (who, folder) in [("A", &pa), ("B", &pb)] {
            let mut store = Store::open_existing(&state_dir(folder)).unwrap();
            let t = Instant::now();
            let diff = store.verify_derived_state().unwrap();
            oracle_ms.push(t.elapsed().as_millis() as u64);
            s.check(diff.is_empty(), format!("{who}'s oracle: {diff:?}"));
            s.check(
                store.inbox_counts().unwrap().total() == 0,
                format!("{who} has staged rows"),
            );
            s.check(
                store.outgoing_unacknowledged().unwrap() == 0,
                format!("{who} has unacknowledged outbox rows"),
            );
            let shared: BTreeSet<String> = store.shared_event_inventory().unwrap();
            let projection = store.projection(&store.active).unwrap();
            journals.push((shared, projection.entries));
        }
        s.check(
            journals[0].0 == journals[1].0,
            format!(
                "equal shared journals: A {} events, B {} events",
                journals[0].0.len(),
                journals[1].0.len()
            ),
        );
        s.check(journals[0].1 == journals[1].1, "equal projections");
        // Profiling aid: the workers' derivation phase dumps.
        for (who, folder) in [("A", &pa), ("B", &pb)] {
            for name in ["worker.log.3", "worker.log.2", "worker.log.1", "worker.log"] {
                if let Ok(text) = std::fs::read_to_string(state_dir(folder).join(name)) {
                    for line in text.lines().filter(|l| l.starts_with("DERIVE_PHASES")) {
                        eprintln!("{who} {line}");
                    }
                }
            }
        }
        let edits: Vec<u64> = editors
            .iter()
            .map(|e| e.edits.load(Ordering::Relaxed))
            .collect();
        s.metric_with(
            "settle_s",
            if converged { settle_s } else { f64::NAN },
            "s",
            json!({
                "edit_minutes": edited.as_secs_f64() / 60.0,
                "shared_percent": shared_percent,
                "pause_ms": [pause_ms.0, pause_ms.1],
                "faults": faults,
                "edits": edits,
                "refused_edits": editors.iter().map(|e| e.refused.load(Ordering::Relaxed)).collect::<Vec<_>>(),
                "last_refusal": editors.iter().map(|e| e.last_error.lock().unwrap().clone()).collect::<Vec<_>>(),
                "shared_events": journals[0].0.len(),
                "entries": journals[0].1.len(),
                "unresolved_conflicts": [final_a["journal"]["unresolved_conflicts"].clone(), final_b["journal"]["unresolved_conflicts"].clone()],
                "worker_cpu_ms_end": cpu_end,
                "oracle_ms": oracle_ms,
                "final": {"a": brief(&final_a), "b": brief(&final_b)},
            }),
        );
        s.metric(
            "oracle_ms",
            oracle_ms.iter().copied().max().unwrap_or(0) as f64,
            "ms",
        );
        s.note(format!(
            "phases: {}",
            serde_json::to_string(&phases).unwrap()
        ));
        s.note(format!(
            "samples: {}",
            serde_json::to_string(&samples).unwrap()
        ));
        s.finish();
    }
}

/// (a) Conflict storm: both sides edit one shared name pool.
#[test]
#[ignore = "WP5.1"]
fn conflict_storm() {
    heavy::run("conflict_storm", 100, false);
}

/// (b) Fast two-way editing: 10 % of the names shared; the activation
/// backlog must not trend upwards.
#[test]
#[ignore = "WP5.1"]
fn fast_two_way_editing() {
    heavy::run("fast_two_way_editing", 10, true);
}

/// Profiling aid (temporary): the fixed costs of a derivation on a store.
#[test]
#[ignore = "WP5.1"]
fn profile_derivation() {
    let Ok(dir) = std::env::var("TKFS_WP51_PROFILE_STORE") else {
        return;
    };
    let store = Store::open_existing(Path::new(&dir)).unwrap();
    let branch = store.active.clone();
    let events: Vec<tkfs::core::Event> = store
        .events()
        .unwrap()
        .into_iter()
        .filter(|e| e.branch.id == branch)
        .collect();
    for _ in 0..3 {
        let before = tkfs::core::derive::thread_phase_micros();
        let (p, ms) = time_ms(|| tkfs::core::derive::project(&branch, &events).unwrap());
        let now = tkfs::core::derive::thread_phase_micros();
        let phases: Vec<_> = now
            .iter()
            .filter(|(k, _)| k.starts_with("pr."))
            .map(|(k, (_, m))| (*k, m - before.get(k).map_or(0, |b| b.1)))
            .collect();
        eprintln!(
            "project {ms:.1} ms: {} entries {} conflicts {phases:?}",
            p.entries.len(),
            p.conflicts.len()
        );
    }
    let p = tkfs::core::derive::project(&branch, &events).unwrap();
    let (_, clone_ms) = time_ms(|| p.clone());
    let (json, json_ms) = time_ms(|| serde_json::to_string(&p.conflicts).unwrap());
    let (_, all_ms) = time_ms(|| serde_json::to_string(&p).unwrap());
    let bases: usize = p.conflicts.iter().map(|c| c.bases.len()).sum();
    eprintln!(
        "clone {clone_ms:.2} ms, conflicts json {json_ms:.2} ms ({} bytes, {bases} bases), projection json {all_ms:.2} ms",
        json.len()
    );
    let last = events
        .iter()
        .max_by_key(|e| e.created_ms)
        .unwrap()
        .id
        .clone();
    let mut ordered: Vec<tkfs::core::Event> =
        events.iter().filter(|e| e.id != last).cloned().collect();
    ordered.push(events.iter().find(|e| e.id == last).unwrap().clone());
    for _ in 0..3 {
        let before = tkfs::core::derive::thread_phase_micros();
        let (out, ms) = time_ms(|| {
            tkfs::core::derive::derive_conflicts(&tkfs::core::derive::DeriveInput {
                branch: &branch,
                projection: &p,
                events: &ordered,
                new_event: Some(&last),
                reviewed: &[],
            })
            .unwrap()
        });
        let now = tkfs::core::derive::thread_phase_micros();
        let mut phases: Vec<_> = now
            .iter()
            .map(|(k, (c, m))| {
                let (c0, m0) = before.get(k).copied().unwrap_or((0, 0));
                (*k, c - c0, (m - m0) / 1000)
            })
            .filter(|p| p.1 > 0)
            .collect();
        phases.sort_by(|a, b| b.2.cmp(&a.2));
        eprintln!(
            "derive {ms:.1} ms ({} rows): {phases:?}",
            out.conflicts().len()
        );
    }
}
