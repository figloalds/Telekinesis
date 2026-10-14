//! Qualification scenarios for WP2.4 (off-lock slow path and event cache).
//! REVAMP_PLAN §14.6, §4.4.
//!
//! Created by WP0.5 against the 6298d0b baseline, `#[ignore = "WP2.4"]`;
//! un-ignored and extended by WP2.4 (they stay `#[ignore]`d by the qualify
//! convention: release only).
//!
//! The engine lock is the worker's `Mutex<Engine>`, held by the activator
//! for each slice and for each `commit_prepared`. `lock_hold_slow_event_2k`
//! and `stale_convergence` drive the store API exactly as the background
//! activator does (§4.4: slices of 64 events / 25 ms under the lock, slow
//! events prepared off-lock on a read handle, committed under the lock,
//! `Stale` re-queued with backoff) around a `Mutex<Store>`, with a probe that
//! takes the lock every few ms: the probe's worst wait bounds the hold. They
//! measure this package's part without the worker's activator (WP2.3, built
//! in parallel). The `_worker` variants observe the same from outside a real
//! worker process (a probe issuing the engine-locked `branches` op) and run
//! once the worker activates in the background; until then they skip.
//! `cargo test --release --test qual_wp2_4_offlock -- --ignored --test-threads=1 --nocapture`
mod support;

use serde_json::json;
use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use support::*;
use tkfs::core::{
    Bundle, Event, Store, id,
    inbox::{CommitOutcome, Prepared, SliceBudget, StagedEvent},
};

const FILE: &str = "qual_wp2_4_offlock";

/// B: a replica of A's `history`-event journal with one unseen local edit.
/// Returns B's state, ids, and A's `extra` new events (concurrent with B's edit).
struct Diverged {
    _dir: tempfile::TempDir,
    b_state: PathBuf,
    repo: String,
    a_device: String,
    b_device: String,
    events: Vec<Event>,
}

fn diverged(history: usize, extra: usize) -> Diverged {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = Store::open(&dir.path().join("a"), Some(&repo)).unwrap();
    linear_history(&mut a, history).unwrap();
    let (mut a, mut b) = clone_replica(a, &dir.path().join("b")).unwrap();
    let edit = b.lookup("lin-000000.txt").unwrap();
    b.write_revision(
        &edit.id,
        edit.revisions.get("content").cloned(),
        b"local edit on B",
    )
    .unwrap();
    let known = b.shared_event_inventory().unwrap();
    for i in 0..extra {
        a.create(&format!("concurrent-{i:04}"), "directory")
            .unwrap();
    }
    let mut events: Vec<Event> = a
        .events()
        .unwrap()
        .into_iter()
        .filter(|e| !known.contains(&e.id))
        .collect();
    causal_sort(&mut events);
    let out = Diverged {
        b_state: b.root.clone(),
        repo,
        a_device: a.device.clone(),
        b_device: b.device.clone(),
        events,
        _dir: dir,
    };
    drop(b);
    out
}

/// The worker's engine: the store behind a mutex, as `WorkerCtx` holds it,
/// with the count of filesystem-style waiters (`lock_engine_fs` counts them;
/// the activator yields to them, §4.2).
#[derive(Clone)]
struct Engine {
    store: Arc<Mutex<Store>>,
    waiters: Arc<AtomicUsize>,
}
impl Engine {
    fn new(store: Store) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
            waiters: Arc::new(AtomicUsize::new(0)),
        }
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, Store> {
        self.store.lock().unwrap()
    }
}

/// Lock `engine` as the activator does, run `f`, record how long the lock
/// was held, then yield to waiters before the next hold (§4.2).
fn held<T>(engine: &Engine, holds: &mut Vec<f64>, f: impl FnOnce(&mut Store) -> T) -> T {
    let mut guard = engine.lock();
    let started = Instant::now();
    let value = f(&mut guard);
    holds.push(started.elapsed().as_secs_f64() * 1000.0);
    drop(guard);
    let yielded = Instant::now();
    while engine.waiters.load(Ordering::Acquire) > 0
        && yielded.elapsed() < Duration::from_millis(20)
    {
        thread::yield_now();
    }
    value
}

/// A probe that takes the engine lock every ~2 ms, as a filesystem callback
/// does (counted as a waiter), and records each wait.
struct Probe {
    stop: Arc<AtomicBool>,
    thread: thread::JoinHandle<Vec<f64>>,
}
impl Probe {
    fn start(engine: &Engine) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let (stop, engine) = (stop.clone(), engine.clone());
            thread::spawn(move || {
                let mut waits = vec![];
                while !stop.load(Ordering::Relaxed) {
                    let started = Instant::now();
                    engine.waiters.fetch_add(1, Ordering::AcqRel);
                    let guard = engine.lock();
                    engine.waiters.fetch_sub(1, Ordering::AcqRel);
                    waits.push(started.elapsed().as_secs_f64() * 1000.0);
                    drop(guard);
                    thread::sleep(Duration::from_millis(2));
                }
                waits
            })
        };
        Self { stop, thread }
    }
    fn finish(self) -> Vec<f64> {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.join().unwrap()
    }
}

/// What an activator run did.
#[derive(Debug, Default)]
struct Run {
    holds: Vec<f64>,
    commit_holds: Vec<f64>,
    prepare_ms: Vec<f64>,
    slow: usize,
    stale: usize,
    failed: usize,
}

/// The background activator's loop (§4.4) over `engine` until `done`:
/// slices under the lock, slow events prepared off-lock on `read` and
/// committed under the lock, `Stale` re-queued with a short backoff (the
/// activator's policy is WP2.3's; a stale ticket is not a failure, the next
/// preparation reads a newer state, so 10 ms doubling to 80 ms here), waiting
/// while only retry rows are left. `done` is asked on the read handle.
fn activator(
    engine: &Engine,
    read: &Store,
    deadline: Instant,
    done: impl Fn(&Store) -> bool,
) -> Run {
    let mut run = Run::default();
    let mut attempts: std::collections::BTreeMap<String, u32> = Default::default();
    while Instant::now() < deadline {
        let slice = held(engine, &mut run.holds, |s| {
            s.activate_slice(SliceBudget::ACTIVATOR).unwrap()
        });
        if let Some(ticket) = slice.slow_ticket {
            let started = Instant::now();
            let prepared = Store::prepare_slow(read, &ticket).unwrap();
            run.prepare_ms
                .push(started.elapsed().as_secs_f64() * 1000.0);
            assert!(matches!(&prepared, Prepared::Computed(p) if p.is_computed()));
            let mut holds = vec![];
            let outcome = held(engine, &mut holds, |s| s.commit_prepared(prepared).unwrap());
            run.commit_holds.extend(&holds);
            run.holds.extend(holds);
            match outcome {
                CommitOutcome::Committed { slow, .. } => run.slow += usize::from(slow),
                CommitOutcome::Stale => {
                    run.stale += 1;
                    let n = attempts.entry(ticket.id.clone()).or_default();
                    let delay = (10u64 << (*n).min(3)).min(80);
                    *n += 1;
                    held(engine, &mut run.holds, |s| {
                        s.requeue(&ticket.id, delay).unwrap()
                    });
                }
                CommitOutcome::Failed(error) => {
                    run.failed += 1;
                    panic!("{error:?}");
                }
            }
            continue;
        }
        if slice.more {
            continue;
        }
        // Checked on the read handle: no engine lock.
        if done(read) {
            break;
        }
        // Only retry rows (not yet due) or nothing: wait as the activator does.
        thread::sleep(Duration::from_millis(10));
    }
    run
}

fn max(samples: &[f64]) -> f64 {
    samples.iter().copied().fold(0.0, f64::max)
}

/// The engine lock is held < 50 ms while a slow-path event (two-way, 2K
/// history) activates through the off-lock path.
#[test]
#[ignore = "WP2.4"]
fn lock_hold_slow_event_2k() {
    let mut s = Scenario::new(FILE, "lock_hold_slow_event_2k");
    let d = diverged(scaled(2000), 1);
    let b = Store::open(&d.b_state, Some(&d.repo)).unwrap();
    let read = Store::open_secondary(&b.root, &b.repo, &b.device).unwrap();
    let allowed = BTreeSet::from([d.a_device.clone(), d.b_device.clone()]);
    let target = d.events[0].id.clone();
    // The inline slow commit of the same event, for reference (a copy).
    let inline_ms = {
        let copy = tempfile::tempdir().unwrap();
        b.db.execute(
            "VACUUM INTO ?",
            [copy.path().join("metadata.sqlite").to_str().unwrap()],
        )
        .unwrap();
        std::fs::create_dir_all(copy.path().join("objects")).unwrap();
        let mut copy = Store::open_secondary(copy.path(), &b.repo, &b.device).unwrap();
        let (_, ms) = time_ms(|| {
            copy.receive(
                Bundle {
                    repo: d.repo.clone(),
                    branches: vec![],
                    events: d.events.clone(),
                    objects: Default::default(),
                },
                &allowed,
            )
            .unwrap()
        });
        ms
    };
    let engine = Engine::new(b);
    let probe = Probe::start(&engine);
    thread::sleep(Duration::from_millis(100));
    s.measure_from_now();
    let mut stage_holds = vec![];
    held(&engine, &mut stage_holds, |s| {
        s.stage_incoming(
            d.events
                .iter()
                .cloned()
                .map(|event| StagedEvent {
                    event,
                    source: None,
                })
                .collect(),
            &allowed,
        )
        .unwrap()
    });
    let run = activator(&engine, &read, s.deadline(), |s| {
        s.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM events WHERE id=?)",
            [&target],
            |r| r.get::<_, bool>(0),
        )
        .unwrap()
    });
    thread::sleep(Duration::from_millis(50));
    let waits = probe.finish();
    let summary = summarize(&waits);
    let committed = engine
        .lock()
        .events()
        .unwrap()
        .iter()
        .any(|e| e.id == target);
    s.metric_with(
        "max_blocked_ms",
        summary.max,
        "ms",
        json!({"probe": summary.json(), "max_hold_ms": max(&run.holds),
            "commit_prepared_ms": run.commit_holds, "prepare_ms": run.prepare_ms,
            "stage_ms": stage_holds, "inline_slow_commit_ms": inline_ms,
            "slow_commits": run.slow, "stale": run.stale}),
    );
    s.metric("max_hold_ms", max(&run.holds), "ms");
    s.metric("prepare_ms", max(&run.prepare_ms), "ms");
    s.metric("inline_slow_commit_ms", inline_ms, "ms");
    s.check(run.slow == 1, format!("one slow commit ({run:?})"));
    {
        let mut b = engine.lock();
        let diff = b.verify_derived_state().unwrap();
        s.check(diff.is_empty(), format!("oracle {diff:?}"));
    }
    if !committed {
        s.timeout("slow event not committed in budget");
    }
    s.finish();
}

/// Local commits racing prepared slow events make them `Stale`; they are
/// re-queued and every remote event converges, with the oracle empty. The
/// local writer saves in bursts for the whole run (a commit every 100 ms for
/// 0.5 s, then 1 s idle), racing the 200 slow events (a causal chain, so
/// each waits for the one before). A prepared commit lands once no local
/// commit falls between its read and its commit; preparing is O(history),
/// about 60 ms at 2K. A writer that commits more often than a preparation
/// takes, without pause, starves slow events by design (§4.4: `Stale` is
/// never committed inline; WP2.3 reports `activation.starved_since_ms`).
#[test]
#[ignore = "WP2.4"]
fn stale_convergence() {
    let mut s = Scenario::new(FILE, "stale_convergence");
    let d = diverged(scaled(2000), 200);
    let b = Store::open(&d.b_state, Some(&d.repo)).unwrap();
    let read = Store::open_secondary(&b.root, &b.repo, &b.device).unwrap();
    let allowed = BTreeSet::from([d.a_device.clone(), d.b_device.clone()]);
    let engine = Engine::new(b);
    let probe = Probe::start(&engine);
    let stop = Arc::new(AtomicBool::new(false));
    let local_writes = Arc::new(AtomicUsize::new(0));
    let writer = {
        let (stop, engine, local_writes) = (stop.clone(), engine.clone(), local_writes.clone());
        thread::spawn(move || {
            let mut i = 0;
            let started = Instant::now();
            while !stop.load(Ordering::Relaxed) {
                if !(started.elapsed().as_millis() / 500).is_multiple_of(3) {
                    thread::sleep(Duration::from_millis(20));
                    continue;
                }
                engine.waiters.fetch_add(1, Ordering::AcqRel);
                let mut store = engine.lock();
                engine.waiters.fetch_sub(1, Ordering::AcqRel);
                store.create(&format!("local-{i:04}.txt"), "file").unwrap();
                drop(store);
                local_writes.fetch_add(1, Ordering::Relaxed);
                i += 1;
                thread::sleep(Duration::from_millis(100));
            }
        })
    };
    s.measure_from_now();
    let wanted: BTreeSet<String> = d.events.iter().map(|e| e.id.clone()).collect();
    let mut stage_holds = vec![];
    for page in d.events.chunks(16) {
        held(&engine, &mut stage_holds, |s| {
            s.stage_incoming(
                page.iter()
                    .cloned()
                    .map(|event| StagedEvent {
                        event,
                        source: None,
                    })
                    .collect(),
                &allowed,
            )
            .unwrap()
        });
    }
    let watch = Stopwatch::start();
    let run = activator(&engine, &read, s.deadline(), |s| {
        let ids: BTreeSet<String> = s.events().unwrap().into_iter().map(|e| e.id).collect();
        wanted.is_subset(&ids)
    });
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    let waits = probe.finish();
    let converged = {
        let b = engine.lock();
        let ids: BTreeSet<String> = b.events().unwrap().into_iter().map(|e| e.id).collect();
        wanted.is_subset(&ids)
    };
    s.metric_with(
        "converge_ms",
        s.started.elapsed().as_secs_f64() * 1000.0,
        "ms",
        json!({"after_delivery_ms": watch.ms(), "local_writes": local_writes.load(Ordering::Relaxed),
            "slow_commits": run.slow, "stale": run.stale,
            "max_hold_ms": max(&run.holds), "max_blocked_ms": max(&waits),
            "max_prepare_ms": max(&run.prepare_ms)}),
    );
    s.metric("stale", run.stale as f64, "tickets");
    s.metric("max_blocked_ms", max(&waits), "ms");
    s.check(run.stale > 0, "local commits made prepared commits stale");
    {
        let mut b = engine.lock();
        let diff = b.verify_derived_state().unwrap();
        s.check(diff.is_empty(), format!("oracle {diff:?}"));
    }
    if !converged {
        s.timeout("remote events not all committed");
    }
    s.finish();
}

/// Whether the worker activates in the background (WP2.3's activator; its
/// `status` reply then carries `activation`).
fn activates_in_background(worker: &WorkerProc) -> bool {
    worker
        .rpc(json!({"op":"status"}))
        .is_ok_and(|status| !status["activation"].is_null())
}

/// `lock_hold_slow_event_2k` from outside a real worker: a probe issues an
/// engine-locked op (`branches`) every few ms while the slow-path event is
/// delivered over the legacy lane; its worst latency bounds the hold.
#[test]
#[ignore = "WP2.4"]
fn lock_hold_slow_event_2k_worker() {
    let mut s = Scenario::new(FILE, "lock_hold_slow_event_2k_worker");
    let d = diverged(scaled(2000), 1);
    let mut worker = WorkerProc::spawn(&d.b_state, &[], &[]).unwrap();
    worker.wait_ready(Duration::from_secs(300)).unwrap();
    if !activates_in_background(&worker) {
        s.skip("the worker does not activate in the background in this build (WP2.3)");
        return;
    }
    let stop = Arc::new(AtomicBool::new(false));
    let samples = Arc::new(Mutex::new(Vec::new()));
    let probe = {
        let (stop, samples, state) = (stop.clone(), samples.clone(), d.b_state.clone());
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let (_, ms) = time_ms(|| rpc_at(&state, json!({"op":"branches"})));
                samples.lock().unwrap().push(ms);
                thread::sleep(Duration::from_millis(5));
            }
        })
    };
    thread::sleep(Duration::from_millis(300));
    s.measure_from_now();
    let route = legacy_route(&id(), &d.repo, &d.b_device, &d.a_device);
    let page = legacy_page(
        &d.repo,
        &d.a_device,
        &d.b_device,
        &d.events,
        &BTreeSet::new(),
    );
    let target = d.events[0].id.clone();
    let (reply, exchange_ms) = time_ms(|| {
        retry_busy(s.budget, || {
            worker.rpc(
                json!({"op":"published-exchange","route":route,"page":page,"metadata_only":true}),
            )
        })
    });
    s.note(format!("exchange: {:?}", reply.as_ref().map(|_| "ok")));
    let committed = wait_until(s.remaining(), || {
        worker.rpc(json!({"op":"events"})).is_ok_and(|e| {
            e.as_array()
                .is_some_and(|e| e.iter().any(|e| e["id"] == target))
        })
    });
    thread::sleep(Duration::from_millis(200));
    stop.store(true, Ordering::Relaxed);
    probe.join().unwrap();
    let samples = samples.lock().unwrap().clone();
    let summary = summarize(&samples);
    s.metric_with(
        "max_blocked_ms",
        summary.max,
        "ms",
        json!({"probe": summary.json(), "exchange_ms": exchange_ms}),
    );
    if let Ok(status) = worker.rpc(json!({"op":"status"})) {
        s.note(format!("engine gauges: {}", status["engine"]));
    }
    if !committed {
        s.timeout("slow event not committed in budget");
    }
    s.finish();
}

/// `stale_convergence` from outside a real worker: local imports race the
/// background activator's prepared commits over the legacy lane.
#[test]
#[ignore = "WP2.4"]
fn stale_convergence_worker() {
    let mut s = Scenario::new(FILE, "stale_convergence_worker");
    let d = diverged(scaled(2000), 200);
    let mut worker = WorkerProc::spawn(&d.b_state, &[], &[]).unwrap();
    worker.wait_ready(Duration::from_secs(300)).unwrap();
    if !activates_in_background(&worker) {
        s.skip("the worker does not activate in the background in this build (WP2.3)");
        return;
    }
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (stop, state) = (stop.clone(), d.b_state.clone());
        thread::spawn(move || {
            let mut i = 0;
            while !stop.load(Ordering::Relaxed) {
                let _ = rpc_at(
                    &state,
                    json!({"op":"import","path":format!("local-{i:04}.txt"),"hex":hex::encode(format!("{i}"))}),
                );
                i += 1;
                thread::sleep(Duration::from_millis(50));
            }
            i
        })
    };
    s.measure_from_now();
    let route = legacy_route(&id(), &d.repo, &d.b_device, &d.a_device);
    for page in d.events.chunks(16) {
        if s.over_budget() {
            break;
        }
        let body = legacy_page(&d.repo, &d.a_device, &d.b_device, page, &BTreeSet::new());
        let _ = retry_busy(s.remaining(), || {
            worker.rpc(
                json!({"op":"published-exchange","route":route,"page":body,"metadata_only":true}),
            )
        });
    }
    let wanted: BTreeSet<String> = d.events.iter().map(|e| e.id.clone()).collect();
    let watch = Stopwatch::start();
    let converged = wait_until(s.remaining(), || {
        let empty = legacy_page(&d.repo, &d.a_device, &d.b_device, &[], &BTreeSet::new());
        let _ = worker.rpc(
            json!({"op":"published-exchange","route":route,"page":empty,"metadata_only":true}),
        );
        worker.rpc(json!({"op":"events"})).is_ok_and(|events| {
            let ids: BTreeSet<String> = events
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|e| e["id"].as_str().map(str::to_owned))
                .collect();
            wanted.is_subset(&ids)
        })
    });
    stop.store(true, Ordering::Relaxed);
    let local_writes = writer.join().unwrap();
    s.metric_with(
        "converge_ms",
        s.started.elapsed().as_secs_f64() * 1000.0,
        "ms",
        json!({"after_delivery_ms": watch.ms(), "local_writes": local_writes}),
    );
    if !converged {
        s.timeout("remote events not all committed");
    }
    s.finish();
}
