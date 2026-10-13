//! Qualification scenarios for WP2.4 (off-lock slow path and event cache).
//! REVAMP_PLAN §14.6, §4.4.
//!
//! Created by WP0.5 against the 6298d0b baseline, `#[ignore = "WP2.4"]`.
//! The engine-lock hold is observed from outside: a probe issues an
//! engine-locked op (`branches`) every few ms while a slow-path event is
//! delivered; the probe's worst latency bounds the hold.
//! `cargo test --release --test qual_wp2_4_offlock -- --ignored --test-threads=1 --nocapture`
mod support;

use serde_json::json;
use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use support::*;
use tkfs::core::{Event, Store, id};

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

/// The engine lock is held < 50 ms while a slow-path event (two-way, 2K history) activates.
#[test]
#[ignore = "WP2.4"]
fn lock_hold_slow_event_2k() {
    let mut s = Scenario::new(FILE, "lock_hold_slow_event_2k");
    let d = diverged(scaled(2000), 1);
    let mut worker = WorkerProc::spawn(&d.b_state, &[], &[]).unwrap();
    worker.wait_ready(Duration::from_secs(300)).unwrap();
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
        // WP2.3+ heartbeat gauges, when present (§4.5).
        s.note(format!("engine gauges: {}", status["engine"]));
    }
    if !committed {
        s.timeout("slow event not committed in budget");
    }
    s.finish();
}

/// Local commits racing a prepared slow event make it Stale; it is re-queued and
/// converges.
#[test]
#[ignore = "WP2.4"]
fn stale_convergence() {
    let mut s = Scenario::new(FILE, "stale_convergence");
    let d = diverged(scaled(2000), 200);
    let mut worker = WorkerProc::spawn(&d.b_state, &[], &[]).unwrap();
    worker.wait_ready(Duration::from_secs(300)).unwrap();
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
