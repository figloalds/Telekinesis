//! Qualification scenarios for WP1.12 (interim stabilizers on the legacy lanes
//! and supervisor). REVAMP_PLAN §14.6, §15.
//!
//! Created by WP0.5 against the 6298d0b baseline, `#[ignore = "WP1.12"]`.
//! `cargo test --release --test qual_wp1_12_legacy -- --ignored --test-threads=1 --nocapture`
mod support;

use serde_json::{Value, json};
use std::{
    fs,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use support::*;

const FILE: &str = "qual_wp1_12_legacy";

/// Repository status of `repo` on every lane of the first peer.
fn repo_statuses(peers: &Value, repo: &str) -> Vec<Value> {
    peers[0]["lanes"]
        .as_array()
        .map(|lanes| {
            lanes
                .iter()
                .map(|l| l["repositories"][repo].clone())
                .collect()
        })
        .unwrap_or_default()
}

/// A hung remote worker for repo X is charged to X and backed off; repo Y on
/// the same peer keeps syncing within 45 s.
#[test]
#[ignore = "WP1.12"]
fn hung_remote_repo_charged() {
    let mut s = Scenario::new(FILE, "hung_remote_repo_charged");
    let a = Node::new();
    let b = Node::start(NodeConfig::default().connections(2));
    let px = a.create("Hung X", true);
    let py = a.create("Healthy Y", true);
    write_file(&px, "x.txt", b"x");
    write_file(&py, "y.txt", b"y");
    b.pair(&a, &a.code());
    let bx = b.receive(&a, &px);
    let by = b.receive(&a, &py);
    b.wait_for("initial sync", Duration::from_secs(60), || {
        has_file(&bx, "x.txt", b"x") && has_file(&by, "y.txt", b"y")
    });
    let hung = HungServer::start();
    let redirect = redirect_runtime_json(&state_dir(&px), &hung.endpoint()).unwrap();
    s.measure_from_now();
    write_file(&py, "after-hang.txt", b"healthy progress");
    let x_repo = px["repo"].as_str().unwrap().to_owned();
    let mut charged = false;
    let watch = Stopwatch::start();
    let progressed = wait_until(s.remaining(), || {
        if let Ok(peers) = b.raw_call(json!({"op":"peers"})) {
            charged |= repo_statuses(&peers, &x_repo).iter().any(|r| {
                !r["error"].is_null() || r["consecutive_failures"].as_u64().unwrap_or(0) > 0
            });
        }
        has_file(&by, "after-hang.txt", b"healthy progress")
    });
    let delay = watch.ms();
    s.metric_with(
        "healthy_delay_ms",
        if progressed { delay } else { f64::NAN },
        "ms",
        json!({"hung_requests": hung.received()}),
    );
    if !progressed {
        s.timeout("healthy repository did not progress");
    }
    // Keep sampling briefly so the charge can be observed after Y moved.
    let _ = wait_until(Duration::from_secs(20).min(s.remaining()), || {
        if let Ok(peers) = b.raw_call(json!({"op":"peers"})) {
            charged |= repo_statuses(&peers, &x_repo).iter().any(|r| {
                !r["error"].is_null() || r["consecutive_failures"].as_u64().unwrap_or(0) > 0
            });
        }
        charged
    });
    s.metric("hung_repo_charged", if charged { 1.0 } else { 0.0 }, "bool");
    s.check(charged, "the hung repository's failure is charged to it");
    redirect.restore().unwrap();
    s.finish();
}

/// With `sync_interval_ms = 20000`, idle lanes stay connected: 0 reconnects in 60 s.
#[test]
#[ignore = "WP1.12"]
fn interval_20s_no_reconnects() {
    let mut s = Scenario::new(FILE, "interval_20s_no_reconnects");
    let a = Node::new();
    let b = Node::start(NodeConfig::default().interval(20_000));
    let proxy = FaultProxy::start(&a.endpoint());
    let pa = a.create("Idle source", true);
    b.pair_endpoint(&proxy.endpoint(), &a.code(), Some(3));
    let _pb = b.receive_from(&proxy.endpoint(), &pa);
    b.wait_for("lanes connected", Duration::from_secs(90), || {
        b.raw_call(json!({"op":"peers"})).is_ok_and(|p| {
            p[0]["lanes"]
                .as_array()
                .is_some_and(|l| l.iter().all(|l| l["connected"] == true))
        })
    });
    thread::sleep(Duration::from_secs(5));
    let before = proxy.connections();
    s.measure_from_now();
    let window = Duration::from_secs(60);
    thread::sleep(window);
    let reconnects = proxy.connections() - before;
    s.metric_with(
        "reconnects",
        reconnects as f64,
        "count",
        json!({"window_s": window.as_secs(), "connections_before": before}),
    );
    s.finish();
}

/// `WORKER_BUSY` is Contention: never counted as a failure, never resets `caught_up`.
#[test]
#[ignore = "WP1.12"]
fn worker_busy_not_counted() {
    let mut s = Scenario::new(FILE, "worker_busy_not_counted");
    let a = Node::new();
    let b = Node::new();
    let pa = a.create("Busy source", true);
    write_file(&pa, "seed.txt", b"seed");
    b.pair(&a, &a.code());
    let pb = b.receive(&a, &pa);
    b.wait_for("seed", Duration::from_secs(60), || {
        has_file(&pb, "seed.txt", b"seed")
    });
    let repo = pa["repo"].as_str().unwrap().to_owned();
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let stop = stop.clone();
        let pa = pa.clone();
        thread::spawn(move || {
            let mut i = 0;
            while !stop.load(Ordering::Relaxed) {
                let _ = try_rpc(
                    &pa,
                    json!({"op":"import","path":format!("busy-{i}.bin"),
                        "hex":hex::encode(seeded_bytes(i, 2 * 1024 * 1024))}),
                );
                i += 1;
            }
        })
    };
    s.measure_from_now();
    let mut busy_seen = 0;
    let mut busy_counted = 0;
    let mut busy_retries_max = 0;
    let window = Duration::from_secs(20).min(s.budget);
    let watch = Stopwatch::start();
    while watch.elapsed() < window {
        if let Ok(peers) = b.raw_call(json!({"op":"peers"})) {
            for r in repo_statuses(&peers, &repo) {
                // WP1.12: Contention is counted in `busy_retries` and never
                // reaches `error`; a legacy daemon reports it as the error.
                let busy_retries = r["busy_retries"].as_u64().unwrap_or(0);
                busy_retries_max = busy_retries_max.max(busy_retries);
                let busy_error = r["error"]
                    .as_str()
                    .is_some_and(|e| e.contains("WORKER_BUSY"));
                if busy_error || busy_retries > 0 {
                    busy_seen += 1;
                }
                if busy_error && r["consecutive_failures"].as_u64().unwrap_or(0) > 0 {
                    busy_counted += 1;
                }
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    s.metric("busy_observed", busy_seen as f64, "samples");
    s.metric("busy_counted", busy_counted as f64, "samples");
    s.metric("busy_retries_max", busy_retries_max as f64, "count");
    if busy_seen == 0 {
        s.note("no WORKER_BUSY observed in the window; nothing to judge");
    }
    s.check(busy_counted == 0, "WORKER_BUSY counted as a failure");
    s.finish();
}

/// A worker that exits immediately is restarted with backoff (1, 2, 4… s);
/// `List` stays responsive.
#[test]
#[ignore = "WP1.12"]
fn crash_backoff() {
    let mut s = Scenario::new(FILE, "crash_backoff");
    let a = Node::new();
    let p = a.create("Crasher", true);
    let state = state_id(&p);
    // Delete the store only after the worker has exited.
    a.stop_folder_exited(&p, STOP_EXIT_TIMEOUT).unwrap();
    let dir = state_dir(&p);
    for name in [
        "metadata.sqlite",
        "metadata.sqlite-wal",
        "metadata.sqlite-shm",
    ] {
        let _ = fs::remove_file(dir.join(name));
    }
    let log = dir.join("worker.log");
    let count = || {
        fs::read_to_string(&log)
            .unwrap_or_default()
            .matches("STATE_NOT_INITIALIZED")
            .count()
    };
    let before = count();
    let _ = a.raw_call(json!({"op":"start","state":state}));
    s.measure_from_now();
    let window = Duration::from_secs(20);
    let watch = Stopwatch::start();
    let mut latencies = vec![];
    while watch.elapsed() < window {
        let (r, ms) = time_ms(|| a.raw_call(json!({"op":"list"})));
        s.check(r.is_ok(), "list answers");
        latencies.push(ms);
        thread::sleep(Duration::from_millis(100));
    }
    let restarts = count() - before;
    let summary = summarize(&latencies);
    s.metric("restarts_20s", restarts as f64, "count");
    s.metric_with("list_p99_ms", summary.p99, "ms", summary.json());
    s.check(
        summary.p99 < 1000.0,
        "List responsive during the crash loop",
    );
    s.finish();
}
