//! Qualification scenarios for WP2.3 (background activation, status cell,
//! maintenance, notifier). REVAMP_PLAN §14.6, §4.2, §4.4, §4.5.
//!
//! Created by WP0.5 against the 6298d0b baseline, `#[ignore = "WP2.3"]`.
//! Events reach the worker through its legacy `published-exchange` op (the path
//! the bridged legacy lanes use in Wave 2).
//! `cargo test --release --test qual_wp2_3_worker -- --ignored --test-threads=1 --nocapture`
mod support;

use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};
use support::*;
use tkfs::core::{Event, Store, id};

const FILE: &str = "qual_wp2_3_worker";

/// A worker state B (with a small shared base) and `n` remote creates authored
/// by A that B has not seen, in causal order.
struct CatchUp {
    _dir: tempfile::TempDir,
    state: PathBuf,
    repo: String,
    b_device: String,
    a_device: String,
    events: Vec<Event>,
}

fn catch_up(n: usize) -> CatchUp {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let state = dir.path().join("b");
    let mut b = Store::initialize(&state, &repo, &id()).unwrap();
    let mut a = Store::open(&dir.path().join("a"), Some(&repo)).unwrap();
    a.create("base", "directory").unwrap();
    for i in 0..20 {
        let e = a.create(&format!("base/f{i:02}.txt"), "file").unwrap();
        a.write_revision(
            &e.id,
            e.revisions.get("content").cloned(),
            format!("file {i}").as_bytes(),
        )
        .unwrap();
    }
    let allowed = devices([&a, &b]);
    sync_into(&a, &mut b, &allowed).unwrap();
    linear_history_named(&mut a, "catchup", n).unwrap();
    let events = metadata_bundle(&a, &b).unwrap().events;
    CatchUp {
        state,
        repo: repo.clone(),
        b_device: b.device.clone(),
        a_device: a.device.clone(),
        events,
        _dir: dir,
    }
}

/// Progress of `deliver`: events in pages the worker accepted, and pages it
/// refused (after retrying `BUSY` for 120 s) with the last error.
#[derive(Default)]
struct Delivery {
    delivered: AtomicUsize,
    failed_pages: AtomicUsize,
    last_error: Mutex<Option<String>>,
}
impl Delivery {
    fn delivered(&self) -> usize {
        self.delivered.load(Ordering::Relaxed)
    }
    /// Every page accepted; otherwise the failure count and last error. A
    /// refused page means the measured phase had less (or nothing) to activate.
    fn check(&self, s: &mut Scenario) {
        let failed = self.failed_pages.load(Ordering::Relaxed);
        let last = self.last_error.lock().unwrap().clone();
        s.check(
            failed == 0,
            format!("{failed} pages refused by published-exchange, last: {last:?}"),
        );
    }
}

/// Deliver `events` to the worker at `state` in pages of 64 through
/// `published-exchange` (metadata only, bounded activation), then keep
/// exchanging empty pages until nothing is pending. Stops when `stop` is set.
fn deliver(state: &Path, c: &CatchUp, stop: &AtomicBool, progress: &Delivery) {
    let route = legacy_route(&id(), &c.repo, &c.b_device, &c.a_device);
    for page in c.events.chunks(64) {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let body = legacy_page(&c.repo, &c.a_device, &c.b_device, page, &BTreeSet::new());
        let reply = retry_busy(Duration::from_secs(120), || {
            rpc_at(
                state,
                json!({"op":"published-exchange","route":route,"page":body,"metadata_only":true}),
            )
        });
        match reply {
            Ok(_) => {
                progress.delivered.fetch_add(page.len(), Ordering::Relaxed);
            }
            Err(e) => {
                progress.failed_pages.fetch_add(1, Ordering::Relaxed);
                *progress.last_error.lock().unwrap() = Some(format!("{e:#}"));
            }
        }
    }
    while !stop.load(Ordering::Relaxed) {
        let pending = rpc_at(state, json!({"op":"status"}))
            .ok()
            .and_then(|s| s["incoming_pending"].as_i64());
        if pending == Some(0) {
            return;
        }
        let body = legacy_page(&c.repo, &c.a_device, &c.b_device, &[], &BTreeSet::new());
        let _ = rpc_at(
            state,
            json!({"op":"published-exchange","route":route,"page":body,"metadata_only":true}),
        );
        thread::sleep(Duration::from_millis(20));
    }
}

/// Filesystem operations on the mount stay fast while 2K remote events activate.
#[test]
#[ignore = "WP2.3"]
fn fs_p99_during_catchup() {
    let s = Scenario::new(FILE, "fs_p99_during_catchup");
    if !mount_supported() {
        return s.skip("no mount support");
    }
    let mut s = s;
    let c = Arc::new(catch_up(scaled(2000)));
    let mount = c.state.parent().unwrap().join("mnt");
    let mount_arg = mount.to_string_lossy().into_owned();
    let mut worker = WorkerProc::spawn(&c.state, &["--mount", &mount_arg], &[]).unwrap();
    worker.wait_ready(Duration::from_secs(120)).unwrap();
    if worker.rpc(json!({"op":"mount-status"})).is_ok() {
        // Supervised builds attach on request; legacy builds attached at start.
        let _ = worker.rpc(json!({"op":"mount-attach","path":mount_arg}));
    }
    let stop = Arc::new(AtomicBool::new(false));
    let progress = Arc::new(Delivery::default());
    s.measure_from_now();
    let feeder = {
        let (stop, progress, c) = (stop.clone(), progress.clone(), c.clone());
        thread::spawn(move || deliver(&c.state.clone(), &c, &stop, &progress))
    };
    let mut samples = vec![];
    let mut fs_errors = 0usize;
    let mut last_fs_error = None;
    // At least one round of FS operations, even if the feeder finishes first.
    while samples.len() + fs_errors == 0 || (!feeder.is_finished() && !s.over_budget()) {
        for i in 0..20 {
            let (r, ms) = time_ms(|| {
                fs::metadata(mount.join(format!("base/f{i:02}.txt")))?;
                fs::read(mount.join(format!("base/f{i:02}.txt")))
            });
            match r {
                Ok(_) => samples.push(ms),
                Err(e) => {
                    fs_errors += 1;
                    last_fs_error = Some(e.to_string());
                }
            }
            thread::sleep(Duration::from_millis(10));
        }
        let (r, ms) = time_ms(|| fs::read_dir(mount.join("base")).map(|d| d.count()));
        match r {
            Ok(_) => samples.push(ms),
            Err(e) => {
                fs_errors += 1;
                last_fs_error = Some(e.to_string());
            }
        }
    }
    let timed_out = !feeder.is_finished();
    stop.store(true, Ordering::Relaxed);
    let _ = feeder.join();
    // Only successful operations are timed: with none (mount not attached,
    // files unreadable) the p99 is NaN and fails its floor; errors fail too.
    let summary = summarize(&samples);
    s.metric_with(
        "fs_p99_ms",
        summary.p99,
        "ms",
        json!({"fs": summary.json(), "fs_errors": fs_errors, "delivered": progress.delivered(),
            "events": c.events.len()}),
    );
    s.check(!samples.is_empty(), "at least one FS operation measured");
    s.check(
        fs_errors == 0,
        format!("{fs_errors} FS operations failed during catch-up, last: {last_fs_error:?}"),
    );
    progress.check(&mut s);
    if timed_out {
        s.timeout("catch-up not finished in budget");
    }
    s.finish();
}

/// The `status` RPC answers fast, with full counts (never `busy`/`stale`),
/// while 2K remote events activate.
#[test]
#[ignore = "WP2.3"]
fn status_latency_during_activation() {
    let mut s = Scenario::new(FILE, "status_latency_during_activation");
    let c = Arc::new(catch_up(scaled(2000)));
    let mut worker = WorkerProc::spawn(&c.state, &[], &[]).unwrap();
    worker.wait_ready(Duration::from_secs(120)).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let progress = Arc::new(Delivery::default());
    s.measure_from_now();
    let feeder = {
        let (stop, progress, c) = (stop.clone(), progress.clone(), c.clone());
        thread::spawn(move || deliver(&c.state.clone(), &c, &stop, &progress))
    };
    let mut samples = vec![];
    let mut busy = 0;
    let mut missing_counts = 0;
    let mut status_errors = 0usize;
    let mut last_status_error = None;
    // At least one status call, even if the feeder finishes first.
    while samples.len() + status_errors == 0 || (!feeder.is_finished() && !s.over_budget()) {
        let (r, ms) = time_ms(|| worker.rpc(json!({"op":"status"})));
        match r {
            Ok(status) => {
                samples.push(ms);
                if status["busy"] == true || status["stale"] == true {
                    busy += 1;
                }
                if status["incoming_pending"].is_null() || status["content"].is_null() {
                    missing_counts += 1;
                }
            }
            Err(e) => {
                status_errors += 1;
                last_status_error = Some(format!("{e:#}"));
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    let timed_out = !feeder.is_finished();
    stop.store(true, Ordering::Relaxed);
    let _ = feeder.join();
    // Only answered calls are timed: with none the p99 is NaN and fails its
    // floor; errors fail too.
    let summary = summarize(&samples);
    s.metric_with(
        "status_p99_ms",
        summary.p99,
        "ms",
        json!({"status": summary.json(), "busy_replies": busy, "missing_counts": missing_counts,
            "status_errors": status_errors, "delivered": progress.delivered()}),
    );
    s.check(!samples.is_empty(), "at least one status reply measured");
    s.check(
        status_errors == 0,
        format!("{status_errors} status calls failed, last: {last_status_error:?}"),
    );
    progress.check(&mut s);
    s.check(busy == 0, format!("{busy} busy/stale status replies"));
    s.check(
        missing_counts == 0,
        format!("{missing_counts} replies without counts"),
    );
    if timed_out {
        s.timeout("activation not finished in budget");
    }
    s.finish();
}

/// Startup with 5K staged events reaches Ready in < 3 s; the backlog drains in
/// the background.
#[test]
#[ignore = "WP2.3"]
fn startup_with_5k_staged() {
    let mut s = Scenario::new(FILE, "startup_with_5k_staged");
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let n = scaled(5000);
    {
        let mut store = Store::initialize(&state, &id(), &id()).unwrap();
        staged_inbox(&mut store, n).unwrap();
    }
    s.measure_from_now();
    let mut worker = WorkerProc::spawn(&state, &[], &[]).unwrap();
    match worker.wait_ready(s.budget) {
        Ok(_) => {
            s.metric(
                "ready_ms",
                worker.spawned_at.elapsed().as_secs_f64() * 1000.0,
                "ms",
            );
        }
        Err(e) => {
            s.metric("ready_ms", f64::NAN, "ms");
            s.timeout(format!("not Ready: {}", error_code(&format!("{e:#}"))));
            return s.finish();
        }
    }
    let watch = Stopwatch::start();
    let pending = |w: &WorkerProc| -> Option<i64> {
        w.rpc(json!({"op":"status"}))
            .ok()
            .and_then(|s: Value| s["incoming_pending"].as_i64())
    };
    let drained = wait_until(s.remaining(), || pending(&worker) == Some(0));
    s.metric("drain_ms", watch.ms(), "ms");
    if !drained {
        s.timeout(format!(
            "backlog not drained: {:?} pending",
            pending(&worker)
        ));
    }
    s.finish();
}
