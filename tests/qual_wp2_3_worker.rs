//! Qualification scenarios for WP2.3 (background activation, status cell,
//! maintenance, notifier). REVAMP_PLAN §14.6, §4.2, §4.4, §4.5.
//!
//! Created by WP0.5 against the 6298d0b baseline, `#[ignore = "WP2.3"]`
//! (kept ignored: `qualify.ps1` runs ignored scenarios only; WP2.3 makes them
//! pass, release).
//! Events reach the worker through its legacy `published-exchange` op (the path
//! the bridged legacy lanes use in Wave 2). With the background activator the
//! op only stages; the scenarios also check that it never answers
//! `WORKER_BUSY` (WP2.3 acceptance).
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
use tkfs::core::{Event, Store, id, inbox::StagedEvent};

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
    /// A's committed shared inventory: what A's pages advertise as `known`,
    /// as a real legacy lane's pages do (WP2.3: with `known` empty, every
    /// exchange made B re-offer its whole history under the engine lock,
    /// which measured the page builder, not activation).
    known: BTreeSet<String>,
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
    let known = a.shared_event_inventory().unwrap();
    CatchUp {
        state,
        repo: repo.clone(),
        b_device: b.device.clone(),
        a_device: a.device.clone(),
        events,
        known,
        _dir: dir,
    }
}

/// Progress of `deliver`: events in pages the worker accepted, pages it
/// refused (after retrying `BUSY` for 120 s) with the last error, and every
/// `WORKER_BUSY` answer (retried, but counted: WP2.3 requires none while the
/// activator runs).
#[derive(Default)]
struct Delivery {
    delivered: AtomicUsize,
    failed_pages: AtomicUsize,
    busy_replies: AtomicUsize,
    last_error: Mutex<Option<String>>,
}
impl Delivery {
    fn delivered(&self) -> usize {
        self.delivered.load(Ordering::Relaxed)
    }
    fn busy(&self) -> usize {
        self.busy_replies.load(Ordering::Relaxed)
    }
    /// Every page accepted and none answered `WORKER_BUSY`; otherwise the
    /// counts and last error. A refused page means the measured phase had less
    /// (or nothing) to activate.
    fn check(&self, s: &mut Scenario) {
        let failed = self.failed_pages.load(Ordering::Relaxed);
        let last = self.last_error.lock().unwrap().clone();
        s.check(
            failed == 0,
            format!("{failed} pages refused by published-exchange, last: {last:?}"),
        );
        let busy = self.busy();
        s.check(
            busy == 0,
            format!("{busy} published-exchange replies were WORKER_BUSY"),
        );
    }
}

/// One `published-exchange`, retrying `WORKER_BUSY` (counted) for up to 120 s.
fn exchange(state: &Path, payload: &Value, progress: &Delivery) -> anyhow::Result<Value> {
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        match rpc_at(state, payload.clone()) {
            Err(e) if format!("{e:#}").contains("WORKER_BUSY") => {
                progress.busy_replies.fetch_add(1, Ordering::Relaxed);
                if std::time::Instant::now() >= deadline {
                    return Err(e);
                }
                thread::sleep(Duration::from_millis(10));
            }
            other => return other,
        }
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
        let body = legacy_page(&c.repo, &c.a_device, &c.b_device, page, &c.known);
        let reply = exchange(
            state,
            &json!({"op":"published-exchange","route":route,"page":body,"metadata_only":true}),
            progress,
        );
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
        let body = legacy_page(&c.repo, &c.a_device, &c.b_device, &[], &c.known);
        let _ = exchange(
            state,
            &json!({"op":"published-exchange","route":route,"page":body,"metadata_only":true}),
            progress,
        );
        thread::sleep(Duration::from_millis(20));
    }
}

/// Stage `events` on a secondary connection to B's store in pages of 64, the
/// way feed exchanges stage (§6.3, WP2.5: `stage_incoming` on a pooled store,
/// no engine lock), waking the activator after each page (`revalidate` with
/// no ids only pokes it), then wait until nothing is pending.
fn stage(state: &Path, c: &CatchUp, stop: &AtomicBool, progress: &Delivery) {
    let store = Store::open_secondary(state, &c.repo, &c.b_device).unwrap();
    let allowed: BTreeSet<String> = [c.a_device.clone(), c.b_device.clone()].into();
    for page in c.events.chunks(64) {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let staged = store.stage_incoming(
            page.iter()
                .cloned()
                .map(|event| StagedEvent {
                    event,
                    source: Some((c.a_device.clone(), 0)),
                })
                .collect(),
            &allowed,
        );
        match staged {
            Ok(outcome) if outcome.rejected.is_empty() => {
                progress.delivered.fetch_add(page.len(), Ordering::Relaxed);
            }
            other => {
                progress.failed_pages.fetch_add(1, Ordering::Relaxed);
                *progress.last_error.lock().unwrap() = Some(format!("{other:?}"));
            }
        }
        let _ = rpc_at(state, json!({"op":"revalidate","ids":[]}));
    }
    while !stop.load(Ordering::Relaxed) {
        let pending = rpc_at(state, json!({"op":"status"}))
            .ok()
            .and_then(|s| s["incoming_pending"].as_i64());
        if pending == Some(0) {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

/// Filesystem operations on the mount stay fast while 2K remote events
/// activate (floor `fs_p99_during_catchup`). The events are staged the way
/// Wave 2+ deliveries reach the activator, without the engine lock (`stage`):
/// the legacy `published-exchange` builds its whole reply page under the
/// engine lock (O(history) per call, §4.2 "unchanged semantics"; about 40 ms
/// per call at 2K events), so a legacy-fed catch-up measures that page
/// builder rather than activation. That variant is reported, without a
/// floor, by `fs_p99_during_legacy_catchup`.
#[test]
#[ignore = "WP2.3"]
fn fs_p99_during_catchup() {
    fs_scenario("fs_p99_during_catchup", "fs_p99_ms", false);
}

/// The same catch-up fed through the legacy lanes' `published-exchange` (one
/// page of 64 events per call, then empty exchanges until drained). Evidence
/// only (no floor): the legacy op holds the engine for its page build.
#[test]
#[ignore = "WP2.3"]
fn fs_p99_during_legacy_catchup() {
    fs_scenario("fs_p99_during_legacy_catchup", "fs_p99_legacy_ms", true);
}

fn fs_scenario(name: &str, metric: &str, legacy_lane: bool) {
    let s = Scenario::new(FILE, name);
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
        thread::spawn(move || {
            if legacy_lane {
                deliver(&c.state.clone(), &c, &stop, &progress)
            } else {
                stage(&c.state.clone(), &c, &stop, &progress)
            }
        })
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
    // The engine-lock gauges of the worker's heartbeat (longest hold, wait p99).
    let engine = worker
        .rpc(json!({"op":"watch","after_seq":0,"wait_ms":0}))
        .map(|hb| hb["engine"].clone())
        .unwrap_or(Value::Null);
    // Only successful operations are timed: with none (mount not attached,
    // files unreadable) the p99 is NaN and fails its floor; errors fail too.
    let summary = summarize(&samples);
    s.metric_with(
        metric,
        summary.p99,
        "ms",
        json!({"fs": summary.json(), "fs_errors": fs_errors, "delivered": progress.delivered(),
            "busy_replies": progress.busy(), "events": c.events.len(), "engine": engine}),
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
    // One persistent connection, as the orchestrator's pooled worker client
    // calls (a new WebSocket per call would time the handshake).
    let info: tkfs::runtime::Discovery =
        serde_json::from_slice(&fs::read(c.state.join("runtime.json")).unwrap()).unwrap();
    let mut lane = tkfs::runtime::RpcLane::default();
    let mut samples = vec![];
    let mut slowest: Vec<(f64, f64)> = vec![];
    let mut busy = 0;
    let mut missing_counts = 0;
    let mut status_errors = 0usize;
    let mut last_status_error = None;
    // At least one status call, even if the feeder finishes first.
    while samples.len() + status_errors == 0 || (!feeder.is_finished() && !s.over_budget()) {
        let offset = s.started.elapsed().as_secs_f64() * 1000.0;
        let (r, ms) = time_ms(|| lane.call(&info, &id(), json!({"op":"status"})));
        match r {
            Ok(status) => {
                samples.push(ms);
                slowest.push((ms, offset));
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
    // The three slowest replies, `(ms, at_ms)` from the start of the phase.
    slowest.sort_by(|a, b| b.0.total_cmp(&a.0));
    slowest.truncate(3);
    s.metric_with(
        "status_p99_ms",
        summary.p99,
        "ms",
        // Not `status`: that key marks a scenario's status line in QUAL output.
        json!({"status_rpc": summary.json(), "slowest_ms_at_ms": slowest,
            "busy_status_replies": busy,
            "missing_counts": missing_counts, "status_errors": status_errors,
            "delivered": progress.delivered(), "busy_exchange_replies": progress.busy()}),
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
    // The drain is the background activator's: its heartbeat counters moved.
    let mut after = 0;
    let mut activated = 0;
    let moved = wait_until(Duration::from_secs(10), || {
        let Ok(hb) = worker.rpc(json!({"op":"watch","after_seq":after,"wait_ms":2000})) else {
            return false;
        };
        after = hb["seq"].as_u64().unwrap_or(after);
        activated = hb["activation"]["activated"].as_u64().unwrap_or(0);
        activated >= n as u64 && hb["inbox"]["ready"] == 0
    });
    s.metric("heartbeat_activated", activated as f64, "events");
    s.check(
        moved,
        format!("heartbeat activation counters: {activated} of {n}"),
    );
    let _ = worker.stop();
    s.finish();
}

/// WP2.INT's confirmation review (WP2.3 finding 5): the `status` right after
/// a client mutation on a large store answers from the cell; the content
/// refresh, which reloads the projection after the mutation's commit
/// (1.6–2.3 s at 50K events when the caller ran it), runs in the background
/// (`content_pending` until it lands). No floor: `status_after_mutation_ms`
/// is the reply time after each of ten imports, `content_catch_up_ms` how
/// long the content took to show each import.
#[test]
#[ignore = "WP2.INT"]
fn status_after_mutation_large() {
    let mut s = Scenario::new(FILE, "status_after_mutation_large");
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let n = scaled(50_000);
    {
        let mut store = Store::initialize(&state, &id(), &id()).unwrap();
        linear_history(&mut store, n).unwrap();
    }
    let mut worker = WorkerProc::spawn(&state, &[], &[]).unwrap();
    if let Err(e) = worker.wait_ready(Duration::from_secs(300)) {
        s.timeout(format!("not Ready: {}", error_code(&format!("{e:#}"))));
        return s.finish();
    }
    let info: tkfs::runtime::Discovery =
        serde_json::from_slice(&fs::read(state.join("runtime.json")).unwrap()).unwrap();
    let mut lane = tkfs::runtime::RpcLane::default();
    let mut status = || lane.call(&info, &id(), json!({"op":"status"}));
    let objects = |status: &Value| status["content"]["current_objects"].as_u64();
    // The startup's full refresh has run.
    let mut before = None;
    let ready = wait_until(Duration::from_secs(120), || {
        before = status().ok().and_then(|s| objects(&s));
        before.is_some()
    });
    s.check(ready, "the status cell has its content");
    let before = before.unwrap_or(0);
    s.measure_from_now();
    let mut replies = vec![];
    let mut catch_up = vec![];
    let mut pending = 0;
    for i in 0..10u64 {
        let name = format!("mutation-{i:02}.txt");
        let imported = rpc_at(
            &state,
            json!({"op":"import","path":name,"hex":hex::encode(name.as_bytes())}),
        );
        if !s.check(imported.is_ok(), format!("import {i}: {imported:?}")) {
            break;
        }
        let watch = Stopwatch::start();
        let (reply, ms) = time_ms(&mut status);
        replies.push(ms);
        if reply.as_ref().is_ok_and(|r| r["content_pending"] == true) {
            pending += 1;
        }
        // Each import adds one current object.
        let current = before + i + 1;
        let caught_up = wait_until(Duration::from_secs(60), || {
            status()
                .is_ok_and(|r| objects(&r) == Some(current) && r.get("content_pending").is_none())
        });
        catch_up.push(watch.ms());
        if !s.check(caught_up, format!("the content shows import {i}")) {
            break;
        }
    }
    let summary = summarize(&replies);
    s.metric_with(
        "status_after_mutation_ms",
        summary.p99,
        "ms",
        json!({"replies": summary.json(), "content_pending_replies": pending,
            "content_catch_up_ms": summarize(&catch_up).json(), "files": n}),
    );
    let _ = worker.stop();
    s.finish();
}
