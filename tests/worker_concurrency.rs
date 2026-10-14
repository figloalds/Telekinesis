//! WP2.3: background activation, the status cell, maintenance and the
//! notifier (REVAMP_PLAN §4.2, §4.4, §4.5, §14 WP2.3).
//!
//! In process: a worker context with its RPC server, heartbeat and maintenance
//! threads (and the activator where a test needs it), driven through the
//! worker's JSON RPC and the engine lock exactly as the filesystem callbacks
//! take it. One test runs the real worker binary (deferred startup
//! activation). Invariant 30's guard is `status_never_blocks`.
mod support;

use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use support::*;
use tkfs::{
    core::{Event, OpenOptions, Store, VerifyMode, id, inbox::StagedEvent},
    runtime::{
        self, Discovery, Engine, RpcServer, Shared,
        ctx::WorkerCtx,
        lock_engine,
        notify::{
            ACTION_MODIFIED, NOTIFY_QUEUE_PATHS, NotifyBatch, NotifyQueue, ROOT_PATH,
            delta_notifications, namespace_notifications,
        },
    },
    worker,
};

/// An in-process worker: context, server and threads. Dropping it stops the
/// threads.
struct Worker {
    ctx: Arc<WorkerCtx>,
    server: RpcServer,
    engine: Shared,
    info: Discovery,
}

impl Worker {
    /// Opens `state` as `worker::run` does (activation deferred) and starts the
    /// RPC server, heartbeat and maintenance; the activator when asked.
    fn start(state: &Path, activator: bool) -> Self {
        let store = Store::open_existing_with(
            state,
            OpenOptions {
                defer_activation: true,
                verify: VerifyMode::Changed,
            },
        )
        .unwrap();
        let engine: Shared = Arc::new(Mutex::new(Engine::new(store)));
        let ctx = worker::build_ctx(engine.clone(), state, false, None);
        let server = runtime::server::start_rpc_server(ctx.clone()).unwrap();
        worker::heartbeat::spawn(ctx.clone(), server.vitals().clone()).unwrap();
        worker::maintenance::spawn(ctx.clone()).unwrap();
        if activator {
            worker::activator::try_spawn(ctx.clone()).unwrap();
        }
        let info = server.publish(None).unwrap();
        Self {
            ctx,
            server,
            engine,
            info,
        }
    }
    fn rpc(&self, payload: Value) -> anyhow::Result<Value> {
        runtime::rpc(&self.info, &id(), payload)
    }
    fn status(&self) -> Value {
        self.rpc(json!({"op":"status"})).unwrap()
    }
    /// Committed shared events, read on a secondary connection.
    fn committed(&self) -> usize {
        self.ctx
            .sync_stores
            .checkout()
            .unwrap()
            .shared_event_inventory()
            .unwrap()
            .len()
    }
    /// Stages `events` on a secondary connection (as the feed RPCs do), then
    /// bumps the status generation and pokes the activator.
    fn stage(&self, events: &[Event], allowed: &BTreeSet<String>) {
        let store = self.ctx.sync_stores.checkout().unwrap();
        let outcome = store
            .stage_incoming(
                events
                    .iter()
                    .cloned()
                    .map(|event| StagedEvent {
                        event,
                        source: None,
                    })
                    .collect(),
                allowed,
            )
            .unwrap();
        assert!(outcome.rejected.is_empty(), "{outcome:?}");
        self.ctx.status_generation.fetch_add(1, Ordering::SeqCst);
        self.ctx.activation.poke();
    }
    fn wait_drained(&self, timeout: Duration) {
        assert!(
            wait_until(timeout, || self.status()["incoming_pending"] == 0),
            "not drained: {}",
            self.status()
        );
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.ctx.stopping.store(true, Ordering::SeqCst);
    }
}

/// A repository shared by an author A and a worker state B (B holds A's base:
/// one directory with `files` files).
struct Pair {
    _dir: tempfile::TempDir,
    a: Store,
    state: PathBuf,
    repo: String,
    b_device: String,
    allowed: BTreeSet<String>,
}

fn pair(files: usize) -> Pair {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let state = dir.path().join("b");
    let mut b = Store::initialize(&state, &repo, &id()).unwrap();
    let mut a = Store::open(&dir.path().join("a"), Some(&repo)).unwrap();
    a.create("base", "directory").unwrap();
    for i in 0..files {
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
    let b_device = b.device.clone();
    Pair {
        _dir: dir,
        a,
        state,
        repo,
        b_device,
        allowed,
    }
}

impl Pair {
    /// A's shared events that `known` lacks, causally sorted.
    fn events_after(&self, known: &BTreeSet<String>) -> Vec<Event> {
        let mut events: Vec<Event> = self
            .a
            .events()
            .unwrap()
            .into_iter()
            .filter(|e| e.branch.shared && !known.contains(&e.id))
            .collect();
        causal_sort(&mut events);
        events
    }
    fn route(&self) -> Value {
        legacy_route(&id(), &self.repo, &self.b_device, &self.a.device)
    }
    fn page(&self, events: &[Event]) -> Value {
        legacy_page(
            &self.repo,
            &self.a.device,
            &self.b_device,
            events,
            &BTreeSet::new(),
        )
    }
}

fn p99(samples: &[f64]) -> f64 {
    summarize(samples).p99
}

/// Invariant 30 (§10): `status` never takes the engine lock. While another
/// thread holds the engine, `status` answers at once with full counts, never
/// the busy fallback, and reads its writes: rows staged on another connection
/// (as the feed RPCs stage) show up as soon as the status generation moved.
#[test]
fn status_never_blocks() {
    let mut p = pair(3);
    linear_history_named(&mut p.a, "held", 5).unwrap();
    let w = Worker::start(&p.state, false);
    let before = w.status();
    assert_eq!(before["incoming_pending"], 0, "{before}");
    let events = p.events_after(
        &w.ctx
            .sync_stores
            .checkout()
            .unwrap()
            .shared_event_inventory()
            .unwrap(),
    );
    assert_eq!(events.len(), 5);
    let held = Arc::new(AtomicBool::new(true));
    let holder = {
        let (engine, held) = (w.engine.clone(), held.clone());
        let (tx, rx) = std::sync::mpsc::channel();
        let holder = thread::spawn(move || {
            let _guard = lock_engine(&engine);
            tx.send(()).unwrap();
            while held.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(5));
            }
        });
        rx.recv().unwrap();
        holder
    };
    let mut samples = vec![];
    for _ in 0..20 {
        let (status, ms) = time_ms(|| w.status());
        samples.push(ms);
        assert!(status.get("busy").is_none(), "{status}");
        assert!(status.get("stale").is_none(), "{status}");
        assert!(status["content"]["complete"].is_boolean(), "{status}");
    }
    // Staged on another connection while the engine is held: read back.
    w.stage(&events, &p.allowed);
    let (status, ms) = time_ms(|| w.status());
    samples.push(ms);
    assert_eq!(status["incoming_pending"], 5, "{status}");
    assert_eq!(status["incoming_active"], 5, "{status}");
    assert_eq!(status["inbox"]["quarantined"], 0, "{status}");
    held.store(false, Ordering::Relaxed);
    holder.join().unwrap();
    let max = samples.iter().cloned().fold(0.0, f64::max);
    assert!(max < 1000.0, "a status reply waited {max} ms");
}

/// The status reply reuses the cell while nothing changed (no content scan per
/// call), and a client mutation is visible in the very next reply, content
/// included (read-your-writes). No maintenance thread here: only the
/// requests refresh the cell.
#[test]
fn status_reuses_the_cell_and_reads_its_writes() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    drop(Store::initialize(&state, &id(), &id()).unwrap());
    let engine: Shared = Arc::new(Mutex::new(Engine::new(
        Store::open_existing(&state).unwrap(),
    )));
    let ctx = worker::build_ctx(engine, &state, false, None);
    let server = runtime::server::start_rpc_server(ctx.clone()).unwrap();
    let info = server.publish(None).unwrap();
    let call = |payload: Value| runtime::rpc(&info, &id(), payload).unwrap();
    let meta = || ctx.status_cell.load()["cell"].clone();
    let first = call(json!({"op":"status"}));
    assert_eq!(first["content"]["current_objects"], 0, "{first}");
    let computed = meta();
    for _ in 0..5 {
        call(json!({"op":"status"}));
    }
    assert_eq!(
        meta()["content_at_ms"],
        computed["content_at_ms"],
        "no content scan while nothing changed"
    );
    assert_eq!(meta()["generation"], computed["generation"]);
    call(json!({"op":"import","path":"one.txt","hex":hex::encode(b"one")}));
    let after = call(json!({"op":"status"}));
    assert_eq!(after["content"]["current_objects"], 1, "{after}");
    assert!(
        after["status_generation"].as_u64().unwrap() > computed["generation"].as_u64().unwrap()
    );
    ctx.stopping.store(true, Ordering::SeqCst);
}

/// While the activator runs, a legacy `published-exchange` only stages (budget
/// 0): nothing is committed or acknowledged by the call; the activator is
/// poked and commits everything in the background.
#[test]
fn published_exchange_stages_only_while_the_activator_runs() {
    let mut p = pair(2);
    linear_history_named(&mut p.a, "staged", 40).unwrap();
    let w = Worker::start(&p.state, false);
    // The activator's flags, before its thread runs.
    lock_engine(&w.engine).background_activation = true;
    w.ctx.activator_running.store(true, Ordering::SeqCst);
    let committed = w.committed();
    let events = p.events_after(
        &w.ctx
            .sync_stores
            .checkout()
            .unwrap()
            .shared_event_inventory()
            .unwrap(),
    );
    assert_eq!(events.len(), 40);
    let pokes = w.ctx.activation.count();
    let reply = w
        .rpc(json!({"op":"published-exchange","route":p.route(),"page":p.page(&events),"metadata_only":true}))
        .unwrap();
    assert_eq!(
        reply["ack"],
        json!([]),
        "staged events are never acknowledged"
    );
    assert!(reply["pending_events"].as_u64().unwrap() > 0, "{reply}");
    assert_eq!(w.committed(), committed, "nothing committed by the call");
    assert!(w.ctx.activation.count() > pokes, "the activator was poked");
    let status = w.status();
    assert_eq!(status["incoming_pending"], 40, "{status}");
    assert_eq!(status["incoming_active"], 40, "{status}");
    worker::activator::try_spawn(w.ctx.clone()).unwrap();
    w.wait_drained(Duration::from_secs(60));
    assert_eq!(w.committed(), committed + 40);
    let status = w.status();
    assert!(
        status["activation"]["activated"].as_u64().unwrap() >= 40,
        "{status}"
    );
    assert_eq!(status["activation"]["running"], true, "{status}");
}

/// Events staged behind the worker's back activate in the background: the
/// inbox drains, the activation counters and `generations.sync` move in the
/// heartbeat, the journal tip follows, and `published-summary` reports the
/// sync generation.
#[test]
fn staged_events_activate_in_the_background_and_move_the_heartbeat() {
    let mut p = pair(2);
    linear_history_named(&mut p.a, "bg", 300).unwrap();
    let w = Worker::start(&p.state, true);
    let first = worker::heartbeat::beat(&w.ctx, w.server.vitals());
    let committed = w.committed();
    let events = p.events_after(
        &w.ctx
            .sync_stores
            .checkout()
            .unwrap()
            .shared_event_inventory()
            .unwrap(),
    );
    assert_eq!(events.len(), 300);
    w.stage(&events, &p.allowed);
    w.wait_drained(Duration::from_secs(120));
    assert_eq!(w.committed(), committed + 300);
    // The heartbeat thread publishes the counters without the engine lock.
    let heartbeat = w
        .rpc(json!({"op":"watch","after_seq":first.seq,"wait_ms":3000}))
        .unwrap();
    assert!(
        wait_until(Duration::from_secs(5), || {
            let hb = worker::heartbeat::beat(&w.ctx, w.server.vitals());
            hb.activation
                .is_some_and(|a| a.activated >= 300 && a.running)
                && hb.inbox.is_some_and(|i| i.pending_active() == 0)
        }),
        "{heartbeat}"
    );
    let hb = worker::heartbeat::beat(&w.ctx, w.server.vitals());
    assert!(hb.generations.sync > first.generations.sync, "{hb:?}");
    let tip = hb.tip.clone().unwrap();
    assert!(tip.seq > first.tip.clone().map_or(0, |t| t.seq), "{hb:?}");
    assert_eq!(hb.journal.as_ref().unwrap().tip, tip.seq);
    let summary = w.rpc(json!({"op":"published-summary"})).unwrap();
    assert!(
        summary["sync_generation"].as_u64().unwrap() > 0,
        "{summary}"
    );
}

/// While 2,000 remote events activate in the background: simulated
/// filesystem operations (three engine acquisitions each, as a lookup, an
/// open and a close; 2 ms apart) stay fast (§14 WP2.3 acceptance: p99
/// < 50 ms in release, < 200 ms in debug; the mounted variant is
/// `qual_wp2_3_worker::fs_p99_during_catchup`), and `status` stays fast with
/// full counts and is never busy. The events are staged on a secondary
/// connection (as feed exchanges stage), so only activation competes with
/// the filesystem for the engine.
#[test]
fn filesystem_and_status_stay_responsive_during_background_activation() {
    let mut p = pair(5);
    linear_history_named(&mut p.a, "load", 2000).unwrap();
    let w = Arc::new(Worker::start(&p.state, true));
    let events = p.events_after(
        &w.ctx
            .sync_stores
            .checkout()
            .unwrap()
            .shared_event_inventory()
            .unwrap(),
    );
    let total = events.len();
    let done = Arc::new(AtomicBool::new(false));
    let fs = {
        let (w, done) = (w.clone(), done.clone());
        thread::spawn(move || {
            let mut samples = vec![];
            while !done.load(Ordering::Relaxed) {
                for i in 0..5 {
                    let path = format!("base/f{i:02}.txt");
                    let (_, ms) = time_ms(|| {
                        let entry = lock_engine(&w.engine).store.lookup(&path).unwrap();
                        let (h, _) = lock_engine(&w.engine).open(&path, None, false).unwrap();
                        lock_engine(&w.engine).close(h).unwrap();
                        entry
                    });
                    samples.push(ms);
                    thread::sleep(Duration::from_millis(2));
                }
            }
            samples
        })
    };
    let status = {
        let (w, done) = (w.clone(), done.clone());
        thread::spawn(move || {
            let mut samples = vec![];
            while !done.load(Ordering::Relaxed) {
                let (status, ms) = time_ms(|| w.status());
                samples.push(ms);
                assert!(status.get("busy").is_none(), "{status}");
                assert!(status.get("stale").is_none(), "{status}");
                assert!(status["incoming_pending"].is_u64(), "{status}");
                assert!(status["content"].is_object(), "{status}");
                thread::sleep(Duration::from_millis(20));
            }
            samples
        })
    };
    w.stage(&events, &p.allowed);
    w.wait_drained(Duration::from_secs(300));
    done.store(true, Ordering::Relaxed);
    let fs = fs.join().unwrap();
    let status = status.join().unwrap();
    assert!(!fs.is_empty() && !status.is_empty());
    let (fs_p99, status_p99) = (p99(&fs), p99(&status));
    eprintln!(
        "{total} events: fs p99 {fs_p99:.1} ms ({} ops), status p99 {status_p99:.1} ms ({} calls)",
        fs.len(),
        status.len()
    );
    // The acceptance's release bound applies whenever the suite runs in
    // release (`cargo test --release`).
    let fs_bound = if cfg!(debug_assertions) { 200.0 } else { 50.0 };
    assert!(
        fs_p99 < fs_bound,
        "filesystem p99 {fs_p99} ms (bound {fs_bound})"
    );
    assert!(status_p99 < 200.0, "status p99 {status_p99} ms");
}

/// With the activator draining a backlog, legacy `published-exchange` calls
/// (empty pages advertising the sender's inventory, as a lane's
/// anti-entropy) never answer `WORKER_BUSY`: time spent behind an activator
/// slice is not charged to the call's 100 ms `try_lock` budget.
#[test]
fn published_exchange_is_never_busy_while_the_activator_runs() {
    let mut p = pair(3);
    linear_history_named(&mut p.a, "busy", 600).unwrap();
    let w = Worker::start(&p.state, true);
    let events = p.events_after(
        &w.ctx
            .sync_stores
            .checkout()
            .unwrap()
            .shared_event_inventory()
            .unwrap(),
    );
    let known = p.a.shared_event_inventory().unwrap();
    let route = p.route();
    w.stage(&events, &p.allowed);
    let mut calls = 0;
    let mut busy = 0;
    let started = Instant::now();
    while w.status()["incoming_pending"] != 0 || calls < 5 {
        let page = legacy_page(&p.repo, &p.a.device, &p.b_device, &[], &known);
        match w
            .rpc(json!({"op":"published-exchange","route":route,"page":page,"metadata_only":true}))
        {
            Ok(_) => {}
            Err(error) if format!("{error:#}").contains("WORKER_BUSY") => busy += 1,
            Err(error) => panic!("published-exchange: {error:#}"),
        }
        calls += 1;
        assert!(
            started.elapsed() < Duration::from_secs(300),
            "not drained: {}",
            w.status()
        );
    }
    assert!(calls >= 5);
    assert_eq!(busy, 0, "{busy} of {calls} exchanges answered WORKER_BUSY");
    assert!(w.status()["activation"]["activated"].as_u64().unwrap() >= 600);
}

/// A verified read handle is not re-referenced (so not re-hashed on its next
/// read) when an activation changes other entities; an activation that
/// changes its content re-references it once (fixes the old receive path,
/// which re-referenced every clean session on every receive).
#[test]
fn verified_read_handle_survives_unrelated_activation() {
    let mut p = pair(2);
    let kept = p.a.create("kept.txt", "file").unwrap();
    let first =
        p.a.write_revision(
            &kept.id,
            kept.revisions.get("content").cloned(),
            b"kept bytes",
        )
        .unwrap();
    {
        let mut b = Store::open_existing(&p.state).unwrap();
        sync_into(&p.a, &mut b, &p.allowed).unwrap();
    }
    let w = Worker::start(&p.state, true);
    let handle = lock_engine(&w.engine)
        .open("kept.txt", None, false)
        .unwrap()
        .0;
    runtime::prepare_content(&w.engine, handle, Duration::from_secs(10)).unwrap();
    let verified = |w: &Worker| {
        let e = lock_engine(&w.engine);
        e.session(handle).unwrap().bytes.reference().is_none()
    };
    assert!(verified(&w), "prepare_content verified the bytes");
    let reloads = w.server.vitals().session_reloads();
    // Unrelated remote changes.
    for i in 0..3 {
        p.a.create(&format!("other-{i}.txt"), "file").unwrap();
    }
    let known = w
        .ctx
        .sync_stores
        .checkout()
        .unwrap()
        .shared_event_inventory()
        .unwrap();
    w.stage(&p.events_after(&known), &p.allowed);
    w.wait_drained(Duration::from_secs(60));
    assert!(lock_engine(&w.engine).store.lookup("other-2.txt").is_ok());
    assert!(
        verified(&w),
        "an unrelated activation kept the verified bytes"
    );
    assert_eq!(w.server.vitals().session_reloads(), reloads);
    // A remote edit of the open file: its session follows (once).
    let entry = p.a.lookup("kept.txt").unwrap();
    let second =
        p.a.write_revision(&entry.id, Some(first.clone()), b"edited remotely")
            .unwrap();
    w.ctx
        .sync_stores
        .checkout()
        .unwrap()
        .put_object(b"edited remotely")
        .unwrap();
    let known = w
        .ctx
        .sync_stores
        .checkout()
        .unwrap()
        .shared_event_inventory()
        .unwrap();
    w.stage(&p.events_after(&known), &p.allowed);
    w.wait_drained(Duration::from_secs(60));
    assert_eq!(w.server.vitals().session_reloads(), reloads + 1);
    let e = lock_engine(&w.engine);
    let session = e.session(handle).unwrap();
    assert_eq!(session.content_base.as_deref(), Some(second.as_str()));
    assert!(
        session.bytes.reference().is_some(),
        "re-referenced: verified on its next read"
    );
}

/// CCR-WP1.6-3 (legacy compatibility kept): `incoming_pending` still counts
/// every staged row and `incoming_quarantined` the rows with an error, in
/// `Store::status` and in the worker `status`; `incoming_active` and
/// `incoming_blocked` carry the state machine's accounting; and the worker
/// `status` `caught_up` no longer counts a quarantined row as pending.
#[test]
fn status_keeps_legacy_counts_and_caught_up_ignores_quarantine() {
    let temp = tempfile::tempdir().unwrap();
    let repo = id();
    let (da, db) = (id(), id());
    let mut a = Engine::new(Store::initialize(&temp.path().join("a"), &repo, &da).unwrap());
    let b_state = temp.path().join("b");
    drop(Store::initialize(&b_state, &repo, &db).unwrap());
    let ga = tkfs::paired_sync::SyncRoute {
        peer: id(),
        repo: repo.clone(),
        local_replica: da.clone(),
        remote_replica: db.clone(),
    };
    let gb = tkfs::paired_sync::SyncRoute {
        peer: id(),
        repo: repo.clone(),
        local_replica: db.clone(),
        remote_replica: da.clone(),
    };
    let w = Worker::start(&b_state, false);
    let file = a.store.create("one.txt", "file").unwrap();
    a.store
        .write_revision(&file.id, file.revisions.get("content").cloned(), b"one")
        .unwrap();
    // A → B and B → A through B's RPC, until both sides are caught up.
    let roundtrip = |a: &mut Engine| {
        let page: tkfs::paired_sync::SyncPage = serde_json::from_value(
            tkfs::paired_sync::worker_control(
                a,
                &json!({"op":"published-offer","route":ga,"metadata_only":true}),
            )
            .unwrap(),
        )
        .unwrap();
        let sent: BTreeSet<_> = page.bundle.events.iter().map(|e| e.id.clone()).collect();
        let response = w
            .rpc(json!({"op":"published-exchange","route":gb,"page":page,"metadata_only":true}))
            .unwrap();
        tkfs::paired_sync::worker_control(
            a,
            &json!({"op":"published-apply","route":ga,"page":response,"request":page.request,"sent":sent,"metadata_only":true}),
        )
        .unwrap();
        let page: tkfs::paired_sync::SyncPage = serde_json::from_value(
            w.rpc(json!({"op":"published-offer","route":gb,"metadata_only":true}))
                .unwrap(),
        )
        .unwrap();
        let sent: BTreeSet<_> = page.bundle.events.iter().map(|e| e.id.clone()).collect();
        let response = tkfs::paired_sync::worker_control(
            a,
            &json!({"op":"published-exchange","route":ga,"page":page,"metadata_only":true}),
        )
        .unwrap();
        w.rpc(json!({"op":"published-apply","route":gb,"page":response,"request":page.request,"sent":sent,"metadata_only":true}))
            .unwrap()
    };
    roundtrip(&mut a);
    // An invalid event authored as A, quarantined on B.
    let branch = a.store.branch(&a.store.active).unwrap();
    let mut bad = a.store.event(&branch, invalid_kind_changes("bad.txt"));
    bad.dependencies.clear();
    let page = legacy_page(&repo, &da, &db, &[bad], &BTreeSet::new());
    w.rpc(json!({"op":"published-exchange","route":gb,"page":page,"metadata_only":true}))
        .unwrap();
    let applied = roundtrip(&mut a);
    assert_eq!(applied["caught_up"], true, "{applied}");
    let status = w.status();
    assert_eq!(status["incoming_pending"], 1, "every staged row: {status}");
    assert_eq!(status["incoming_quarantined"], 1, "{status}");
    assert_eq!(status["incoming_active"], 0, "{status}");
    assert_eq!(status["incoming_blocked"], 0, "{status}");
    assert_eq!(status["inbox"]["quarantined"], 1, "{status}");
    assert!(
        wait_until(Duration::from_secs(10), || w.status()["caught_up"] == true),
        "the worker status counts a quarantined row as pending: {}",
        w.status()
    );
    let store = Store::open_secondary(&b_state, &repo, &db).unwrap();
    let legacy = store.status().unwrap();
    assert_eq!(legacy["incoming_pending"], 1, "{legacy}");
    assert_eq!(legacy["incoming_quarantined"], 1, "{legacy}");
    assert_eq!(legacy["incoming_active"], 0, "{legacy}");
}

/// CCR-WP1.6-4: a bulk-lane `published-apply` (a page with object parts)
/// keeps the `content` member `worker_control` built (inbox counters and the
/// unacknowledged outbox), not the O(repository) `content_status()` scan.
#[test]
fn published_apply_with_parts_keeps_the_inbox_counters() {
    let temp = tempfile::tempdir().unwrap();
    let repo = id();
    let (da, db) = (id(), id());
    let a_state = temp.path().join("a");
    drop(Store::initialize(&a_state, &repo, &da).unwrap());
    let mut b = Engine::new(Store::initialize(&temp.path().join("b"), &repo, &db).unwrap());
    let file = b.store.create("bytes.bin", "file").unwrap();
    b.store
        .write_revision(
            &file.id,
            file.revisions.get("content").cloned(),
            b"object bytes from b",
        )
        .unwrap();
    let ga = json!({"peer":id(),"repo":repo,"local_replica":da,"remote_replica":db});
    let gb = tkfs::paired_sync::SyncRoute {
        peer: id(),
        repo: repo.clone(),
        local_replica: db.clone(),
        remote_replica: da.clone(),
    };
    let w = Worker::start(&a_state, false);
    let mut applied = Value::Null;
    for _ in 0..4 {
        let page: tkfs::paired_sync::SyncPage = serde_json::from_value(
            w.rpc(json!({"op":"published-offer","route":ga,"max_frame":4*1024*1024}))
                .unwrap(),
        )
        .unwrap();
        let sent: BTreeSet<_> = page.bundle.events.iter().map(|e| e.id.clone()).collect();
        let response = tkfs::paired_sync::worker_control(
            &mut b,
            &json!({"op":"published-exchange","route":gb,"page":page,"max_frame":4*1024*1024}),
        )
        .unwrap();
        let parts = response["object_parts"]
            .as_object()
            .is_some_and(|p| !p.is_empty())
            || response["bundle"]["objects"]
                .as_object()
                .is_some_and(|o| !o.is_empty());
        let result = w
            .rpc(json!({"op":"published-apply","route":ga,"page":response,"request":page.request,"sent":sent,"max_frame":4*1024*1024}))
            .unwrap();
        if parts {
            applied = result;
            break;
        }
    }
    assert!(applied.is_object(), "no page carried object parts");
    assert!(applied["content"]["incoming"].is_object(), "{applied}");
    assert!(
        applied["content"]["outgoing_unacknowledged"].is_u64(),
        "{applied}"
    );
    assert!(applied["content"].get("items").is_none(), "{applied}");
    assert!(
        wait_until(
            Duration::from_secs(10),
            || w.status()["content"]["complete"] == true
        ),
        "the object is installed: {}",
        w.status()
    );
}

/// Pending saves are retried with a per-entity backoff (1 s, 2 s, …): a save
/// that keeps failing is delayed alone, the others commit.
#[test]
fn pending_save_retries_back_off_per_entity() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::new(Store::open(dir.path(), Some("wp2-3")).unwrap());
    let mut handles = vec![];
    let mut entities = vec![];
    for name in ["refused.txt", "accepted.txt"] {
        let (h, entry) = engine.open(name, Some("file"), true).unwrap();
        engine.write(h, 0, name.as_bytes(), false, false).unwrap();
        handles.push(h);
        entities.push(entry.id);
    }
    // Both retained, then closed while committing a retained save is refused:
    // two dirty, unreferenced sessions, which is what `retry_pending` commits.
    assert_eq!(engine.retain_all_dirty().unwrap(), 2);
    let refuse = |engine: &Engine, when: &str| {
        engine
            .store
            .db
            .execute_batch(&format!(
                "CREATE TRIGGER wp2_3_refuse BEFORE DELETE ON pending_saves {when}                  BEGIN SELECT RAISE(ABORT, 'refused by test'); END;"
            ))
            .unwrap();
    };
    let allow = |engine: &Engine| {
        engine
            .store
            .db
            .execute_batch("DROP TRIGGER wp2_3_refuse")
            .unwrap();
    };
    refuse(&engine, "");
    for h in handles {
        assert!(engine.close(h).is_err());
    }
    allow(&engine);
    for entity in &entities {
        assert!(engine.sessions[entity].dirty && engine.sessions[entity].references == 0);
    }
    refuse(&engine, &format!("WHEN OLD.entity='{}'", entities[0]));
    let start = Instant::now();
    assert_eq!(
        engine.retry_pending_due(start),
        1,
        "the other entity commits"
    );
    assert!(engine.sessions[&entities[0]].dirty);
    assert!(!engine.sessions.contains_key(&entities[1]));
    // Not due before its backoff (1 s), due after; then 2 s.
    assert_eq!(
        engine.retry_pending_due(start + Duration::from_millis(500)),
        0
    );
    assert_eq!(
        engine.retry_pending_due(start + Duration::from_millis(1100)),
        0
    );
    allow(&engine);
    assert_eq!(
        engine.retry_pending_due(start + Duration::from_millis(2500)),
        0,
        "the second failure backs off 2 s"
    );
    assert_eq!(
        engine.retry_pending_due(start + Duration::from_millis(3200)),
        1
    );
    assert!(!engine.sessions.contains_key(&entities[0]));
    let rows: i64 = engine
        .store
        .db
        .query_row("SELECT count(*) FROM pending_saves", [], |r| r.get(0))
        .unwrap();
    assert_eq!(rows, 0);
}

/// The notifier queue coalesces duplicates, holds at most
/// `NOTIFY_QUEUE_PATHS` distinct pairs and overflows into one root
/// invalidation; what is dropped while detached becomes one root
/// invalidation on the next attach.
#[test]
fn notify_queue_coalesces_and_overflows_into_one_root_invalidation() {
    let queue = NotifyQueue::default();
    queue.push(vec![
        ("\\a".into(), 1),
        ("\\a".into(), 1),
        ("\\b".into(), 3),
    ]);
    assert_eq!(queue.len(), 2);
    assert_eq!(
        queue.take(),
        NotifyBatch {
            items: vec![("\\a".into(), 1), ("\\b".into(), 3)]
        }
    );
    assert!(queue.is_empty());
    queue.push((0..=NOTIFY_QUEUE_PATHS).map(|i| (format!("\\f{i}"), 1)));
    assert_eq!(queue.overflows(), 1);
    assert_eq!(queue.len(), 1, "one root invalidation");
    assert_eq!(
        queue.take().items,
        vec![(ROOT_PATH.to_owned(), ACTION_MODIFIED)]
    );
    queue.push(vec![("\\c".into(), 2)]);
    queue.detached();
    assert_eq!(queue.len(), 1, "a refresh is owed");
    assert_eq!(
        queue.take().items,
        vec![(ROOT_PATH.to_owned(), ACTION_MODIFIED)]
    );
    assert!(queue.take().items.is_empty());
}

/// The notifications computed for the activated entities only equal the full
/// projection diff: creates, a content edit, a delete, and a directory rename
/// that moves its descendants.
#[test]
fn delta_notifications_equal_the_full_projection_diff() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Store::open(dir.path(), Some("wp2-3")).unwrap();
    s.create("d", "directory").unwrap();
    s.create("d/sub", "directory").unwrap();
    s.create("d/sub/deep.txt", "file").unwrap();
    s.create("d/x.txt", "file").unwrap();
    s.create("edit.txt", "file").unwrap();
    s.create("gone.txt", "file").unwrap();
    s.create("still.txt", "file").unwrap();
    let before = s.projection(&s.active).unwrap();
    let known: BTreeSet<String> = s.events().unwrap().into_iter().map(|e| e.id).collect();
    let d = s.lookup("d").unwrap();
    s.rename(&d.id, "renamed", false).unwrap();
    let edit = s.lookup("edit.txt").unwrap();
    s.write_revision(&edit.id, edit.revisions.get("content").cloned(), b"edited")
        .unwrap();
    let gone = s.lookup("gone.txt").unwrap();
    s.delete(&gone.id).unwrap();
    s.create("new.txt", "file").unwrap();
    let mut entities = BTreeSet::new();
    let mut namespace = false;
    for event in s
        .events()
        .unwrap()
        .into_iter()
        .filter(|e| !known.contains(&e.id))
    {
        for change in event.changes {
            namespace |= matches!(change.field.as_str(), "location" | "alive" | "kind");
            entities.insert(change.entity);
        }
    }
    let after = s.projection(&s.active).unwrap();
    let mut full = namespace_notifications(&before, &after);
    let mut delta = delta_notifications(&before, &s, &entities, namespace);
    full.sort();
    delta.sort();
    assert!(!full.is_empty());
    assert_eq!(delta, full);
    assert!(
        full.iter()
            .any(|(path, _)| path == "\\renamed\\sub\\deep.txt"),
        "{full:?}"
    );
}

/// The real worker binary defers startup activation (§4.1 step 3): staged
/// rows are activated in the background after Ready, and the heartbeat's
/// activation counters show it.
#[test]
fn worker_binary_activates_staged_rows_after_ready() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    {
        let mut store = Store::initialize(&state, &id(), &id()).unwrap();
        staged_inbox(&mut store, 200).unwrap();
    }
    let mut worker = WorkerProc::spawn(&state, &[], &[]).unwrap();
    worker.wait_ready(Duration::from_secs(60)).unwrap();
    assert!(
        wait_until(Duration::from_secs(60), || {
            worker
                .rpc(json!({"op":"status"}))
                .is_ok_and(|s| s["incoming_pending"] == 0)
        }),
        "not drained: {}",
        worker.log_tail()
    );
    let mut after = 0;
    let activated = wait_until(Duration::from_secs(10), || {
        let hb = worker
            .rpc(json!({"op":"watch","after_seq":after,"wait_ms":2000}))
            .unwrap();
        after = hb["seq"].as_u64().unwrap_or(0);
        hb["activation"]["activated"].as_u64().unwrap_or(0) >= 200
            && hb["activation"]["running"] == true
            && hb["inbox"]["ready"] == 0
    });
    assert!(activated, "{}", worker.log_tail());
    worker.stop().unwrap();
}

/// Waits until the engine's hold count stops moving for `quiet` (the worker
/// settled after a drain), and returns it.
fn settled_holds(w: &Worker, quiet: Duration) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut holds = w.ctx.gauges.snapshot().holds;
    loop {
        thread::sleep(quiet);
        let now = w.ctx.gauges.snapshot().holds;
        if now == holds || Instant::now() >= deadline {
            return now;
        }
        holds = now;
    }
}

/// An idle worker never takes the engine (review WP2.3 fix round 1): the
/// activator's wakeups (its 1 s timeout, pokes after empty exchanges) first
/// probe a secondary store for ready, unclassified or due rows, and
/// maintenance and the notifier take the engine only for actual work. In
/// particular no `before` projection (an O(entries) clone under the engine
/// lock, captured only while mounted on Windows) is captured on an idle
/// cycle, while a working cycle does capture one.
#[test]
fn an_idle_worker_never_takes_the_engine() {
    let mut p = pair(3);
    linear_history_named(&mut p.a, "idle", 50).unwrap();
    let w = Worker::start(&p.state, true);
    runtime::notify::spawn_notifier(w.ctx.clone()).unwrap();
    let vitals = w.server.vitals().clone();
    // Mounted as far as the engine knows: activations compute notifications.
    lock_engine(&w.engine).mounted = true;
    let events = p.events_after(
        &w.ctx
            .sync_stores
            .checkout()
            .unwrap()
            .shared_event_inventory()
            .unwrap(),
    );
    w.stage(&events, &p.allowed);
    w.wait_drained(Duration::from_secs(60));
    if cfg!(windows) {
        assert!(vitals.captures() > 0, "a working cycle captures `before`");
    }
    let holds = settled_holds(&w, Duration::from_millis(400));
    let captures = vitals.captures();
    // Pokes as empty exchanges send them, two idle timeouts, two
    // maintenance ticks and several notifier rounds.
    for _ in 0..10 {
        w.ctx.activation.poke();
        thread::sleep(Duration::from_millis(250));
    }
    assert_eq!(
        w.ctx.gauges.snapshot().holds,
        holds,
        "an idle worker took the engine"
    );
    assert_eq!(
        vitals.captures(),
        captures,
        "an idle cycle captured `before`"
    );
    // Still working: a new row is activated at once.
    p.a.create("late.txt", "file").unwrap();
    let known = w
        .ctx
        .sync_stores
        .checkout()
        .unwrap()
        .shared_event_inventory()
        .unwrap();
    w.stage(&p.events_after(&known), &p.allowed);
    w.wait_drained(Duration::from_secs(60));
    assert!(lock_engine(&w.engine).store.lookup("late.txt").is_ok());
}

/// With the background activator running, `revalidate` only re-queues and
/// wakes it (review WP2.3 fix round 1): nothing is activated on the request,
/// so a slow event it re-queues is never derived under the request's engine
/// hold (§4.4); the activator then commits the rows.
#[test]
fn revalidate_only_requeues_while_the_activator_runs() {
    let mut p = pair(2);
    linear_history_named(&mut p.a, "requeued", 5).unwrap();
    let w = Worker::start(&p.state, false);
    // The activator's flags, before its thread runs.
    lock_engine(&w.engine).background_activation = true;
    w.ctx.activator_running.store(true, Ordering::SeqCst);
    let committed = w.committed();
    let events = p.events_after(
        &w.ctx
            .sync_stores
            .checkout()
            .unwrap()
            .shared_event_inventory()
            .unwrap(),
    );
    assert_eq!(events.len(), 5);
    w.stage(&events, &p.allowed);
    {
        // Classified only (a slice of no attempts), then the ready row (the
        // head of the linear history; the others wait for it) deferred, as
        // after a transient failure.
        let mut engine = lock_engine(&w.engine);
        let slice = engine
            .store
            .activate_slice(tkfs::core::inbox::SliceBudget {
                max_events: 0,
                max_ms: 0,
                inline_slow: false,
            })
            .unwrap();
        assert!(slice.activated.is_empty());
        for event in &events {
            engine.store.requeue(&event.id, 3_600_000).unwrap();
        }
    }
    let status = w.status();
    assert_eq!(status["inbox"]["retry"], 1, "{status}");
    assert_eq!(status["inbox"]["ready"], 0, "{status}");
    let pokes = w.ctx.activation.count();
    let reply = w.rpc(json!({"op":"revalidate"})).unwrap();
    assert_eq!(reply["requeued"], 1, "{reply}");
    assert_eq!(w.committed(), committed, "nothing activated on the request");
    assert!(w.ctx.activation.count() > pokes, "the activator was poked");
    // Re-queued (back to classification), not activated.
    let status = w.status();
    assert_eq!(status["inbox"]["retry"], 0, "{status}");
    assert_eq!(status["incoming_active"], 5, "{status}");
    worker::activator::try_spawn(w.ctx.clone()).unwrap();
    w.wait_drained(Duration::from_secs(60));
    assert_eq!(w.committed(), committed + 5);
}

/// A slow event whose off-lock preparation fails is re-queued with its own
/// backoff (review WP2.3 fix round 1): the events staged behind it activate
/// meanwhile (no head-of-line stall), and it commits once due.
#[test]
fn a_failed_slow_preparation_requeues_only_its_event() {
    use tkfs::util::fault::{self, FaultAction, FaultConfig};
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let state = dir.path().join("b");
    let mut b = Store::initialize(&state, &repo, &id()).unwrap();
    let mut a = Store::open(&dir.path().join("a"), Some(&repo)).unwrap();
    let mut c = Store::open(&dir.path().join("c"), Some(&repo)).unwrap();
    a.create("shared.txt", "file").unwrap();
    let allowed = devices([&a, &b, &c]);
    sync_into(&a, &mut b, &allowed).unwrap();
    let write = |s: &mut Store, bytes: &[u8]| {
        let entry = s.lookup("shared.txt").unwrap();
        s.write_revision(&entry.id, entry.revisions.get("content").cloned(), bytes)
            .unwrap();
    };
    // A two-way edit: the worker's own edit is committed, the remote one
    // needs the slow path. C's creates follow the worker's edit, so they are
    // fast successors there, independent of A's edit.
    write(&mut b, b"from b");
    sync_into(&b, &mut c, &allowed).unwrap();
    write(&mut a, b"from a");
    for i in 0..5 {
        c.create(&format!("c-{i}.txt"), "file").unwrap();
    }
    let edit = missing_bundle(&a, &b).unwrap();
    assert_eq!(edit.events.len(), 1);
    let creates = metadata_bundle(&c, &b).unwrap().events;
    assert_eq!(creates.len(), 5);
    drop(b);
    let w = Worker::start(&state, false);
    {
        let store = w.ctx.sync_stores.checkout().unwrap();
        for bytes in edit.objects.values() {
            store.put_object(&hex::decode(bytes).unwrap()).unwrap();
        }
    }
    // Every preparation fails until the independent creates are in.
    fault::configure(Some(FaultConfig::new(
        "worker.activator.prepare",
        FaultAction::Fail,
    )));
    // The edit is staged first, so it is popped first.
    let mut staged = edit.events.clone();
    staged.extend(creates.iter().cloned());
    w.stage(&staged, &allowed);
    worker::activator::try_spawn(w.ctx.clone()).unwrap();
    let committed = |event: &str| -> bool {
        w.ctx
            .sync_stores
            .checkout()
            .unwrap()
            .db
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM events WHERE id=?)",
                [event],
                |r| r.get(0),
            )
            .unwrap()
    };
    let created = wait_until(Duration::from_secs(60), || {
        creates.iter().all(|create| committed(&create.id))
    });
    let edit_committed = committed(&edit.events[0].id);
    fault::configure(None);
    assert!(
        created,
        "the events behind the failing preparation waited for it: {}",
        w.status()
    );
    assert!(!edit_committed, "committed while its preparation failed");
    let status = w.status();
    assert!(
        status["activation"]["retried"].as_u64().unwrap() >= 1,
        "{status}"
    );
    // Once preparations succeed again, it commits (after its backoff).
    w.wait_drained(Duration::from_secs(60));
    assert!(committed(&edit.events[0].id));
    let status = w.status();
    assert!(
        status["activation"]["slow"].as_u64().unwrap() >= 1,
        "{status}"
    );
}

/// Writes through the mount move the engine's store revision, not
/// `status_generation` (review WP2.3 fix round 1): the next `status`
/// recomputes the cheap counters, so a file written and closed shows in the
/// journal tip and the outbox at once (never reported acknowledged by an
/// older cell, which made `caught_up` briefly true).
#[test]
fn status_follows_writes_through_the_mount() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    drop(Store::initialize(&state, &id(), &id()).unwrap());
    let engine: Shared = Arc::new(Mutex::new(Engine::new(
        Store::open_existing(&state).unwrap(),
    )));
    let ctx = worker::build_ctx(engine.clone(), &state, false, None);
    let server = runtime::server::start_rpc_server(ctx.clone()).unwrap();
    let info = server.publish(None).unwrap();
    let call = |payload: Value| runtime::rpc(&info, &id(), payload).unwrap();
    let before = call(json!({"op":"status"}));
    let generation = ctx.status_generation.load(Ordering::SeqCst);
    {
        // As the filesystem callbacks do, under the engine lock.
        let mut e = lock_engine(&engine);
        let (h, _) = e.open("written.txt", Some("file"), true).unwrap();
        e.write(h, 0, b"through the mount", false, false).unwrap();
        e.close(h).unwrap();
    }
    assert_eq!(
        ctx.status_generation.load(Ordering::SeqCst),
        generation,
        "no request moved the status generation"
    );
    let after = call(json!({"op":"status"}));
    let tip = |s: &Value| s["tip"]["seq"].as_i64().unwrap();
    assert!(tip(&after) > tip(&before), "{before}\n{after}");
    assert_eq!(after["journal"]["tip"], after["tip"]["seq"], "{after}");
    assert_eq!(after["unflushed_files"], 0, "{after}");
    if after["branch"]["shared"] == true {
        assert!(
            after["outgoing_unacknowledged"].as_u64() > before["outgoing_unacknowledged"].as_u64(),
            "{before}\n{after}"
        );
    }
    ctx.stopping.store(true, Ordering::SeqCst);
}

/// While another connection holds SQLite's writer (a feed exchange staging a
/// page, its fsync on a busy disk), the activator does not wait for it under
/// the engine lock: its slice gives up at once and is retried, so the
/// filesystem keeps getting the engine (review WP2.3 fix round 1). The
/// backlog activates once the writer is released.
#[test]
fn the_activator_never_waits_for_another_writer_under_the_engine_lock() {
    let mut p = pair(2);
    linear_history_named(&mut p.a, "writer", 40).unwrap();
    let w = Worker::start(&p.state, false);
    let committed = w.committed();
    let events = p.events_after(
        &w.ctx
            .sync_stores
            .checkout()
            .unwrap()
            .shared_event_inventory()
            .unwrap(),
    );
    w.stage(&events, &p.allowed);
    let writer = w.ctx.sync_stores.checkout().unwrap();
    writer.db.execute_batch("BEGIN IMMEDIATE").unwrap();
    worker::activator::try_spawn(w.ctx.clone()).unwrap();
    // Engine waits, as a filesystem callback sees them, while the writer is
    // held for 600 ms.
    let held = Instant::now();
    let mut longest = Duration::ZERO;
    while held.elapsed() < Duration::from_millis(600) {
        let asked = Instant::now();
        drop(lock_engine(&w.engine));
        longest = longest.max(asked.elapsed());
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        w.committed(),
        committed,
        "nothing commits behind the writer"
    );
    writer.db.execute_batch("COMMIT").unwrap();
    drop(writer);
    w.wait_drained(Duration::from_secs(60));
    assert_eq!(w.committed(), committed + 40);
    assert!(
        longest < Duration::from_millis(250),
        "the engine was held {longest:?} while another connection held the writer"
    );
}
