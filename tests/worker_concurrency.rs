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

/// Serializes the tests that arm a fault point: the configuration is
/// process-wide, and arming one replaces the other's.
static FAULTS: Mutex<()> = Mutex::new(());

/// Arms `config` until dropped, holding [`FAULTS`].
struct Armed(#[allow(dead_code)] std::sync::MutexGuard<'static, ()>);

impl Armed {
    fn new(config: tkfs::util::fault::FaultConfig) -> Self {
        let guard = FAULTS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        tkfs::util::fault::configure(Some(config));
        Self(guard)
    }
}

impl Drop for Armed {
    fn drop(&mut self) {
        tkfs::util::fault::configure(None);
    }
}

/// An in-process worker: context, server and threads. Dropping it stops the
/// threads.
struct Worker {
    ctx: Arc<WorkerCtx>,
    server: RpcServer,
    engine: Shared,
    info: Discovery,
    /// One persistent connection for the test's calls (as the orchestrator's
    /// pooled client): a new socket per call ran a loaded machine out of
    /// ephemeral ports (WSAENOBUFS) in a full suite.
    lane: Mutex<runtime::RpcLane>,
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
            lane: Mutex::default(),
        }
    }
    fn rpc(&self, payload: Value) -> anyhow::Result<Value> {
        self.lane
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .call(&self.info, &id(), payload)
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
        // A failed test assertion can poison Engine. Stop/wake its actors
        // directly in that case; entering the production poison handler from
        // an unwinding destructor would hide the original failure in an abort.
        let until = Instant::now() + Duration::from_secs(5);
        while !self.engine.is_poisoned() && !self.ctx.stopping.load(Ordering::SeqCst) {
            match self.server.stop(tkfs::model::ipc::StopMode::Force) {
                Ok(_) => return,
                Err(error)
                    if format!("{error:#}").starts_with("WORKER_BUSY")
                        && Instant::now() < until =>
                {
                    thread::sleep(Duration::from_millis(5))
                }
                Err(_) => break,
            }
        }
        self.ctx.stopping.store(true, Ordering::SeqCst);
        if let Ok(address) = self.info.address.trim_start_matches("ws://").parse() {
            let _ = std::net::TcpStream::connect_timeout(&address, Duration::from_secs(1));
        }
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

/// Unrelated activation preserves the current immutable selection and pin;
/// changing the open file replaces that selection exactly once.
#[test]
fn verified_read_handle_survives_unrelated_activation() {
    use tkfs::{
        content::{LocalContent, ReadContext},
        runtime::{content::ContentWait, content_reads},
        util::Cancel,
    };
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
    let native = LocalContent::open(&p.state).unwrap();
    let context = ReadContext {
        deadline: Instant::now() + Duration::from_secs(30),
        cancel: Cancel::new(),
    };
    let reference = native
        .import(&mut b"kept bytes".as_slice(), 10, &context)
        .unwrap();
    assert_eq!(
        Store::open_existing(&p.state)
            .unwrap()
            .lookup("kept.txt")
            .unwrap()
            .content_ref,
        Some(reference)
    );
    let w = Worker::start(&p.state, true);
    let handle = lock_engine(&w.engine)
        .open("kept.txt", None, false)
        .unwrap()
        .0;
    let wait = ContentWait::fixed(Duration::from_secs(10));
    content_reads::prepare_open(&w.engine, handle, wait).unwrap();
    let selection = lock_engine(&w.engine)
        .session(handle)
        .unwrap()
        .bytes
        .content_selection()
        .unwrap();
    assert_eq!(
        content_reads::read_mount_vec(&w.engine, handle, 0, 100, wait)
            .unwrap()
            .unwrap(),
        b"kept bytes"
    );
    let reloads = w.server.vitals().session_reloads();
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
    assert!(Arc::ptr_eq(
        &selection,
        &lock_engine(&w.engine)
            .session(handle)
            .unwrap()
            .bytes
            .content_selection()
            .unwrap()
    ));
    assert_eq!(w.server.vitals().session_reloads(), reloads);
    assert_eq!(
        content_reads::read_mount_vec(&w.engine, handle, 0, 100, wait)
            .unwrap()
            .unwrap(),
        b"kept bytes"
    );
    let entry = p.a.lookup("kept.txt").unwrap();
    let second =
        p.a.write_revision(&entry.id, Some(first), b"edited remotely")
            .unwrap();
    let reference = native
        .import(&mut b"edited remotely".as_slice(), 15, &context)
        .unwrap();
    assert_eq!(p.a.lookup("kept.txt").unwrap().content_ref, Some(reference));
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
    {
        let e = lock_engine(&w.engine);
        let session = e.session(handle).unwrap();
        assert_eq!(session.content_base.as_deref(), Some(second.as_str()));
        assert!(!Arc::ptr_eq(
            &selection,
            &session.bytes.content_selection().unwrap()
        ));
    }
    assert_eq!(
        content_reads::read_mount_vec(&w.engine, handle, 0, 100, wait)
            .unwrap()
            .unwrap(),
        b"edited remotely"
    );
    runtime::engine::close_handle(&w.engine, handle).unwrap();
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
    use tkfs::util::fault::{FaultAction, FaultConfig};
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
    // The point is scoped to this worker's repository (WP2.INT).
    let armed = Armed::new(FaultConfig::new(
        &format!("worker.activator.prepare.{}", w.ctx.repo),
        FaultAction::Fail,
    ));
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
    drop(armed);
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

/// One slice-by-slice drain of [`seal_forecast_follows_the_committing_branch`].
#[derive(Debug, Default)]
struct Drain {
    /// Slices that committed at least one event.
    slices: usize,
    /// Periodic seals alone in their slice, as forecast (`at == Some(1)`).
    isolated: usize,
    /// Periodic seals inside a slice of several events (a forecast miss).
    inside: usize,
    /// Slow-path events (committed alone; they always seal).
    slow: usize,
    committed: usize,
}

impl Drain {
    fn seals(&self) -> usize {
        self.isolated + self.inside
    }
}

/// Every branch's sealed startup-snapshot cursor.
fn snapshot_cursors(store: &Store) -> std::collections::BTreeMap<String, i64> {
    store
        .db
        .prepare("SELECT branch,cursor FROM startup_snapshots")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// Stages `events` on `store`, then drains them slice by slice as the
/// activator does (`seal_budget` of `seal_forecast`; a time budget no slice
/// reaches, so only the forecast cuts a slice short), checking the forecasts
/// against the seals core actually made: a forecast seal (`Some(1)`) always
/// happens, and `sealed_past` sees exactly the seals.
fn drain_with_forecast(store: &mut Store, events: &[Event], allowed: &BTreeSet<String>) -> Drain {
    use tkfs::{
        core::inbox::SliceBudget,
        worker::activator::{seal_budget, seal_forecast, sealed_past},
    };
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
    let full = SliceBudget {
        max_events: 64,
        max_ms: 3_600_000,
        inline_slow: false,
    };
    let mut drain = Drain::default();
    loop {
        let forecast = seal_forecast(store, full.max_events).expect("forecast");
        let before = snapshot_cursors(store);
        let slice = store
            .activate_slice(seal_budget(full, forecast.at))
            .unwrap();
        assert!(slice.quarantined.is_empty(), "{:?}", slice.quarantined);
        let after = snapshot_cursors(store);
        let sealed: Vec<&String> = after
            .iter()
            .filter(|(branch, cursor)| before.get(*branch) != Some(*cursor))
            .map(|(branch, _)| branch)
            .collect();
        assert_eq!(sealed_past(store, forecast.tip), !sealed.is_empty());
        let commits = (slice.fast + slice.slow) as usize;
        drain.committed += commits;
        if commits > 0 {
            drain.slices += 1;
            if forecast.at == Some(1) {
                assert_eq!(commits, 1, "{forecast:?}");
                assert_eq!(
                    sealed.len(),
                    1,
                    "the forecast seal did not happen: {forecast:?}, cursors {after:?}"
                );
                drain.isolated += 1;
            } else {
                assert!(sealed.len() <= 1, "{sealed:?}");
                drain.inside += sealed.len();
            }
        }
        if slice.slow_ticket.is_some() {
            // A slow event (the fork's root here) commits alone, inline.
            let slow = store
                .activate_slice(SliceBudget {
                    inline_slow: true,
                    max_events: 1,
                    ..full
                })
                .unwrap();
            assert_eq!(slow.slow, 1, "{slow:?}");
            drain.slow += 1;
            drain.committed += 1;
            continue;
        }
        if !slice.more {
            break;
        }
    }
    drain
}

/// Review WP2.3 fix round 2 (major): `core::snapshot::maybe_persist` seals
/// the startup snapshot of the committing event's branch, and the
/// activator's seal forecast follows it. A backlog on a shared branch the
/// worker has not checked out (a peer working there), drained slice by slice
/// with the activator's budget: every periodic seal happens alone in its
/// slice, as forecast, and the slices stay near events / 64. The first
/// round's forecast read the active branch's cursor, so after ~127 events
/// of another branch it cut every slice to one event and none of them
/// sealed. Then a backlog interleaving the active branch and that branch:
/// every forecast seal happens, and the slices stay near events / 64 (a seal
/// of the branch not at the head may fall inside a slice).
#[test]
fn seal_forecast_follows_the_committing_branch() {
    let mut p = pair(2);
    let main = p.a.active.clone();
    let feature = p.a.fork("feature", true).unwrap();
    p.a.checkout(&feature.id).unwrap();
    linear_history_named(&mut p.a, "ft", 600).unwrap();
    let mut b = Store::open_existing_with(
        &p.state,
        OpenOptions {
            defer_activation: true,
            verify: VerifyMode::Changed,
        },
    )
    .unwrap();
    assert_eq!(b.active, main);
    let events = p.events_after(&b.shared_event_inventory().unwrap());
    assert_eq!(events.len(), 601, "the fork's root and 600 creates");
    let drain = drain_with_forecast(&mut b, &events, &p.allowed);
    assert_eq!(drain.committed, 601, "{drain:?}");
    assert_eq!(drain.slow, 1, "{drain:?}");
    // One periodic seal per 128 journal sequences of the feature branch,
    // each alone in its slice.
    assert!((3..=6).contains(&drain.isolated), "{drain:?}");
    assert_eq!(drain.inside, 0, "{drain:?}");
    // Two slices per 128 events plus the seal's own.
    assert!(
        drain.slices <= drain.committed / 64 + 2 * drain.seals() + 2,
        "{drain:?}"
    );

    // Interleaved: the active branch and the feature branch alternate.
    p.a.checkout(&main).unwrap();
    linear_history_named(&mut p.a, "mn", 300).unwrap();
    p.a.checkout(&feature.id).unwrap();
    linear_history_named(&mut p.a, "ft2", 300).unwrap();
    let events = p.events_after(&b.shared_event_inventory().unwrap());
    assert_eq!(events.len(), 600);
    let (on_main, on_feature): (Vec<Event>, Vec<Event>) = events
        .into_iter()
        .partition(|event| event.branch.id == main);
    assert_eq!((on_main.len(), on_feature.len()), (300, 300));
    let interleaved: Vec<Event> = on_main
        .into_iter()
        .zip(on_feature)
        .flat_map(|(a, b)| [a, b])
        .collect();
    let drain = drain_with_forecast(&mut b, &interleaved, &p.allowed);
    assert_eq!(drain.committed, 600, "{drain:?}");
    assert_eq!(drain.slow, 0, "{drain:?}");
    // Both branches seal about every 128 journal sequences.
    assert!((6..=12).contains(&drain.seals()), "{drain:?}");
    assert!(drain.isolated >= 1, "{drain:?}");
    assert!(
        drain.slices <= drain.committed / 64 + 2 * drain.seals() + 2,
        "{drain:?}"
    );
    eprintln!("interleaved: {drain:?}");
}

/// Review WP2.3 fix round 2 (major), through the activator thread: a
/// backlog on a shared branch the worker has not checked out activates in
/// full slices (the first round's forecast made 483 slices of 600 events
/// there, one event each after the first ~127).
#[test]
fn a_backlog_on_another_shared_branch_activates_in_full_slices() {
    let mut p = pair(2);
    let feature = p.a.fork("feature", true).unwrap();
    p.a.checkout(&feature.id).unwrap();
    linear_history_named(&mut p.a, "ft", 600).unwrap();
    let w = Worker::start(&p.state, true);
    let committed = w.committed();
    let events = p.events_after(
        &w.ctx
            .sync_stores
            .checkout()
            .unwrap()
            .shared_event_inventory()
            .unwrap(),
    );
    assert_eq!(events.len(), 601);
    let slices = |status: &Value| status["activation"]["slices"].as_u64().unwrap();
    let before = slices(&w.status());
    w.stage(&events, &p.allowed);
    w.wait_drained(Duration::from_secs(120));
    assert_eq!(w.committed(), committed + 601);
    let status = w.status();
    let used = slices(&status) - before;
    // 64 events or 25 ms per slice: at least 2 events per slice on average
    // even on a slow debug build (the first round: about 1).
    assert!(used <= 300, "{used} slices for 601 events: {status}");
    eprintln!("feature backlog: {used} slices for 601 events");
}

/// Review WP2.3 fix round 2 (minor): a `revalidate` with the activator
/// running waits for an activator cycle that ran its slice. While another
/// connection holds SQLite's writer, the activator's slices give up at once
/// and are retried; those cycles did nothing, so the reply still waits, and
/// it reflects the pass that ran once the writer was released (the first
/// round counted the abandoned cycles as finished and replied before the
/// re-queued rows were activated).
#[test]
fn revalidate_waits_for_a_pass_that_ran_while_the_writer_is_busy() {
    let mut p = pair(2);
    linear_history_named(&mut p.a, "busy", 5).unwrap();
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
        // Classified, then deferred, as after a transient failure (see
        // `revalidate_only_requeues_while_the_activator_runs`).
        let mut engine = lock_engine(&w.engine);
        engine
            .store
            .activate_slice(tkfs::core::inbox::SliceBudget {
                max_events: 0,
                max_ms: 0,
                inline_slow: false,
            })
            .unwrap();
        for event in &events {
            engine.store.requeue(&event.id, 3_600_000).unwrap();
        }
    }
    let retrying = || -> i64 {
        w.ctx
            .sync_stores
            .checkout()
            .unwrap()
            .db
            .query_row(
                "SELECT count(*) FROM incoming_events WHERE state='retry'",
                [],
                |r| r.get(0),
            )
            .unwrap()
    };
    assert_eq!(retrying(), 1);
    thread::scope(|scope| {
        let revalidate = scope.spawn(|| {
            let reply = w.rpc(json!({"op":"revalidate"})).unwrap();
            (reply, w.committed())
        });
        // Once the request re-queued the row (and waits for a pass), another
        // connection takes SQLite's writer and the activator starts.
        assert!(wait_until(Duration::from_secs(10), || retrying() == 0));
        let writer = w.ctx.sync_stores.checkout().unwrap();
        writer.db.execute_batch("BEGIN IMMEDIATE").unwrap();
        worker::activator::try_spawn(w.ctx.clone()).unwrap();
        let held = Instant::now();
        while held.elapsed() < Duration::from_millis(150) {
            thread::sleep(Duration::from_millis(5));
        }
        let waiting = !revalidate.is_finished();
        writer.db.execute_batch("COMMIT").unwrap();
        drop(writer);
        let (reply, after) = revalidate.join().unwrap();
        assert_eq!(reply["requeued"], 1, "{reply}");
        assert!(
            waiting,
            "revalidate replied while the writer was held, before any pass ran"
        );
        assert_eq!(
            after,
            committed + 5,
            "the reply did not reflect the activator's pass"
        );
    });
    w.wait_drained(Duration::from_secs(60));
}

/// Review WP2.3 fix round 2 (minor): a panic in the activator's idle step
/// (which takes the engine to end a notification batch) is contained as a
/// cycle's is: the activator backs off and goes on, so rows staged later
/// still activate (the first round let it unwind out of the loop: no
/// activator thread, yet `activator_running` and the engine's
/// `background_activation` still set, so legacy pages only staged). When the
/// worker stops, the activator's drop guard clears both flags.
#[test]
fn a_panic_while_idle_is_contained() {
    use tkfs::util::fault::{FaultAction, FaultConfig};
    let mut p = pair(2);
    linear_history_named(&mut p.a, "idle", 20).unwrap();
    let w = Worker::start(&p.state, true);
    let committed = w.committed();
    let point = format!("worker.activator.idle.{}", w.ctx.repo);
    let armed = Armed::new(FaultConfig {
        count: Some(1),
        ..FaultConfig::new(&point, FaultAction::Panic)
    });
    // Nothing staged: a poke wakes the activator into its idle step.
    w.ctx.activation.poke();
    thread::sleep(Duration::from_millis(500));
    w.ctx.activation.poke();
    assert!(
        wait_until(Duration::from_secs(10), || {
            // Consumes the one hit if the activator has not idled yet.
            tkfs::util::fault::hit(&point).is_none()
        }),
        "the activator never idled"
    );
    drop(armed);
    assert!(w.ctx.activator_running.load(Ordering::SeqCst));
    let events = p.events_after(
        &w.ctx
            .sync_stores
            .checkout()
            .unwrap()
            .shared_event_inventory()
            .unwrap(),
    );
    assert_eq!(events.len(), 20);
    w.stage(&events, &p.allowed);
    w.wait_drained(Duration::from_secs(60));
    assert_eq!(w.committed(), committed + 20);
    // Stopped: no activator, so neither flag stays set.
    w.ctx.stopping.store(true, Ordering::SeqCst);
    w.ctx.activation.poke();
    assert!(
        wait_until(Duration::from_secs(10), || {
            !w.ctx.activator_running.load(Ordering::SeqCst)
                && !lock_engine(&w.engine).background_activation
        }),
        "the activator's flags outlived it"
    );
}

// ---------------------------------------------------------------------------
// Post-integration re-review (WP2.INT)

/// A worker state B whose author A edited `shared.txt` while B edited it too
/// (a two-way edit: A's event needs the slow path on B), after `history`
/// shared events; plus `side` events of a third replica C on another shared
/// branch (`side`, forked by C, then `side` directories): nothing local to B
/// touches that branch, so they activate whatever B's own branch does.
struct TwoWay {
    _dir: tempfile::TempDir,
    state: PathBuf,
    edit: tkfs::core::Bundle,
    side: Vec<Event>,
    allowed: BTreeSet<String>,
}

fn two_way_edit(history: usize, side: usize) -> TwoWay {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let state = dir.path().join("b");
    let mut b = Store::initialize(&state, &repo, &id()).unwrap();
    let mut a = Store::open(&dir.path().join("a"), Some(&repo)).unwrap();
    let mut c = Store::open(&dir.path().join("c"), Some(&repo)).unwrap();
    a.create("shared.txt", "file").unwrap();
    linear_history_named(&mut a, "history", history).unwrap();
    let allowed = devices([&a, &b, &c]);
    sync_into(&a, &mut b, &allowed).unwrap();
    let write = |s: &mut Store, bytes: &[u8]| {
        let entry = s.lookup("shared.txt").unwrap();
        s.write_revision(&entry.id, entry.revisions.get("content").cloned(), bytes)
            .unwrap();
    };
    write(&mut b, b"from b");
    sync_into(&b, &mut c, &allowed).unwrap();
    write(&mut a, b"from a");
    if side > 0 {
        let branch = c.fork("side", true).unwrap();
        c.checkout(&branch.id).unwrap();
        for i in 0..side {
            c.create(&format!("side-{i}"), "directory").unwrap();
        }
    }
    let edit = missing_bundle(&a, &b).unwrap();
    assert_eq!(edit.events.len(), 1);
    let side = metadata_bundle(&c, &b).unwrap().events;
    drop(b);
    TwoWay {
        _dir: dir,
        state,
        edit,
        side,
        allowed,
    }
}

impl Worker {
    /// Whether event `id` is committed (secondary connection).
    fn has_committed(&self, event: &str) -> bool {
        self.ctx
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
    }
    /// Puts the objects of `bundle` into the store (as a bulk lane would).
    fn put_objects(&self, bundle: &tkfs::core::Bundle) {
        let store = self.ctx.sync_stores.checkout().unwrap();
        for bytes in bundle.objects.values() {
            store.put_object(&hex::decode(bytes).unwrap()).unwrap();
        }
    }
}

/// The slow path's prepared commit (and the re-queues around it) never wait
/// for another connection's writer under the engine lock, as a slice never
/// does: a connection holds SQLite's writer 600 ms at a time (20 ms apart)
/// while a slow event is prepared and committed, and the filesystem's engine
/// waits stay short. Before the fix the commit polled for the writer under
/// the lock: one engine wait of most of a hold (the review measured 295–301
/// ms with 300 ms holds).
#[test]
fn a_slow_commit_never_waits_for_another_writer_under_the_engine_lock() {
    let t = two_way_edit(40, 0);
    let w = Worker::start(&t.state, false);
    w.put_objects(&t.edit);
    w.stage(&t.edit.events, &t.allowed);
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (ctx, stop) = (w.ctx.clone(), stop.clone());
        thread::spawn(move || {
            let store = ctx.sync_stores.checkout().unwrap();
            store.db.busy_timeout(Duration::from_secs(10)).unwrap();
            while !stop.load(Ordering::SeqCst) {
                store.db.execute_batch("BEGIN IMMEDIATE").unwrap();
                thread::sleep(Duration::from_millis(600));
                store.db.execute_batch("COMMIT").unwrap();
                thread::sleep(Duration::from_millis(20));
            }
        })
    };
    thread::sleep(Duration::from_millis(50));
    worker::activator::try_spawn(w.ctx.clone()).unwrap();
    let edit = t.edit.events[0].id.clone();
    let started = Instant::now();
    let mut longest = Duration::ZERO;
    while !w.has_committed(&edit) && started.elapsed() < Duration::from_secs(60) {
        let asked = Instant::now();
        drop(lock_engine(&w.engine));
        longest = longest.max(asked.elapsed());
        thread::sleep(Duration::from_millis(5));
    }
    stop.store(true, Ordering::SeqCst);
    writer.join().unwrap();
    assert!(w.has_committed(&edit), "the slow event never committed");
    let status = w.status();
    assert!(
        status["activation"]["slow"].as_u64().unwrap() >= 1,
        "{status}"
    );
    assert!(
        longest < Duration::from_millis(250),
        "the engine was held {longest:?} while another connection held the writer"
    );
}

/// A slow event that keeps going `Stale` behind a local writer that commits
/// more often than a preparation takes is committed inline after
/// `STALE_ESCALATE` stale rounds (CCR-WP2.INT-1), and its starvation is
/// reported for as long as it lasts: unrelated activations (C's events on
/// another branch) do not clear `starved_since_ms`. Before the fix the event
/// never committed while the writer ran, and any activation reset the
/// starvation report.
#[test]
fn a_starving_slow_event_is_committed_inline_and_reported_until_then() {
    let t = two_way_edit(200, 20);
    let w = Worker::start(&t.state, false);
    w.put_objects(&t.edit);
    w.stage(&t.edit.events, &t.allowed);
    let stop = Arc::new(AtomicBool::new(false));
    // A local writer: a save through the engine every few milliseconds.
    let writer = {
        let (engine, stop) = (w.engine.clone(), stop.clone());
        thread::spawn(move || {
            let mut n = 0u64;
            while !stop.load(Ordering::SeqCst) {
                {
                    let mut e = lock_engine(&engine);
                    let create = (n == 0).then_some("file");
                    let (h, _) = e.open("local.txt", create, true).unwrap();
                    e.write(h, 0, format!("save {n}").as_bytes(), false, false)
                        .unwrap();
                    e.close(h).unwrap();
                }
                n += 1;
                thread::sleep(Duration::from_millis(3));
            }
            n
        })
    };
    worker::activator::try_spawn(w.ctx.clone()).unwrap();
    let edit = t.edit.events[0].id.clone();
    let starved =
        || -> Option<u64> { w.ctx.status_cell.load()["activation"]["starved_since_ms"].as_u64() };
    // The first Stale round: the starvation is reported.
    assert!(
        wait_until(Duration::from_secs(30), || starved().is_some()),
        "no starvation reported: {}",
        w.status()
    );
    let since = starved().unwrap();
    // Unrelated events activate meanwhile; the report stays.
    w.stage(&t.side, &t.allowed);
    let mut samples = vec![];
    let mut seen_unrelated = false;
    let started = Instant::now();
    while !w.has_committed(&edit) && started.elapsed() < Duration::from_secs(60) {
        let side = t.side.iter().all(|e| w.has_committed(&e.id));
        let report = starved();
        if side && !w.has_committed(&edit) {
            seen_unrelated = true;
            samples.push(report);
        }
        thread::sleep(Duration::from_millis(20));
    }
    let committed = w.has_committed(&edit);
    stop.store(true, Ordering::SeqCst);
    let saves = writer.join().unwrap();
    assert!(
        committed,
        "the slow event never committed behind {saves} local saves: {}",
        w.status()
    );
    assert!(
        seen_unrelated,
        "the unrelated events did not activate while the event starved"
    );
    let cleared: Vec<_> = samples
        .iter()
        .filter(|s| **s != Some(since))
        .collect::<Vec<_>>();
    assert!(
        cleared.len() <= 1,
        "the starvation report changed while the event starved: {cleared:?}"
    );
    let status = w.status();
    assert!(
        status["activation"]["stale"].as_u64().unwrap()
            >= u64::from(worker::activator::STALE_ESCALATE),
        "{status}"
    );
    assert!(
        wait_until(Duration::from_secs(10), || starved().is_none()),
        "the starvation report outlived the event: {}",
        w.status()
    );
}

/// The status totals a refresh reads (journal counters, per-peer inventory
/// match) are cached per connection until the database changes: an idle
/// store's maintenance refresh no longer scans the journal and `peer_known`
/// at every tick. A WAL checkpoint is no change; another connection's commit
/// is.
#[test]
fn status_totals_are_cached_until_the_database_changes() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let state = dir.path().join("state");
    let mut s = Store::initialize(&state, &repo, &id()).unwrap();
    linear_history(&mut s, 50).unwrap();
    let device = s.device.clone();
    let reader = Store::open_secondary(&state, &repo, &device).unwrap();
    let journal = reader.journal_counters().unwrap();
    let inventory = reader.inventory_match().unwrap();
    let computed = reader.status_computations();
    assert_eq!(computed, 2);
    let other = Store::open_secondary(&state, &repo, &device).unwrap();
    let _ = other
        .db
        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_| Ok(()));
    for _ in 0..5 {
        assert_eq!(reader.journal_counters().unwrap(), journal);
        assert_eq!(reader.inventory_match().unwrap(), inventory);
    }
    assert_eq!(
        reader.status_computations(),
        computed,
        "recomputed while idle"
    );
    s.create("one-more", "directory").unwrap();
    assert_eq!(
        reader.journal_counters().unwrap().events,
        journal.events + 1
    );
    assert_eq!(reader.status_computations(), computed + 1);
}

/// The per-peer inventory match is recomputed only when the feed tip moves
/// (shared events): a private commit (a fork) moves the journal tip, not the
/// cell's feed `tip`. Before the fix the cell compared the feed tip with the
/// journal tip it recorded, so once the newest event was private every
/// `status` recomputed the match.
#[test]
fn a_private_journal_tip_does_not_recompute_the_inventory_match() {
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
    // No maintenance thread: only `status` calls refresh the cell.
    let engine: Shared = Arc::new(Mutex::new(Engine::new(
        Store::open_existing(&b_state).unwrap(),
    )));
    let ctx = worker::build_ctx(engine, &b_state, false, None);
    let server = runtime::server::start_rpc_server(ctx.clone()).unwrap();
    let info = server.publish(None).unwrap();
    let call = |payload: Value| runtime::rpc(&info, &id(), payload).unwrap();
    let file = a.store.create("one.txt", "file").unwrap();
    a.store
        .write_revision(&file.id, file.revisions.get("content").cloned(), b"one")
        .unwrap();
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
        let response =
            call(json!({"op":"published-exchange","route":gb,"page":page,"metadata_only":true}));
        tkfs::paired_sync::worker_control(
            a,
            &json!({"op":"published-apply","route":ga,"page":response,"request":page.request,"sent":sent,"metadata_only":true}),
        )
        .unwrap();
        let page: tkfs::paired_sync::SyncPage = serde_json::from_value(call(
            json!({"op":"published-offer","route":gb,"metadata_only":true}),
        ))
        .unwrap();
        let sent: BTreeSet<_> = page.bundle.events.iter().map(|e| e.id.clone()).collect();
        let response = tkfs::paired_sync::worker_control(
            a,
            &json!({"op":"published-exchange","route":ga,"page":page,"metadata_only":true}),
        )
        .unwrap();
        call(
            json!({"op":"published-apply","route":gb,"page":response,"request":page.request,"sent":sent,"metadata_only":true}),
        )
    };
    roundtrip(&mut a);
    roundtrip(&mut a);
    // A private commit on B: its journal tip moves, its feed tip does not.
    call(json!({"op":"branch","name":"private"}));
    roundtrip(&mut a);
    assert!(
        wait_until(
            Duration::from_secs(10),
            || call(json!({"op":"status"}))["caught_up"] == true
        ),
        "{}",
        call(json!({"op":"status"}))
    );
    let status = call(json!({"op":"status"}));
    assert!(
        status["journal"]["tip"].as_i64() > status["tip"]["seq"].as_i64(),
        "the newest event is private: {status}"
    );
    let computed = ctx.status_cell.load()["cell"]["inventory_at_ms"].clone();
    let mut recomputed = 0;
    for _ in 0..20 {
        assert_eq!(call(json!({"op":"status"}))["caught_up"], true);
        if ctx.status_cell.load()["cell"]["inventory_at_ms"] != computed {
            recomputed += 1;
        }
    }
    assert_eq!(recomputed, 0, "the inventory match was recomputed");
    ctx.stopping.store(true, Ordering::SeqCst);
}

/// The `status` after a client mutation reads its writes (the content
/// moved) without recomputing the journal counters and the inventory match
/// (maintenance brings those): a mutation no longer makes the next `status`
/// pay for them.
#[test]
fn status_after_a_mutation_refreshes_the_content_only() {
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
    call(json!({"op":"status"}));
    let before = meta();
    call(json!({"op":"import","path":"one.txt","hex":hex::encode(b"one")}));
    let after = call(json!({"op":"status"}));
    assert_eq!(after["content"]["current_objects"], 1, "{after}");
    assert!(meta()["content_at_ms"].as_u64() >= before["content_at_ms"].as_u64());
    assert!(
        meta()["content_generation"].as_u64() > before["content_generation"].as_u64(),
        "{}",
        meta()
    );
    assert_eq!(
        meta()["inventory_at_ms"],
        before["inventory_at_ms"],
        "the inventory match was recomputed by the read-your-writes refresh"
    );
    ctx.stopping.store(true, Ordering::SeqCst);
}

/// WP2.INT's confirmation review: the `status` after a client mutation never
/// computes the content on the caller. After another connection's commit
/// the content scan reloads the projection (1.6–2.3 s at 50K events); here
/// a fault point makes each background refresh take 1.5 s. While the last
/// refresh was cheap the reply waits for the next one (it reads its
/// writes); once one was slow, the reply answers from the cell at once with
/// `content_pending: true` and current cheap counters, and the content
/// catches up in the background. Before the fix every such reply computed
/// the content itself and took the 1.5 s.
#[test]
fn status_after_a_mutation_never_waits_for_a_slow_content_refresh() {
    use tkfs::util::fault::{FaultAction, FaultConfig};
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
    let import = |name: &str| call(json!({"op":"import","path":name,"hex":hex::encode(name)}));
    call(json!({"op":"status"}));
    let point = format!(
        "{}.{}",
        worker::status_cell::faults::CONTENT_REFRESH,
        ctx.repo
    );
    let _armed = Armed::new(FaultConfig {
        ms: Some(1500),
        ..FaultConfig::new(&point, FaultAction::Hang)
    });
    // The last full refresh was cheap: the reply waits for the content.
    import("one.txt");
    let (first, ms) = time_ms(|| call(json!({"op":"status"})));
    assert_eq!(first["content"]["current_objects"], 1, "{first}");
    assert!(first.get("content_pending").is_none(), "{first}");
    assert!(
        ms >= 1400.0,
        "the reply did not wait for the refresh: {ms} ms"
    );
    // That refresh took 1.5 s: the next reply does not wait for one.
    import("two.txt");
    let (second, ms) = time_ms(|| call(json!({"op":"status"})));
    assert!(ms < 1000.0, "the reply waited {ms} ms for the content");
    assert_eq!(second["content_pending"], true, "{second}");
    assert_eq!(second["content"]["current_objects"], 1, "{second}");
    assert!(
        second["journal"]["tip"].as_i64() > first["journal"]["tip"].as_i64(),
        "the cheap counters are current: {second}"
    );
    // The background refresh catches the content up.
    let mut last = Value::Null;
    assert!(
        wait_until(Duration::from_secs(15), || {
            let (status, ms) = time_ms(|| call(json!({"op":"status"})));
            assert!(ms < 1000.0, "a reply waited {ms} ms for the content");
            last = status;
            last["content"]["current_objects"] == 2 && last.get("content_pending").is_none()
        }),
        "the content never caught up: {last}"
    );
    ctx.stopping.store(true, Ordering::SeqCst);
}

/// WP2.3 review (minor), fixed by WP2.INT: a panic in a maintenance tick is
/// contained like the activator's (and the recovery loop's before it): the
/// thread lives on, and its next ticks still refresh the status cell's
/// content members.
#[test]
fn a_panicking_maintenance_tick_is_contained() {
    use tkfs::util::fault::{FaultAction, FaultConfig};
    let p = pair(2);
    let w = Worker::start(&p.state, false);
    let point = format!("worker.maintenance.tick.{}", w.ctx.repo);
    let content_at = |w: &Worker| w.ctx.status_cell.load()["cell"]["content_at_ms"].as_u64();
    {
        let _armed = Armed::new(FaultConfig {
            count: Some(1),
            ..FaultConfig::new(&point, FaultAction::Panic)
        });
        // Maintenance ticks every second: give it three to hit the point,
        // then make sure it did (a hit left here would be the test's own).
        thread::sleep(Duration::from_secs(3));
        assert!(
            tkfs::util::fault::hit(&point).is_none(),
            "maintenance never ticked into the armed point"
        );
    }
    let after_panic = content_at(&w).unwrap_or(0);
    // The full refresh is spaced by its own cost; ticks resume after the
    // first failure's 1 s backoff.
    assert!(
        wait_until(Duration::from_secs(15), || content_at(&w)
            .is_some_and(|at| at > after_panic)),
        "maintenance stopped after the panic"
    );
}
