//! Owned by WP1.4.
//!
//! The worker's loopback RPC server (REVAMP_PLAN §4.1, §6.1): a blocking
//! accept loop that never gives up on transient errors, one thread per
//! connection with a persistent request loop, `catch_unwind` around every
//! request, and the IPC v3 ops (`hello`, `watch`, `stop{mode}`,
//! `mount-attach`/`-detach`/`-status`, `published-summary`, prefix routing for
//! `feed-*` and `objects.*`) next to the legacy ones.
//!
//! Crash-only internals: a request that panics while holding the engine
//! poisons it, and the poison policy (`runtime::lock_engine`) then persists
//! every dirty session and exits 70 (`PoisonPolicy::Exit`, the worker); 50
//! consecutive accept errors persist them too (best effort: the engine must
//! be free within 2 s) and exit 70. The supervisor restarts the worker.
//!
//! Mount attaches and stops are serialized by one gate: an attach (the
//! legacy startup attach or `mount-attach`) never starts once a stop was
//! accepted, and a stop that arrives during an attach waits for it and then
//! detaches what it attached, so a worker never exits with a mount it
//! attached after its stop.
use super::{
    Discovery, EngineGuard, EngineVitals, Shared, check_engine_poison, content::fetch_verified,
    ctx::WorkerCtx, export::export_bucket, lock_engine_tracked, notify::deliver_notifications,
    objects_rpc, published::published_rpc, register_worker, sync_rpc, try_lock_engine,
};
use crate::{
    core::*,
    model::{
        errors::ErrorWire,
        health::MountStatus,
        ipc::{
            HelloReply, IPC_VERSION, MountAttachRequest, MountDetachRequest, PublishedSummary,
            StopMode, StopReply, StopRequest, WatchRequest, ops, timing,
        },
        orchestrator::FeedTip,
    },
    util::{fault, lock::lock, notify::Notify, time::now_ms},
};
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use std::{
    io::Read,
    net::{SocketAddr, TcpListener, TcpStream},
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
    sync::{
        Arc, Condvar, Mutex, MutexGuard, PoisonError, TryLockError,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

/// How long the accept-error exit waits for the engine before it exits
/// without retaining ([`Core::exit_after_accept_errors`]).
const EXIT_RETAIN_WAIT: Duration = Duration::from_secs(2);

/// Legacy entry point (in-process tests and tools): builds a minimal
/// `WorkerCtx` (unsupervised, no previous record, no activator), starts the
/// RPC server and the heartbeat, and publishes `runtime.json` with `mount`
/// (as given: callers pass an absolute path, as the legacy worker did).
pub fn start_rpc(engine: Shared, state: &Path, mount: Option<String>) -> Result<Discovery> {
    let ctx = crate::worker::build_ctx(engine, state, false, None);
    let server = start_rpc_server(ctx.clone())?;
    // The heartbeat first, as in `worker::run`: nothing is published by a
    // call that then fails.
    crate::worker::heartbeat::spawn(ctx, server.vitals().clone())?;
    server.publish(mount)
}

/// Starts the RPC listener for `ctx` (§4.1 step 6) and returns its
/// discovery (with `mount: None`). Nothing is written to disk: the caller
/// publishes `runtime.json` ([`RpcServer::publish`]) and `worker.json`.
pub fn start_rpc_ctx(ctx: Arc<WorkerCtx>) -> Result<Discovery> {
    Ok(start_rpc_server(ctx)?.discovery().clone())
}

/// [`start_rpc_ctx`], returning the handle `worker::run` waits on.
pub fn start_rpc_server(ctx: Arc<WorkerCtx>) -> Result<RpcServer> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    register_worker(&ctx);
    let (observation, vitals) = {
        let e = lock_engine_tracked(&ctx.engine);
        (e.observation.clone(), e.vitals.clone())
    };
    let discovery = Discovery {
        format: 2,
        address: format!("ws://{address}"),
        state: ctx.state.to_string_lossy().into_owned(),
        mount: None,
        repo: ctx.repo.clone(),
        device: ctx.device.clone(),
    };
    let core = Arc::new(Core {
        bulk: crate::bulk_sync::Worker {
            root: ctx.root.clone(),
            repo: ctx.repo.clone(),
            device: ctx.device.clone(),
            locks: ctx.locks.clone(),
        },
        ctx,
        vitals,
        observation,
        discovery,
        address,
        stop: Notify::new(),
        transitions: Mutex::new(()),
        published: Mutex::new(None),
        reply: Mutex::new(StopReplyState::default()),
        reply_flushed: Condvar::new(),
        connections: AtomicU64::new(0),
    });
    let recovery = core.clone();
    thread::Builder::new()
        .name("recovery".into())
        .spawn(move || recovery.recovery_loop())?;
    let accept = core.clone();
    thread::Builder::new()
        .name("rpc-accept".into())
        .spawn(move || accept.accept_loop(listener))?;
    Ok(RpcServer { core })
}

/// Sets the engine's discovery bytes (the mount's `.tkfs-runtime.json`) and
/// writes `STATE/runtime.json` (`Discovery`, unchanged format) atomically.
pub fn publish_discovery(engine: &Shared, state: &Path, info: &Discovery) -> Result<()> {
    lock_engine_tracked(engine).discovery = serde_json::to_vec(info)?;
    crate::worker::record::replace_file(
        &state.join("runtime.json"),
        &serde_json::to_vec_pretty(info)?,
    )
}

/// A running RPC server. Dropping the handle does not stop it; the worker
/// stops through `stop` (an RPC, an idle exit or a signal).
pub struct RpcServer {
    core: Arc<Core>,
}

impl RpcServer {
    pub fn discovery(&self) -> &Discovery {
        &self.core.discovery
    }
    pub fn ctx(&self) -> &Arc<WorkerCtx> {
        &self.core.ctx
    }
    pub fn vitals(&self) -> &Arc<EngineVitals> {
        &self.core.vitals
    }
    /// Publishes this server's discovery with `mount` (an absolute path, or
    /// `None`): the engine's discovery bytes and `STATE/runtime.json`.
    pub fn publish(&self, mount: Option<String>) -> Result<Discovery> {
        self.core.publish(mount)
    }
    /// Attaches the mount at `path` (already prepared: absolute, normalized,
    /// not overlapping the state) unless a stop was accepted
    /// (`WORKER_STOPPING`); a stop requested meanwhile waits for it. On
    /// success `runtime.json` advertises `path`.
    pub fn attach(&self, path: &Path) -> Result<MountStatus, ErrorWire> {
        self.core.attach(path)
    }
    /// The `stop{mode}` op without a reply to flush (signals, idle exit).
    /// It never waits for the engine or for a mount attach: while either is
    /// busy it refuses with `WORKER_BUSY` and the caller retries later.
    pub fn stop(&self, mode: StopMode) -> Result<StopReply> {
        let reply = self.core.stop(mode, false)?;
        self.core.wake_accept();
        Ok(reply)
    }
    /// Counts stop requests; pass it to [`RpcServer::wait_stop`].
    pub fn stop_count(&self) -> u64 {
        self.core.stop.count()
    }
    /// Waits until a stop newer than `seen` was requested, or `timeout`.
    pub fn wait_stop(&self, seen: u64, timeout: Duration) {
        self.core.stop.wait(seen, timeout);
    }
    /// After a stop requested over RPC, waits (≤ `timeout`) until its reply
    /// was written. Returns `false` on timeout.
    pub fn wait_stop_reply(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut reply = lock(&self.core.reply);
        while reply.pending && !reply.flushed {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return false;
            };
            reply = self
                .core
                .reply_flushed
                .wait_timeout(reply, remaining)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        true
    }
}

#[derive(Default)]
struct StopReplyState {
    /// A stop was accepted over RPC; its reply is still to be written.
    pending: bool,
    flushed: bool,
}

/// What every connection thread shares.
struct Core {
    ctx: Arc<WorkerCtx>,
    vitals: Arc<EngineVitals>,
    /// The legacy busy-status observation (`Engine::publish_observation`).
    observation: Arc<Mutex<Value>>,
    bulk: crate::bulk_sync::Worker,
    /// This server's discovery with `mount: None`.
    discovery: Discovery,
    address: SocketAddr,
    stop: Notify,
    /// Serializes mount attaches with stops (see the module docs). Taken
    /// before the engine, never while holding it.
    transitions: Mutex<()>,
    /// The `mount` last published in `runtime.json`.
    published: Mutex<Option<String>>,
    reply: Mutex<StopReplyState>,
    reply_flushed: Condvar,
    connections: AtomicU64,
}

/// One handled request: its result, and whether it accepted a stop (the
/// reply must then be flushed before the process exits).
struct Handled {
    result: Result<Value>,
    stop: bool,
}

impl Core {
    /// The 250 ms recovery loop (unchanged duty): retry pending saves, publish
    /// the observation, deliver filesystem notifications. A panic is contained;
    /// if it poisoned the engine, the poison policy runs.
    fn recovery_loop(&self) {
        while !self.ctx.stopping.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(250));
            let tick = catch_unwind(AssertUnwindSafe(|| {
                if let Some(mut e) = try_lock_engine(&self.ctx.engine) {
                    let _ = e.retry_pending();
                    e.publish_observation();
                }
                deliver_notifications(&self.ctx.engine);
            }));
            if tick.is_err() {
                check_engine_poison(&self.ctx.engine);
            }
        }
    }

    /// Blocking accept, one thread per connection. Never exits on an error:
    /// log, pause 100 ms, and after 50 consecutive errors retain the dirty
    /// sessions and exit 70 ([`Core::exit_after_accept_errors`]). Returns only
    /// once the worker is stopping (the stop path wakes it).
    fn accept_loop(self: Arc<Self>, listener: TcpListener) {
        let mut consecutive = 0u32;
        loop {
            let accepted =
                listener
                    .accept()
                    .map_err(anyhow::Error::from)
                    .and_then(|(stream, _)| {
                        fault::fail("worker.accept_error")?;
                        Ok(stream)
                    });
            if self.ctx.stopping.load(Ordering::Relaxed) {
                return;
            }
            let error = match accepted {
                Ok(stream) => {
                    let id = self.connections.fetch_add(1, Ordering::Relaxed) + 1;
                    let core = self.clone();
                    match thread::Builder::new()
                        .name(format!("rpc-conn-{id}"))
                        .spawn(move || core.serve(stream))
                    {
                        Ok(_) => {
                            consecutive = 0;
                            continue;
                        }
                        Err(error) => anyhow::Error::from(error),
                    }
                }
                Err(error) => error,
            };
            consecutive += 1;
            let wire = ErrorWire::from_error(&error);
            crate::util::log::event!(warn, (wire.code.as_str()), {
                "site": "worker.accept",
                "message": wire.message,
                "consecutive": consecutive,
            });
            if consecutive >= timing::ACCEPT_ERROR_LIMIT {
                self.exit_after_accept_errors(consecutive);
            }
            thread::sleep(timing::ACCEPT_ERROR_PAUSE);
        }
    }

    /// The exit after [`timing::ACCEPT_ERROR_LIMIT`] consecutive accept errors
    /// (exit 70; the supervisor restarts the worker). Like the poison exit, it
    /// first retains every dirty session (invariant 8): a realistic cause is
    /// descriptor exhaustion, with a mount up and files unsaved.
    ///
    /// The engine is taken without waiting on a wedged holder
    /// (`try_lock_engine`, retried for at most [`EXIT_RETAIN_WAIT`]), then
    /// `retain_all_dirty` runs with a panic contained, and the fatal
    /// `WORKER_EXITED` line logs the outcome (`retained_dirty`, `error`,
    /// `retain_panicked`, `engine_busy`). The process exits while still holding
    /// the engine, so nothing changes after the retain. An engine still busy at
    /// the deadline is logged and the exit proceeds: nobody can retain from a
    /// wedged engine. A poisoned engine takes the poison exit instead (its
    /// policy runs inside `try_lock_engine`). `runtime.json` and `worker.json`
    /// stay, as after the poison exit: the next worker's FUSE recovery reads
    /// this worker's record.
    fn exit_after_accept_errors(&self, consecutive: u32) -> ! {
        let deadline = Instant::now() + EXIT_RETAIN_WAIT;
        let mut engine = loop {
            match try_lock_engine(&self.ctx.engine) {
                Some(engine) => break Some(engine),
                None if Instant::now() >= deadline => break None,
                None => thread::sleep(Duration::from_millis(10)),
            }
        };
        let outcome = engine
            .as_mut()
            .map(|engine| catch_unwind(AssertUnwindSafe(|| engine.retain_all_dirty())));
        let (retained, error, panicked) = match &outcome {
            Some(Ok(Ok(retained))) => (Some(*retained), None, false),
            Some(Ok(Err(error))) => (None, Some(format!("{error:#}")), false),
            Some(Err(_)) => (None, None, true),
            None => (None, None, false),
        };
        crate::util::log::event!(fatal, WORKER_EXITED, {
            "reason": "consecutive accept errors",
            "consecutive": consecutive,
            "retained_dirty": retained,
            "error": error,
            "retain_panicked": panicked,
            "engine_busy": engine.is_none(),
            "exit_code": crate::model::ipc::EXIT_INTERNAL,
        });
        // Exit while still holding the engine (if taken).
        std::process::exit(crate::model::ipc::EXIT_INTERNAL)
    }

    /// One connection: a persistent request loop. Notifications are delivered
    /// by the recovery loop, never on the reply path.
    fn serve(&self, stream: TcpStream) {
        let Ok(mut socket) = crate::ws::accept(stream) else {
            return;
        };
        loop {
            let Ok(request) = crate::ws::receive::<Value>(&mut socket) else {
                return;
            };
            let handled = match catch_unwind(AssertUnwindSafe(|| self.dispatch(&request))) {
                Ok(handled) => handled,
                Err(panic) => {
                    // A panic under the engine lock poisoned it: the policy
                    // persists dirty sessions and exits (or panics in tests).
                    check_engine_poison(&self.ctx.engine);
                    Handled {
                        result: Err(anyhow::anyhow!(
                            "UNKNOWN_ERROR: request handler panicked: {}",
                            panic_message(panic.as_ref())
                        )),
                        stop: false,
                    }
                }
            };
            let sent = crate::ws::reply(&mut socket, handled.result);
            if handled.stop {
                self.stop_reply_flushed();
                self.wake_accept();
            }
            if sent.is_err() || self.ctx.stopping.load(Ordering::Relaxed) {
                return;
            }
        }
    }

    fn dispatch(&self, request: &Value) -> Handled {
        let mut stop = false;
        let result = self.route(request, &mut stop);
        Handled { result, stop }
    }

    fn route(&self, r: &Value, stop: &mut bool) -> Result<Value> {
        let request = r["request"].as_str().context("INVALID_REQUEST_ID")?;
        let payload = &r["payload"];
        let op = payload["op"].as_str().unwrap_or_default();
        if !op.is_empty() {
            fault::trigger(&format!("worker.rpc.{op}"))?;
        }
        match op {
            ops::HELLO => self.hello(),
            ops::WATCH => self.watch(payload),
            ops::STOP => {
                let StopRequest { mode } = serde_json::from_value(payload.clone())?;
                let reply = self.stop(mode, true)?;
                *stop = true;
                Ok(json!(reply))
            }
            ops::MOUNT_ATTACH => self.mount_attach(payload),
            ops::MOUNT_DETACH => {
                let MountDetachRequest { force } = serde_json::from_value(payload.clone())?;
                let status = self.ctx.mount.detach(force);
                self.beat();
                Ok(json!(status.map_err(ErrorWire::into_anyhow)?))
            }
            ops::MOUNT_STATUS => Ok(json!(self.ctx.mount.status())),
            // `Store::revalidate` arrives with WP1.6; WP1.INT wires this arm.
            ops::REVALIDATE => bail!("UNSUPPORTED_OPERATION: {op}"),
            ops::PUBLISHED_SUMMARY => self.published_summary(),
            ops::STATUS => self.status(request, payload),
            op if op.starts_with(ops::FEED_PREFIX) => sync_rpc::dispatch(&self.ctx, op, payload),
            op if op.starts_with(ops::OBJECTS_PREFIX) => {
                let (header, body) = objects_rpc::dispatch(&self.ctx, op, payload, &[])
                    .map_err(ErrorWire::into_anyhow)?;
                ensure!(
                    body.is_empty(),
                    "BINARY_OPS_REQUIRE_SESSION: {op} replied with {} body bytes",
                    body.len()
                );
                Ok(header)
            }
            op if op.starts_with("published-bulk-") => {
                crate::bulk_sync::worker_control(&self.bulk, payload)
            }
            op if op.starts_with("published-") => published_rpc(&self.ctx.engine, payload),
            "preview-object" | "cat-object" => self.object(op, payload),
            "bucket-export" => export_bucket(&self.ctx.engine, payload),
            "checkout" => {
                let mounted = self.engine()?.mounted;
                if mounted {
                    crate::mount::checkout(&self.ctx.engine, request, payload)
                } else {
                    self.engine()?.control(request, payload)
                }
            }
            _ => self.engine()?.control(request, payload),
        }
    }

    /// The engine for an RPC. `engine.poison` fires here: with
    /// `TKFS_FAULT_ACTION=panic` it panics while holding the lock (poisoning
    /// it), `hang` holds it, `fail` fails the request.
    fn engine(&self) -> Result<EngineGuard<'_>> {
        let guard = lock_engine_tracked(&self.ctx.engine);
        fault::trigger("engine.poison")?;
        Ok(guard)
    }

    /// [`Core::engine`] without waiting: `None` while another thread holds it.
    fn try_engine(&self) -> Result<Option<EngineGuard<'_>>> {
        let Some(guard) = try_lock_engine(&self.ctx.engine) else {
            return Ok(None);
        };
        fault::trigger("engine.poison")?;
        Ok(Some(guard))
    }

    fn contact(&self) {
        self.ctx.last_contact_ms.store(now_ms(), Ordering::Relaxed);
    }

    fn beat(&self) {
        crate::worker::heartbeat::beat(&self.ctx, &self.vitals);
    }

    fn hello(&self) -> Result<Value> {
        self.contact();
        let ctx = &self.ctx;
        Ok(json!(HelloReply {
            instance: ctx.instance.clone(),
            pid: std::process::id(),
            repo: ctx.repo.clone(),
            device: ctx.device.clone(),
            version: ctx.version.clone(),
            ipc: IPC_VERSION,
            started_at_ms: ctx.started_at_ms,
            process_start: crate::worker::record::process_start(),
        }))
    }

    /// `watch{after_seq, wait_ms ≤ 5000}`: the first heartbeat with
    /// `seq > after_seq`, or the current one when `wait_ms` elapses.
    fn watch(&self, payload: &Value) -> Result<Value> {
        let request: WatchRequest = serde_json::from_value(payload.clone())?;
        self.contact();
        fault::trigger("worker.watch_hang")?;
        let deadline = Instant::now() + request.wait();
        loop {
            let (heartbeat, generation) = self.ctx.heartbeat.load_versioned();
            let remaining = deadline.saturating_duration_since(Instant::now());
            if heartbeat.seq > request.after_seq
                || remaining.is_zero()
                || self.ctx.stopping.load(Ordering::Relaxed)
            {
                return Ok(json!(*heartbeat));
            }
            self.ctx.heartbeat.wait_newer(generation, remaining);
        }
    }

    /// `stop{mode}` (§4.1 step 11, §3.3 stop ladder). Also called by the main
    /// thread (idle exit, signals), so it takes the engine without the
    /// `engine.poison` fault point.
    ///
    /// * graceful: `quiet()` (else `BUSY_VIEW: …`), detach the mount
    ///   gracefully, then stop;
    /// * force: detach the mount with `force` (a failure is logged: the exit
    ///   tears the mount down), fence new opens, `retain_all_dirty` (a failure
    ///   refuses the stop: nothing is lost by staying up), then stop.
    ///
    /// Over RPC (`reply_follows`) it waits for the transition gate and the
    /// engine. The main thread's callers (no reply) must keep servicing the
    /// idle timer and signals, so for them a busy gate or engine is a
    /// `WORKER_BUSY` refusal they retry, never a wait on a wedged engine.
    fn stop(&self, mode: StopMode, reply_follows: bool) -> Result<StopReply> {
        let busy = "WORKER_BUSY: the engine or a mount attach is busy; stop deferred";
        let _transition = if reply_follows {
            lock(&self.transitions)
        } else {
            try_lock(&self.transitions).context(busy)?
        };
        let engine = || -> Result<EngineGuard<'_>> {
            if reply_follows {
                Ok(lock_engine_tracked(&self.ctx.engine))
            } else {
                try_lock_engine(&self.ctx.engine).context(busy)
            }
        };
        let attached = self.ctx.mount.status().state.is_attached();
        let reply = match mode {
            StopMode::Graceful => {
                engine()?.quiet()?;
                let detached = self.ctx.mount.detach(false);
                self.beat();
                detached.map_err(ErrorWire::into_anyhow)?;
                StopReply {
                    stopped: true,
                    persisted_dirty: 0,
                    detached: attached,
                }
            }
            StopMode::Force => {
                let detached = self.ctx.mount.detach(true);
                self.beat();
                if let Err(error) = &detached {
                    crate::util::log::event!(warn, (error.code.as_str()), {
                        "site": "worker.stop",
                        "mode": "force",
                        "message": error.message,
                    });
                }
                let persisted = {
                    let mut e = engine()?;
                    let fenced = e.switching;
                    e.switching = true;
                    match e.retain_all_dirty() {
                        Ok(persisted) => persisted,
                        Err(error) => {
                            e.switching = fenced;
                            return Err(error);
                        }
                    }
                };
                StopReply {
                    stopped: true,
                    persisted_dirty: u32::try_from(persisted).unwrap_or(u32::MAX),
                    detached: attached && detached.is_ok(),
                }
            }
        };
        if reply_follows {
            *lock(&self.reply) = StopReplyState {
                pending: true,
                flushed: false,
            };
        }
        self.ctx.stopping.store(true, Ordering::SeqCst);
        self.stop.poke();
        Ok(reply)
    }

    fn stop_reply_flushed(&self) {
        lock(&self.reply).flushed = true;
        self.reply_flushed.notify_all();
    }

    /// Wakes the blocking accept so the listener closes once stopping.
    fn wake_accept(&self) {
        let _ = TcpStream::connect_timeout(&self.address, Duration::from_millis(200));
    }

    fn mount_attach(&self, payload: &Value) -> Result<Value> {
        let MountAttachRequest { path } = serde_json::from_value(payload.clone())?;
        let attached = crate::worker::prepare_mount_path(&self.ctx.state, &path)
            .and_then(|path| self.attach(&path));
        Ok(json!(attached.map_err(ErrorWire::into_anyhow)?))
    }

    /// [`RpcServer::attach`]. Under the transition gate: refused once a stop
    /// was accepted; on success `runtime.json` advertises the attached path
    /// (published before the gate is released, so never after the exit
    /// removed it). A failed publish is logged; the mount stays attached.
    fn attach(&self, path: &Path) -> Result<MountStatus, ErrorWire> {
        let attached = {
            let _transition = lock(&self.transitions);
            if self.ctx.stopping.load(Ordering::SeqCst) {
                return Err(ErrorWire::coded(
                    "WORKER_STOPPING",
                    format!("{} was not attached: a stop was accepted", path.display()),
                ));
            }
            let attached = self.ctx.mount.attach(path);
            let mount = Some(path.to_string_lossy().into_owned());
            let changed = *lock(&self.published) != mount;
            if attached.is_ok()
                && changed
                && let Err(error) = self.publish(mount)
            {
                let wire = ErrorWire::from_error(&error);
                crate::util::log::event!(warn, (wire.code.as_str()), {
                    "site": "worker.publish",
                    "message": wire.message,
                });
            }
            attached
        };
        self.beat();
        attached
    }

    /// [`RpcServer::publish`]; serialized by `published`.
    fn publish(&self, mount: Option<String>) -> Result<Discovery> {
        let mut published = lock(&self.published);
        let mut info = self.discovery.clone();
        info.mount = mount;
        publish_discovery(&self.ctx.engine, &self.ctx.state, &info)?;
        published.clone_from(&info.mount);
        Ok(info)
    }

    /// `published-summary` from a pooled secondary store (no engine lock).
    fn published_summary(&self) -> Result<Value> {
        let store = self.ctx.sync_stores.checkout()?;
        let branches = store
            .branches()?
            .into_iter()
            .filter(|branch| branch.shared)
            .collect();
        let seq: i64 = store.db.query_row(
            "SELECT COALESCE(MAX(seq),0) FROM journal_sequence",
            [],
            |row| row.get(0),
        )?;
        Ok(json!(PublishedSummary {
            repo: self.ctx.repo.clone(),
            replica: self.ctx.device.clone(),
            label: self.ctx.repo.clone(),
            branches,
            // The feed epoch arrives with WP2.5, the sync generation with WP2.3.
            tip: FeedTip {
                epoch: String::new(),
                seq,
            },
            sync_generation: 0,
        }))
    }

    /// The legacy status JSON (or, while the engine is busy, the last
    /// observation marked `busy`/`stale`), plus the typed `mount`,
    /// `store_health` and `heartbeat_seq` members (§6.1).
    fn status(&self, request: &str, payload: &Value) -> Result<Value> {
        let mut status = match self.try_engine()? {
            Some(mut e) => e.control(request, payload)?,
            None => {
                let mut status = lock(&self.observation).clone();
                status["busy"] = json!(true);
                status["stale"] = json!(true);
                status
            }
        };
        status["mount"] = json!(self.ctx.mount.status());
        status["store_health"] = json!(self.vitals.store_health());
        status["heartbeat_seq"] = json!(self.ctx.heartbeat.load().seq);
        Ok(status)
    }

    fn object(&self, op: &str, payload: &Value) -> Result<Value> {
        let object = payload["object"].as_str().context("MISSING_ARGUMENT")?;
        let mut file = fetch_verified(
            &self.ctx.engine,
            object,
            Instant::now() + Duration::from_secs(15),
        )?;
        use std::io::Seek;
        file.rewind()?;
        let total = file.metadata()?.len();
        if op == "cat-object" {
            ensure!(total <= MAX_FILE as u64, "OBJECT_REQUIRES_STREAMING");
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            return Ok(json!({"hex":hex::encode(bytes)}));
        }
        let mut bytes = Vec::new();
        file.take(64 * 1024).read_to_end(&mut bytes)?;
        let binary = bytes.contains(&0) || std::str::from_utf8(&bytes).is_err();
        Ok(
            json!({"object":object,"size":total,"truncated":total > bytes.len() as u64,
            "text":if binary { "Binary content; text preview unavailable".to_owned() } else { String::from_utf8_lossy(&bytes).into_owned() }}),
        )
    }
}

/// A poison-tolerant `try_lock`: `None` while another thread holds `mutex`.
fn try_lock<T>(mutex: &Mutex<T>) -> Option<MutexGuard<'_, T>> {
    match mutex.try_lock() {
        Ok(guard) => Some(guard),
        Err(TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
        Err(TryLockError::WouldBlock) => None,
    }
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".into())
}
