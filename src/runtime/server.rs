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
//! consecutive accept errors exit 70 too. The supervisor restarts the worker.
use super::{
    Discovery, EngineGuard, EngineVitals, Shared, check_engine_poison, content::fetch_verified,
    ctx::WorkerCtx, export::export_bucket, lock_engine_tracked, notify::deliver_notifications,
    objects_rpc, published::published_rpc, register_worker, sync_rpc, try_lock_engine,
};
use crate::{
    core::*,
    model::{
        errors::ErrorWire,
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
        Arc, Condvar, Mutex, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

/// Legacy entry point (in-process tests and tools): builds a minimal
/// `WorkerCtx` (unsupervised, no previous record, no activator), starts the
/// RPC server and the heartbeat, and publishes `runtime.json` with `mount`.
pub fn start_rpc(engine: Shared, state: &Path, mount: Option<String>) -> Result<Discovery> {
    let ctx = crate::worker::build_ctx(engine, state, false, None);
    let server = start_rpc_server(ctx.clone())?;
    let mut info = server.discovery().clone();
    info.mount = mount;
    publish_discovery(&ctx.engine, state, &info)?;
    crate::worker::heartbeat::spawn(ctx, server.vitals().clone())?;
    Ok(info)
}

/// Starts the RPC listener for `ctx` (§4.1 step 6) and returns its
/// discovery (with `mount: None`). Nothing is written to disk: the caller
/// publishes `runtime.json` ([`publish_discovery`]) and `worker.json`.
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
/// writes `STATE/runtime.json` (`Discovery`, unchanged format).
pub fn publish_discovery(engine: &Shared, state: &Path, info: &Discovery) -> Result<()> {
    lock_engine_tracked(engine).discovery = serde_json::to_vec(info)?;
    std::fs::write(state.join("runtime.json"), serde_json::to_vec_pretty(info)?)?;
    Ok(())
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
    /// The `stop{mode}` op without a reply to flush (signals, idle exit).
    pub fn stop(&self, mode: StopMode) -> Result<StopReply> {
        self.core.stop(mode, false)
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
    discovery: Discovery,
    address: SocketAddr,
    stop: Notify,
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
    /// log, pause 100 ms, and exit 70 after 50 consecutive errors. Returns
    /// only once the worker is stopping (the stop path wakes it).
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
                crate::util::log::event!(fatal, WORKER_EXITED, {
                    "reason": "consecutive accept errors",
                    "consecutive": consecutive,
                    "exit_code": crate::model::ipc::EXIT_INTERNAL,
                });
                std::process::exit(crate::model::ipc::EXIT_INTERNAL);
            }
            thread::sleep(timing::ACCEPT_ERROR_PAUSE);
        }
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
    fn stop(&self, mode: StopMode, reply_follows: bool) -> Result<StopReply> {
        let attached = self.ctx.mount.status().state.is_attached();
        let reply = match mode {
            StopMode::Graceful => {
                lock_engine_tracked(&self.ctx.engine).quiet()?;
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
                    let mut e = lock_engine_tracked(&self.ctx.engine);
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
            .and_then(|path| self.ctx.mount.attach(&path));
        self.beat();
        Ok(json!(attached.map_err(ErrorWire::into_anyhow)?))
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

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".into())
}
