//! Owned by WP1.4; WP2.3 in Wave 2; WP3.2 in Wave 3 (loopback sessions).
//!
//! The worker's loopback RPC server (REVAMP_PLAN §4.1, §6.1): a blocking
//! accept loop that never gives up on transient errors, one thread per
//! connection with a persistent request loop, `catch_unwind` around every
//! request, and the IPC v3 ops (`hello`, `watch`, `stop{mode}`,
//! `mount-attach`/`-detach`/`-status`, `revalidate`, `published-summary`, prefix routing for
//! `feed-*` and `objects.*`) next to the legacy ones.
//!
//! WP2.3: `status` is served from the status cell without the engine lock
//! (`worker::status_cell::reply`; never `WORKER_BUSY`, never the old
//! busy/stale fallback), and every mutating request refreshes the cell before
//! it replies (read-your-writes). Pending-save retries and filesystem
//! notifications left this server: the maintenance and notifier threads own
//! them (the old 250 ms recovery loop is gone). The legacy `start_rpc`
//! wrapper starts the heartbeat, maintenance and notifier threads, but no
//! background activator, so in-process callers keep the legacy bounded
//! activation of `published-*` (≤ 32 events per metadata exchange).
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
//!
//! **Loopback sessions (WP3.2, §6.1, §6.4).** The same listener serves v3
//! loopback sessions: a connection whose WebSocket upgrade carries
//! `X-Tkfs-Sync: 3-loopback` (read with `peek`, so nothing is consumed) goes
//! to `session::accept_loopback` and `Conn::serve`; every other upgrade is
//! the JSON RPC, unchanged. On a session, `objects.*` ops carry binary bodies
//! (`objects_rpc::dispatch`). The other ops are the JSON RPC ops over the
//! session: the request header is the op's payload (or, when the request has
//! a body, the body is the payload's JSON), and the reply is the JSON result
//! as the header, or, above [`INLINE_REPLY`] bytes, the header
//! `{"in_body": true}` with the JSON as the body. `stop` stays on the JSON
//! RPC, whose reply flush the stop ladder waits for. Workers never dial
//! (invariant 25): sessions are accepted only.
//!
//! **Local token (WP4.1, §14).** Each server generates a per-instance token
//! (32 random bytes, hex) before its listener accepts anything, writes it to
//! `STATE/runtime.token` (owner-only, `worker::record::write_private`) and
//! requires it as `X-Tkfs-Token` on every JSON RPC upgrade
//! (`ws::accept_authorized`) and loopback session upgrade
//! (`session::accept_loopback`): a connection without it is refused at the
//! upgrade (HTTP 403, `ACCESS_DENIED`). Before its upgrade is authorized a
//! connection is bounded (WP4.INT re-review): its whole pre-auth phase (the
//! peek and the upgrade) within [`UPGRADE_BUDGET`] (a watchdog), and at most
//! [`MAX_PREAUTH_CONNECTIONS`] of them at once (the oldest is closed for a
//! new one), so connections that never present the token cannot exhaust the
//! worker's threads (a crash-restart loop through the accept-error exit).
//! `worker::run` also puts it in `worker.json` and the Ready line
//! ([`RpcServer::token`]). Clients of this
//! process find it through `ws::tokens` (the server remembers its own
//! address there); `runtime::rpc` and `runtime::connect_worker` read the file.
use super::{
    Discovery, EngineGuard, EngineVitals, Shared, check_engine_poison, content::fetch_verified,
    ctx::WorkerCtx, export::export_bucket, lock_engine_tracked, objects_rpc,
    published::published_rpc, register_worker, sync_rpc, try_lock_engine,
};
use crate::{
    core::*,
    model::{
        errors::ErrorWire,
        health::MountStatus,
        ipc::{
            HelloReply, IPC_VERSION, MountAttachRequest, MountDetachRequest, PublishedSummary,
            RevalidateReply, RevalidateRequest, StopMode, StopReply, StopRequest, WatchRequest,
            ops, timing,
        },
        orchestrator::FeedTip,
        protocol::{UPGRADE_HEADER, UPGRADE_LOOPBACK, UPGRADE_V3},
    },
    session::{self, ConnOptions, Handler, Incoming, UpgradeKind},
    util::{fault, lock::lock, notify::Notify, time::now_ms},
};
use anyhow::{Context, Result, ensure};
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
/// `revalidate` activates what it re-queued in one bounded pass (the legacy
/// lane receive budget, WP1.INT task 1).
const REVALIDATE_ACTIVATION_EVENTS: usize = 32;
const REVALIDATE_ACTIVATION_MS: u64 = 50;
/// How long `revalidate` waits for the background activator's pass over the
/// rows it re-queued (the request holds no engine lock meanwhile).
pub const REVALIDATE_WAIT: Duration = Duration::from_secs(1);
/// How long a new connection may take to send its WebSocket upgrade request
/// before the listener decides what it is (`peek`, nothing consumed).
const UPGRADE_PEEK_TIMEOUT: Duration = Duration::from_secs(5);
/// Bytes of an upgrade request read to find its `X-Tkfs-Sync` header (the
/// pre-auth bound of §6.2).
const UPGRADE_PEEK_MAX: usize = 16 * 1024;
/// Largest JSON result a loopback session returns as its reply header (the
/// session header bound is 64 KiB); larger ones travel as the body.
pub const INLINE_REPLY: usize = 60 * 1024;
/// Connections whose upgrade has not yet carried the instance's token, at
/// most (WP4.INT re-review; the peer server's `limits::MAX_PREAUTH`): every
/// accepted connection takes one of these slots until its upgrade is
/// authorized, and when all are taken the oldest is closed to admit the new
/// one. So local connections that never present the token hold at most this
/// many threads (and a watchdog thread each), and cannot keep a client that
/// upgrades at once out.
pub const MAX_PREAUTH_CONNECTIONS: usize = crate::model::orchestrator::limits::MAX_PREAUTH;
/// The whole pre-auth phase of one connection: the upgrade peek, the token
/// check and the upgrade, enforced by a watchdog that shuts the socket down
/// (a client trickling its request cannot extend it; the per-read socket
/// timeouts alone are renewed by every byte).
pub const UPGRADE_BUDGET: Duration = crate::model::protocol::frame::HANDSHAKE_TIMEOUT;
/// Handler threads and queued requests of one loopback session: the
/// orchestrator keeps up to 4 reads and 4 writes in flight per transfer, and
/// one session (`worker_client::session_for`) serves every lane.
const LOOPBACK_HANDLERS: usize = 8;
const LOOPBACK_QUEUE: usize = 64;

/// The options of an accepted loopback session.
pub fn loopback_options() -> ConnOptions {
    ConnOptions {
        handlers: LOOPBACK_HANDLERS,
        handler_queue: LOOPBACK_QUEUE,
        ..ConnOptions::default()
    }
}

/// The kind of the WebSocket upgrade request waiting on `stream`, from its
/// `X-Tkfs-Sync` header, read with `peek` (the request stays for the
/// handshake that follows). `Legacy` when no complete request head arrives
/// within [`UPGRADE_PEEK_TIMEOUT`] or [`UPGRADE_PEEK_MAX`] bytes: the JSON
/// RPC handshake then refuses it as before.
pub fn peek_upgrade(stream: &TcpStream) -> UpgradeKind {
    // Windows inherits the listener's mode on accepted sockets.
    if stream.set_nonblocking(false).is_err()
        || stream.set_read_timeout(Some(UPGRADE_PEEK_TIMEOUT)).is_err()
    {
        return UpgradeKind::Legacy;
    }
    let deadline = Instant::now() + UPGRADE_PEEK_TIMEOUT;
    let mut buffer = vec![0u8; UPGRADE_PEEK_MAX];
    let mut seen = 0;
    let kind = loop {
        let Ok(count) = stream.peek(&mut buffer) else {
            break UpgradeKind::Legacy;
        };
        if count == 0 {
            break UpgradeKind::Legacy;
        }
        let head = &buffer[..count];
        if let Some(end) = head.windows(4).position(|w| w == b"\r\n\r\n") {
            break upgrade_kind_of(&head[..end]);
        }
        if count == buffer.len() || Instant::now() >= deadline {
            break UpgradeKind::Legacy;
        }
        if count == seen {
            // Nothing new yet: `peek` returns at once while bytes wait.
            thread::sleep(Duration::from_millis(1));
        }
        seen = count;
    };
    let _ = stream.set_read_timeout(None);
    kind
}

/// `UpgradeKind` from a request head's `X-Tkfs-Sync` header (case-insensitive
/// name, trimmed value), as `session::upgrade_kind` reads a parsed request.
fn upgrade_kind_of(head: &[u8]) -> UpgradeKind {
    let text = String::from_utf8_lossy(head);
    for line in text.split("\r\n").skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.trim().eq_ignore_ascii_case(UPGRADE_HEADER) {
            return match value.trim() {
                UPGRADE_LOOPBACK => UpgradeKind::Loopback,
                UPGRADE_V3 => UpgradeKind::V3,
                _ => UpgradeKind::Legacy,
            };
        }
    }
    UpgradeKind::Legacy
}

/// `Engine::control` ops that change nothing (no status refresh after them).
const READ_ONLY_CONTROL_OPS: &[&str] = &[
    "status",
    "folder-details",
    "preview-content",
    "branches",
    "conflicts",
    "history",
    "state",
    "cat-object",
    "events",
    "pending-saves",
];

/// Legacy entry point (in-process tests and tools): builds a minimal
/// `WorkerCtx` (unsupervised, no previous record), starts the RPC server, the
/// heartbeat, maintenance and notifier threads (no background activator: a
/// `published-*` exchange keeps its legacy bounded activation), and publishes
/// `runtime.json` with `mount` (as given: callers pass an absolute path, as
/// the legacy worker did).
pub fn start_rpc(engine: Shared, state: &Path, mount: Option<String>) -> Result<Discovery> {
    let ctx = crate::worker::build_ctx(engine, state, false, None);
    let server = start_rpc_server(ctx.clone())?;
    // The threads first, as in `worker::run`: nothing is published by a call
    // that then fails.
    crate::worker::heartbeat::spawn(ctx.clone(), server.vitals().clone())?;
    crate::worker::maintenance::spawn(ctx.clone())?;
    super::notify::spawn_notifier(ctx)?;
    server.publish(mount)
}

/// Starts the RPC listener for `ctx` (§4.1 step 6) and returns its
/// discovery (with `mount: None`). Only the instance's token is written
/// (`STATE/runtime.token`, before the listener accepts); the caller
/// publishes `runtime.json` ([`RpcServer::publish`]) and `worker.json`.
pub fn start_rpc_ctx(ctx: Arc<WorkerCtx>) -> Result<Discovery> {
    Ok(start_rpc_server(ctx)?.discovery().clone())
}

/// [`start_rpc_ctx`], returning the handle `worker::run` waits on.
pub fn start_rpc_server(ctx: Arc<WorkerCtx>) -> Result<RpcServer> {
    register_worker(&ctx);
    super::content_writes::NativeWrites::install(
        &ctx.engine,
        &crate::content::ReadContext {
            deadline: Instant::now() + Duration::from_secs(30),
            cancel: crate::util::Cancel::new(),
        },
    )?;
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let vitals = lock_engine_tracked(&ctx.engine).vitals.clone();
    // The first status cell: the counters now (no content scan before
    // Ready); the first `status` reply computes the content unless
    // maintenance has by then.
    crate::worker::status_cell::refresh_cheap(&ctx)?;
    crate::worker::status_cell::want_content(&ctx);
    let discovery = Discovery {
        format: 2,
        address: format!("ws://{address}"),
        state: ctx.state.to_string_lossy().into_owned(),
        mount: None,
        repo: ctx.repo.clone(),
        device: ctx.device.clone(),
    };
    // The instance's local token, on disk before anything can connect. The
    // previous instance's `runtime.json` goes first (WP4.INT re-review): one
    // that is still there names the listener of a worker that did not exit
    // cleanly (the caller holds `owner.lock`, so no live worker owns it), and
    // a local process may listen on that freed port; `runtime::worker_token`
    // pairs `runtime.token` only with the address `runtime.json` names, so
    // the new token must never sit next to the old address. `publish` writes
    // this instance's discovery after.
    remove_stale_discovery(&ctx.state)?;
    let token = crate::ws::tokens::generate();
    crate::worker::record::write_private(
        &crate::worker::record::token_path(&ctx.state),
        token.as_bytes(),
    )?;
    let remembered = crate::ws::tokens::remember(address, &token);
    let core = Arc::new(Core {
        bulk: crate::bulk_sync::Worker {
            root: ctx.root.clone(),
            repo: ctx.repo.clone(),
            device: ctx.device.clone(),
            locks: ctx.locks.clone(),
        },
        ctx,
        vitals,
        discovery,
        address,
        stop: Notify::new(),
        transitions: Mutex::new(()),
        published: Mutex::new(None),
        reply: Mutex::new(StopReplyState::default()),
        reply_flushed: Condvar::new(),
        connections: AtomicU64::new(0),
        preauth: Mutex::new(std::collections::BTreeMap::new()),
        token,
        _remembered: remembered,
    });
    let accept = core.clone();
    thread::Builder::new()
        .name("rpc-accept".into())
        .spawn(move || accept.accept_loop(listener))?;
    Ok(RpcServer { core })
}

/// Removes `STATE/runtime.json` before a new instance writes its token
/// ([`start_rpc_server`]); a missing file is not an error, and one that
/// cannot be removed fails the start (fail closed: its address would be
/// paired with the new token).
fn remove_stale_discovery(state: &Path) -> Result<()> {
    match std::fs::remove_file(state.join("runtime.json")) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(anyhow::Error::new(
            error,
        )
        .context("cannot remove the previous worker's runtime.json before writing the new token")),
        _ => Ok(()),
    }
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
    /// This instance's local token (`STATE/runtime.token`; WP4.1): required
    /// on every JSON RPC and loopback session upgrade.
    pub fn token(&self) -> &str {
        &self.core.token
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
    /// Connections not yet authorized, by connection id (ids grow with each
    /// accept): their sockets, to close the oldest ([`MAX_PREAUTH_CONNECTIONS`]).
    preauth: Mutex<std::collections::BTreeMap<u64, Preauth>>,
    /// The instance's local token (module docs).
    token: String,
    /// Lets clients of this process find the token (`ws::tokens`).
    _remembered: Option<Arc<crate::ws::tokens::Hold>>,
}

/// A connection in its pre-auth phase.
struct Preauth {
    killer: Option<TcpStream>,
    /// Closed for a newer connection; its thread is ending.
    evicted: bool,
}

/// Ends a connection's pre-auth phase when dropped (its slot is free).
struct PreauthSlot<'a> {
    core: &'a Core,
    id: u64,
}

impl Drop for PreauthSlot<'_> {
    fn drop(&mut self) {
        lock(&self.core.preauth).remove(&self.id);
    }
}

/// One handled request: its result, and whether it accepted a stop (the
/// reply must then be flushed before the process exits).
struct Handled {
    result: Result<Value>,
    stop: bool,
}

impl Core {
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
                    self.admit_preauth(id, &stream);
                    let core = self.clone();
                    match thread::Builder::new()
                        .name(format!("rpc-conn-{id}"))
                        .spawn(move || core.serve(id, stream))
                    {
                        Ok(_) => {
                            consecutive = 0;
                            continue;
                        }
                        Err(error) => {
                            lock(&self.preauth).remove(&id);
                            anyhow::Error::from(error)
                        }
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
        let engine = loop {
            match try_lock_engine(&self.ctx.engine) {
                Some(engine) => break Some(engine),
                None if Instant::now() >= deadline => break None,
                None => thread::sleep(Duration::from_millis(10)),
            }
        };
        if !super::exit::begin() {
            drop(engine);
            super::exit::wait();
        }
        let retained_state = engine
            .map(|engine| super::exit::retain(&self.ctx.engine, engine, &super::exit::context()));
        let outcome = retained_state.as_ref().map(|state| &state.outcome);
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
            "engine_busy": retained_state.as_ref().is_none_or(|state| state.engine.is_none()),
            "exit_code": crate::model::ipc::EXIT_INTERNAL,
        });
        // Exit while still holding the engine (if taken).
        std::process::exit(crate::model::ipc::EXIT_INTERNAL)
    }

    /// Registers connection `id` in its pre-auth phase; when
    /// [`MAX_PREAUTH_CONNECTIONS`] are already there, the oldest is closed
    /// (its thread sees its socket shut down and ends).
    fn admit_preauth(&self, id: u64, stream: &TcpStream) {
        let mut preauth = lock(&self.preauth);
        let waiting: Vec<u64> = preauth
            .iter()
            .filter(|(_, slot)| !slot.evicted)
            .map(|(id, _)| *id)
            .collect();
        if waiting.len() >= MAX_PREAUTH_CONNECTIONS
            && let Some(oldest) = waiting.first().and_then(|id| preauth.get_mut(id))
        {
            oldest.evicted = true;
            if let Some(killer) = &oldest.killer {
                let _ = killer.shutdown(std::net::Shutdown::Both);
            }
        }
        preauth.insert(
            id,
            Preauth {
                killer: stream.try_clone().ok(),
                evicted: false,
            },
        );
    }

    /// One connection: its pre-auth phase (the upgrade peek, then the JSON
    /// RPC or loopback session upgrade with the token) within
    /// [`UPGRADE_BUDGET`] (a watchdog; no watchdog, no connection), then a v3
    /// loopback session ([`Core::serve_session`]) or the JSON RPC's
    /// persistent request loop. Notifications are delivered by the notifier
    /// thread, never on the reply path.
    fn serve(self: Arc<Self>, id: u64, stream: TcpStream) {
        let slot = PreauthSlot { core: &self, id };
        let Ok(watchdog) = crate::ws::watchdog::Watchdog::start(&stream) else {
            return;
        };
        watchdog.arm(UPGRADE_BUDGET);
        if peek_upgrade(&stream) == UpgradeKind::Loopback {
            let accepted = session::accept_loopback(stream, Some(&self.token), loopback_options());
            watchdog.disarm();
            let fired = watchdog.fired();
            drop(watchdog);
            drop(slot);
            match accepted {
                // The socket was shut down: even a completed upgrade is
                // unusable.
                Ok(conn) if fired => conn.close(),
                Ok(conn) => self.serve_session(conn),
                Err(error) => {
                    crate::util::log::event!(info, (error.code.as_str()), {
                        "site": "worker.loopback.accept",
                        "message": error.message,
                    });
                }
            }
            return;
        }
        let upgraded = crate::ws::accept_authorized(stream, Some(&self.token));
        watchdog.disarm();
        if watchdog.fired() {
            return;
        }
        drop(watchdog);
        drop(slot);
        let Ok(mut socket) = upgraded else {
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

    /// One loopback session, accepted (`session::accept_loopback`, with the
    /// instance's token): served by its handler pool, closed when the worker
    /// stops.
    fn serve_session(self: Arc<Self>, conn: session::Conn) {
        conn.serve(Arc::new(LoopbackHandler { core: self.clone() }));
        loop {
            if conn.wait_closed(Duration::from_millis(500)).is_some() {
                return;
            }
            if self.ctx.stopping.load(Ordering::Relaxed) {
                conn.close();
                return;
            }
        }
    }

    /// One request of a loopback session (module docs).
    fn session_request(
        &self,
        op: &str,
        header: Value,
        body: Vec<u8>,
        deadline: Instant,
        cancelled: &dyn Fn() -> bool,
    ) -> Result<(Value, Vec<u8>), ErrorWire> {
        let wire = |error: anyhow::Error| ErrorWire::from_error(&error);
        if op.starts_with(ops::OBJECTS_PREFIX) {
            // The JSON ops below reach `worker.rpc.<op>` in `route`, once,
            // as over the JSON RPC.
            fault::trigger(&format!("worker.rpc.{op}")).map_err(wire)?;
            if op == crate::model::protocol::objects::ops::WANT {
                if !body.is_empty() {
                    return Err(ErrorWire::coded(
                        "INVALID_REQUEST",
                        "want body must be empty",
                    ));
                }
                return objects_rpc::want_request(&self.ctx, &header, deadline, cancelled);
            }
            if op == crate::model::content_wire::LEGACY_PREPARE {
                if !body.is_empty() {
                    return Err(ErrorWire::coded(
                        "INVALID_REQUEST",
                        "prepare body must be empty",
                    ));
                }
                return objects_rpc::legacy_prepare_request(
                    &self.ctx, &header, deadline, cancelled,
                );
            }
            if matches!(
                op,
                crate::model::protocol::objects::ops::READ
                    | crate::model::content_wire::LEGACY_READ
            ) {
                if !body.is_empty() {
                    return Err(ErrorWire::coded(
                        "INVALID_REQUEST",
                        "read body must be empty",
                    ));
                }
                return objects_rpc::read_request(&self.ctx, op, &header, deadline, cancelled);
            }
            return objects_rpc::dispatch(&self.ctx, op, &header, &body);
        }
        if op == ops::STOP {
            return Err(ErrorWire::coded(
                "UNSUPPORTED_OPERATION",
                "stop is served on the JSON RPC (its reply is flushed before the exit)",
            ));
        }
        let mut payload = if body.is_empty() {
            header
        } else {
            serde_json::from_slice(&body).map_err(|error| {
                ErrorWire::coded(
                    "INVALID_REQUEST",
                    format!("{op}: body is not JSON: {error}"),
                )
            })?
        };
        let Some(members) = payload.as_object_mut() else {
            return Err(ErrorWire::coded(
                "INVALID_REQUEST",
                format!("{op}: the payload must be a JSON object"),
            ));
        };
        members.insert("op".into(), json!(op));
        // A fresh request id: session ids restart per connection, and request
        // ids key the engine's idempotency receipts.
        let request = json!({"request": crate::core::id(), "payload": payload});
        let mut stop = false;
        let value = if matches!(
            op,
            "preview-content" | "request-content" | "cancel-content-request"
        ) {
            fault::trigger(&format!("worker.rpc.{op}")).map_err(wire)?;
            let result = super::content_inspection::dispatch(
                &self.ctx,
                op,
                &request["payload"],
                deadline,
                cancelled,
            );
            if op != "preview-content" {
                crate::worker::status_cell::note_mutation(&self.ctx, &self.vitals, true);
            }
            result.map_err(wire)?
        } else if op == "bucket-export" {
            fault::trigger("worker.rpc.bucket-export").map_err(wire)?;
            export_bucket(&self.ctx, &request["payload"], deadline, cancelled).map_err(wire)?
        } else {
            self.route(&request, &mut stop).map_err(wire)?
        };
        let text = serde_json::to_vec(&value).map_err(|error| wire(error.into()))?;
        if text.len() <= INLINE_REPLY {
            Ok((value, vec![]))
        } else {
            Ok((json!({"in_body": true}), text))
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
            // `Store::revalidate` (WP1.6), wired by WP1.INT (§14 WP1.INT task 1).
            ops::REVALIDATE => self.revalidate(payload),
            ops::PUBLISHED_SUMMARY => self.published_summary(),
            ops::STATUS => self.status(),
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
            op if op.starts_with("published-") => published_rpc(&self.ctx, &self.vitals, payload),
            "preview-content" | "request-content" | "cancel-content-request" => {
                let result = super::content_inspection::dispatch(
                    &self.ctx,
                    op,
                    payload,
                    Instant::now() + Duration::from_secs(15),
                    &|| false,
                );
                if op != "preview-content" {
                    crate::worker::status_cell::note_mutation(&self.ctx, &self.vitals, true);
                }
                result
            }
            "cat-object" => self.object(payload),
            "import" => {
                let result = self.import_content(request, payload);
                crate::worker::status_cell::note_mutation(&self.ctx, &self.vitals, true);
                result
            }
            "bucket-export" => export_bucket(
                &self.ctx,
                payload,
                Instant::now() + Duration::from_secs(240),
                &|| false,
            ),
            "checkout" => {
                let mounted = self.engine()?.mounted;
                let result = if mounted {
                    crate::mount::checkout(&self.ctx.engine, request, payload)
                } else {
                    self.engine()?.control(request, payload)
                };
                crate::worker::status_cell::note_mutation(&self.ctx, &self.vitals, true);
                result
            }
            // Read details on a secondary snapshot, preserving the worker's
            // feed/status claims and checking checkout on both sides of it.
            "folder-details" => {
                let (root, branch, generation) = {
                    let engine = self.engine()?;
                    if let Some(generation) = payload["generation"].as_u64() {
                        ensure!(generation == engine.store.generation, "STALE_VIEW");
                    }
                    (
                        engine.store.root.clone(),
                        engine.store.active.clone(),
                        engine.store.generation,
                    )
                };
                let mut details = {
                    let store = self.ctx.sync_stores.checkout()?;
                    let snapshot = store.db.unchecked_transaction()?;
                    let identity: (String, Option<String>) = snapshot.query_row(
                        "SELECT (SELECT value FROM config WHERE key='active'),
                         (SELECT value FROM config WHERE key='generation')",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )?;
                    let observed = identity.1.as_deref().unwrap_or("0").parse::<u64>()?;
                    ensure!(
                        store.root == root
                            && store.active == branch
                            && store.generation == generation
                            && identity.0 == branch
                            && observed == generation,
                        "STALE_VIEW"
                    );
                    let offset = usize::try_from(payload["offset"].as_u64().unwrap_or(0))?;
                    let details = super::control::folder_details(&store, offset)?;
                    snapshot.commit()?;
                    details
                };
                let status = self.status()?;
                {
                    let engine = self.engine()?;
                    ensure!(
                        engine.store.root == root
                            && engine.store.active == branch
                            && engine.store.generation == generation,
                        "STALE_VIEW"
                    );
                }
                ensure!(
                    status["generation"].as_u64() == Some(generation),
                    "STALE_VIEW"
                );
                for member in ["downloading", "requested", "partial_bytes"] {
                    details["content"][member] = status["content"][member].clone();
                    details["content_counters"][member] =
                        status["content_counters"][member].clone();
                }
                details["status"] = status;
                details["status"]["content"] = details["content"].take();
                details["status"]["content_counters"] = details["content_counters"].take();
                details["status"]["content_complete"] =
                    details["status"]["content"]["complete"].clone();
                Ok(details)
            }
            op => {
                let read_only = READ_ONLY_CONTROL_OPS.contains(&op);
                let mut engine = if read_only {
                    self.engine()?
                } else {
                    // A mutation needs a quiet engine: give the saves of
                    // files closed just before it their few milliseconds
                    // instead of refusing it (CCR-WP5.2-4).
                    self.engine_after_saves()?
                };
                let result = engine.control(request, payload);
                drop(engine);
                if !read_only {
                    // Journaled mutations and anything unknown: refreshed
                    // before the reply.
                    crate::worker::status_cell::note_mutation(&self.ctx, &self.vitals, true);
                }
                result
            }
        }
    }

    /// Prepare immutable chunks without Engine or a metadata transaction. The
    /// second phase rechecks the selected namespace before committing its receipt.
    fn import_content(&self, request: &str, payload: &Value) -> Result<Value> {
        let fingerprint = hash(&serde_json::to_vec(payload)?);
        let path = payload["path"].as_str().context("MISSING_ARGUMENT")?;
        let context = super::content_reads::request_context(
            &self.ctx.engine,
            super::content::ContentWait::fixed(Duration::from_secs(240)),
        )?;
        let (root, branch, generation, basis) = {
            let engine = self.engine_after_saves()?;
            if let Some(reply) = engine.store.receipt_fingerprint(request, &fingerprint)? {
                return Ok(reply);
            }
            if let Some(generation) = payload["generation"].as_u64() {
                ensure!(generation == engine.store.generation, "STALE_VIEW");
            }
            engine.quiet()?;
            engine.store.check_content_write()?;
            (
                engine.store.root.clone(),
                engine.store.active.clone(),
                engine.store.generation,
                super::control::import_basis(&engine.store, path)?,
            )
        };
        let bytes = hex::decode(payload["hex"].as_str().context("MISSING_ARGUMENT")?)?;
        context.check()?;
        fault::trigger("content.import.before_prepare")?;
        let local = crate::content::LocalContent::open(&root)?;
        let prepared = local.prepare_import(&mut bytes.as_slice(), bytes.len() as u64, &context)?;
        fault::trigger("content.import.prepared")?;
        context.check()?;
        let mut engine = self.engine_after_saves()?;
        if let Some(reply) = engine.store.receipt_fingerprint(request, &fingerprint)? {
            return Ok(reply);
        }
        ensure!(
            engine.store.root == root
                && engine.store.active == branch
                && engine.store.generation == generation,
            "STALE_VIEW"
        );
        ensure!(
            super::control::import_basis(&engine.store, path)? == basis,
            "STALE_VIEW"
        );
        engine.control_prepared(request, payload, Some(&prepared), &fingerprint)
    }

    /// The engine for an RPC. `engine.poison` fires here: with
    /// `TKFS_FAULT_ACTION=panic` it panics while holding the lock (poisoning
    /// it), `hang` holds it, `fail` fails the request.
    fn engine(&self) -> Result<EngineGuard<'_>> {
        let guard = lock_engine_tracked(&self.ctx.engine);
        fault::trigger("engine.poison")?;
        Ok(guard)
    }

    /// [`Self::engine`] once the saves of closed files have ended, if they
    /// are all that keeps the engine from being quiet (at most
    /// `engine::QUIET_SAVES_WAIT`; CCR-WP5.2-4).
    fn engine_after_saves(&self) -> Result<EngineGuard<'_>> {
        let guard =
            super::engine::lock_after_saves(&self.ctx.engine, super::engine::QUIET_SAVES_WAIT);
        fault::trigger("engine.poison")?;
        Ok(guard)
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
                if reply_follows {
                    // The saves of files closed just before the stop end
                    // first, instead of a `BUSY_VIEW` refusal the controller
                    // retries 2 s later (CCR-WP5.2-4).
                    super::engine::lock_after_saves(
                        &self.ctx.engine,
                        super::engine::QUIET_SAVES_WAIT,
                    )
                    .quiet()?;
                } else {
                    engine()?.quiet()?;
                }
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
                    drop(e);
                    let native = super::content_writes::retain_dirty(
                        &self.ctx.engine,
                        &crate::content::ReadContext {
                            deadline: Instant::now() + super::engine::QUIET_SAVES_WAIT,
                            cancel: crate::util::Cancel::new(),
                        },
                    );
                    let mut e = engine()?;
                    if let Err(error) = native {
                        e.switching = fenced;
                        return Err(error);
                    }
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
        super::content_writes::stop_worker(&self.ctx.engine);
        super::content_rpc::stop(&self.ctx);
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

    /// `revalidate{ids?}` (§6.1, wired by WP1.INT): re-queue quarantined,
    /// blocked and retry inbox rows (all, or `ids` and the rows blocked on
    /// them) through `Store::revalidate`, then activate what is now ready in
    /// one bounded pass, as a legacy lane's receive does, so the reply
    /// reflects that pass. While the background activator runs, the pass is
    /// the activator's: the request wakes it and waits (without the engine,
    /// at most [`REVALIDATE_WAIT`]) for one activator cycle that began after
    /// the re-queue and ran its slice (a cycle whose slice found SQLite's
    /// writer busy did nothing and does not count; re-queued rows keep their
    /// arrival order, so they are at the head of that cycle's slice), and
    /// nothing is activated under the
    /// request's own engine hold (§4.4: a slow event is prepared off-lock,
    /// never committed inline on a request). Without the activator (the
    /// in-process `start_rpc` wrapper) the request activates inline (≤ 32
    /// events, ≤ 50 ms, with the engine's namespace notifications and
    /// session refresh); whatever is left activates on the next receive. The
    /// rows are re-queued durably before the activation, so an activation
    /// failure (or a wait that ran out) is logged, not returned.
    fn revalidate(&self, payload: &Value) -> Result<Value> {
        let RevalidateRequest { ids } = serde_json::from_value(payload.clone())?;
        let background = self.ctx.activator_running.load(Ordering::SeqCst);
        let requeued = {
            let mut engine = self.engine()?;
            let requeued = engine.store.revalidate(ids.as_deref())?;
            if requeued > 0
                && !background
                && let Err(error) =
                    engine.activate_bounded(REVALIDATE_ACTIVATION_EVENTS, REVALIDATE_ACTIVATION_MS)
            {
                crate::util::log::event!(info, (crate::model::errors::code_of(&error)), {
                    "site": "worker.revalidate",
                    "requeued": requeued,
                    "message": format!("{error:#}"),
                });
            }
            requeued
        };
        // Read after the re-queue committed: a cycle numbered past `begun`
        // loads its slice after it.
        let (begun, _) = self.vitals.activator_cycles();
        self.ctx.activation.poke();
        if requeued > 0 && background {
            let deadline = Instant::now() + REVALIDATE_WAIT;
            let mut polls = 0u32;
            while self.vitals.activator_cycles().1 <= begun
                && self.ctx.activator_running.load(Ordering::SeqCst)
            {
                // A cycle numbered before `begun` was read can still run the
                // slice that activates the re-queued rows; the activator
                // then idles without numbering a cycle. So every 10 ms the
                // wait also ends once no row is left to activate (WP2.3
                // review minor, fixed by WP2.INT).
                polls += 1;
                if polls.is_multiple_of(10)
                    && matches!(crate::worker::activator::has_work(&self.ctx), Ok(false))
                {
                    break;
                }
                if Instant::now() >= deadline {
                    crate::util::log::event!(info, REQUEST_TIMEOUT, {
                        "site": "worker.revalidate",
                        "requeued": requeued,
                        "message": "the activator has not activated the re-queued rows yet",
                    });
                    break;
                }
                thread::sleep(Duration::from_millis(1));
            }
        }
        if requeued > 0 {
            crate::worker::status_cell::note_mutation(&self.ctx, &self.vitals, true);
        }
        Ok(json!(RevalidateReply {
            requeued: requeued as u64
        }))
    }

    /// `published-summary` from a pooled secondary store (no engine lock).
    fn published_summary(&self) -> Result<Value> {
        let store = self.ctx.sync_stores.checkout()?;
        let branches = store
            .branches()?
            .into_iter()
            .filter(|branch| branch.shared)
            .collect();
        Ok(json!(PublishedSummary {
            repo: self.ctx.repo.clone(),
            replica: self.ctx.device.clone(),
            // The worker does not know its catalog label: its repository id,
            // as the previous release published. The orchestrator publishes
            // the catalog label instead (CCR-WP4.3-2).
            label: self.ctx.repo.clone(),
            branches,
            // The feed tip (WP2.5): shared events only, so the remote catalog
            // never counts private ones (CCR-WP2.5-5, wired by WP2.INT).
            tip: FeedTip {
                epoch: store.feed_epoch()?,
                seq: store.feed_tip()?,
            },
            sync_generation: crate::worker::status_cell::sync_generation(&self.ctx, &self.vitals),
        }))
    }

    /// The status reply (§6.1, WP2.3 task 4): the status cell plus the
    /// engine's lock-free vitals (`worker::status_cell::reply`), and the typed
    /// `mount`, `store_health` and `heartbeat_seq` members. Never takes the
    /// engine lock and never answers `WORKER_BUSY`.
    fn status(&self) -> Result<Value> {
        let mut status = crate::worker::status_cell::reply(&self.ctx, &self.vitals)?;
        status["mount"] = json!(self.ctx.mount.status());
        status["store_health"] = json!(self.vitals.store_health());
        status["heartbeat_seq"] = json!(self.ctx.heartbeat.load().seq);
        Ok(status)
    }

    fn object(&self, payload: &Value) -> Result<Value> {
        let object = payload["object"].as_str().context("MISSING_ARGUMENT")?;
        let mut file = fetch_verified(
            &self.ctx.engine,
            object,
            Instant::now() + Duration::from_secs(15),
        )?;
        use std::io::Seek;
        file.rewind()?;
        let total = file.metadata()?.len();
        ensure!(total <= MAX_FILE as u64, "OBJECT_REQUIRES_STREAMING");
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(json!({"hex":hex::encode(bytes)}))
    }
}

/// Serves the requests of one loopback session on its handler pool.
struct LoopbackHandler {
    core: Arc<Core>,
}

impl Handler for LoopbackHandler {
    fn handle(&self, req: Incoming) {
        let Incoming {
            op,
            header,
            body,
            chunks,
            deadline,
            responder,
            ..
        } = req;
        if chunks.is_some() {
            responder.error(ErrorWire::coded(
                "INVALID_REQUEST",
                format!("{op}: worker ops take no streamed body"),
            ));
            return;
        }
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            self.core
                .session_request(&op, header, body, deadline, &|| responder.cancelled())
        }));
        match outcome {
            Ok(Ok((header, body))) => responder.reply(header, body),
            Ok(Err(error)) => responder.error(error),
            Err(panic) => {
                check_engine_poison(&self.core.ctx.engine);
                responder.error(ErrorWire::coded(
                    "UNKNOWN_ERROR",
                    format!(
                        "request handler panicked: {}",
                        panic_message(panic.as_ref())
                    ),
                ));
            }
        }
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
