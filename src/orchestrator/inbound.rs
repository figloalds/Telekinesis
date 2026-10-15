//! Owned by WP2.8 — the inbound v3 server (REVAMP_PLAN §3.8).
//!
//! The serving side of the peer protocol: bounded, authenticated and
//! observable. Built standalone in Wave 2; since WP3.1 the daemon runs it on
//! its network listener ([`start`], from `run::serve`) and stops it with
//! [`InboundServer::shutdown`] (through `ctx.network`'s shutdown).
//!
//! **Bounds (invariant 22).** One `net-accept` thread accepts. At most
//! [`InboundOptions::max_preauth`] (32) connections are in the pre-auth
//! handshake at once; a connection beyond that is closed at once, before
//! anything is read from it. Each handshake message is ≤ 16 KiB and each step
//! ≤ 5 s (`session::handshake`; tungstenite also refuses an upgrade request
//! above 64 KiB), so a slot is always given back. At most
//! [`InboundOptions::max_sessions`] (128) sessions exist: the handshake
//! reserves a session slot when it admits the dialer's `HELLO` (before
//! `AUTH`), and the dialer beyond the cap is told
//! `GOAWAY{TOO_MANY_SESSIONS}`. A failed handshake gives its slot back.
//!
//! **Policy.** [`CatalogAcceptPolicy`] answers the handshake from the
//! catalog snapshot (O(1)): our installation (self-pair refusal), the
//! verifier and its generation, and revocation (`peer_meta.revoked_ms`,
//! which nothing sets before WP4.1).
//!
//! **Sessions.** Each session gets the handler pool of its role (4 threads for
//! control and probe sessions, 2 for bulk, queue 16; a full queue answers
//! `BUSY`, session layer). Requests are checked, in order: the daemon is not
//! stopping (`ORCHESTRATOR_STOPPING`), the session's verifier generation is
//! still current (`PAIRING_CODE_CHANGED`), the peer is not revoked
//! (`PEER_REVOKED`), and the op is allowed on the session's role
//! (`model::protocol::op_allowed`: `meta.exchange` and `catalog.list` off a
//! control (or, for `catalog.list`, probe) session → `METADATA_ON_BULK_LANE`;
//! `objects.*` off a bulk session → `BULK_ON_CONTROL_LANE`). Then:
//!
//! | Op | Handling |
//! |---|---|
//! | `ping` | `{alive:true}` (the session's I/O thread answers it first) |
//! | `catalog.list` | `PublishedCatalogApi::list()`, O(1), never a worker; `unchanged:true` when `since` is the generation |
//! | `meta.exchange` | route `{peer: authenticated installation, repo, local_replica: folder device, remote_replica: request.sender}` → worker `feed-exchange` |
//! | `objects.*` | [`super::inbound_objects::handle`] (lane check, interim `objects.exchange`) |
//! | anything else | `UNKNOWN_PEER_OPERATION` |
//!
//! A folder-addressed op on a folder whose sync is paused answers
//! `FOLDER_SYNC_PAUSED`, on a folder that is not Running
//! `REMOTE_WORKER_STARTING`, on a repo this daemon does not manage
//! `REPOSITORY_NOT_MANAGED`; none of them reaches a worker.
//!
//! **Exchanges never hold a handler.** `meta.exchange` and `objects.*` run on
//! a thread of their own, at most as many per session as its role has handler
//! threads and at most [`InboundOptions::max_exchanges`] server-wide (beyond
//! either: `BUSY`, Contention). An exchange keeps its slots until its worker
//! call returns, also after its session is gone, so redialing cannot pile up
//! worker calls. The handler pool therefore only ever runs the O(1) checks
//! and `catalog.list`, which answer at once even while every exchange of the
//! session waits on a hung worker (RC8). Status is recorded before the reply.
//!
//! **Wire shapes.** `catalog.list` answers its
//! [`CatalogListReply`](crate::model::protocol::CatalogListReply) as the
//! response header; a catalog too large for a header (64 KiB) is sent as the
//! JSON body instead, with the header `{generation, unchanged:false,
//! in_body:true}` (no `entries`, so a reader that parses only the header
//! fails instead of reading an empty catalog). `meta.exchange` takes the header `{repo}` and
//! the JSON `MetaExchangeRequest` body, and answers the header `{repo}` and
//! the JSON `MetaExchangeReply` body.
//!
//! **Rotation.** A watcher thread follows the catalog. When the verifier
//! generation changes, every session authenticated under another generation
//! gets `GOAWAY{PAIRING_CODE_CHANGED}` (within the catalog's wake-up, well
//! under 1 s); a revoked peer's sessions get `GOAWAY{PEER_REVOKED}`. A
//! session that completes its handshake while the verifier rotates is
//! checked when it registers, under the same lock the watcher scans with, so
//! it cannot slip through.
//!
//! **Visibility (RC12).** `InboundSession{opened}` when a control or bulk
//! session starts and ends (probe sessions are short-lived pairing and
//! listing sessions and are not reported as lanes); `ExchangeFinished
//! {direction: inbound}` for every exchange a worker served (ok or failed,
//! with the worker's own `FeedApplyResult` summary for `meta.exchange`);
//! `InboundIncompatible` for a legacy (v2) hello or an unsupported protocol
//! version. A served `meta.exchange` records `ExchangeStarted{direction:
//! inbound}` before its worker call (CCR-WP2.8-3, switched on by WP3.1: the
//! hub books it on the inbound lane, so an inbound lane shows the exchange
//! in flight). A served `meta.exchange` also sends the folder its gate
//! evidence from the acceptor's own view (`FolderCmd::GateEvidence`).
use crate::{
    model::{
        errors::ErrorWire,
        ipc::{self, FeedExchangeReply, FeedExchangeRequest},
        orchestrator::{
            CallError, CatalogSnapshot, Ctx, FolderCmd, FolderPhase, FolderView, PublishedEntry,
            WorkerClient, limits,
        },
        protocol::{
            self, CatalogListRequest, LaneRole, MetaExchangeHeader,
            feed::{ExchangeSummary, MetaExchangeRequest},
            frame::{ClientHello, Limits},
            op_allowed, op_role, ops,
        },
        status::{Direction, StatusEvent},
    },
    paired_sync::SyncRoute,
    pairing_code::Verifier,
    session::{self, AcceptPolicy, AcceptedPeer, Conn, ConnOptions, Handler, Incoming, Side},
    util::{cancel::Cancel, lock::Tracked, log::event, notify::Notify},
};
use polling::{Event as Readiness, Events, Poller};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io,
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// The listener's key in the accept poller.
const LISTENER_KEY: usize = 0;
/// Pause after an accept error (for example out of descriptors), so a
/// listener whose backlog stays readable does not spin.
const ACCEPT_ERROR_PAUSE: Duration = Duration::from_millis(100);
/// How long the rotation watcher waits for a catalog change before it
/// re-checks cancellation. A change wakes it at once.
const WATCH_WAIT: Duration = Duration::from_millis(250);
/// What a worker call leaves of the peer's deadline for the reply to travel.
const REPLY_MARGIN: Duration = Duration::from_millis(250);
/// A worker call needs at least this much time to be worth starting.
const MIN_WORKER_DEADLINE: Duration = Duration::from_millis(50);
/// `retry_after_ms` of a `BUSY` refusal.
const BUSY_RETRY_MS: u64 = 50;
/// A `catalog.list` reply above this many header bytes goes in the body
/// (the response header bound is 64 KiB, framing included).
const CATALOG_HEADER_MAX: usize = protocol::frame::MAX_HEADER - 1024;
/// Longest peer-chosen text (a legacy hello's installation) put in status.
const MAX_PEER_TEXT: usize = 64;

/// Limits of the inbound server. The defaults are §3.8's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InboundOptions {
    /// Concurrent pre-auth handshakes (32).
    pub max_preauth: usize,
    /// Sessions, counting the ones whose handshake has admitted the dialer
    /// (128).
    pub max_sessions: usize,
    /// Worker-bound exchanges running at once, server-wide (128). Each
    /// session also runs at most as many as its role has handler threads.
    /// An exchange keeps its slot until its worker call returns, also after
    /// its session went away, so session churn cannot grow the threads.
    pub max_exchanges: usize,
    /// Base session options. Each session runs with the handler pool of its
    /// role (`limits::CONTROL_HANDLERS`/`BULK_HANDLERS`, queue
    /// `limits::HANDLER_QUEUE`); the bounds announced in `HELLO_ACK` come
    /// from here.
    pub conn: ConnOptions,
}

impl Default for InboundOptions {
    fn default() -> Self {
        Self {
            max_preauth: limits::MAX_PREAUTH,
            max_sessions: limits::MAX_SESSIONS,
            max_exchanges: limits::MAX_SESSIONS,
            conn: ConnOptions::default(),
        }
    }
}

/// Counters of the inbound server (diagnostics and tests).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct InboundStats {
    /// TCP connections accepted.
    pub accepted: u64,
    /// Closed at once: every pre-auth slot was taken.
    pub refused_preauth: u64,
    /// Refused with `TOO_MANY_SESSIONS`.
    pub refused_sessions: u64,
    /// Handshakes that ended without a session (any reason).
    pub handshake_failures: u64,
    /// Legacy (v2) hellos answered with `PEER_NEEDS_UPGRADE`.
    pub legacy_refused: u64,
    /// Sessions established since start.
    pub sessions_opened: u64,
    /// `GOAWAY` sent for a verifier rotation or a revocation.
    pub rotation_goaways: u64,
    /// Requests refused `BUSY` because the session's (or the server's)
    /// exchange slots were full.
    pub busy_refusals: u64,
    /// Worker-bound exchanges running now (server-wide).
    pub exchanges: usize,
    /// Sessions now (including admitted handshakes).
    pub sessions: usize,
    /// Pre-auth handshakes now.
    pub preauth: usize,
    /// Most concurrent pre-auth handshakes seen.
    pub preauth_peak: usize,
    /// Most concurrent sessions seen (including admitted handshakes).
    pub sessions_peak: usize,
}

/// One established session (no secrets).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct InboundSessionInfo {
    pub installation: String,
    pub role: LaneRole,
    #[serde(default)]
    pub lane: Option<protocol::LaneSpec>,
    /// The status lane: 0 for control and probe sessions, `index + 1` for bulk.
    pub status_lane: u32,
    pub session: String,
    pub address: String,
    pub verifier_generation: u64,
    pub opened_ms: u64,
    /// The `GOAWAY` code sent to it, if any.
    #[serde(default)]
    pub goaway: Option<String>,
}

/// The running inbound server; see the module docs.
pub struct InboundServer {
    inner: Arc<Inner>,
    threads: Mutex<Vec<(&'static str, JoinHandle<()>)>>,
}

/// Starts serving `listener` with the §3.8 limits. Never fails: a server
/// whose accept loop cannot start logs why and serves nothing.
pub fn start(ctx: Arc<Ctx>, listener: TcpListener) -> Arc<InboundServer> {
    start_with(ctx, listener, InboundOptions::default())
}

/// [`start`] with explicit limits (tests, diagnostics).
pub fn start_with(
    ctx: Arc<Ctx>,
    listener: TcpListener,
    options: InboundOptions,
) -> Arc<InboundServer> {
    let cancel = ctx.shutdown.child();
    let poller = match Poller::new() {
        Ok(poller) => Some(Arc::new(poller)),
        Err(error) => {
            event!(error, WS_CLOSED, {
                "detail": "inbound accept poller could not start; serving nothing",
                "message": error.to_string(),
            });
            None
        }
    };
    let inner = Arc::new(Inner {
        local_addr: listener.local_addr().ok(),
        options: InboundOptions {
            conn: options.conn.sanitized(),
            ..options
        },
        cancel,
        poller: poller.clone(),
        state: Tracked::new(
            "inbound.state",
            State {
                preauth: 0,
                reserved: 0,
                sessions: BTreeMap::new(),
                stopping: None,
            },
        ),
        changed: Notify::new(),
        counters: Counters::default(),
        next_session: AtomicU64::new(1),
        exchanges: AtomicUsize::new(0),
        ctx,
    });
    if let Some(poller) = &poller {
        let poller = poller.clone();
        inner.cancel.on_cancel(Box::new(move || {
            let _ = poller.notify();
        }));
    }
    let server = Arc::new(InboundServer {
        inner: inner.clone(),
        threads: Mutex::new(Vec::new()),
    });
    // Read before any session can register: a rotation after this point is
    // a change the watcher sees.
    let generation = inner.ctx.catalog.snapshot().generation;
    let mut threads = Vec::new();
    if let Some(poller) = poller {
        let accept = inner.clone();
        match thread::Builder::new()
            .name("net-accept".into())
            .spawn(move || accept.accept_loop(listener, poller))
        {
            Ok(handle) => threads.push(("net-accept", handle)),
            Err(error) => {
                event!(error, WS_CLOSED, {
                    "detail": "inbound accept thread could not start; serving nothing",
                    "message": error.to_string(),
                });
            }
        }
    }
    let watcher = inner.clone();
    match thread::Builder::new()
        .name("net-rotation".into())
        .spawn(move || watcher.watch_catalog(generation))
    {
        Ok(handle) => threads.push(("net-rotation", handle)),
        Err(error) => {
            event!(error, WS_CLOSED, {
                "detail": "inbound rotation watcher could not start",
                "message": error.to_string(),
            });
        }
    }
    *server
        .threads
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = threads;
    server
}

impl InboundServer {
    /// Stops accepting, sends `GOAWAY{ORCHESTRATOR_STOPPING}` to every
    /// session, waits up to `deadline` for the sessions, the handshakes and
    /// the worker calls in flight to end, then closes the sessions that are
    /// left. Idempotent.
    pub fn shutdown(&self, deadline: Duration) {
        let until = Instant::now().checked_add(deadline);
        let inner = &self.inner;
        inner.begin_stop(stopping_error());
        inner.cancel.cancel();
        let threads =
            std::mem::take(&mut *self.threads.lock().unwrap_or_else(PoisonError::into_inner));
        for (_, handle) in threads {
            // Both threads leave within `WATCH_WAIT` of the cancel.
            let _ = handle.join();
        }
        let remaining = |until: Option<Instant>| {
            until.map_or(Duration::from_secs(3600), |until| {
                until.saturating_duration_since(Instant::now())
            })
        };
        let mut seen = inner.changed.count();
        loop {
            // An admitted handshake (`reserved`) either registers, and is
            // sent away at once, or gives its slot back: wait for it too.
            // So are worker calls of exchanges whose session is gone.
            let (sessions, preauth) = {
                let state = inner.state.lock();
                (state.sessions.len() + state.reserved, state.preauth)
            };
            let exchanges = inner.exchanges.load(Ordering::Acquire);
            if sessions == 0 && preauth == 0 && exchanges == 0 {
                return;
            }
            let left = remaining(until);
            if left.is_zero() {
                break;
            }
            seen = inner.changed.wait(seen, left);
        }
        // The deadline passed: close whatever is left, then give the I/O
        // threads a moment to release their sockets.
        let conns: Vec<Conn> = inner
            .state
            .lock()
            .sessions
            .values()
            .map(|session| session.conn.clone())
            .collect();
        for conn in &conns {
            conn.close();
        }
        for conn in &conns {
            conn.wait_closed(Duration::from_millis(200));
        }
    }

    /// The address the listener was bound to.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.inner.local_addr
    }

    pub fn stats(&self) -> InboundStats {
        let (sessions, preauth) = {
            let state = self.inner.state.lock();
            (state.sessions.len() + state.reserved, state.preauth)
        };
        let c = &self.inner.counters;
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        InboundStats {
            accepted: load(&c.accepted),
            refused_preauth: load(&c.refused_preauth),
            refused_sessions: load(&c.refused_sessions),
            handshake_failures: load(&c.handshake_failures),
            legacy_refused: load(&c.legacy_refused),
            sessions_opened: load(&c.sessions_opened),
            rotation_goaways: load(&c.rotation_goaways),
            busy_refusals: load(&c.busy_refusals),
            exchanges: self.inner.exchanges.load(Ordering::Acquire),
            sessions,
            preauth,
            preauth_peak: c.preauth_peak.load(Ordering::Relaxed),
            sessions_peak: c.sessions_peak.load(Ordering::Relaxed),
        }
    }

    /// The established sessions, oldest first.
    pub fn sessions(&self) -> Vec<InboundSessionInfo> {
        self.inner
            .state
            .lock()
            .sessions
            .values()
            .map(Session::info)
            .collect()
    }
}

impl Drop for InboundServer {
    /// A dropped server stops accepting and lets its sessions go (no wait).
    fn drop(&mut self) {
        self.inner.begin_stop(stopping_error());
        self.inner.cancel.cancel();
    }
}

impl std::fmt::Debug for InboundServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InboundServer")
            .field("local_addr", &self.inner.local_addr)
            .field("stats", &self.stats())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Server state

struct Inner {
    ctx: Arc<Ctx>,
    options: InboundOptions,
    local_addr: Option<SocketAddr>,
    /// Child of `ctx.shutdown`; cancelled by `shutdown` (or with the daemon).
    cancel: Cancel,
    /// Wakes the accept loop (`notify` on cancel).
    poller: Option<Arc<Poller>>,
    /// Map operations only; never held across I/O.
    state: Tracked<State>,
    /// Poked whenever a session or handshake ends (shutdown waits on it).
    changed: Notify,
    counters: Counters,
    next_session: AtomicU64,
    /// Worker-bound exchanges running now, server-wide.
    exchanges: AtomicUsize,
}

struct State {
    /// Connections in the pre-auth handshake.
    preauth: usize,
    /// Session slots admitted by a handshake that has not registered yet.
    reserved: usize,
    sessions: BTreeMap<u64, Session>,
    /// Set once the server stops: new sessions are told this and go away.
    stopping: Option<ErrorWire>,
}

#[derive(Default)]
struct Counters {
    accepted: AtomicU64,
    refused_preauth: AtomicU64,
    refused_sessions: AtomicU64,
    handshake_failures: AtomicU64,
    legacy_refused: AtomicU64,
    sessions_opened: AtomicU64,
    rotation_goaways: AtomicU64,
    busy_refusals: AtomicU64,
    preauth_peak: AtomicUsize,
    sessions_peak: AtomicUsize,
}

struct Session {
    conn: Conn,
    peer: AcceptedPeer,
    address: SocketAddr,
    opened_ms: u64,
    goaway: Option<String>,
}

impl Session {
    fn info(&self) -> InboundSessionInfo {
        InboundSessionInfo {
            installation: self.peer.installation.clone(),
            role: self.peer.role,
            lane: self.peer.lane,
            status_lane: status_lane(&self.peer),
            session: self.peer.session.clone(),
            address: self.address.to_string(),
            verifier_generation: self.peer.verifier_generation,
            opened_ms: self.opened_ms,
            goaway: self.goaway.clone(),
        }
    }
}

fn stopping_error() -> ErrorWire {
    ErrorWire::coded("ORCHESTRATOR_STOPPING", "the orchestrator is shutting down")
}

fn rotation_error() -> ErrorWire {
    ErrorWire::coded(
        "PAIRING_CODE_CHANGED",
        "this orchestrator's pairing code changed; pair again with the new code",
    )
}

fn revoked_error() -> ErrorWire {
    ErrorWire::coded("PEER_REVOKED", "this peer was revoked")
}

/// The lane a session is reported on: 0 for control (and probe) sessions,
/// `index + 1` for bulk lanes (lane 0 of a pool is its control session).
pub(crate) fn status_lane(peer: &AcceptedPeer) -> u32 {
    match peer.role {
        LaneRole::Bulk => peer.lane.map_or(1, |lane| lane.index.saturating_add(1)),
        LaneRole::Control | LaneRole::Probe => 0,
    }
}

/// Whether the catalog no longer accepts the generation `peer` authenticated
/// with (a rotation, or the verifier is gone).
fn rotated(snapshot: &CatalogSnapshot, peer: &AcceptedPeer) -> bool {
    snapshot.verifier.is_none() || snapshot.verifier_generation != peer.verifier_generation
}

fn revoked(snapshot: &CatalogSnapshot, installation: &str) -> bool {
    snapshot
        .peer_meta
        .get(installation)
        .is_some_and(|meta| meta.revoked_ms.is_some())
}

impl Inner {
    fn now_ms(&self) -> u64 {
        self.ctx.clock.now_ms()
    }

    fn record(&self, event: StatusEvent) {
        self.ctx.status.record(event);
    }

    fn session_options(&self, role: LaneRole) -> ConnOptions {
        let mut options = self.options.conn;
        options.handlers = match role {
            LaneRole::Bulk => limits::BULK_HANDLERS,
            LaneRole::Control | LaneRole::Probe => limits::CONTROL_HANDLERS,
        };
        options.handler_queue = limits::HANDLER_QUEUE;
        options
    }

    /// The limits announced in `HELLO_ACK`.
    fn hello_limits(&self) -> Limits {
        let conn = self.options.conn;
        Limits {
            max_header: conn.max_header as u64,
            max_chunk: conn.max_chunk as u64,
            max_body: conn.max_body as u64,
        }
    }

    // -- accept ------------------------------------------------------------

    fn accept_loop(self: Arc<Self>, listener: TcpListener, poller: Arc<Poller>) {
        if let Err(error) = listener.set_nonblocking(true) {
            event!(error, WS_CLOSED, {
                "detail": "inbound listener could not switch to non-blocking; serving nothing",
                "message": error.to_string(),
            });
            return;
        }
        // SAFETY: the listener is deleted from the poller below, before it
        // is dropped at the end of this function.
        if let Err(error) = unsafe { poller.add(&listener, Readiness::readable(LISTENER_KEY)) } {
            event!(error, WS_CLOSED, {
                "detail": "inbound listener could not be polled; serving nothing",
                "message": error.to_string(),
            });
            return;
        }
        let mut events = Events::new();
        'serve: while !self.cancel.is_cancelled() {
            events.clear();
            if let Err(error) = poller.wait(&mut events, None)
                && error.kind() != io::ErrorKind::Interrupted
            {
                event!(warn, WS_CLOSED, {
                    "detail": "inbound accept poll failed",
                    "message": error.to_string(),
                });
                self.cancel.sleep(ACCEPT_ERROR_PAUSE);
            }
            loop {
                if self.cancel.is_cancelled() {
                    break 'serve;
                }
                match listener.accept() {
                    Ok((stream, address)) => self.on_connection(stream, address),
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    // That connection is gone already (reset or aborted
                    // before we took it): take the next one at once.
                    Err(error) if accept_error_is_per_connection(&error) => {}
                    Err(error) => {
                        // Out of descriptors and the like: the connection
                        // stays in the backlog; pause rather than spin.
                        event!(warn, WS_CLOSED, {
                            "detail": "inbound accept failed",
                            "message": error.to_string(),
                        });
                        self.cancel.sleep(ACCEPT_ERROR_PAUSE);
                        break;
                    }
                }
            }
            if let Err(error) = poller.modify(&listener, Readiness::readable(LISTENER_KEY)) {
                event!(error, WS_CLOSED, {
                    "detail": "inbound listener could not be re-armed; serving nothing",
                    "message": error.to_string(),
                });
                break;
            }
        }
        let _ = poller.delete(&listener);
    }

    /// Takes a pre-auth slot for `stream` and runs its handshake on a thread
    /// of its own, or closes it at once when every slot is taken.
    fn on_connection(self: &Arc<Self>, stream: TcpStream, address: SocketAddr) {
        self.counters.accepted.fetch_add(1, Ordering::Relaxed);
        {
            let mut state = self.state.lock();
            if state.stopping.is_some() {
                return;
            }
            if state.preauth >= self.options.max_preauth {
                drop(state);
                self.counters
                    .refused_preauth
                    .fetch_add(1, Ordering::Relaxed);
                // Dropping the stream closes it; nothing was read.
                return;
            }
            state.preauth += 1;
            self.counters
                .preauth_peak
                .fetch_max(state.preauth, Ordering::Relaxed);
        }
        let inner = self.clone();
        let spawned = thread::Builder::new()
            .name("net-conn".into())
            .spawn(move || inner.serve_connection(stream, address));
        if let Err(error) = spawned {
            // The closure (and the stream) is dropped: the connection closes.
            self.end_preauth();
            event!(warn, WS_CLOSED, {
                "detail": "inbound connection thread could not start; connection closed",
                "message": error.to_string(),
            });
        }
    }

    fn end_preauth(&self) {
        {
            let mut state = self.state.lock();
            state.preauth = state.preauth.saturating_sub(1);
        }
        self.changed.poke();
    }

    /// Reserves a session slot for an admitted handshake.
    fn reserve(&self) -> Result<(), ErrorWire> {
        let mut state = self.state.lock();
        let used = state.sessions.len() + state.reserved;
        if used >= self.options.max_sessions {
            drop(state);
            self.counters
                .refused_sessions
                .fetch_add(1, Ordering::Relaxed);
            return Err(ErrorWire::coded(
                "TOO_MANY_SESSIONS",
                format!(
                    "this orchestrator already serves {used} sessions (limit {})",
                    self.options.max_sessions
                ),
            ));
        }
        state.reserved += 1;
        self.counters
            .sessions_peak
            .fetch_max(used + 1, Ordering::Relaxed);
        Ok(())
    }

    fn release_reservation(&self) {
        {
            let mut state = self.state.lock();
            state.reserved = state.reserved.saturating_sub(1);
        }
        self.changed.poke();
    }

    /// The handshake, then (on success) the session's life: this thread
    /// registers it, serves it and unregisters it when it closes. Every slot
    /// it holds is a guard, so even a panic gives it back.
    fn serve_connection(self: Arc<Self>, stream: TcpStream, address: SocketAddr) {
        let preauth = PreauthSlot(&self);
        let policy = ConnPolicy {
            policy: CatalogAcceptPolicy::new(self.ctx.clone()),
            inner: self.clone(),
            reserved: AtomicBool::new(false),
        };
        let accepted = session::accept_v3(stream, &policy, self.hello_limits());
        drop(preauth);
        let (ws, peer) = match accepted {
            Ok(accepted) => accepted,
            Err(error) => {
                drop(policy);
                self.handshake_failed(address, &error);
                return;
            }
        };
        let conn = match Conn::from_established(
            ws,
            Side::Server,
            peer.clone(),
            self.session_options(peer.role),
        ) {
            Ok(conn) => conn,
            Err(error) => {
                drop(policy);
                self.counters
                    .handshake_failures
                    .fetch_add(1, Ordering::Relaxed);
                event!(warn, CONNECTION_LOST, {
                    "detail": "inbound session I/O did not start",
                    "address": address.to_string(),
                    "message": error.to_string(),
                });
                return;
            }
        };
        let reserved = policy.take_reservation();
        drop(policy);
        let registered = Registered {
            inner: &self,
            id: self.register(&conn, &peer, address, reserved),
        };
        conn.serve(Arc::new(SessionHandler {
            inner: self.clone(),
            peer: peer.clone(),
            exchanges: Arc::new(AtomicUsize::new(0)),
            exchange_slots: self.session_options(peer.role).handlers,
        }));
        let reason = loop {
            if let Some(reason) = conn.wait_closed(Duration::from_secs(3600)) {
                break reason;
            }
        };
        drop(registered);
        // The peer unpaired us (its GOAWAY explains the close). A peer we do
        // not dial ourselves shared nothing with us but through these
        // sessions: it is gone, not offline (WP3.INT re-review; the hub
        // forgets it once its last session closed). A peer we also dial
        // stays: our own pairing decides that.
        if reason.code == "PEER_NOT_PAIRED"
            && peer.role != LaneRole::Probe
            && !self
                .ctx
                .catalog
                .snapshot()
                .peers
                .iter()
                .any(|record| record.installation == peer.installation)
        {
            self.record(StatusEvent::PeerRemoved {
                installation: peer.installation.clone(),
            });
        }
    }

    fn handshake_failed(&self, address: SocketAddr, error: &ErrorWire) {
        self.counters
            .handshake_failures
            .fetch_add(1, Ordering::Relaxed);
        match error.code.as_str() {
            // Recorded (with the hello) by `legacy_attempt`.
            "PEER_NEEDS_UPGRADE" => {}
            "PEER_PROTOCOL_UNSUPPORTED" => self.record(StatusEvent::InboundIncompatible {
                address: address.to_string(),
                detail: error.message.clone(),
            }),
            _ => {
                event!(info, (error.code.as_str()), {
                    "detail": "inbound handshake failed",
                    "address": address.to_string(),
                    "message": error.message,
                });
            }
        }
    }

    /// Registers an authenticated session. The generation and revocation
    /// checks run under the state lock after the insert, which is the lock
    /// the rotation watcher scans under: a session is either seen by the
    /// watcher or sees the new catalog here.
    fn register(
        &self,
        conn: &Conn,
        peer: &AcceptedPeer,
        address: SocketAddr,
        reserved: bool,
    ) -> u64 {
        let id = self.next_session.fetch_add(1, Ordering::Relaxed);
        let goaway = {
            let mut state = self.state.lock();
            if reserved {
                state.reserved = state.reserved.saturating_sub(1);
            }
            let snapshot = self.ctx.catalog.snapshot();
            let goaway = if let Some(stopping) = &state.stopping {
                Some(stopping.clone())
            } else if rotated(&snapshot, peer) {
                Some(rotation_error())
            } else if revoked(&snapshot, &peer.installation) {
                Some(revoked_error())
            } else {
                None
            };
            state.sessions.insert(
                id,
                Session {
                    conn: conn.clone(),
                    peer: peer.clone(),
                    address,
                    opened_ms: self.now_ms(),
                    goaway: goaway.as_ref().map(|error| error.code.clone()),
                },
            );
            let used = state.sessions.len() + state.reserved;
            self.counters
                .sessions_peak
                .fetch_max(used, Ordering::Relaxed);
            goaway
        };
        self.counters
            .sessions_opened
            .fetch_add(1, Ordering::Relaxed);
        if peer.role != LaneRole::Probe {
            self.record(StatusEvent::InboundSession {
                installation: peer.installation.clone(),
                lane: status_lane(peer),
                role: peer.role,
                opened: true,
            });
        }
        if let Some(error) = goaway {
            if error.code != "ORCHESTRATOR_STOPPING" {
                self.counters
                    .rotation_goaways
                    .fetch_add(1, Ordering::Relaxed);
            }
            conn.goaway(&error.code, &error.message);
        }
        id
    }

    fn unregister(&self, id: u64) {
        let removed = self.state.lock().sessions.remove(&id);
        if let Some(session) = removed
            && session.peer.role != LaneRole::Probe
        {
            self.record(StatusEvent::InboundSession {
                installation: session.peer.installation.clone(),
                lane: status_lane(&session.peer),
                role: session.peer.role,
                opened: false,
            });
        }
        self.changed.poke();
    }

    /// Stops admitting sessions and sends every session `reason` (once).
    fn begin_stop(&self, reason: ErrorWire) {
        let conns: Vec<Conn> = {
            let mut state = self.state.lock();
            if state.stopping.is_some() {
                return;
            }
            state.stopping = Some(reason.clone());
            state
                .sessions
                .values_mut()
                .map(|session| {
                    session.goaway.get_or_insert_with(|| reason.code.clone());
                    session.conn.clone()
                })
                .collect()
        };
        for conn in conns {
            conn.goaway(&reason.code, &reason.message);
        }
        if let Some(poller) = &self.poller {
            let _ = poller.notify();
        }
    }

    // -- rotation ----------------------------------------------------------

    /// Follows the catalog: a verifier rotation or a revocation sends the
    /// affected sessions away. Ends (stopping the server) with the daemon.
    fn watch_catalog(self: Arc<Self>, mut generation: u64) {
        loop {
            if self.cancel.is_cancelled() {
                self.begin_stop(stopping_error());
                return;
            }
            let snapshot = self.ctx.catalog.wait_newer(generation, WATCH_WAIT);
            if snapshot.generation == generation {
                continue;
            }
            generation = snapshot.generation;
            self.enforce(&snapshot);
        }
    }

    /// Sends `GOAWAY` to every session the catalog no longer accepts.
    fn enforce(&self, snapshot: &CatalogSnapshot) {
        let mut goaways = Vec::new();
        {
            let mut state = self.state.lock();
            for session in state.sessions.values_mut() {
                if session.goaway.is_some() {
                    continue;
                }
                let error = if rotated(snapshot, &session.peer) {
                    rotation_error()
                } else if revoked(snapshot, &session.peer.installation) {
                    revoked_error()
                } else {
                    continue;
                };
                session.goaway = Some(error.code.clone());
                goaways.push((session.conn.clone(), error));
            }
        }
        self.counters
            .rotation_goaways
            .fetch_add(goaways.len() as u64, Ordering::Relaxed);
        for (conn, error) in goaways {
            conn.goaway(&error.code, &error.message);
        }
    }
}

/// A pre-auth slot, given back when dropped.
struct PreauthSlot<'a>(&'a Inner);

impl Drop for PreauthSlot<'_> {
    fn drop(&mut self) {
        self.0.end_preauth();
    }
}

/// A registered session, unregistered when dropped (which also breaks the
/// session ↔ handler reference cycle through `Inner`).
struct Registered<'a> {
    inner: &'a Inner,
    id: u64,
}

impl Drop for Registered<'_> {
    fn drop(&mut self) {
        self.inner.unregister(self.id);
    }
}

// ---------------------------------------------------------------------------
// Accept policy

/// [`AcceptPolicy`] over the catalog snapshot (§3.8): our installation (for
/// the self-pair refusal), the verifier and its generation, and revocation.
/// Every answer is an O(1) snapshot load.
pub struct CatalogAcceptPolicy {
    ctx: Arc<Ctx>,
}

impl CatalogAcceptPolicy {
    pub fn new(ctx: Arc<Ctx>) -> Self {
        Self { ctx }
    }
}

impl AcceptPolicy for CatalogAcceptPolicy {
    fn installation(&self) -> String {
        self.ctx.installation.clone()
    }

    fn verifier(&self) -> Option<(Verifier, u64)> {
        let snapshot = self.ctx.catalog.snapshot();
        snapshot
            .verifier
            .clone()
            .map(|verifier| (verifier, snapshot.verifier_generation))
    }

    fn admit(&self, hello: &ClientHello) -> Result<(), ErrorWire> {
        if revoked(&self.ctx.catalog.snapshot(), &hello.installation) {
            return Err(revoked_error());
        }
        Ok(())
    }

    fn legacy_attempt(&self, addr: SocketAddr, hello: &Value) {
        let who = hello
            .get("installation")
            .and_then(Value::as_str)
            .map(peer_text);
        let detail = match who {
            Some(installation) => format!(
                "PEER_NEEDS_UPGRADE: legacy (v2) peer \"{installation}\" must upgrade to tkfs \
                 peer protocol 3"
            ),
            None => "PEER_NEEDS_UPGRADE: a legacy (v2) peer must upgrade to tkfs peer protocol 3"
                .to_owned(),
        };
        self.ctx.status.record(StatusEvent::InboundIncompatible {
            address: addr.to_string(),
            detail,
        });
    }
}

/// The policy of one connection: the catalog policy plus the session cap.
/// `admit` reserves the connection's session slot; the connection thread
/// turns it into a session or gives it back.
struct ConnPolicy {
    policy: CatalogAcceptPolicy,
    inner: Arc<Inner>,
    reserved: AtomicBool,
}

impl ConnPolicy {
    /// Hands the reservation over to the session (it is no longer given
    /// back when the policy drops).
    fn take_reservation(&self) -> bool {
        self.reserved.swap(false, Ordering::AcqRel)
    }
}

impl Drop for ConnPolicy {
    /// A handshake that admitted the dialer and then failed gives its
    /// session slot back.
    fn drop(&mut self) {
        if *self.reserved.get_mut() {
            self.inner.release_reservation();
        }
    }
}

impl AcceptPolicy for ConnPolicy {
    fn installation(&self) -> String {
        self.policy.installation()
    }

    fn verifier(&self) -> Option<(Verifier, u64)> {
        self.policy.verifier()
    }

    fn admit(&self, hello: &ClientHello) -> Result<(), ErrorWire> {
        self.policy.admit(hello)?;
        if !self.reserved.load(Ordering::Acquire) {
            self.inner.reserve()?;
            self.reserved.store(true, Ordering::Release);
        }
        Ok(())
    }

    fn legacy_attempt(&self, addr: SocketAddr, hello: &Value) {
        self.inner
            .counters
            .legacy_refused
            .fetch_add(1, Ordering::Relaxed);
        self.policy.legacy_attempt(addr, hello);
    }
}

/// Peer-chosen text made safe for status: at most [`MAX_PEER_TEXT`] bytes,
/// control characters escaped.
fn peer_text(text: &str) -> String {
    let mut end = text.len().min(MAX_PEER_TEXT);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    let mut safe = String::new();
    for c in text[..end].chars() {
        if c.is_control() {
            safe.extend(c.escape_default());
        } else {
            safe.push(c);
        }
    }
    if end < text.len() {
        safe.push('…');
    }
    safe
}

/// Whether an `accept` error concerns only the connection it would have
/// returned (`WSAECONNRESET`, `ECONNABORTED`, and the network errors Linux's
/// `accept(2)` says to retry on), so the loop takes the next one at once.
/// Anything else (out of descriptors or memory, unknown) pauses the loop.
pub(crate) fn accept_error_is_per_connection(error: &io::Error) -> bool {
    use io::ErrorKind::*;
    if matches!(
        error.kind(),
        Interrupted
            | ConnectionAborted
            | ConnectionReset
            | ConnectionRefused
            | NetworkDown
            | NetworkUnreachable
            | HostUnreachable
            | TimedOut
    ) {
        return true;
    }
    #[cfg(unix)]
    if let Some(code) = error.raw_os_error() {
        return [
            libc::EPROTO,
            libc::ENOPROTOOPT,
            libc::EHOSTDOWN,
            libc::EOPNOTSUPP,
            libc::ENETDOWN,
            libc::ENETUNREACH,
            libc::EHOSTUNREACH,
        ]
        .contains(&code);
    }
    false
}

// ---------------------------------------------------------------------------
// Request routing

/// Serves one session's requests (runs on its handler pool).
struct SessionHandler {
    inner: Arc<Inner>,
    peer: AcceptedPeer,
    /// Exchanges of this session running now.
    exchanges: Arc<AtomicUsize>,
    /// At most this many at once (the role's handler count).
    exchange_slots: usize,
}

impl Handler for SessionHandler {
    fn handle(&self, req: Incoming) {
        // Cancelled, expired, or its session closed while it was queued:
        // dropping a cancelled responder sends nothing.
        if req.responder.cancelled() {
            return;
        }
        if let Err(error) = self.admissible(&req.op) {
            req.responder.error(error);
            return;
        }
        match req.op.as_str() {
            ops::PING => req.responder.reply(json!({ "alive": true }), Vec::new()),
            ops::CATALOG_LIST => catalog_list(&self.inner.ctx, req),
            ops::META_EXCHANGE => self.exchange(req, meta_exchange),
            op if op.starts_with("objects.") => self.exchange(req, objects),
            op => {
                let error = ErrorWire::coded(
                    "UNKNOWN_PEER_OPERATION",
                    format!(
                        "{:?} is not a tkfs peer protocol 3 operation",
                        peer_text(op)
                    ),
                );
                req.responder.error(error);
            }
        }
    }
}

impl SessionHandler {
    /// The per-request checks that need no folder (module docs).
    fn admissible(&self, op: &str) -> Result<(), ErrorWire> {
        let ctx = &self.inner.ctx;
        if ctx.stopping.load(Ordering::Acquire) || self.inner.cancel.is_cancelled() {
            return Err(stopping_error());
        }
        let snapshot = ctx.catalog.snapshot();
        if rotated(&snapshot, &self.peer) {
            return Err(rotation_error());
        }
        if revoked(&snapshot, &self.peer.installation) {
            return Err(revoked_error());
        }
        if !op_allowed(op, self.peer.role) {
            return Err(role_violation(op, self.peer.role));
        }
        Ok(())
    }

    /// Runs a worker-bound exchange on a thread of its own (module docs),
    /// or refuses it `BUSY` when the session's or the server's exchange
    /// slots are full.
    fn exchange(&self, req: Incoming, run: fn(&Arc<Ctx>, &AcceptedPeer, Incoming)) {
        let guard = match SlotGuard::take(&self.inner, &self.exchanges, self.exchange_slots) {
            Ok(guard) => guard,
            Err(detail) => {
                self.inner
                    .counters
                    .busy_refusals
                    .fetch_add(1, Ordering::Relaxed);
                req.responder.error(busy(detail));
                return;
            }
        };
        // The guard moves into the job: it gives the slots back when the job
        // ends (or panics), or when an unstarted job is dropped.
        let slot = Arc::new(Mutex::new(Some(req)));
        let job = {
            let slot = slot.clone();
            let ctx = self.inner.ctx.clone();
            let peer = self.peer.clone();
            move || {
                let _guard = guard;
                let taken = slot.lock().unwrap_or_else(PoisonError::into_inner).take();
                if let Some(req) = taken
                    && !req.responder.cancelled()
                {
                    run(&ctx, &peer, req);
                }
            }
        };
        if thread::Builder::new()
            .name("net-exchange".into())
            .spawn(job)
            .is_err()
        {
            // The job was dropped unrun (its guard gave the slots back):
            // answer the request here.
            let taken = slot.lock().unwrap_or_else(PoisonError::into_inner).take();
            if let Some(req) = taken {
                req.responder
                    .error(busy("no thread is available for the exchange".into()));
            }
        }
    }
}

/// One session slot and one server-wide slot of a running exchange, given
/// back when dropped (also when the exchange panics).
struct SlotGuard {
    inner: Arc<Inner>,
    session: Arc<AtomicUsize>,
}

impl SlotGuard {
    /// Takes both slots, or says which limit is reached.
    fn take(
        inner: &Arc<Inner>,
        session: &Arc<AtomicUsize>,
        session_slots: usize,
    ) -> Result<SlotGuard, String> {
        if session.fetch_add(1, Ordering::AcqRel) >= session_slots {
            session.fetch_sub(1, Ordering::AcqRel);
            return Err(format!(
                "{session_slots} exchanges of this session are already running"
            ));
        }
        let limit = inner.options.max_exchanges;
        if inner.exchanges.fetch_add(1, Ordering::AcqRel) >= limit {
            inner.exchanges.fetch_sub(1, Ordering::AcqRel);
            session.fetch_sub(1, Ordering::AcqRel);
            return Err(format!(
                "{limit} exchanges are already running on this orchestrator"
            ));
        }
        Ok(SlotGuard {
            inner: inner.clone(),
            session: session.clone(),
        })
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        self.session.fetch_sub(1, Ordering::AcqRel);
        self.inner.exchanges.fetch_sub(1, Ordering::AcqRel);
        self.inner.changed.poke();
    }
}

fn busy(detail: String) -> ErrorWire {
    ErrorWire::coded("BUSY", detail).with_retry_after(BUSY_RETRY_MS)
}

/// The code of an op sent on a session whose role does not carry it.
fn role_violation(op: &str, role: LaneRole) -> ErrorWire {
    match op_role(op) {
        Some(LaneRole::Bulk) => ErrorWire::coded(
            "BULK_ON_CONTROL_LANE",
            format!(
                "{op} needs a bulk session; this is a {} session",
                role.as_str()
            ),
        ),
        _ => ErrorWire::coded(
            "METADATA_ON_BULK_LANE",
            format!(
                "{op} needs a control session; this is a {} session",
                role.as_str()
            ),
        ),
    }
}

/// `catalog.list`'s reply, serialized like
/// [`CatalogListReply`](crate::model::protocol::CatalogListReply) without
/// copying the cached entries.
#[derive(Serialize)]
struct CatalogListRef<'a> {
    generation: u64,
    unchanged: bool,
    entries: &'a [PublishedEntry],
}

/// `catalog.list{since}` from the published-catalog cache: O(1) in the
/// workers (it never calls one), O(entries) to encode.
fn catalog_list(ctx: &Arc<Ctx>, req: Incoming) {
    let Incoming {
        header, responder, ..
    } = req;
    let request: CatalogListRequest = if header.is_null() {
        CatalogListRequest::default()
    } else {
        match serde_json::from_value(header) {
            Ok(request) => request,
            Err(error) => {
                responder.error(invalid_request("catalog.list", &error));
                return;
            }
        }
    };
    let (generation, entries) = ctx.published.list();
    let unchanged = request.since == Some(generation);
    let reply = CatalogListRef {
        generation,
        unchanged,
        entries: if unchanged { &[] } else { entries.as_slice() },
    };
    let encoded = match serde_json::to_vec(&reply) {
        Ok(encoded) => encoded,
        Err(error) => {
            responder.error(ErrorWire::coded("UNKNOWN_ERROR", error));
            return;
        }
    };
    if encoded.len() <= CATALOG_HEADER_MAX {
        match serde_json::from_slice::<Value>(&encoded) {
            Ok(header) => responder.reply(header, Vec::new()),
            Err(error) => responder.error(ErrorWire::coded("UNKNOWN_ERROR", error)),
        }
        return;
    }
    // Too large for a header: the same JSON as the body.
    let limit = protocol::max_body(ops::CATALOG_LIST);
    if encoded.len() > limit {
        responder.error(ErrorWire::coded(
            "WS_MESSAGE_TOO_LARGE",
            format!(
                "the published catalog is {} bytes, above the {limit}-byte catalog.list bound",
                encoded.len()
            ),
        ));
        return;
    }
    // No `entries` here: a reader that only parses the header fails rather
    // than read an empty catalog.
    let header = json!({
        "generation": generation,
        "unchanged": false,
        "in_body": true,
    });
    responder.reply(header, encoded);
}

pub(crate) fn invalid_request(op: &str, error: &dyn std::fmt::Display) -> ErrorWire {
    ErrorWire::coded("INVALID_REQUEST", format!("invalid {op} request: {error}"))
}

/// A folder an inbound op may use: its view, its worker and the route bound
/// to the authenticated peer.
pub(crate) struct Served {
    pub view: Arc<FolderView>,
    pub client: Arc<dyn WorkerClient>,
    pub route: SyncRoute,
}

/// Resolves `repo` to a folder that serves sync now. The route's `peer` is
/// the **authenticated** installation of the session, never anything the
/// request says (invariant 15); `remote_replica` is the replica the request
/// is from, which the worker checks against its own (`DUPLICATE_REPLICA_IDENTITY`).
pub(crate) fn resolve(
    ctx: &Ctx,
    peer: &AcceptedPeer,
    repo: &str,
    remote_replica: &str,
) -> Result<Served, ErrorWire> {
    let snapshot = ctx.catalog.snapshot();
    let Some(record) = snapshot.folder_by_repo(repo) else {
        return Err(ErrorWire::coded(
            "REPOSITORY_NOT_MANAGED",
            format!(
                "this orchestrator does not manage repository {:?}",
                peer_text(repo)
            ),
        ));
    };
    if !record.sync_enabled {
        return Err(ErrorWire::coded(
            "FOLDER_SYNC_PAUSED",
            format!("sync of folder {} is paused", record.label),
        ));
    }
    let starting = || {
        ErrorWire::coded(
            "REMOTE_WORKER_STARTING",
            format!("folder {} is not running yet", record.label),
        )
    };
    let view = ctx
        .folders
        .view(&record.state)
        .filter(|view| matches!(view.phase, FolderPhase::Running { .. }))
        .ok_or_else(starting)?;
    let client = ctx.folders.client(&record.state).ok_or_else(starting)?;
    Ok(Served {
        route: SyncRoute {
            peer: peer.installation.clone(),
            repo: record.repo.clone(),
            local_replica: record.device.clone(),
            remote_replica: remote_replica.to_owned(),
        },
        view,
        client,
    })
}

/// The worker deadline of a request: its own `cap`, or what is left of the
/// peer's deadline minus the reply margin; `None` when too little is left.
pub(crate) fn worker_deadline(peer_deadline: Instant, cap: Duration) -> Option<Duration> {
    let left = peer_deadline
        .saturating_duration_since(Instant::now())
        .saturating_sub(REPLY_MARGIN);
    (left >= MIN_WORKER_DEADLINE).then(|| left.min(cap))
}

/// The error a peer is answered for a failed worker call. A worker that
/// cannot be reached (nothing was sent) is, to the peer, a remote folder
/// that is not serving yet.
pub(crate) fn peer_error(error: &CallError) -> ErrorWire {
    match error {
        CallError::Unavailable(detail) => ErrorWire::coded(
            "REMOTE_WORKER_STARTING",
            format!("the folder's worker is not reachable: {detail}"),
        ),
        other => other.to_wire(),
    }
}

/// A failed worker-bound exchange: what the peer is answered, and what the
/// hub records (they differ only for an unreachable worker, see
/// [`peer_error`]).
pub(crate) struct Failed {
    pub peer: ErrorWire,
    pub local: ErrorWire,
}

impl Failed {
    pub(crate) fn both(error: ErrorWire) -> Box<Failed> {
        Box::new(Failed {
            peer: error.clone(),
            local: error,
        })
    }

    pub(crate) fn call(error: &CallError) -> Box<Failed> {
        Box::new(Failed {
            peer: peer_error(error),
            local: error.to_wire(),
        })
    }
}

pub(crate) fn deadline_error(op: &str) -> ErrorWire {
    ErrorWire::coded(
        "REQUEST_TIMEOUT",
        format!("too little of the {op} deadline is left to call the worker"),
    )
}

/// One finished exchange a worker served, for the hub (§7.1).
pub(crate) struct Finished<'a> {
    pub peer: &'a AcceptedPeer,
    pub repo: &'a str,
    pub op: &'a str,
    pub result: Result<(), ErrorWire>,
    pub summary: Option<ExchangeSummary>,
    pub bytes_up: u64,
    pub bytes_down: u64,
    pub objects_up: u64,
    pub objects_down: u64,
    pub started: Instant,
}

pub(crate) fn record_finished(ctx: &Ctx, finished: Finished<'_>) {
    let (ok, error) = match finished.result {
        Ok(()) => (true, None),
        Err(error) => (false, Some(error)),
    };
    ctx.status.record(StatusEvent::ExchangeFinished {
        installation: finished.peer.installation.clone(),
        repo: finished.repo.to_owned(),
        lane: status_lane(finished.peer),
        op: finished.op.to_owned(),
        direction: Direction::Inbound,
        ok,
        error,
        summary: finished.summary,
        bytes_up: finished.bytes_up,
        bytes_down: finished.bytes_down,
        objects_up: finished.objects_up,
        objects_down: finished.objects_down,
        duration_ms: u64::try_from(finished.started.elapsed().as_millis()).unwrap_or(u64::MAX),
    });
}

/// `objects.*`: the role is checked already; [`super::inbound_objects`]
/// does the rest.
fn objects(ctx: &Arc<Ctx>, peer: &AcceptedPeer, req: Incoming) {
    super::inbound_objects::handle(ctx, peer, req);
}

/// `meta.exchange` (§6.3.5 step 2): the worker stages the dialer's page and
/// answers with its own (`feed-exchange`). The acceptor's view of the
/// exchange (`local`) goes to the hub and, as gate evidence, to the folder.
fn meta_exchange(ctx: &Arc<Ctx>, peer: &AcceptedPeer, req: Incoming) {
    let started = Instant::now();
    let Incoming {
        header,
        body,
        deadline,
        responder,
        ..
    } = req;
    let op = ops::META_EXCHANGE;
    let parsed = (|| {
        let head: MetaExchangeHeader =
            serde_json::from_value(header).map_err(|error| invalid_request(op, &error))?;
        let request: MetaExchangeRequest =
            serde_json::from_slice(&body).map_err(|error| invalid_request(op, &error))?;
        if request.repo != head.repo {
            return Err(ErrorWire::coded(
                "REPOSITORY_MISMATCH",
                "the meta.exchange header and body name different repositories",
            ));
        }
        Ok(request)
    })();
    let request = match parsed {
        Ok(request) => request,
        Err(error) => {
            responder.error(error);
            return;
        }
    };
    let served = match resolve(ctx, peer, &request.repo, &request.sender) {
        Ok(served) => served,
        Err(error) => {
            responder.error(error);
            return;
        }
    };
    let repo = request.repo.clone();
    let bytes_down = body.len() as u64;
    let finish =
        |result: Result<(), ErrorWire>, summary: Option<ExchangeSummary>, bytes_up: u64| {
            record_finished(
                ctx,
                Finished {
                    peer,
                    repo: &repo,
                    op,
                    result,
                    summary,
                    bytes_up,
                    bytes_down,
                    objects_up: 0,
                    objects_down: 0,
                    started,
                },
            );
        };
    if request.receiver != served.route.local_replica {
        let error = ErrorWire::coded(
            "LOCAL_REPLICA_BINDING_MISMATCH",
            format!(
                "the request is for replica {:?}; this folder is replica {}",
                peer_text(&request.receiver),
                served.route.local_replica
            ),
        );
        finish(Err(error.clone()), None, 0);
        responder.error(error);
        return;
    }
    let Some(wait) = worker_deadline(deadline, ipc::deadlines::FEED_EXCHANGE) else {
        responder.error(deadline_error(op));
        return;
    };
    let request_id = request.request.clone();
    let payload = match serde_json::to_value(FeedExchangeRequest {
        route: served.route.clone(),
        request,
    }) {
        Ok(payload) => payload,
        Err(error) => {
            responder.error(ErrorWire::coded("UNKNOWN_ERROR", error));
            return;
        }
    };
    if responder.cancelled() {
        return;
    }
    // CCR-WP2.8-3: the exchange is in flight on the inbound lane until its
    // `ExchangeFinished` (every path below records one).
    ctx.status.record(StatusEvent::ExchangeStarted {
        installation: peer.installation.clone(),
        repo: repo.clone(),
        lane: status_lane(peer),
        op: op.to_owned(),
        at_ms: ctx.clock.now_ms(),
        direction: Direction::Inbound,
    });
    let reply = served
        .client
        .call(ipc::ops::FEED_EXCHANGE, payload, wait)
        .map_err(|error| Failed::call(&error))
        .and_then(|value| {
            let reply: FeedExchangeReply = serde_json::from_value(value).map_err(|error| {
                Failed::both(ErrorWire::coded(
                    "INVALID_REPLY",
                    format!("the worker's feed-exchange reply is invalid: {error}"),
                ))
            })?;
            if reply.reply.reply_to != request_id {
                return Err(Failed::both(ErrorWire::coded(
                    "UNBOUND_PEER_RESPONSE",
                    "the worker's meta.exchange reply is not bound to the request",
                )));
            }
            let body = serde_json::to_vec(&reply.reply)
                .map_err(|error| Failed::both(ErrorWire::coded("UNKNOWN_ERROR", error)))?;
            let limit = protocol::max_body(op);
            if body.len() > limit {
                return Err(Failed::both(ErrorWire::coded(
                    "WS_MESSAGE_TOO_LARGE",
                    format!(
                        "the meta.exchange reply is {} bytes, above {limit}",
                        body.len()
                    ),
                )));
            }
            Ok((reply, body))
        });
    match reply {
        Ok((reply, body)) => {
            let bytes_up = body.len() as u64;
            let FeedExchangeReply { local, .. } = reply;
            // The dialer's mapping (one helper): a caught-up source with
            // blocked, quarantined or rejected rows is the Blocked gate.
            let evidence = super::meta_exchange::evidence_of(
                &peer.installation,
                ctx.clock.now_ms(),
                &local.gate,
                local.summary.caught_up,
            );
            let mut summary = local.summary;
            if summary.bytes_up == 0 {
                summary.bytes_up = bytes_up;
            }
            if summary.bytes_down == 0 {
                summary.bytes_down = bytes_down;
            }
            // Status and evidence first: once the reply is out, the dialer
            // may reuse this exchange's slot, and readers of status should
            // already see the exchange.
            finish(Ok(()), Some(summary), bytes_up);
            let _ = ctx
                .folders
                .send(&served.view.record.state, FolderCmd::GateEvidence(evidence));
            responder.reply(json!({ "repo": served.route.repo }), body);
        }
        Err(failed) => {
            finish(Err(failed.local), None, 0);
            responder.error(failed.peer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::accept_error_is_per_connection;
    use std::io::{Error, ErrorKind};

    /// A reset or aborted connection is skipped at once; running out of
    /// descriptors (or anything unknown) pauses the accept loop.
    #[test]
    fn accept_errors_that_concern_one_connection_do_not_pause() {
        for kind in [
            ErrorKind::ConnectionReset,
            ErrorKind::ConnectionAborted,
            ErrorKind::Interrupted,
        ] {
            assert!(
                accept_error_is_per_connection(&Error::from(kind)),
                "{kind:?}"
            );
        }
        for kind in [
            ErrorKind::OutOfMemory,
            ErrorKind::Other,
            ErrorKind::PermissionDenied,
        ] {
            assert!(
                !accept_error_is_per_connection(&Error::from(kind)),
                "{kind:?}"
            );
        }
        #[cfg(windows)]
        {
            // WSAECONNRESET, WSAEMFILE.
            assert!(accept_error_is_per_connection(&Error::from_raw_os_error(
                10054
            )));
            assert!(!accept_error_is_per_connection(&Error::from_raw_os_error(
                10024
            )));
        }
        #[cfg(unix)]
        {
            assert!(accept_error_is_per_connection(&Error::from_raw_os_error(
                libc::ECONNABORTED
            )));
            assert!(accept_error_is_per_connection(&Error::from_raw_os_error(
                libc::EPROTO
            )));
            assert!(!accept_error_is_per_connection(&Error::from_raw_os_error(
                libc::EMFILE
            )));
            assert!(!accept_error_is_per_connection(&Error::from_raw_os_error(
                libc::ENFILE
            )));
        }
    }
}
