//! Owned by WP1.3 (JSON client) and WP3.3 (v3 loopback session client).
//!
//! Two [`WorkerClient`]s of one worker instance (REVAMP_PLAN §3.3, §6.1, §6.4):
//!
//! # `JsonWorkerClient` (WP1.3)
//!
//! Requests `{"request": id, "payload": {"op": …}}` over the worker's loopback
//! WebSocket endpoint, the same envelope and `{ok, result | error}` reply as
//! `ws::call`.
//!
//! * **Token (WP4.1).** Every upgrade carries the instance's token
//!   (`WorkerEndpoint.token`, from the Ready line or `worker.json`) as
//!   `X-Tkfs-Token`; an endpoint without one presents the token this process
//!   knows for the address (`ws::tokens::lookup`), if any, so a legacy
//!   worker (no token) is still reached. A client with a token also
//!   remembers it for the address while it lives (`ws::tokens::remember`),
//!   so other callers of this process that only know the address (the
//!   `Folder` passthrough's `compat::worker_call`) present it too. A refused
//!   token is `Unavailable(LOCAL_WORKER_UNAVAILABLE: … ACCESS_DENIED …)`
//!   (nothing was sent).
//!
//! * **Pool.** Up to [`MAX_IDLE`] idle connections are kept. A call checks one
//!   out (or opens a new one) and returns it afterwards; no socket is ever used
//!   while the pool lock is held. A socket idle longer than [`MAX_IDLE_AGE`]
//!   (the worker closes idle connections after 15 s), or one the worker has
//!   closed, is not reused, so a stale socket never turns a call uncertain.
//! * **Deadline.** The whole call (connect, upgrade, send, reply) is bounded by
//!   its deadline: the TCP connect, and every socket read and write, use the
//!   time left as their timeout.
//! * **Errors** ([`CallError`]):
//!   - a complete error reply → `Remote(ErrorWire)`; the socket stays usable
//!     (invariant 17) and goes back to the pool;
//!   - a socket timeout after the request was (or may have been) written →
//!     `Timeout`; any other failure after that point → `ConnectionLost`.
//!     Both are uncertain (the worker may have executed the request), the
//!     socket is dropped, and the request is **never** replayed (invariant 16);
//!   - failing to connect or to upgrade → `Unavailable` (nothing was sent).
//! * `call_bytes` (binary bodies) goes through the instance's
//!   [`SessionWorkerClient`] ([`session_for`]); a worker that cannot open one
//!   answers `Unavailable(BINARY_OPS_REQUIRE_SESSION: …)` (nothing was sent).
//!
//! # `SessionWorkerClient` (WP3.3)
//!
//! One multiplexed v3 loopback session (`X-Tkfs-Sync: 3-loopback`,
//! `session::dial_loopback`) per worker instance, shared by every bulk lane
//! and inbound exchange of the daemon ([`session_for`] caches it per
//! instance). It carries the worker's objects v3 ops (§6.4) with binary
//! bodies:
//!
//! | method | op | request | reply |
//! |---|---|---|---|
//! | [`SessionWorkerClient::want`] | `objects.want` | header `WantHeader` | header `WantReply` |
//! | [`SessionWorkerClient::read`] | `objects.read` | header `ReadHeader` (len ≤ 1 MiB) | header `ReadReply{total}`, body = bytes |
//! | [`SessionWorkerClient::read_stream`] | `objects.read` × n | ≤ `INFLIGHT_READS` reads in flight, delivered in order | |
//! | [`SessionWorkerClient::write_batch`] | `objects.write` | header `WriteHeader`, body = the items' bytes concatenated (≤ 1 MiB) | header `WriteReply` |
//! | [`SessionWorkerClient::report`] | `objects.report` | header `ReportHeader` | header `ReportReply` |
//!
//! A JSON reply is read from the response body when there is one, else from
//! the header. The [`WorkerClient`] methods send `call(op, payload)` as a
//! header-only request (`payload` is the header, the reply header is the
//! result) and `call_bytes` as a request with a body.
//!
//! * **Session.** Dialed by [`SessionWorkerClient::connect`]; a session that
//!   closed (the worker restarted its listener, a transport error) is redialed
//!   by the next call, without the client's lock (§2.2): callers that need
//!   the session meanwhile wait for that one dial, and share its failure. A
//!   call that was in flight on it is uncertain and is never replayed
//!   (invariant 16).
//! * **Refusals.** A request the session refused without running it is
//!   sent again after 10 ms → 250 ms, within the call's deadline: the
//!   worker session's `BUSY` (Contention: no handler took it, its queue was
//!   full) or the local queue being full (`CallError::Busy`). Nothing else
//!   is retried; in particular not the worker's own `WORKER_BUSY`, which it
//!   may answer after it handed the command on (a mount transition that is
//!   still running): sent again, it could run twice (invariant 16).
//! * **No objects v3.** A worker that does not echo the loopback upgrade
//!   header (a Wave 2 worker) gives `Unavailable(UNSUPPORTED_OPERATION: …)`;
//!   [`session_for`] remembers that for the instance (a worker instance never
//!   changes its build), and the bulk lanes fail that repository's key
//!   (`UNSUPPORTED_OPERATION`, Transient: backed off by class) until the
//!   worker is restarted on this build; there is no interim path any more
//!   (WP4.5, CCR-WP4.5-1).
use crate::{
    model::{
        errors::{ErrorWire, UNKNOWN_CODE, code_in},
        orchestrator::{CallError, WorkerClient, WorkerEndpoint},
        protocol::objects::{
            self as wire, ReadHeader, ReadReply, ReportAction, ReportHeader, ReportReply,
            WantHeader, WantItem, WantReply, WriteHeader, WriteItem, WriteReply, WriteResult,
        },
    },
    paired_sync::SyncRoute,
    session::{self, Conn, ConnOptions, DialError, Priority, Reply as SessionReply},
    util::lock::lock,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt, io,
    net::{TcpStream, ToSocketAddrs},
    sync::{
        Arc, Condvar, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tungstenite::{
    Message, WebSocket, client::IntoClientRequest, handshake::HandshakeError,
    protocol::WebSocketConfig,
};

/// Idle connections kept per client.
pub const MAX_IDLE: usize = 8;
/// An idle connection older than this is closed instead of reused (the
/// worker's per-connection read timeout is 15 s).
pub const MAX_IDLE_AGE: Duration = Duration::from_secs(10);

/// Pool tuning (tests shorten the idle age).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientOptions {
    pub max_idle: usize,
    pub max_idle_age: Duration,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            max_idle: MAX_IDLE,
            max_idle_age: MAX_IDLE_AGE,
        }
    }
}

type Socket = WebSocket<TcpStream>;

struct Idle {
    socket: Socket,
    since: Instant,
}

/// JSON RPC client of one worker instance.
pub struct JsonWorkerClient {
    endpoint: WorkerEndpoint,
    options: ClientOptions,
    idle: Mutex<Vec<Idle>>,
    connects: AtomicU64,
    /// The endpoint's token, remembered for its address (module docs).
    _remembered: Vec<Arc<crate::ws::tokens::Hold>>,
}

impl fmt::Debug for JsonWorkerClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonWorkerClient")
            .field("instance", &self.endpoint.instance)
            .field("address", &self.endpoint.address)
            .field("idle", &self.idle())
            .field("connects", &self.connects())
            .finish_non_exhaustive()
    }
}

/// A parsed reply: a result, or a complete error reply.
enum Reply {
    Ok(Value),
    Err(ErrorWire),
}

impl JsonWorkerClient {
    pub fn new(endpoint: WorkerEndpoint) -> Arc<Self> {
        Self::with_options(endpoint, ClientOptions::default())
    }

    pub fn with_options(endpoint: WorkerEndpoint, options: ClientOptions) -> Arc<Self> {
        let remembered = match &endpoint.token {
            Some(token) => crate::ws::tokens::addresses(&endpoint.address)
                .into_iter()
                .filter_map(|address| crate::ws::tokens::remember(address, token))
                .collect(),
            None => Vec::new(),
        };
        Arc::new(Self {
            endpoint,
            options,
            idle: Mutex::new(Vec::new()),
            connects: AtomicU64::new(0),
            _remembered: remembered,
        })
    }

    pub fn endpoint(&self) -> &WorkerEndpoint {
        &self.endpoint
    }

    /// Idle pooled connections.
    pub fn idle(&self) -> usize {
        crate::util::lock::lock(&self.idle).len()
    }

    /// Connections opened so far (successful upgrades).
    pub fn connects(&self) -> u64 {
        self.connects.load(Ordering::Relaxed)
    }

    /// A reusable idle socket, or a new connection.
    fn checkout(&self, deadline_at: Instant) -> Result<Socket, CallError> {
        loop {
            let candidate = crate::util::lock::lock(&self.idle).pop();
            let Some(idle) = candidate else { break };
            if idle.since.elapsed() < self.options.max_idle_age && reusable(idle.socket.get_ref()) {
                return Ok(idle.socket);
            }
            // Dropped (closed) outside the pool lock.
        }
        self.connect(deadline_at)
    }

    /// Returns a healthy socket to the pool (or closes it when the pool is
    /// full); stale idle sockets are closed on the way.
    fn checkin(&self, socket: Socket) {
        let mut closing = Vec::new();
        {
            let mut idle = crate::util::lock::lock(&self.idle);
            let age = self.options.max_idle_age;
            let (keep, stale): (Vec<_>, Vec<_>) =
                idle.drain(..).partition(|i| i.since.elapsed() < age);
            *idle = keep;
            closing.extend(stale);
            if idle.len() < self.options.max_idle {
                idle.push(Idle {
                    socket,
                    since: Instant::now(),
                });
            } else {
                closing.push(Idle {
                    socket,
                    since: Instant::now(),
                });
            }
        }
        drop(closing);
    }

    fn connect(&self, deadline_at: Instant) -> Result<Socket, CallError> {
        let address = &self.endpoint.address;
        let unavailable = |detail: String| {
            CallError::Unavailable(format!(
                "LOCAL_WORKER_UNAVAILABLE: worker {} at {address}: {detail}",
                self.endpoint.instance
            ))
        };
        let uri = crate::ws::endpoint_uri(address).map_err(|e| unavailable(format!("{e:#}")))?;
        if uri.scheme_str() != Some("ws") {
            return Err(unavailable(
                "worker endpoints are plain ws:// loopback addresses".into(),
            ));
        }
        let host = uri.host().unwrap_or_default().trim_matches(['[', ']']);
        let port = uri.port_u16().unwrap_or(80);
        let addresses = (host, port)
            .to_socket_addrs()
            .map_err(|e| unavailable(format!("cannot resolve: {e}")))?;
        let mut last = None;
        let mut stream = None;
        for candidate in addresses {
            let Some(left) = remaining(deadline_at) else {
                break;
            };
            match TcpStream::connect_timeout(&candidate, left) {
                Ok(connected) => {
                    stream = Some(connected);
                    break;
                }
                Err(error) => last = Some(error),
            }
        }
        let Some(stream) = stream else {
            return Err(unavailable(match last {
                Some(error) => format!("connect failed: {error}"),
                None => "connect timed out".into(),
            }));
        };
        let left = remaining(deadline_at).ok_or_else(|| unavailable("connect timed out".into()))?;
        set_timeouts(&stream, left).map_err(|e| unavailable(format!("socket setup: {e}")))?;
        let mut request = address
            .as_str()
            .into_client_request()
            .map_err(|e| unavailable(format!("{e}")))?;
        let token = self
            .endpoint
            .token
            .clone()
            .or_else(|| crate::ws::tokens::lookup_endpoint(address));
        if let Some(token) = token {
            let value = tungstenite::http::HeaderValue::from_str(&token)
                .map_err(|_| unavailable("the worker token is not a header value".into()))?;
            request
                .headers_mut()
                .insert(crate::ws::tokens::HEADER, value);
        }
        match tungstenite::client::client_with_config(request, stream, Some(config())) {
            Ok((socket, _)) => {
                self.connects.fetch_add(1, Ordering::Relaxed);
                Ok(socket)
            }
            // A socket read timeout surfaces as an interrupted handshake.
            Err(HandshakeError::Interrupted(_)) => {
                Err(unavailable("WebSocket upgrade timed out".into()))
            }
            Err(HandshakeError::Failure(tungstenite::Error::Http(response)))
                if matches!(response.status().as_u16(), 401 | 403) =>
            {
                Err(unavailable(format!(
                    "ACCESS_DENIED: the worker refused the upgrade (HTTP {}): its local token \
                     is required",
                    response.status()
                )))
            }
            Err(HandshakeError::Failure(error)) => {
                Err(unavailable(format!("WebSocket upgrade failed: {error}")))
            }
        }
    }
}

impl WorkerClient for JsonWorkerClient {
    fn instance(&self) -> &str {
        &self.endpoint.instance
    }

    fn call(&self, op: &str, payload: Value, deadline: Duration) -> Result<Value, CallError> {
        let started = Instant::now();
        let deadline_at = started + deadline;
        let text = envelope(op, payload)?;
        let mut socket = self.checkout(deadline_at)?;
        match exchange(&mut socket, text, started, deadline_at) {
            Ok(Reply::Ok(value)) => {
                self.checkin(socket);
                Ok(value)
            }
            Ok(Reply::Err(error)) => {
                self.checkin(socket);
                Err(CallError::Remote(error))
            }
            Err(Keep(error)) => {
                self.checkin(socket);
                Err(error)
            }
            // The socket is dropped: its state is unknown.
            Err(Discard(error)) => Err(error),
        }
    }

    /// Binary ops travel on the instance's v3 loopback session
    /// ([`session_for`]). When none can be opened, nothing was sent:
    /// `Unavailable(BINARY_OPS_REQUIRE_SESSION: …)`, with the reason.
    fn call_bytes(
        &self,
        op: &str,
        header: Value,
        body: &[u8],
        deadline: Duration,
    ) -> Result<(Value, Vec<u8>), CallError> {
        let session = session_for(&self.endpoint).map_err(|error| {
            CallError::Unavailable(format!(
                "BINARY_OPS_REQUIRE_SESSION: {op} needs a v3 loopback session: {error}"
            ))
        })?;
        session.call_bytes(op, header, body, deadline)
    }
}

/// A failed exchange, and what becomes of its socket. The `CallError` says
/// whether the request may have executed (`is_uncertain`).
enum Failure {
    /// Nothing was written and the socket is clean: back to the pool.
    Keep(CallError),
    /// The request was (or may have been) written, or the socket is broken:
    /// dropped.
    Discard(CallError),
}
use Failure::{Discard, Keep};

/// `{"request": id, "payload": {"op": op, …payload}}` as JSON text.
fn envelope(op: &str, payload: Value) -> Result<String, CallError> {
    let mut payload = match payload {
        Value::Object(map) => map,
        Value::Null => Map::new(),
        other => {
            return Err(CallError::Unavailable(format!(
                "INVALID_REQUEST: the payload of {op} must be a JSON object, not {other}"
            )));
        }
    };
    payload.insert("op".into(), Value::String(op.to_owned()));
    let request = serde_json::json!({"request": crate::core::id(), "payload": payload});
    let text = request.to_string();
    if text.len() > crate::ws::MAX_MESSAGE {
        return Err(CallError::Unavailable(format!(
            "WS_MESSAGE_TOO_LARGE: the {op} request is {} bytes",
            text.len()
        )));
    }
    Ok(text)
}

fn exchange(
    socket: &mut Socket,
    text: String,
    started: Instant,
    deadline_at: Instant,
) -> Result<Reply, Failure> {
    let timed_out = || CallError::Timeout {
        after_ms: started.elapsed().as_millis() as u64,
    };
    let Some(left) = remaining(deadline_at) else {
        return Err(Keep(CallError::Unavailable(
            "REQUEST_TIMEOUT: the deadline elapsed before the request was sent".into(),
        )));
    };
    if let Err(error) = set_timeouts(socket.get_ref(), left) {
        return Err(Discard(CallError::Unavailable(format!(
            "LOCAL_WORKER_UNAVAILABLE: socket setup: {error}"
        ))));
    }
    if let Err(error) = socket.send(Message::text(text)) {
        return Err(Discard(transport(error, &timed_out)));
    }
    loop {
        let Some(left) = remaining(deadline_at) else {
            return Err(Discard(timed_out()));
        };
        if let Err(error) = socket.get_ref().set_read_timeout(Some(left)) {
            return Err(Discard(CallError::ConnectionLost(format!(
                "socket setup: {error}"
            ))));
        }
        match socket.read() {
            Ok(Message::Text(text)) => return parse_reply(text.as_bytes()).map_err(Discard),
            Ok(Message::Binary(bytes)) => return parse_reply(&bytes).map_err(Discard),
            Ok(Message::Close(_)) => {
                return Err(Discard(CallError::ConnectionLost(
                    "WS_CLOSED: the worker closed the connection".into(),
                )));
            }
            // Ping/Pong/raw frames: tungstenite queues the Pong; flush it.
            Ok(_) => {
                let _ = socket.flush();
            }
            Err(error) => return Err(Discard(transport(error, &timed_out))),
        }
    }
}

fn parse_reply(bytes: &[u8]) -> Result<Reply, CallError> {
    let invalid = |detail: &str| CallError::ConnectionLost(format!("INVALID_WS_REPLY: {detail}"));
    let reply: Value = serde_json::from_slice(bytes).map_err(|e| invalid(&e.to_string()))?;
    match reply.get("ok").and_then(Value::as_bool) {
        Some(true) => reply
            .get("result")
            .cloned()
            .map(Reply::Ok)
            .ok_or_else(|| invalid("missing result")),
        Some(false) => reply
            .get("error")
            .and_then(Value::as_str)
            .map(|message| Reply::Err(remote_error(message)))
            .ok_or_else(|| invalid("missing error")),
        None => Err(invalid("missing boolean ok")),
    }
}

/// A worker error reply (`format!("{error:#}")` of an anyhow chain) as a wire
/// error: the first code in the text, or `UNKNOWN_ERROR`.
fn remote_error(message: &str) -> ErrorWire {
    ErrorWire::new(code_in(message).unwrap_or(UNKNOWN_CODE), message)
}

fn transport(error: tungstenite::Error, timed_out: &dyn Fn() -> CallError) -> CallError {
    match &error {
        tungstenite::Error::Io(io)
            if matches!(
                io.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
            ) =>
        {
            timed_out()
        }
        tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed => {
            CallError::ConnectionLost("WS_CLOSED: the connection was closed".into())
        }
        _ => CallError::ConnectionLost(error.to_string()),
    }
}

/// Time left until `deadline_at`, or `None` when it has passed.
fn remaining(deadline_at: Instant) -> Option<Duration> {
    let left = deadline_at.saturating_duration_since(Instant::now());
    (!left.is_zero()).then_some(left)
}

fn set_timeouts(stream: &TcpStream, timeout: Duration) -> io::Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))
}

/// Whether an idle socket can carry another request: nothing unread and not
/// closed by the worker (a non-blocking peek would block).
fn reusable(stream: &TcpStream) -> bool {
    if stream.set_nonblocking(true).is_err() {
        return false;
    }
    let mut byte = [0u8; 1];
    let open = matches!(stream.peek(&mut byte), Err(e) if e.kind() == io::ErrorKind::WouldBlock);
    stream.set_nonblocking(false).is_ok() && open
}

fn config() -> WebSocketConfig {
    WebSocketConfig::default()
        .max_message_size(Some(crate::ws::MAX_MESSAGE))
        .max_frame_size(Some(crate::ws::MAX_MESSAGE))
}

// ---------------------------------------------------------------------------
// v3 loopback session client (WP3.3)

/// Per-call deadlines of the worker's objects ops.
pub mod deadlines {
    use std::time::Duration;
    /// `objects.want`: a worker query plus lease bookkeeping.
    pub const WANT: Duration = Duration::from_secs(10);
    /// `objects.read` of ≤ 1 MiB. Generous: the first serve of an object
    /// verifies it whole (verify-on-first-serve, §14 WP3.2).
    pub const READ: Duration = Duration::from_secs(60);
    /// `objects.write` of ≤ 1 MiB: append, spool fsync, `spool_state` commit,
    /// and on completion a full re-read SHA-256, install and registration.
    pub const WRITE: Duration = Duration::from_secs(60);
    /// `objects.report`: the sender re-verifies its copy.
    pub const REPORT: Duration = Duration::from_secs(60);
    /// The whole-object hash a worker may do inside a call (the write that
    /// completes an object, the first read of an object, a report) is
    /// allowed this rate on top of the call's own deadline.
    pub const VERIFY_BYTES_PER_S: u64 = 64 * 1024 * 1024;
    /// At most this much verification time is allowed (sizes may come from
    /// a peer: a claimed size never buys an unbounded deadline). 30 min at
    /// [`VERIFY_BYTES_PER_S`] covers objects of 112 GiB.
    pub const MAX_VERIFY: Duration = Duration::from_secs(30 * 60);

    /// `base` plus the time to verify an object of `size` bytes (at most
    /// [`MAX_VERIFY`]).
    pub fn with_verify(base: Duration, size: u64) -> Duration {
        base + Duration::from_secs(size / VERIFY_BYTES_PER_S).min(MAX_VERIFY)
    }
}

/// First pause before a request the worker refused (`BUSY`) is sent again.
const REFUSED_RETRY_FIRST: Duration = Duration::from_millis(10);
/// Longest pause between two attempts of a refused request.
const REFUSED_RETRY_CAP: Duration = Duration::from_millis(250);
/// Worker instances [`session_for`] remembers.
const MAX_CACHED_SESSIONS: usize = 256;

/// The v3 loopback session client of one worker instance (module docs).
pub struct SessionWorkerClient {
    endpoint: WorkerEndpoint,
    options: ConnOptions,
    conn: Mutex<ConnSlot>,
    /// A dial finished (see [`ConnSlot::dialing`]).
    dialed: Condvar,
    dials: AtomicU64,
}

/// The current session of a [`SessionWorkerClient`]. Dials run without the
/// lock (§2.2: no lock across I/O); callers that need a session meanwhile
/// wait on the condvar for that one dial instead of dialing too.
#[derive(Default)]
struct ConnSlot {
    conn: Option<Conn>,
    dialing: bool,
    /// Dials finished so far, and the error of the last one if it failed.
    finished: u64,
    failed: Option<CallError>,
}

impl fmt::Debug for SessionWorkerClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionWorkerClient")
            .field("instance", &self.endpoint.instance)
            .field("address", &self.endpoint.address)
            .field("open", &self.is_open())
            .field("dials", &self.dials())
            .finish_non_exhaustive()
    }
}

/// One range of an object for [`SessionWorkerClient::read_stream`]
/// (`len` ≤ [`wire::MAX_READ_LEN`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadRange {
    pub hash: String,
    pub offset: u64,
    pub len: u64,
}

/// The consumer of [`SessionWorkerClient::read_stream`]: one range's
/// `(total, bytes)` or its error, in range order.
pub type ReadConsumer<'a> =
    dyn FnMut(&ReadRange, Result<(u64, Vec<u8>), CallError>) -> ReadFlow + 'a;

/// What the consumer of a read stream wants next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadFlow {
    Continue,
    /// No more ranges of this range's object (reads already in flight are
    /// dropped).
    SkipObject,
    /// End the stream.
    Stop,
}

impl SessionWorkerClient {
    /// Dials the worker's loopback endpoint now (`Unavailable` when it
    /// cannot: `UNSUPPORTED_OPERATION` for a worker without loopback
    /// sessions, `LOCAL_WORKER_UNAVAILABLE` otherwise).
    pub fn connect(ep: &WorkerEndpoint) -> Result<Arc<Self>, CallError> {
        Self::connect_with(ep, ConnOptions::default())
    }

    /// [`SessionWorkerClient::connect`] with explicit session options.
    pub fn connect_with(ep: &WorkerEndpoint, options: ConnOptions) -> Result<Arc<Self>, CallError> {
        let client = Arc::new(Self {
            endpoint: ep.clone(),
            options,
            conn: Mutex::new(ConnSlot::default()),
            dialed: Condvar::new(),
            dials: AtomicU64::new(0),
        });
        let conn = client.dial()?;
        lock(&client.conn).conn = Some(conn);
        Ok(client)
    }

    pub fn endpoint(&self) -> &WorkerEndpoint {
        &self.endpoint
    }

    /// Sessions dialed so far (the first one included).
    pub fn dials(&self) -> u64 {
        self.dials.load(Ordering::Relaxed)
    }

    /// Whether the current session is open.
    pub fn is_open(&self) -> bool {
        lock(&self.conn)
            .conn
            .as_ref()
            .is_some_and(|conn| conn.closed().is_none())
    }

    /// Closes the current session (calls in flight fail `ConnectionLost`);
    /// the next call dials a new one.
    pub fn close(&self) {
        let taken = lock(&self.conn).conn.take();
        if let Some(conn) = taken {
            conn.close();
        }
    }

    fn dial(&self) -> Result<Conn, CallError> {
        let ep = &self.endpoint;
        let conn = session::dial_loopback(&ep.address, ep.token.as_deref(), self.options)
            .map_err(|error| dial_error(ep, error))?;
        self.dials.fetch_add(1, Ordering::Relaxed);
        Ok(conn)
    }

    /// The open session, dialing a new one when the last one closed. The
    /// dial runs without the lock; concurrent callers wait for it (and get
    /// its error if it fails) instead of each dialing.
    pub fn conn(&self) -> Result<Conn, CallError> {
        let mut slot = lock(&self.conn);
        let mut waited_for: Option<u64> = None;
        loop {
            if let Some(conn) = slot.conn.as_ref()
                && conn.closed().is_none()
            {
                return Ok(conn.clone());
            }
            if let Some(finished) = waited_for
                && slot.finished != finished
                && let Some(error) = &slot.failed
            {
                return Err(error.clone());
            }
            if !slot.dialing {
                break;
            }
            waited_for = Some(slot.finished);
            slot = self
                .dialed
                .wait(slot)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        slot.dialing = true;
        drop(slot);
        let dialed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.dial()))
            .unwrap_or_else(|_| {
                Err(CallError::Unavailable(format!(
                    "LOCAL_WORKER_UNAVAILABLE: dialing worker {} panicked",
                    self.endpoint.instance
                )))
            });
        let mut slot = lock(&self.conn);
        slot.dialing = false;
        slot.finished += 1;
        let result = match dialed {
            Ok(conn) => {
                slot.conn = Some(conn.clone());
                slot.failed = None;
                Ok(conn)
            }
            Err(error) => {
                slot.failed = Some(error.clone());
                Err(error)
            }
        };
        drop(slot);
        self.dialed.notify_all();
        result
    }

    /// One request. A refusal that ran nothing (the session's `BUSY`, a full
    /// local queue) is sent again within `deadline`; any other outcome is
    /// returned as is (an uncertain one is never replayed, invariant 16).
    pub fn request(
        &self,
        op: &str,
        header: Value,
        body: Vec<u8>,
        prio: Priority,
        deadline: Duration,
    ) -> Result<SessionReply, CallError> {
        let started = Instant::now();
        let mut pause = REFUSED_RETRY_FIRST;
        let mut body = Some(body);
        loop {
            let conn = self.conn()?;
            let left = deadline
                .saturating_sub(started.elapsed())
                .max(Duration::from_millis(1));
            // The body is only copied when this attempt may be refused and
            // sent again (rare: a full handler queue).
            let attempt = match body.take() {
                Some(owned) if left <= pause => owned,
                Some(owned) => {
                    let copy = owned.clone();
                    body = Some(owned);
                    copy
                }
                None => Vec::new(),
            };
            let result = conn.call(op, header.clone(), attempt, prio, left);
            match result {
                Err(error) if refused(&error) && body.is_some() => {
                    let left = deadline.saturating_sub(started.elapsed());
                    if left <= pause {
                        return Err(error);
                    }
                    thread::sleep(pause);
                    pause = (pause * 2).min(REFUSED_RETRY_CAP);
                }
                other => return other,
            }
        }
    }

    /// Worker `objects.want`.
    pub fn want(
        &self,
        header: &WantHeader,
        deadline: Duration,
    ) -> Result<Vec<WantItem>, CallError> {
        let reply = self.request(
            wire::ops::WANT,
            to_header(header)?,
            Vec::new(),
            Priority::Control,
            deadline,
        )?;
        let reply: WantReply = json_reply(&reply, wire::ops::WANT)?;
        Ok(reply.items)
    }

    /// Worker `objects.read`: `(total, bytes)`.
    pub fn read(
        &self,
        header: &ReadHeader,
        deadline: Duration,
    ) -> Result<(u64, Vec<u8>), CallError> {
        let reply = self.request(
            wire::ops::READ,
            to_header(header)?,
            Vec::new(),
            Priority::Control,
            deadline,
        )?;
        let head: ReadReply = serde_json::from_value(reply.header)
            .map_err(|error| invalid_reply(wire::ops::READ, &error))?;
        Ok((head.total, reply.body))
    }

    /// Reads `ranges` with at most `inflight` `objects.read` calls in flight
    /// and hands each outcome to `consume` **in range order** (at most
    /// `inflight` results are held at once). `consume` sees `(total, bytes)`
    /// or the call's error, and decides what follows. Returns when every
    /// range was consumed or skipped, or `consume` stopped the stream (reads
    /// in flight then end on their own, within `deadline`).
    pub fn read_stream(
        &self,
        route: &SyncRoute,
        ranges: &[ReadRange],
        inflight: usize,
        deadline: Duration,
        consume: &mut ReadConsumer<'_>,
    ) {
        if ranges.is_empty() {
            return;
        }
        let inflight = inflight.clamp(1, ranges.len());
        let shared = ReadShared {
            state: Mutex::new(ReadState::default()),
            changed: Condvar::new(),
        };
        thread::scope(|scope| {
            for _ in 0..inflight {
                scope.spawn(|| read_loop(self, route, ranges, inflight, deadline, &shared));
            }
            // Ends the readers however the consumer leaves (a panic too):
            // the scope joins them, and one waiting for the window to move
            // would otherwise wait forever.
            let _stop = StopReaders(&shared);
            let mut index = 0;
            while index < ranges.len() {
                let outcome = {
                    let mut state = lock(&shared.state);
                    loop {
                        if let Some(outcome) = state.results.remove(&index) {
                            state.consumed = index + 1;
                            break outcome;
                        }
                        state = shared
                            .changed
                            .wait(state)
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                    }
                };
                shared.changed.notify_all();
                let range = &ranges[index];
                index += 1;
                let Some(result) = outcome else {
                    continue;
                };
                if lock(&shared.state).skipped.contains(&range.hash) {
                    continue;
                }
                match consume(range, result) {
                    ReadFlow::Continue => {}
                    ReadFlow::SkipObject => {
                        lock(&shared.state).skipped.insert(range.hash.clone());
                    }
                    ReadFlow::Stop => break,
                }
            }
        });
    }

    /// Worker `objects.write`: one call, `body` = the items' bytes in order.
    pub fn write_batch(
        &self,
        route: &SyncRoute,
        session: &str,
        items: Vec<WriteItem>,
        body: Vec<u8>,
        deadline: Duration,
    ) -> Result<Vec<WriteResult>, CallError> {
        let header = WriteHeader {
            route: route.clone(),
            session: session.to_owned(),
            items,
        };
        let reply = self.request(
            wire::ops::WRITE,
            to_header(&header)?,
            body,
            Priority::Data,
            deadline,
        )?;
        let reply: WriteReply = json_reply(&reply, wire::ops::WRITE)?;
        Ok(reply.results)
    }

    /// Worker `objects.report{route, hash, code}`.
    pub fn report(
        &self,
        route: &SyncRoute,
        hash: &str,
        code: &str,
        deadline: Duration,
    ) -> Result<ReportAction, CallError> {
        let header = ReportHeader {
            route: route.clone(),
            hash: hash.to_owned(),
            code: code.to_owned(),
        };
        let reply = self.request(
            wire::ops::REPORT,
            to_header(&header)?,
            Vec::new(),
            Priority::Control,
            deadline,
        )?;
        let reply: ReportReply = json_reply(&reply, wire::ops::REPORT)?;
        Ok(reply.action)
    }
}

impl WorkerClient for SessionWorkerClient {
    fn instance(&self) -> &str {
        &self.endpoint.instance
    }

    /// A JSON op as a header-only request; the reply is its JSON body, or
    /// its header when it has no body.
    fn call(&self, op: &str, payload: Value, deadline: Duration) -> Result<Value, CallError> {
        let payload = match payload {
            Value::Object(map) => Value::Object(map),
            Value::Null => Value::Object(Map::new()),
            other => {
                return Err(CallError::Unavailable(format!(
                    "INVALID_REQUEST: the payload of {op} must be a JSON object, not {other}"
                )));
            }
        };
        let reply = self.request(op, payload, Vec::new(), Priority::Control, deadline)?;
        if reply.body.is_empty() {
            Ok(reply.header)
        } else {
            serde_json::from_slice(&reply.body).map_err(|error| invalid_reply(op, &error))
        }
    }

    fn call_bytes(
        &self,
        op: &str,
        header: Value,
        body: &[u8],
        deadline: Duration,
    ) -> Result<(Value, Vec<u8>), CallError> {
        let prio = if body.is_empty() {
            Priority::Control
        } else {
            Priority::Data
        };
        let reply = self.request(op, header, body.to_vec(), prio, deadline)?;
        Ok((reply.header, reply.body))
    }
}

/// Whether a call was refused without running (worth sending again): the
/// local queue was full, or the worker session's handler queue was (`BUSY`,
/// the session layer's refusal: no handler took the request). Not
/// `WORKER_BUSY`: the worker's own answer, possibly after it handed the
/// command on (module docs, "Refusals").
fn refused(error: &CallError) -> bool {
    match error {
        CallError::Busy => true,
        CallError::Remote(wire) => wire.code == "BUSY",
        _ => false,
    }
}

fn dial_error(ep: &WorkerEndpoint, error: DialError) -> CallError {
    match error {
        DialError::NeedsUpgrade { detail } => CallError::Unavailable(format!(
            "UNSUPPORTED_OPERATION: worker {} does not serve objects v3 loopback sessions: {detail}",
            ep.instance
        )),
        DialError::Unauthorized(error) | DialError::Rejected(error) => {
            CallError::Unavailable(format!(
                "{}: worker {} refused the loopback session: {}",
                error.code, ep.instance, error.message
            ))
        }
        other => CallError::Unavailable(format!(
            "LOCAL_WORKER_UNAVAILABLE: worker {} at {}: {other}",
            ep.instance, ep.address
        )),
    }
}

fn to_header(value: &impl Serialize) -> Result<Value, CallError> {
    serde_json::to_value(value).map_err(|error| {
        CallError::Unavailable(format!(
            "INVALID_REQUEST: cannot encode the request: {error}"
        ))
    })
}

fn invalid_reply(op: &str, error: &dyn fmt::Display) -> CallError {
    CallError::Remote(ErrorWire::coded(
        "INVALID_REPLY",
        format!("the worker's {op} reply does not parse: {error}"),
    ))
}

/// A JSON reply: the response body when there is one, else the header.
fn json_reply<T: DeserializeOwned>(reply: &SessionReply, op: &str) -> Result<T, CallError> {
    if reply.body.is_empty() {
        serde_json::from_value(reply.header.clone()).map_err(|error| invalid_reply(op, &error))
    } else {
        serde_json::from_slice(&reply.body).map_err(|error| invalid_reply(op, &error))
    }
}

type ReadOutcome = Option<Result<(u64, Vec<u8>), CallError>>;

#[derive(Default)]
struct ReadState {
    /// Next range index a reader takes.
    next: usize,
    /// Ranges handed to the consumer so far.
    consumed: usize,
    /// Results waiting for the consumer (`None`: skipped, never read).
    results: BTreeMap<usize, ReadOutcome>,
    skipped: HashSet<String>,
    stop: bool,
}

struct ReadShared {
    state: Mutex<ReadState>,
    changed: Condvar,
}

/// Stops a read stream's readers when dropped.
struct StopReaders<'a>(&'a ReadShared);

impl Drop for StopReaders<'_> {
    fn drop(&mut self) {
        lock(&self.0.state).stop = true;
        self.0.changed.notify_all();
    }
}

/// One reader of [`SessionWorkerClient::read_stream`]: takes ranges in
/// order while fewer than `window` are ahead of the consumer.
fn read_loop(
    client: &SessionWorkerClient,
    route: &SyncRoute,
    ranges: &[ReadRange],
    window: usize,
    deadline: Duration,
    shared: &ReadShared,
) {
    loop {
        let (index, skip) = {
            let mut state = lock(&shared.state);
            loop {
                if state.stop || state.next >= ranges.len() {
                    return;
                }
                if state.next < state.consumed + window {
                    break;
                }
                state = shared
                    .changed
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            let index = state.next;
            state.next += 1;
            (index, state.skipped.contains(&ranges[index].hash))
        };
        let outcome = if skip {
            None
        } else {
            let range = &ranges[index];
            let header = ReadHeader {
                route: route.clone(),
                hash: range.hash.clone(),
                offset: range.offset,
                len: range.len,
            };
            // A panicking read is an error result, so the consumer, which
            // waits for this range, is never left waiting.
            let read = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                client.read(&header, deadline)
            }));
            Some(read.unwrap_or_else(|_| {
                Err(CallError::Remote(ErrorWire::coded(
                    "UNKNOWN_ERROR",
                    "objects.read panicked",
                )))
            }))
        };
        lock(&shared.state).results.insert(index, outcome);
        shared.changed.notify_all();
    }
}

enum Cached {
    Client(Arc<SessionWorkerClient>),
    /// The instance has no loopback sessions (its `UNSUPPORTED_OPERATION`).
    Unsupported(String),
}

struct CacheEntry {
    address: String,
    cached: Cached,
    used: u64,
}

fn sessions() -> &'static Mutex<HashMap<String, CacheEntry>> {
    static SESSIONS: OnceLock<Mutex<HashMap<String, CacheEntry>>> = OnceLock::new();
    SESSIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

static CACHE_CLOCK: AtomicU64 = AtomicU64::new(0);

/// The loopback session client of worker instance `ep` (a per-instance
/// cache; a worker instance never changes, so neither does whether it serves
/// loopback sessions). A failed dial other than "no loopback sessions" is not
/// remembered: the next call dials again.
pub fn session_for(ep: &WorkerEndpoint) -> Result<Arc<SessionWorkerClient>, CallError> {
    let tick = CACHE_CLOCK.fetch_add(1, Ordering::Relaxed);
    {
        let mut map = lock(sessions());
        if let Some(entry) = map.get_mut(&ep.instance)
            && entry.address == ep.address
        {
            entry.used = tick;
            match &entry.cached {
                Cached::Client(client) => return Ok(client.clone()),
                Cached::Unsupported(detail) => {
                    return Err(CallError::Unavailable(detail.clone()));
                }
            }
        }
    }
    let dialed = SessionWorkerClient::connect(ep);
    let mut map = lock(sessions());
    let mut evicted = Vec::new();
    let result = match dialed {
        Ok(client) => {
            if let Some(entry) = map.get(&ep.instance)
                && entry.address == ep.address
                && let Cached::Client(existing) = &entry.cached
            {
                // Another caller dialed it meanwhile: keep that one.
                client.close();
                Ok(existing.clone())
            } else {
                evicted.extend(map.insert(
                    ep.instance.clone(),
                    CacheEntry {
                        address: ep.address.clone(),
                        cached: Cached::Client(client.clone()),
                        used: tick,
                    },
                ));
                Ok(client)
            }
        }
        Err(error) if error.code() == "UNSUPPORTED_OPERATION" => {
            evicted.extend(map.insert(
                ep.instance.clone(),
                CacheEntry {
                    address: ep.address.clone(),
                    cached: Cached::Unsupported(error.to_string()),
                    used: tick,
                },
            ));
            Err(error)
        }
        Err(error) => Err(error),
    };
    if map.len() > MAX_CACHED_SESSIONS {
        // Closed sessions go first. Whether one is open is asked without
        // the cache's lock (each client's own lock is never held across
        // I/O, but the cache's is everyone's).
        let clients: Vec<(String, Arc<SessionWorkerClient>)> = map
            .iter()
            .filter_map(|(instance, entry)| match &entry.cached {
                Cached::Client(client) => Some((instance.clone(), client.clone())),
                Cached::Unsupported(_) => None,
            })
            .collect();
        drop(map);
        let closed: Vec<(String, Arc<SessionWorkerClient>)> = clients
            .into_iter()
            .filter(|(_, client)| !client.is_open())
            .collect();
        map = lock(sessions());
        for (instance, client) in closed {
            let same = map.get(&instance).is_some_and(|entry| {
                matches!(&entry.cached, Cached::Client(cached) if Arc::ptr_eq(cached, &client))
            });
            if same {
                evicted.extend(map.remove(&instance));
            }
        }
        while map.len() > MAX_CACHED_SESSIONS {
            let Some(oldest) = map
                .iter()
                .min_by_key(|(_, entry)| entry.used)
                .map(|(instance, _)| instance.clone())
            else {
                break;
            };
            evicted.extend(map.remove(&oldest));
        }
    }
    drop(map);
    for entry in evicted {
        if let Cached::Client(client) = entry.cached {
            client.close();
        }
    }
    result
}

/// Forgets (and closes) the cached session client of `instance`, if any.
pub fn drop_session(instance: &str) {
    let removed = lock(sessions()).remove(instance);
    if let Some(CacheEntry {
        cached: Cached::Client(client),
        ..
    }) = removed
    {
        client.close();
    }
}
