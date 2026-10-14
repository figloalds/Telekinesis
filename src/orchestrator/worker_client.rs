//! Owned by WP1.3.
//!
//! The JSON [`WorkerClient`] (REVAMP_PLAN §3.3, §6.1, §14 WP1.3): requests
//! `{"request": id, "payload": {"op": …}}` over the worker's loopback
//! WebSocket endpoint, the same envelope and `{ok, result | error}` reply as
//! `ws::call`. WP3.3 adds the v3 loopback session client and WP4.1 the worker
//! token.
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
//! * `call_bytes` → `Unavailable(BINARY_OPS_REQUIRE_SESSION)` until WP3.3.
use crate::model::{
    errors::{ErrorWire, UNKNOWN_CODE, code_in},
    orchestrator::{CallError, WorkerClient, WorkerEndpoint},
};
use serde_json::{Map, Value};
use std::{
    fmt, io,
    net::{TcpStream, ToSocketAddrs},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
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
        Arc::new(Self {
            endpoint,
            options,
            idle: Mutex::new(Vec::new()),
            connects: AtomicU64::new(0),
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
        let request = address
            .as_str()
            .into_client_request()
            .map_err(|e| unavailable(format!("{e}")))?;
        match tungstenite::client::client_with_config(request, stream, Some(config())) {
            Ok((socket, _)) => {
                self.connects.fetch_add(1, Ordering::Relaxed);
                Ok(socket)
            }
            // A socket read timeout surfaces as an interrupted handshake.
            Err(HandshakeError::Interrupted(_)) => {
                Err(unavailable("WebSocket upgrade timed out".into()))
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

    fn call_bytes(
        &self,
        op: &str,
        _header: Value,
        _body: &[u8],
        _deadline: Duration,
    ) -> Result<(Value, Vec<u8>), CallError> {
        Err(CallError::Unavailable(format!(
            "BINARY_OPS_REQUIRE_SESSION: {op} needs a v3 loopback session (Wave 3)"
        )))
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
