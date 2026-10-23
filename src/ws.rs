//! The only wire transport: bounded JSON messages over persistent WebSockets.
use anyhow::{Context, Result, bail, ensure};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    time::{Duration, Instant},
};
use tungstenite::{
    Connector, Message, WebSocket, client::IntoClientRequest, protocol::WebSocketConfig,
    stream::MaybeTlsStream,
};

pub struct Socket {
    inner: WebSocket<MaybeTlsStream<TcpStream>>,
    binary: bool,
    pub sent_bytes: u64,
    pub received_bytes: u64,
}
impl Socket {
    fn new(inner: WebSocket<MaybeTlsStream<TcpStream>>, binary: bool) -> Self {
        Self {
            inner,
            binary,
            sent_bytes: 0,
            received_bytes: 0,
        }
    }
    pub fn codec(&self) -> &'static str {
        if self.binary {
            crate::wire::PROTOCOL
        } else {
            "json"
        }
    }
}
impl std::ops::Deref for Socket {
    type Target = WebSocket<MaybeTlsStream<TcpStream>>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl std::ops::DerefMut for Socket {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
pub const MAX_MESSAGE: usize = 64 * 1024 * 1024;

/// A complete error reply leaves the connection usable. Transport and malformed
/// reply errors do not: callers must reconnect before issuing another request.
#[derive(Debug)]
pub struct RemoteError(pub String);

impl std::fmt::Display for RemoteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RemoteError {}

fn config() -> WebSocketConfig {
    WebSocketConfig::default()
        .max_message_size(Some(MAX_MESSAGE))
        .max_frame_size(Some(MAX_MESSAGE))
}

/// Shared desktop/transport validation. WSS uses the platform's certificate trust.
pub fn endpoint_uri(endpoint: &str) -> Result<tungstenite::http::Uri> {
    let uri: tungstenite::http::Uri = endpoint.parse().context("INVALID_WS_ENDPOINT")?;
    ensure!(
        matches!(uri.scheme_str(), Some("ws" | "wss")),
        "WS_OR_WSS_ENDPOINT_REQUIRED"
    );
    let host = uri
        .host()
        .filter(|host| !host.is_empty())
        .context("WS_HOST_REQUIRED")?;
    let authority = uri.authority().context("WS_HOST_REQUIRED")?.as_str();
    ensure!(!authority.contains('@'), "WS_USERINFO_NOT_SUPPORTED");
    // Uri::port() also returns None for an out-of-range port; don't silently
    // interpret an explicitly invalid port as the scheme's default.
    let port_suffix = authority
        .strip_prefix(host)
        .context("INVALID_WS_ENDPOINT")?;
    ensure!(
        port_suffix.is_empty()
            || port_suffix
                .strip_prefix(':')
                .and_then(|p| p.parse::<u16>().ok())
                .is_some(),
        "INVALID_WS_PORT"
    );
    Ok(uri)
}

/// Connects to a JSON WebSocket endpoint. A loopback endpoint this process
/// knows a local token for ([`tokens::lookup`]: a server of this process, or
/// one it was told about) is sent that token; see [`connect_authorized`].
pub fn connect(endpoint: &str) -> Result<Socket> {
    connect_with_connector(endpoint, None, None)
}

/// [`connect`] presenting `token` as `X-Tkfs-Token` on the upgrade (§14
/// WP4.1 local tokens). With `None`, the token [`tokens::lookup`] knows for
/// the address, if any. An endpoint that refuses the token answers the
/// upgrade with HTTP 403: `ACCESS_DENIED`.
pub fn connect_authorized(endpoint: &str, token: Option<&str>) -> Result<Socket> {
    connect_with_connector(endpoint, None, token)
}

fn connect_with_connector(
    endpoint: &str,
    connector: Option<Connector>,
    token: Option<&str>,
) -> Result<Socket> {
    let uri = endpoint_uri(endpoint)?;
    let host = uri
        .host()
        .context("WS_HOST_REQUIRED")?
        .trim_matches(['[', ']']);
    let port = uri
        .port_u16()
        .unwrap_or(if uri.scheme_str() == Some("wss") {
            443
        } else {
            80
        });
    let mut last = None;
    for address in (host, port).to_socket_addrs()? {
        match TcpStream::connect_timeout(&address, Duration::from_secs(3)) {
            Ok(stream) => {
                timeouts(&stream)?;
                let mut request = endpoint.into_client_request()?;
                request
                    .headers_mut()
                    .insert("X-Tkfs-Wire", crate::wire::PROTOCOL.parse()?);
                let presented = token.map(str::to_owned).or_else(|| tokens::lookup(address));
                if let Some(presented) = presented {
                    request.headers_mut().insert(
                        tokens::HEADER,
                        tungstenite::http::HeaderValue::from_str(&presented)
                            .context("INVALID_REQUEST: the local token is not a header value")?,
                    );
                }
                let (socket, response) =
                    tungstenite::client_tls_with_config(request, stream, Some(config()), connector)
                        .map_err(|error| refused(endpoint, handshake_failure(error)))?;
                let binary = response
                    .headers()
                    .get("X-Tkfs-Wire")
                    .is_some_and(|value| value == crate::wire::PROTOCOL);
                return Ok(Socket::new(socket, binary));
            }
            Err(error) => last = Some(error),
        }
    }
    bail!(
        "WS_CONNECT_FAILED: {}",
        last.map(|e| e.to_string()).unwrap_or_default()
    )
}

/// A failed client upgrade; an HTTP 401/403 refusal is `ACCESS_DENIED` (the
/// endpoint wants a local token this client did not present, or another).
fn refused(endpoint: &str, error: tungstenite::Error) -> anyhow::Error {
    let status = match &error {
        tungstenite::Error::Http(response) => Some(response.status().as_u16()),
        _ => None,
    };
    match status {
        Some(status @ (401 | 403)) => anyhow::Error::new(error).context(format!(
            "ACCESS_DENIED: {endpoint} refused the upgrade (HTTP {status}): a local token is \
             required (X-Tkfs-Token)"
        )),
        _ => anyhow::Error::new(error),
    }
}

pub fn timeouts(stream: &TcpStream) -> Result<()> {
    // Windows inherits the listener's nonblocking mode on accepted sockets.
    stream.set_nonblocking(false)?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(15)))?;
    stream.set_write_timeout(Some(Duration::from_secs(15)))?;
    Ok(())
}

/// Accepts a JSON WebSocket connection. When this process protects the
/// listener's address with a local token ([`tokens::protect`]), the upgrade
/// must carry it; see [`accept_authorized`]. Fails closed: a socket whose
/// local address cannot be read is refused, since its listener may be a
/// protected one (WP4.INT, the WP4.1 review's minor). The daemon's control
/// server does not rely on this lookup: it calls [`accept_authorized`] with
/// its token (CCR-WP4.1-3).
#[allow(clippy::result_large_err)]
pub fn accept(stream: TcpStream) -> Result<Socket> {
    let local = stream.local_addr().context(
        "ACCESS_DENIED: the listener's address is unknown, so its local token cannot be checked",
    )?;
    let required = tokens::enforced(local);
    accept_authorized(stream, required.as_deref())
}

/// [`accept`] requiring `token` (when `Some`) as the upgrade's `X-Tkfs-Token`
/// (§14 WP4.1: a worker's JSON RPC, the daemon's control endpoint). A missing
/// or different token is refused before the upgrade completes: HTTP 403, and
/// `ACCESS_DENIED` here. The comparison is constant-time, and nothing logged
/// carries a token.
#[allow(clippy::result_large_err)]
pub fn accept_authorized(stream: TcpStream, token: Option<&str>) -> Result<Socket> {
    timeouts(&stream)?;
    let peer = stream.peer_addr().ok();
    let negotiated = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let selected = negotiated.clone();
    let denied = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let refusal = denied.clone();
    let inner = tungstenite::accept_hdr_with_config(
        MaybeTlsStream::Plain(stream),
        move |request: &tungstenite::handshake::server::Request,
              mut response: tungstenite::handshake::server::Response| {
            if let Some(expected) = token
                && !tokens::matches(request.headers().get(tokens::HEADER), expected)
            {
                refusal.store(true, std::sync::atomic::Ordering::Relaxed);
                let mut refused = tungstenite::handshake::server::ErrorResponse::new(None);
                *refused.status_mut() = tungstenite::http::StatusCode::FORBIDDEN;
                return Err(refused);
            }
            let binary = request
                .headers()
                .get("X-Tkfs-Wire")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| {
                    value
                        .split(',')
                        .any(|item| item.trim() == crate::wire::PROTOCOL)
                });
            if binary {
                response
                    .headers_mut()
                    .insert("X-Tkfs-Wire", crate::wire::PROTOCOL.parse().unwrap());
            }
            selected.store(binary, std::sync::atomic::Ordering::Relaxed);
            Ok(response)
        },
        Some(config()),
    );
    let inner = match inner {
        Ok(inner) => inner,
        Err(error) if denied.load(std::sync::atomic::Ordering::Relaxed) => {
            crate::util::log::event!(info, ACCESS_DENIED, {
                "site": "ws.accept",
                "peer": peer.map(|peer| peer.to_string()),
                "detail": "upgrade without the local token refused",
            });
            return Err(anyhow::Error::new(handshake_failure(error))
                .context("ACCESS_DENIED: the upgrade did not carry the local token"));
        }
        Err(error) => return Err(handshake_failure(error).into()),
    };
    Ok(Socket::new(
        inner,
        negotiated.load(std::sync::atomic::Ordering::Relaxed),
    ))
}

/// A raw WebSocket over TCP or TLS, as the v3 session layer (`session`) uses
/// it: binary frames, no JSON codec.
pub type RawSocket = WebSocket<MaybeTlsStream<TcpStream>>;

/// Dials `endpoint` with extra upgrade `headers` (for example
/// `X-Tkfs-Sync: 3`) and returns the raw WebSocket together with the server's
/// upgrade response, so the caller can check what the server echoed.
///
/// Every resolved address is tried in turn, and `timeout` is the budget of
/// one address's whole attempt: the TCP connect plus the TLS and WebSocket
/// upgrade share it (the upgrade gets what the connect left, as its socket
/// read/write timeouts and as a watchdog deadline that shuts down the socket
/// of a server trickling its response). The first address whose upgrade
/// succeeds wins. The returned socket keeps those socket timeouts (at most
/// `timeout`) and the default 64 MiB message bound; callers set their own.
/// `timeout` must be non-zero. Unlike [`connect`], no `X-Tkfs-Wire` codec is
/// offered. An attempt whose watchdog cannot be started fails rather than
/// run unbounded.
///
/// Errors carry `WS_CONNECT_FAILED` (or `INVALID_WS_ENDPOINT` and friends for
/// a malformed endpoint); the last address's `tungstenite::Error` stays in the
/// chain, so a caller can tell an HTTP refusal (`Error::Http`) apart.
pub fn connect_with(
    endpoint: &str,
    headers: &[(&str, &str)],
    connector: Option<Connector>,
    timeout: Duration,
) -> Result<(RawSocket, tungstenite::handshake::client::Response)> {
    connect_with_cancel(endpoint, headers, connector, timeout, None)
}

/// [`connect_with`], interruptible by `cancel` (CCR-WP2.6-2, applied by
/// WP3.1): it is checked before each address, and once an address's TCP
/// connect returns, a hook on `cancel` shuts that socket down (through a
/// verified duplicate, as the watchdog's). The hook stays registered for as
/// long as `cancel` lives: a caller passes a short-lived token (a `child()`
/// it drops after the dial, see `util::cancel`), so the session handshake
/// that follows is covered too. On Unix the shutdown ends a read in
/// progress at once. On Windows it sends our FIN, which ends the read as
/// soon as the peer closes in turn (any tkfs server, proxy or relay does);
/// a peer that ignores it is still bounded by the step's socket timeout.
/// A TCP connect itself is bounded by `timeout` but not interrupted. A
/// cancelled dial fails with `WS_CONNECT_FAILED: dial cancelled`.
pub fn connect_with_cancel(
    endpoint: &str,
    headers: &[(&str, &str)],
    connector: Option<Connector>,
    timeout: Duration,
    cancel: Option<&crate::util::cancel::Cancel>,
) -> Result<(RawSocket, tungstenite::handshake::client::Response)> {
    let uri = endpoint_uri(endpoint)?;
    let host = uri
        .host()
        .context("WS_HOST_REQUIRED")?
        .trim_matches(['[', ']'])
        .to_owned();
    let port = uri
        .port_u16()
        .unwrap_or(if uri.scheme_str() == Some("wss") {
            443
        } else {
            80
        });
    let mut request = endpoint.into_client_request()?;
    for (name, value) in headers {
        request.headers_mut().insert(
            tungstenite::http::HeaderName::from_bytes(name.as_bytes())
                .context("INVALID_REQUEST: upgrade header name")?,
            tungstenite::http::HeaderValue::from_str(value)
                .context("INVALID_REQUEST: upgrade header value")?,
        );
    }
    let addresses: Vec<SocketAddr> = (host.as_str(), port)
        .to_socket_addrs()
        .with_context(|| format!("WS_CONNECT_FAILED: cannot resolve {host}"))?
        .collect();
    connect_any(&host, &addresses, &request, connector, timeout, cancel)
}

/// The error of a dial its token cancelled.
fn dial_cancelled() -> anyhow::Error {
    anyhow::anyhow!("WS_CONNECT_FAILED: dial cancelled")
}

/// [`connect_with`] over already-resolved `addresses`, in order.
fn connect_any(
    host: &str,
    addresses: &[SocketAddr],
    request: &tungstenite::handshake::client::Request,
    connector: Option<Connector>,
    timeout: Duration,
    cancel: Option<&crate::util::cancel::Cancel>,
) -> Result<(RawSocket, tungstenite::handshake::client::Response)> {
    let mut connector = connector;
    let mut last = None;
    for (index, address) in addresses.iter().enumerate() {
        if cancel.is_some_and(crate::util::cancel::Cancel::is_cancelled) {
            return Err(dial_cancelled());
        }
        let last_address = index + 1 == addresses.len();
        // A connector that cannot be copied is used once (never silently
        // replaced by the default trust store on a later address).
        let (attempt, final_attempt) = match connector.as_ref().map(clone_connector) {
            None => (None, last_address),
            Some(Some(copy)) if !last_address => (Some(copy), false),
            Some(_) => (connector.take(), true),
        };
        match connect_one(*address, request, attempt, timeout, cancel) {
            Ok(connected) => return Ok(connected),
            Err(error) => last = Some(error.context(format!("WS_CONNECT_FAILED: {address}"))),
        }
        if final_attempt {
            break;
        }
    }
    Err(last.unwrap_or_else(|| anyhow::anyhow!("WS_CONNECT_FAILED: {host} resolved to no address")))
}

fn clone_connector(connector: &Connector) -> Option<Connector> {
    match connector {
        Connector::Plain => Some(Connector::Plain),
        Connector::NativeTls(tls) => Some(Connector::NativeTls(tls.clone())),
        #[allow(unreachable_patterns)]
        _ => None,
    }
}

fn connect_one(
    address: SocketAddr,
    request: &tungstenite::handshake::client::Request,
    connector: Option<Connector>,
    timeout: Duration,
    cancel: Option<&crate::util::cancel::Cancel>,
) -> Result<(RawSocket, tungstenite::handshake::client::Response)> {
    // One deadline for the whole attempt: the upgrade gets what the TCP
    // connect left of `timeout`.
    let deadline = Instant::now() + timeout;
    let stream = TcpStream::connect_timeout(&address, timeout)?;
    if let Some(cancel) = cancel {
        // A verified duplicate, as the watchdog's (on Windows a plain clone
        // taken right after the connect can be stale, and its `shutdown`
        // would fail). The hook runs at once if the token is already
        // cancelled.
        let killer = watchdog::watchdog_handle(&stream)?;
        cancel.on_cancel(Box::new(move || {
            let _ = killer.shutdown(std::net::Shutdown::Both);
        }));
        if cancel.is_cancelled() {
            return Err(dial_cancelled());
        }
    }
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(upgrade_timed_out().into());
    }
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(remaining))?;
    stream.set_write_timeout(Some(remaining))?;
    // The socket timeouts bound each read; the watchdog bounds the whole TLS
    // and WebSocket upgrade, so a server trickling its response cannot hold
    // the dialer. No watchdog, no attempt.
    let watchdog =
        watchdog::Watchdog::start(&stream).context("websocket upgrade watchdog could not start")?;
    watchdog.arm(remaining);
    let result =
        tungstenite::client_tls_with_config(request.clone(), stream, Some(config()), connector);
    watchdog.disarm();
    let fired = watchdog.fired();
    match result {
        // The watchdog shut the socket down: even a completed upgrade is
        // unusable.
        Ok(_) if fired => Err(upgrade_timed_out().into()),
        Ok(connected) => Ok(connected),
        Err(error) if fired => {
            Err(anyhow::Error::new(handshake_failure(error)).context("websocket upgrade timed out"))
        }
        Err(error) => Err(anyhow::Error::new(handshake_failure(error))),
    }
}

fn upgrade_timed_out() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::TimedOut, "websocket upgrade timed out")
}

/// A blocking handshake that stopped on `WouldBlock` timed out (Unix reports
/// a read timeout as `WouldBlock`, Windows as `TimedOut`).
fn handshake_failure<R: tungstenite::handshake::HandshakeRole>(
    error: tungstenite::HandshakeError<R>,
) -> tungstenite::Error {
    match error {
        tungstenite::HandshakeError::Failure(error) => error,
        tungstenite::HandshakeError::Interrupted(_) => tungstenite::Error::Io(upgrade_timed_out()),
    }
}

/// Accepts a WebSocket upgrade on `stream` with every incoming message (and
/// frame) bounded by `max_message` bytes.
///
/// `on_request` sees the upgrade request and may add response headers (for
/// example the `X-Tkfs-Sync` echo). Setting a status other than
/// `101 Switching Protocols` on the response refuses the upgrade: that status
/// is sent with no headers and an error is returned.
///
/// The stream is switched to blocking mode with `TCP_NODELAY`. Socket
/// timeouts already set by the caller are kept; a stream with none gets the
/// legacy 15 s, so a silent client cannot hold the upgrade forever.
#[allow(clippy::result_large_err)]
pub fn accept_with(
    stream: TcpStream,
    max_message: usize,
    on_request: impl FnOnce(
        &tungstenite::handshake::server::Request,
        &mut tungstenite::handshake::server::Response,
    ),
) -> Result<RawSocket> {
    // Windows inherits the listener's nonblocking mode on accepted sockets.
    stream.set_nonblocking(false)?;
    stream.set_nodelay(true)?;
    if stream.read_timeout()?.is_none() {
        stream.set_read_timeout(Some(Duration::from_secs(15)))?;
    }
    if stream.write_timeout()?.is_none() {
        stream.set_write_timeout(Some(Duration::from_secs(15)))?;
    }
    let config = WebSocketConfig::default()
        .max_message_size(Some(max_message))
        .max_frame_size(Some(max_message));
    tungstenite::accept_hdr_with_config(
        MaybeTlsStream::Plain(stream),
        move |request: &tungstenite::handshake::server::Request,
              mut response: tungstenite::handshake::server::Response| {
            on_request(request, &mut response);
            if response.status() == tungstenite::http::StatusCode::SWITCHING_PROTOCOLS {
                Ok(response)
            } else {
                let mut refusal = tungstenite::handshake::server::ErrorResponse::new(None);
                *refusal.status_mut() = response.status();
                Err(refusal)
            }
        },
        Some(config),
    )
    .map_err(|error| anyhow::Error::new(handshake_failure(error)))
}

pub fn send<T: Serialize>(socket: &mut Socket, value: &T) -> Result<()> {
    if socket.binary {
        let bytes = crate::wire::encode(serde_json::to_value(value)?, MAX_MESSAGE)?;
        socket.sent_bytes += bytes.len() as u64;
        socket.send(Message::Binary(bytes.into()))?;
        return Ok(());
    }
    let bytes = serde_json::to_string(value)?;
    ensure!(bytes.len() <= MAX_MESSAGE, "WS_MESSAGE_TOO_LARGE");
    socket.sent_bytes += bytes.len() as u64;
    socket.send(Message::Text(bytes.into()))?;
    Ok(())
}

pub fn receive<T: DeserializeOwned>(socket: &mut Socket) -> Result<T> {
    loop {
        match socket.read()? {
            Message::Text(bytes) => {
                socket.received_bytes += bytes.len() as u64;
                return Ok(serde_json::from_str(&bytes)?);
            }
            Message::Binary(bytes) => {
                socket.received_bytes += bytes.len() as u64;
                return if socket.binary {
                    Ok(serde_json::from_value(crate::wire::decode(
                        &bytes,
                        MAX_MESSAGE,
                    )?)?)
                } else {
                    Ok(serde_json::from_slice(&bytes)?)
                };
            }
            Message::Ping(_) => socket.flush()?,
            Message::Pong(_) => {}
            Message::Close(_) => bail!("WS_CLOSED"),
            _ => bail!("INVALID_WS_MESSAGE"),
        }
    }
}

pub fn reply(socket: &mut Socket, result: Result<Value>) -> Result<()> {
    send(
        socket,
        &match result {
            Ok(value) => json!({"ok": true, "result": value}),
            Err(error) => json!({"ok": false, "error": format!("{error:#}")}),
        },
    )
}

pub fn call<T: Serialize>(socket: &mut Socket, request: &T) -> Result<Value> {
    send(socket, request)?;
    let response: Value = receive(socket)?;
    match response.get("ok").and_then(Value::as_bool) {
        Some(true) => response
            .get("result")
            .cloned()
            .context("INVALID_WS_REPLY: missing result"),
        Some(false) => {
            let error = response["error"]
                .as_str()
                .context("INVALID_WS_REPLY: missing error")?;
            Err(RemoteError(error.to_owned()).into())
        }
        None => bail!("INVALID_WS_REPLY: missing boolean ok"),
    }
}

/// A deadline on a whole blocking exchange over one TCP socket (a WebSocket
/// upgrade, a handshake step). Socket timeouts bound each `read`/`write`
/// call; a watchdog bounds the whole exchange, so a peer that trickles one
/// byte at a time cannot hold it open. [`connect_with`] uses it for the
/// upgrade, and the v3 handshake (`session::handshake`) for every step. It
/// lives here, in the transport, so the transport never depends on the
/// session layer.
pub(crate) mod watchdog {
    use std::{
        io,
        net::{Shutdown, TcpStream},
        sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError},
        thread::{self, JoinHandle},
        time::{Duration, Instant},
    };

    /// The socket timeouts an expired watchdog leaves: the next blocking call
    /// on the socket returns at once.
    const EXPIRED_TIMEOUT: Duration = Duration::from_millis(1);

    /// Ends a blocking exchange that overruns its armed budget: it shrinks
    /// the socket timeouts and shuts the socket down (through a duplicate
    /// handle; see [`watchdog_handle`]).
    pub(crate) struct Watchdog {
        shared: Arc<(Mutex<Watch>, Condvar)>,
        thread: Option<JoinHandle<()>>,
    }

    #[derive(Default)]
    struct Watch {
        deadline: Option<Instant>,
        fired: bool,
        done: bool,
    }

    fn lock_watch(mutex: &Mutex<Watch>) -> MutexGuard<'_, Watch> {
        mutex.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[cfg(test)]
    thread_local! {
        /// Test hook: makes [`Watchdog::start`] fail on this thread, as a
        /// failed thread spawn would.
        pub(crate) static REFUSE_WATCHDOG: std::cell::Cell<bool> =
            const { std::cell::Cell::new(false) };
        /// Test hook: a watchdog started on this thread finds its `shutdown`
        /// failing when it fires (as a stale Windows duplicate would).
        pub(crate) static FAIL_SHUTDOWN: std::cell::Cell<bool> =
            const { std::cell::Cell::new(false) };
    }

    impl Watchdog {
        /// Starts a watchdog for `stream`, initially disarmed.
        ///
        /// Fails when no verified duplicate handle can be made or the thread
        /// cannot be spawned. Callers then refuse the connection: without a
        /// watchdog only the per-call socket timeouts apply, and a peer
        /// trickling bytes (or fragments) would be bounded by nothing (fail
        /// closed, invariant 22).
        pub(crate) fn start(stream: &TcpStream) -> io::Result<Watchdog> {
            let handle = watchdog_handle(stream)?;
            #[cfg(test)]
            if REFUSE_WATCHDOG.with(std::cell::Cell::get) {
                return Err(io::Error::other("watchdog thread refused (test hook)"));
            }
            #[cfg(test)]
            let shutdown_fails = FAIL_SHUTDOWN.with(std::cell::Cell::get);
            #[cfg(not(test))]
            let shutdown_fails = false;
            let shared = Arc::new((Mutex::new(Watch::default()), Condvar::new()));
            let watched = shared.clone();
            let thread = thread::Builder::new()
                .name("tkfs-handshake-watchdog".into())
                .spawn(move || watch(&watched, &handle, shutdown_fails))?;
            Ok(Watchdog {
                shared,
                thread: Some(thread),
            })
        }

        /// A watchdog with no thread, which never fires (tests of the bounds
        /// that must hold without one).
        #[cfg(test)]
        pub(crate) fn inert() -> Watchdog {
            Watchdog {
                shared: Arc::new((Mutex::new(Watch::default()), Condvar::new())),
                thread: None,
            }
        }

        pub(crate) fn arm(&self, budget: Duration) {
            lock_watch(&self.shared.0).deadline = Some(Instant::now() + budget);
            self.shared.1.notify_all();
        }

        /// Once `disarm` returns, [`Watchdog::fired`] no longer changes for
        /// the budget just disarmed.
        pub(crate) fn disarm(&self) {
            lock_watch(&self.shared.0).deadline = None;
            self.shared.1.notify_all();
        }

        pub(crate) fn fired(&self) -> bool {
            lock_watch(&self.shared.0).fired
        }
    }

    impl Drop for Watchdog {
        fn drop(&mut self) {
            lock_watch(&self.shared.0).done = true;
            self.shared.1.notify_all();
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    /// A duplicate handle the watchdog can shut the socket down with, or an
    /// error when none can be verified (the caller then refuses the
    /// connection: fail closed).
    ///
    /// On Windows, a duplicate taken right after a non-blocking connect
    /// (what `TcpStream::connect_timeout` does) can carry a stale "not
    /// connected" state, and `shutdown` on it then fails with `WSAENOTCONN`,
    /// so an overrunning exchange would not be interrupted. A stale duplicate
    /// stays stale (measured: 23 of 3,000 connects gave one, and none of them
    /// recovered on a second `getpeername` or `shutdown`), but a `getpeername`
    /// on the original refreshes the state a new duplicate is created from
    /// (each of those 23 re-duplicated cleanly). So the original is refreshed
    /// before each clone and the duplicate verified the same way. A duplicate
    /// that fails verification three times is never used: its `shutdown`
    /// would fail when it matters. Elsewhere the first clone verifies, unless
    /// the connection is already gone.
    pub(crate) fn watchdog_handle(stream: &TcpStream) -> io::Result<TcpStream> {
        let mut last = None;
        for _ in 0..3 {
            let _ = stream.peer_addr();
            let duplicate = stream.try_clone()?;
            match duplicate.peer_addr() {
                Ok(_) => return Ok(duplicate),
                Err(error) => last = Some(error),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::NotConnected,
            format!(
                "no verified socket handle for the watchdog: {}",
                last.map(|error| error.to_string()).unwrap_or_default()
            ),
        ))
    }

    fn watch(shared: &(Mutex<Watch>, Condvar), stream: &TcpStream, shutdown_fails: bool) {
        let (mutex, condvar) = shared;
        let mut state = lock_watch(mutex);
        loop {
            if state.done {
                return;
            }
            match state.deadline {
                None => {
                    state = condvar.wait(state).unwrap_or_else(PoisonError::into_inner);
                }
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        state.fired = true;
                        state.deadline = None;
                        drop(state);
                        expire(stream, shutdown_fails);
                        return;
                    }
                    state = condvar
                        .wait_timeout(state, deadline - now)
                        .map(|(state, _)| state)
                        .unwrap_or_else(|error| error.into_inner().0);
                }
            }
        }
    }

    /// Ends the exchange: `shutdown` ends the call in progress and every
    /// later one.
    ///
    /// The socket timeouts are shrunk first, as a second line should
    /// `shutdown` fail (it is then logged as `WATCHDOG_SHUTDOWN_FAILED`). On
    /// Unix they belong to the socket, so every later blocking call on the
    /// owner's handle returns at once and a trickling peer is cut off at its
    /// next pause. On Windows they belong to the handle (measured: a timeout
    /// set through a duplicate leaves the original's unchanged), so there the
    /// bound rests on `shutdown` through the duplicate [`watchdog_handle`]
    /// verified. The one failure known there is a connection already gone,
    /// which ends the owner's call by itself.
    fn expire(stream: &TcpStream, shutdown_fails: bool) {
        let read = stream.set_read_timeout(Some(EXPIRED_TIMEOUT));
        let write = stream.set_write_timeout(Some(EXPIRED_TIMEOUT));
        let shutdown = if shutdown_fails {
            Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "shutdown refused (test hook)",
            ))
        } else {
            stream.shutdown(Shutdown::Both)
        };
        if let Err(error) = shutdown {
            crate::util::log::event!(warn, WATCHDOG_SHUTDOWN_FAILED, {
                "error": error.to_string(),
                "timeouts_shrunk": read.is_ok() && write.is_ok(),
            });
        }
    }
}

/// Local tokens (§14 WP4.1): the `X-Tkfs-Token` a loopback endpoint of this
/// installation requires on its WebSocket upgrade (the daemon's control
/// endpoint, a worker's JSON RPC and loopback sessions). Clients outside the
/// process read the token from its owner-only file (`DATA/orchestrator.token`,
/// `STATE/runtime.token`) and pass it explicitly
/// ([`super::connect_authorized`], `session::dial_loopback`).
///
/// This module is also a process-local registry of the tokens this process
/// already knows, keyed by the endpoint's socket address:
///
/// * [`protect`]: a server of this process whose listener answers through
///   [`super::accept`] (the daemon's control endpoint): `accept` then
///   *requires* the token ([`enforced`]);
/// * [`remember`]: a token this process was given for an endpoint (a worker
///   server in this process, a worker endpoint the orchestrator learned from
///   a Ready line or `worker.json`);
/// * [`note`]: a token read next to a worker's published discovery;
/// * [`note_source`]: where to read one, when the address is looked up (a
///   worker's discovery parsed by this process: nothing is read while it is
///   parsed).
///
/// A client that is not handed a token uses [`lookup`], so in-process
/// clients and callers that only know an address keep working with
/// mandatory tokens. Entries are only ever made for loopback addresses, a
/// registration replaces an older one of the same kind for the same
/// address, and an entry dies with its owner ([`Hold`]) or its liveness
/// token. Nothing here is logged or serialized, and the registry's lock is
/// never held while a source reads its token.
pub mod tokens {
    use crate::util::cancel::Cancel;
    use rand::RngCore;
    use std::{
        net::SocketAddr,
        sync::{Arc, Mutex, PoisonError, Weak},
    };

    /// The upgrade header that carries a local token.
    pub use crate::model::protocol::TOKEN_HEADER as HEADER;

    /// Ownerless entries ([`note`], [`note_source`]) kept at most; the
    /// oldest go first.
    const MAX_NOTED: usize = 256;

    /// Reads a token when its address is looked up ([`note_source`]);
    /// `None` when there is none (any more).
    pub type Source = Arc<dyn Fn() -> Option<String> + Send + Sync>;

    /// Keeps a [`protect`] or [`remember`] registration alive: the entry is
    /// retired once the last clone is dropped.
    #[derive(Debug)]
    pub struct Hold {
        _private: (),
    }

    #[derive(Clone)]
    enum Token {
        Known(String),
        Read(Source),
    }

    struct Entry {
        address: SocketAddr,
        token: Token,
        /// `accept` requires the token (a [`protect`] entry).
        enforce: bool,
        /// `None` for a [`note`] or [`note_source`] entry (no owner).
        hold: Option<Weak<Hold>>,
        /// The entry dies when this is cancelled.
        alive: Option<Cancel>,
    }

    impl Entry {
        fn live(&self) -> bool {
            self.hold
                .as_ref()
                .is_none_or(|hold| hold.strong_count() > 0)
                && self
                    .alive
                    .as_ref()
                    .is_none_or(|alive| !alive.is_cancelled())
        }
    }

    fn registry() -> std::sync::MutexGuard<'static, Vec<Entry>> {
        static REGISTRY: Mutex<Vec<Entry>> = Mutex::new(Vec::new());
        REGISTRY.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A fresh token: 32 random bytes, hex-encoded (64 characters).
    pub fn generate() -> String {
        let mut bytes = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        hex::encode(bytes)
    }

    /// Whether a presented header value is `expected` (constant-time).
    pub fn matches(presented: Option<&tungstenite::http::HeaderValue>, expected: &str) -> bool {
        let presented = presented.map_or(&b""[..], |value| value.as_bytes());
        // Compared even when absent, so timing does not tell the two apart.
        crate::util::secret::ct_eq(presented, expected.as_bytes()) && !expected.is_empty()
    }

    fn register(
        address: SocketAddr,
        token: Token,
        enforce: bool,
        alive: Option<Cancel>,
        owned: bool,
    ) -> Option<Arc<Hold>> {
        if !address.ip().is_loopback() || matches!(&token, Token::Known(known) if known.is_empty())
        {
            return None;
        }
        let hold = owned.then(|| Arc::new(Hold { _private: () }));
        let read = matches!(token, Token::Read(_));
        let mut entries = registry();
        entries.retain(|entry| {
            entry.live()
                && !(entry.address == address
                    && entry.enforce == enforce
                    && matches!(entry.token, Token::Read(_)) == read)
        });
        if !owned {
            let noted = entries.iter().filter(|entry| entry.hold.is_none()).count();
            if noted >= MAX_NOTED
                && let Some(oldest) = entries.iter().position(|entry| entry.hold.is_none())
            {
                entries.remove(oldest);
            }
        }
        entries.push(Entry {
            address,
            token,
            enforce,
            hold: hold.as_ref().map(Arc::downgrade),
            alive,
        });
        hold
    }

    /// Requires `token` on every [`super::accept`] at `address` (and lets
    /// clients of this process find it) while the returned [`Hold`] is held
    /// and `alive` is not cancelled. `None` (nothing registered) for a
    /// non-loopback address or an empty token.
    pub fn protect(address: SocketAddr, token: &str, alive: Option<Cancel>) -> Option<Arc<Hold>> {
        register(address, Token::Known(token.to_owned()), true, alive, true)
    }

    /// Lets clients of this process present `token` to `address` while the
    /// returned [`Hold`] is held.
    pub fn remember(address: SocketAddr, token: &str) -> Option<Arc<Hold>> {
        register(address, Token::Known(token.to_owned()), false, None, true)
    }

    /// [`remember`] without an owner: kept until a newer registration for
    /// the address replaces it (at most [`MAX_NOTED`] such entries).
    pub fn note(address: SocketAddr, token: &str) {
        register(address, Token::Known(token.to_owned()), false, None, false);
    }

    /// [`note`] of a token that `source` reads when the address is looked
    /// up (never while this is called, nor under the registry's lock). It
    /// replaces an older source for the address, not a known token; a
    /// source that finds nothing defers to the older entries.
    pub fn note_source(address: SocketAddr, source: Source) {
        register(address, Token::Read(source), false, None, false);
    }

    /// The token this process knows for `address`: the protecting server's,
    /// else the newest one remembered or noted (a source is read now, after
    /// the registry's lock is released; one that finds nothing is skipped).
    pub fn lookup(address: SocketAddr) -> Option<String> {
        let candidates: Vec<Token> = {
            let entries = registry();
            let live = |entry: &&Entry| entry.address == address && entry.live();
            entries
                .iter()
                .filter(live)
                .filter(|entry| entry.enforce)
                .chain(entries.iter().rev().filter(live))
                .map(|entry| entry.token.clone())
                .collect()
        };
        candidates.into_iter().find_map(|token| match token {
            Token::Known(token) => Some(token),
            Token::Read(source) => source().filter(|token| !token.is_empty()),
        })
    }

    /// The token [`super::accept`] requires at `address`, if it is protected.
    pub fn enforced(address: SocketAddr) -> Option<String> {
        registry()
            .iter()
            .find(|entry| entry.address == address && entry.enforce && entry.live())
            .and_then(|entry| match &entry.token {
                Token::Known(token) => Some(token.clone()),
                Token::Read(_) => None,
            })
    }

    /// The loopback socket addresses of a `ws://` endpoint whose host is an
    /// IP literal: no name is resolved (a worker publishes its listener's
    /// socket address). Empty for anything else.
    pub fn literal_addresses(endpoint: &str) -> Vec<SocketAddr> {
        let Ok(uri) = super::endpoint_uri(endpoint) else {
            return Vec::new();
        };
        let (Some(host), Some(port)) = (uri.host(), uri.port_u16()) else {
            return Vec::new();
        };
        match host.trim_matches(['[', ']']).parse::<std::net::IpAddr>() {
            Ok(ip) if ip.is_loopback() => vec![SocketAddr::new(ip, port)],
            _ => Vec::new(),
        }
    }

    /// The loopback socket addresses of a `ws://` endpoint (no DNS lookup
    /// beyond `localhost`): the keys [`lookup`] is asked with.
    pub fn addresses(endpoint: &str) -> Vec<SocketAddr> {
        use std::net::ToSocketAddrs;
        let Ok(uri) = super::endpoint_uri(endpoint) else {
            return Vec::new();
        };
        let Some(host) = uri
            .host()
            .map(|host| host.trim_matches(['[', ']']).to_owned())
        else {
            return Vec::new();
        };
        let port = uri
            .port_u16()
            .unwrap_or(if uri.scheme_str() == Some("wss") {
                443
            } else {
                80
            });
        let local = host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback());
        if !local {
            return Vec::new();
        }
        (host.as_str(), port)
            .to_socket_addrs()
            .map(|addresses| addresses.filter(|a| a.ip().is_loopback()).collect())
            .unwrap_or_default()
    }

    /// [`lookup`] for a `ws://` endpoint.
    pub fn lookup_endpoint(endpoint: &str) -> Option<String> {
        addresses(endpoint).into_iter().find_map(lookup)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{net::TcpListener, thread};

    #[test]
    fn remote_error_preserves_connection_for_the_next_request() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let mut socket = accept(listener.accept().unwrap().0).unwrap();
            let _: Value = receive(&mut socket).unwrap();
            reply(&mut socket, Err(anyhow::anyhow!("FOLDER_UNAVAILABLE"))).unwrap();
            let request: Value = receive(&mut socket).unwrap();
            reply(&mut socket, Ok(request)).unwrap();
        });
        let mut socket = connect(&endpoint).unwrap();
        let error = call(&mut socket, &json!({"folder":"broken"})).unwrap_err();
        assert!(error.downcast_ref::<RemoteError>().is_some());
        assert_eq!(error.to_string(), "FOLDER_UNAVAILABLE");
        assert_eq!(
            call(&mut socket, &json!({"folder":"healthy"})).unwrap(),
            json!({"folder":"healthy"})
        );
        server.join().unwrap();
    }

    #[test]
    fn malformed_replies_are_not_remote_application_errors() {
        let replies = [
            json!({}),
            json!({"ok":"true","result":1}),
            json!({"ok":true}),
            json!({"ok":false}),
            json!({"ok":false,"error":42}),
        ];
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let count = replies.len();
        let server = thread::spawn(move || {
            for response in replies {
                let mut socket = accept(listener.accept().unwrap().0).unwrap();
                let _: Value = receive(&mut socket).unwrap();
                send(&mut socket, &response).unwrap();
            }
        });
        for _ in 0..count {
            let mut socket = connect(&endpoint).unwrap();
            let error = call(&mut socket, &json!({})).unwrap_err();
            assert!(error.downcast_ref::<RemoteError>().is_none());
            assert!(error.to_string().contains("INVALID_WS_REPLY"));
        }
        server.join().unwrap();
    }

    #[test]
    fn endpoint_validation_accepts_ws_and_wss() {
        for endpoint in [
            "ws://host:43180",
            "wss://host",
            "wss://host:8443/sync?lane=1",
            "ws://[::1]:43180",
        ] {
            assert!(endpoint_uri(endpoint).is_ok(), "{endpoint}");
        }
        for endpoint in [
            "",
            "host:43180",
            "https://host",
            "tcp://host:80",
            "wss://",
            "wss://user:pass@host",
            "wss://host:99999",
        ] {
            assert!(endpoint_uri(endpoint).is_err(), "{endpoint}");
        }
    }

    fn trusted_connector() -> Connector {
        let ca = native_tls::Certificate::from_pem(include_bytes!("../tests/fixtures/tls/ca.pem"))
            .unwrap();
        Connector::NativeTls(
            native_tls::TlsConnector::builder()
                .add_root_certificate(ca)
                .build()
                .unwrap(),
        )
    }

    // Tungstenite requires the header callback to return its unboxed HTTP error response.
    #[allow(clippy::result_large_err)]
    fn tls_server(binary: bool) -> (u16, thread::JoinHandle<()>) {
        let identity = native_tls::Identity::from_pkcs12(
            include_bytes!("../tests/fixtures/tls/localhost.p12"),
            "telekinesis-test",
        )
        .unwrap();
        let acceptor = native_tls::TlsAcceptor::new(identity).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (tcp, _) = listener.accept().unwrap();
            timeouts(&tcp).unwrap();
            // Rejection tests intentionally abort during certificate verification.
            let Ok(tls) = acceptor.accept(tcp) else {
                return;
            };
            let handshake = tungstenite::accept_hdr_with_config(
                MaybeTlsStream::NativeTls(tls),
                move |request: &tungstenite::handshake::server::Request,
                      mut response: tungstenite::handshake::server::Response| {
                    assert_eq!(request.uri(), "/sync?lane=1");
                    if binary {
                        assert_eq!(request.headers()["X-Tkfs-Wire"], crate::wire::PROTOCOL);
                        response
                            .headers_mut()
                            .insert("X-Tkfs-Wire", crate::wire::PROTOCOL.parse().unwrap());
                    }
                    Ok(response)
                },
                Some(config()),
            );
            let Ok(socket) = handshake else {
                return;
            };
            let mut socket = Socket::new(socket, binary);
            for _ in 0..2 {
                let request: Value = receive(&mut socket).unwrap();
                reply(&mut socket, Ok(request)).unwrap();
            }
        });
        (port, server)
    }

    #[test]
    fn wss_exchanges_persistent_messages_and_preserves_route() {
        let (port, server) = tls_server(false);
        let mut socket = connect_with_connector(
            &format!("wss://localhost:{port}/sync?lane=1"),
            Some(trusted_connector()),
            None,
        )
        .unwrap();
        for sequence in 0..2 {
            let request = json!({"sequence":sequence});
            assert_eq!(call(&mut socket, &request).unwrap(), request);
        }
        server.join().unwrap();
    }

    #[test]
    fn wss_rejects_untrusted_certificates() {
        let (port, server) = tls_server(false);
        assert!(connect(&format!("wss://localhost:{port}/sync?lane=1")).is_err());
        server.join().unwrap();
    }

    #[test]
    fn wss_rejects_hostname_mismatch_even_with_trusted_ca() {
        let (port, server) = tls_server(false);
        assert!(
            connect_with_connector(
                &format!("wss://127.0.0.1:{port}/sync?lane=1"),
                Some(trusted_connector()),
                None,
            )
            .is_err()
        );
        server.join().unwrap();
    }

    #[test]
    fn wss_negotiates_binary_gzip_and_reports_compressed_payload_bytes() {
        let (port, server) = tls_server(true);
        let mut socket = connect_with_connector(
            &format!("wss://localhost:{port}/sync?lane=1"),
            Some(trusted_connector()),
            None,
        )
        .unwrap();
        assert_eq!(socket.codec(), crate::wire::PROTOCOL);
        for _ in 0..2 {
            let value = json!({"hex":"0a".repeat(128*1024)});
            assert_eq!(call(&mut socket, &value).unwrap(), value);
        }
        assert!(socket.sent_bytes < 16 * 1024 && socket.received_bytes < 16 * 1024);
        server.join().unwrap();
    }

    #[test]
    fn connect_with_tries_every_address_in_turn() {
        // Hosts-independent fallback: a refused address, then a silent one
        // (it accepts TCP, never answers the upgrade), then a listening one.
        let refused = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let silent_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let silent = silent_listener.local_addr().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let open = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            accept_with(stream, 1024, |_, response| {
                response.headers_mut().insert(
                    "X-Fallback",
                    tungstenite::http::HeaderValue::from_static("reached"),
                );
            })
            .map(|_| ())
        });
        let request = format!("ws://{open}").into_client_request().unwrap();
        let budget = Duration::from_millis(800);
        let started = Instant::now();
        let (_socket, response) = connect_any(
            "fallback",
            &[refused, silent, open],
            &request,
            None,
            budget,
            None,
        )
        .unwrap();
        assert_eq!(response.headers()["X-Fallback"], "reached");
        // The silent address was abandoned once its budget ran out (the
        // refused one costs at most its own budget).
        let elapsed = started.elapsed();
        assert!(
            elapsed >= budget - Duration::from_millis(100) && elapsed < Duration::from_secs(10),
            "connected after {elapsed:?}"
        );
        server.join().unwrap().unwrap();
        drop(silent_listener);

        // Every address refused: WS_CONNECT_FAILED naming the last one tried.
        let error = connect_any("fallback", &[silent, refused], &request, None, budget, None)
            .expect_err("no address accepts");
        let text = format!("{error:#}");
        assert!(
            text.contains(&format!("WS_CONNECT_FAILED: {refused}")),
            "{text}"
        );
        // No address at all.
        let error = connect_any("fallback", &[], &request, None, budget, None)
            .expect_err("nothing to dial");
        assert!(format!("{error:#}").contains("WS_CONNECT_FAILED"));
    }

    /// WP4.1: `accept_authorized` refuses an upgrade without the token (HTTP
    /// 403, `ACCESS_DENIED` on both sides) and serves one that carries it.
    #[test]
    fn an_authorized_endpoint_refuses_upgrades_without_its_token() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let mut outcomes = Vec::new();
            for _ in 0..3 {
                let stream = listener.accept().unwrap().0;
                match accept_authorized(stream, Some("s3cr3t")) {
                    Ok(mut socket) => {
                        let request: Value = receive(&mut socket).unwrap();
                        reply(&mut socket, Ok(request)).unwrap();
                        outcomes.push("served".to_owned());
                    }
                    Err(error) => outcomes.push(crate::model::errors::code_of(&error)),
                }
            }
            outcomes
        });
        let refused = |token: Option<&str>| {
            let error = connect_authorized(&endpoint, token).err().expect("refused");
            crate::model::errors::code_of(&error)
        };
        assert_eq!(refused(None), "ACCESS_DENIED");
        assert_eq!(refused(Some("wrong")), "ACCESS_DENIED");
        let mut socket = connect_authorized(&endpoint, Some("s3cr3t")).unwrap();
        assert_eq!(
            call(&mut socket, &json!({"n": 1})).unwrap(),
            json!({"n": 1})
        );
        assert_eq!(
            server.join().unwrap(),
            ["ACCESS_DENIED", "ACCESS_DENIED", "served"]
        );
    }

    /// WP4.1: a noted source is read when its address is looked up, never
    /// when it is noted; one that finds nothing defers to the older entries;
    /// only IP-literal loopback endpoints give addresses to note.
    #[test]
    fn a_noted_source_is_read_when_its_address_is_looked_up() {
        use std::sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        };
        let address: std::net::SocketAddr = "127.0.0.1:7".parse().unwrap();
        let reads = Arc::new(AtomicUsize::new(0));
        let answer = Arc::new(Mutex::new(Some("from-source".to_owned())));
        let source: tokens::Source = {
            let (reads, answer) = (reads.clone(), answer.clone());
            Arc::new(move || {
                reads.fetch_add(1, Ordering::SeqCst);
                answer.lock().unwrap().clone()
            })
        };
        tokens::note(address, "known");
        tokens::note_source(address, source);
        assert_eq!(
            reads.load(Ordering::SeqCst),
            0,
            "nothing is read when noted"
        );
        assert_eq!(tokens::lookup(address).as_deref(), Some("from-source"));
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        *answer.lock().unwrap() = None;
        assert_eq!(
            tokens::lookup(address).as_deref(),
            Some("known"),
            "a source that finds nothing defers to the older note"
        );
        assert_eq!(tokens::enforced(address), None);
        assert_eq!(tokens::literal_addresses("ws://127.0.0.1:7"), vec![address]);
        assert_eq!(
            tokens::literal_addresses("ws://[::1]:7"),
            vec!["[::1]:7".parse::<std::net::SocketAddr>().unwrap()]
        );
        assert!(tokens::literal_addresses("ws://localhost:7").is_empty());
        assert!(tokens::literal_addresses("ws://10.0.0.1:7").is_empty());
        assert!(tokens::literal_addresses("not an endpoint").is_empty());
    }

    /// WP4.1: a protected address makes `accept` require the token and lets
    /// clients of this process find it; the protection ends with its hold or
    /// its liveness token; other addresses are untouched.
    #[test]
    fn protected_addresses_are_enforced_while_their_hold_lives() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let endpoint = format!("ws://{address}");
        let alive = crate::util::cancel::Cancel::new();
        let hold = tokens::protect(address, "control-token", Some(alive.clone())).unwrap();
        assert_eq!(tokens::enforced(address).as_deref(), Some("control-token"));
        assert_eq!(
            tokens::lookup_endpoint(&endpoint).as_deref(),
            Some("control-token")
        );
        // A hint for the same address never weakens the enforced token.
        let hint = tokens::remember(address, "stale").unwrap();
        assert_eq!(tokens::lookup(address).as_deref(), Some("control-token"));
        drop(hint);
        let server = thread::spawn(move || {
            let mut outcomes = Vec::new();
            for _ in 0..3 {
                let stream = listener.accept().unwrap().0;
                outcomes.push(match accept(stream) {
                    Ok(_) => "accepted".to_owned(),
                    Err(error) => crate::model::errors::code_of(&error),
                });
            }
            outcomes
        });
        // A raw client without the token is refused; `connect` finds it.
        assert!(matches!(
            tungstenite::connect(endpoint.as_str()),
            Err(tungstenite::Error::Http(response)) if response.status() == 403
        ));
        connect(&endpoint).unwrap();
        // Retired with its liveness token: `accept` no longer requires it.
        alive.cancel();
        assert_eq!(tokens::enforced(address), None);
        assert_eq!(tokens::lookup(address), None);
        tungstenite::connect(endpoint.as_str()).unwrap();
        assert_eq!(
            server.join().unwrap(),
            ["ACCESS_DENIED", "accepted", "accepted"]
        );
        drop(hold);

        // A dropped hold retires its entry; non-loopback addresses and empty
        // tokens are never registered; a note is a hint only.
        let other: SocketAddr = "127.0.0.1:9".parse().unwrap();
        let held = tokens::remember(other, "worker-token").unwrap();
        assert_eq!(tokens::lookup(other).as_deref(), Some("worker-token"));
        assert_eq!(tokens::enforced(other), None);
        drop(held);
        assert_eq!(tokens::lookup(other), None);
        assert!(tokens::remember("10.0.0.1:9".parse().unwrap(), "t").is_none());
        assert!(tokens::remember(other, "").is_none());
        tokens::note(other, "noted");
        assert_eq!(tokens::lookup(other).as_deref(), Some("noted"));
        assert_eq!(tokens::enforced(other), None);
        assert!(tokens::addresses("ws://example.com:80").is_empty());
        assert_eq!(tokens::generate().len(), 64);
        assert_ne!(tokens::generate(), tokens::generate());
    }

    /// Unix only: Windows still answers `getpeername` on a socket whose
    /// connection is closed, so this state cannot be built there.
    #[cfg(unix)]
    #[test]
    fn a_watchdog_never_runs_on_an_unverified_handle() {
        use std::{io::Read, net::Shutdown};
        // A socket whose connection is fully closed: no duplicate of it
        // verifies, so no watchdog starts (fail closed) rather than one
        // whose `shutdown` would fail.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        drop(listener.accept().unwrap().0);
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        assert_eq!((&client).read(&mut [0u8; 4]).unwrap(), 0);
        client.shutdown(Shutdown::Both).unwrap();
        let started = Instant::now();
        while client.peer_addr().is_ok() {
            assert!(started.elapsed() < Duration::from_secs(5), "never closed");
            thread::sleep(Duration::from_millis(10));
        }
        let error = watchdog::Watchdog::start(&client)
            .err()
            .expect("no watchdog on a closed socket");
        assert_eq!(error.kind(), std::io::ErrorKind::NotConnected, "{error}");
    }
}
