//! Session establishment (§6.2 Upgrade and Handshake). Implemented by WP1.10.
//!
//! **Upgrade.** A v3 dialer sends `X-Tkfs-Sync: 3` and a v3 server echoes it.
//! A dialer that sees no echo is facing a legacy (6298d0b, v2) server and
//! fails with [`DialError::NeedsUpgrade`] *before sending anything*: the
//! pairing code is never offered to a server that would receive it in
//! plaintext. A server that sees no header is facing a legacy dialer: it reads
//! one legacy JSON message (≤ 16 KiB, 5 s), reports it through
//! [`AcceptPolicy::legacy_attempt`] (with the code redacted), replies the
//! legacy-format `PEER_NEEDS_UPGRADE` as a text JSON message and closes.
//!
//! **Handshake** (blocking, pre-auth bounds: every message ≤ 16 KiB of header
//! and every step ≤ 5 s, enforced by socket timeouts plus a watchdog that
//! shuts the socket down when a step overruns, so a trickling peer cannot
//! hold a slot either; a connection whose watchdog cannot start is refused):
//! 1. → `HELLO{ClientHello}` with a fresh client nonce;
//! 2. ← `HELLO_ACK{ServerHello}` with the verifier salt, a fresh server nonce
//!    and the verifier generation, or `GOAWAY{CANNOT_PAIR_WITH_SELF |
//!    PAIRING_CODE_NOT_SET | TOO_MANY_SESSIONS | PEER_REVOKED |
//!    PEER_PROTOCOL_UNSUPPORTED}`;
//! 3. → `AUTH{proof = hex(HMAC(K, "client" ‖ T))}` (see [`super::auth`]);
//! 4. ← `AUTH_OK{server_proof = hex(HMAC(K, "server" ‖ T))}`, which the client
//!    verifies (mutual authentication), or `GOAWAY{INVALID_PAIRING_CODE}`
//!    (`GOAWAY{PAIRING_CODE_CHANGED}` when the verifier rotated between steps
//!    2 and 4);
//! 5. the client optionally checks the server's installation against the
//!    catalog (`PEER_INSTALLATION_CHANGED`, [`dial_expecting`]);
//! 6. both sides clear the socket timeouts, raise the WebSocket bounds to the
//!    session limits, switch to non-blocking and hand the socket to
//!    [`Conn::from_established`].
//!
//! **Who is blamed.** A peer's close frame during the handshake is a closed
//! connection (`WS_CLOSED`), whatever reason it carries: only the client reads
//! a server's protocol-error close (code 1002, what this server sends on a
//! violation) as [`DialError::Protocol`]. A server whose own installation id
//! is invalid refuses every handshake as its own failure
//! (`INSTALLATION_REQUIRED`, close code 1011), never as the dialer's
//! `PROTOCOL_VIOLATION`.
//!
//! The step watchdog is the transport's (`ws::watchdog`), shared with the
//! dialer's upgrade in [`ws::connect_with`].
//!
//! The pre-auth frames use the §6.2 frame layout (8-byte prefix, JSON header,
//! no body). They are encoded here rather than by `session::frame` (WP1.9) so
//! this package has no intra-wave dependency; the layout is the same.
//!
//! **Per-peer credentials (WP4.1).** A dialer whose [`ClientAuth`] holds a
//! per-peer key offers [`PEER_HMAC_V1`] in `ClientHello.features`. A server
//! that holds an active credential for the dialer's installation
//! ([`AcceptPolicy::peer_key`]) echoes it in `ServerHello.features`, and both
//! proofs are then keyed by `K_peer` over the per-peer transcript
//! ([`Transcript::per_peer`]); a server with no credential, or a revoked
//! one, answers `GOAWAY{PEER_REVOKED}` instead of `HELLO_ACK` (the dialer may
//! retry with the code). A server that does not know the scheme (an older
//! acceptor) ignores it and the dialer falls back to the code in the same
//! handshake. A wrong per-peer proof is `PEER_REVOKED` on either side, and a
//! credential revoked between `HELLO_ACK` and `AUTH` refuses the proof.
//! Rotation applies to both schemes: the session records the verifier
//! generation, which a `ResetPairingCode` bumps.
//!
//! **Loopback tokens (WP4.1).** [`dial_loopback`] with no token presents the
//! one this process knows for the address (`ws::tokens::lookup`), if any.
use crate::{
    model::{
        errors::{ErrorClass, ErrorWire},
        protocol::{
            LaneRole, LaneSpec, PROTOCOL_NAME, PROTOCOL_VERSION, SUPPORTED_VERSIONS, TOKEN_HEADER,
            UPGRADE_HEADER, UPGRADE_LOOPBACK, UPGRADE_V3,
            frame::{
                Auth, AuthOk, ClientHello, DIAL_ATTEMPT, FrameKind, GOAWAY_HANDSHAKE_CODES, Goaway,
                HANDSHAKE_TIMEOUT, KNOWN_FLAGS, Limits, MAX_CHUNK, MAX_HEADER, MAX_HEADER_PRE_AUTH,
                PREFIX_LEN, ServerHello,
            },
        },
    },
    pairing_code::Verifier,
    session::{
        auth::{PEER_HMAC_V1, Transcript, decode_nonce, new_nonce, valid_id, verify_proof},
        conn::{AcceptedPeer, Conn, ConnOptions, PeerInfo, Side},
    },
    util::{cancel::Cancel, hmac::ct_eq},
    ws::{self, RawSocket, watchdog::Watchdog},
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{
    fmt, io,
    net::{SocketAddr, TcpStream},
    sync::Arc,
    time::{Duration, Instant},
};
use tungstenite::{
    Message,
    protocol::{CloseFrame, frame::coding::CloseCode},
    stream::MaybeTlsStream,
};

/// What kind of client sent a WebSocket upgrade.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UpgradeKind {
    /// `X-Tkfs-Sync: 3`: a v3 peer session.
    V3,
    /// `X-Tkfs-Sync: 3-loopback`: an orchestrator ↔ worker session.
    Loopback,
    /// No (or an unknown) `X-Tkfs-Sync` header: a legacy v2 client.
    Legacy,
}

/// Why dialing failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DialError {
    /// TCP/TLS/WebSocket connect failed on every resolved address.
    Connect(String),
    /// The server did not echo `X-Tkfs-Sync`: it runs the legacy protocol. The
    /// code was never sent.
    NeedsUpgrade { detail: String },
    /// No common protocol version.
    Unsupported {
        offered: Vec<u32>,
        supported: Vec<u32>,
    },
    /// `GOAWAY{INVALID_PAIRING_CODE}` or a failed server proof.
    Unauthorized(ErrorWire),
    /// Any other `GOAWAY` (self-pair, code not set, too many sessions, …).
    Rejected(ErrorWire),
    /// A handshake step exceeded its 5 s budget.
    Timeout,
    /// The peer violated the handshake protocol.
    Protocol(String),
}

impl DialError {
    pub fn to_wire(&self) -> ErrorWire {
        match self {
            DialError::Connect(detail) => ErrorWire::coded("WS_CONNECT_FAILED", detail),
            DialError::NeedsUpgrade { detail } => ErrorWire::coded("PEER_NEEDS_UPGRADE", detail),
            DialError::Unsupported { offered, supported } => ErrorWire::coded(
                "PEER_PROTOCOL_UNSUPPORTED",
                format!("offered {offered:?}, supported {supported:?}"),
            ),
            DialError::Unauthorized(error) | DialError::Rejected(error) => error.clone(),
            DialError::Timeout => ErrorWire::coded("REQUEST_TIMEOUT", "handshake timed out"),
            DialError::Protocol(detail) => ErrorWire::coded("PROTOCOL_VIOLATION", detail),
        }
    }

    pub fn class(&self) -> ErrorClass {
        self.to_wire().class
    }
}

impl fmt::Display for DialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_wire())
    }
}

impl std::error::Error for DialError {}

/// Supplies the 32-byte HMAC key for a server's verifier salt (`CodeAuth`
/// derives it from the pairing code with `Verifier::derive_key`).
pub trait ClientAuth: Send + Sync {
    fn key(&self, salt: &str) -> Result<[u8; 32], ErrorWire>;

    /// A per-peer credential `K_peer` for this server (WP4.1, `PeerAuth`):
    /// with one, the hello offers [`PEER_HMAC_V1`] and a server that echoes
    /// it is authenticated with this key instead of [`ClientAuth::key`].
    fn peer_key(&self) -> Option<[u8; 32]> {
        None
    }

    /// Called once per handshake when the server's `HELLO_ACK` arrived and
    /// the key is about to be used (either scheme).
    fn begin_auth(&self) {}
}

/// The accepting side's view of the catalog (WP2.8 implements it over
/// `CatalogSnapshot`).
pub trait AcceptPolicy: Send + Sync {
    /// This daemon's installation id (self-pair refusal). It must be a valid
    /// id (1..=128 bytes, no control characters); otherwise every handshake is
    /// refused as this server's failure (`INSTALLATION_REQUIRED`).
    fn installation(&self) -> String;
    /// The current verifier and its generation; `None` → `PAIRING_CODE_NOT_SET`.
    fn verifier(&self) -> Option<(Verifier, u64)>;
    /// Admission beyond authentication (revocation, session caps).
    fn admit(&self, hello: &ClientHello) -> Result<(), ErrorWire>;
    /// A legacy (v2) client tried to connect; `hello` is its first message.
    fn legacy_attempt(&self, addr: SocketAddr, hello: &Value);

    /// The active per-peer credential `K_peer` of the dialer `installation`
    /// (WP4.1), asked when its hello offers [`PEER_HMAC_V1`]. A missing or
    /// revoked credential is `PEER_REVOKED`, sent as the `GOAWAY`; so is any
    /// policy without credentials (the default).
    fn peer_key(&self, installation: &str) -> Result<[u8; 32], ErrorWire> {
        let _ = installation;
        Err(ErrorWire::coded(
            "PEER_REVOKED",
            "this server keeps no per-peer credentials",
        ))
    }
}

// ---------------------------------------------------------------------------
// Public entry points

/// A `ClientHello` for this build: protocol `tkfs-peer`, versions `[3]`, a
/// fresh session uuid and client nonce, no features.
pub fn client_hello(
    installation: &str,
    role: LaneRole,
    lane: Option<LaneSpec>,
    pool: u32,
) -> ClientHello {
    ClientHello {
        protocol: PROTOCOL_NAME.to_owned(),
        versions: SUPPORTED_VERSIONS.to_vec(),
        installation: installation.to_owned(),
        role,
        lane,
        pool,
        session: uuid::Uuid::new_v4().to_string(),
        client_nonce: new_nonce(),
        features: Vec::new(),
    }
}

/// Dials `endpoint`, runs the v3 handshake and returns the authenticated
/// connection and the server's hello. The caller checks
/// `ServerHello.installation` against its catalog, or uses
/// [`dial_expecting`], which does that before the connection starts.
pub fn dial(
    endpoint: &str,
    hello: ClientHello,
    auth: &dyn ClientAuth,
    connector: Option<tungstenite::Connector>,
    o: ConnOptions,
) -> Result<(Conn, ServerHello), DialError> {
    dial_expecting(endpoint, hello, auth, None, connector, o)
}

/// [`dial`], refusing (`PEER_INSTALLATION_CHANGED`) an authenticated server
/// whose installation is not `expected_installation` (§6.2 step 5).
pub fn dial_expecting(
    endpoint: &str,
    hello: ClientHello,
    auth: &dyn ClientAuth,
    expected_installation: Option<&str>,
    connector: Option<tungstenite::Connector>,
    o: ConnOptions,
) -> Result<(Conn, ServerHello), DialError> {
    dial_with(
        endpoint,
        hello,
        auth,
        expected_installation,
        connector,
        o,
        None,
    )
}

/// [`dial_expecting`], interruptible by `cancel` (CCR-WP2.6-2, applied by
/// WP3.1): an unpair, a respawn or a shutdown no longer waits for a dial to
/// finish. `cancel` is checked before each step; while a socket exists, its
/// cancellation shuts the socket down (a hook on a child token that lives
/// only as long as the dial, see `ws::connect_with_cancel`), so an upgrade
/// or a handshake step in progress fails at once instead of running out its
/// budget (on Windows: as soon as the peer answers our FIN by closing, as
/// every tkfs server and relay does; a peer that ignores it is bounded by
/// the step's timeout). A TCP connect in progress (at most `DIAL_ATTEMPT`,
/// 3 s, per address) runs to its end. A cancelled dial fails with
/// `DialError::Connect("dial cancelled")`, and a session it may have
/// completed meanwhile is closed, never returned.
pub fn dial_cancellable(
    endpoint: &str,
    hello: ClientHello,
    auth: &dyn ClientAuth,
    expected_installation: Option<&str>,
    connector: Option<tungstenite::Connector>,
    o: ConnOptions,
    cancel: &Cancel,
) -> Result<(Conn, ServerHello), DialError> {
    // The hooks the dial registers (socket shutdowns) live on this child
    // and go with it when the dial returns.
    let guard = cancel.child();
    let dialed = dial_with(
        endpoint,
        hello,
        auth,
        expected_installation,
        connector,
        o,
        Some(&guard),
    );
    if guard.is_cancelled() {
        if let Ok((conn, _)) = &dialed {
            conn.close();
        }
        return Err(cancelled());
    }
    dialed
}

fn cancelled() -> DialError {
    DialError::Connect("dial cancelled".into())
}

fn dial_with(
    endpoint: &str,
    hello: ClientHello,
    auth: &dyn ClientAuth,
    expected_installation: Option<&str>,
    connector: Option<tungstenite::Connector>,
    o: ConnOptions,
    cancel: Option<&Cancel>,
) -> Result<(Conn, ServerHello), DialError> {
    let role = hello.role;
    let lane = hello.lane;
    let session = hello.session.clone();
    let (ws, server) = connect_v3_with(
        endpoint,
        hello,
        auth,
        expected_installation,
        connector,
        cancel,
    )?;
    let peer = PeerInfo {
        installation: server.installation.clone(),
        role,
        lane,
        session,
        verifier_generation: server.verifier_generation,
    };
    let conn = into_conn(ws, Side::Client, peer, o)
        .map_err(|error| DialError::Connect(format!("session I/O did not start: {error}")))?;
    Ok((conn, server))
}

/// Everything [`dial_expecting`] does except starting the session: connect
/// (every resolved address, 3 s each), check the `X-Tkfs-Sync` echo, run
/// [`client_handshake`] and the installation check. Returns the authenticated
/// socket in blocking mode with no socket timeouts and session-sized bounds.
pub fn connect_v3(
    endpoint: &str,
    hello: ClientHello,
    auth: &dyn ClientAuth,
    expected_installation: Option<&str>,
    connector: Option<tungstenite::Connector>,
) -> Result<(RawSocket, ServerHello), DialError> {
    connect_v3_with(
        endpoint,
        hello,
        auth,
        expected_installation,
        connector,
        None,
    )
}

/// [`connect_v3`]; with `cancel`, interruptible as [`dial_cancellable`]
/// describes (the caller passes a short-lived token).
fn connect_v3_with(
    endpoint: &str,
    hello: ClientHello,
    auth: &dyn ClientAuth,
    expected_installation: Option<&str>,
    connector: Option<tungstenite::Connector>,
    cancel: Option<&Cancel>,
) -> Result<(RawSocket, ServerHello), DialError> {
    if cancel.is_some_and(Cancel::is_cancelled) {
        return Err(cancelled());
    }
    let (mut ws, response) = ws::connect_with_cancel(
        endpoint,
        &[(UPGRADE_HEADER, UPGRADE_V3)],
        connector,
        DIAL_ATTEMPT,
        cancel,
    )
    .map_err(connect_error)?;
    if cancel.is_some_and(Cancel::is_cancelled) {
        abandon_ws(&mut ws);
        return Err(cancelled());
    }
    match echoed(&response) {
        Some(UPGRADE_V3) => {}
        None => {
            abandon_ws(&mut ws);
            return Err(DialError::NeedsUpgrade {
                detail: format!(
                    "{endpoint} runs the legacy peer protocol (no {UPGRADE_HEADER} echo); \
                     upgrade the remote orchestrator. The pairing code was not sent"
                ),
            });
        }
        Some(other) => {
            let supported = other
                .trim()
                .parse::<u32>()
                .map(|v| vec![v])
                .unwrap_or_default();
            abandon_ws(&mut ws);
            return Err(DialError::Unsupported {
                offered: hello.versions,
                supported,
            });
        }
    }
    let handshake = client_handshake(ws, hello, auth);
    if cancel.is_some_and(Cancel::is_cancelled) {
        if let Ok((mut ws, _)) = handshake {
            abandon_ws(&mut ws);
        }
        return Err(cancelled());
    }
    let (mut ws, server) = handshake?;
    if let Some(expected) = expected_installation
        && server.installation != expected
    {
        abandon_ws(&mut ws);
        return Err(DialError::Rejected(ErrorWire::coded(
            "PEER_INSTALLATION_CHANGED",
            format!(
                "{endpoint} is installation {}, the catalog expects {expected}",
                server.installation
            ),
        )));
    }
    Ok((ws, server))
}

/// The client side of the handshake (steps 1–4) on an upgraded socket. A
/// fresh client nonce always replaces `hello.client_nonce`, so a recorded
/// server proof can never be replayed to this client. On success the socket
/// is blocking with no timeouts and session-sized bounds; on failure it is
/// closed.
pub fn client_handshake(
    ws: RawSocket,
    mut hello: ClientHello,
    auth: &dyn ClientAuth,
) -> Result<(RawSocket, ServerHello), DialError> {
    hello.client_nonce = new_nonce();
    // The per-peer scheme is offered exactly when there is a key for it.
    hello.features.retain(|feature| feature != PEER_HMAC_V1);
    if auth.peer_key().is_some() {
        hello.features.push(PEER_HMAC_V1.to_owned());
    }
    if let Err(detail) = check_client_hello(&hello) {
        let mut ws = ws;
        abandon_ws(&mut ws);
        return Err(DialError::Protocol(format!(
            "invalid client hello: {detail}"
        )));
    }
    let mut guarded = Guarded::new(ws, None)
        .map_err(|error| DialError::Connect(format!("handshake socket setup failed: {error}")))?;
    match client_steps(&mut guarded, &hello, auth) {
        Ok(server) => {
            let ws = guarded
                .finish(MAX_HEADER, MAX_CHUNK)
                .map_err(StepError::into_dial)?;
            Ok((ws, server))
        }
        Err(error) => {
            guarded.abandon();
            Err(error)
        }
    }
}

/// Accepts one TCP connection: upgrade, legacy detection, v3 handshake, then
/// the session.
pub fn accept(
    stream: TcpStream,
    policy: Arc<dyn AcceptPolicy>,
    o: ConnOptions,
) -> Result<(Conn, AcceptedPeer), ErrorWire> {
    let limits = Limits {
        max_header: o.max_header as u64,
        max_chunk: o.max_chunk as u64,
        max_body: o.max_body as u64,
    };
    let (ws, peer) = accept_v3(stream, policy.as_ref(), limits)?;
    let conn = into_conn(ws, Side::Server, peer.clone(), o).map_err(|error| {
        ErrorWire::coded(
            "CONNECTION_LOST",
            format!("session I/O did not start: {error}"),
        )
    })?;
    Ok((conn, peer))
}

/// Everything [`accept`] does except starting the session: the upgrade
/// (≤ 5 s), legacy detection and [`server_handshake`]. `limits` is announced
/// in `HELLO_ACK`. Returns the authenticated socket in blocking mode with no
/// socket timeouts and session-sized bounds.
pub fn accept_v3(
    stream: TcpStream,
    policy: &dyn AcceptPolicy,
    limits: Limits,
) -> Result<(RawSocket, AcceptedPeer), ErrorWire> {
    let addr = stream
        .peer_addr()
        .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));
    set_timeouts(&stream, Some(HANDSHAKE_TIMEOUT)).map_err(closed_wire)?;
    let watchdog = Watchdog::start(&stream).map_err(watchdog_wire)?;
    watchdog.arm(HANDSHAKE_TIMEOUT);
    let mut kind = UpgradeKind::Legacy;
    let upgraded = ws::accept_with(stream, PRE_AUTH_MESSAGE, |request, response| {
        kind = upgrade_kind(request);
        match kind {
            UpgradeKind::V3 => {
                response.headers_mut().insert(
                    UPGRADE_HEADER,
                    tungstenite::http::HeaderValue::from_static(UPGRADE_V3),
                );
            }
            // Loopback sessions belong on a worker's endpoint, never here.
            UpgradeKind::Loopback => {
                *response.status_mut() = tungstenite::http::StatusCode::BAD_REQUEST;
            }
            UpgradeKind::Legacy => {}
        }
    });
    watchdog.disarm();
    if watchdog.fired() {
        // The socket was shut down: even a completed upgrade is unusable.
        return Err(StepError::Timeout.into_wire());
    }
    let ws = upgraded.map_err(|error| {
        if kind == UpgradeKind::Loopback {
            ErrorWire::coded(
                "PROTOCOL_VIOLATION",
                "loopback session upgrade refused on the peer endpoint",
            )
        } else {
            upgrade_wire(&error)
        }
    })?;
    let mut guarded = Guarded::new(ws, Some(watchdog)).map_err(closed_wire)?;
    match kind {
        UpgradeKind::V3 => server_steps(guarded, policy, limits),
        _ => {
            let error = refuse_legacy(&mut guarded, addr, policy);
            guarded.close_gracefully(None);
            Err(error)
        }
    }
}

/// The server side of the handshake (steps 1–4) on an upgraded v3 socket,
/// announcing the default limits. On success the socket is blocking with no
/// timeouts and session-sized bounds; on failure a `GOAWAY` (refusals) or a
/// close frame (protocol violations) is sent and the socket closed.
pub fn server_handshake(
    ws: RawSocket,
    policy: &dyn AcceptPolicy,
) -> Result<(RawSocket, AcceptedPeer), ErrorWire> {
    let guarded = Guarded::new(ws, None).map_err(closed_wire)?;
    server_steps(guarded, policy, Limits::default())
}

/// Dials a worker's loopback endpoint (`X-Tkfs-Sync: 3-loopback`, plus
/// `X-Tkfs-Token`). There is no HELLO/AUTH: loopback sessions are local and
/// token-checked (WP4.1). With `token: None`, the token this process knows
/// for the address (`ws::tokens::lookup`) is presented, if any. A worker that
/// does not echo the header gives `NeedsUpgrade`; a refused token gives
/// `Unauthorized(ACCESS_DENIED)`.
pub fn dial_loopback(
    address: &str,
    token: Option<&str>,
    o: ConnOptions,
) -> Result<Conn, DialError> {
    let known = match token {
        Some(_) => None,
        None => ws::tokens::lookup_endpoint(address),
    };
    let mut headers = vec![(UPGRADE_HEADER, UPGRADE_LOOPBACK)];
    if let Some(token) = token.or(known.as_deref()) {
        headers.push((TOKEN_HEADER, token));
    }
    let (mut ws, response) =
        ws::connect_with(address, &headers, None, DIAL_ATTEMPT).map_err(connect_error)?;
    if echoed(&response) != Some(UPGRADE_LOOPBACK) {
        abandon_ws(&mut ws);
        return Err(DialError::NeedsUpgrade {
            detail: format!("{address} does not serve loopback sessions ({UPGRADE_HEADER})"),
        });
    }
    into_conn(ws, Side::Client, loopback_peer(), o)
        .map_err(|error| DialError::Connect(format!("session I/O did not start: {error}")))
}

/// Accepts a loopback session upgrade (≤ 5 s). Anything but
/// `X-Tkfs-Sync: 3-loopback` is refused with HTTP 400; with `token` set, a
/// missing or different `X-Tkfs-Token` is refused with HTTP 403
/// (`ACCESS_DENIED`, constant-time compare).
pub fn accept_loopback(
    stream: TcpStream,
    token: Option<&str>,
    o: ConnOptions,
) -> Result<Conn, ErrorWire> {
    set_timeouts(&stream, Some(HANDSHAKE_TIMEOUT)).map_err(closed_wire)?;
    let watchdog = Watchdog::start(&stream).map_err(watchdog_wire)?;
    watchdog.arm(HANDSHAKE_TIMEOUT);
    let mut kind = None;
    let mut authorized = false;
    let upgraded = ws::accept_with(
        stream,
        session_message_bound(o.max_header, o.max_chunk),
        |request, response| {
            let upgrade = upgrade_kind(request);
            kind = Some(upgrade);
            if upgrade != UpgradeKind::Loopback {
                *response.status_mut() = tungstenite::http::StatusCode::BAD_REQUEST;
                return;
            }
            authorized = token.is_none_or(|expected| {
                request
                    .headers()
                    .get(TOKEN_HEADER)
                    .is_some_and(|presented| ct_eq(presented.as_bytes(), expected.as_bytes()))
            });
            if !authorized {
                *response.status_mut() = tungstenite::http::StatusCode::FORBIDDEN;
                return;
            }
            response.headers_mut().insert(
                UPGRADE_HEADER,
                tungstenite::http::HeaderValue::from_static(UPGRADE_LOOPBACK),
            );
        },
    );
    watchdog.disarm();
    if watchdog.fired() {
        return Err(StepError::Timeout.into_wire());
    }
    let ws = upgraded.map_err(|error| match (kind, authorized) {
        (Some(UpgradeKind::Loopback), false) => {
            ErrorWire::coded("ACCESS_DENIED", "loopback session token refused")
        }
        (Some(UpgradeKind::V3 | UpgradeKind::Legacy), _) => ErrorWire::coded(
            "PROTOCOL_VIOLATION",
            "not a loopback session upgrade (X-Tkfs-Sync: 3-loopback)",
        ),
        _ => upgrade_wire(&error),
    })?;
    drop(watchdog);
    into_conn(ws, Side::Server, loopback_peer(), o).map_err(|error| {
        ErrorWire::coded(
            "CONNECTION_LOST",
            format!("session I/O did not start: {error}"),
        )
    })
}

/// Classifies a WebSocket upgrade request by its `X-Tkfs-Sync` header.
pub fn upgrade_kind(request: &tungstenite::handshake::server::Request) -> UpgradeKind {
    match request
        .headers()
        .get(UPGRADE_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
    {
        Some(UPGRADE_V3) => UpgradeKind::V3,
        Some(UPGRADE_LOOPBACK) => UpgradeKind::Loopback,
        _ => UpgradeKind::Legacy,
    }
}

// ---------------------------------------------------------------------------
// Client steps

fn check_client_hello(hello: &ClientHello) -> Result<(), String> {
    if !valid_id(&hello.installation) {
        return Err("installation must be 1..=128 bytes without control characters".into());
    }
    if !valid_id(&hello.session) {
        return Err("session must be 1..=128 bytes without control characters".into());
    }
    if hello.versions.is_empty() {
        return Err("no protocol version offered".into());
    }
    check_lane(hello.role, hello.lane)
}

fn client_steps(
    guarded: &mut Guarded,
    hello: &ClientHello,
    auth: &dyn ClientAuth,
) -> Result<ServerHello, DialError> {
    guarded
        .write_frame(FrameKind::Hello, hello)
        .map_err(StepError::into_dial)?;
    let server: ServerHello = match guarded.read_frame().map_err(StepError::into_dial)? {
        (FrameKind::HelloAck, header) => parse_header(&header).map_err(DialError::Protocol)?,
        (FrameKind::Goaway, header) => return Err(goaway_dial_error(&header, &hello.versions)),
        (kind, _) => {
            return Err(DialError::Protocol(format!(
                "expected HELLO_ACK or GOAWAY, received {kind:?}"
            )));
        }
    };
    if server.protocol != PROTOCOL_NAME {
        return Err(DialError::Protocol(format!(
            "server speaks {:?}, not {PROTOCOL_NAME}",
            server.protocol
        )));
    }
    if server.version != PROTOCOL_VERSION || !hello.versions.contains(&server.version) {
        return Err(DialError::Unsupported {
            offered: hello.versions.clone(),
            supported: vec![server.version],
        });
    }
    if server.role != hello.role || server.lane != hello.lane {
        return Err(DialError::Protocol(
            "server hello does not echo the requested role and lane".into(),
        ));
    }
    if !valid_id(&server.installation) {
        return Err(DialError::Protocol(
            "server hello has an invalid installation".into(),
        ));
    }
    if decode_nonce(&server.server_nonce).is_none() || server.server_nonce == hello.client_nonce {
        return Err(DialError::Protocol(
            "server hello has an invalid nonce".into(),
        ));
    }
    if server.installation == hello.installation {
        return Err(DialError::Rejected(ErrorWire::coded(
            "CANNOT_PAIR_WITH_SELF",
            "the endpoint is this installation",
        )));
    }
    let offered = hello.features.iter().any(|feature| feature == PEER_HMAC_V1);
    let per_peer = server
        .features
        .iter()
        .any(|feature| feature == PEER_HMAC_V1);
    if per_peer && !offered {
        return Err(DialError::Protocol(format!(
            "server hello chose {PEER_HMAC_V1}, which was not offered"
        )));
    }
    auth.begin_auth();
    let (key, transcript) = if per_peer {
        let key = auth.peer_key().ok_or_else(|| {
            DialError::Protocol(format!("{PEER_HMAC_V1} was offered without a key"))
        })?;
        let transcript = Transcript::per_peer(hello, &server)
            .map_err(|e| DialError::Protocol(violation_detail(e)))?;
        (key, transcript)
    } else {
        let key = auth.key(&server.salt).map_err(DialError::Unauthorized)?;
        let transcript = Transcript::new(hello, &server)
            .map_err(|e| DialError::Protocol(violation_detail(e)))?;
        (key, transcript)
    };
    let proof = hex::encode(transcript.client_proof(&key));
    guarded
        .write_frame(FrameKind::Auth, &Auth { proof })
        .map_err(StepError::into_dial)?;
    match guarded.read_frame().map_err(StepError::into_dial)? {
        (FrameKind::AuthOk, header) => {
            let ok: AuthOk = parse_header(&header).map_err(DialError::Protocol)?;
            if verify_proof(&transcript.server_proof(&key), &ok.server_proof) {
                Ok(server)
            } else if per_peer {
                Err(DialError::Unauthorized(ErrorWire::coded(
                    "PEER_REVOKED",
                    "the server could not prove it holds this peer credential",
                )))
            } else {
                Err(DialError::Unauthorized(ErrorWire::coded(
                    "INVALID_PAIRING_CODE",
                    "the server could not prove it holds this pairing code",
                )))
            }
        }
        (FrameKind::Goaway, header) => Err(goaway_dial_error(&header, &hello.versions)),
        (kind, _) => Err(DialError::Protocol(format!(
            "expected AUTH_OK or GOAWAY, received {kind:?}"
        ))),
    }
}

/// Maps a handshake `GOAWAY` to a dial error. Only the codes a handshake may
/// carry are trusted; anything else is a protocol violation.
fn goaway_dial_error(header: &[u8], offered: &[u32]) -> DialError {
    let goaway: Goaway = match parse_header(header) {
        Ok(goaway) => goaway,
        Err(detail) => return DialError::Protocol(detail),
    };
    let error = ErrorWire::new(goaway.code.clone(), goaway.message.clone());
    match goaway.code.as_str() {
        "INVALID_PAIRING_CODE" | "PAIRING_CODE_CHANGED" => DialError::Unauthorized(error),
        "PEER_PROTOCOL_UNSUPPORTED" => DialError::Unsupported {
            offered: offered.to_vec(),
            supported: supported_versions(&goaway.message),
        },
        code if GOAWAY_HANDSHAKE_CODES.contains(&code) => DialError::Rejected(error),
        code => DialError::Protocol(format!("unexpected handshake GOAWAY {code}")),
    }
}

/// The supported `versions [..]` list (the last one) of a
/// `PEER_PROTOCOL_UNSUPPORTED` message.
fn supported_versions(message: &str) -> Vec<u32> {
    message
        .rfind("versions [")
        .map(|start| &message[start + "versions [".len()..])
        .and_then(|rest| rest.split_once(']'))
        .map(|(list, _)| {
            list.split(',')
                .filter_map(|item| item.trim().parse().ok())
                .collect()
        })
        .unwrap_or_default()
}

fn connect_error(error: anyhow::Error) -> DialError {
    if let Some(status) =
        error
            .chain()
            .find_map(|cause| match cause.downcast_ref::<tungstenite::Error>() {
                Some(tungstenite::Error::Http(response)) => Some(response.status()),
                _ => None,
            })
    {
        if matches!(status.as_u16(), 401 | 403) {
            return DialError::Unauthorized(ErrorWire::coded(
                "ACCESS_DENIED",
                format!("upgrade refused with HTTP {status}"),
            ));
        }
        return DialError::Connect(format!("upgrade refused with HTTP {status}"));
    }
    let wire = ErrorWire::from_error(&error);
    if wire.code == "WS_CONNECT_FAILED" {
        let detail = wire
            .message
            .trim_start_matches("WS_CONNECT_FAILED: ")
            .to_owned();
        DialError::Connect(detail)
    } else {
        DialError::Rejected(wire)
    }
}

fn echoed(response: &tungstenite::handshake::client::Response) -> Option<&str> {
    response
        .headers()
        .get(UPGRADE_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
}

// ---------------------------------------------------------------------------
// Server steps

fn server_steps(
    mut guarded: Guarded,
    policy: &dyn AcceptPolicy,
    limits: Limits,
) -> Result<(RawSocket, AcceptedPeer), ErrorWire> {
    match server_auth(&mut guarded, policy, limits) {
        Ok(peer) => {
            let ws = guarded
                .finish(limits.max_header as usize, limits.max_chunk as usize)
                .map_err(StepError::into_wire)?;
            Ok((ws, peer))
        }
        Err(Failure::Goaway(error)) => {
            let goaway = Goaway {
                code: error.code.clone(),
                message: error.message.clone(),
            };
            if guarded.write_frame(FrameKind::Goaway, &goaway).is_ok() {
                guarded.close_gracefully(None);
            } else {
                guarded.abandon();
            }
            Err(error)
        }
        Err(Failure::Violation(detail)) => {
            let error = ErrorWire::coded("PROTOCOL_VIOLATION", &detail);
            guarded.close_gracefully(Some(CloseFrame {
                code: CloseCode::Protocol,
                reason: truncate(&error.message, 120).into(),
            }));
            Err(error)
        }
        Err(Failure::Local { error, reason }) => {
            guarded.close_gracefully(Some(CloseFrame {
                code: CloseCode::Error,
                reason: reason.into(),
            }));
            Err(error)
        }
        Err(Failure::Step(step)) => {
            guarded.abandon();
            Err(step.into_wire())
        }
    }
}

/// How the server side ends a failed handshake.
enum Failure {
    /// A refusal the dialer is told about with `GOAWAY{code}`.
    Goaway(ErrorWire),
    /// A protocol violation: close frame, no GOAWAY.
    Violation(String),
    /// This server cannot run the handshake (its own configuration is
    /// invalid): an internal-error close frame (1011) with a short `reason`,
    /// and `error` for the caller. The dialer is not blamed.
    Local {
        error: ErrorWire,
        reason: &'static str,
    },
    /// The socket timed out or closed: nothing more is sent.
    Step(StepError),
}

impl From<StepError> for Failure {
    fn from(error: StepError) -> Self {
        match error {
            StepError::Violation(detail) => Failure::Violation(detail),
            other => Failure::Step(other),
        }
    }
}

fn server_auth(
    guarded: &mut Guarded,
    policy: &dyn AcceptPolicy,
    limits: Limits,
) -> Result<AcceptedPeer, Failure> {
    let hello: ClientHello = match guarded.read_frame()? {
        (FrameKind::Hello, header) => parse_header(&header).map_err(Failure::Violation)?,
        (kind, _) => {
            return Err(Failure::Violation(format!(
                "expected HELLO, received {kind:?}"
            )));
        }
    };
    if hello.protocol != PROTOCOL_NAME || !hello.versions.contains(&PROTOCOL_VERSION) {
        return Err(Failure::Goaway(ErrorWire::coded(
            "PEER_PROTOCOL_UNSUPPORTED",
            format!(
                "offered {:?} {:?}, supported {PROTOCOL_NAME:?} versions {SUPPORTED_VERSIONS:?}",
                truncate(&hello.protocol, 32),
                &hello.versions[..hello.versions.len().min(16)],
            ),
        )));
    }
    if let Err(detail) = check_client_hello(&hello) {
        return Err(Failure::Violation(format!(
            "invalid client hello: {detail}"
        )));
    }
    if decode_nonce(&hello.client_nonce).is_none() {
        return Err(Failure::Violation(
            "invalid client hello: client_nonce must be 32 hex-encoded bytes".into(),
        ));
    }
    let installation = policy.installation();
    // Our own id goes into `ServerHello` and `T`: an invalid one is this
    // server's misconfiguration, not the dialer's violation.
    if !valid_id(&installation) {
        return Err(Failure::Local {
            error: ErrorWire::coded(
                "INSTALLATION_REQUIRED",
                "this orchestrator's installation id is not valid (1..=128 bytes without \
                 control characters); handshake refused",
            ),
            reason: "the server's installation id is invalid",
        });
    }
    if hello.installation == installation {
        return Err(Failure::Goaway(ErrorWire::coded(
            "CANNOT_PAIR_WITH_SELF",
            "the dialer is this installation",
        )));
    }
    let Some((verifier, generation)) = policy.verifier() else {
        return Err(Failure::Goaway(ErrorWire::coded(
            "PAIRING_CODE_NOT_SET",
            "this orchestrator has no pairing code",
        )));
    };
    policy.admit(&hello).map_err(Failure::Goaway)?;
    // WP4.1: a dialer offering the per-peer scheme is authenticated with its
    // credential, or refused (`PEER_REVOKED`) when there is no active one.
    let per_peer = hello.features.iter().any(|feature| feature == PEER_HMAC_V1);
    let peer_key = if per_peer {
        Some(
            policy
                .peer_key(&hello.installation)
                .map_err(Failure::Goaway)?,
        )
    } else {
        None
    };
    let server = ServerHello {
        protocol: PROTOCOL_NAME.to_owned(),
        version: PROTOCOL_VERSION,
        installation,
        role: hello.role,
        lane: hello.lane,
        salt: verifier.salt.clone(),
        server_nonce: new_nonce(),
        verifier_generation: generation,
        features: if per_peer {
            vec![PEER_HMAC_V1.to_owned()]
        } else {
            Vec::new()
        },
        limits,
    };
    let transcript = if per_peer {
        Transcript::per_peer(&hello, &server)
    } else {
        Transcript::new(&hello, &server)
    }
    .map_err(|e| Failure::Violation(violation_detail(e)))?;
    guarded.write_frame(FrameKind::HelloAck, &server)?;
    let auth: Auth = match guarded.read_frame()? {
        (FrameKind::Auth, header) => parse_header(&header).map_err(Failure::Violation)?,
        (FrameKind::Goaway, _) => {
            return Err(Failure::Step(StepError::Closed(
                "the dialer gave up after HELLO_ACK".into(),
            )));
        }
        (kind, _) => {
            return Err(Failure::Violation(format!(
                "expected AUTH, received {kind:?}"
            )));
        }
    };
    let key = peer_key.unwrap_or_else(|| verifier.key());
    if !verify_proof(&transcript.client_proof(&key), &auth.proof) {
        return Err(Failure::Goaway(if per_peer {
            ErrorWire::coded("PEER_REVOKED", "the peer credential was not accepted")
        } else {
            ErrorWire::coded("INVALID_PAIRING_CODE", "the pairing code was not accepted")
        }));
    }
    // A rotation between HELLO_ACK and AUTH invalidates the proof's generation.
    let current = policy.verifier();
    if !current.is_some_and(|(now, now_generation)| {
        now_generation == generation && ct_eq(now.hash.as_bytes(), verifier.hash.as_bytes())
    }) {
        return Err(Failure::Goaway(ErrorWire::coded(
            "PAIRING_CODE_CHANGED",
            "the pairing code changed during the handshake",
        )));
    }
    // A credential revoked (or replaced) between HELLO_ACK and AUTH.
    if per_peer
        && !policy
            .peer_key(&hello.installation)
            .is_ok_and(|now| ct_eq(&now, &key))
    {
        return Err(Failure::Goaway(ErrorWire::coded(
            "PEER_REVOKED",
            "the peer credential was revoked during the handshake",
        )));
    }
    let server_proof = hex::encode(transcript.server_proof(&key));
    guarded.write_frame(FrameKind::AuthOk, &AuthOk { server_proof })?;
    Ok(PeerInfo {
        installation: hello.installation,
        role: hello.role,
        lane: hello.lane,
        session: hello.session,
        verifier_generation: generation,
    })
}

/// Reads the legacy client's first message, reports it with the pairing code
/// redacted, and answers the legacy-format `PEER_NEEDS_UPGRADE`.
fn refuse_legacy(guarded: &mut Guarded, addr: SocketAddr, policy: &dyn AcceptPolicy) -> ErrorWire {
    // The legacy hello is one JSON message (no frame prefix): ≤ 16 KiB.
    guarded.ws.set_config(|config| {
        config.max_message_size = Some(MAX_HEADER_PRE_AUTH);
        config.max_frame_size = Some(MAX_HEADER_PRE_AUTH);
    });
    let hello = match guarded.read_message() {
        Ok(Message::Text(text)) => serde_json::from_str(text.as_str()).unwrap_or(Value::Null),
        Ok(Message::Binary(bytes)) => serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        Ok(_) => Value::Null,
        Err(error) => return error.into_wire(),
    };
    policy.legacy_attempt(addr, &redact_legacy_hello(hello));
    let _ = guarded.send(Message::Text(
        crate::model::protocol::LEGACY_NEEDS_UPGRADE_REPLY.into(),
    ));
    ErrorWire::coded(
        "PEER_NEEDS_UPGRADE",
        format!("legacy (v2) peer at {addr}; replied that it must upgrade"),
    )
}

/// The legacy hello carries the plaintext pairing code: it never leaves here.
fn redact_legacy_hello(mut hello: Value) -> Value {
    if let Some(object) = hello.as_object_mut()
        && let Some(code) = object.get_mut("code")
    {
        *code = Value::String("<redacted>".into());
    }
    hello
}

// ---------------------------------------------------------------------------
// Pre-auth socket handling

/// The pre-auth WebSocket message bound: one frame prefix plus a 16 KiB header.
const PRE_AUTH_MESSAGE: usize = PREFIX_LEN + MAX_HEADER_PRE_AUTH;
/// How long a refusing side waits for the peer to read its `GOAWAY` and close.
const CLOSE_DRAIN: Duration = Duration::from_secs(1);

fn session_message_bound(max_header: usize, max_chunk: usize) -> usize {
    PREFIX_LEN + max_header + max_chunk
}

fn loopback_peer() -> PeerInfo {
    PeerInfo {
        installation: String::new(),
        role: LaneRole::Control,
        lane: None,
        session: uuid::Uuid::new_v4().to_string(),
        verifier_generation: 0,
    }
}

fn check_lane(role: LaneRole, lane: Option<LaneSpec>) -> Result<(), String> {
    match lane {
        Some(lane) if !lane.is_valid() => Err(format!(
            "lane {}/{} is outside 1 ≤ count ≤ 15, index < count",
            lane.index, lane.count
        )),
        None if role == LaneRole::Bulk => Err("a bulk session needs a lane".into()),
        _ => Ok(()),
    }
}

fn tcp_of(ws: &RawSocket) -> Option<&TcpStream> {
    match ws.get_ref() {
        MaybeTlsStream::Plain(stream) => Some(stream),
        MaybeTlsStream::NativeTls(tls) => Some(tls.get_ref()),
        #[allow(unreachable_patterns)]
        _ => None,
    }
}

fn set_timeouts(stream: &TcpStream, timeout: Option<Duration>) -> io::Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(timeout)?;
    stream.set_write_timeout(timeout)
}

/// Raises the bounds to the session limits and starts the session (§6.2
/// step 6).
fn into_conn(mut ws: RawSocket, side: Side, peer: PeerInfo, o: ConnOptions) -> io::Result<Conn> {
    let bound = session_message_bound(o.max_header, o.max_chunk);
    ws.set_config(|config| {
        config.max_message_size = Some(bound);
        config.max_frame_size = Some(bound);
    });
    if let Some(stream) = tcp_of(&ws) {
        set_timeouts(stream, None)?;
        stream.set_nonblocking(true)?;
    }
    Conn::from_established(ws, side, peer, o)
}

/// Best-effort close frame, then drop (no drain).
fn abandon_ws(ws: &mut RawSocket) {
    if let Some(stream) = tcp_of(ws) {
        let _ = stream.set_write_timeout(Some(CLOSE_DRAIN));
    }
    let _ = ws.close(None);
    let _ = ws.flush();
}

fn closed_wire(error: io::Error) -> ErrorWire {
    ErrorWire::coded("WS_CLOSED", error)
}

/// No watchdog, no handshake: without one a trickling peer is unbounded, so
/// the connection is refused (fail closed, invariant 22).
fn watchdog_wire(error: io::Error) -> ErrorWire {
    ErrorWire::coded(
        "WS_CLOSED",
        format!("handshake watchdog could not start, connection refused: {error}"),
    )
}

fn upgrade_wire(error: &anyhow::Error) -> ErrorWire {
    match error
        .chain()
        .find_map(|cause| cause.downcast_ref::<tungstenite::Error>())
    {
        Some(error) => StepError::from_ws(error).into_wire(),
        None => ErrorWire::coded("WS_CLOSED", format!("{error:#}")),
    }
}

/// The detail of a `PROTOCOL_VIOLATION` error, without its code prefix.
fn violation_detail(error: ErrorWire) -> String {
    error
        .message
        .strip_prefix("PROTOCOL_VIOLATION: ")
        .map(str::to_owned)
        .unwrap_or(error.message)
}

fn parse_header<T: DeserializeOwned>(header: &[u8]) -> Result<T, String> {
    let value: Value = serde_json::from_slice(header)
        .map_err(|error| format!("frame header is not JSON: {error}"))?;
    if !value.is_object() {
        return Err("frame header is not a JSON object".into());
    }
    serde_json::from_value(value).map_err(|error| format!("invalid frame header: {error}"))
}

fn truncate(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// A peer-chosen close reason made safe for an error message: at most 123
/// bytes (the WebSocket limit) with control characters escaped.
fn close_reason(reason: &str) -> String {
    let mut safe = String::new();
    for c in truncate(reason, 123).chars() {
        if c.is_control() {
            safe.extend(c.escape_default());
        } else {
            safe.push(c);
        }
    }
    safe
}

/// ` (close code N: "reason")`, or ` (close code N)` without a reason.
fn close_detail(code: CloseCode, reason: &str) -> String {
    let code = u16::from(code);
    if reason.is_empty() {
        format!(" (close code {code})")
    } else {
        format!(" (close code {code}: {:?})", truncate(reason, 123))
    }
}

/// Encodes one pre-auth frame: kind, no flags, reserved zero, header length
/// (u32 LE), JSON header, no body.
fn encode_frame(kind: FrameKind, header: &impl Serialize) -> Result<Vec<u8>, String> {
    let json = serde_json::to_vec(header).map_err(|error| error.to_string())?;
    if json.len() > MAX_HEADER_PRE_AUTH {
        return Err(format!(
            "{kind:?} header of {} bytes exceeds the {MAX_HEADER_PRE_AUTH}-byte pre-auth bound",
            json.len()
        ));
    }
    let mut frame = Vec::with_capacity(PREFIX_LEN + json.len());
    frame.extend_from_slice(&[kind.as_u8(), 0, 0, 0]);
    frame.extend_from_slice(&(json.len() as u32).to_le_bytes());
    frame.extend_from_slice(&json);
    Ok(frame)
}

/// Decodes one pre-auth frame with every §6.2 bound: known kind allowed
/// before authentication, no flags, zero reserved field, header ≤ 16 KiB, and
/// nothing after the header (pre-auth kinds carry no body).
fn decode_frame(message: &[u8]) -> Result<(FrameKind, Vec<u8>), String> {
    if message.len() < PREFIX_LEN {
        return Err("truncated frame".into());
    }
    let kind = FrameKind::from_u8(message[0])
        .ok_or_else(|| format!("unknown frame kind {}", message[0]))?;
    if message[1] & !KNOWN_FLAGS != 0 {
        return Err(format!("unknown frame flags {:#04x}", message[1]));
    }
    if message[1] != 0 {
        return Err(format!("{kind:?} frames carry no flags"));
    }
    if message[2] != 0 || message[3] != 0 {
        return Err("reserved field is not zero".into());
    }
    let len = u32::from_le_bytes([message[4], message[5], message[6], message[7]]) as usize;
    if len > MAX_HEADER_PRE_AUTH {
        return Err(format!(
            "header of {len} bytes exceeds the {MAX_HEADER_PRE_AUTH}-byte pre-auth bound"
        ));
    }
    if !kind.pre_auth() {
        return Err(format!("{kind:?} frame before authentication"));
    }
    let end = PREFIX_LEN + len;
    if message.len() < end {
        return Err("truncated frame".into());
    }
    if message.len() > end {
        return Err(format!("{kind:?} frames carry no body"));
    }
    Ok((kind, message[PREFIX_LEN..end].to_vec()))
}

/// Why one handshake step failed at the socket level.
#[derive(Debug)]
enum StepError {
    /// The step overran its budget (socket timeout or watchdog).
    Timeout,
    /// The peer closed or reset the connection.
    Closed(String),
    /// The peer sent a close frame. Its reason is peer-chosen text, kept only
    /// as escaped diagnostic detail.
    PeerClosed { code: CloseCode, reason: String },
    /// The peer broke the protocol (bad frame, oversize message, text…).
    Violation(String),
}

impl StepError {
    /// A socket-level failure. A step whose watchdog fired is a
    /// [`StepError::Timeout`] whatever the socket reported; callers check that
    /// first.
    fn from_ws(error: &tungstenite::Error) -> StepError {
        use tungstenite::error::{Error, ProtocolError};
        match error {
            Error::Io(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                ) =>
            {
                StepError::Timeout
            }
            Error::Io(error) => StepError::Closed(error.to_string()),
            Error::ConnectionClosed | Error::AlreadyClosed => {
                StepError::Closed("connection closed".into())
            }
            Error::Protocol(
                ProtocolError::ResetWithoutClosingHandshake | ProtocolError::HandshakeIncomplete,
            ) => StepError::Closed(error.to_string()),
            Error::Capacity(detail) => {
                StepError::Violation(format!("message exceeds the pre-auth bound: {detail}"))
            }
            other => StepError::Violation(other.to_string()),
        }
    }

    fn into_wire(self) -> ErrorWire {
        match self {
            StepError::Timeout => ErrorWire::coded(
                "REQUEST_TIMEOUT",
                format!("handshake step exceeded {} s", HANDSHAKE_TIMEOUT.as_secs()),
            ),
            StepError::Closed(detail) => ErrorWire::coded("WS_CLOSED", detail),
            StepError::PeerClosed { code, reason } => ErrorWire::coded(
                "WS_CLOSED",
                format!(
                    "the peer closed the connection{}",
                    close_detail(code, &reason)
                ),
            ),
            StepError::Violation(detail) => ErrorWire::coded("PROTOCOL_VIOLATION", detail),
        }
    }

    fn into_dial(self) -> DialError {
        match self {
            StepError::Timeout => DialError::Timeout,
            StepError::Closed(detail) => {
                DialError::Connect(format!("connection closed during the handshake: {detail}"))
            }
            // What this server sends on a violation: the server says the
            // dialer broke the protocol.
            StepError::PeerClosed {
                code: CloseCode::Protocol,
                reason,
            } => {
                let reason = close_reason(&reason);
                let reason = reason
                    .strip_prefix("PROTOCOL_VIOLATION: ")
                    .unwrap_or(&reason);
                DialError::Protocol(if reason.is_empty() {
                    "the server closed the handshake with a protocol error".into()
                } else {
                    format!("the server closed the handshake: {reason}")
                })
            }
            StepError::PeerClosed { code, reason } => DialError::Connect(format!(
                "connection closed during the handshake: the server closed it{}",
                close_detail(code, &reason)
            )),
            StepError::Violation(detail) => DialError::Protocol(detail),
        }
    }
}

/// An upgraded socket during the handshake: pre-auth bounds, 5 s socket
/// timeouts and a step watchdog. A step whose watchdog fired fails with
/// [`StepError::Timeout`], whatever the socket reported, because the socket
/// has been shut down.
struct Guarded {
    ws: RawSocket,
    watchdog: Watchdog,
    /// The budget of one step ([`HANDSHAKE_TIMEOUT`]).
    budget: Duration,
}

impl Guarded {
    /// Fails (and drops `ws`) when the socket is not TCP or no watchdog can
    /// be started for it: the handshake is never run unbounded.
    fn new(mut ws: RawSocket, watchdog: Option<Watchdog>) -> io::Result<Guarded> {
        ws.set_config(|config| {
            config.max_message_size = Some(PRE_AUTH_MESSAGE);
            config.max_frame_size = Some(PRE_AUTH_MESSAGE);
        });
        let stream = tcp_of(&ws).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "the handshake needs a TCP socket",
            )
        })?;
        set_timeouts(stream, Some(HANDSHAKE_TIMEOUT))?;
        let watchdog = match watchdog {
            Some(watchdog) => watchdog,
            None => Watchdog::start(stream)?,
        };
        Ok(Guarded {
            ws,
            watchdog,
            budget: HANDSHAKE_TIMEOUT,
        })
    }

    /// One message within the step budget, skipping Ping/Pong. Control
    /// messages are not progress: the step deadline is also checked here, so
    /// a peer that keeps pinging cannot extend the step even if the watchdog
    /// failed to shut the socket down.
    fn read_message(&mut self) -> Result<Message, StepError> {
        let deadline = Instant::now() + self.budget;
        self.watchdog.arm(self.budget);
        let result = loop {
            match self.ws.read() {
                Ok(Message::Ping(_) | Message::Pong(_)) => {
                    if self.watchdog.fired() || Instant::now() >= deadline {
                        break Err(StepError::Timeout);
                    }
                }
                // A close is a closed connection on either side, whatever
                // its reason; only `into_dial` reads a protocol close.
                Ok(Message::Close(frame)) => {
                    break Err(match frame {
                        Some(frame) => StepError::PeerClosed {
                            code: frame.code,
                            reason: frame.reason.as_str().to_owned(),
                        },
                        None => StepError::Closed("the peer closed the connection".into()),
                    });
                }
                Ok(message) => break Ok(message),
                Err(error) => break Err(StepError::from_ws(&error)),
            }
        };
        self.watchdog.disarm();
        if self.watchdog.fired() {
            return Err(StepError::Timeout);
        }
        result
    }

    fn read_frame(&mut self) -> Result<(FrameKind, Vec<u8>), StepError> {
        match self.read_message()? {
            Message::Binary(bytes) => decode_frame(&bytes).map_err(StepError::Violation),
            Message::Text(_) => Err(StepError::Violation(
                "text message during the v3 handshake".into(),
            )),
            _ => Err(StepError::Violation("unexpected WebSocket message".into())),
        }
    }

    fn send(&mut self, message: Message) -> Result<(), StepError> {
        self.watchdog.arm(self.budget);
        let result = self.ws.send(message);
        self.watchdog.disarm();
        if self.watchdog.fired() {
            return Err(StepError::Timeout);
        }
        result.map_err(|error| StepError::from_ws(&error))
    }

    fn write_frame(&mut self, kind: FrameKind, header: &impl Serialize) -> Result<(), StepError> {
        let frame = encode_frame(kind, header).map_err(StepError::Violation)?;
        self.send(Message::Binary(frame.into()))
    }

    /// Success: no socket timeouts, session-sized bounds, blocking mode. A
    /// watchdog that fired as the last step completed has shut the socket
    /// down, so that is a [`StepError::Timeout`], not a dead socket handed
    /// to the session.
    fn finish(self, max_header: usize, max_chunk: usize) -> Result<RawSocket, StepError> {
        let Guarded {
            mut ws, watchdog, ..
        } = self;
        let fired = watchdog.fired();
        drop(watchdog);
        if fired {
            return Err(StepError::Timeout);
        }
        let bound = session_message_bound(max_header, max_chunk);
        ws.set_config(|config| {
            config.max_message_size = Some(bound);
            config.max_frame_size = Some(bound);
        });
        if let Some(stream) = tcp_of(&ws) {
            set_timeouts(stream, None)
                .map_err(|error| StepError::Closed(format!("socket setup failed: {error}")))?;
        }
        Ok(ws)
    }

    /// Sends a close frame and waits (≤ 1 s) for the peer to close, so the
    /// frames already sent (a `GOAWAY`, the legacy reply) are read before the
    /// socket goes away (closing with unread input would reset it). The wait
    /// is bounded by the watchdog and by its own deadline, so a peer that
    /// keeps sending cannot hold it.
    fn close_gracefully(mut self, frame: Option<CloseFrame>) {
        if let Some(stream) = tcp_of(&self.ws) {
            let _ = stream.set_read_timeout(Some(CLOSE_DRAIN));
            let _ = stream.set_write_timeout(Some(CLOSE_DRAIN));
        }
        let deadline = Instant::now() + CLOSE_DRAIN;
        self.watchdog.arm(CLOSE_DRAIN);
        if self.ws.close(frame).is_ok() {
            while !self.watchdog.fired() && Instant::now() < deadline {
                if self.ws.read().is_err() {
                    break;
                }
            }
        }
        self.watchdog.disarm();
    }

    /// Best-effort close frame, then drop.
    fn abandon(mut self) {
        if !self.watchdog.fired() {
            abandon_ws(&mut self.ws);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{session::auth::CodeAuth, ws::watchdog::REFUSE_WATCHDOG};
    use std::{
        cell::Cell,
        io::Read,
        net::TcpListener,
        thread::{self, LocalKey},
    };

    /// Sets a watchdog test hook on this thread until dropped.
    struct Hook(&'static LocalKey<Cell<bool>>);

    impl Hook {
        fn set(hook: &'static LocalKey<Cell<bool>>) -> Hook {
            hook.with(|hook| hook.set(true));
            Hook(hook)
        }
    }

    impl Drop for Hook {
        fn drop(&mut self) {
            self.0.with(|hook| hook.set(false));
        }
    }

    /// A policy the tested paths must never consult.
    struct Unreachable;

    impl AcceptPolicy for Unreachable {
        fn installation(&self) -> String {
            unreachable!("installation")
        }
        fn verifier(&self) -> Option<(Verifier, u64)> {
            unreachable!("verifier")
        }
        fn admit(&self, _: &ClientHello) -> Result<(), ErrorWire> {
            unreachable!("admit")
        }
        fn legacy_attempt(&self, _: SocketAddr, _: &Value) {
            unreachable!("legacy_attempt")
        }
    }

    /// An upgraded (client, server) WebSocket pair over loopback TCP.
    fn pair() -> (RawSocket, RawSocket) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            ws::accept_with(stream, 64 * 1024, |_, _| {}).unwrap()
        });
        let (client, _) = ws::connect_with(&endpoint, &[], None, DIAL_ATTEMPT).unwrap();
        (client, server.join().unwrap())
    }

    /// The peer of a refused connection sees it closed, without data.
    fn assert_closed(mut stream: TcpStream) {
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut buffer = [0u8; 64];
        assert!(matches!(stream.read(&mut buffer), Ok(0) | Err(_)));
    }

    fn accepted_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        (client, server)
    }

    /// CCR-WP2.6-2 (applied by WP3.1): a dial whose token is cancelled
    /// returns at once, inside its upgrade (a server that never answers the
    /// upgrade, where the dial would otherwise wait its 3 s budget; it closes
    /// when the dialer does, as every server does), and a dial whose token is
    /// already cancelled connects nothing.
    #[test]
    fn a_cancelled_dial_returns_at_once() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let held = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            // Reads the upgrade request and never answers; closes on EOF.
            let mut buffer = [0u8; 4096];
            while matches!(stream.read(&mut buffer), Ok(n) if n > 0) {}
        });
        let cancel = Cancel::new();
        let canceller = {
            let cancel = cancel.clone();
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(300));
                cancel.cancel();
            })
        };
        let started = Instant::now();
        let result = dial_cancellable(
            &endpoint,
            client_hello("client", LaneRole::Control, None, 2),
            &CodeAuth { code: "c".into() },
            None,
            None,
            ConnOptions::default(),
            &cancel,
        );
        let took = started.elapsed();
        canceller.join().unwrap();
        match result {
            Err(DialError::Connect(detail)) => assert!(detail.contains("cancelled"), "{detail}"),
            other => panic!("not cancelled: {:?}", other.map(|_| ())),
        }
        assert!(
            took < Duration::from_millis(1500),
            "the dial took {took:?} (its upgrade budget is 3 s)"
        );

        // An already cancelled token: nothing is dialed.
        let quiet = TcpListener::bind("127.0.0.1:0").unwrap();
        quiet.set_nonblocking(true).unwrap();
        let endpoint = format!("ws://{}", quiet.local_addr().unwrap());
        let result = dial_cancellable(
            &endpoint,
            client_hello("client", LaneRole::Control, None, 2),
            &CodeAuth { code: "c".into() },
            None,
            None,
            ConnOptions::default(),
            &cancel,
        );
        assert!(
            matches!(&result, Err(DialError::Connect(detail)) if detail.contains("cancelled")),
            "{:?}",
            result.map(|_| ())
        );
        thread::sleep(Duration::from_millis(50));
        assert!(quiet.accept().is_err(), "a cancelled dial connected");
        held.join().unwrap();

        // Unix: the shutdown ends the read in progress by itself, also when
        // the server ignores our FIN and holds the connection.
        #[cfg(unix)]
        {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let endpoint = format!("ws://{}", listener.local_addr().unwrap());
            let held = thread::spawn(move || {
                let (stream, _) = listener.accept().unwrap();
                thread::sleep(Duration::from_secs(4));
                drop(stream);
            });
            let cancel = Cancel::new();
            let canceller = {
                let cancel = cancel.clone();
                thread::spawn(move || {
                    thread::sleep(Duration::from_millis(300));
                    cancel.cancel();
                })
            };
            let started = Instant::now();
            let result = dial_cancellable(
                &endpoint,
                client_hello("client", LaneRole::Control, None, 2),
                &CodeAuth { code: "c".into() },
                None,
                None,
                ConnOptions::default(),
                &cancel,
            );
            canceller.join().unwrap();
            assert!(result.is_err());
            assert!(
                started.elapsed() < Duration::from_millis(1500),
                "{:?}",
                started.elapsed()
            );
            held.join().unwrap();
        }
    }

    #[test]
    fn a_watchdog_that_cannot_start_refuses_the_connection() {
        // Upgraded sockets for the handshake functions, made before the hook.
        let (client_ws, server_ws) = pair();
        let _hook = Hook::set(&REFUSE_WATCHDOG);
        let started = Instant::now();

        let (client, server) = accepted_pair();
        let error =
            accept_v3(server, &Unreachable, Limits::default()).expect_err("accept_v3 refuses");
        assert_eq!(error.code, "WS_CLOSED", "{error}");
        assert!(error.message.contains("watchdog"), "{error}");
        assert_closed(client);

        let (client, server) = accepted_pair();
        let error = accept_loopback(server, None, ConnOptions::default())
            .expect_err("accept_loopback refuses");
        assert_eq!(error.code, "WS_CLOSED", "{error}");
        assert_closed(client);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let error =
            ws::connect_with(&endpoint, &[], None, DIAL_ATTEMPT).expect_err("connect_with refuses");
        let text = format!("{error:#}");
        assert!(text.contains("WS_CONNECT_FAILED"), "{text}");
        assert!(text.contains("watchdog"), "{text}");

        let error = client_handshake(
            client_ws,
            client_hello("client", LaneRole::Control, None, 1),
            &CodeAuth { code: "c".into() },
        )
        .expect_err("client_handshake refuses");
        assert!(
            matches!(&error, DialError::Connect(detail) if detail.contains("watchdog")),
            "{error:?}"
        );
        let error =
            server_handshake(server_ws, &Unreachable).expect_err("server_handshake refuses");
        assert_eq!(error.code, "WS_CLOSED", "{error}");
        assert!(started.elapsed() < Duration::from_secs(3));
    }

    fn wait_fired(watchdog: &Watchdog) {
        let started = Instant::now();
        while !watchdog.fired() {
            assert!(started.elapsed() < Duration::from_secs(5), "never fired");
            thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn a_fired_watchdog_fails_the_step_and_the_finish() {
        // The budget ran out as the last step completed: no dead socket is
        // handed to the session.
        let (client, _server) = pair();
        let guarded = Guarded::new(client, None).unwrap();
        guarded.watchdog.arm(Duration::ZERO);
        wait_fired(&guarded.watchdog);
        assert!(matches!(
            guarded.finish(MAX_HEADER, MAX_CHUNK),
            Err(StepError::Timeout)
        ));

        let (client, _server) = pair();
        let mut guarded = Guarded::new(client, None).unwrap();
        guarded.watchdog.arm(Duration::ZERO);
        wait_fired(&guarded.watchdog);
        assert!(matches!(guarded.read_message(), Err(StepError::Timeout)));
        assert!(matches!(
            guarded.send(Message::Binary(vec![0; 8].into())),
            Err(StepError::Timeout)
        ));
    }

    #[test]
    fn pings_cannot_extend_a_step_or_the_close_drain() {
        // Without a watchdog to shut the socket down, a peer pinging every
        // 100 ms keeps every read short; the step and drain deadlines still
        // end both loops.
        let (client, mut server) = pair();
        let pinger = thread::spawn(move || {
            let started = Instant::now();
            while started.elapsed() < Duration::from_secs(20)
                && server.send(Message::Ping(Vec::new().into())).is_ok()
            {
                thread::sleep(Duration::from_millis(100));
            }
        });
        let mut guarded = Guarded {
            ws: client,
            watchdog: Watchdog::inert(),
            budget: Duration::from_millis(600),
        };
        let started = Instant::now();
        assert!(matches!(guarded.read_message(), Err(StepError::Timeout)));
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(600) && elapsed < Duration::from_secs(3),
            "step ended after {elapsed:?}"
        );
        let started = Instant::now();
        guarded.close_gracefully(None);
        let elapsed = started.elapsed();
        assert!(
            elapsed < CLOSE_DRAIN + Duration::from_millis(1_500),
            "drain ended after {elapsed:?}"
        );
        pinger.join().unwrap();
    }

    /// Unix only: there the socket timeouts the watchdog shrinks belong to
    /// the socket. On Windows they belong to the handle, and the bound rests
    /// on `shutdown` through a verified duplicate (see
    /// `ws::watchdog::watchdog_handle`).
    #[cfg(unix)]
    #[test]
    fn a_failed_watchdog_shutdown_still_ends_a_trickled_step() {
        use crate::ws::watchdog::FAIL_SHUTDOWN;
        use std::{io::Write, net::Shutdown};
        // The watchdog fires but its `shutdown` fails. A peer trickling one
        // data frame a byte at a time keeps every read short, so the 5 s
        // per-call timeout never trips; the timeouts the watchdog shrank end
        // the step.
        let (client, server) = pair();
        let mut guarded = {
            let _hook = Hook::set(&FAIL_SHUTDOWN);
            Guarded::new(client, None).unwrap()
        };
        guarded.budget = Duration::from_millis(600);
        let mut raw = tcp_of(&server).unwrap().try_clone().unwrap();
        let control = raw.try_clone().unwrap();
        let trickler = thread::spawn(move || {
            // An unmasked (server to client) binary frame of 60 bytes, one
            // byte every 100 ms: 6 s for the whole frame.
            if raw.write_all(&[0x82, 60]).is_err() {
                return;
            }
            for _ in 0..60 {
                thread::sleep(Duration::from_millis(100));
                if raw.write_all(&[0]).is_err() {
                    return;
                }
            }
        });
        let started = Instant::now();
        let result = guarded.read_message();
        let elapsed = started.elapsed();
        assert!(matches!(result, Err(StepError::Timeout)), "{result:?}");
        assert!(guarded.watchdog.fired());
        assert!(
            elapsed >= Duration::from_millis(600) && elapsed < Duration::from_secs(3),
            "step ended after {elapsed:?}"
        );
        let _ = control.shutdown(Shutdown::Both);
        trickler.join().unwrap();
    }

    #[test]
    fn a_peer_close_is_a_closed_connection_not_a_violation() {
        let close = |code, reason: &str| StepError::PeerClosed {
            code,
            reason: reason.into(),
        };
        // Server side: whatever the dialer's close says, it is WS_CLOSED, and
        // its reason is escaped detail only.
        for (code, reason) in [
            (CloseCode::Away, "going away"),
            (CloseCode::Protocol, "PROTOCOL_VIOLATION: forged"),
            (CloseCode::Normal, ""),
        ] {
            assert!(matches!(
                Failure::from(close(code, reason)),
                Failure::Step(StepError::PeerClosed { .. })
            ));
            let error = close(code, reason).into_wire();
            assert_eq!(error.code, "WS_CLOSED", "{error}");
            assert!(error.message.contains(reason), "{error}");
        }
        let error = close(CloseCode::Away, "line\nbreak\u{7}").into_wire();
        assert!(error.message.contains(r"line\nbreak\u{7}"), "{error}");
        assert!(!error.message.contains('\n'), "{error}");
        // Client side: a protocol close from the server is a violation it
        // reports; any other close is a closed connection.
        match close(
            CloseCode::Protocol,
            "PROTOCOL_VIOLATION: invalid client hello",
        )
        .into_dial()
        {
            DialError::Protocol(detail) => assert_eq!(
                detail,
                "the server closed the handshake: invalid client hello"
            ),
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            close(CloseCode::Protocol, "").into_dial(),
            DialError::Protocol(_)
        ));
        match close(CloseCode::Protocol, "bad\u{1b}[2J").into_dial() {
            DialError::Protocol(detail) => assert!(!detail.contains('\u{1b}'), "{detail}"),
            other => panic!("{other:?}"),
        }
        match close(CloseCode::Away, "going away").into_dial() {
            DialError::Connect(detail) => assert!(detail.contains("1001"), "{detail}"),
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            close(CloseCode::Error, "the server's installation id is invalid").into_dial(),
            DialError::Connect(_)
        ));
    }
}
