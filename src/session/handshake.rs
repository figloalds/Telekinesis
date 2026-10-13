//! Session establishment (§6.2 Upgrade and Handshake). Stub landed by WP0.4
//! with the final public signatures; WP1.10 implements it. `upgrade_kind` is
//! already real because it is a pure header check.
use crate::{
    model::{
        errors::{ErrorClass, ErrorWire},
        protocol::{
            UPGRADE_HEADER, UPGRADE_LOOPBACK, UPGRADE_V3,
            frame::{ClientHello, ServerHello},
        },
    },
    pairing_code::Verifier,
    session::{
        conn::{AcceptedPeer, Conn, ConnOptions},
        not_implemented,
    },
};
use serde_json::Value;
use std::{
    fmt,
    net::{SocketAddr, TcpStream},
    sync::Arc,
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
}

/// The accepting side's view of the catalog (WP2.8 implements it over
/// `CatalogSnapshot`).
pub trait AcceptPolicy: Send + Sync {
    /// This daemon's installation id (self-pair refusal).
    fn installation(&self) -> String;
    /// The current verifier and its generation; `None` → `PAIRING_CODE_NOT_SET`.
    fn verifier(&self) -> Option<(Verifier, u64)>;
    /// Admission beyond authentication (revocation, session caps).
    fn admit(&self, hello: &ClientHello) -> Result<(), ErrorWire>;
    /// A legacy (v2) client tried to connect; `hello` is its first message.
    fn legacy_attempt(&self, addr: SocketAddr, hello: &Value);
}

/// Dials `endpoint`, runs the v3 handshake and returns the authenticated
/// connection and the server's hello. Stub: WP1.10.
pub fn dial(
    endpoint: &str,
    hello: ClientHello,
    auth: &dyn ClientAuth,
    connector: Option<tungstenite::Connector>,
    o: ConnOptions,
) -> Result<(Conn, ServerHello), DialError> {
    let _ = (endpoint, hello, auth, connector, o);
    Err(DialError::Rejected(not_implemented("session::dial")))
}

/// Accepts one TCP connection: upgrade, legacy detection, v3 handshake.
/// Stub: WP1.10.
pub fn accept(
    stream: TcpStream,
    policy: Arc<dyn AcceptPolicy>,
    o: ConnOptions,
) -> Result<(Conn, AcceptedPeer), ErrorWire> {
    let _ = (stream, policy, o);
    Err(not_implemented("session::accept"))
}

/// Dials a worker's loopback endpoint (`X-Tkfs-Sync: 3-loopback`; the token is
/// sent as `X-Tkfs-Token` from WP4.1). Stub: WP1.10.
pub fn dial_loopback(
    address: &str,
    token: Option<&str>,
    o: ConnOptions,
) -> Result<Conn, DialError> {
    let _ = (address, token, o);
    Err(DialError::Rejected(not_implemented(
        "session::dial_loopback",
    )))
}

/// Accepts a loopback session upgrade. Stub: WP1.10.
pub fn accept_loopback(
    stream: TcpStream,
    token: Option<&str>,
    o: ConnOptions,
) -> Result<Conn, ErrorWire> {
    let _ = (stream, token, o);
    Err(not_implemented("session::accept_loopback"))
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
