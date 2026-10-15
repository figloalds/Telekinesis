//! Peer and loopback v3 sessions (§6.2).
//!
//! * [`frame`]: the frame codec and its bounds (WP1.9);
//! * [`conn`]: multiplexed request/response connections with deadlines,
//!   cancellation, streaming, backpressure and a server handler pool (WP1.9);
//! * `io`: the per-connection I/O thread, the only code that touches a
//!   session socket (WP1.9);
//! * [`handshake`] and [`auth`]: session establishment, HMAC authentication
//!   and legacy detection (WP1.10). `dial`/`accept` (and the loopback pair)
//!   run the handshake and hand the authenticated socket to
//!   [`Conn::from_established`]; `connect_v3`/`accept_v3` stop at the
//!   authenticated socket.
pub mod auth;
pub mod conn;
pub mod frame;
pub mod handshake;
pub mod io;

pub use crate::model::{
    orchestrator::CallError,
    protocol::{
        LaneRole, LaneSpec,
        frame::{ChunkHeader, ClientHello, ServerHello},
    },
};
pub use auth::CodeAuth;
pub use conn::{
    AcceptedPeer, Conn, ConnHealth, ConnOptions, Handler, Incoming, OnChunk, PeerInfo, Priority,
    Reply, Responder, Side, Upload,
};
pub use handshake::{
    AcceptPolicy, ClientAuth, DialError, UpgradeKind, accept, accept_loopback, accept_v3,
    client_handshake, client_hello, connect_v3, dial, dial_cancellable, dial_expecting,
    dial_loopback, server_handshake, upgrade_kind,
};
