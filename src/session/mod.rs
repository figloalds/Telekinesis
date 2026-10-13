//! Peer and loopback v3 sessions (§6.2).
//!
//! * [`frame`]: the frame codec and its bounds (WP1.9);
//! * [`conn`]: multiplexed request/response connections with deadlines,
//!   cancellation, streaming, backpressure and a server handler pool (WP1.9);
//! * `io`: the per-connection I/O thread, the only code that touches a
//!   session socket (WP1.9);
//! * [`handshake`] and [`auth`]: session establishment (WP1.10). Until that
//!   package lands, its fallible entry points return `NOT_IMPLEMENTED`.
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
    AcceptPolicy, ClientAuth, DialError, UpgradeKind, accept, accept_loopback, dial, dial_loopback,
    upgrade_kind,
};

/// The code every stubbed entry point fails with until its package lands.
pub(crate) const NOT_IMPLEMENTED: &str = "NOT_IMPLEMENTED";

pub(crate) fn not_implemented(what: &str) -> crate::model::errors::ErrorWire {
    crate::model::errors::ErrorWire::new(
        NOT_IMPLEMENTED,
        format!("{NOT_IMPLEMENTED}: {what} lands in Wave 1"),
    )
}
