//! Peer and loopback v3 sessions (§6.2).
//!
//! Stub landed by WP0.4 with the final public signatures; WP1.9 implements
//! `frame`, `io` and `conn`, WP1.10 implements `handshake` and `auth`. Until
//! then every fallible entry point returns a `NOT_IMPLEMENTED` error.
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
