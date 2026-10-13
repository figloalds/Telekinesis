//! Multiplexed request/response connections over one authenticated socket
//! (§6.2). Stub landed by WP0.4 with the final public API; WP1.9 implements
//! it. Until then `Conn::from_established` fails with `NOT_IMPLEMENTED`, so no
//! `Conn` can exist and the methods below are unreachable placeholders.
use crate::{
    model::{
        errors::ErrorWire,
        orchestrator::CallError,
        protocol::{LaneRole, LaneSpec, frame},
    },
    session::not_implemented,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    net::TcpStream,
    sync::{Arc, mpsc::Receiver},
    time::{Duration, Instant},
};
use tungstenite::{WebSocket, stream::MaybeTlsStream};

pub use crate::model::protocol::frame::ChunkHeader;

/// The per-chunk callback of [`Conn::call_streaming`]:
/// `&mut dyn FnMut(ChunkHeader, &[u8]) -> Result<(), ErrorWire>`.
pub type OnChunk<'a> = dyn FnMut(ChunkHeader, &[u8]) -> Result<(), ErrorWire> + 'a;

/// Which end of the connection this is. Only clients originate requests.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Client,
    Server,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnOptions {
    /// HEARTBEAT after this much send idleness (5 s).
    pub heartbeat: Duration,
    /// Dead after this long with no received frame (20 s).
    pub dead_after: Duration,
    /// Header bound after authentication (64 KiB).
    pub max_header: usize,
    /// Body bound of one frame (1 MiB).
    pub max_chunk: usize,
    /// Reassembled body bound (16 MiB); per-op limits apply below it.
    pub max_body: usize,
    /// Server handler threads for this connection.
    pub handlers: usize,
    /// Queued requests per connection before `BUSY`.
    pub handler_queue: usize,
    /// Control queue bound (16 MiB).
    pub control_queue_bytes: usize,
    /// Data queue bound (32 MiB); producers block when it is full.
    pub data_queue_bytes: usize,
}

impl Default for ConnOptions {
    fn default() -> Self {
        Self {
            heartbeat: frame::HEARTBEAT_AFTER,
            dead_after: frame::DEAD_AFTER,
            max_header: frame::MAX_HEADER,
            max_chunk: frame::MAX_CHUNK,
            max_body: frame::MAX_BODY_META,
            handlers: crate::model::orchestrator::limits::CONTROL_HANDLERS,
            handler_queue: crate::model::orchestrator::limits::HANDLER_QUEUE,
            control_queue_bytes: frame::CONTROL_QUEUE_BYTES,
            data_queue_bytes: frame::DATA_QUEUE_BYTES,
        }
    }
}

impl ConnOptions {
    /// Defaults for a session of `role` (bulk servers use 2 handler threads).
    pub fn for_role(role: LaneRole) -> Self {
        let mut options = Self::default();
        if role == LaneRole::Bulk {
            options.handlers = crate::model::orchestrator::limits::BULK_HANDLERS;
        }
        options
    }
}

/// Write-queue class of an outgoing frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Priority {
    /// REQUEST/RESPONSE headers, PROGRESS, CANCEL, HEARTBEAT, GOAWAY.
    Control,
    /// Response bodies.
    Response,
    /// Data CHUNKs (round-robin across stream ids).
    Data,
}

/// A successful response.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Reply {
    pub header: Value,
    pub body: Vec<u8>,
}

/// The authenticated identity and placement of a session.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerInfo {
    pub installation: String,
    pub role: LaneRole,
    #[serde(default)]
    pub lane: Option<LaneSpec>,
    /// Session uuid from `ClientHello.session`.
    pub session: String,
    pub verifier_generation: u64,
}

/// What the accepting side learns about the dialer.
pub type AcceptedPeer = PeerInfo;

/// Connection metrics (WP1.9).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnHealth {
    pub since_ms: u64,
    #[serde(default)]
    pub rtt_ms: Option<u64>,
    pub last_rx_ms: u64,
    pub last_tx_ms: u64,
    pub bytes_up: u64,
    pub bytes_down: u64,
    pub inflight: u32,
    pub queued_bytes: u64,
}

/// A handle to one connection; clones share it. Dropping the last clone does
/// not close it: call [`Conn::close`] or [`Conn::goaway`].
#[derive(Clone)]
pub struct Conn {
    inner: Arc<Inner>,
}

// Constructed by `from_established` once WP1.9 lands.
#[allow(dead_code)]
struct Inner {
    side: Side,
    peer: PeerInfo,
    options: ConnOptions,
}

impl std::fmt::Debug for Conn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Conn")
            .field("side", &self.inner.side)
            .field("peer", &self.inner.peer)
            .field("options", &self.inner.options)
            .finish()
    }
}

fn unavailable() -> CallError {
    CallError::Unavailable(not_implemented("session::Conn").message)
}

impl Conn {
    /// Takes over an authenticated socket and starts its I/O thread.
    pub fn from_established(
        ws: WebSocket<MaybeTlsStream<TcpStream>>,
        side: Side,
        peer: PeerInfo,
        o: ConnOptions,
    ) -> std::io::Result<Conn> {
        let _ = (ws, side, peer, o);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            not_implemented("session::Conn::from_established").message,
        ))
    }

    /// One request with an absolute deadline. On expiry a CANCEL is sent and
    /// `CallError::Timeout` returned; a late reply is dropped.
    pub fn call(
        &self,
        op: &str,
        header: Value,
        body: Vec<u8>,
        prio: Priority,
        deadline: Duration,
    ) -> Result<Reply, CallError> {
        let _ = (op, header, body, prio, deadline);
        Err(unavailable())
    }

    /// A request whose response streams CHUNK frames into `on_chunk` before the
    /// final RESPONSE. PROGRESS and chunks reset the `idle` deadline. An error
    /// from `on_chunk` cancels the request.
    #[allow(clippy::too_many_arguments)]
    pub fn call_streaming(
        &self,
        op: &str,
        header: Value,
        body: Vec<u8>,
        prio: Priority,
        deadline: Option<Duration>,
        idle: Duration,
        on_chunk: &mut OnChunk<'_>,
    ) -> Result<Reply, CallError> {
        let _ = (op, header, body, prio, deadline, idle, on_chunk);
        Err(unavailable())
    }

    /// Starts a request whose body is streamed with [`Upload::chunk`].
    pub fn upload(
        &self,
        op: &str,
        header: Value,
        prio: Priority,
        idle: Duration,
    ) -> Result<Upload, CallError> {
        let _ = (op, header, prio, idle);
        Err(unavailable())
    }

    /// Serves incoming requests on this (server-side) connection.
    pub fn serve(&self, handler: Arc<dyn Handler>) {
        let _ = handler;
    }

    /// No new requests; in-flight requests complete; close after idle or 5 s.
    pub fn goaway(&self, code: &str, message: &str) {
        let _ = (code, message);
    }

    pub fn close(&self) {}

    pub fn health(&self) -> ConnHealth {
        ConnHealth::default()
    }

    /// Why the connection closed, once it has.
    pub fn closed(&self) -> Option<ErrorWire> {
        Some(not_implemented("session::Conn"))
    }

    /// Waits up to `t` for the connection to close.
    pub fn wait_closed(&self, t: Duration) -> Option<ErrorWire> {
        let _ = t;
        self.closed()
    }

    pub fn peer(&self) -> &PeerInfo {
        &self.inner.peer
    }
}

/// A streaming request body in progress.
pub struct Upload {
    id: u64,
    conn: Conn,
}

impl Upload {
    pub fn chunk(&mut self, h: ChunkHeader, bytes: &[u8]) -> Result<(), CallError> {
        let _ = (self.id, &self.conn, h, bytes);
        Err(unavailable())
    }

    /// Ends the body and waits for the response.
    pub fn finish(self) -> Result<Reply, CallError> {
        let _ = (self.id, self.conn);
        Err(unavailable())
    }
}

/// Server-side request handler; runs on the connection's handler pool.
pub trait Handler: Send + Sync {
    fn handle(&self, req: Incoming);
}

/// One request delivered to a [`Handler`].
pub struct Incoming {
    pub id: u64,
    pub op: String,
    pub header: Value,
    pub body: Vec<u8>,
    /// Streamed request parts (`objects.push`), when the op streams.
    pub chunks: Option<Receiver<(ChunkHeader, Vec<u8>)>>,
    pub deadline: Instant,
    pub responder: Responder,
}

/// Answers one request. Exactly one of `reply` or `error` should be called.
pub struct Responder {
    id: u64,
}

impl Responder {
    /// Streams one response part (backpressured by the data queue).
    pub fn chunk(&self, h: ChunkHeader, bytes: &[u8]) -> Result<(), CallError> {
        let _ = (self.id, h, bytes);
        Err(unavailable())
    }

    /// Resets the client's idle deadline.
    pub fn progress(&self) {}

    pub fn reply(self, header: Value, body: Vec<u8>) {
        let _ = (self.id, header, body);
    }

    /// A Remote error: the connection stays usable (invariant 17).
    pub fn error(self, e: ErrorWire) {
        let _ = (self.id, e);
    }

    /// Whether the client cancelled or the connection closed.
    pub fn cancelled(&self) -> bool {
        false
    }
}
