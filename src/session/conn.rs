//! Multiplexed request/response connections over one authenticated socket
//! (§6.2). Owned by WP1.9.
//!
//! A [`Conn`] is a handle to one WebSocket connection. One I/O thread
//! (`super::io`) owns the socket; everything else talks to it through the
//! shared state below and wakes it with `Poller::notify()`.
//!
//! * **Requests.** Only the client side originates requests; ids are
//!   allocated by the client. [`Conn::call`] has an absolute deadline,
//!   [`Conn::call_streaming`] an optional absolute deadline plus an idle
//!   deadline that PROGRESS and CHUNK frames reset, and [`Conn::upload`]
//!   streams the request body. On expiry, on an error from the chunk callback
//!   and when an [`Upload`] is dropped unfinished, the call sends CANCEL and
//!   drops whatever of it is still queued; a late reply is discarded. A
//!   request whose connection fails is *uncertain* (`ConnectionLost`) and is
//!   never replayed (invariant 16).
//! * **Responses.** A RESPONSE with `ok:false` is a [`CallError::Remote`] and
//!   leaves the connection usable (invariant 17).
//! * **Serving.** [`Conn::serve`] starts the connection's handler pool
//!   (`ConnOptions::handlers` threads). Requests wait in a bounded queue
//!   (`ConnOptions::handler_queue` requests beyond those idle handler threads
//!   are about to take); a full queue answers `BUSY` (Contention) at once. A
//!   streamed request waiting for a handler is also refused `BUSY` once its
//!   parts no longer fit the receive bound (`data_queue_bytes`), so it never
//!   holds the connection's reads. `ping` is answered by the I/O thread
//!   itself, so liveness checks never wait behind busy handlers.
//! * **Queues.** Outgoing frames wait in three queues, written in priority
//!   order: control (header-only REQUEST/RESPONSE, PROGRESS, CANCEL,
//!   HEARTBEAT, GOAWAY; ≤ `control_queue_bytes`), response bodies
//!   (round-robin per request id; ≤ `control_queue_bytes`) and data CHUNKs
//!   (round-robin per request id; ≤ `data_queue_bytes`). Producers block,
//!   outside the I/O thread, while their queue is full (backpressure): data
//!   producers, request bodies and handler replies wait for room; a request
//!   header that finds the control queue full is refused (`Busy`). Frames the
//!   I/O thread produces itself (`ping` replies, `BUSY` and refusals) cannot
//!   wait: while the control queue is over its bound, the server's I/O thread
//!   stops reading, so a peer that leaves our replies unread is held back by
//!   TCP (and judged dead after `dead_after` without reading anything). An
//!   empty queue admits one item of any size; producers waiting for room are
//!   admitted in FIFO order per queue, so such an item is not starved. All
//!   frames of one request id stay in order: the final RESPONSE of a streamed
//!   reply follows its chunks through the data queue.
//! * **Bodies** larger than one chunk (`max_chunk`, 1 MiB) are split: the
//!   REQUEST/RESPONSE frame carries the first part with `MORE` and
//!   `total`, CHUNK frames (`hash: None`) carry the rest, the last one with
//!   `LAST`. They are reassembled up to the per-op limit
//!   (`protocol::max_body`, capped by `ConnOptions::max_body`).
//! * **Streams.** A streamed response is CHUNK frames (`MORE`, each delivered
//!   to the `call_streaming` callback, which may hold the connection's reads
//!   for at most `dead_after` without taking any) followed by the RESPONSE.
//!   A streamed request is a REQUEST with `MORE` and no `total`, CHUNK frames
//!   with `MORE` (delivered to [`Incoming::chunks`]), and an empty CHUNK
//!   without `MORE` that ends the body.
use crate::{
    model::{
        errors::ErrorWire,
        orchestrator::CallError,
        protocol::{
            self, LaneRole, LaneSpec,
            frame::{
                self as wire, CancelHeader, FLAG_LAST, FLAG_MORE, FrameKind, ProgressHeader,
                RequestHeader, ResponseHeader,
            },
        },
    },
    session::{
        frame::{FrameError, FrameLimits, encode_with},
        io,
    },
    util::lock::lock,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::TcpStream,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Condvar, Mutex, MutexGuard, PoisonError, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::Receiver,
    },
    thread,
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
    /// Queued requests per connection before `BUSY` (beyond those idle
    /// handler threads are about to take).
    pub handler_queue: usize,
    /// Control queue bound (16 MiB).
    pub control_queue_bytes: usize,
    /// Data queue bound (32 MiB); producers block when it is full.
    pub data_queue_bytes: usize,
}

impl Default for ConnOptions {
    fn default() -> Self {
        Self {
            heartbeat: wire::HEARTBEAT_AFTER,
            dead_after: wire::DEAD_AFTER,
            max_header: wire::MAX_HEADER,
            max_chunk: wire::MAX_CHUNK,
            max_body: wire::MAX_BODY_META,
            handlers: crate::model::orchestrator::limits::CONTROL_HANDLERS,
            handler_queue: crate::model::orchestrator::limits::HANDLER_QUEUE,
            control_queue_bytes: wire::CONTROL_QUEUE_BYTES,
            data_queue_bytes: wire::DATA_QUEUE_BYTES,
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

    /// The options a connection actually runs with: timers of at least 10 ms
    /// (`dead_after` ≥ `heartbeat`), a chunk size within the wire bound of
    /// 1 MiB, at least one handler thread, and queue bounds that admit one
    /// full frame.
    pub fn sanitized(self) -> Self {
        let max_chunk = self.max_chunk.clamp(1, wire::MAX_CHUNK);
        let max_header = self.max_header.clamp(256, wire::MAX_HEADER);
        let frame = wire::PREFIX_LEN + max_header + max_chunk;
        let heartbeat = self.heartbeat.max(Duration::from_millis(10));
        Self {
            heartbeat,
            dead_after: self.dead_after.max(heartbeat),
            max_header,
            max_chunk,
            max_body: self.max_body.max(max_chunk),
            handlers: self.handlers.max(1),
            handler_queue: self.handler_queue,
            control_queue_bytes: self.control_queue_bytes.max(frame),
            data_queue_bytes: self.data_queue_bytes.max(frame),
        }
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

/// Connection metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnHealth {
    /// When the connection was established (Unix ms).
    pub since_ms: u64,
    /// Round-trip time from the last heartbeat echo.
    #[serde(default)]
    pub rtt_ms: Option<u64>,
    /// Last frame received / written (Unix ms).
    pub last_rx_ms: u64,
    pub last_tx_ms: u64,
    /// Frame bytes written / received.
    pub bytes_up: u64,
    pub bytes_down: u64,
    /// Requests in flight: pending calls plus requests being served.
    pub inflight: u32,
    /// Bytes waiting in the outgoing queues.
    pub queued_bytes: u64,
}

/// A handle to one connection; clones share it. Dropping the last clone does
/// not close it: call [`Conn::close`] or [`Conn::goaway`].
#[derive(Clone)]
pub struct Conn {
    inner: Arc<Shared>,
}

impl std::fmt::Debug for Conn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Conn")
            .field("name", &self.inner.name)
            .field("side", &self.inner.side)
            .field("peer", &self.inner.peer)
            .field("options", &self.inner.options)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Shared state between the API, the handler pool and the I/O thread.

/// How long a waiter sleeps before re-checking cancellation flags.
pub(crate) const RECHECK: Duration = wire::DEADLINE_SCAN;
/// A stand-in for "no deadline" that cannot overflow `Instant`.
const FAR: Duration = Duration::from_secs(30 * 24 * 3600);
/// Received chunk bodies at least this large are copied into recycled
/// buffers of `wire::MAX_CHUNK` bytes (see `Shared::chunk_body`).
const POOLED_CHUNK: usize = 512 * 1024;
/// Recycled chunk buffers a connection keeps (at most 4 MiB, only once it
/// has received large stream chunks).
const POOL_BUFFERS: usize = 4;
/// Longest error message carried in a RESPONSE or GOAWAY header.
const MAX_ERROR_MESSAGE: usize = 4096;

pub(crate) fn ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

pub(crate) fn after(start: Instant, duration: Duration) -> Instant {
    start.checked_add(duration).unwrap_or_else(|| start + FAR)
}

fn earliest(a: Option<Instant>, b: Option<Instant>) -> Option<Instant> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

fn truncate(mut text: String) -> String {
    if text.len() > MAX_ERROR_MESSAGE {
        let mut end = MAX_ERROR_MESSAGE;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
    text
}

/// A local refusal for a frame that cannot be encoded (nothing was sent).
fn local_error(error: FrameError) -> CallError {
    match error {
        FrameError::HeaderTooLarge { .. } | FrameError::BodyTooLarge { .. } => {
            CallError::Remote(ErrorWire::coded("WS_MESSAGE_TOO_LARGE", error))
        }
        other => CallError::Remote(ErrorWire::new("PROTOCOL_VIOLATION", other.to_string())),
    }
}

/// Outgoing queue a frame waits in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Lane {
    Control,
    Response,
    Data,
}

impl Lane {
    fn index(self) -> usize {
        match self {
            Lane::Control => 0,
            Lane::Response => 1,
            Lane::Data => 2,
        }
    }
}

/// Per-request-id FIFO queues served round-robin.
#[derive(Default)]
pub(crate) struct RoundRobin {
    streams: HashMap<u64, VecDeque<Vec<u8>>>,
    order: VecDeque<u64>,
    bytes: usize,
}

impl RoundRobin {
    fn push(&mut self, id: u64, frame: Vec<u8>) {
        self.bytes += frame.len();
        match self.streams.get_mut(&id) {
            Some(queue) => queue.push_back(frame),
            None => {
                self.streams.insert(id, VecDeque::from([frame]));
                self.order.push_back(id);
            }
        }
    }

    fn pop(&mut self) -> Option<Vec<u8>> {
        while let Some(id) = self.order.pop_front() {
            let Some(queue) = self.streams.get_mut(&id) else {
                continue;
            };
            let frame = queue.pop_front();
            if queue.is_empty() {
                self.streams.remove(&id);
            } else {
                self.order.push_back(id);
            }
            if let Some(frame) = frame {
                self.bytes -= frame.len();
                return Some(frame);
            }
        }
        None
    }

    fn holds(&self, id: u64) -> bool {
        self.streams.contains_key(&id)
    }

    fn purge(&mut self, id: u64) -> usize {
        let Some(queue) = self.streams.remove(&id) else {
            return 0;
        };
        self.order.retain(|other| *other != id);
        let bytes = queue.iter().map(Vec::len).sum::<usize>();
        self.bytes -= bytes;
        bytes
    }

    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    fn clear(&mut self) {
        self.streams.clear();
        self.order.clear();
        self.bytes = 0;
    }
}

/// The three outgoing queues of a connection.
#[derive(Default)]
pub(crate) struct Queues {
    control: VecDeque<(u64, Vec<u8>)>,
    control_bytes: usize,
    /// Requests with a PROGRESS frame still queued (one resets the same idle
    /// deadline as many).
    progress: HashSet<u64>,
    response: RoundRobin,
    pub(crate) data: RoundRobin,
}

impl Queues {
    pub(crate) fn push(&mut self, lane: Lane, id: u64, frame: Vec<u8>) {
        match lane {
            Lane::Control => {
                self.control_bytes += frame.len();
                self.control.push_back((id, frame));
            }
            Lane::Response => self.response.push(id, frame),
            Lane::Data => self.data.push(id, frame),
        }
    }

    /// Queues a PROGRESS frame of request `id` unless one is still queued.
    pub(crate) fn push_progress(&mut self, id: u64, frame: Vec<u8>) -> bool {
        if !self.progress.insert(id) {
            return false;
        }
        self.push(Lane::Control, id, frame);
        true
    }

    /// The next frame in priority order.
    pub(crate) fn pop(&mut self) -> Option<Vec<u8>> {
        if let Some((id, frame)) = self.control.pop_front() {
            self.control_bytes -= frame.len();
            if frame.first() == Some(&FrameKind::Progress.as_u8()) {
                self.progress.remove(&id);
            }
            return Some(frame);
        }
        if let Some(frame) = self.response.pop() {
            return Some(frame);
        }
        self.data.pop()
    }

    /// Drops every queued frame of request `id`; returns the bytes freed.
    pub(crate) fn purge(&mut self, id: u64) -> usize {
        let mut freed = 0;
        self.control.retain(|(other, frame)| {
            let keep = *other != id;
            if !keep {
                freed += frame.len();
            }
            keep
        });
        self.control_bytes -= freed;
        self.progress.remove(&id);
        freed + self.response.purge(id) + self.data.purge(id)
    }

    pub(crate) fn bytes(&self) -> usize {
        self.control_bytes + self.response.bytes() + self.data.bytes()
    }

    pub(crate) fn control_bytes(&self) -> usize {
        self.control_bytes
    }

    /// Whether frames of request `id` are still waiting to be written.
    fn holds(&self, id: u64) -> bool {
        self.response.holds(id)
            || self.data.holds(id)
            || self.control.iter().any(|(other, _)| *other == id)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.control.is_empty() && self.response.is_empty() && self.data.is_empty()
    }

    pub(crate) fn clear(&mut self) {
        self.control.clear();
        self.control_bytes = 0;
        self.progress.clear();
        self.response.clear();
        self.data.clear();
    }
}

/// Whether `bytes` more fit a queue holding `queued` under `cap`. An empty
/// queue admits one item of any size (producers wait for room in FIFO order,
/// so such an item is not starved by smaller ones: see `Shared::wait_room`).
fn fits(queued: usize, bytes: usize, cap: usize) -> bool {
    bytes == 0 || queued == 0 || queued + bytes <= cap
}

/// What a waiting call receives from the I/O thread. A stream chunk owns a
/// copy of its body (a slice of the received message would pin tungstenite's
/// read buffer; see `Shared::chunk_body`), with what it was charged to
/// receive-side flow control (`rx_buffered`: the whole frame, or the copy's
/// buffer if larger, plus a fixed overhead, so header-only chunks count too).
pub(crate) enum Event {
    Chunk(ChunkHeader, Vec<u8>, usize),
    /// PROGRESS frames received since the caller last looked (coalesced: one
    /// resets the idle deadline as well as many).
    Progress,
    Done(Result<Reply, ErrorWire>),
    Lost(String),
    /// The I/O thread gave up on the call: its consumer took none of its
    /// queued chunks for `dead_after` while they held the connection's reads
    /// (see `io`). The caller reports `CallError::Timeout`.
    Expired,
}

#[derive(Default)]
struct SlotEvents {
    /// Chunks and the outcome, in arrival order (PROGRESS is not queued).
    queue: VecDeque<Event>,
    /// PROGRESS frames received so far (wrapping).
    progress: u64,
}

/// One pending client call. The I/O thread pushes events only while holding
/// the state lock and while the call is still in `State::pending`, so a
/// caller that removes its own entry under the lock knows no more events can
/// arrive.
pub(crate) struct Slot {
    /// Reassembly limit of the response body.
    pub(crate) limit: usize,
    events: Mutex<SlotEvents>,
    ready: Condvar,
}

impl Slot {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            events: Mutex::new(SlotEvents::default()),
            ready: Condvar::new(),
        }
    }

    /// Queues a chunk or the outcome (a `Progress` event only counts, so a
    /// peer's PROGRESS frames cannot pile up while nobody drains the slot).
    pub(crate) fn push(&self, event: Event) {
        {
            let mut events = lock(&self.events);
            match event {
                Event::Progress => events.progress = events.progress.wrapping_add(1),
                event => events.queue.push_back(event),
            }
        }
        self.ready.notify_one();
    }

    /// PROGRESS frames received so far (compare two readings).
    fn progress(&self) -> u64 {
        lock(&self.events).progress
    }

    /// The next event, waiting until `until` (`None`: no limit). `None` on
    /// timeout. `seen` is the progress count the caller has already handled;
    /// newer PROGRESS frames are reported as one `Event::Progress`.
    fn next(&self, until: Option<Instant>, seen: &mut u64) -> Option<Event> {
        let mut events = lock(&self.events);
        loop {
            if let Some(event) = events.queue.pop_front() {
                return Some(event);
            }
            if events.progress != *seen {
                *seen = events.progress;
                return Some(Event::Progress);
            }
            match until {
                None => {
                    events = self
                        .ready
                        .wait(events)
                        .unwrap_or_else(PoisonError::into_inner)
                }
                Some(until) => {
                    let now = Instant::now();
                    if now >= until {
                        return None;
                    }
                    events = self
                        .ready
                        .wait_timeout(events, until - now)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0;
                }
            }
        }
    }

    fn drain(&self) -> Vec<Event> {
        lock(&self.events).queue.drain(..).collect()
    }

    /// What the chunks queued and not yet taken were charged to receive-side
    /// flow control (`Shared::rx_buffered`).
    fn queued_cost(&self) -> usize {
        lock(&self.events)
            .queue
            .iter()
            .map(|event| match event {
                Event::Chunk(_, _, cost) => *cost,
                _ => 0,
            })
            .sum()
    }

    /// Whether the call's outcome (`Done`, `Lost` or `Expired`) is queued.
    /// The I/O thread queues it once the call has left `State::pending`
    /// (`Done` and `Lost` in the same critical section that removes it).
    fn has_outcome(&self) -> bool {
        lock(&self.events)
            .queue
            .iter()
            .any(|event| matches!(event, Event::Done(_) | Event::Lost(_) | Event::Expired))
    }

    /// The error an unfinished upload (started at `started`) reports once its
    /// request has ended.
    fn ended_error(&self, started: Instant) -> CallError {
        let events = lock(&self.events);
        for event in events.queue.iter() {
            match event {
                Event::Done(Err(error)) => return CallError::Remote(error.clone()),
                Event::Lost(detail) => return CallError::ConnectionLost(detail.clone()),
                Event::Expired => {
                    return CallError::Timeout {
                        after_ms: ms(started.elapsed()),
                    };
                }
                _ => {}
            }
        }
        // Answered before the body ended (or abandoned): `finish` returns the
        // reply, if there is one.
        CallError::Cancelled
    }
}

/// A request being served.
pub(crate) struct Serving {
    pub(crate) cancel: Arc<AtomicBool>,
    pub(crate) deadline: Instant,
    /// Set (under the state lock) when a handler thread takes the request.
    pub(crate) started: Arc<AtomicBool>,
}

#[derive(Default)]
pub(crate) struct State {
    pub(crate) queues: Queues,
    /// Client calls in flight.
    pub(crate) pending: HashMap<u64, Arc<Slot>>,
    /// Server requests dispatched (queued or running) and not yet answered.
    pub(crate) serving: HashMap<u64, Serving>,
    /// Requests waiting for a handler thread.
    pub(crate) incoming: VecDeque<Incoming>,
    pub(crate) handler: Option<Arc<dyn Handler>>,
    handler_threads: usize,
    /// Handler threads running a request (the others take the next queued
    /// ones as soon as they run: see [`State::idle_handlers`]).
    busy_handlers: usize,
    pub(crate) space_waiters: usize,
    /// Producers waiting for room, per lane, in arrival order (tickets).
    room_waiters: [VecDeque<u64>; 3],
    next_ticket: u64,
    pub(crate) close_request: Option<ErrorWire>,
    pub(crate) goaway_sent: Option<ErrorWire>,
    pub(crate) goaway_received: Option<ErrorWire>,
    pub(crate) goaway_deadline: Option<Instant>,
    /// Why the connection closed; set once, by the I/O thread.
    pub(crate) closed: Option<ErrorWire>,
    /// The I/O thread released the socket.
    pub(crate) finished: bool,
    /// (Client.) Calls that ended (their outcome is queued) while chunks
    /// their callers had not taken yet were still queued before it: those
    /// still count against the receive bound until taken, so the hold bound
    /// (`Shared::expire_heaviest`) must be able to free them too.
    undrained: Vec<(u64, Weak<Slot>)>,
}

impl State {
    /// Why a new request is refused (nothing is sent), if it is.
    fn refusal(&self) -> Option<String> {
        self.closed
            .as_ref()
            .or(self.close_request.as_ref())
            .or(self.goaway_received.as_ref())
            .or(self.goaway_sent.as_ref())
            .map(ToString::to_string)
    }

    /// Takes queued (not yet running) requests matching `pick` out of the
    /// handler queue. The caller drops them after releasing the lock.
    pub(crate) fn take_incoming(&mut self, pick: impl Fn(u64) -> bool) -> Vec<Incoming> {
        let mut taken = Vec::new();
        let mut kept = VecDeque::with_capacity(self.incoming.len());
        for incoming in self.incoming.drain(..) {
            if pick(incoming.id) {
                taken.push(incoming);
            } else {
                kept.push_back(incoming);
            }
        }
        self.incoming = kept;
        taken
    }

    /// Handler threads not running a request: waiting for one, or just
    /// started by `serve` and about to look. The first `idle_handlers()`
    /// entries of `incoming` are about to be taken.
    pub(crate) fn idle_handlers(&self) -> usize {
        self.handler_threads.saturating_sub(self.busy_handlers)
    }

    /// Ids of queued requests that idle handler threads are about to take.
    pub(crate) fn starting(&self) -> impl Iterator<Item = u64> + '_ {
        self.incoming
            .iter()
            .take(self.idle_handlers())
            .map(|incoming| incoming.id)
    }
}

/// How a wait for queue room ended.
pub(crate) enum Room<E> {
    /// The frames fit; the caller queues them under the same lock.
    Ready,
    /// The caller's stop condition fired.
    Stopped(E),
    /// The deadline passed without room.
    Expired,
}

pub(crate) struct Shared {
    pub(crate) name: String,
    pub(crate) side: Side,
    pub(crate) peer: PeerInfo,
    pub(crate) options: ConnOptions,
    pub(crate) poller: Arc<polling::Poller>,
    pub(crate) state: Mutex<State>,
    /// Producers waiting for data-queue space.
    pub(crate) space: Condvar,
    /// Handler threads waiting for requests.
    pub(crate) work: Condvar,
    /// `wait_closed` callers.
    pub(crate) finished: Condvar,
    /// Next client request id (ids start at 1).
    pub(crate) next_id: AtomicU64,
    pub(crate) since_ms: u64,
    pub(crate) last_rx_ms: AtomicU64,
    pub(crate) last_tx_ms: AtomicU64,
    pub(crate) bytes_up: AtomicU64,
    pub(crate) bytes_down: AtomicU64,
    /// `u64::MAX` until the first heartbeat echo.
    pub(crate) rtt_ms: AtomicU64,
    /// Received stream chunks not yet taken by their callers, at their
    /// charge to the receive bound (frame bytes plus a fixed overhead).
    pub(crate) rx_buffered: AtomicUsize,
    /// Chunks taken out of `rx_buffered` so far (by their callers, or freed
    /// when a call ended): the I/O thread's evidence that consumers make
    /// progress while it holds its reads.
    pub(crate) rx_taken: AtomicU64,
    /// The I/O thread holds its reads: receivers are behind, or (server) the
    /// peer leaves our replies unread.
    pub(crate) rx_paused: AtomicBool,
    /// (Client.) Recycled buffers for large chunk bodies.
    chunk_pool: Mutex<Vec<Vec<u8>>>,
}

impl Shared {
    pub(crate) fn lock(&self) -> MutexGuard<'_, State> {
        lock(&self.state)
    }

    /// Wakes the I/O thread.
    pub(crate) fn wake(&self) {
        let _ = self.poller.notify();
    }

    /// Bounds for frames this side encodes.
    pub(crate) fn send_limits(&self) -> FrameLimits {
        FrameLimits {
            max_header: self.options.max_header,
            max_body: self.options.max_chunk,
        }
    }

    /// Bounds for frames received from the peer (the wire bound for bodies).
    pub(crate) fn receive_limits(&self) -> FrameLimits {
        FrameLimits {
            max_header: self.options.max_header,
            max_body: wire::MAX_CHUNK,
        }
    }

    /// Reassembled body limit of `op`.
    pub(crate) fn body_limit(&self, op: &str) -> usize {
        protocol::max_body(op).min(self.options.max_body)
    }

    /// Received-but-undelivered bytes above which the I/O thread stops reading.
    pub(crate) fn rx_cap(&self) -> usize {
        self.options.data_queue_bytes
    }

    /// Bound of the control queue, and separately of the response-body queue.
    pub(crate) fn tx_cap(&self) -> usize {
        self.options.control_queue_bytes
    }

    /// (Client, I/O thread.) A received stream chunk's body, copied out of
    /// the message: a slice would pin tungstenite's read buffer. A large body
    /// (from [`POOLED_CHUNK`]) goes into a recycled buffer of
    /// `wire::MAX_CHUNK` bytes, which its charge counts: a fresh allocation
    /// of that size per chunk maps fresh pages (on Windows each large
    /// allocation is its own mapping), which cost about a third of the
    /// streamed download rate where the copy itself costs little.
    pub(crate) fn chunk_body(&self, body: &[u8]) -> Vec<u8> {
        if body.len() < POOLED_CHUNK || body.len() > wire::MAX_CHUNK {
            return body.to_vec();
        }
        let recycled = lock(&self.chunk_pool).pop();
        let mut buffer = recycled.unwrap_or_else(|| Vec::with_capacity(wire::MAX_CHUNK));
        buffer.clear();
        buffer.extend_from_slice(body);
        buffer
    }

    /// Returns a chunk body taken by its caller (or dropped) to the pool, if
    /// it came from it and the pool has room.
    pub(crate) fn recycle(&self, body: Vec<u8>) {
        if body.capacity() < POOLED_CHUNK || body.capacity() > wire::MAX_CHUNK {
            return;
        }
        let mut pool = lock(&self.chunk_pool);
        if pool.len() < POOL_BUFFERS {
            pool.push(body);
        }
    }

    fn rx_consumed(&self, bytes: usize) {
        self.rx_taken.fetch_add(1, Ordering::AcqRel);
        let before = self.rx_buffered.fetch_sub(bytes, Ordering::AcqRel);
        if self.rx_paused.load(Ordering::Acquire)
            && before.saturating_sub(bytes) <= self.rx_cap() / 2
        {
            self.wake();
        }
    }

    /// Drops whatever events a finished or abandoned call left in its slot,
    /// returning their stream bytes to receive-side flow control. Every way
    /// out of a call ends here (`PendingGuard`, `Upload`), so a chunk the I/O
    /// thread counted is always uncounted, even when the caller stopped early
    /// (or the I/O thread gave up on it: `expire_heaviest`).
    fn release(&self, slot: &Slot) {
        for event in slot.drain() {
            if let Event::Chunk(_, body, cost) = event {
                self.rx_consumed(cost);
                self.recycle(body);
            }
        }
    }

    fn data_room(&self, state: &State, bytes: usize) -> bool {
        fits(
            state.queues.data.bytes(),
            bytes,
            self.options.data_queue_bytes,
        )
    }

    /// Whether `frames` fit the bounds of their queues.
    fn room(&self, state: &State, frames: &[(Lane, Vec<u8>)]) -> bool {
        let need = |lane: Lane| -> usize {
            frames
                .iter()
                .filter(|(other, _)| *other == lane)
                .map(|(_, frame)| frame.len())
                .sum()
        };
        let queues = &state.queues;
        fits(queues.control_bytes, need(Lane::Control), self.tx_cap())
            && fits(queues.response.bytes(), need(Lane::Response), self.tx_cap())
            && self.data_room(state, need(Lane::Data))
    }

    /// Drops the frames of call `id` still queued once it has its outcome (an
    /// early reply to a streamed request leaves no reason to send the rest).
    pub(crate) fn completed(&self, state: &mut State, id: u64) {
        if state.queues.purge(id) > 0 && state.space_waiters > 0 {
            self.space.notify_all();
        }
    }

    fn wait_space<'a>(
        &self,
        mut state: MutexGuard<'a, State>,
        until: Instant,
    ) -> MutexGuard<'a, State> {
        let now = Instant::now();
        let wait = until.saturating_duration_since(now).min(RECHECK);
        state.space_waiters += 1;
        let mut state = self
            .space
            .wait_timeout(state, wait)
            .unwrap_or_else(PoisonError::into_inner)
            .0;
        state.space_waiters -= 1;
        state
    }

    /// Waits until `frames` fit their queues. Producers that find no room
    /// wait in FIFO order per lane (the lane of the first frame; every batch
    /// uses one lane): while one waits, later batches for that lane queue
    /// behind it, so a batch larger than the bound (admitted only by an empty
    /// queue) is not starved by smaller ones. `stop` is checked first on
    /// every wake-up (at least every [`RECHECK`]); `until` gives the current
    /// deadline (it may move, e.g. when PROGRESS restarts an idle window).
    /// On `Ready` the caller queues the frames under the returned guard.
    pub(crate) fn wait_room<'a, E>(
        &self,
        mut state: MutexGuard<'a, State>,
        frames: &[(Lane, Vec<u8>)],
        mut stop: impl FnMut(&State) -> Option<E>,
        mut until: impl FnMut() -> Instant,
    ) -> (MutexGuard<'a, State>, Room<E>) {
        let lane = frames.first().map_or(0, |(lane, _)| lane.index());
        let mut ticket: Option<u64> = None;
        let room = loop {
            if let Some(error) = stop(&state) {
                break Room::Stopped(error);
            }
            let waiters = &state.room_waiters[lane];
            let turn = match ticket {
                None => waiters.is_empty(),
                Some(ticket) => waiters.front() == Some(&ticket),
            };
            if turn && self.room(&state, frames) {
                break Room::Ready;
            }
            let until = until();
            if Instant::now() >= until {
                break Room::Expired;
            }
            if ticket.is_none() {
                let next = state.next_ticket;
                state.next_ticket = next.wrapping_add(1);
                state.room_waiters[lane].push_back(next);
                ticket = Some(next);
            }
            state = self.wait_space(state, until);
        };
        if let Some(ticket) = ticket {
            let waiters = &mut state.room_waiters[lane];
            let first = waiters.front() == Some(&ticket);
            waiters.retain(|other| *other != ticket);
            if first && !waiters.is_empty() {
                // The next waiter's turn.
                self.space.notify_all();
            }
        }
        (state, room)
    }

    fn ensure_client(&self) -> Result<(), CallError> {
        if self.side == Side::Client {
            Ok(())
        } else {
            Err(CallError::Unavailable(
                "UNSUPPORTED_OPERATION: only the client side of a session originates requests"
                    .into(),
            ))
        }
    }

    /// Frames of one request: the REQUEST (with the first body part) and, for
    /// bodies above one chunk, CHUNK continuations. `upload` marks a streamed
    /// body (`MORE` without `total`).
    #[allow(clippy::too_many_arguments)]
    fn request_frames(
        &self,
        id: u64,
        op: &str,
        header: Value,
        body: &[u8],
        deadline: Option<Duration>,
        idle: Option<Duration>,
        prio: Priority,
        upload: bool,
    ) -> Result<Vec<(Lane, Vec<u8>)>, CallError> {
        let chunk = self.options.max_chunk;
        let split = body.len() > chunk;
        let first = &body[..body.len().min(chunk)];
        let request = RequestHeader {
            id,
            op: op.to_owned(),
            deadline_ms: deadline.map(ms),
            idle_ms: idle.map(ms),
            header,
            total: split.then_some(body.len() as u64),
        };
        let flags = if split || upload { FLAG_MORE } else { 0 };
        let head = encode_with(
            FrameKind::Request,
            flags,
            &request,
            first,
            self.send_limits(),
        )
        .map_err(local_error)?;
        let head_lane = match prio {
            Priority::Control if first.is_empty() => Lane::Control,
            Priority::Control | Priority::Response => Lane::Response,
            Priority::Data => Lane::Data,
        };
        let rest_lane = if prio == Priority::Data {
            Lane::Data
        } else {
            Lane::Response
        };
        let mut frames = vec![(head_lane, head)];
        frames.extend(
            self.continuation(id, body, first.len())
                .map_err(local_error)?
                .into_iter()
                .map(|frame| (rest_lane, frame)),
        );
        Ok(frames)
    }

    /// CHUNK continuations of `body` after its first `sent` bytes.
    fn continuation(&self, id: u64, body: &[u8], sent: usize) -> Result<Vec<Vec<u8>>, FrameError> {
        let chunk = self.options.max_chunk;
        let mut frames = Vec::new();
        let mut offset = sent;
        while offset < body.len() {
            let end = (offset + chunk).min(body.len());
            let last = end == body.len();
            let header = ChunkHeader {
                id,
                hash: None,
                offset: offset as u64,
                total: body.len() as u64,
                last,
            };
            let flags = if last { FLAG_LAST } else { FLAG_MORE };
            frames.push(encode_with(
                FrameKind::Chunk,
                flags,
                &header,
                &body[offset..end],
                self.send_limits(),
            )?);
            offset = end;
        }
        Ok(frames)
    }

    /// Frames of one response. A streamed reply (chunks already sent) goes
    /// through the data queue behind its chunks; otherwise a header-only
    /// RESPONSE is a control frame and a body goes through the response queue.
    pub(crate) fn response_frames(
        &self,
        id: u64,
        result: Result<Value, ErrorWire>,
        body: &[u8],
        streamed: bool,
    ) -> Vec<(Lane, Vec<u8>)> {
        let (header, error, body) = match result {
            Ok(header) => (header, None, body),
            Err(mut error) => {
                error.message = truncate(error.message);
                (Value::Null, Some(error), &[][..])
            }
        };
        let ok = error.is_none();
        let chunk = self.options.max_chunk;
        let split = body.len() > chunk;
        let first = &body[..body.len().min(chunk)];
        let response = ResponseHeader {
            id,
            ok,
            header,
            error,
            total: split.then_some(body.len() as u64),
        };
        let flags = if split { FLAG_MORE } else { 0 };
        let head = encode_with(
            FrameKind::Response,
            flags,
            &response,
            first,
            self.send_limits(),
        )
        .and_then(|head| Ok((head, self.continuation(id, body, first.len())?)));
        let (head, rest) = match head {
            Ok(frames) => frames,
            Err(error) => {
                // The reply header or body could not be encoded: answer with
                // an error instead, so the caller is not left waiting.
                let fallback = ResponseHeader {
                    id,
                    ok: false,
                    header: Value::Null,
                    error: Some(ErrorWire::coded(
                        "WS_MESSAGE_TOO_LARGE",
                        truncate(error.to_string()),
                    )),
                    total: None,
                };
                match encode_with(FrameKind::Response, 0, &fallback, &[], self.send_limits()) {
                    Ok(head) => (head, Vec::new()),
                    Err(_) => return Vec::new(),
                }
            }
        };
        let (head_lane, rest_lane) = if streamed {
            (Lane::Data, Lane::Data)
        } else if first.is_empty() {
            (Lane::Control, Lane::Response)
        } else {
            (Lane::Response, Lane::Response)
        };
        let mut frames = vec![(head_lane, head)];
        frames.extend(rest.into_iter().map(|frame| (rest_lane, frame)));
        frames
    }

    /// Queues a request's frames once there is room, and registers its slot.
    fn submit(
        &self,
        id: u64,
        slot: &Arc<Slot>,
        frames: Vec<(Lane, Vec<u8>)>,
        space_until: Instant,
    ) -> Result<(), CallError> {
        let control: usize = frames
            .iter()
            .filter(|(lane, _)| *lane == Lane::Control)
            .map(|(_, frame)| frame.len())
            .sum();
        let state = self.lock();
        let stop = |state: &State| {
            if let Some(reason) = state.refusal() {
                return Some(CallError::Unavailable(reason));
            }
            // A full control queue refuses a request header at once; bodies
            // and data wait for room until `space_until`.
            if control > 0 && state.queues.control_bytes + control > self.tx_cap() {
                return Some(CallError::Busy);
            }
            None
        };
        let (mut state, room) = self.wait_room(state, &frames, stop, || space_until);
        match room {
            Room::Ready => {}
            Room::Stopped(error) => return Err(error),
            Room::Expired => return Err(CallError::Busy),
        }
        state.pending.insert(id, slot.clone());
        for (lane, frame) in frames {
            state.queues.push(lane, id, frame);
        }
        drop(state);
        self.wake();
        Ok(())
    }

    /// Gives up on call `id`: unregisters it, drops its queued frames and
    /// sends CANCEL. Returns `false` when the call was no longer pending: it
    /// ended (its outcome is then in the slot, see [`Slot::has_outcome`]) or
    /// was abandoned before.
    fn abandon(&self, id: u64, slot: &Slot) -> bool {
        let removed = {
            let mut state = self.lock();
            let removed = state.pending.remove(&id).is_some();
            if removed {
                let freed = state.queues.purge(id);
                if state.closed.is_none()
                    && let Ok(cancel) = encode_with(
                        FrameKind::Cancel,
                        0,
                        &CancelHeader { id },
                        &[],
                        self.send_limits(),
                    )
                {
                    state.queues.push(Lane::Control, id, cancel);
                }
                if freed > 0 && state.space_waiters > 0 {
                    self.space.notify_all();
                }
            }
            removed
        };
        if removed {
            self.wake();
            self.release(slot);
        }
        removed
    }

    /// (Client, I/O thread, under the state lock.) Call `id` has just had
    /// its outcome queued; if chunks its caller has not taken are queued
    /// before it, it is remembered for [`Shared::expire_heaviest`].
    pub(crate) fn track_undrained(&self, state: &mut State, id: u64, slot: &Arc<Slot>) {
        state
            .undrained
            .retain(|(_, slot)| slot.upgrade().is_some_and(|slot| slot.queued_cost() > 0));
        if slot.queued_cost() > 0 {
            state.undrained.push((id, Arc::downgrade(slot)));
        }
    }

    /// (Client, I/O thread.) Gives up on the call whose queued chunks cost
    /// the most, and frees them; its caller gets `Event::Expired` (a
    /// `Timeout`) once its callback returns. A pending call is abandoned
    /// (CANCEL sent, its queued frames dropped); a call that has already
    /// ended loses its queued outcome too, as its caller can no longer have
    /// every chunk. The I/O thread does this when its reads have been held
    /// for slow consumers and none of them took anything for `dead_after`, so
    /// a stuck consumer cannot suspend liveness, and the idle limits of the
    /// other calls, for longer than that. Returns whether a call was given up.
    pub(crate) fn expire_heaviest(&self) -> bool {
        let heaviest = {
            let mut state = self.lock();
            state.undrained.retain(|(_, slot)| slot.strong_count() > 0);
            let pending = state
                .pending
                .iter()
                .map(|(id, slot)| (*id, slot.clone(), true));
            let ended = state
                .undrained
                .iter()
                .filter_map(|(id, slot)| slot.upgrade().map(|slot| (*id, slot, false)));
            pending
                .chain(ended)
                .map(|(id, slot, pending)| (slot.queued_cost(), id, slot, pending))
                .filter(|(cost, ..)| *cost > 0)
                .max_by_key(|(cost, id, ..)| (*cost, *id))
        };
        let Some((_, id, slot, pending)) = heaviest else {
            return false;
        };
        if pending {
            // `false`: the call ended meanwhile (it is then undrained, and a
            // later turn can expire it).
            if !self.abandon(id, &slot) {
                return false;
            }
        } else {
            self.lock().undrained.retain(|(other, _)| *other != id);
            self.release(&slot);
        }
        slot.push(Event::Expired);
        true
    }

    /// Waits for the outcome of call `id`.
    fn await_reply(
        &self,
        id: u64,
        slot: &Slot,
        started: Instant,
        until: Option<Instant>,
        idle: Option<Duration>,
        on_chunk: &mut OnChunk<'_>,
    ) -> Result<Reply, CallError> {
        let mut guard = PendingGuard {
            shared: self,
            id,
            slot,
            armed: true,
        };
        let mut active = Instant::now();
        let mut ended = false;
        let mut seen = slot.progress();
        loop {
            let idle_at = idle.map(|idle| after(active, idle));
            let wait = if ended {
                None
            } else {
                earliest(until, idle_at)
            };
            let Some(event) = slot.next(wait, &mut seen) else {
                let now = Instant::now();
                if idle_at.is_some_and(|at| now >= at)
                    && until.is_none_or(|until| now < until)
                    && (self.rx_paused.load(Ordering::Acquire) || self.lock().queues.holds(id))
                {
                    // The peer cannot be judged idle: this side holds its
                    // reads (some consumer is behind), so the stream's next
                    // frame may wait unread behind the hold; or part of the
                    // request itself (e.g. an upload's end) has not been
                    // written yet, typically because the peer holds its reads
                    // and the socket is full. The idle deadline restarts; the
                    // absolute deadline still applies, and a dead link is
                    // caught by liveness (`dead_after`).
                    active = now;
                    continue;
                }
                if guard.abandon() {
                    return Err(CallError::Timeout {
                        after_ms: ms(started.elapsed()),
                    });
                }
                if slot.has_outcome() {
                    // The outcome arrived as the deadline passed; it is
                    // queued, so the wait below cannot block.
                    ended = true;
                    continue;
                }
                // Abandoned earlier (an `Upload` whose `chunk` timed out):
                // nothing more will arrive.
                return Err(CallError::Timeout {
                    after_ms: ms(started.elapsed()),
                });
            };
            match event {
                Event::Chunk(header, body, cost) => {
                    self.rx_consumed(cost);
                    let taken = on_chunk(header, &body);
                    self.recycle(body);
                    if let Err(error) = taken {
                        guard.abandon();
                        return Err(CallError::Remote(error));
                    }
                    active = Instant::now();
                }
                Event::Progress => active = Instant::now(),
                Event::Done(result) => {
                    guard.armed = false;
                    return result.map_err(CallError::Remote);
                }
                Event::Lost(detail) => {
                    guard.armed = false;
                    return Err(CallError::ConnectionLost(detail));
                }
                Event::Expired => {
                    // Abandoned by the I/O thread (`Shared::expire_heaviest`).
                    guard.armed = false;
                    return Err(CallError::Timeout {
                        after_ms: ms(started.elapsed()),
                    });
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn request(
        &self,
        op: &str,
        header: Value,
        body: Vec<u8>,
        prio: Priority,
        deadline: Option<Duration>,
        idle: Option<Duration>,
        on_chunk: &mut OnChunk<'_>,
    ) -> Result<Reply, CallError> {
        let started = Instant::now();
        self.ensure_client()?;
        let limit = self.body_limit(op);
        if body.len() > limit {
            return Err(CallError::Remote(ErrorWire::coded(
                "WS_MESSAGE_TOO_LARGE",
                format!("{op} body of {} bytes exceeds {limit}", body.len()),
            )));
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let frames = self.request_frames(id, op, header, &body, deadline, idle, prio, false)?;
        drop(body);
        let slot = Arc::new(Slot::new(limit));
        let until = deadline.map(|deadline| after(started, deadline));
        let space_until = until
            .or_else(|| idle.map(|idle| after(started, idle)))
            .unwrap_or_else(|| after(started, FAR));
        self.submit(id, &slot, frames, space_until)?;
        self.await_reply(id, &slot, started, until, idle, on_chunk)
    }
}

/// Abandons a call (CANCEL) when its caller returns or unwinds early, and
/// always releases what is left in its slot.
struct PendingGuard<'a> {
    shared: &'a Shared,
    id: u64,
    slot: &'a Slot,
    armed: bool,
}

impl PendingGuard<'_> {
    fn abandon(&mut self) -> bool {
        self.armed = false;
        self.shared.abandon(self.id, self.slot)
    }
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.shared.abandon(self.id, self.slot);
        }
        // A call that stopped early (its callback failed or panicked) after
        // the I/O thread had queued more chunks, or its outcome, leaves them
        // here; no event can arrive any more.
        self.shared.release(self.slot);
    }
}

static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);

impl Conn {
    /// Takes over an authenticated socket and starts its I/O thread. The
    /// socket is switched to non-blocking mode (its handshake timeouts are
    /// cleared); frames already buffered by tungstenite are processed first.
    pub fn from_established(
        ws: WebSocket<MaybeTlsStream<TcpStream>>,
        side: Side,
        peer: PeerInfo,
        o: ConnOptions,
    ) -> std::io::Result<Conn> {
        let options = o.sanitized();
        let now_ms = crate::util::time::now_ms();
        let shared = Arc::new(Shared {
            name: format!("sess-{}", NEXT_SESSION.fetch_add(1, Ordering::Relaxed)),
            side,
            peer,
            options,
            poller: Arc::new(polling::Poller::new()?),
            state: Mutex::new(State::default()),
            space: Condvar::new(),
            work: Condvar::new(),
            finished: Condvar::new(),
            next_id: AtomicU64::new(1),
            since_ms: now_ms,
            last_rx_ms: AtomicU64::new(now_ms),
            last_tx_ms: AtomicU64::new(now_ms),
            bytes_up: AtomicU64::new(0),
            bytes_down: AtomicU64::new(0),
            rtt_ms: AtomicU64::new(u64::MAX),
            rx_buffered: AtomicUsize::new(0),
            rx_taken: AtomicU64::new(0),
            rx_paused: AtomicBool::new(false),
            chunk_pool: Mutex::new(Vec::new()),
        });
        io::start(ws, shared.clone())?;
        Ok(Conn { inner: shared })
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
        let mut unexpected = |header: ChunkHeader, _: &[u8]| -> Result<(), ErrorWire> {
            Err(ErrorWire::new(
                "PROTOCOL_VIOLATION",
                format!(
                    "PROTOCOL_VIOLATION: request {} received a stream chunk (use call_streaming)",
                    header.id
                ),
            ))
        };
        self.inner.request(
            op,
            header,
            body,
            prio,
            Some(deadline),
            None,
            &mut unexpected,
        )
    }

    /// A request whose response streams CHUNK frames into `on_chunk` before the
    /// final RESPONSE. PROGRESS and chunks reset the `idle` deadline. An error
    /// from `on_chunk` cancels the request and is returned as
    /// `CallError::Remote` (the connection stays usable). Chunks the callback
    /// has not taken yet count against the receive bound
    /// (`ConnOptions::data_queue_bytes`); above it the connection holds its
    /// reads until they are taken. If no stream callback of the connection
    /// takes anything for `dead_after` during such a hold, the call with the
    /// most chunks queued is cancelled and returns `CallError::Timeout`, so a
    /// stuck callback never suspends the connection's liveness or the other
    /// calls for longer than that.
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
        self.inner
            .request(op, header, body, prio, deadline, Some(idle), on_chunk)
    }

    /// Starts a request whose body is streamed with [`Upload::chunk`]. The
    /// REQUEST frame goes out with `prio`; chunks use the data queue.
    pub fn upload(
        &self,
        op: &str,
        header: Value,
        prio: Priority,
        idle: Duration,
    ) -> Result<Upload, CallError> {
        let shared = &self.inner;
        let started = Instant::now();
        shared.ensure_client()?;
        let id = shared.next_id.fetch_add(1, Ordering::Relaxed);
        let frames = shared.request_frames(id, op, header, &[], None, Some(idle), prio, true)?;
        let slot = Arc::new(Slot::new(shared.body_limit(op)));
        shared.submit(id, &slot, frames, after(started, idle))?;
        Ok(Upload {
            conn: self.clone(),
            id,
            slot,
            idle,
            sent: 0,
            started,
            done: false,
            failed: None,
        })
    }

    /// Serves incoming requests on this (server-side) connection with
    /// `ConnOptions::handlers` threads. Calling it again replaces the handler.
    /// Requests that arrived before the first call wait in the queue.
    pub fn serve(&self, handler: Arc<dyn Handler>) {
        let shared = &self.inner;
        if shared.side != Side::Server {
            return;
        }
        let spawn = {
            let mut state = shared.lock();
            state.handler = Some(handler);
            if state.closed.is_some() {
                0
            } else {
                let spawn = shared
                    .options
                    .handlers
                    .saturating_sub(state.handler_threads);
                state.handler_threads += spawn;
                spawn
            }
        };
        for index in 0..spawn {
            let worker = shared.clone();
            let spawned = thread::Builder::new()
                .name(format!("{}-h{index}", shared.name))
                .spawn(move || handler_loop(worker));
            if spawned.is_err() {
                shared.lock().handler_threads -= 1;
            }
        }
        shared.work.notify_all();
    }

    /// No new requests; in-flight requests complete; close after idle or 5 s.
    /// The peer closes once its own calls have ended; this side keeps reading
    /// until then (or the 5 s grace), so requests the peer sent before it
    /// read the GOAWAY are refused with `WS_CLOSED` rather than lost.
    pub fn goaway(&self, code: &str, message: &str) {
        let shared = &self.inner;
        let message = truncate(message.to_owned());
        let error = ErrorWire::new(code, message.clone());
        let frame = encode_with(
            FrameKind::Goaway,
            0,
            &wire::Goaway {
                code: code.to_owned(),
                message,
            },
            &[],
            shared.send_limits(),
        );
        {
            let mut state = shared.lock();
            if state.closed.is_some() || state.goaway_sent.is_some() {
                return;
            }
            state.goaway_sent = Some(error);
            let deadline = after(Instant::now(), wire::GOAWAY_GRACE);
            state.goaway_deadline =
                Some(state.goaway_deadline.map_or(deadline, |d| d.min(deadline)));
            if let Ok(frame) = frame {
                state.queues.push(Lane::Control, 0, frame);
            }
        }
        shared.wake();
    }

    /// Closes the connection now: calls in flight fail with `ConnectionLost`
    /// (uncertain), requests being served see `cancelled()`.
    pub fn close(&self) {
        let shared = &self.inner;
        {
            let mut state = shared.lock();
            if state.closed.is_none() && state.close_request.is_none() {
                state.close_request = Some(ErrorWire::coded(
                    "WS_CLOSED",
                    "the connection was closed locally",
                ));
            }
        }
        shared.wake();
    }

    pub fn health(&self) -> ConnHealth {
        let shared = &self.inner;
        let (inflight, queued) = {
            let state = shared.lock();
            (
                state.pending.len() + state.serving.len(),
                state.queues.bytes(),
            )
        };
        let rtt = shared.rtt_ms.load(Ordering::Relaxed);
        ConnHealth {
            since_ms: shared.since_ms,
            rtt_ms: (rtt != u64::MAX).then_some(rtt),
            last_rx_ms: shared.last_rx_ms.load(Ordering::Relaxed),
            last_tx_ms: shared.last_tx_ms.load(Ordering::Relaxed),
            bytes_up: shared.bytes_up.load(Ordering::Relaxed),
            bytes_down: shared.bytes_down.load(Ordering::Relaxed),
            inflight: u32::try_from(inflight).unwrap_or(u32::MAX),
            queued_bytes: queued as u64,
        }
    }

    /// Why the connection closed, once it has.
    pub fn closed(&self) -> Option<ErrorWire> {
        self.inner.lock().closed.clone()
    }

    /// Waits up to `t` for the connection to close and its socket to be
    /// released; returns the reason if it closed.
    pub fn wait_closed(&self, t: Duration) -> Option<ErrorWire> {
        let shared = &self.inner;
        let until = after(Instant::now(), t);
        let mut state = shared.lock();
        while !state.finished {
            let now = Instant::now();
            if now >= until {
                break;
            }
            state = shared
                .finished
                .wait_timeout(state, until - now)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        state.closed.clone()
    }

    pub fn peer(&self) -> &PeerInfo {
        &self.inner.peer
    }
}

/// A streaming request body in progress. Dropping it unfinished cancels the
/// request.
pub struct Upload {
    conn: Conn,
    id: u64,
    slot: Arc<Slot>,
    idle: Duration,
    sent: u64,
    started: Instant,
    /// The request no longer needs abandoning when dropped.
    done: bool,
    /// Why this side gave up on the request (a `chunk` that found no room
    /// within `idle`); later calls, `finish` included, report it.
    failed: Option<CallError>,
}

impl Upload {
    /// Queues one part of the body (blocks while the data queue is full, at
    /// most `idle` without a PROGRESS from the server; then the request is
    /// cancelled and `Timeout` returned, here and by every later call). `h.id`
    /// is set to this request's id. Fails once the request has ended:
    /// `Remote` if the server answered with an error (`BUSY` when no handler
    /// was free and the body did not fit its receive buffer meanwhile),
    /// `Cancelled` if it answered early (call [`Upload::finish`] for the
    /// reply), `ConnectionLost` if the connection failed.
    pub fn chunk(&mut self, h: ChunkHeader, bytes: &[u8]) -> Result<(), CallError> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        if self.done {
            return Err(CallError::Cancelled);
        }
        let shared = self.conn.inner.clone();
        let mut header = h;
        header.id = self.id;
        let flags = FLAG_MORE | if header.last { FLAG_LAST } else { 0 };
        let frame = encode_with(
            FrameKind::Chunk,
            flags,
            &header,
            bytes,
            shared.send_limits(),
        )
        .map_err(local_error)?;
        let frames = vec![(Lane::Data, frame)];
        let (id, idle, slot) = (self.id, self.idle, self.slot.clone());
        // The wait gives up after `idle` without room; a PROGRESS from the
        // server (it is alive and working on this request, e.g. while it
        // holds its reads for another stream) restarts the window.
        let mut seen = slot.progress();
        let mut until = after(Instant::now(), idle);
        let deadline = || {
            let progress = slot.progress();
            if progress != seen {
                seen = progress;
                until = after(Instant::now(), idle);
            }
            until
        };
        let ended = |state: &State| (!state.pending.contains_key(&id)).then_some(());
        let state = shared.lock();
        let (mut state, room) = shared.wait_room(state, &frames, ended, deadline);
        match room {
            Room::Ready => {}
            Room::Stopped(()) => {
                drop(state);
                return Err(self.slot.ended_error(self.started));
            }
            Room::Expired => {
                drop(state);
                if shared.abandon(self.id, &self.slot) {
                    let error = CallError::Timeout {
                        after_ms: ms(self.started.elapsed()),
                    };
                    self.done = true;
                    self.failed = Some(error.clone());
                    return Err(error);
                }
                // The request ended as the wait expired: `finish` has its
                // outcome.
                return Err(self.slot.ended_error(self.started));
            }
        }
        for (lane, frame) in frames {
            state.queues.push(lane, id, frame);
        }
        drop(state);
        shared.wake();
        self.sent += bytes.len() as u64;
        Ok(())
    }

    /// Ends the body and waits for the response (idle deadline; PROGRESS
    /// resets it, and it does not run while part of the request is still
    /// queued on this side). After a `chunk` timeout it returns that
    /// `Timeout` at once.
    pub fn finish(mut self) -> Result<Reply, CallError> {
        self.done = true;
        if let Some(error) = self.failed.take() {
            return Err(error);
        }
        let shared = self.conn.inner.clone();
        {
            let mut state = shared.lock();
            if state.pending.contains_key(&self.id) {
                let end = ChunkHeader {
                    id: self.id,
                    hash: None,
                    offset: self.sent,
                    total: self.sent,
                    last: true,
                };
                if let Ok(frame) =
                    encode_with(FrameKind::Chunk, FLAG_LAST, &end, &[], shared.send_limits())
                {
                    state.queues.push(Lane::Data, self.id, frame);
                }
            }
        }
        shared.wake();
        let mut ignore = |_: ChunkHeader, _: &[u8]| -> Result<(), ErrorWire> { Ok(()) };
        shared.await_reply(
            self.id,
            &self.slot,
            self.started,
            None,
            Some(self.idle),
            &mut ignore,
        )
    }
}

impl Drop for Upload {
    fn drop(&mut self) {
        let shared = &self.conn.inner;
        if !self.done {
            shared.abandon(self.id, &self.slot);
        }
        shared.release(&self.slot);
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
    /// Streamed request parts (`objects.push`), when the op streams. The
    /// channel disconnects when the body ends, or early when the request is
    /// cancelled (check [`Responder::cancelled`]).
    pub chunks: Option<Receiver<(ChunkHeader, Vec<u8>)>>,
    pub deadline: Instant,
    pub responder: Responder,
}

/// Answers one request. Exactly one of `reply` or `error` should be called;
/// dropping it unanswered replies `UNKNOWN_ERROR`. A reply waits while its
/// queue is full (the peer is behind on reading); it is dropped if the
/// request is cancelled or the connection closes meanwhile.
pub struct Responder {
    id: u64,
    shared: Arc<Shared>,
    cancel: Arc<AtomicBool>,
    /// Response body limit of the op.
    limit: usize,
    /// Atomics, so a `&Responder` can be shared with a progress thread.
    streamed: AtomicBool,
    answered: AtomicBool,
}

impl Responder {
    pub(crate) fn new(shared: Arc<Shared>, id: u64, cancel: Arc<AtomicBool>, limit: usize) -> Self {
        Self {
            id,
            shared,
            cancel,
            limit,
            streamed: AtomicBool::new(false),
            answered: AtomicBool::new(false),
        }
    }

    /// Streams one response part (backpressured by the data queue). `h.id` is
    /// set to this request's id.
    pub fn chunk(&self, h: ChunkHeader, bytes: &[u8]) -> Result<(), CallError> {
        if self.cancelled() {
            return Err(CallError::Cancelled);
        }
        let shared = &self.shared;
        let mut header = h;
        header.id = self.id;
        let flags = FLAG_MORE | if header.last { FLAG_LAST } else { 0 };
        let frame = encode_with(
            FrameKind::Chunk,
            flags,
            &header,
            bytes,
            shared.send_limits(),
        )
        .map_err(local_error)?;
        let frames = vec![(Lane::Data, frame)];
        let stop = |state: &State| {
            if let Some(closed) = &state.closed {
                return Some(CallError::ConnectionLost(closed.to_string()));
            }
            if self.cancelled() || !state.serving.contains_key(&self.id) {
                return Some(CallError::Cancelled);
            }
            None
        };
        let never = after(Instant::now(), FAR);
        let state = shared.lock();
        let (mut state, room) = shared.wait_room(state, &frames, stop, || never);
        match room {
            Room::Ready => {}
            Room::Stopped(error) => return Err(error),
            Room::Expired => return Err(CallError::Cancelled),
        }
        for (lane, frame) in frames {
            state.queues.push(lane, self.id, frame);
        }
        drop(state);
        shared.wake();
        self.streamed.store(true, Ordering::Release);
        Ok(())
    }

    /// Resets the client's idle deadline. A PROGRESS still queued for this
    /// request makes another one redundant, so at most one waits at a time.
    pub fn progress(&self) {
        if self.cancelled() {
            return;
        }
        let shared = &self.shared;
        let Ok(frame) = encode_with(
            FrameKind::Progress,
            0,
            &ProgressHeader {
                id: self.id,
                bytes: None,
            },
            &[],
            shared.send_limits(),
        ) else {
            return;
        };
        {
            let mut state = shared.lock();
            if !state.serving.contains_key(&self.id) || !state.queues.push_progress(self.id, frame)
            {
                return;
            }
        }
        shared.wake();
    }

    pub fn reply(self, header: Value, body: Vec<u8>) {
        if body.len() > self.limit {
            let error = ErrorWire::coded(
                "WS_MESSAGE_TOO_LARGE",
                format!("reply body of {} bytes exceeds {}", body.len(), self.limit),
            );
            self.answer(Err(error), &[]);
        } else {
            self.answer(Ok(header), &body);
        }
    }

    /// A Remote error: the connection stays usable (invariant 17).
    pub fn error(self, e: ErrorWire) {
        self.answer(Err(e), &[]);
    }

    /// Whether the client cancelled, the request's deadline passed, or the
    /// connection closed.
    pub fn cancelled(&self) -> bool {
        self.cancel.load(Ordering::Acquire)
    }

    /// Queues the reply once its queues have room (backpressure: a peer that
    /// leaves replies unread holds handlers here, not memory). Gives up when
    /// the request is cancelled or expires, or the connection closes.
    fn answer(&self, result: Result<Value, ErrorWire>, body: &[u8]) {
        self.answered.store(true, Ordering::Release);
        if self.cancelled() {
            return;
        }
        let shared = &self.shared;
        let streamed = self.streamed.load(Ordering::Acquire);
        let frames = shared.response_frames(self.id, result, body, streamed);
        {
            let gone = |state: &State| {
                (state.closed.is_some()
                    || self.cancelled()
                    || !state.serving.contains_key(&self.id))
                .then_some(())
            };
            let never = after(Instant::now(), FAR);
            let state = shared.lock();
            let (mut state, room) = shared.wait_room(state, &frames, gone, || never);
            if !matches!(room, Room::Ready) {
                return;
            }
            state.serving.remove(&self.id);
            for (lane, frame) in frames {
                state.queues.push(lane, self.id, frame);
            }
        }
        shared.wake();
    }
}

impl Drop for Responder {
    fn drop(&mut self) {
        if !self.answered.load(Ordering::Acquire) && !self.cancelled() {
            self.answer(
                Err(ErrorWire::coded(
                    "UNKNOWN_ERROR",
                    "the handler dropped the request without replying",
                )),
                &[],
            );
        }
    }
}

/// One thread of a connection's handler pool.
fn handler_loop(shared: Arc<Shared>) {
    // Every turn after the first follows a request this thread ran.
    let mut ran = false;
    loop {
        let (incoming, handler) = {
            let mut state = shared.lock();
            if ran {
                state.busy_handlers -= 1;
            }
            ran = true;
            loop {
                if let Some(incoming) = state.incoming.pop_front() {
                    state.busy_handlers += 1;
                    if let Some(serving) = state.serving.get(&incoming.id) {
                        serving.started.store(true, Ordering::Release);
                    }
                    if incoming.chunks.is_some() {
                        // Its buffered parts now count as a running
                        // consumer's (receive-side flow control).
                        shared.wake();
                    }
                    break (incoming, state.handler.clone());
                }
                if state.closed.is_some() {
                    state.handler_threads = state.handler_threads.saturating_sub(1);
                    return;
                }
                state = shared
                    .work
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        };
        match handler {
            // A panicking handler drops its responder, which answers
            // `UNKNOWN_ERROR`; the pool thread survives.
            Some(handler) => {
                let _ = catch_unwind(AssertUnwindSafe(|| handler.handle(incoming)));
            }
            None => drop(incoming),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// PROGRESS frames only count: a peer that sends them in a loop while
    /// nobody drains the slot (an `Upload` still sending its parts) cannot
    /// grow it, and the waiter sees them as one event.
    #[test]
    fn progress_events_are_coalesced() {
        let slot = Slot::new(0);
        let mut seen = slot.progress();
        for _ in 0..100_000 {
            slot.push(Event::Progress);
        }
        assert!(lock(&slot.events).queue.is_empty());
        assert!(matches!(
            slot.next(Some(Instant::now()), &mut seen),
            Some(Event::Progress)
        ));
        assert!(slot.next(Some(Instant::now()), &mut seen).is_none());
        // Chunks and the outcome keep their order; PROGRESS never displaces
        // them.
        slot.push(Event::Done(Ok(Reply::default())));
        slot.push(Event::Progress);
        assert!(matches!(slot.next(None, &mut seen), Some(Event::Done(_))));
        assert!(matches!(slot.next(None, &mut seen), Some(Event::Progress)));
        assert!(slot.next(Some(Instant::now()), &mut seen).is_none());
    }
}
