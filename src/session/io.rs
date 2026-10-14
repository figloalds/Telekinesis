//! The per-connection I/O thread (§6.2 I/O model). Owned by WP1.9. It has no
//! public API: `Conn` owns it.
//!
//! One thread per connection owns the `WebSocket<MaybeTlsStream<TcpStream>>`
//! in non-blocking mode plus a `polling::Poller` (oneshot interest, re-armed
//! after every delivered event). Nothing else touches the socket, so
//! tungstenite's automatic Pong and Close replies, which `read()` writes, are
//! safe. Each turn of the loop:
//!
//! 1. reads until `WouldBlock` (always drained, because TLS and tungstenite
//!    buffer internally; a turn stops early only after a large burst and then
//!    polls again without sleeping) and dispatches every frame;
//! 2. retries delivery of streamed request parts its handlers have not taken;
//! 3. runs the timers: dead after `dead_after` without hearing from the peer,
//!    HEARTBEAT after `heartbeat` of send idleness (one queued at a time), the
//!    250 ms scan of request deadlines, GOAWAY grace;
//! 4. writes from the priority queues while tungstenite's buffer holds less
//!    than 256 KiB, then flushes (`WouldBlock` arms writable interest);
//! 5. closes gracefully once a GOAWAY is in effect and nothing is in flight;
//! 6. waits for readiness, a producer's `notify()`, or the next timer.
//!
//! Liveness evidence is a received message or readable readiness of the
//! socket (bytes arrived, possibly part of a large message on a slow link).
//!
//! Reads are held in two cases, and TCP then pushes back on the peer:
//! * receive-side flow control: more than `data_queue_bytes` of received
//!   stream data waits for its consumers; reading resumes once they are back
//!   under half of that. Each buffered part counts its whole frame plus a
//!   fixed overhead (`part_cost`), so empty or header-only parts count too.
//!   Only consumers that run (or that idle handler threads are about to
//!   start) can cause a hold: a streamed request still
//!   waiting for a handler may buffer parts only while they fit the bound,
//!   and is refused `BUSY` otherwise (`make_room`), so a hold always ends as
//!   running consumers catch up. Liveness is not judged while this hold lasts
//!   (the silence is ours), and neither are the idle limits of streams whose
//!   data may wait unread behind it; the server tells their clients with
//!   PROGRESS (`progress_held_streams`);
//! * (server) outgoing pressure: the control queue holds more than
//!   `control_queue_bytes`, because the peer leaves our replies unread while
//!   it keeps sending requests. Every request read produces a reply, so not
//!   reading is what bounds the queue (`ping` replies, `BUSY` and refusals
//!   come from this thread, which cannot wait for room). Reading resumes at
//!   half the bound; the peer is dead after `dead_after` without taking any
//!   byte from us.
use crate::{
    model::{
        errors::ErrorWire,
        protocol::{
            self, PingReply,
            frame::{
                CancelHeader, ChunkHeader, FrameKind, Goaway, Heartbeat, HeartbeatEcho,
                ProgressHeader, RequestHeader, ResponseHeader,
            },
            ops,
        },
    },
    session::{
        conn::{Event, Incoming, Lane, Reply, Responder, Serving, Shared, Side, after, ms},
        frame::{FrameRef, decode_ref, encode_with},
    },
    util::time::now_ms,
};
use polling::{Event as Readiness, Events};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt, io,
    net::{Shutdown, TcpStream},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{SyncSender, TrySendError, sync_channel},
    },
    thread,
    time::{Duration, Instant},
};
use tungstenite::{
    Bytes, Message, WebSocket,
    error::{Error as WsError, ProtocolError},
    protocol::{CloseFrame, frame::coding::CloseCode},
    stream::MaybeTlsStream,
};

type Socket = WebSocket<MaybeTlsStream<TcpStream>>;

/// Poller key of the connection's socket.
const KEY: usize = 7;
/// A read turn yields after this many bytes or messages, and a write turn
/// after this many bytes (the loop then polls again without sleeping), so
/// reads, writes and timers interleave while the socket never blocks. One
/// 1 MiB frame per turn measured best (release loopback download about
/// 850–965 MiB/s against 450–540 with 4 MiB turns) and keeps a reply that
/// waits for the next read turn behind at most one frame of writing.
const READ_BURST_BYTES: usize = 1024 * 1024;
const READ_BURST_MESSAGES: usize = 1024;
const WRITE_BURST_BYTES: usize = 1024 * 1024;
/// Parts of one streamed request buffered in its handler channel.
const UPLOAD_CHANNEL: usize = 4;
/// Idle limit of a streamed request that announced none.
const DEFAULT_UPLOAD_IDLE: Duration = Duration::from_secs(60);
/// Retry period while parts wait for a slow handler or reading is paused.
const RETRY: Duration = Duration::from_millis(5);
/// How long a closing connection keeps flushing.
const LINGER: Duration = Duration::from_millis(1000);
/// A request whose deadline is unknown expires this long after it arrived.
const DEFAULT_DEADLINE: Duration = Duration::from_secs(24 * 3600);
/// Expiry of a response reassembly (the scan drops it once its call ends).
const NEVER: Duration = Duration::from_secs(30 * 24 * 3600);
/// Memory a buffered stream part holds beyond its frame bytes (its queue
/// entry and the parsed header's fixed fields).
const PART_OVERHEAD: usize = 128;

/// What one received stream part costs the receive bound
/// (`data_queue_bytes`, invariant 22): its whole frame (prefix, header and
/// body; a client-side chunk is a slice that keeps the whole message alive,
/// a server-side part keeps its body and the parsed header, whose `hash` is
/// at most the header's length) plus [`PART_OVERHEAD`]. Header-only parts
/// therefore count like data, and every buffered part costs more than zero,
/// so a count of zero always means nothing is buffered.
fn part_cost(frame_len: usize) -> usize {
    frame_len.saturating_add(PART_OVERHEAD)
}

/// The TCP socket under the WebSocket (and TLS) layers.
fn tcp(ws: &Socket) -> io::Result<&TcpStream> {
    match ws.get_ref() {
        MaybeTlsStream::Plain(stream) => Ok(stream),
        MaybeTlsStream::NativeTls(stream) => Ok(stream.get_ref()),
        _ => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "UNSUPPORTED_OPERATION: unsupported TLS stream",
        )),
    }
}

fn would_block(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    )
}

/// Takes over `ws` and starts the connection's I/O thread.
pub(crate) fn start(mut ws: Socket, shared: Arc<Shared>) -> io::Result<()> {
    {
        let stream = tcp(&ws)?;
        stream.set_read_timeout(None)?;
        stream.set_write_timeout(None)?;
        let _ = stream.set_nodelay(true);
        stream.set_nonblocking(true)?;
    }
    let max_message = shared.receive_limits().max_message();
    ws.set_config(|config| {
        config.max_message_size = Some(max_message);
        config.max_frame_size = Some(max_message);
    });
    // SAFETY: `Io` owns the socket and deletes it from the poller in `Drop`,
    // before the socket itself is dropped.
    unsafe { shared.poller.add(tcp(&ws)?, Readiness::readable(KEY))? };
    let io = Io::new(ws, shared.clone());
    thread::Builder::new()
        .name(shared.name.clone())
        .spawn(move || io.run_guarded())?;
    Ok(())
}

/// Why the loop stopped, which decides how the socket is released.
enum Stop {
    /// Local close or a completed GOAWAY: close handshake, short linger.
    Graceful(ErrorWire),
    /// The peer sent Close: flush tungstenite's automatic reply.
    Remote(ErrorWire),
    /// The peer broke the protocol: GOAWAY + Close, short linger.
    Violation(ErrorWire),
    /// The transport failed or went silent: drop it.
    Lost(ErrorWire),
}

impl Stop {
    fn reason(&self) -> &ErrorWire {
        match self {
            Stop::Graceful(reason)
            | Stop::Remote(reason)
            | Stop::Violation(reason)
            | Stop::Lost(reason) => reason,
        }
    }
}

fn violation(detail: impl fmt::Display) -> Stop {
    Stop::Violation(ErrorWire::new("PROTOCOL_VIOLATION", detail.to_string()))
}

/// A body being reassembled from CHUNK continuations.
struct Partial {
    total: usize,
    body: Vec<u8>,
    expires: Instant,
    kind: PartialKind,
}

enum PartialKind {
    Request(RequestHeader),
    Response(Value),
}

/// A part of a streamed request body that waits for room in its handler's
/// channel.
struct Part {
    header: ChunkHeader,
    body: Vec<u8>,
    /// Its charge to the receive bound ([`part_cost`]).
    cost: usize,
}

/// A streamed request body being delivered to its handler.
struct UploadIn {
    /// `None` once the body ended with nothing left to deliver (the handler's
    /// channel then disconnects after its buffered parts).
    tx: Option<SyncSender<(ChunkHeader, Vec<u8>)>>,
    cancel: Arc<AtomicBool>,
    /// Set by the handler pool when a handler thread takes the request.
    started: Arc<AtomicBool>,
    /// This thread's view of `started`: a running stream's undelivered parts
    /// count in `Io::overflow_bytes` (and may hold reads); a waiting stream's
    /// parts, channel included, count in `Io::waiting_bytes` (and never hold
    /// reads: see `Io::make_room`). Parts count at their [`part_cost`].
    running: bool,
    /// Cost of this stream's parts counted in `Io::waiting_bytes` (waiting
    /// only).
    waiting: usize,
    overflow: VecDeque<Part>,
    ended: bool,
    idle: Duration,
    /// Last progress: a part arrived, or the handler took one.
    active: Instant,
}

impl UploadIn {
    /// Cost of the parts waiting for room in the channel.
    fn overflow_len(&self) -> usize {
        self.overflow.iter().map(|part| part.cost).sum()
    }

    /// Its running handler leaves parts untaken (its channel is full).
    fn behind(&self) -> bool {
        self.running && !self.overflow.is_empty()
    }
}

struct Io {
    ws: Socket,
    shared: Arc<Shared>,
    events: Events,
    /// Interest currently armed in the poller (`None`: disarmed by delivery).
    armed: Option<(bool, bool)>,
    /// Bytes handed to tungstenite since its last complete flush.
    unflushed: usize,
    write_blocked: bool,
    /// The last read turn stopped before `WouldBlock`.
    read_more: bool,
    /// The last write turn stopped with frames still queued.
    write_more: bool,
    /// Reading is held: received stream data waits for slow consumers.
    rx_held: bool,
    /// Reading is held (server): the peer leaves our replies unread.
    tx_held: bool,
    /// Last liveness evidence: a received message or readable readiness.
    last_rx: Instant,
    last_tx: Instant,
    /// Last time the socket took bytes from us: a flush that got everything
    /// out, or writable readiness after a blocked write.
    last_write_progress: Instant,
    last_heartbeat: Instant,
    heartbeat_seq: u64,
    /// A HEARTBEAT is queued and not yet written.
    heartbeat_queued: bool,
    /// The peer's last heartbeat `(seq, at_ms, received)`, echoed back.
    peer_heartbeat: Option<(u64, u64, Instant)>,
    next_scan: Instant,
    partial: HashMap<u64, Partial>,
    partial_bytes: usize,
    uploads: HashMap<u64, UploadIn>,
    /// Cost of the parts of running streams waiting for room in their
    /// handler channels.
    overflow_bytes: usize,
    /// Cost of the parts (channel and overflow) of streams no handler has
    /// taken yet.
    waiting_bytes: usize,
}

impl Drop for Io {
    fn drop(&mut self) {
        if let Ok(stream) = tcp(&self.ws) {
            let _ = self.shared.poller.delete(stream);
        }
    }
}

impl Io {
    fn new(ws: Socket, shared: Arc<Shared>) -> Self {
        let now = Instant::now();
        Self {
            ws,
            shared,
            events: Events::new(),
            armed: Some((true, false)),
            unflushed: 0,
            write_blocked: false,
            read_more: true,
            write_more: false,
            rx_held: false,
            tx_held: false,
            last_rx: now,
            last_tx: now,
            last_write_progress: now,
            last_heartbeat: now,
            heartbeat_seq: 0,
            heartbeat_queued: false,
            peer_heartbeat: None,
            next_scan: now + protocol::frame::DEADLINE_SCAN,
            partial: HashMap::new(),
            partial_bytes: 0,
            uploads: HashMap::new(),
            overflow_bytes: 0,
            waiting_bytes: 0,
        }
    }

    fn run_guarded(mut self) {
        let stop = match catch_unwind(AssertUnwindSafe(|| self.run())) {
            Ok(stop) => stop,
            Err(_) => Stop::Lost(ErrorWire::coded(
                "UNKNOWN_ERROR",
                "the session I/O thread panicked",
            )),
        };
        self.finish(stop);
    }

    fn run(&mut self) -> Stop {
        loop {
            if let Some(stop) = self.requested_stop() {
                return stop;
            }
            if let Err(stop) = self.read_turn() {
                return stop;
            }
            self.deliver_uploads();
            self.update_holds();
            if let Err(stop) = self.timers() {
                return stop;
            }
            if let Err(stop) = self.write_turn() {
                return stop;
            }
            // Writing may have drained the control queue below its bound.
            self.update_holds();
            if let Some(stop) = self.goaway_complete() {
                return stop;
            }
            if let Err(stop) = self.wait() {
                return stop;
            }
        }
    }

    // -- reading --------------------------------------------------------

    /// Reading is held (see the module docs).
    fn paused(&self) -> bool {
        self.rx_held || self.tx_held
    }

    fn read_turn(&mut self) -> Result<(), Stop> {
        self.read_more = false;
        let mut bytes = 0usize;
        let mut messages = 0usize;
        while !self.paused() {
            match self.ws.read() {
                Ok(Message::Binary(data)) => {
                    let len = data.len();
                    self.received(len);
                    self.on_frame(data)?;
                    self.update_holds();
                    bytes += len;
                    messages += 1;
                    if bytes >= READ_BURST_BYTES || messages >= READ_BURST_MESSAGES {
                        self.read_more = true;
                        return Ok(());
                    }
                }
                Ok(Message::Text(_)) => {
                    return Err(violation(
                        "PROTOCOL_VIOLATION: text message on a session connection",
                    ));
                }
                Ok(Message::Ping(_) | Message::Pong(_)) => self.received(0),
                Ok(Message::Close(frame)) => return Err(self.remote_closed(frame)),
                Ok(Message::Frame(_)) => {}
                Err(WsError::Io(error)) if would_block(&error) => return Ok(()),
                Err(error) => return Err(self.read_error(error)),
            }
        }
        Ok(())
    }

    fn received(&mut self, bytes: usize) {
        self.last_rx = Instant::now();
        let shared = &self.shared;
        shared.last_rx_ms.store(now_ms(), Ordering::Relaxed);
        shared.bytes_down.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// The reason a lost transport reports: a GOAWAY in effect (sent by
    /// either side) explains it.
    fn lost(&self, code: &str, detail: impl fmt::Display) -> ErrorWire {
        let state = self.shared.lock();
        state
            .goaway_sent
            .clone()
            .or_else(|| state.goaway_received.clone())
            .unwrap_or_else(|| ErrorWire::coded(code, detail))
    }

    fn remote_closed(&self, frame: Option<CloseFrame>) -> Stop {
        let detail = match frame {
            Some(frame) if !frame.reason.is_empty() => {
                format!("the peer closed the connection ({})", frame.reason)
            }
            _ => "the peer closed the connection".to_owned(),
        };
        Stop::Remote(self.lost("WS_CLOSED", detail))
    }

    fn read_error(&self, error: WsError) -> Stop {
        match error {
            WsError::Capacity(error) => violation(format!("PROTOCOL_VIOLATION: {error}")),
            WsError::Utf8(error) => violation(format!("PROTOCOL_VIOLATION: {error}")),
            WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake) => {
                Stop::Lost(self.lost("CONNECTION_LOST", "connection reset by peer"))
            }
            WsError::Protocol(error) => violation(format!("PROTOCOL_VIOLATION: {error}")),
            other => Stop::Lost(self.lost("CONNECTION_LOST", other)),
        }
    }

    fn on_frame(&mut self, message: Bytes) -> Result<(), Stop> {
        let limits = self.shared.receive_limits();
        let frame = decode_ref(&message, limits).map_err(violation)?;
        let side = self.shared.side;
        match frame.kind {
            FrameKind::Hello | FrameKind::HelloAck | FrameKind::Auth | FrameKind::AuthOk => Err(
                violation("PROTOCOL_VIOLATION: handshake frame on an established session"),
            ),
            FrameKind::Request if side == Side::Server => self.on_request(frame),
            FrameKind::Response if side == Side::Client => self.on_response(frame),
            FrameKind::Chunk => {
                // The body is the tail of the message: hand it on without a copy.
                let body = message.slice(message.len() - frame.body.len()..);
                let cost = part_cost(message.len());
                self.on_chunk(frame, body, cost)
            }
            FrameKind::Progress if side == Side::Client => self.on_progress(frame),
            FrameKind::Cancel if side == Side::Server => self.on_cancel(frame),
            FrameKind::Heartbeat => self.on_heartbeat(frame),
            FrameKind::Goaway => self.on_goaway(frame),
            kind => Err(violation(format!(
                "PROTOCOL_VIOLATION: {kind:?} frame sent to the {side:?} side"
            ))),
        }
    }

    fn header<T: serde::de::DeserializeOwned>(frame: &FrameRef<'_>) -> Result<T, Stop> {
        frame.header_as().map_err(violation)
    }

    /// Request ids start at 1; 0 keys this side's own HEARTBEAT and GOAWAY
    /// frames in the outgoing queues, which a peer's id must never match.
    fn reserved_id(what: &str) -> Stop {
        violation(format!("PROTOCOL_VIOLATION: {what} with the reserved id 0"))
    }

    // -- server side ----------------------------------------------------

    fn on_request(&mut self, frame: FrameRef<'_>) -> Result<(), Stop> {
        let request: RequestHeader = Self::header(&frame)?;
        let id = request.id;
        if id == 0 {
            return Err(Self::reserved_id("request"));
        }
        if self.partial.contains_key(&id)
            || self.uploads.contains_key(&id)
            || self.shared.lock().serving.contains_key(&id)
        {
            return Err(violation(format!(
                "PROTOCOL_VIOLATION: request id {id} is already in use"
            )));
        }
        let limit = self.shared.body_limit(&request.op);
        if !frame.more() {
            if request
                .total
                .is_some_and(|total| total != frame.body.len() as u64)
            {
                return Err(violation(format!(
                    "PROTOCOL_VIOLATION: request {id} total disagrees with its body"
                )));
            }
            if frame.body.len() > limit {
                return Err(violation(format!(
                    "PROTOCOL_VIOLATION: request {id} body exceeds {limit} bytes"
                )));
            }
            self.dispatch(request, frame.body.to_vec(), false);
            return Ok(());
        }
        match request.total {
            Some(total) => {
                let total = usize::try_from(total).unwrap_or(usize::MAX);
                if total > limit {
                    return Err(violation(format!(
                        "PROTOCOL_VIOLATION: request {id} body of {total} bytes exceeds {limit}"
                    )));
                }
                if total <= frame.body.len() {
                    return Err(violation(format!(
                        "PROTOCOL_VIOLATION: request {id} announces {total} bytes but needs no continuation"
                    )));
                }
                let cap = self.shared.options.max_body.saturating_mul(4);
                if self.partial_bytes.saturating_add(total) > cap {
                    return Err(violation(format!(
                        "PROTOCOL_VIOLATION: more than {cap} bytes of request bodies in reassembly"
                    )));
                }
                let expires = after(
                    Instant::now(),
                    request
                        .deadline_ms
                        .or(request.idle_ms)
                        .map_or(DEFAULT_UPLOAD_IDLE, Duration::from_millis),
                );
                let mut body = Vec::with_capacity(total);
                body.extend_from_slice(frame.body);
                self.partial_bytes += total;
                self.partial.insert(
                    id,
                    Partial {
                        total,
                        body,
                        expires,
                        kind: PartialKind::Request(request),
                    },
                );
                Ok(())
            }
            None => {
                if !frame.body.is_empty() {
                    return Err(violation(format!(
                        "PROTOCOL_VIOLATION: streamed request {id} carries a body"
                    )));
                }
                self.dispatch(request, Vec::new(), true);
                Ok(())
            }
        }
    }

    /// Hands a complete (or streamed) request to the handler pool, or answers
    /// it here: `ping`, `BUSY` when the queue is full, or a refusal while the
    /// session is going away.
    fn dispatch(&mut self, request: RequestHeader, body: Vec<u8>, streamed: bool) {
        let shared = self.shared.clone();
        let id = request.id;
        if request.op == ops::PING && !streamed {
            let header = serde_json::to_value(PingReply { alive: true }).unwrap_or_default();
            self.answer(id, Ok(header));
            return;
        }
        let refusal = {
            let state = shared.lock();
            if let Some(goaway) = state.goaway_sent.as_ref().or(state.close_request.as_ref()) {
                Some(ErrorWire::coded(
                    "WS_CLOSED",
                    format!("the session is going away ({goaway})"),
                ))
            } else if state.incoming.len()
                >= shared
                    .options
                    .handler_queue
                    .saturating_add(state.idle_handlers())
            {
                // `handler_queue` requests already wait beyond those idle
                // handler threads are about to take (so `handler_queue: 0`
                // still serves a request while a handler is idle).
                Some(
                    ErrorWire::coded(
                        "BUSY",
                        format!(
                            "{} requests are already waiting for a handler",
                            state.incoming.len()
                        ),
                    )
                    .with_retry_after(50),
                )
            } else {
                None
            }
        };
        if let Some(error) = refusal {
            self.answer(id, Err(error));
            return;
        }
        let now = Instant::now();
        let deadline = after(
            now,
            request
                .deadline_ms
                .map_or(DEFAULT_DEADLINE, Duration::from_millis),
        );
        let cancel = Arc::new(AtomicBool::new(false));
        let started = Arc::new(AtomicBool::new(false));
        let chunks = streamed.then(|| {
            let (tx, rx) = sync_channel(UPLOAD_CHANNEL);
            self.uploads.insert(
                id,
                UploadIn {
                    tx: Some(tx),
                    cancel: cancel.clone(),
                    started: started.clone(),
                    running: false,
                    waiting: 0,
                    overflow: VecDeque::new(),
                    ended: false,
                    idle: request
                        .idle_ms
                        .map_or(DEFAULT_UPLOAD_IDLE, Duration::from_millis),
                    active: now,
                },
            );
            rx
        });
        let limit = shared.body_limit(&request.op);
        let incoming = Incoming {
            id,
            op: request.op,
            header: request.header,
            body,
            chunks,
            deadline,
            responder: Responder::new(shared.clone(), id, cancel.clone(), limit),
        };
        {
            let mut state = shared.lock();
            state.serving.insert(
                id,
                Serving {
                    cancel,
                    deadline,
                    started,
                },
            );
            state.incoming.push_back(incoming);
        }
        shared.work.notify_one();
    }

    /// Queues a RESPONSE produced by the I/O thread itself. It cannot wait for
    /// room; the control queue stays bounded because reading stops while the
    /// queue is over its bound (`update_holds`).
    fn answer(&self, id: u64, result: Result<Value, ErrorWire>) {
        let frames = self.shared.response_frames(id, result, &[], false);
        let mut state = self.shared.lock();
        for (lane, frame) in frames {
            state.queues.push(lane, id, frame);
        }
    }

    fn on_cancel(&mut self, frame: FrameRef<'_>) -> Result<(), Stop> {
        let CancelHeader { id } = Self::header(&frame)?;
        if id == 0 {
            return Err(Self::reserved_id("cancel"));
        }
        // Cancel first: a handler that sees its parts channel disconnect must
        // already see `cancelled()`.
        let taken = {
            let mut state = self.shared.lock();
            if let Some(serving) = state.serving.remove(&id) {
                serving.cancel.store(true, Ordering::Release);
            }
            let freed = state.queues.purge(id);
            if freed > 0 && state.space_waiters > 0 {
                self.shared.space.notify_all();
            }
            state.take_incoming(|other| other == id)
        };
        self.drop_partial(id);
        self.drop_upload(id);
        drop(taken);
        Ok(())
    }

    fn drop_partial(&mut self, id: u64) {
        if let Some(partial) = self.partial.remove(&id) {
            self.partial_bytes -= partial.total;
        }
    }

    fn drop_upload(&mut self, id: u64) {
        if let Some(upload) = self.uploads.remove(&id) {
            if upload.running {
                self.overflow_bytes -= upload.overflow_len();
            } else {
                self.waiting_bytes -= upload.waiting;
            }
        }
    }

    /// A handler took streamed request `id`: its undelivered parts now belong
    /// to a running consumer. (The parts already in its channel leave the
    /// count, as for every running stream.)
    fn start_upload(&mut self, id: u64) {
        let Some(upload) = self.uploads.get_mut(&id) else {
            return;
        };
        if upload.running {
            return;
        }
        upload.running = true;
        upload.active = Instant::now();
        self.waiting_bytes -= std::mem::take(&mut upload.waiting);
        self.overflow_bytes += upload.overflow_len();
        if upload.ended && upload.overflow.is_empty() {
            self.uploads.remove(&id);
        }
    }

    /// Notices streamed requests handler threads have taken.
    fn sync_started(&mut self) {
        let started: Vec<u64> = self
            .uploads
            .iter()
            .filter(|(_, upload)| !upload.running && upload.started.load(Ordering::Acquire))
            .map(|(id, _)| *id)
            .collect();
        for id in started {
            self.start_upload(id);
        }
    }

    /// Received stream data not yet taken by its consumers.
    fn rx_total(&self) -> usize {
        self.shared.rx_buffered.load(Ordering::Acquire) + self.overflow_bytes + self.waiting_bytes
    }

    /// Keeps the receive side within `data_queue_bytes` without ever holding
    /// reads for a stream no handler has taken: such a stream may wait for a
    /// handler only while its parts fit. When the total is over the bound,
    /// waiting streams are refused `BUSY` (newest first) and their parts
    /// dropped, until it fits or only streams idle handler threads are about
    /// to take are left. Reads are then held only for consumers that are
    /// running or about to run (`update_holds`), so a hold always ends; were
    /// a waiting stream's parts to hold reads, the running streams whose
    /// handlers it waits for could starve behind the hold and never finish.
    fn make_room(&mut self) {
        let cap = self.shared.rx_cap();
        while self.waiting_bytes > 0 && self.rx_total() > cap {
            let starting: HashSet<u64> = self.shared.lock().starting().collect();
            let newest = self
                .uploads
                .iter()
                .filter(|(id, upload)| {
                    !upload.running && upload.waiting > 0 && !starting.contains(id)
                })
                .map(|(id, _)| *id)
                .max();
            let Some(id) = newest else {
                return;
            };
            if !self.refuse_waiting(id) {
                // A handler took it meanwhile.
                self.start_upload(id);
            }
        }
    }

    /// Refuses waiting streamed request `id` with `BUSY` (nothing ran: no
    /// handler took it) and drops its parts. `false` if a handler took it
    /// meanwhile.
    fn refuse_waiting(&mut self, id: u64) -> bool {
        let shared = self.shared.clone();
        let error = ErrorWire::coded(
            "BUSY",
            format!(
                "no handler is free for streamed request {id}, and its body does not fit the \
                 {} byte receive buffer while it waits",
                shared.rx_cap()
            ),
        )
        .with_retry_after(50);
        let frames = shared.response_frames(id, Err(error), &[], false);
        let taken = {
            let mut state = shared.lock();
            let taken = state.take_incoming(|other| other == id);
            if taken.is_empty() {
                return false;
            }
            if let Some(serving) = state.serving.remove(&id) {
                serving.cancel.store(true, Ordering::Release);
            }
            if state.queues.purge(id) > 0 && state.space_waiters > 0 {
                shared.space.notify_all();
            }
            for (lane, frame) in frames {
                state.queues.push(lane, id, frame);
            }
            taken
        };
        self.drop_upload(id);
        drop(taken);
        true
    }

    // -- client side ----------------------------------------------------

    /// Whether `id` was ever allocated by this (client) side.
    fn allocated(&self, id: u64) -> bool {
        id != 0 && id < self.shared.next_id.load(Ordering::Relaxed)
    }

    fn on_response(&mut self, frame: FrameRef<'_>) -> Result<(), Stop> {
        let response: ResponseHeader = Self::header(&frame)?;
        let id = response.id;
        if !self.allocated(id) {
            return Err(violation(format!(
                "PROTOCOL_VIOLATION: response to request {id}, which was never sent"
            )));
        }
        if self.partial.contains_key(&id) {
            return Err(violation(format!(
                "PROTOCOL_VIOLATION: second response to request {id}"
            )));
        }
        let shared = self.shared.clone();
        let mut state = shared.lock();
        let Some(slot) = state.pending.get(&id).cloned() else {
            // A late reply to an abandoned call. If its body continues, the
            // continuations are dropped on arrival too (`on_chunk`).
            return Ok(());
        };
        if frame.body.len() > slot.limit {
            return Err(violation(format!(
                "PROTOCOL_VIOLATION: response {id} body exceeds {} bytes",
                slot.limit
            )));
        }
        if frame.more() {
            let total = response
                .total
                .map(|total| usize::try_from(total).unwrap_or(usize::MAX));
            let Some(total) = total.filter(|total| *total > frame.body.len()) else {
                return Err(violation(format!(
                    "PROTOCOL_VIOLATION: response {id} continues without a valid total"
                )));
            };
            if !response.ok || total > slot.limit {
                return Err(violation(format!(
                    "PROTOCOL_VIOLATION: response {id} body of {total} bytes is not allowed"
                )));
            }
            drop(state);
            let mut body = Vec::with_capacity(total);
            body.extend_from_slice(frame.body);
            self.partial_bytes += total;
            self.partial.insert(
                id,
                Partial {
                    total,
                    body,
                    expires: after(Instant::now(), NEVER),
                    kind: PartialKind::Response(response.header),
                },
            );
            return Ok(());
        }
        state.pending.remove(&id);
        shared.completed(&mut state, id);
        let result = if response.ok {
            Ok(Reply {
                header: response.header,
                body: frame.body.to_vec(),
            })
        } else {
            Err(response.error.unwrap_or_else(|| {
                ErrorWire::coded("UNKNOWN_ERROR", "error response without an error")
            }))
        };
        slot.push(Event::Done(result));
        Ok(())
    }

    fn on_progress(&mut self, frame: FrameRef<'_>) -> Result<(), Stop> {
        let ProgressHeader { id, .. } = Self::header(&frame)?;
        if !self.allocated(id) {
            return Err(violation(format!(
                "PROTOCOL_VIOLATION: progress for request {id}, which was never sent"
            )));
        }
        let state = self.shared.lock();
        if let Some(slot) = state.pending.get(&id) {
            slot.push(Event::Progress);
        }
        Ok(())
    }

    // -- chunks (both sides) --------------------------------------------

    /// A CHUNK frame; `cost` is its charge to the receive bound if it is
    /// buffered as a stream part ([`part_cost`]).
    fn on_chunk(&mut self, frame: FrameRef<'_>, body: Bytes, cost: usize) -> Result<(), Stop> {
        let header: ChunkHeader = Self::header(&frame)?;
        let id = header.id;
        if id == 0 {
            return Err(Self::reserved_id("chunk"));
        }
        if header.last != frame.last() {
            return Err(violation(format!(
                "PROTOCOL_VIOLATION: chunk of request {id} disagrees with its LAST flag"
            )));
        }
        if self.partial.contains_key(&id) {
            return self.on_continuation(header, frame);
        }
        match self.shared.side {
            Side::Server => {
                if self.uploads.contains_key(&id) {
                    self.on_upload_chunk(header, frame, cost)
                } else {
                    Ok(()) // a cancelled or answered request
                }
            }
            Side::Client => {
                if !self.allocated(id) {
                    return Err(violation(format!(
                        "PROTOCOL_VIOLATION: chunk for request {id}, which was never sent"
                    )));
                }
                let state = self.shared.lock();
                let Some(slot) = state.pending.get(&id) else {
                    // A late frame of a call its caller gave up on (a stream
                    // chunk, or a body continuation of a reply that crossed
                    // the CANCEL or whose reassembly was dropped): ids are
                    // never reused, so it is discarded whatever its flags.
                    return Ok(());
                };
                if !frame.more() {
                    return Err(violation(format!(
                        "PROTOCOL_VIOLATION: stream chunk of request {id} without MORE"
                    )));
                }
                // Charged at its cost, header included (a header-only chunk
                // still holds its message), and uncharged by the same amount
                // when the caller takes it or the call ends.
                self.shared.rx_buffered.fetch_add(cost, Ordering::AcqRel);
                slot.push(Event::Chunk(header, body, cost));
                Ok(())
            }
        }
    }

    /// Appends one body continuation; completes the request or response.
    fn on_continuation(&mut self, header: ChunkHeader, frame: FrameRef<'_>) -> Result<(), Stop> {
        let id = header.id;
        if matches!(
            self.partial.get(&id).map(|partial| &partial.kind),
            Some(PartialKind::Response(_))
        ) && !self.shared.lock().pending.contains_key(&id)
        {
            // The caller gave up on this reply: free its reassembly now. The
            // rest of the body is dropped on arrival (`on_chunk`).
            self.drop_partial(id);
            return Ok(());
        }
        let Some(partial) = self.partial.get_mut(&id) else {
            return Ok(());
        };
        let end = partial.body.len() + frame.body.len();
        let complete = end == partial.total;
        let valid = header.hash.is_none()
            && header.offset == partial.body.len() as u64
            && header.total == partial.total as u64
            && end <= partial.total
            && (complete || !frame.body.is_empty())
            && complete == header.last
            && complete != frame.more();
        if !valid {
            return Err(violation(format!(
                "PROTOCOL_VIOLATION: invalid body continuation for request {id}"
            )));
        }
        partial.body.extend_from_slice(frame.body);
        if !complete {
            return Ok(());
        }
        let partial = self.partial.remove(&id).expect("partial body");
        self.partial_bytes -= partial.total;
        match partial.kind {
            PartialKind::Request(request) => {
                if self.shared.side == Side::Server {
                    self.dispatch(request, partial.body, false);
                }
            }
            PartialKind::Response(header) => {
                let shared = self.shared.clone();
                let mut state = shared.lock();
                if let Some(slot) = state.pending.remove(&id) {
                    shared.completed(&mut state, id);
                    slot.push(Event::Done(Ok(Reply {
                        header,
                        body: partial.body,
                    })));
                }
            }
        }
        Ok(())
    }

    fn on_upload_chunk(
        &mut self,
        header: ChunkHeader,
        frame: FrameRef<'_>,
        cost: usize,
    ) -> Result<(), Stop> {
        let id = header.id;
        if self
            .uploads
            .get(&id)
            .is_some_and(|upload| !upload.running && upload.started.load(Ordering::Acquire))
        {
            self.start_upload(id);
        }
        let Some(upload) = self.uploads.get_mut(&id) else {
            return Ok(());
        };
        if upload.ended {
            return Err(violation(format!(
                "PROTOCOL_VIOLATION: chunk after the end of streamed request {id}"
            )));
        }
        upload.active = Instant::now();
        if !frame.more() {
            if !frame.body.is_empty() {
                return Err(violation(format!(
                    "PROTOCOL_VIOLATION: the end of streamed request {id} carries data"
                )));
            }
            upload.ended = true;
            if upload.overflow.is_empty() {
                // Disconnects the handler's channel after its parts. A
                // waiting stream stays listed: its parts still count.
                upload.tx = None;
                if upload.running {
                    self.uploads.remove(&id);
                }
            }
            return Ok(());
        }
        if upload.cancel.load(Ordering::Acquire) {
            self.drop_upload(id);
            return Ok(());
        }
        let body = frame.body.to_vec();
        // Parts stay in order: while some wait, a new one waits behind them.
        let unsent = if !upload.overflow.is_empty() {
            Some(Part { header, body, cost })
        } else {
            match upload.tx.as_ref().map(|tx| tx.try_send((header, body))) {
                Some(Ok(())) => None,
                Some(Err(TrySendError::Full((header, body)))) => Some(Part { header, body, cost }),
                // The handler stopped reading parts: drop the rest of the
                // body.
                Some(Err(TrySendError::Disconnected(_))) | None => {
                    self.drop_upload(id);
                    return Ok(());
                }
            }
        };
        if upload.running {
            if let Some(part) = unsent {
                self.overflow_bytes += part.cost;
                upload.overflow.push_back(part);
            }
        } else {
            // No handler yet: everything it buffers counts, channel included.
            if let Some(part) = unsent {
                upload.overflow.push_back(part);
            }
            upload.waiting += cost;
            self.waiting_bytes += cost;
        }
        self.make_room();
        Ok(())
    }

    /// Some running handler leaves parts untaken (they wait in its stream's
    /// overflow). Delivery is driven by this, not by `overflow_bytes`.
    fn any_behind(&self) -> bool {
        self.uploads.values().any(UploadIn::behind)
    }

    /// Moves buffered parts into the channels of running handlers that have
    /// room again.
    fn deliver_uploads(&mut self) {
        self.sync_started();
        if !self.any_behind() {
            return;
        }
        let mut finished = Vec::new();
        let mut abandoned = Vec::new();
        let now = Instant::now();
        for (id, upload) in &mut self.uploads {
            if !upload.running {
                continue;
            }
            if upload.cancel.load(Ordering::Acquire) {
                abandoned.push(*id);
                continue;
            }
            let Some(tx) = upload.tx.as_ref() else {
                continue;
            };
            while let Some(Part { header, body, cost }) = upload.overflow.pop_front() {
                match tx.try_send((header, body)) {
                    Ok(()) => {
                        // The handler took a part: it is making progress.
                        self.overflow_bytes -= cost;
                        upload.active = now;
                    }
                    Err(TrySendError::Full((header, body))) => {
                        upload.overflow.push_front(Part { header, body, cost });
                        break;
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        self.overflow_bytes -= cost;
                        let rest = upload
                            .overflow
                            .drain(..)
                            .map(|part| part.cost)
                            .sum::<usize>();
                        self.overflow_bytes -= rest;
                    }
                }
            }
            if upload.ended && upload.overflow.is_empty() {
                finished.push(*id);
            }
        }
        for id in finished {
            self.uploads.remove(&id);
        }
        for id in abandoned {
            self.drop_upload(id);
        }
        if self.any_behind() {
            // A handler that answered stops reading its parts: drop them.
            let answered: Vec<u64> = {
                let state = self.shared.lock();
                self.uploads
                    .iter()
                    .filter(|(id, upload)| {
                        upload.running
                            && !upload.overflow.is_empty()
                            && !state.serving.contains_key(id)
                    })
                    .map(|(id, _)| *id)
                    .collect()
            };
            for id in answered {
                self.drop_upload(id);
            }
        }
    }

    /// Holds or resumes reading (see the module docs), with hysteresis: a
    /// hold starts above a bound and ends at half of it. Over the receive
    /// bound, `make_room` has already refused every waiting stream that no
    /// idle handler is about to take, so what remains belongs to consumers
    /// that run (or are about to) and the hold ends as they catch up.
    fn update_holds(&mut self) {
        let was = self.paused();
        let shared = self.shared.clone();
        let buffered = self.rx_total();
        let rx_cap = shared.rx_cap();
        if !self.rx_held && buffered > rx_cap {
            self.rx_held = true;
        } else if self.rx_held && buffered <= rx_cap / 2 {
            self.rx_held = false;
        }
        // Only a server answers what it reads; a client's control queue is
        // bounded where requests are submitted.
        if shared.side == Side::Server {
            let queued = shared.lock().queues.control_bytes();
            let tx_cap = shared.tx_cap();
            if !self.tx_held && queued > tx_cap {
                self.tx_held = true;
                // The peer has `dead_after` from now to take bytes again.
                self.last_write_progress = Instant::now();
            } else if self.tx_held && queued <= tx_cap / 2 {
                self.tx_held = false;
            }
        }
        let paused = self.paused();
        if paused == was {
            return;
        }
        shared.rx_paused.store(paused, Ordering::Release);
        if !paused {
            // Silence while reading was held was ours, not the peer's; so was
            // the stall of streamed bodies whose parts waited behind the hold.
            let now = Instant::now();
            self.last_rx = now;
            self.read_more = true;
            for upload in self.uploads.values_mut() {
                upload.active = now;
            }
        }
    }

    // -- heartbeats and goaway ------------------------------------------

    fn on_heartbeat(&mut self, frame: FrameRef<'_>) -> Result<(), Stop> {
        let heartbeat: Heartbeat = Self::header(&frame)?;
        self.peer_heartbeat = Some((heartbeat.seq, heartbeat.at_ms, Instant::now()));
        if let Some(echo) = heartbeat.echo
            && echo.seq >= 1
            && echo.seq <= self.heartbeat_seq
        {
            let rtt = now_ms()
                .saturating_sub(echo.at_ms)
                .saturating_sub(echo.held_ms);
            self.shared.rtt_ms.store(rtt, Ordering::Relaxed);
        }
        Ok(())
    }

    fn on_goaway(&mut self, frame: FrameRef<'_>) -> Result<(), Stop> {
        let goaway: Goaway = Self::header(&frame)?;
        let mut state = self.shared.lock();
        if state.goaway_received.is_none() {
            state.goaway_received = Some(ErrorWire::new(goaway.code, goaway.message));
            let deadline = after(Instant::now(), protocol::frame::GOAWAY_GRACE);
            state.goaway_deadline =
                Some(state.goaway_deadline.map_or(deadline, |d| d.min(deadline)));
        }
        Ok(())
    }

    fn queue_heartbeat(&mut self, now: Instant) {
        self.heartbeat_seq += 1;
        self.last_heartbeat = now;
        let heartbeat = Heartbeat {
            seq: self.heartbeat_seq,
            at_ms: now_ms(),
            echo: self
                .peer_heartbeat
                .map(|(seq, at_ms, received)| HeartbeatEcho {
                    seq,
                    at_ms,
                    held_ms: ms(received.elapsed()),
                }),
        };
        if let Ok(frame) = encode_with(
            FrameKind::Heartbeat,
            0,
            &heartbeat,
            &[],
            self.shared.send_limits(),
        ) {
            self.shared.lock().queues.push(Lane::Control, 0, frame);
            self.heartbeat_queued = true;
        }
    }

    fn requested_stop(&self) -> Option<Stop> {
        let state = self.shared.lock();
        state.close_request.clone().map(Stop::Graceful)
    }

    fn timers(&mut self) -> Result<(), Stop> {
        let now = Instant::now();
        let options = self.shared.options;
        if self.tx_held {
            // Reading stopped because the peer leaves our replies unread; it
            // is dead once it has taken nothing from us for `dead_after`.
            let silent = now.duration_since(self.last_write_progress);
            if silent >= options.dead_after {
                return Err(Stop::Lost(ErrorWire::coded(
                    "TRANSPORT_DEAD",
                    format!("the peer read nothing for {} ms", ms(silent)),
                )));
            }
        } else if !self.rx_held && now.duration_since(self.last_rx) >= options.dead_after {
            return Err(Stop::Lost(ErrorWire::coded(
                "TRANSPORT_DEAD",
                format!(
                    "no frame received for {} ms",
                    ms(now.duration_since(self.last_rx))
                ),
            )));
        }
        if !self.heartbeat_queued
            && now.duration_since(self.last_tx.max(self.last_heartbeat)) >= options.heartbeat
        {
            self.queue_heartbeat(now);
        }
        if now >= self.next_scan {
            self.scan(now);
            self.next_scan = now + protocol::frame::DEADLINE_SCAN;
        }
        let state = self.shared.lock();
        if let Some(deadline) = state.goaway_deadline
            && now >= deadline
        {
            let reason = state
                .goaway_sent
                .clone()
                .or_else(|| state.goaway_received.clone())
                .unwrap_or_else(|| ErrorWire::coded("WS_CLOSED", "the session went away"));
            return Err(Stop::Graceful(reason));
        }
        Ok(())
    }

    /// The 250 ms deadline scan.
    fn scan(&mut self, now: Instant) {
        // Bodies that never completed.
        let expired: Vec<u64> = self
            .partial
            .iter()
            .filter(|(_, partial)| partial.expires <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            self.drop_partial(id);
        }
        let shared = self.shared.clone();
        match shared.side {
            Side::Client => {
                // Reassemblies of calls their callers abandoned.
                let stale: Vec<u64> = {
                    let state = shared.lock();
                    self.partial
                        .keys()
                        .filter(|id| !state.pending.contains_key(id))
                        .copied()
                        .collect()
                };
                for id in stale {
                    self.drop_partial(id);
                }
            }
            Side::Server => {
                self.sync_started();
                // Streamed requests that stalled: answer and cancel. A body
                // stalls when no part arrives for its idle limit; while reading
                // is held, parts may wait unread in TCP, so only a request
                // whose running handler left parts untaken for that long (its
                // own consumer is behind) is judged then. A stream waiting for
                // a handler is not judged once its body ended (it is a queued
                // request; `make_room` refuses it if its parts are in the way).
                let held = self.paused();
                let stalled: Vec<(u64, Duration, bool)> = self
                    .uploads
                    .iter()
                    .filter(|(_, upload)| {
                        (upload.behind() || (!held && !upload.ended))
                            && now.duration_since(upload.active) >= upload.idle
                    })
                    .map(|(id, upload)| (*id, upload.idle, upload.behind()))
                    .collect();
                for (id, idle, behind) in stalled {
                    let detail = if behind {
                        format!(
                            "the handler took no part of the streamed body for {} ms",
                            ms(idle)
                        )
                    } else {
                        format!("no part of the streamed body for {} ms", ms(idle))
                    };
                    let frames = shared.response_frames(
                        id,
                        Err(ErrorWire::coded("REQUEST_TIMEOUT", detail)),
                        &[],
                        false,
                    );
                    let taken = {
                        let mut state = shared.lock();
                        if let Some(serving) = state.serving.remove(&id) {
                            serving.cancel.store(true, Ordering::Release);
                            if state.queues.purge(id) > 0 && state.space_waiters > 0 {
                                shared.space.notify_all();
                            }
                            for (lane, frame) in frames {
                                state.queues.push(lane, id, frame);
                            }
                        }
                        state.take_incoming(|other| other == id)
                    };
                    self.drop_upload(id);
                    drop(taken);
                }
                // Requests past their deadline: the client gave up on them.
                let taken = {
                    let mut state = shared.lock();
                    let expired: Vec<u64> = state
                        .serving
                        .iter()
                        .filter(|(_, serving)| serving.deadline <= now)
                        .map(|(id, _)| *id)
                        .collect();
                    let mut freed = 0;
                    for id in &expired {
                        if let Some(serving) = state.serving.remove(id) {
                            serving.cancel.store(true, Ordering::Release);
                        }
                        freed += state.queues.purge(*id);
                    }
                    if freed > 0 && state.space_waiters > 0 {
                        shared.space.notify_all();
                    }
                    state.take_incoming(|id| expired.contains(&id))
                };
                drop(taken);
                // Bodies of requests no longer served (answered, expired or
                // cancelled): their remaining parts are dropped on arrival.
                let ended: Vec<u64> = {
                    let state = shared.lock();
                    self.uploads
                        .iter()
                        .filter(|(id, upload)| {
                            upload.cancel.load(Ordering::Acquire) || !state.serving.contains_key(id)
                        })
                        .map(|(id, _)| *id)
                        .collect()
                };
                for id in ended {
                    self.drop_upload(id);
                }
                if self.rx_held {
                    self.progress_held_streams();
                }
            }
        }
    }

    /// While this server holds its reads for slow consumers, the parts of
    /// other streamed requests may wait unread in TCP, and their clients'
    /// `Upload` idle windows would run although nothing is wrong with them.
    /// Each scan queues one PROGRESS (at most one waits per request) for every
    /// streamed request still receiving its body whose own handler is not
    /// behind: the client restarts its idle window. A request whose handler is
    /// behind gets none; it is judged here after its idle limit.
    fn progress_held_streams(&mut self) {
        let victims: Vec<u64> = self
            .uploads
            .iter()
            .filter(|(_, upload)| !upload.ended && !upload.behind())
            .map(|(id, _)| *id)
            .collect();
        if victims.is_empty() {
            return;
        }
        let limits = self.shared.send_limits();
        let mut state = self.shared.lock();
        for id in victims {
            if !state.serving.contains_key(&id) {
                continue;
            }
            let progress = ProgressHeader { id, bytes: None };
            if let Ok(frame) = encode_with(FrameKind::Progress, 0, &progress, &[], limits) {
                state.queues.push_progress(id, frame);
            }
        }
    }

    /// Closes gracefully once a GOAWAY is in effect and nothing is in flight.
    fn goaway_complete(&self) -> Option<Stop> {
        if self.unflushed > 0
            || self.write_blocked
            || !self.partial.is_empty()
            || !self.uploads.is_empty()
        {
            return None;
        }
        let state = self.shared.lock();
        let reason = state
            .goaway_sent
            .clone()
            .or_else(|| state.goaway_received.clone())?;
        let idle = state.pending.is_empty()
            && state.serving.is_empty()
            && state.incoming.is_empty()
            && state.queues.is_empty();
        idle.then_some(Stop::Graceful(reason))
    }

    // -- writing ----------------------------------------------------------

    fn sent(&mut self, bytes: usize) {
        self.unflushed += bytes;
        self.last_tx = Instant::now();
        let shared = &self.shared;
        shared.last_tx_ms.store(now_ms(), Ordering::Relaxed);
        shared.bytes_up.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn write_turn(&mut self) -> Result<(), Stop> {
        self.write_more = false;
        let mut written = 0usize;
        loop {
            // While the socket is blocked, frames stay in our queues (where
            // they keep their priority and can be purged) until a flush gets
            // through.
            while !self.write_blocked && self.unflushed < protocol::frame::WRITE_HIGH_WATER {
                let next = {
                    let mut state = self.shared.lock();
                    let next = state.queues.pop();
                    // Producers and handler replies may wait for room in any
                    // queue.
                    if next.is_some() && state.space_waiters > 0 {
                        self.shared.space.notify_all();
                    }
                    next
                };
                let Some(frame) = next else {
                    break;
                };
                if frame.first() == Some(&FrameKind::Heartbeat.as_u8()) {
                    self.heartbeat_queued = false;
                }
                let len = frame.len();
                written += len;
                match self.ws.write(Message::Binary(Bytes::from(frame))) {
                    Ok(()) => self.sent(len),
                    Err(WsError::Io(error)) if would_block(&error) => {
                        // The frame is in tungstenite's buffer.
                        self.sent(len);
                        self.write_blocked = true;
                        break;
                    }
                    Err(error) => return Err(self.write_error(error)),
                }
            }
            match self.ws.flush() {
                Ok(()) => {
                    if self.unflushed > 0 || self.write_blocked {
                        self.last_write_progress = Instant::now();
                    }
                    self.unflushed = 0;
                    self.write_blocked = false;
                }
                Err(WsError::Io(error)) if would_block(&error) => {
                    self.write_blocked = true;
                    return Ok(());
                }
                Err(error) => return Err(self.write_error(error)),
            }
            if self.shared.lock().queues.is_empty() {
                return Ok(());
            }
            if written >= WRITE_BURST_BYTES {
                self.write_more = true;
                return Ok(());
            }
        }
    }

    fn write_error(&self, error: WsError) -> Stop {
        match error {
            WsError::ConnectionClosed | WsError::AlreadyClosed => {
                Stop::Lost(self.lost("WS_CLOSED", "the connection closed"))
            }
            other => Stop::Lost(self.lost("CONNECTION_LOST", other)),
        }
    }

    // -- waiting ----------------------------------------------------------

    fn next_wake(&self, now: Instant) -> Instant {
        let options = self.shared.options;
        let mut wake = self.next_scan;
        if !self.heartbeat_queued {
            wake = wake.min(after(
                self.last_tx.max(self.last_heartbeat),
                options.heartbeat,
            ));
        }
        if self.tx_held {
            wake = wake.min(after(self.last_write_progress, options.dead_after));
        } else if !self.rx_held {
            wake = wake.min(after(self.last_rx, options.dead_after));
        }
        // Consumers taking buffered parts do not notify this thread. (A hold
        // for outgoing pressure ends through our own writes.)
        if self.rx_held || self.any_behind() {
            wake = wake.min(now + RETRY);
        }
        if let Some(deadline) = self.shared.lock().goaway_deadline {
            wake = wake.min(deadline);
        }
        wake
    }

    fn wait(&mut self) -> Result<(), Stop> {
        let desired = (!self.paused(), self.write_blocked);
        if self.armed != Some(desired) {
            let interest = Readiness::new(KEY, desired.0, desired.1);
            let armed =
                tcp(&self.ws).and_then(|stream| self.shared.poller.modify(stream, interest));
            if let Err(error) = armed {
                return Err(Stop::Lost(self.lost("CONNECTION_LOST", error)));
            }
            self.armed = Some(desired);
        }
        let now = Instant::now();
        let timeout = if self.read_more || self.write_more {
            Duration::ZERO
        } else {
            self.next_wake(now).saturating_duration_since(now)
        };
        self.events.clear();
        if let Err(error) = self.shared.poller.wait(&mut self.events, Some(timeout))
            && error.kind() != io::ErrorKind::Interrupted
        {
            return Err(Stop::Lost(self.lost("CONNECTION_LOST", error)));
        }
        let (mut readable, mut writable) = (false, false);
        for event in self.events.iter().filter(|event| event.key == KEY) {
            readable |= event.readable;
            writable |= event.writable;
        }
        if readable || writable {
            self.armed = None;
        }
        let now = Instant::now();
        if readable {
            // Bytes arrived (every read turn drains to `WouldBlock` before
            // waiting): the peer is alive even when they only continue a
            // large message on a slow link.
            self.last_rx = now;
        }
        if writable {
            // The socket took bytes off our send buffer: the peer is reading.
            self.last_write_progress = now;
        }
        Ok(())
    }

    // -- closing ----------------------------------------------------------

    fn finish(&mut self, stop: Stop) {
        let shared = self.shared.clone();
        let reason = stop.reason().clone();
        let taken = {
            let mut state = shared.lock();
            let reason = state.closed.get_or_insert(reason).clone();
            for (_, slot) in state.pending.drain() {
                slot.push(Event::Lost(reason.to_string()));
            }
            for (_, serving) in state.serving.drain() {
                serving.cancel.store(true, Ordering::Release);
            }
            state.queues.clear();
            state.take_incoming(|_| true)
        };
        shared.space.notify_all();
        shared.work.notify_all();
        drop(taken);
        self.uploads.clear();
        self.partial.clear();
        self.overflow_bytes = 0;
        self.waiting_bytes = 0;
        self.partial_bytes = 0;
        shared.rx_paused.store(false, Ordering::Release);
        match &stop {
            Stop::Graceful(_) => {
                let _ = self.ws.close(None);
                self.linger(true);
            }
            Stop::Remote(_) => self.linger(false),
            Stop::Violation(reason) => {
                let goaway = Goaway {
                    code: reason.code.clone(),
                    message: reason.message.clone(),
                };
                if let Ok(frame) =
                    encode_with(FrameKind::Goaway, 0, &goaway, &[], shared.send_limits())
                {
                    let _ = self.ws.write(Message::Binary(Bytes::from(frame)));
                }
                let _ = self.ws.close(Some(CloseFrame {
                    code: CloseCode::Protocol,
                    reason: "".into(),
                }));
                self.linger(false);
            }
            Stop::Lost(_) => {}
        }
        if let Ok(stream) = tcp(&self.ws) {
            let _ = stream.shutdown(Shutdown::Both);
        }
        shared.lock().finished = true;
        shared.finished.notify_all();
    }

    /// Flushes what tungstenite holds (and, with `read`, reads until the peer
    /// completes the close handshake) for at most [`LINGER`].
    fn linger(&mut self, read: bool) {
        let until = Instant::now() + LINGER;
        loop {
            let flushed = match self.ws.flush() {
                Ok(()) => true,
                Err(WsError::Io(error)) if would_block(&error) => false,
                Err(_) => return,
            };
            if flushed && !read {
                return;
            }
            if read {
                loop {
                    match self.ws.read() {
                        Ok(_) => continue,
                        Err(WsError::Io(error)) if would_block(&error) => break,
                        Err(_) => return,
                    }
                }
            }
            let now = Instant::now();
            if now >= until {
                return;
            }
            let interest = Readiness::new(KEY, read, !flushed);
            let armed =
                tcp(&self.ws).and_then(|stream| self.shared.poller.modify(stream, interest));
            if armed.is_err() {
                return;
            }
            self.events.clear();
            let wait = (until - now).min(Duration::from_millis(50));
            if self
                .shared
                .poller
                .wait(&mut self.events, Some(wait))
                .is_err()
            {
                return;
            }
        }
    }
}
