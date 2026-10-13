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
//! 3. runs the timers: dead after `dead_after` without a received frame,
//!    HEARTBEAT after `heartbeat` of send idleness, the 250 ms scan of request
//!    deadlines, GOAWAY grace;
//! 4. writes from the priority queues while tungstenite's buffer holds less
//!    than 256 KiB, then flushes (`WouldBlock` arms writable interest);
//! 5. closes gracefully once a GOAWAY is in effect and nothing is in flight;
//! 6. waits for readiness, a producer's `notify()`, or the next timer.
//!
//! Receive-side flow control: when more than `data_queue_bytes` of received
//! stream data waits for its consumers, the thread stops reading (TCP then
//! pushes back on the sender) until they catch up to half of that. Liveness is
//! not judged while reading is paused.
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
    collections::{HashMap, VecDeque},
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
/// reads, writes and timers interleave while the socket never blocks.
const READ_BURST_BYTES: usize = 4 * 1024 * 1024;
const READ_BURST_MESSAGES: usize = 1024;
const WRITE_BURST_BYTES: usize = 4 * 1024 * 1024;
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

/// A streamed request body being delivered to its handler.
struct UploadIn {
    tx: SyncSender<(ChunkHeader, Vec<u8>)>,
    cancel: Arc<AtomicBool>,
    overflow: VecDeque<(ChunkHeader, Vec<u8>)>,
    ended: bool,
    idle: Duration,
    active: Instant,
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
    paused: bool,
    last_rx: Instant,
    last_tx: Instant,
    last_heartbeat: Instant,
    heartbeat_seq: u64,
    /// The peer's last heartbeat `(seq, at_ms, received)`, echoed back.
    peer_heartbeat: Option<(u64, u64, Instant)>,
    next_scan: Instant,
    partial: HashMap<u64, Partial>,
    partial_bytes: usize,
    uploads: HashMap<u64, UploadIn>,
    overflow_bytes: usize,
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
            paused: false,
            last_rx: now,
            last_tx: now,
            last_heartbeat: now,
            heartbeat_seq: 0,
            peer_heartbeat: None,
            next_scan: now + protocol::frame::DEADLINE_SCAN,
            partial: HashMap::new(),
            partial_bytes: 0,
            uploads: HashMap::new(),
            overflow_bytes: 0,
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
            self.update_pause();
            if let Err(stop) = self.timers() {
                return stop;
            }
            if let Err(stop) = self.write_turn() {
                return stop;
            }
            if let Some(stop) = self.goaway_complete() {
                return stop;
            }
            if let Err(stop) = self.wait() {
                return stop;
            }
        }
    }

    // -- reading --------------------------------------------------------

    fn read_turn(&mut self) -> Result<(), Stop> {
        self.read_more = false;
        let mut bytes = 0usize;
        let mut messages = 0usize;
        while !self.paused {
            match self.ws.read() {
                Ok(Message::Binary(data)) => {
                    let len = data.len();
                    self.received(len);
                    self.on_frame(data)?;
                    self.update_pause();
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
                self.on_chunk(frame, body)
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

    // -- server side ----------------------------------------------------

    fn on_request(&mut self, frame: FrameRef<'_>) -> Result<(), Stop> {
        let request: RequestHeader = Self::header(&frame)?;
        let id = request.id;
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
            } else if state.incoming.len() >= shared.options.handler_queue {
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
        let chunks = streamed.then(|| {
            let (tx, rx) = sync_channel(UPLOAD_CHANNEL);
            self.uploads.insert(
                id,
                UploadIn {
                    tx,
                    cancel: cancel.clone(),
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
            state.serving.insert(id, Serving { cancel, deadline });
            state.incoming.push_back(incoming);
        }
        shared.work.notify_one();
    }

    /// Queues a RESPONSE produced by the I/O thread itself.
    fn answer(&self, id: u64, result: Result<Value, ErrorWire>) {
        let frames = self.shared.response_frames(id, result, &[], false);
        let mut state = self.shared.lock();
        for (lane, frame) in frames {
            state.queues.push(lane, id, frame);
        }
    }

    fn on_cancel(&mut self, frame: FrameRef<'_>) -> Result<(), Stop> {
        let CancelHeader { id } = Self::header(&frame)?;
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
            self.overflow_bytes -= upload
                .overflow
                .iter()
                .map(|(_, bytes)| bytes.len())
                .sum::<usize>();
        }
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
            return Ok(()); // a late reply to an abandoned call
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

    fn on_chunk(&mut self, frame: FrameRef<'_>, body: Bytes) -> Result<(), Stop> {
        let header: ChunkHeader = Self::header(&frame)?;
        let id = header.id;
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
                    self.on_upload_chunk(header, frame)
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
                if !frame.more() {
                    return Err(violation(format!(
                        "PROTOCOL_VIOLATION: stream chunk of request {id} without MORE"
                    )));
                }
                let state = self.shared.lock();
                if let Some(slot) = state.pending.get(&id) {
                    self.shared
                        .rx_buffered
                        .fetch_add(frame.body.len(), Ordering::AcqRel);
                    slot.push(Event::Chunk(header, body));
                }
                Ok(())
            }
        }
    }

    /// Appends one body continuation; completes the request or response.
    fn on_continuation(&mut self, header: ChunkHeader, frame: FrameRef<'_>) -> Result<(), Stop> {
        let id = header.id;
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
                let mut state = self.shared.lock();
                if let Some(slot) = state.pending.remove(&id) {
                    slot.push(Event::Done(Ok(Reply {
                        header,
                        body: partial.body,
                    })));
                }
            }
        }
        Ok(())
    }

    fn on_upload_chunk(&mut self, header: ChunkHeader, frame: FrameRef<'_>) -> Result<(), Stop> {
        let id = header.id;
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
                self.uploads.remove(&id); // disconnects the handler's channel
            }
            return Ok(());
        }
        if upload.cancel.load(Ordering::Acquire) {
            self.drop_upload(id);
            return Ok(());
        }
        let item = (header, frame.body.to_vec());
        if !upload.overflow.is_empty() {
            self.overflow_bytes += item.1.len();
            upload.overflow.push_back(item);
            return Ok(());
        }
        match upload.tx.try_send(item) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(item)) => {
                self.overflow_bytes += item.1.len();
                upload.overflow.push_back(item);
                Ok(())
            }
            // The handler stopped reading parts: drop the rest of the body.
            Err(TrySendError::Disconnected(_)) => {
                self.drop_upload(id);
                Ok(())
            }
        }
    }

    /// Moves buffered parts into handler channels that have room again.
    fn deliver_uploads(&mut self) {
        if self.overflow_bytes == 0 {
            return;
        }
        let mut finished = Vec::new();
        let mut abandoned = Vec::new();
        for (id, upload) in &mut self.uploads {
            if upload.cancel.load(Ordering::Acquire) {
                abandoned.push(*id);
                continue;
            }
            while let Some(item) = upload.overflow.pop_front() {
                let len = item.1.len();
                match upload.tx.try_send(item) {
                    Ok(()) => self.overflow_bytes -= len,
                    Err(TrySendError::Full(item)) => {
                        upload.overflow.push_front(item);
                        break;
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        self.overflow_bytes -= len;
                        let rest = upload
                            .overflow
                            .drain(..)
                            .map(|(_, bytes)| bytes.len())
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
        if self.overflow_bytes > 0 {
            // A handler that answered stops reading its parts: drop them.
            let answered: Vec<u64> = {
                let state = self.shared.lock();
                self.uploads
                    .iter()
                    .filter(|(id, upload)| {
                        !upload.overflow.is_empty() && !state.serving.contains_key(id)
                    })
                    .map(|(id, _)| *id)
                    .collect()
            };
            for id in answered {
                self.drop_upload(id);
            }
        }
    }

    fn update_pause(&mut self) {
        let shared = &self.shared;
        let buffered = shared.rx_buffered.load(Ordering::Acquire) + self.overflow_bytes;
        let cap = shared.rx_cap();
        if !self.paused && buffered > cap {
            self.paused = true;
            shared.rx_paused.store(true, Ordering::Release);
        } else if self.paused && buffered <= cap / 2 {
            self.paused = false;
            shared.rx_paused.store(false, Ordering::Release);
            // Silence while paused was ours, not the peer's.
            self.last_rx = Instant::now();
            self.read_more = true;
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
        }
    }

    fn requested_stop(&self) -> Option<Stop> {
        let state = self.shared.lock();
        state.close_request.clone().map(Stop::Graceful)
    }

    fn timers(&mut self) -> Result<(), Stop> {
        let now = Instant::now();
        let options = self.shared.options;
        if !self.paused && now.duration_since(self.last_rx) >= options.dead_after {
            return Err(Stop::Lost(ErrorWire::coded(
                "TRANSPORT_DEAD",
                format!(
                    "no frame received for {} ms",
                    ms(now.duration_since(self.last_rx))
                ),
            )));
        }
        if now.duration_since(self.last_tx.max(self.last_heartbeat)) >= options.heartbeat {
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
                // Streamed requests whose body stalled: answer and cancel.
                let stalled: Vec<(u64, Duration)> = self
                    .uploads
                    .iter()
                    .filter(|(_, upload)| {
                        !upload.ended && now.duration_since(upload.active) >= upload.idle
                    })
                    .map(|(id, upload)| (*id, upload.idle))
                    .collect();
                for (id, idle) in stalled {
                    let frames = shared.response_frames(
                        id,
                        Err(ErrorWire::coded(
                            "REQUEST_TIMEOUT",
                            format!("no part of the streamed body for {} ms", ms(idle)),
                        )),
                        &[],
                        false,
                    );
                    let taken = {
                        let mut state = shared.lock();
                        if let Some(serving) = state.serving.remove(&id) {
                            serving.cancel.store(true, Ordering::Release);
                            state.queues.purge(id);
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
            while self.unflushed < protocol::frame::WRITE_HIGH_WATER {
                let next = {
                    let mut state = self.shared.lock();
                    let next = state.queues.pop();
                    if matches!(next, Some((_, true))) && state.space_waiters > 0 {
                        self.shared.space.notify_all();
                    }
                    next
                };
                let Some((frame, _)) = next else {
                    break;
                };
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
        wake = wake.min(self.last_tx.max(self.last_heartbeat) + options.heartbeat);
        if !self.paused {
            wake = wake.min(self.last_rx + options.dead_after);
        }
        if self.paused || self.overflow_bytes > 0 {
            wake = wake.min(now + RETRY);
        }
        if let Some(deadline) = self.shared.lock().goaway_deadline {
            wake = wake.min(deadline);
        }
        wake
    }

    fn wait(&mut self) -> Result<(), Stop> {
        let desired = (!self.paused, self.write_blocked);
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
        if self.events.iter().any(|event| event.key == KEY) {
            self.armed = None;
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
