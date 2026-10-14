//! WP1.9 acceptance: the v3 session core (frames, I/O thread, multiplexing),
//! REVAMP_PLAN §6.2 and §14 WP1.9.
//!
//! Every test builds an in-process tungstenite pair (plain WS, or WSS with the
//! `tests/fixtures/tls` certificates), optionally through a `FaultProxy`, and
//! hands both ends to `Conn::from_established`. Raw tungstenite peers stand
//! in for misbehaving or legacy endpoints.
mod support;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};
use support::FaultProxy;
use tkfs::{
    model::{
        errors::{ErrorClass, ErrorWire},
        protocol::{
            LaneRole,
            frame::{
                CancelHeader, ChunkHeader, FLAG_LAST, FLAG_MORE, FrameKind, Goaway, Heartbeat,
                MAX_CHUNK, MAX_HEADER, RequestHeader, ResponseHeader,
            },
        },
    },
    session::{
        CallError, Conn, ConnOptions, Handler, Incoming, PeerInfo, Priority, Side,
        frame::{FrameLimits, decode, encode_parts, encode_with},
    },
};
use tungstenite::{Message, WebSocket, stream::MaybeTlsStream};

type Ws = WebSocket<MaybeTlsStream<TcpStream>>;

const MIB: usize = 1024 * 1024;

// ---------------------------------------------------------------------------
// Transports

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Transport {
    Plain,
    Tls,
}

fn accept_ws(listener: TcpListener, transport: Transport) -> thread::JoinHandle<Ws> {
    thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream.set_nonblocking(false).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let stream = match transport {
            Transport::Plain => MaybeTlsStream::Plain(stream),
            Transport::Tls => {
                let identity = native_tls::Identity::from_pkcs12(
                    include_bytes!("fixtures/tls/localhost.p12"),
                    "telekinesis-test",
                )
                .unwrap();
                let acceptor = native_tls::TlsAcceptor::new(identity).unwrap();
                MaybeTlsStream::NativeTls(acceptor.accept(stream).expect("TLS accept"))
            }
        };
        tungstenite::accept(stream).expect("WebSocket accept")
    })
}

fn connect_ws(port: u16, transport: Transport) -> Ws {
    let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let (url, connector) = match transport {
        Transport::Plain => (format!("ws://127.0.0.1:{port}/"), None),
        Transport::Tls => {
            let ca =
                native_tls::Certificate::from_pem(include_bytes!("fixtures/tls/ca.pem")).unwrap();
            let connector = native_tls::TlsConnector::builder()
                .add_root_certificate(ca)
                .build()
                .unwrap();
            (
                format!("wss://localhost:{port}/"),
                Some(tungstenite::Connector::NativeTls(connector)),
            )
        }
    };
    tungstenite::client_tls_with_config(url, stream, None, connector)
        .expect("WebSocket connect")
        .0
}

/// A connected (client, server) WebSocket pair.
fn ws_pair(transport: Transport) -> (Ws, Ws) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = accept_ws(listener, transport);
    let client = connect_ws(port, transport);
    (client, server.join().unwrap())
}

/// A (client, server) pair whose bytes pass through a `FaultProxy`.
fn ws_pair_proxied(transport: Transport) -> (Ws, Ws, FaultProxy) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let proxy = FaultProxy::start(&format!("127.0.0.1:{port}"));
    let server = accept_ws(listener, transport);
    let client = connect_ws(proxy.addr().port(), transport);
    (client, server.join().unwrap(), proxy)
}

fn peer() -> PeerInfo {
    PeerInfo {
        installation: "inst-test".into(),
        role: LaneRole::Control,
        lane: None,
        session: "session-test".into(),
        verifier_generation: 1,
    }
}

fn conns(pair: (Ws, Ws), client: ConnOptions, server: ConnOptions) -> (Conn, Conn) {
    let client = Conn::from_established(pair.0, Side::Client, peer(), client).unwrap();
    let server = Conn::from_established(pair.1, Side::Server, peer(), server).unwrap();
    (client, server)
}

fn session(transport: Transport) -> (Conn, Conn) {
    conns(
        ws_pair(transport),
        ConnOptions::default(),
        ConnOptions::default(),
    )
}

fn fast_liveness() -> ConnOptions {
    ConnOptions {
        heartbeat: Duration::from_millis(200),
        dead_after: Duration::from_secs(1),
        ..ConnOptions::default()
    }
}

struct FnHandler<F>(F);

impl<F: Fn(Incoming) + Send + Sync> Handler for FnHandler<F> {
    fn handle(&self, req: Incoming) {
        (self.0)(req)
    }
}

fn handler(f: impl Fn(Incoming) + Send + Sync + 'static) -> Arc<dyn Handler> {
    Arc::new(FnHandler(f))
}

/// Echoes the header and body of every request.
fn echo() -> Arc<dyn Handler> {
    handler(|req| req.responder.reply(req.header, req.body))
}

/// Seeded test bytes.
fn pattern(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(len);
    out
}

fn wait_until(what: &str, limit: Duration, mut done: impl FnMut() -> bool) {
    let start = Instant::now();
    while !done() {
        assert!(start.elapsed() < limit, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(2));
    }
}

/// Latency bound: the §14 value in release builds; generous in debug builds,
/// whose tests run in parallel with the rest of the suite (REVAMP_PLAN §15.3:
/// strict thresholds only in release qualification runs).
fn latency_bound(release_ms: u64) -> Duration {
    if cfg!(debug_assertions) {
        Duration::from_millis(release_ms * 5)
    } else {
        Duration::from_millis(release_ms)
    }
}

fn p99(samples: &mut [Duration]) -> Duration {
    samples.sort();
    samples[(samples.len() * 99).div_ceil(100) - 1]
}

fn call(conn: &Conn, op: &str, header: Value) -> Result<tkfs::session::Reply, CallError> {
    conn.call(
        op,
        header,
        vec![],
        Priority::Control,
        Duration::from_secs(10),
    )
}

// ---------------------------------------------------------------------------
// Frame codec and bounds (invariant 22)

#[test]
fn bounds() {
    // Codec: every kind round-trips; bounds hold for headers and bodies.
    for kind in FrameKind::ALL {
        let body = if kind.body_forbidden() {
            vec![]
        } else {
            vec![9; 3]
        };
        let bytes = encode_with(
            kind,
            0,
            &CancelHeader { id: 3 },
            &body,
            FrameLimits::SESSION,
        )
        .unwrap();
        let frame = decode(&bytes, FrameLimits::SESSION).unwrap();
        assert_eq!((frame.kind, frame.body), (kind, body));
    }
    let header = json!({ "pad": "x".repeat(16 * 1024) });
    assert!(encode_with(FrameKind::Hello, 0, &header, &[], FrameLimits::PRE_AUTH).is_err());
    assert!(encode_with(FrameKind::Hello, 0, &header, &[], FrameLimits::SESSION).is_ok());
    let header = json!({ "pad": "x".repeat(MAX_HEADER) });
    assert!(encode_with(FrameKind::Request, 0, &header, &[], FrameLimits::SESSION).is_err());
    assert!(
        encode_with(
            FrameKind::Chunk,
            0,
            &ChunkHeader::default(),
            &vec![0; MAX_CHUNK + 1],
            FrameLimits::SESSION
        )
        .is_err()
    );

    // Calls: a body over the op's limit (1 MiB for ops other than
    // meta.exchange) and a header over 64 KiB are refused before sending, and
    // the connection stays usable.
    let (client, server) = session(Transport::Plain);
    server.serve(echo());
    let error = client
        .call(
            "echo",
            json!({}),
            vec![0; MIB + 1],
            Priority::Data,
            Duration::from_secs(5),
        )
        .unwrap_err();
    assert_eq!(error.code(), "WS_MESSAGE_TOO_LARGE", "{error}");
    let error = client
        .call(
            "meta.exchange",
            json!({}),
            vec![0; 16 * MIB + 1],
            Priority::Data,
            Duration::from_secs(5),
        )
        .unwrap_err();
    assert_eq!(error.code(), "WS_MESSAGE_TOO_LARGE", "{error}");
    let error = call(&client, "echo", json!({ "pad": "x".repeat(MAX_HEADER) })).unwrap_err();
    assert_eq!(error.code(), "WS_MESSAGE_TOO_LARGE", "{error}");
    // A handler reply over the op's limit becomes an error, not a violation.
    let (client2, server2) = session(Transport::Plain);
    server2.serve(handler(|req| {
        req.responder.reply(json!({}), vec![1; MIB + 1])
    }));
    let error = call(&client2, "echo", json!({})).unwrap_err();
    assert_eq!(error.code(), "WS_MESSAGE_TOO_LARGE", "{error}");
    assert_eq!(
        call(&client, "echo", json!({"ok": 1})).unwrap().header["ok"],
        1
    );
    assert_eq!(
        call(&client2, "ping", json!({})).unwrap().header["alive"],
        true
    );
    assert!(client.closed().is_none() && client2.closed().is_none());
}

// ---------------------------------------------------------------------------
// Multiplexing

fn thousand_concurrent_requests(transport: Transport) {
    const N: usize = 1000;
    let options = ConnOptions {
        handlers: 2,
        handler_queue: N,
        ..ConnOptions::default()
    };
    let (client, server) = conns(ws_pair(transport), ConnOptions::default(), options);
    // Hold every request until all N are in flight, then answer them in
    // reverse arrival order with a header derived from the request.
    let held = Arc::new(Mutex::new(Vec::<Incoming>::new()));
    let gate = held.clone();
    server.serve(handler(move |req| {
        let ready = {
            let mut held = gate.lock().unwrap();
            held.push(req);
            (held.len() == N).then(|| std::mem::take(&mut *held))
        };
        for req in ready.into_iter().flatten().rev() {
            let n = req.header["n"].as_u64().unwrap();
            req.responder
                .reply(json!({ "n": n, "square": n * n }), n.to_le_bytes().to_vec());
        }
    }));
    let threads: Vec<_> = (0..N as u64)
        .map(|n| {
            let client = client.clone();
            thread::Builder::new()
                .stack_size(256 * 1024)
                .spawn(move || {
                    let reply = client
                        .call(
                            "square",
                            json!({ "n": n }),
                            vec![],
                            Priority::Control,
                            Duration::from_secs(60),
                        )
                        .unwrap();
                    assert_eq!(reply.header["n"], n);
                    assert_eq!(reply.header["square"], n * n);
                    assert_eq!(reply.body, n.to_le_bytes());
                })
                .unwrap()
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(client.health().inflight, 0);
    assert!(held.lock().unwrap().is_empty());
}

#[test]
fn thousand_concurrent_requests_are_paired() {
    thousand_concurrent_requests(Transport::Plain);
}

#[test]
fn thousand_concurrent_requests_are_paired_over_wss() {
    thousand_concurrent_requests(Transport::Tls);
}

fn slow_does_not_block_fast(transport: Transport) {
    let (client, server) = session(transport);
    let (release, released) = mpsc::channel::<()>();
    let released = Mutex::new(released);
    server.serve(handler(move |req| match req.op.as_str() {
        "slow" => {
            // A 20 s handler, released early once the fast calls are done.
            let _ = released
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(20));
            req.responder.reply(json!({ "slow": true }), vec![]);
        }
        _ => req.responder.reply(req.header, req.body),
    }));
    let slow = {
        let client = client.clone();
        thread::spawn(move || {
            client.call(
                "slow",
                json!({}),
                vec![],
                Priority::Control,
                Duration::from_secs(30),
            )
        })
    };
    wait_until("the slow request", Duration::from_secs(5), || {
        server.health().inflight == 1
    });
    let mut samples = Vec::new();
    for i in 0..50 {
        let started = Instant::now();
        let reply = client
            .call(
                "echo",
                json!({ "i": i }),
                pattern(i, 1024),
                Priority::Control,
                Duration::from_secs(5),
            )
            .unwrap();
        samples.push(started.elapsed());
        assert_eq!(reply.header["i"], i);
        assert_eq!(reply.body, pattern(i, 1024));
    }
    let p99 = p99(&mut samples);
    assert!(p99 < latency_bound(50), "fast p99 {p99:?}");
    assert!(!slow.is_finished(), "the slow call must still be running");
    release.send(()).unwrap();
    assert_eq!(slow.join().unwrap().unwrap().header["slow"], true);
}

#[test]
fn slow_handler_does_not_delay_fast_calls() {
    slow_does_not_block_fast(Transport::Plain);
}

#[test]
fn slow_handler_does_not_delay_fast_calls_over_wss() {
    slow_does_not_block_fast(Transport::Tls);
}

#[test]
fn ping_is_answered_without_a_handler() {
    let (client, server) = session(Transport::Plain);
    // No serve(): ping is answered by the I/O thread.
    let reply = client
        .call(
            "ping",
            json!({}),
            vec![],
            Priority::Control,
            Duration::from_secs(5),
        )
        .unwrap();
    assert_eq!(reply.header, json!({ "alive": true }));
    assert_eq!(server.health().inflight, 0);
}

#[test]
fn full_handler_queue_answers_busy() {
    let options = ConnOptions {
        handlers: 1,
        handler_queue: 1,
        ..ConnOptions::default()
    };
    let (client, server) = conns(ws_pair(Transport::Plain), ConnOptions::default(), options);
    let (release, released) = mpsc::channel::<()>();
    let released = Mutex::new(released);
    server.serve(handler(move |req| {
        if req.op == "block" {
            let _ = released
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10));
        }
        req.responder.reply(json!({ "op": req.op }), vec![]);
    }));
    let blocked: Vec<_> = (0..2)
        .map(|_| {
            let client = client.clone();
            let started = thread::spawn(move || call(&client, "block", json!({})));
            thread::sleep(Duration::from_millis(50));
            started
        })
        .collect();
    wait_until(
        "one running and one queued request",
        Duration::from_secs(5),
        || server.health().inflight == 2,
    );
    let error = call(&client, "echo", json!({})).unwrap_err();
    match &error {
        CallError::Remote(wire) => {
            assert_eq!(wire.code, "BUSY");
            assert_eq!(wire.class, ErrorClass::Contention);
        }
        other => panic!("expected BUSY, got {other:?}"),
    }
    assert!(!error.is_uncertain());
    release.send(()).unwrap();
    release.send(()).unwrap();
    for blocked in blocked {
        assert_eq!(blocked.join().unwrap().unwrap().header["op"], "block");
    }
    assert_eq!(
        call(&client, "echo", json!({})).unwrap().header["op"],
        "echo"
    );
}

#[test]
fn server_side_does_not_originate_requests() {
    let (client, server) = session(Transport::Plain);
    client.serve(echo()); // ignored on the client side
    let error = call(&server, "echo", json!({})).unwrap_err();
    assert_eq!(error.code(), "UNSUPPORTED_OPERATION", "{error}");
    assert!(matches!(error, CallError::Unavailable(_)));
}

// ---------------------------------------------------------------------------
// Deadlines, cancellation, errors (invariants 16, 17)

fn timeout_cancels(transport: Transport) {
    let (client, server) = session(transport);
    let (saw, seen) = mpsc::channel();
    server.serve(handler(move |req| {
        if req.op == "hang" {
            let started = Instant::now();
            while !req.responder.cancelled() && started.elapsed() < Duration::from_secs(5) {
                thread::sleep(Duration::from_millis(2));
            }
            saw.send(req.responder.cancelled()).unwrap();
            // A late reply: dropped by the session.
            req.responder.reply(json!({ "late": true }), vec![]);
        } else {
            req.responder.reply(req.header, req.body);
        }
    }));
    let started = Instant::now();
    let error = client
        .call(
            "hang",
            json!({}),
            vec![],
            Priority::Control,
            Duration::from_secs(1),
        )
        .unwrap_err();
    let elapsed = started.elapsed();
    assert!(matches!(error, CallError::Timeout { .. }), "{error:?}");
    assert!(error.is_uncertain());
    assert!(
        elapsed >= Duration::from_secs(1) && elapsed < Duration::from_secs(3),
        "{elapsed:?}"
    );
    assert!(
        seen.recv_timeout(Duration::from_secs(2)).unwrap(),
        "the handler must see cancelled()"
    );
    for i in 0..5 {
        assert_eq!(
            call(&client, "echo", json!({ "i": i })).unwrap().header["i"],
            i
        );
    }
    assert_eq!(client.health().inflight, 0);
    wait_until(
        "the server to forget the request",
        Duration::from_secs(2),
        || server.health().inflight == 0,
    );
    // A timed-out request never resets the connection.
    assert!(client.closed().is_none() && server.closed().is_none());
}

#[test]
fn timeout_sends_cancel_and_the_handler_sees_it() {
    timeout_cancels(Transport::Plain);
}

#[test]
fn timeout_sends_cancel_and_the_handler_sees_it_over_wss() {
    timeout_cancels(Transport::Tls);
}

/// The frames of a successful reply to request `id` with `body`, as a peer
/// sends them: the RESPONSE (with `MORE` and `total` above one chunk) and its
/// CHUNK continuations.
fn reply_frames(id: u64, header: Value, body: &[u8]) -> Vec<Vec<u8>> {
    let split = body.len() > MAX_CHUNK;
    let first = &body[..body.len().min(MAX_CHUNK)];
    let response = ResponseHeader {
        id,
        ok: true,
        header,
        error: None,
        total: split.then_some(body.len() as u64),
    };
    let flags = if split { FLAG_MORE } else { 0 };
    let mut frames = vec![
        encode_with(
            FrameKind::Response,
            flags,
            &response,
            first,
            FrameLimits::SESSION,
        )
        .unwrap(),
    ];
    let mut offset = first.len();
    while offset < body.len() {
        let end = (offset + MAX_CHUNK).min(body.len());
        let last = end == body.len();
        let chunk = ChunkHeader {
            id,
            hash: None,
            offset: offset as u64,
            total: body.len() as u64,
            last,
        };
        let flags = if last { FLAG_LAST } else { FLAG_MORE };
        frames.push(
            encode_with(
                FrameKind::Chunk,
                flags,
                &chunk,
                &body[offset..end],
                FrameLimits::SESSION,
            )
            .unwrap(),
        );
        offset = end;
    }
    frames
}

/// A raw peer that ignores CANCEL and answers timed-out requests anyway: the
/// client drops each late reply (header-only; a 2 MiB body sent after the
/// CANCEL; a 3 MiB body whose first part arrived before the deadline and the
/// rest after the CANCEL) and the next call pairs correctly.
#[test]
fn late_reply_after_cancel_is_dropped() {
    let (client_ws, mut raw) = ws_pair(Transport::Plain);
    let client =
        Conn::from_established(client_ws, Side::Client, peer(), ConnOptions::default()).unwrap();
    let server = thread::spawn(move || {
        let read = |raw: &mut Ws| loop {
            match raw.read().unwrap() {
                Message::Binary(bytes) => {
                    let frame = decode(&bytes, FrameLimits::SESSION).unwrap();
                    if frame.kind != FrameKind::Heartbeat {
                        return frame;
                    }
                }
                _ => continue,
            }
        };
        let request = |raw: &mut Ws| -> RequestHeader {
            let frame = read(raw);
            assert_eq!(frame.kind, FrameKind::Request);
            frame.header_as().unwrap()
        };
        let cancelled = |raw: &mut Ws, id: u64| {
            let cancel = read(raw);
            assert_eq!(
                cancel.kind,
                FrameKind::Cancel,
                "a timed-out call sends CANCEL"
            );
            assert_eq!(cancel.header_as::<CancelHeader>().unwrap().id, id);
        };
        let send = |raw: &mut Ws, frames: Vec<Vec<u8>>| {
            for frame in frames {
                raw.send(Message::Binary(frame.into())).unwrap();
            }
        };
        // A header-only reply after the CANCEL.
        let first = request(&mut raw);
        cancelled(&mut raw, first.id);
        send(&mut raw, reply_frames(first.id, json!({ "late": 1 }), &[]));
        // A reply above one chunk after the CANCEL: RESPONSE(MORE) + CHUNK(LAST).
        let second = request(&mut raw);
        cancelled(&mut raw, second.id);
        send(
            &mut raw,
            reply_frames(second.id, json!({ "late": 2 }), &pattern(2, 2 * MIB)),
        );
        // A reply that crosses the CANCEL: its first part arrives while the
        // call is pending, the continuations after it gave up.
        let third = request(&mut raw);
        let mut frames = reply_frames(third.id, json!({ "late": 3 }), &pattern(3, 3 * MIB));
        let rest = frames.split_off(1);
        send(&mut raw, frames);
        cancelled(&mut raw, third.id);
        send(&mut raw, rest);
        // The next call pairs with its own reply.
        let next = request(&mut raw);
        assert!(next.id > third.id);
        send(&mut raw, reply_frames(next.id, next.header, b"ok"));
        raw
    });
    for op in ["hang", "meta.exchange", "meta.exchange"] {
        let error = client
            .call(
                op,
                json!({}),
                vec![],
                Priority::Control,
                Duration::from_secs(1),
            )
            .unwrap_err();
        assert!(
            matches!(error, CallError::Timeout { .. }),
            "{op}: {error:?}"
        );
        assert!(client.closed().is_none(), "{op}: {:?}", client.closed());
    }
    let reply = call(&client, "echo", json!({ "next": true })).unwrap();
    assert_eq!(reply.header, json!({ "next": true }));
    assert_eq!(reply.body, b"ok");
    let _raw = server.join().unwrap();
    assert!(client.closed().is_none(), "{:?}", client.closed());
    assert_eq!(client.health().inflight, 0);
}

#[test]
fn remote_error_keeps_conn() {
    for transport in [Transport::Plain, Transport::Tls] {
        let (client, server) = session(transport);
        server.serve(handler(|req| match req.op.as_str() {
            "fail" => req
                .responder
                .error(ErrorWire::new("EVENT_ID_REUSED", "EVENT_ID_REUSED: boom")),
            _ => req.responder.reply(req.header, req.body),
        }));
        for round in 0..3 {
            let error = call(&client, "fail", json!({})).unwrap_err();
            match &error {
                CallError::Remote(wire) => {
                    assert_eq!(wire.code, "EVENT_ID_REUSED");
                    assert_eq!(wire.class, ErrorClass::Fatal);
                }
                other => panic!("expected a remote error, got {other:?}"),
            }
            assert!(!error.is_uncertain());
            assert_eq!(
                call(&client, "echo", json!({ "round": round }))
                    .unwrap()
                    .header["round"],
                round
            );
        }
        assert!(client.closed().is_none() && server.closed().is_none());
    }
}

#[test]
fn handler_panic_answers_and_the_pool_survives() {
    let options = ConnOptions {
        handlers: 1,
        ..ConnOptions::default()
    };
    let (client, server) = conns(ws_pair(Transport::Plain), ConnOptions::default(), options);
    server.serve(handler(|req| {
        if req.op == "panic" {
            panic!("handler panic (expected by the test)");
        }
        req.responder.reply(req.header, req.body)
    }));
    let error = call(&client, "panic", json!({})).unwrap_err();
    assert_eq!(error.code(), "UNKNOWN_ERROR", "{error}");
    // The single pool thread survived the panic.
    assert_eq!(
        call(&client, "echo", json!({ "x": 1 })).unwrap().header["x"],
        1
    );
}

#[test]
fn lost_request_not_replayed() {
    let (client_ws, server_ws, proxy) = ws_pair_proxied(Transport::Plain);
    let (client, server) = conns(
        (client_ws, server_ws),
        ConnOptions::default(),
        ConnOptions::default(),
    );
    let executions = Arc::new(AtomicUsize::new(0));
    let counter = executions.clone();
    server.serve(handler(move |req| {
        counter.fetch_add(1, Ordering::SeqCst);
        // Never answers: the connection fails first.
        while !req.responder.cancelled() {
            thread::sleep(Duration::from_millis(5));
        }
    }));
    let pending = {
        let client = client.clone();
        thread::spawn(move || {
            client.call(
                "mutate",
                json!({ "n": 1 }),
                vec![],
                Priority::Control,
                Duration::from_secs(20),
            )
        })
    };
    wait_until("the request to execute", Duration::from_secs(5), || {
        executions.load(Ordering::SeqCst) == 1
    });
    proxy.reset_all();
    let error = pending.join().unwrap().unwrap_err();
    assert!(matches!(error, CallError::ConnectionLost(_)), "{error:?}");
    assert!(error.is_uncertain(), "a lost request is uncertain");
    let closed = client.wait_closed(Duration::from_secs(5)).unwrap();
    assert_eq!(closed.code, "CONNECTION_LOST", "{closed}");
    // Nothing is replayed: not by the session, not on later calls.
    let error = call(&client, "mutate", json!({ "n": 2 })).unwrap_err();
    assert!(matches!(error, CallError::Unavailable(_)), "{error:?}");
    assert!(!error.is_uncertain());
    thread::sleep(Duration::from_millis(300));
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert!(server.wait_closed(Duration::from_secs(5)).is_some());
}

#[test]
fn close_fails_in_flight_calls_as_uncertain() {
    let (client, server) = session(Transport::Plain);
    let (saw, seen) = mpsc::channel();
    server.serve(handler(move |req| {
        let started = Instant::now();
        while !req.responder.cancelled() && started.elapsed() < Duration::from_secs(5) {
            thread::sleep(Duration::from_millis(2));
        }
        saw.send(req.responder.cancelled()).unwrap();
    }));
    let pending = {
        let client = client.clone();
        thread::spawn(move || call(&client, "hold", json!({})))
    };
    wait_until("the request", Duration::from_secs(5), || {
        server.health().inflight == 1
    });
    client.close();
    let error = pending.join().unwrap().unwrap_err();
    assert!(matches!(error, CallError::ConnectionLost(_)), "{error:?}");
    let closed = client.wait_closed(Duration::from_secs(5)).unwrap();
    assert_eq!(closed.code, "WS_CLOSED");
    assert!(server.wait_closed(Duration::from_secs(5)).is_some());
    assert!(seen.recv_timeout(Duration::from_secs(5)).unwrap());
    let error = call(&client, "hold", json!({})).unwrap_err();
    assert!(matches!(error, CallError::Unavailable(_)), "{error:?}");
}

#[test]
fn progress_resets_the_idle_deadline() {
    let (client, server) = session(Transport::Plain);
    server.serve(handler(|req| {
        let progress = req.op == "progress";
        for _ in 0..15 {
            thread::sleep(Duration::from_millis(100));
            if progress {
                req.responder.progress();
            }
        }
        req.responder.reply(json!({ "done": true }), vec![]);
    }));
    let mut none = |_: ChunkHeader, _: &[u8]| -> Result<(), ErrorWire> { Ok(()) };
    let reply = client
        .call_streaming(
            "progress",
            json!({}),
            vec![],
            Priority::Control,
            None,
            Duration::from_millis(700),
            &mut none,
        )
        .unwrap();
    assert_eq!(reply.header["done"], true);
    let error = client
        .call_streaming(
            "silent",
            json!({}),
            vec![],
            Priority::Control,
            None,
            Duration::from_millis(700),
            &mut none,
        )
        .unwrap_err();
    assert!(matches!(error, CallError::Timeout { .. }), "{error:?}");
    // An absolute deadline still applies to a streaming call with progress.
    let error = client
        .call_streaming(
            "progress",
            json!({}),
            vec![],
            Priority::Control,
            Some(Duration::from_millis(1000)),
            Duration::from_millis(700),
            &mut none,
        )
        .unwrap_err();
    assert!(matches!(error, CallError::Timeout { .. }), "{error:?}");
}

// ---------------------------------------------------------------------------
// Liveness

fn stall_is_detected(transport: Transport) {
    let (client_ws, server_ws, proxy) = ws_pair_proxied(transport);
    let (client, server) = conns((client_ws, server_ws), fast_liveness(), fast_liveness());
    server.serve(handler(|req| {
        while !req.responder.cancelled() {
            thread::sleep(Duration::from_millis(5));
        }
    }));
    let calls: Vec<_> = (0..3)
        .map(|i| {
            let client = client.clone();
            thread::spawn(move || {
                let result = call(&client, "hold", json!({ "i": i }));
                (result, Instant::now())
            })
        })
        .collect();
    wait_until("three requests in flight", Duration::from_secs(5), || {
        server.health().inflight == 3
    });
    let stalled = Instant::now();
    // The §14 scenario: a 2 s stall against heartbeat 200 ms / dead 1 s.
    proxy.stall_for(Duration::from_secs(2));
    for call in calls {
        let (result, at) = call.join().unwrap();
        let error = result.unwrap_err();
        assert!(matches!(error, CallError::ConnectionLost(_)), "{error:?}");
        let detected = at - stalled;
        assert!(
            detected >= Duration::from_millis(750) && detected < Duration::from_millis(1900),
            "detected after {detected:?}"
        );
    }
    let closed = client.wait_closed(Duration::from_secs(5)).unwrap();
    assert_eq!(closed.code, "TRANSPORT_DEAD", "{closed}");
    assert_eq!(closed.class, ErrorClass::Transient);
    let closed = server.wait_closed(Duration::from_secs(5)).unwrap();
    assert_eq!(closed.code, "TRANSPORT_DEAD", "{closed}");
}

#[test]
fn heartbeat_detects_a_stalled_link() {
    stall_is_detected(Transport::Plain);
}

#[test]
fn heartbeat_detects_a_stalled_link_over_wss() {
    stall_is_detected(Transport::Tls);
}

#[test]
fn idle_connection_stays_up() {
    for transport in [Transport::Plain, Transport::Tls] {
        let (client_ws, server_ws, proxy) = ws_pair_proxied(transport);
        let (client, server) = conns((client_ws, server_ws), fast_liveness(), fast_liveness());
        server.serve(echo());
        // Idle for three times dead_after: heartbeats keep both ends alive.
        thread::sleep(Duration::from_secs(3));
        assert!(client.closed().is_none() && server.closed().is_none());
        assert_eq!(
            call(&client, "echo", json!({ "x": 1 })).unwrap().header["x"],
            1
        );
        let health = client.health();
        assert!(health.rtt_ms.is_some(), "{health:?}");
        assert!(health.rtt_ms.unwrap() < 500, "{health:?}");
        assert!(proxy.bytes_up() > 0 && proxy.bytes_down() > 0);
        assert_eq!(proxy.resets(), 0);
    }
}

#[test]
fn health_reports_traffic() {
    let (client, server) = session(Transport::Plain);
    server.serve(echo());
    let before = client.health();
    for i in 0..10 {
        client
            .call(
                "echo",
                json!({ "i": i }),
                pattern(i, 10_000),
                Priority::Response,
                Duration::from_secs(5),
            )
            .unwrap();
    }
    let health = client.health();
    assert!(health.bytes_up >= before.bytes_up + 100_000, "{health:?}");
    assert!(
        health.bytes_down >= before.bytes_down + 100_000,
        "{health:?}"
    );
    assert!(health.last_rx_ms >= health.since_ms && health.last_tx_ms >= health.since_ms);
    assert_eq!((health.inflight, health.queued_bytes), (0, 0));
    let server_health = server.health();
    assert!(server_health.bytes_down >= 100_000 && server_health.bytes_up >= 100_000);
}

// ---------------------------------------------------------------------------
// Streams, bodies, backpressure

/// Consumes a streamed request body; returns `(bytes, parts)`.
fn drain_parts(req: &Incoming) -> (u64, u64) {
    let (mut bytes, mut parts) = (0u64, 0u64);
    for (_, data) in req.chunks.as_ref().unwrap() {
        bytes += data.len() as u64;
        parts += 1;
    }
    (bytes, parts)
}

/// Serves `download` (streams `count` chunks of `size` bytes, then replies with
/// the digest) and `upload` (consumes `Incoming::chunks`, replies with the byte
/// count and digest), optionally sleeping per upload chunk. Also: `sink`
/// (counts a streamed body without hashing it), `late_reader` (starts reading
/// its parts after `delay_ms`) and `paced` (streams `count` chunks of `size`
/// bytes, one every `pace_ms`).
fn stream_service(upload_delay: Duration) -> Arc<dyn Handler> {
    handler(move |req| match req.op.as_str() {
        "sink" | "late_reader" => {
            if let Some(delay) = req.header["delay_ms"].as_u64() {
                thread::sleep(Duration::from_millis(delay));
            }
            let (bytes, parts) = drain_parts(&req);
            if !req.responder.cancelled() {
                req.responder
                    .reply(json!({ "bytes": bytes, "parts": parts }), vec![]);
            }
        }
        "paced" => {
            let count = req.header["count"].as_u64().unwrap();
            let size = req.header["size"].as_u64().unwrap();
            let pace = Duration::from_millis(req.header["pace_ms"].as_u64().unwrap());
            for i in 0..count {
                thread::sleep(pace);
                let header = ChunkHeader {
                    id: 0,
                    hash: None,
                    offset: i * size,
                    total: count * size,
                    last: i + 1 == count,
                };
                if req
                    .responder
                    .chunk(header, &vec![i as u8; size as usize])
                    .is_err()
                {
                    return;
                }
            }
            req.responder.reply(json!({ "parts": count }), vec![]);
        }
        "download" => {
            let count = req.header["count"].as_u64().unwrap();
            let size = req.header["size"].as_u64().unwrap() as usize;
            let mut digest = Sha256::new();
            let total = count * size as u64;
            for i in 0..count {
                let data = pattern(i, size);
                digest.update(&data);
                let header = ChunkHeader {
                    id: 0,
                    hash: Some(format!("object-{i}")),
                    offset: i * size as u64,
                    total,
                    last: i + 1 == count,
                };
                if req.responder.chunk(header, &data).is_err() {
                    return;
                }
            }
            req.responder
                .reply(json!({ "digest": hex::encode(digest.finalize()) }), vec![]);
        }
        "upload" => {
            let mut digest = Sha256::new();
            let mut bytes = 0u64;
            let mut parts = 0u64;
            for (header, data) in req.chunks.as_ref().unwrap() {
                assert_eq!(header.offset, bytes, "parts arrive in order");
                bytes += data.len() as u64;
                parts += 1;
                digest.update(&data);
                if !upload_delay.is_zero() {
                    thread::sleep(upload_delay);
                }
            }
            if req.responder.cancelled() {
                return;
            }
            req.responder.reply(
                json!({ "bytes": bytes, "parts": parts, "digest": hex::encode(digest.finalize()) }),
                vec![],
            );
        }
        "reject" => req.responder.error(ErrorWire::new(
            "UNSOLICITED_OBJECT",
            "UNSOLICITED_OBJECT: not wanted",
        )),
        _ => req.responder.reply(req.header, req.body),
    })
}

/// Uploads `count` chunks of `size` bytes; returns the expected digest.
fn upload(conn: &Conn, count: u64, size: usize) -> (tkfs::session::Reply, String) {
    let mut expected = Sha256::new();
    let mut upload = conn
        .upload("upload", json!({}), Priority::Data, Duration::from_secs(30))
        .unwrap();
    for i in 0..count {
        let data = pattern(1000 + i, size);
        expected.update(&data);
        upload
            .chunk(
                ChunkHeader {
                    id: 0,
                    hash: Some(format!("object-{i}")),
                    offset: i * size as u64,
                    total: count * size as u64,
                    last: i + 1 == count,
                },
                &data,
            )
            .unwrap();
    }
    (upload.finish().unwrap(), hex::encode(expected.finalize()))
}

fn download(conn: &Conn, count: u64, size: usize) -> (String, String, u64) {
    let mut received = Sha256::new();
    let mut expected = Sha256::new();
    let mut parts = 0u64;
    let mut on_chunk = |header: ChunkHeader, data: &[u8]| -> Result<(), ErrorWire> {
        assert_eq!(header.offset, parts * size as u64);
        assert_eq!(
            header.hash.as_deref(),
            Some(format!("object-{parts}").as_str())
        );
        expected.update(pattern(parts, size));
        received.update(data);
        parts += 1;
        Ok(())
    };
    let reply = conn
        .call_streaming(
            "download",
            json!({ "count": count, "size": size }),
            vec![],
            Priority::Control,
            None,
            Duration::from_secs(30),
            &mut on_chunk,
        )
        .unwrap();
    assert_eq!(
        reply.header["digest"].as_str().unwrap(),
        hex::encode(expected.clone().finalize())
    );
    (
        hex::encode(received.finalize()),
        hex::encode(expected.finalize()),
        parts,
    )
}

fn duplex_32mib(transport: Transport) {
    let (client, server) = session(transport);
    server.serve(stream_service(Duration::ZERO));
    let started = Instant::now();
    let up = {
        let client = client.clone();
        thread::spawn(move || upload(&client, 32, MIB))
    };
    let down = {
        let client = client.clone();
        thread::spawn(move || download(&client, 32, MIB))
    };
    let (reply, expected) = up.join().unwrap();
    assert_eq!(reply.header["bytes"], 32 * MIB as u64);
    assert_eq!(reply.header["parts"], 32);
    assert_eq!(reply.header["digest"].as_str().unwrap(), expected);
    let (received, expected, parts) = down.join().unwrap();
    assert_eq!((received, parts), (expected, 32));
    assert!(
        started.elapsed() < Duration::from_secs(120),
        "{:?}",
        started.elapsed()
    );
    assert!(client.closed().is_none());
    let health = client.health();
    assert!(health.bytes_up >= 32 * MIB as u64 && health.bytes_down >= 32 * MIB as u64);
}

#[test]
fn duplex_32mib_each_way() {
    duplex_32mib(Transport::Plain);
}

#[test]
fn duplex_32mib_each_way_over_wss() {
    duplex_32mib(Transport::Tls);
}

fn large_bodies(transport: Transport) {
    let (client, server) = session(transport);
    server.serve(echo());
    for (i, prio) in [Priority::Control, Priority::Response, Priority::Data]
        .into_iter()
        .enumerate()
    {
        let body = pattern(i as u64, 5 * MIB + 123);
        let reply = client
            .call(
                "meta.exchange",
                json!({ "repo": "r", "i": i }),
                body.clone(),
                prio,
                Duration::from_secs(30),
            )
            .unwrap();
        assert_eq!(reply.header["i"], i);
        assert!(reply.body == body, "body {i} differs");
    }
    // A body of exactly one chunk is not split; one byte more is.
    for len in [MIB, MIB + 1, 2 * MIB] {
        let body = pattern(len as u64, len);
        let reply = client
            .call(
                "meta.exchange",
                json!({}),
                body.clone(),
                Priority::Response,
                Duration::from_secs(30),
            )
            .unwrap();
        assert!(reply.body == body, "body of {len} bytes differs");
    }
    // Concurrent large calls interleave on one connection.
    let threads: Vec<_> = (0..4u64)
        .map(|i| {
            let client = client.clone();
            thread::spawn(move || {
                let body = pattern(100 + i, 3 * MIB);
                let reply = client
                    .call(
                        "meta.exchange",
                        json!({ "i": i }),
                        body.clone(),
                        Priority::Response,
                        Duration::from_secs(30),
                    )
                    .unwrap();
                assert!(reply.body == body && reply.header["i"] == i);
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
}

#[test]
fn large_bodies_are_chunked_and_reassembled() {
    large_bodies(Transport::Plain);
}

#[test]
fn large_bodies_are_chunked_and_reassembled_over_wss() {
    large_bodies(Transport::Tls);
}

/// A control call is answered within the bound while at least 30 MiB of data
/// is queued ahead of it: a producer keeps the client's data queue (32 MiB)
/// full for the whole measurement, and every sample is checked.
fn control_fast_while_data_queued(transport: Transport) {
    let (client, server) = session(transport);
    server.serve(stream_service(Duration::ZERO));
    let stop = Arc::new(AtomicBool::new(false));
    let producer = {
        let client = client.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            let mut upload = client
                .upload("sink", json!({}), Priority::Data, Duration::from_secs(30))
                .unwrap();
            let data = pattern(7, MIB);
            let mut parts = 0u64;
            while !stop.load(Ordering::SeqCst) {
                let header = ChunkHeader {
                    id: 0,
                    hash: None,
                    offset: parts * MIB as u64,
                    total: 0,
                    last: false,
                };
                upload.chunk(header, &data).unwrap();
                parts += 1;
            }
            let reply = upload.finish().unwrap();
            assert_eq!(reply.header["bytes"], parts * MIB as u64);
        })
    };
    let mut samples = Vec::new();
    for round in 0..10u64 {
        wait_until("30 MiB queued", Duration::from_secs(10), || {
            client.health().queued_bytes >= 30 * MIB as u64
        });
        let started = Instant::now();
        let reply = client
            .call(
                "echo",
                json!({ "round": round }),
                vec![],
                Priority::Control,
                Duration::from_secs(5),
            )
            .unwrap();
        samples.push(started.elapsed());
        assert_eq!(reply.header["round"], round);
    }
    stop.store(true, Ordering::SeqCst);
    producer.join().unwrap();
    let worst = *samples.iter().max().unwrap();
    assert!(
        worst < latency_bound(50),
        "control latency with at least 30 MiB queued: {samples:?}"
    );
}

#[test]
fn control_call_is_fast_while_data_is_queued() {
    control_fast_while_data_queued(Transport::Plain);
}

#[test]
fn control_call_is_fast_while_data_is_queued_over_wss() {
    control_fast_while_data_queued(Transport::Tls);
}

fn small_queues() -> ConnOptions {
    ConnOptions {
        data_queue_bytes: 4 * MIB,
        ..ConnOptions::default()
    }
}

/// Samples `queued_bytes` of `conn` every millisecond until `stop`.
fn sample_queue(conn: &Conn, stop: Arc<AtomicBool>) -> thread::JoinHandle<u64> {
    let conn = conn.clone();
    thread::spawn(move || {
        let mut max = 0;
        while !stop.load(Ordering::SeqCst) {
            max = max.max(conn.health().queued_bytes);
            thread::sleep(Duration::from_millis(1));
        }
        max
    })
}

fn backpressure_upload(transport: Transport) {
    // Slow receiver: the handler takes 20 ms per 1 MiB part.
    const PARTS: u64 = 64;
    let (client, server) = conns(ws_pair(transport), small_queues(), small_queues());
    server.serve(stream_service(Duration::from_millis(20)));
    let parts: Vec<Vec<u8>> = (0..PARTS).map(|i| pattern(1000 + i, MIB)).collect();
    let mut expected = Sha256::new();
    for part in &parts {
        expected.update(part);
    }
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = sample_queue(&client, stop.clone());
    let started = Instant::now();
    let mut upload = client
        .upload("upload", json!({}), Priority::Data, Duration::from_secs(30))
        .unwrap();
    let mut blocked = Duration::ZERO;
    for (i, data) in (0..PARTS).zip(&parts) {
        let chunk_started = Instant::now();
        upload
            .chunk(
                ChunkHeader {
                    id: 0,
                    hash: None,
                    offset: i * MIB as u64,
                    total: PARTS * MIB as u64,
                    last: i + 1 == PARTS,
                },
                data,
            )
            .unwrap();
        blocked = blocked.max(chunk_started.elapsed());
    }
    let produced = started.elapsed();
    let reply = upload.finish().unwrap();
    stop.store(true, Ordering::SeqCst);
    let max = sampler.join().unwrap();
    assert_eq!(
        reply.header["digest"].as_str().unwrap(),
        hex::encode(expected.finalize())
    );
    assert_eq!(reply.header["bytes"], PARTS * MIB as u64);
    // Queue memory never exceeds the cap (plus a few small control frames)...
    assert!(max <= 4 * MIB as u64 + 64 * 1024, "max queued {max}");
    // ...although the producer outran the receiver: the queue filled and
    // chunk() blocked, end to end through the receiver's paused reads.
    assert!(max >= 2 * MIB as u64, "the queue never filled: {max}");
    assert!(
        blocked >= Duration::from_millis(10),
        "chunk() never blocked (longest {blocked:?}, produced in {produced:?})"
    );
}

#[test]
fn backpressure_bounds_queue_memory_with_a_slow_receiver() {
    backpressure_upload(Transport::Plain);
}

#[test]
fn backpressure_bounds_queue_memory_with_a_slow_receiver_over_wss() {
    backpressure_upload(Transport::Tls);
}

#[test]
fn backpressure_bounds_a_streamed_reply() {
    // Slow client consumer: the server's responder blocks instead of queueing.
    let (client, server) = conns(ws_pair(Transport::Plain), small_queues(), small_queues());
    server.serve(stream_service(Duration::ZERO));
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = sample_queue(&server, stop.clone());
    let mut parts = 0;
    let mut slow = |_: ChunkHeader, _: &[u8]| -> Result<(), ErrorWire> {
        parts += 1;
        thread::sleep(Duration::from_millis(20));
        Ok(())
    };
    let started = Instant::now();
    client
        .call_streaming(
            "download",
            json!({ "count": 48, "size": MIB }),
            vec![],
            Priority::Control,
            None,
            Duration::from_secs(30),
            &mut slow,
        )
        .unwrap();
    stop.store(true, Ordering::SeqCst);
    let max = sampler.join().unwrap();
    assert_eq!(parts, 48);
    assert!(started.elapsed() >= Duration::from_millis(900));
    assert!(max <= 4 * MIB as u64 + 64 * 1024, "max queued {max}");
}

#[test]
fn upload_rejected_early_reports_the_error() {
    let (client, server) = session(Transport::Plain);
    server.serve(stream_service(Duration::ZERO));
    let mut upload = client
        .upload(
            "reject",
            json!({}),
            Priority::Control,
            Duration::from_secs(10),
        )
        .unwrap();
    let data = vec![7u8; 64 * 1024];
    let mut failed = None;
    let started = Instant::now();
    for i in 0.. {
        if started.elapsed() > Duration::from_secs(10) {
            break;
        }
        if let Err(error) = upload.chunk(
            ChunkHeader {
                id: 0,
                hash: None,
                offset: i * data.len() as u64,
                total: 0,
                last: false,
            },
            &data,
        ) {
            failed = Some(error);
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    let error = failed.expect("chunks fail once the server answered");
    assert_eq!(error.code(), "UNSOLICITED_OBJECT", "{error}");
    let error = upload.finish().unwrap_err();
    assert_eq!(error.code(), "UNSOLICITED_OBJECT", "{error}");
    assert_eq!(
        call(&client, "echo", json!({ "x": 2 })).unwrap().header["x"],
        2
    );
}

#[test]
fn dropped_upload_cancels_the_request() {
    let (client, server) = session(Transport::Plain);
    let (saw, seen) = mpsc::channel();
    let started = Arc::new(AtomicBool::new(false));
    let running = started.clone();
    server.serve(handler(move |req| {
        running.store(true, Ordering::SeqCst);
        let parts = req.chunks.as_ref().unwrap().iter().count();
        saw.send((parts, req.responder.cancelled())).unwrap();
    }));
    let mut upload = client
        .upload("upload", json!({}), Priority::Data, Duration::from_secs(10))
        .unwrap();
    for i in 0..2u64 {
        upload
            .chunk(
                ChunkHeader {
                    id: 0,
                    hash: None,
                    offset: i,
                    total: 0,
                    last: false,
                },
                b"x",
            )
            .unwrap();
    }
    wait_until("the handler to start", Duration::from_secs(5), || {
        started.load(Ordering::SeqCst)
    });
    drop(upload);
    let (parts, cancelled) = seen.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(cancelled, "the handler sees the cancelled upload");
    assert!(parts <= 2);
    assert_eq!(client.health().inflight, 0);
    wait_until(
        "the server to forget the upload",
        Duration::from_secs(2),
        || server.health().inflight == 0,
    );
}

// ---------------------------------------------------------------------------
// Flow control on every way out of a call, bounded outgoing queues

/// A stream callback that fails or panics after the I/O thread already
/// received the rest of the stream and the outcome, and a plain `call` that
/// receives a stream, leave nothing counted against receive-side flow
/// control: the connection keeps reading and serving.
#[test]
fn failed_stream_callbacks_release_flow_control() {
    // Receive cap 4 MiB; each failed 3 MiB stream would leave up to 2 MiB
    // counted, which would hold the client's reads for good.
    let (client, server) = conns(ws_pair(Transport::Plain), small_queues(), small_queues());
    server.serve(stream_service(Duration::ZERO));
    let header = json!({ "count": 3, "size": MIB });
    let stream_done = |client: &Conn| {
        wait_until(
            "the stream and its outcome",
            Duration::from_secs(10),
            || client.health().inflight == 0,
        );
    };
    for round in 0..3 {
        let mut first = true;
        let mut fail_late = |_: ChunkHeader, _: &[u8]| -> Result<(), ErrorWire> {
            if first {
                first = false;
                stream_done(&client);
            }
            Err(ErrorWire::new("DISK_FULL", "DISK_FULL: test callback"))
        };
        let error = client
            .call_streaming(
                "download",
                header.clone(),
                vec![],
                Priority::Control,
                None,
                Duration::from_secs(10),
                &mut fail_late,
            )
            .unwrap_err();
        assert_eq!(error.code(), "DISK_FULL", "round {round}: {error}");
    }
    for round in 0..2 {
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut panic_late = |_: ChunkHeader, _: &[u8]| -> Result<(), ErrorWire> {
                stream_done(&client);
                panic!("test callback panics after the stream completed");
            };
            client.call_streaming(
                "download",
                header.clone(),
                vec![],
                Priority::Control,
                None,
                Duration::from_secs(10),
                &mut panic_late,
            )
        }));
        assert!(unwound.is_err(), "round {round}: the callback panicked");
    }
    for round in 0..3 {
        // A stream sent to a plain call: refused locally, the call cancelled.
        let error = client
            .call(
                "download",
                header.clone(),
                vec![],
                Priority::Control,
                Duration::from_secs(10),
            )
            .unwrap_err();
        assert_eq!(error.code(), "PROTOCOL_VIOLATION", "round {round}: {error}");
    }
    let started = Instant::now();
    assert_eq!(
        call(&client, "echo", json!({ "x": 1 })).unwrap().header["x"],
        1
    );
    assert_eq!(
        call(&client, "ping", json!({})).unwrap().header["alive"],
        true
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    // Twice the receive cap still streams through (reads hold and resume).
    let (received, expected, parts) = download(&client, 8, MIB);
    assert_eq!((received, parts), (expected, 8));
    assert!(client.closed().is_none() && server.closed().is_none());
}

/// After a `chunk()` timeout the upload is cancelled; `finish()` (and any
/// later `chunk()`) reports that timeout at once instead of waiting for an
/// outcome that cannot come.
#[test]
fn finish_after_a_chunk_timeout_returns_at_once() {
    let (client_ws, server_ws, proxy) = ws_pair_proxied(Transport::Plain);
    let one_frame = ConnOptions {
        data_queue_bytes: 1,
        ..ConnOptions::default()
    };
    let (client, server) = conns((client_ws, server_ws), one_frame, ConnOptions::default());
    server.serve(stream_service(Duration::ZERO));
    // A stalled link: the data queue fills and stays full.
    proxy.stall_for(Duration::from_secs(60));
    let mut upload = client
        .upload(
            "sink",
            json!({}),
            Priority::Data,
            Duration::from_millis(500),
        )
        .unwrap();
    let data = vec![1u8; MIB];
    let part = |i: u64| ChunkHeader {
        id: 0,
        hash: None,
        offset: i * MIB as u64,
        total: 0,
        last: false,
    };
    let mut failed = None;
    for i in 0..1000u64 {
        if let Err(error) = upload.chunk(part(i), &data) {
            failed = Some(error);
            break;
        }
    }
    let error = failed.expect("chunk() times out on a stalled link");
    assert!(matches!(error, CallError::Timeout { .. }), "{error:?}");
    let again = upload.chunk(part(0), b"x").unwrap_err();
    assert!(matches!(again, CallError::Timeout { .. }), "{again:?}");
    assert_eq!(client.health().inflight, 0, "the upload was cancelled");
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || tx.send(upload.finish()).unwrap());
    let outcome = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("finish() returns at once after a chunk() timeout");
    assert!(
        matches!(outcome, Err(CallError::Timeout { .. })),
        "{outcome:?}"
    );
    // The link recovers; whatever the server answers the abandoned upload is
    // dropped, and the connection keeps serving.
    proxy.clear();
    assert_eq!(
        call(&client, "echo", json!({ "after": 1 })).unwrap().header["after"],
        1
    );
    assert!(client.closed().is_none() && server.closed().is_none());
}

/// Invariant 22 for outgoing queues: a peer that keeps sending requests and
/// never reads cannot grow the server's queues past their bounds. The server
/// stops reading (TCP pushes the flood back) and, once the peer has taken
/// nothing for `dead_after`, closes the connection as `TRANSPORT_DEAD`.
fn unread_replies_stay_bounded(op: &str) {
    let options = ConnOptions {
        control_queue_bytes: MIB,
        heartbeat: Duration::from_millis(200),
        dead_after: Duration::from_secs(1),
        ..ConnOptions::default()
    };
    let cap = options.sanitized().control_queue_bytes as u64;
    let (mut raw, server_ws) = ws_pair(Transport::Plain);
    let server = Conn::from_established(server_ws, Side::Server, peer(), options).unwrap();
    // `big`: a tiny request, a 512 KiB reply.
    server.serve(handler(|req| {
        req.responder
            .reply(json!({ "op": req.op }), vec![7u8; 512 * 1024])
    }));
    if let MaybeTlsStream::Plain(stream) = raw.get_mut() {
        stream
            .set_write_timeout(Some(Duration::from_millis(500)))
            .unwrap();
    }
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = sample_queue(&server, stop.clone());
    let started = Instant::now();
    let mut sent = 0u64;
    'flood: while started.elapsed() < Duration::from_secs(30) {
        for _ in 0..256 {
            sent += 1;
            let request = raw_frame(5, 0, 0, &request_header(sent, op, None), b"");
            if raw.write(request).is_err() {
                break 'flood;
            }
        }
        if raw.flush().is_err() {
            break;
        }
    }
    let flooded = started.elapsed();
    assert!(
        flooded < Duration::from_secs(30),
        "{op}: the flood was never pushed back ({sent} requests)"
    );
    // The queues stay bounded while the hold lasts, and the server gives up
    // once the peer has taken nothing for `dead_after` (1 s).
    let closed = server.wait_closed(Duration::from_secs(10));
    stop.store(true, Ordering::SeqCst);
    let max = sampler.join().unwrap();
    let closed =
        closed.unwrap_or_else(|| panic!("{op}: the server kept a peer that reads nothing"));
    assert_eq!(closed.code, "TRANSPORT_DEAD", "{op}: {closed}");
    assert!(
        closed.message.contains("read nothing"),
        "{op}: judged by the write side: {closed}"
    );
    drop(raw);
    // One bound for the control queue (`ping` replies, `BUSY`), one for reply
    // bodies, plus one small frame.
    let bound = if op == "ping" { cap } else { 2 * cap };
    assert!(
        max <= bound + 64 * 1024,
        "{op}: {max} bytes queued (bound {bound}) after {sent} requests"
    );
    assert!(max > cap / 2, "{op}: the queue never filled ({max} bytes)");
}

#[test]
fn unread_ping_replies_hold_reads_and_stay_bounded() {
    // Answered by the I/O thread itself.
    unread_replies_stay_bounded("ping");
}

#[test]
fn unread_handler_replies_stay_bounded() {
    // Handler replies wait for room in the reply-body queue; the handler
    // queue then fills, the I/O thread answers BUSY, and reading stops.
    unread_replies_stay_bounded("big");
}

/// An early reply to a streamed request drops the rest of its body still
/// queued on the client instead of sending it to a server that discards it.
#[test]
fn early_reply_purges_the_rest_of_an_upload() {
    let (client_ws, server_ws, proxy) = ws_pair_proxied(Transport::Plain);
    let (client, server) = conns(
        (client_ws, server_ws),
        ConnOptions::default(),
        ConnOptions::default(),
    );
    server.serve(stream_service(Duration::ZERO));
    // A slow uplink: queued data would take many seconds to drain.
    proxy.set_bandwidth(2 * MIB as u64);
    let mut upload = client
        .upload(
            "reject",
            json!({}),
            Priority::Control,
            Duration::from_secs(10),
        )
        .unwrap();
    let data = vec![3u8; MIB];
    let mut failed = None;
    for i in 0..1000u64 {
        let header = ChunkHeader {
            id: 0,
            hash: None,
            offset: i * MIB as u64,
            total: 0,
            last: false,
        };
        if let Err(error) = upload.chunk(header, &data) {
            failed = Some(error);
            break;
        }
    }
    let error = failed.expect("chunks fail once the server answered");
    assert_eq!(error.code(), "UNSOLICITED_OBJECT", "{error}");
    wait_until(
        "the rejected upload's queued data to be dropped",
        Duration::from_secs(1),
        || client.health().queued_bytes == 0,
    );
    let error = upload.finish().unwrap_err();
    assert_eq!(error.code(), "UNSOLICITED_OBJECT", "{error}");
    proxy.set_bandwidth(0);
    assert_eq!(
        call(&client, "echo", json!({ "x": 3 })).unwrap().header["x"],
        3
    );
}

/// While the server holds its reads because one handler is behind on its
/// parts, another streamed request whose parts wait unread behind the hold is
/// not timed out for idleness.
#[test]
fn server_idle_limit_does_not_run_while_reads_are_held() {
    let one_frame = ConnOptions {
        data_queue_bytes: 1,
        ..ConnOptions::default()
    };
    let (client, server) = conns(ws_pair(Transport::Plain), ConnOptions::default(), one_frame);
    server.serve(stream_service(Duration::ZERO));
    // The victim: idle 500 ms, one 16 KiB part every 100 ms for 2.5 s.
    let victim = {
        let client = client.clone();
        thread::spawn(move || {
            let mut upload = client
                .upload(
                    "sink",
                    json!({}),
                    Priority::Data,
                    Duration::from_millis(500),
                )
                .unwrap();
            for i in 0..25u64 {
                let header = ChunkHeader {
                    id: 0,
                    hash: None,
                    offset: i * 16 * 1024,
                    total: 0,
                    last: false,
                };
                upload.chunk(header, &[5u8; 16 * 1024])?;
                thread::sleep(Duration::from_millis(100));
            }
            upload.finish()
        })
    };
    wait_until(
        "the server to serve the victim",
        Duration::from_secs(5),
        || server.health().inflight == 1,
    );
    // The culprit: its handler starts reading 1.5 s late, so its 8 MiB fill
    // the server's receive buffer (cap ~1 MiB) and the server holds its reads.
    let mut culprit = client
        .upload(
            "late_reader",
            json!({ "delay_ms": 1500 }),
            Priority::Data,
            Duration::from_secs(10),
        )
        .unwrap();
    for i in 0..8u64 {
        let header = ChunkHeader {
            id: 0,
            hash: None,
            offset: i * MIB as u64,
            total: 0,
            last: false,
        };
        culprit.chunk(header, &vec![6u8; MIB]).unwrap();
    }
    let reply = culprit.finish().unwrap();
    assert_eq!(reply.header["bytes"], 8 * MIB as u64);
    let reply = victim.join().unwrap().unwrap();
    assert_eq!(reply.header["bytes"], 25 * 16 * 1024);
    assert!(client.closed().is_none() && server.closed().is_none());
}

/// While the client holds its reads because one stream's callback is behind,
/// another stream whose chunks wait unread behind the hold is not timed out
/// for idleness.
#[test]
fn client_idle_limit_does_not_run_while_reads_are_held() {
    let one_frame = ConnOptions {
        data_queue_bytes: 1,
        ..ConnOptions::default()
    };
    let (client, server) = conns(ws_pair(Transport::Plain), one_frame, ConnOptions::default());
    server.serve(stream_service(Duration::ZERO));
    let (sleeping, asleep) = mpsc::channel();
    // The culprit: its callback sleeps 1.5 s on the first chunk, so the next
    // ones fill the receive buffer (cap ~1 MiB) and the client holds its reads.
    let culprit = {
        let client = client.clone();
        thread::spawn(move || {
            let mut first = true;
            let mut slow = |_: ChunkHeader, _: &[u8]| -> Result<(), ErrorWire> {
                if first {
                    first = false;
                    sleeping.send(()).unwrap();
                    thread::sleep(Duration::from_millis(1500));
                }
                Ok(())
            };
            client.call_streaming(
                "download",
                json!({ "count": 4, "size": MIB }),
                vec![],
                Priority::Control,
                None,
                Duration::from_secs(10),
                &mut slow,
            )
        })
    };
    asleep.recv_timeout(Duration::from_secs(5)).unwrap();
    thread::sleep(Duration::from_millis(100));
    // The victim: idle 500 ms, one chunk every 100 ms.
    let mut parts = 0;
    let mut count = |_: ChunkHeader, _: &[u8]| -> Result<(), ErrorWire> {
        parts += 1;
        Ok(())
    };
    let reply = client
        .call_streaming(
            "paced",
            json!({ "count": 10, "size": 16 * 1024, "pace_ms": 100 }),
            vec![],
            Priority::Control,
            None,
            Duration::from_millis(500),
            &mut count,
        )
        .unwrap();
    assert_eq!(reply.header["parts"], 10);
    assert_eq!(parts, 10);
    culprit.join().unwrap().unwrap();
    assert!(client.closed().is_none() && server.closed().is_none());
}

/// A handler that stops taking its parts is the cause of a hold, not a victim
/// of it: it is cancelled after its request's idle limit (`REQUEST_TIMEOUT`),
/// which releases the hold, and the connection keeps serving.
#[test]
fn stuck_handler_is_cancelled_after_its_idle_limit() {
    let one_frame = ConnOptions {
        data_queue_bytes: 1,
        ..ConnOptions::default()
    };
    let (client, server) = conns(ws_pair(Transport::Plain), ConnOptions::default(), one_frame);
    let (saw, seen) = mpsc::channel();
    server.serve(handler(move |req| {
        if req.op != "stuck" {
            return req.responder.reply(req.header, req.body);
        }
        while !req.responder.cancelled() {
            thread::sleep(Duration::from_millis(5));
        }
        saw.send(()).unwrap();
    }));
    let started = Instant::now();
    let mut upload = client
        .upload("stuck", json!({}), Priority::Data, Duration::from_secs(1))
        .unwrap();
    let mut failed = None;
    for i in 0..64u64 {
        let header = ChunkHeader {
            id: 0,
            hash: None,
            offset: i * MIB as u64,
            total: 0,
            last: false,
        };
        if let Err(error) = upload.chunk(header, &vec![4u8; MIB]) {
            failed = Some(error);
            break;
        }
    }
    let error = match failed {
        Some(error) => error,
        None => upload.finish().unwrap_err(),
    };
    assert_eq!(error.code(), "REQUEST_TIMEOUT", "{error}");
    seen.recv_timeout(Duration::from_secs(5))
        .expect("the stuck handler sees cancelled()");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(
        call(&client, "echo", json!({ "x": 4 })).unwrap().header["x"],
        4
    );
    assert!(client.closed().is_none() && server.closed().is_none());
}

// ---------------------------------------------------------------------------
// More streamed requests than handlers, idle limits across a peer's hold,
// fair admission (fix round 2)

fn mib_part(i: u64) -> ChunkHeader {
    ChunkHeader {
        id: 0,
        hash: None,
        offset: i * MIB as u64,
        total: 0,
        last: false,
    }
}

/// Serves `sink`: reports the request's `name` once a handler starts it, then
/// counts its streamed body. Other ops reply `{len}` of their body.
fn reporting_sink(started: mpsc::Sender<String>) -> Arc<dyn Handler> {
    let started = Mutex::new(started);
    handler(move |req| {
        if req.op != "sink" {
            return req
                .responder
                .reply(json!({ "len": req.body.len() }), vec![]);
        }
        let name = req.header["name"].as_str().unwrap_or("?").to_owned();
        let _ = started.lock().unwrap().send(name);
        let (bytes, _) = drain_parts(&req);
        if !req.responder.cancelled() {
            req.responder.reply(json!({ "bytes": bytes }), vec![]);
        }
    })
}

/// Sends 1 MiB parts `first..parts`, then finishes.
fn send_parts(
    mut upload: tkfs::session::Upload,
    first: u64,
    parts: u64,
) -> Result<tkfs::session::Reply, CallError> {
    let data = vec![9u8; MIB];
    for i in first..parts {
        upload.chunk(mib_part(i), &data)?;
    }
    upload.finish()
}

fn assert_busy(error: &CallError) {
    match error {
        CallError::Remote(wire) if wire.code == "BUSY" => {
            assert_eq!(wire.class, ErrorClass::Contention, "{wire}");
            assert!(wire.retry_after_ms.is_some(), "{wire}");
            assert!(!error.is_uncertain());
        }
        other => panic!("expected BUSY, got {other:?}"),
    }
}

/// A streamed request waiting for a handler never holds the connection's
/// reads: once its parts do not fit the receive bound it is refused `BUSY`
/// (nothing ran), and the running stream it waits for keeps flowing. Before
/// fix round 2 its parts held the server's reads, the running stream's parts
/// waited unread behind the hold, and both uploads timed out at their idle
/// limit.
#[test]
fn a_queued_stream_never_holds_reads_for_running_ones() {
    let server_options = ConnOptions {
        handlers: 1,
        data_queue_bytes: 4 * MIB,
        ..ConnOptions::default()
    };
    let (client, server) = conns(
        ws_pair(Transport::Plain),
        ConnOptions::default(),
        server_options,
    );
    let (started, starts) = mpsc::channel();
    server.serve(reporting_sink(started));
    let idle = Duration::from_secs(5);
    let mut a = client
        .upload("sink", json!({ "name": "A" }), Priority::Data, idle)
        .unwrap();
    a.chunk(mib_part(0), &vec![9u8; MIB]).unwrap();
    assert_eq!(starts.recv_timeout(Duration::from_secs(5)).unwrap(), "A");
    // B's 16 MiB arrive while the only handler waits for A's next part.
    let begun = Instant::now();
    let b = client
        .upload("sink", json!({ "name": "B" }), Priority::Data, idle)
        .unwrap();
    let error = send_parts(b, 0, 16).unwrap_err();
    assert_busy(&error);
    assert!(
        begun.elapsed() < Duration::from_secs(3),
        "refused after {:?}",
        begun.elapsed()
    );
    // A was never starved.
    let reply = send_parts(a, 1, 16).unwrap();
    assert_eq!(reply.header["bytes"], 16 * MIB as u64);
    // Nothing ran for the refused B; retried once the handler is free, it
    // is served.
    let b = client
        .upload("sink", json!({ "name": "B" }), Priority::Data, idle)
        .unwrap();
    assert_eq!(
        send_parts(b, 0, 16).unwrap().header["bytes"],
        16 * MIB as u64
    );
    assert_eq!(starts.try_iter().collect::<Vec<_>>(), vec!["B".to_owned()]);
    assert!(client.closed().is_none() && server.closed().is_none());
}

/// A streamed request that fits the receive bound while it waits is served
/// in turn.
#[test]
fn a_queued_stream_that_fits_waits_its_turn() {
    let server_options = ConnOptions {
        handlers: 1,
        ..ConnOptions::default()
    };
    let (client, server) = conns(
        ws_pair(Transport::Plain),
        ConnOptions::default(),
        server_options,
    );
    let (started, starts) = mpsc::channel();
    server.serve(reporting_sink(started));
    let idle = Duration::from_secs(10);
    let mut a = client
        .upload("sink", json!({ "name": "A" }), Priority::Data, idle)
        .unwrap();
    a.chunk(mib_part(0), &vec![9u8; MIB]).unwrap();
    assert_eq!(starts.recv_timeout(Duration::from_secs(5)).unwrap(), "A");
    // B (8 MiB, within the 32 MiB bound) waits for the handler.
    let b = client
        .upload("sink", json!({ "name": "B" }), Priority::Data, idle)
        .unwrap();
    let b = thread::spawn(move || send_parts(b, 0, 8));
    wait_until("B to wait for the handler", Duration::from_secs(5), || {
        server.health().inflight == 2
    });
    assert_eq!(
        send_parts(a, 1, 16).unwrap().header["bytes"],
        16 * MIB as u64
    );
    assert_eq!(b.join().unwrap().unwrap().header["bytes"], 8 * MIB as u64);
    assert_eq!(starts.try_iter().collect::<Vec<_>>(), vec!["B".to_owned()]);
}

/// Bulk defaults (2 handlers, 32 MiB bound) with three concurrent 48 MiB
/// uploads: the running ones complete, the queued one completes or is
/// refused `BUSY` (never an idle timeout), and a retry is served.
#[test]
fn more_streams_than_handlers_are_served_or_refused_busy() {
    const PARTS: u64 = 48;
    let (client, server) = conns(
        ws_pair(Transport::Plain),
        ConnOptions::default(),
        ConnOptions::for_role(LaneRole::Bulk),
    );
    let (started, starts) = mpsc::channel();
    server.serve(reporting_sink(started));
    let idle = Duration::from_secs(10);
    let mut uploads = Vec::new();
    for n in 0..3 {
        let mut upload = client
            .upload(
                "sink",
                json!({ "name": format!("U{n}") }),
                Priority::Data,
                idle,
            )
            .unwrap();
        upload.chunk(mib_part(0), &vec![9u8; MIB]).unwrap();
        if n < 2 {
            // Both handlers are busy before the third upload arrives.
            starts.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        uploads.push(upload);
    }
    let senders: Vec<_> = uploads
        .into_iter()
        .map(|upload| thread::spawn(move || send_parts(upload, 1, PARTS)))
        .collect();
    let mut refused = false;
    for (n, sender) in senders.into_iter().enumerate() {
        match sender.join().unwrap() {
            Ok(reply) => assert_eq!(reply.header["bytes"], PARTS * MIB as u64),
            Err(error) => {
                assert_eq!(n, 2, "a running upload failed: {error:?}");
                assert_busy(&error);
                refused = true;
            }
        }
    }
    if refused {
        let upload = client
            .upload("sink", json!({ "name": "U2" }), Priority::Data, idle)
            .unwrap();
        assert_eq!(
            send_parts(upload, 0, PARTS).unwrap().header["bytes"],
            PARTS * MIB as u64
        );
    }
    assert!(client.closed().is_none() && server.closed().is_none());
}

/// `handler_queue: 0` means no request waits for a handler, not that none
/// is served: an idle handler takes it.
#[test]
fn zero_handler_queue_serves_while_a_handler_is_idle() {
    let options = ConnOptions {
        handlers: 2,
        handler_queue: 0,
        ..ConnOptions::default()
    };
    let (client, server) = conns(ws_pair(Transport::Plain), ConnOptions::default(), options);
    let (release, released) = mpsc::channel::<()>();
    let released = Mutex::new(released);
    server.serve(handler(move |req| {
        if req.op == "block" {
            let _ = released
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10));
        }
        req.responder.reply(json!({ "op": req.op }), vec![]);
    }));
    assert_eq!(
        call(&client, "echo", json!({})).unwrap().header["op"],
        "echo"
    );
    let blocked: Vec<_> = (0..2)
        .map(|_| {
            let client = client.clone();
            thread::spawn(move || call(&client, "block", json!({})))
        })
        .collect();
    wait_until("both handlers busy", Duration::from_secs(5), || {
        server.health().inflight == 2
    });
    assert_busy(&call(&client, "echo", json!({})).unwrap_err());
    release.send(()).unwrap();
    release.send(()).unwrap();
    for blocked in blocked {
        assert_eq!(blocked.join().unwrap().unwrap().header["op"], "block");
    }
    assert_eq!(
        call(&client, "echo", json!({})).unwrap().header["op"],
        "echo"
    );
}

/// While a server holds its reads for one slow stream, it sends PROGRESS for
/// the other streamed requests whose parts may wait unread behind the hold
/// (so their clients' idle windows restart), and none for the stream whose
/// own handler is behind.
#[test]
fn a_holding_server_sends_progress_for_streams_behind_the_hold() {
    let one_frame = ConnOptions {
        data_queue_bytes: 1,
        ..ConnOptions::default()
    };
    let (mut raw, server_ws) = ws_pair(Transport::Plain);
    let server = Conn::from_established(server_ws, Side::Server, peer(), one_frame).unwrap();
    server.serve(stream_service(Duration::ZERO));
    let streamed = |id: u64, op: &str, header: Value, idle_ms: u64| {
        let request = serde_json::to_vec(&RequestHeader {
            id,
            op: op.into(),
            deadline_ms: None,
            idle_ms: Some(idle_ms),
            header,
            total: None,
        })
        .unwrap();
        raw_frame(5, FLAG_MORE, 0, &request, b"")
    };
    let part = |id: u64, i: u64, len: usize| {
        let header = serde_json::to_vec(&ChunkHeader {
            id,
            hash: None,
            offset: i * len as u64,
            total: 0,
            last: false,
        })
        .unwrap();
        raw_frame(7, FLAG_MORE, 0, &header, &vec![1u8; len])
    };
    // The victim (id 1, idle 500 ms) is being served; then the culprit (id 2,
    // idle 10 s), whose handler reads 2 s late, fills the ~1 MiB receive
    // bound: 4 parts in its channel, 2 more over the bound, and the server
    // holds its reads. The victim is not timed out meanwhile.
    raw.send(streamed(1, "sink", json!({}), 500)).unwrap();
    raw.send(part(1, 0, 1024)).unwrap();
    wait_until("the victim to be served", Duration::from_secs(5), || {
        server.health().inflight == 1
    });
    raw.send(streamed(
        2,
        "late_reader",
        json!({ "delay_ms": 2000 }),
        10_000,
    ))
    .unwrap();
    for i in 0..6 {
        raw.send(part(2, i, MIB)).unwrap();
    }
    let mut progress = [0u32; 3];
    let started = Instant::now();
    if let MaybeTlsStream::Plain(stream) = raw.get_mut() {
        stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
    }
    while started.elapsed() < Duration::from_millis(1500) {
        match raw.read() {
            Ok(Message::Binary(bytes)) => {
                let frame = decode(&bytes, FrameLimits::SESSION).unwrap();
                assert_ne!(frame.kind, FrameKind::Response, "nothing is answered yet");
                if frame.kind == FrameKind::Progress {
                    let id = frame
                        .header_as::<tkfs::model::protocol::frame::ProgressHeader>()
                        .unwrap()
                        .id;
                    progress[id as usize] += 1;
                }
            }
            Ok(_) => {}
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(error) => panic!("{error}"),
        }
    }
    assert!(
        progress[1] >= 2,
        "PROGRESS for the stream behind the hold: {progress:?}"
    );
    assert_eq!(progress[2], 0, "none for the stream that is behind");
    assert!(server.closed().is_none());
}

/// A server's hold does not expire the client's idle limits of streams the
/// server already serves (PROGRESS) or whose frames still wait in the
/// client's own queue (the socket is full because the server holds its
/// reads): each idle limit (800 ms) is shorter than the hold (2 s).
#[test]
fn client_idle_limits_survive_a_server_hold() {
    let one_frame = ConnOptions {
        data_queue_bytes: 1,
        ..ConnOptions::default()
    };
    let idle = Duration::from_millis(800);
    let small = |i: u64| ChunkHeader {
        offset: i * 16 * 1024,
        ..mib_part(0)
    };
    // Known to the server before the hold: kept alive by PROGRESS.
    {
        let (client, server) = conns(ws_pair(Transport::Plain), ConnOptions::default(), one_frame);
        server.serve(stream_service(Duration::ZERO));
        let mut victim = client
            .upload("sink", json!({}), Priority::Data, idle)
            .unwrap();
        victim.chunk(small(0), &[5u8; 16 * 1024]).unwrap();
        wait_until("the victim to be served", Duration::from_secs(5), || {
            server.health().inflight == 1
        });
        let mut culprit = client
            .upload(
                "late_reader",
                json!({ "delay_ms": 2000 }),
                Priority::Data,
                Duration::from_secs(10),
            )
            .unwrap();
        for i in 0..8 {
            culprit.chunk(mib_part(i), &vec![6u8; MIB]).unwrap();
        }
        thread::sleep(Duration::from_millis(300));
        for i in 1..4 {
            victim.chunk(small(i), &[5u8; 16 * 1024]).unwrap();
        }
        let waited = Instant::now();
        let reply = victim.finish().unwrap();
        assert_eq!(reply.header["bytes"], 4 * 16 * 1024);
        assert!(waited.elapsed() > idle, "the hold ended early");
        assert_eq!(culprit.finish().unwrap().header["bytes"], 8 * MIB as u64);
    }
    // New during the hold, its frames still queued here behind 24 MiB.
    {
        let (client, server) = conns(ws_pair(Transport::Plain), ConnOptions::default(), one_frame);
        server.serve(stream_service(Duration::ZERO));
        let mut culprit = client
            .upload(
                "late_reader",
                json!({ "delay_ms": 2000 }),
                Priority::Data,
                Duration::from_secs(10),
            )
            .unwrap();
        for i in 0..24 {
            culprit.chunk(mib_part(i), &vec![6u8; MIB]).unwrap();
        }
        thread::sleep(Duration::from_millis(300));
        let waited = Instant::now();
        let mut victim = client
            .upload("sink", json!({}), Priority::Data, idle)
            .unwrap();
        for i in 0..4 {
            victim.chunk(small(i), &[5u8; 16 * 1024]).unwrap();
        }
        let reply = victim.finish().unwrap();
        assert_eq!(reply.header["bytes"], 4 * 16 * 1024);
        assert!(waited.elapsed() > idle, "the hold ended early");
        assert_eq!(culprit.finish().unwrap().header["bytes"], 24 * MIB as u64);
        assert!(client.closed().is_none() && server.closed().is_none());
    }
}

/// A batch larger than its queue's bound (admitted only by an empty queue)
/// is not starved by producers that keep the queue busy: waiters are
/// admitted in FIFO order. Before fix round 2 the 6 MiB request waited for
/// an empty data queue that two continuous uploads never left, and failed
/// `Busy` at its deadline.
#[test]
fn an_oversized_batch_is_not_starved_by_smaller_ones() {
    let (client_ws, server_ws, proxy) = ws_pair_proxied(Transport::Plain);
    let (client, server) = conns((client_ws, server_ws), small_queues(), small_queues());
    let (started, _starts) = mpsc::channel();
    server.serve(reporting_sink(started));
    // A slow link: producers refill the 4 MiB data queue far faster than it
    // drains.
    proxy.set_bandwidth(8 * MIB as u64);
    let stop = Arc::new(AtomicBool::new(false));
    let producers: Vec<_> = (0..2)
        .map(|n| {
            let client = client.clone();
            let stop = stop.clone();
            thread::spawn(move || {
                let mut upload = client
                    .upload(
                        "sink",
                        json!({ "name": format!("P{n}") }),
                        Priority::Data,
                        Duration::from_secs(30),
                    )
                    .unwrap();
                let data = vec![2u8; MIB];
                let mut i = 0;
                while !stop.load(Ordering::SeqCst) {
                    upload.chunk(mib_part(i), &data).unwrap();
                    i += 1;
                }
                upload.finish().unwrap();
            })
        })
        .collect();
    wait_until("the data queue to fill", Duration::from_secs(5), || {
        client.health().queued_bytes >= 3 * MIB as u64
    });
    let begun = Instant::now();
    let reply = client.call(
        "meta.exchange",
        json!({}),
        vec![1u8; 6 * MIB],
        Priority::Data,
        Duration::from_secs(10),
    );
    let waited = begun.elapsed();
    stop.store(true, Ordering::SeqCst);
    proxy.set_bandwidth(0);
    for producer in producers {
        producer.join().unwrap();
    }
    assert_eq!(reply.unwrap().header["len"], 6 * MIB, "after {waited:?}");
    assert!(client.closed().is_none() && server.closed().is_none());
}

/// A `Responder` can be shared with a progress thread (`&Responder` is
/// `Send`), as the WP0.4 stub allowed.
#[test]
fn responder_is_send_and_sync() {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<tkfs::session::Responder>();
    send_sync::<Conn>();
}

// ---------------------------------------------------------------------------
// Empty and header-only stream parts (WP1.INT fix round 1; invariant 22)

/// A header-only stream part whose header carries `pad` bytes of `hash`.
fn padded_part(pad: usize) -> ChunkHeader {
    ChunkHeader {
        id: 0,
        hash: Some("h".repeat(pad)),
        offset: 0,
        total: 0,
        last: false,
    }
}

/// Four 1 KiB parts, then six empty ones, then the end of the body.
fn data_then_empty_parts(
    mut upload: tkfs::session::Upload,
) -> Result<tkfs::session::Reply, CallError> {
    let part = |offset: u64| ChunkHeader {
        id: 0,
        hash: None,
        offset,
        total: 0,
        last: false,
    };
    for i in 0..4u64 {
        upload.chunk(part(i * 1024), &[7u8; 1024])?;
    }
    for _ in 0..6 {
        upload.chunk(part(4 * 1024), &[])?;
    }
    upload.finish()
}

/// Empty parts that find the handler's channel full wait behind the other
/// parts and are delivered in order, so the body ends and the handler
/// answers. Before WP1.INT fix round 1 they were counted as 0 bytes, the
/// delivery that keyed on the byte count never ran, and the upload stalled
/// until its idle limit (`REQUEST_TIMEOUT`), whether its handler was running
/// or still had to start.
#[test]
fn empty_parts_behind_a_full_channel_are_delivered() {
    let idle = Duration::from_secs(3);
    // Running: the handler took the request and reads late, so the four data
    // parts fill its channel and the empty ones wait in the overflow.
    {
        let (client, server) = session(Transport::Plain);
        server.serve(stream_service(Duration::ZERO));
        let upload = client
            .upload(
                "late_reader",
                json!({ "delay_ms": 500 }),
                Priority::Data,
                idle,
            )
            .unwrap();
        wait_until(
            "the handler to take the request",
            Duration::from_secs(5),
            || server.health().inflight == 1,
        );
        let begun = Instant::now();
        let reply = data_then_empty_parts(upload).unwrap();
        assert_eq!(reply.header["parts"], 10);
        assert_eq!(reply.header["bytes"], 4 * 1024);
        assert!(
            begun.elapsed() < idle,
            "answered after {:?}",
            begun.elapsed()
        );
    }
    // Waiting: the only handler is busy, so the parts wait with the request
    // (the empty ones in its overflow) until a handler takes it.
    {
        let one_handler = ConnOptions {
            handlers: 1,
            ..ConnOptions::default()
        };
        let (client, server) = conns(
            ws_pair(Transport::Plain),
            ConnOptions::default(),
            one_handler,
        );
        server.serve(stream_service(Duration::ZERO));
        let busy = client
            .upload(
                "late_reader",
                json!({ "delay_ms": 800 }),
                Priority::Data,
                idle,
            )
            .unwrap();
        let busy = thread::spawn(move || busy.finish());
        wait_until(
            "the handler to take the first request",
            Duration::from_secs(5),
            || server.health().inflight == 1,
        );
        let upload = client
            .upload("late_reader", json!({}), Priority::Data, idle)
            .unwrap();
        wait_until(
            "the second request to wait for the handler",
            Duration::from_secs(5),
            || server.health().inflight == 2,
        );
        let begun = Instant::now();
        let reply = data_then_empty_parts(upload).unwrap();
        assert_eq!(reply.header["parts"], 10);
        assert_eq!(reply.header["bytes"], 4 * 1024);
        assert!(
            begun.elapsed() < idle,
            "answered after {:?}",
            begun.elapsed()
        );
        assert_eq!(busy.join().unwrap().unwrap().header["parts"], 0);
        assert!(client.closed().is_none() && server.closed().is_none());
    }
}

/// Header-only parts are charged to the receive bound like data (frame
/// bytes plus a fixed overhead): a peer streaming them to a handler that
/// takes none is held back by TCP once the bound is reached, the stream is
/// judged stalled, and the connection keeps serving. Before WP1.INT fix round
/// 1 they were charged nothing and the server read and buffered them without
/// limit (the WP1.9 review measured +0.8–1.9 GiB).
#[test]
fn header_only_parts_are_charged_to_the_receive_bound() {
    let small = ConnOptions {
        data_queue_bytes: MIB,
        ..ConnOptions::default()
    };
    let (client, server) = conns(ws_pair(Transport::Plain), small, small);
    server.serve(stream_service(Duration::ZERO));
    let before = server.health().bytes_down;
    let mut upload = client
        .upload(
            "late_reader",
            json!({ "delay_ms": 5000 }),
            Priority::Data,
            Duration::from_secs(3),
        )
        .unwrap();
    wait_until(
        "the handler to take the request",
        Duration::from_secs(5),
        || server.health().inflight == 1,
    );
    // 4,096 parts of ~16 KiB headers: 64 MiB if nothing held them back.
    const PARTS: usize = 4096;
    let sent = Arc::new(AtomicUsize::new(0));
    let sender = {
        let sent = sent.clone();
        thread::spawn(move || {
            let header = padded_part(16 * 1024);
            for _ in 0..PARTS {
                upload.chunk(header.clone(), &[])?;
                sent.fetch_add(1, Ordering::SeqCst);
            }
            upload.finish()
        })
    };
    // Before the stream's 3 s idle limit can end the hold.
    thread::sleep(Duration::from_secs(2));
    let received = server.health().bytes_down - before;
    let accepted = sent.load(Ordering::SeqCst);
    assert!(
        received < 4 * MIB as u64,
        "the server read {received} bytes of header-only parts ({accepted} parts queued)"
    );
    assert!(accepted < PARTS, "the sender was never held back");
    // The handler takes nothing for the idle limit: the stream ends with an
    // error on one side or the other, and that ends the hold.
    match sender.join().unwrap() {
        Err(CallError::Timeout { .. }) => {}
        Err(CallError::Remote(error)) if error.code == "REQUEST_TIMEOUT" => {}
        other => panic!("expected a timeout, got {other:?}"),
    }
    assert_eq!(
        call(&client, "echo", json!({ "after": 1 })).unwrap().header["after"],
        1
    );
    assert!(client.closed().is_none() && server.closed().is_none());
}

/// The client side charges header-only reply chunks the same way: a slow
/// callback holds the client's reads instead of buffering them without
/// limit, and every chunk is delivered once it catches up.
#[test]
fn header_only_stream_chunks_are_charged_to_the_receive_bound() {
    const CHUNKS: u64 = 1024;
    let small = ConnOptions {
        data_queue_bytes: MIB,
        ..ConnOptions::default()
    };
    let (client, server) = conns(ws_pair(Transport::Plain), small, ConnOptions::default());
    server.serve(handler(|req| {
        let header = padded_part(16 * 1024);
        for _ in 0..CHUNKS {
            if req.responder.chunk(header.clone(), &[]).is_err() {
                return;
            }
        }
        req.responder.reply(json!({ "chunks": CHUNKS }), vec![]);
    }));
    let before = client.health().bytes_down;
    let (sleeping, asleep) = mpsc::channel();
    let caller = {
        let client = client.clone();
        thread::spawn(move || {
            let mut seen = 0u64;
            let reply = {
                let mut on_chunk = |_: ChunkHeader, body: &[u8]| -> Result<(), ErrorWire> {
                    assert!(body.is_empty());
                    if seen == 0 {
                        sleeping.send(()).unwrap();
                        thread::sleep(Duration::from_millis(2000));
                    }
                    seen += 1;
                    Ok(())
                };
                client.call_streaming(
                    "flood",
                    json!({}),
                    vec![],
                    Priority::Control,
                    None,
                    Duration::from_secs(10),
                    &mut on_chunk,
                )
            };
            (reply, seen)
        })
    };
    asleep.recv_timeout(Duration::from_secs(5)).unwrap();
    thread::sleep(Duration::from_millis(1000));
    let received = client.health().bytes_down - before;
    assert!(
        received < 4 * MIB as u64,
        "the client read {received} bytes of header-only chunks behind a sleeping callback"
    );
    let (reply, seen) = caller.join().unwrap();
    assert_eq!(reply.unwrap().header["chunks"], CHUNKS);
    assert_eq!(seen, CHUNKS);
    assert!(client.closed().is_none() && server.closed().is_none());
}

// ---------------------------------------------------------------------------
// GOAWAY

#[test]
fn goaway_drains_in_flight_and_refuses_new_calls() {
    let (client, server) = session(Transport::Plain);
    server.serve(handler(|req| {
        if req.op == "slow" {
            thread::sleep(Duration::from_millis(500));
        }
        req.responder.reply(json!({ "op": req.op }), vec![]);
    }));
    let in_flight = {
        let client = client.clone();
        thread::spawn(move || call(&client, "slow", json!({})))
    };
    wait_until("the slow request", Duration::from_secs(5), || {
        server.health().inflight == 1
    });
    server.goaway("PAIRING_CODE_CHANGED", "the pairing code was rotated");
    // The server refuses new work at once; the client learns of it shortly.
    wait_until("the client to see GOAWAY", Duration::from_secs(2), || {
        call(&client, "echo", json!({})).is_err()
    });
    let error = call(&client, "echo", json!({})).unwrap_err();
    assert!(matches!(error, CallError::Unavailable(_)), "{error:?}");
    assert_eq!(error.code(), "PAIRING_CODE_CHANGED");
    assert_eq!(error.class(), ErrorClass::NeedsUserAction);
    // The request in flight completes.
    assert_eq!(in_flight.join().unwrap().unwrap().header["op"], "slow");
    let closed = server.wait_closed(Duration::from_secs(3)).unwrap();
    assert_eq!(closed.code, "PAIRING_CODE_CHANGED");
    let closed = client.wait_closed(Duration::from_secs(3)).unwrap();
    assert_eq!(closed.code, "PAIRING_CODE_CHANGED");
}

#[test]
fn goaway_closes_after_the_grace_period() {
    let (client, server) = session(Transport::Plain);
    server.serve(handler(|req| {
        while !req.responder.cancelled() {
            thread::sleep(Duration::from_millis(5));
        }
    }));
    let in_flight = {
        let client = client.clone();
        thread::spawn(move || {
            let result = call(&client, "forever", json!({}));
            (result, Instant::now())
        })
    };
    wait_until("the request", Duration::from_secs(5), || {
        server.health().inflight == 1
    });
    let started = Instant::now();
    client.goaway("ORCHESTRATOR_STOPPING", "shutting down");
    let (result, at) = in_flight.join().unwrap();
    let error = result.unwrap_err();
    assert!(matches!(error, CallError::ConnectionLost(_)), "{error:?}");
    let waited = at - started;
    assert!(
        waited >= Duration::from_millis(4800) && waited < Duration::from_secs(7),
        "{waited:?}"
    );
    assert_eq!(
        client.wait_closed(Duration::from_secs(2)).unwrap().code,
        "ORCHESTRATOR_STOPPING"
    );
}

// ---------------------------------------------------------------------------
// Protocol violations close the connection (invariant 22)

/// Sends `message` from a raw client to a `Conn` server, which must close with
/// `PROTOCOL_VIOLATION` and tell the raw peer with GOAWAY.
fn server_rejects(message: Message, what: &str) {
    server_rejects_with(message, what, true);
}

/// As [`server_rejects`]; `goaway: false` skips the GOAWAY check for messages
/// the server stops reading half way (the unread rest may reset the socket
/// before the raw peer reads the GOAWAY).
fn server_rejects_with(message: Message, what: &str, goaway: bool) {
    let (mut raw, server_ws) = ws_pair(Transport::Plain);
    let server =
        Conn::from_established(server_ws, Side::Server, peer(), ConnOptions::default()).unwrap();
    server.serve(echo());
    let _ = raw.send(message);
    let closed = server
        .wait_closed(Duration::from_secs(5))
        .unwrap_or_else(|| panic!("{what}: the connection stayed open"));
    assert_eq!(closed.code, "PROTOCOL_VIOLATION", "{what}: {closed}");
    if !goaway {
        return;
    }
    let mut goaway = None;
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(5) {
        match raw.read() {
            Ok(Message::Binary(bytes)) => {
                let frame = decode(&bytes, FrameLimits::SESSION).unwrap();
                if frame.kind == FrameKind::Goaway {
                    goaway = Some(frame.header_as::<Goaway>().unwrap());
                }
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }
    let goaway = goaway.unwrap_or_else(|| panic!("{what}: no GOAWAY"));
    assert_eq!(goaway.code, "PROTOCOL_VIOLATION", "{what}");
}

fn raw_frame(kind: u8, flags: u8, reserved: u16, header: &[u8], body: &[u8]) -> Message {
    let mut bytes = vec![kind, flags];
    bytes.extend_from_slice(&reserved.to_le_bytes());
    bytes.extend_from_slice(&(header.len() as u32).to_le_bytes());
    bytes.extend_from_slice(header);
    bytes.extend_from_slice(body);
    Message::Binary(bytes.into())
}

fn request_header(id: u64, op: &str, total: Option<u64>) -> Vec<u8> {
    serde_json::to_vec(&RequestHeader {
        id,
        op: op.into(),
        deadline_ms: Some(5000),
        idle_ms: None,
        header: json!({}),
        total,
    })
    .unwrap()
}

#[test]
fn protocol_violations_close_the_connection() {
    let request = request_header(1, "echo", None);
    let heartbeat = serde_json::to_vec(&Heartbeat::default()).unwrap();
    server_rejects(raw_frame(12, 0, 0, b"{}", b""), "unknown kind");
    server_rejects(raw_frame(0, 0, 0, b"{}", b""), "kind zero");
    server_rejects(raw_frame(5, 0, 1, &request, b""), "non-zero reserved");
    server_rejects(raw_frame(5, 0b100, 0, &request, b""), "unknown flag");
    server_rejects(
        raw_frame(5, FLAG_LAST, 0, &request, b""),
        "LAST on a request",
    );
    server_rejects(raw_frame(10, 0, 0, &heartbeat, b"x"), "trailing bytes");
    server_rejects(raw_frame(5, 0, 0, b"{\"id\":", b""), "invalid header");
    server_rejects(raw_frame(5, 0, 0, b"[1,2]", b""), "non-object header");
    server_rejects(raw_frame(5, 0, 0, b"{}", b""), "header missing fields");
    server_rejects(Message::Binary(vec![5, 0, 0].into()), "truncated prefix");
    let mut truncated = vec![5, 0, 0, 0];
    truncated.extend_from_slice(&100u32.to_le_bytes());
    truncated.extend_from_slice(b"{}");
    server_rejects(Message::Binary(truncated.into()), "truncated header");
    let big = serde_json::to_vec(&json!({ "pad": "x".repeat(MAX_HEADER) })).unwrap();
    server_rejects(raw_frame(5, 0, 0, &big, b""), "oversize header");
    server_rejects(
        raw_frame(
            7,
            0,
            0,
            b"{\"id\":1,\"offset\":0,\"total\":0,\"last\":false}",
            &vec![0; MAX_CHUNK + 1],
        ),
        "oversize body",
    );
    server_rejects_with(
        Message::Binary(vec![0u8; 8 + MAX_HEADER + MAX_CHUNK + 1].into()),
        "oversize message",
        false,
    );
    server_rejects(Message::Text("{\"op\":\"pair\"}".into()), "text message");
    let response = serde_json::to_vec(&ResponseHeader {
        id: 1,
        ok: true,
        header: json!({}),
        error: None,
        total: None,
    })
    .unwrap();
    server_rejects(
        raw_frame(6, 0, 0, &response, b""),
        "response sent to the server",
    );
    server_rejects(raw_frame(1, 0, 0, b"{}", b""), "hello after authentication");
    // Id 0 is reserved (this side's HEARTBEAT and GOAWAY are queued under it,
    // and a CANCEL or expiry of request 0 would purge them).
    server_rejects(
        raw_frame(5, 0, 0, &request_header(0, "echo", None), b""),
        "request id 0",
    );
    server_rejects(raw_frame(9, 0, 0, b"{\"id\":0}", b""), "cancel id 0");
    server_rejects(
        raw_frame(
            7,
            FLAG_MORE,
            0,
            b"{\"id\":0,\"offset\":0,\"total\":0,\"last\":false}",
            b"x",
        ),
        "chunk id 0",
    );
    // A body announced at 2 MiB for an op whose limit is 1 MiB.
    server_rejects(
        raw_frame(
            5,
            FLAG_MORE,
            0,
            &request_header(1, "echo", Some(2 * MIB as u64)),
            &[0; 10],
        ),
        "body over the op limit",
    );
    // A continuation at the wrong offset.
    let (mut raw, server_ws) = ws_pair(Transport::Plain);
    let server =
        Conn::from_established(server_ws, Side::Server, peer(), ConnOptions::default()).unwrap();
    server.serve(echo());
    raw.send(raw_frame(
        5,
        FLAG_MORE,
        0,
        &request_header(1, "meta.exchange", Some(3 * MIB as u64)),
        &vec![0; MIB],
    ))
    .unwrap();
    let chunk = serde_json::to_vec(&ChunkHeader {
        id: 1,
        hash: None,
        offset: 5,
        total: 3 * MIB as u64,
        last: false,
    })
    .unwrap();
    raw.send(raw_frame(7, FLAG_MORE, 0, &chunk, &vec![0; MIB]))
        .unwrap();
    let closed = server.wait_closed(Duration::from_secs(5)).unwrap();
    assert_eq!(closed.code, "PROTOCOL_VIOLATION", "{closed}");
    // A duplicate request id while the first is still being served.
    let (mut raw, server_ws) = ws_pair(Transport::Plain);
    let server =
        Conn::from_established(server_ws, Side::Server, peer(), ConnOptions::default()).unwrap();
    server.serve(handler(|req| {
        while !req.responder.cancelled() {
            thread::sleep(Duration::from_millis(5));
        }
    }));
    let request = request_header(9, "hold", None);
    raw.send(raw_frame(5, 0, 0, &request, b"")).unwrap();
    raw.send(raw_frame(5, 0, 0, &request, b"")).unwrap();
    let closed = server.wait_closed(Duration::from_secs(5)).unwrap();
    assert_eq!(closed.code, "PROTOCOL_VIOLATION", "{closed}");
}

/// The client side refuses frames only a client may send, and responses to
/// requests it never sent.
#[test]
fn client_rejects_server_violations() {
    let cases: Vec<(Message, &str)> = vec![
        (
            raw_frame(5, 0, 0, &request_header(1, "echo", None), b""),
            "request sent to the client",
        ),
        (
            raw_frame(
                6,
                0,
                0,
                &serde_json::to_vec(&ResponseHeader {
                    id: 77,
                    ok: true,
                    header: json!({}),
                    error: None,
                    total: None,
                })
                .unwrap(),
                b"",
            ),
            "response to a request never sent",
        ),
        (
            raw_frame(9, 0, 0, b"{\"id\":1}", b""),
            "cancel sent to the client",
        ),
    ];
    for (message, what) in cases {
        let (client_ws, mut raw) = ws_pair(Transport::Plain);
        let client =
            Conn::from_established(client_ws, Side::Client, peer(), ConnOptions::default())
                .unwrap();
        raw.send(message).unwrap();
        let closed = client
            .wait_closed(Duration::from_secs(5))
            .unwrap_or_else(|| panic!("{what}: the connection stayed open"));
        assert_eq!(closed.code, "PROTOCOL_VIOLATION", "{what}: {closed}");
    }
}

/// Exact encodings the raw peers above rely on.
#[test]
fn raw_frame_layout_matches_the_codec() {
    let header = request_header(4, "echo", None);
    let Message::Binary(raw) = raw_frame(5, 0, 0, &header, b"abc") else {
        unreachable!()
    };
    assert_eq!(
        raw.to_vec(),
        encode_parts(FrameKind::Request, 0, &header, b"abc", FrameLimits::SESSION).unwrap()
    );
}
