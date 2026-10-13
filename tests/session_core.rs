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

/// A raw peer that ignores CANCEL and answers the timed-out request anyway:
/// the client drops the late reply and the next call pairs correctly.
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
        let first = read(&mut raw);
        assert_eq!(first.kind, FrameKind::Request);
        let first: RequestHeader = first.header_as().unwrap();
        let cancel = read(&mut raw);
        assert_eq!(
            cancel.kind,
            FrameKind::Cancel,
            "a timed-out call sends CANCEL"
        );
        assert_eq!(cancel.header_as::<CancelHeader>().unwrap().id, first.id);
        let late = ResponseHeader {
            id: first.id,
            ok: true,
            header: json!({ "late": true }),
            error: None,
            total: None,
        };
        let late = encode_with(FrameKind::Response, 0, &late, &[], FrameLimits::SESSION).unwrap();
        raw.send(Message::Binary(late.into())).unwrap();
        let second = read(&mut raw);
        let second: RequestHeader = second.header_as().unwrap();
        assert_ne!(second.id, first.id);
        let reply = ResponseHeader {
            id: second.id,
            ok: true,
            header: second.header,
            error: None,
            total: None,
        };
        let reply = encode_with(FrameKind::Response, 0, &reply, &[], FrameLimits::SESSION).unwrap();
        raw.send(Message::Binary(reply.into())).unwrap();
        raw
    });
    let error = client
        .call(
            "hang",
            json!({}),
            vec![],
            Priority::Control,
            Duration::from_secs(1),
        )
        .unwrap_err();
    assert!(matches!(error, CallError::Timeout { .. }), "{error:?}");
    let reply = call(&client, "echo", json!({ "second": true })).unwrap();
    assert_eq!(reply.header, json!({ "second": true }));
    let _raw = server.join().unwrap();
    assert!(client.closed().is_none());
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

/// Serves `download` (streams `count` chunks of `size` bytes, then replies with
/// the digest) and `upload` (consumes `Incoming::chunks`, replies with the byte
/// count and digest), optionally sleeping per upload chunk.
fn stream_service(upload_delay: Duration) -> Arc<dyn Handler> {
    handler(move |req| match req.op.as_str() {
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

fn control_fast_while_data_queued(transport: Transport) {
    let (client, server) = session(transport);
    server.serve(stream_service(Duration::ZERO));
    let mut samples = Vec::new();
    for round in 0..5u64 {
        let mut upload = client
            .upload("upload", json!({}), Priority::Data, Duration::from_secs(30))
            .unwrap();
        let data = pattern(round, MIB);
        for i in 0..30u64 {
            upload
                .chunk(
                    ChunkHeader {
                        id: 0,
                        hash: None,
                        offset: i * MIB as u64,
                        total: 30 * MIB as u64,
                        last: i == 29,
                    },
                    &data,
                )
                .unwrap();
        }
        let queued = client.health().queued_bytes;
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
        let elapsed = started.elapsed();
        assert_eq!(reply.header["round"], round);
        assert!(
            queued >= 8 * MIB as u64,
            "only {queued} bytes were queued when the control call started"
        );
        samples.push(elapsed);
        let reply = upload.finish().unwrap();
        assert_eq!(reply.header["bytes"], 30 * MIB as u64);
    }
    samples.sort();
    let median = samples[samples.len() / 2];
    assert!(
        median < latency_bound(50),
        "control latency with 30 MiB queued: {samples:?}"
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
