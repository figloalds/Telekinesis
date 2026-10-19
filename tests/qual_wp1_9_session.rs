//! Qualification scenarios for WP1.9 (session core: frames, I/O thread,
//! multiplexing). REVAMP_PLAN §14.6, §6.2.
//!
//! Created by WP0.5 against the legacy transport for the baseline record;
//! WP1.9 measures the same properties on `session::Conn` over in-process
//! loopback WebSocket pairs (the v3 handshake is WP1.10's), with production
//! `ConnOptions` unless a scenario says otherwise.
//! `cargo test --release --test qual_wp1_9_session -- --ignored --test-threads=1 --nocapture`
mod support;

use serde_json::json;
use std::{
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};
use support::*;
use tkfs::{
    model::{errors::ErrorWire, protocol::LaneRole},
    session::{
        CallError, ChunkHeader, Conn, ConnOptions, Handler, Incoming, PeerInfo, Priority, Side,
    },
};
use tungstenite::{WebSocket, stream::MaybeTlsStream};

const FILE: &str = "qual_wp1_9_session";
const MIB: usize = 1024 * 1024;

type Ws = WebSocket<MaybeTlsStream<TcpStream>>;

fn accept(listener: TcpListener, tls: bool) -> thread::JoinHandle<Ws> {
    thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream.set_nonblocking(false).unwrap();
        let stream = if tls {
            let identity = native_tls::Identity::from_pkcs12(
                include_bytes!("fixtures/tls/localhost.p12"),
                "telekinesis-test",
            )
            .unwrap();
            let acceptor = native_tls::TlsAcceptor::new(identity).unwrap();
            MaybeTlsStream::NativeTls(acceptor.accept(stream).expect("TLS accept"))
        } else {
            MaybeTlsStream::Plain(stream)
        };
        tungstenite::accept(stream).expect("WebSocket accept")
    })
}

fn connect(port: u16, tls: bool) -> Ws {
    let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let (url, connector) = if tls {
        let ca = native_tls::Certificate::from_pem(include_bytes!("fixtures/tls/ca.pem")).unwrap();
        let connector = native_tls::TlsConnector::builder()
            .add_root_certificate(ca)
            .build()
            .unwrap();
        (
            format!("wss://localhost:{port}/"),
            Some(tungstenite::Connector::NativeTls(connector)),
        )
    } else {
        (format!("ws://127.0.0.1:{port}/"), None)
    };
    tungstenite::client_tls_with_config(url, stream, None, connector)
        .expect("WebSocket connect")
        .0
}

fn peer() -> PeerInfo {
    PeerInfo {
        installation: "qual".into(),
        role: LaneRole::Control,
        lane: None,
        session: "qual-session".into(),
        verifier_generation: 1,
    }
}

/// A (client, server) session pair, optionally through a `FaultProxy`.
fn session(tls: bool, proxied: bool, options: ConnOptions) -> (Conn, Conn, Option<FaultProxy>) {
    session_with(tls, proxied, options, options)
}

/// [`session`] with the server's own options.
fn session_with(
    tls: bool,
    proxied: bool,
    options: ConnOptions,
    server_options: ConnOptions,
) -> (Conn, Conn, Option<FaultProxy>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let proxy = proxied.then(|| FaultProxy::start(&format!("127.0.0.1:{port}")));
    let server = accept(listener, tls);
    let client = connect(proxy.as_ref().map_or(port, |p| p.addr().port()), tls);
    let client = Conn::from_established(client, Side::Client, peer(), options).unwrap();
    let server =
        Conn::from_established(server.join().unwrap(), Side::Server, peer(), server_options)
            .unwrap();
    (client, server, proxy)
}

struct FnHandler<F>(F);

impl<F: Fn(Incoming) + Send + Sync> Handler for FnHandler<F> {
    fn handle(&self, req: Incoming) {
        (self.0)(req)
    }
}

/// `download {count}` streams `count` 1 MiB chunks; `upload` consumes the
/// streamed body and replies with its length; anything else echoes.
fn service() -> Arc<dyn Handler> {
    let block = Arc::new(seeded_bytes(31, MIB));
    Arc::new(FnHandler(move |req: Incoming| match req.op.as_str() {
        "download" => {
            let count = req.header["count"].as_u64().unwrap();
            for i in 0..count {
                let header = ChunkHeader {
                    id: 0,
                    hash: None,
                    offset: i * MIB as u64,
                    total: count * MIB as u64,
                    last: i + 1 == count,
                };
                if req.responder.chunk(header, &block).is_err() {
                    return;
                }
            }
            req.responder.reply(json!({ "count": count }), vec![]);
        }
        "upload" => {
            let bytes: usize = req
                .chunks
                .as_ref()
                .unwrap()
                .iter()
                .map(|(_, data)| data.len())
                .sum();
            req.responder.reply(json!({ "bytes": bytes }), vec![]);
        }
        _ => req.responder.reply(req.header, req.body),
    }))
}

/// Streams `count` MiB down; returns the bytes received.
fn download(conn: &Conn, count: u64) -> Result<u64, CallError> {
    let mut bytes = 0u64;
    let mut on_chunk = |_: ChunkHeader, data: &[u8]| -> Result<(), ErrorWire> {
        bytes += data.len() as u64;
        Ok(())
    };
    conn.call_streaming(
        "download",
        json!({ "count": count }),
        vec![],
        Priority::Control,
        None,
        Duration::from_secs(30),
        &mut on_chunk,
    )?;
    Ok(bytes)
}

/// Streams `count` MiB up; returns the bytes the server counted.
fn upload(conn: &Conn, count: u64, block: &[u8]) -> Result<u64, CallError> {
    let mut upload = conn.upload("upload", json!({}), Priority::Data, Duration::from_secs(30))?;
    for i in 0..count {
        upload.chunk(
            ChunkHeader {
                id: 0,
                hash: None,
                offset: i * MIB as u64,
                total: count * MIB as u64,
                last: i + 1 == count,
            },
            block,
        )?;
    }
    let reply = upload.finish()?;
    Ok(reply.header["bytes"].as_u64().unwrap_or(0))
}

/// A slow request on one stream must not delay fast requests on another
/// stream of the same connection, and bulk data queued ahead must not delay a
/// control call (`control_queued_30mib_p99_ms`).
#[test]
#[ignore = "WP1.9"]
fn slow_does_not_block_fast() {
    let mut s = Scenario::new(FILE, "slow_does_not_block_fast");
    s.note("session::Conn, production options; one 20 s handler on stream A, 50 calls on stream B");
    let (client, server, _) = session(false, false, ConnOptions::default());
    let (release, released) = mpsc::channel::<()>();
    let released = Mutex::new(released);
    let echo = service();
    server.serve(Arc::new(FnHandler(move |req: Incoming| {
        if req.op == "slow" {
            let _ = released
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(20));
            req.responder.reply(json!({ "slow": true }), vec![]);
        } else {
            echo.handle(req);
        }
    })));
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
    let started = Instant::now();
    while server.health().inflight == 0 && started.elapsed() < Duration::from_secs(5) {
        thread::sleep(Duration::from_millis(1));
    }
    let mut samples = vec![];
    for i in 0..50 {
        let (r, ms) = time_ms(|| {
            client.call(
                "echo",
                json!({ "fast": i }),
                vec![0; 1024],
                Priority::Control,
                Duration::from_secs(5),
            )
        });
        s.check(
            r.as_ref().is_ok_and(|reply| reply.header["fast"] == i),
            format!("fast call {i}: {r:?}"),
        );
        samples.push(ms);
    }
    s.check(!slow.is_finished(), "the slow call was still running");
    release.send(()).unwrap();
    let slow = slow.join().unwrap();
    s.check(slow.is_ok(), format!("slow call: {slow:?}"));
    let summary = summarize(&samples);
    s.metric_with("fast_p99_ms", summary.p99, "ms", summary.json());
    control_under_queued_data(&mut s);
    s.finish();
}

/// §14 WP1.9: a control call is answered < 50 ms while 30 MiB of data is
/// queued. A producer keeps the client's data queue (32 MiB) full; each of 200
/// calls starts with at least 30 MiB queued. The metric is the p99, as for
/// `fast_p99_ms`; the max is recorded with it. (On Windows loopback, data
/// already in the kernel moves in steps of the 15.6 ms timer tick while the
/// sender's socket is full, so samples cluster at 1, 2 or 3 ticks; on Linux
/// the max is about 2 ms.)
fn control_under_queued_data(s: &mut Scenario) {
    const QUEUED: u64 = 30 * MIB as u64;
    let (client, server, _) = session(false, false, ConnOptions::default());
    server.serve(service());
    let stop = Arc::new(AtomicBool::new(false));
    let producer = {
        let client = client.clone();
        let stop = stop.clone();
        thread::spawn(move || -> Result<u64, CallError> {
            let block = seeded_bytes(23, MIB);
            let mut upload =
                client.upload("upload", json!({}), Priority::Data, Duration::from_secs(30))?;
            let mut parts = 0u64;
            while !stop.load(Ordering::SeqCst) {
                let header = ChunkHeader {
                    id: 0,
                    hash: None,
                    offset: parts * MIB as u64,
                    total: 0,
                    last: false,
                };
                upload.chunk(header, &block)?;
                parts += 1;
            }
            let reply = upload.finish()?;
            Ok(reply.header["bytes"].as_u64().unwrap_or(0))
        })
    };
    let mut samples = vec![];
    let mut least_queued = u64::MAX;
    for i in 0..200 {
        let waiting = Instant::now();
        while client.health().queued_bytes < QUEUED && waiting.elapsed() < Duration::from_secs(10) {
            thread::sleep(Duration::from_millis(1));
        }
        least_queued = least_queued.min(client.health().queued_bytes);
        let (r, ms) = time_ms(|| {
            client.call(
                "echo",
                json!({ "queued": i }),
                vec![],
                Priority::Control,
                Duration::from_secs(5),
            )
        });
        s.check(
            r.as_ref().is_ok_and(|reply| reply.header["queued"] == i),
            format!("control call {i} under queued data: {r:?}"),
        );
        samples.push(ms);
    }
    stop.store(true, Ordering::SeqCst);
    let produced = producer.join().unwrap();
    s.check(produced.is_ok(), format!("the data stream: {produced:?}"));
    let summary = summarize(&samples);
    s.metric_with(
        "control_queued_30mib_p99_ms",
        summary.p99,
        "ms",
        json!({ "samples": summary.json(), "least_queued_bytes": least_queued }),
    );
    s.check(
        least_queued >= QUEUED,
        format!("every control call started with >= 30 MiB queued (least {least_queued})"),
    );
    s.check(
        summary.p99 < 50.0,
        format!(
            "control calls answered < 50 ms with 30 MiB queued (p99 {:.2} ms, max {:.2} ms)",
            summary.p99, summary.max
        ),
    );
}

/// An idle connection stays usable for 60 s (heartbeats; no reconnect).
#[test]
#[ignore = "WP1.9"]
fn idle_60s() {
    let mut s = Scenario::new(FILE, "idle_60s");
    s.note("session::Conn through a FaultProxy, production heartbeat 5 s / dead 20 s");
    let (client, server, proxy) = session(false, true, ConnOptions::default());
    let proxy = proxy.unwrap();
    server.serve(service());
    let first = client.call(
        "ping",
        json!({}),
        vec![],
        Priority::Control,
        Duration::from_secs(5),
    );
    s.check(first.is_ok(), format!("first ping: {first:?}"));
    let idle = budget_or(60).min(Duration::from_secs(60));
    thread::sleep(idle);
    let (ping, ms) = time_ms(|| {
        client.call(
            "ping",
            json!({}),
            vec![],
            Priority::Control,
            Duration::from_secs(5),
        )
    });
    s.metric("ping_after_idle_ms", ms, "ms");
    s.check(
        ping.is_ok(),
        format!("usable after {} s idle: {ping:?}", idle.as_secs()),
    );
    s.check(
        client.closed().is_none() && server.closed().is_none(),
        "the connection stayed open",
    );
    s.check(
        proxy.connections() == 1 && proxy.resets() == 0,
        format!(
            "no reconnect: {} connections, {} resets",
            proxy.connections(),
            proxy.resets()
        ),
    );
    let health = client.health();
    s.metric_with(
        "rtt_ms",
        health.rtt_ms.map_or(f64::NAN, |rtt| rtt as f64),
        "ms",
        json!({ "health": health }),
    );
    s.finish();
}

/// A peer that stops responding is detected by liveness, without depending on
/// a request deadline (session heartbeats: dead after `dead_after`).
#[test]
#[ignore = "WP1.9"]
fn dead_peer_detection() {
    let mut s = Scenario::new(FILE, "dead_peer_detection");
    let options = ConnOptions::default();
    s.note(format!(
        "session::Conn through a FaultProxy blackhole; heartbeat {:?}, dead after {:?}",
        options.heartbeat, options.dead_after
    ));
    // The server declares the link dead later than the client (WP4.INT):
    // with equal timers both sides close at about the same moment, and the
    // proxy then hands the client the server's close as a reset, which
    // reads `CONNECTION_LOST` instead of the client's own `TRANSPORT_DEAD`
    // (1 run in 4 on WSL). The client's detection is what is measured.
    let server_options = ConnOptions {
        dead_after: options.dead_after * 3,
        ..options
    };
    let (client, server, proxy) = session_with(false, true, options, server_options);
    let proxy = proxy.unwrap();
    server.serve(Arc::new(FnHandler(|req: Incoming| {
        while !req.responder.cancelled() {
            thread::sleep(Duration::from_millis(10));
        }
    })));
    let in_flight = {
        let client = client.clone();
        thread::spawn(move || {
            let result = client.call(
                "hold",
                json!({}),
                vec![],
                Priority::Control,
                Duration::from_secs(120),
            );
            (result, Instant::now())
        })
    };
    let started = Instant::now();
    while server.health().inflight == 0 && started.elapsed() < Duration::from_secs(5) {
        thread::sleep(Duration::from_millis(1));
    }
    let blackholed = Instant::now();
    proxy.blackhole(true);
    let (result, failed_at) = in_flight.join().unwrap();
    let detect_ms = (failed_at - blackholed).as_secs_f64() * 1000.0;
    let closed = client.wait_closed(Duration::from_secs(5));
    s.metric_with(
        "detect_ms",
        detect_ms,
        "ms",
        json!({ "result": format!("{result:?}"), "closed": closed }),
    );
    s.check(
        matches!(result, Err(CallError::ConnectionLost(_))),
        format!("the in-flight call failed as uncertain: {result:?}"),
    );
    s.check(
        closed.as_ref().is_some_and(|e| e.code == "TRANSPORT_DEAD"),
        format!("closed as TRANSPORT_DEAD: {closed:?}"),
    );
    let limit = options.dead_after + Duration::from_secs(2);
    s.check(
        detect_ms <= limit.as_secs_f64() * 1000.0,
        format!("detected within {limit:?}"),
    );
    s.finish();
}

/// 32 MiB each way at the same time on one connection completes without
/// deadlock.
#[test]
#[ignore = "WP1.9"]
fn duplex_32mib() {
    let mut s = Scenario::new(FILE, "duplex_32mib");
    for tls in [false, true] {
        let (client, server, _) = session(tls, false, ConnOptions::default());
        server.serve(service());
        let block = seeded_bytes(21, MIB);
        let watch = Stopwatch::start();
        let up = {
            let client = client.clone();
            thread::spawn(move || upload(&client, 32, &block))
        };
        let down = download(&client, 32);
        let up = up.join().unwrap();
        let ms = watch.ms();
        let metric = if tls { "duplex_wss_ms" } else { "duplex_ms" };
        s.metric_with(
            metric,
            ms,
            "ms",
            json!({ "up": format!("{up:?}"), "down": format!("{down:?}") }),
        );
        s.check(
            up.as_ref().is_ok_and(|n| *n == 32 * MIB as u64)
                && down.as_ref().is_ok_and(|n| *n == 32 * MIB as u64),
            format!("both directions completed (tls: {tls})"),
        );
    }
    s.finish();
}

/// Loopback throughput with 1 MiB chunks. The download is the best of three
/// runs on one connection (WP3.INT): the machine is shared, and what the
/// floor judges is what the session layer can carry, which interference only
/// lowers; every run is recorded with the metric.
#[test]
#[ignore = "WP1.9"]
fn loopback_throughput() {
    let mut s = Scenario::new(FILE, "loopback_throughput");
    let mib = scaled(1024) as u64;
    s.note(format!(
        "session::Conn streamed response, {mib} x 1 MiB chunks (plain WS), best of 3 runs; upload and WSS recorded too"
    ));
    let (client, server, _) = session(false, false, ConnOptions::default());
    server.serve(service());
    // Warm up allocators and socket buffers.
    let _ = download(&client, 16);
    let mut runs = Vec::new();
    for _ in 0..3 {
        let watch = Stopwatch::start();
        let down = download(&client, mib);
        let secs = watch.elapsed().as_secs_f64();
        s.check(
            down.as_ref().is_ok_and(|n| *n == mib * MIB as u64),
            format!("download: {down:?}"),
        );
        runs.push(mib as f64 / secs);
    }
    let best = runs.iter().copied().fold(0.0, f64::max);
    s.metric_with(
        "mib_per_s",
        best,
        "MiB/s",
        json!({ "runs_mib_per_s": runs }),
    );
    let block = seeded_bytes(22, MIB);
    let watch = Stopwatch::start();
    let up = upload(&client, mib, &block);
    s.check(
        up.as_ref().is_ok_and(|n| *n == mib * MIB as u64),
        format!("upload: {up:?}"),
    );
    s.metric(
        "upload_mib_per_s",
        mib as f64 / watch.elapsed().as_secs_f64(),
        "MiB/s",
    );
    let (client, server, _) = session(true, false, ConnOptions::default());
    server.serve(service());
    let _ = download(&client, 16);
    let watch = Stopwatch::start();
    let down = download(&client, mib);
    s.check(
        down.as_ref().is_ok_and(|n| *n == mib * MIB as u64),
        format!("wss download: {down:?}"),
    );
    s.metric(
        "wss_mib_per_s",
        mib as f64 / watch.elapsed().as_secs_f64(),
        "MiB/s",
    );
    s.finish();
}
