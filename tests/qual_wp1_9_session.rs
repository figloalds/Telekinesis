//! Qualification scenarios for WP1.9 (session core: frames, I/O thread,
//! multiplexing). REVAMP_PLAN §14.6, §6.2.
//!
//! Created by WP0.5, `#[ignore = "WP1.9"]`. The session API does not exist at
//! 6298d0b, so each scenario measures the same property on the legacy
//! transport (`tkfs::ws`, one blocking request/response per socket) to record
//! the baseline. WP1.9 replaces the transport calls with `session::Conn`
//! (`call`, `call_streaming`, `upload`, `health`) and un-ignores them.
//! `cargo test --release --test qual_wp1_9_session -- --ignored --test-threads=1 --nocapture`
mod support;

use serde_json::{Value, json};
use std::{
    net::TcpListener,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};
use support::*;
use tkfs::{core::id, ws};

const FILE: &str = "qual_wp1_9_session";

/// A legacy echo server: replies with the request after `sleep_ms` (if any).
fn echo_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            thread::spawn(move || {
                let Ok(mut socket) = ws::accept(stream) else {
                    return;
                };
                while let Ok(request) = ws::receive::<Value>(&mut socket) {
                    if let Some(ms) = request["sleep_ms"].as_u64() {
                        thread::sleep(Duration::from_millis(ms));
                    }
                    if ws::reply(&mut socket, Ok(request)).is_err() {
                        return;
                    }
                }
            });
        }
    });
    endpoint
}

/// A slow request on one stream must not delay fast requests on another
/// stream of the same connection.
#[test]
#[ignore = "WP1.9"]
fn slow_does_not_block_fast() {
    let mut s = Scenario::new(FILE, "slow_does_not_block_fast");
    s.note("baseline transport: legacy ws (no multiplexing; one in-flight request per socket)");
    let endpoint = echo_server();
    let socket = Arc::new(Mutex::new(ws::connect(&endpoint).unwrap()));
    let slow_socket = socket.clone();
    let slow = thread::spawn(move || {
        let mut socket = slow_socket.lock().unwrap();
        ws::call(&mut socket, &json!({"sleep_ms": 3000})).unwrap();
    });
    thread::sleep(Duration::from_millis(100));
    let mut samples = vec![];
    for i in 0..50 {
        let (r, ms) = time_ms(|| {
            let mut socket = socket.lock().unwrap();
            ws::call(&mut socket, &json!({"fast": i}))
        });
        s.check(r.is_ok(), "fast call ok");
        samples.push(ms);
    }
    slow.join().unwrap();
    let summary = summarize(&samples);
    s.metric_with("fast_p99_ms", summary.p99, "ms", summary.json());
    s.finish();
}

/// An idle authenticated peer connection stays usable for 60 s.
#[test]
#[ignore = "WP1.9"]
fn idle_60s() {
    let mut s = Scenario::new(FILE, "idle_60s");
    let a = Node::new();
    let code = a.code();
    let mut socket = ws::connect(&a.endpoint()).unwrap();
    let hello = ws::call(
        &mut socket,
        &json!({"op":"pair","version":2,"code":code,"installation":id(),"lane":"control",
            "bulk_protocol":1,"bulk_lane":null}),
    );
    s.check(hello.is_ok(), format!("pair hello: {hello:?}"));
    let idle = budget_or(60).min(Duration::from_secs(60));
    thread::sleep(idle);
    let (ping, ms) = time_ms(|| ws::call(&mut socket, &json!({"op":"ping"})));
    s.metric("ping_after_idle_ms", ms, "ms");
    s.check(
        ping.is_ok(),
        format!("usable after {} s idle: {ping:?}", idle.as_secs()),
    );
    s.finish();
}

/// A peer that stops responding is detected by liveness, without a request in
/// flight (session heartbeats: dead after `dead_after`).
#[test]
#[ignore = "WP1.9"]
fn dead_peer_detection() {
    let mut s = Scenario::new(FILE, "dead_peer_detection");
    let endpoint = echo_server();
    let proxy = FaultProxy::start(&endpoint);
    let mut socket = ws::connect(&proxy.endpoint()).unwrap();
    ws::call(&mut socket, &json!({"warm": true})).unwrap();
    proxy.blackhole(true);
    // The legacy transport has no liveness: only the next request notices,
    // after its 15 s socket timeout. Record that as the baseline number.
    let (r, ms) = time_ms(|| ws::call(&mut socket, &json!({"after": "blackhole"})));
    s.metric_with(
        "request_detect_ms",
        ms,
        "ms",
        json!({"result": format!("{r:?}")}),
    );
    s.unsupported("idle dead-peer detection needs session heartbeats (WP1.9 Conn::health)");
    s.finish();
}

/// 16 MiB of payload each way at the same time on one connection (32 MiB of
/// hex on the wire per direction) completes without deadlock.
#[test]
#[ignore = "WP1.9"]
fn duplex_32mib() {
    let mut s = Scenario::new(FILE, "duplex_32mib");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let payload = hex::encode(seeded_bytes(21, 16 * 1024 * 1024));
    let server_payload = payload.clone();
    let server = thread::spawn(move || -> anyhow::Result<usize> {
        let mut socket = ws::accept(listener.accept()?.0)?;
        ws::send(&mut socket, &json!({"hex": server_payload}))?;
        let got: Value = ws::receive(&mut socket)?;
        Ok(got["hex"].as_str().map_or(0, str::len))
    });
    let watch = Stopwatch::start();
    let client = (|| -> anyhow::Result<usize> {
        let mut socket = ws::connect(&endpoint)?;
        ws::send(&mut socket, &json!({"hex": payload}))?;
        let got: Value = ws::receive(&mut socket)?;
        Ok(got["hex"].as_str().map_or(0, str::len))
    })();
    let server = server.join().unwrap();
    let ms = watch.ms();
    s.metric_with(
        "duplex_ms",
        ms,
        "ms",
        json!({"client": format!("{client:?}"), "server": format!("{server:?}")}),
    );
    s.check(
        client.as_ref().is_ok_and(|n| *n == 32 * 1024 * 1024)
            && server.as_ref().is_ok_and(|n| *n == 32 * 1024 * 1024),
        "both directions completed",
    );
    s.finish();
}

/// Loopback throughput with 1 MiB messages.
#[test]
#[ignore = "WP1.9"]
fn loopback_throughput() {
    let mut s = Scenario::new(FILE, "loopback_throughput");
    let endpoint = echo_server();
    let mut socket = ws::connect(&endpoint).unwrap();
    let chunk = hex::encode(seeded_bytes(22, 1024 * 1024));
    let rounds = scaled(128);
    let watch = Stopwatch::start();
    for i in 0..rounds {
        let reply = ws::call(&mut socket, &json!({"i": i, "hex": chunk})).unwrap();
        assert_eq!(reply["i"], i);
    }
    // Payload moved in one direction (the echo doubles the wire traffic).
    let mib = rounds as f64;
    s.metric("mib_per_s", mib / watch.elapsed().as_secs_f64(), "MiB/s");
    s.finish();
}
