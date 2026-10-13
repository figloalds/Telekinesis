//! Qualification scenarios for WP3.1 (cutover to v3 networking).
//! REVAMP_PLAN §14.6, §6.2, §3.7, §3.8.
//!
//! Created by WP0.5 against the 6298d0b baseline, `#[ignore = "WP3.1"]`.
//! Daemons are driven with dynamic action JSON; v3 observations (Status,
//! `PEER_NEEDS_UPGRADE`) are read by name.
//! `cargo test --release --test qual_wp3_1_transport -- --ignored --test-threads=1 --nocapture`
mod support;

use serde_json::{Value, json};
use std::{
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use support::*;
use tkfs::{core::id, ws};

const FILE: &str = "qual_wp3_1_transport";

/// Milliseconds from writing `name` on `src` until it is visible on `dst`.
fn propagation_ms(src: &Value, dst: &Value, name: &str, limit: Duration) -> Option<f64> {
    let bytes = name.as_bytes();
    write_file(src, name, bytes);
    let watch = Stopwatch::start();
    wait_until(limit, || has_file(dst, name, bytes)).then(|| watch.ms())
}

fn median(samples: &[f64]) -> f64 {
    percentile(samples, 50.0)
}

/// A hung remote worker for repo X adds < 2 s to repo Y's propagation on the same peer.
#[test]
#[ignore = "WP3.1"]
fn hung_remote_repo_extra_delay() {
    let mut s = Scenario::new(FILE, "hung_remote_repo_extra_delay");
    let mut a = Node::new();
    a.pin_endpoint();
    let b = Node::start(NodeConfig::default().connections(2));
    let px = a.create("Hung X", true);
    let py = a.create("Healthy Y", true);
    b.pair(&a, &a.code());
    let bx = b.receive(&a, &px);
    let by = b.receive(&a, &py);
    write_file(&px, "x.txt", b"x");
    b.wait_for("X synced", Duration::from_secs(60), || {
        has_file(&bx, "x.txt", b"x")
    });
    let mut before = vec![];
    for i in 0..3 {
        if let Some(ms) = propagation_ms(
            &py,
            &by,
            &format!("before-{i}.txt"),
            Duration::from_secs(60),
        ) {
            before.push(ms);
        }
    }
    // Hang X's worker for peer requests: the v3 fault point, and for the legacy
    // path a discovery redirect to a server that never replies.
    a.restart_with_env(vec![
        ("TKFS_FAULT_POINT".into(), "worker.rpc.feed-exchange".into()),
        ("TKFS_FAULT_ACTION".into(), "hang".into()),
        ("TKFS_FAULT_SCOPE".into(), state_id(&px)),
    ]);
    for folder in [&px, &py] {
        let state = state_id(folder);
        a.wait_for("running", Duration::from_secs(60), || {
            a.try_inspect(&state).is_ok_and(|p| p["running"] == true)
        });
    }
    // Queue work for X before its worker becomes unreachable.
    write_file(&px, "x-trigger.txt", b"x-trigger");
    let hung = HungServer::start();
    let redirect = redirect_runtime_json(&state_dir(&px), &hung.endpoint()).ok();
    s.measure_from_now();
    let mut after = vec![];
    for i in 0..3 {
        let limit = s.remaining().max(Duration::from_secs(1));
        match propagation_ms(&py, &by, &format!("after-{i}.txt"), limit) {
            Some(ms) => after.push(ms),
            None => {
                s.timeout("healthy repository stalled");
                break;
            }
        }
    }
    let extra = if after.len() == 3 {
        median(&after) - median(&before)
    } else {
        f64::NAN
    };
    s.metric_with(
        "extra_delay_ms",
        extra,
        "ms",
        json!({"before": before, "after": after, "hung_requests": hung.received()}),
    );
    if let Some(r) = redirect {
        let _ = r.restore();
    }
    s.finish();
}

/// With `sync_interval_ms = 20000`, idle sessions stay up: 0 reconnects in 60 s.
#[test]
#[ignore = "WP3.1"]
fn interval_20s_no_reconnects() {
    let mut s = Scenario::new(FILE, "interval_20s_no_reconnects");
    let a = Node::new();
    let b = Node::start(NodeConfig::default().interval(20_000));
    let proxy = FaultProxy::start(&a.endpoint());
    let pa = a.create("Idle source", true);
    b.pair_endpoint(&proxy.endpoint(), &a.code(), Some(3));
    let _pb = b.receive_from(&proxy.endpoint(), &pa);
    thread::sleep(Duration::from_secs(10));
    let before = proxy.connections();
    s.measure_from_now();
    thread::sleep(Duration::from_secs(60));
    let reconnects = proxy.connections() - before;
    s.metric_with(
        "reconnects",
        reconnects as f64,
        "count",
        json!({"connections_before": before, "active": proxy.active()}),
    );
    s.finish();
}

/// A v2 peer fails closed both ways: a v3 dialer never sends the code to a v2
/// acceptor, and a v2 dialer gets `PEER_NEEDS_UPGRADE`.
#[test]
#[ignore = "WP3.1"]
fn v2_peer_fails_closed() {
    let mut s = Scenario::new(FILE, "v2_peer_fails_closed");
    let a = Node::new();
    // (a) An emulated 6298d0b acceptor that records whether it saw a code.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let saw_code = Arc::new(AtomicBool::new(false));
    let seen = saw_code.clone();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let seen = seen.clone();
            thread::spawn(move || {
                let Ok(mut socket) = ws::accept(stream) else {
                    return;
                };
                while let Ok(hello) = ws::receive::<Value>(&mut socket) {
                    if hello.get("code").is_some() {
                        seen.store(true, Ordering::Relaxed);
                    }
                    let reply = json!({"installation": id(), "version": 2, "lane": hello["lane"],
                        "bulk_protocol": 1, "bulk_lane": hello["bulk_lane"]});
                    if ws::reply(&mut socket, Ok(reply)).is_err() {
                        return;
                    }
                }
            });
        }
    });
    let paired =
        a.raw_call(json!({"op":"pair","endpoint":endpoint,"code":"secret-code","connections":2}));
    s.note(format!("v3 → v2: {paired:?}"));
    s.check(
        paired
            .as_ref()
            .is_err_and(|e| format!("{e:#}").contains("PEER_NEEDS_UPGRADE")),
        "pairing with a v2 acceptor fails with PEER_NEEDS_UPGRADE",
    );
    s.check(
        !saw_code.load(Ordering::Relaxed),
        "the v2 acceptor never received a code",
    );
    // (b) A 6298d0b dialer against this node.
    let code = a.code();
    let reply = ws::connect(&a.endpoint()).and_then(|mut socket| {
        ws::call(
            &mut socket,
            &json!({"op":"pair","version":2,"code":code,"installation":id(),"lane":"control",
                "bulk_protocol":1,"bulk_lane":null}),
        )
    });
    s.note(format!("v2 → v3: {reply:?}"));
    s.check(
        reply
            .as_ref()
            .is_err_and(|e| format!("{e:#}").contains("PEER_NEEDS_UPGRADE")),
        "a v2 dialer gets PEER_NEEDS_UPGRADE",
    );
    // (c) The acceptor's status shows the incompatible attempt.
    match a.raw_call(json!({"op":"status"})) {
        Ok(status) => s.check(
            status.to_string().contains("PEER_NEEDS_UPGRADE"),
            "status shows PEER_NEEDS_UPGRADE",
        ),
        Err(e) => {
            s.unsupported(format!("status action: {}", describe_error(&e)));
            false
        }
    };
    s.finish();
}

/// The acceptor's Status shows the inbound peer with lanes, rates and per-folder state.
#[test]
#[ignore = "WP3.1"]
fn inbound_status() {
    let mut s = Scenario::new(FILE, "inbound_status");
    let a = Node::new();
    let b = Node::new();
    let pa = a.create("Inbound", true);
    write_file(&pa, "a.txt", b"a");
    b.pair(&a, &a.code());
    let pb = b.receive(&a, &pa);
    b.wait_for("synced", Duration::from_secs(60), || {
        has_file(&pb, "a.txt", b"a")
    });
    let status = match a.raw_call(json!({"op":"status"})) {
        Ok(status) => status,
        Err(e) => {
            s.unsupported(format!("status action: {}", describe_error(&e)));
            return s.finish();
        }
    };
    let b_installation = b.installation();
    let peer = status["peers"]
        .as_array()
        .and_then(|p| {
            p.iter()
                .find(|p| p["installation"] == b_installation.as_str())
        })
        .cloned()
        .unwrap_or(Value::Null);
    s.check(!peer.is_null(), "inbound peer listed on the acceptor");
    s.check(
        matches!(peer["direction"].as_str(), Some("inbound" | "both")),
        format!("direction: {}", peer["direction"]),
    );
    s.check(
        peer["lanes"].as_array().is_some_and(|l| !l.is_empty())
            || peer["inbound_sessions"].as_u64().unwrap_or(0) > 0,
        "inbound lanes or sessions",
    );
    let folder = status["folders"]
        .as_array()
        .and_then(|f| f.iter().find(|f| f["state"] == pa["state"]))
        .cloned()
        .unwrap_or(Value::Null);
    s.check(
        folder["sync"]["peers"]
            .to_string()
            .contains(&b_installation),
        "per-folder state for the inbound peer",
    );
    s.finish();
}

/// Rotating the code closes sessions; the dialer is Unauthorized until re-paired.
#[test]
#[ignore = "WP3.1"]
fn rotation_unauthorized_until_repaired() {
    let mut s = Scenario::new(FILE, "rotation_unauthorized_until_repaired");
    let a = Node::new();
    let b = Node::new();
    let pa = a.create("Rotation", true);
    b.pair(&a, &a.code());
    let pb = b.receive(&a, &pa);
    let a_installation = a.installation();
    let unauthorized = |b: &Node| -> bool {
        if let Ok(status) = b.raw_call(json!({"op":"status"})) {
            return status["peers"].as_array().is_some_and(|p| {
                p.iter().any(|p| {
                    p["installation"] == a_installation.as_str() && p["state"] == "unauthorized"
                })
            });
        }
        b.raw_call(json!({"op":"peers"})).is_ok_and(|p| {
            p[0]["lanes"].as_array().is_some_and(|lanes| {
                lanes.iter().all(|l| {
                    l["connected"] == false
                        && l["error"].as_str().is_some_and(|e| {
                            e.contains("INVALID_PAIRING_CODE") || e.contains("PAIRING_CODE_CHANGED")
                        })
                })
            })
        })
    };
    s.measure_from_now();
    let next = a.code();
    let watch = Stopwatch::start();
    let became = wait_until(Duration::from_secs(30), || unauthorized(&b));
    s.metric("unauthorized_ms", watch.ms(), "ms");
    s.check(became, "dialer becomes unauthorized after rotation");
    thread::sleep(Duration::from_secs(10));
    s.check(unauthorized(&b), "stays unauthorized until re-paired");
    b.pair(&a, &next);
    write_file(&pa, "after-repair.txt", b"after re-pair");
    let resumed = wait_until(Duration::from_secs(60), || {
        has_file(&pb, "after-repair.txt", b"after re-pair")
    });
    s.check(resumed, "sync resumes after re-pairing");
    s.finish();
}
