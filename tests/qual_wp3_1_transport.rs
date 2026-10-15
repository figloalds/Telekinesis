//! Qualification scenarios for WP3.1 (cutover to v3 networking).
//! REVAMP_PLAN §14.6, §6.2, §3.7, §3.8.
//!
//! Created by WP0.5 against the 6298d0b baseline, `#[ignore = "WP3.1"]`;
//! completed by WP3.1 for the v3 stack (they stay ignored: `scripts/qualify.ps1`
//! runs them in release).
//! `cargo test --release --test qual_wp3_1_transport -- --ignored --test-threads=1 --nocapture`
mod support;
#[path = "v3_wire_tap/mod.rs"]
mod wire_tap;

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

/// The typed status (`Status`) of `node`.
fn status(node: &Node) -> Value {
    node.raw_call(json!({"op":"status"}))
        .unwrap_or_else(|error| panic!("status: {}", describe_error(&error)))
}

/// `installation`'s entry in `status.peers`.
fn peer_of(status: &Value, installation: &str) -> Value {
    status["peers"]
        .as_array()
        .and_then(|peers| peers.iter().find(|p| p["installation"] == installation))
        .cloned()
        .unwrap_or(Value::Null)
}

/// Whether an open error with `code` is in `status.daemon.errors`.
fn daemon_error(status: &Value, code: &str) -> bool {
    status["daemon"]["errors"].as_array().is_some_and(|errors| {
        errors
            .iter()
            .any(|record| record["error"]["code"] == code && record["resolved_at_ms"].is_null())
    })
}

/// Every outbound lane of `installation` is `ready` in `node`'s status.
fn lanes_ready(node: &Node, installation: &str) -> bool {
    let peer = peer_of(&status(node), installation);
    peer["lanes"].as_array().is_some_and(|lanes| {
        let outbound: Vec<&Value> = lanes
            .iter()
            .filter(|lane| lane["direction"] == "outbound")
            .collect();
        !outbound.is_empty() && outbound.iter().all(|lane| lane["state"] == "ready")
    })
}

/// `name` holds `bytes` and the folder's content is complete (the object is
/// installed, not only its metadata).
fn has_content(folder: &Value, name: &str, bytes: &[u8]) -> bool {
    has_file(folder, name, bytes) && content_status(folder)["complete"] == true
}

/// A hung remote worker for repo X adds < 2 s to repo Y's propagation on the
/// same peer. The hang is X's worker op of the v3 path (`feed-exchange`,
/// scoped): the daemon never reads `runtime.json` to reach a worker.
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
    // Queue work for X: its exchanges now hang in A's worker.
    write_file(&px, "x-trigger.txt", b"x-trigger");
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
        json!({"before": before, "after": after, "hang": "worker.rpc.feed-exchange"}),
    );
    // The hung exchange is charged to X alone, at its dialer deadline (30 s):
    // the legacy Peers reply shows every failure (Status hides a Transient
    // error until its third attempt, §7.1).
    let x_repo = px["repo"].as_str().unwrap().to_owned();
    let charged = wait_until(Duration::from_secs(75), || {
        b.raw_call(json!({"op":"peers"})).is_ok_and(|peers| {
            peers[0]["lanes"].as_array().is_some_and(|lanes| {
                lanes.iter().any(|lane| {
                    let repo = &lane["repositories"][&x_repo];
                    repo["error"].is_string()
                        && repo["consecutive_failures"].as_u64().unwrap_or(0) > 0
                })
            })
        })
    });
    s.check(charged, "the hung repository's exchange is charged to it");
    s.finish();
}

/// With `sync_interval_ms = 20000`, idle sessions stay up: 0 reconnects in
/// 60 s, and every lane is still ready at the end (v3 sessions heartbeat).
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
    let installation = a.installation();
    b.wait_for("lanes ready", Duration::from_secs(60), || {
        lanes_ready(&b, &installation)
    });
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
    s.check(before >= 3, format!("{before} sessions before the window"));
    s.check(
        lanes_ready(&b, &installation),
        "every lane is still ready after 60 s idle",
    );
    s.finish();
}

/// A v2 peer fails closed on both daemons: a v3 dialer never sends the code
/// to a v2 acceptor, a v2 dialer gets `PEER_NEEDS_UPGRADE`, and both
/// daemons' Status say so (the dialer's refused pairing, the acceptor's
/// inbound attempt).
#[test]
#[ignore = "WP3.1"]
fn v2_peer_fails_closed() {
    let mut s = Scenario::new(FILE, "v2_peer_fails_closed");
    let a = Node::new();
    let b = Node::new();
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
    let watch = Stopwatch::start();
    let paired =
        a.raw_call(json!({"op":"pair","endpoint":endpoint,"code":"secret-code","connections":2}));
    s.metric("v3_to_v2_refusal_ms", watch.ms(), "ms");
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
    // (b) A 6298d0b dialer against a v3 node.
    let code = b.code();
    let reply = ws::connect(&b.endpoint()).and_then(|mut socket| {
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
    // (c) Both daemons' status show it, and never a code.
    for (node, side) in [(&a, "dialer"), (&b, "acceptor")] {
        let shown = wait_until(Duration::from_secs(10), || {
            daemon_error(&status(node), "PEER_NEEDS_UPGRADE")
        });
        s.check(
            shown,
            format!("the {side}'s status shows PEER_NEEDS_UPGRADE"),
        );
        let text = status(node).to_string();
        s.check(
            !text.contains(&code) && !text.contains("secret-code"),
            format!("no code in the {side}'s status"),
        );
    }
    s.finish();
}

/// The acceptor's Status shows the inbound peer with lanes, rates and
/// per-folder state.
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
    write_file(&pb, "b.txt", b"b");
    a.wait_for("synced back", Duration::from_secs(60), || {
        has_file(&pa, "b.txt", b"b")
    });
    let b_installation = b.installation();
    let status = status(&a);
    let peer = peer_of(&status, &b_installation);
    s.check(!peer.is_null(), "inbound peer listed on the acceptor");
    s.check(
        matches!(peer["direction"].as_str(), Some("inbound" | "both")),
        format!("direction: {}", peer["direction"]),
    );
    let inbound_ready = |role: &str| {
        peer["lanes"].as_array().is_some_and(|lanes| {
            lanes.iter().any(|lane| {
                lane["direction"] == "inbound" && lane["role"] == role && lane["state"] == "ready"
            })
        })
    };
    s.check(
        inbound_ready("control") && inbound_ready("bulk"),
        format!("inbound control and bulk lanes ready: {}", peer["lanes"]),
    );
    s.check(
        peer["inbound_sessions"].as_u64().unwrap_or(0) >= 2,
        "inbound sessions counted",
    );
    s.check(
        peer["rate_up_bps"].is_u64() && peer["rate_down_bps"].is_u64(),
        "rates",
    );
    let folder = status["folders"]
        .as_array()
        .and_then(|f| f.iter().find(|f| f["state"] == pa["state"]))
        .cloned()
        .unwrap_or(Value::Null);
    let pair = folder["sync"]["peers"]
        .as_array()
        .and_then(|peers| peers.iter().find(|p| p["installation"] == b_installation))
        .cloned()
        .unwrap_or(Value::Null);
    s.check(!pair.is_null(), "per-folder state for the inbound peer");
    s.check(
        pair["metadata"]["last_exchange_ms"].is_u64(),
        format!("the served exchanges are on record: {pair}"),
    );
    s.note(format!("peer: {peer}"));
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
    let unauthorized =
        |b: &Node| -> bool { peer_of(&status(b), &a_installation)["state"] == "unauthorized" };
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

/// Two real daemons over a faulty link (`FaultProxy`, 50 ms each way, a
/// bandwidth cap, a stall longer than the sessions' 20 s liveness bound, and
/// resets in the middle of transfers), both directions: sync never sticks,
/// and no request is ever sent twice (a tap in front of the proxy reads the
/// dialer's wire: invariant 16).
#[test]
#[ignore = "WP3.1"]
fn faulty_link_no_stuck_sync_no_replay() {
    let mut s = Scenario::with_budget(FILE, "faulty_link_no_stuck_sync_no_replay", budget_or(600));
    let a = Node::new();
    let b = Node::start(NodeConfig::default().connections(3));
    let proxy = FaultProxy::start(&a.endpoint());
    let tap = wire_tap::WireTap::start(&proxy.endpoint());
    let pa = a.create("Faulty link", true);
    write_file(&pa, "seed.txt", b"seed");
    b.pair_endpoint(&tap.endpoint(), &a.code(), Some(3));
    let pb = b.receive_from(&tap.endpoint(), &pa);
    b.wait_for("seed", Duration::from_secs(60), || {
        has_file(&pb, "seed.txt", b"seed")
    });
    s.measure_from_now();

    // 1. Latency and bandwidth: 100 ms RTT, 1 MiB/s each way.
    proxy.set_latency(Duration::from_millis(50));
    proxy.set_bandwidth(1024 * 1024);
    let big = seeded_bytes(1, 4 * 1024 * 1024);
    let watch = Stopwatch::start();
    write_file(&pa, "big.bin", &big);
    write_file(&pb, "from-b.txt", b"written at B");
    let moved = wait_until(Duration::from_secs(180), || {
        has_content(&pb, "big.bin", &big) && has_file(&pa, "from-b.txt", b"written at B")
    });
    s.metric("latency_bandwidth_sync_ms", watch.ms(), "ms");
    s.check(moved, "both directions under latency and a bandwidth cap");

    // 2. A 25 s stall: every session is dead after 20 s without a frame,
    // the lanes redial, and what was written meanwhile arrives both ways.
    let connections = proxy.connections();
    proxy.stall_for(Duration::from_secs(25));
    let watch = Stopwatch::start();
    write_file(&pa, "during-stall-a.txt", b"stall A");
    write_file(&pb, "during-stall-b.txt", b"stall B");
    let recovered = wait_until(Duration::from_secs(180), || {
        has_file(&pb, "during-stall-a.txt", b"stall A")
            && has_file(&pa, "during-stall-b.txt", b"stall B")
    });
    s.metric_with(
        "stall_recovery_ms",
        watch.ms(),
        "ms",
        json!({"stall_ms": 25_000, "redials": proxy.connections() - connections}),
    );
    s.check(recovered, "sync recovers after a 25 s stall");
    s.check(
        proxy.connections() > connections,
        "the dead sessions were replaced",
    );

    // 3. Resets in the middle of transfers, three times.
    proxy.set_bandwidth(2 * 1024 * 1024);
    let resets = proxy.resets();
    let mut objects = vec![];
    let watch = Stopwatch::start();
    for round in 0..3u64 {
        let bytes = seeded_bytes(10 + round, 3 * 1024 * 1024);
        write_file(&pa, &format!("reset-{round}.bin"), &bytes);
        proxy.reset_after(1024 * 1024);
        objects.push((format!("reset-{round}.bin"), bytes));
        thread::sleep(Duration::from_secs(2));
    }
    let completed = wait_until(Duration::from_secs(240), || {
        objects
            .iter()
            .all(|(name, bytes)| has_content(&pb, name, bytes))
    });
    s.metric_with(
        "reset_transfers_ms",
        watch.ms(),
        "ms",
        json!({"resets": proxy.resets() - resets}),
    );
    s.check(completed, "transfers cut by resets complete");
    s.check(proxy.resets() > resets, "connections were reset");

    // 4. A clean link: nothing is left stuck.
    proxy.clear();
    let clean = propagation_ms(&pa, &pb, "clean-a.txt", Duration::from_secs(60)).is_some()
        && propagation_ms(&pb, &pa, "clean-b.txt", Duration::from_secs(60)).is_some();
    s.check(clean, "both directions on a clean link");

    // Nothing was sent twice.
    let duplicates = tap.duplicates();
    s.metric_with(
        "replayed_requests",
        duplicates.len() as f64,
        "count",
        json!({
            "meta_exchange": tap.count("meta.exchange"),
            "objects_exchange": tap.count("objects.exchange"),
            "connections": tap.connections(),
            "malformed": tap.malformed(),
            "duplicates": duplicates,
        }),
    );
    s.check(duplicates.is_empty(), "no request was sent twice");
    s.check(tap.malformed() == 0, "the tap followed the dialer's stream");
    s.check(
        tap.count("meta.exchange") > 0 && tap.count("objects.exchange") > 0,
        "exchanges were seen",
    );
    s.finish();
}

/// A 6298d0b daemon (`TKFS_BASELINE_BIN`) on its own root.
struct Baseline {
    root: std::path::PathBuf,
    child: Option<std::process::Child>,
}

impl Baseline {
    fn start(bin: &std::path::Path, root: &std::path::Path, cfg: &NodeConfig) -> Self {
        std::fs::write(root.join("orchestrator.toml"), cfg.toml()).unwrap();
        let log = std::fs::File::create(root.join("daemon.log")).unwrap();
        let child = std::process::Command::new(bin)
            .arg("-f")
            .arg(root.join("orchestrator.toml"))
            .arg("orchestrator")
            .stdin(std::process::Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        let node = Self {
            root: root.to_owned(),
            child: Some(child),
        };
        assert!(
            wait_until(Duration::from_secs(30), || node
                .call(json!({"op":"list"}))
                .is_ok()),
            "the baseline daemon did not start"
        );
        node
    }

    fn call(&self, action: Value) -> anyhow::Result<Value> {
        Control {
            data_dir: self.root.join("data"),
        }
        .call(&action)
    }

    fn endpoint(&self) -> String {
        let discovery: Value = serde_json::from_slice(
            &std::fs::read(self.root.join("data/orchestrator.json")).unwrap(),
        )
        .unwrap();
        discovery["network"].as_str().unwrap().to_owned()
    }

    fn code(&self) -> String {
        self.call(json!({"op":"reset-pairing-code"})).unwrap()["code"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    /// Shutdown, and wait for the process to exit.
    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = self.call(json!({"op":"shutdown"}));
            if !wait_until(Duration::from_secs(20), || {
                child.try_wait().ok().flatten().is_some()
            }) {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

impl Drop for Baseline {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The upgrade path against a real 6298d0b peer: X and Y (both 6298d0b)
/// are paired both ways; X is upgraded to this build on its data directory
/// (same endpoint). Both sides fail closed and show it: X dials Y and its
/// Status shows Y `incompatible` with `PEER_NEEDS_UPGRADE` (the code is
/// never offered, §6.2); Y's legacy lanes dial X and are answered
/// `PEER_NEEDS_UPGRADE`, which Y's own Peers reply shows; X's Status shows
/// Y's attempts. Skipped without `TKFS_BASELINE_BIN`.
#[test]
#[ignore = "WP3.1"]
fn upgrade_against_a_6298d0b_peer_fails_closed_on_both_sides() {
    let s = Scenario::new(
        FILE,
        "upgrade_against_a_6298d0b_peer_fails_closed_on_both_sides",
    );
    let Some(bin) = std::env::var_os("TKFS_BASELINE_BIN").map(std::path::PathBuf::from) else {
        return s.skip("TKFS_BASELINE_BIN (a 6298d0b tkfs binary) is not set");
    };
    let mut s = s;
    // X keeps its endpoint across the upgrade (its peer Y dials it there).
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let x_cfg = NodeConfig {
        listen: Some(format!("127.0.0.1:{port}")),
        ..NodeConfig::default()
    };
    let x_root = tempfile::tempdir().unwrap();
    let y_root = tempfile::tempdir().unwrap();
    let mut x = Baseline::start(&bin, x_root.path(), &x_cfg);
    let y = Baseline::start(&bin, y_root.path(), &NodeConfig::default());
    let paired_xy =
        x.call(json!({"op":"pair","endpoint":y.endpoint(),"code":y.code(),"connections":2}));
    let paired_yx =
        y.call(json!({"op":"pair","endpoint":x.endpoint(),"code":x.code(),"connections":2}));
    s.check(
        paired_xy.is_ok() && paired_yx.is_ok(),
        format!("6298d0b pairing: {paired_xy:?} {paired_yx:?}"),
    );
    let y_installation = y.call(json!({"op":"list"})).unwrap()["installation"]
        .as_str()
        .unwrap()
        .to_owned();
    // Upgrade X.
    x.stop();
    drop(x);
    let mut x = Node::adopt_root(x_root, x_cfg);
    s.measure_from_now();
    let watch = Stopwatch::start();
    let dialer = wait_until(Duration::from_secs(60), || {
        let peer = peer_of(&status(&x), &y_installation);
        peer["state"] == "incompatible" && peer["error"]["error"]["code"] == "PEER_NEEDS_UPGRADE"
    });
    s.metric("upgraded_dialer_visible_ms", watch.ms(), "ms");
    s.check(
        dialer,
        "the upgraded daemon shows its 6298d0b peer incompatible",
    );
    let old_side = wait_until(Duration::from_secs(90), || {
        y.call(json!({"op":"peers"})).is_ok_and(|peers| {
            peers[0]["lanes"].as_array().is_some_and(|lanes| {
                lanes.iter().any(|lane| {
                    lane["error"]
                        .as_str()
                        .is_some_and(|e| e.contains("PEER_NEEDS_UPGRADE"))
                })
            })
        })
    });
    s.metric("old_side_visible_ms", watch.ms(), "ms");
    s.check(
        old_side,
        "the 6298d0b daemon's Peers show PEER_NEEDS_UPGRADE",
    );
    let inbound = wait_until(Duration::from_secs(30), || {
        daemon_error(&status(&x), "PEER_NEEDS_UPGRADE")
    });
    s.check(
        inbound,
        "the upgraded daemon's Status shows the inbound attempts",
    );
    s.note(format!(
        "6298d0b peers: {}",
        y.call(json!({"op":"peers"})).unwrap_or_default()
    ));
    let _ = x.stop();
    s.finish();
}
