//! Qualification scenarios for WP3.INT (end to end, two or three daemons).
//! REVAMP_PLAN §14 WP3.INT, §15 (Wave 3 floors, including the daemon-level
//! idle exchange, 150K feed and large event).
//!
//! Created by WP0.5 against the 6298d0b baseline, `#[ignore = "WP3.INT"]`.
//! Large folder contents are written in-process into a stopped folder
//! (`Node::with_stopped_folder`), then synchronized by the daemons.
//! `cargo test --release --test qual_wp3_int_e2e -- --ignored --test-threads=1 --nocapture`
mod support;

use serde_json::{Value, json};
use std::{
    fs,
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

const FILE: &str = "qual_wp3_int_e2e";

const SETUP: Duration = Duration::from_secs(900);

/// Number of 64-hex object files in a state's CAS directory.
fn cas_files(folder: &Value) -> usize {
    fs::read_dir(state_dir(folder).join("objects"))
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            name.len() == 64 && name.bytes().all(|c| c.is_ascii_hexdigit())
        })
        .count()
}

fn complete(folder: &Value, objects: u64) -> bool {
    content_complete(folder, objects)
}

/// Transfer one object of `size` bytes A → B on loopback. Returns (seconds, done).
fn transfer_one(s: &mut Scenario, size: u64) -> (f64, bool) {
    let a = Node::new();
    let b = Node::new();
    let pa = a.create("Big object", true);
    let hash = a
        .with_stopped_folder(&pa, SETUP, |st| large_object_in(st, "big.bin", size, 31))
        .unwrap();
    b.pair(&a, &a.code());
    s.measure_from_now();
    let pb = b.receive(&a, &pa);
    let done = wait_until(s.remaining(), || {
        has_hash(&pb, "big.bin", &hash) && complete(&pb, 1)
    });
    (s.started.elapsed().as_secs_f64(), done)
}

/// One 256 MiB object between daemons on loopback ≥ 40 MiB/s.
#[test]
#[ignore = "WP3.INT"]
fn object_256mib_loopback() {
    let mut s = Scenario::new(FILE, "object_256mib_loopback");
    let size = scaled(256) as u64 * 1024 * 1024;
    let (secs, done) = transfer_one(&mut s, size);
    s.metric_with(
        "mib_per_s",
        if done {
            size as f64 / 1048576.0 / secs
        } else {
            f64::NAN
        },
        "MiB/s",
        json!({"bytes": size, "seconds": secs}),
    );
    if !done {
        s.timeout("object not transferred in budget");
    }
    s.finish();
}

/// 1 GiB through a 100 Mbit / 50 ms RTT link reaches ≥ 80 % of the link rate.
#[test]
#[ignore = "WP3.INT"]
fn one_gib_through_proxy() {
    let mut s = Scenario::new(FILE, "one_gib_through_proxy");
    let size = scaled(1024) as u64 * 1024 * 1024;
    let link_rate = 12_500_000u64;
    let a = Node::new();
    let b = Node::new();
    let pa = a.create("Gigabyte", true);
    let hash = a
        .with_stopped_folder(&pa, SETUP, |st| large_object_in(st, "gib.bin", size, 32))
        .unwrap();
    let proxy = FaultProxy::start(&a.endpoint());
    proxy.set_latency(Duration::from_millis(25));
    proxy.set_bandwidth(link_rate);
    b.pair_endpoint(&proxy.endpoint(), &a.code(), Some(3));
    s.measure_from_now();
    let pb = b.receive_from(&proxy.endpoint(), &pa);
    let done = wait_until(s.remaining(), || {
        has_hash(&pb, "gib.bin", &hash) && complete(&pb, 1)
    });
    let secs = s.started.elapsed().as_secs_f64();
    let moved = if done {
        size as f64
    } else {
        content_status(&pb)["partial_bytes"].as_f64().unwrap_or(0.0)
    };
    s.metric_with(
        "link_utilization",
        moved / secs / link_rate as f64,
        "ratio",
        json!({"bytes": size, "seconds": secs, "proxy_down": proxy.bytes_down()}),
    );
    if !done {
        s.timeout("object not transferred in budget");
    }
    s.finish();
}

fn tiny_files(s: &mut Scenario, count: usize) {
    let a = Node::new();
    let b = Node::new();
    let pa = a.create("Tiny files", true);
    a.with_stopped_folder(&pa, SETUP, |st| {
        small_objects_in(st, "tiny", count, 4096, 33)
    })
    .unwrap();
    b.pair(&a, &a.code());
    s.measure_from_now();
    let pb = b.receive(&a, &pa);
    let done = wait_until(s.remaining(), || complete(&pb, count as u64));
    let secs = s.started.elapsed().as_secs_f64();
    let available = content_status(&pb)["shared_available"]
        .as_f64()
        .unwrap_or(0.0);
    s.metric_with(
        "files_per_s",
        available / secs,
        "files/s",
        json!({"files": count, "available": available, "seconds": secs}),
    );
    if !done {
        s.timeout(format!("{available} of {count} files in budget"));
    }
}

/// 10K × 4 KiB files ≥ 300 files/s.
#[test]
#[ignore = "WP3.INT"]
fn tiny_files_10k() {
    let mut s = Scenario::new(FILE, "tiny_files_10k");
    tiny_files(&mut s, scaled(10_000));
    s.finish();
}

/// Objects flow while activation is slowed: the receiver holds objects whose
/// events are not yet activated (`objects_ahead` > 0).
#[test]
#[ignore = "WP3.INT"]
fn objects_before_activation() {
    let mut s = Scenario::new(FILE, "objects_before_activation");
    let a = Node::new();
    let b = Node::start(
        NodeConfig::default()
            .env("TKFS_FAULT_POINT", "worker.activator.slice")
            .env("TKFS_FAULT_ACTION", "hang")
            .env("TKFS_FAULT_MS", "500"),
    );
    let pa = a.create("Before activation", true);
    let count = scaled(300);
    a.with_stopped_folder(&pa, SETUP, |st| {
        batched(st, |st| {
            for i in 0..count {
                let e = st.create(&format!("early-{i:04}.bin"), "file")?;
                st.write_revision(
                    &e.id,
                    e.revisions.get("content").cloned(),
                    &seeded_bytes(400 + i as u64, 1024),
                )?;
            }
            Ok(())
        })
    })
    .unwrap();
    b.pair(&a, &a.code());
    s.measure_from_now();
    let pb = b.receive(&a, &pa);
    let mut ahead = 0usize;
    let mut previous = 0usize;
    let done = wait_until(s.remaining(), || {
        // Count files first, so every counted file was installed before the
        // status snapshot (the empty file's object is always present).
        let present = cas_files(&pb).saturating_sub(1);
        let status = content_status(&pb);
        // A busy reply carries no counts: skip the sample.
        let Some(committed) = status["shared_available"].as_u64() else {
            return false;
        };
        let now = present.saturating_sub(committed as usize);
        // Only a surplus seen in two consecutive samples counts (no install race).
        ahead = ahead.max(now.min(previous));
        previous = now;
        status["complete"] == true && status["shared_objects"].as_u64() == Some(count as u64 + 1)
    });
    s.metric("objects_ahead", ahead as f64, "objects");
    s.metric(
        "complete_ms",
        s.started.elapsed().as_secs_f64() * 1000.0,
        "ms",
    );
    if !done {
        s.timeout("transfer not complete in budget");
    }
    s.finish();
}

/// A ↔ B ↔ C relay of 1 MiB objects in both directions.
#[test]
#[ignore = "WP3.INT"]
fn relay_with_objects() {
    let mut s = Scenario::new(FILE, "relay_with_objects");
    let a = Node::new();
    let b = Node::new();
    let c = Node::new();
    let pa = a.create("Relay", true);
    b.pair(&a, &a.code());
    let pb = b.receive(&a, &pa);
    c.pair(&b, &b.code());
    let pc = c.receive(&b, &pb);
    let from_a = seeded_bytes(501, 1024 * 1024);
    let from_c = seeded_bytes(502, 1024 * 1024);
    s.measure_from_now();
    write_file(&pa, "from-a.bin", &from_a);
    write_file(&pc, "from-c.bin", &from_c);
    let done = wait_until(s.remaining(), || {
        has_file(&pc, "from-a.bin", &from_a)
            && has_file(&pa, "from-c.bin", &from_c)
            && complete(&pc, 2)
            && complete(&pa, 2)
    });
    s.metric("relay_ms", s.started.elapsed().as_secs_f64() * 1000.0, "ms");
    if !done {
        s.timeout("relay incomplete");
    }
    s.finish();
}

/// Mutual pairing transfers ≤ 1.1× the object bytes (counted on the wire).
#[test]
#[ignore = "WP3.INT"]
fn mutual_pairing_duplicate_bytes() {
    let mut s = Scenario::new(FILE, "mutual_pairing_duplicate_bytes");
    let a = Node::new();
    let b = Node::new();
    let to_a = FaultProxy::start(&a.endpoint());
    let to_b = FaultProxy::start(&b.endpoint());
    let pa = a.create("Mutual", true);
    let files = scaled(8);
    let size = 4 * 1024 * 1024;
    a.with_stopped_folder(&pa, SETUP, |st| {
        for i in 0..files {
            large_object_in(st, &format!("m-{i}.bin"), size as u64, 600 + i as u64)?;
        }
        Ok(())
    })
    .unwrap();
    b.pair_endpoint(&to_a.endpoint(), &a.code(), Some(3));
    a.pair_endpoint(&to_b.endpoint(), &b.code(), Some(3));
    s.measure_from_now();
    let pb = b.receive_from(&to_a.endpoint(), &pa);
    let done = wait_until(s.remaining(), || complete(&pb, files as u64));
    let wire = to_a.bytes_up() + to_a.bytes_down() + to_b.bytes_up() + to_b.bytes_down();
    let object_bytes = (files * size) as f64;
    s.metric_with(
        "wire_ratio",
        wire as f64 / object_bytes,
        "ratio",
        json!({"wire_bytes": wire, "object_bytes": object_bytes}),
    );
    if !done {
        s.timeout("transfer incomplete");
    }
    s.finish();
}

/// A 10K-event catch-up between daemons ≤ 60 s.
#[test]
#[ignore = "WP3.INT"]
fn catch_up_10k_events() {
    let mut s = Scenario::new(FILE, "catch_up_10k_events");
    let n = scaled(10_000);
    let a = Node::new();
    let b = Node::new();
    let pa = a.create("Catch-up", true);
    a.with_stopped_folder(&pa, SETUP, |st| linear_history(st, n).map(|_| ()))
        .unwrap();
    b.pair(&a, &a.code());
    s.measure_from_now();
    let pb = b.receive(&a, &pa);
    let last = format!("lin-{:06}.txt", n - 1);
    let empty = tkfs::core::hash(b"");
    let done = wait_until(s.remaining(), || has_hash(&pb, &last, &empty));
    s.metric(
        "seconds",
        if done {
            s.started.elapsed().as_secs_f64()
        } else {
            f64::NAN
        },
        "s",
    );
    if !done {
        s.timeout("catch-up incomplete");
    }
    s.finish();
}

/// Two-way backlog end to end: A authors 2K creates while B has one unseen
/// local edit; both converge in ≤ 3 min.
#[test]
#[ignore = "WP3.INT"]
fn two_way_backlog_e2e() {
    let mut s = Scenario::new(FILE, "two_way_backlog_e2e");
    let n = scaled(2000);
    let a = Node::new();
    let b = Node::new();
    let pa = a.create("Backlog", true);
    write_file(&pa, "shared.txt", b"seed");
    b.pair(&a, &a.code());
    let pb = b.receive(&a, &pa);
    b.wait_for("seed", Duration::from_secs(60), || {
        has_file(&pb, "shared.txt", b"seed")
    });
    // Both folders stop; A gains the backlog and B its local edit offline.
    b.with_stopped_folder(&pb, SETUP, |st| {
        let e = st.lookup("shared.txt")?;
        st.write_revision(&e.id, e.revisions.get("content").cloned(), b"edit on B")?;
        Ok(())
    })
    .unwrap();
    a.with_stopped_folder(&pa, SETUP, |st| {
        linear_history_named(st, "backlog", n).map(|_| ())
    })
    .unwrap();
    s.measure_from_now();
    let last = format!("backlog-{:06}.txt", n - 1);
    let empty = tkfs::core::hash(b"");
    let done = wait_until(s.remaining(), || {
        has_hash(&pb, &last, &empty) && has_file(&pa, "shared.txt", b"edit on B")
    });
    s.metric(
        "seconds",
        if done {
            s.started.elapsed().as_secs_f64()
        } else {
            f64::NAN
        },
        "s",
    );
    if !done {
        s.timeout("backlog not converged");
    }
    s.finish();
}

/// A v2 (6298d0b) peer fails closed on both sides.
#[test]
#[ignore = "WP3.INT"]
fn v2_peer_fails_closed() {
    let mut s = Scenario::new(FILE, "v2_peer_fails_closed");
    let a = Node::new();
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
    s.check(
        paired
            .as_ref()
            .is_err_and(|e| format!("{e:#}").contains("PEER_NEEDS_UPGRADE")),
        format!("v3 → v2 pairing: {paired:?}"),
    );
    s.check(
        !saw_code.load(Ordering::Relaxed),
        "code never sent to a v2 acceptor",
    );
    let code = a.code();
    let reply = ws::connect(&a.endpoint()).and_then(|mut socket| {
        ws::call(
            &mut socket,
            &json!({"op":"pair","version":2,"code":code,"installation":id(),"lane":"control",
                "bulk_protocol":1,"bulk_lane":null}),
        )
    });
    s.check(
        reply
            .as_ref()
            .is_err_and(|e| format!("{e:#}").contains("PEER_NEEDS_UPGRADE")),
        format!("v2 → v3 pairing: {reply:?}"),
    );
    s.finish();
}

/// The 6298d0b legacy data directory upgrades in place, then syncs with a new node.
#[test]
#[ignore = "WP3.INT"]
fn legacy_fixture_upgrade_then_sync() {
    let mut s = Scenario::new(FILE, "legacy_fixture_upgrade_then_sync");
    let fx = copy_legacy_datadir();
    let manifest = fx.manifest.clone();
    let legacy = Node::adopt_root(fx.root, NodeConfig::default());
    let folder = manifest["folders"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["file"].is_string())
        .cloned()
        .unwrap();
    let state = folder["state"].as_str().unwrap().to_owned();
    legacy.wait_for("legacy folder running", Duration::from_secs(120), || {
        legacy
            .try_inspect(&state)
            .is_ok_and(|p| p["running"] == true)
    });
    let source = legacy.inspect(&state);
    let fresh = Node::new();
    fresh.pair(&legacy, &legacy.code());
    s.measure_from_now();
    let received = fresh.receive(&legacy, &source);
    let name = folder["file"].as_str().unwrap();
    let content = folder["content"].as_str().unwrap();
    let synced = wait_until(s.remaining(), || {
        has_hash(&received, name, content) && complete(&received, 1)
    });
    s.metric("sync_ms", s.started.elapsed().as_secs_f64() * 1000.0, "ms");
    s.check(synced, "legacy content reaches the new node");
    write_file(&received, "back.txt", b"back to the upgraded node");
    let back = wait_until(Duration::from_secs(60), || {
        has_file(&source, "back.txt", b"back to the upgraded node")
    });
    s.check(back, "the upgraded node receives new content");
    s.finish();
}

/// Idle exchanges between daemons sharing a 50K-event folder cost ≤ 4 KiB each.
#[test]
#[ignore = "WP3.INT"]
fn idle_exchange_daemons() {
    let mut s = Scenario::new(FILE, "idle_exchange_daemons");
    let n = scaled(50_000);
    let a = Node::new();
    let b = Node::new();
    let proxy = FaultProxy::start(&a.endpoint());
    let (pa, _pb) = synced_replicas(&a, &b, &proxy.endpoint(), "Idle 50K", |st| {
        for start in (0..n).step_by(10_000) {
            linear_history_named(st, &format!("idle{start:06}"), (n - start).min(10_000))?;
        }
        Ok(())
    })
    .unwrap();
    let repo = pa["repo"].as_str().unwrap().to_owned();
    let caught_up = wait_until(SETUP, || {
        b.raw_call(json!({"op":"peers"}))
            .is_ok_and(|p| p[0]["lanes"][0]["repositories"][&repo]["metadata_caught_up"] == true)
    });
    s.check(caught_up, "replicas caught up");
    // Let the catch-up's last exchanges (acknowledgements) finish.
    thread::sleep(Duration::from_secs(5));
    // The control lane's metadata exchanges so far, and both feeds' tips as
    // B sees them, from B's Peers reply (WP3.INT: `None` when it does not
    // answer, never a zero that a later sample would be subtracted from).
    let sample = |b: &Node| -> Option<(u64, Value)> {
        let peers = b.raw_call(json!({"op":"peers"})).ok()?;
        let control = &peers[0]["lanes"][0];
        let exchanges = control["exchanges"].as_u64()?;
        let progress = control["repositories"][&repo]["metadata_progress"].clone();
        Some((exchanges, progress))
    };
    let settled = |b: &Node| {
        let mut taken = None;
        wait_until(Duration::from_secs(30), || {
            taken = sample(b);
            taken.is_some()
        });
        taken
    };
    s.measure_from_now();
    let before = settled(&b);
    let bytes0 = proxy.bytes_up() + proxy.bytes_down();
    thread::sleep(Duration::from_secs(30).min(s.budget));
    let bytes1 = proxy.bytes_up() + proxy.bytes_down();
    let after = settled(&b);
    // WP3.INT (WP0.5 review major): the floor holds only for exchanges that
    // happened, and were idle: at least `MIN_IDLE` metadata exchanges in the
    // window, and neither feed moved (`metadata_progress` unchanged).
    const MIN_IDLE: u64 = 10;
    let exchanged = match (&before, &after) {
        (Some((ex0, _)), Some((ex1, _))) => ex1.checked_sub(*ex0),
        _ => None,
    };
    let bytes = bytes1.checked_sub(bytes0);
    let idle =
        matches!((&before, &after), (Some((_, p0)), Some((_, p1))) if p0 == p1 && !p0.is_null());
    s.check(
        exchanged.is_some_and(|exchanged| exchanged >= MIN_IDLE),
        format!("at least {MIN_IDLE} idle exchanges in the window (saw {exchanged:?})"),
    );
    s.check(idle, "the feeds did not move during the window");
    let per = match (bytes, exchanged) {
        (Some(bytes), Some(exchanged)) if exchanged > 0 && idle => bytes as f64 / exchanged as f64,
        _ => f64::NAN,
    };
    s.metric_with(
        "bytes_per_exchange",
        per,
        "bytes",
        json!({"bytes": bytes, "exchanges": exchanged, "events": n,
            "progress": after.map(|(_, progress)| progress)}),
    );
    s.finish();
}

/// A 150K-event folder synchronizes between daemons (no inventory caps).
#[test]
#[ignore = "WP3.INT"]
fn feed_150k_daemons() {
    let mut s = Scenario::new(FILE, "feed_150k_daemons");
    let n = scaled(150_000);
    let a = Node::new();
    let b = Node::new();
    let pa = a.create("Feed 150K", true);
    a.with_stopped_folder(&pa, SETUP, |st| {
        for start in (0..n).step_by(10_000) {
            linear_history_named(st, &format!("f{start:06}"), (n - start).min(10_000))?;
        }
        Ok(())
    })
    .unwrap();
    b.pair(&a, &a.code());
    s.measure_from_now();
    let pb = b.receive(&a, &pa);
    let last_start = (n - 1) / 10_000 * 10_000;
    let last = format!("f{last_start:06}-{:06}.txt", n - 1 - last_start);
    let empty = tkfs::core::hash(b"");
    let done = wait_until(s.remaining(), || has_hash(&pb, &last, &empty));
    s.metric_with(
        "completed",
        if done { 1.0 } else { 0.0 },
        "bool",
        json!({"events": n, "seconds": s.started.elapsed().as_secs_f64()}),
    );
    if !done {
        let peers = b.raw_call(json!({"op":"peers"})).unwrap_or(Value::Null);
        s.note(format!(
            "lane 0 error: {}",
            peers[0]["lanes"][0]["repositories"]
        ));
        s.timeout("150K-event folder not synchronized");
    }
    s.finish();
}

/// One multi-MiB event (above the legacy 349,525-byte cliff) synchronizes.
#[test]
#[ignore = "WP3.INT"]
fn large_event_8mib_daemons() {
    let mut s = Scenario::new(FILE, "large_event_8mib_daemons");
    let a = Node::new();
    let b = Node::new();
    let pa = a.create("Large event", true);
    a.with_stopped_folder(&pa, SETUP, |st| {
        large_event(st, "big", 2400, 200).map(|_| ())
    })
    .unwrap();
    b.pair(&a, &a.code());
    s.measure_from_now();
    let pb = b.receive(&a, &pa);
    let done = wait_until(s.remaining(), || {
        try_rpc(&pb, json!({"op":"state"}))
            .is_ok_and(|st| st["entries"].as_object().is_some_and(|e| e.len() > 2400))
    });
    s.metric("completed", if done { 1.0 } else { 0.0 }, "bool");
    if !done {
        s.timeout("large event not synchronized");
    }
    s.finish();
}
