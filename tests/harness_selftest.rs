//! WP0.5 harness self-test: the fault proxy, the hung server, fixtures, metrics,
//! the legacy fixtures and the compatibility goldens.
//!
//! Capture (baseline 6298d0b only; never re-run after Wave 0):
//! `cargo test --test harness_selftest -- --ignored capture_ --test-threads=1`
//! writes `tests/fixtures/legacy/**` and `tests/fixtures/compat/*.json`.
mod support;

use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};
use support::*;
use tkfs::{
    core::{Store, hash, id},
    runtime::Engine,
    ws,
};

// ---------------------------------------------------------------- fault proxy

/// A raw TCP echo server; returns its address.
fn echo_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            thread::spawn(move || {
                let mut buf = vec![0; 64 * 1024];
                loop {
                    match stream.read(&mut buf) {
                        Ok(0) | Err(_) => return,
                        Ok(n) => {
                            if stream.write_all(&buf[..n]).is_err() {
                                return;
                            }
                        }
                    }
                }
            });
        }
    });
    addr
}

fn round_trip(stream: &mut TcpStream, bytes: &[u8]) -> std::io::Result<Vec<u8>> {
    stream.write_all(bytes)?;
    let mut out = vec![0; bytes.len()];
    stream.read_exact(&mut out)?;
    Ok(out)
}

fn connect(proxy: &FaultProxy) -> TcpStream {
    let s = TcpStream::connect(proxy.addr()).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    s
}

#[test]
fn proxy_relays_bytes_and_websocket_messages() {
    let proxy = FaultProxy::start(&echo_server());
    let mut s = connect(&proxy);
    let payload = seeded_bytes(1, 256 * 1024);
    assert_eq!(round_trip(&mut s, &payload).unwrap(), payload);
    assert!(wait_until(Duration::from_secs(2), || proxy.bytes_down()
        == payload.len() as u64));
    assert_eq!(proxy.bytes_up(), payload.len() as u64);
    assert_eq!(proxy.connections(), 1);
    assert_eq!(proxy.active(), 1);
    drop(s);
    assert!(wait_until(Duration::from_secs(2), || proxy.active() == 0));

    // WebSocket request/response through the proxy (the daemon wire).
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let target = format!("ws://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let mut socket = ws::accept(listener.accept().unwrap().0).unwrap();
        for _ in 0..2 {
            let request: Value = ws::receive(&mut socket).unwrap();
            ws::reply(&mut socket, Ok(request)).unwrap();
        }
    });
    let proxy = FaultProxy::start(&target);
    let mut socket = ws::connect(&proxy.endpoint()).unwrap();
    for n in 0..2 {
        let request = json!({"n": n, "hex": hex::encode(seeded_bytes(n, 4096))});
        assert_eq!(ws::call(&mut socket, &request).unwrap(), request);
    }
    server.join().unwrap();
}

#[test]
fn proxy_stalls_and_adds_latency() {
    let proxy = FaultProxy::start(&echo_server());
    let mut s = connect(&proxy);
    round_trip(&mut s, b"warm").unwrap();
    proxy.stall_for(Duration::from_millis(600));
    let (_, ms) = time_ms(|| round_trip(&mut s, b"stalled").unwrap());
    assert!(ms >= 550.0, "stall_for: {ms} ms");

    proxy.stall_after(1000, Duration::from_millis(500));
    let (_, fast) = time_ms(|| round_trip(&mut s, &[1; 100]).unwrap());
    assert!(fast < 400.0, "before threshold: {fast} ms");
    let (_, slow) = time_ms(|| round_trip(&mut s, &[2; 2000]).unwrap());
    assert!(slow >= 450.0, "stall_after: {slow} ms");
    let (_, again) = time_ms(|| round_trip(&mut s, &[3; 2000]).unwrap());
    assert!(again < 400.0, "stall_after is one-shot: {again} ms");

    proxy.set_latency(Duration::from_millis(100));
    let (_, rtt) = time_ms(|| round_trip(&mut s, b"latency").unwrap());
    assert!((190.0..800.0).contains(&rtt), "latency rtt: {rtt} ms");
    // Pipelined, not stop-and-wait: 20 chunks in flight still take ~one RTT.
    let (_, burst) = time_ms(|| {
        for _ in 0..20 {
            s.write_all(&[9; 1000]).unwrap();
            thread::sleep(Duration::from_millis(2));
        }
        let mut out = vec![0; 20_000];
        s.read_exact(&mut out).unwrap();
    });
    assert!(burst < 900.0, "latency is pipelined: {burst} ms");
    proxy.clear();
    let (_, clear) = time_ms(|| round_trip(&mut s, b"clear").unwrap());
    assert!(clear < 150.0, "clear: {clear} ms");
}

#[test]
fn proxy_caps_bandwidth_within_20_percent() {
    // A sink that reports when it has read everything.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let total = 2 * 1024 * 1024;
    let sink = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = vec![0; 64 * 1024];
        let mut read = 0;
        let mut first = None;
        while read < total {
            let n = stream.read(&mut buf).unwrap();
            assert!(n > 0);
            first.get_or_insert_with(Instant::now);
            read += n;
        }
        first.unwrap().elapsed()
    });
    let proxy = FaultProxy::start(&addr);
    let rate = 1024 * 1024;
    proxy.set_bandwidth(rate);
    let mut s = connect(&proxy);
    let data = seeded_bytes(3, total);
    let writer = thread::spawn(move || s.write_all(&data).unwrap());
    let elapsed = sink.join().unwrap().as_secs_f64();
    writer.join().unwrap();
    let expected = total as f64 / rate as f64;
    let ratio = elapsed / expected;
    assert!(
        (0.8..=1.2).contains(&ratio),
        "bandwidth cap: {elapsed:.2} s for {expected:.2} s ({ratio:.2}x)"
    );
}

#[test]
fn proxy_resets_blackholes_and_refuses() {
    let proxy = FaultProxy::start(&echo_server());
    let mut s = connect(&proxy);
    round_trip(&mut s, b"before reset").unwrap();
    proxy.reset_all();
    let mut buf = [0; 16];
    let read = s.read(&mut buf);
    assert!(matches!(read, Ok(0) | Err(_)), "reset_all: {read:?}");
    assert_eq!(proxy.resets(), 1);

    let mut s = connect(&proxy);
    proxy.reset_after(4096);
    let result = round_trip(&mut s, &[7; 8192]).and_then(|_| round_trip(&mut s, &[8; 64]));
    assert!(result.is_err(), "reset_after did not drop the connection");
    assert!(proxy.resets() >= 2);

    let mut s = connect(&proxy);
    round_trip(&mut s, b"open").unwrap();
    proxy.blackhole(true);
    s.set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    s.write_all(b"dropped").unwrap();
    assert!(s.read(&mut buf).is_err(), "blackhole relayed bytes");
    assert_eq!(proxy.active(), 1, "blackhole keeps the connection");
    proxy.blackhole(false);

    proxy.refuse(true);
    let mut refused = TcpStream::connect(proxy.addr()).unwrap();
    refused
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let _ = refused.write_all(b"hello");
    assert!(matches!(refused.read(&mut buf), Ok(0) | Err(_)));
    assert!(wait_until(Duration::from_secs(2), || proxy.refused() == 1));
    proxy.refuse(false);
    let mut s = connect(&proxy);
    assert_eq!(round_trip(&mut s, b"after").unwrap(), b"after");
}

// ---------------------------------------------------------------- hung server

#[test]
fn hung_server_accepts_upgrade_and_never_replies() {
    let server = HungServer::start();
    let mut socket = ws::connect(&server.endpoint()).unwrap();
    if let tungstenite::stream::MaybeTlsStream::Plain(tcp) = socket.get_mut() {
        tcp.set_read_timeout(Some(Duration::from_millis(1500)))
            .unwrap();
    }
    let started = Instant::now();
    let reply = ws::call(&mut socket, &json!({"op":"status"}));
    assert!(reply.is_err(), "hung server replied: {reply:?}");
    assert!(started.elapsed() >= Duration::from_millis(1400));
    assert!(wait_until(Duration::from_secs(2), || server.received() == 1));
    assert_eq!(server.connections(), 1);

    let state = tempfile::tempdir().unwrap();
    let original = json!({"format":2,"address":"ws://127.0.0.1:1","state":"s","mount":null,"repo":"r","device":"d"});
    fs::write(state.path().join("runtime.json"), original.to_string()).unwrap();
    let redirect = redirect_runtime_json(state.path(), &server.endpoint()).unwrap();
    let redirected = discovery_at(state.path()).unwrap();
    assert_eq!(redirected.address, server.endpoint());
    assert_eq!(redirected.repo, "r");
    redirect.restore().unwrap();
    assert_eq!(
        discovery_at(state.path()).unwrap().address,
        "ws://127.0.0.1:1"
    );
}

// ---------------------------------------------------------------- fixtures

fn stores(n: usize, repo: &str) -> (tempfile::TempDir, Vec<Store>) {
    let dir = tempfile::tempdir().unwrap();
    let stores = (0..n)
        .map(|i| Store::open(&dir.path().join(format!("s{i}")), Some(repo)).unwrap())
        .collect();
    (dir, stores)
}

fn visible(store: &Store) -> BTreeSet<(String, Option<String>)> {
    let p = store.projection(&store.active).unwrap();
    p.entries
        .values()
        .filter(|e| e.alive)
        .map(|e| (e.name.clone(), e.content.clone()))
        .collect()
}

/// Seeded fixtures are deterministic. For `concurrent_histories` the contract
/// is the intent log, the sync schedule and the committed history read back
/// from the stores (`ConcurrentSummary::history`, ids replaced by names); the
/// projected tie-breaks between concurrent operations depend on random ids and
/// are compared across the replicas of one run only.
#[test]
fn fixtures_are_deterministic_per_seed() {
    let run = |seed| {
        let (dir, mut s) = stores(3, "fixture-concurrent");
        let summary = concurrent_histories(&mut s, 60, seed).unwrap();
        let first = s[0].shared_event_inventory().unwrap();
        for other in &s[1..] {
            assert_eq!(other.shared_event_inventory().unwrap(), first);
            assert_eq!(visible(other), visible(&s[0]), "replicas diverged");
        }
        drop(s);
        drop(dir);
        summary
    };
    let a = run(7);
    let b = run(7);
    let c = run(8);
    assert_eq!(a.log, b.log, "same seed, same operations");
    assert_eq!(a.syncs, b.syncs);
    assert_ne!(a.log, c.log, "different seed, different operations");
    assert_eq!(a.log.len(), 60);
    // Store state, not just the RNG: the same committed events for the same seed.
    assert_eq!(a.history, b.history, "same seed, same committed history");
    assert_ne!(a.history, c.history, "different seed, different history");
    assert_eq!(
        a.history.len() + a.refused,
        a.log.len(),
        "one committed event per accepted operation"
    );

    assert_eq!(seeded_bytes(5, 64), seeded_bytes(5, 64));
    assert_ne!(seeded_bytes(5, 64), seeded_bytes(6, 64));

    let objects = |count| {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = Engine::new(Store::open(dir.path(), Some("fixture-objects")).unwrap());
        let hashes = small_objects(&mut engine, count, 100).unwrap();
        for h in &hashes {
            assert!(engine.store.object_available(h), "{h}");
        }
        let large = large_object(&mut engine, 3 * 1024 * 1024 + 17).unwrap();
        assert_eq!(
            engine.store.object_size(&large).unwrap(),
            3 * 1024 * 1024 + 17
        );
        assert_eq!(
            engine
                .store
                .lookup("objects/obj-000004.bin")
                .unwrap()
                .content,
            Some(hashes[4].clone())
        );
        (hashes, large)
    };
    let (h1, l1) = objects(25);
    let (h2, l2) = objects(25);
    assert_eq!(h1, h2);
    assert_eq!(l1, l2);
    assert_eq!(
        h1.iter().collect::<BTreeSet<_>>().len(),
        25,
        "unique objects"
    );

    let (_dir, mut s) = stores(2, "fixture-linear");
    let ids = linear_history(&mut s[0], 40).unwrap();
    assert_eq!(ids.len(), 40);
    assert!(s[0].lookup("lin-000039.txt").is_ok());
    let (first, rest) = s.split_at_mut(1);
    let backlog = two_way_backlog(&mut first[0], &mut rest[0], 30).unwrap();
    assert_eq!(backlog.bundle.events.len(), 30);
    assert!(
        rest[0]
            .events()
            .unwrap()
            .iter()
            .any(|e| e.id == backlog.local_edit)
    );
    assert!(
        !first[0]
            .events()
            .unwrap()
            .iter()
            .any(|e| e.id == backlog.local_edit)
    );
    let paged = pages(&backlog.bundle, 7);
    assert_eq!(paged.len(), 5);
    for page in paged {
        rest[0].receive(page, &backlog.allowed).unwrap();
    }
    assert!(rest[0].lookup("backlog-000029.txt").is_ok());

    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), Some("fixture-staged")).unwrap();
    let staged = staged_inbox(&mut store, 12).unwrap();
    assert_eq!(staged.len(), 12);
    let rows: i64 = store
        .db
        .query_row("SELECT count(*) FROM incoming_events", [], |r| r.get(0))
        .unwrap();
    assert_eq!(rows, 12);
    assert!(
        store.lookup("staged-000000.txt").is_err(),
        "staged, not active"
    );
    drop(store);
    let store = Store::open_existing(dir.path()).unwrap();
    assert!(store.lookup("staged-000011.txt").is_ok(), "startup drain");

    let clone_dir = tempfile::tempdir().unwrap();
    let (original, clone) = clone_replica(store, &clone_dir.path().join("clone")).unwrap();
    assert_eq!(
        original.shared_event_inventory().unwrap(),
        clone.shared_event_inventory().unwrap()
    );
    assert_ne!(original.device, clone.device);
    assert_eq!(original.repo, clone.repo);
    // The clone resumes from the original's snapshot re-stamped with its device
    // (a fallback rebuild would have re-serialized it in struct field order).
    let payload = |s: &Store| -> String {
        s.db.query_row(
            "SELECT payload FROM startup_snapshots WHERE branch=?",
            [&s.active],
            |r| r.get(0),
        )
        .unwrap()
    };
    let mut expected: Value = serde_json::from_str(&payload(&original)).unwrap();
    expected["device"] = json!(clone.device);
    assert_eq!(payload(&clone), serde_json::to_string(&expected).unwrap());

    replicas_have_receiver_rows();
}

fn hashes_in(store: &Store, table: &str) -> BTreeSet<String> {
    store
        .db
        .prepare(&format!("SELECT hash FROM {table}"))
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// `clone_replica` and `metadata_replica` hold the rows a real receiver holds:
/// no author-only publication grants, acknowledged outbox, knowledge that the
/// author holds every event, and (metadata only) no availability for objects
/// whose bytes were not received.
fn replicas_have_receiver_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut author = Store::open(&dir.path().join("author"), Some("fixture-clone")).unwrap();
    let hashes = small_objects_in(&mut author, "objs", 5, 100, 3).unwrap();
    let published = hashes_in(&author, "published_objects");
    assert!(
        hashes.iter().all(|h| published.contains(h)),
        "author grants"
    );
    assert!(author.outgoing_unacknowledged().unwrap() > 0);

    let (author, clone) = clone_replica(author, &dir.path().join("clone")).unwrap();
    assert_eq!(
        hashes_in(&author, "published_objects"),
        published,
        "original untouched"
    );
    assert!(hashes_in(&clone, "published_objects").is_empty());
    assert_eq!(hashes_in(&clone, "received_shared_objects"), published);
    assert_eq!(clone.outgoing_unacknowledged().unwrap(), 0);
    assert_eq!(
        clone.known_by_peer(&author.device).unwrap(),
        author.shared_event_inventory().unwrap()
    );
    assert_eq!(
        clone.shared_object_inventory().unwrap(),
        author.shared_object_inventory().unwrap()
    );
    assert_eq!(clone.content_status().unwrap()["complete"], true);

    let (author, meta) = metadata_replica(author, &dir.path().join("meta")).unwrap();
    let empty = hash(b"");
    assert_eq!(
        meta.shared_event_inventory().unwrap(),
        author.shared_event_inventory().unwrap()
    );
    assert!(hashes_in(&meta, "published_objects").is_empty());
    assert!(
        hashes_in(&meta, "received_shared_objects")
            .iter()
            .all(|h| *h == empty)
    );
    assert!(hashes.iter().all(|h| !meta.object_available(h)));
    assert!(
        meta.shared_object_inventory()
            .unwrap()
            .iter()
            .all(|h| *h == empty)
    );
    assert_eq!(meta.outgoing_unacknowledged().unwrap(), 0);
    assert_eq!(meta.content_status().unwrap()["complete"], false);
}

/// Large fixtures write rows directly (`direct_creates`); the result must equal a
/// full re-projection of the journal, reopen from its sealed snapshot, and accept
/// further local and remote work.
#[test]
fn direct_rows_match_full_projection() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut s = Store::open(&dir.path().join("a"), Some(&repo)).unwrap();
    let n = DIRECT_ROWS + 300;
    let ids = linear_history(&mut s, n).unwrap();
    assert_eq!(ids.len(), n);
    let hashes = small_objects_in(&mut s, "objs", DIRECT_ROWS + 50, 40, 9).unwrap();
    let check = |s: &Store| {
        let events: Vec<_> = s
            .events()
            .unwrap()
            .into_iter()
            .filter(|e| e.branch.id == s.active)
            .collect();
        let full = tkfs::core::project(&s.active, &events).unwrap();
        assert_eq!(s.projection(&s.active).unwrap(), full, "derived state");
        assert!(s.conflicts().unwrap().is_empty());
    };
    check(&s);
    assert_eq!(
        s.lookup("objs/obj-000007.bin").unwrap().content,
        Some(hashes[7].clone())
    );
    assert!(s.object_available(&hashes[DIRECT_ROWS + 49]));
    drop(s);
    let mut s = Store::open_existing(&dir.path().join("a")).unwrap();
    check(&s);
    s.create("after-direct.txt", "file").unwrap();
    check(&s);
    let status = s.content_status().unwrap();
    assert_eq!(status["complete"], true, "{status}");
    // The sealed snapshot resumes without a full rebuild: a reopen is quick.
    drop(s);
    let (reopened, ms) = time_ms(|| Store::open_existing(&dir.path().join("a")).unwrap());
    check(&reopened);
    assert!(ms < 30_000.0, "reopen took {ms} ms");
}

// ---------------------------------------------------------------- node helpers

/// `Node::control()` works from another thread, always through the token path
/// (the daemon's own WP4.1 token when it wrote one, otherwise a dummy token
/// file, which a pre-WP4.1 daemon ignores); `CallCounts` tells answered,
/// refused and unreachable calls apart; `with_stopped_folder` edits the store
/// only after the worker has exited.
#[test]
fn node_control_and_stopped_folder_edit() {
    let node = Node::new();
    let folder = node.create("Edit", true);
    let token = node.data_dir().join("orchestrator.token");
    let dummy_token = !token.exists();
    if dummy_token {
        fs::write(&token, "selftest-token\n").unwrap();
    }
    let counts = std::sync::Arc::new(CallCounts::default());
    let (control, thread_counts) = (node.control(), counts.clone());
    thread::spawn(move || {
        thread_counts.record(&control.call(&json!({"op":"list"})));
        thread_counts.record(&control.call(&json!({"op":"inspect","state":"no-such-folder"})));
    })
    .join()
    .unwrap();
    let unreachable = Control {
        data_dir: node.path().join("missing"),
    };
    counts.record(&unreachable.call(&json!({"op":"list"})));
    assert_eq!(counts.get(), (1, 1, 1), "{}", counts.json());
    if dummy_token {
        fs::remove_file(&token).unwrap();
    }

    let dir = state_dir(&folder);
    let before = node.worker_pids(&folder);
    assert!(!before.is_empty(), "worker running");
    let edited = node
        .with_stopped_folder(&folder, Duration::from_secs(60), |store| {
            assert!(
                !worker_listening(&dir),
                "worker still listening during the edit"
            );
            Ok(store.create("edited-offline.txt", "file")?.id)
        })
        .unwrap();
    assert!(!edited.is_empty());
    let after = node.inspect(&state_id(&folder));
    assert_eq!(after["running"], true);
    assert!(
        wait_until(Duration::from_secs(10), || has_hash(
            &after,
            "edited-offline.txt",
            &hash(b"")
        )),
        "offline edit visible after restart"
    );
    assert!(
        node.worker_pids(&folder)
            .iter()
            .all(|p| !before.contains(p)),
        "a new worker process"
    );
}

// ---------------------------------------------------------------- metrics

#[test]
fn metrics_percentiles_cpu_and_pids() {
    let samples: Vec<f64> = (1..=100).map(f64::from).collect();
    assert_eq!(percentile(&samples, 50.0), 50.0);
    assert_eq!(percentile(&samples, 99.0), 99.0);
    assert_eq!(percentile(&samples, 100.0), 100.0);
    assert_eq!(percentile(&[3.0], 99.0), 3.0);
    let s = summarize(&samples);
    assert_eq!((s.count, s.min, s.max, s.p90), (100, 1.0, 100.0, 90.0));
    assert!(percentile(&[], 50.0).is_nan());
    // No samples: NaN statistics (never 0.0, which would pass every `<` floor),
    // emitted as null, failing every floor.
    let empty = summarize(&[]);
    assert_eq!(empty.count, 0);
    for v in [
        empty.min, empty.mean, empty.p50, empty.p90, empty.p99, empty.max,
    ] {
        assert!(v.is_nan(), "{empty:?}");
    }
    for op in ["<", "<=", ">", ">=", "=="] {
        assert!(!meets(&json!({"op":op,"value":50}), empty.p99), "{op}");
    }
    assert_eq!(empty.json()["p99"], Value::Null);
    assert_eq!(empty.json()["count"], 0);

    let pid = std::process::id();
    let before = process_cpu_ms(pid).expect("own cpu time");
    let watch = Stopwatch::start();
    let mut x = 0u64;
    while watch.ms() < 300.0 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
    }
    assert!(x != 1);
    let after = process_cpu_ms(pid).unwrap();
    assert!(
        after > before,
        "cpu time did not advance: {before} → {after}"
    );

    assert!(meets(&json!({"op":"<=","value":5}), 5.0));
    assert!(!meets(&json!({"op":"<","value":5}), 5.0));
    assert!(meets(&json!({"op":">=","value":40}), 41.0));
    assert!(!meets(&json!({"op":"<=","value":5}), f64::NAN));

    // A real worker process: WorkerProc, find_pids and per-PID CPU.
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    drop(Store::initialize(&state, &id(), &id()).unwrap());
    let mut worker = WorkerProc::spawn(&state, &[], &[]).unwrap();
    let ready = worker.wait_ready(Duration::from_secs(30)).unwrap();
    assert_eq!(ready["mounted"], false);
    let worker_pid = worker.pid().unwrap();
    assert!(process_cpu_ms(worker_pid).is_some());
    let needle = state.to_string_lossy().into_owned();
    assert!(
        find_pids(&needle).contains(&worker_pid),
        "find_pids({needle}) missed {worker_pid}"
    );
    assert_eq!(
        worker.rpc(json!({"op":"status"})).unwrap()["mounted"],
        false
    );
    worker.stop().unwrap();
    assert!(worker.try_exit().is_some());
}

// ---------------------------------------------------------------- shapes

#[test]
fn normalization_and_shape_rules() {
    let state = id();
    let known = vec![(state.clone(), "<state:a>".to_string())];
    let raw = json!({
        "state": state, "other": id(), "label": "Literal label",
        "directory": r"C:\Users\x\data\states\abc", "unix": "/tmp/x",
        "endpoint": "ws://127.0.0.1:5555", "last_success_ms": 17, "retry_at_ms": null,
        "connections": 3, "exchanges": 99, "ratio": 0.5, "running": true,
        "transport": "tkfs-binary-gzip-v1", "error": "WORKER_BUSY: engine locked",
        "publication_error": null,
        "repositories": {state.clone(): {"caught_up": true}},
        "lanes": [{"lane": 0, "role": "control"}],
    });
    let n = normalize(&raw, &known);
    assert_eq!(n["state"], "<state:a>");
    assert_eq!(n["other"], "<id>");
    assert_eq!(n["label"], "Literal label");
    assert_eq!(n["directory"], "<path>");
    assert_eq!(n["unix"], "<path>");
    assert_eq!(n["endpoint"], "<endpoint>");
    assert_eq!(n["last_success_ms"], "<ms?>");
    assert_eq!(n["retry_at_ms"], "<ms?>");
    assert_eq!(n["connections"], 3);
    assert_eq!(n["exchanges"], "<u64>");
    assert_eq!(n["ratio"], "<f64>");
    assert_eq!(n["transport"], "<transport>");
    assert_eq!(n["error"], "<error:WORKER_BUSY>");
    assert_eq!(n["publication_error"], Value::Null);
    assert_eq!(n["repositories"]["<state:a>"]["caught_up"], true);
    assert_eq!(n["lanes"][0]["lane"], 0);

    // The raw value has the shape of its own normalization.
    assert_same_shape(&n, &raw);
    // Additive members are accepted; removed members are not.
    let mut extended = raw.clone();
    extended["status"] = json!({"lifecycle": {"phase": "running"}});
    assert_same_shape(&n, &extended);
    let mut removed = raw.clone();
    removed.as_object_mut().unwrap().remove("running");
    assert!(!shape_problems(&n, &removed).is_empty());
    let mut retyped = raw.clone();
    retyped["running"] = json!("yes");
    assert!(!shape_problems(&n, &retyped).is_empty());
    let mut nullable = raw.clone();
    nullable["last_success_ms"] = Value::Null;
    nullable["error"] = Value::Null;
    assert_same_shape(&n, &nullable);
    // Dynamic keys match the marker template.
    let mut dynamic = raw.clone();
    dynamic["repositories"] = json!({id(): {"caught_up": false}, id(): {"caught_up": true}});
    assert_same_shape(&n, &dynamic);
    dynamic["repositories"] = json!({id(): {"caught_up": "no"}});
    assert!(!shape_problems(&n, &dynamic).is_empty());
    // Projection ignores additive members for identical re-capture.
    assert_eq!(project_onto(&n, &normalize(&extended, &known)), n);
    assert_eq!(
        error_code("x: PAIRING_CODE_CHANGED: y"),
        "PAIRING_CODE_CHANGED"
    );
    assert_eq!(error_code("plain text"), "TEXT");
}

// ---------------------------------------------------------------- legacy fixtures

/// Fixture copies hold only committed files: SQLite side files left next to a
/// fixture database (ignored by git) are not copied.
#[test]
fn legacy_copies_skip_sqlite_side_files() {
    let src = tempfile::tempdir().unwrap();
    fs::create_dir_all(src.path().join("nested")).unwrap();
    for name in [
        "metadata.sqlite",
        "metadata.sqlite-wal",
        "metadata.sqlite-shm",
        "nested/folders.sqlite",
        "nested/folders.sqlite-journal",
        "nested/keep.txt",
    ] {
        fs::write(src.path().join(name), name).unwrap();
    }
    let dst = tempfile::tempdir().unwrap();
    legacy::copy_fixture(src.path(), &dst.path().join("copy")).unwrap();
    let copy = dst.path().join("copy");
    for kept in [
        "metadata.sqlite",
        "nested/folders.sqlite",
        "nested/keep.txt",
    ] {
        assert!(copy.join(kept).is_file(), "{kept} copied");
    }
    assert_eq!(legacy::sqlite_side_files(&copy), Vec::<PathBuf>::new());
    assert_eq!(legacy::sqlite_side_files(src.path()).len(), 3);
    // The real copies, before anything opens them.
    let worker = copy_legacy_worker();
    assert_eq!(
        legacy::sqlite_side_files(&worker.state),
        Vec::<PathBuf>::new()
    );
    let datadir = copy_legacy_datadir();
    assert_eq!(
        legacy::sqlite_side_files(&datadir.data),
        Vec::<PathBuf>::new()
    );
}

#[test]
fn legacy_worker_fixture_opens_with_current_build() {
    let fx = copy_legacy_worker();
    let m = fx.manifest.clone();
    assert!(
        legacy::tree_size(&legacy::legacy_dir().join("worker")) <= 2 * 1024 * 1024,
        "fixture over 2 MB"
    );
    {
        let store = Store::open_existing(&fx.state).unwrap();
        assert_eq!(store.repo, m["repo"].as_str().unwrap());
        assert_eq!(store.device, m["device"].as_str().unwrap());
        let events: i64 = store
            .db
            .query_row("SELECT count(*) FROM events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(events, m["events"].as_i64().unwrap());
        let conflicts = store.conflicts().unwrap();
        assert!(
            conflicts.iter().any(|c| c.resolved_by.is_none()),
            "unresolved"
        );
        assert!(
            conflicts.iter().any(|c| c.resolved_by.is_some()),
            "resolved"
        );
        let count = |sql: &str| -> i64 { store.db.query_row(sql, [], |r| r.get(0)).unwrap() };
        assert_eq!(
            count("SELECT count(*) FROM incoming_events"),
            m["incoming"]["total"].as_i64().unwrap(),
            "staged rows are never dropped"
        );
        assert_eq!(
            count("SELECT count(*) FROM incoming_events WHERE error IS NOT NULL"),
            m["incoming"]["quarantined"].as_i64().unwrap()
        );
        let partial = store.partial_objects().unwrap();
        let spool = m["partial"]["hash"].as_str().unwrap();
        assert_eq!(
            partial.get(spool).copied(),
            m["partial"]["offset"].as_u64(),
            "partial spool kept"
        );
        let mut engine = Engine::new(store);
        engine.retry_pending().unwrap();
        assert_eq!(
            engine
                .store
                .lookup(m["pending_save"]["path"].as_str().unwrap())
                .unwrap()
                .content
                .as_deref(),
            m["pending_save"]["content"].as_str(),
            "pending save recovered"
        );
    }
    // The real binary opens a second copy and recovers the pending save.
    let fx = copy_legacy_worker();
    let mut worker = WorkerProc::spawn(&fx.state, &[], &[]).unwrap();
    let ready = worker.wait_ready(Duration::from_secs(60)).unwrap();
    assert_eq!(ready["repo"], m["repo"]);
    let path = m["pending_save"]["path"].as_str().unwrap().to_owned();
    let content = m["pending_save"]["content"].as_str().unwrap().to_owned();
    assert!(
        wait_until(Duration::from_secs(20), || {
            worker.rpc(json!({"op":"state"})).is_ok_and(|s| {
                s["entries"].as_object().is_some_and(|e| {
                    e.values()
                        .any(|e| e["name"] == path && e["content"] == content)
                })
            })
        }),
        "worker did not recover the pending save:\n{}",
        worker.log_tail()
    );
    worker.stop().unwrap();
}

#[test]
fn legacy_datadir_fixture_opens_with_current_build() {
    assert!(
        legacy::tree_size(&legacy::legacy_dir().join("datadir")) <= 2 * 1024 * 1024,
        "fixture over 2 MB"
    );
    let fx = copy_legacy_datadir();
    let m = fx.manifest.clone();
    let node = Node::adopt_root(fx.root, NodeConfig::default());
    let list = node.list();
    assert_eq!(list["installation"], m["installation"]);
    let labels: BTreeSet<_> = list["folders"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["label"].as_str().unwrap().to_owned())
        .collect();
    let expected: BTreeSet<_> = m["folders"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["label"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(labels, expected);
    let peers = node.peers();
    assert_eq!(peers.as_array().unwrap().len(), 1);
    // The copy's peer is moved to a closed port (no dials into parallel tests).
    assert_eq!(peers[0]["endpoint"], legacy::DEAD_PEER_ENDPOINT);
    assert_eq!(peers[0]["installation"], m["peers"][0]["installation"]);
    for folder in m["folders"].as_array().unwrap() {
        let state = folder["state"].as_str().unwrap();
        let inspect = node.inspect(state);
        assert_eq!(inspect["desired_running"], folder["desired_running"]);
        assert_eq!(
            inspect["initial_sync_pending"], folder["initial_sync"],
            "{inspect}"
        );
        if let Some(name) = folder["file"].as_str() {
            node.wait_for("legacy folder running", Duration::from_secs(60), || {
                node.try_inspect(state).is_ok_and(|p| p["running"] == true)
            });
            let p = node.inspect(state);
            assert!(
                wait_until(Duration::from_secs(10), || has_hash(
                    &p,
                    name,
                    folder["content"].as_str().unwrap()
                )),
                "legacy content missing"
            );
        }
    }
    // The verifier survived: rotating the code still works and changes it.
    let verifier = || -> String {
        let db = rusqlite::Connection::open(node.data_dir().join("folders.sqlite")).unwrap();
        db.query_row("SELECT value FROM settings WHERE key='pairing'", [], |r| {
            r.get(0)
        })
        .unwrap()
    };
    let before = verifier();
    assert!(before.contains("salt"));
    node.code();
    assert_ne!(verifier(), before);
}

// ---------------------------------------------------------------- compat goldens

const GOLDENS: &[&str] = &[
    "create_starting",
    "inspect_running_headless",
    "inspect_gated_received",
    "inspect_mounted",
    "list",
    "peers_dialer",
    "peers_acceptor",
    "stop",
    "shutdown",
];

struct CompatRun {
    responses: BTreeMap<String, Value>,
    known: Vec<(String, String)>,
}

fn any_error(value: &Value) -> bool {
    match value {
        Value::Object(map) => map.iter().any(|(k, v)| {
            ((k == "error" || k.ends_with("_error")) && !v.is_null()) || any_error(v)
        }),
        Value::Array(items) => items.iter().any(any_error),
        _ => false,
    }
}

fn lanes_ready(sync: &Value) -> bool {
    sync["connected"] == true
        && sync["lanes"].as_array().is_some_and(|lanes| {
            !lanes.is_empty()
                && lanes
                    .iter()
                    .all(|l| l["connected"] == true && l["repository"].is_object())
        })
}

/// Register a folder's volatile ids under symbolic names (`<state:tag>`, ...).
fn name(known: &mut Vec<(String, String)>, folder: &Value, tag: &str, repo: bool) {
    known.push((state_id(folder), format!("<state:{tag}>")));
    known.push((
        folder["device"].as_str().unwrap().into(),
        format!("<device:{tag}>"),
    ));
    if repo {
        known.push((
            folder["repo"].as_str().unwrap().into(),
            format!("<repo:{tag}>"),
        ));
    }
}

/// The baseline daemon responses for the §7.3 compatibility projection.
/// `datadir`: also copy the dialer's data directory there (legacy fixture capture).
fn compat_scenario(datadir: Option<&Path>) -> CompatRun {
    let a = Node::new();
    let mut b = Node::new();
    let mut responses = BTreeMap::new();
    let mut known = vec![
        (a.installation(), "<installation:A>".to_string()),
        (b.installation(), "<installation:B>".to_string()),
    ];

    // Create (starting) and the shared source folder.
    let created =
        a.call(json!({"op":"create","label":"Compat source","mount":null,"headless":true}));
    assert_eq!(created["starting"], true, "{created}");
    responses.insert("create_starting".into(), created.clone());
    a.wait_for("source running", Duration::from_secs(30), || {
        a.try_inspect(&state_id(&created))
            .is_ok_and(|p| p["running"] == true)
    });
    let source = a.inspect(&state_id(&created));
    name(&mut known, &source, "source", true);
    write_file(&source, "hello.txt", b"compat hello");

    // A source whose metadata can never complete: one child event without its parent.
    let gated_source = a.create("Compat gated source", true);
    name(&mut known, &gated_source, "gated-source", true);
    let author_dir = tempfile::tempdir().unwrap();
    let mut author = Store::open(
        author_dir.path(),
        Some(gated_source["repo"].as_str().unwrap()),
    )
    .unwrap();
    author.create("parent", "directory").unwrap();
    author.create("parent/child", "directory").unwrap();
    let child: Vec<_> = author
        .events()
        .unwrap()
        .into_iter()
        .filter(|e| !e.dependencies.is_empty())
        .collect();
    assert_eq!(child.len(), 1);
    let repo = gated_source["repo"].as_str().unwrap();
    let gated_device = gated_source["device"].as_str().unwrap();
    let route = legacy_route(&id(), repo, gated_device, &author.device);
    let page = legacy_page(repo, &author.device, gated_device, &child, &BTreeSet::new());
    rpc(
        &gated_source,
        json!({"op":"published-exchange","route":route,"page":page,"metadata_only":true}),
    );
    assert_eq!(
        rpc(&gated_source, json!({"op":"status"}))["incoming_pending"],
        1
    );

    // Pairing, a headless receive and a gated mounted receive on B.
    let code = a.code();
    b.code();
    b.pair(&a, &code);
    let received = b.receive(&a, &source);
    name(&mut known, &received, "received", false);
    let gated = b.call(json!({"op":"receive","endpoint":a.endpoint(),"repo":repo,
        "label":"Compat gated","mount":b.path().join("gated-mount"),"headless":false}));
    name(&mut known, &gated, "gated", false);
    let mounted = if mount_supported() {
        let p = a.create_mounted("Compat mounted", &a.path().join("compat-mount"));
        name(&mut known, &p, "mounted", true);
        Some(p)
    } else {
        None
    };
    let stop_target = a.create("Compat stop", true);
    name(&mut known, &stop_target, "stop", true);

    // Steady state on the dialer.
    let received_state = state_id(&received);
    let gated_state = state_id(&gated);
    b.wait_for("compat steady state", Duration::from_secs(90), || {
        let (Ok(r), Ok(g), Ok(peers)) = (
            b.try_inspect(&received_state),
            b.try_inspect(&gated_state),
            b.raw_call(json!({"op":"peers"})),
        ) else {
            return false;
        };
        r["running"] == true
            && r["sync"][0]["remote_available"] == true
            && r["sync"][0]["metadata"]["caught_up"] == true
            && lanes_ready(&r["sync"][0])
            && g["startup_phase"] == "synchronizing remote metadata"
            && g["initial_sync_pending"] == true
            && g["sync"][0]["metadata"]["last_success_ms"].is_number()
            && lanes_ready(&g["sync"][0])
            && peers[0]["lanes"].as_array().is_some_and(|lanes| {
                lanes.iter().all(|l| {
                    l["connected"] == true
                        && l["repositories"].as_object().is_some_and(|r| r.len() == 2)
                })
            })
            && has_file(&received, "hello.txt", b"compat hello")
    });
    let mut captured = BTreeMap::new();
    for attempt in 0..20 {
        captured.clear();
        captured.insert("inspect_running_headless", b.inspect(&received_state));
        captured.insert("inspect_gated_received", b.inspect(&gated_state));
        captured.insert("list", b.list());
        captured.insert("peers_dialer", b.peers());
        captured.insert("peers_acceptor", a.peers());
        if let Some(p) = &mounted {
            captured.insert("inspect_mounted", a.inspect(&state_id(p)));
        }
        if !captured.values().any(any_error)
            && lanes_ready(&captured["inspect_running_headless"]["sync"][0])
        {
            break;
        }
        assert!(
            attempt < 19,
            "transient errors never cleared: {captured:#?}"
        );
        thread::sleep(Duration::from_millis(500));
    }
    for (k, v) in captured {
        responses.insert(k.to_string(), v);
    }
    let stopped = a.call(json!({"op":"stop","state":state_id(&stop_target)}));
    responses.insert("stop".into(), stopped);

    // A stopped local folder for the legacy datadir (after List was captured).
    if datadir.is_some() {
        let local = b.create("Compat local", true);
        write_file(&local, "local.txt", b"compat local");
        b.stop_folder(&state_id(&local));
    }
    let (reply, status) = b.shutdown_within(Duration::from_secs(15));
    let status = status.expect("dialer exits after shutdown");
    assert!(status.success(), "{status}");
    responses.insert("shutdown".into(), reply.unwrap());
    if let Some(dest) = datadir {
        capture_datadir(&b, dest, &known);
    }
    drop(author);
    CompatRun { responses, known }
}

#[test]
fn compat_goldens_recapture_identically() {
    let run = compat_scenario(None);
    let mut compared = 0;
    for name in GOLDENS {
        let path = compat_dir().join(format!("{name}.json"));
        let Some(actual) = run.responses.get(*name) else {
            assert_eq!(*name, "inspect_mounted", "missing response {name}");
            continue; // no mount support on this machine
        };
        if !path.is_file() {
            assert_eq!(*name, "inspect_mounted", "missing golden {name}");
            continue;
        }
        let golden = golden(name);
        assert_same_shape(&golden, actual);
        let normalized = normalize(actual, &run.known);
        let projected = project_onto(&golden, &normalized);
        assert_eq!(
            projected,
            golden,
            "{name} changed:\n{}",
            serde_json::to_string_pretty(&normalized).unwrap()
        );
        compared += 1;
    }
    assert!(compared >= GOLDENS.len() - 1);
}

#[test]
#[ignore = "WP0.5 capture (baseline 6298d0b only)"]
fn capture_compat_goldens_and_legacy_datadir() {
    let dest = legacy::legacy_dir().join("datadir");
    let _ = fs::remove_dir_all(&dest);
    let run = compat_scenario(Some(&dest));
    fs::create_dir_all(compat_dir()).unwrap();
    for (name, value) in &run.responses {
        let normalized = normalize(value, &run.known);
        fs::write(
            compat_dir().join(format!("{name}.json")),
            serde_json::to_string_pretty(&normalized).unwrap() + "\n",
        )
        .unwrap();
    }
    assert!(legacy::tree_size(&dest) <= 2 * 1024 * 1024);
}

fn git_head() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_default()
}

fn remove_logs(root: &Path) {
    for entry in fs::read_dir(root).unwrap().flatten() {
        let path = entry.path();
        if path.is_dir() {
            remove_logs(&path);
        } else if path.extension().is_some_and(|e| e == "log")
            || path.file_name().is_some_and(|n| n == "runtime.json")
        {
            fs::remove_file(path).unwrap();
        }
    }
}

/// Copy the stopped dialer's data directory and describe it in `manifest.json`.
fn capture_datadir(b: &Node, dest: &Path, known: &[(String, String)]) {
    let data = dest.join("data");
    copy_dir(&b.data_dir(), &data).unwrap();
    legacy::checkpoint_tree(&data).unwrap();
    remove_logs(&data);
    let db = rusqlite::Connection::open(data.join("folders.sqlite")).unwrap();
    let installation: String = db
        .query_row(
            "SELECT value FROM settings WHERE key='installation'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let directory: String = db
        .query_row("SELECT directory FROM projects LIMIT 1", [], |r| r.get(0))
        .unwrap();
    let data_dir = directory
        .rsplit_once("states")
        .map(|(prefix, _)| prefix.trim_end_matches(['\\', '/']).to_owned())
        .unwrap();
    let root = data_dir
        .rsplit_once(['\\', '/'])
        .map(|(prefix, _)| prefix.to_owned())
        .unwrap();
    let gates: BTreeSet<String> = db
        .prepare("SELECT state FROM initial_sync")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    let folders: Vec<Value> = db
        .prepare("SELECT state,label,repo,desired FROM projects ORDER BY label,state")
        .unwrap()
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, bool>(3)?,
            ))
        })
        .unwrap()
        .map(|row| {
            let (state, label, repo, desired) = row.unwrap();
            let file = match label.as_str() {
                "Compat source" if desired => {
                    json!({"file":"hello.txt","content":hash(b"compat hello")})
                }
                _ => json!({}),
            };
            let mut folder = json!({"state":state,"label":label,"repo":repo,
                "desired_running":desired,"initial_sync":gates.contains(&state)});
            for (k, v) in file.as_object().unwrap() {
                folder[k] = v.clone();
            }
            folder
        })
        .collect();
    let peers: Vec<Value> = db
        .prepare("SELECT endpoint,installation,connections FROM peers ORDER BY endpoint")
        .unwrap()
        .query_map([], |r| {
            Ok(
                json!({"endpoint":r.get::<_, String>(0)?,"installation":r.get::<_, String>(1)?,
                "connections":r.get::<_, i64>(2)?}),
            )
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    drop(db);
    let manifest = json!({
        "format": 1,
        "captured_with": {"commit": git_head(), "binary": "tkfs (WP0.5 baseline worktree)"},
        "description": "Dialer data directory after a graceful Shutdown: a received running folder, a received folder gated by initial_sync, a stopped local folder, one outbound peer and a pairing verifier.",
        "installation": installation,
        "data_directory": data_dir,
        "root": root,
        "folders": folders,
        "peers": peers,
        "verifier": true,
        "symbols": known.iter().map(|(v, n)| json!({"value":v,"name":n})).collect::<Vec<_>>(),
    });
    fs::write(
        dest.join("manifest.json"),
        serde_json::to_string_pretty(&manifest).unwrap() + "\n",
    )
    .unwrap();
}

// ---------------------------------------------------------------- legacy worker capture

/// The exact payload layout of `runtime::PendingSave` at 6298d0b (private there):
/// field order matters for byte-identical rows.
#[derive(serde::Serialize)]
struct PendingSaveRecord {
    branch: String,
    entry: tkfs::core::Entry,
    stage: String,
    content_base: Option<String>,
    alive_base: Option<String>,
    preserve_times: u8,
}

#[test]
#[ignore = "WP0.5 capture (baseline 6298d0b only)"]
fn capture_legacy_worker_fixture() {
    let dest = legacy::legacy_dir().join("worker");
    let _ = fs::remove_dir_all(&dest);
    let temp = tempfile::tempdir().unwrap();
    let repo = id();
    let state = temp.path().join("state");
    let mut b = Store::initialize(&state, &repo, &id()).unwrap();
    let mut a = Store::open(&temp.path().join("author"), Some(&repo)).unwrap();
    let allowed: BTreeSet<String> = [a.device.clone(), b.device.clone()].into();

    // Shared base.
    a.create("docs", "directory").unwrap();
    for (path, bytes) in [
        ("docs/readme.txt", b"readme v1".as_slice()),
        ("notes.txt", b"notes v1"),
        ("plain.txt", b"plain"),
    ] {
        let e = a.create(path, "file").unwrap();
        a.write_revision(&e.id, e.revisions.get("content").cloned(), bytes)
            .unwrap();
    }
    sync_into(&a, &mut b, &allowed).unwrap();

    // Two concurrent content edits; B resolves one of the conflicts.
    for (path, side_a, side_b) in [
        (
            "notes.txt",
            b"notes from A".as_slice(),
            b"notes from B".as_slice(),
        ),
        ("docs/readme.txt", b"readme from A", b"readme from B"),
    ] {
        let ea = a.lookup(path).unwrap();
        a.write_revision(&ea.id, ea.revisions.get("content").cloned(), side_a)
            .unwrap();
        let eb = b.lookup(path).unwrap();
        b.write_revision(&eb.id, eb.revisions.get("content").cloned(), side_b)
            .unwrap();
    }
    sync_into(&a, &mut b, &allowed).unwrap();
    sync_into(&b, &mut a, &allowed).unwrap();
    let readme = b.lookup("docs/readme.txt").unwrap().id;
    let conflict = b
        .conflicts()
        .unwrap()
        .into_iter()
        .find(|c| c.entity == readme && c.field == "content")
        .expect("readme conflict");
    let winner = conflict.alternatives[0].revision.clone();
    b.resolve(std::slice::from_ref(&conflict.id), &winner)
        .unwrap();
    sync_into(&b, &mut a, &allowed).unwrap();

    // A large file whose metadata is delivered without bytes, then a partial spool.
    let large = seeded_bytes(42, 256 * 1024);
    let e = a.create("large.bin", "file").unwrap();
    a.write_revision(&e.id, e.revisions.get("content").cloned(), &large)
        .unwrap();
    let mut bundle = missing_bundle(&a, &b).unwrap();
    bundle.objects.clear();
    b.receive(bundle, &allowed).unwrap();
    let large_hash = hash(&large);
    let offset = 64 * 1024;
    let part = tkfs::core::ObjectPart {
        offset: 0,
        total: large.len() as u64,
        hex: hex::encode(&large[..offset]),
    };
    let completed = b
        .receive_parts_data(BTreeMap::from([(large_hash.clone(), part)]), &[], &allowed)
        .unwrap();
    assert!(completed.is_empty());

    // Quarantined rows: an invalid event and a descendant that depends on it.
    let bad = foreign_event(&a, &a.device, invalid_kind_changes("bad.txt")).unwrap();
    let mut child = a.event(
        &a.branch(&a.active.clone()).unwrap(),
        invalid_kind_changes("child-of-bad.txt"),
    );
    child.dependencies = vec![bad.id.clone()];
    for e in [bad.clone(), child.clone()] {
        b.receive(
            tkfs::core::Bundle {
                repo: repo.clone(),
                branches: vec![],
                events: vec![e],
                objects: BTreeMap::new(),
            },
            &allowed,
        )
        .unwrap();
    }

    // Staged rows waiting for a parent that never arrives.
    let before = a.shared_event_inventory().unwrap();
    a.create("pending-parent", "directory").unwrap();
    a.create("pending-parent/child-1", "directory").unwrap();
    a.create("pending-parent/child-2", "directory").unwrap();
    let mut new = a.shared_bundle(&before).unwrap();
    causal_sort(&mut new.events);
    new.events.remove(0); // withhold the parent
    new.objects.clear();
    b.receive(new, &allowed).unwrap();

    // A pending save, written exactly as Engine::retain_pending does at 6298d0b.
    let e = b.create("draft.txt", "file").unwrap();
    b.write_revision(&e.id, e.revisions.get("content").cloned(), b"saved draft")
        .unwrap();
    let draft = b.lookup("draft.txt").unwrap();
    let unsaved = b"unsaved draft bytes kept across a crash".to_vec();
    let stage = format!("{}.stage", id());
    fs::create_dir_all(state.join("staging")).unwrap();
    fs::write(state.join("staging").join(&stage), &unsaved).unwrap();
    let record = PendingSaveRecord {
        branch: b.active.clone(),
        entry: draft.clone(),
        stage,
        content_base: draft.revisions.get("content").cloned(),
        alive_base: draft.revisions.get("alive").cloned(),
        preserve_times: 0,
    };
    b.db.execute_batch(
        "CREATE TABLE IF NOT EXISTS pending_saves(entity TEXT PRIMARY KEY,payload TEXT NOT NULL)",
    )
    .unwrap();
    b.db.execute(
        "INSERT INTO pending_saves VALUES(?,?) ON CONFLICT(entity) DO UPDATE SET payload=excluded.payload",
        rusqlite::params![draft.id, serde_json::to_string(&record).unwrap()],
    )
    .unwrap();

    // Describe and copy.
    let count = |sql: &str| -> i64 { b.db.query_row(sql, [], |r| r.get(0)).unwrap() };
    let conflicts = b.conflicts().unwrap();
    let manifest = json!({
        "format": 1,
        "captured_with": {"commit": git_head(), "library": "tkfs (WP0.5 baseline worktree)"},
        "description": "Worker state with resolved and unresolved conflicts, staged rows waiting for a missing parent, quarantined rows, a partial spool and one pending save.",
        "repo": b.repo,
        "device": b.device,
        "author_device": a.device,
        "events": count("SELECT count(*) FROM events"),
        "conflicts": {"unresolved": conflicts.iter().filter(|c| c.resolved_by.is_none()).count(),
                      "resolved": conflicts.iter().filter(|c| c.resolved_by.is_some()).count()},
        "incoming": {"total": count("SELECT count(*) FROM incoming_events"),
                     "quarantined": count("SELECT count(*) FROM incoming_events WHERE error IS NOT NULL"),
                     "waiting": count("SELECT count(*) FROM incoming_events WHERE error IS NULL")},
        "partial": {"hash": large_hash, "offset": offset, "total": large.len()},
        "pending_save": {"path": "draft.txt", "content": hash(&unsaved)},
    });
    assert_eq!(manifest["incoming"]["quarantined"], 2, "{manifest}");
    assert_eq!(manifest["incoming"]["waiting"], 2, "{manifest}");
    assert!(manifest["conflicts"]["unresolved"].as_u64().unwrap() >= 1);
    assert!(manifest["conflicts"]["resolved"].as_u64().unwrap() >= 1);
    drop(a);
    drop(b);
    copy_dir(&state, &dest.join("state")).unwrap();
    legacy::checkpoint_tree(&dest.join("state")).unwrap();
    fs::write(
        dest.join("manifest.json"),
        serde_json::to_string_pretty(&manifest).unwrap() + "\n",
    )
    .unwrap();
    assert!(legacy::tree_size(&dest) <= 2 * 1024 * 1024);
}

// ---------------------------------------------------------------- qualification files

/// Owner of each `tests/qual_*.rs` file (REVAMP_PLAN §14 WP0.5).
const QUAL_FILES: &[(&str, &str)] = &[
    ("qual_wp1_4_worker", "WP1.4"),
    ("qual_wp1_5_mount", "WP1.5"),
    ("qual_wp1_6_activation", "WP1.6"),
    ("qual_wp1_7_derive", "WP1.7"),
    ("qual_wp1_8_objects", "WP1.8"),
    ("qual_wp1_9_session", "WP1.9"),
    ("qual_wp1_12_legacy", "WP1.12"),
    ("qual_wp2_1_lifecycle", "WP2.1"),
    ("qual_wp2_3_worker", "WP2.3"),
    ("qual_wp2_4_offlock", "WP2.4"),
    ("qual_wp2_5_feed", "WP2.5"),
    ("qual_wp3_1_transport", "WP3.1"),
    ("qual_wp3_2_objects", "WP3.2"),
    ("qual_wp3_int_e2e", "WP3.INT"),
    ("qual_scale", "WP4.2"),
];

fn tests_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests")
}

#[test]
fn qual_files_exist_and_ignores_name_their_owner() {
    for (file, owner) in QUAL_FILES {
        let path = tests_dir().join(format!("{file}.rs"));
        let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{file}: {e}"));
        assert!(text.contains("mod support;"), "{file} uses the harness");
        for line in text
            .lines()
            .filter(|l| l.trim_start().starts_with("#[ignore"))
        {
            assert!(
                line.contains(&format!("#[ignore = \"{owner}\"]")),
                "{file}: ignore must name its owner {owner}: {line}"
            );
        }
    }
}

/// `scripts/qualify.sh` really runs (argument parsing and file selection) from
/// an LF and from a CRLF copy, as `bash scripts/qualify.sh …` does from WSL on
/// a Windows checkout (core.autocrlf=true). Regression: a CR-stripping
/// prologue that deleted newlines instead made every run a silent exit 0.
#[cfg(unix)]
#[test]
fn qualify_sh_runs_with_lf_and_crlf() {
    let source =
        fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/qualify.sh"))
            .unwrap()
            .replace("\r\n", "\n");
    for (kind, text) in [
        ("lf", source.clone()),
        ("crlf", source.replace('\n', "\r\n")),
    ] {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir_all(root.path().join("scripts")).unwrap();
        let script = root.path().join("scripts/qualify.sh");
        fs::write(&script, text).unwrap();
        for (args, message) in [
            (&["--bogus-arg"][..], "unknown argument --bogus-arg"),
            (&["--wave", "9"][..], "use --wave 1..4"),
            (
                &["--wave", "1", "--files", "qual_none", "--no-build"][..],
                "no qualification files selected",
            ),
        ] {
            let out = std::process::Command::new("bash")
                .arg(&script)
                .args(args)
                .output()
                .unwrap();
            let stderr = String::from_utf8_lossy(&out.stderr);
            assert_eq!(out.status.code(), Some(2), "{kind} {args:?}: {stderr}");
            assert!(stderr.contains(message), "{kind} {args:?}: {stderr}");
        }
    }
}

/// `scripts/qualify.ps1 -SelfTest` under Windows PowerShell 5.1 (the host the
/// script documents) and, when installed, PowerShell 7: the process runner
/// must report pass only for exit 0 with an `ok.` result line, fail for any
/// other exit code or result, and timeout when killed. Regression: under 5.1
/// `Start-Process -PassThru` left ExitCode `$null`, so every file was "fail".
#[cfg(windows)]
#[test]
fn qualify_ps1_runner_reports_exit_codes() {
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/qualify.ps1");
    let mut hosts = 0;
    for host in ["powershell", "pwsh"] {
        let out = match std::process::Command::new(host)
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
            ])
            .arg("-File")
            .arg(&script)
            .arg("-SelfTest")
            .output()
        {
            Ok(out) => out,
            Err(e) if host == "pwsh" && e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => panic!("{host}: {e}"),
        };
        hosts += 1;
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            out.status.success() && stdout.contains("qualify selftest: ok"),
            "{host}: {:?}
{stdout}
{stderr}",
            out.status
        );
    }
    assert!(hosts >= 1);
}

#[test]
fn floors_reference_existing_scenarios() {
    let doc: Value = serde_json::from_slice(&fs::read(floors_path()).unwrap()).unwrap();
    let floors = doc["floors"].as_array().unwrap();
    assert!(floors.len() >= 20);
    let mut ids = BTreeSet::new();
    for floor in floors {
        let id = floor["id"].as_str().unwrap();
        assert!(ids.insert(id.to_owned()), "duplicate floor {id}");
        let scenario = floor["scenario"].as_str().unwrap();
        let (file, test) = scenario.split_once("::").unwrap();
        let text = fs::read_to_string(tests_dir().join(format!("{file}.rs")))
            .unwrap_or_else(|e| panic!("{id}: {file}: {e}"));
        assert!(
            text.contains(&format!("fn {test}(")),
            "{id}: no scenario {scenario}"
        );
        assert!(
            floor["metric"].is_string() && floor["value"].is_number(),
            "{id}"
        );
        assert!(
            matches!(floor["op"].as_str(), Some("<" | "<=" | ">" | ">=" | "==")),
            "{id}"
        );
        assert!((1..=4).contains(&floor["wave"].as_u64().unwrap()), "{id}");
    }
}

/// The committed baseline evidence is strict JSON (no UTF-8 BOM, which
/// serde_json rejects) and records every qualification file.
#[test]
fn baseline_evidence_is_strict_json() {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test-evidence/qualification-baseline.json");
    let bytes = fs::read(&path).unwrap();
    assert!(!bytes.starts_with(&[0xef, 0xbb, 0xbf]), "UTF-8 BOM");
    let doc: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(doc["kind"], "baseline");
    let files: BTreeSet<_> = doc["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["file"].as_str().unwrap().to_owned())
        .collect();
    for (file, _) in QUAL_FILES {
        assert!(files.contains(*file), "baseline evidence lacks {file}");
    }
    // File statuses agree with the recorded exit code and result line (the
    // PowerShell 5.1 runner bug recorded "fail" with no exit code).
    for f in doc["files"].as_array().unwrap() {
        let result = f["result"].as_str().unwrap_or("");
        let expected = match (f["status"].as_str().unwrap(), f["exit_code"].as_i64()) {
            ("timeout", None) => continue,
            (_, Some(0)) if result.starts_with("test result: ok.") => "pass",
            (_, Some(_)) => "fail",
            (_, None) => panic!("{} has no exit code: {f}", f["file"]),
        };
        assert_eq!(f["status"], expected, "{}", f["file"]);
    }
}
