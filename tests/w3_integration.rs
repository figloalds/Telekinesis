//! Owned by WP3.INT: integration tests of Wave 3 that belong to no package
//! (REVAMP_PLAN §14 WP3.INT). The daemon-level end to end of objects v3 is
//! `tests/orchestrator.rs` (WP3.1's file, run on objects v3 since WP3.INT)
//! and the release scenarios of `tests/qual_wp3_int_e2e.rs`.
mod support;

use serde_json::{Value, json};
use std::{fs, path::Path, time::Duration};
use support::{Node, content_complete, has_file, mount_supported, wait_until};
use tkfs::core::{MAX_SEAL_INTERVAL, MIN_SEAL_INTERVAL, Store, seal_interval};

/// CCR-WP2.4-3 (applied by WP3.INT): the periodic seal interval grows with
/// the sealed payload, one event per 4 KiB, between 128 and 1024 events.
#[test]
fn the_seal_interval_scales_with_the_sealed_payload() {
    assert_eq!(MIN_SEAL_INTERVAL, 128);
    assert_eq!(MAX_SEAL_INTERVAL, 1024);
    assert_eq!(seal_interval(None), 128, "no seal yet");
    assert_eq!(seal_interval(Some(0)), 128);
    assert_eq!(seal_interval(Some(512 * 1024 - 1)), 128);
    assert_eq!(seal_interval(Some(512 * 1024)), 128);
    assert_eq!(seal_interval(Some(1024 * 1024)), 256);
    assert_eq!(seal_interval(Some(2 * 1024 * 1024)), 512);
    assert_eq!(seal_interval(Some(4 * 1024 * 1024)), 1024);
    assert_eq!(seal_interval(Some(64 * 1024 * 1024)), 1024);
}

fn sealed(store: &Store) -> (i64, i64) {
    store
        .db
        .query_row(
            "SELECT cursor,octet_length(payload) FROM startup_snapshots WHERE branch=?",
            [&store.active],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
}

/// A store follows the scaled rule: each periodic seal falls exactly
/// `seal_interval(previous payload)` journal sequences after the previous
/// one, so a projection of about 1 MiB seals every ~256 events instead of
/// every 128; and it still opens (the replay of a tail longer than 128
/// events resumes from the snapshot or rebuilds, never fails).
#[test]
fn a_large_projection_seals_less_often() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), Some("w3-seal-interval")).unwrap();
    let long = "n".repeat(200);
    let mut seals: Vec<(i64, i64)> = vec![sealed(&store)];
    for index in 0..2600 {
        store.create(&format!("{long}-{index:05}"), "file").unwrap();
        let now = sealed(&store);
        if now.0 != seals.last().unwrap().0 {
            seals.push(now);
        }
    }
    assert!(seals.len() >= 3, "{seals:?}");
    let mut longer = 0;
    for pair in seals.windows(2) {
        let (before, after) = (pair[0], pair[1]);
        let interval = seal_interval(Some(before.1));
        assert_eq!(after.0 - before.0, interval, "{seals:?}");
        if interval > MIN_SEAL_INTERVAL {
            longer += 1;
        }
    }
    assert!(longer > 0, "the payload grew past 512 KiB: {seals:?}");
    let expected = store.projection(&store.active).unwrap();
    drop(store);
    let store = Store::open(dir.path(), Some("w3-seal-interval")).unwrap();
    assert_eq!(store.projection(&store.active).unwrap(), expected);
}

fn state_of(folder: &Value) -> String {
    folder["state"].as_str().unwrap().to_owned()
}

/// Whether `path` (inside a mount) holds exactly `bytes`.
fn reads(path: &Path, bytes: &[u8]) -> bool {
    fs::read(path).is_ok_and(|read| read == bytes)
}

/// The end to end of Wave 3 (§14 WP3.INT): two real daemons sync two
/// folders mounted on the filesystem (WinFsp on Windows, FUSE on Linux) over
/// v3 sessions and objects v3, both directions: files and directories
/// written through one mount are read back through the other (metadata and
/// content), as are renames and deletions; the lanes are `tkfs-peer/3`.
#[test]
fn two_daemons_sync_mounted_folders_both_ways_over_v3() {
    if !mount_supported() {
        eprintln!("skipped: no mount support on this machine");
        return;
    }
    let a = Node::new();
    let b = Node::new();
    let mounts = tempfile::tempdir().unwrap();
    let (mount_a, mount_b) = (mounts.path().join("a"), mounts.path().join("b"));
    let pa = a.create_mounted("Mounted v3", &mount_a);
    let mounted =
        |node: &Node, state: &str| node.try_inspect(state).is_ok_and(|p| p["mounted"] == true);
    a.wait_for("A mounted", Duration::from_secs(60), || {
        mounted(&a, &state_of(&pa))
    });
    b.pair(&a, &a.code());
    let pb = b.call_ready(
        json!({"op":"receive","endpoint":a.endpoint(),"repo":pa["repo"],
        "label":pa["label"],"mount":mount_b,"headless":false}),
    );
    b.wait_for("B mounted", Duration::from_secs(60), || {
        mounted(&b, &state_of(&pb))
    });
    let big = |seed: u8| -> Vec<u8> {
        (0..3 * 1024 * 1024u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8 ^ seed)
            .collect()
    };
    let (big_a, big_b) = (big(0xa5), big(0x5a));
    let settle = Duration::from_secs(60);

    // A -> B, written through A's mount.
    fs::write(mount_a.join("from-a.txt"), b"written through A's mount").unwrap();
    fs::create_dir(mount_a.join("dir-a")).unwrap();
    fs::write(mount_a.join("dir-a").join("big-a.bin"), &big_a).unwrap();
    b.wait_for("A's files in B's store", settle, || {
        has_file(&pb, "from-a.txt", b"written through A's mount")
            && has_file(&pb, "big-a.bin", &big_a)
            && content_complete(&pb, 2)
    });
    assert!(reads(
        &mount_b.join("from-a.txt"),
        b"written through A's mount"
    ));
    assert!(reads(&mount_b.join("dir-a").join("big-a.bin"), &big_a));

    // B -> A, written through B's mount.
    fs::write(mount_b.join("from-b.txt"), b"written through B's mount").unwrap();
    fs::create_dir(mount_b.join("dir-b")).unwrap();
    fs::write(mount_b.join("dir-b").join("big-b.bin"), &big_b).unwrap();
    a.wait_for("B's files in A's store", settle, || {
        has_file(&pa, "from-b.txt", b"written through B's mount")
            && has_file(&pa, "big-b.bin", &big_b)
            && content_complete(&pa, 4)
    });
    assert!(reads(
        &mount_a.join("from-b.txt"),
        b"written through B's mount"
    ));
    assert!(reads(&mount_a.join("dir-b").join("big-b.bin"), &big_b));

    // Metadata only: a rename through B's mount and a deletion through A's.
    fs::rename(mount_b.join("from-a.txt"), mount_b.join("renamed-on-b.txt")).unwrap();
    fs::remove_file(mount_a.join("from-b.txt")).unwrap();
    assert!(
        wait_until(settle, || {
            reads(
                &mount_a.join("renamed-on-b.txt"),
                b"written through A's mount",
            ) && !mount_a.join("from-a.txt").exists()
                && !mount_b.join("from-b.txt").exists()
        }),
        "the rename and the deletion did not reach the other mount"
    );

    // Over v3 only.
    let peers = b.peers();
    let lanes = peers[0]["lanes"].as_array().unwrap();
    assert!(lanes.len() >= 2, "{peers}");
    for lane in lanes {
        assert_eq!(lane["transport"], "tkfs-peer/3", "{peers}");
        assert_eq!(lane["connected"], true, "{peers}");
    }
    for (node, folder) in [(&a, &pa), (&b, &pb)] {
        let status = node.raw_call(json!({"op":"status"})).unwrap();
        let ours = status["folders"]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["state"] == folder["state"])
            .cloned()
            .unwrap();
        assert_eq!(ours["mount"]["state"], "attached", "{ours}");
    }
}

/// WP3.INT: a handle's cached view survives another connection's commit that
/// changed no projection row (feed staging, object registration, status
/// writes), instead of reloading the whole projection at its next use; a
/// commit that did change the projection is seen at once.
#[test]
fn a_view_survives_foreign_commits_that_leave_the_projection_alone() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Store::open(dir.path(), Some("w3-view-freshness")).unwrap();
    engine.create("first", "file").unwrap();
    let other = rusqlite::Connection::open(dir.path().join("metadata.sqlite")).unwrap();
    other
        .busy_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    // Loaded once.
    engine.lookup("first").unwrap();
    let loads = engine.view_loads();
    engine.lookup("first").unwrap();
    assert_eq!(engine.view_loads(), loads, "a fresh view is reused");
    // A foreign commit to a table outside the projection: kept.
    other
        .execute(
            "INSERT INTO object_requests VALUES('w3-not-a-projection-row', 1)",
            [],
        )
        .unwrap();
    engine.lookup("first").unwrap();
    assert_eq!(
        engine.view_loads(),
        loads,
        "a commit that left the projection alone keeps the view"
    );
    // A foreign commit that changes a projection row: reloaded, and seen.
    let renamed: String = other
        .query_row(
            "SELECT entry FROM current_parts WHERE entry LIKE '%\"first\"%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let renamed = renamed.replace("\"first\"", "\"changed-elsewhere\"");
    other
        .execute(
            "UPDATE current_parts SET entry=? WHERE entry LIKE '%\"first\"%'",
            [&renamed],
        )
        .unwrap();
    assert!(
        engine.lookup("changed-elsewhere").is_ok(),
        "the change is seen"
    );
    assert!(
        engine.view_loads() > loads,
        "reloaded after a projection change"
    );
    // Its own commits keep the view fresh, also across foreign commits.
    let loads = engine.view_loads();
    engine.create("second", "file").unwrap();
    other
        .execute(
            "INSERT INTO object_requests VALUES('w3-another-row', 2)",
            [],
        )
        .unwrap();
    engine.create("third", "file").unwrap();
    engine.lookup("second").unwrap();
    engine.lookup("third").unwrap();
    assert_eq!(
        engine.view_loads(),
        loads,
        "own commits patch the view; a foreign non-projection commit keeps it"
    );
}

/// The WP3.1 review's minor (WP3.INT): `compat::worker_call`, the `Folder`
/// passthrough's bounded call that replaced `peering::DeadlineLane::call_as`
/// and lost its test with it. A worker that never answers is
/// `REQUEST_TIMEOUT` within the deadline, each call sends its request once
/// (never replayed, invariant 16), and a reply within the deadline is the
/// call's result.
#[test]
fn the_folder_passthrough_call_is_bounded_and_never_replayed() {
    use std::{
        net::TcpListener,
        sync::{Arc, Mutex},
        thread,
        time::Instant,
    };
    use tkfs::{orchestrator::compat::worker_call, runtime::Discovery};
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("ws://{}", listener.local_addr().unwrap());
    let received = Arc::new(Mutex::new(Vec::<String>::new()));
    let server = {
        let received = received.clone();
        thread::spawn(move || {
            for (index, stream) in listener.incoming().take(2).enumerate() {
                let mut socket = tungstenite::accept(stream.unwrap()).unwrap();
                let message = socket.read().unwrap();
                received
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&message.into_data()).into_owned());
                if index == 1 {
                    let reply = json!({"ok": true, "result": {"answered": true}});
                    socket
                        .send(tungstenite::Message::text(reply.to_string()))
                        .unwrap();
                }
                // The first connection never answers; both are held until
                // the client leaves.
                while socket.read().is_ok() {}
            }
        })
    };
    let info = Discovery {
        format: 2,
        address,
        state: String::new(),
        mount: None,
        repo: String::new(),
        device: String::new(),
    };
    let deadline = Duration::from_millis(400);
    let started = Instant::now();
    let error = worker_call(&info, "request-1", json!({"op": "status"}), deadline).unwrap_err();
    let elapsed = started.elapsed();
    assert!(
        format!("{error:#}").contains("REQUEST_TIMEOUT"),
        "{error:#}"
    );
    assert!(
        elapsed >= Duration::from_millis(300) && elapsed < Duration::from_secs(3),
        "answered after {elapsed:?} (deadline {deadline:?})"
    );
    let reply = worker_call(
        &info,
        "request-2",
        json!({"op": "status"}),
        Duration::from_secs(10),
    )
    .unwrap();
    assert_eq!(reply, json!({"answered": true}));
    server.join().unwrap();
    let received = received.lock().unwrap();
    assert_eq!(received.len(), 2, "one request per call: {received:?}");
    assert!(received[0].contains("request-1"), "{received:?}");
    assert!(received[1].contains("request-2"), "{received:?}");
}

/// WP3.INT: the worker's off-lock checkpoint (`maintenance::checkpoint_db`)
/// truncates a WAL of at least `WAL_TRUNCATE_FRAMES` frames once a passive
/// checkpoint copied it in full, but never waits long for a reader still on
/// it (the reader keeps reading); a small WAL is left as it is.
#[test]
fn a_large_copied_wal_is_truncated_once_no_reader_is_on_it() {
    use std::time::Instant;
    use tkfs::worker::maintenance::{WAL_TRUNCATE_FRAMES, checkpoint_db};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), Some("w3-wal-truncate")).unwrap();
    let path = dir.path().join("metadata.sqlite");
    let wal = dir.path().join("metadata.sqlite-wal");
    let wal_bytes = || fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
    let writer = rusqlite::Connection::open(&path).unwrap();
    writer
        .execute_batch("PRAGMA wal_autocheckpoint=0; CREATE TABLE w3_filler(b BLOB);")
        .unwrap();
    // About 80 MiB of frames, more than the threshold.
    let blob = vec![7u8; 1024 * 1024];
    for _ in 0..80 {
        writer
            .execute("INSERT INTO w3_filler VALUES(?)", [&blob])
            .unwrap();
    }
    assert!(
        wal_bytes() >= WAL_TRUNCATE_FRAMES as u64 * 4096,
        "{}",
        wal_bytes()
    );
    let checkpointer = rusqlite::Connection::open(&path).unwrap();
    checkpointer.busy_timeout(Duration::from_secs(5)).unwrap();

    // A reader on the WAL: everything is copied, nothing is truncated, and
    // the attempt gives up quickly.
    let reader = rusqlite::Connection::open(&path).unwrap();
    reader.execute_batch("BEGIN").unwrap();
    let rows = |c: &rusqlite::Connection| -> i64 {
        c.query_row("SELECT count(*) FROM w3_filler", [], |r| r.get(0))
            .unwrap()
    };
    assert_eq!(rows(&reader), 80);
    let started = Instant::now();
    let (log, copied) = checkpoint_db(&checkpointer).unwrap();
    assert!(
        log >= WAL_TRUNCATE_FRAMES && copied == log,
        "{log} {copied}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    assert!(
        wal_bytes() >= WAL_TRUNCATE_FRAMES as u64 * 4096,
        "truncated under a reader"
    );
    assert_eq!(rows(&reader), 80, "the reader still reads its snapshot");
    // The checkpointer's busy timeout is restored.
    let timeout: i64 = checkpointer
        .query_row("PRAGMA busy_timeout", [], |r| r.get(0))
        .unwrap();
    assert_eq!(timeout, 5000);
    reader.execute_batch("COMMIT").unwrap();

    // No reader left: truncated.
    checkpoint_db(&checkpointer).unwrap();
    assert_eq!(wal_bytes(), 0, "the copied WAL is truncated");
    assert_eq!(rows(&writer), 80);

    // A small WAL is only copied.
    writer
        .execute("INSERT INTO w3_filler VALUES(x'00')", [])
        .unwrap();
    let small = wal_bytes();
    assert!(small > 0);
    checkpoint_db(&checkpointer).unwrap();
    assert_eq!(wal_bytes(), small, "a small WAL is left as it is");
    // The store still works.
    drop(store);
    let mut store = Store::open(dir.path(), Some("w3-wal-truncate")).unwrap();
    store.create("after-truncation", "file").unwrap();
}

/// WP3.INT: a startup that resumes its snapshot replays the suffix against
/// one loaded view, patched per event as a fast commit patches it, instead of
/// reloading the whole projection for every suffix event (with suffixes of
/// up to 1024 events since CCR-WP2.4-3, a 150K-entry folder took minutes to
/// open); the reopened projection is the one before.
#[test]
fn a_startup_replays_the_snapshot_suffix_against_one_view() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), Some("w3-suffix")).unwrap();
    for index in 0..200 {
        store.create(&format!("s-{index:04}"), "file").unwrap();
    }
    let (cursor, _) = sealed(&store);
    let tip: i64 = store
        .db
        .query_row("SELECT max(seq) FROM journal_sequence", [], |r| r.get(0))
        .unwrap();
    let suffix = tip - cursor;
    assert!(suffix >= 32, "a suffix of {suffix} events");
    let expected = store.projection(&store.active).unwrap();
    drop(store);
    let store = Store::open(dir.path(), Some("w3-suffix")).unwrap();
    let loads = store.view_loads();
    assert!(
        loads <= 3,
        "{loads} view loads to replay a {suffix}-event suffix"
    );
    assert_eq!(store.projection(&store.active).unwrap(), expected);
    // The suffix was replayed (not rebuilt) and sealed.
    assert_eq!(sealed(&store).0, tip);
}

/// WP3.INT: a session's socket gets a 4 MiB receive buffer on Windows
/// (`session::io::SOCKET_BUFFER_BYTES`), and its send buffer grows to 4 MiB
/// when the socket drains fast. With the defaults (64 KiB) every 1 MiB frame
/// waited for writable readiness and a loopback download ran at 270–360
/// MiB/s, below `session_loopback_throughput`'s 300 MiB/s floor; with them,
/// 3.2–4.0 GiB/s. The send buffer starts at the system's own size and
/// follows the drain rate (WP3.INT re-review: a fixed 4 MiB held control
/// frames behind megabytes of data on a slow uplink; the drain rate test is
/// `objects_v3_lanes::a_saturating_push_does_not_delay_pulls_on_a_slow_uplink`).
/// The options are read through a duplicate of the socket's handle, which
/// reports the send buffer as set but not the receive buffer, so only the
/// send buffer is checked here; the qualification scenario measures the
/// effect of both.
#[cfg(windows)]
#[test]
fn session_sockets_get_large_buffers_on_windows() {
    use std::{
        net::{TcpListener, TcpStream},
        os::windows::io::AsRawSocket,
        sync::Arc,
        thread,
    };
    use tkfs::{
        model::protocol::{LaneRole, frame::ChunkHeader},
        session::{
            Conn, ConnOptions, Handler, Incoming, PeerInfo, Priority, Side, io::SOCKET_BUFFER_BYTES,
        },
    };
    #[link(name = "ws2_32")]
    unsafe extern "system" {
        fn getsockopt(
            socket: usize,
            level: i32,
            name: i32,
            value: *mut u8,
            length: *mut i32,
        ) -> i32;
    }
    let option = |stream: &TcpStream, name: i32| -> i32 {
        let mut value = [0u8; 4];
        let mut length = 4i32;
        // SAFETY: an open socket and a 4-byte buffer with its length.
        let status = unsafe {
            getsockopt(
                stream.as_raw_socket() as usize,
                0xffff,
                name,
                value.as_mut_ptr(),
                &mut length,
            )
        };
        assert_eq!(status, 0);
        i32::from_ne_bytes(value)
    };
    /// Streams `MIB` 1 MiB chunks to the caller.
    struct Download;
    const MIB: usize = 1024 * 1024;
    impl Handler for Download {
        fn handle(&self, req: Incoming) {
            let block = vec![7u8; MIB];
            for index in 0..256u64 {
                let header = ChunkHeader {
                    id: 0,
                    hash: None,
                    offset: index * MIB as u64,
                    total: 256 * MIB as u64,
                    last: index == 255,
                };
                if req.responder.chunk(header, &block).is_err() {
                    return;
                }
            }
            req.responder.reply(json!({}), Vec::new());
        }
    }
    let peer = || PeerInfo {
        installation: "w3".into(),
        role: LaneRole::Control,
        lane: None,
        session: "w3-session".into(),
        verifier_generation: 1,
    };
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let accepted = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let clone = stream.try_clone().unwrap();
        let ws = tungstenite::accept(tungstenite::stream::MaybeTlsStream::Plain(stream)).unwrap();
        (ws, clone)
    });
    let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let client_socket = stream.try_clone().unwrap();
    let default = option(&client_socket, 0x1001);
    assert_ne!(default, SOCKET_BUFFER_BYTES);
    let ws =
        tungstenite::client_tls_with_config(format!("ws://127.0.0.1:{port}/"), stream, None, None)
            .unwrap()
            .0;
    let client = Conn::from_established(ws, Side::Client, peer(), ConnOptions::default()).unwrap();
    let (ws, server_socket) = accepted.join().unwrap();
    let server = Conn::from_established(ws, Side::Server, peer(), ConnOptions::default()).unwrap();
    // Nothing sent yet: both send buffers are the system's.
    for socket in [&client_socket, &server_socket] {
        assert_eq!(option(socket, 0x1001), default, "SO_SNDBUF");
    }
    // A fast download: the server's send buffer stays the system's while
    // the kernel takes every frame at once, and grows to the largest size
    // once the socket refuses bytes while draining fast (it never shrinks
    // below the system's); the client, which sent almost nothing, keeps the
    // system's.
    server.serve(Arc::new(Download));
    let mut received = 0usize;
    client
        .call_streaming(
            "download",
            json!({}),
            Vec::new(),
            Priority::Control,
            Some(Duration::from_secs(120)),
            Duration::from_secs(30),
            &mut |_: ChunkHeader, bytes: &[u8]| {
                received += bytes.len();
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(received, 256 * MIB);
    let grown = option(&server_socket, 0x1001);
    assert!(
        [default, SOCKET_BUFFER_BYTES].contains(&grown),
        "server SO_SNDBUF {grown}"
    );
    assert_eq!(option(&client_socket, 0x1001), default, "client SO_SNDBUF");
    drop((client, server));
}

/// The adaptive send buffer's rule (WP3.INT re-review): it grows to about two
/// round trips of the drain rate on a fast link (the largest size on
/// loopback), shrinks on a slow one, never below the system's own size, and
/// a window in which the socket took everything changes nothing.
#[test]
fn the_send_buffer_follows_the_drain_rate() {
    use std::time::Instant;
    use tkfs::session::io::{SEND_BUFFER_MIN, SEND_BUFFER_WINDOW, SendBuffer};
    const MIB: usize = 1024 * 1024;
    let max = 4 * MIB;
    let start = Instant::now();
    let mut buffer = SendBuffer::new(max, start);
    let mut at = start;
    let mut window = |buffer: &mut SendBuffer, bytes: usize, saturated: bool, rtt: u64| {
        buffer.took(bytes);
        if saturated {
            buffer.refused();
        }
        at += SEND_BUFFER_WINDOW;
        buffer.resize(at, rtt)
    };
    // Not saturated: nothing changes, however fast.
    assert_eq!(window(&mut buffer, 512 * MIB, false, 1), None);
    assert_eq!(buffer.size(), None);
    // Loopback, saturated at 300 MiB/s: the largest size.
    assert_eq!(window(&mut buffer, 75 * MIB, true, 1), Some(max));
    // The same again: no change.
    assert_eq!(window(&mut buffer, 75 * MIB, true, 1), None);
    // A slow uplink (128 KiB/s, short round trip): the smallest size.
    assert_eq!(
        window(&mut buffer, 32 * 1024, true, 1),
        Some(SEND_BUFFER_MIN)
    );
    // A long fat link (12.5 MB/s at 200 ms): two round trips, 5 MB (the
    // largest size).
    let mut fat = SendBuffer::new(max, start);
    fat.took(3_125_000);
    fat.refused();
    assert_eq!(fat.resize(start + SEND_BUFFER_WINDOW, 200), Some(max));
    // A buffer-bound link grows (rate = size / round trip).
    let mut slow = SendBuffer::new(max, start);
    let mut at = start;
    let mut size = 0usize;
    for _ in 0..8 {
        let current = slow.size().unwrap_or(SEND_BUFFER_MIN);
        // 100 ms round trip: a quarter-second window moves 2.5 buffers.
        slow.took(current * 5 / 2);
        slow.refused();
        at += SEND_BUFFER_WINDOW;
        if let Some(next) = slow.resize(at, 100) {
            assert!(next > size, "{next} after {size}");
            size = next;
        }
    }
    assert_eq!(size, max);
    // A fresh buffer on a slow link keeps the system's size.
    let mut fresh = SendBuffer::new(max, start);
    fresh.took(16 * 1024);
    fresh.refused();
    assert_eq!(fresh.resize(start + SEND_BUFFER_WINDOW, 5), None);
    assert_eq!(fresh.size(), None);
}

/// WP4.INT (the WP4.5 review's minor): a loopback socket's send buffer
/// (`SendBuffer::for_loopback`) holds two `LOOPBACK_SEND_HORIZON_MS` of data
/// at its drain rate, whatever round trip the session measured (a session
/// to a proxy or an emulator on this machine has the far link's), clamped
/// to [`SEND_BUFFER_MIN`, max].
#[test]
fn a_loopback_send_buffer_follows_its_own_horizon() {
    use std::time::Instant;
    use tkfs::session::io::{
        LOOPBACK_SEND_HORIZON_MS, SEND_BUFFER_MIN, SEND_BUFFER_WINDOW, SendBuffer,
    };
    const MIB: usize = 1024 * 1024;
    let max = 4 * MIB;
    // 25 MiB in a 250 ms window: 100 MiB/s, two 10 ms horizons: 2 MiB.
    let sized = |rtt: u64, bytes: usize| -> Option<usize> {
        let start = Instant::now();
        let mut buffer = SendBuffer::for_loopback(max, start);
        buffer.took(bytes);
        buffer.refused();
        buffer.resize(start + SEND_BUFFER_WINDOW, rtt)
    };
    let expected = (100.0 * MIB as f64 * 2.0 * LOOPBACK_SEND_HORIZON_MS as f64 / 1000.0) as usize;
    for rtt in [1, 50, 100, 400, u64::MAX] {
        let size = sized(rtt, 25 * MIB).expect("a saturated window resizes");
        assert!(
            size.abs_diff(expected) < 64 * 1024,
            "rtt {rtt}: {size} (expected about {expected})"
        );
    }
    // Clamped to the largest size at loopback rates, and never below the
    // smallest.
    assert_eq!(sized(50, 200 * MIB), Some(max));
    assert_eq!(sized(400, 256 * 1024), None, "the system's size is kept");
    let _ = SEND_BUFFER_MIN;
}
