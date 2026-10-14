//! Owned by WP2.INT (REVAMP_PLAN §14 WP2.INT task 2): daemon-level checks of
//! the Wave 2 wiring, which need several Wave 2 packages at once: the actor
//! orchestrator (WP2.1), the management API v2 and its CLI (WP2.2), the
//! background-activating workers (WP2.3) and the feed (WP2.5).
//!
//! - `Retry` recovers a `Failed` folder;
//! - `Attach` after fixing a mount path mounts in place (no worker restart);
//! - pause and resume on the legacy lanes, on either side;
//! - `tkfs manage status` and `tkfs manage watch` show lifecycle and mount;
//! - a `subscribe` sees a change within 1 s.
//!
//! And the CCRs WP2.INT applied that change behaviour: the feed tip in
//! `published-summary` and the heartbeat (CCR-WP2.5-5), readers after a
//! caller's rollback (CCR-WP2.4-6), the in-process `caught_up` rule
//! (CCR-WP2.3-1).
mod support;

use serde_json::{Value, json};
use std::{
    fs,
    io::{BufRead, BufReader},
    process::{Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};
use support::{
    Node, bin, has_file, mount_supported, rpc, state_dir, state_id, try_rpc, wait_until, write_file,
};
use tkfs::{
    core::{Store, id},
    model::status::{STATUS_SCHEMA, StatusSnapshot},
    runtime::Engine,
};

const START: Duration = Duration::from_secs(60);

/// The folder's member of the daemon's `status` snapshot.
fn folder_status(node: &Node, state: &str) -> Option<Value> {
    let snapshot = node.raw_call(json!({"op":"status"})).ok()?;
    snapshot["folders"]
        .as_array()?
        .iter()
        .find(|folder| folder["state"] == state)
        .cloned()
}

fn phase(node: &Node, state: &str) -> Option<String> {
    folder_status(node, state)
        .and_then(|folder| folder["lifecycle"]["phase"].as_str().map(str::to_owned))
}

fn worker_pid(node: &Node, state: &str) -> Option<u64> {
    folder_status(node, state).and_then(|folder| folder["lifecycle"]["pid"].as_u64())
}

/// Creates a headless folder and waits until its worker runs.
fn running_folder(node: &Node, label: &str) -> (Value, String) {
    let folder = node.create(label, true);
    let state = state_id(&folder);
    node.wait_for("folder running", START, || {
        node.try_inspect(&state).is_ok_and(|p| p["running"] == true)
    });
    (node.inspect(&state), state)
}

/// Stops a folder and waits until its worker process is gone.
fn stopped(node: &Node, folder: &Value, state: &str) {
    let _ = node.raw_call(json!({"op":"stop","state":state}));
    node.wait_for("folder stopped", START, || {
        node.try_inspect(state)
            .is_ok_and(|p| p["running"] == false && p["starting"] == false)
    });
    node.wait_for("worker gone", START, || node.worker_pids(folder).is_empty());
}

// ---------------------------------------------------------------------------
// §14 WP2.INT task 2

/// A folder whose worker exits with a Fatal error is `Failed` at once (§3.3);
/// once the cause is fixed, `retry` starts it again.
#[test]
fn retry_recovers_a_failed_folder() {
    let a = Node::new();
    let (folder, state) = running_folder(&a, "Retry me");
    write_file(&folder, "before.txt", b"before");
    stopped(&a, &folder, &state);

    // Take the store away: the next worker exits with a Fatal
    // `STATE_NOT_INITIALIZED`, and the controller moves to Failed.
    let db = state_dir(&folder).join("metadata.sqlite");
    let aside = state_dir(&folder).join("metadata.sqlite.aside");
    fs::rename(&db, &aside).unwrap();
    a.call(json!({"op":"start","state":state}));
    a.wait_for("folder failed", START, || {
        phase(&a, &state).as_deref() == Some("failed")
    });
    let failed = folder_status(&a, &state).unwrap();
    assert!(
        failed.to_string().contains("STATE_NOT_INITIALIZED"),
        "{failed:#}"
    );
    assert_eq!(failed["sync"]["state"], "needs_attention", "{failed:#}");
    // Failed is not left by itself.
    thread::sleep(Duration::from_secs(2));
    assert_eq!(phase(&a, &state).as_deref(), Some("failed"));

    // Fix the cause, then retry.
    fs::rename(&aside, &db).unwrap();
    let reply = a.call(json!({"op":"retry","state":state}));
    assert_eq!(reply["state"], state.as_str(), "{reply}");
    a.wait_for("folder running after retry", START, || {
        a.try_inspect(&state).is_ok_and(|p| p["running"] == true)
    });
    // The status snapshot is rebuilt at most 4 times a second.
    a.wait_for("status shows running", Duration::from_secs(5), || {
        phase(&a, &state).as_deref() == Some("running")
    });
    let inspect = a.inspect(&state);
    assert!(has_file(&inspect, "before.txt", b"before"));
    write_file(&inspect, "after.txt", b"after");
    assert!(has_file(&inspect, "after.txt", b"after"));
}

/// A mount path that is in the way leaves the folder Running headless with
/// `attach_failed` (NeedsUserAction); after the user moves it aside, `attach`
/// mounts the folder in place: the same worker process serves the mount.
#[test]
fn attach_after_fixing_the_mount_path_keeps_the_worker() {
    if !mount_supported() {
        eprintln!("skipped: no mount support on this machine");
        return;
    }
    let a = Node::new();
    let mount = a.path().join("occupied");
    let created =
        a.call(json!({"op":"create","label":"Attach later","mount":mount,"headless":false}));
    let state = state_id(&created);
    a.wait_for("folder running", START, || {
        a.try_inspect(&state).is_ok_and(|p| p["running"] == true)
    });
    stopped(&a, &created, &state);
    fs::create_dir_all(&mount).unwrap();
    fs::write(mount.join("user.txt"), b"user data").unwrap();
    a.call(json!({"op":"start","state":state}));
    a.wait_for("running headless with attach_failed", START, || {
        a.try_inspect(&state).is_ok_and(|p| {
            p["running"] == true
                && p["mounted"] == false
                && p["status"]["mount"]["state"]
                    .to_string()
                    .contains("attach_failed")
        })
    });
    let pid = worker_pid(&a, &state).expect("worker pid");
    let inspect = a.inspect(&state);
    write_file(&inspect, "synced.txt", b"synced while headless");

    // The user moves the directory aside, then asks for the mount.
    fs::rename(&mount, a.path().join("moved-aside")).unwrap();
    let reply = a.call(json!({"op":"attach","state":state}));
    assert_eq!(reply["state"], state.as_str(), "{reply}");
    a.wait_for("mounted", START, || {
        a.try_inspect(&state).is_ok_and(|p| p["mounted"] == true)
    });
    assert_eq!(worker_pid(&a, &state), Some(pid), "attached in place");
    a.wait_for(
        "status shows the mount attached",
        Duration::from_secs(5),
        || {
            folder_status(&a, &state).is_some_and(|folder| {
                let mount = folder["mount"]["state"].to_string();
                mount.contains("attached") && !mount.contains("attach_failed")
            })
        },
    );
    // The mount serves the folder's content.
    assert!(wait_until(Duration::from_secs(10), || fs::read(
        mount.join("synced.txt")
    )
    .ok()
    .as_deref()
        == Some(b"synced while headless".as_slice())));
    assert_eq!(
        fs::read(a.path().join("moved-aside").join("user.txt")).unwrap(),
        b"user data"
    );
    stopped(&a, &inspect, &state);
}

/// Pause and resume of a received folder on the legacy lanes: while the
/// receiving side (the dialer) is paused nothing arrives, and the same while
/// the serving side (the acceptor) is paused; each resume delivers the
/// change.
#[test]
fn pause_and_resume_on_the_legacy_lanes() {
    let a = Node::new();
    let b = Node::new();
    let code = a.code();
    b.pair(&a, &code);
    let (fa, state_a) = running_folder(&a, "Paused share");
    let received = b.receive(&a, &fa);
    let state_b = state_id(&received);
    b.wait_for("received folder running", START, || {
        b.try_inspect(&state_b).is_ok_and(|p| p["running"] == true)
    });
    let fb = b.inspect(&state_b);
    write_file(&fa, "one.txt", b"one");
    b.wait_for("one.txt at B", START, || has_file(&fb, "one.txt", b"one"));

    // The dialer pauses.
    let reply = b.call(json!({"op":"pause-sync","state":state_b}));
    assert_eq!(reply["status"]["intents"]["sync_enabled"], false, "{reply}");
    assert_eq!(reply["status"]["sync"]["state"], "paused", "{reply}");
    assert_eq!(
        b.inspect(&state_b)["status"]["intents"]["sync_enabled"],
        false
    );
    write_file(&fa, "two.txt", b"two");
    thread::sleep(Duration::from_secs(3));
    assert!(!has_file(&fb, "two.txt", b"two"), "paused at B");
    b.call(json!({"op":"resume-sync","state":state_b}));
    b.wait_for("two.txt at B after resume", START, || {
        has_file(&fb, "two.txt", b"two")
    });

    // The acceptor pauses: B's exchanges are answered FOLDER_SYNC_PAUSED.
    let reply = a.call(json!({"op":"pause-sync","state":state_a}));
    assert_eq!(reply["status"]["sync"]["state"], "paused", "{reply}");
    write_file(&fa, "three.txt", b"three");
    thread::sleep(Duration::from_secs(3));
    assert!(!has_file(&fb, "three.txt", b"three"), "paused at A");
    a.call(json!({"op":"resume-sync","state":state_a}));
    b.wait_for("three.txt at B after resume", START, || {
        has_file(&fb, "three.txt", b"three")
    });
}

/// `tkfs manage status` (text and `--json`) and `tkfs manage watch --json`
/// against the real daemon (CCR-WP2.2-4's stable text).
#[test]
fn manage_status_and_watch_show_lifecycle_and_mount() {
    let a = Node::new();
    let (_, state) = running_folder(&a, "Cli folder");
    let config = a.config_path();
    let tkfs = |args: &[&str]| {
        let out = Command::new(bin())
            .arg("-f")
            .arg(&config)
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{args:?}: {}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    };
    let text = tkfs(&["manage", "status"]);
    assert!(text.starts_with("Telekinesis daemon"), "{text}");
    assert!(
        text.contains(&format!("\nCli folder [{}]\n", &state[..8])),
        "{text}"
    );
    assert!(text.contains("\n  lifecycle  running for "), "{text}");
    assert!(text.contains("\n  mount      "), "{text}");
    let snapshot: StatusSnapshot = serde_json::from_str(&tkfs(&["manage", "status", "--json"]))
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(snapshot.schema, STATUS_SCHEMA);
    let folder = snapshot
        .folders
        .iter()
        .find(|folder| folder.state == state)
        .expect("folder in the snapshot");
    assert_eq!(
        serde_json::to_value(folder.lifecycle.phase).unwrap(),
        "running"
    );

    // `watch --json`: a snapshot at once, then one per change.
    let mut watch = Command::new(bin())
        .arg("-f")
        .arg(&config)
        .args(["manage", "watch", "--json"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let (lines, rx) = mpsc::channel::<String>();
    let stdout = watch.stdout.take().unwrap();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if lines.send(line).is_err() {
                break;
            }
        }
    });
    let first: StatusSnapshot =
        serde_json::from_str(&rx.recv_timeout(Duration::from_secs(10)).unwrap()).unwrap();
    assert!(first.folders.iter().any(|folder| folder.state == state));
    a.call(json!({"op":"pause-sync","state":state}));
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut paused = false;
    while !paused && Instant::now() < deadline {
        let Ok(line) = rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) else {
            break;
        };
        let snapshot: StatusSnapshot = serde_json::from_str(&line).unwrap();
        paused = snapshot
            .folders
            .iter()
            .any(|folder| folder.state == state && !folder.intents.sync_enabled);
    }
    let _ = watch.kill();
    let _ = watch.wait();
    assert!(paused, "watch --json never printed the paused folder");
}

/// A `subscribe` long poll sees a change within 1 s (§14 WP2.INT, the
/// `subscribe_latency` floor): heartbeats move the hub too, so it follows
/// generations until a snapshot shows the change.
#[test]
fn subscribe_sees_a_change_within_1s() {
    let a = Node::new();
    let (_, state) = running_folder(&a, "Subscriber");
    let control = a.control();
    let mut since = a.call(json!({"op":"status"}))["generation"]
        .as_u64()
        .unwrap();
    let changed = Instant::now();
    a.call(json!({"op":"pause-sync","state":state}));
    let acknowledged = Instant::now();
    let seen = loop {
        let snapshot = control
            .call(&json!({"op":"subscribe","since":since,"wait_ms":5000}))
            .unwrap();
        let generation = snapshot["generation"].as_u64().unwrap();
        let paused = snapshot["folders"]
            .as_array()
            .unwrap()
            .iter()
            .any(|folder| {
                folder["state"] == state.as_str() && folder["intents"]["sync_enabled"] == false
            });
        if paused {
            break Instant::now();
        }
        assert!(
            changed.elapsed() < Duration::from_secs(10),
            "the change never reached a subscriber"
        );
        since = generation;
    };
    let latency = seen.saturating_duration_since(acknowledged);
    assert!(latency < Duration::from_secs(1), "{latency:?}");
}

// ---------------------------------------------------------------------------
// CCRs applied by WP2.INT

/// CCR-WP2.5-5: `published-summary` and the heartbeat carry the feed tip
/// (shared events only) and the feed epoch; a private commit moves neither,
/// while the journal tip moves.
#[test]
fn the_published_and_heartbeat_tips_count_shared_events_only() {
    let a = Node::new();
    let (folder, _) = running_folder(&a, "Feed tip");
    write_file(&folder, "shared.txt", b"shared");
    let summary = rpc(&folder, json!({"op":"published-summary"}));
    let tip = summary["tip"].clone();
    let epoch = tip["epoch"].as_str().unwrap().to_owned();
    assert_eq!(epoch.len(), 32, "{summary}");
    assert!(epoch.bytes().all(|b| b.is_ascii_hexdigit()), "{summary}");
    let seq = tip["seq"].as_i64().unwrap();
    assert!(seq > 0, "{summary}");

    rpc(&folder, json!({"op":"branch","name":"private"}));
    rpc(&folder, json!({"op":"checkout","name":"private"}));
    write_file(&folder, "private.txt", b"private");
    let status = rpc(&folder, json!({"op":"status"}));
    assert!(
        status["journal"]["tip"].as_i64().unwrap() > seq,
        "the private commit moved the journal: {status:#}"
    );
    assert_eq!(status["journal"]["epoch"], epoch.as_str(), "{status:#}");
    assert_eq!(
        rpc(&folder, json!({"op":"published-summary"}))["tip"],
        tip,
        "a private event never moves the published tip"
    );
    assert_eq!(status["tip"], tip, "{status:#}");
    assert!(wait_until(Duration::from_secs(10), || try_rpc(
        &folder,
        json!({"op":"watch","after_seq":0,"wait_ms":0})
    )
    .is_ok_and(|hb| hb["tip"] == tip)));
}

/// CCR-WP2.4-6: after a caller rolls back a commit of this connection
/// (an outer `ROLLBACK` before its next request, or a nested `ROLLBACK TO`),
/// readers through the store's view (`entry`, `lookup`, `projection`) never
/// see the rolled-back revision.
#[test]
fn a_reader_never_sees_a_commit_its_caller_rolled_back() {
    for nested in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Store::open(dir.path(), Some("w2-rollback")).unwrap();
        let read = Store::open_secondary(&s.root, &s.repo, &s.device).unwrap();
        let file = s.create("file", "file").unwrap().id;
        let committed = |s: &Store| s.entry(&file).unwrap().revisions.get("content").cloned();
        // An edit of the committed revision (a base of None would be a
        // concurrent edit, a conflict).
        let kept = s.write_revision(&file, committed(&s), b"kept").unwrap();
        let active = s.active.clone();
        assert_eq!(committed(&s).as_deref(), Some(kept.as_str()));
        for i in 0..3 {
            s.db.execute_batch("BEGIN IMMEDIATE").unwrap();
            if nested {
                s.db.execute_batch("SAVEPOINT request").unwrap();
            }
            // A content successor: a fast commit that patches the view.
            s.write_revision(
                &file,
                Some(kept.clone()),
                format!("rolled back {i}").as_bytes(),
            )
            .unwrap();
            if nested {
                s.db.execute_batch("ROLLBACK TO request; RELEASE request")
                    .unwrap();
            } else {
                s.db.execute_batch("ROLLBACK").unwrap();
                s.db.execute_batch("BEGIN IMMEDIATE").unwrap();
            }
            assert_eq!(
                committed(&s).as_deref(),
                Some(kept.as_str()),
                "nested {nested}, round {i}: entry()"
            );
            assert_eq!(
                s.lookup("file").unwrap().revisions.get("content").cloned(),
                Some(kept.clone()),
                "nested {nested}, round {i}: lookup()"
            );
            assert_eq!(
                s.projection(&active).unwrap(),
                read.projection(&active).unwrap(),
                "nested {nested}, round {i}: the view is the stored projection"
            );
            s.db.execute_batch("COMMIT").unwrap();
        }
    }
}

/// CCR-WP2.3-1: the in-process `status` (`Engine::control`, also the
/// `folder-details` member) keeps `caught_up` for quarantined or blocked
/// inbox rows, which are reported apart; a row that still needs activation
/// clears it.
#[test]
fn in_process_caught_up_ignores_quarantined_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = Engine::new(Store::open(dir.path(), Some("w2-caught-up")).unwrap());
    e.store.create("code", "file").unwrap();
    e.sync_peers.insert("configured-peer".into());
    let known: Vec<String> = e
        .store
        .events()
        .unwrap()
        .into_iter()
        .filter(|event| event.branch.shared)
        .map(|event| event.id)
        .collect();
    e.store.acknowledge(&known).unwrap();
    e.store
        .remember_peer_inventory("configured-peer", &known.iter().cloned().collect())
        .unwrap();
    let now = tkfs::util::time::now_ms();
    e.last_sync = Some(json!({"caught_up":true,"at":now}));
    e.peer_sync
        .insert("configured-peer".into(), json!({"at":now}));
    let status = |e: &mut Engine| e.control(&id(), &json!({"op":"status"})).unwrap();
    assert_eq!(status(&mut e)["caught_up"], true);

    e.store
        .db
        .execute(
            "INSERT INTO incoming_events(id,payload,error,state,code) \
             VALUES('quarantined-row','{}','INVALID_EVENT: test','quarantined','INVALID_EVENT')",
            [],
        )
        .unwrap();
    let s = status(&mut e);
    assert_eq!(s["incoming_pending"], 1, "{s}");
    assert_eq!(s["incoming_quarantined"], 1, "{s}");
    assert_eq!(s["incoming_active"], 0, "{s}");
    assert_eq!(s["caught_up"], true, "a quarantined row is reported apart");

    e.store
        .db
        .execute(
            "INSERT INTO incoming_events(id,payload,state) VALUES('ready-row','{}','ready')",
            [],
        )
        .unwrap();
    let s = status(&mut e);
    assert_eq!(s["incoming_active"], 1, "{s}");
    assert_eq!(s["caught_up"], false, "a row to activate is work left");
}

// ---------------------------------------------------------------------------
// Review findings of packages not approved, fixed by WP2.INT

type Rows = Vec<(i64, String)>;

fn rows(db: &rusqlite::Connection, sql: &str) -> Rows {
    db.prepare(sql)
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

const SHARED_JOURNAL: &str =
    "SELECT s.seq,s.event FROM journal_sequence s JOIN events e ON e.id=s.event
    JOIN branches b ON b.id=e.branch WHERE b.shared=1 ORDER BY s.seq";
const FEED_JOURNAL: &str = "SELECT seq,event FROM feed_journal ORDER BY seq";

fn config(db: &rusqlite::Connection, key: &str) -> Option<String> {
    use rusqlite::OptionalExtension;
    db.query_row("SELECT value FROM config WHERE key=?1", [key], |r| r.get(0))
        .optional()
        .unwrap()
}

/// WP2.5 review (major): the `feed_journal` build is a backfill (§9, I10,
/// invariant 28): chunked, resumable and reporting startup progress. A build
/// interrupted after some chunks resumes from its cursor (rows already built
/// are not built again), private events stay out, the marker is set at the
/// end, and a dropped table is rebuilt whole.
#[test]
fn the_feed_journal_backfill_resumes_from_its_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    {
        let mut s = Store::open(root, Some("w2-backfill")).unwrap();
        for i in 0..40 {
            s.create(&format!("shared-{i}"), "file").unwrap();
        }
        s.fork("private", false).unwrap();
        s.checkout("private").unwrap();
        for i in 0..5 {
            s.create(&format!("private-{i}"), "file").unwrap();
        }
        assert_eq!(config(&s.db, "feed_journal_v1").as_deref(), Some("1"));
        assert_eq!(config(&s.db, "feed_journal_v1_cursor"), None);
    }
    // An interrupted build: the table exists, the rows up to the cursor are
    // built (the first one marked), the rest are not, no marker.
    let (expected, half) = {
        let db = rusqlite::Connection::open(root.join("metadata.sqlite")).unwrap();
        let expected = rows(&db, SHARED_JOURNAL);
        let all = rows(&db, "SELECT seq,event FROM journal_sequence ORDER BY seq");
        assert!(all.len() > expected.len(), "private events in the journal");
        assert_eq!(rows(&db, FEED_JOURNAL), expected);
        let half = expected[expected.len() / 2].0;
        db.execute("DELETE FROM feed_journal WHERE seq>?1", [half])
            .unwrap();
        db.execute(
            "UPDATE feed_journal SET event='built-before' WHERE seq=?1",
            [expected[0].0],
        )
        .unwrap();
        db.execute("DELETE FROM config WHERE key='feed_journal_v1'", [])
            .unwrap();
        db.execute(
            "INSERT OR REPLACE INTO config(key,value) VALUES('feed_journal_v1_cursor',?1)",
            [half.to_string()],
        )
        .unwrap();
        (expected, half)
    };
    let s = Store::open(root, Some("w2-backfill")).unwrap();
    let built = rows(&s.db, FEED_JOURNAL);
    assert_eq!(built[0].1, "built-before", "resumed after {half}");
    assert_eq!(built[1..], expected[1..]);
    assert_eq!(config(&s.db, "feed_journal_v1").as_deref(), Some("1"));
    assert_eq!(config(&s.db, "feed_journal_v1_cursor"), None);
    drop(s);
    // A dropped table is built again from the whole journal.
    {
        let db = rusqlite::Connection::open(root.join("metadata.sqlite")).unwrap();
        db.execute_batch(
            "DROP TRIGGER feed_journal_insert; DROP TRIGGER feed_journal_update;
            DROP TRIGGER feed_journal_delete; DROP TRIGGER feed_journal_branch;
            DROP TABLE feed_journal;",
        )
        .unwrap();
    }
    let s = Store::open(root, Some("w2-backfill")).unwrap();
    assert_eq!(rows(&s.db, FEED_JOURNAL), expected);
    assert_eq!(config(&s.db, "feed_journal_v1").as_deref(), Some("1"));
}
