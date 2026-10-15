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

/// Stages `count` ready rows on `branch` with `pad` bytes of padding each
/// (only their keys, size and branch matter to the forecast).
fn stage_rows(s: &Store, branch: &str, count: usize, pad: usize) {
    let padding = "x".repeat(pad);
    for i in 0..count {
        let payload = json!({
            "id": format!("{branch}-{i}"),
            "repo": "w2-forecast",
            "branch": {"id": branch, "name": branch, "shared": true},
            "pad": padding,
        })
        .to_string();
        s.db.execute(
            "INSERT INTO incoming_events(id,payload,state,received_ms) VALUES(?1,?2,'ready',?3)",
            rusqlite::params![format!("{branch}-{i}"), payload, i as i64],
        )
        .unwrap();
    }
}

/// WP2.3 review (major): the seal forecast runs under the engine lock before
/// every slice, so it never parses large payloads: a head of 64 events of
/// 1 MiB is forecast in milliseconds (it parsed up to 128 such payloads), and
/// small rows are still read (a head row on a branch due for its seal makes
/// a one-event slice).
#[test]
fn the_seal_forecast_never_parses_large_payloads() {
    use tkfs::worker::activator::{FORECAST_ROWS, seal_forecast};
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(dir.path(), Some("w2-forecast")).unwrap();
    stage_rows(&s, "large-branch", 64, 1024 * 1024);
    // Best of three, so a load spike on a shared machine is not measured:
    // about 0.4 ms here (debug), against about 90 ms when every head
    // payload was parsed (1 MiB each; 8 MiB events cost 8 times as much).
    let took = (0..3)
        .map(|_| {
            let started = Instant::now();
            let forecast = seal_forecast(&s, 64).expect("forecast");
            let took = started.elapsed();
            assert_eq!(
                forecast.at, None,
                "a large row's branch is not read: it caps nothing"
            );
            took
        })
        .min()
        .unwrap();
    assert!(took < Duration::from_millis(25), "{took:?}");

    // Small rows on a branch without a snapshot (due at its next commit):
    // the first row seals if it commits, so the slice is one event.
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(dir.path(), Some("w2-forecast")).unwrap();
    stage_rows(&s, "small-branch", FORECAST_ROWS * 2, 100);
    assert_eq!(seal_forecast(&s, 64).expect("forecast").at, Some(1));
}

// ---------------------------------------------------------------------------
// Post-integration re-review (WP2.INT): retired legacy lanes go quiet

/// The state of `installation`'s outbound lane `lane` in `node`'s status.
fn lane_state(node: &Node, installation: &str, lane: u64) -> Option<String> {
    let snapshot = node.raw_call(json!({"op":"status"})).ok()?;
    snapshot["peers"]
        .as_array()?
        .iter()
        .find(|peer| peer["installation"] == installation)?["lanes"]
        .as_array()?
        .iter()
        .find(|entry| entry["lane"] == lane && entry["direction"] == "outbound")
        .and_then(|entry| entry["state"].as_str().map(str::to_owned))
}

/// Whether `node`'s status lists the peer `installation` at all.
fn lists_peer(node: &Node, installation: &str) -> bool {
    node.raw_call(json!({"op":"status"})).is_ok_and(|snapshot| {
        snapshot["peers"].as_array().is_some_and(|peers| {
            peers
                .iter()
                .any(|peer| peer["installation"] == installation)
        })
    })
}

/// After Unpair the peer is gone from the dialer's status and stays gone: a
/// cancelled lane's late events (its `LegacyLink`, its final `Closed`) never
/// recreate it. Before the fix the hub showed the removed peer again,
/// `offline`, until the daemon restarted.
#[test]
fn unpair_removes_the_peer_from_status_for_good() {
    let a = Node::new();
    let b = Node::new();
    let code = b.code();
    a.pair(&b, &code);
    let peer = b.installation();
    a.wait_for("a ready control lane", START, || {
        lane_state(&a, &peer, 0).as_deref() == Some("ready")
    });
    a.call(json!({"op":"unpair","endpoint":b.endpoint()}));
    a.wait_for("the peer gone from status", Duration::from_secs(10), || {
        !lists_peer(&a, &peer)
    });
    // The retired lanes' threads end meanwhile; nothing brings the peer back.
    let until = Instant::now() + Duration::from_secs(5);
    while Instant::now() < until {
        assert!(!lists_peer(&a, &peer), "the removed peer came back");
        thread::sleep(Duration::from_millis(250));
    }
}

/// A re-pair while the old control lane is parked in a slow local worker
/// call: the new lane stays `ready` in the hub. The old lane's late `Closed`
/// (when its call returns, 10 s later) is muted. Before the fix it
/// overwrote the new lane's `Ready`, and the healthy lane showed `closed`
/// until the daemon restarted.
#[test]
fn a_re_pair_keeps_the_new_lane_ready_while_the_old_one_ends_late() {
    let a = Node::new();
    let mut b = Node::new();
    let code = a.code();
    b.pair(&a, &code);
    let (fa, _) = running_folder(&a, "Slow offer");
    let received = b.receive(&a, &fa);
    let state_b = state_id(&received);
    // Only the old control lane's first local offer hangs (10 s).
    b.restart_with_env(vec![
        (
            "TKFS_FAULT_POINT".into(),
            "worker.rpc.published-offer".into(),
        ),
        ("TKFS_FAULT_ACTION".into(), "hang".into()),
        ("TKFS_FAULT_MS".into(), "10000".into()),
        ("TKFS_FAULT_COUNT".into(), "1".into()),
        ("TKFS_FAULT_SCOPE".into(), state_b.clone()),
    ]);
    let peer = a.installation();
    b.wait_for("b ready control lane", START, || {
        lane_state(&b, &peer, 0).as_deref() == Some("ready")
    });
    b.wait_for("the received folder running", START, || {
        b.try_inspect(&state_b).is_ok_and(|p| p["running"] == true)
    });
    // Give the old lane time to enter the hung offer, then re-pair.
    thread::sleep(Duration::from_millis(1500));
    b.pair(&a, &code);
    let repaired = Instant::now();
    let mut ready_seen = false;
    let mut states = vec![];
    while repaired.elapsed() < Duration::from_secs(18) {
        let state = lane_state(&b, &peer, 0);
        if state.as_deref() == Some("ready") {
            ready_seen = true;
        } else if ready_seen {
            states.push((repaired.elapsed(), state.clone()));
        }
        thread::sleep(Duration::from_millis(250));
    }
    assert!(ready_seen, "the new control lane never became ready");
    assert!(
        states.is_empty(),
        "the new ready lane was overwritten: {states:?}"
    );
    assert_eq!(lane_state(&b, &peer, 0).as_deref(), Some("ready"));
}

// ---------------------------------------------------------------------------
// The v3 peer stack, end to end (not wired into the daemon until WP3.1)

/// An in-process daemon context (WP2.1's `ctx::build_with`) whose workers are
/// the real `tkfs` binary. `serve` is never called: the test decides who uses
/// the network listener.
fn v3_daemon() -> (tempfile::TempDir, tkfs::orchestrator::ctx::Daemon) {
    use tkfs::orchestrator::{
        Config, Network,
        ctx::{BuildOptions, build_with},
    };
    let root = tempfile::tempdir().unwrap();
    let config = Config {
        format_version: 2,
        data_directory: root.path().join("data"),
        default_mount_directory: root.path().join("mounts"),
        maximum_running: 8,
        network: Network {
            listen: Some("127.0.0.1:0".into()),
            connections: 2,
            sync_interval_ms: 200,
        },
    };
    let daemon = build_with(
        config,
        BuildOptions {
            executable: Some(bin()),
            ..BuildOptions::default()
        },
    )
    .unwrap();
    (root, daemon)
}

fn running_view(ctx: &tkfs::model::orchestrator::Ctx, state: &str) -> bool {
    ctx.folders.view(state).is_some_and(|view| {
        matches!(
            view.phase,
            tkfs::model::orchestrator::FolderPhase::Running { .. }
        )
    })
}

/// WP2.5 (worker feed ops), WP2.6 (PeerManager and lanes), WP2.7
/// (MetaScheduler and the interim bulk lane) and WP2.8 (the inbound server)
/// together, over real v3 sessions between two in-process daemons with real
/// workers: A pairs with B through a probe session, lists B's catalog, holds
/// B's repository, and both directions sync (metadata through
/// `meta.exchange`, objects through `objects.exchange`), dialer-driven. The
/// hubs see the exchanges on both sides. WP3.1 wires this stack into the
/// daemon; this proves the parts fit before it does.
#[test]
fn the_v3_stack_syncs_two_daemons_end_to_end() {
    use tkfs::{
        model::orchestrator::{CatalogCmd, FolderRecord, NetworkApi},
        orchestrator::{Action, manage::manage},
        pairing_code::Verifier,
    };
    let (_root_a, a) = v3_daemon();
    let (_root_b, mut b) = v3_daemon();
    // Since WP3.1's cutover the daemons' network is this stack: A's
    // `ctx.network` runs the PeerManager, and B serves its listener with the
    // inbound server (what `run::serve` does with it).

    // B: a pairing code, the v3 inbound server on its listener, a folder.
    let (code, verifier) = Verifier::generate();
    b.ctx
        .catalog
        .apply(CatalogCmd::SetVerifier { verifier })
        .unwrap();
    let server = b
        .net
        .serve_inbound(&b.ctx, b.network.take().unwrap())
        .unwrap();
    let endpoint = format!("ws://{}", server.local_addr().unwrap());
    let peers = a.net.peers().unwrap();
    // Whatever happens below, the peer stack and the workers are stopped
    // (the workers would otherwise outlive the test process).
    let _cleanup = V3Cleanup {
        ctxs: vec![a.ctx.clone(), b.ctx.clone()],
        peers: peers.clone(),
        server: server.clone(),
    };
    let source = manage(
        &b.ctx,
        Action::Create {
            label: "V3 source".into(),
            mount: None,
            headless: true,
        },
    )
    .unwrap();
    let source_state = state_id(&source);
    assert!(wait_until(START, || running_view(&b.ctx, &source_state)));
    write_file(&source, "from-b.txt", b"written at B");
    let repo = source["repo"].as_str().unwrap().to_owned();

    // A: pairs through a probe session and lists B's catalog.
    let paired = peers.pair(&endpoint, &code, Some(2)).unwrap();
    assert_eq!(paired.installation, b.ctx.installation);
    assert!(
        wait_until(START, || peers.remote_list(&endpoint).is_ok_and(
            |entries| entries.iter().any(|entry| entry.repo == repo)
        )),
        "B's folder never appeared in its catalog"
    );

    // A holds B's repository (what Receive does, without the legacy
    // RemoteList check): its controller starts it.
    let state = id();
    let directory = a.ctx.config.data_directory.join("states").join(&state);
    let device = id();
    drop(Store::initialize(&directory, &repo, &device).unwrap());
    a.ctx
        .catalog
        .apply(CatalogCmd::CreateFolder {
            record: FolderRecord {
                state: state.clone(),
                label: "V3 received".into(),
                repo: repo.clone(),
                device,
                directory: directory.clone(),
                mount: None,
                desired_running: true,
                sync_enabled: true,
                mount_enabled: true,
            },
            gate: None,
        })
        .unwrap();
    assert!(wait_until(START, || running_view(&a.ctx, &state)));
    let received = json!({"state": state, "directory": directory});

    // B → A: metadata and objects.
    assert!(
        wait_until(START, || has_file(&received, "from-b.txt", b"written at B")),
        "metadata from B never reached A"
    );
    assert!(
        wait_until(START, || support::content_complete(&received, 1)),
        "objects from B never reached A: {}",
        support::content_status(&received)
    );
    // A → B, still dialed by A.
    write_file(&received, "from-a.txt", b"written at A");
    assert!(
        wait_until(START, || has_file(&source, "from-a.txt", b"written at A")),
        "metadata from A never reached B"
    );
    assert!(
        wait_until(START, || support::content_complete(&source, 2)),
        "objects from A never reached B: {}",
        support::content_status(&source)
    );

    // Both hubs saw it: A's v3 lanes and outbound exchanges, B's inbound
    // sessions and served exchanges.
    let a_status = a.hub.snapshot_now();
    let peer = a_status
        .peers
        .iter()
        .find(|peer| peer.installation == b.ctx.installation)
        .expect("B in A's status");
    assert_eq!(peer.protocol.as_deref(), Some("tkfs-peer/3"), "{peer:?}");
    let b_status = b.hub.snapshot_now();
    let dialer = b_status
        .peers
        .iter()
        .find(|peer| peer.installation == a.ctx.installation)
        .expect("A in B's status");
    assert!(dialer.inbound_sessions >= 1, "{dialer:?}");
    let served = b_status
        .folders
        .iter()
        .find(|folder| folder.state == source_state)
        .unwrap()
        .sync
        .peers
        .iter()
        .find(|pair| pair.installation == a.ctx.installation)
        .cloned()
        .expect("B's folder shows A");
    assert!(served.metadata.last_exchange_ms.is_some(), "{served:?}");
}

/// Stops a v3 test's peer stack and daemons (and so their workers).
struct V3Cleanup {
    ctxs: Vec<std::sync::Arc<tkfs::model::orchestrator::Ctx>>,
    peers: std::sync::Arc<tkfs::orchestrator::peer::PeerManager>,
    server: std::sync::Arc<tkfs::orchestrator::inbound::InboundServer>,
}

impl Drop for V3Cleanup {
    fn drop(&mut self) {
        use tkfs::model::orchestrator::NetworkApi;
        self.peers.shutdown(Duration::from_secs(5));
        self.server.shutdown(Duration::from_secs(5));
        for ctx in &self.ctxs {
            tkfs::orchestrator::shutdown::run(ctx, Duration::from_secs(30));
            ctx.shutdown.cancel();
        }
    }
}

// ---------------------------------------------------------------------------
// Interoperability with a 6298d0b peer (§13 rule 8.4; waves 1-2 stay
// wire-compatible). `w1_integration::interop_with_a_6298d0b_peer` covers
// pairing, receiving and both directions in both dialing directions; this
// adds what Wave 2 changed on the paths a 6298d0b peer meets.

/// A 6298d0b daemon run from `TKFS_BASELINE_BIN`.
struct Baseline {
    root: tempfile::TempDir,
    child: Option<std::process::Child>,
}

impl Baseline {
    fn start() -> Self {
        let bin = std::path::PathBuf::from(
            std::env::var("TKFS_BASELINE_BIN").expect("TKFS_BASELINE_BIN: path of a 6298d0b tkfs"),
        );
        assert!(bin.is_file(), "{}", bin.display());
        let root = tempfile::tempdir().unwrap();
        fs::write(
            root.path().join("orchestrator.toml"),
            support::NodeConfig::default().toml(),
        )
        .unwrap();
        let log = fs::File::create(root.path().join("daemon.log")).unwrap();
        let child = Command::new(bin)
            .arg("-f")
            .arg(root.path().join("orchestrator.toml"))
            .arg("orchestrator")
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        let node = Self {
            root,
            child: Some(child),
        };
        assert!(
            wait_until(Duration::from_secs(30), || node
                .raw(json!({"op":"list"}))
                .is_ok()),
            "the baseline daemon did not start"
        );
        node
    }
    fn raw(&self, action: Value) -> anyhow::Result<Value> {
        support::Control {
            data_dir: self.root.path().join("data"),
        }
        .call(&action)
    }
    fn call(&self, action: Value) -> Value {
        self.raw(action.clone())
            .unwrap_or_else(|e| panic!("baseline {action} failed: {e:#}"))
    }
    fn endpoint(&self) -> String {
        let discovery: Value = serde_json::from_slice(
            &fs::read(self.root.path().join("data/orchestrator.json")).unwrap(),
        )
        .unwrap();
        discovery["network"].as_str().unwrap().into()
    }
    fn code(&self) -> String {
        self.call(json!({"op":"reset-pairing-code"}))["code"]
            .as_str()
            .unwrap()
            .into()
    }
    fn running(&self, state: &str) -> Value {
        assert!(wait_until(START, || self
            .raw(json!({"op":"inspect","state":state}))
            .is_ok_and(|p| p["running"] == true)));
        self.call(json!({"op":"inspect","state":state}))
    }
}

impl Drop for Baseline {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = self.raw(json!({"op":"shutdown"}));
            if !wait_until(Duration::from_secs(10), || {
                child.try_wait().ok().flatten().is_some()
            }) {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

/// Wave 2 against a 6298d0b daemon: pause and resume of a shared folder on
/// either side of the legacy lanes (this build's acceptor answers a paused
/// folder `FOLDER_SYNC_PAUSED`, a code the old dialer does not know; its
/// lanes keep working), the old daemon keeps answering its API while it
/// meets that answer, and this build's CLI (WP2.2's `ControlClient`) drives
/// the old daemon's legacy actions. Run with `TKFS_BASELINE_BIN=<6298d0b tkfs>
/// cargo test --test w2_integration -- --ignored interop`.
#[test]
#[ignore = "WP2.INT interop (waves 1-2 only; since WP3.1 a 6298d0b peer fails closed, see qual_wp3_1_transport::upgrade_against_a_6298d0b_peer_fails_closed_on_both_sides): needs TKFS_BASELINE_BIN"]
fn interop_wave_2_with_a_6298d0b_peer() {
    let old = Baseline::start();
    let new = Node::new();

    // 1. The old daemon dials this build and receives its folder.
    let (served, served_state) = running_folder(&new, "Served by wave 2");
    write_file(&served, "w2-1.txt", b"wave 2, first");
    old.call(json!({"op":"pair","endpoint":new.endpoint(),"code":new.code(),"connections":3}));
    let received = old.call(json!({"op":"receive","endpoint":new.endpoint(),
        "repo":served["repo"],"label":"From wave 2","mount":null,"headless":true}));
    let received = old.running(received["state"].as_str().unwrap());
    assert!(wait_until(START, || has_file(
        &received,
        "w2-1.txt",
        b"wave 2, first"
    )));

    // This build pauses the folder it serves: the old dialer gets
    // FOLDER_SYNC_PAUSED, nothing moves, and the old daemon stays usable.
    new.call(json!({"op":"pause-sync","state":served_state}));
    write_file(&served, "w2-2.txt", b"wave 2, second");
    thread::sleep(Duration::from_secs(3));
    assert!(!has_file(&received, "w2-2.txt", b"wave 2, second"));
    assert!(old.raw(json!({"op":"peers"})).is_ok());
    new.call(json!({"op":"resume-sync","state":served_state}));
    assert!(wait_until(START, || has_file(
        &received,
        "w2-2.txt",
        b"wave 2, second"
    )));
    // And the old side's writes still come back.
    write_file(&received, "old-1.txt", b"6298d0b, first");
    assert!(wait_until(START, || has_file(
        &served,
        "old-1.txt",
        b"6298d0b, first"
    )));

    // 2. This build dials the old daemon, receives its folder, and pauses
    // it as the dialer.
    let old_folder =
        old.call(json!({"op":"create","label":"Served by 6298d0b","mount":null,"headless":true}));
    let old_folder = old.running(old_folder["state"].as_str().unwrap());
    write_file(&old_folder, "old-2.txt", b"6298d0b, second");
    new.call(json!({"op":"pair","endpoint":old.endpoint(),"code":old.code(),"connections":3}));
    let mine = new.receive_from(&old.endpoint(), &old_folder);
    let mine_state = state_id(&mine);
    new.wait_for("received from 6298d0b", START, || {
        new.try_inspect(&mine_state)
            .is_ok_and(|p| p["running"] == true)
    });
    let mine = new.inspect(&mine_state);
    assert!(wait_until(START, || has_file(
        &mine,
        "old-2.txt",
        b"6298d0b, second"
    )));
    new.call(json!({"op":"pause-sync","state":mine_state}));
    write_file(&old_folder, "old-3.txt", b"6298d0b, third");
    thread::sleep(Duration::from_secs(3));
    assert!(!has_file(&mine, "old-3.txt", b"6298d0b, third"));
    new.call(json!({"op":"resume-sync","state":mine_state}));
    assert!(wait_until(START, || has_file(
        &mine,
        "old-3.txt",
        b"6298d0b, third"
    )));

    // 3. This build's CLI against the old daemon: the legacy actions work
    // through the ControlClient (one request per connection there); the
    // v2 `status` is refused by the old daemon, as an error.
    let config = old.root.path().join("orchestrator.toml");
    let list = Command::new(bin())
        .arg("-f")
        .arg(&config)
        .args(["manage", "list"])
        .output()
        .unwrap();
    assert!(
        list.status.success(),
        "{}",
        String::from_utf8_lossy(&list.stderr)
    );
    let listed: Value = serde_json::from_slice(&list.stdout).unwrap();
    assert_eq!(listed["folders"].as_array().unwrap().len(), 2, "{listed}");
    let status = Command::new(bin())
        .arg("-f")
        .arg(&config)
        .args(["manage", "status"])
        .output()
        .unwrap();
    assert!(!status.status.success(), "a 6298d0b daemon has no status");
}
