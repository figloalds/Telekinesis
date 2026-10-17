//! WP4.INT post-integration re-review: pending saves that cannot be restored
//! and the spools an interrupted run leaves behind.
//!
//! * a `pending_saves` record whose staging file is missing or unreadable is
//!   kept, reported (`RECOVERY_REQUIRED`, naming the entity) and retried,
//!   while every other record still loads and commits; it no longer makes the
//!   folder unstoppable or refuse control requests (`quiet()`, Stop,
//!   `mkdir`), and `discard-pending-save` drops it;
//! * a record whose staging file comes back (a scanner held it at startup) is
//!   restored by the retry and committed;
//! * a later save of the same file supersedes a record that was not restored;
//! * at worker startup, staging files no record names and temporary object
//!   files (`objects/*.part`) are removed.
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tkfs::{
    core::*,
    runtime::{self, Engine},
};

const REPO: &str = "pending-saves";

fn pending_entities(engine: &Engine) -> Vec<String> {
    engine
        .store
        .db
        .prepare("SELECT entity FROM pending_saves ORDER BY entity")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn stage_of(engine: &Engine, entity: &str) -> PathBuf {
    let payload: String = engine
        .store
        .db
        .query_row(
            "SELECT payload FROM pending_saves WHERE entity=?",
            [entity],
            |row| row.get(0),
        )
        .unwrap();
    let record: Value = serde_json::from_str(&payload).unwrap();
    engine
        .store
        .root
        .join("staging")
        .join(record["stage"].as_str().unwrap())
}

fn content(engine: &Engine, path: &str) -> Vec<u8> {
    let entry = engine.store.lookup(path).unwrap();
    engine
        .store
        .read_object(entry.content.as_deref().unwrap())
        .unwrap()
}

/// `count` files of 300 KiB (spooled to disk) left unsaved and retained, as a
/// force detach or a poison exit leaves them; returns their entity ids and
/// bytes in creation order. The engine is then dropped (its handles close,
/// the retained spools stay), as a crash after the retain would.
fn retained(dir: &Path, count: usize) -> Vec<(String, Vec<u8>)> {
    let mut engine = Engine::new(Store::open(dir, Some(REPO)).unwrap());
    let mut files = Vec::new();
    for index in 0..count {
        let bytes: Vec<u8> = (0..300 * 1024)
            .map(|i| ((i + index * 7) % 251) as u8)
            .collect();
        let (h, entry) = engine
            .open(&format!("unsaved-{index}.bin"), Some("file"), true)
            .unwrap();
        engine.write(h, 0, &bytes, false, false).unwrap();
        files.push((entry.id, bytes));
    }
    assert_eq!(engine.retain_all_dirty().unwrap(), count);
    drop(engine);
    files
}

/// The review's reproduction: three retained 300 KiB files, one staging file
/// deleted, then a restart. Before the fix the first bad record stopped the
/// load (one session loaded), the engine's health became a sticky
/// `RECOVERY_REQUIRED` that nothing cleared, `quiet()` refused with
/// `BUSY_VIEW: 0 open contexts, 0 unsaved files; …` (so no graceful Stop and
/// no control request) and the record behind the bad one was never retried.
#[test]
fn an_unrestorable_pending_save_degrades_the_store_and_blocks_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let files = retained(dir.path(), 3);
    let (broken, _) = files[1].clone();
    {
        let engine = Engine::new(Store::open_existing(dir.path()).unwrap());
        fs::remove_file(stage_of(&engine, &broken)).unwrap();
    }
    let mut engine = Engine::new(Store::open_existing(dir.path()).unwrap());
    // Every other record loaded: two dirty sessions.
    let loaded: Vec<_> = engine
        .sessions
        .iter()
        .filter(|(_, s)| s.dirty)
        .map(|(id, _)| id.clone())
        .collect();
    let mut expected: Vec<_> = files
        .iter()
        .map(|(id, _)| id.clone())
        .filter(|id| id != &broken)
        .collect();
    expected.sort();
    assert_eq!(loaded, expected);
    let reported = engine.reported_health().unwrap();
    assert!(reported.starts_with("PENDING_SAVES"), "{reported}");
    // The loaded ones commit; the broken one stays, reported.
    engine.retry_pending_due(Instant::now() + Duration::from_secs(1));
    assert!(engine.health.is_none(), "{:?}", engine.health);
    let reported = engine.reported_health().unwrap();
    assert!(reported.starts_with("RECOVERY_REQUIRED"), "{reported}");
    assert!(reported.contains(&broken), "{reported}");
    assert_eq!(engine.vitals.unrecovered_saves(), 1);
    assert!(engine.vitals.store_health().error().is_some());
    for (index, (id, bytes)) in files.iter().enumerate() {
        if id != &broken {
            assert_eq!(&content(&engine, &format!("unsaved-{index}.bin")), bytes);
        }
    }
    assert_eq!(pending_entities(&engine), vec![broken.clone()]);
    // Nothing is gated: quiet (a graceful Stop), a control mutation.
    engine.quiet().unwrap();
    engine
        .control("mkdir-after", &json!({"op": "mkdir", "path": "made"}))
        .unwrap();
    // Retries keep it (its bytes are gone), and keep reporting it.
    engine.retry_pending_due(Instant::now() + Duration::from_secs(120));
    assert_eq!(pending_entities(&engine), vec![broken.clone()]);
    // `pending-saves` lists it with its error; `discard-pending-save` drops it.
    let listed = engine
        .control("list", &json!({"op": "pending-saves"}))
        .unwrap();
    let saves = listed["pending_saves"].as_array().unwrap();
    assert_eq!(saves.len(), 1, "{listed}");
    assert_eq!(saves[0]["entity"], broken.as_str());
    assert_eq!(saves[0]["restored"], false);
    assert!(
        saves[0]["error"]
            .as_str()
            .unwrap()
            .starts_with("RECOVERY_REQUIRED"),
        "{listed}"
    );
    let discarded = engine
        .control(
            "discard",
            &json!({"op": "discard-pending-save", "entity": broken}),
        )
        .unwrap();
    assert_eq!(discarded["discarded"], true, "{discarded}");
    assert!(pending_entities(&engine).is_empty());
    assert_eq!(engine.reported_health(), None);
    assert_eq!(engine.vitals.unrecovered_saves(), 0);
    assert_eq!(engine.vitals.pending_saves(), 0);
    // A restart finds nothing to restore.
    drop(engine);
    let engine = Engine::new(Store::open_existing(dir.path()).unwrap());
    assert_eq!(engine.reported_health(), None);
    engine.quiet().unwrap();
}

/// The same through the worker's RPC: a graceful `stop` is accepted (it was
/// refused `BUSY_VIEW` for good), and the status reports the store degraded.
#[test]
fn a_worker_with_an_unrestorable_pending_save_stops_gracefully() {
    let dir = tempfile::tempdir().unwrap();
    let files = retained(dir.path(), 2);
    {
        let engine = Engine::new(Store::open_existing(dir.path()).unwrap());
        fs::remove_file(stage_of(&engine, &files[0].0)).unwrap();
    }
    let mut engine = Engine::new(Store::open_existing(dir.path()).unwrap());
    engine.retry_pending_due(Instant::now() + Duration::from_secs(1));
    let engine = Arc::new(Mutex::new(engine));
    let discovery = runtime::start_rpc(engine, dir.path(), None).unwrap();
    let status = runtime::rpc(&discovery, "status", json!({"op": "status"})).unwrap();
    assert!(
        status["health"]
            .as_str()
            .is_some_and(|health| health.starts_with("RECOVERY_REQUIRED")),
        "{status}"
    );
    let stopped = runtime::rpc(&discovery, "stop", json!({"op": "stop"})).unwrap();
    assert_eq!(stopped["stopped"], true, "{stopped}");
}

/// A staging file that a scanner held at startup (here: moved away and back)
/// is restored by the next retry and committed; the store is healthy again.
#[test]
fn a_pending_save_unreadable_at_startup_is_restored_by_a_retry() {
    let dir = tempfile::tempdir().unwrap();
    let files = retained(dir.path(), 2);
    let (held, bytes) = files[0].clone();
    let (stage, aside) = {
        let engine = Engine::new(Store::open_existing(dir.path()).unwrap());
        let stage = stage_of(&engine, &held);
        let aside = stage.with_extension("held");
        fs::rename(&stage, &aside).unwrap();
        (stage, aside)
    };
    let mut engine = Engine::new(Store::open_existing(dir.path()).unwrap());
    let started = Instant::now();
    engine.retry_pending_due(started + Duration::from_millis(1));
    assert_eq!(engine.vitals.unrecovered_saves(), 1);
    assert_eq!(pending_entities(&engine), vec![held.clone()]);
    fs::rename(&aside, &stage).unwrap();
    // The first retry is due after 1 s; the restored session commits on the
    // pass after it.
    engine.retry_pending_due(started + Duration::from_secs(3));
    engine.retry_pending_due(started + Duration::from_secs(6));
    assert_eq!(engine.vitals.unrecovered_saves(), 0);
    assert!(pending_entities(&engine).is_empty());
    assert_eq!(engine.reported_health(), None);
    assert_eq!(content(&engine, "unsaved-0.bin"), bytes);
    assert_eq!(content(&engine, "unsaved-1.bin"), files[1].1);
}

/// A later save of the same file supersedes a record that was not restored:
/// its row goes with the commit and the store is healthy again.
#[test]
fn a_later_save_of_the_same_file_supersedes_an_unrestored_record() {
    let dir = tempfile::tempdir().unwrap();
    let files = retained(dir.path(), 1);
    let (entity, _) = files[0].clone();
    {
        let engine = Engine::new(Store::open_existing(dir.path()).unwrap());
        fs::remove_file(stage_of(&engine, &entity)).unwrap();
    }
    let mut engine = Engine::new(Store::open_existing(dir.path()).unwrap());
    assert!(engine.reported_health().is_some());
    let (h, entry) = engine.open("unsaved-0.bin", None, true).unwrap();
    assert_eq!(entry.id, entity);
    engine.truncate(h, 0, false).unwrap();
    engine.write(h, 0, b"rewritten", false, false).unwrap();
    engine.close(h).unwrap();
    assert!(pending_entities(&engine).is_empty());
    assert_eq!(engine.reported_health(), None);
    assert_eq!(content(&engine, "unsaved-0.bin"), b"rewritten");
}

/// The review's minor: write spools of sessions a crash interrupted
/// (`staging/*.stage` no record names) and temporary object files
/// (`objects/*.part`) were never removed. The worker's startup removes them
/// and keeps the staging files of pending saves.
#[test]
fn orphaned_spools_are_removed_and_pending_ones_kept() {
    let dir = tempfile::tempdir().unwrap();
    let files = retained(dir.path(), 1);
    let kept = {
        let engine = Engine::new(Store::open_existing(dir.path()).unwrap());
        stage_of(&engine, &files[0].0)
    };
    // A session still being written when the process died (never retained):
    // its spool outlives it.
    let orphan = {
        let mut engine = Engine::new(Store::open_existing(dir.path()).unwrap());
        let big: Vec<u8> = vec![7; 1024 * 1024];
        let (h, entry) = engine.open("open.bin", Some("file"), true).unwrap();
        engine.write(h, 0, &big, false, false).unwrap();
        let before: Vec<PathBuf> = fs::read_dir(dir.path().join("staging"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|path| path != &kept)
            .collect();
        assert!(engine.sessions[&entry.id].dirty);
        std::mem::forget(engine);
        assert_eq!(before.len(), 1, "{before:?}");
        before[0].clone()
    };
    let part = dir
        .path()
        .join("objects")
        .join(format!("{}.part", uuid::Uuid::new_v4()));
    fs::write(&part, vec![1u8; 4096]).unwrap();
    let unrelated = dir.path().join("objects").join("notes.part");
    fs::write(&unrelated, b"not ours").unwrap();
    let engine = Engine::new(Store::open_existing(dir.path()).unwrap());
    let (removed, bytes) = engine.remove_orphaned_spools().unwrap();
    assert_eq!(removed, 2);
    assert!(bytes >= 1024 * 1024 + 4096, "{bytes}");
    assert!(
        !orphan.exists(),
        "the interrupted session's spool is still there"
    );
    assert!(!part.exists(), "the temporary object file is still there");
    assert!(unrelated.exists(), "a file that is not ours was removed");
    assert!(kept.exists(), "a pending save's staging file was removed");
    assert_eq!(engine.sessions.len(), 1);
}
