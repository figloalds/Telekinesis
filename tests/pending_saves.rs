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
//! * a record is never lost to another branch (WP4.INT confirmation review):
//!   a checkout is refused while a record of the active branch is not
//!   restored, and a record of another branch is never superseded;
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

fn control(engine: &mut Engine, request: &str, payload: Value) -> anyhow::Result<Value> {
    engine.control(request, &payload)
}

fn checkout(engine: &mut Engine, request: &str, branch: &str) -> anyhow::Result<Value> {
    control(engine, request, json!({"op": "checkout", "name": branch}))
}

/// Saves `bytes` as the whole content of `path` (created when `create`).
fn save(engine: &mut Engine, path: &str, create: bool, bytes: &[u8]) {
    let (h, _) = engine.open(path, create.then_some("file"), true).unwrap();
    engine.truncate(h, 0, false).unwrap();
    engine.write(h, 0, bytes, false, false).unwrap();
    engine.close(h).unwrap();
}

fn unsaved_bytes() -> Vec<u8> {
    (0..300 * 1024).map(|i| (i % 241) as u8).collect()
}

/// WP4.INT confirmation review (blocker): `pending_saves` holds one record
/// per entity, each of the branch it was saved on. Since an unrestored record
/// no longer gated `quiet()`, a checkout to another branch was allowed while
/// one existed; a save of the same file there then superseded it (its row
/// deleted with that commit, its staging file removed) and the first branch's
/// unsaved bytes were gone. The review's reproduction: the checkout is now
/// refused (`RECOVERY_REQUIRED`) until the record is restored and saved on its
/// own branch, and after that both branches keep their content.
#[test]
fn a_checkout_never_leaves_an_unrestored_save_of_its_branch_behind() {
    let dir = tempfile::tempdir().unwrap();
    let unsaved = unsaved_bytes();
    let entity = {
        let mut engine = Engine::new(Store::open(dir.path(), Some(REPO)).unwrap());
        save(&mut engine, "f.bin", true, b"committed v1");
        control(
            &mut engine,
            "branch-other",
            json!({"op": "branch", "name": "other"}),
        )
        .unwrap();
        // On main: 300 KiB unsaved (spooled), retained, then a crash (the
        // engine is dropped, its retained spool stays).
        let (h, entry) = engine.open("f.bin", None, true).unwrap();
        engine.truncate(h, 0, false).unwrap();
        engine.write(h, 0, &unsaved, false, false).unwrap();
        assert_eq!(engine.retain_all_dirty().unwrap(), 1);
        drop(engine);
        entry.id
    };
    // A scanner holds the staging file at startup.
    let (stage, aside) = {
        let engine = Engine::new(Store::open_existing(dir.path()).unwrap());
        let stage = stage_of(&engine, &entity);
        let aside = stage.with_extension("held");
        drop(engine);
        fs::rename(&stage, &aside).unwrap();
        (stage, aside)
    };
    let mut engine = Engine::new(Store::open_existing(dir.path()).unwrap());
    let main = engine.store.active.clone();
    let reported = engine.reported_health().unwrap();
    assert!(reported.starts_with("RECOVERY_REQUIRED"), "{reported}");
    // Stop and other requests are not gated; a checkout is.
    engine.quiet().unwrap();
    let refused = checkout(&mut engine, "checkout-1", "other").unwrap_err();
    let message = format!("{refused:#}");
    assert!(
        message.starts_with("RECOVERY_REQUIRED: checkout refused"),
        "{message}"
    );
    assert!(message.contains(&entity), "{message}");
    assert_eq!(engine.store.active, main);
    assert_eq!(pending_entities(&engine), vec![entity.clone()]);
    // The scanner lets go: the retry restores the save and commits it on main.
    fs::rename(&aside, &stage).unwrap();
    let started = Instant::now();
    engine.retry_pending_due(started + Duration::from_secs(3));
    engine.retry_pending_due(started + Duration::from_secs(6));
    assert!(pending_entities(&engine).is_empty());
    assert_eq!(engine.reported_health(), None);
    assert_eq!(content(&engine, "f.bin"), unsaved);
    // Now the checkout is allowed; a save of the same file on the other branch
    // leaves main's content alone.
    checkout(&mut engine, "checkout-2", "other").unwrap();
    assert_eq!(content(&engine, "f.bin"), b"committed v1");
    save(&mut engine, "f.bin", false, b"edited on other");
    checkout(&mut engine, "checkout-3", "main").unwrap();
    for pass in 1..=5 {
        engine.retry_pending_due(started + Duration::from_secs(10 * pass));
    }
    assert_eq!(engine.store.active, main);
    assert_eq!(
        content(&engine, "f.bin"),
        unsaved,
        "main's unsaved file is lost"
    );
    checkout(&mut engine, "checkout-4", "other").unwrap();
    assert_eq!(content(&engine, "f.bin"), b"edited on other");
}

/// Discarding the record (its bytes are gone) unblocks the checkout.
#[test]
fn a_discarded_unrestorable_save_no_longer_blocks_a_checkout() {
    let dir = tempfile::tempdir().unwrap();
    let files = retained(dir.path(), 1);
    let (entity, _) = files[0].clone();
    {
        let engine = Engine::new(Store::open_existing(dir.path()).unwrap());
        fs::remove_file(stage_of(&engine, &entity)).unwrap();
    }
    let mut engine = Engine::new(Store::open_existing(dir.path()).unwrap());
    control(
        &mut engine,
        "branch-other",
        json!({"op": "branch", "name": "other"}),
    )
    .unwrap();
    let refused = checkout(&mut engine, "checkout-1", "other").unwrap_err();
    assert!(
        format!("{refused:#}").starts_with("RECOVERY_REQUIRED: checkout refused"),
        "{refused:#}"
    );
    control(
        &mut engine,
        "discard",
        json!({"op": "discard-pending-save", "entity": entity}),
    )
    .unwrap();
    checkout(&mut engine, "checkout-2", "other").unwrap();
}

/// A record of another branch (left by an earlier build, which allowed the
/// checkout above) is never superseded: a writable open of its file on this
/// branch is refused, a save of the same entity on this branch (a session
/// older than the record's arrival, simulated by lifting the handle's
/// refusal) commits without touching the record, a retain does not overwrite
/// it, and a checkout of its branch restores and commits it there.
#[test]
fn a_pending_save_of_another_branch_is_never_superseded() {
    let dir = tempfile::tempdir().unwrap();
    let unsaved = unsaved_bytes();
    let (entity, main, other) = {
        let mut engine = Engine::new(Store::open(dir.path(), Some(REPO)).unwrap());
        let main = engine.store.active.clone();
        save(&mut engine, "f.bin", true, b"committed v1");
        let other = control(
            &mut engine,
            "branch-other",
            json!({"op": "branch", "name": "other"}),
        )
        .unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned();
        checkout(&mut engine, "checkout-1", "other").unwrap();
        let (h, entry) = engine.open("f.bin", None, true).unwrap();
        engine.truncate(h, 0, false).unwrap();
        engine.write(h, 0, &unsaved, false, false).unwrap();
        assert_eq!(engine.retain_all_dirty().unwrap(), 1);
        drop(engine);
        (entry.id, main, other)
    };
    // The earlier build's checkout back to main, which left the record.
    {
        let db = rusqlite::Connection::open(dir.path().join("metadata.sqlite")).unwrap();
        db.execute("UPDATE config SET value=? WHERE key='active'", [&main])
            .unwrap();
    }
    let mut engine = Engine::new(Store::open_existing(dir.path()).unwrap());
    assert_eq!(engine.store.active, main);
    let record = |engine: &Engine| -> String {
        engine
            .store
            .db
            .query_row(
                "SELECT payload FROM pending_saves WHERE entity=?",
                [&entity],
                |row| row.get(0),
            )
            .unwrap()
    };
    let kept = record(&engine);
    assert!(kept.contains(&other), "{kept}");
    let stage = stage_of(&engine, &entity);
    assert!(engine.reported_health().is_some());
    // A writable open on main is refused; a read is not.
    let refused = engine.open("f.bin", None, true).unwrap_err();
    assert!(
        format!("{refused:#}").starts_with("ACCESS_DENIED"),
        "{refused:#}"
    );
    let (h, _) = engine.open("f.bin", None, false).unwrap();
    assert_eq!(content(&engine, "f.bin"), b"committed v1");
    // A session of main that writes anyway: its retain refuses to overwrite
    // the other branch's record, and its save leaves the record alone.
    engine.handles.get_mut(&h).unwrap().writable = true;
    engine.truncate(h, 0, false).unwrap();
    engine.write(h, 0, b"main edit", false, false).unwrap();
    let error = engine.retain_all_dirty().unwrap_err();
    assert!(
        format!("{error:#}").contains("RECOVERY_BRANCH_MISMATCH"),
        "{error:#}"
    );
    assert_eq!(record(&engine), kept);
    engine.close(h).unwrap();
    assert_eq!(content(&engine, "f.bin"), b"main edit");
    assert_eq!(record(&engine), kept);
    assert!(
        stage.exists(),
        "the other branch's staging file was removed"
    );
    // Main's records do not block a checkout; the record is restored and
    // committed on its own branch.
    engine.quiet().unwrap();
    checkout(&mut engine, "checkout-2", "other").unwrap();
    let now = Instant::now();
    engine.retry_pending_due(now);
    engine.retry_pending_due(now + Duration::from_millis(10));
    assert!(pending_entities(&engine).is_empty());
    assert_eq!(engine.reported_health(), None);
    assert_eq!(content(&engine, "f.bin"), unsaved);
    checkout(&mut engine, "checkout-3", "main").unwrap();
    assert_eq!(content(&engine, "f.bin"), b"main edit");
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

/// The production review's minor (RC9): a retained save opened its
/// `BEGIN IMMEDIATE` before storing the file's bytes, so SQLite's writer slot
/// was held for the whole hash, copy and sync of the object (another
/// connection's writer failed after its 5 s busy timeout behind a 3 GiB
/// save). The bytes are now stored first: while the flush runs, another
/// connection finds the writer slot free for a good part of it (before the
/// fix: never after the first moments).
#[test]
fn a_retained_save_takes_the_writer_only_for_its_commit() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::new(Store::open(dir.path(), Some(REPO)).unwrap());
    let large: Vec<u8> = (0..32 * 1024 * 1024).map(|i| (i % 253) as u8).collect();
    let (h, entry) = engine.open("large.bin", Some("file"), true).unwrap();
    engine.write(h, 0, &large, false, false).unwrap();
    assert_eq!(engine.retain_all_dirty().unwrap(), 1);
    let flushing = Arc::new(AtomicBool::new(true));
    let probe = {
        let flushing = flushing.clone();
        let database = dir.path().join("metadata.sqlite");
        std::thread::spawn(move || {
            let other = rusqlite::Connection::open(database).unwrap();
            other.busy_timeout(Duration::ZERO).unwrap();
            let (mut free, mut held) = (0u32, 0u32);
            while flushing.load(Ordering::Relaxed) {
                match other.execute_batch("BEGIN IMMEDIATE") {
                    Ok(()) => {
                        other.execute_batch("ROLLBACK").unwrap();
                        free += 1;
                    }
                    Err(_) => held += 1,
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            (free, held)
        })
    };
    std::thread::sleep(Duration::from_millis(20));
    engine.flush(h).unwrap();
    flushing.store(false, Ordering::Relaxed);
    let (free, held) = probe.join().unwrap();
    assert!(!engine.sessions[&entry.id].dirty);
    assert!(pending_entities(&engine).is_empty());
    let saved = engine.store.lookup("large.bin").unwrap();
    assert_eq!(saved.content.as_deref(), Some(hash(&large).as_str()));
    let samples = free + held;
    eprintln!("writer slot free for {free} of {samples} probes during the save");
    assert!(samples >= 20, "{free} free, {held} held");
    // The probe ran 20 ms before the flush; the copy phase must leave the
    // slot free for a good part of the rest.
    assert!(
        f64::from(free) / f64::from(samples) >= 0.3,
        "the writer slot was free for {free} of {samples} probes during the save"
    );
}
