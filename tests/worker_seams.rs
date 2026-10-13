//! WP0.2 engine seams: `Engine::retain_all_dirty` and `runtime::lock_engine`.
use std::sync::{Arc, Mutex};
use tkfs::{
    core::*,
    runtime::{self, Engine},
};

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

fn content(engine: &Engine, path: &str) -> Vec<u8> {
    let entry = engine.store.lookup(path).unwrap();
    engine
        .store
        .read_object(entry.content.as_deref().unwrap())
        .unwrap()
}

#[test]
fn retain_all_dirty_persists_every_dirty_session() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::new(Store::open(dir.path(), Some("worker-seams")).unwrap());

    // Committed content that is edited later; its base must survive recovery.
    let (h, _) = engine.open("edited.txt", Some("file"), true).unwrap();
    engine.write(h, 0, b"committed v1", false, false).unwrap();
    engine.close(h).unwrap();
    // Two clean sessions with open handles: never retained.
    let (flushed, _) = engine.open("flushed.txt", Some("file"), true).unwrap();
    engine
        .write(flushed, 0, b"already durable", false, false)
        .unwrap();
    engine.flush(flushed).unwrap();
    let (untouched, untouched_entry) = engine.open("untouched.txt", Some("file"), false).unwrap();

    // Three dirty sessions, all with open handles (the force-detach shape):
    // in-memory staging, disk-spooled staging, and an edit of committed content.
    let small = b"small unsaved bytes".to_vec();
    let large: Vec<u8> = (0..300 * 1024).map(|i| (i % 251) as u8).collect();
    let edited = b"edited v2, longer than v1 and never committed".to_vec();
    let (a, a_entry) = engine.open("small.txt", Some("file"), true).unwrap();
    engine.write(a, 0, &small, false, false).unwrap();
    let (b, b_entry) = engine.open("large.bin", Some("file"), true).unwrap();
    engine.write(b, 0, &large, false, false).unwrap();
    let (c, c_entry) = engine.open("edited.txt", None, true).unwrap();
    let edited_base = engine.session(c).unwrap().content_base.clone();
    assert!(edited_base.is_some());
    engine.write(c, 0, &edited, false, false).unwrap();
    assert!(engine.health.is_none());
    assert!(pending_entities(&engine).is_empty());

    let mut expected = vec![a_entry.id.clone(), b_entry.id.clone(), c_entry.id.clone()];
    expected.sort();
    assert_eq!(engine.retain_all_dirty().unwrap(), 3);
    assert_eq!(pending_entities(&engine), expected);
    // Retaining never commits: sessions stay dirty and the view stays busy.
    for id in &expected {
        assert!(engine.sessions[id].dirty);
    }
    assert!(!engine.sessions[&untouched_entry.id].dirty);
    assert!(
        engine
            .health
            .as_deref()
            .unwrap()
            .starts_with("PENDING_SAVE: ")
    );
    assert!(engine.quiet().is_err());
    assert_eq!(content(&engine, "edited.txt"), b"committed v1");
    // Idempotent: a second pass rewrites the same rows.
    assert_eq!(engine.retain_all_dirty().unwrap(), 3);
    assert_eq!(pending_entities(&engine), expected);
    assert!(engine.handles.contains_key(&untouched) && engine.handles.contains_key(&flushed));

    // Crash without flushing: handles vanish, the retained staging survives.
    drop(engine);
    let mut recovered = Engine::new(Store::open_existing(dir.path()).unwrap());
    assert!(recovered.health.is_some());
    assert_eq!(recovered.sessions.len(), 3);
    assert_eq!(recovered.sessions[&a_entry.id].bytes, &small);
    assert_eq!(recovered.sessions[&b_entry.id].bytes, &large);
    assert_eq!(recovered.sessions[&c_entry.id].bytes, &edited);
    assert_eq!(recovered.sessions[&c_entry.id].content_base, edited_base);
    recovered.retry_pending().unwrap();
    assert!(recovered.health.is_none());
    recovered.quiet().unwrap();
    assert!(pending_entities(&recovered).is_empty());
    assert_eq!(content(&recovered, "small.txt"), small);
    assert_eq!(content(&recovered, "large.bin"), large);
    assert_eq!(content(&recovered, "edited.txt"), edited);
    assert_eq!(content(&recovered, "flushed.txt"), b"already durable");
    assert!(recovered.store.conflicts().unwrap().is_empty());
}

#[test]
fn retain_all_dirty_without_dirty_sessions_is_a_no_op() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::new(Store::open(dir.path(), Some("worker-seams")).unwrap());
    let (h, _) = engine.open("clean.txt", Some("file"), true).unwrap();
    assert_eq!(engine.retain_all_dirty().unwrap(), 0);
    assert!(engine.health.is_none());
    assert!(pending_entities(&engine).is_empty());
    engine.close(h).unwrap();
    engine.quiet().unwrap();
}

#[test]
fn lock_engine_guards_the_shared_engine() {
    let dir = tempfile::tempdir().unwrap();
    let shared: runtime::Shared = Arc::new(Mutex::new(Engine::new(
        Store::open(dir.path(), Some("worker-seams")).unwrap(),
    )));
    {
        let mut engine = runtime::lock_engine(&shared);
        let (h, _) = engine.open("seam.txt", Some("file"), true).unwrap();
        engine
            .write(h, 0, b"through the seam", false, false)
            .unwrap();
        engine.close(h).unwrap();
        assert!(shared.try_lock().is_err());
    }
    let engine = runtime::lock_engine(&shared);
    engine.quiet().unwrap();
    assert_eq!(content(&engine, "seam.txt"), b"through the seam");
}
