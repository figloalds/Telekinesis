use std::collections::{BTreeMap, BTreeSet};
use tkfs::{
    core::{Bundle, Change, Event, Store, admission::reference_id, id},
    model::{
        content::{ContentKind, Digest, FileContentRef},
        workspace::AdmissionAction,
    },
};

fn setup(enabled: bool) -> (tempfile::TempDir, Store) {
    let temp = tempfile::tempdir().unwrap();
    let store = Store::initialize(temp.path(), "content-admission", &id()).unwrap();
    if enabled {
        store
            .db
            .execute("INSERT INTO config VALUES('fixture:content_v1','1')", [])
            .unwrap();
    }
    (temp, store)
}

fn descriptor(seed: &[u8]) -> FileContentRef {
    // Remote metadata must activate even when this root and all bytes are absent.
    FileContentRef::new(ContentKind::Manifest, Digest::bytes(seed), 1_000_000).unwrap()
}

fn revision(store: &Store, entity: &str, root: &FileContentRef) -> Event {
    let entry = store.entry(entity).unwrap();
    store.event(
        &store.branch(&store.active).unwrap(),
        vec![Change {
            entity: entity.into(),
            field: "content".into(),
            parents: entry
                .revisions
                .get("content")
                .cloned()
                .into_iter()
                .collect(),
            value: serde_json::json!(root),
        }],
    )
}

fn receive(to: &mut Store, events: Vec<Event>) -> anyhow::Result<()> {
    let allowed = events
        .iter()
        .map(|event| event.device.clone())
        .collect::<BTreeSet<_>>();
    to.receive(
        Bundle {
            repo: to.repo.clone(),
            branches: vec![],
            objects: BTreeMap::new(),
            events,
        },
        &allowed,
    )?;
    Ok(())
}

fn count(store: &Store, table: &str) -> i64 {
    store
        .db
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
}

#[test]
fn remote_descriptor_activates_without_any_content_and_survives_restart() {
    let (_source_temp, mut source) = setup(true);
    let (temp, mut destination) = setup(true);
    let file = source.create("file", "file").unwrap();
    receive(&mut destination, source.events().unwrap()).unwrap();
    let root = descriptor(b"unavailable remote manifest");
    let event = revision(&source, &file.id, &root);
    receive(&mut destination, vec![event.clone()]).unwrap();
    assert_eq!(
        destination.lookup("file").unwrap().content_ref,
        Some(root.clone())
    );
    assert!(destination.lookup("file").unwrap().content.is_none());
    assert_eq!(destination.content_admission_page(0, 256).unwrap().len(), 1);
    assert!(destination.verify_derived_state().unwrap().is_empty());
    assert!(
        !temp.path().join("manifests-v1").exists(),
        "metadata activated by opening content"
    );
    drop(destination);
    let mut destination = Store::open_existing(temp.path()).unwrap();
    assert_eq!(destination.lookup("file").unwrap().content_ref, Some(root));
    assert!(destination.verify_derived_state().unwrap().is_empty());
    let page = destination.content_admission_page(0, 256).unwrap();
    assert_eq!(page[0].reference, reference_id(&event.id, &file.id));
    assert!(page[0].shared);
    assert!(
        !page[0].publishable,
        "an incoming hash promoted private deduplicated bytes"
    );
}

#[test]
fn metadata_and_outbox_commit_atomically_and_lost_reply_is_idempotent() {
    let (_temp, mut store) = setup(true);
    let file = store.create("file", "file").unwrap();
    let before = store.projection(&store.active).unwrap();
    let event = revision(&store, &file.id, &descriptor(b"root"));
    store
        .db
        .execute_batch(
            "CREATE TEMP TRIGGER reject_content_outbox BEFORE INSERT ON content_root_outbox
        BEGIN SELECT RAISE(ABORT,'injected outbox failure'); END",
        )
        .unwrap();
    assert!(store.commit(event.clone(), true).is_err());
    assert!(!store.events().unwrap().iter().any(|old| old.id == event.id));
    assert_eq!(count(&store, "content_root_metadata"), 0);
    assert_eq!(count(&store, "content_root_outbox"), 0);
    assert_eq!(store.projection(&store.active).unwrap(), before);
    store
        .db
        .execute_batch("DROP TRIGGER reject_content_outbox")
        .unwrap();
    store.commit(event.clone(), true).unwrap();
    store.commit(event, true).unwrap();
    assert_eq!(count(&store, "content_root_metadata"), 1);
    assert_eq!(count(&store, "content_root_outbox"), 1);
    assert!(store.content_admission_page(0, 256).unwrap()[0].publishable);
}

#[test]
fn consumer_effects_and_cursor_are_atomic_and_replay_after_lost_notification() {
    let (temp, mut store) = setup(true);
    let file = store.create("file", "file").unwrap();
    store
        .commit(revision(&store, &file.id, &descriptor(b"root")), true)
        .unwrap();
    let mut objects = Store::open_secondary(temp.path(), &store.repo, &store.device).unwrap();
    objects
        .db
        .execute_batch(
            "CREATE TEMP TRIGGER reject_admission_cursor BEFORE INSERT ON object_admission_cursor
        BEGIN SELECT RAISE(ABORT,'lost cursor write'); END",
        )
        .unwrap();
    assert!(
        objects
            .consume_content_admission("objects-v1", 256)
            .is_err()
    );
    assert_eq!(objects.content_admission_cursor("objects-v1").unwrap(), 0);
    assert_eq!(count(&objects, "object_root_admission"), 0);
    drop(objects);
    let mut objects = Store::open_secondary(temp.path(), &store.repo, &store.device).unwrap();
    assert_eq!(
        objects.consume_content_admission("objects-v1", 1).unwrap(),
        1
    );
    assert_eq!(objects.content_admission_cursor("objects-v1").unwrap(), 1);
    assert_eq!(
        objects
            .consume_content_admission("objects-v1", 256)
            .unwrap(),
        0
    );
    assert_eq!(
        objects
            .consume_content_admission("second-replay", 256)
            .unwrap(),
        1
    );
    assert_eq!(count(&objects, "object_root_admission"), 1);
    assert!(objects.content_admission_page(0, 257).is_err());
}

#[test]
fn reference_scoped_revocation_rechecks_metadata_before_consumer_catches_up() {
    let (_temp, mut store) = setup(true);
    let first = store.create("first", "file").unwrap();
    let second = store.create("second", "file").unwrap();
    let root = descriptor(b"shared immutable content");
    let a = revision(&store, &first.id, &root);
    store.commit(a.clone(), true).unwrap();
    let b = revision(&store, &second.id, &root);
    store.commit(b.clone(), true).unwrap();
    store.consume_content_admission("objects-v1", 256).unwrap();
    let a = reference_id(&a.id, &first.id);
    let b = reference_id(&b.id, &second.id);
    assert_eq!(
        store
            .set_content_admission(&a, 1, AdmissionAction::Withdraw)
            .unwrap(),
        2
    );
    assert!(!store.content_reference_authorized(&a, 1).unwrap());
    assert!(store.content_reference_authorized(&b, 1).unwrap());
    assert_eq!(
        store
            .set_content_admission(&a, 1, AdmissionAction::Withdraw)
            .unwrap(),
        2
    );
    assert_eq!(
        store
            .set_content_admission(&a, 2, AdmissionAction::Grant)
            .unwrap(),
        3
    );
    assert!(
        store
            .set_content_admission(&a, 1, AdmissionAction::Withdraw)
            .is_err()
    );
    assert!(store.content_reference_authorized(&a, 3).unwrap());
    store.consume_content_admission("objects-v1", 256).unwrap();
    store.consume_content_admission("stale-replay", 1).unwrap();
    let generation: i64 = store
        .db
        .query_row(
            "SELECT generation FROM object_root_admission WHERE reference=?",
            [&a],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(generation, 3);
}

#[test]
fn inactive_and_future_descriptors_are_unacknowledged_not_quarantined() {
    let (_source_temp, mut source) = setup(true);
    let (_temp, mut destination) = setup(false);
    let file = source.create("file", "file").unwrap();
    receive(&mut destination, source.events().unwrap()).unwrap();
    let mut event = revision(&source, &file.id, &descriptor(b"root"));
    let error = receive(&mut destination, vec![event.clone()]).unwrap_err();
    assert!(error.to_string().contains("PEER_NEEDS_UPGRADE"));
    assert_eq!(count(&destination, "incoming_events"), 0);
    assert_eq!(count(&destination, "content_root_outbox"), 0);
    destination
        .db
        .execute("INSERT INTO config VALUES('fixture:content_v1','1')", [])
        .unwrap();
    event.changes[0].value["format"] = serde_json::json!(2);
    assert!(
        receive(&mut destination, vec![event])
            .unwrap_err()
            .to_string()
            .contains("PEER_NEEDS_UPGRADE")
    );
    assert_eq!(count(&destination, "incoming_events"), 0);
}

#[test]
fn an_already_staged_unsupported_descriptor_waits_without_a_corruption_verdict() {
    use tkfs::core::inbox::{SliceBudget, StagedEvent};
    let (_temp, mut store) = setup(true);
    let file = store.create("file", "file").unwrap();
    let event = revision(&store, &file.id, &descriptor(b"root"));
    store
        .stage_incoming(
            vec![StagedEvent {
                event: event.clone(),
                source: None,
            }],
            &BTreeSet::from([store.device.clone()]),
        )
        .unwrap();
    store
        .db
        .execute("DELETE FROM config WHERE key='fixture:content_v1'", [])
        .unwrap();
    let slice = store.activate_slice(SliceBudget::ACTIVATOR).unwrap();
    assert!(slice.quarantined.is_empty());
    assert!(slice.activated.is_empty());
    let state: (String, Option<String>, String) = store
        .db
        .query_row(
            "SELECT state,error,error_class FROM incoming_events WHERE id=?",
            [&event.id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(state, ("retry".into(), None, "incompatible".into()));
    assert!(!store.events().unwrap().iter().any(|old| old.id == event.id));
}

#[test]
fn offline_conflicts_converge_to_oracle_without_manifests_or_chunks() {
    let (_ta, mut a) = setup(true);
    let (_tb, mut b) = setup(true);
    let file = a.create("file", "file").unwrap();
    receive(&mut b, a.events().unwrap()).unwrap();
    let ea = revision(&a, &file.id, &descriptor(b"a"));
    let eb = revision(&b, &file.id, &descriptor(b"b"));
    a.commit(ea.clone(), true).unwrap();
    b.commit(eb.clone(), true).unwrap();
    receive(&mut a, vec![eb.clone(), eb]).unwrap();
    receive(&mut b, vec![ea.clone(), ea]).unwrap();
    assert_eq!(
        a.projection(&a.active).unwrap(),
        b.projection(&b.active).unwrap()
    );
    assert!(a.verify_derived_state().unwrap().is_empty());
    assert!(b.verify_derived_state().unwrap().is_empty());
    let projection = a.projection(&a.active).unwrap();
    assert_eq!(projection.heads[&format!("{}/content", file.id)].len(), 2);
    assert_eq!(count(&a, "content_root_metadata"), 2);
    assert_eq!(count(&b, "content_root_metadata"), 2);
}

#[test]
fn worker_consumes_durable_roots_while_the_semantic_engine_is_held() {
    use std::{
        sync::{Arc, Mutex, atomic::Ordering},
        thread,
        time::{Duration, Instant},
    };
    use tkfs::{
        runtime::{Engine, Shared},
        worker,
    };
    let (temp, mut store) = setup(true);
    let file = store.create("file", "file").unwrap();
    store
        .commit(revision(&store, &file.id, &descriptor(b"first")), true)
        .unwrap();
    let read = Store::open_secondary(temp.path(), &store.repo, &store.device).unwrap();
    let engine: Shared = Arc::new(Mutex::new(Engine::new(store)));
    let ctx = worker::build_ctx(engine.clone(), temp.path(), false, None);
    let maintenance = worker::maintenance::spawn(ctx.clone()).unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    while read.content_admission_cursor("objects-v1").unwrap() < 1 && Instant::now() < until {
        thread::sleep(Duration::from_millis(10));
    }
    let mut held = engine.lock().unwrap();
    let event = revision(&held.store, &file.id, &descriptor(b"second"));
    held.store.commit(event, true).unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    while read.content_admission_cursor("objects-v1").unwrap() < 2 && Instant::now() < until {
        thread::sleep(Duration::from_millis(10));
    }
    let cursor = read.content_admission_cursor("objects-v1").unwrap();
    ctx.stopping.store(true, Ordering::Relaxed);
    drop(held);
    maintenance.join().unwrap();
    assert_eq!(
        cursor, 2,
        "root handoff waited for the semantic engine lock"
    );
}
