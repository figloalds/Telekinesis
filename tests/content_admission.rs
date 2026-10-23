use std::collections::{BTreeMap, BTreeSet};
use tkfs::{
    core::{Bundle, Change, Event, Store, admission::reference_id, id},
    model::{
        content::{ContentKind, Digest, FileContentRef},
        workspace::AdmissionAction,
    },
};

fn setup() -> (tempfile::TempDir, Store) {
    let temp = tempfile::tempdir().unwrap();
    let store = Store::initialize(temp.path(), "content-admission", &id()).unwrap();
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
fn scalar_content_never_commits_or_partially_stages_a_metadata_batch() {
    use tkfs::core::inbox::StagedEvent;
    let (_temp, mut store) = setup();
    let file = store.create("file", "file").unwrap();
    let before = store.projection(&store.active).unwrap();
    let events = store.events().unwrap();
    for value in [
        serde_json::json!(tkfs::core::hash(b"former whole-file id")),
        serde_json::json!(""),
        serde_json::Value::Null,
        serde_json::json!(false),
        serde_json::json!(7),
        serde_json::json!([]),
        serde_json::json!(["direct", 1, "sha256", tkfs::core::hash(b"tuple"), 5]),
    ] {
        for local in [true, false] {
            let mut invalid = revision(&store, &file.id, &descriptor(b"valid shape"));
            invalid.changes[0].value = value.clone();
            let error = store.commit(invalid, local).unwrap_err();
            assert!(
                format!("{error:#}").contains("INVALID_CONTENT"),
                "{error:#}"
            );
            assert_eq!(store.events().unwrap(), events);
            assert_eq!(store.projection(&store.active).unwrap(), before);
            assert_eq!(count(&store, "content_root_metadata"), 1);
            assert_eq!(count(&store, "content_root_outbox"), 1);
        }
        let valid = revision(&store, &file.id, &descriptor(b"should not stage early"));
        let mut invalid = valid.clone();
        invalid.id = id();
        invalid.changes[0].value = value;
        let error = store
            .stage_incoming(
                vec![
                    StagedEvent {
                        event: valid,
                        source: None,
                    },
                    StagedEvent {
                        event: invalid,
                        source: None,
                    },
                ],
                &BTreeSet::from([store.device.clone()]),
            )
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("INVALID_CONTENT"),
            "{error:#}"
        );
        assert_eq!(store.inbox_counts().unwrap().total(), 0);
        assert_eq!(count(&store, "incoming_sources"), 0);
        assert_eq!(count(&store, "content_root_outbox"), 1);
    }
}

#[test]
fn a_digest_matching_string_content_feed_payload_is_refused_without_ack() {
    use tkfs::{model::protocol::feed, paired_sync::SyncRoute};
    let (_ta, mut a) = setup();
    let (_tb, mut b) = setup();
    let file = a.create("file", "file").unwrap();
    receive(&mut b, a.events().unwrap()).unwrap();
    let revision = a
        .write_revision(
            &file.id,
            file.revisions.get("content").cloned(),
            b"current bytes",
        )
        .unwrap();
    let route = |from: &Store, to: &Store| SyncRoute {
        peer: format!("installation-of-{}", to.device),
        repo: from.repo.clone(),
        local_replica: from.device.clone(),
        remote_replica: to.device.clone(),
    };
    let request = b.build_exchange_request(&route(&b, &a)).unwrap();
    let (mut reply, _) = a.handle_exchange_request(&route(&a, &b), &request).unwrap();
    let entry = reply
        .page
        .entries
        .iter_mut()
        .find(|entry| entry.id == revision)
        .unwrap();
    let event = entry.event.as_mut().unwrap();
    event
        .changes
        .iter_mut()
        .find(|change| change.field == "content")
        .unwrap()
        .value = serde_json::json!(tkfs::core::hash(b"current bytes"));
    entry.digest = feed::digest(&serde_json::to_string(event).unwrap());
    let result = b
        .apply_exchange_reply(&route(&b, &a), &request.request, &reply)
        .unwrap();
    assert_eq!(result.summary.rejected, 1);
    assert_eq!(b.inbox_counts().unwrap().total(), 0);
    assert!(!b.events().unwrap().iter().any(|event| event.id == revision));
    assert_eq!(count(&b, "content_root_metadata"), 1);
    assert_eq!(count(&b, "content_root_outbox"), 1);
    let known: bool =
        b.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM peer_known WHERE peer=?1 AND event=?2)",
            rusqlite::params![a.device, revision],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!known);
    let ack: bool =
        a.db.query_row(
            "SELECT acknowledged FROM outbox WHERE event=?1",
            [&revision],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!ack);
    let code: String =
        b.db.query_row(
            "SELECT code FROM feed_rejected WHERE peer=?1 AND event=?2",
            rusqlite::params![a.device, revision],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(code, "INVALID_CONTENT");
}

#[test]
fn string_content_bundle_is_refused_before_bytes_or_shared_provenance_are_installed() {
    let (_temp, mut store) = setup();
    let file = store.create("file", "file").unwrap();
    let bytes = b"unsupported whole-file payload";
    let object = tkfs::core::hash(bytes);
    let mut event = revision(&store, &file.id, &descriptor(b"unused"));
    event.changes[0].value = serde_json::json!(object);
    event
        .object_sizes
        .insert(object.clone(), bytes.len() as u64);
    let before = store.projection(&store.active).unwrap();
    let error = store
        .receive(
            Bundle {
                repo: store.repo.clone(),
                branches: vec![],
                events: vec![event],
                objects: BTreeMap::from([(object.clone(), hex::encode(bytes))]),
            },
            &BTreeSet::from([store.device.clone()]),
        )
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("INVALID_CONTENT"),
        "{error:#}"
    );
    assert!(!store.root.join("objects").join(&object).exists());
    assert_eq!(count(&store, "received_shared_objects"), 0);
    assert_eq!(count(&store, "object_sizes"), 0);
    assert_eq!(store.inbox_counts().unwrap().total(), 0);
    assert_eq!(count(&store, "content_root_metadata"), 1);
    assert_eq!(count(&store, "content_root_outbox"), 1);
    assert_eq!(store.projection(&store.active).unwrap(), before);
}

#[test]
fn projection_codec_and_visible_sizes_require_current_file_descriptors() {
    let (_temp, mut store) = setup();
    let file = store.create("file", "file").unwrap();
    let mut value = serde_json::to_value(&file).unwrap();
    value["content"] = serde_json::json!(tkfs::core::hash(b"old projection"));
    assert!(
        serde_json::from_value::<tkfs::core::Entry>(value)
            .unwrap_err()
            .to_string()
            .contains("INVALID_CONTENT")
    );
    let mut invalid = file.clone();
    invalid.content = Some(tkfs::core::hash(b"old projection"));
    assert!(
        serde_json::to_value(&invalid)
            .unwrap_err()
            .to_string()
            .contains("INVALID_CONTENT")
    );
    invalid.content_ref = None;
    assert!(
        invalid.content_value().is_none(),
        "string field became a content register"
    );
    let directory = store.create("directory", "directory").unwrap();
    let mut metadata_only = file.clone();
    let reference = descriptor(b"absent large manifest");
    metadata_only.content_ref = Some(reference.clone());
    let engine = tkfs::runtime::Engine::new(store);
    assert_eq!(
        engine.visible_size(&metadata_only).unwrap(),
        reference.total_len as usize
    );
    assert_eq!(engine.visible_size(&directory).unwrap(), 0);
    assert!(
        engine
            .visible_size(&invalid)
            .unwrap_err()
            .to_string()
            .contains("MISSING_CONTENT_DESCRIPTOR")
    );
    assert_eq!(
        serde_json::from_value::<tkfs::core::Entry>(serde_json::to_value(file.clone()).unwrap())
            .unwrap(),
        file
    );
}

#[test]
fn remote_descriptor_activates_without_any_content_and_survives_restart() {
    let (_source_temp, mut source) = setup();
    let (temp, mut destination) = setup();
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
    assert_eq!(destination.content_admission_page(0, 256).unwrap().len(), 2);
    assert!(destination.verify_derived_state().unwrap().is_empty());
    assert!(
        std::fs::read_dir(temp.path().join("manifests-v1"))
            .unwrap()
            .next()
            .is_none(),
        "metadata activated by opening content"
    );
    drop(destination);
    let mut destination = Store::open_existing(temp.path()).unwrap();
    assert_eq!(destination.lookup("file").unwrap().content_ref, Some(root));
    assert!(destination.verify_derived_state().unwrap().is_empty());
    let page = destination.content_admission_page(0, 256).unwrap();
    let admitted = page
        .iter()
        .find(|change| change.reference == reference_id(&event.id, &file.id))
        .unwrap();
    assert!(admitted.shared);
    assert_eq!(
        page.iter()
            .filter(|change| change.root.total_len == 0)
            .count(),
        1
    );
    assert!(
        !admitted.publishable,
        "an incoming hash promoted private deduplicated bytes"
    );
}

#[test]
fn metadata_and_outbox_commit_atomically_and_lost_reply_is_idempotent() {
    let (_temp, mut store) = setup();
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
    assert_eq!(count(&store, "content_root_metadata"), 1);
    assert_eq!(count(&store, "content_root_outbox"), 1);
    assert_eq!(store.projection(&store.active).unwrap(), before);
    store
        .db
        .execute_batch("DROP TRIGGER reject_content_outbox")
        .unwrap();
    store.commit(event.clone(), true).unwrap();
    store.commit(event, true).unwrap();
    assert_eq!(count(&store, "content_root_metadata"), 2);
    assert_eq!(count(&store, "content_root_outbox"), 2);
    assert!(
        store
            .content_admission_page(0, 256)
            .unwrap()
            .iter()
            .all(|change| change.publishable)
    );
}

#[test]
fn consumer_effects_and_cursor_are_atomic_and_replay_after_lost_notification() {
    let (temp, mut store) = setup();
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
        1
    );
    assert_eq!(objects.content_admission_cursor("objects-v1").unwrap(), 2);
    assert_eq!(
        objects
            .consume_content_admission("second-replay", 256)
            .unwrap(),
        2
    );
    assert_eq!(count(&objects, "object_root_admission"), 2);
    assert!(objects.content_admission_page(0, 257).is_err());
}

#[test]
fn reference_scoped_revocation_rechecks_metadata_before_consumer_catches_up() {
    let (_temp, mut store) = setup();
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
    let original = store
        .content_admission_page(0, 256)
        .unwrap()
        .into_iter()
        .find(|change| change.reference == a)
        .unwrap();
    store
        .consume_content_admission("stale-replay", original.cursor as usize)
        .unwrap();
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
fn current_descriptors_need_no_marker_and_future_formats_are_unacknowledged() {
    let (_source_temp, mut source) = setup();
    let (_temp, mut destination) = setup();
    let file = source.create("file", "file").unwrap();
    receive(&mut destination, source.events().unwrap()).unwrap();
    let current = revision(&source, &file.id, &descriptor(b"root"));
    receive(&mut destination, vec![current.clone()]).unwrap();
    assert!(
        destination
            .events()
            .unwrap()
            .iter()
            .any(|old| old.id == current.id)
    );
    assert_eq!(count(&destination, "content_root_outbox"), 2);
    let mut future = revision(&source, &file.id, &descriptor(b"future root"));
    future.changes[0].value["format"] = serde_json::json!(2);
    assert!(
        receive(&mut destination, vec![future.clone()])
            .unwrap_err()
            .to_string()
            .contains("PEER_NEEDS_UPGRADE")
    );
    assert_eq!(count(&destination, "incoming_events"), 0);
    assert_eq!(count(&destination, "content_root_outbox"), 2);
    assert!(
        !destination
            .events()
            .unwrap()
            .iter()
            .any(|old| old.id == future.id)
    );
}

#[test]
fn an_already_staged_unsupported_descriptor_waits_without_a_corruption_verdict() {
    use tkfs::core::inbox::{SliceBudget, StagedEvent};
    let (_temp, mut store) = setup();
    let file = store.create("file", "file").unwrap();
    let mut event = revision(&store, &file.id, &descriptor(b"root"));
    store
        .stage_incoming(
            vec![StagedEvent {
                event: event.clone(),
                source: None,
            }],
            &BTreeSet::from([store.device.clone()]),
        )
        .unwrap();
    // Inject a future wire descriptor into the durable inbox: normal admission
    // correctly refuses it before staging, so this specifically tests retry.
    event.changes[0].value["format"] = serde_json::json!(2);
    store
        .db
        .execute(
            "UPDATE incoming_events SET payload=? WHERE id=?",
            rusqlite::params![serde_json::to_string(&event).unwrap(), &event.id],
        )
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
    let (_ta, mut a) = setup();
    let (_tb, mut b) = setup();
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
    assert_eq!(count(&a, "content_root_metadata"), 3);
    assert_eq!(count(&b, "content_root_metadata"), 3);
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
    let (temp, mut store) = setup();
    let file = store.create("file", "file").unwrap();
    store
        .commit(revision(&store, &file.id, &descriptor(b"first")), true)
        .unwrap();
    let first_cursor = store
        .content_admission_page(0, 256)
        .unwrap()
        .last()
        .unwrap()
        .cursor;
    let read = Store::open_secondary(temp.path(), &store.repo, &store.device).unwrap();
    let engine: Shared = Arc::new(Mutex::new(Engine::new(store)));
    let ctx = worker::build_ctx(engine.clone(), temp.path(), false, None);
    let maintenance = worker::maintenance::spawn(ctx.clone()).unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    while read.content_admission_cursor("objects-v1").unwrap() < first_cursor
        && Instant::now() < until
    {
        thread::sleep(Duration::from_millis(10));
    }
    let mut held = engine.lock().unwrap();
    let event = revision(&held.store, &file.id, &descriptor(b"second"));
    held.store.commit(event, true).unwrap();
    let second_cursor = held
        .store
        .content_admission_page(first_cursor, 256)
        .unwrap()
        .last()
        .unwrap()
        .cursor;
    let until = Instant::now() + Duration::from_secs(5);
    while read.content_admission_cursor("objects-v1").unwrap() < second_cursor
        && Instant::now() < until
    {
        thread::sleep(Duration::from_millis(10));
    }
    let cursor = read.content_admission_cursor("objects-v1").unwrap();
    ctx.stopping.store(true, Ordering::Relaxed);
    drop(held);
    maintenance.join().unwrap();
    assert_eq!(
        cursor, second_cursor,
        "root handoff waited for the semantic engine lock"
    );
}
