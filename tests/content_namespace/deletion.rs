use super::*;
fn records(f: &Fixture) -> Vec<tkfs::core::content_saves::ContentDeleteIntent> {
    f.store
        .content_delete_intents(None, 16, &context())
        .unwrap()
}
#[test]
fn dispositions_are_durable_idempotent_cancelable_and_do_not_publish_or_delete() {
    let mut f = Fixture::new();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let before = f.store.events().unwrap();
    let first = f
        .store
        .stage_content_delete(a, f.saved.intent.operation, &context())
        .unwrap()
        .unwrap();
    assert_eq!(
        f.store
            .stage_content_delete(a, f.saved.intent.operation, &context())
            .unwrap(),
        Some(first)
    );
    f.store
        .stage_content_delete(b, f.saved.intent.operation, &context())
        .unwrap();
    assert_eq!(records(&f).len(), 2);
    assert_eq!(f.store.events().unwrap(), before);
    assert_eq!(f.publications(), 0);
    error(
        f.store.cancel_content_delete(a, Uuid::new_v4(), &context()),
        "CONTENT_SAVE_INTENT_MISMATCH",
    );
    let branch = f.store.active.clone();
    error(f.store.checkout(&branch), "RECOVERY_REQUIRED");
    f.store
        .cancel_content_delete(a, f.saved.intent.operation, &context())
        .unwrap();
    assert_eq!(records(&f).len(), 1);
    assert_eq!(records(&f)[0].request, b);
    f.store
        .cancel_content_delete(a, f.saved.intent.operation, &context())
        .unwrap();
    f.store
        .cancel_content_delete(b, f.saved.intent.operation, &context())
        .unwrap();
    assert!(records(&f).is_empty());
    assert_eq!(f.store.lookup("file").unwrap().id, f.entity);
    f.oracle();
}
#[test]
fn staging_rechecks_the_writer_after_inserting_and_rolls_back_a_silent_fence() {
    let mut f = Fixture::new();
    let request = Uuid::new_v4();
    f.store.db.execute_batch("CREATE TEMP TRIGGER fence_delete_stage AFTER INSERT ON content_delete_intents BEGIN UPDATE content_save_entities SET owner=NULL,generation=generation+1 WHERE branch=NEW.branch AND entity=NEW.entity; END;").unwrap();
    error(
        f.store
            .stage_content_delete(request, f.saved.intent.operation, &context()),
        "STALE_CONTENT_SAVE",
    );
    assert!(records(&f).is_empty());
    assert_eq!(f.store.lookup("file").unwrap().id, f.entity);
    f.store
        .db
        .execute_batch("DROP TRIGGER fence_delete_stage;")
        .unwrap();
    let record = f
        .store
        .stage_content_delete(request, f.saved.intent.operation, &context())
        .unwrap()
        .unwrap();
    f.store
        .validate_content_delete_stage(&record, &context())
        .unwrap();
    f.oracle();
}
#[test]
fn disposition_tracks_the_entity_through_rename_and_consumes_all_handles_atomically() {
    let mut f = Fixture::new();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let record = f
        .store
        .stage_content_delete(a, f.saved.intent.operation, &context())
        .unwrap()
        .unwrap();
    f.store
        .stage_content_delete(b, f.saved.intent.operation, &context())
        .unwrap();
    f.store
        .apply_content_checkpoint_rename(
            &f.saved.intent,
            Uuid::new_v4(),
            Some(&f.saved),
            "moved",
            false,
            &context(),
        )
        .unwrap();
    let replacement = f.store.create("file", "file").unwrap().id;
    let target = f.store.content_delete_target(&record, &context()).unwrap();
    assert_eq!(target.path, "moved");
    f.store
        .apply_content_checkpoint_unlink(
            NamespaceDestination {
                intent: f.saved.intent.clone(),
                checkpoint: Some(f.saved.clone()),
            },
            a,
            &target,
            &context(),
        )
        .unwrap();
    assert!(records(&f).is_empty());
    assert!(f.store.lookup("moved").is_err());
    assert_eq!(f.store.lookup("file").unwrap().id, replacement);
    assert!(!f.store.entry(&f.entity).unwrap().alive);
    f.oracle();
}
#[test]
fn disposition_survives_a_byte_continuation_and_never_claims_a_superseding_writer() {
    let mut f = Fixture::new();
    let request = Uuid::new_v4();
    let record = f
        .store
        .stage_content_delete(request, f.saved.intent.operation, &context())
        .unwrap()
        .unwrap();
    let prepared = f.session.prepare(&f.local, &f.saved, &context()).unwrap();
    f.store.publish_content_save(&prepared, &context()).unwrap();
    f.store.inspect_content_delete(&record, &context()).unwrap();
    error(
        f.store.validate_content_delete(&record, &context()),
        "STALE_CONTENT_SAVE",
    );
    let next = f.store.continue_content_save(&f.saved, &context()).unwrap();
    assert_eq!(
        f.store
            .validate_content_delete(&record, &context())
            .unwrap(),
        *next.intent()
    );
    let newer = f.store.begin_content_save(&f.entity, &context()).unwrap();
    assert_ne!(newer.operation, record.operation);
    error(
        f.store.validate_content_delete(&record, &context()),
        "STALE_CONTENT_SAVE",
    );
    f.store
        .cancel_content_delete(request, record.operation, &context())
        .unwrap();
    assert!(records(&f).is_empty());
}
#[test]
fn disposition_records_and_pages_are_bounded_and_corruption_is_not_a_default() {
    let mut f = Fixture::new();
    let mut ids = Vec::new();
    for _ in 0..64 {
        let id = Uuid::new_v4();
        f.store
            .stage_content_delete(id, f.saved.intent.operation, &context())
            .unwrap();
        ids.push(id);
    }
    error(
        f.store
            .stage_content_delete(Uuid::new_v4(), f.saved.intent.operation, &context()),
        "CONTENT_RESOURCE_LIMIT",
    );
    error(
        f.store.content_delete_intents(None, 17, &context()),
        "CONTENT_SAVE_CAPTURE_LIMIT",
    );
    ids.sort();
    let mut got = Vec::new();
    let mut after = None;
    loop {
        let page = f
            .store
            .content_delete_intents(after, 16, &context())
            .unwrap();
        if page.is_empty() {
            break;
        }
        after = page.last().map(|r| r.request);
        got.extend(page.into_iter().map(|r| r.request));
    }
    assert_eq!(got, ids);
    f.store
        .db
        .execute(
            "UPDATE content_delete_intents SET entity='foreign-entity' WHERE request=?",
            [ids[0].to_string()],
        )
        .unwrap();
    error(
        f.store.content_delete_intent(ids[0], &context()),
        "CORRUPT_CONTENT_SAVE",
    );
    error(
        f.store.content_delete_intents(None, 16, &context()),
        "CORRUPT_CONTENT_SAVE",
    );
    assert_eq!(f.publications(), 0);
    assert!(f.store.entry(&f.entity).unwrap().alive);
}
#[test]
#[ignore = "actual exit helper for durable disposition/cancellation"]
fn disposition_crash_child() {
    let root = std::path::PathBuf::from(std::env::var_os("TKFS_DELETE_INTENT_ROOT").unwrap());
    let operation =
        Uuid::parse_str(&std::env::var("TKFS_DELETE_INTENT_OPERATION").unwrap()).unwrap();
    let request = Uuid::parse_str(&std::env::var("TKFS_DELETE_INTENT_REQUEST").unwrap()).unwrap();
    let mut store = Store::open_existing(&root).unwrap();
    let point = std::env::var("TKFS_DELETE_INTENT_POINT").unwrap();
    fault::configure(Some(FaultConfig::new(&point, FaultAction::Exit)));
    if point.contains("cancel") {
        store
            .cancel_content_delete(request, operation, &context())
            .unwrap();
    } else {
        store
            .stage_content_delete(request, operation, &context())
            .unwrap();
    }
    panic!("disposition did not reach its requested exit boundary");
}
#[test]
fn process_exit_preserves_disposition_and_cancellation_commit_boundaries() {
    for (point, present) in [
        ("content.save.delete_intent_written", false),
        ("content.save.delete_intent_committed", true),
        ("content.save.delete_cancel_written", true),
        ("content.save.delete_cancel_committed", false),
    ] {
        let mut f = Fixture::new();
        let request = Uuid::new_v4();
        if point.contains("cancel") {
            f.store
                .stage_content_delete(request, f.saved.intent.operation, &context())
                .unwrap();
        }
        let Fixture {
            store,
            local,
            _journal,
            session,
            entity,
            saved,
            _dir,
        } = f;
        drop(store);
        drop(local);
        drop(_journal);
        drop(session);
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "deletion::disposition_crash_child",
                "--ignored",
                "--nocapture",
            ])
            .env("TKFS_DELETE_INTENT_ROOT", _dir.path())
            .env(
                "TKFS_DELETE_INTENT_OPERATION",
                saved.intent.operation.to_string(),
            )
            .env("TKFS_DELETE_INTENT_REQUEST", request.to_string())
            .env("TKFS_DELETE_INTENT_POINT", point)
            .output()
            .unwrap();
        assert_eq!(
            result.status.code(),
            Some(86),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let store = Store::open_existing(_dir.path()).unwrap();
        assert_eq!(
            store
                .content_delete_intent(request, &context())
                .unwrap()
                .is_some(),
            present
        );
        assert_eq!(store.lookup("file").unwrap().id, entity);
    }
}
