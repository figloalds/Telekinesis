use super::*;
use tkfs::core::content_saves::{DirectoryMutation, DirectoryMutationRequest, NamespaceTarget};

fn capture(f: &Fixture, entity: &str, action: DirectoryMutation) -> DirectoryMutationRequest {
    DirectoryMutationRequest {
        mutation: Uuid::new_v4(),
        before: f.store.directory_snapshot(entity, &context()).unwrap(),
        action,
    }
}
fn target(f: &Fixture, path: &str) -> NamespaceTarget {
    NamespaceTarget {
        path: path.into(),
        parent: f
            .store
            .lookup(&tkfs::core::split_path(path).unwrap().0)
            .unwrap()
            .id,
    }
}
fn update(write_time: u64) -> DirectoryMutation {
    DirectoryMutation::Basic(ContentBasicUpdate {
        write_time: Some(write_time),
        ..Default::default()
    })
}

#[test]
fn directory_changes_merge_exact_metadata_and_replay_receipts_without_replaying_names() {
    let mut f = Fixture::new();
    let id = f.store.create("dir", "directory").unwrap().id;
    let initial = f.store.entry(&id).unwrap().basic_info();
    let q = capture(&f, &id, update(101));
    let receipt = f.store.apply_directory_mutation(&q, &context()).unwrap();
    let basic = f.store.entry(&id).unwrap().basic_info();
    assert_eq!(basic.write_time, 101);
    assert_eq!(basic.access_time, initial.access_time);
    assert_eq!(basic.creation_time, initial.creation_time);
    assert_eq!(basic.change_time, initial.change_time);
    assert_eq!(basic.attributes, 0x10);
    let rename = capture(
        &f,
        &id,
        DirectoryMutation::Rename {
            source: Some(target(&f, "dir")),
            target: target(&f, "moved"),
            destination: None,
        },
    );
    f.store
        .apply_directory_mutation(&rename, &context())
        .unwrap();
    let remove = capture(
        &f,
        &id,
        DirectoryMutation::Remove {
            source: Some(target(&f, "moved")),
        },
    );
    f.store
        .apply_directory_mutation(&remove, &context())
        .unwrap();
    assert!(f.store.directory_is_removed(&id, &context()).unwrap());
    let dead = capture(&f, &id, update(202));
    f.store.apply_directory_mutation(&dead, &context()).unwrap();
    assert!(!f.store.entry(&id).unwrap().alive);
    assert_eq!(f.store.entry(&id).unwrap().basic_info().write_time, 202);
    let new = f.store.create("dir", "directory").unwrap().id;
    assert_eq!(
        f.store.apply_directory_mutation(&q, &context()).unwrap(),
        receipt
    );
    assert_eq!(f.store.lookup("dir").unwrap().id, new);
    let mut altered = q;
    altered.action = update(303);
    error(
        f.store.apply_directory_mutation(&altered, &context()),
        "CONTENT_METADATA_MUTATION_REUSED",
    );
    assert_eq!(f.publications(), 0);
    f.oracle();
}

#[test]
fn directory_changes_refuse_stale_heads_paths_nonempty_targets_and_cycles() {
    let mut f = Fixture::new();
    let id = f.store.create("dir", "directory").unwrap().id;
    let stale = capture(&f, &id, update(101));
    let fresh = capture(&f, &id, update(202));
    f.store
        .apply_directory_mutation(&fresh, &context())
        .unwrap();
    error(
        f.store.apply_directory_mutation(&stale, &context()),
        "STALE_CONTENT_DIRECTORY",
    );
    let child = f.store.create("dir/child", "directory").unwrap().id;
    let remove = capture(&f, &id, DirectoryMutation::Remove { source: None });
    error(
        f.store.apply_directory_mutation(&remove, &context()),
        "DIRECTORY_NOT_EMPTY",
    );
    let cycle = capture(
        &f,
        &id,
        DirectoryMutation::Rename {
            source: None,
            target: target(&f, "dir/child/dir"),
            destination: None,
        },
    );
    error(
        f.store.apply_directory_mutation(&cycle, &context()),
        "DIRECTORY_CYCLE",
    );
    let other = f.store.create("other", "directory").unwrap().id;
    let wrong = capture(
        &f,
        &id,
        DirectoryMutation::Remove {
            source: Some(target(&f, "other")),
        },
    );
    error(
        f.store.apply_directory_mutation(&wrong, &context()),
        "STALE_CONTENT_DESTINATION",
    );
    let replace = capture(
        &f,
        &other,
        DirectoryMutation::Rename {
            source: None,
            target: target(&f, "dir"),
            destination: Some(Box::new(
                f.store.directory_snapshot(&id, &context()).unwrap(),
            )),
        },
    );
    error(
        f.store.apply_directory_mutation(&replace, &context()),
        "DIRECTORY_NOT_EMPTY",
    );
    f.store.delete(&child).unwrap();
    f.store
        .apply_directory_mutation(&replace, &context())
        .unwrap();
    assert_eq!(f.store.lookup("dir").unwrap().id, other);
    assert!(f.store.directory_is_removed(&id, &context()).unwrap());
    let stale = capture(&f, &id, update(101));
    let entry = f.store.entry(&id).unwrap();
    let event = f.store.event(
        &f.store.branch(&f.store.active).unwrap(),
        vec![tkfs::core::Change {
            entity: id.clone(),
            field: "alive".into(),
            parents: vec![entry.revisions["alive"].clone()],
            value: serde_json::json!(true),
        }],
    );
    f.store.commit(event, true).unwrap();
    error(
        f.store.apply_directory_mutation(&stale, &context()),
        "STALE_CONTENT_DIRECTORY",
    );
    f.oracle();
}

#[test]
fn directory_dispositions_rebase_owned_changes_block_children_cancel_and_consume_exactly() {
    let mut f = Fixture::new();
    let id = f.store.create("dir", "directory").unwrap().id;
    let snapshot = f.store.directory_snapshot(&id, &context()).unwrap();
    let one = Uuid::new_v4();
    let two = Uuid::new_v4();
    f.store
        .stage_directory_delete(one, &snapshot, &context())
        .unwrap();
    f.store
        .stage_directory_delete(two, &snapshot, &context())
        .unwrap();
    error(f.store.checkout("main"), "RECOVERY_REQUIRED");
    let request = super::directory::request(&f, "dir/new");
    error(
        f.store.apply_directory_create(&request, &context()),
        "ACCESS_DENIED",
    );
    let basic = capture(&f, &id, update(202));
    f.store
        .apply_directory_mutation(&basic, &context())
        .unwrap();
    let rename = capture(
        &f,
        &id,
        DirectoryMutation::Rename {
            source: None,
            target: target(&f, "moved"),
            destination: None,
        },
    );
    f.store
        .apply_directory_mutation(&rename, &context())
        .unwrap();
    for request in [one, two] {
        let row = f
            .store
            .directory_delete_intent(request, &context())
            .unwrap()
            .unwrap();
        f.store.validate_directory_delete(&row, &context()).unwrap();
        assert_eq!(row.snapshot.entry.name, "moved");
        assert_eq!(row.snapshot.entry.basic_info().write_time, 202);
    }
    f.store
        .cancel_directory_delete(one, &id, &context())
        .unwrap();
    assert!(
        f.store
            .directory_delete_intent(one, &context())
            .unwrap()
            .is_none()
    );
    let pending = f
        .store
        .directory_delete_intent(two, &context())
        .unwrap()
        .unwrap();
    let remove = DirectoryMutationRequest {
        mutation: two,
        before: pending.snapshot,
        action: DirectoryMutation::Remove { source: None },
    };
    f.store
        .apply_directory_mutation(&remove, &context())
        .unwrap();
    assert!(
        f.store
            .directory_delete_intents(None, 16, &context())
            .unwrap()
            .is_empty()
    );
    assert!(!f.store.entry(&id).unwrap().alive);
    assert_eq!(f.publications(), 0);
    f.oracle();
}

#[test]
fn directory_dispositions_do_not_adopt_foreign_changes_or_mutation_identities() {
    let mut f = Fixture::new();
    let id = f.store.create("dir", "directory").unwrap().id;
    let snapshot = f.store.directory_snapshot(&id, &context()).unwrap();
    let request = Uuid::new_v4();
    let row = f
        .store
        .stage_directory_delete(request, &snapshot, &context())
        .unwrap();
    f.store.rename(&id, "foreign", false).unwrap();
    error(
        f.store.validate_directory_delete(&row, &context()),
        "STALE_CONTENT_DIRECTORY",
    );
    let q = capture(&f, &id, update(101));
    error(
        f.store.apply_directory_mutation(&q, &context()),
        "STALE_CONTENT_DIRECTORY",
    );
    f.store
        .cancel_directory_delete(request, &id, &context())
        .unwrap();
    let other = f.store.create("other", "directory").unwrap().id;
    let snapshot = f.store.directory_snapshot(&other, &context()).unwrap();
    f.store
        .stage_directory_delete(request, &snapshot, &context())
        .unwrap();
    let mut q = capture(&f, &id, DirectoryMutation::Remove { source: None });
    q.mutation = request;
    error(
        f.store.apply_directory_mutation(&q, &context()),
        "CONTENT_METADATA_MUTATION_REUSED",
    );
    assert!(f.store.entry(&id).unwrap().alive);
    assert!(f.store.entry(&other).unwrap().alive);
    f.oracle();
}

#[test]
fn directory_mutations_and_dispositions_roll_back_silent_receipt_loss() {
    for sql in [
        "CREATE TEMP TRIGGER sabotage BEFORE INSERT ON content_save_namespace_changes BEGIN SELECT RAISE(IGNORE); END;",
        "CREATE TEMP TRIGGER sabotage AFTER INSERT ON content_save_namespace_changes BEGIN DELETE FROM directory_delete_intents; END;",
        "CREATE TEMP TRIGGER sabotage AFTER INSERT ON content_save_namespace_changes BEGIN DELETE FROM events WHERE id=NEW.mutation; END;",
    ] {
        let mut f = Fixture::new();
        let id = f.store.create("dir", "directory").unwrap().id;
        let snapshot = f.store.directory_snapshot(&id, &context()).unwrap();
        let request = Uuid::new_v4();
        f.store
            .stage_directory_delete(request, &snapshot, &context())
            .unwrap();
        let q = capture(&f, &id, update(101));
        f.store.db.execute_batch(sql).unwrap();
        assert!(
            f.store.apply_directory_mutation(&q, &context()).is_err(),
            "{sql}"
        );
        assert_eq!(
            f.store.directory_snapshot(&id, &context()).unwrap(),
            snapshot
        );
        assert_eq!(
            f.store
                .directory_delete_intent(request, &context())
                .unwrap()
                .unwrap()
                .snapshot,
            snapshot
        );
        f.store.db.execute_batch("DROP TRIGGER sabotage").unwrap();
        f.store.apply_directory_mutation(&q, &context()).unwrap();
        f.oracle();
    }
}

#[test]
fn directory_change_faults_have_one_exact_durable_outcome() {
    for point in [
        "content.directory.change_before_event",
        "content.directory.change_event_written",
        "content.directory.change_written",
        "content.directory.change_committed",
    ] {
        let mut f = Fixture::new();
        let id = f.store.create("dir", "directory").unwrap().id;
        let q = capture(&f, &id, update(101));
        let mut config = FaultConfig::new(point, FaultAction::Fail);
        config.count = Some(1);
        fault::configure(Some(config));
        let result = f.store.apply_directory_mutation(&q, &context());
        let hits = fault::fired();
        fault::configure(None);
        error(result, "FAULT_INJECTED");
        assert_eq!(hits, 1);
        assert_eq!(
            f.store
                .directory_mutation_receipt(&q, &context())
                .unwrap()
                .is_some(),
            point.ends_with("committed")
        );
        let receipt = f.store.apply_directory_mutation(&q, &context()).unwrap();
        assert_eq!(receipt.entries[0].basic_info().write_time, 101);
        f.oracle();
    }
}

#[test]
fn directory_receipt_insertion_cannot_substitute_workspace_branch_or_view_scope_before_ack() {
    for sql in [
        "UPDATE config SET value=CAST(value AS INTEGER)+1 WHERE key='generation'",
        "UPDATE config SET value='unknown' WHERE key='active'",
        "UPDATE config SET value='00000000-0000-4000-8000-000000000001' WHERE key='content_save_workspace'",
    ] {
        let mut f = Fixture::new();
        let id = f.store.create("dir", "directory").unwrap().id;
        let q = capture(&f, &id, update(101));
        f.store.db.execute_batch(&format!("CREATE TEMP TRIGGER redirect_scope AFTER INSERT ON content_save_namespace_changes BEGIN {sql}; END;")).unwrap();
        assert!(
            f.store.apply_directory_mutation(&q, &context()).is_err(),
            "{sql}"
        );
        assert_eq!(
            f.store.directory_snapshot(&id, &context()).unwrap(),
            q.before
        );
        assert!(
            f.store
                .directory_mutation_receipt(&q, &context())
                .unwrap()
                .is_none()
        );
        f.store
            .db
            .execute_batch("DROP TRIGGER redirect_scope")
            .unwrap();
        f.store.apply_directory_mutation(&q, &context()).unwrap();
        f.oracle();
    }
}

#[test]
fn file_and_directory_dispositions_share_one_bounded_capacity_and_bounded_pages() {
    let mut f = Fixture::new();
    let id = f.store.create("dir", "directory").unwrap().id;
    let snapshot = f.store.directory_snapshot(&id, &context()).unwrap();
    f.store
        .stage_content_delete(Uuid::new_v4(), f.saved.intent.operation, &context())
        .unwrap();
    let mut requests = vec![];
    for _ in 0..63 {
        let request = Uuid::new_v4();
        requests.push(request);
        f.store
            .stage_directory_delete(request, &snapshot, &context())
            .unwrap();
    }
    error(
        f.store
            .stage_directory_delete(Uuid::new_v4(), &snapshot, &context()),
        "CONTENT_RESOURCE_LIMIT",
    );
    error(
        f.store
            .stage_content_delete(Uuid::new_v4(), f.saved.intent.operation, &context()),
        "CONTENT_RESOURCE_LIMIT",
    );
    error(
        f.store.directory_delete_intents(None, 17, &context()),
        "CONTENT_SAVE_CAPTURE_LIMIT",
    );
    let mut after = None;
    let mut total = 0;
    loop {
        let page = f
            .store
            .directory_delete_intents(after, 16, &context())
            .unwrap();
        if page.is_empty() {
            break;
        }
        assert!(page.len() <= 16);
        total += page.len();
        after = Some(page.last().unwrap().request);
    }
    assert_eq!(total, 63);
    f.store
        .cancel_directory_delete(requests[0], &id, &context())
        .unwrap();
    f.store
        .stage_content_delete(Uuid::new_v4(), f.saved.intent.operation, &context())
        .unwrap();
    error(
        f.store
            .stage_directory_delete(Uuid::new_v4(), &snapshot, &context()),
        "CONTENT_RESOURCE_LIMIT",
    );
    f.oracle();
}

#[test]
fn another_handle_cannot_stage_over_a_foreign_change_to_an_existing_directory_disposition() {
    let mut f = Fixture::new();
    let id = f.store.create("dir", "directory").unwrap().id;
    let snapshot = f.store.directory_snapshot(&id, &context()).unwrap();
    let first = Uuid::new_v4();
    f.store
        .stage_directory_delete(first, &snapshot, &context())
        .unwrap();
    f.store.rename(&id, "foreign", false).unwrap();
    let fresh = f.store.directory_snapshot(&id, &context()).unwrap();
    error(
        f.store
            .stage_directory_delete(Uuid::new_v4(), &fresh, &context()),
        "STALE_CONTENT_DIRECTORY",
    );
    assert_eq!(
        f.store
            .directory_delete_intents(None, 16, &context())
            .unwrap()
            .len(),
        1
    );
    f.store
        .cancel_directory_delete(first, &id, &context())
        .unwrap();
    f.store
        .stage_directory_delete(Uuid::new_v4(), &fresh, &context())
        .unwrap();
    f.oracle();
}

#[test]
#[ignore = "actual exit helper for directory mutation/disposition atomicity"]
fn directory_mutation_exit_child() {
    let root = std::path::PathBuf::from(std::env::var_os("TKFS_DIRECTORY_CHANGE_ROOT").unwrap());
    let request: DirectoryMutationRequest =
        serde_json::from_str(&std::env::var("TKFS_DIRECTORY_CHANGE_REQUEST").unwrap()).unwrap();
    let point = std::env::var("TKFS_DIRECTORY_CHANGE_POINT").unwrap();
    let mut store = Store::open_existing(&root).unwrap();
    fault::configure(Some(FaultConfig::new(&point, FaultAction::Exit)));
    store
        .apply_directory_mutation(&request, &context())
        .unwrap();
    panic!("expected directory mutation exit boundary");
}
#[test]
fn process_exit_directory_removal_preserves_or_consumes_all_pending_requests_atomically() {
    for point in [
        "content.directory.change_before_event",
        "content.directory.change_written",
        "content.directory.change_committed",
    ] {
        let mut f = Fixture::new();
        let entity = f.store.create("dir", "directory").unwrap().id;
        let mut q = capture(&f, &entity, DirectoryMutation::Remove { source: None });
        let request = Uuid::new_v4();
        f.store
            .stage_directory_delete(request, &q.before, &context())
            .unwrap();
        q.mutation = request;
        f.store
            .stage_directory_delete(Uuid::new_v4(), &q.before, &context())
            .unwrap();
        let root = f.store.root.clone();
        drop(f.store);
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "directory_changes::directory_mutation_exit_child",
                "--ignored",
                "--nocapture",
            ])
            .env("TKFS_DIRECTORY_CHANGE_ROOT", &root)
            .env(
                "TKFS_DIRECTORY_CHANGE_REQUEST",
                serde_json::to_string(&q).unwrap(),
            )
            .env("TKFS_DIRECTORY_CHANGE_POINT", point)
            .output()
            .unwrap();
        assert_eq!(
            result.status.code(),
            Some(86),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let mut store = Store::open_existing(&root).unwrap();
        let committed = point.ends_with("committed");
        assert_eq!(store.entry(&entity).unwrap().alive, !committed);
        assert_eq!(
            store
                .directory_delete_intents(None, 16, &context())
                .unwrap()
                .len(),
            if committed { 0 } else { 2 }
        );
        let receipt = store.apply_directory_mutation(&q, &context()).unwrap();
        assert!(!receipt.entries[0].alive);
        assert_eq!(
            store.apply_directory_mutation(&q, &context()).unwrap(),
            receipt
        );
        assert!(
            store
                .directory_delete_intents(None, 16, &context())
                .unwrap()
                .is_empty()
        );
        assert!(store.verify_derived_state().unwrap().is_empty());
    }
}
