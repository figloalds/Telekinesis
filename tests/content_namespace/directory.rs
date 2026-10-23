use super::*;
use tkfs::core::content_saves::{DirectoryCreateRequest, NamespaceTarget};

pub(super) fn request(f: &Fixture, path: &str) -> DirectoryCreateRequest {
    let mut basic = f.basic();
    basic.attributes = 0x10;
    basic.posix_mode = Some(0o750);
    DirectoryCreateRequest {
        mutation: Uuid::new_v4(),
        entity: Uuid::new_v4(),
        workspace: f.saved.intent.base.workspace.clone(),
        branch: f.store.active.clone(),
        view_generation: f.store.generation,
        target: NamespaceTarget {
            path: path.into(),
            parent: f
                .store
                .lookup(&tkfs::core::split_path(path).unwrap().0)
                .unwrap()
                .id,
        },
        basic,
    }
}
#[test]
fn directory_creation_has_exact_metadata_receipts_without_any_byte_owner_or_publication() {
    let mut f = Fixture::new();
    let req = request(&f, "created");
    let before: u64 = f
        .store
        .db
        .query_row("SELECT count(*) FROM content_save_operations", [], |r| {
            r.get(0)
        })
        .unwrap();
    let receipt = f.store.apply_directory_create(&req, &context()).unwrap();
    assert_eq!(receipt.entity, req.entity.to_string());
    let entry = f.store.validate_directory_create(&req, &context()).unwrap();
    assert_eq!(entry.kind, "directory");
    assert_eq!(entry.basic_info(), req.basic);
    assert!(entry.content.is_none() && entry.content_ref.is_none());
    assert_eq!(
        f.store
            .db
            .query_row("SELECT count(*) FROM content_save_operations", [], |r| r
                .get::<_, u64>(0))
            .unwrap(),
        before
    );
    assert_eq!(f.publications(), 0);
    f.store.rename(&entry.id, "moved", false).unwrap();
    assert!(f.store.validate_directory_create(&req, &context()).is_err());
    f.store.delete(&entry.id).unwrap();
    let replacement = f.store.create("created", "directory").unwrap().id;
    assert_eq!(
        f.store.apply_directory_create(&req, &context()).unwrap(),
        receipt
    );
    assert_eq!(
        f.store.directory_create_receipt(&req, &context()).unwrap(),
        Some(receipt)
    );
    assert_eq!(f.store.lookup("created").unwrap().id, replacement);
    let mut changed = req.clone();
    changed.basic.access_time += 1;
    error(
        f.store.apply_directory_create(&changed, &context()),
        "CONTENT_METADATA_MUTATION_REUSED",
    );
    f.oracle();
}
#[test]
fn directory_creation_binds_workspace_view_parent_permissions_and_reserved_identity() {
    let mut f = Fixture::new();
    let req = request(&f, "created");
    let mut wrong = req.clone();
    wrong.workspace.workspace = Uuid::new_v4();
    error(
        f.store.apply_directory_create(&wrong, &context()),
        "STALE_CONTENT_WORKSPACE",
    );
    wrong = req.clone();
    wrong.view_generation += 1;
    error(
        f.store.apply_directory_create(&wrong, &context()),
        "STALE_CONTENT_VIEW",
    );
    wrong = req.clone();
    wrong.target.parent = "other".into();
    error(
        f.store.apply_directory_create(&wrong, &context()),
        "STALE_CONTENT_DESTINATION",
    );
    wrong = req.clone();
    wrong.target.path = "file".into();
    error(
        f.store.apply_directory_create(&wrong, &context()),
        "NAME_COLLISION",
    );
    wrong = req.clone();
    wrong.mutation = f.saved.intent.operation;
    error(
        f.store.apply_directory_create(&wrong, &context()),
        "CONTENT_METADATA_MUTATION_REUSED",
    );
    wrong = req.clone();
    wrong.basic.attributes = 0x80;
    error(
        f.store.apply_directory_create(&wrong, &context()),
        "INVALID_ATTRIBUTES",
    );
    wrong = req.clone();
    wrong.entity = wrong.mutation;
    error(
        f.store.apply_directory_create(&wrong, &context()),
        "CONTENT_SAVE_INTENT_MISMATCH",
    );
    assert!(f.store.lookup("created").is_err());
    assert!(
        f.store
            .directory_create_receipt(&req, &context())
            .unwrap()
            .is_none()
    );
    f.store.apply_directory_create(&req, &context()).unwrap();
    f.oracle();
}
#[test]
fn directory_creation_rejects_silent_receipt_event_and_view_changes_before_ack() {
    for sql in [
        "CREATE TEMP TRIGGER sabotage BEFORE INSERT ON content_save_create_changes BEGIN SELECT RAISE(IGNORE); END;",
        "CREATE TEMP TRIGGER sabotage AFTER INSERT ON content_save_create_changes BEGIN DELETE FROM content_save_create_changes WHERE mutation=NEW.mutation; END;",
        "CREATE TEMP TRIGGER sabotage AFTER INSERT ON content_save_create_changes BEGIN DELETE FROM events WHERE id=NEW.mutation; END;",
        "CREATE TEMP TRIGGER sabotage AFTER INSERT ON content_save_create_changes BEGIN UPDATE config SET value=CAST(value AS INTEGER)+1 WHERE key='generation'; END;",
    ] {
        let mut f = Fixture::new();
        let req = request(&f, "created");
        f.store.db.execute_batch(sql).unwrap();
        assert!(
            f.store.apply_directory_create(&req, &context()).is_err(),
            "{sql}"
        );
        assert!(f.store.lookup("created").is_err());
        assert!(
            f.store
                .directory_create_receipt(&req, &context())
                .unwrap()
                .is_none()
        );
        f.store.db.execute_batch("DROP TRIGGER sabotage").unwrap();
        f.store.apply_directory_create(&req, &context()).unwrap();
        f.oracle();
    }
}
#[test]
fn directory_creation_faults_roll_back_or_leave_one_historical_receipt() {
    for point in [
        "content.directory.before_event",
        "content.directory.event_written",
        "content.directory.written",
        "content.directory.committed",
    ] {
        let mut f = Fixture::new();
        let req = request(&f, "created");
        let mut config = FaultConfig::new(point, FaultAction::Fail);
        config.count = Some(1);
        fault::configure(Some(config));
        let result = f.store.apply_directory_create(&req, &context());
        let hits = fault::fired();
        fault::configure(None);
        error(result, "FAULT_INJECTED");
        assert_eq!(hits, 1);
        let committed = point == "content.directory.committed";
        assert_eq!(f.store.lookup("created").is_ok(), committed);
        assert_eq!(
            f.store
                .directory_create_receipt(&req, &context())
                .unwrap()
                .is_some(),
            committed
        );
        let receipt = f.store.apply_directory_create(&req, &context()).unwrap();
        assert_eq!(
            f.store.apply_directory_create(&req, &context()).unwrap(),
            receipt
        );
        assert_eq!(f.publications(), 0);
        f.oracle();
    }
}
#[test]
#[ignore = "actual exit helper for directory creation"]
fn directory_creation_exit_child() {
    let root = std::path::PathBuf::from(std::env::var_os("TKFS_DIRECTORY_ROOT").unwrap());
    let req: DirectoryCreateRequest =
        serde_json::from_str(&std::env::var("TKFS_DIRECTORY_REQUEST").unwrap()).unwrap();
    let point = std::env::var("TKFS_DIRECTORY_POINT").unwrap();
    let mut store = Store::open_existing(&root).unwrap();
    fault::configure(Some(FaultConfig::new(&point, FaultAction::Exit)));
    store.apply_directory_create(&req, &context()).unwrap();
    panic!("directory creation did not exit at the requested boundary");
}
#[test]
fn process_exit_directory_creation_is_empty_or_committed_once_without_a_byte_journal() {
    for point in [
        "content.directory.before_event",
        "content.directory.written",
        "content.directory.committed",
    ] {
        let f = Fixture::new();
        let req = request(&f, "created");
        let Fixture {
            store,
            local,
            _journal,
            session,
            _dir,
            ..
        } = f;
        drop(store);
        drop(session);
        drop(_journal);
        drop(local);
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "directory::directory_creation_exit_child",
                "--ignored",
                "--nocapture",
            ])
            .env("TKFS_DIRECTORY_ROOT", _dir.path())
            .env(
                "TKFS_DIRECTORY_REQUEST",
                serde_json::to_string(&req).unwrap(),
            )
            .env("TKFS_DIRECTORY_POINT", point)
            .output()
            .unwrap();
        assert_eq!(
            result.status.code(),
            Some(86),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let mut store = Store::open_existing(_dir.path()).unwrap();
        let committed = point == "content.directory.committed";
        assert_eq!(store.lookup("created").is_ok(), committed);
        assert_eq!(
            store
                .directory_create_receipt(&req, &context())
                .unwrap()
                .is_some(),
            committed
        );
        store.apply_directory_create(&req, &context()).unwrap();
        let events = store.events().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|e| e.id == req.mutation.to_string())
                .count(),
            1
        );
        assert!(store.verify_derived_state().unwrap().is_empty());
    }
}
