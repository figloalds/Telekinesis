use super::*;
use tkfs::core::content_saves::NamespaceTarget;

fn target(f: &Fixture) -> NamespaceTarget {
    NamespaceTarget {
        path: "file".into(),
        parent: f.store.entry(&f.entity).unwrap().parent,
    }
}
fn destination(f: &Fixture) -> NamespaceDestination {
    NamespaceDestination {
        intent: f.saved.intent.clone(),
        checkpoint: Some(f.saved.clone()),
    }
}
fn unlink(
    f: &mut Fixture,
    mutation: Uuid,
) -> anyhow::Result<tkfs::core::content_saves::ContentMetadataReceipt> {
    f.store
        .apply_content_checkpoint_unlink(destination(f), mutation, &target(f), &context())
}

#[test]
fn unlink_keeps_saved_bytes_and_later_saves_dead_when_the_name_is_reused() {
    let mut f = Fixture::new();
    let old = f.store.entry(&f.entity).unwrap().content_ref;
    let target = target(&f);
    let destination = destination(&f);
    let mutation = Uuid::new_v4();
    let receipt = unlink(&mut f, mutation).unwrap();
    assert_eq!(receipt.operation, f.saved.intent.operation);
    assert!(f.store.lookup("file").is_err());
    assert!(!f.store.entry(&f.entity).unwrap().alive);
    assert_eq!(f.store.entry(&f.entity).unwrap().content_ref, old);
    assert_eq!(f.publications(), 0);
    let handoff = f
        .store
        .content_namespace_continuation(&f.saved.intent, &context())
        .unwrap()
        .unwrap();
    let session = f
        .session
        .adopt_namespace_continuation(&handoff, &context())
        .unwrap();
    let mut bytes = [0; 7];
    session.read_range(0, &mut bytes, &context()).unwrap();
    assert_eq!(&bytes, b"private");
    let replacement = f.store.create("file", "file").unwrap().id;
    session.write_at(0, b"unnamed", &context()).unwrap();
    let saved = session.checkpoint(&context()).unwrap().unwrap();
    f.store
        .select_content_checkpoint(&saved, &context())
        .unwrap();
    let prepared = session.prepare(&f.local, &saved, &context()).unwrap();
    f.store.publish_content_save(&prepared, &context()).unwrap();
    assert_eq!(f.store.lookup("file").unwrap().id, replacement);
    assert!(!f.store.entry(&f.entity).unwrap().alive);
    assert_eq!(f.publications(), 1);
    assert_eq!(
        f.store
            .content_checkpoint_unlink_receipt(destination.clone(), mutation, &target, &context())
            .unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(
        f.store
            .apply_content_checkpoint_unlink(destination, mutation, &target, &context())
            .unwrap(),
        receipt
    );
    f.oracle();
}

#[test]
fn unlink_faults_roll_back_or_have_one_exact_receipt_without_republishing_bytes() {
    for (point, committed) in [
        ("content.save.before_unlink", false),
        ("content.save.unlink_written", false),
        ("content.save.unlink_committed", true),
    ] {
        let mut f = Fixture::new();
        let destination = destination(&f);
        let target = target(&f);
        let mutation = Uuid::new_v4();
        let mut config = FaultConfig::new(point, FaultAction::Fail);
        config.count = Some(1);
        fault::configure(Some(config));
        let result = unlink(&mut f, mutation);
        let fired = fault::fired();
        fault::configure(None);
        assert_eq!(fired, 1, "{point}");
        error(result, "FAULT_INJECTED");
        assert_eq!(!f.store.entry(&f.entity).unwrap().alive, committed);
        assert_eq!(f.publications(), 0);
        let receipt = f
            .store
            .content_checkpoint_unlink_receipt(destination.clone(), mutation, &target, &context())
            .unwrap();
        assert_eq!(receipt.is_some(), committed);
        assert_eq!(
            f.store
                .content_namespace_continuation(&f.saved.intent, &context())
                .unwrap()
                .is_some(),
            committed
        );
        let receipt = f
            .store
            .apply_content_checkpoint_unlink(destination.clone(), mutation, &target, &context())
            .unwrap();
        assert_eq!(
            f.store
                .apply_content_checkpoint_unlink(destination, mutation, &target, &context())
                .unwrap(),
            receipt
        );
        let count: u64 = f
            .store
            .db
            .query_row(
                "SELECT count(*) FROM content_save_namespace_changes",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
        f.oracle();
    }
}

#[test]
fn unlink_binds_the_observed_name_parent_checkpoint_and_mutation() {
    let mut f = Fixture::new();
    let mutation = Uuid::new_v4();
    let before = target(&f);
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
    error(
        f.store
            .apply_content_checkpoint_unlink(destination(&f), mutation, &before, &context()),
        "STALE_CONTENT_DESTINATION",
    );
    assert_eq!(f.store.lookup("file").unwrap().id, replacement);
    let mut target = NamespaceTarget {
        path: "moved".into(),
        parent: before.parent,
    };
    target.parent = "another-parent".into();
    error(
        f.store
            .apply_content_checkpoint_unlink(destination(&f), mutation, &target, &context()),
        "STALE_CONTENT_DESTINATION",
    );
    target.parent = f.store.entry(&f.entity).unwrap().parent;
    f.store
        .apply_content_checkpoint_unlink(destination(&f), mutation, &target, &context())
        .unwrap();
    let mut changed = destination(&f);
    changed.checkpoint = None;
    error(
        f.store
            .content_checkpoint_unlink_receipt(changed, mutation, &target, &context()),
        "CONTENT_METADATA_MUTATION_REUSED",
    );
    target.path = "file".into();
    error(
        f.store
            .content_checkpoint_unlink_receipt(destination(&f), mutation, &target, &context()),
        "CONTENT_METADATA_MUTATION_REUSED",
    );
    assert_eq!(f.store.lookup("file").unwrap().id, replacement);
    f.oracle();
}

#[test]
fn corrupt_unlink_evidence_cannot_authorize_a_private_namespace_handoff() {
    let mut f = Fixture::new();
    let mutation = Uuid::new_v4();
    unlink(&mut f, mutation).unwrap();
    f.store
        .db
        .execute(
            "UPDATE content_save_namespace_changes SET record=? WHERE mutation=?",
            rusqlite::params![b"{}".as_slice(), mutation.to_string()],
        )
        .unwrap();
    error(
        f.store
            .content_namespace_continuation(&f.saved.intent, &context()),
        "CORRUPT_CONTENT_SAVE",
    );
    assert_eq!(f.publications(), 0);
    assert!(!f.store.entry(&f.entity).unwrap().alive);
}

#[test]
#[ignore = "actual exit helper for unlink recovery"]
fn unlink_crash_child() {
    let root = std::path::PathBuf::from(std::env::var_os("TKFS_UNLINK_CRASH_ROOT").unwrap());
    let operation = Uuid::parse_str(&std::env::var("TKFS_UNLINK_OPERATION").unwrap()).unwrap();
    let mutation = Uuid::parse_str(&std::env::var("TKFS_UNLINK_MUTATION").unwrap()).unwrap();
    let mut store = Store::open_existing(&root).unwrap();
    let local = Arc::new(LocalContent::open(&root).unwrap());
    let journal = WriteJournal::open(&root).unwrap();
    let session = journal.resume(operation, local, &context()).unwrap();
    let checkpoint = session.checkpoint(&context()).unwrap().unwrap();
    let entry = store.entry(&checkpoint.intent.base.entity).unwrap();
    fault::configure(Some(FaultConfig::new(
        &std::env::var("TKFS_UNLINK_POINT").unwrap(),
        FaultAction::Exit,
    )));
    store
        .apply_content_checkpoint_unlink(
            NamespaceDestination {
                intent: checkpoint.intent.clone(),
                checkpoint: Some(checkpoint),
            },
            mutation,
            &NamespaceTarget {
                path: "file".into(),
                parent: entry.parent,
            },
            &context(),
        )
        .unwrap();
    panic!("unlink did not reach the requested exit boundary");
}

#[test]
fn process_exit_during_unlink_recovers_the_tombstone_and_saved_bytes_exactly_once() {
    for point in [
        "content.save.before_unlink",
        "content.save.unlink_written",
        "content.save.unlink_committed",
    ] {
        let f = Fixture::new();
        let target = target(&f);
        let destination = destination(&f);
        let mutation = Uuid::new_v4();
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
        drop(session);
        drop(_journal);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "unlink::unlink_crash_child",
                "--ignored",
                "--nocapture",
            ])
            .env("TKFS_UNLINK_CRASH_ROOT", _dir.path())
            .env("TKFS_UNLINK_OPERATION", saved.intent.operation.to_string())
            .env("TKFS_UNLINK_MUTATION", mutation.to_string())
            .env("TKFS_UNLINK_POINT", point)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let mut store = Store::open_existing(_dir.path()).unwrap();
        let committed = point == "content.save.unlink_committed";
        assert_eq!(!store.entry(&entity).unwrap().alive, committed);
        assert_eq!(
            store
                .content_checkpoint_unlink_receipt(
                    destination.clone(),
                    mutation,
                    &target,
                    &context()
                )
                .unwrap()
                .is_some(),
            committed
        );
        let local = Arc::new(LocalContent::open(_dir.path()).unwrap());
        let journal = WriteJournal::open(_dir.path()).unwrap();
        let mut session = journal
            .resume(saved.intent.operation, local.clone(), &context())
            .unwrap();
        if !committed {
            store
                .apply_content_checkpoint_unlink(destination, mutation, &target, &context())
                .unwrap();
        }
        let handoff = store
            .content_namespace_continuation(&saved.intent, &context())
            .unwrap()
            .unwrap();
        session = session
            .adopt_namespace_continuation(&handoff, &context())
            .unwrap();
        let mut bytes = [0; 7];
        session.read_range(0, &mut bytes, &context()).unwrap();
        assert_eq!(&bytes, b"private");
        let checkpoint = session.checkpoint(&context()).unwrap().unwrap();
        store
            .select_content_checkpoint(&checkpoint, &context())
            .unwrap();
        let prepared = session.prepare(&local, &checkpoint, &context()).unwrap();
        store.publish_content_save(&prepared, &context()).unwrap();
        assert!(store.lookup("file").is_err());
        assert!(!store.entry(&entity).unwrap().alive);
        assert!(store.verify_derived_state().unwrap().is_empty());
        let count: u64 = store
            .db
            .query_row(
                "SELECT count(*) FROM content_save_namespace_changes",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }
}
