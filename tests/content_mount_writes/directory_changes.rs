use super::*;
use tkfs::core::{
    Entry,
    content_saves::{ContentBasicUpdate, NamespaceTarget},
};

fn create(f: &Fixture, path: &str) -> (u64, Entry) {
    content_writes::create_directory(
        &f.shared,
        path,
        content_writes::CreateOptions::default(),
        wait(),
    )
    .unwrap()
    .unwrap()
}
fn target(f: &Fixture, path: &str) -> NamespaceTarget {
    let e = tkfs::runtime::lock_engine(&f.shared);
    NamespaceTarget {
        path: path.into(),
        parent: e
            .store
            .lookup(&tkfs::core::split_path(path).unwrap().0)
            .unwrap()
            .id,
    }
}
fn basic(f: &Fixture, h: u64, time: u64) {
    assert!(
        content_writes::update_basic(
            &f.shared,
            h,
            ContentBasicUpdate {
                write_time: Some(time),
                ..Default::default()
            },
            wait()
        )
        .unwrap()
    );
}
fn pending(f: &Fixture) -> usize {
    tkfs::runtime::lock_engine(&f.shared)
        .store
        .directory_delete_intents(None, 16, &context())
        .unwrap()
        .len()
}

#[test]
fn native_directory_metadata_rename_remove_and_retained_handles_have_no_byte_owners() {
    let f = Fixture::new();
    let (h, entry) = create(&f, "dir");
    let other = tkfs::runtime::lock_engine(&f.shared)
        .open("dir", None, false)
        .unwrap()
        .0;
    basic(&f, h, 101);
    assert_eq!(
        tkfs::runtime::lock_engine(&f.shared)
            .handle_entry(other)
            .unwrap()
            .basic_info()
            .write_time,
        101
    );
    content_writes::rename_directory(
        &f.shared,
        &entry.id,
        Some(h),
        None,
        &target(&f, "moved"),
        false,
        wait(),
    )
    .unwrap();
    assert_eq!(
        tkfs::runtime::lock_engine(&f.shared)
            .handle_entry(other)
            .unwrap()
            .name,
        "moved"
    );
    content_writes::remove_directory(&f.shared, &entry.id, &target(&f, "moved"), wait()).unwrap();
    let (new, newentry) = create(&f, "moved");
    basic(&f, h, 202);
    let mut e = tkfs::runtime::lock_engine(&f.shared);
    let old = e.handle_entry(other).unwrap();
    assert!(!old.alive);
    assert_eq!(old.basic_info().write_time, 202);
    assert_eq!(e.store.lookup("moved").unwrap().id, newentry.id);
    assert_eq!(
        e.store
            .db
            .query_row("SELECT count(*) FROM content_save_operations", [], |r| r
                .get::<_, u64>(0))
            .unwrap(),
        0
    );
    assert!(e.directory_entry(h, 0, true).unwrap().is_some());
    drop(e);
    f.close(h);
    f.close(other);
    f.close(new);
    assert_eq!(f.publications(), 0);
    f.oracle();
    f.stop();
}

#[test]
fn directory_replacement_retires_only_destination_handles_and_consumes_their_pending_delete() {
    let f = Fixture::new();
    let (src, source) = create(&f, "source");
    let (dst, destination) = create(&f, "destination");
    content_writes::set_delete_disposition(&f.shared, dst, true, wait()).unwrap();
    assert_eq!(pending(&f), 1);
    content_writes::rename_directory(
        &f.shared,
        &source.id,
        Some(src),
        None,
        &target(&f, "destination"),
        true,
        wait(),
    )
    .unwrap();
    assert_eq!(pending(&f), 0);
    assert!(
        content_writes::complete_delete_disposition(&f.shared, dst, "destination", wait()).unwrap()
    );
    basic(&f, dst, 101);
    let e = tkfs::runtime::lock_engine(&f.shared);
    assert_eq!(e.store.lookup("destination").unwrap().id, source.id);
    assert!(!e.handle_entry(dst).unwrap().alive);
    assert_eq!(e.handle_entry(dst).unwrap().id, destination.id);
    assert!(e.handle_entry(src).unwrap().alive);
    drop(e);
    f.close(src);
    f.close(dst);
    f.oracle();
    f.stop();
}

#[test]
fn old_directory_handles_cannot_mutate_a_restored_existence_head() {
    let f = Fixture::new();
    let (old, entry) = create(&f, "dir");
    content_writes::remove_directory(&f.shared, &entry.id, &target(&f, "dir"), wait()).unwrap();
    {
        let mut e = tkfs::runtime::lock_engine(&f.shared);
        let change = e
            .store
            .current_change(&entry.id, "alive", serde_json::json!(true))
            .unwrap();
        let event = e
            .store
            .event(&e.store.branch(&e.store.active).unwrap(), vec![change]);
        let id = event.id.clone();
        e.store.commit(event, true).unwrap();
        let delta = e.activation_delta(&[id], None).unwrap();
        e.after_activation(&delta);
        assert!(!e.handle_entry(old).unwrap().alive);
    }
    let fresh = tkfs::runtime::lock_engine(&f.shared)
        .open("dir", None, false)
        .unwrap()
        .0;
    basic(&f, fresh, 303);
    error(
        content_writes::update_basic(
            &f.shared,
            old,
            ContentBasicUpdate {
                write_time: Some(101),
                ..Default::default()
            },
            wait(),
        ),
        "STALE_CONTENT_DIRECTORY",
    );
    error(
        content_writes::set_delete_disposition(&f.shared, old, true, wait()),
        "STALE_CONTENT_DIRECTORY",
    );
    error(
        content_writes::rename_directory(
            &f.shared,
            &entry.id,
            Some(old),
            None,
            &target(&f, "wrong"),
            false,
            wait(),
        ),
        "STALE_CONTENT_DIRECTORY",
    );
    let mut e = tkfs::runtime::lock_engine(&f.shared);
    assert_eq!(e.handle_entry(fresh).unwrap().basic_info().write_time, 303);
    error(e.directory_entry(old, 0, true), "STALE_VIEW");
    drop(e);
    f.close(old);
    f.close(fresh);
    f.oracle();
    f.stop();
}

#[test]
fn directory_dispositions_are_per_handle_block_new_children_and_follow_owned_renames() {
    let f = Fixture::new();
    let (a, entry) = create(&f, "dir");
    let b = tkfs::runtime::lock_engine(&f.shared)
        .open("dir", None, false)
        .unwrap()
        .0;
    content_writes::set_delete_disposition(&f.shared, a, true, wait()).unwrap();
    content_writes::set_delete_disposition(&f.shared, b, true, wait()).unwrap();
    assert_eq!(pending(&f), 2);
    error(
        content_writes::create_directory(
            &f.shared,
            "dir/new",
            content_writes::CreateOptions::default(),
            wait(),
        ),
        "ACCESS_DENIED",
    );
    error(
        content_writes::create(
            &f.shared,
            "dir/new",
            content_writes::CreateOptions::default(),
            wait(),
        ),
        "ACCESS_DENIED",
    );
    basic(&f, a, 101);
    content_writes::rename_directory(
        &f.shared,
        &entry.id,
        Some(a),
        None,
        &target(&f, "moved"),
        false,
        wait(),
    )
    .unwrap();
    content_writes::set_delete_disposition(&f.shared, a, false, wait()).unwrap();
    assert_eq!(pending(&f), 1);
    content_writes::complete_delete_disposition(&f.shared, b, "dir", wait()).unwrap();
    assert_eq!(pending(&f), 0);
    assert!(
        tkfs::runtime::lock_engine(&f.shared)
            .store
            .lookup("moved")
            .is_err()
    );
    f.close(a);
    f.close(b);
    f.oracle();
    f.stop();
}

#[test]
fn directory_mutations_wait_for_full_wal_outside_engine_and_reserve_the_target() {
    let f = Fixture::new();
    let (h, entry) = create(&f, "dir");
    let target = target(&f, "moved");
    let reset = arm("content.directory.change_written", FaultAction::Hang, 500);
    let shared = f.shared.clone();
    let thread = std::thread::spawn(move || {
        content_writes::rename_directory(&shared, &entry.id, Some(h), None, &target, false, wait())
    });
    fired();
    let began = Instant::now();
    let e = tkfs::runtime::try_lock_engine(&f.shared).expect("directory FULL write held Engine");
    assert!(began.elapsed() < Duration::from_millis(50));
    drop(e);
    error(
        content_writes::create_directory(
            &f.shared,
            "moved",
            content_writes::CreateOptions::default(),
            wait(),
        ),
        "CONTENT_RESOURCE_LIMIT",
    );
    thread.join().unwrap().unwrap();
    drop(reset);
    f.close(h);
    f.oracle();
    f.stop();
}

#[test]
fn accepted_directory_cleanup_failure_recovers_before_requests_are_accepted() {
    let f = Fixture::new();
    let (h, entry) = create(&f, "dir");
    content_writes::set_delete_disposition(&f.shared, h, true, wait()).unwrap();
    basic(&f, h, 101);
    content_writes::rename_directory(
        &f.shared,
        &entry.id,
        Some(h),
        None,
        &target(&f, "moved"),
        false,
        wait(),
    )
    .unwrap();
    let reset = arm("content.directory.change_written", FaultAction::Fail, 0);
    error(
        content_writes::complete_delete_disposition(&f.shared, h, "dir", wait()),
        "FAULT_INJECTED",
    );
    drop(reset);
    assert_eq!(pending(&f), 1);
    f.stop();
    let Fixture {
        shared,
        writes,
        native,
        dir,
        ..
    } = f;
    drop(writes);
    drop(shared);
    drop(native);
    let shared = Arc::new(Mutex::new(Engine::new(
        Store::open_existing(dir.path()).unwrap(),
    )));
    let writes = NativeWrites::install(&shared, &context()).unwrap();
    let mut e = tkfs::runtime::lock_engine(&shared);
    assert!(!e.store.entry(&entry.id).unwrap().alive);
    assert!(e.store.lookup("moved").is_err());
    assert!(
        e.store
            .directory_delete_intents(None, 16, &context())
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        e.store
            .db
            .query_row("SELECT count(*) FROM content_save_operations", [], |r| r
                .get::<_, u64>(0))
            .unwrap(),
        0
    );
    assert!(e.store.verify_derived_state().unwrap().is_empty());
    drop(e);
    writes.shutdown(&context()).unwrap();
}

#[test]
fn lost_directory_disposition_and_cancellation_replies_have_exact_authority() {
    let f = Fixture::new();
    let (h, _) = create(&f, "dir");
    for (point, delete, accepted, count) in [
        ("content.directory.delete_written", true, false, 0),
        ("content.directory.delete_committed", true, true, 1),
        ("content.directory.cancel_written", false, false, 1),
        ("content.directory.cancel_committed", false, true, 0),
    ] {
        let reset = arm(point, FaultAction::Fail, 0);
        let result = content_writes::set_delete_disposition(&f.shared, h, delete, wait());
        assert_eq!(fault::fired(), 1);
        drop(reset);
        assert_eq!(result.is_ok(), accepted, "{point}: {result:?}");
        assert_eq!(pending(&f), count);
    }
    f.close(h);
    f.oracle();
    f.stop();
}

#[test]
fn committed_directory_removals_reconcile_retained_handles_after_observation_deadlines() {
    for action in ["remove", "cleanup", "replace"] {
        let f = Fixture::new();
        let (old, entry) = create(&f, "dir");
        let other = tkfs::runtime::lock_engine(&f.shared)
            .open("dir", None, false)
            .unwrap()
            .0;
        let destination = target(&f, "dir");
        let source = (action == "replace").then(|| create(&f, "source"));
        if action == "cleanup" {
            content_writes::set_delete_disposition(&f.shared, old, true, wait()).unwrap();
        }
        let reset = arm("content.directory.change_committed", FaultAction::Hang, 700);
        let short = ContentWait::fixed(Duration::from_millis(300));
        let result = match action {
            "remove" => content_writes::remove_directory(&f.shared, &entry.id, &destination, short),
            "cleanup" => content_writes::complete_delete_disposition(&f.shared, old, "dir", short),
            _ => {
                let (handle, source) = source.as_ref().unwrap();
                content_writes::rename_directory(
                    &f.shared,
                    &source.id,
                    Some(*handle),
                    None,
                    &destination,
                    true,
                    short,
                )
            }
        };
        assert_eq!(fault::fired(), 1, "{action}: {result:?}");
        drop(reset);
        error(result, "CONTENT_DEADLINE");
        let committed = {
            let e = tkfs::runtime::lock_engine(&f.shared);
            let committed = e.store.entry(&entry.id).unwrap();
            assert!(!committed.alive);
            assert_eq!(
                e.handle_entry(old).unwrap().revisions.get("alive"),
                entry.revisions.get("alive")
            );
            committed
        };
        let replacement = if action != "replace" {
            Some(create(&f, "dir"))
        } else {
            None
        };
        {
            let mut e = tkfs::runtime::lock_engine(&f.shared);
            for handle in [old, other] {
                let (dot, _) = e.directory_entry(handle, 0, true).unwrap().unwrap();
                assert_eq!(dot.name, ".");
                assert!(!dot.alive);
            }
        }
        if action == "cleanup" {
            // The committed deletion consumed the disposition; retry must only observe it.
            assert_eq!(pending(&f), 0);
            let count_events = || {
                tkfs::runtime::lock_engine(&f.shared)
                    .store
                    .db
                    .query_row("SELECT count(*) FROM events", [], |row| {
                        row.get::<_, u64>(0)
                    })
                    .unwrap()
            };
            let before = count_events();
            content_writes::complete_delete_disposition(&f.shared, old, "dir", wait()).unwrap();
            assert_eq!(count_events(), before);
        }
        basic(&f, old, 101);
        basic(&f, other, 202);
        {
            let e = tkfs::runtime::lock_engine(&f.shared);
            for handle in [old, other] {
                let retained = e.handle_entry(handle).unwrap();
                assert!(!retained.alive);
                assert_eq!(
                    retained.revisions.get("alive"),
                    committed.revisions.get("alive")
                );
                assert_eq!(retained.basic_info().write_time, 202);
            }
            let current = e.store.lookup("dir").unwrap();
            assert_ne!(current.id, entry.id);
            assert_ne!(current.basic_info().write_time, 202);
        }
        f.close(old);
        f.close(other);
        if let Some((handle, _)) = source {
            f.close(handle);
        }
        if let Some((handle, _)) = replacement {
            f.close(handle);
        }
        assert_eq!(f.publications(), 0);
        f.oracle();
        f.stop();
    }
}

#[test]
fn an_unobserved_directory_removal_does_not_adopt_a_later_restoration() {
    let f = Fixture::new();
    let (old, entry) = create(&f, "dir");
    let destination = target(&f, "dir");
    let reset = arm("content.directory.change_committed", FaultAction::Hang, 700);
    let result = content_writes::remove_directory(
        &f.shared,
        &entry.id,
        &destination,
        ContentWait::fixed(Duration::from_millis(300)),
    );
    assert_eq!(fault::fired(), 1);
    drop(reset);
    error(result, "CONTENT_DEADLINE");
    {
        let mut e = tkfs::runtime::lock_engine(&f.shared);
        let change = e
            .store
            .current_change(&entry.id, "alive", serde_json::json!(true))
            .unwrap();
        let event = e
            .store
            .event(&e.store.branch(&e.store.active).unwrap(), vec![change]);
        let id = event.id.clone();
        e.store.commit(event, true).unwrap();
        let delta = e.activation_delta(&[id], None).unwrap();
        e.after_activation(&delta);
    }
    let fresh = tkfs::runtime::lock_engine(&f.shared)
        .open("dir", None, false)
        .unwrap()
        .0;
    basic(&f, fresh, 303);
    error(
        content_writes::update_basic(
            &f.shared,
            old,
            ContentBasicUpdate {
                write_time: Some(101),
                ..Default::default()
            },
            wait(),
        ),
        "STALE_CONTENT_DIRECTORY",
    );
    error(
        content_writes::set_delete_disposition(&f.shared, old, true, wait()),
        "STALE_CONTENT_DIRECTORY",
    );
    assert_eq!(
        tkfs::runtime::lock_engine(&f.shared)
            .store
            .entry(&entry.id)
            .unwrap()
            .basic_info()
            .write_time,
        303
    );
    error(
        tkfs::runtime::lock_engine(&f.shared).directory_entry(old, 0, true),
        "STALE_VIEW",
    );
    f.close(old);
    f.close(fresh);
    f.oracle();
    f.stop();
}
