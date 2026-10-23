use super::*;
use tkfs::runtime::content_writes::CreateOptions;

fn create(f: &Fixture, path: &str) -> (u64, tkfs::core::Entry) {
    content_writes::create(
        &f.shared,
        path,
        CreateOptions {
            writable: true,
            ..Default::default()
        },
        wait(),
    )
    .unwrap()
    .unwrap()
}
fn receipts(f: &Fixture) -> u64 {
    tkfs::runtime::lock_engine(&f.shared)
        .store
        .db
        .query_row(
            "SELECT count(*) FROM content_save_create_changes",
            [],
            |r| r.get(0),
        )
        .unwrap()
}
fn open(f: &Fixture, path: &str, writable: bool) -> u64 {
    let h = tkfs::runtime::lock_engine(&f.shared)
        .open(path, None, writable)
        .unwrap()
        .0;
    content_reads::prepare_open(&f.shared, h, wait()).unwrap();
    h
}
#[test]
fn native_create_uses_typed_empty_content_buffered_writes_and_a_durable_close() {
    let f = Fixture::new();
    let (h, entry) = create(&f, "created");
    assert!(entry.content.is_none());
    assert_eq!(
        entry.content_ref.unwrap().kind,
        tkfs::model::content::ContentKind::Direct
    );
    assert!(matches!(
        &tkfs::runtime::lock_engine(&f.shared)
            .session(h)
            .unwrap()
            .bytes,
        tkfs::staging::Staged::Cow(_)
    ));
    assert_eq!(
        tkfs::runtime::lock_engine(&f.shared)
            .store
            .lookup("created")
            .unwrap()
            .id,
        entry.id
    );
    let reset = arm("content.write.before_checkpoint", FaultAction::Fail, 0);
    content_writes::write(&f.shared, h, 0, b"created", false, false, wait()).unwrap();
    assert_eq!(fault::fired(), 0);
    let reader = open(&f, "created", false);
    assert_eq!(&f.bytes(reader, 7), b"created");
    f.close(reader);
    assert_eq!(fault::fired(), 0);
    assert_eq!(f.publications(), 0);
    drop(reset);
    f.close(h);
    assert_eq!(f.publications(), 1);
    assert_eq!(receipts(&f), 1);
    let reader = open(&f, "created", false);
    assert_eq!(&f.bytes(reader, 7), b"created");
    f.close(reader);
    f.oracle();
    f.stop();
}
#[test]
fn a_buffered_native_child_is_committed_and_keeps_its_directory_nonempty() {
    let f = Fixture::new();
    let (directory, dir) =
        content_writes::create_directory(&f.shared, "parent", Default::default(), wait())
            .unwrap()
            .unwrap();
    let (child, entry) = create(&f, "parent/child");
    content_writes::write(&f.shared, child, 0, b"private child", false, false, wait()).unwrap();
    {
        let e = tkfs::runtime::lock_engine(&f.shared);
        let committed = e.store.lookup("parent/child").unwrap();
        assert_eq!(committed.id, entry.id);
        assert_eq!(committed.content_ref.unwrap().total_len, 0);
        let children = e.children_visible(&dir.id).unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].id, entry.id);
        assert_eq!(e.visible_size(&children[0]).unwrap(), 13);
    }
    error(
        content_writes::remove_directory(
            &f.shared,
            &dir.id,
            &tkfs::core::content_saves::NamespaceTarget {
                path: "parent".into(),
                parent: dir.parent.clone(),
            },
            wait(),
        ),
        "DIRECTORY_NOT_EMPTY",
    );
    assert_eq!(f.bytes(child, 13), b"private child");
    assert_eq!(f.publications(), 0);
    f.close(child);
    content_writes::unlink(&f.shared, &entry.id, "parent/child", wait()).unwrap();
    content_writes::remove_directory(
        &f.shared,
        &dir.id,
        &tkfs::core::content_saves::NamespaceTarget {
            path: "parent".into(),
            parent: dir.parent,
        },
        wait(),
    )
    .unwrap();
    f.close(directory);
    f.oracle();
    f.stop();
}

#[test]
fn creation_initial_permissions_allow_the_creating_handle_but_fence_later_write_opens() {
    let f = Fixture::new();
    let (h, _) = content_writes::create(
        &f.shared,
        "readonly",
        CreateOptions {
            writable: true,
            attributes: Some(1),
            posix_mode: Some(0o444),
            expected_parent: None,
        },
        wait(),
    )
    .unwrap()
    .unwrap();
    content_writes::write(&f.shared, h, 0, b"created", false, false, wait()).unwrap();
    error(
        tkfs::runtime::lock_engine(&f.shared).open("readonly", None, true),
        "ACCESS_DENIED",
    );
    f.close(h);
    let reader = open(&f, "readonly", false);
    assert_eq!(&f.bytes(reader, 7), b"created");
    f.close(reader);
    let entry = tkfs::runtime::lock_engine(&f.shared)
        .store
        .lookup("readonly")
        .unwrap();
    assert_eq!(entry.basic_info().posix_mode, Some(0o444));
    assert_eq!(entry.basic_info().attributes & 1, 1);
    f.oracle();
    f.stop();
}
#[test]
fn native_create_failures_reconcile_only_committed_receipts_and_leave_empty_failed_opens_readable()
{
    for point in [
        "content.create.before_event",
        "content.create.event_written",
        "content.create.written",
        "content.create.committed",
        "content.create.private_created",
    ] {
        let f = Fixture::new();
        let reset = arm(point, FaultAction::Fail, 0);
        let result = content_writes::create(
            &f.shared,
            "created",
            CreateOptions {
                writable: true,
                ..Default::default()
            },
            wait(),
        );
        drop(reset);
        let committed = matches!(
            point,
            "content.create.committed" | "content.create.private_created"
        );
        assert_eq!(receipts(&f), u64::from(committed));
        if point == "content.create.committed" {
            let h = result.unwrap().unwrap().0;
            f.close(h);
        } else {
            error(result, "FAULT_INJECTED");
            assert!(tkfs::runtime::lock_engine(&f.shared).handles.is_empty());
            if committed {
                let h = open(&f, "created", true);
                assert!(f.bytes(h, 7).is_empty());
                f.close(h);
            } else {
                let (h, _) = create(&f, "created");
                f.close(h);
            }
        }
        assert_eq!(receipts(&f), 1);
        assert_eq!(f.publications(), 0);
        f.oracle();
        f.stop();
    }
}
#[test]
fn creation_reply_expiry_keeps_the_committed_name_without_a_leaked_handle_or_duplicate_retry() {
    let f = Fixture::new();
    let reset = arm("content.create.committed", FaultAction::Hang, 700);
    error(
        content_writes::create(
            &f.shared,
            "created",
            CreateOptions {
                writable: true,
                ..Default::default()
            },
            ContentWait::fixed(Duration::from_millis(200)),
        ),
        "CONTENT_DEADLINE",
    );
    drop(reset);
    assert!(tkfs::runtime::lock_engine(&f.shared).handles.is_empty());
    error(
        content_writes::create(&f.shared, "created", CreateOptions::default(), wait()),
        "NAME_COLLISION",
    );
    let h = open(&f, "created", true);
    assert!(f.bytes(h, 7).is_empty());
    content_writes::write(&f.shared, h, 0, b"created", false, false, wait()).unwrap();
    f.close(h);
    assert_eq!(receipts(&f), 1);
    assert_eq!(f.publications(), 1);
    f.oracle();
    f.stop();
}
#[test]
fn create_reserves_names_and_adapter_capacity_while_existing_reads_remain_available() {
    static SLOTS: tkfs::runtime::content::WaitSlots = tkfs::runtime::content::WaitSlots::new(1);
    let f = Fixture::new();
    let reader = f.open(false);
    let reset = arm("content.create.before_event", FaultAction::Hang, 600);
    let shared = f.shared.clone();
    let pending = std::thread::spawn(move || {
        content_writes::create(
            &shared,
            "created",
            CreateOptions {
                writable: true,
                ..Default::default()
            },
            ContentWait {
                slots: Some(&SLOTS),
                ..wait()
            },
        )
    });
    fired();
    let began = Instant::now();
    error(
        content_writes::create(
            &f.shared,
            "other",
            CreateOptions::default(),
            ContentWait {
                slots: Some(&SLOTS),
                ..wait()
            },
        ),
        "CONTENT_RESOURCE_LIMIT",
    );
    error(
        content_writes::create(&f.shared, "CREATED", CreateOptions::default(), wait()),
        "CONTENT_RESOURCE_LIMIT",
    );
    assert_eq!(&f.bytes(reader, 7), &f.data[..7]);
    assert!(began.elapsed() < Duration::from_millis(300));
    let (h, _) = pending.join().unwrap().unwrap().unwrap();
    drop(reset);
    f.close(h);
    f.close(reader);
    let (h, _) = create(&f, "other");
    f.close(h);
    assert_eq!(receipts(&f), 2);
    f.oracle();
    f.stop();
}
#[test]
fn concurrent_native_creators_cannot_acknowledge_two_entities_for_one_folded_name() {
    let f = Fixture::new();
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let creators: Vec<_> = ["created", "CREATED"]
        .into_iter()
        .map(|path| {
            let shared = f.shared.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                content_writes::create(
                    &shared,
                    path,
                    CreateOptions {
                        writable: true,
                        ..Default::default()
                    },
                    wait(),
                )
            })
        })
        .collect();
    barrier.wait();
    let mut successes = 0;
    for creator in creators {
        match creator.join().unwrap() {
            Ok(Some((h, _))) => {
                successes += 1;
                f.close(h);
            }
            Err(error) => assert!(matches!(
                tkfs::model::errors::code_of(&error).as_str(),
                "NAME_COLLISION" | "CONTENT_RESOURCE_LIMIT"
            )),
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(successes, 1);
    assert_eq!(receipts(&f), 1);
    f.oracle();
    f.stop();
}
#[test]
fn a_creator_cannot_install_an_owner_fenced_during_private_preparation() {
    for fenced in [false, true] {
        let f = Fixture::new();
        let reset = arm("content.create.private_created", FaultAction::Hang, 600);
        let shared = f.shared.clone();
        let pending = std::thread::spawn(move || {
            content_writes::create(
                &shared,
                "created",
                CreateOptions {
                    writable: true,
                    ..Default::default()
                },
                wait(),
            )
        });
        fired();
        // This reader opens the committed empty descriptor before the creator
        // attaches its private session. It must share the final Cow session.
        let reader = open(&f, "created", false);
        assert!(f.bytes(reader, 7).is_empty());
        if fenced {
            let mut e = tkfs::runtime::lock_engine(&f.shared);
            let entry = e.store.lookup("created").unwrap();
            let mut basic = entry.basic_info();
            basic.write_time += 1;
            e.store.set_basic(&entry.id, basic).unwrap();
        }
        let result = pending.join().unwrap();
        drop(reset);
        if fenced {
            error(result, "STALE_CONTENT_SAVE");
            assert_eq!(tkfs::runtime::lock_engine(&f.shared).handles.len(), 1);
            f.close(reader);
            let h = open(&f, "created", true);
            assert!(f.bytes(h, 7).is_empty());
            f.close(h);
        } else {
            let (h, _) = result.unwrap().unwrap();
            content_writes::write(&f.shared, h, 0, b"created", false, false, wait()).unwrap();
            assert_eq!(&f.bytes(reader, 7), b"created");
            f.close(reader);
            assert_eq!(f.publications(), 0);
            f.close(h);
            assert_eq!(f.publications(), 1);
        }
        assert_eq!(receipts(&f), 1);
        f.oracle();
        f.stop();
    }
}
#[test]
#[ignore = "actual crash helper for native_creation_process_exit_preserves_names_and_promised_bytes"]
fn native_create_crash_child() {
    let root = std::path::PathBuf::from(std::env::var_os("TKFS_CREATE_CRASH_ROOT").unwrap());
    let shared = Arc::new(Mutex::new(Engine::new(
        Store::open_existing(&root).unwrap(),
    )));
    NativeWrites::install(&shared, &context()).unwrap();
    let point = std::env::var("TKFS_CREATE_CRASH_POINT").unwrap();
    fault::configure(Some(FaultConfig::new(&point, FaultAction::Exit)));
    let (h, _) = content_writes::create(
        &shared,
        "created",
        CreateOptions {
            writable: true,
            ..Default::default()
        },
        wait(),
    )
    .unwrap()
    .unwrap();
    content_writes::write(&shared, h, 0, b"private", false, false, wait()).unwrap();
    tkfs::runtime::engine::close_handle(&shared, h).unwrap();
    if point == "after_close" {
        std::process::exit(86);
    }
    panic!("did not reach {point}");
}
#[test]
fn native_creation_process_exit_preserves_names_and_promised_bytes() {
    for point in [
        "content.create.before_event",
        "content.create.event_written",
        "content.create.written",
        "content.create.committed",
        "content.create.private_created",
        "content.write.checkpointed",
        "content.save.event_written",
        "content.save.committed",
        "after_close",
    ] {
        let f = Fixture::new();
        let Fixture {
            shared,
            writes,
            native,
            dir,
            data,
        } = f;
        writes.shutdown(&context()).unwrap();
        drop(writes);
        drop(shared);
        drop(native);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "create::native_create_crash_child",
                "--ignored",
                "--nocapture",
            ])
            .env("TKFS_CREATE_CRASH_ROOT", dir.path())
            .env("TKFS_CREATE_CRASH_POINT", point)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let store = Store::open_existing(dir.path()).unwrap();
        let committed = !matches!(
            point,
            "content.create.before_event"
                | "content.create.event_written"
                | "content.create.written"
        );
        assert_eq!(store.lookup("created").is_ok(), committed, "{point}");
        let shared = Arc::new(Mutex::new(Engine::new(store)));
        let writes = NativeWrites::install(&shared, &context()).unwrap();
        let native = Arc::new(LocalContent::open(dir.path()).unwrap());
        let f = Fixture {
            shared,
            writes,
            native,
            dir,
            data,
        };
        assert_eq!(receipts(&f), u64::from(committed));
        if committed {
            let h = open(&f, "created", false);
            let saved = matches!(
                point,
                "content.write.checkpointed"
                    | "content.save.event_written"
                    | "content.save.committed"
                    | "after_close"
            );
            assert_eq!(
                f.bytes(h, 7),
                if saved { b"private".to_vec() } else { vec![] },
                "{point}"
            );
            f.close(h);
            assert_eq!(f.publications(), usize::from(saved), "{point}");
        }
        f.oracle();
        f.stop();
    }
}

#[test]
#[ignore = "explicit isolated actual WinFsp/FUSE native create, close and replacement"]
fn isolated_kernel_native_create_close_and_replace_preserve_typed_bytes() {
    use std::io::{Seek, SeekFrom, Write};
    if let Some(root) = std::env::var_os("TKFS_CREATE_KERNEL_ROOT") {
        #[cfg(target_os = "linux")]
        unsafe {
            libc::umask(0o022);
        }
        let root = std::path::PathBuf::from(root);
        let shared = Arc::new(Mutex::new(Engine::new(
            Store::open_existing(&root).unwrap(),
        )));
        let writes = NativeWrites::install(&shared, &context()).unwrap();
        let point = root.join("create-mount");
        tkfs::mount::bind(&shared).unwrap();
        tkfs::mount::attach(&point).unwrap();
        struct Mounted;
        impl Drop for Mounted {
            fn drop(&mut self) {
                let _ = tkfs::mount::detach(true);
            }
        }
        let mounted = Mounted;
        let mut source = std::fs::File::create(point.join("created")).unwrap();
        let entry = || {
            tkfs::runtime::lock_engine(&shared)
                .store
                .lookup("created")
                .unwrap()
        };
        assert!(entry().content.is_none());
        assert_eq!(entry().content_ref.unwrap().total_len, 0);
        source.write_all(b"native!").unwrap();
        assert_eq!(source.metadata().unwrap().len(), 7);
        let count = || {
            tkfs::runtime::lock_engine(&shared)
                .store
                .db
                .query_row("SELECT count(*) FROM content_save_history", [], |r| {
                    r.get::<_, u64>(0)
                })
                .unwrap()
        };
        assert_eq!(count(), 0);
        source.sync_all().unwrap();
        assert_eq!(count(), 1);
        let mut old = std::fs::File::create(point.join("target")).unwrap();
        old.write_all(b"old-data").unwrap();
        std::fs::rename(point.join("created"), point.join("target")).unwrap();
        assert_eq!(
            count(),
            1,
            "rename must not publish the destination's bytes"
        );
        old.seek(SeekFrom::Start(0)).unwrap();
        old.write_all(b"retired!").unwrap();
        drop(old);
        assert_eq!(
            count(),
            2,
            "close must durably finish the newly-created unnamed file"
        );
        source.seek(SeekFrom::Start(0)).unwrap();
        source.write_all(b"later!!").unwrap();
        drop(source);
        assert_eq!(
            count(),
            3,
            "source close must save without an explicit final fsync"
        );
        assert_eq!(std::fs::read(point.join("target")).unwrap(), b"later!!");
        assert!(!point.join("created").exists());
        drop(std::fs::File::create(point.join("empty")).unwrap());
        assert!(std::fs::read(point.join("empty")).unwrap().is_empty());
        assert_eq!(count(), 3, "clean empty close is only a namespace creation");
        std::fs::create_dir(point.join("nested")).unwrap();
        let big = vec![0x6b; CHUNK_SIZE as usize + 3];
        std::fs::write(point.join("nested").join("chunked"), &big).unwrap();
        assert_eq!(count(), 4);
        assert_eq!(
            std::fs::read(point.join("nested").join("chunked")).unwrap(),
            big
        );
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(
                std::fs::metadata(point.join("nested").join("chunked"))
                    .unwrap()
                    .mode()
                    & 0o777,
                0o644
            );
        }
        tkfs::mount::detach(false).unwrap();
        drop(mounted);
        writes.shutdown(&context()).unwrap();
        let mut e = tkfs::runtime::lock_engine(&shared);
        assert_eq!(
            e.store
                .lookup("nested/chunked")
                .unwrap()
                .content_ref
                .unwrap()
                .kind,
            tkfs::model::content::ContentKind::Manifest
        );
        assert!(e.store.lookup("target").unwrap().content.is_none());
        assert!(e.store.lookup("empty").unwrap().content.is_none());
        let creates: u64 = e
            .store
            .db
            .query_row(
                "SELECT count(*) FROM content_save_create_changes",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let directory_creates: u64=e.store.db.query_row("SELECT count(*) FROM content_save_create_changes c JOIN events e ON e.id=c.mutation WHERE json_extract(e.payload,'$.changes[0].value')='directory'",[],|r|r.get(0)).unwrap();
        assert_eq!(directory_creates, 1);
        assert_eq!(creates - directory_creates, 4);
        assert!(e.store.verify_derived_state().unwrap().is_empty());
        let publications: u64 = e
            .store
            .db
            .query_row("SELECT count(*) FROM content_save_history", [], |r| {
                r.get(0)
            })
            .unwrap();
        let result = serde_json::json!({"platform":std::env::consts::OS,"real_kernel_mount":true,
            "typed_empty_before_write":true,"buffered_write":true,"explicit_fsync":true,
            "native_close_without_explicit_fsync":true,"empty_close_without_byte_publication":true,
            "newly_created_open_replacement":true,"retired_created_destination_close":true,
            "large_create_uses_manifest":true,"nested_parent_identity":true,"create_receipts":creates,"file_create_receipts":creates-directory_creates,"directory_create_receipts":directory_creates,"byte_publications":publications,
            "activation":"isolated fixture only"});
        std::fs::write(
            root.join("native-create-kernel-result.json"),
            serde_json::to_vec(&result).unwrap(),
        )
        .unwrap();
        return;
    }
    let f = Fixture::new();
    let Fixture {
        shared,
        writes,
        native,
        dir,
        ..
    } = f;
    writes.shutdown(&context()).unwrap();
    drop(writes);
    drop(shared);
    drop(native);
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "create::isolated_kernel_native_create_close_and_replace_preserve_typed_bytes",
            "--ignored",
            "--nocapture",
        ])
        .env("TKFS_CREATE_KERNEL_ROOT", dir.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(
        &std::fs::read(dir.path().join("native-create-kernel-result.json")).unwrap(),
    )
    .unwrap();
    println!("WAVE6_NATIVE_CREATE {result}");
}
