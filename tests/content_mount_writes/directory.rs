use super::*;
use content_writes::CreateOptions;

fn create(f: &Fixture, path: &str) -> (u64, tkfs::core::Entry) {
    content_writes::create_directory(
        &f.shared,
        path,
        CreateOptions {
            posix_mode: Some(0o750),
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
#[test]
fn native_directory_creation_attaches_metadata_and_keeps_enumeration_cuts_without_byte_saves() {
    let f = Fixture::new();
    let (h, entry) = create(&f, "folder");
    assert_eq!(entry.kind, "directory");
    assert_eq!(entry.basic_info().attributes, 0x10);
    assert_eq!(entry.basic_info().posix_mode, Some(0o750));
    assert!(entry.content.is_none() && entry.content_ref.is_none());
    {
        let mut e = tkfs::runtime::lock_engine(&f.shared);
        assert_eq!(e.session(h).unwrap().bytes.len(), 0);
        assert!(!matches!(
            e.session(h).unwrap().bytes,
            tkfs::staging::Staged::Cow(_)
        ));
        assert!(!e.session(h).unwrap().dirty);
        assert_eq!(
            e.store
                .db
                .query_row("SELECT count(*) FROM content_save_operations", [], |r| r
                    .get::<_, u64>(0))
                .unwrap(),
            0
        );
        assert_eq!(e.directory_entry(h, 0, true).unwrap().unwrap().0.name, ".");
        assert!(e.directory_entry(h, 2, false).unwrap().is_none());
    }
    let (child, _) =
        content_writes::create(&f.shared, "folder/child", CreateOptions::default(), wait())
            .unwrap()
            .unwrap();
    f.close(child);
    let mut e = tkfs::runtime::lock_engine(&f.shared);
    assert!(e.directory_entry(h, 2, false).unwrap().is_none());
    assert_eq!(
        e.directory_entry(h, 2, true).unwrap().unwrap().0.name,
        "child"
    );
    drop(e);
    assert_eq!(receipts(&f), 2);
    f.close(h);
    assert_eq!(f.publications(), 0);
    f.oracle();
    f.stop();
}
#[test]
fn failed_directory_creation_never_attaches_a_handle_or_retries_an_absent_receipt() {
    for point in [
        "content.directory.before_event",
        "content.directory.event_written",
        "content.directory.written",
        "content.directory.committed",
        "content.directory.before_attach",
    ] {
        let f = Fixture::new();
        let reset = arm(point, FaultAction::Fail, 0);
        let result =
            content_writes::create_directory(&f.shared, "folder", CreateOptions::default(), wait());
        assert_eq!(fault::fired(), 1);
        drop(reset);
        let committed = matches!(
            point,
            "content.directory.committed" | "content.directory.before_attach"
        );
        assert_eq!(receipts(&f), u64::from(committed));
        if point == "content.directory.committed" {
            f.close(result.unwrap().unwrap().0);
        } else {
            error(result, "FAULT_INJECTED");
            assert!(tkfs::runtime::lock_engine(&f.shared).handles.is_empty());
            if committed {
                error(
                    content_writes::create_directory(
                        &f.shared,
                        "folder",
                        CreateOptions::default(),
                        wait(),
                    ),
                    "NAME_COLLISION",
                );
            } else {
                f.close(create(&f, "folder").0);
            }
        }
        assert_eq!(receipts(&f), 1);
        assert_eq!(f.publications(), 0);
        f.oracle();
        f.stop();
    }
}
#[test]
fn directory_commit_reserves_names_and_capacity_without_holding_engine() {
    static SLOTS: tkfs::runtime::content::WaitSlots = tkfs::runtime::content::WaitSlots::new(1);
    let f = Fixture::new();
    let reader = f.open(false);
    let reset = arm("content.directory.written", FaultAction::Hang, 500);
    let shared = f.shared.clone();
    let thread = std::thread::spawn(move || {
        content_writes::create_directory(
            &shared,
            "folder",
            CreateOptions::default(),
            ContentWait {
                slots: Some(&SLOTS),
                ..wait()
            },
        )
    });
    fired();
    let began = Instant::now();
    let e = tkfs::runtime::try_lock_engine(&f.shared).expect("directory commit held Engine");
    assert!(began.elapsed() < Duration::from_millis(50));
    drop(e);
    error(
        content_writes::create_directory(
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
        content_writes::create_directory(&f.shared, "FOLDER", CreateOptions::default(), wait()),
        "CONTENT_RESOURCE_LIMIT",
    );
    error(
        content_writes::create(&f.shared, "FOLDER", CreateOptions::default(), wait()),
        "CONTENT_RESOURCE_LIMIT",
    );
    assert_eq!(&f.bytes(reader, 7), &f.data[..7]);
    assert!(began.elapsed() < Duration::from_millis(300));
    let (h, _) = thread.join().unwrap().unwrap().unwrap();
    drop(reset);
    f.close(h);
    f.close(reader);
    f.close(create(&f, "other").0);
    f.oracle();
    f.stop();
}
#[test]
fn expired_directory_ack_retains_its_name_without_leaking_a_handle() {
    let f = Fixture::new();
    let reset = arm("content.directory.before_attach", FaultAction::Hang, 400);
    error(
        content_writes::create_directory(
            &f.shared,
            "folder",
            CreateOptions::default(),
            ContentWait::fixed(Duration::from_millis(100)),
        ),
        "CONTENT_DEADLINE",
    );
    drop(reset);
    assert_eq!(receipts(&f), 1);
    assert!(tkfs::runtime::lock_engine(&f.shared).handles.is_empty());
    error(
        content_writes::create_directory(&f.shared, "folder", CreateOptions::default(), wait()),
        "NAME_COLLISION",
    );
    let h = tkfs::runtime::lock_engine(&f.shared)
        .open("folder", None, false)
        .unwrap()
        .0;
    f.close(h);
    assert_eq!(f.publications(), 0);
    f.oracle();
    f.stop();
}
#[test]
fn inode_parent_reuse_is_refused_before_directory_creation() {
    let f = Fixture::new();
    let (h, old) = create(&f, "parent");
    f.close(h);
    let mut e = tkfs::runtime::lock_engine(&f.shared);
    e.store.delete(&old.id).unwrap();
    e.store.create("parent", "directory").unwrap();
    drop(e);
    error(
        content_writes::create_directory(
            &f.shared,
            "parent/child",
            CreateOptions {
                expected_parent: Some(old.id),
                ..Default::default()
            },
            wait(),
        ),
        "STALE_CONTENT_DESTINATION",
    );
    assert!(
        tkfs::runtime::lock_engine(&f.shared)
            .store
            .lookup("parent/child")
            .is_err()
    );
    assert_eq!(receipts(&f), 1);
    f.oracle();
    f.stop();
}
#[test]
#[ignore = "actual WinFsp/FUSE directory creation outside Engine and nested file regression"]
fn isolated_kernel_directory_creation_preserves_metadata_and_native_children() {
    if let Some(root) = std::env::var_os("TKFS_DIRECTORY_KERNEL_ROOT") {
        let root = std::path::PathBuf::from(root);
        let shared = Arc::new(Mutex::new(Engine::new(
            Store::open_existing(&root).unwrap(),
        )));
        let writes = NativeWrites::install(&shared, &context()).unwrap();
        let (cold_metadata_handle, _) = content_writes::create(
            &shared,
            "unopened-metadata",
            CreateOptions::default(),
            wait(),
        )
        .unwrap()
        .unwrap();
        tkfs::runtime::lock_engine(&shared)
            .release_handle(cold_metadata_handle)
            .unwrap();
        let point = root.join("directory-mount");
        tkfs::mount::bind(&shared).unwrap();
        tkfs::mount::attach(&point).unwrap();
        struct Mounted;
        impl Drop for Mounted {
            fn drop(&mut self) {
                let _ = tkfs::mount::detach(true);
            }
        }
        let mounted = Mounted;
        let metadata_reader = std::fs::File::open(point.join("file")).unwrap();
        let reset = arm("content.directory.written", FaultAction::Hang, 500);
        let path = point.join("folder");
        let thread = std::thread::spawn(move || {
            #[cfg(target_os = "linux")]
            {
                use std::os::unix::fs::DirBuilderExt;
                std::fs::DirBuilder::new().mode(0o750).create(path)
            }
            #[cfg(windows)]
            std::fs::create_dir(path)
        });
        fired();
        let began = Instant::now();
        let e =
            tkfs::runtime::try_lock_engine(&shared).expect("actual directory commit held Engine");
        assert!(began.elapsed() < Duration::from_millis(50));
        drop(e);
        assert!(metadata_reader.metadata().unwrap().is_file());
        assert!(began.elapsed() < Duration::from_millis(300));
        let independent_ms = began.elapsed().as_secs_f64() * 1000.0;
        // A name lookup can wait in the kernel on the mkdir parent's inode.
        // Retain that scenario as a measured overlap, separate from fstat.
        let lookup_path = point.join("unopened-metadata");
        let named_metadata = std::thread::spawn(move || {
            let began = Instant::now();
            assert!(std::fs::metadata(lookup_path).unwrap().is_file());
            began.elapsed().as_secs_f64() * 1000.0
        });
        thread.join().unwrap().unwrap();
        let named_metadata_ms = named_metadata.join().unwrap();
        drop(reset);
        drop(metadata_reader);
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(
                std::fs::metadata(point.join("folder")).unwrap().mode() & 0o777,
                0o750
            );
        }
        std::fs::create_dir(point.join("folder/nested")).unwrap();
        std::fs::write(point.join("folder/nested/child"), b"native child").unwrap();
        assert_eq!(
            std::fs::read(point.join("folder/nested/child")).unwrap(),
            b"native child"
        );
        let names: Vec<_> = std::fs::read_dir(point.join("folder"))
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, vec![std::ffi::OsString::from("nested")]);
        tkfs::mount::detach(false).unwrap();
        drop(mounted);
        writes.shutdown(&context()).unwrap();
        let mut e = tkfs::runtime::lock_engine(&shared);
        let folder = e.store.lookup("folder").unwrap();
        assert!(folder.content.is_none() && folder.content_ref.is_none());
        assert_eq!(
            e.store
                .db
                .query_row(
                    "SELECT count(*) FROM content_save_entities WHERE entity=?",
                    [&folder.id],
                    |r| r.get::<_, u64>(0)
                )
                .unwrap(),
            0
        );
        assert!(
            e.store
                .lookup("folder/nested/child")
                .unwrap()
                .content_ref
                .is_some()
        );
        let events = e.store.events().unwrap();
        let directory_events: Vec<_> = events
            .iter()
            .filter(|event| event.changes.iter().any(|c| c.entity == folder.id))
            .collect();
        assert_eq!(directory_events.len(), 1);
        assert_eq!(directory_events[0].changes.len(), 4);
        assert!(e.store.verify_derived_state().unwrap().is_empty());
        let record = serde_json::json!({"platform":std::env::consts::OS,"real_kernel_mount":true,"directory_commit_outside_engine":true,"existing_handle_metadata_ms":independent_ms,"unopened_name_metadata_ms":named_metadata_ms,"native_nested_file":true,"directory_byte_owner_count":0,"activation":"isolated fixture only"});
        std::fs::write(
            root.join("native-directory-kernel-result.json"),
            serde_json::to_vec(&record).unwrap(),
        )
        .unwrap();
        return;
    }
    let Fixture {
        shared,
        writes,
        native,
        dir,
        ..
    } = Fixture::new();
    writes.shutdown(&context()).unwrap();
    drop(writes);
    drop(shared);
    drop(native);
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "directory::isolated_kernel_directory_creation_preserves_metadata_and_native_children",
            "--ignored",
            "--nocapture",
        ])
        .env("TKFS_DIRECTORY_KERNEL_ROOT", dir.path())
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    let record: serde_json::Value = serde_json::from_slice(
        &std::fs::read(dir.path().join("native-directory-kernel-result.json")).unwrap(),
    )
    .unwrap();
    println!("WAVE6_NATIVE_DIRECTORY {record}");
}
#[test]
#[ignore = "actual native rename with parallel new-name open and old-name reuse"]
fn isolated_kernel_native_namespace_preserves_parallel_open_and_name_reuse() {
    use std::io::{Read, Seek, SeekFrom, Write};
    if let Some(root) = std::env::var_os("TKFS_NAMESPACE_PARALLEL_KERNEL_ROOT") {
        let root = std::path::PathBuf::from(root);
        let shared = Arc::new(Mutex::new(Engine::new(
            Store::open_existing(&root).unwrap(),
        )));
        let writes = NativeWrites::install(&shared, &context()).unwrap();
        let point = root.join("parallel-namespace-mount");
        tkfs::mount::bind(&shared).unwrap();
        tkfs::mount::attach(&point).unwrap();
        struct Mounted;
        impl Drop for Mounted {
            fn drop(&mut self) {
                let _ = tkfs::mount::detach(true);
            }
        }
        let mounted = Mounted;
        let mut writer = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(point.join("file"))
            .unwrap();
        writer.write_all(b"private!").unwrap();
        let old_entity = tkfs::runtime::lock_engine(&shared)
            .store
            .lookup("file")
            .unwrap()
            .id;
        let reset = arm("content.save.rename_committed", FaultAction::Hang, 500);
        let source = point.join("file");
        let target = point.join("moved");
        let rename = std::thread::spawn(move || std::fs::rename(source, target));
        fired();
        let began = Instant::now();
        let e =
            tkfs::runtime::try_lock_engine(&shared).expect("rename held Engine after Core commit");
        assert!(began.elapsed() < Duration::from_millis(50));
        drop(e);
        let path = point.join("moved");
        let reader = std::thread::spawn(move || {
            let mut file = std::fs::File::open(path).unwrap();
            let mut bytes = [0; 8];
            file.read_exact(&mut bytes).unwrap();
            bytes
        });
        let path = point.join("file");
        let reused = std::thread::spawn(move || std::fs::write(path, b"new-name"));
        rename.join().unwrap().unwrap();
        assert_eq!(&reader.join().unwrap(), b"private!");
        reused.join().unwrap().unwrap();
        drop(reset);
        writer.seek(SeekFrom::Start(0)).unwrap();
        writer.write_all(b"further!").unwrap();
        writer.sync_all().unwrap();
        drop(writer);
        assert_eq!(std::fs::read(point.join("file")).unwrap(), b"new-name");
        let mut moved = std::fs::File::open(point.join("moved")).unwrap();
        let mut bytes = [0; 8];
        moved.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"further!");
        drop(moved);
        tkfs::mount::detach(false).unwrap();
        drop(mounted);
        writes.shutdown(&context()).unwrap();
        let mut e = tkfs::runtime::lock_engine(&shared);
        assert_eq!(e.store.lookup("moved").unwrap().id, old_entity);
        assert_ne!(e.store.lookup("file").unwrap().id, old_entity);
        assert!(e.store.verify_derived_state().unwrap().is_empty());
        let record = serde_json::json!({"platform":std::env::consts::OS,"real_kernel_mount":true,"parallel_new_name_open":true,"parallel_old_name_reuse":true,"retained_writer_fsync":true,"entity_identity_preserved":true,"activation":"isolated fixture only"});
        std::fs::write(
            root.join("native-parallel-namespace-kernel-result.json"),
            serde_json::to_vec(&record).unwrap(),
        )
        .unwrap();
        return;
    }
    let Fixture {
        shared,
        writes,
        native,
        dir,
        ..
    } = Fixture::new();
    writes.shutdown(&context()).unwrap();
    drop(writes);
    drop(shared);
    drop(native);
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "directory::isolated_kernel_native_namespace_preserves_parallel_open_and_name_reuse",
            "--ignored",
            "--nocapture",
        ])
        .env("TKFS_NAMESPACE_PARALLEL_KERNEL_ROOT", dir.path())
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    let record: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            dir.path()
                .join("native-parallel-namespace-kernel-result.json"),
        )
        .unwrap(),
    )
    .unwrap();
    println!("WAVE6_NATIVE_PARALLEL_NAMESPACE {record}");
}
