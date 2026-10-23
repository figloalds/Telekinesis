use super::*;

#[cfg(windows)]
fn open_rename_source(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    use std::os::windows::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .access_mode(0xc0010000)
        .open(path)
}
#[cfg(not(windows))]
fn open_rename_source(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
}
#[cfg(windows)]
fn classic_rename(from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn MoveFileExW(from: *const u16, to: *const u16, flags: u32) -> i32;
    }
    let from: Vec<_> = from.as_os_str().encode_wide().chain(Some(0)).collect();
    let to: Vec<_> = to.as_os_str().encode_wide().chain(Some(0)).collect();
    if unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), 1) } == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}
#[cfg(windows)]
fn replace_open_destination(
    source: &std::fs::File,
    target: &std::path::Path,
) -> std::io::Result<()> {
    use std::os::windows::{ffi::OsStrExt, io::AsRawHandle};
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn SetFileInformationByHandle(
            handle: *mut std::ffi::c_void,
            class: u32,
            info: *const std::ffi::c_void,
            size: u32,
        ) -> i32;
    }
    let name: Vec<u16> = target.as_os_str().encode_wide().collect();
    let size = 24 + name.len() * 2;
    let mut aligned = vec![0u64; size.div_ceil(8)];
    let bytes = aligned.as_mut_ptr().cast::<u8>();
    // FILE_RENAME_INFO, FileRenameInfoEx22, REPLACE_IF_EXISTS|POSIX_SEMANTICS.
    // https://learn.microsoft.com/windows-hardware/drivers/ddi/ntifs/ns-ntifs-_file_rename_information
    unsafe {
        std::ptr::write_unaligned(bytes.cast::<u32>(), 3);
        std::ptr::write_unaligned(bytes.add(16).cast::<u32>(), (name.len() * 2) as u32);
        std::ptr::copy_nonoverlapping(name.as_ptr().cast::<u8>(), bytes.add(20), name.len() * 2);
        if SetFileInformationByHandle(source.as_raw_handle(), 22, bytes.cast(), size as u32) == 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

fn target(f: &Fixture, writable: bool) -> (String, u64) {
    let root = f
        .native
        .import(&mut b"original".as_slice(), 8, &context())
        .unwrap();
    let mut e = tkfs::runtime::lock_engine(&f.shared);
    let entity = e.store.create("target", "file").unwrap().id;
    publish(&mut e.store, &entity, &root);
    let handle = e.open("target", None, writable).unwrap().0;
    drop(e);
    content_reads::prepare_open(&f.shared, handle, wait()).unwrap();
    (entity, handle)
}

#[test]
fn native_replacement_retains_the_open_destination_writer_and_never_resurrects_its_name() {
    let f = Fixture::new();
    let source = f.open(true);
    let (old_entity, destination) = target(&f, true);
    content_writes::write(&f.shared, source, 0, b"source!!", false, false, wait()).unwrap();
    content_writes::write(&f.shared, destination, 0, b"private!", false, false, wait()).unwrap();
    let entity = tkfs::runtime::lock_engine(&f.shared).handles[&source]
        .entity
        .clone();
    assert!(content_writes::rename(&f.shared, &entity, "target", true, wait()).unwrap());
    assert_eq!(f.publications(), 0);
    assert_eq!(&f.bytes(destination, 8), b"private!");
    content_writes::write(&f.shared, destination, 0, b"retired!", false, false, wait()).unwrap();
    content_writes::flush(&f.shared, destination, wait()).unwrap();
    assert_eq!(&f.bytes(destination, 8), b"retired!");
    assert_eq!(
        tkfs::runtime::lock_engine(&f.shared)
            .store
            .lookup("target")
            .unwrap()
            .id,
        entity
    );
    assert!(
        !tkfs::runtime::lock_engine(&f.shared)
            .store
            .entry(&old_entity)
            .unwrap()
            .alive
    );
    content_writes::flush(&f.shared, source, wait()).unwrap();
    let current = tkfs::runtime::lock_engine(&f.shared)
        .open("target", None, false)
        .unwrap()
        .0;
    content_reads::prepare_open(&f.shared, current, wait()).unwrap();
    assert_eq!(&f.bytes(current, 8), b"source!!");
    f.close(destination);
    f.close(current);
    f.close(source);
    assert_eq!(f.publications(), 2);
    f.oracle();
    f.stop();
}

fn entity(f: &Fixture, handle: u64) -> String {
    tkfs::runtime::lock_engine(&f.shared).handles[&handle]
        .entity
        .clone()
}
fn replace(f: &Fixture, source: u64) -> anyhow::Result<bool> {
    content_writes::rename(&f.shared, &entity(f, source), "target", true, wait())
}
fn namespace_receipts(f: &Fixture) -> u64 {
    tkfs::runtime::lock_engine(&f.shared)
        .store
        .db
        .query_row(
            "SELECT count(*) FROM content_save_namespace_changes",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

#[test]
fn native_replacement_of_a_synchronized_destination_keeps_its_old_fd_writable() {
    let f = Fixture::new();
    let source = f.open(true);
    let source_entity = entity(&f, source);
    let (old, destination) = target(&f, true);
    content_writes::write(&f.shared, destination, 0, b"private!", false, false, wait()).unwrap();
    content_writes::flush(&f.shared, destination, wait()).unwrap();
    assert_eq!(f.publications(), 1);
    assert!(replace(&f, source).unwrap());
    content_writes::write(&f.shared, destination, 0, b"retired!", false, false, wait()).unwrap();
    content_writes::flush(&f.shared, destination, wait()).unwrap();
    assert_eq!(&f.bytes(destination, 8), b"retired!");
    assert!(
        !tkfs::runtime::lock_engine(&f.shared)
            .store
            .entry(&old)
            .unwrap()
            .alive
    );
    assert_eq!(
        tkfs::runtime::lock_engine(&f.shared)
            .store
            .lookup("target")
            .unwrap()
            .id,
        source_entity
    );
    f.close(destination);
    f.close(source);
    assert_eq!(f.publications(), 2);
    f.oracle();
    f.stop();
}

#[test]
fn native_replacement_promotes_read_only_and_unopened_typed_destinations_without_publishing_bytes()
{
    for keep_open in [false, true] {
        let f = Fixture::new();
        let source = f.open(true);
        let (old, destination) = target(&f, false);
        if !keep_open {
            f.close(destination);
        }
        content_writes::write(&f.shared, source, 0, b"source!!", false, false, wait()).unwrap();
        assert!(replace(&f, source).unwrap());
        assert_eq!(f.publications(), 0);
        if keep_open {
            assert_eq!(&f.bytes(destination, 8), b"original");
            assert!(
                content_writes::update_basic(
                    &f.shared,
                    destination,
                    tkfs::core::content_saves::ContentBasicUpdate {
                        write_time: Some(42),
                        ..Default::default()
                    },
                    wait()
                )
                .unwrap()
            );
            f.close(destination);
            assert_eq!(f.publications(), 0);
            assert_eq!(
                tkfs::runtime::lock_engine(&f.shared)
                    .store
                    .entry(&old)
                    .unwrap()
                    .basic_info()
                    .write_time,
                42
            );
        }
        content_writes::flush(&f.shared, source, wait()).unwrap();
        assert_eq!(f.publications(), 1);
        assert!(
            !tkfs::runtime::lock_engine(&f.shared)
                .store
                .entry(&old)
                .unwrap()
                .alive
        );
        assert_eq!(namespace_receipts(&f), 1);
        f.close(source);
        f.oracle();
        f.stop();
    }
}

#[test]
fn failed_and_lost_native_replacement_replies_keep_each_writers_ownership_independent() {
    for point in [
        "content.save.before_rename",
        "content.save.rename_written",
        "content.save.rename_committed",
    ] {
        let f = Fixture::new();
        let source = f.open(true);
        let (old, destination) = target(&f, true);
        content_writes::write(&f.shared, source, 0, b"source!!", false, false, wait()).unwrap();
        content_writes::write(&f.shared, destination, 0, b"private!", false, false, wait())
            .unwrap();
        let reset = arm(point, FaultAction::Fail, 0);
        let result = replace(&f, source);
        assert_eq!(fault::fired(), 1);
        drop(reset);
        let committed = point.ends_with("committed");
        if committed {
            assert!(result.unwrap());
        } else {
            error(result, "FAULT_INJECTED");
        }
        content_writes::write(&f.shared, destination, 0, b"retired!", false, false, wait())
            .unwrap();
        content_writes::flush(&f.shared, destination, wait()).unwrap();
        assert_eq!(&f.bytes(destination, 8), b"retired!");
        assert_eq!(
            tkfs::runtime::lock_engine(&f.shared)
                .store
                .entry(&old)
                .unwrap()
                .alive,
            !committed
        );
        let expected = if committed { entity(&f, source) } else { old };
        assert_eq!(
            tkfs::runtime::lock_engine(&f.shared)
                .store
                .lookup("target")
                .unwrap()
                .id,
            expected
        );
        content_writes::flush(&f.shared, source, wait()).unwrap();
        assert_eq!(namespace_receipts(&f), u64::from(committed));
        assert_eq!(f.publications(), 2);
        f.close(destination);
        f.close(source);
        f.oracle();
        f.stop();
    }
}

#[test]
fn expired_replacement_reply_allows_the_destination_to_reconcile_before_the_source() {
    let f = Fixture::new();
    let source = f.open(true);
    let (old, destination) = target(&f, true);
    content_writes::write(&f.shared, source, 0, b"source!!", false, false, wait()).unwrap();
    content_writes::write(&f.shared, destination, 0, b"private!", false, false, wait()).unwrap();
    let reset = arm("content.save.rename_committed", FaultAction::Hang, 800);
    error(
        content_writes::rename(
            &f.shared,
            &entity(&f, source),
            "target",
            true,
            ContentWait::fixed(Duration::from_millis(300)),
        ),
        "CONTENT_DEADLINE",
    );
    drop(reset);
    assert_eq!(&f.bytes(destination, 8), b"private!");
    content_writes::write(&f.shared, destination, 0, b"retired!", false, false, wait()).unwrap();
    content_writes::flush(&f.shared, destination, wait()).unwrap();
    content_writes::write(&f.shared, source, 0, b"latest!!", false, false, wait()).unwrap();
    content_writes::flush(&f.shared, source, wait()).unwrap();
    assert!(
        !tkfs::runtime::lock_engine(&f.shared)
            .store
            .entry(&old)
            .unwrap()
            .alive
    );
    assert_eq!(namespace_receipts(&f), 1);
    assert_eq!(f.publications(), 2);
    assert_eq!(&f.bytes(destination, 8), b"retired!");
    assert_eq!(&f.bytes(source, 8), b"latest!!");
    f.close(destination);
    f.close(source);
    f.oracle();
    f.stop();
}

#[test]
fn lost_private_retirement_reply_is_reconciled_by_a_read_without_applying_the_failed_write() {
    let f = Fixture::new();
    let source = f.open(true);
    let (_, destination) = target(&f, true);
    content_writes::write(&f.shared, destination, 0, b"private!", false, false, wait()).unwrap();
    assert!(replace(&f, source).unwrap());
    let reset = arm("content.write.namespace_adopted", FaultAction::Fail, 0);
    error(
        content_writes::write(&f.shared, destination, 0, b"bad-data", false, false, wait()),
        "FAULT_INJECTED",
    );
    assert_eq!(fault::fired(), 1);
    drop(reset);
    assert_eq!(&f.bytes(destination, 8), b"private!");
    assert_eq!(f.publications(), 0);
    content_writes::write(&f.shared, destination, 0, b"retired!", false, false, wait()).unwrap();
    content_writes::flush(&f.shared, destination, wait()).unwrap();
    assert_eq!(f.publications(), 1);
    assert_eq!(namespace_receipts(&f), 1);
    f.close(destination);
    f.close(source);
    f.oracle();
    f.stop();
}

#[test]
fn reciprocal_native_replacements_acquire_both_entity_gates_in_one_order() {
    let f = Fixture::new();
    let source = f.open(true);
    let (_, destination) = target(&f, true);
    content_writes::write(&f.shared, source, 0, b"source!!", false, false, wait()).unwrap();
    content_writes::write(&f.shared, destination, 0, b"private!", false, false, wait()).unwrap();
    let a = entity(&f, source);
    let b = entity(&f, destination);
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let left = {
        let shared = f.shared.clone();
        let barrier = barrier.clone();
        let a = a.clone();
        std::thread::spawn(move || {
            barrier.wait();
            content_writes::rename(
                &shared,
                &a,
                "target",
                true,
                ContentWait::fixed(Duration::from_secs(3)),
            )
        })
    };
    let right = {
        let shared = f.shared.clone();
        let barrier = barrier.clone();
        let b = b.clone();
        std::thread::spawn(move || {
            barrier.wait();
            content_writes::rename(
                &shared,
                &b,
                "file",
                true,
                ContentWait::fixed(Duration::from_secs(3)),
            )
        })
    };
    barrier.wait();
    let left = left.join().unwrap();
    let right = right.join().unwrap();
    assert_ne!(left.is_ok(), right.is_ok(), "{left:?} {right:?}");
    assert_eq!(namespace_receipts(&f), 1);
    assert_eq!(f.publications(), 0);
    assert_eq!(&f.bytes(source, 8), b"source!!");
    assert_eq!(&f.bytes(destination, 8), b"private!");
    content_writes::flush(&f.shared, source, wait()).unwrap();
    content_writes::flush(&f.shared, destination, wait()).unwrap();
    assert_eq!(f.publications(), 2);
    f.close(source);
    f.close(destination);
    f.oracle();
    f.stop();
}

#[test]
fn replacement_reserves_adapter_capacity_before_waiting_for_either_entity_gate() {
    static SLOTS: tkfs::runtime::content::WaitSlots = tkfs::runtime::content::WaitSlots::new(1);
    let f = Fixture::new();
    let source = f.open(true);
    let (_, destination) = target(&f, true);
    let a = entity(&f, source);
    let b = entity(&f, destination);
    let slots = &SLOTS;
    let reset = arm("content.save.before_rename", FaultAction::Hang, 500);
    let first = {
        let shared = f.shared.clone();
        std::thread::spawn(move || {
            content_writes::rename(
                &shared,
                &a,
                "target",
                true,
                ContentWait {
                    slots: Some(slots),
                    ..wait()
                },
            )
        })
    };
    fired();
    let began = Instant::now();
    error(
        content_writes::rename(
            &f.shared,
            &b,
            "file",
            true,
            ContentWait {
                slots: Some(slots),
                ..wait()
            },
        ),
        "CONTENT_RESOURCE_LIMIT",
    );
    assert!(began.elapsed() < Duration::from_millis(100));
    assert!(first.join().unwrap().unwrap());
    drop(reset);
    f.close(destination);
    f.close(source);
    f.oracle();
    f.stop();
}

#[test]
fn replacement_adopts_both_older_receipts_without_publishing_their_newer_buffers() {
    let f = Fixture::new();
    let source = f.open(true);
    let (_, destination) = target(&f, true);
    for handle in [source, destination] {
        content_writes::write(&f.shared, handle, 0, b"earlier!", false, false, wait()).unwrap();
        let reset = arm("content.executor.before_adoption", FaultAction::Fail, 0);
        error(
            content_writes::flush(&f.shared, handle, wait()),
            "FAULT_INJECTED",
        );
        drop(reset);
        content_writes::write(
            &f.shared,
            handle,
            0,
            if handle == source {
                b"source!!"
            } else {
                b"private!"
            },
            false,
            false,
            wait(),
        )
        .unwrap();
    }
    assert_eq!(f.publications(), 2);
    assert!(replace(&f, source).unwrap());
    assert_eq!(f.publications(), 2);
    assert_eq!(&f.bytes(source, 8), b"source!!");
    assert_eq!(&f.bytes(destination, 8), b"private!");
    content_writes::flush(&f.shared, destination, wait()).unwrap();
    content_writes::flush(&f.shared, source, wait()).unwrap();
    assert_eq!(f.publications(), 4);
    f.close(destination);
    f.close(source);
    f.oracle();
    f.stop();
}

fn restart(f: Fixture) -> Fixture {
    let Fixture {
        shared,
        native,
        writes,
        dir,
        data,
    } = f;
    writes.shutdown(&context()).unwrap();
    drop(shared);
    drop(writes);
    drop(native);
    let shared = Arc::new(Mutex::new(Engine::new(
        Store::open_existing(dir.path()).unwrap(),
    )));
    let writes = NativeWrites::install(&shared, &context()).unwrap();
    let native = Arc::new(LocalContent::open(dir.path()).unwrap());
    Fixture {
        shared,
        native,
        writes,
        dir,
        data,
    }
}
fn await_recovery(f: &Fixture, publications: usize) {
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        let ready = {
            let e = tkfs::runtime::lock_engine(&f.shared);
            let count: usize = e
                .store
                .db
                .query_row("SELECT count(*) FROM content_save_history", [], |r| {
                    r.get(0)
                })
                .unwrap();
            count == publications && e.sessions.values().all(|s| !s.dirty && !s.saving)
        };
        if ready && tkfs::runtime::engine::background_saves() == 0 {
            return;
        }
        assert!(Instant::now() < end, "native recovery did not finish");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn startup_finishes_an_unnamed_destination_without_holding_up_visible_private_reads() {
    let f = Fixture::new();
    let source = f.open(true);
    let (_, destination) = target(&f, true);
    content_writes::write(&f.shared, source, 0, b"source!!", false, false, wait()).unwrap();
    content_writes::write(&f.shared, destination, 0, b"private!", false, false, wait()).unwrap();
    assert!(replace(&f, source).unwrap());
    let reset = arm("content.executor.before_prepare", FaultAction::Hang, 1200);
    let began = Instant::now();
    let f = restart(f);
    assert!(began.elapsed() < Duration::from_millis(300));
    fired();
    let handle = tkfs::runtime::lock_engine(&f.shared)
        .open("target", None, false)
        .unwrap()
        .0;
    content_reads::prepare_open(&f.shared, handle, wait()).unwrap();
    let began = Instant::now();
    assert_eq!(&f.bytes(handle, 8), b"source!!");
    assert!(began.elapsed() < Duration::from_millis(300));
    assert_eq!(f.publications(), 0);
    let end = Instant::now() + Duration::from_secs(5);
    while f.publications() != 1 {
        assert!(Instant::now() < end);
        std::thread::sleep(Duration::from_millis(5));
    }
    drop(reset);
    f.close(handle);
    await_recovery(&f, 2);
    assert_eq!(namespace_receipts(&f), 1);
    f.oracle();
    f.stop();
}

#[test]
#[ignore = "actual process-exit helper for native_replacement_process_exit_recovers_both_saved_operations"]
fn native_replacement_crash_child() {
    let root = std::path::PathBuf::from(std::env::var_os("TKFS_NATIVE_REPLACE_ROOT").unwrap());
    let shared = Arc::new(Mutex::new(Engine::new(
        Store::open_existing(&root).unwrap(),
    )));
    let _writes = NativeWrites::install(&shared, &context()).unwrap();
    let source = tkfs::runtime::lock_engine(&shared)
        .open("file", None, true)
        .unwrap()
        .0;
    let destination = tkfs::runtime::lock_engine(&shared)
        .open("target", None, true)
        .unwrap()
        .0;
    content_reads::prepare_open(&shared, source, wait()).unwrap();
    content_reads::prepare_open(&shared, destination, wait()).unwrap();
    fault::configure(Some(FaultConfig::new(
        &std::env::var("TKFS_NATIVE_REPLACE_POINT").unwrap(),
        FaultAction::Exit,
    )));
    let entity = tkfs::runtime::lock_engine(&shared).handles[&source]
        .entity
        .clone();
    content_writes::rename(&shared, &entity, "target", true, wait()).unwrap();
    content_writes::flush(&shared, destination, wait()).unwrap();
    panic!("did not reach native replacement crash boundary");
}

#[test]
fn native_replacement_process_exit_recovers_both_saved_operations() {
    for point in [
        "content.save.before_rename",
        "content.save.rename_written",
        "content.save.rename_committed",
        "content.write.before_namespace_adoption",
        "content.write.namespace_adoption_written",
        "content.write.namespace_adopted",
        "content.save.before_metadata",
        "content.save.event_written",
        "content.save.committed",
    ] {
        let f = Fixture::new();
        let source = f.open(true);
        let (old, destination) = target(&f, true);
        let source_entity = entity(&f, source);
        content_writes::write(&f.shared, source, 0, b"source!!", false, false, wait()).unwrap();
        content_writes::write(&f.shared, destination, 0, b"private!", false, false, wait())
            .unwrap();
        content_writes::retain_dirty(&f.shared, &context()).unwrap();
        let Fixture {
            shared,
            native,
            writes,
            dir,
            data,
        } = f;
        writes.shutdown(&context()).unwrap();
        drop(shared);
        drop(native);
        drop(writes);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "replacement::native_replacement_crash_child",
                "--ignored",
                "--nocapture",
            ])
            .env("TKFS_NATIVE_REPLACE_ROOT", dir.path())
            .env("TKFS_NATIVE_REPLACE_POINT", point)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let committed = !matches!(
            point,
            "content.save.before_rename" | "content.save.rename_written"
        );
        let store = Store::open_existing(dir.path()).unwrap();
        assert_eq!(
            store.lookup("target").unwrap().id,
            if committed {
                source_entity.clone()
            } else {
                old.clone()
            }
        );
        let shared = Arc::new(Mutex::new(Engine::new(store)));
        let writes = NativeWrites::install(&shared, &context()).unwrap();
        let native = Arc::new(LocalContent::open(dir.path()).unwrap());
        let f = Fixture {
            shared,
            native,
            writes,
            dir,
            data,
        };
        if !committed {
            let source = f.open(true);
            let destination = tkfs::runtime::lock_engine(&f.shared)
                .open("target", None, true)
                .unwrap()
                .0;
            content_reads::prepare_open(&f.shared, destination, wait()).unwrap();
            assert_eq!(&f.bytes(source, 8), b"source!!");
            assert_eq!(&f.bytes(destination, 8), b"private!");
            assert_eq!(f.publications(), 0);
            assert!(replace(&f, source).unwrap());
            f.close(destination);
            f.close(source);
        }
        await_recovery(&f, 2);
        assert_eq!(namespace_receipts(&f), 1);
        assert!(
            !tkfs::runtime::lock_engine(&f.shared)
                .store
                .entry(&old)
                .unwrap()
                .alive
        );
        let handle = tkfs::runtime::lock_engine(&f.shared)
            .open("target", None, false)
            .unwrap()
            .0;
        content_reads::prepare_open(&f.shared, handle, wait()).unwrap();
        assert_eq!(&f.bytes(handle, 8), b"source!!");
        f.close(handle);
        f.oracle();
        f.stop();
    }
}

#[test]
#[ignore = "explicit isolated real WinFsp/FUSE replacement and retained destination FD"]
fn isolated_kernel_native_replacement_preserves_open_destination() {
    use std::io::{Read, Seek, SeekFrom, Write};
    if let Some(root) = std::env::var_os("TKFS_REPLACE_KERNEL_CHILD") {
        let root = std::path::PathBuf::from(root);
        let point = root.join("replacement-mount");
        let shared = Arc::new(Mutex::new(Engine::new(
            Store::open_existing(&root).unwrap(),
        )));
        let writes = NativeWrites::install(&shared, &context()).unwrap();
        tkfs::mount::bind(&shared).unwrap();
        tkfs::mount::attach(&point).unwrap();
        struct Mounted;
        impl Drop for Mounted {
            fn drop(&mut self) {
                let _ = tkfs::mount::detach(true);
            }
        }
        let mounted = Mounted;
        let source_entity = tkfs::runtime::lock_engine(&shared)
            .store
            .lookup("file")
            .unwrap()
            .id;
        let old_entity = tkfs::runtime::lock_engine(&shared)
            .store
            .lookup("target")
            .unwrap()
            .id;
        let mut source = open_rename_source(&point.join("file")).unwrap();
        let mut old = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(point.join("target"))
            .unwrap();
        source.write_all(b"source!!").unwrap();
        old.write_all(b"private!").unwrap();
        // Dedicated native removal parents now own unlink/delete-on-close.
        // Keep this destination live for the replacement assertions below.
        #[cfg(not(windows))]
        std::fs::rename(point.join("file"), point.join("target")).unwrap();
        #[cfg(windows)]
        {
            assert!(
                classic_rename(&point.join("file"), &point.join("target")).is_err(),
                "classic Windows rename must retain its open-target refusal"
            );
            // std::fs::rename uses a POSIX FileRenameInfoEx fallback on this toolchain.
            std::fs::rename(point.join("file"), point.join("target")).unwrap();
        }
        assert!(!point.join("file").exists());
        assert_eq!(old.metadata().unwrap().len(), 8);
        let explicit = std::time::UNIX_EPOCH + Duration::from_millis(42);
        old.set_times(std::fs::FileTimes::new().set_modified(explicit))
            .unwrap();
        assert_eq!(old.metadata().unwrap().modified().unwrap(), explicit);
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(old.metadata().unwrap().nlink(), 0);
        }
        let mut bytes = [0; 8];
        old.seek(SeekFrom::Start(0)).unwrap();
        old.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"private!");
        {
            let mut current = std::fs::File::open(point.join("target")).unwrap();
            current.read_exact(&mut bytes).unwrap();
            assert_eq!(&bytes, b"source!!");
        }
        let count = || {
            tkfs::runtime::lock_engine(&shared)
                .store
                .db
                .query_row("SELECT count(*) FROM content_save_history", [], |r| {
                    r.get::<_, u64>(0)
                })
                .unwrap()
        };
        assert_eq!(
            count(),
            0,
            "namespace and read-only close must not publish either writer"
        );
        old.seek(SeekFrom::Start(0)).unwrap();
        old.write_all(b"retired!").unwrap();
        old.sync_all().unwrap();
        assert_eq!(count(), 1);
        assert!(
            !tkfs::runtime::lock_engine(&shared)
                .store
                .entry(&old_entity)
                .unwrap()
                .alive
        );
        old.seek(SeekFrom::Start(0)).unwrap();
        old.write_all(b"closing!").unwrap();
        drop(old);
        assert_eq!(
            count(),
            2,
            "close must recover the final unnamed write without an explicit fsync"
        );
        source.sync_all().unwrap();
        drop(source);
        assert_eq!(count(), 3);
        let mut current = std::fs::File::open(point.join("target")).unwrap();
        current.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"source!!");
        drop(current);
        tkfs::mount::detach(false).unwrap();
        drop(mounted);
        writes.shutdown(&context()).unwrap();
        let mut e = tkfs::runtime::lock_engine(&shared);
        assert_eq!(e.store.lookup("target").unwrap().id, source_entity);
        assert!(!e.store.entry(&old_entity).unwrap().alive);
        assert!(e.store.verify_derived_state().unwrap().is_empty());
        let result = serde_json::json!({"platform":std::env::consts::OS,"real_kernel_mount":true,"open_destination_read":true,"open_destination_metadata":true,"open_destination_write_fsync":true,"unnamed_close_without_explicit_fsync":true,"new_path_keeps_source_bytes":true,"no_namespace_byte_publication":true,"retired_publications":2,"source_publications":1,"inode_link_count_zero":cfg!(target_os="linux"),"activation":"isolated fixture only"});
        std::fs::write(
            root.join("replacement-kernel-result.json"),
            serde_json::to_vec(&result).unwrap(),
        )
        .unwrap();
        return;
    }
    #[cfg(windows)]
    {
        use std::io::{Read, Seek, SeekFrom};
        let control = tempfile::tempdir().unwrap();
        let a = control.path().join("source");
        let b = control.path().join("target");
        std::fs::write(&a, b"source!!").unwrap();
        std::fs::write(&b, b"original").unwrap();
        let source = open_rename_source(&a).unwrap();
        let mut old = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&b)
            .unwrap();
        assert!(
            classic_rename(&a, &b).is_err(),
            "native classic rename control"
        );
        replace_open_destination(&source, &b).unwrap();
        let mut bytes = [0; 8];
        old.seek(SeekFrom::Start(0)).unwrap();
        old.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"original");
        assert_eq!(&std::fs::read(&b).unwrap(), b"source!!");
    }
    let f = Fixture::new();
    let (_, h) = target(&f, false);
    f.close(h);
    f.stop();
    let Fixture {
        shared,
        native,
        writes,
        dir,
        ..
    } = f;
    drop(shared);
    drop(native);
    drop(writes);
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "replacement::isolated_kernel_native_replacement_preserves_open_destination",
            "--ignored",
            "--nocapture",
        ])
        .env("TKFS_REPLACE_KERNEL_CHILD", dir.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(
        &std::fs::read(dir.path().join("replacement-kernel-result.json")).unwrap(),
    )
    .unwrap();
    println!("WAVE6_NATIVE_REPLACEMENT {result}");
}

#[test]
fn rename_keeps_source_and_destination_live_after_their_last_user_handles_close() {
    for expire in [false, true] {
        let f = Fixture::new();
        let source = f.open(true);
        let a = entity(&f, source);
        let (b, destination) = target(&f, true);
        let reset = arm(
            "content.namespace.before_entity_gates",
            FaultAction::Hang,
            400,
        );
        let shared = f.shared.clone();
        let moving = a.clone();
        let rename = std::thread::spawn(move || {
            content_writes::rename(
                &shared,
                &moving,
                "target",
                true,
                ContentWait::fixed(Duration::from_millis(if expire { 40 } else { 5_000 })),
            )
        });
        fired();
        // The kernel may finish the last asynchronous release while the
        // path-based operation is waiting to acquire its entity gates.
        f.close(source);
        f.close(destination);
        {
            let e = tkfs::runtime::lock_engine(&f.shared);
            assert!(
                e.quiet().is_err(),
                "in-flight namespace ownership must fence checkout"
            );
            assert_eq!(e.handles.len(), 2);
            assert_eq!(e.sessions[&a].references, 1);
            assert_eq!(e.sessions[&b].references, 1);
        }
        let result = rename.join().unwrap();
        drop(reset);
        if expire {
            error(result, "CONTENT_DEADLINE");
            assert_eq!(namespace_receipts(&f), 0);
            {
                let e = tkfs::runtime::lock_engine(&f.shared);
                assert!(e.handles.is_empty() && e.sessions.is_empty());
                assert_eq!(e.store.lookup("file").unwrap().id, a);
                assert_eq!(e.store.lookup("target").unwrap().id, b);
            }
            assert!(content_writes::rename(&f.shared, &a, "target", true, wait()).unwrap());
        } else {
            assert!(result.unwrap());
        }
        {
            let e = tkfs::runtime::lock_engine(&f.shared);
            assert!(e.handles.is_empty() && e.sessions.is_empty());
            assert!(e.quiet().is_ok());
            assert_eq!(e.store.lookup("target").unwrap().id, a);
            assert!(!e.store.entry(&b).unwrap().alive);
        }
        assert_eq!(namespace_receipts(&f), 1);
        assert_eq!(f.publications(), 0);
        let reader = tkfs::runtime::lock_engine(&f.shared)
            .open("target", None, false)
            .unwrap()
            .0;
        content_reads::prepare_open(&f.shared, reader, wait()).unwrap();
        assert_eq!(f.bytes(reader, f.data.len()), f.data);
        f.close(reader);
        f.oracle();
        f.stop();
    }
}

#[test]
fn native_rename_preserves_both_private_writers_when_replacing_a_new_current_file() {
    let f = Fixture::new();
    let source = f.open(true);
    let (destination, target) = content_writes::create(
        &f.shared,
        "target",
        content_writes::CreateOptions {
            writable: true,
            ..Default::default()
        },
        wait(),
    )
    .unwrap()
    .unwrap();
    content_writes::write(&f.shared, source, 0, b"source!!", false, false, wait()).unwrap();
    content_writes::write(
        &f.shared,
        destination,
        0,
        b"destination!!",
        false,
        false,
        wait(),
    )
    .unwrap();
    error(
        content_writes::rename(&f.shared, &entity(&f, source), "TARGET", false, wait()),
        "NAME_COLLISION",
    );
    assert_eq!(namespace_receipts(&f), 0);
    assert_eq!(f.publications(), 0);
    assert!(
        content_writes::rename(&f.shared, &entity(&f, source), "target", true, wait()).unwrap()
    );
    assert_eq!(namespace_receipts(&f), 1);
    assert_eq!(f.publications(), 0);
    assert_eq!(f.bytes(source, 8), b"source!!");
    assert_eq!(f.bytes(destination, 13), b"destination!!");
    let source_entity = entity(&f, source);
    assert_eq!(
        tkfs::runtime::lock_engine(&f.shared)
            .store
            .lookup("target")
            .unwrap()
            .id,
        source_entity
    );
    assert!(
        !tkfs::runtime::lock_engine(&f.shared)
            .store
            .entry(&target.id)
            .unwrap()
            .alive
    );
    f.close(destination);
    f.close(source);
    f.oracle();
    f.stop();
}

#[test]
fn native_rename_reserves_current_creates_without_blocking_unrelated_private_io() {
    for fail in [false, true] {
        let f = Fixture::new();
        let source = f.open(true);
        let (other, _) = content_writes::create(
            &f.shared,
            "other",
            content_writes::CreateOptions {
                writable: true,
                ..Default::default()
            },
            wait(),
        )
        .unwrap()
        .unwrap();
        content_writes::write(&f.shared, source, 0, b"source!!", false, false, wait()).unwrap();
        let a = entity(&f, source);
        let reset = arm(
            "content.save.before_rename",
            if fail {
                FaultAction::Fail
            } else {
                FaultAction::Hang
            },
            600,
        );
        if fail {
            error(
                content_writes::rename(&f.shared, &a, "target", false, wait()),
                "FAULT_INJECTED",
            );
            drop(reset);
            let (h, _) = content_writes::create(&f.shared, "target", Default::default(), wait())
                .unwrap()
                .unwrap();
            f.close(h);
        } else {
            let shared = f.shared.clone();
            let rename = std::thread::spawn(move || {
                content_writes::rename(&shared, &a, "target", false, wait())
            });
            fired();
            let began = Instant::now();
            error(
                content_writes::create(&f.shared, "TARGET", Default::default(), wait()),
                "CONTENT_RESOURCE_LIMIT",
            );
            error(
                content_writes::create_directory(&f.shared, "target", Default::default(), wait()),
                "CONTENT_RESOURCE_LIMIT",
            );
            content_writes::write(&f.shared, other, 0, b"unrelated", false, false, wait()).unwrap();
            assert_eq!(f.bytes(other, 9), b"unrelated");
            assert!(began.elapsed() < Duration::from_millis(200));
            assert!(rename.join().unwrap().unwrap());
            drop(reset);
            let source_entity = entity(&f, source);
            assert_eq!(
                tkfs::runtime::lock_engine(&f.shared)
                    .store
                    .lookup("target")
                    .unwrap()
                    .id,
                source_entity
            );
            assert_eq!(f.bytes(source, 8), b"source!!");
        }
        f.close(other);
        f.close(source);
        f.oracle();
        f.stop();
    }
}
