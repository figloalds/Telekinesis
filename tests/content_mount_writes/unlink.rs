use super::*;

fn entity(f: &Fixture, h: u64) -> String {
    tkfs::runtime::lock_engine(&f.shared).handles[&h]
        .entity
        .clone()
}
fn remove(f: &Fixture, h: u64) -> anyhow::Result<bool> {
    content_writes::unlink(&f.shared, &entity(f, h), "file", wait())
}
fn receipts(f: &Fixture) -> u64 {
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
fn native_unlink_preserves_open_readers_and_writers_when_the_name_is_reused() {
    let f = Fixture::new();
    let writer = f.open(true);
    let reader = f.open(false);
    let old = entity(&f, writer);
    content_writes::write(&f.shared, writer, 0, b"private!", false, false, wait()).unwrap();
    assert!(remove(&f, writer).unwrap());
    assert_eq!(f.publications(), 0);
    assert_eq!(&f.bytes(reader, 8), b"private!");
    let (replacement, entry) = content_writes::create(
        &f.shared,
        "file",
        content_writes::CreateOptions {
            writable: true,
            ..Default::default()
        },
        wait(),
    )
    .unwrap()
    .unwrap();
    assert_ne!(entry.id, old);
    content_writes::write(&f.shared, writer, 0, b"retired!", false, false, wait()).unwrap();
    content_writes::flush(&f.shared, writer, wait()).unwrap();
    assert_eq!(&f.bytes(reader, 8), b"retired!");
    assert!(
        remove(&f, writer).unwrap(),
        "a repeated retired cleanup must not remove the replacement"
    );
    assert_eq!(receipts(&f), 1);
    assert_eq!(
        tkfs::runtime::lock_engine(&f.shared)
            .store
            .lookup("file")
            .unwrap()
            .id,
        entry.id
    );
    f.close(writer);
    f.close(reader);
    f.close(replacement);
    assert!(
        !tkfs::runtime::lock_engine(&f.shared)
            .store
            .entry(&old)
            .unwrap()
            .alive
    );
    assert_eq!(f.publications(), 1);
    f.oracle();
    f.stop();
}

#[test]
fn native_unlink_promotes_read_only_and_unopened_typed_files_without_byte_publication() {
    for opened in [false, true] {
        let f = Fixture::new();
        let h = opened.then(|| f.open(false));
        let entity = tkfs::runtime::lock_engine(&f.shared)
            .store
            .lookup("file")
            .unwrap()
            .id;
        assert!(content_writes::unlink(&f.shared, &entity, "file", wait()).unwrap());
        if let Some(h) = h {
            assert_eq!(f.bytes(h, 40), f.data[..40]);
            f.close(h);
        }
        assert_eq!(f.publications(), 0);
        assert_eq!(receipts(&f), 1);
        f.oracle();
        f.stop();
    }
}

#[test]
fn native_unlink_recovers_lost_metadata_and_private_adoption_replies_once() {
    for point in [
        "content.save.unlink_committed",
        "content.write.before_namespace_adoption",
        "content.write.namespace_adoption_written",
        "content.write.namespace_adopted",
    ] {
        let f = Fixture::new();
        let h = f.open(true);
        content_writes::write(&f.shared, h, 0, b"private!", false, false, wait()).unwrap();
        let reset = arm(point, FaultAction::Fail, 0);
        assert!(remove(&f, h).unwrap(), "{point}");
        assert_eq!(fault::fired(), 1);
        drop(reset);
        assert_eq!(receipts(&f), 1);
        assert_eq!(f.publications(), 0);
        assert_eq!(&f.bytes(h, 8), b"private!");
        f.close(h);
        assert_eq!(f.publications(), 1);
        assert!(
            tkfs::runtime::lock_engine(&f.shared)
                .store
                .lookup("file")
                .is_err()
        );
        f.oracle();
        f.stop();
    }
}

#[test]
fn failed_native_unlink_retains_its_name_and_saved_bytes_for_retry() {
    for point in ["content.save.before_unlink", "content.save.unlink_written"] {
        let f = Fixture::new();
        let h = f.open(true);
        content_writes::write(&f.shared, h, 0, b"private!", false, false, wait()).unwrap();
        let reset = arm(point, FaultAction::Fail, 0);
        error(remove(&f, h), "FAULT_INJECTED");
        assert_eq!(fault::fired(), 1);
        drop(reset);
        assert_eq!(receipts(&f), 0);
        assert_eq!(f.publications(), 0);
        assert_eq!(&f.bytes(h, 8), b"private!");
        let observed = tkfs::runtime::lock_engine(&f.shared)
            .store
            .lookup("file")
            .unwrap()
            .id;
        assert_eq!(observed, entity(&f, h));
        assert!(remove(&f, h).unwrap());
        f.close(h);
        assert_eq!(f.publications(), 1);
        f.oracle();
        f.stop();
    }
}

#[test]
fn native_unlink_checkpoint_and_sql_wait_do_not_hold_engine() {
    let f = Fixture::new();
    let h = f.open(true);
    content_writes::write(&f.shared, h, 0, b"private!", false, false, wait()).unwrap();
    let old = entity(&f, h);
    let shared = f.shared.clone();
    let reset = arm("content.save.unlink_written", FaultAction::Hang, 500);
    let thread = std::thread::spawn(move || content_writes::unlink(&shared, &old, "file", wait()));
    fired();
    let began = Instant::now();
    let guard = tkfs::runtime::try_lock_engine(&f.shared)
        .expect("unlink held Engine during SQL commit delay");
    assert!(began.elapsed() < Duration::from_millis(50));
    drop(guard);
    assert!(thread.join().unwrap().unwrap());
    drop(reset);
    f.close(h);
    f.oracle();
    f.stop();
}

#[cfg(windows)]
pub(super) fn remove_open(path: &std::path::Path) -> std::io::Result<()> {
    use std::os::windows::{fs::OpenOptionsExt, io::AsRawHandle};
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn SetFileInformationByHandle(
            handle: *mut std::ffi::c_void,
            class: u32,
            info: *const std::ffi::c_void,
            size: u32,
        ) -> i32;
    }
    let file = std::fs::OpenOptions::new()
        .access_mode(0x10000)
        .open(path)?;
    // FileDispositionInfoEx, DELETE | POSIX_SEMANTICS; no fallback is tested.
    // https://learn.microsoft.com/windows-hardware/drivers/ddi/ntddk/ns-ntddk-_file_disposition_information_ex
    let flags = 3u32;
    if unsafe {
        SetFileInformationByHandle(file.as_raw_handle(), 21, (&flags as *const u32).cast(), 4)
    } == 0
    {
        return Err(std::io::Error::last_os_error());
    }
    drop(file);
    Ok(())
}
#[cfg(not(windows))]
pub(super) fn remove_open(path: &std::path::Path) -> std::io::Result<()> {
    std::fs::remove_file(path)
}

#[test]
#[ignore = "explicit actual WinFsp/FUSE unlink, retained descriptors and Windows delete-on-close"]
fn isolated_kernel_native_unlink_retains_open_bytes_and_reused_names() {
    use std::io::{Read, Seek, SeekFrom, Write};
    if let Some(root) = std::env::var_os("TKFS_UNLINK_KERNEL_ROOT") {
        let root = std::path::PathBuf::from(root);
        let shared = Arc::new(Mutex::new(Engine::new(
            Store::open_existing(&root).unwrap(),
        )));
        let writes = NativeWrites::install(&shared, &context()).unwrap();
        let old = tkfs::runtime::lock_engine(&shared)
            .store
            .lookup("file")
            .unwrap()
            .id;
        let point = root.join("unlink-mount");
        tkfs::mount::bind(&shared).unwrap();
        tkfs::mount::attach(&point).unwrap();
        struct Mounted;
        impl Drop for Mounted {
            fn drop(&mut self) {
                let _ = tkfs::mount::detach(true);
            }
        }
        let mounted = Mounted;
        let count = || {
            tkfs::runtime::lock_engine(&shared)
                .store
                .db
                .query_row("SELECT count(*) FROM content_save_history", [], |r| {
                    r.get::<_, u64>(0)
                })
                .unwrap()
        };
        let mut writer = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(point.join("file"))
            .unwrap();
        let mut reader = std::fs::File::open(point.join("file")).unwrap();
        writer.write_all(b"private!").unwrap();
        assert_eq!(count(), 0);
        remove_open(&point.join("file")).unwrap();
        assert!(!point.join("file").exists());
        assert_eq!(count(), 0);
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(writer.metadata().unwrap().nlink(), 0);
        }
        let mut bytes = [0; 8];
        reader.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"private!");
        std::fs::write(point.join("file"), b"new-name").unwrap();
        assert_eq!(count(), 1);
        let new = tkfs::runtime::lock_engine(&shared)
            .store
            .lookup("file")
            .unwrap()
            .id;
        assert_ne!(new, old);
        writer.seek(SeekFrom::Start(0)).unwrap();
        writer.write_all(b"retired!").unwrap();
        writer.sync_all().unwrap();
        assert_eq!(count(), 2);
        reader.seek(SeekFrom::Start(0)).unwrap();
        reader.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"retired!");
        drop(reader);
        drop(writer);
        assert_eq!(std::fs::read(point.join("file")).unwrap(), b"new-name");
        assert_eq!(count(), 2);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            let mut doc = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .custom_flags(0x04000000)
                .open(point.join("doc"))
                .unwrap();
            doc.write_all(b"delete-on-close").unwrap();
            drop(doc);
            assert!(!point.join("doc").exists());
            assert_eq!(count(), 3);
            let e = tkfs::runtime::lock_engine(&shared);
            assert!(e.store.lookup("doc").is_err());
        }
        tkfs::mount::detach(false).unwrap();
        drop(mounted);
        writes.shutdown(&context()).unwrap();
        let mut e = tkfs::runtime::lock_engine(&shared);
        assert!(!e.store.entry(&old).unwrap().alive);
        assert_eq!(e.store.lookup("file").unwrap().id, new);
        assert!(e.store.verify_derived_state().unwrap().is_empty());
        let result = serde_json::json!({"platform":std::env::consts::OS,"real_kernel_mount":true,"namespace_only_unlink":true,"open_reader_preserved":true,"open_writer_fsync":true,"reused_name_isolated":true,"inode_link_count_zero":cfg!(target_os="linux"),"windows_posix_delete":cfg!(windows),"windows_new_delete_on_close":cfg!(windows),"activation":"isolated fixture only"});
        std::fs::write(
            root.join("native-unlink-kernel-result.json"),
            serde_json::to_vec(&result).unwrap(),
        )
        .unwrap();
        return;
    }
    let Fixture {
        shared,
        native,
        writes,
        dir,
        ..
    } = Fixture::new();
    writes.shutdown(&context()).unwrap();
    drop(writes);
    drop(shared);
    drop(native);
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "unlink::isolated_kernel_native_unlink_retains_open_bytes_and_reused_names",
            "--ignored",
            "--nocapture",
        ])
        .env("TKFS_UNLINK_KERNEL_ROOT", dir.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(
        &std::fs::read(dir.path().join("native-unlink-kernel-result.json")).unwrap(),
    )
    .unwrap();
    println!("WAVE6_NATIVE_UNLINK {result}");
}
