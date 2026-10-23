use super::*;
fn pending(f: &Fixture) -> usize {
    tkfs::runtime::lock_engine(&f.shared)
        .store
        .content_delete_intents(None, 16, &context())
        .unwrap()
        .len()
}
fn close_for_restart(f: Fixture) -> (tempfile::TempDir, String) {
    let entity = tkfs::runtime::lock_engine(&f.shared)
        .store
        .lookup("file")
        .unwrap()
        .id;
    f.writes.shutdown(&context()).unwrap();
    let Fixture {
        shared,
        native,
        writes,
        dir,
        ..
    } = f;
    drop(writes);
    drop(shared);
    drop(native);
    (dir, entity)
}
fn restart(root: &std::path::Path) -> (Shared, Arc<NativeWrites>) {
    let shared = Arc::new(Mutex::new(Engine::new(Store::open_existing(root).unwrap())));
    let writes = NativeWrites::install(&shared, &context()).unwrap();
    (shared, writes)
}
#[test]
fn cancellation_is_per_handle_and_does_not_clear_another_handles_disposition() {
    let f = Fixture::new();
    let a = f.open(false);
    let b = f.open(true);
    assert!(content_writes::set_delete_disposition(&f.shared, a, true, wait()).unwrap());
    assert!(content_writes::set_delete_disposition(&f.shared, b, true, wait()).unwrap());
    assert_eq!(pending(&f), 2);
    content_writes::set_delete_disposition(&f.shared, a, false, wait()).unwrap();
    assert_eq!(pending(&f), 1);
    content_writes::set_delete_disposition(&f.shared, a, false, wait()).unwrap();
    assert_eq!(pending(&f), 1);
    content_writes::set_delete_disposition(&f.shared, b, false, wait()).unwrap();
    assert_eq!(pending(&f), 0);
    assert_eq!(f.publications(), 0);
    assert!(
        tkfs::runtime::lock_engine(&f.shared)
            .store
            .lookup("file")
            .is_ok()
    );
    f.close(a);
    f.close(b);
    let (dir, entity) = close_for_restart(f);
    let (shared, writes) = restart(dir.path());
    assert_eq!(
        tkfs::runtime::lock_engine(&shared)
            .store
            .lookup("file")
            .unwrap()
            .id,
        entity
    );
    writes.shutdown(&context()).unwrap();
}
#[test]
fn repeated_disposition_cannot_mask_a_writer_fence_with_an_existing_request() {
    let f = Fixture::new();
    let h = f.open(true);
    content_writes::set_delete_disposition(&f.shared, h, true, wait()).unwrap();
    let entity = tkfs::runtime::lock_engine(&f.shared).handles[&h]
        .entity
        .clone();
    tkfs::runtime::lock_engine(&f.shared)
        .store
        .begin_content_save(&entity, &context())
        .unwrap();
    error(
        content_writes::set_delete_disposition(&f.shared, h, true, wait()),
        "STALE_CONTENT_SAVE",
    );
    assert_eq!(pending(&f), 1);
    content_writes::set_delete_disposition(&f.shared, h, false, wait()).unwrap();
    assert_eq!(pending(&f), 0);
    assert_eq!(f.publications(), 0);
    f.stop();
}
#[test]
fn lost_disposition_and_cancellation_replies_are_resolved_from_exact_authority() {
    let f = Fixture::new();
    let h = f.open(true);
    for (point, delete, accepted, present) in [
        ("content.save.delete_intent_written", true, false, 0),
        ("content.save.delete_intent_committed", true, true, 1),
        ("content.save.delete_cancel_written", false, false, 1),
        ("content.save.delete_cancel_committed", false, true, 0),
    ] {
        let reset = arm(point, FaultAction::Fail, 0);
        let result = content_writes::set_delete_disposition(&f.shared, h, delete, wait());
        assert_eq!(fault::fired(), 1, "{point}");
        drop(reset);
        assert_eq!(result.is_ok(), accepted, "{point}: {result:?}");
        assert_eq!(pending(&f), present, "{point}");
        assert!(
            tkfs::runtime::lock_engine(&f.shared)
                .store
                .lookup("file")
                .is_ok()
        );
        assert_eq!(f.publications(), 0);
    }
    f.close(h);
    f.stop();
}
#[test]
fn pending_clean_disposition_restarts_without_saved_or_canonical_byte_work() {
    let f = Fixture::new();
    let h = f.open(false);
    content_writes::set_delete_disposition(&f.shared, h, true, wait()).unwrap();
    assert_eq!(pending(&f), 1);
    let (dir, entity) = close_for_restart(f);
    let (shared, writes) = restart(dir.path());
    let mut e = tkfs::runtime::lock_engine(&shared);
    assert!(!e.store.entry(&entity).unwrap().alive);
    assert!(e.store.lookup("file").is_err());
    assert!(
        e.store
            .content_delete_intents(None, 16, &context())
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        e.store
            .db
            .query_row("SELECT count(*) FROM content_save_history", [], |r| r
                .get::<_, u64>(0))
            .unwrap(),
        0
    );
    assert!(e.store.verify_derived_state().unwrap().is_empty());
    drop(e);
    writes.shutdown(&context()).unwrap();
}
#[test]
fn failed_void_cleanup_keeps_its_durable_request_and_saved_bytes_until_restart() {
    let f = Fixture::new();
    let h = f.open(true);
    content_writes::set_delete_disposition(&f.shared, h, true, wait()).unwrap();
    content_writes::write(&f.shared, h, 0, b"private!", false, false, wait()).unwrap();
    let mut config = FaultConfig::new("content.save.unlink_written", FaultAction::Fail);
    config.count = None;
    fault::configure(Some(config));
    let result = content_writes::complete_delete_disposition(&f.shared, h, "file", wait());
    let hits = fault::fired();
    fault::configure(None);
    assert!(result.is_err());
    assert!(hits >= 1);
    assert_eq!(pending(&f), 1);
    assert_eq!(f.publications(), 0);
    assert_eq!(&f.bytes(h, 8), b"private!");
    let (dir, entity) = close_for_restart(f);
    let (shared, writes) = restart(dir.path());
    tkfs::runtime::engine::settle_entity(&shared, &entity).unwrap();
    let mut e = tkfs::runtime::lock_engine(&shared);
    assert!(!e.store.entry(&entity).unwrap().alive);
    assert!(e.store.lookup("file").is_err());
    let reference = e.store.entry(&entity).unwrap().content_ref.unwrap();
    let native = LocalContent::open(dir.path()).unwrap();
    let pin = native.pin(&reference).unwrap();
    let mut bytes = [0; 8];
    native.read_range(&pin, 0, &mut bytes, &context()).unwrap();
    assert_eq!(&bytes, b"private!");
    assert!(
        e.store
            .content_delete_intents(None, 16, &context())
            .unwrap()
            .is_empty()
    );
    assert!(e.store.verify_derived_state().unwrap().is_empty());
    drop(e);
    writes.shutdown(&context()).unwrap();
}
#[test]
fn durable_disposition_follows_published_generations_and_an_owned_rename_at_restart() {
    let f = Fixture::new();
    let h = f.open(true);
    content_writes::set_delete_disposition(&f.shared, h, true, wait()).unwrap();
    content_writes::write(&f.shared, h, 0, b"flushed!", false, false, wait()).unwrap();
    content_writes::flush(&f.shared, h, wait()).unwrap();
    let entity = tkfs::runtime::lock_engine(&f.shared).handles[&h]
        .entity
        .clone();
    content_writes::rename(&f.shared, &entity, "moved", false, wait()).unwrap();
    assert_eq!(pending(&f), 1);
    f.writes.shutdown(&context()).unwrap();
    let Fixture {
        shared,
        native,
        writes,
        dir,
        ..
    } = f;
    drop(writes);
    drop(shared);
    drop(native);
    let (shared, writes) = restart(dir.path());
    let mut e = tkfs::runtime::lock_engine(&shared);
    assert!(e.store.lookup("moved").is_err());
    assert!(!e.store.entry(&entity).unwrap().alive);
    assert!(e.store.verify_derived_state().unwrap().is_empty());
    drop(e);
    writes.shutdown(&context()).unwrap();
}
#[test]
fn stale_disposition_is_refused_before_recovery_mutates_the_private_journal() {
    let f = Fixture::new();
    let h = f.open(true);
    content_writes::set_delete_disposition(&f.shared, h, true, wait()).unwrap();
    let entity = tkfs::runtime::lock_engine(&f.shared).handles[&h]
        .entity
        .clone();
    tkfs::runtime::lock_engine(&f.shared)
        .store
        .begin_content_save(&entity, &context())
        .unwrap();
    let (dir, _) = close_for_restart(f);
    let journal = dir.path().join("write-sessions-v1.sqlite");
    let before = std::fs::read(&journal).unwrap();
    let shared = Arc::new(Mutex::new(Engine::new(
        Store::open_existing(dir.path()).unwrap(),
    )));
    error(
        NativeWrites::install(&shared, &context()),
        "STALE_CONTENT_SAVE",
    );
    assert_eq!(std::fs::read(journal).unwrap(), before);
    assert_eq!(
        tkfs::runtime::lock_engine(&shared)
            .store
            .content_delete_intents(None, 16, &context())
            .unwrap()
            .len(),
        1
    );
}

#[cfg(windows)]
fn disposition(file: &std::fs::File, delete: bool) {
    use std::os::windows::io::AsRawHandle;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn SetFileInformationByHandle(
            handle: *mut std::ffi::c_void,
            class: u32,
            info: *const std::ffi::c_void,
            size: u32,
        ) -> i32;
    }
    let flags = if delete { 3u32 } else { 0 };
    assert_ne!(
        unsafe {
            SetFileInformationByHandle(file.as_raw_handle(), 21, (&flags as *const u32).cast(), 4)
        },
        0,
        "{}",
        std::io::Error::last_os_error()
    );
}
#[test]
#[ignore = "actual WinFsp cleanup-failure restart/cancellation and FUSE fallible unlink"]
fn isolated_kernel_delete_failure_restarts_without_losing_bytes_or_cancellation() {
    use std::io::Write;
    if let Some(root) = std::env::var_os("TKFS_DELETE_RECOVERY_KERNEL_ROOT") {
        let root = std::path::PathBuf::from(root);
        let shared = Arc::new(Mutex::new(Engine::new(
            Store::open_existing(&root).unwrap(),
        )));
        let writes = NativeWrites::install(&shared, &context()).unwrap();
        let entity = tkfs::runtime::lock_engine(&shared)
            .store
            .lookup("file")
            .unwrap()
            .id;
        let point = root.join("delete-recovery-mount");
        tkfs::mount::bind(&shared).unwrap();
        tkfs::mount::attach(&point).unwrap();
        struct Mounted;
        impl Drop for Mounted {
            fn drop(&mut self) {
                let _ = tkfs::mount::detach(true);
            }
        }
        let mounted = Mounted;
        std::fs::write(point.join("cancel"), b"keep cancel").unwrap();
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            let handle = std::fs::OpenOptions::new()
                .read(true)
                .access_mode(0x80010000)
                .open(point.join("cancel"))
                .unwrap();
            disposition(&handle, true);
            assert_eq!(
                tkfs::runtime::lock_engine(&shared)
                    .store
                    .content_delete_intents(None, 16, &context())
                    .unwrap()
                    .len(),
                1
            );
            disposition(&handle, false);
            assert!(
                tkfs::runtime::lock_engine(&shared)
                    .store
                    .content_delete_intents(None, 16, &context())
                    .unwrap()
                    .is_empty()
            );
            drop(handle);
            assert_eq!(std::fs::read(point.join("cancel")).unwrap(), b"keep cancel");
        }
        let mut writer = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(point.join("file"))
            .unwrap();
        writer.write_all(b"private!").unwrap();
        #[cfg(windows)]
        let mut doc = {
            use std::os::windows::fs::OpenOptionsExt;
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .custom_flags(0x04000000)
                .open(point.join("doc"))
                .unwrap();
            file.write_all(b"pending-doc").unwrap();
            file
        };
        #[cfg(windows)]
        let doc_entity = tkfs::runtime::lock_engine(&shared)
            .store
            .lookup("doc")
            .unwrap()
            .id;
        let mut config = FaultConfig::new("content.save.unlink_written", FaultAction::Fail);
        config.count = None;
        fault::configure(Some(config));
        let removed = super::unlink::remove_open(&point.join("file"));
        #[cfg(windows)]
        {
            removed.unwrap();
            assert!(
                !tkfs::runtime::lock_engine(&shared)
                    .store
                    .content_delete_intents(None, 16, &context())
                    .unwrap()
                    .is_empty()
            );
        }
        #[cfg(not(windows))]
        {
            assert!(removed.is_err());
            assert!(
                tkfs::runtime::lock_engine(&shared)
                    .store
                    .content_delete_intents(None, 16, &context())
                    .unwrap()
                    .is_empty()
            );
            assert!(point.join("file").exists());
        }
        drop(writer);
        #[cfg(windows)]
        {
            let _ = doc.flush();
            drop(doc);
        }
        tkfs::mount::detach(true).unwrap();
        drop(mounted);
        writes.shutdown(&context()).unwrap();
        fault::configure(None);
        #[cfg(windows)]
        assert_eq!(
            tkfs::runtime::lock_engine(&shared)
                .store
                .content_delete_intents(None, 16, &context())
                .unwrap()
                .len(),
            2
        );
        drop(writes);
        drop(shared);
        let recovered = Arc::new(Mutex::new(Engine::new(
            Store::open_existing(&root).unwrap(),
        )));
        tkfs::mount::bind(&recovered).unwrap();
        let writes = NativeWrites::install(&recovered, &context()).unwrap();
        tkfs::runtime::engine::settle_entity(&recovered, &entity).unwrap();
        #[cfg(windows)]
        tkfs::runtime::engine::settle_entity(&recovered, &doc_entity).unwrap();
        let mut e = tkfs::runtime::lock_engine(&recovered);
        #[cfg(windows)]
        {
            assert!(e.store.lookup("file").is_err());
            assert!(!e.store.entry(&entity).unwrap().alive);
            assert!(e.store.lookup("doc").is_err());
        }
        #[cfg(not(windows))]
        assert_eq!(e.store.lookup("file").unwrap().id, entity);
        assert!(e.store.lookup("cancel").unwrap().alive);
        assert!(
            e.store
                .content_delete_intents(None, 16, &context())
                .unwrap()
                .is_empty()
        );
        let reference = e.store.entry(&entity).unwrap().content_ref.unwrap();
        let native = LocalContent::open(&root).unwrap();
        let pin = native.pin(&reference).unwrap();
        let mut bytes = [0; 8];
        native.read_range(&pin, 0, &mut bytes, &context()).unwrap();
        assert_eq!(&bytes, b"private!");
        #[cfg(windows)]
        {
            let reference = e.store.entry(&doc_entity).unwrap().content_ref.unwrap();
            let pin = native.pin(&reference).unwrap();
            let mut bytes = [0; 11];
            native.read_range(&pin, 0, &mut bytes, &context()).unwrap();
            assert_eq!(&bytes, b"pending-doc");
        }
        assert!(e.store.verify_derived_state().unwrap().is_empty());
        drop(e);
        writes.shutdown(&context()).unwrap();
        let result = serde_json::json!({"platform":std::env::consts::OS,"real_kernel_mount":true,"persistent_metadata_failure":true,"saved_bytes_survive_restart":true,
            "windows_cancellation_preserved":cfg!(windows),"windows_doc_recovered":cfg!(windows),"windows_dispositions_consumed_on_restart":cfg!(windows),"fuse_failure_is_fallible":cfg!(target_os="linux"),"activation":"isolated fixture only"});
        std::fs::write(
            root.join("native-delete-recovery-kernel-result.json"),
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
    let result=std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact","deletion::isolated_kernel_delete_failure_restarts_without_losing_bytes_or_cancellation","--ignored","--nocapture"])
        .env("TKFS_DELETE_RECOVERY_KERNEL_ROOT",dir.path()).output().unwrap();
    assert!(
        result.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    let record: serde_json::Value = serde_json::from_slice(
        &std::fs::read(dir.path().join("native-delete-recovery-kernel-result.json")).unwrap(),
    )
    .unwrap();
    println!("WAVE6_NATIVE_DELETE_RECOVERY {record}");
}
