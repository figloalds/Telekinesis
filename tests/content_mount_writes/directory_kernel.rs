use super::*;

#[cfg(windows)]
fn open_directory(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    use std::os::windows::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .access_mode(0x10180)
        .share_mode(7)
        .custom_flags(0x02000000)
        .open(path)
}
#[cfg(target_os = "linux")]
fn open_directory(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    std::fs::File::open(path)
}

#[cfg(windows)]
fn disposition(file: &std::fs::File, delete: bool) -> std::io::Result<()> {
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
    if unsafe {
        SetFileInformationByHandle(file.as_raw_handle(), 21, (&flags as *const u32).cast(), 4)
    } == 0
    {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}
#[cfg(windows)]
fn remove_open(path: &std::path::Path) -> std::io::Result<()> {
    let file = open_directory(path)?;
    disposition(&file, true)?;
    drop(file);
    Ok(())
}
#[cfg(target_os = "linux")]
fn remove_open(path: &std::path::Path) -> std::io::Result<()> {
    std::fs::remove_dir(path)
}

#[cfg(windows)]
fn replace_open(
    file: &std::fs::File,
    _from: &std::path::Path,
    to: &std::path::Path,
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
    let name: Vec<u16> = to.as_os_str().encode_wide().collect();
    let size = 24 + name.len() * 2;
    let mut aligned = vec![0u64; size.div_ceil(8)];
    let bytes = aligned.as_mut_ptr().cast::<u8>();
    unsafe {
        std::ptr::write_unaligned(bytes.cast::<u32>(), 3);
        std::ptr::write_unaligned(bytes.add(16).cast::<u32>(), (name.len() * 2) as u32);
        std::ptr::copy_nonoverlapping(name.as_ptr().cast::<u8>(), bytes.add(20), name.len() * 2);
        if SetFileInformationByHandle(file.as_raw_handle(), 22, bytes.cast(), size as u32) == 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}
#[cfg(target_os = "linux")]
fn replace_open(
    _file: &std::fs::File,
    from: &std::path::Path,
    to: &std::path::Path,
) -> std::io::Result<()> {
    std::fs::rename(from, to)
}

struct Mounted;
impl Drop for Mounted {
    fn drop(&mut self) {
        let _ = tkfs::mount::detach(true);
    }
}
fn child_root() -> Option<std::path::PathBuf> {
    std::env::var_os("TKFS_DIRECTORY_CHANGES_KERNEL_ROOT").map(std::path::PathBuf::from)
}
fn run_child(case: &str, result_name: &str) -> serde_json::Value {
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
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", case, "--ignored", "--nocapture"])
        .env("TKFS_DIRECTORY_CHANGES_KERNEL_ROOT", dir.path())
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    let record: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join(result_name)).unwrap()).unwrap();
    assert_eq!(record["real_kernel_mount"], true);
    record
}

#[test]
#[ignore = "actual WinFsp/FUSE directory metadata, rename, replacement and retained handles"]
fn isolated_kernel_directory_mutations_preserve_metadata_handles_and_empty_replacement() {
    let Some(root) = child_root() else {
        let record = run_child(
            "directory_kernel::isolated_kernel_directory_mutations_preserve_metadata_handles_and_empty_replacement",
            "directory-mutations-kernel-result.json",
        );
        assert_eq!(record["retained_directory_handles"], true);
        assert_eq!(record["directory_byte_owners"], 0);
        return;
    };
    let shared = Arc::new(Mutex::new(Engine::new(
        Store::open_existing(&root).unwrap(),
    )));
    let writes = NativeWrites::install(&shared, &context()).unwrap();
    let point = root.join("directory-mutations-mount");
    tkfs::mount::bind(&shared).unwrap();
    tkfs::mount::attach(&point).unwrap();
    let mounted = Mounted;
    // Synthetic inode metadata still uses its ordinary adapter path.
    assert!(std::fs::metadata(&point).unwrap().is_dir());
    let _ = std::fs::metadata(point.join(".tkfs-runtime.json")).unwrap();
    std::fs::create_dir(point.join("source")).unwrap();
    std::fs::create_dir(point.join("destination")).unwrap();
    let source = open_directory(&point.join("source")).unwrap();
    let destination = open_directory(&point.join("destination")).unwrap();
    let source_id = tkfs::runtime::lock_engine(&shared)
        .store
        .lookup("source")
        .unwrap()
        .id;
    let destination_id = tkfs::runtime::lock_engine(&shared)
        .store
        .lookup("destination")
        .unwrap()
        .id;
    let clock = std::time::UNIX_EPOCH + Duration::from_secs(1_600_000_000);
    source
        .set_times(std::fs::FileTimes::new().set_modified(clock))
        .unwrap();
    assert_eq!(source.metadata().unwrap().modified().unwrap(), clock);
    let reader = std::fs::File::open(point.join("file")).unwrap();
    let reset = arm("content.directory.change_written", FaultAction::Hang, 500);
    let from = point.join("source");
    let moved = point.join("moved");
    let thread = std::thread::spawn(move || std::fs::rename(from, moved));
    fired();
    let began = Instant::now();
    let e = tkfs::runtime::try_lock_engine(&shared).expect("actual directory rename held Engine");
    assert!(began.elapsed() < Duration::from_millis(50));
    drop(e);
    assert!(reader.metadata().unwrap().is_file());
    assert!(began.elapsed() < Duration::from_millis(300));
    let independent_ms = began.elapsed().as_secs_f64() * 1000.0;
    thread.join().unwrap().unwrap();
    drop(reset);
    drop(reader);
    assert!(source.metadata().unwrap().is_dir());
    assert!(!point.join("source").exists());
    replace_open(&source, &point.join("moved"), &point.join("destination")).unwrap();
    assert_eq!(
        tkfs::runtime::lock_engine(&shared)
            .store
            .lookup("destination")
            .unwrap()
            .id,
        source_id
    );
    let retired_clock = clock + Duration::from_secs(11);
    destination
        .set_times(std::fs::FileTimes::new().set_modified(retired_clock))
        .unwrap();
    assert_eq!(
        destination.metadata().unwrap().modified().unwrap(),
        retired_clock
    );
    assert_eq!(source.metadata().unwrap().modified().unwrap(), clock);
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(destination.metadata().unwrap().nlink(), 0);
    }
    remove_open(&point.join("destination")).unwrap();
    assert!(!point.join("destination").exists());
    std::fs::create_dir(point.join("destination")).unwrap();
    let replacement_id = tkfs::runtime::lock_engine(&shared)
        .store
        .lookup("destination")
        .unwrap()
        .id;
    assert_ne!(replacement_id, source_id);
    source
        .set_times(std::fs::FileTimes::new().set_modified(clock + Duration::from_secs(22)))
        .unwrap();
    assert_eq!(
        source.metadata().unwrap().modified().unwrap(),
        clock + Duration::from_secs(22)
    );
    std::fs::create_dir(point.join("destination/nonempty")).unwrap();
    std::fs::write(point.join("destination/nonempty/child"), b"native child").unwrap();
    assert!(std::fs::remove_dir(point.join("destination/nonempty")).is_err());
    std::fs::remove_file(point.join("destination/nonempty/child")).unwrap();
    std::fs::remove_dir(point.join("destination/nonempty")).unwrap();
    drop(source);
    drop(destination);
    tkfs::mount::detach(false).unwrap();
    drop(mounted);
    writes.shutdown(&context()).unwrap();
    let mut e = tkfs::runtime::lock_engine(&shared);
    assert!(!e.store.entry(&source_id).unwrap().alive);
    assert!(!e.store.entry(&destination_id).unwrap().alive);
    assert_eq!(e.store.lookup("destination").unwrap().id, replacement_id);
    let owners: u64 = e
        .store
        .db
        .query_row(
            "SELECT count(*) FROM content_save_entities WHERE entity IN(?,?,?)",
            [&source_id, &destination_id, &replacement_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(owners, 0);
    assert!(
        e.store
            .directory_delete_intents(None, 16, &context())
            .unwrap()
            .is_empty()
    );
    assert!(e.store.verify_derived_state().unwrap().is_empty());
    drop(e);
    let record = serde_json::json!({"platform":std::env::consts::OS,"real_kernel_mount":true,"retained_directory_handles":true,"directory_byte_owners":owners,"existing_handle_metadata_ms":independent_ms,"empty_replacement":true,"nonempty_removal_refused":true,"name_reuse_safe":true,"activation":"isolated fixture only"});
    std::fs::write(
        root.join("directory-mutations-kernel-result.json"),
        serde_json::to_vec(&record).unwrap(),
    )
    .unwrap();
}

#[test]
#[ignore = "actual WinFsp durable directory Cleanup failure/cancellation and fallible FUSE removal"]
fn isolated_kernel_directory_cleanup_failure_has_exact_restart_and_cancellation() {
    let Some(root) = child_root() else {
        let record = run_child(
            "directory_kernel::isolated_kernel_directory_cleanup_failure_has_exact_restart_and_cancellation",
            "directory-cleanup-kernel-result.json",
        );
        assert_eq!(record["directory_byte_owners"], 0);
        assert_eq!(record["windows_durable_cleanup_recovered"], cfg!(windows));
        return;
    };
    let shared = Arc::new(Mutex::new(Engine::new(
        Store::open_existing(&root).unwrap(),
    )));
    let writes = NativeWrites::install(&shared, &context()).unwrap();
    let point = root.join("directory-cleanup-mount");
    tkfs::mount::bind(&shared).unwrap();
    tkfs::mount::attach(&point).unwrap();
    let mounted = Mounted;
    std::fs::create_dir(point.join("cancel")).unwrap();
    #[cfg(windows)]
    {
        let handle = open_directory(&point.join("cancel")).unwrap();
        disposition(&handle, true).unwrap();
        assert_eq!(
            tkfs::runtime::lock_engine(&shared)
                .store
                .directory_delete_intents(None, 16, &context())
                .unwrap()
                .len(),
            1
        );
        disposition(&handle, false).unwrap();
        assert!(
            tkfs::runtime::lock_engine(&shared)
                .store
                .directory_delete_intents(None, 16, &context())
                .unwrap()
                .is_empty()
        );
        drop(handle);
    }
    std::fs::create_dir(point.join("pending")).unwrap();
    let entity = tkfs::runtime::lock_engine(&shared)
        .store
        .lookup("pending")
        .unwrap()
        .id;
    let mut config = FaultConfig::new("content.directory.change_written", FaultAction::Fail);
    config.count = None;
    fault::configure(Some(config));
    let removed = remove_open(&point.join("pending"));
    let hits = fault::fired();
    assert!(hits >= 1);
    #[cfg(windows)]
    {
        removed.unwrap();
        assert_eq!(
            tkfs::runtime::lock_engine(&shared)
                .store
                .directory_delete_intents(None, 16, &context())
                .unwrap()
                .len(),
            1
        );
    }
    #[cfg(target_os = "linux")]
    {
        assert!(removed.is_err());
        assert!(point.join("pending").exists());
        assert!(
            tkfs::runtime::lock_engine(&shared)
                .store
                .directory_delete_intents(None, 16, &context())
                .unwrap()
                .is_empty()
        );
    }
    tkfs::mount::detach(true).unwrap();
    drop(mounted);
    writes.shutdown(&context()).unwrap();
    fault::configure(None);
    drop(writes);
    drop(shared);
    let recovered = Arc::new(Mutex::new(Engine::new(
        Store::open_existing(&root).unwrap(),
    )));
    let writes = NativeWrites::install(&recovered, &context()).unwrap();
    let mut e = tkfs::runtime::lock_engine(&recovered);
    assert!(e.store.lookup("cancel").unwrap().alive);
    assert_eq!(e.store.entry(&entity).unwrap().alive, !cfg!(windows));
    assert!(
        e.store
            .directory_delete_intents(None, 16, &context())
            .unwrap()
            .is_empty()
    );
    let owners: u64 = e
        .store
        .db
        .query_row(
            "SELECT count(*) FROM content_save_entities WHERE entity=?",
            [&entity],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(owners, 0);
    assert!(e.store.verify_derived_state().unwrap().is_empty());
    drop(e);
    writes.shutdown(&context()).unwrap();
    let record = serde_json::json!({"platform":std::env::consts::OS,"real_kernel_mount":true,"persistent_cleanup_fault_hits":hits,"windows_durable_cleanup_recovered":cfg!(windows),"fuse_failure_is_fallible":cfg!(target_os="linux"),"directory_byte_owners":owners,"activation":"isolated fixture only"});
    std::fs::write(
        root.join("directory-cleanup-kernel-result.json"),
        serde_json::to_vec(&record).unwrap(),
    )
    .unwrap();
}
