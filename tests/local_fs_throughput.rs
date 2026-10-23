//! Current COW/range and explicit mounted local-filesystem scenarios.
//! Standalone whole-file/deferred-save mechanisms are retired in favor of the
//! current save/executor/create coverage recorded in the Wave 6 inventory.
//! Mounted cases require explicit invocation and fail if mounting is unavailable.
#[path = "support/current_reads.rs"]
mod current_reads;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};
use tkfs::{
    core::*,
    runtime::{
        self, Engine, Shared,
        content::{ContentWait, WaitSlots},
    },
    util::fault::{self, FaultAction, FaultConfig},
};
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|poison| poison.into_inner())
}

fn shared(root: &Path) -> Shared {
    Arc::new(Mutex::new(Engine::new(
        Store::open(root, Some("local-fs")).unwrap(),
    )))
}

fn lock(engine: &Shared) -> MutexGuard<'_, Engine> {
    engine.lock().unwrap()
}

fn bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn pending_rows(e: &Engine) -> Vec<(String, String)> {
    e.store
        .db
        .prepare("SELECT entity,payload FROM pending_saves ORDER BY entity")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn no_conflicts(e: &Engine) {
    let open: Vec<_> = e
        .store
        .conflicts()
        .unwrap()
        .into_iter()
        .filter(|c| c.resolved_by.is_none())
        .collect();
    assert!(open.is_empty(), "unexpected conflicts: {open:?}");
}

fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if predicate() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn content(e: &Engine, path: &str) -> Vec<u8> {
    use tkfs::content::ContentProvider;
    let reference = e.store.lookup(path).unwrap().content_ref.unwrap();
    let local = tkfs::content::LocalContent::open(&e.store.root).unwrap();
    let pin = local.pin(&reference).unwrap();
    let mut bytes = vec![0; reference.total_len as usize];
    local
        .read_range(&pin, 0, &mut bytes, &current_reads::context())
        .unwrap();
    bytes
}
struct Disarm;
impl Drop for Disarm {
    fn drop(&mut self) {
        fault::configure(None);
    }
}

/// A local current writer with explicit executor and handle ownership.
struct CurrentFile {
    shared: Shared,
    native: tkfs::content::LocalContent,
    writes: Arc<runtime::content_writes::NativeWrites>,
    handle: u64,
}
impl CurrentFile {
    fn new(root: &Path, data: &[u8]) -> Self {
        let shared = shared(root);
        current_reads::committed(&shared, "file", data);
        let native = tkfs::content::LocalContent::open(root).unwrap();
        let writes =
            runtime::content_writes::NativeWrites::install(&shared, &current_reads::context())
                .unwrap();
        let handle = lock(&shared).open("file", None, true).unwrap().0;
        runtime::content_reads::prepare_open(
            &shared,
            handle,
            ContentWait::fixed(Duration::from_secs(30)),
        )
        .unwrap();
        Self {
            shared,
            native,
            writes,
            handle,
        }
    }
    fn read(&self) -> Vec<u8> {
        let len = lock(&self.shared).session(self.handle).unwrap().bytes.len();
        // Adapter requests are bounded; exercise the same range path repeatedly.
        (0..len)
            .step_by(512 * 1024)
            .flat_map(|offset| {
                runtime::content_reads::read_mount_vec(
                    &self.shared,
                    self.handle,
                    offset as u64,
                    (len - offset).min(512 * 1024),
                    ContentWait::fixed(Duration::from_secs(30)),
                )
                .unwrap()
                .unwrap()
            })
            .collect()
    }
}
impl Drop for CurrentFile {
    fn drop(&mut self) {
        let _ = lock(&self.shared).release_handle(self.handle);
        let result = self.writes.shutdown(&current_reads::context());
        if !std::thread::panicking() {
            result.unwrap();
        }
    }
}
#[test]
fn the_first_large_file_edit_is_chunk_local_and_releases_the_engine_lock() {
    use tkfs::{model::content::CHUNK_SIZE, runtime::content_writes, util::phases::Phase};
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let original = bytes(7, 2 * 1024 * 1024 + 17);
    let f = CurrentFile::new(dir.path(), &original);
    let wait = ContentWait::fixed(Duration::from_secs(30));
    assert!(matches!(
        lock(&f.shared).session(f.handle).unwrap().bytes,
        tkfs::staging::Staged::Cow(_)
    ));
    assert!(!lock(&f.shared).session(f.handle).unwrap().dirty);
    let before = f.native.counters.snapshot();
    fault::configure(Some(FaultConfig {
        ms: Some(300),
        count: Some(1),
        ..FaultConfig::new("content.native.before_write", FaultAction::Hang)
    }));
    let reset = Disarm;
    let engine = f.shared.clone();
    let h = f.handle;
    let offset = CHUNK_SIZE + 17;
    let writer = std::thread::spawn(move || {
        content_writes::write(&engine, h, offset, &vec![99; 4096], false, false, wait)
    });
    assert!(wait_until(Duration::from_secs(5), || fault::fired() > 0));
    let began = Instant::now();
    lock(&f.shared)
        .store
        .create("unrelated", "directory")
        .unwrap();
    let elapsed = began.elapsed();
    assert_eq!(writer.join().unwrap().unwrap(), Some(4096));
    drop(reset);
    assert!(
        elapsed < Duration::from_millis(250),
        "Engine blocked for {elapsed:?}"
    );
    let buffered = f.native.counters.snapshot();
    for phase in [Phase::Write, Phase::ManifestWrite] {
        assert_eq!(
            buffered[phase as usize].operations,
            before[phase as usize].operations
        );
    }
    assert!(content_writes::flush(&f.shared, f.handle, wait).unwrap());
    let after = f.native.counters.snapshot();
    assert!(
        after[Phase::Read as usize].bytes - before[Phase::Read as usize].bytes <= 4 * CHUNK_SIZE
    );
    assert_eq!(
        after[Phase::Write as usize].operations - before[Phase::Write as usize].operations,
        1
    );
    assert!(
        after[Phase::ManifestWrite as usize].operations
            - before[Phase::ManifestWrite as usize].operations
            <= 2
    );
    assert_eq!(
        fs::read_dir(dir.path().join("staging")).map_or(0, |d| d.count()),
        0
    );
    let mut expected = original;
    expected[offset as usize..offset as usize + 4096].fill(99);
    assert_eq!(f.read(), expected);
    let before = f.native.counters.snapshot();
    assert!(content_writes::truncate(&f.shared, f.handle, 0, false, wait).unwrap());
    assert!(content_writes::flush(&f.shared, f.handle, wait).unwrap());
    assert_eq!(
        f.native.counters.snapshot()[Phase::Read as usize].bytes,
        before[Phase::Read as usize].bytes
    );
    assert!(f.read().is_empty());
    let e = lock(&f.shared);
    let reference = e.store.lookup("file").unwrap().content_ref.unwrap();
    assert_eq!(
        reference,
        tkfs::model::content::FileContentRef::new(
            tkfs::model::content::ContentKind::Direct,
            tkfs::model::content::Digest::bytes(&[]),
            0
        )
        .unwrap()
    );
}
#[test]
fn a_corrupt_current_chunk_refuses_the_edit_before_acknowledgement() {
    use tkfs::{
        model::content::{CHUNK_SIZE, Digest, ObjectKey, ObjectKind},
        runtime::content_writes,
        util::phases::Phase,
    };
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let original = bytes(9, 600 * 1024);
    let f = CurrentFile::new(dir.path(), &original);
    let wait = ContentWait::fixed(Duration::from_secs(30));
    let entry = lock(&f.shared).session(f.handle).unwrap().entry.clone();
    assert_eq!(
        content_writes::write(&f.shared, f.handle, 0, &[], false, false, wait).unwrap(),
        Some(0)
    );
    assert!(!lock(&f.shared).session(f.handle).unwrap().dirty);
    let chunk = &original[..CHUNK_SIZE as usize];
    let key = ObjectKey {
        kind: ObjectKind::Bytes,
        digest: Digest::bytes(chunk),
    };
    let mut damaged = chunk.to_vec();
    damaged[100] ^= 0xff;
    fs::write(
        dir.path().join("objects").join(key.digest.to_string()),
        damaged,
    )
    .unwrap();
    let before = f.native.counters.snapshot();
    let error =
        content_writes::write(&f.shared, f.handle, 5, b"EDIT", false, false, wait).unwrap_err();
    assert!(
        format!("{error:#}").contains("CORRUPT_CONTENT"),
        "{error:#}"
    );
    assert!(!lock(&f.shared).session(f.handle).unwrap().dirty);
    assert_eq!(lock(&f.shared).session(f.handle).unwrap().entry, entry);
    let error =
        content_writes::truncate(&f.shared, f.handle, CHUNK_SIZE / 2, false, wait).unwrap_err();
    assert!(
        format!("{error:#}").contains("CORRUPT_CONTENT"),
        "{error:#}"
    );
    assert!(!lock(&f.shared).session(f.handle).unwrap().dirty);
    assert_eq!(lock(&f.shared).session(f.handle).unwrap().entry, entry);
    assert_eq!(
        lock(&f.shared).session(f.handle).unwrap().bytes.len(),
        original.len()
    );
    for phase in [Phase::Write, Phase::ManifestWrite] {
        assert_eq!(
            f.native.counters.snapshot()[phase as usize].operations,
            before[phase as usize].operations
        );
    }
    assert_eq!(
        tkfs::content::write::WriteJournal::open(dir.path())
            .unwrap()
            .pending_count(),
        0
    );
    assert_eq!(
        fs::read_dir(dir.path().join("staging")).map_or(0, |d| d.count()),
        0
    );
    assert_eq!(
        runtime::content_reads::read_mount_vec(&f.shared, f.handle, CHUNK_SIZE, 17, wait)
            .unwrap()
            .unwrap(),
        original[CHUNK_SIZE as usize..CHUNK_SIZE as usize + 17]
    );
    f.native
        .install(key, chunk, &current_reads::context())
        .unwrap();
    assert_eq!(
        content_writes::write(&f.shared, f.handle, 5, b"EDIT", false, false, wait).unwrap(),
        Some(4)
    );
    let second = &original[CHUNK_SIZE as usize..2 * CHUNK_SIZE as usize];
    let second_key = ObjectKey {
        kind: ObjectKind::Bytes,
        digest: Digest::bytes(second),
    };
    let mut damaged = second.to_vec();
    damaged[100] ^= 0xff;
    fs::write(
        dir.path()
            .join("objects")
            .join(second_key.digest.to_string()),
        damaged,
    )
    .unwrap();
    let error = content_writes::write(
        &f.shared,
        f.handle,
        CHUNK_SIZE + 5,
        b"NOPE",
        false,
        false,
        wait,
    )
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("CORRUPT_CONTENT"),
        "{error:#}"
    );
    assert!(
        lock(&f.shared).session(f.handle).unwrap().dirty,
        "earlier accepted bytes stay dirty"
    );
    f.native
        .install(second_key, second, &current_reads::context())
        .unwrap();
    assert!(content_writes::flush(&f.shared, f.handle, wait).unwrap());
    let mut expected = original;
    expected[5..9].copy_from_slice(b"EDIT");
    assert_eq!(f.read(), expected);
}

fn never() -> bool {
    false
}

#[test]
fn a_read_waits_while_verified_chunks_in_its_range_arrive() {
    use tkfs::model::content::CHUNK_SIZE;
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let f = current_reads::CurrentRead::new(dir.path(), bytes(42, 2 * 1024 * 1024));
    f.remove_chunks(12);
    let wait = ContentWait {
        stall: Duration::from_millis(400),
        bound: Duration::from_secs(30),
        slots: None,
        cancelled: never,
    };
    f.feed(12, 1, Duration::from_millis(150));
    let began = Instant::now();
    let data = f.read(0, 12 * CHUNK_SIZE as usize, wait).unwrap();
    let waited = began.elapsed();
    f.finish();
    assert!(
        waited > Duration::from_millis(1500),
        "the range waited through verified progress ({waited:?})"
    );
    assert_eq!(data, &f.data[..12 * CHUNK_SIZE as usize]);
    assert!(f.pending().is_empty());
}

#[test]
fn a_stalled_fenced_or_crowded_current_range_releases_its_waiter() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let f = current_reads::CurrentRead::new(dir.path(), bytes(43, 300 * 1024));
    f.remove_chunks(1);
    let stall = ContentWait {
        stall: Duration::from_millis(300),
        bound: Duration::from_secs(30),
        slots: None,
        cancelled: never,
    };
    let engine = f.shared.clone();
    let h = f.handle;
    let reader =
        std::thread::spawn(move || runtime::content_reads::read_mount_vec(&engine, h, 0, 1, stall));
    let demand = f.wait_pending();
    assert_eq!(demand.demand.reference, f.reference_id);
    let began = Instant::now();
    let error = reader.join().unwrap().unwrap_err();
    assert!(began.elapsed() < Duration::from_secs(5));
    assert!(
        format!("{error:#}").starts_with("CONTENT_UNAVAILABLE"),
        "{error:#}"
    );
    assert!(f.pending().is_empty());
    struct Unfence;
    impl Drop for Unfence {
        fn drop(&mut self) {
            tkfs::worker::mount_ctl::adapter::set_fenced(false);
        }
    }
    tkfs::worker::mount_ctl::adapter::set_fenced(true);
    let reset = Unfence;
    let fenced = ContentWait {
        stall: Duration::from_secs(30),
        bound: Duration::from_secs(30),
        slots: None,
        cancelled: tkfs::worker::mount_ctl::adapter::fenced,
    };
    let began = Instant::now();
    let error = f.read(0, 1, fenced).unwrap_err();
    drop(reset);
    assert!(began.elapsed() < Duration::from_secs(2));
    assert!(
        format!("{error:#}").contains("CONTENT_CANCELLED"),
        "{error:#}"
    );
    assert!(f.pending().is_empty());
    static ONE: WaitSlots = WaitSlots::new(1);
    let crowded = ContentWait {
        stall: Duration::from_millis(1500),
        bound: Duration::from_secs(30),
        slots: Some(&ONE),
        cancelled: never,
    };
    let second = current_reads::CurrentRead::new(&dir.path().join("second"), bytes(44, 300 * 1024));
    second.remove_chunks(1);
    let engine = f.shared.clone();
    let h = f.handle;
    let first = std::thread::spawn(move || {
        runtime::content_reads::read_mount_vec(&engine, h, 0, 1, crowded)
    });
    f.wait_pending();
    assert_eq!(ONE.used(), 1);
    let began = Instant::now();
    let error = second.read(0, 1, crowded).unwrap_err();
    let elapsed = began.elapsed();
    assert!(first.join().unwrap().is_err());
    assert!(elapsed < Duration::from_secs(1));
    assert!(
        format!("{error:#}").contains("CONTENT_RESOURCE_LIMIT"),
        "{error:#}"
    );
    assert_eq!(ONE.used(), 0);
    assert!(f.pending().is_empty());
    assert!(second.pending().is_empty());
}

#[test]
fn shorter_content_wait_bounds_come_only_from_the_environment() {
    let (stall, bound) =
        runtime::content::wait_bounds(Duration::from_secs(60), Duration::from_secs(240));
    // Unset in the test environment unless a scenario sets them: never
    // longer than the defaults either way.
    assert!(stall <= Duration::from_secs(60) && bound <= Duration::from_secs(240));
}

#[cfg(any(windows, target_os = "linux"))]
mod mounted {
    use super::*;
    use tkfs::{model::health::MountControl, worker::mount_ctl::MountController};

    pub struct Mounted {
        pub _dir: tempfile::TempDir,
        pub state: PathBuf,
        pub engine: Shared,
        pub mount: PathBuf,
        pub controller: Arc<MountController>,
    }

    impl Mounted {
        /// `None` when this machine cannot mount.
        pub fn new() -> Option<Self> {
            #[cfg(target_os = "linux")]
            if !Path::new("/dev/fuse").exists() {
                return None;
            }
            let dir = tempfile::tempdir().unwrap();
            let state = dir.path().join("state");
            let engine = Arc::new(Mutex::new(Engine::new(
                Store::initialize(&state, &id(), &id()).unwrap(),
            )));
            let controller = MountController::new(engine.clone(), None);
            let mount = dir.path().join("mnt");
            match controller.attach(&mount) {
                Ok(status) if status.state.is_attached() => Some(Self {
                    _dir: dir,
                    state,
                    engine,
                    mount,
                    controller,
                }),
                Ok(status) => panic!("attach: {status:?}"),
                Err(error)
                    if error.code == "WINFSP_RUNTIME_UNAVAILABLE"
                        || error.code == "FUSE_MOUNT_FAILED" =>
                {
                    eprintln!("skipped: {error}");
                    None
                }
                Err(error) => panic!("attach: {error}"),
            }
        }

        pub fn quiet(&self, timeout: Duration) -> bool {
            wait_until(timeout, || lock(&self.engine).quiet().is_ok())
        }
    }

    impl Drop for Mounted {
        fn drop(&mut self) {
            self.engine.clear_poison();
            let deadline = Instant::now() + Duration::from_secs(10);
            while self.controller.detach(false).is_err() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(50));
            }
            let _ = self.controller.detach(true);
        }
    }
}

#[cfg(any(windows, target_os = "linux"))]
#[test]
#[ignore = "explicit actual WinFsp/FUSE local filesystem scenario"]
fn small_files_copied_through_the_mount_are_all_committed() {
    let _serial = serial();
    let m = mounted::Mounted::new().expect("actual WinFsp/FUSE mount unavailable");
    let mut written = BTreeMap::new();
    for d in 0..10 {
        fs::create_dir(m.mount.join(format!("d{d:02}"))).unwrap();
    }
    for i in 0..300u64 {
        let relative = format!("d{:02}/f{i:04}.bin", i % 10);
        let data = bytes(1000 + i, 1024 + (i as usize * 7919) % (63 * 1024));
        fs::write(m.mount.join(&relative), &data).unwrap();
        written.insert(relative, data);
    }
    // Read back through the mount at once (unsaved bytes are served too).
    for (relative, data) in &written {
        assert_eq!(
            &fs::read(m.mount.join(relative)).unwrap(),
            data,
            "{relative}"
        );
    }
    assert!(m.quiet(Duration::from_secs(60)), "every file is committed");
    let e = lock(&m.engine);
    for (relative, data) in &written {
        assert_eq!(&content(&e, relative), data, "{relative}");
    }
    assert!(pending_rows(&e).is_empty());
    no_conflicts(&e);
    drop(e);
    assert!(!m.state.join("objects").read_dir().unwrap().any(|entry| {
        entry
            .unwrap()
            .path()
            .extension()
            .is_some_and(|ext| ext == "part")
    }));
}

#[cfg(any(windows, target_os = "linux"))]
#[test]
#[ignore = "explicit actual WinFsp/FUSE local filesystem scenario"]
fn the_atomic_save_pattern_through_the_mount_keeps_one_history() {
    let _serial = serial();
    let m = mounted::Mounted::new().expect("actual WinFsp/FUSE mount unavailable");
    let target = m.mount.join("project.sln");
    for round in 0..30u64 {
        let data = bytes(round, 2000 + round as usize * 31);
        let temp = m.mount.join(format!("project.sln.tmp{round}"));
        fs::write(&temp, &data).unwrap();
        fs::rename(&temp, &target).unwrap();
        assert_eq!(fs::read(&target).unwrap(), data, "round {round}");
        // And a short-lived file deleted right after its close.
        let scratch = m.mount.join(format!("scratch{round}.tmp"));
        fs::write(&scratch, b"short lived").unwrap();
        fs::remove_file(&scratch).unwrap();
    }
    assert!(m.quiet(Duration::from_secs(30)));
    let e = lock(&m.engine);
    assert_eq!(content(&e, "project.sln"), bytes(29, 2000 + 29 * 31));
    for round in 0..30 {
        assert!(e.store.lookup(&format!("scratch{round}.tmp")).is_err());
    }
    no_conflicts(&e);
}

#[cfg(any(windows, target_os = "linux"))]
#[test]
#[ignore = "explicit actual WinFsp/FUSE local filesystem scenario"]
fn a_mounted_read_of_content_that_cannot_arrive_fails_meaningfully() {
    let _serial = serial();
    let m = mounted::Mounted::new().expect("actual WinFsp/FUSE mount unavailable");
    let data = bytes(77, 200 * 1024);
    current_reads::committed(&m.engine, "remote.bin", &data);
    let fixture = current_reads::CurrentRead::from_shared(m.engine.clone(), "remote.bin", data);
    fixture.remove_chunks(1);
    // SAFETY: the content-wait bounds are read by the read callback; the
    // tests that mount run one at a time.
    unsafe { std::env::set_var("TKFS_CONTENT_STALL_MS", "300") };
    let started = Instant::now();
    let error = fs::read(m.mount.join("remote.bin")).unwrap_err();
    unsafe { std::env::remove_var("TKFS_CONTENT_STALL_MS") };
    assert!(started.elapsed() < Duration::from_secs(20));
    #[cfg(windows)]
    {
        // ERROR_NOT_READY: "The device is not ready.", not an invalid
        // argument (ERROR_FILE_OFFLINE, which the C runtime maps to EINVAL).
        assert_eq!(error.raw_os_error(), Some(21), "{error}");
    }
    #[cfg(target_os = "linux")]
    {
        assert_eq!(error.raw_os_error(), Some(libc::EIO), "{error}");
    }
    assert_ne!(error.kind(), std::io::ErrorKind::InvalidInput, "{error}");
}

#[cfg(any(windows, target_os = "linux"))]
#[test]
#[ignore = "explicit actual WinFsp/FUSE local filesystem scenario"]
fn a_mounted_read_waits_for_a_download_and_returns_its_bytes() {
    let _serial = serial();
    let m = mounted::Mounted::new().expect("actual WinFsp/FUSE mount unavailable");
    let data = bytes(78, 6 * 1024 * 1024);
    current_reads::committed(&m.engine, "downloading.bin", &data);
    let fixture =
        current_reads::CurrentRead::from_shared(m.engine.clone(), "downloading.bin", data.clone());
    let count = data
        .len()
        .div_ceil(tkfs::model::content::CHUNK_SIZE as usize);
    fixture.remove_chunks(count);
    unsafe { std::env::set_var("TKFS_CONTENT_STALL_MS", "1500") };
    fixture.feed(count, 5, Duration::from_millis(200));
    let read = fs::read(m.mount.join("downloading.bin"));
    unsafe { std::env::remove_var("TKFS_CONTENT_STALL_MS") };
    fixture.finish();
    assert_eq!(read.unwrap(), data);
    drop(fixture);
    // Other operations never waited for it.
    let unrelated: BTreeSet<_> = fs::read_dir(&m.mount)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert!(unrelated.contains(std::ffi::OsStr::new("downloading.bin")));
}

#[cfg(any(windows, target_os = "linux"))]
#[test]
#[ignore = "explicit actual WinFsp/FUSE local filesystem scenario"]
fn a_graceful_detach_right_after_writes_waits_for_their_saves() {
    let _serial = serial();
    let m = mounted::Mounted::new().expect("actual WinFsp/FUSE mount unavailable");
    use tkfs::model::health::{MountControl, MountState};
    let mut written = BTreeMap::new();
    for i in 0..40u64 {
        let relative = format!("w{i:02}.bin");
        let data = bytes(5000 + i, 4096 + i as usize * 1000);
        fs::write(m.mount.join(&relative), &data).unwrap();
        written.insert(relative, data);
    }
    // At once: a checkout waits for the files still being saved.
    {
        let e = lock(&m.engine);
        if e.settling() > 0 {
            assert!(
                format!("{:#}", e.checkout_allowed().unwrap_err()).contains("BUSY_VIEW"),
                "a checkout waits for the saves"
            );
        }
    }
    assert!(
        wait_until(Duration::from_secs(10), || lock(&m.engine)
            .handles
            .is_empty()),
        "the kernel closed every handle"
    );
    // The graceful detach waits for the saves instead of refusing.
    let status = m.controller.detach(false).unwrap();
    assert_eq!(status.state, MountState::Detached);
    let e = lock(&m.engine);
    assert!(
        e.sessions.values().all(|s| !s.dirty),
        "nothing left unsaved"
    );
    for (relative, data) in &written {
        assert_eq!(&content(&e, relative), data, "{relative}");
    }
    assert!(pending_rows(&e).is_empty());
    assert!(e.checkout_allowed().is_ok());
    no_conflicts(&e);
}

#[cfg(any(windows, target_os = "linux"))]
#[test]
#[ignore = "explicit actual WinFsp/FUSE local filesystem scenario"]
fn many_concurrent_writers_never_stall_the_mount() {
    let _serial = serial();
    tkfs::util::log::set_min_level(tkfs::util::log::Level::Debug);
    let m = mounted::Mounted::new().expect("actual WinFsp/FUSE mount unavailable");
    // A build: eight writers at once, small files, each read back and some
    // renamed, while another thread keeps listing and stating.
    let mount = m.mount.clone();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let prober = {
        let (mount, stop) = (mount.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut worst = Duration::ZERO;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let started = Instant::now();
                let _ = fs::read_dir(&mount).map(|d| d.count());
                let _ = fs::metadata(mount.join("w0"));
                worst = worst.max(started.elapsed());
                std::thread::sleep(Duration::from_millis(5));
            }
            worst
        })
    };
    let writers: Vec<_> = (0..8u64)
        .map(|w| {
            let mount = mount.clone();
            std::thread::spawn(move || {
                let dir = mount.join(format!("w{w}"));
                fs::create_dir(&dir).unwrap();
                let mut worst = Duration::ZERO;
                let mut written = BTreeMap::new();
                for i in 0..150u64 {
                    let data = bytes(w * 1000 + i, 512 + (i as usize * 1531) % 40_000);
                    let started = Instant::now();
                    let path = dir.join(format!("f{i:03}.obj"));
                    fs::write(&path, &data).unwrap();
                    assert_eq!(fs::read(&path).unwrap(), data);
                    let name = if i % 5 == 0 {
                        let renamed = format!("w{w}/r{i:03}.obj");
                        fs::rename(&path, mount.join(&renamed)).unwrap();
                        renamed
                    } else {
                        format!("w{w}/f{i:03}.obj")
                    };
                    worst = worst.max(started.elapsed());
                    written.insert(name, data);
                }
                (worst, written)
            })
        })
        .collect();
    let mut written = BTreeMap::new();
    let mut worst = Duration::ZERO;
    for writer in writers {
        let (slowest, files) = writer.join().unwrap();
        worst = worst.max(slowest);
        written.extend(files);
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let probe_worst = prober.join().unwrap();
    assert!(
        worst < Duration::from_secs(5),
        "a write, read back and rename took {worst:?}"
    );
    assert!(
        probe_worst < Duration::from_secs(5),
        "a listing or stat took {probe_worst:?}"
    );
    assert!(m.quiet(Duration::from_secs(60)), "every file is committed");
    let e = lock(&m.engine);
    for (relative, data) in &written {
        assert_eq!(&content(&e, relative), data, "{relative}");
    }
    assert!(pending_rows(&e).is_empty());
    no_conflicts(&e);
}

#[cfg(any(windows, target_os = "linux"))]
#[test]
#[ignore = "explicit actual WinFsp/FUSE local filesystem scenario"]
fn a_file_held_open_after_its_create_is_listed_renamed_and_deleted_through_the_mount() {
    use std::io::Write;
    let _serial = serial();
    let m = mounted::Mounted::new().expect("actual WinFsp/FUSE mount unavailable");
    let held_path = m.mount.join("held.txt");
    let mut held = fs::File::create(&held_path).unwrap();
    held.write_all(b"first part").unwrap();
    // Typed empty creation is committed before the first buffered write.
    assert!(
        lock(&m.engine)
            .store
            .lookup("held.txt")
            .unwrap()
            .content_ref
            .is_some()
    );
    assert!(fs::metadata(&held_path).unwrap().is_file());
    let listed: BTreeSet<_> = fs::read_dir(&m.mount)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert!(listed.contains(std::ffi::OsStr::new("held.txt")));
    // A second exclusive create of the name fails.
    let again = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(m.mount.join("held.txt"));
    assert_eq!(again.unwrap_err().kind(), std::io::ErrorKind::AlreadyExists);
    // Rename retains the same current private owner and later writes.
    fs::rename(&held_path, m.mount.join("renamed.txt")).unwrap();
    {
        let e = lock(&m.engine);
        assert!(e.store.lookup("renamed.txt").is_ok());
        assert!(e.lookup_visible("held.txt").is_err());
    }
    held.write_all(b", second part").unwrap();
    drop(held);
    // Both adapters support unlink while the native descriptor stays open.
    let gone_path = m.mount.join("gone.txt");
    let mut gone = fs::File::create(&gone_path).unwrap();
    gone.write_all(b"short lived").unwrap();
    fs::remove_file(&gone_path).unwrap();
    gone.write_all(b", retained after unlink").unwrap();
    drop(gone);
    assert!(m.quiet(Duration::from_secs(30)), "everything is committed");
    assert!(!gone_path.exists());
    let e = lock(&m.engine);
    assert_eq!(content(&e, "renamed.txt"), b"first part, second part");
    assert!(e.lookup_visible("gone.txt").is_err());
    assert!(e.lookup_visible("held.txt").is_err());
    assert!(pending_rows(&e).is_empty());
    no_conflicts(&e);
}

#[cfg(any(windows, target_os = "linux"))]
#[test]
#[ignore = "explicit actual WinFsp/FUSE local filesystem scenario"]
fn an_fsync_during_a_burst_of_closes_commits_before_it_returns() {
    use std::io::Write;
    let _serial = serial();
    let m = mounted::Mounted::new().expect("actual WinFsp/FUSE mount unavailable");
    let mut files = Vec::new();
    let mut written = BTreeMap::new();
    for i in 0..8u64 {
        let path = format!("burst{i:02}.bin");
        let data = bytes(9100 + i, 2048);
        let mut file = fs::File::create(m.mount.join(&path)).unwrap();
        file.write_all(&data).unwrap();
        files.push(file);
        written.insert(path, data);
    }
    let main_bytes = bytes(9200, 4096);
    let mut main = fs::File::create(m.mount.join("explicit-fsync")).unwrap();
    main.write_all(&main_bytes).unwrap();
    fault::configure(Some(FaultConfig {
        ms: Some(300),
        count: Some(1),
        ..FaultConfig::new("content.write.before_checkpoint", FaultAction::Hang)
    }));
    let reset = Disarm;
    let closes: Vec<_> = files
        .into_iter()
        .map(|file| std::thread::spawn(move || drop(file)))
        .collect();
    assert!(wait_until(Duration::from_secs(5), || fault::fired() > 0));
    main.sync_all().unwrap();
    // Inspect canonical provider bytes while the fsync handle remains open.
    assert_eq!(content(&lock(&m.engine), "explicit-fsync"), main_bytes);
    drop(main);
    for close in closes {
        close.join().unwrap();
    }
    drop(reset);
    assert!(
        m.quiet(Duration::from_secs(30)),
        "all closes reach current durable publication"
    );
    let e = lock(&m.engine);
    for (path, data) in written {
        assert_eq!(content(&e, &path), data);
    }
    assert!(pending_rows(&e).is_empty());
    no_conflicts(&e);
}
