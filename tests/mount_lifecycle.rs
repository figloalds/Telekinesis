//! WP1.5: the MountController and its adapters (REVAMP_PLAN §4.3), in process.
//!
//! Real mounts: WinFsp on Windows, `/dev/fuse` and `fusermount3` on Linux.
//! Scenarios that need a real mount pass without checking anything when the
//! platform has none (no `/dev/fuse`, or WinFsp not installed). The adapter is
//! one per process (one worker, one engine, one mount), so every scenario runs
//! under `serial()`.
#![cfg(any(windows, target_os = "linux"))]

use serde_json::json;
use std::{
    fs,
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};
use tkfs::{
    core::{Store, id},
    model::{
        errors::{ErrorClass, ErrorWire},
        health::{MountControl, MountIntent, MountState, MountStatus},
        ipc::WorkerRecord,
    },
    runtime::{Engine, Shared},
    util::fault::{self, FaultAction, FaultConfig},
    worker::mount_ctl::MountController,
};

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|poison| poison.into_inner())
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
        std::thread::sleep(Duration::from_millis(20));
    }
}

struct Fixture {
    dir: tempfile::TempDir,
    state: PathBuf,
    engine: Shared,
    mount: PathBuf,
    controller: Arc<MountController>,
}

impl Fixture {
    fn new(previous: Option<WorkerRecord>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state");
        let engine = Arc::new(Mutex::new(Engine::new(
            Store::initialize(&state, &id(), &id()).unwrap(),
        )));
        let controller = MountController::new(engine.clone(), previous);
        Self {
            mount: dir.path().join("mnt"),
            dir,
            state,
            engine,
            controller,
        }
    }

    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    fn identity(&self) -> (String, String) {
        let e = self.engine.lock().unwrap();
        (e.store.repo.clone(), e.store.device.clone())
    }

    /// Attaches at `self.mount`; `false` when this machine cannot mount.
    fn attach(&self) -> bool {
        match self.controller.attach(&self.mount) {
            Ok(status) => {
                assert!(status.state.is_attached(), "{status:?}");
                assert_eq!(status.intent, MountIntent::Attach);
                assert_eq!(status.path.as_deref(), Some(self.mount.as_path()));
                let e = self.engine.lock().unwrap();
                assert!(e.mounted && !e.switching);
                true
            }
            Err(error) if unsupported(&error) => {
                eprintln!("skipped: {error}");
                false
            }
            Err(error) => panic!("attach: {error}"),
        }
    }

    /// Graceful detach, tolerating a transient handle (antivirus, indexer)
    /// for a few seconds.
    fn detach(&self) -> MountStatus {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            match self.controller.detach(false) {
                Ok(status) => {
                    assert_eq!(status.state, MountState::Detached);
                    assert_eq!(status.intent, MountIntent::Detach);
                    self.assert_unfenced();
                    return status;
                }
                Err(error) if error.code == "BUSY_VIEW" && Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(error) => panic!("detach: {error}"),
            }
        }
    }

    fn assert_unfenced(&self) {
        let e = self.engine.lock().unwrap();
        assert!(!e.switching, "the fence must be lifted after a detach");
        assert!(!e.mounted);
    }

    fn pending_saves(&self) -> i64 {
        self.engine
            .lock()
            .unwrap()
            .store
            .db
            .query_row("SELECT count(*) FROM pending_saves", [], |r| r.get(0))
            .unwrap()
    }

    fn health(&self) -> Option<String> {
        self.engine.lock().unwrap().health.clone()
    }

    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    fn record(&self, instance: &str) -> WorkerRecord {
        let (repo, device) = self.identity();
        WorkerRecord {
            format: 1,
            pid: 1,
            instance: instance.into(),
            address: "ws://127.0.0.1:1".into(),
            version: "test".into(),
            ipc: 3,
            repo,
            device,
            started_at_ms: 1,
            process_start: 1,
            executable: PathBuf::from("tkfs"),
            supervised: true,
            token: None,
        }
    }
}

impl Drop for Fixture {
    /// A failed scenario must not leave its mount to the next one.
    fn drop(&mut self) {
        self.engine.clear_poison();
        let _ = self.controller.detach(true);
    }
}

/// No mount support on this machine (the scenario then checks nothing).
fn unsupported(error: &ErrorWire) -> bool {
    error.code == "WINFSP_RUNTIME_UNAVAILABLE"
        || (cfg!(target_os = "linux") && error.code == "FUSE_MOUNT_FAILED")
}

fn mount_supported() -> bool {
    #[cfg(windows)]
    {
        true
    }
    #[cfg(target_os = "linux")]
    {
        std::path::Path::new("/dev/fuse").exists()
            && std::process::Command::new("fusermount3")
                .arg("-V")
                .output()
                .is_ok()
    }
}

fn arm(point: &str) {
    fault::configure(Some(FaultConfig {
        count: Some(1),
        ..FaultConfig::new(point, FaultAction::Fail)
    }));
}

/// Disarms the fault point even when the scenario panics.
struct Disarm;
impl Drop for Disarm {
    fn drop(&mut self) {
        fault::configure(None);
    }
}

#[test]
fn attach_detach_attach_in_one_process() {
    let _serial = serial();
    if !mount_supported() {
        return;
    }
    let f = Fixture::new(None);
    if !f.attach() {
        return;
    }
    fs::write(f.mount.join("a.txt"), b"first attach").unwrap();
    let generation = f.controller.generation();
    let detached = f.detach();
    assert_eq!(detached.path.as_deref(), Some(f.mount.as_path()));
    assert!(!f.mount.exists(), "the mount point is gone after a detach");
    assert!(f.controller.generation() > generation);
    // A fresh native instance in the same process (WinFsp used to refuse with
    // ONE_MOUNT_PER_RUNTIME, and both adapters left the engine fenced).
    assert!(f.attach());
    assert_eq!(fs::read(f.mount.join("a.txt")).unwrap(), b"first attach");
    fs::write(f.mount.join("b.txt"), b"second attach").unwrap();
    f.detach();
    assert!(f.attach());
    assert_eq!(fs::read(f.mount.join("b.txt")).unwrap(), b"second attach");
    f.detach();
    assert_eq!(f.health(), None);
}

#[test]
fn graceful_detach_with_an_open_handle_is_busy_and_stays_attached() {
    let _serial = serial();
    if !mount_supported() {
        return;
    }
    let f = Fixture::new(None);
    if !f.attach() {
        return;
    }
    let mut file = fs::File::create(f.mount.join("open.txt")).unwrap();
    file.write_all(b"partial").unwrap();
    let error = f.controller.detach(false).unwrap_err();
    assert_eq!(error.code, "BUSY_VIEW", "{error}");
    assert_eq!(error.class, ErrorClass::Contention);
    let status = f.controller.status();
    assert!(status.state.is_attached(), "{status:?}");
    assert_eq!(status.intent, MountIntent::Attach);
    {
        let e = f.engine.lock().unwrap();
        assert!(
            e.mounted && !e.switching,
            "a refused detach lifts its fence"
        );
    }
    // Still served after the refusal.
    file.write_all(b" and more").unwrap();
    drop(file);
    f.detach();
    assert_eq!(f.pending_saves(), 0);
    assert!(f.attach());
    assert_eq!(
        fs::read(f.mount.join("open.txt")).unwrap(),
        b"partial and more"
    );
    f.detach();
}

#[test]
fn force_detach_persists_dirty_sessions_and_reattach_shows_them() {
    let _serial = serial();
    if !mount_supported() {
        return;
    }
    let f = Fixture::new(None);
    if !f.attach() {
        return;
    }
    let bytes = b"dirty bytes kept by a force detach";
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(f.mount.join("dirty.txt"))
        .unwrap();
    file.write_all(bytes).unwrap();
    assert!(wait_until(Duration::from_secs(5), || {
        f.engine.lock().unwrap().sessions.values().any(|s| s.dirty)
    }));
    let status = f.controller.detach(true).unwrap();
    assert_eq!(status.state, MountState::Detached);
    assert!(
        f.pending_saves() >= 1,
        "the dirty session is in pending_saves"
    );
    {
        let e = f.engine.lock().unwrap();
        assert!(!e.switching && !e.mounted);
        assert!(e.handles.is_empty(), "orphaned handles are dropped");
        assert!(
            e.sessions.values().any(|s| s.dirty && s.references == 0),
            "the dirty session is kept for the pending-save retry"
        );
    }
    // The application's handle died with the mount.
    drop(file);
    assert!(f.attach());
    assert!(wait_until(Duration::from_secs(10), || {
        fs::read(f.mount.join("dirty.txt")).ok().as_deref() == Some(bytes.as_slice())
    }));
    // The pending-save retry (the worker's recovery thread) commits it.
    f.engine.lock().unwrap().retry_pending().unwrap();
    assert!(wait_until(Duration::from_secs(10), || f.pending_saves() == 0));
    f.detach();
    assert!(f.attach());
    assert_eq!(fs::read(f.mount.join("dirty.txt")).unwrap(), bytes);
    f.detach();
}

#[test]
fn foreign_mount_untouched() {
    let _serial = serial();
    let f = Fixture::new(None);
    let foreign = f.dir.path().join("foreign");
    fs::create_dir_all(foreign.join("nested")).unwrap();
    fs::write(foreign.join("keep.txt"), b"foreign data").unwrap();
    fs::write(foreign.join("nested/inner.txt"), b"inner").unwrap();
    for attempt in 1..=2 {
        let error = f.controller.attach(&foreign).unwrap_err();
        if unsupported(&error) {
            assert_eq!(error.class, ErrorClass::NeedsUserAction);
            return;
        }
        assert_eq!(error.code, "MOUNT_PATH_MUST_NOT_EXIST", "{error}");
        assert_eq!(error.class, ErrorClass::NeedsUserAction);
        let status = f.controller.status();
        assert_eq!(status.intent, MountIntent::Attach);
        assert_eq!(status.path.as_deref(), Some(foreign.as_path()));
        assert_eq!(
            status.state,
            MountState::AttachFailed {
                error: error.clone(),
                attempt,
                next_retry_at_ms: None,
            }
        );
    }
    assert_eq!(f.health(), None, "mount failures never write engine health");
    {
        let e = f.engine.lock().unwrap();
        assert!(!e.mounted && !e.switching);
    }
    assert_eq!(fs::read(foreign.join("keep.txt")).unwrap(), b"foreign data");
    assert_eq!(
        fs::read(foreign.join("nested/inner.txt")).unwrap(),
        b"inner"
    );
    assert_eq!(fs::read_dir(&foreign).unwrap().count(), 2);
    // An empty directory is not proven ours either.
    let empty = f.dir.path().join("empty");
    fs::create_dir(&empty).unwrap();
    let error = f.controller.attach(&empty).unwrap_err();
    assert_eq!(error.code, "MOUNT_PATH_MUST_NOT_EXIST", "{error}");
    assert!(empty.is_dir());
    #[cfg(target_os = "linux")]
    linux::stale_mounts_not_proven_ours_are_untouched(&f);
}

#[test]
fn a_path_overlapping_the_state_is_refused() {
    let _serial = serial();
    let f = Fixture::new(None);
    let inside = f.state.join("mnt");
    let error = f.controller.attach(&inside).unwrap_err();
    assert_eq!(error.code, "MOUNT_OVERLAPS_STATE", "{error}");
    assert_eq!(error.class, ErrorClass::NeedsUserAction);
    assert!(!inside.exists(), "nothing is created inside the state");
    let error = f.controller.attach(f.dir.path()).unwrap_err();
    assert_eq!(error.code, "MOUNT_OVERLAPS_STATE", "{error}");
    assert!(matches!(
        f.controller.status().state,
        MountState::AttachFailed { attempt: 2, .. }
    ));
    assert_eq!(f.health(), None);
}

#[test]
fn a_second_engine_cannot_take_an_attached_mount() {
    let _serial = serial();
    if !mount_supported() {
        return;
    }
    let first = Fixture::new(None);
    if !first.attach() {
        return;
    }
    let second = Fixture::new(None);
    let error = second.controller.attach(&second.mount).unwrap_err();
    assert_eq!(error.code, "ONE_MOUNT_PER_RUNTIME", "{error}");
    assert_eq!(error.class, ErrorClass::Transient);
    match second.controller.status().state {
        MountState::AttachFailed {
            attempt: 1,
            next_retry_at_ms: Some(at),
            ..
        } => assert!(at > tkfs::util::time::now_ms()),
        other => panic!("expected a transient AttachFailed, got {other:?}"),
    }
    assert!(!second.mount.exists());
    assert!(first.controller.status().state.is_attached());
    first.detach();
    // Rebinding is allowed once nothing is attached.
    assert!(second.attach());
    second.detach();
}

#[test]
fn close_failure_is_mount_scoped_and_stop_still_works() {
    let _serial = serial();
    if !mount_supported() {
        return;
    }
    let f = Fixture::new(None);
    if !f.attach() {
        return;
    }
    let before = f.controller.mount_errors();
    let _disarm = Disarm;
    arm("fs.close");
    fs::write(f.mount.join("closed.txt"), b"closed bytes").unwrap();
    assert!(
        wait_until(Duration::from_secs(10), || f.controller.mount_errors()
            > before),
        "the close failure is counted as a mount error"
    );
    fault::configure(None);
    assert_eq!(f.health(), None, "no store health change");
    assert_eq!(
        fs::read(f.mount.join("closed.txt")).unwrap(),
        b"closed bytes"
    );
    // The legacy `stop` op path (quiet, then a graceful detach) still works.
    legacy_stop(&f);
    // The controller reports what the adapter did.
    assert_eq!(
        f.controller.detach(false).unwrap().state,
        MountState::Detached
    );
    assert!(f.attach());
    f.detach();
}

#[test]
fn checkout_remounts_and_a_failed_remount_is_lost() {
    let _serial = serial();
    if !mount_supported() {
        return;
    }
    let f = Fixture::new(None);
    if !f.attach() {
        return;
    }
    fs::write(f.mount.join("kept.txt"), b"on every branch").unwrap();
    // Semantic operations need a quiet engine; the kernel's close may lag.
    assert!(wait_until(Duration::from_secs(10), || f
        .engine
        .lock()
        .unwrap()
        .quiet()
        .is_ok()));
    let fork = f
        .engine
        .lock()
        .unwrap()
        .control("fork", &json!({"op":"branch","name":"other"}));
    fork.unwrap();
    let checkout = |request: &str, name: &str| {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            match tkfs::mount::checkout(&f.engine, request, &json!({"op":"checkout","name":name})) {
                Err(error)
                    if format!("{error:#}").contains("BUSY_VIEW") && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(50));
                }
                result => return result,
            }
        }
    };
    checkout("to-other", "other").unwrap();
    assert!(f.controller.status().state.is_attached());
    {
        let e = f.engine.lock().unwrap();
        assert!(e.mounted && !e.switching);
    }
    assert_eq!(
        fs::read(f.mount.join("kept.txt")).unwrap(),
        b"on every branch"
    );
    let _disarm = Disarm;
    arm("fs.remount");
    let error = checkout("to-main", "main").unwrap_err();
    fault::configure(None);
    assert!(
        format!("{error:#}").starts_with("REMOUNT_FAILED"),
        "{error:#}"
    );
    match f.controller.status().state {
        MountState::Lost { error } => assert_eq!(error.code, "REMOUNT_FAILED"),
        other => panic!("expected Lost, got {other:?}"),
    }
    assert_eq!(
        f.health(),
        None,
        "a failed remount never writes engine health"
    );
    f.assert_unfenced();
    f.detach();
    assert!(f.attach());
    assert_eq!(
        fs::read(f.mount.join("kept.txt")).unwrap(),
        b"on every branch"
    );
    f.detach();
}

#[test]
fn status_never_waits_for_a_transition() {
    let _serial = serial();
    if !mount_supported() {
        return;
    }
    let f = Fixture::new(None);
    if !f.attach() {
        return;
    }
    let held = f.engine.lock().unwrap();
    let controller = f.controller.clone();
    let detaching = std::thread::spawn(move || controller.detach(false));
    assert!(wait_until(Duration::from_secs(5), || {
        f.controller.status().state == MountState::Detaching
    }));
    let started = Instant::now();
    for _ in 0..100 {
        let _ = f.controller.status();
    }
    assert!(started.elapsed() < Duration::from_millis(500));
    drop(held);
    let status = detaching.join().unwrap().unwrap();
    assert_eq!(status.state, MountState::Detached);
    f.assert_unfenced();
}

#[test]
fn a_transition_slower_than_the_reply_wait_answers_worker_busy() {
    let _serial = serial();
    if !mount_supported() {
        return;
    }
    let f = Fixture::new(None);
    if !f.attach() {
        return;
    }
    f.controller
        .set_reply_wait(Some(Duration::from_millis(300)));
    // The detach cannot fence while the engine is held.
    let held = f.engine.lock().unwrap();
    let started = Instant::now();
    let error = f.controller.detach(false).unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(error.code, "WORKER_BUSY", "{error}");
    assert_eq!(error.class, ErrorClass::Contention);
    assert_eq!(f.controller.status().state, MountState::Detaching);
    drop(held);
    // The transition goes on and finishes on its own.
    assert!(wait_until(Duration::from_secs(15), || f
        .controller
        .status()
        .state
        == MountState::Detached));
    f.controller.set_reply_wait(None);
    f.assert_unfenced();
    assert!(f.attach());
    f.detach();
}

#[test]
fn legacy_start_and_stop_lift_the_fence_and_restart_in_process() {
    let _serial = serial();
    if !mount_supported() {
        return;
    }
    let f = Fixture::new(None);
    if let Err(error) = tkfs::mount::start(f.engine.clone(), &f.mount) {
        let error = ErrorWire::from_error(&error);
        assert!(unsupported(&error), "{error}");
        return;
    }
    fs::write(f.mount.join("legacy.txt"), b"legacy").unwrap();
    legacy_stop(&f);
    tkfs::mount::start(f.engine.clone(), &f.mount).unwrap();
    assert_eq!(fs::read(f.mount.join("legacy.txt")).unwrap(), b"legacy");
    // The controller adopts a mount started outside it...
    let status = f.controller.attach(&f.mount).unwrap();
    assert!(status.state.is_attached());
    // ...and does not trust its own `Attached` once it was stopped outside it.
    legacy_stop(&f);
    assert!(f.attach());
    assert!(tkfs::mount::attached().is_some());
    assert_eq!(fs::read(f.mount.join("legacy.txt")).unwrap(), b"legacy");
    f.detach();
}

/// The legacy `stop` op path, tolerating a transient handle.
fn legacy_stop(f: &Fixture) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while let Err(error) = tkfs::mount::stop(&f.engine) {
        assert!(
            format!("{error:#}").contains("BUSY_VIEW") && Instant::now() < deadline,
            "{error:#}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    f.assert_unfenced();
}

#[cfg(target_os = "linux")]
#[test]
fn dispatch_death_is_lost_not_engine_health() {
    let _serial = serial();
    if !mount_supported() {
        return;
    }
    let f = Fixture::new(None);
    if !f.attach() {
        return;
    }
    let _disarm = Disarm;
    arm("fs.dispatch");
    let _ = fs::metadata(f.mount.join("probe"));
    assert!(wait_until(Duration::from_secs(10), || matches!(
        f.controller.status().state,
        MountState::Lost { .. }
    )));
    fault::configure(None);
    match f.controller.status().state {
        MountState::Lost { error } => assert_eq!(error.code, "FUSE_DISPATCH_FAILED"),
        other => panic!("expected Lost, got {other:?}"),
    }
    assert!(wait_until(Duration::from_secs(5), || !f
        .engine
        .lock()
        .unwrap()
        .mounted));
    assert_eq!(f.health(), None);
    // A lost mount detaches (stale mount point unmounted and removed) and
    // re-attaches.
    f.detach();
    assert!(linux::mounts_at(&f.mount).is_empty());
    assert!(!f.mount.exists());
    assert!(f.attach());
    fs::write(f.mount.join("again.txt"), b"again").unwrap();
    f.detach();
}

#[cfg(target_os = "linux")]
#[test]
fn dispatch_death_retains_dirty_sessions() {
    let _serial = serial();
    if !mount_supported() {
        return;
    }
    let f = Fixture::new(None);
    if !f.attach() {
        return;
    }
    let bytes = b"unsaved when the dispatch thread died";
    let mut file = fs::File::create(f.mount.join("dirty.txt")).unwrap();
    file.write_all(bytes).unwrap();
    assert!(wait_until(Duration::from_secs(5), || {
        f.engine.lock().unwrap().sessions.values().any(|s| s.dirty)
    }));
    assert_eq!(f.pending_saves(), 0);
    let _disarm = Disarm;
    arm("fs.dispatch");
    let _ = fs::metadata(f.mount.join("probe"));
    assert!(wait_until(Duration::from_secs(10), || matches!(
        f.controller.status().state,
        MountState::Lost { .. }
    )));
    fault::configure(None);
    // In `pending_saves` before any detach (invariant 8).
    assert!(f.pending_saves() >= 1, "the dirty session is retained");
    assert!(!f.engine.lock().unwrap().mounted);
    drop(file);
    f.detach();
    assert!(f.attach());
    assert_eq!(fs::read(f.mount.join("dirty.txt")).unwrap(), bytes);
    f.engine.lock().unwrap().retry_pending().unwrap();
    assert!(wait_until(Duration::from_secs(10), || f.pending_saves() == 0));
    f.detach();
}

#[cfg(target_os = "linux")]
#[test]
fn a_lost_mount_during_a_refused_detach_stays_lost() {
    let _serial = serial();
    if !mount_supported() {
        return;
    }
    let f = Fixture::new(None);
    if !f.attach() {
        return;
    }
    // An open handle makes the graceful detach wait (up to 1 s) for quiet,
    // then refuse.
    let mut file = fs::File::create(f.mount.join("open.txt")).unwrap();
    file.write_all(b"open").unwrap();
    let controller = f.controller.clone();
    let detaching = std::thread::spawn(move || controller.detach(false));
    assert!(wait_until(Duration::from_secs(5), || {
        f.controller.status().state == MountState::Detaching
    }));
    let _disarm = Disarm;
    arm("fs.dispatch");
    let _ = fs::metadata(f.mount.join("probe"));
    let error = detaching.join().unwrap().unwrap_err();
    fault::configure(None);
    assert_eq!(error.code, "BUSY_VIEW", "{error}");
    // Lost, whichever came first: never the `Attached` the detach started from.
    assert!(wait_until(Duration::from_secs(5), || matches!(
        f.controller.status().state,
        MountState::Lost { .. }
    )));
    std::thread::sleep(Duration::from_millis(200));
    match f.controller.status().state {
        MountState::Lost { error } => assert_eq!(error.code, "FUSE_DISPATCH_FAILED"),
        other => panic!("expected Lost, got {other:?}"),
    }
    drop(file);
    f.detach();
    assert!(linux::mounts_at(&f.mount).is_empty());
    assert!(f.attach());
    assert_eq!(fs::read(f.mount.join("open.txt")).unwrap(), b"open");
    f.detach();
}

#[cfg(target_os = "linux")]
#[test]
fn external_unmount_is_lost_and_recovers() {
    let _serial = serial();
    if !mount_supported() {
        return;
    }
    let f = Fixture::new(None);
    if !f.attach() {
        return;
    }
    fs::write(f.mount.join("x.txt"), b"before the external unmount").unwrap();
    let lost = |f: &Fixture| {
        assert!(
            wait_until(Duration::from_secs(5), || matches!(
                f.controller.status().state,
                MountState::Lost { .. }
            )),
            "{:?}",
            f.controller.status()
        );
        match f.controller.status().state {
            MountState::Lost { error } => assert_eq!(error.code, "MOUNT_NOT_RUNNING", "{error}"),
            other => panic!("expected Lost, got {other:?}"),
        }
        assert!(wait_until(Duration::from_secs(5), || !f
            .engine
            .lock()
            .unwrap()
            .mounted));
        assert_eq!(f.health(), None, "a lost mount never writes engine health");
    };
    // `fusermount3 -u` outside the worker: the connection ends (ENODEV).
    linux::external_unmount(&f.mount);
    lost(&f);
    // A detach of the vanished mount does not unmount again; it cleans up.
    f.detach();
    assert!(!f.mount.exists(), "our empty mount point is removed");
    assert!(f.attach());
    assert_eq!(
        fs::read(f.mount.join("x.txt")).unwrap(),
        b"before the external unmount"
    );
    // Again, then attach the same path directly: a real mount, not the
    // stale `Attached`.
    linux::external_unmount(&f.mount);
    lost(&f);
    assert!(f.attach());
    assert_eq!(linux::mounts_at(&f.mount).len(), 1);
    fs::write(f.mount.join("y.txt"), b"in the store").unwrap();
    assert!(f.engine.lock().unwrap().store.lookup("y.txt").is_ok());
    f.detach();
    assert!(linux::mounts_at(&f.mount).is_empty());
}

#[cfg(target_os = "linux")]
#[test]
fn lazy_external_unmount_with_an_open_file_is_not_reported_attached() {
    let _serial = serial();
    if !mount_supported() {
        return;
    }
    let f = Fixture::new(None);
    if !f.attach() {
        return;
    }
    let bytes = b"written before a lazy external unmount";
    let mut held = fs::File::create(f.mount.join("held.txt")).unwrap();
    held.write_all(bytes).unwrap();
    // `fusermount3 -u -z` outside the worker: the mount leaves the table, but
    // the open file keeps the connection (and the dispatch thread) alive.
    assert!(
        std::process::Command::new("fusermount3")
            .args(["-u", "-z", "--"])
            .arg(&f.mount)
            .status()
            .unwrap()
            .success()
    );
    assert!(linux::mounts_at(&f.mount).is_empty());
    assert!(f.controller.status().state.is_attached());
    // Attaching the same path cleans the vanished mount up and mounts afresh,
    // instead of answering the old `Attached`. The bytes survive (the kernel
    // flushes the file at the lazy unmount; a dirty session would be retained).
    assert!(f.attach());
    assert_eq!(linux::mounts_at(&f.mount).len(), 1);
    assert_eq!(fs::read(f.mount.join("held.txt")).unwrap(), bytes);
    drop(held);
    fs::write(f.mount.join("after.txt"), b"in the store").unwrap();
    assert!(f.engine.lock().unwrap().store.lookup("after.txt").is_ok());
    f.detach();
    assert!(linux::mounts_at(&f.mount).is_empty());
}

#[cfg(target_os = "linux")]
#[test]
fn stale_owned_fuse_mount_is_recovered() {
    let _serial = serial();
    if !mount_supported() {
        return;
    }
    // The previous worker of this store crashed with its mount attached.
    let previous = uuid::Uuid::new_v4().to_string();
    let mut f = Fixture::new(None);
    f.controller = MountController::new(f.engine.clone(), Some(f.record(&previous)));
    if !linux::stale_mount(&f.mount, &format!("tkfs-{previous}")) {
        return;
    }
    let current = uuid::Uuid::new_v4().to_string();
    f.controller.set_instance(&current);
    assert!(f.attach(), "the stale owned mount is recovered");
    assert_eq!(
        linux::mounts_at(&f.mount),
        vec![("fuse.tkfs".to_owned(), format!("tkfs-{current}"))]
    );
    fs::write(f.mount.join("recovered.txt"), b"recovered").unwrap();
    assert_eq!(
        fs::read(f.mount.join("recovered.txt")).unwrap(),
        b"recovered"
    );
    f.detach();
    assert!(linux::mounts_at(&f.mount).is_empty());
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::{os::fd::AsRawFd, os::unix::net::UnixStream, path::Path, process::Command};

    /// `(fstype, source)` of every mount at `path`.
    pub fn mounts_at(path: &Path) -> Vec<(String, String)> {
        let text = fs::read_to_string("/proc/self/mountinfo").unwrap();
        text.lines()
            .filter_map(|line| {
                let fields: Vec<_> = line.split_whitespace().collect();
                let separator = fields.iter().position(|field| *field == "-")?;
                (Path::new(&fields[4].replace("\\040", " ")) == path).then(|| {
                    (
                        fields[separator + 1].to_owned(),
                        fields[separator + 2].to_owned(),
                    )
                })
            })
            .collect()
    }

    /// A FUSE mount at `path` whose daemon died before answering anything,
    /// as a crashed worker leaves it. `false` when fusermount3 refuses.
    pub fn stale_mount(path: &Path, source: &str) -> bool {
        fs::create_dir_all(path).unwrap();
        let (parent, child) = UnixStream::pair().unwrap();
        assert_eq!(
            unsafe { libc::fcntl(child.as_raw_fd(), libc::F_SETFD, 0) },
            0
        );
        let status = Command::new("fusermount3")
            .args([
                "-o",
                &format!("rw,nosuid,nodev,fsname={source},subtype=tkfs"),
                "--",
            ])
            .arg(path)
            .env("_FUSE_COMMFD", child.as_raw_fd().to_string())
            .status();
        // Closing both ends releases the device descriptor in flight: the
        // connection aborts and the mount goes stale.
        drop(child);
        drop(parent);
        if !status.is_ok_and(|status| status.success()) {
            let _ = fs::remove_dir(path);
            return false;
        }
        assert!(wait_until(Duration::from_secs(5), || {
            fs::metadata(path).is_err_and(|error| error.raw_os_error() == Some(libc::ENOTCONN))
        }));
        true
    }

    fn unmount(path: &Path) {
        let _ = Command::new("fusermount3")
            .args(["-u", "--"])
            .arg(path)
            .status();
        let _ = fs::remove_dir(path);
    }

    /// `fusermount3 -u` by someone else than the worker (the kernel may still
    /// be releasing a just-closed file for a moment).
    pub fn external_unmount(path: &Path) {
        assert!(
            wait_until(Duration::from_secs(5), || {
                Command::new("fusermount3")
                    .args(["-u", "--"])
                    .arg(path)
                    .status()
                    .is_ok_and(|status| status.success())
            }),
            "fusermount3 -u {}",
            path.display()
        );
        assert!(mounts_at(path).is_empty());
    }

    /// A stale `tkfs` mount is left alone unless the previous `worker.json`
    /// proves it ours (same store, its instance named by the mount source).
    pub fn stale_mounts_not_proven_ours_are_untouched(f: &Fixture) {
        if !mount_supported() {
            return;
        }
        let owner = uuid::Uuid::new_v4().to_string();
        let mut other_store = f.record(&owner);
        other_store.repo = id();
        let previous = [
            None,
            Some(f.record(&uuid::Uuid::new_v4().to_string())),
            Some(other_store),
        ];
        for (case, previous) in previous.into_iter().enumerate() {
            let path = f.dir.path().join(format!("stale-{case}"));
            if !stale_mount(&path, &format!("tkfs-{owner}")) {
                return;
            }
            let controller = MountController::new(f.engine.clone(), previous);
            let error = controller.attach(&path).unwrap_err();
            assert_eq!(
                error.code, "MOUNT_PATH_MUST_NOT_EXIST",
                "case {case}: {error}"
            );
            assert_eq!(error.class, ErrorClass::NeedsUserAction);
            assert_eq!(
                mounts_at(&path),
                vec![("fuse.tkfs".to_owned(), format!("tkfs-{owner}"))],
                "case {case}: the stale mount is untouched"
            );
            unmount(&path);
        }
    }
}
