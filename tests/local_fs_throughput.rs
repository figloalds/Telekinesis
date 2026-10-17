//! WP5.2: the local filesystem path.
//!
//! * Saves off the engine lock (CCR-WP4.INT-5): a save snapshots the
//!   session under the lock, hashes and stores the bytes without it, and
//!   commits with a base check; writes to a staging file wait for its save,
//!   in-memory bytes may change meanwhile (the session then stays dirty);
//!   every failed step retains the session; a crash at every step keeps the
//!   last committed bytes (child processes, `TKFS_FAULT_*`).
//! * The first write to a large file copies it without the engine lock.
//! * Closed files are saved in the background, never racing a delete or a
//!   rename over them.
//! * Reads of content still downloading wait while it progresses
//!   (CCR-WP4.INT-2), end on a stall, a fence or the wait bound, and are
//!   bounded in number.
//! * A file created through the mount commits with its first save, in one
//!   event (deferred creates): visible meanwhile, retained and restored like
//!   any unsaved file, committed before a rename or a delete of it, and a
//!   crash at every step of its first save leaves a consistent store.
//!
//! The mounted scenarios need WinFsp or FUSE and pass without checking when
//! the machine has neither. The mount adapter is one per process and fault
//! points are armed process-wide, so every scenario runs under `serial()`.
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
        engine::{self, SaveStart},
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

/// Bytes of `len` from `seed` (not compressible, distinct per seed).
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

/// Creates `path` with `content` committed; returns its entity.
fn committed_file(e: &mut Engine, path: &str, content: &[u8]) -> String {
    let (h, entry) = e.open(path, Some("file"), true).unwrap();
    e.write(h, 0, content, false, false).unwrap();
    e.close(h).unwrap();
    entry.id
}

/// The committed bytes at `path`.
fn content(e: &Engine, path: &str) -> Vec<u8> {
    let object = e.store.lookup(path).unwrap().content.unwrap();
    e.store.read_object(&object).unwrap()
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

/// The bytes of the staging file a `pending_saves` row names.
fn staged_bytes(root: &Path, record: &str) -> Vec<u8> {
    let record: serde_json::Value = serde_json::from_str(record).unwrap();
    fs::read(root.join("staging").join(record["stage"].as_str().unwrap())).unwrap()
}

fn part_files(root: &Path) -> Vec<PathBuf> {
    fs::read_dir(root.join("objects"))
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "part"))
        .collect()
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

fn started(start: SaveStart) -> runtime::engine::SaveJob {
    match start {
        SaveStart::Started(job) => job,
        SaveStart::Clean => panic!("expected a save, the session is clean"),
        SaveStart::Busy(_) => panic!("expected a save, one runs"),
    }
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

/// Disarms the fault point even when the scenario panics.
struct Disarm;
impl Drop for Disarm {
    fn drop(&mut self) {
        fault::configure(None);
    }
}

fn arm_fail(point: &str) -> Disarm {
    fault::configure(Some(FaultConfig {
        count: Some(1),
        ..FaultConfig::new(point, FaultAction::Fail)
    }));
    Disarm
}

#[test]
fn a_save_stores_a_staging_file_without_the_engine_lock_and_holds_its_writers() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let engine = shared(dir.path());
    let big = bytes(1, 3 * 1024 * 1024);
    let (h, entry) = {
        let mut e = lock(&engine);
        let (h, entry) = e.open("big.bin", Some("file"), true).unwrap();
        e.write(h, 0, &big, false, false).unwrap();
        assert!(e.session(h).unwrap().bytes.is_file(), "staged in a file");
        (h, entry)
    };
    let job = started(lock(&engine).begin_save(&entry.id).unwrap());
    // Phase 2 runs without the lock: the engine serves everything else.
    let other = {
        let mut e = engine
            .try_lock()
            .expect("the engine is free during phase 2");
        let other = committed_file(&mut e, "other.txt", b"saved meanwhile");
        assert!(e.write_blocked(h).is_some(), "writers of the file wait");
        assert!(
            format!("{:#}", e.write(h, 0, b"x", false, false).unwrap_err()).contains("BUSY_VIEW"),
            "an unsynchronized write is refused, never torn into the snapshot"
        );
        other
    };
    let stored = engine::store_snapshot(&job).unwrap();
    assert_eq!(stored.object, hash(&big));
    assert_eq!(stored.size, big.len() as u64);
    {
        let mut e = lock(&engine);
        e.finish_save(job, Ok(stored)).unwrap();
        assert!(e.write_blocked(h).is_none());
        let session = e.session(h).unwrap();
        assert!(!session.dirty && !session.saving);
        assert_eq!(content(&e, "big.bin"), big);
        assert_eq!(content(&e, "other.txt"), b"saved meanwhile");
        assert!(e.store.entry(&other).is_ok());
        e.close(h).unwrap();
        no_conflicts(&e);
        assert!(e.store.conflicts().unwrap().is_empty());
    }
    assert!(part_files(dir.path()).is_empty());
}

#[test]
fn a_session_written_during_its_save_stays_dirty_and_saves_again_in_line() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let engine = shared(dir.path());
    let mut e = lock(&engine);
    let entity = committed_file(&mut e, "notes.txt", b"v1");
    let (h, _) = e.open("notes.txt", None, true).unwrap();
    e.write(h, 0, b"v2: snapshot", false, false).unwrap();
    let job = started(e.begin_save(&entity).unwrap());
    // In-memory bytes are copied: the application may go on writing.
    assert!(e.write_blocked(h).is_none());
    e.write(h, 0, b"v3: after the snapshot", false, false)
        .unwrap();
    // A second save of the same file waits for the first.
    assert!(matches!(e.begin_save(&entity).unwrap(), SaveStart::Busy(_)));
    // The in-lock flush leaves it to the running save.
    e.flush(h).unwrap();
    let seen = e.save_signal().generation();
    drop(e);
    let stored = engine::store_snapshot(&job);
    let mut e = lock(&engine);
    e.finish_save(job, stored).unwrap();
    assert!(e.save_signal().generation() > seen, "the save signal moved");
    assert_eq!(content(&e, "notes.txt"), b"v2: snapshot");
    assert!(e.session(h).unwrap().dirty, "later bytes stay unsaved");
    e.flush(h).unwrap();
    assert!(!e.session(h).unwrap().dirty);
    assert_eq!(content(&e, "notes.txt"), b"v3: after the snapshot");
    e.close(h).unwrap();
    no_conflicts(&e);
}

#[test]
fn every_failed_step_of_a_save_retains_the_file() {
    let _serial = serial();
    for (point, size) in [
        ("engine.save.snapshot", 1000usize),
        ("engine.save.store", 1000),
        ("engine.save.flushed", 1000),
        ("engine.save.installed", 1000),
        ("engine.save.commit", 1000),
        ("engine.save.store", 600 * 1024),
        ("engine.save.flushed", 600 * 1024),
        ("engine.save.installed", 600 * 1024),
        ("engine.save.commit", 600 * 1024),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let engine = shared(dir.path());
        let new = bytes(size as u64, size);
        let (h, entity) = {
            let mut e = lock(&engine);
            let entity = committed_file(&mut e, "f.bin", b"base");
            let (h, _) = e.open("f.bin", None, true).unwrap();
            e.write(h, 0, &new, false, false).unwrap();
            (h, entity)
        };
        let error = {
            let _disarm = arm_fail(point);
            engine::save_entity(&engine, &entity).unwrap_err()
        };
        assert!(
            format!("{error:#}").contains("FAULT_INJECTED"),
            "{point}: {error:#}"
        );
        let e = lock(&engine);
        assert_eq!(content(&e, "f.bin"), b"base", "{point}: nothing committed");
        let session = e.session(h).unwrap();
        assert!(session.dirty && !session.saving, "{point}: still unsaved");
        let rows = pending_rows(&e);
        assert_eq!(rows.len(), 1, "{point}: the session is retained");
        assert_eq!(staged_bytes(dir.path(), &rows[0].1), new, "{point}");
        assert!(part_files(dir.path()).is_empty(), "{point}: no .part left");
        drop(e);
        // The next save commits it and drops the record.
        engine::save_handle(&engine, h).unwrap();
        let mut e = lock(&engine);
        assert_eq!(content(&e, "f.bin"), new, "{point}");
        assert!(pending_rows(&e).is_empty(), "{point}");
        e.close(h).unwrap();
        no_conflicts(&e);
    }
}

#[test]
fn a_retain_during_a_save_is_settled_by_its_commit() {
    let _serial = serial();
    // Not edited after the snapshot: the commit removes the record.
    let dir = tempfile::tempdir().unwrap();
    let engine = shared(dir.path());
    {
        let mut e = lock(&engine);
        let entity = committed_file(&mut e, "a.txt", b"a1");
        let (h, _) = e.open("a.txt", None, true).unwrap();
        e.write(h, 0, b"a2 retained", false, false).unwrap();
        let job = started(e.begin_save(&entity).unwrap());
        // A force detach retains every dirty session meanwhile.
        assert_eq!(e.retain_all_dirty().unwrap(), 1);
        assert_eq!(pending_rows(&e).len(), 1);
        let stored = engine::store_snapshot(&job);
        e.finish_save(job, stored).unwrap();
        assert!(
            pending_rows(&e).is_empty(),
            "the record goes with the commit"
        );
        assert_eq!(content(&e, "a.txt"), b"a2 retained");
        e.close(h).unwrap();
        no_conflicts(&e);
    }
    assert_eq!(
        fs::read_dir(dir.path().join("staging")).unwrap().count(),
        0,
        "its staging file went with the session"
    );

    // Edited after the snapshot, then retained: the record holds the later
    // bytes on top of the commit, and a restart saves them in line.
    let dir = tempfile::tempdir().unwrap();
    let engine = shared(dir.path());
    {
        let mut e = lock(&engine);
        let entity = committed_file(&mut e, "b.txt", b"b1");
        let (h, _) = e.open("b.txt", None, true).unwrap();
        e.write(h, 0, b"b2 snapshot", false, false).unwrap();
        let job = started(e.begin_save(&entity).unwrap());
        e.write(h, 0, b"b3 later, retained", false, false).unwrap();
        assert_eq!(e.retain_all_dirty().unwrap(), 1);
        let stored = engine::store_snapshot(&job);
        e.finish_save(job, stored).unwrap();
        assert_eq!(content(&e, "b.txt"), b"b2 snapshot");
        let rows = pending_rows(&e);
        assert_eq!(rows.len(), 1, "the later bytes stay retained");
        let record: serde_json::Value = serde_json::from_str(&rows[0].1).unwrap();
        let committed = e.store.lookup("b.txt").unwrap();
        assert_eq!(
            record["content_base"].as_str(),
            committed.revisions.get("content").map(String::as_str),
            "the record is rebased onto the commit"
        );
    }
    drop(engine);
    let mut e = Engine::new(Store::open(dir.path(), Some("local-fs")).unwrap());
    e.retry_pending().unwrap();
    assert_eq!(content(&e, "b.txt"), b"b3 later, retained");
    assert!(pending_rows(&e).is_empty());
    no_conflicts(&e);
}

#[test]
fn the_first_write_to_a_large_file_copies_it_without_the_engine_lock() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let engine = shared(dir.path());
    let original = bytes(7, 2 * 1024 * 1024 + 17);
    let h = {
        let mut e = lock(&engine);
        committed_file(&mut e, "large.bin", &original);
        let (h, _) = e.open("large.bin", None, true).unwrap();
        assert!(e.session(h).unwrap().bytes.large_reference().is_some());
        h
    };
    runtime::prepare_content(&engine, h, Duration::from_secs(5)).unwrap();
    runtime::content::prepare_writable(&engine, h, None).unwrap();
    let mut e = lock(&engine);
    let session = e.session(h).unwrap();
    assert!(session.bytes.is_file(), "the writable copy is installed");
    assert!(
        session.bytes == original,
        "the copy holds the object's bytes"
    );
    assert!(!session.dirty, "a copy is not an edit");
    e.write(h, 5, b"EDIT", false, false).unwrap();
    e.close(h).unwrap();
    let mut expected = original.clone();
    expected[5..9].copy_from_slice(b"EDIT");
    assert_eq!(content(&e, "large.bin"), expected);
    // A resize to zero copies nothing.
    let (h, _) = e.open("large.bin", None, true).unwrap();
    drop(e);
    runtime::content::prepare_writable(&engine, h, Some((0, false))).unwrap();
    let e = lock(&engine);
    assert!(e.session(h).unwrap().bytes.large_reference().is_some());
    drop(e);
    // A copy made for a reference that changed meanwhile is dropped.
    let staging = dir.path().join("staging");
    let before = fs::read_dir(&staging).map_or(0, |d| d.count());
    let (path, object, size) = lock(&engine)
        .session(h)
        .unwrap()
        .bytes
        .large_reference()
        .unwrap();
    let copy = tkfs::staging::writable_copy(dir.path(), &path, &object, size).unwrap();
    let mut other = tkfs::staging::Staged::memory(b"changed".to_vec());
    assert!(!other.install_writable(&object, size, copy));
    assert_eq!(fs::read_dir(&staging).map_or(0, |d| d.count()), before);
    lock(&engine).close(h).unwrap();
}

#[test]
fn a_corrupt_object_is_never_copied_for_writing() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let engine = shared(dir.path());
    let original = bytes(9, 600 * 1024);
    let h = {
        let mut e = lock(&engine);
        committed_file(&mut e, "c.bin", &original);
        e.open("c.bin", None, true).unwrap().0
    };
    let object = hash(&original);
    let path = dir.path().join("objects").join(&object);
    let mut damaged = original.clone();
    damaged[100] ^= 0xff;
    fs::write(&path, &damaged).unwrap();
    let error = runtime::content::prepare_writable(&engine, h, None).unwrap_err();
    assert!(format!("{error:#}").contains("CORRUPT_OBJECT"), "{error:#}");
    assert_eq!(
        fs::read_dir(dir.path().join("staging")).map_or(0, |d| d.count()),
        0,
        "the failed copy is removed"
    );
    lock(&engine).close(h).unwrap();
}

#[test]
fn closed_files_are_saved_in_the_background_and_a_stop_waits_for_them() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let engine = shared(dir.path());
    let mut written = BTreeMap::new();
    for i in 0..60u64 {
        let path = format!("d{}/f{i:03}.bin", i % 4);
        let data = bytes(100 + i, 512 + (i as usize * 997) % 70_000);
        let h = {
            let mut e = lock(&engine);
            if e.store.lookup(&format!("d{}", i % 4)).is_err() {
                e.store.create(&format!("d{}", i % 4), "directory").unwrap();
            }
            let (h, _) = e.open(&path, Some("file"), true).unwrap();
            e.write(h, 0, &data, false, false).unwrap();
            e.cleanup_unsaved(h).unwrap();
            h
        };
        engine::save_handle_in_background(&engine, h).unwrap();
        engine::close_handle(&engine, h).unwrap();
        written.insert(path, data);
    }
    assert!(
        wait_until(Duration::from_secs(30), || lock(&engine).quiet().is_ok()),
        "every closed file is committed"
    );
    assert!(wait_until(
        Duration::from_secs(5),
        || engine::background_saves() == 0
    ));
    let e = lock(&engine);
    for (path, data) in &written {
        assert_eq!(&content(&e, path), data, "{path}");
    }
    assert!(e.sessions.is_empty(), "committed sessions are dropped");
    assert!(pending_rows(&e).is_empty());
    no_conflicts(&e);
}

#[test]
fn a_delete_or_a_rename_over_a_closed_file_waits_for_its_save() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let engine = shared(dir.path());
    for round in 0..20u64 {
        let target = format!("target{round}.txt");
        let temp = format!("temp{round}.txt");
        let gone = format!("gone{round}.txt");
        let close_in_background = |path: &str, data: &[u8]| {
            let h = {
                let mut e = lock(&engine);
                let create = e.store.lookup(path).is_err().then_some("file");
                let (h, _) = e.open(path, create, true).unwrap();
                e.truncate(h, 0, false).unwrap();
                e.write(h, 0, data, false, false).unwrap();
                e.cleanup_unsaved(h).unwrap();
                h
            };
            engine::save_handle_in_background(&engine, h).unwrap();
            engine::close_handle(&engine, h).unwrap();
        };
        // The atomic-save pattern: write the target, then a temporary file,
        // and rename the temporary file over the target at once.
        close_in_background(&target, b"first version");
        close_in_background(&temp, b"second version");
        engine::settle_path(&engine, &target).unwrap();
        {
            let e = lock(&engine);
            let source = e.store.lookup(&temp).unwrap().id;
            drop(e);
            engine::settle_entity(&engine, &source).unwrap();
            let mut e = lock(&engine);
            e.store.rename(&source, &target, true).unwrap();
        }
        // And a delete right after the close.
        close_in_background(&gone, b"short lived");
        engine::settle_path(&engine, &gone).unwrap();
        {
            let mut e = lock(&engine);
            let entity = e.store.lookup(&gone).unwrap().id;
            e.store.delete(&entity).unwrap();
        }
        assert!(wait_until(Duration::from_secs(10), || lock(&engine)
            .quiet()
            .is_ok()));
        let e = lock(&engine);
        assert_eq!(content(&e, &target), b"second version", "round {round}");
        assert!(
            e.store.lookup(&gone).is_err(),
            "round {round}: stays deleted"
        );
        assert!(e.store.lookup(&temp).is_err(), "round {round}");
        no_conflicts(&e);
    }
}

/// Simulates a download of `object` into `state`: its spool grows by
/// `step` bytes every `every` for `steps` steps, then the object is
/// installed (verified) and the spool removed.
fn simulated_download(
    state: PathBuf,
    object: String,
    data: Vec<u8>,
    step: usize,
    every: Duration,
    steps: usize,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let spool = state.join("incoming-objects").join(&object);
        fs::create_dir_all(spool.parent().unwrap()).unwrap();
        for i in 1..=steps {
            fs::write(&spool, &data[..(i * step).min(data.len())]).unwrap();
            std::thread::sleep(every);
        }
        let temp = state.join("objects").join(format!("{}.part", id()));
        fs::write(&temp, &data).unwrap();
        fs::rename(&temp, state.join("objects").join(&object)).unwrap();
        let _ = fs::remove_file(&spool);
    })
}

/// A store whose `remote.bin` references bytes it does not have.
fn missing_content(dir: &Path, data: &[u8]) -> (Shared, u64, String) {
    let engine = shared(dir);
    let object = {
        let mut e = lock(&engine);
        committed_file(&mut e, "remote.bin", data);
        let object = hash(data);
        fs::remove_file(dir.join("objects").join(&object)).unwrap();
        object
    };
    let h = lock(&engine).open("remote.bin", None, false).unwrap().0;
    (engine, h, object)
}

fn never() -> bool {
    false
}

#[test]
fn a_read_waits_while_its_download_progresses() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let data = bytes(42, 2 * 1024 * 1024);
    let (engine, h, object) = missing_content(dir.path(), &data);
    let wait = ContentWait {
        stall: Duration::from_millis(400),
        bound: Duration::from_secs(30),
        slots: None,
        cancelled: never,
    };
    // Twelve steps of 150 ms: the whole download takes far longer than the
    // stall bound, but bytes keep arriving.
    let feeder = simulated_download(
        dir.path().to_owned(),
        object,
        data.clone(),
        data.len() / 12,
        Duration::from_millis(150),
        12,
    );
    let started = Instant::now();
    runtime::content::prepare_content_waiting(&engine, h, wait).unwrap();
    let waited = started.elapsed();
    feeder.join().unwrap();
    assert!(
        waited > Duration::from_millis(1500),
        "the read waited through the download ({waited:?})"
    );
    let e = lock(&engine);
    assert!(
        e.session(h).unwrap().bytes == data,
        "it serves verified bytes"
    );
}

#[test]
fn a_stalled_or_fenced_or_crowded_wait_ends_with_content_unavailable() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let data = bytes(43, 300 * 1024);
    let (engine, h, _object) = missing_content(dir.path(), &data);
    // No progress: the stall bound ends it.
    let stall = ContentWait {
        stall: Duration::from_millis(300),
        bound: Duration::from_secs(30),
        slots: None,
        cancelled: never,
    };
    let started = Instant::now();
    let error = runtime::content::prepare_content_waiting(&engine, h, stall).unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(5));
    let message = format!("{error:#}");
    assert!(message.starts_with("CONTENT_UNAVAILABLE"), "{message}");
    assert!(
        lock(&engine).store.wanted_objects().unwrap().len() == 1,
        "the read registered demand"
    );
    // A fenced mount ends it at once.
    tkfs::worker::mount_ctl::adapter::set_fenced(true);
    let fenced = ContentWait {
        stall: Duration::from_secs(30),
        bound: Duration::from_secs(30),
        slots: None,
        cancelled: tkfs::worker::mount_ctl::adapter::fenced,
    };
    let started = Instant::now();
    let error = runtime::content::prepare_content_waiting(&engine, h, fenced).unwrap_err();
    tkfs::worker::mount_ctl::adapter::set_fenced(false);
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(format!("{error:#}").contains("detached"), "{error:#}");
    // With every wait slot taken a read fails at once.
    static ONE: WaitSlots = WaitSlots::new(1);
    let crowded = ContentWait {
        stall: Duration::from_millis(1500),
        bound: Duration::from_secs(30),
        slots: Some(&ONE),
        cancelled: never,
    };
    let (engine2, h2, _) = missing_content(&dir.path().join("second"), &bytes(44, 300 * 1024));
    let first = {
        let engine = engine.clone();
        std::thread::spawn(move || runtime::content::prepare_content_waiting(&engine, h, crowded))
    };
    assert!(wait_until(Duration::from_secs(5), || ONE.used() == 1));
    let started = Instant::now();
    let error = runtime::content::prepare_content_waiting(&engine2, h2, crowded).unwrap_err();
    assert!(started.elapsed() < Duration::from_millis(1000));
    assert!(format!("{error:#}").contains("already wait"), "{error:#}");
    assert!(first.join().unwrap().is_err());
    assert_eq!(ONE.used(), 0, "slots are given back");
}

#[test]
fn shorter_content_wait_bounds_come_only_from_the_environment() {
    let (stall, bound) =
        runtime::content::wait_bounds(Duration::from_secs(60), Duration::from_secs(240));
    // Unset in the test environment unless a scenario sets them: never
    // longer than the defaults either way.
    assert!(stall <= Duration::from_secs(60) && bound <= Duration::from_secs(240));
}

// ---------------------------------------------------------------------------
// Crash at every step of a save (child processes).

/// Child: saves `f.bin` with new bytes in the store `TKFS_TEST_SAVE_WORKER`
/// names; `TKFS_TEST_SAVE_RETAINED=1` retains the session (a force detach)
/// and edits it once more first. A `TKFS_FAULT_*` exit point ends it.
#[test]
fn save_crash_worker() {
    let Ok(root) = std::env::var("TKFS_TEST_SAVE_WORKER") else {
        return;
    };
    fault::reload_from_env();
    let engine = shared(Path::new(&root));
    let retained = std::env::var("TKFS_TEST_SAVE_RETAINED").is_ok();
    let size: usize = std::env::var("TKFS_TEST_SAVE_SIZE")
        .unwrap()
        .parse()
        .unwrap();
    let deferred = std::env::var("TKFS_TEST_SAVE_DEFERRED").is_ok();
    let h = {
        let mut e = lock(&engine);
        let (h, _) = if deferred {
            e.create_deferred("new.bin", true, None).unwrap()
        } else {
            e.open("f.bin", None, true).unwrap()
        };
        e.write(h, 0, &bytes(5, size), false, false).unwrap();
        if retained {
            e.retain_all_dirty().unwrap();
            e.write(h, 0, &bytes(6, size), false, false).unwrap();
            e.retain_all_dirty().unwrap();
        }
        h
    };
    engine::save_handle(&engine, h).unwrap();
    std::process::exit(0);
}

#[test]
fn a_crash_at_every_step_of_a_save_keeps_the_last_committed_bytes() {
    let _serial = serial();
    let points = [
        ("engine.save.snapshot", false),
        ("engine.save.store", false),
        ("engine.save.flushed", false),
        ("engine.save.installed", false),
        ("engine.save.commit", false),
        ("engine.save.committed", true),
        // `Store::put_object`'s legacy crash points fire in a save too.
        ("object_flushed", false),
        ("object_installed", false),
    ];
    for retained in [false, true] {
        for size in [4096usize, 700 * 1024] {
            for (point, committed) in points {
                let dir = tempfile::tempdir().unwrap();
                {
                    let mut e = Engine::new(Store::open(dir.path(), Some("local-fs")).unwrap());
                    committed_file(&mut e, "f.bin", b"base");
                }
                let mut child = std::process::Command::new(std::env::current_exe().unwrap());
                child
                    .args(["--exact", "save_crash_worker", "--nocapture"])
                    .env("TKFS_TEST_SAVE_WORKER", dir.path())
                    .env("TKFS_TEST_SAVE_SIZE", size.to_string());
                if point.starts_with("object_") {
                    child.env("TKFS_FAULT", point);
                } else {
                    child
                        .env("TKFS_FAULT_POINT", point)
                        .env("TKFS_FAULT_ACTION", "exit");
                }
                if retained {
                    child.env("TKFS_TEST_SAVE_RETAINED", "1");
                }
                let output = child.output().unwrap();
                let what = format!("{point}, {size} bytes, retained {retained}");
                assert_eq!(
                    output.status.code(),
                    Some(86),
                    "{what}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
                // Restart: recovery restores a retained save, the orphaned
                // spools go, and the store is consistent.
                let mut e = Engine::new(Store::open(dir.path(), Some("local-fs")).unwrap());
                e.remove_orphaned_spools().unwrap();
                assert!(part_files(dir.path()).is_empty(), "{what}");
                let expected_now = if committed {
                    bytes(if retained { 6 } else { 5 }, size)
                } else {
                    b"base".to_vec()
                };
                assert_eq!(content(&e, "f.bin"), expected_now, "{what}");
                if retained {
                    // The retained later bytes are saved, once, in line.
                    e.retry_pending().unwrap();
                    assert_eq!(content(&e, "f.bin"), bytes(6, size), "{what}");
                }
                assert!(pending_rows(&e).is_empty(), "{what}");
                no_conflicts(&e);
                let diff = e.store.verify_derived_state().unwrap();
                assert!(diff.is_empty(), "{what}: {diff:?}");
            }
        }
    }
}

/// Events in the journal.
fn events(e: &Engine) -> i64 {
    e.store
        .db
        .query_row("SELECT count(*) FROM events", [], |r| r.get(0))
        .unwrap()
}

#[test]
fn a_file_created_through_the_mount_commits_with_its_first_save() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let engine = shared(dir.path());
    let creation = 133_500_000_000_000_000u64;
    let write = 133_600_000_000_000_000u64;
    let (h, entity, before) = {
        let mut e = lock(&engine);
        e.store.create("d", "directory").unwrap();
        let before = events(&e);
        let (h, entry) = e.create_deferred("d/New.txt", true, None).unwrap();
        assert_eq!(events(&e), before, "nothing is committed at the create");
        assert!(e.store.lookup("d/New.txt").is_err());
        assert!(e.is_deferred(&entry.id));
        // The mount shows it, under any case of its name.
        assert_eq!(e.lookup_visible("d/new.TXT").unwrap().id, entry.id);
        let d = e.store.lookup("d").unwrap().id;
        let names: Vec<_> = e
            .children_visible(&d)
            .unwrap()
            .into_iter()
            .map(|child| child.name)
            .collect();
        assert_eq!(names, ["New.txt"]);
        // A second create of the name, a file's or a directory's, collides.
        let again = e.create_deferred("d/NEW.txt", true, None).map(|_| ());
        let directory = e.open("d/new.txt", Some("directory"), false).map(|_| ());
        for create in [again, directory] {
            assert!(
                format!("{:#}", create.unwrap_err()).contains("NAME_COLLISION"),
                "a second create collides"
            );
        }
        // A second open shares the session.
        let (second, same) = e.open("d/new.txt", None, false).unwrap();
        assert_eq!(same.id, entry.id);
        e.release_handle(second).unwrap();
        e.write(h, 0, b"hello", false, false).unwrap();
        // Times set before the first save stay in the session.
        e.set_basic(h, u32::MAX, [creation, 0, write, 0]).unwrap();
        assert_eq!(events(&e), before, "still nothing committed");
        assert!(e.quiet().is_err(), "an uncommitted create is unsaved");
        (h, entry.id, before)
    };
    engine::save_handle(&engine, h).unwrap();
    let mut e = lock(&engine);
    assert_eq!(
        events(&e),
        before + 1,
        "one event creates the file with its content"
    );
    assert!(!e.is_deferred(&entity));
    let committed = e.store.lookup("d/New.txt").unwrap();
    assert_eq!(committed.id, entity);
    assert_eq!(content(&e, "d/New.txt"), b"hello");
    let basic = committed.basic_info();
    assert_eq!(basic.creation_time, creation);
    assert_eq!(basic.write_time, write, "a time set explicitly is kept");
    assert_ne!(basic.attributes & 0x20, 0, "saved: archive");
    e.close(h).unwrap();
    assert_eq!(events(&e), before + 1, "a clean close commits nothing");
    assert!(e.quiet().is_ok());
    let d = e.store.lookup("d").unwrap().id;
    assert_eq!(e.children_visible(&d).unwrap().len(), 1, "listed once");
    assert!(pending_rows(&e).is_empty());
    no_conflicts(&e);
    let diff = e.store.verify_derived_state().unwrap();
    assert!(diff.is_empty(), "{diff:?}");
}

#[test]
fn a_deferred_create_is_committed_before_anything_replaces_or_removes_its_name() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let engine = shared(dir.path());
    // A rename's target held by a writer: settled (committed) at once.
    let (held, held_entity) = {
        let mut e = lock(&engine);
        let (h, entry) = e.create_deferred("target.txt", true, None).unwrap();
        e.write(h, 0, b"held", false, false).unwrap();
        (h, entry.id)
    };
    engine::settle_path(&engine, "target.txt").unwrap();
    {
        let mut e = lock(&engine);
        assert!(!e.is_deferred(&held_entity));
        assert_eq!(e.store.lookup("target.txt").unwrap().id, held_entity);
        assert_eq!(content(&e, "target.txt"), b"held");
        // The writer goes on; its later bytes are saved as usual.
        e.write(held, 4, b" on", false, false).unwrap();
        e.close(held).unwrap();
        assert_eq!(content(&e, "target.txt"), b"held on");
        // In line, under the lock (the mount's rename, after a race).
        let (h, entry) = e.create_deferred("other.txt", true, None).unwrap();
        e.write(h, 0, b"other", false, false).unwrap();
        e.commit_created_at("other.txt").unwrap();
        assert!(!e.is_deferred(&entry.id));
        assert_eq!(content(&e, "other.txt"), b"other");
        e.close(h).unwrap();
        // A committed file renamed over one replaces it, without conflict.
        let source = committed_file(&mut e, "source.txt", b"replacement");
        let (h, replaced) = e.create_deferred("replaced.txt", true, None).unwrap();
        e.write(h, 0, b"replaced", false, false).unwrap();
        e.release_handle(h).unwrap();
        drop(e);
        engine::settle_path(&engine, "replaced.txt").unwrap();
        let mut e = lock(&engine);
        e.store.rename(&source, "replaced.txt", true).unwrap();
        assert_eq!(content(&e, "replaced.txt"), b"replacement");
        assert!(!e.store.entry(&replaced.id).unwrap().alive);
        // A delete: the creation commits, then the delete.
        let (h, doomed) = e.create_deferred("doomed.txt", true, None).unwrap();
        e.write(h, 0, b"short lived", false, false).unwrap();
        e.cleanup(h, true).unwrap();
        e.close(h).unwrap();
        assert!(e.store.lookup("doomed.txt").is_err());
        assert!(e.lookup_visible("doomed.txt").is_err());
        assert!(!e.store.entry(&doomed.id).unwrap().alive);
    }
    assert!(wait_until(Duration::from_secs(10), || lock(&engine)
        .quiet()
        .is_ok()));
    let mut e = lock(&engine);
    assert!(e.sessions.is_empty());
    assert!(pending_rows(&e).is_empty());
    no_conflicts(&e);
    let diff = e.store.verify_derived_state().unwrap();
    assert!(diff.is_empty(), "{diff:?}");
}

#[test]
fn a_retained_deferred_create_is_shown_again_after_a_restart_and_committed_once() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let data = bytes(9, 100_000);
    let entity = {
        let mut e = Engine::new(Store::open(dir.path(), Some("local-fs")).unwrap());
        let (h, entry) = e.create_deferred("kept.bin", true, None).unwrap();
        e.write(h, 0, &data, false, false).unwrap();
        // A force detach: the file is retained, the engine goes unsaved.
        assert_eq!(e.retain_all_dirty().unwrap(), 1);
        entry.id
    };
    let mut e = Engine::new(Store::open(dir.path(), Some("local-fs")).unwrap());
    assert!(e.store.lookup("kept.bin").is_err());
    assert!(e.is_deferred(&entity), "restored as a deferred create");
    assert_eq!(e.lookup_visible("kept.bin").unwrap().id, entity);
    assert!(e.quiet().is_err(), "unsaved until committed");
    let before = events(&e);
    e.retry_pending().unwrap();
    assert_eq!(events(&e), before + 1, "committed once, in one event");
    assert!(!e.is_deferred(&entity));
    assert_eq!(content(&e, "kept.bin"), data);
    assert!(pending_rows(&e).is_empty());
    no_conflicts(&e);
    drop(e);
    // A restart after the commit shows it once.
    let mut e = Engine::new(Store::open(dir.path(), Some("local-fs")).unwrap());
    assert!(!e.is_deferred(&entity));
    let listed = e.children_visible(ROOT).unwrap();
    assert_eq!(
        listed
            .iter()
            .filter(|child| child.name == "kept.bin")
            .count(),
        1
    );
    let diff = e.store.verify_derived_state().unwrap();
    assert!(diff.is_empty(), "{diff:?}");
}

#[test]
fn a_crash_at_every_step_of_the_first_save_of_a_deferred_create_leaves_a_consistent_store() {
    let _serial = serial();
    let points = [
        ("engine.save.snapshot", false),
        ("engine.save.store", false),
        ("engine.save.flushed", false),
        ("engine.save.installed", false),
        ("engine.save.commit", false),
        ("engine.save.committed", true),
        ("object_flushed", false),
        ("object_installed", false),
    ];
    for retained in [false, true] {
        for (point, committed) in points {
            let size = 4096usize;
            let dir = tempfile::tempdir().unwrap();
            drop(Engine::new(
                Store::open(dir.path(), Some("local-fs")).unwrap(),
            ));
            let mut child = std::process::Command::new(std::env::current_exe().unwrap());
            child
                .args(["--exact", "save_crash_worker", "--nocapture"])
                .env("TKFS_TEST_SAVE_WORKER", dir.path())
                .env("TKFS_TEST_SAVE_SIZE", size.to_string())
                .env("TKFS_TEST_SAVE_DEFERRED", "1");
            if point.starts_with("object_") {
                child.env("TKFS_FAULT", point);
            } else {
                child
                    .env("TKFS_FAULT_POINT", point)
                    .env("TKFS_FAULT_ACTION", "exit");
            }
            if retained {
                child.env("TKFS_TEST_SAVE_RETAINED", "1");
            }
            let output = child.output().unwrap();
            let what = format!("{point}, retained {retained}");
            assert_eq!(
                output.status.code(),
                Some(86),
                "{what}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let mut e = Engine::new(Store::open(dir.path(), Some("local-fs")).unwrap());
            e.remove_orphaned_spools().unwrap();
            assert!(part_files(dir.path()).is_empty(), "{what}");
            let latest = bytes(if retained { 6 } else { 5 }, size);
            if committed {
                assert_eq!(content(&e, "new.bin"), latest, "{what}");
            } else if retained {
                // Restored, shown, and committed by its save.
                assert!(e.store.lookup("new.bin").is_err(), "{what}");
                assert!(e.lookup_visible("new.bin").is_ok(), "{what}");
                e.retry_pending().unwrap();
                assert_eq!(content(&e, "new.bin"), latest, "{what}");
            } else {
                // Never committed, never retained: only the new file is lost.
                assert!(e.lookup_visible("new.bin").is_err(), "{what}");
                assert!(e.sessions.is_empty(), "{what}");
            }
            assert!(pending_rows(&e).is_empty(), "{what}");
            no_conflicts(&e);
            let diff = e.store.verify_derived_state().unwrap();
            assert!(diff.is_empty(), "{what}: {diff:?}");
        }
    }
}

#[test]
fn deferred_create_switch_worker() {
    let Ok(root) = std::env::var("TKFS_TEST_DEFERRED_SWITCH") else {
        return;
    };
    assert!(!engine::deferred_creates());
    let mut e = Engine::new(Store::open(Path::new(&root), Some("local-fs")).unwrap());
    let (h, entry) = e.create_deferred("at-once.txt", true, None).unwrap();
    assert!(!e.is_deferred(&entry.id));
    assert_eq!(e.store.lookup("at-once.txt").unwrap().id, entry.id);
    e.write(h, 0, b"switched off", false, false).unwrap();
    e.close(h).unwrap();
    std::process::exit(0);
}

#[test]
fn deferred_creates_can_be_switched_off() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    drop(Engine::new(
        Store::open(dir.path(), Some("local-fs")).unwrap(),
    ));
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "deferred_create_switch_worker", "--nocapture"])
        .env("TKFS_TEST_DEFERRED_SWITCH", dir.path())
        .env("TKFS_DEFERRED_CREATE", "0")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let e = Engine::new(Store::open(dir.path(), Some("local-fs")).unwrap());
    assert_eq!(content(&e, "at-once.txt"), b"switched off");
    no_conflicts(&e);
}

// ---------------------------------------------------------------------------
// Mounted scenarios.

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
fn small_files_copied_through_the_mount_are_all_committed() {
    let _serial = serial();
    let Some(m) = mounted::Mounted::new() else {
        return;
    };
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
    // The availability rows of the saved objects land in batches.
    assert!(wait_until(Duration::from_secs(10), || {
        let e = lock(&m.engine);
        let rows: i64 = e
            .store
            .db
            .query_row("SELECT count(*) FROM object_availability", [], |r| r.get(0))
            .unwrap();
        rows as usize >= written.len()
    }));
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
fn the_atomic_save_pattern_through_the_mount_keeps_one_history() {
    let _serial = serial();
    let Some(m) = mounted::Mounted::new() else {
        return;
    };
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
fn a_mounted_read_of_content_that_cannot_arrive_fails_meaningfully() {
    let _serial = serial();
    let Some(m) = mounted::Mounted::new() else {
        return;
    };
    let data = bytes(77, 200 * 1024);
    {
        let mut e = lock(&m.engine);
        committed_file(&mut e, "remote.bin", &data);
        fs::remove_file(m.state.join("objects").join(hash(&data))).unwrap();
    }
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
fn a_mounted_read_waits_for_a_download_and_returns_its_bytes() {
    let _serial = serial();
    let Some(m) = mounted::Mounted::new() else {
        return;
    };
    let data = bytes(78, 6 * 1024 * 1024);
    let object = hash(&data);
    {
        let mut e = lock(&m.engine);
        committed_file(&mut e, "downloading.bin", &data);
        fs::remove_file(m.state.join("objects").join(&object)).unwrap();
    }
    unsafe { std::env::set_var("TKFS_CONTENT_STALL_MS", "1500") };
    let feeder = simulated_download(
        m.state.clone(),
        object,
        data.clone(),
        data.len() / 20,
        Duration::from_millis(200),
        20,
    );
    let read = fs::read(m.mount.join("downloading.bin"));
    unsafe { std::env::remove_var("TKFS_CONTENT_STALL_MS") };
    feeder.join().unwrap();
    assert!(
        read.unwrap() == data,
        "the read returns the downloaded bytes"
    );
    // Other operations never waited for it.
    let unrelated: BTreeSet<_> = fs::read_dir(&m.mount)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert!(unrelated.contains(std::ffi::OsStr::new("downloading.bin")));
}

#[cfg(any(windows, target_os = "linux"))]
#[test]
fn a_graceful_detach_right_after_writes_waits_for_their_saves() {
    let _serial = serial();
    let Some(m) = mounted::Mounted::new() else {
        return;
    };
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
fn many_concurrent_writers_never_stall_the_mount() {
    let _serial = serial();
    let Some(m) = mounted::Mounted::new() else {
        return;
    };
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
fn a_file_held_open_after_its_create_is_listed_renamed_and_deleted_through_the_mount() {
    use std::io::Write;
    let _serial = serial();
    let Some(m) = mounted::Mounted::new() else {
        return;
    };
    let held_path = m.mount.join("held.txt");
    let mut held = fs::File::create(&held_path).unwrap();
    held.write_all(b"first part").unwrap();
    // Shown at once. A WinFsp create commits at its first save; any close of
    // the file saves it, the stat's own (or a scanner's) too, so whether it
    // is committed yet is not checked here.
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
    // A rename of the held file commits its creation first.
    fs::rename(&held_path, m.mount.join("renamed.txt")).unwrap();
    {
        let e = lock(&m.engine);
        assert!(e.store.lookup("renamed.txt").is_ok());
        assert!(e.lookup_visible("held.txt").is_err());
    }
    held.write_all(b", second part").unwrap();
    drop(held);
    // A file created and deleted while it is held open (the FUSE adapter
    // refuses to unlink an open file: deleted after its close there).
    let gone_path = m.mount.join("gone.txt");
    let mut gone = fs::File::create(&gone_path).unwrap();
    gone.write_all(b"short lived").unwrap();
    #[cfg(windows)]
    fs::remove_file(&gone_path).unwrap();
    drop(gone);
    #[cfg(not(windows))]
    fs::remove_file(&gone_path).unwrap();
    assert!(m.quiet(Duration::from_secs(30)), "everything is committed");
    assert!(!gone_path.exists());
    let e = lock(&m.engine);
    assert_eq!(content(&e, "renamed.txt"), b"first part, second part");
    assert!(e.lookup_visible("gone.txt").is_err());
    assert!(e.lookup_visible("held.txt").is_err());
    assert!(pending_rows(&e).is_empty());
    no_conflicts(&e);
}
