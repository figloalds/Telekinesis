use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tkfs::{
    content::{ContentProvider, LocalContent, ReadContext},
    core::{Store, id},
    model::content::CHUNK_SIZE,
    runtime::{
        Engine, Shared,
        content::ContentWait,
        content_reads,
        content_writes::{self, NativeWrites},
    },
    util::{
        Cancel,
        fault::{self, FaultAction, FaultConfig},
    },
};
fn context() -> ReadContext {
    ReadContext {
        deadline: Instant::now() + Duration::from_secs(30),
        cancel: Cancel::new(),
    }
}
fn wait() -> ContentWait {
    ContentWait::fixed(Duration::from_secs(30))
}
fn error<T>(r: anyhow::Result<T>, code: &str) {
    let e = r.err().expect("expected failure");
    assert!(format!("{e:#}").contains(code), "{e:#}");
}
struct FaultReset;
impl Drop for FaultReset {
    fn drop(&mut self) {
        fault::configure(None);
    }
}
fn arm(point: &str, action: FaultAction, ms: u64) -> FaultReset {
    let mut config = FaultConfig::new(point, action);
    config.ms = Some(ms);
    config.count = Some(1);
    fault::configure(Some(config));
    FaultReset
}
fn fired() {
    let end = Instant::now() + Duration::from_secs(5);
    while fault::fired() == 0 {
        assert!(Instant::now() < end);
        std::thread::sleep(Duration::from_millis(1));
    }
}
fn publish(store: &mut Store, entity: &str, reference: &tkfs::model::content::FileContentRef) {
    let change = store
        .current_change(entity, "content", serde_json::json!(reference))
        .unwrap();
    let event = store.event(&store.branch(&store.active).unwrap(), vec![change]);
    store.commit(event, true).unwrap();
}
struct Fixture {
    shared: Shared,
    native: Arc<LocalContent>,
    writes: Arc<NativeWrites>,
    dir: tempfile::TempDir,
    data: Vec<u8>,
}
impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::initialize(dir.path(), "native-writes", &id()).unwrap();
        store
            .db
            .execute("INSERT INTO config VALUES('fixture:content_v1','1')", [])
            .unwrap();
        let entity = store.create("file", "file").unwrap().id;
        let native = Arc::new(LocalContent::open(dir.path()).unwrap());
        let data: Vec<_> = (0..3 * CHUNK_SIZE as usize)
            .map(|n| (n % 251) as u8)
            .collect();
        let reference = native
            .import(&mut data.as_slice(), data.len() as u64, &context())
            .unwrap();
        publish(&mut store, &entity, &reference);
        let shared = Arc::new(Mutex::new(Engine::new(store)));
        let writes = NativeWrites::install(&shared, &context()).unwrap();
        Self {
            shared,
            native,
            writes,
            dir,
            data,
        }
    }
    fn open(&self, writable: bool) -> u64 {
        let h = tkfs::runtime::lock_engine(&self.shared)
            .open("file", None, writable)
            .unwrap()
            .0;
        content_reads::prepare_open(&self.shared, h, wait()).unwrap();
        h
    }
    fn bytes(&self, h: u64, len: usize) -> Vec<u8> {
        content_reads::read_mount_vec(&self.shared, h, 0, len, wait())
            .unwrap()
            .unwrap()
    }
    fn publications(&self) -> usize {
        tkfs::runtime::lock_engine(&self.shared)
            .store
            .db
            .query_row("SELECT count(*) FROM content_save_history", [], |r| {
                r.get(0)
            })
            .unwrap()
    }
    fn close(&self, h: u64) {
        tkfs::runtime::engine::close_handle(&self.shared, h).unwrap();
    }
    fn oracle(&self) {
        assert!(
            tkfs::runtime::lock_engine(&self.shared)
                .store
                .verify_derived_state()
                .unwrap()
                .is_empty()
        );
    }
    fn stop(&self) {
        self.writes.shutdown(&context()).unwrap();
    }
}
#[test]
fn native_write_conversion_is_explicit_and_not_enabled_for_ordinary_repositories() {
    let dir = tempfile::tempdir().unwrap();
    let shared = Arc::new(Mutex::new(Engine::new(
        Store::initialize(dir.path(), "ordinary", &id()).unwrap(),
    )));
    error(
        NativeWrites::install(&shared, &context()),
        "PEER_NEEDS_UPGRADE",
    );
}
#[test]
fn preflight_of_a_four_kib_edit_in_a_one_gib_tree_is_chunk_local_and_installs_nothing() {
    use tkfs::{
        model::content::{
            ChildKind, ContentKind, Digest, FileContentRef, ManifestChild, ManifestNode, ObjectKey,
            ObjectKind,
        },
        util::phases::Phase,
    };
    let f = Fixture::new();
    let original = vec![199; 65536];
    let chunk = Digest::bytes(&original);
    f.native
        .install(
            ObjectKey {
                kind: ObjectKind::Bytes,
                digest: chunk,
            },
            &original,
            &context(),
        )
        .unwrap();
    let leaf = ManifestNode {
        level: 0,
        total_len: 16 * 1024 * 1024,
        children: vec![
            ManifestChild {
                kind: ChildKind::Chunk,
                len: 65536,
                digest: chunk
            };
            256
        ],
    };
    let leaf_digest = leaf.digest().unwrap();
    f.native
        .install(
            ObjectKey {
                kind: ObjectKind::Manifest,
                digest: leaf_digest,
            },
            &leaf.encode().unwrap(),
            &context(),
        )
        .unwrap();
    let root = ManifestNode {
        level: 1,
        total_len: 1024 * 1024 * 1024,
        children: vec![
            ManifestChild {
                kind: ChildKind::Node,
                len: 16 * 1024 * 1024,
                digest: leaf_digest
            };
            64
        ],
    };
    let digest = root.digest().unwrap();
    f.native
        .install(
            ObjectKey {
                kind: ObjectKind::Manifest,
                digest,
            },
            &root.encode().unwrap(),
            &context(),
        )
        .unwrap();
    let reference = FileContentRef::new(ContentKind::Manifest, digest, 1024 * 1024 * 1024).unwrap();
    {
        let mut e = tkfs::runtime::lock_engine(&f.shared);
        let entity = e.store.lookup("file").unwrap().id;
        publish(&mut e.store, &entity, &reference);
    }
    let h = f.open(true);
    let before = f.native.counters.snapshot();
    let began = Instant::now();
    assert_eq!(
        content_writes::write(
            &f.shared,
            h,
            512 * 1024 * 1024 + 8192,
            &vec![19; 4096],
            false,
            false,
            wait()
        )
        .unwrap(),
        Some(4096)
    );
    let elapsed = began.elapsed();
    let after = f.native.counters.snapshot();
    let bytes_read = after[Phase::Read as usize].bytes - before[Phase::Read as usize].bytes;
    let pages_read = after[Phase::ManifestRead as usize].operations
        - before[Phase::ManifestRead as usize].operations;
    assert!(
        bytes_read <= 2 * 65536,
        "preflight read {bytes_read} data bytes"
    );
    assert!(
        pages_read <= 8,
        "preflight read {pages_read} manifest pages"
    );
    for phase in [Phase::Write, Phase::ManifestWrite] {
        assert_eq!(
            after[phase as usize].operations, before[phase as usize].operations,
            "preflight installed CAS objects"
        );
    }
    assert!(
        tkfs::content::write::WriteJournal::open(f.dir.path())
            .unwrap()
            .pending(None, 64, &context())
            .unwrap()
            .is_empty()
    );
    assert!(content_writes::flush(&f.shared, h, wait()).unwrap());
    let mut out = vec![0; 4096];
    content_reads::read_content_range(&f.shared, h, 512 * 1024 * 1024 + 8192, &mut out, &context())
        .unwrap()
        .unwrap();
    assert_eq!(out, vec![19; 4096]);
    f.close(h);
    f.oracle();
    f.stop();
    println!(
        "WAVE6_PREFLIGHT {}",
        serde_json::json!({"base_bytes":1073741824_u64,"changed_bytes":4096,"data_bytes_read":bytes_read,"manifest_pages_read":pages_read,"cas_objects_installed":0,"full_checkpoint":false,"preflight_ms":elapsed.as_secs_f64()*1000.0,"seed_excluded":true})
    );
}
#[test]
fn mutable_read_reserves_adapter_capacity_before_waiting_for_a_started_write() {
    static SLOTS: tkfs::runtime::content::WaitSlots = tkfs::runtime::content::WaitSlots::new(1);
    let f = Fixture::new();
    let h = f.open(true);
    let _reset = arm("content.native.before_write", FaultAction::Hang, 350);
    let shared = f.shared.clone();
    let writer = std::thread::spawn(move || {
        content_writes::write(&shared, h, 13, b"changed", false, false, wait())
    });
    fired();
    let waiting = ContentWait {
        slots: Some(&SLOTS),
        ..wait()
    };
    let shared = f.shared.clone();
    let reader =
        std::thread::spawn(move || content_reads::read_mount_vec(&shared, h, 13, 7, waiting));
    let until = Instant::now() + Duration::from_secs(1);
    while SLOTS.used() == 0 {
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(1));
    }
    let began = Instant::now();
    error(
        content_reads::read_mount_vec(&f.shared, h, 13, 7, waiting),
        "CONTENT_RESOURCE_LIMIT",
    );
    assert!(began.elapsed() < Duration::from_millis(100));
    assert!(
        tkfs::runtime::lock_engine(&f.shared)
            .store
            .lookup("file")
            .is_ok()
    );
    writer.join().unwrap().unwrap();
    assert_eq!(reader.join().unwrap().unwrap().unwrap(), b"changed");
    assert_eq!(SLOTS.used(), 0);
    f.close(h);
    f.stop();
}
#[test]
fn two_writers_and_a_reader_share_private_bytes_and_flush_publishes_one_revision() {
    let f = Fixture::new();
    let h = f.open(true);
    let other = f.open(true);
    let reader = f.open(false);
    assert_eq!(
        content_writes::write(&f.shared, h, 13, b"changed", false, false, wait()).unwrap(),
        Some(7)
    );
    let mut expected = f.data.clone();
    expected[13..20].copy_from_slice(b"changed");
    assert_eq!(f.bytes(reader, expected.len()), expected);
    assert_eq!(f.publications(), 0);
    assert!(content_writes::flush(&f.shared, other, wait()).unwrap());
    assert_eq!(f.publications(), 1);
    assert_eq!(
        tkfs::runtime::lock_engine(&f.shared)
            .session(h)
            .unwrap()
            .bytes
            .len(),
        expected.len()
    );
    let entry = tkfs::runtime::lock_engine(&f.shared)
        .store
        .lookup("file")
        .unwrap();
    let pin = f.native.pin(entry.content_ref.as_ref().unwrap()).unwrap();
    let mut actual = vec![0; expected.len()];
    f.native
        .read_range(&pin, 0, &mut actual, &context())
        .unwrap();
    assert_eq!(actual, expected);
    f.close(h);
    f.close(other);
    f.close(reader);
    assert_eq!(f.publications(), 1);
    f.oracle();
    f.stop();
}
#[test]
fn full_chunk_overwrite_does_not_hydrate_the_missing_base_and_close_is_durable() {
    let f = Fixture::new();
    let h = f.open(true);
    std::fs::remove_file(
        f.dir
            .path()
            .join("objects")
            .join(tkfs::model::content::Digest::bytes(&f.data[..CHUNK_SIZE as usize]).to_string()),
    )
    .unwrap();
    let bytes = vec![19; CHUNK_SIZE as usize];
    content_writes::write(&f.shared, h, 0, &bytes, false, false, wait()).unwrap();
    assert_eq!(f.bytes(h, bytes.len()), bytes);
    f.close(h);
    assert_eq!(f.publications(), 1);
    let reader = f.open(false);
    let mut expected = f.data.clone();
    expected[..bytes.len()].copy_from_slice(&bytes);
    assert_eq!(f.bytes(reader, expected.len()), expected);
    f.close(reader);
    f.oracle();
    f.stop();
}
#[test]
fn truncate_grow_and_a_lost_write_reply_keep_length_and_holes_coherent() {
    let f = Fixture::new();
    let h = f.open(true);
    content_writes::truncate(&f.shared, h, 31, false, wait()).unwrap();
    content_writes::truncate(&f.shared, h, 1000, false, wait()).unwrap();
    let mut expected = vec![0; 1000];
    expected[..31].copy_from_slice(&f.data[..31]);
    assert_eq!(f.bytes(h, 1000), expected);
    let reset = arm("content.write.buffered", FaultAction::Fail, 0);
    error(
        content_writes::write(&f.shared, h, 1005, b"tail", false, false, wait()),
        "FAULT_INJECTED",
    );
    drop(reset);
    expected.resize(1009, 0);
    expected[1005..].copy_from_slice(b"tail");
    assert_eq!(
        tkfs::runtime::lock_engine(&f.shared)
            .session(h)
            .unwrap()
            .bytes
            .len(),
        1009
    );
    assert_eq!(f.bytes(h, 1009), expected);
    f.close(h);
    assert_eq!(f.publications(), 1);
    f.oracle();
    f.stop();
}
#[test]
fn append_offsets_are_atomic_across_native_handles() {
    let f = Fixture::new();
    let h = f.open(true);
    content_writes::truncate(&f.shared, h, 0, false, wait()).unwrap();
    let mut threads = Vec::new();
    for tag in 1..=8u8 {
        let shared = f.shared.clone();
        threads.push(std::thread::spawn(move || {
            for _ in 0..16 {
                assert_eq!(
                    content_writes::write(&shared, h, 0, &[tag; 7], true, false, wait()).unwrap(),
                    Some(7)
                );
            }
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    let bytes = f.bytes(h, 8 * 16 * 7);
    assert_eq!(bytes.len(), 8 * 16 * 7);
    for tag in 1..=8 {
        assert_eq!(bytes.chunks_exact(7).filter(|b| *b == [tag; 7]).count(), 16);
    }
    f.close(h);
    assert_eq!(f.publications(), 1);
    f.oracle();
    f.stop();
}
#[test]
fn flush_cannot_pass_a_write_already_started_and_engine_remains_available() {
    let f = Fixture::new();
    let h = f.open(true);
    let reset = arm("content.native.before_write", FaultAction::Hang, 350);
    let shared = f.shared.clone();
    let writer = std::thread::spawn(move || {
        content_writes::write(&shared, h, 0, b"accepted", false, false, wait())
    });
    fired();
    let began = Instant::now();
    let shared = f.shared.clone();
    let flush = std::thread::spawn(move || content_writes::flush(&shared, h, wait()));
    let until = Instant::now() + Duration::from_millis(150);
    loop {
        if let Some(e) = tkfs::runtime::try_lock_engine(&f.shared) {
            assert!(e.session(h).unwrap().dirty);
            break;
        }
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(1));
    }
    writer.join().unwrap().unwrap();
    flush.join().unwrap().unwrap();
    assert!(began.elapsed() >= Duration::from_millis(200));
    drop(reset);
    assert_eq!(&f.bytes(h, 8), b"accepted");
    assert!(
        !tkfs::runtime::lock_engine(&f.shared)
            .session(h)
            .unwrap()
            .dirty
    );
    f.close(h);
    assert_eq!(f.publications(), 1);
    f.stop();
}
#[test]
fn selected_open_rechecks_metadata_before_it_can_supersede_a_save_owner() {
    let f = Fixture::new();
    let h = tkfs::runtime::lock_engine(&f.shared)
        .open("file", None, true)
        .unwrap()
        .0;
    let reset = arm("content.native.before_capture", FaultAction::Hang, 350);
    let shared = f.shared.clone();
    let opening = std::thread::spawn(move || content_reads::prepare_open(&shared, h, wait()));
    fired();
    {
        let mut e = tkfs::runtime::lock_engine(&f.shared);
        let entry = e.store.lookup("file").unwrap();
        publish(&mut e.store, &entry.id, entry.content_ref.as_ref().unwrap());
    }
    error(opening.join().unwrap(), "STALE_CONTENT_VIEW");
    drop(reset);
    assert_eq!(
        tkfs::runtime::lock_engine(&f.shared)
            .store
            .db
            .query_row("SELECT count(*) FROM content_save_operations", [], |r| {
                r.get::<_, usize>(0)
            })
            .unwrap(),
        0
    );
    tkfs::runtime::lock_engine(&f.shared)
        .release_handle(h)
        .unwrap();
    f.stop();
}
#[test]
fn deadline_retains_saved_and_reopening_restores_the_private_view_before_retry() {
    let f = Fixture::new();
    let h = f.open(true);
    content_writes::write(&f.shared, h, 0, b"recover", false, false, wait()).unwrap();
    let writer = rusqlite::Connection::open(f.dir.path().join("metadata.sqlite")).unwrap();
    writer.execute_batch("BEGIN IMMEDIATE").unwrap();
    error(
        content_writes::flush(&f.shared, h, ContentWait::fixed(Duration::from_millis(100))),
        "CONTENT_DEADLINE",
    );
    assert_eq!(f.publications(), 0);
    assert_eq!(
        tkfs::runtime::lock_engine(&f.shared)
            .retain_all_dirty()
            .unwrap(),
        1
    );
    writer.execute_batch("ROLLBACK").unwrap();
    drop(writer);
    f.stop();
    let Fixture {
        shared,
        writes,
        native,
        dir,
        data: _,
    } = f;
    drop(shared);
    drop(writes);
    drop(native);
    let shared = Arc::new(Mutex::new(Engine::new(
        Store::open_existing(dir.path()).unwrap(),
    )));
    let writes = NativeWrites::install(&shared, &context()).unwrap();
    assert!(tkfs::runtime::lock_engine(&shared).quiet().is_err());
    let h = tkfs::runtime::lock_engine(&shared)
        .open("file", None, false)
        .unwrap()
        .0;
    assert_eq!(
        content_reads::read_mount_vec(&shared, h, 0, 7, wait())
            .unwrap()
            .unwrap(),
        b"recover"
    );
    content_writes::flush(&shared, h, wait()).unwrap();
    tkfs::runtime::engine::close_handle(&shared, h).unwrap();
    assert!(tkfs::runtime::lock_engine(&shared).quiet().is_ok());
    writes.shutdown(&context()).unwrap();
}
#[test]
fn forced_detach_checkpoints_native_sessions_without_engine_byte_io() {
    let f = Fixture::new();
    let h = f.open(true);
    content_writes::write(&f.shared, h, 0, b"retain", false, false, wait()).unwrap();
    error(
        tkfs::runtime::lock_engine(&f.shared).retain_all_dirty(),
        "CONTENT_SAVE_REQUIRES_OFFLOCK_RETAIN",
    );
    let reset = arm(
        "content.write.before_checkpoint_commit",
        FaultAction::Hang,
        350,
    );
    let shared = f.shared.clone();
    let retaining =
        std::thread::spawn(move || tkfs::worker::mount_ctl::adapter::persist_dirty(&shared));
    fired();
    let until = Instant::now() + Duration::from_millis(150);
    loop {
        if let Some(e) = tkfs::runtime::try_lock_engine(&f.shared) {
            assert!(e.session(h).unwrap().dirty);
            break;
        }
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(1));
    }
    retaining.join().unwrap();
    drop(reset);
    assert_eq!(
        tkfs::runtime::lock_engine(&f.shared)
            .retain_all_dirty()
            .unwrap(),
        1
    );
    f.close(h);
    f.oracle();
    f.stop();
}

#[test]
fn a_stalled_native_publication_keeps_another_files_private_io_and_metadata_live() {
    let f = Fixture::new();
    {
        let mut e = tkfs::runtime::lock_engine(&f.shared);
        let reference = e.store.lookup("file").unwrap().content_ref.unwrap();
        let entity = e.store.create("other", "file").unwrap().id;
        publish(&mut e.store, &entity, &reference);
    }
    let h = f.open(true);
    let other = tkfs::runtime::lock_engine(&f.shared)
        .open("other", None, true)
        .unwrap()
        .0;
    content_reads::prepare_open(&f.shared, other, wait()).unwrap();
    content_writes::write(&f.shared, h, 0, b"first", false, false, wait()).unwrap();
    let reset = arm("content.save.event_written", FaultAction::Hang, 1000);
    let shared = f.shared.clone();
    let flushing = std::thread::spawn(move || content_writes::flush(&shared, h, wait()));
    fired();
    let began = Instant::now();
    let until = began + Duration::from_millis(300);
    loop {
        if let Some(e) = tkfs::runtime::try_lock_engine(&f.shared) {
            assert!(e.store.lookup("other").unwrap().alive);
            break;
        }
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(1));
    }
    content_writes::write(&f.shared, other, 0, b"second", false, false, wait()).unwrap();
    assert_eq!(f.bytes(other, 6), b"second");
    let elapsed_ms = began.elapsed().as_secs_f64() * 1000.0;
    assert!(began.elapsed() < Duration::from_millis(300));
    flushing.join().unwrap().unwrap();
    drop(reset);
    f.close(h);
    f.close(other);
    assert_eq!(f.publications(), 2);
    f.oracle();
    f.stop();
    println!(
        "WAVE6_NATIVE_IO {}",
        serde_json::json!({"metadata_writer_stall_ms":1000,"other_private_io_ms":elapsed_ms,"independent_engine":true,"publications":2})
    );
}

#[test]
#[ignore = "parent invokes the actual native close crash helper"]
fn native_close_crash_child() {
    let Some(root) = std::env::var_os("TKFS_NATIVE_CLOSE_CHILD") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let shared = Arc::new(Mutex::new(Engine::new(
        Store::open_existing(&root).unwrap(),
    )));
    let _writes = NativeWrites::install(&shared, &context()).unwrap();
    let h = tkfs::runtime::lock_engine(&shared)
        .open("file", None, true)
        .unwrap()
        .0;
    content_reads::prepare_open(&shared, h, wait()).unwrap();
    content_writes::write(&shared, h, 0, b"durable-child", false, false, wait()).unwrap();
    tkfs::runtime::engine::close_handle(&shared, h).unwrap();
    panic!("crash boundary did not exit");
}
#[test]
fn process_exit_during_native_close_restores_saved_and_replays_publication_once() {
    for point in [
        "content.executor.before_prepare",
        "content.executor.published",
    ] {
        let f = Fixture::new();
        f.stop();
        let Fixture {
            shared,
            writes,
            native,
            dir,
            data: _,
        } = f;
        drop(shared);
        drop(writes);
        drop(native);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "native_close_crash_child",
                "--ignored",
                "--nocapture",
            ])
            .env("TKFS_NATIVE_CLOSE_CHILD", dir.path())
            .env("TKFS_FAULT_POINT", point)
            .env("TKFS_FAULT_ACTION", "exit")
            .env("TKFS_FAULT_COUNT", "1")
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let shared = Arc::new(Mutex::new(Engine::new(
            Store::open_existing(dir.path()).unwrap(),
        )));
        let writes = NativeWrites::install(&shared, &context()).unwrap();
        let h = tkfs::runtime::lock_engine(&shared)
            .open("file", None, false)
            .unwrap()
            .0;
        assert_eq!(
            content_reads::read_mount_vec(&shared, h, 0, 13, wait())
                .unwrap()
                .unwrap(),
            b"durable-child"
        );
        tkfs::runtime::engine::close_handle(&shared, h).unwrap();
        let mut e = tkfs::runtime::lock_engine(&shared);
        assert_eq!(
            e.store
                .db
                .query_row("SELECT count(*) FROM content_save_history", [], |r| r
                    .get::<_, usize>(0))
                .unwrap(),
            1
        );
        assert!(e.store.verify_derived_state().unwrap().is_empty());
        drop(e);
        writes.shutdown(&context()).unwrap();
    }
}
#[test]
#[ignore = "explicit isolated actual WinFsp/FUSE native COW writes, never a user mount"]
fn isolated_kernel_mount_writes_truncates_flushes_and_reopens_native_content() {
    use std::io::{Read, Seek, SeekFrom, Write};
    if let Some(root) = std::env::var_os("TKFS_NATIVE_KERNEL_CHILD") {
        let root = std::path::PathBuf::from(root);
        let point = root.join("native-write-mount");
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
        let path = point.join("file");
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(13)).unwrap();
        file.write_all(b"mounted").unwrap();
        file.set_len(70001).unwrap();
        file.seek(SeekFrom::End(0)).unwrap();
        file.write_all(b"TAIL").unwrap();
        file.sync_all().unwrap();
        assert_eq!(file.metadata().unwrap().len(), 70005);
        let mut expected: Vec<_> = (0..70001).map(|n| (n % 251) as u8).collect();
        expected[13..20].copy_from_slice(b"mounted");
        expected.extend_from_slice(b"TAIL");
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut read = Vec::new();
        file.read_to_end(&mut read).unwrap();
        assert_eq!(read, expected);
        drop(file);
        assert_eq!(std::fs::read(&path).unwrap(), expected);
        tkfs::mount::detach(false).unwrap();
        drop(mounted);
        writes.shutdown(&context()).unwrap();
        let mut e = tkfs::runtime::lock_engine(&shared);
        let reference = e.store.lookup("file").unwrap().content_ref.unwrap();
        assert_eq!(reference.total_len, 70005);
        assert!(e.store.verify_derived_state().unwrap().is_empty());
        let result = serde_json::json!({"platform":std::env::consts::OS,"real_kernel_mount":true,"native_cow":true,"bytes":70005,"write_bytes":11,"truncate":true,"explicit_fsync":true,"reopen_equal":true,"activation":"isolated fixture only"});
        std::fs::write(
            root.join("native-kernel-result.json"),
            serde_json::to_vec(&result).unwrap(),
        )
        .unwrap();
        return;
    }
    let f = Fixture::new();
    f.stop();
    let Fixture {
        shared,
        writes,
        native,
        dir,
        data: _,
    } = f;
    drop(shared);
    drop(writes);
    drop(native);
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "isolated_kernel_mount_writes_truncates_flushes_and_reopens_native_content",
            "--ignored",
            "--nocapture",
        ])
        .env("TKFS_NATIVE_KERNEL_CHILD", dir.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(
        &std::fs::read(dir.path().join("native-kernel-result.json")).unwrap(),
    )
    .unwrap();
    println!("WAVE6_NATIVE_WRITE {result}");
}
