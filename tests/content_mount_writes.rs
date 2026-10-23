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
#[path = "content_mount_writes/create.rs"]
mod create;
#[path = "content_mount_writes/deletion.rs"]
mod deletion;
#[path = "content_mount_writes/directory.rs"]
mod directory;
#[path = "content_mount_writes/directory_changes.rs"]
mod directory_changes;
#[path = "content_mount_writes/directory_kernel.rs"]
mod directory_kernel;
#[path = "content_mount_writes/replacement.rs"]
mod replacement;
#[path = "content_mount_writes/unlink.rs"]
mod unlink;
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
fn current_native_service_needs_no_marker_and_reuses_only_a_running_executor() {
    let dir = tempfile::tempdir().unwrap();
    let shared = Arc::new(Mutex::new(Engine::new(
        Store::initialize(dir.path(), "ordinary", &id()).unwrap(),
    )));
    let first = NativeWrites::install(&shared, &context()).unwrap();
    let second = NativeWrites::install(&shared, &context()).unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    first.shutdown(&context()).unwrap();
    error(
        NativeWrites::install(&shared, &context()),
        "CONTENT_CANCELLED",
    );
}

#[test]
fn native_writer_uses_each_mutating_handles_time_mask_and_flush_keeps_the_write_clock() {
    let f = Fixture::new();
    let a = f.open(true);
    let b = f.open(true);
    let original = tkfs::runtime::lock_engine(&f.shared)
        .session(a)
        .unwrap()
        .entry
        .basic_info();
    tkfs::runtime::lock_engine(&f.shared)
        .handles
        .get_mut(&a)
        .unwrap()
        .time_update_disabled = 2;
    content_writes::write(&f.shared, a, 13, b"first", false, false, wait()).unwrap();
    let first = tkfs::runtime::lock_engine(&f.shared)
        .session(a)
        .unwrap()
        .entry
        .basic_info();
    assert_eq!(first.write_time, original.write_time);
    assert_eq!(f.publications(), 0);
    content_writes::flush(&f.shared, a, wait()).unwrap();
    assert_eq!(
        tkfs::runtime::lock_engine(&f.shared)
            .store
            .lookup("file")
            .unwrap()
            .basic_info(),
        first
    );
    std::thread::sleep(Duration::from_millis(10));
    content_writes::write(&f.shared, b, 20, b"second", false, false, wait()).unwrap();
    let second = tkfs::runtime::lock_engine(&f.shared)
        .session(b)
        .unwrap()
        .entry
        .basic_info();
    assert!(second.write_time > first.write_time);
    std::thread::sleep(Duration::from_millis(20));
    content_writes::flush(&f.shared, b, wait()).unwrap();
    assert_eq!(
        tkfs::runtime::lock_engine(&f.shared)
            .store
            .lookup("file")
            .unwrap()
            .basic_info(),
        second
    );
    std::thread::sleep(Duration::from_millis(10));
    content_writes::write(&f.shared, a, 30, b"third", false, false, wait()).unwrap();
    let third = tkfs::runtime::lock_engine(&f.shared)
        .session(a)
        .unwrap()
        .entry
        .basic_info();
    assert_eq!(third.write_time, second.write_time);
    assert!(third.change_time > second.change_time);
    content_writes::flush(&f.shared, a, wait()).unwrap();
    assert_eq!(
        tkfs::runtime::lock_engine(&f.shared)
            .store
            .lookup("file")
            .unwrap()
            .basic_info(),
        third
    );
    assert_eq!(third.creation_time, original.creation_time);
    assert_eq!(third.access_time, original.access_time);
    f.close(a);
    f.close(b);
    f.oracle();
    f.stop();
    println!(
        "WAVE6_NATIVE_TIMES {}",
        serde_json::json!({"scenario":"writer_masks_and_mutation_clock","handles":2,"mutations":3,"per_handle_mask":true,"local_visibility_before_flush":true,"flush_time_not_used":true,"adapter_setter_exercised":false})
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
fn private_native_read_finishes_while_its_full_checkpoint_is_stalled() {
    let f = Fixture::new();
    let writer = f.open(true);
    let reader = f.open(false);
    content_writes::write(&f.shared, writer, 13, b"changed", false, false, wait()).unwrap();
    let reset = arm(
        "content.write.before_checkpoint_commit",
        FaultAction::Hang,
        1500,
    );
    let shared = f.shared.clone();
    let saving = std::thread::spawn(move || content_writes::flush(&shared, writer, wait()));
    fired();
    let began = Instant::now();
    let read = content_reads::read_mount_vec(
        &f.shared,
        reader,
        13,
        7,
        ContentWait::fixed(Duration::from_millis(500)),
    );
    let elapsed = began.elapsed();
    let still_checkpointing = !saving.is_finished();
    let publications_when_read_finished = f.publications();
    let flushed = saving.join().unwrap();
    drop(reset);
    flushed.unwrap();
    assert_eq!(
        read.expect("a private reader must not wait for FULL checkpoint I/O")
            .unwrap(),
        b"changed"
    );
    assert!(still_checkpointing);
    assert_eq!(publications_when_read_finished, 0);
    assert!(elapsed < Duration::from_millis(300), "{elapsed:?}");
    println!(
        "WAVE6_PRIVATE_READER {}",
        serde_json::json!({
            "scenario":"native_same_file", "read_ms":elapsed.as_secs_f64()*1000.0,
            "bound_ms":300, "deadline_ms":500, "checkpoint_stall_ms":1500,
            "writer_still_checkpointing":still_checkpointing, "private_bytes_equal":true,
            "publications_when_read_finished":publications_when_read_finished,
            "real_full_wal_transaction":true, "physical_disk_stall":false
        })
    );
    assert_eq!(f.publications(), 1);
    f.close(writer);
    f.close(reader);
    f.oracle();
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
fn read_only_native_cleanup_and_close_keep_other_writers_buffered_but_fsync_saves() {
    let f = Fixture::new();
    let writer = f.open(true);
    content_writes::write(&f.shared, writer, 13, b"private", false, false, wait()).unwrap();
    let reader = f.open(false);
    let reset = arm(
        "content.write.before_checkpoint_commit",
        FaultAction::Fail,
        0,
    );
    tkfs::runtime::engine::save_handle_on_cleanup(&f.shared, reader).unwrap();
    tkfs::runtime::lock_engine(&f.shared)
        .cleanup_unsaved(reader)
        .unwrap();
    f.close(reader);
    assert_eq!(fault::fired(), 0, "read-only close must not checkpoint");
    assert_eq!(f.publications(), 0);
    assert!(
        tkfs::runtime::lock_engine(&f.shared)
            .session(writer)
            .unwrap()
            .dirty
    );
    drop(reset);
    let reader = f.open(false);
    tkfs::runtime::engine::save_handle(&f.shared, reader).unwrap();
    assert_eq!(
        f.publications(),
        1,
        "explicit fsync remains a durability point"
    );
    assert!(
        !tkfs::runtime::lock_engine(&f.shared)
            .session(writer)
            .unwrap()
            .dirty
    );
    f.close(reader);
    f.close(writer);
    f.oracle();
    f.stop();
}
#[test]
fn read_only_close_does_not_publish_a_saved_revision_while_its_writer_remains_open() {
    let f = Fixture::new();
    let writer = f.open(true);
    content_writes::write(&f.shared, writer, 13, b"private", false, false, wait()).unwrap();
    content_writes::retain_dirty(&f.shared, &context()).unwrap();
    let reader = f.open(false);
    f.close(reader);
    assert_eq!(
        f.publications(),
        0,
        "a metadata checkpoint is not permission to publish the active writer's buffer"
    );
    assert!(
        tkfs::runtime::lock_engine(&f.shared)
            .session(writer)
            .unwrap()
            .dirty
    );
    f.close(writer);
    assert_eq!(f.publications(), 1);
    f.oracle();
    f.stop();
}
#[test]
fn read_only_cleanup_rechecks_checkpoint_state_after_a_started_native_write() {
    let f = Fixture::new();
    let writer = f.open(true);
    let reader = f.open(false);
    let reset = arm("content.native.before_write", FaultAction::Hang, 500);
    let shared = f.shared.clone();
    let writing = std::thread::spawn(move || {
        content_writes::write(&shared, writer, 0, b"private", false, false, wait())
    });
    fired();
    let shared = f.shared.clone();
    let (done, completed) = std::sync::mpsc::channel();
    let closing = std::thread::spawn(move || {
        let saved = tkfs::runtime::engine::save_handle_on_cleanup(&shared, reader);
        done.send(()).unwrap();
        saved
    });
    assert!(matches!(
        completed.recv_timeout(Duration::from_millis(50)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    writing.join().unwrap().unwrap();
    closing.join().unwrap().unwrap();
    drop(reset);
    assert_eq!(f.publications(), 0);
    assert!(
        tkfs::runtime::lock_engine(&f.shared)
            .session(writer)
            .unwrap()
            .dirty
    );
    f.close(reader);
    assert_eq!(f.publications(), 0);
    f.close(writer);
    assert_eq!(f.publications(), 1);
    f.oracle();
    f.stop();
}
#[test]
fn activation_refreshes_native_fields_without_losing_the_private_mutation_clocks() {
    let f = Fixture::new();
    let h = f.open(true);
    let before = tkfs::runtime::lock_engine(&f.shared)
        .session(h)
        .unwrap()
        .entry
        .basic_info();
    std::thread::sleep(Duration::from_millis(20));
    content_writes::write(&f.shared, h, 13, b"private", false, false, wait()).unwrap();
    let private = tkfs::runtime::lock_engine(&f.shared)
        .session(h)
        .unwrap()
        .entry
        .basic_info();
    assert!(private.write_time > before.write_time);
    {
        let mut e = tkfs::runtime::lock_engine(&f.shared);
        let entity = e.session(h).unwrap().entry.id.clone();
        let mut basic = before;
        basic.posix_mode = Some(0o640);
        basic.access_time = basic.access_time.saturating_sub(10_000_000);
        let change = e
            .store
            .current_change(&entity, "basic", serde_json::json!(basic))
            .unwrap();
        let event = e
            .store
            .event(&e.store.branch(&e.store.active).unwrap(), vec![change]);
        let event_id = event.id.clone();
        e.store.commit(event, false).unwrap();
        e.after_activation(&tkfs::runtime::engine::ActivatedDelta {
            ids: vec![event_id],
            entities: [entity].into_iter().collect(),
            ..Default::default()
        });
        let visible = e.session(h).unwrap().entry.basic_info();
        assert_eq!(
            [visible.write_time, visible.change_time],
            [private.write_time, private.change_time]
        );
        assert_eq!(visible.posix_mode, Some(0o640));
        assert_eq!(visible.access_time, basic.access_time);
        assert!(e.session(h).unwrap().dirty);
    }
    assert_eq!(f.publications(), 0);
    f.close(h);
    f.oracle();
    f.stop();
}
#[test]
fn native_basic_setter_preserves_selected_bytes_and_uses_each_handles_disable_and_reset_flags() {
    let f = Fixture::new();
    let a = f.open(true);
    let b = f.open(true);
    content_writes::write(&f.shared, a, 0, b"first", false, false, wait()).unwrap();
    let reset = arm("content.executor.before_prepare", FaultAction::Fail, 0);
    error(
        content_writes::flush(&f.shared, a, wait()),
        "FAULT_INJECTED",
    );
    drop(reset);
    content_writes::write(&f.shared, a, 13, b"newer", false, false, wait()).unwrap();
    let write_time = tkfs::core::filetime(tkfs::util::time::now_ms() - 5000);
    let change_time = tkfs::core::filetime(tkfs::util::time::now_ms() - 4000);
    assert!(
        content_writes::set_basic(&f.shared, a, 0x80, [0, 0, write_time, change_time], wait())
            .unwrap()
    );
    assert_eq!(f.publications(), 0);
    let basic = tkfs::runtime::lock_engine(&f.shared)
        .session(a)
        .unwrap()
        .entry
        .basic_info();
    assert_eq!(
        [basic.write_time, basic.change_time],
        [write_time, change_time]
    );
    assert_eq!(basic.attributes, 0x80);
    let reader = f.open(false);
    f.close(reader);
    assert_eq!(f.publications(), 0);
    let reset = arm(
        "content.write.before_checkpoint_commit",
        FaultAction::Fail,
        0,
    );
    assert!(
        content_writes::set_basic(&f.shared, a, u32::MAX, [0, 0, u64::MAX, u64::MAX], wait())
            .unwrap()
    );
    assert_eq!(
        fault::fired(),
        0,
        "mask-only setter must not checkpoint bytes"
    );
    drop(reset);
    content_writes::write(&f.shared, a, 30, b"held", false, false, wait()).unwrap();
    let held = tkfs::runtime::lock_engine(&f.shared)
        .session(a)
        .unwrap()
        .entry
        .basic_info();
    assert_eq!(
        [held.write_time, held.change_time],
        [write_time, change_time]
    );
    content_writes::write(&f.shared, b, 40, b"other", false, false, wait()).unwrap();
    let other = tkfs::runtime::lock_engine(&f.shared)
        .session(b)
        .unwrap()
        .entry
        .basic_info();
    assert!(other.write_time > write_time && other.change_time > change_time);
    content_writes::write(&f.shared, a, 50, b"still", false, false, wait()).unwrap();
    let held = tkfs::runtime::lock_engine(&f.shared)
        .session(a)
        .unwrap()
        .entry
        .basic_info();
    assert_eq!(
        [held.write_time, held.change_time],
        [other.write_time, other.change_time]
    );
    assert!(
        content_writes::set_basic(
            &f.shared,
            a,
            u32::MAX,
            [0, 0, u64::MAX - 1, u64::MAX - 1],
            wait()
        )
        .unwrap()
    );
    std::thread::sleep(Duration::from_millis(20));
    content_writes::write(&f.shared, a, 60, b"reset", false, false, wait()).unwrap();
    let reset_basic = tkfs::runtime::lock_engine(&f.shared)
        .session(a)
        .unwrap()
        .entry
        .basic_info();
    assert!(
        reset_basic.write_time > other.write_time && reset_basic.change_time > other.change_time
    );
    content_writes::flush(&f.shared, a, wait()).unwrap();
    assert_eq!(f.publications(), 1);
    assert_eq!(
        tkfs::runtime::lock_engine(&f.shared)
            .store
            .lookup("file")
            .unwrap()
            .basic_info(),
        reset_basic
    );
    f.close(a);
    f.close(b);
    f.oracle();
    f.stop();
}
#[test]
fn native_basic_setter_recovers_a_lost_reply_but_does_not_retry_an_uncommitted_request() {
    for point in ["content.save.before_basic", "content.save.basic_committed"] {
        let f = Fixture::new();
        let h = f.open(true);
        content_writes::write(&f.shared, h, 0, b"private", false, false, wait()).unwrap();
        let reset = arm(point, FaultAction::Fail, 0);
        let result =
            content_writes::set_basic(&f.shared, h, 0x80, [0, 0, u64::MAX, u64::MAX], wait());
        drop(reset);
        let committed = point.ends_with("committed");
        if committed {
            assert!(result.unwrap());
        } else {
            error(result, "FAULT_INJECTED");
        }
        let e = tkfs::runtime::lock_engine(&f.shared);
        assert_eq!(
            e.handles[&h].time_update_disabled,
            if committed { 6 } else { 0 }
        );
        assert_eq!(
            e.store
                .db
                .query_row(
                    "SELECT count(*) FROM content_save_metadata_changes",
                    [],
                    |r| r.get::<_, u64>(0)
                )
                .unwrap(),
            u64::from(committed)
        );
        drop(e);
        assert_eq!(f.publications(), 0);
        content_writes::write(&f.shared, h, 13, b"later", false, false, wait()).unwrap();
        f.close(h);
        assert_eq!(f.publications(), 1);
        f.oracle();
        f.stop();
    }
}
#[test]
fn committed_native_metadata_is_reconciled_before_a_write_after_the_reply_deadline() {
    let f = Fixture::new();
    let h = f.open(true);
    content_writes::write(&f.shared, h, 0, b"private", false, false, wait()).unwrap();
    let before = tkfs::runtime::lock_engine(&f.shared)
        .session(h)
        .unwrap()
        .entry
        .basic_info();
    let reset = arm("content.save.basic_committed", FaultAction::Hang, 1500);
    let result = content_writes::set_basic(
        &f.shared,
        h,
        1,
        [
            0,
            0,
            u64::MAX,
            tkfs::core::filetime(tkfs::util::time::now_ms() - 5000),
        ],
        ContentWait::fixed(Duration::from_secs(1)),
    );
    assert_eq!(
        fault::fired(),
        1,
        "the metadata commit must precede the deadline failure"
    );
    drop(reset);
    error(result, "CONTENT_DEADLINE");
    assert_eq!(f.publications(), 0);
    content_writes::write(&f.shared, h, 13, b"later", false, false, wait()).unwrap();
    let e = tkfs::runtime::lock_engine(&f.shared);
    assert_eq!(e.handles[&h].time_update_disabled, 2);
    assert_eq!(
        e.session(h).unwrap().entry.basic_info().write_time,
        before.write_time,
        "the committed handle policy must be applied before the next byte mutation"
    );
    assert_eq!(e.session(h).unwrap().entry.basic_info().attributes, 0x21);
    assert_eq!(
        e.store
            .db
            .query_row(
                "SELECT count(*) FROM content_save_metadata_changes",
                [],
                |r| r.get::<_, u64>(0)
            )
            .unwrap(),
        1
    );
    drop(e);
    f.close(h);
    assert_eq!(f.publications(), 1);
    f.oracle();
    f.stop();
}
#[test]
fn native_basic_setter_adopts_a_committed_save_without_publishing_newer_private_bytes() {
    let f = Fixture::new();
    let h = f.open(true);
    content_writes::write(&f.shared, h, 0, b"first", false, false, wait()).unwrap();
    let reset = arm("content.executor.published", FaultAction::Fail, 0);
    error(
        content_writes::flush(&f.shared, h, wait()),
        "FAULT_INJECTED",
    );
    drop(reset);
    assert_eq!(f.publications(), 1);
    content_writes::write(&f.shared, h, 13, b"newer", false, false, wait()).unwrap();
    let time = tkfs::core::filetime(tkfs::util::time::now_ms() - 5000);
    assert!(content_writes::set_basic(&f.shared, h, 0x80, [0, 0, time, 0], wait()).unwrap());
    assert_eq!(
        f.publications(),
        1,
        "metadata may adopt the old receipt, not publish the newer bytes"
    );
    assert_eq!(f.bytes(h, 18)[13..18], *b"newer");
    let e = tkfs::runtime::lock_engine(&f.shared);
    assert_eq!(e.session(h).unwrap().entry.basic_info().write_time, time);
    assert_eq!(
        e.session(h).unwrap().content_base,
        e.store
            .lookup("file")
            .unwrap()
            .revisions
            .get("content")
            .cloned()
    );
    drop(e);
    content_writes::flush(&f.shared, h, wait()).unwrap();
    assert_eq!(f.publications(), 2);
    assert_eq!(
        tkfs::runtime::lock_engine(&f.shared)
            .store
            .lookup("file")
            .unwrap()
            .basic_info()
            .write_time,
        time
    );
    f.close(h);
    f.oracle();
    f.stop();
}
#[test]
fn native_rename_prepares_a_closed_source_without_hydrating_its_missing_bytes() {
    rename_immutable_source(false);
}
#[test]
fn native_rename_prepares_a_readonly_source_without_publishing_file_bytes() {
    rename_immutable_source(true);
}
fn rename_immutable_source(readonly: bool) {
    let f = Fixture::new();
    let before = tkfs::runtime::lock_engine(&f.shared)
        .store
        .lookup("file")
        .unwrap();
    let reader = readonly.then(|| f.open(false));
    let chunk = f
        .dir
        .path()
        .join("objects")
        .join(tkfs::model::content::Digest::bytes(&f.data[..CHUNK_SIZE as usize]).to_string());
    std::fs::remove_file(&chunk).unwrap();
    assert!(content_writes::rename(&f.shared, &before.id, "renamed", false, wait()).unwrap());
    assert!(!chunk.exists(), "metadata rename must not hydrate bytes");
    assert_eq!(f.publications(), 0);
    {
        let e = tkfs::runtime::lock_engine(&f.shared);
        assert!(e.store.lookup("file").is_err());
        let after = e.store.lookup("renamed").unwrap();
        assert_eq!(after.id, before.id);
        assert_eq!(after.content_ref, before.content_ref);
        assert_eq!(
            after.revisions.get("content"),
            before.revisions.get("content")
        );
        assert_eq!(e.handles.len(), usize::from(readonly));
        assert_eq!(e.sessions.len(), usize::from(readonly));
        if let Some(handle) = reader {
            assert_eq!(e.handle_entry(handle).unwrap().name, "renamed");
            assert!(!e.handles[&handle].writable);
            assert!(!e.session(handle).unwrap().dirty);
        }
        assert_eq!(
            e.store
                .db
                .query_row("SELECT count(*) FROM pending_saves", [], |r| r
                    .get::<_, u64>(0))
                .unwrap(),
            0
        );
    }
    std::fs::write(&chunk, &f.data[..CHUNK_SIZE as usize]).unwrap();
    let handle = reader.unwrap_or_else(|| {
        let h = tkfs::runtime::lock_engine(&f.shared)
            .open("renamed", None, false)
            .unwrap()
            .0;
        content_reads::prepare_open(&f.shared, h, wait()).unwrap();
        h
    });
    assert_eq!(f.bytes(handle, f.data.len()), f.data);
    f.close(handle);
    assert_eq!(f.publications(), 0);
    f.oracle();
    f.stop();
}

#[test]
fn native_rename_reconciles_a_committed_reply_and_does_not_repeat_an_uncommitted_move() {
    for point in [
        "content.save.before_rename",
        "content.save.rename_committed",
    ] {
        let f = Fixture::new();
        let h = f.open(true);
        let entity = tkfs::runtime::lock_engine(&f.shared)
            .session(h)
            .unwrap()
            .entry
            .id
            .clone();
        content_writes::write(&f.shared, h, 13, b"changed", false, false, wait()).unwrap();
        let reset = arm(point, FaultAction::Fail, 0);
        let result = content_writes::rename(&f.shared, &entity, "renamed", false, wait());
        assert_eq!(fault::fired(), 1);
        drop(reset);
        let committed = point == "content.save.rename_committed";
        if committed {
            assert!(result.unwrap());
        } else {
            error(result, "FAULT_INJECTED");
        }
        content_writes::write(&f.shared, h, 20, b"later", false, false, wait()).unwrap();
        let e = tkfs::runtime::lock_engine(&f.shared);
        assert_eq!(
            e.session(h).unwrap().entry.name,
            if committed { "renamed" } else { "file" }
        );
        assert_eq!(
            e.store
                .lookup(if committed { "renamed" } else { "file" })
                .unwrap()
                .id,
            entity
        );
        assert_eq!(
            e.store
                .db
                .query_row(
                    "SELECT count(*) FROM content_save_namespace_changes",
                    [],
                    |r| r.get::<_, u64>(0)
                )
                .unwrap(),
            u64::from(committed)
        );
        drop(e);
        assert_eq!(f.publications(), 0);
        assert_eq!(&f.bytes(h, 25)[13..25], b"changedlater");
        f.close(h);
        assert_eq!(f.publications(), 1);
        f.oracle();
        f.stop();
    }
}

#[test]
fn native_rename_committed_after_reply_expiry_is_observed_before_the_next_write() {
    let f = Fixture::new();
    let h = f.open(true);
    let entity = tkfs::runtime::lock_engine(&f.shared)
        .session(h)
        .unwrap()
        .entry
        .id
        .clone();
    content_writes::write(&f.shared, h, 13, b"changed", false, false, wait()).unwrap();
    let reset = arm("content.save.rename_committed", FaultAction::Hang, 1500);
    let result = content_writes::rename(
        &f.shared,
        &entity,
        "renamed",
        false,
        ContentWait::fixed(Duration::from_secs(1)),
    );
    assert_eq!(fault::fired(), 1);
    drop(reset);
    error(result, "CONTENT_DEADLINE");
    content_writes::write(&f.shared, h, 20, b"later", false, false, wait()).unwrap();
    assert_eq!(
        tkfs::runtime::lock_engine(&f.shared)
            .session(h)
            .unwrap()
            .entry
            .name,
        "renamed"
    );
    assert_eq!(&f.bytes(h, 25)[13..25], b"changedlater");
    assert_eq!(f.publications(), 0);
    f.close(h);
    assert_eq!(f.publications(), 1);
    f.oracle();
    f.stop();
}

#[test]
fn native_rename_adopts_a_committed_save_without_publishing_newer_private_bytes() {
    let f = Fixture::new();
    let h = f.open(true);
    let entity = tkfs::runtime::lock_engine(&f.shared)
        .session(h)
        .unwrap()
        .entry
        .id
        .clone();
    content_writes::write(&f.shared, h, 0, b"first", false, false, wait()).unwrap();
    let reset = arm("content.executor.published", FaultAction::Fail, 0);
    error(
        content_writes::flush(&f.shared, h, wait()),
        "FAULT_INJECTED",
    );
    drop(reset);
    assert_eq!(f.publications(), 1);
    content_writes::write(&f.shared, h, 13, b"newer", false, false, wait()).unwrap();
    assert!(content_writes::rename(&f.shared, &entity, "renamed", false, wait()).unwrap());
    assert_eq!(f.publications(), 1);
    assert_eq!(f.bytes(h, 18)[13..18], *b"newer");
    let e = tkfs::runtime::lock_engine(&f.shared);
    assert_eq!(e.session(h).unwrap().entry.name, "renamed");
    assert_eq!(
        e.session(h).unwrap().content_base,
        e.store
            .lookup("renamed")
            .unwrap()
            .revisions
            .get("content")
            .cloned()
    );
    drop(e);
    f.close(h);
    assert_eq!(f.publications(), 2);
    f.oracle();
    f.stop();
}

#[test]
fn failed_native_rename_does_not_run_after_the_colliding_destination_is_removed() {
    for replace in [false, true] {
        let f = Fixture::new();
        let h = f.open(true);
        let (entity, target) = {
            let mut e = tkfs::runtime::lock_engine(&f.shared);
            (
                e.session(h).unwrap().entry.id.clone(),
                e.store
                    .create("renamed", if replace { "directory" } else { "file" })
                    .unwrap()
                    .id,
            )
        };
        content_writes::write(&f.shared, h, 13, b"changed", false, false, wait()).unwrap();
        error(
            content_writes::rename(&f.shared, &entity, "renamed", replace, wait()),
            "NAME_COLLISION",
        );
        tkfs::runtime::lock_engine(&f.shared)
            .store
            .delete(&target)
            .unwrap();
        content_writes::write(&f.shared, h, 20, b"later", false, false, wait()).unwrap();
        let e = tkfs::runtime::lock_engine(&f.shared);
        assert_eq!(e.session(h).unwrap().entry.name, "file");
        assert_eq!(e.store.lookup("file").unwrap().id, entity);
        assert!(e.store.lookup("renamed").is_err());
        assert_eq!(
            e.store
                .db
                .query_row(
                    "SELECT count(*) FROM content_save_namespace_changes",
                    [],
                    |r| r.get::<_, u64>(0)
                )
                .unwrap(),
            0
        );
        drop(e);
        assert_eq!(&f.bytes(h, 25)[13..25], b"changedlater");
        f.close(h);
        assert_eq!(f.publications(), 1);
        f.oracle();
        f.stop();
    }
}

#[test]
fn native_rename_reserves_adapter_capacity_before_waiting_for_the_file_gate() {
    static SLOTS: tkfs::runtime::content::WaitSlots = tkfs::runtime::content::WaitSlots::new(1);
    let f = Fixture::new();
    let h = f.open(true);
    let entity = tkfs::runtime::lock_engine(&f.shared)
        .session(h)
        .unwrap()
        .entry
        .id
        .clone();
    content_writes::write(&f.shared, h, 13, b"changed", false, false, wait()).unwrap();
    let reset = arm("content.save.before_rename", FaultAction::Hang, 350);
    let shared = f.shared.clone();
    let source = entity.clone();
    let admitted = ContentWait {
        slots: Some(&SLOTS),
        ..wait()
    };
    let moving = std::thread::spawn(move || {
        content_writes::rename(&shared, &source, "renamed", false, admitted)
    });
    fired();
    assert_eq!(SLOTS.used(), 1);
    let began = Instant::now();
    error(
        content_writes::rename(&f.shared, &entity, "other", false, admitted),
        "CONTENT_RESOURCE_LIMIT",
    );
    assert!(began.elapsed() < Duration::from_millis(100));
    assert!(moving.join().unwrap().unwrap());
    drop(reset);
    assert_eq!(SLOTS.used(), 0);
    let e = tkfs::runtime::lock_engine(&f.shared);
    assert_eq!(e.store.lookup("renamed").unwrap().id, entity);
    assert!(e.store.lookup("other").is_err());
    drop(e);
    assert_eq!(f.publications(), 0);
    f.close(h);
    assert_eq!(f.publications(), 1);
    f.oracle();
    f.stop();
}

#[test]
fn native_time_masks_preserve_earlier_buffered_clocks_before_one_fsync() {
    for mask in [2, 4, 6] {
        let f = Fixture::new();
        let automatic = f.open(true);
        let disabled = f.open(true);
        tkfs::runtime::lock_engine(&f.shared)
            .handles
            .get_mut(&disabled)
            .unwrap()
            .time_update_disabled = mask;
        std::thread::sleep(Duration::from_millis(20));
        content_writes::write(&f.shared, automatic, 13, b"first", false, false, wait()).unwrap();
        let first = tkfs::runtime::lock_engine(&f.shared)
            .session(automatic)
            .unwrap()
            .entry
            .basic_info();
        std::thread::sleep(Duration::from_millis(20));
        content_writes::write(&f.shared, disabled, 30, b"second", false, false, wait()).unwrap();
        let buffered = tkfs::runtime::lock_engine(&f.shared)
            .session(automatic)
            .unwrap()
            .entry
            .basic_info();
        if mask & 2 != 0 {
            assert_eq!(buffered.write_time, first.write_time);
        } else {
            assert!(buffered.write_time > first.write_time);
        }
        if mask & 4 != 0 {
            assert_eq!(buffered.change_time, first.change_time);
        } else {
            assert!(buffered.change_time > first.change_time);
        }
        assert_eq!(f.publications(), 0);
        content_writes::flush(&f.shared, disabled, wait()).unwrap();
        assert_eq!(
            tkfs::runtime::lock_engine(&f.shared)
                .store
                .lookup("file")
                .unwrap()
                .basic_info(),
            buffered,
            "one fsync must preserve both buffered clocks across per-handle masks"
        );
        f.close(automatic);
        f.close(disabled);
        f.oracle();
        f.stop();
    }
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
    if std::env::var_os("TKFS_NATIVE_BASIC_CHILD").is_some() {
        content_writes::write(&shared, h, 0, b"metadata-private", false, false, wait()).unwrap();
        content_writes::retain_dirty(&shared, &context()).unwrap();
        let journal = tkfs::content::write::WriteJournal::open(&root).unwrap();
        let saved = journal
            .pending(None, 64, &context())
            .unwrap()
            .pop()
            .unwrap();
        let mut store = Store::open_existing(&root).unwrap();
        let mut basic = store.entry(&saved.intent.base.entity).unwrap().basic_info();
        basic.write_time = tkfs::core::filetime(tkfs::util::time::now_ms() - 2000);
        basic.change_time = tkfs::core::filetime(tkfs::util::time::now_ms() - 1000);
        basic.posix_mode = Some(0o640);
        basic.attributes = 0x80;
        std::fs::write(
            root.join("basic-oracle.json"),
            serde_json::to_vec(&basic).unwrap(),
        )
        .unwrap();
        store
            .apply_content_checkpoint_basic(
                &saved.intent,
                uuid::Uuid::new_v4(),
                Some(&saved),
                tkfs::core::content_saves::ContentBasicUpdate {
                    write_time: Some(basic.write_time),
                    change_time: Some(basic.change_time),
                    posix_mode: basic.posix_mode,
                    attributes: Some(basic.attributes),
                    ..Default::default()
                },
                &context(),
            )
            .unwrap();
        panic!("metadata commit boundary did not exit");
    }
    let clock_mask = std::env::var("TKFS_NATIVE_CLOSE_CLOCKS")
        .ok()
        .map(|mask| mask.parse::<u8>().unwrap());
    if clock_mask.is_some() {
        std::thread::sleep(Duration::from_millis(20));
    }
    content_writes::write(&shared, h, 0, b"durable-child", false, false, wait()).unwrap();
    if let Some(mask) = clock_mask {
        let other = tkfs::runtime::lock_engine(&shared)
            .open("file", None, true)
            .unwrap()
            .0;
        content_reads::prepare_open(&shared, other, wait()).unwrap();
        tkfs::runtime::lock_engine(&shared)
            .handles
            .get_mut(&other)
            .unwrap()
            .time_update_disabled = mask;
        std::thread::sleep(Duration::from_millis(20));
        content_writes::write(&shared, other, 30, b"after", false, false, wait()).unwrap();
        let basic = tkfs::runtime::lock_engine(&shared)
            .session(other)
            .unwrap()
            .entry
            .basic_info();
        std::fs::write(
            root.join("clock-oracle.json"),
            serde_json::to_vec(&basic).unwrap(),
        )
        .unwrap();
        tkfs::runtime::engine::close_handle(&shared, other).unwrap();
    }
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
fn saved_native_clock_pairs_survive_process_exit_before_publication_and_reopen() {
    for mask in [2, 4, 6] {
        let f = Fixture::new();
        f.stop();
        let Fixture {
            shared,
            writes,
            native,
            dir,
            mut data,
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
            .env("TKFS_NATIVE_CLOSE_CLOCKS", mask.to_string())
            .env("TKFS_FAULT_POINT", "content.executor.before_prepare")
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
        let expected: tkfs::core::BasicInfo =
            serde_json::from_slice(&std::fs::read(dir.path().join("clock-oracle.json")).unwrap())
                .unwrap();
        let shared = Arc::new(Mutex::new(Engine::new(
            Store::open_existing(dir.path()).unwrap(),
        )));
        let writes = NativeWrites::install(&shared, &context()).unwrap();
        let h = tkfs::runtime::lock_engine(&shared)
            .open("file", None, false)
            .unwrap()
            .0;
        assert_eq!(
            tkfs::runtime::lock_engine(&shared)
                .session(h)
                .unwrap()
                .entry
                .basic_info(),
            expected,
            "recovered cache must use the exact Saved clock pair"
        );
        data[..13].copy_from_slice(b"durable-child");
        data[30..35].copy_from_slice(b"after");
        assert_eq!(
            content_reads::read_mount_vec(&shared, h, 0, data.len(), wait())
                .unwrap()
                .unwrap(),
            data
        );
        tkfs::runtime::engine::close_handle(&shared, h).unwrap();
        let mut e = tkfs::runtime::lock_engine(&shared);
        assert_eq!(e.store.lookup("file").unwrap().basic_info(), expected);
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
fn owned_metadata_after_saved_survives_process_exit_and_native_recovery() {
    let f = Fixture::new();
    f.stop();
    let Fixture {
        shared,
        writes,
        native,
        dir,
        mut data,
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
        .env("TKFS_NATIVE_BASIC_CHILD", "1")
        .env("TKFS_FAULT_POINT", "content.save.basic_committed")
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
    let expected: tkfs::core::BasicInfo =
        serde_json::from_slice(&std::fs::read(dir.path().join("basic-oracle.json")).unwrap())
            .unwrap();
    let store = Store::open_existing(dir.path()).unwrap();
    assert_eq!(
        store
            .db
            .query_row("SELECT count(*) FROM content_save_history", [], |r| r
                .get::<_, u64>(0))
            .unwrap(),
        0
    );
    let shared = Arc::new(Mutex::new(Engine::new(store)));
    let writes = NativeWrites::install(&shared, &context()).unwrap();
    let h = tkfs::runtime::lock_engine(&shared)
        .open("file", None, false)
        .unwrap()
        .0;
    assert_eq!(
        tkfs::runtime::lock_engine(&shared)
            .session(h)
            .unwrap()
            .entry
            .basic_info(),
        expected
    );
    data[..16].copy_from_slice(b"metadata-private");
    assert_eq!(
        content_reads::read_mount_vec(&shared, h, 0, data.len(), wait())
            .unwrap()
            .unwrap(),
        data
    );
    tkfs::runtime::engine::close_handle(&shared, h).unwrap();
    let mut e = tkfs::runtime::lock_engine(&shared);
    assert_eq!(e.store.lookup("file").unwrap().basic_info(), expected);
    assert_eq!(
        e.store
            .db
            .query_row("SELECT count(*) FROM content_save_history", [], |r| r
                .get::<_, u64>(0))
            .unwrap(),
        1
    );
    assert!(e.store.verify_derived_state().unwrap().is_empty());
    drop(e);
    writes.shutdown(&context()).unwrap();
}
#[test]
#[ignore = "explicit isolated actual WinFsp/FUSE native COW writes, never a user mount"]
fn isolated_kernel_mount_writes_truncates_flushes_and_reopens_native_content() {
    native_kernel_write_scenario(false);
}
#[cfg(windows)]
#[test]
#[ignore = "unresolved WinFsp 2.1 strips -1 before SetBasicInfo; explicit failing compatibility probe"]
fn isolated_kernel_windows_time_policy_requires_an_observable_driver_contract() {
    native_kernel_write_scenario(true);
}
fn native_kernel_write_scenario(time_policy_probe: bool) {
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
        let before_open = std::fs::metadata(&path).unwrap();
        assert_eq!(
            before_open.len(),
            3 * CHUNK_SIZE,
            "typed descriptor size before open"
        );
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(13)).unwrap();
        std::thread::sleep(Duration::from_millis(20));
        file.write_all(b"mounted").unwrap();
        let cached_write_time = {
            let e = tkfs::runtime::lock_engine(&shared);
            let entry = e.store.lookup("file").unwrap();
            let session = e.sessions.get(&entry.id).unwrap();
            assert!(
                session.dirty,
                "metadata check must precede content publication"
            );
            session.entry.basic_info().write_time
        };
        let expected_ms = cached_write_time.saturating_sub(tkfs::core::FILETIME_EPOCH) / 10_000;
        let path_ms = std::fs::metadata(&path)
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let handle_ms = file
            .metadata()
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert_eq!(path_ms, expected_ms, "dirty path timestamp before fsync");
        assert_eq!(
            handle_ms, expected_ms,
            "dirty handle timestamp before fsync"
        );
        let reader = std::fs::File::open(&path).unwrap();
        assert_eq!(reader.metadata().unwrap().len(), 3 * CHUNK_SIZE);
        drop(reader);
        {
            let e = tkfs::runtime::lock_engine(&shared);
            let entity = e.store.lookup("file").unwrap().id;
            assert!(
                e.sessions[&entity].dirty,
                "metadata reads must not publish the writer's buffer"
            );
            let publications: u64 = e
                .store
                .db
                .query_row("SELECT count(*) FROM content_save_history", [], |r| {
                    r.get(0)
                })
                .unwrap();
            assert_eq!(
                publications, 0,
                "metadata reads must not checkpoint/publish unrelated dirty bytes"
            );
        }
        let explicit_ms = tkfs::util::time::now_ms() - 5000;
        file.set_times(
            std::fs::FileTimes::new()
                .set_modified(std::time::UNIX_EPOCH + Duration::from_millis(explicit_ms)),
        )
        .unwrap();
        for metadata in [file.metadata().unwrap(), std::fs::metadata(&path).unwrap()] {
            assert_eq!(
                metadata
                    .modified()
                    .unwrap()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                explicit_ms,
                "native set-basic must be visible before fsync"
            );
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o640
            );
            assert_eq!(
                file.metadata()
                    .unwrap()
                    .modified()
                    .unwrap()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                explicit_ms,
                "chmod must retain the explicit write-time of the same byte revision"
            );
        }
        #[cfg(windows)]
        if time_policy_probe {
            use std::os::windows::io::AsRawHandle;
            // https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/wdm/ns-wdm-_file_basic_information
            #[repr(C)]
            struct Basic {
                times: [u64; 4],
                attributes: u32,
            }
            #[link(name = "kernel32")]
            unsafe extern "system" {
                fn SetFileInformationByHandle(
                    handle: *mut std::ffi::c_void,
                    class: u32,
                    info: *const std::ffi::c_void,
                    size: u32,
                ) -> i32;
            }
            let native_handle = file.as_raw_handle();
            let policy = |value: u64| {
                let basic = Basic {
                    times: [0, 0, value, value],
                    attributes: 0,
                };
                let result = unsafe {
                    SetFileInformationByHandle(
                        native_handle,
                        0,
                        (&basic as *const Basic).cast(),
                        std::mem::size_of::<Basic>() as u32,
                    )
                };
                assert_ne!(
                    result,
                    0,
                    "Win32 time policy: {}",
                    std::io::Error::last_os_error()
                );
            };
            let (held, metadata_events) = {
                let e = tkfs::runtime::lock_engine(&shared);
                let entity = e.store.lookup("file").unwrap().id;
                (
                    e.sessions[&entity].entry.basic_info(),
                    e.store
                        .db
                        .query_row(
                            "SELECT count(*) FROM content_save_metadata_changes",
                            [],
                            |r| r.get::<_, u64>(0),
                        )
                        .unwrap(),
                )
            };
            policy(u64::MAX);
            std::thread::sleep(Duration::from_millis(20));
            file.seek(SeekFrom::Start(13)).unwrap();
            file.write_all(b"mounted").unwrap();
            {
                let e = tkfs::runtime::lock_engine(&shared);
                let entity = e.store.lookup("file").unwrap().id;
                let basic = e.sessions[&entity].entry.basic_info();
                assert_eq!(
                    [basic.write_time, basic.change_time],
                    [held.write_time, held.change_time]
                );
                assert!(
                    e.handles.values().any(|h| h.entity == entity
                        && h.writable
                        && h.time_update_disabled & 6 == 6)
                );
            }
            policy(u64::MAX - 1);
            std::thread::sleep(Duration::from_millis(20));
            file.seek(SeekFrom::Start(13)).unwrap();
            file.write_all(b"mounted").unwrap();
            {
                let e = tkfs::runtime::lock_engine(&shared);
                let entity = e.store.lookup("file").unwrap().id;
                let basic = e.sessions[&entity].entry.basic_info();
                assert!(basic.write_time > held.write_time && basic.change_time > held.change_time);
                assert_eq!(
                    e.store
                        .db
                        .query_row(
                            "SELECT count(*) FROM content_save_metadata_changes",
                            [],
                            |r| r.get::<_, u64>(0)
                        )
                        .unwrap(),
                    metadata_events,
                    "Win32 mask-only calls must not commit metadata events"
                );
            }
        }
        {
            let e = tkfs::runtime::lock_engine(&shared);
            let entity = e.store.lookup("file").unwrap().id;
            assert!(e.sessions[&entity].dirty);
            if !time_policy_probe {
                assert_eq!(
                    e.sessions[&entity].entry.basic_info().write_time,
                    tkfs::core::filetime(explicit_ms)
                );
            }
            assert_eq!(
                e.store
                    .db
                    .query_row("SELECT count(*) FROM content_save_history", [], |r| r
                        .get::<_, u64>(0))
                    .unwrap(),
                0,
                "native metadata changes and their stat queries must not publish the active writer's bytes"
            );
        }
        let moved = point.join("renamed-file");
        let rename_file = {
            let e = tkfs::runtime::lock_engine(&shared);
            let entity = e.store.lookup("file").unwrap().id;
            match &e.sessions[&entity].bytes {
                tkfs::staging::Staged::Cow(file) => file.clone(),
                _ => panic!("expected the native private file"),
            }
        };
        std::fs::rename(&path, &moved).unwrap();
        assert!(!path.exists());
        assert_eq!(std::fs::metadata(&moved).unwrap().len(), 3 * CHUNK_SIZE);
        let moved_bytes = std::fs::read(&moved).unwrap();
        assert_eq!(&moved_bytes[13..20], b"mounted");
        std::fs::rename(&moved, &path).unwrap();
        assert!(!moved.exists());
        {
            let e = tkfs::runtime::lock_engine(&shared);
            let entity = e.store.lookup("file").unwrap().id;
            match &e.sessions[&entity].bytes {
                tkfs::staging::Staged::Cow(file) => {
                    assert!(Arc::ptr_eq(file, &rename_file));
                    assert_eq!(file.operation(), rename_file.operation());
                }
                _ => panic!("rename replaced the native private file"),
            }
            assert_eq!(
                e.store
                    .db
                    .query_row(
                        "SELECT count(*) FROM content_save_namespace_changes",
                        [],
                        |r| r.get::<_, u64>(0)
                    )
                    .unwrap(),
                2
            );
        }
        assert_eq!(
            tkfs::runtime::lock_engine(&shared)
                .store
                .db
                .query_row("SELECT count(*) FROM content_save_history", [], |r| r
                    .get::<_, u64>(0))
                .unwrap(),
            0,
            "renaming an open native file must not publish its buffered bytes"
        );
        let checkpoint_probe = arm("content.write.before_checkpoint", FaultAction::Fail, 0);
        file.set_len(70001).unwrap();
        assert_eq!(
            fault::fired(),
            0,
            "truncate through an open handle must stay buffered until fsync/close"
        );
        drop(checkpoint_probe);
        {
            let e = tkfs::runtime::lock_engine(&shared);
            assert_eq!(
                e.store
                    .db
                    .query_row("SELECT count(*) FROM content_save_history", [], |r| r
                        .get::<_, u64>(0))
                    .unwrap(),
                0,
                "truncate must not publish the active writer's buffered bytes"
            );
        }
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
        let result = serde_json::json!({"platform":std::env::consts::OS,"real_kernel_mount":true,"native_cow":true,"bytes":70005,"write_bytes":if time_policy_probe {25} else {11},"truncate":true,"truncate_without_full_checkpoint":true,"truncate_without_byte_publication":true,"explicit_fsync":true,"reopen_equal":true,"descriptor_len_before_open":true,"dirty_path_and_handle_clock":true,"metadata_reads_do_not_publish":true,"read_only_close_does_not_publish":true,"native_set_basic":true,"metadata_visible_before_fsync":true,"metadata_setter_does_not_publish_bytes":true,"native_posix_mode":cfg!(target_os="linux"),"native_windows_time_flags":time_policy_probe,"native_rename_open_handle":true,"rename_read_new_path_private_bytes":true,"rename_without_byte_publication":true,"write_after_rename":true,"activation":"isolated fixture only"});
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
            if time_policy_probe {
                "isolated_kernel_windows_time_policy_requires_an_observable_driver_contract"
            } else {
                "isolated_kernel_mount_writes_truncates_flushes_and_reopens_native_content"
            },
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
