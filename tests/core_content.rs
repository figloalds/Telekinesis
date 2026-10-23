use serde_json::{Value, json};
use std::{
    io::{self, Read},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tkfs::{
    content::{ContentProvider, LocalContent, ReadContext},
    core::{Store, id},
    model::{
        content::{CHUNK_SIZE, ContentKind},
        ipc::StopMode,
    },
    runtime::{self, Engine, RpcServer, Shared},
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
fn data() -> Vec<u8> {
    (0..2 * CHUNK_SIZE as usize + 17)
        .map(|n| (n % 251) as u8)
        .collect()
}
fn bytes(store: &Store, path: &str) -> Vec<u8> {
    let entry = store.lookup(path).unwrap();
    assert!(entry.content.is_none());
    let reference = entry.content_ref.unwrap();
    let local = LocalContent::open(&store.root).unwrap();
    let pin = local.pin(&reference).unwrap();
    let mut out = vec![0; reference.total_len as usize];
    local.read_range(&pin, 0, &mut out, &context()).unwrap();
    out
}
fn oracle(store: &mut Store) {
    assert!(store.verify_derived_state().unwrap().is_empty());
    for event in store.events().unwrap() {
        for change in event.changes.iter().filter(|c| c.field == "content") {
            assert!(change.value.is_object(), "bare content: {change:?}");
            let reference: tkfs::model::content::FileContentRef =
                serde_json::from_value(change.value.clone()).unwrap();
            assert!(matches!(
                reference.kind,
                ContentKind::Direct | ContentKind::Manifest
            ));
        }
    }
}
#[test]
fn fresh_core_create_and_direct_write_use_current_descriptors_and_atomic_admission() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::initialize(dir.path(), "core-current", &id()).unwrap();
    let file = store.create("file", "file").unwrap();
    assert!(file.content.is_none());
    assert_eq!(file.content_ref.as_ref().unwrap().kind, ContentKind::Direct);
    assert_eq!(bytes(&store, "file"), b"");
    assert_eq!(store.content_admission_page(0, 64).unwrap().len(), 1);
    let revision = store
        .write_revision(
            &file.id,
            file.revisions.get("content").cloned(),
            b"direct bytes",
        )
        .unwrap();
    let event = store
        .events()
        .unwrap()
        .into_iter()
        .find(|e| e.id == revision)
        .unwrap();
    for change in event
        .changes
        .iter()
        .filter(|c| ["content", "alive"].contains(&c.field.as_str()))
    {
        assert_eq!(change.parents, vec![file.revisions[&change.field].clone()]);
    }
    assert_eq!(bytes(&store, "file"), b"direct bytes");
    assert_eq!(store.content_admission_page(0, 64).unwrap().len(), 2);
    oracle(&mut store);
    drop(store);
    let store = Store::open_existing(dir.path()).unwrap();
    assert_eq!(bytes(&store, "file"), b"direct bytes");
}
#[test]
fn stream_import_is_chunk_bounded_exact_length_and_does_not_store_a_whole_file_object() {
    struct Bounded<'a> {
        remaining: &'a [u8],
        largest: usize,
    }
    impl Read for Bounded<'_> {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            assert!(out.len() <= CHUNK_SIZE as usize, "whole-file read buffer");
            self.largest = self.largest.max(out.len());
            self.remaining.read(out)
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::initialize(dir.path(), "core-stream", &id()).unwrap();
    let file = store.create("large", "file").unwrap();
    let data = data();
    let mut source = data.clone();
    source.extend_from_slice(b"tail belongs to caller");
    let mut source = Bounded {
        remaining: &source,
        largest: 0,
    };
    store
        .write_stream_with_base_and_times(
            &file.id,
            file.revisions.get("content").cloned(),
            file.revisions.get("alive").cloned(),
            &mut source,
            data.len() as u64,
            0,
        )
        .unwrap();
    assert_eq!(source.remaining, b"tail belongs to caller");
    assert_eq!(source.largest, CHUNK_SIZE as usize);
    assert_eq!(
        store.lookup("large").unwrap().content_ref.unwrap().kind,
        ContentKind::Manifest
    );
    assert_eq!(bytes(&store, "large"), data);
    for entry in std::fs::read_dir(dir.path().join("objects")).unwrap() {
        assert!(entry.unwrap().metadata().unwrap().len() <= CHUNK_SIZE);
    }
    oracle(&mut store);
}
#[test]
fn explicit_content_and_alive_parents_preserve_concurrent_edits_and_clocks() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::initialize(dir.path(), "core-parents", &id()).unwrap();
    let file = store
        .create_with_attributes("file", "file", Some(32))
        .unwrap();
    let basic = file.basic_info();
    std::thread::sleep(Duration::from_millis(10));
    let a = store
        .write_with_base_and_times(
            &file.id,
            file.revisions.get("content").cloned(),
            file.revisions.get("alive").cloned(),
            b"a",
            2,
        )
        .unwrap();
    let after = store.entry(&file.id).unwrap().basic_info();
    assert_eq!(after.write_time, basic.write_time);
    assert!(after.change_time > basic.change_time);
    let b = store
        .write_with_base(
            &file.id,
            file.revisions.get("content").cloned(),
            file.revisions.get("alive").cloned(),
            b"b",
        )
        .unwrap();
    let projection = store.projection(&store.active).unwrap();
    assert_eq!(projection.heads[&format!("{}/content", file.id)].len(), 2);
    for revision in [a, b] {
        let event = store
            .events()
            .unwrap()
            .into_iter()
            .find(|e| e.id == revision)
            .unwrap();
        for c in event
            .changes
            .iter()
            .filter(|c| ["content", "alive"].contains(&c.field.as_str()))
        {
            assert_eq!(c.parents, vec![file.revisions[&c.field].clone()]);
        }
    }
    oracle(&mut store);
}
#[test]
fn failed_import_never_commits_and_a_corrupt_deduplicated_chunk_is_repaired() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::initialize(dir.path(), "core-repair", &id()).unwrap();
    let file = store.create("file", "file").unwrap();
    let initial = store.events().unwrap().len();
    assert!(
        store
            .write_stream_with_base_and_times(
                &file.id,
                file.revisions.get("content").cloned(),
                file.revisions.get("alive").cloned(),
                &mut b"short".as_slice(),
                CHUNK_SIZE + 3,
                0
            )
            .is_err()
    );
    assert_eq!(store.events().unwrap().len(), initial);
    assert_eq!(bytes(&store, "file"), b"");
    store
        .write_revision(&file.id, file.revisions.get("content").cloned(), b"correct")
        .unwrap();
    let entry = store.entry(&file.id).unwrap();
    let path = dir
        .path()
        .join("objects")
        .join(entry.content_ref.as_ref().unwrap().digest.to_string());
    std::fs::write(&path, b"CORRUPT").unwrap();
    store
        .write_revision(
            &file.id,
            entry.revisions.get("content").cloned(),
            b"correct",
        )
        .unwrap();
    assert_eq!(bytes(&store, "file"), b"correct");
    assert!(
        std::fs::read_dir(dir.path().join("quarantine-content"))
            .unwrap()
            .count()
            >= 1
    );
    oracle(&mut store);
}
#[test]
fn prepared_publication_refuses_another_store_and_uses_no_byte_io_at_commit() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::initialize(dir.path(), "core-pin", &id()).unwrap();
    let file = store.create("file", "file").unwrap();
    let other = tempfile::tempdir().unwrap();
    let other = LocalContent::open(other.path()).unwrap();
    let data = data();
    let prepared = other
        .prepare_import(&mut data.as_slice(), data.len() as u64, &context())
        .unwrap();
    assert!(
        store
            .write_prepared_content_with_base_and_times(
                &file.id,
                file.revisions.get("content").cloned(),
                file.revisions.get("alive").cloned(),
                &prepared,
                0
            )
            .unwrap_err()
            .to_string()
            .contains("CONTENT_SAVE_WRONG_STORE")
    );
    let local = LocalContent::open(dir.path()).unwrap();
    let prepared = local
        .prepare_import(&mut data.as_slice(), data.len() as u64, &context())
        .unwrap();
    let counters = local.counters.clone();
    let before = serde_json::to_value(counters.snapshot()).unwrap();
    store
        .write_prepared_content_with_base_and_times(
            &file.id,
            file.revisions.get("content").cloned(),
            file.revisions.get("alive").cloned(),
            &prepared,
            0,
        )
        .unwrap();
    let after = serde_json::to_value(counters.snapshot()).unwrap();
    assert_eq!(before, after, "publication performed content work");
    assert_eq!(bytes(&store, "file"), data);
    oracle(&mut store);
}
struct Running {
    shared: Shared,
    server: RpcServer,
    dir: tempfile::TempDir,
}
impl Running {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::initialize(dir.path(), "core-rpc", &id()).unwrap();
        let shared = Arc::new(Mutex::new(Engine::new(store)));
        let ctx = tkfs::worker::build_ctx(shared.clone(), dir.path(), false, None);
        let server = runtime::server::start_rpc_server(ctx).unwrap();
        Self {
            dir,
            shared,
            server,
        }
    }
    fn call(&self, request: &str, payload: Value) -> anyhow::Result<Value> {
        runtime::rpc(self.server.discovery(), request, payload)
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.server.stop(StopMode::Force);
    }
}
struct FaultReset;
impl Drop for FaultReset {
    fn drop(&mut self) {
        fault::configure(None);
    }
}
fn arm() -> FaultReset {
    let mut config = FaultConfig::new("content.import.prepared", FaultAction::Hang);
    config.ms = Some(1000);
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
#[test]
fn rpc_import_prepares_off_engine_and_receipt_replay_is_exact() {
    let worker = Running::new();
    let data = data();
    let payload = json!({"op":"import","path":"file","hex":hex::encode(&data)});
    let request = id();
    let _fault = arm();
    let info = worker.server.discovery().clone();
    let body = payload.clone();
    let key = request.clone();
    let call = std::thread::spawn(move || runtime::rpc(&info, &key, body));
    fired();
    {
        let engine = worker
            .shared
            .try_lock()
            .expect("Engine held across import preparation");
        assert!(engine.store.db.is_autocommit());
        assert!(engine.store.lookup("file").is_err());
        assert!(engine.store.receipt(&request, &payload).unwrap().is_none());
    }
    let reply = call.join().unwrap().unwrap();
    fault::configure(None);
    let mut engine = runtime::lock_engine(&worker.shared);
    let events = engine.store.events().unwrap().len();
    assert_eq!(bytes(&engine.store, "file"), data);
    oracle(&mut engine.store);
    drop(engine);
    assert_eq!(worker.call(&request, payload).unwrap(), reply);
    let reused = worker
        .call(
            &request,
            json!({"op":"import","path":"file","hex":hex::encode(b"changed")}),
        )
        .unwrap_err();
    assert!(format!("{reused:#}").contains("REQUEST_ID_REUSED"));
    assert_eq!(
        runtime::lock_engine(&worker.shared)
            .store
            .events()
            .unwrap()
            .len(),
        events
    );
    assert!(worker.dir.path().join("metadata.sqlite").is_file());
}
#[test]
fn rpc_import_receipt_failure_rolls_back_creation_content_and_admission_together() {
    let worker = Running::new();
    let payload = json!({"op":"import","path":"file","hex":hex::encode(data())});
    let request = id();
    {
        let engine = runtime::lock_engine(&worker.shared);
        engine.store.db.execute_batch("CREATE TEMP TRIGGER reject_import_receipt BEFORE INSERT ON receipts BEGIN SELECT RAISE(ABORT,'receipt failure'); END").unwrap();
    }
    assert!(worker.call(&request, payload.clone()).is_err());
    {
        let engine = runtime::lock_engine(&worker.shared);
        assert!(engine.store.db.is_autocommit());
        assert!(engine.store.lookup("file").is_err());
        assert!(
            engine
                .store
                .content_admission_page(0, 64)
                .unwrap()
                .is_empty()
        );
        assert!(engine.store.receipt(&request, &payload).unwrap().is_none());
        engine
            .store
            .db
            .execute_batch("DROP TRIGGER reject_import_receipt")
            .unwrap();
    }
    worker.call(&request, payload).unwrap();
    let mut engine = runtime::lock_engine(&worker.shared);
    assert_eq!(bytes(&engine.store, "file"), data());
    assert_eq!(engine.store.content_admission_page(0, 64).unwrap().len(), 2);
    oracle(&mut engine.store);
}
#[test]
fn rpc_import_cannot_replace_an_incarnation_that_changed_during_preparation() {
    let worker = Running::new();
    worker
        .call(
            &id(),
            json!({"op":"import","path":"file","hex":hex::encode(b"original")}),
        )
        .unwrap();
    let request = id();
    let payload = json!({"op":"import","path":"file","hex":hex::encode(data())});
    let _fault = arm();
    let info = worker.server.discovery().clone();
    let key = request.clone();
    let body = payload.clone();
    let call = std::thread::spawn(move || runtime::rpc(&info, &key, body));
    fired();
    {
        let mut engine = worker.shared.try_lock().unwrap();
        let old = engine.store.lookup("file").unwrap();
        engine.store.rename(&old.id, "moved", false).unwrap();
        engine.store.create("file", "file").unwrap();
    }
    let error = call.join().unwrap().unwrap_err();
    assert!(format!("{error:#}").contains("STALE_VIEW"));
    let mut engine = runtime::lock_engine(&worker.shared);
    assert_eq!(bytes(&engine.store, "file"), b"");
    assert_eq!(bytes(&engine.store, "moved"), b"original");
    assert!(engine.store.receipt(&request, &payload).unwrap().is_none());
    oracle(&mut engine.store);
}

#[test]
fn content_preparation_refuses_outer_transactions_and_publication_requires_full_wal() {
    struct NeverRead;
    impl Read for NeverRead {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            panic!("read inside metadata transaction");
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::initialize(dir.path(), "core-full", &id()).unwrap();
    let file = store.create("file", "file").unwrap();
    let local = LocalContent::open(dir.path()).unwrap();
    let prepared = local
        .prepare_import(&mut b"durable".as_slice(), 7, &context())
        .unwrap();
    store.db.execute_batch("BEGIN IMMEDIATE").unwrap();
    let error = store
        .write_stream_with_base_and_times(
            &file.id,
            file.revisions.get("content").cloned(),
            file.revisions.get("alive").cloned(),
            &mut NeverRead,
            7,
            0,
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("CONTENT_SAVE_REQUIRES_OUTER_TRANSACTION")
    );
    // The prepared pin is explicitly kept through the caller's outer commit.
    store
        .write_prepared_content_with_base_and_times(
            &file.id,
            file.revisions.get("content").cloned(),
            file.revisions.get("alive").cloned(),
            &prepared,
            0,
        )
        .unwrap();
    store
        .db
        .execute_batch("COMMIT; PRAGMA synchronous=NORMAL")
        .unwrap();
    let entry = store.entry(&file.id).unwrap();
    let before = store.events().unwrap().len();
    let error = store
        .write_prepared_content_with_base_and_times(
            &file.id,
            entry.revisions.get("content").cloned(),
            entry.revisions.get("alive").cloned(),
            &prepared,
            0,
        )
        .unwrap_err();
    assert!(error.to_string().contains("CONTENT_SAVE_REQUIRES_FULL_WAL"));
    assert_eq!(store.events().unwrap().len(), before);
    store.db.execute_batch("PRAGMA synchronous=FULL").unwrap();
    assert_eq!(bytes(&store, "file"), b"durable");
    oracle(&mut store);
}

#[test]
fn typed_data_changes_notify_full_and_incremental_views_with_unchanged_clocks() {
    use tkfs::{
        core::ROOT,
        model::content::{Digest, FileContentRef},
        runtime::notify::{ACTION_MODIFIED, delta_notifications, namespace_notifications},
    };
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::initialize(dir.path(), "core-notify", &id()).unwrap();
    let file = store.create("file", "file").unwrap();
    assert_eq!(file.parent, ROOT);
    let timestamp = file.modified_ms;
    for data in [b"first", b"other"] {
        let reference = FileContentRef::new(ContentKind::Direct, Digest::bytes(data), 5).unwrap();
        let change = store
            .current_change(&file.id, "content", json!(reference))
            .unwrap();
        let mut event = store.event(&store.branch(&store.active).unwrap(), vec![change]);
        event.created_ms = timestamp;
        if data == b"first" {
            store.commit(event, false).unwrap();
            continue;
        }
        let before = store.projection(&store.active).unwrap();
        store.commit(event, false).unwrap();
        let after = store.projection(&store.active).unwrap();
        let old = &before.entries[&file.id];
        let new = &after.entries[&file.id];
        assert!(old.content.is_none() && new.content.is_none());
        assert_eq!(old.modified_ms, new.modified_ms);
        assert_eq!(old.basic_info(), new.basic_info());
        assert_ne!(old.content_ref, new.content_ref);
        let expected = vec![("\\file".to_owned(), ACTION_MODIFIED)];
        assert_eq!(namespace_notifications(&before, &after), expected);
        assert_eq!(
            delta_notifications(
                &before,
                &store,
                &std::collections::BTreeSet::from([file.id.clone()]),
                false
            ),
            expected
        );
        assert!(namespace_notifications(&after, &after).is_empty());
    }
}
