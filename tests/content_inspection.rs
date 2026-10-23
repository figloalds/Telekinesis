use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tkfs::{
    content::{
        LocalContent, ReadContext,
        receipt::{self, ReceivedFrame},
    },
    core::{Store, admission::reference_id, id},
    model::{
        content::{CHUNK_SIZE, Digest, FileContentRef, ObjectKey, ObjectKind},
        ipc::StopMode,
        orchestrator::{WorkerClient, WorkerEndpoint},
        workspace::AdmissionAction,
    },
    orchestrator::worker_client::SessionWorkerClient,
    runtime::{self, Engine, content_inspection, ctx::WorkerCtx},
    util::{
        Cancel,
        fault::{self, FaultAction, FaultConfig},
    },
};

// Fault injection and global prefetch budgets are process-wide. Keep cases
// independent under the default parallel test runner; each case still exercises
// concurrent operations and, where required, multiple workers.
static TESTS: Mutex<()> = Mutex::new(());
fn serial() -> std::sync::MutexGuard<'static, ()> {
    TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn context() -> ReadContext {
    ReadContext {
        deadline: Instant::now() + Duration::from_secs(30),
        cancel: Cancel::new(),
    }
}
fn data() -> Vec<u8> {
    let mut bytes = vec![b'x'; 2 * CHUNK_SIZE as usize + 3];
    bytes[CHUNK_SIZE as usize..2 * CHUNK_SIZE as usize].fill(b'y');
    bytes[2 * CHUNK_SIZE as usize..].fill(b'z');
    bytes
}
fn error<T>(result: anyhow::Result<T>, code: &str) {
    let error = format!(
        "{:#}",
        result.err().expect("operation unexpectedly succeeded")
    );
    assert!(error.contains(code), "expected {code}, got {error}");
}
fn until(mut predicate: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(5);
    while !predicate() {
        assert!(Instant::now() < end, "condition did not become true");
        std::thread::sleep(Duration::from_millis(2));
    }
}
struct File {
    reference: String,
    descriptor: FileContentRef,
    entity: String,
    revision: String,
}
struct Fixture {
    ctx: Arc<WorkerCtx>,
    provider: Arc<LocalContent>,
    server: runtime::RpcServer,
    info: runtime::Discovery,
    stopped: AtomicBool,
    _dir: tempfile::TempDir,
}
impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::initialize(dir.path(), "content-inspection", &id()).unwrap();
        let shared = Arc::new(Mutex::new(Engine::new(store)));
        let ctx = tkfs::worker::build_ctx(shared, dir.path(), false, None);
        let server = runtime::server::start_rpc_server(ctx.clone()).unwrap();
        let info = server.publish(None).unwrap();
        let provider = runtime::content_rpc::state(&ctx).unwrap().provider.clone();
        Self {
            ctx,
            provider,
            server,
            info,
            stopped: AtomicBool::new(false),
            _dir: dir,
        }
    }
    fn file(&self, path: &str, bytes: &[u8]) -> File {
        let mut store = self.ctx.sync_stores.checkout().unwrap();
        let entry = store.create(path, "file").unwrap();
        let revision = store
            .write_revision(&entry.id, entry.revisions.get("content").cloned(), bytes)
            .unwrap();
        let entry = store.entry(&entry.id).unwrap();
        self.index(&mut store);
        File {
            reference: reference_id(&revision, &entry.id),
            descriptor: entry.content_ref.unwrap(),
            entity: entry.id,
            revision,
        }
    }
    fn index(&self, store: &mut Store) {
        while store
            .consume_content_admission("inspection-test", 256)
            .unwrap()
            != 0
        {}
        loop {
            let items = store.content_candidates(256, false).unwrap();
            if items.is_empty() {
                break;
            }
            for item in items {
                if item.edge.level.is_some() {
                    store
                        .expand_content_edge(&self.provider, &item, &context())
                        .unwrap();
                } else {
                    store
                        .verify_content_edge(&self.provider, &item, &context())
                        .unwrap();
                }
            }
        }
    }
    fn rpc(&self, payload: Value) -> anyhow::Result<Value> {
        runtime::RpcLane::default().call(&self.info, &id(), payload)
    }
    fn call(
        &self,
        op: &str,
        payload: Value,
        timeout: Duration,
        cancelled: &dyn Fn() -> bool,
    ) -> anyhow::Result<Value> {
        content_inspection::dispatch(&self.ctx, op, &payload, Instant::now() + timeout, cancelled)
    }
    fn remove(&self, bytes: &[u8]) {
        std::fs::remove_file(
            self.ctx
                .root
                .join("objects")
                .join(Digest::bytes(bytes).to_string()),
        )
        .unwrap();
    }
    fn feed(&self, file: &File, bytes: &[u8]) {
        let mut store = self.ctx.sync_stores.checkout().unwrap();
        let generation = store.content_root(&file.reference).unwrap().0;
        receipt::receive(
            &mut store,
            &self.provider,
            ReceivedFrame {
                reference: &file.reference,
                generation,
                key: ObjectKey {
                    kind: ObjectKind::Bytes,
                    digest: Digest::bytes(bytes),
                },
                bytes,
                source: "fixture-source",
            },
            &context(),
        )
        .unwrap();
    }
    fn waiters(&self) -> Vec<tkfs::content::scheduler::DemandSnapshot> {
        runtime::content_rpc::state(&self.ctx)
            .unwrap()
            .scheduler
            .snapshots(self.provider.identity(), &self.ctx.repo)
    }
    fn client(&self) -> Arc<SessionWorkerClient> {
        SessionWorkerClient::connect(&WorkerEndpoint {
            instance: self.ctx.instance.clone(),
            pid: std::process::id(),
            address: self.info.address.clone(),
            version: self.ctx.version.clone(),
            token: Some(self.server.token().into()),
        })
        .unwrap()
    }
    fn stop(&self) {
        self.server.stop(StopMode::Force).unwrap();
        self.stopped.store(true, Ordering::Release);
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fault::configure(None);
        if !self.stopped.load(Ordering::Acquire) {
            let result = self.server.stop(StopMode::Force);
            if !std::thread::panicking() {
                result.expect("inspection fixture failed to stop");
            }
        }
    }
}

#[test]
fn rpc_preview_uses_current_empty_direct_and_only_the_first_manifest_range() {
    let _serial = serial();
    let f = Fixture::new();
    for (name, bytes, text) in [
        ("empty", &b""[..], ""),
        ("direct", &b"hello"[..], "hello"),
        (
            "binary",
            &b"a\0b"[..],
            "Binary content; text preview unavailable",
        ),
    ] {
        let file = f.file(name, bytes);
        let preview = f
            .rpc(json!({"op":"preview-content","reference":file.reference}))
            .unwrap();
        assert_eq!(preview["descriptor"], json!(file.descriptor));
        assert_eq!(preview["size"], bytes.len() as u64);
        assert_eq!(preview["text"], text);
        assert_eq!(preview["truncated"], false);
    }
    let bytes = data();
    let file = f.file("manifest", &bytes);
    f.remove(&bytes[CHUNK_SIZE as usize..2 * CHUNK_SIZE as usize]);
    let preview = f
        .rpc(json!({"op":"preview-content","reference":file.reference}))
        .unwrap();
    assert_eq!(
        preview["text"].as_str().unwrap().as_bytes(),
        &bytes[..CHUNK_SIZE as usize]
    );
    assert_eq!(preview["size"], bytes.len() as u64);
    assert_eq!(preview["truncated"], true);
    assert!(f.waiters().is_empty());
    error(
        f.rpc(json!({"op":"preview-object","object":file.descriptor.digest})),
        "UNKNOWN_OPERATION",
    );
    error(
        f.rpc(json!({"op":"request-object","object":file.descriptor.digest})),
        "UNKNOWN_OPERATION",
    );
    error(
        f.rpc(json!({"op":"preview-content","reference":file.descriptor.digest})),
        "UNAUTHORIZED_OBJECT",
    );
}

#[test]
fn historical_conflict_previews_use_each_alternatives_registered_reference() {
    let _serial = serial();
    let f = Fixture::new();
    let file = f.file("file", b"base");
    let mut store = f.ctx.sync_stores.checkout().unwrap();
    for text in [b"left".as_slice(), b"right".as_slice()] {
        let root = f
            .provider
            .import(&mut &text[..], text.len() as u64, &context())
            .unwrap();
        let event = store.event(
            &store.branch(&store.active).unwrap(),
            vec![tkfs::core::Change {
                entity: file.entity.clone(),
                field: "content".into(),
                parents: vec![file.revision.clone()],
                value: json!(root),
            }],
        );
        store.commit(event, true).unwrap();
    }
    let details = f.rpc(json!({"op":"folder-details"})).unwrap();
    let conflict = details["conflicts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["field"] == "content")
        .unwrap();
    let mut previews = std::collections::BTreeSet::new();
    for version in conflict["alternatives"].as_array().unwrap() {
        let reference = reference_id(
            version["revision"].as_str().unwrap(),
            conflict["entity"].as_str().unwrap(),
        );
        let preview = f
            .rpc(json!({"op":"preview-content","reference":reference}))
            .unwrap();
        assert_eq!(preview["descriptor"], version["value"]);
        previews.insert(preview["text"].as_str().unwrap().to_owned());
    }
    assert_eq!(
        previews,
        std::collections::BTreeSet::from(["left".into(), "right".into()])
    );
    assert_eq!(
        f.rpc(json!({"op":"preview-content","reference":file.reference}))
            .unwrap()["text"],
        "base"
    );
}

#[test]
fn missing_preview_registers_exact_range_and_receipt_delivery_leaves_engine_available() {
    let _serial = serial();
    let f = Fixture::new();
    let bytes = data();
    let file = f.file("file", &bytes);
    f.remove(&bytes[..CHUNK_SIZE as usize]);
    f.remove(&bytes[CHUNK_SIZE as usize..2 * CHUNK_SIZE as usize]);
    let baseline = f.provider.pinned_roots().1;
    std::thread::scope(|scope| {
        let read =
            scope.spawn(|| f.rpc(json!({"op":"preview-content","reference":file.reference})));
        until(|| f.waiters().len() == 1);
        let pending = f.waiters().pop().unwrap().demand;
        assert_eq!(pending.reference, file.reference);
        assert_eq!(pending.revision.entity, file.entity);
        assert_eq!(
            pending.range,
            tkfs::model::workspace::ByteRange {
                offset: 0,
                len: CHUNK_SIZE
            }
        );
        until(|| match f.ctx.engine.try_lock() {
            Ok(mut engine) => {
                engine.store.create("during-preview", "directory").unwrap();
                true
            }
            Err(_) => false,
        });
        f.feed(&file, &bytes[..CHUNK_SIZE as usize]);
        assert_eq!(
            read.join().unwrap().unwrap()["text"]
                .as_str()
                .unwrap()
                .as_bytes(),
            &bytes[..CHUNK_SIZE as usize]
        );
    });
    assert!(f.waiters().is_empty());
    assert_eq!(f.provider.pinned_roots().1, baseline);
    assert!(
        !f.ctx
            .root
            .join("objects")
            .join(Digest::bytes(&bytes[CHUNK_SIZE as usize..2 * CHUNK_SIZE as usize]).to_string())
            .exists()
    );
}

#[test]
fn cancellation_and_withdrawal_release_independent_preview_waiters() {
    let _serial = serial();
    let f = Fixture::new();
    let file = f.file("file", b"missing");
    f.remove(b"missing");
    let cancelled = AtomicBool::new(false);
    let baseline = f.provider.pinned_roots().1;
    std::thread::scope(|scope| {
        let one = scope.spawn(|| {
            f.call(
                "preview-content",
                json!({"reference":file.reference}),
                Duration::from_secs(4),
                &|| cancelled.load(Ordering::Acquire),
            )
        });
        let two = scope.spawn(|| {
            f.call(
                "preview-content",
                json!({"reference":file.reference}),
                Duration::from_secs(4),
                &|| false,
            )
        });
        until(|| f.waiters().len() == 2);
        cancelled.store(true, Ordering::Release);
        error(one.join().unwrap(), "CONTENT_CANCELLED");
        assert_eq!(f.waiters().len(), 1);
        f.ctx
            .sync_stores
            .checkout()
            .unwrap()
            .set_content_admission(&file.reference, 1, AdmissionAction::Withdraw)
            .unwrap();
        error(two.join().unwrap(), "STALE_ADMISSION_GENERATION");
    });
    assert!(f.waiters().is_empty());
    assert_eq!(f.provider.pinned_roots().1, baseline);
}

#[test]
fn session_preview_uses_reply_body_and_honors_its_shorter_deadline() {
    let _serial = serial();
    let f = Fixture::new();
    let bytes = data();
    let file = f.file("file", &bytes);
    let client = f.client();
    let preview = client
        .call(
            "preview-content",
            json!({"reference":file.reference}),
            Duration::from_secs(3),
        )
        .unwrap();
    assert_eq!(
        preview["text"].as_str().unwrap().as_bytes(),
        &bytes[..CHUNK_SIZE as usize]
    );
    f.remove(&bytes[..CHUNK_SIZE as usize]);
    assert!(
        client
            .call(
                "preview-content",
                json!({"reference":file.reference}),
                Duration::from_millis(150)
            )
            .is_err()
    );
    until(|| f.waiters().is_empty());
    assert!(
        client.is_open(),
        "a preview deadline closed the shared session"
    );
    assert!(
        client
            .call("status", json!({}), Duration::from_secs(3))
            .is_ok()
    );
    client.close();
}

#[test]
fn explicit_range_requests_join_cancel_and_release_after_verified_completion() {
    let _serial = serial();
    let f = Fixture::new();
    let bytes = data();
    let file = f.file("file", &bytes);
    f.remove(&bytes[..CHUNK_SIZE as usize]);
    f.remove(&bytes[CHUNK_SIZE as usize..2 * CHUNK_SIZE as usize]);
    let baseline = f.provider.pinned_roots().1;
    let request = json!({"op":"request-content","reference":file.reference,"offset":CHUNK_SIZE,"len":CHUNK_SIZE});
    let one = f.rpc(request.clone()).unwrap();
    let joined = f.rpc(request).unwrap();
    assert_eq!(one["queued"], true);
    assert_eq!(one["token"], joined["token"]);
    assert_eq!(f.waiters().len(), 1);
    let two = f
        .rpc(json!({"op":"request-content","reference":file.reference,"offset":0,"len":16}))
        .unwrap();
    assert_eq!(f.waiters().len(), 2);
    assert_eq!(
        f.rpc(json!({"op":"cancel-content-request","token":one["token"]}))
            .unwrap()["cancelled"],
        true
    );
    assert_eq!(
        f.rpc(json!({"op":"cancel-content-request","token":one["token"]}))
            .unwrap()["cancelled"],
        false
    );
    assert_eq!(f.waiters().len(), 1);
    f.feed(&file, &bytes[..CHUNK_SIZE as usize]);
    runtime::content_rpc::reap(&f.ctx);
    assert!(f.waiters().is_empty());
    assert_eq!(f.provider.pinned_roots().1, baseline);
    assert_eq!(
        f.rpc(json!({"op":"cancel-content-request","token":two["token"]}))
            .unwrap()["cancelled"],
        false
    );
    let available = f
        .rpc(json!({"op":"request-content","reference":file.reference,"offset":0,"len":16}))
        .unwrap();
    assert_eq!(available["queued"], false);
    assert_eq!(available["available"], true);
    assert_eq!(
        f.ctx
            .sync_stores
            .checkout()
            .unwrap()
            .db
            .query_row("SELECT count(*) FROM object_requests", [], |r| r
                .get::<_, u64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn simultaneous_session_previews_leave_handlers_for_status_and_completion() {
    let _serial = serial();
    let f = Fixture::new();
    let file = f.file("file", b"missing");
    f.remove(b"missing");
    let client = f.client();
    std::thread::scope(|scope| {
        let one = scope.spawn(|| {
            client.call(
                "preview-content",
                json!({"reference":file.reference}),
                Duration::from_secs(4),
            )
        });
        let two = scope.spawn(|| {
            client.call(
                "preview-content",
                json!({"reference":file.reference}),
                Duration::from_secs(4),
            )
        });
        until(|| f.waiters().len() == 2);
        assert!(
            client
                .call("status", json!({}), Duration::from_secs(2))
                .is_ok()
        );
        error(
            f.rpc(json!({"op":"preview-content","reference":file.reference})),
            "CONTENT_RESOURCE_LIMIT",
        );
        f.ctx
            .sync_stores
            .checkout()
            .unwrap()
            .set_content_admission(&file.reference, 1, AdmissionAction::Withdraw)
            .unwrap();
        assert!(one.join().unwrap().is_err());
        assert!(two.join().unwrap().is_err());
    });
    assert!(f.waiters().is_empty());
    assert_eq!(
        tkfs::util::lock::lock(&f.ctx.inflight_objects).inspection_calls,
        0
    );
    assert!(client.is_open());
    client.close();
}

#[test]
fn explicit_requests_are_bounded_and_shutdown_releases_their_pins_and_waiters() {
    let _serial = serial();
    let f = Fixture::new();
    let mut bytes = data();
    bytes.resize(2 * 1024 * 1024, b'q');
    let file = f.file("file", &bytes);
    f.remove(&bytes[..CHUNK_SIZE as usize]);
    let baseline = f.provider.pinned_roots().1;
    for offset in 0..16 {
        assert_eq!(
            f.rpc(
                json!({"op":"request-content","reference":file.reference,"offset":offset,"len":1})
            )
            .unwrap()["queued"],
            true
        );
    }
    error(
        f.rpc(json!({"op":"request-content","reference":file.reference,"offset":17,"len":1})),
        "CONTENT_RESOURCE_LIMIT",
    );
    assert_eq!(f.waiters().len(), 16);
    // Explicit prefetches do not consume all filesystem/preview waiter slots.
    error(
        f.call(
            "preview-content",
            json!({"reference":file.reference}),
            Duration::from_millis(30),
            &|| false,
        ),
        "CONTENT_DEADLINE",
    );
    error(
        f.rpc(json!({"op":"request-content","reference":file.reference,"len":1024*1024+1})),
        "CONTENT_BUFFER_LIMIT",
    );
    let state = runtime::content_rpc::state(&f.ctx).unwrap();
    f.stop();
    assert_eq!(state.scheduler.usage().2, 0);
    assert_eq!(
        runtime::content_rpc::state(&f.ctx)
            .err()
            .map(|e| format!("{e:#}"))
            .unwrap(),
        "CONTENT_CANCELLED"
    );
    assert_eq!(f.provider.pinned_roots().1, baseline);
}

#[test]
fn queue_reaping_fences_withdrawal_and_checkout_without_affecting_new_owners() {
    let _serial = serial();
    let f = Fixture::new();
    let file = f.file("file", b"missing");
    f.remove(b"missing");
    let one = f
        .rpc(json!({"op":"request-content","reference":file.reference}))
        .unwrap();
    let mut store = f.ctx.sync_stores.checkout().unwrap();
    store
        .set_content_admission(&file.reference, 1, AdmissionAction::Withdraw)
        .unwrap();
    runtime::content_rpc::reap(&f.ctx);
    assert!(f.waiters().is_empty());
    store
        .set_content_admission(&file.reference, 2, AdmissionAction::Grant)
        .unwrap();
    let two = f
        .rpc(json!({"op":"request-content","reference":file.reference}))
        .unwrap();
    assert_ne!(one["token"], two["token"]);
    assert_eq!(
        f.rpc(json!({"op":"cancel-content-request","token":one["token"]}))
            .unwrap()["cancelled"],
        false
    );
    assert_eq!(f.waiters().len(), 1);
    {
        let mut engine = runtime::lock_engine(&f.ctx.engine);
        engine.store.fork("other", false).unwrap();
        engine.store.checkout("other").unwrap();
    }
    runtime::content_rpc::reap(&f.ctx);
    assert!(f.waiters().is_empty());
    assert_eq!(
        f.rpc(json!({"op":"cancel-content-request","token":two["token"]}))
            .unwrap()["cancelled"],
        false
    );
}

#[test]
fn global_prefetch_budget_leaves_other_workers_foreground_reads_available() {
    let _serial = serial();
    let one = Fixture::new();
    let bytes = data();
    let file = one.file("file", &bytes);
    one.remove(&bytes[..CHUNK_SIZE as usize]);
    for offset in 0..16 {
        one.rpc(json!({"op":"request-content","reference":file.reference,"offset":offset,"len":1}))
            .unwrap();
    }
    let two = Fixture::new();
    let other = two.file("other", b"other bytes");
    two.remove(b"other bytes");
    error(
        two.rpc(json!({"op":"request-content","reference":other.reference})),
        "CONTENT_RESOURCE_LIMIT",
    );
    assert_eq!(one.waiters().len(), 16);
    assert!(two.waiters().is_empty());
    error(
        two.call(
            "preview-content",
            json!({"reference":other.reference}),
            Duration::from_millis(40),
            &|| false,
        ),
        "CONTENT_DEADLINE",
    );
    assert!(two.waiters().is_empty());
    one.stop();
    let queued = two
        .rpc(json!({"op":"request-content","reference":other.reference}))
        .unwrap();
    assert_eq!(queued["queued"], true);
    assert_eq!(
        two.rpc(json!({"op":"cancel-content-request","token":queued["token"]}))
            .unwrap()["cancelled"],
        true
    );
}

#[test]
fn available_preview_rechecks_checkout_after_off_lock_byte_work() {
    let _serial = serial();
    let f = Fixture::new();
    let file = f.file("file", b"old view");
    let mut config = FaultConfig::new("content.inspection.before_read", FaultAction::Hang);
    config.ms = Some(400);
    config.count = Some(1);
    fault::configure(Some(config));
    std::thread::scope(|scope| {
        let read =
            scope.spawn(|| f.rpc(json!({"op":"preview-content","reference":file.reference})));
        until(|| fault::fired() == 1);
        {
            let mut engine = f.ctx.engine.try_lock().expect("byte work held Engine");
            engine.store.fork("other", false).unwrap();
            engine.store.checkout("other").unwrap();
        }
        error(read.join().unwrap(), "STALE_VIEW");
    });
    fault::configure(None);
    assert!(f.waiters().is_empty());
}

#[test]
fn unavailable_private_content_and_sql_contention_do_not_leave_requests_or_pins() {
    let _serial = serial();
    let f = Fixture::new();
    let file = f.file("file", b"missing");
    f.remove(b"missing");
    let store = f.ctx.sync_stores.checkout().unwrap();
    let baseline = f.provider.pinned_roots().1;
    store.db.execute_batch("BEGIN IMMEDIATE").unwrap();
    error(
        f.call(
            "preview-content",
            json!({"reference":file.reference}),
            Duration::from_millis(100),
            &|| false,
        ),
        "CONTENT_DEADLINE",
    );
    store.db.execute_batch("ROLLBACK").unwrap();
    assert!(f.waiters().is_empty());
    assert_eq!(f.provider.pinned_roots().1, baseline);
    {
        let mut engine = runtime::lock_engine(&f.ctx.engine);
        engine.store.fork("private", false).unwrap();
        engine.store.checkout("private").unwrap();
    }
    let private = f.file("private-file", b"private");
    f.remove(b"private");
    error(
        f.call(
            "preview-content",
            json!({"reference":private.reference}),
            Duration::from_secs(1),
            &|| false,
        ),
        "CONTENT_UNAVAILABLE",
    );
    error(
        f.rpc(json!({"op":"request-content","reference":private.reference})),
        "CONTENT_UNAVAILABLE",
    );
    assert!(f.waiters().is_empty());
    assert_eq!(f.provider.pinned_roots().1, baseline);
}
