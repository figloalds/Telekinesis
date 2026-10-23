use serde_json::json;
use std::{
    io::Read,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tkfs::{
    backend::{DirectoryBackend, ObjectBackend},
    content::LocalContent,
    core::{Store, id},
    model::{
        content::{CHUNK_SIZE, Digest, FileContentRef, ObjectKind},
        ipc::StopMode,
        orchestrator::{WorkerClient, WorkerEndpoint},
    },
    orchestrator::worker_client::SessionWorkerClient,
    runtime::{self, Engine},
    util::fault::{self, FaultAction, FaultConfig},
};
#[path = "support/export.rs"]
mod export;

static TESTS: Mutex<()> = Mutex::new(());
fn serial() -> std::sync::MutexGuard<'static, ()> {
    TESTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
fn data() -> Vec<u8> {
    (0..3 * CHUNK_SIZE as usize + 13)
        .map(|i| ((i / 65_536 * 31 + i) % 251) as u8)
        .collect()
}
fn write(store: &mut Store, path: &str, bytes: &[u8]) -> FileContentRef {
    let entry = store
        .lookup(path)
        .unwrap_or_else(|_| store.create(path, "file").unwrap());
    store
        .write_revision(&entry.id, entry.revisions.get("content").cloned(), bytes)
        .unwrap();
    store.lookup(path).unwrap().content_ref.unwrap()
}
fn no_temporary_index(root: &std::path::Path) {
    assert!(
        !root.join("objects").read_dir().unwrap().any(|p| p
            .unwrap()
            .path()
            .extension()
            .is_some_and(|e| e == "part"))
    );
}
fn error<T>(result: anyhow::Result<T>, code: &str) {
    let error = format!("{:#}", result.err().expect("unexpected success"));
    assert!(error.contains(code), "expected {code}: {error}");
}

#[test]
fn core_export_preserves_current_history_chunks_and_manifest_identity_without_private_bytes() {
    let _serial = serial();
    let state = tempfile::tempdir().unwrap();
    let mut store = Store::initialize(state.path(), "export", &id()).unwrap();
    let direct = write(&mut store, "direct", b"first shared revision");
    let bytes = data();
    let large = write(&mut store, "large", &bytes);
    let duplicate = write(&mut store, "duplicate", &bytes);
    assert_eq!(large, duplicate);
    write(&mut store, "direct", b"second shared revision");
    let private = store.fork("private", false).unwrap();
    store.checkout(&private.id).unwrap();
    let canary = b"PRIVATE EXPORT CANARY";
    write(&mut store, "large", canary);
    let bucket = tempfile::tempdir().unwrap();
    let backend = DirectoryBackend::new(bucket.path()).unwrap();
    let report = store.export_shared_objects(&backend).unwrap();
    let destination = tempfile::tempdir().unwrap();
    let (index, provider) = export::restore(&backend, &report, destination.path());
    assert_eq!(report["remote_validated"], false);
    assert_eq!(
        export::read(&provider, &direct, 0, direct.total_len as usize),
        b"first shared revision"
    );
    assert_eq!(export::read(&provider, &large, 0, bytes.len()), bytes);
    assert!(
        index["events"]
            .as_array()
            .unwrap()
            .iter()
            .all(|e| e["branch"]["shared"] == true)
    );
    assert!(!serde_json::to_string(&index).unwrap().contains(&private.id));
    assert!(
        !bucket
            .path()
            .join(Digest::bytes(canary).to_string())
            .exists()
    );
    let pages: Vec<_> = index["objects"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|o| o["key"]["kind"] == "manifest")
        .collect();
    assert!(!pages.is_empty());
    for page in pages {
        assert_ne!(page["key"]["digest"], page["object"]);
    }
    no_temporary_index(state.path());
    assert!(store.verify_derived_state().unwrap().is_empty());
}

#[test]
fn canonical_empty_export_needs_no_physical_empty_source_chunk() {
    let _serial = serial();
    let state = tempfile::tempdir().unwrap();
    let mut store = Store::initialize(state.path(), "empty-export", &id()).unwrap();
    let empty = store.create("empty", "file").unwrap().content_ref.unwrap();
    assert!(
        !state
            .path()
            .join("objects")
            .join(empty.digest.to_string())
            .exists()
    );
    let bucket = tempfile::tempdir().unwrap();
    let backend = DirectoryBackend::new(bucket.path()).unwrap();
    let report = store.export_shared_objects(&backend).unwrap();
    assert_eq!(report["objects"], 1);
    let target = tempfile::tempdir().unwrap();
    let (_, copy) = export::restore(&backend, &report, target.path());
    assert_eq!(export::read(&copy, &empty, 0, 0), b"");
    assert_eq!(
        backend.get_verified(&empty.digest.to_string()).unwrap(),
        b""
    );
}

struct Backend {
    inner: DirectoryBackend,
    streams: AtomicUsize,
    wrong_object: bool,
    fail_stream: bool,
    cancel: Option<Arc<AtomicBool>>,
    mutate: Option<Arc<Mutex<Store>>>,
    changed: AtomicBool,
}
impl Backend {
    fn new(path: &std::path::Path) -> Self {
        Self {
            inner: DirectoryBackend::new(path).unwrap(),
            streams: AtomicUsize::new(0),
            wrong_object: false,
            fail_stream: false,
            cancel: None,
            mutate: None,
            changed: AtomicBool::new(false),
        }
    }
}
impl ObjectBackend for Backend {
    fn get_verified(&self, object: &str) -> anyhow::Result<Vec<u8>> {
        self.inner.get_verified(object)
    }
    fn put_verified(&self, bytes: &[u8]) -> anyhow::Result<String> {
        assert!(
            bytes.len() <= CHUNK_SIZE as usize,
            "file bytes must remain chunk bounded"
        );
        if let Some(store) = &self.mutate {
            let mut store = store
                .try_lock()
                .expect("backend I/O held the metadata owner");
            assert!(
                store.db.is_autocommit(),
                "backend I/O held the capture transaction"
            );
            if !self.changed.swap(true, Ordering::AcqRel) {
                store.create("after-capture", "directory")?;
            }
        }
        if let Some(cancel) = &self.cancel {
            cancel.store(true, Ordering::Release);
        }
        if self.wrong_object {
            return Ok("wrong-backend-hash".into());
        }
        self.inner.put_verified(bytes)
    }
    fn put_stream(&self, reader: &mut dyn Read) -> anyhow::Result<String> {
        self.streams.fetch_add(1, Ordering::AcqRel);
        if self.fail_stream {
            anyhow::bail!("BACKEND_TEST_FAILURE");
        }
        self.inner.put_stream(reader)
    }
}

#[test]
fn capture_pins_exact_history_and_backend_work_runs_after_its_transaction() {
    let _serial = serial();
    let state = tempfile::tempdir().unwrap();
    let mut store = Store::initialize(state.path(), "snapshot-export", &id()).unwrap();
    let reference = write(&mut store, "file", &data());
    let provider = LocalContent::open(state.path()).unwrap();
    assert!(provider.pinned_roots().1.is_empty());
    let plan = store
        .prepare_shared_export(&export::context(), &|| false)
        .unwrap();
    assert!(!provider.pinned_roots().1.is_empty());
    let later = write(&mut store, "file", b"newer bytes");
    let store = Arc::new(Mutex::new(store));
    let bucket = tempfile::tempdir().unwrap();
    let mut backend = Backend::new(bucket.path());
    backend.mutate = Some(store.clone());
    let report = plan.write(&backend, &export::context(), &|| false).unwrap();
    assert_eq!(backend.streams.load(Ordering::Acquire), 1);
    let target = tempfile::tempdir().unwrap();
    let (index, copy) = export::restore(&backend, &report, target.path());
    assert!(
        !serde_json::to_string(&index)
            .unwrap()
            .contains("after-capture")
    );
    assert!(
        !serde_json::to_string(&index)
            .unwrap()
            .contains(&later.digest.to_string())
    );
    assert_eq!(export::read(&copy, &reference, 0, data().len()), data());
    drop(plan);
    assert!(provider.pinned_roots().1.is_empty());
    assert!(
        store
            .lock()
            .unwrap()
            .verify_derived_state()
            .unwrap()
            .is_empty()
    );
    no_temporary_index(state.path());
}

#[test]
fn missing_and_corrupt_chunks_or_pages_never_publish_an_export_index_and_repair_retries() {
    let _serial = serial();
    for page in [false, true] {
        for corrupt in [false, true] {
            let state = tempfile::tempdir().unwrap();
            let mut store = Store::initialize(state.path(), "damaged-export", &id()).unwrap();
            let bytes = data();
            let reference = write(&mut store, "file", &bytes);
            let safe = write(&mut store, "unaffected", b"safe");
            let provider = LocalContent::open(state.path()).unwrap();
            let key = if page {
                tkfs::model::content::ObjectKey::root(&reference)
            } else {
                tkfs::model::content::ObjectKey {
                    kind: ObjectKind::Bytes,
                    digest: Digest::bytes(&bytes[..CHUNK_SIZE as usize]),
                }
            };
            let path = state
                .path()
                .join(if page { "manifests-v1" } else { "objects" })
                .join(key.digest.to_string());
            let original = std::fs::read(&path).unwrap();
            if corrupt {
                std::fs::write(&path, b"corrupt").unwrap();
            } else {
                std::fs::remove_file(&path).unwrap();
            }
            let before = serde_json::to_value(store.events().unwrap()).unwrap();
            let bucket = tempfile::tempdir().unwrap();
            let backend = Backend::new(bucket.path());
            error(
                store.export_shared_objects(&backend),
                if corrupt {
                    "CORRUPT_CONTENT"
                } else {
                    "CONTENT_UNAVAILABLE"
                },
            );
            assert_eq!(backend.streams.load(Ordering::Acquire), 0);
            assert!(provider.pinned_roots().1.is_empty());
            assert_eq!(export::read(&provider, &safe, 0, 4), b"safe");
            no_temporary_index(state.path());
            assert_eq!(
                serde_json::to_value(store.events().unwrap()).unwrap(),
                before
            );
            provider
                .install(key, &original, &export::context())
                .unwrap();
            let report = store.export_shared_objects(&backend).unwrap();
            let destination = tempfile::tempdir().unwrap();
            let (_, copy) = export::restore(&backend, &report, destination.path());
            assert_eq!(export::read(&copy, &reference, 0, bytes.len()), bytes);
        }
    }
}

#[test]
fn cancelled_expired_or_failed_exports_release_pins_and_remove_temporary_indexes() {
    let _serial = serial();
    for mode in ["cancel", "deadline", "wrong-object", "failed-index"] {
        let state = tempfile::tempdir().unwrap();
        let mut store = Store::initialize(state.path(), "failed-export", &id()).unwrap();
        write(&mut store, "file", &data());
        let provider = LocalContent::open(state.path()).unwrap();
        let bucket = tempfile::tempdir().unwrap();
        let mut backend = Backend::new(bucket.path());
        let cancel = Arc::new(AtomicBool::new(false));
        backend.wrong_object = mode == "wrong-object";
        backend.fail_stream = mode == "failed-index";
        if mode == "cancel" {
            backend.cancel = Some(cancel.clone());
        }
        let plan = store
            .prepare_shared_export(&export::context(), &|| false)
            .unwrap();
        let mut context = export::context();
        if mode == "deadline" {
            context.deadline = Instant::now();
        }
        error(
            plan.write(&backend, &context, &|| cancel.load(Ordering::Acquire)),
            match mode {
                "cancel" => "CONTENT_CANCELLED",
                "deadline" => "CONTENT_DEADLINE",
                "wrong-object" => "BACKEND_HASH_MISMATCH",
                _ => "BACKEND_TEST_FAILURE",
            },
        );
        drop(plan);
        assert!(provider.pinned_roots().1.is_empty());
        no_temporary_index(state.path());
        assert!(store.verify_derived_state().unwrap().is_empty());
        store
            .export_shared_objects(&Backend::new(bucket.path()))
            .unwrap();
    }
}

struct Worker {
    ctx: Arc<runtime::ctx::WorkerCtx>,
    server: runtime::RpcServer,
    info: runtime::Discovery,
    state: tempfile::TempDir,
}
impl Worker {
    fn new() -> Self {
        let state = tempfile::tempdir().unwrap();
        let mut store = Store::initialize(state.path(), "worker-export", &id()).unwrap();
        write(&mut store, "file", &data());
        let ctx = tkfs::worker::build_ctx(
            Arc::new(Mutex::new(Engine::new(store))),
            state.path(),
            false,
            None,
        );
        let server = runtime::server::start_rpc_server(ctx.clone()).unwrap();
        let info = server.publish(None).unwrap();
        Self {
            ctx,
            server,
            info,
            state,
        }
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
}
impl Drop for Worker {
    fn drop(&mut self) {
        fault::configure(None);
        let result = self.server.stop(StopMode::Force);
        if !std::thread::panicking() {
            result.unwrap();
        }
    }
}

#[test]
fn json_and_session_exports_share_the_current_graph_and_keep_state_path_guards() {
    let _serial = serial();
    let worker = Worker::new();
    let buckets = tempfile::tempdir().unwrap();
    let report = runtime::RpcLane::default()
        .call(
            &worker.info,
            &id(),
            json!({"op":"bucket-export","directory":buckets.path().join("json")}),
        )
        .unwrap();
    let client = worker.client();
    let session = client
        .call(
            "bucket-export",
            json!({"directory":buckets.path().join("session")}),
            Duration::from_secs(10),
        )
        .unwrap();
    assert_eq!(session, report);
    let backend = DirectoryBackend::new(&buckets.path().join("session")).unwrap();
    let target = tempfile::tempdir().unwrap();
    let (_, copy) = export::restore(&backend, &session, target.path());
    let reference = runtime::lock_engine(&worker.ctx.engine)
        .store
        .lookup("file")
        .unwrap()
        .content_ref
        .unwrap();
    assert_eq!(export::read(&copy, &reference, 0, data().len()), data());
    error(
        runtime::RpcLane::default().call(
            &worker.info,
            &id(),
            json!({"op":"bucket-export","directory":worker.state.path().join("bucket")}),
        ),
        "BUCKET_MUST_BE_OUTSIDE_STATE",
    );
}

#[test]
fn a_slow_export_leaves_engine_and_metadata_writes_available_and_honors_session_deadline() {
    let _serial = serial();
    let worker = Worker::new();
    let bucket = tempfile::tempdir().unwrap();
    let client = worker.client();
    let mut config = FaultConfig::new("content.export.before_object", FaultAction::Hang);
    config.ms = Some(400);
    config.count = Some(1);
    fault::configure(Some(config));
    let path = bucket.path().to_owned();
    let export_client = client.clone();
    let exporting = std::thread::spawn(move || {
        export_client.call(
            "bucket-export",
            json!({"directory":path}),
            Duration::from_millis(80),
        )
    });
    let end = Instant::now() + Duration::from_secs(5);
    while fault::fired() == 0 {
        assert!(Instant::now() < end);
        std::thread::sleep(Duration::from_millis(1));
    }
    let began = Instant::now();
    {
        let mut engine =
            runtime::try_lock_engine(&worker.ctx.engine).expect("export byte work held Engine");
        engine.store.create("during-export", "directory").unwrap();
    }
    assert!(began.elapsed() < Duration::from_millis(200));
    assert!(
        client
            .call("branches", json!({}), Duration::from_secs(2))
            .is_ok()
    );
    assert!(exporting.join().unwrap().is_err());
    fault::configure(None);
    let end = Instant::now() + Duration::from_secs(3);
    while worker
        .state
        .path()
        .join("objects")
        .read_dir()
        .unwrap()
        .any(|p| p.unwrap().path().extension().is_some_and(|e| e == "part"))
    {
        assert!(
            Instant::now() < end,
            "expired export retained its temporary index"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let report = client
        .call(
            "bucket-export",
            json!({"directory":bucket.path()}),
            Duration::from_secs(10),
        )
        .unwrap();
    assert!(report["objects"].as_u64().unwrap() > 1);
}

#[test]
fn saturated_exports_refuse_another_export_without_blocking_metadata_and_release_capacity() {
    let _serial = serial();
    let worker = Worker::new();
    let buckets = tempfile::tempdir().unwrap();
    let client = worker.client();
    let mut config = FaultConfig::new("content.export.before_object", FaultAction::Hang);
    config.ms = Some(1_000);
    config.count = Some(2);
    fault::configure(Some(config));
    let jobs: Vec<_> = (0..2)
        .map(|n| {
            let client = client.clone();
            let path = buckets.path().join(n.to_string());
            std::thread::spawn(move || {
                client.call(
                    "bucket-export",
                    json!({"directory":path}),
                    Duration::from_secs(10),
                )
            })
        })
        .collect();
    let end = Instant::now() + Duration::from_secs(5);
    while fault::fired() < 2 {
        assert!(Instant::now() < end);
        std::thread::sleep(Duration::from_millis(1));
    }
    let began = Instant::now();
    let refused = client
        .call(
            "bucket-export",
            json!({"directory":buckets.path().join("third")}),
            Duration::from_secs(2),
        )
        .unwrap_err();
    assert!(
        format!("{refused:?}").contains("CONTENT_RESOURCE_LIMIT"),
        "{refused:?}"
    );
    assert!(began.elapsed() < Duration::from_millis(200));
    assert!(
        client
            .call("branches", json!({}), Duration::from_secs(2))
            .is_ok()
    );
    for job in jobs {
        job.join().unwrap().unwrap();
    }
    fault::configure(None);
    client
        .call(
            "bucket-export",
            json!({"directory":buckets.path().join("third")}),
            Duration::from_secs(10),
        )
        .unwrap();
    no_temporary_index(worker.state.path());
}
