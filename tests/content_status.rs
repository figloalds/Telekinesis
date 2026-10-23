use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tkfs::{
    content::{LocalContent, ReadContext},
    core::{Bundle, Store, admission::reference_id, id},
    model::content::{CHUNK_SIZE, ContentKind, Digest, FileContentRef, ObjectKey},
    runtime::{self, Engine},
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
fn store(root: &std::path::Path) -> Store {
    Store::initialize(root, "current-content-status", &id()).unwrap()
}
fn commit(store: &mut Store, path: &str, root: &FileContentRef) -> String {
    let entry = store.create(path, "file").unwrap();
    let event = store.event(
        &store.branch(&store.active).unwrap(),
        vec![
            store
                .current_change(&entry.id, "content", json!(root))
                .unwrap(),
        ],
    );
    let reference = reference_id(&event.id, &entry.id);
    store.commit(event, true).unwrap();
    reference
}
fn index(store: &mut Store, provider: &LocalContent) {
    while store.consume_content_admission("status-test", 256).unwrap() != 0 {}
    loop {
        let items = store.content_candidates(256, false).unwrap();
        if items.is_empty() {
            break;
        }
        for item in items {
            if item.edge.level.is_some() {
                store
                    .expand_content_edge(provider, &item, &context())
                    .unwrap();
            } else {
                store
                    .verify_content_edge(provider, &item, &context())
                    .unwrap();
            }
        }
    }
}
fn import(provider: &LocalContent, bytes: &[u8]) -> FileContentRef {
    provider
        .import(&mut &bytes[..], bytes.len() as u64, &context())
        .unwrap()
}
fn state(store: &Store) -> serde_json::Value {
    let before = store.object_hashes();
    let value = store.content_status().unwrap();
    assert_eq!(
        store.object_hashes(),
        before,
        "status must not hash file bytes"
    );
    value
}

#[test]
fn empty_and_direct_descriptors_are_counted_without_legacy_object_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = store(dir.path());
    assert_eq!(state(&store)["current_objects"], 0);
    store.create("empty-a", "file").unwrap();
    store.create("empty-b", "file").unwrap();
    // Current creates initialize the provider. Remove its empty directory to
    // exercise a missing manifest directory without inventing an old store.
    std::fs::remove_dir(dir.path().join("manifests-v1")).unwrap();
    let empty = state(&store);
    assert_eq!(empty["current_objects"], 1);
    assert_eq!(empty["current_available"], 1);
    assert_eq!(empty["complete"], true);
    assert!(!dir.path().join("manifests-v1").exists());
    let provider = LocalContent::open(dir.path()).unwrap();
    let root = import(&provider, b"small current bytes");
    commit(&mut store, "direct", &root);
    let pending = state(&store);
    assert_eq!(pending["current_objects"], 2);
    assert_eq!(pending["current_missing"], 1);
    assert_eq!(pending["items"][0]["state"], "indexing");
    assert_eq!(pending["items"][0]["descriptor"], json!(root));
    assert_eq!(pending["current_missing_bytes"], root.total_len);
    index(&mut store, &provider);
    let ready = state(&store);
    assert_eq!(ready["current_available"], 2);
    assert_eq!(ready["complete"], true);
    assert_eq!(ready["items"], json!([]));
    assert_eq!(
        store
            .db
            .query_row("SELECT count(*) FROM object_sizes", [], |r| r
                .get::<_, u64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn metadata_only_manifest_is_missing_with_reference_and_logical_size() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = store(dir.path());
    store.create("directory", "directory").unwrap();
    let root = FileContentRef::new(
        ContentKind::Manifest,
        Digest::bytes(b"not installed"),
        5_000_003,
    )
    .unwrap();
    let reference = commit(&mut store, "directory/remote", &root);
    let report = state(&store);
    assert_eq!(report["current_objects"], 1);
    assert_eq!(report["current_missing_bytes"], root.total_len);
    assert_eq!(report["items"][0]["reference"], reference);
    assert_eq!(report["items"][0]["path"], "directory/remote");
    assert_eq!(report["items"][0]["state"], "missing");
    assert_eq!(report["complete"], false);
}

#[test]
fn nested_manifest_counts_repeated_chunks_and_absent_pages_by_logical_coverage() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = store(dir.path());
    let provider = LocalContent::open(dir.path()).unwrap();
    let bytes = vec![23; 257 * CHUNK_SIZE as usize];
    let root = import(&provider, &bytes);
    let reference = commit(&mut store, "large", &root);
    index(&mut store, &provider);
    assert_eq!(state(&store)["complete"], true);
    let path = dir
        .path()
        .join("objects")
        .join(Digest::bytes(&bytes[..CHUNK_SIZE as usize]).to_string());
    std::fs::remove_file(&path).unwrap();
    let missing = state(&store);
    assert_eq!(missing["current_objects"], 1);
    assert_eq!(missing["current_missing_bytes"], bytes.len() as u64);
    assert_eq!(missing["items"][0]["state"], "missing");
    provider
        .install(
            ObjectKey::root(
                &FileContentRef::new(
                    ContentKind::Direct,
                    Digest::bytes(&bytes[..CHUNK_SIZE as usize]),
                    CHUNK_SIZE,
                )
                .unwrap(),
            ),
            &bytes[..CHUNK_SIZE as usize],
            &context(),
        )
        .unwrap();
    assert_eq!(state(&store)["complete"], true);
    let edges = store
        .content_range_candidates(
            &reference,
            tkfs::model::workspace::ByteRange {
                offset: 256 * CHUNK_SIZE,
                len: CHUNK_SIZE,
            },
        )
        .unwrap();
    let tail_page = edges
        .iter()
        .find(|edge| edge.edge.level == Some(0))
        .unwrap();
    std::fs::remove_file(
        dir.path()
            .join("manifests-v1")
            .join(tail_page.edge.key.digest.to_string()),
    )
    .unwrap();
    let missing = state(&store);
    assert_eq!(missing["current_missing_bytes"], CHUNK_SIZE);
    assert_eq!(missing["items"][0]["state"], "missing");
}

#[test]
fn derived_adjacency_and_observed_corruption_cannot_claim_completeness() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = store(dir.path());
    let provider = LocalContent::open(dir.path()).unwrap();
    let bytes = vec![73; 2 * CHUNK_SIZE as usize + 7];
    let root = import(&provider, &bytes);
    let reference = commit(&mut store, "file", &root);
    index(&mut store, &provider);
    assert_eq!(state(&store)["complete"], true);
    store
        .db
        .execute(
            "UPDATE content_manifest_edges SET len=len-1 WHERE parent=?1 AND ordinal=0",
            [root.digest.to_string()],
        )
        .unwrap();
    assert_eq!(state(&store)["items"][0]["state"], "indexing");
    store
        .db
        .execute(
            "UPDATE content_manifest_edges SET len=len+1 WHERE parent=?1 AND ordinal=0",
            [root.digest.to_string()],
        )
        .unwrap();
    assert_eq!(state(&store)["complete"], true);
    let key = ObjectKey::root(
        &FileContentRef::new(
            ContentKind::Direct,
            Digest::bytes(&bytes[..CHUNK_SIZE as usize]),
            CHUNK_SIZE,
        )
        .unwrap(),
    );
    let edge = store.content_edge(&reference, 1, key).unwrap();
    store
        .defer_content_edge(&edge, &anyhow::anyhow!("CORRUPT_CONTENT"), 1000)
        .unwrap();
    let report = state(&store);
    assert_eq!(report["items"][0]["state"], "corrupt");
    assert_eq!(report["current_missing_bytes"], 2 * CHUNK_SIZE);
    assert_eq!(report["complete"], false);
    store
        .verify_content_edge(&provider, &edge, &context())
        .unwrap();
    assert_eq!(state(&store)["complete"], true);
}

#[test]
fn received_private_dedup_is_readable_but_shared_complete_requires_receipts() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = store(&dir.path().join("source"));
    let ap = LocalContent::open(&source.root).unwrap();
    let root = import(&ap, b"same bytes private and shared");
    commit(&mut source, "remote", &root);
    let mut dest = store(&dir.path().join("dest"));
    let bp = LocalContent::open(&dest.root).unwrap();
    import(&bp, b"same bytes private and shared");
    dest.receive(
        Bundle {
            repo: source.repo.clone(),
            branches: vec![],
            objects: BTreeMap::new(),
            events: source.events().unwrap(),
        },
        &BTreeSet::from([source.device.clone()]),
    )
    .unwrap();
    index(&mut dest, &bp);
    let private = state(&dest);
    assert_eq!(private["current_available"], 1);
    assert_eq!(private["shared_available"], 1); // canonical empty historical root
    assert_eq!(private["shared_missing_bytes"], root.total_len);
    assert_eq!(private["complete"], false);
    // A real public receipt is independent of dedup verification.
    let reference = dest
        .db
        .query_row(
            "SELECT reference FROM content_root_metadata WHERE root=?1",
            [root.encode().unwrap().as_slice()],
            |r| r.get::<_, String>(0),
        )
        .unwrap();
    let (_, bytes) = {
        index(&mut source, &ap);
        source
            .read_content_frame(&ap, &reference, ObjectKey::root(&root), &context())
            .unwrap()
    };
    tkfs::content::receipt::receive(
        &mut dest,
        &bp,
        tkfs::content::receipt::ReceivedFrame {
            reference: &reference,
            generation: 1,
            key: ObjectKey::root(&root),
            bytes: &bytes,
            source: &source.device,
        },
        &context(),
    )
    .unwrap();
    assert_eq!(state(&dest)["complete"], true);
}

#[test]
fn public_manifest_receipt_does_not_promote_private_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = store(&dir.path().join("source"));
    let ap = LocalContent::open(&source.root).unwrap();
    let bytes = vec![61; 2 * CHUNK_SIZE as usize + 3];
    let root = import(&ap, &bytes);
    let reference = commit(&mut source, "file", &root);
    index(&mut source, &ap);
    let mut dest = store(&dir.path().join("dest"));
    let bp = LocalContent::open(&dest.root).unwrap();
    import(&bp, &bytes);
    dest.receive(
        Bundle {
            repo: source.repo.clone(),
            branches: vec![],
            objects: BTreeMap::new(),
            events: source.events().unwrap(),
        },
        &BTreeSet::from([source.device.clone()]),
    )
    .unwrap();
    index(&mut dest, &bp);
    assert_eq!(state(&dest)["current_available"], 1);
    let mut keys = vec![ObjectKey::root(&root)];
    keys.extend(bytes.chunks(CHUNK_SIZE as usize).map(|chunk| {
        ObjectKey::root(
            &FileContentRef::new(
                ContentKind::Direct,
                Digest::bytes(chunk),
                chunk.len() as u64,
            )
            .unwrap(),
        )
    }));
    keys.dedup();
    for (ordinal, key) in keys.into_iter().enumerate() {
        let (_, frame) = source
            .read_content_frame(&ap, &reference, key, &context())
            .unwrap();
        tkfs::content::receipt::receive(
            &mut dest,
            &bp,
            tkfs::content::receipt::ReceivedFrame {
                reference: &reference,
                generation: 1,
                key,
                bytes: &frame,
                source: &source.device,
            },
            &context(),
        )
        .unwrap();
        let report = state(&dest);
        if ordinal == 0 {
            assert_eq!(report["shared_missing_bytes"], bytes.len() as u64);
        } else if ordinal == 1 {
            assert_eq!(report["shared_missing_bytes"], 3);
        } else {
            assert_eq!(report["complete"], true);
        }
    }
}

#[test]
fn rollback_and_foreign_verification_invalidate_status_without_writing_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = store(dir.path());
    let provider = LocalContent::open(dir.path()).unwrap();
    let root = import(&provider, b"transactional content");
    let reference = commit(&mut store, "file", &root);
    while store.consume_content_admission("status-test", 256).unwrap() != 0 {}
    assert_eq!(state(&store)["items"][0]["state"], "indexing");
    store.db.execute_batch("SAVEPOINT test_status").unwrap();
    store
        .db
        .execute(
            "UPDATE content_object_jobs SET verified=1 WHERE digest=?1",
            [root.digest.to_string()],
        )
        .unwrap();
    assert_eq!(state(&store)["complete"], true);
    store
        .db
        .execute_batch("ROLLBACK TO test_status; RELEASE test_status; BEGIN")
        .unwrap();
    assert_eq!(state(&store)["items"][0]["state"], "indexing");
    store.db.execute_batch("ROLLBACK").unwrap();
    let mut secondary = Store::open_secondary(dir.path(), &store.repo, &store.device).unwrap();
    let edge = secondary
        .content_edge(&reference, 1, ObjectKey::root(&root))
        .unwrap();
    secondary
        .verify_content_edge(&provider, &edge, &context())
        .unwrap();
    let changes = store.db.total_changes();
    assert_eq!(state(&store)["complete"], true);
    assert_eq!(store.db.total_changes(), changes);
}

struct Running {
    shared: runtime::Shared,
    server: runtime::RpcServer,
    info: runtime::Discovery,
}
impl Running {
    fn start(root: &std::path::Path) -> Self {
        let mut store = store(root);
        let descriptor =
            FileContentRef::new(ContentKind::Manifest, Digest::bytes(b"remote"), 150_000).unwrap();
        commit(&mut store, "remote", &descriptor);
        let shared = Arc::new(Mutex::new(Engine::new(store)));
        let ctx = tkfs::worker::build_ctx(shared.clone(), root, false, None);
        let server = runtime::server::start_rpc_server(ctx).unwrap();
        let info = server.publish(None).unwrap();
        Self {
            shared,
            server,
            info,
        }
    }
    fn details(&self, generation: u64) -> anyhow::Result<serde_json::Value> {
        runtime::RpcLane::default().call(
            &self.info,
            &id(),
            json!({"op":"folder-details","generation":generation}),
        )
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        fault::configure(None);
        let result = self.server.stop(tkfs::model::ipc::StopMode::Force);
        if !std::thread::panicking() {
            result.expect("current status fixture failed to stop");
        }
    }
}

#[test]
fn current_worker_without_peers_verifies_installed_bytes_and_finishes_indexing() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = store(dir.path());
    let provider = LocalContent::open(dir.path()).unwrap();
    let bytes = vec![91; 2 * CHUNK_SIZE as usize + 5];
    let root = import(&provider, &bytes);
    commit(&mut store, "local", &root);
    assert_eq!(state(&store)["items"][0]["state"], "indexing");
    let shared = Arc::new(Mutex::new(Engine::new(store)));
    let ctx = tkfs::worker::build_ctx(shared.clone(), dir.path(), false, None);
    let server = runtime::server::start_rpc_server(ctx.clone()).unwrap();
    let info = server.publish(None).unwrap();
    let running = Running {
        shared,
        server,
        info,
    };
    tkfs::worker::maintenance::spawn(ctx.clone()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let report = ctx
            .sync_stores
            .checkout()
            .unwrap()
            .content_status()
            .unwrap();
        if report["complete"] == true {
            assert_eq!(report["current_available"], 1);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "local current data never completed indexing: {report}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    drop(running);
}

#[test]
fn folder_details_rpc_reports_typed_missing_content_and_checks_generation() {
    let dir = tempfile::tempdir().unwrap();
    let running = Running::start(dir.path());
    let generation = runtime::lock_engine(&running.shared).store.generation;
    let details = running.details(generation).unwrap();
    assert_eq!(details["status"]["content"]["current_objects"], 1);
    assert_eq!(
        details["missing_content"][0]["content"]["descriptor"]["kind"],
        "manifest"
    );
    assert_eq!(details["missing_content"][0]["content"]["state"], "missing");
    assert!(details["review_token"].is_string());
    assert!(format!("{:#}", running.details(generation + 1).unwrap_err()).contains("STALE_VIEW"));
}

#[test]
fn folder_details_releases_engine_during_snapshot_and_refuses_checkout_races() {
    let dir = tempfile::tempdir().unwrap();
    let running = Running::start(dir.path());
    let generation = runtime::lock_engine(&running.shared).store.generation;
    let mut config = FaultConfig::new("folder_details.snapshot", FaultAction::Hang);
    config.ms = Some(700);
    config.count = Some(1);
    fault::configure(Some(config));
    std::thread::scope(|scope| {
        let query = scope.spawn(|| running.details(generation));
        let deadline = Instant::now() + Duration::from_secs(5);
        while fault::fired() == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(fault::fired(), 1);
        {
            let mut engine = running
                .shared
                .try_lock()
                .expect("folder details held Engine during metadata work");
            engine.store.fork("other", false).unwrap();
            engine.store.checkout("other").unwrap();
        }
        assert!(format!("{:#}", query.join().unwrap().unwrap_err()).contains("STALE_VIEW"));
    });
    fault::configure(None);
}
