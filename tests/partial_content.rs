use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    sync::{Arc, Mutex},
    time::Duration,
};
use tkfs::{
    core::*,
    paired_sync::{self, SyncPage, SyncRoute},
    runtime::{self, Engine},
};

fn engine(root: &std::path::Path) -> Engine {
    Engine::new(Store::open(root, Some("partial-mesh")).unwrap())
}
fn write(e: &mut Engine, path: &str, bytes: &[u8]) -> String {
    let f = e.store.create(path, "file").unwrap();
    e.store
        .write_revision(&f.id, f.revisions.get("content").cloned(), bytes)
        .unwrap();
    hash(bytes)
}
fn exchange(a: &mut Engine, b: &mut Engine, metadata: bool) -> Value {
    let ab = SyncRoute {
        peer: b.store.device.clone(),
        repo: a.store.repo.clone(),
        local_replica: a.store.device.clone(),
        remote_replica: b.store.device.clone(),
    };
    let ba = SyncRoute {
        peer: a.store.device.clone(),
        repo: a.store.repo.clone(),
        local_replica: b.store.device.clone(),
        remote_replica: a.store.device.clone(),
    };
    let page: SyncPage = serde_json::from_value(paired_sync::worker_control(a, &json!({"op":"published-offer","route":ab,"metadata_only":metadata,"max_frame":1024*1024})).unwrap()).unwrap();
    let sent: BTreeSet<_> = page.bundle.events.iter().map(|e| e.id.clone()).collect();
    let reply = paired_sync::worker_control(b, &json!({"op":"published-exchange","route":ba,"page":page,"metadata_only":metadata,"max_frame":1024*1024})).unwrap();
    paired_sync::worker_control(a, &json!({"op":"published-apply","route":ab,"page":reply,"request":page.request,"sent":sent,"metadata_only":metadata,"max_frame":1024*1024})).unwrap()
}

#[test]
fn metadata_mount_state_and_restart_do_not_require_content() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = engine(&dir.path().join("a"));
    let mut b = engine(&dir.path().join("b"));
    let object = write(&mut a, "remote.bin", &vec![7; 1024 * 1024]);
    exchange(&mut a, &mut b, true);
    exchange(&mut a, &mut b, true);
    assert_eq!(b.store.object_size(&object).unwrap(), 1024 * 1024);
    assert!(!b.store.object_available(&object));
    assert!(b.store.lookup("remote.bin").is_ok());
    assert_eq!(b.store.content_status().unwrap()["current_missing"], 1);
    let f = b.store.lookup("remote.bin").unwrap();
    b.store.rename(&f.id, "renamed.bin", false).unwrap();
    let branch = b.store.fork("offline", false).unwrap();
    b.store.checkout(&branch.id).unwrap();
    drop(b);
    let mut b = engine(&dir.path().join("b"));
    let (h, _) = b.open("renamed.bin", None, true).unwrap();
    assert_eq!(b.session(h).unwrap().bytes.len(), 1024 * 1024);
    b.truncate(h, 0, false).unwrap(); // explicit discard needs no base download
    b.write(h, 0, b"replacement", false, false).unwrap();
    b.close(h).unwrap();
    assert_eq!(
        b.store
            .read_object(&b.store.lookup("renamed.bin").unwrap().content.unwrap())
            .unwrap(),
        b"replacement"
    );
}

#[test]
fn content_queue_survives_metadata_ack_and_progresses_in_bounded_pages() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = engine(&dir.path().join("a"));
    let mut b = engine(&dir.path().join("b"));
    let bytes = vec![23; 2 * 1024 * 1024];
    let object = write(&mut a, "large", &bytes);
    exchange(&mut a, &mut b, true);
    exchange(&mut a, &mut b, true);
    assert_eq!(
        a.store.shared_event_inventory().unwrap(),
        b.store.shared_event_inventory().unwrap()
    );
    assert_eq!(a.store.outgoing_unacknowledged().unwrap(), 0);
    b.store.request_object(&object).unwrap();
    for _ in 0..24 {
        exchange(&mut a, &mut b, false);
        if b.store.content_status().unwrap()["complete"] == true {
            break;
        }
    }
    assert_eq!(b.store.read_object(&object).unwrap(), bytes);
    assert_eq!(b.store.content_status().unwrap()["complete"], true);
    assert!(b.store.partial_objects().unwrap().is_empty());
    assert!(b.store.wanted_objects().unwrap().is_empty());
}

#[test]
fn missing_content_relays_metadata_across_three_origins_then_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = engine(&dir.path().join("a"));
    let mut b = engine(&dir.path().join("b"));
    let mut c = engine(&dir.path().join("c"));
    let bytes = vec![45; 512 * 1024];
    let object = write(&mut a, "mesh", &bytes);
    fs::remove_file(a.store.root.join("objects").join(&object)).unwrap();
    exchange(&mut a, &mut b, true);
    exchange(&mut b, &mut c, true);
    assert!(c.store.lookup("mesh").is_ok());
    c.store.request_object(&object).unwrap();
    exchange(&mut c, &mut b, true);
    exchange(&mut b, &mut a, true);
    assert!(b.store.wanted_objects().unwrap().contains(&object));
    assert!(a.store.wanted_objects().unwrap().contains(&object));
    a.store.put_object(&bytes).unwrap();
    for _ in 0..8 {
        exchange(&mut a, &mut b, false);
        exchange(&mut b, &mut c, false);
    }
    assert_eq!(c.store.read_object(&object).unwrap(), bytes);
}

#[test]
fn read_wait_releases_engine_and_missing_content_has_a_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = engine(&dir.path().join("a"));
    let mut b = engine(&dir.path().join("b"));
    let object = write(&mut a, "demand", b"downloaded");
    exchange(&mut a, &mut b, true);
    let (h, _) = b.open("demand", None, false).unwrap();
    let shared = Arc::new(Mutex::new(b));
    let reader = shared.clone();
    let waiting =
        std::thread::spawn(move || runtime::prepare_content(&reader, h, Duration::from_secs(2)));
    for _ in 0..100 {
        if shared
            .lock()
            .unwrap()
            .store
            .wanted_objects()
            .unwrap()
            .contains(&object)
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    {
        let mut b = shared.lock().unwrap();
        b.store.create("unrelated", "directory").unwrap();
        b.store
            .receive_parts(
                BTreeMap::from([(
                    object.clone(),
                    ObjectPart {
                        offset: 0,
                        total: 10,
                        hex: hex::encode(b"downloaded"),
                    },
                )]),
                &[],
                &BTreeSet::new(),
            )
            .unwrap();
    }
    waiting.join().unwrap().unwrap();
    assert_eq!(
        shared.lock().unwrap().session(h).unwrap().bytes,
        b"downloaded"
    );
    let mut b = shared.lock().unwrap();
    b.close(h).unwrap();
    fs::remove_file(b.store.root.join("objects").join(&object)).unwrap();
    let (h, _) = b.open("demand", None, false).unwrap();
    drop(b);
    assert!(
        runtime::prepare_content(&shared, h, Duration::from_millis(25))
            .unwrap_err()
            .to_string()
            .contains("CONTENT_UNAVAILABLE")
    );
}

#[test]
fn corrupt_object_does_not_destroy_metadata_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = engine(dir.path());
    let object = write(&mut a, "retained", b"original");
    fs::write(a.store.root.join("objects").join(&object), b"corrupt!").unwrap();
    drop(a);
    let a = engine(dir.path());
    assert_eq!(
        a.store.lookup("retained").unwrap().content.as_deref(),
        Some(object.as_str())
    );
    assert!(!a.store.shared_object_inventory().unwrap().contains(&object));
    assert_eq!(a.store.content_status().unwrap()["current_missing"], 1);
}

#[test]
fn conflict_review_rejects_stale_snapshot_and_can_choose_absent_content() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = engine(dir.path());
    write(&mut a, "conflict", b"base");
    let entry = a.store.lookup("conflict").unwrap();
    let base = entry.revisions.get("content").cloned();
    a.store
        .write_revision(&entry.id, base.clone(), b"first")
        .unwrap();
    let selected = a
        .store
        .write_revision(&entry.id, base.clone(), b"second")
        .unwrap();
    let detail = a.control(&id(), &json!({"op":"folder-details"})).unwrap();
    let conflicts: Vec<_> = detail["conflicts"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["field"] == "content")
        .map(|c| c["id"].clone())
        .collect();
    a.store.write_revision(&entry.id, base, b"third").unwrap();
    assert!(a.control(&id(), &json!({"op":"resolve","conflicts":conflicts,"revision":selected,"review_token":detail["review_token"]})).unwrap_err().to_string().contains("STALE_CONFLICT_REVIEW"));
    let detail = a.control(&id(), &json!({"op":"folder-details"})).unwrap();
    let conflicts: Vec<_> = detail["conflicts"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["field"] == "content")
        .map(|c| c["id"].clone())
        .collect();
    fs::remove_file(a.store.root.join("objects").join(hash(b"second"))).unwrap();
    let request = id();
    let payload = json!({"op":"resolve","conflicts":conflicts,"revision":selected,"review_token":detail["review_token"]});
    let result = a.control(&request, &payload).unwrap();
    assert_eq!(a.control(&request, &payload).unwrap(), result);
    assert_eq!(
        a.store.lookup("conflict").unwrap().content,
        Some(hash(b"second"))
    );
}

#[test]
fn bulk_install_wait_does_not_block_control_rpc() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = engine(&dir.path().join("a"));
    let b = Arc::new(Mutex::new(engine(&dir.path().join("b"))));
    write(&mut a, "bulk", b"bytes");
    let (ab, ba) = {
        let b = b.lock().unwrap();
        (
            SyncRoute {
                peer: b.store.device.clone(),
                repo: a.store.repo.clone(),
                local_replica: a.store.device.clone(),
                remote_replica: b.store.device.clone(),
            },
            SyncRoute {
                peer: a.store.device.clone(),
                repo: a.store.repo.clone(),
                local_replica: b.store.device.clone(),
                remote_replica: a.store.device.clone(),
            },
        )
    };
    let info = runtime::start_rpc(b.clone(), &dir.path().join("b"), None).unwrap();
    let page = paired_sync::offer(&a, &ab, None).unwrap();
    let remote = info.clone();
    let transfer_lock = b.lock().unwrap().transfers.for_object(&hash(b"bytes"));
    let guard = transfer_lock.lock().unwrap();
    let install = std::thread::spawn(move || {
        // `published_rpc` takes the engine with `try_lock` and answers
        // WORKER_BUSY, before applying anything, when it collides with the
        // polling loop below; retry it (CCR-WP0.INT-1).
        loop {
            match runtime::rpc(
                &remote,
                &id(),
                json!({"op":"published-exchange","route":ba,"page":page}),
            ) {
                Err(error) if error.to_string().contains("WORKER_BUSY") => {
                    std::thread::sleep(Duration::from_millis(1))
                }
                result => break result,
            }
        }
    });
    // Up to 10 s (was 0.5 s): the metadata apply can be retried above.
    for _ in 0..2000 {
        if b.lock().unwrap().store.lookup("bulk").is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let status = runtime::rpc(&info, &id(), json!({"op":"status"})).unwrap();
    assert_eq!(status["content"]["current_missing"], 1);
    runtime::rpc(&info, &id(), json!({"op":"mkdir","path":"responsive"})).unwrap();
    drop(guard);
    install.join().unwrap().unwrap();
    b.lock()
        .unwrap()
        .stopping
        .store(true, std::sync::atomic::Ordering::Relaxed);
}

#[test]
fn complete_and_lost_partial_cursors_recover_without_blocking_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = engine(&dir.path().join("a"));
    let mut b = engine(&dir.path().join("b"));
    let bytes = vec![11; 512 * 1024];
    let object = write(&mut a, "resume", &bytes);
    exchange(&mut a, &mut b, true);
    exchange(&mut a, &mut b, true);
    let folder = b.store.root.join("incoming-objects");
    fs::create_dir_all(&folder).unwrap();
    fs::write(folder.join(&object), &bytes).unwrap(); // crash just before finalization
    for _ in 0..6 {
        exchange(&mut b, &mut a, false);
    }
    assert_eq!(b.store.read_object(&object).unwrap(), bytes);
    fs::remove_file(b.store.root.join("objects").join(&object)).unwrap();
    a.store
        .remember_peer_objects(&b.store.device, &BTreeSet::new())
        .unwrap();
    a.store
        .remember_peer_partials(
            &b.store.device,
            &BTreeMap::from([(object.clone(), 100_000)]),
        )
        .unwrap();
    for _ in 0..10 {
        exchange(&mut a, &mut b, false);
    }
    assert_eq!(b.store.read_object(&object).unwrap(), bytes);
}
