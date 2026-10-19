use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};
use tkfs::{
    bulk_sync::{Lane, Page},
    core::*,
    paired_sync::{self, SyncRoute},
    runtime::{self, Engine},
};

#[allow(clippy::type_complexity)]
fn fixture() -> (
    tempfile::TempDir,
    Engine,
    Arc<Mutex<Engine>>,
    SyncRoute,
    SyncRoute,
    runtime::Discovery,
    Vec<(String, Vec<u8>)>,
) {
    let root = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = Engine::new(Store::initialize(&root.path().join("a"), &repo, &id()).unwrap());
    let mut b = Engine::new(Store::initialize(&root.path().join("b"), &repo, &id()).unwrap());
    let ab = SyncRoute {
        peer: id(),
        repo: repo.clone(),
        local_replica: a.store.device.clone(),
        remote_replica: b.store.device.clone(),
    };
    let ba = SyncRoute {
        peer: id(),
        repo,
        local_replica: ab.remote_replica.clone(),
        remote_replica: ab.local_replica.clone(),
    };
    let mut objects = Vec::new();
    for index in 0..3 {
        let lane = Lane { index, count: 3 };
        let bytes = (0u64..)
            .map(|seed| seed.to_le_bytes().repeat(16 * 1024))
            .find(|bytes| lane.owns(&hash(bytes)))
            .unwrap();
        let file = a.store.create(&format!("file-{index}"), "file").unwrap();
        a.store
            .write_revision(&file.id, file.revisions.get("content").cloned(), &bytes)
            .unwrap();
        objects.push((hash(&bytes), bytes));
    }
    let page = paired_sync::worker_control(
        &mut a,
        &json!({"op":"published-offer","route":ab,"metadata_only":true}),
    )
    .unwrap();
    let response = paired_sync::worker_control(
        &mut b,
        &json!({"op":"published-exchange","route":ba,"page":page,"metadata_only":true}),
    )
    .unwrap();
    let sent: Vec<_> = a
        .store
        .events()
        .unwrap()
        .iter()
        .map(|event| event.id.clone())
        .collect();
    paired_sync::worker_control(&mut a, &json!({"op":"published-apply","route":ab,"page":response,"request":page["request"],"sent":sent,"metadata_only":true})).unwrap();
    let b = Arc::new(Mutex::new(b));
    let info = runtime::start_rpc(b.clone(), &root.path().join("b"), None).unwrap();
    (root, a, b, ab, ba, info, objects)
}
fn incoming(
    route: &SyncRoute,
    lane: Lane,
    object: &str,
    bytes: &[u8],
    offset: usize,
    end: usize,
) -> Page {
    Page {
        format: 1,
        repo: route.repo.clone(),
        sender: route.remote_replica.clone(),
        receiver: route.local_replica.clone(),
        request: id(),
        reply_to: None,
        lane,
        known_objects: BTreeSet::new(),
        partial_objects: BTreeMap::new(),
        wanted_objects: BTreeSet::new(),
        object_parts: BTreeMap::from([(
            object.to_owned(),
            ObjectPart {
                offset: offset as u64,
                total: bytes.len() as u64,
                hex: hex::encode(&bytes[offset..end]),
            },
        )]),
    }
}
fn exchange(info: &runtime::Discovery, route: &SyncRoute, page: &Page) -> anyhow::Result<Value> {
    runtime::rpc(
        info,
        &id(),
        json!({"op":"published-bulk-exchange","route":route,"lane":page.lane,"page":page}),
    )
}

#[test]
fn different_object_lanes_and_discovery_progress_while_an_object_and_the_engine_are_locked() {
    let (_root, _a, b, _ab, ba, info, objects) = fixture();
    let lock = b.lock().unwrap().transfers.for_object(&objects[0].0);
    let blocked = lock.lock().unwrap();
    let page0 = incoming(
        &ba,
        Lane { index: 0, count: 3 },
        &objects[0].0,
        &objects[0].1,
        0,
        objects[0].1.len(),
    );
    let remote = info.clone();
    let route = ba.clone();
    let (tx, rx) = mpsc::channel();
    let first = std::thread::spawn(move || tx.send(exchange(&remote, &route, &page0)).unwrap());
    assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
    // Holding this mutex proves the second lane does not merely acquire it faster.
    let engine_guard = b.lock().unwrap();
    let page1 = incoming(
        &ba,
        Lane { index: 1, count: 3 },
        &objects[1].0,
        &objects[1].1,
        0,
        objects[1].1.len(),
    );
    let remote = info.clone();
    let route = ba.clone();
    let (tx2, rx2) = mpsc::channel();
    let second = std::thread::spawn(move || {
        let result = exchange(&remote, &route, &page1)
            .and_then(|_| runtime::rpc(&remote, &id(), json!({"op":"published-summary"})));
        tx2.send(result).unwrap();
    });
    let progressed = rx2.recv_timeout(Duration::from_secs(3));
    drop(engine_guard);
    drop(blocked);
    assert!(progressed.unwrap().is_ok());
    rx.recv_timeout(Duration::from_secs(3)).unwrap().unwrap();
    first.join().unwrap();
    second.join().unwrap();
    for (hash, bytes) in &objects[..2] {
        assert_eq!(b.lock().unwrap().store.read_object(hash).unwrap(), *bytes);
    }
    b.lock()
        .unwrap()
        .stopping
        .store(true, std::sync::atomic::Ordering::Relaxed);
}

#[test]
fn lane_reassignment_and_overlapping_resumes_keep_verified_content_and_reject_wrong_routes() {
    let (_root, _a, b, _ab, ba, info, objects) = fixture();
    let (object, bytes) = &objects[0];
    let mut page = incoming(&ba, Lane { index: 0, count: 3 }, object, bytes, 0, 70000);
    exchange(&info, &ba, &page).unwrap();
    assert!(!b.lock().unwrap().store.object_available(object));
    // A new pool and a stale peer can overlap the old durable prefix.
    page = incoming(
        &ba,
        Lane { index: 0, count: 1 },
        object,
        bytes,
        50000,
        bytes.len(),
    );
    exchange(&info, &ba, &page).unwrap();
    assert_eq!(b.lock().unwrap().store.read_object(object).unwrap(), *bytes);
    page.lane = Lane { index: 1, count: 3 };
    assert!(
        exchange(&info, &ba, &page)
            .unwrap_err()
            .to_string()
            .contains("OBJECT_ON_WRONG_LANE")
    );
    page.lane = Lane { index: 0, count: 1 };
    page.receiver = id();
    assert!(
        exchange(&info, &ba, &page)
            .unwrap_err()
            .to_string()
            .contains("BULK_ROUTE_MISMATCH")
    );
    b.lock()
        .unwrap()
        .stopping
        .store(true, std::sync::atomic::Ordering::Relaxed);
}

#[test]
fn bulk_inventory_cannot_authorize_private_bytes_or_acknowledge_metadata() {
    let (_root, a, b, ab, ba, info, objects) = fixture();
    let private = a.store.put_object(b"private-only bytes").unwrap();
    let mut page = incoming(
        &ba,
        Lane { index: 0, count: 1 },
        &private,
        b"private-only bytes",
        0,
        18,
    );
    assert!(
        exchange(&info, &ba, &page)
            .unwrap_err()
            .to_string()
            .contains("UNAUTHORIZED_OBJECT")
    );
    let known_before = b
        .lock()
        .unwrap()
        .store
        .known_by_peer(&ba.remote_replica)
        .unwrap();
    page = incoming(
        &ba,
        Lane { index: 0, count: 1 },
        &objects[0].0,
        &objects[0].1,
        0,
        100,
    );
    page.reply_to = Some(id());
    assert!(
        exchange(&info, &ba, &page)
            .unwrap_err()
            .to_string()
            .contains("INVALID_SYNC_REQUEST")
    );
    assert_eq!(
        b.lock()
            .unwrap()
            .store
            .known_by_peer(&ba.remote_replica)
            .unwrap(),
        known_before
    );
    // Unknown/private requests do not become permitted output on the other end.
    let a_root = a.store.root.clone();
    let a = Arc::new(Mutex::new(a));
    let source = runtime::start_rpc(a.clone(), &a_root, None).unwrap();
    let mut inventory = page.inventory();
    inventory.sender = ab.remote_replica.clone();
    inventory.receiver = ab.local_replica.clone();
    inventory.wanted_objects.insert(private.clone());
    let offered = runtime::rpc(
        &source,
        &id(),
        json!({"op":"published-bulk-offer","route":ab,"lane":inventory.lane,"inventory":inventory}),
    )
    .unwrap();
    assert!(offered["object_parts"].get(&private).is_none());
    a.lock()
        .unwrap()
        .stopping
        .store(true, std::sync::atomic::Ordering::Relaxed);
    b.lock()
        .unwrap()
        .stopping
        .store(true, std::sync::atomic::Ordering::Relaxed);
}
