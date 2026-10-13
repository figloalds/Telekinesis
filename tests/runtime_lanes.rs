use serde_json::{Value, json};
use std::{
    net::TcpListener,
    sync::{Arc, Mutex},
    thread,
};
use tkfs::{
    core::{Store, id},
    runtime::{self, Discovery, Engine, RpcLane},
    ws,
};

#[test]
fn control_activation_is_bounded_and_only_committed_events_are_acknowledged() {
    use tkfs::paired_sync::{self, SyncRoute};
    let root = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = Engine::new(Store::initialize(&root.path().join("a"), &repo, &id()).unwrap());
    for n in 0..80 {
        a.store.create(&format!("dir-{n}"), "directory").unwrap();
    }
    let b = Arc::new(Mutex::new(Engine::new(
        Store::initialize(&root.path().join("b"), &repo, &id()).unwrap(),
    )));
    let ab = SyncRoute {
        peer: id(),
        repo: repo.clone(),
        local_replica: a.store.device.clone(),
        remote_replica: b.lock().unwrap().store.device.clone(),
    };
    let ba = SyncRoute {
        peer: id(),
        repo,
        local_replica: ab.remote_replica.clone(),
        remote_replica: ab.local_replica.clone(),
    };
    let info = runtime::start_rpc(b.clone(), &root.path().join("b"), None).unwrap();
    let page = paired_sync::worker_control(
        &mut a,
        &json!({"op":"published-offer","route":ab,"metadata_only":true}),
    )
    .unwrap();
    let mut lane = RpcLane::default();
    let mut previous = 0;
    for round in 0..100 {
        let reply = lane
            .call(
                &info,
                &id(),
                json!({"op":"published-exchange","route":ba,"page":page,"metadata_only":true}),
            )
            .unwrap();
        let committed = b.lock().unwrap().store.shared_event_inventory().unwrap();
        assert!(committed.len() - previous <= 32);
        assert!(
            reply["ack"]
                .as_array()
                .unwrap()
                .iter()
                .all(|event| committed.contains(event.as_str().unwrap()))
        );
        if round == 0 {
            assert!(reply["pending_events"].as_u64().unwrap() > 0);
        }
        lane.call(&info, &id(), json!({"op":"published-summary"}))
            .unwrap();
        if committed.len() == 80 {
            break;
        }
        previous = committed.len();
        assert!(round < 99, "bounded activation did not converge");
    }
    b.lock()
        .unwrap()
        .stopping
        .store(true, std::sync::atomic::Ordering::Relaxed);
}

#[test]
fn worker_serves_multiple_requests_and_recovers_from_application_errors_on_one_socket() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Mutex::new(Engine::new(
        Store::initialize(dir.path(), &id(), &id()).unwrap(),
    )));
    let info = runtime::start_rpc(engine.clone(), dir.path(), None).unwrap();
    let mut socket = ws::connect(&info.address).unwrap();
    let request = |payload| json!({"request":id(),"payload":payload});
    assert_eq!(
        ws::call(&mut socket, &request(json!({"op":"published-summary"}))).unwrap()["repo"],
        info.repo
    );
    assert!(
        ws::call(&mut socket, &request(json!({"op":"no-such-operation"})))
            .unwrap_err()
            .downcast_ref::<ws::RemoteError>()
            .is_some()
    );
    ws::call(
        &mut socket,
        &request(json!({"op":"mkdir","path":"after-error"})),
    )
    .unwrap();
    assert!(engine.lock().unwrap().store.lookup("after-error").is_ok());
    ws::call(&mut socket, &request(json!({"op":"stop"}))).unwrap();
}

#[test]
fn worker_lane_reuses_socket_but_does_not_replay_an_uncertain_request() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let info = Discovery {
        format: 2,
        address: format!("ws://{}", listener.local_addr().unwrap()),
        state: String::new(),
        mount: None,
        repo: id(),
        device: id(),
    };
    let server = thread::spawn(move || {
        let mut socket = ws::accept(listener.accept().unwrap().0).unwrap();
        let first: Value = ws::receive(&mut socket).unwrap();
        assert_eq!(first["request"], "first");
        ws::reply(&mut socket, Err(anyhow::anyhow!("REJECTED"))).unwrap();
        let uncertain: Value = ws::receive(&mut socket).unwrap();
        assert_eq!(uncertain["request"], "uncertain");
        drop(socket); // The mutation might have committed before the connection died.
        let mut socket = ws::accept(listener.accept().unwrap().0).unwrap();
        let next: Value = ws::receive(&mut socket).unwrap();
        assert_eq!(next["request"], "next");
        ws::reply(&mut socket, Ok(json!({"done":true}))).unwrap();
    });
    let mut lane = RpcLane::default();
    assert!(
        lane.call(&info, "first", json!({}))
            .unwrap_err()
            .downcast_ref::<ws::RemoteError>()
            .is_some()
    );
    assert!(lane.call(&info, "uncertain", json!({})).is_err());
    assert_eq!(lane.call(&info, "next", json!({})).unwrap()["done"], true);
    server.join().unwrap();
}
