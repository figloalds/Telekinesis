//! The `objects.store.write` fault, in a test binary of its own: the fault
//! state is process-global, so arming it must not share a process with other
//! tests. One test only.
//!
//! A listener whose worker write fails answers `objects.store` with an error
//! and reports nothing stored; the digest stays listed, so the same object is
//! accepted once the fault is spent.
use serde_json::{Value, json};
use std::{
    net::{TcpListener, TcpStream},
    sync::Arc,
    thread,
    time::Duration,
};
use tkfs::{
    content::Kind,
    model::{
        orchestrator::CallError,
        protocol::{LaneRole, LaneSpec},
    },
    orchestrator::{
        catalog::Catalog,
        config::Config,
        demand::{Demand, Seq},
        network::{self, FolderDirectory, PeerService},
        sync::{Object, Service},
        worker::Ready,
        worker_client::WorkerLink,
    },
    runtime::Engine,
    session::{self, Conn, ConnOptions, Handler, PeerInfo, Priority, Side},
    util::{
        cancel::Cancel,
        fault::{self, FaultAction, FaultConfig},
    },
};
use tungstenite::{WebSocket, stream::MaybeTlsStream};

const TOKEN: &str = "fault-test-token";

struct Published(Arc<WorkerLink>);

impl FolderDirectory for Published {
    fn link_by_repo(&self, repo: &str) -> Option<Arc<WorkerLink>> {
        (repo == "r1").then(|| self.0.clone())
    }
    fn published(&self) -> Vec<Value> {
        Vec::new()
    }
}

type Ws = WebSocket<MaybeTlsStream<TcpStream>>;

fn ws_pair() -> (Ws, Ws) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        tungstenite::accept(MaybeTlsStream::Plain(stream)).unwrap()
    });
    let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let client = tungstenite::client(
        format!("ws://127.0.0.1:{port}/"),
        MaybeTlsStream::Plain(stream),
    )
    .unwrap()
    .0;
    (client, server.join().unwrap())
}

fn info() -> PeerInfo {
    PeerInfo {
        installation: "peer-a".into(),
        role: LaneRole::Bulk,
        lane: Some(LaneSpec::new(0, 1)),
        session: "session".into(),
        verifier_generation: 1,
        features: Vec::new(),
    }
}

fn call(client: &Conn, op: &str, header: Value, body: Vec<u8>) -> Result<Value, String> {
    match client.call(op, header, body, Priority::Data, Duration::from_secs(10)) {
        Ok(reply) => Ok(serde_json::from_slice(&reply.body).unwrap()),
        Err(CallError::Remote(error)) => Err(error.code),
        Err(other) => panic!("{op}: {other}"),
    }
}

#[test]
fn a_failed_worker_write_stores_nothing_and_the_retry_succeeds() {
    let home = tempfile::tempdir().unwrap();
    let source = Engine::open(&home.path().join("source"), Some("r1")).unwrap();
    let target = Engine::open(&home.path().join("target"), Some("r1")).unwrap();
    let h = source.open_file("one", true, false, true).unwrap();
    source.write(h, 0, b"first", false, false).unwrap();
    source.flush(Some(h)).unwrap();
    source.close(h).unwrap();
    let events: Vec<_> = source
        .store
        .lock()
        .unwrap()
        .events_after(0, 4096)
        .unwrap()
        .into_iter()
        .map(|(_, event)| event)
        .collect();
    let service = Arc::new(Service {
        engine: target.clone(),
        demand: Demand::new(Seq::new(), target.working.content().packs.clone()),
    });
    service
        .handle_request(
            "feed.apply",
            &json!({}),
            &serde_json::to_vec(&events).unwrap(),
        )
        .unwrap();
    let digest = source.lookup("one").unwrap().content.unwrap().digest;
    let object = Object {
        digest,
        kind: Kind::Chunk,
        bytes: (*source.working.content().packs.get(digest).unwrap()).clone(),
    };
    // The worker, served over loopback to the orchestrator's link.
    let socket = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("ws://{}", socket.local_addr().unwrap());
    let handler: Arc<dyn Handler> = service.clone();
    thread::spawn(move || {
        let mut conns = Vec::new();
        for stream in socket.incoming() {
            let Ok(stream) = stream else { break };
            if let Ok(conn) = session::accept_loopback(
                stream,
                Some(TOKEN),
                ConnOptions::for_role(LaneRole::Control),
            ) {
                conn.serve(handler.clone());
                conns.push(conn);
            }
        }
    });
    let link = WorkerLink::connect(
        &Ready {
            ready: true,
            repo: "r1".into(),
            device: "device".into(),
            instance: "instance".into(),
            pid: 1,
            address,
            token: TOKEN.into(),
            version: "test".into(),
            ipc: 1,
            mounts: Vec::new(),
        },
        2,
    )
    .unwrap();
    let mut config = Config::initialize(&home.path().join("orchestrator.toml")).unwrap();
    config.network.listen = String::new();
    let catalog = Catalog::open(&config.data_directory).unwrap();
    catalog.save_paired("peer-a", [7; 32]).unwrap();
    let net = network::start(
        &config,
        catalog,
        Arc::new(Published(link.clone())),
        Cancel::new(),
        None,
    )
    .unwrap();
    let (client, server) = ws_pair();
    let client =
        Conn::from_established(client, Side::Client, info(), ConnOptions::default()).unwrap();
    let server =
        Conn::from_established(server, Side::Server, info(), ConnOptions::default()).unwrap();
    server.serve(Arc::new(PeerService::new(net, info())));

    let listed = call(
        &client,
        "objects.wanted",
        json!({"repo": "r1", "limit": 64, "exclude": [], "after": null}),
        Vec::new(),
    )
    .unwrap();
    assert_eq!(listed["missing"].as_array().unwrap().len(), 1, "{listed}");

    // Exactly one write fails.
    let mut config = FaultConfig::new("objects.store.write", FaultAction::Fail);
    config.count = Some(1);
    fault::configure(Some(config));
    let body = serde_json::to_vec(&[object]).unwrap();
    let store = |body: &[u8]| {
        call(
            &client,
            "objects.store",
            json!({"repo": "r1"}),
            body.to_vec(),
        )
    };
    let failed = store(&body);
    assert_eq!(fault::fired(), 1);
    assert_eq!(failed.unwrap_err(), "FAULT_INJECTED");
    assert!(
        !target.working.content().packs.contains(digest).unwrap(),
        "nothing is durable after a failed write"
    );

    // The digest is still wanted: the same object is stored on the retry.
    let stored = store(&body).unwrap();
    assert_eq!(stored["stored"], json!([digest]), "{stored}");
    assert_eq!(stored["refused"], json!([]), "{stored}");
    assert!(target.working.content().packs.contains(digest).unwrap());
    fault::configure(None);
    client.close();
    server.close();
    link.close();
}
