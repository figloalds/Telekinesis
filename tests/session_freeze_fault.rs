//! A session thread that did not run (a frozen process) must not take its
//! peer for dead. Its own test binary: the fault state is process-global.
use anyhow::Result;
use serde_json::json;
use std::{
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
};
use tkfs::{
    model::protocol::LaneRole,
    session::{Conn, ConnOptions, PeerInfo, Priority, Side},
    util::fault::{self, FaultAction, FaultConfig},
};
use tungstenite::{WebSocket, stream::MaybeTlsStream};

type Ws = WebSocket<MaybeTlsStream<TcpStream>>;

fn peer() -> PeerInfo {
    PeerInfo {
        installation: "inst-test".into(),
        role: LaneRole::Control,
        lane: None,
        session: "session-test".into(),
        verifier_generation: 1,
        features: Vec::new(),
    }
}

fn ws_pair() -> (Ws, Ws) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        tungstenite::accept(MaybeTlsStream::Plain(stream)).expect("WebSocket accept")
    });
    let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let client = tungstenite::client(
        format!("ws://127.0.0.1:{port}/"),
        MaybeTlsStream::Plain(stream),
    )
    .expect("WebSocket connect")
    .0;
    (client, server.join().unwrap())
}

#[test]
fn a_stalled_io_thread_does_not_declare_its_peer_dead() -> Result<()> {
    let options = ConnOptions {
        heartbeat: Duration::from_millis(100),
        dead_after: Duration::from_millis(600),
        ..ConnOptions::default()
    };
    let (client_ws, server_ws) = ws_pair();
    let client = Conn::from_established(client_ws, Side::Client, peer(), options)?;
    let server = Conn::from_established(server_ws, Side::Server, peer(), options)?;
    // Each side's I/O thread stalls once for longer than `dead_after`.
    let mut config = FaultConfig::new("session.io.turn", FaultAction::Hang);
    config.ms = Some(1000);
    config.count = Some(2);
    fault::configure(Some(config));
    // Wait out both stalls, with room for the threads to run again.
    thread::sleep(Duration::from_millis(2500));
    assert_eq!(fault::fired(), 2);
    assert!(client.closed().is_none(), "client: {:?}", client.closed());
    assert!(server.closed().is_none(), "server: {:?}", server.closed());
    let reply = client.call(
        "ping",
        json!({}),
        vec![],
        Priority::Control,
        Duration::from_secs(5),
    )?;
    assert_eq!(reply.header, json!({ "alive": true }));
    client.close();
    server.close();
    Ok(())
}
