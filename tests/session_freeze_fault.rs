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

/// The fault state is process-global: the tests of this binary take turns.
static ONE_AT_A_TIME: std::sync::Mutex<()> = std::sync::Mutex::new(());

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
    let _turn = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
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

/// A peer that stays silent is still found dead across repeated stalls: only
/// the time the thread ran counts, the stalls are skipped, not forgiven.
#[test]
fn a_silent_peer_is_still_dead_across_repeated_stalls() {
    let _turn = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let options = ConnOptions {
        heartbeat: Duration::from_millis(100),
        dead_after: Duration::from_millis(600),
        ..ConnOptions::default()
    };
    // The raw peer never reads or writes after the handshake.
    let (_silent_peer, server_ws) = ws_pair();
    let server = Conn::from_established(server_ws, Side::Server, peer(), options).unwrap();
    let mut config = FaultConfig::new("session.io.turn", FaultAction::Hang);
    config.ms = Some(1000);
    config.count = Some(2);
    fault::configure(Some(config));
    let start = std::time::Instant::now();
    while server.closed().is_none() {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "a silent peer was never declared dead"
        );
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(server.closed().unwrap().code, "TRANSPORT_DEAD");
    assert_eq!(fault::fired(), 2);
}
