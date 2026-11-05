//! The `session.features.none` fault, in a test binary of its own: the fault
//! state is process-global, so arming it must not share a process with other
//! tests. One test only.
use std::{net::TcpListener, sync::Arc, thread};
use tkfs::{
    model::protocol::{LaneRole, features},
    pairing_code::Verifier,
    session::{
        self, CodeAuth, ConnOptions,
        handshake::{self, AcceptPolicy},
    },
    util::fault::{self, FaultAction, FaultConfig},
};

struct Policy(Verifier);

impl AcceptPolicy for Policy {
    fn installation(&self) -> String {
        "server".into()
    }
    fn verifier(&self) -> Option<(Verifier, u64)> {
        Some((self.0.clone(), 1))
    }
    fn admit(
        &self,
        _: &tkfs::model::protocol::frame::ClientHello,
    ) -> Result<(), tkfs::model::errors::ErrorWire> {
        Ok(())
    }
    fn legacy_attempt(&self, _: std::net::SocketAddr, _: &serde_json::Value) {}
}

#[test]
fn the_fault_is_evaluated_once_per_side_and_empties_both_lists() {
    let (code, verifier) = Verifier::generate();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let policy: Arc<dyn AcceptPolicy> = Arc::new(Policy(verifier));
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        session::accept(stream, policy, ConnOptions::default())
    });
    // One hit per side: a count of 2 covers exactly one handshake.
    let mut config = FaultConfig::new(features::FAULT_NONE, FaultAction::Fail);
    config.count = Some(2);
    fault::configure(Some(config));
    let mut hello = handshake::client_hello("client", LaneRole::Control, None, 3);
    hello.features = vec![features::NOTIFY.into()];
    let (client, server_hello) = session::dial(
        &endpoint,
        hello,
        &CodeAuth { code },
        None,
        ConnOptions::default(),
    )
    .unwrap();
    let (server_conn, _) = server.join().unwrap().unwrap();
    assert_eq!(fault::fired(), 2, "once per side, not once per use");
    assert!(server_hello.features.is_empty());
    assert!(client.peer().features.is_empty());
    assert!(server_conn.peer().features.is_empty());
    client.close();
    server_conn.close();
}
