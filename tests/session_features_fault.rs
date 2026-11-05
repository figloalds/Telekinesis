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
fn the_fault_is_evaluated_once_per_side_and_agrees_with_what_each_side_sent() {
    let (code, verifier) = Verifier::generate();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let policy: Arc<dyn AcceptPolicy> = Arc::new(Policy(verifier));
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        session::accept(stream, policy, ConnOptions::default())
    });
    // Exactly one evaluation fires. The dialer evaluates first, so it acts as
    // an older peer; the listener's own evaluation does not fire.
    let mut config = FaultConfig::new(features::FAULT_NONE, FaultAction::Fail);
    config.count = Some(1);
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
    assert_eq!(fault::fired(), 1);
    // The listener did not fire: it advertised its features, and recorded
    // the dialer's HELLO, which the dialer sent empty.
    assert_eq!(
        server_hello.features,
        vec![
            features::CONTENT_PUSH.to_string(),
            features::NOTIFY.to_string()
        ]
    );
    assert!(server_conn.peer().features.is_empty());
    // The dialer fired: sent empty, and records nothing although the
    // server advertised features.
    assert!(client.peer().features.is_empty());
    client.close();
    server_conn.close();
}
