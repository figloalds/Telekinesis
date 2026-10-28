//! Current session establishment and application credentials over TLS/WSS or VPN.
//!
//! The session layer (`Conn`, WP1.9) is built in parallel, so these tests
//! drive the handshake up to the authenticated socket (`connect_v3`,
//! `accept_v3`, `client_handshake`, `server_handshake`). `dial`/`accept`
//! (and the loopback pair) are checked to hand the authenticated socket to
//! `Conn::from_established`, whichever way that stub or its implementation
//! answers.
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tkfs::{
    model::{
        errors::{ErrorClass, ErrorWire},
        protocol::{
            LANE_HEADER, LEGACY_NEEDS_UPGRADE_REPLY, LaneRole, LaneSpec, PROTOCOL_NAME,
            TOKEN_HEADER, UPGRADE_HEADER, UPGRADE_LOOPBACK,
            frame::{ClientHello, Limits, ServerHello},
        },
    },
    pairing_code::Verifier,
    session::{
        self, CodeAuth, ConnOptions, PeerInfo,
        handshake::{self, AcceptPolicy, DialError},
    },
    ws::{self, RawSocket},
};
use tungstenite::{
    Message,
    protocol::{CloseFrame, frame::coding::CloseCode},
    stream::MaybeTlsStream,
};

const CLIENT: &str = "client-installation";
const SERVER: &str = "server-installation";
const GENERATION: u64 = 7;

// ---------------------------------------------------------------------------
// Fixtures

/// An `AcceptPolicy` with a settable verifier, an optional rotation applied
/// right after the next verifier read, a scripted admission answer, and a
/// record of legacy attempts and admitted hellos.
struct Policy {
    installation: String,
    verifier: Mutex<Option<(Verifier, u64)>>,
    rotate_after_read: Mutex<Option<(Verifier, u64)>>,
    refuse: Mutex<Option<ErrorWire>>,
    legacy: Mutex<Vec<(SocketAddr, Value)>>,
    admitted: Mutex<Vec<ClientHello>>,
}

impl Policy {
    fn new(installation: &str, verifier: Option<(Verifier, u64)>) -> Arc<Policy> {
        Arc::new(Policy {
            installation: installation.into(),
            verifier: Mutex::new(verifier),
            rotate_after_read: Mutex::new(None),
            refuse: Mutex::new(None),
            legacy: Mutex::new(Vec::new()),
            admitted: Mutex::new(Vec::new()),
        })
    }

    fn set_verifier(&self, verifier: Option<(Verifier, u64)>) {
        *self.verifier.lock().unwrap() = verifier;
    }
}

impl AcceptPolicy for Policy {
    fn installation(&self) -> String {
        self.installation.clone()
    }

    fn verifier(&self) -> Option<(Verifier, u64)> {
        let current = self.verifier.lock().unwrap().clone();
        if let Some(next) = self.rotate_after_read.lock().unwrap().take() {
            *self.verifier.lock().unwrap() = Some(next);
        }
        current
    }

    fn admit(&self, hello: &ClientHello) -> Result<(), ErrorWire> {
        self.admitted.lock().unwrap().push(hello.clone());
        match self.refuse.lock().unwrap().clone() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn legacy_attempt(&self, addr: SocketAddr, hello: &Value) {
        self.legacy.lock().unwrap().push((addr, hello.clone()));
    }
}

/// A server policy holding a fresh pairing code at `GENERATION`.
fn paired() -> (String, Verifier, Arc<Policy>) {
    let (code, verifier) = Verifier::generate();
    let policy = Policy::new(SERVER, Some((verifier.clone(), GENERATION)));
    (code, verifier, policy)
}

type Accepted = Result<(RawSocket, PeerInfo), ErrorWire>;

/// Accepts one connection on a fresh listener and runs `accept_v3` on it.
fn serve_once(policy: Arc<Policy>) -> (String, SocketAddr, JoinHandle<Accepted>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        handshake::accept_v3(stream, policy.as_ref(), Limits::default())
    });
    (format!("ws://{addr}"), addr, server)
}

fn tcp(ws: &RawSocket) -> &TcpStream {
    match ws.get_ref() {
        MaybeTlsStream::Plain(stream) => stream,
        MaybeTlsStream::NativeTls(tls) => tls.get_ref(),
        _ => unreachable!(),
    }
}

/// Bounds a test's blocking reads on an authenticated socket.
fn bounded(ws: &RawSocket) {
    let stream = tcp(ws);
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
}

fn read_binary(ws: &mut RawSocket) -> Vec<u8> {
    loop {
        match ws.read().unwrap() {
            Message::Binary(bytes) => return bytes.to_vec(),
            Message::Ping(_) | Message::Pong(_) => {}
            other => panic!("unexpected message {other:?}"),
        }
    }
}

/// Both authenticated sockets carry a 200 KiB message each way: the pre-auth
/// bound (16 KiB) was raised and nothing is left in the pipe.
fn exchange_after_auth(client: &mut RawSocket, server: &mut RawSocket) {
    bounded(client);
    bounded(server);
    let up = vec![0x5a; 200 * 1024];
    let down = vec![0xa5; 200 * 1024];
    // The reader runs concurrently: a blocking send must not rely on socket
    // buffers absorbing the whole message.
    thread::scope(|scope| {
        let reader = scope.spawn(|| read_binary(server));
        client.send(Message::Binary(up.clone().into())).unwrap();
        assert_eq!(reader.join().unwrap(), up);
    });
    thread::scope(|scope| {
        let reader = scope.spawn(|| read_binary(client));
        server.send(Message::Binary(down.clone().into())).unwrap();
        assert_eq!(reader.join().unwrap(), down);
    });
}

/// A test-side encoder of the §6.2 pre-auth frame layout (independent of the
/// implementation): kind, flags, reserved u16, header_len u32 LE, header.
fn frame_bytes(kind: u8, header: &impl Serialize) -> Vec<u8> {
    let header = serde_json::to_vec(header).unwrap();
    let mut frame = vec![kind, 0, 0, 0];
    frame.extend_from_slice(&(header.len() as u32).to_le_bytes());
    frame.extend_from_slice(&header);
    frame
}

fn send_frame(ws: &mut RawSocket, kind: u8, header: &impl Serialize) {
    ws.send(Message::Binary(frame_bytes(kind, header).into()))
        .unwrap();
}

fn read_frame(ws: &mut RawSocket) -> (u8, Value) {
    let bytes = read_binary(ws);
    assert!(bytes.len() >= 8, "short frame");
    assert_eq!(&bytes[1..4], &[0, 0, 0], "flags and reserved are zero");
    let len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    assert_eq!(bytes.len(), 8 + len, "pre-auth frames carry no body");
    (bytes[0], serde_json::from_slice(&bytes[8..]).unwrap())
}

/// A raw v3 client socket (upgrade only), for scripting frames by hand.
fn raw_v3(endpoint: &str) -> RawSocket {
    let (ws, response) = ws::connect_with(
        endpoint,
        &[("X-Tkfs-Sync", "4")],
        None,
        Duration::from_secs(3),
    )
    .unwrap();
    assert_eq!(response.headers()["X-Tkfs-Sync"], "4");
    bounded(&ws);
    ws
}

/// Reads until the peer closes; returns the data messages seen.
fn drain_until_closed(ws: &mut RawSocket) -> Vec<Message> {
    let mut seen = Vec::new();
    loop {
        match ws.read() {
            Ok(Message::Close(_)) => {
                // Send the queued close reply, as a well-behaved peer does.
                let _ = ws.flush();
                return seen;
            }
            Err(_) => return seen,
            Ok(Message::Ping(_) | Message::Pong(_)) => {}
            Ok(message) => seen.push(message),
        }
    }
}

/// A TCP relay that records every byte in each direction.
struct Relay {
    endpoint: String,
    up: Arc<Mutex<Vec<u8>>>,
    down: Arc<Mutex<Vec<u8>>>,
}

impl Relay {
    fn start(target: SocketAddr) -> Relay {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let up = Arc::new(Mutex::new(Vec::new()));
        let down = Arc::new(Mutex::new(Vec::new()));
        let (log_up, log_down) = (up.clone(), down.clone());
        thread::spawn(move || {
            let (client, _) = listener.accept().unwrap();
            let server = TcpStream::connect(target).unwrap();
            for stream in [&client, &server] {
                stream
                    .set_read_timeout(Some(Duration::from_secs(20)))
                    .unwrap();
            }
            let pipe = |mut from: TcpStream, mut to: TcpStream, log: Arc<Mutex<Vec<u8>>>| {
                thread::spawn(move || {
                    let mut buffer = [0u8; 16 * 1024];
                    loop {
                        match from.read(&mut buffer) {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                log.lock().unwrap().extend_from_slice(&buffer[..n]);
                                if to.write_all(&buffer[..n]).is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    let _ = to.shutdown(Shutdown::Write);
                })
            };
            let a = pipe(
                client.try_clone().unwrap(),
                server.try_clone().unwrap(),
                log_up,
            );
            let b = pipe(server, client, log_down);
            let _ = (a.join(), b.join());
        });
        Relay { endpoint, up, down }
    }

    /// Per direction: the raw bytes, then what they conveyed (see
    /// [`conveyed`]), so a search covers masked and unmasked forms.
    fn bytes(&self) -> (Vec<u8>, Vec<u8>) {
        let both = |raw: &Mutex<Vec<u8>>| {
            let raw = raw.lock().unwrap().clone();
            let mut all = raw.clone();
            all.extend_from_slice(b"\n--conveyed--\n");
            all.extend(conveyed(&raw));
            all
        };
        (both(&self.up), both(&self.down))
    }
}

/// What a recorded direction conveyed: the HTTP upgrade text plus every
/// WebSocket frame payload, unmasked. Client frames are XOR-masked on the
/// wire (with the key sent in clear), so a raw search alone would miss any
/// plaintext the client sent.
fn conveyed(raw: &[u8]) -> Vec<u8> {
    let Some(head) = raw.windows(4).position(|window| window == b"\r\n\r\n") else {
        return raw.to_vec();
    };
    let mut out = raw[..head + 4].to_vec();
    let mut rest = &raw[head + 4..];
    while rest.len() >= 2 {
        let masked = rest[1] & 0x80 != 0;
        let (mut len, mut at) = ((rest[1] & 0x7f) as usize, 2);
        if len == 126 && rest.len() >= 4 {
            (len, at) = (u16::from_be_bytes([rest[2], rest[3]]) as usize, 4);
        } else if len == 127 && rest.len() >= 10 {
            (len, at) = (
                u64::from_be_bytes(rest[2..10].try_into().unwrap()) as usize,
                10,
            );
        }
        let mask = if masked && rest.len() >= at + 4 {
            at += 4;
            Some([rest[at - 4], rest[at - 3], rest[at - 2], rest[at - 1]])
        } else {
            None
        };
        let end = (at + len).min(rest.len());
        let payload = &rest[at.min(end)..end];
        out.extend(payload.iter().enumerate().map(|(index, byte)| match mask {
            Some(mask) => byte ^ mask[index % 4],
            None => *byte,
        }));
        rest = &rest[end..];
    }
    out
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

/// None of `secrets` (as text, upper-case text, or raw key bytes) is in `bytes`.
fn assert_absent(bytes: &[u8], secrets: &[&str], what: &str) {
    for secret in secrets {
        assert!(
            !contains(bytes, secret.as_bytes()),
            "{what} carries a secret"
        );
        assert!(
            !contains(bytes, secret.to_uppercase().as_bytes()),
            "{what} carries a secret (upper case)"
        );
        if let Ok(raw) = hex::decode(secret) {
            assert!(!contains(bytes, &raw), "{what} carries raw secret bytes");
        }
    }
}

fn hello(role: LaneRole, lane: Option<LaneSpec>) -> ClientHello {
    handshake::client_hello(CLIENT, role, lane, 3)
}

// ---------------------------------------------------------------------------
// Authentication

#[test]
fn right_code_authenticates_the_client_for_every_role() {
    for (role, lane) in [
        (LaneRole::Control, None),
        (LaneRole::Bulk, Some(LaneSpec::new(1, 3))),
        (LaneRole::Probe, None),
    ] {
        let (code, _, policy) = paired();
        let (endpoint, _, server) = serve_once(policy.clone());
        let hello = hello(role, lane);
        let session = hello.session.clone();
        let (mut client_ws, server_hello) =
            handshake::connect_v3(&endpoint, hello, &CodeAuth { code }, Some(SERVER), None)
                .unwrap_or_else(|error| panic!("{role:?}: {error}"));
        let (mut server_ws, peer) = server.join().unwrap().unwrap();
        assert_eq!(server_hello.protocol, PROTOCOL_NAME);
        assert_eq!(server_hello.version, 4);
        assert_eq!(server_hello.installation, SERVER);
        assert_eq!((server_hello.role, server_hello.lane), (role, lane));
        assert_eq!(server_hello.verifier_generation, GENERATION);
        assert_eq!(server_hello.limits, Limits::default());
        assert_eq!(
            peer,
            PeerInfo {
                installation: CLIENT.into(),
                role,
                lane,
                session,
                verifier_generation: GENERATION,
            }
        );
        assert_eq!(policy.admitted.lock().unwrap().len(), 1);
        exchange_after_auth(&mut client_ws, &mut server_ws);
    }
}

#[test]
fn application_credential_uses_the_selected_channel_without_custom_wrapping() {
    let (code, verifier, policy) = paired();
    let (_, addr, server) = serve_once(policy);
    let relay = Relay::start(addr);
    let (mut client_ws, server_hello) = handshake::connect_v3(
        &relay.endpoint,
        hello(LaneRole::Control, None),
        &CodeAuth { code: code.clone() },
        None,
        None,
    )
    .unwrap();
    let (mut server_ws, _) = server.join().unwrap().unwrap();
    let (up, down) = relay.bytes();
    assert!(
        contains(&up, PROTOCOL_NAME.as_bytes()),
        "the relay saw the HELLO"
    );
    assert!(
        contains(&down, server_hello.salt.as_bytes()),
        "the relay saw the HELLO_ACK"
    );
    assert!(
        contains(&up, verifier.hash.as_bytes()),
        "the application credential is carried by the selected transport"
    );
    assert_absent(&up, &[&code], "client → server");
    assert_absent(&down, &[&code, &verifier.hash], "server → client");
    exchange_after_auth(&mut client_ws, &mut server_ws);
}

#[test]
fn wrong_code_is_unauthorized_without_verifier_leakage() {
    let (code, verifier, policy) = paired();
    let (_, addr, server) = serve_once(policy);
    let relay = Relay::start(addr);
    let wrong = Verifier::generate().0;
    let error = handshake::connect_v3(
        &relay.endpoint,
        hello(LaneRole::Control, None),
        &CodeAuth {
            code: wrong.clone(),
        },
        None,
        None,
    )
    .unwrap_err();
    let DialError::Unauthorized(wire) = &error else {
        panic!("expected Unauthorized, got {error:?}");
    };
    assert_eq!(wire.code, "INVALID_PAIRING_CODE");
    assert_eq!(error.class(), ErrorClass::NeedsUserAction);
    let server_error = server.join().unwrap().unwrap_err();
    assert_eq!(server_error.code, "INVALID_PAIRING_CODE");
    let reports = format!("{error:?} {error} {server_error:?} {server_error}");
    assert_absent(
        reports.as_bytes(),
        &[&code, &wrong, &verifier.hash, &verifier.salt],
        "error reports",
    );
    let (up, down) = relay.bytes();
    assert_absent(&up, &[&code, &wrong, &verifier.hash], "client → server");
    assert_absent(&down, &[&code, &wrong, &verifier.hash], "server → client");
}

#[test]
fn an_application_credential_can_establish_fresh_connections() {
    let (code, _, policy) = paired();
    for _ in 0..2 {
        let (endpoint, _, server) = serve_once(policy.clone());
        let mut socket = raw_v3(&endpoint);
        send_frame(&mut socket, 1, &hello(LaneRole::Control, None));
        let (kind, ack) = read_frame(&mut socket);
        assert_eq!(kind, 2);
        let hello: ServerHello = serde_json::from_value(ack).unwrap();
        let key = Verifier::derive_key(&hello.salt, &code);
        send_frame(
            &mut socket,
            3,
            &json!({"credential":hex::encode(key),"peer":false}),
        );
        let (kind, ack) = read_frame(&mut socket);
        assert_eq!(kind, 4);
        assert_eq!(ack, json!({}));
        server.join().unwrap().unwrap();
        let _ = socket.close(None);
    }
}

/// A scripted v3 server: echoes the upgrade, reads HELLO and answers an
/// honest `ServerHello` after `lie` edits it, then returns every data message
/// the client sends next (until it closes).
fn lying_server(
    lie: impl FnOnce(&ClientHello, &mut ServerHello) + Send + 'static,
) -> (String, JoinHandle<Vec<Message>>) {
    let (_, verifier) = Verifier::generate();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut ws = ws::accept_with(stream, 64 * 1024, |_, response| {
            response
                .headers_mut()
                .insert("X-Tkfs-Sync", "4".parse().unwrap());
        })
        .unwrap();
        let (kind, hello) = read_frame(&mut ws);
        assert_eq!(kind, 1);
        let hello: ClientHello = serde_json::from_value(hello).unwrap();
        let mut server = ServerHello {
            protocol: PROTOCOL_NAME.into(),
            version: 4,
            installation: SERVER.into(),
            role: hello.role,
            lane: hello.lane,
            salt: verifier.salt,
            verifier_generation: GENERATION,
            features: vec![],
            limits: Limits::default(),
        };
        lie(&hello, &mut server);
        send_frame(&mut ws, 2, &server);
        drain_until_closed(&mut ws)
    });
    (endpoint, server)
}

#[test]
fn client_checks_the_server_hello_before_proving() {
    type Lie = Box<dyn FnOnce(&ClientHello, &mut ServerHello) + Send>;
    let bulk = Some(LaneSpec::new(1, 3));
    // (case, requested role and lane, the lie, expected code)
    let cases: Vec<(&str, LaneRole, Option<LaneSpec>, Lie, &str)> = vec![
        (
            "echoes another role",
            LaneRole::Control,
            None,
            Box::new(|_, server| server.role = LaneRole::Probe),
            "PROTOCOL_VIOLATION",
        ),
        (
            "echoes another lane",
            LaneRole::Bulk,
            bulk,
            Box::new(|_, server| server.lane = Some(LaneSpec::new(2, 3))),
            "PROTOCOL_VIOLATION",
        ),
        (
            "drops the lane",
            LaneRole::Bulk,
            bulk,
            Box::new(|_, server| server.lane = None),
            "PROTOCOL_VIOLATION",
        ),
        (
            "claims the client's installation",
            LaneRole::Control,
            None,
            Box::new(|_, server| server.installation = CLIENT.into()),
            "CANNOT_PAIR_WITH_SELF",
        ),
        (
            "has no installation",
            LaneRole::Control,
            None,
            Box::new(|_, server| server.installation = String::new()),
            "PROTOCOL_VIOLATION",
        ),
        (
            "speaks another protocol",
            LaneRole::Control,
            None,
            Box::new(|_, server| server.protocol = "other".into()),
            "PROTOCOL_VIOLATION",
        ),
        (
            "answers another version",
            LaneRole::Control,
            None,
            Box::new(|_, server| server.version = 3),
            "PEER_PROTOCOL_UNSUPPORTED",
        ),
    ];
    let (code, _) = Verifier::generate();
    for (case, role, lane, lie, expected) in cases {
        let (endpoint, server) = lying_server(lie);
        let error = handshake::connect_v3(
            &endpoint,
            hello(role, lane),
            &CodeAuth { code: code.clone() },
            None,
            None,
        )
        .unwrap_err();
        assert_eq!(error.to_wire().code, expected, "{case}: {error:?}");
        if expected == "CANNOT_PAIR_WITH_SELF" {
            assert!(matches!(error, DialError::Rejected(_)), "{case}: {error:?}");
            assert_eq!(error.class(), ErrorClass::NeedsUserAction, "{case}");
        }
        // The client refused before proving anything to that server.
        let received = server.join().unwrap();
        assert!(
            received.is_empty(),
            "{case}: the client sent {} message(s) after HELLO_ACK",
            received.len()
        );
    }
}

#[test]
fn self_pair_is_refused_before_auth() {
    let (code, verifier) = Verifier::generate();
    let policy = Policy::new(CLIENT, Some((verifier, GENERATION)));
    let (endpoint, _, server) = serve_once(policy.clone());
    let error = handshake::connect_v3(
        &endpoint,
        hello(LaneRole::Probe, None),
        &CodeAuth { code },
        None,
        None,
    )
    .unwrap_err();
    assert!(
        matches!(&error, DialError::Rejected(wire) if wire.code == "CANNOT_PAIR_WITH_SELF"),
        "{error:?}"
    );
    assert_eq!(error.class(), ErrorClass::NeedsUserAction);
    assert_eq!(
        server.join().unwrap().unwrap_err().code,
        "CANNOT_PAIR_WITH_SELF"
    );
    assert!(policy.admitted.lock().unwrap().is_empty());
}

#[test]
fn rotated_generation_refuses_old_codes() {
    let (old_code, _, policy) = paired();
    let (new_code, new_verifier) = Verifier::generate();
    policy.set_verifier(Some((new_verifier.clone(), GENERATION + 1)));

    let (endpoint, _, server) = serve_once(policy.clone());
    let error = handshake::connect_v3(
        &endpoint,
        hello(LaneRole::Control, None),
        &CodeAuth { code: old_code },
        None,
        None,
    )
    .unwrap_err();
    assert!(
        matches!(&error, DialError::Unauthorized(wire) if wire.code == "INVALID_PAIRING_CODE"),
        "{error:?}"
    );
    assert_eq!(
        server.join().unwrap().unwrap_err().code,
        "INVALID_PAIRING_CODE"
    );

    let (endpoint, _, server) = serve_once(policy.clone());
    let (_, server_hello) = handshake::connect_v3(
        &endpoint,
        hello(LaneRole::Control, None),
        &CodeAuth {
            code: new_code.clone(),
        },
        None,
        None,
    )
    .unwrap();
    assert_eq!(server_hello.verifier_generation, GENERATION + 1);
    let (_, peer) = server.join().unwrap().unwrap();
    assert_eq!(peer.verifier_generation, GENERATION + 1);

    // The code rotates between HELLO_ACK and AUTH: the proof is for the
    // announced verifier, but that generation is no longer current.
    let (newest_code, newest) = Verifier::generate();
    *policy.rotate_after_read.lock().unwrap() = Some((newest, GENERATION + 2));
    let (endpoint, _, server) = serve_once(policy.clone());
    let error = handshake::connect_v3(
        &endpoint,
        hello(LaneRole::Control, None),
        &CodeAuth { code: new_code },
        None,
        None,
    )
    .unwrap_err();
    assert!(
        matches!(&error, DialError::Unauthorized(wire) if wire.code == "PAIRING_CODE_CHANGED"),
        "{error:?}"
    );
    assert_eq!(
        server.join().unwrap().unwrap_err().code,
        "PAIRING_CODE_CHANGED"
    );

    // The rotated-in code works.
    let (endpoint, _, server) = serve_once(policy);
    let (_, server_hello) = handshake::connect_v3(
        &endpoint,
        hello(LaneRole::Control, None),
        &CodeAuth { code: newest_code },
        None,
        None,
    )
    .unwrap();
    assert_eq!(server_hello.verifier_generation, GENERATION + 2);
    server.join().unwrap().unwrap();
}

#[test]
fn refusals_before_auth_use_their_goaway_codes() {
    let (code, verifier) = Verifier::generate();
    // (expected code, server policy, offered versions)
    type Case = (&'static str, Arc<Policy>, Option<Vec<u32>>);
    let cases: Vec<Case> = vec![
        ("PAIRING_CODE_NOT_SET", Policy::new(SERVER, None), None),
        ("PEER_REVOKED", paired().2, None),
        ("TOO_MANY_SESSIONS", paired().2, None),
        (
            "PEER_PROTOCOL_UNSUPPORTED",
            Policy::new(SERVER, Some((verifier, GENERATION))),
            Some(vec![3]),
        ),
    ];
    for (code_expected, policy, versions) in cases {
        if code_expected == "PEER_REVOKED" || code_expected == "TOO_MANY_SESSIONS" {
            *policy.refuse.lock().unwrap() = Some(ErrorWire::coded(code_expected, "refused"));
        }
        let (endpoint, _, server) = serve_once(policy.clone());
        let mut hello = hello(LaneRole::Control, None);
        if let Some(versions) = versions {
            hello.versions = versions;
        }
        let error = handshake::connect_v3(
            &endpoint,
            hello,
            &CodeAuth { code: code.clone() },
            None,
            None,
        )
        .unwrap_err();
        match (&error, code_expected) {
            (DialError::Unsupported { offered, supported }, "PEER_PROTOCOL_UNSUPPORTED") => {
                assert_eq!(
                    (offered.as_slice(), supported.as_slice()),
                    (&[3][..], &[4][..])
                );
            }
            (DialError::Rejected(wire), _) => assert_eq!(wire.code, code_expected),
            _ => panic!("{code_expected}: {error:?}"),
        }
        assert_eq!(error.to_wire().code, code_expected);
        assert_eq!(server.join().unwrap().unwrap_err().code, code_expected);
    }
}

#[test]
fn installation_change_is_detected_after_auth() {
    let (code, _, policy) = paired();
    let (endpoint, _, server) = serve_once(policy);
    let error = handshake::connect_v3(
        &endpoint,
        hello(LaneRole::Control, None),
        &CodeAuth { code },
        Some("the-installation-in-the-catalog"),
        None,
    )
    .unwrap_err();
    assert!(
        matches!(&error, DialError::Rejected(wire) if wire.code == "PEER_INSTALLATION_CHANGED"),
        "{error:?}"
    );
    assert_eq!(error.class(), ErrorClass::NeedsUserAction);
    // The server authenticated the dialer; the check is the dialer's.
    server.join().unwrap().unwrap();
}

// ---------------------------------------------------------------------------
// Legacy detection (invariant 20)

/// An emulated 6298d0b peer endpoint: plain `ws::accept` (no `X-Tkfs-Sync`
/// echo), recording every data message it receives on `connections` sockets.
fn legacy_server(connections: usize) -> (String, JoinHandle<Vec<Message>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let mut received = Vec::new();
        for _ in 0..connections {
            let (stream, _) = listener.accept().unwrap();
            let mut socket = ws::accept(stream).unwrap();
            loop {
                match socket.read() {
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(Message::Ping(_) | Message::Pong(_)) => {}
                    Ok(message) => received.push(message),
                }
            }
        }
        received
    });
    (endpoint, server)
}

#[test]
fn legacy_both_directions() {
    // A v2 dialer (6298d0b `peering::dial_role`) facing a v3 server.
    let (code, _, policy) = paired();
    let (endpoint, _, server) = serve_once(policy.clone());
    let mut socket = ws::connect(&endpoint).unwrap();
    assert_eq!(socket.codec(), "json", "the v3 server offers no v2 codec");
    let local = match socket.get_ref() {
        MaybeTlsStream::Plain(stream) => stream.local_addr().unwrap(),
        _ => unreachable!(),
    };
    let error = ws::call(
        &mut socket,
        &json!({"op":"pair","version":2,"code":code,"installation":CLIENT,"lane":"control",
                "bulk_protocol":1,"bulk_lane":null}),
    )
    .unwrap_err();
    let remote = error
        .downcast_ref::<ws::RemoteError>()
        .expect("a legacy-format error reply");
    let expected: Value = serde_json::from_str(LEGACY_NEEDS_UPGRADE_REPLY).unwrap();
    assert_eq!(remote.0, expected["error"].as_str().unwrap());
    assert!(remote.0.starts_with("PEER_NEEDS_UPGRADE: "));
    let server_error = server.join().unwrap().unwrap_err();
    assert_eq!(server_error.code, "PEER_NEEDS_UPGRADE");
    assert_eq!(server_error.class, ErrorClass::Incompatible);
    let attempts = policy.legacy.lock().unwrap().clone();
    assert_eq!(attempts.len(), 1, "legacy_attempt is called once");
    let (addr, hello) = &attempts[0];
    assert_eq!(*addr, local);
    assert_eq!(
        (&hello["op"], &hello["version"], &hello["installation"]),
        (&json!("pair"), &json!(2), &json!(CLIENT))
    );
    assert_eq!(hello["code"], "<redacted>");
    assert!(!hello.to_string().contains(&code));
    assert!(policy.admitted.lock().unwrap().is_empty());

    // A v3 dialer facing a v2 server: no echo, so nothing is sent at all.
    let (legacy, received) = legacy_server(2);
    let addr = legacy.trim_start_matches("ws://").parse().unwrap();
    let relay = Relay::start(addr);
    let error = handshake::connect_v3(
        &relay.endpoint,
        hello_for_legacy(),
        &CodeAuth { code: code.clone() },
        None,
        None,
    )
    .unwrap_err();
    assert!(matches!(error, DialError::NeedsUpgrade { .. }), "{error:?}");
    assert_eq!(error.to_wire().code, "PEER_NEEDS_UPGRADE");
    assert_eq!(error.class(), ErrorClass::Incompatible);
    // The frozen `dial` entry point fails the same way.
    let error = session::dial(
        &legacy,
        hello_for_legacy(),
        &CodeAuth { code: code.clone() },
        None,
        ConnOptions::default(),
    )
    .unwrap_err();
    assert!(matches!(error, DialError::NeedsUpgrade { .. }), "{error:?}");
    let received = received.join().unwrap();
    assert!(
        received.is_empty(),
        "the v2 server received {} message(s)",
        received.len()
    );
    let (up, _) = relay.bytes();
    assert_absent(&up, &[&code], "v3 dialer → v2 server");
    assert!(
        contains(&up.to_ascii_lowercase(), b"x-tkfs-sync: 4"),
        "the upgrade request offered v3"
    );
}

fn hello_for_legacy() -> ClientHello {
    hello(LaneRole::Control, None)
}

// ---------------------------------------------------------------------------
// Pre-auth bounds (invariant 22)

#[test]
fn oversize_preauth_messages_are_rejected() {
    // A 17 KiB HELLO.
    let (_, _, policy) = paired();
    let (endpoint, _, server) = serve_once(policy.clone());
    let mut ws = raw_v3(&endpoint);
    let mut big = hello(LaneRole::Control, None);
    big.features = vec!["x".repeat(17 * 1024)];
    let started = Instant::now();
    send_frame(&mut ws, 1, &big);
    assert!(drain_until_closed(&mut ws).is_empty());
    let error = server.join().unwrap().unwrap_err();
    assert_eq!(error.code, "PROTOCOL_VIOLATION", "{error}");
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(policy.admitted.lock().unwrap().is_empty());

    // A 17 KiB legacy hello.
    let (endpoint, _, server) = serve_once(policy.clone());
    let mut socket = ws::connect(&endpoint).unwrap();
    ws::send(
        &mut socket,
        &json!({"op":"pair","version":2,"padding":"x".repeat(17 * 1024)}),
    )
    .unwrap();
    let error = server.join().unwrap().unwrap_err();
    assert_eq!(error.code, "PROTOCOL_VIOLATION", "{error}");
    assert!(policy.legacy.lock().unwrap().is_empty());

    // The legacy hello bound is 16 KiB of JSON exactly (it has no frame
    // prefix): 16,384 bytes is a legacy attempt, 16,385 a violation.
    let padded = |total: usize| {
        let empty = json!({"op":"pair","version":2,"padding":""});
        let base = serde_json::to_string(&empty).unwrap().len();
        let padded = json!({"op":"pair","version":2,"padding":"x".repeat(total - base)});
        assert_eq!(serde_json::to_string(&padded).unwrap().len(), total);
        padded
    };
    let (endpoint, _, server) = serve_once(policy.clone());
    let mut socket = ws::connect(&endpoint).unwrap();
    let error = ws::call(&mut socket, &padded(16 * 1024)).unwrap_err();
    assert!(
        error.downcast_ref::<ws::RemoteError>().is_some(),
        "{error:#}"
    );
    assert_eq!(
        server.join().unwrap().unwrap_err().code,
        "PEER_NEEDS_UPGRADE"
    );
    assert_eq!(policy.legacy.lock().unwrap().len(), 1);
    let (endpoint, _, server) = serve_once(policy.clone());
    let mut socket = ws::connect(&endpoint).unwrap();
    ws::send(&mut socket, &padded(16 * 1024 + 1)).unwrap();
    let error = server.join().unwrap().unwrap_err();
    assert_eq!(error.code, "PROTOCOL_VIOLATION", "{error}");
    assert_eq!(
        policy.legacy.lock().unwrap().len(),
        1,
        "an oversize legacy hello is not reported"
    );
}

#[test]
fn preauth_protocol_violations_close_the_connection() {
    let hello_json = serde_json::to_vec(&hello(LaneRole::Control, None)).unwrap();
    let mut reserved = frame_bytes(1, &hello(LaneRole::Control, None));
    reserved[2] = 1;
    let mut flagged = frame_bytes(1, &hello(LaneRole::Control, None));
    flagged[1] = 1;
    let mut trailing = frame_bytes(1, &hello(LaneRole::Control, None));
    trailing.extend_from_slice(b"body");
    let mut lying = frame_bytes(1, &hello(LaneRole::Control, None));
    lying[4..8].copy_from_slice(&(hello_json.len() as u32 + 10).to_le_bytes());
    let no_lane = hello(LaneRole::Bulk, None);
    let cases: Vec<(&str, Message)> = vec![
        ("reserved field", Message::Binary(reserved.into())),
        ("flags", Message::Binary(flagged.into())),
        ("trailing body", Message::Binary(trailing.into())),
        ("truncated header", Message::Binary(lying.into())),
        (
            "unknown kind",
            Message::Binary(frame_bytes(99, &json!({})).into()),
        ),
        (
            "REQUEST before auth",
            Message::Binary(frame_bytes(5, &json!({"id":1,"op":"ping"})).into()),
        ),
        (
            "AUTH first",
            Message::Binary(frame_bytes(3, &json!({"credential":"00","peer":false})).into()),
        ),
        (
            "array header",
            Message::Binary(frame_bytes(1, &json!([1, 2])).into()),
        ),
        (
            "bulk without lane",
            Message::Binary(frame_bytes(1, &no_lane).into()),
        ),
        (
            "text message",
            Message::Text(String::from_utf8(hello_json).unwrap().into()),
        ),
        ("short frame", Message::Binary(vec![1, 0, 0].into())),
    ];
    for (case, message) in cases {
        let (_, _, policy) = paired();
        let (endpoint, _, server) = serve_once(policy.clone());
        let mut ws = raw_v3(&endpoint);
        ws.send(message).unwrap();
        assert!(drain_until_closed(&mut ws).is_empty(), "{case}");
        let error = server.join().unwrap().unwrap_err();
        assert_eq!(error.code, "PROTOCOL_VIOLATION", "{case}: {error}");
        assert!(policy.admitted.lock().unwrap().is_empty(), "{case}");
    }
}

/// Reads until the peer closes, expecting no data message; returns the code
/// of its close frame (`None` when the connection ended without one).
fn close_code(ws: &mut RawSocket) -> Option<CloseCode> {
    loop {
        match ws.read() {
            Ok(Message::Close(frame)) => {
                let _ = ws.flush();
                return frame.map(|frame| frame.code);
            }
            Err(_) => return None,
            Ok(Message::Ping(_) | Message::Pong(_)) => {}
            Ok(message) => panic!("unexpected message {message:?}"),
        }
    }
}

/// A scripted v3 server: echoes the upgrade, reads HELLO, then closes with
/// `code` and `reason` instead of answering.
fn closing_server(code: CloseCode, reason: &'static str) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut ws = ws::accept_with(stream, 64 * 1024, |_, response| {
            response
                .headers_mut()
                .insert("X-Tkfs-Sync", "4".parse().unwrap());
        })
        .unwrap();
        assert_eq!(read_frame(&mut ws).0, 1);
        ws.close(Some(CloseFrame {
            code,
            reason: reason.into(),
        }))
        .unwrap();
        drain_until_closed(&mut ws);
    });
    (endpoint, server)
}

#[test]
fn peer_closes_are_not_protocol_violations() {
    // A v3 dialer that hangs up politely, with a reason, instead of sending
    // HELLO: the server reports a closed connection, not a violation, and
    // does not answer with a protocol close.
    let (_, _, policy) = paired();
    let (endpoint, _, server) = serve_once(policy.clone());
    let mut ws = raw_v3(&endpoint);
    ws.close(Some(CloseFrame {
        code: CloseCode::Away,
        reason: "going away\n".into(),
    }))
    .unwrap();
    assert_ne!(close_code(&mut ws), Some(CloseCode::Protocol));
    let error = server.join().unwrap().unwrap_err();
    assert_eq!(error.code, "WS_CLOSED", "{error}");
    assert!(error.message.contains(r"going away\n"), "{error}");
    assert!(!error.message.contains('\n'), "{error}");
    assert!(policy.admitted.lock().unwrap().is_empty());

    // The same from a legacy dialer: no legacy attempt, no violation.
    let (endpoint, _, server) = serve_once(policy.clone());
    let mut socket = ws::connect(&endpoint).unwrap();
    socket
        .close(Some(CloseFrame {
            code: CloseCode::Away,
            reason: "going away".into(),
        }))
        .unwrap();
    let error = server.join().unwrap().unwrap_err();
    assert_eq!(error.code, "WS_CLOSED", "{error}");
    assert!(policy.legacy.lock().unwrap().is_empty());

    // Client side: a server that closes politely is a closed connection
    // (Transient); one that closes with a protocol error is a violation.
    let (code, _) = Verifier::generate();
    for (close, reason, expected) in [
        (CloseCode::Away, "going away", "WS_CONNECT_FAILED"),
        (
            CloseCode::Protocol,
            "PROTOCOL_VIOLATION: bad hello",
            "PROTOCOL_VIOLATION",
        ),
    ] {
        let (endpoint, server) = closing_server(close, reason);
        let error = handshake::connect_v3(
            &endpoint,
            hello(LaneRole::Control, None),
            &CodeAuth { code: code.clone() },
            None,
            None,
        )
        .unwrap_err();
        assert_eq!(error.to_wire().code, expected, "{error:?}");
        server.join().unwrap();
    }
}

#[test]
fn an_invalid_server_installation_is_the_servers_failure() {
    // The server's own id is invalid: it refuses every handshake as its own
    // failure (INSTALLATION_REQUIRED, close code 1011) and never blames the
    // dialer with PROTOCOL_VIOLATION.
    let (code, verifier) = Verifier::generate();
    for installation in [String::new(), "bad\u{0}id".into(), "x".repeat(129)] {
        let policy = Policy::new(&installation, Some((verifier.clone(), GENERATION)));
        let (endpoint, _, server) = serve_once(policy.clone());
        let error = handshake::connect_v3(
            &endpoint,
            hello(LaneRole::Control, None),
            &CodeAuth { code: code.clone() },
            None,
            None,
        )
        .unwrap_err();
        assert!(matches!(error, DialError::Connect(_)), "{error:?}");
        assert_eq!(error.class(), ErrorClass::Transient);
        let failure = server.join().unwrap().unwrap_err();
        assert_eq!(failure.code, "INSTALLATION_REQUIRED", "{failure}");
        assert!(policy.admitted.lock().unwrap().is_empty());

        // On the wire: no HELLO_ACK, an internal-error close.
        let (endpoint, _, server) = serve_once(policy.clone());
        let mut ws = raw_v3(&endpoint);
        send_frame(&mut ws, 1, &hello(LaneRole::Control, None));
        assert_eq!(close_code(&mut ws), Some(CloseCode::Error));
        assert_eq!(
            server.join().unwrap().unwrap_err().code,
            "INSTALLATION_REQUIRED"
        );
    }
}

fn assert_closed_at_handshake_timeout(started: Instant, what: &str) {
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(4_500) && elapsed < Duration::from_secs(9),
        "{what}: closed after {elapsed:?}"
    );
}

#[test]
fn silent_and_trickling_peers_are_closed_at_5s() {
    let cases: Vec<JoinHandle<()>> = vec![
        // A TCP client that never sends the upgrade.
        thread::spawn(|| {
            let (_, _, policy) = paired();
            let (endpoint, _, server) = serve_once(policy);
            let started = Instant::now();
            let mut client = TcpStream::connect(endpoint.trim_start_matches("ws://")).unwrap();
            let error = server.join().unwrap().unwrap_err();
            assert_eq!(error.code, "REQUEST_TIMEOUT", "{error}");
            assert_closed_at_handshake_timeout(started, "silent TCP client");
            client
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut buffer = [0u8; 64];
            assert!(matches!(client.read(&mut buffer), Ok(0) | Err(_)));
        }),
        // A v3 client that upgrades and never sends HELLO.
        thread::spawn(|| {
            let (_, _, policy) = paired();
            let (endpoint, _, server) = serve_once(policy.clone());
            let ws = raw_v3(&endpoint);
            let started = Instant::now();
            let error = server.join().unwrap().unwrap_err();
            assert_eq!(error.code, "REQUEST_TIMEOUT", "{error}");
            assert_closed_at_handshake_timeout(started, "silent v3 client");
            drop(ws);
        }),
        // A v3 client that pings every second instead of sending HELLO:
        // control messages are not progress.
        thread::spawn(|| {
            let (_, _, policy) = paired();
            let (endpoint, _, server) = serve_once(policy.clone());
            let mut ws = raw_v3(&endpoint);
            let started = Instant::now();
            let pinger = thread::spawn(move || {
                while started.elapsed() < Duration::from_secs(30)
                    && ws.send(Message::Ping(Vec::new().into())).is_ok()
                {
                    thread::sleep(Duration::from_secs(1));
                }
            });
            let error = server.join().unwrap().unwrap_err();
            assert_eq!(error.code, "REQUEST_TIMEOUT", "{error}");
            assert_closed_at_handshake_timeout(started, "pinging v3 client");
            assert!(policy.admitted.lock().unwrap().is_empty());
            pinger.join().unwrap();
        }),
        // A legacy client that upgrades and never sends its hello.
        thread::spawn(|| {
            let (_, _, policy) = paired();
            let (endpoint, _, server) = serve_once(policy.clone());
            let socket = ws::connect(&endpoint).unwrap();
            let started = Instant::now();
            let error = server.join().unwrap().unwrap_err();
            assert_eq!(error.code, "REQUEST_TIMEOUT", "{error}");
            assert_closed_at_handshake_timeout(started, "silent legacy client");
            assert!(policy.legacy.lock().unwrap().is_empty());
            drop(socket);
        }),
        // A client trickling its upgrade request one byte every 300 ms: every
        // read returns well within the socket timeout, so only the step
        // watchdog can close it.
        thread::spawn(|| {
            let (_, _, policy) = paired();
            let (endpoint, _, server) = serve_once(policy);
            let mut client = TcpStream::connect(endpoint.trim_start_matches("ws://")).unwrap();
            let started = Instant::now();
            let trickle = thread::spawn(move || {
                for byte in b"GET / HTTP/1.1\r\nHost: localhost\r\nX-Padding: "
                    .iter()
                    .cycle()
                {
                    if client.write_all(&[*byte]).is_err() {
                        return;
                    }
                    thread::sleep(Duration::from_millis(300));
                    if started.elapsed() > Duration::from_secs(30) {
                        panic!("the trickling client was never closed");
                    }
                }
            });
            let error = server.join().unwrap().unwrap_err();
            assert_eq!(error.code, "REQUEST_TIMEOUT", "{error}");
            assert_closed_at_handshake_timeout(started, "trickling client");
            trickle.join().unwrap();
        }),
        // A v3 server that upgrades and never answers HELLO.
        thread::spawn(|| {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let endpoint = format!("ws://{}", listener.local_addr().unwrap());
            let silent = thread::spawn(move || {
                let (stream, _) = listener.accept().unwrap();
                let mut ws = ws::accept_with(stream, 64 * 1024, |_, response| {
                    response
                        .headers_mut()
                        .insert("X-Tkfs-Sync", "4".parse().unwrap());
                })
                .unwrap();
                bounded(&ws);
                drain_until_closed(&mut ws)
            });
            let started = Instant::now();
            let error = handshake::connect_v3(
                &endpoint,
                hello(LaneRole::Control, None),
                &CodeAuth { code: "c".into() },
                None,
                None,
            )
            .unwrap_err();
            assert_eq!(error, DialError::Timeout);
            assert_closed_at_handshake_timeout(started, "silent v3 server");
            // It received the HELLO and nothing else.
            assert_eq!(silent.join().unwrap().len(), 1);
        }),
        // A server trickling its upgrade response one byte every 300 ms: the
        // dial attempt is abandoned at 3 s.
        thread::spawn(|| {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let endpoint = format!("ws://{}", listener.local_addr().unwrap());
            let trickler = thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                for byte in b"HTTP/1.1 101 Switching Protocols\r\nX-Padding: "
                    .iter()
                    .cycle()
                {
                    if stream.write_all(&[*byte]).is_err() {
                        return;
                    }
                    thread::sleep(Duration::from_millis(300));
                }
            });
            let started = Instant::now();
            let error = handshake::connect_v3(
                &endpoint,
                hello(LaneRole::Control, None),
                &CodeAuth { code: "c".into() },
                None,
                None,
            )
            .unwrap_err();
            assert!(matches!(error, DialError::Connect(_)), "{error:?}");
            let elapsed = started.elapsed();
            assert!(
                elapsed >= Duration::from_millis(2_500) && elapsed < Duration::from_secs(7),
                "trickling server: abandoned after {elapsed:?}"
            );
            trickler.join().unwrap();
        }),
    ];
    for case in cases {
        case.join().unwrap();
    }
}

// ---------------------------------------------------------------------------
// Dialing, WSS, and the hand-over to `Conn`

#[test]
fn dial_tries_every_resolved_address() {
    let addresses: Vec<SocketAddr> = ("localhost", 0).to_socket_addrs().unwrap().collect();
    let last = *addresses.last().unwrap();
    // Listen on the last address only, so every earlier one is refused.
    let listener = TcpListener::bind(SocketAddr::new(last.ip(), 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    if addresses.len() < 2 {
        // `ws::tests::connect_with_tries_every_address_in_turn` covers the
        // fallback without the resolver.
        eprintln!("localhost resolves to one address here; fallback not exercised end to end");
    }
    let (code, _, policy) = paired();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        handshake::accept_v3(stream, policy.as_ref(), Limits::default())
    });
    let (_, server_hello) = handshake::connect_v3(
        &format!("ws://localhost:{port}"),
        hello(LaneRole::Control, None),
        &CodeAuth { code },
        Some(SERVER),
        None,
    )
    .unwrap();
    assert_eq!(server_hello.installation, SERVER);
    server.join().unwrap().unwrap();

    let error = session::dial(
        "ws://127.0.0.1:1",
        hello(LaneRole::Control, None),
        &CodeAuth { code: "c".into() },
        None,
        ConnOptions::default(),
    )
    .unwrap_err();
    assert!(matches!(error, DialError::Connect(_)), "{error:?}");
    assert_eq!(error.to_wire().code, "WS_CONNECT_FAILED");
}

fn trusted_connector() -> tungstenite::Connector {
    let ca = native_tls::Certificate::from_pem(include_bytes!("fixtures/tls/ca.pem")).unwrap();
    tungstenite::Connector::NativeTls(
        native_tls::TlsConnector::builder()
            .add_root_certificate(ca)
            .build()
            .unwrap(),
    )
}

#[test]
#[allow(clippy::result_large_err)]
fn wss_handshake_authenticates_over_tls() {
    let identity = native_tls::Identity::from_pkcs12(
        include_bytes!("fixtures/tls/localhost.p12"),
        "telekinesis-test",
    )
    .unwrap();
    let acceptor = native_tls::TlsAcceptor::new(identity).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (code, _, policy) = paired();
    let server = thread::spawn(move || {
        let (tcp, _) = listener.accept().unwrap();
        tcp.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let tls = acceptor.accept(tcp).unwrap();
        let ws = tungstenite::accept_hdr_with_config(
            MaybeTlsStream::NativeTls(tls),
            |request: &tungstenite::handshake::server::Request,
             mut response: tungstenite::handshake::server::Response| {
                assert_eq!(handshake::upgrade_kind(request), handshake::UpgradeKind::V3);
                response
                    .headers_mut()
                    .insert("X-Tkfs-Sync", "4".parse().unwrap());
                Ok(response)
            },
            None,
        )
        .unwrap();
        handshake::server_handshake(ws, policy.as_ref())
    });
    // `localhost` may resolve to `::1` first: the TLS connector is reused
    // for the next address.
    let (mut client_ws, server_hello) = handshake::connect_v3(
        &format!("wss://localhost:{port}/sync"),
        hello(LaneRole::Bulk, Some(LaneSpec::new(0, 1))),
        &CodeAuth { code },
        Some(SERVER),
        Some(trusted_connector()),
    )
    .unwrap();
    assert!(matches!(client_ws.get_ref(), MaybeTlsStream::NativeTls(_)));
    assert_eq!(server_hello.installation, SERVER);
    let (mut server_ws, peer) = server.join().unwrap().unwrap();
    assert_eq!(peer.role, LaneRole::Bulk);
    exchange_after_auth(&mut client_ws, &mut server_ws);
}

/// Either the session started (WP1.9 merged) or the handshake completed and
/// the socket reached the `Conn::from_established` stub.
fn reached_conn<T, E: std::fmt::Debug>(result: &Result<T, E>) -> bool {
    match result {
        Ok(_) => true,
        Err(error) => format!("{error:?}").contains("NOT_IMPLEMENTED"),
    }
}

#[test]
fn dial_and_accept_hand_the_authenticated_socket_to_conn() {
    let (code, _, policy) = paired();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        session::accept(stream, policy, ConnOptions::default())
    });
    let hello = hello(LaneRole::Bulk, Some(LaneSpec::new(2, 3)));
    let session_id = hello.session.clone();
    let dialed = handshake::dial_expecting(
        &endpoint,
        hello,
        &CodeAuth { code },
        Some(SERVER),
        None,
        ConnOptions::for_role(LaneRole::Bulk),
    );
    let accepted = server.join().unwrap();
    assert!(reached_conn(&dialed), "{dialed:?}");
    assert!(reached_conn(&accepted), "{accepted:?}");
    let expected = PeerInfo {
        installation: CLIENT.into(),
        role: LaneRole::Bulk,
        lane: Some(LaneSpec::new(2, 3)),
        session: session_id,
        verifier_generation: GENERATION,
    };
    if let (Ok((conn, server_hello)), Ok((server_conn, peer))) = (&dialed, &accepted) {
        assert_eq!(server_hello.installation, SERVER);
        assert_eq!(conn.peer().installation, SERVER);
        assert_eq!(conn.peer().session, expected.session);
        assert_eq!(peer, &expected);
        assert_eq!(server_conn.peer(), &expected);
        conn.close();
        server_conn.close();
    }
}

#[test]
fn loopback_sessions_check_header_and_token() {
    let accept_loopback = |token: Option<&'static str>| {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            session::accept_loopback(stream, token, ConnOptions::default())
        });
        (endpoint, server)
    };

    // Right token: the upgrade completes and the socket reaches `Conn`.
    let (endpoint, server) = accept_loopback(Some("t0k3n"));
    let dialed = session::dial_loopback(&endpoint, Some("t0k3n"), ConnOptions::default());
    let accepted = server.join().unwrap();
    assert!(reached_conn(&dialed), "{dialed:?}");
    assert!(reached_conn(&accepted), "{accepted:?}");
    if let (Ok(client), Ok(server)) = (&dialed, &accepted) {
        client.close();
        server.close();
    }

    // Wrong or missing token: refused at the upgrade.
    for token in [Some("wrong"), None] {
        let (endpoint, server) = accept_loopback(Some("t0k3n"));
        let error = session::dial_loopback(&endpoint, token, ConnOptions::default()).unwrap_err();
        assert!(
            matches!(&error, DialError::Unauthorized(wire) if wire.code == "ACCESS_DENIED"),
            "{error:?}"
        );
        assert_eq!(server.join().unwrap().unwrap_err().code, "ACCESS_DENIED");
    }

    // A v3 peer dialer on a loopback endpoint, and a loopback dialer on a
    // peer endpoint, are refused at the upgrade.
    let (endpoint, server) = accept_loopback(None);
    let error = handshake::connect_v3(
        &endpoint,
        hello(LaneRole::Control, None),
        &CodeAuth { code: "c".into() },
        None,
        None,
    )
    .unwrap_err();
    assert!(matches!(error, DialError::Connect(_)), "{error:?}");
    assert_eq!(
        server.join().unwrap().unwrap_err().code,
        "PROTOCOL_VIOLATION"
    );
    let (_, _, policy) = paired();
    let (endpoint, _, server) = serve_once(policy);
    let error = session::dial_loopback(&endpoint, None, ConnOptions::default()).unwrap_err();
    assert!(matches!(error, DialError::Connect(_)), "{error:?}");
    assert_eq!(
        server.join().unwrap().unwrap_err().code,
        "PROTOCOL_VIOLATION"
    );

    // A worker that does not serve loopback sessions (no echo).
    let (legacy, received) = legacy_server(1);
    let error = session::dial_loopback(&legacy, None, ConnOptions::default()).unwrap_err();
    assert!(matches!(error, DialError::NeedsUpgrade { .. }), "{error:?}");
    assert!(received.join().unwrap().is_empty());
}

struct Echo;
impl session::Handler for Echo {
    fn handle(&self, req: session::Incoming) {
        req.responder.reply(json!({}), req.body);
    }
}

/// One loopback accept (token `t0k3n`) serving [`Echo`].
fn loopback_server() -> (String, JoinHandle<Result<session::Conn, ErrorWire>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let conn = session::accept_loopback(stream, Some("t0k3n"), ConnOptions::default())?;
        conn.serve(Arc::new(Echo));
        Ok(conn)
    });
    (endpoint, server)
}

#[test]
fn loopback_lane_header_sets_the_accepted_role_and_its_op_policy() {
    let call = |conn: &session::Conn, op: &str| {
        conn.call(
            op,
            json!({}),
            b"[]".to_vec(),
            session::Priority::Control,
            Duration::from_secs(10),
        )
    };
    let refused = |result: Result<session::Reply, session::CallError>| match result {
        Err(session::CallError::Remote(wire)) => wire.code,
        other => panic!("expected a refusal, got {other:?}"),
    };

    // A bulk lane serves object ops only.
    let (endpoint, server) = loopback_server();
    let bulk = session::dial_loopback_as(
        &endpoint,
        Some("t0k3n"),
        LaneRole::Bulk,
        ConnOptions::for_role(LaneRole::Bulk),
    )
    .unwrap();
    let accepted = server.join().unwrap().unwrap();
    assert_eq!(accepted.peer().role, LaneRole::Bulk);
    assert_eq!(bulk.peer().role, LaneRole::Bulk);
    assert_eq!(call(&bulk, "objects.fetch").unwrap().body, b"[]");
    assert_eq!(refused(call(&bulk, "hello")), "OP_NOT_ALLOWED");
    bulk.close();
    accepted.close();

    // A control lane (`dial_loopback`, as the CLI dials) refuses them.
    let (endpoint, server) = loopback_server();
    let control = session::dial_loopback(&endpoint, Some("t0k3n"), ConnOptions::default()).unwrap();
    let accepted = server.join().unwrap().unwrap();
    assert_eq!(accepted.peer().role, LaneRole::Control);
    assert_eq!(refused(call(&control, "objects.fetch")), "OP_NOT_ALLOWED");
    assert_eq!(call(&control, "hello").unwrap().body, b"[]");
    control.close();
    accepted.close();

    // No lane header (an older dialer) is a control lane.
    let (endpoint, server) = loopback_server();
    let mut raw = ws::connect_with(
        &endpoint,
        &[(UPGRADE_HEADER, UPGRADE_LOOPBACK), (TOKEN_HEADER, "t0k3n")],
        None,
        Duration::from_secs(5),
    )
    .unwrap()
    .0;
    let accepted = server.join().unwrap().unwrap();
    assert_eq!(accepted.peer().role, LaneRole::Control);
    accepted.close();
    let _ = raw.close(None);

    // An unknown lane is refused at the upgrade.
    for lane in ["probe", "objects"] {
        let (endpoint, server) = loopback_server();
        let error = ws::connect_with(
            &endpoint,
            &[
                (UPGRADE_HEADER, UPGRADE_LOOPBACK),
                (TOKEN_HEADER, "t0k3n"),
                (LANE_HEADER, lane),
            ],
            None,
            Duration::from_secs(5),
        );
        assert!(error.is_err(), "lane {lane} was upgraded");
        assert_eq!(
            server.join().unwrap().unwrap_err().code,
            "PROTOCOL_VIOLATION",
            "lane {lane}"
        );
    }
    // A dialer cannot ask for a probe loopback lane.
    let error = session::dial_loopback_as(
        "ws://127.0.0.1:9",
        Some("t0k3n"),
        LaneRole::Probe,
        ConnOptions::default(),
    )
    .unwrap_err();
    assert!(matches!(error, DialError::Protocol(_)), "{error:?}");
}
