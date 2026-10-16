//! WP4.1 — security hardening (§14 WP4.1): per-peer credentials, revocation,
//! local tokens.
//!
//! * local tokens: the control endpoint and a worker's JSON RPC and loopback
//!   sessions refuse an upgrade without their token; the CLI, the
//!   management client, `runtime::rpc` and the harness present it; the
//!   token files are owner-only (Unix);
//! * per-peer credentials: pairing enrolls one on both sides, and a session
//!   authenticates with it (`peer-hmac-v1`) without the code; a wrong key, an
//!   unknown dialer and a replayed handshake are refused;
//! * an unpair revokes the dialer's inbound access within 1 s;
//! * a rotation invalidates live sessions and every per-peer credential, and
//!   a re-pair enrolls a new one;
//! * peers paired before WP4.1 (no credential table) enroll on their next
//!   connection without user action;
//! * no pairing code, per-peer key or local token appears in status JSON or
//!   in the daemons' and workers' logs.
//!
//! In-process daemons ([`Daemon`]) where the test needs the inbound server's
//! sessions or the catalog's credentials; real `tkfs` processes (`Node`)
//! where workers and logs are involved.
mod support;

use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use support::*;
use tkfs::{
    model::{
        errors::code_of,
        orchestrator::Ctx,
        protocol::{
            LaneRole,
            frame::{AuthOk, ClientHello, ServerHello},
        },
        status::LaneState,
    },
    orchestrator::{
        self, Action, Config, Network,
        catalog::{Catalog, CredentialSide},
        ctx::DaemonNetwork,
        manage::manage,
    },
    session::{
        self, ConnOptions, DialError,
        auth::{PEER_HMAC_V1, PeerAuth, Transcript},
    },
    ws,
};
use tungstenite::{Message, client::IntoClientRequest};

// ---------------------------------------------------------------------------
// In-process daemons

/// A daemon run in this process (no folders, so no workers): its inbound
/// server's sessions and its catalog's credentials are read directly.
struct Daemon {
    root: tempfile::TempDir,
    config: Config,
    ctx: Arc<Ctx>,
    net: Arc<DaemonNetwork>,
    catalog: Arc<Catalog>,
    server: Option<JoinHandle<anyhow::Result<()>>>,
}

impl Daemon {
    fn start() -> Self {
        let root = tempfile::tempdir().unwrap();
        let config = Config {
            format_version: 2,
            data_directory: root.path().join("data"),
            default_mount_directory: root.path().join("mounts"),
            maximum_running: 8,
            network: Network {
                listen: Some("127.0.0.1:0".into()),
                connections: 2,
                sync_interval_ms: 200,
            },
        };
        Self::on(root, config)
    }

    fn on(root: tempfile::TempDir, config: Config) -> Self {
        let daemon = orchestrator::ctx::build(config.clone()).unwrap();
        let (ctx, net, catalog) = (
            daemon.ctx.clone(),
            daemon.net.clone(),
            daemon.catalog.clone(),
        );
        let server = thread::spawn(move || orchestrator::serve_with(daemon, false));
        let started = Self {
            root,
            config,
            ctx,
            net,
            catalog,
            server: Some(server),
        };
        assert!(
            wait_until(Duration::from_secs(10), || started.net.inbound().is_some()),
            "the inbound server did not start"
        );
        started
    }

    /// Stops this daemon and starts a new one on the same data directory,
    /// listening where it listened (its peers dial that endpoint).
    fn restart(mut self) -> Self {
        let endpoint = self.endpoint();
        self.stop();
        let root = std::mem::replace(&mut self.root, tempfile::tempdir().unwrap());
        let mut config = self.config.clone();
        config.network.listen = endpoint.strip_prefix("ws://").map(str::to_owned);
        Self::on(root, config)
    }

    fn stop(&mut self) {
        if let Some(server) = self.server.take() {
            orchestrator::shutdown::run(&self.ctx, Duration::from_secs(10));
            self.ctx.shutdown.cancel();
            let _ = server.join();
        }
    }

    fn installation(&self) -> String {
        self.ctx.installation.clone()
    }

    fn endpoint(&self) -> String {
        self.ctx.endpoints.load().network.clone().unwrap()
    }

    fn code(&self) -> String {
        manage(&self.ctx, Action::ResetPairingCode).unwrap()["code"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    fn pair(&self, other: &Daemon, code: &str) {
        self.ctx
            .network()
            .unwrap()
            .pair(&other.endpoint(), code, Some(2))
            .unwrap();
    }

    /// An active credential of `installation` on `side`.
    fn credential(&self, installation: &str, side: CredentialSide) -> Option<[u8; 32]> {
        self.catalog.credentials().active(installation, side)
    }

    /// Whether a credential of `installation` on `side` is stored but revoked.
    fn revoked(&self, installation: &str, side: CredentialSide) -> bool {
        self.catalog
            .credentials()
            .get(installation, side)
            .is_some_and(|credential| credential.revoked_ms.is_some())
    }

    /// Inbound sessions from `installation` that have not been sent away.
    fn inbound_from(&self, installation: &str) -> usize {
        self.net.inbound().map_or(0, |server| {
            server
                .sessions()
                .iter()
                .filter(|session| session.installation == installation)
                .count()
        })
    }

    /// Inbound sessions from `installation` that are registered and were not
    /// sent away (no `GOAWAY`).
    fn served_from(&self, installation: &str) -> usize {
        self.net.inbound().map_or(0, |server| {
            server
                .sessions()
                .iter()
                .filter(|session| session.installation == installation && session.goaway.is_none())
                .count()
        })
    }

    /// The states of our lanes to `installation`.
    fn lanes(&self, installation: &str) -> Vec<LaneState> {
        self.net
            .peers()
            .and_then(|peers| peers.peer(installation))
            .map(|peer| peer.lanes.iter().map(|lane| lane.state).collect())
            .unwrap_or_default()
    }

    fn lanes_ready(&self, installation: &str) -> bool {
        let lanes = self.lanes(installation);
        !lanes.is_empty() && lanes.iter().all(|state| *state == LaneState::Ready)
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.stop();
    }
}

/// A probe session to `server` as installation `as_installation`, with the
/// per-peer key `key` and no code to fall back to.
fn dial_per_peer(
    server: &Daemon,
    as_installation: &str,
    key: [u8; 32],
) -> Result<(session::Conn, ServerHello), DialError> {
    session::dial(
        &server.endpoint(),
        session::client_hello(as_installation, LaneRole::Probe, None, 2),
        &PeerAuth { key, code: None },
        None,
        ConnOptions::default(),
    )
}

/// `A` (dialer) paired with `B` (acceptor), the credential enrolled on both
/// sides and every lane of `A` Ready.
fn paired() -> (Daemon, Daemon) {
    let a = Daemon::start();
    let b = Daemon::start();
    let code = b.code();
    a.pair(&b, &code);
    wait_enrolled(&a, &b);
    assert!(
        wait_until(Duration::from_secs(20), || a.lanes_ready(&b.installation())),
        "A's lanes to B: {:?}",
        a.lanes(&b.installation())
    );
    (a, b)
}

/// Waits until `a` holds an outbound credential for `b` equal to `b`'s
/// active inbound credential for `a`; returns it.
fn wait_enrolled(a: &Daemon, b: &Daemon) -> [u8; 32] {
    let mut key = None;
    assert!(
        wait_until(Duration::from_secs(20), || {
            let outbound = a.credential(&b.installation(), CredentialSide::Outbound);
            let inbound = b.credential(&a.installation(), CredentialSide::Inbound);
            key = outbound.filter(|_| outbound == inbound);
            key.is_some()
        }),
        "no matching per-peer credential: A {:?}, B {:?}",
        a.catalog.credentials(),
        b.catalog.credentials()
    );
    key.unwrap()
}

// ---------------------------------------------------------------------------
// Local tokens

/// A raw WebSocket upgrade to `endpoint` with `headers` and no token
/// handling: the HTTP status the server answered (101 on success).
fn upgrade_status(endpoint: &str, headers: &[(&str, &str)]) -> u16 {
    let mut request = endpoint.into_client_request().unwrap();
    for (name, value) in headers {
        request.headers_mut().insert(
            tungstenite::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            value.parse().unwrap(),
        );
    }
    match tungstenite::connect(request) {
        Ok((mut socket, response)) => {
            let _ = socket.close(None);
            response.status().as_u16()
        }
        Err(tungstenite::Error::Http(response)) => response.status().as_u16(),
        Err(error) => panic!("upgrade to {endpoint} failed: {error}"),
    }
}

#[cfg(unix)]
fn assert_owner_only(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let mode = fs::metadata(path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600, "{} is {mode:o}", path.display());
}

#[cfg(not(unix))]
fn assert_owner_only(path: &Path) {
    // Windows: the file inherits its directory's ACL (the user's profile).
    assert!(path.is_file(), "{}", path.display());
}

/// §14 WP4.1 acceptance: an unauthenticated local connection to the control
/// endpoint is refused at the upgrade; the CLI, the management client and
/// the harness present `DATA/orchestrator.token`; the token is owner-only
/// and fresh at every daemon start.
#[test]
fn the_control_endpoint_requires_the_daemon_token() {
    let mut node = Node::new();
    let control = node.discovery().unwrap()["control"]
        .as_str()
        .unwrap()
        .to_owned();
    let token_path = node.data_dir().join("orchestrator.token");
    let token = fs::read_to_string(&token_path).unwrap();
    assert_eq!(token.trim().len(), 64, "32 random bytes, hex");
    assert_owner_only(&token_path);

    // No token, a wrong one: refused before any action is read.
    assert_eq!(upgrade_status(&control, &[]), 403);
    assert_eq!(upgrade_status(&control, &[("X-Tkfs-Token", "wrong")]), 403);
    let mut stolen = token.trim().to_owned();
    stolen.replace_range(..1, if stolen.starts_with('0') { "1" } else { "0" });
    assert_eq!(upgrade_status(&control, &[("X-Tkfs-Token", &stolen)]), 403);
    // This process knows no token for the daemon: `ws::connect` is refused.
    let error = ws::connect(&control)
        .err()
        .expect("refused without the token");
    assert_eq!(code_of(&error), "ACCESS_DENIED", "{error:#}");
    // The right token is accepted.
    assert_eq!(
        upgrade_status(&control, &[("X-Tkfs-Token", token.trim())]),
        101
    );

    // The management client, the CLI and the harness read the token file.
    let config = Config::load(&node.config_path()).unwrap();
    let listed = orchestrator::client(&config, &Action::List).unwrap();
    assert!(listed["folders"].is_array(), "{listed}");
    let output = Command::new(bin())
        .arg("-f")
        .arg(node.config_path())
        .args(["manage", "list"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    node.list();

    // A restarted daemon has a new token; the old one no longer opens it.
    node.restart();
    let control = node.discovery().unwrap()["control"]
        .as_str()
        .unwrap()
        .to_owned();
    let fresh = fs::read_to_string(&token_path).unwrap();
    assert_ne!(fresh.trim(), token.trim());
    assert_eq!(
        upgrade_status(&control, &[("X-Tkfs-Token", token.trim())]),
        403
    );
    node.list();
    orchestrator::client(&config, &Action::List).unwrap();
}

/// §14 WP4.1 acceptance: a worker's JSON RPC and loopback sessions refuse an
/// upgrade without the instance's token, which is in `worker.json`,
/// `STATE/runtime.token` (owner-only) and nowhere in `runtime.json`;
/// `runtime::rpc` and the CLI present it; a clean stop removes it.
#[test]
fn worker_endpoints_require_the_instance_token() {
    let node = Node::new();
    let folder = node.create("Tokens", true);
    let state = state_dir(&folder);
    // Read as JSON, not as a `Discovery`: nothing in this test process learns
    // the token by reading it (`runtime::Discovery` would note it).
    let runtime: Value =
        serde_json::from_slice(&fs::read(state.join("runtime.json")).unwrap()).unwrap();
    let address = runtime["address"].as_str().unwrap().to_owned();
    let token_path = state.join("runtime.token");
    let token = fs::read_to_string(&token_path).unwrap().trim().to_owned();
    assert_eq!(token.len(), 64);
    assert_owner_only(&token_path);
    assert!(
        !runtime.to_string().contains(&token),
        "runtime.json: {runtime}"
    );
    let record: Value =
        serde_json::from_slice(&fs::read(state.join("worker.json")).unwrap()).unwrap();
    assert_eq!(record["token"], token.as_str());
    // Every file that carries the token is owner-only: `worker.json`, and
    // `worker.log`, where the launcher copies the Ready line.
    assert_owner_only(&state.join("worker.json"));
    assert_owner_only(&state.join("worker.log"));

    // JSON RPC upgrades: no token, a wrong token, the right one.
    assert_eq!(upgrade_status(&address, &[]), 403);
    assert_eq!(upgrade_status(&address, &[("X-Tkfs-Token", "wrong")]), 403);
    assert_eq!(upgrade_status(&address, &[("X-Tkfs-Token", &token)]), 101);
    // Loopback session upgrades.
    let loopback = [("X-Tkfs-Sync", "3-loopback")];
    assert_eq!(upgrade_status(&address, &loopback), 403);
    let error =
        session::dial_loopback(&address, Some("wrong"), ConnOptions::default()).unwrap_err();
    assert!(
        matches!(&error, DialError::Unauthorized(wire) if wire.code == "ACCESS_DENIED"),
        "{error:?}"
    );
    let conn = session::dial_loopback(&address, Some(&token), ConnOptions::default()).unwrap();
    conn.close();

    // `runtime::rpc` (the harness's worker calls) and the CLI read the file.
    let status = rpc(&folder, json!({"op": "status"}));
    assert_eq!(status["repo"], folder["repo"], "{status}");
    let output = Command::new(bin())
        .arg("--runtime")
        .arg(state.join("runtime.json"))
        .arg("status")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    // A discovery that names another address never gets this worker's
    // token (the file is used only for the address `runtime.json` names).
    let elsewhere = tkfs::runtime::Discovery {
        format: 2,
        address: "ws://127.0.0.1:9".into(),
        state: state.to_string_lossy().into_owned(),
        mount: None,
        repo: String::new(),
        device: String::new(),
    };
    assert_eq!(tkfs::runtime::worker_token(&elsewhere), None);

    // A clean stop removes the token file with the discovery.
    node.stop_folder(&state_id(&folder));
    assert!(
        wait_until(Duration::from_secs(20), || !token_path.exists()),
        "runtime.token outlived the worker"
    );
}

// ---------------------------------------------------------------------------
// Per-peer credentials

/// Pairing enrolls a per-peer credential on both sides (§14 WP4.1 task 1),
/// and a session authenticates with it alone (`peer-hmac-v1`, no code); a
/// wrong key and a dialer without a credential are refused `PEER_REVOKED`.
#[test]
fn pairing_enrolls_a_per_peer_credential_that_replaces_the_code() {
    let (a, b) = paired();
    let key = wait_enrolled(&a, &b);

    let (conn, server) = dial_per_peer(&b, &a.installation(), key).unwrap();
    assert!(
        server
            .features
            .iter()
            .any(|feature| feature == PEER_HMAC_V1),
        "{:?}",
        server.features
    );
    let listed = tkfs::orchestrator::peer::list_catalog(&conn, None).unwrap();
    assert_eq!(listed.entries, vec![], "B publishes nothing");
    conn.close();

    let mut wrong = key;
    wrong[0] ^= 1;
    let error = dial_per_peer(&b, &a.installation(), wrong).unwrap_err();
    assert_eq!(error.to_wire().code, "PEER_REVOKED", "{error:?}");
    let error = dial_per_peer(&b, "a-dialer-that-never-enrolled", key).unwrap_err();
    assert_eq!(error.to_wire().code, "PEER_REVOKED", "{error:?}");
    // Neither refusal touched the dialer's credential.
    assert_eq!(
        b.credential(&a.installation(), CredentialSide::Inbound),
        Some(key)
    );
}

/// One pre-auth frame: kind, no flags, reserved zero, header length, header.
fn frame(kind: u8, header: &Value) -> Vec<u8> {
    let json = serde_json::to_vec(header).unwrap();
    let mut bytes = vec![kind, 0, 0, 0];
    bytes.extend_from_slice(&(json.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&json);
    bytes
}

fn read_frame(socket: &mut ws::RawSocket) -> (u8, Value) {
    loop {
        match socket.read().unwrap() {
            Message::Binary(bytes) => {
                let len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
                return (
                    bytes[0],
                    serde_json::from_slice(&bytes[8..8 + len]).unwrap(),
                );
            }
            Message::Ping(_) | Message::Pong(_) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
}

/// A v3 upgrade to `endpoint` and its `HELLO`; returns the socket and the
/// server's `HELLO_ACK`.
fn hello(endpoint: &str, hello: &ClientHello) -> (ws::RawSocket, ServerHello) {
    let (mut socket, _) = ws::connect_with(
        endpoint,
        &[("X-Tkfs-Sync", "3")],
        None,
        Duration::from_secs(5),
    )
    .unwrap();
    socket
        .send(Message::Binary(frame(1, &json!(hello)).into()))
        .unwrap();
    let (kind, header) = read_frame(&mut socket);
    assert_eq!(kind, 2, "HELLO_ACK expected: {header}");
    (socket, serde_json::from_value(header).unwrap())
}

/// §14 WP4.1 acceptance: a replayed per-peer handshake fails. The `AUTH` of
/// a successful handshake, sent again on a new connection with the very same
/// `HELLO` (same client nonce), meets a fresh server nonce and is refused.
#[test]
fn a_replayed_per_peer_handshake_fails() {
    let b = Daemon::start();
    let _code = b.code();
    let dialer = "replaying-dialer";
    let key = tkfs::session::auth::peer_key(&tkfs::session::auth::new_secret());
    b.catalog
        .enroll(dialer, CredentialSide::Inbound, key)
        .unwrap();
    let mut client = session::client_hello(dialer, LaneRole::Probe, None, 2);
    client.features = vec![PEER_HMAC_V1.to_owned()];

    let (mut socket, server) = hello(&b.endpoint(), &client);
    assert!(
        server
            .features
            .iter()
            .any(|feature| feature == PEER_HMAC_V1)
    );
    let transcript = Transcript::per_peer(&client, &server).unwrap();
    let auth = frame(
        3,
        &json!({ "proof": hex::encode(transcript.client_proof(&key)) }),
    );
    socket.send(Message::Binary(auth.clone().into())).unwrap();
    let (kind, header) = read_frame(&mut socket);
    assert_eq!(kind, 4, "AUTH_OK expected: {header}");
    let ok: AuthOk = serde_json::from_value(header).unwrap();
    assert_eq!(
        ok.server_proof,
        hex::encode(transcript.server_proof(&key)),
        "mutual authentication"
    );
    let _ = socket.close(None);

    let (mut replay, again) = hello(&b.endpoint(), &client);
    assert_ne!(again.server_nonce, server.server_nonce);
    replay.send(Message::Binary(auth.into())).unwrap();
    let (kind, header) = read_frame(&mut replay);
    assert_eq!(kind, 11, "GOAWAY expected: {header}");
    assert_eq!(header["code"], "PEER_REVOKED", "{header}");
}

/// A dialer holding a credential offers `peer-hmac-v1`; an acceptor that
/// does not know the scheme (a pre-WP4.1 v3 orchestrator) ignores it, and the
/// dialer authenticates with its pairing code in the same handshake.
#[test]
fn a_credential_falls_back_to_the_code_with_an_acceptor_without_the_scheme() {
    let (code, verifier) = tkfs::pairing_code::Verifier::generate();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let acceptor = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut socket = ws::accept_with(stream, 64 * 1024, |_, response| {
            response
                .headers_mut()
                .insert("X-Tkfs-Sync", "3".parse().unwrap());
        })
        .unwrap();
        let (kind, header) = read_frame(&mut socket);
        assert_eq!(kind, 1);
        let client: ClientHello = serde_json::from_value(header).unwrap();
        let offered = client.features.clone();
        let server = ServerHello {
            protocol: "tkfs-peer".into(),
            version: 3,
            installation: "older-acceptor".into(),
            role: client.role,
            lane: client.lane,
            salt: verifier.salt.clone(),
            server_nonce: tkfs::session::auth::new_nonce(),
            verifier_generation: 1,
            // An older acceptor announces no features.
            features: vec![],
            limits: tkfs::model::protocol::frame::Limits::default(),
        };
        socket
            .send(Message::Binary(frame(2, &json!(server)).into()))
            .unwrap();
        let (kind, header) = read_frame(&mut socket);
        assert_eq!(kind, 3);
        let transcript = Transcript::new(&client, &server).unwrap();
        let expected = hex::encode(transcript.client_proof(&verifier.key()));
        assert_eq!(header["proof"], expected.as_str(), "a code proof");
        let ok = hex::encode(transcript.server_proof(&verifier.key()));
        socket
            .send(Message::Binary(
                frame(4, &json!({ "server_proof": ok })).into(),
            ))
            .unwrap();
        thread::sleep(Duration::from_millis(200));
        offered
    });
    let auth = PeerAuth {
        key: tkfs::session::auth::peer_key(&[1; 32]),
        code: Some(code),
    };
    let (socket, server) = session::connect_v3(
        &endpoint,
        session::client_hello("dialer", LaneRole::Probe, None, 2),
        &auth,
        None,
        None,
    )
    .unwrap();
    drop(socket);
    assert!(server.features.is_empty());
    assert_eq!(acceptor.join().unwrap(), vec![PEER_HMAC_V1.to_owned()]);
}

/// The client hello of a per-peer handshake as `installation`.
fn per_peer_hello(installation: &str) -> ClientHello {
    let mut client = session::client_hello(installation, LaneRole::Probe, None, 2);
    client.features = vec![PEER_HMAC_V1.to_owned()];
    client
}

/// The `AUTH` frame of a per-peer handshake with `key`.
fn per_peer_auth(client: &ClientHello, server: &ServerHello, key: &[u8; 32]) -> Vec<u8> {
    let transcript = Transcript::per_peer(client, server).unwrap();
    frame(
        3,
        &json!({ "proof": hex::encode(transcript.client_proof(key)) }),
    )
}

/// Reads frames until one of `kind` (heartbeats and other frames skipped).
fn read_until(socket: &mut ws::RawSocket, kind: u8) -> Value {
    if let tungstenite::stream::MaybeTlsStream::Plain(stream) = socket.get_ref() {
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
    }
    loop {
        let (read, header) = read_frame(socket);
        if read == kind {
            return header;
        }
    }
}

/// A credential revoked, or replaced, between the server's `HELLO_ACK` and
/// the dialer's `AUTH` refuses the proof (`PEER_REVOKED`), although the
/// proof matches the key the handshake started with.
#[test]
fn a_credential_revoked_during_the_handshake_refuses_the_proof() {
    let b = Daemon::start();
    let _code = b.code();
    let dialer = "racing-dialer";
    let key = tkfs::session::auth::peer_key(&tkfs::session::auth::new_secret());
    let other = tkfs::session::auth::peer_key(&tkfs::session::auth::new_secret());
    for change in ["revoked", "replaced"] {
        b.catalog
            .enroll(dialer, CredentialSide::Inbound, key)
            .unwrap();
        let client = per_peer_hello(dialer);
        let (mut socket, server) = hello(&b.endpoint(), &client);
        assert!(server.features.iter().any(|f| f == PEER_HMAC_V1));
        match change {
            "revoked" => b.catalog.revoke(dialer, CredentialSide::Inbound).unwrap(),
            _ => b
                .catalog
                .enroll(dialer, CredentialSide::Inbound, other)
                .unwrap(),
        }
        socket
            .send(Message::Binary(
                per_peer_auth(&client, &server, &key).into(),
            ))
            .unwrap();
        let (kind, header) = read_frame(&mut socket);
        assert_eq!(kind, 11, "{change}: GOAWAY expected: {header}");
        assert_eq!(header["code"], "PEER_REVOKED", "{change}: {header}");
    }
    assert_eq!(b.served_from(dialer), 0);
}

/// A `peer-hmac-v1` handshake that read the dialer's credential before a
/// `peer.revoke` of that installation is sent away when it registers, even
/// when the same credential is active again by its `AUTH`: the inbound
/// server's per-installation revocation epoch moved meanwhile. A handshake
/// started after the revocation is served.
#[test]
fn a_per_peer_handshake_that_raced_a_revocation_is_sent_away() {
    let b = Daemon::start();
    let _code = b.code();
    let dialer = "revoked-dialer";
    let key = tkfs::session::auth::peer_key(&tkfs::session::auth::new_secret());
    b.catalog
        .enroll(dialer, CredentialSide::Inbound, key)
        .unwrap();
    let (conn, _) = dial_per_peer(&b, dialer, key).unwrap();
    // The racing handshake reads the credential (HELLO_ACK) ...
    let client = per_peer_hello(dialer);
    let (mut socket, server) = hello(&b.endpoint(), &client);
    // ... the dialer revokes itself on its other session ...
    let reply = conn
        .call(
            tkfs::session::auth::ops::PEER_REVOKE,
            json!({}),
            Vec::new(),
            session::Priority::Control,
            Duration::from_secs(5),
        )
        .unwrap();
    assert_eq!(reply.header["revoked"], true, "{:?}", reply.header);
    assert!(b.revoked(dialer, CredentialSide::Inbound));
    // ... and the same key is active again before the racing AUTH, so only
    // the revocation epoch tells that this handshake started before it.
    b.catalog
        .enroll(dialer, CredentialSide::Inbound, key)
        .unwrap();
    socket
        .send(Message::Binary(
            per_peer_auth(&client, &server, &key).into(),
        ))
        .unwrap();
    let (kind, header) = read_frame(&mut socket);
    assert_eq!(
        kind, 4,
        "AUTH_OK expected (the credential is active): {header}"
    );
    let goaway = read_until(&mut socket, 11);
    assert_eq!(goaway["code"], "PEER_REVOKED", "{goaway}");
    assert_eq!(
        b.served_from(dialer),
        0,
        "{:?}",
        b.net.inbound().unwrap().sessions()
    );
    let _ = socket.close(None);
    conn.close();

    let (fresh, _) = dial_per_peer(&b, dialer, key).unwrap();
    assert!(
        wait_until(Duration::from_secs(5), || b.served_from(dialer) == 1),
        "{:?}",
        b.net.inbound().unwrap().sessions()
    );
    fresh.close();
}

/// What a scripted impostor at a peer's address answers.
#[derive(Clone, Copy)]
enum Impostor {
    /// `HELLO_ACK` (echoing `peer-hmac-v1` when offered), then an `AUTH_OK`
    /// with a wrong server proof: it holds neither the key nor the code.
    WrongServerProof,
    /// `GOAWAY{PEER_REVOKED}` before `HELLO_ACK` to a hello offering the
    /// scheme, `GOAWAY{INVALID_PAIRING_CODE}` to a code hello.
    Refuses,
}

fn try_read_frame(socket: &mut ws::RawSocket) -> anyhow::Result<(u8, Value)> {
    loop {
        match socket.read()? {
            Message::Binary(bytes) if bytes.len() >= 8 => {
                let len = u32::from_le_bytes(bytes[4..8].try_into()?) as usize;
                let header = bytes
                    .get(8..8 + len)
                    .ok_or_else(|| anyhow::anyhow!("truncated frame"))?;
                return Ok((bytes[0], serde_json::from_slice(header)?));
            }
            Message::Ping(_) | Message::Pong(_) => {}
            other => anyhow::bail!("unexpected {other:?}"),
        }
    }
}

/// One connection to an impostor posing as `installation`; whether the
/// hello offered `peer-hmac-v1` is pushed to `offers` before it answers.
fn impersonate(
    stream: std::net::TcpStream,
    installation: &str,
    behaviour: Impostor,
    offers: &std::sync::Mutex<Vec<bool>>,
) -> anyhow::Result<()> {
    let mut socket = ws::accept_with(stream, 64 * 1024, |_, response| {
        response
            .headers_mut()
            .insert("X-Tkfs-Sync", "3".parse().unwrap());
    })?;
    let (kind, header) = try_read_frame(&mut socket)?;
    anyhow::ensure!(kind == 1, "HELLO expected");
    let client: ClientHello = serde_json::from_value(header)?;
    let offered = client.features.iter().any(|f| f == PEER_HMAC_V1);
    offers.lock().unwrap().push(offered);
    match behaviour {
        Impostor::Refuses => {
            let code = if offered {
                "PEER_REVOKED"
            } else {
                "INVALID_PAIRING_CODE"
            };
            socket.send(Message::Binary(
                frame(11, &json!({ "code": code, "message": "refused" })).into(),
            ))?;
        }
        Impostor::WrongServerProof => {
            let (_, verifier) = tkfs::pairing_code::Verifier::generate();
            let server = ServerHello {
                protocol: "tkfs-peer".into(),
                version: 3,
                installation: installation.to_owned(),
                role: client.role,
                lane: client.lane,
                salt: verifier.salt.clone(),
                server_nonce: tkfs::session::auth::new_nonce(),
                verifier_generation: 1,
                features: if offered {
                    vec![PEER_HMAC_V1.to_owned()]
                } else {
                    vec![]
                },
                limits: tkfs::model::protocol::frame::Limits::default(),
            };
            socket.send(Message::Binary(frame(2, &json!(server)).into()))?;
            let (kind, _) = try_read_frame(&mut socket)?;
            anyhow::ensure!(kind == 3, "AUTH expected");
            socket.send(Message::Binary(
                frame(4, &json!({ "server_proof": hex::encode([0u8; 32]) })).into(),
            ))?;
        }
    }
    thread::sleep(Duration::from_millis(200));
    Ok(())
}

/// A scripted impostor posing as `installation` at a fresh loopback
/// address: its endpoint, and per connection whether the hello offered
/// `peer-hmac-v1`.
fn impostor(installation: &str, behaviour: Impostor) -> (String, Arc<std::sync::Mutex<Vec<bool>>>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let offers = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (seen, installation) = (offers.clone(), installation.to_owned());
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let (seen, installation) = (seen.clone(), installation.clone());
            thread::spawn(move || {
                let _ = impersonate(stream, &installation, behaviour, &seen);
            });
        }
    });
    (endpoint, offers)
}

/// Pairs `daemon` with the impostor at `endpoint` by catalog writes: our
/// outbound credential `key` first, then the peer record.
fn pair_with_impostor(daemon: &Daemon, endpoint: &str, installation: &str, key: [u8; 32]) {
    use tkfs::model::orchestrator::{CatalogApi, CatalogCmd, PeerRecord};
    let (code, _) = tkfs::pairing_code::Verifier::generate();
    daemon
        .catalog
        .enroll(installation, CredentialSide::Outbound, key)
        .unwrap();
    daemon
        .catalog
        .apply(CatalogCmd::UpsertPeer {
            record: PeerRecord {
                endpoint: endpoint.to_owned(),
                code,
                installation: installation.to_owned(),
                connections: 2,
                revision: "impostor".into(),
            },
        })
        .unwrap();
}

/// A refusal at the peer's address is not authenticated, so it never makes
/// the dialer delete its per-peer credential (an impostor, a reused address
/// or port):
///
/// * an acceptor that echoes `peer-hmac-v1` but fails the server proof is
///   `INVALID_PAIRING_CODE` (`Unauthorized`), and the lanes keep presenting
///   the key on every later dial;
/// * a `GOAWAY{PEER_REVOKED}` before `HELLO_ACK` makes the supervisor fall
///   back to the code (in memory), and the stored credential stays until a
///   code-authenticated session replaces it.
#[test]
fn an_unauthenticated_refusal_never_drops_the_credential() {
    let a = Daemon::start();
    let failing = "impostor-failing-its-proof";
    let refusing = "impostor-refusing-the-key";
    let failing_key = tkfs::session::auth::peer_key(&tkfs::session::auth::new_secret());
    let refusing_key = tkfs::session::auth::peer_key(&tkfs::session::auth::new_secret());
    let (failing_endpoint, failing_offers) = impostor(failing, Impostor::WrongServerProof);
    let (refusing_endpoint, refusing_offers) = impostor(refusing, Impostor::Refuses);

    // The session layer: the impostor's wrong server proof.
    let error = session::dial(
        &failing_endpoint,
        session::client_hello(&a.installation(), LaneRole::Probe, None, 2),
        &PeerAuth {
            key: failing_key,
            code: None,
        },
        None,
        ConnOptions::default(),
    )
    .unwrap_err();
    assert!(
        matches!(&error, DialError::Unauthorized(wire) if wire.code == "INVALID_PAIRING_CODE"),
        "{error:?}"
    );

    // The lanes.
    pair_with_impostor(&a, &failing_endpoint, failing, failing_key);
    pair_with_impostor(&a, &refusing_endpoint, refusing, refusing_key);
    for installation in [failing, refusing] {
        assert!(
            wait_until(Duration::from_secs(20), || a
                .lanes(installation)
                .contains(&LaneState::Unauthorized)),
            "{installation}: {:?}",
            a.lanes(installation)
        );
    }
    let view = a.net.peers().unwrap().peer(failing).unwrap();
    assert!(
        view.lanes.iter().any(|lane| lane
            .error
            .as_ref()
            .is_some_and(|error| error.code == "INVALID_PAIRING_CODE")),
        "{:?}",
        view.lanes
    );
    // The refusing impostor saw the key offered first, then the code.
    assert!(
        wait_until(Duration::from_secs(5), || refusing_offers
            .lock()
            .unwrap()
            .contains(&false)),
        "{:?}",
        refusing_offers.lock().unwrap()
    );
    assert_eq!(refusing_offers.lock().unwrap().first(), Some(&true));

    // A recheck re-dials: the key failing its impostor is still presented.
    let dialed = failing_offers.lock().unwrap().len();
    a.ctx.network().unwrap().reconfigure();
    assert!(
        wait_until(Duration::from_secs(10), || failing_offers
            .lock()
            .unwrap()
            .len()
            > dialed),
        "no re-dial after the recheck"
    );
    let offers = failing_offers.lock().unwrap().clone();
    assert!(offers.iter().all(|offered| *offered), "{offers:?}");

    // Neither refusal deleted a credential (a deletion would run off the
    // lane thread: give it time).
    thread::sleep(Duration::from_millis(500));
    assert_eq!(
        a.credential(failing, CredentialSide::Outbound),
        Some(failing_key)
    );
    assert_eq!(
        a.credential(refusing, CredentialSide::Outbound),
        Some(refusing_key)
    );
}

/// An acceptor that lost the dialer's credential (a restored database)
/// refuses it; the dialer falls back to its still valid code, and lane 0's
/// code-authenticated session enrolls a new credential that replaces the
/// refused one on both sides. No user action.
#[test]
fn a_lost_acceptor_credential_is_replaced_over_the_code() {
    let (a, b) = paired();
    let key = wait_enrolled(&a, &b);
    b.catalog
        .forget(&a.installation(), CredentialSide::Inbound)
        .unwrap();
    // A's sessions drop with the restart; its lanes re-dial with the key.
    let b = b.restart();
    let mut fresh = None;
    assert!(
        wait_until(Duration::from_secs(30), || {
            let outbound = a.credential(&b.installation(), CredentialSide::Outbound);
            let inbound = b.credential(&a.installation(), CredentialSide::Inbound);
            fresh = outbound.filter(|k| *k != key && inbound == Some(*k));
            fresh.is_some()
        }),
        "A {:?}, B {:?}",
        a.catalog.credentials(),
        b.catalog.credentials()
    );
    assert!(
        wait_until(Duration::from_secs(20), || a.lanes_ready(&b.installation())),
        "{:?}",
        a.lanes(&b.installation())
    );
    let (conn, _) = dial_per_peer(&b, &a.installation(), fresh.unwrap()).unwrap();
    conn.close();
}

/// The catalog's credential rules (§9, WP4.1): enroll, revoke and forget per
/// installation and side; a rotation revokes every inbound credential and
/// leaves ours alone; an unpair forgets our outbound credential once no
/// record names the installation; everything survives a reopen;
/// `folders.sqlite` is owner-only (Unix); `Catalog::of` finds the catalog
/// behind its `dyn CatalogApi`.
#[test]
fn the_catalog_keeps_credentials_per_installation_and_side() {
    use tkfs::model::orchestrator::{CatalogApi, CatalogCmd, PeerRecord};
    let root = tempfile::tempdir().unwrap();
    let config = Config {
        format_version: 2,
        data_directory: root.path().join("data"),
        default_mount_directory: root.path().join("mounts"),
        maximum_running: 8,
        network: Network::default(),
    };
    let catalog = Catalog::open(&config).unwrap();
    let api: Arc<dyn CatalogApi> = catalog.clone();
    assert!(Arc::ptr_eq(&Catalog::of(&api).unwrap(), &catalog));
    let (inbound, outbound) = ([1u8; 32], [2u8; 32]);
    catalog
        .enroll("peer", CredentialSide::Inbound, inbound)
        .unwrap();
    catalog
        .enroll("peer", CredentialSide::Outbound, outbound)
        .unwrap();
    let credentials = catalog.credentials();
    assert_eq!(
        credentials.active("peer", CredentialSide::Inbound),
        Some(inbound)
    );
    assert_eq!(
        credentials.active("peer", CredentialSide::Outbound),
        Some(outbound)
    );
    assert!(!format!("{credentials:?}").contains(&hex::encode(inbound)));
    let own = catalog.snapshot().installation.clone();
    assert_eq!(
        catalog
            .enroll(&own, CredentialSide::Inbound, inbound)
            .unwrap_err()
            .code,
        "CANNOT_PAIR_WITH_SELF"
    );

    // A rotation revokes the inbound credential only.
    let (_, verifier) = tkfs::pairing_code::Verifier::generate();
    catalog.apply(CatalogCmd::SetVerifier { verifier }).unwrap();
    let credentials = catalog.credentials();
    assert_eq!(credentials.active("peer", CredentialSide::Inbound), None);
    assert!(
        credentials
            .get("peer", CredentialSide::Inbound)
            .unwrap()
            .revoked_ms
            .is_some()
    );
    assert_eq!(
        credentials.active("peer", CredentialSide::Outbound),
        Some(outbound)
    );
    // Enrolling again replaces (and un-revokes) it; revoke and forget.
    catalog
        .enroll("peer", CredentialSide::Inbound, [3; 32])
        .unwrap();
    assert_eq!(
        catalog
            .credentials()
            .active("peer", CredentialSide::Inbound),
        Some([3; 32])
    );
    catalog.revoke("peer", CredentialSide::Inbound).unwrap();
    assert_eq!(
        catalog
            .credentials()
            .active("peer", CredentialSide::Inbound),
        None
    );

    // Unpair: the outbound credential goes with the last record of the
    // installation, not before.
    let record = |endpoint: &str| PeerRecord {
        endpoint: endpoint.into(),
        code: "c".into(),
        installation: "peer".into(),
        connections: 2,
        revision: String::new(),
    };
    for endpoint in ["ws://a:1", "ws://b:2"] {
        catalog
            .apply(CatalogCmd::UpsertPeer {
                record: record(endpoint),
            })
            .unwrap();
    }
    catalog
        .apply(CatalogCmd::RemovePeer {
            endpoint: "ws://a:1".into(),
        })
        .unwrap();
    assert_eq!(
        catalog
            .credentials()
            .active("peer", CredentialSide::Outbound),
        Some(outbound)
    );
    catalog
        .apply(CatalogCmd::RemovePeer {
            endpoint: "ws://b:2".into(),
        })
        .unwrap();
    assert!(
        catalog
            .credentials()
            .get("peer", CredentialSide::Outbound)
            .is_none()
    );
    // A lane-0 enrollment is stored only while a record names the peer
    // (checked in the catalog's transaction): one that raced an unpair
    // leaves nothing behind.
    assert_eq!(
        catalog
            .enroll_paired("peer", CredentialSide::Outbound, outbound)
            .unwrap_err()
            .code,
        "PEER_NOT_PAIRED"
    );
    assert!(
        catalog
            .credentials()
            .get("peer", CredentialSide::Outbound)
            .is_none()
    );
    catalog
        .apply(CatalogCmd::UpsertPeer {
            record: record("ws://c:3"),
        })
        .unwrap();
    catalog
        .enroll_paired("peer", CredentialSide::Outbound, outbound)
        .unwrap();
    assert_eq!(
        catalog
            .credentials()
            .active("peer", CredentialSide::Outbound),
        Some(outbound)
    );
    catalog
        .apply(CatalogCmd::RemovePeer {
            endpoint: "ws://c:3".into(),
        })
        .unwrap();
    catalog
        .enroll("other", CredentialSide::Outbound, outbound)
        .unwrap();
    catalog.forget("other", CredentialSide::Outbound).unwrap();
    assert!(
        catalog
            .credentials()
            .get("other", CredentialSide::Outbound)
            .is_none()
    );

    // A reopen reads the same rows; the older binaries' schema is untouched.
    let before = catalog.credentials();
    assert!(catalog.close());
    drop(catalog);
    let catalog = Catalog::open(&config).unwrap();
    assert_eq!(catalog.credentials(), before);
    #[cfg(unix)]
    assert_owner_only(&config.data_directory.join("folders.sqlite"));
}

/// §14 WP4.1 task 2 and acceptance: an unpair sends `peer.revoke` first; the
/// acceptor revokes the dialer's credential and closes every session of the
/// dialer within 1 s, and the old key is refused afterwards. The dialer's
/// own credential goes with the peer record.
#[test]
fn unpair_revokes_inbound_access_within_one_second() {
    let (a, b) = paired();
    let key = wait_enrolled(&a, &b);
    assert!(
        wait_until(Duration::from_secs(10), || b
            .inbound_from(&a.installation())
            >= 2),
        "B serves A's lanes"
    );
    let endpoint = b.endpoint();
    let started = Instant::now();
    let unpair = {
        let network = a.ctx.network().unwrap().clone();
        thread::spawn(move || network.unpair(&endpoint))
    };
    let mut revoked_at = None;
    let mut closed_at = None;
    while started.elapsed() < Duration::from_secs(3)
        && (revoked_at.is_none() || closed_at.is_none())
    {
        if revoked_at.is_none() && b.revoked(&a.installation(), CredentialSide::Inbound) {
            revoked_at = Some(started.elapsed());
        }
        if closed_at.is_none() && b.inbound_from(&a.installation()) == 0 {
            closed_at = Some(started.elapsed());
        }
        thread::sleep(Duration::from_millis(5));
    }
    unpair.join().unwrap().unwrap();
    let revoked_at = revoked_at.expect("B never revoked A's credential");
    let closed_at = closed_at.expect("A's sessions at B never closed");
    assert!(
        revoked_at < Duration::from_secs(1),
        "revoked after {revoked_at:?}"
    );
    assert!(
        closed_at < Duration::from_secs(1),
        "sessions closed after {closed_at:?}"
    );
    let error = dial_per_peer(&b, &a.installation(), key).unwrap_err();
    assert_eq!(error.to_wire().code, "PEER_REVOKED", "{error:?}");
    assert_eq!(
        a.catalog
            .credentials()
            .get(&b.installation(), CredentialSide::Outbound),
        None,
        "the dialer forgot its credential with the peer"
    );
    // Pairing again with the code is still possible, and enrolls anew.
    let code = b.code();
    a.pair(&b, &code);
    let fresh = wait_enrolled(&a, &b);
    assert_ne!(fresh, key);
}

/// §14 WP4.1 task 3 and acceptance: `ResetPairingCode` revokes every
/// per-peer credential and sends every session away (invariant 21); the
/// dialer turns `Unauthorized` (its code is stale) and a re-pair enrolls a
/// new credential.
#[test]
fn rotation_invalidates_live_sessions_and_per_peer_credentials() {
    let (a, b) = paired();
    let key = wait_enrolled(&a, &b);
    assert!(wait_until(Duration::from_secs(10), || b
        .inbound_from(&a.installation())
        >= 2));
    let started = Instant::now();
    let code = b.code();
    assert!(b.revoked(&a.installation(), CredentialSide::Inbound));
    assert!(
        wait_until(Duration::from_secs(1), || b.inbound_from(&a.installation())
            == 0),
        "sessions outlived the rotation by {:?}",
        started.elapsed()
    );
    let error = dial_per_peer(&b, &a.installation(), key).unwrap_err();
    assert_eq!(error.to_wire().code, "PEER_REVOKED", "{error:?}");
    // A stops presenting the refused credential and falls back to its
    // (stale) code. The refusal is not authenticated, so A keeps the stored
    // credential until a code-authenticated session replaces it.
    assert!(
        wait_until(Duration::from_secs(20), || a
            .lanes(&b.installation())
            .contains(&LaneState::Unauthorized)),
        "{:?}",
        a.lanes(&b.installation())
    );
    assert_eq!(
        a.credential(&b.installation(), CredentialSide::Outbound),
        Some(key)
    );
    // A re-pair with the new code enrolls a new credential, which replaces
    // the refused one.
    a.pair(&b, &code);
    let fresh = wait_enrolled(&a, &b);
    assert_ne!(fresh, key);
    assert!(wait_until(Duration::from_secs(20), || a.lanes_ready(&b.installation())));
}

/// The upgrade path (§14 WP4.1, brief): daemons paired before WP4.1 have no
/// `peer_credentials` table. On their next start the table is created and
/// the dialer's control lane, authenticated with the code, enrolls a
/// credential without any user action.
#[test]
fn code_paired_peers_enroll_on_their_next_connection() {
    let (a, b) = paired();
    let (a_installation, b_installation) = (a.installation(), b.installation());
    let a_data = a.config.data_directory.clone();
    let b_data = b.config.data_directory.clone();
    let (mut a, mut b) = (a, b);
    a.stop();
    b.stop();
    for data in [&a_data, &b_data] {
        let db = rusqlite::Connection::open(data.join("folders.sqlite")).unwrap();
        db.execute_batch("DROP TABLE peer_credentials").unwrap();
    }
    let b = b.restart();
    let a = a.restart();
    assert_eq!(a.installation(), a_installation);
    assert_eq!(b.installation(), b_installation);
    let key = wait_enrolled(&a, &b);
    let (conn, _) = dial_per_peer(&b, &a_installation, key).unwrap();
    conn.close();
    assert!(wait_until(Duration::from_secs(20), || a.lanes_ready(&b_installation)));
}

// ---------------------------------------------------------------------------
// No secrets in status or logs

/// Every file under `dir` whose name ends in `.log` (worker logs, rotated
/// logs), recursively.
fn log_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            log_files(&path, out);
        } else if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.contains(".log"))
        {
            out.push(path);
        }
    }
}

/// The hex keys of every `peer_credentials` row of a data directory.
fn stored_keys(data: &Path) -> Vec<String> {
    let db = rusqlite::Connection::open_with_flags(
        data.join("folders.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let mut statement = db
        .prepare("SELECT secret_hash FROM peer_credentials")
        .unwrap();
    statement
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

/// §14 WP4.1 acceptance: a grep finds no pairing code, per-peer key or local
/// token in status JSON or in the log files, after pairing, enrollment,
/// sync, an unpair and a rotation between two real daemons.
#[test]
fn no_pairing_code_secret_or_token_in_status_or_logs() {
    let a = Node::new();
    let b = Node::new();
    let mut needles: Vec<(String, &str)> = Vec::new();
    let code = b.code();
    needles.push((code.clone(), "pairing code"));
    a.pair(&b, &code);
    let source = b.create("Secrets", true);
    write_file(&source, "hello.txt", b"no secrets here");
    let received = a.receive(&b, &source);
    a.wait_for("the file to sync", Duration::from_secs(60), || {
        has_file(&received, "hello.txt", b"no secrets here")
    });
    a.wait_for("the credential", Duration::from_secs(20), || {
        !stored_keys(&a.data_dir()).is_empty() && !stored_keys(&b.data_dir()).is_empty()
    });
    for data in [a.data_dir(), b.data_dir()] {
        for key in stored_keys(&data) {
            needles.push((key, "per-peer key"));
        }
        needles.push((
            fs::read_to_string(data.join("orchestrator.token"))
                .unwrap()
                .trim()
                .to_owned(),
            "control token",
        ));
    }
    let mut worker_tokens = Vec::new();
    for folder in [&source, &received] {
        let token = fs::read_to_string(state_dir(folder).join("runtime.token")).unwrap();
        worker_tokens.push(token.trim().to_owned());
    }

    // Status JSON of both daemons, then an unpair and a rotation, then the
    // status again.
    let mut status = Vec::new();
    let snapshot = |node: &Node, folder: &Value, status: &mut Vec<Value>| {
        for op in ["status", "list", "peers"] {
            status.push(node.call(json!({ "op": op })));
        }
        status.push(node.inspect(&state_id(folder)));
    };
    snapshot(&a, &received, &mut status);
    snapshot(&b, &source, &mut status);
    a.call(json!({"op": "unpair", "endpoint": b.endpoint()}));
    let rotated = b.code();
    needles.push((rotated, "pairing code"));
    thread::sleep(Duration::from_secs(2));
    status.push(a.call(json!({"op": "status"})));
    status.push(b.call(json!({"op": "status"})));
    let status = serde_json::to_string(&status).unwrap();

    let mut logs = vec![a.log_path(), b.log_path()];
    log_files(&a.data_dir(), &mut logs);
    log_files(&b.data_dir(), &mut logs);
    assert!(logs.len() >= 4, "daemon and worker logs: {logs:?}");
    let mut found = Vec::new();
    for (needle, what) in &needles {
        assert!(needle.len() >= 32, "{what}");
        if status.contains(needle.as_str()) {
            found.push(format!("{what} in status JSON"));
        }
        for log in &logs {
            if fs::read_to_string(log)
                .unwrap_or_default()
                .contains(needle.as_str())
            {
                found.push(format!("{what} in {}", log.display()));
            }
        }
    }
    for token in &worker_tokens {
        if status.contains(token.as_str()) {
            found.push("worker token in status JSON".into());
        }
        for log in &logs {
            let text = fs::read_to_string(log).unwrap_or_default();
            // CCR-WP4.1-4: the launcher copies the worker's Ready line (which
            // carries the token for its supervisor, §14 WP4.1) verbatim into
            // `worker.log`; every other line must not carry it.
            let leaked = text
                .lines()
                .filter(|line| !line.starts_with(r#"{"ready":true"#))
                .any(|line| line.contains(token.as_str()));
            if leaked {
                found.push(format!("worker token in {}", log.display()));
            }
        }
    }
    assert!(found.is_empty(), "secrets found: {found:#?}");
}
