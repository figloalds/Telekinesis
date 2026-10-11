//! Code-authenticated, automatic public identity exchange. This dedicated TLS
//! profile defers certificate identity authentication to OPAQUE plus the TLS
//! exporter. It cannot be used for sync or to transmit a bearer pairing code.
use crate::{
    pairing::{Identity, Peer},
    pairing_service::{BudgetSocket, Config},
    wss_transport::{self, TimedStream},
};
use anyhow::{Context, Result, ensure};
use opaque_ke::{
    ClientLogin, ClientLoginFinishParameters, ClientRegistration,
    ClientRegistrationFinishParameters, CredentialFinalization, CredentialRequest,
    CredentialResponse, ServerLogin, ServerLoginParameters, ServerRegistration, ServerSetup,
};
use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, ServerName, UnixTime},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use zeroize::{Zeroize, Zeroizing};

const MAXIMUM: usize = 32 * 1024;
const SUITE: &str = "OPAQUE-3DH-Ristretto255-SHA512-Argon2id-m19456-t2-p1-v1";
const PRINCIPAL: &[u8] = b"tkfs-enrollment-capability-v3";
const DEADLINE: Duration = Duration::from_secs(15);
#[cfg(test)]
type Pause = (std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>);
#[cfg(test)]
static BEFORE_FINISH: std::sync::OnceLock<
    std::sync::Mutex<std::collections::BTreeMap<u16, Pause>>,
> = std::sync::OnceLock::new();
#[cfg(test)]
static DROP_FINAL_ACK: std::sync::OnceLock<
    std::sync::Mutex<std::collections::BTreeSet<std::path::PathBuf>>,
> = std::sync::OnceLock::new();
pub(crate) struct Suite;
impl opaque_ke::CipherSuite for Suite {
    type OprfCs = opaque_ke::Ristretto255;
    type KeyExchange = opaque_ke::TripleDh<opaque_ke::Ristretto255, sha2::Sha512>;
    type Ksf = opaque_ke::argon2::Argon2<'static>;
}
fn ksf() -> opaque_ke::argon2::Argon2<'static> {
    use opaque_ke::argon2::{Algorithm, Argon2, Params, Version};
    Argon2::new(
        Algorithm::Argon2id,
        Version::V0x13,
        Params::new(19456, 2, 1, None).expect("fixed KSF parameters"),
    )
}
/// Registration runs entirely on the owner host during an explicit reset.
pub(crate) fn registration(code: &str) -> Result<(Zeroizing<Vec<u8>>, Zeroizing<Vec<u8>>)> {
    let mut rng = rand::rngs::OsRng;
    let setup = ServerSetup::<Suite>::new(&mut rng);
    let client = ClientRegistration::<Suite>::start(&mut rng, code.as_bytes())?;
    let server = ServerRegistration::<Suite>::start(&setup, client.message, PRINCIPAL)?;
    let mut upload = client.state.finish(
        &mut rng,
        code.as_bytes(),
        server.message,
        ClientRegistrationFinishParameters {
            ksf: Some(&ksf()),
            ..Default::default()
        },
    )?;
    upload.export_key.zeroize();
    let record = ServerRegistration::<Suite>::finish(upload.message);
    Ok((
        Zeroizing::new(setup.serialize().to_vec()),
        Zeroizing::new(record.serialize().to_vec()),
    ))
}
pub(crate) fn validate_registration(setup: &[u8], record: &[u8]) -> Result<()> {
    let _setup = ServerSetup::<Suite>::deserialize(setup)?;
    let _record = ServerRegistration::<Suite>::deserialize(record)?;
    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    protocol_version: u32,
    suite: String,
    generation: u64,
    orchestrator: String,
    issuer: Peer,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Start {
    protocol_version: u32,
    commitment: String,
    ke1: Vec<u8>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Challenge {
    metadata: Metadata,
    ke2: Vec<u8>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Finish {
    ke3: Vec<u8>,
    nonce: [u8; 32],
    candidate: Peer,
}
fn commitment(candidate: &Peer, nonce: &[u8; 32]) -> Result<String> {
    Ok(crate::core::hash(&serde_json::to_vec(&(
        "tkfs-candidate-v3",
        nonce,
        candidate,
    ))?))
}
fn context(
    metadata: &Metadata,
    exporter: &[u8; 32],
    leaf: &[u8],
    commitment: &str,
) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&(
        "tkfs-enrollment-v3",
        metadata,
        exporter,
        crate::core::hash(leaf),
        commitment,
    ))?)
}
fn cancelled(stop: &AtomicBool) -> Result<()> {
    ensure!(!stop.load(Ordering::Relaxed), "PAIRING_CANCELLED");
    Ok(())
}
fn rejected<T>(result: Result<T>) -> Result<T> {
    result.map_err(|_| anyhow::anyhow!("PAIRING_CODE_REJECTED"))
}

/// Only this module can construct the provisional verifier. TLS proves possession
/// of the leaf key; OPAQUE binds its identity before any candidate/trust is used.
#[derive(Debug)]
struct ProvisionalEnrollment;
impl ServerCertVerifier for ProvisionalEnrollment {
    fn verify_server_cert(
        &self,
        end: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        if end.len() > 8192 || !intermediates.is_empty() {
            return Err(rustls::Error::General(
                "INVALID_ENROLLMENT_CERTIFICATE".into(),
            ));
        }
        let mut roots = rustls::RootCertStore::empty();
        roots.add(end.clone()).map_err(|_| {
            rustls::Error::InvalidCertificate(rustls::CertificateError::BadEncoding)
        })?;
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::General("TLS13_REQUIRED".into()))
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            signature,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}
fn provisional_tls() -> Result<Arc<rustls::ClientConfig>> {
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])?
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(ProvisionalEnrollment))
    .with_no_client_auth();
    config.enable_early_data = false;
    config.resumption = rustls::client::Resumption::disabled();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}
fn validate_metadata(metadata: &Metadata, leaf: &[u8]) -> Result<()> {
    ensure!(
        metadata.protocol_version == 3 && metadata.suite == SUITE && metadata.generation > 0,
        "PAIRING_CODE_REJECTED"
    );
    uuid::Uuid::parse_str(&metadata.orchestrator)?;
    uuid::Uuid::parse_str(&metadata.issuer.installation)?;
    ensure!(
        !metadata.issuer.revoked && metadata.issuer.certificate == leaf,
        "PAIRING_CODE_REJECTED"
    );
    wss_transport::endpoint(&metadata.issuer.endpoint, false)?;
    ensure!(
        metadata.issuer.endpoint.starts_with("wss://"),
        "PAIRING_CODE_REJECTED"
    );
    // Validate certificate syntax, lifetime and claimed UUID name as well as the
    // already verified TLS CertificateVerify; this still grants no identity trust.
    let mut roots = rustls::RootCertStore::empty();
    roots.add(leaf.to_vec().into())?;
    let verifier = rustls::client::WebPkiServerVerifier::builder(Arc::new(roots)).build()?;
    verifier.verify_server_cert(
        &leaf.to_vec().into(),
        &[],
        &ServerName::try_from(format!("{}.tkfs.invalid", metadata.issuer.installation))?,
        &[],
        UnixTime::now(),
    )?;
    Ok(())
}

pub(crate) fn join(
    config: &Config,
    host: &str,
    port: u16,
    code: Zeroizing<String>,
    stop: Arc<AtomicBool>,
) -> Result<Value> {
    crate::pairing_code::validate(&code)?;
    let address = SocketAddr::new(
        host.parse::<IpAddr>()
            .context("PAIRING_NUMERIC_IP_REQUIRED")?,
        port,
    );
    ensure!(port != 0, "PAIRING_PORT_REQUIRED");
    cancelled(&stop)?;
    let identity = config.identity()?;
    let result = join_identity(config, &identity, address, code, stop.clone());
    if stop.load(Ordering::Relaxed) {
        anyhow::bail!("PAIRING_CANCELLED");
    }
    result.map_err(|error| match error.to_string().as_str() {
        "PAIRING_PROTOCOL_UPGRADE_REQUIRED"
        | "PAIRING_CODE_REJECTED"
        | "PAIR_KEY_CHANGED: explicit rotation required"
        | "PAIR_REVOKED" => error,
        _ => anyhow::anyhow!("PAIRING_UNAVAILABLE"),
    })
}
fn join_identity(
    config: &Config,
    identity: &Identity,
    address: SocketAddr,
    mut code: Zeroizing<String>,
    stop: Arc<AtomicBool>,
) -> Result<Value> {
    let (mut socket, leaf, exporter) = channel(address, stop.clone())?;
    enroll_authenticated(
        config,
        identity,
        address,
        &mut code,
        &stop,
        &mut socket,
        &leaf,
        &exporter,
    )
}
type ProvisionalChannel =
    tungstenite::WebSocket<rustls::StreamOwned<rustls::ClientConnection, crate::ws_tunnel::Socket>>;
fn channel(
    address: SocketAddr,
    stop: Arc<AtomicBool>,
) -> Result<(ProvisionalChannel, Vec<u8>, [u8; 32])> {
    let mut conn = rustls::ClientConnection::new(
        provisional_tls()?,
        ServerName::try_from("enrollment.tkfs.invalid")?,
    )?;
    let raw = std::net::TcpStream::connect_timeout(&address, Duration::from_secs(5))?;
    let mut socket = crate::ws_tunnel::Socket::Direct(BudgetSocket::new(
        raw,
        Duration::from_secs(5),
        stop.clone(),
    )?);
    while conn.is_handshaking() {
        conn.complete_io(&mut socket)?;
    }
    ensure!(
        conn.alpn_protocol() == Some(b"http/1.1"),
        "PAIRING_UNAVAILABLE"
    );
    let leaf = conn
        .peer_certificates()
        .and_then(|chain| chain.first())
        .context("PAIRING_CODE_REJECTED")?
        .as_ref()
        .to_vec();
    let exporter =
        conn.export_keying_material([0u8; 32], b"EXPORTER-Channel-Binding", Some(&[]))?;
    let request = tungstenite::http::Request::builder()
        .method("GET")
        .uri(format!("wss://{address}/tkfs/enroll-v3"))
        .header("Host", address.to_string())
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header(
            "Sec-WebSocket-Key",
            tungstenite::handshake::client::generate_key(),
        )
        .header("Sec-WebSocket-Protocol", "tkfs-wss-v1")
        .body(())?;
    let (mut socket, response) = tungstenite::client::client_with_config(
        request,
        rustls::StreamOwned::new(conn, socket),
        Some(wss_transport::websocket_config()),
    )
    .map_err(|_| anyhow::anyhow!("PAIRING_UNAVAILABLE"))?;
    ensure!(
        response
            .headers()
            .get("Sec-WebSocket-Protocol")
            .is_some_and(|v| v == "tkfs-wss-v1"),
        "PAIRING_UNAVAILABLE"
    );
    socket.get_mut().budget(DEADLINE);
    socket.get_mut().sock.limit(128 * 1024);
    Ok((socket, leaf, exporter))
}
fn enroll_authenticated(
    config: &Config,
    identity: &Identity,
    _address: SocketAddr,
    code: &mut Zeroizing<String>,
    stop: &AtomicBool,
    socket: &mut ProvisionalChannel,
    leaf: &[u8],
    exporter: &[u8; 32],
) -> Result<Value> {
    let mut rng = rand::rngs::OsRng;
    let candidate = Peer {
        installation: identity.installation.clone(),
        certificate: identity.certificate.clone(),
        endpoint: config.advertise.clone(),
        revoked: false,
    };
    use rand::Rng;
    let nonce: [u8; 32] = rng.r#gen();
    let commitment = commitment(&candidate, &nonce)?;
    let client = ClientLogin::<Suite>::start(&mut rng, code.as_bytes())?;
    wss_transport::send(
        socket,
        &Start {
            protocol_version: 3,
            commitment: commitment.clone(),
            ke1: client.message.serialize().to_vec(),
        },
        MAXIMUM,
    )?;
    let challenge: Value = wss_transport::receive(socket, MAXIMUM)?;
    if challenge.get("error").and_then(Value::as_str) == Some("PAIRING_PROTOCOL_UPGRADE_REQUIRED") {
        anyhow::bail!("PAIRING_PROTOCOL_UPGRADE_REQUIRED");
    }
    let challenge: Challenge = rejected(serde_json::from_value(challenge).map_err(Into::into))?;
    rejected(validate_metadata(&challenge.metadata, leaf))?;
    ensure!(
        candidate.installation != challenge.metadata.issuer.installation,
        "PAIRING_CODE_REJECTED"
    );
    let context = context(&challenge.metadata, exporter, leaf, &commitment)?;
    let mut finish = rejected(
        client
            .state
            .finish(
                &mut rng,
                code.as_bytes(),
                CredentialResponse::deserialize(&challenge.ke2).map_err(anyhow::Error::from)?,
                ClientLoginFinishParameters {
                    context: Some(&context),
                    ksf: Some(&ksf()),
                    ..Default::default()
                },
            )
            .map_err(Into::into),
    )?;
    code.zeroize();
    finish.session_key.zeroize();
    finish.export_key.zeroize();
    // A known installation is never rebound just because a fresh capability
    // exchange succeeded. The final trust transaction repeats this check.
    for prior in config.registry()?.peers()? {
        if prior.installation == challenge.metadata.issuer.installation {
            ensure!(!prior.revoked, "PAIR_REVOKED");
            ensure!(
                prior.certificate == leaf,
                "PAIR_KEY_CHANGED: explicit rotation required"
            );
        }
    }
    #[cfg(test)]
    {
        let pause = BEFORE_FINISH
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .remove(&_address.port());
        if let Some((ready, release)) = pause {
            ready.send(()).unwrap();
            release.recv_timeout(Duration::from_secs(8)).unwrap();
        }
    }
    cancelled(stop)?;
    // Only server-authenticated OPAQUE success permits public candidate disclosure.
    wss_transport::send(
        socket,
        &Finish {
            ke3: finish.message.serialize().to_vec(),
            nonce,
            candidate,
        },
        MAXIMUM,
    )?;
    let result: Value = wss_transport::receive(socket, MAXIMUM)?;
    ensure!(
        result["ok"] == true
            && result["protocol_version"] == 3
            && result["generation"] == challenge.metadata.generation,
        "PAIRING_CODE_REJECTED"
    );
    let pending = result["invitation"]
        .as_str()
        .context("PAIRING_CODE_REJECTED")?;
    uuid::Uuid::parse_str(pending)?;
    cancelled(stop)?;
    let issuer = challenge.metadata.issuer;
    config.registry()?.trust(&issuer)?; // refuses existing pin drift/revocation
    Ok(
        json!({"invitation":pending,"issuer":issuer.installation,"fingerprint":issuer.fingerprint(),"status":"awaiting-issuer-local-approval"}),
    )
}

pub(crate) fn serve(
    config: &Config,
    identity: &Identity,
    socket: &mut tungstenite::WebSocket<
        rustls::StreamOwned<rustls::ServerConnection, crate::ws_tunnel::Socket>,
    >,
    stop: &AtomicBool,
) -> Result<()> {
    let result = serve_inner(config, identity, socket, stop);
    if let Err(error) = &result {
        let reason = if error.to_string() == "PAIRING_PROTOCOL_UPGRADE_REQUIRED" {
            "PAIRING_PROTOCOL_UPGRADE_REQUIRED"
        } else {
            "PAIRING_CODE_REJECTED"
        };
        let _ = wss_transport::send(socket, &json!({"ok":false,"error":reason}), MAXIMUM);
    }
    result
}
fn serve_inner(
    config: &Config,
    identity: &Identity,
    socket: &mut tungstenite::WebSocket<
        rustls::StreamOwned<rustls::ServerConnection, crate::ws_tunnel::Socket>,
    >,
    stop: &AtomicBool,
) -> Result<()> {
    ensure!(
        config.advertise.starts_with("wss://"),
        "PAIRING_CODE_REJECTED"
    );
    socket.get_mut().sock.limit(128 * 1024);
    let start: Start = wss_transport::receive(socket, MAXIMUM)?;
    ensure!(
        start.protocol_version == 3
            && start.commitment.len() == 64
            && start.commitment.bytes().all(|b| b.is_ascii_hexdigit()),
        "PAIRING_CODE_REJECTED"
    );
    let request = CredentialRequest::<Suite>::deserialize(&start.ke1)?;
    let mut store = crate::pairing_code::Store::for_config(
        config
            .orchestrator_config
            .as_deref()
            .context("PAIRING_PROTOCOL_UPGRADE_REQUIRED")?,
    )?;
    let snapshot = store.begin_opaque()?; // charge attempts, including abandoned KE3
    let metadata = Metadata {
        protocol_version: 3,
        suite: SUITE.into(),
        generation: snapshot.generation,
        orchestrator: snapshot.orchestrator,
        issuer: Peer {
            installation: identity.installation.clone(),
            certificate: identity.certificate.clone(),
            endpoint: config.advertise.clone(),
            revoked: false,
        },
    };
    let exporter = socket.get_ref().conn.export_keying_material(
        [0u8; 32],
        b"EXPORTER-Channel-Binding",
        Some(&[]),
    )?;
    let context = context(
        &metadata,
        &exporter,
        &identity.certificate,
        &start.commitment,
    )?;
    let setup = ServerSetup::<Suite>::deserialize(&snapshot.setup)?;
    let record = ServerRegistration::<Suite>::deserialize(&snapshot.record)?;
    let parameters = ServerLoginParameters {
        context: Some(&context),
        ..Default::default()
    };
    let login = ServerLogin::start(
        &mut rand::rngs::OsRng,
        &setup,
        Some(record),
        request,
        PRINCIPAL,
        parameters.clone(),
    )?;
    wss_transport::send(
        socket,
        &Challenge {
            metadata,
            ke2: login.message.serialize().to_vec(),
        },
        MAXIMUM,
    )?;
    let finish: Finish = wss_transport::receive(socket, MAXIMUM)?;
    ensure!(
        commitment(&finish.candidate, &finish.nonce)? == start.commitment,
        "PAIRING_CODE_REJECTED"
    );
    let mut result = login.state.finish(
        CredentialFinalization::deserialize(&finish.ke3)?,
        parameters,
    )?;
    result.session_key.zeroize();
    cancelled(stop)?;
    let pending = store.authorize_generation(snapshot.generation, || {
        config.registry()?.submit_code_candidate(&finish.candidate)
    })?;
    #[cfg(test)]
    if DROP_FINAL_ACK
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .remove(&config.data_directory)
    {
        return Ok(()); // committed admission, connection closes without any ack
    }
    wss_transport::send(
        socket,
        &json!({"ok":true,"protocol_version":3,"generation":snapshot.generation,"invitation":pending}),
        MAXIMUM,
    )?;
    // Exactly one PAKE instance per TLS connection; caller closes immediately.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn opaque_authenticates_password_transcript_and_fresh_server_state() {
        let code = "7".repeat(200);
        let (setup, record) = registration(&code).unwrap();
        let setup = ServerSetup::<Suite>::deserialize(&setup).unwrap();
        let record = ServerRegistration::<Suite>::deserialize(&record).unwrap();
        let mut rng = rand::rngs::OsRng;
        let client = ClientLogin::<Suite>::start(&mut rng, code.as_bytes()).unwrap();
        let params = ServerLoginParameters {
            context: Some(b"bound-exporter-certificate-generation-identity"),
            ..Default::default()
        };
        let server = ServerLogin::start(
            &mut rng,
            &setup,
            Some(record.clone()),
            client.message.clone(),
            PRINCIPAL,
            params.clone(),
        )
        .unwrap();
        for (password, context) in [
            (&"8".repeat(200), params.context.unwrap()),
            (&code, b"substituted-exporter-or-identity".as_slice()),
        ] {
            assert!(
                client
                    .state
                    .clone()
                    .finish(
                        &mut rng,
                        password.as_bytes(),
                        server.message.clone(),
                        ClientLoginFinishParameters {
                            context: Some(context),
                            ksf: Some(&ksf()),
                            ..Default::default()
                        }
                    )
                    .is_err()
            );
        }
        let finish = client
            .state
            .finish(
                &mut rng,
                code.as_bytes(),
                server.message,
                ClientLoginFinishParameters {
                    context: params.context,
                    ksf: Some(&ksf()),
                    ..Default::default()
                },
            )
            .unwrap();
        let replay = ServerLogin::start(
            &mut rng,
            &setup,
            Some(record),
            client.message,
            PRINCIPAL,
            params.clone(),
        )
        .unwrap();
        assert!(
            replay
                .state
                .finish(finish.message.clone(), params.clone())
                .is_err()
        );
        let accepted = server.state.finish(finish.message, params).unwrap();
        assert_eq!(finish.session_key, accepted.session_key);
    }
    #[test]
    fn transcript_covers_all_bootstrap_authority_and_channel_fields() {
        let identity = Identity::generate(&crate::core::id()).unwrap();
        let mut metadata = Metadata {
            protocol_version: 3,
            suite: SUITE.into(),
            generation: 1,
            orchestrator: crate::core::id(),
            issuer: Peer {
                installation: identity.installation.clone(),
                certificate: identity.certificate.clone(),
                endpoint: "wss://127.0.0.1:34567/tkfs/sync".into(),
                revoked: false,
            },
        };
        validate_metadata(&metadata, &identity.certificate).unwrap();
        let original = context(&metadata, &[1; 32], &identity.certificate, "commitment").unwrap();
        assert_ne!(
            original,
            context(&metadata, &[2; 32], &identity.certificate, "commitment").unwrap()
        );
        assert_ne!(
            original,
            context(&metadata, &[1; 32], b"other-leaf", "commitment").unwrap()
        );
        assert_ne!(
            original,
            context(
                &metadata,
                &[1; 32],
                &identity.certificate,
                "other-candidate"
            )
            .unwrap()
        );
        for field in [
            "protocol_version",
            "suite",
            "generation",
            "orchestrator",
            "issuer",
        ] {
            let mut value = serde_json::to_value(&metadata).unwrap();
            match field {
                "protocol_version" => value[field] = json!(2),
                "suite" => value[field] = json!("other-suite"),
                "generation" => value[field] = json!(2),
                "orchestrator" => value[field] = json!(crate::core::id()),
                _ => value[field]["endpoint"] = json!("wss://127.0.0.1:34568/tkfs/sync"),
            }
            let changed: Metadata = serde_json::from_value(value).unwrap();
            assert_ne!(
                original,
                context(&changed, &[1; 32], &identity.certificate, "commitment").unwrap()
            );
        }
        metadata.issuer.certificate = Identity::generate(&crate::core::id())
            .unwrap()
            .certificate
            .clone();
        assert!(validate_metadata(&metadata, &identity.certificate).is_err());
    }
    #[cfg(windows)]
    #[test]
    fn address_only_real_tls_pairing_rotation_restart_and_no_pin_replacement() {
        use crate::{
            core::id,
            pairing::CredentialSource,
            pairing_service::{Service, Transport},
        };
        let temp = tempfile::tempdir().unwrap();
        crate::local_ipc::protect_directory(temp.path()).unwrap();
        let o1 = crate::pairing_code::tests::configuration(temp.path());
        let mut codes = crate::pairing_code::Store::for_config(&o1).unwrap();
        let code = codes.reset().unwrap();
        let server_identity = Arc::new(Identity::generate(&id()).unwrap());
        let port = {
            let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            reserved.local_addr().unwrap().port()
        };
        let server = Config {
            format_version: 1,
            orchestrator_config: Some(o1),
            installation: server_identity.installation.clone(),
            data_directory: temp.path().join("server"),
            credential: CredentialSource::WindowsDpapi {
                file: temp.path().join("unused.dpapi"),
            },
            transport: Transport::Wss,
            inbound: true,
            dial: false,
            listen: format!("127.0.0.1:{port}"),
            enrollment_listen: String::new(),
            advertise: format!("wss://127.0.0.1:{port}/tkfs/sync"),
            enrollment_advertise: format!("wss://127.0.0.1:{port}/tkfs/enroll"),
        };
        let client_identity = Identity::generate(&id()).unwrap();
        let credential = CredentialSource::WindowsDpapi {
            file: temp.path().join("client.dpapi"),
        };
        credential.save_windows(&client_identity).unwrap();
        let client = Config {
            installation: client_identity.installation.clone(),
            orchestrator_config: None,
            data_directory: temp.path().join("client"),
            credential,
            inbound: false,
            dial: true,
            listen: String::new(),
            advertise: "outbound-only".into(),
            enrollment_advertise: String::new(),
            ..server.clone()
        };
        struct Running {
            stop: Arc<AtomicBool>,
            thread: Option<std::thread::JoinHandle<Result<()>>>,
        }
        impl Drop for Running {
            fn drop(&mut self) {
                self.stop.store(true, Ordering::Relaxed);
                if let Some(t) = self.thread.take() {
                    t.join().unwrap().unwrap();
                }
            }
        }
        let start = || {
            let service = Service::open_identity(server.clone(), server_identity.clone()).unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let flag = stop.clone();
            Running {
                stop,
                thread: Some(std::thread::spawn(move || service.run(flag))),
            }
        };
        let running = start();
        let address = SocketAddr::new("127.0.0.1".parse().unwrap(), port);
        let refusal = |wire: &mut ProvisionalChannel| {
            let response: Result<Value> = wss_transport::receive(wire, MAXIMUM);
            assert!(response.is_err() || response.unwrap()["ok"] == false);
        };
        for oversized in [false, true] {
            let (mut wire, _, _) = channel(address, Arc::new(AtomicBool::new(false))).unwrap();
            if oversized {
                wire.send(tungstenite::Message::Binary(vec![0; MAXIMUM + 1].into()))
                    .unwrap();
            } else {
                wss_transport::send(
                    &mut wire,
                    &Start {
                        protocol_version: 3,
                        commitment: "0".repeat(64),
                        ke1: vec![],
                    },
                    MAXIMUM,
                )
                .unwrap();
            }
            refusal(&mut wire);
        }
        let opening = || {
            let (mut wire, leaf, exporter) =
                channel(address, Arc::new(AtomicBool::new(false))).unwrap();
            let candidate = Peer {
                installation: client_identity.installation.clone(),
                certificate: client_identity.certificate.clone(),
                endpoint: "outbound-only".into(),
                revoked: false,
            };
            let nonce = [9u8; 32];
            let digest = commitment(&candidate, &nonce).unwrap();
            let login =
                ClientLogin::<Suite>::start(&mut rand::rngs::OsRng, code.expose().as_bytes())
                    .unwrap();
            wss_transport::send(
                &mut wire,
                &Start {
                    protocol_version: 3,
                    commitment: digest.clone(),
                    ke1: login.message.serialize().to_vec(),
                },
                MAXIMUM,
            )
            .unwrap();
            let challenge: Challenge = wss_transport::receive(&mut wire, MAXIMUM).unwrap();
            let context = context(&challenge.metadata, &exporter, &leaf, &digest).unwrap();
            (wire, login, challenge, context, candidate, nonce)
        };
        let (wire, login, challenge, context, candidate, nonce) = opening();
        let mut finish = login
            .state
            .finish(
                &mut rand::rngs::OsRng,
                code.expose().as_bytes(),
                CredentialResponse::deserialize(&challenge.ke2).unwrap(),
                ClientLoginFinishParameters {
                    context: Some(&context),
                    ksf: Some(&ksf()),
                    ..Default::default()
                },
            )
            .unwrap();
        finish.session_key.zeroize();
        finish.export_key.zeroize();
        let replay = Finish {
            ke3: finish.message.serialize().to_vec(),
            nonce,
            candidate,
        };
        drop(wire); // abandoned KE3 remains charged
        let (mut wire, _, _, _, _, _) = opening();
        wss_transport::send(&mut wire, &replay, MAXIMUM).unwrap();
        refusal(&mut wire);
        drop(wire);
        for oversized in [false, true] {
            let (mut wire, _, _, _, candidate, nonce) = opening();
            if oversized {
                wire.send(tungstenite::Message::Binary(vec![0; MAXIMUM + 1].into()))
                    .unwrap();
            } else {
                wss_transport::send(
                    &mut wire,
                    &Finish {
                        ke3: vec![],
                        nonce,
                        candidate,
                    },
                    MAXIMUM,
                )
                .unwrap();
            }
            refusal(&mut wire);
        }
        assert_eq!(server.registry().unwrap().pending().unwrap(), json!([]));
        let join = |key: &str| {
            join(
                &client,
                "127.0.0.1",
                port,
                Zeroizing::new(key.into()),
                Arc::new(AtomicBool::new(false)),
            )
        };
        assert!(join(&"0".repeat(200)).is_err());
        assert!(client.registry().unwrap().peers().unwrap().is_empty());
        // An active TLS-terminating relay has valid keys for its own certificate
        // and forwards the genuine PAKE response. Exporter/leaf/identity binding
        // still prevents candidate disclosure and local trust persistence.
        let proxy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let proxy_port = proxy.local_addr().unwrap().port();
        let relay_identity = Identity::generate(&id()).unwrap();
        let relay = std::thread::spawn(move || {
            let flag = Arc::new(AtomicBool::new(false));
            let (raw, _) = proxy.accept().unwrap();
            let mut raw = crate::ws_tunnel::Socket::Direct(
                BudgetSocket::new(raw, Duration::from_secs(5), flag.clone()).unwrap(),
            );
            let mut conn =
                rustls::ServerConnection::new(relay_identity.tls_wss_server(&[]).unwrap()).unwrap();
            while conn.is_handshaking() {
                conn.complete_io(&mut raw).unwrap();
            }
            let callback =
                |_: &tungstenite::handshake::server::Request,
                 mut response: tungstenite::handshake::server::Response| {
                    response
                        .headers_mut()
                        .insert("Sec-WebSocket-Protocol", "tkfs-wss-v1".parse().unwrap());
                    Ok(response)
                };
            let mut downstream = tungstenite::accept_hdr_with_config(
                rustls::StreamOwned::new(conn, raw),
                callback,
                Some(wss_transport::websocket_config()),
            )
            .unwrap();
            downstream.get_mut().budget(Duration::from_secs(8));
            let (mut upstream, _, _) =
                channel(SocketAddr::new("127.0.0.1".parse().unwrap(), port), flag).unwrap();
            let start: Start = wss_transport::receive(&mut downstream, MAXIMUM).unwrap();
            wss_transport::send(&mut upstream, &start, MAXIMUM).unwrap();
            let mut challenge: Challenge = wss_transport::receive(&mut upstream, MAXIMUM).unwrap();
            challenge.metadata.issuer.installation = relay_identity.installation.clone();
            challenge.metadata.issuer.certificate = relay_identity.certificate.clone();
            challenge.metadata.issuer.endpoint = format!("wss://127.0.0.1:{proxy_port}/tkfs/sync");
            wss_transport::send(&mut downstream, &challenge, MAXIMUM).unwrap();
            wss_transport::receive::<Value>(&mut downstream, MAXIMUM).is_err()
        });
        assert_eq!(
            super::join(
                &client,
                "127.0.0.1",
                proxy_port,
                Zeroizing::new(code.expose().into()),
                Arc::new(AtomicBool::new(false))
            )
            .unwrap_err()
            .to_string(),
            "PAIRING_CODE_REJECTED"
        );
        assert!(
            relay.join().unwrap(),
            "relay must not receive candidate/KE3"
        );
        assert!(client.registry().unwrap().peers().unwrap().is_empty());
        assert_eq!(server.registry().unwrap().pending().unwrap(), json!([]));
        let first = join(code.expose()).unwrap();
        let second = join(code.expose()).unwrap();
        assert_eq!(first["invitation"], second["invitation"]);
        let issuer = &client.registry().unwrap().peers().unwrap()[0];
        assert_eq!(issuer.certificate, server_identity.certificate);
        drop(running);
        let running = start();
        assert_eq!(
            first["invitation"],
            join(code.expose()).unwrap()["invitation"]
        );
        let fresh = codes.reset().unwrap();
        assert!(join(code.expose()).is_err());
        assert!(join(fresh.expose()).is_ok());
        assert_eq!(
            client.registry().unwrap().peers().unwrap()[0].certificate,
            server_identity.certificate
        );
        let make_client = || {
            let identity = Identity::generate(&id()).unwrap();
            let credential = CredentialSource::WindowsDpapi {
                file: temp.path().join(format!("{}.dpapi", identity.installation)),
            };
            credential.save_windows(&identity).unwrap();
            Config {
                installation: identity.installation.clone(),
                data_directory: temp.path().join(&identity.installation),
                credential,
                ..client.clone()
            }
        };
        let drifted = make_client();
        let alternate = Identity::generate(&id()).unwrap();
        let prior = Peer {
            installation: server_identity.installation.clone(),
            certificate: alternate.certificate.clone(),
            endpoint: server.advertise.clone(),
            revoked: false,
        };
        drifted.registry().unwrap().trust(&prior).unwrap();
        assert_eq!(
            super::join(
                &drifted,
                "127.0.0.1",
                port,
                Zeroizing::new(fresh.expose().into()),
                Arc::new(AtomicBool::new(false))
            )
            .unwrap_err()
            .to_string(),
            "PAIR_KEY_CHANGED: explicit rotation required"
        );
        assert_eq!(
            drifted.registry().unwrap().peers().unwrap()[0].certificate,
            prior.certificate
        );
        let pending = server.registry().unwrap().pending().unwrap();
        let racing = make_client();
        let worker_config = racing.clone();
        let worker_code = Zeroizing::new(fresh.expose().to_owned());
        let (ready, reached) = std::sync::mpsc::channel();
        let (release, resume) = std::sync::mpsc::channel();
        BEFORE_FINISH
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .insert(port, (ready, resume));
        let worker = std::thread::spawn(move || {
            super::join(
                &worker_config,
                "127.0.0.1",
                port,
                worker_code,
                Arc::new(AtomicBool::new(false)),
            )
        });
        reached.recv_timeout(Duration::from_secs(8)).unwrap();
        let rotated = codes.reset().unwrap();
        release.send(()).unwrap();
        assert_eq!(
            worker.join().unwrap().unwrap_err().to_string(),
            "PAIRING_CODE_REJECTED"
        );
        assert_eq!(server.registry().unwrap().pending().unwrap(), pending);
        assert!(racing.registry().unwrap().peers().unwrap().is_empty());
        let ack_lost = make_client();
        DROP_FINAL_ACK
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .insert(server.data_directory.clone());
        assert!(
            super::join(
                &ack_lost,
                "127.0.0.1",
                port,
                Zeroizing::new(rotated.expose().into()),
                Arc::new(AtomicBool::new(false))
            )
            .is_err()
        );
        assert!(ack_lost.registry().unwrap().peers().unwrap().is_empty());
        let pending_after_loss = server.registry().unwrap().pending().unwrap();
        assert_eq!(
            pending_after_loss.as_array().unwrap().len(),
            pending.as_array().unwrap().len() + 1
        );
        // Explicit retry requires re-entering the key and deduplicates admission.
        assert!(
            super::join(
                &ack_lost,
                "127.0.0.1",
                port,
                Zeroizing::new(rotated.expose().into()),
                Arc::new(AtomicBool::new(false))
            )
            .is_ok()
        );
        assert_eq!(
            server.registry().unwrap().pending().unwrap(),
            pending_after_loss
        );
        let cancelled = Arc::new(AtomicBool::new(true));
        assert_eq!(
            super::join(
                &client,
                "127.0.0.1",
                port,
                Zeroizing::new(fresh.expose().into()),
                cancelled
            )
            .unwrap_err()
            .to_string(),
            "PAIRING_CANCELLED"
        );
        drop(running);
    }
}
