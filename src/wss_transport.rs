//! One TLS ingress, optionally carried inside loopback WS. HTTP headers never establish an identity. Local
//! management remains outside this protocol; each data operation reloads grants.
use crate::{
    core::id,
    pairing::*,
    pairing_service::{self, BudgetSocket, Config},
};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tungstenite::{
    Message, WebSocket,
    handshake::{
        client::generate_key,
        server::{Request, Response},
    },
    http::{self, StatusCode},
    protocol::WebSocketConfig,
};
use zeroize::Zeroizing;

pub(crate) const MAX_PAGE: usize = 8 * 1024 * 1024;
const HEADER: usize = 8192;
const ENROLLMENT: usize = 64 * 1024;
const HANDSHAKE: Duration = Duration::from_secs(5);
const OPERATION: Duration = Duration::from_secs(45);
const IDLE: Duration = Duration::from_secs(60);
const RETAIN_IDLE: Duration = Duration::from_secs(30);
const SHUTDOWN: Duration = Duration::from_secs(35);
const MAX_DEVICES: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Lane {
    Control,
    Bulk,
}
impl Lane {
    fn maximum(self) -> usize {
        if self == Self::Control { 1 } else { 2 }
    }
}
trait TimedStream: Read + Write {
    fn budget(&mut self, duration: Duration);
}
impl TimedStream for rustls::StreamOwned<rustls::ClientConnection, crate::ws_tunnel::Socket> {
    fn budget(&mut self, duration: Duration) {
        self.sock.stage(duration, Instant::now() + duration);
        self.sock.limit(MAX_PAGE + 64 * 1024);
    }
}
impl TimedStream for rustls::StreamOwned<rustls::ServerConnection, crate::ws_tunnel::Socket> {
    fn budget(&mut self, duration: Duration) {
        self.sock.stage(duration, Instant::now() + duration);
        self.sock.limit(MAX_PAGE + 64 * 1024);
    }
}
type Client = WebSocket<rustls::StreamOwned<rustls::ClientConnection, crate::ws_tunnel::Socket>>;
pub(crate) fn is_websocket(value: &str) -> bool {
    value.starts_with("wss://") || value.starts_with("ws://")
}
fn websocket_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .read_buffer_size(8192)
        .write_buffer_size(0)
        .max_write_buffer_size(2 * MAX_PAGE + 1)
        .max_message_size(Some(MAX_PAGE))
        .max_frame_size(Some(MAX_PAGE))
}
pub(crate) fn endpoint(value: &str, enrollment: bool) -> Result<String> {
    let uri: http::Uri = value.parse().context("INVALID_WSS_ENDPOINT")?;
    ensure!(
        matches!(uri.scheme_str(), Some("wss" | "ws")),
        "WSS_OR_LOOPBACK_WS_REQUIRED"
    );
    let address = uri.authority().context("WSS_ADDRESS_REQUIRED")?.as_str();
    // Initial slice uses an explicit numeric address/port. TLS identity is the
    // approved installation certificate, never a hostname/proxy header.
    let socket: std::net::SocketAddr = address.parse().context("WSS_NUMERIC_ADDRESS_REQUIRED")?;
    if uri.scheme_str() == Some("ws") {
        ensure!(
            crate::ws_tunnel::loopback(socket.ip()),
            "WS_LOOPBACK_REQUIRED"
        );
    }
    ensure!(
        socket.port() != 0 && uri.query().is_none(),
        "INVALID_WSS_ENDPOINT"
    );
    ensure!(
        uri.path()
            == if enrollment {
                "/tkfs/enroll"
            } else {
                "/tkfs/sync"
            },
        "INVALID_WSS_PATH"
    );
    Ok(address.into())
}
pub(crate) fn validate_config(config: &Config) -> Result<()> {
    ensure!(config.enrollment_listen.is_empty(), "WSS_USES_ONE_LISTENER");
    if config.inbound {
        let listen = config
            .listen
            .parse::<std::net::SocketAddr>()
            .context("INVALID_LISTEN_ADDRESS")?;
        let sync = endpoint(&config.advertise, false)?;
        let enrollment = endpoint(&config.enrollment_advertise, true)?;
        ensure!(sync == enrollment, "WSS_USES_ONE_PUBLIC_ENDPOINT");
        ensure!(
            config.advertise.starts_with("ws://")
                == config.enrollment_advertise.starts_with("ws://"),
            "WS_ENDPOINT_SCHEME_MISMATCH"
        );
        if config.advertise.starts_with("ws://") {
            ensure!(
                crate::ws_tunnel::loopback(listen.ip()),
                "WS_LOOPBACK_BIND_REQUIRED"
            );
        }
    } else {
        ensure!(
            config.listen.is_empty()
                && config.enrollment_advertise.is_empty()
                && config.advertise == "outbound-only",
            "OUTBOUND_ONLY_HAS_NO_LISTENERS"
        );
    }
    Ok(())
}
fn send<T: Serialize>(
    socket: &mut WebSocket<impl TimedStream>,
    value: &T,
    maximum: usize,
) -> Result<()> {
    let bytes = Zeroizing::new(serde_json::to_vec(value)?);
    ensure!(bytes.len() <= maximum, "WSS_MESSAGE_TOO_LARGE");
    socket.send(Message::Binary(bytes.to_vec().into()))?;
    Ok(())
}
fn receive<T: DeserializeOwned>(
    socket: &mut WebSocket<impl TimedStream>,
    maximum: usize,
) -> Result<T> {
    socket.set_config(|config| {
        config.max_message_size = Some(maximum);
        config.max_frame_size = Some(maximum);
    });
    let mut controls = 0;
    loop {
        match socket.read()? {
            Message::Binary(bytes) => {
                ensure!(bytes.len() <= maximum, "WSS_MESSAGE_TOO_LARGE");
                return serde_json::from_slice(&bytes).context("INVALID_WSS_REQUEST");
            }
            Message::Ping(_) => socket.flush()?, // Tungstenite queues the Pong.
            Message::Pong(_) => {}
            Message::Close(_) => bail!("WSS_SESSION_CLOSED"),
            _ => bail!("WSS_BINARY_MESSAGES_REQUIRED"),
        }
        controls += 1;
        ensure!(controls <= 32, "WSS_CONTROL_FRAME_BUDGET");
        // Control frames never reset the absolute message/idle budget.
    }
}
fn connect(
    identity: &Identity,
    peer: &Peer,
    enrollment: bool,
    lane: Lane,
    stop: Arc<AtomicBool>,
) -> Result<Client> {
    ensure!(!peer.revoked, "PAIR_REVOKED");
    let address = endpoint(&peer.endpoint, enrollment)?;
    let name =
        rustls::pki_types::ServerName::try_from(format!("{}.tkfs.invalid", peer.installation))?;
    let mut conn = rustls::ClientConnection::new(identity.tls_wss_client(peer, enrollment)?, name)?;
    let raw = crate::runtime::stream(&address)?;
    let ws = peer.endpoint.starts_with("ws://");
    if ws {
        crate::ws_tunnel::check_socket(&raw)?;
    }
    let socket = BudgetSocket::new(raw, HANDSHAKE, stop)?;
    let mut socket = if ws {
        crate::ws_tunnel::Socket::client(socket, &address)?
    } else {
        crate::ws_tunnel::Socket::Direct(socket)
    };
    while conn.is_handshaking() {
        conn.complete_io(&mut socket)?;
    }
    ensure!(
        conn.alpn_protocol() == Some(b"http/1.1"),
        "WSS_HTTP_PROTOCOL_REQUIRED"
    );
    ensure!(
        conn.peer_certificates()
            .and_then(|chain| chain.first())
            .is_some_and(|certificate| certificate.as_ref() == peer.certificate),
        "PAIR_KEY_MISMATCH"
    );
    let url = if enrollment {
        peer.endpoint.clone()
    } else {
        format!(
            "{}/{}",
            peer.endpoint,
            if lane == Lane::Control {
                "control"
            } else {
                "bulk"
            }
        )
    };
    let request = http::Request::builder()
        .method("GET")
        .uri(&url)
        .header("Host", &address)
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header("Sec-WebSocket-Key", generate_key())
        .header("Sec-WebSocket-Protocol", "tkfs-wss-v1")
        .body(())?;
    let stream = rustls::StreamOwned::new(conn, socket);
    let (websocket, response) =
        tungstenite::client::client_with_config(request, stream, Some(websocket_config()))
            .map_err(|_| anyhow::anyhow!("WSS_UPGRADE_FAILED"))?;
    ensure!(
        response
            .headers()
            .get("Sec-WebSocket-Protocol")
            .is_some_and(|value| value == "tkfs-wss-v1"),
        "WSS_PROTOCOL_REQUIRED"
    );
    Ok(websocket)
}
pub(crate) fn enroll<T: Serialize>(
    identity: &Identity,
    issuer: &Peer,
    request: &T,
) -> Result<Value> {
    let mut socket = connect(
        identity,
        issuer,
        true,
        Lane::Control,
        Arc::new(AtomicBool::new(false)),
    )?;
    socket.get_mut().budget(Duration::from_secs(10));
    send(&mut socket, request, ENROLLMENT)?;
    receive(&mut socket, ENROLLMENT)
}

struct IdleConnection {
    socket: Client,
    used: Instant,
    created: Instant,
    certificate: Vec<u8>,
    endpoint: String,
    _quota: Option<Connection>,
}
impl IdleConnection {
    fn reusable(&self, device: &str, policy: Option<&PoolPolicy>, now: Instant) -> bool {
        now.saturating_duration_since(self.used) < RETAIN_IDLE
            && now.saturating_duration_since(self.created) < Duration::from_secs(600)
            && policy.is_none_or(|policy| policy.allows(device, &self.certificate, &self.endpoint))
    }
}
struct PoolPolicy {
    dial: bool,
    peers: BTreeMap<String, Peer>,
}
impl PoolPolicy {
    fn allows(&self, device: &str, certificate: &[u8], endpoint: &str) -> bool {
        self.dial
            && self.peers.get(device).is_some_and(|peer| {
                !peer.revoked
                    && is_websocket(&peer.endpoint)
                    && peer.endpoint == endpoint
                    && peer.certificate == certificate
            })
    }
}
#[derive(Default)]
struct PoolState {
    idle: BTreeMap<(String, Lane), Vec<IdleConnection>>,
    busy: BTreeMap<(String, Lane), usize>,
    policy: Option<PoolPolicy>,
}
impl PoolState {
    fn prune(&mut self, now: Instant) {
        let policy = self.policy.as_ref();
        self.idle.retain(|(device, _), entries| {
            entries.retain(|entry| entry.reusable(device, policy, now));
            !entries.is_empty()
        });
    }
}
#[derive(Default)]
pub(crate) struct Pool {
    state: Mutex<PoolState>,
    admission: Option<Arc<Admission>>,
}
struct Lease<'a> {
    pool: &'a Pool,
    key: (String, Lane),
    connection: Option<IdleConnection>,
    keep: bool,
}
impl Drop for Lease<'_> {
    fn drop(&mut self) {
        let mut state = self.pool.state.lock().unwrap();
        *state.busy.get_mut(&self.key).unwrap() -= 1;
        if self.keep
            && let Some(connection) = self.connection.take()
            && connection.reusable(&self.key.0, state.policy.as_ref(), Instant::now())
        {
            state
                .idle
                .entry(self.key.clone())
                .or_default()
                .push(connection);
        }
    }
}
impl Pool {
    fn with_admission(admission: Arc<Admission>) -> Self {
        Self {
            admission: Some(admission),
            ..Default::default()
        }
    }
    fn lease<'a>(
        &'a self,
        identity: &Identity,
        peer: &Peer,
        lane: Lane,
        stop: Arc<AtomicBool>,
    ) -> Result<Lease<'a>> {
        let key = (peer.installation.clone(), lane);
        let mut state = self.state.lock().unwrap();
        let now = Instant::now();
        state.prune(now);
        ensure!(
            state.policy.as_ref().is_none_or(|policy| policy.allows(
                &peer.installation,
                &peer.certificate,
                &peer.endpoint
            )),
            "WSS_DIAL_POLICY_CHANGED"
        );
        let devices: std::collections::BTreeSet<_> = state
            .busy
            .iter()
            .filter(|(_, count)| **count > 0)
            .map(|((device, _), _)| device.clone())
            .chain(
                state
                    .idle
                    .iter()
                    .filter(|(_, entries)| !entries.is_empty())
                    .map(|((device, _), _)| device.clone()),
            )
            .collect();
        ensure!(
            devices.contains(&peer.installation) || devices.len() < MAX_DEVICES,
            "WSS_DEVICE_POOL_FULL"
        );
        let busy = *state.busy.get(&key).unwrap_or(&0);
        ensure!(busy < lane.maximum(), "WSS_PEER_BACKPRESSURE");
        *state.busy.entry(key.clone()).or_default() += 1;
        let connection = state
            .idle
            .entry(key.clone())
            .or_default()
            .pop()
            .filter(|entry| {
                entry.certificate == peer.certificate && entry.endpoint == peer.endpoint
            });
        drop(state);
        let mut lease = Lease {
            pool: self,
            key,
            connection,
            keep: false,
        };
        if let Some(entry) = lease.connection.as_mut()
            && entry.used.elapsed() >= Duration::from_secs(10)
        {
            entry.socket.get_mut().budget(Duration::from_secs(3));
            let nonce = id();
            let healthy = (|| -> Result<()> {
                entry
                    .socket
                    .send(Message::Ping(nonce.as_bytes().to_vec().into()))?;
                loop {
                    match entry.socket.read()? {
                        Message::Pong(value) if value.as_ref() == nonce.as_bytes() => return Ok(()),
                        Message::Ping(_) => entry.socket.flush()?,
                        _ => bail!("WSS_HEARTBEAT_FAILED"),
                    }
                }
            })()
            .is_ok();
            if !healthy {
                lease.connection = None;
            }
        }
        if lease.connection.is_none() {
            let quota = self
                .admission
                .as_ref()
                .map(|admission| admission.connection(peer, lane))
                .transpose()?;
            lease.connection = Some(IdleConnection {
                socket: connect(identity, peer, false, lane, stop)?,
                used: Instant::now(),
                created: Instant::now(),
                certificate: peer.certificate.clone(),
                endpoint: peer.endpoint.clone(),
                _quota: quota,
            });
        }
        Ok(lease)
    }
    fn maintain(&self, allowed: &[Peer], dial: bool) {
        let mut state = self.state.lock().unwrap();
        state.policy = Some(PoolPolicy {
            dial,
            peers: allowed
                .iter()
                .map(|peer| (peer.installation.clone(), peer.clone()))
                .collect(),
        });
        // Run on every service tick, even when there is no future outgoing job.
        // Dropping entries also drops their shared incoming/outgoing quota guards.
        state.prune(Instant::now());
    }
    fn clear(&self) {
        let mut state = self.state.lock().unwrap();
        state.policy = Some(PoolPolicy {
            dial: false,
            peers: BTreeMap::new(),
        });
        state.idle.clear();
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Begin {
    id: String,
    kind: String,
    repo: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    id: String,
    request: NetworkRequest,
}
pub(crate) fn request(
    identity: &Identity,
    peer: &Peer,
    request: &NetworkRequest,
    pool: Option<&Pool>,
    stop: Arc<AtomicBool>,
) -> Result<Value> {
    request_inner(identity, peer, request, pool, stop, None)
}
pub(crate) fn authorized_request(
    config: &Config,
    identity: &Identity,
    peer: &Peer,
    request: &NetworkRequest,
    pool: Option<&Pool>,
    stop: Arc<AtomicBool>,
    grant: &SyncConfiguration,
) -> Result<Value> {
    request_inner(identity, peer, request, pool, stop, Some((config, grant)))
}
fn request_inner(
    identity: &Identity,
    peer: &Peer,
    request: &NetworkRequest,
    pool: Option<&Pool>,
    stop: Arc<AtomicBool>,
    authorization: Option<(&Config, &SyncConfiguration)>,
) -> Result<Value> {
    let owned_pool = Pool::default();
    let pool = pool.unwrap_or(&owned_pool);
    let (lane, kind, repo) = match request {
        NetworkRequest::ListPublished {} => (Lane::Control, "list", None),
        NetworkRequest::Exchange { repo, .. } => (Lane::Bulk, "exchange", Some(repo.clone())),
    };
    let mut lease = pool.lease(identity, peer, lane, stop)?;
    if let Some(admission) = &pool.admission {
        admission.operation(&peer.installation, lane)?;
    }
    let entry = lease.connection.as_mut().unwrap();
    entry.socket.get_mut().budget(OPERATION);
    let request_id = id();
    send(
        &mut entry.socket,
        &Begin {
            id: request_id.clone(),
            kind: kind.into(),
            repo,
        },
        HEADER,
    )?;
    let ready: Value = receive(&mut entry.socket, HEADER)?;
    ensure!(ready["ready"] == request_id, "WSS_OPERATION_NOT_READY");
    let registry = authorization
        .map(|(config, _)| config.registry())
        .transpose()?;
    let access = registry.as_ref().map(Registry::begin_access).transpose()?;
    if let Some((_, grant)) = authorization {
        let registry = registry.as_ref().unwrap();
        registry.authenticate(&peer.certificate, &peer.installation)?;
        ensure!(
            serde_json::to_value(registry.grant(&grant.peer, &grant.repo)?)?
                == serde_json::to_value(grant)?,
            "STALE_REPOSITORY_GRANT"
        );
    }
    send(
        &mut entry.socket,
        &json!({"id":request_id,"request":request}),
        MAX_PAGE,
    )?;
    drop(access);
    let response: Value = receive(&mut entry.socket, MAX_PAGE)?;
    ensure!(response["id"] == request_id, "UNBOUND_WSS_RESPONSE");
    let result = response
        .get("result")
        .context("INVALID_WSS_RESPONSE")?
        .clone();
    entry.used = Instant::now();
    lease.keep = result["ok"] == true;
    Ok(result)
}

#[derive(Default)]
struct Device {
    connections: BTreeMap<Lane, usize>,
    operations: BTreeMap<Lane, VecDeque<Instant>>,
    seen: Option<Instant>,
}
#[derive(Clone, Copy)]
struct Settings {
    handshake: Duration,
    operation: Duration,
    enrollment: Duration,
    idle: Duration,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            handshake: HANDSHAKE,
            operation: OPERATION,
            enrollment: Duration::from_secs(10),
            idle: IDLE,
        }
    }
}
#[derive(Default)]
struct Admission {
    settings: Settings,
    pending: AtomicUsize,
    enrollment: AtomicUsize,
    active: AtomicUsize,
    devices: Mutex<BTreeMap<String, Device>>,
}
struct Count<'a>(&'a AtomicUsize);
impl Drop for Count<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}
fn count(counter: &AtomicUsize, maximum: usize) -> Result<Count<'_>> {
    ensure!(
        counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| (value
                < maximum)
                .then_some(value + 1))
            .is_ok(),
        "WSS_CAPACITY"
    );
    Ok(Count(counter))
}
struct Connection {
    admission: Arc<Admission>,
    device: String,
    lane: Lane,
}
impl Drop for Connection {
    fn drop(&mut self) {
        *self
            .admission
            .devices
            .lock()
            .unwrap()
            .get_mut(&self.device)
            .unwrap()
            .connections
            .get_mut(&self.lane)
            .unwrap() -= 1;
    }
}
impl Admission {
    fn connection(self: &Arc<Self>, peer: &Peer, lane: Lane) -> Result<Connection> {
        let mut devices = self.devices.lock().unwrap();
        devices.retain(|_, device| {
            device.connections.values().any(|count| *count > 0)
                || device.seen.is_some_and(|seen| seen.elapsed() < IDLE)
        });
        ensure!(
            devices.contains_key(&peer.installation) || devices.len() < MAX_DEVICES,
            "WSS_DEVICE_CAPACITY"
        );
        let device = devices.entry(peer.installation.clone()).or_default();
        let connections = device.connections.entry(lane).or_default();
        ensure!(*connections < lane.maximum(), "WSS_PEER_CONNECTION_LIMIT");
        *connections += 1;
        device.seen = Some(Instant::now());
        Ok(Connection {
            admission: self.clone(),
            device: peer.installation.clone(),
            lane,
        })
    }
    fn operation(&self, device: &str, lane: Lane) -> Result<()> {
        let mut devices = self.devices.lock().unwrap();
        let device = devices.get_mut(device).context("WSS_SESSION_MISSING")?;
        let operations = device.operations.entry(lane).or_default();
        let now = Instant::now();
        while operations
            .front()
            .is_some_and(|at| now.duration_since(*at) >= Duration::from_secs(1))
        {
            operations.pop_front();
        }
        ensure!(
            operations.len() < if lane == Lane::Control { 8 } else { 4 },
            "WSS_PEER_RATE_LIMIT"
        );
        operations.push_back(now);
        device.seen = Some(now);
        Ok(())
    }
}
fn rejected() -> http::Response<Option<String>> {
    http::Response::builder()
        .status(StatusCode::FORBIDDEN)
        .body(Some("WSS_UPGRADE_DENIED".into()))
        .unwrap()
}
fn accepted(
    config: &Config,
    identity: &Identity,
    socket: TcpStream,
    stop: Arc<AtomicBool>,
    admission: &Arc<Admission>,
    pending: Count<'_>,
) -> Result<()> {
    let ws = config.advertise.starts_with("ws://");
    if ws {
        crate::ws_tunnel::check_socket(&socket)?;
    }
    let socket = BudgetSocket::new(socket, admission.settings.handshake, stop.clone())?;
    let mut socket = if ws {
        crate::ws_tunnel::Socket::server(socket)?
    } else {
        crate::ws_tunnel::Socket::Direct(socket)
    };
    let registry = config.registry()?;
    let mut conn = rustls::ServerConnection::new(identity.tls_wss_server(&registry.peers()?)?)?;
    while conn.is_handshaking() {
        conn.complete_io(&mut socket)?;
    }
    ensure!(
        conn.alpn_protocol() == Some(b"http/1.1"),
        "WSS_HTTP_PROTOCOL_REQUIRED"
    );
    let certificate = conn
        .peer_certificates()
        .and_then(|chain| chain.first())
        .map(|certificate| certificate.as_ref().to_vec());
    let peer = certificate
        .as_deref()
        .map(|certificate| pairing_service::authenticated(&registry, certificate))
        .transpose()?;
    let mut selected = None;
    // The library callback API fixes this error type to an HTTP response.
    #[allow(clippy::result_large_err)]
    let callback = |request: &Request, mut response: Response| {
        if request
            .headers()
            .get("Sec-WebSocket-Key")
            .and_then(|value| data_encoding::BASE64.decode(value.as_bytes()).ok())
            .is_none_or(|nonce| nonce.len() != 16)
        {
            return Err(rejected());
        }
        if request
            .headers()
            .get("Sec-WebSocket-Protocol")
            .is_none_or(|value| value != "tkfs-wss-v1")
            || request.uri().query().is_some()
        {
            return Err(rejected());
        }
        selected = match request.uri().path() {
            "/tkfs/enroll" if certificate.is_none() => Some(None),
            "/tkfs/sync/control" if peer.is_some() => Some(Some(Lane::Control)),
            "/tkfs/sync/bulk" if peer.is_some() => Some(Some(Lane::Bulk)),
            _ => return Err(rejected()),
        };
        response
            .headers_mut()
            .insert("Sec-WebSocket-Protocol", "tkfs-wss-v1".parse().unwrap());
        Ok(response)
    };
    let mut socket = tungstenite::accept_hdr_with_config(
        rustls::StreamOwned::new(conn, socket),
        callback,
        Some(websocket_config()),
    )
    .map_err(|_| anyhow::anyhow!("WSS_UPGRADE_DENIED"))?;
    drop(pending);
    match selected.context("WSS_ROUTE_REQUIRED")? {
        None => {
            let _enrollment = count(&admission.enrollment, 2)?;
            socket.get_mut().budget(admission.settings.enrollment);
            let request: pairing_service::Enrollment = receive(&mut socket, ENROLLMENT)?;
            let result = config
                .registry()?
                .submit(&request.invitation, &request.candidate);
            let response = match result {
                Ok(()) => json!({"ok":true,"status":"pending-local-approval"}),
                Err(error) => json!({"ok":false,"error":format!("{error:#}")}),
            };
            send(&mut socket, &response, ENROLLMENT)?;
        }
        Some(lane) => {
            let peer = peer.context("CLIENT_CERTIFICATE_REQUIRED")?;
            let _connection = admission.connection(&peer, lane)?;
            let certificate = certificate.context("CLIENT_CERTIFICATE_REQUIRED")?;
            loop {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                socket.get_mut().budget(admission.settings.idle);
                let begin: Begin = receive(&mut socket, HEADER)?;
                uuid::Uuid::parse_str(&begin.id).context("INVALID_WSS_REQUEST_ID")?;
                ensure!(
                    match lane {
                        Lane::Control => begin.kind == "list" && begin.repo.is_none(),
                        Lane::Bulk => begin.kind == "exchange" && begin.repo.is_some(),
                    },
                    "WSS_LANE_MISMATCH"
                );
                admission.operation(&peer.installation, lane)?;
                socket.get_mut().budget(admission.settings.operation);
                {
                    let registry = config.registry()?;
                    let _access = registry.begin_access()?;
                    registry.authenticate(&certificate, &peer.installation)?;
                    if let Some(repo) = &begin.repo {
                        registry.grant(&peer.installation, repo)?;
                    }
                }
                send(&mut socket, &json!({"ready":begin.id}), HEADER)?;
                let payload: Payload = receive(
                    &mut socket,
                    if lane == Lane::Control {
                        HEADER
                    } else {
                        MAX_PAGE
                    },
                )?;
                ensure!(payload.id == begin.id, "UNBOUND_WSS_REQUEST");
                ensure!(
                    match &payload.request {
                        NetworkRequest::ListPublished {} => lane == Lane::Control,
                        NetworkRequest::Exchange { repo, .. } =>
                            lane == Lane::Bulk && Some(repo) == begin.repo.as_ref(),
                    },
                    "WSS_LANE_MISMATCH"
                );
                let registry = config.registry()?;
                let _access = registry.begin_access()?;
                let result = (|| {
                    let peer = registry.authenticate(&certificate, &peer.installation)?;
                    pairing_service::dispatch(&registry, &peer, payload.request, &stop, MAX_PAGE)
                })();
                let response = match result {
                    Ok(value) => json!({"ok":true,"result":value}),
                    Err(error) => json!({"ok":false,"error":format!("{error:#}")}),
                };
                send(
                    &mut socket,
                    &json!({"id":begin.id,"result":response}),
                    if lane == Lane::Control {
                        256 * 1024
                    } else {
                        MAX_PAGE
                    },
                )?;
            }
        }
    }
    Ok(())
}

pub(crate) fn run(
    config: &Config,
    identity: &Arc<Identity>,
    listener: Option<&TcpListener>,
    stop: Arc<AtomicBool>,
) -> Result<()> {
    let admission = Arc::new(Admission::default());
    run_with_admission(config, identity, listener, stop, admission)
}
fn run_with_admission(
    config: &Config,
    identity: &Arc<Identity>,
    listener: Option<&TcpListener>,
    stop: Arc<AtomicBool>,
    admission: Arc<Admission>,
) -> Result<()> {
    struct StopOnExit(Arc<AtomicBool>);
    impl Drop for StopOnExit {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }
    let _stop_on_exit = StopOnExit(stop.clone());
    let pool = Arc::new(Pool::with_admission(admission.clone()));
    let (finished, results) = std::sync::mpsc::sync_channel(8);
    let mut jobs = BTreeMap::<(String, String), (Instant, Duration)>::new();
    let mut inflight = std::collections::BTreeSet::new();
    while !stop.load(Ordering::Relaxed) {
        if let Some(listener) = listener
            && let Ok((socket, _)) = listener.accept()
            && admission.pending.load(Ordering::Relaxed) < 8
        {
            admission.pending.fetch_add(1, Ordering::Relaxed);
            admission.active.fetch_add(1, Ordering::Relaxed);
            let admission = admission.clone();
            let config = config.clone();
            let identity = identity.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let _active = Count(&admission.active);
                let pending = Count(&admission.pending);
                let _ = accepted(&config, &identity, socket, stop, &admission, pending);
            });
        }
        while let Ok((grant, result)) = results.try_recv() {
            let grant: SyncConfiguration = grant;
            let result: Result<Value> = result;
            let key = (grant.peer.clone(), grant.repo.clone());
            inflight.remove(&key);
            let job = jobs
                .entry(key)
                .or_insert((Instant::now(), Duration::from_secs(1)));
            config.registry()?.sync_status(
                &grant,
                &match &result {
                    Ok(value) => serde_json::to_string(value)?,
                    Err(error) => format!("unavailable: {error:#}"),
                },
            )?;
            job.1 = if result.is_ok() {
                Duration::from_secs(1)
            } else {
                (job.1 * 2).min(Duration::from_secs(60))
            };
            // Full jitter within the retry window, with a 250ms lower bound.
            let jitter = if result.is_ok() {
                job.1
            } else {
                Duration::from_millis(rand::random::<u64>() % (job.1.as_millis() as u64) + 250)
            };
            job.0 = Instant::now() + jitter;
        }
        let registry = config.registry()?;
        let peers = registry.peers()?;
        let mut grants = registry.configurations()?;
        let dial_peers: Vec<_> = peers
            .iter()
            .filter(|peer| {
                grants
                    .iter()
                    .any(|grant| grant.enabled && grant.peer == peer.installation)
            })
            .cloned()
            .collect();
        pool.maintain(&dial_peers, config.dial);
        // Earliest eligible job first; no stable repo ordering monopolizes slots.
        grants.sort_by_key(|grant| {
            jobs.get(&(grant.peer.clone(), grant.repo.clone()))
                .map(|job| job.0)
        });
        for grant in grants.into_iter().filter(|grant| {
            config.dial
                && grant.enabled
                && peers.iter().any(|peer| {
                    !peer.revoked
                        && peer.installation == grant.peer
                        && peer.endpoint != "outbound-only"
                })
        }) {
            let key = (grant.peer.clone(), grant.repo.clone());
            let job = jobs
                .entry(key.clone())
                .or_insert((Instant::now(), Duration::from_secs(1)));
            if inflight.len() >= 8 || inflight.contains(&key) || Instant::now() < job.0 {
                continue;
            }
            inflight.insert(key);
            let config = config.clone();
            let identity = identity.clone();
            let pool = pool.clone();
            let stop = stop.clone();
            let finished = finished.clone();
            std::thread::spawn(move || {
                let result =
                    pairing_service::synchronize_wss(&config, &identity, &grant, stop, &pool);
                let _ = finished.send((grant, result));
            });
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    pool.clear();
    let deadline = Instant::now() + SHUTDOWN;
    while admission.active.load(Ordering::Relaxed) > 0 || !inflight.is_empty() {
        ensure!(Instant::now() < deadline, "PAIRING_SHUTDOWN_DEADLINE");
        while let Ok((grant, _)) = results.try_recv() {
            inflight.remove(&(grant.peer, grant.repo));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{core::Store, paired_sync, pairing_service::Transport, runtime::Engine};
    // Fixture credentials stay in memory. The production OS credential path is
    // exercised separately by the subprocess acceptance test.
    struct Running {
        stop: Arc<AtomicBool>,
        thread: Option<std::thread::JoinHandle<Result<()>>>,
    }
    impl Drop for Running {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            let result = self.thread.take().unwrap().join().unwrap();
            if !std::thread::panicking() {
                assert!(result.is_ok(), "fixture service shutdown failed");
            }
        }
    }
    struct Fixture {
        running: Running,
        config: Config,
        issuer: Arc<Identity>,
        home: Identity,
        peer: Peer,
        admission: Arc<Admission>,
        engine: Arc<Mutex<Engine>>,
        client_engine: Engine,
        home_grant: SyncConfiguration,
        _temp: tempfile::TempDir,
    }
    fn fixture(settings: Settings) -> Fixture {
        fixture_scheme(settings, "wss")
    }
    fn fixture_scheme(settings: Settings, scheme: &str) -> Fixture {
        let temp = tempfile::tempdir().unwrap();
        let issuer = Arc::new(Identity::generate(&id()).unwrap());
        let home = Identity::generate(&id()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let config = Config {
            format_version: 1,
            transport: Transport::Wss,
            inbound: true,
            dial: false,
            installation: issuer.installation.clone(),
            data_directory: temp.path().join("server"),
            credential: CredentialSource::WindowsDpapi {
                file: temp.path().join("unused-fixture"),
            },
            listen: address.clone(),
            enrollment_listen: String::new(),
            advertise: format!("{scheme}://{address}/tkfs/sync"),
            enrollment_advertise: format!("{scheme}://{address}/tkfs/enroll"),
        };
        validate_config(&config).unwrap();
        let peer = Peer {
            installation: issuer.installation.clone(),
            certificate: issuer.certificate.clone(),
            endpoint: config.advertise.clone(),
            revoked: false,
        };
        config
            .registry()
            .unwrap()
            .trust(&Peer {
                installation: home.installation.clone(),
                certificate: home.certificate.clone(),
                endpoint: "outbound-only".into(),
                revoked: false,
            })
            .unwrap();
        let repo = id();
        let server_replica = id();
        let home_replica = id();
        #[cfg(target_os = "linux")]
        crate::private_storage::Directory::open(&temp.path().join("server-state")).unwrap();
        let engine = Arc::new(Mutex::new(Engine::new(
            Store::initialize(&temp.path().join("server-state"), &repo, &server_replica).unwrap(),
        )));
        crate::runtime::start_rpc(engine.clone(), &temp.path().join("server-state"), None).unwrap();
        let client_engine = Engine::new(
            Store::initialize(&temp.path().join("home-state"), &repo, &home_replica).unwrap(),
        );
        let server_grant = SyncConfiguration {
            peer: home.installation.clone(),
            repo: repo.clone(),
            local_runtime: temp.path().join("server-state/runtime.json"),
            local_replica: server_replica.clone(),
            remote_replica: home_replica.clone(),
            enabled: true,
        };
        config
            .registry()
            .unwrap()
            .configure_sync(&server_grant)
            .unwrap();
        let home_grant = SyncConfiguration {
            peer: issuer.installation.clone(),
            repo,
            local_runtime: temp.path().join("unused-runtime"),
            local_replica: home_replica,
            remote_replica: server_replica,
            enabled: true,
        };
        let stop = Arc::new(AtomicBool::new(false));
        let admission = Arc::new(Admission {
            settings,
            ..Default::default()
        });
        let thread_config = config.clone();
        let thread_identity = issuer.clone();
        let thread_stop = stop.clone();
        let thread_admission = admission.clone();
        let thread = std::thread::spawn(move || {
            run_with_admission(
                &thread_config,
                &thread_identity,
                Some(&listener),
                thread_stop,
                thread_admission,
            )
        });
        Fixture {
            running: Running {
                stop,
                thread: Some(thread),
            },
            config,
            issuer,
            home,
            peer,
            admission,
            engine,
            client_engine,
            home_grant,
            _temp: temp,
        }
    }
    fn wait(mut ready: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(3);
        while !ready() {
            assert!(Instant::now() < deadline, "fixture readiness timeout");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn write(engine: &mut Engine, name: &str, bytes: &[u8]) {
        let file = engine.store.create(name, "file").unwrap();
        engine.store.write_revision(&file.id, None, bytes).unwrap();
    }
    fn exchange(fixture: &mut Fixture, pool: &Pool) -> Value {
        let page =
            paired_sync::offer_bounded(&fixture.client_engine, &fixture.home_grant, None, MAX_PAGE)
                .unwrap();
        let sent: Vec<_> = page
            .bundle
            .events
            .iter()
            .map(|event| event.id.clone())
            .collect();
        let request_id = page.request.clone();
        let response = request(
            &fixture.home,
            &fixture.peer,
            &NetworkRequest::Exchange {
                repo: fixture.home_grant.repo.clone(),
                page: Box::new(page),
            },
            Some(pool),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
        assert_eq!(response["ok"], true);
        paired_sync::worker_control(&mut fixture.client_engine, &json!({"op":"published-apply","grant":fixture.home_grant,"page":response["result"],"request":request_id,"sent":sent})).unwrap()
    }
    #[test]
    fn wss_reuses_pool_transfers_large_objects_both_ways_and_keeps_private_cas_hidden() {
        large_private_flow("wss");
    }
    #[test]
    fn ws_loopback_preserves_large_transfer_private_isolation_and_live_revocation() {
        large_private_flow("ws");
    }
    fn large_private_flow(scheme: &str) {
        let mut fixture = fixture_scheme(Settings::default(), scheme);
        let pool = Pool::default();
        let bytes = vec![0x5a; 12 * 1024 * 1024];
        write(&mut fixture.client_engine, "large.bin", &bytes);
        {
            let mut server = fixture.engine.lock().unwrap();
            write(
                &mut server,
                "reverse.txt",
                b"return over the home connection",
            );
            server.store.fork("private", false).unwrap();
            server.store.checkout("private").unwrap();
            write(&mut server, "secret.txt", b"private-only CAS");
        }
        for _ in 0..12 {
            exchange(&mut fixture, &pool);
            if fixture
                .engine
                .lock()
                .unwrap()
                .store
                .shared_object_inventory()
                .unwrap()
                .contains(&crate::core::hash(&bytes))
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(275));
        }
        fixture.client_engine.store.checkout("main").unwrap();
        assert!(fixture.client_engine.store.lookup("reverse.txt").is_ok());
        assert!(fixture.client_engine.store.lookup("secret.txt").is_err());
        assert!(
            !fixture
                .client_engine
                .store
                .shared_object_inventory()
                .unwrap()
                .contains(&crate::core::hash(b"private-only CAS"))
        );
        let server = fixture.engine.lock().unwrap();
        // Shared sync projects into the shared branch while the server's private
        // checkout remains selected; inspect the shared data through its catalog.
        assert!(
            server
                .store
                .shared_object_inventory()
                .unwrap()
                .contains(&crate::core::hash(&bytes))
        );
        drop(server);
        let key = (fixture.peer.installation.clone(), Lane::Bulk);
        assert_eq!(pool.state.lock().unwrap().idle[&key].len(), 1);
        let first_created = pool.state.lock().unwrap().idle[&key][0].created;
        std::thread::sleep(Duration::from_millis(275));
        exchange(&mut fixture, &pool);
        assert_eq!(
            pool.state.lock().unwrap().idle[&key][0].created,
            first_created
        );
        fixture
            .config
            .registry()
            .unwrap()
            .revoke(&fixture.home.installation)
            .unwrap();
        assert!(
            request(
                &fixture.home,
                &fixture.peer,
                &NetworkRequest::ListPublished {},
                Some(&pool),
                Arc::new(AtomicBool::new(false))
            )
            .is_err()
        );
        // The already established bulk session also reloads revocation.
        let page =
            paired_sync::offer_bounded(&fixture.client_engine, &fixture.home_grant, None, MAX_PAGE)
                .unwrap();
        assert!(
            request(
                &fixture.home,
                &fixture.peer,
                &NetworkRequest::Exchange {
                    repo: fixture.home_grant.repo.clone(),
                    page: Box::new(page)
                },
                Some(&pool),
                Arc::new(AtomicBool::new(false))
            )
            .is_err()
        );
    }
    #[test]
    fn wss_device_limits_reserve_control_and_cancel_enrollment_and_bulk_trickles() {
        cancellation_flow("wss");
    }
    #[test]
    fn ws_loopback_retains_quotas_and_absolute_trickle_deadlines() {
        cancellation_flow("ws");
    }
    fn cancellation_flow(scheme: &str) {
        let fixture = fixture_scheme(
            Settings {
                operation: Duration::from_millis(350),
                enrollment: Duration::from_millis(350),
                handshake: Duration::from_millis(700),
                idle: Duration::from_secs(2),
            },
            scheme,
        );
        let mut bulk = vec![];
        for _ in 0..2 {
            let mut socket = connect(
                &fixture.home,
                &fixture.peer,
                false,
                Lane::Bulk,
                Arc::new(AtomicBool::new(false)),
            )
            .unwrap();
            socket.get_mut().budget(Duration::from_secs(2));
            let operation = id();
            send(
                &mut socket,
                &Begin {
                    id: operation.clone(),
                    kind: "exchange".into(),
                    repo: Some(fixture.home_grant.repo.clone()),
                },
                HEADER,
            )
            .unwrap();
            let ready: Value = receive(&mut socket, HEADER).unwrap();
            assert_eq!(ready["ready"], operation);
            // Fragment the binary message, with Ping controls between bytes;
            // progress/control traffic must not extend the absolute operation.
            bulk.push(socket);
        }
        let mut issuer = fixture.peer.clone();
        issuer.endpoint = fixture.config.enrollment_advertise.clone();
        let mut enrollment = vec![];
        for _ in 0..2 {
            enrollment.push(
                connect(
                    &fixture.home,
                    &issuer,
                    true,
                    Lane::Control,
                    Arc::new(AtomicBool::new(false)),
                )
                .unwrap(),
            );
        }
        wait(|| fixture.admission.enrollment.load(Ordering::Relaxed) == 2);
        let trickles: Vec<_> = bulk
            .into_iter()
            .chain(enrollment)
            .map(|mut socket| {
                std::thread::spawn(move || {
                    use tungstenite::protocol::frame::{
                        Frame,
                        coding::{Data, OpCode},
                    };
                    for index in 0..20 {
                        let kind = if index == 0 {
                            Data::Binary
                        } else {
                            Data::Continue
                        };
                        if socket
                            .send(Message::Frame(Frame::message(
                                vec![b' '],
                                OpCode::Data(kind),
                                false,
                            )))
                            .is_err()
                        {
                            break;
                        }
                        if socket.send(Message::Ping(vec![index].into())).is_err() {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(35));
                    }
                })
            })
            .collect();
        let before = Instant::now();
        let response = request(
            &fixture.home,
            &fixture.peer,
            &NetworkRequest::ListPublished {},
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
        assert_eq!(response["ok"], true);
        assert!(before.elapsed() < Duration::from_secs(1));
        wait(|| {
            fixture.admission.devices.lock().unwrap()[&fixture.home.installation]
                .connections
                .get(&Lane::Bulk)
                == Some(&0)
        });
        wait(|| fixture.admission.enrollment.load(Ordering::Relaxed) == 0);
        let pool = Pool::default();
        let a = pool
            .lease(
                &fixture.home,
                &fixture.peer,
                Lane::Bulk,
                Arc::new(AtomicBool::new(false)),
            )
            .unwrap();
        let b = pool
            .lease(
                &fixture.home,
                &fixture.peer,
                Lane::Bulk,
                Arc::new(AtomicBool::new(false)),
            )
            .unwrap();
        assert!(
            pool.lease(
                &fixture.home,
                &fixture.peer,
                Lane::Bulk,
                Arc::new(AtomicBool::new(false))
            )
            .is_err()
        );
        let control = pool
            .lease(
                &fixture.home,
                &fixture.peer,
                Lane::Control,
                Arc::new(AtomicBool::new(false)),
            )
            .unwrap();
        assert!(
            pool.lease(
                &fixture.home,
                &fixture.peer,
                Lane::Control,
                Arc::new(AtomicBool::new(false))
            )
            .is_err()
        );
        drop((a, b, control));
        for thread in trickles {
            thread.join().unwrap();
        }
        let raw = TcpStream::connect(&fixture.config.listen).unwrap();
        let before = Instant::now();
        drop(fixture.running);
        assert!(before.elapsed() < Duration::from_secs(1));
        drop(raw);
    }
    #[test]
    fn wss_rejects_unapproved_certificate_admin_ops_wrong_binding_and_oversized_messages() {
        let fixture = fixture(Settings::default());
        let impostor = Identity::generate(&fixture.home.installation).unwrap();
        assert!(
            connect(
                &impostor,
                &fixture.peer,
                false,
                Lane::Control,
                Arc::new(AtomicBool::new(false))
            )
            .is_err()
        );
        let unpaired = Identity::generate(&id()).unwrap();
        assert!(
            connect(
                &unpaired,
                &fixture.peer,
                false,
                Lane::Control,
                Arc::new(AtomicBool::new(false))
            )
            .is_err()
        );
        for operation in ["stop", "invite", "object", "publish", "checkout"] {
            let mut socket = connect(
                &fixture.home,
                &fixture.peer,
                false,
                Lane::Control,
                Arc::new(AtomicBool::new(false)),
            )
            .unwrap();
            socket.get_mut().budget(Duration::from_secs(2));
            let operation_id = id();
            send(
                &mut socket,
                &Begin {
                    id: operation_id.clone(),
                    kind: "list".into(),
                    repo: None,
                },
                HEADER,
            )
            .unwrap();
            let _: Value = receive(&mut socket, HEADER).unwrap();
            send(
                &mut socket,
                &json!({"id":operation_id,"request":{"op":operation}}),
                HEADER,
            )
            .unwrap();
            assert!(receive::<Value>(&mut socket, HEADER).is_err());
            wait(|| {
                fixture.admission.devices.lock().unwrap()[&fixture.home.installation]
                    .connections
                    .get(&Lane::Control)
                    == Some(&0)
            });
        }
        let pool = Pool::default();
        let mut wrong =
            paired_sync::offer_bounded(&fixture.client_engine, &fixture.home_grant, None, MAX_PAGE)
                .unwrap();
        wrong.sender = id();
        let response = request(
            &fixture.home,
            &fixture.peer,
            &NetworkRequest::Exchange {
                repo: fixture.home_grant.repo.clone(),
                page: Box::new(wrong),
            },
            Some(&pool),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
        assert_eq!(response["ok"], false);
        assert!(
            response["error"]
                .as_str()
                .unwrap()
                .contains("REMOTE_REPLICA")
        );
        let mut socket = connect(
            &fixture.home,
            &fixture.peer,
            false,
            Lane::Control,
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
        socket.get_mut().budget(Duration::from_secs(2));
        socket
            .send(Message::Binary(vec![b' '; HEADER + 1].into()))
            .unwrap();
        assert!(receive::<Value>(&mut socket, HEADER).is_err());
    }
    #[test]
    fn wss_config_and_device_rate_limits_do_not_depend_on_repo_count() {
        let fixture = fixture(Settings::default());
        let mut config = fixture.config.clone();
        config.enrollment_listen = "127.0.0.1:1".into();
        assert!(validate_config(&config).is_err());
        config = fixture.config.clone();
        config.enrollment_advertise = "wss://127.0.0.1:1/tkfs/enroll".into();
        assert!(validate_config(&config).is_err());
        config = fixture.config.clone();
        config.inbound = false;
        config.listen.clear();
        config.enrollment_advertise.clear();
        config.advertise = "outbound-only".into();
        assert!(validate_config(&config).is_ok());
        let admission = Arc::new(Admission::default());
        let peer = Peer {
            installation: fixture.home.installation.clone(),
            certificate: fixture.home.certificate.clone(),
            endpoint: "outbound-only".into(),
            revoked: false,
        };
        let a = admission.connection(&peer, Lane::Bulk).unwrap();
        let b = admission.connection(&peer, Lane::Bulk).unwrap();
        assert!(admission.connection(&peer, Lane::Bulk).is_err());
        let control = admission.connection(&peer, Lane::Control).unwrap();
        let pool = Pool::with_admission(admission.clone());
        let error = pool
            .lease(
                &fixture.issuer,
                &peer,
                Lane::Bulk,
                Arc::new(AtomicBool::new(false)),
            )
            .err()
            .unwrap();
        assert!(error.to_string().contains("WSS_PEER_CONNECTION_LIMIT"));
        for _ in 0..4 {
            admission.operation(&peer.installation, Lane::Bulk).unwrap();
        }
        assert!(admission.operation(&peer.installation, Lane::Bulk).is_err());
        admission
            .operation(&peer.installation, Lane::Control)
            .unwrap();
        drop((a, b, control));
        // Reopening connections cannot reset the per-device rate window.
        let _a = admission.connection(&peer, Lane::Bulk).unwrap();
        assert!(admission.operation(&peer.installation, Lane::Bulk).is_err());
    }
    #[test]
    fn wss_anonymous_enrollment_requires_local_approval_and_forwarded_identity_is_ignored() {
        let fixture = fixture(Settings::default());
        let candidate = Identity::generate(&id()).unwrap();
        let invitation = fixture
            .config
            .registry()
            .unwrap()
            .create_invitation(
                &fixture.issuer,
                &fixture.config.enrollment_advertise,
                &fixture.config.advertise,
                10000,
            )
            .unwrap();
        let invitation_id = invitation.id.clone();
        let mut issuer = fixture.peer.clone();
        issuer.endpoint = fixture.config.enrollment_advertise.clone();
        let response = enroll(
            &candidate,
            &issuer,
            &pairing_service::Enrollment {
                invitation,
                candidate: Peer {
                    installation: candidate.installation.clone(),
                    certificate: candidate.certificate.clone(),
                    endpoint: "outbound-only".into(),
                    revoked: false,
                },
            },
        )
        .unwrap();
        assert_eq!(response["ok"], true);
        assert!(
            connect(
                &candidate,
                &fixture.peer,
                false,
                Lane::Control,
                Arc::new(AtomicBool::new(false))
            )
            .is_err()
        );
        fixture
            .config
            .registry()
            .unwrap()
            .approve(&invitation_id)
            .unwrap();
        let response = request(
            &candidate,
            &fixture.peer,
            &NetworkRequest::ListPublished {},
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
        assert_eq!(response["result"], json!([]));
        let address = endpoint(&fixture.peer.endpoint, false).unwrap();
        let mut conn = rustls::ClientConnection::new(
            candidate.tls_wss_client(&fixture.peer, true).unwrap(),
            rustls::pki_types::ServerName::try_from(format!(
                "{}.tkfs.invalid",
                fixture.peer.installation
            ))
            .unwrap(),
        )
        .unwrap();
        let mut raw = BudgetSocket::new(
            crate::runtime::stream(&address).unwrap(),
            HANDSHAKE,
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
        while conn.is_handshaking() {
            conn.complete_io(&mut raw).unwrap();
        }
        let upgrade = http::Request::builder()
            .method("GET")
            .uri(format!("{}/control", fixture.peer.endpoint))
            .header("Host", &address)
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", generate_key())
            .header("Sec-WebSocket-Protocol", "tkfs-wss-v1")
            .header("X-SSL-Client-Cert", hex::encode(&candidate.certificate))
            .header("X-TKFS-Installation", &candidate.installation)
            .body(())
            .unwrap();
        assert!(
            tungstenite::client::client_with_config(
                upgrade,
                rustls::StreamOwned::new(conn, raw),
                Some(websocket_config())
            )
            .is_err()
        );
    }

    #[test]
    fn periodic_pool_maintenance_releases_idle_quotas_without_another_outgoing_lease() {
        let a = fixture(Settings::default());
        let b = fixture(Settings::default());
        a.config.registry().unwrap().trust(&b.peer).unwrap();
        b.config.registry().unwrap().trust(&a.peer).unwrap();
        let pool = Pool::with_admission(a.admission.clone());
        fn incoming(a: &Fixture, b: &Fixture) -> bool {
            let Ok(mut socket) = connect(
                &b.issuer,
                &a.peer,
                false,
                Lane::Bulk,
                Arc::new(AtomicBool::new(false)),
            ) else {
                return false;
            };
            socket.get_mut().budget(Duration::from_secs(1));
            if socket
                .send(Message::Ping(b"capacity".to_vec().into()))
                .is_err()
            {
                return false;
            }
            matches!(socket.read(), Ok(Message::Pong(value)) if value.as_ref() == b"capacity")
        }
        // This is the maintenance operation called by each service tick. No
        // additional outgoing lease/request is made after each policy change.
        for case in 0..8 {
            pool.maintain(std::slice::from_ref(&b.peer), true);
            let mut first = pool
                .lease(
                    &a.issuer,
                    &b.peer,
                    Lane::Bulk,
                    Arc::new(AtomicBool::new(false)),
                )
                .unwrap();
            let mut second = pool
                .lease(
                    &a.issuer,
                    &b.peer,
                    Lane::Bulk,
                    Arc::new(AtomicBool::new(false)),
                )
                .unwrap();
            first.keep = true;
            second.keep = true;
            drop((first, second));
            assert!(
                !incoming(&a, &b),
                "idle outgoing quotas did not fill both bulk slots"
            );
            let mut changed = b.peer.clone();
            let mut allowed = vec![changed.clone()];
            let mut dial = true;
            match case {
                0 => {
                    changed.endpoint = "outbound-only".into();
                    allowed = vec![changed];
                }
                1 => {
                    changed.endpoint = "wss://127.0.0.1:1/tkfs/sync".into();
                    allowed = vec![changed];
                }
                2 => {
                    changed.certificate = a.home.certificate.clone();
                    allowed = vec![changed];
                }
                3 => dial = false,
                4 => allowed.clear(),
                5 => {
                    changed.revoked = true;
                    allowed = vec![changed];
                }
                6 | 7 => {
                    let mut state = pool.state.lock().unwrap();
                    for entry in state.idle.values_mut().flatten() {
                        if case == 6 {
                            entry.used = Instant::now() - RETAIN_IDLE - Duration::from_secs(1);
                        } else {
                            entry.created = Instant::now() - Duration::from_secs(601);
                        }
                    }
                }
                _ => unreachable!(),
            }
            pool.maintain(&allowed, dial);
            assert!(
                pool.state.lock().unwrap().idle.is_empty(),
                "stale idle pool survived maintenance case {case}"
            );
            assert_eq!(
                a.admission.devices.lock().unwrap()[&b.issuer.installation].connections
                    [&Lane::Bulk],
                0
            );
            wait(|| {
                b.admission.devices.lock().unwrap()[&a.issuer.installation].connections[&Lane::Bulk]
                    == 0
            });
            assert!(
                incoming(&a, &b),
                "incoming capacity did not recover after maintenance case {case}"
            );
            wait(|| {
                a.admission.devices.lock().unwrap()[&b.issuer.installation].connections[&Lane::Bulk]
                    == 0
            });
        }
        // A response completing after the tick must not put an obsolete lease
        // back into the pool and reacquire its shared quota indefinitely.
        pool.maintain(std::slice::from_ref(&b.peer), true);
        let mut late = pool
            .lease(
                &a.issuer,
                &b.peer,
                Lane::Bulk,
                Arc::new(AtomicBool::new(false)),
            )
            .unwrap();
        pool.maintain(std::slice::from_ref(&b.peer), false);
        late.keep = true;
        drop(late);
        assert!(pool.state.lock().unwrap().idle.is_empty());
        assert_eq!(
            a.admission.devices.lock().unwrap()[&b.issuer.installation].connections[&Lane::Bulk],
            0
        );
        assert!(incoming(&a, &b));
    }
    #[test]
    fn ws_endpoints_require_numeric_loopback_and_matching_loopback_listener() {
        for address in [
            "127.0.0.1:1234",
            "127.23.4.5:1234",
            "[::1]:1234",
            "[::ffff:127.0.0.1]:1234",
        ] {
            assert!(
                endpoint(&format!("ws://{address}/tkfs/sync"), false).is_ok(),
                "{address}"
            );
        }
        for address in [
            "localhost:1234",
            "localhost.example:1234",
            "127.0.0.1.example:1234",
            "2130706433:1234",
            "0.0.0.0:1234",
            "192.168.1.2:1234",
            "172.20.0.1:1234",
            "[::]:1234",
            "[::ffff:172.20.0.1]:1234",
            "[::127.0.0.1]:1234",
            "[fe80::1%25eth0]:1234",
            "user@127.0.0.1:1234",
            "127.0.0.1:0",
        ] {
            assert!(
                endpoint(&format!("ws://{address}/tkfs/sync"), false).is_err(),
                "{address}"
            );
        }
        assert!(endpoint("ws://127.0.0.1:1234/tkfs/sync?token=invalid", false).is_err());
        assert!(endpoint("wss://172.20.0.1:1234/tkfs/sync", false).is_ok());
        let fixture = fixture_scheme(Settings::default(), "ws");
        for address in [
            "0.0.0.0:1234",
            "172.20.0.1:1234",
            "[::]:1234",
            "[::ffff:172.20.0.1]:1234",
        ] {
            let mut config = fixture.config.clone();
            config.listen = address.into();
            assert!(validate_config(&config).is_err());
        }
        let mut config = fixture.config.clone();
        config.enrollment_advertise = config.enrollment_advertise.replacen("ws://", "wss://", 1);
        assert!(validate_config(&config).is_err());
    }
    #[test]
    fn ws_http_upgrade_and_claimed_identity_do_not_authenticate_a_device() {
        let fixture = fixture_scheme(Settings::default(), "ws");
        let stop = Arc::new(AtomicBool::new(false));
        assert!(
            request(
                &fixture.home,
                &fixture.peer,
                &NetworkRequest::ListPublished {},
                None,
                stop.clone()
            )
            .is_ok()
        );
        let impostor = Identity::generate(&fixture.home.installation).unwrap();
        assert!(connect(&impostor, &fixture.peer, false, Lane::Control, stop.clone()).is_err());
        let mut wrong_server = fixture.peer.clone();
        wrong_server.certificate = impostor.certificate.clone();
        assert!(
            connect(
                &fixture.home,
                &wrong_server,
                false,
                Lane::Control,
                stop.clone()
            )
            .is_err()
        );
        let address = endpoint(&fixture.peer.endpoint, false).unwrap();
        let raw = crate::runtime::stream(&address).unwrap();
        crate::ws_tunnel::check_socket(&raw).unwrap();
        let mut carrier = crate::ws_tunnel::Socket::client(
            BudgetSocket::new(raw, Duration::from_secs(2), stop).unwrap(),
            &address,
        )
        .unwrap();
        // Upgraded WS accepts only TLS bytes, never this plaintext identity claim.
        carrier.write_all(serde_json::to_vec(&json!({"installation":fixture.home.installation,"certificate":fixture.home.certificate,"op":"stop"})).unwrap().as_slice()).unwrap();
        let mut reply = [0; 64];
        if let Ok(count) = carrier.read(&mut reply) {
            assert!(
                count == 0 || reply[0] == 21,
                "plaintext claim received a non-TLS response"
            );
        }
        assert!(!fixture.running.stop.load(Ordering::Relaxed));
    }
}
