//! Separate TLS data service, owned by one local OS account. Local commands edit
//! its private registry directly; no management dispatcher is exposed remotely.
use crate::{
    core::id,
    paired_sync::SyncPage,
    pairing::*,
    runtime::{self, Discovery},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use zeroize::Zeroizing;

#[derive(Clone, Copy)]
struct Limits {
    handshake: Duration,
    enrollment: Duration,
    sync: Duration,
    shutdown: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            handshake: Duration::from_secs(5),
            enrollment: Duration::from_secs(10),
            sync: Duration::from_secs(45),
            shutdown: Duration::from_secs(35),
        }
    }
}
/// Every socket read/write uses the remaining absolute budget. Progress never
/// extends it. Cancellation is polled even when the peer only trickles bytes.
pub(crate) struct BudgetSocket {
    socket: TcpStream,
    deadline: Instant,
    stop: Arc<AtomicBool>,
    read_left: usize,
    write_left: usize,
}
impl BudgetSocket {
    pub(crate) fn new(
        socket: TcpStream,
        budget: Duration,
        stop: Arc<AtomicBool>,
    ) -> std::io::Result<Self> {
        socket.set_nonblocking(false)?;
        Ok(Self {
            socket,
            deadline: Instant::now() + budget,
            stop,
            read_left: 128 * 1024,
            write_left: 128 * 1024,
        })
    }
    fn remaining(&self) -> std::io::Result<Duration> {
        if self.stop.load(Ordering::Relaxed) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "PAIRING_CANCELLED",
            ));
        }
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "PAIRING_ABSOLUTE_DEADLINE",
            ));
        }
        Ok(remaining.min(Duration::from_millis(100)))
    }
    pub(crate) fn stage(&mut self, budget: Duration, total: Instant) {
        self.deadline = (Instant::now() + budget).min(total);
        self.limit(runtime::MAX_FRAME + 1024 * 1024);
    }
    pub(crate) fn limit(&mut self, maximum: usize) {
        self.read_left = maximum;
        self.write_left = maximum;
    }
}
impl Read for BudgetSocket {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if self.read_left == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "PAIRING_IO_BUDGET",
                ));
            }
            self.socket.set_read_timeout(Some(self.remaining()?))?;
            let count = bytes.len().min(self.read_left);
            match self.socket.read(&mut bytes[..count]) {
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    continue;
                }
                Ok(count) => {
                    self.read_left -= count;
                    return Ok(count);
                }
                Err(error) => return Err(error),
            }
        }
    }
}
impl Write for BudgetSocket {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        loop {
            if self.write_left == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "PAIRING_IO_BUDGET",
                ));
            }
            self.socket.set_write_timeout(Some(self.remaining()?))?;
            match self
                .socket
                .write(&bytes[..bytes.len().min(self.write_left)])
            {
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    continue;
                }
                Ok(count) => {
                    self.write_left -= count;
                    return Ok(count);
                }
                Err(error) => return Err(error),
            }
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.remaining()?;
        self.socket.flush()
    }
}
type Client = rustls::StreamOwned<rustls::ClientConnection, BudgetSocket>;
fn cancelled(stop: &AtomicBool) -> Result<()> {
    ensure!(!stop.load(Ordering::Relaxed), "PAIRING_CANCELLED");
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub format_version: u32,
    pub installation: String,
    pub data_directory: PathBuf,
    pub credential: CredentialSource,
    #[serde(default)]
    pub transport: Transport,
    #[serde(default = "yes")]
    pub inbound: bool,
    #[serde(default = "yes")]
    pub dial: bool,
    #[serde(default)]
    pub listen: String,
    #[serde(default)]
    pub enrollment_listen: String,
    #[serde(default)]
    pub advertise: String,
    #[serde(default)]
    pub enrollment_advertise: String,
}
fn yes() -> bool {
    true
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Transport {
    #[default]
    Tls,
    Wss,
}
impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let path = std::fs::canonicalize(path)
            .context("PAIRING_CONFIG_NOT_FOUND: use -f or cwd pairing.toml")?;
        let mut config: Self =
            toml::from_str(&std::fs::read_to_string(&path)?).context("INVALID_PAIRING_CONFIG")?;
        ensure!(
            config.format_version == 1,
            "UNSUPPORTED_PAIRING_CONFIG_VERSION"
        );
        uuid::Uuid::parse_str(&config.installation)?;
        if config.transport == Transport::Wss {
            crate::wss_transport::validate_config(&config)?;
        } else {
            for listen in [
                &config.listen,
                &config.enrollment_listen,
                &config.advertise,
                &config.enrollment_advertise,
            ] {
                listen
                    .parse::<std::net::SocketAddr>()
                    .context("INVALID_LISTEN_ADDRESS")?;
            }
        }
        let parent = path.parent().unwrap();
        if config.data_directory.is_relative() {
            config.data_directory = parent.join(&config.data_directory);
        }
        if let CredentialSource::WindowsDpapi { file } = &mut config.credential
            && file.is_relative()
        {
            *file = parent.join(&*file);
        }
        Ok(config)
    }
    pub fn registry(&self) -> Result<Registry> {
        prepare_directory(&self.data_directory)?;
        Registry::open(
            &self.data_directory.join("pairs.sqlite"),
            &self.installation,
        )
    }
    pub fn identity(&self) -> Result<Identity> {
        self.credential.load(&self.installation)
    }
}
pub fn prepare_directory(path: &Path) -> Result<()> {
    crate::private_storage::Directory::open(path)?.validate_existing_files()?;
    Ok(())
}
pub fn local_runtime(grant: &SyncConfiguration) -> Result<Discovery> {
    let discovery: Discovery = serde_json::from_slice(
        &std::fs::read(&grant.local_runtime).context("WORKER_UNAVAILABLE")?,
    )?;
    ensure!(
        discovery.repo == grant.repo && discovery.device == grant.local_replica,
        "LOCAL_REPLICA_BINDING_MISMATCH"
    );
    #[cfg(windows)]
    ensure!(
        discovery.address.starts_with("pipe:"),
        "WORKER_REQUIRES_OWNER_PIPE"
    );
    #[cfg(target_os = "linux")]
    ensure!(
        discovery.address.starts_with("unix:"),
        "WORKER_REQUIRES_OWNER_SOCKET"
    );
    Ok(discovery)
}
pub fn worker(
    grant: &SyncConfiguration,
    op: &str,
    page: Option<Value>,
    request: Option<&str>,
    sent: Option<Value>,
) -> Result<Value> {
    worker_bounded(grant, op, page, request, sent, runtime::MAX_FRAME)
}
fn worker_bounded(
    grant: &SyncConfiguration,
    op: &str,
    page: Option<Value>,
    request: Option<&str>,
    sent: Option<Value>,
    maximum: usize,
) -> Result<Value> {
    runtime::rpc(
        &local_runtime(grant)?,
        &id(),
        json!({"op":op,"grant":grant,"page":page,"request":request,"sent":sent,"max_frame":maximum}),
    )
}
fn frame<T: Serialize>(stream: &mut impl Write, value: &T) -> Result<()> {
    let bytes = Zeroizing::new(serde_json::to_vec(value)?);
    runtime::send_frame(stream, &bytes)
}
fn read<T: DeserializeOwned>(stream: &mut impl Read, maximum: usize) -> Result<T> {
    let mut len = [0; 4];
    stream.read_exact(&mut len)?;
    let count = u32::from_be_bytes(len) as usize;
    ensure!(count <= maximum, "PAIRING_FRAME_TOO_LARGE");
    let mut bytes = Zeroizing::new(vec![0; count]);
    stream.read_exact(&mut bytes)?;
    serde_json::from_slice(&bytes).context("INVALID_PAIRING_REQUEST")
}
fn client(identity: &Identity, peer: &Peer, enrollment: bool) -> Result<Client> {
    client_budget(
        identity,
        peer,
        enrollment,
        Arc::new(AtomicBool::new(false)),
        Limits::default(),
    )
}
fn client_budget(
    identity: &Identity,
    peer: &Peer,
    enrollment: bool,
    stop: Arc<AtomicBool>,
    limits: Limits,
) -> Result<Client> {
    ensure!(!peer.revoked, "PAIR_REVOKED");
    let mut config = (*identity.tls_client(peer)?).clone();
    config.alpn_protocols = vec![if enrollment {
        b"tkfs-enrollment-v1".to_vec()
    } else {
        b"tkfs-published-sync-v1".to_vec()
    }];
    let name =
        rustls::pki_types::ServerName::try_from(format!("{}.tkfs.invalid", peer.installation))?;
    let mut conn = rustls::ClientConnection::new(Arc::new(config), name)?;
    cancelled(&stop)?;
    let frame_budget = if enrollment {
        limits.enrollment
    } else {
        limits.sync
    };
    let total = Instant::now() + limits.handshake + frame_budget;
    let mut socket = BudgetSocket::new(runtime::stream(&peer.endpoint)?, limits.handshake, stop)?;
    while conn.is_handshaking() {
        conn.complete_io(&mut socket)?;
    }
    let protocol = if enrollment {
        b"tkfs-enrollment-v1".as_slice()
    } else {
        b"tkfs-published-sync-v1".as_slice()
    };
    ensure!(
        conn.alpn_protocol() == Some(protocol),
        "PAIRING_PROTOCOL_MISMATCH"
    );
    ensure!(
        conn.peer_certificates()
            .and_then(|certificates| certificates.first())
            .is_some_and(|certificate| certificate.as_ref() == peer.certificate),
        "PAIR_KEY_MISMATCH"
    );
    socket.stage(frame_budget, total);
    Ok(rustls::StreamOwned::new(conn, socket))
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Enrollment {
    pub(crate) invitation: Invitation,
    pub(crate) candidate: Peer,
}
pub fn join(config: &Config, invitation: Invitation) -> Result<Value> {
    join_with_identity(config, &config.identity()?, invitation)
}
fn join_with_identity(
    config: &Config,
    identity: &Identity,
    invitation: Invitation,
) -> Result<Value> {
    ensure!(
        crate::core::time() < invitation.expires_ms,
        "INVITATION_EXPIRED"
    );
    ensure!(
        identity.installation != invitation.issuer.installation,
        "SELF_PAIR_REJECTED"
    );
    let mut issuer = invitation.issuer.clone();
    let id = invitation.id.clone();
    issuer.endpoint = invitation.enrollment_endpoint.clone();
    let candidate = Peer {
        installation: identity.installation.clone(),
        certificate: identity.certificate.clone(),
        endpoint: config.advertise.clone(),
        revoked: false,
    };
    let trusted = invitation.issuer.clone();
    let enrollment = Enrollment {
        invitation,
        candidate,
    };
    let response: Value = if issuer.endpoint.starts_with("wss://") {
        crate::wss_transport::enroll(identity, &issuer, &enrollment)?
    } else {
        let mut stream = client(identity, &issuer, true)?;
        frame(&mut stream, &enrollment)?;
        read(&mut stream, 64 * 1024)?
    };
    ensure!(
        response["ok"] == true,
        "ENROLLMENT_REJECTED: {}",
        response["error"]
    );
    config.registry()?.trust(&trusted)?;
    Ok(
        json!({"invitation":id,"issuer":trusted.installation,"fingerprint":trusted.fingerprint(),"status":"awaiting-issuer-local-approval"}),
    )
}
pub fn remote_list(config: &Config, peer_id: &str) -> Result<Value> {
    let registry = config.registry()?;
    let peer = registry
        .peers()?
        .into_iter()
        .find(|peer| peer.installation == peer_id && !peer.revoked)
        .context("PAIR_NOT_FOUND_OR_REVOKED")?;
    if peer.endpoint.starts_with("wss://") {
        let response = crate::wss_transport::request(
            &config.identity()?,
            &peer,
            &NetworkRequest::ListPublished {},
            None,
            Arc::new(AtomicBool::new(false)),
        )?;
        let _access = registry.begin_access()?;
        registry.authenticate(&peer.certificate, &peer.installation)?;
        ensure!(
            response["ok"] == true,
            "PUBLISHED_LIST_DENIED: {}",
            response["error"]
        );
        return Ok(response["result"].clone());
    }
    let mut stream = client(&config.identity()?, &peer, false)?;
    {
        let _access = registry.begin_access()?;
        registry.authenticate(&peer.certificate, &peer.installation)?;
        frame(&mut stream, &NetworkRequest::ListPublished {})?;
        stream.flush()?;
    }
    let response: Value = read(&mut stream, runtime::MAX_FRAME)?;
    ensure!(
        response["ok"] == true,
        "PUBLISHED_LIST_DENIED: {}",
        response["error"]
    );
    let _access = registry.begin_access()?;
    registry.authenticate(&peer.certificate, &peer.installation)?;
    Ok(response["result"].clone())
}
pub fn synchronize(config: &Config, grant: &SyncConfiguration) -> Result<Value> {
    synchronize_with_identity(config, &config.identity()?, grant)
}
fn synchronize_with_identity(
    config: &Config,
    identity: &Identity,
    grant: &SyncConfiguration,
) -> Result<Value> {
    synchronize_budget(
        config,
        identity,
        grant,
        Arc::new(AtomicBool::new(false)),
        Limits::default(),
        None,
    )
}
pub(crate) fn synchronize_wss(
    config: &Config,
    identity: &Identity,
    grant: &SyncConfiguration,
    stop: Arc<AtomicBool>,
    pool: &crate::wss_transport::Pool,
) -> Result<Value> {
    synchronize_budget(config, identity, grant, stop, Limits::default(), Some(pool))
}
fn synchronize_budget(
    config: &Config,
    identity: &Identity,
    grant: &SyncConfiguration,
    stop: Arc<AtomicBool>,
    limits: Limits,
    pool: Option<&crate::wss_transport::Pool>,
) -> Result<Value> {
    cancelled(&stop)?;
    let registry = config.registry()?;
    let current = registry.grant(&grant.peer, &grant.repo)?;
    ensure!(
        serde_json::to_value(&current)? == serde_json::to_value(grant)?,
        "STALE_REPOSITORY_GRANT"
    );
    let peer = registry
        .peers()?
        .into_iter()
        .find(|peer| peer.installation == grant.peer && !peer.revoked)
        .context("PAIR_REVOKED")?;
    let page: SyncPage = {
        let _access = registry.begin_access()?;
        registry.authenticate(&peer.certificate, &peer.installation)?;
        serde_json::from_value(worker_bounded(
            grant,
            "published-offer",
            None,
            None,
            None,
            if peer.endpoint.starts_with("wss://") {
                crate::wss_transport::MAX_PAGE
            } else {
                runtime::MAX_FRAME
            },
        )?)?
    };
    let request = page.request.clone();
    let sent: Vec<_> = page
        .bundle
        .events
        .iter()
        .map(|event| event.id.clone())
        .collect();
    cancelled(&stop)?;
    let network_request = NetworkRequest::Exchange {
        repo: grant.repo.clone(),
        page: Box::new(page),
    };
    let response: Value = if peer.endpoint.starts_with("wss://") {
        {
            let _access = registry.begin_access()?;
            registry.authenticate(&peer.certificate, &peer.installation)?;
        }
        crate::wss_transport::authorized_request(
            config,
            identity,
            &peer,
            &network_request,
            pool,
            stop.clone(),
            grant,
        )?
    } else {
        let mut stream = client_budget(identity, &peer, false, stop.clone(), limits)?;
        {
            let _access = registry.begin_access()?;
            registry.authenticate(&peer.certificate, &peer.installation)?;
            ensure!(
                serde_json::to_value(registry.grant(&grant.peer, &grant.repo)?)?
                    == serde_json::to_value(grant)?,
                "STALE_REPOSITORY_GRANT"
            );
            frame(&mut stream, &network_request)?;
            stream.flush()?;
        }
        read(&mut stream, runtime::MAX_FRAME)?
    };
    ensure!(response["ok"] == true, "SYNC_DENIED: {}", response["error"]);
    cancelled(&stop)?;
    let _access = registry.begin_access()?;
    registry.authenticate(&peer.certificate, &peer.installation)?;
    ensure!(
        serde_json::to_value(registry.grant(&grant.peer, &grant.repo)?)?
            == serde_json::to_value(grant)?,
        "STALE_REPOSITORY_GRANT"
    );
    worker(
        grant,
        "published-apply",
        Some(response["result"].clone()),
        Some(&request),
        Some(json!(sent)),
    )
}
pub(crate) fn authenticated(registry: &Registry, certificate: &[u8]) -> Result<Peer> {
    let peer = registry
        .peers()?
        .into_iter()
        .find(|peer| peer.certificate == certificate)
        .context("PAIR_NOT_FOUND")?;
    registry.authenticate(certificate, &peer.installation)
}
pub(crate) fn dispatch(
    registry: &Registry,
    peer: &Peer,
    request: NetworkRequest,
    stop: &AtomicBool,
    maximum: usize,
) -> Result<Value> {
    cancelled(stop)?;
    match request {
        NetworkRequest::ListPublished {} => {
            let mut states = vec![];
            for grant in registry
                .configurations()?
                .into_iter()
                .filter(|grant| grant.peer == peer.installation && grant.enabled)
            {
                cancelled(stop)?;
                let value = worker(&grant, "published-summary", None, None, None)?;
                let state: PublishedState = serde_json::from_value(value)?;
                if !state.branches.is_empty() {
                    states.push(state);
                }
            }
            Ok(json!(states))
        }
        NetworkRequest::Exchange { repo, page } => {
            let grant = registry.grant(&peer.installation, &repo)?;
            ensure!(
                page.sender == grant.remote_replica && page.receiver == grant.local_replica,
                "REMOTE_REPLICA_BINDING_MISMATCH"
            );
            worker_bounded(
                &grant,
                "published-exchange",
                Some(json!(page)),
                None,
                None,
                maximum,
            )
        }
    }
}
fn accepted(
    config: &Config,
    identity: &Identity,
    socket: TcpStream,
    enrollment: bool,
    stop: Arc<AtomicBool>,
    limits: Limits,
) -> Result<()> {
    let frame_budget = if enrollment {
        limits.enrollment
    } else {
        limits.sync
    };
    let total = Instant::now() + limits.handshake + frame_budget;
    let mut socket = BudgetSocket::new(socket, limits.handshake, stop.clone())?;
    let registry = config.registry()?;
    let peers = registry.peers()?;
    let mut conn = rustls::ServerConnection::new(identity.tls_server(&peers, enrollment)?)?;
    while conn.is_handshaking() {
        conn.complete_io(&mut socket)?;
    }
    let protocol = if enrollment {
        b"tkfs-enrollment-v1".as_slice()
    } else {
        b"tkfs-published-sync-v1".as_slice()
    };
    ensure!(
        conn.alpn_protocol() == Some(protocol),
        "PAIRING_PROTOCOL_MISMATCH"
    );
    let certificate = conn
        .peer_certificates()
        .and_then(|certificates| certificates.first())
        .map(|certificate| certificate.as_ref().to_vec());
    socket.stage(frame_budget, total);
    let mut stream = rustls::StreamOwned::new(conn, socket);
    if enrollment {
        let request: Enrollment = read(&mut stream, 64 * 1024)?;
        let mut registry = config.registry()?;
        let result = registry.submit(&request.invitation, &request.candidate);
        let response = match result {
            Ok(()) => json!({"ok":true,"status":"pending-local-approval"}),
            Err(error) => json!({"ok":false,"error":format!("{error:#}")}),
        };
        frame(&mut stream, &response)?;
    } else {
        // Authorization is reloaded AFTER the TLS handshake and request read.
        // Cached TLS roots cannot bypass owner revocation on an existing session.
        let request: NetworkRequest = read(&mut stream, runtime::MAX_FRAME)?;
        let registry = config.registry()?;
        let _access = registry.begin_access()?;
        let result = (|| {
            let peer = authenticated(
                &registry,
                certificate
                    .as_deref()
                    .context("CLIENT_CERTIFICATE_REQUIRED")?,
            )?;
            dispatch(&registry, &peer, request, &stop, runtime::MAX_FRAME)
        })();
        let response = match result {
            Ok(value) => json!({"ok":true,"result":value}),
            Err(error) => json!({"ok":false,"error":format!("{error:#}")}),
        };
        frame(&mut stream, &response)?;
    }
    stream.flush()?;
    stream.conn.send_close_notify();
    let _ = stream.conn.complete_io(&mut stream.sock);
    Ok(())
}
pub struct Service {
    config: Config,
    identity: Arc<Identity>,
    sync: Option<TcpListener>,
    enrollment: Option<TcpListener>,
    _lock: std::fs::File,
    _storage: crate::private_storage::Directory,
    limits: Limits,
}
impl Service {
    pub fn open(config: Config) -> Result<Self> {
        let identity = Arc::new(config.identity()?);
        Self::open_identity(config, identity)
    }
    pub(crate) fn open_identity(config: Config, identity: Arc<Identity>) -> Result<Self> {
        ensure!(
            identity.installation == config.installation,
            "CREDENTIAL_IDENTITY_MISMATCH"
        );
        config.registry()?;
        let storage = crate::private_storage::Directory::open(&config.data_directory)?;
        storage.validate_existing_files()?;
        let path = config.data_directory.join("service.lock");
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            options.share_mode(0);
        }
        let lock = options
            .open(path)
            .context("PAIRING_SERVICE_ALREADY_RUNNING")?;
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            ensure!(
                unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
                "PAIRING_SERVICE_ALREADY_RUNNING"
            );
        }
        let sync = if config.inbound {
            Some(TcpListener::bind(&config.listen)?)
        } else {
            None
        };
        let enrollment = if config.inbound && config.transport == Transport::Tls {
            Some(TcpListener::bind(&config.enrollment_listen)?)
        } else {
            None
        };
        for listener in [&sync, &enrollment].into_iter().flatten() {
            listener.set_nonblocking(true)?;
        }
        Ok(Self {
            config,
            identity,
            sync,
            enrollment,
            _lock: lock,
            _storage: storage,
            limits: Limits::default(),
        })
    }
    pub fn addresses(&self) -> Result<(String, String)> {
        let sync = self
            .sync
            .as_ref()
            .map(TcpListener::local_addr)
            .transpose()?;
        let enrollment = self
            .enrollment
            .as_ref()
            .map(TcpListener::local_addr)
            .transpose()?;
        if self.config.transport == Transport::Wss {
            Ok((
                sync.map(|address| format!("wss://{address}/tkfs/sync"))
                    .unwrap_or_else(|| "outbound-only".into()),
                sync.map(|address| format!("wss://{address}/tkfs/enroll"))
                    .unwrap_or_else(|| "outbound-only".into()),
            ))
        } else {
            Ok((
                sync.map(|address| address.to_string())
                    .unwrap_or_else(|| "outbound-only".into()),
                enrollment
                    .map(|address| address.to_string())
                    .unwrap_or_else(|| "outbound-only".into()),
            ))
        }
    }
    pub fn run(self, stop: Arc<AtomicBool>) -> Result<()> {
        if self.config.transport == Transport::Wss {
            return crate::wss_transport::run(
                &self.config,
                &self.identity,
                self.sync.as_ref(),
                stop,
            );
        }
        let mut jobs = std::collections::BTreeMap::new();
        let mut inflight = std::collections::BTreeSet::new();
        let (finished, results) = std::sync::mpsc::channel();
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let enroll_active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        while !stop.load(Ordering::Relaxed) {
            for (listener, enrollment) in [(&self.sync, false), (&self.enrollment, true)]
                .into_iter()
                .filter_map(|(listener, enrollment)| {
                    listener.as_ref().map(|listener| (listener, enrollment))
                })
            {
                if let Ok((socket, _)) = listener.accept() {
                    let counter = if enrollment { &enroll_active } else { &active };
                    let maximum = if enrollment { 2 } else { 8 };
                    if counter.load(Ordering::Relaxed) >= maximum {
                        drop(socket);
                        continue;
                    }
                    counter.fetch_add(1, Ordering::Relaxed);
                    let counter = counter.clone();
                    let config = self.config.clone();
                    let identity = self.identity.clone();
                    let stop = stop.clone();
                    let limits = self.limits;
                    std::thread::spawn(move || {
                        struct Slot(Arc<std::sync::atomic::AtomicUsize>);
                        impl Drop for Slot {
                            fn drop(&mut self) {
                                self.0.fetch_sub(1, Ordering::Relaxed);
                            }
                        }
                        let _slot = Slot(counter);
                        let _ = accepted(&config, &identity, socket, enrollment, stop, limits);
                    });
                }
            }
            let registry = self.config.registry()?;
            let grants = registry.configurations()?;
            let approved: std::collections::BTreeSet<_> = registry
                .peers()?
                .into_iter()
                .filter(|peer| !peer.revoked)
                .map(|peer| peer.installation)
                .collect();
            while let Ok((grant, result)) = results.try_recv() {
                let grant: SyncConfiguration = grant;
                let result: Result<Value> = result;
                let key = (grant.peer.clone(), grant.repo.clone());
                inflight.remove(&key);
                let job = jobs
                    .entry(key)
                    .or_insert((Instant::now(), Duration::from_secs(1)));
                let status = match &result {
                    Ok(value) => serde_json::to_string(value)?,
                    Err(error) => format!("unavailable: {error:#}"),
                };
                self.config.registry()?.sync_status(&grant, &status)?;
                job.1 = if result.is_ok() {
                    Duration::from_secs(1)
                } else {
                    (job.1 * 2).min(Duration::from_secs(60))
                };
                job.0 = Instant::now() + job.1;
            }
            for grant in grants
                .into_iter()
                .filter(|grant| self.config.dial && grant.enabled && approved.contains(&grant.peer))
            {
                let key = (grant.peer.clone(), grant.repo.clone());
                let job = jobs
                    .entry(key)
                    .or_insert((Instant::now(), Duration::from_secs(1)));
                let key = (grant.peer.clone(), grant.repo.clone());
                if Instant::now() < job.0 || inflight.contains(&key) || inflight.len() >= 8 {
                    continue;
                }
                inflight.insert(key);
                let finished = finished.clone();
                let config = self.config.clone();
                let identity = self.identity.clone();
                let stop = stop.clone();
                let limits = self.limits;
                std::thread::spawn(move || {
                    let result = synchronize_budget(&config, &identity, &grant, stop, limits, None);
                    let _ = finished.send((grant, result));
                });
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        // Stop interrupts socket IO within a poll interval; local worker RPCs
        // retain their own fixed timeout. Never wait indefinitely for a peer.
        let drain = Instant::now() + self.limits.shutdown;
        while active.load(Ordering::Relaxed) > 0
            || enroll_active.load(Ordering::Relaxed) > 0
            || !inflight.is_empty()
        {
            ensure!(Instant::now() < drain, "PAIRING_SHUTDOWN_DEADLINE");
            while let Ok((grant, _)) = results.try_recv() {
                inflight.remove(&(grant.peer, grant.repo));
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        Ok(())
    }
}

/// Invitation paste does not echo or enter argv/shell history. Noninteractive
/// stdin is supported for pipelines; the application never writes it to disk.
pub fn read_invitation() -> Result<Invitation> {
    #[cfg(target_os = "linux")]
    let saved = {
        let mut old = std::mem::MaybeUninit::<libc::termios>::uninit();
        if unsafe { libc::tcgetattr(0, old.as_mut_ptr()) } == 0 {
            let old = unsafe { old.assume_init() };
            let mut changed = old;
            changed.c_lflag &= !(libc::ECHO | libc::ECHONL);
            ensure!(
                unsafe { libc::tcsetattr(0, libc::TCSANOW, &changed) } == 0,
                "MASKED_INPUT_UNAVAILABLE"
            );
            Some(old)
        } else {
            None
        }
    };
    #[cfg(windows)]
    let saved = {
        let (handle, mode) = console_mode();
        if let Some(mode) = mode {
            ensure!(
                set_console_mode(handle, mode & !4),
                "MASKED_INPUT_UNAVAILABLE"
            );
        }
        (handle, mode)
    };
    eprint!("Paste invitation (input hidden): ");
    use std::io::BufRead;
    let mut input = Zeroizing::new(String::new());
    let result = std::io::stdin()
        .lock()
        .take(128 * 1024)
        .read_line(&mut input);
    #[cfg(target_os = "linux")]
    if let Some(old) = saved {
        unsafe { libc::tcsetattr(0, libc::TCSANOW, &old) };
    }
    #[cfg(windows)]
    if let Some(mode) = saved.1 {
        set_console_mode(saved.0, mode);
    }
    eprintln!();
    result?;
    ensure!(input.len() < 128 * 1024, "INVITATION_TOO_LARGE");
    let encoded = Zeroizing::new(hex::decode(input.trim()).context("INVALID_INVITATION")?);
    serde_json::from_slice(&encoded).context("INVALID_INVITATION")
}
#[cfg(windows)]
fn console_mode() -> (*mut std::ffi::c_void, Option<u32>) {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetStdHandle(id: u32) -> *mut std::ffi::c_void;
        fn GetConsoleMode(handle: *mut std::ffi::c_void, mode: *mut u32) -> i32;
    }
    let handle = unsafe { GetStdHandle((-10i32) as u32) };
    let mut mode = 0;
    let success = unsafe { GetConsoleMode(handle, &mut mode) };
    (handle, (success != 0).then_some(mode))
}
#[cfg(windows)]
fn set_console_mode(handle: *mut std::ffi::c_void, mode: u32) -> bool {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn SetConsoleMode(handle: *mut std::ffi::c_void, mode: u32) -> i32;
    }
    unsafe { SetConsoleMode(handle, mode) != 0 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{core::Store, runtime::Engine};
    use std::sync::Mutex;
    struct Running {
        stop: Arc<AtomicBool>,
        thread: Option<std::thread::JoinHandle<Result<()>>>,
    }
    impl Drop for Running {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            if let Some(thread) = self.thread.take() {
                thread.join().unwrap().unwrap();
            }
        }
    }
    fn fixture_config(path: &Path, identity: &Identity) -> Config {
        Config {
            format_version: 1,
            transport: Transport::Tls,
            inbound: true,
            dial: true,
            installation: identity.installation.clone(),
            data_directory: path.into(),
            credential: CredentialSource::WindowsDpapi {
                file: path.join("unused-fixture"),
            },
            listen: "127.0.0.1:0".into(),
            enrollment_listen: "127.0.0.1:0".into(),
            advertise: "127.0.0.1:0".into(),
            enrollment_advertise: "127.0.0.1:0".into(),
        }
    }
    fn start(config: Config, identity: Arc<Identity>) -> (Config, Running) {
        start_limits(config, identity, Limits::default())
    }
    fn start_limits(
        mut config: Config,
        identity: Arc<Identity>,
        limits: Limits,
    ) -> (Config, Running) {
        let mut service = Service::open_identity(config.clone(), identity).unwrap();
        service.limits = limits;
        let (sync, enrollment) = service.addresses().unwrap();
        config.listen = sync.clone();
        config.advertise = sync;
        config.enrollment_listen = enrollment.clone();
        config.enrollment_advertise = enrollment;
        service.config = config.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let thread = std::thread::spawn(move || service.run(flag));
        (
            config,
            Running {
                stop,
                thread: Some(thread),
            },
        )
    }
    fn request(identity: &Identity, peer: &Peer, request: Value) -> Result<Value> {
        let mut stream = client(identity, peer, false)?;
        frame(&mut stream, &request)?;
        read(&mut stream, runtime::MAX_FRAME)
    }
    fn endpoint(config: &Config, identity: &Identity) -> Peer {
        Peer {
            installation: identity.installation.clone(),
            certificate: identity.certificate.clone(),
            endpoint: config.advertise.clone(),
            revoked: false,
        }
    }
    #[test]
    fn slow_frame_progress_cannot_extend_absolute_budget_and_cancel_interrupts_read_exact() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let server = std::thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            let began = Instant::now();
            let mut socket = BudgetSocket::new(socket, Duration::from_millis(220), flag).unwrap();
            let result = read::<Value>(&mut socket, 65536);
            (began.elapsed(), result)
        });
        let mut sender = TcpStream::connect(address).unwrap();
        sender.write_all(&65536u32.to_be_bytes()).unwrap();
        for _ in 0..10 {
            if sender.write_all(b" ").is_err() {
                break;
            }
            std::thread::sleep(Duration::from_millis(40));
        }
        let (elapsed, result) = server.join().unwrap();
        assert!(result.is_err());
        assert!(elapsed < Duration::from_millis(500));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let sender = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (receiver, _) = listener.accept().unwrap();
        let flag = stop.clone();
        let thread = std::thread::spawn(move || {
            let mut receiver = BudgetSocket::new(receiver, Duration::from_secs(60), flag).unwrap();
            let mut byte = [0];
            receiver.read_exact(&mut byte)
        });
        stop.store(true, Ordering::Relaxed);
        let now = Instant::now();
        assert!(thread.join().unwrap().is_err());
        assert!(now.elapsed() < Duration::from_millis(500));
        drop(sender);
    }
    #[test]
    fn enrollment_trickles_cannot_take_sync_capacity_and_shutdown_cancels_stalled_tls() {
        let temp = tempfile::tempdir().unwrap();
        let ia = Arc::new(Identity::generate(&id()).unwrap());
        let ib = Identity::generate(&id()).unwrap();
        let config = fixture_config(&temp.path().join("service"), &ia);
        config
            .registry()
            .unwrap()
            .trust(&Peer {
                installation: ib.installation.clone(),
                certificate: ib.certificate.clone(),
                endpoint: "127.0.0.1:1".into(),
                revoked: false,
            })
            .unwrap();
        let limits = Limits {
            handshake: Duration::from_millis(500),
            enrollment: Duration::from_millis(900),
            sync: Duration::from_secs(2),
            shutdown: Duration::from_secs(2),
        };
        let (config, running) = start_limits(config, ia.clone(), limits);
        let peer = Peer {
            endpoint: config.enrollment_advertise.clone(),
            ..endpoint(&config, &ia)
        };
        let mut trickles = vec![];
        for _ in 0..2 {
            let mut socket = client(&ib, &peer, true).unwrap();
            socket.write_all(&65536u32.to_be_bytes()).unwrap();
            socket.flush().unwrap();
            trickles.push(std::thread::spawn(move || {
                let began = Instant::now();
                for _ in 0..40 {
                    if socket.write_all(b" ").and_then(|_| socket.flush()).is_err() {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(40));
                }
                began.elapsed()
            }));
        }
        let flood: Vec<_> = (0..8)
            .map(|_| TcpStream::connect(&config.enrollment_listen).unwrap())
            .collect();
        std::thread::sleep(Duration::from_millis(600));
        let began = Instant::now();
        let response =
            request(&ib, &endpoint(&config, &ia), json!({"op":"list-published"})).unwrap();
        assert_eq!(response["ok"], true);
        assert!(began.elapsed() < Duration::from_millis(800));
        for thread in trickles {
            assert!(thread.join().unwrap() < Duration::from_millis(1500));
        }
        // Stalled unauthenticated handshakes are cancelled rather than drained forever.
        let stalled = TcpStream::connect(&config.enrollment_listen).unwrap();
        let began = Instant::now();
        drop(running);
        assert!(began.elapsed() < Duration::from_secs(1));
        drop(stalled);
        drop(flood);
    }
    #[test]
    fn actual_tls_enrollment_grants_sync_resume_and_revoke_existing_connection() {
        let temp = tempfile::tempdir().unwrap();
        let ia = Arc::new(Identity::generate(&id()).unwrap());
        let ib = Arc::new(Identity::generate(&id()).unwrap());
        let (ca, run_a) = start(
            fixture_config(&temp.path().join("service-a"), &ia),
            ia.clone(),
        );
        let (cb, run_b) = start(
            fixture_config(&temp.path().join("service-b"), &ib),
            ib.clone(),
        );
        let mut ra = ca.registry().unwrap();
        let invitation = ra
            .create_invitation(&ia, &ca.enrollment_advertise, &ca.advertise, 10000)
            .unwrap();
        let invitation_id = invitation.id.clone();
        let duplicate = Zeroizing::new(serde_json::to_vec(&invitation).unwrap());
        join_with_identity(&cb, &ib, invitation).unwrap();
        assert!(request(&ib, &endpoint(&ca, &ia), json!({"op":"list-published"})).is_err());
        assert!(join_with_identity(&cb, &ib, serde_json::from_slice(&duplicate).unwrap()).is_err());
        ra.approve(&invitation_id).unwrap();
        let impostor = Identity::generate(&ib.installation).unwrap();
        assert!(
            request(
                &impostor,
                &endpoint(&ca, &ia),
                json!({"op":"list-published"})
            )
            .is_err()
        );
        let stranger = Identity::generate(&id()).unwrap();
        assert!(
            request(
                &stranger,
                &endpoint(&ca, &ia),
                json!({"op":"list-published"})
            )
            .is_err()
        );
        assert_eq!(
            request(&ib, &endpoint(&ca, &ia), json!({"op":"list-published"})).unwrap()["result"],
            json!([])
        );
        for op in [
            "create",
            "start",
            "stop",
            "checkout",
            "invite",
            "approve",
            "revoke",
            "cat-object",
            "shell",
        ] {
            assert!(request(&ib, &endpoint(&ca, &ia), json!({"op":op})).is_err());
        }
        let repo = id();
        let da = id();
        let db = id();
        let state_a = temp.path().join("worker-a");
        let state_b = temp.path().join("worker-b");
        let a = Arc::new(Mutex::new(Engine::new(
            Store::initialize(&state_a, &repo, &da).unwrap(),
        )));
        let b = Arc::new(Mutex::new(Engine::new(
            Store::initialize(&state_b, &repo, &db).unwrap(),
        )));
        runtime::start_rpc(a.clone(), &state_a, None).unwrap();
        runtime::start_rpc(b.clone(), &state_b, None).unwrap();
        {
            let mut a = a.lock().unwrap();
            let file = a.store.create("published.txt", "file").unwrap();
            a.store
                .write_revision(&file.id, None, b"wire verified")
                .unwrap();
            a.store.fork("private", false).unwrap();
            a.store.checkout("private").unwrap();
            let secret = a.store.create("secret.txt", "file").unwrap();
            a.store
                .write_revision(&secret.id, None, b"not on wire")
                .unwrap();
        }
        let ga = SyncConfiguration {
            peer: ib.installation.clone(),
            repo: repo.clone(),
            local_runtime: state_a.join("runtime.json"),
            local_replica: da.clone(),
            remote_replica: db.clone(),
            enabled: true,
        };
        let gb = SyncConfiguration {
            peer: ia.installation.clone(),
            repo: repo.clone(),
            local_runtime: state_b.join("runtime.json"),
            local_replica: db,
            remote_replica: da,
            enabled: true,
        };
        ca.registry().unwrap().configure_sync(&ga).unwrap();
        cb.registry().unwrap().configure_sync(&gb).unwrap();
        let list = request(&ib, &endpoint(&ca, &ia), json!({"op":"list-published"})).unwrap();
        assert_eq!(list["result"].as_array().unwrap().len(), 1);
        assert!(!serde_json::to_string(&list).unwrap().contains("private"));
        assert!(!serde_json::to_string(&list).unwrap().contains("worker-a"));
        let deadline = Instant::now() + Duration::from_secs(15);
        while b.lock().unwrap().store.lookup("published.txt").is_err() {
            assert!(Instant::now() < deadline, "configured sync did not run");
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(b.lock().unwrap().store.lookup("secret.txt").is_err());
        assert!(
            !b.lock()
                .unwrap()
                .store
                .shared_object_inventory()
                .unwrap()
                .contains(&crate::core::hash(b"not on wire"))
        );
        let mut wrong: SyncPage =
            serde_json::from_value(worker(&gb, "published-offer", None, None, None).unwrap())
                .unwrap();
        wrong.sender = id();
        let response = request(
            &ib,
            &endpoint(&ca, &ia),
            json!({"op":"exchange","repo":repo,"page":wrong}),
        )
        .unwrap();
        assert_eq!(response["ok"], false);
        let response = request(
            &ib,
            &endpoint(&ca, &ia),
            json!({"op":"exchange","repo":id(),"page":wrong}),
        )
        .unwrap();
        assert_eq!(response["ok"], false);
        drop(run_a);
        drop(run_b);
        {
            let mut a = a.lock().unwrap();
            a.store.checkout("main").unwrap();
            let file = a.store.create("after-restart.txt", "file").unwrap();
            a.store.write_revision(&file.id, None, b"resume").unwrap();
        }
        let (ca, run_a) = start(ca, ia.clone());
        let (_cb, run_b) = start(cb, ib.clone());
        let deadline = Instant::now() + Duration::from_secs(15);
        while b.lock().unwrap().store.lookup("after-restart.txt").is_err() {
            assert!(Instant::now() < deadline, "persisted sync did not resume");
            std::thread::sleep(Duration::from_millis(50));
        }
        // Complete mTLS now, revoke BEFORE this established connection asks for data.
        let mut established = client(&ib, &endpoint(&ca, &ia), false).unwrap();
        ca.registry().unwrap().revoke(&ib.installation).unwrap();
        frame(&mut established, &NetworkRequest::ListPublished {}).unwrap();
        let denied: Value = read(&mut established, 64 * 1024).unwrap();
        assert_eq!(denied["ok"], false);
        assert!(denied["error"].as_str().unwrap().contains("REVOKED"));
        assert!(request(&ib, &endpoint(&ca, &ia), json!({"op":"list-published"})).is_err());
        assert!(synchronize_with_identity(&ca, &ia, &ga).is_err());
        let impostor = Identity::generate(&ib.installation).unwrap();
        assert!(
            request(
                &impostor,
                &endpoint(&ca, &ia),
                json!({"op":"list-published"})
            )
            .is_err()
        );
        drop(run_a);
        drop(run_b);
    }
}
