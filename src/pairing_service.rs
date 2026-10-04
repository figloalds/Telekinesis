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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub format_version: u32,
    pub installation: String,
    pub data_directory: PathBuf,
    pub credential: CredentialSource,
    pub listen: String,
    pub enrollment_listen: String,
    pub advertise: String,
    pub enrollment_advertise: String,
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
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::{DirBuilderExt, MetadataExt};
        if !path.exists() {
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(path)?;
        }
        let metadata = std::fs::symlink_metadata(path)?;
        ensure!(
            metadata.is_dir()
                && !metadata.file_type().is_symlink()
                && metadata.uid() == unsafe { libc::geteuid() }
                && metadata.mode() & 0o077 == 0,
            "PAIRING_DIRECTORY_NOT_PRIVATE"
        );
    }
    #[cfg(windows)]
    {
        std::fs::create_dir_all(path)?;
        crate::local_ipc::protect_directory(path)?;
    }
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
    runtime::rpc(
        &local_runtime(grant)?,
        &id(),
        json!({"op":op,"grant":grant,"page":page,"request":request,"sent":sent}),
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
fn client(
    identity: &Identity,
    peer: &Peer,
    enrollment: bool,
) -> Result<rustls::StreamOwned<rustls::ClientConnection, TcpStream>> {
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
    let mut socket = runtime::stream(&peer.endpoint)?;
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
    Ok(rustls::StreamOwned::new(conn, socket))
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Enrollment {
    invitation: Invitation,
    candidate: Peer,
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
    let mut stream = client(identity, &issuer, true)?;
    let candidate = Peer {
        installation: identity.installation.clone(),
        certificate: identity.certificate.clone(),
        endpoint: config.advertise.clone(),
        revoked: false,
    };
    let trusted = invitation.issuer.clone();
    frame(
        &mut stream,
        &Enrollment {
            invitation,
            candidate,
        },
    )?;
    let response: Value = read(&mut stream, 64 * 1024)?;
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
        serde_json::from_value(worker(grant, "published-offer", None, None, None)?)?
    };
    let request = page.request.clone();
    let sent: Vec<_> = page
        .bundle
        .events
        .iter()
        .map(|event| event.id.clone())
        .collect();
    let mut stream = client(identity, &peer, false)?;
    {
        let _access = registry.begin_access()?;
        registry.authenticate(&peer.certificate, &peer.installation)?;
        ensure!(
            serde_json::to_value(registry.grant(&grant.peer, &grant.repo)?)?
                == serde_json::to_value(grant)?,
            "STALE_REPOSITORY_GRANT"
        );
        frame(
            &mut stream,
            &NetworkRequest::Exchange {
                repo: grant.repo.clone(),
                page: Box::new(page),
            },
        )?;
        stream.flush()?;
    }
    let response: Value = read(&mut stream, runtime::MAX_FRAME)?;
    ensure!(response["ok"] == true, "SYNC_DENIED: {}", response["error"]);
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
fn authenticated(registry: &Registry, certificate: &[u8]) -> Result<Peer> {
    let peer = registry
        .peers()?
        .into_iter()
        .find(|peer| peer.certificate == certificate)
        .context("PAIR_NOT_FOUND")?;
    registry.authenticate(certificate, &peer.installation)
}
fn dispatch(registry: &Registry, peer: &Peer, request: NetworkRequest) -> Result<Value> {
    match request {
        NetworkRequest::ListPublished {} => {
            let mut states = vec![];
            for grant in registry
                .configurations()?
                .into_iter()
                .filter(|grant| grant.peer == peer.installation && grant.enabled)
            {
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
            worker(&grant, "published-exchange", Some(json!(page)), None, None)
        }
    }
}
fn accepted(
    config: &Config,
    identity: &Identity,
    mut socket: TcpStream,
    enrollment: bool,
) -> Result<()> {
    socket.set_nonblocking(false)?;
    socket.set_read_timeout(Some(Duration::from_secs(10)))?;
    socket.set_write_timeout(Some(Duration::from_secs(15)))?;
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
            dispatch(&registry, &peer, request)
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
    sync: TcpListener,
    enrollment: TcpListener,
    _lock: std::fs::File,
}
impl Service {
    pub fn open(config: Config) -> Result<Self> {
        let identity = Arc::new(config.identity()?);
        Self::open_identity(config, identity)
    }
    fn open_identity(config: Config, identity: Arc<Identity>) -> Result<Self> {
        ensure!(
            identity.installation == config.installation,
            "CREDENTIAL_IDENTITY_MISMATCH"
        );
        config.registry()?;
        let path = config.data_directory.join("service.lock");
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
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
        let sync = TcpListener::bind(&config.listen)?;
        let enrollment = TcpListener::bind(&config.enrollment_listen)?;
        sync.set_nonblocking(true)?;
        enrollment.set_nonblocking(true)?;
        Ok(Self {
            config,
            identity,
            sync,
            enrollment,
            _lock: lock,
        })
    }
    pub fn addresses(&self) -> Result<(String, String)> {
        Ok((
            self.sync.local_addr()?.to_string(),
            self.enrollment.local_addr()?.to_string(),
        ))
    }
    pub fn run(self, stop: Arc<AtomicBool>) -> Result<()> {
        let mut jobs = std::collections::BTreeMap::new();
        let mut inflight = std::collections::BTreeSet::new();
        let (finished, results) = std::sync::mpsc::channel();
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        while !stop.load(Ordering::Relaxed) {
            for (listener, enrollment) in [(&self.sync, false), (&self.enrollment, true)] {
                if let Ok((socket, _)) = listener.accept() {
                    if active.load(Ordering::Relaxed) >= 8 {
                        drop(socket);
                        continue;
                    }
                    active.fetch_add(1, Ordering::Relaxed);
                    let active = active.clone();
                    let config = self.config.clone();
                    let identity = self.identity.clone();
                    std::thread::spawn(move || {
                        let result = accepted(&config, &identity, socket, enrollment);
                        let _ = result;
                        active.fetch_sub(1, Ordering::Relaxed);
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
                .filter(|grant| grant.enabled && approved.contains(&grant.peer))
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
                std::thread::spawn(move || {
                    let result = synchronize_with_identity(&config, &identity, &grant);
                    let _ = finished.send((grant, result));
                });
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        // Each accepted connection is bounded; drain own operations before lock release.
        while active.load(Ordering::Relaxed) > 0 || !inflight.is_empty() {
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
    fn start(mut config: Config, identity: Arc<Identity>) -> (Config, Running) {
        let mut service = Service::open_identity(config.clone(), identity).unwrap();
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
