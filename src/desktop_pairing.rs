//! Owner-local desktop pairing adapter. Invitation secrets are ephemeral; the
//! only durable credential source is the existing protected platform backend.
use crate::{
    core::id,
    pairing::{CredentialSource, Identity, Invitation, PublishedState, SyncConfiguration},
    pairing_service::{self, Config, Transport},
    private_storage::Directory,
    runtime::Discovery,
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicBool},
};
use zeroize::Zeroizing;

#[derive(Clone, Default)]
pub struct Snapshot {
    pub ready: bool,
    pub identity: String,
    pub review: String,
    pub peers: Vec<(String, String, bool)>,
    pub published: Vec<PublishedState>,
    pub peer: String,
    pub syncs: String,
    pub local: String,
    pub grants: Vec<(String, String, bool, bool)>,
}
pub struct Client {
    path: PathBuf,
    config: Config,
    invitation: Option<Invitation>,
    expected_endpoint: String,
    snapshot: Snapshot,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Setup {
    installation: String,
    config: PathBuf,
}
fn publish_config(path: &Path, bytes: &[u8], directory: &mut Directory) -> Result<()> {
    use std::os::windows::{ffi::OsStrExt, fs::OpenOptionsExt, io::AsRawHandle};
    #[repr(C)]
    struct RenameInfo {
        flags: u32,
        root: std::os::windows::io::RawHandle,
        length: u32,
        name: [u16; 1],
    }
    #[repr(C)]
    struct IoStatus {
        status: usize,
        information: usize,
    }
    #[link(name = "ntdll")]
    unsafe extern "system" {
        fn NtSetInformationFile(
            file: std::os::windows::io::RawHandle,
            status: *mut IoStatus,
            info: *const std::ffi::c_void,
            size: u32,
            class: u32,
        ) -> i32;
        fn RtlNtStatusToDosError(status: i32) -> u32;
    }
    // Prepare bytes with the ordinary namespace pins held. Publication then
    // reserves and verifies the same directory identity and renames relative
    // to that handle; no absolute path is resolved during publication.
    let temporary = path.with_extension(format!("{}.tmp", id()));
    let result = (|| -> Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .access_mode(0x40010000)
            .share_mode(1)
            .open(&temporary)
            .context("PAIRING_CONFIG_TEMP_CREATE")?;
        crate::core::fault("desktop_pairing_config_opened");
        let middle = bytes.len() / 2;
        file.write_all(&bytes[..middle])?;
        crate::core::fault("desktop_pairing_config_write_started");
        file.write_all(&bytes[middle..])?;
        file.sync_all()?;
        crate::core::fault("desktop_pairing_config_ready");
        let parent = directory.reserve_publication()?;
        let name: Vec<_> = path
            .file_name()
            .context("PAIRING_CONFIG_FILENAME_REQUIRED")?
            .encode_wide()
            .collect();
        let size = std::mem::size_of::<RenameInfo>() + name.len() * 2;
        let mut buffer = vec![0u64; size.div_ceil(8)];
        let info = buffer.as_mut_ptr().cast::<RenameInfo>();
        // The u64 allocation provides HANDLE alignment and space for the
        // variable UTF-16 tail. Flags=0 refuses every existing destination.
        unsafe {
            (*info).flags = 0;
            (*info).root = parent.as_raw_handle();
            (*info).length = (name.len() * 2) as u32;
            std::ptr::copy_nonoverlapping(
                name.as_ptr(),
                std::ptr::addr_of_mut!((*info).name).cast(),
                name.len(),
            );
        }
        let mut ios = IoStatus {
            status: 0,
            information: 0,
        };
        let status = unsafe {
            NtSetInformationFile(file.as_raw_handle(), &mut ios, info.cast(), size as u32, 10)
        };
        ensure!(
            status >= 0,
            "PAIRING_CONFIG_CREATE_ONLY_PUBLISH: {}",
            std::io::Error::from_raw_os_error(unsafe { RtlNtStatusToDosError(status) } as i32)
        );
        file.sync_all()?;
        Ok(())
    })();
    // On failure retain the owner-private public temporary file. Resolving its
    // absolute path for cleanup after the reservation would reopen the parent
    // namespace. Successful rename already removes its temporary name.
    result
}
fn client_config(config: &Config) -> Result<()> {
    ensure!(
        config.transport == Transport::Wss && !config.inbound && config.dial,
        "DESKTOP_OUTBOUND_WSS_REQUIRED"
    );
    Ok(())
}
impl Client {
    pub fn open(path: &Path) -> Result<Self> {
        let config = Config::load(path)?;
        client_config(&config)?;
        let mut client = Self {
            path: std::fs::canonicalize(path)?,
            config,
            invitation: None,
            expected_endpoint: String::new(),
            snapshot: Snapshot::default(),
        };
        client.refresh()?;
        Ok(client)
    }
    pub fn create(path: &Path) -> Result<Self> {
        ensure!(path.is_absolute(), "PAIRING_ABSOLUTE_CONFIG_PATH_REQUIRED");
        let parent = path.parent().context("PAIRING_DIRECTORY_REQUIRED")?;
        let mut directory = Directory::open(parent)?;
        let _guard = crate::desktop::exclusive(&parent.join("client-setup.lock"))?;
        ensure!(!path.try_exists()?, "PAIRING_CONFIG_ALREADY_EXISTS");
        let setup_path = parent.join("client-setup.json");
        let setup: Setup = if setup_path.try_exists()? {
            let _file = directory.file(&setup_path, false, false)?;
            serde_json::from_slice(&std::fs::read(&setup_path)?)?
        } else {
            let setup = Setup {
                installation: id(),
                config: path.into(),
            };
            let mut file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&setup_path)?;
            file.write_all(&serde_json::to_vec(&setup)?)?;
            file.sync_all()?;
            setup
        };
        ensure!(setup.config == path, "PAIRING_CONFIG_CHANGED");
        uuid::Uuid::parse_str(&setup.installation)?;
        let credential = CredentialSource::WindowsDpapi {
            file: parent.join("identity.dpapi"),
        };
        // Never overwrite an existing key, or fall back to an unprotected file.
        let identity = if parent.join("identity.dpapi").try_exists()? {
            credential.load(&setup.installation)?
        } else {
            let identity = Identity::generate(&setup.installation)?;
            credential.save_windows(&identity)?;
            identity
        };
        let config = Config {
            format_version: 1,
            installation: identity.installation.clone(),
            data_directory: parent.join("registry"),
            credential,
            transport: Transport::Wss,
            inbound: false,
            dial: true,
            listen: String::new(),
            enrollment_listen: String::new(),
            advertise: "outbound-only".into(),
            enrollment_advertise: String::new(),
        };
        publish_config(
            path,
            toml::to_string_pretty(&config)?.as_bytes(),
            &mut directory,
        )?;
        drop(directory);
        Self::open(path).context("PAIRING_CLIENT_OPEN_AFTER_CREATE")
    }
    fn current(&self) -> Result<Config> {
        let config = Config::load(&self.path)?;
        ensure!(
            serde_json::to_value(&config)? == serde_json::to_value(&self.config)?,
            "PAIRING_CONFIG_CHANGED"
        );
        client_config(&config)?;
        Ok(config)
    }
    pub fn snapshot(&self) -> Snapshot {
        self.snapshot.clone()
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn refresh(&mut self) -> Result<()> {
        let config = self.current()?;
        self.snapshot.ready = false;
        let reference = match &config.credential {
            CredentialSource::WindowsDpapi { file } => {
                format!("Windows CurrentUser DPAPI · {}", file.display())
            }
            CredentialSource::Systemd { name } => format!("systemd credential · {name}"),
        };
        self.snapshot.identity = format!(
            "Installation: {}\nCredential reference: {}",
            config.installation, reference
        );
        match config.identity() {
            Ok(identity) => {
                self.snapshot.ready = true;
                self.snapshot.identity.push_str(&format!("\nLocal certificate SHA-256: {}", identity.fingerprint()));
            }
            Err(_) => self.snapshot.identity.push_str("\nCredential unavailable or unsafe. Supply the protected credential; no fallback is used."),
        }
        let registry = config.registry()?;
        self.snapshot.peers = registry
            .peers()?
            .into_iter()
            .map(|peer| (peer.installation.clone(), peer.fingerprint(), peer.revoked))
            .collect();
        self.snapshot.grants = registry
            .configurations()?
            .into_iter()
            .map(|grant| {
                let revoked = self.snapshot.peers.iter().any(|p| p.0 == grant.peer && p.2);
                (grant.peer, grant.repo, grant.enabled, revoked)
            })
            .collect();
        self.snapshot.syncs = registry
            .configurations()?
            .into_iter()
            .map(|grant| {
                let status = if self.snapshot.peers.iter().any(|p| p.0 == grant.peer && p.2) {
                    "Revoked"
                } else if grant.enabled {
                    "Enabled"
                } else {
                    "Paused"
                };
                format!(
                    "{} · {}\n{}\nLocal replica: {}\nRemote replica: {}",
                    grant.repo, status, grant.peer, grant.local_replica, grant.remote_replica
                )
            })
            .collect::<Vec<_>>()
            .join("\n\n");
        Ok(())
    }
    pub fn inspect(&mut self, token: Zeroizing<String>, endpoint: &str) -> Result<()> {
        self.invitation = None;
        self.snapshot.review.clear();
        self.current()?.identity()?;
        ensure!(token.len() <= 65536, "INVALID_INVITATION");
        let bytes = Zeroizing::new(
            hex::decode(token.trim()).map_err(|_| anyhow::anyhow!("INVALID_INVITATION"))?,
        );
        let invitation: Invitation =
            serde_json::from_slice(&bytes).map_err(|_| anyhow::anyhow!("INVALID_INVITATION"))?;
        ensure!(
            invitation.expires_ms > crate::core::time(),
            "INVITATION_EXPIRED"
        );
        ensure!(
            invitation.issuer.installation != self.config.installation
                && !invitation.issuer.revoked,
            "INVALID_INVITATION"
        );
        uuid::Uuid::parse_str(&invitation.id).context("INVALID_INVITATION")?;
        uuid::Uuid::parse_str(&invitation.issuer.installation).context("INVALID_INVITATION")?;
        ensure!(
            endpoint.starts_with("wss://") && invitation.enrollment_endpoint.starts_with("wss://"),
            "DESKTOP_WSS_REQUIRED"
        );
        let address = crate::wss_transport::endpoint(endpoint, false)?;
        ensure!(
            invitation.issuer.endpoint == endpoint
                && crate::wss_transport::endpoint(&invitation.enrollment_endpoint, true)?
                    == address,
            "INVITATION_ENDPOINT_MISMATCH"
        );
        let mut roots = rustls::RootCertStore::empty();
        ensure!(
            !invitation.issuer.certificate.is_empty()
                && invitation.issuer.certificate.len() <= 8192,
            "INVALID_INVITATION"
        );
        roots
            .add(invitation.issuer.certificate.clone().into())
            .context("INVALID_INVITATION")?;
        self.snapshot.review = format!(
            "Server installation: {}\nCertificate SHA-256: {}\nSync endpoint: {}\nEnrollment endpoint: {}\nExpires in {} seconds (checked again on submit)\nVerify this exact fingerprint with the server owner before enrollment.",
            invitation.issuer.installation,
            invitation.issuer.fingerprint(),
            invitation.issuer.endpoint,
            invitation.enrollment_endpoint,
            invitation.expires_ms.saturating_sub(crate::core::time()) / 1000
        );
        self.expected_endpoint = endpoint.into();
        self.invitation = Some(invitation);
        Ok(())
    }
    pub fn clear_invitation(&mut self) {
        self.invitation = None;
        self.snapshot.review.clear();
        self.expected_endpoint.clear();
    }
    pub fn select_peer(&mut self, peer: &str) {
        self.snapshot.peer = peer.into();
        self.snapshot.published.clear();
        self.clear_invitation();
    }
    pub fn join(
        &mut self,
        endpoint: &str,
        confirmed: bool,
        stop: Arc<AtomicBool>,
    ) -> Result<Value> {
        ensure!(confirmed, "EXACT_KEY_CONFIRMATION_REQUIRED");
        ensure!(
            endpoint == self.expected_endpoint,
            "INVITATION_ENDPOINT_MISMATCH"
        );
        let config = self.current()?;
        let invitation = self
            .invitation
            .take()
            .context("REVIEW_INVITATION_REQUIRED")?;
        self.snapshot.peer = invitation.issuer.installation.clone();
        self.snapshot.review.clear();
        // Consumed on submission. A timeout/cancel may have used the one-time
        // invitation; ask for a fresh invitation instead of silently replaying it.
        let result = pairing_service::join_cancellable(&config, invitation, stop);
        self.refresh()?;
        result
    }
    pub fn list(&mut self, peer: &str, stop: Arc<AtomicBool>) -> Result<()> {
        self.snapshot.published.clear();
        self.snapshot.peer = peer.into();
        let value = pairing_service::remote_list_cancellable(&self.current()?, peer, stop)?;
        self.snapshot.published =
            serde_json::from_value(value).context("INVALID_PUBLISHED_LIST")?;
        ensure!(
            self.snapshot
                .published
                .iter()
                .all(|state| state.branches.iter().all(|branch| branch.shared)),
            "PRIVATE_BRANCH_REJECTED"
        );
        Ok(())
    }
    pub fn configure(
        &mut self,
        peer: &str,
        repo: &str,
        runtime: &Path,
        confirmed: bool,
    ) -> Result<()> {
        ensure!(confirmed, "PUBLISHED_SYNC_CONFIRMATION_REQUIRED");
        let published = self
            .snapshot
            .published
            .iter()
            .find(|state| state.repo == repo)
            .filter(|_| self.snapshot.peer == peer)
            .context("REVIEW_PUBLISHED_REPOSITORY_REQUIRED")?;
        let config = self.current()?;
        let runtime = std::fs::canonicalize(runtime)?;
        let discovery: Discovery = serde_json::from_slice(&std::fs::read(&runtime)?)?;
        ensure!(
            discovery.format == 1 && discovery.repo == published.repo,
            "LOCAL_REPLICA_REPOSITORY_MISMATCH"
        );
        ensure!(
            std::fs::canonicalize(&discovery.state)?
                == runtime.parent().context("INVALID_LOCAL_RUNTIME")?,
            "LOCAL_RUNTIME_PATH_MISMATCH"
        );
        let grant = SyncConfiguration {
            peer: peer.into(),
            repo: repo.into(),
            local_runtime: runtime,
            local_replica: discovery.device.clone(),
            remote_replica: published.replica.clone(),
            enabled: true,
        };
        let summary = pairing_service::worker(&grant, "published-summary", None, None, None)?;
        ensure!(
            summary["repo"] == grant.repo && summary["replica"] == grant.local_replica,
            "LOCAL_REPLICA_BINDING_MISMATCH"
        );
        config.registry()?.configure_sync(&grant)?;
        self.snapshot.local = format!(
            "Local replica: {}\nMount: {}",
            discovery.device,
            discovery.mount.as_ref().cloned().unwrap_or_else(|| {
                "Headless · mounting must be configured on the worker".into()
            })
        );
        self.refresh()
    }
    pub fn enable(&mut self, peer: &str, repo: &str, enabled: bool) -> Result<()> {
        let config = self.current()?;
        let mut registry = config.registry()?;
        let mut grant = registry
            .configurations()?
            .into_iter()
            .find(|grant| grant.peer == peer && grant.repo == repo)
            .context("REPOSITORY_NOT_GRANTED")?;
        grant.enabled = enabled;
        registry.configure_sync(&grant)?;
        drop(registry);
        self.refresh()
    }
    pub fn sync(&mut self, peer: &str, repo: &str, stop: Arc<AtomicBool>) -> Result<Value> {
        let config = self.current()?;
        let grant = config.registry()?.grant(peer, repo)?;
        pairing_service::synchronize_cancellable(&config, &grant, stop)
    }
    pub fn revoke(&mut self, peer: &str, confirmed: bool) -> Result<()> {
        ensure!(confirmed, "REVOCATION_CONFIRMATION_REQUIRED");
        self.current()?.registry()?.revoke(peer)?;
        self.snapshot.published.clear();
        self.refresh()
    }
}
/// Do not echo remote error payloads, tokens, certificates or paths into logs.
pub fn error_message(error: &anyhow::Error) -> String {
    let text = format!("{error:#}");
    let message = if text.contains("CANCELLED") {
        "Stopped local pairing I/O. A submitted invitation may have been consumed and sync may have committed data. Check approval or sync again to resume; cancellation does not undo data."
    } else if text.contains("INVITATION_EXPIRED") {
        "This invitation expired. Ask for a fresh invitation."
    } else if text.contains("ENDPOINT_MISMATCH") {
        "The endpoint differs from the invitation. Review the server endpoint and exact key again."
    } else if text.contains("REPLICA_REPOSITORY_MISMATCH") {
        "This local replica belongs to a different repository. A matching running replica is required; this UI cannot provision or adopt it yet."
    } else if text.contains("CONFIRMATION_REQUIRED") {
        "Review and explicitly confirm the exact key, published-data grant or revocation first."
    } else if text.contains("CONFIG_CHANGED") {
        "The pairing configuration changed. Reload it and review the connection again."
    } else if text.contains("REVOKED") {
        "This pairing is revoked. Resume cannot restore a revoked pair."
    } else if text.contains("WSS_REQUIRED") {
        "Use an outbound-only WSS client configuration. No plaintext or transport downgrade is offered."
    } else if text.contains("INVALID_INVITATION") {
        "The invitation is invalid. Paste a fresh invitation privately; it is never saved."
    } else if text.contains("CREDENTIAL") || text.contains("PRIVATE_STORAGE") {
        "Protected credential/storage is unavailable or unsafe. Use the configured platform credential; existing permissions are never repaired automatically."
    } else {
        "Pairing operation did not complete. Check endpoint, exact-key approval, repository grants and local worker readiness. After an uncertain enrollment, ask for a fresh invitation. Committed sync data is retained and can resume."
    };
    message.into()
}
