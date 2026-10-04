//! Durable pairing and published-sync authorization. Local administration is
//! deliberately absent from the network request schema.
use crate::core::{hash, id, time};
use anyhow::{Context, Result, bail, ensure};
use rand::RngCore;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, Zeroizing};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub installation: String,
    pub certificate: Vec<u8>,
    private_key: Vec<u8>,
}
impl std::fmt::Debug for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Identity")
            .field("installation", &self.installation)
            .field("fingerprint", &hash(&self.certificate))
            .field("private_key", &"[REDACTED]")
            .finish()
    }
}
impl Drop for Identity {
    fn drop(&mut self) {
        self.private_key.zeroize();
    }
}
impl Identity {
    pub fn generate(installation: &str) -> Result<Self> {
        uuid::Uuid::parse_str(installation).context("INVALID_INSTALLATION_ID")?;
        let certified =
            rcgen::generate_simple_self_signed(vec![format!("{installation}.tkfs.invalid")])?;
        Ok(Self {
            installation: installation.into(),
            certificate: certified.cert.der().to_vec(),
            private_key: certified.signing_key.serialize_der(),
        })
    }
    pub fn fingerprint(&self) -> String {
        hash(&self.certificate)
    }
    fn certificate_chain(&self) -> Vec<rustls::pki_types::CertificateDer<'static>> {
        vec![self.certificate.clone().into()]
    }
    fn key(&self) -> rustls::pki_types::PrivateKeyDer<'static> {
        rustls::pki_types::PrivatePkcs8KeyDer::from(self.private_key.clone()).into()
    }
    pub fn tls_client(&self, peer: &Peer) -> Result<Arc<rustls::ClientConfig>> {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(peer.certificate.clone().into())?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut config = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_root_certificates(roots)
            .with_client_auth_cert(self.certificate_chain(), self.key())?;
        config.enable_early_data = false;
        config.alpn_protocols = vec![b"tkfs-published-sync-v1".to_vec()];
        config.resumption = rustls::client::Resumption::disabled();
        Ok(Arc::new(config))
    }
    pub fn tls_server(
        &self,
        peers: &[Peer],
        enrollment: bool,
    ) -> Result<Arc<rustls::ServerConfig>> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])?;
        let mut config = if enrollment {
            builder
                .with_no_client_auth()
                .with_single_cert(self.certificate_chain(), self.key())?
        } else {
            let mut roots = rustls::RootCertStore::empty();
            for peer in peers.iter().filter(|peer| !peer.revoked) {
                roots.add(peer.certificate.clone().into())?;
            }
            let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                Arc::new(roots),
                provider,
            )
            .build()?;
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(self.certificate_chain(), self.key())?
        };
        config.max_early_data_size = 0;
        config.alpn_protocols = vec![if enrollment {
            b"tkfs-enrollment-v1".to_vec()
        } else {
            b"tkfs-published-sync-v1".to_vec()
        }];
        config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
        config.send_tls13_tickets = 0;
        Ok(Arc::new(config))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "backend", rename_all = "kebab-case", deny_unknown_fields)]
pub enum CredentialSource {
    WindowsDpapi { file: PathBuf },
    Systemd { name: String },
}
impl CredentialSource {
    pub fn load(&self, installation: &str) -> Result<Identity> {
        let bytes = match self {
            Self::WindowsDpapi { file } => {
                #[cfg(windows)]
                {
                    let directory = crate::private_storage::Directory::open(
                        file.parent().context("CREDENTIAL_DIRECTORY_REQUIRED")?,
                    )?;
                    let _guard = directory
                        .file(file, false, false)
                        .context("CREDENTIAL_MISSING_OR_UNSAFE")?;
                    dpapi(&std::fs::read(file).context("CREDENTIAL_MISSING")?, false)?
                }
                #[cfg(not(windows))]
                {
                    let _ = file;
                    bail!("CREDENTIAL_BACKEND_UNAVAILABLE: Windows DPAPI");
                }
            }
            Self::Systemd { name } => {
                #[cfg(target_os = "linux")]
                {
                    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
                    ensure!(
                        !name.is_empty()
                            && name.len() <= 128
                            && name
                                .bytes()
                                .all(|v| v.is_ascii_alphanumeric() || b"-_.".contains(&v))
                            && name != "."
                            && name != "..",
                        "INVALID_CREDENTIAL_NAME"
                    );
                    let directory = std::env::var_os("CREDENTIALS_DIRECTORY")
                        .context("CREDENTIAL_LOCKED: systemd credential directory unavailable")?;
                    let mut file = std::fs::OpenOptions::new()
                        .read(true)
                        .custom_flags(libc::O_NOFOLLOW)
                        .open(Path::new(&directory).join(name))
                        .context("CREDENTIAL_MISSING")?;
                    let metadata = file.metadata()?;
                    ensure!(
                        metadata.is_file()
                            && metadata.len() <= 16384
                            && metadata.mode() & 0o077 == 0
                            && (metadata.uid() == unsafe { libc::geteuid() }
                                || metadata.uid() == 0),
                        "CREDENTIAL_ACCESS_REFUSED"
                    );
                    let mut bytes = Zeroizing::new(vec![]);
                    std::io::Read::read_to_end(&mut file, &mut bytes)?;
                    bytes
                }
                #[cfg(not(target_os = "linux"))]
                {
                    let _ = name;
                    bail!("CREDENTIAL_BACKEND_UNAVAILABLE: systemd");
                }
            }
        };
        let identity: Identity =
            serde_json::from_slice(&bytes).map_err(|_| anyhow::anyhow!("CREDENTIAL_INVALID"))?;
        ensure!(
            identity.installation == installation,
            "CREDENTIAL_IDENTITY_MISMATCH"
        );
        ensure!(
            !identity.private_key.is_empty() && !identity.certificate.is_empty(),
            "CREDENTIAL_INVALID"
        );
        identity
            .tls_server(&[], true)
            .context("CREDENTIAL_INVALID: certificate/key mismatch")?;
        Ok(identity)
    }
    pub fn save_windows(&self, identity: &Identity) -> Result<()> {
        #[cfg(windows)]
        {
            let Self::WindowsDpapi { file } = self else {
                bail!("CREDENTIAL_PROVISION_EXTERNALLY: use systemd encrypted credentials");
            };
            ensure!(!file.exists(), "CREDENTIAL_ALREADY_EXISTS");
            let parent = file.parent().context("CREDENTIAL_DIRECTORY_REQUIRED")?;
            let _directory = crate::private_storage::Directory::open(parent)?;
            let serialized = Zeroizing::new(serde_json::to_vec(identity)?);
            let encrypted = dpapi(&serialized, true)?;
            use std::io::Write;
            let mut output = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(file)?;
            output.write_all(&encrypted)?;
            output.sync_all()?;
            Ok(())
        }
        #[cfg(not(windows))]
        {
            let _ = identity;
            bail!("CREDENTIAL_PROVISION_EXTERNALLY: no plaintext fallback");
        }
    }
}
#[cfg(windows)]
fn dpapi(bytes: &[u8], protect: bool) -> Result<Zeroizing<Vec<u8>>> {
    #[repr(C)]
    struct Blob {
        size: u32,
        data: *mut u8,
    }
    #[link(name = "crypt32")]
    unsafe extern "system" {
        fn CryptProtectData(
            input: *mut Blob,
            description: *const u16,
            entropy: *mut Blob,
            reserved: *mut std::ffi::c_void,
            prompt: *mut std::ffi::c_void,
            flags: u32,
            output: *mut Blob,
        ) -> i32;
        fn CryptUnprotectData(
            input: *mut Blob,
            description: *mut *mut u16,
            entropy: *mut Blob,
            reserved: *mut std::ffi::c_void,
            prompt: *mut std::ffi::c_void,
            flags: u32,
            output: *mut Blob,
        ) -> i32;
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn LocalFree(memory: *mut std::ffi::c_void) -> *mut std::ffi::c_void;
    }
    let mut input = Blob {
        size: u32::try_from(bytes.len())?,
        data: bytes.as_ptr().cast_mut(),
    };
    let mut output = Blob {
        size: 0,
        data: std::ptr::null_mut(),
    };
    let ok = unsafe {
        if protect {
            CryptProtectData(
                &mut input,
                std::ptr::null(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                1,
                &mut output,
            )
        } else {
            CryptUnprotectData(
                &mut input,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                1,
                &mut output,
            )
        }
    };
    ensure!(ok != 0, "CREDENTIAL_LOCKED: CurrentUser DPAPI unavailable");
    let result = Zeroizing::new(
        unsafe { std::slice::from_raw_parts(output.data, output.size as usize) }.to_vec(),
    );
    unsafe {
        std::ptr::write_bytes(output.data, 0, output.size as usize);
        LocalFree(output.data.cast());
    }
    Ok(result)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Peer {
    pub installation: String,
    pub certificate: Vec<u8>,
    pub endpoint: String,
    pub revoked: bool,
}
impl Peer {
    pub fn fingerprint(&self) -> String {
        hash(&self.certificate)
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Invitation {
    pub id: String,
    pub issuer: Peer,
    pub enrollment_endpoint: String,
    pub expires_ms: u64,
    secret: Vec<u8>,
}
impl Drop for Invitation {
    fn drop(&mut self) {
        self.secret.zeroize();
    }
}
impl std::fmt::Debug for Invitation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Invitation")
            .field("id", &self.id)
            .field("expires_ms", &self.expires_ms)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncConfiguration {
    pub peer: String,
    pub repo: String,
    pub local_runtime: PathBuf,
    pub local_replica: String,
    pub remote_replica: String,
    pub enabled: bool,
}

pub struct Registry {
    db: Connection,
    _file: std::fs::File,
    _directory: crate::private_storage::Directory,
    pub installation: String,
}
impl Registry {
    pub fn open(path: &Path, installation: &str) -> Result<Self> {
        uuid::Uuid::parse_str(installation).context("INVALID_INSTALLATION_ID")?;
        let directory = crate::private_storage::Directory::open(
            path.parent().context("PAIRING_DIRECTORY_REQUIRED")?,
        )?;
        directory.validate_existing_files()?;
        let file = directory.file(path, true, false)?;
        let db = Connection::open(path)?;
        directory.validate_existing_files()?;
        db.busy_timeout(std::time::Duration::from_secs(3))?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
          CREATE TABLE IF NOT EXISTS identity(id TEXT PRIMARY KEY);
          CREATE TABLE IF NOT EXISTS invitations(id TEXT PRIMARY KEY,verifier TEXT NOT NULL,expires INTEGER NOT NULL,consumed INTEGER NOT NULL,candidate TEXT);
          CREATE TABLE IF NOT EXISTS pairs(id TEXT PRIMARY KEY,certificate BLOB NOT NULL,endpoint TEXT NOT NULL,revoked INTEGER NOT NULL);
          CREATE TABLE IF NOT EXISTS syncs(peer TEXT NOT NULL,repo TEXT NOT NULL,payload TEXT NOT NULL,status TEXT NOT NULL,PRIMARY KEY(peer,repo));")?;
        let existing: Option<String> = db
            .query_row("SELECT id FROM identity", [], |row| row.get(0))
            .optional()?;
        if let Some(existing) = existing {
            ensure!(
                existing == installation,
                "PAIRING_REGISTRY_IDENTITY_MISMATCH"
            );
        } else {
            db.execute("INSERT INTO identity VALUES(?)", [installation])?;
        }
        Ok(Self {
            db,
            _file: file,
            _directory: directory,
            installation: installation.into(),
        })
    }
    pub fn create_invitation(
        &mut self,
        identity: &Identity,
        enrollment: &str,
        sync: &str,
        lifetime_ms: u64,
    ) -> Result<Invitation> {
        ensure!(
            identity.installation == self.installation,
            "CREDENTIAL_IDENTITY_MISMATCH"
        );
        ensure!(
            (1000..=600_000).contains(&lifetime_ms),
            "INVALID_INVITATION_LIFETIME"
        );
        let mut secret = vec![0; 32];
        rand::rngs::OsRng.fill_bytes(&mut secret);
        let invitation = Invitation {
            id: id(),
            issuer: Peer {
                installation: identity.installation.clone(),
                certificate: identity.certificate.clone(),
                endpoint: sync.into(),
                revoked: false,
            },
            enrollment_endpoint: enrollment.into(),
            expires_ms: time() + lifetime_ms,
            secret,
        };
        self.db.execute(
            "INSERT INTO invitations VALUES(?,?,?,0,NULL)",
            params![
                invitation.id,
                hash(&invitation.secret),
                invitation.expires_ms
            ],
        )?;
        Ok(invitation)
    }
    pub fn submit(&mut self, invitation: &Invitation, candidate: &Peer) -> Result<()> {
        ensure!(
            invitation.issuer.installation == self.installation,
            "INVITATION_ISSUER_MISMATCH"
        );
        ensure!(
            candidate.installation != self.installation && !candidate.revoked,
            "INVALID_PAIR_CANDIDATE"
        );
        uuid::Uuid::parse_str(&candidate.installation).context("INVALID_PAIR_CANDIDATE")?;
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let (verifier, expires, consumed): (String, u64, bool) = tx
            .query_row(
                "SELECT verifier,expires,consumed FROM invitations WHERE id=?",
                [&invitation.id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .context("INVITATION_NOT_FOUND")?;
        ensure!(!consumed, "INVITATION_ALREADY_USED");
        ensure!(time() < expires, "INVITATION_EXPIRED");
        ensure!(
            invitation.secret.len() == 32
                && bool::from(
                    hash(&invitation.secret)
                        .as_bytes()
                        .ct_eq(verifier.as_bytes())
                ),
            "INVITATION_REJECTED"
        );
        tx.execute(
            "UPDATE invitations SET consumed=1,candidate=? WHERE id=?",
            params![serde_json::to_string(candidate)?, invitation.id],
        )?;
        tx.commit()?;
        Ok(())
    }
    pub fn approve(&mut self, invitation: &str) -> Result<Peer> {
        let candidate: String = self
            .db
            .query_row(
                "SELECT candidate FROM invitations WHERE id=? AND consumed=1",
                [invitation],
                |row| row.get(0),
            )
            .context("PAIR_CANDIDATE_NOT_FOUND")?;
        let peer: Peer = serde_json::from_str(&candidate)?;
        self.trust(&peer)?;
        Ok(peer)
    }
    pub fn trust(&mut self, peer: &Peer) -> Result<()> {
        let transaction = self.begin_access()?;
        ensure!(
            peer.installation != self.installation && !peer.revoked,
            "INVALID_PAIR_CANDIDATE"
        );
        uuid::Uuid::parse_str(&peer.installation).context("INVALID_PAIR_CANDIDATE")?;
        ensure!(
            !peer.certificate.is_empty() && peer.certificate.len() <= 8192,
            "INVALID_PAIR_CERTIFICATE"
        );
        ensure!(
            peer.endpoint
                .parse::<std::net::SocketAddr>()
                .context("PAIR_ENDPOINT_REQUIRES_NUMERIC_ADDRESS")?
                .port()
                != 0,
            "INVALID_PAIR_ENDPOINT"
        );
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(peer.certificate.clone().into())
            .context("INVALID_PAIR_CERTIFICATE")?;
        let prior: Option<(Vec<u8>, bool)> = self
            .db
            .query_row(
                "SELECT certificate,revoked FROM pairs WHERE id=?",
                [&peer.installation],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((certificate, revoked)) = prior {
            ensure!(!revoked, "PAIR_REVOKED");
            ensure!(
                certificate == peer.certificate,
                "PAIR_KEY_CHANGED: explicit rotation required"
            );
        }
        self.db.execute("INSERT INTO pairs VALUES(?,?,?,0) ON CONFLICT(id) DO UPDATE SET endpoint=excluded.endpoint",params![peer.installation,peer.certificate,peer.endpoint])?;
        transaction.commit()?;
        Ok(())
    }
    pub fn peers(&self) -> Result<Vec<Peer>> {
        Ok(self
            .db
            .prepare("SELECT id,certificate,endpoint,revoked FROM pairs")?
            .query_map([], |row| {
                Ok(Peer {
                    installation: row.get(0)?,
                    certificate: row.get(1)?,
                    endpoint: row.get(2)?,
                    revoked: row.get(3)?,
                })
            })?
            .collect::<std::result::Result<_, _>>()?)
    }
    pub fn authenticate(&self, certificate: &[u8], claimed: &str) -> Result<Peer> {
        let peer = self
            .peers()?
            .into_iter()
            .find(|peer| peer.installation == claimed)
            .context("PAIR_NOT_FOUND")?;
        ensure!(!peer.revoked, "PAIR_REVOKED");
        ensure!(peer.certificate == certificate, "PAIR_KEY_MISMATCH");
        Ok(peer)
    }
    pub fn revoke(&mut self, peer: &str) -> Result<()> {
        let transaction = self.begin_access()?;
        ensure!(
            self.db
                .execute("UPDATE pairs SET revoked=1 WHERE id=?", [peer])?
                == 1,
            "PAIR_NOT_FOUND"
        );
        self.db
            .execute("UPDATE syncs SET status='revoked' WHERE peer=?", [peer])?;
        transaction.commit()?;
        Ok(())
    }
    pub fn configure_sync(&mut self, configuration: &SyncConfiguration) -> Result<()> {
        let transaction = self.begin_access()?;
        let peer = self
            .peers()?
            .into_iter()
            .find(|peer| peer.installation == configuration.peer)
            .context("PAIR_NOT_FOUND")?;
        ensure!(!peer.revoked, "PAIR_REVOKED");
        for identity in [
            &configuration.repo,
            &configuration.local_replica,
            &configuration.remote_replica,
        ] {
            uuid::Uuid::parse_str(identity).context("INVALID_SYNC_CONFIGURATION")?;
        }
        ensure!(
            configuration.local_replica != configuration.remote_replica,
            "DUPLICATE_REPLICA_IDENTITY"
        );
        let peers = self.peers()?;
        let prior = self.configurations()?.into_iter().find(|prior| {
            prior.repo == configuration.repo
                && prior.peer != configuration.peer
                && prior.enabled
                && peers
                    .iter()
                    .any(|peer| peer.installation == prior.peer && !peer.revoked)
        });
        ensure!(prior.is_none(), "FIRST_SLICE_ONE_PEER_PER_REPOSITORY");
        self.db.execute("INSERT INTO syncs VALUES(?,?,?,'configured') ON CONFLICT(peer,repo) DO UPDATE SET payload=excluded.payload,status='configured'",params![configuration.peer,configuration.repo,serde_json::to_string(configuration)?])?;
        transaction.commit()?;
        Ok(())
    }
    pub fn configurations(&self) -> Result<Vec<SyncConfiguration>> {
        Ok(self
            .db
            .prepare("SELECT payload FROM syncs ORDER BY peer,repo")?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .map(|v| serde_json::from_str(&v))
            .collect::<std::result::Result<_, _>>()?)
    }
    pub fn sync_status(&mut self, configuration: &SyncConfiguration, status: &str) -> Result<()> {
        self.db.execute(
            "UPDATE syncs SET status=? WHERE peer=? AND repo=? AND payload=? AND EXISTS(SELECT 1 FROM pairs WHERE id=syncs.peer AND revoked=0)",
            params![status, configuration.peer, configuration.repo,serde_json::to_string(configuration)?],
        )?;
        Ok(())
    }
    pub fn statuses(&self) -> Result<serde_json::Value> {
        let rows = self
            .db
            .prepare("SELECT peer,repo,status FROM syncs")?
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(serde_json::json!(rows.into_iter().map(|(peer,repo,status)|serde_json::json!({"peer":peer,"repo":repo,"status":status})).collect::<Vec<_>>()))
    }
    pub fn grant(&self, peer: &str, repo: &str) -> Result<SyncConfiguration> {
        let grant = self
            .configurations()?
            .into_iter()
            .find(|configuration| {
                configuration.peer == peer && configuration.repo == repo && configuration.enabled
            })
            .context("REPOSITORY_NOT_GRANTED")?;
        Ok(grant)
    }
    pub fn pending(&self) -> Result<serde_json::Value> {
        let rows = self
            .db
            .prepare(
                "SELECT id,candidate FROM invitations WHERE consumed=1 AND candidate IS NOT NULL",
            )?
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let peers = self.peers()?;
        let pending=rows.into_iter().map(|(invitation,candidate)|->Result<_>{
            let peer:Peer=serde_json::from_str(&candidate)?;
            Ok(serde_json::json!({"invitation":invitation,"installation":peer.installation,"fingerprint":peer.fingerprint(),"endpoint":peer.endpoint,"approved":peers.iter().any(|known|known.installation==peer.installation&&!known.revoked)}))
        }).collect::<Result<Vec<_>>>()?;
        Ok(serde_json::json!(pending))
    }
    // Serialize data operations with owner-driven revoke/configuration updates.
    // The caller must hold this guard until the response has been sent.
    pub fn begin_access(&self) -> Result<rusqlite::Transaction<'_>> {
        Ok(rusqlite::Transaction::new_unchecked(
            &self.db,
            rusqlite::TransactionBehavior::Immediate,
        )?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case", deny_unknown_fields)]
pub enum NetworkRequest {
    ListPublished {},
    Exchange {
        repo: String,
        page: Box<crate::paired_sync::SyncPage>,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishedState {
    pub repo: String,
    pub replica: String,
    pub label: String,
    pub branches: Vec<crate::core::Branch>,
}

#[cfg(test)]
mod tests {
    use super::*;
    fn private_temp() -> tempfile::TempDir {
        let temp = tempfile::tempdir().unwrap();
        #[cfg(windows)]
        crate::local_ipc::protect_directory(temp.path()).unwrap();
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        temp
    }
    fn peer(identity: &Identity) -> Peer {
        Peer {
            installation: identity.installation.clone(),
            certificate: identity.certificate.clone(),
            endpoint: "127.0.0.1:1".into(),
            revoked: false,
        }
    }
    #[test]
    fn invitation_is_single_use_requires_approval_and_persists_revocation() {
        let temp = private_temp();
        let path = temp.path().join("pairs.db");
        let a = Identity::generate(&id()).unwrap();
        let b = Identity::generate(&id()).unwrap();
        let mut registry = Registry::open(&path, &a.installation).unwrap();
        let invitation = registry
            .create_invitation(&a, "127.0.0.1:2", "127.0.0.1:1", 1000)
            .unwrap();
        registry.submit(&invitation, &peer(&b)).unwrap();
        assert!(
            registry
                .authenticate(&b.certificate, &b.installation)
                .is_err()
        );
        assert!(
            registry
                .submit(&invitation, &peer(&b))
                .unwrap_err()
                .to_string()
                .contains("ALREADY_USED")
        );
        registry.approve(&invitation.id).unwrap();
        assert!(
            registry
                .authenticate(&b.certificate, &b.installation)
                .is_ok()
        );
        let grant = SyncConfiguration {
            peer: b.installation.clone(),
            repo: id(),
            local_runtime: temp.path().join("runtime.json"),
            local_replica: id(),
            remote_replica: id(),
            enabled: true,
        };
        registry.configure_sync(&grant).unwrap();
        drop(registry);
        let mut registry = Registry::open(&path, &a.installation).unwrap();
        assert_eq!(
            registry.configurations().unwrap()[0].remote_replica,
            grant.remote_replica
        );
        assert!(registry.grant(&b.installation, &id()).is_err());
        registry.revoke(&b.installation).unwrap();
        drop(registry);
        let mut registry = Registry::open(&path, &a.installation).unwrap();
        assert!(
            registry
                .authenticate(&b.certificate, &b.installation)
                .unwrap_err()
                .to_string()
                .contains("REVOKED")
        );
        assert!(registry.trust(&peer(&b)).is_err());
        assert!(registry.configure_sync(&grant).is_err());
        let replacement = Identity::generate(&id()).unwrap();
        registry.trust(&peer(&replacement)).unwrap();
        let mut rotated = grant.clone();
        rotated.peer = replacement.installation.clone();
        registry.configure_sync(&rotated).unwrap();
    }
    #[test]
    fn expired_wrong_secret_and_wrong_identity_invitations_fail() {
        let temp = private_temp();
        let a = Identity::generate(&id()).unwrap();
        let b = Identity::generate(&id()).unwrap();
        let mut registry = Registry::open(&temp.path().join("pairs.db"), &a.installation).unwrap();
        let mut invitation = registry
            .create_invitation(&a, "127.0.0.1:2", "127.0.0.1:1", 1000)
            .unwrap();
        invitation.secret[0] ^= 1;
        assert!(registry.submit(&invitation, &peer(&b)).is_err());
        invitation.secret[0] ^= 1;
        registry
            .db
            .execute("UPDATE invitations SET expires=0", [])
            .unwrap();
        assert!(
            registry
                .submit(&invitation, &peer(&b))
                .unwrap_err()
                .to_string()
                .contains("EXPIRED")
        );
        assert!(Registry::open(&temp.path().join("pairs.db"), &b.installation).is_err());
    }
    #[test]
    fn concurrent_invitation_consumption_accepts_exactly_one() {
        let temp = private_temp();
        let path = temp.path().join("pairs.db");
        let a = Identity::generate(&id()).unwrap();
        let b = Identity::generate(&id()).unwrap();
        let mut registry = Registry::open(&path, &a.installation).unwrap();
        let invitation = registry
            .create_invitation(&a, "127.0.0.1:2", "127.0.0.1:1", 10000)
            .unwrap();
        let serialized = Zeroizing::new(serde_json::to_vec(&invitation).unwrap());
        let barrier = Arc::new(std::sync::Barrier::new(4));
        let mut threads = vec![];
        for _ in 0..4 {
            let path = path.clone();
            let installation = a.installation.clone();
            let encoded = Zeroizing::new(serialized.to_vec());
            let candidate = peer(&b);
            let barrier = barrier.clone();
            threads.push(std::thread::spawn(move || {
                let mut registry = Registry::open(&path, &installation).unwrap();
                let invitation: Invitation = serde_json::from_slice(&encoded).unwrap();
                barrier.wait();
                registry.submit(&invitation, &candidate).is_ok()
            }));
        }
        assert_eq!(
            threads
                .into_iter()
                .filter_map(|thread| thread.join().ok())
                .filter(|success| *success)
                .count(),
            1
        );
    }
    #[test]
    fn management_and_unknown_fields_are_absent_from_network_schema() {
        for op in [
            "create",
            "adopt",
            "start",
            "stop",
            "shutdown",
            "checkout",
            "publish",
            "invite",
            "approve",
            "revoke",
            "shell",
            "cat-object",
        ] {
            assert!(
                serde_json::from_value::<NetworkRequest>(serde_json::json!({"op":op})).is_err()
            );
        }
        assert!(
            serde_json::from_value::<NetworkRequest>(
                serde_json::json!({"op":"list-published","admin":true})
            )
            .is_err()
        );
    }
    #[test]
    fn revocation_serializes_with_inflight_data_and_cannot_be_overwritten_by_late_status() {
        let temp = private_temp();
        let path = temp.path().join("pairs.db");
        let a = Identity::generate(&id()).unwrap();
        let b = Identity::generate(&id()).unwrap();
        let mut registry = Registry::open(&path, &a.installation).unwrap();
        registry.trust(&peer(&b)).unwrap();
        let grant = SyncConfiguration {
            peer: b.installation.clone(),
            repo: id(),
            local_runtime: temp.path().join("runtime.json"),
            local_replica: id(),
            remote_replica: id(),
            enabled: true,
        };
        registry.configure_sync(&grant).unwrap();
        let access = registry.begin_access().unwrap();
        let (started, ready) = std::sync::mpsc::channel();
        let (completed, result) = std::sync::mpsc::channel();
        let installation = a.installation.clone();
        let peer = b.installation.clone();
        let thread = std::thread::spawn(move || {
            let mut registry = Registry::open(&path, &installation).unwrap();
            started.send(()).unwrap();
            let result = registry.revoke(&peer);
            completed.send(result.is_ok()).unwrap();
        });
        ready.recv().unwrap();
        assert!(
            result
                .recv_timeout(std::time::Duration::from_millis(100))
                .is_err()
        );
        access.commit().unwrap();
        assert!(
            result
                .recv_timeout(std::time::Duration::from_secs(3))
                .unwrap()
        );
        thread.join().unwrap();
        assert!(
            registry
                .authenticate(&b.certificate, &b.installation)
                .is_err()
        );
        registry.sync_status(&grant, "caught-up").unwrap();
        assert_eq!(registry.statuses().unwrap()[0]["status"], "revoked");
        assert!(registry.configure_sync(&grant).is_err());
    }
    #[cfg(windows)]
    #[test]
    fn dpapi_fixture_roundtrip_has_no_plaintext_or_fallback() {
        let temp = private_temp();
        let identity = Identity::generate(&id()).unwrap();
        let source = CredentialSource::WindowsDpapi {
            file: temp.path().join("credential.dpapi"),
        };
        source.save_windows(&identity).unwrap();
        let loaded = source.load(&identity.installation).unwrap();
        assert_eq!(loaded.fingerprint(), identity.fingerprint());
        assert!(source.save_windows(&identity).is_err());
        assert!(source.load(&id()).is_err());
        let encrypted = std::fs::read(temp.path().join("credential.dpapi")).unwrap();
        assert!(
            !encrypted
                .windows(identity.private_key.len())
                .any(|bytes| bytes == identity.private_key)
        );
        std::fs::write(temp.path().join("credential.dpapi"), b"invalid fixture").unwrap();
        assert!(source.load(&identity.installation).is_err());
    }
}
