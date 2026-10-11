//! Explicit app-owned setup. Existing configurations and credentials are never replaced.
use crate::{
    pairing::{CredentialSource, Identity},
    pairing_service::{Config, Transport},
    private_storage::Directory,
};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    io::Write,
    path::{Path, PathBuf},
};
use zeroize::Zeroizing;

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Intent {
    version: u32,
    orchestrator: PathBuf,
    owner: String,
    receiver: bool,
    address: String,
    port: u16,
}

pub fn client(orchestrator: &Path) -> Result<PathBuf> {
    setup(orchestrator, None)
}
/// Read-only startup discovery. Unlike explicit Connect, this never provisions
/// or repairs files, including a lost completed configuration or registry.
pub fn existing_client(orchestrator: &Path) -> Result<Option<PathBuf>> {
    let orchestrator = std::fs::canonicalize(orchestrator)?;
    let config = crate::orchestrator::Config::load(&orchestrator)?;
    let owner = config.existing_installation()?;
    let root = std::fs::canonicalize(config.data_directory)?.join("desktop-pairing");
    let path = root.join("client.toml");
    if !path.try_exists()? {
        ensure!(
            !root.join("setup.sqlite").try_exists()?,
            "PAIRING_SETUP_RECOVERY_REQUIRED"
        );
        return Ok(None);
    }
    let directory = Directory::open(&root)?;
    let _file = directory.file(&path, false, false)?;
    let pairing = Config::load(&path)?;
    validate_existing(
        &pairing,
        &Intent {
            version: 1,
            orchestrator,
            owner,
            receiver: false,
            address: String::new(),
            port: 0,
        },
        &root,
    )?;
    crate::managed_pairing::Binding::load(&path)?;
    Ok(Some(path))
}
pub fn receiver(orchestrator: &Path, address: &str, port: u16) -> Result<PathBuf> {
    let ip: std::net::IpAddr = address.parse().context("PAIRING_NUMERIC_IP_REQUIRED")?;
    ensure!(
        !ip.is_unspecified() && !ip.is_multicast() && port != 0,
        "PAIRING_ADDRESS_REQUIRED"
    );
    setup(orchestrator, Some((ip.to_string(), port)))
}
pub fn receiver_path(orchestrator: &Path) -> Result<PathBuf> {
    let config = crate::orchestrator::Config::load(orchestrator)?;
    config.existing_installation()?;
    Ok(std::fs::canonicalize(config.data_directory)?.join("receiving-pairing/server.toml"))
}
pub fn approve(orchestrator: &Path, invitation: &str) -> Result<Value> {
    let path = receiver_path(orchestrator)?;
    crate::managed_pairing::Binding::load_receiver(&path, orchestrator)?;
    let config = Config::load(&path)?;
    ensure!(
        config.orchestrator_config.as_deref()
            == Some(std::fs::canonicalize(orchestrator)?.as_path()),
        "PAIRING_SETUP_BINDING_CHANGED"
    );
    let peer = config.registry()?.approve(invitation)?;
    Ok(json!({"installation":peer.installation,"status":"approved"}))
}
fn setup(orchestrator: &Path, endpoint: Option<(String, u16)>) -> Result<PathBuf> {
    let orchestrator = std::fs::canonicalize(orchestrator)?;
    let owner_config = crate::orchestrator::Config::load(&orchestrator)?;
    let owner = owner_config.existing_installation()?;
    let receiver = endpoint.is_some();
    let root = std::fs::canonicalize(&owner_config.data_directory)?.join(if receiver {
        "receiving-pairing"
    } else {
        "desktop-pairing"
    });
    let mut directory = Directory::open(&root)?;
    let path = root.join(if receiver {
        "server.toml"
    } else {
        "client.toml"
    });
    let (address, port) = endpoint.unwrap_or_default();
    let intent = Intent {
        version: 1,
        orchestrator,
        owner,
        receiver,
        address,
        port,
    };
    // This private setup lock serializes all retries, including the publication gap.
    let lock_path = root.join("setup.lock");
    drop(directory.file(&lock_path, true, false)?);
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.share_mode(0);
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let lock = options.open(&lock_path).context("PAIRING_SETUP_BUSY")?;
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        ensure!(
            unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
            "PAIRING_SETUP_BUSY"
        );
    }
    let _lock = lock;
    if path.try_exists()? {
        let _file = directory.file(&path, false, false)?;
        let config = Config::load(&path)?;
        validate_existing(&config, &intent, &root)?;
        // No auto-repair: a published config requires its original durable identity.
        config
            .identity()
            .context("PAIRING_SETUP_RECOVERY_REQUIRED")?;
        let registry = Directory::open(&config.data_directory)?;
        let _db = registry
            .file(&config.data_directory.join("pairs.sqlite"), false, false)
            .context("PAIRING_SETUP_RECOVERY_REQUIRED")?;
        if receiver {
            crate::managed_pairing::Binding::load_receiver(&path, &intent.orchestrator)?;
        } else {
            crate::managed_pairing::Binding::load(&path)?;
        }
        let db = Connection::open(root.join("setup.sqlite"))?;
        db.pragma_update(None, "synchronous", "FULL")?;
        db.execute("UPDATE setup SET phase=2 WHERE singleton=1 AND phase=1", [])?;
        return Ok(path);
    }
    let db_path = root.join("setup.sqlite");
    let fresh = !db_path.try_exists()?;
    let _db_file = directory.file(&db_path, true, false)?;
    let mut db = Connection::open(&db_path)?;
    db.pragma_update(None, "journal_mode", "DELETE")?;
    db.pragma_update(None, "synchronous", "FULL")?;
    if fresh {
        db.execute_batch("BEGIN IMMEDIATE; CREATE TABLE setup(singleton INTEGER PRIMARY KEY CHECK(singleton=1),intent TEXT NOT NULL,installation TEXT NOT NULL,credential BLOB NOT NULL,phase INTEGER NOT NULL CHECK(phase IN(0,1,2))); PRAGMA user_version=1; COMMIT;")?;
    }
    ensure!(
        db.pragma_query_value(None, "user_version", |r| r.get::<_, u32>(0))? == 1,
        "PAIRING_SETUP_RECOVERY_REQUIRED"
    );
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let stored: Option<(String, String, Vec<u8>, u32)> = tx
        .query_row(
            "SELECT intent,installation,credential,phase FROM setup WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?;
    let name = format!(
        "tkfs-identity-{}-{}",
        intent.owner,
        if receiver { "receiver" } else { "client" }
    );
    let (installation, encrypted) = if let Some((saved, installation, encrypted, phase)) = stored {
        ensure!(
            phase == 0,
            "PAIRING_SETUP_RECOVERY_REQUIRED: published configuration is missing"
        );
        ensure!(
            serde_json::from_str::<Intent>(&saved)? == intent,
            "PAIRING_SETUP_BINDING_CHANGED"
        );
        (installation, Zeroizing::new(encrypted))
    } else {
        let installation = crate::core::id();
        let identity = Identity::generate(&installation)?;
        let plaintext = Zeroizing::new(serde_json::to_vec(&identity)?);
        let encrypted = crate::protected_secret::seal(&name, &plaintext)?;
        tx.execute(
            "INSERT INTO setup VALUES(1,?,?,?,0)",
            params![serde_json::to_string(&intent)?, installation, &*encrypted],
        )?;
        (installation, encrypted)
    };
    tx.commit()?;
    crate::core::fault("pairing_setup_identity_committed");
    let credential_path = root.join("identity.encrypted");
    let credential_exists = credential_path.try_exists()?;
    if credential_exists {
        let _file = directory.file(&credential_path, false, false)?;
        ensure!(
            std::fs::read(&credential_path)? == *encrypted,
            "PAIRING_SETUP_RECOVERY_REQUIRED"
        );
    }
    let plaintext = crate::protected_secret::open(&name, &encrypted)?;
    let identity: Identity =
        serde_json::from_slice(&plaintext).context("PAIRING_SETUP_RECOVERY_REQUIRED")?;
    ensure!(
        identity.installation == installation,
        "PAIRING_SETUP_BINDING_CHANGED"
    );
    identity
        .tls_server(&[], true)
        .context("PAIRING_SETUP_RECOVERY_REQUIRED")?;
    let credential = CredentialSource::PlatformEncrypted {
        file: credential_path.clone(),
        name,
    };
    let socket = if receiver {
        std::net::SocketAddr::new(intent.address.parse()?, intent.port).to_string()
    } else {
        String::new()
    };
    let config = Config {
        format_version: 1,
        installation,
        data_directory: root.join("registry"),
        credential,
        transport: Transport::Wss,
        inbound: receiver,
        dial: true,
        listen: socket.clone(),
        enrollment_listen: String::new(),
        advertise: if receiver {
            format!("wss://{socket}/tkfs/sync")
        } else {
            "outbound-only".into()
        },
        enrollment_advertise: if receiver {
            format!("wss://{socket}/tkfs/enroll")
        } else {
            String::new()
        },
        orchestrator_config: if receiver {
            Some(intent.orchestrator.clone())
        } else {
            None
        },
    };
    config.registry()?;
    // Finish all protected-path opens before acquiring the Windows publication
    // reservation, which intentionally excludes competing directory pins.
    if !credential_exists {
        publish(&credential_path, &encrypted, &mut directory)?;
    }
    crate::core::fault("pairing_setup_before_config");
    // Fence publication durably. An absent config after this point is ambiguous,
    // so recovery must not recreate it or bootstrap a fresh trust registry.
    db.execute("UPDATE setup SET phase=1 WHERE singleton=1 AND phase=0", [])?;
    crate::core::fault("pairing_setup_publishing");
    publish(
        &path,
        toml::to_string_pretty(&config)?.as_bytes(),
        &mut directory,
    )?;
    crate::core::fault("pairing_setup_config_published");
    db.execute("UPDATE setup SET phase=2 WHERE singleton=1 AND phase=1", [])?;
    Ok(path)
}
fn validate_existing(config: &Config, intent: &Intent, root: &Path) -> Result<()> {
    let directory = Directory::open(root)?;
    let _marker = directory
        .file(&root.join("setup.sqlite"), false, false)
        .context("PAIRING_SETUP_RECOVERY_REQUIRED")?;
    let socket = if intent.receiver {
        std::net::SocketAddr::new(intent.address.parse()?, intent.port).to_string()
    } else {
        String::new()
    };
    ensure!(
        config.transport == Transport::Wss
            && config.inbound == intent.receiver
            && config.dial
            && config.data_directory == root.join("registry")
            && config.listen == socket
            && config.enrollment_listen.is_empty()
            && config.advertise
                == if intent.receiver {
                    format!("wss://{socket}/tkfs/sync")
                } else {
                    "outbound-only".into()
                }
            && config.enrollment_advertise
                == if intent.receiver {
                    format!("wss://{socket}/tkfs/enroll")
                } else {
                    String::new()
                }
            && config.orchestrator_config.as_deref()
                == if intent.receiver {
                    Some(intent.orchestrator.as_path())
                } else {
                    None
                },
        "PAIRING_SETUP_BINDING_CHANGED"
    );
    let db = Connection::open_with_flags(
        root.join("setup.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .context("PAIRING_SETUP_RECOVERY_REQUIRED")?;
    let (saved, installation, encrypted): (String, String, Vec<u8>) = db.query_row(
        "SELECT intent,installation,credential FROM setup WHERE singleton=1 AND phase IN(1,2)",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    ensure!(
        serde_json::from_str::<Intent>(&saved)? == *intent && installation == config.installation,
        "PAIRING_SETUP_BINDING_CHANGED"
    );
    let expected_name = format!(
        "tkfs-identity-{}-{}",
        intent.owner,
        if intent.receiver {
            "receiver"
        } else {
            "client"
        }
    );
    ensure!(
        matches!(&config.credential,CredentialSource::PlatformEncrypted{file,name} if file==&root.join("identity.encrypted") && name==&expected_name),
        "PAIRING_SETUP_BINDING_CHANGED"
    );
    let _credential = directory
        .file(&root.join("identity.encrypted"), false, false)
        .context("PAIRING_SETUP_RECOVERY_REQUIRED")?;
    ensure!(
        std::fs::read(root.join("identity.encrypted"))? == encrypted,
        "PAIRING_SETUP_RECOVERY_REQUIRED"
    );
    Ok(())
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;
    fn configuration(root: &Path) -> PathBuf {
        let path = root.join("orchestrator.toml");
        std::fs::write(&path,format!("format_version=1\ndata_directory='o1'\n[control]\ntransport='named-pipe'\nname='setup-test-{}'\n",crate::core::id())).unwrap();
        drop(
            crate::orchestrator::Supervisor::open(
                crate::orchestrator::Config::load(&path).unwrap(),
            )
            .unwrap(),
        );
        path
    }
    #[test]
    fn setup_reuses_identity_separates_roles_and_refuses_missing_or_changed_credentials() {
        let temp = tempfile::tempdir().unwrap();
        let owner = configuration(temp.path());
        assert!(existing_client(&owner).unwrap().is_none());
        assert!(!temp.path().join("o1/desktop-pairing").exists());
        let path = client(&owner).unwrap();
        assert_eq!(existing_client(&owner).unwrap(), Some(path.clone()));
        let first = Config::load(&path).unwrap();
        let fingerprint = first.identity().unwrap().fingerprint();
        assert_eq!(client(&owner).unwrap(), path);
        assert_eq!(
            Config::load(&path)
                .unwrap()
                .identity()
                .unwrap()
                .fingerprint(),
            fingerprint
        );
        let receiver_path = receiver(&owner, "127.0.0.1", 47443).unwrap();
        let receive = Config::load(&receiver_path).unwrap();
        assert_ne!(first.installation, receive.installation);
        assert!(crate::managed_pairing::Binding::load(&receiver_path).is_err());
        assert!(crate::managed_pairing::Binding::load_receiver(&receiver_path, &owner).is_ok());
        assert!(receiver(&owner, "127.0.0.1", 47444).is_err());
        let CredentialSource::PlatformEncrypted { file, .. } = &first.credential else {
            panic!()
        };
        std::fs::remove_file(file).unwrap();
        assert!(client(&owner).is_err());
        assert!(!file.exists());
    }
    #[test]
    #[ignore = "subprocess fixture invoked by durable_setup_identity_survives_crash"]
    fn crash_fixture() {
        client(Path::new(&std::env::var_os("TKFS_SETUP_FIXTURE").unwrap())).unwrap();
        panic!("expected injected crash");
    }
    #[test]
    fn durable_setup_identity_survives_crash() {
        for point in [
            "pairing_setup_identity_committed",
            "pairing_setup_before_config",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let owner = configuration(temp.path());
            let exit = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "pairing_setup::tests::crash_fixture",
                    "--exact",
                    "--ignored",
                ])
                .env("TKFS_SETUP_FIXTURE", &owner)
                .env("TKFS_FAULT", point)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .unwrap();
            assert!(!exit.success());
            let root = temp.path().join("o1/desktop-pairing");
            let db = Connection::open(root.join("setup.sqlite")).unwrap();
            let before: (String, Vec<u8>) = db
                .query_row("SELECT installation,credential FROM setup", [], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .unwrap();
            drop(db);
            let path = client(&owner).unwrap();
            let config = Config::load(&path).unwrap();
            assert_eq!(config.installation, before.0);
            assert_eq!(
                std::fs::read(root.join("identity.encrypted")).unwrap(),
                before.1
            );
        }
    }
    #[test]
    fn completed_or_uncertain_publication_never_repairs_missing_files() {
        for missing in ["client.toml", "identity.encrypted", "registry/pairs.sqlite"] {
            let temp = tempfile::tempdir().unwrap();
            let owner = configuration(temp.path());
            let path = client(&owner).unwrap();
            let root = path.parent().unwrap();
            std::fs::remove_file(root.join(missing)).unwrap();
            // Missing config together with a missing credential must also remain absent.
            if missing == "client.toml" {
                std::fs::remove_file(root.join("identity.encrypted")).unwrap();
            }
            assert!(client(&owner).is_err());
            assert!(existing_client(&owner).is_err());
            assert!(!root.join(missing).exists());
            if missing == "client.toml" {
                assert!(!root.join("identity.encrypted").exists());
            }
        }
        for point in ["pairing_setup_publishing", "pairing_setup_config_published"] {
            let temp = tempfile::tempdir().unwrap();
            let owner = configuration(temp.path());
            let exit = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "pairing_setup::tests::crash_fixture",
                    "--exact",
                    "--ignored",
                ])
                .env("TKFS_SETUP_FIXTURE", &owner)
                .env("TKFS_FAULT", point)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .unwrap();
            assert!(!exit.success());
            let path = temp.path().join("o1/desktop-pairing/client.toml");
            if point == "pairing_setup_publishing" {
                assert!(client(&owner).is_err());
                assert!(!path.exists());
            } else {
                assert_eq!(
                    client(&owner).unwrap(),
                    std::fs::canonicalize(path).unwrap()
                );
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn publish(path: &Path, bytes: &[u8], directory: &mut Directory) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let temporary = path.with_extension(format!("{}.tmp", crate::core::id()));
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    // link creates the destination atomically and refuses an existing name.
    std::fs::hard_link(&temporary, path)?;
    std::fs::remove_file(&temporary)?;
    std::fs::File::open(path.parent().unwrap())?.sync_all()?;
    directory.file(path, false, false)?;
    Ok(())
}
#[cfg(windows)]
fn publish(path: &Path, bytes: &[u8], directory: &mut Directory) -> Result<()> {
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
    let temporary = path.with_extension(format!("{}.tmp", crate::core::id()));
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
