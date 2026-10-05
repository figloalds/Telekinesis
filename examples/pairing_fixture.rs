//! Test-only credential delivery for disposable foreground cross-OS acceptance.
//! Not shipped as a production credential provider; never prints key material.
use anyhow::{Result, ensure};
use std::path::PathBuf;
use tkfs::{
    core::{Store, id},
    pairing::{CredentialSource, Identity},
    pairing_service::{Config, Transport},
};
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    ensure!(
        args.len() == 5 && args[1] == "--disposable-test-fixture",
        "TEST_FIXTURE_ARGUMENTS_REQUIRED"
    );
    let root = PathBuf::from(&args[2]);
    ensure!(
        root.is_absolute()
            && root
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(".tkfs-pairing-test-")
            && !root.exists(),
        "NEW_SCOPED_TEST_DIRECTORY_REQUIRED"
    );
    let identity = Identity::generate(&id())?;
    let replica = id();
    let credential_dir = root.join("credentials");
    tkfs::private_storage::Directory::open(&credential_dir)?;
    #[cfg(windows)]
    let credential = {
        let source = CredentialSource::WindowsDpapi {
            file: credential_dir.join("identity.dpapi"),
        };
        source.save_windows(&identity)?;
        source
    };
    #[cfg(target_os = "linux")]
    let credential = {
        use std::{io::Write, os::unix::fs::OpenOptionsExt};
        let bytes = zeroize::Zeroizing::new(serde_json::to_vec(&identity)?);
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(credential_dir.join("tkfs.identity"))?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        CredentialSource::Systemd {
            name: "tkfs.identity".into(),
        }
    };
    let inbound = args[4] != "outbound-only";
    let config = Config {
        format_version: 1,
        installation: identity.installation.clone(),
        data_directory: root.join("pairing-data"),
        credential,
        transport: Transport::Wss,
        inbound,
        dial: !inbound,
        listen: if inbound {
            args[4]
                .split("://")
                .nth(1)
                .unwrap()
                .split('/')
                .next()
                .unwrap()
                .into()
        } else {
            String::new()
        },
        enrollment_listen: String::new(),
        advertise: args[4].clone(),
        enrollment_advertise: if inbound {
            args[4].replace("/tkfs/sync", "/tkfs/enroll")
        } else {
            String::new()
        },
    };
    let config_path = root.join("pairing.toml");
    std::fs::write(&config_path, toml::to_string(&config)?)?;
    Config::load(&config_path)?;
    drop(Store::initialize(&root.join("state"), &args[3], &replica)?);
    println!(
        "{}",
        serde_json::json!({"installation":identity.installation,"fingerprint":identity.fingerprint(),"replica":replica,"repo":args[3],"config":config_path,"runtime":root.join("state/runtime.json"),"credentials":credential_dir})
    );
    Ok(())
}
