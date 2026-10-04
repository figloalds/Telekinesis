#![cfg(target_os = "linux")]
use serde_json::Value;
use std::{os::unix::fs::PermissionsExt, path::Path, process::Command};
use tkfs::{
    core::id,
    pairing::{CredentialSource, Identity},
    pairing_service::Config,
};
fn status(config: &Path, directory: Option<&Path>) -> Value {
    let mut command = Command::new(env!("CARGO_BIN_EXE_tkfs"));
    command
        .args(["pairing", "-f"])
        .arg(config)
        .arg("status")
        .env_remove("CREDENTIALS_DIRECTORY");
    if let Some(directory) = directory {
        command.env("CREDENTIALS_DIRECTORY", directory);
    }
    let output = command.output().unwrap();
    assert!(output.status.success(), "credential status command failed");
    serde_json::from_slice(&output.stdout).unwrap()
}
#[test]
fn systemd_source_reports_locked_missing_invalid_and_refuses_unprotected_or_symlink_credentials() {
    let temp = tempfile::tempdir().unwrap();
    let identity = Identity::generate(&id()).unwrap();
    let config = Config {
        format_version: 1,
        installation: identity.installation.clone(),
        data_directory: temp.path().join("private-state"),
        credential: CredentialSource::Systemd {
            name: "tkfs.identity".into(),
        },
        listen: "127.0.0.1:0".into(),
        enrollment_listen: "127.0.0.1:0".into(),
        advertise: "127.0.0.1:0".into(),
        enrollment_advertise: "127.0.0.1:0".into(),
    };
    let config_path = temp.path().join("pairing with spaces.toml");
    std::fs::write(&config_path, toml::to_string(&config).unwrap()).unwrap();
    let credential = temp.path().join("tkfs.identity");
    assert!(
        status(&config_path, None)["credential"]["error"]
            .as_str()
            .unwrap()
            .contains("CREDENTIAL_LOCKED")
    );
    assert!(
        status(&config_path, Some(temp.path()))["credential"]["error"]
            .as_str()
            .unwrap()
            .contains("CREDENTIAL_MISSING")
    );
    // Disposable test fixture ONLY: production CLI never writes this plaintext.
    let serialized = zeroize::Zeroizing::new(serde_json::to_vec(&identity).unwrap());
    std::fs::write(&credential, &serialized).unwrap();
    std::fs::set_permissions(&credential, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(
        status(&config_path, Some(temp.path()))["credential"]["status"],
        "available"
    );
    std::fs::set_permissions(&credential, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(
        status(&config_path, Some(temp.path()))["credential"]["error"]
            .as_str()
            .unwrap()
            .contains("CREDENTIAL_ACCESS_REFUSED")
    );
    std::fs::set_permissions(&credential, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::write(&credential, b"invalid fixture").unwrap();
    assert!(
        status(&config_path, Some(temp.path()))["credential"]["error"]
            .as_str()
            .unwrap()
            .contains("CREDENTIAL_INVALID")
    );
    std::fs::remove_file(&credential).unwrap();
    let target = temp.path().join("fixture-target");
    std::fs::write(&target, &serialized).unwrap();
    std::os::unix::fs::symlink(target, &credential).unwrap();
    assert_ne!(
        status(&config_path, Some(temp.path()))["credential"]["status"],
        "available"
    );
    assert!(
        CredentialSource::Systemd {
            name: "../fixture-target".into()
        }
        .load(&identity.installation)
        .is_err()
    );
}
