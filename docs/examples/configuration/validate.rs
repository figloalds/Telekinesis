//! Documentation-only parser harness. Run on an isolated copy of the examples.
//! No credential loading, supervisor/registry opening, network or mount calls.
use std::{collections::BTreeMap, fs, path::{Path, PathBuf}};
use tkfs::{orchestrator, pairing::CredentialSource, pairing_service::{Config, Transport}};

fn inventory(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut entries = BTreeMap::new();
    for entry in fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() { entries.extend(inventory(&path)); }
        else { entries.insert(path.clone(), fs::read(path).unwrap()); }
    }
    entries
}

fn main() {
    let root = fs::canonicalize(std::env::args_os().nth(1).expect("isolated examples path")).unwrap();
    let before = inventory(&root);
    let (native, foreign) = if cfg!(windows) { ("windows", "linux") } else { ("linux", "windows") };
    let o1 = root.join(format!("orchestrator.{native}.toml"));
    let config = orchestrator::Config::load(&o1).unwrap();
    assert_eq!(config.data_directory, root.join("data"));
    assert!(config.installation_id.is_none());
    assert!(config.default_mount_directory.is_none());
    assert!(orchestrator::Config::load(&root.join(format!("orchestrator.{foreign}.toml"))).is_err());
    if cfg!(windows) {
        let expanded = orchestrator::Config::load(&root.join("orchestrator.expanded.toml")).unwrap();
        assert_eq!(expanded.default_mount_directory.unwrap(), root.join("Projects"));
    }
    let client_path = root.join("data/desktop-pairing/client.toml");
    let client = Config::load(&client_path).unwrap();
    assert_eq!(client.transport, Transport::Wss);
    assert!(!client.inbound && client.dial && client.orchestrator_config.is_none());
    assert_eq!(client.advertise, "outbound-only");
    assert!(client.listen.is_empty() && client.enrollment_listen.is_empty() && client.enrollment_advertise.is_empty());
    assert_eq!(client.data_directory, client_path.parent().unwrap().join("registry"));
    match client.credential {
        CredentialSource::PlatformEncrypted { file, .. } => assert_eq!(file, client_path.parent().unwrap().join("identity.encrypted")),
        _ => panic!("wrong managed credential schema"),
    }
    let server = Config::load(&root.join(format!("data/receiving-pairing/server.{native}.toml"))).unwrap();
    assert!(server.inbound && server.dial);
    assert_eq!(server.orchestrator_config.unwrap(), fs::canonicalize(&o1).unwrap());
    assert!(Config::load(&root.join(format!("data/receiving-pairing/server.{foreign}.toml"))).is_err());
    let tls = Config::load(&root.join("advanced/tls-systemd.toml")).unwrap();
    assert_eq!(tls.transport, Transport::Tls);
    assert!(tls.inbound && tls.dial);
    assert!(matches!(tls.credential, CredentialSource::Systemd { .. }));
    let dpapi = Config::load(&root.join("advanced/wss-dpapi.toml")).unwrap();
    assert!(matches!(dpapi.credential, CredentialSource::WindowsDpapi { .. }));
    assert_eq!(before, inventory(&root), "Config::load changed a file");
    assert!(!root.join("data/orchestrator.sqlite").exists());
    assert!(!client_path.parent().unwrap().join("registry").exists());
    println!("PASS: native O1/receiver, client, advanced TLS/DPAPI, defaults, relative paths, foreign-platform rejection; inputs unchanged; no credentials, catalogs or listeners opened");
}
