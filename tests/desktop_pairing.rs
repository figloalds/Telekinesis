#![cfg(windows)]
//! Disposable same-machine WSS desktop acceptance. Owns only fixture processes,
//! protected fixture keys and one fixture mount; no service/ACL/network setup.
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    io::Write,
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, atomic::AtomicBool},
    time::{Duration, Instant},
};
use tkfs::{
    core::{Store, id},
    desktop_pairing::{Client, error_message},
    pairing::{CredentialSource, Identity},
    pairing_service::{Config, Transport},
    runtime::{self, Discovery},
};
use zeroize::Zeroizing;

struct Owned(Child);
impl Drop for Owned {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn daemon(state: &Path, mount: Option<&Path>) -> Owned {
    let mut command = Command::new(env!("CARGO_BIN_EXE_tkfs"));
    command
        .args(["daemon", "--state"])
        .arg(state)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(mount) = mount {
        command.arg("--mount").arg(mount);
    }
    Owned(command.spawn().unwrap())
}
fn wait(mut predicate: impl FnMut() -> bool) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(25);
    while !predicate() {
        ensure!(
            Instant::now() < deadline,
            "Disposable pairing fixture readiness timeout"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}
fn rpc(state: &Path, payload: Value) -> Result<Value> {
    let discovery: Discovery = serde_json::from_slice(&std::fs::read(state.join("runtime.json"))?)?;
    runtime::rpc(&discovery, &id(), payload)
}

#[test]
fn desktop_client_refuses_downgrade_changed_configuration_and_secret_error_echo() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("client/client.toml");
    let client = Client::create(&path).unwrap();
    let config = Config::load(&path).unwrap();
    let before = std::fs::read(&path).unwrap();
    assert!(Client::create(&path).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    // Simulate interruption after the durable key but before config publication.
    let fingerprint = config.identity().unwrap().fingerprint();
    drop(client);
    std::fs::remove_file(&path).unwrap();
    let mut client = Client::create(&path).unwrap();
    assert_eq!(
        Config::load(&path)
            .unwrap()
            .identity()
            .unwrap()
            .fingerprint(),
        fingerprint
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert!(
        client
            .inspect(
                Zeroizing::new("invalid-secret-sentinel".into()),
                "ws://127.0.0.1:1234/tkfs/sync"
            )
            .is_err()
    );
    assert!(
        client
            .join(
                "wss://127.0.0.1:1234/tkfs/sync",
                false,
                Arc::new(AtomicBool::new(false))
            )
            .is_err()
    );
    assert!(config.registry().unwrap().peers().unwrap().is_empty());
    let message = error_message(&anyhow::anyhow!(
        "untrusted remote echo invalid-secret-sentinel"
    ));
    assert!(!message.contains("invalid-secret-sentinel"));
    let mut changed = config;
    changed.installation = id();
    std::fs::write(&path, toml::to_string(&changed).unwrap()).unwrap();
    assert!(
        client
            .refresh()
            .unwrap_err()
            .to_string()
            .contains("CONFIG_CHANGED")
    );
    let mut downgrade = changed;
    downgrade.transport = Transport::Tls;
    downgrade.inbound = true;
    downgrade.listen = "127.0.0.1:1234".into();
    downgrade.enrollment_listen = "127.0.0.1:1235".into();
    downgrade.advertise = downgrade.listen.clone();
    downgrade.enrollment_advertise = downgrade.enrollment_listen.clone();
    std::fs::write(&path, toml::to_string(&downgrade).unwrap()).unwrap();
    assert!(Client::open(&path).is_err());
}

#[test]
fn desktop_config_create_only_publication_recovers_open_partial_and_synced_crashes() {
    if let Some(path) = std::env::var_os("TKFS_DESKTOP_CONFIG_FAULT_FIXTURE") {
        Client::create(Path::new(&path)).unwrap();
        panic!("Fault injection did not terminate the fixture child");
    }
    let temp = tempfile::tempdir().unwrap();
    for point in [
        "desktop_pairing_config_opened",
        "desktop_pairing_config_write_started",
        "desktop_pairing_config_ready",
    ] {
        let config = temp.path().join(point).join("client.toml");
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "desktop_config_create_only_publication_recovers_open_partial_and_synced_crashes",
                "--nocapture",
            ])
            .env("TKFS_DESKTOP_CONFIG_FAULT_FIXTURE", &config)
            .env("TKFS_FAULT", point)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86));
        assert!(!config.exists(), "Incomplete config was published");
        let setup: Value = serde_json::from_slice(
            &std::fs::read(config.parent().unwrap().join("client-setup.json")).unwrap(),
        )
        .unwrap();
        let source = CredentialSource::WindowsDpapi {
            file: config.parent().unwrap().join("identity.dpapi"),
        };
        let fingerprint = source
            .load(setup["installation"].as_str().unwrap())
            .unwrap()
            .fingerprint();
        let key_bytes = std::fs::read(config.parent().unwrap().join("identity.dpapi")).unwrap();
        let _ = Client::create(&config).unwrap();
        assert_eq!(
            Config::load(&config)
                .unwrap()
                .identity()
                .unwrap()
                .fingerprint(),
            fingerprint
        );
        assert_eq!(
            std::fs::read(config.parent().unwrap().join("identity.dpapi")).unwrap(),
            key_bytes
        );
    }
}

#[test]
fn native_gui_wss_pairing_light_dark_protected_keys_mount_privacy_and_revocation() -> Result<()> {
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_tkfs"));
    let evidence = std::env::var_os("TKFS_DESKTOP_PAIRING_EVIDENCE").map(PathBuf::from);
    for theme in ["light", "dark"] {
        let temp = tempfile::tempdir()?;
        let root = temp.path();
        let server_root = root.join("server");
        tkfs::private_storage::Directory::open(&server_root)?;
        let server_identity = Identity::generate(&id())?;
        let credential = CredentialSource::WindowsDpapi {
            file: server_root.join("identity.dpapi"),
        };
        credential.save_windows(&server_identity)?;
        let address = TcpListener::bind("127.0.0.1:0")?.local_addr()?;
        let server = Config {
            format_version: 1,
            installation: server_identity.installation.clone(),
            data_directory: server_root.join("registry"),
            credential,
            transport: Transport::Wss,
            inbound: true,
            dial: false,
            listen: address.to_string(),
            enrollment_listen: String::new(),
            advertise: format!("wss://{address}/tkfs/sync"),
            enrollment_advertise: format!("wss://{address}/tkfs/enroll"),
        };
        let server_config = server_root.join("server.toml");
        std::fs::write(&server_config, toml::to_string(&server)?)?;
        let service = Owned(
            Command::new(&binary)
                .args(["pairing", "-f"])
                .arg(&server_config)
                .arg("run")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()?,
        );
        wait(|| std::net::TcpStream::connect(address).is_ok())
            .context("WSS fixture listener readiness")?;
        let client_config = root.join("client/client.toml");
        let _ = Client::create(&client_config)?;
        let repo = id();
        let server_state = root.join("server-state");
        let client_state = root.join("client-state");
        {
            let mut store = Store::initialize(&server_state, &repo, &id())?;
            let file = store.create("published.txt", "file")?;
            // This fixture is a sequential edit. None is an explicit unbased
            // write, leaving the initial empty revision as a concurrent head.
            store.write_revision(
                &file.id,
                file.revisions.get("content").cloned(),
                b"published desktop pairing bytes",
            )?;
            ensure!(
                store.read_object(
                    store
                        .lookup("published.txt")?
                        .content
                        .as_deref()
                        .context("Fixture source content")?
                )? == b"published desktop pairing bytes",
                "Fixture source visible bytes"
            );
            ensure!(
                store.conflicts()?.is_empty(),
                "Fixture source has unresolved conflicts"
            );
            store.fork("fixture-private", false)?;
            store.checkout("fixture-private")?;
            let file = store.create("private-canary.txt", "file")?;
            store.write_revision(
                &file.id,
                file.revisions.get("content").cloned(),
                b"private desktop pairing canary",
            )?;
            store.checkout("main")?;
        }
        drop(Store::initialize(&client_state, &repo, &id())?);
        let mount = root.join("Client mounted view");
        let server_worker = daemon(&server_state, None);
        let client_worker = daemon(&client_state, Some(&mount));
        wait(|| {
            rpc(&server_state, json!({"op":"status"})).is_ok()
                && rpc(&client_state, json!({"op":"status"})).is_ok()
                && mount.exists()
        })
        .context("Fixture workers/mount readiness")?;
        let wrong_runtime = root.join("wrong-runtime.json");
        std::fs::write(
            &wrong_runtime,
            serde_json::to_vec(
                &json!({"format":1,"repo":id(),"device":id(),"state":root,"address":"pipe:unused","mount":null,"token":""}),
            )?,
        )?;
        let invitation = server.registry()?.create_invitation(
            &server_identity,
            &server.enrollment_advertise,
            &server.advertise,
            300000,
        )?;
        let token = Zeroizing::new(hex::encode(Zeroizing::new(serde_json::to_vec(
            &invitation,
        )?)));
        let output_dir = root.join("gui");
        std::fs::create_dir(&output_dir)?;
        let report = output_dir.join("report.json");
        let stdout = output_dir.join("stdout.log");
        let stderr = output_dir.join("stderr.log");
        let mut gui = Owned(
            Command::new(&binary)
                .args(["gui-test", "--report"])
                .arg(&report)
                .env("TKFS_UI_TEST_PHASE", "pairing")
                .env("TKFS_UI_TEST_THEME", theme)
                .stdin(Stdio::piped())
                .stdout(std::fs::File::create(&stdout)?)
                .stderr(std::fs::File::create(&stderr)?)
                .spawn()?,
        );
        let input = Zeroizing::new(serde_json::to_vec(
            &json!({"token":*token,"endpoint":server.advertise,"client_config":client_config,"server_config":server_config,"client_runtime":client_state.join("runtime.json"),"server_runtime":server_state.join("runtime.json"),"wrong_runtime":wrong_runtime}),
        )?);
        gui.0
            .stdin
            .take()
            .context("GUI_FIXTURE_STDIN_REQUIRED")?
            .write_all(&input)?;
        let deadline = Instant::now() + Duration::from_secs(115);
        while gui.0.try_wait()?.is_none() {
            ensure!(Instant::now() < deadline, "GUI_PAIRING_TIMEOUT");
            std::thread::sleep(Duration::from_millis(100));
        }
        let result: Value =
            serde_json::from_slice(&std::fs::read(&report).context("GUI_REPORT_MISSING")?)?;
        ensure!(
            result["passed"] == true,
            "GUI_PAIRING_ACCEPTANCE_FAILED: {}",
            result["error"]
        );
        let projection = rpc(&client_state, json!({"op":"state"}))?;
        let entry = projection["entries"]
            .as_object()
            .context("Fixture projection entries")?
            .values()
            .find(|entry| entry["name"] == "published.txt" && entry["alive"] == true)
            .context("Fixture published file projection")?;
        let object = entry["content"]
            .as_str()
            .context("Fixture published content register")?;
        let verified = rpc(&client_state, json!({"op":"cat-object","object":object}))?;
        ensure!(
            verified["hex"] == hex::encode(b"published desktop pairing bytes"),
            "Fixture worker committed unexpected published content"
        );
        wait(|| {
            std::fs::read(mount.join("published.txt")).ok().as_deref()
                == Some(b"published desktop pairing bytes")
        })
        .context("Fixture mounted bytes after worker reported caught-up")?;
        assert_eq!(
            std::fs::read(mount.join("published.txt"))?,
            b"published desktop pairing bytes"
        );
        assert!(!mount.join("private-canary.txt").exists());
        assert_eq!(
            rpc(&client_state, json!({"op":"status"}))?["branch"]["name"],
            "main"
        );
        assert!(
            !rpc(&client_state, json!({"op":"branches"}))?
                .to_string()
                .contains("fixture-private")
        );
        for path in [&stdout, &stderr, &report, &client_config] {
            assert!(
                !std::fs::read_to_string(path)?.contains(token.as_str()),
                "Invitation secret leaked into fixture output"
            );
        }
        let mut reopened = Client::open(&client_config)?;
        assert!(reopened.snapshot().peers.iter().all(|p| p.2));
        assert!(
            reopened
                .list(&server.installation, Arc::new(AtomicBool::new(false)))
                .is_err()
        );
        if let Some(evidence) = &evidence {
            let directory = evidence.join(theme);
            std::fs::create_dir_all(&directory)?;
            for entry in std::fs::read_dir(&output_dir)? {
                let entry = entry?;
                std::fs::copy(entry.path(), directory.join(entry.file_name()))?;
            }
        }
        drop(gui);
        drop(client_worker);
        drop(server_worker);
        drop(service);
        wait(|| !mount.exists()).context("Fixture mount cleanup")?;
        println!(
            "{theme}: desktop GUI WSS flow, mounted published bytes, private isolation, secret-free outputs, durable revocation passed"
        );
    }
    Ok(())
}
