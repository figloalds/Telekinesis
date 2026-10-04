//! Disposable process-level enrollment and crash/restart acceptance. No installed
//! service, live daemon, real credential or public listener is used.
use serde_json::{Value, json};
use std::{
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tkfs::{
    core::{Store, id},
    pairing::{CredentialSource, Identity},
    pairing_service::Config,
    runtime::{self, Discovery},
};
struct Owned(Child);
impl Drop for Owned {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn command(config: &Path, credentials: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_tkfs"));
    command.args(["pairing", "-f"]).arg(config);
    #[cfg(target_os = "linux")]
    command.env("CREDENTIALS_DIRECTORY", credentials);
    #[cfg(not(target_os = "linux"))]
    let _ = credentials;
    command
}
fn cli(config: &Path, credentials: &Path, args: &[&str]) -> Value {
    let output = command(config, credentials).args(args).output().unwrap();
    assert!(output.status.success(), "public pairing CLI command failed");
    serde_json::from_slice(&output.stdout).unwrap()
}
fn service(config: &Path, credentials: &Path, log: &Path) -> Owned {
    let log = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(log)
        .unwrap();
    Owned(
        command(config, credentials)
            .arg("run")
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap(),
    )
}
fn daemon(state: &Path, log: &Path) -> Owned {
    let log = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(log)
        .unwrap();
    Owned(
        Command::new(env!("CARGO_BIN_EXE_tkfs"))
            .arg("daemon")
            .arg("--state")
            .arg(state)
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap(),
    )
}
fn rpc(state: &Path, payload: Value) -> anyhow::Result<Value> {
    let discovery: Discovery = serde_json::from_slice(&std::fs::read(state.join("runtime.json"))?)?;
    runtime::rpc(&discovery, &id(), payload)
}
fn wait(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !predicate() {
        assert!(Instant::now() < deadline, "fixture readiness/sync timeout");
        std::thread::sleep(Duration::from_millis(50));
    }
}
fn fixture(path: &Path, ports: (&str, &str)) -> (Config, PathBuf, PathBuf, Identity) {
    std::fs::create_dir_all(path).unwrap();
    let identity = Identity::generate(&id()).unwrap();
    let credentials = path.join("credentials");
    std::fs::create_dir(&credentials).unwrap();
    #[cfg(windows)]
    let credential = {
        let source = CredentialSource::WindowsDpapi {
            file: credentials.join("identity.dpapi"),
        };
        source.save_windows(&identity).unwrap();
        source
    };
    #[cfg(target_os = "linux")]
    let credential = {
        use std::os::unix::fs::PermissionsExt;
        let bytes = zeroize::Zeroizing::new(serde_json::to_vec(&identity).unwrap());
        let file = credentials.join("tkfs.identity");
        std::fs::write(&file, &bytes).unwrap();
        std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o600)).unwrap();
        CredentialSource::Systemd {
            name: "tkfs.identity".into(),
        }
    };
    let config = Config {
        format_version: 1,
        installation: identity.installation.clone(),
        data_directory: path.join("data"),
        credential,
        listen: ports.0.into(),
        enrollment_listen: ports.1.into(),
        advertise: ports.0.into(),
        enrollment_advertise: ports.1.into(),
    };
    let config_path = path.join("pairing with spaces.toml");
    std::fs::write(&config_path, toml::to_string(&config).unwrap()).unwrap();
    (config, config_path, credentials, identity)
}
#[test]
fn cli_pairing_and_configured_sync_resume_after_both_worker_and_service_process_restarts() {
    let temp = tempfile::tempdir().unwrap();
    let reserved: Vec<_> = (0..4)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<_> = reserved
        .iter()
        .map(|listener| listener.local_addr().unwrap().to_string())
        .collect();
    let (ca, pa, ka, ia) = fixture(&temp.path().join("a"), (&ports[0], &ports[1]));
    let (cb, pb, kb, ib) = fixture(&temp.path().join("b"), (&ports[2], &ports[3]));
    drop(reserved);
    let repo = id();
    let da = id();
    let db = id();
    let sa = temp.path().join("state-a");
    let sb = temp.path().join("state-b");
    drop(Store::initialize(&sa, &repo, &da).unwrap());
    drop(Store::initialize(&sb, &repo, &db).unwrap());
    let mut wa = daemon(&sa, &temp.path().join("worker-a.log"));
    let mut wb = daemon(&sb, &temp.path().join("worker-b.log"));
    wait(|| rpc(&sa, json!({"op":"status"})).is_ok() && rpc(&sb, json!({"op":"status"})).is_ok());
    let mut service_a = service(&pa, &ka, &temp.path().join("service-a.log"));
    let mut service_b = service(&pb, &kb, &temp.path().join("service-b.log"));
    wait(|| {
        std::net::TcpStream::connect(&ca.enrollment_listen).is_ok()
            && std::net::TcpStream::connect(&cb.enrollment_listen).is_ok()
    });
    let invitation = ca
        .registry()
        .unwrap()
        .create_invitation(&ia, &ca.enrollment_advertise, &ca.advertise, 10000)
        .unwrap();
    let invitation_id = invitation.id.clone();
    let bytes = zeroize::Zeroizing::new(serde_json::to_vec(&invitation).unwrap());
    let token = zeroize::Zeroizing::new(format!("{}\n", hex::encode(&bytes)));
    let mut join = command(&pb, &kb)
        .arg("join")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    join.stdin
        .take()
        .unwrap()
        .write_all(token.as_bytes())
        .unwrap();
    assert!(
        join.wait_with_output().unwrap().status.success(),
        "masked/piped CLI enrollment failed"
    );
    cli(
        &pa,
        &ka,
        &[
            "approve",
            &invitation_id,
            "--installation",
            &ib.installation,
            "--fingerprint",
            &ib.fingerprint(),
        ],
    );
    let ra = sa.join("runtime.json");
    let rb = sb.join("runtime.json");
    cli(
        &pa,
        &ka,
        &[
            "grant",
            &ib.installation,
            "--repo",
            &repo,
            "--runtime",
            ra.to_str().unwrap(),
            "--remote-replica",
            &db,
        ],
    );
    cli(
        &pb,
        &kb,
        &[
            "grant",
            &ia.installation,
            "--repo",
            &repo,
            "--runtime",
            rb.to_str().unwrap(),
            "--remote-replica",
            &da,
        ],
    );
    rpc(
        &sa,
        json!({"op":"import","path":"first.txt","hex":hex::encode(b"first process") }),
    )
    .unwrap();
    wait(|| {
        rpc(&sb, json!({"op":"state"}))
            .is_ok_and(|value| serde_json::to_string(&value).unwrap().contains("first.txt"))
    });
    drop(service_a);
    drop(service_b);
    drop(wa);
    drop(wb);
    wa = daemon(&sa, &temp.path().join("worker-a.log"));
    wb = daemon(&sb, &temp.path().join("worker-b.log"));
    wait(|| rpc(&sa, json!({"op":"status"})).is_ok() && rpc(&sb, json!({"op":"status"})).is_ok());
    rpc(
        &sa,
        json!({"op":"import","path":"resumed.txt","hex":hex::encode(b"after both restarts") }),
    )
    .unwrap();
    service_a = service(&pa, &ka, &temp.path().join("service-a.log"));
    service_b = service(&pb, &kb, &temp.path().join("service-b.log"));
    wait(|| {
        rpc(&sb, json!({"op":"state"})).is_ok_and(|value| {
            serde_json::to_string(&value)
                .unwrap()
                .contains("resumed.txt")
        })
    });
    let status = cli(&pa, &ka, &["status"]);
    assert_eq!(status["credential"]["status"], "available");
    assert_eq!(status["syncs"].as_array().unwrap().len(), 1);
    cli(&pa, &ka, &["revoke", &ib.installation]);
    rpc(
        &sa,
        json!({"op":"import","path":"revoked.txt","hex":hex::encode(b"denied") }),
    )
    .unwrap();
    let sync = command(&pb, &kb)
        .args(["sync", &ia.installation, "--repo", &repo])
        .output()
        .unwrap();
    assert!(!sync.status.success());
    assert!(
        !serde_json::to_string(&rpc(&sb, json!({"op":"state"})).unwrap())
            .unwrap()
            .contains("revoked.txt")
    );
    drop(service_a);
    drop(service_b);
    drop(wa);
    drop(wb);
}
