//! Isolated headless supervisor only: no mounts, credentials or TCP listeners.
use std::{
    fs,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tkfs::orchestrator::{self, Action, Config, Request};
use zeroize::Zeroizing;

struct Owned(Child);
impl Drop for Owned {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn command(root: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_tkfs"));
    command.current_dir(root).stdin(Stdio::null());
    command
}
fn reset(root: &Path) -> Zeroizing<String> {
    let mut output = command(root)
        .args(["manage", "reset-pairing-code"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "fixture reset failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "reset must not emit a retry receipt"
    );
    let code = Zeroizing::new(String::from_utf8(std::mem::take(&mut output.stdout)).unwrap());
    let code = Zeroizing::new(code.trim_end_matches(['\r', '\n']).to_owned());
    assert!(code.len() == 200 && code.bytes().all(|b| b.is_ascii_digit()));
    code
}

#[test]
fn reset_cli_rotates_only_fixture_verifier_and_never_journals_code() {
    let root = tempfile::tempdir().unwrap();
    let initialized = command(root.path()).arg("init").output().unwrap();
    assert!(
        initialized.status.success(),
        "fixture init failed: {}",
        String::from_utf8_lossy(&initialized.stderr)
    );
    let path = root.path().join("orchestrator.toml");
    let before = fs::read(&path).unwrap();
    let config = Config::load(&path).unwrap();
    let _child = Owned(
        command(root.path())
            .arg("start")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        let hello = orchestrator::client(
            &config,
            &Request {
                version: 1,
                target_installation: config.installation_id.clone(),
                operation_id: None,
                expected_generation: None,
                action: Action::Hello,
            },
        );
        if hello.is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "fixture supervisor readiness timeout"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let first = reset(root.path());
    let second = reset(root.path());
    assert!(
        first.as_str() != second.as_str(),
        "rotation must generate a new code"
    );
    assert_eq!(fs::read(&path).unwrap(), before);
    let db = rusqlite::Connection::open_with_flags(
        config.data_directory.join("orchestrator.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let operations: u32 = db
        .query_row("SELECT count(*) FROM operations", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        operations, 0,
        "reset must bypass durable operation receipts"
    );
    let states: u32 = db
        .query_row("SELECT count(*) FROM states", [], |r| r.get(0))
        .unwrap();
    assert_eq!(states, 0, "reset must not create or start a state");
    for entry in fs::read_dir(root.path().join("orchestrator.toml.pairing")).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            let bytes = fs::read(entry.path()).unwrap();
            for secret in [&first, &second] {
                assert!(
                    !bytes.windows(200).any(|window| window == secret.as_bytes()),
                    "verifier files must not contain the original code"
                );
            }
        }
    }
}

#[test]
fn reset_refuses_replay_flags_and_join_has_no_secret_argument() {
    let root = tempfile::tempdir().unwrap();
    for flag in ["--request-id", "--generation"] {
        let output = command(root.path())
            .args([flag, "1", "manage", "reset-pairing-code"])
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("PAIRING_CODE_RESET_NOT_REPLAYABLE")
        );
    }
    let output = command(root.path())
        .args(["pairing", "join-code", "--code", "secret-sentinel"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    // Help contains only public bootstrap/endpoint/fingerprint arguments.
    let help = command(root.path())
        .args(["pairing", "join-code", "--help"])
        .output()
        .unwrap();
    assert!(help.status.success());
    let help = String::from_utf8_lossy(&help.stdout);
    assert!(
        help.contains("--issuer") && help.contains("--fingerprint") && !help.contains("--code")
    );
    assert!(!root.path().join("orchestrator.toml.pairing").exists());
}
