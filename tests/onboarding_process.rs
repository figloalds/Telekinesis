//! Owned foreground process fixtures; never use an installed service or live state.
use serde_json::Value;
use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tkfs::{
    core::id,
    orchestrator::{self, Action, Config, Request},
};
fn command(root: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_tkfs"));
    cmd.current_dir(root);
    cmd
}
fn output(root: &Path, args: &[&str]) -> std::process::Output {
    command(root).args(args).output().unwrap()
}
fn cli(root: &Path, args: &[&str]) -> Value {
    let result = output(root, args);
    assert!(
        result.status.success(),
        "{}: {}",
        args.join(" "),
        String::from_utf8_lossy(&result.stderr)
    );
    serde_json::from_slice(&result.stdout).unwrap()
}
fn wait(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(35);
    while !ready() {
        assert!(Instant::now() < deadline, "owned fixture readiness timeout");
        std::thread::sleep(Duration::from_millis(50));
    }
}
fn api(
    config: &Config,
    action: Action,
    operation: Option<String>,
    generation: Option<u64>,
) -> Value {
    orchestrator::client(
        config,
        &Request {
            version: 1,
            target_installation: config.installation_id.clone(),
            operation_id: operation,
            expected_generation: generation,
            action,
        },
    )
    .unwrap()
}
struct Fixture {
    root: tempfile::TempDir,
    supervisor: Option<Child>,
    config: Config,
}
impl Fixture {
    fn retain_logs(&self) {
        let Some(evidence) = std::env::var_os("TKFS_TEST_EVIDENCE") else {
            return;
        };
        let evidence = PathBuf::from(evidence);
        assert!(
            evidence.is_absolute(),
            "fixture evidence must be explicitly scoped"
        );
        let destination = evidence.join(self.config.installation_id.as_deref().unwrap());
        fs::create_dir_all(&destination).unwrap();
        let log = self.root.path().join("supervisor.log");
        if log.is_file() {
            fs::copy(log, destination.join("supervisor.log")).unwrap();
        }
        if let Ok(logs) = fs::read_dir(self.config.data_directory.join("logs")) {
            for entry in logs {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_file() {
                    fs::copy(entry.path(), destination.join(entry.file_name())).unwrap();
                }
            }
        }
    }
    fn new() -> Self {
        Self::in_root(tempfile::tempdir().unwrap())
    }
    fn in_root(root: tempfile::TempDir) -> Self {
        cli(root.path(), &["init"]);
        let config = Config::load(&root.path().join("orchestrator.toml")).unwrap();
        let mut fixture = Self {
            root,
            supervisor: None,
            config,
        };
        fixture.start(None);
        fixture
    }
    fn start(&mut self, fault: Option<&str>) {
        let mut cmd = command(self.root.path());
        cmd.arg("start");
        if let Some(fault) = fault {
            cmd.env("TKFS_FAULT", fault);
        }
        let log = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(self.root.path().join("supervisor.log"))
            .unwrap();
        self.supervisor = Some(cmd.stdout(Stdio::null()).stderr(log).spawn().unwrap());
        wait(|| {
            orchestrator::client(
                &self.config,
                &Request {
                    version: 1,
                    target_installation: self.config.installation_id.clone(),
                    operation_id: None,
                    expected_generation: None,
                    action: Action::Hello,
                },
            )
            .is_ok()
        });
    }
    fn crash_supervisor(&mut self) {
        let mut child = self.supervisor.take().unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
    }
    fn state(&self, state: &str) -> Value {
        api(
            &self.config,
            Action::Inspect {
                state: state.into(),
            },
            None,
            None,
        )["result"]
            .clone()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if self.supervisor.is_none()
            && !self
                .config
                .data_directory
                .join("orchestrator.sqlite")
                .is_file()
        {
            self.retain_logs();
            return;
        }
        if self.supervisor.is_none() {
            self.start(None);
        }
        let request = Request {
            version: 1,
            target_installation: self.config.installation_id.clone(),
            operation_id: None,
            expected_generation: None,
            action: Action::List,
        };
        if let Ok(list) = orchestrator::client(&self.config, &request) {
            let _ = orchestrator::client(
                &self.config,
                &Request {
                    action: Action::Shutdown,
                    operation_id: Some(id()),
                    expected_generation: list["result"]["catalog_generation"].as_u64(),
                    ..request
                },
            );
        }
        if let Some(mut child) = self.supervisor.take() {
            let deadline = Instant::now() + Duration::from_secs(10);
            while child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(50));
            }
            let _ = child.kill();
            let _ = child.wait();
        }
        self.retain_logs();
    }
}
#[test]
fn init_is_create_only_concurrent_and_preserves_standalone_compatibility() {
    let root = tempfile::tempdir().unwrap();
    let children: Vec<_> = (0..6)
        .map(|_| {
            command(root.path())
                .arg("init")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap()
        })
        .collect();
    let mut identities = std::collections::BTreeSet::new();
    for child in children {
        let result = child.wait_with_output().unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let value: Value = serde_json::from_slice(&result.stdout).unwrap();
        identities.insert(value["installation_id"].clone().to_string());
    }
    assert_eq!(identities.len(), 1);
    let path = root.path().join("orchestrator.toml");
    let bytes = fs::read(&path).unwrap();
    cli(root.path(), &["init"]);
    assert_eq!(fs::read(&path).unwrap(), bytes);
    let parsed: toml::Value = toml::from_str(std::str::from_utf8(&bytes).unwrap()).unwrap();
    assert_eq!(parsed["data_directory"].as_str(), Some(".tkfs/data"));
    assert_eq!(parsed["network"]["enabled"].as_bool(), Some(false));
    assert_eq!(parsed["workers"]["maximum_running"].as_integer(), Some(8));
    let first = cli(root.path(), &["init", "--state", "standalone"]);
    let second = cli(root.path(), &["init", "--state", "standalone"]);
    assert_eq!(first["repo"], second["repo"]);
    assert_eq!(first["device"], second["device"]);
    fs::write(&path, "malformed [").unwrap();
    assert!(!output(root.path(), &["init"]).status.success());
    assert_eq!(fs::read_to_string(path).unwrap(), "malformed [");
    let conflict = tempfile::tempdir().unwrap();
    fs::create_dir_all(conflict.path().join(".tkfs/data")).unwrap();
    fs::write(conflict.path().join(".tkfs/data/precious"), b"keep").unwrap();
    assert!(!output(conflict.path(), &["init"]).status.success());
    assert!(!conflict.path().join("orchestrator.toml").exists());
    assert_eq!(
        fs::read(conflict.path().join(".tkfs/data/precious")).unwrap(),
        b"keep"
    );
}
#[test]
fn publication_crashes_leave_only_absent_or_complete_config_and_reuse_published_identity() {
    for fault in ["onboarding_config_ready", "onboarding_config_published"] {
        let root = tempfile::tempdir().unwrap();
        let result = command(root.path())
            .arg("init")
            .env("TKFS_FAULT", fault)
            .output()
            .unwrap();
        assert_eq!(result.status.code(), Some(86));
        let config = root.path().join("orchestrator.toml");
        let before = fs::read(&config).ok();
        if let Some(bytes) = &before {
            let _: toml::Value = toml::from_str(std::str::from_utf8(bytes).unwrap()).unwrap();
        }
        cli(root.path(), &["init"]);
        if let Some(bytes) = before {
            assert_eq!(fs::read(config).unwrap(), bytes);
        }
    }
}
#[test]
fn foreground_headless_receipts_reattach_and_missing_catalog_are_safe() {
    let mut fixture = Fixture::new();
    let generation =
        api(&fixture.config, Action::List, None, None)["result"]["catalog_generation"].as_u64();
    let operation = id();
    let request = Action::Create {
        label: "Headless".into(),
        mount: None,
    };
    let first = api(
        &fixture.config,
        request.clone(),
        Some(operation.clone()),
        generation,
    );
    assert_eq!(first["ok"], true);
    assert_eq!(
        api(&fixture.config, request, Some(operation), generation),
        first
    );
    let state = first["result"]["state_id"].as_str().unwrap();
    let old = fixture.state(state)["observation"]["instance"].clone();
    let state_root = fixture.config.data_directory.join("states").join(state);
    assert!(
        !output(
            fixture.root.path(),
            &["daemon", "--state", state_root.to_str().unwrap()]
        )
        .status
        .success()
    );
    fixture.crash_supervisor();
    fixture.start(None);
    assert_eq!(
        fixture.state(state)["observation"]["instance"],
        old,
        "restart replaced a surviving authenticated worker"
    );
    let other = tempfile::tempdir().unwrap();
    let mut wrong = Config::load(&fixture.root.path().join("orchestrator.toml")).unwrap();
    wrong.data_directory = other.path().to_path_buf();
    let hello = Request {
        version: 1,
        target_installation: wrong.installation_id.clone(),
        operation_id: None,
        expected_generation: None,
        action: Action::Hello,
    };
    assert!(orchestrator::client(&wrong, &hello).is_err());
    let generation = fixture.state(state)["management_generation"].as_u64();
    let stopped = api(
        &fixture.config,
        Action::Stop {
            state: state.into(),
        },
        Some(id()),
        generation,
    );
    assert_eq!(stopped["ok"], true);
    let list = api(&fixture.config, Action::List, None, None);
    api(
        &fixture.config,
        Action::Shutdown,
        Some(id()),
        list["result"]["catalog_generation"].as_u64(),
    );
    let mut child = fixture.supervisor.take().unwrap();
    child.wait().unwrap();
    fs::remove_file(fixture.config.data_directory.join("orchestrator.sqlite")).unwrap();
    assert!(!output(fixture.root.path(), &["start"]).status.success());
    assert!(!output(fixture.root.path(), &["init"]).status.success());
    // Do not ask Drop to recreate a deliberately damaged fixture catalog.
}
#[test]
#[ignore = "requires installed WinFsp or /dev/fuse and fusermount3; owned fixtures only"]
fn real_mount_create_private_busy_retry_restart_and_linux_crash_recovery() {
    let mut fixture = Fixture::new();
    let project = cli(fixture.root.path(), &["create", "MyProject"]);
    let repeated = cli(fixture.root.path(), &["create", "MyProject"]);
    assert_eq!(project["state_id"], repeated["state_id"]);
    let state = project["state_id"].as_str().unwrap();
    let mount = PathBuf::from(project["mount"].as_str().unwrap());
    let mut file = File::create(mount.join("saved.txt")).unwrap();
    file.write_all(b"durable managed mount").unwrap();
    file.sync_all().unwrap();
    drop(file);
    fs::rename(mount.join("saved.txt"), mount.join("renamed.txt")).unwrap();
    fs::write(mount.join("delete.txt"), b"remove").unwrap();
    fs::remove_file(mount.join("delete.txt")).unwrap();
    let runtime = fixture
        .config
        .data_directory
        .join("states")
        .join(state)
        .join("runtime.json");
    cli(
        fixture.root.path(),
        &["--runtime", runtime.to_str().unwrap(), "branch", "private"],
    );
    cli(
        fixture.root.path(),
        &[
            "--runtime",
            runtime.to_str().unwrap(),
            "checkout",
            "private",
        ],
    );
    fs::write(mount.join("secret.txt"), b"private managed bytes").unwrap();
    cli(
        fixture.root.path(),
        &["--runtime", runtime.to_str().unwrap(), "checkout", "main"],
    );
    assert!(!mount.join("secret.txt").exists());
    let second = cli(fixture.root.path(), &["create", "Second"]);
    let second_mount = PathBuf::from(second["mount"].as_str().unwrap());
    let held = File::open(mount.join("renamed.txt")).unwrap();
    let generation = fixture.state(state)["management_generation"].as_u64();
    let operation = id();
    let pending = api(
        &fixture.config,
        Action::Stop {
            state: state.into(),
        },
        Some(operation.clone()),
        generation,
    );
    assert_eq!(pending["ok"], false);
    assert_eq!(pending["retryable"], true);
    fs::write(second_mount.join("still-usable.txt"), b"independent").unwrap();
    assert_eq!(
        fs::read(second_mount.join("still-usable.txt")).unwrap(),
        b"independent"
    );
    drop(held);
    let complete = api(
        &fixture.config,
        Action::Stop {
            state: state.into(),
        },
        Some(operation.clone()),
        generation,
    );
    assert_eq!(complete["ok"], true);
    assert_eq!(
        api(
            &fixture.config,
            Action::Stop {
                state: state.into()
            },
            Some(operation),
            generation
        ),
        complete
    );
    let generation = fixture.state(state)["management_generation"].as_u64();
    assert_eq!(
        api(
            &fixture.config,
            Action::Start {
                state: state.into()
            },
            Some(id()),
            generation
        )["ok"],
        true
    );
    assert_eq!(
        fs::read(mount.join("renamed.txt")).unwrap(),
        b"durable managed mount"
    );
    let old = fixture.state(state)["observation"]["instance"].clone();
    fixture.crash_supervisor();
    fixture.start(None);
    assert_eq!(fixture.state(state)["observation"]["instance"], old);
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::{AsRawFd, FromRawFd};
        let pid = fixture.state(state)["observation"]["pid"].as_i64().unwrap();
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) } as i32;
        assert!(fd >= 0);
        let process = unsafe { File::from_raw_fd(fd) };
        assert_eq!(
            unsafe {
                libc::syscall(
                    libc::SYS_pidfd_send_signal,
                    process.as_raw_fd(),
                    libc::SIGKILL,
                    std::ptr::null::<libc::siginfo_t>(),
                    0,
                )
            },
            0
        );
        wait(|| {
            let info = fixture.state(state);
            info["observation"]["instance"] != old && info["observation"]["mounted"] == true
        });
        assert_eq!(
            fs::read(mount.join("renamed.txt")).unwrap(),
            b"durable managed mount"
        );
    }
    let count = api(&fixture.config, Action::List, None, None)["result"]["states"]
        .as_array()
        .unwrap()
        .len();
    assert_eq!(count, 2);
    let before_cold = fixture.state(state)["observation"]["instance"].clone();
    let catalog = api(&fixture.config, Action::List, None, None);
    assert_eq!(
        api(
            &fixture.config,
            Action::Shutdown,
            Some(id()),
            catalog["result"]["catalog_generation"].as_u64()
        )["ok"],
        true
    );
    fixture.supervisor.take().unwrap().wait().unwrap();
    fixture.start(None);
    assert_ne!(fixture.state(state)["observation"]["instance"], before_cold);
    assert_eq!(
        fs::read(mount.join("renamed.txt")).unwrap(),
        b"durable managed mount"
    );
    assert_eq!(
        fs::read(second_mount.join("still-usable.txt")).unwrap(),
        b"independent"
    );
}

#[test]
#[ignore = "requires installed WinFsp or /dev/fuse and fusermount3; owned fixtures only"]
fn convenient_create_survives_delivery_crashes_and_concurrent_generations() {
    let mut fixture = Fixture::new();
    let children: Vec<_> = (0..4)
        .map(|number| {
            command(fixture.root.path())
                .args(["create", &format!("Concurrent{number}")])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap()
        })
        .collect();
    let mut states = std::collections::BTreeSet::new();
    for child in children {
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        states.insert(value["state_id"].clone().to_string());
    }
    assert_eq!(states.len(), 4);
    // Publication of the request precedes every send; a dead CLI can be retried.
    let saved_output = command(fixture.root.path())
        .args(["create", "SavedFirst"])
        .env("TKFS_FAULT", "onboarding_create_saved")
        .output()
        .unwrap();
    assert_eq!(saved_output.status.code(), Some(86));
    let saved = cli(fixture.root.path(), &["create", "SavedFirst"]);
    assert_eq!(
        cli(fixture.root.path(), &["create", "SavedFirst"])["state_id"],
        saved["state_id"]
    );
    // Persisted server intent and a lost response replay the saved operation,
    // even when the supervisor died after installing the state or spawning it.
    for (number, fault) in [
        "orchestrator_intent_committed",
        "orchestrator_store_installed",
        "orchestrator_worker_spawned",
    ]
    .into_iter()
    .enumerate()
    {
        fixture.crash_supervisor();
        fixture.start(Some(fault));
        let label = format!("Crash{number}");
        assert!(
            !output(fixture.root.path(), &["create", &label])
                .status
                .success()
        );
        let mut child = fixture.supervisor.take().unwrap();
        assert_eq!(child.wait().unwrap().code(), Some(86));
        fixture.start(None);
        let first = cli(fixture.root.path(), &["create", &label]);
        let repeat = cli(fixture.root.path(), &["create", &label]);
        assert_eq!(first["state_id"], repeat["state_id"]);
    }
    assert_eq!(
        api(&fixture.config, Action::List, None, None)["result"]["states"]
            .as_array()
            .unwrap()
            .len(),
        8
    );
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires /dev/fuse and fusermount3; owned fixtures only"]
fn long_native_data_paths_use_short_private_ipc_endpoints() {
    let parent = tempfile::tempdir().unwrap();
    let nested = parent.path().join("nested-project-".repeat(10));
    fs::create_dir(&nested).unwrap();
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&nested, fs::Permissions::from_mode(0o700)).unwrap();
    let root = tempfile::tempdir_in(&nested).unwrap();
    let fixture = Fixture::in_root(root);
    let state = cli(fixture.root.path(), &["create", "Deep"]);
    let runtime = fixture
        .config
        .data_directory
        .join("states")
        .join(state["state_id"].as_str().unwrap())
        .join("runtime.json");
    let info: Value = serde_json::from_slice(&fs::read(&runtime).unwrap()).unwrap();
    assert!(
        info["address"]
            .as_str()
            .unwrap()
            .starts_with("unix:/tmp/tkfs-ipc-")
    );
    cli(
        fixture.root.path(),
        &["--runtime", runtime.to_str().unwrap(), "status"],
    );
    fs::write(
        Path::new(state["mount"].as_str().unwrap()).join("works.txt"),
        b"long paths",
    )
    .unwrap();
    // Explicit relative config aliases identify the same effective data root.
    cli(
        fixture.root.path(),
        &["create", "Deep", "-f", "Projects/../orchestrator.toml"],
    );
}

#[test]
#[ignore = "requires installed WinFsp or /dev/fuse and fusermount3; owned fixtures only"]
fn corrected_mount_obstruction_gets_a_fresh_create_after_terminal_rejection() {
    let fixture = Fixture::new();
    let obstruction = fixture.root.path().join("Projects/Corrected");
    fs::write(&obstruction, b"owned obstruction").unwrap();
    let rejected = output(fixture.root.path(), &["create", "Corrected"]);
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("MOUNT_PATH_OCCUPIED"));
    assert!(
        api(&fixture.config, Action::List, None, None)["result"]["states"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    fs::remove_file(obstruction).unwrap();
    let first = cli(fixture.root.path(), &["create", "Corrected"]);
    assert_eq!(
        cli(fixture.root.path(), &["create", "Corrected"])["state_id"],
        first["state_id"]
    );
    assert_eq!(
        api(&fixture.config, Action::List, None, None)["result"]["states"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires /dev/fuse and fusermount3; owned fixtures only"]
fn pending_stop_after_worker_crash_cleans_only_its_owned_disconnected_mount() {
    use std::os::fd::{AsRawFd, FromRawFd};
    let fixture = Fixture::new();
    let state = cli(fixture.root.path(), &["create", "StoppedCrash"]);
    let state_id = state["state_id"].as_str().unwrap();
    let mount = Path::new(state["mount"].as_str().unwrap());
    fs::write(mount.join("held.txt"), b"owned held bytes").unwrap();
    let held = File::open(mount.join("held.txt")).unwrap();
    let observed = fixture.state(state_id);
    let generation = observed["management_generation"].as_u64();
    let operation = id();
    let action = Action::Stop {
        state: state_id.into(),
    };
    assert_eq!(
        api(
            &fixture.config,
            action.clone(),
            Some(operation.clone()),
            generation
        )["retryable"],
        true
    );
    let pid = observed["observation"]["pid"].as_i64().unwrap();
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) } as i32;
    assert!(fd >= 0);
    let process = unsafe { File::from_raw_fd(fd) };
    assert_eq!(
        unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                process.as_raw_fd(),
                libc::SIGKILL,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        },
        0
    );
    let mut event = libc::pollfd {
        fd: process.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    assert_eq!(unsafe { libc::poll(&mut event, 1, 5000) }, 1);
    let pending = api(
        &fixture.config,
        action.clone(),
        Some(operation.clone()),
        generation,
    );
    drop(held);
    let complete = api(&fixture.config, action, Some(operation), generation);
    // If the regression returns a false success, clean only this test's exact
    // disconnected mount before failing, so its fixture never leaks a mount.
    let leftover = fs::metadata(mount).is_err_and(|e| e.raw_os_error() == Some(libc::ENOTCONN));
    if leftover {
        assert!(
            Command::new("fusermount3")
                .args(["-u", "--"])
                .arg(mount)
                .status()
                .unwrap()
                .success()
        );
        fs::remove_dir(mount).unwrap();
    }
    assert_eq!(
        pending["ok"], false,
        "stop completed while its disconnected mount was still busy"
    );
    assert_eq!(pending["retryable"], true);
    assert_eq!(complete["ok"], true);
    assert!(!leftover, "successful stop left a disconnected mount");
    assert!(!mount.exists());
}
