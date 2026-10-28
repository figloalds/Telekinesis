//! Real worker process lifecycle: runtime files, the Ready line, the loopback
//! control session and quiet exits. Headless only: never passes `--mount`.
//! Every wait is bounded; a child that outlives its bound is killed and the
//! test fails.
use serde_json::{Value, json};
use std::{
    fs,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};
use tkfs::orchestrator::{
    control,
    worker::{Ready, WorkerRecord},
};

const BOUND: Duration = Duration::from_secs(30);
const RUNTIME_FILES: [&str; 3] = ["runtime.json", "runtime.token", "worker.json"];

/// Kills a still-running child on drop, so a failed assertion never strands
/// it. A child still alive 10 s after the kill is reported as stranded, and
/// fails the test unless it is already failing.
struct Guard(Child, PathBuf);
impl Guard {
    fn spawn(state: &Path, command: &mut Command) -> Self {
        Guard(command.spawn().unwrap(), stderr_log(state))
    }
    /// The child's stderr so far, for failure messages.
    fn stderr(&self) -> String {
        fs::read_to_string(&self.1).unwrap_or_default()
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = self.0.kill();
            let end = Instant::now() + Duration::from_secs(10);
            while matches!(self.0.try_wait(), Ok(None)) && Instant::now() < end {
                std::thread::sleep(Duration::from_millis(20));
            }
            if matches!(self.0.try_wait(), Ok(None)) {
                let message = format!("STRANDED_PROCESS: pid {}", self.0.id());
                eprintln!("{message}");
                if !std::thread::panicking() {
                    panic!("{message}");
                }
            }
        }
    }
}

fn stderr_log(state: &Path) -> PathBuf {
    let mut log = state.as_os_str().to_owned();
    log.push(".stderr.log");
    log.into()
}

/// The child's stderr goes to `<state>.stderr.log` (inside the test's temp
/// directory): out of the test console, quoted in failure messages.
fn tkfs(state: &Path) -> Command {
    let stderr = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(stderr_log(state))
        .unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_tkfs"));
    command
        .arg("--state")
        .arg(state)
        .stdin(Stdio::null())
        .stderr(stderr)
        .env_remove("TKFS_FAKE_NOW_MS")
        .env_remove("TKFS_UNSUPERVISED_EXIT_S");
    command
}

/// Waits at most `bound` for the child to exit; kills it and fails otherwise.
fn wait_status(guard: &mut Guard, bound: Duration) -> ExitStatus {
    let end = Instant::now() + bound;
    loop {
        if let Some(status) = guard.0.try_wait().unwrap() {
            return status;
        }
        if Instant::now() >= end {
            let _ = guard.0.kill();
            panic!(
                "process {} did not exit within {bound:?}; stderr:
{}",
                guard.0.id(),
                guard.stderr()
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn wait_exit(guard: &mut Guard, bound: Duration) {
    let status = wait_status(guard, bound);
    assert_eq!(
        status.code(),
        Some(0),
        "worker exit status {status}; stderr:
{}",
        guard.stderr()
    );
}

/// Reads stdout lines on a thread so line waits can be bounded.
fn lines(child: &mut Child) -> mpsc::Receiver<String> {
    let stdout = child.stdout.take().expect("piped stdout");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    rx
}

fn init(state: &Path) {
    let mut guard = Guard::spawn(state, tkfs(state).arg("init").stdout(Stdio::null()));
    let status = wait_status(&mut guard, BOUND);
    assert!(
        status.success(),
        "init failed: {status}; stderr:
{}",
        guard.stderr()
    );
}

/// Runs `init`, then `daemon [--supervised]`, and reads stdout until the
/// Ready line (≤ 30 s).
fn spawn_worker(state: &Path, supervised: bool, env: &[(&str, &str)]) -> (Guard, Ready) {
    init(state);
    let mut command = tkfs(state);
    command.arg("daemon");
    if supervised {
        command.arg("--supervised");
    }
    for (key, value) in env {
        command.env(key, value);
    }
    let mut guard = Guard::spawn(state, command.stdout(Stdio::piped()));
    let rx = lines(&mut guard.0);
    let end = Instant::now() + BOUND;
    loop {
        let left = end.saturating_duration_since(Instant::now());
        match rx.recv_timeout(left) {
            Ok(line) => {
                if let Ok(value) = serde_json::from_str::<Value>(&line)
                    && value["ready"] == true
                {
                    return (guard, serde_json::from_value(value).unwrap());
                }
            }
            Err(error) => {
                let _ = guard.0.kill();
                panic!(
                    "no Ready line within {BOUND:?}: {error}; stderr:
{}",
                    guard.stderr()
                );
            }
        }
    }
}

fn json_file<T: serde::de::DeserializeOwned>(path: &Path) -> T {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

#[test]
fn worker_writes_runtime_files_and_ready_line_then_removes_them_on_shutdown() {
    let home = tempfile::tempdir().unwrap();
    let state = home.path().join("s");
    let (mut child, ready) = spawn_worker(&state, true, &[]);
    assert!(ready.ready);
    assert_eq!(ready.ipc, 4);
    assert!(
        ready.address.starts_with("ws://127.0.0.1:"),
        "{}",
        ready.address
    );
    assert_eq!(ready.pid, child.0.id());
    assert!(ready.mounts.is_empty());
    assert_eq!(ready.token.len(), 64);

    let runtime: Value = json_file(&state.join("runtime.json"));
    assert_eq!(runtime["address"], ready.address);
    assert_eq!(runtime["pid"], ready.pid);
    assert_eq!(runtime["instance"], ready.instance);
    assert_eq!(runtime["ipc"], 4);
    assert_eq!(runtime["mounts"], json!([]));
    assert!(
        !runtime.to_string().contains(&ready.token),
        "runtime.json must not carry the token"
    );

    let record: WorkerRecord = json_file(&state.join("worker.json"));
    assert_eq!(record.format, 1);
    assert_eq!(record.pid, ready.pid);
    assert_eq!(record.instance, ready.instance);
    assert_eq!(record.address, ready.address);
    assert_eq!(record.version, ready.version);
    assert_eq!(record.ipc, 4);
    assert!(record.start_ms >= tkfs::clock::FLOOR_MS);

    assert_eq!(
        fs::read_to_string(state.join("runtime.token")).unwrap(),
        ready.token
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(state.join("runtime.token"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    assert!(!state.join("control.json").exists());

    let hello = control::request(&state, "hello", json!({})).unwrap();
    assert_eq!(hello["repo"], ready.repo);
    assert_eq!(hello["device"], ready.device);
    assert_eq!(hello["instance"], ready.instance);
    assert_eq!(hello["pid"], ready.pid);
    assert_eq!(hello["version"], ready.version);
    assert_eq!(hello["ipc"], 4);
    assert_eq!(hello["mounts"], json!([]));

    // Filesystem commands keep working through the loopback session.
    assert_eq!(
        control::request(&state, "status", json!({})).unwrap()["pid"],
        ready.pid
    );
    control::request(&state, "mkdir", json!({"path":"d"})).unwrap();
    let error = control::request(&state, "no-such-op", json!({})).unwrap_err();
    assert!(
        error.to_string().starts_with("UNSUPPORTED_OPERATION"),
        "{error:#}"
    );

    control::request(&state, "shutdown", json!({"force": false})).unwrap();
    wait_exit(&mut child, BOUND);
    for f in RUNTIME_FILES {
        assert!(!state.join(f).exists(), "{f} left behind");
    }
    let error = control::request(&state, "hello", json!({})).unwrap_err();
    assert!(
        error.to_string().starts_with("WORKER_NOT_RUNNING"),
        "{error:#}"
    );
}

#[test]
fn worker_serves_feed_ops_on_its_control_lane_and_no_pairing_ops() {
    let home = tempfile::tempdir().unwrap();
    let state = home.path().join("s");
    let (mut child, _) = spawn_worker(&state, true, &[]);
    control::request(&state, "mkdir", json!({"path":"d"})).unwrap();
    let page = control::request(&state, "feed.outgoing", json!({"after":0})).unwrap();
    let events = page["events"].as_array().unwrap();
    assert!(!events.is_empty(), "{page}");
    assert_eq!(page["cursor"], events.len());
    assert_eq!(page["more"], false);
    let empty = control::request(&state, "feed.outgoing", json!({"after":page["cursor"]})).unwrap();
    assert_eq!(empty["events"], json!([]));
    assert_eq!(empty["cursor"], page["cursor"]);
    // Object ops never travel on a control lane (bulk loopback lanes come
    // with the orchestrator's worker link).
    let error = control::request(&state, "objects.missing", json!({"limit":1})).unwrap_err();
    assert!(error.to_string().contains("OP_NOT_ALLOWED"), "{error:#}");
    // Pairing is not a worker concern any more.
    for op in ["pairing-code", "pair", "peers", "unpair", "peer-info"] {
        let error = control::request(&state, op, json!({})).unwrap_err();
        assert!(
            error.to_string().starts_with("UNSUPPORTED_OPERATION"),
            "{op}: {error:#}"
        );
    }
    let snapshot = control::request(&state, "desktop-snapshot", json!({})).unwrap();
    assert_eq!(snapshot["peers"], json!([]));
    control::request(&state, "shutdown", json!({"force": false})).unwrap();
    wait_exit(&mut child, BOUND);
}

#[test]
fn a_wrong_token_is_refused() {
    let home = tempfile::tempdir().unwrap();
    let state = home.path().join("s");
    let (mut child, ready) = spawn_worker(&state, true, &[]);
    let error = tkfs::session::dial_loopback(
        &ready.address,
        Some(&"0".repeat(64)),
        tkfs::session::ConnOptions::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("ACCESS_DENIED"), "{error}");
    control::request(&state, "shutdown", json!({"force": true})).unwrap();
    wait_exit(&mut child, BOUND);
}

#[test]
fn watch_returns_immediately_when_seq_advanced_and_waits_otherwise() {
    let home = tempfile::tempdir().unwrap();
    let state = home.path().join("s");
    let (mut child, _ready) = spawn_worker(&state, true, &[]);

    let started = Instant::now();
    let first = control::request(&state, "watch", json!({"after_seq":0,"wait_ms":2000})).unwrap();
    assert!(started.elapsed() <= Duration::from_millis(1500));
    let seq = first["seq"].as_u64().unwrap();
    assert!(seq >= 1, "{first}");
    let heartbeat = &first["heartbeat"];
    assert!(heartbeat["at_ms"].as_u64().unwrap() >= tkfs::clock::FLOOR_MS);
    assert!(heartbeat["entries"].is_u64(), "{first}");
    assert_eq!(heartbeat["conflicts"], 0);
    assert_eq!(heartbeat["writable_handles"], 0);
    assert!(heartbeat["error"].is_null());
    assert_eq!(first["demand"], json!([]));

    // Park until the next heartbeat bump, so the following short watch starts
    // right after one and the next bump is ~1 s away.
    let started = Instant::now();
    let synced =
        control::request(&state, "watch", json!({"after_seq":seq,"wait_ms":2000})).unwrap();
    assert!(started.elapsed() <= Duration::from_millis(1500));
    let seq = synced["seq"].as_u64().unwrap();
    assert!(seq > first["seq"].as_u64().unwrap(), "{synced}");

    let started = Instant::now();
    let quiet = control::request(&state, "watch", json!({"after_seq":seq,"wait_ms":300})).unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(250) && elapsed <= Duration::from_millis(1500),
        "{elapsed:?}"
    );
    assert_eq!(quiet["seq"].as_u64(), Some(seq), "{quiet}");

    control::request(&state, "shutdown", json!({"force": false})).unwrap();
    wait_exit(&mut child, BOUND);
}

#[test]
fn unsupervised_worker_exits_quietly_after_no_contact() {
    let home = tempfile::tempdir().unwrap();
    let state = home.path().join("s");
    let (mut child, _ready) = spawn_worker(&state, true, &[("TKFS_UNSUPERVISED_EXIT_S", "2")]);
    let ready_at = Instant::now();
    wait_exit(&mut child, Duration::from_secs(10));
    assert!(
        ready_at.elapsed() >= Duration::from_millis(1500),
        "exited before the quiet period: {:?}",
        ready_at.elapsed()
    );
    for f in RUNTIME_FILES {
        assert!(!state.join(f).exists(), "{f} left behind");
    }
}

#[test]
#[cfg_attr(
    not(debug_assertions),
    ignore = "TKFS_FAKE_NOW_MS is only honoured by debug builds"
)]
fn worker_refuses_to_start_when_the_clock_is_invalid() {
    let home = tempfile::tempdir().unwrap();
    let state = home.path().join("s");
    let mut child = Guard::spawn(
        &state,
        tkfs(&state)
            .arg("daemon")
            .arg("--supervised")
            .env("TKFS_FAKE_NOW_MS", "1000000000000")
            .stdout(Stdio::piped()),
    );
    let rx = lines(&mut child.0);
    let status = wait_status(&mut child, BOUND);
    assert_eq!(status.code(), Some(78), "{status}");
    // The reader thread ends at stdout EOF; bound the drain anyway.
    let mut output = Vec::new();
    let end = Instant::now() + Duration::from_secs(5);
    while let Ok(line) = rx.recv_timeout(end.saturating_duration_since(Instant::now())) {
        output.push(line);
    }
    let fatal = output
        .iter()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|value| value["fatal"] == true)
        .unwrap_or_else(|| panic!("no fatal line in {output:?}"));
    assert_eq!(fatal["code"], "CLOCK_INVALID");
    assert_eq!(fatal["exit"], 78);
    assert!(
        fatal["message"]
            .as_str()
            .unwrap()
            .starts_with("CLOCK_INVALID"),
        "{fatal}"
    );
    for f in RUNTIME_FILES {
        assert!(!state.join(f).exists(), "{f} written by a refused start");
    }
}
