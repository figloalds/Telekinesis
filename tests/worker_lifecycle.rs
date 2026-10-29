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
    sync::mpsc::{self, RecvTimeoutError},
    time::{Duration, Instant},
};
use tkfs::{
    content::Kind,
    model::{content::Digest, protocol::LaneRole},
    orchestrator::{
        control,
        process::{self, LaunchSpec, ObservedProcess, ProcessEvent},
        sync::{OBJECT_BATCH, Object},
        worker::{Ready, WorkerRecord},
        worker_client::{CALL_DEADLINE, WorkerLink},
    },
    runtime::Engine,
    session::{self, CallError, Conn, ConnOptions, Priority},
    util::lock::lock,
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
    init_repo(state, None);
}

/// `init`, with `--repo <repo>` when given.
fn init_repo(state: &Path, repo: Option<&str>) {
    let mut command = tkfs(state);
    command.arg("init");
    if let Some(repo) = repo {
        command.arg("--repo").arg(repo);
    }
    let mut guard = Guard::spawn(state, command.stdout(Stdio::null()));
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
    start_worker(state, supervised, env)
}

/// `daemon [--supervised]` on an initialised store, up to its Ready line
/// (≤ 30 s).
fn start_worker(state: &Path, supervised: bool, env: &[(&str, &str)]) -> (Guard, Ready) {
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
    assert_eq!(heartbeat["mounts"], json!([]), "headless: {first}");
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

// ---------------------------------------------------------------------------
// The launcher (`orchestrator::process`): spawn, adoption, verified kill.

/// A `WorkerProcess` never kills on drop (workers outlive the daemon), so a
/// launcher-spawned worker gets this instead: on drop it kills the process if
/// it is still the same one (`kill_verified`) and waits ≤ 10 s for it to go,
/// reporting a survivor as `STRANDED_PROCESS`.
struct Reaper {
    record: WorkerRecord,
    observed: Option<ObservedProcess>,
}
impl Reaper {
    fn new(pid: u32) -> Self {
        let observed = ObservedProcess::open(pid).ok();
        let (start_ms, image) = process::process_identity(pid).unwrap_or_default();
        let record = WorkerRecord {
            format: 1,
            pid,
            start_ms,
            image,
            version: String::new(),
            instance: String::new(),
            address: String::new(),
            ipc: 4,
        };
        Reaper { record, observed }
    }
}
impl Drop for Reaper {
    fn drop(&mut self) {
        let Some(observed) = &self.observed else {
            return;
        };
        if matches!(observed.exited(), Ok(true)) {
            return;
        }
        let killed = process::kill_verified(&self.record);
        if !matches!(observed.wait(Duration::from_secs(10)), Ok(true)) {
            let message = format!("STRANDED_PROCESS: pid {} ({killed:?})", observed.pid);
            eprintln!("{message}");
            if !std::thread::panicking() {
                panic!("{message}");
            }
        }
    }
}

/// Two readings of one process's start time: equal, or on Linux within the
/// tolerance for a wall-clock step between them.
fn assert_same_start(a: u64, b: u64) {
    #[cfg(target_os = "linux")]
    assert!(a.abs_diff(b) <= process::START_TOLERANCE_MS, "{a} vs {b}");
    #[cfg(not(target_os = "linux"))]
    assert_eq!(a, b);
}

fn spec(state: &Path) -> LaunchSpec {
    LaunchSpec {
        executable: env!("CARGO_BIN_EXE_tkfs").into(),
        state: state.to_path_buf(),
        mount: None,
    }
}

/// The next launcher event within `bound`; fails with `worker.log` otherwise.
fn next_event(rx: &mpsc::Receiver<ProcessEvent>, state: &Path, bound: Duration) -> ProcessEvent {
    rx.recv_timeout(bound).unwrap_or_else(|error| {
        panic!(
            "no process event within {bound:?}: {error}; worker.log:\n{}",
            fs::read_to_string(state.join("worker.log")).unwrap_or_default()
        )
    })
}

fn expect_ready(rx: &mpsc::Receiver<ProcessEvent>, state: &Path) -> Ready {
    match next_event(rx, state, BOUND) {
        ProcessEvent::Ready(ready) => ready,
        other => panic!("expected Ready, got {other:?}"),
    }
}

fn expect_exited(
    rx: &mpsc::Receiver<ProcessEvent>,
    state: &Path,
    bound: Duration,
) -> (Option<i32>, String) {
    match next_event(rx, state, bound) {
        ProcessEvent::Exited { code, tail } => (code, tail),
        other => panic!("expected Exited, got {other:?}"),
    }
}

/// `Exited` was the last event: every sender of the process is gone.
fn expect_closed(rx: &mpsc::Receiver<ProcessEvent>) {
    match rx.recv_timeout(Duration::from_secs(5)) {
        Err(RecvTimeoutError::Disconnected) => {}
        other => panic!("an event after Exited, or the sink is still held: {other:?}"),
    }
}

#[test]
fn spawn_reports_ready_then_exited_and_logs_redacted_ready_line() {
    let home = tempfile::tempdir().unwrap();
    let state = home.path().join("s");
    init(&state);
    let (tx, rx) = mpsc::channel();
    let worker = process::spawn(&spec(&state), tx).unwrap();
    let _reaper = Reaper::new(worker.pid);
    let ready = expect_ready(&rx, &state);
    assert_eq!(ready.pid, worker.pid);
    assert_eq!(ready.token.len(), 64);

    let log = fs::read_to_string(state.join("worker.log")).unwrap();
    assert!(log.contains("\"ready\":true"), "{log}");
    assert!(!log.contains(&ready.token), "worker.log carries the token");

    // The record names this very process: its OS start time and image.
    let record: WorkerRecord = json_file(&state.join("worker.json"));
    let (start_ms, image) = process::process_identity(worker.pid).unwrap();
    assert_eq!(record.pid, worker.pid);
    assert_same_start(record.start_ms, start_ms);
    assert_eq!(record.image, image);

    control::request(&state, "shutdown", json!({"force": false})).unwrap();
    let (code, tail) = expect_exited(&rx, &state, BOUND);
    assert_eq!(code, Some(0), "tail:\n{tail}");
    assert!(!tail.is_empty() && tail.len() <= process::TAIL_BYTES);
    assert!(!tail.contains(&ready.token));
    expect_closed(&rx);
}

#[test]
fn spawn_reports_a_fatal_line_then_exited() {
    let home = tempfile::tempdir().unwrap();
    let state = home.path().join("s");
    init(&state);
    let (tx, rx) = mpsc::channel();
    let first = process::spawn(&spec(&state), tx).unwrap();
    let _first_reaper = Reaper::new(first.pid);
    let ready = expect_ready(&rx, &state);

    // A second worker on the same store is refused: a fatal line, exit 75.
    let (tx, second_rx) = mpsc::channel();
    let second = process::spawn(&spec(&state), tx).unwrap();
    let _second_reaper = Reaper::new(second.pid);
    let mut fatal = None;
    let code = loop {
        match next_event(&second_rx, &state, BOUND) {
            ProcessEvent::Fatal { code, message } => fatal = Some((code, message)),
            ProcessEvent::Exited { code, .. } => break code,
            ProcessEvent::Ready(ready) => panic!("a second worker got Ready: {ready:?}"),
        }
    };
    let (fatal_code, message) = fatal.expect("a Fatal event before Exited");
    assert_eq!(fatal_code, "STORE_ALREADY_OPEN", "{message}");
    assert_eq!(code, Some(75));
    expect_closed(&second_rx);

    assert_eq!(
        control::request(&state, "hello", json!({})).unwrap()["pid"],
        ready.pid
    );
    control::request(&state, "shutdown", json!({"force": false})).unwrap();
    assert_eq!(expect_exited(&rx, &state, BOUND).0, Some(0));
}

#[test]
fn adopt_matches_identity_and_rejects_a_stale_record() {
    let home = tempfile::tempdir().unwrap();
    let state = home.path().join("s");
    init(&state);
    let (tx, rx) = mpsc::channel();
    let worker = process::spawn(&spec(&state), tx).unwrap();
    let _reaper = Reaper::new(worker.pid);
    let ready = expect_ready(&rx, &state);
    // Dropping the handle never kills: the worker keeps answering.
    drop(worker);
    assert_eq!(
        control::request(&state, "hello", json!({})).unwrap()["pid"],
        ready.pid
    );

    let (adopt_tx, adopt_rx) = mpsc::channel();
    let (record, adopted) = process::adopt(&state, adopt_tx)
        .unwrap()
        .expect("the live worker is adopted");
    assert_eq!(record.pid, ready.pid);
    assert_eq!(adopted.pid, ready.pid);
    assert_eq!(record.instance, ready.instance);

    // A record whose start time differs names another process. Linux allows
    // ±2 s (tick rounding), so the stale record is 5 s off there.
    let mut stale = record.clone();
    stale.start_ms += if cfg!(target_os = "linux") { 5000 } else { 1 };
    fs::write(
        state.join("worker.json"),
        serde_json::to_vec(&stale).unwrap(),
    )
    .unwrap();
    let (stale_tx, _stale_rx) = mpsc::channel();
    assert!(process::adopt(&state, stale_tx).unwrap().is_none());
    assert!(!process::kill_verified(&stale).unwrap());
    assert_eq!(
        control::request(&state, "hello", json!({})).unwrap()["pid"],
        ready.pid,
        "kill_verified of a stale record must not kill"
    );

    // The real identity is killed; the spawn's waiter and the adopter both
    // observe the exit.
    fs::write(
        state.join("worker.json"),
        serde_json::to_vec(&record).unwrap(),
    )
    .unwrap();
    assert!(process::kill_verified(&record).unwrap());
    let kill_bound = Duration::from_secs(10);
    let (code, _) = expect_exited(&adopt_rx, &state, kill_bound);
    assert_ne!(code, Some(0));
    let (code, _) = expect_exited(&rx, &state, kill_bound);
    assert_ne!(code, Some(0));
    expect_closed(&rx);
    drop(adopted);
    expect_closed(&adopt_rx);

    // The crash left the runtime files behind; they name a dead process.
    assert!(state.join("worker.json").exists());
    let (gone_tx, _gone_rx) = mpsc::channel();
    assert!(process::adopt(&state, gone_tx).unwrap().is_none());
    assert!(!process::kill_verified(&record).unwrap());

    // The next spawn still starts.
    let (tx, rx) = mpsc::channel();
    let worker = process::spawn(&spec(&state), tx).unwrap();
    let _next_reaper = Reaper::new(worker.pid);
    let next = expect_ready(&rx, &state);
    assert_ne!(next.instance, ready.instance);
    control::request(&state, "shutdown", json!({"force": false})).unwrap();
    let (code, tail) = expect_exited(&rx, &state, BOUND);
    assert_eq!(code, Some(0), "tail:\n{tail}");
}

#[test]
fn process_identity_of_self_is_stable() {
    let a = process::process_identity(std::process::id()).unwrap();
    let b = process::process_identity(std::process::id()).unwrap();
    assert_same_start(a.0, b.0);
    assert_eq!(a.1, b.1);
    let exe = std::env::current_exe().unwrap();
    assert!(
        a.1.ends_with(exe.file_name().unwrap()),
        "{} vs {}",
        a.1.display(),
        exe.display()
    );
    assert!(a.0 >= tkfs::clock::FLOOR_MS, "{}", a.0);
    assert!(a.0 <= tkfs::util::time::now_ms() + 2000, "{}", a.0);
}

// ---------------------------------------------------------------------------
// The orchestrator's worker link: one control lane and N bulk lanes.

/// 1 MiB of objects: 16 distinct full 64 KiB chunks (the largest a chunk may
/// be), whose bytes vary so their JSON encoding is not trivially short.
fn one_mib_of_chunks() -> Vec<Object> {
    (0..16u32)
        .map(|n| {
            let bytes: Vec<u8> = (0..64 * 1024u32)
                .map(|i| ((i * 31 + n * 7) % 251) as u8)
                .collect();
            Object {
                digest: Digest::bytes(&bytes),
                kind: Kind::Chunk,
                bytes,
            }
        })
        .collect()
}

fn remote_code<T>(result: Result<T, CallError>) -> String {
    match result {
        Err(CallError::Remote(wire)) => wire.code,
        Err(other) => panic!("expected a coded remote error, got {other:?}"),
        Ok(_) => panic!("expected a coded remote error, the call succeeded"),
    }
}

/// Waits ≤ 5 s for every lane of `link` to report closed.
fn wait_link_closed(link: &WorkerLink) {
    let end = Instant::now() + Duration::from_secs(5);
    while link.alive() {
        assert!(Instant::now() < end, "lanes still open 5 s after close()");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn worker_link_uses_control_for_rpc_and_bulk_for_objects() {
    let home = tempfile::tempdir().unwrap();
    let state = home.path().join("s");
    let (mut child, ready) = spawn_worker(&state, true, &[]);
    let link = WorkerLink::connect(&ready, 2).unwrap();
    assert!(link.alive());
    let roles: Vec<LaneRole> = link.health().iter().map(|(role, _)| *role).collect();
    assert_eq!(roles, [LaneRole::Control, LaneRole::Bulk, LaneRole::Bulk]);

    let hello = link.call_json("hello", json!({})).unwrap();
    assert_eq!(hello["repo"], ready.repo);
    assert_eq!(hello["instance"], ready.instance);

    // A batch's object bytes travel on the bulk lane of its first digest;
    // the control lane carries none of them.
    let objects = one_mib_of_chunks();
    let object = objects[0].clone();
    let lane = link.lane_for(&object.digest);
    assert_eq!(lane, object.digest.0[0] as usize % 2);
    let control_before = link.health()[0].1.bytes_up;
    let bulk_before = link.health()[1 + lane].1.bytes_up;
    assert_eq!(link.write(objects.clone()).unwrap(), objects.len());
    let health = link.health();
    let control_sent = health[0].1.bytes_up - control_before;
    assert!(
        control_sent < 4096,
        "control lane sent {control_sent} bytes"
    );
    let bulk_sent = health[1 + lane].1.bytes_up - bulk_before;
    assert!(bulk_sent >= 1024 * 1024, "bulk lane sent {bulk_sent} bytes");

    let wanted: Vec<(Digest, Kind)> = objects.iter().map(|o| (o.digest, o.kind)).collect();
    let fetched = link.fetch(&wanted).unwrap();
    assert_eq!(fetched.len(), objects.len());
    for (fetched, object) in fetched.iter().zip(&objects) {
        assert_eq!(fetched.digest, object.digest);
        assert_eq!(fetched.bytes, object.bytes);
    }

    // Empty batches make no call; oversized ones are refused locally.
    assert!(link.fetch(&[]).unwrap().is_empty());
    assert_eq!(link.write(Vec::new()).unwrap(), 0);
    let too_many = vec![(object.digest, Kind::Chunk); OBJECT_BATCH + 1];
    assert_eq!(remote_code(link.fetch(&too_many)), "OBJECT_BATCH_TOO_LARGE");
    let small = Object {
        digest: Digest::bytes(b"x"),
        kind: Kind::Chunk,
        bytes: b"x".to_vec(),
    };
    assert_eq!(
        remote_code(link.write(vec![small; OBJECT_BATCH + 1])),
        "OBJECT_BATCH_TOO_LARGE"
    );

    // Object ops are refused on the control lane before anything is sent.
    let before = link.health()[0].1.bytes_up;
    let body = serde_json::to_vec(&vec![(object.digest, Kind::Chunk)]).unwrap();
    assert_eq!(
        remote_code(link.call(
            "objects.fetch",
            json!({"repo": ready.repo}),
            body,
            CALL_DEADLINE
        )),
        "OP_NOT_ALLOWED"
    );
    assert_eq!(
        link.health()[0].1.bytes_up,
        before,
        "the refusal sent bytes"
    );

    link.close();
    wait_link_closed(&link);
    control::request(&state, "shutdown", json!({"force": false})).unwrap();
    wait_exit(&mut child, BOUND);
}

#[test]
fn loopback_lane_header_selects_the_served_op_policy() {
    let home = tempfile::tempdir().unwrap();
    let state = home.path().join("s");
    let (mut child, ready) = spawn_worker(&state, true, &[]);
    let dial = |role| {
        session::dial_loopback_as(
            &ready.address,
            Some(&ready.token),
            role,
            ConnOptions::for_role(role),
        )
        .unwrap()
    };
    let fetch_body = serde_json::to_vec(&Vec::<(Digest, Kind)>::new()).unwrap();
    let call = |conn: &Conn, op: &str, body: Vec<u8>| {
        conn.call(
            op,
            json!({"repo": ready.repo}),
            body,
            Priority::Control,
            CALL_DEADLINE,
        )
    };

    let bulk = dial(LaneRole::Bulk);
    let fetched = call(&bulk, "objects.fetch", fetch_body.clone()).unwrap();
    assert_eq!(fetched.body, b"[]");
    assert_eq!(
        remote_code(call(&bulk, "hello", Vec::new())),
        "OP_NOT_ALLOWED"
    );
    bulk.close();

    let control_lane = dial(LaneRole::Control);
    assert_eq!(
        remote_code(call(&control_lane, "objects.fetch", fetch_body)),
        "OP_NOT_ALLOWED"
    );
    assert!(call(&control_lane, "hello", Vec::new()).is_ok());
    control_lane.close();

    control::request(&state, "shutdown", json!({"force": false})).unwrap();
    wait_exit(&mut child, BOUND);
}

#[test]
fn watch_long_poll_returns_demand_promptly() {
    let home = tempfile::tempdir().unwrap();
    let state = home.path().join("s");
    init_repo(&state, Some("linked"));

    // An entry with content, written by another replica of the same
    // repository: only its event reaches the worker, not its chunk.
    let source_home = tempfile::tempdir().unwrap();
    let source = Engine::open(source_home.path(), Some("linked")).unwrap();
    let content = b"bytes the worker does not have yet".to_vec();
    let h = source.open_file("f", true, false, true).unwrap();
    source.write(h, 0, &content, false, false).unwrap();
    source.flush(Some(h)).unwrap();
    source.close(h).unwrap();
    let events: Vec<_> = {
        let mut store = lock(&source.store);
        store.sync_metadata().unwrap();
        store
            .events_after(0, 4096)
            .unwrap()
            .into_iter()
            .map(|(_, event)| event)
            .collect()
    };
    let digest = source.lookup("f").unwrap().content.unwrap().digest;
    source.shutdown().unwrap();

    let (mut child, ready) = start_worker(&state, true, &[]);
    assert_eq!(ready.repo, "linked");
    let link = WorkerLink::connect(&ready, 2).unwrap();
    let applied = link
        .call(
            "feed.apply",
            json!({}),
            serde_json::to_vec(&events).unwrap(),
            CALL_DEADLINE,
        )
        .unwrap();
    let applied: Value = serde_json::from_slice(&applied.body).unwrap();
    assert!(applied["applied"].as_u64().unwrap() >= 1, "{applied}");

    let started = Instant::now();
    let first = link.watch(0).unwrap();
    assert!(started.elapsed() <= Duration::from_secs(2));
    assert!(first.seq >= 1, "{first:?}");
    assert!(first.demand.is_empty(), "{first:?}");

    // A reader of the entry blocks on the missing chunk; the parked watch
    // reports it at once.
    let (sent, received) = mpsc::channel();
    let reader_state = state.clone();
    let asked = Instant::now();
    std::thread::spawn(move || {
        let _ = sent.send(control::request(&reader_state, "cat", json!({"path": "f"})));
    });
    let mut seq = first.seq;
    let demanded = loop {
        let watch = link.watch(seq).unwrap();
        if watch.demand.contains(&(digest, Kind::Chunk)) {
            break asked.elapsed();
        }
        assert!(
            asked.elapsed() < Duration::from_secs(5),
            "no demand for {digest} within 5 s: {watch:?}"
        );
        seq = watch.seq;
    };
    assert!(
        demanded <= Duration::from_millis(500),
        "demand reported after {demanded:?}"
    );

    // Answering the demand completes the read.
    let chunk = Object {
        digest,
        kind: Kind::Chunk,
        bytes: content.clone(),
    };
    assert_eq!(link.write(vec![chunk]).unwrap(), 1);
    let read = received
        .recv_timeout(BOUND)
        .expect("cat did not finish within 30 s")
        .unwrap();
    let bytes: Vec<u8> = serde_json::from_value(read["bytes"].clone()).unwrap();
    assert_eq!(bytes, content);
    assert!(link.watch(seq).unwrap().demand.is_empty());

    link.close();
    wait_link_closed(&link);
    control::request(&state, "shutdown", json!({"force": false})).unwrap();
    wait_exit(&mut child, BOUND);
}
