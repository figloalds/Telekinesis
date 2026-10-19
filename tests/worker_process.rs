//! WP1.3: the worker process launcher, adoption by verified identity, and the
//! JSON worker client (REVAMP_PLAN §3.3, §14 WP1.3). Runs on Windows and Linux.
//!
//! Fake children are scripted with `cmd /c` (Windows) and `sh -c` (Unix); the
//! real `daemon --supervised` launch is checked in WP1.INT, where the flag
//! exists.
mod support;

use serde_json::{Value, json};
use std::{
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Barrier, Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};
use tkfs::{
    core::{Store, id},
    model::{
        ipc::{StartupLine, WorkerRecord},
        orchestrator::{
            CallError, LaunchSpec, ProcessEvent, ProcessSink, WorkerClient, WorkerEndpoint,
            WorkerLauncher,
        },
    },
    orchestrator::{
        process::{LogPolicy, RealLauncher, identity_matches, kill_verified, process_identity},
        worker_client::{ClientOptions, JsonWorkerClient, MAX_IDLE},
    },
    runtime::{self, Engine, Shared},
};

const PROGRESS: &str = r#"{"startup_progress":1,"phase":"opening state"}"#;
const READY: &str = r#"{"ready":true,"repo":"r1","device":"d1","runtime":"rt","mounted":false,"instance":"i1","pid":7,"address":"ws://127.0.0.1:9","version":"v1","ipc":3}"#;
const FATAL: &str = r#"{"fatal":true,"code":"STORE_IDENTITY_MISMATCH","class":"fatal","message":"STORE_IDENTITY_MISMATCH: wrong repository","exit_code":73}"#;

// ---------------------------------------------------------------------------
// Helpers

fn sink() -> (ProcessSink, mpsc::Receiver<ProcessEvent>) {
    let (tx, rx) = mpsc::channel();
    let tx = Mutex::new(tx);
    (
        Arc::new(move |event| {
            let _ = tx.lock().unwrap().send(event);
        }),
        rx,
    )
}

/// Every event up to and including `Exited`.
fn until_exit(rx: &mpsc::Receiver<ProcessEvent>, timeout: Duration) -> Vec<ProcessEvent> {
    let deadline = Instant::now() + timeout;
    let mut events = Vec::new();
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let event = rx
            .recv_timeout(left)
            .unwrap_or_else(|_| panic!("no Exited event within {timeout:?}; got {events:#?}"));
        let done = matches!(event, ProcessEvent::Exited { .. });
        events.push(event);
        if done {
            return events;
        }
    }
}

fn exit_of(event: &ProcessEvent) -> (Option<i32>, Option<String>, String) {
    match event {
        ProcessEvent::Exited {
            code,
            fatal,
            stderr_tail,
        } => (
            *code,
            fatal.as_ref().map(|f| f.code.clone()),
            stderr_tail.clone(),
        ),
        other => panic!("expected Exited, got {other:?}"),
    }
}

#[cfg(windows)]
fn shell(script: &str) -> Command {
    use std::os::windows::process::CommandExt;
    let mut command = Command::new("cmd");
    command.raw_arg(format!("/d /c {script}"));
    command
}

#[cfg(unix)]
fn shell(script: &str) -> Command {
    let mut command = Command::new("sh");
    command.arg("-c").arg(script);
    command
}

fn script(parts: &[String]) -> String {
    parts.join(if cfg!(windows) { " & " } else { "; " })
}

fn echo(line: &str) -> String {
    if cfg!(windows) {
        format!("echo {line}")
    } else {
        format!("echo '{line}'")
    }
}

fn echo_err(line: &str) -> String {
    if cfg!(windows) {
        format!("echo {line} 1>&2")
    } else {
        format!("echo '{line}' >&2")
    }
}

fn exit_with(code: i32) -> String {
    if cfg!(windows) {
        format!("exit /b {code}")
    } else {
        format!("exit {code}")
    }
}

/// The absolute path of a long-running, harmless program and its arguments.
fn sleeper_command() -> (PathBuf, Vec<&'static str>) {
    #[cfg(windows)]
    {
        let root = std::env::var_os("SystemRoot").unwrap_or_else(|| "C:\\Windows".into());
        (
            PathBuf::from(root).join("System32").join("PING.EXE"),
            vec!["-n", "120", "127.0.0.1"],
        )
    }
    #[cfg(unix)]
    {
        let path = std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
            .map(|dir| dir.join("sleep"))
            .find(|candidate| candidate.is_file())
            .expect("sleep on PATH");
        (path, vec!["120"])
    }
}

/// A sleeper spawned directly by the test (so the test can reap it).
///
/// Returns once the OS reports the sleeper's own image: on Linux `spawn` can
/// return before `/proc/<pid>/exe` switches to the new image (the vfork parent
/// resumes before the child's address space is replaced), and an identity
/// read in that window is the parent's. A real worker records its identity
/// itself, after exec, so adoption never sees that window.
fn sleeper() -> (Child, PathBuf) {
    let (path, args) = sleeper_command();
    let child = Command::new(&path)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let expected = canonical(&path);
    let deadline = Instant::now() + Duration::from_secs(10);
    while !process_identity(child.id())
        .is_some_and(|(_, image)| fs::canonicalize(image).is_ok_and(|image| image == expected))
    {
        assert!(Instant::now() < deadline, "the sleeper never finished exec");
        thread::sleep(Duration::from_millis(5));
    }
    (child, path)
}

fn canonical(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|e| panic!("canonicalize {}: {e}", path.display()))
}

fn record(pid: u32, process_start: u64, executable: PathBuf) -> WorkerRecord {
    WorkerRecord {
        format: 1,
        pid,
        instance: id(),
        address: "ws://127.0.0.1:1".into(),
        version: "v".into(),
        ipc: 3,
        repo: "r".into(),
        device: "d".into(),
        started_at_ms: 0,
        process_start,
        executable,
        supervised: true,
        token: None,
    }
}

fn wait_exit(child: &mut Child, timeout: Duration) -> std::process::ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        assert!(Instant::now() < deadline, "process did not exit");
        thread::sleep(Duration::from_millis(20));
    }
}

// ---------------------------------------------------------------------------
// Launcher: stdout parser, worker.log, exit events

#[test]
fn stdout_lines_are_parsed_logged_and_followed_by_exit() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("state").join("worker.log");
    fs::create_dir_all(log.parent().unwrap()).unwrap();
    fs::write(&log, "previous run\n").unwrap();
    let launcher = RealLauncher::new(LogPolicy::default());
    let (events, rx) = sink();
    let command = shell(&script(&[
        echo("not json"),
        echo(PROGRESS),
        echo(READY),
        exit_with(0),
    ]));
    let process = launcher
        .launch_command(command, &log, "parse", events)
        .unwrap();
    assert!(process.pid() > 0);
    assert!(!process.adopted());

    let events = until_exit(&rx, Duration::from_secs(20));
    assert_eq!(events.len(), 3, "{events:#?}");
    match &events[0] {
        ProcessEvent::Line(StartupLine::Progress(line)) => {
            assert_eq!(line.startup_progress, 1);
            assert_eq!(line.phase, "opening state");
        }
        other => panic!("expected the progress line, got {other:?}"),
    }
    match &events[1] {
        ProcessEvent::Line(StartupLine::Ready(line)) => {
            assert_eq!(
                (line.repo.as_str(), line.device.as_str(), line.ipc, line.pid),
                ("r1", "d1", 3, 7)
            );
            assert_eq!(line.address, "ws://127.0.0.1:9");
            assert!(!line.mounted);
        }
        other => panic!("expected the ready line, got {other:?}"),
    }
    let (code, fatal, tail) = exit_of(&events[2]);
    assert_eq!(code, Some(0));
    assert_eq!(fatal, None);
    assert!(tail.contains(r#""ready":true"#), "tail: {tail}");
    assert!(
        rx.recv_timeout(Duration::from_millis(300)).is_err(),
        "Exited is the last event"
    );

    // worker.log is appended: the old content stays, every stdout line
    // (parsed or not) is copied.
    let text = fs::read_to_string(&log).unwrap();
    assert!(text.starts_with("previous run\n"), "{text}");
    for expected in ["not json", PROGRESS, READY] {
        assert!(
            text.contains(expected),
            "worker.log lacks {expected}: {text}"
        );
    }
}

#[test]
fn exit_event_carries_the_fatal_line_and_the_log_tail() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("worker.log");
    let launcher = RealLauncher::new(LogPolicy::default());
    let (events, rx) = sink();
    let command = shell(&script(&[
        echo(FATAL),
        echo_err("oops-on-stderr"),
        exit_with(73),
    ]));
    launcher
        .launch_command(command, &log, "fatal", events)
        .unwrap();
    let events = until_exit(&rx, Duration::from_secs(20));
    assert_eq!(events.len(), 2, "{events:#?}");
    match &events[0] {
        ProcessEvent::Line(StartupLine::Fatal(line)) => {
            assert_eq!(line.code, "STORE_IDENTITY_MISMATCH");
            assert_eq!(line.exit_code, 73);
        }
        other => panic!("expected the fatal line, got {other:?}"),
    }
    match &events[1] {
        ProcessEvent::Exited {
            code,
            fatal,
            stderr_tail,
        } => {
            assert_eq!(*code, Some(73));
            let fatal = fatal.as_ref().expect("the last FatalLine");
            assert_eq!(fatal.code, "STORE_IDENTITY_MISMATCH");
            assert_eq!(fatal.message, "STORE_IDENTITY_MISMATCH: wrong repository");
            assert!(stderr_tail.contains("oops-on-stderr"), "{stderr_tail}");
            assert!(
                stderr_tail.contains("STORE_IDENTITY_MISMATCH"),
                "{stderr_tail}"
            );
        }
        other => panic!("expected Exited, got {other:?}"),
    }
    let text = fs::read_to_string(&log).unwrap();
    assert!(
        text.contains("oops-on-stderr"),
        "stderr is appended: {text}"
    );
}

#[test]
fn stderr_tail_is_the_last_4_kib_of_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("worker.log");
    let launcher = RealLauncher::new(LogPolicy::default());
    let (events, rx) = sink();
    let filler = "x".repeat(48);
    let flood = if cfg!(windows) {
        format!("(for /l %i in (1,1,200) do @echo line-%i-{filler} 1>&2)")
    } else {
        format!("i=0; while [ $i -lt 200 ]; do echo line-$i-{filler} >&2; i=$((i+1)); done")
    };
    let command = shell(&script(&[flood, echo_err("END-MARKER"), exit_with(1)]));
    launcher
        .launch_command(command, &log, "tail", events)
        .unwrap();
    let events = until_exit(&rx, Duration::from_secs(30));
    let (code, fatal, tail) = exit_of(events.last().unwrap());
    assert_eq!(code, Some(1));
    assert_eq!(fatal, None);
    assert!(fs::metadata(&log).unwrap().len() > 8000);
    assert!(tail.len() <= 4096, "tail is {} bytes", tail.len());
    assert!(tail.len() > 3000, "tail is {} bytes", tail.len());
    assert!(tail.contains("END-MARKER"), "{tail}");
    assert!(
        !tail.contains("line-1-"),
        "the head of the log is not in the tail"
    );
}

#[test]
fn kill_ends_a_spawned_worker_and_reports_the_exit_promptly() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("worker.log");
    let launcher = RealLauncher::new(LogPolicy::default());
    let (events, rx) = sink();
    let (program, args) = sleeper_command();
    let mut command = Command::new(program);
    command.args(args);
    let process = launcher
        .launch_command(command, &log, "kill", events)
        .unwrap();
    let pid = process.pid();
    assert!(process_identity(pid).is_some(), "the child is running");
    assert!(
        rx.recv_timeout(Duration::from_millis(300)).is_err(),
        "no event before the kill (the sleeper prints no startup lines)"
    );
    let started = Instant::now();
    process.kill().unwrap();
    let events = until_exit(&rx, Duration::from_secs(10));
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "exit reported after {:?}",
        started.elapsed()
    );
    let (code, _, _) = exit_of(events.last().unwrap());
    if cfg!(windows) {
        assert_eq!(code, Some(1), "TerminateProcess exit code");
    } else {
        assert_eq!(code, None, "killed by a signal");
    }
    process.kill().expect("killing an exited worker is a no-op");
}

#[test]
fn launch_runs_daemon_state_supervised_with_the_spec_env() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    fs::create_dir_all(&state).unwrap();
    let ready = READY;
    #[cfg(windows)]
    let executable = {
        let path = dir.path().join("fake-worker.cmd");
        fs::write(
            &path,
            format!(
                "@echo off\r\necho args: %*\r\necho mark: %TKFS_TEST_MARK%\r\necho {ready}\r\nexit /b 0\r\n"
            ),
        )
        .unwrap();
        path
    };
    #[cfg(unix)]
    let executable = {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.path().join("fake-worker.sh");
        fs::write(
            &path,
            format!(
                "#!/bin/sh\necho \"args: $*\"\necho \"mark: $TKFS_TEST_MARK\"\necho '{ready}'\nexit 0\n"
            ),
        )
        .unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        path
    };
    let spec = LaunchSpec {
        state: "s1".into(),
        directory: state.clone(),
        executable,
        supervised: true,
        log: state.join("worker.log"),
        env: vec![("TKFS_TEST_MARK".into(), "m-42".into())],
    };
    let launcher = RealLauncher::new(LogPolicy::default());
    let (events, rx) = sink();
    // A freshly written script can be briefly busy on Linux (ETXTBSY) while
    // another test thread forks; retry that case only.
    let mut attempts = 0;
    loop {
        match launcher.launch(&spec, events.clone()) {
            Ok(_) => break,
            Err(error) if error.message.contains("busy") && attempts < 20 => {
                attempts += 1;
                thread::sleep(Duration::from_millis(50));
            }
            Err(error) => panic!("launch failed: {error}"),
        }
    }
    let events = until_exit(&rx, Duration::from_secs(20));
    assert!(
        events
            .iter()
            .any(|e| matches!(e, ProcessEvent::Line(StartupLine::Ready(r)) if r.repo == "r1")),
        "{events:#?}"
    );
    assert_eq!(exit_of(events.last().unwrap()).0, Some(0));
    let text = fs::read_to_string(state.join("worker.log")).unwrap();
    let args = text
        .lines()
        .find_map(|line| line.strip_prefix("args: "))
        .unwrap_or_else(|| panic!("no args line: {text}"));
    let args: Vec<&str> = args.split_whitespace().collect();
    assert_eq!(args.len(), 4, "{args:?}");
    assert_eq!(args[0], "daemon");
    assert_eq!(args[1], "--state");
    assert_eq!(
        canonical(Path::new(args[2].trim_matches('"'))),
        canonical(&state)
    );
    assert_eq!(args[3], "--supervised");
    assert!(text.contains("mark: m-42"), "{text}");
}

#[test]
fn command_line_follows_the_launch_spec() {
    let spec = LaunchSpec {
        state: "s".into(),
        directory: PathBuf::from("states").join("abc"),
        executable: PathBuf::from("tkfs-worker"),
        supervised: true,
        log: PathBuf::from("worker.log"),
        env: vec![("TKFS_FAULT_POINT".into(), "worker.startup".into())],
    };
    let command = RealLauncher::command(&spec);
    assert_eq!(command.get_program(), "tkfs-worker");
    let args: Vec<_> = command.get_args().collect();
    assert_eq!(
        args,
        [
            "daemon".as_ref(),
            "--state".as_ref(),
            spec.directory.as_os_str(),
            "--supervised".as_ref()
        ]
    );
    let envs: Vec<_> = command.get_envs().collect();
    assert_eq!(
        envs,
        [("TKFS_FAULT_POINT".as_ref(), Some("worker.startup".as_ref()))]
    );

    let legacy = LaunchSpec {
        supervised: false,
        executable: PathBuf::new(),
        ..spec
    };
    let command = RealLauncher::command(&legacy);
    assert_eq!(
        Path::new(command.get_program()),
        std::env::current_exe().unwrap(),
        "an empty executable means the current one"
    );
    assert_eq!(command.get_args().count(), 3, "no --supervised");
}

#[test]
fn launch_failure_is_a_worker_start_error() {
    let dir = tempfile::tempdir().unwrap();
    let launcher = RealLauncher::new(LogPolicy::default());
    let spec = LaunchSpec {
        state: "missing".into(),
        directory: dir.path().join("state"),
        executable: dir.path().join("no-such-binary.exe"),
        supervised: true,
        log: dir.path().join("state").join("worker.log"),
        env: vec![],
    };
    let (events, rx) = sink();
    let error = match launcher.launch(&spec, events) {
        Ok(_) => panic!("spawning a missing binary must fail"),
        Err(error) => error,
    };
    assert_eq!(error.code, "WORKER_START");
    assert!(rx.recv_timeout(Duration::from_millis(200)).is_err());
}

// ---------------------------------------------------------------------------
// Identity, adoption and verified kills

#[test]
fn process_identity_of_this_process_and_of_a_spawned_sleeper() {
    let pid = std::process::id();
    let (start, image) = process_identity(pid).expect("own identity");
    assert!(start > 0);
    assert_eq!(
        canonical(&image),
        canonical(&std::env::current_exe().unwrap())
    );
    assert_eq!(
        process_identity(pid),
        Some((start, image.clone())),
        "stable"
    );
    assert!(identity_matches(pid, start, &image));
    assert!(!identity_matches(pid, start + 1, &image));
    #[cfg(target_os = "linux")]
    {
        // `/proc/self/stat` field 22, as worker.json records it.
        let stat = fs::read_to_string("/proc/self/stat").unwrap();
        let rest = &stat[stat.rfind(')').unwrap() + 2..];
        let field22: u64 = rest.split_whitespace().nth(19).unwrap().parse().unwrap();
        assert_eq!(start, field22);
    }

    let (mut child, path) = sleeper();
    let (child_start, child_image) = process_identity(child.id()).expect("sleeper identity");
    assert_eq!(canonical(&child_image), canonical(&path));
    assert!(
        child_start >= start,
        "the sleeper started after this process"
    );
    assert!(identity_matches(child.id(), child_start, &path));
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(
        !identity_matches(child.id(), child_start, &path),
        "an exited process has no identity"
    );
}

#[test]
fn adoption_requires_pid_start_time_and_image_to_match() {
    let launcher = RealLauncher::new(LogPolicy::default());
    let (mut child, _) = sleeper();
    let pid = child.id();
    let (start, image) = process_identity(pid).unwrap();

    let (events, _rx) = sink();
    let refused = launcher
        .adopt(&record(pid, start + 1, image.clone()), events.clone())
        .unwrap();
    assert!(refused.is_none(), "mismatched start time");
    let refused = launcher
        .adopt(
            &record(pid, start, std::env::current_exe().unwrap()),
            events.clone(),
        )
        .unwrap();
    assert!(refused.is_none(), "foreign image");
    let refused = launcher
        .adopt(&record(0, start, image.clone()), events)
        .unwrap();
    assert!(refused.is_none(), "pid 0");
    thread::sleep(Duration::from_millis(200));
    assert!(
        child.try_wait().unwrap().is_none(),
        "a refused adoption never kills"
    );

    let (events, rx) = sink();
    let adopted = launcher
        .adopt(&record(pid, start, image.clone()), events)
        .unwrap()
        .expect("matching identity adopts");
    assert!(adopted.adopted());
    assert_eq!(adopted.pid(), pid);
    assert!(
        rx.recv_timeout(Duration::from_millis(1300)).is_err(),
        "no exit while the worker lives (the probe ran at least once)"
    );
    let started = Instant::now();
    adopted.kill().unwrap();
    let event = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the probe reports the exit");
    assert!(started.elapsed() < Duration::from_secs(5));
    let (code, fatal, tail) = exit_of(&event);
    assert_eq!(fatal, None);
    assert_eq!(tail, "", "an adopted worker's log is not known");
    if cfg!(windows) {
        assert_eq!(code, Some(1));
    } else {
        assert_eq!(code, None, "not our child: no exit code");
    }
    let status = wait_exit(&mut child, Duration::from_secs(10));
    assert!(!status.success());

    // A dead pid is never adopted (and if it was recycled, the identity
    // differs).
    let (events, _rx) = sink();
    assert!(
        launcher
            .adopt(&record(pid, start, image), events)
            .unwrap()
            .is_none()
    );
}

#[test]
fn dropping_an_adopted_process_never_kills_it() {
    let launcher = RealLauncher::new(LogPolicy::default());
    let (mut child, _) = sleeper();
    let pid = child.id();
    let (start, image) = process_identity(pid).unwrap();
    let (events, _rx) = sink();
    let adopted = launcher
        .adopt(&record(pid, start, image), events)
        .unwrap()
        .expect("adopts");
    drop(adopted);
    thread::sleep(Duration::from_millis(1500));
    assert!(child.try_wait().unwrap().is_none(), "still running");
    child.kill().unwrap();
    child.wait().unwrap();
}

#[test]
fn kill_refuses_a_mismatched_identity() {
    let (mut child, _) = sleeper();
    let pid = child.id();
    let (start, image) = process_identity(pid).unwrap();

    let error = kill_verified(pid, start + 1, &image).unwrap_err();
    assert_eq!(error.code, "WORKER_NOT_RUNNING", "{error}");
    let error = kill_verified(pid, start, &std::env::current_exe().unwrap()).unwrap_err();
    assert_eq!(error.code, "WORKER_NOT_RUNNING", "{error}");
    thread::sleep(Duration::from_millis(300));
    assert!(child.try_wait().unwrap().is_none(), "nothing was killed");

    kill_verified(pid, start, &image).expect("matching identity is killed");
    let status = wait_exit(&mut child, Duration::from_secs(10));
    assert!(!status.success());

    // Already exited. On Windows the test still holds the child's handle, so
    // the pid cannot have been recycled; on Linux it may have been, and then
    // the identity differs and nothing is killed either.
    let again = kill_verified(pid, start, &image);
    if cfg!(windows) {
        assert_eq!(again, Ok(()));
    } else {
        assert!(
            again.is_ok() || again.as_ref().unwrap_err().code == "WORKER_NOT_RUNNING",
            "{again:?}"
        );
    }
}

#[test]
fn launcher_client_targets_the_endpoint_instance() {
    let launcher = RealLauncher::new(LogPolicy::default());
    let endpoint = WorkerEndpoint {
        instance: "inst-1".into(),
        pid: 1,
        address: "ws://127.0.0.1:1".into(),
        version: "v".into(),
        token: None,
    };
    let client = launcher.client(&endpoint);
    assert_eq!(client.instance(), "inst-1");
}

// ---------------------------------------------------------------------------
// JsonWorkerClient

struct Worker {
    _dir: tempfile::TempDir,
    engine: Shared,
    repo: String,
    endpoint: WorkerEndpoint,
}

impl Drop for Worker {
    fn drop(&mut self) {
        if let Ok(engine) = self.engine.lock() {
            engine
                .stopping
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

/// A real worker RPC listener (`runtime::start_rpc`, the legacy wrapper).
fn worker() -> Worker {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("w");
    let repo = id();
    let store = Store::initialize(&state, &repo, &id()).unwrap();
    let engine: Shared = Arc::new(Mutex::new(Engine::new(store)));
    let info = runtime::start_rpc(engine.clone(), &state, None).unwrap();
    Worker {
        _dir: dir,
        engine,
        repo,
        endpoint: endpoint(info.address),
    }
}

fn endpoint(address: String) -> WorkerEndpoint {
    WorkerEndpoint {
        instance: id(),
        pid: std::process::id(),
        address,
        version: "test".into(),
        token: None,
    }
}

#[test]
fn remote_error_keeps_the_socket() {
    let worker = worker();
    let client = JsonWorkerClient::new(worker.endpoint.clone());
    assert_eq!(client.instance(), worker.endpoint.instance);
    let status = client
        .call("status", json!({}), Duration::from_secs(10))
        .unwrap();
    assert_eq!(status["repo"], worker.repo);
    assert_eq!((client.connects(), client.idle()), (1, 1));

    let error = client
        .call("no-such-op", Value::Null, Duration::from_secs(10))
        .unwrap_err();
    match &error {
        CallError::Remote(wire) => assert_eq!(wire.code, "UNKNOWN_OPERATION", "{wire}"),
        other => panic!("expected a remote error, got {other:?}"),
    }
    assert!(!error.is_uncertain());
    assert_eq!(
        (client.connects(), client.idle()),
        (1, 1),
        "a complete error reply keeps the connection"
    );

    let state = client
        .call("events", json!({}), Duration::from_secs(10))
        .unwrap();
    assert!(state.is_array());
    assert_eq!(
        client.connects(),
        1,
        "the same socket served all three calls"
    );
}

#[test]
fn timeout_drops_the_socket_and_is_uncertain() {
    let worker = worker();
    let client = JsonWorkerClient::new(worker.endpoint.clone());
    client
        .call("status", json!({}), Duration::from_secs(10))
        .unwrap();
    assert_eq!(client.connects(), 1);

    let error = {
        // The worker serves `events` under the engine lock, which we hold.
        let _held = worker.engine.lock().unwrap();
        let started = Instant::now();
        let error = client
            .call("events", json!({}), Duration::from_millis(400))
            .unwrap_err();
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "the deadline bounds the call: {:?}",
            started.elapsed()
        );
        error
    };
    match error {
        CallError::Timeout { after_ms } => assert!(after_ms >= 400, "{after_ms}"),
        other => panic!("expected a timeout, got {other:?}"),
    }
    assert!(error.is_uncertain());
    assert_eq!(error.code(), "REQUEST_TIMEOUT");
    assert_eq!(client.idle(), 0, "a timed-out socket is dropped");

    client
        .call("status", json!({}), Duration::from_secs(10))
        .unwrap();
    assert_eq!(client.connects(), 2, "the next call reconnects");
}

#[test]
fn uncertain_request_is_never_replayed() {
    let hung = support::HungServer::start();
    let client = JsonWorkerClient::new(endpoint(hung.endpoint()));
    let error = client
        .call("mkdir", json!({"path": "a"}), Duration::from_millis(300))
        .unwrap_err();
    assert!(matches!(error, CallError::Timeout { .. }), "{error:?}");
    assert!(error.is_uncertain());
    thread::sleep(Duration::from_millis(600));
    assert_eq!(hung.connections(), 1, "no second connection");
    assert_eq!(hung.received(), 1, "the request was sent exactly once");
    assert_eq!(client.idle(), 0);
}

#[test]
fn uncertain_mutation_on_a_real_worker_runs_at_most_once() {
    let worker = worker();
    let client = JsonWorkerClient::new(worker.endpoint.clone());
    let error = {
        // The worker reads the request, then waits for the engine lock.
        let _held = worker.engine.lock().unwrap();
        client
            .call("mkdir", json!({"path": "a"}), Duration::from_millis(300))
            .unwrap_err()
    };
    assert!(matches!(error, CallError::Timeout { .. }), "{error:?}");
    assert!(error.is_uncertain());
    assert_eq!(client.connects(), 1, "no reconnect, so no replay");
    assert_eq!(client.idle(), 0);

    // The request did execute, once, after the lock was released: it was
    // uncertain, not failed.
    let deadline = Instant::now() + Duration::from_secs(10);
    while worker.engine.lock().unwrap().store.lookup("a").is_err() {
        assert!(Instant::now() < deadline, "the uncertain mkdir never ran");
        thread::sleep(Duration::from_millis(20));
    }
    thread::sleep(Duration::from_millis(300));
    let events = worker.engine.lock().unwrap().store.events().unwrap();
    assert_eq!(events.len(), 1, "exactly one journaled mkdir");
    assert_eq!(client.connects(), 1);
}

#[test]
fn unreachable_worker_is_unavailable_and_certain() {
    let address = {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        format!("ws://{}", listener.local_addr().unwrap())
    };
    let client = JsonWorkerClient::new(endpoint(address));
    let error = client
        .call("status", json!({}), Duration::from_secs(2))
        .unwrap_err();
    assert!(matches!(error, CallError::Unavailable(_)), "{error:?}");
    assert!(!error.is_uncertain());
    assert_eq!(error.code(), "LOCAL_WORKER_UNAVAILABLE");
    assert_eq!(client.connects(), 0);
}

#[test]
fn an_upgrade_that_never_completes_is_bounded_by_the_deadline() {
    // The kernel completes the TCP handshake (backlog) but nobody accepts, so
    // the WebSocket upgrade never gets an answer.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client =
        JsonWorkerClient::new(endpoint(format!("ws://{}", listener.local_addr().unwrap())));
    let started = Instant::now();
    let error = client
        .call("hello", json!({}), Duration::from_millis(400))
        .unwrap_err();
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    assert!(matches!(error, CallError::Unavailable(_)), "{error:?}");
    assert!(!error.is_uncertain(), "nothing was sent");
    drop(listener);
}

#[test]
fn binary_ops_require_a_session() {
    let client = JsonWorkerClient::new(endpoint("ws://127.0.0.1:1".into()));
    let error = client
        .call_bytes("objects.read", json!({}), b"", Duration::from_secs(1))
        .unwrap_err();
    assert!(matches!(error, CallError::Unavailable(_)), "{error:?}");
    assert_eq!(error.code(), "BINARY_OPS_REQUIRE_SESSION");
}

#[test]
fn a_payload_that_is_not_an_object_is_refused_before_connecting() {
    let worker = worker();
    let client = JsonWorkerClient::new(worker.endpoint.clone());
    let error = client
        .call("status", json!([1, 2]), Duration::from_secs(2))
        .unwrap_err();
    assert_eq!(error.code(), "INVALID_REQUEST");
    assert!(!error.is_uncertain());
    assert_eq!(client.connects(), 0);
}

#[test]
fn concurrent_calls_share_a_pool_of_at_most_eight_idle_sockets() {
    let worker = worker();
    let client = JsonWorkerClient::new(worker.endpoint.clone());
    let threads = 12;
    let barrier = Arc::new(Barrier::new(threads));
    let handles: Vec<_> = (0..threads)
        .map(|_| {
            let client = client.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..5 {
                    client
                        .call("status", json!({}), Duration::from_secs(20))
                        .unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    assert!(client.idle() <= MAX_IDLE, "idle {}", client.idle());
    assert!(client.idle() >= 1);
    let connects = client.connects();
    client
        .call("status", json!({}), Duration::from_secs(10))
        .unwrap();
    assert_eq!(client.connects(), connects, "an idle socket is reused");
}

#[test]
fn stale_or_closed_idle_sockets_are_replaced_not_reused() {
    // Idle age: a socket older than the limit is closed, not reused.
    let worker = worker();
    let client = JsonWorkerClient::with_options(
        worker.endpoint.clone(),
        ClientOptions {
            max_idle: 8,
            max_idle_age: Duration::from_millis(100),
        },
    );
    client
        .call("status", json!({}), Duration::from_secs(10))
        .unwrap();
    thread::sleep(Duration::from_millis(250));
    client
        .call("status", json!({}), Duration::from_secs(10))
        .unwrap();
    assert_eq!(client.connects(), 2);

    // A worker that closes the connection after each reply: the closed idle
    // socket is detected and a fresh one is used (no uncertain failure).
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("ws://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        for (n, stream) in listener.incoming().take(2).enumerate() {
            let mut socket = tkfs::ws::accept(stream.unwrap()).unwrap();
            let request: Value = tkfs::ws::receive(&mut socket).unwrap();
            assert_eq!(request["payload"]["op"], "status");
            tkfs::ws::reply(&mut socket, Ok(json!({"n": n}))).unwrap();
        }
    });
    let client = JsonWorkerClient::new(endpoint(address));
    let first = client
        .call("status", json!({}), Duration::from_secs(10))
        .unwrap();
    thread::sleep(Duration::from_millis(200));
    let second = client
        .call("status", json!({}), Duration::from_secs(10))
        .unwrap();
    assert_eq!(
        (first["n"].as_u64(), second["n"].as_u64()),
        (Some(0), Some(1))
    );
    assert_eq!(client.connects(), 2);
    server.join().unwrap();
}
