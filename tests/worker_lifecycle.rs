//! WP1.4: worker lifecycle (REVAMP_PLAN §4.1, §4.2, §6.1, §14 WP1.4).
//!
//! Real `tkfs daemon` processes (through `support::WorkerProc`) for startup,
//! Ready/Fatal lines, `worker.json`, `hello`/`watch`/`stop`, accept faults,
//! idle exit and the poison exit; in-process engines (through
//! `runtime::start_rpc`) for the deterministic stop, poison-hook, gauge and
//! transaction cases. Mount cases need WinFsp or `/dev/fuse` and skip
//! otherwise (`support::mount_supported`).
mod support;

use rusqlite::Connection;
use serde_json::{Value, json};
use std::{
    fs,
    io::{BufRead, BufReader, Write},
    net::{SocketAddr, TcpStream},
    path::{Component, Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        Arc, Mutex,
        atomic::Ordering,
        mpsc::{self, RecvTimeoutError},
    },
    thread,
    time::{Duration, Instant},
};
use support::*;
use tkfs::{
    core::{Store, id},
    model::ipc::{HelloReply, ReadyLine, StopMode, StopReply, WorkerHeartbeat, WorkerRecord},
    runtime::{self, Engine, Shared},
    util::fault::{self, FaultAction, FaultConfig},
    worker::{self, record},
    ws,
};

const READY: Duration = Duration::from_secs(60);

fn new_state(dir: &Path) -> PathBuf {
    let state = dir.join("state");
    drop(Store::initialize(&state, &id(), &id()).unwrap());
    state
}

fn contract_golden(name: &str) -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/contracts")
        .join(format!("{name}.json"));
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

fn keys(value: &Value) -> Vec<String> {
    let mut keys: Vec<_> = value.as_object().unwrap().keys().cloned().collect();
    keys.sort();
    keys
}

fn pending_saves(state: &Path) -> i64 {
    Connection::open(state.join("metadata.sqlite"))
        .unwrap()
        .query_row("SELECT count(*) FROM pending_saves", [], |r| r.get(0))
        .unwrap_or(0)
}

/// Committed content of `path` in a stopped worker's store.
fn committed(state: &Path, path: &str) -> Vec<u8> {
    let store = Store::open_existing(state).unwrap();
    let entry = store.lookup(path).unwrap();
    store.read_object(&entry.content.unwrap()).unwrap()
}

fn remote_error(result: anyhow::Result<Value>) -> String {
    let error = result.expect_err("the call should fail");
    error
        .downcast_ref::<ws::RemoteError>()
        .unwrap_or_else(|| panic!("not a remote error: {error:#}"))
        .0
        .clone()
}

fn fatal_line(worker: &mut WorkerProc) -> Value {
    let error = worker
        .wait_ready(READY)
        .expect_err("the worker must not start");
    worker
        .seen
        .iter()
        .find(|line| line["fatal"] == true)
        .cloned()
        .unwrap_or_else(|| panic!("no fatal line: {error:#}"))
}

/// Waits until the worker reports no unsaved files and no health message.
fn wait_saved(worker: &WorkerProc) {
    wait_for("pending saves committed", Duration::from_secs(20), || {
        worker
            .rpc(json!({"op":"status"}))
            .is_ok_and(|status| status["unflushed_files"] == 0 && status["health"].is_null())
    });
}

/// FUSE commits a dirty file on every `FLUSH`, and the kernel also sends one
/// when a process forked by a parallel test closes its inherited copy of the
/// test's descriptor (at `exec`). A mounted dirty session may therefore be
/// clean (committed, nothing to retain) by the time a stop runs; the restart
/// checks below still require the bytes.
fn fuse_may_flush() -> bool {
    cfg!(target_os = "linux")
}

/// A worker spawned directly, for what `WorkerProc` cannot do: a chosen
/// working directory, and the arrival time of every stdout line. Killed on
/// drop if still running.
struct TimedWorker {
    child: Child,
    lines: mpsc::Receiver<(Instant, Value)>,
}

impl TimedWorker {
    fn spawn(cwd: &Path, state: &Path, args: &[&str], env: &[(&str, &str)]) -> Self {
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(state.join("worker.log"))
            .unwrap();
        let mut command = Command::new(bin());
        command
            .current_dir(cwd)
            .arg("daemon")
            .arg("--state")
            .arg(state)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(log);
        for (key, value) in env {
            command.env(key, value);
        }
        let mut child = command.spawn().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sender, lines) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if let Ok(value) = serde_json::from_str::<Value>(&line)
                    && sender.send((Instant::now(), value)).is_err()
                {
                    break;
                }
            }
        });
        Self { child, lines }
    }

    /// Every stdout line up to and including Ready, with its arrival time.
    fn until_ready(&mut self, state: &Path) -> Vec<(Instant, Value)> {
        let deadline = Instant::now() + READY;
        let mut seen = Vec::new();
        loop {
            match self.lines.recv_timeout(Duration::from_millis(100)) {
                Ok((at, line)) => {
                    assert!(line["fatal"] != true, "{line}");
                    let ready = line["ready"] == true;
                    seen.push((at, line));
                    if ready {
                        return seen;
                    }
                }
                Err(RecvTimeoutError::Timeout) => assert!(
                    Instant::now() < deadline,
                    "no Ready: {seen:?}\n{}",
                    tail(&state.join("worker.log"), 8 * 1024)
                ),
                Err(RecvTimeoutError::Disconnected) => panic!(
                    "the worker exited before Ready: {seen:?}\n{}",
                    tail(&state.join("worker.log"), 8 * 1024)
                ),
            }
        }
    }

    fn wait_exit(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().ok().flatten() {
                return Some(status);
            }
            if Instant::now() >= deadline {
                return None;
            }
            thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for TimedWorker {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// Whether process `pid` owns a listening TCP socket; `None` where this test
/// cannot tell (yet). Windows: `netstat -ano -p TCP` rows of `pid` whose
/// foreign address is the unspecified `0.0.0.0:0` (listening rows; the state
/// column is localized, the addresses are not). Linux: a `LISTEN` (`0A`) row
/// of `/proc/<pid>/net/tcp{,6}` whose inode is one of the process's sockets,
/// once the worker holds `owner.lock` (step 1 of `worker::run`): until `exec`
/// has closed them, the forked child still holds the test process's
/// descriptors, other tests' listeners included (and `/proc/<pid>/exe`
/// already names the new image before they are closed).
#[cfg(windows)]
fn listening(pid: u32) -> Option<bool> {
    let output = Command::new("netstat")
        .args(["-ano", "-p", "TCP"])
        .output()
        .ok()?;
    let pid = pid.to_string();
    Some(String::from_utf8_lossy(&output.stdout).lines().any(|line| {
        let columns: Vec<_> = line.split_whitespace().collect();
        columns.len() == 5 && columns[0] == "TCP" && columns[2] == "0.0.0.0:0" && columns[4] == pid
    }))
}
#[cfg(target_os = "linux")]
fn listening(pid: u32) -> Option<bool> {
    let descriptors: Vec<PathBuf> = fs::read_dir(format!("/proc/{pid}/fd"))
        .ok()?
        .filter_map(|entry| fs::read_link(entry.ok()?.path()).ok())
        .collect();
    if !descriptors
        .iter()
        .any(|target| target.ends_with("owner.lock"))
    {
        return None;
    }
    let sockets: std::collections::HashSet<String> = descriptors
        .iter()
        .filter_map(|target| {
            let target = target.to_str()?;
            Some(
                target
                    .strip_prefix("socket:[")?
                    .strip_suffix(']')?
                    .to_owned(),
            )
        })
        .collect();
    let mut listening = false;
    for table in ["tcp", "tcp6"] {
        let Ok(text) = fs::read_to_string(format!("/proc/{pid}/net/{table}")) else {
            continue;
        };
        listening |= text.lines().skip(1).any(|line| {
            let columns: Vec<_> = line.split_whitespace().collect();
            columns.len() > 9 && columns[3] == "0A" && sockets.contains(columns[9])
        });
    }
    Some(listening)
}
#[cfg(not(any(windows, target_os = "linux")))]
fn listening(_pid: u32) -> Option<bool> {
    None
}

/// After a test left a FUSE mount behind (a force stop or an exit with the
/// mount up), unmount it so the temporary directory can be removed.
fn release_mount(_path: &Path) {
    #[cfg(target_os = "linux")]
    {
        let _ = std::process::Command::new("fusermount3")
            .args(["-u", "-z", "--"])
            .arg(_path)
            .status();
    }
}

// ---------------------------------------------------------------------------
// Real binary: startup, Ready, worker.json, hello, stop

/// Ready is printed with `mounted:false` and the mount error when the mount
/// path exists; RPC keeps working; the Ready line has the golden shape.
#[test]
fn mount_failure_is_reported_in_ready_and_rpc_keeps_serving() {
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    let mount = dir.path().join("occupied");
    fs::create_dir_all(&mount).unwrap();
    fs::write(mount.join("keep.txt"), b"user data").unwrap();
    let mount_arg = mount.to_string_lossy().into_owned();
    let mut worker = WorkerProc::spawn(&state, &["--mount", &mount_arg], &[]).unwrap();
    let ready = worker.wait_ready(READY).unwrap();
    assert_eq!(ready["mounted"], false, "{ready}");
    let code = ready["mount_error"]["code"].as_str().unwrap();
    if mount_supported() {
        assert_eq!(code, "MOUNT_PATH_MUST_NOT_EXIST", "{ready}");
    } else {
        assert!(
            ["MOUNT_PATH_MUST_NOT_EXIST", "WINFSP_RUNTIME_UNAVAILABLE"].contains(&code),
            "{ready}"
        );
    }
    assert_eq!(
        ready["mount_error"]["class"], "needs_user_action",
        "{ready}"
    );
    assert_same_shape(&contract_golden("ipc.ready_line"), &ready);
    let typed: ReadyLine = serde_json::from_value(ready.clone()).unwrap();
    assert_eq!(typed.ipc, 3);
    assert_eq!(Some(typed.pid), worker.pid());

    // runtime.json advertises the absolute path it tried to attach, as the
    // legacy worker did (bucket-export keeps refusing to write there).
    assert_eq!(
        discovery_at(&state).unwrap().mount.map(PathBuf::from),
        Some(mount.clone())
    );
    let status = worker.rpc(json!({"op":"status"})).unwrap();
    assert_eq!(status["mounted"], false);
    assert_eq!(status["mount"]["state"], "attach_failed", "{status}");
    assert!(status["store_health"]["state"] == "healthy", "{status}");
    let summary = worker.rpc(json!({"op":"published-summary"})).unwrap();
    assert_eq!(summary["repo"], typed.repo);
    assert_eq!(summary["replica"], typed.device);
    assert!(summary["tip"]["seq"].is_i64(), "{summary}");
    worker
        .rpc(json!({"op":"import","path":"a.txt","hex":hex::encode(b"a")}))
        .unwrap();
    assert_eq!(
        fs::read(mount.join("keep.txt")).unwrap(),
        b"user data",
        "the user directory is untouched"
    );
    let stopped = worker.rpc(json!({"op":"stop"})).unwrap();
    assert_eq!(
        stopped,
        json!({"stopped":true,"persisted_dirty":0,"detached":false})
    );
    let status = worker.wait_exit(Duration::from_secs(10)).expect("exit");
    assert_eq!(status.code(), Some(0));
    assert_eq!(committed(&state, "a.txt"), b"a");
}

/// `worker.json` matches the golden record and the process; `hello` agrees
/// with Ready and the record; a clean stop removes both discovery files.
#[test]
fn ready_worker_record_and_hello_describe_the_same_process() {
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    let mut worker = WorkerProc::spawn(&state, &[], &[]).unwrap();
    let ready: ReadyLine = serde_json::from_value(worker.wait_ready(READY).unwrap()).unwrap();
    assert!(ready.ready && !ready.mounted && ready.mount_error.is_none());
    assert_eq!(ready.ipc, 3);
    assert_eq!(Some(ready.pid), worker.pid());
    assert!(uuid::Uuid::parse_str(&ready.instance).is_ok());
    assert!(ready.version.contains('+'), "{}", ready.version);
    assert!(ready.token.is_none());
    let discovery = discovery_at(&state).unwrap();
    assert_eq!(ready.address, discovery.address);
    assert_eq!(
        fs::canonicalize(&ready.runtime).unwrap(),
        fs::canonicalize(state.join("runtime.json")).unwrap()
    );

    let raw: Value = serde_json::from_slice(&fs::read(record::path(&state)).unwrap()).unwrap();
    assert_eq!(keys(&raw), keys(&contract_golden("ipc.worker_record")));
    let written: WorkerRecord = serde_json::from_value(raw).unwrap();
    assert_eq!(record::read(&state).unwrap().as_ref(), Some(&written));
    assert_eq!(written.format, 1);
    assert_eq!(written.ipc, 3);
    assert_eq!(written.pid, ready.pid);
    assert_eq!(written.instance, ready.instance);
    assert_eq!(written.address, ready.address);
    assert_eq!(written.version, ready.version);
    assert_eq!(
        (&written.repo, &written.device),
        (&ready.repo, &ready.device)
    );
    assert!(!written.supervised);
    assert!(written.process_start > 0);
    assert_eq!(
        Some(written.process_start),
        record::process_start_of(written.pid)
    );
    assert_eq!(
        fs::canonicalize(&written.executable).unwrap(),
        fs::canonicalize(bin()).unwrap()
    );

    let hello: HelloReply =
        serde_json::from_value(worker.rpc(json!({"op":"hello"})).unwrap()).unwrap();
    assert_eq!(
        hello,
        HelloReply {
            instance: written.instance.clone(),
            pid: written.pid,
            repo: written.repo.clone(),
            device: written.device.clone(),
            version: written.version.clone(),
            ipc: 3,
            started_at_ms: written.started_at_ms,
            process_start: written.process_start,
        }
    );
    // WP1.INT wired `revalidate` to `Store::revalidate` (§14 WP1.INT task
    // 1): nothing is quarantined here, so nothing is re-queued.
    assert_eq!(
        worker.rpc(json!({"op":"revalidate"})).unwrap(),
        json!({"requeued": 0})
    );
    assert!(
        remote_error(worker.rpc(json!({"op":"feed-offer","route":{}})))
            .starts_with("UNSUPPORTED_OPERATION")
    );
    assert!(
        remote_error(worker.rpc(json!({"op":"objects.unknown"})))
            .starts_with("UNSUPPORTED_OPERATION")
    );

    worker.rpc(json!({"op":"stop","mode":"graceful"})).unwrap();
    let status = worker.wait_exit(Duration::from_secs(10)).expect("exit");
    assert_eq!(status.code(), Some(0));
    assert!(!state.join("runtime.json").exists());
    assert!(!record::path(&state).exists());
    assert_eq!(record::read(&state).unwrap(), None);
}

/// `process_start_of` is a liveness check as well as an identity one: a
/// worker that has exited but is not reaped yet (a zombie on Linux; on
/// Windows, a process object kept by this test's `Child` handle) reads as
/// absent, so adoption never takes a dead worker for its live record.
#[test]
fn process_start_of_treats_an_exited_unreaped_worker_as_absent() {
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    assert!(record::process_start_of(std::process::id()).is_some());
    let mut worker = TimedWorker::spawn(dir.path(), &state, &[], &[]);
    worker.until_ready(&state);
    let pid = worker.child.id();
    let written = record::read(&state).unwrap().unwrap();
    assert_eq!(written.pid, pid);
    assert_eq!(record::process_start_of(pid), Some(written.process_start));
    rpc_at(&state, json!({"op":"stop"})).unwrap();
    // The child is neither waited for nor polled until the check passes.
    wait_for(
        "the exited, unreaped worker reads as absent",
        Duration::from_secs(20),
        || record::process_start_of(pid).is_none(),
    );
    let exit = worker.child.wait().unwrap();
    assert_eq!(exit.code(), Some(0));
    assert_eq!(record::process_start_of(pid), None);
}

/// A fatal failure once `runtime.json` is published (§4.1 step 7; here the
/// `worker.json` write, failed by `worker.write_record`) withdraws
/// `runtime.json` before the `FatalLine` (exit 70), so nobody finds a
/// listener that dies with the process; a previous worker's `worker.json`
/// (what a crash leaves) is kept for the next worker's recovery.
#[test]
fn fatal_after_publish_withdraws_runtime_json_and_keeps_the_previous_record() {
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    let mut previous = WorkerProc::spawn(&state, &[], &[]).unwrap();
    previous.wait_ready(READY).unwrap();
    let previous_record = fs::read(record::path(&state)).unwrap();
    previous.stop().unwrap();
    fs::write(record::path(&state), &previous_record).unwrap();

    let mut worker = WorkerProc::spawn(
        &state,
        &[],
        &[
            ("TKFS_FAULT_POINT", "worker.write_record"),
            ("TKFS_FAULT_ACTION", "fail"),
        ],
    )
    .unwrap();
    let fatal = fatal_line(&mut worker);
    assert_eq!(fatal["code"], "FAULT_INJECTED", "{fatal}");
    assert_eq!(fatal["exit_code"], 70, "{fatal}");
    let exit = worker.wait_exit(Duration::from_secs(10)).expect("exit");
    assert_eq!(exit.code(), Some(70));
    assert!(
        worker.seen.iter().all(|line| line["ready"] != true),
        "{:?}",
        worker.seen
    );
    assert!(
        !state.join("runtime.json").exists(),
        "runtime.json withdrawn"
    );
    assert_eq!(
        fs::read(record::path(&state)).unwrap(),
        previous_record,
        "the previous worker's record is kept"
    );
}

/// Invariant 24: store recovery (including pending saves) completes before
/// the listener exists. `worker.startup` holds the worker between recovery
/// and the listener: nothing is published, the process owns no listening
/// socket, and only after Ready does the recovered save commit.
#[test]
fn rpc_after_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    let saved = b"bytes retained before a crash".to_vec();
    {
        let mut engine = Engine::new(Store::open_existing(&state).unwrap());
        let (h, _) = engine.open("retained.txt", Some("file"), true).unwrap();
        engine.write(h, 0, &saved, false, false).unwrap();
        assert_eq!(engine.retain_all_dirty().unwrap(), 1);
    }
    assert_eq!(pending_saves(&state), 1);
    let mut worker = WorkerProc::spawn(
        &state,
        &[],
        &[
            ("TKFS_FAULT_POINT", "worker.startup"),
            ("TKFS_FAULT_ACTION", "hang"),
            ("TKFS_FAULT_MS", "2500"),
        ],
    )
    .unwrap();
    let pid = worker.pid().unwrap();
    // The hold starts after spawn, so it lasts at least until here.
    let held_until = worker.spawned_at + Duration::from_millis(2400);
    let started = Instant::now();
    let mut sampled = 0;
    while started.elapsed() < Duration::from_millis(1500) {
        assert!(
            !state.join("runtime.json").exists(),
            "runtime.json before recovery"
        );
        assert!(
            !record::path(&state).exists(),
            "worker.json before recovery"
        );
        // A listener opened early (before the fault point) shows up here
        // even if the files were still published after it.
        if let Some(listening) = listening(pid)
            && Instant::now() < held_until
        {
            assert!(!listening, "a listening socket before recovery");
            sampled += 1;
        }
        assert!(worker.try_exit().is_none());
        thread::sleep(Duration::from_millis(50));
    }
    if cfg!(any(windows, target_os = "linux")) {
        assert!(sampled >= 1, "no listener sample inside the hold");
    }
    let ready = worker.wait_ready(READY).unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(2400),
        "the fault held startup"
    );
    if let Some(listening) = listening(pid) {
        assert!(listening, "the probe sees the listener once Ready");
    }
    let ready_at = worker.seen.iter().position(|l| l["ready"] == true).unwrap();
    assert!(
        worker.seen[..ready_at]
            .iter()
            .all(|l| l["startup_progress"].is_u64()),
        "{:?}",
        worker.seen
    );
    assert_eq!(ready["mounted"], false);
    wait_saved(&worker);
    worker.stop().unwrap();
    assert_eq!(pending_saves(&state), 0);
    assert_eq!(committed(&state, "retained.txt"), saved);
}

/// `owner.lock` held → `ALREADY_RUNNING` (exit 75); an uninitialized state →
/// `STATE_NOT_INITIALIZED` (exit 73); a missing state directory → exit 78.
#[test]
fn startup_failures_print_a_fatal_line_and_exit_with_its_code() {
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    let mut first = WorkerProc::spawn(&state, &[], &[]).unwrap();
    first.wait_ready(READY).unwrap();
    let mut second = WorkerProc::spawn(&state, &[], &[]).unwrap();
    let fatal = fatal_line(&mut second);
    assert_eq!(fatal["code"], "ALREADY_RUNNING", "{fatal}");
    assert_eq!(fatal["class"], "needs_user_action");
    assert_eq!(fatal["exit_code"], 75);
    let exit = second.wait_exit(Duration::from_secs(10)).expect("exit");
    assert_eq!(exit.code(), Some(75));
    // The running worker and its records are untouched.
    assert!(first.rpc(json!({"op":"hello"})).is_ok());
    assert_eq!(
        record::read(&state).unwrap().unwrap().pid,
        first.pid().unwrap()
    );
    first.stop().unwrap();

    let empty = dir.path().join("empty");
    fs::create_dir_all(&empty).unwrap();
    let mut uninitialized = WorkerProc::spawn(&empty, &[], &[]).unwrap();
    let fatal = fatal_line(&mut uninitialized);
    assert_eq!(fatal["code"], "STATE_NOT_INITIALIZED", "{fatal}");
    assert_eq!(fatal["class"], "fatal");
    let exit = uninitialized
        .wait_exit(Duration::from_secs(10))
        .expect("exit");
    assert_eq!(exit.code(), Some(73));

    // A missing state directory cannot hold the log `WorkerProc` opens, so
    // spawn the binary directly.
    let missing = dir.path().join("missing");
    let output = std::process::Command::new(bin())
        .arg("daemon")
        .arg("--state")
        .arg(&missing)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(78));
    let line: Value = serde_json::from_slice(
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .last()
            .unwrap()
            .as_bytes(),
    )
    .unwrap();
    assert_eq!(line["fatal"], true);
    assert_eq!(line["code"], "STATE_NOT_INITIALIZED");
    assert_eq!(line["exit_code"], 78);
}

/// `worker.accept_error` (3 injected errors): every error is logged and the
/// listener keeps serving.
#[test]
fn accept_errors_never_stop_the_listener() {
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    let mut worker = WorkerProc::spawn(
        &state,
        &[],
        &[
            ("TKFS_FAULT_POINT", "worker.accept_error"),
            ("TKFS_FAULT_ACTION", "fail"),
            ("TKFS_FAULT_COUNT", "3"),
        ],
    )
    .unwrap();
    worker.wait_ready(READY).unwrap();
    let results: Vec<bool> = (0..10)
        .map(|_| worker.rpc(json!({"op":"hello"})).is_ok())
        .collect();
    let failed = results.iter().filter(|ok| !**ok).count();
    assert!((1..=3).contains(&failed), "{results:?}");
    assert!(
        results[results.len() - 5..].iter().all(|ok| *ok),
        "{results:?}"
    );
    let log = fs::read_to_string(state.join("worker.log")).unwrap();
    assert_eq!(
        log.matches("FAULT_INJECTED: worker.accept_error").count(),
        3,
        "{log}"
    );
    assert!(worker.try_exit().is_none(), "worker alive");
    worker.stop().unwrap();
}

/// `watch{after_seq}` returns the next heartbeat within 1.5 s; a `watch` that
/// waits for a sequence far ahead returns the current heartbeat at `wait_ms`.
#[test]
fn watch_returns_the_next_heartbeat_within_1500ms() {
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    let mut worker = WorkerProc::spawn(&state, &[], &[]).unwrap();
    let ready = worker.wait_ready(READY).unwrap();
    let first: WorkerHeartbeat = serde_json::from_value(
        worker
            .rpc(json!({"op":"watch","after_seq":0,"wait_ms":2000}))
            .unwrap(),
    )
    .unwrap();
    assert!(first.seq >= 1);
    assert_eq!(first.instance, ready["instance"].as_str().unwrap());
    assert!(first.store_health.is_healthy());
    let mut after = first.seq;
    for _ in 0..3 {
        let started = Instant::now();
        let next: WorkerHeartbeat = serde_json::from_value(
            worker
                .rpc(json!({"op":"watch","after_seq":after,"wait_ms":5000}))
                .unwrap(),
        )
        .unwrap();
        let waited = started.elapsed();
        assert!(next.seq > after, "seq {} after {after}", next.seq);
        assert!(waited <= Duration::from_millis(1500), "{waited:?}");
        after = next.seq;
    }
    let started = Instant::now();
    let current: WorkerHeartbeat = serde_json::from_value(
        worker
            .rpc(json!({"op":"watch","after_seq":after + 1000,"wait_ms":300}))
            .unwrap(),
    )
    .unwrap();
    assert!(current.seq <= after + 1000);
    assert!(started.elapsed() < Duration::from_millis(1500));
    // The engine-lock gauges move with RPCs that take the engine.
    worker.rpc(json!({"op":"state"})).unwrap();
    let later: WorkerHeartbeat = serde_json::from_value(
        worker
            .rpc(json!({"op":"watch","after_seq":current.seq,"wait_ms":3000}))
            .unwrap(),
    )
    .unwrap();
    assert!(later.engine.holds >= 1, "{later:?}");
    assert!(!later.engine.stalled);
    assert!(later.uptime_ms >= first.uptime_ms);
    worker.stop().unwrap();
}

/// `--supervised`: Ready before any attach; `mount-status`; `mount-attach`
/// errors are replies (never exits); an attach of a valid path works when
/// mounts are supported; `worker.json` records `supervised`.
#[test]
fn supervised_worker_is_ready_before_mount_and_attaches_on_request() {
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    let mount = dir.path().join("mnt");
    let mount_arg = mount.to_string_lossy().into_owned();
    let mut worker = WorkerProc::spawn(
        &state,
        &["--supervised", "--mount", &mount_arg],
        &[("TKFS_UNSUPERVISED_EXIT_S", "0")],
    )
    .unwrap();
    let ready = worker.wait_ready(READY).unwrap();
    assert_eq!(ready["mounted"], false);
    assert!(ready["mount_error"].is_null(), "{ready}");
    assert!(!mount.exists(), "nothing attached before mount-attach");
    assert_eq!(
        discovery_at(&state).unwrap().mount,
        None,
        "nothing advertised before mount-attach"
    );
    assert!(record::read(&state).unwrap().unwrap().supervised);
    let status = worker.rpc(json!({"op":"mount-status"})).unwrap();
    assert_eq!(status["state"], "detached", "{status}");

    let inside = state.join("inside");
    let error = remote_error(worker.rpc(json!({"op":"mount-attach","path":inside})));
    assert!(error.starts_with("MOUNT_OVERLAPS_STATE"), "{error}");
    assert!(!inside.exists());

    if mount_supported() {
        let attached = worker
            .rpc(json!({"op":"mount-attach","path":mount_arg}))
            .unwrap();
        assert_eq!(attached["state"], "attached", "{attached}");
        assert_eq!(
            discovery_at(&state).unwrap().mount.map(PathBuf::from),
            Some(mount.clone()),
            "runtime.json advertises the attached path"
        );
        fs::write(mount.join("through-mount.txt"), b"mounted bytes").unwrap();
        let status = worker.rpc(json!({"op":"status"})).unwrap();
        assert_eq!(status["mounted"], true, "{status}");
        let heartbeat = worker
            .rpc(json!({"op":"watch","after_seq":0,"wait_ms":0}))
            .unwrap();
        assert_eq!(heartbeat["mount"]["state"], "attached", "{heartbeat}");
        // A scanner may briefly hold the new file open: retry BUSY_VIEW.
        let mut stopped = Value::Null;
        wait_for("graceful stop", Duration::from_secs(20), || {
            match worker.rpc(json!({"op":"stop"})) {
                Ok(reply) => {
                    stopped = reply;
                    true
                }
                Err(error) => {
                    assert!(format!("{error:#}").contains("BUSY_VIEW"), "{error:#}");
                    false
                }
            }
        });
        assert_eq!(stopped["detached"], true, "{stopped}");
        let exit = worker.wait_exit(Duration::from_secs(10)).expect("exit");
        assert_eq!(exit.code(), Some(0));
        assert_eq!(committed(&state, "through-mount.txt"), b"mounted bytes");
    } else {
        assert_eq!(discovery_at(&state).unwrap().mount, None);
        worker.stop().unwrap();
    }
}

/// `mount-attach` of an occupied path is an error reply with the mount
/// controller's code; the worker keeps serving and reports `attach_failed`.
#[test]
fn mount_attach_failure_is_a_reply_and_the_worker_keeps_serving() {
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    let occupied = dir.path().join("occupied");
    fs::create_dir_all(&occupied).unwrap();
    fs::write(occupied.join("keep.txt"), b"user data").unwrap();
    let mut worker = WorkerProc::spawn(
        &state,
        &["--supervised"],
        &[("TKFS_UNSUPERVISED_EXIT_S", "0")],
    )
    .unwrap();
    worker.wait_ready(READY).unwrap();
    let error = remote_error(worker.rpc(json!({"op":"mount-attach","path":occupied})));
    if mount_supported() {
        assert!(error.starts_with("MOUNT_PATH_MUST_NOT_EXIST"), "{error}");
    }
    let status = worker.rpc(json!({"op":"mount-status"})).unwrap();
    assert_eq!(status["state"], "attach_failed", "{status}");
    assert_eq!(status["error"]["class"], "needs_user_action", "{status}");
    worker
        .rpc(json!({"op":"mkdir","path":"still-serving"}))
        .unwrap();
    assert!(worker.try_exit().is_none());
    assert_eq!(fs::read(occupied.join("keep.txt")).unwrap(), b"user data");
    worker.stop().unwrap();
}

/// A supervised worker that nobody contacts stops itself (gracefully, exit 0,
/// records removed); `watch` contact keeps it alive.
#[test]
fn unsupervised_worker_exits_when_idle_and_contact_keeps_it_alive() {
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    let mut idle = WorkerProc::spawn(
        &state,
        &["--supervised"],
        &[("TKFS_UNSUPERVISED_EXIT_S", "1")],
    )
    .unwrap();
    idle.wait_ready(READY).unwrap();
    let exit = idle.wait_exit(Duration::from_secs(15)).expect("idle exit");
    assert_eq!(exit.code(), Some(0));
    assert!(!record::path(&state).exists());
    assert!(!state.join("runtime.json").exists());
    let log = fs::read_to_string(state.join("worker.log")).unwrap();
    assert!(log.contains("WORKER_STOPPING"), "{log}");

    let mut watched = WorkerProc::spawn(
        &state,
        &["--supervised"],
        &[("TKFS_UNSUPERVISED_EXIT_S", "2")],
    )
    .unwrap();
    watched.wait_ready(READY).unwrap();
    let until = Instant::now() + Duration::from_secs(4);
    let mut after = 0;
    while Instant::now() < until {
        let heartbeat = watched
            .rpc(json!({"op":"watch","after_seq":after,"wait_ms":500}))
            .unwrap();
        after = heartbeat["seq"].as_u64().unwrap();
        assert!(watched.try_exit().is_none(), "exited while watched");
    }
    watched.stop().unwrap();
}

// ---------------------------------------------------------------------------
// Real binary with a mount: open handles, dirty sessions, poison

/// Graceful stop with an open file is refused (`BUSY_VIEW`) and the process
/// stays up; force stop persists the dirty session, exits 0, and a restart
/// commits the bytes.
#[test]
fn mounted_stop_ladder_refuses_then_persists_dirty_files() {
    if !mount_supported() {
        eprintln!("skipped: no mount support");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    let mount = dir.path().join("mnt");
    let mount_arg = mount.to_string_lossy().into_owned();
    let mut worker = WorkerProc::spawn(&state, &["--mount", &mount_arg], &[]).unwrap();
    let ready = worker.wait_ready(READY).unwrap();
    assert_eq!(ready["mounted"], true, "{ready}");
    let bytes = b"dirty bytes that must survive a force stop".to_vec();
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(mount.join("dirty.txt"))
        .unwrap();
    file.write_all(&bytes).unwrap();
    wait_for("the dirty session", Duration::from_secs(10), || {
        worker
            .rpc(json!({"op":"status"}))
            .is_ok_and(|s| s["unflushed_files"] == 1)
    });
    let refused = remote_error(worker.rpc(json!({"op":"stop"})));
    assert!(refused.starts_with("BUSY_VIEW"), "{refused}");
    thread::sleep(Duration::from_millis(300));
    assert!(
        worker.try_exit().is_none(),
        "graceful refusal keeps the worker"
    );
    assert!(worker.rpc(json!({"op":"hello"})).is_ok());

    let stopped = worker.rpc(json!({"op":"stop","mode":"force"})).unwrap();
    assert_eq!(stopped["stopped"], true, "{stopped}");
    let persisted = stopped["persisted_dirty"].as_u64().unwrap();
    assert!(persisted >= 1 || fuse_may_flush(), "{stopped}");
    let exit = worker.wait_exit(Duration::from_secs(15)).expect("exit");
    drop(file);
    assert_eq!(exit.code(), Some(0));
    if persisted >= 1 {
        assert!(pending_saves(&state) >= 1);
    }
    release_mount(&mount);

    let mut restarted = WorkerProc::spawn(&state, &[], &[]).unwrap();
    restarted.wait_ready(READY).unwrap();
    wait_saved(&restarted);
    restarted.stop().unwrap();
    assert_eq!(pending_saves(&state), 0);
    assert_eq!(committed(&state, "dirty.txt"), bytes);
}

/// Invariant 8 guard (§10): `engine.poison` (panic under the engine lock):
/// the worker persists the dirty session and exits 70; a restart commits the
/// bytes.
#[test]
fn poison_exit_persists_dirty() {
    if !mount_supported() {
        eprintln!("skipped: no mount support");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    let mount = dir.path().join("mnt");
    let mount_arg = mount.to_string_lossy().into_owned();
    let mut worker = WorkerProc::spawn(
        &state,
        &["--mount", &mount_arg],
        &[
            ("TKFS_FAULT_POINT", "engine.poison"),
            ("TKFS_FAULT_ACTION", "panic"),
        ],
    )
    .unwrap();
    worker.wait_ready(READY).unwrap();
    let bytes = b"dirty bytes that must survive a poisoned engine".to_vec();
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(mount.join("dirty.txt"))
        .unwrap();
    file.write_all(&bytes).unwrap();
    // `hello` and `watch` never take the engine; `state` does and poisons it.
    assert!(worker.rpc(json!({"op":"hello"})).is_ok());
    let _ = worker.rpc(json!({"op":"state"}));
    let exit = worker.wait_exit(Duration::from_secs(30)).expect("exit");
    drop(file);
    assert_eq!(exit.code(), Some(70));
    assert!(pending_saves(&state) >= 1 || fuse_may_flush());
    let log = fs::read_to_string(state.join("worker.log")).unwrap();
    assert!(log.contains("ENGINE_POISONED"), "{log}");
    release_mount(&mount);

    let mut restarted = WorkerProc::spawn(&state, &[], &[]).unwrap();
    restarted.wait_ready(READY).unwrap();
    wait_saved(&restarted);
    restarted.stop().unwrap();
    assert_eq!(committed(&state, "dirty.txt"), bytes);
}

/// Invariant 8 on the other planned exit: after 50 consecutive accept errors
/// (`worker.accept_error` on every accept) the worker retains the dirty
/// session before it exits 70, logs the outcome in the fatal `WORKER_EXITED`
/// line, and a restart commits the bytes. Without mount support it still
/// exits 70, with nothing to retain.
#[test]
fn accept_error_exit_persists_dirty() {
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    let mount = dir.path().join("mnt");
    let mount_arg = mount.to_string_lossy().into_owned();
    let mounted = mount_supported();
    let mount_args = ["--mount", mount_arg.as_str()];
    let args: &[&str] = if mounted { &mount_args } else { &[] };
    let mut worker = WorkerProc::spawn(
        &state,
        args,
        &[
            ("TKFS_FAULT_POINT", "worker.accept_error"),
            ("TKFS_FAULT_ACTION", "fail"),
        ],
    )
    .unwrap();
    let ready = worker.wait_ready(READY).unwrap();
    assert_eq!(ready["mounted"], mounted, "{ready}");
    let bytes = b"dirty bytes that must survive the accept-error exit".to_vec();
    let file = mounted.then(|| {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(mount.join("dirty.txt"))
            .unwrap();
        file.write_all(&bytes).unwrap();
        file
    });
    // Every accept fails: connect until the worker gives up (50 errors,
    // 100 ms apart).
    let address: SocketAddr = ready["address"]
        .as_str()
        .unwrap()
        .trim_start_matches("ws://")
        .parse()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    let exit = loop {
        if let Some(exit) = worker.try_exit() {
            break exit;
        }
        assert!(
            Instant::now() < deadline,
            "no exit after accept errors\n{}",
            worker.log_tail()
        );
        let _ = TcpStream::connect_timeout(&address, Duration::from_millis(500));
        thread::sleep(Duration::from_millis(50));
    };
    drop(file);
    assert_eq!(exit.code(), Some(70));
    let log = fs::read_to_string(state.join("worker.log")).unwrap();
    let exited = log
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|line| line["code"] == "WORKER_EXITED")
        .unwrap_or_else(|| panic!("no WORKER_EXITED line: {log}"));
    if mounted {
        assert!(
            pending_saves(&state) >= 1 || fuse_may_flush(),
            "the dirty session was not retained: {exited}"
        );
    }
    assert_eq!(exited["level"], "fatal", "{exited}");
    assert_eq!(exited["consecutive"], 50, "{exited}");
    assert_eq!(exited["engine_busy"], false, "{exited}");
    assert_eq!(exited["retain_panicked"], false, "{exited}");
    assert!(exited["error"].is_null(), "{exited}");
    let retained = exited["retained_dirty"]
        .as_u64()
        .unwrap_or_else(|| panic!("no retained_dirty: {exited}"));
    if !mounted {
        assert_eq!(retained, 0, "{exited}");
        return;
    }
    assert!(retained >= 1 || fuse_may_flush(), "{exited}");
    release_mount(&mount);

    let mut restarted = WorkerProc::spawn(&state, &[], &[]).unwrap();
    restarted.wait_ready(READY).unwrap();
    wait_saved(&restarted);
    restarted.stop().unwrap();
    assert_eq!(committed(&state, "dirty.txt"), bytes);
}

// ---------------------------------------------------------------------------
// In-process: stop modes, poison hook, gauges, staged-save transactions

fn shared(root: &Path) -> Shared {
    Arc::new(Mutex::new(Engine::new(
        Store::open(root, Some("worker-lifecycle")).unwrap(),
    )))
}

fn call(info: &runtime::Discovery, payload: Value) -> anyhow::Result<Value> {
    runtime::rpc(info, &id(), payload)
}

/// Waits until only the test holds `engine` (the server threads exited).
fn wait_released(engine: &Shared) {
    wait_for(
        "server threads released the engine",
        Duration::from_secs(10),
        || Arc::strong_count(engine) == 1,
    );
}

/// Invariant 8 guard (§10), in process and without a mount: a graceful stop
/// with an open dirty session is refused (`BUSY_VIEW`, the worker stays up);
/// a force stop fences new opens, writes the `pending_saves` row and stops;
/// after a "crash" the recovered engine commits the bytes.
#[test]
fn force_stop_persists_dirty() {
    let dir = tempfile::tempdir().unwrap();
    let engine = shared(dir.path());
    let info = runtime::start_rpc(engine.clone(), dir.path(), None).unwrap();
    let bytes = b"unsaved in-process bytes".to_vec();
    let entity = {
        let mut e = runtime::lock_engine(&engine);
        let (h, entry) = e.open("open.txt", Some("file"), true).unwrap();
        e.write(h, 0, &bytes, false, false).unwrap();
        entry.id
    };
    let refused = remote_error(call(&info, json!({"op":"stop"})));
    assert_eq!(
        refused,
        "BUSY_VIEW: 1 open contexts, 1 unsaved files; healthy"
    );
    let stopping = runtime::lock_engine(&engine).stopping.clone();
    assert!(!stopping.load(std::sync::atomic::Ordering::SeqCst));
    assert!(call(&info, json!({"op":"status"})).is_ok());

    let stopped = call(&info, json!({"op":"stop","mode":"force"})).unwrap();
    assert_eq!(
        stopped,
        json!({"stopped":true,"persisted_dirty":1,"detached":false})
    );
    assert!(stopping.load(std::sync::atomic::Ordering::SeqCst));
    {
        let e = runtime::lock_engine(&engine);
        assert!(e.switching, "force stop fences new opens");
        assert!(e.sessions[&entity].dirty, "retained, not committed");
        let rows: i64 = e
            .store
            .db
            .query_row("SELECT count(*) FROM pending_saves", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 1);
        assert_eq!(e.vitals.pending_saves(), 1);
    }
    // The listener closes once the stop is answered.
    wait_for("listener closed", Duration::from_secs(10), || {
        call(&info, json!({"op":"hello"})).is_err()
    });
    wait_released(&engine);
    drop(engine);
    let mut recovered = Engine::new(Store::open_existing(dir.path()).unwrap());
    assert_eq!(recovered.sessions[&entity].bytes, &bytes);
    recovered.retry_pending().unwrap();
    recovered.quiet().unwrap();
    let entry = recovered.store.lookup("open.txt").unwrap();
    assert_eq!(
        recovered
            .store
            .read_object(&entry.content.unwrap())
            .unwrap(),
        bytes
    );
}

/// The poison hook never commits what the panicked request left half done:
/// it rolls the open transaction back, or, when that `ROLLBACK` fails
/// (`engine.poison_rollback`), replaces the connection (closing it discards
/// the transaction); then it retains every dirty session. One test, because
/// it is the only user of that process-wide fault point.
#[test]
fn poison_hook_never_commits_the_open_transaction_and_retains_dirty_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::new(Store::open(dir.path(), Some("worker-lifecycle")).unwrap());
    let (h, entry) = engine.open("dirty.txt", Some("file"), true).unwrap();
    engine.write(h, 0, b"dirty", false, false).unwrap();
    // A request that panicked mid-transaction left it open, with savepoints.
    engine
        .store
        .db
        .execute_batch(
            "BEGIN IMMEDIATE; SAVEPOINT control_request; \
             CREATE TABLE wp1_4_probe(x INTEGER); INSERT INTO wp1_4_probe VALUES(1); \
             SAVEPOINT staged_save;",
        )
        .unwrap();
    assert!(!engine.store.db.is_autocommit());
    assert_eq!(engine.persist_after_poison().unwrap(), 1);
    assert!(engine.store.db.is_autocommit());
    // Another connection sees the retained row (committed on its own) and
    // none of the rolled-back request.
    let other = Connection::open(dir.path().join("metadata.sqlite")).unwrap();
    let rows: Vec<String> = other
        .prepare("SELECT entity FROM pending_saves")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(rows, vec![entry.id.clone()]);
    let probe: i64 = other
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE name='wp1_4_probe'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(probe, 0);
    // From autocommit the hook is just `retain_all_dirty`.
    assert_eq!(engine.persist_after_poison().unwrap(), 1);

    // The ROLLBACK fails: the connection is replaced, never committed.
    let (h2, second) = engine.open("second.txt", Some("file"), true).unwrap();
    engine.write(h2, 0, b"second", false, false).unwrap();
    engine
        .store
        .db
        .execute_batch(
            "PRAGMA cache_size=1234; BEGIN IMMEDIATE; SAVEPOINT control_request; \
             CREATE TABLE wp1_4_half_done(x INTEGER); INSERT INTO wp1_4_half_done VALUES(1);",
        )
        .unwrap();
    fault::configure(Some(FaultConfig {
        count: Some(1),
        ..FaultConfig::new("engine.poison_rollback", FaultAction::Fail)
    }));
    let retained = engine.persist_after_poison();
    fault::configure(None);
    assert_eq!(retained.unwrap(), 2);
    let cache_size: i64 = engine
        .store
        .db
        .query_row("PRAGMA cache_size", [], |r| r.get(0))
        .unwrap();
    assert_ne!(cache_size, 1234, "the connection was replaced");
    assert!(engine.store.db.is_autocommit());
    let mut rows: Vec<String> = other
        .prepare("SELECT entity FROM pending_saves")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    rows.sort();
    let mut expected = vec![entry.id.clone(), second.id.clone()];
    expected.sort();
    assert_eq!(rows, expected);
    let half_done: i64 = other
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE name='wp1_4_half_done'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(half_done, 0, "the half-done transaction was discarded");
    // The fresh connection serves the engine: both sessions commit.
    engine.close(h).unwrap();
    engine.close(h2).unwrap();
    engine.retry_pending().unwrap();
    engine.quiet().unwrap();
    assert_eq!(engine.vitals.pending_saves(), 0);
    let entry = engine.store.lookup("second.txt").unwrap();
    assert_eq!(
        engine.store.read_object(&entry.content.unwrap()).unwrap(),
        b"second"
    );
}

#[test]
fn poisoned_engine_panics_under_the_test_policy() {
    let dir = tempfile::tempdir().unwrap();
    let engine = shared(dir.path());
    let poisoner = engine.clone();
    let _ = thread::spawn(move || {
        let _guard = runtime::lock_engine_tracked(&poisoner);
        panic!("poison the engine");
    })
    .join();
    assert!(engine.is_poisoned());
    for attempt in [
        std::panic::catch_unwind(|| drop(runtime::lock_engine(&engine))),
        std::panic::catch_unwind(|| drop(runtime::lock_engine_tracked(&engine))),
        std::panic::catch_unwind(|| drop(runtime::try_lock_engine(&engine))),
        std::panic::catch_unwind(|| runtime::check_engine_poison(&engine)),
    ] {
        let panic = attempt.expect_err("PoisonPolicy::Panic panics");
        let message = panic
            .downcast_ref::<&str>()
            .map(|m| (*m).to_owned())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap();
        assert!(message.starts_with("ENGINE_POISONED"), "{message}");
    }
}

#[test]
fn tracked_engine_lock_feeds_gauges_vitals_and_the_heartbeat() {
    let dir = tempfile::tempdir().unwrap();
    let engine = shared(dir.path());
    let ctx = worker::build_ctx(engine.clone(), dir.path(), false, None);
    let server = runtime::server::start_rpc_server(ctx.clone()).unwrap();
    {
        let mut e = runtime::lock_engine_tracked(&engine);
        let (h, _) = e.open("vital.txt", Some("file"), true).unwrap();
        e.write(h, 0, b"vital", false, false).unwrap();
        thread::sleep(Duration::from_millis(40));
        assert!(ctx.gauges.snapshot().current_hold_ms >= 40);
    }
    let gauges = ctx.gauges.snapshot();
    assert!(gauges.holds >= 1, "{gauges:?}");
    assert!(gauges.longest_hold_ms >= 40, "{gauges:?}");
    assert_eq!(gauges.current_hold_ms, 0, "{gauges:?}");
    assert_eq!(server.vitals().open_handles(), 1);
    assert_eq!(server.vitals().unflushed_files(), 1);
    // A waiter is counted while another thread holds the engine.
    let holder = engine.clone();
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let hold = thread::spawn(move || {
        let _guard = runtime::lock_engine_tracked(&holder);
        held_tx.send(()).unwrap();
        thread::sleep(Duration::from_millis(300));
    });
    held_rx.recv().unwrap();
    let waiter_engine = engine.clone();
    let waiter = thread::spawn(move || drop(runtime::lock_engine(&waiter_engine)));
    wait_for("a counted waiter", Duration::from_secs(5), || {
        ctx.gauges.snapshot().waiters == 1
    });
    assert!(ctx.gauges.snapshot().current_hold_ms > 0);
    hold.join().unwrap();
    waiter.join().unwrap();
    assert!(ctx.gauges.snapshot().wait_p99_ms >= 100);
    // The heartbeat reads all of it without the engine lock.
    let blocker = runtime::lock_engine(&engine);
    let heartbeat = worker::heartbeat::beat(&ctx, server.vitals());
    drop(blocker);
    assert_eq!(heartbeat.instance, ctx.instance);
    assert_eq!(heartbeat.open_handles, 1);
    assert_eq!(heartbeat.unflushed_files, 1);
    assert_eq!(heartbeat.pending_saves, 0);
    assert!(heartbeat.engine.holds >= 2);
    let next = worker::heartbeat::beat(&ctx, server.vitals());
    assert_eq!(next.seq, heartbeat.seq + 1);
    server.stop(tkfs::model::ipc::StopMode::Force).unwrap();
    assert_eq!(
        worker::heartbeat::beat(&ctx, server.vitals()).pending_saves,
        1
    );
}

/// CCR-WP0.INT-5: committing a retained save (`pending_saves` row present)
/// takes SQLite's writer slot before it reads, so another connection holding
/// the slot makes it wait instead of failing with `SQLITE_BUSY`.
#[test]
fn retained_save_commit_waits_for_another_writer_instead_of_failing() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::new(Store::open(dir.path(), Some("worker-lifecycle")).unwrap());
    let (h, _) = engine.open("contended.txt", Some("file"), true).unwrap();
    engine.write(h, 0, b"version one", false, false).unwrap();
    engine.close(h).unwrap();
    let (h, entry) = engine.open("contended.txt", None, true).unwrap();
    engine.write(h, 0, b"version two!", false, false).unwrap();
    assert_eq!(engine.retain_all_dirty().unwrap(), 1);
    let other = Connection::open(dir.path().join("metadata.sqlite")).unwrap();
    other
        .execute_batch("CREATE TABLE wp1_4_writer(x INTEGER NOT NULL)")
        .unwrap();
    other
        .execute_batch("BEGIN IMMEDIATE; INSERT INTO wp1_4_writer VALUES(1)")
        .unwrap();
    let hold = Duration::from_millis(300);
    let writer = thread::spawn(move || {
        thread::sleep(hold);
        other.execute_batch("COMMIT").unwrap();
        other
    });
    let started = Instant::now();
    let flushed = engine.flush(h);
    let waited = started.elapsed();
    let other = writer.join().unwrap();
    flushed.expect("the save waits for the writer slot");
    assert!(
        waited >= hold / 2,
        "the writer slot was contended: {waited:?}"
    );
    assert!(engine.store.db.is_autocommit());
    assert!(engine.health.is_none(), "{:?}", engine.health);
    assert!(!engine.sessions[&entry.id].dirty);
    let rows: i64 = other
        .query_row("SELECT count(*) FROM pending_saves", [], |r| r.get(0))
        .unwrap();
    assert_eq!(rows, 0, "the save and the row removal committed together");
    assert_eq!(engine.vitals.pending_saves(), 0);
    let probe: i64 = other
        .query_row("SELECT count(*) FROM wp1_4_writer", [], |r| r.get(0))
        .unwrap();
    assert_eq!(probe, 1);
    engine.close(h).unwrap();
    let entry = engine.store.lookup("contended.txt").unwrap();
    assert_eq!(
        engine.store.read_object(&entry.content.unwrap()).unwrap(),
        b"version two!"
    );
}

/// A failed commit of a retained save rolls the whole save back (bytes and
/// row removal) and keeps the row.
#[test]
fn failed_retained_save_commit_rolls_back_and_keeps_the_row() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::new(Store::open(dir.path(), Some("worker-lifecycle")).unwrap());
    let (h, entry) = engine.open("refused.txt", Some("file"), true).unwrap();
    engine
        .write(h, 0, b"never committed", false, false)
        .unwrap();
    assert_eq!(engine.retain_all_dirty().unwrap(), 1);
    engine
        .store
        .db
        .execute_batch(
            "CREATE TRIGGER wp1_4_refuse BEFORE DELETE ON pending_saves \
             BEGIN SELECT RAISE(ABORT, 'refused by test'); END;",
        )
        .unwrap();
    let error = engine.flush(h).unwrap_err();
    assert!(
        format!("{error:#}").contains("refused by test"),
        "{error:#}"
    );
    assert!(engine.store.db.is_autocommit());
    assert!(engine.sessions[&entry.id].dirty);
    let committed = engine.store.lookup("refused.txt").unwrap().content;
    assert!(
        committed.is_none_or(|object| engine.store.read_object(&object).unwrap().is_empty()),
        "the write rolled back with the row removal"
    );
    engine
        .store
        .db
        .execute_batch("DROP TRIGGER wp1_4_refuse")
        .unwrap();
    engine.flush(h).unwrap();
    engine.close(h).unwrap();
    engine.quiet().unwrap();
}

/// The `pending_saves` gauge is counted once the save's transaction is
/// resolved. Here the commit fails (a deferred foreign key violation leaves
/// the transaction open, without the row) and re-retaining the session fails
/// too: the gauge still shows the row the rollback kept.
#[test]
fn failed_commit_then_failed_retain_keeps_the_pending_saves_gauge() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::new(Store::open(dir.path(), Some("worker-lifecycle")).unwrap());
    let (h, entry) = engine.open("gauged.txt", Some("file"), true).unwrap();
    engine.write(h, 0, b"retained", false, false).unwrap();
    assert_eq!(engine.retain_all_dirty().unwrap(), 1);
    assert_eq!(engine.vitals.pending_saves(), 1);
    engine
        .store
        .db
        .execute_batch(
            "CREATE TABLE wp1_4_parent(id INTEGER PRIMARY KEY);
             CREATE TABLE wp1_4_child(parent INTEGER REFERENCES wp1_4_parent(id)
                 DEFERRABLE INITIALLY DEFERRED);
             CREATE TRIGGER wp1_4_orphan AFTER DELETE ON pending_saves
                 BEGIN INSERT INTO wp1_4_child VALUES(42); END;
             CREATE TRIGGER wp1_4_refuse_insert BEFORE INSERT ON pending_saves
                 BEGIN SELECT RAISE(ABORT, 'retain refused by test'); END;
             CREATE TRIGGER wp1_4_refuse_update BEFORE UPDATE ON pending_saves
                 BEGIN SELECT RAISE(ABORT, 'retain refused by test'); END;",
        )
        .unwrap();
    let error = format!("{:#}", engine.flush(h).unwrap_err());
    assert!(error.contains("PENDING_SAVE_PERSIST_FAILED"), "{error}");
    assert!(error.contains("retain refused by test"), "{error}");
    assert!(engine.store.db.is_autocommit());
    let rows: i64 = engine
        .store
        .db
        .query_row("SELECT count(*) FROM pending_saves", [], |r| r.get(0))
        .unwrap();
    assert_eq!(rows, 1, "the rollback kept the row");
    assert_eq!(
        engine.vitals.pending_saves(),
        1,
        "counted after the rollback"
    );
    assert!(engine.sessions[&entry.id].dirty);
    engine
        .store
        .db
        .execute_batch(
            "DROP TRIGGER wp1_4_orphan; DROP TRIGGER wp1_4_refuse_insert; \
             DROP TRIGGER wp1_4_refuse_update;",
        )
        .unwrap();
    engine.flush(h).unwrap();
    assert_eq!(engine.vitals.pending_saves(), 0);
    engine.close(h).unwrap();
    engine.quiet().unwrap();
}

/// The main thread's stops (idle exit, signals) never wait for the engine: a
/// held engine refuses them at once with `WORKER_BUSY` (they retry), and the
/// next attempt stops.
#[test]
fn reply_less_stop_refuses_a_busy_engine_instead_of_waiting() {
    let dir = tempfile::tempdir().unwrap();
    let engine = shared(dir.path());
    let ctx = worker::build_ctx(engine.clone(), dir.path(), false, None);
    let server = runtime::server::start_rpc_server(ctx.clone()).unwrap();
    let held = runtime::lock_engine(&engine);
    let started = Instant::now();
    let refused = format!("{:#}", server.stop(StopMode::Graceful).unwrap_err());
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "it did not wait"
    );
    assert!(refused.starts_with("WORKER_BUSY"), "{refused}");
    assert!(!ctx.stopping.load(Ordering::SeqCst));
    drop(held);
    assert_eq!(
        server.stop(StopMode::Graceful).unwrap(),
        StopReply {
            stopped: true,
            persisted_dirty: 0,
            detached: false,
        }
    );
    assert!(ctx.stopping.load(Ordering::SeqCst));
    // A stopping worker attaches nothing.
    let refused = server.attach(&dir.path().join("late-mount")).unwrap_err();
    assert_eq!(refused.code, "WORKER_STOPPING", "{refused}");
    drop(server);
    drop(ctx);
    wait_released(&engine);
}

/// A stop accepted after RPC started (step 6) but before the legacy
/// synchronous attach (step 8, held by `worker.legacy_attach`) wins: nothing
/// is attached, Ready reports `WORKER_STOPPING`, the worker exits 0.
#[test]
fn stop_before_the_legacy_attach_skips_the_mount() {
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    let mount = dir.path().join("mnt");
    let mount_arg = mount.to_string_lossy().into_owned();
    let mut worker = WorkerProc::spawn(
        &state,
        &["--mount", &mount_arg],
        &[
            ("TKFS_FAULT_POINT", "worker.legacy_attach"),
            ("TKFS_FAULT_ACTION", "hang"),
            ("TKFS_FAULT_MS", "3000"),
        ],
    )
    .unwrap();
    wait_for("runtime.json", READY, || record::path(&state).exists());
    let stopped = worker.rpc(json!({"op":"stop"})).unwrap();
    assert_eq!(
        stopped,
        json!({"stopped":true,"persisted_dirty":0,"detached":false})
    );
    let ready = worker.wait_ready(READY).unwrap();
    assert_eq!(ready["mounted"], false, "{ready}");
    assert_eq!(ready["mount_error"]["code"], "WORKER_STOPPING", "{ready}");
    let exit = worker.wait_exit(Duration::from_secs(10)).expect("exit");
    assert_eq!(exit.code(), Some(0));
    assert!(!mount.exists(), "never attached");
    assert!(!state.join("runtime.json").exists());
    assert!(!record::path(&state).exists());
}

/// `runtime.json.mount` is the absolute, normalized mount path even when
/// `--mount` is relative to the worker's working directory (the legacy worker
/// published `absolute(--mount)`), so `bucket-export` resolves it: an export
/// outside the mount works, one inside it is refused.
#[test]
fn relative_mount_argument_is_published_absolute() {
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    // Occupied: the attach fails (reported in Ready) and nothing is mounted.
    let occupied = dir.path().join("occupied");
    fs::create_dir_all(&occupied).unwrap();
    fs::write(occupied.join("keep.txt"), b"user data").unwrap();
    let relative = Path::new(".").join("sub").join("..").join("occupied");
    let mut worker = TimedWorker::spawn(
        dir.path(),
        &state,
        &["--mount", relative.to_str().unwrap()],
        &[],
    );
    let (_, ready) = worker.until_ready(&state).pop().unwrap();
    assert_eq!(ready["mounted"], false, "{ready}");
    let published = PathBuf::from(discovery_at(&state).unwrap().mount.expect("published"));
    assert!(published.is_absolute(), "{}", published.display());
    assert!(
        published
            .components()
            .all(|c| !matches!(c, Component::CurDir | Component::ParentDir)),
        "normalized: {}",
        published.display()
    );
    assert_eq!(
        fs::canonicalize(&published).unwrap(),
        fs::canonicalize(&occupied).unwrap()
    );
    rpc_at(
        &state,
        json!({"op":"import","path":"a.txt","hex":hex::encode(b"a")}),
    )
    .unwrap();
    let exported = rpc_at(
        &state,
        json!({"op":"bucket-export","directory":dir.path().join("bucket")}),
    )
    .unwrap();
    assert_eq!(
        exported["backend"], "local-directory-test-bucket",
        "{exported}"
    );
    let inside = remote_error(rpc_at(
        &state,
        json!({"op":"bucket-export","directory":occupied.join("bucket")}),
    ));
    assert!(
        inside.starts_with("BUCKET_MUST_BE_OUTSIDE_MOUNT"),
        "{inside}"
    );
    rpc_at(&state, json!({"op":"stop"})).unwrap();
    let exit = worker.wait_exit(Duration::from_secs(10)).expect("exit");
    assert_eq!(exit.code(), Some(0));
    assert_eq!(fs::read(occupied.join("keep.txt")).unwrap(), b"user data");
}

/// Invariant 28 guard (§10): long recovery work reports startup progress from
/// the recovering thread. Pending-save recovery is slowed down by
/// `engine.recover_pending` (600 ms per save): a `recovering pending saves`
/// line arrives while recovery still runs (well before Ready), every line
/// before Ready is startup progress with increasing sequence numbers, and
/// nothing prints progress after Ready (the heartbeat is not progress).
#[test]
fn backfill_reports_progress() {
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    let saves = 4;
    {
        let mut engine = Engine::new(Store::open_existing(&state).unwrap());
        for index in 0..saves {
            let (h, _) = engine
                .open(&format!("backfill-{index}.txt"), Some("file"), true)
                .unwrap();
            engine
                .write(h, 0, format!("bytes {index}").as_bytes(), false, false)
                .unwrap();
        }
        assert_eq!(engine.retain_all_dirty().unwrap(), saves);
    }
    let mut worker = TimedWorker::spawn(
        dir.path(),
        &state,
        &[],
        &[
            ("TKFS_FAULT_POINT", "engine.recover_pending"),
            ("TKFS_FAULT_ACTION", "hang"),
            ("TKFS_FAULT_MS", "600"),
        ],
    );
    let mut lines = worker.until_ready(&state);
    let (ready_at, _) = lines.pop().unwrap();
    let mut sequence = 0;
    for (_, line) in &lines {
        let next = line["startup_progress"]
            .as_u64()
            .unwrap_or_else(|| panic!("not startup progress: {line}"));
        assert!(next > sequence, "{lines:?}");
        sequence = next;
    }
    let (recovering_at, recovering) = lines
        .iter()
        .find(|(_, line)| {
            line["phase"] == "recovering pending saves"
                && line["startup_progress"].as_u64() >= Some(2)
        })
        .unwrap_or_else(|| panic!("no progress during recovery: {lines:?}"));
    assert!(
        ready_at.duration_since(*recovering_at) >= Duration::from_millis(500),
        "{recovering} arrived {:?} before Ready",
        ready_at.duration_since(*recovering_at)
    );
    thread::sleep(Duration::from_millis(1500));
    let after: Vec<_> = worker.lines.try_iter().map(|(_, line)| line).collect();
    assert!(
        after
            .iter()
            .all(|line| line.get("startup_progress").is_none()),
        "progress after Ready: {after:?}"
    );
    wait_for("pending saves committed", Duration::from_secs(20), || {
        rpc_at(&state, json!({"op":"status"}))
            .is_ok_and(|status| status["unflushed_files"] == 0 && status["health"].is_null())
    });
    rpc_at(&state, json!({"op":"stop"})).unwrap();
    let exit = worker.wait_exit(Duration::from_secs(10)).expect("exit");
    assert_eq!(exit.code(), Some(0));
    assert_eq!(pending_saves(&state), 0);
    for index in 0..saves {
        assert_eq!(
            committed(&state, &format!("backfill-{index}.txt")),
            format!("bytes {index}").as_bytes()
        );
    }
}
