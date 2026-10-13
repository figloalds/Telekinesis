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
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};
use support::*;
use tkfs::{
    core::{Store, id},
    model::ipc::{HelloReply, ReadyLine, WorkerHeartbeat, WorkerRecord},
    runtime::{self, Engine, Shared},
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
    assert_eq!(
        remote_error(worker.rpc(json!({"op":"revalidate"}))),
        "UNSUPPORTED_OPERATION: revalidate"
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

/// Invariant 24: store recovery (including pending saves) completes before
/// the listener exists. `worker.startup` holds the worker between recovery
/// and the listener: nothing is published, and only after Ready does the
/// recovered save commit.
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
            ("TKFS_FAULT_MS", "1500"),
        ],
    )
    .unwrap();
    let started = Instant::now();
    while started.elapsed() < Duration::from_millis(1000) {
        assert!(
            !state.join("runtime.json").exists(),
            "listener before recovery"
        );
        assert!(
            !record::path(&state).exists(),
            "worker.json before recovery"
        );
        assert!(worker.try_exit().is_none());
        thread::sleep(Duration::from_millis(50));
    }
    let ready = worker.wait_ready(READY).unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(1400),
        "the fault held startup"
    );
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

/// `engine.poison` (panic under the engine lock): the worker persists the
/// dirty session and exits 70; a restart commits the bytes.
#[test]
fn engine_poison_exits_70_and_persists_dirty_sessions() {
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

#[test]
fn graceful_stop_refuses_an_open_session_and_force_stop_retains_it() {
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

#[test]
fn poison_hook_rolls_back_the_open_transaction_then_retains_dirty_sessions() {
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
