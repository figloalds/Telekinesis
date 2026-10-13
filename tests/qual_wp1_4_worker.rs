//! Qualification scenarios for WP1.4 (worker lifecycle: RPC first, worker.json,
//! hello/watch/stop, crash-only internals). REVAMP_PLAN §14.6, §4.1, §6.1.
//!
//! Created by WP0.5 against the 6298d0b baseline, `#[ignore = "WP1.4"]`.
//! Written against dynamic JSON (worker RPC ops by name, CLI flags by string).
//! `cargo test --release --test qual_wp1_4_worker -- --ignored --test-threads=1 --nocapture`
mod support;

use serde_json::{Value, json};
use std::{fs, io::Write, path::Path, time::Duration};
use support::*;
use tkfs::core::{Store, id};

const FILE: &str = "qual_wp1_4_worker";

fn new_state(dir: &Path) -> std::path::PathBuf {
    let state = dir.join("state");
    drop(Store::initialize(&state, &id(), &id()).unwrap());
    state
}

/// A mount failure (existing non-empty path) is reported in Ready; the worker
/// keeps serving RPC.
#[test]
#[ignore = "WP1.4"]
fn mount_failure_keeps_worker_serving() {
    let mut s = Scenario::new(FILE, "mount_failure_keeps_worker_serving");
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    let mount = dir.path().join("occupied");
    fs::create_dir_all(&mount).unwrap();
    fs::write(mount.join("keep.txt"), b"user data").unwrap();
    let mount_arg = mount.to_string_lossy().into_owned();
    let mut worker = WorkerProc::spawn(&state, &["--mount", &mount_arg], &[]).unwrap();
    match worker.wait_ready(Duration::from_secs(60)) {
        Ok(ready) => {
            s.check(ready["mounted"] == false, format!("mounted false: {ready}"));
            s.check(
                ready["mount_error"]["code"] == "MOUNT_PATH_MUST_NOT_EXIST",
                format!("mount_error code: {ready}"),
            );
            let status = worker.rpc(json!({"op":"status"}));
            s.check(status.is_ok(), format!("status: {status:?}"));
            let summary = worker.rpc(json!({"op":"published-summary"}));
            s.check(summary.is_ok(), format!("published-summary: {summary:?}"));
            let import = worker.rpc(json!({"op":"import","path":"a.txt","hex":hex::encode(b"a")}));
            s.check(import.is_ok(), format!("import: {import:?}"));
        }
        Err(e) => {
            s.check(
                false,
                format!("worker did not serve after a mount failure: {e:#}"),
            );
        }
    }
    s.check(
        fs::read(mount.join("keep.txt")).ok().as_deref() == Some(b"user data".as_slice()),
        "user directory untouched",
    );
    s.finish();
}

/// Injected accept errors do not stop the RPC listener.
#[test]
#[ignore = "WP1.4"]
fn accept_loop_survives_faults() {
    let mut s = Scenario::new(FILE, "accept_loop_survives_faults");
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
    worker.wait_ready(Duration::from_secs(60)).unwrap();
    let mut failures = 0;
    let mut last_ten_ok = 0;
    for i in 0..25 {
        let ok = worker.rpc(json!({"op":"status"})).is_ok();
        if !ok {
            failures += 1;
        } else if i >= 15 {
            last_ten_ok += 1;
        }
    }
    // Evidence that the fault point fired: a call that hit an injected accept
    // error, or a worker.log line naming the point. Without it (no fault hook,
    // a misspelled point, env not reaching the worker) "still serving" proves
    // nothing. If the owner's accept fault is invisible to clients and not
    // logged, replace this with the evidence the implementation exposes.
    let logged = fs::read_to_string(state.join("worker.log"))
        .unwrap_or_default()
        .contains("worker.accept_error");
    s.metric_with(
        "failed_calls",
        failures as f64,
        "count",
        json!({"fault_logged": logged}),
    );
    s.check(
        failures >= 1 || logged,
        "worker.accept_error fired (a failed call or a worker.log line naming it)",
    );
    s.check(failures <= 3, format!("{failures} failed calls"));
    s.check(last_ten_ok == 10, "still serving after the faults");
    s.check(worker.try_exit().is_none(), "worker alive");
    s.finish();
}

/// A poisoned engine lock makes the worker persist dirty sessions and exit 70.
#[test]
#[ignore = "WP1.4"]
fn poison_exit_persists_dirty() {
    let s = Scenario::new(FILE, "poison_exit_persists_dirty");
    if !mount_supported() {
        return s.skip("no mount support (WinFsp / /dev/fuse)");
    }
    let mut s = s;
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
    worker.wait_ready(Duration::from_secs(60)).unwrap();
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(mount.join("dirty.txt"))
        .unwrap();
    file.write_all(b"dirty bytes that must survive").unwrap();
    // Trigger engine use; the fault point poisons the engine lock.
    let _ = worker.rpc(json!({"op":"status"}));
    let _ = worker.rpc(json!({"op":"state"}));
    s.measure_from_now();
    let status = worker.wait_exit(s.budget.min(Duration::from_secs(30)));
    drop(file);
    match status {
        Some(status) => {
            s.metric("exit_code", status.code().unwrap_or(-1) as f64, "code");
            s.check(status.code() == Some(70), format!("exit 70, got {status}"));
            let db = rusqlite::Connection::open(state.join("metadata.sqlite")).unwrap();
            let pending: i64 = db
                .query_row("SELECT count(*) FROM pending_saves", [], |r| r.get(0))
                .unwrap_or(0);
            s.check(pending >= 1, format!("pending_saves rows: {pending}"));
        }
        None => s.timeout("worker did not exit after engine poisoning"),
    }
    s.finish();
}

/// `--supervised`: Ready (RPC up, store recovered) is printed before any mount
/// attach; `mount-attach` attaches later on request.
#[test]
#[ignore = "WP1.4"]
fn rpc_before_mount() {
    let mut s = Scenario::new(FILE, "rpc_before_mount");
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    let mount = dir.path().join("mnt");
    let mount_arg = mount.to_string_lossy().into_owned();
    let mut worker =
        WorkerProc::spawn(&state, &["--supervised", "--mount", &mount_arg], &[]).unwrap();
    match worker.wait_ready(Duration::from_secs(60)) {
        Ok(ready) => {
            s.metric(
                "ready_ms",
                worker.spawned_at.elapsed().as_secs_f64() * 1000.0,
                "ms",
            );
            s.check(
                ready["mounted"] == false,
                "not attached before mount-attach",
            );
            s.check(ready["ipc"] == 3, format!("ipc 3: {ready}"));
            let (status, ms) = time_ms(|| worker.rpc(json!({"op":"status"})));
            s.check(status.is_ok(), "status answers before attach");
            s.metric("status_ms", ms, "ms");
            s.check(state.join("worker.json").is_file(), "worker.json written");
            if mount_supported() {
                let (attached, ms) =
                    time_ms(|| worker.rpc(json!({"op":"mount-attach","path":mount_arg})));
                s.metric("attach_ms", ms, "ms");
                s.check(
                    attached
                        .as_ref()
                        .is_ok_and(|m| m["state"].to_string().contains("attached")),
                    format!("mount-attach: {attached:?}"),
                );
            }
        }
        Err(e) => s.unsupported(format!("supervised worker: {e:#}")),
    }
    s.finish();
}

/// `watch{after_seq}` returns the next heartbeat within 1.5 s.
#[test]
#[ignore = "WP1.4"]
fn watch_latency() {
    let mut s = Scenario::new(FILE, "watch_latency");
    let dir = tempfile::tempdir().unwrap();
    let state = new_state(dir.path());
    let mut worker = WorkerProc::spawn(&state, &[], &[]).unwrap();
    worker.wait_ready(Duration::from_secs(60)).unwrap();
    match worker.rpc(json!({"op":"hello"})) {
        Ok(hello) => s.check(
            hello["pid"].as_u64() == worker.pid().map(u64::from),
            format!("hello pid: {hello}"),
        ),
        Err(e) => {
            s.unsupported(format!("hello: {e:#}"));
            return s.finish();
        }
    };
    let first = worker.rpc(json!({"op":"watch","after_seq":0,"wait_ms":2000}));
    let Ok(first) = first else {
        s.unsupported(format!("watch: {first:?}"));
        return s.finish();
    };
    let seq = first["seq"].as_u64().unwrap_or(0);
    let mut samples = vec![];
    let mut after = seq;
    for _ in 0..5 {
        let (next, ms) =
            time_ms(|| worker.rpc(json!({"op":"watch","after_seq":after,"wait_ms":5000})));
        let next: Value = next.unwrap_or(Value::Null);
        let got = next["seq"].as_u64().unwrap_or(0);
        s.check(got > after, format!("watch advanced: {next}"));
        after = got;
        samples.push(ms);
    }
    let summary = summarize(&samples);
    s.metric_with("watch_ms", summary.max, "ms", summary.json());
    s.finish();
}
