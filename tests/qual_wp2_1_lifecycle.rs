//! Qualification scenarios for WP2.1 (actor orchestrator wiring, legacy bridge,
//! Shutdown). REVAMP_PLAN §14.6, §3.3, §3.6, §7.3.
//!
//! Created by WP0.5 against the 6298d0b baseline; enabled by WP2.1. They stay
//! `#[ignore]` because `scripts/qualify.ps1` runs the release qualification
//! with `--ignored` (the `api_p99_under_churn`, `shutdown_busy_folder` and
//! `subscribe_latency` floors are wave 2 floors); the same behaviours run in
//! the default suite as `tests/orchestrator_runtime.rs`.
//! Written against dynamic action JSON (`raw_call`), so the new `status` member
//! (§7.3) and new actions are read by name.
//! `cargo test --release --test qual_wp2_1_lifecycle -- --ignored --test-threads=1 --nocapture`
//!
//! WP2.1 changes to the WP0.5 drafts (see `docs/revamp/notes/wp2-1.md`):
//! * a crash loop is `TKFS_FAULT_POINT=worker.startup` (exit), scoped to one
//!   folder (§14 WP2.1 acceptance). Deleting a folder's store, the baseline's
//!   crasher, is a Fatal `STATE_NOT_INITIALIZED` now: `Failed` at once, no loop;
//! * a hung worker is a frozen process (SIGSTOP / `NtSuspendProcess`): with
//!   `worker.watch_hang` alone the worker still answers `hello`, which §3.3
//!   treats as alive (`watch_hang_alone_is_not_unresponsive`).
mod support;

use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use support::*;

const FILE: &str = "qual_wp2_1_lifecycle";

/// The daemon environment that makes `folder` crash-loop: its worker exits
/// right after recovery (`worker.startup`, exit 86), every start.
fn crash_env(folder: &Value) -> Vec<(String, String)> {
    vec![
        ("TKFS_FAULT_POINT".into(), "worker.startup".into()),
        ("TKFS_FAULT_ACTION".into(), "exit".into()),
        ("TKFS_FAULT_SCOPE".into(), state_id(folder)),
    ]
}

/// Lifecycle phase from the §7.3 `status` member (None before WP2.1).
fn phase(inspect: &Value) -> Option<String> {
    inspect["status"]["lifecycle"]["phase"]
        .as_str()
        .map(str::to_owned)
}

/// The `pid` of `STATE/worker.json`.
fn worker_pid(folder: &Value) -> Option<u32> {
    let bytes = fs::read(state_dir(folder).join("worker.json")).ok()?;
    let record: Value = serde_json::from_slice(&bytes).ok()?;
    record["pid"].as_u64().map(|pid| pid as u32)
}

/// `LOCK_HOLD_EXCEEDED` lines in the daemon log (debug builds log every
/// `Tracked` hold over 50 ms; release builds log none).
fn lock_violations(node: &Node) -> usize {
    fs::read_to_string(node.log_path())
        .unwrap_or_default()
        .lines()
        .filter(|line| line.contains("LOCK_HOLD_EXCEEDED"))
        .count()
}

/// Freezes a process (every thread stops; it holds its sockets and locks).
#[cfg(windows)]
fn freeze(pid: u32) -> bool {
    type Handle = *mut core::ffi::c_void;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn OpenProcess(access: u32, inherit: i32, pid: u32) -> Handle;
        fn CloseHandle(handle: Handle) -> i32;
    }
    #[link(name = "ntdll")]
    unsafe extern "system" {
        fn NtSuspendProcess(handle: Handle) -> i32;
    }
    const PROCESS_SUSPEND_RESUME: u32 = 0x0800;
    // SAFETY: a handle opened, used and closed here.
    unsafe {
        let handle = OpenProcess(PROCESS_SUSPEND_RESUME, 0, pid);
        if handle.is_null() {
            return false;
        }
        let suspended = NtSuspendProcess(handle) >= 0;
        CloseHandle(handle);
        suspended
    }
}

#[cfg(unix)]
fn freeze(pid: u32) -> bool {
    // SAFETY: a signal to a worker this scenario started.
    unsafe { libc::kill(pid as i32, libc::SIGSTOP) == 0 }
}

/// `List` p99 < 100 ms over 200 calls while one folder crash-loops, one is
/// churned by Start/Stop and (with mounts) one is StopBlocked by an open file.
#[test]
#[ignore = "WP2.1"]
fn api_latency_under_churn() {
    let mut s = Scenario::new(FILE, "api_latency_under_churn");
    let mut a = Node::new();
    let crasher = a.create("Crash loop", true);
    let churn = a.create("Churn", true);
    let mut _open = None;
    let mut busy = None;
    if mount_supported() {
        let mount = a.path().join("busy-mount");
        let folder = a.create_mounted("Busy", &mount);
        let mut f = fs::File::create(mount.join("open.txt")).unwrap();
        f.write_all(b"held open").unwrap();
        _open = Some(f);
        busy = Some(folder);
    }
    // The crasher's worker exits at every start from now on; the busy worker
    // is left running by the restart (open file) and adopted.
    a.restart_with_env(crash_env(&crasher));
    if let Some(busy) = &busy {
        let state = state_id(busy);
        a.wait_for("busy adopted", Duration::from_secs(60), || {
            a.try_inspect(&state).is_ok_and(|p| p["running"] == true)
        });
        let _ = a.raw_call(json!({"op":"stop","state":state,"wait":false}));
        a.wait_for("busy stop blocked", Duration::from_secs(30), || {
            a.try_inspect(&state)
                .is_ok_and(|p| phase(&p).as_deref() == Some("stop_blocked"))
        });
    }
    let stop = Arc::new(AtomicBool::new(false));
    let counts = Arc::new(CallCounts::default());
    let churner = {
        let (stop, counts, control) = (stop.clone(), counts.clone(), a.control());
        let churn_state = state_id(&churn);
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                for op in ["stop", "start"] {
                    counts.record(&control.call(&json!({"op":op,"state":churn_state})));
                }
            }
        })
    };
    s.measure_from_now();
    let mut samples = vec![];
    for _ in 0..200 {
        if s.over_budget() {
            s.timeout(format!("{} of 200 List calls in budget", samples.len()));
            break;
        }
        let (r, ms) = time_ms(|| a.raw_call(json!({"op":"list"})));
        s.check(r.is_ok(), "list ok");
        samples.push(ms);
        thread::sleep(Duration::from_millis(20));
    }
    stop.store(true, Ordering::Relaxed);
    churner.join().unwrap();
    // A churner whose calls never reach the daemon (for example rejected
    // upgrades) would let the floor pass without any churn.
    let (ok, _, _) = counts.get();
    s.check(ok >= 2, format!("churner made progress: {}", counts.json()));
    let crasher_phase = a
        .try_inspect(&state_id(&crasher))
        .ok()
        .and_then(|p| phase(&p));
    s.check(
        matches!(
            crasher_phase.as_deref(),
            Some("backoff" | "starting" | "failed")
        ),
        format!("the crasher crash-loops: {crasher_phase:?}"),
    );
    let violations = lock_violations(&a);
    s.check(
        violations == 0,
        format!("{violations} orchestrator lock holds over 50 ms (debug builds log them)"),
    );
    let summary = summarize(&samples);
    let mut extra = summary.json();
    extra["churn"] = counts.json();
    extra["lock_violations_logged"] = json!(violations);
    s.metric_with("list_p99_ms", summary.p99, "ms", extra);
    if let Some(busy) = &busy {
        // Let the blocked stop go: its file closes with the scenario.
        let _ = a.raw_call(json!({"op":"start","state":state_id(busy)}));
    }
    s.finish();
}

/// A crash-looping folder reaches `Failed{CRASH_LOOP}` in its status member.
#[test]
#[ignore = "WP2.1"]
fn crash_loop_reaches_failed() {
    let mut s = Scenario::new(FILE, "crash_loop_reaches_failed");
    let mut a = Node::new();
    let crasher = a.create("Crash loop", true);
    let healthy = a.create("Healthy", true);
    a.restart_with_env(crash_env(&crasher));
    s.measure_from_now();
    let state = state_id(&crasher);
    let watch = Stopwatch::start();
    let failed = wait_until(s.remaining(), || {
        a.try_inspect(&state).is_ok_and(|p| {
            phase(&p).as_deref() == Some("failed") && p["status"].to_string().contains("CRASH_LOOP")
        })
    });
    s.metric("time_to_failed_ms", watch.ms(), "ms");
    if !failed {
        let last = a.try_inspect(&state).unwrap_or(Value::Null);
        s.timeout(format!(
            "no Failed{{CRASH_LOOP}}: status {}",
            last["status"]
        ));
    }
    s.check(
        a.try_inspect(&state_id(&healthy))
            .is_ok_and(|p| p["running"] == true),
        "the other folder keeps running",
    );
    s.finish();
}

/// Shutdown with a busy folder returns within 30 s with `left_running`, exit 0,
/// `desired_running` unchanged; the next start adopts the same worker.
#[test]
#[ignore = "WP2.1"]
fn shutdown_with_busy_folder() {
    let s = Scenario::new(FILE, "shutdown_with_busy_folder");
    if !mount_supported() {
        return s.skip("no mount support (a busy folder needs an open file)");
    }
    let mut s = s;
    let mut a = Node::new();
    let mount = a.path().join("busy");
    let busy = a.create_mounted("Busy", &mount);
    let mut file = fs::File::create(mount.join("open.txt")).unwrap();
    file.write_all(b"dirty and open").unwrap();
    let pids = a.worker_pids(&busy);
    let address = worker_address(&busy);
    s.measure_from_now();
    let watch = Stopwatch::start();
    let (reply, status) = a.shutdown_within(Duration::from_secs(30));
    s.metric_with(
        "shutdown_ms",
        if status.is_some() {
            watch.ms()
        } else {
            f64::NAN
        },
        "ms",
        json!({"reply": format!("{reply:?}")}),
    );
    s.check(
        status.is_some_and(|st| st.success()),
        format!("daemon exits 0: {status:?}"),
    );
    let left = reply.as_ref().is_ok_and(|r| {
        r["folders"].as_array().is_some_and(|f| {
            f.iter()
                .any(|o| o["state"] == busy["state"] && o["outcome"] == "left_running")
        })
    });
    s.check(left, format!("outcome left_running: {reply:?}"));
    s.check(
        !a.data_dir().join("orchestrator.json").exists(),
        "orchestrator.json removed",
    );
    if status.is_some() {
        a.spawn();
        let state = state_id(&busy);
        let inspect = a.inspect(&state);
        s.check(
            inspect["desired_running"] == true,
            "desired_running unchanged",
        );
        let adopted = wait_until(Duration::from_secs(30), || {
            a.try_inspect(&state).is_ok_and(|p| p["running"] == true)
        }) && worker_address(&busy) == address
            && a.worker_pids(&busy) == pids;
        s.check(adopted, "the busy worker is adopted (same process)");
    }
    drop(file);
    s.finish();
}

/// Killing the daemon leaves workers running; the restart adopts them and sync resumes.
#[test]
#[ignore = "WP2.1"]
fn adoption_after_daemon_kill() {
    let mut s = Scenario::new(FILE, "adoption_after_daemon_kill");
    let a = Node::new();
    let mut b = Node::new();
    let pa = a.create("Adoption source", true);
    write_file(&pa, "before.txt", b"before");
    b.pair(&a, &a.code());
    let pb = b.receive(&a, &pa);
    b.wait_for("before", Duration::from_secs(60), || {
        has_file(&pb, "before.txt", b"before")
    });
    let address = worker_address(&pb);
    let pids = b.worker_pids(&pb);
    b.kill();
    write_file(&pa, "while-down.txt", b"while the daemon was down");
    s.measure_from_now();
    b.spawn();
    let state = state_id(&pb);
    b.wait_for("running after restart", Duration::from_secs(60), || {
        b.try_inspect(&state).is_ok_and(|p| p["running"] == true)
    });
    let adopted = worker_address(&pb) == address && b.worker_pids(&pb) == pids;
    s.metric("adopted", if adopted { 1.0 } else { 0.0 }, "bool");
    s.check(adopted, "running worker adopted, not restarted");
    s.check(
        b.try_inspect(&state)
            .is_ok_and(|p| p["status"]["lifecycle"]["adopted"] == true),
        "status says adopted",
    );
    let watch = Stopwatch::start();
    let resumed = wait_until(s.remaining(), || {
        has_file(&pb, "while-down.txt", b"while the daemon was down")
    });
    s.metric("resume_ms", watch.ms(), "ms");
    s.check(resumed, "sync resumes");
    s.finish();
}

/// A worker that stops answering (its process frozen: `watch` and `hello`
/// both stall) is detected (Unresponsive), killed and restarted; the other
/// folder's worker is unaffected.
#[test]
#[ignore = "WP2.1"]
fn hung_worker_restarted() {
    let mut s = Scenario::new(FILE, "hung_worker_restarted");
    let a = Node::new();
    let hung = a.create("Hung watch", true);
    let healthy = a.create("Healthy", true);
    let hung_address = worker_address(&hung);
    let healthy_address = worker_address(&healthy);
    let pid = worker_pid(&hung).unwrap();
    s.check(freeze(pid), "worker frozen");
    s.measure_from_now();
    let watch = Stopwatch::start();
    let restarted = wait_until(s.remaining(), || {
        let now = worker_address(&hung);
        now.is_some() && now != hung_address && worker_pid(&hung) != Some(pid)
    });
    s.metric("restart_ms", watch.ms(), "ms");
    if !restarted {
        s.timeout("hung worker never restarted");
    }
    let state = state_id(&hung);
    let recovered = wait_until(Duration::from_secs(30).min(s.remaining()), || {
        a.try_inspect(&state).is_ok_and(|p| p["running"] == true)
    });
    s.check(recovered, "the folder runs again");
    s.check(
        fs::read_to_string(a.log_path())
            .unwrap_or_default()
            .contains("WORKER_UNRESPONSIVE"),
        "WORKER_UNRESPONSIVE logged",
    );
    s.check(
        worker_address(&healthy) == healthy_address,
        "healthy folder's worker untouched",
    );
    s.finish();
}

/// `worker.watch_hang` alone: the worker still answers `hello`, which §3.3
/// treats as alive, so it is never killed (a worker that answers may hold
/// dirty sessions, invariant 8); the other folder is unaffected.
#[test]
#[ignore = "WP2.1"]
fn watch_hang_alone_is_not_unresponsive() {
    let mut s = Scenario::new(FILE, "watch_hang_alone_is_not_unresponsive");
    let mut a = Node::new();
    let hung = a.create("Hung watch", true);
    let healthy = a.create("Healthy", true);
    a.restart_with_env(vec![
        ("TKFS_FAULT_POINT".into(), "worker.watch_hang".into()),
        ("TKFS_FAULT_ACTION".into(), "hang".into()),
        ("TKFS_FAULT_SCOPE".into(), state_id(&hung)),
    ]);
    for folder in [&hung, &healthy] {
        let state = state_id(folder);
        a.wait_for("running", Duration::from_secs(60), || {
            a.try_inspect(&state).is_ok_and(|p| p["running"] == true)
        });
    }
    let pid = worker_pid(&hung);
    s.measure_from_now();
    // Three watch failures (4 s each) and the heartbeat timeout (8 s) pass
    // more than once in 25 s; each ends with a hello probe that answers.
    let killed = wait_until(Duration::from_secs(25), || worker_pid(&hung) != pid);
    s.check(!killed, "a worker answering hello is not killed");
    s.check(
        a.try_inspect(&state_id(&hung))
            .is_ok_and(|p| p["running"] == true),
        "still running",
    );
    s.check(
        a.try_inspect(&state_id(&healthy))
            .is_ok_and(|p| p["running"] == true),
        "the other folder runs",
    );
    s.finish();
}

/// A non-empty mount path: the folder runs headless with
/// `mount.state = attach_failed` and still syncs.
#[test]
#[ignore = "WP2.1"]
fn mount_failure_end_to_end() {
    let mut s = Scenario::new(FILE, "mount_failure_end_to_end");
    let a = Node::new();
    let mount = a.path().join("occupied");
    let created =
        a.call(json!({"op":"create","label":"Mount failure","mount":mount,"headless":false}));
    let state = state_id(&created);
    let _ = a.raw_call(json!({"op":"stop","state":state}));
    a.wait_for("stopped", Duration::from_secs(30), || {
        a.try_inspect(&state)
            .is_ok_and(|p| p["running"] == false && p["starting"] == false)
    });
    fs::create_dir_all(&mount).unwrap();
    fs::write(mount.join("user.txt"), b"user data").unwrap();
    let _ = a.raw_call(json!({"op":"start","state":state}));
    s.measure_from_now();
    let running = wait_until(s.remaining(), || {
        a.try_inspect(&state).is_ok_and(|p| p["running"] == true)
    });
    if !running {
        s.timeout("folder never ran headless after the mount failure");
        return s.finish();
    }
    // The status member follows the hub, which may still hold the heartbeat
    // taken before the attach result for a moment (CCR-WP2.1-7).
    let attach_failed = wait_until(Duration::from_secs(10), || {
        a.try_inspect(&state).is_ok_and(|p| {
            p["status"]["mount"]["state"]
                .to_string()
                .contains("attach_failed")
        })
    });
    let inspect = a.inspect(&state);
    s.check(
        attach_failed,
        format!("status.mount.state: {}", inspect["status"]["mount"]),
    );
    s.check(inspect["mounted"] == false, "not mounted");
    let import = try_rpc(&inspect, json!({"op":"import","path":"a.txt","hex":"61"}));
    s.check(import.is_ok(), format!("sync/RPC works: {import:?}"));
    s.check(
        fs::read(mount.join("user.txt")).ok().as_deref() == Some(b"user data".as_slice()),
        "user directory untouched",
    );
    s.finish();
}

/// SIGTERM produces the same outcome as the Shutdown action (Unix): exit 0,
/// every worker stopped, `orchestrator.json` removed, `desired_running` kept.
#[test]
#[ignore = "WP2.1"]
fn sigterm_matches_shutdown() {
    let s = Scenario::new(FILE, "sigterm_matches_shutdown");
    #[cfg(not(unix))]
    {
        s.skip("SIGTERM is Unix-only (run in WSL)");
    }
    #[cfg(unix)]
    {
        let mut s = s;
        let mut a = Node::new();
        let folders = [a.create("Signal one", true), a.create("Signal two", true)];
        let workers: Vec<u32> = folders.iter().filter_map(worker_pid).collect();
        let pid = a.pid().unwrap();
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }
        let status = a.wait_exit(Duration::from_secs(30));
        s.check(
            status.is_some_and(|st| st.success()),
            format!("exit 0 after SIGTERM: {status:?}"),
        );
        s.check(
            !a.data_dir().join("orchestrator.json").exists(),
            "orchestrator.json removed",
        );
        let alive = |pid: u32| unsafe { libc::kill(pid as i32, 0) == 0 };
        s.check(
            wait_until(Duration::from_secs(10), || {
                !workers.iter().any(|w| alive(*w))
            }),
            "SIGTERM stopped every worker",
        );
        a.spawn();
        for folder in &folders {
            let state = state_id(folder);
            s.check(
                a.inspect(&state)["desired_running"] == true,
                "desired_running unchanged",
            );
            a.wait_for("running", Duration::from_secs(60), || {
                a.try_inspect(&state).is_ok_and(|p| p["running"] == true)
            });
        }
        // The Shutdown action on the same folders: the same outcome.
        let reply = a.stop();
        let stopped = reply["folders"].as_array().is_some_and(|outcomes| {
            outcomes.len() == folders.len() && outcomes.iter().all(|o| o["outcome"] == "stopped")
        });
        s.check(stopped, format!("Shutdown stops every worker too: {reply}"));
        s.finish();
    }
}

/// Whether a status snapshot (§7.1) lists a folder labelled `label`.
fn lists_folder(snapshot: &Value, label: &str) -> bool {
    snapshot["folders"]
        .as_array()
        .is_some_and(|folders| folders.iter().any(|f| f["label"] == label))
}

/// `subscribe{since}` returns within 1 s of a lifecycle change.
///
/// Only a reply that reflects the Create counts (its snapshot lists the new
/// folder, which cannot exist before the Create is sent). A reply caused by an
/// unrelated generation bump is followed by a new `subscribe` from its
/// generation, so it can neither end the measurement early nor record ~0 ms.
#[test]
#[ignore = "WP2.1"]
fn subscribe_latency() {
    const LABEL: &str = "Subscribe";
    let mut s = Scenario::new(FILE, "subscribe_latency");
    let a = Node::new();
    let status = a.raw_call(json!({"op":"status"}));
    let Ok(status) = status else {
        s.unsupported(format!("status action: {status:?}"));
        return s.finish();
    };
    s.check(
        status["generation"].is_u64(),
        format!("status has a generation: {status}"),
    );
    s.check(
        !lists_folder(&status, LABEL),
        "no Subscribe folder before the Create",
    );
    let generation = status["generation"].as_u64().unwrap_or(0);
    let control = a.control();
    let waiter = thread::spawn(move || -> anyhow::Result<(Value, Instant, u32)> {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut since = generation;
        let mut unrelated = 0;
        loop {
            let asked = Instant::now();
            let reply = control.call(&json!({"op":"subscribe","since":since,"wait_ms":10000}))?;
            let at = Instant::now();
            if lists_folder(&reply, LABEL) {
                return Ok((reply, at, unrelated));
            }
            unrelated += 1;
            if at >= deadline {
                anyhow::bail!(
                    "SUBSCRIBE_NEVER_SHOWED_CREATE after {unrelated} replies, last: {reply}"
                );
            }
            let next = reply["generation"].as_u64().unwrap_or(since);
            if next <= since && at.duration_since(asked) < Duration::from_millis(50) {
                // Neither a newer generation nor a long poll: do not spin.
                thread::sleep(Duration::from_millis(20));
            }
            since = since.max(next);
        }
    });
    thread::sleep(Duration::from_millis(300));
    let changed = Instant::now();
    let created = a.raw_call(json!({"op":"create","label":LABEL,"mount":null,"headless":true}));
    s.check(created.is_ok(), format!("create: {created:?}"));
    match waiter.join().unwrap() {
        Ok((reply, at, unrelated)) => {
            let ms = at.saturating_duration_since(changed).as_secs_f64() * 1000.0;
            s.metric_with(
                "subscribe_ms",
                ms,
                "ms",
                json!({"unrelated_replies": unrelated}),
            );
            s.check(
                reply["generation"].as_u64().unwrap_or(0) > generation,
                "newer generation",
            );
        }
        Err(e) if is_unsupported(&e) => s.unsupported(format!("subscribe: {e:#}")),
        Err(e) => {
            s.check(false, format!("subscribe: {e:#}"));
        }
    }
    s.finish();
}
