//! Qualification scenarios for WP2.1 (actor orchestrator wiring, legacy bridge,
//! Shutdown). REVAMP_PLAN §14.6, §3.3, §3.6, §7.3.
//!
//! Created by WP0.5 against the 6298d0b baseline, `#[ignore = "WP2.1"]`.
//! Written against dynamic action JSON (`raw_call`), so the new `status` member
//! (§7.3) and new actions are read by name.
//! `cargo test --release --test qual_wp2_1_lifecycle -- --ignored --test-threads=1 --nocapture`
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
    time::Duration,
};
use support::*;

const FILE: &str = "qual_wp2_1_lifecycle";

/// Turn `folder` into a crash loop: remove its store so every start fails.
/// The store is deleted only after the worker has exited.
fn make_crasher(node: &Node, folder: &Value) {
    let state = state_id(folder);
    node.stop_folder_exited(folder, STOP_EXIT_TIMEOUT).unwrap();
    for name in [
        "metadata.sqlite",
        "metadata.sqlite-wal",
        "metadata.sqlite-shm",
    ] {
        let _ = fs::remove_file(state_dir(folder).join(name));
    }
    let _ = node.raw_call(json!({"op":"start","state":state}));
}

/// Lifecycle phase from the §7.3 `status` member (None before WP2.1).
fn phase(inspect: &Value) -> Option<String> {
    inspect["status"]["lifecycle"]["phase"]
        .as_str()
        .map(str::to_owned)
}

/// `List` p99 < 100 ms over 200 calls while one folder crash-loops, one is
/// churned by Start/Stop and (with mounts) one is StopBlocked by an open file.
#[test]
#[ignore = "WP2.1"]
fn api_latency_under_churn() {
    let mut s = Scenario::new(FILE, "api_latency_under_churn");
    let a = Node::new();
    let crasher = a.create("Crash loop", true);
    make_crasher(&a, &crasher);
    let churn = a.create("Churn", true);
    let mut _open = None;
    if mount_supported() {
        let mount = a.path().join("busy-mount");
        let busy = a.create_mounted("Busy", &mount);
        let mut f = fs::File::create(mount.join("open.txt")).unwrap();
        f.write_all(b"held open").unwrap();
        _open = Some(f);
        let _ = a.raw_call(json!({"op":"stop","state":state_id(&busy),"wait":false}));
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
    let summary = summarize(&samples);
    let mut extra = summary.json();
    extra["churn"] = counts.json();
    s.metric_with("list_p99_ms", summary.p99, "ms", extra);
    s.finish();
}

/// A crash-looping folder reaches `Failed{CRASH_LOOP}` in its status member.
#[test]
#[ignore = "WP2.1"]
fn crash_loop_reaches_failed() {
    let mut s = Scenario::new(FILE, "crash_loop_reaches_failed");
    let a = Node::new();
    let crasher = a.create("Crash loop", true);
    make_crasher(&a, &crasher);
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
    let watch = Stopwatch::start();
    let resumed = wait_until(s.remaining(), || {
        has_file(&pb, "while-down.txt", b"while the daemon was down")
    });
    s.metric("resume_ms", watch.ms(), "ms");
    s.check(resumed, "sync resumes");
    s.finish();
}

/// A worker whose `watch` hangs is detected (Unresponsive) and restarted;
/// another folder's worker is unaffected.
#[test]
#[ignore = "WP2.1"]
fn hung_worker_restarted() {
    let mut s = Scenario::new(FILE, "hung_worker_restarted");
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
    let hung_address = worker_address(&hung);
    let healthy_address = worker_address(&healthy);
    s.measure_from_now();
    let watch = Stopwatch::start();
    let restarted = wait_until(s.remaining(), || {
        let now = worker_address(&hung);
        now.is_some() && now != hung_address
    });
    s.metric("restart_ms", watch.ms(), "ms");
    if !restarted {
        s.timeout("hung worker never restarted");
    }
    s.check(
        worker_address(&healthy) == healthy_address,
        "healthy folder's worker untouched",
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
    let inspect = a.inspect(&state);
    s.check(
        inspect["status"]["mount"]["state"]
            .to_string()
            .contains("attach_failed"),
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

/// SIGTERM produces the same outcome as the Shutdown action (Unix).
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
        let p = a.create("Signal", true);
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
        a.spawn();
        let inspect = a.inspect(&state_id(&p));
        s.check(
            inspect["desired_running"] == true,
            "desired_running unchanged",
        );
        s.finish();
    }
}

/// `subscribe{since}` returns within 1 s of a lifecycle change.
#[test]
#[ignore = "WP2.1"]
fn subscribe_latency() {
    let mut s = Scenario::new(FILE, "subscribe_latency");
    let a = Node::new();
    let status = a.raw_call(json!({"op":"status"}));
    let Ok(status) = status else {
        s.unsupported(format!("status action: {status:?}"));
        return s.finish();
    };
    let generation = status["generation"].as_u64().unwrap_or(0);
    let control = a.control();
    let waiter = thread::spawn(move || {
        let reply = control.call(&json!({"op":"subscribe","since":generation,"wait_ms":10000}));
        Ok::<_, anyhow::Error>((reply?, std::time::Instant::now()))
    });
    thread::sleep(Duration::from_millis(300));
    let changed = std::time::Instant::now();
    let _ = a.raw_call(json!({"op":"create","label":"Subscribe","mount":null,"headless":true}));
    match waiter.join().unwrap() {
        Ok((reply, at)) => {
            let ms = at.saturating_duration_since(changed).as_secs_f64() * 1000.0;
            s.metric("subscribe_ms", ms, "ms");
            s.check(
                reply["generation"].as_u64().unwrap_or(0) > generation,
                "newer generation",
            );
        }
        Err(e) => s.unsupported(format!("subscribe: {e:#}")),
    }
    s.finish();
}
