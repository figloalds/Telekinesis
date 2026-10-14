//! WP1.12: interim stabilizers on the legacy lanes and supervisor
//! (REVAMP_PLAN §14 WP1.12). Real daemons through `tests/support`; the
//! release-mode scenarios with floors live in `qual_wp1_12_legacy.rs`.
mod support;

use serde_json::{Value, json};
use std::{
    fs,
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use support::*;
use tkfs::{core::id, ws};

/// Every lane's status of `repo` on the first peer.
fn repo_statuses(peers: &Value, repo: &str) -> Vec<Value> {
    peers[0]["lanes"]
        .as_array()
        .map(|lanes| {
            lanes
                .iter()
                .map(|l| l["repositories"][repo].clone())
                .collect()
        })
        .unwrap_or_default()
}

fn lanes_connected(node: &Node) -> bool {
    node.raw_call(json!({"op":"peers"})).is_ok_and(|p| {
        p[0]["lanes"]
            .as_array()
            .is_some_and(|l| !l.is_empty() && l.iter().all(|l| l["connected"] == true))
    })
}

/// A hung remote worker is charged to its own repository and backed off,
/// while the other repository on the same peer keeps syncing; the acceptor's
/// `list` stays within its per-worker deadline.
#[test]
fn hung_remote_worker_is_charged_while_the_healthy_repository_syncs() {
    let a = Node::new();
    let b = Node::start(NodeConfig::default().connections(2));
    let px = a.create("Hung X", true);
    let py = a.create("Healthy Y", true);
    write_file(&px, "x.txt", b"x");
    write_file(&py, "y.txt", b"y");
    b.pair(&a, &a.code());
    let bx = b.receive(&a, &px);
    let by = b.receive(&a, &py);
    b.wait_for("initial sync", Duration::from_secs(60), || {
        has_file(&bx, "x.txt", b"x") && has_file(&by, "y.txt", b"y")
    });
    let hung = HungServer::start();
    let redirect = redirect_runtime_json(&state_dir(&px), &hung.endpoint()).unwrap();
    let started = Instant::now();
    write_file(&py, "after-hang.txt", b"healthy progress");
    let x_repo = px["repo"].as_str().unwrap().to_owned();
    let mut charged = None;
    let sample = |charged: &mut Option<Value>| {
        if charged.is_none()
            && let Ok(peers) = b.raw_call(json!({"op":"peers"}))
        {
            *charged = repo_statuses(&peers, &x_repo).into_iter().find(|r| {
                r["error"].is_string() && r["consecutive_failures"].as_u64().unwrap_or(0) > 0
            });
        }
    };
    let progressed = wait_until(Duration::from_secs(90), || {
        sample(&mut charged);
        has_file(&by, "after-hang.txt", b"healthy progress")
    });
    let delay = started.elapsed();
    assert!(progressed, "the healthy repository stalled\n{}", b.logs());
    assert!(
        delay < Duration::from_secs(45),
        "healthy repository took {delay:?}"
    );
    // The hung exchange times out after the 15 s socket timeout.
    wait_until(Duration::from_secs(40), || {
        sample(&mut charged);
        charged.is_some()
    });
    let charged = charged.unwrap_or_else(|| panic!("hung repository never charged\n{}", b.logs()));
    assert!(charged["retry_at_ms"].is_u64(), "backed off: {charged}");
    assert!(hung.received() > 0, "the hung worker was asked");
    // The acceptor answers `list` within the per-worker deadline, without X.
    let (listed, ms) = time_ms(|| b.raw_call(json!({"op":"remote-list","endpoint":a.endpoint()})));
    let listed = listed.unwrap();
    assert!(ms < 8000.0, "remote-list took {ms} ms");
    let repos: Vec<_> = listed
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["repo"].clone())
        .collect();
    assert!(repos.contains(&py["repo"]) && !repos.contains(&px["repo"]));
    let error = a.inspect(&state_id(&px))["publication_error"].clone();
    assert!(
        error.as_str().is_some_and(
            |e| e.contains("PUBLISHED_SUMMARY_FAILED") && e.contains("REQUEST_TIMEOUT")
        ),
        "{error}"
    );
    redirect.restore().unwrap();
}

/// `list` asks every worker concurrently under its own 3 s deadline, so two
/// hung workers cost one deadline, not two.
#[test]
fn published_list_is_bounded_by_one_worker_deadline() {
    let a = Node::new();
    let healthy = a.create("Healthy", true);
    let hung = HungServer::start();
    let mut redirects = vec![];
    let mut stuck = vec![];
    for label in ["Hung one", "Hung two"] {
        let p = a.create(label, true);
        redirects.push(redirect_runtime_json(&state_dir(&p), &hung.endpoint()).unwrap());
        stuck.push(p);
    }
    let code = a.code();
    let mut socket = ws::connect(&a.endpoint()).unwrap();
    ws::call(
        &mut socket,
        &json!({"op":"pair","version":2,"code":code,"installation":id(),"lane":"control"}),
    )
    .unwrap();
    for _ in 0..2 {
        let (listed, ms) = time_ms(|| ws::call(&mut socket, &json!({"op":"list"})));
        let listed = listed.unwrap();
        assert!(
            (2500.0..5500.0).contains(&ms),
            "list took {ms} ms with two hung workers"
        );
        let repos: Vec<_> = listed
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["repo"].clone())
            .collect();
        assert_eq!(repos, vec![healthy["repo"].clone()]);
    }
    for p in &stuck {
        let error = a.inspect(&state_id(p))["publication_error"].clone();
        assert!(
            error
                .as_str()
                .is_some_and(|e| e.contains("REQUEST_TIMEOUT")),
            "{error}"
        );
    }
    for redirect in redirects {
        redirect.restore().unwrap();
    }
    a.wait_for("publication errors clear", Duration::from_secs(30), || {
        ws::call(&mut socket, &json!({"op":"list"}))
            .is_ok_and(|l| l.as_array().is_some_and(|l| l.len() == 3))
            && a.inspect(&state_id(&stuck[0]))["publication_error"].is_null()
    });
}

/// A dial alone is not a connection: lanes report `connected` only after
/// their first request (`list` / `ping`) succeeded.
#[test]
fn lanes_are_connected_only_after_a_request_succeeds() {
    let node = Node::new();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let release = Arc::new(AtomicBool::new(false));
    let held = Arc::new(AtomicU64::new(0));
    {
        let (release, held) = (release.clone(), held.clone());
        thread::spawn(move || {
            let installation = id();
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                let (release, held, installation) =
                    (release.clone(), held.clone(), installation.clone());
                thread::spawn(move || {
                    let Ok(mut socket) = ws::accept(stream) else {
                        return;
                    };
                    let Ok(hello) = ws::receive::<Value>(&mut socket) else {
                        return;
                    };
                    let reply =
                        json!({"installation":installation,"version":2,"lane":hello["lane"]});
                    if ws::reply(&mut socket, Ok(reply)).is_err() {
                        return;
                    }
                    while let Ok(request) = ws::receive::<Value>(&mut socket) {
                        // Hold the first requests (well below the 15 s timeout).
                        let since = Instant::now();
                        if !release.load(Ordering::Relaxed) {
                            held.fetch_add(1, Ordering::Relaxed);
                        }
                        while !release.load(Ordering::Relaxed)
                            && since.elapsed() < Duration::from_secs(12)
                        {
                            thread::sleep(Duration::from_millis(20));
                        }
                        let answer = match request["op"].as_str() {
                            Some("list") => Ok(json!([])),
                            Some("ping") => Ok(json!({"alive":true})),
                            _ => Err(anyhow::anyhow!("UNKNOWN_PEER_OPERATION")),
                        };
                        if ws::reply(&mut socket, answer).is_err() {
                            return;
                        }
                    }
                });
            }
        });
    }
    let paired = node.call(json!({"op":"pair","endpoint":endpoint,"code":"test","connections":1}));
    assert_eq!(paired["connections"], 2);
    node.wait_for("both lanes dialed", Duration::from_secs(10), || {
        held.load(Ordering::Relaxed) >= 2
    });
    let peers = node.peers();
    let lanes = peers[0]["lanes"].as_array().unwrap();
    assert_eq!(lanes.len(), 2);
    for lane in lanes {
        assert_eq!(lane["connected"], false, "dialed but unproven: {lane}");
    }
    release.store(true, Ordering::Relaxed);
    node.wait_for("lanes connected", Duration::from_secs(10), || {
        lanes_connected(&node)
    });
}

/// A worker that exits at once is restarted after 1, 2, 4 … s (not every
/// reconcile), its error is reported, and `list` stays responsive.
#[test]
fn crash_looping_worker_backs_off_and_list_stays_responsive() {
    let a = Node::new();
    let p = a.create("Crasher", true);
    let state = state_id(&p);
    a.stop_folder_exited(&p, STOP_EXIT_TIMEOUT).unwrap();
    let dir = state_dir(&p);
    for name in [
        "metadata.sqlite",
        "metadata.sqlite-wal",
        "metadata.sqlite-shm",
    ] {
        let _ = fs::remove_file(dir.join(name));
    }
    let log = dir.join("worker.log");
    // One failed start per line naming the code. WP1.INT: since WP1.4 a
    // failed start writes its `FatalLine` (code and message) plus a `fatal`
    // log event repeating it, so count lines, not matches, and skip the log
    // event (`"level":`).
    let count = || {
        fs::read_to_string(&log)
            .unwrap_or_default()
            .lines()
            .filter(|line| line.contains("STATE_NOT_INITIALIZED") && !line.contains("\"level\":"))
            .count()
    };
    let before = count();
    let started = Instant::now();
    a.call(json!({"op":"start","state":state}));
    let mut starts = vec![];
    let mut seen = before;
    let mut latencies = vec![];
    while started.elapsed() < Duration::from_secs(16) {
        let now = count();
        if now > seen {
            starts.extend((seen..now).map(|_| started.elapsed().as_secs_f64()));
            seen = now;
        }
        let (listed, ms) = time_ms(|| a.raw_call(json!({"op":"list"})));
        assert!(listed.is_ok(), "list failed during the crash loop");
        latencies.push(ms);
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        (3..=6).contains(&starts.len()),
        "starts in 16 s at {starts:?}\n{}",
        a.logs()
    );
    for (attempt, pair) in starts.windows(2).take(3).enumerate() {
        let gap = pair[1] - pair[0];
        let floor = 0.8 * f64::from(1u32 << attempt) * 0.95;
        assert!(
            gap >= floor,
            "restart {} came after {gap:.2} s (< {floor:.2} s): {starts:?}",
            attempt + 1
        );
    }
    let summary = summarize(&latencies);
    assert!(summary.p99 < 1000.0, "list p99 {} ms", summary.p99);
    let inspected = a.inspect(&state);
    assert!(
        inspected["error"]
            .as_str()
            .is_some_and(|e| e.contains("WORKER_EXITED")),
        "{inspected}"
    );
    // Stop ends the loop; an explicit Start would retry at once.
    a.call(json!({"op":"stop","state":state}));
}

/// With `sync_interval_ms` above the acceptor's 15 s idle drop, lanes ping
/// while waiting and never reconnect.
#[test]
fn idle_lanes_survive_an_interval_longer_than_the_idle_timeout() {
    let a = Node::new();
    let b = Node::start(NodeConfig::default().interval(20_000).connections(2));
    let proxy = FaultProxy::start(&a.endpoint());
    let pa = a.create("Idle source", true);
    b.pair_endpoint(&proxy.endpoint(), &a.code(), Some(2));
    let _pb = b.receive_from(&proxy.endpoint(), &pa);
    b.wait_for("lanes connected", Duration::from_secs(60), || {
        lanes_connected(&b)
    });
    thread::sleep(Duration::from_secs(2));
    let before = proxy.connections();
    thread::sleep(Duration::from_secs(26));
    assert_eq!(
        proxy.connections() - before,
        0,
        "idle lanes reconnected\n{}",
        b.logs()
    );
    assert!(lanes_connected(&b));
}

/// Shutdown sets `stopping` first, continues past a folder that cannot be
/// stopped (left running for the next daemon), and reports every outcome.
#[test]
fn shutdown_continues_past_a_folder_that_cannot_stop() {
    let mut a = Node::new();
    let stuck = a.create("Unreachable", true);
    let normal = a.create("Normal", true);
    let stuck_pids = a.worker_pids(&stuck);
    assert!(!stuck_pids.is_empty());
    // The worker keeps running, but the daemon can no longer reach it.
    let _redirect = redirect_runtime_json(&state_dir(&stuck), "ws://127.0.0.1:0").unwrap();
    let (reply, status) = a.shutdown_within(Duration::from_secs(30));
    let reply = reply.unwrap();
    let status = status.expect("the daemon exits after shutdown");
    assert!(status.success(), "{status}");
    assert_eq!(reply["stopped"], true);
    let outcome = |folder: &Value| {
        reply["folders"]
            .as_array()
            .unwrap()
            .iter()
            .find(|o| o["state"] == folder["state"])
            .cloned()
            .unwrap_or_else(|| panic!("no outcome for {folder} in {reply}"))
    };
    let failed = outcome(&stuck);
    assert_eq!(failed["outcome"], "left_running", "{reply}");
    assert!(failed["error"]["code"].is_string(), "{reply}");
    let stopped = outcome(&normal);
    assert_eq!(stopped["outcome"], "stopped", "{reply}");
    assert!(stopped["error"].is_null());
    assert!(!worker_listening(&state_dir(&normal)));
    assert!(
        !a.worker_pids(&stuck).is_empty(),
        "the unreachable worker is left running"
    );
    for pid in stuck_pids {
        kill_pid(pid);
    }
}

/// A requested mount that fails no longer kills the worker: it reports Ready
/// with `mounted:false` and `mount_error` (WP1.4), the supervisor keeps it
/// running headless, reports the error and keeps syncing.
#[test]
fn mount_failure_keeps_the_folder_syncing_headless() {
    if !mount_supported() {
        return;
    }
    let a = Node::new();
    let b = Node::new();
    let mount = a.path().join("blocked-mount");
    let p = a.create_mounted("Blocked mount", &mount);
    a.stop_folder_exited(&p, STOP_EXIT_TIMEOUT).unwrap();
    // A pre-existing, non-empty mount path: the attach must fail.
    fs::create_dir_all(&mount).unwrap();
    fs::write(mount.join("squatter.txt"), b"not ours").unwrap();
    let state = state_id(&p);
    a.call(json!({"op":"start","state":state}));
    a.wait_for("running headless", Duration::from_secs(30), || {
        a.try_inspect(&state)
            .is_ok_and(|i| i["running"] == true && i["mounted"] == false)
    });
    let inspected = a.inspect(&state);
    assert!(
        inspected["error"]
            .as_str()
            .is_some_and(|e| e.contains("MOUNT_PATH_MUST_NOT_EXIST")),
        "{inspected}"
    );
    let pids = a.worker_pids(&p);
    b.pair(&a, &a.code());
    let pb = b.receive(&a, &p);
    write_file(&p, "while-unmounted.txt", b"still syncing");
    b.wait_for("sync without the mount", Duration::from_secs(60), || {
        has_file(&pb, "while-unmounted.txt", b"still syncing")
    });
    assert_eq!(a.worker_pids(&p), pids, "the worker was never restarted");
    assert_eq!(fs::read(mount.join("squatter.txt")).unwrap(), b"not ours");
}
