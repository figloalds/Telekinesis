//! Daemon-level tests (two or three real daemons). WP3.1 migrated them to
//! v3 networking (REVAMP_PLAN §14 WP3.1 task 3): the legacy (v2) lanes are
//! gone, the List/Inspect/Peers shapes are the hub's projection (§7.3), and
//! a 6298d0b peer fails closed (`legacy_peer_fails_closed_with_needs_upgrade`).
use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};
use tkfs::{
    core::{hash, id},
    model::{
        errors::ErrorWire,
        ipc::{FeedExchangeReply, PublishedSummary},
        orchestrator::{EntryState, PublishedEntry, WorkerEndpoint},
        protocol::{
            CatalogListReply,
            feed::{LARGE_EVENT_BYTES, MetaExchangeRequest},
            objects::{
                ObjectOutcome, ObjectResult, ObjectsPush, ObjectsResult, ObjectsWantsReply,
                ObjectsWantsRequest, WantHeader, WriteItem,
            },
        },
    },
    orchestrator::{self, Action, Config, worker_client::SessionWorkerClient},
    paired_sync::SyncRoute,
    runtime::{self, Discovery},
    session::{
        self, AcceptPolicy, AcceptedPeer, ClientHello, CodeAuth, Conn, ConnOptions, DialError,
        Handler, Incoming, LaneRole, Priority, Reply,
    },
    ws,
};

struct Node {
    root: tempfile::TempDir,
    config: Config,
    child: Option<Child>,
}
impl Node {
    fn new() -> Self {
        Self::with_interval(50)
    }
    fn with_interval(interval: u64) -> Self {
        let root = tempfile::tempdir().unwrap();
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(root.path(), fs::Permissions::from_mode(0o777)).unwrap();
        }
        let path = root.path().join("orchestrator.toml");
        fs::write(&path,format!("format_version=2\ndata_directory='data'\ndefault_mount_directory='mounts'\nmaximum_running=8\n[network]\nlisten='127.0.0.1:0'\nconnections=3\nsync_interval_ms={interval}\n")).unwrap();
        let config = Config::load(&path).unwrap();
        let mut node = Self {
            root,
            config,
            child: None,
        };
        node.start();
        node
    }
    fn start(&mut self) {
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.path().join("daemon.log"))
            .unwrap();
        self.child = Some(
            Command::new(env!("CARGO_BIN_EXE_tkfs"))
                .arg("-f")
                .arg(self.root.path().join("orchestrator.toml"))
                .arg("orchestrator")
                .stdin(Stdio::null())
                .stdout(log.try_clone().unwrap())
                .stderr(log)
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if self.try_call(Action::List).is_ok() {
                break;
            }
            if let Some(status) = self.child.as_mut().unwrap().try_wait().unwrap() {
                panic!(
                    "daemon exited {status}: {}",
                    fs::read_to_string(self.root.path().join("daemon.log")).unwrap()
                );
            }
            assert!(Instant::now() < deadline, "daemon startup timeout");
            thread::sleep(Duration::from_millis(25));
        }
    }
    fn try_call(&self, action: Action) -> anyhow::Result<Value> {
        orchestrator::client(&self.config, &action)
    }
    fn call(&self, action: Action) -> Value {
        let result = self.try_call(action).unwrap();
        if result["starting"] == true {
            let state = result["state"].as_str().unwrap().to_owned();
            wait_for(|| {
                self.try_call(Action::Inspect {
                    state: state.clone(),
                })
                .is_ok_and(|p| p["running"] == true)
            });
            return self.try_call(Action::Inspect { state }).unwrap();
        }
        result
    }
    fn endpoint(&self) -> String {
        let d: Value = serde_json::from_slice(
            &fs::read(self.config.data_directory.join("orchestrator.json")).unwrap(),
        )
        .unwrap();
        d["network"].as_str().unwrap().into()
    }
    fn code(&self) -> String {
        self.call(Action::ResetPairingCode)["code"]
            .as_str()
            .unwrap()
            .into()
    }
    fn installation(&self) -> String {
        self.call(Action::List)["installation"]
            .as_str()
            .unwrap()
            .into()
    }
    /// The typed status (`Status`, schema 2) as JSON.
    fn status(&self) -> Value {
        self.try_call(Action::Status).unwrap()
    }
    fn create(&self, label: &str, headless: bool) -> Value {
        let p = self.call(Action::Create {
            label: label.into(),
            mount: None,
            headless,
        });
        assert_eq!(p["running"], true, "{p}");
        p
    }
    fn pair(&self, other: &Node, code: &str) {
        self.call(Action::Pair {
            endpoint: other.endpoint(),
            code: code.into(),
            connections: Some(3),
        });
    }
    fn receive(&self, other: &Node, p: &Value) -> Value {
        let result = self.call(Action::Receive {
            endpoint: other.endpoint(),
            repo: p["repo"].as_str().unwrap().into(),
            label: p["label"].as_str().unwrap().into(),
            mount: None,
            headless: true,
        });
        assert_eq!(result["running"], true, "{result}");
        result
    }
    fn stop(&mut self) {
        self.call(Action::Shutdown);
        let mut child = self.child.take().unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(25));
        }
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = self.try_call(Action::Shutdown);
            for _ in 0..40 {
                if child.try_wait().ok().flatten().is_some() {
                    return;
                }
                thread::sleep(Duration::from_millis(25));
            }
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
fn info(p: &Value) -> Discovery {
    serde_json::from_slice(
        &fs::read(Path::new(p["directory"].as_str().unwrap()).join("runtime.json")).unwrap(),
    )
    .unwrap()
}
fn rpc(p: &Value, payload: Value) -> Value {
    runtime::rpc(&info(p), &id(), payload).unwrap()
}
fn write(p: &Value, name: &str, bytes: &[u8]) {
    rpc(
        p,
        json!({"op":"import","path":name,"hex":hex::encode(bytes)}),
    );
}

#[test]
fn active_bulk_pages_do_not_wait_for_the_idle_poll_interval() {
    let a = Node::with_interval(5000);
    let b = Node::with_interval(5000);
    let pa = a.create("Bulk source", true);
    let bytes = vec![37; 6 * 1024 * 1024];
    write(&pa, "large.bin", &bytes);
    b.pair(&a, &a.code());
    let pb = b.receive(&a, &pa);
    // Wait for discovery/metadata authorization and the first data page. Those
    // may legitimately use the idle interval; subsequent pages must not.
    wait_for(|| {
        let content = rpc(&pb, json!({"op":"status"}))["content"].clone();
        content["partial_bytes"].as_u64().unwrap_or(0) > 0
            || content["current_objects"] == 1 && content["complete"] == true
    });
    let started = Instant::now();
    loop {
        let content = rpc(&pb, json!({"op":"status"}))["content"].clone();
        if content["current_objects"] == 1 && content["complete"] == true {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "active content transfer was throttled by idle polling: {}",
            b.call(Action::Peers)
        );
        thread::sleep(Duration::from_millis(50));
    }
    assert!(has(&pb, "large.bin", &bytes));
    let peers = b.call(Action::Peers);
    assert_eq!(peers[0]["lanes"][0]["role"], "control");
    assert!(peers[0]["lanes"].as_array().unwrap().iter().any(|lane| {
        lane["repositories"][pa["repo"].as_str().unwrap()]["transfer"]["received_object_bytes"]
            .is_number()
    }));
}

#[test]
fn one_repository_uses_all_bulk_lanes_and_keeps_control_free_of_objects() {
    let a = Node::new();
    let b = Node::new();
    let pa = a.create("Parallel source", true);
    let mut contents = Vec::new();
    for index in 0..3 {
        let lane = tkfs::bulk_sync::Lane { index, count: 3 };
        let bytes = (0u64..)
            .map(|seed| seed.to_le_bytes().repeat(8192))
            .find(|bytes| lane.owns(&hash(bytes)))
            .unwrap();
        let name = format!("lane-{index}.bin");
        write(&pa, &name, &bytes);
        contents.push((name, bytes));
    }
    b.call(Action::Pair {
        endpoint: a.endpoint(),
        code: a.code(),
        connections: Some(4),
    });
    let pb = b.receive(&a, &pa);
    wait_for(|| contents.iter().all(|(name, bytes)| has(&pb, name, bytes)));
    wait_for(|| {
        let peers = b.call(Action::Peers);
        (1..4).all(|index| peers[0]["lanes"][index]["repositories"][pa["repo"].as_str().unwrap()]["total_received_object_bytes"].as_u64().unwrap_or(0) >= 65536)
    });
    let peers = b.call(Action::Peers);
    for lane in peers[0]["lanes"].as_array().unwrap() {
        assert_eq!(lane["parallel_bulk"], true);
        // WP3.1: every lane is a v3 session (§7.3); the rest of this test is
        // served by the hub's projection of the v3 lanes.
        assert_eq!(lane["transport"], "tkfs-peer/3");
    }
    let control = &peers[0]["lanes"][0]["repositories"][pa["repo"].as_str().unwrap()];
    assert_eq!(control["total_received_object_bytes"], 0);
    assert_eq!(control["total_sent_object_bytes"], 0);
    let inspect = || {
        b.call(Action::Inspect {
            state: pb["state"].as_str().unwrap().into(),
        })
    };
    wait_for(|| {
        let status = inspect();
        let lanes = status["sync"][0]["lanes"].as_array().unwrap();
        status["sync"][0]["remote_available"] == true
            && lanes.len() == 4
            && lanes[1..].iter().all(|l| {
                let s = &l["repository"];
                s["object_inventory"]["local_available"]
                    == s["object_inventory"]["remote_available"]
                    && s["object_inventory"]["local_available"]
                        .as_u64()
                        .unwrap_or(0)
                        > 0
                    && s["last_progress_ms"].is_u64()
                    && s["total_received_wire_bytes"].as_u64().unwrap_or(0) > 0
            })
    });
    // A connected orchestrator is not evidence of an available folder worker.
    a.call(Action::Stop {
        state: pa["state"].as_str().unwrap().into(),
        wait: None,
    });
    wait_for(|| {
        let status = inspect();
        status["sync"][0]["connected"] == true && status["sync"][0]["remote_available"] == false
    });
}
#[track_caller]
fn wait_for(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !predicate() {
        assert!(Instant::now() < deadline, "condition timeout");
        thread::sleep(Duration::from_millis(50));
    }
}
fn has(p: &Value, name: &str, bytes: &[u8]) -> bool {
    let Ok(bytes_discovery) =
        fs::read(Path::new(p["directory"].as_str().unwrap()).join("runtime.json"))
    else {
        return false;
    };
    let Ok(discovery) = serde_json::from_slice(&bytes_discovery) else {
        return false;
    };
    let Ok(state) = runtime::rpc(&discovery, &id(), json!({"op":"state"})) else {
        return false;
    };
    state["entries"].as_object().is_some_and(|entries| {
        entries
            .values()
            .any(|e| e["name"] == name && e["alive"] == true && e["content"] == hash(bytes))
    })
}

#[test]
fn folder_lifecycle_persistent_pool_bidirectional_sync_and_restart() {
    let mut a = Node::new();
    let mut b = Node::new();
    let first = a.create("First", true);
    let second = a.create("Second", true);
    let code = a.code();
    b.pair(&a, &code);
    let first_b = b.receive(&a, &first);
    let second_b = b.receive(&a, &second);
    write(&first, "from-a.txt", b"A to B");
    write(&second_b, "from-b.txt", b"B to A");
    wait_for(|| has(&first_b, "from-a.txt", b"A to B") && has(&second, "from-b.txt", b"B to A"));
    wait_for(|| {
        b.call(Action::Peers)[0]["lanes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|l| l["connected"] == true)
    });
    // A has no dial configuration; replies travel over B's persistent connections.
    assert_eq!(a.call(Action::Peers), json!([]));
    let status = b.call(Action::Peers);
    assert!(!status.to_string().contains(&code));
    // The inbound daemon persists only salt/hash, never its code.
    let db = rusqlite::Connection::open(a.config.data_directory.join("folders.sqlite")).unwrap();
    let record: String = db
        .query_row("SELECT value FROM settings WHERE key='pairing'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert!(!record.contains(&code));
    assert!(record.contains("salt"));
    drop(db);
    // Desired-running folders and outbound pairing survive process restarts.
    b.stop();
    write(&first, "offline.txt", b"queued while B stopped");
    b.start();
    wait_for(|| has(&first_b, "offline.txt", b"queued while B stopped"));
    let state = first_b["state"].as_str().unwrap().to_owned();
    b.call(Action::Stop {
        state: state.clone(),
        wait: None,
    });
    let inspect = b.call(Action::Inspect {
        state: state.clone(),
    });
    assert_eq!(inspect["running"], false);
    assert_eq!(inspect["desired_running"], false);
    b.stop();
    b.start();
    assert_eq!(
        b.call(Action::Inspect {
            state: state.clone()
        })["running"],
        false
    );
    b.call(Action::Start { state });
    wait_for(|| has(&first_b, "from-a.txt", b"A to B"));
    b.stop();
    a.stop();
}

#[test]
fn pairing_code_only_rotation_invalidates_persistent_connections_and_rejects_legacy_transports() {
    let a = Node::new();
    let b = Node::new();
    let code = a.code();
    assert!(
        b.try_call(Action::Pair {
            endpoint: a.endpoint(),
            code: "wrong".into(),
            connections: None
        })
        .is_err()
    );
    b.pair(&a, &code);
    // WP3.1: a raw v3 probe session (what pairing and RemoteList dial) is
    // the persistent connection; the legacy (v2) pair request is refused.
    let probe = |code: &str| {
        let hello = session::client_hello(&id(), LaneRole::Probe, None, 2);
        session::dial(
            &a.endpoint(),
            hello,
            &CodeAuth { code: code.into() },
            None,
            ConnOptions::for_role(LaneRole::Probe),
        )
    };
    let (conn, server) = probe(&code).unwrap();
    assert_eq!(server.installation, a.installation());
    list_catalog(&conn).unwrap();
    let next = a.code();
    assert_ne!(next, code);
    // The rotation sends the older session away: GOAWAY{PAIRING_CODE_CHANGED}.
    let closed = conn
        .wait_closed(Duration::from_secs(5))
        .expect("the session outlived the rotation");
    assert_eq!(closed.code, "PAIRING_CODE_CHANGED", "{closed}");
    assert!(list_catalog(&conn).is_err());
    // The old code no longer opens a session; the new one does.
    match probe(&code) {
        Err(DialError::Unauthorized(error)) => assert_eq!(error.code, "INVALID_PAIRING_CODE"),
        other => panic!("the old code was not refused: {:?}", other.map(|_| ())),
    }
    let (fresh, _) = probe(&next).unwrap();
    list_catalog(&fresh).unwrap();
    fresh.close();
    // A legacy (v2) transport is refused before any code is looked at.
    let mut socket = ws::connect(&a.endpoint()).unwrap();
    let refused = ws::call(
        &mut socket,
        &json!({"op":"pair","version":2,"code":next,"installation":id()}),
    )
    .unwrap_err();
    assert!(
        format!("{refused:#}").contains("PEER_NEEDS_UPGRADE"),
        "{refused:#}"
    );
    b.pair(&a, &next);
    wait_for(|| {
        b.call(Action::Peers)[0]["lanes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|l| l["connected"] == true)
    });
    b.call(Action::Unpair {
        endpoint: a.endpoint(),
    });
    assert_eq!(b.call(Action::Peers), json!([]));
    assert!(ws::connect("https://127.0.0.1:1").is_err());
    assert!(ws::endpoint_uri("wss://localhost:443/sync").is_ok());
    assert!(ws::connect("tcp://127.0.0.1:1").is_err());
}

#[test]
fn failed_repository_does_not_starve_healthy_folders_on_the_same_lane() {
    let a = Node::new();
    let b = Node::new();
    let broken = a.create("A broken", true);
    let healthy = a.create("Z healthy", true);
    let bytes = b"repairable object";
    write(&broken, "repair.txt", bytes);
    let object = Path::new(broken["directory"].as_str().unwrap())
        .join("objects")
        .join(hash(bytes));
    fs::write(&object, vec![0; bytes.len()]).unwrap();
    b.call(Action::Pair {
        endpoint: a.endpoint(),
        code: a.code(),
        connections: Some(2),
    });
    let broken_b = b.receive(&a, &broken);
    let healthy_b = b.receive(&a, &healthy);
    let repo = broken["repo"].as_str().unwrap();
    // WP3.INT (objects v3): a worker finds a corrupt copy when it first serves
    // it (verify-on-first-serve, §6.4), quarantines it and answers
    // NOT_AVAILABLE; bytes that reach the receiver first fail its whole-object
    // check (CORRUPT_AT_SOURCE). 6298d0b's bulk page failed with
    // CORRUPT_OBJECT. Either way the object fails alone, booked on the lane's
    // repository (CCR-WP3.INT-1), and its retry waits for the per-object
    // backoff (§8 objects: 30 s, then longer), hence the longer waits below.
    let failed = |error: &str| {
        ["NOT_AVAILABLE", "CORRUPT_AT_SOURCE", "CORRUPT_OBJECT"]
            .iter()
            .any(|code| error.contains(code))
    };
    wait_for(|| {
        let peers = b.call(Action::Peers);
        let lane = &peers[0]["lanes"][1];
        lane["connected"] == true
            && lane["repositories"][repo]["error"]
                .as_str()
                .is_some_and(failed)
    });
    write(&healthy, "forward.txt", b"unblocked forward");
    write(&healthy_b, "reverse.txt", b"unblocked reverse");
    wait_for(|| {
        has(&healthy_b, "forward.txt", b"unblocked forward")
            && has(&healthy, "reverse.txt", b"unblocked reverse")
    });
    let peers = b.call(Action::Peers);
    let lane = &peers[0]["lanes"][1];
    assert_eq!(lane["connected"], true);
    assert!(lane["error"].is_null());
    assert!(
        lane["repositories"][repo]["consecutive_failures"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(lane["repositories"][repo]["retry_at_ms"].is_u64());
    fs::write(&object, bytes).unwrap();
    wait_long("the repaired object", Duration::from_secs(120), || {
        has_content(&broken_b, "repair.txt", bytes)
    });
    wait_for(|| b.call(Action::Peers)[0]["lanes"][0]["repositories"][repo]["caught_up"] == true);

    // Corrupt outbound content must also leave the other folder and socket usable.
    let state = broken["state"].as_str().unwrap().to_owned();
    a.call(Action::Stop {
        state: state.clone(),
        wait: None,
    });
    let local_bytes = b"local unsent object";
    write(&broken_b, "local-repair.txt", local_bytes);
    let local_object = Path::new(broken_b["directory"].as_str().unwrap())
        .join("objects")
        .join(hash(local_bytes));
    fs::write(&local_object, vec![0; local_bytes.len()]).unwrap();
    a.call(Action::Start { state });
    wait_long(
        "the corrupt outbound object",
        Duration::from_secs(60),
        || {
            b.call(Action::Peers)[0]["lanes"][1]["repositories"][repo]["error"]
                .as_str()
                .is_some_and(failed)
        },
    );
    write(&healthy, "still.txt", b"healthy during local failure");
    wait_for(|| has(&healthy_b, "still.txt", b"healthy during local failure"));
    fs::write(&local_object, local_bytes).unwrap();
    wait_long(
        "the repaired outbound object",
        Duration::from_secs(120),
        || has_content(&broken, "local-repair.txt", local_bytes),
    );
    wait_for(|| {
        let peers = b.call(Action::Peers);
        let status = &peers[0]["lanes"][1]["repositories"][repo];
        peers[0]["lanes"][0]["repositories"][repo]["caught_up"] == true
            && status["error"].is_null()
            && status["retry_at_ms"].is_null()
            && status["consecutive_failures"] == 0
    });
}

#[test]
fn unavailable_worker_does_not_fail_remote_discovery_and_reports_its_error() {
    let mut a = Node::new();
    let b = Node::new();
    // Keep A's peer endpoint across the daemon restarts below.
    a.config.network.listen = Some(a.endpoint().trim_start_matches("ws://").into());
    fs::write(
        a.root.path().join("orchestrator.toml"),
        toml::to_string(&a.config).unwrap(),
    )
    .unwrap();
    let unavailable = a.create("A unavailable", true);
    let healthy = a.create("Z healthy", true);
    b.pair(&a, &a.code());
    // Published entries come from each folder's controller (its
    // `published-summary` on Ready), so a `runtime.json` redirect no longer
    // reaches them (WP2.1): the worker fails that call instead. The daemon
    // restarts with the fault armed for one folder (workers inherit its
    // environment), and that folder is restarted.
    let state = unavailable["state"].as_str().unwrap().to_owned();
    fn restart(node: &mut Node, state: &str, fault: bool) {
        node.stop();
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(node.root.path().join("daemon.log"))
            .unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_tkfs"));
        command
            .arg("-f")
            .arg(node.root.path().join("orchestrator.toml"))
            .arg("orchestrator")
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log);
        if fault {
            command
                .env("TKFS_FAULT_POINT", "worker.rpc.published-summary")
                .env("TKFS_FAULT_ACTION", "fail")
                .env("TKFS_FAULT_SCOPE", state);
        }
        node.child = Some(command.spawn().unwrap());
        wait_for(|| node.try_call(Action::List).is_ok());
        node.call(Action::Stop {
            state: state.into(),
            wait: None,
        });
        node.call(Action::Start {
            state: state.into(),
        });
    }
    let inspect = |node: &Node| {
        node.call(Action::Inspect {
            state: state.clone(),
        })
    };
    let listed = |published: &Value, folder: &Value| {
        published
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["repo"] == folder["repo"])
    };
    restart(&mut a, &state, true);
    wait_for(|| {
        inspect(&a)["publication_error"]
            .as_str()
            .is_some_and(|e| e.contains("PUBLISHED_SUMMARY_FAILED"))
    });
    let mut published = Value::Null;
    wait_for(|| {
        published = b.call(Action::RemoteList {
            endpoint: a.endpoint(),
        });
        listed(&published, &healthy)
    });
    assert!(!listed(&published, &unavailable), "{published}");
    assert!(
        inspect(&a)["publication_error"]
            .as_str()
            .unwrap()
            .contains("PUBLISHED_SUMMARY_FAILED")
    );
    restart(&mut a, &state, false);
    wait_for(|| {
        let published = b.call(Action::RemoteList {
            endpoint: a.endpoint(),
        });
        published.as_array().unwrap().len() == 2 && inspect(&a)["publication_error"].is_null()
    });
}

#[test]
fn three_orchestrators_relay_origins_and_private_branches_stay_local() {
    let a = Node::new();
    let b = Node::new();
    let c = Node::new();
    let pa = a.create("Relay", true);
    b.pair(&a, &a.code());
    let pb = b.receive(&a, &pa);
    c.pair(&b, &b.code());
    let pc = c.receive(&b, &pb);
    write(&pa, "alpha.txt", b"A through B to C");
    write(&pc, "charlie.txt", b"C through B to A");
    wait_for(|| {
        has(&pc, "alpha.txt", b"A through B to C") && has(&pa, "charlie.txt", b"C through B to A")
    });
    rpc(&pa, json!({"op":"branch","name":"private"}));
    rpc(&pa, json!({"op":"checkout","name":"private"}));
    write(&pa, "private.txt", b"local work");
    write(&pc, "barrier.txt", b"still shared");
    wait_for(|| has(&pb, "barrier.txt", b"still shared"));
    assert!(!has(&pb, "private.txt", b"local work"));
    assert!(!has(&pc, "private.txt", b"local work"));
}

#[cfg(any(windows, target_os = "linux"))]
#[test]
fn real_mount_can_write_stop_reconfigure_and_remount() {
    #[cfg(target_os = "linux")]
    if !Path::new("/dev/fuse").exists() {
        return;
    }
    let a = Node::new();
    let p = a.create("Mounted", false);
    let mount = Path::new(p["mount"].as_str().unwrap());
    fs::write(mount.join("hello.txt"), b"durable mounted bytes").unwrap();
    assert_eq!(
        fs::read(mount.join("hello.txt")).unwrap(),
        b"durable mounted bytes"
    );
    let state = p["state"].as_str().unwrap().to_owned();
    assert!(
        a.try_call(Action::ConfigureMount {
            state: state.clone(),
            mount: None
        })
        .is_err()
    );
    a.call(Action::Stop {
        state: state.clone(),
        wait: None,
    });
    let new_mount = a.root.path().join("relocated");
    a.call(Action::ConfigureMount {
        state: state.clone(),
        mount: Some(new_mount.clone()),
    });
    a.call(Action::Start { state });
    assert_eq!(
        fs::read(new_mount.join("hello.txt")).unwrap(),
        b"durable mounted bytes"
    );
}

#[test]
fn startup_returns_observable_state_and_can_be_cancelled() {
    let node = Node::new();
    let folder = node
        .try_call(Action::Create {
            label: "Cancellable".into(),
            mount: None,
            headless: true,
        })
        .unwrap();
    assert_eq!(folder["starting"], true);
    assert_eq!(folder["running"], false);
    assert!(folder["startup_phase"].is_string());
    let state = folder["state"].as_str().unwrap().to_owned();
    let stopped = node
        .try_call(Action::Stop {
            state: state.clone(),
            wait: None,
        })
        .unwrap();
    assert_eq!(stopped["starting"], false);
    assert_eq!(stopped["running"], false);
    assert_eq!(stopped["desired_running"], false);
    assert_eq!(stopped["startup_phase"], Value::Null);
    node.try_call(Action::ConfigureMount { state, mount: None })
        .unwrap();
}

#[test]
fn v2_configuration_uses_ordinary_paths_and_does_not_accept_credential_configuration() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("orchestrator.toml");
    let config = Config::initialize(&path).unwrap();
    assert_eq!(config.network.connections, 4);
    assert_eq!(config.network.listen.as_deref(), Some("127.0.0.1:43180"));
    assert!(Config::initialize(&path).is_err());
    fs::write(
        &path,
        "format_version=1\ndata_directory='data'\ndefault_mount_directory='mounts'\n",
    )
    .unwrap();
    assert!(Config::load(&path).is_err());
    fs::write(&path,"format_version=2\ndata_directory='data'\ndefault_mount_directory='mounts'\n[credential]\nbackend='windows-dpapi'\n").unwrap();
    assert!(Config::load(&path).is_err());
}

#[test]
fn large_pages_resume_after_daemon_crash_and_server_restart_keeps_its_verifier() {
    let mut a = Node::new();
    let mut b = Node::new();
    // Persist the ephemeral test port so the receiving endpoint survives restart.
    a.config.network.listen = Some(a.endpoint().trim_start_matches("ws://").into());
    fs::write(
        a.root.path().join("orchestrator.toml"),
        toml::to_string(&a.config).unwrap(),
    )
    .unwrap();
    let pa = a.create("Large", true);
    let code = a.code();
    b.pair(&a, &code);
    let pb = b.receive(&a, &pa);
    let bytes: Vec<_> = (0..6 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    write(&pa, "large.bin", &bytes);
    let mut child = b.child.take().unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    // The surviving worker still holds its state. Startup must adopt that
    // exact worker and recover the persistent outbound pool.
    b.start();
    wait_for(|| has(&pb, "large.bin", &bytes));
    assert_eq!(
        rpc(&pb, json!({"op":"cat-object","object":hash(&bytes)}))["hex"],
        hex::encode(&bytes)
    );
    a.stop();
    a.start();
    wait_for(|| {
        b.call(Action::Peers)[0]["lanes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|l| l["connected"] == true)
    });
    write(&pb, "after-restart.txt", b"same code, same repository");
    wait_for(|| has(&pa, "after-restart.txt", b"same code, same repository"));
}

/// An emulated 6298d0b acceptor: it upgrades any WebSocket (without the
/// `X-Tkfs-Sync` echo, which a 6298d0b server does not know) and answers a
/// legacy pairing hello. It records whether a pairing code ever reached it.
struct LegacyAcceptor {
    endpoint: String,
    connections: Arc<AtomicUsize>,
    saw_code: Arc<AtomicBool>,
}

impl LegacyAcceptor {
    fn start() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let connections = Arc::new(AtomicUsize::new(0));
        let saw_code = Arc::new(AtomicBool::new(false));
        let (count, seen) = (connections.clone(), saw_code.clone());
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                count.fetch_add(1, Ordering::SeqCst);
                let seen = seen.clone();
                thread::spawn(move || {
                    let Ok(mut socket) = ws::accept(stream) else {
                        return;
                    };
                    while let Ok(hello) = ws::receive::<Value>(&mut socket) {
                        if hello.get("code").is_some() {
                            seen.store(true, Ordering::SeqCst);
                        }
                        let reply = json!({"installation": id(), "version": 2,
                            "lane": hello["lane"], "bulk_protocol": 1,
                            "bulk_lane": hello["bulk_lane"]});
                        if ws::reply(&mut socket, Ok(reply)).is_err() {
                            return;
                        }
                    }
                });
            }
        });
        Self {
            endpoint,
            connections,
            saw_code,
        }
    }
}

/// WP3.1 replaces `pairing_refuses_a_peer_that_does_not_reserve_lanes` (a v2
/// rule): a 6298d0b peer fails closed with `PEER_NEEDS_UPGRADE` on both
/// sides (§6.2, invariant 20), and both daemons show it.
#[test]
fn legacy_peer_fails_closed_with_needs_upgrade() {
    let mut a = Node::new();
    let b = Node::new();
    // (a) A v3 node pairing with a v2 acceptor: refused before the code is
    // sent (no `X-Tkfs-Sync` echo), and nothing is stored.
    let legacy = LegacyAcceptor::start();
    let error = a
        .try_call(Action::Pair {
            endpoint: legacy.endpoint.clone(),
            code: "secret-code".into(),
            connections: Some(2),
        })
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("PEER_NEEDS_UPGRADE"),
        "{error:#}"
    );
    assert!(legacy.connections.load(Ordering::SeqCst) > 0);
    assert!(!legacy.saw_code.load(Ordering::SeqCst), "a code was sent");
    assert_eq!(a.call(Action::Peers), json!([]));
    // (b) A v2 dialer facing a v3 node: answered in the legacy format.
    let code = b.code();
    let mut socket = ws::connect(&b.endpoint()).unwrap();
    let refused = ws::call(
        &mut socket,
        &json!({"op":"pair","version":2,"code":code,"installation":id(),
            "lane":"control","bulk_protocol":1,"bulk_lane":null}),
    )
    .unwrap_err();
    assert!(
        format!("{refused:#}").contains("PEER_NEEDS_UPGRADE"),
        "{refused:#}"
    );
    // (c) Both daemons' Status show it: the dialer's refused pairing, the
    // acceptor's inbound attempt (never the code).
    for (node, side) in [(&a, "dialer"), (&b, "acceptor")] {
        wait_for(|| {
            node.status()["daemon"]["errors"]
                .as_array()
                .is_some_and(|errors| {
                    errors.iter().any(|record| {
                        record["error"]["code"] == "PEER_NEEDS_UPGRADE"
                            && record["resolved_at_ms"].is_null()
                    })
                })
        });
        let status = node.status().to_string();
        assert!(!status.contains(&code), "{side}: the code is in status");
        assert!(
            !status.contains("secret-code"),
            "{side}: the code is in status"
        );
    }
    // A peer paired before the upgrade (a 6298d0b data directory) that still
    // runs 6298d0b: the lanes stay Incompatible (no tight redial), Status and
    // the legacy Peers reply say so, and the code is never sent.
    a.stop();
    let peer = id();
    {
        let db =
            rusqlite::Connection::open(a.config.data_directory.join("folders.sqlite")).unwrap();
        db.execute(
            "INSERT INTO peers VALUES(?1,?2,?3,2,?4)",
            rusqlite::params![legacy.endpoint, "legacy-code", peer, id()],
        )
        .unwrap();
    }
    a.start();
    wait_for(|| {
        let status = a.status();
        status["peers"].as_array().is_some_and(|peers| {
            peers.iter().any(|entry| {
                entry["installation"] == peer.as_str()
                    && entry["state"] == "incompatible"
                    && entry["error"]["error"]["code"] == "PEER_NEEDS_UPGRADE"
            })
        })
    });
    let peers = a.call(Action::Peers);
    assert_eq!(peers[0]["installation"], peer.as_str());
    assert!(
        peers[0]["lanes"].as_array().unwrap().iter().any(|lane| {
            lane["connected"] == false
                && lane["error"]
                    .as_str()
                    .is_some_and(|error| error.contains("PEER_NEEDS_UPGRADE"))
        }),
        "{peers}"
    );
    let dials = legacy.connections.load(Ordering::SeqCst);
    thread::sleep(Duration::from_secs(3));
    assert!(
        legacy.connections.load(Ordering::SeqCst) <= dials + 1,
        "an incompatible peer is redialed in a loop"
    );
    assert!(!legacy.saw_code.load(Ordering::SeqCst), "a code was sent");
}

/// A v3 acceptor emulated in the test (`session::accept`): its folder is an
/// in-process worker (`runtime::start_rpc`) that serves the feed
/// (`feed-exchange`) and objects v3 over a loopback session (`objects.want`,
/// `objects.write`), as a daemon's inbound server would for the dialer's
/// `objects.wants` and `objects.push` (WP3.INT: the bulk lanes carry objects
/// v3 since WP3.3, no longer the interim `objects.exchange`). Its first
/// `objects.push` stalls for `stall` before its stream is read.
struct V3Acceptor {
    endpoint: String,
    code: String,
    sessions: Arc<Mutex<Vec<(LaneRole, Conn)>>>,
    stalled: mpsc::Receiver<()>,
}

struct AcceptorPolicy {
    installation: String,
    verifier: tkfs::pairing_code::Verifier,
}

impl AcceptPolicy for AcceptorPolicy {
    fn installation(&self) -> String {
        self.installation.clone()
    }
    fn verifier(&self) -> Option<(tkfs::pairing_code::Verifier, u64)> {
        Some((self.verifier.clone(), 1))
    }
    fn admit(&self, _: &ClientHello) -> Result<(), ErrorWire> {
        Ok(())
    }
    fn legacy_attempt(&self, _: std::net::SocketAddr, _: &Value) {}
}

struct AcceptorHandler {
    peer: AcceptedPeer,
    worker: Discovery,
    stall: Duration,
    stalled: Arc<AtomicBool>,
    blocked: mpsc::Sender<()>,
}

/// The emulated acceptor's lease session at its worker (one per test).
const ACCEPTOR_LEASE: &str = "push:test-acceptor";

impl AcceptorHandler {
    /// The worker's loopback session (objects v3, binary bodies).
    fn objects(&self) -> Result<Arc<SessionWorkerClient>, ErrorWire> {
        SessionWorkerClient::connect(&WorkerEndpoint {
            instance: format!("test-acceptor-{}", self.worker.device),
            pid: std::process::id(),
            address: self.worker.address.clone(),
            version: "test".into(),
            token: None,
        })
        .map_err(|error| error.to_wire())
    }
}

impl Handler for AcceptorHandler {
    fn handle(&self, req: Incoming) {
        let Incoming {
            op,
            header,
            body,
            chunks,
            responder,
            ..
        } = req;
        let worker = |payload: Value| {
            runtime::rpc(&self.worker, &id(), payload)
                .map_err(|error| ErrorWire::from_error(&error))
        };
        let route = |sender: &str| SyncRoute {
            peer: self.peer.installation.clone(),
            repo: self.worker.repo.clone(),
            local_replica: self.worker.device.clone(),
            remote_replica: sender.to_owned(),
        };
        let reply: Result<(Value, Vec<u8>), ErrorWire> = match op.as_str() {
            "ping" => Ok((json!({"alive": true}), vec![])),
            "catalog.list" => worker(json!({"op":"published-summary"})).map(|summary| {
                let summary: PublishedSummary = serde_json::from_value(summary).unwrap();
                let entry = PublishedEntry {
                    repo: summary.repo,
                    replica: summary.replica,
                    label: summary.label,
                    branches: summary.branches,
                    state: EntryState::Running,
                    tip: Some(summary.tip),
                    sync_generation: summary.sync_generation,
                    content: None,
                    error: None,
                    updated_at_ms: 0,
                };
                let reply = CatalogListReply {
                    generation: summary.sync_generation + 1,
                    unchanged: false,
                    entries: vec![entry],
                };
                (serde_json::to_value(reply).unwrap(), vec![])
            }),
            "meta.exchange" => {
                let request: MetaExchangeRequest = serde_json::from_slice(&body).unwrap();
                let route = route(&request.sender);
                worker(json!({"op":"feed-exchange","route":route,"request":request})).map(|value| {
                    let reply: FeedExchangeReply = serde_json::from_value(value).unwrap();
                    (
                        json!({"repo": route.repo}),
                        serde_json::to_vec(&reply.reply).unwrap(),
                    )
                })
            }
            "objects.wants" => {
                let request: ObjectsWantsRequest = serde_json::from_value(header).unwrap();
                let want = WantHeader {
                    route: route(&request.sender),
                    lane: request.lane,
                    max_items: request.max_items,
                    max_bytes: request.max_bytes,
                    session: ACCEPTOR_LEASE.into(),
                };
                self.objects().and_then(|client| {
                    client
                        .want(&want, Duration::from_secs(10))
                        .map_err(|error| error.to_wire())
                        .map(|items| {
                            let reply = ObjectsWantsReply { items };
                            (serde_json::to_value(reply).unwrap(), vec![])
                        })
                })
            }
            "objects.push" => {
                if !self.stalled.swap(true, Ordering::SeqCst) {
                    let _ = self.blocked.send(());
                    thread::sleep(self.stall);
                }
                let request: ObjectsPush = serde_json::from_value(header).unwrap();
                let route = route(&request.sender);
                self.objects().map(|client| {
                    // Each chunk is one write at its offset (the dialer sends
                    // every object in order); the last write's outcome is the
                    // object's.
                    let mut results: Vec<ObjectResult> = Vec::new();
                    for (chunk, bytes) in chunks.into_iter().flatten() {
                        let hash = chunk.hash.clone().unwrap_or_default();
                        let item = WriteItem {
                            hash: hash.clone(),
                            offset: chunk.offset,
                            total: chunk.total,
                            len: bytes.len() as u64,
                            last: chunk.last,
                        };
                        let len = item.len;
                        let outcome = match client.write_batch(
                            &route,
                            ACCEPTOR_LEASE,
                            vec![item],
                            bytes,
                            Duration::from_secs(60),
                        ) {
                            Ok(mut written) => written.remove(0).outcome,
                            Err(error) => ObjectOutcome::Failed(error.to_wire()),
                        };
                        match results.iter_mut().find(|result| result.hash == hash) {
                            Some(result) => {
                                result.outcome = outcome;
                                result.bytes += len;
                            }
                            None => results.push(ObjectResult {
                                hash,
                                outcome,
                                bytes: len,
                            }),
                        }
                    }
                    let reply = ObjectsResult { results };
                    (serde_json::to_value(reply).unwrap(), vec![])
                })
            }
            // The dialer holds everything this acceptor has: nothing to serve.
            "objects.pull" => Ok((
                serde_json::to_value(ObjectsResult { results: vec![] }).unwrap(),
                vec![],
            )),
            other => Err(ErrorWire::coded("UNKNOWN_PEER_OPERATION", other)),
        };
        match reply {
            Ok((header, body)) => responder.reply(header, body),
            Err(error) => responder.error(error),
        }
    }
}

impl V3Acceptor {
    fn start(worker: Discovery, stall: Duration) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let (code, verifier) = tkfs::pairing_code::Verifier::generate();
        let policy = Arc::new(AcceptorPolicy {
            installation: id(),
            verifier,
        });
        let sessions = Arc::new(Mutex::new(Vec::new()));
        let (blocked, stalled) = mpsc::channel();
        let stalled_once = Arc::new(AtomicBool::new(false));
        let kept = sessions.clone();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let (policy, kept, worker, blocked, stalled_once) = (
                    policy.clone(),
                    kept.clone(),
                    worker.clone(),
                    blocked.clone(),
                    stalled_once.clone(),
                );
                thread::spawn(move || {
                    let Ok((conn, peer)) = session::accept(
                        stream,
                        policy as Arc<dyn AcceptPolicy>,
                        ConnOptions::default(),
                    ) else {
                        return;
                    };
                    kept.lock().unwrap().push((peer.role, conn.clone()));
                    conn.serve(Arc::new(AcceptorHandler {
                        peer,
                        worker,
                        stall,
                        stalled: stalled_once,
                        blocked,
                    }));
                });
            }
        });
        Self {
            endpoint,
            code,
            sessions,
            stalled,
        }
    }

    /// Sessions accepted so far, by role.
    fn accepted(&self, role: LaneRole) -> usize {
        self.sessions
            .lock()
            .unwrap()
            .iter()
            .filter(|(accepted, _)| *accepted == role)
            .count()
    }
}

/// Rewritten by WP3.1 for v3 sessions: the acceptor's bulk handler stalls
/// 20 s (longer than the legacy lanes' 15 s timeout) while the control
/// session's `meta.exchange` is served by the acceptor's feed. The event
/// still arrives within 8 s, and no lane resets (no new session is dialed).
/// WP3.INT: the stalled bulk request is the objects v3 `objects.push`
/// (WP3.3 replaced the interim `objects.exchange` the WP3.1 version stalled).
#[test]
fn reserved_connection_delivers_events_while_bulk_is_blocked() {
    use tkfs::{core::Store, runtime::Engine};

    let mut node = Node::new();
    let project = node.create("Reserved lane", true);
    write(&project, "content.bin", &vec![3; 2 * 1024 * 1024]);
    let remote_root = tempfile::tempdir().unwrap();
    let remote = Arc::new(Mutex::new(Engine::new(
        Store::initialize(remote_root.path(), project["repo"].as_str().unwrap(), &id()).unwrap(),
    )));
    let worker = runtime::start_rpc(remote.clone(), remote_root.path(), None).unwrap();
    let acceptor = V3Acceptor::start(worker, Duration::from_secs(20));
    // One pairing probe, then one reserved (control) and one bulk session even
    // though the caller requested a pool of one.
    let paired = node.call(Action::Pair {
        endpoint: acceptor.endpoint.clone(),
        code: acceptor.code.clone(),
        connections: Some(1),
    });
    assert_eq!(paired["connections"], 2);
    acceptor
        .stalled
        .recv_timeout(Duration::from_secs(20))
        .unwrap();
    let stalled_at = Instant::now();
    rpc(&project, json!({"op":"mkdir","path":"during-bulk-stall"}));
    // An event created after the bulk exchange was blocked must arrive before
    // the stall ends. Its file-write ancestor is valid metadata even without
    // bytes; both the file and the new directory must already be visible.
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let delivered = remote
            .lock()
            .unwrap()
            .store
            .lookup("during-bulk-stall")
            .is_ok();
        if delivered {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "control lane starved by bulk transfer"
        );
        thread::sleep(Duration::from_millis(25));
    }
    assert!(stalled_at.elapsed() < Duration::from_secs(20));
    assert!(remote.lock().unwrap().store.lookup("content.bin").is_ok());
    assert_eq!(
        remote.lock().unwrap().store.content_status().unwrap()["complete"],
        false
    );
    let peers = node.call(Action::Peers);
    assert_eq!(peers[0]["reserved_connections"], 1);
    assert_eq!(peers[0]["configured_connections"], 1);
    assert_eq!(peers[0]["lanes"][0]["role"], "control");
    assert_eq!(peers[0]["lanes"][1]["role"], "bulk");
    assert!(peers[0]["lanes"][0]["exchanges"].as_u64().unwrap() > 0);
    // Once the stall ends the push is served on the same session.
    wait_for(|| remote.lock().unwrap().store.content_status().unwrap()["complete"] == true);
    assert!(stalled_at.elapsed() >= Duration::from_secs(19));
    assert_eq!(acceptor.accepted(LaneRole::Probe), 1);
    assert_eq!(
        acceptor.accepted(LaneRole::Control),
        1,
        "the control lane reset"
    );
    assert_eq!(acceptor.accepted(LaneRole::Bulk), 1, "the bulk lane reset");
    node.stop();
}

/// CCR-WP2.5-2 (need both ways): a dialer's event over `LARGE_EVENT_BYTES`
/// is only flagged in its request page; the acceptor asks for it in its
/// reply (`need`) and the dialer serves it in its next request
/// (`need_served`). The dialer books that next request at once, as it does
/// for `page.more`: under one-way pairing the event never waits for the
/// idle interval (20 s here). WP3.1 fix round 1: the dialer counted only
/// `page.more` as more work, so the event waited one full interval unless
/// something else (a later heartbeat, a remote tip) called for an exchange.
/// The test leaves nothing else: the event is made while B's sync is
/// paused, so resuming calls for exactly one exchange.
#[test]
fn a_dialers_large_event_reaches_the_acceptor_without_waiting_for_the_interval() {
    const DIRS: usize = 260;
    let a = Node::with_interval(20_000);
    let b = Node::with_interval(20_000);
    let pa = a.create("Large dialer event", true);
    // B dials A; A never dials B (one-way pairing).
    b.pair(&a, &a.code());
    let pb = b.receive(&a, &pa);
    let state = pb["state"].as_str().unwrap().to_owned();
    // B's current state: directories with 724-byte names, so that B's
    // shared fork of it (one event that lists them all) is a large event.
    let stem = "€".repeat(240);
    for index in 0..DIRS {
        rpc(
            &pb,
            json!({"op":"mkdir","path":format!("{stem}{index:04}")}),
        );
    }
    let dirs = |p: &Value| {
        rpc(p, json!({"op":"state"}))["entries"]
            .as_object()
            .map_or(0, |entries| {
                entries
                    .values()
                    .filter(|e| {
                        e["alive"] == true
                            && e["name"]
                                .as_str()
                                .is_some_and(|name| name.starts_with(&stem))
                    })
                    .count()
            })
    };
    wait_long("B's directories at A", Duration::from_secs(180), || {
        dirs(&pa) == DIRS
    });
    // B has seen A's final tip (B polls A's catalog every 2 s, and a changed
    // remote tip calls for an exchange).
    let tip_at = |node: &Node| {
        node.status()["folders"][0]["journal"]["tip"]
            .as_u64()
            .unwrap_or(0)
    };
    wait_long("B sees A's tip", Duration::from_secs(60), || {
        let peer = &b.status()["folders"][0]["sync"]["peers"][0]["metadata"];
        peer["remote_tip"].as_u64() == Some(tip_at(&a))
            && peer["to_send"] == 0
            && peer["to_receive"] == 0
    });
    thread::sleep(Duration::from_secs(3));
    // The fork, while B's sync is paused: B's heartbeats report the new tip
    // before the lane may use it.
    b.call(Action::PauseSync {
        state: state.clone(),
    });
    let before = tip_at(&b);
    rpc(
        &pb,
        json!({"op":"publish","name":"big","current_state_only":true}),
    );
    wait_long("B's tip after the fork", Duration::from_secs(30), || {
        tip_at(&b) == before + 1
    });
    thread::sleep(Duration::from_secs(3));
    let has_branch = |p: &Value| {
        rpc(p, json!({"op":"branches"}))
            .as_array()
            .is_some_and(|branches| {
                branches
                    .iter()
                    .any(|branch| branch["name"] == "big" && branch["shared"] == true)
            })
    };
    assert!(!has_branch(&pa));
    let started = Instant::now();
    b.call(Action::ResumeSync { state });
    wait_long("the large event at A", Duration::from_secs(90), || {
        has_branch(&pa)
    });
    let elapsed = started.elapsed();
    // It is one large event of B's, and A committed it.
    let on_big = |p: &Value| -> Vec<Value> {
        rpc(p, json!({"op":"events"}))
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| event["branch"]["name"] == "big")
            .cloned()
            .collect()
    };
    let sent = on_big(&pb);
    assert_eq!(sent.len(), 1);
    let size = serde_json::to_string(&sent[0]).unwrap().len();
    assert!(size > LARGE_EVENT_BYTES, "the fork is {size} bytes");
    let committed = on_big(&pa);
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0]["id"], sent[0]["id"]);
    assert!(
        elapsed < Duration::from_secs(10),
        "the large event took {elapsed:?} to reach the acceptor (idle interval 20 s)"
    );
}

/// `catalog.list` on a v3 session.
fn list_catalog(conn: &Conn) -> Result<Reply, tkfs::model::orchestrator::CallError> {
    conn.call(
        "catalog.list",
        json!({}),
        vec![],
        Priority::Control,
        Duration::from_secs(10),
    )
}
#[cfg(any(windows, target_os = "linux"))]
#[test]
fn real_partial_mount_fetches_on_read_without_blocking_unrelated_metadata() {
    #[cfg(target_os = "linux")]
    if !Path::new("/dev/fuse").exists() {
        return;
    }
    let a = Node::new();
    let b = Node::new();
    let pa = a.create("Demand source", true);
    let bytes = b"remote content fetched after source returns";
    write(&pa, "remote.txt", bytes);
    b.pair(&a, &a.code());
    let pb = b.receive(&a, &pa);
    // `content.complete` is vacuously true before the event commits, so also
    // require the object to be installed before it is deleted below
    // (CCR-WP0.5-3, applied by WP0.INT).
    wait_for(|| {
        let content = rpc(&pb, json!({"op":"status"}))["content"].clone();
        content["complete"] == true
            && content["current_available"].as_u64().unwrap_or(0) >= 1
            && has(&pb, "remote.txt", bytes)
    });
    a.call(Action::Stop {
        state: pa["state"].as_str().unwrap().into(),
        wait: None,
    });
    let state = pb["state"].as_str().unwrap().to_owned();
    b.call(Action::Stop {
        state: state.clone(),
        wait: None,
    });
    fs::remove_file(
        Path::new(pb["directory"].as_str().unwrap())
            .join("objects")
            .join(hash(bytes)),
    )
    .unwrap();
    let mount = b.root.path().join("partial-mount");
    b.call(Action::ConfigureMount {
        state: state.clone(),
        mount: Some(mount.clone()),
    });
    let pb = b.call(Action::Start { state });
    assert_eq!(
        fs::metadata(mount.join("remote.txt")).unwrap().len(),
        bytes.len() as u64
    );
    assert!(
        fs::read_dir(&mount)
            .unwrap()
            .any(|e| e.unwrap().file_name() == "remote.txt")
    );
    let reader_path = mount.join("remote.txt");
    let (tx, rx) = std::sync::mpsc::channel();
    let reader = thread::spawn(move || {
        let _ = tx.send(fs::read(reader_path));
    });
    wait_for(|| {
        rpc(&pb, json!({"op":"status"}))["content"]["requested"]
            .as_u64()
            .unwrap_or(0)
            > 0
    });
    fs::create_dir(mount.join("while-downloading")).unwrap();
    assert!(
        fs::metadata(mount.join("while-downloading"))
            .unwrap()
            .is_dir()
    );
    a.call(Action::Start {
        state: pa["state"].as_str().unwrap().into(),
    });
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(15)).unwrap().unwrap(),
        bytes
    );
    reader.join().unwrap();
}

#[cfg(any(windows, target_os = "linux"))]
#[test]
fn received_mount_waits_for_complete_metadata_across_daemon_restart() {
    #[cfg(target_os = "linux")]
    if !Path::new("/dev/fuse").exists() {
        return;
    }
    use tkfs::{
        core::Store,
        paired_sync::{self, SyncRoute},
        runtime::Engine,
    };
    let a = Node::new();
    let mut b = Node::new();
    let pa = a.create("Bootstrap source", true);
    let original = tempfile::tempdir().unwrap();
    let mut author = Engine::new(
        Store::initialize(original.path(), pa["repo"].as_str().unwrap(), &id()).unwrap(),
    );
    author.store.create("parent", "directory").unwrap();
    author.store.create("parent/child", "directory").unwrap();
    let route = SyncRoute {
        peer: id(),
        repo: author.store.repo.clone(),
        local_replica: author.store.device.clone(),
        remote_replica: pa["device"].as_str().unwrap().into(),
    };
    let reverse = SyncRoute {
        local_replica: route.remote_replica.clone(),
        remote_replica: route.local_replica.clone(),
        ..route.clone()
    };
    let full = paired_sync::offer(&author, &route, None).unwrap();
    let mut incomplete = full.clone();
    incomplete
        .bundle
        .events
        .retain(|e| !e.dependencies.is_empty());
    assert_eq!(incomplete.bundle.events.len(), 1);
    rpc(
        &pa,
        json!({"op":"published-exchange","route":reverse,"page":incomplete,"metadata_only":true}),
    );
    assert_eq!(rpc(&pa, json!({"op":"status"}))["incoming_pending"], 1);
    b.pair(&a, &a.code());
    let mount = b.root.path().join("received");
    let pb = b
        .try_call(Action::Receive {
            endpoint: a.endpoint(),
            repo: pa["repo"].as_str().unwrap().into(),
            label: "Received".into(),
            mount: Some(mount.clone()),
            headless: false,
        })
        .unwrap();
    let state = pb["state"].as_str().unwrap().to_owned();
    let inspect = |node: &Node| {
        node.try_call(Action::Inspect {
            state: state.clone(),
        })
        .unwrap()
    };
    wait_for(|| inspect(&b)["startup_phase"] == "synchronizing remote metadata");
    wait_for(|| inspect(&b)["sync"][0]["metadata"]["last_success_ms"].is_number());
    assert_eq!(inspect(&b)["running"], false);
    assert_eq!(inspect(&b)["mounted"], false);
    assert!(!mount.exists());
    // Waiting for remote prerequisites is not a worker-start timeout, and its
    // durable marker must survive a supervisor restart.
    b.stop();
    b.start();
    wait_for(|| inspect(&b)["startup_phase"] == "synchronizing remote metadata");
    assert_eq!(inspect(&b)["initial_sync_pending"], true);
    assert!(!mount.exists());
    // The gated worker runs already (RPC first, the mount attached in place
    // once the gate is satisfied): its pid is the same after the attach.
    let worker_pid = || {
        let record: Value = serde_json::from_slice(
            &fs::read(Path::new(pb["directory"].as_str().unwrap()).join("worker.json")).unwrap(),
        )
        .unwrap();
        record["pid"].as_u64().unwrap()
    };
    let gated_pid = worker_pid();
    rpc(
        &pa,
        json!({"op":"published-exchange","route":reverse,"page":full,"metadata_only":true}),
    );
    wait_for(|| inspect(&b)["running"] == true);
    assert_eq!(inspect(&b)["initial_sync_pending"], false);
    assert_eq!(inspect(&b)["mounted"], true);
    assert_eq!(worker_pid(), gated_pid, "the worker restarted to attach");
    assert!(mount.join("parent/child").is_dir());
    // Once initialized, remounting remains possible with the source stopped.
    a.call(Action::Stop {
        state: pa["state"].as_str().unwrap().into(),
        wait: None,
    });
    b.call(Action::Stop {
        state: state.clone(),
        wait: None,
    });
    b.call(Action::Start { state });
    assert!(mount.join("parent/child").is_dir());
}

// ---------------------------------------------------------------------------
// WP3.1: real daemons over a faulty v3 link

#[allow(dead_code)]
#[path = "support/proxy.rs"]
mod proxy;
#[path = "v3_wire_tap/mod.rs"]
mod wire_tap;

/// Deterministic bytes that do not compress.
fn noise(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

/// `name` holds `bytes`, and the object is installed (read back from the
/// store; the status cell's `content.complete` may lag a new event).
fn has_content(p: &Value, name: &str, bytes: &[u8]) -> bool {
    has(p, name, bytes) && object_is(p, bytes)
}

/// The object of `bytes`, read back from the folder's store, is `bytes`.
fn object_is(p: &Value, bytes: &[u8]) -> bool {
    runtime::rpc(
        &info(p),
        &id(),
        json!({"op":"cat-object","object":hash(bytes)}),
    )
    .is_ok_and(|object| object["hex"] == hex::encode(bytes))
}

#[track_caller]
fn wait_long(what: &str, timeout: Duration, mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !predicate() {
        assert!(Instant::now() < deadline, "timeout: {what}");
        thread::sleep(Duration::from_millis(100));
    }
}

/// Two real daemons sync over v3 through a faulty link (`FaultProxy`): added
/// latency and a bandwidth cap, a stall longer than the sessions' 20 s
/// liveness bound (every session dies and is redialed), and resets in the
/// middle of a transfer. Sync never sticks, in either direction, and no
/// request is ever sent twice: a tap in front of the proxy reads the
/// dialer's wire and sees every exchange identity once (invariant 16).
#[test]
fn sync_survives_latency_bandwidth_stalls_and_resets_without_replays() {
    let a = Node::new();
    let b = Node::new();
    let proxy = proxy::FaultProxy::start(&a.endpoint());
    let tap = wire_tap::WireTap::start(&proxy.endpoint());
    let fa = a.create("Faulty link", true);
    write(&fa, "seed.txt", b"seed");
    b.call(Action::Pair {
        endpoint: tap.endpoint(),
        code: a.code(),
        connections: Some(3),
    });
    let fb = b.call(Action::Receive {
        endpoint: tap.endpoint(),
        repo: fa["repo"].as_str().unwrap().into(),
        label: "Faulty link".into(),
        mount: None,
        headless: true,
    });
    assert_eq!(fb["running"], true, "{fb}");
    wait_long("the seed at B", Duration::from_secs(60), || {
        has(&fb, "seed.txt", b"seed")
    });

    // 1. 40 ms RTT and 1 MiB/s each way: an object and events, both ways.
    proxy.set_latency(Duration::from_millis(20));
    proxy.set_bandwidth(1024 * 1024);
    let big = noise(1, 2 * 1024 * 1024);
    write(&fa, "big.bin", &big);
    write(&fb, "from-b.txt", b"written at B");
    wait_long(
        "both directions under latency",
        Duration::from_secs(90),
        || has_content(&fb, "big.bin", &big) && has(&fa, "from-b.txt", b"written at B"),
    );
    assert!(object_is(&fb, &big));

    // 2. A 22 s stall: no frame crosses the link for longer than the 20 s
    // liveness bound, so every session is declared dead and redialed once
    // the link is back. What was written meanwhile arrives, both ways.
    let before_stall = proxy.connections();
    proxy.stall_for(Duration::from_secs(22));
    let stalled = Instant::now();
    write(&fa, "during-stall-a.txt", b"written at A during the stall");
    write(&fb, "during-stall-b.txt", b"written at B during the stall");
    wait_long(
        "the writes made during the stall",
        Duration::from_secs(90),
        || {
            has(&fb, "during-stall-a.txt", b"written at A during the stall")
                && has(&fa, "during-stall-b.txt", b"written at B during the stall")
        },
    );
    assert!(stalled.elapsed() >= Duration::from_secs(20));
    assert!(
        proxy.connections() > before_stall,
        "the dead sessions were not replaced"
    );

    // 3. Resets in the middle of a transfer: the transfer completes on new
    // sessions (the uncertain exchanges are not replayed: fresh requests).
    proxy.set_bandwidth(2 * 1024 * 1024);
    let resets = proxy.resets();
    let large = noise(2, 3 * 1024 * 1024);
    write(&fa, "reset.bin", &large);
    proxy.reset_after(512 * 1024);
    wait_long(
        "the transfer cut by a reset",
        Duration::from_secs(120),
        || has_content(&fb, "reset.bin", &large),
    );
    assert!(proxy.resets() > resets, "no connection was reset");
    assert!(object_is(&fb, &large));
    proxy.reset_all();
    write(&fb, "after-reset.txt", b"after the resets");
    wait_long("a write after the resets", Duration::from_secs(60), || {
        has(&fa, "after-reset.txt", b"after the resets")
    });

    // 4. A clean link: nothing is left stuck.
    proxy.clear();
    write(&fa, "clean-a.txt", b"clean A");
    write(&fb, "clean-b.txt", b"clean B");
    wait_long(
        "both directions on a clean link",
        Duration::from_secs(60),
        || has(&fb, "clean-a.txt", b"clean A") && has(&fa, "clean-b.txt", b"clean B"),
    );

    // Nothing was ever sent twice.
    assert!(tap.count("meta.exchange") > 0, "{:?}", tap.seen().len());
    // WP3.INT: objects travel as objects v3 (WP3.3), never through the
    // interim `objects.exchange` fallback.
    assert!(tap.objects_v3() > 0);
    assert_eq!(tap.count("objects.exchange"), 0);
    assert_eq!(tap.malformed(), 0, "the tap lost the dialer's stream");
    // Every exchange request was identified, so the check below covers
    // replays across connections too (not only reused session ids).
    let unidentified = tap.unidentified();
    assert!(unidentified.is_empty(), "{unidentified:#?}");
    let duplicates = tap.duplicates();
    assert!(duplicates.is_empty(), "{duplicates:#?}");
}

/// WP3.1 fix round 2: a Shutdown whose sequence panics before its network
/// step (§3.6 step 2) still stops the network before the daemon ends. Both
/// stacks' tokens are children of `ctx.shutdown`, so they stop accepting by
/// themselves, but only `DaemonNetwork::shutdown` drains the inbound
/// sessions and joins the peer supervisors within the deadline; and they
/// hold the context, so the daemon letting go of its handle never did. Once
/// `serve_with` returned, the network has been shut down (both stacks taken
/// and stopped) and nothing accepts on the peer listener. In-process,
/// because the fault is injected in this process (no other test of this
/// file runs a daemon in-process).
#[test]
fn a_shutdown_sequence_that_panics_still_stops_the_network() {
    use tkfs::util::fault::{self, FaultAction, FaultConfig};
    let root = tempfile::tempdir().unwrap();
    let config = Config {
        format_version: 2,
        data_directory: root.path().join("data"),
        default_mount_directory: root.path().join("mounts"),
        maximum_running: 8,
        network: orchestrator::Network {
            listen: Some("127.0.0.1:0".into()),
            connections: 2,
            sync_interval_ms: 500,
        },
    };
    let daemon = orchestrator::ctx::build(config).unwrap();
    let endpoints = daemon.ctx.endpoints.load();
    let control = endpoints.control.clone();
    let network: std::net::SocketAddr = endpoints
        .network
        .as_deref()
        .and_then(|endpoint| endpoint.strip_prefix("ws://"))
        .expect("a peer listener")
        .parse()
        .unwrap();
    let net = daemon.net.clone();
    let server = thread::spawn(move || orchestrator::serve_with(daemon, false));
    let accepts = || std::net::TcpStream::connect_timeout(&network, Duration::from_secs(5)).is_ok();
    assert!(accepts(), "the peer listener does not accept");
    fault::configure(Some(FaultConfig::new(
        orchestrator::shutdown::faults::SEQUENCE,
        FaultAction::Panic,
    )));
    let answered = ws::connect(&control)
        .and_then(|mut socket| ws::call(&mut socket, &json!({"op":"shutdown"})));
    fault::configure(None);
    assert!(
        answered.is_err(),
        "the sequence did not panic: {answered:?}"
    );
    let until = Instant::now() + Duration::from_secs(30);
    while !server.is_finished() {
        assert!(Instant::now() < until, "the daemon did not end");
        thread::sleep(Duration::from_millis(50));
    }
    server.join().unwrap().unwrap();
    assert!(
        net.inbound().is_none() && net.peers().is_none(),
        "the daemon ended without shutting its network down: {net:?}"
    );
    assert!(
        !accepts(),
        "the peer listener still accepts after the daemon ended"
    );
}
