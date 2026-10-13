use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};
use tkfs::{
    core::{hash, id},
    orchestrator::{self, Action, Config},
    runtime::{self, Discovery},
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
        assert_eq!(lane["transport"], "tkfs-binary-gzip-v1");
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
    let mut socket = ws::connect(&a.endpoint()).unwrap();
    ws::call(
        &mut socket,
        &json!({"op":"pair","version":2,"code":code,"installation":id()}),
    )
    .unwrap();
    ws::call(&mut socket, &json!({"op":"list"})).unwrap();
    let next = a.code();
    assert_ne!(next, code);
    assert!(
        ws::call(&mut socket, &json!({"op":"list"}))
            .unwrap_err()
            .to_string()
            .contains("PAIRING_CODE_CHANGED")
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
    wait_for(|| {
        let peers = b.call(Action::Peers);
        let lane = &peers[0]["lanes"][1];
        lane["connected"] == true
            && lane["repositories"][repo]["error"]
                .as_str()
                .is_some_and(|e| e.contains("CORRUPT_OBJECT"))
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
    wait_for(|| has(&broken_b, "repair.txt", bytes));
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
    wait_for(|| {
        b.call(Action::Peers)[0]["lanes"][1]["repositories"][repo]["error"]
            .as_str()
            .is_some_and(|e| e.contains("CORRUPT_OBJECT"))
    });
    write(&healthy, "still.txt", b"healthy during local failure");
    wait_for(|| has(&healthy_b, "still.txt", b"healthy during local failure"));
    fs::write(&local_object, local_bytes).unwrap();
    wait_for(|| has(&broken, "local-repair.txt", local_bytes));
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
    let a = Node::new();
    let b = Node::new();
    let unavailable = a.create("A unavailable", true);
    let healthy = a.create("Z healthy", true);
    b.pair(&a, &a.code());
    let path = Path::new(unavailable["directory"].as_str().unwrap()).join("runtime.json");
    let original = fs::read(&path).unwrap();
    let mut discovery = info(&unavailable);
    discovery.address = "ws://127.0.0.1:0".into();
    fs::write(&path, serde_json::to_vec(&discovery).unwrap()).unwrap();
    let result = b.try_call(Action::RemoteList {
        endpoint: a.endpoint(),
    });
    let inspected = a.call(Action::Inspect {
        state: unavailable["state"].as_str().unwrap().into(),
    });
    fs::write(&path, original).unwrap();
    let published = result.unwrap();
    assert!(
        published
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["repo"] == healthy["repo"])
    );
    assert!(
        !published
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["repo"] == unavailable["repo"])
    );
    assert!(
        inspected["publication_error"]
            .as_str()
            .unwrap()
            .contains("PUBLISHED_SUMMARY_FAILED")
    );
    wait_for(|| {
        let published = b.call(Action::RemoteList {
            endpoint: a.endpoint(),
        });
        published.as_array().unwrap().len() == 2
            && a.call(Action::Inspect {
                state: unavailable["state"].as_str().unwrap().into(),
            })["publication_error"]
                .is_null()
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
    // The surviving worker still holds its state. Startup must stop/restart
    // that exact worker and recover the persistent outbound pool.
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

#[test]
fn pairing_refuses_a_peer_that_does_not_reserve_lanes() {
    let node = Node::new();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        let mut socket = ws::accept(listener.accept().unwrap().0).unwrap();
        let hello: Value = ws::receive(&mut socket).unwrap();
        assert_eq!(hello["lane"], "control");
        ws::reply(&mut socket, Ok(json!({"installation":id(),"version":2}))).unwrap();
    });
    let error = node
        .try_call(Action::Pair {
            endpoint,
            code: "test".into(),
            connections: None,
        })
        .unwrap_err();
    assert!(error.to_string().contains("RESERVED_LANES_UNSUPPORTED"));
    assert_eq!(node.call(Action::Peers), json!([]));
    server.join().unwrap();
}

#[test]
fn reserved_connection_delivers_events_while_bulk_is_blocked() {
    use std::{
        net::TcpListener,
        sync::{Arc, Mutex, mpsc},
    };
    use tkfs::{
        core::Store,
        paired_sync::{self, SyncPage, SyncRoute},
        runtime::Engine,
    };

    let mut node = Node::new();
    let project = node.create("Reserved lane", true);
    write(&project, "content.bin", &vec![3; 2 * 1024 * 1024]);
    let remote_root = tempfile::tempdir().unwrap();
    let remote = Arc::new(Mutex::new(Engine::new(
        Store::initialize(remote_root.path(), project["repo"].as_str().unwrap(), &id()).unwrap(),
    )));
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let (blocked_tx, blocked_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let server_remote = remote.clone();
    let server =
        thread::spawn(move || {
            let installation = id();
            let mut handlers = Vec::new();
            // One pairing probe, then one reserved and one bulk connection even
            // though the caller requested a pool of one.
            for _ in 0..3 {
                let (tcp, _) = listener.accept().unwrap();
                let remote = server_remote.clone();
                let installation = installation.clone();
                let blocked = blocked_tx.clone();
                let release = release_rx.clone();
                handlers.push(thread::spawn(move || {
                    let mut socket = ws::accept(tcp).unwrap();
                    let hello: Value = ws::receive(&mut socket).unwrap();
                    let role = hello["lane"].as_str().unwrap_or("probe");
                    ws::reply(
                        &mut socket,
                        Ok(json!({"installation":installation,"version":2,"lane":hello["lane"]})),
                    )
                    .unwrap();
                    let mut stalled = false;
                    while let Ok(request) = ws::receive::<Value>(&mut socket) {
                        if role == "bulk" && request["op"] == "exchange" && !stalled {
                            blocked.send(()).unwrap();
                            release
                                .lock()
                                .unwrap()
                                .recv_timeout(Duration::from_secs(15))
                                .unwrap();
                            stalled = true;
                        }
                        let mut engine = remote.lock().unwrap();
                        let response =
                            match request["op"].as_str().unwrap() {
                                "list" => {
                                    assert_eq!(role, "control");
                                    json!([paired_sync::summary(&engine).unwrap()])
                                }
                                "ping" => json!({"alive":true}),
                                "exchange" => {
                                    let page: SyncPage =
                                        serde_json::from_value(request["page"].clone()).unwrap();
                                    if role == "control" {
                                        assert!(page.bundle.objects.is_empty());
                                        assert!(page.object_parts.is_empty());
                                    }
                                    let route = SyncRoute {
                                        peer: hello["installation"].as_str().unwrap().into(),
                                        repo: engine.store.repo.clone(),
                                        local_replica: engine.store.device.clone(),
                                        remote_replica: page.sender.clone(),
                                    };
                                    paired_sync::worker_control(&mut engine, &json!({
                                "op":"published-exchange","route":route,"page":page,
                                "metadata_only":role == "control","max_frame":4*1024*1024
                            })).unwrap()
                                }
                                other => panic!("unexpected request {other}"),
                            };
                        drop(engine);
                        if ws::reply(&mut socket, Ok(response)).is_err() {
                            break;
                        }
                    }
                }));
            }
            for handler in handlers {
                handler.join().unwrap();
            }
        });
    let paired = node.call(Action::Pair {
        endpoint,
        code: "test".into(),
        connections: Some(1),
    });
    assert_eq!(paired["connections"], 2);
    blocked_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    rpc(&project, json!({"op":"mkdir","path":"during-bulk-stall"}));
    // An event created after the bulk exchange was blocked must arrive before
    // release. Its file-write ancestor is valid metadata even without bytes;
    // both the file and the new directory must already be visible.
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let delivered: bool = remote
            .lock()
            .unwrap()
            .store
            .db
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM events WHERE payload LIKE '%during-bulk-stall%')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        if delivered {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "control lane starved by bulk transfer"
        );
        thread::sleep(Duration::from_millis(25));
    }
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
    release_tx.send(()).unwrap();
    wait_for(|| remote.lock().unwrap().store.content_status().unwrap()["complete"] == true);
    wait_for(|| {
        remote
            .lock()
            .unwrap()
            .store
            .lookup("during-bulk-stall")
            .is_ok()
    });
    node.stop();
    server.join().unwrap();
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
    rpc(
        &pa,
        json!({"op":"published-exchange","route":reverse,"page":full,"metadata_only":true}),
    );
    wait_for(|| inspect(&b)["running"] == true);
    assert_eq!(inspect(&b)["initial_sync_pending"], false);
    assert_eq!(inspect(&b)["mounted"], true);
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
