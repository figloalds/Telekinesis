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
        let root = tempfile::tempdir().unwrap();
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(root.path(), fs::Permissions::from_mode(0o777)).unwrap();
        }
        let path = root.path().join("orchestrator.toml");
        fs::write(&path,"format_version=2\ndata_directory='data'\ndefault_mount_directory='mounts'\nmaximum_running=8\n[network]\nlisten='127.0.0.1:0'\nconnections=3\nsync_interval_ms=50\n").unwrap();
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
        self.try_call(action).unwrap()
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
fn wait_for(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !predicate() {
        assert!(Instant::now() < deadline, "condition timeout");
        thread::sleep(Duration::from_millis(50));
    }
}
fn has(p: &Value, name: &str, bytes: &[u8]) -> bool {
    let Ok(state) = runtime::rpc(&info(p), &id(), json!({"op":"state"})) else {
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
    assert!(ws::connect("wss://127.0.0.1:1").is_err());
    assert!(ws::connect("tcp://127.0.0.1:1").is_err());
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
