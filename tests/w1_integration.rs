//! Owned by WP1.INT (REVAMP_PLAN §14 WP1.INT task 2): checks that need two
//! Wave 1 packages at once.
//!
//! - `RealLauncher` (WP1.3) launches the real `daemon --supervised` (WP1.4),
//!   a `FolderRegistry` (WP1.2) over a real `Catalog` (WP1.1) with a
//!   `StatusHub` (WP1.11) runs it to `Running`, and after a daemon crash a new
//!   registry adopts the same worker.
//! - `session::dial` ↔ `session::accept` run the real handshake (WP1.10) and
//!   hand the authenticated socket to a running `Conn` (WP1.9).
//! - The worker `revalidate` RPC (WP1.4's arm, WP1.6's `Store::revalidate`,
//!   wired by WP1.INT, task 1).
//! - Interoperability with a 6298d0b peer (§13 rule 8.4): `#[ignore]`d, run
//!   with `TKFS_BASELINE_BIN` pointing at a 6298d0b `tkfs` binary.
mod support;

use serde_json::{Value, json};
use std::{
    fs,
    io::{BufRead, BufReader},
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};
use support::{
    Control, Node, NodeConfig, WorkerProc, copy_legacy_worker, has_file, kill_pid, retry_busy,
    state_id, tail, try_rpc, wait_until,
};
use tkfs::{
    core::{Store, id},
    model::{
        errors::ErrorWire,
        orchestrator::{
            CallError, CatalogApi, CatalogCmd, FolderCmd, FolderDirectory, FolderPhase,
            FolderRecord, StatusSource,
        },
        protocol::{LaneRole, frame::ClientHello},
        status::LifecyclePhase,
    },
    orchestrator::{
        Config, Network,
        catalog::Catalog,
        hub::{HubConfig, StatusHub},
        process::{LogPolicy, RealLauncher, process_identity},
        published::PublishedCatalog,
        registry::{FolderRegistry, LifecycleLimits, RegistryDeps},
    },
    pairing_code::Verifier,
    session::{
        self, AcceptPolicy, CodeAuth, ConnOptions, Handler, Incoming, Priority,
        handshake::client_hello,
    },
    util::time::SystemClock,
    worker::record,
};

/// Child-process mode of this binary (see [`run_child`]).
const CHILD_ENV: &str = "TKFS_W1_CHILD";
/// The data root a child process works in.
const ROOT_ENV: &str = "TKFS_W1_ROOT";

fn tkfs_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_tkfs"))
}

fn config(root: &Path) -> Config {
    Config {
        format_version: 2,
        data_directory: root.join("data"),
        default_mount_directory: root.join("mounts"),
        maximum_running: 8,
        network: Network {
            listen: None,
            ..Network::default()
        },
    }
}

fn start_registry(catalog: Arc<Catalog>, hub: Arc<StatusHub>) -> Arc<FolderRegistry> {
    FolderRegistry::start(RegistryDeps {
        catalog,
        published: PublishedCatalog::new(),
        status: hub,
        launcher: RealLauncher::new(LogPolicy::default()),
        clock: Arc::new(SystemClock),
        limits: LifecycleLimits::new(tkfs_bin(), 8),
    })
}

fn hub() -> Arc<StatusHub> {
    let hub = StatusHub::new(
        HubConfig::for_sync_interval(Duration::from_millis(500)),
        Arc::new(SystemClock),
    );
    let _ticker = hub.spawn_ticker(tkfs::util::Cancel::new());
    hub
}

/// Waits up to `timeout` for the folder to be `Running`; returns
/// `(pid, instance, adopted)`.
fn wait_running(registry: &FolderRegistry, state: &str, timeout: Duration) -> (u32, String, bool) {
    let deadline = Instant::now() + timeout;
    loop {
        let view = registry.view(state);
        if let Some(FolderPhase::Running {
            pid,
            instance,
            adopted,
            ..
        }) = view.as_ref().map(|view| view.phase.clone())
        {
            return (pid, instance, adopted);
        }
        assert!(
            Instant::now() < deadline,
            "folder {state} not running within {timeout:?}: {view:#?}"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

/// Waits until the hub shows the folder's lifecycle as `Running` with `pid`.
fn wait_hub_running(hub: &StatusHub, state: &str, pid: u32) {
    let ok = wait_until(Duration::from_secs(10), || {
        hub.snapshot().folders.iter().any(|folder| {
            folder.state == state
                && folder.lifecycle.phase == LifecyclePhase::Running
                && folder.lifecycle.pid == Some(pid)
        })
    });
    assert!(ok, "hub: {:#?}", hub.snapshot().folders);
}

/// Runs one test of this binary in a child process with `CHILD_ENV=mode` and
/// returns its exit status, its stdout (read until it exited) and its stderr
/// log. A worker the child launches can inherit the child's stdout (Windows
/// handle inheritance), so the pipe is never read to its end: a reader thread
/// collects lines, and the child's own exit ends the wait (120 s at most).
fn run_child(test: &str, mode: &str, root: &Path) -> (ExitStatus, String, String) {
    let log = root.join(format!("{mode}.stderr.log"));
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([test, "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, mode)
        .env(ROOT_ENV, root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(fs::File::create(&log).unwrap())
        .spawn()
        .unwrap();
    let lines = Arc::new(Mutex::new(String::new()));
    let reader = child.stdout.take().unwrap();
    let collected = lines.clone();
    thread::spawn(move || {
        for line in BufReader::new(reader).lines() {
            let Ok(line) = line else { break };
            let mut all = collected.lock().unwrap();
            all.push_str(&line);
            all.push('\n');
        }
    });
    let deadline = Instant::now() + Duration::from_secs(120);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            break child.wait().unwrap();
        }
        thread::sleep(Duration::from_millis(50));
    };
    // Lines printed just before the exit may still be in flight.
    wait_until(Duration::from_secs(2), || {
        lines.lock().unwrap().contains("RUNNING ")
    });
    let stdout = lines.lock().unwrap().clone();
    (status, stdout, tail(&log, 16 * 1024))
}

/// Kills a worker left behind by a failed assertion.
struct KillOnDrop(Option<u32>);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        if let Some(pid) = self.0
            && process_identity(pid).is_some()
        {
            kill_pid(pid);
        }
    }
}

/// The first "daemon" (a child process): creates the folder in a real
/// catalog, starts it through the registry with the real launcher (the real
/// `daemon --supervised`), checks the hub and `worker.json`, prints the
/// worker, then exits without a shutdown, as a crashed daemon would.
fn first_daemon(root: &Path) {
    let config = config(root);
    let catalog = Catalog::open(&config).unwrap();
    let state = id();
    let record = FolderRecord {
        directory: config.data_directory.join("states").join(&state),
        state: state.clone(),
        label: "Wave 1".into(),
        repo: id(),
        device: id(),
        mount: None,
        desired_running: true,
        sync_enabled: true,
        mount_enabled: true,
    };
    Store::initialize(&record.directory, &record.repo, &record.device).unwrap();
    catalog
        .apply(CatalogCmd::CreateFolder {
            record: record.clone(),
            gate: None,
        })
        .unwrap();
    let hub = hub();
    let registry = start_registry(catalog.clone(), hub.clone());
    let (pid, instance, adopted) = wait_running(&registry, &state, Duration::from_secs(60));
    assert!(!adopted, "a fresh worker is spawned, not adopted");
    wait_hub_running(&hub, &state, pid);
    let written = record::read(&record.directory)
        .unwrap()
        .expect("worker.json");
    assert_eq!(
        (written.pid, written.instance.as_str()),
        (pid, instance.as_str())
    );
    assert!(written.supervised);
    let hello = registry
        .client(&state)
        .expect("a running folder has a client")
        .call("hello", json!({}), Duration::from_secs(5))
        .unwrap();
    assert_eq!(hello["instance"], instance);
    println!("RUNNING {state} {pid} {instance}");
    // A daemon crash: no Shutdown. The worker outlives this process.
    std::process::exit(0);
}

#[test]
fn real_launcher_registry_and_adoption_after_a_daemon_crash() {
    if std::env::var(CHILD_ENV).as_deref() == Ok("first_daemon") {
        first_daemon(Path::new(&std::env::var(ROOT_ENV).unwrap()));
        return;
    }
    if std::env::var(CHILD_ENV).is_ok() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let (status, stdout, stderr) = run_child(
        "real_launcher_registry_and_adoption_after_a_daemon_crash",
        "first_daemon",
        root.path(),
    );
    let line = stdout
        .lines()
        .find_map(|line| line.find("RUNNING ").map(|at| &line[at..]))
        .unwrap_or_else(|| panic!("first daemon failed ({status}):\n{stdout}\n{stderr}"));
    let fields: Vec<&str> = line.split_whitespace().collect();
    let (state, pid, instance) = (fields[1], fields[2].parse::<u32>().unwrap(), fields[3]);
    let mut guard = KillOnDrop(Some(pid));
    assert!(
        status.success(),
        "first daemon exit {status}:\n{stdout}\n{stderr}"
    );
    assert!(
        process_identity(pid).is_some(),
        "the worker outlives the crashed daemon"
    );

    // The next daemon: same catalog, a new registry adopts the worker.
    let config = config(root.path());
    let catalog = Catalog::open(&config).unwrap();
    let hub = hub();
    let registry = start_registry(catalog.clone(), hub.clone());
    let (adopted_pid, adopted_instance, adopted) =
        wait_running(&registry, state, Duration::from_secs(60));
    assert!(adopted, "the running worker is adopted, not replaced");
    assert_eq!((adopted_pid, adopted_instance.as_str()), (pid, instance));
    wait_hub_running(&hub, state, pid);
    let hub_folder = hub
        .snapshot()
        .folders
        .iter()
        .find(|folder| folder.state == state)
        .cloned()
        .unwrap();
    assert!(hub_folder.lifecycle.adopted, "{hub_folder:#?}");

    // The adopted worker serves the controller's RPCs, `revalidate` included.
    let client = registry.client(state).expect("client");
    assert_eq!(
        client
            .call("hello", json!({}), Duration::from_secs(5))
            .unwrap()["pid"],
        pid
    );
    let view = registry
        .request(
            state,
            FolderCmd::Revalidate { ids: None },
            Duration::from_secs(10),
        )
        .unwrap();
    assert!(view.last_error.is_none(), "{view:#?}");

    // Shutdown stops the idle worker (never left running unmanaged).
    let outcomes = registry.shutdown(Duration::from_secs(20));
    assert_eq!(outcomes.len(), 1, "{outcomes:?}");
    assert_eq!(outcomes[0].outcome, "stopped", "{outcomes:?}");
    assert!(
        wait_until(Duration::from_secs(15), || process_identity(pid).is_none()),
        "the adopted worker exits after Shutdown"
    );
    guard.0 = None;
    assert!(catalog.close());
}

// ---------------------------------------------------------------------------
// Sessions: the real handshake hands its socket to a running Conn.

const CLIENT: &str = "w1-client-installation";
const SERVER: &str = "w1-server-installation";

struct Policy {
    verifier: Verifier,
    admitted: Mutex<Vec<ClientHello>>,
}

impl AcceptPolicy for Policy {
    fn installation(&self) -> String {
        SERVER.into()
    }
    fn verifier(&self) -> Option<(Verifier, u64)> {
        Some((self.verifier.clone(), 3))
    }
    fn admit(&self, hello: &ClientHello) -> Result<(), ErrorWire> {
        self.admitted.lock().unwrap().push(hello.clone());
        Ok(())
    }
    fn legacy_attempt(&self, _: SocketAddr, _: &Value) {}
}

/// Echoes the request: header `{op, len, header}`; `fail` ops answer an error.
struct Echo;
impl Handler for Echo {
    fn handle(&self, req: Incoming) {
        if req.op.ends_with(".fail") {
            req.responder.error(ErrorWire::new(
                "INVALID_REQUEST",
                "INVALID_REQUEST: asked to fail",
            ));
            return;
        }
        let header = json!({"op": req.op, "len": req.body.len(), "header": req.header});
        let body = req.body.iter().rev().copied().collect();
        req.responder.reply(header, body);
    }
}

#[test]
fn session_dial_and_accept_run_a_real_conn() {
    let (code, verifier) = Verifier::generate();
    let policy = Arc::new(Policy {
        verifier,
        admitted: Mutex::new(vec![]),
    });
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let server_policy = policy.clone();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let (conn, peer) = session::accept(
            stream,
            server_policy,
            ConnOptions::for_role(LaneRole::Control),
        )
        .unwrap();
        conn.serve(Arc::new(Echo));
        (conn, peer)
    });
    let hello = client_hello(CLIENT, LaneRole::Control, None, 2);
    let session_id = hello.session.clone();
    let (conn, server_hello) = session::dial(
        &endpoint,
        hello,
        &CodeAuth { code: code.clone() },
        None,
        ConnOptions::for_role(LaneRole::Control),
    )
    .unwrap();
    let (server_conn, peer) = server.join().unwrap();
    assert_eq!(server_hello.installation, SERVER);
    assert_eq!(server_hello.verifier_generation, 3);
    assert_eq!(conn.peer().installation, SERVER);
    assert_eq!(peer.installation, CLIENT);
    assert_eq!(peer.session, session_id);
    assert_eq!(server_conn.peer(), &peer);
    assert_eq!(policy.admitted.lock().unwrap().len(), 1);

    // Requests are paired over the authenticated session, a body above
    // 1 MiB is chunked and reassembled, and a remote error keeps the
    // connection usable (invariant 17).
    let reply = conn
        .call(
            "catalog.list",
            json!({"since": null}),
            vec![],
            Priority::Control,
            Duration::from_secs(10),
        )
        .unwrap();
    assert_eq!(reply.header["op"], "catalog.list");
    let body: Vec<u8> = (0..3 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    let reply = conn
        .call(
            "meta.exchange",
            json!({"n": 1}),
            body.clone(),
            Priority::Data,
            Duration::from_secs(30),
        )
        .unwrap();
    assert_eq!(reply.header["len"], body.len());
    assert_eq!(reply.body, body.iter().rev().copied().collect::<Vec<u8>>());
    let error = conn
        .call(
            "catalog.fail",
            json!({}),
            vec![],
            Priority::Control,
            Duration::from_secs(10),
        )
        .unwrap_err();
    assert!(
        matches!(&error, CallError::Remote(wire) if wire.code == "INVALID_REQUEST"),
        "{error:?}"
    );
    let calls: Vec<_> = (0..64)
        .map(|n| {
            let conn = conn.clone();
            thread::spawn(move || {
                conn.call(
                    "catalog.list",
                    json!({"n": n}),
                    vec![],
                    Priority::Control,
                    Duration::from_secs(10),
                )
                .unwrap()
                .header["header"]["n"]
                    .clone()
            })
        })
        .collect();
    for (n, call) in calls.into_iter().enumerate() {
        assert_eq!(call.join().unwrap(), n);
    }
    assert!(conn.closed().is_none() && server_conn.closed().is_none());

    // A wrong code never reaches a Conn.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let server_policy = policy.clone();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        session::accept(stream, server_policy, ConnOptions::default()).map(|_| ())
    });
    let wrong = session::dial(
        &endpoint,
        client_hello(CLIENT, LaneRole::Control, None, 2),
        &CodeAuth {
            code: format!("{code}x"),
        },
        None,
        ConnOptions::default(),
    );
    assert!(
        matches!(&wrong, Err(session::DialError::Unauthorized(wire)) if wire.code == "INVALID_PAIRING_CODE"),
        "{:?}",
        wrong.map(|_| ())
    );
    assert!(server.join().unwrap().is_err());

    conn.close();
    assert!(server_conn.wait_closed(Duration::from_secs(10)).is_some());
}

// ---------------------------------------------------------------------------
// revalidate (task 1)

fn status(worker: &WorkerProc) -> Value {
    retry_busy(Duration::from_secs(30), || {
        worker.rpc(json!({"op":"status"}))
    })
    .unwrap()
}

#[test]
fn revalidate_requeues_quarantined_rows_and_reactivates_them() {
    // The 6298d0b worker fixture: an INVALID_KIND row and its quarantined
    // descendant, plus two rows waiting for a parent that never arrives.
    let legacy = copy_legacy_worker();
    let mut worker = WorkerProc::spawn(&legacy.state, &[], &[]).unwrap();
    worker.wait_ready(Duration::from_secs(60)).unwrap();
    assert_eq!(status(&worker)["incoming_quarantined"], 2);

    // Unknown or committed ids re-queue nothing.
    let reply = worker
        .rpc(json!({"op":"revalidate","ids":["no-such-event"]}))
        .unwrap();
    assert_eq!(reply, json!({"requeued": 0}));

    // Everything quarantined or blocked is re-queued and activated again in
    // the same request: still invalid, so quarantined (and blocked) again.
    let reply = retry_busy(Duration::from_secs(30), || {
        worker.rpc(json!({"op":"revalidate"}))
    })
    .unwrap();
    assert_eq!(reply, json!({"requeued": 2}), "{}", worker.log_tail());
    assert_eq!(
        status(&worker)["incoming_quarantined"],
        2,
        "the re-queued rows were activated (and refused) at once"
    );
    // The fixture's pending save keeps a graceful stop refused (BUSY_VIEW)
    // until its recovery commits; a force stop persists it and exits.
    worker.rpc(json!({"op":"stop","mode":"force"})).unwrap();
    assert!(worker.wait_exit(Duration::from_secs(15)).is_some());
}

// ---------------------------------------------------------------------------
// Interoperability with a 6298d0b peer (§13 rule 8.4)

/// A daemon run from another binary (the 6298d0b baseline), driven through
/// the same dynamic JSON control calls as `support::Node`.
struct BaselineNode {
    root: tempfile::TempDir,
    child: Option<Child>,
}

impl BaselineNode {
    fn start(bin: &Path) -> Self {
        let root = tempfile::tempdir().unwrap();
        fs::write(
            root.path().join("orchestrator.toml"),
            NodeConfig::default().toml(),
        )
        .unwrap();
        let log = fs::File::create(root.path().join("daemon.log")).unwrap();
        let child = Command::new(bin)
            .arg("-f")
            .arg(root.path().join("orchestrator.toml"))
            .arg("orchestrator")
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        let node = Self {
            root,
            child: Some(child),
        };
        assert!(
            wait_until(Duration::from_secs(30), || node
                .raw(json!({"op":"list"}))
                .is_ok()),
            "baseline daemon did not start:\n{}",
            node.logs()
        );
        node
    }
    fn control(&self) -> Control {
        Control {
            data_dir: self.root.path().join("data"),
        }
    }
    fn raw(&self, action: Value) -> anyhow::Result<Value> {
        self.control().call(&action)
    }
    fn call(&self, action: Value) -> Value {
        self.raw(action.clone())
            .unwrap_or_else(|e| panic!("baseline {action} failed: {e:#}\n{}", self.logs()))
    }
    fn endpoint(&self) -> String {
        let discovery: Value = serde_json::from_slice(
            &fs::read(self.root.path().join("data/orchestrator.json")).unwrap(),
        )
        .unwrap();
        discovery["network"].as_str().unwrap().into()
    }
    fn code(&self) -> String {
        self.call(json!({"op":"reset-pairing-code"}))["code"]
            .as_str()
            .unwrap()
            .into()
    }
    fn running(&self, result: Value) -> Value {
        if result["starting"] != true {
            return result;
        }
        let state = result["state"].as_str().unwrap().to_owned();
        assert!(
            wait_until(Duration::from_secs(60), || self
                .raw(json!({"op":"inspect","state":state}))
                .is_ok_and(|p| p["running"] == true)),
            "baseline folder not running:\n{}",
            self.logs()
        );
        self.call(json!({"op":"inspect","state":state}))
    }
    fn logs(&self) -> String {
        tail(&self.root.path().join("daemon.log"), 8 * 1024)
    }
}

impl Drop for BaselineNode {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = self.raw(json!({"op":"shutdown"}));
            if !wait_until(Duration::from_secs(10), || {
                child.try_wait().ok().flatten().is_some()
            }) {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        for pid in support::find_pids(&self.root.path().to_string_lossy()) {
            kill_pid(pid);
        }
    }
}

fn write(folder: &Value, name: &str, bytes: &[u8]) {
    retry_busy(Duration::from_secs(30), || {
        try_rpc(
            folder,
            json!({"op":"import","path":name,"hex":hex::encode(bytes)}),
        )
    })
    .unwrap_or_else(|e| panic!("import {name}: {e:#}"));
}

fn arrives(what: &str, folder: &Value, name: &str, bytes: &[u8], logs: impl Fn() -> String) {
    assert!(
        wait_until(Duration::from_secs(90), || has_file(folder, name, bytes)),
        "{what}: {name} did not arrive\n{}",
        logs()
    );
}

/// Pair, receive and bidirectional sync between this build and a 6298d0b
/// daemon, in both dialing directions (waves 1–2 must stay wire-compatible).
/// Run with `TKFS_BASELINE_BIN=<6298d0b tkfs binary> cargo test --test
/// w1_integration -- --ignored interop`.
#[test]
#[ignore = "WP1.INT interop: needs TKFS_BASELINE_BIN (a 6298d0b tkfs binary)"]
fn interop_with_a_6298d0b_peer() {
    let baseline = PathBuf::from(
        std::env::var("TKFS_BASELINE_BIN").expect("TKFS_BASELINE_BIN: path of a 6298d0b tkfs"),
    );
    assert!(baseline.is_file(), "{}", baseline.display());
    let old = BaselineNode::start(&baseline);
    let new = Node::start(NodeConfig::default());
    let logs = || format!("{}\n--- new ---\n{}", old.logs(), new.logs());

    // 1. This build dials the old daemon and receives its folder.
    let paired = new.call(
        json!({"op":"pair","endpoint":old.endpoint(),"code":old.code(),
        "connections":3}),
    );
    assert_eq!(paired["connections"], 3, "{paired}");
    let old_folder = old
        .running(old.call(json!({"op":"create","label":"From old","mount":null,"headless":true})));
    // The peer really is the old build: its worker has no IPC v3 `hello`.
    let old_hello = try_rpc(&old_folder, json!({"op":"hello"}));
    assert!(old_hello.is_err(), "{old_hello:?}");
    write(&old_folder, "old-1.txt", b"written by 6298d0b");
    let received = new.receive_from(&old.endpoint(), &old_folder);
    arrives(
        "old -> new",
        &received,
        "old-1.txt",
        b"written by 6298d0b",
        logs,
    );
    write(&received, "new-1.txt", b"written by wave 1");
    arrives(
        "new -> old",
        &old_folder,
        "new-1.txt",
        b"written by wave 1",
        logs,
    );

    // 2. The old daemon dials this build and receives its folder.
    let paired = old.call(
        json!({"op":"pair","endpoint":new.endpoint(),"code":new.code(),
        "connections":3}),
    );
    assert_eq!(paired["connections"], 3, "{paired}");
    let new_folder = new.create("From new", true);
    write(&new_folder, "new-2.txt", b"wave 1 to 6298d0b");
    let old_received = old.running(old.call(json!({"op":"receive","endpoint":new.endpoint(),
        "repo":new_folder["repo"],"label":new_folder["label"],"mount":null,"headless":true})));
    arrives(
        "new -> old (old dials)",
        &old_received,
        "new-2.txt",
        b"wave 1 to 6298d0b",
        logs,
    );
    write(&old_received, "old-2.txt", b"6298d0b to wave 1");
    arrives(
        "old -> new (old dials)",
        &new_folder,
        "old-2.txt",
        b"6298d0b to wave 1",
        logs,
    );

    // Both sides report their lanes connected and the folders caught up.
    let list = new.list();
    let folders = list["folders"].as_array().unwrap();
    assert_eq!(folders.len(), 2, "{list}");
    assert!(
        wait_until(Duration::from_secs(60), || {
            old.call(json!({"op":"peers"}))
                .as_array()
                .is_some_and(|peers| !peers.is_empty())
                && new
                    .peers()
                    .as_array()
                    .is_some_and(|peers| !peers.is_empty())
        }),
        "{}",
        logs()
    );
    let state = state_id(&received);
    assert!(new.inspect(&state)["running"] == true);
    // Leave both stores consistent: a final round in each direction.
    write(&old_folder, "old-3.txt", b"final");
    arrives("old -> new (final)", &received, "old-3.txt", b"final", logs);
}
