//! WP2.1: the actor orchestrator wired (REVAMP_PLAN §14 WP2.1).
//!
//! The global `Supervisor` lock is gone (RC8). These tests prove, on real
//! daemons and workers and on in-process daemons with a fake launcher, that a
//! hung, crash-looping or slow-stopping folder never blocks the other
//! folders, the management API (List, Inspect, Start, Stop, Create, Peers)
//! or the peer lanes; that Shutdown reports one outcome per folder; and that
//! the orchestrator's own locks are never held across I/O (no `Tracked` hold
//! over 50 ms). They also replace, with fault injection, the WP1.12 scenarios
//! that redirected `runtime.json` (the daemon no longer reads it; see the
//! WP2.1 notes, CCR-WP2.1-2).
mod support;

use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use support::*;
use tkfs::{
    model::{
        errors::{ErrorRecord, ErrorWire, Scope},
        health::{MountIntent, MountState, MountStatus},
        ipc::WorkerRecord,
        orchestrator::{
            CallError, ExitRecord, FolderPhase, FolderRecord, FolderView, GateState, LaunchSpec,
            ProcessEvent, ProcessSink, WorkerClient, WorkerEndpoint, WorkerLauncher, WorkerProcess,
        },
    },
    orchestrator::{
        Config, Network,
        ctx::{BuildOptions, build_with},
        manage::{self, PHASE_ATTACHING, PHASE_GATED},
        serve_with, shutdown,
    },
    util::{
        fault::{self, FaultAction, FaultConfig},
        lock::hold_violations,
    },
    ws,
};

/// Daemon scenarios measure latencies: run them one at a time.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
// The §7.3 projection (pure)

fn record(mount: Option<&str>) -> FolderRecord {
    FolderRecord {
        state: "s".into(),
        label: "Folder".into(),
        repo: "r".into(),
        device: "d".into(),
        directory: "/data/states/s".into(),
        mount: mount.map(Into::into),
        desired_running: true,
        sync_enabled: true,
        mount_enabled: true,
    }
}

fn running() -> FolderPhase {
    FolderPhase::Running {
        since_ms: 1,
        instance: "i".into(),
        pid: 7,
        address: "ws://127.0.0.1:1".into(),
        adopted: false,
    }
}

fn mount(state: MountState) -> MountStatus {
    MountStatus {
        intent: MountIntent::Attach,
        path: Some("/m".into()),
        state,
    }
}

fn view(phase: FolderPhase, gate: GateState, mount: MountStatus) -> FolderView {
    FolderView {
        phase,
        gate,
        mount,
        ..FolderView::default()
    }
}

fn error(code: &str) -> ErrorRecord {
    ErrorRecord::new(
        ErrorWire::new(code, format!("{code}: detail")),
        Scope::Folder { state: "s".into() },
        1,
    )
}

#[test]
fn projection_follows_the_section_7_3_rules() {
    let headless = record(None);
    let mounted = record(Some("/m"));
    let intent_off = FolderRecord {
        mount_enabled: false,
        ..mounted.clone()
    };
    let awaiting = GateState::Awaiting {
        source: "peer".into(),
        since_ms: 1,
        waiting_on: vec![],
    };
    let starting = FolderPhase::Starting {
        since_ms: 1,
        attempt: 1,
        phase: "launching worker".into(),
        progress_seq: 0,
        adopting: false,
    };
    let attach_failed = MountState::AttachFailed {
        error: ErrorWire::new(
            "MOUNT_PATH_MUST_NOT_EXIST",
            "MOUNT_PATH_MUST_NOT_EXIST: /m exists",
        ),
        attempt: 1,
        next_retry_at_ms: None,
    };
    // (record, view, gate row) → running, starting, mounted, startup_phase, error
    let cases: Vec<(&str, &FolderRecord, FolderView, bool, Value)> = vec![
        (
            "stopped",
            &headless,
            view(
                FolderPhase::Stopped,
                GateState::None,
                MountStatus::default(),
            ),
            false,
            json!([false, false, false, null, null]),
        ),
        (
            "starting",
            &headless,
            view(starting.clone(), GateState::None, MountStatus::default()),
            false,
            json!([false, true, false, "launching worker", null]),
        ),
        (
            "running headless",
            &headless,
            view(running(), GateState::None, MountStatus::default()),
            false,
            json!([true, false, false, null, null]),
        ),
        (
            "running, gated",
            &mounted,
            view(running(), awaiting.clone(), mount(MountState::Detached)),
            true,
            json!([false, true, false, PHASE_GATED, null]),
        ),
        (
            "running, attaching",
            &mounted,
            view(
                running(),
                GateState::Satisfied { at_ms: 1 },
                mount(MountState::Attaching),
            ),
            true,
            json!([false, true, false, PHASE_ATTACHING, null]),
        ),
        (
            "running, attached",
            &mounted,
            view(
                running(),
                GateState::None,
                mount(MountState::Attached { since_ms: 1 }),
            ),
            false,
            json!([true, false, true, null, null]),
        ),
        (
            "running, attach failed (headless, reported)",
            &mounted,
            view(running(), GateState::None, mount(attach_failed.clone())),
            false,
            json!([
                true,
                false,
                false,
                null,
                "MOUNT_PATH_MUST_NOT_EXIST: /m exists"
            ]),
        ),
        (
            "running, mount intent off",
            &intent_off,
            view(running(), GateState::None, mount(MountState::Detached)),
            false,
            json!([true, false, false, null, null]),
        ),
        (
            "failed",
            &headless,
            view(
                FolderPhase::Failed {
                    error: error("CRASH_LOOP"),
                },
                GateState::None,
                MountStatus::default(),
            ),
            false,
            json!([false, false, false, null, "CRASH_LOOP: detail"]),
        ),
    ];
    for (name, record, view, gate_row, expected) in cases {
        let value = manage::project(record, &view, gate_row);
        let actual = json!([
            value["running"],
            value["starting"],
            value["mounted"],
            value["startup_phase"],
            value["error"]
        ]);
        assert_eq!(actual, expected, "{name}");
        assert_eq!(value["initial_sync_pending"], json!(gate_row), "{name}");
        // The legacy `Project` members are all there.
        for key in [
            "state",
            "label",
            "repo",
            "device",
            "directory",
            "mount",
            "desired_running",
        ] {
            assert!(value.contains_key(key), "{name}: {key}");
        }
    }
    // An open controller error shows while backing off; a resolved one does not.
    let mut backoff = view(
        FolderPhase::Backoff {
            until_ms: 9,
            attempt: 2,
            last_exit: ExitRecord::default(),
        },
        GateState::None,
        MountStatus::default(),
    );
    backoff.last_error = Some(error("WORKER_EXITED"));
    assert_eq!(
        manage::project(&headless, &backoff, false)["error"],
        "WORKER_EXITED: detail"
    );
    backoff.last_error.as_mut().unwrap().resolve(2);
    assert!(manage::project(&headless, &backoff, false)["error"].is_null());
    // The publication error is the open PUBLISHED_SUMMARY_FAILED.
    let mut published = view(running(), GateState::None, MountStatus::default());
    published.published_error = Some(error("PUBLISHED_SUMMARY_FAILED"));
    assert_eq!(
        manage::project(&headless, &published, false)["publication_error"],
        "PUBLISHED_SUMMARY_FAILED: detail"
    );
}

// ---------------------------------------------------------------------------
// In-process daemons with a fake launcher (no worker processes)

/// A worker that never prints Ready (it stays `Starting`) until killed.
struct FakeProcess {
    pid: u32,
    events: ProcessSink,
    killed: AtomicBool,
}

impl WorkerProcess for FakeProcess {
    fn pid(&self) -> u32 {
        self.pid
    }
    fn kill(&self) -> Result<(), ErrorWire> {
        if !self.killed.swap(true, Ordering::AcqRel) {
            (self.events)(ProcessEvent::Exited {
                code: None,
                fatal: None,
                stderr_tail: String::new(),
            });
        }
        Ok(())
    }
    fn adopted(&self) -> bool {
        false
    }
}

struct FakeClient;

impl WorkerClient for FakeClient {
    fn instance(&self) -> &str {
        "fake"
    }
    fn call(&self, op: &str, _: Value, _: Duration) -> Result<Value, CallError> {
        Err(CallError::Unavailable(format!(
            "LOCAL_WORKER_UNAVAILABLE: fake {op}"
        )))
    }
    fn call_bytes(
        &self,
        op: &str,
        _: Value,
        _: &[u8],
        _: Duration,
    ) -> Result<(Value, Vec<u8>), CallError> {
        Err(CallError::Unavailable(format!(
            "LOCAL_WORKER_UNAVAILABLE: fake {op}"
        )))
    }
}

#[derive(Default)]
struct FakeLauncher {
    launches: AtomicU32,
}

impl WorkerLauncher for FakeLauncher {
    fn launch(
        &self,
        _spec: &LaunchSpec,
        events: ProcessSink,
    ) -> Result<Box<dyn WorkerProcess>, ErrorWire> {
        let n = self.launches.fetch_add(1, Ordering::AcqRel);
        Ok(Box::new(FakeProcess {
            pid: 40_000 + n,
            events,
            killed: AtomicBool::new(false),
        }))
    }
    fn adopt(
        &self,
        _record: &WorkerRecord,
        _events: ProcessSink,
    ) -> Result<Option<Box<dyn WorkerProcess>>, ErrorWire> {
        Ok(None)
    }
    fn client(&self, _endpoint: &WorkerEndpoint) -> Arc<dyn WorkerClient> {
        Arc::new(FakeClient)
    }
}

struct InProcess {
    _root: tempfile::TempDir,
    control: String,
    data: std::path::PathBuf,
    server: Option<thread::JoinHandle<anyhow::Result<()>>>,
}

impl InProcess {
    fn start(maximum_running: usize) -> Self {
        Self::start_listening(maximum_running, "127.0.0.1:0")
    }

    /// An in-process daemon whose peer listener binds `listen`.
    fn start_listening(maximum_running: usize, listen: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let config = Config {
            format_version: 2,
            data_directory: root.path().join("data"),
            default_mount_directory: root.path().join("mounts"),
            maximum_running,
            network: Network {
                listen: Some(listen.into()),
                connections: 2,
                sync_interval_ms: 500,
            },
        };
        let data = config.data_directory.clone();
        let daemon = build_with(
            config,
            BuildOptions {
                launcher: Some(Arc::new(FakeLauncher::default())),
                ..BuildOptions::default()
            },
        )
        .unwrap();
        let control = daemon.ctx.endpoints.load().control.clone();
        let server = thread::spawn(move || serve_with(daemon, false));
        Self {
            _root: root,
            control,
            data,
            server: Some(server),
        }
    }

    fn socket(&self) -> ws::Socket {
        ws::connect(&self.control).unwrap()
    }

    fn call(&self, action: Value) -> anyhow::Result<Value> {
        ws::call(&mut self.socket(), &action)
    }

    /// Waits for the daemon thread to end (after a Shutdown).
    fn join(&mut self, timeout: Duration) {
        let server = self.server.take().unwrap();
        assert!(
            wait_until(timeout, || server.is_finished()),
            "the in-process daemon did not end"
        );
        server.join().unwrap().unwrap();
    }
}

impl Drop for InProcess {
    fn drop(&mut self) {
        if self.server.is_some() {
            let _ = self.call(json!({"op":"shutdown"}));
            if let Some(server) = self.server.take() {
                wait_until(Duration::from_secs(10), || server.is_finished());
            }
        }
    }
}

/// Names of the orchestrator's own `Tracked` locks (§2.2 rule 2).
fn orchestrator_lock(name: &str) -> bool {
    [
        "legacy-bridge.",
        "control.",
        "folder-registry",
        "folder-slots",
        "catalog.",
    ]
    .iter()
    .any(|prefix| name.starts_with(prefix))
}

#[test]
fn control_connections_are_persistent_and_answer_unparsable_requests() {
    let _serial = serial();
    let daemon = InProcess::start(8);
    let mut socket = daemon.socket();
    // Several requests on one connection; a request that does not parse is
    // answered and the connection stays usable.
    let listed = ws::call(&mut socket, &json!({"op":"list"})).unwrap();
    assert_eq!(listed["folders"], json!([]));
    let error = ws::call(&mut socket, &json!({"op":"no-such-action"})).unwrap_err();
    assert!(
        error.downcast_ref::<ws::RemoteError>().is_some()
            && error.to_string().contains("INVALID_REQUEST")
            && error.to_string().contains("unknown variant"),
        "{error:#}"
    );
    assert_eq!(
        ws::call(&mut socket, &json!({"op":"peers"})).unwrap(),
        json!([])
    );
    // New variants are forwarded to manage_v2 (WP2.2's), not refused here.
    let v2 = ws::call(&mut socket, &json!({"op":"status"}));
    if let Err(error) = &v2 {
        assert!(!error.to_string().contains("INVALID_REQUEST"), "{error:#}");
    }
    // The discovery file has format 3 and names this daemon.
    let discovery: Value =
        serde_json::from_slice(&fs::read(daemon.data.join("orchestrator.json")).unwrap()).unwrap();
    assert_eq!(discovery["format"], 3);
    assert_eq!(discovery["pid"], std::process::id());
    assert_eq!(discovery["control"], daemon.control);
    assert!(discovery["network"].as_str().unwrap().starts_with("ws://"));
    assert!(discovery["started_at_ms"].is_u64());
    assert!(discovery["installation"].is_string());
}

#[test]
fn a_folder_stuck_starting_blocks_nothing_and_shutdown_kills_it() {
    let _serial = serial();
    let mut daemon = InProcess::start(2);
    let created = daemon
        .call(json!({"op":"create","label":"Stuck","mount":null,"headless":true}))
        .unwrap();
    assert_eq!(created["starting"], true, "{created}");
    assert_eq!(created["running"], false);
    assert_eq!(created["startup_phase"], "launching worker", "{created}");
    assert_eq!(created["status"]["lifecycle"]["phase"], "starting");
    let state = created["state"].as_str().unwrap().to_owned();
    // The worker never becomes ready: the API is not affected.
    let mut samples = vec![];
    for _ in 0..50 {
        let (listed, ms) = time_ms(|| daemon.call(json!({"op":"list"})));
        assert!(listed.is_ok());
        samples.push(ms);
    }
    let summary = summarize(&samples);
    assert!(summary.p99 < 250.0, "list p99 {} ms", summary.p99);
    // Another folder starts and stops while the first one is stuck.
    let other = daemon
        .call(json!({"op":"create","label":"Other","mount":null,"headless":true}))
        .unwrap();
    let other_state = other["state"].as_str().unwrap().to_owned();
    let (stopped, ms) = time_ms(|| daemon.call(json!({"op":"stop","state":other_state})));
    let stopped = stopped.unwrap();
    assert!(ms < 2000.0, "stop took {ms} ms");
    assert_eq!(stopped["running"], false);
    assert_eq!(stopped["starting"], false);
    assert_eq!(stopped["desired_running"], false);
    // maximum_running = 2: two folders hold slots (one stuck, one started).
    daemon
        .call(json!({"op":"start","state":other_state}))
        .unwrap();
    let error = daemon
        .call(json!({"op":"create","label":"Third","mount":null,"headless":true}))
        .unwrap_err();
    assert!(error.to_string().contains("WORKER_LIMIT"), "{error:#}");
    // ConfigureMount is refused while a folder has a worker.
    let error = daemon
        .call(json!({"op":"configure-mount","state":state,"mount":null}))
        .unwrap_err();
    assert!(
        error.to_string().contains("STOP_FOLDER_BEFORE_CONFIGURING"),
        "{error:#}"
    );
    // Shutdown: every folder reports, a Starting worker is killed, and the
    // daemon then refuses new actions until it is gone.
    let mut socket = daemon.socket();
    let mut late = daemon.socket();
    let (reply, ms) = time_ms(|| ws::call(&mut socket, &json!({"op":"shutdown"})));
    let reply = reply.unwrap();
    assert!(ms < 10_000.0, "shutdown took {ms} ms");
    assert_eq!(reply["stopped"], true);
    let outcomes = reply["folders"].as_array().unwrap();
    assert_eq!(outcomes.len(), 2, "{reply}");
    assert!(outcomes.iter().all(|o| o["outcome"] == "killed"), "{reply}");
    // Either refused (stopping) or the connection was closed at exit; never
    // answered.
    let refused = ws::call(&mut late, &json!({"op":"list"}));
    assert_refused_while_stopping(refused);
    daemon.join(Duration::from_secs(10));
    assert!(!daemon.data.join("orchestrator.json").exists());
    // The orchestrator's own locks were never held across I/O.
    let violations: Vec<_> = hold_violations()
        .into_iter()
        .filter(|v| orchestrator_lock(v.name))
        .collect();
    assert!(violations.is_empty(), "{violations:?}");
}

#[test]
fn shutdown_runs_once_and_a_second_request_is_refused() {
    let _serial = serial();
    let mut daemon = InProcess::start(8);
    let mut first = daemon.socket();
    let mut second = daemon.socket();
    let reply = ws::call(&mut first, &json!({"op":"shutdown"})).unwrap();
    assert_eq!(reply, json!({"stopped":true,"folders":[]}));
    // The sequence runs once: a second Shutdown is refused (stopping) or
    // finds the daemon gone, never answered.
    assert_refused_while_stopping(ws::call(&mut second, &json!({"op":"shutdown"})));
    daemon.join(Duration::from_secs(10));
}

/// A Shutdown whose sequence panics still ends the daemon: the request is
/// answered `UNKNOWN_ERROR`, the main thread is released anyway (no wedge on
/// a latch that never finishes) and `orchestrator.json` is removed.
#[test]
fn a_shutdown_sequence_that_panics_still_ends_the_daemon() {
    let _serial = serial();
    let mut daemon = InProcess::start(8);
    fault::configure(Some(FaultConfig::new(
        shutdown::faults::SEQUENCE,
        FaultAction::Panic,
    )));
    let answered = daemon.call(json!({"op":"shutdown"}));
    fault::configure(None);
    let error = answered.expect_err("the sequence panicked");
    assert!(
        error.downcast_ref::<ws::RemoteError>().is_some()
            && error.to_string().contains("UNKNOWN_ERROR"),
        "{error:#}"
    );
    daemon.join(Duration::from_secs(10));
    assert!(!daemon.data.join("orchestrator.json").exists());
}

/// A request sent after the Shutdown reply: either refused with
/// `ORCHESTRATOR_STOPPING`, or a transport error (the daemon closed the
/// connection). An answer fails the test.
fn assert_refused_while_stopping(result: anyhow::Result<Value>) {
    let error = match result {
        Ok(answer) => panic!("answered after the Shutdown reply: {answer}"),
        Err(error) => error,
    };
    let text = format!("{error:#}");
    assert!(
        text.contains("ORCHESTRATOR_STOPPING") || error.downcast_ref::<ws::RemoteError>().is_none(),
        "{text}"
    );
}

/// `network.listen` on every interface (`0.0.0.0`, and `[::]` where the host
/// has IPv6): Shutdown still ends the daemon. Stopping a blocking accept
/// must not depend on connecting to the unspecified address (Windows refuses
/// that connect, `WSAEADDRNOTAVAIL`).
#[test]
fn shutdown_ends_a_daemon_listening_on_every_interface() {
    let _serial = serial();
    let mut listens = vec!["0.0.0.0:0"];
    if std::net::TcpListener::bind("[::]:0").is_ok() {
        listens.push("[::]:0");
    }
    for listen in listens {
        let mut daemon = InProcess::start_listening(8, listen);
        let reply = daemon.call(json!({"op":"shutdown"})).unwrap();
        assert_eq!(reply["stopped"], true, "{listen}: {reply}");
        let server = daemon.server.take().unwrap();
        assert!(
            wait_until(Duration::from_secs(5), || server.is_finished()),
            "{listen}: the daemon did not end after Shutdown"
        );
        server.join().unwrap().unwrap();
        assert!(
            !daemon.data.join("orchestrator.json").exists(),
            "{listen}: orchestrator.json left behind"
        );
        assert!(
            !daemon.data.join("orchestrator.json.tmp").exists(),
            "{listen}"
        );
    }
}

// ---------------------------------------------------------------------------
// Real daemons and workers

fn inspect(node: &Node, state: &str) -> Value {
    node.inspect(state)
}

fn phase(inspect: &Value) -> String {
    inspect["status"]["lifecycle"]["phase"]
        .as_str()
        .unwrap_or_default()
        .to_owned()
}

/// `STATE/worker.json` of a folder (pid, instance).
fn worker_record(folder: &Value) -> Option<Value> {
    let bytes = fs::read(state_dir(folder).join("worker.json")).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn worker_pid(folder: &Value) -> Option<u32> {
    worker_record(folder)
        .and_then(|r| r["pid"].as_u64())
        .map(|p| p as u32)
}

fn alive(pid: u32) -> bool {
    tkfs::orchestrator::process::process_identity(pid).is_some()
}

fn lock_violations_in(log: &Path) -> Vec<String> {
    fs::read_to_string(log)
        .unwrap_or_default()
        .lines()
        .filter(|line| line.contains("LOCK_HOLD_EXCEEDED"))
        .map(str::to_owned)
        .collect()
}

/// A worker that exits at once (`worker.startup`, exit) backs off 1, 2, 4 …
/// s and opens the breaker (`Failed{CRASH_LOOP}`); the API and the other
/// folder never notice. Replaces WP1.12's store-deletion crash loop, which is
/// `Failed{STATE_NOT_INITIALIZED}` (Fatal) at once now (§3.3).
#[test]
fn a_crash_looping_folder_backs_off_and_blocks_nothing() {
    let _serial = serial();
    let mut a = Node::new();
    let crasher = a.create("Crasher", true);
    let healthy = a.create("Healthy", true);
    let state = state_id(&crasher);
    a.restart_with_env(vec![
        ("TKFS_FAULT_POINT".into(), "worker.startup".into()),
        ("TKFS_FAULT_ACTION".into(), "exit".into()),
        ("TKFS_FAULT_SCOPE".into(), state.clone()),
    ]);
    let healthy_state = state_id(&healthy);
    a.wait_for("healthy running", Duration::from_secs(30), || {
        a.try_inspect(&healthy_state)
            .is_ok_and(|p| p["running"] == true)
    });
    let healthy_pid = worker_pid(&healthy).unwrap();
    let log = state_dir(&crasher).join("worker.log");
    let starts = || {
        fs::read_to_string(&log)
            .unwrap_or_default()
            .lines()
            .filter(|line| line.contains("opening state") && line.contains("startup_progress"))
            .count()
    };
    let started = Instant::now();
    let before = starts();
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = {
        let (stop, control) = (stop.clone(), a.control());
        thread::spawn(move || {
            let mut samples = vec![];
            while !stop.load(Ordering::Relaxed) {
                let (listed, ms) = time_ms(|| control.call(&json!({"op":"list"})));
                assert!(listed.is_ok(), "list failed: {listed:?}");
                samples.push(ms);
                thread::sleep(Duration::from_millis(20));
            }
            samples
        })
    };
    let mut times = vec![];
    let mut seen = before;
    let mut backoff_seen = false;
    let failed = wait_until(Duration::from_secs(60), || {
        let now = starts();
        if now > seen {
            times.extend((seen..now).map(|_| started.elapsed().as_secs_f64()));
            seen = now;
        }
        let p = inspect(&a, &state);
        if phase(&p) == "backoff" {
            backoff_seen = true;
            assert!(
                p["error"]
                    .as_str()
                    .is_some_and(|e| e.contains("WORKER_EXITED")),
                "{p}"
            );
            assert_eq!(p["running"], false);
        }
        phase(&p) == "failed" && p["status"].to_string().contains("CRASH_LOOP")
    });
    stop.store(true, Ordering::Relaxed);
    let samples = sampler.join().unwrap();
    assert!(failed, "no Failed{{CRASH_LOOP}}\n{}", a.logs());
    assert!(backoff_seen, "the backoff phase was never visible");
    let inspected = inspect(&a, &state);
    assert!(
        inspected["error"]
            .as_str()
            .is_some_and(|e| e.contains("CRASH_LOOP")),
        "{inspected}"
    );
    // Five starts, each after a growing delay (1, 2, 4, 8 s, ±20 %).
    assert!(
        (4..=6).contains(&times.len()),
        "starts at {times:?}\n{}",
        a.logs()
    );
    for (attempt, pair) in times.windows(2).take(3).enumerate() {
        let gap = pair[1] - pair[0];
        let floor = 0.8 * f64::from(1u32 << attempt) * 0.9;
        assert!(
            gap >= floor,
            "restart {} after {gap:.2} s: {times:?}",
            attempt + 1
        );
    }
    let summary = summarize(&samples);
    assert!(summary.p99 < 500.0, "list p99 {} ms", summary.p99);
    // The other folder's worker was never touched.
    assert_eq!(worker_pid(&healthy), Some(healthy_pid));
    assert!(alive(healthy_pid));
    assert_eq!(inspect(&a, &healthy_state)["running"], true);
    // Start leaves Failed (§3.3) and starts over.
    let restarted = a.call(json!({"op":"start","state":state}));
    assert_eq!(restarted["starting"], true, "{restarted}");
    assert_ne!(phase(&restarted), "failed", "{restarted}");
    a.call(json!({"op":"stop","state":state}));
    assert!(lock_violations_in(&a.log_path()).is_empty());
}

/// A worker whose `stop` hangs: Stop waits at most 15 s (13 s) and says it
/// is still stopping; meanwhile List, Inspect, Peers and the other folders' Stop,
/// Start and Create are unaffected. Shutdown then reports one outcome per
/// folder (the hung worker `left_running`, the others `stopped`) within 30 s,
/// exits 0 and removes `orchestrator.json`; the next daemon adopts the worker
/// it left (same process) and keeps every `desired_running`.
#[test]
fn a_slow_stopping_folder_blocks_nothing_and_shutdown_reports_each_outcome() {
    let _serial = serial();
    let mut a = Node::new();
    let slow = a.create("Slow stop", true);
    let other = a.create("Other", true);
    let state = state_id(&slow);
    let other_state = state_id(&other);
    a.restart_with_env(vec![
        ("TKFS_FAULT_POINT".into(), "worker.rpc.stop".into()),
        ("TKFS_FAULT_ACTION".into(), "hang".into()),
        ("TKFS_FAULT_SCOPE".into(), state.clone()),
    ]);
    for s in [&state, &other_state] {
        a.wait_for("running", Duration::from_secs(30), || {
            a.try_inspect(s).is_ok_and(|p| p["running"] == true)
        });
    }
    let slow_pid = worker_pid(&slow).unwrap();
    // Stop the slow folder in the background: it waits at most 15 s.
    let stopper = {
        let control = a.control();
        let state = state.clone();
        thread::spawn(move || time_ms(|| control.call(&json!({"op":"stop","state":state}))))
    };
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = {
        let stop = stop.clone();
        let control = a.control();
        thread::spawn(move || {
            let mut samples = vec![];
            while !stop.load(Ordering::Relaxed) {
                let (r, ms) = time_ms(|| control.call(&json!({"op":"list"})));
                assert!(r.is_ok(), "list failed: {r:?}");
                samples.push(ms);
                thread::sleep(Duration::from_millis(20));
            }
            samples
        })
    };
    a.wait_for("stopping", Duration::from_secs(10), || {
        phase(&inspect(&a, &state)) == "stopping"
    });
    // Everything else answers at once while that stop hangs.
    let (peers, ms) = time_ms(|| a.raw_call(json!({"op":"peers"})));
    assert!(peers.is_ok() && ms < 1000.0, "peers {ms} ms");
    let (stopped, ms) = time_ms(|| a.call(json!({"op":"stop","state":other_state})));
    assert!(ms < 8000.0, "other stop took {ms} ms");
    assert_eq!(stopped["running"], false, "{stopped}");
    let (started, ms) = time_ms(|| a.call(json!({"op":"start","state":other_state})));
    assert!(ms < 3000.0, "other start took {ms} ms");
    assert_eq!(started["starting"], true, "{started}");
    let (created, ms) = time_ms(|| a.create("Created meanwhile", true));
    assert!(ms < 30_000.0, "create took {ms} ms");
    let created_state = state_id(&created);
    let (reply, stop_ms) = stopper.join().unwrap();
    let reply = reply.unwrap();
    // Stop waits at most 15 s (`STOP_REPLY_WAIT`, 13 s: inside a legacy
    // client's 15 s socket timeout).
    assert!(
        (12_000.0..15_000.0).contains(&stop_ms),
        "Stop of the hung folder answered after {stop_ms} ms: {reply}"
    );
    assert_eq!(reply["stopping"], true, "{reply}");
    assert!(reply["waiting_for"].is_string(), "{reply}");
    assert_eq!(reply["desired_running"], false);
    stop.store(true, Ordering::Relaxed);
    let samples = sampler.join().unwrap();
    let summary = summarize(&samples);
    assert!(summary.p99 < 500.0, "list p99 {} ms", summary.p99);
    // Start again: the folder is wanted again (the stop it is in continues).
    a.call(json!({"op":"start","state":state}));
    a.wait_for("other running", Duration::from_secs(30), || {
        a.try_inspect(&other_state)
            .is_ok_and(|p| p["running"] == true)
    });
    // Shutdown: one outcome per folder, within 30 s, exit 0.
    let (reply, status) = a.shutdown_within(Duration::from_secs(30));
    let reply = reply.unwrap();
    let status = status.unwrap_or_else(|| panic!("no exit within 30 s\n{}", a.logs()));
    assert!(status.success(), "{status}");
    let outcome = |s: &str| {
        reply["folders"]
            .as_array()
            .unwrap()
            .iter()
            .find(|o| o["state"] == s)
            .cloned()
            .unwrap_or_else(|| panic!("no outcome for {s}: {reply}"))
    };
    let left = outcome(&state);
    assert_eq!(left["outcome"], "left_running", "{reply}");
    assert!(left["error"]["code"].is_string(), "{reply}");
    for s in [&other_state, &created_state] {
        assert_eq!(outcome(s)["outcome"], "stopped", "{reply}");
        assert!(outcome(s)["error"].is_null(), "{reply}");
    }
    assert!(!a.data_dir().join("orchestrator.json").exists());
    assert!(alive(slow_pid), "the hung worker is left running");
    // The next daemon adopts it: same process, desired_running kept.
    a.cfg.env = vec![];
    a.spawn();
    a.wait_for("adopted", Duration::from_secs(30), || {
        a.try_inspect(&state)
            .is_ok_and(|p| p["running"] == true && p["status"]["lifecycle"]["adopted"] == true)
    });
    assert_eq!(worker_pid(&slow), Some(slow_pid));
    let inspected = inspect(&a, &state);
    assert_eq!(inspected["status"]["lifecycle"]["pid"], slow_pid);
    assert_eq!(inspected["desired_running"], true);
    assert_eq!(inspect(&a, &other_state)["desired_running"], true);
    assert!(lock_violations_in(&a.log_path()).is_empty());
    // Its stop still hangs: end it here rather than at the node's drop.
    kill_pid(slow_pid);
}

/// A remote folder whose worker hangs on `published-exchange` is charged to
/// that folder (error, backoff) while the other folder shared by the same
/// peer keeps syncing within 45 s, and both daemons' APIs stay responsive.
/// Replaces WP1.12's `runtime.json` redirect, which no longer reaches the
/// daemon (it uses the controller's endpoint).
#[test]
fn a_hung_remote_folder_is_charged_while_the_other_keeps_syncing() {
    let _serial = serial();
    let mut a = Node::new();
    let b = Node::start(NodeConfig::default().connections(2));
    a.pin_endpoint();
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
    a.restart_with_env(vec![
        (
            "TKFS_FAULT_POINT".into(),
            "worker.rpc.published-exchange".into(),
        ),
        ("TKFS_FAULT_ACTION".into(), "hang".into()),
        ("TKFS_FAULT_SCOPE".into(), state_id(&px)),
    ]);
    for folder in [&px, &py] {
        let state = state_id(folder);
        a.wait_for("running", Duration::from_secs(30), || {
            a.try_inspect(&state).is_ok_and(|p| p["running"] == true)
        });
    }
    let started = Instant::now();
    write_file(&py, "after-hang.txt", b"healthy progress");
    let x_repo = px["repo"].as_str().unwrap().to_owned();
    let mut charged = false;
    let mut samples = vec![];
    let progressed = wait_until(Duration::from_secs(90), || {
        let (peers, ms) = time_ms(|| b.raw_call(json!({"op":"peers"})));
        samples.push(ms);
        if let Ok(peers) = peers {
            charged |= peers[0]["lanes"].as_array().is_some_and(|lanes| {
                lanes.iter().any(|lane| {
                    let r = &lane["repositories"][&x_repo];
                    r["error"].is_string() && r["consecutive_failures"].as_u64().unwrap_or(0) > 0
                })
            });
        }
        let (listed, ms) = time_ms(|| a.raw_call(json!({"op":"list"})));
        assert!(listed.is_ok());
        samples.push(ms);
        has_file(&by, "after-hang.txt", b"healthy progress")
    });
    let delay = started.elapsed();
    assert!(progressed, "the healthy folder stalled\n{}", b.logs());
    assert!(
        delay < Duration::from_secs(45),
        "healthy folder took {delay:?}"
    );
    // The hung exchange ends at the 15 s per-request deadline.
    let charged = charged
        || wait_until(Duration::from_secs(40), || {
            b.raw_call(json!({"op":"peers"})).is_ok_and(|peers| {
                peers[0]["lanes"].as_array().is_some_and(|lanes| {
                    lanes.iter().any(|lane| {
                        let r = &lane["repositories"][&x_repo];
                        r["error"].is_string()
                            && r["consecutive_failures"].as_u64().unwrap_or(0) > 0
                    })
                })
            })
        });
    assert!(charged, "the hung folder was never charged\n{}", b.logs());
    let summary = summarize(&samples);
    assert!(summary.p99 < 1000.0, "API p99 {} ms", summary.p99);
}

/// SIGTERM runs the same sequence as the Shutdown action: exit 0, workers
/// stopped, `orchestrator.json` removed, `desired_running` kept.
#[cfg(unix)]
#[test]
fn sigterm_runs_the_shutdown_sequence() {
    let _serial = serial();
    let mut a = Node::new();
    let p = a.create("Signal", true);
    let worker = worker_pid(&p).unwrap();
    let pid = a.pid().unwrap();
    // SAFETY: a signal to the daemon this test started.
    unsafe {
        libc::kill(pid as i32, libc::SIGTERM);
    }
    let status = a.wait_exit(Duration::from_secs(30));
    assert!(
        status.is_some_and(|s| s.success()),
        "{status:?}\n{}",
        a.logs()
    );
    assert!(!a.data_dir().join("orchestrator.json").exists());
    assert!(
        wait_until(Duration::from_secs(10), || !alive(worker)),
        "the worker was not stopped"
    );
    a.spawn();
    assert_eq!(a.inspect(&state_id(&p))["desired_running"], true);
}

/// CCR-WP1.INT-1: a worker must not inherit the daemon's standard handles.
/// The daemon's stdout is a pipe here; after the daemon is killed (its worker
/// lives on, as designed), reading that pipe reaches its end at once instead
/// of waiting for the worker.
#[test]
fn a_killed_daemon_leaves_its_stdout_to_no_worker() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
        sync::mpsc,
    };
    let _serial = serial();
    let root = tempfile::tempdir().unwrap();
    fs::write(
        root.path().join("orchestrator.toml"),
        NodeConfig::default().toml(),
    )
    .unwrap();
    let mut daemon = Command::new(bin())
        .arg("-f")
        .arg(root.path().join("orchestrator.toml"))
        .arg("orchestrator")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let stdout = daemon.stdout.take().unwrap();
    let (lines, received) = mpsc::channel();
    let reader = thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            let _ = lines.send(line);
        }
    });
    // The discovery line, then a folder with a running worker.
    let discovery: Value =
        serde_json::from_str(&received.recv_timeout(Duration::from_secs(20)).unwrap()).unwrap();
    assert_eq!(discovery["format"], 3);
    let control = Control {
        data_dir: root.path().join("data"),
    };
    let created = control
        .call(&json!({"op":"create","label":"Inherit","mount":null,"headless":true}))
        .unwrap();
    let state = state_id(&created);
    assert!(wait_until(Duration::from_secs(30), || {
        control
            .call(&json!({"op":"inspect","state":state}))
            .is_ok_and(|p| p["running"] == true)
    }));
    let worker = worker_pid(&created).unwrap();
    // Killed on every path: a worker left behind would hold this test
    // process's own inherited handles too (CCR-WP2.1-3).
    let _worker = KillOnDrop(worker);
    daemon.kill().unwrap();
    daemon.wait().unwrap();
    // The worker is alive, and the pipe is at its end.
    assert!(alive(worker));
    assert!(
        wait_until(Duration::from_secs(5), || reader.is_finished()),
        "the daemon's stdout is held open by its worker"
    );
}

/// Kills a process when dropped (also when the test panics).
struct KillOnDrop(u32);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        kill_pid(self.0);
    }
}

/// CCR-WP1.INT-1, the general case: the daemon also inherits handles that
/// are not its standard handles (any inheritable handle of its parent). A
/// worker must inherit none of them: here the write end of a pipe this test
/// lends the daemon is closed everywhere once the daemon is killed, although
/// its worker lives on.
#[cfg(windows)]
#[test]
fn workers_inherit_none_of_the_handles_the_daemon_inherited() {
    use std::{
        io::Read,
        os::windows::io::AsRawHandle,
        process::{Command, Stdio},
    };
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn SetHandleInformation(handle: *mut core::ffi::c_void, mask: u32, flags: u32) -> i32;
    }
    const HANDLE_FLAG_INHERIT: u32 = 1;
    let _serial = serial();
    let root = tempfile::tempdir().unwrap();
    fs::write(
        root.path().join("orchestrator.toml"),
        NodeConfig::default().toml(),
    )
    .unwrap();
    let (mut pipe, lent) = std::io::pipe().unwrap();
    // SAFETY: flags of a handle this test owns. Inheritable only for the
    // daemon's spawn (no other test spawns processes meanwhile: `serial`).
    unsafe {
        SetHandleInformation(
            lent.as_raw_handle(),
            HANDLE_FLAG_INHERIT,
            HANDLE_FLAG_INHERIT,
        )
    };
    let mut daemon = Command::new(bin())
        .arg("-f")
        .arg(root.path().join("orchestrator.toml"))
        .arg("orchestrator")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    // SAFETY: as above.
    unsafe { SetHandleInformation(lent.as_raw_handle(), HANDLE_FLAG_INHERIT, 0) };
    drop(lent);
    let control = Control {
        data_dir: root.path().join("data"),
    };
    assert!(wait_until(Duration::from_secs(20), || control
        .call(&json!({"op":"list"}))
        .is_ok()));
    let created = control
        .call(&json!({"op":"create","label":"Lent","mount":null,"headless":true}))
        .unwrap();
    let state = state_id(&created);
    assert!(wait_until(Duration::from_secs(30), || {
        control
            .call(&json!({"op":"inspect","state":state}))
            .is_ok_and(|p| p["running"] == true)
    }));
    let worker = worker_pid(&created).unwrap();
    let _worker = KillOnDrop(worker);
    daemon.kill().unwrap();
    daemon.wait().unwrap();
    assert!(alive(worker));
    let reader = thread::spawn(move || {
        let mut rest = Vec::new();
        let _ = pipe.read_to_end(&mut rest);
    });
    assert!(
        wait_until(Duration::from_secs(5), || reader.is_finished()),
        "the worker inherited a handle the daemon had inherited"
    );
}

/// The initial-sync gate holds a received folder's mount while its worker
/// already runs (headless) and syncs; once the source's metadata is complete
/// the same worker attaches the mount in place (no restart, §3.3), and the
/// gate marker goes only after the attach (invariant 26).
#[test]
fn a_received_mount_attaches_in_place_once_its_gate_is_satisfied() {
    use tkfs::{
        core::{Store, id},
        paired_sync::{self, SyncRoute},
        runtime::Engine,
    };
    if !mount_supported() {
        return;
    }
    let _serial = serial();
    let a = Node::new();
    let b = Node::new();
    let pa = a.create("Gate source", true);
    // A holds an event whose parent it lacks: its metadata is not complete.
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
    rpc(
        &pa,
        json!({"op":"published-exchange","route":reverse,"page":incomplete,"metadata_only":true}),
    );
    b.pair(&a, &a.code());
    let mount = b.path().join("received");
    let pb = b.call(
        json!({"op":"receive","endpoint":a.endpoint(),"repo":pa["repo"],
        "label":"Received","mount":mount,"headless":false}),
    );
    let state = state_id(&pb);
    b.wait_for("gated", Duration::from_secs(30), || {
        b.try_inspect(&state).is_ok_and(|p| {
            p["startup_phase"] == PHASE_GATED
                && p["sync"][0]["metadata"]["last_success_ms"].is_number()
        })
    });
    let gated = b.inspect(&state);
    assert_eq!(gated["running"], false, "{gated}");
    assert_eq!(gated["starting"], true);
    assert_eq!(gated["mounted"], false);
    assert_eq!(gated["initial_sync_pending"], true);
    assert_eq!(phase(&gated), "running", "the worker runs while gated");
    assert!(!mount.exists());
    let pid = worker_pid(&pb).unwrap();
    rpc(
        &pa,
        json!({"op":"published-exchange","route":reverse,"page":full,"metadata_only":true}),
    );
    b.wait_for("mounted", Duration::from_secs(60), || {
        b.try_inspect(&state).is_ok_and(|p| p["running"] == true)
    });
    let mounted = b.inspect(&state);
    assert_eq!(mounted["mounted"], true, "{mounted}");
    assert_eq!(mounted["initial_sync_pending"], false);
    assert_eq!(worker_pid(&pb), Some(pid), "attached in place");
    assert!(alive(pid));
    assert!(mount.join("parent/child").is_dir());
}

/// A peer's `list` never waits for a worker: it is the published-catalog
/// cache (§3.5, §3.8). With one folder's `published-summary` hung, `list`
/// answers at once without that folder, and the folder reports
/// `PUBLISHED_SUMMARY_FAILED` (`REQUEST_TIMEOUT`). Replaces WP1.12's timing of
/// the `list` fan-out over hung workers (one 3 s deadline), which no longer
/// exists.
#[test]
fn a_hung_published_summary_never_slows_list() {
    let _serial = serial();
    let mut a = Node::new();
    let hung = a.create("Hung summary", true);
    let healthy = a.create("Healthy", true);
    a.restart_with_env(vec![
        (
            "TKFS_FAULT_POINT".into(),
            "worker.rpc.published-summary".into(),
        ),
        ("TKFS_FAULT_ACTION".into(), "hang".into()),
        ("TKFS_FAULT_SCOPE".into(), state_id(&hung)),
    ]);
    let hung_state = state_id(&hung);
    a.wait_for("publication error", Duration::from_secs(30), || {
        a.try_inspect(&hung_state).is_ok_and(|p| {
            p["publication_error"].as_str().is_some_and(|e| {
                e.contains("PUBLISHED_SUMMARY_FAILED") && e.contains("REQUEST_TIMEOUT")
            })
        })
    });
    assert_eq!(a.inspect(&hung_state)["running"], true);
    let code = a.code();
    let mut socket = ws::connect(&a.endpoint()).unwrap();
    ws::call(
        &mut socket,
        &json!({"op":"pair","version":2,"code":code,"installation":tkfs::core::id(),"lane":"control"}),
    )
    .unwrap();
    let mut samples = vec![];
    for _ in 0..20 {
        let (listed, ms) = time_ms(|| ws::call(&mut socket, &json!({"op":"list"})));
        let repos: Vec<Value> = listed
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["repo"].clone())
            .collect();
        assert_eq!(repos, vec![healthy["repo"].clone()]);
        samples.push(ms);
    }
    let summary = summarize(&samples);
    assert!(summary.p99 < 250.0, "list p99 {} ms", summary.p99);
}
