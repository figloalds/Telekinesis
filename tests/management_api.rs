//! WP2.2: Management API v2, `ControlClient` and the status text
//! (REVAMP_PLAN §3.5, §7.2, §14 WP2.2).
//!
//! * Every new action against a fake `CatalogApi` and `FolderDirectory` with
//!   a real `StatusHub`: the intent is written before the folder command,
//!   each command waits as long as its action allows, and the reply's
//!   `status` already shows the controller's answer (read-your-writes).
//! * `subscribe` returns within 50 ms of a recorded event, and on timeout
//!   returns the current snapshot.
//! * `ControlClient` against a scripted control server: a mutating action
//!   whose connection drops after it was sent fails with `CONNECTION_LOST`
//!   and is never sent again (invariant 16), a read-only one is retried once,
//!   a connection the daemon closed is replaced before sending, an error
//!   reply keeps the connection (invariant 17), the deadline bounds the wait.
//! * `format_status` golden text for a fixture snapshot, and no pairing code
//!   in any reply or text (invariant 21).
use anyhow::Result;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    net::TcpListener,
    path::PathBuf,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};
use tkfs::{
    model::{
        errors::{ErrorRecord, ErrorWire, Scope, code_of},
        health::{
            ActivationCounters, ContentCounters, InboxCounts, JournalCounters, MountIntent,
            MountState, MountStatus,
        },
        orchestrator::{
            CallError, CatalogApi, CatalogCmd, CatalogSnapshot, Ctx, DaemonEndpoints, ExitRecord,
            FolderCmd, FolderDirectory, FolderPhase, FolderRecord, FolderView, GateRecord,
            GateState, PeerRecord, PublishedCatalogApi, PublishedEntry, ShutdownOutcome,
            StatusSink, StatusSource, WorkerClient, limits,
        },
        status::{
            ActivationStatus, ActivityEntry, DaemonStatus, Direction, FolderPeerState,
            FolderPeerSync, FolderStatus, FolderSync, FolderSyncState, GateKind, GateStatus,
            InflightOp, Intents, LaneRole, LaneState, LaneStatus, LifecyclePhase, LifecycleStatus,
            MetaProgress, PeerConnState, PeerStatus, STATUS_SCHEMA, StatusEvent, StatusSnapshot,
            StoreStatus, TransferProgress,
        },
    },
    orchestrator::{
        self, Action, Config, Network,
        hub::{HubConfig, StatusHub},
        manage_v2::{
            self, ControlClient, QUICK_WAIT, REVALIDATE_DEADLINE, format_folder, format_status,
        },
    },
    util::{
        cancel::Cancel,
        snapshot::Snapshot,
        time::{SystemClock, now_ms},
    },
    ws,
};

const STATE: &str = "3c0f8a21-1111-4222-8333-444455556666";
const REPO: &str = "repo-photos";
const PEER: &str = "inst-laptop";
/// A pairing code the fake catalog holds; it must never reach a reply.
const CODE: &str = "pairing-secret-7f3a-91c2";

// ===========================================================================
// Fakes

type Log = Arc<Mutex<Vec<String>>>;

fn push(log: &Log, entry: impl Into<String>) {
    log.lock().unwrap().push(entry.into());
}

fn entries(log: &Log) -> Vec<String> {
    log.lock().unwrap().clone()
}

struct FakeCatalog {
    snapshot: Mutex<Arc<CatalogSnapshot>>,
    log: Log,
}

impl CatalogApi for FakeCatalog {
    fn snapshot(&self) -> Arc<CatalogSnapshot> {
        self.snapshot.lock().unwrap().clone()
    }

    fn apply(&self, cmd: CatalogCmd) -> Result<Arc<CatalogSnapshot>, ErrorWire> {
        push(&self.log, format!("catalog:{}", cmd.name()));
        let mut current = self.snapshot.lock().unwrap();
        let mut next = (**current).clone();
        next.generation += 1;
        let folder = |next: &mut CatalogSnapshot, state: &str| {
            next.folders
                .iter_mut()
                .position(|folder| folder.state == state)
                .ok_or_else(|| ErrorWire::new("FOLDER_NOT_FOUND", "FOLDER_NOT_FOUND: fake"))
        };
        match cmd {
            CatalogCmd::SetDesired { state, desired } => {
                let index = folder(&mut next, &state)?;
                next.folders[index].desired_running = desired;
            }
            CatalogCmd::SetIntents {
                state,
                sync_enabled,
                mount_enabled,
            } => {
                let index = folder(&mut next, &state)?;
                if let Some(sync) = sync_enabled {
                    next.folders[index].sync_enabled = sync;
                }
                if let Some(mount) = mount_enabled {
                    next.folders[index].mount_enabled = mount;
                }
            }
            CatalogCmd::OverrideGate { state } => {
                if let Some(gate) = next.gates.get_mut(&state) {
                    gate.override_ms = Some(now_ms());
                }
            }
            other => panic!("unexpected catalog command {other:?}"),
        }
        *current = Arc::new(next);
        Ok(current.clone())
    }

    fn wait_newer(&self, _generation: u64, _timeout: Duration) -> Arc<CatalogSnapshot> {
        self.snapshot()
    }
}

/// A worker that answers `revalidate` with a fixed count.
struct FakeWorker {
    requeued: u64,
    log: Log,
}

impl WorkerClient for FakeWorker {
    fn instance(&self) -> &str {
        "instance-1"
    }

    fn call(&self, op: &str, payload: Value, deadline: Duration) -> Result<Value, CallError> {
        push(
            &self.log,
            format!("worker:{op} {payload} {}s", deadline.as_secs()),
        );
        match op {
            "revalidate" => Ok(json!({"requeued": self.requeued})),
            _ => Err(CallError::Remote(ErrorWire::new(
                "UNSUPPORTED_OPERATION",
                format!("UNSUPPORTED_OPERATION: {op}"),
            ))),
        }
    }

    fn call_bytes(
        &self,
        op: &str,
        _header: Value,
        _body: &[u8],
        _deadline: Duration,
    ) -> Result<(Value, Vec<u8>), CallError> {
        Err(CallError::Unavailable(format!(
            "UNSUPPORTED_OPERATION: {op}"
        )))
    }
}

/// A registry whose "controllers" apply each command to their view at once,
/// publish it to the hub (as a FolderController does before it answers) and
/// answer with it. It also checks that the catalog already holds the intent
/// a command depends on (invariant 26).
struct FakeFolders {
    hub: Arc<StatusHub>,
    catalog: Arc<FakeCatalog>,
    views: Mutex<BTreeMap<String, FolderView>>,
    worker: Mutex<Option<Arc<FakeWorker>>>,
    log: Log,
}

impl FakeFolders {
    fn publish(&self, view: &FolderView) {
        self.hub
            .record(StatusEvent::FolderLifecycle(Box::new(view.clone())));
    }
}

/// `attach`, `detach force=true`, `set_sync enabled=false`, …
fn cmd_text(cmd: &FolderCmd) -> String {
    let name = serde_json::to_value(cmd).unwrap()["cmd"]
        .as_str()
        .unwrap()
        .to_owned();
    match cmd {
        FolderCmd::Detach { force } => format!("{name} force={force}"),
        FolderCmd::SetSync { enabled } => format!("{name} enabled={enabled}"),
        FolderCmd::Revalidate { ids } => format!("{name} ids={}", json!(ids)),
        _ => name,
    }
}

impl FolderDirectory for FakeFolders {
    fn view(&self, state: &str) -> Option<Arc<FolderView>> {
        self.views.lock().unwrap().get(state).cloned().map(Arc::new)
    }

    fn by_repo(&self, repo: &str) -> Option<Arc<FolderView>> {
        let views = self.views.lock().unwrap();
        views
            .values()
            .find(|view| view.record.repo == repo)
            .cloned()
            .map(Arc::new)
    }

    fn all(&self) -> Vec<Arc<FolderView>> {
        let views = self.views.lock().unwrap();
        views.values().cloned().map(Arc::new).collect()
    }

    fn send(&self, state: &str, cmd: FolderCmd) -> Result<(), ErrorWire> {
        push(&self.log, format!("send:{} {state}", cmd_text(&cmd)));
        Ok(())
    }

    fn request(
        &self,
        state: &str,
        cmd: FolderCmd,
        wait: Duration,
    ) -> Result<Arc<FolderView>, ErrorWire> {
        push(
            &self.log,
            format!("folder:{} {}s", cmd_text(&cmd), wait.as_secs()),
        );
        let catalog = self.catalog.snapshot();
        let record = catalog.folder(state).cloned();
        let mut views = self.views.lock().unwrap();
        let view = views
            .get_mut(state)
            .ok_or_else(|| ErrorWire::new("FOLDER_NOT_FOUND", "FOLDER_NOT_FOUND: fake"))?;
        let now = now_ms();
        match &cmd {
            FolderCmd::Attach => {
                view.mount.intent = MountIntent::Attach;
                view.mount.state = MountState::Attached { since_ms: now };
            }
            FolderCmd::Detach { .. } => {
                view.mount.intent = MountIntent::Detach;
                view.mount.state = MountState::Detached;
            }
            FolderCmd::SetSync { enabled } => {
                if record.as_ref().map(|r| r.sync_enabled) != Some(*enabled) {
                    push(&self.log, "violation: set_sync before the catalog intent");
                }
                view.record.sync_enabled = *enabled;
            }
            FolderCmd::OverrideGate => {
                if view.gate != GateState::None {
                    view.gate = GateState::Overridden { at_ms: now };
                }
            }
            FolderCmd::ForceStop => {
                if record.as_ref().map(|r| r.desired_running) != Some(false) {
                    push(&self.log, "violation: force_stop before desired=0");
                }
                view.record.desired_running = false;
                view.phase = FolderPhase::Stopped;
                view.mount.state = MountState::Detached;
            }
            FolderCmd::Retry => {
                view.phase = FolderPhase::Starting {
                    since_ms: now,
                    attempt: 1,
                    phase: "opening the store".into(),
                    progress_seq: 1,
                    adopting: false,
                };
            }
            FolderCmd::Revalidate { .. } => {
                if let GateState::Blocked { source, .. } = &view.gate {
                    view.gate = GateState::Awaiting {
                        source: source.clone(),
                        since_ms: now,
                        waiting_on: vec![],
                    };
                }
            }
            other => panic!("unexpected folder command {other:?}"),
        }
        view.generation += 1;
        let view = view.clone();
        drop(views);
        self.publish(&view);
        Ok(Arc::new(view))
    }

    fn client(&self, _state: &str) -> Option<Arc<dyn WorkerClient>> {
        self.worker
            .lock()
            .unwrap()
            .clone()
            .map(|worker| worker as Arc<dyn WorkerClient>)
    }

    fn generation(&self) -> u64 {
        0
    }

    fn wait_newer(&self, generation: u64, _timeout: Duration) -> u64 {
        generation
    }

    fn shutdown(&self, _deadline: Duration) -> Vec<ShutdownOutcome> {
        vec![]
    }
}

struct FakePublished;

impl PublishedCatalogApi for FakePublished {
    fn update(&self, _state: &str, _entry: Option<PublishedEntry>) {}

    fn list(&self) -> (u64, Arc<Vec<PublishedEntry>>) {
        (0, Arc::new(vec![]))
    }

    fn wait_newer(&self, generation: u64, _timeout: Duration) -> u64 {
        generation
    }
}

struct Fx {
    ctx: Arc<Ctx>,
    hub: Arc<StatusHub>,
    catalog: Arc<FakeCatalog>,
    folders: Arc<FakeFolders>,
    log: Log,
    _dir: tempfile::TempDir,
}

fn record() -> FolderRecord {
    FolderRecord {
        state: STATE.into(),
        label: "Photos".into(),
        repo: REPO.into(),
        device: "device-local".into(),
        directory: PathBuf::from("C:/tkfs/data/folders").join(STATE),
        mount: Some(PathBuf::from("D:/Photos")),
        desired_running: true,
        sync_enabled: true,
        mount_enabled: true,
    }
}

fn running_view(gate: GateState) -> FolderView {
    FolderView {
        generation: 1,
        record: record(),
        phase: FolderPhase::Running {
            since_ms: now_ms(),
            instance: "instance-1".into(),
            pid: 5151,
            address: "ws://127.0.0.1:5555".into(),
            adopted: false,
        },
        gate,
        mount: MountStatus {
            intent: MountIntent::Attach,
            path: Some(PathBuf::from("D:/Photos")),
            state: MountState::Detached,
        },
        ..FolderView::default()
    }
}

/// A daemon context with one running folder (gated when `gate` is given).
fn fixture(gate: Option<GateRecord>, view_gate: GateState) -> Fx {
    let dir = tempfile::tempdir().unwrap();
    let log: Log = Arc::default();
    let hub = StatusHub::new(HubConfig::default(), Arc::new(SystemClock));
    let mut snapshot = CatalogSnapshot {
        generation: 1,
        installation: "inst-local".into(),
        folders: vec![record()],
        peers: vec![PeerRecord {
            endpoint: "ws://10.0.0.2:43180".into(),
            code: CODE.into(),
            installation: PEER.into(),
            connections: 4,
            revision: "r1".into(),
        }],
        ..CatalogSnapshot::default()
    };
    if let Some(gate) = gate {
        snapshot.gates.insert(STATE.into(), gate);
    }
    let catalog = Arc::new(FakeCatalog {
        snapshot: Mutex::new(Arc::new(snapshot)),
        log: log.clone(),
    });
    let folders = Arc::new(FakeFolders {
        hub: hub.clone(),
        catalog: catalog.clone(),
        views: Mutex::new(BTreeMap::from([(
            STATE.to_owned(),
            running_view(view_gate),
        )])),
        worker: Mutex::new(Some(Arc::new(FakeWorker {
            requeued: 3,
            log: log.clone(),
        }))),
        log: log.clone(),
    });
    folders.publish(&folders.view(STATE).unwrap());
    hub.record(StatusEvent::PeerConfigured {
        installation: PEER.into(),
        endpoint: "ws://10.0.0.2:43180".into(),
        display_name: None,
    });
    let config = Config {
        format_version: 2,
        data_directory: dir.path().join("data"),
        default_mount_directory: dir.path().join("mounts"),
        maximum_running: 8,
        network: Network::default(),
    };
    let ctx = Arc::new(Ctx {
        installation: "inst-local".into(),
        config: Arc::new(config),
        catalog: catalog.clone(),
        folders: folders.clone(),
        published: Arc::new(FakePublished),
        status: hub.clone(),
        status_source: hub.clone(),
        network: OnceLock::new(),
        legacy: OnceLock::new(),
        clock: Arc::new(SystemClock),
        shutdown: Cancel::new(),
        stopping: Arc::new(AtomicBool::new(false)),
        started_at_ms: now_ms(),
        endpoints: Snapshot::new(DaemonEndpoints::default()),
    });
    Fx {
        ctx,
        hub,
        catalog,
        folders,
        log,
        _dir: dir,
    }
}

fn plain() -> Fx {
    fixture(None, GateState::None)
}

fn gate_record() -> GateRecord {
    GateRecord {
        state: STATE.into(),
        source_installation: Some(PEER.into()),
        source_endpoint: "ws://10.0.0.2:43180".into(),
        created_ms: now_ms(),
        override_ms: None,
    }
}

fn handle(fx: &Fx, action: Action) -> Result<Value> {
    manage_v2::handle(&fx.ctx, action)
}

/// The reply's typed folder status (it must be present).
fn status_of(reply: &Value) -> FolderStatus {
    assert_eq!(reply["state"], STATE, "{reply}");
    serde_json::from_value(reply["status"].clone()).expect("a FolderStatus")
}

fn folder_log(fx: &Fx) -> Vec<String> {
    entries(&fx.log)
        .into_iter()
        .filter(|entry| !entry.starts_with("worker:"))
        .collect()
}

// ===========================================================================
// Actions

#[test]
fn status_returns_the_hub_snapshot() {
    let fx = plain();
    let reply = handle(&fx, Action::Status).unwrap();
    let snapshot: StatusSnapshot = serde_json::from_value(reply).unwrap();
    assert_eq!(snapshot.schema, STATUS_SCHEMA);
    let folder = snapshot
        .folders
        .iter()
        .find(|folder| folder.state == STATE)
        .expect("the folder");
    assert_eq!(folder.lifecycle.phase, LifecyclePhase::Running);
    assert_eq!(folder.label, "Photos");
    assert!(snapshot.peers.iter().any(|peer| peer.installation == PEER));
    assert_eq!(snapshot.generation, fx.hub.snapshot().generation);
    assert!(folder_log(&fx).is_empty(), "a read sends no command");
}

#[test]
fn subscribe_returns_within_50_ms_of_a_recorded_event() {
    let fx = plain();
    let generation = fx.hub.snapshot_now().generation;
    let (tx, rx) = mpsc::channel();
    let hub = fx.hub.clone();
    let recorder = thread::spawn(move || {
        // Let the subscriber block, and the snapshot throttle refill.
        thread::sleep(Duration::from_millis(400));
        let at = Instant::now();
        hub.record(StatusEvent::Activity(ActivityEntry {
            at_ms: now_ms(),
            kind: "test".into(),
            folder: None,
            peer: None,
            message: "an event".into(),
        }));
        tx.send(at).unwrap();
    });
    let reply = handle(
        &fx,
        Action::Subscribe {
            since: generation,
            wait_ms: Some(10_000),
        },
    )
    .unwrap();
    let returned = Instant::now();
    recorder.join().unwrap();
    let recorded = rx.recv().unwrap();
    let latency = returned.saturating_duration_since(recorded);
    assert!(latency < Duration::from_millis(50), "latency {latency:?}");
    let snapshot: StatusSnapshot = serde_json::from_value(reply).unwrap();
    assert!(snapshot.generation > generation);
    assert!(
        snapshot
            .activity
            .iter()
            .any(|entry| entry.message == "an event")
    );
}

#[test]
fn subscribe_times_out_with_the_current_snapshot() {
    let fx = plain();
    let current = fx.hub.snapshot_now();
    let started = Instant::now();
    let reply = handle(
        &fx,
        Action::Subscribe {
            since: current.generation,
            wait_ms: Some(300),
        },
    )
    .unwrap();
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(290) && waited < Duration::from_secs(2),
        "waited {waited:?}"
    );
    let snapshot: StatusSnapshot = serde_json::from_value(reply).unwrap();
    assert_eq!(snapshot.generation, current.generation);
    assert_eq!(snapshot, *current, "the current snapshot, unchanged");

    // A generation from a previous daemon run (ahead of the hub) and an
    // over-long wait: the current snapshot at once.
    let started = Instant::now();
    let reply = handle(
        &fx,
        Action::Subscribe {
            since: current.generation + 1_000,
            wait_ms: Some(600_000),
        },
    )
    .unwrap();
    assert!(started.elapsed() < Duration::from_secs(1));
    let snapshot: StatusSnapshot = serde_json::from_value(reply).unwrap();
    assert_eq!(snapshot.generation, current.generation);
}

#[test]
fn attach_and_detach_wait_for_the_mount_and_reply_with_it() {
    let fx = plain();
    let reply = handle(
        &fx,
        Action::Attach {
            state: STATE.into(),
        },
    )
    .unwrap();
    let status = status_of(&reply);
    assert!(status.mount.state.is_attached(), "{:?}", status.mount);
    let attach_wait = limits::ATTACH_DEADLINE.as_secs();
    assert_eq!(folder_log(&fx), [format!("folder:attach {attach_wait}s")]);

    let reply = handle(
        &fx,
        Action::Detach {
            state: STATE.into(),
            force: true,
        },
    )
    .unwrap();
    let status = status_of(&reply);
    assert_eq!(status.mount.state, MountState::Detached);
    assert_eq!(status.mount.intent, MountIntent::Detach);
    handle(
        &fx,
        Action::Detach {
            state: STATE.into(),
            force: false,
        },
    )
    .unwrap();
    assert_eq!(
        &folder_log(&fx)[1..],
        [
            format!("folder:detach force=true {attach_wait}s"),
            format!("folder:detach force=false {attach_wait}s")
        ]
    );
}

#[test]
fn pause_and_resume_write_the_intent_before_the_command() {
    let fx = plain();
    let quick = QUICK_WAIT.as_secs();
    let reply = handle(
        &fx,
        Action::PauseSync {
            state: STATE.into(),
        },
    )
    .unwrap();
    assert_eq!(
        folder_log(&fx),
        [
            "catalog:set_intents".to_owned(),
            format!("folder:set_sync enabled=false {quick}s")
        ]
    );
    assert!(!fx.catalog.snapshot().folder(STATE).unwrap().sync_enabled);
    let status = status_of(&reply);
    assert!(!status.intents.sync_enabled, "read-your-writes");
    assert_eq!(status.sync.state, FolderSyncState::Paused);

    // Already paused: no second catalog write, the controller is still told.
    handle(
        &fx,
        Action::PauseSync {
            state: STATE.into(),
        },
    )
    .unwrap();
    assert_eq!(folder_log(&fx).len(), 3);
    assert_eq!(
        folder_log(&fx)[2],
        format!("folder:set_sync enabled=false {quick}s")
    );

    let reply = handle(
        &fx,
        Action::ResumeSync {
            state: STATE.into(),
        },
    )
    .unwrap();
    assert!(status_of(&reply).intents.sync_enabled);
    assert!(fx.catalog.snapshot().folder(STATE).unwrap().sync_enabled);
    assert_eq!(
        &folder_log(&fx)[3..],
        [
            "catalog:set_intents".to_owned(),
            format!("folder:set_sync enabled=true {quick}s")
        ]
    );
    assert!(
        !entries(&fx.log)
            .iter()
            .any(|entry| entry.starts_with("violation"))
    );
}

#[test]
fn override_gate_writes_an_existing_gate_then_tells_the_folder() {
    let gate = GateState::Awaiting {
        source: PEER.into(),
        since_ms: now_ms(),
        waiting_on: vec!["metadata".into()],
    };
    let fx = fixture(Some(gate_record()), gate);
    let reply = handle(
        &fx,
        Action::OverrideGate {
            state: STATE.into(),
        },
    )
    .unwrap();
    assert_eq!(
        folder_log(&fx),
        [
            "catalog:override_gate".to_owned(),
            format!("folder:override_gate {}s", QUICK_WAIT.as_secs())
        ]
    );
    assert!(
        fx.catalog.snapshot().gates[STATE].override_ms.is_some(),
        "the override is durable"
    );
    assert_eq!(status_of(&reply).gate.state, GateKind::Overridden);

    // Overridden already, or no gate at all ("mount now" on a plain folder):
    // no catalog write, the controller still re-arms the attach.
    handle(
        &fx,
        Action::OverrideGate {
            state: STATE.into(),
        },
    )
    .unwrap();
    let fx = plain();
    handle(
        &fx,
        Action::OverrideGate {
            state: STATE.into(),
        },
    )
    .unwrap();
    assert_eq!(
        folder_log(&fx),
        [format!("folder:override_gate {}s", QUICK_WAIT.as_secs())]
    );
}

#[test]
fn force_stop_persists_desired_before_stopping() {
    let fx = plain();
    let reply = handle(
        &fx,
        Action::ForceStop {
            state: STATE.into(),
        },
    )
    .unwrap();
    assert_eq!(
        folder_log(&fx),
        [
            "catalog:set_desired".to_owned(),
            format!("folder:force_stop {}s", limits::API_STOP_WAIT.as_secs())
        ]
    );
    assert!(!fx.catalog.snapshot().folder(STATE).unwrap().desired_running);
    let status = status_of(&reply);
    assert_eq!(status.lifecycle.phase, LifecyclePhase::Stopped);
    assert!(!status.intents.desired_running);
}

#[test]
fn retry_tells_the_folder_and_shows_it_starting() {
    let fx = plain();
    let reply = handle(
        &fx,
        Action::Retry {
            state: STATE.into(),
        },
    )
    .unwrap();
    assert_eq!(
        folder_log(&fx),
        [format!("folder:retry {}s", QUICK_WAIT.as_secs())]
    );
    assert_eq!(
        status_of(&reply).lifecycle.phase,
        LifecyclePhase::Starting,
        "read-your-writes"
    );
}

#[test]
fn revalidate_asks_the_worker_and_reopens_a_blocked_gate() {
    let fx = plain();
    let reply = handle(
        &fx,
        Action::Revalidate {
            state: STATE.into(),
            ids: Some(vec!["event-a".into(), "event-b".into()]),
        },
    )
    .unwrap();
    assert_eq!(reply["requeued"], 3);
    status_of(&reply);
    assert_eq!(
        entries(&fx.log),
        [format!(
            "worker:revalidate {} {}s",
            json!({"ids": ["event-a", "event-b"]}),
            REVALIDATE_DEADLINE.as_secs()
        )],
        "no folder command while the gate is not blocked"
    );

    let blocked = GateState::Blocked {
        source: PEER.into(),
        reason: "3 quarantined events".into(),
    };
    let fx = fixture(Some(gate_record()), blocked);
    let reply = handle(
        &fx,
        Action::Revalidate {
            state: STATE.into(),
            ids: None,
        },
    )
    .unwrap();
    assert_eq!(reply["requeued"], 3);
    assert_eq!(
        entries(&fx.log),
        [
            format!(
                "worker:revalidate {} {}s",
                json!({"ids": null}),
                REVALIDATE_DEADLINE.as_secs()
            ),
            format!(
                "folder:revalidate ids=[] {}s",
                REVALIDATE_DEADLINE.as_secs()
            )
        ],
        "the worker re-queues once; the controller only reopens the gate"
    );
    assert_eq!(status_of(&reply).gate.state, GateKind::Awaiting);

    *fx.folders.worker.lock().unwrap() = None;
    let error = handle(
        &fx,
        Action::Revalidate {
            state: STATE.into(),
            ids: None,
        },
    )
    .unwrap_err();
    assert_eq!(code_of(&error), "WORKER_NOT_RUNNING");
}

#[test]
fn diagnostics_returns_the_bundle() {
    let fx = plain();
    let reply = handle(&fx, Action::Diagnostics).unwrap();
    assert_eq!(reply["schema"], 1);
}

#[test]
fn folder_actions_are_recorded_as_activity() {
    let fx = plain();
    handle(
        &fx,
        Action::Attach {
            state: STATE.into(),
        },
    )
    .unwrap();
    let snapshot = fx.hub.snapshot_now();
    assert!(
        snapshot.activity.iter().any(|entry| entry.kind == "action"
            && entry.folder.as_deref() == Some(STATE)
            && entry.message == "attach requested"),
        "{:?}",
        snapshot.activity
    );
}

fn every_folder_action(state: &str) -> Vec<Action> {
    let state = || state.to_owned();
    vec![
        Action::Attach { state: state() },
        Action::Detach {
            state: state(),
            force: false,
        },
        Action::PauseSync { state: state() },
        Action::ResumeSync { state: state() },
        Action::OverrideGate { state: state() },
        Action::ForceStop { state: state() },
        Action::Retry { state: state() },
        Action::Revalidate {
            state: state(),
            ids: None,
        },
    ]
}

#[test]
fn unknown_folders_and_a_stopping_daemon_are_refused_before_any_write() {
    let fx = plain();
    for action in every_folder_action("no-such-folder") {
        let op = serde_json::to_value(&action).unwrap()["op"].clone();
        let error = handle(&fx, action).unwrap_err();
        assert_eq!(code_of(&error), "FOLDER_NOT_FOUND", "{op}: {error:#}");
    }
    assert!(entries(&fx.log).is_empty(), "{:?}", entries(&fx.log));

    fx.ctx.stopping.store(true, Ordering::Release);
    for action in every_folder_action(STATE) {
        let op = serde_json::to_value(&action).unwrap()["op"].clone();
        let error = handle(&fx, action).unwrap_err();
        assert_eq!(code_of(&error), "ORCHESTRATOR_STOPPING", "{op}: {error:#}");
    }
    assert!(entries(&fx.log).is_empty(), "{:?}", entries(&fx.log));
    // Reads keep working while the daemon stops.
    handle(&fx, Action::Status).unwrap();
}

#[test]
fn legacy_actions_are_not_served_here() {
    let fx = plain();
    for action in [
        Action::List,
        Action::Peers,
        Action::Shutdown,
        Action::Start {
            state: STATE.into(),
        },
    ] {
        let error = handle(&fx, action).unwrap_err();
        assert_eq!(code_of(&error), "UNSUPPORTED_OPERATION");
    }
    assert!(entries(&fx.log).is_empty());
}

#[test]
fn no_pairing_code_in_replies_or_text() {
    let fx = plain();
    fx.hub.record(StatusEvent::Error {
        scope: Scope::Peer {
            installation: PEER.into(),
        },
        error: ErrorWire::new("INVALID_PAIRING_CODE", "INVALID_PAIRING_CODE: rejected"),
    });
    let mut texts = vec![];
    for action in [Action::Status, Action::Diagnostics] {
        texts.push(handle(&fx, action).unwrap().to_string());
    }
    for action in every_folder_action(STATE) {
        texts.push(handle(&fx, action).unwrap().to_string());
    }
    let snapshot = fx.hub.snapshot_now();
    texts.push(format_status(&snapshot));
    for folder in &snapshot.folders {
        texts.push(format_folder(folder, &snapshot.peers, snapshot.at_ms));
    }
    for text in texts {
        assert!(!text.contains(CODE), "pairing code leaked: {text}");
    }
}

// ===========================================================================
// ControlClient

/// What the scripted control server does with one request.
#[derive(Clone)]
enum Answer {
    Reply(Value),
    Error(&'static str),
    /// Closes the connection without answering (a daemon crash).
    Drop,
    /// Answers after a delay.
    Late(Duration),
    /// Answers, then closes the connection (an idle timeout, or a 6298d0b
    /// daemon, which serves one request per connection).
    ReplyThenClose(Value),
}

type Script = dyn Fn(&str, usize) -> Answer + Send + Sync;

/// A control endpoint speaking the daemon's wire (`ws::accept`,
/// `ws::receive`, `ws::reply`). `script(op, connection)` decides each answer.
struct ControlServer {
    endpoint: String,
    received: Arc<Mutex<Vec<(usize, String)>>>,
    connections: Arc<AtomicUsize>,
}

impl ControlServer {
    fn start(script: impl Fn(&str, usize) -> Answer + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let received: Arc<Mutex<Vec<(usize, String)>>> = Arc::default();
        let connections = Arc::new(AtomicUsize::new(0));
        let script: Arc<Script> = Arc::new(script);
        let (seen, count) = (received.clone(), connections.clone());
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let index = count.fetch_add(1, Ordering::SeqCst);
                let (seen, script) = (seen.clone(), script.clone());
                thread::spawn(move || {
                    let Ok(mut socket) = ws::accept(stream) else {
                        return;
                    };
                    while let Ok(action) = ws::receive::<Value>(&mut socket) {
                        let op = action["op"].as_str().unwrap_or_default().to_owned();
                        seen.lock().unwrap().push((index, op.clone()));
                        match script(&op, index) {
                            Answer::Reply(value) => {
                                let _ = ws::reply(&mut socket, Ok(value));
                            }
                            Answer::Error(message) => {
                                let _ = ws::reply(&mut socket, Err(anyhow::anyhow!(message)));
                            }
                            Answer::Drop => return,
                            Answer::Late(delay) => {
                                thread::sleep(delay);
                                let _ = ws::reply(&mut socket, Ok(json!({"late": true})));
                            }
                            Answer::ReplyThenClose(value) => {
                                let _ = ws::reply(&mut socket, Ok(value));
                                return;
                            }
                        }
                    }
                });
            }
        });
        Self {
            endpoint,
            received,
            connections,
        }
    }

    /// How many requests of `op` arrived.
    fn count(&self, op: &str) -> usize {
        let received = self.received.lock().unwrap();
        received.iter().filter(|(_, seen)| seen == op).count()
    }
}

fn start() -> Action {
    Action::Start {
        state: STATE.into(),
    }
}

const DEADLINE: Duration = Duration::from_secs(5);

#[test]
fn a_mutating_action_lost_after_sending_is_never_replayed() {
    let server = ControlServer::start(|op, _| match op {
        "start" => Answer::Drop,
        _ => Answer::Reply(json!({"folders": []})),
    });
    let mut client = ControlClient::connect_endpoint(&server.endpoint).unwrap();
    let error = client.call(&start(), DEADLINE).unwrap_err();
    assert_eq!(code_of(&error), "CONNECTION_LOST", "{error:#}");
    assert!(
        error.to_string().contains("not sent again"),
        "the caller learns it must check: {error:#}"
    );
    thread::sleep(Duration::from_millis(200));
    assert_eq!(server.count("start"), 1, "never replayed");
    // A read-only action reconnects.
    let reply = client.call(&Action::List, DEADLINE).unwrap();
    assert_eq!(reply, json!({"folders": []}));
    assert_eq!(client.connections(), 2);
    assert_eq!(server.count("start"), 1);
}

#[test]
fn a_read_only_action_is_retried_once_on_a_fresh_connection() {
    let server = ControlServer::start(|op, connection| match (op, connection) {
        ("list", 0) | ("status", _) => Answer::Drop,
        _ => Answer::Reply(json!({"folders": []})),
    });
    let mut client = ControlClient::connect_endpoint(&server.endpoint).unwrap();
    let reply = client.call(&Action::List, DEADLINE).unwrap();
    assert_eq!(reply, json!({"folders": []}));
    assert_eq!(server.count("list"), 2, "sent again on a fresh connection");
    assert_eq!(client.connections(), 2);
    // Retried once only.
    let error = client.call(&Action::Status, DEADLINE).unwrap_err();
    assert_eq!(code_of(&error), "CONNECTION_LOST", "{error:#}");
    assert_eq!(server.count("status"), 2);
}

#[test]
fn a_connection_the_daemon_closed_is_replaced_before_sending() {
    let server = ControlServer::start(|_, _| Answer::ReplyThenClose(json!({"starting": true})));
    let mut client = ControlClient::connect_endpoint(&server.endpoint).unwrap();
    assert_eq!(
        client.call(&start(), DEADLINE).unwrap(),
        json!({"starting": true})
    );
    thread::sleep(Duration::from_millis(200));
    // The second mutation goes out exactly once, on a new connection.
    assert_eq!(
        client.call(&start(), DEADLINE).unwrap(),
        json!({"starting": true})
    );
    assert_eq!(server.count("start"), 2);
    assert_eq!(client.connections(), 2);
    assert_eq!(server.connections.load(Ordering::SeqCst), 2);
}

#[test]
fn an_error_reply_keeps_the_connection() {
    let server = ControlServer::start(|op, _| match op {
        "start" => Answer::Error("FOLDER_NOT_FOUND: no folder with that state"),
        _ => Answer::Reply(json!({"folders": []})),
    });
    let mut client = ControlClient::connect_endpoint(&server.endpoint).unwrap();
    let error = client.call(&start(), DEADLINE).unwrap_err();
    assert_eq!(code_of(&error), "FOLDER_NOT_FOUND");
    assert!(error.downcast_ref::<ws::RemoteError>().is_some());
    client.call(&Action::List, DEADLINE).unwrap();
    assert_eq!(client.connections(), 1, "invariant 17");
    assert_eq!(server.count("start"), 1);
}

#[test]
fn the_deadline_bounds_the_wait_and_nothing_late_is_replayed() {
    let server = ControlServer::start(|op, _| match op {
        "start" | "status" => Answer::Late(Duration::from_millis(1500)),
        _ => Answer::Reply(json!({"folders": []})),
    });
    let mut client = ControlClient::connect_endpoint(&server.endpoint).unwrap();
    let started = Instant::now();
    let error = client
        .call(&start(), Duration::from_millis(300))
        .unwrap_err();
    assert_eq!(code_of(&error), "REQUEST_TIMEOUT", "{error:#}");
    assert!(started.elapsed() < Duration::from_millis(1200));
    // A read-only action whose deadline is spent is not retried either.
    let error = client
        .call(&Action::Status, Duration::from_millis(300))
        .unwrap_err();
    assert_eq!(code_of(&error), "REQUEST_TIMEOUT", "{error:#}");
    thread::sleep(Duration::from_millis(1700));
    assert_eq!(server.count("start"), 1);
    assert_eq!(server.count("status"), 1);
    // The client recovers on a fresh connection.
    client.call(&Action::List, DEADLINE).unwrap();
}

#[test]
fn one_connection_serves_many_calls() {
    let server = ControlServer::start(|_, _| Answer::Reply(json!({"folders": []})));
    let mut client = ControlClient::connect_endpoint(&server.endpoint).unwrap();
    for _ in 0..20 {
        client.call(&Action::List, DEADLINE).unwrap();
    }
    assert_eq!(client.connections(), 1);
    assert_eq!(server.connections.load(Ordering::SeqCst), 1);
    assert_eq!(client.endpoint(), Some(server.endpoint.as_str()));
}

#[test]
fn the_client_follows_the_discovery_file_to_a_restarted_daemon() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        format_version: 2,
        data_directory: dir.path().to_owned(),
        default_mount_directory: dir.path().join("mounts"),
        maximum_running: 8,
        network: Network::default(),
    };
    let error = ControlClient::connect(&config).unwrap_err();
    assert_eq!(code_of(&error), "ORCHESTRATOR_NOT_RUNNING");
    let error = orchestrator::client(&config, &Action::List).unwrap_err();
    assert_eq!(code_of(&error), "ORCHESTRATOR_NOT_RUNNING");

    let discovery = |endpoint: &str| {
        std::fs::write(
            dir.path().join("orchestrator.json"),
            serde_json::to_vec(&json!({"format": 3, "control": endpoint})).unwrap(),
        )
        .unwrap();
    };
    let first = ControlServer::start(|_, _| Answer::ReplyThenClose(json!({"from": "first"})));
    discovery(&first.endpoint);
    let mut client = ControlClient::connect(&config).unwrap();
    assert_eq!(
        client.call(&Action::List, DEADLINE).unwrap(),
        json!({"from": "first"})
    );
    // The daemon restarts on another port.
    let second = ControlServer::start(|_, _| Answer::Reply(json!({"from": "second"})));
    discovery(&second.endpoint);
    thread::sleep(Duration::from_millis(200));
    assert_eq!(
        client.call(&Action::List, DEADLINE).unwrap(),
        json!({"from": "second"})
    );
    assert_eq!(client.endpoint(), Some(second.endpoint.as_str()));
    assert_eq!(
        orchestrator::client(&config, &Action::List).unwrap(),
        json!({"from": "second"})
    );
}

#[test]
fn read_only_actions_and_deadlines() {
    use manage_v2::{default_deadline, is_read_only};
    let state = || STATE.to_owned();
    for action in [
        Action::List,
        Action::Inspect { state: state() },
        Action::Peers,
        Action::Status,
        Action::Subscribe {
            since: 0,
            wait_ms: None,
        },
        Action::RemoteList {
            endpoint: "ws://x".into(),
        },
    ] {
        assert!(is_read_only(&action), "{action:?}");
    }
    let mut mutating = every_folder_action(STATE);
    mutating.extend([
        start(),
        Action::Stop {
            state: state(),
            wait: None,
        },
        Action::Shutdown,
        Action::Diagnostics,
        Action::ResetPairingCode,
        Action::Folder {
            state: state(),
            request: "r".into(),
            payload: json!({"op": "resolve"}),
        },
    ]);
    for action in &mutating {
        assert!(!is_read_only(action), "{action:?}");
    }
    // A long poll's deadline covers its wait (beyond the 15 s socket default).
    let subscribe = |wait_ms| Action::Subscribe { since: 0, wait_ms };
    assert!(default_deadline(&subscribe(Some(25_000))) > Duration::from_secs(25));
    assert_eq!(
        default_deadline(&subscribe(None)),
        default_deadline(&subscribe(Some(600_000))),
        "clamped to the 25 s maximum"
    );
    assert!(
        default_deadline(&Action::Attach { state: state() }) > limits::ATTACH_DEADLINE,
        "an attach waits up to its deadline"
    );
    assert!(default_deadline(&start()) >= Duration::from_secs(15));
}

// ===========================================================================
// Status text

/// "Now" of the fixture snapshot.
const T: u64 = 1_790_000_000_000;
const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

fn error(
    code: &str,
    message: &str,
    scope: Scope,
    first_ago_ms: u64,
    last_ago_ms: u64,
    count: u64,
) -> ErrorRecord {
    ErrorRecord {
        error: ErrorWire::new(code, message),
        scope,
        first_at_ms: T - first_ago_ms,
        last_at_ms: T - last_ago_ms,
        count,
        next_retry_at_ms: None,
        resolved_at_ms: None,
    }
}

fn peer_row(installation: &str, state: FolderPeerState) -> FolderPeerSync {
    FolderPeerSync {
        installation: installation.into(),
        endpoint: Some("ws://10.0.0.2:43180".into()),
        label: Some("laptop".into()),
        direction: Direction::Both,
        state,
        ..FolderPeerSync::default()
    }
}

fn mount(path: &str, intent: MountIntent, state: MountState) -> MountStatus {
    MountStatus {
        intent,
        path: Some(PathBuf::from(path)),
        state,
    }
}

/// A daemon with three folders (downloading; failed and paused; waiting for
/// its initial sync while uploading), two peers (one online with a lane in
/// backoff, one that needs an upgrade) and some activity.
fn fixture_snapshot() -> StatusSnapshot {
    let photos = FolderStatus {
        state: STATE.into(),
        label: "Photos".into(),
        repo: REPO.into(),
        device: "device-local".into(),
        lifecycle: LifecycleStatus {
            phase: LifecyclePhase::Running,
            detail: Some("adopted".into()),
            since_ms: T - 7_380_000,
            pid: Some(5151),
            instance: Some("instance-1".into()),
            adopted: true,
            restarts: 1,
            ..LifecycleStatus::default()
        },
        intents: Intents {
            desired_running: true,
            sync_enabled: true,
            mount_enabled: true,
        },
        mount: mount(
            "D:\\Photos",
            MountIntent::Attach,
            MountState::Attached {
                since_ms: T - 7_380_000,
            },
        ),
        gate: GateStatus::default(),
        store: StoreStatus {
            open_handles: 2,
            ..StoreStatus::default()
        },
        activation: ActivationStatus {
            inbox: InboxCounts {
                ready: 400,
                waiting: 10,
                retry: 2,
                blocked: 1,
                quarantined: 3,
                unclassified: 0,
            },
            counters: ActivationCounters {
                running: true,
                activated: 5_000,
                fast: 4_900,
                slow: 100,
                last_activation_at_ms: Some(T - 2_000),
                ..ActivationCounters::default()
            },
        },
        content: ContentCounters {
            current_objects: 1_300,
            current_available: 1_234,
            current_missing: 66,
            current_missing_bytes: GIB + GIB / 5,
            shared_objects: 1_500,
            shared_available: 1_400,
            downloading: 3,
            partial_bytes: 12 * MIB,
            complete: false,
            ..ContentCounters::default()
        },
        journal: JournalCounters {
            events: 5_432,
            shared_events: 5_000,
            tip: 5_432,
            outgoing_unacknowledged: 12,
            unresolved_conflicts: 2,
            ..JournalCounters::default()
        },
        sync: FolderSync {
            state: FolderSyncState::Downloading,
            summary: "Downloading · 1 of 1 peer · 1.2 GiB left · 3.4 MiB/s · ETA 6 min".into(),
            peers: vec![
                FolderPeerSync {
                    metadata: MetaProgress {
                        to_receive: 412,
                        local_pending: 3,
                        caught_up: false,
                        last_exchange_ms: Some(T - 2_000),
                        ..MetaProgress::default()
                    },
                    objects_down: TransferProgress {
                        objects_remaining: 66,
                        bytes_remaining: GIB + GIB / 5,
                        in_flight: 2,
                        rate_bps: 3_565_158,
                        eta_s: Some(355),
                        total_bytes: 3 * GIB,
                        last_progress_ms: Some(T - 3_000),
                    },
                    last_progress_ms: Some(T - 3_000),
                    error: Some(ErrorRecord {
                        next_retry_at_ms: Some(T + 8_000),
                        ..error(
                            "REQUEST_TIMEOUT",
                            "REQUEST_TIMEOUT: no reply after 30000 ms",
                            Scope::FolderPeer {
                                state: STATE.into(),
                                installation: PEER.into(),
                            },
                            60_000,
                            12_000,
                            5,
                        )
                    }),
                    ..peer_row(PEER, FolderPeerState::Downloading)
                },
                FolderPeerSync {
                    endpoint: Some("ws://10.0.0.9:43180".into()),
                    label: None,
                    ..peer_row("inst-old", FolderPeerState::NotShared)
                },
            ],
        },
        errors: vec![
            error(
                "CORRUPT_OBJECT",
                "CORRUPT_OBJECT: hash mismatch after download",
                Scope::Object {
                    state: STATE.into(),
                    hash: "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08".into(),
                },
                600_000,
                90_000,
                2,
            ),
            ErrorRecord {
                resolved_at_ms: Some(T - 1_800_000),
                ..error(
                    "WORKER_EXITED",
                    "WORKER_EXITED: exit code 70",
                    Scope::Folder {
                        state: STATE.into(),
                    },
                    7_400_000,
                    7_400_000,
                    1,
                )
            },
        ],
    };
    let docs = FolderStatus {
        state: "9a8b7c6d-2222-4333-8444-555566667777".into(),
        label: "Docs".into(),
        repo: "repo-docs".into(),
        device: "device-docs".into(),
        lifecycle: LifecycleStatus {
            phase: LifecyclePhase::Failed,
            detail: Some("CRASH_LOOP: 5 worker failures in 10 min".into()),
            since_ms: T - 240_000,
            restarts: 5,
            last_exit: Some(ExitRecord {
                code: Some(1),
                fatal: None,
                stderr_tail: "opening the store\nerror: disk I/O error (os error 5)\n\n".into(),
                at_ms: T - 245_000,
            }),
            ..LifecycleStatus::default()
        },
        intents: Intents {
            desired_running: true,
            sync_enabled: false,
            mount_enabled: true,
        },
        mount: mount("D:\\Docs", MountIntent::Attach, MountState::Detached),
        sync: FolderSync {
            state: FolderSyncState::NeedsAttention,
            summary: "Needs attention: CRASH_LOOP: 5 worker failures in 10 min".into(),
            peers: vec![],
        },
        errors: vec![error(
            "CRASH_LOOP",
            "CRASH_LOOP: 5 worker failures in 10 min",
            Scope::Folder {
                state: "9a8b7c6d-2222-4333-8444-555566667777".into(),
            },
            240_000,
            240_000,
            1,
        )],
        ..FolderStatus::default()
    };
    let received = FolderStatus {
        state: "5e4d3c2b-3333-4444-8555-666677778888".into(),
        label: "Received".into(),
        repo: "repo-received".into(),
        device: "device-received".into(),
        lifecycle: LifecycleStatus {
            phase: LifecyclePhase::Running,
            since_ms: T - 185_000,
            pid: Some(6262),
            ..LifecycleStatus::default()
        },
        intents: Intents {
            desired_running: true,
            sync_enabled: true,
            mount_enabled: true,
        },
        mount: mount("E:\\Received", MountIntent::Attach, MountState::Detached),
        gate: GateStatus {
            state: GateKind::Awaiting,
            source: Some(PEER.into()),
            since_ms: Some(T - 180_000),
            waiting_on: vec!["metadata".into(), "remote pending events".into()],
        },
        content: ContentCounters {
            current_objects: 40,
            current_available: 40,
            complete: true,
            ..ContentCounters::default()
        },
        journal: JournalCounters {
            events: 1,
            ..JournalCounters::default()
        },
        sync: FolderSync {
            state: FolderSyncState::InitialSync,
            summary: "Waiting for initial sync from laptop (ws://10.0.0.2:43180): metadata".into(),
            peers: vec![FolderPeerSync {
                metadata: MetaProgress {
                    to_receive: 1_200,
                    remote_pending: 4,
                    last_exchange_ms: Some(T - 500),
                    ..MetaProgress::default()
                },
                objects_up: TransferProgress {
                    objects_remaining: 12,
                    bytes_remaining: 3 * MIB,
                    rate_bps: MIB,
                    eta_s: Some(3),
                    ..TransferProgress::default()
                },
                ..peer_row(PEER, FolderPeerState::Syncing)
            }],
        },
        ..FolderStatus::default()
    };
    let laptop = PeerStatus {
        installation: PEER.into(),
        endpoint: Some("ws://10.0.0.2:43180".into()),
        label: Some("laptop".into()),
        direction: Direction::Both,
        state: PeerConnState::Online,
        protocol: None,
        since_ms: Some(T - 7_200_000),
        rtt_ms: Some(12),
        rate_up_bps: MIB,
        rate_down_bps: 3_565_158,
        lanes: vec![
            LaneStatus {
                lane: 0,
                role: LaneRole::Control,
                state: LaneState::Ready,
                ..LaneStatus::default()
            },
            LaneStatus {
                lane: 1,
                role: LaneRole::Bulk,
                state: LaneState::Ready,
                inflight: vec![InflightOp {
                    op: "published-bulk-exchange".into(),
                    repo: REPO.into(),
                    since_ms: T - 1_000,
                    bytes: 0,
                }],
                ..LaneStatus::default()
            },
            LaneStatus {
                lane: 2,
                role: LaneRole::Bulk,
                state: LaneState::Backoff,
                backoff_until_ms: Some(T + 5_000),
                ..LaneStatus::default()
            },
        ],
        inbound_sessions: 1,
        shared_folders: vec![REPO.into(), "repo-received".into()],
        error: None,
    };
    let old = PeerStatus {
        installation: "inst-old".into(),
        endpoint: Some("ws://10.0.0.9:43180".into()),
        label: None,
        direction: Direction::Outbound,
        state: PeerConnState::Incompatible,
        lanes: vec![LaneStatus {
            lane: 0,
            role: LaneRole::Control,
            state: LaneState::Incompatible,
            ..LaneStatus::default()
        }],
        error: Some(error(
            "PEER_NEEDS_UPGRADE",
            "PEER_NEEDS_UPGRADE: the peer speaks tkfs-peer/2",
            Scope::Peer {
                installation: "inst-old".into(),
            },
            86_400_000 + 3_600_000,
            600_000,
            3,
        )),
        ..PeerStatus::default()
    };
    StatusSnapshot {
        schema: STATUS_SCHEMA,
        generation: 42,
        at_ms: T,
        daemon: DaemonStatus {
            installation: "1f2e3d4c-5b6a-4789-8abc-def012345678".into(),
            version: "0.1.0+abc1234".into(),
            pid: 4242,
            started_at_ms: T - 7_500_000,
            stopping: false,
            control: "ws://127.0.0.1:50123".into(),
            network: Some("ws://0.0.0.0:43180".into()),
            log_path: Some("C:\\tkfs\\data\\daemon.log".into()),
            errors: vec![
                error(
                    "PAIRING_CODE_NOT_SET",
                    "PAIRING_CODE_NOT_SET: run tkfs manage reset-pairing-code",
                    Scope::Daemon,
                    300_000,
                    300_000,
                    1,
                ),
                // Listed under its peer, not under the daemon.
                old.error.clone().unwrap(),
            ],
        },
        folders: vec![photos, docs, received],
        peers: vec![laptop, old],
        activity: vec![
            ActivityEntry {
                at_ms: T - 240_000,
                kind: "lifecycle".into(),
                folder: Some("9a8b7c6d-2222-4333-8444-555566667777".into()),
                peer: None,
                message: "failed: CRASH_LOOP: 5 worker failures in 10 min".into(),
            },
            ActivityEntry {
                at_ms: T - 60_000,
                kind: "peer".into(),
                folder: None,
                peer: Some("inst-old".into()),
                message: "incompatible".into(),
            },
            ActivityEntry {
                at_ms: T - 12_000,
                kind: "action".into(),
                folder: Some(STATE.into()),
                peer: None,
                message: "attach requested".into(),
            },
        ],
    }
}

const GOLDEN: &str = r#"Telekinesis daemon 0.1.0+abc1234 · pid 4242 · up 2 h 5 min
  installation 1f2e3d4c-5b6a-4789-8abc-def012345678
  control      ws://127.0.0.1:50123
  network      ws://0.0.0.0:43180
  log          C:\tkfs\data\daemon.log
  errors       ! [needs_user_action] PAIRING_CODE_NOT_SET: run tkfs manage reset-pairing-code · at 14:08:20 UTC (5 min ago)

3 folders (1 needs attention, 1 initial sync, 1 downloading) · 2 peers (1 online, 1 incompatible)

Photos [3c0f8a21]
  sync       Downloading · 1 of 1 peer · 1.2 GiB left · 3.4 MiB/s · ETA 6 min
  lifecycle  running for 2 h 3 min · pid 5151 · adopted · 1 restart
  mount      attached at D:\Photos for 2 h 3 min
  store      healthy · 2 open files
  inbox      412 events to activate (400 ready, 10 waiting for parents, 2 retrying) · 3 quarantined · 1 blocked
  activation running · 5,000 activated (4,900 fast, 100 slow) · last 2 s ago
  content    1,234 of 1,300 files available · 66 missing (1.2 GiB) · 3 downloading (12.0 MiB so far) · history 1,400 of 1,500 available
  journal    5,432 events · 12 not yet acknowledged by a peer · 2 unresolved conflicts
  peer       laptop (ws://10.0.0.2:43180) · both ways · downloading
               metadata 412 to receive · 3 received, not yet activated · not caught up · last exchange 2 s ago
               download 66 files · 1.2 GiB of 3.0 GiB left · 3.4 MiB/s · ETA 6 min · 2 in flight
               progress last 3 s ago
               error    [transient] REQUEST_TIMEOUT: no reply after 30000 ms · 5x since 14:12:20 UTC · last at 14:13:08 UTC (12 s ago) · retry in 8 s
  errors     ! [transient] CORRUPT_OBJECT: hash mismatch after download · object 9f86d081884c · 2x since 14:03:20 UTC · last at 14:11:50 UTC (1 min ago)
  hint       retry the quarantined events: tkfs manage revalidate 3c0f8a21

Docs [9a8b7c6d]
  sync       ! Needs attention: CRASH_LOOP: 5 worker failures in 10 min
  lifecycle  failed · CRASH_LOOP: 5 worker failures in 10 min · 5 restarts
  last exit  exit code 1 · log: error: disk I/O error (os error 5) · 4 min ago
  mount      not mounted (worker not running) · D:\Docs
  intents    sync paused
  errors     ! [needs_user_action] CRASH_LOOP: 5 worker failures in 10 min · at 14:09:20 UTC (4 min ago)
  hint       restart the failed worker: tkfs manage retry 9a8b7c6d
  hint       sync is paused: tkfs manage resume 9a8b7c6d

Received [5e4d3c2b]
  sync       Waiting for initial sync from laptop (ws://10.0.0.2:43180): metadata
  lifecycle  running for 3 min · pid 6262
  mount      waiting for the initial sync before mounting at E:\Received
  gate       waiting for the initial sync from laptop (ws://10.0.0.2:43180) for 3 min: metadata, remote pending events
  store      healthy
  inbox      nothing to activate
  content    all 40 files available
  journal    1 event
  peer       laptop (ws://10.0.0.2:43180) · both ways · syncing
               metadata 1,200 to receive · 4 pending at the peer · not caught up · last exchange just now
               upload   12 files · 3.0 MiB left · 1.0 MiB/s · ETA 3 s
  hint       mount before the initial sync finishes: tkfs manage mount-now 5e4d3c2b

Peers (2)
  laptop (ws://10.0.0.2:43180) · online for 2 h · both ways · rtt 12 ms · up 1.0 MiB/s · down 3.4 MiB/s
    lanes   2 of 3 ready · lane 2 backing off (retry in 5 s) · 1 request in flight · 1 inbound session
    folders Photos, Received
  ws://10.0.0.9:43180 · needs upgrade (incompatible protocol) · outbound
    lanes   0 of 1 ready · lane 0 incompatible
    error   [incompatible] PEER_NEEDS_UPGRADE: the peer speaks tkfs-peer/2 · 3x since 2026-09-20 13:13:20 UTC · last at 14:03:20 UTC (10 min ago)

Recent activity
  12 s ago    action     Photos: attach requested
  1 min ago   peer       ws://10.0.0.9:43180: incompatible
  4 min ago   lifecycle  Docs: failed: CRASH_LOOP: 5 worker failures in 10 min
"#;

#[test]
fn format_status_golden() {
    let text = format_status(&fixture_snapshot());
    if text != GOLDEN {
        panic!("format_status changed; the new text is:\n{text}");
    }
}

#[test]
fn format_status_of_an_empty_daemon() {
    let snapshot = StatusSnapshot {
        schema: STATUS_SCHEMA,
        generation: 1,
        at_ms: T,
        daemon: DaemonStatus {
            control: "ws://127.0.0.1:50123".into(),
            ..DaemonStatus::default()
        },
        ..StatusSnapshot::default()
    };
    assert_eq!(
        format_status(&snapshot),
        "Telekinesis daemon\n  control      ws://127.0.0.1:50123\n  network      not listening\n\n\
         0 folders · 0 peers\n\
         No folders yet: tkfs manage create <label> --mount <path>\n"
    );
}

#[test]
fn format_folder_is_the_folder_block_of_format_status() {
    let snapshot = fixture_snapshot();
    let text = format_status(&snapshot);
    for folder in &snapshot.folders {
        let block = format_folder(folder, &snapshot.peers, snapshot.at_ms);
        assert!(text.contains(&block), "{block}");
    }
}

// ===========================================================================
// CLI: `tkfs manage …` against a control endpoint that serves
// `manage_v2::handle` (what WP2.1's `manage` forwards the v2 actions to)

/// Serves `manage_v2::handle` on a loopback control endpoint, one persistent
/// request loop per connection, the way the daemon's control threads do.
fn serve_v2(ctx: Arc<Ctx>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let ctx = ctx.clone();
            thread::spawn(move || {
                let Ok(mut socket) = ws::accept(stream) else {
                    return;
                };
                while let Ok(value) = ws::receive::<Value>(&mut socket) {
                    let reply = serde_json::from_value::<Action>(value)
                        .map_err(anyhow::Error::from)
                        .and_then(|action| manage_v2::handle(&ctx, action));
                    if ws::reply(&mut socket, reply).is_err() {
                        return;
                    }
                }
            });
        }
    });
    endpoint
}

/// A configuration whose daemon discovery points at [`serve_v2`].
struct Cli {
    config: PathBuf,
    _dir: tempfile::TempDir,
}

fn cli(fx: &Fx) -> Cli {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("orchestrator.toml");
    std::fs::write(
        &config,
        "format_version = 2\ndata_directory = \"data\"\ndefault_mount_directory = \"mounts\"\n",
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("data")).unwrap();
    std::fs::write(
        dir.path().join("data").join("orchestrator.json"),
        serde_json::to_vec(&json!({"format": 3, "control": serve_v2(fx.ctx.clone())})).unwrap(),
    )
    .unwrap();
    Cli { config, _dir: dir }
}

fn tkfs(cli: &Cli, args: &[&str]) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_tkfs"))
        .arg("-f")
        .arg(&cli.config)
        .args(args)
        .stdin(std::process::Stdio::null())
        .output()
        .unwrap()
}

/// stdout of a successful run.
#[track_caller]
fn ok(output: std::process::Output) -> String {
    assert!(
        output.status.success(),
        "exit {:?}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn cli_status_and_folder_actions() {
    let fx = plain();
    let cli = cli(&fx);

    let text = ok(tkfs(&cli, &["manage", "status"]));
    assert!(text.starts_with("Telekinesis daemon"), "{text}");
    assert!(text.contains("\nPhotos [3c0f8a21]\n"), "{text}");
    assert!(text.contains("\n  lifecycle  running for "), "{text}");
    assert!(text.contains("\n  mount      not mounted ("), "{text}");
    let snapshot: StatusSnapshot =
        serde_json::from_str(&ok(tkfs(&cli, &["manage", "status", "--json"]))).unwrap();
    assert_eq!(snapshot.schema, STATUS_SCHEMA);

    // A unique label names the folder.
    let text = ok(tkfs(&cli, &["manage", "pause", "Photos"]));
    assert_eq!(
        text.lines().next(),
        Some("Sync of Photos is paused."),
        "{text}"
    );
    assert!(text.contains("\n  intents    sync paused\n"), "{text}");
    assert!(!fx.catalog.snapshot().folder(STATE).unwrap().sync_enabled);
    // So does a unique prefix of its state.
    let text = ok(tkfs(&cli, &["manage", "resume", "3c0f"]));
    assert_eq!(
        text.lines().next(),
        Some("Sync of Photos is resumed."),
        "{text}"
    );
    let text = ok(tkfs(&cli, &["manage", "attach", "3c0f8a21"]));
    assert!(
        text.lines()
            .next()
            .is_some_and(|line| line.starts_with("Photos is mounted at ")),
        "{text}"
    );
    // `--json` prints the reply as it is.
    let reply: Value = serde_json::from_str(&ok(tkfs(
        &cli,
        &[
            "manage",
            "revalidate",
            "Photos",
            "--id",
            "event-a",
            "--id",
            "event-b",
            "--json",
        ],
    )))
    .unwrap();
    assert_eq!(reply["requeued"], 3);
    assert_eq!(reply["state"], STATE);
    assert!(
        entries(&fx.log)
            .iter()
            .any(|entry| entry.contains(r#"{"ids":["event-a","event-b"]}"#)),
        "{:?}",
        entries(&fx.log)
    );
    let text = ok(tkfs(&cli, &["manage", "revalidate", "Photos"]));
    assert_eq!(
        text.lines().next(),
        Some("3 events of Photos re-queued for activation.")
    );

    // An unknown folder is the daemon's FOLDER_NOT_FOUND.
    let output = tkfs(&cli, &["manage", "retry", "no-such-folder"]);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("FOLDER_NOT_FOUND"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Lines a child writes to stdout, read on a thread so a test never blocks
/// on a silent child.
fn lines_of(child: &mut std::process::Child) -> mpsc::Receiver<String> {
    use std::io::BufRead;
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for line in std::io::BufReader::new(stdout).lines() {
            let Ok(line) = line else { return };
            if tx.send(line).is_err() {
                return;
            }
        }
    });
    rx
}

#[test]
fn cli_watch_follows_changes() {
    let fx = plain();
    let cli = cli(&fx);
    let spawn = |json: bool| {
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_tkfs"));
        command
            .arg("-f")
            .arg(&cli.config)
            .args(["manage", "watch"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null());
        if json {
            command.arg("--json");
        }
        command.spawn().unwrap()
    };

    // `--json`: one snapshot per line, at once and then on each change.
    let mut child = spawn(true);
    let lines = lines_of(&mut child);
    let first: StatusSnapshot =
        serde_json::from_str(&lines.recv_timeout(Duration::from_secs(20)).unwrap()).unwrap();
    assert!(first.folders.iter().any(|folder| folder.state == STATE));
    fx.hub.record(StatusEvent::Activity(ActivityEntry {
        at_ms: now_ms(),
        kind: "test".into(),
        folder: None,
        peer: None,
        message: "a change".into(),
    }));
    let started = Instant::now();
    let next: StatusSnapshot =
        serde_json::from_str(&lines.recv_timeout(Duration::from_secs(10)).unwrap()).unwrap();
    assert!(next.generation > first.generation);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "a change is shown at once, not at the next poll: {:?}",
        started.elapsed()
    );
    let _ = child.kill();
    let _ = child.wait();

    // Text (stdout is a pipe, not a terminal): the status, printed on change.
    let mut child = spawn(false);
    let lines = lines_of(&mut child);
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut seen = Vec::new();
    while !seen.iter().any(|line: &String| line == "Photos [3c0f8a21]") {
        let left = deadline.saturating_duration_since(Instant::now());
        match lines.recv_timeout(left) {
            Ok(line) => seen.push(line),
            Err(_) => panic!("no folder block in {seen:#?}"),
        }
    }
    let _ = child.kill();
    let _ = child.wait();
}
