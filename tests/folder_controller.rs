//! WP1.2 acceptance (REVAMP_PLAN §14 WP1.2): the FolderController and the
//! FolderRegistry against fakes of the frozen model traits.
//!
//! `FakeLauncher` (with `FakeWorker` processes and their `FakeWorkerClient`),
//! `FakeCatalog`, `FakePublished` and `FakeStatus` are defined here; lifecycle
//! timers run on `util::time::ManualClock`, which the tests advance (followed
//! by a `Tick`) instead of sleeping. Every catalog command and every worker
//! RPC except `watch` is appended to one ordered `Log`, so ordering claims
//! (`desired=0` before the stop RPC, `ClearGate` only after `Attached`) are
//! checked directly.
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tkfs::{
    core::Branch,
    model::{
        errors::{ErrorClass, ErrorWire},
        health::{EngineGauges, Generations, MountIntent, MountState, MountStatus},
        ipc::{
            FatalLine, HelloReply, IPC_VERSION, ProgressLine, PublishedSummary, ReadyLine,
            StartupLine, StopMode, StopReply, WorkerHeartbeat, WorkerRecord,
        },
        orchestrator::{
            CallError, CatalogApi, CatalogCmd, CatalogSnapshot, EntryState, FeedTip, FolderCmd,
            FolderDirectory, FolderPhase, FolderRecord, FolderView, GateEvidence, GateRecord,
            GateState, LaunchSpec, PeerRecord, ProcessEvent, ProcessSink, PublishedCatalogApi,
            PublishedEntry, StatusSink, StopStage, WorkerClient, WorkerEndpoint, WorkerLauncher,
            WorkerProcess, outcomes,
        },
        status::StatusEvent,
    },
    orchestrator::registry::{FolderRegistry, LifecycleLimits, RegistryDeps},
    util::{
        snapshot::Snapshot,
        time::{Clock, ManualClock},
    },
};

const WAIT: Duration = Duration::from_secs(15);

// ---------------------------------------------------------------------------
// Ordered log

#[derive(Default)]
struct Log(Mutex<Vec<String>>);

impl Log {
    fn push(&self, entry: impl Into<String>) {
        self.0.lock().unwrap().push(entry.into());
    }
    fn all(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
    fn count(&self, prefix: &str) -> usize {
        self.all().iter().filter(|e| e.starts_with(prefix)).count()
    }
    fn position(&self, prefix: &str) -> Option<usize> {
        self.all().iter().position(|e| e.starts_with(prefix))
    }
}

// ---------------------------------------------------------------------------
// Catalog, published catalog, status

struct FakeCatalog {
    snapshot: Snapshot<CatalogSnapshot>,
    log: Arc<Log>,
    clock: Arc<ManualClock>,
    /// Commands (by log entry, e.g. `clear_gate:a`) that block in `apply`
    /// until removed: a slow catalog write.
    held: Mutex<BTreeSet<String>>,
}

impl FakeCatalog {
    fn mutate(&self, change: impl FnOnce(&mut CatalogSnapshot)) -> Arc<CatalogSnapshot> {
        self.snapshot.update(|current| {
            let mut next = current.clone();
            change(&mut next);
            next.generation = current.generation + 1;
            next
        });
        self.snapshot.load()
    }

    fn folder(&self, state: &str) -> FolderRecord {
        self.snapshot.load().folder(state).cloned().unwrap()
    }
}

impl CatalogApi for FakeCatalog {
    fn snapshot(&self) -> Arc<CatalogSnapshot> {
        self.snapshot.load()
    }

    fn apply(&self, command: CatalogCmd) -> Result<Arc<CatalogSnapshot>, ErrorWire> {
        let now_ms = self.clock.now_ms();
        let entry = match &command {
            CatalogCmd::SetDesired { state, desired } => format!("set_desired:{state}:{desired}"),
            CatalogCmd::ClearGate { state } => format!("clear_gate:{state}"),
            CatalogCmd::OverrideGate { state } => format!("override_gate:{state}"),
            CatalogCmd::RetargetGate {
                state,
                source_installation,
                ..
            } => format!(
                "retarget_gate:{state}:{}",
                source_installation.as_deref().unwrap_or("")
            ),
            other => other.name().to_owned(),
        };
        self.log.push(format!("catalog:{entry}"));
        while self.held.lock().unwrap().contains(&entry) {
            thread::sleep(Duration::from_millis(5));
        }
        Ok(self.mutate(|snapshot| match command {
            CatalogCmd::SetDesired { state, desired } => {
                for folder in &mut snapshot.folders {
                    if folder.state == state {
                        folder.desired_running = desired;
                    }
                }
            }
            CatalogCmd::ClearGate { state } => {
                snapshot.gates.remove(&state);
            }
            CatalogCmd::OverrideGate { state } => {
                if let Some(gate) = snapshot.gates.get_mut(&state) {
                    gate.override_ms = Some(now_ms);
                }
            }
            CatalogCmd::RetargetGate {
                state,
                source_installation,
                source_endpoint,
            } => {
                if let Some(gate) = snapshot.gates.get_mut(&state) {
                    gate.source_installation = source_installation;
                    gate.source_endpoint = source_endpoint;
                }
            }
            CatalogCmd::RemovePeer { endpoint } => {
                snapshot.peers.retain(|peer| peer.endpoint != endpoint);
            }
            _ => {}
        }))
    }

    fn wait_newer(&self, generation: u64, timeout: Duration) -> Arc<CatalogSnapshot> {
        self.snapshot.wait_newer(generation, timeout);
        self.snapshot.load()
    }
}

#[derive(Default)]
struct FakePublished {
    entries: Mutex<BTreeMap<String, PublishedEntry>>,
    updates: AtomicU64,
}

impl FakePublished {
    fn entry(&self, state: &str) -> Option<PublishedEntry> {
        self.entries.lock().unwrap().get(state).cloned()
    }
}

impl PublishedCatalogApi for FakePublished {
    fn update(&self, state: &str, entry: Option<PublishedEntry>) {
        let mut entries = self.entries.lock().unwrap();
        match entry {
            Some(entry) => entries.insert(state.to_owned(), entry),
            None => entries.remove(state),
        };
        self.updates.fetch_add(1, Ordering::SeqCst);
    }
    fn list(&self) -> (u64, Arc<Vec<PublishedEntry>>) {
        let entries = self.entries.lock().unwrap();
        (
            self.updates.load(Ordering::SeqCst),
            Arc::new(entries.values().cloned().collect()),
        )
    }
    fn wait_newer(&self, generation: u64, _: Duration) -> u64 {
        generation
    }
}

#[derive(Default)]
struct FakeStatus(Mutex<Vec<StatusEvent>>);

impl FakeStatus {
    fn events(&self) -> Vec<StatusEvent> {
        self.0.lock().unwrap().clone()
    }
}

impl StatusSink for FakeStatus {
    fn record(&self, event: StatusEvent) {
        self.0.lock().unwrap().push(event);
    }
}

// ---------------------------------------------------------------------------
// Fake workers

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Ok,
    Fail,
    Hang,
    /// Blocks while the mode stays `Hold`, then answers like `Ok`.
    Hold,
    /// `watch` only: takes the heartbeat when called, then blocks like `Hold`
    /// and returns that snapshot (a watch in flight across a mount change).
    HoldStale,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StopB {
    /// Replies `stopped` and exits shortly after.
    Exit,
    /// Refuses with `BUSY_VIEW`.
    Busy,
    /// Never replies (until the call deadline).
    Hang,
    /// Replies `stopped` but never exits.
    AckNoExit,
    /// Fails at once (connection lost).
    Fail,
}

#[derive(Clone, Debug)]
enum AttachB {
    Attached,
    /// Answers `AttachFailed{error}`.
    Failed(ErrorWire),
}

#[derive(Clone, Debug)]
struct Behavior {
    watch: Mode,
    hello: Mode,
    stop: StopB,
    force: StopB,
    summary: Mode,
    attach: VecDeque<AttachB>,
}

impl Default for Behavior {
    fn default() -> Self {
        Self {
            watch: Mode::Ok,
            hello: Mode::Ok,
            stop: StopB::Exit,
            force: StopB::Exit,
            summary: Mode::Ok,
            attach: VecDeque::new(),
        }
    }
}

#[derive(Clone, Debug)]
enum Plan {
    Ready,
    ReadyWith {
        repo: Option<String>,
        ipc: u32,
    },
    Silent,
    Crash {
        code: Option<i32>,
        fatal: Option<FatalLine>,
    },
}

#[derive(Clone, Debug)]
enum AdoptPlan {
    /// Dead pid or identity mismatch.
    None,
    Alive {
        version: String,
        behavior: Behavior,
        mount: Option<MountStatus>,
    },
}

fn alive(version: &str) -> AdoptPlan {
    AdoptPlan::Alive {
        version: version.into(),
        behavior: Behavior::default(),
        mount: None,
    }
}

#[derive(Default)]
struct HbTemplate {
    branches: u64,
    stalled: bool,
    /// The engine hold in progress the heartbeat reports (0: unlocked).
    hold_ms: u64,
    mount: Option<MountStatus>,
}

struct FakeWorker {
    pid: u32,
    instance: String,
    address: String,
    repo: String,
    device: String,
    version: String,
    adopted: bool,
    /// Where exits and startup lines go: the sink of the launch, or of the
    /// latest `adopt` of this process (the real launcher starts a new pid
    /// probe per adoption).
    sink: Mutex<ProcessSink>,
    alive: AtomicBool,
    killable: AtomicBool,
    /// The exit code reported after a graceful or forced `stop`.
    clean_exit: Option<i32>,
    behavior: Mutex<Behavior>,
    seq: AtomicU64,
    hb: Mutex<HbTemplate>,
    log: Arc<Log>,
}

impl FakeWorker {
    fn exit(&self, code: Option<i32>, fatal: Option<FatalLine>) {
        if self.alive.swap(false, Ordering::SeqCst) {
            let sink = self.sink();
            sink(ProcessEvent::Exited {
                code,
                fatal,
                stderr_tail: "fake worker log tail".into(),
            });
        }
    }

    fn alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }

    fn set(&self, change: impl FnOnce(&mut Behavior)) {
        change(&mut self.behavior.lock().unwrap());
    }

    fn sink(&self) -> ProcessSink {
        self.sink.lock().unwrap().clone()
    }

    fn emit(&self, event: ProcessEvent) {
        let sink = self.sink();
        sink(event);
    }

    /// Sleeps until `deadline` or the worker's death; `true` if it died.
    fn sleep(&self, duration: Duration) -> bool {
        let until = Instant::now() + duration;
        while Instant::now() < until {
            if !self.alive() {
                return true;
            }
            thread::sleep(Duration::from_millis(10));
        }
        !self.alive()
    }

    fn mount_status(&self) -> MountStatus {
        self.hb.lock().unwrap().mount.clone().unwrap_or_default()
    }

    fn heartbeat(&self) -> WorkerHeartbeat {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let hb = self.hb.lock().unwrap();
        WorkerHeartbeat {
            instance: self.instance.clone(),
            seq,
            mount: hb.mount.clone().unwrap_or_default(),
            engine: EngineGauges {
                stalled: hb.stalled,
                current_hold_ms: hb.hold_ms,
                ..EngineGauges::default()
            },
            generations: Generations {
                branches: hb.branches,
                ..Generations::default()
            },
            tip: Some(FeedTip {
                epoch: String::new(),
                seq: seq as i64,
            }),
            ..WorkerHeartbeat::default()
        }
    }

    fn ready_line(&self) -> ReadyLine {
        ReadyLine {
            ready: true,
            repo: self.repo.clone(),
            device: self.device.clone(),
            runtime: PathBuf::from("runtime.json"),
            mounted: false,
            mount_error: None,
            instance: self.instance.clone(),
            pid: self.pid,
            address: self.address.clone(),
            version: self.version.clone(),
            ipc: IPC_VERSION,
            token: None,
        }
    }

    fn call(&self, op: &str, payload: Value, deadline: Duration) -> Result<Value, CallError> {
        if !self.alive() {
            return Err(CallError::ConnectionLost("the worker exited".into()));
        }
        let behavior = self.behavior.lock().unwrap().clone();
        if op != "watch" {
            let mode = payload["mode"].as_str().unwrap_or("legacy");
            let entry = if op == "stop" {
                format!("rpc:stop:{mode}:{}", self.instance)
            } else {
                format!("rpc:{op}:{}", self.instance)
            };
            self.log.push(entry);
        }
        let timeout = || CallError::Timeout {
            after_ms: deadline.as_millis() as u64,
        };
        match op {
            "watch" => match behavior.watch {
                Mode::Ok => {
                    if self.sleep(Duration::from_millis(50)) {
                        return Err(CallError::ConnectionLost("the worker exited".into()));
                    }
                    Ok(serde_json::to_value(self.heartbeat()).unwrap())
                }
                Mode::Fail => {
                    thread::sleep(Duration::from_millis(20));
                    Err(CallError::ConnectionLost("watch failed".into()))
                }
                Mode::Hang => {
                    self.sleep(deadline);
                    Err(timeout())
                }
                Mode::Hold => {
                    let until = Instant::now() + deadline;
                    while self.behavior.lock().unwrap().watch == Mode::Hold {
                        if self.sleep(Duration::from_millis(10)) {
                            return Err(CallError::ConnectionLost("the worker exited".into()));
                        }
                        if Instant::now() >= until {
                            return Err(timeout());
                        }
                    }
                    Ok(serde_json::to_value(self.heartbeat()).unwrap())
                }
                Mode::HoldStale => {
                    let stale = self.heartbeat();
                    self.log.push(format!("watch-held:{}", self.instance));
                    let until = Instant::now() + deadline;
                    while self.behavior.lock().unwrap().watch == Mode::HoldStale {
                        if self.sleep(Duration::from_millis(10)) {
                            return Err(CallError::ConnectionLost("the worker exited".into()));
                        }
                        if Instant::now() >= until {
                            return Err(timeout());
                        }
                    }
                    Ok(serde_json::to_value(stale).unwrap())
                }
            },
            "hello" => match behavior.hello {
                Mode::Ok => Ok(serde_json::to_value(HelloReply {
                    instance: self.instance.clone(),
                    pid: self.pid,
                    repo: self.repo.clone(),
                    device: self.device.clone(),
                    version: self.version.clone(),
                    ipc: IPC_VERSION,
                    started_at_ms: 0,
                    process_start: 1,
                })
                .unwrap()),
                Mode::Fail => Err(timeout()),
                Mode::Hang | Mode::Hold | Mode::HoldStale => {
                    self.sleep(deadline);
                    Err(timeout())
                }
            },
            "stop" => {
                let force = payload["mode"] == "force";
                let stop = if force { behavior.force } else { behavior.stop };
                let reply = serde_json::to_value(StopReply {
                    stopped: true,
                    persisted_dirty: 0,
                    detached: true,
                })
                .unwrap();
                match stop {
                    StopB::Exit => {
                        self.exit_soon();
                        Ok(reply)
                    }
                    StopB::Busy => Err(CallError::Remote(ErrorWire::new(
                        "BUSY_VIEW",
                        "BUSY_VIEW: 1 open handle",
                    ))),
                    StopB::Hang => {
                        self.sleep(deadline);
                        Err(timeout())
                    }
                    StopB::AckNoExit => Ok(reply),
                    StopB::Fail => Err(CallError::ConnectionLost("stop failed".into())),
                }
            }
            "mount-attach" => {
                let next = self
                    .behavior
                    .lock()
                    .unwrap()
                    .attach
                    .pop_front()
                    .unwrap_or(AttachB::Attached);
                let path = payload["path"].as_str().map(PathBuf::from);
                let status = match next {
                    AttachB::Attached => MountStatus {
                        intent: MountIntent::Attach,
                        path,
                        state: MountState::Attached { since_ms: 1 },
                    },
                    AttachB::Failed(error) => MountStatus {
                        intent: MountIntent::Attach,
                        path,
                        state: MountState::AttachFailed {
                            error,
                            attempt: 1,
                            next_retry_at_ms: None,
                        },
                    },
                };
                self.hb.lock().unwrap().mount = Some(status.clone());
                Ok(serde_json::to_value(status).unwrap())
            }
            "mount-detach" => {
                let mut status = self.mount_status();
                status.intent = MountIntent::Detach;
                status.state = MountState::Detached;
                self.hb.lock().unwrap().mount = Some(status.clone());
                Ok(serde_json::to_value(status).unwrap())
            }
            "published-summary" => match behavior.summary {
                Mode::Ok => Ok(serde_json::to_value(PublishedSummary {
                    repo: self.repo.clone(),
                    replica: self.device.clone(),
                    // Ignored: the controller publishes the catalog label
                    // (CCR-WP4.3-2).
                    label: format!("fake-label-{}", self.repo),
                    // One branch, named after the branches generation the
                    // summary saw (WP4.INT: the label named it before).
                    branches: vec![Branch {
                        id: "main".into(),
                        name: format!("fake-b{}", self.hb.lock().unwrap().branches),
                        shared: true,
                    }],
                    tip: FeedTip::default(),
                    sync_generation: 1,
                })
                .unwrap()),
                _ => Err(CallError::Remote(ErrorWire::new(
                    "FAULT_INJECTED",
                    "FAULT_INJECTED: worker.rpc.published-summary",
                ))),
            },
            "revalidate" => Ok(json!({"requeued": 0})),
            "status" => Ok(json!({"repo": self.repo, "device": self.device})),
            other => Err(CallError::Remote(ErrorWire::new(
                "UNKNOWN_OPERATION",
                format!("UNKNOWN_OPERATION: {other}"),
            ))),
        }
    }

    fn exit_soon(&self) {
        // The real worker flushes the reply and exits; the fake stops serving
        // at once and reports the exit shortly after.
        if self.alive.swap(false, Ordering::SeqCst) {
            let sink = self.sink();
            let code = self.clean_exit;
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(20));
                sink(ProcessEvent::Exited {
                    code,
                    fatal: None,
                    stderr_tail: String::new(),
                });
            });
        }
    }
}

struct FakeProcess {
    worker: Arc<FakeWorker>,
}

impl WorkerProcess for FakeProcess {
    fn pid(&self) -> u32 {
        self.worker.pid
    }
    fn kill(&self) -> Result<(), ErrorWire> {
        self.worker.log.push(format!("kill:{}", self.worker.pid));
        if self.worker.killable.load(Ordering::SeqCst) {
            self.worker.exit(None, None);
        }
        Ok(())
    }
    fn adopted(&self) -> bool {
        self.worker.adopted
    }
}

struct FakeWorkerClient {
    workers: Arc<Mutex<Vec<Arc<FakeWorker>>>>,
    address: String,
    instance: String,
}

impl WorkerClient for FakeWorkerClient {
    fn instance(&self) -> &str {
        &self.instance
    }
    fn call(&self, op: &str, payload: Value, deadline: Duration) -> Result<Value, CallError> {
        let worker = self
            .workers
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|worker| worker.address == self.address)
            .cloned();
        match worker {
            Some(worker) => worker.call(op, payload, deadline),
            None => Err(CallError::Unavailable(
                "LOCAL_WORKER_UNAVAILABLE: nothing listens there".into(),
            )),
        }
    }
    fn call_bytes(
        &self,
        _: &str,
        _: Value,
        _: &[u8],
        _: Duration,
    ) -> Result<(Value, Vec<u8>), CallError> {
        Err(CallError::Unavailable("BINARY_OPS_REQUIRE_SESSION".into()))
    }
}

/// Who a fake worker claims to be.
struct Identity {
    pid: u32,
    instance: String,
    address: String,
    version: String,
    adopted: bool,
}

struct FakeLauncher {
    log: Arc<Log>,
    catalog: Arc<FakeCatalog>,
    workers: Arc<Mutex<Vec<Arc<FakeWorker>>>>,
    default_plan: Mutex<Plan>,
    plan_for: Mutex<BTreeMap<String, VecDeque<Plan>>>,
    default_behavior: Mutex<Behavior>,
    behavior_for: Mutex<BTreeMap<String, Behavior>>,
    adopt: Mutex<AdoptPlan>,
    /// Whether the exit code of an adopted worker can be read. WP1.3 reads it
    /// on Windows (a process handle); on Unix a pid probe reports `None`.
    adopted_exit_known: AtomicBool,
    next_pid: AtomicU32,
}

impl FakeLauncher {
    fn new_worker(
        &self,
        id: Identity,
        record: &FolderRecord,
        behavior: Behavior,
        sink: ProcessSink,
    ) -> Arc<FakeWorker> {
        let Identity {
            pid,
            instance,
            address,
            version,
            adopted,
        } = id;
        let worker = Arc::new(FakeWorker {
            pid,
            instance,
            address,
            repo: record.repo.clone(),
            device: record.device.clone(),
            version,
            adopted,
            sink: Mutex::new(sink),
            alive: AtomicBool::new(true),
            killable: AtomicBool::new(true),
            clean_exit: if adopted && !self.adopted_exit_known.load(Ordering::SeqCst) {
                None
            } else {
                Some(0)
            },
            behavior: Mutex::new(behavior),
            seq: AtomicU64::new(0),
            hb: Mutex::new(HbTemplate {
                mount: record
                    .mount
                    .clone()
                    .filter(|_| record.mount_enabled)
                    .map(|path| MountStatus {
                        intent: MountIntent::Attach,
                        path: Some(path),
                        state: MountState::Detached,
                    }),
                ..HbTemplate::default()
            }),
            log: self.log.clone(),
        });
        self.workers.lock().unwrap().push(worker.clone());
        worker
    }

    fn launches(&self) -> usize {
        self.log.count("launch:")
    }
}

impl WorkerLauncher for FakeLauncher {
    fn launch(
        &self,
        spec: &LaunchSpec,
        events: ProcessSink,
    ) -> Result<Box<dyn WorkerProcess>, ErrorWire> {
        self.log.push(format!("launch:{}", spec.state));
        assert!(spec.supervised, "workers are launched --supervised");
        let plan = self
            .plan_for
            .lock()
            .unwrap()
            .get_mut(&spec.state)
            .and_then(VecDeque::pop_front)
            .unwrap_or_else(|| self.default_plan.lock().unwrap().clone());
        let record = self.catalog.folder(&spec.state);
        let behavior = self
            .behavior_for
            .lock()
            .unwrap()
            .get(&spec.state)
            .cloned()
            .unwrap_or_else(|| self.default_behavior.lock().unwrap().clone());
        let pid = self.next_pid.fetch_add(1, Ordering::SeqCst);
        let instance = format!("inst-{}-{pid}", spec.state);
        let worker = self.new_worker(
            Identity {
                pid,
                address: format!("fake://{instance}"),
                instance,
                version: "0.1.0+fake".into(),
                adopted: false,
            },
            &record,
            behavior,
            events,
        );
        match plan {
            Plan::Ready => {
                worker.emit(ProcessEvent::Line(StartupLine::Progress(ProgressLine {
                    startup_progress: 1,
                    phase: "opening state".into(),
                })));
                worker.emit(ProcessEvent::Line(StartupLine::Ready(worker.ready_line())));
            }
            Plan::ReadyWith { repo, ipc } => {
                let mut line = worker.ready_line();
                if let Some(repo) = repo {
                    line.repo = repo;
                }
                line.ipc = ipc;
                worker.emit(ProcessEvent::Line(StartupLine::Ready(line)));
            }
            Plan::Silent => {}
            Plan::Crash { code, fatal } => {
                if let Some(fatal) = &fatal {
                    worker.emit(ProcessEvent::Line(StartupLine::Fatal(fatal.clone())));
                }
                worker.exit(code, fatal);
            }
        }
        Ok(Box::new(FakeProcess { worker }))
    }

    fn adopt(
        &self,
        record: &WorkerRecord,
        events: ProcessSink,
    ) -> Result<Option<Box<dyn WorkerProcess>>, ErrorWire> {
        self.log.push(format!("adopt:{}", record.pid));
        // Like the real identity check: a recorded worker that has exited is
        // dead, and a live one is the same process.
        let known = self
            .workers
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|w| w.instance == record.instance)
            .cloned();
        if let Some(worker) = known {
            if !worker.alive() {
                return Ok(None);
            }
            *worker.sink.lock().unwrap() = events;
            return Ok(Some(Box::new(FakeProcess { worker })));
        }
        match self.adopt.lock().unwrap().clone() {
            AdoptPlan::None => Ok(None),
            AdoptPlan::Alive {
                version,
                behavior,
                mount,
            } => {
                let folder = self
                    .catalog
                    .snapshot()
                    .folder_by_repo(&record.repo)
                    .cloned()
                    .unwrap();
                let worker = self.new_worker(
                    Identity {
                        pid: record.pid,
                        instance: record.instance.clone(),
                        address: record.address.clone(),
                        version,
                        adopted: true,
                    },
                    &folder,
                    behavior,
                    events,
                );
                if mount.is_some() {
                    worker.hb.lock().unwrap().mount = mount;
                }
                Ok(Some(Box::new(FakeProcess { worker })))
            }
        }
    }

    fn client(&self, endpoint: &WorkerEndpoint) -> Arc<dyn WorkerClient> {
        Arc::new(FakeWorkerClient {
            workers: self.workers.clone(),
            address: endpoint.address.clone(),
            instance: endpoint.instance.clone(),
        })
    }
}

// ---------------------------------------------------------------------------
// Environment

struct EnvBuilder {
    dir: tempfile::TempDir,
    folders: Vec<FolderRecord>,
    peers: Vec<PeerRecord>,
    gates: BTreeMap<String, GateRecord>,
    limits: LifecycleLimits,
    plan: Plan,
    plan_for: BTreeMap<String, VecDeque<Plan>>,
    behavior: Behavior,
    behavior_for: BTreeMap<String, Behavior>,
    adopt: AdoptPlan,
    adopted_exit_known: bool,
}

impl EnvBuilder {
    fn new() -> Self {
        let mut limits = LifecycleLimits::new(PathBuf::from("fake-tkfs"), 8);
        limits.worker_version = "0.1.0".into();
        Self {
            dir: tempfile::tempdir().unwrap(),
            folders: Vec::new(),
            peers: Vec::new(),
            gates: BTreeMap::new(),
            limits,
            plan: Plan::Ready,
            plan_for: BTreeMap::new(),
            behavior: Behavior::default(),
            behavior_for: BTreeMap::new(),
            adopt: AdoptPlan::None,
            adopted_exit_known: false,
        }
    }

    fn dir(&self, state: &str) -> PathBuf {
        self.dir.path().join("states").join(state)
    }

    fn folder(mut self, state: &str, desired: bool) -> Self {
        let directory = self.dir(state);
        std::fs::create_dir_all(&directory).unwrap();
        self.folders.push(FolderRecord {
            state: state.into(),
            label: state.into(),
            repo: format!("repo-{state}"),
            device: format!("dev-{state}"),
            directory,
            mount: None,
            desired_running: desired,
            sync_enabled: true,
            mount_enabled: false,
        });
        self
    }

    fn mounted(mut self, state: &str) -> Self {
        let mount = self.dir.path().join("mounts").join(state);
        let folder = self.folders.iter_mut().find(|f| f.state == state).unwrap();
        folder.mount = Some(mount);
        folder.mount_enabled = true;
        self
    }

    fn peer(mut self, installation: &str) -> Self {
        self.peers.push(PeerRecord {
            endpoint: format!("ws://{installation}"),
            code: "secret".into(),
            installation: installation.into(),
            connections: 2,
            revision: "1".into(),
        });
        self
    }

    fn gate(mut self, state: &str, source: &str) -> Self {
        self.gates.insert(
            state.into(),
            GateRecord {
                state: state.into(),
                source_installation: Some(source.into()),
                source_endpoint: format!("ws://{source}"),
                created_ms: 1,
                override_ms: None,
            },
        );
        self
    }

    fn limits(mut self, change: impl FnOnce(&mut LifecycleLimits)) -> Self {
        change(&mut self.limits);
        self
    }

    fn plan(mut self, plan: Plan) -> Self {
        self.plan = plan;
        self
    }

    fn plan_for(mut self, state: &str, plan: Plan) -> Self {
        self.plan_for
            .entry(state.into())
            .or_default()
            .push_back(plan);
        self
    }

    fn behavior(mut self, change: impl FnOnce(&mut Behavior)) -> Self {
        change(&mut self.behavior);
        self
    }

    fn behavior_for(mut self, state: &str, change: impl FnOnce(&mut Behavior)) -> Self {
        let mut behavior = self.behavior.clone();
        change(&mut behavior);
        self.behavior_for.insert(state.into(), behavior);
        self
    }

    fn adopt(mut self, plan: AdoptPlan) -> Self {
        self.adopt = plan;
        self
    }

    /// Adopted workers report their real exit code (Windows) instead of
    /// `None` (Unix, the default).
    fn adopted_exit_known(mut self, known: bool) -> Self {
        self.adopted_exit_known = known;
        self
    }

    /// Writes `STATE/worker.json` naming a previous worker.
    fn worker_record(self, state: &str, pid: u32, version: &str) -> Self {
        let folder = self.folders.iter().find(|f| f.state == state).unwrap();
        write_worker_record(folder, pid, version);
        self
    }

    fn start(self) -> Env {
        let log = Arc::new(Log::default());
        let clock = Arc::new(ManualClock::default());
        let catalog = Arc::new(FakeCatalog {
            snapshot: Snapshot::new(CatalogSnapshot {
                generation: 0,
                installation: "local".into(),
                folders: self.folders,
                peers: self.peers,
                gates: self.gates,
                ..CatalogSnapshot::default()
            }),
            log: log.clone(),
            clock: clock.clone(),
            held: Mutex::default(),
        });
        let published = Arc::new(FakePublished::default());
        let status = Arc::new(FakeStatus::default());
        let launcher = Arc::new(FakeLauncher {
            log: log.clone(),
            catalog: catalog.clone(),
            workers: Arc::new(Mutex::new(Vec::new())),
            default_plan: Mutex::new(self.plan),
            plan_for: Mutex::new(self.plan_for),
            default_behavior: Mutex::new(self.behavior),
            behavior_for: Mutex::new(self.behavior_for),
            adopt: Mutex::new(self.adopt),
            adopted_exit_known: AtomicBool::new(self.adopted_exit_known),
            next_pid: AtomicU32::new(1000),
        });
        let registry = FolderRegistry::start(RegistryDeps {
            catalog: catalog.clone(),
            published: published.clone(),
            status: status.clone(),
            launcher: launcher.clone(),
            clock: clock.clone(),
            limits: self.limits,
        });
        Env {
            dir: self.dir,
            log,
            clock,
            catalog,
            published,
            status,
            launcher,
            registry,
        }
    }
}

/// `STATE/worker.json` of a previous worker of `folder` (instance
/// `old-<state>`, address `fake://old-<state>`).
fn write_worker_record(folder: &FolderRecord, pid: u32, version: &str) {
    let state = &folder.state;
    let record = WorkerRecord {
        format: 1,
        pid,
        instance: format!("old-{state}"),
        address: format!("fake://old-{state}"),
        version: version.into(),
        ipc: IPC_VERSION,
        repo: folder.repo.clone(),
        device: folder.device.clone(),
        started_at_ms: 1,
        process_start: 7,
        executable: PathBuf::from("fake-tkfs"),
        supervised: true,
        token: None,
    };
    std::fs::write(
        folder.directory.join("worker.json"),
        serde_json::to_vec(&record).unwrap(),
    )
    .unwrap();
}

struct Env {
    dir: tempfile::TempDir,
    log: Arc<Log>,
    clock: Arc<ManualClock>,
    catalog: Arc<FakeCatalog>,
    published: Arc<FakePublished>,
    status: Arc<FakeStatus>,
    launcher: Arc<FakeLauncher>,
    registry: Arc<FolderRegistry>,
}

impl Drop for Env {
    fn drop(&mut self) {
        if !thread::panicking() {
            self.registry.shutdown(Duration::from_millis(300));
        }
        for worker in self.launcher.workers.lock().unwrap().iter() {
            worker.alive.store(false, Ordering::SeqCst);
        }
    }
}

fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(5));
    }
}

impl Env {
    fn dir(&self, state: &str) -> PathBuf {
        self.dir.path().join("states").join(state)
    }

    fn view(&self, state: &str) -> Arc<FolderView> {
        self.registry.view(state).expect("folder view")
    }

    fn wait_view(
        &self,
        state: &str,
        what: &str,
        done: impl Fn(&FolderView) -> bool,
    ) -> Arc<FolderView> {
        let deadline = Instant::now() + WAIT;
        loop {
            let view = self.view(state);
            if done(&view) {
                return view;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {state}: {what}; last view: {view:#?}; log: {:?}",
                self.log.all()
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn running(&self, state: &str) -> Arc<FolderView> {
        self.wait_view(state, "running", |v| {
            matches!(v.phase, FolderPhase::Running { .. })
        })
    }

    /// Advances the manual clock and wakes every controller.
    fn advance(&self, by: Duration) {
        self.clock.advance(by);
        for view in self.registry.all() {
            let _ = self.registry.send(&view.record.state, FolderCmd::Tick);
        }
    }

    /// Advances the clock by `step` (and ticks) until `done`, one step at a
    /// time: a timer armed by a result still in flight is not skipped.
    fn advance_until(
        &self,
        state: &str,
        what: &str,
        step: Duration,
        done: impl Fn(&FolderView) -> bool,
    ) -> Arc<FolderView> {
        let deadline = Instant::now() + WAIT;
        loop {
            let view = self.view(state);
            if done(&view) {
                return view;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {state}: {what}; last view: {view:#?}; log: {:?}",
                self.log.all()
            );
            self.advance(step);
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// The most recent fake worker of `state`.
    fn worker(&self, state: &str) -> Arc<FakeWorker> {
        let repo = format!("repo-{state}");
        self.launcher
            .workers
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|w| w.repo == repo)
            .cloned()
            .expect("a worker")
    }

    fn kills(&self) -> usize {
        self.log.count("kill:")
    }

    /// Lets queued messages and helper threads settle.
    fn settle(&self) {
        thread::sleep(Duration::from_millis(250));
    }
}

fn error_code(phase: &FolderPhase) -> Option<&str> {
    match phase {
        FolderPhase::Failed { error } => Some(error.error.code.as_str()),
        _ => None,
    }
}

fn last_error(view: &FolderView) -> Option<&str> {
    view.last_error
        .as_ref()
        .map(|record| record.error.code.as_str())
}

// ---------------------------------------------------------------------------
// start → ready → running

#[test]
fn start_ready_running_publishes_view_entry_and_status() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .folder("b", false)
        .start();
    let view = env.running("a");
    let FolderPhase::Running {
        instance, adopted, ..
    } = &view.phase
    else {
        unreachable!()
    };
    assert!(!adopted);
    let worker = env.worker("a");
    assert_eq!(instance, &worker.instance);
    let endpoint = view.worker.clone().expect("endpoint");
    assert_eq!(endpoint.instance, worker.instance);
    assert_eq!(endpoint.pid, worker.pid);
    assert!(view.record.desired_running);
    assert_eq!(env.launcher.launches(), 1, "{:?}", env.log.all());
    assert_eq!(env.view("b").phase, FolderPhase::Stopped);

    // Published entry from published-summary; the heartbeat refreshes the tip.
    wait_for("published entry", || {
        env.published
            .entry("a")
            .is_some_and(|e| e.state == EntryState::Running && e.repo == "repo-a")
    });
    env.wait_view("a", "a heartbeat", |v| v.heartbeat.is_some());
    assert!(env.registry.client("a").is_some());
    assert!(env.registry.client("b").is_none());
    assert_eq!(env.registry.by_repo("repo-a").unwrap().record.state, "a");
    assert_eq!(env.registry.all().len(), 2);

    // Status: lifecycle transitions and heartbeats.
    wait_for("status events", || {
        let events = env.status.events();
        let phases: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                StatusEvent::FolderLifecycle(view) if view.record.state == "a" => {
                    Some(view.phase.lifecycle())
                }
                _ => None,
            })
            .collect();
        let heartbeat = events
            .iter()
            .any(|e| matches!(e, StatusEvent::WorkerHeartbeat { state, .. } if state == "a"));
        phases.contains(&tkfs::model::status::LifecyclePhase::Starting)
            && phases.contains(&tkfs::model::status::LifecyclePhase::Running)
            && heartbeat
    });

    // The aggregate generation moves with the views.
    let generation = env.registry.generation();
    assert!(env.registry.wait_newer(generation, Duration::from_secs(5)) > generation);

    // Start of a stopped folder: persisted, then running.
    env.registry.send("b", FolderCmd::Start).unwrap();
    env.running("b");
    assert!(env.catalog.folder("b").desired_running);
    assert!(env.log.position("catalog:set_desired:b:true").is_some());
}

#[test]
fn ready_mismatch_fails_with_worker_state_mismatch() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .folder("b", true)
        .plan_for(
            "a",
            Plan::ReadyWith {
                repo: Some("another-repo".into()),
                ipc: IPC_VERSION,
            },
        )
        .plan_for("b", Plan::ReadyWith { repo: None, ipc: 2 })
        .start();
    let a = env.wait_view("a", "failed", |v| error_code(&v.phase).is_some());
    assert_eq!(error_code(&a.phase), Some("WORKER_STATE_MISMATCH"));
    let FolderPhase::Failed { error } = &a.phase else {
        unreachable!()
    };
    assert_eq!(error.error.class, ErrorClass::Fatal);
    let b = env.wait_view("b", "failed", |v| error_code(&v.phase).is_some());
    assert_eq!(error_code(&b.phase), Some("WORKER_IPC_MISMATCH"));
    assert_eq!(env.kills(), 2, "mismatched workers are killed");
    // Failed is terminal until Retry/Start/catalog change.
    env.advance(Duration::from_secs(600));
    env.settle();
    assert_eq!(env.launcher.launches(), 2);
    assert!(error_code(&env.view("a").phase).is_some());
}

// ---------------------------------------------------------------------------
// backoff, breaker, watchdog, liveness

#[test]
fn backoff_schedule_within_jitter_then_breaker_then_retry_recovers() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .plan(Plan::Crash {
            code: Some(70),
            fatal: None,
        })
        .start();
    for attempt in 1..=4u32 {
        let view = env.wait_view(
            "a",
            "backoff",
            |v| matches!(v.phase, FolderPhase::Backoff { attempt: a, .. } if a == attempt),
        );
        let FolderPhase::Backoff {
            until_ms,
            last_exit,
            ..
        } = &view.phase
        else {
            unreachable!()
        };
        assert_eq!(last_exit.code, Some(70));
        assert_eq!(last_error(&view), Some("WORKER_EXITED"));
        let delay = until_ms - env.clock.now_ms();
        let nominal = 1000u64 << (attempt - 1);
        assert!(
            delay >= nominal * 8 / 10 && delay <= nominal * 12 / 10,
            "attempt {attempt}: {delay} ms outside {nominal} ms ± 20 %"
        );
        // Nothing restarts before the timer.
        env.advance(Duration::from_millis(delay / 2));
        env.settle();
        assert_eq!(env.launcher.launches(), attempt as usize);
        env.advance(Duration::from_millis(delay - delay / 2 + 1));
    }
    let view = env.wait_view("a", "crash loop", |v| error_code(&v.phase).is_some());
    assert_eq!(error_code(&view.phase), Some("CRASH_LOOP"));
    let FolderPhase::Failed { error } = &view.phase else {
        unreachable!()
    };
    assert_eq!(error.error.class, ErrorClass::NeedsUserAction);
    assert_eq!(env.launcher.launches(), 5);
    assert_eq!(view.restarts, 4);
    // The breaker holds.
    env.advance(Duration::from_secs(1000));
    env.settle();
    assert_eq!(env.launcher.launches(), 5);

    *env.launcher.default_plan.lock().unwrap() = Plan::Ready;
    env.registry.send("a", FolderCmd::Retry).unwrap();
    env.running("a");
    assert_eq!(env.launcher.launches(), 6);
}

#[test]
fn fatal_line_needing_user_action_fails_without_retry() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .plan(Plan::Crash {
            code: Some(73),
            fatal: Some(FatalLine::new(
                "STORE_IDENTITY_MISMATCH",
                "STORE_IDENTITY_MISMATCH: metadata.sqlite belongs to another store",
                73,
            )),
        })
        .start();
    let view = env.wait_view("a", "failed", |v| error_code(&v.phase).is_some());
    assert_eq!(error_code(&view.phase), Some("STORE_IDENTITY_MISMATCH"));
    env.advance(Duration::from_secs(60));
    env.settle();
    assert_eq!(env.launcher.launches(), 1);
}

#[test]
fn watchdog_expiry_kills_and_backs_off() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .plan(Plan::Silent)
        .start();
    env.wait_view("a", "starting", |v| {
        matches!(
            v.phase,
            FolderPhase::Starting {
                adopting: false,
                ..
            }
        )
    });
    env.advance(Duration::from_secs(20));
    env.settle();
    assert!(matches!(env.view("a").phase, FolderPhase::Starting { .. }));
    // Progress from the recovering thread renews the lease (invariant 28).
    env.worker("a")
        .emit(ProcessEvent::Line(StartupLine::Progress(ProgressLine {
            startup_progress: 1,
            phase: "replaying history".into(),
        })));
    env.wait_view("a", "progress", |v| {
        matches!(&v.phase, FolderPhase::Starting { progress_seq: 1, phase, .. } if phase == "replaying history")
    });
    env.advance(Duration::from_secs(20));
    env.settle();
    assert!(matches!(env.view("a").phase, FolderPhase::Starting { .. }));
    assert_eq!(env.kills(), 0);
    env.advance(Duration::from_secs(10));
    let view = env.wait_view("a", "backoff", |v| {
        matches!(v.phase, FolderPhase::Backoff { .. })
    });
    assert_eq!(last_error(&view), Some("WORKER_START_TIMEOUT"));
    assert_eq!(env.kills(), 1);
    assert!(!env.worker("a").alive());
}

#[test]
fn hung_watch_is_unresponsive_then_killed_and_restarted() {
    let env = EnvBuilder::new().folder("a", true).start();
    env.running("a");
    env.wait_view("a", "heartbeat", |v| v.heartbeat.is_some());
    let first = env.worker("a");
    first.set(|b| {
        b.watch = Mode::Hang;
        b.hello = Mode::Fail;
    });
    env.settle();
    env.advance(Duration::from_secs(9));
    let view = env.wait_view("a", "backoff", |v| {
        matches!(v.phase, FolderPhase::Backoff { .. })
    });
    assert_eq!(last_error(&view), Some("WORKER_UNRESPONSIVE"));
    assert_eq!(env.kills(), 1);
    assert!(env.log.position("rpc:hello").is_some());
    env.advance(Duration::from_secs(2));
    let view = env.running("a");
    let FolderPhase::Running { instance, .. } = &view.phase else {
        unreachable!()
    };
    assert_ne!(instance, &first.instance);
    assert_eq!(env.launcher.launches(), 2);
}

#[test]
fn three_failed_watches_probe_hello_and_kill() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .behavior(|b| {
            b.watch = Mode::Fail;
            b.hello = Mode::Fail;
        })
        .start();
    let view = env.wait_view("a", "backoff", |v| {
        matches!(v.phase, FolderPhase::Backoff { .. })
    });
    assert_eq!(last_error(&view), Some("WORKER_UNRESPONSIVE"));
    assert_eq!(env.kills(), 1);
}

#[test]
fn stalled_engine_marks_degraded_without_kill() {
    let env = EnvBuilder::new().folder("a", true).start();
    env.running("a");
    env.worker("a").hb.lock().unwrap().stalled = true;
    env.wait_view("a", "degraded", |v| v.degraded);
    env.worker("a").hb.lock().unwrap().stalled = false;
    env.wait_view("a", "not degraded", |v| !v.degraded);
    assert_eq!(env.kills(), 0);
    assert!(matches!(env.view("a").phase, FolderPhase::Running { .. }));
}

// ---------------------------------------------------------------------------
// stop ladder

#[test]
fn graceful_stop_persists_desired_before_the_stop_rpc() {
    let env = EnvBuilder::new().folder("a", true).start();
    env.running("a");
    // (The worker's entry: before it, the folder is published unavailable,
    // WP3.INT re-review 2.)
    wait_for("entry", || {
        env.published
            .entry("a")
            .is_some_and(|entry| entry.state == EntryState::Running)
    });
    let view = env
        .registry
        .request(
            "a",
            FolderCmd::Stop {
                mode: StopMode::Graceful,
            },
            Duration::from_secs(10),
        )
        .unwrap();
    assert_eq!(view.phase, FolderPhase::Stopped);
    let persisted = env.log.position("catalog:set_desired:a:false").unwrap();
    let stop = env.log.position("rpc:stop:graceful").unwrap();
    assert!(
        persisted < stop,
        "desired=0 is persisted first: {:?}",
        env.log.all()
    );
    assert!(!env.catalog.folder("a").desired_running);
    assert!(!view.record.desired_running);
    assert_eq!(env.kills(), 0);
    // The entry stays published, unavailable: peers wait for the folder
    // instead of reading it as no longer shared (WP3.INT re-review; it was
    // removed). Only leaving the catalog removes it (below).
    assert!(
        env.published
            .entry("a")
            .is_some_and(|entry| entry.state == EntryState::Unavailable),
        "{:?}",
        env.published.entry("a")
    );
    assert_eq!(env.registry.running_slots(), 0);
    assert!(env.registry.client("a").is_none());
    // A stopped folder is never restarted on its own (invariant 26).
    env.advance(Duration::from_secs(600));
    env.settle();
    assert_eq!(env.launcher.launches(), 1);
}

#[test]
fn busy_view_blocks_the_stop_with_retries_then_force_stop() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .behavior(|b| b.stop = StopB::Busy)
        .start();
    env.running("a");
    let view = env
        .registry
        .request(
            "a",
            FolderCmd::Stop {
                mode: StopMode::Graceful,
            },
            Duration::from_secs(10),
        )
        .unwrap();
    let FolderPhase::StopBlocked { reason, .. } = &view.phase else {
        panic!("expected StopBlocked: {view:#?}")
    };
    assert_eq!(reason.code, "BUSY_VIEW");
    assert_eq!(last_error(&view), Some("STOP_BLOCKED_BY_OPEN_FILES"));
    assert_eq!(env.log.count("rpc:stop:graceful"), 1);
    env.advance(Duration::from_secs(2));
    wait_for("second graceful stop", || {
        env.log.count("rpc:stop:graceful") == 2
    });
    // The next retry is armed when the controller handles this refusal (2 s
    // after the clock it reads then): wait for that before moving the clock.
    env.wait_view("a", "second refusal handled", |v| {
        v.last_error.as_ref().is_some_and(|e| e.count == 2)
    });
    env.advance(Duration::from_secs(2));
    wait_for("third graceful stop", || {
        env.log.count("rpc:stop:graceful") == 3
    });
    assert!(matches!(
        env.view("a").phase,
        FolderPhase::StopBlocked { .. }
    ));
    assert!(env.worker("a").alive(), "a busy worker is never killed");

    let view = env
        .registry
        .request("a", FolderCmd::ForceStop, Duration::from_secs(10))
        .unwrap();
    assert_eq!(view.phase, FolderPhase::Stopped);
    assert!(env.log.position("rpc:stop:force").is_some());
    assert_eq!(env.kills(), 0);
}

#[test]
fn start_cancels_a_blocked_stop() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .behavior(|b| b.stop = StopB::Busy)
        .start();
    let before = env.running("a");
    env.registry
        .request(
            "a",
            FolderCmd::Stop {
                mode: StopMode::Graceful,
            },
            Duration::from_secs(10),
        )
        .unwrap();
    env.registry.send("a", FolderCmd::Start).unwrap();
    let after = env.running("a");
    assert_eq!(before.worker, after.worker, "same worker keeps running");
    assert!(env.catalog.folder("a").desired_running);
    assert_eq!(env.launcher.launches(), 1);
}

#[test]
fn acknowledged_stop_that_never_exits_is_killed() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .behavior(|b| b.stop = StopB::AckNoExit)
        .start();
    env.running("a");
    env.registry
        .send(
            "a",
            FolderCmd::Stop {
                mode: StopMode::Graceful,
            },
        )
        .unwrap();
    env.wait_view("a", "await exit", |v| {
        matches!(
            v.phase,
            FolderPhase::Stopping {
                stage: StopStage::AwaitExit,
                ..
            }
        )
    });
    env.advance(Duration::from_secs(9));
    env.settle();
    assert_eq!(env.kills(), 0);
    env.advance(Duration::from_secs(2));
    env.wait_view("a", "stopped", |v| v.phase == FolderPhase::Stopped);
    assert_eq!(env.kills(), 1);
}

/// A force-stopped worker whose stop RPC does not come back (its handler
/// waits for the engine) and whose heartbeat keeps reporting an engine hold
/// in progress (a long save), not stalled.
fn force_stop_behind_a_busy_engine(hold_ms: u64, stalled: bool) -> Env {
    let env = EnvBuilder::new()
        .folder("a", true)
        .limits(|limits| {
            limits.stop_graceful = Duration::from_millis(100);
            limits.stop_exit_wait = Duration::from_millis(400);
        })
        .behavior(|b| b.force = StopB::Hang)
        .start();
    env.running("a");
    {
        let worker = env.worker("a");
        let mut hb = worker.hb.lock().unwrap();
        hb.hold_ms = hold_ms;
        hb.stalled = stalled;
    }
    env.registry.send("a", FolderCmd::ForceStop).unwrap();
    env.wait_view("a", "await exit", |v| {
        matches!(
            v.phase,
            FolderPhase::Stopping {
                stage: StopStage::AwaitExit,
                ..
            }
        )
    });
    env
}

/// WP4.INT re-review (the production review's major, invariant 8): a force
/// stop killed the worker 15 s + 10 s after it was sent whatever the worker
/// was doing, so a save holding the engine longer than that (a multi-GiB
/// file) lost every unsaved session the stop's retain was waiting for. Now,
/// while fresh heartbeats report an engine hold in progress that is not
/// stalled, the exit wait starts again: 20 s of manual time pass (50 exit
/// waits) without a kill, and the worker exits by itself once the hold ends.
#[test]
fn force_stop_waits_for_a_busy_engine_instead_of_killing() {
    let env = force_stop_behind_a_busy_engine(30_000, false);
    for _ in 0..40 {
        env.advance(Duration::from_millis(500));
        // A heartbeat (every 50 ms) arrives before the next step.
        thread::sleep(Duration::from_millis(80));
        assert_eq!(
            env.kills(),
            0,
            "killed behind a busy engine: {:?}",
            env.log.all()
        );
    }
    assert!(env.worker("a").alive());
    // The save ends; the stop's retain runs and the worker exits.
    env.worker("a").hb.lock().unwrap().hold_ms = 0;
    env.worker("a").exit(Some(0), None);
    env.wait_view("a", "stopped", |v| v.phase == FolderPhase::Stopped);
    assert_eq!(env.kills(), 0);
}

/// The wait has bounds: a stalled engine (a hold beyond the worker's stall
/// bound) is killed at the exit wait as before, and so is a busy one once
/// `FORCE_STOP_ENGINE_PATIENCE` has passed since the stop began.
#[test]
fn force_stop_kills_a_stalled_or_endlessly_busy_engine() {
    use tkfs::orchestrator::folder::FORCE_STOP_ENGINE_PATIENCE;
    let env = force_stop_behind_a_busy_engine(200_000, true);
    env.advance_until("a", "killed", Duration::from_millis(500), |v| {
        v.phase == FolderPhase::Stopped
    });
    assert_eq!(env.kills(), 1);
    drop(env);

    let env = force_stop_behind_a_busy_engine(30_000, false);
    let started = Instant::now();
    let mut waited = Duration::ZERO;
    while env.kills() == 0 {
        assert!(
            started.elapsed() < WAIT,
            "never killed: {:?}",
            env.log.all()
        );
        env.advance(Duration::from_secs(1));
        waited += Duration::from_secs(1);
        thread::sleep(Duration::from_millis(60));
    }
    assert!(
        waited >= FORCE_STOP_ENGINE_PATIENCE,
        "killed after {waited:?} of a busy engine"
    );
    assert!(
        waited <= FORCE_STOP_ENGINE_PATIENCE + Duration::from_secs(5),
        "{waited:?}"
    );
}

#[test]
fn starting_worker_is_killed_directly_on_stop() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .plan(Plan::Silent)
        .start();
    env.wait_view("a", "starting", |v| {
        matches!(v.phase, FolderPhase::Starting { .. })
    });
    let view = env
        .registry
        .request(
            "a",
            FolderCmd::Stop {
                mode: StopMode::Graceful,
            },
            Duration::from_secs(10),
        )
        .unwrap();
    assert_eq!(view.phase, FolderPhase::Stopped);
    assert_eq!(env.kills(), 1);
    assert_eq!(env.log.count("rpc:stop"), 0, "no RPC before Ready");
}

#[test]
fn restart_stops_then_starts_a_new_worker() {
    let env = EnvBuilder::new().folder("a", true).start();
    let before = env.running("a");
    env.registry.send("a", FolderCmd::Restart).unwrap();
    let after = env.wait_view("a", "new worker", |v| {
        matches!(v.phase, FolderPhase::Running { .. }) && v.worker != before.worker
    });
    assert!(after.record.desired_running);
    assert_eq!(env.launcher.launches(), 2);
    assert_eq!(env.log.position("catalog:set_desired"), None);
}

#[test]
fn restart_while_starting_kills_the_spawned_worker_and_starts_again() {
    // Fix round 2: Restart while Starting answered OK and did nothing.
    let env = EnvBuilder::new()
        .folder("a", true)
        .plan_for("a", Plan::Silent)
        .start();
    env.wait_view("a", "starting", |v| {
        matches!(v.phase, FolderPhase::Starting { .. })
    });
    let first = env.worker("a");
    env.registry
        .request("a", FolderCmd::Restart, Duration::from_secs(10))
        .unwrap();
    let view = env.running("a");
    assert!(!first.alive());
    assert_ne!(view.worker.as_ref().map(|w| w.pid), Some(first.pid));
    assert_eq!(env.kills(), 1);
    assert_eq!(env.launcher.launches(), 2);
    assert_eq!(env.log.count("rpc:stop"), 0, "no RPC before Ready");
    assert!(last_error(&view).is_none(), "a restart is not a failure");
}

#[test]
fn restart_while_adopting_stops_the_adopted_worker_then_spawns() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .worker_record("a", 4242, "0.1.0")
        .adopt(AdoptPlan::Alive {
            version: "0.1.0".into(),
            behavior: Behavior {
                hello: Mode::Hang,
                ..Behavior::default()
            },
            mount: None,
        })
        .start();
    env.wait_view("a", "adopting", |v| {
        matches!(v.phase, FolderPhase::Starting { adopting: true, .. })
    });
    let adopted = env.worker("a");
    env.registry.send("a", FolderCmd::Restart).unwrap();
    let view = env.wait_view("a", "spawned worker running", |v| {
        matches!(v.phase, FolderPhase::Running { adopted: false, .. })
    });
    assert!(!adopted.alive());
    assert!(
        env.log.position("rpc:stop:graceful:old-a").is_some(),
        "{:?}",
        env.log.all()
    );
    assert_eq!(env.kills(), 0);
    assert_eq!(env.launcher.launches(), 1);
    assert!(last_error(&view).is_none(), "{view:#?}");
}

#[test]
fn restart_whose_worker_crashes_while_stopping_backs_off() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .behavior(|b| b.stop = StopB::AckNoExit)
        .start();
    env.running("a");
    env.registry.send("a", FolderCmd::Restart).unwrap();
    env.wait_view("a", "await exit", |v| {
        matches!(
            v.phase,
            FolderPhase::Stopping {
                stage: StopStage::AwaitExit,
                ..
            }
        )
    });
    env.worker("a").exit(Some(70), None);
    let view = env.wait_view("a", "backoff", |v| {
        matches!(v.phase, FolderPhase::Backoff { attempt: 1, .. })
    });
    assert_eq!(last_error(&view), Some("WORKER_EXITED"));
    assert_eq!(env.launcher.launches(), 1);
    env.advance(Duration::from_secs(2));
    env.running("a");
    assert_eq!(env.launcher.launches(), 2);
}

#[test]
fn restart_whose_spawned_worker_is_signalled_while_stopping_backs_off() {
    // A spawned worker is our child: no exit code means a signal, a crash.
    let env = EnvBuilder::new()
        .folder("a", true)
        .behavior(|b| b.stop = StopB::AckNoExit)
        .start();
    env.running("a");
    env.registry.send("a", FolderCmd::Restart).unwrap();
    env.wait_view("a", "await exit", |v| {
        matches!(
            v.phase,
            FolderPhase::Stopping {
                stage: StopStage::AwaitExit,
                ..
            }
        )
    });
    env.worker("a").exit(None, None);
    let view = env.wait_view("a", "backoff", |v| {
        matches!(v.phase, FolderPhase::Backoff { attempt: 1, .. })
    });
    assert_eq!(last_error(&view), Some("WORKER_EXITED"));
    assert_eq!(env.launcher.launches(), 1);
}

#[test]
fn default_limits_follow_the_plan_constants() {
    let limits = LifecycleLimits::default();
    assert_eq!(limits.worker_version, env!("CARGO_PKG_VERSION"));
    assert_eq!(limits.startup_silence, Duration::from_secs(30));
    assert_eq!(limits.stop_graceful, Duration::from_secs(5));
    assert_eq!(limits.stop_exit_wait, Duration::from_secs(10));
    assert_eq!(limits.backoff_base, Duration::from_secs(1));
    assert_eq!(limits.backoff_cap, Duration::from_secs(300));
    assert_eq!(limits.backoff_reset_after, Duration::from_secs(600));
    assert_eq!(limits.breaker_failures, 5);
    assert_eq!(limits.breaker_window, Duration::from_secs(600));
    assert_eq!(limits.heartbeat_timeout, Duration::from_secs(8));
    assert_eq!(limits.hello_timeout, Duration::from_secs(2));
    assert_eq!(limits.adopt_patience, Duration::from_secs(30));
    assert_eq!(limits.attach_deadline, Duration::from_secs(60));
    assert_eq!(limits.mount_retry_base, Duration::from_secs(2));
    assert_eq!(limits.mount_retry_cap, Duration::from_secs(120));
    assert!(limits.shutdown_kill_starting);
    assert_eq!(limits.maximum_running, 8);
}

#[test]
fn send_returns_fast_while_the_controller_is_mid_stop() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .behavior(|b| b.stop = StopB::Hang)
        .start();
    env.running("a");
    env.registry
        .send(
            "a",
            FolderCmd::Stop {
                mode: StopMode::Graceful,
            },
        )
        .unwrap();
    env.wait_view("a", "stopping", |v| {
        matches!(
            v.phase,
            FolderPhase::Stopping {
                stage: StopStage::Requested,
                ..
            }
        )
    });
    let mut slowest = Duration::ZERO;
    for _ in 0..200 {
        let started = Instant::now();
        env.registry.send("a", FolderCmd::Tick).unwrap();
        let _ = env.registry.view("a");
        slowest = slowest.max(started.elapsed());
    }
    assert!(
        slowest < Duration::from_millis(5),
        "slowest send {slowest:?}"
    );
}

// ---------------------------------------------------------------------------
// shutdown

#[test]
fn shutdown_of_five_folders_finishes_by_the_deadline_with_outcomes() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .folder("b", true)
        .folder("c", true)
        .folder("d", true)
        .folder("e", false)
        .behavior_for("b", |b| b.stop = StopB::Hang)
        .behavior_for("c", |b| b.stop = StopB::Busy)
        .plan_for("d", Plan::Silent)
        .start();
    for state in ["a", "b", "c"] {
        env.running(state);
    }
    env.wait_view("d", "starting", |v| {
        matches!(v.phase, FolderPhase::Starting { .. })
    });
    let started = Instant::now();
    let outcomes = env.registry.shutdown(Duration::from_secs(3));
    let took = started.elapsed();
    assert!(took < Duration::from_millis(3600), "shutdown took {took:?}");
    let by_state: BTreeMap<_, _> = outcomes
        .iter()
        .map(|o| (o.state.as_str(), o.outcome.as_str()))
        .collect();
    assert_eq!(
        by_state,
        BTreeMap::from([
            ("a", outcomes::STOPPED),
            ("b", outcomes::LEFT_RUNNING),
            ("c", outcomes::LEFT_RUNNING),
            ("d", outcomes::KILLED),
            ("e", outcomes::NOT_RUNNING),
        ]),
        "{outcomes:#?}"
    );
    let busy = outcomes.iter().find(|o| o.state == "c").unwrap();
    assert_eq!(busy.error.as_ref().unwrap().code, "BUSY_VIEW");
    // Shutdown never changes desired_running and never kills a busy worker.
    assert_eq!(
        env.log.count("catalog:set_desired"),
        0,
        "{:?}",
        env.log.all()
    );
    for state in ["a", "b", "c", "d"] {
        assert!(env.catalog.folder(state).desired_running);
    }
    assert!(env.worker("b").alive());
    assert!(env.worker("c").alive());
    assert_eq!(env.kills(), 1);
    // After shutdown the registry refuses commands.
    let refused = env.registry.send("a", FolderCmd::Start).unwrap_err();
    assert_eq!(refused.code, "ORCHESTRATOR_STOPPING");
    // A second call returns the same outcomes.
    assert_eq!(env.registry.shutdown(Duration::from_secs(1)), outcomes);
}

// ---------------------------------------------------------------------------
// adoption

#[test]
fn adoption_match_runs_adopted_without_spawn() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .worker_record("a", 4242, "0.1.0+old-build")
        .adopt(alive("0.1.0+old-build"))
        .start();
    let view = env.running("a");
    assert!(
        matches!(
            &view.phase,
            FolderPhase::Running { instance, pid: 4242, address, adopted: true, .. }
                if instance == "old-a" && address == "fake://old-a"
        ),
        "{view:#?}"
    );
    assert_eq!(env.launcher.launches(), 0);
    assert_eq!(env.kills(), 0);
    assert!(env.log.position("rpc:hello:old-a").is_some());
    env.wait_view("a", "heartbeat", |v| v.heartbeat.is_some());
}

#[test]
fn adoption_version_mismatch_stops_then_spawns() {
    // The old worker exits after its stop with an unknown exit code (Unix: a
    // pid probe cannot read it) or with 0 (Windows): a replacement either way.
    for known in [false, true] {
        let env = EnvBuilder::new()
            .folder("a", true)
            .worker_record("a", 4242, "0.0.9")
            .adopt(alive("0.0.9"))
            .adopted_exit_known(known)
            .start();
        let view = env.wait_view("a", "spawned worker running", |v| {
            matches!(v.phase, FolderPhase::Running { adopted: false, .. })
        });
        let stop = env.log.position("rpc:stop:graceful:old-a").unwrap();
        let launch = env.log.position("launch:a").unwrap();
        assert!(stop < launch, "{:?}", env.log.all());
        assert_eq!(env.kills(), 0);
        assert_ne!(view.worker.as_ref().unwrap().pid, 4242);
        assert_eq!(env.log.count("catalog:set_desired"), 0);
        assert_eq!(env.launcher.launches(), 1, "exit code known: {known}");
        assert!(last_error(&view).is_none(), "not a failure: {view:#?}");
    }
}

#[test]
fn restart_of_an_adopted_worker_spawns_at_once_whatever_its_exit_code() {
    for known in [false, true] {
        let env = EnvBuilder::new()
            .folder("a", true)
            .worker_record("a", 4242, "0.1.0")
            .adopt(alive("0.1.0"))
            .adopted_exit_known(known)
            .start();
        env.wait_view("a", "adopted", |v| {
            matches!(v.phase, FolderPhase::Running { adopted: true, .. })
        });
        env.registry.send("a", FolderCmd::Restart).unwrap();
        let view = env.wait_view("a", "spawned worker running", |v| {
            matches!(v.phase, FolderPhase::Running { adopted: false, .. })
        });
        assert!(env.log.position("rpc:stop:graceful:old-a").is_some());
        assert_eq!(env.launcher.launches(), 1, "exit code known: {known}");
        assert_eq!(env.kills(), 0);
        assert!(last_error(&view).is_none(), "not a failure: {view:#?}");
        assert!(!env.status.events().iter().any(|e| matches!(
            e,
            StatusEvent::FolderLifecycle(v) if matches!(v.phase, FolderPhase::Backoff { .. })
        )));
    }
}

#[test]
fn adopted_worker_that_refused_a_restart_and_died_counts_as_a_crash() {
    // The stop was refused (StopBlocked) and no retry was in flight: an exit
    // with an unknown code is not the stop, so it is a counted failure.
    let env = EnvBuilder::new()
        .folder("a", true)
        .worker_record("a", 4242, "0.1.0")
        .adopt(AdoptPlan::Alive {
            version: "0.1.0".into(),
            behavior: Behavior {
                stop: StopB::Busy,
                ..Behavior::default()
            },
            mount: None,
        })
        .start();
    env.wait_view("a", "adopted", |v| {
        matches!(v.phase, FolderPhase::Running { adopted: true, .. })
    });
    env.registry.send("a", FolderCmd::Restart).unwrap();
    env.wait_view("a", "stop blocked", |v| {
        matches!(v.phase, FolderPhase::StopBlocked { .. })
    });
    env.worker("a").exit(None, None);
    let view = env.wait_view("a", "backoff", |v| {
        matches!(v.phase, FolderPhase::Backoff { attempt: 1, .. })
    });
    assert_eq!(last_error(&view), Some("WORKER_EXITED"));
    assert_eq!(env.launcher.launches(), 0);
    env.advance(Duration::from_secs(2));
    env.running("a");
    assert_eq!(env.launcher.launches(), 1);
}

#[test]
fn adoption_of_a_silent_worker_fails_after_patience_without_kill_or_spawn() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .worker_record("a", 4242, "0.1.0")
        .adopt(AdoptPlan::Alive {
            version: "0.1.0".into(),
            behavior: Behavior {
                hello: Mode::Fail,
                force: StopB::Fail,
                ..Behavior::default()
            },
            mount: None,
        })
        .start();
    env.wait_view("a", "adopting", |v| {
        matches!(v.phase, FolderPhase::Starting { adopting: true, .. })
    });
    env.advance(Duration::from_secs(10));
    env.settle();
    assert!(matches!(env.view("a").phase, FolderPhase::Starting { .. }));
    env.advance(Duration::from_secs(21));
    let view = env.wait_view("a", "failed", |v| error_code(&v.phase).is_some());
    assert_eq!(
        error_code(&view.phase),
        Some("PREVIOUS_WORKER_UNRESPONSIVE")
    );
    assert!(env.log.count("rpc:hello") >= 2, "hello retried");
    assert_eq!(env.launcher.launches(), 0);
    assert_eq!(env.kills(), 0);
    assert!(env.worker("a").alive());

    // The user can ForceStop it: stop{force}, then a verified kill.
    env.registry.send("a", FolderCmd::ForceStop).unwrap();
    env.wait_view("a", "await exit", |v| {
        matches!(
            v.phase,
            FolderPhase::Stopping {
                force: true,
                stage: StopStage::AwaitExit,
                ..
            }
        )
    });
    env.advance(Duration::from_secs(11));
    env.wait_view("a", "stopped", |v| v.phase == FolderPhase::Stopped);
    assert_eq!(env.kills(), 1);
    assert_eq!(env.launcher.launches(), 0);
}

/// Folder `a` whose adopted worker never answers `hello`:
/// `Failed{PREVIOUS_WORKER_UNRESPONSIVE}` with the process still alive and
/// its `maximum_running` slot held.
fn unresponsive_adopted_folder() -> Env {
    let env = EnvBuilder::new()
        .folder("a", true)
        .worker_record("a", 4242, "0.1.0")
        .adopt(AdoptPlan::Alive {
            version: "0.1.0".into(),
            behavior: Behavior {
                hello: Mode::Fail,
                ..Behavior::default()
            },
            mount: None,
        })
        .start();
    env.wait_view("a", "adopting", |v| {
        matches!(v.phase, FolderPhase::Starting { adopting: true, .. })
    });
    let view = env.advance_until("a", "failed", Duration::from_secs(31), |v| {
        error_code(&v.phase).is_some()
    });
    assert_eq!(
        error_code(&view.phase),
        Some("PREVIOUS_WORKER_UNRESPONSIVE")
    );
    assert!(env.worker("a").alive());
    assert_eq!(env.registry.running_slots(), 1);
    env
}

#[test]
fn start_of_a_failed_folder_holding_a_live_worker_adopts_it_again() {
    // Fix round 2: Start left such a folder Stopped (desired=1) for good.
    let env = unresponsive_adopted_folder();
    let hellos = env.log.count("rpc:hello");
    env.registry.send("a", FolderCmd::Start).unwrap();
    let view = env.wait_view("a", "adopting again", |v| {
        matches!(v.phase, FolderPhase::Starting { adopting: true, .. })
    });
    assert!(view.record.desired_running);
    wait_for("hello again", || env.log.count("rpc:hello") > hellos);
    // The worker answers now: Running{adopted}, nothing spawned or killed.
    env.worker("a").set(|b| b.hello = Mode::Ok);
    let view = env.advance_until("a", "adopted", Duration::from_secs(1), |v| {
        matches!(v.phase, FolderPhase::Running { adopted: true, .. })
    });
    let error = view
        .last_error
        .as_ref()
        .expect("the failure stays as history");
    assert_eq!(error.error.code, "PREVIOUS_WORKER_UNRESPONSIVE");
    assert!(error.is_resolved(), "{view:#?}");
    assert_eq!(env.launcher.launches(), 0);
    assert_eq!(env.kills(), 0);
    assert_eq!(env.registry.running_slots(), 1);
}

#[test]
fn stop_retry_start_of_a_failed_folder_holding_a_live_worker_adopts_it_again() {
    // Fix round 2: Retry with desired=0 cleared Failed but kept the process,
    // and the next Start was ignored.
    let env = unresponsive_adopted_folder();
    let wait = Duration::from_secs(10);
    let view = env
        .registry
        .request(
            "a",
            FolderCmd::Stop {
                mode: StopMode::Graceful,
            },
            wait,
        )
        .unwrap();
    assert_eq!(
        error_code(&view.phase),
        Some("PREVIOUS_WORKER_UNRESPONSIVE")
    );
    assert!(!view.record.desired_running);
    // Nothing to start: it stays Failed, since its process is still there.
    let view = env.registry.request("a", FolderCmd::Retry, wait).unwrap();
    assert_eq!(
        error_code(&view.phase),
        Some("PREVIOUS_WORKER_UNRESPONSIVE"),
        "{view:#?}"
    );
    let hellos = env.log.count("rpc:hello");
    let view = env.registry.request("a", FolderCmd::Start, wait).unwrap();
    assert!(
        matches!(view.phase, FolderPhase::Starting { adopting: true, .. }),
        "{view:#?}"
    );
    assert!(view.record.desired_running);
    wait_for("hello again", || env.log.count("rpc:hello") > hellos);
    // Still silent: Failed again after the patience. A catalog change for
    // this folder adopts it once more (§3.3: Retry, Start or a catalog change).
    env.advance_until("a", "failed again", Duration::from_secs(5), |v| {
        error_code(&v.phase).is_some()
    });
    let hellos = env.log.count("rpc:hello");
    env.catalog
        .mutate(|s| s.folders[0].label = "renamed".into());
    env.wait_view("a", "adopting after a catalog change", |v| {
        matches!(v.phase, FolderPhase::Starting { adopting: true, .. })
    });
    wait_for("hello after the catalog change", || {
        env.log.count("rpc:hello") > hellos
    });
    assert_eq!(env.launcher.launches(), 0);
    assert_eq!(env.kills(), 0);
    assert_eq!(env.registry.running_slots(), 1);
    assert!(env.worker("a").alive());
}

#[test]
fn adopted_worker_dying_during_adoption_is_replaced_without_backoff() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .worker_record("a", 4242, "0.1.0")
        .adopt(AdoptPlan::Alive {
            version: "0.1.0".into(),
            behavior: Behavior {
                hello: Mode::Hang,
                ..Behavior::default()
            },
            mount: None,
        })
        .start();
    env.wait_view("a", "adopting", |v| {
        matches!(v.phase, FolderPhase::Starting { adopting: true, .. })
    });
    // The launcher's pid probe reports the death of the adopted process.
    env.worker("a").exit(None, None);
    let view = env.wait_view("a", "spawned worker running", |v| {
        matches!(v.phase, FolderPhase::Running { adopted: false, .. })
    });
    assert_eq!(env.launcher.launches(), 1);
    assert_eq!(view.restarts, 0);
    assert!(last_error(&view).is_none(), "{view:#?}");
    assert_eq!(env.kills(), 0);
}

#[test]
fn foreign_image_is_never_killed_and_a_held_owner_lock_blocks_the_spawn() {
    let builder = EnvBuilder::new()
        .folder("a", true)
        .worker_record("a", 4242, "0.1.0")
        .adopt(AdoptPlan::None);
    let lock = tkfs::orchestrator::lock_state(&builder.dir("a")).unwrap();
    let env = builder.start();
    let view = env.wait_view(
        "a",
        "waiting for the lock",
        |v| matches!(&v.phase, FolderPhase::Starting { phase, .. } if phase.contains("owner.lock")),
    );
    assert!(matches!(
        view.phase,
        FolderPhase::Starting {
            adopting: false,
            ..
        }
    ));
    assert!(env.log.position("adopt:4242").is_some());
    env.advance(Duration::from_secs(5));
    env.settle();
    assert_eq!(
        env.launcher.launches(),
        0,
        "never spawned while owner.lock is held"
    );
    env.advance(Duration::from_secs(6));
    // Post-integration re-review (WP2.INT): past the wait the folder shows
    // the error but keeps waiting (Starting): a lock wait is not a worker
    // failure, so it never backs off nor opens the crash breaker.
    let view = env.wait_view("a", "the lock wait error, still waiting", |v| {
        last_error(v) == Some("PREVIOUS_WORKER_STOP_TIMEOUT")
    });
    assert!(
        matches!(&view.phase, FolderPhase::Starting { phase, .. } if phase.contains("owner.lock")),
        "{view:#?}"
    );
    assert_eq!(env.launcher.launches(), 0);
    drop(lock);
    env.advance(Duration::from_secs(2));
    env.running("a");
    assert_eq!(env.launcher.launches(), 1);
    assert_eq!(env.kills(), 0, "the foreign process is never killed");
}

#[test]
fn legacy_orphan_is_stopped_before_the_spawn() {
    // A runtime.json without worker.json: status, stop, then spawn.
    let builder = EnvBuilder::new().folder("a", true).plan(Plan::Ready);
    let discovery = json!({
        "format": 1, "address": "fake://legacy", "state": builder.dir("a"),
        "mount": null, "repo": "repo-a", "device": "dev-a",
    });
    std::fs::write(
        builder.dir("a").join("runtime.json"),
        serde_json::to_vec(&discovery).unwrap(),
    )
    .unwrap();
    let log = Arc::new(Log::default());
    let clock = Arc::new(ManualClock::default());
    let catalog = Arc::new(FakeCatalog {
        snapshot: Snapshot::new(CatalogSnapshot {
            folders: builder.folders.clone(),
            ..CatalogSnapshot::default()
        }),
        log: log.clone(),
        clock: clock.clone(),
        held: Mutex::default(),
    });
    let launcher = Arc::new(FakeLauncher {
        log: log.clone(),
        catalog: catalog.clone(),
        workers: Arc::new(Mutex::new(Vec::new())),
        default_plan: Mutex::new(Plan::Ready),
        plan_for: Mutex::new(BTreeMap::new()),
        default_behavior: Mutex::new(Behavior::default()),
        behavior_for: Mutex::new(BTreeMap::new()),
        adopt: Mutex::new(AdoptPlan::None),
        adopted_exit_known: AtomicBool::new(false),
        next_pid: AtomicU32::new(1000),
    });
    let sink: ProcessSink = Arc::new(|_| {});
    let orphan = launcher.new_worker(
        Identity {
            pid: 77,
            instance: "legacy".into(),
            address: "fake://legacy".into(),
            version: "0.1.0".into(),
            adopted: false,
        },
        &builder.folders[0],
        Behavior::default(),
        sink,
    );
    let registry = FolderRegistry::start(RegistryDeps {
        catalog: catalog.clone(),
        published: Arc::new(FakePublished::default()),
        status: Arc::new(FakeStatus::default()),
        launcher: launcher.clone(),
        clock,
        limits: builder.limits.clone(),
    });
    wait_for("running", || {
        registry
            .view("a")
            .is_some_and(|v| matches!(v.phase, FolderPhase::Running { .. }))
    });
    let status = log.position("rpc:status:legacy").expect("status probe");
    let stop = log.position("rpc:stop:legacy:legacy").expect("legacy stop");
    let launch = log.position("launch:a").expect("spawn");
    assert!(status < stop && stop < launch, "{:?}", log.all());
    assert!(!orphan.alive());
    registry.shutdown(Duration::from_millis(300));
}

// ---------------------------------------------------------------------------
// gate and mount

fn evidence(installation: &str, caught_up: bool, blocked: bool) -> FolderCmd {
    FolderCmd::GateEvidence(GateEvidence {
        installation: installation.into(),
        at_ms: 5,
        caught_up,
        blocked,
        waiting_on: if caught_up {
            Vec::new()
        } else {
            vec!["metadata".into()]
        },
    })
}

#[test]
fn gate_holds_the_attach_until_evidence_and_clears_only_after_attached() {
    let env = EnvBuilder::new()
        .peer("p1")
        .peer("p2")
        .folder("a", true)
        .mounted("a")
        .gate("a", "p1")
        .start();
    let view = env.running("a");
    assert!(matches!(&view.gate, GateState::Awaiting { source, .. } if source == "p1"));
    env.settle();
    assert_eq!(
        env.log.count("rpc:mount-attach"),
        0,
        "no attach while Awaiting"
    );
    // Evidence from another peer, or not caught up, does not open the gate.
    env.registry.send("a", evidence("p2", true, false)).unwrap();
    env.registry
        .send("a", evidence("p1", false, false))
        .unwrap();
    env.wait_view(
        "a",
        "waiting_on",
        |v| matches!(&v.gate, GateState::Awaiting { waiting_on, .. } if !waiting_on.is_empty()),
    );
    env.settle();
    assert_eq!(env.log.count("rpc:mount-attach"), 0);
    env.registry.send("a", evidence("p1", true, false)).unwrap();
    let view = env.wait_view("a", "attached and cleared", |v| {
        v.mount.state.is_attached() && v.gate == GateState::None
    });
    assert!(view.mount.path.is_some());
    let attach = env.log.position("rpc:mount-attach").unwrap();
    let clear = env.log.position("catalog:clear_gate:a").unwrap();
    assert!(attach < clear, "{:?}", env.log.all());
    assert!(!env.catalog.snapshot().gates.contains_key("a"));
    assert_eq!(env.log.count("catalog:clear_gate"), 1);
}

#[test]
fn failed_attach_keeps_the_marker_and_a_transient_failure_is_retried() {
    let transient = ErrorWire::new(
        "WINFSP_MOUNT_FAILED",
        "WINFSP_MOUNT_FAILED: status 0xC0000022",
    );
    assert_eq!(transient.class, ErrorClass::Transient);
    let env = EnvBuilder::new()
        .peer("p1")
        .folder("a", true)
        .mounted("a")
        .gate("a", "p1")
        .behavior(|b| b.attach = VecDeque::from([AttachB::Failed(transient)]))
        .start();
    env.running("a");
    env.registry.send("a", evidence("p1", true, false)).unwrap();
    let view = env.wait_view("a", "attach failed", |v| {
        matches!(v.mount.state, MountState::AttachFailed { .. })
    });
    let MountState::AttachFailed {
        next_retry_at_ms, ..
    } = &view.mount.state
    else {
        unreachable!()
    };
    assert!(next_retry_at_ms.is_some(), "transient: retry scheduled");
    assert!(matches!(view.gate, GateState::Satisfied { .. }));
    assert_eq!(env.log.count("catalog:clear_gate"), 0, "marker kept");
    assert!(env.catalog.snapshot().gates.contains_key("a"));
    assert!(
        matches!(view.phase, FolderPhase::Running { .. }),
        "mount failures are mount-scoped"
    );
    env.advance(Duration::from_millis(2500));
    env.wait_view("a", "attached", |v| {
        v.mount.state.is_attached() && v.gate == GateState::None
    });
    assert_eq!(env.log.count("rpc:mount-attach"), 2);
    assert_eq!(env.log.count("catalog:clear_gate"), 1);
}

#[test]
fn needs_user_action_attach_is_not_retried_until_attach() {
    let occupied = ErrorWire::new(
        "MOUNT_PATH_MUST_NOT_EXIST",
        "MOUNT_PATH_MUST_NOT_EXIST: the path exists and is not empty",
    );
    let env = EnvBuilder::new()
        .folder("a", true)
        .mounted("a")
        .behavior(|b| b.attach = VecDeque::from([AttachB::Failed(occupied)]))
        .start();
    env.running("a");
    let view = env.wait_view("a", "attach failed", |v| {
        matches!(v.mount.state, MountState::AttachFailed { .. })
    });
    assert!(matches!(
        &view.mount.state,
        MountState::AttachFailed {
            next_retry_at_ms: None,
            ..
        }
    ));
    env.advance(Duration::from_secs(300));
    env.settle();
    assert_eq!(env.log.count("rpc:mount-attach"), 1, "no automatic retry");
    assert!(matches!(env.view("a").phase, FolderPhase::Running { .. }));
    let view = env
        .registry
        .request("a", FolderCmd::Attach, Duration::from_secs(10))
        .unwrap();
    assert!(view.mount.state.is_attached(), "{view:#?}");
    assert_eq!(env.log.count("rpc:mount-attach"), 2);
}

#[test]
fn heartbeat_taken_before_an_attach_result_does_not_undo_it() {
    // Found by parallel stress runs in fix round 2: a watch already in
    // flight when the attach answered returned the earlier AttachFailed
    // state, which re-armed the attach (a redundant mount-attach).
    let occupied = ErrorWire::new(
        "MOUNT_PATH_MUST_NOT_EXIST",
        "MOUNT_PATH_MUST_NOT_EXIST: the path exists and is not empty",
    );
    let env = EnvBuilder::new()
        .folder("a", true)
        .mounted("a")
        .behavior(|b| b.attach = VecDeque::from([AttachB::Failed(occupied)]))
        .start();
    env.running("a");
    env.wait_view("a", "attach failed", |v| {
        matches!(v.mount.state, MountState::AttachFailed { .. })
    });
    // The next watch takes its heartbeat (AttachFailed) now and answers late.
    let worker = env.worker("a");
    worker.set(|b| b.watch = Mode::HoldStale);
    wait_for("a watch in flight", || env.log.count("watch-held:") >= 1);
    let view = env
        .registry
        .request("a", FolderCmd::Attach, Duration::from_secs(10))
        .unwrap();
    assert!(view.mount.state.is_attached(), "{view:#?}");
    let heartbeats = || {
        env.status
            .events()
            .iter()
            .filter(|e| matches!(e, StatusEvent::WorkerHeartbeat { state, .. } if state == "a"))
            .count()
    };
    let before = heartbeats();
    worker.set(|b| b.watch = Mode::Ok);
    wait_for("the stale heartbeat and a fresh one", || {
        heartbeats() >= before + 2
    });
    env.settle();
    assert_eq!(env.log.count("rpc:mount-attach"), 2, "{:?}", env.log.all());
    let view = env.view("a");
    assert!(view.mount.state.is_attached(), "{view:#?}");
}

#[test]
fn blocked_evidence_holds_the_gate_until_override() {
    let env = EnvBuilder::new()
        .peer("p1")
        .folder("a", true)
        .mounted("a")
        .gate("a", "p1")
        .start();
    env.running("a");
    env.registry.send("a", evidence("p1", true, true)).unwrap();
    let view = env.wait_view("a", "blocked", |v| {
        matches!(v.gate, GateState::Blocked { .. })
    });
    assert_eq!(last_error(&view), Some("GATE_BLOCKED"));
    assert!(!view.last_error.as_ref().unwrap().is_resolved());
    env.settle();
    assert_eq!(env.log.count("rpc:mount-attach"), 0);
    env.registry.send("a", FolderCmd::OverrideGate).unwrap();
    let view = env.wait_view("a", "attached", |v| {
        v.mount.state.is_attached() && v.gate == GateState::None
    });
    let overridden = env.log.position("catalog:override_gate:a").unwrap();
    let attach = env.log.position("rpc:mount-attach").unwrap();
    let clear = env.log.position("catalog:clear_gate:a").unwrap();
    assert!(overridden < attach && attach < clear, "{:?}", env.log.all());
    // Fix round 2: the override ends the block (history, not an active error).
    assert_eq!(last_error(&view), Some("GATE_BLOCKED"));
    assert!(view.last_error.as_ref().unwrap().is_resolved(), "{view:#?}");
}

#[test]
fn revalidate_reopens_a_blocked_gate() {
    let env = EnvBuilder::new()
        .peer("p1")
        .folder("a", true)
        .mounted("a")
        .gate("a", "p1")
        .start();
    env.running("a");
    env.registry.send("a", evidence("p1", true, true)).unwrap();
    env.wait_view("a", "blocked", |v| {
        matches!(v.gate, GateState::Blocked { .. })
    });
    let view = env
        .registry
        .request(
            "a",
            FolderCmd::Revalidate { ids: None },
            Duration::from_secs(10),
        )
        .unwrap();
    assert!(
        matches!(&view.gate, GateState::Awaiting { source, .. } if source == "p1"),
        "{view:#?}"
    );
    assert_eq!(env.log.count("rpc:revalidate"), 1);
    // Revalidating does not end the block by itself; evidence does.
    assert_eq!(last_error(&view), Some("GATE_BLOCKED"));
    assert!(!view.last_error.as_ref().unwrap().is_resolved());
    // Fresh evidence after the revalidation opens the gate.
    env.registry.send("a", evidence("p1", true, false)).unwrap();
    let view = env.wait_view("a", "attached", |v| {
        v.mount.state.is_attached() && v.gate == GateState::None
    });
    assert!(view.last_error.as_ref().unwrap().is_resolved(), "{view:#?}");
}

#[test]
fn shutdown_can_leave_starting_workers_running() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .plan(Plan::Silent)
        .limits(|l| l.shutdown_kill_starting = false)
        .start();
    env.wait_view("a", "starting", |v| {
        matches!(v.phase, FolderPhase::Starting { .. })
    });
    let outcomes = env.registry.shutdown(Duration::from_secs(2));
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].outcome, outcomes::LEFT_RUNNING);
    assert_eq!(env.kills(), 0);
}

#[test]
fn unpaired_gate_source_is_retargeted_to_a_peer_sharing_the_folder() {
    let env = EnvBuilder::new()
        .peer("p1")
        .peer("p2")
        .folder("a", true)
        .mounted("a")
        .gate("a", "p1")
        .start();
    env.running("a");
    // p2 shares the folder (it sent evidence), but is not the source yet.
    env.registry
        .send("a", evidence("p2", false, false))
        .unwrap();
    env.settle();
    env.catalog
        .mutate(|snapshot| snapshot.peers.retain(|p| p.installation != "p1"));
    wait_for("retarget", || {
        env.log.position("catalog:retarget_gate:a:p2").is_some()
    });
    env.wait_view(
        "a",
        "awaiting p2",
        |v| matches!(&v.gate, GateState::Awaiting { source, .. } if source == "p2"),
    );
    env.settle();
    assert_eq!(env.log.count("rpc:mount-attach"), 0);
    env.registry.send("a", evidence("p2", true, false)).unwrap();
    env.wait_view("a", "attached", |v| {
        v.mount.state.is_attached() && v.gate == GateState::None
    });
}

#[test]
fn detach_holds_the_mount_until_attach() {
    let env = EnvBuilder::new().folder("a", true).mounted("a").start();
    env.running("a");
    env.wait_view("a", "attached", |v| v.mount.state.is_attached());
    let view = env
        .registry
        .request(
            "a",
            FolderCmd::Detach { force: false },
            Duration::from_secs(10),
        )
        .unwrap();
    assert_eq!(view.mount.state, MountState::Detached);
    env.advance(Duration::from_secs(300));
    env.settle();
    assert_eq!(
        env.log.count("rpc:mount-attach"),
        1,
        "no re-attach after Detach"
    );
    let view = env
        .registry
        .request("a", FolderCmd::Attach, Duration::from_secs(10))
        .unwrap();
    assert!(view.mount.state.is_attached());
}

#[test]
fn adopted_attached_worker_is_not_attached_again() {
    let builder = EnvBuilder::new().folder("a", true).mounted("a");
    let path = builder.folders[0].mount.clone();
    let env = builder
        .worker_record("a", 4242, "0.1.0")
        .adopt(AdoptPlan::Alive {
            version: "0.1.0".into(),
            behavior: Behavior::default(),
            mount: Some(MountStatus {
                intent: MountIntent::Attach,
                path,
                state: MountState::Attached { since_ms: 1 },
            }),
        })
        .start();
    env.running("a");
    env.wait_view("a", "attached from heartbeat", |v| {
        v.mount.state.is_attached()
    });
    env.settle();
    assert_eq!(env.log.count("rpc:mount-attach"), 0);
}

// ---------------------------------------------------------------------------
// registry: limits, catalog changes, published entries

#[test]
fn worker_limit_is_enforced_and_a_waiting_folder_starts_when_a_slot_frees() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .folder("b", true)
        .limits(|l| l.maximum_running = 1)
        .start();
    wait_for("one running, one refused", || {
        let views = env.registry.all();
        let running = views
            .iter()
            .filter(|v| matches!(v.phase, FolderPhase::Running { .. }))
            .count();
        let refused = views
            .iter()
            .filter(|v| v.phase == FolderPhase::Stopped && last_error(v) == Some("WORKER_LIMIT"))
            .count();
        running == 1 && refused == 1
    });
    let views = env.registry.all();
    let running = views
        .iter()
        .find(|v| matches!(v.phase, FolderPhase::Running { .. }))
        .unwrap()
        .record
        .state
        .clone();
    let waiting = if running == "a" { "b" } else { "a" };
    let refused = env.registry.send(waiting, FolderCmd::Start).unwrap_err();
    assert_eq!(refused.code, "WORKER_LIMIT");
    assert_eq!(env.registry.running_slots(), 1);
    env.registry
        .request(
            &running,
            FolderCmd::Stop {
                mode: StopMode::Graceful,
            },
            Duration::from_secs(10),
        )
        .unwrap();
    env.running(waiting);
    assert_eq!(env.registry.running_slots(), 1);
}

#[test]
fn catalog_changes_spawn_start_and_stop_controllers() {
    let env = EnvBuilder::new().folder("a", false).start();
    assert_eq!(env.view("a").phase, FolderPhase::Stopped);
    // desired 0 → 1 in the catalog (another writer): the folder starts.
    env.catalog.mutate(|s| s.folders[0].desired_running = true);
    env.running("a");
    // A new folder appears.
    let directory = env.dir("n");
    std::fs::create_dir_all(&directory).unwrap();
    env.catalog.mutate(|s| {
        s.folders.push(FolderRecord {
            state: "n".into(),
            label: "n".into(),
            repo: "repo-n".into(),
            device: "dev-n".into(),
            directory,
            mount: None,
            desired_running: true,
            sync_enabled: true,
            mount_enabled: false,
        })
    });
    wait_for("controller for n", || env.registry.view("n").is_some());
    env.running("n");
    // desired 1 → 0: stopped without another catalog write.
    env.catalog.mutate(|s| s.folders[0].desired_running = false);
    env.wait_view("a", "stopped", |v| v.phase == FolderPhase::Stopped);
    assert_eq!(env.log.count("catalog:set_desired"), 0);
    // The folder leaves the catalog: its controller is retired.
    env.catalog.mutate(|s| s.folders.retain(|f| f.state != "n"));
    wait_for("n retired", || env.registry.view("n").is_none());
    wait_for("FolderRemoved", || {
        env.status
            .events()
            .iter()
            .any(|e| matches!(e, StatusEvent::FolderRemoved { state } if state == "n"))
    });
    assert_eq!(
        env.registry.send("n", FolderCmd::Start).unwrap_err().code,
        "FOLDER_NOT_FOUND"
    );
}

#[test]
fn retiring_a_failed_folder_leaves_its_unresponsive_worker_and_ends_the_controller() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .worker_record("a", 4242, "0.1.0")
        .adopt(AdoptPlan::Alive {
            version: "0.1.0".into(),
            behavior: Behavior {
                hello: Mode::Fail,
                ..Behavior::default()
            },
            mount: None,
        })
        .start();
    env.wait_view("a", "adopting", |v| {
        matches!(v.phase, FolderPhase::Starting { adopting: true, .. })
    });
    env.advance(Duration::from_secs(31));
    let view = env.wait_view("a", "failed", |v| error_code(&v.phase).is_some());
    assert_eq!(
        error_code(&view.phase),
        Some("PREVIOUS_WORKER_UNRESPONSIVE")
    );
    assert_eq!(env.registry.running_slots(), 1);
    env.catalog.mutate(|s| s.folders.clear());
    wait_for("a retired", || env.registry.view("a").is_none());
    // The controller ends (its slot is released); the worker is left running.
    wait_for("controller ended", || env.registry.running_slots() == 0);
    assert!(env.worker("a").alive());
    assert_eq!(env.kills(), 0);
    assert_eq!(env.log.count("rpc:stop"), 0);
}

#[test]
fn retiring_a_running_folder_sends_nothing_after_folder_removed() {
    // Fix round 2: the retired controller kept sending FolderLifecycle
    // (Stopping, Stopped) after FolderRemoved, and the StatusHub registers a
    // folder again on any FolderLifecycle.
    let env = EnvBuilder::new().folder("a", true).start();
    env.running("a");
    wait_for("a heartbeat event", || {
        env.status
            .events()
            .iter()
            .any(|e| matches!(e, StatusEvent::WorkerHeartbeat { state, .. } if state == "a"))
    });
    let worker = env.worker("a");
    env.catalog.mutate(|s| s.folders.clear());
    wait_for("a retired", || env.registry.view("a").is_none());
    wait_for("worker stopped", || !worker.alive());
    // The controller ends once the exit is in (its slot is released).
    wait_for("controller ended", || env.registry.running_slots() == 0);
    env.settle();
    assert_eq!(env.log.count("rpc:stop:graceful"), 1);
    assert_eq!(env.kills(), 0);
    assert!(env.published.entry("a").is_none());
    let events = env.status.events();
    let removed = events
        .iter()
        .position(|e| matches!(e, StatusEvent::FolderRemoved { state } if state == "a"))
        .expect("FolderRemoved");
    let late: Vec<&StatusEvent> = events[removed + 1..]
        .iter()
        .filter(|e| match e {
            StatusEvent::FolderLifecycle(view) => view.record.state == "a",
            StatusEvent::WorkerHeartbeat { state, .. } | StatusEvent::FolderRemoved { state } => {
                state == "a"
            }
            _ => false,
        })
        .collect();
    assert!(late.is_empty(), "events after FolderRemoved: {late:#?}");
    assert!(
        events[..removed]
            .iter()
            .any(|e| matches!(e, StatusEvent::FolderLifecycle(view) if view.record.state == "a")),
        "lifecycle events before the removal"
    );
}

#[test]
fn retiring_during_a_kill_ends_the_controller_even_if_the_worker_never_exits() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .plan(Plan::Silent)
        .start();
    env.wait_view("a", "starting", |v| {
        matches!(v.phase, FolderPhase::Starting { .. })
    });
    let worker = env.worker("a");
    worker.killable.store(false, Ordering::SeqCst);
    // Watchdog expiry: kill, then Backoff once the worker is gone.
    env.advance(Duration::from_secs(31));
    wait_for("first kill", || env.kills() == 1);
    env.advance(Duration::from_secs(11));
    wait_for("second kill", || env.kills() == 2);
    env.catalog.mutate(|s| s.folders.clear());
    wait_for("a retired", || env.registry.view("a").is_none());
    assert_eq!(env.registry.running_slots(), 1);
    // The kills are exhausted (WORKER_STOP_TIMEOUT): the retired controller
    // ends instead of waiting forever. No Tick reaches a retired folder, so
    // its own timer (at most 2 s) fires.
    env.clock.advance(Duration::from_secs(11));
    wait_for("controller ended", || env.registry.running_slots() == 0);
    assert!(worker.alive());
    assert_eq!(env.kills(), 2);
}

#[test]
fn shutdown_racing_catalog_additions_reaches_every_controller() {
    // Folders keep appearing while shutdown runs. Every controller that exists
    // afterwards answered the shutdown (none was added after its handles were
    // collected), and every launched worker belongs to one of them.
    for round in 0..12u64 {
        let env = EnvBuilder::new().folder("a", true).start();
        env.running("a");
        let stop = Arc::new(AtomicBool::new(false));
        let adder = {
            let catalog = env.catalog.clone();
            let stop = stop.clone();
            let base = env.dir.path().join("states");
            thread::spawn(move || {
                let mut next = 0;
                while !stop.load(Ordering::SeqCst) && next < 400 {
                    let state = format!("n{next}");
                    next += 1;
                    catalog.mutate(|s| {
                        s.folders.push(FolderRecord {
                            state: state.clone(),
                            label: state.clone(),
                            repo: format!("repo-{state}"),
                            device: format!("dev-{state}"),
                            directory: base.join(&state),
                            mount: None,
                            desired_running: next % 2 == 0,
                            sync_enabled: true,
                            mount_enabled: false,
                        })
                    });
                    thread::yield_now();
                }
            })
        };
        thread::sleep(Duration::from_millis(2 + round % 5));
        let outcomes = env.registry.shutdown(Duration::from_secs(2));
        stop.store(true, Ordering::SeqCst);
        adder.join().unwrap();
        thread::sleep(Duration::from_millis(100));
        let answered: BTreeSet<String> = outcomes.iter().map(|o| o.state.clone()).collect();
        for view in env.registry.all() {
            assert!(
                answered.contains(&view.record.state),
                "round {round}: {} has a controller but no shutdown outcome",
                view.record.state
            );
        }
        for entry in env.log.all() {
            if let Some(state) = entry.strip_prefix("launch:") {
                assert!(
                    answered.contains(state),
                    "round {round}: {state} launched a worker outside the shutdown"
                );
            }
        }
    }
}

/// The branches generation the fake worker's summary saw (its one branch).
fn summary_seen(entry: &PublishedEntry) -> &str {
    entry
        .branches
        .first()
        .map_or("", |branch| branch.name.as_str())
}

#[test]
fn published_entry_taken_before_the_first_heartbeat_is_refreshed_once() {
    // The summary on Ready precedes every heartbeat, so its branches
    // generation is unknown; a branch change before the first heartbeat must
    // still reach the entry.
    let env = EnvBuilder::new()
        .folder("a", true)
        .behavior(|b| b.watch = Mode::Hold)
        .start();
    env.running("a");
    // (The worker's entry: before it, the folder is published unavailable,
    // WP3.INT re-review 2.)
    wait_for("entry", || {
        env.published
            .entry("a")
            .is_some_and(|entry| entry.state == EntryState::Running)
    });
    assert_eq!(summary_seen(&env.published.entry("a").unwrap()), "fake-b0");
    assert_eq!(env.log.count("rpc:published-summary"), 1);
    let worker = env.worker("a");
    worker.hb.lock().unwrap().branches = 5;
    worker.set(|b| b.watch = Mode::Ok);
    wait_for("refreshed entry", || {
        env.published
            .entry("a")
            .is_some_and(|e| summary_seen(&e) == "fake-b5")
    });
    env.wait_view("a", "heartbeats", |v| {
        v.heartbeat.as_ref().is_some_and(|hb| hb.seq >= 4)
    });
    assert_eq!(
        env.log.count("rpc:published-summary"),
        2,
        "one extra summary per start, not one per heartbeat"
    );
}

#[test]
fn published_summary_failure_marks_the_entry_unavailable_and_branch_changes_refresh() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .behavior(|b| b.summary = Mode::Fail)
        .start();
    env.running("a");
    wait_for("unavailable entry", || {
        env.published.entry("a").is_some_and(|e| {
            e.state == EntryState::Unavailable
                && e.error
                    .as_ref()
                    .is_some_and(|e| e.code == "PUBLISHED_SUMMARY_FAILED")
        })
    });
    let view = env.wait_view("a", "published error", |v| v.published_error.is_some());
    assert_eq!(
        view.published_error.as_ref().unwrap().error.code,
        "PUBLISHED_SUMMARY_FAILED"
    );
    assert!(
        matches!(view.phase, FolderPhase::Running { .. }),
        "only the entry is affected"
    );
    // The retry below then carries a known branches generation (a summary
    // taken before the first heartbeat is followed by one more, see
    // `published_entry_taken_before_the_first_heartbeat_is_refreshed_once`).
    env.wait_view("a", "heartbeat", |v| v.heartbeat.is_some());
    env.worker("a").set(|b| b.summary = Mode::Ok);
    env.advance(Duration::from_secs(61));
    wait_for("running entry", || {
        env.published
            .entry("a")
            .is_some_and(|e| e.state == EntryState::Running && e.error.is_none())
    });
    env.wait_view("a", "published error resolved", |v| {
        v.published_error
            .as_ref()
            .is_some_and(|r| r.resolved_at_ms.is_some())
    });
    // The tip follows the heartbeat without an RPC.
    let calls = env.log.count("rpc:published-summary");
    let tip = env.published.entry("a").unwrap().tip;
    wait_for("tip moves", || env.published.entry("a").unwrap().tip != tip);
    assert_eq!(
        env.log.count("rpc:published-summary"),
        calls,
        "{:?}",
        env.log.all()
    );
    // A branch change triggers another summary.
    env.worker("a").hb.lock().unwrap().branches = 9;
    wait_for("summary after branch change", || {
        env.log.count("rpc:published-summary") > calls
    });
    // SetSync{false} pauses the entry.
    env.registry
        .send("a", FolderCmd::SetSync { enabled: false })
        .unwrap();
    wait_for("paused entry", || {
        env.published
            .entry("a")
            .is_some_and(|e| e.state == EntryState::Paused)
    });
}

/// A folder that is stopped when the daemon starts is published
/// `unavailable`, built from its record (WP3.INT re-review 2): its peers keep
/// the pair and wait for it, instead of reading the share as withdrawn ("Not
/// shared with any peer") until it runs again. Its label follows the record
/// while it is stopped; the worker's summary replaces it once it runs (with
/// the record's label still, CCR-WP4.3-2); only leaving the catalog removes
/// it.
#[test]
fn a_folder_stopped_at_start_is_published_unavailable() {
    let env = EnvBuilder::new()
        .folder("a", false)
        .folder("b", true)
        .start();
    env.running("b");
    wait_for("entry b", || {
        env.published
            .entry("b")
            .is_some_and(|entry| entry.state == EntryState::Running)
    });
    env.advance(Duration::from_secs(5));
    env.settle();
    let record = env.catalog.folder("a");
    let entry = env
        .published
        .entry("a")
        .expect("a folder stopped at start is published");
    assert_eq!(entry.state, EntryState::Unavailable);
    assert_eq!(
        (&entry.repo, &entry.replica, &entry.label),
        (&record.repo, &record.device, &record.label)
    );
    assert!(entry.branches.is_empty() && entry.tip.is_none() && entry.content.is_none());
    assert_eq!(env.view("a").phase, FolderPhase::Stopped);
    // Renamed while stopped: the entry follows.
    env.catalog.mutate(|snapshot| {
        for folder in &mut snapshot.folders {
            if folder.state == "a" {
                folder.label = "renamed".into();
            }
        }
    });
    wait_for("renamed entry", || {
        env.published
            .entry("a")
            .is_some_and(|entry| entry.label == "renamed" && entry.state == EntryState::Unavailable)
    });
    // Started: the worker's summary replaces it.
    env.registry
        .request("a", FolderCmd::Start, Duration::from_secs(10))
        .unwrap();
    env.running("a");
    // (Its label stays the catalog's, never the worker's repository id:
    // CCR-WP4.3-2, applied by WP4.INT; this checked the fake's label.)
    wait_for("running entry a", || {
        env.published.entry("a").is_some_and(|entry| {
            entry.state == EntryState::Running
                && summary_seen(&entry) == "fake-b0"
                && entry.label == "renamed"
        })
    });
    // Renamed while it runs: the entry follows too.
    env.catalog.mutate(|snapshot| {
        for folder in &mut snapshot.folders {
            if folder.state == "a" {
                folder.label = "renamed again".into();
            }
        }
    });
    wait_for("renamed running entry", || {
        env.published.entry("a").is_some_and(|entry| {
            entry.state == EntryState::Running && entry.label == "renamed again"
        })
    });
    // Stopped again: unavailable, and kept.
    env.registry
        .request(
            "a",
            FolderCmd::Stop {
                mode: StopMode::Graceful,
            },
            Duration::from_secs(10),
        )
        .unwrap();
    wait_for("unavailable entry a", || {
        env.published
            .entry("a")
            .is_some_and(|entry| entry.state == EntryState::Unavailable)
    });
    // Leaving the catalog removes it (and the other folder's stays).
    env.catalog
        .mutate(|snapshot| snapshot.folders.retain(|folder| folder.state != "a"));
    wait_for("a retired", || env.registry.view("a").is_none());
    wait_for("entry a removed", || env.published.entry("a").is_none());
    env.settle();
    assert!(env.published.entry("a").is_none());
    assert!(env.published.entry("b").is_some());
}

#[test]
fn request_to_an_unknown_folder_is_not_found() {
    let env = EnvBuilder::new().folder("a", false).start();
    assert_eq!(
        env.registry
            .request("zzz", FolderCmd::Start, Duration::from_secs(1))
            .unwrap_err()
            .code,
        "FOLDER_NOT_FOUND"
    );
    assert!(env.registry.view("zzz").is_none());
    assert!(env.registry.by_repo("repo-zzz").is_none());
}

// ---------------------------------------------------------------------------
// Post-integration re-review (WP2.INT): lifecycle isolation

/// A slow catalog write never freezes a folder's lifecycle (§2.3 Catalog
/// row): with the `ClearGate` that follows a gated attach held in the
/// catalog, a worker exit is still handled at once, commands are answered,
/// and Shutdown reports the dead worker as not running. Before the fix the
/// controller waited for the write on its own thread: still `Running` for
/// the dead worker, `Retry` answered only at its deadline, and Shutdown
/// reported `left_running REQUEST_TIMEOUT`.
#[test]
fn a_slow_catalog_write_never_freezes_the_folder_lifecycle() {
    let env = EnvBuilder::new()
        .peer("p1")
        .folder("a", true)
        .mounted("a")
        .gate("a", "p1")
        .folder("b", true)
        .start();
    env.running("a");
    env.running("b");
    env.catalog
        .held
        .lock()
        .unwrap()
        .insert("clear_gate:a".into());
    env.registry.send("a", evidence("p1", true, false)).unwrap();
    wait_for("the ClearGate write in the catalog", || {
        env.log.count("catalog:clear_gate:a") == 1
    });
    env.wait_view("a", "attached", |v| v.mount.state.is_attached());
    // Both workers die while the write is held.
    env.worker("a").exit(Some(70), None);
    env.worker("b").exit(Some(70), None);
    env.wait_view("b", "backoff", |v| {
        matches!(v.phase, FolderPhase::Backoff { .. })
    });
    let started = Instant::now();
    env.wait_view("a", "backoff while the write is held", |v| {
        matches!(v.phase, FolderPhase::Backoff { .. })
    });
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    let started = Instant::now();
    let view = env
        .registry
        .request("a", FolderCmd::Tick, Duration::from_secs(2))
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(
        matches!(view.phase, FolderPhase::Backoff { .. }),
        "{view:#?}"
    );
    // The marker is still there (the write did not finish), the attach result
    // stands, nothing else was written twice.
    assert_eq!(env.log.count("catalog:clear_gate:a"), 1);
    let outcomes = env.registry.shutdown(Duration::from_secs(1));
    let a = outcomes.iter().find(|o| o.state == "a").unwrap();
    assert_eq!(a.outcome, outcomes::NOT_RUNNING, "{outcomes:?}");
    env.catalog.held.lock().unwrap().clear();
}

/// A folder with `desired_running = 0` whose previous worker is still alive
/// (a Stop that was StopBlocked when the daemon ended, a Shutdown that left
/// it running) is adopted at controller start and the stop is driven again:
/// graceful, StopBlocked with retries, ForceStop. Before the fix it showed
/// `Stopped` with the worker running unsupervised, and ForceStop did
/// nothing.
#[test]
fn a_live_worker_of_a_stopped_folder_is_adopted_and_stopped() {
    let env = EnvBuilder::new()
        .folder("a", false)
        .worker_record("a", 4242, "0.1.0")
        .adopt(AdoptPlan::Alive {
            version: "0.1.0".into(),
            behavior: Behavior {
                stop: StopB::Busy,
                ..Behavior::default()
            },
            mount: None,
        })
        .start();
    let view = env.wait_view("a", "stop blocked", |v| {
        matches!(v.phase, FolderPhase::StopBlocked { .. })
    });
    assert!(!view.record.desired_running);
    assert!(
        env.log.position("adopt:4242").is_some(),
        "{:?}",
        env.log.all()
    );
    assert!(env.log.position("rpc:stop:graceful:old-a").is_some());
    assert_eq!(
        env.registry.running_slots(),
        1,
        "the live worker holds a slot"
    );
    // StopBlocked retries the graceful stop.
    let stops = env.log.count("rpc:stop:graceful");
    env.advance(Duration::from_secs(3));
    wait_for("a retried stop", || {
        env.log.count("rpc:stop:graceful") > stops
    });
    // ForceStop now acts on the adopted worker.
    env.registry.send("a", FolderCmd::ForceStop).unwrap();
    env.wait_view("a", "stopped", |v| v.phase == FolderPhase::Stopped);
    assert!(env.log.position("rpc:stop:force:old-a").is_some());
    assert!(!env.worker("a").alive());
    assert_eq!(env.kills(), 0);
    assert_eq!(
        env.launcher.launches(),
        0,
        "nothing is spawned for desired=0"
    );
    assert_eq!(env.registry.running_slots(), 0);
}

/// A received folder with no mount to attach clears its gate marker once the
/// gate is satisfied (the legacy rule). Before the fix the marker stayed:
/// `initial_sync_pending` for good, and after a restart with the source
/// unreachable the folder read as starting ("synchronizing remote
/// metadata").
#[test]
fn a_headless_received_folder_clears_its_gate_once_satisfied() {
    let env = EnvBuilder::new()
        .peer("p1")
        .folder("a", true)
        .gate("a", "p1")
        .start();
    env.running("a");
    env.settle();
    assert_eq!(
        env.log.count("catalog:clear_gate"),
        0,
        "not before evidence"
    );
    env.registry.send("a", evidence("p1", true, false)).unwrap();
    env.wait_view("a", "gate cleared", |v| v.gate == GateState::None);
    assert_eq!(env.log.count("catalog:clear_gate:a"), 1);
    assert!(!env.catalog.snapshot().gates.contains_key("a"));
    assert_eq!(env.log.count("rpc:mount-attach"), 0);
    // A mount configured later attaches at once (the metadata was complete).
    env.catalog.mutate(|snapshot| {
        let folder = &mut snapshot.folders[0];
        folder.mount = Some(PathBuf::from("/m/a"));
        folder.mount_enabled = true;
    });
    env.wait_view("a", "attached", |v| v.mount.state.is_attached());
}

/// A live previous worker is adopted even when every `maximum_running` slot
/// is taken: it already runs. Before the fix the start took a slot first, so
/// the folder showed `WORKER_LIMIT` (Stopped) and the live worker ran
/// unsupervised.
#[test]
fn adoption_of_a_live_worker_takes_a_slot_beyond_the_limit() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .folder("b", false)
        .limits(|l| l.maximum_running = 1)
        .adopt(alive("0.1.0"))
        .start();
    env.running("a");
    assert_eq!(env.registry.running_slots(), 1);
    // b's previous worker is alive; b is started while a holds the only slot.
    write_worker_record(&env.catalog.folder("b"), 4343, "0.1.0");
    env.catalog
        .mutate(|snapshot| snapshot.folders[1].desired_running = true);
    let view = env.wait_view("b", "adopted", |v| {
        matches!(v.phase, FolderPhase::Running { adopted: true, .. })
    });
    assert_ne!(last_error(&view), Some("WORKER_LIMIT"), "{view:#?}");
    assert!(env.log.position("adopt:4343").is_some());
    assert_eq!(env.launcher.launches(), 1, "only a was spawned");
    assert_eq!(env.registry.running_slots(), 2);
    // Once b stops, the count is back under the limit.
    env.registry
        .request(
            "b",
            FolderCmd::Stop {
                mode: StopMode::Graceful,
            },
            Duration::from_secs(10),
        )
        .unwrap();
    env.wait_view("b", "stopped", |v| v.phase == FolderPhase::Stopped);
    assert_eq!(env.registry.running_slots(), 1);
}

/// A graceful Stop sent while a previous worker is being adopted (it has not
/// answered `hello`) follows the adoption rule: the failed stop is retried,
/// and after the adoption patience the folder is
/// `Failed{PREVIOUS_WORKER_UNRESPONSIVE}`; it is never killed without
/// ForceStop. Before the fix the failed stop led to a `hello` probe whose
/// failure killed the worker without the `stop{force}` that persists its
/// dirty sessions.
#[test]
fn a_failed_stop_of_a_worker_being_adopted_never_kills_it() {
    let env = EnvBuilder::new()
        .folder("a", true)
        .worker_record("a", 4242, "0.1.0")
        .adopt(AdoptPlan::Alive {
            version: "0.1.0".into(),
            behavior: Behavior {
                hello: Mode::Fail,
                stop: StopB::Fail,
                force: StopB::Exit,
                ..Behavior::default()
            },
            mount: None,
        })
        .start();
    env.wait_view("a", "adopting", |v| {
        matches!(v.phase, FolderPhase::Starting { adopting: true, .. })
    });
    env.registry
        .send(
            "a",
            FolderCmd::Stop {
                mode: StopMode::Graceful,
            },
        )
        .unwrap();
    wait_for("the graceful stop", || {
        env.log.count("rpc:stop:graceful:old-a") >= 1
    });
    let view = env.advance_until("a", "failed", Duration::from_secs(3), |v| {
        error_code(&v.phase).is_some()
    });
    assert_eq!(
        error_code(&view.phase),
        Some("PREVIOUS_WORKER_UNRESPONSIVE"),
        "{view:#?}"
    );
    assert!(env.log.count("rpc:stop:graceful:old-a") >= 2, "retried");
    assert_eq!(env.kills(), 0, "{:?}", env.log.all());
    assert!(env.worker("a").alive());
    // ForceStop is the user's kill: stop{force} first.
    env.registry.send("a", FolderCmd::ForceStop).unwrap();
    env.wait_view("a", "stopped", |v| v.phase == FolderPhase::Stopped);
    assert!(env.log.position("rpc:stop:force:old-a").is_some());
    assert_eq!(env.kills(), 0);
    assert_eq!(env.launcher.launches(), 0);
}

/// A live process that holds `owner.lock` without a `worker.json` (an orphan
/// still recovering its store after a daemon crash) keeps the folder waiting
/// without counting failures, and is adopted once it publishes its
/// `worker.json`. Before the fix each 10 s wait was a counted failure: after
/// five, `Failed{CRASH_LOOP}` with no worker ever launched.
#[test]
fn a_lock_wait_never_opens_the_crash_breaker_and_adopts_a_late_worker() {
    let builder = EnvBuilder::new().folder("a", true).adopt(alive("0.1.0"));
    let lock = tkfs::orchestrator::lock_state(&builder.dir("a")).unwrap();
    let env = builder.start();
    env.wait_view(
        "a",
        "waiting for the lock",
        |v| matches!(&v.phase, FolderPhase::Starting { phase, .. } if phase.contains("owner.lock")),
    );
    for _ in 0..16 {
        env.advance(Duration::from_secs(5));
        thread::sleep(Duration::from_millis(20));
    }
    env.settle();
    let view = env.view("a");
    assert!(
        matches!(&view.phase, FolderPhase::Starting { phase, .. } if phase.contains("owner.lock")),
        "{view:#?}"
    );
    assert_eq!(last_error(&view), Some("PREVIOUS_WORKER_STOP_TIMEOUT"));
    assert_eq!(view.restarts, 0);
    assert_eq!(env.launcher.launches(), 0);
    // The orphan finishes its recovery and publishes its worker.json.
    write_worker_record(&env.catalog.folder("a"), 4242, "0.1.0");
    env.advance(Duration::from_secs(1));
    let view = env.wait_view("a", "adopted", |v| {
        matches!(v.phase, FolderPhase::Running { adopted: true, .. })
    });
    assert!(view.last_error.as_ref().unwrap().is_resolved(), "{view:#?}");
    assert_eq!(env.launcher.launches(), 0);
    assert_eq!(env.kills(), 0);
    drop(lock);
}
