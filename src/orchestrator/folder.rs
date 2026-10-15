//! Owned by WP1.2.
//!
//! The per-folder `FolderController` (REVAMP_PLAN §3.3, §14 WP1.2).
//!
//! One actor thread (`folder-<state>`) owns one folder's worker process. Its
//! mailbox ([`Msg`]) receives commands, process events from the launcher's
//! sink, heartbeats and watch failures from the watch thread, and the results
//! of worker RPCs, which run on short-lived helper threads so the actor never
//! blocks on a worker. The loop waits with `recv_timeout(next deadline)` and
//! publishes a `Snapshot<FolderView>` after every step.
//!
//! * **Phases** are derived from sub-states (`failed`, `stop`, `start`,
//!   `backoff_state`, `running`) in that precedence, so a phase can never
//!   disagree with the plan that drives it.
//! * **Start**: adoption through `STATE/worker.json` (`launcher.adopt`, then
//!   `hello` identity and version checks), the legacy orphan path
//!   (`runtime.json` without `worker.json`: `status`, `stop`), and a spawn that
//!   happens only after `owner.lock` is free (invariant 23). A spawned worker
//!   is watched by `startup::Watchdog`; its Ready line is validated (repo,
//!   device, `ipc == 3`).
//! * **Failures**: exits are classified from the FatalLine class and the exit
//!   code; Transient failures back off (1 s · 2^(n-1), cap 300 s, ±20 %), and
//!   5 failures in 10 min open the breaker (`CRASH_LOOP`).
//! * **Stop ladder**: `desired=0` is persisted before the stop RPC
//!   (invariant 26); graceful `stop`, `BUSY_VIEW` → StopBlocked (retried every
//!   2 s), ForceStop → `stop{force}`, wait 10 s for the exit, then a verified
//!   kill. A worker that has not printed Ready is killed directly.
//! * **Shutdown** never changes `desired_running` and never kills a worker that
//!   may hold dirty sessions (invariant 8): busy or silent workers are
//!   `left_running` and adopted by the next daemon.
//! * **Liveness**: a dedicated client loops `watch`; no heartbeat for 8 s or 3
//!   failed watches → `hello` probe → on failure a verified kill and Backoff.
//! * **Gate and mount**: `mount-attach` only while the gate allows it
//!   (invariant 26); the gate marker is cleared only after `Attached`.
//! * **Published entry**: `published-summary` on Ready, on a
//!   `generations.branches` change and every 60 s while unavailable; tip and
//!   content counters follow the heartbeat without an RPC.
use crate::{
    model::{
        errors::{ErrorRecord, ErrorWire, Scope},
        health::{MountIntent, MountState, MountStatus},
        ipc::{
            self, FatalLine, HelloReply, IPC_VERSION, PublishedSummary, ReadyLine, StartupLine,
            WatchRequest, WorkerHeartbeat, WorkerRecord, deadlines, ops,
        },
        orchestrator::{
            CallError, CatalogApi, CatalogCmd, CatalogSnapshot, EntryState, ExitRecord, FolderCmd,
            FolderPhase, FolderRecord, FolderView, GateEvidence, GateRecord, GateState, LaunchSpec,
            PeerRecord, ProcessEvent, ProcessSink, PublishedCatalogApi, PublishedEntry,
            ShutdownOutcome, StatusSink, StopStage, WorkerClient, WorkerEndpoint, WorkerLauncher,
            WorkerProcess, limits, outcomes,
        },
        status::StatusEvent,
    },
    runtime::Discovery,
    startup::Watchdog,
    util::{
        backoff::Backoff, cancel::Cancel, lock::Tracked, log::event, notify::Notify,
        snapshot::Snapshot, time::Clock,
    },
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, VecDeque},
    fs,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock,
        mpsc::{self, Receiver, RecvTimeoutError, Sender},
    },
    thread,
    time::{Duration, Instant},
};

/// Lifecycle timing and limits of every FolderController (§3.3). The
/// defaults are the constants of `model::orchestrator::limits`.
#[derive(Clone, Debug, PartialEq)]
pub struct LifecycleLimits {
    /// The binary launched as `daemon --state S --supervised`.
    pub executable: PathBuf,
    /// Folders that may hold a worker at once (`WORKER_LIMIT` beyond it).
    pub maximum_running: usize,
    /// A spawned worker with no `startup_progress` for this long is killed.
    pub startup_silence: Duration,
    pub stop_graceful: Duration,
    pub stop_exit_wait: Duration,
    pub backoff_base: Duration,
    pub backoff_cap: Duration,
    pub backoff_reset_after: Duration,
    pub breaker_failures: u32,
    pub breaker_window: Duration,
    pub heartbeat_timeout: Duration,
    pub hello_timeout: Duration,
    pub adopt_patience: Duration,
    pub attach_deadline: Duration,
    pub mount_retry_base: Duration,
    pub mount_retry_cap: Duration,
    /// Shutdown kills workers that are still Starting (no RPC served yet).
    pub shutdown_kill_starting: bool,
    /// The worker version this orchestrator would spawn; an adopted worker
    /// whose `hello.version` differs is replaced. Without `+build` metadata
    /// only the part of `hello.version` before `+` is compared; empty accepts
    /// any version. (Added by WP1.2; see `docs/revamp/notes/wp1-2.md`.)
    pub worker_version: String,
}

impl LifecycleLimits {
    /// The defaults with a given executable and running limit.
    pub fn new(executable: PathBuf, maximum_running: usize) -> Self {
        Self {
            executable,
            maximum_running,
            ..Self::default()
        }
    }
}

impl Default for LifecycleLimits {
    fn default() -> Self {
        Self {
            executable: std::env::current_exe().unwrap_or_default(),
            maximum_running: 8,
            startup_silence: limits::STARTUP_SILENCE,
            stop_graceful: limits::STOP_GRACEFUL,
            stop_exit_wait: limits::STOP_EXIT_WAIT,
            backoff_base: limits::RESTART_BACKOFF_BASE,
            backoff_cap: limits::RESTART_BACKOFF_CAP,
            backoff_reset_after: limits::RESTART_BACKOFF_RESET_AFTER,
            breaker_failures: limits::BREAKER_FAILURES,
            breaker_window: limits::BREAKER_WINDOW,
            heartbeat_timeout: limits::HEARTBEAT_TIMEOUT,
            hello_timeout: limits::HELLO_TIMEOUT,
            adopt_patience: limits::ADOPT_PATIENCE,
            attach_deadline: limits::ATTACH_DEADLINE,
            mount_retry_base: limits::MOUNT_RETRY_BASE,
            mount_retry_cap: limits::MOUNT_RETRY_CAP,
            shutdown_kill_starting: true,
            // `stringify!`: the contracts code-literal scan would read a quoted
            // CARGO_PKG_VERSION as an error code (CCR-WP1.2-1).
            worker_version: env!(stringify!(CARGO_PKG_VERSION)).to_owned(),
        }
    }
}

/// The longest the controller sleeps without a message or a due timer.
const MAX_IDLE_WAIT: Duration = Duration::from_secs(2);
/// `owner.lock` is re-probed this often while a previous worker releases it.
const LOCK_POLL: Duration = Duration::from_millis(250);
/// Pause between two adoption `hello` attempts.
const ADOPT_HELLO_RETRY: Duration = Duration::from_secs(1);
/// A stop RPC that has not come back this long after its own deadline is
/// treated as timed out (a client that ignores its deadline).
const RPC_GRACE: Duration = Duration::from_secs(1);
/// Kill attempts before a never-exiting worker is given up on.
const KILL_ATTEMPTS: u32 = 2;
/// Watch loop pauses (interruptible): after a failure, and after a reply that
/// carried no new heartbeat.
const WATCH_RETRY_PAUSE: Duration = Duration::from_millis(500);
const WATCH_IDLE_PAUSE: Duration = Duration::from_millis(250);

// ---------------------------------------------------------------------------
// Shared with the registry

/// Everything a controller needs; one instance is shared by every controller
/// of a registry.
pub(crate) struct ControllerDeps {
    pub catalog: Arc<dyn CatalogApi>,
    pub published: Arc<dyn PublishedCatalogApi>,
    pub status: Arc<dyn StatusSink>,
    pub launcher: Arc<dyn WorkerLauncher>,
    pub clock: Arc<dyn Clock>,
    pub limits: LifecycleLimits,
    pub slots: Arc<Slots>,
    /// Poked on every published view (the registry's aggregate generation).
    pub changes: Arc<Notify>,
}

/// The reply channel of `FolderDirectory::request`.
pub(crate) type Reply = Sender<Result<Arc<FolderView>, ErrorWire>>;

/// The controller mailbox (§3.3).
pub(crate) enum Msg {
    Cmd(FolderCmd, Option<Reply>),
    /// Pushed by the launcher sink of worker `epoch`.
    Process(u64, ProcessEvent),
    /// Pushed by the watch thread of worker `epoch`.
    Heartbeat(u64, Arc<WorkerHeartbeat>),
    WatchFailed(u64, CallError),
    /// The result of one worker RPC (the mount attach result among them).
    Rpc(u64, RpcKind, Result<Value, CallError>),
    /// The result of a catalog write the controller issued on a helper
    /// thread (it never waits for the catalog itself).
    Catalog(CatalogWrite, Result<Arc<CatalogSnapshot>, ErrorWire>),
    /// Stop for daemon shutdown by the (real-time) deadline and report once.
    Shutdown(Instant, Sender<ShutdownOutcome>),
    /// The folder left the catalog.
    Retire,
}

/// A catalog write the controller runs on a helper thread
/// ([`Controller::catalog_write`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CatalogWrite {
    ClearGate,
    /// Retarget the gate to this installation.
    RetargetGate(String),
}

/// What the registry keeps per folder.
pub(crate) struct ControllerHandle {
    pub tx: Sender<Msg>,
    pub view: Arc<Snapshot<FolderView>>,
    pub client: Arc<Mutex<Option<Arc<dyn WorkerClient>>>>,
}

type Waker = Box<dyn Fn(Vec<String>) + Send + Sync>;

/// `maximum_running` accounting. A folder holds a slot from the moment it
/// starts (or adopts) a worker until it is Stopped or Failed with no process.
pub(crate) struct Slots {
    maximum: usize,
    state: Tracked<SlotState>,
    waker: OnceLock<Waker>,
}

#[derive(Default)]
struct SlotState {
    held: BTreeSet<String>,
    waiting: BTreeSet<String>,
}

impl Slots {
    pub fn new(maximum: usize) -> Self {
        Self {
            maximum,
            state: Tracked::new("folder-slots", SlotState::default()),
            waker: OnceLock::new(),
        }
    }

    /// Called with the folders that waited for a slot whenever one frees.
    pub fn set_waker(&self, waker: Waker) {
        let _ = self.waker.set(waker);
    }

    /// Takes a slot for `state`; a refused folder is remembered as waiting.
    pub fn try_acquire(&self, state: &str) -> bool {
        let mut slots = self.state.lock();
        if slots.held.contains(state) {
            return true;
        }
        if slots.held.len() < self.maximum {
            slots.held.insert(state.to_owned());
            slots.waiting.remove(state);
            return true;
        }
        slots.waiting.insert(state.to_owned());
        false
    }

    /// Takes a slot for `state` even beyond `maximum`: an adopted worker is
    /// already running and already uses its resources, so it is never left
    /// unsupervised for want of a slot (§2.3 Daemon row; post-integration
    /// re-review, WP2.INT). Later starts wait until the count is below the
    /// maximum again.
    pub fn force_acquire(&self, state: &str) {
        let mut slots = self.state.lock();
        slots.held.insert(state.to_owned());
        slots.waiting.remove(state);
    }

    /// Whether a start of `state` would get a slot right now.
    pub fn admits(&self, state: &str) -> bool {
        let slots = self.state.lock();
        slots.held.contains(state) || slots.held.len() < self.maximum
    }

    pub fn release(&self, state: &str) {
        let waiting = {
            let mut slots = self.state.lock();
            if !slots.held.remove(state) || slots.waiting.is_empty() {
                return;
            }
            std::mem::take(&mut slots.waiting)
                .into_iter()
                .collect::<Vec<_>>()
        };
        if let Some(waker) = self.waker.get() {
            waker(waiting);
        }
    }

    pub fn cancel_wait(&self, state: &str) {
        self.state.lock().waiting.remove(state);
    }

    pub fn held(&self) -> usize {
        self.state.lock().held.len()
    }
}

/// The view a folder shows before its controller has run.
pub(crate) fn initial_view(record: &FolderRecord) -> FolderView {
    FolderView {
        record: record.clone(),
        mount: idle_mount(record),
        ..FolderView::default()
    }
}

/// Starts the controller thread of `record.state`.
pub(crate) fn spawn(
    record: FolderRecord,
    deps: Arc<ControllerDeps>,
) -> std::io::Result<ControllerHandle> {
    let (tx, rx) = mpsc::channel();
    let view = Arc::new(Snapshot::new(initial_view(&record)));
    let client = Arc::new(Mutex::new(None));
    let state = record.state.clone();
    let controller = Controller::new(record, deps, tx.clone(), view.clone(), client.clone());
    thread::Builder::new()
        .name(format!("folder-{state}"))
        .spawn(move || controller.run(rx))?;
    Ok(ControllerHandle { tx, view, client })
}

// ---------------------------------------------------------------------------
// Controller state

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RpcKind {
    AdoptHello,
    Probe,
    Stop,
    Attach,
    Detach,
    Published,
    Revalidate,
    LegacyStatus,
    LegacyStop,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Settle {
    Now,
    /// The folder is no longer Stopping (Stopped, StopBlocked, Starting, …).
    Stop,
    /// No mount attach or detach is in flight.
    Mount,
    Revalidate,
}

struct Waiter {
    settle: Settle,
    reply: Reply,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LegacyStage {
    Status,
    Stop,
}

enum StartPlan {
    /// The previous worker of this state was adopted; `hello` decides.
    Adopt {
        since: Instant,
        next_hello: Option<Instant>,
        rpc: Option<u64>,
    },
    /// A legacy orphan (`runtime.json` without `worker.json`) is probed and stopped.
    Legacy {
        client: Arc<dyn WorkerClient>,
        stage: LegacyStage,
        rpc: Option<u64>,
    },
    /// Waiting for `owner.lock` to be free before spawning (invariant 23).
    /// `warned`: the wait passed `limits::ADOPT_LOCK_WAIT` and its error is
    /// shown (a lock wait never counts as a worker failure).
    WaitLock {
        since: Instant,
        next_poll: Instant,
        warned: bool,
    },
    /// A spawned worker that has not printed Ready.
    Spawned {
        watchdog: Watchdog,
        last_progress: Instant,
        progress_seq: u64,
    },
}

#[derive(Clone, Debug)]
enum AfterStop {
    Stopped,
    Start,
    /// A failure (counted toward the breaker) once the worker is gone.
    Backoff(ErrorWire),
    Fail(ErrorWire),
    Retire,
}

struct Blocked {
    since_ms: u64,
    reason: ErrorWire,
}

struct StopPlan {
    force: bool,
    stage: StopStage,
    since_ms: u64,
    deadline: Option<Instant>,
    deadline_ms: u64,
    after: AfterStop,
    rpc: Option<u64>,
    blocked: Option<Blocked>,
    retry_at: Option<Instant>,
    kills: u32,
    /// The stop was sent to an adopted worker that never answered `hello`
    /// (since this instant): a failed graceful stop is retried, never
    /// followed by a probe and a kill, and after `adopt_patience` the folder
    /// is `Failed{PREVIOUS_WORKER_UNRESPONSIVE}` like a silent adoption.
    unverified_since: Option<Instant>,
}

struct BackoffState {
    until: Instant,
    until_ms: u64,
    attempt: u32,
    last_exit: ExitRecord,
}

struct RunningState {
    since_ms: u64,
    adopted: bool,
}

struct Worker {
    epoch: u64,
    process: Box<dyn WorkerProcess>,
    record: Option<WorkerRecord>,
    endpoint: Option<WorkerEndpoint>,
    client: Option<Arc<dyn WorkerClient>>,
    watch: Option<Cancel>,
    fatal: Option<FatalLine>,
    killed: bool,
}

struct Liveness {
    last_heartbeat: Instant,
    watch_failures: u32,
    probe: Option<u64>,
}

struct MountCtl {
    attach: Option<u64>,
    detach: Option<u64>,
    /// A NeedsUserAction failure: no retry until Attach or a mount change.
    hold: bool,
    /// The user detached: no automatic attach until Attach.
    user_detached: bool,
    retry: Backoff,
    retry_at: Option<Instant>,
    attempt: u32,
    /// Whether the worker's mount state is known (Ready line or heartbeat);
    /// an adopted worker may already be attached.
    known: bool,
    /// The next heartbeat may predate the last mount-attach/-detach result:
    /// its watch could already be in flight when the result arrived. The
    /// watch loop is sequential and the mailbox FIFO, so the heartbeat after
    /// it was requested after the result; its mount state is merged again.
    stale_heartbeat: bool,
}

impl MountCtl {
    fn new(limits: &LifecycleLimits) -> Self {
        Self {
            attach: None,
            detach: None,
            hold: false,
            user_detached: false,
            retry: Backoff::new(
                limits.mount_retry_base,
                limits.mount_retry_cap,
                limits::BACKOFF_JITTER,
                Duration::ZERO,
            ),
            retry_at: None,
            attempt: 0,
            known: false,
            stale_heartbeat: false,
        }
    }

    fn rearm(&mut self) {
        self.hold = false;
        self.retry_at = None;
        self.retry.reset();
        self.attempt = 0;
    }
}

#[derive(Default)]
struct GateCtl {
    /// Paired installations that sent evidence for this folder (they share it).
    evidence_from: BTreeSet<String>,
    /// The source peer is gone; the next paired evidence source becomes it.
    retarget_pending: bool,
    clear_retry_at: Option<Instant>,
    /// A `ClearGate` / `RetargetGate` write is in flight (helper thread).
    clearing: bool,
    retargeting: bool,
}

#[derive(Default)]
struct PublishedCtl {
    entry: Option<PublishedEntry>,
    rpc: Option<u64>,
    retry_at: Option<Instant>,
    refresh: bool,
    branches_gen: Option<u64>,
    requested_gen: Option<u64>,
}

struct ShutdownCtl {
    deadline: Instant,
    reply: Sender<ShutdownOutcome>,
    replied: bool,
}

struct Controller {
    state: String,
    deps: Arc<ControllerDeps>,
    tx: Sender<Msg>,
    view: Arc<Snapshot<FolderView>>,
    client_slot: Arc<Mutex<Option<Arc<dyn WorkerClient>>>>,

    record: FolderRecord,
    gate_record: Option<GateRecord>,
    peers: Vec<PeerRecord>,
    desired: bool,

    failed: Option<ErrorRecord>,
    stop: Option<StopPlan>,
    start: Option<StartPlan>,
    start_since_ms: u64,
    backoff_state: Option<BackoffState>,
    running: Option<RunningState>,

    worker: Option<Worker>,
    epoch: u64,
    ticket: u64,

    gate: GateState,
    gate_ctl: GateCtl,
    mount: MountStatus,
    mount_ctl: MountCtl,
    heartbeat: Option<Arc<WorkerHeartbeat>>,
    liveness: Liveness,
    published: PublishedCtl,
    revalidate: Option<u64>,

    last_error: Option<ErrorRecord>,
    restarts: u32,
    degraded: bool,
    published_error: Option<ErrorRecord>,
    backoff: Backoff,
    failures: VecDeque<Instant>,

    /// The instance of an adopted worker this controller replaced (version
    /// mismatch); its stale `worker.json` is never adopted again.
    replaced: Option<String>,
    /// The `worker.json` instance last probed without an adoption (dead, not
    /// the recorded image, or another identity): a lock wait re-probes the
    /// file only when it names another instance.
    probed: Option<String>,

    waiters: Vec<Waiter>,
    shutdown: Option<ShutdownCtl>,
    slot_held: bool,
    waiting_slot: bool,
    retired: bool,
    done: bool,
    last_lifecycle: Option<FolderView>,
}

fn ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn stopping_error() -> ErrorWire {
    ErrorWire::new(
        "ORCHESTRATOR_STOPPING",
        "ORCHESTRATOR_STOPPING: the folder controller is shutting down",
    )
}

fn idle_mount(record: &FolderRecord) -> MountStatus {
    let wanted = record.mount_enabled && record.mount.is_some();
    MountStatus {
        intent: if wanted {
            MountIntent::Attach
        } else {
            MountIntent::Headless
        },
        path: record.mount.clone(),
        state: if wanted {
            MountState::Detached
        } else {
            MountState::Disabled
        },
    }
}

fn parse<T: DeserializeOwned>(result: Result<Value, CallError>) -> Result<T, CallError> {
    result.and_then(|value| {
        serde_json::from_value(value).map_err(|error| {
            CallError::Remote(ErrorWire::new(
                "INVALID_REPLY",
                format!("INVALID_REPLY: {error}"),
            ))
        })
    })
}

fn read_worker_record(directory: &Path) -> Option<WorkerRecord> {
    let bytes = fs::read(directory.join(ipc::WORKER_RECORD_FILE)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn read_discovery(directory: &Path) -> Option<Discovery> {
    let bytes = fs::read(directory.join("runtime.json")).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Whether no live process holds `STATE/owner.lock` (invariant 23). A missing
/// state directory counts as free: the worker reports that itself.
fn owner_lock_free(directory: &Path) -> bool {
    if !directory.is_dir() {
        return true;
    }
    crate::orchestrator::lock_state(directory).is_ok()
}

/// The error an unexpected worker exit stands for. `adopted`: the worker is
/// not our child, so a missing exit code means "unknown", not a signal.
fn classify_exit(exit: &ExitRecord, adopted: bool, log: &Path) -> ErrorWire {
    if let Some(fatal) = &exit.fatal {
        return fatal.to_wire();
    }
    match exit.code {
        Some(ipc::EXIT_LOCKED) => ErrorWire::new(
            "ALREADY_RUNNING",
            format!(
                "ALREADY_RUNNING: owner.lock is held by another process (exit {}); see {}",
                ipc::EXIT_LOCKED,
                log.display()
            ),
        ),
        Some(code) => ErrorWire::new(
            "WORKER_EXITED",
            format!("WORKER_EXITED: exit code {code}; see {}", log.display()),
        ),
        None if adopted => ErrorWire::new(
            "WORKER_EXITED",
            format!(
                "WORKER_EXITED: the adopted worker exited (its exit code cannot be read); see {}",
                log.display()
            ),
        ),
        None => ErrorWire::new(
            "WORKER_EXITED",
            format!("WORKER_EXITED: terminated; see {}", log.display()),
        ),
    }
}

/// Whether a worker that exited during a stop that would restart it crashed
/// (§3.3 "Stopping → Backoff (crashed while stopping)"). A kill of ours is
/// never a crash, and a known exit code is the answer: 0 is the clean end of
/// the stop. Without a code:
///
/// * an adopted worker is not our child, so the launcher cannot read its exit
///   code (WP1.3 reports `None` from its pid probe on Unix). Its exit is the
///   stop it was asked for (acknowledged, in flight, or lost with the
///   connection), unless it had refused that stop (StopBlocked) and no retry
///   was in flight;
/// * a spawned worker was terminated by a signal: a crash.
fn crashed_while_stopping(plan: &StopPlan, killed: bool, adopted: bool, code: Option<i32>) -> bool {
    if killed {
        return false;
    }
    match code {
        Some(code) => code != ipc::EXIT_OK,
        None if adopted => plan.blocked.is_some() && plan.rpc.is_none(),
        None => true,
    }
}

impl Controller {
    fn new(
        record: FolderRecord,
        deps: Arc<ControllerDeps>,
        tx: Sender<Msg>,
        view: Arc<Snapshot<FolderView>>,
        client_slot: Arc<Mutex<Option<Arc<dyn WorkerClient>>>>,
    ) -> Self {
        let now = deps.clock.instant();
        let limits = &deps.limits;
        let backoff = Backoff::new(
            limits.backoff_base,
            limits.backoff_cap,
            limits::BACKOFF_JITTER,
            limits.backoff_reset_after,
        );
        let mount_ctl = MountCtl::new(limits);
        Self {
            state: record.state.clone(),
            tx,
            view,
            client_slot,
            desired: record.desired_running,
            mount: idle_mount(&record),
            record,
            gate_record: None,
            peers: Vec::new(),
            failed: None,
            stop: None,
            start: None,
            start_since_ms: 0,
            backoff_state: None,
            running: None,
            worker: None,
            epoch: 0,
            ticket: 0,
            gate: GateState::None,
            gate_ctl: GateCtl::default(),
            mount_ctl,
            heartbeat: None,
            liveness: Liveness {
                last_heartbeat: now,
                watch_failures: 0,
                probe: None,
            },
            published: PublishedCtl::default(),
            revalidate: None,
            last_error: None,
            restarts: 0,
            degraded: false,
            published_error: None,
            backoff,
            failures: VecDeque::new(),
            replaced: None,
            probed: None,
            waiters: Vec::new(),
            shutdown: None,
            slot_held: false,
            waiting_slot: false,
            retired: false,
            done: false,
            last_lifecycle: None,
            deps,
        }
    }

    fn run(self, rx: Receiver<Msg>) {
        let state = self.state.clone();
        let deps = self.deps.clone();
        let view = self.view.clone();
        let outcome = catch_unwind(AssertUnwindSafe(move || {
            let mut controller = self;
            controller.run_loop(rx);
        }));
        if outcome.is_err() {
            event!(error, UNKNOWN_ERROR, {
                "folder": state,
                "detail": "the folder controller panicked",
            });
            let error = ErrorWire::new(
                "UNKNOWN_ERROR",
                "UNKNOWN_ERROR: the folder controller panicked; see the daemon log",
            );
            let at_ms = deps.clock.now_ms();
            view.update(|current| {
                let mut next = current.clone();
                next.phase = FolderPhase::Failed {
                    error: ErrorRecord::new(
                        error,
                        Scope::Folder {
                            state: state.clone(),
                        },
                        at_ms,
                    ),
                };
                next.generation = current.generation + 1;
                next
            });
            deps.slots.release(&state);
            deps.changes.poke();
        }
    }

    fn run_loop(&mut self, rx: Receiver<Msg>) {
        self.init();
        self.after_step();
        while !self.done {
            let wait = self.next_wait();
            match rx.recv_timeout(wait) {
                Ok(message) => self.handle(message),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
            if !self.done {
                self.on_timers();
            }
            self.end_if_retired();
            self.after_step();
        }
        self.finish();
    }

    // -- clock and small helpers --------------------------------------------

    fn now(&self) -> Instant {
        self.deps.clock.instant()
    }

    fn now_ms(&self) -> u64 {
        self.deps.clock.now_ms()
    }

    fn limits(&self) -> &LifecycleLimits {
        &self.deps.limits
    }

    fn scope(&self) -> Scope {
        Scope::Folder {
            state: self.state.clone(),
        }
    }

    fn worker_log(&self) -> PathBuf {
        self.record.directory.join("worker.log")
    }

    fn next_epoch(&mut self) -> u64 {
        self.epoch += 1;
        self.epoch
    }

    fn sink(&self, epoch: u64) -> ProcessSink {
        let tx = self.tx.clone();
        Arc::new(move |event| {
            let _ = tx.send(Msg::Process(epoch, event));
        })
    }

    fn worker_client(&self) -> Option<Arc<dyn WorkerClient>> {
        self.worker
            .as_ref()
            .and_then(|worker| worker.client.clone())
    }

    fn current_epoch(&self, epoch: u64) -> bool {
        self.worker
            .as_ref()
            .is_some_and(|worker| worker.epoch == epoch)
    }

    fn idle(&self) -> bool {
        self.worker.is_none()
            && self.start.is_none()
            && self.stop.is_none()
            && self.backoff_state.is_none()
            && self.running.is_none()
    }

    fn observe_error(&mut self, error: ErrorWire) {
        let now_ms = self.now_ms();
        match self.last_error.as_mut() {
            Some(record) => record.observe(error, now_ms),
            None => self.last_error = Some(ErrorRecord::new(error, self.scope(), now_ms)),
        }
    }

    fn resolve_error(&mut self) {
        let now_ms = self.now_ms();
        if let Some(record) = self.last_error.as_mut() {
            record.resolve(now_ms);
        }
    }

    /// Runs one worker RPC on a helper thread; the result comes back as
    /// `Msg::Rpc(ticket, kind, ..)`.
    fn rpc(
        &mut self,
        kind: RpcKind,
        client: Arc<dyn WorkerClient>,
        op: &'static str,
        payload: Value,
        deadline: Duration,
    ) -> u64 {
        self.ticket += 1;
        let ticket = self.ticket;
        let tx = self.tx.clone();
        let spawned = thread::Builder::new()
            .name(format!("folder-{}-{op}", self.state))
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| client.call(op, payload, deadline)))
                    .unwrap_or_else(|_| {
                        Err(CallError::ConnectionLost(format!(
                            "{op}: the worker client panicked"
                        )))
                    });
                let _ = tx.send(Msg::Rpc(ticket, kind, result));
            });
        if let Err(error) = spawned {
            let _ = self.tx.send(Msg::Rpc(
                ticket,
                kind,
                Err(CallError::Unavailable(format!(
                    "LOCAL_WORKER_UNAVAILABLE: cannot start an RPC thread: {error}"
                ))),
            ));
        }
        ticket
    }

    /// A deadline capped by the remaining shutdown budget.
    fn shutdown_cap(&self, deadline: Duration) -> Duration {
        match &self.shutdown {
            Some(shutdown) => deadline
                .min(shutdown.deadline.saturating_duration_since(Instant::now()))
                .max(Duration::from_millis(1)),
            None => deadline,
        }
    }

    // -- startup and the main loop ------------------------------------------

    fn init(&mut self) {
        let snapshot = self.deps.catalog.snapshot();
        if let Some(record) = snapshot.folder(&self.state) {
            self.record = record.clone();
            self.desired = record.desired_running;
        }
        self.peers = snapshot.peers.clone();
        self.sync_gate(snapshot.gates.get(&self.state).cloned());
        self.check_gate_source();
        self.mount = idle_mount(&self.record);
        if self.desired {
            let _ = self.begin_start();
        } else {
            self.adopt_to_stop();
        }
    }

    /// `desired=0` at controller start: a live, verified previous worker
    /// (a stop that was StopBlocked when the daemon ended, a Shutdown that
    /// left it running, a daemon crash) is adopted all the same, and the
    /// stop the user asked for is driven again once it answers `hello`
    /// (graceful stop, StopBlocked with retries, ForceStop available).
    /// Otherwise it would run unsupervised behind a `Stopped` folder (§2.3
    /// Daemon row, §3.3; post-integration re-review, WP2.INT).
    fn adopt_to_stop(&mut self) {
        if let Some((epoch, record, process)) = self.probe_previous_worker() {
            self.take_slot_for_adoption();
            self.start_since_ms = self.now_ms();
            self.adopt_previous(epoch, record, process);
        }
    }

    /// Reads `STATE/worker.json` and asks the launcher whether the recorded
    /// worker is alive and verified (pid, start time, image). `None` when
    /// there is no such record for this folder, or its process is gone or
    /// not the recorded one (never killed: a foreign process may own the pid
    /// now); such a record is remembered in `probed`.
    fn probe_previous_worker(&mut self) -> Option<(u64, WorkerRecord, Box<dyn WorkerProcess>)> {
        let record = read_worker_record(&self.record.directory)?;
        let ours = record.repo == self.record.repo && record.device == self.record.device;
        if !ours || self.replaced.as_deref() == Some(record.instance.as_str()) {
            return None;
        }
        let epoch = self.next_epoch();
        match self.deps.launcher.adopt(&record, self.sink(epoch)) {
            Ok(Some(process)) => return Some((epoch, record, process)),
            // Dead, or not the recorded image.
            Ok(None) => {}
            Err(error) => {
                event!(warn, (error.code.as_str()), {
                    "folder": self.state,
                    "detail": "adoption probe failed",
                    "message": error.message,
                });
            }
        }
        self.probed = Some(record.instance);
        None
    }

    /// Holds a slot for an adopted worker, beyond `maximum_running` if need
    /// be ([`Slots::force_acquire`]).
    fn take_slot_for_adoption(&mut self) {
        self.waiting_slot = false;
        if !self.slot_held {
            self.deps.slots.force_acquire(&self.state);
            self.slot_held = true;
        }
    }

    /// Becomes the controller of a probed previous worker; `hello` decides.
    fn adopt_previous(
        &mut self,
        epoch: u64,
        record: WorkerRecord,
        process: Box<dyn WorkerProcess>,
    ) {
        let endpoint = WorkerEndpoint {
            instance: record.instance.clone(),
            pid: record.pid,
            address: record.address.clone(),
            version: record.version.clone(),
            token: record.token.clone(),
        };
        let client = self.deps.launcher.client(&endpoint);
        self.worker = Some(Worker {
            epoch,
            process,
            record: Some(record),
            endpoint: Some(endpoint),
            client: Some(client),
            watch: None,
            fatal: None,
            killed: false,
        });
        self.start = Some(StartPlan::Adopt {
            since: self.now(),
            next_hello: None,
            rpc: None,
        });
        self.adopt_hello();
    }

    fn handle(&mut self, message: Msg) {
        match message {
            Msg::Cmd(command, reply) => {
                let result = self.on_cmd(command);
                match (result, reply) {
                    (Ok(settle), Some(reply)) => self.waiters.push(Waiter { settle, reply }),
                    (Err(error), Some(reply)) => {
                        let _ = reply.send(Err(error));
                    }
                    (_, None) => {}
                }
            }
            Msg::Process(epoch, event) => self.on_process(epoch, event),
            Msg::Heartbeat(epoch, heartbeat) => self.on_heartbeat(epoch, heartbeat),
            Msg::WatchFailed(epoch, error) => self.on_watch_failed(epoch, error),
            Msg::Rpc(ticket, kind, result) => self.on_rpc(ticket, kind, result),
            Msg::Catalog(kind, result) => self.on_catalog_write(kind, result),
            Msg::Shutdown(deadline, reply) => self.on_shutdown(deadline, reply),
            Msg::Retire => self.on_retire(),
        }
    }

    /// The time until the earliest pending timer (never longer than
    /// `MAX_IDLE_WAIT`). Timers are consumed when due, so a past instant never
    /// makes the loop spin.
    fn next_wait(&self) -> Duration {
        let now = self.now();
        let mut next: Option<Instant> = None;
        let mut consider = |at: Option<Instant>| {
            if let Some(at) = at {
                next = Some(next.map_or(at, |current| current.min(at)));
            }
        };
        match &self.start {
            Some(StartPlan::Adopt { next_hello, .. }) => consider(*next_hello),
            Some(StartPlan::WaitLock { next_poll, .. }) => consider(Some(*next_poll)),
            Some(StartPlan::Spawned { last_progress, .. }) => {
                consider(last_progress.checked_add(self.limits().startup_silence))
            }
            _ => {}
        }
        consider(self.backoff_state.as_ref().map(|backoff| backoff.until));
        if let Some(plan) = &self.stop {
            consider(plan.deadline);
            if plan.rpc.is_none() {
                consider(plan.retry_at);
            }
        }
        if self.liveness_active() {
            consider(
                self.liveness
                    .last_heartbeat
                    .checked_add(self.limits().heartbeat_timeout),
            );
        }
        consider(self.mount_ctl.retry_at);
        consider(self.published.retry_at);
        consider(self.gate_ctl.clear_retry_at);
        let mut wait = next
            .map_or(MAX_IDLE_WAIT, |at| at.saturating_duration_since(now))
            .min(MAX_IDLE_WAIT);
        if let Some(shutdown) = &self.shutdown {
            wait = wait.min(shutdown.deadline.saturating_duration_since(Instant::now()));
        }
        wait
    }

    fn on_timers(&mut self) {
        let now = self.now();
        if self
            .shutdown
            .as_ref()
            .is_some_and(|shutdown| Instant::now() >= shutdown.deadline)
        {
            self.shutdown_deadline_passed();
            return;
        }
        let mut hello = false;
        let mut poll = false;
        let mut expired = false;
        match &self.start {
            Some(StartPlan::Adopt {
                next_hello: Some(at),
                ..
            }) => hello = now >= *at,
            Some(StartPlan::WaitLock { next_poll, .. }) => poll = now >= *next_poll,
            Some(StartPlan::Spawned { watchdog, .. }) => {
                expired = self.watchdog_expired(watchdog, now)
            }
            _ => {}
        }
        if hello {
            self.adopt_hello();
        }
        if poll {
            self.poll_lock();
        }
        if expired {
            self.startup_timeout();
        }
        if self
            .backoff_state
            .as_ref()
            .is_some_and(|backoff| now >= backoff.until)
        {
            self.backoff_state = None;
            if self.desired && self.shutdown.is_none() {
                self.restarts = self.restarts.saturating_add(1);
                let _ = self.begin_start();
            }
        }
        let mut retry_stop = None;
        let mut stop_deadline = false;
        if let Some(plan) = self.stop.as_mut() {
            if plan.retry_at.is_some_and(|at| now >= at) {
                plan.retry_at = None;
                if plan.rpc.is_none() {
                    retry_stop = Some(plan.force);
                }
            } else if plan.deadline.is_some_and(|at| now >= at) {
                plan.deadline = None;
                stop_deadline = true;
            }
        }
        if let Some(force) = retry_stop {
            self.send_stop(force, AfterStop::Stopped);
        }
        if stop_deadline {
            self.stop_deadline_passed();
        }
        if self.liveness_active()
            && now.saturating_duration_since(self.liveness.last_heartbeat)
                >= self.limits().heartbeat_timeout
        {
            self.probe();
        }
        if self.mount_ctl.retry_at.is_some_and(|at| now >= at) {
            self.mount_ctl.retry_at = None;
        }
        if self.published.retry_at.is_some_and(|at| now >= at) {
            self.published.retry_at = None;
        }
        if self.gate_ctl.clear_retry_at.is_some_and(|at| now >= at) {
            self.gate_ctl.clear_retry_at = None;
        }
    }

    /// Re-evaluates the conditions that do not depend on one message: slot,
    /// mount attach, gate marker, published entry; then publishes and settles
    /// waiters.
    fn after_step(&mut self) {
        if !self.done {
            self.sync_slot();
            if self.gate.allows_attach() {
                // Satisfied, Overridden or cleared: the block is over.
                self.resolve_gate_blocked();
            }
            if self.want_attach() {
                self.try_attach();
            }
            // Invariant 26: the gate marker goes only after `Attached`, or,
            // for a folder with no mount to attach, once the gate is
            // satisfied or overridden (the legacy rule: a headless received
            // folder is complete then; a mount configured later attaches at
            // once). Otherwise a headless folder kept its marker for good:
            // `initial_sync_pending` forever, and after a restart with the
            // source unreachable an Awaiting gate that showed it "starting"
            // (post-integration re-review, WP2.INT).
            let complete = self.mount.state.is_attached()
                || (!self.mount_requested()
                    && matches!(
                        self.gate,
                        GateState::Satisfied { .. } | GateState::Overridden { .. }
                    ));
            if complete
                && self.gate_record.is_some()
                && !self.gate_ctl.clearing
                && self.gate_ctl.clear_retry_at.is_none()
                && self.running.is_some()
            {
                self.clear_gate();
            }
            self.maybe_refresh_published();
        }
        self.publish();
        self.settle_waiters();
    }

    fn finish(&mut self) {
        if let Some(cancel) = self
            .worker
            .as_ref()
            .and_then(|worker| worker.watch.as_ref())
        {
            cancel.cancel();
        }
        *crate::util::lock::lock(&self.client_slot) = None;
        let error = if self.retired {
            ErrorWire::new(
                "FOLDER_NOT_FOUND",
                format!("FOLDER_NOT_FOUND: {} left the catalog", self.state),
            )
        } else {
            stopping_error()
        };
        for waiter in self.waiters.drain(..) {
            let _ = waiter.reply.send(Err(error.clone()));
        }
        self.deps.slots.cancel_wait(&self.state);
        if self.slot_held {
            self.slot_held = false;
            self.deps.slots.release(&self.state);
        }
        self.publish();
    }

    // -- view ----------------------------------------------------------------

    fn phase(&self) -> FolderPhase {
        if let Some(error) = &self.failed {
            return FolderPhase::Failed {
                error: error.clone(),
            };
        }
        if let Some(plan) = &self.stop {
            if let Some(blocked) = &plan.blocked {
                let heartbeat = self.heartbeat.as_deref();
                return FolderPhase::StopBlocked {
                    since_ms: blocked.since_ms,
                    reason: blocked.reason.clone(),
                    open_handles: heartbeat.map_or(0, |hb| hb.open_handles),
                    unsaved: heartbeat.map_or(0, |hb| hb.unflushed_files),
                };
            }
            return FolderPhase::Stopping {
                since_ms: plan.since_ms,
                stage: plan.stage,
                deadline_ms: plan.deadline_ms,
                force: plan.force,
            };
        }
        if let Some(start) = &self.start {
            let (phase, progress_seq, adopting) = match start {
                StartPlan::Adopt { .. } => ("adopting the running worker".to_owned(), 0, true),
                StartPlan::Legacy { .. } => ("stopping the previous worker".to_owned(), 0, true),
                StartPlan::WaitLock { .. } => (
                    "waiting for the previous worker to release owner.lock".to_owned(),
                    0,
                    false,
                ),
                StartPlan::Spawned {
                    watchdog,
                    progress_seq,
                    ..
                } => (watchdog.phase.clone(), *progress_seq, false),
            };
            return FolderPhase::Starting {
                since_ms: self.start_since_ms,
                attempt: self.backoff.attempt().saturating_add(1),
                phase,
                progress_seq,
                adopting,
            };
        }
        if let Some(backoff) = &self.backoff_state {
            return FolderPhase::Backoff {
                until_ms: backoff.until_ms,
                attempt: backoff.attempt,
                last_exit: backoff.last_exit.clone(),
            };
        }
        if let (Some(running), Some(endpoint)) = (
            &self.running,
            self.worker.as_ref().and_then(|w| w.endpoint.as_ref()),
        ) {
            return FolderPhase::Running {
                since_ms: running.since_ms,
                instance: endpoint.instance.clone(),
                pid: endpoint.pid,
                address: endpoint.address.clone(),
                adopted: running.adopted,
            };
        }
        FolderPhase::Stopped
    }

    fn build_view(&self) -> FolderView {
        let mut record = self.record.clone();
        record.desired_running = self.desired;
        let worker = if self.running.is_some() {
            self.worker.as_ref().and_then(|w| w.endpoint.clone())
        } else {
            None
        };
        FolderView {
            generation: 0,
            record,
            phase: self.phase(),
            gate: self.gate.clone(),
            mount: self.mount.clone(),
            worker,
            heartbeat: self.heartbeat.clone(),
            last_error: self.last_error.clone(),
            restarts: self.restarts,
            degraded: self.degraded,
            published_error: self.published_error.clone(),
        }
    }

    fn publish(&mut self) {
        let mut view = self.build_view();
        let (current, generation) = self.view.load_versioned();
        view.generation = current.generation;
        if *current == view {
            return;
        }
        view.generation = generation + 1;
        let view = Arc::new(view);
        self.view.store_arc(view.clone());
        self.deps.changes.poke();
        // After `FolderRemoved` the status hub hears nothing more of this
        // folder: a later FolderLifecycle would register it again (WP1.11),
        // leaving a ghost entry while the retired controller stops its worker.
        // The local snapshot above still follows every change.
        if self.retired {
            return;
        }
        let mut key = (*view).clone();
        key.generation = 0;
        key.heartbeat = None;
        if self.last_lifecycle.as_ref() != Some(&key) {
            self.last_lifecycle = Some(key);
            self.deps
                .status
                .record(StatusEvent::FolderLifecycle(Box::new((*view).clone())));
        }
    }

    fn settled(&self, settle: Settle) -> bool {
        match settle {
            Settle::Now => true,
            Settle::Stop => self.stop.as_ref().is_none_or(|plan| plan.blocked.is_some()),
            Settle::Mount => self.mount_ctl.attach.is_none() && self.mount_ctl.detach.is_none(),
            Settle::Revalidate => self.revalidate.is_none(),
        }
    }

    fn settle_waiters(&mut self) {
        if self.waiters.is_empty() {
            return;
        }
        let view = self.view.load();
        let waiters = std::mem::take(&mut self.waiters);
        for waiter in waiters {
            if self.settled(waiter.settle) {
                let _ = waiter.reply.send(Ok(view.clone()));
            } else {
                self.waiters.push(waiter);
            }
        }
    }

    fn fail_waiters(&mut self, settle: Settle, error: &ErrorWire) {
        let waiters = std::mem::take(&mut self.waiters);
        for waiter in waiters {
            if waiter.settle == settle {
                let _ = waiter.reply.send(Err(error.clone()));
            } else {
                self.waiters.push(waiter);
            }
        }
    }

    // -- slots ---------------------------------------------------------------

    fn acquire_slot(&mut self) -> bool {
        if self.slot_held {
            return true;
        }
        self.slot_held = self.deps.slots.try_acquire(&self.state);
        self.slot_held
    }

    fn sync_slot(&mut self) {
        if self.slot_held && self.idle() {
            self.slot_held = false;
            self.deps.slots.release(&self.state);
        }
    }

    // -- commands ------------------------------------------------------------

    fn on_cmd(&mut self, command: FolderCmd) -> Result<Settle, ErrorWire> {
        if self.shutdown.is_some() || self.done {
            return Err(stopping_error());
        }
        if self.retired {
            return Err(ErrorWire::new(
                "FOLDER_NOT_FOUND",
                format!("FOLDER_NOT_FOUND: {} left the catalog", self.state),
            ));
        }
        match command {
            FolderCmd::Start => self.cmd_start(),
            FolderCmd::Stop { mode } => self.cmd_stop(mode == ipc::StopMode::Force),
            FolderCmd::ForceStop => self.cmd_stop(true),
            FolderCmd::Restart => self.cmd_restart(),
            FolderCmd::Attach => Ok(self.cmd_attach()),
            FolderCmd::Detach { force } => Ok(self.cmd_detach(force)),
            FolderCmd::SetSync { enabled } => {
                self.record.sync_enabled = enabled;
                self.refresh_entry_state();
                Ok(Settle::Now)
            }
            FolderCmd::OverrideGate => self.cmd_override_gate(),
            FolderCmd::Retry => self.cmd_retry(),
            FolderCmd::Revalidate { ids } => self.cmd_revalidate(ids),
            FolderCmd::CatalogChanged => {
                self.on_catalog_changed();
                Ok(Settle::Now)
            }
            FolderCmd::GateEvidence(evidence) => {
                self.on_evidence(evidence);
                Ok(Settle::Now)
            }
            FolderCmd::Tick => {
                if self.waiting_slot && self.desired && self.idle() {
                    let _ = self.begin_start();
                }
                Ok(Settle::Now)
            }
        }
    }

    /// Persists `desired_running` (a no-op when the catalog already has it).
    fn persist_desired(&mut self, desired: bool) -> Result<(), ErrorWire> {
        let snapshot = self.deps.catalog.snapshot();
        match snapshot.folder(&self.state) {
            Some(record) if record.desired_running != desired => self
                .deps
                .catalog
                .apply(CatalogCmd::SetDesired {
                    state: self.state.clone(),
                    desired,
                })
                .map(|_| ()),
            _ => Ok(()),
        }
    }

    fn cmd_start(&mut self) -> Result<Settle, ErrorWire> {
        self.persist_desired(true)?;
        self.desired = true;
        if let Some(plan) = self.stop.as_mut() {
            let cancellable = plan.blocked.is_some()
                && matches!(plan.after, AfterStop::Stopped)
                && self.running.is_some();
            if cancellable {
                // Start cancels a blocked stop: the worker keeps running (a
                // retry in flight is forgotten; if it did stop the worker, the
                // exit is handled as a crash and restarted).
                self.stop = None;
                self.resolve_error();
            } else if matches!(plan.after, AfterStop::Stopped) {
                plan.after = AfterStop::Start;
            }
            return Ok(Settle::Now);
        }
        // Failed → Starting on Start (§3.3), also while a Failed folder still
        // holds a live process (PREVIOUS_WORKER_UNRESPONSIVE, or one that
        // survived its kills): begin_start drops that untrusted handle and
        // adopts the worker again (hello), or waits for owner.lock.
        let was_failed = self.failed.is_some();
        if was_failed {
            self.reset_failures();
        }
        if self.idle() || was_failed {
            self.begin_start()?;
        }
        Ok(Settle::Now)
    }

    fn cmd_stop(&mut self, force: bool) -> Result<Settle, ErrorWire> {
        // Invariant 26: the intent is durable before anything is stopped.
        self.persist_desired(false)?;
        self.desired = false;
        self.waiting_slot = false;
        self.deps.slots.cancel_wait(&self.state);
        self.stop_worker(force, AfterStop::Stopped);
        Ok(Settle::Stop)
    }

    fn cmd_restart(&mut self) -> Result<Settle, ErrorWire> {
        self.persist_desired(true)?;
        self.desired = true;
        if self.failed.is_some() || self.backoff_state.is_some() {
            self.reset_failures();
            self.backoff_state = None;
            self.begin_start()?;
            return Ok(Settle::Now);
        }
        if self.running.is_some() || self.stop.is_some() || self.start.is_some() {
            // Running, stopping or starting: the stop ladder, then a start. A
            // spawned worker before Ready is killed directly; an adopting
            // worker gets the graceful stop (like Stop); a lock wait or the
            // legacy path, which own no process, start over.
            self.stop_worker(false, AfterStop::Start);
            return Ok(Settle::Stop);
        }
        if self.idle() {
            self.begin_start()?;
        }
        Ok(Settle::Now)
    }

    fn cmd_retry(&mut self) -> Result<Settle, ErrorWire> {
        self.mount_ctl.rearm();
        self.published.retry_at = None;
        if !self.desired && self.failed.is_some() && self.worker.is_some() {
            // Nothing to start (desired=0), and the live process this Failed
            // folder holds is not ours to forget: it stays Failed (not a
            // Stopped phase with a worker behind it). Start adopts it again;
            // ForceStop kills it.
            return Ok(Settle::Now);
        }
        if self.failed.is_some() || self.backoff_state.is_some() {
            self.reset_failures();
            self.failed = None;
            self.backoff_state = None;
            if self.desired {
                self.begin_start()?;
            }
            return Ok(Settle::Now);
        }
        let now = self.now();
        if let Some(plan) = self.stop.as_mut()
            && plan.rpc.is_none()
            && plan.blocked.is_some()
        {
            plan.retry_at = Some(now);
        }
        Ok(Settle::Now)
    }

    fn reset_failures(&mut self) {
        self.failures.clear();
        self.backoff.reset();
        self.failed = None;
    }

    fn cmd_attach(&mut self) -> Settle {
        self.mount_ctl.rearm();
        self.mount_ctl.user_detached = false;
        if self.mount_ctl.attach.is_some() {
            return Settle::Mount;
        }
        if self.want_attach() {
            self.try_attach();
            return Settle::Mount;
        }
        Settle::Now
    }

    fn cmd_detach(&mut self, force: bool) -> Settle {
        self.mount_ctl.user_detached = true;
        self.mount_ctl.retry_at = None;
        if self.mount_ctl.detach.is_some() {
            return Settle::Mount;
        }
        let Some(client) = self.worker_client().filter(|_| self.running.is_some()) else {
            return Settle::Now;
        };
        let ticket = self.rpc(
            RpcKind::Detach,
            client,
            ops::MOUNT_DETACH,
            json!({ "force": force }),
            deadlines::MOUNT_DETACH,
        );
        self.mount_ctl.detach = Some(ticket);
        Settle::Mount
    }

    fn cmd_override_gate(&mut self) -> Result<Settle, ErrorWire> {
        // The management action writes the catalog before it sends this
        // command (WP2.2): the latest snapshot usually has the override, and
        // then the controller writes nothing (its own write is bounded,
        // `catalog::APPLY_WAIT`).
        let latest = self.deps.catalog.snapshot().gates.get(&self.state).cloned();
        if latest
            .as_ref()
            .is_some_and(|gate| gate.override_ms.is_some())
        {
            self.gate_record = latest;
        }
        if let Some(gate) = &self.gate_record
            && gate.override_ms.is_none()
        {
            let snapshot = self.deps.catalog.apply(CatalogCmd::OverrideGate {
                state: self.state.clone(),
            })?;
            self.gate_record = snapshot.gates.get(&self.state).cloned();
        }
        if self.gate_record.is_some() || self.gate != GateState::None {
            let at_ms = self
                .gate_record
                .as_ref()
                .and_then(|gate| gate.override_ms)
                .unwrap_or_else(|| self.now_ms());
            self.gate = GateState::Overridden { at_ms };
        }
        // "MountNow": a held attach is retried.
        self.mount_ctl.rearm();
        Ok(Settle::Now)
    }

    fn cmd_revalidate(&mut self, ids: Option<Vec<String>>) -> Result<Settle, ErrorWire> {
        if let GateState::Blocked { source, .. } = &self.gate {
            self.gate = GateState::Awaiting {
                source: source.clone(),
                since_ms: self.gate_record.as_ref().map_or(0, |gate| gate.created_ms),
                waiting_on: Vec::new(),
            };
        }
        if self.revalidate.is_some() {
            return Ok(Settle::Revalidate);
        }
        let Some(client) = self.worker_client().filter(|_| self.running.is_some()) else {
            return Err(ErrorWire::new(
                "WORKER_NOT_RUNNING",
                format!("WORKER_NOT_RUNNING: {} has no running worker", self.state),
            ));
        };
        let ticket = self.rpc(
            RpcKind::Revalidate,
            client,
            ops::REVALIDATE,
            json!({ "ids": ids }),
            deadlines::REVALIDATE,
        );
        self.revalidate = Some(ticket);
        Ok(Settle::Revalidate)
    }

    // -- catalog -------------------------------------------------------------

    fn on_catalog_changed(&mut self) {
        let snapshot = self.deps.catalog.snapshot();
        let Some(record) = snapshot.folder(&self.state).cloned() else {
            self.on_retire();
            return;
        };
        let old_record = std::mem::replace(&mut self.record, record.clone());
        let old_gate = self.gate_record.clone();
        self.peers = snapshot.peers.clone();
        self.sync_gate(snapshot.gates.get(&self.state).cloned());
        self.check_gate_source();
        let changed = !same_folder(&old_record, &self.record) || old_gate != self.gate_record;
        if record.mount != old_record.mount || record.mount_enabled != old_record.mount_enabled {
            self.mount_ctl.rearm();
            if self.worker.is_none() {
                self.mount = idle_mount(&self.record);
            }
        }
        if record.sync_enabled != old_record.sync_enabled {
            self.refresh_entry_state();
        }
        if record.desired_running != self.desired {
            self.desired = record.desired_running;
            if self.desired {
                if let Some(plan) = self.stop.as_mut() {
                    if matches!(plan.after, AfterStop::Stopped) {
                        plan.after = AfterStop::Start;
                    }
                } else if self.idle() || self.failed.is_some() {
                    self.reset_failures();
                    let _ = self.begin_start();
                }
            } else {
                self.waiting_slot = false;
                self.deps.slots.cancel_wait(&self.state);
                self.stop_worker(false, AfterStop::Stopped);
            }
        } else if changed && self.desired && self.failed.is_some() {
            // Failed leaves to Starting on a catalog change for this folder
            // (a live process it still holds is adopted again, as on Start).
            self.reset_failures();
            let _ = self.begin_start();
        }
    }

    fn on_retire(&mut self) {
        if self.retired {
            return;
        }
        self.retired = true;
        self.deps.status.record(StatusEvent::FolderRemoved {
            state: self.state.clone(),
        });
        self.remove_entry();
        let error = ErrorWire::new(
            "FOLDER_NOT_FOUND",
            format!("FOLDER_NOT_FOUND: {} left the catalog", self.state),
        );
        for waiter in self.waiters.drain(..) {
            let _ = waiter.reply.send(Err(error.clone()));
        }
        if self.worker.is_none() || self.failed.is_some() {
            // No process, or one a graceful stop does not touch (an
            // unresponsive adopted worker, or one that survived its kills):
            // it is left running, like a busy worker at retire.
            self.done = true;
        } else {
            self.stop_worker(false, AfterStop::Retire);
        }
    }

    /// A retired controller ends once it owns no process, or once the process
    /// it still has is one it gives up on (Failed). Whatever the stop plan
    /// would have done afterwards (Backoff, Fail) no longer applies.
    fn end_if_retired(&mut self) {
        if self.retired && (self.worker.is_none() || self.failed.is_some()) {
            self.done = true;
        }
    }

    // -- start ---------------------------------------------------------------

    /// Adoption, the legacy orphan path, or a spawn once `owner.lock` is free.
    /// A live previous worker is probed before any slot accounting, and its
    /// adoption always takes a slot: otherwise a fresh spawn could take the
    /// last one and leave the live worker unsupervised (§2.3 Daemon row;
    /// post-integration re-review, WP2.INT).
    fn begin_start(&mut self) -> Result<(), ErrorWire> {
        if self.shutdown.is_some() || self.retired || self.done {
            return Err(stopping_error());
        }
        self.failed = None;
        self.backoff_state = None;
        let previous = self.probe_previous_worker();
        if previous.is_some() {
            self.take_slot_for_adoption();
        } else if !self.acquire_slot() {
            self.waiting_slot = true;
            let error = ErrorWire::new(
                "WORKER_LIMIT",
                format!(
                    "WORKER_LIMIT: {} folders already hold a worker (maximum_running)",
                    self.limits().maximum_running
                ),
            );
            self.observe_error(error.clone());
            return Err(error);
        }
        self.waiting_slot = false;
        if let Some(old) = self.worker.take() {
            // A process we no longer trust (for example an unresponsive adopted
            // worker): never killed here; owner.lock keeps it from doubling.
            if let Some(cancel) = &old.watch {
                cancel.cancel();
            }
        }
        self.drop_worker_rpcs();
        self.running = None;
        self.heartbeat = None;
        self.start_since_ms = self.now_ms();
        if let Some((epoch, record, process)) = previous {
            self.adopt_previous(epoch, record, process);
            return Ok(());
        }
        let directory = self.record.directory.clone();
        if read_worker_record(&directory).is_none()
            && let Some(discovery) = read_discovery(&directory)
            && discovery.repo == self.record.repo
            && discovery.device == self.record.device
        {
            // Legacy orphan: status, stop, wait for the lock, then spawn.
            let client = self.deps.launcher.client(&WorkerEndpoint {
                instance: String::new(),
                pid: 0,
                address: discovery.address,
                version: String::new(),
                token: None,
            });
            let deadline = self.limits().stop_graceful;
            let ticket = self.rpc(
                RpcKind::LegacyStatus,
                client.clone(),
                ops::STATUS,
                json!({}),
                deadline,
            );
            self.start = Some(StartPlan::Legacy {
                client,
                stage: LegacyStage::Status,
                rpc: Some(ticket),
            });
            return Ok(());
        }
        self.wait_lock_then_spawn();
        Ok(())
    }

    /// Spawns once `owner.lock` is free (invariant 23); never while the
    /// folder is not desired (an adoption that ended without a worker).
    fn wait_lock_then_spawn(&mut self) {
        if !self.desired {
            self.start = None;
            return;
        }
        if owner_lock_free(&self.record.directory) {
            self.spawn_worker();
            return;
        }
        let now = self.now();
        self.start = Some(StartPlan::WaitLock {
            since: now,
            next_poll: now + LOCK_POLL,
            warned: false,
        });
    }

    /// One look at `owner.lock` while waiting to spawn. A wait never counts
    /// as a worker failure (nothing was launched, nothing crashed): it goes
    /// on for as long as a live process holds the lock, and a previous
    /// worker that publishes its `worker.json` meanwhile (an orphan that was
    /// still recovering its store) is adopted. Past
    /// `limits::ADOPT_LOCK_WAIT` the folder shows
    /// `PREVIOUS_WORKER_STOP_TIMEOUT` (post-integration re-review, WP2.INT:
    /// the wait used to end in a counted failure, five of which opened the
    /// crash breaker without any worker having run).
    fn poll_lock(&mut self) {
        let Some(StartPlan::WaitLock { since, warned, .. }) = &self.start else {
            return;
        };
        let (since, warned) = (*since, *warned);
        if !self.desired {
            self.start = None;
            return;
        }
        if owner_lock_free(&self.record.directory) {
            self.spawn_worker();
            return;
        }
        let fresh = read_worker_record(&self.record.directory)
            .is_some_and(|record| self.probed.as_deref() != Some(record.instance.as_str()));
        if fresh && let Some((epoch, record, process)) = self.probe_previous_worker() {
            self.take_slot_for_adoption();
            self.adopt_previous(epoch, record, process);
            return;
        }
        let now = self.now();
        let waited = now.saturating_duration_since(since) >= limits::ADOPT_LOCK_WAIT;
        if waited && !warned {
            let error = ErrorWire::new(
                "PREVIOUS_WORKER_STOP_TIMEOUT",
                format!(
                    "PREVIOUS_WORKER_STOP_TIMEOUT: {} is still locked by another process after {} s; \
                     a second worker is not started: the folder waits for that process to exit",
                    self.record.directory.join("owner.lock").display(),
                    limits::ADOPT_LOCK_WAIT.as_secs()
                ),
            );
            event!(warn, PREVIOUS_WORKER_STOP_TIMEOUT, { "folder": self.state });
            self.observe_error(error);
        }
        if let Some(StartPlan::WaitLock {
            next_poll, warned, ..
        }) = self.start.as_mut()
        {
            *next_poll = now + LOCK_POLL;
            *warned |= waited;
        }
    }

    fn no_exit(&self) -> ExitRecord {
        ExitRecord {
            code: None,
            fatal: None,
            stderr_tail: String::new(),
            at_ms: self.now_ms(),
        }
    }

    fn spawn_worker(&mut self) {
        let epoch = self.next_epoch();
        let spec = LaunchSpec {
            state: self.state.clone(),
            directory: self.record.directory.clone(),
            executable: self.limits().executable.clone(),
            supervised: true,
            log: self.worker_log(),
            env: Vec::new(),
        };
        match self.deps.launcher.launch(&spec, self.sink(epoch)) {
            Ok(process) => {
                let now = self.now();
                self.worker = Some(Worker {
                    epoch,
                    process,
                    record: None,
                    endpoint: None,
                    client: None,
                    watch: None,
                    fatal: None,
                    killed: false,
                });
                self.start = Some(StartPlan::Spawned {
                    watchdog: Watchdog::new(now),
                    last_progress: now,
                    progress_seq: 0,
                });
            }
            Err(error) => {
                self.start = None;
                event!(warn, (error.code.as_str()), {
                    "folder": self.state,
                    "detail": "worker launch failed",
                    "message": error.message,
                });
                let exit = self.no_exit();
                self.fail(error, exit);
            }
        }
    }

    /// `startup::Watchdog` expiry with the configurable startup silence
    /// (`Watchdog::expired_after`, CCR-WP1.2-4).
    fn watchdog_expired(&self, watchdog: &Watchdog, now: Instant) -> bool {
        watchdog.expired_after(now, self.limits().startup_silence)
    }

    fn startup_timeout(&mut self) {
        let phase = match &self.start {
            Some(StartPlan::Spawned { watchdog, .. }) => watchdog.phase.clone(),
            _ => return,
        };
        let error = ErrorWire::new(
            "WORKER_START_TIMEOUT",
            format!(
                "WORKER_START_TIMEOUT: no startup progress for {} s during {phase}; see {}",
                self.limits().startup_silence.as_secs(),
                self.worker_log().display()
            ),
        );
        event!(warn, WORKER_START_TIMEOUT, { "folder": self.state, "phase": phase });
        self.observe_error(error.clone());
        self.kill_then(AfterStop::Backoff(error));
    }

    fn adopt_hello(&mut self) {
        let Some(client) = self.worker_client() else {
            return;
        };
        let deadline = self.limits().hello_timeout;
        let ticket = self.rpc(RpcKind::AdoptHello, client, ops::HELLO, json!({}), deadline);
        if let Some(StartPlan::Adopt {
            next_hello, rpc, ..
        }) = self.start.as_mut()
        {
            *next_hello = None;
            *rpc = Some(ticket);
        }
    }

    fn on_adopt_hello(&mut self, result: Result<Value, CallError>) {
        let Some(StartPlan::Adopt { since, .. }) = &self.start else {
            return;
        };
        let since = *since;
        let Some(record) = self.worker.as_ref().and_then(|w| w.record.clone()) else {
            return;
        };
        match parse::<HelloReply>(result) {
            Ok(hello) => {
                if hello.instance != record.instance
                    || hello.repo != self.record.repo
                    || hello.device != self.record.device
                {
                    // Not the worker the record names: never killed. Wait for
                    // owner.lock instead (invariant 23).
                    event!(warn, WORKER_STATE_MISMATCH, {
                        "folder": self.state,
                        "detail": "the adopted process answered hello with another identity",
                        "instance": hello.instance,
                    });
                    self.probed = Some(record.instance.clone());
                    self.start = None;
                    if let Some(worker) = self.worker.take()
                        && let Some(cancel) = worker.watch
                    {
                        cancel.cancel();
                    }
                    self.wait_lock_then_spawn();
                    return;
                }
                if self.version_matches(&hello.version) && hello.ipc == IPC_VERSION {
                    if let Some(endpoint) = self.worker.as_mut().and_then(|w| w.endpoint.as_mut()) {
                        endpoint.version = hello.version;
                    }
                    self.enter_running(true);
                    if !self.desired {
                        // Adopted to be stopped (`adopt_to_stop`): the stop
                        // ladder from here, as a Stop of a running folder.
                        self.stop_worker(false, AfterStop::Stopped);
                    }
                } else {
                    // Another version: graceful stop, then spawn once the lock
                    // is free (if the folder is still wanted).
                    self.replaced = Some(record.instance.clone());
                    self.start = None;
                    let after = if self.desired {
                        AfterStop::Start
                    } else {
                        AfterStop::Stopped
                    };
                    self.send_stop(false, after);
                }
            }
            Err(error) => {
                let now = self.now();
                if now.saturating_duration_since(since) >= self.limits().adopt_patience {
                    let error = ErrorWire::new(
                        "PREVIOUS_WORKER_UNRESPONSIVE",
                        format!(
                            "PREVIOUS_WORKER_UNRESPONSIVE: worker pid {} is alive but has not answered \
                             hello for {} s ({error}); ForceStop kills it",
                            record.pid,
                            self.limits().adopt_patience.as_secs()
                        ),
                    );
                    event!(warn, PREVIOUS_WORKER_UNRESPONSIVE, {
                        "folder": self.state,
                        "pid": record.pid,
                    });
                    self.start = None;
                    self.enter_failed(error);
                } else if let Some(StartPlan::Adopt {
                    next_hello, rpc, ..
                }) = self.start.as_mut()
                {
                    *rpc = None;
                    *next_hello = Some(now + ADOPT_HELLO_RETRY);
                }
            }
        }
    }

    fn version_matches(&self, version: &str) -> bool {
        let ours = self.limits().worker_version.as_str();
        if ours.is_empty() {
            return true;
        }
        if ours.contains('+') {
            version == ours
        } else {
            version.split('+').next() == Some(ours)
        }
    }

    fn on_legacy(&mut self, kind: RpcKind, result: Result<Value, CallError>) {
        let Some(StartPlan::Legacy { client, stage, .. }) = &self.start else {
            return;
        };
        let client = client.clone();
        let stage = *stage;
        match (kind, stage) {
            (RpcKind::LegacyStatus, LegacyStage::Status) => {
                let ours = result.as_ref().is_ok_and(|status| {
                    status["repo"] == self.record.repo.as_str()
                        && status["device"] == self.record.device.as_str()
                });
                if ours {
                    let deadline = self.limits().stop_graceful;
                    let ticket =
                        self.rpc(RpcKind::LegacyStop, client, ops::STOP, json!({}), deadline);
                    if let Some(StartPlan::Legacy { stage, rpc, .. }) = self.start.as_mut() {
                        *stage = LegacyStage::Stop;
                        *rpc = Some(ticket);
                    }
                } else {
                    self.start = None;
                    self.wait_lock_then_spawn();
                }
            }
            (RpcKind::LegacyStop, LegacyStage::Stop) => {
                self.start = None;
                self.wait_lock_then_spawn();
            }
            _ => {}
        }
    }

    // -- process events ------------------------------------------------------

    fn on_process(&mut self, epoch: u64, event: ProcessEvent) {
        if !self.current_epoch(epoch) {
            return;
        }
        match event {
            ProcessEvent::Line(StartupLine::Progress(progress)) => {
                let now = self.now();
                if let Some(StartPlan::Spawned {
                    watchdog,
                    last_progress,
                    progress_seq,
                }) = self.start.as_mut()
                    && progress.startup_progress > *progress_seq
                {
                    watchdog.observe(progress.startup_progress, &progress.phase, now);
                    *progress_seq = progress.startup_progress;
                    *last_progress = now;
                }
            }
            ProcessEvent::Line(StartupLine::Ready(ready)) => self.on_ready(ready),
            ProcessEvent::Line(StartupLine::Fatal(fatal)) => {
                if let Some(worker) = self.worker.as_mut() {
                    worker.fatal = Some(fatal);
                }
            }
            ProcessEvent::Exited {
                code,
                fatal,
                stderr_tail,
            } => self.on_exit(code, fatal, stderr_tail),
        }
    }

    fn on_ready(&mut self, ready: ReadyLine) {
        if !matches!(self.start, Some(StartPlan::Spawned { .. })) {
            return;
        }
        let mismatch = if ready.repo != self.record.repo || ready.device != self.record.device {
            Some(ErrorWire::new(
                "WORKER_STATE_MISMATCH",
                format!(
                    "WORKER_STATE_MISMATCH: the worker opened repo {} device {}; the catalog has repo {} \
                     device {}",
                    ready.repo, ready.device, self.record.repo, self.record.device
                ),
            ))
        } else if ready.ipc != IPC_VERSION {
            Some(ErrorWire::new(
                "WORKER_IPC_MISMATCH",
                format!(
                    "WORKER_IPC_MISMATCH: the worker speaks IPC {}, this orchestrator IPC {IPC_VERSION}",
                    ready.ipc
                ),
            ))
        } else {
            None
        };
        if let Some(error) = mismatch {
            event!(error, (error.code.as_str()), { "folder": self.state, "message": error.message });
            self.observe_error(error.clone());
            self.kill_then(AfterStop::Fail(error));
            return;
        }
        let mount = self.mount_from_ready(&ready);
        let Some(worker) = self.worker.as_mut() else {
            return;
        };
        let endpoint = WorkerEndpoint {
            instance: ready.instance,
            pid: if ready.pid != 0 {
                ready.pid
            } else {
                worker.process.pid()
            },
            address: ready.address,
            version: ready.version,
            token: ready.token,
        };
        worker.client = Some(self.deps.launcher.client(&endpoint));
        worker.endpoint = Some(endpoint);
        self.mount = mount;
        self.enter_running(false);
    }

    fn mount_from_ready(&self, ready: &ReadyLine) -> MountStatus {
        let mut mount = idle_mount(&self.record);
        if ready.mounted {
            mount.state = MountState::Attached {
                since_ms: self.now_ms(),
            };
        } else if let Some(error) = &ready.mount_error {
            mount.state = MountState::AttachFailed {
                error: error.clone(),
                attempt: 1,
                next_retry_at_ms: None,
            };
        }
        mount
    }

    fn enter_running(&mut self, adopted: bool) {
        let now = self.now();
        self.start = None;
        self.backoff_state = None;
        self.failed = None;
        self.running = Some(RunningState {
            since_ms: self.now_ms(),
            adopted,
        });
        self.backoff.success(now);
        self.resolve_error();
        self.liveness = Liveness {
            last_heartbeat: now,
            watch_failures: 0,
            probe: None,
        };
        self.mount_ctl.rearm();
        self.mount_ctl.user_detached = false;
        self.mount_ctl.known = !adopted;
        self.mount_ctl.stale_heartbeat = false;
        self.published.refresh = false;
        self.published.retry_at = None;
        self.published.branches_gen = None;
        let Some(worker) = self.worker.as_mut() else {
            return;
        };
        let Some(endpoint) = worker.endpoint.clone() else {
            return;
        };
        *crate::util::lock::lock(&self.client_slot) = worker.client.clone();
        // A dedicated connection for watch, never shared with other calls.
        let watch_client = self.deps.launcher.client(&endpoint);
        let cancel = Cancel::new();
        let tx = self.tx.clone();
        let epoch = worker.epoch;
        let stop = cancel.clone();
        let spawned = thread::Builder::new()
            .name(format!("folder-{}-watch", self.state))
            .spawn(move || watch_loop(epoch, watch_client, tx, stop));
        if let Err(error) = spawned {
            let _ = self.tx.send(Msg::WatchFailed(
                epoch,
                CallError::Unavailable(format!(
                    "LOCAL_WORKER_UNAVAILABLE: cannot start the watch thread: {error}"
                )),
            ));
        }
        worker.watch = Some(cancel);
    }

    fn on_exit(&mut self, code: Option<i32>, fatal: Option<FatalLine>, stderr_tail: String) {
        let Some(worker) = self.worker.take() else {
            return;
        };
        if let Some(cancel) = &worker.watch {
            cancel.cancel();
        }
        *crate::util::lock::lock(&self.client_slot) = None;
        let exit = ExitRecord {
            code,
            fatal: fatal.or(worker.fatal),
            stderr_tail,
            at_ms: self.now_ms(),
        };
        let killed = worker.killed;
        let adopted = worker.record.is_some() || worker.process.adopted();
        // The stop plan's RPC state decides the exit's meaning (below); it is
        // read before the worker's RPCs are forgotten.
        let crashed = self
            .stop
            .as_ref()
            .is_some_and(|plan| crashed_while_stopping(plan, killed, adopted, exit.code));
        self.drop_worker_rpcs();
        self.running = None;
        self.heartbeat = None;
        self.degraded = false;
        self.mount = idle_mount(&self.record);
        self.withdraw_entry();
        if self.shutdown.is_some() {
            self.stop = None;
            self.start = None;
            let outcome = if killed {
                outcomes::KILLED
            } else {
                outcomes::STOPPED
            };
            self.finish_shutdown(outcome, None);
            return;
        }
        if let Some(plan) = self.stop.take() {
            if crashed && matches!(plan.after, AfterStop::Start) {
                // Crashed while stopping for a restart: back off (§3.3).
                let error = classify_exit(&exit, adopted, &self.worker_log());
                self.fail(error, exit);
                return;
            }
            self.after_stop(plan.after, exit);
            return;
        }
        if self.retired {
            self.done = true;
            return;
        }
        if self.failed.is_some() {
            // For example an unresponsive adopted worker that finally died.
            self.start = None;
            return;
        }
        if matches!(self.start, Some(StartPlan::Adopt { .. })) {
            // The previous worker died while it was being adopted: not a
            // failure of ours; spawn once owner.lock is free.
            self.start = None;
            self.wait_lock_then_spawn();
            return;
        }
        self.start = None;
        let error = classify_exit(&exit, adopted, &self.worker_log());
        event!(warn, WORKER_EXITED, {
            "folder": self.state,
            "exit_code": exit.code,
            "error": error.code,
        });
        self.fail(error, exit);
    }

    fn after_stop(&mut self, after: AfterStop, exit: ExitRecord) {
        match after {
            AfterStop::Stopped => {
                self.failed = None;
                self.resolve_error();
            }
            AfterStop::Start => {
                if self.desired {
                    let _ = self.begin_start();
                }
            }
            AfterStop::Backoff(error) => self.fail(error, exit),
            AfterStop::Fail(error) => self.enter_failed(error),
            AfterStop::Retire => self.done = true,
        }
    }

    /// Records a failure: Backoff, or Failed for non-retryable classes and
    /// when the breaker opens.
    fn fail(&mut self, error: ErrorWire, exit: ExitRecord) {
        self.observe_error(error.clone());
        self.start = None;
        self.running = None;
        if !self.desired || self.shutdown.is_some() || self.retired {
            return;
        }
        if !error.class.retries_automatically() {
            self.enter_failed(error);
            return;
        }
        let now = self.now();
        if error.class.counts_as_failure() {
            self.failures.push_back(now);
        }
        let window = self.limits().breaker_window;
        while self
            .failures
            .front()
            .is_some_and(|first| now.saturating_duration_since(*first) > window)
        {
            self.failures.pop_front();
        }
        if self.failures.len() >= self.limits().breaker_failures.max(1) as usize {
            let crash = ErrorWire::new(
                "CRASH_LOOP",
                format!(
                    "CRASH_LOOP: {} worker failures within {} s; last: {error}",
                    self.failures.len(),
                    window.as_secs()
                ),
            );
            event!(error, CRASH_LOOP, { "folder": self.state, "last": error.code });
            self.enter_failed(crash);
            return;
        }
        let delay = self.backoff.failure(now);
        self.backoff_state = Some(BackoffState {
            until: now + delay,
            until_ms: self.now_ms().saturating_add(ms(delay)),
            attempt: self.backoff.attempt(),
            last_exit: exit,
        });
    }

    fn enter_failed(&mut self, error: ErrorWire) {
        self.observe_error(error.clone());
        self.start = None;
        self.backoff_state = None;
        self.running = None;
        let at_ms = self.now_ms();
        self.failed = Some(ErrorRecord::new(error, self.scope(), at_ms));
    }

    /// Forgets every worker RPC in flight (their results are ignored).
    fn drop_worker_rpcs(&mut self) {
        self.liveness.probe = None;
        self.mount_ctl.attach = None;
        self.mount_ctl.detach = None;
        self.published.rpc = None;
        self.revalidate = None;
        if let Some(plan) = self.stop.as_mut() {
            plan.rpc = None;
        }
    }

    // -- heartbeat and liveness ---------------------------------------------

    fn liveness_active(&self) -> bool {
        self.running.is_some()
            && self.shutdown.is_none()
            && self.liveness.probe.is_none()
            && self.worker.as_ref().is_some_and(|w| !w.killed)
            && self
                .stop
                .as_ref()
                .is_none_or(|plan| plan.stage == StopStage::Requested)
    }

    fn on_heartbeat(&mut self, epoch: u64, heartbeat: Arc<WorkerHeartbeat>) {
        if !self.current_epoch(epoch) || self.running.is_none() {
            return;
        }
        let ours = self
            .worker
            .as_ref()
            .and_then(|w| w.endpoint.as_ref())
            .is_some_and(|endpoint| endpoint.instance == heartbeat.instance);
        if !ours {
            self.on_watch_failed(
                epoch,
                CallError::Remote(ErrorWire::new(
                    "WORKER_STATE_MISMATCH",
                    format!(
                        "WORKER_STATE_MISMATCH: watch answered for instance {}",
                        heartbeat.instance
                    ),
                )),
            );
            return;
        }
        self.liveness.last_heartbeat = self.now();
        self.liveness.watch_failures = 0;
        if self.degraded != heartbeat.engine.stalled {
            self.degraded = heartbeat.engine.stalled;
            if self.degraded {
                event!(warn, LOCK_HOLD_EXCEEDED, {
                    "folder": self.state,
                    "detail": "the worker engine lock is stalled; the folder is degraded",
                    "current_hold_ms": heartbeat.engine.current_hold_ms,
                });
            }
        }
        if self.mount_ctl.attach.is_none() && self.mount_ctl.detach.is_none() {
            if self.mount_ctl.stale_heartbeat {
                // Possibly older than the attach/detach result: the result
                // stands until the next heartbeat (an AttachFailed taken
                // before a successful attach would re-arm the attach).
                self.mount_ctl.stale_heartbeat = false;
            } else {
                self.mount = self.merged_mount(&heartbeat.mount);
            }
        }
        self.mount_ctl.known = true;
        let branches = heartbeat.generations.branches;
        match self.published.branches_gen {
            Some(seen) if seen != branches => self.published.refresh = true,
            // The entry was taken before any heartbeat (the summary on Ready),
            // so the branches generation it reflects is unknown and a change
            // since then would be missed: one more summary, requested with
            // this heartbeat's generation (one extra call per start).
            None if self.published.entry.is_some() && self.published.rpc.is_none() => {
                self.published.refresh = true
            }
            _ => {}
        }
        self.heartbeat = Some(heartbeat.clone());
        self.update_entry_from_heartbeat(&heartbeat);
        if !self.retired {
            // Nothing follows FolderRemoved (see `publish`).
            self.deps.status.record(StatusEvent::WorkerHeartbeat {
                state: self.state.clone(),
                hb: heartbeat,
            });
        }
    }

    fn on_watch_failed(&mut self, epoch: u64, _error: CallError) {
        if !self.current_epoch(epoch) || self.running.is_none() {
            return;
        }
        self.liveness.watch_failures = self.liveness.watch_failures.saturating_add(1);
        if self.liveness.watch_failures >= limits::WATCH_FAILURES && self.liveness_active() {
            self.probe();
        }
    }

    fn probe(&mut self) {
        if self.liveness.probe.is_some() || self.shutdown.is_some() {
            return;
        }
        let Some(client) = self.worker_client() else {
            return;
        };
        let deadline = self.limits().hello_timeout;
        let ticket = self.rpc(RpcKind::Probe, client, ops::HELLO, json!({}), deadline);
        self.liveness.probe = Some(ticket);
    }

    fn on_probe(&mut self, result: Result<Value, CallError>) {
        self.liveness.probe = None;
        let instance = self
            .worker
            .as_ref()
            .and_then(|w| w.endpoint.as_ref())
            .map(|endpoint| endpoint.instance.clone());
        let failure = match parse::<HelloReply>(result) {
            Ok(hello) if Some(&hello.instance) == instance.as_ref() => None,
            Ok(hello) => Some(format!("hello answered for instance {}", hello.instance)),
            Err(error) => Some(error.to_string()),
        };
        let Some(detail) = failure else {
            self.liveness.last_heartbeat = self.now();
            self.liveness.watch_failures = 0;
            return;
        };
        if self.shutdown.is_some() || self.worker.is_none() {
            return;
        }
        let error = ErrorWire::new(
            "WORKER_UNRESPONSIVE",
            format!(
                "WORKER_UNRESPONSIVE: no heartbeat for {} s or {} failed watches, and hello failed: {detail}",
                self.limits().heartbeat_timeout.as_secs(),
                limits::WATCH_FAILURES
            ),
        );
        event!(warn, WORKER_UNRESPONSIVE, { "folder": self.state, "detail": detail });
        self.observe_error(error.clone());
        let after = match &self.stop {
            Some(plan) => plan.after.clone(),
            None => AfterStop::Backoff(error),
        };
        self.kill_then(after);
    }

    // -- stop ladder ---------------------------------------------------------

    /// Stops the worker (or cancels a start without one) and then does `after`.
    fn stop_worker(&mut self, force: bool, after: AfterStop) {
        if let Some(plan) = self.stop.as_mut() {
            // Retire overrides any plan (a kill bound for Backoff or Fail too).
            if matches!(after, AfterStop::Retire)
                || matches!(plan.after, AfterStop::Stopped | AfterStop::Start)
            {
                plan.after = after;
            }
            let upgrade = force && !plan.force && plan.stage == StopStage::Requested;
            plan.force |= force;
            if upgrade {
                // ForceStop while a graceful stop is pending or blocked.
                plan.blocked = None;
                self.send_stop(true, AfterStop::Stopped);
            }
            return;
        }
        if self.failed.is_some() && self.worker.is_some() && !force {
            // An unresponsive adopted worker (PREVIOUS_WORKER_UNRESPONSIVE) or
            // one that survived its kills: a graceful Stop records the intent
            // only; ForceStop kills it.
            return;
        }
        self.failed = None;
        if self.worker.is_none() {
            // Stopped, Backoff, Failed, or a start that owns no process yet.
            self.start = None;
            self.backoff_state = None;
            self.running = None;
            let exit = self.no_exit();
            self.after_stop(after, exit);
            return;
        }
        if matches!(self.start, Some(StartPlan::Spawned { .. })) {
            // No RPC served yet: kill directly (store recovery is crash-safe).
            self.kill_then(after);
            return;
        }
        self.send_stop(force, after);
    }

    /// Sends `stop{mode}`; `after` applies only when no stop plan exists yet.
    fn send_stop(&mut self, force: bool, after: AfterStop) {
        let Some(client) = self.worker_client() else {
            let after = self.stop.as_ref().map_or(after, |plan| plan.after.clone());
            self.kill_then(after);
            return;
        };
        let limits = self.limits();
        let deadline = if force {
            limits.stop_graceful + limits.stop_exit_wait
        } else {
            limits.stop_graceful
        };
        let deadline = self.shutdown_cap(deadline);
        let mode = if force { "force" } else { "graceful" };
        let ticket = self.rpc(
            RpcKind::Stop,
            client,
            ops::STOP,
            json!({ "mode": mode }),
            deadline,
        );
        let now = self.now();
        let now_ms = self.now_ms();
        let stage_deadline = deadline + RPC_GRACE;
        // A worker being adopted has not answered `hello` yet.
        let unverified_since = match &self.start {
            Some(StartPlan::Adopt { since, .. }) => Some(*since),
            _ => None,
        };
        self.start = None;
        match self.stop.as_mut() {
            Some(plan) => {
                plan.rpc = Some(ticket);
                plan.force |= force;
                plan.retry_at = None;
                if plan.blocked.is_none() {
                    plan.stage = StopStage::Requested;
                }
                plan.deadline = Some(now + stage_deadline);
                plan.deadline_ms = now_ms.saturating_add(ms(stage_deadline));
            }
            None => {
                self.stop = Some(StopPlan {
                    force,
                    stage: StopStage::Requested,
                    since_ms: now_ms,
                    deadline: Some(now + stage_deadline),
                    deadline_ms: now_ms.saturating_add(ms(stage_deadline)),
                    after,
                    rpc: Some(ticket),
                    blocked: None,
                    retry_at: None,
                    kills: 0,
                    unverified_since,
                });
            }
        }
    }

    fn on_stop_result(&mut self, result: Result<Value, CallError>) {
        enum Next {
            AwaitExit,
            LeftRunning(ErrorWire),
            Blocked(ErrorWire),
            Uncertain,
            Unverified(CallError),
        }
        let now = self.now();
        let now_ms = self.now_ms();
        let exit_wait = self.limits().stop_exit_wait;
        let patience = self.limits().adopt_patience;
        let shutting_down = self.shutdown.is_some();
        let Some(plan) = self.stop.as_mut() else {
            return;
        };
        plan.rpc = None;
        let next = match result {
            Ok(_) => Next::AwaitExit,
            // Busy or silent at shutdown: left running, adopted next time.
            Err(error) if shutting_down => Next::LeftRunning(error.to_wire()),
            // stop{force}: wait for the exit either way, then kill.
            Err(_) if plan.force => Next::AwaitExit,
            // Graceful refusal (BUSY_VIEW: open files or dirty sessions).
            Err(CallError::Remote(error)) => Next::Blocked(error),
            // A worker being adopted never answered `hello`: the adoption
            // rule applies (§3.3: retry, then
            // `Failed{PREVIOUS_WORKER_UNRESPONSIVE}`; only ForceStop kills
            // it), not the liveness probe and kill of a verified worker
            // (post-integration re-review, WP2.INT).
            Err(error) if plan.unverified_since.is_some() => Next::Unverified(error),
            Err(_) => Next::Uncertain,
        };
        match next {
            Next::AwaitExit => {
                plan.stage = StopStage::AwaitExit;
                plan.blocked = None;
                plan.retry_at = None;
                plan.deadline = Some(now + exit_wait);
                plan.deadline_ms = now_ms.saturating_add(ms(exit_wait));
                self.resolve_stop_blocked();
            }
            Next::LeftRunning(error) => {
                self.finish_shutdown(outcomes::LEFT_RUNNING, Some(error));
            }
            Next::Blocked(error) => {
                plan.deadline = None;
                plan.retry_at = Some(now + limits::STOP_BLOCKED_RETRY);
                match plan.blocked.as_mut() {
                    Some(blocked) => blocked.reason = error.clone(),
                    None => {
                        plan.blocked = Some(Blocked {
                            since_ms: now_ms,
                            reason: error.clone(),
                        })
                    }
                }
                let retire = matches!(plan.after, AfterStop::Retire);
                let shown = if error.code == "BUSY_VIEW" {
                    ErrorWire::new(
                        "STOP_BLOCKED_BY_OPEN_FILES",
                        format!(
                            "STOP_BLOCKED_BY_OPEN_FILES: {error}; close the files or ForceStop"
                        ),
                    )
                } else {
                    error
                };
                self.observe_error(shown);
                if retire {
                    // The folder left the catalog: leave its busy worker running.
                    self.done = true;
                }
            }
            Next::Uncertain => {
                // Uncertain or unreachable: retry after a liveness probe; a
                // failed probe kills (the worker is unresponsive).
                plan.deadline = None;
                plan.retry_at = Some(now + limits::STOP_BLOCKED_RETRY);
                self.probe();
            }
            Next::Unverified(error) => {
                plan.deadline = None;
                let silent_for = plan
                    .unverified_since
                    .map_or(Duration::ZERO, |since| now.saturating_duration_since(since));
                if silent_for < patience {
                    plan.retry_at = Some(now + limits::STOP_BLOCKED_RETRY);
                    return;
                }
                let pid = self.worker.as_ref().map_or(0, |w| w.process.pid());
                let error = ErrorWire::new(
                    "PREVIOUS_WORKER_UNRESPONSIVE",
                    format!(
                        "PREVIOUS_WORKER_UNRESPONSIVE: worker pid {pid} is alive but has answered \
                         neither hello nor stop for {} s ({error}); ForceStop kills it",
                        patience.as_secs()
                    ),
                );
                event!(warn, PREVIOUS_WORKER_UNRESPONSIVE, {
                    "folder": self.state,
                    "pid": pid,
                });
                self.stop = None;
                self.enter_failed(error);
            }
        }
    }

    fn resolve_stop_blocked(&mut self) {
        self.resolve_error_code("STOP_BLOCKED_BY_OPEN_FILES");
    }

    fn resolve_gate_blocked(&mut self) {
        self.resolve_error_code("GATE_BLOCKED");
    }

    /// Resolves `last_error` if it is an unresolved `code`.
    fn resolve_error_code(&mut self, code: &str) {
        if self
            .last_error
            .as_ref()
            .is_some_and(|record| record.error.code == code && !record.is_resolved())
        {
            self.resolve_error();
        }
    }

    fn stop_deadline_passed(&mut self) {
        let Some(plan) = self.stop.as_ref() else {
            return;
        };
        match plan.stage {
            StopStage::Requested => {
                if plan.rpc.is_some() {
                    let after_ms = ms(self.limits().stop_graceful + RPC_GRACE);
                    self.on_stop_result(Err(CallError::Timeout { after_ms }));
                }
            }
            StopStage::AwaitExit => {
                if self.shutdown.is_none() {
                    let after = plan.after.clone();
                    self.kill_then(after);
                }
            }
            StopStage::Killing => {
                if plan.kills < KILL_ATTEMPTS {
                    let after = plan.after.clone();
                    self.kill_then(after);
                } else {
                    let pid = self.worker.as_ref().map_or(0, |w| w.process.pid());
                    let error = ErrorWire::new(
                        "WORKER_STOP_TIMEOUT",
                        format!(
                            "WORKER_STOP_TIMEOUT: worker pid {pid} did not exit after {KILL_ATTEMPTS} \
                             kill attempts"
                        ),
                    );
                    event!(error, WORKER_STOP_TIMEOUT, { "folder": self.state, "pid": pid });
                    self.stop = None;
                    self.enter_failed(error);
                }
            }
        }
    }

    /// Kills the worker after the launcher re-verifies its identity, then
    /// waits for the exit before doing `after` (never spawns before it).
    fn kill_then(&mut self, after: AfterStop) {
        let now = self.now();
        let now_ms = self.now_ms();
        let exit_wait = self.limits().stop_exit_wait;
        let (since_ms, kills) = self
            .stop
            .as_ref()
            .map_or((now_ms, 0), |plan| (plan.since_ms, plan.kills));
        let Some(worker) = self.worker.as_mut() else {
            self.stop = None;
            let exit = self.no_exit();
            self.after_stop(after, exit);
            return;
        };
        if let Err(error) = worker.process.kill() {
            event!(warn, (error.code.as_str()), {
                "folder": self.state,
                "detail": "kill refused",
                "pid": worker.process.pid(),
                "message": error.message,
            });
        }
        worker.killed = true;
        self.start = None;
        self.stop = Some(StopPlan {
            force: true,
            stage: StopStage::Killing,
            since_ms,
            deadline: Some(now + exit_wait),
            deadline_ms: now_ms.saturating_add(ms(exit_wait)),
            after,
            rpc: None,
            blocked: None,
            retry_at: None,
            kills: kills + 1,
            unverified_since: None,
        });
    }

    // -- shutdown ------------------------------------------------------------

    fn on_shutdown(&mut self, deadline: Instant, reply: Sender<ShutdownOutcome>) {
        if self.shutdown.is_some() {
            return;
        }
        self.shutdown = Some(ShutdownCtl {
            deadline,
            reply,
            replied: false,
        });
        let error = stopping_error();
        for waiter in self.waiters.drain(..) {
            let _ = waiter.reply.send(Err(error.clone()));
        }
        self.backoff_state = None;
        self.waiting_slot = false;
        if self.worker.is_none() {
            self.start = None;
            self.finish_shutdown(outcomes::NOT_RUNNING, None);
            return;
        }
        if matches!(self.start, Some(StartPlan::Spawned { .. })) {
            if self.limits().shutdown_kill_starting {
                self.kill_then(AfterStop::Stopped);
            } else {
                self.finish_shutdown(outcomes::LEFT_RUNNING, None);
            }
            return;
        }
        if let Some(error) = self.failed.clone() {
            // An unresponsive adopted worker: never killed at shutdown.
            self.finish_shutdown(outcomes::LEFT_RUNNING, Some(error.error));
            return;
        }
        // Waiting for the exit, or a stop RPC in flight decides; otherwise a
        // graceful stop (never `desired=0`: Shutdown keeps the intent).
        let pending = self
            .stop
            .as_ref()
            .is_some_and(|plan| plan.stage != StopStage::Requested || plan.rpc.is_some());
        if !pending {
            self.send_stop(false, AfterStop::Stopped);
        }
    }

    fn shutdown_deadline_passed(&mut self) {
        let killed = self.worker.as_ref().is_some_and(|w| w.killed);
        if killed {
            self.finish_shutdown(outcomes::KILLED, None);
        } else if self.worker.is_some() {
            let error = ErrorWire::new(
                "WORKER_STOP_TIMEOUT",
                "WORKER_STOP_TIMEOUT: the worker did not stop before the shutdown deadline; \
                 it is left running and adopted at the next start",
            );
            self.finish_shutdown(outcomes::LEFT_RUNNING, Some(error));
        } else {
            self.finish_shutdown(outcomes::STOPPED, None);
        }
    }

    fn finish_shutdown(&mut self, outcome: &str, error: Option<ErrorWire>) {
        let Some(shutdown) = self.shutdown.as_mut() else {
            return;
        };
        if !shutdown.replied {
            shutdown.replied = true;
            let _ = shutdown.reply.send(ShutdownOutcome {
                state: self.state.clone(),
                outcome: outcome.to_owned(),
                error,
            });
        }
        self.done = true;
    }

    // -- gate ----------------------------------------------------------------

    fn source_installation(&self, gate: &GateRecord) -> Option<String> {
        gate.source_installation.clone().or_else(|| {
            self.peers
                .iter()
                .find(|peer| peer.endpoint == gate.source_endpoint)
                .map(|peer| peer.installation.clone())
        })
    }

    fn gate_source_label(&self, gate: &GateRecord) -> String {
        self.source_installation(gate)
            .unwrap_or_else(|| gate.source_endpoint.clone())
    }

    fn sync_gate(&mut self, record: Option<GateRecord>) {
        let old = self.gate_record.take();
        match &record {
            None => {
                self.gate = GateState::None;
                self.gate_ctl.retarget_pending = false;
            }
            Some(gate) => {
                if let Some(at_ms) = gate.override_ms {
                    self.gate = GateState::Overridden { at_ms };
                } else {
                    let same_source = old.as_ref().is_some_and(|old| {
                        old.source_installation == gate.source_installation
                            && old.source_endpoint == gate.source_endpoint
                            && old.override_ms.is_none()
                    });
                    if !same_source
                        || matches!(self.gate, GateState::None | GateState::Overridden { .. })
                    {
                        self.gate = GateState::Awaiting {
                            source: self.gate_source_label(gate),
                            since_ms: gate.created_ms,
                            waiting_on: Vec::new(),
                        };
                    }
                }
            }
        }
        self.gate_record = record;
    }

    /// Retargets the gate when its source peer is no longer paired (unpair):
    /// to a paired peer known to share this folder, or to the next paired
    /// peer whose evidence arrives.
    fn check_gate_source(&mut self) {
        let Some(gate) = self.gate_record.clone() else {
            return;
        };
        if gate.override_ms.is_some() {
            return;
        }
        let present = self
            .source_installation(&gate)
            .is_some_and(|installation| self.paired(&installation));
        if present {
            self.gate_ctl.retarget_pending = false;
            return;
        }
        let candidate = self
            .gate_ctl
            .evidence_from
            .iter()
            .find(|installation| self.paired(installation))
            .cloned();
        match candidate {
            Some(installation) => self.retarget(&installation),
            None => self.gate_ctl.retarget_pending = true,
        }
    }

    fn paired(&self, installation: &str) -> bool {
        self.peers
            .iter()
            .any(|peer| peer.installation == installation)
    }

    /// Retargets the gate to `installation` (a catalog write on a helper
    /// thread; the result arrives as `Msg::Catalog`).
    fn retarget(&mut self, installation: &str) {
        if self.gate_ctl.retargeting {
            return;
        }
        let Some(endpoint) = self
            .peers
            .iter()
            .find(|peer| peer.installation == installation)
            .map(|peer| peer.endpoint.clone())
        else {
            return;
        };
        self.gate_ctl.retargeting = true;
        self.gate_ctl.retarget_pending = false;
        self.catalog_write(
            CatalogWrite::RetargetGate(installation.to_owned()),
            CatalogCmd::RetargetGate {
                state: self.state.clone(),
                source_installation: Some(installation.to_owned()),
                source_endpoint: endpoint,
            },
        );
    }

    /// Runs one catalog write on a helper thread and posts its result back
    /// as `Msg::Catalog`: the controller never waits for the catalog, so a
    /// slow catalog write cannot freeze this folder's lifecycle (worker
    /// exits, commands, status; §2.3 Catalog row, RC10; post-integration
    /// re-review, WP2.INT). `CatalogApi::apply` itself is bounded.
    fn catalog_write(&mut self, kind: CatalogWrite, command: CatalogCmd) {
        let catalog = self.deps.catalog.clone();
        let tx = self.tx.clone();
        let spawned = {
            let kind = kind.clone();
            thread::Builder::new()
                .name(format!("folder-{}-catalog", self.state))
                .spawn(move || {
                    let result = catch_unwind(AssertUnwindSafe(|| catalog.apply(command)))
                        .unwrap_or_else(|_| {
                            Err(ErrorWire::new(
                                "CATALOG_UNAVAILABLE",
                                "CATALOG_UNAVAILABLE: the catalog client panicked",
                            ))
                        });
                    let _ = tx.send(Msg::Catalog(kind, result));
                })
        };
        if let Err(error) = spawned {
            let _ = self.tx.send(Msg::Catalog(
                kind,
                Err(ErrorWire::new(
                    "CATALOG_UNAVAILABLE",
                    format!("CATALOG_UNAVAILABLE: cannot start a catalog thread: {error}"),
                )),
            ));
        }
    }

    /// The result of a [`catalog_write`](Self::catalog_write). The gate and
    /// the peers are taken from the latest snapshot (at least as new as the
    /// write's, which a later catalog change may already have passed).
    fn on_catalog_write(
        &mut self,
        kind: CatalogWrite,
        result: Result<Arc<CatalogSnapshot>, ErrorWire>,
    ) {
        match kind {
            CatalogWrite::ClearGate => self.gate_ctl.clearing = false,
            CatalogWrite::RetargetGate(_) => self.gate_ctl.retargeting = false,
        }
        if self.done || self.retired {
            return;
        }
        match (kind, result) {
            (kind, Ok(_)) => {
                let snapshot = self.deps.catalog.snapshot();
                self.peers = snapshot.peers.clone();
                self.sync_gate(snapshot.gates.get(&self.state).cloned());
                if self.gate_record.is_none() {
                    self.gate_ctl = GateCtl::default();
                } else if matches!(kind, CatalogWrite::RetargetGate(_)) {
                    self.check_gate_source();
                }
            }
            (CatalogWrite::ClearGate, Err(error)) => {
                event!(warn, (error.code.as_str()), {
                    "folder": self.state,
                    "detail": "clearing the gate marker failed; retried",
                    "message": error.message,
                });
                self.gate_ctl.clear_retry_at = Some(self.now() + limits::STOP_BLOCKED_RETRY);
            }
            (CatalogWrite::RetargetGate(_), Err(error)) => {
                event!(warn, (error.code.as_str()), {
                    "folder": self.state,
                    "detail": "gate retarget failed",
                    "message": error.message,
                });
                self.gate_ctl.retarget_pending = true;
            }
        }
    }

    fn on_evidence(&mut self, evidence: GateEvidence) {
        let paired = self.paired(&evidence.installation);
        if paired {
            self.gate_ctl
                .evidence_from
                .insert(evidence.installation.clone());
        }
        if self
            .gate_record
            .as_ref()
            .is_none_or(|gate| gate.override_ms.is_some())
        {
            return;
        }
        if self.gate_ctl.retarget_pending && paired {
            self.retarget(&evidence.installation);
        }
        let Some(gate) = self.gate_record.clone() else {
            return;
        };
        if self.source_installation(&gate).as_deref() != Some(evidence.installation.as_str()) {
            return;
        }
        if !matches!(
            self.gate,
            GateState::Awaiting { .. } | GateState::Blocked { .. }
        ) {
            return;
        }
        let source = evidence.installation.clone();
        self.gate = if evidence.caught_up && !evidence.blocked {
            GateState::Satisfied {
                at_ms: evidence.at_ms,
            }
        } else if evidence.caught_up {
            let reason = format!(
                "GATE_BLOCKED: {source} is caught up, but blocked, quarantined or rejected rows from \
                 it remain; Revalidate or MountNow"
            );
            self.observe_error(ErrorWire::new("GATE_BLOCKED", reason.clone()));
            GateState::Blocked { source, reason }
        } else {
            GateState::Awaiting {
                source,
                since_ms: gate.created_ms,
                waiting_on: evidence.waiting_on,
            }
        };
    }

    /// Deletes the gate marker (a catalog write on a helper thread; the
    /// result arrives as `Msg::Catalog`).
    fn clear_gate(&mut self) {
        if self.gate_ctl.clearing {
            return;
        }
        self.gate_ctl.clearing = true;
        self.catalog_write(
            CatalogWrite::ClearGate,
            CatalogCmd::ClearGate {
                state: self.state.clone(),
            },
        );
    }

    // -- mount ---------------------------------------------------------------

    /// Whether the folder has a mount to attach (a path, and the intent on).
    fn mount_requested(&self) -> bool {
        self.record.mount_enabled && self.record.mount.is_some()
    }

    fn want_attach(&self) -> bool {
        self.running.is_some()
            && self.stop.is_none()
            && self.shutdown.is_none()
            && self.worker_client().is_some()
            && self.mount_ctl.known
            && self.record.mount_enabled
            && self.record.mount.is_some()
            && self.gate.allows_attach()
            && self.mount_ctl.attach.is_none()
            && self.mount_ctl.detach.is_none()
            && !self.mount_ctl.hold
            && !self.mount_ctl.user_detached
            && self.mount_ctl.retry_at.is_none()
            && !matches!(
                self.mount.state,
                MountState::Attached { .. }
                    | MountState::Attaching
                    | MountState::Detaching
                    | MountState::Preflight
            )
    }

    fn try_attach(&mut self) {
        let (Some(client), Some(path)) = (self.worker_client(), self.record.mount.clone()) else {
            return;
        };
        let deadline = self.limits().attach_deadline;
        let ticket = self.rpc(
            RpcKind::Attach,
            client,
            ops::MOUNT_ATTACH,
            json!({ "path": path }),
            deadline,
        );
        self.mount_ctl.attach = Some(ticket);
        self.mount = MountStatus {
            intent: MountIntent::Attach,
            path: Some(path),
            state: MountState::Attaching,
        };
    }

    fn on_attach(&mut self, result: Result<Value, CallError>) {
        self.mount_ctl.attach = None;
        self.mount_ctl.stale_heartbeat = true;
        match parse::<MountStatus>(result) {
            Ok(status) if status.state.is_attached() => {
                self.mount = status;
                self.mount_ctl.rearm();
                // Invariant 26: the gate marker goes only after Attached.
                if self.gate_record.is_some() && !self.gate_ctl.clearing {
                    self.clear_gate();
                }
            }
            Ok(status) => {
                let hold = matches!(status.state, MountState::Disabled);
                let error = status.state.error().cloned().unwrap_or_else(|| {
                    ErrorWire::new(
                        "MOUNT_NOT_RUNNING",
                        format!(
                            "MOUNT_NOT_RUNNING: mount-attach answered {}",
                            status.state.name()
                        ),
                    )
                });
                self.mount = status;
                self.attach_failed(error, hold);
            }
            Err(error) => self.attach_failed(error.to_wire(), false),
        }
    }

    fn attach_failed(&mut self, error: ErrorWire, hold: bool) {
        let now = self.now();
        self.mount_ctl.attempt = self.mount_ctl.attempt.saturating_add(1);
        let next_retry_at_ms = if hold || !error.class.retries_automatically() {
            self.mount_ctl.hold = true;
            None
        } else {
            let delay = self.mount_ctl.retry.failure(now);
            self.mount_ctl.retry_at = Some(now + delay);
            Some(self.now_ms().saturating_add(ms(delay)))
        };
        event!(warn, (error.code.as_str()), {
            "folder": self.state,
            "detail": "mount attach failed",
            "attempt": self.mount_ctl.attempt,
            "retry": next_retry_at_ms.is_some(),
        });
        match &mut self.mount.state {
            MountState::AttachFailed {
                next_retry_at_ms: next,
                ..
            } => *next = next_retry_at_ms,
            state => {
                *state = MountState::AttachFailed {
                    error,
                    attempt: self.mount_ctl.attempt,
                    next_retry_at_ms,
                }
            }
        }
    }

    /// The worker's mount status, with the controller's own retry schedule
    /// (the worker does not know when the controller retries).
    fn merged_mount(&self, reported: &MountStatus) -> MountStatus {
        let mut mount = reported.clone();
        if let MountState::AttachFailed {
            next_retry_at_ms, ..
        } = &mut mount.state
        {
            *next_retry_at_ms = if self.mount_ctl.hold {
                None
            } else {
                self.mount_ctl.retry_at.map(|at| {
                    let now = self.now();
                    self.now_ms()
                        .saturating_add(ms(at.saturating_duration_since(now)))
                })
            };
        }
        mount
    }

    fn on_detach(&mut self, result: Result<Value, CallError>) {
        self.mount_ctl.detach = None;
        self.mount_ctl.stale_heartbeat = true;
        match parse::<MountStatus>(result) {
            Ok(status) => self.mount = status,
            Err(error) => {
                let error = error.to_wire();
                self.fail_waiters(Settle::Mount, &error);
            }
        }
    }

    // -- published entry -----------------------------------------------------

    fn entry_state(&self) -> EntryState {
        if self.record.sync_enabled {
            EntryState::Running
        } else {
            EntryState::Paused
        }
    }

    fn maybe_refresh_published(&mut self) {
        let wanted = self.published.entry.is_none()
            || self.published.refresh
            || self
                .published
                .entry
                .as_ref()
                .is_some_and(|entry| entry.state == EntryState::Unavailable);
        if !wanted
            || self.running.is_none()
            || self.stop.is_some()
            || self.shutdown.is_some()
            || self.published.rpc.is_some()
            || self.published.retry_at.is_some()
        {
            return;
        }
        let Some(client) = self.worker_client() else {
            return;
        };
        self.published.refresh = false;
        self.published.requested_gen = self.heartbeat.as_ref().map(|hb| hb.generations.branches);
        let ticket = self.rpc(
            RpcKind::Published,
            client,
            ops::PUBLISHED_SUMMARY,
            json!({}),
            deadlines::PUBLISHED_SUMMARY,
        );
        self.published.rpc = Some(ticket);
    }

    fn on_published(&mut self, result: Result<Value, CallError>) {
        self.published.rpc = None;
        let now_ms = self.now_ms();
        match parse::<PublishedSummary>(result) {
            Ok(summary) => {
                let heartbeat = self.heartbeat.as_deref();
                let entry = PublishedEntry {
                    repo: summary.repo,
                    replica: summary.replica,
                    label: summary.label,
                    branches: summary.branches,
                    state: self.entry_state(),
                    tip: heartbeat
                        .and_then(|hb| hb.tip.clone())
                        .or(Some(summary.tip)),
                    sync_generation: summary
                        .sync_generation
                        .max(heartbeat.map_or(0, |hb| hb.generations.sync)),
                    content: heartbeat.and_then(|hb| hb.content),
                    error: None,
                    updated_at_ms: now_ms,
                };
                self.published.branches_gen = self.published.requested_gen;
                self.published.entry = Some(entry.clone());
                self.deps.published.update(&self.state, Some(entry));
                if let Some(record) = self.published_error.as_mut() {
                    record.resolve(now_ms);
                }
            }
            Err(error) => {
                let wire = ErrorWire::new(
                    "PUBLISHED_SUMMARY_FAILED",
                    format!("PUBLISHED_SUMMARY_FAILED: {error}"),
                );
                let mut entry = self
                    .published
                    .entry
                    .clone()
                    .unwrap_or_else(|| PublishedEntry {
                        repo: self.record.repo.clone(),
                        replica: self.record.device.clone(),
                        label: self.record.label.clone(),
                        ..PublishedEntry::default()
                    });
                entry.state = EntryState::Unavailable;
                entry.error = Some(wire.clone());
                entry.updated_at_ms = now_ms;
                self.published.entry = Some(entry.clone());
                self.deps.published.update(&self.state, Some(entry));
                match self.published_error.as_mut() {
                    Some(record) => record.observe(wire, now_ms),
                    None => {
                        self.published_error = Some(ErrorRecord::new(wire, self.scope(), now_ms))
                    }
                }
                self.published.retry_at = Some(self.now() + limits::PUBLISHED_RETRY);
            }
        }
    }

    fn update_entry_from_heartbeat(&mut self, heartbeat: &WorkerHeartbeat) {
        let Some(entry) = self.published.entry.as_mut() else {
            return;
        };
        if entry.state == EntryState::Unavailable {
            return;
        }
        let tip = heartbeat.tip.clone().or_else(|| entry.tip.clone());
        let content = heartbeat.content.or(entry.content);
        let sync_generation = entry.sync_generation.max(heartbeat.generations.sync);
        if tip == entry.tip && content == entry.content && sync_generation == entry.sync_generation
        {
            return;
        }
        entry.tip = tip;
        entry.content = content;
        entry.sync_generation = sync_generation;
        entry.updated_at_ms = self.deps.clock.now_ms();
        let entry = entry.clone();
        self.deps.published.update(&self.state, Some(entry));
    }

    fn refresh_entry_state(&mut self) {
        let state = self.entry_state();
        let Some(entry) = self.published.entry.as_mut() else {
            return;
        };
        if entry.state == EntryState::Unavailable || entry.state == state {
            return;
        }
        entry.state = state;
        entry.updated_at_ms = self.deps.clock.now_ms();
        let entry = entry.clone();
        self.deps.published.update(&self.state, Some(entry));
    }

    /// The worker is gone (stopped, crashed, restarting): the entry stays
    /// published as `unavailable`, so peers wait for the folder (their pair
    /// reads `WaitingForRemote`, §3.7) instead of reading it as no longer
    /// shared (WP3.INT re-review). It is published again once the worker
    /// runs (`maybe_refresh_published`), and removed when the folder leaves
    /// the catalog.
    fn withdraw_entry(&mut self) {
        self.published.rpc = None;
        self.published.retry_at = None;
        self.published.refresh = false;
        self.published.branches_gen = None;
        let now_ms = self.deps.clock.now_ms();
        if let Some(entry) = self.published.entry.as_mut()
            && entry.state != EntryState::Unavailable
        {
            entry.state = EntryState::Unavailable;
            entry.error = None;
            entry.updated_at_ms = now_ms;
            let entry = entry.clone();
            self.deps.published.update(&self.state, Some(entry));
        }
    }

    fn remove_entry(&mut self) {
        self.published.rpc = None;
        self.published.retry_at = None;
        self.published.refresh = false;
        self.published.branches_gen = None;
        if self.published.entry.take().is_some() {
            self.deps.published.update(&self.state, None);
        }
    }

    // -- RPC results ---------------------------------------------------------

    fn on_rpc(&mut self, ticket: u64, kind: RpcKind, result: Result<Value, CallError>) {
        match kind {
            RpcKind::AdoptHello => {
                if matches!(self.start, Some(StartPlan::Adopt { rpc: Some(t), .. }) if t == ticket)
                {
                    self.on_adopt_hello(result);
                }
            }
            RpcKind::Probe => {
                if self.liveness.probe == Some(ticket) {
                    self.on_probe(result);
                }
            }
            RpcKind::Stop => {
                if self.stop.as_ref().and_then(|plan| plan.rpc) == Some(ticket) {
                    self.on_stop_result(result);
                }
            }
            RpcKind::Attach => {
                if self.mount_ctl.attach == Some(ticket) {
                    self.on_attach(result);
                }
            }
            RpcKind::Detach => {
                if self.mount_ctl.detach == Some(ticket) {
                    self.on_detach(result);
                }
            }
            RpcKind::Published => {
                if self.published.rpc == Some(ticket) {
                    self.on_published(result);
                }
            }
            RpcKind::Revalidate => {
                if self.revalidate == Some(ticket) {
                    self.revalidate = None;
                    if let Err(error) = result {
                        let error = error.to_wire();
                        self.fail_waiters(Settle::Revalidate, &error);
                    }
                }
            }
            RpcKind::LegacyStatus | RpcKind::LegacyStop => {
                if matches!(self.start, Some(StartPlan::Legacy { rpc: Some(t), .. }) if t == ticket)
                {
                    self.on_legacy(kind, result);
                }
            }
        }
    }
}

/// Whether two records describe the same folder configuration, ignoring
/// `desired_running` (which the controller tracks itself).
fn same_folder(a: &FolderRecord, b: &FolderRecord) -> bool {
    a.label == b.label
        && a.repo == b.repo
        && a.device == b.device
        && a.directory == b.directory
        && a.mount == b.mount
        && a.sync_enabled == b.sync_enabled
        && a.mount_enabled == b.mount_enabled
}

/// The watch thread: a dedicated client loops `watch{after_seq, wait_ms}`
/// (§3.3 Liveness) until cancelled.
fn watch_loop(epoch: u64, client: Arc<dyn WorkerClient>, tx: Sender<Msg>, cancel: Cancel) {
    let mut after_seq = 0u64;
    while !cancel.is_cancelled() {
        let request = WatchRequest {
            after_seq,
            wait_ms: deadlines::WATCH_WAIT_MS,
        };
        let payload = serde_json::to_value(request).unwrap_or(Value::Null);
        let result = catch_unwind(AssertUnwindSafe(|| {
            client.call(ops::WATCH, payload, deadlines::WATCH)
        }))
        .unwrap_or_else(|_| {
            Err(CallError::ConnectionLost(
                "watch: the worker client panicked".into(),
            ))
        });
        if cancel.is_cancelled() {
            break;
        }
        match parse::<WorkerHeartbeat>(result) {
            Ok(heartbeat) if heartbeat.seq > after_seq => {
                after_seq = heartbeat.seq;
                if tx.send(Msg::Heartbeat(epoch, Arc::new(heartbeat))).is_err() {
                    break;
                }
            }
            Ok(_) => {
                if cancel.sleep(WATCH_IDLE_PAUSE) {
                    break;
                }
            }
            Err(error) => {
                if tx.send(Msg::WatchFailed(epoch, error)).is_err()
                    || cancel.sleep(WATCH_RETRY_PAUSE)
                {
                    break;
                }
            }
        }
    }
}
