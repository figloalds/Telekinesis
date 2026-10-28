//! The orchestrator daemon: one per data directory (`DATA/orchestrator.lock`).
//! It owns the catalog, drives one [`Controller`] per folder, serves the
//! control endpoint (`endpoint`) and runs the shutdown sequence on
//! `manage shutdown`, SIGINT/SIGTERM or a console close (`signals`).
//!
//! Threads:
//! * the main thread ticks every controller every [`TICK`] (`try_lock`: a
//!   controller busy in a blocking `stop` is skipped) and reconciles: a
//!   Running folder whose record says `desired_running = false` (a
//!   `stop --no-wait`, or a stop that arrived while it was still Starting) is
//!   stopped on a background thread; a backing-off one is stopped at once;
//! * control requests run on their connection's thread. A blocking `stop`
//!   holds only its own folder's lock (`Slot::controller`), never the folder
//!   map, so the other folders keep ticking and answering. `status` reads the
//!   view each tick caches and never waits for a busy controller.
//!
//! A stop of a folder that is still Starting first waits (ticking it) until
//! it runs, then stops it: `Controller::stop` gives `NotRunning` from
//! Starting without stopping anything.
//!
//! Shutdown ([`Daemon::shutdown`], once per daemon; later callers wait for
//! the first one's result), bounded by [`SHUTDOWN_BOUND`] and never changing
//! `desired_running`:
//! 1. every folder `stop(false)` in parallel;
//! 2. at [`FORCE_AT`] at the latest (as soon as every graceful stop
//!    returned), a folder left running gets `stop(true)`, and one whose
//!    graceful stop is still waiting is sent `shutdown {force}` on its link;
//! 3. at [`KILL_AT`] a worker not shown gone is killed: one still Starting
//!    through its controller (`Controller::abort_start`: the child's handle,
//!    no `worker.json` needed), any other by `kill_verified` from
//!    `STATE/worker.json`; a worker neither shown gone nor killed is reported
//!    `stranded` with the reason, never `not_running`;
//! 4. every folder that needed step 2 or 3 logs `FORCED_STOP`; then requests
//!    in flight get what is left of [`SHUTDOWN_BOUND`] (at most 2 s).
//!
//! Before step 1 the peer network closes (spec §7.1: close peers, then stop
//! folders), within [`NETWORK_SHUTDOWN`]; the folder steps keep their times
//! from the sequence's start.
use super::{
    catalog::{Catalog, FolderRecord},
    config::Config,
    endpoint,
    folder::{Controller, FORCE_STOP_WAIT, Launcher, Phase, RealLauncher, STOP_WAIT, StopOutcome},
    network::{self, FolderDirectory, Network},
    process::{self, READY_TIMEOUT},
    signals,
    worker::{RECORD_FILE, WorkerRecord},
    worker_client::{CALL_DEADLINE, WorkerLink},
};
use crate::{
    runtime::Engine,
    util::{
        cancel::Cancel,
        lock::lock,
        log::{self, event},
        time::{Clock, SystemClock},
    },
};
use anyhow::{Context, Result, anyhow, bail, ensure};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    net::TcpListener,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError, TryLockError, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{self, Receiver, TryRecvError},
    },
    thread,
    time::{Duration, Instant},
};

/// The whole shutdown sequence ends within this bound.
pub const SHUTDOWN_BOUND: Duration = Duration::from_secs(30);
/// Graceful stops give way to forcing at this point of the sequence: a
/// forced stop still has [`FORCE_STOP_WAIT`] before the bound.
pub const FORCE_AT: Duration = SHUTDOWN_BOUND.saturating_sub(FORCE_STOP_WAIT);
/// A worker not confirmed gone at this point of the sequence is killed.
pub const KILL_AT: Duration = SHUTDOWN_BOUND.saturating_sub(Duration::from_secs(5));
/// After the kill, how long stop threads get to report.
const KILL_SETTLE: Duration = Duration::from_secs(2);
/// How often the main loop ticks every controller.
pub const TICK: Duration = Duration::from_millis(50);
/// A `stop` of a still-starting folder waits this long for it to run.
pub const START_WAIT: Duration = Duration::from_secs(READY_TIMEOUT.as_secs() + 5);
/// After the sequence, the longest the daemon waits for requests in flight
/// (the `shutdown` reply among them) before exiting; never past
/// [`SHUTDOWN_BOUND`] from the sequence's start.
const REQUEST_DRAIN: Duration = Duration::from_secs(2);
/// How long the shutdown sequence gives the peer network to close.
pub const NETWORK_SHUTDOWN: Duration = Duration::from_secs(2);
/// How long `inspect`, `folder` and the shutdown sequence wait for a
/// controller before falling back to its cached view.
const BRIEF_LOCK: Duration = Duration::from_millis(500);
/// A background stop that left the worker running is retried after this.
const STOP_RETRY: Duration = Duration::from_secs(30);
pub const LOCK_FILE: &str = "orchestrator.lock";
/// Exit code of a second orchestrator on a data directory (`EX_TEMPFAIL`).
pub const EXIT_ALREADY_RUNNING: i32 = 75;

/// Actions that change folders; refused with `ORCHESTRATOR_STOPPING` once
/// shutdown began.
const MUTATIONS: &[&str] = &[
    "create",
    "start",
    "stop",
    "force-stop",
    "retry",
    "pause",
    "resume",
    "configure-mount",
    "folder",
    "reset-pairing-code",
    "pair",
    "unpair",
];
/// Worker ops the `folder` action forwards.
pub const FOLDER_OPS: &[&str] = &[
    "status",
    "conflicts",
    "resolve",
    "preview-version",
    "events",
    "tree",
];

/// One folder: its controller and the view the last tick saw.
struct Slot {
    controller: Mutex<Controller>,
    view: Mutex<Value>,
    /// A stop (action or reconciler) is in progress.
    stopping: AtomicBool,
    /// No background stop before this.
    retry_stop_at: Mutex<Option<Instant>>,
}

impl Slot {
    fn new(controller: Controller) -> Arc<Self> {
        let view = controller.view();
        Arc::new(Self {
            controller: Mutex::new(controller),
            view: Mutex::new(view),
            stopping: AtomicBool::new(false),
            retry_stop_at: Mutex::new(None),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Controller> {
        lock(&self.controller)
    }

    fn try_lock(&self) -> Option<MutexGuard<'_, Controller>> {
        match self.controller.try_lock() {
            Ok(controller) => Some(controller),
            Err(TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
            Err(TryLockError::WouldBlock) => None,
        }
    }

    /// The controller if it is free within [`BRIEF_LOCK`]: a tick holds it
    /// for moments, a stop in progress for seconds (callers then work from
    /// the cached view).
    fn lock_briefly(&self) -> Option<MutexGuard<'_, Controller>> {
        let end = Instant::now() + BRIEF_LOCK;
        loop {
            if let Some(controller) = self.try_lock() {
                return Some(controller);
            }
            if Instant::now() >= end {
                return None;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn refresh(&self, controller: &Controller) {
        *lock(&self.view) = controller.view();
    }

    fn view(&self) -> Value {
        lock(&self.view).clone()
    }

    fn phase_name(&self) -> String {
        lock(&self.view)["phase"]["phase"]
            .as_str()
            .unwrap_or_default()
            .to_owned()
    }

    fn active(&self) -> bool {
        matches!(
            self.phase_name().as_str(),
            "Starting" | "Running" | "Stopping" | "Backoff"
        )
    }

    /// Waits at most `start_wait` (ticking it) for a starting worker to run,
    /// then stops it. The cached view shows `Stopping` meanwhile.
    fn stop(&self, force: bool, start_wait: Duration) -> Result<StopOutcome> {
        let end = Instant::now() + start_wait;
        loop {
            let mut controller = self.lock();
            if !matches!(controller.phase(), Phase::Starting { .. }) || Instant::now() >= end {
                break;
            }
            controller.tick();
            self.refresh(&controller);
            drop(controller);
            thread::sleep(TICK);
        }
        let mut controller = self.lock();
        if matches!(controller.phase(), Phase::Running { .. }) {
            let now = crate::util::time::now_ms();
            let wait = if force { FORCE_STOP_WAIT } else { STOP_WAIT };
            let mut view = controller.view();
            view["phase"] = json!({
                "phase": "Stopping",
                "since_ms": now,
                "force": force,
                "deadline_ms": now + wait.as_millis() as u64,
            });
            *lock(&self.view) = view;
        }
        let outcome = controller.stop(force);
        self.refresh(&controller);
        outcome
    }

    /// Claims the slot's stop, waiting at most `bound` for another to end.
    fn claim_stop(self: &Arc<Self>, bound: Duration) -> Result<StopClaim> {
        let end = Instant::now() + bound;
        while self.stopping.swap(true, Ordering::AcqRel) {
            ensure!(
                Instant::now() < end,
                "FOLDER_BUSY: another stop of this folder is still running"
            );
            thread::sleep(TICK);
        }
        Ok(StopClaim(self.clone()))
    }
}

/// Releases a [`Slot`]'s stop claim.
struct StopClaim(Arc<Slot>);

impl Drop for StopClaim {
    fn drop(&mut self) {
        self.0.stopping.store(false, Ordering::Release);
    }
}

/// The once-per-daemon shutdown result.
#[derive(Default)]
struct Latch {
    state: Mutex<Option<Option<Value>>>,
    done: Condvar,
}

impl Latch {
    /// `true` for the caller that runs the sequence.
    fn begin(&self) -> bool {
        let mut state = lock(&self.state);
        if state.is_none() {
            *state = Some(None);
            true
        } else {
            false
        }
    }

    fn finish(&self, reply: Value) {
        *lock(&self.state) = Some(Some(reply));
        self.done.notify_all();
    }

    fn wait(&self, timeout: Duration) -> Option<Value> {
        let end = Instant::now() + timeout;
        let mut state = lock(&self.state);
        loop {
            if let Some(Some(reply)) = &*state {
                return Some(reply.clone());
            }
            let left = end.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return None;
            }
            state = self
                .done
                .wait_timeout(state, left)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }
}

pub struct Daemon {
    pub config: Config,
    pub config_path: PathBuf,
    pub catalog: Arc<Catalog>,
    /// Cancelled when shutdown begins (an action, a signal).
    pub stopping: Cancel,
    pub installation: String,
    pub started_ms: u64,
    folders: Mutex<BTreeMap<String, Arc<Slot>>>,
    executable: PathBuf,
    launcher: Arc<dyn Launcher>,
    clock: Arc<dyn Clock>,
    /// The control endpoint's `host:port` (set by `endpoint::serve`).
    control: OnceLock<String>,
    /// The bound peer listen address (`host:port`), if listening.
    listen: Option<String>,
    /// The peer listener, bound at startup so a configured port 0 has its
    /// real port in `orchestrator.json`; `start_network` hands it over.
    listener: Mutex<Option<TcpListener>>,
    /// The peer network, once `run` started it.
    network: OnceLock<Arc<Network>>,
    /// Serializes `maximum_running` admission with the start it admits.
    admission: Mutex<()>,
    requests: AtomicUsize,
    latch: Latch,
    /// When the shutdown sequence began.
    shutdown_started: OnceLock<Instant>,
    _lock: File,
}

/// Counts a control request in flight (see [`Daemon::begin_request`]).
pub struct RequestGuard<'a>(&'a AtomicUsize);

impl Drop for RequestGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Runs the orchestrator until `manage shutdown` or a signal. A second one
/// on the same data directory prints `ALREADY_RUNNING` and exits 75 before
/// touching anything but `orchestrator.toml`.
pub fn run(config_path: &Path) -> Result<()> {
    let config_path = std::path::absolute(config_path)?;
    let config = Config::load_or_init(&config_path)?;
    crate::clock::check()?;
    let data = config.data_directory.clone();
    fs::create_dir_all(&data)
        .with_context(|| format!("DATA_DIRECTORY_UNAVAILABLE: {}", data.display()))?;
    let lock_path = data.join(LOCK_FILE);
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("LOCK_UNAVAILABLE: {}", lock_path.display()))?;
    if let Err(error) = try_lock(&lock) {
        eprintln!(
            "ALREADY_RUNNING: another orchestrator holds {}: {error}",
            lock_path.display()
        );
        std::process::exit(EXIT_ALREADY_RUNNING);
    }
    let _log = match log::install_daemon_log(&data) {
        Ok(log) => Some(log),
        Err(error) => {
            eprintln!("LOG_UNAVAILABLE: {error}");
            None
        }
    };
    let daemon = Daemon::new(config, config_path, lock)?;
    // From here a signal is a shutdown, never the default termination.
    signals::install(daemon.stopping.clone())?;
    daemon.start_network()?;
    let discovery = Discovery(data.clone());
    let control = endpoint::serve(daemon.clone())?;
    event!(info, ORCHESTRATOR_STARTED, {
        "pid": std::process::id(),
        "control": control,
        "listen": daemon.listen.clone(),
        "data_directory": data.display().to_string(),
        "folders": daemon.slots().len(),
    });
    if !daemon.stopping.is_cancelled() {
        daemon.autostart();
    }
    while !daemon.stopping.is_cancelled() {
        daemon.tick();
        daemon.stopping.sleep(TICK);
    }
    let reply = daemon.shutdown();
    let left = daemon
        .shutdown_started
        .get()
        .map_or(REQUEST_DRAIN, |started| {
            SHUTDOWN_BOUND.saturating_sub(started.elapsed())
        });
    daemon.drain_requests(left.min(REQUEST_DRAIN));
    drop(discovery);
    event!(info, ORCHESTRATOR_STOPPED, { "folders": reply["folders"].clone() });
    signals::completed();
    Ok(())
}

/// Removes `orchestrator.json` and `orchestrator.token` when dropped.
struct Discovery(PathBuf);

impl Drop for Discovery {
    fn drop(&mut self) {
        for name in [endpoint::DISCOVERY_FILE, endpoint::TOKEN_FILE] {
            let _ = fs::remove_file(self.0.join(name));
        }
    }
}

/// Locks `file` exclusively without waiting.
fn try_lock(file: &File) -> std::io::Result<()> {
    #[cfg(windows)]
    return file.try_lock().map_err(std::io::Error::from);
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: flock(2) on a descriptor the caller owns.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
}

impl Daemon {
    /// Opens the catalog, binds the peer listen address and builds a
    /// stopped controller per folder.
    fn new(config: Config, config_path: PathBuf, lock: File) -> Result<Arc<Self>> {
        let catalog = Catalog::open(&config.data_directory)?;
        let installation = catalog.installation()?;
        let listener = match config.listen() {
            Some(address) => Some(
                TcpListener::bind(address).with_context(|| format!("LISTEN_FAILED: {address}"))?,
            ),
            None => None,
        };
        let listen = listener
            .as_ref()
            .map(|listener| listener.local_addr().map(|a| a.to_string()))
            .transpose()?;
        let daemon = Arc::new(Self {
            catalog,
            config_path,
            stopping: Cancel::new(),
            installation,
            started_ms: crate::util::time::now_ms(),
            folders: Mutex::new(BTreeMap::new()),
            executable: std::env::current_exe().context("EXECUTABLE_UNKNOWN")?,
            launcher: Arc::new(RealLauncher),
            clock: Arc::new(SystemClock),
            control: OnceLock::new(),
            listen,
            listener: Mutex::new(listener),
            network: OnceLock::new(),
            admission: Mutex::new(()),
            requests: AtomicUsize::new(0),
            latch: Latch::default(),
            shutdown_started: OnceLock::new(),
            _lock: lock,
            config,
        });
        for record in daemon.catalog.folders()? {
            daemon.register(record);
        }
        Ok(daemon)
    }

    fn register(&self, record: FolderRecord) -> Arc<Slot> {
        let id = record.id.clone();
        let slot = Slot::new(Controller::new(
            record,
            self.executable.clone(),
            self.config.network.worker_lanes,
            self.launcher.clone(),
            self.clock.clone(),
        ));
        lock(&self.folders).insert(id, slot.clone());
        slot
    }

    fn slots(&self) -> Vec<Arc<Slot>> {
        lock(&self.folders).values().cloned().collect()
    }

    fn slot(&self, id: &str) -> Result<Arc<Slot>> {
        lock(&self.folders)
            .get(id)
            .cloned()
            .with_context(|| format!("FOLDER_NOT_FOUND: no folder {id:?}"))
    }

    /// Starts the peer network on the bound listener.
    fn start_network(self: &Arc<Self>) -> Result<()> {
        let listener = lock(&self.listener).take();
        let network = network::start(
            &self.config,
            self.catalog.clone(),
            Arc::new(Directory(Arc::downgrade(self))),
            self.stopping.clone(),
            listener,
        )?;
        let _ = self.network.set(network);
        Ok(())
    }

    fn network(&self) -> Result<&Arc<Network>> {
        self.network
            .get()
            .context("NETWORK_UNAVAILABLE: the peer network is not running")
    }

    /// The endpoint's address, once (`endpoint::serve`).
    pub fn set_control(&self, address: String) {
        let _ = self.control.set(address);
    }

    pub fn listen(&self) -> Option<&str> {
        self.listen.as_deref()
    }

    /// `orchestrator.json`'s content for the control endpoint `control`.
    pub fn discovery(&self, control: &str) -> Value {
        json!({
            "control": control,
            "installation": self.installation,
            "pid": std::process::id(),
            "version": env!("CARGO_PKG_VERSION"),
            "listen": self.listen,
            "advertise": self.config.advertise(),
            "started_ms": self.started_ms,
            "data_directory": self.config.data_directory,
        })
    }

    pub fn begin_request(&self) -> RequestGuard<'_> {
        self.requests.fetch_add(1, Ordering::AcqRel);
        RequestGuard(&self.requests)
    }

    fn drain_requests(&self, bound: Duration) {
        let end = Instant::now() + bound;
        while self.requests.load(Ordering::Acquire) > 0 && Instant::now() < end {
            thread::sleep(Duration::from_millis(10));
        }
    }

    /// Starts every folder that should run, at most `maximum_running`.
    fn autostart(&self) {
        let _admission = lock(&self.admission);
        let mut running = 0;
        for slot in self.slots() {
            let mut controller = slot.lock();
            if !controller.record().desired_running {
                continue;
            }
            if running >= self.config.maximum_running {
                event!(warn, WORKER_LIMIT, {
                    "folder": controller.record().id.clone(),
                    "maximum_running": self.config.maximum_running,
                });
                continue;
            }
            controller.start();
            slot.refresh(&controller);
            running += 1;
        }
    }

    /// One pass of the main loop over every folder.
    fn tick(self: &Arc<Self>) {
        for slot in self.slots() {
            let Some(mut controller) = slot.try_lock() else {
                continue;
            };
            let unwanted = !controller.record().desired_running;
            if unwanted && matches!(controller.phase(), Phase::Backoff { .. }) {
                // Never blocks from Backoff.
                let _ = controller.stop(false);
            }
            controller.tick();
            slot.refresh(&controller);
            let stop = unwanted && matches!(controller.phase(), Phase::Running { .. });
            drop(controller);
            if stop {
                self.stop_in_background(&slot);
            }
        }
    }

    /// The reconciler's stop of a running folder that should not run.
    fn stop_in_background(self: &Arc<Self>, slot: &Arc<Slot>) {
        if lock(&slot.retry_stop_at).is_some_and(|at| Instant::now() < at) {
            return;
        }
        if slot.stopping.swap(true, Ordering::AcqRel) {
            return;
        }
        let claim = StopClaim(slot.clone());
        let daemon = self.clone();
        let spawned = thread::Builder::new()
            .name("tkfs-folder-stop".into())
            .spawn(move || {
                let slot = claim.0.clone();
                let result = slot.stop(false, START_WAIT);
                let id = slot.lock().record().id.clone();
                match result {
                    Ok(StopOutcome::LeftRunning(reason)) => {
                        event!(warn, FOLDER_STOP_REFUSED, {"folder": id.clone(), "error": reason.clone()});
                        *lock(&slot.retry_stop_at) = Some(Instant::now() + STOP_RETRY);
                        if reason.starts_with("BUSY_VIEW") {
                            let _ = daemon.set_desired(&slot, &id, true);
                        }
                    }
                    Ok(_) => {}
                    Err(error) => {
                        event!(error, FOLDER_STOP_FAILED, {"folder": id, "error": format!("{error:#}")});
                        *lock(&slot.retry_stop_at) = Some(Instant::now() + STOP_RETRY);
                    }
                }
                drop(claim);
            });
        if let Err(error) = spawned {
            event!(error, FOLDER_STOP_FAILED, {"error": error.to_string()});
        }
    }

    /// Writes `desired_running` to the catalog and the controller's record.
    fn set_desired(&self, slot: &Slot, id: &str, running: bool) -> Result<()> {
        self.catalog.set_desired(id, running)?;
        self.sync_record(slot, id)
    }

    /// Copies the catalog's record into the controller.
    fn sync_record(&self, slot: &Slot, id: &str) -> Result<()> {
        let record = self.catalog.resolve(id)?;
        let mut controller = slot.lock();
        controller.set_record(record);
        slot.refresh(&controller);
        Ok(())
    }

    /// `WORKER_LIMIT` unless another worker may start (`except` is not
    /// counted). Call under `admission`.
    fn admit(&self, except: Option<&str>) -> Result<()> {
        let active = lock(&self.folders)
            .iter()
            .filter(|(id, slot)| Some(id.as_str()) != except && slot.active())
            .count();
        ensure!(
            active < self.config.maximum_running,
            "WORKER_LIMIT: {active} workers already run (maximum_running = {})",
            self.config.maximum_running
        );
        Ok(())
    }

    /// Runs one control action (see the module docs of `endpoint`).
    pub fn execute(&self, action: &str, payload: &Value) -> Result<Value> {
        if self.stopping.is_cancelled() && MUTATIONS.contains(&action) {
            bail!("ORCHESTRATOR_STOPPING: the orchestrator is shutting down");
        }
        let selector = || -> Result<FolderRecord> {
            let state = payload["state"]
                .as_str()
                .context("ARGUMENT_REQUIRED: state")?;
            self.catalog.resolve(state)
        };
        match action {
            "status" => Ok(self.snapshot()),
            "list" => Ok(serde_json::to_value(self.catalog.folders()?)?),
            "inspect" => self.inspect(&selector()?),
            "create" => self.create(payload),
            "start" => self.start(&selector()?, false),
            "retry" => self.start(&selector()?, true),
            "stop" => self.stop(
                &selector()?,
                payload["wait"].as_bool().unwrap_or(true),
                false,
            ),
            "force-stop" => self.stop(&selector()?, true, true),
            "pause" | "resume" => {
                let record = selector()?;
                self.catalog.set_paused(&record.id, action == "pause")?;
                self.sync_record(&*self.slot(&record.id)?, &record.id)?;
                Ok(serde_json::to_value(self.catalog.resolve(&record.id)?)?)
            }
            "configure-mount" => self.configure_mount(&selector()?, payload),
            "folder" => self.folder(&selector()?, payload),
            "reset-pairing-code" => Ok(json!({"code": self.network()?.reset_pairing_code()?})),
            "pair" => {
                let endpoint = payload["endpoint"]
                    .as_str()
                    .context("ARGUMENT_REQUIRED: endpoint")?;
                let code = payload["code"]
                    .as_str()
                    .context("ARGUMENT_REQUIRED: code")?;
                let connections = match &payload["connections"] {
                    Value::Null => None,
                    value => Some(
                        value
                            .as_u64()
                            .and_then(|n| usize::try_from(n).ok())
                            .context("INVALID_REQUEST: connections must be a number")?,
                    ),
                };
                let installation = self.network()?.pair(endpoint, code, connections)?;
                Ok(json!({"installation": installation}))
            }
            "unpair" => {
                let installation = payload["installation"]
                    .as_str()
                    .context("ARGUMENT_REQUIRED: installation")?;
                self.network()?.unpair(installation)?;
                Ok(json!({}))
            }
            "peers" => Ok(self.network()?.peers()),
            "remote-list" => {
                let endpoint = payload["endpoint"]
                    .as_str()
                    .context("ARGUMENT_REQUIRED: endpoint")?;
                Ok(Value::Array(self.network()?.remote_list(endpoint)?))
            }
            "shutdown" => {
                self.stopping.cancel();
                Ok(self.shutdown())
            }
            _ => bail!("UNSUPPORTED_ACTION: {action:?}"),
        }
    }

    /// `{record, phase, heartbeat, lanes, sync, worker}`: `worker` is the
    /// worker's `status` reply while it runs, else `null`.
    fn inspect(&self, record: &FolderRecord) -> Result<Value> {
        let slot = self.slot(&record.id)?;
        let (mut view, link) = match slot.lock_briefly() {
            Some(controller) => (controller.view(), controller.link()),
            None => (slot.view(), None),
        };
        let worker = link
            .and_then(|link| link.call_json("status", json!({})).ok())
            .unwrap_or(Value::Null);
        view["sync"] = json!({});
        view["worker"] = worker;
        Ok(view)
    }

    fn create(&self, payload: &Value) -> Result<Value> {
        let label = payload["label"]
            .as_str()
            .context("ARGUMENT_REQUIRED: label")?;
        let repo = payload["repo"]
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(crate::core::id);
        let mount = optional_path(payload, "mount")?;
        let _admission = lock(&self.admission);
        self.admit(None)?;
        let record = self.catalog.create_folder(label, &repo, mount)?;
        // Initialize the store, then release it (`owner.lock`) for the worker.
        let initialized = Engine::open(&record.state, Some(&repo)).and_then(|engine| {
            let result = engine.shutdown();
            drop(engine);
            result
        });
        if let Err(error) = initialized {
            self.catalog.set_desired(&record.id, false)?;
            self.register(self.catalog.resolve(&record.id)?);
            return Err(error.context(format!("FOLDER_INIT_FAILED: {}", record.state.display())));
        }
        let slot = self.register(record.clone());
        let mut controller = slot.lock();
        // A shutdown that began meanwhile may already have listed the
        // folders: a worker started now would escape it.
        ensure!(
            !self.stopping.is_cancelled(),
            "ORCHESTRATOR_STOPPING: the orchestrator is shutting down; {} was created but not started",
            record.label
        );
        controller.start();
        slot.refresh(&controller);
        Ok(serde_json::to_value(record)?)
    }

    /// `start` (or `retry`, which also clears the crash-loop breaker).
    fn start(&self, record: &FolderRecord, retry: bool) -> Result<Value> {
        let slot = self.slot(&record.id)?;
        let _admission = lock(&self.admission);
        if !slot.active() {
            self.admit(Some(&record.id))?;
        }
        ensure!(
            !self.stopping.is_cancelled(),
            "ORCHESTRATOR_STOPPING: the orchestrator is shutting down"
        );
        self.set_desired(&slot, &record.id, true)?;
        let mut controller = slot.lock();
        // Checked again under the lock the shutdown's stop takes: a start
        // after the sequence listed this folder would escape it.
        ensure!(
            !self.stopping.is_cancelled(),
            "ORCHESTRATOR_STOPPING: the orchestrator is shutting down"
        );
        if retry {
            controller.retry();
        } else {
            controller.start();
        }
        slot.refresh(&controller);
        Ok(controller.view())
    }

    /// `stop` / `force-stop`: `desired_running = false`, then (with `wait`)
    /// the stop itself. A refusal (`BUSY_VIEW`) restores `desired_running`
    /// and is the action's error; without `wait` the reconciler stops it.
    fn stop(&self, record: &FolderRecord, wait: bool, force: bool) -> Result<Value> {
        let slot = self.slot(&record.id)?;
        if !wait {
            self.set_desired(&slot, &record.id, false)?;
            let mut view = slot.view();
            view["outcome"] = json!("stopping");
            return Ok(view);
        }
        let _claim = slot.claim_stop(START_WAIT + CALL_DEADLINE + STOP_WAIT)?;
        self.set_desired(&slot, &record.id, false)?;
        let outcome = slot.stop(force, START_WAIT);
        let outcome = match outcome {
            Ok(StopOutcome::LeftRunning(reason)) => {
                if reason.starts_with("BUSY_VIEW") {
                    self.set_desired(&slot, &record.id, true)?;
                }
                return Err(anyhow!(reason));
            }
            Ok(outcome) => outcome,
            Err(error) => return Err(error),
        };
        let mut view = slot.view();
        view["outcome"] = json!(outcome_name(&outcome));
        Ok(view)
    }

    fn configure_mount(&self, record: &FolderRecord, payload: &Value) -> Result<Value> {
        let slot = self.slot(&record.id)?;
        let mount = optional_path(payload, "mount")?;
        let controller = slot.lock();
        ensure!(
            matches!(controller.phase(), Phase::Stopped | Phase::Failed { .. }),
            "FOLDER_RUNNING: stop {} before changing its mount",
            record.label
        );
        self.catalog.set_mount(&record.id, mount)?;
        drop(controller);
        self.sync_record(&slot, &record.id)?;
        Ok(serde_json::to_value(self.catalog.resolve(&record.id)?)?)
    }

    /// Forwards `{op, payload}` to the running worker's control lane.
    fn folder(&self, record: &FolderRecord, payload: &Value) -> Result<Value> {
        let op = payload["op"].as_str().context("ARGUMENT_REQUIRED: op")?;
        ensure!(
            FOLDER_OPS.contains(&op),
            "OP_NOT_ALLOWED: {op} is not forwarded to workers"
        );
        let slot = self.slot(&record.id)?;
        let link = slot
            .lock_briefly()
            .filter(|controller| matches!(controller.phase(), Phase::Running { .. }))
            .and_then(|controller| controller.link())
            .with_context(|| {
                format!("FOLDER_NOT_RUNNING: {} has no running worker", record.label)
            })?;
        let args = match &payload["payload"] {
            Value::Null => json!({}),
            args => args.clone(),
        };
        link.call_json(op, args).map_err(|error| anyhow!("{error}"))
    }

    /// `{daemon, folders: [view + conflicts + error], peers}` from the views
    /// the last tick cached: never waits for a busy controller.
    pub fn snapshot(&self) -> Value {
        let folders: Vec<Value> = self
            .slots()
            .iter()
            .map(|slot| folder_entry(slot.view()))
            .collect();
        json!({
            "daemon": {
                "installation": self.installation,
                "pid": std::process::id(),
                "version": env!("CARGO_PKG_VERSION"),
                "started_ms": self.started_ms,
                "config": self.config_path,
                "data_directory": self.config.data_directory,
                "listen": self.listen,
                "advertise": self.config.advertise(),
                "control": self.control.get(),
                "stopping": self.stopping.is_cancelled(),
            },
            "folders": folders,
            "peers": self.network.get().map_or_else(|| json!([]), |network| network.peers()),
        })
    }

    /// The shutdown sequence (module docs); runs once, a later caller gets
    /// the first one's reply `{"stopped":true,"folders":[{state, label,
    /// outcome, error}]}`.
    pub fn shutdown(&self) -> Value {
        if !self.latch.begin() {
            return self
                .latch
                .wait(SHUTDOWN_BOUND + Duration::from_secs(10))
                .unwrap_or_else(|| json!({"stopped": true, "folders": []}));
        }
        let _ = self.shutdown_started.set(Instant::now());
        self.stopping.cancel();
        event!(info, ORCHESTRATOR_STOPPING, { "folders": self.slots().len() });
        let started = Instant::now();
        if let Some(network) = self.network.get() {
            network.shutdown(NETWORK_SHUTDOWN);
        }
        let folders = shutdown_folders(self.slots(), TIMINGS.after(started.elapsed()));
        let reply = json!({"stopped": true, "folders": folders});
        self.latch.finish(reply.clone());
        reply
    }
}

/// When the steps of the shutdown sequence happen (from its start).
#[derive(Clone, Copy, Debug)]
struct Timings {
    force_at: Duration,
    kill_at: Duration,
    settle: Duration,
}

const TIMINGS: Timings = Timings {
    force_at: FORCE_AT,
    kill_at: KILL_AT,
    settle: KILL_SETTLE,
};

impl Timings {
    /// The same points in time for a sequence whose folder steps begin
    /// `elapsed` after its start (the peer network closed first).
    fn after(self, elapsed: Duration) -> Self {
        Self {
            force_at: self.force_at.saturating_sub(elapsed),
            kill_at: self.kill_at.saturating_sub(elapsed),
            settle: self.settle,
        }
    }
}

/// The daemon's folders as the peer network sees them.
struct Directory(Weak<Daemon>);

impl FolderDirectory for Directory {
    fn link_by_repo(&self, repo: &str) -> Option<Arc<WorkerLink>> {
        let daemon = self.0.upgrade()?;
        daemon
            .catalog
            .folders()
            .ok()?
            .into_iter()
            .filter(|record| record.repo == repo && !record.paused)
            .find_map(|record| {
                let slot = daemon.slot(&record.id).ok()?;
                let controller = slot.lock_briefly()?;
                matches!(controller.phase(), Phase::Running { .. })
                    .then(|| controller.link())
                    .flatten()
            })
    }

    fn published(&self) -> Vec<Value> {
        let Some(daemon) = self.0.upgrade() else {
            return Vec::new();
        };
        daemon
            .catalog
            .folders()
            .unwrap_or_default()
            .into_iter()
            .map(|record| json!({"repo": record.repo, "label": record.label}))
            .collect()
    }
}

/// How a kill of the sequence's step 3 went.
#[derive(Debug)]
enum Kill {
    Killed,
    /// The worker was already gone.
    Gone,
    /// Not killed, and not known to be gone.
    Failed(String),
}

/// One folder of the shutdown sequence.
struct Job {
    slot: Arc<Slot>,
    id: String,
    label: String,
    state: PathBuf,
    link: Option<Arc<WorkerLink>>,
    /// The worker's record when the sequence began.
    record: Option<WorkerRecord>,
    results: Receiver<Result<StopOutcome, String>>,
    outcome: Option<Result<StopOutcome, String>>,
    forced: bool,
    kill: Option<Kill>,
}

impl Job {
    fn poll(&mut self) {
        if self.outcome.is_some() {
            return;
        }
        match self.results.try_recv() {
            Ok(result) => self.outcome = Some(result),
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => {
                self.outcome = Some(Err("STOP_THREAD_LOST: the stop thread ended".into()));
            }
        }
    }

    /// The stop has not shown the worker gone: no result yet, a refusal or
    /// an error, or a folder still starting (its stop found nothing to stop).
    fn unresolved(&self) -> bool {
        match &self.outcome {
            None | Some(Ok(StopOutcome::LeftRunning(_))) | Some(Err(_)) => true,
            Some(Ok(_)) => self.slot.phase_name() == "Starting",
        }
    }

    /// The stop thread confirmed the worker's exit.
    fn confirmed(&self) -> Option<&'static str> {
        match &self.outcome {
            Some(Ok(outcome @ (StopOutcome::Stopped | StopOutcome::Killed))) => {
                Some(outcome_name(outcome))
            }
            _ => None,
        }
    }

    /// `(outcome, error)` for the reply. A worker neither confirmed gone nor
    /// killed is `stranded`, never `not_running`.
    fn result(&self) -> (&'static str, Option<String>) {
        match &self.kill {
            Some(Kill::Killed) => ("killed", None),
            Some(Kill::Gone) => (self.confirmed().unwrap_or("stopped"), None),
            Some(Kill::Failed(error)) => match self.confirmed() {
                Some(outcome) => (outcome, None),
                None => ("stranded", Some(error.clone())),
            },
            None => match &self.outcome {
                Some(Ok(StopOutcome::LeftRunning(reason))) => ("stranded", Some(reason.clone())),
                Some(Ok(outcome)) => (outcome_name(outcome), None),
                Some(Err(error)) => ("stranded", Some(error.clone())),
                None => ("stranded", Some("STOP_UNCONFIRMED".into())),
            },
        }
    }

    /// Step 3: a starting worker is aborted through its controller, any
    /// other killed by its verified record.
    fn kill(&self) -> Kill {
        if self.slot.phase_name() == "Starting" {
            let Some(mut controller) = self.slot.lock_briefly() else {
                return Kill::Failed("FOLDER_BUSY: the controller is held by a stop".into());
            };
            if matches!(controller.phase(), Phase::Starting { .. }) {
                let aborted = controller.abort_start();
                self.slot.refresh(&controller);
                return match aborted {
                    Ok(true) => Kill::Killed,
                    Ok(false) => Kill::Gone,
                    Err(error) => Kill::Failed(format!("{error:#}")),
                };
            }
        }
        let Some(record) = read_record(&self.state).or_else(|| self.record.clone()) else {
            return Kill::Failed("WORKER_RECORD_MISSING: no worker.json to kill by".into());
        };
        match process::kill_verified(&record) {
            Ok(true) => Kill::Killed,
            Ok(false) if process_alive(record.pid) => Kill::Failed(format!(
                "WORKER_KILL_REFUSED: pid {} is alive but does not match its record",
                record.pid
            )),
            Ok(false) => Kill::Gone,
            Err(error) => Kill::Failed(format!("{error:#}")),
        }
    }
}

/// Whether `pid` names a live process (conservative: a recycled pid counts).
fn process_alive(pid: u32) -> bool {
    process::ObservedProcess::open(pid)
        .is_ok_and(|process| process.exited().is_ok_and(|exited| !exited))
}

/// Runs `slot.stop(force, start_wait)` on a thread; its result arrives on
/// the returned channel.
fn stop_thread(
    slot: Arc<Slot>,
    force: bool,
    start_wait: Duration,
) -> Receiver<Result<StopOutcome, String>> {
    let (sender, results) = mpsc::channel();
    let failed = sender.clone();
    let spawned = thread::Builder::new()
        .name("tkfs-shutdown-stop".into())
        .spawn(move || {
            let result = slot.stop(force, start_wait).map_err(|e| format!("{e:#}"));
            let _ = sender.send(result);
        });
    if let Err(error) = spawned {
        let _ = failed.send(Err(format!("STOP_THREAD_FAILED: {error}")));
    }
    results
}

/// Polls `jobs` until `done` holds for each, or `until`.
fn collect(jobs: &mut [Job], until: Instant, done: impl Fn(&Job) -> bool) {
    loop {
        jobs.iter_mut().for_each(Job::poll);
        if jobs.iter().all(&done) || Instant::now() >= until {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn shutdown_folders(slots: Vec<Arc<Slot>>, timings: Timings) -> Vec<Value> {
    let started = Instant::now();
    // 1. Every folder stops gracefully, in parallel.
    let mut jobs: Vec<Job> = slots
        .into_iter()
        .map(|slot| {
            let (id, label, state, link) = {
                let controller = slot.lock_briefly();
                let record = controller
                    .as_ref()
                    .map(|c| c.record().clone())
                    .unwrap_or_else(|| record_from_view(&slot.view()));
                (
                    record.id,
                    record.label,
                    record.state,
                    controller.and_then(|c| c.link()),
                )
            };
            let record = read_record(&state);
            let results = stop_thread(slot.clone(), false, timings.force_at);
            Job {
                slot,
                id,
                label,
                state,
                link,
                record,
                results,
                outcome: None,
                forced: false,
                kill: None,
            }
        })
        .collect();
    collect(&mut jobs, started + timings.force_at, |job| {
        job.outcome.is_some()
    });
    // 2. Force what is left: `stop(true)` where the graceful stop ended
    // without the worker gone (it waits for a starting one to run), and
    // `shutdown {force}` on the link where it is still waiting.
    let kill_at = started + timings.kill_at;
    for job in &mut jobs {
        if !job.unresolved() {
            continue;
        }
        job.forced = true;
        if job.outcome.is_some() {
            job.outcome = None;
            let wait = kill_at.saturating_duration_since(Instant::now());
            job.results = stop_thread(job.slot.clone(), true, wait);
        } else if let Some(link) = job.link.clone() {
            let deadline = kill_at.saturating_duration_since(Instant::now());
            let _ = thread::Builder::new()
                .name("tkfs-shutdown-force".into())
                .spawn(move || {
                    let _ = link.call("shutdown", json!({"force": true}), Vec::new(), deadline);
                });
        }
    }
    collect(&mut jobs, kill_at, |job| !job.unresolved());
    // 3. Kill every worker not shown gone, a starting one included.
    let mut killing = false;
    for job in &mut jobs {
        if job.unresolved() {
            job.forced = true;
            job.kill = Some(job.kill());
            killing = true;
        }
    }
    if killing {
        collect(&mut jobs, Instant::now() + timings.settle, |job| {
            job.kill.is_none() || job.outcome.is_some()
        });
    }
    let elapsed_ms = started.elapsed().as_millis() as u64;
    jobs.iter()
        .map(|job| {
            let (outcome, error) = job.result();
            if job.forced {
                event!(warn, FORCED_STOP, {
                    "folder": job.id.clone(),
                    "label": job.label.clone(),
                    "outcome": outcome,
                    "error": error.clone(),
                    "elapsed_ms": elapsed_ms,
                });
            }
            json!({
                "state": job.id,
                "label": job.label,
                "outcome": outcome,
                "error": error,
            })
        })
        .collect()
}

fn record_from_view(view: &Value) -> FolderRecord {
    serde_json::from_value(view["record"].clone()).unwrap_or(FolderRecord {
        id: String::new(),
        label: String::new(),
        state: PathBuf::new(),
        repo: String::new(),
        mount: None,
        desired_running: false,
        paused: false,
        created_ms: 0,
    })
}

/// `STATE/worker.json`, if readable.
fn read_record(state: &Path) -> Option<WorkerRecord> {
    serde_json::from_slice(&fs::read(state.join(RECORD_FILE)).ok()?).ok()
}

fn outcome_name(outcome: &StopOutcome) -> &'static str {
    match outcome {
        StopOutcome::Stopped => "stopped",
        StopOutcome::LeftRunning(_) => "left_running",
        StopOutcome::Killed => "killed",
        StopOutcome::NotRunning => "not_running",
    }
}

/// A status folder entry: the view plus `conflicts` (from the heartbeat)
/// and `error` (the phase's, else the worker's).
fn folder_entry(mut view: Value) -> Value {
    let conflicts = view["heartbeat"]["conflicts"].as_u64().unwrap_or(0);
    let phase = &view["phase"];
    let error = match phase["phase"].as_str() {
        Some("Backoff") => phase["error"].clone(),
        Some("Failed") => json!(format!(
            "{}: {}",
            phase["code"].as_str().unwrap_or_default(),
            phase["message"].as_str().unwrap_or_default()
        )),
        _ => view["heartbeat"]["error"].clone(),
    };
    view["conflicts"] = json!(conflicts);
    view["error"] = error;
    view
}

/// `payload[key]` as an absolute path; `null`/absent is `None`.
fn optional_path(payload: &Value, key: &str) -> Result<Option<PathBuf>> {
    match &payload[key] {
        Value::Null => Ok(None),
        Value::String(path) => {
            let path = PathBuf::from(path);
            ensure!(
                path.is_absolute(),
                "INVALID_REQUEST: {key} must be an absolute path"
            );
            Ok(Some(path))
        }
        _ => bail!("INVALID_REQUEST: {key} must be a path"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        model::orchestrator::CallError,
        orchestrator::{
            process::{LaunchSpec, ProcessEvent, WorkerProcess},
            worker::Ready,
        },
    };
    use std::sync::mpsc::Sender;

    #[test]
    fn the_sequence_forces_kills_and_drains_within_the_bound() {
        assert_eq!(FORCE_AT, Duration::from_secs(15));
        assert!(KILL_AT > FORCE_AT);
        // The network closes before the folders, well before forcing.
        assert!(NETWORK_SHUTDOWN < FORCE_AT);
        let shifted = TIMINGS.after(NETWORK_SHUTDOWN);
        assert_eq!(shifted.kill_at + NETWORK_SHUTDOWN, KILL_AT);
        assert_eq!(shifted.force_at + NETWORK_SHUTDOWN, FORCE_AT);
        // The kill, the settle and the longest drain all end within the bound.
        assert!(KILL_AT + KILL_SETTLE + REQUEST_DRAIN <= SHUTDOWN_BOUND);
    }

    /// Spawns workers that never print Ready; kills are recorded (and
    /// succeed).
    #[derive(Default)]
    struct NeverReady {
        kills: Mutex<Vec<u32>>,
    }

    impl Launcher for NeverReady {
        fn spawn(&self, _: &LaunchSpec, sink: Sender<ProcessEvent>) -> Result<WorkerProcess> {
            // Kept open: the worker never exits on its own.
            std::mem::forget(sink);
            Ok(WorkerProcess::detached(PID))
        }

        fn adopt(
            &self,
            _: &Path,
            _: Sender<ProcessEvent>,
        ) -> Result<Option<(WorkerRecord, WorkerProcess)>> {
            Ok(None)
        }

        fn connect(&self, _: &Ready, _: usize) -> Result<Arc<WorkerLink>, CallError> {
            unreachable!("never Ready")
        }

        fn kill_verified(&self, record: &WorkerRecord) -> Result<bool> {
            lock(&self.kills).push(record.pid);
            Ok(true)
        }
    }

    const PID: u32 = 4242;

    fn record(id: &str, state: &Path) -> FolderRecord {
        FolderRecord {
            id: id.into(),
            label: id.into(),
            state: state.to_owned(),
            repo: "repo".into(),
            mount: None,
            desired_running: true,
            paused: false,
            created_ms: 1,
        }
    }

    fn slot(launcher: &Arc<NeverReady>, record: FolderRecord, start: bool) -> Arc<Slot> {
        let mut controller = Controller::new(
            record,
            PathBuf::from("tkfs-test"),
            1,
            launcher.clone(),
            Arc::new(SystemClock),
        );
        if start {
            controller.start();
        }
        Slot::new(controller)
    }

    fn worker_record(pid: u32) -> WorkerRecord {
        WorkerRecord {
            format: 1,
            pid,
            start_ms: 1,
            image: PathBuf::from("tkfs-test"),
            version: "test".into(),
            instance: "instance".into(),
            address: "127.0.0.1:1".into(),
            ipc: 1,
        }
    }

    #[test]
    fn a_worker_still_starting_at_the_kill_is_killed_or_stranded_never_not_running() {
        let launcher = Arc::new(NeverReady::default());
        let root = tempfile::tempdir().unwrap();
        // Starting, with the record a worker writes before Ready.
        let recorded = root.path().join("recorded");
        fs::create_dir_all(&recorded).unwrap();
        fs::write(
            recorded.join(RECORD_FILE),
            serde_json::to_vec(&worker_record(PID)).unwrap(),
        )
        .unwrap();
        // Starting, hung before writing any record, with no handle that can
        // kill it (a detached stand-in).
        let unrecorded = root.path().join("unrecorded");
        let slots = vec![
            slot(&launcher, record("recorded", &recorded), true),
            slot(&launcher, record("unrecorded", &unrecorded), true),
            slot(&launcher, record("stopped", &root.path().join("s")), false),
        ];
        let timings = Timings {
            force_at: Duration::from_millis(100),
            kill_at: Duration::from_millis(300),
            settle: Duration::from_millis(100),
        };
        let started = Instant::now();
        let folders = shutdown_folders(slots.clone(), timings);
        assert!(started.elapsed() < Duration::from_secs(5));
        let outcome = |id: &str| {
            folders
                .iter()
                .find(|folder| folder["state"] == id)
                .unwrap()
                .clone()
        };
        assert_eq!(outcome("recorded")["outcome"], "killed", "{folders:?}");
        assert_eq!(lock(&launcher.kills).as_slice(), &[PID]);
        assert_eq!(slots[0].lock().phase(), &Phase::Stopped);
        let stranded = outcome("unrecorded");
        assert_eq!(stranded["outcome"], "stranded", "{folders:?}");
        assert!(
            stranded["error"]
                .as_str()
                .is_some_and(|error| error.contains("PROCESS_DETACHED")),
            "{stranded}"
        );
        assert_eq!(outcome("stopped")["outcome"], "not_running");
        assert!(outcome("stopped")["error"].is_null());
    }

    #[test]
    fn a_killed_or_gone_worker_is_never_reported_running_and_a_failed_kill_is_stranded() {
        let launcher = Arc::new(NeverReady::default());
        let root = tempfile::tempdir().unwrap();
        let slot = slot(&launcher, record("f", root.path()), false);
        let (_sender, results) = mpsc::channel();
        let mut job = Job {
            slot,
            id: "f".into(),
            label: "f".into(),
            state: root.path().to_owned(),
            link: None,
            record: None,
            results,
            outcome: Some(Ok(StopOutcome::NotRunning)),
            forced: true,
            kill: Some(Kill::Failed("WORKER_KILL_FAILED: denied".into())),
        };
        assert_eq!(
            job.result(),
            ("stranded", Some("WORKER_KILL_FAILED: denied".into()))
        );
        job.outcome = Some(Ok(StopOutcome::Stopped));
        assert_eq!(job.result(), ("stopped", None));
        job.outcome = Some(Ok(StopOutcome::LeftRunning("BUSY_VIEW: 1".into())));
        job.kill = Some(Kill::Gone);
        assert_eq!(job.result(), ("stopped", None));
        job.kill = Some(Kill::Killed);
        assert_eq!(job.result(), ("killed", None));
        job.kill = None;
        job.outcome = None;
        assert_eq!(job.result().0, "stranded");
    }

    /// A daemon on `root` that never listens for peers.
    fn daemon(root: &Path) -> Arc<Daemon> {
        let path = root.join("orchestrator.toml");
        let mut config = Config::initialize(&path).unwrap();
        config.network.listen = String::new();
        fs::create_dir_all(&config.data_directory).unwrap();
        let lock = File::create(config.data_directory.join(LOCK_FILE)).unwrap();
        Daemon::new(config, path, lock).unwrap()
    }

    #[test]
    fn starts_are_refused_once_shutdown_began() {
        let root = tempfile::tempdir().unwrap();
        let daemon = daemon(root.path());
        let record = daemon
            .catalog
            .create_folder("Project", "repo", None)
            .unwrap();
        daemon.catalog.set_desired(&record.id, false).unwrap();
        let record = daemon.catalog.resolve(&record.id).unwrap();
        let slot = daemon.register(record.clone());
        daemon.stopping.cancel();
        for retry in [false, true] {
            let error = format!("{:#}", daemon.start(&record, retry).unwrap_err());
            assert!(error.starts_with("ORCHESTRATOR_STOPPING"), "{error}");
        }
        assert_eq!(slot.lock().phase(), &Phase::Stopped);
        assert!(!daemon.catalog.resolve(&record.id).unwrap().desired_running);
        // `create` registers the folder but does not start it.
        let error = format!(
            "{:#}",
            daemon.create(&json!({"label": "Other"})).unwrap_err()
        );
        assert!(error.starts_with("ORCHESTRATOR_STOPPING"), "{error}");
        let other = daemon.catalog.resolve("Other").unwrap();
        assert_eq!(
            daemon.slot(&other.id).unwrap().lock().phase(),
            &Phase::Stopped
        );
        // The action itself is refused on arrival.
        let error = format!(
            "{:#}",
            daemon
                .execute("start", &json!({"state": "Project"}))
                .unwrap_err()
        );
        assert!(error.starts_with("ORCHESTRATOR_STOPPING"), "{error}");
    }

    #[test]
    fn folder_entries_carry_conflicts_and_the_phase_error() {
        let entry = folder_entry(json!({
            "record": {},
            "phase": {"phase": "Failed", "code": "CRASH_LOOP", "message": "5 failures"},
            "heartbeat": {"conflicts": 3, "error": null},
        }));
        assert_eq!(entry["conflicts"], 3);
        assert_eq!(entry["error"], "CRASH_LOOP: 5 failures");
        let entry = folder_entry(json!({
            "phase": {"phase": "Backoff", "error": "WORKER_EXITED: exit code 73"},
            "heartbeat": null,
        }));
        assert_eq!(entry["conflicts"], 0);
        assert_eq!(entry["error"], "WORKER_EXITED: exit code 73");
        let entry = folder_entry(json!({
            "phase": {"phase": "Running"},
            "heartbeat": {"conflicts": 0, "error": "SEAL_FAILED"},
        }));
        assert_eq!(entry["error"], "SEAL_FAILED");
    }

    #[test]
    fn mount_paths_must_be_absolute() {
        assert_eq!(optional_path(&json!({}), "mount").unwrap(), None);
        assert!(optional_path(&json!({"mount": "relative"}), "mount").is_err());
        assert!(optional_path(&json!({"mount": 3}), "mount").is_err());
        let absolute = std::env::temp_dir().join("m");
        assert_eq!(
            optional_path(&json!({"mount": absolute}), "mount").unwrap(),
            Some(absolute)
        );
    }

    #[test]
    fn the_latch_runs_once_and_shares_its_result() {
        let latch = Latch::default();
        assert!(latch.begin());
        assert!(!latch.begin());
        assert_eq!(latch.wait(Duration::from_millis(10)), None);
        latch.finish(json!({"stopped": true}));
        assert_eq!(
            latch.wait(Duration::from_millis(10)),
            Some(json!({"stopped": true}))
        );
    }
}
