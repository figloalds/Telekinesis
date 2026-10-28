//! One folder's worker, as the daemon sees it: a state machine the daemon
//! drives by calling [`Controller::tick`] (every ~50 ms, from one thread).
//!
//! * [`Controller::start`] adopts the worker an earlier daemon left running
//!   (`process::adopt`), else spawns one; its [`ProcessEvent`]s arrive on a
//!   channel that `tick` drains. Ready → a [`WorkerLink`] → `Running`.
//! * Liveness: a watch thread per running worker long-polls `watch` and sends
//!   every result over a channel. No heartbeat for [`HEARTBEAT_LOST`], or
//!   [`FAILED_WATCHES`] failed watches in a row, or a closed link, makes
//!   `tick` probe `hello` ([`PROBE_DEADLINE`]); a failed probe kills the
//!   worker (`kill_verified`) and counts as a failure.
//! * Failures (an exit while starting or running, a lost heartbeat, a failed
//!   connect, spawn or adoption, no Ready line in time) back off by
//!   [`backoff_delay`]; [`BREAKER_FAILURES`] within [`BREAKER_WINDOW`] trip
//!   the breaker: `Failed{CRASH_LOOP}` until [`Controller::retry`]. A stop is
//!   never a failure.
//! * Every timer runs on `clock.instant()`; `clock.now_ms()` only fills the
//!   `*_ms` fields of [`Phase`] for display, so a wall-clock step changes no
//!   timing.
//!
//! Dropping a controller never kills its worker: the next daemon adopts it.
use super::{
    catalog::FolderRecord,
    process::{self, LaunchSpec, ProcessEvent, READY_TIMEOUT, WorkerProcess},
    worker::{RECORD_FILE, Ready, TOKEN_FILE, WorkerRecord},
    worker_client::{Watch, WorkerLink},
};
use crate::{
    model::{errors::ErrorWire, orchestrator::CallError},
    util::{log::event, time::Clock},
};
use anyhow::Result;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, Sender, TryRecvError},
    },
    thread,
    time::{Duration, Instant},
};

/// A running worker without a heartbeat this long is probed.
pub const HEARTBEAT_LOST: Duration = Duration::from_secs(8);
/// This many failed watches in a row also trigger the probe.
pub const FAILED_WATCHES: u32 = 3;
/// How long `stop` waits for the worker to exit after `shutdown`…
pub const STOP_WAIT: Duration = Duration::from_secs(13);
/// …and after `shutdown {force}`, before killing it.
pub const FORCE_STOP_WAIT: Duration = Duration::from_secs(15);
/// The longest backoff before jitter.
pub const BACKOFF_CAP: Duration = Duration::from_secs(300);
/// This many failures within [`BREAKER_WINDOW`] trip the breaker.
pub const BREAKER_FAILURES: usize = 5;
pub const BREAKER_WINDOW: Duration = Duration::from_secs(600);
/// Deadline of the `hello` liveness probe.
pub const PROBE_DEADLINE: Duration = Duration::from_secs(2);
/// Jitter bound of a sampled backoff (±20 %).
const JITTER: f64 = 0.2;
/// After a failed watch the watch thread pauses this long (real time).
const WATCH_RETRY: Duration = Duration::from_millis(500);
/// `stop` re-reads the clock at least this often while waiting for an exit.
const STOP_POLL: Duration = Duration::from_millis(50);

/// The backoff after `attempt` failures in a row: `1 s · 2^(attempt−1)`
/// capped at [`BACKOFF_CAP`], then `× (1 + jitter)` with `jitter` clamped to
/// `−0.2..=0.2` (NaN → 0), rounded to milliseconds.
pub fn backoff_delay(attempt: u32, jitter: f64) -> Duration {
    let exponent = attempt.saturating_sub(1).min(31);
    let cap_ms = duration_ms(BACKOFF_CAP);
    let nominal_ms = (1000u64 << exponent).min(cap_ms);
    let jitter = if jitter.is_finite() {
        jitter.clamp(-JITTER, JITTER)
    } else {
        0.0
    };
    Duration::from_millis((nominal_ms as f64 * (1.0 + jitter)).round() as u64)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "phase")]
pub enum Phase {
    Stopped,
    Starting {
        since_ms: u64,
        attempt: u32,
        adopting: bool,
    },
    Running {
        since_ms: u64,
        instance: String,
        pid: u32,
        address: String,
        adopted: bool,
    },
    Stopping {
        since_ms: u64,
        force: bool,
        deadline_ms: u64,
    },
    /// `attempt` failures in a row; the next start is attempt `attempt + 1`.
    Backoff {
        until_ms: u64,
        attempt: u32,
        error: String,
    },
    Failed {
        since_ms: u64,
        code: String,
        message: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum StopOutcome {
    Stopped,
    /// The worker refused (`BUSY_VIEW…`), did not exit in time
    /// (`STOP_TIMEOUT…`) or could not be killed safely; it is still running.
    LeftRunning(String),
    Killed,
    NotRunning,
}

/// How a controller starts, reaches and kills workers.
pub trait Launcher: Send + Sync {
    fn spawn(&self, spec: &LaunchSpec, sink: Sender<ProcessEvent>) -> Result<WorkerProcess>;
    fn adopt(
        &self,
        state: &Path,
        sink: Sender<ProcessEvent>,
    ) -> Result<Option<(WorkerRecord, WorkerProcess)>>;
    fn connect(&self, ready: &Ready, lanes: usize) -> Result<Arc<WorkerLink>, CallError>;
    /// Kills the recorded worker only while it is still that process
    /// ([`process::kill_verified`]); overridden by test launchers.
    fn kill_verified(&self, record: &WorkerRecord) -> Result<bool> {
        process::kill_verified(record)
    }
}

/// Real processes and loopback links.
pub struct RealLauncher;

impl Launcher for RealLauncher {
    fn spawn(&self, spec: &LaunchSpec, sink: Sender<ProcessEvent>) -> Result<WorkerProcess> {
        process::spawn(spec, sink)
    }

    fn adopt(
        &self,
        state: &Path,
        sink: Sender<ProcessEvent>,
    ) -> Result<Option<(WorkerRecord, WorkerProcess)>> {
        process::adopt(state, sink)
    }

    fn connect(&self, ready: &Ready, lanes: usize) -> Result<Arc<WorkerLink>, CallError> {
        WorkerLink::connect(ready, lanes)
    }
}

/// The background long poll of one running worker.
struct WatchThread {
    results: Receiver<Result<Watch, CallError>>,
    stop: Arc<AtomicBool>,
}

impl WatchThread {
    /// Loops `link.watch(after)` and sends every result; ends once the stop
    /// flag is set, the link is closed or the receiver is gone.
    fn start(link: Arc<WorkerLink>) -> std::io::Result<Self> {
        let (sender, results) = mpsc::channel();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        thread::Builder::new()
            .name("tkfs-folder-watch".into())
            .spawn(move || {
                let mut after = 0;
                while !stopped.load(Ordering::Acquire) {
                    let result = link.watch(after);
                    let failed = result.is_err();
                    if let Ok(watch) = &result {
                        after = watch.seq;
                    }
                    if sender.send(result).is_err() || (failed && !link.alive()) {
                        return;
                    }
                    if failed && !stopped.load(Ordering::Acquire) {
                        thread::sleep(WATCH_RETRY);
                    }
                }
            })?;
        Ok(Self { results, stop })
    }
}

impl Drop for WatchThread {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
    }
}

pub struct Controller {
    record: FolderRecord,
    phase: Phase,
    launcher: Arc<dyn Launcher>,
    clock: Arc<dyn Clock>,
    executable: PathBuf,
    lanes: usize,
    /// A fixed backoff jitter (tests); random in ±0.2 otherwise.
    jitter: Option<f64>,
    /// The current worker, its kill record (once known) and its events.
    process: Option<WorkerProcess>,
    worker: Option<WorkerRecord>,
    events: Option<Receiver<ProcessEvent>>,
    /// The fatal line of the current worker, reported with its exit.
    fatal: Option<String>,
    /// `shutdown` was accepted: the worker is exiting. Its exit is a stop,
    /// not a failure, and liveness no longer probes or kills it.
    stop_requested: bool,
    link: Option<Arc<WorkerLink>>,
    watch: Option<WatchThread>,
    heartbeat: Option<Value>,
    last_beat: Instant,
    watch_errors: u32,
    /// When the current Starting or Running phase began.
    phase_start: Instant,
    backoff_until: Instant,
    /// Failures in a row (reset by a start from Stopped or Failed, by
    /// `retry`, and by running for [`BREAKER_WINDOW`]).
    streak: u32,
    failures: VecDeque<Instant>,
}

impl Controller {
    pub fn new(
        record: FolderRecord,
        executable: PathBuf,
        lanes: usize,
        launcher: Arc<dyn Launcher>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let now = clock.instant();
        Self {
            record,
            phase: Phase::Stopped,
            launcher,
            clock,
            executable,
            lanes,
            jitter: None,
            process: None,
            worker: None,
            events: None,
            fatal: None,
            stop_requested: false,
            link: None,
            watch: None,
            heartbeat: None,
            last_beat: now,
            watch_errors: 0,
            phase_start: now,
            backoff_until: now,
            streak: 0,
            failures: VecDeque::new(),
        }
    }

    /// Uses `jitter` (clamped to ±0.2) for every backoff instead of a random
    /// one.
    pub fn with_jitter(mut self, jitter: f64) -> Self {
        self.jitter = Some(jitter);
        self
    }

    pub fn record(&self) -> &FolderRecord {
        &self.record
    }

    /// Replaces the catalog record; a running worker keeps running.
    pub fn set_record(&mut self, record: FolderRecord) {
        self.record = record;
    }

    pub fn phase(&self) -> &Phase {
        &self.phase
    }

    /// The running worker's link.
    pub fn link(&self) -> Option<Arc<WorkerLink>> {
        self.link.clone()
    }

    /// Stopped, Failed or Backoff → Starting: adopts the worker an earlier
    /// daemon left running, else spawns one. A no-op in any other phase.
    pub fn start(&mut self) {
        match self.phase {
            Phase::Stopped | Phase::Failed { .. } => self.streak = 0,
            Phase::Backoff { .. } => {}
            _ => return,
        }
        self.teardown();
        let attempt = self.streak.saturating_add(1);
        self.phase_start = self.clock.instant();
        self.fatal = None;
        self.stop_requested = false;
        let (sink, events) = mpsc::channel();
        match self.launcher.adopt(&self.record.state, sink.clone()) {
            Ok(Some((worker, process))) => {
                self.phase = self.starting(attempt, true);
                self.worker = Some(worker);
                self.process = Some(process);
                self.events = Some(events);
                // Linked on the next tick: `start` never dials.
                return;
            }
            Ok(None) => {}
            Err(error) => {
                // The store may still be held by a worker this daemon cannot
                // see: spawning another would only collide with it.
                self.phase = self.starting(attempt, true);
                self.fail(format!("WORKER_ADOPT_FAILED: {error:#}"));
                return;
            }
        }
        let spec = LaunchSpec {
            executable: self.executable.clone(),
            state: self.record.state.clone(),
            mount: self.record.mount.clone(),
        };
        self.phase = self.starting(attempt, false);
        match self.launcher.spawn(&spec, sink) {
            Ok(process) => {
                self.process = Some(process);
                self.events = Some(events);
            }
            Err(error) => self.fail(format!("{error:#}")),
        }
    }

    /// Clears the failure history (and so the breaker), then starts.
    pub fn retry(&mut self) {
        if matches!(
            self.phase,
            Phase::Stopped | Phase::Failed { .. } | Phase::Backoff { .. }
        ) {
            self.failures.clear();
            self.teardown();
            self.phase = Phase::Stopped;
            self.start();
        }
    }

    /// Asks the running worker to shut down and waits for its exit, at most
    /// [`STOP_WAIT`] (or [`FORCE_STOP_WAIT`] with `force`, after which it is
    /// killed). `BUSY_VIEW` (writable handles open) and a timeout without
    /// `force` leave it running. A backing-off folder is just stopped
    /// (`NotRunning`); any other phase but Running gives `NotRunning`.
    pub fn stop(&mut self, force: bool) -> Result<StopOutcome> {
        match self.phase {
            Phase::Running { .. } => {}
            Phase::Backoff { .. } => {
                self.teardown();
                self.phase = Phase::Stopped;
                return Ok(StopOutcome::NotRunning);
            }
            _ => return Ok(StopOutcome::NotRunning),
        }
        let Some(link) = self.link.clone() else {
            return Ok(StopOutcome::NotRunning);
        };
        match link.call_json("shutdown", json!({ "force": force })) {
            Err(CallError::Remote(error)) if error.code == "BUSY_VIEW" => {
                return Ok(StopOutcome::LeftRunning(error.to_string()));
            }
            // The reply can be lost to the worker closing its sessions: its
            // exit decides.
            Err(error) => {
                event!(warn, FOLDER_SHUTDOWN_REPLY_FAILED, {
                    "folder": self.record.id.clone(),
                    "error": error.to_string(),
                });
            }
            Ok(_) => {}
        }
        self.stop_requested = true;
        let running = self.phase.clone();
        let wait = if force { FORCE_STOP_WAIT } else { STOP_WAIT };
        let now_ms = self.clock.now_ms();
        self.phase = Phase::Stopping {
            since_ms: now_ms,
            force,
            deadline_ms: now_ms.saturating_add(duration_ms(wait)),
        };
        if self.wait_exit(wait) {
            self.teardown();
            self.phase = Phase::Stopped;
            return Ok(StopOutcome::Stopped);
        }
        // The wait is not a lost heartbeat.
        self.last_beat = self.clock.instant();
        self.watch_errors = 0;
        if !force {
            self.phase = running;
            return Ok(StopOutcome::LeftRunning(format!(
                "STOP_TIMEOUT: the worker did not exit within {} s",
                wait.as_secs()
            )));
        }
        match self.kill_current() {
            Some(Ok(true)) => {
                self.teardown();
                self.phase = Phase::Stopped;
                Ok(StopOutcome::Killed)
            }
            // Not killed: stopped only if it is gone.
            Some(Ok(false))
                if self
                    .process
                    .as_ref()
                    .is_some_and(|process| process.exited().unwrap_or(false)) =>
            {
                self.teardown();
                self.phase = Phase::Stopped;
                Ok(StopOutcome::Stopped)
            }
            Some(Ok(false)) | None => {
                self.phase = running;
                Ok(StopOutcome::LeftRunning(
                    "WORKER_KILL_REFUSED: the worker's identity could not be verified".into(),
                ))
            }
            Some(Err(error)) => {
                self.phase = running;
                Err(error)
            }
        }
    }

    /// Drains process events and watch results, checks liveness and the
    /// Ready deadline, and ends an expired backoff. Blocks only in bounded
    /// calls: a loopback dial or the 2 s `hello` probe.
    pub fn tick(&mut self) {
        self.drain_events();
        if matches!(self.phase, Phase::Starting { adopting: true, .. }) && self.link.is_none() {
            self.connect_adopted();
        }
        if matches!(self.phase, Phase::Running { .. }) {
            self.check_liveness();
        }
        let now = self.clock.instant();
        if matches!(self.phase, Phase::Starting { .. })
            && now.saturating_duration_since(self.phase_start) >= READY_TIMEOUT
        {
            self.kill_current();
            self.fail(format!(
                "WORKER_READY_TIMEOUT: no Ready line within {} s",
                READY_TIMEOUT.as_secs()
            ));
        }
        if matches!(self.phase, Phase::Backoff { .. }) && now >= self.backoff_until {
            self.start();
        }
    }

    /// `{record, phase, heartbeat, lanes: [{role, health}]}`.
    pub fn view(&self) -> Value {
        let lanes: Vec<Value> = self
            .link
            .iter()
            .flat_map(|link| link.health())
            .map(|(role, health)| json!({"role": role, "health": health}))
            .collect();
        json!({
            "record": self.record,
            "phase": self.phase,
            "heartbeat": self.heartbeat,
            "lanes": lanes,
        })
    }

    fn starting(&self, attempt: u32, adopting: bool) -> Phase {
        Phase::Starting {
            since_ms: self.clock.now_ms(),
            attempt,
            adopting,
        }
    }

    fn drain_events(&mut self) {
        loop {
            let Some(events) = &self.events else {
                return;
            };
            let event = match events.try_recv() {
                Ok(event) => event,
                Err(TryRecvError::Empty) => return,
                // `Exited` always comes last; a source gone without one (an
                // adopted watcher that ended early) is an exit too.
                Err(TryRecvError::Disconnected) => {
                    self.events = None;
                    ProcessEvent::Exited {
                        code: None,
                        tail: String::new(),
                    }
                }
            };
            self.on_event(event);
        }
    }

    fn on_event(&mut self, event: ProcessEvent) {
        match event {
            ProcessEvent::Ready(ready) => {
                if matches!(
                    self.phase,
                    Phase::Starting {
                        adopting: false,
                        ..
                    }
                ) && self.link.is_none()
                {
                    self.connect(ready, false);
                }
            }
            // Followed by its exit, which reports it.
            ProcessEvent::Fatal { code, message } => {
                self.fatal = Some(ErrorWire::new(code, message).to_string());
            }
            ProcessEvent::Exited { code, tail } => {
                if !matches!(self.phase, Phase::Starting { .. } | Phase::Running { .. }) {
                    return;
                }
                event!(warn, FOLDER_WORKER_EXITED, {
                    "folder": self.record.id.clone(),
                    "code": code,
                    "tail": tail,
                });
                if self.stop_requested {
                    self.teardown();
                    self.phase = Phase::Stopped;
                    return;
                }
                let error = self.fatal.take().unwrap_or_else(|| match code {
                    Some(code) => format!("WORKER_EXITED: exit code {code}"),
                    None => "WORKER_EXITED: no exit code".into(),
                });
                self.fail(error);
            }
        }
    }

    /// Links to the worker `ready` names, then runs it with a watch thread.
    fn connect(&mut self, ready: Ready, adopted: bool) {
        if !adopted {
            // Written before the Ready line. Without it the worker cannot be
            // killed: a record without its OS identity never matches.
            let recorded = read_record(&self.record.state)
                .filter(|record| record.pid == ready.pid && record.instance == ready.instance);
            self.worker = Some(recorded.unwrap_or_else(|| unverified_record(&ready)));
        }
        let link = match self.launcher.connect(&ready, self.lanes) {
            Ok(link) => link,
            Err(error) => {
                self.kill_current();
                self.fail(format!("WORKER_CONNECT_FAILED: {error}"));
                return;
            }
        };
        let now = self.clock.instant();
        self.phase_start = now;
        self.last_beat = now;
        self.watch_errors = 0;
        // Without a watch thread the probe still runs every HEARTBEAT_LOST.
        self.watch = match WatchThread::start(link.clone()) {
            Ok(watch) => Some(watch),
            Err(error) => {
                event!(error, FOLDER_WATCH_START_FAILED, {
                    "folder": self.record.id.clone(),
                    "error": error.to_string(),
                });
                None
            }
        };
        self.link = Some(link);
        self.phase = Phase::Running {
            since_ms: self.clock.now_ms(),
            instance: ready.instance,
            pid: ready.pid,
            address: ready.address,
            adopted,
        };
    }

    /// Links to an adopted worker with the token it wrote at startup.
    fn connect_adopted(&mut self) {
        let Some(worker) = self.worker.clone() else {
            return;
        };
        let path = self.record.state.join(TOKEN_FILE);
        match fs::read_to_string(&path) {
            Ok(token) => {
                let ready = Ready {
                    ready: true,
                    repo: self.record.repo.clone(),
                    device: String::new(),
                    instance: worker.instance,
                    pid: worker.pid,
                    address: worker.address,
                    token: token.trim().to_owned(),
                    version: worker.version,
                    ipc: worker.ipc,
                    mounts: Vec::new(),
                };
                self.connect(ready, true);
            }
            Err(error) => {
                self.kill_current();
                self.fail(format!(
                    "WORKER_TOKEN_UNREADABLE: {}: {error}",
                    path.display()
                ));
            }
        }
    }

    fn check_liveness(&mut self) {
        let results: Vec<_> = self
            .watch
            .as_ref()
            .map(|watch| watch.results.try_iter().collect())
            .unwrap_or_default();
        for result in results {
            match result {
                Ok(watch) => {
                    self.heartbeat = Some(watch.heartbeat);
                    self.last_beat = self.clock.instant();
                    self.watch_errors = 0;
                }
                Err(_) => self.watch_errors = self.watch_errors.saturating_add(1),
            }
        }
        let Some(link) = self.link.clone() else {
            return;
        };
        // The worker accepted `shutdown` and is exiting (flushing, say): a
        // quiet heartbeat or a closed link is expected, never a reason to
        // kill it or a failure. Its exit is a stop; `stop(true)` can still
        // force it.
        if self.stop_requested {
            return;
        }
        let now = self.clock.instant();
        let quiet = now.saturating_duration_since(self.last_beat);
        if quiet < HEARTBEAT_LOST && self.watch_errors < FAILED_WATCHES && link.alive() {
            return;
        }
        match link.call("hello", json!({}), Vec::new(), PROBE_DEADLINE) {
            Ok(_) => {
                self.last_beat = now;
                self.watch_errors = 0;
            }
            Err(error) => {
                let reason = format!(
                    "HEARTBEAT_LOST: no heartbeat for {} ms, {} failed watches; hello: {error}",
                    quiet.as_millis(),
                    self.watch_errors
                );
                self.kill_current();
                self.fail(reason);
            }
        }
    }

    /// `kill_verified` on the current worker; `None` without a record.
    fn kill_current(&mut self) -> Option<Result<bool>> {
        let record = self.worker.clone().or_else(|| {
            let pid = self.process.as_ref()?.pid;
            read_record(&self.record.state).filter(|record| record.pid == pid)
        })?;
        let result = self.launcher.kill_verified(&record);
        match &result {
            Ok(killed) => {
                event!(warn, FOLDER_WORKER_KILLED, {
                    "folder": self.record.id.clone(),
                    "pid": record.pid,
                    "killed": *killed,
                });
            }
            Err(error) => {
                event!(error, FOLDER_WORKER_KILL_FAILED, {
                    "folder": self.record.id.clone(),
                    "pid": record.pid,
                    "error": format!("{error:#}"),
                });
            }
        }
        Some(result)
    }

    /// Records a failure: `Backoff`, or `Failed{CRASH_LOOP}` once
    /// [`BREAKER_FAILURES`] fall within [`BREAKER_WINDOW`].
    fn fail(&mut self, error: String) {
        let now = self.clock.instant();
        if matches!(self.phase, Phase::Running { .. })
            && now.saturating_duration_since(self.phase_start) >= BREAKER_WINDOW
        {
            self.streak = 0;
        }
        self.teardown();
        self.failures.push_back(now);
        while self
            .failures
            .front()
            .is_some_and(|at| now.saturating_duration_since(*at) >= BREAKER_WINDOW)
        {
            self.failures.pop_front();
        }
        self.streak = self.streak.saturating_add(1);
        event!(warn, FOLDER_WORKER_FAILED, {
            "folder": self.record.id.clone(),
            "attempt": self.streak,
            "error": error.clone(),
        });
        let now_ms = self.clock.now_ms();
        if self.failures.len() >= BREAKER_FAILURES {
            self.phase = Phase::Failed {
                since_ms: now_ms,
                code: "CRASH_LOOP".into(),
                message: format!(
                    "{} worker failures within {} s; last: {error}",
                    self.failures.len(),
                    BREAKER_WINDOW.as_secs()
                ),
            };
            return;
        }
        let jitter = self
            .jitter
            .unwrap_or_else(|| rand::thread_rng().gen_range(-JITTER..=JITTER));
        let delay = backoff_delay(self.streak, jitter);
        self.backoff_until = now + delay;
        self.phase = Phase::Backoff {
            until_ms: now_ms.saturating_add(duration_ms(delay)),
            attempt: self.streak,
            error,
        };
    }

    /// Waits ≤ `wait` for the worker's exit, measured by the clock and, as a
    /// backstop for a clock that does not move (tests), by real time;
    /// whether it exited.
    fn wait_exit(&mut self, wait: Duration) -> bool {
        let start = self.clock.instant();
        let backstop = Instant::now() + wait;
        let Some(events) = &self.events else {
            return false;
        };
        loop {
            let elapsed = self.clock.instant().saturating_duration_since(start);
            let left = wait
                .saturating_sub(elapsed)
                .min(backstop.saturating_duration_since(Instant::now()));
            if left.is_zero() {
                return false;
            }
            match events.recv_timeout(left.min(STOP_POLL)) {
                Ok(ProcessEvent::Exited { .. }) | Err(RecvTimeoutError::Disconnected) => {
                    return true;
                }
                Ok(_) | Err(RecvTimeoutError::Timeout) => {}
            }
        }
    }

    /// Lets go of the current worker without killing it.
    fn teardown(&mut self) {
        // Stop flag first, then close: the in-flight watch fails at once.
        self.watch = None;
        if let Some(link) = self.link.take() {
            link.close();
        }
        self.events = None;
        self.process = None;
        self.worker = None;
        self.heartbeat = None;
        self.watch_errors = 0;
    }
}

impl Drop for Controller {
    /// Closes the link; the worker keeps running for the next daemon.
    fn drop(&mut self) {
        self.watch = None;
        if let Some(link) = self.link.take() {
            link.close();
        }
    }
}

/// `STATE/worker.json`, if readable.
fn read_record(state: &Path) -> Option<WorkerRecord> {
    let bytes = fs::read(state.join(RECORD_FILE)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// A record naming `ready`'s worker without its OS identity: `kill_verified`
/// never matches it, so it never kills.
fn unverified_record(ready: &Ready) -> WorkerRecord {
    WorkerRecord {
        format: 1,
        pid: ready.pid,
        start_ms: 0,
        image: PathBuf::new(),
        version: ready.version.clone(),
        instance: ready.instance.clone(),
        address: ready.address.clone(),
        ipc: ready.ipc,
    }
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        model::errors::ErrorWire,
        session::{self, ConnOptions, Handler, Incoming, LaneRole},
        util::{lock::lock, time::ManualClock},
    };
    use std::{net::TcpListener, sync::Mutex};

    const TOKEN: &str = "folder-test-token";
    const PID: u32 = 4001;

    struct FnHandler<F>(F);

    impl<F: Fn(Incoming) + Send + Sync> Handler for FnHandler<F> {
        fn handle(&self, req: Incoming) {
            (self.0)(req)
        }
    }

    /// An in-process loopback server standing in for a worker: `watch` is
    /// parked and never answered (its heartbeat goes quiet), `shutdown` is
    /// accepted (without exiting) when `accept_shutdown`, else refused with
    /// `BUSY_VIEW`, and anything else (the `hello` probe) fails. Its address,
    /// for a [`Ready`].
    fn fake_worker(accept_shutdown: bool) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("ws://{}", listener.local_addr().unwrap());
        let parked = Arc::new(Mutex::new(Vec::<Incoming>::new()));
        let handler: Arc<dyn Handler> =
            Arc::new(FnHandler(move |req: Incoming| match req.op.as_str() {
                "watch" => lock(&parked).push(req),
                "shutdown" if accept_shutdown => req.responder.reply(json!({}), b"{}".to_vec()),
                "shutdown" => req
                    .responder
                    .error(ErrorWire::coded("BUSY_VIEW", "1 files open")),
                _ => req
                    .responder
                    .error(ErrorWire::coded("WORKER_UNRESPONSIVE", "probe refused")),
            }));
        thread::spawn(move || {
            let mut conns = Vec::new();
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                let accepted = session::accept_loopback(
                    stream,
                    Some(TOKEN),
                    ConnOptions::for_role(LaneRole::Control),
                );
                if let Ok(conn) = accepted {
                    conn.serve(handler.clone());
                    conns.push(conn);
                }
            }
        });
        address
    }

    fn ready(address: &str, pid: u32) -> Ready {
        Ready {
            ready: true,
            repo: "repo-1".into(),
            device: "device-1".into(),
            instance: format!("instance-{pid}"),
            pid,
            address: address.into(),
            token: TOKEN.into(),
            version: "test".into(),
            ipc: 1,
            mounts: Vec::new(),
        }
    }

    /// A scripted launcher: each spawn takes the next script of events (none
    /// once they run out), keeps its sink open for [`Stub::emit`], and
    /// connects for real to the fake worker. Kills are only recorded.
    #[derive(Default)]
    struct Stub {
        scripts: Mutex<VecDeque<Vec<ProcessEvent>>>,
        sink: Mutex<Option<Sender<ProcessEvent>>>,
        spawns: Mutex<u32>,
        kills: Mutex<Vec<u32>>,
    }

    impl Stub {
        fn new(scripts: Vec<Vec<ProcessEvent>>) -> Arc<Self> {
            Arc::new(Self {
                scripts: Mutex::new(scripts.into()),
                ..Self::default()
            })
        }

        /// Sends `event` as the latest spawned worker.
        fn emit(&self, event: ProcessEvent) {
            lock(&self.sink).as_ref().unwrap().send(event).unwrap();
        }

        fn spawns(&self) -> u32 {
            *lock(&self.spawns)
        }

        fn kills(&self) -> Vec<u32> {
            lock(&self.kills).clone()
        }
    }

    impl Launcher for Stub {
        fn spawn(&self, spec: &LaunchSpec, sink: Sender<ProcessEvent>) -> Result<WorkerProcess> {
            assert_eq!(spec.executable, PathBuf::from("tkfs-test"));
            let mut spawns = lock(&self.spawns);
            *spawns += 1;
            for event in lock(&self.scripts).pop_front().unwrap_or_default() {
                sink.send(event).unwrap();
            }
            *lock(&self.sink) = Some(sink);
            Ok(WorkerProcess::detached(PID + *spawns - 1))
        }

        fn adopt(
            &self,
            _state: &Path,
            _sink: Sender<ProcessEvent>,
        ) -> Result<Option<(WorkerRecord, WorkerProcess)>> {
            Ok(None)
        }

        fn connect(&self, ready: &Ready, lanes: usize) -> Result<Arc<WorkerLink>, CallError> {
            WorkerLink::connect(ready, lanes)
        }

        fn kill_verified(&self, record: &WorkerRecord) -> Result<bool> {
            lock(&self.kills).push(record.pid);
            Ok(true)
        }
    }

    fn folder() -> FolderRecord {
        FolderRecord {
            id: "folder-1".into(),
            label: "Folder".into(),
            // Never created: no worker.json to read.
            state: std::env::temp_dir().join("tkfs-folder-test-absent-state"),
            repo: "repo-1".into(),
            mount: None,
            desired_running: true,
            paused: false,
            created_ms: 1,
        }
    }

    fn controller(stub: &Arc<Stub>, clock: &Arc<ManualClock>) -> Controller {
        Controller::new(
            folder(),
            PathBuf::from("tkfs-test"),
            1,
            stub.clone(),
            clock.clone(),
        )
        .with_jitter(0.0)
    }

    fn exited(code: i32) -> ProcessEvent {
        ProcessEvent::Exited {
            code: Some(code),
            tail: "tail".into(),
        }
    }

    #[test]
    fn backoff_curve() {
        assert_eq!(backoff_delay(1, 0.0), Duration::from_secs(1));
        assert_eq!(backoff_delay(4, 0.0), Duration::from_secs(8));
        assert_eq!(backoff_delay(12, 0.0), Duration::from_secs(300));
        assert_eq!(backoff_delay(3, 0.2), Duration::from_millis(4800));
        assert_eq!(backoff_delay(3, -0.2), Duration::from_millis(3200));
        // Out-of-range jitter is clamped to ±0.2.
        assert_eq!(backoff_delay(1, 5.0), Duration::from_millis(1200));
        assert_eq!(backoff_delay(1, f64::NAN), Duration::from_secs(1));
    }

    #[test]
    fn exit_while_running_backs_off_then_restarts_on_monotonic_time_only() {
        let address = fake_worker(false);
        let stub = Stub::new(vec![vec![ProcessEvent::Ready(ready(&address, PID))]]);
        let clock = Arc::new(ManualClock::default());
        let mut controller = controller(&stub, &clock);
        controller.start();
        assert!(matches!(
            controller.phase(),
            Phase::Starting {
                attempt: 1,
                adopting: false,
                ..
            }
        ));
        controller.tick();
        match controller.phase() {
            Phase::Running {
                pid,
                adopted,
                instance,
                address: at,
                ..
            } => {
                assert_eq!((*pid, *adopted), (PID, false));
                assert_eq!(instance, &format!("instance-{PID}"));
                assert_eq!(at, &address);
            }
            other => panic!("not running: {other:?}"),
        }
        assert!(controller.link().is_some());

        stub.emit(exited(1));
        controller.tick();
        let until = match controller.phase() {
            Phase::Backoff {
                until_ms,
                attempt,
                error,
            } => {
                assert_eq!(*attempt, 1);
                assert!(error.contains("WORKER_EXITED"), "{error}");
                *until_ms
            }
            other => panic!("not backing off: {other:?}"),
        };
        assert_eq!(until, clock.now_ms() + 1000);
        assert!(controller.link().is_none());

        // A wall-clock step past `until_ms` does not end the backoff.
        clock.advance_wall(Duration::from_secs(3600));
        controller.tick();
        assert!(matches!(
            controller.phase(),
            Phase::Backoff { attempt: 1, .. }
        ));
        clock.advance(Duration::from_millis(999));
        controller.tick();
        assert!(matches!(
            controller.phase(),
            Phase::Backoff { attempt: 1, .. }
        ));
        assert_eq!(stub.spawns(), 1);

        clock.advance(Duration::from_millis(1));
        controller.tick();
        assert!(matches!(
            controller.phase(),
            Phase::Starting {
                attempt: 2,
                adopting: false,
                ..
            }
        ));
        assert_eq!(stub.spawns(), 2);
    }

    #[test]
    fn five_failures_in_ten_minutes_trip_the_breaker() {
        let stub = Stub::new((0..5).map(|_| vec![exited(1)]).collect());
        let clock = Arc::new(ManualClock::default());
        let mut controller = controller(&stub, &clock);
        controller.start();
        for attempt in 1..=4 {
            assert!(
                matches!(controller.phase(), Phase::Starting { attempt: a, .. } if *a == attempt)
            );
            controller.tick();
            assert!(
                matches!(controller.phase(), Phase::Backoff { attempt: a, .. } if *a == attempt),
                "{:?}",
                controller.phase()
            );
            clock.advance(backoff_delay(attempt, 0.0));
            controller.tick();
        }
        assert!(matches!(
            controller.phase(),
            Phase::Starting { attempt: 5, .. }
        ));
        controller.tick();
        match controller.phase() {
            Phase::Failed { code, message, .. } => {
                assert_eq!(code, "CRASH_LOOP");
                assert!(message.contains("WORKER_EXITED"), "{message}");
            }
            other => panic!("breaker did not trip: {other:?}"),
        }
        // Failed stays failed: no automatic restart.
        clock.advance(Duration::from_secs(3600));
        controller.tick();
        assert!(matches!(controller.phase(), Phase::Failed { .. }));
        assert_eq!(stub.spawns(), 5);

        controller.retry();
        assert!(matches!(
            controller.phase(),
            Phase::Starting { attempt: 1, .. }
        ));
        assert_eq!(stub.spawns(), 6);
    }

    #[test]
    fn stop_without_force_passes_busy_view_through_as_left_running() {
        let address = fake_worker(false);
        let stub = Stub::new(vec![vec![ProcessEvent::Ready(ready(&address, PID))]]);
        let clock = Arc::new(ManualClock::default());
        let mut controller = controller(&stub, &clock);
        assert_eq!(controller.stop(false).unwrap(), StopOutcome::NotRunning);

        controller.start();
        controller.tick();
        let running = controller.phase().clone();
        assert!(matches!(running, Phase::Running { .. }), "{running:?}");
        match controller.stop(false).unwrap() {
            StopOutcome::LeftRunning(message) => {
                assert!(message.starts_with("BUSY_VIEW"), "{message}")
            }
            other => panic!("not left running: {other:?}"),
        }
        assert_eq!(controller.phase(), &running);
        assert!(controller.link().is_some());
        assert!(stub.kills().is_empty());

        let view = controller.view();
        assert_eq!(view["record"]["id"], "folder-1");
        assert_eq!(view["phase"]["phase"], "Running");
        assert_eq!(view["phase"]["pid"], PID);
        assert!(view["heartbeat"].is_null());
        let roles: Vec<&Value> = view["lanes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|lane| &lane["role"])
            .collect();
        assert_eq!(roles, [&json!("control"), &json!("bulk")]);
        assert!(view["lanes"][0]["health"].is_object());
    }

    #[test]
    fn lost_heartbeat_probes_then_kills() {
        let address = fake_worker(false);
        let stub = Stub::new(vec![vec![ProcessEvent::Ready(ready(&address, PID))]]);
        let clock = Arc::new(ManualClock::default());
        let mut controller = controller(&stub, &clock);
        controller.start();
        controller.tick();
        assert!(matches!(controller.phase(), Phase::Running { .. }));

        clock.advance(HEARTBEAT_LOST - Duration::from_millis(1));
        controller.tick();
        assert!(matches!(controller.phase(), Phase::Running { .. }));
        assert!(stub.kills().is_empty());

        clock.advance(Duration::from_millis(1));
        controller.tick();
        assert_eq!(stub.kills(), vec![PID]);
        match controller.phase() {
            Phase::Backoff { attempt, error, .. } => {
                assert_eq!(*attempt, 1);
                assert!(error.contains("HEARTBEAT_LOST"), "{error}");
            }
            other => panic!("not backing off: {other:?}"),
        }
        assert!(controller.link().is_none());
        controller.tick();
        assert_eq!(stub.kills(), vec![PID]);
    }

    #[test]
    fn stop_timeout_leaves_worker_running_and_later_exit_is_a_clean_stop() {
        let address = fake_worker(true);
        let stub = Stub::new(vec![vec![ProcessEvent::Ready(ready(&address, PID))]]);
        let clock = Arc::new(ManualClock::default());
        let mut controller = controller(&stub, &clock);
        controller.start();
        controller.tick();
        assert!(matches!(controller.phase(), Phase::Running { .. }));

        // The stop wait runs on the clock: pass STOP_WAIT while it waits.
        let advancer = {
            let clock = clock.clone();
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(200));
                clock.advance(STOP_WAIT);
            })
        };
        let waited = Instant::now();
        match controller.stop(false).unwrap() {
            StopOutcome::LeftRunning(message) => {
                assert!(message.starts_with("STOP_TIMEOUT"), "{message}")
            }
            other => panic!("not left running: {other:?}"),
        }
        assert!(waited.elapsed() < STOP_WAIT, "the wait ignored the clock");
        advancer.join().unwrap();
        assert!(matches!(controller.phase(), Phase::Running { .. }));

        // The worker is exiting: a quiet heartbeat is neither probed to
        // death nor a failure.
        clock.advance(Duration::from_secs(10));
        controller.tick();
        assert!(stub.kills().is_empty());
        assert!(
            matches!(controller.phase(), Phase::Running { .. }),
            "{:?}",
            controller.phase()
        );
        assert!(controller.failures.is_empty());

        stub.emit(exited(0));
        controller.tick();
        assert_eq!(controller.phase(), &Phase::Stopped);
        assert!(controller.link().is_none());
        assert!(stub.kills().is_empty());
        assert!(controller.failures.is_empty());
    }
}
