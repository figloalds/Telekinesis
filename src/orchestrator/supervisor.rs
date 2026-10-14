use super::{
    api::Project,
    config::Config,
    lanes::{lane_role, link_key},
    lock_state,
    status::LinkStatus,
};
use crate::{
    core::{Store, id},
    model::{
        errors::ErrorWire,
        orchestrator::{ShutdownOutcome, outcomes},
    },
    paired_sync::PublishedState,
    runtime::{self, Discovery},
    util::backoff::Backoff,
};
use anyhow::{Context, Result, bail, ensure};
use rusqlite::Connection;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::AtomicBool,
        mpsc::{self, Receiver},
    },
    time::{Duration, Instant},
};

pub(crate) struct Worker {
    pub(crate) child: Child,
    pub(crate) messages: Receiver<Value>,
    pub(crate) startup: Option<crate::startup::Watchdog>,
    /// Requested at launch; after Ready, the worker's actual mount state.
    pub(crate) mounted: bool,
    /// A requested mount that failed (Ready with `mounted:false` and
    /// `mount_error`): the worker keeps syncing headless and the folder
    /// reports this error until it is restarted.
    pub(crate) mount_error: Option<String>,
}
/// Per-folder restart backoff (WP1.12): a failed worker is restarted after
/// 1 s, 2 s, 4 s … up to 60 s instead of on every reconcile.
pub(crate) struct Restart {
    pub(crate) backoff: Backoff,
    pub(crate) not_before: Instant,
}
impl Restart {
    fn new() -> Self {
        Self {
            // ±20 % jitter; the attempt counter resets after 60 s running.
            backoff: Backoff::new(
                Duration::from_secs(1),
                Duration::from_secs(60),
                0.2,
                Duration::from_secs(60),
            ),
            not_before: Instant::now(),
        }
    }
}
pub struct Supervisor {
    pub config: Config,
    pub(crate) db: Connection,
    pub(crate) installation: String,
    pub(crate) workers: BTreeMap<String, Worker>,
    pub(crate) failures: BTreeMap<String, String>,
    pub(crate) publication_errors: BTreeMap<String, String>,
    pub(crate) links: BTreeMap<String, LinkStatus>,
    pub(crate) remote_catalogs: BTreeMap<String, Vec<PublishedState>>,
    pub(crate) restarts: BTreeMap<String, Restart>,
    pub(crate) stopping: Arc<AtomicBool>,
    pub(crate) _lock: File,
}
pub type Shared = Arc<Mutex<Supervisor>>;

impl Supervisor {
    pub(crate) fn create(
        &mut self,
        label: String,
        repo: Option<String>,
        mount: Option<PathBuf>,
        headless: bool,
        initial_peer: Option<&str>,
    ) -> Result<Value> {
        ensure!(
            !label.trim().is_empty() && label.len() <= 200,
            "INVALID_FOLDER_LABEL"
        );
        let state = id();
        let repo = repo.unwrap_or_else(id);
        ensure!(
            !self.projects()?.iter().any(|p| p.repo == repo),
            "REPOSITORY_ALREADY_MANAGED"
        );
        ensure!(
            self.workers.len() < self.config.maximum_running,
            "WORKER_LIMIT"
        );
        let directory = self.config.data_directory.join("states").join(&state);
        let mount = if headless {
            ensure!(mount.is_none(), "HEADLESS_MOUNT_CONFLICT");
            None
        } else {
            if mount.is_none() {
                crate::core::name_key(&label).context("INVALID_MOUNT_FOLDER_NAME")?;
            }
            Some(mount.unwrap_or_else(|| self.config.default_mount_directory.join(&label)))
        };
        let mount = mount.map(|p| self.mount_path(&directory, &p)).transpose()?;
        let device = id();
        let _store = Store::initialize(&directory, &repo, &device)?;
        drop(_store);
        let tx = self.db.unchecked_transaction()?;
        Self::insert_project_tx(
            &tx,
            &state,
            &label,
            &repo,
            &device,
            &directory,
            mount.as_deref(),
        )?;
        if let Some(endpoint) = initial_peer.filter(|_| mount.is_some()) {
            Self::insert_initial_sync_tx(&tx, &state, endpoint)?;
        }
        tx.commit()?;
        // Keep the durable record if the mount fails; list/inspect reports the error
        // and configure-mount/start can repair it without losing the state.
        if let Err(error) = self.start(&state) {
            self.failures.insert(state.clone(), format!("{error:#}"));
        }
        self.inspect(&state)
    }
    pub(crate) fn mount_path(&self, directory: &Path, requested: &Path) -> Result<PathBuf> {
        let path = std::path::absolute(requested)?;
        let parent = path.parent().context("MOUNT_PARENT_REQUIRED")?;
        fs::create_dir_all(parent)?;
        let path =
            fs::canonicalize(parent)?.join(path.file_name().context("FOLDER_MOUNT_REQUIRED")?);
        let data = fs::canonicalize(&self.config.data_directory)?;
        ensure!(
            !path.starts_with(&data) && !data.starts_with(&path) && !directory.starts_with(&path),
            "MOUNT_OVERLAPS_DATA"
        );
        ensure!(!path.exists(), "MOUNT_PATH_MUST_NOT_EXIST");
        for p in self.projects()? {
            if let Some(other) = p.mount {
                ensure!(
                    !path.starts_with(&other) && !other.starts_with(&path),
                    "MOUNT_OVERLAPS_FOLDER"
                );
            }
        }
        Ok(path)
    }
    pub(crate) fn discovery(&self, state: &str) -> Result<Discovery> {
        let p = self.project(state)?;
        ensure!(
            self.workers.get(state).is_some_and(|w| w.startup.is_none()),
            "WORKER_NOT_RUNNING"
        );
        let info: Discovery = serde_json::from_slice(&fs::read(p.directory.join("runtime.json"))?)?;
        ensure!(
            info.repo == p.repo && info.device == p.device,
            "WORKER_STATE_MISMATCH"
        );
        Ok(info)
    }
    /// An explicit start (Start, Create): a fresh attempt that forgets the
    /// folder's crash backoff.
    pub(crate) fn start(&mut self, state: &str) -> Result<()> {
        self.restarts.remove(state);
        self.launch(state)
    }
    fn launch(&mut self, state: &str) -> Result<()> {
        if self.workers.contains_key(state) {
            return Ok(());
        }
        ensure!(
            self.workers.len() < self.config.maximum_running,
            "WORKER_LIMIT"
        );
        let p = self.project(state)?;
        // Discover an existing worker after daemon restart; do not start a duplicate.
        // Only a process holding `owner.lock` can be one: when the lock is free,
        // a stale `runtime.json` (a crashed worker) needs no probe, which keeps
        // a refused connect (1-2 s on Windows) out of this locked path.
        let owner_running = lock_state(&p.directory).is_err();
        if owner_running
            && let Ok(bytes) = fs::read(p.directory.join("runtime.json"))
            && let Ok(info) = serde_json::from_slice::<Discovery>(&bytes)
            && info.repo == p.repo
            && info.device == p.device
            && let Ok(status) = runtime::rpc(&info, &id(), json!({"op":"status"}))
            && status["repo"] == p.repo
            && status["device"] == p.device
        {
            runtime::rpc(&info, &id(), json!({"op":"stop"}))?;
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if let Ok(lock) = lock_state(&p.directory) {
                    drop(lock);
                    break;
                }
                ensure!(Instant::now() < deadline, "PREVIOUS_WORKER_STOP_TIMEOUT");
                std::thread::sleep(Duration::from_millis(25));
            }
        }
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(p.directory.join("worker.log"))?;
        let mut command = Command::new(std::env::current_exe()?);
        command.arg("daemon").arg("--state").arg(&p.directory);
        let mounted = p.mount.is_some() && self.initial_peer(state)?.is_none();
        if let Some(mount) = &p.mount.filter(|_| mounted) {
            command.arg("--mount").arg(mount);
        }
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(log.try_clone()?);
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        let mut child = command.spawn()?;
        let stdout = child.stdout.take().expect("piped worker stdout");
        let (sender, messages) = mpsc::channel();
        std::thread::spawn(move || {
            let mut log = log;
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if let Ok(value) = serde_json::from_str::<Value>(&line) {
                    let _ = sender.send(value);
                }
                let _ = writeln!(log, "{line}");
            }
        });
        self.workers.insert(
            state.into(),
            Worker {
                child,
                messages,
                startup: Some(crate::startup::Watchdog::new(Instant::now())),
                mounted,
                mount_error: None,
            },
        );
        Ok(())
    }
    fn poll_worker(&mut self, state: &str) -> Result<()> {
        let p = self.project(state)?;
        let worker = self.workers.get_mut(state).context("WORKER_NOT_RUNNING")?;
        if let Some(status) = worker.child.try_wait()? {
            bail!(
                "WORKER_EXITED: {status}; see {}",
                p.directory.join("worker.log").display()
            );
        }
        // Only this child's stdout can renew its lease. Old runtime.json files
        // and a responsive side thread cannot keep stalled recovery alive.
        for message in worker.messages.try_iter() {
            if let Some(startup) = &mut worker.startup {
                if let Some(sequence) = message["startup_progress"].as_u64() {
                    startup.observe(
                        sequence,
                        message["phase"].as_str().unwrap_or("recovering"),
                        Instant::now(),
                    );
                }
                if message["ready"] == true {
                    let (mounted, mount_error) = ready_mount(&message, &p, worker.mounted)?;
                    worker.mounted = mounted;
                    worker.mount_error = mount_error;
                    worker.startup = None;
                }
            }
        }
        if worker.startup.is_none() {
            // A failed mount stays the folder's error while its worker keeps
            // syncing headless; it is never a reason to restart the worker.
            match &worker.mount_error {
                Some(error) => {
                    self.failures.insert(state.into(), error.clone());
                }
                None => {
                    self.failures.remove(state);
                }
            }
        }
        if let Some(startup) = &worker.startup {
            ensure!(
                !startup.expired(Instant::now()),
                "WORKER_START_TIMEOUT: no recovery progress for 30s during {}; see {}",
                startup.phase,
                p.directory.join("worker.log").display()
            );
        }
        Ok(())
    }
    pub(crate) fn stop(&mut self, state: &str) -> Result<()> {
        self.stop_outcome(state).map(|_| ())
    }
    /// Stops one folder for daemon Shutdown and reports the outcome (§3.6
    /// `ShutdownOutcome`). A failure never aborts the caller: a worker that
    /// cannot be stopped is left running for the next daemon to probe.
    /// `desired_running` is never changed.
    pub(crate) fn shutdown_folder(&mut self, state: &str) -> ShutdownOutcome {
        let result = self.stop_outcome(state);
        let outcome = match &result {
            Ok(outcome) => outcome,
            Err(_) => {
                let alive = self
                    .workers
                    .get_mut(state)
                    .is_some_and(|worker| worker.child.try_wait().ok().flatten().is_none());
                if alive {
                    outcomes::LEFT_RUNNING
                } else {
                    outcomes::NOT_RUNNING
                }
            }
        };
        ShutdownOutcome {
            state: state.into(),
            outcome: outcome.to_string(),
            error: result.err().map(|error| ErrorWire::from_error(&error)),
        }
    }
    fn stop_outcome(&mut self, state: &str) -> Result<&'static str> {
        if !self.workers.contains_key(state) {
            return Ok(outcomes::NOT_RUNNING);
        }
        // Drain readiness before deciding whether the worker can stop via RPC.
        let _ = self.poll_worker(state);
        if self.workers[state].startup.is_some() {
            let worker = self.workers.get_mut(state).unwrap();
            if worker.child.try_wait()?.is_none() {
                worker.child.kill()?;
            }
            worker.child.wait()?;
            self.workers.remove(state);
            return Ok(outcomes::KILLED);
        }
        let info = self.discovery(state)?;
        runtime::rpc(&info, &id(), json!({"op":"stop"}))?;
        let deadline = Instant::now() + Duration::from_secs(5);
        while self
            .workers
            .get_mut(state)
            .unwrap()
            .child
            .try_wait()?
            .is_none()
        {
            ensure!(Instant::now() < deadline, "WORKER_STOP_TIMEOUT");
            std::thread::sleep(Duration::from_millis(25));
        }
        self.workers.remove(state);
        Ok(outcomes::STOPPED)
    }
    pub(crate) fn inspect(&self, state: &str) -> Result<Value> {
        let project = self.project(state)?;
        let mut value = json!(project);
        let worker = self.workers.get(state);
        let initial_peer = self.initial_peer(state)?;
        value["running"] =
            json!(worker.is_some_and(|w| w.startup.is_none()) && initial_peer.is_none());
        value["starting"] =
            json!(worker.is_some_and(|w| w.startup.is_some() || initial_peer.is_some()));
        value["mounted"] = json!(worker.is_some_and(|w| w.startup.is_none() && w.mounted));
        value["initial_sync_pending"] = json!(initial_peer.is_some());
        value["startup_phase"] = json!(worker.and_then(|w| w.startup.as_ref()).map(|s| &s.phase));
        if worker.is_some_and(|w| w.startup.is_none()) && initial_peer.is_some() {
            value["startup_phase"] = json!("synchronizing remote metadata");
        }
        value["sync"] =
            json!(self.peers()?.iter().map(|peer| {
            let link = self.links.get(&link_key(peer, 0));
            let remote_available = link.filter(|l| l.connected).and_then(|_|
                self.remote_catalogs.get(&link_key(peer, 0)))
                .map(|catalog| catalog.iter().any(|p| p.repo == project.repo));
            json!({"endpoint":peer.endpoint,"connected":link.is_some_and(|l| l.connected),
                "remote_available":remote_available,
                "error":link.and_then(|l| l.error.as_ref()),
                "metadata":link.and_then(|l| l.repositories.get(&project.repo)),
                "lanes":(0..peer.pool_size()).map(|lane| {
                    let status = self.links.get(&link_key(peer, lane));
                    json!({"lane":lane,"role":lane_role(lane),
                        "connected":status.is_some_and(|s| s.connected),
                        "transport":status.and_then(|s| s.transport.as_ref()),
                        "parallel_bulk":status.is_some_and(|s| s.parallel_bulk),
                        "error":status.and_then(|s| s.error.as_ref()),
                        "repository":status.and_then(|s| s.repositories.get(&project.repo))})
                }).collect::<Vec<_>>()})
        }).collect::<Vec<_>>());
        value["error"] = json!(self.failures.get(state));
        value["publication_error"] = json!(self.publication_errors.get(state));
        Ok(value)
    }
    pub(crate) fn reconcile(&mut self) -> Result<()> {
        // Shutdown owns the workers from the moment it sets `stopping`.
        if self.stopping.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }
        let now = Instant::now();
        let states: Vec<_> = self.workers.keys().cloned().collect();
        for state in states {
            match self.poll_worker(&state) {
                Ok(()) => {
                    if self.workers[&state].startup.is_none()
                        && let Some(restart) = self.restarts.get_mut(&state)
                    {
                        restart.backoff.success(now);
                    }
                }
                Err(error) => {
                    let mut worker = self.workers.remove(&state).unwrap();
                    let _ = worker.child.kill();
                    let _ = worker.child.wait();
                    self.failures.insert(state.clone(), format!("{error:#}"));
                    self.defer_restart(&state, now);
                }
            }
        }
        for p in self.projects()? {
            if p.desired_running
                && self
                    .workers
                    .get(&p.state)
                    .is_some_and(|w| w.startup.is_none())
                && let Some(endpoint) = self.initial_peer(&p.state)?
                && let Some(peer) = self
                    .peers()?
                    .into_iter()
                    .find(|peer| peer.endpoint == endpoint)
                && self.links.get(&link_key(&peer, 0)).is_some_and(|link| {
                    link.connected
                        && link.repositories.get(&p.repo).is_some_and(|status| {
                            status.metadata_caught_up
                                && status.error.is_none()
                                && status.last_success_ms.is_some_and(|at| {
                                    crate::core::time().saturating_sub(at) < 15000
                                })
                        })
                })
            {
                // The headless worker has durably accepted the remote journal.
                // Stop it before enabling the mount; the marker survives failures
                // and daemon restarts, and normal later remounts remain offline-capable.
                match self.stop(&p.state) {
                    Ok(()) => {
                        self.delete_initial_sync(&p.state)?;
                    }
                    Err(error) => {
                        self.failures.insert(p.state.clone(), format!("{error:#}"));
                    }
                }
            }
            if p.desired_running
                && !self.workers.contains_key(&p.state)
                && self
                    .restarts
                    .get(&p.state)
                    .is_none_or(|restart| now >= restart.not_before)
                && let Err(error) = self.launch(&p.state)
            {
                self.failures.insert(p.state.clone(), format!("{error:#}"));
                self.defer_restart(&p.state, now);
            }
        }
        Ok(())
    }
    /// Schedules the next restart of a folder whose worker failed (exited,
    /// timed out, mismatched) or could not be launched.
    fn defer_restart(&mut self, state: &str, now: Instant) {
        let restart = self
            .restarts
            .entry(state.into())
            .or_insert_with(Restart::new);
        restart.not_before = now + restart.backoff.failure(now);
    }
    /// When the supervision loop should reconcile next: `fallback`, or
    /// earlier when a backed-off restart falls due before it.
    pub(crate) fn next_reconcile(&self, fallback: Instant) -> Instant {
        let now = Instant::now();
        self.restarts
            .values()
            .map(|restart| restart.not_before)
            .filter(|at| *at > now)
            .fold(fallback, Instant::min)
    }
}

/// Checks a worker Ready line against its folder and the requested mount.
/// Returns the actual mount state and, when a requested mount failed, the
/// error the worker reported. A Ready line with `mounted:false` is accepted
/// only together with its `mount_error` (WP1.4 legacy mode: mount failures
/// are reported, never fatal).
pub(crate) fn ready_mount(
    message: &Value,
    project: &Project,
    requested: bool,
) -> Result<(bool, Option<String>)> {
    ensure!(
        message["repo"] == project.repo && message["device"] == project.device,
        "WORKER_STATE_MISMATCH"
    );
    match message["mounted"].as_bool() {
        Some(mounted) if mounted == requested => Ok((mounted, None)),
        Some(false) => {
            let error = match &message["mount_error"] {
                Value::Null => bail!("WORKER_STATE_MISMATCH: unmounted without a mount_error"),
                Value::String(text) => text.clone(),
                other => serde_json::from_value::<ErrorWire>(other.clone())
                    .map(|wire| wire.to_string())
                    .unwrap_or_else(|_| other.to_string()),
            };
            Ok((false, Some(error)))
        }
        _ => bail!("WORKER_STATE_MISMATCH"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::Network;
    use std::sync::atomic::Ordering;

    fn project() -> Project {
        Project {
            state: id(),
            label: "Folder".into(),
            repo: id(),
            device: id(),
            directory: PathBuf::from("state"),
            mount: Some(PathBuf::from("mount")),
            desired_running: true,
        }
    }

    fn ready(p: &Project, mounted: bool, mount_error: Value) -> Value {
        json!({"ready":true,"repo":p.repo,"device":p.device,"runtime":"runtime.json",
            "mounted":mounted,"mount_error":mount_error})
    }

    #[test]
    fn ready_line_with_a_failed_mount_keeps_the_worker() {
        let p = project();
        assert_eq!(
            ready_mount(&ready(&p, true, Value::Null), &p, true).unwrap(),
            (true, None)
        );
        assert_eq!(
            ready_mount(&ready(&p, false, Value::Null), &p, false).unwrap(),
            (false, None)
        );
        // WP1.4 legacy mode: Ready with `mounted:false` and a typed mount_error.
        let wire = ErrorWire::new(
            "MOUNT_PATH_MUST_NOT_EXIST",
            "MOUNT_PATH_MUST_NOT_EXIST: mount",
        );
        let (mounted, error) = ready_mount(&ready(&p, false, json!(wire)), &p, true).unwrap();
        assert!(!mounted);
        assert!(error.unwrap().starts_with("MOUNT_PATH_MUST_NOT_EXIST"));
        let (_, error) = ready_mount(
            &ready(&p, false, json!("WINFSP_RUNTIME_UNAVAILABLE")),
            &p,
            true,
        )
        .unwrap();
        assert_eq!(error.as_deref(), Some("WINFSP_RUNTIME_UNAVAILABLE"));
    }

    #[test]
    fn ready_line_mismatches_are_still_rejected() {
        let p = project();
        // Unmounted without saying why, or mounted although headless.
        for (line, requested) in [
            (ready(&p, false, Value::Null), true),
            (ready(&p, true, Value::Null), false),
            (json!({"ready":true,"repo":p.repo,"device":p.device}), true),
        ] {
            let error = ready_mount(&line, &p, requested).unwrap_err();
            assert!(format!("{error:#}").contains("WORKER_STATE_MISMATCH"));
        }
        let mut other = ready(&p, true, Value::Null);
        other["repo"] = json!(id());
        assert!(ready_mount(&other, &p, true).is_err());
        let mut other = ready(&p, true, Value::Null);
        other["device"] = json!(id());
        assert!(ready_mount(&other, &p, true).is_err());
    }

    fn supervisor(root: &Path) -> Supervisor {
        Supervisor::open(Config {
            format_version: 2,
            data_directory: root.join("data"),
            default_mount_directory: root.join("mounts"),
            maximum_running: 1,
            network: Network::default(),
        })
        .unwrap()
    }

    #[test]
    fn failed_workers_restart_after_a_growing_delay() {
        let root = tempfile::tempdir().unwrap();
        let mut s = supervisor(root.path());
        let now = Instant::now();
        let fallback = now + Duration::from_secs(600);
        let mut delays = vec![];
        for _ in 0..8 {
            s.defer_restart("folder", now);
            let due = s.restarts["folder"].not_before;
            assert_eq!(s.next_reconcile(fallback), due);
            delays.push(due - now);
        }
        for (attempt, delay) in delays.iter().enumerate() {
            let nominal = Duration::from_secs((1u64 << attempt).min(60));
            assert!(
                *delay >= nominal.mul_f64(0.8) && *delay <= nominal.mul_f64(1.2),
                "attempt {} waited {delay:?}",
                attempt + 1
            );
        }
        // An explicit Start is a fresh attempt.
        assert!(s.start("folder").is_err(), "no such folder");
        assert!(!s.restarts.contains_key("folder"));
        assert_eq!(s.next_reconcile(fallback), fallback);
    }

    #[test]
    fn reconcile_leaves_workers_alone_once_shutdown_began() {
        let root = tempfile::tempdir().unwrap();
        let mut s = supervisor(root.path());
        s.stopping.store(true, Ordering::Relaxed);
        // Even an unreadable catalog is not touched after Shutdown began.
        s.db.execute_batch("DROP TABLE projects").unwrap();
        s.reconcile().unwrap();
        assert!(s.workers.is_empty());
        let outcome = s.shutdown_folder("absent");
        assert_eq!(outcome.outcome, outcomes::NOT_RUNNING);
        assert!(outcome.error.is_none());
    }
}
