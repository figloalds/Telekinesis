use super::{
    config::Config,
    lanes::{lane_role, link_key},
    lock_state,
    status::LinkStatus,
};
use crate::{
    core::{Store, id},
    paired_sync::PublishedState,
    runtime::{self, Discovery},
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
    pub(crate) mounted: bool,
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
    pub(crate) fn start(&mut self, state: &str) -> Result<()> {
        if self.workers.contains_key(state) {
            return Ok(());
        }
        ensure!(
            self.workers.len() < self.config.maximum_running,
            "WORKER_LIMIT"
        );
        let p = self.project(state)?;
        // Discover an existing worker after daemon restart; do not start a duplicate.
        if let Ok(bytes) = fs::read(p.directory.join("runtime.json"))
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
                    ensure!(
                        message["repo"] == p.repo
                            && message["device"] == p.device
                            && message["mounted"] == worker.mounted,
                        "WORKER_STATE_MISMATCH"
                    );
                    worker.startup = None;
                }
            }
        }
        if worker.startup.is_none() {
            self.failures.remove(state);
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
        if !self.workers.contains_key(state) {
            return Ok(());
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
            return Ok(());
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
        Ok(())
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
        let states: Vec<_> = self.workers.keys().cloned().collect();
        for state in states {
            if let Err(error) = self.poll_worker(&state) {
                let mut worker = self.workers.remove(&state).unwrap();
                let _ = worker.child.kill();
                let _ = worker.child.wait();
                self.failures.insert(state, format!("{error:#}"));
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
                && let Err(error) = self.start(&p.state)
            {
                self.failures.insert(p.state, format!("{error:#}"));
            }
        }
        Ok(())
    }
}
