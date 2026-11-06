//! The orchestrator and its folder workers on threads of one process (the
//! Android app). A [`Host`] owns the orchestrator thread, a thread per running
//! worker and the engines those workers serve, so the app reads a folder
//! through the very `Engine` its worker holds. It serves no control endpoint
//! and writes no discovery files: the app calls [`Daemon::execute`] itself.
use super::{
    folder::Launcher,
    hub::{self, Daemon, SHUTDOWN_BOUND},
    process::{EmbeddedWorker, LaunchSpec, ProcessEvent, WorkerProcess},
    worker::{self, Fatal, Ready, WorkerRecord},
    worker_client::WorkerLink,
};
use crate::{model::orchestrator::CallError, runtime::Engine};
use anyhow::{Context, Result, anyhow, bail};
use serde_json::json;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, MutexGuard, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Sender},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// How long a stopping worker gets to exit (its own close bounds are 9 s).
const KILL_WAIT: Duration = Duration::from_secs(12);
/// How long [`Host::stop`] waits for the orchestrator thread after its own
/// shutdown sequence (itself bounded by [`SHUTDOWN_BOUND`]).
const STOP_WAIT: Duration = Duration::from_secs(5);
const POLL: Duration = Duration::from_millis(20);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

/// The orchestrator thread's state, shared with the launcher it runs.
struct Shared {
    daemon: OnceLock<Arc<Daemon>>,
    /// Why the orchestrator thread ended with an error.
    failure: Mutex<Option<String>>,
    /// Set when the orchestrator thread has returned.
    finished: AtomicBool,
    /// Each running folder's engine, by state directory.
    engines: Mutex<BTreeMap<PathBuf, Arc<Engine>>>,
    /// Every worker thread spawned and not yet pruned, ready or not.
    workers: Mutex<Vec<Arc<ThreadWorker>>>,
}

pub struct Host {
    shared: Arc<Shared>,
    orchestrator: Mutex<Option<JoinHandle<()>>>,
}

impl Host {
    /// How long a cold-started request waits for the orchestrator and its worker.
    pub const START_WAIT: Duration = Duration::from_secs(15);

    /// Starts the orchestrator for the configuration at `config` on its own
    /// thread and returns at once; [`Self::daemon`] waits for it.
    pub fn start(config: PathBuf) -> Arc<Host> {
        let shared = Arc::new(Shared {
            daemon: OnceLock::new(),
            failure: Mutex::new(None),
            finished: AtomicBool::new(false),
            engines: Mutex::new(BTreeMap::new()),
            workers: Mutex::new(Vec::new()),
        });
        let running = shared.clone();
        let spawned = thread::Builder::new()
            .name("tkfs-orchestrator".into())
            .spawn(move || {
                let launcher = Arc::new(ThreadLauncher(running.clone()));
                let result = hub::run_embedded(&config, launcher, |daemon| {
                    let _ = running.daemon.set(daemon);
                });
                if let Err(error) = result {
                    *lock(&running.failure) = Some(format!("{error:#}"));
                }
                running.finished.store(true, Ordering::Release);
            });
        let orchestrator = match spawned {
            Ok(handle) => Some(handle),
            Err(error) => {
                *lock(&shared.failure) = Some(error.to_string());
                shared.finished.store(true, Ordering::Release);
                None
            }
        };
        Arc::new(Host {
            shared,
            orchestrator: Mutex::new(orchestrator),
        })
    }

    /// The orchestrator, once it takes requests: `ORCHESTRATOR_FAILED` when it
    /// ended with an error, `ORCHESTRATOR_NOT_READY` after [`Self::START_WAIT`].
    pub fn daemon(&self) -> Result<Arc<Daemon>> {
        let end = Instant::now() + Self::START_WAIT;
        loop {
            if let Some(daemon) = self.shared.daemon.get() {
                return Ok(daemon.clone());
            }
            if let Some(error) = lock(&self.shared.failure).clone() {
                bail!("ORCHESTRATOR_FAILED: {error}");
            }
            if Instant::now() >= end {
                bail!("ORCHESTRATOR_NOT_READY");
            }
            thread::sleep(POLL);
        }
    }

    /// The folder's engine, waiting for its worker on a cold start.
    pub fn engine(&self, folder: &str) -> Result<Arc<Engine>> {
        let daemon = self.daemon()?;
        let record = daemon
            .catalog
            .folders()?
            .into_iter()
            .find(|record| record.id == folder)
            .with_context(|| format!("FOLDER_NOT_FOUND: {folder}"))?;
        let end = Instant::now() + Self::START_WAIT;
        loop {
            // A stopping engine is on its way out: wait for its replacement.
            if let Some(engine) = lock(&self.shared.engines).get(&record.state)
                && !engine.stopping.load(Ordering::Acquire)
            {
                return Ok(engine.clone());
            }
            if Instant::now() >= end {
                bail!("WORKER_NOT_READY: {folder}");
            }
            thread::sleep(POLL);
        }
    }

    /// Runs the orchestrator's shutdown sequence and waits, bounded, for its
    /// thread and every worker thread: `STRANDED_WORKER` when one is left.
    pub fn stop(&self) -> Result<()> {
        match self.daemon() {
            Ok(daemon) => {
                daemon.execute("shutdown", &json!({}))?;
            }
            // It ended on its own, with its error.
            Err(_) if self.shared.finished.load(Ordering::Acquire) => {}
            Err(error) => return Err(error),
        }
        let end = Instant::now() + SHUTDOWN_BOUND + STOP_WAIT;
        while !self.shared.finished.load(Ordering::Acquire) {
            if Instant::now() >= end {
                bail!("STRANDED_WORKER: the orchestrator thread did not stop");
            }
            thread::sleep(POLL);
        }
        if let Some(handle) = lock(&self.orchestrator).take() {
            let _ = handle.join();
        }
        let end = Instant::now() + KILL_WAIT;
        loop {
            let left = lock(&self.shared.workers)
                .iter()
                .filter(|worker| !worker.exited())
                .count();
            if left == 0 {
                return Ok(());
            }
            if Instant::now() >= end {
                bail!("STRANDED_WORKER: {left} worker thread(s) did not stop");
            }
            thread::sleep(POLL);
        }
    }
}

struct ThreadWorker {
    /// Weak: the store's lock must go with the worker thread, even while the
    /// controller still holds this handle and is starting the next worker.
    engine: Weak<Engine>,
    exited: AtomicBool,
    /// Known once the worker reports `Ready`.
    instance: Mutex<Option<String>>,
}

impl EmbeddedWorker for ThreadWorker {
    fn exited(&self) -> bool {
        self.exited.load(Ordering::Acquire)
    }

    fn stop(&self) -> Result<bool> {
        match self.engine.upgrade() {
            Some(engine) if !self.exited() => {
                engine.request_shutdown()?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

/// Runs each folder's worker on a thread and keeps its engine for the host.
struct ThreadLauncher(Arc<Shared>);

impl Launcher for ThreadLauncher {
    fn spawn(&self, spec: &LaunchSpec, sink: Sender<ProcessEvent>) -> Result<WorkerProcess> {
        std::fs::create_dir_all(&spec.state)?;
        let engine = Engine::open(&spec.state, None)
            .map_err(|error| anyhow::Error::new(Fatal::store_open(&error)))?;
        let state = spec.state.clone();
        let shared = self.0.clone();
        lock(&shared.engines).insert(state.clone(), engine.clone());
        let handle = Arc::new(ThreadWorker {
            engine: Arc::downgrade(&engine),
            exited: AtomicBool::new(false),
            instance: Mutex::new(None),
        });
        // Registered now, not at `Ready`: a kill before then must find it.
        {
            let mut workers = lock(&shared.workers);
            workers.retain(|worker| !worker.exited());
            workers.push(handle.clone());
        }
        let (ready_tx, ready_rx) = mpsc::channel::<Ready>();
        {
            let sink = sink.clone();
            let handle = handle.clone();
            thread::Builder::new()
                .name("tkfs-embedded-ready".into())
                .spawn(move || {
                    if let Ok(ready) = ready_rx.recv() {
                        *lock(&handle.instance) = Some(ready.instance.clone());
                        let _ = sink.send(ProcessEvent::Ready(ready));
                    }
                })?;
        }
        let exiting = handle.clone();
        thread::Builder::new()
            .name("tkfs-embedded-worker".into())
            .spawn(move || {
                let options = worker::Options {
                    supervised: false,
                    mount: None,
                    on_ready: Some(ready_tx),
                };
                let result = worker::run(engine, options);
                lock(&shared.engines).remove(&state);
                exiting.exited.store(true, Ordering::Release);
                let code = match result {
                    Ok(()) => Some(0),
                    Err(error) => {
                        let fatal = match error.downcast::<Fatal>() {
                            Ok(fatal) => fatal,
                            Err(error) => Fatal::new(
                                "WORKER_ERROR",
                                format!("{error:#}"),
                                worker::EXIT_INTERNAL,
                            ),
                        };
                        let _ = sink.send(ProcessEvent::Fatal {
                            code: fatal.code.clone(),
                            message: fatal.message.clone(),
                        });
                        Some(fatal.exit)
                    }
                };
                let _ = sink.send(ProcessEvent::Exited {
                    code,
                    tail: String::new(),
                });
            })
            .inspect_err(|_| {
                // No thread will ever run it: it must not look stranded.
                handle.exited.store(true, Ordering::Release);
            })?;
        Ok(WorkerProcess::embedded(handle))
    }

    /// Workers die with the process, so a record left behind names nothing.
    fn adopt(
        &self,
        _: &Path,
        _: Sender<ProcessEvent>,
    ) -> Result<Option<(WorkerRecord, WorkerProcess)>> {
        Ok(None)
    }

    fn connect(&self, ready: &Ready, lanes: usize) -> Result<Arc<WorkerLink>, CallError> {
        WorkerLink::connect(ready, lanes)
    }

    /// A thread cannot be killed: asks it to stop and waits, bounded, so the
    /// store's lock is free before the controller starts the next worker.
    /// A record naming no known instance means a worker not yet `Ready`: the
    /// only such worker, if there is exactly one.
    fn kill_verified(&self, record: &WorkerRecord) -> Result<bool> {
        let worker = {
            let workers = lock(&self.0.workers);
            let named = workers
                .iter()
                .find(|worker| lock(&worker.instance).as_deref() == Some(record.instance.as_str()));
            named.cloned().or_else(|| {
                let mut unready = workers
                    .iter()
                    .filter(|worker| !worker.exited() && lock(&worker.instance).is_none());
                let only = unready.next()?;
                unready.next().is_none().then(|| only.clone())
            })
        };
        let Some(worker) = worker else {
            return Ok(false);
        };
        // A failed final flush still stops it; unsealed data is recovered at open.
        let _ = worker.stop();
        let end = Instant::now() + KILL_WAIT;
        while !worker.exited() {
            if Instant::now() >= end {
                return Err(anyhow!("STRANDED_WORKER: {} did not stop", record.instance));
            }
            thread::sleep(POLL);
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A worker that has not reported `Ready` is registered at spawn, so a
    /// kill naming it by the record the controller would hold still waits for
    /// its thread instead of answering `false`.
    #[test]
    fn a_kill_before_ready_stops_and_waits_for_the_thread() {
        let shared = Arc::new(Shared {
            daemon: OnceLock::new(),
            failure: Mutex::new(None),
            finished: AtomicBool::new(false),
            engines: Mutex::new(BTreeMap::new()),
            workers: Mutex::new(Vec::new()),
        });
        let launcher = ThreadLauncher(shared.clone());
        let state = tempfile::tempdir().unwrap();
        let spec = LaunchSpec {
            executable: PathBuf::new(),
            state: state.path().join("folder"),
            mount: None,
        };
        let (sink, _events) = mpsc::channel();
        let process = launcher.spawn(&spec, sink).unwrap();
        let record = WorkerRecord {
            format: 1,
            pid: process.pid,
            start_ms: 0,
            image: PathBuf::new(),
            version: String::new(),
            instance: "not-yet-known".into(),
            address: String::new(),
            ipc: 0,
        };
        assert!(launcher.kill_verified(&record).unwrap());
        assert!(process.exited().unwrap());
        assert!(lock(&shared.workers).iter().all(|worker| worker.exited()));
    }
}
