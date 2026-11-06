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
        // Both threads or neither: on a failure nothing will ever run this
        // worker, so its engine must not be handed out and it must not look
        // stranded. (Dropping the closure drops `ready_tx`, ending the other.)
        let started = (|| -> std::io::Result<()> {
            let ready_sink = sink.clone();
            let ready_handle = handle.clone();
            thread::Builder::new()
                .name("tkfs-embedded-ready".into())
                .spawn(move || {
                    if let Ok(ready) = ready_rx.recv() {
                        *lock(&ready_handle.instance) = Some(ready.instance.clone());
                        let _ = ready_sink.send(ProcessEvent::Ready(ready));
                    }
                })?;
            let exiting = handle.clone();
            let (shared, state) = (shared.clone(), state.clone());
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
                })?;
            Ok(())
        })();
        if let Err(error) = started {
            lock(&self.0.engines).remove(&spec.state);
            handle.exited.store(true, Ordering::Release);
            return Err(error.into());
        }
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
    /// Matches by instance only: a record exists only after `Ready`, and a
    /// worker that has not reached it is stopped through its
    /// [`WorkerProcess`] and waited for by [`Host::stop`].
    fn kill_verified(&self, record: &WorkerRecord) -> Result<bool> {
        let worker = lock(&self.0.workers)
            .iter()
            .find(|worker| lock(&worker.instance).as_deref() == Some(record.instance.as_str()))
            .cloned();
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

    fn shared() -> Arc<Shared> {
        Arc::new(Shared {
            daemon: OnceLock::new(),
            failure: Mutex::new(None),
            finished: AtomicBool::new(false),
            engines: Mutex::new(BTreeMap::new()),
            workers: Mutex::new(Vec::new()),
        })
    }

    fn spawn(
        launcher: &ThreadLauncher,
        dir: &Path,
        name: &str,
    ) -> (WorkerProcess, mpsc::Receiver<ProcessEvent>) {
        let spec = LaunchSpec {
            executable: PathBuf::new(),
            state: dir.join(name),
            mount: None,
        };
        let (sink, events) = mpsc::channel();
        (launcher.spawn(&spec, sink).unwrap(), events)
    }

    fn record(instance: &str) -> WorkerRecord {
        WorkerRecord {
            format: 1,
            pid: std::process::id(),
            start_ms: 0,
            image: PathBuf::new(),
            version: String::new(),
            instance: instance.into(),
            address: String::new(),
            ipc: 0,
        }
    }

    fn wait_exited(process: &WorkerProcess) {
        let end = Instant::now() + Duration::from_secs(20);
        while !process.exited().unwrap() {
            assert!(Instant::now() < end, "worker did not exit");
            thread::sleep(POLL);
        }
    }

    #[test]
    fn a_record_naming_no_known_instance_stops_no_worker() {
        let launcher = ThreadLauncher(shared());
        let dir = tempfile::tempdir().unwrap();
        let (a, _a_events) = spawn(&launcher, dir.path(), "a");
        let (b, _b_events) = spawn(&launcher, dir.path(), "b");
        assert!(!launcher.kill_verified(&record("stale")).unwrap());
        assert!(!a.exited().unwrap());
        assert!(!b.exited().unwrap());
        a.kill().unwrap();
        b.kill().unwrap();
        wait_exited(&a);
        wait_exited(&b);
    }

    #[test]
    fn a_ready_named_worker_is_found_and_stopped() {
        let launcher = ThreadLauncher(shared());
        let dir = tempfile::tempdir().unwrap();
        let (a, events) = spawn(&launcher, dir.path(), "a");
        let (other, _other_events) = spawn(&launcher, dir.path(), "other");
        let ready = loop {
            match events.recv_timeout(Duration::from_secs(20)).unwrap() {
                ProcessEvent::Ready(ready) => break ready,
                _ => continue,
            }
        };
        assert!(launcher.kill_verified(&record(&ready.instance)).unwrap());
        assert!(a.exited().unwrap());
        assert!(!other.exited().unwrap(), "an unrelated worker was stopped");
        other.kill().unwrap();
        wait_exited(&other);
    }

    #[test]
    fn host_stop_waits_for_a_worker_stopped_before_ready() {
        let shared = shared();
        // An orchestrator that already ended, so `stop` goes straight to workers.
        *lock(&shared.failure) = Some("ended".into());
        shared.finished.store(true, Ordering::Release);
        let launcher = ThreadLauncher(shared.clone());
        let dir = tempfile::tempdir().unwrap();
        let (worker, _events) = spawn(&launcher, dir.path(), "a");
        assert!(worker.kill().unwrap());
        let host = Host {
            shared: shared.clone(),
            orchestrator: Mutex::new(None),
        };
        host.stop().unwrap();
        assert!(worker.exited().unwrap());
        assert!(lock(&shared.workers).iter().all(|worker| worker.exited()));
    }
}
