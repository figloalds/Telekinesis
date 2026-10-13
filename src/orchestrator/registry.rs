//! Owned by WP1.2.
//!
//! The `FolderRegistry` behind `FolderDirectory` (REVAMP_PLAN §3.3, §14 WP1.2).
//!
//! * `state → handle{tx, Snapshot<FolderView>, client}` plus a by-repo index,
//!   in one `Tracked` map that is only held for map operations (never across
//!   I/O or a controller call).
//! * A `folder-registry` thread waits on the catalog generation and spawns,
//!   retires and notifies (`CatalogChanged`) controllers.
//! * The aggregate generation is a `Notify` poked by every published view.
//! * `maximum_running` is enforced through shared slots: a Start that cannot
//!   get one answers `WORKER_LIMIT`, and a folder that waited is started when a
//!   slot frees (the legacy reconcile did the same every 2 s).
//! * `shutdown(deadline)` stops every folder in parallel and returns one
//!   outcome per folder by the deadline.
use super::folder::{self, ControllerDeps, ControllerHandle, Msg, Slots};
use crate::{
    model::{
        errors::ErrorWire,
        orchestrator::{
            CatalogApi, CatalogSnapshot, FolderCmd, FolderDirectory, FolderView,
            PublishedCatalogApi, ShutdownOutcome, StatusSink, WorkerClient, WorkerLauncher,
            outcomes,
        },
    },
    util::{
        cancel::Cancel,
        lock::{Tracked, lock},
        log::event,
        notify::Notify,
        snapshot::Snapshot,
        time::Clock,
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, RecvTimeoutError},
    },
    thread,
    time::{Duration, Instant},
};

pub use super::folder::LifecycleLimits;

/// The registry waits on the catalog generation in slices this long.
const CATALOG_WAIT: Duration = Duration::from_millis(500);
/// Shutdown collects late outcomes this long past the deadline.
const SHUTDOWN_GRACE: Duration = Duration::from_millis(250);

/// What `FolderRegistry::start` needs (§14 WP1.2).
pub struct RegistryDeps {
    pub catalog: Arc<dyn CatalogApi>,
    pub published: Arc<dyn PublishedCatalogApi>,
    pub status: Arc<dyn StatusSink>,
    pub launcher: Arc<dyn WorkerLauncher>,
    pub clock: Arc<dyn Clock>,
    pub limits: LifecycleLimits,
}

/// One FolderController per catalog folder; implements `FolderDirectory`.
pub struct FolderRegistry {
    deps: Arc<ControllerDeps>,
    inner: Tracked<Inner>,
    stopping: AtomicBool,
    shutdown_started: AtomicBool,
    outcomes: Snapshot<Option<Vec<ShutdownOutcome>>>,
    watcher: Cancel,
    /// Held by a whole `reconcile` (its controllers spawned and inserted) and
    /// taken by `shutdown` after `stopping` is set, so a reconcile either
    /// finishes before shutdown collects the handles or sees `stopping`.
    reconciling: Mutex<()>,
}

#[derive(Default)]
struct Inner {
    handles: BTreeMap<String, ControllerHandle>,
    by_repo: BTreeMap<String, String>,
}

fn not_found(state: &str) -> ErrorWire {
    ErrorWire::new(
        "FOLDER_NOT_FOUND",
        format!("FOLDER_NOT_FOUND: no folder with state {state}"),
    )
}

fn stopping() -> ErrorWire {
    ErrorWire::new(
        "ORCHESTRATOR_STOPPING",
        "ORCHESTRATOR_STOPPING: folders are shutting down",
    )
}

fn unavailable(state: &str) -> ErrorWire {
    ErrorWire::new(
        "FOLDER_UNAVAILABLE",
        format!("FOLDER_UNAVAILABLE: the controller of {state} is not running"),
    )
}

impl FolderRegistry {
    /// Spawns a controller for every catalog folder (each adopts or starts its
    /// worker when `desired_running`) and the catalog watcher thread.
    pub fn start(deps: RegistryDeps) -> Arc<FolderRegistry> {
        let slots = Arc::new(Slots::new(deps.limits.maximum_running));
        let controller_deps = Arc::new(ControllerDeps {
            catalog: deps.catalog,
            published: deps.published,
            status: deps.status,
            launcher: deps.launcher,
            clock: deps.clock,
            limits: deps.limits,
            slots: slots.clone(),
            changes: Arc::new(Notify::new()),
        });
        let registry = Arc::new(FolderRegistry {
            deps: controller_deps,
            inner: Tracked::new("folder-registry", Inner::default()),
            stopping: AtomicBool::new(false),
            shutdown_started: AtomicBool::new(false),
            outcomes: Snapshot::new(None),
            watcher: Cancel::new(),
            reconciling: Mutex::new(()),
        });
        let weak = Arc::downgrade(&registry);
        slots.set_waker(Box::new(move |states| {
            if let Some(registry) = weak.upgrade() {
                for state in states {
                    let _ = registry.post(&state, FolderCmd::Tick);
                }
            }
        }));
        let snapshot = registry.deps.catalog.snapshot();
        registry.reconcile(&snapshot);
        let weak = Arc::downgrade(&registry);
        let catalog = registry.deps.catalog.clone();
        let cancel = registry.watcher.clone();
        let generation = snapshot.generation;
        let spawned = thread::Builder::new()
            .name("folder-registry".into())
            .spawn(move || watch_catalog(weak, catalog, cancel, generation));
        if let Err(error) = spawned {
            event!(error, CATALOG_UNAVAILABLE, {
                "detail": "cannot start the folder-registry thread; catalog changes are not followed",
                "error": error.to_string(),
            });
        }
        registry
    }

    /// Folders with a controller.
    pub fn len(&self) -> usize {
        self.inner.lock().handles.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Folders currently holding a `maximum_running` slot.
    pub fn running_slots(&self) -> usize {
        self.deps.slots.held()
    }

    /// Spawns, retires and notifies controllers for a catalog snapshot.
    ///
    /// Serialized with `shutdown` (`reconciling`): every controller this
    /// spawns is in the map before shutdown collects the handles, or none is
    /// spawned. The lock covers channel sends and thread spawns only, never a
    /// catalog or worker call.
    fn reconcile(&self, snapshot: &CatalogSnapshot) {
        let _serial = lock(&self.reconciling);
        if self.stopping.load(Ordering::Acquire) {
            return;
        }
        let (existing, retired, new) = {
            let mut inner = self.inner.lock();
            inner.by_repo = snapshot
                .folders
                .iter()
                .map(|folder| (folder.repo.clone(), folder.state.clone()))
                .collect();
            let gone: Vec<String> = inner
                .handles
                .keys()
                .filter(|state| snapshot.folder(state).is_none())
                .cloned()
                .collect();
            let retired: Vec<_> = gone
                .iter()
                .filter_map(|state| inner.handles.remove(state))
                .map(|handle| handle.tx)
                .collect();
            let existing: Vec<_> = inner
                .handles
                .values()
                .map(|handle| handle.tx.clone())
                .collect();
            let new: Vec<_> = snapshot
                .folders
                .iter()
                .filter(|folder| !inner.handles.contains_key(&folder.state))
                .cloned()
                .collect();
            (existing, retired, new)
        };
        for tx in retired {
            let _ = tx.send(Msg::Retire);
        }
        for tx in existing {
            let _ = tx.send(Msg::Cmd(FolderCmd::CatalogChanged, None));
        }
        let mut spawned = Vec::new();
        for record in new {
            let state = record.state.clone();
            match folder::spawn(record, self.deps.clone()) {
                Ok(handle) => spawned.push((state, handle)),
                Err(error) => {
                    event!(error, FOLDER_UNAVAILABLE, {
                        "folder": state,
                        "detail": "cannot start the folder controller thread",
                        "error": error.to_string(),
                    });
                }
            }
        }
        if !spawned.is_empty() {
            let mut inner = self.inner.lock();
            for (state, handle) in spawned {
                inner.handles.entry(state).or_insert(handle);
            }
        }
        self.deps.changes.poke();
    }

    fn lookup(&self, state: &str) -> Result<mpsc::Sender<Msg>, ErrorWire> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(stopping());
        }
        self.inner
            .lock()
            .handles
            .get(state)
            .map(|handle| handle.tx.clone())
            .ok_or_else(|| not_found(state))
    }

    /// Delivers a command without the `WORKER_LIMIT` pre-check.
    fn post(&self, state: &str, command: FolderCmd) -> Result<(), ErrorWire> {
        self.lookup(state)?
            .send(Msg::Cmd(command, None))
            .map_err(|_| unavailable(state))
    }

    fn check_limit(&self, state: &str, command: &FolderCmd) -> Result<(), ErrorWire> {
        if *command == FolderCmd::Start && !self.deps.slots.admits(state) {
            return Err(ErrorWire::new(
                "WORKER_LIMIT",
                format!(
                    "WORKER_LIMIT: {} folders already hold a worker (maximum_running)",
                    self.deps.limits.maximum_running
                ),
            ));
        }
        Ok(())
    }
}

impl FolderDirectory for FolderRegistry {
    fn view(&self, state: &str) -> Option<Arc<FolderView>> {
        self.inner
            .lock()
            .handles
            .get(state)
            .map(|handle| handle.view.load())
    }

    fn by_repo(&self, repo: &str) -> Option<Arc<FolderView>> {
        let inner = self.inner.lock();
        inner
            .by_repo
            .get(repo)
            .and_then(|state| inner.handles.get(state))
            .map(|handle| handle.view.load())
    }

    fn all(&self) -> Vec<Arc<FolderView>> {
        self.inner
            .lock()
            .handles
            .values()
            .map(|handle| handle.view.load())
            .collect()
    }

    fn send(&self, state: &str, command: FolderCmd) -> Result<(), ErrorWire> {
        let tx = self.lookup(state)?;
        self.check_limit(state, &command)?;
        tx.send(Msg::Cmd(command, None))
            .map_err(|_| unavailable(state))
    }

    fn request(
        &self,
        state: &str,
        command: FolderCmd,
        wait: Duration,
    ) -> Result<Arc<FolderView>, ErrorWire> {
        let (tx, view) = {
            if self.stopping.load(Ordering::Acquire) {
                return Err(stopping());
            }
            let inner = self.inner.lock();
            let handle = inner.handles.get(state).ok_or_else(|| not_found(state))?;
            (handle.tx.clone(), handle.view.clone())
        };
        self.check_limit(state, &command)?;
        let (reply, answer) = mpsc::channel();
        tx.send(Msg::Cmd(command, Some(reply)))
            .map_err(|_| unavailable(state))?;
        match answer.recv_timeout(wait) {
            Ok(result) => result,
            // Not settled yet: the caller reads the phase of the latest view.
            Err(RecvTimeoutError::Timeout) => Ok(view.load()),
            Err(RecvTimeoutError::Disconnected) => {
                if self.stopping.load(Ordering::Acquire) {
                    Err(stopping())
                } else {
                    Err(unavailable(state))
                }
            }
        }
    }

    fn client(&self, state: &str) -> Option<Arc<dyn WorkerClient>> {
        let client = self.inner.lock().handles.get(state)?.client.clone();
        lock(&client).clone()
    }

    fn generation(&self) -> u64 {
        self.deps.changes.count()
    }

    fn wait_newer(&self, generation: u64, timeout: Duration) -> u64 {
        self.deps.changes.wait(generation, timeout)
    }

    fn shutdown(&self, deadline: Duration) -> Vec<ShutdownOutcome> {
        let started = Instant::now();
        let until = started.checked_add(deadline).unwrap_or(started);
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            // Another caller runs it: wait for its outcomes.
            let hard = until + SHUTDOWN_GRACE;
            loop {
                let (outcomes, generation) = self.outcomes.load_versioned();
                if let Some(outcomes) = outcomes.as_ref() {
                    return outcomes.clone();
                }
                let remaining = hard.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Vec::new();
                }
                self.outcomes.wait_newer(generation, remaining);
            }
        }
        self.stopping.store(true, Ordering::Release);
        self.watcher.cancel();
        // Wait for a reconcile in progress: its controllers are inserted
        // before the handles are collected, and any later one sees `stopping`.
        drop(lock(&self.reconciling));
        let handles: Vec<(String, mpsc::Sender<Msg>)> = self
            .inner
            .lock()
            .handles
            .iter()
            .map(|(state, handle)| (state.clone(), handle.tx.clone()))
            .collect();
        let (reply, answers) = mpsc::channel();
        let mut missing = BTreeSet::new();
        let mut outcomes = Vec::new();
        for (state, tx) in handles {
            if tx.send(Msg::Shutdown(until, reply.clone())).is_ok() {
                missing.insert(state);
            } else {
                outcomes.push(ShutdownOutcome {
                    state: state.clone(),
                    outcome: outcomes::NOT_RUNNING.into(),
                    error: Some(unavailable(&state)),
                });
            }
        }
        drop(reply);
        let hard = until + SHUTDOWN_GRACE;
        while !missing.is_empty() {
            let remaining = hard.saturating_duration_since(Instant::now());
            match answers.recv_timeout(remaining) {
                Ok(outcome) => {
                    if missing.remove(&outcome.state) {
                        outcomes.push(outcome);
                    }
                }
                Err(_) => break,
            }
        }
        for state in missing {
            outcomes.push(ShutdownOutcome {
                state,
                outcome: outcomes::LEFT_RUNNING.into(),
                error: Some(ErrorWire::new(
                    "REQUEST_TIMEOUT",
                    "REQUEST_TIMEOUT: the folder controller did not answer before the shutdown deadline",
                )),
            });
        }
        outcomes.sort_by(|a, b| a.state.cmp(&b.state));
        self.outcomes.store(Some(outcomes.clone()));
        outcomes
    }
}

/// The `folder-registry` thread: follows the catalog generation.
fn watch_catalog(
    registry: Weak<FolderRegistry>,
    catalog: Arc<dyn CatalogApi>,
    cancel: Cancel,
    mut generation: u64,
) {
    while !cancel.is_cancelled() {
        let started = Instant::now();
        let snapshot = catalog.wait_newer(generation, CATALOG_WAIT);
        if cancel.is_cancelled() {
            break;
        }
        if snapshot.generation == generation {
            // A catalog that answers at once without a change (its thread died):
            // do not spin.
            if started.elapsed() < CATALOG_WAIT / 10 && cancel.sleep(CATALOG_WAIT) {
                break;
            }
            continue;
        }
        generation = snapshot.generation;
        let Some(registry) = registry.upgrade() else {
            break;
        };
        registry.reconcile(&snapshot);
    }
}
