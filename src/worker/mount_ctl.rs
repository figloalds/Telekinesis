//! Owned by WP1.5.
//!
//! The worker's mount controller (REVAMP_PLAN §4.3) and the adapter support
//! shared by the WinFsp (`mount.rs`) and FUSE (`fuse_linux.rs`) adapters.
//!
//! # Controller
//!
//! - **States** (`MountState`): `Detached` → `Preflight` → `Attaching` →
//!   `Attached{since_ms}`, or `AttachFailed{error, attempt, next_retry_at_ms}`;
//!   `Attached` → `Detaching` → `Detached`; `Attached` → `Lost{error}` when the
//!   mount goes away underneath us (FUSE dispatch death, failed checkout
//!   remount). A refused graceful detach restores the previous state.
//! - **Mailbox:** attach and detach run one at a time on the `mount-ctl`
//!   thread (§2.5). Callers enqueue a transition and wait for its reply, at
//!   most [`ATTACH_WAIT`]/[`DETACH_WAIT`] (then `WORKER_BUSY`, Contention; the
//!   transition goes on and `status()` shows it); no lock is held across
//!   mount I/O, and nothing else waits on a transition.
//! - **Lost** (`Attached`/`Attaching` → `Lost`) is signalled by the adapter.
//!   One that arrives while a detach runs is remembered: a detach refused
//!   afterwards publishes `Lost`, never the `Attached` it started from.
//! - **`status()`** reads a published snapshot. It never takes the engine lock
//!   and never waits for a transition in progress; the snapshot generation
//!   ([`MountController::generation`]) bumps on every state change. A mount
//!   driven around the controller (the legacy `mount::start`/`mount::stop`
//!   wrappers) is reconciled on the next attach or detach; a controller never
//!   adopts or detaches a mount serving another engine.
//! - **Preflight** resolves the path (nearest existing ancestor canonicalized),
//!   refuses an overlap with the folder state (`MOUNT_OVERLAPS_STATE`), creates
//!   the parent directory, and on Linux recovers a stale mount proven ours by
//!   the previous `worker.json` (invariant 27). `MOUNT_PATH_MUST_NOT_EXIST`,
//!   `MOUNT_OVERLAPS_*` and `WINFSP_RUNTIME_UNAVAILABLE` are NeedsUserAction;
//!   every other attach failure is Transient and carries the
//!   `Backoff::mount_retry` time the caller should retry at (the worker never
//!   retries by itself; the FolderController does, §3.3).
//! - **Mount errors never write `Engine.health`.** Attach failures live in the
//!   controller state; callback failures are counted ([`adapter::mount_errors`])
//!   and logged at a bounded rate.
//!
//! # Adapters (`crate::mount`, one per platform, same names)
//!
//! `bind(engine)` (idempotent for the same `Arc`, independent of any path
//! check), `attach(path)` (a fresh native instance per attach), `detach(force)`,
//! `inflight()`, `attached()`, `checkout(..)` (unchanged signature) and the
//! legacy wrappers `start`/`stop`. Detach order (§4.3):
//!
//! 1. fence (`Engine.switching = true`); a graceful detach first waits up to
//!    `adapter::QUIET_WAIT` for quiet (no dirty session, no handle the
//!    application still holds), else `BUSY_VIEW`;
//! 2. drain in-flight callbacks (≤ `DETACH_DRAIN`; a graceful detach that does
//!    not drain is refused with `BUSY_VIEW` and unfenced);
//! 3. force: `retain_all_dirty()` (invariant 8);
//! 4. stop the native instance;
//! 5. drop orphaned handles; dirty sessions are retained again (writes that
//!    raced the drain) and **kept** in memory with no references, so the
//!    pending-save retry commits them and a re-attach shows their bytes; clean
//!    sessions are dropped;
//! 6. clear the fence, `Engine.mounted = false`;
//! 7. the controller publishes `Detached`.
use crate::{
    model::{
        errors::{ErrorClass, ErrorWire},
        health::{MountControl, MountIntent, MountState, MountStatus},
        ipc::WorkerRecord,
    },
    runtime::{Shared, lock_engine},
    util::{backoff::Backoff, lock::lock, snapshot::Snapshot, time::now_ms},
};
use anyhow::Context;
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicU32, Ordering},
        mpsc,
    },
    time::Duration,
};

/// How long an attach caller waits for its reply (queue included) before
/// `WORKER_BUSY`; below the orchestrator's `deadlines::MOUNT_ATTACH` (60 s).
pub const ATTACH_WAIT: Duration = Duration::from_secs(50);
/// How long a detach caller waits for its reply (queue included) before
/// `WORKER_BUSY`: the quiet wait (1 s), the drain (10 s) and the native stop
/// fit, and it stays below `deadlines::MOUNT_DETACH` (15 s).
pub const DETACH_WAIT: Duration = Duration::from_secs(14);

/// Attach failures that need the user (§4.3 preflight); every other attach
/// failure is Transient.
pub const NEEDS_USER_ACTION: &[&str] = &[
    "MOUNT_PATH_MUST_NOT_EXIST",
    "MOUNT_OVERLAPS_DATA",
    "MOUNT_OVERLAPS_FOLDER",
    "MOUNT_OVERLAPS_STATE",
    "WINFSP_RUNTIME_UNAVAILABLE",
];

/// The class of an attach failure: NeedsUserAction for [`NEEDS_USER_ACTION`],
/// Transient otherwise.
pub fn attach_class(code: &str) -> ErrorClass {
    if NEEDS_USER_ACTION.contains(&code) {
        ErrorClass::NeedsUserAction
    } else {
        ErrorClass::Transient
    }
}

type Reply = mpsc::Sender<Result<MountStatus, ErrorWire>>;

enum Command {
    Attach(PathBuf, Reply),
    Detach(bool, Reply),
}

/// The worker's `MountControl` (§4.3). One per worker process.
pub struct MountController {
    inner: Arc<Inner>,
    /// The `mount-ctl` thread's mailbox; `None` if the thread could not start.
    mailbox: Option<mpsc::Sender<Command>>,
}

struct Inner {
    engine: Shared,
    previous: Option<WorkerRecord>,
    status: Snapshot<MountStatus>,
    /// Set by [`MountController::set_instance`].
    instance: Mutex<Option<String>>,
    /// Used when neither `set_instance` nor this process's `worker.json`
    /// names the instance (such a mount is never provably ours later).
    generated: String,
    /// Consecutive failed attaches.
    attempts: AtomicU32,
    /// A `Lost` signalled while a detach ran (taken by that detach).
    lost_while_detaching: Mutex<Option<ErrorWire>>,
    /// Overrides [`ATTACH_WAIT`] and [`DETACH_WAIT`] ([`MountController::set_reply_wait`]).
    reply_wait: Mutex<Option<Duration>>,
    /// The store root and identity, read once from the engine.
    identity: OnceLock<Identity>,
    me: Weak<Inner>,
}

struct Identity {
    root: PathBuf,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    repo: String,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    device: String,
}

impl MountController {
    /// A detached controller for `engine`. `previous` is the previous
    /// `worker.json` of this folder state (FUSE recovery, invariant 27).
    pub fn new(engine: Shared, previous: Option<WorkerRecord>) -> Arc<Self> {
        let inner = Arc::new_cyclic(|me| Inner {
            engine,
            previous,
            status: Snapshot::new(MountStatus {
                intent: MountIntent::Headless,
                path: None,
                state: MountState::Detached,
            }),
            instance: Mutex::new(None),
            generated: uuid::Uuid::new_v4().to_string(),
            attempts: AtomicU32::new(0),
            lost_while_detaching: Mutex::new(None),
            reply_wait: Mutex::new(None),
            identity: OnceLock::new(),
            me: me.clone(),
        });
        let (sender, receiver) = mpsc::channel();
        let worker = inner.clone();
        let mailbox = std::thread::Builder::new()
            .name("mount-ctl".into())
            .spawn(move || worker.serve(receiver))
            .ok()
            .map(|_| sender);
        Arc::new(Self { inner, mailbox })
    }

    /// The previous `worker.json` this controller was built with.
    pub fn previous(&self) -> Option<&WorkerRecord> {
        self.inner.previous.as_ref()
    }

    /// Names the worker instance the mount is attributed to (the FUSE source
    /// `tkfs-<instance>`). Without it the controller uses the `instance` of
    /// this process's `STATE/worker.json`, else a generated one.
    pub fn set_instance(&self, instance: &str) {
        *lock(&self.inner.instance) = Some(instance.to_owned());
    }

    /// The instance the next attach attributes its mount to.
    pub fn instance(&self) -> String {
        self.inner.instance()
    }

    /// Generation of the published status; bumps on every state change.
    pub fn generation(&self) -> u64 {
        self.inner.status.generation()
    }

    /// Callbacks currently running in the native adapter.
    pub fn inflight(&self) -> usize {
        adapter::inflight()
    }

    /// Mount-scoped callback failures since the process started.
    pub fn mount_errors(&self) -> u64 {
        adapter::mount_errors()
    }

    /// Bounds how long `attach`/`detach` callers wait for their reply
    /// (`None`: [`ATTACH_WAIT`] and [`DETACH_WAIT`]).
    pub fn set_reply_wait(&self, wait: Option<Duration>) {
        *lock(&self.inner.reply_wait) = wait;
    }

    /// Enqueues a transition and waits for its reply, at most `wait`: a
    /// transition that takes longer (a hung native stop or `fusermount3`)
    /// answers `WORKER_BUSY` and goes on; `status()` shows where it is.
    fn call(
        &self,
        wait: Duration,
        command: impl FnOnce(Reply) -> Command,
    ) -> Result<MountStatus, ErrorWire> {
        let unavailable =
            || ErrorWire::coded("MOUNT_NOT_RUNNING", "the mount-ctl thread is not running");
        let mailbox = self.mailbox.as_ref().ok_or_else(unavailable)?;
        let (reply, answer) = mpsc::channel();
        mailbox.send(command(reply)).map_err(|_| unavailable())?;
        let wait = lock(&self.inner.reply_wait).unwrap_or(wait);
        match answer.recv_timeout(wait) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => Err(ErrorWire::coded(
                "WORKER_BUSY",
                format!(
                    "a mount transition is still running after {} ms (state {:?})",
                    wait.as_millis(),
                    self.inner.current().state
                ),
            )
            .with_retry_after(1000)),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(unavailable()),
        }
    }
}

impl MountControl for MountController {
    fn attach(&self, path: &Path) -> Result<MountStatus, ErrorWire> {
        self.call(ATTACH_WAIT, |reply| Command::Attach(path.to_owned(), reply))
    }

    fn detach(&self, force: bool) -> Result<MountStatus, ErrorWire> {
        self.call(DETACH_WAIT, |reply| Command::Detach(force, reply))
    }

    fn status(&self) -> MountStatus {
        self.inner.current()
    }
}

impl Inner {
    /// The `mount-ctl` loop: one transition at a time, until the controller
    /// (the only sender) is dropped.
    fn serve(&self, mailbox: mpsc::Receiver<Command>) {
        while let Ok(command) = mailbox.recv() {
            let (reply, result) = match command {
                Command::Attach(path, reply) => (reply, self.guarded(|| self.attach(&path))),
                Command::Detach(force, reply) => (reply, self.guarded(|| self.detach(force))),
            };
            let _ = reply.send(result);
        }
    }

    /// Runs a transition; a panic publishes the state the adapter is in.
    fn guarded(
        &self,
        transition: impl FnOnce() -> Result<MountStatus, ErrorWire>,
    ) -> Result<MountStatus, ErrorWire> {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(transition)).unwrap_or_else(|_| {
            let error = ErrorWire::coded("UNKNOWN_ERROR", "mount transition panicked");
            let current = self.current();
            let state = match self.native() {
                Some(_) => MountState::Lost {
                    error: error.clone(),
                },
                None => MountState::Detached,
            };
            self.publish(current.intent, current.path, state);
            Err(error)
        })
    }

    fn current(&self) -> MountStatus {
        (*self.status.load()).clone()
    }

    /// The native instance serving this controller's engine, if any (another
    /// engine's mount is never adopted or detached).
    fn native(&self) -> Option<PathBuf> {
        crate::mount::attached().filter(|_| crate::mount::serves(&self.engine))
    }

    fn publish(
        &self,
        intent: MountIntent,
        path: Option<PathBuf>,
        state: MountState,
    ) -> MountStatus {
        let status = MountStatus {
            intent,
            path,
            state,
        };
        self.status.store(status.clone());
        status
    }

    fn identity(&self) -> &Identity {
        self.identity.get_or_init(|| {
            let e = lock_engine(&self.engine);
            Identity {
                root: e.store.root.clone(),
                repo: e.store.repo.clone(),
                device: e.store.device.clone(),
            }
        })
    }

    fn instance(&self) -> String {
        if let Some(instance) = lock(&self.instance).clone() {
            return instance;
        }
        self.own_record()
            .map(|record| record.instance)
            .unwrap_or_else(|| self.generated.clone())
    }

    /// This process's `STATE/worker.json` (written before the mount, §4.1).
    fn own_record(&self) -> Option<WorkerRecord> {
        let bytes = std::fs::read(self.identity().root.join("worker.json")).ok()?;
        let record: WorkerRecord = serde_json::from_slice(&bytes).ok()?;
        (record.pid == std::process::id() && uuid::Uuid::parse_str(&record.instance).is_ok())
            .then_some(record)
    }

    fn attach(&self, path: &Path) -> Result<MountStatus, ErrorWire> {
        let mut current = self.current();
        if current.state.is_attached() && self.native().is_none() {
            // Detached outside the controller (legacy `mount::stop`).
            current = self.publish(MountIntent::Detach, current.path, MountState::Detached);
        }
        if self.native().is_some()
            && !matches!(current.state, MountState::Lost { .. })
            && crate::mount::vanished()
        {
            // Unmounted outside the worker before the adapter noticed (a lazy
            // unmount keeps the connection while files are open): lost, so it
            // is cleaned up and attached afresh below, never reported attached.
            current = self.publish(
                current.intent,
                current.path,
                MountState::Lost {
                    error: ErrorWire::coded(
                        "MOUNT_NOT_RUNNING",
                        "the mount was unmounted outside the worker",
                    ),
                },
            );
        }
        if current.state.is_attached() {
            if current.path.as_deref() == Some(path) {
                return Ok(current);
            }
            return Err(ErrorWire::coded(
                "ONE_MOUNT_PER_RUNTIME",
                format!(
                    "attached at {}; detach first",
                    current.path.unwrap_or_default().display()
                ),
            ));
        }
        if let Some(native) = self.native() {
            if !matches!(current.state, MountState::Lost { .. }) {
                // Started outside the controller (legacy `mount::start`):
                // report what is really mounted.
                let status = self.publish(
                    MountIntent::Attach,
                    Some(native.clone()),
                    MountState::Attached { since_ms: now_ms() },
                );
                return if native == path {
                    Ok(status)
                } else {
                    Err(ErrorWire::coded(
                        "ONE_MOUNT_PER_RUNTIME",
                        format!("attached at {}; detach first", native.display()),
                    ))
                };
            }
            // A lost native instance (dead dispatch): clean it up first.
            if let Err(error) = crate::mount::detach(true) {
                return Err(self.attach_failed(path, &error));
            }
        }
        self.publish(
            MountIntent::Attach,
            Some(path.to_owned()),
            MountState::Preflight,
        );
        let attached = (|| -> anyhow::Result<()> {
            crate::mount::bind(&self.engine)?;
            let resolved = self.preflight(path)?;
            self.publish(
                MountIntent::Attach,
                Some(path.to_owned()),
                MountState::Attaching,
            );
            self.watch_lost();
            self.native_attach(&resolved)
        })();
        if let Err(error) = attached {
            return Err(self.attach_failed(path, &error));
        }
        self.attempts.store(0, Ordering::Relaxed);
        let since_ms = now_ms();
        self.status.update(|status| {
            if status.state == MountState::Attaching {
                MountStatus {
                    intent: MountIntent::Attach,
                    path: Some(path.to_owned()),
                    state: MountState::Attached { since_ms },
                }
            } else {
                status.clone()
            }
        });
        let now = self.current();
        match now.state.error() {
            None if now.state.is_attached() => Ok(now),
            error => Err(error.cloned().unwrap_or_else(|| {
                ErrorWire::coded("MOUNT_NOT_RUNNING", "the mount went away while attaching")
            })),
        }
    }

    #[cfg(target_os = "linux")]
    fn native_attach(&self, resolved: &Path) -> anyhow::Result<()> {
        crate::mount::attach_managed(resolved, &self.instance())
    }

    #[cfg(not(target_os = "linux"))]
    fn native_attach(&self, resolved: &Path) -> anyhow::Result<()> {
        crate::mount::attach(resolved)
    }

    fn attach_failed(&self, path: &Path, error: &anyhow::Error) -> ErrorWire {
        let attempt = self
            .attempts
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let mut wire = ErrorWire::from_error(error);
        wire.class = attach_class(&wire.code);
        let next_retry_at_ms = (wire.class == ErrorClass::Transient).then(|| {
            let delay = Backoff::mount_retry().nominal(attempt);
            now_ms().saturating_add(u64::try_from(delay.as_millis()).unwrap_or(u64::MAX))
        });
        self.publish(
            MountIntent::Attach,
            Some(path.to_owned()),
            MountState::AttachFailed {
                error: wire.clone(),
                attempt,
                next_retry_at_ms,
            },
        );
        wire
    }

    /// Path checks before any native work (§4.3). Returns the resolved path
    /// the adapter mounts at.
    fn preflight(&self, path: &Path) -> anyhow::Result<PathBuf> {
        let resolved = resolve(path)?;
        let parent = resolved.parent().context("MOUNT_PARENT_REQUIRED")?;
        resolved.file_name().context("MOUNT_NAME_REQUIRED")?;
        let root = &self.identity().root;
        let root = std::fs::canonicalize(root).unwrap_or_else(|_| root.clone());
        anyhow::ensure!(
            !resolved.starts_with(&root) && !root.starts_with(&resolved),
            "MOUNT_OVERLAPS_STATE: {} overlaps the folder state {}",
            path.display(),
            root.display()
        );
        std::fs::create_dir_all(parent)?;
        #[cfg(target_os = "linux")]
        self.recover_stale(&resolved)?;
        crate::mount::vacant(&resolved)?;
        Ok(resolved)
    }

    /// Unmounts a stale FUSE mount at `resolved` only when the previous
    /// `worker.json` proves it ours: same repo and device, another instance,
    /// and the mount source names that instance (invariant 27).
    #[cfg(target_os = "linux")]
    fn recover_stale(&self, resolved: &Path) -> anyhow::Result<()> {
        let Some(previous) = self.previous.as_ref() else {
            return Ok(());
        };
        let identity = self.identity();
        let ours = previous.repo == identity.repo
            && previous.device == identity.device
            && uuid::Uuid::parse_str(&previous.instance).is_ok()
            && previous.instance != self.instance();
        if !ours {
            return Ok(());
        }
        crate::mount::recover_managed(resolved, &previous.instance)
    }

    fn watch_lost(&self) {
        let me = self.me.clone();
        adapter::set_lost_hook(Some(Arc::new(move |error| {
            if let Some(inner) = me.upgrade() {
                inner.lost(error);
            }
        })));
    }

    /// `Attached`/`Attaching` → `Lost` (signalled by the adapter). While a
    /// detach runs, the signal is remembered for that detach. Both run under
    /// the snapshot's write lock, so a detach that ends afterwards sees it,
    /// and one that ended before leaves a state this signal then changes.
    fn lost(&self, error: ErrorWire) {
        self.status.update(|status| match status.state {
            MountState::Attached { .. } | MountState::Attaching => MountStatus {
                intent: status.intent,
                path: status.path.clone(),
                state: MountState::Lost { error },
            },
            MountState::Detaching => {
                *lock(&self.lost_while_detaching) = Some(error);
                status.clone()
            }
            _ => status.clone(),
        });
    }

    fn detach(&self, force: bool) -> Result<MountStatus, ErrorWire> {
        let current = self.current();
        let Some(native) = self.native() else {
            // Nothing native to stop; never fences the engine.
            return Ok(self.publish(MountIntent::Detach, current.path, MountState::Detached));
        };
        let lost = matches!(current.state, MountState::Lost { .. });
        let path = current.path.clone().or(Some(native));
        lock(&self.lost_while_detaching).take();
        self.publish(MountIntent::Detach, path.clone(), MountState::Detaching);
        match crate::mount::detach(force || lost) {
            Ok(()) => {
                lock(&self.lost_while_detaching).take();
                Ok(self.publish(MountIntent::Detach, path, MountState::Detached))
            }
            Err(error) => {
                let wire = ErrorWire::from_error(&error);
                if self.native().is_some() {
                    // Refused or failed before the native stop: unchanged,
                    // unless the mount was lost meanwhile.
                    self.status.update(|_| {
                        if let Some(error) = lock(&self.lost_while_detaching).take() {
                            MountStatus {
                                intent: current.intent,
                                path,
                                state: MountState::Lost { error },
                            }
                        } else if current.state.is_attached() || lost {
                            current
                        } else {
                            MountStatus {
                                intent: MountIntent::Attach,
                                path,
                                state: MountState::Attached { since_ms: now_ms() },
                            }
                        }
                    });
                } else {
                    self.publish(
                        MountIntent::Detach,
                        path,
                        MountState::Lost {
                            error: wire.clone(),
                        },
                    );
                }
                Err(wire)
            }
        }
    }
}

/// `path` made absolute with its nearest existing ancestor canonicalized; the
/// missing tail is appended as given. Creates nothing.
fn resolve(path: &Path) -> anyhow::Result<PathBuf> {
    let absolute = std::path::absolute(path)?;
    let mut existing = absolute.as_path();
    let mut tail = Vec::new();
    loop {
        if let Ok(canonical) = std::fs::canonicalize(existing) {
            let mut resolved = canonical;
            resolved.extend(tail.iter().rev());
            return Ok(resolved);
        }
        tail.push(existing.file_name().context("MOUNT_NAME_REQUIRED")?);
        existing = existing.parent().context("MOUNT_PARENT_REQUIRED")?;
    }
}

/// Adapter support shared by the WinFsp (`mount.rs`) and FUSE
/// (`fuse_linux.rs`) adapters: the in-flight callback counter and its drain,
/// the fence, the release of orphaned handles, the mount-scoped error counter
/// and its bounded log, and the `Lost` signal.
pub mod adapter {
    use crate::{
        model::errors::{ErrorClass, ErrorWire, classify, code_of},
        runtime::{Engine, Shared, lock_engine},
        util::{
            lock::lock,
            log::{Level, emit, field},
        },
    };
    use anyhow::{Result, ensure};
    use std::{
        path::{Path, PathBuf},
        sync::{
            Arc, Condvar, Mutex,
            atomic::{AtomicU64, AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    static INFLIGHT: AtomicUsize = AtomicUsize::new(0);
    static DRAIN: Mutex<()> = Mutex::new(());
    static DRAINED: Condvar = Condvar::new();
    static MOUNT_ERRORS: AtomicU64 = AtomicU64::new(0);
    type LostHook = Arc<dyn Fn(ErrorWire) + Send + Sync>;
    static LOST: Mutex<Option<LostHook>> = Mutex::new(None);
    /// The attached instance's path, published by the adapter whenever its
    /// transition lock changes the instance; only ever locked for a copy.
    static ATTACHED: Mutex<Option<PathBuf>> = Mutex::new(None);

    /// The path of the adapter's attached instance (also while it is lost).
    /// Never waits for a transition in progress.
    pub fn attached_path() -> Option<PathBuf> {
        lock(&ATTACHED).clone()
    }

    /// Called by the adapter, under its transition lock, whenever it starts
    /// or forgets an instance.
    pub fn set_attached_path(path: Option<PathBuf>) {
        *lock(&ATTACHED) = path;
    }

    /// Counts one native callback until dropped.
    pub struct Callback(());

    /// Marks a callback in progress; the guard ends it.
    pub fn callback() -> Callback {
        INFLIGHT.fetch_add(1, Ordering::SeqCst);
        Callback(())
    }

    impl Drop for Callback {
        fn drop(&mut self) {
            if INFLIGHT.fetch_sub(1, Ordering::SeqCst) == 1 {
                // Taking the lock orders this wake after a drainer's check.
                drop(lock(&DRAIN));
                DRAINED.notify_all();
            }
        }
    }

    /// Native callbacks currently running.
    pub fn inflight() -> usize {
        INFLIGHT.load(Ordering::SeqCst)
    }

    /// Waits until no callback runs; `false` if `timeout` passed first.
    pub fn drain(timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut guard = lock(&DRAIN);
        loop {
            if INFLIGHT.load(Ordering::SeqCst) == 0 {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            guard = DRAINED
                .wait_timeout(guard, deadline - now)
                .map(|(guard, _)| guard)
                .unwrap_or_else(|poison| poison.into_inner().0);
        }
    }

    /// How long a graceful detach waits for the engine to become quiet before
    /// refusing: the kernel reports a close asynchronously (WinFsp `Close`
    /// after `Cleanup`, FUSE `RELEASE` after `FLUSH`), and scanners hold
    /// short-lived handles.
    pub const QUIET_WAIT: Duration = Duration::from_secs(1);

    /// Step 1 of a detach: fences new opens. A graceful detach first requires
    /// quiet, waiting up to [`QUIET_WAIT`]: no dirty session and no handle the
    /// application still holds (a cleaned-up WinFsp handle only awaits its
    /// `Close`, accepts no writes and holds no unsaved bytes). Not quiet:
    /// `BUSY_VIEW`, unfenced.
    pub fn fence(engine: &Shared, force: bool) -> Result<()> {
        let deadline = Instant::now() + QUIET_WAIT;
        loop {
            {
                let mut e = lock_engine(engine);
                let open = e.handles.values().filter(|h| !h.cleaned).count();
                let dirty = e.sessions.values().filter(|s| s.dirty).count();
                if force || (open == 0 && dirty == 0) {
                    e.switching = true;
                    return Ok(());
                }
                ensure!(
                    Instant::now() < deadline,
                    "BUSY_VIEW: {open} open contexts, {dirty} unsaved files"
                );
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Lifts the fence after a refused or failed detach.
    pub fn unfence(engine: &Shared) {
        lock_engine(engine).switching = false;
    }

    /// Step 3 of a force detach: every dirty session into `pending_saves`.
    pub fn persist_dirty(engine: &Shared) {
        retain(&mut lock_engine(engine));
    }

    fn retain(e: &mut Engine) {
        if !e.sessions.values().any(|s| s.dirty) {
            return;
        }
        if let Err(error) = e.retain_all_dirty() {
            let code = code_of(&error);
            emit(
                Level::Error,
                &code,
                &[
                    field("scope", "mount"),
                    field("message", &format!("{error:#}")),
                ],
            );
        }
    }

    /// Steps 5 and 6 after the native instance stopped: retains dirty sessions
    /// again (a write may have raced the drain), drops every handle, keeps
    /// dirty sessions with no references (the pending-save retry commits
    /// them), drops clean ones, clears queued notifications, then lifts the
    /// fence with `mounted = false`.
    pub fn release(engine: &Shared) {
        let mut e = lock_engine(engine);
        retain(&mut e);
        e.handles.clear();
        e.sessions.retain(|_, session| {
            session.references = 0;
            session.dirty
        });
        e.notifications.clear();
        e.mounted = false;
        e.switching = false;
        e.publish_observation();
    }

    /// The native instance died underneath us (FUSE dispatch end): every
    /// dirty session into `pending_saves` (invariant 8; the dead connection's
    /// handles keep them referenced, so the pending-save retry skips them
    /// until the detach that cleans the instance up), then `mounted = false`.
    pub fn lost(engine: &Shared) {
        let mut e = lock_engine(engine);
        retain(&mut e);
        e.mounted = false;
        e.publish_observation();
    }

    /// Engine bookkeeping when a native instance starts.
    pub fn attached(engine: &Shared) {
        let mut e = lock_engine(engine);
        e.mounted = true;
        e.switching = false;
        // A fresh native instance has no cached names to invalidate.
        e.notifications.clear();
        e.publish_observation();
    }

    /// `MOUNT_PATH_MUST_NOT_EXIST` unless nothing is at `path`.
    pub fn vacant(path: &Path) -> Result<()> {
        match std::fs::symlink_metadata(path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Ok(_) => anyhow::bail!("MOUNT_PATH_MUST_NOT_EXIST: {}", path.display()),
            Err(error) => {
                anyhow::bail!("MOUNT_PATH_MUST_NOT_EXIST: {}: {error}", path.display())
            }
        }
    }

    /// A failed native callback. `counted` failures (the close path and
    /// unexpected I/O errors) increment the mount-scoped error counter and are
    /// logged as warnings, unless they are contention (`BUSY_VIEW` while a
    /// detach or checkout fences the engine); expected ones (not found,
    /// collisions, …) are debug. Never touches `Engine.health` (§4.3). The log
    /// is rate limited per code.
    pub fn fs_error(op: u32, error: &anyhow::Error, counted: bool) {
        let code = code_of(error);
        let message = format!("{error:#}");
        if counted && classify(&code) != ErrorClass::Contention {
            let total = MOUNT_ERRORS.fetch_add(1, Ordering::Relaxed) + 1;
            emit(
                Level::Warn,
                &code,
                &[
                    field("scope", "mount"),
                    field("op", &op),
                    field("mount_errors", &total),
                    field("message", &message),
                ],
            );
        } else {
            emit(
                Level::Debug,
                &code,
                &[
                    field("scope", "mount"),
                    field("op", &op),
                    field("message", &message),
                ],
            );
        }
    }

    /// Mount-scoped callback failures since the process started.
    pub fn mount_errors() -> u64 {
        MOUNT_ERRORS.load(Ordering::Relaxed)
    }

    /// Installs the receiver of `Lost` signals (the attaching controller).
    pub fn set_lost_hook(hook: Option<LostHook>) {
        *lock(&LOST) = hook;
    }

    /// The mount went away underneath us: logged and signalled to the
    /// controller (`MountState::Lost`). Never touches `Engine.health`.
    pub fn report_lost(error: &anyhow::Error) {
        let wire = ErrorWire::from_error(error);
        emit(
            Level::Error,
            &wire.code,
            &[field("scope", "mount"), field("message", &wire.message)],
        );
        let hook = lock(&LOST).clone();
        if let Some(hook) = hook {
            hook(wire);
        }
    }
}
