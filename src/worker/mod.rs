//! Owned by WP1.4 (worker lifecycle); WP2.3 adds the activator, status cell
//! and maintenance threads.
//!
//! Worker process entry: one folder state, its engine, the loopback RPC
//! listener and an optional mount. `tkfs daemon --state S [--mount M]
//! [--supervised]` runs [`run`] (REVAMP_PLAN §4.1):
//!
//! 1. take `owner.lock` (held: `FatalLine{ALREADY_RUNNING}`, exit 75);
//! 2. read the previous `worker.json` (`WorkerCtx.previous`);
//! 3. open and recover the store, emitting `startup_progress` from this
//!    thread (invariant 28);
//! 4. `Engine::new` (recovers pending saves);
//! 5. build the `WorkerCtx`;
//! 6. start the RPC listener: recovery is complete before any RPC (invariant 24);
//! 7. write `runtime.json` and `worker.json` (`runtime.json.mount` is the
//!    absolute, normalized legacy `--mount` once prepared, or the path of a
//!    successful `mount-attach`; `None` before);
//! 8. legacy/standalone `--mount` without `--supervised`: attach now, and
//!    report a failure in Ready (never fatal; a stop accepted since step 6
//!    skips it); `--supervised`: the orchestrator attaches later through
//!    `mount-attach`;
//! 9. print the `ReadyLine`;
//! 10. start the heartbeat;
//! 11. wait for `stopping`, let the stop reply flush (≤ 2 s), remove
//!     `runtime.json` and `worker.json`.
//!
//! Failures before Ready print a `FatalLine` and exit 73 (store), 75
//! (locked), 78 (configuration) or 70 (internal).
pub mod activator;
pub mod heartbeat;
pub mod maintenance;
pub mod mount_ctl;
pub mod record;
pub mod status_cell;

use crate::{
    core::{OpenOptions, Store, VerifyMode},
    model::{
        errors::ErrorWire,
        health::MountStatus,
        ipc::{
            EXIT_CONFIG, EXIT_INTERNAL, EXIT_LOCKED, EXIT_STORE, FatalLine, IPC_VERSION, ReadyLine,
            StopMode, WORKER_RECORD_FORMAT, WorkerHeartbeat, WorkerRecord,
            timing::{STOP_REPLY_FLUSH, UNSUPERVISED_EXIT_DEFAULT_S},
        },
    },
    orchestrator,
    runtime::{
        Engine, RpcServer, Shared,
        ctx::{EngineGaugeCell, SyncStorePool, WorkerCtx},
        server::start_rpc_server,
    },
    util::{
        fault,
        lock::{PoisonPolicy, set_poison_policy},
        notify::Notify,
        snapshot::Snapshot,
        time::now_ms,
    },
};
use anyhow::Result;
use serde_json::Value;
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

/// `tkfs daemon` arguments.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerArgs {
    pub state: PathBuf,
    pub mount: Option<PathBuf>,
    /// Launched by the orchestrator: the mount is attached later through
    /// `mount-attach`, and the worker stops itself when nobody has called
    /// `hello`/`watch` for `TKFS_UNSUPERVISED_EXIT_S` (default 600 s).
    pub supervised: bool,
}

/// This build's version, as reported in Ready, `hello` and `worker.json`:
/// `<package version>+<build id>`. The build id is `TKFS_BUILD_ID` when set at
/// compile time, else a digest of the executable's size and modification
/// time, so two different builds of the same package version never compare
/// equal (adoption restarts a worker of another build, §3.3).
pub fn version() -> &'static str {
    static VERSION: OnceLock<String> = OnceLock::new();
    // `stringify!` keeps the variable name out of the source as a quoted
    // literal: `tests/contracts.rs` reads quoted SCREAMING_CASE literals in
    // `src/` as error codes (CCR-WP1.4-3 asks it to skip `CARGO_` names).
    let package = env!(stringify!(CARGO_PKG_VERSION));
    VERSION.get_or_init(|| format!("{package}+{}", build_id()))
}

fn build_id() -> String {
    if let Some(id) = option_env!("TKFS_BUILD_ID").filter(|id| !id.is_empty()) {
        return id.to_owned();
    }
    std::env::current_exe()
        .and_then(fs::metadata)
        .map(|metadata| {
            let modified = metadata
                .modified()
                .ok()
                .and_then(|at| at.duration_since(std::time::UNIX_EPOCH).ok())
                .map_or(0, |at| at.as_nanos());
            crate::core::hash(format!("{}:{modified}", metadata.len()).as_bytes())[..12].to_owned()
        })
        .unwrap_or_else(|_| "unknown".into())
}

/// The worker context for `engine` (§14 WP0.INT): a new instance id, the
/// previous `worker.json`, the mount controller, the engine's stop flag and
/// object locks, an empty heartbeat and status cell, a secondary-store pool.
pub fn build_ctx(
    engine: Shared,
    state: &Path,
    supervised: bool,
    previous: Option<WorkerRecord>,
) -> Arc<WorkerCtx> {
    let (root, repo, device, locks, stopping) = {
        let e = crate::runtime::lock_engine(&engine);
        (
            e.store.root.clone(),
            e.store.repo.clone(),
            e.store.device.clone(),
            e.transfers.clone(),
            e.stopping.clone(),
        )
    };
    let instance = crate::core::id();
    let started_at_ms = now_ms();
    Arc::new(WorkerCtx {
        mount: mount_ctl::MountController::new(engine.clone(), previous.clone()),
        engine,
        sync_stores: SyncStorePool::new(&root, &repo, &device),
        root,
        state: state.to_owned(),
        repo,
        device,
        heartbeat: Snapshot::new(WorkerHeartbeat {
            instance: instance.clone(),
            ..WorkerHeartbeat::default()
        }),
        instance,
        started_at_ms,
        supervised,
        version: version().to_owned(),
        previous,
        locks,
        activation: Arc::new(Notify::new()),
        activator_running: AtomicBool::new(false),
        status_generation: AtomicU64::new(0),
        status_cell: Snapshot::new(Value::Null),
        sync_obs: Mutex::default(),
        inflight_objects: Mutex::default(),
        stopping,
        last_contact_ms: AtomicU64::new(started_at_ms),
        gauges: EngineGaugeCell::new(),
    })
}

/// Resolves a requested mount path the way the legacy worker did: absolute
/// (against this process's working directory) and lexically normalized,
/// never overlapping the state directory (invariant 27; checked before
/// anything is created), parent created. The mount controller checks the
/// rest (the path must not exist, …). The result is also what
/// `runtime.json` advertises as `mount` (`bucket-export` resolves it).
pub fn prepare_mount_path(state: &Path, path: &Path) -> Result<PathBuf, ErrorWire> {
    let io = |error: std::io::Error| ErrorWire::from_error(&error.into());
    let path = normalize(&std::path::absolute(path).map_err(io)?);
    let parent = path
        .parent()
        .ok_or_else(|| ErrorWire::coded("MOUNT_PARENT_REQUIRED", path.display()))?;
    if path.file_name().is_none() {
        return Err(ErrorWire::coded("MOUNT_NAME_REQUIRED", path.display()));
    }
    let state = fs::canonicalize(state).unwrap_or_else(|_| state.to_owned());
    let resolved = resolve_existing_prefix(&path);
    if resolved.starts_with(&state) || state.starts_with(&resolved) {
        return Err(ErrorWire::coded(
            "MOUNT_OVERLAPS_STATE",
            format!("{} overlaps {}", path.display(), state.display()),
        ));
    }
    fs::create_dir_all(parent).map_err(io)?;
    Ok(path)
}

/// `path` without `.` and `..` components (lexically; `absolute` keeps `..`
/// on unix).
fn normalize(path: &Path) -> PathBuf {
    use std::path::Component;
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other),
        }
    }
    normalized
}

/// `path` with its longest existing prefix canonicalized (symlinks resolved;
/// the verbatim form on Windows), so it compares with canonical paths.
fn resolve_existing_prefix(path: &Path) -> PathBuf {
    let mut missing = Vec::new();
    let mut prefix = path;
    loop {
        if let Ok(resolved) = fs::canonicalize(prefix) {
            return missing
                .iter()
                .rev()
                .fold(resolved, |resolved, name| resolved.join(name));
        }
        match (prefix.parent(), prefix.file_name()) {
            (Some(parent), Some(name)) => {
                missing.push(name.to_owned());
                prefix = parent;
            }
            _ => return path.to_owned(),
        }
    }
}

#[cfg(target_os = "linux")]
static SIGNALLED: AtomicBool = AtomicBool::new(false);
#[cfg(target_os = "linux")]
extern "C" fn signal_stop(_: libc::c_int) {
    SIGNALLED.store(true, Ordering::Relaxed);
}

/// How often the main thread re-checks the signal flag and the idle timer
/// while it waits for a stop (a stop request wakes it at once).
const MAIN_TICK: Duration = Duration::from_millis(250);
/// After a refused idle stop (files open), wait this long before retrying.
const IDLE_RETRY: Duration = Duration::from_secs(10);

/// Runs one worker process until it is stopped (§4.1). Failures before Ready
/// print a `FatalLine` and exit the process with its code.
pub fn run(args: WorkerArgs) -> Result<()> {
    set_poison_policy(PoisonPolicy::Exit);
    match start(&args) {
        Ok(worker) => worker.wait(),
        Err(fatal) => crate::startup::fatal_exit(&fatal),
    }
}

/// A worker past Ready.
struct Running {
    server: RpcServer,
    state: PathBuf,
    idle_limit: Option<Duration>,
    /// `owner.lock`, held until the process exits.
    _lock: File,
}

fn start(args: &WorkerArgs) -> Result<Running, FatalLine> {
    let internal = |error: anyhow::Error| FatalLine::from_error(&error, EXIT_INTERNAL);
    // 1. owner.lock
    let state = fs::canonicalize(&args.state).map_err(|error| {
        FatalLine::new(
            "STATE_NOT_INITIALIZED",
            format!("STATE_NOT_INITIALIZED: {}: {error}", args.state.display()),
            EXIT_CONFIG,
        )
    })?;
    fault::set_scope(&state.to_string_lossy());
    let lock = orchestrator::lock_state(&state).map_err(|error| lock_failure(&state, &error))?;
    let reporting = crate::startup::Reporting::begin();
    // 2. the previous worker.json (a malformed one is ignored)
    let previous = record::read(&state).unwrap_or_else(|error| {
        crate::util::log::event!(warn, WORKER_START, {
            "message": format!("{error:#}"),
        });
        None
    });
    // 3. store open and recovery, on this thread
    let store = Store::open_existing_with(
        &state,
        OpenOptions {
            defer_activation: false,
            verify: VerifyMode::Changed,
        },
    )
    .map_err(|error| FatalLine::from_error(&error, EXIT_STORE))?;
    // 4. pending-save recovery
    crate::startup::progress("recovering pending saves");
    let engine = Engine::new(store);
    let vitals = engine.vitals.clone();
    let engine: Shared = Arc::new(Mutex::new(engine));
    fault::trigger("worker.startup").map_err(internal)?;
    // 5. context
    let ctx = build_ctx(engine, &state, args.supervised, previous);
    // 6. RPC
    crate::startup::progress("starting RPC listener");
    let server = start_rpc_server(ctx.clone()).map_err(internal)?;
    // 7. runtime.json and worker.json. The legacy `--mount` is prepared first
    // (absolute, normalized, parent created; a failure is the mount error of
    // step 8), so `runtime.json.mount` is the absolute path step 8 attaches,
    // as the legacy worker published it; `bucket-export` resolves it. A
    // supervised worker publishes `None` until a `mount-attach` succeeds.
    let legacy_mount = match (&args.mount, args.supervised) {
        (Some(path), false) => Some(prepare_mount_path(&state, path)),
        _ => None,
    };
    let published = legacy_mount
        .as_ref()
        .and_then(|prepared| prepared.as_ref().ok())
        .map(|path| path.to_string_lossy().into_owned());
    let discovery = server.publish(published).map_err(internal)?;
    let worker_record = WorkerRecord {
        format: WORKER_RECORD_FORMAT,
        pid: std::process::id(),
        instance: ctx.instance.clone(),
        address: discovery.address.clone(),
        version: ctx.version.clone(),
        ipc: IPC_VERSION,
        repo: ctx.repo.clone(),
        device: ctx.device.clone(),
        started_at_ms: ctx.started_at_ms,
        process_start: record::process_start(),
        executable: record::executable().map_err(internal)?,
        supervised: args.supervised,
        token: None,
    };
    record::write_atomic(&state, &worker_record).map_err(internal)?;
    // 8. legacy synchronous mount; never fatal. RPC is up since step 6: a
    // stop accepted meanwhile wins (`WORKER_STOPPING`, nothing attached), and
    // a stop that arrives during the attach waits for it, then detaches.
    let mut mount_error = None;
    if let (Some(prepared), Some(path)) = (legacy_mount, &args.mount) {
        crate::startup::progress("mounting filesystem");
        fault::hang("worker.legacy_attach");
        if let Err(error) = prepared.and_then(|prepared| server.attach(&prepared)) {
            crate::util::log::event!(warn, (error.code.as_str()), {
                "site": "worker.mount",
                "path": path,
                "message": error.message,
            });
            mount_error = Some(error);
        }
    }
    let mount: MountStatus = ctx.mount.status();
    heartbeat::beat(&ctx, &vitals);
    drop(reporting);
    // The supervised idle clock starts at Ready: a supervisor contacts the
    // worker only after reading it, however long startup took.
    ctx.last_contact_ms.store(now_ms(), Ordering::Relaxed);
    // 9. Ready
    crate::startup::print_line(&ReadyLine {
        ready: true,
        repo: ctx.repo.clone(),
        device: ctx.device.clone(),
        runtime: state.join("runtime.json"),
        mounted: mount.state.is_attached(),
        mount_error,
        instance: ctx.instance.clone(),
        pid: std::process::id(),
        address: discovery.address.clone(),
        version: ctx.version.clone(),
        ipc: IPC_VERSION,
        token: None,
    });
    // 10. background threads
    heartbeat::spawn(ctx.clone(), vitals).map_err(|error| internal(error.into()))?;
    #[cfg(target_os = "linux")]
    unsafe {
        libc::signal(libc::SIGINT, signal_stop as libc::sighandler_t);
        libc::signal(libc::SIGTERM, signal_stop as libc::sighandler_t);
    }
    Ok(Running {
        server,
        state,
        idle_limit: args.supervised.then(idle_limit).flatten(),
        _lock: lock,
    })
}

/// `owner.lock` is held by another process: `ALREADY_RUNNING` (exit 75).
/// Any other failure to take it is a store failure (exit 73).
fn lock_failure(state: &Path, error: &anyhow::Error) -> FatalLine {
    let held = error.chain().any(|cause| {
        // Unix: `flock` refused (`ALREADY_RUNNING`). Windows: the exclusive
        // open failed with ERROR_SHARING_VIOLATION or ERROR_LOCK_VIOLATION.
        let flock = crate::model::errors::code_in(&cause.to_string()) == Some("ALREADY_RUNNING");
        let shared = cfg!(windows)
            && cause
                .downcast_ref::<std::io::Error>()
                .and_then(std::io::Error::raw_os_error)
                .is_some_and(|code| code == 32 || code == 33);
        flock || shared
    });
    if held {
        FatalLine::new(
            "ALREADY_RUNNING",
            format!(
                "ALREADY_RUNNING: another worker owns {}: {error:#}",
                state.display()
            ),
            EXIT_LOCKED,
        )
    } else {
        FatalLine::from_error(error, EXIT_STORE)
    }
}

/// `TKFS_UNSUPERVISED_EXIT_S` (default 600 s; 0 disables the idle exit).
fn idle_limit() -> Option<Duration> {
    let seconds = std::env::var("TKFS_UNSUPERVISED_EXIT_S")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(UNSUPERVISED_EXIT_DEFAULT_S);
    (seconds > 0).then(|| Duration::from_secs(seconds))
}

impl Running {
    /// §4.1 step 11. The main thread never blocks on the engine here: its
    /// stops (idle exit, signals) refuse with `WORKER_BUSY` while the engine
    /// or a mount attach is busy, and are retried.
    fn wait(self) -> Result<()> {
        let ctx = self.server.ctx().clone();
        let mut next_idle_check = Instant::now();
        // A signal whose stop met a busy engine: retried every tick.
        #[cfg(target_os = "linux")]
        let mut signal_pending = false;
        loop {
            let seen = self.server.stop_count();
            if ctx.stopping.load(Ordering::SeqCst) {
                break;
            }
            #[cfg(target_os = "linux")]
            if SIGNALLED.swap(false, Ordering::Relaxed) || signal_pending {
                let retry = signal_pending;
                match self.server.stop(StopMode::Graceful) {
                    Ok(_) => break,
                    Err(error) => {
                        let wire = ErrorWire::from_error(&error);
                        // Open files refuse a signal, as before (BUSY_VIEW;
                        // send it again later). A busy engine is retried.
                        signal_pending = wire.code == "WORKER_BUSY";
                        if !retry {
                            crate::util::log::event!(warn, (wire.code.as_str()), {
                                "site": "worker.signal",
                                "message": wire.message,
                            });
                        }
                    }
                }
            }
            if let Some(limit) = self.idle_limit
                && Instant::now() >= next_idle_check
            {
                let idle_ms = now_ms().saturating_sub(ctx.last_contact_ms.load(Ordering::Relaxed));
                if u128::from(idle_ms) >= limit.as_millis() {
                    match self.server.stop(StopMode::Graceful) {
                        Ok(_) => {
                            crate::util::log::event!(info, WORKER_STOPPING, {
                                "reason": "no hello or watch from the supervisor",
                                "idle_ms": idle_ms,
                            });
                            break;
                        }
                        Err(error) => {
                            let wire = ErrorWire::from_error(&error);
                            crate::util::log::event!(info, (wire.code.as_str()), {
                                "site": "worker.idle_exit",
                                "message": wire.message,
                            });
                            next_idle_check = Instant::now() + IDLE_RETRY;
                        }
                    }
                }
            }
            self.server.wait_stop(seen, MAIN_TICK);
        }
        self.server.wait_stop_reply(STOP_REPLY_FLUSH);
        let _ = fs::remove_file(self.state.join("runtime.json"));
        let _ = record::remove(&self.state);
        Ok(())
    }
}
