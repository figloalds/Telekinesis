//! Owned by WP2.1.
//!
//! Construction of the daemon `Ctx` (`model::orchestrator::Ctx`, REVAMP_PLAN
//! §3.1, §14 WP2.1 task 1). WP3.1 swaps in the v3 network.
//!
//! [`build`] wires the Wave 1 actors, in this order:
//!
//! 1. `Catalog::open` (takes `orchestrator.lock`: a second daemon on the same
//!    data directory fails here, before anything else starts) →
//!    `StatusHub::new` with its 1 Hz `hub-tick` thread → `PublishedCatalog`;
//! 2. the control and network listeners (a busy port fails the start before
//!    any worker is started or adopted);
//! 3. `FolderRegistry::start` with the real launcher and the limits derived
//!    from `Config` (the worker version is `worker::version()`,
//!    CCR-WP1.2-2): every desired folder adopts or starts its worker;
//! 4. `Ctx`;
//! 5. `LegacyBridge::start`, set as `ctx.network` and `ctx.legacy`.
//!
//! Two small adapters live here, on WP2.1's side of frozen contracts:
//!
//! * [`DaemonLauncher`] wraps `RealLauncher` and fills an adopted worker's
//!   empty `stderr_tail` from `STATE/worker.log` (CCR-WP1.3-2);
//! * [`extras`] is a side table for what a daemon keeps beside its frozen
//!   `Ctx`: the concrete hub (for `set_stopping`) and the shutdown latch
//!   (CCR-WP2.1-1).
//!
//! A `folder-slots` thread also re-sends `Tick` to folders that wait for a
//! `maximum_running` slot every 2 s (WP1.2 review: a slot wake-up can be lost
//! while the registry is still inserting a new controller).
use super::{
    catalog::Catalog,
    config::Config,
    folder::LifecycleLimits,
    hub::{HubConfig, StatusHub},
    legacy_bridge::LegacyBridge,
    process::{LogPolicy, RealLauncher, log_tail},
    published::PublishedCatalog,
    registry::{FolderRegistry, RegistryDeps},
    shutdown,
};
use crate::{
    model::{
        errors::ErrorWire,
        ipc::WorkerRecord,
        orchestrator::{
            CatalogApi, Ctx, DaemonEndpoints, FolderCmd, FolderPhase, LaunchSpec, ProcessEvent,
            ProcessSink, WorkerClient, WorkerEndpoint, WorkerLauncher, WorkerProcess,
        },
        status::DaemonStatus,
    },
    util::{
        cancel::Cancel,
        lock::lock,
        log::event,
        snapshot::Snapshot,
        time::{Clock, SystemClock, now_ms},
    },
};
use anyhow::{Context, Result};
use std::{
    net::TcpListener,
    path::PathBuf,
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

/// `Exited.stderr_tail` of an adopted worker: the end of its `worker.log`.
pub const STDERR_TAIL_BYTES: usize = 4096;
/// The slot safety net re-sends `Tick` this often.
pub const SLOT_TICK: Duration = Duration::from_secs(2);

/// A built daemon: its context and its listeners (§14 WP2.1 "Provides"),
/// plus the concrete parts `run` and tests use.
pub struct Daemon {
    pub ctx: Arc<Ctx>,
    pub control: TcpListener,
    pub network: Option<TcpListener>,
    /// `ctx.network` and `ctx.legacy`; `run` hands it inbound connections.
    pub bridge: Arc<LegacyBridge>,
    /// `ctx.status` and `ctx.status_source`.
    pub hub: Arc<StatusHub>,
    /// `ctx.catalog`.
    pub catalog: Arc<Catalog>,
    /// `ctx.folders`.
    pub registry: Arc<FolderRegistry>,
}

impl std::fmt::Debug for Daemon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Daemon")
            .field("ctx", &self.ctx)
            .field("control", &self.control.local_addr().ok())
            .field(
                "network",
                &self.network.as_ref().and_then(|l| l.local_addr().ok()),
            )
            .finish_non_exhaustive()
    }
}

/// Overrides for [`build_with`] (tests run the daemon in-process).
#[derive(Default)]
pub struct BuildOptions {
    /// The worker binary (default: this executable).
    pub executable: Option<PathBuf>,
    /// The launcher (default: [`DaemonLauncher`] over `RealLauncher`).
    pub launcher: Option<Arc<dyn WorkerLauncher>>,
    /// Lifecycle timing (default: `LifecycleLimits` defaults); the executable,
    /// `maximum_running` and the worker version are always set from the
    /// options and the configuration.
    pub limits: Option<LifecycleLimits>,
}

/// [`build_with`] with the defaults.
pub fn build(config: Config) -> Result<Daemon> {
    build_with(config, BuildOptions::default())
}

/// Builds the daemon context (see the module docs for the order).
pub fn build_with(config: Config, options: BuildOptions) -> Result<Daemon> {
    let catalog = Catalog::open(&config)?;
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let shutdown = Cancel::new();
    let hub = StatusHub::new(
        HubConfig::for_sync_interval(Duration::from_millis(config.network.sync_interval_ms)),
        clock.clone(),
    );
    let ticker = shutdown.child();
    // Stops the ticker if anything below fails.
    let stop_ticker = CancelOnDrop(Some(ticker.clone()));
    hub.spawn_ticker(ticker);
    let published = PublishedCatalog::new();
    let control = TcpListener::bind("127.0.0.1:0").context("binding the control listener")?;
    let network = config
        .network
        .listen
        .as_ref()
        .map(TcpListener::bind)
        .transpose()
        .context("binding the network listener")?;
    let endpoints = DaemonEndpoints {
        control: format!("ws://{}", control.local_addr()?),
        network: network
            .as_ref()
            .map(|listener| listener.local_addr().map(|a| format!("ws://{a}")))
            .transpose()?,
        log_path: None,
    };
    let executable = match options.executable {
        Some(path) => path,
        None => std::env::current_exe()?,
    };
    let limits = LifecycleLimits {
        executable,
        maximum_running: config.maximum_running,
        worker_version: crate::worker::version().to_owned(),
        ..options.limits.unwrap_or_default()
    };
    let catalog_api: Arc<dyn CatalogApi> = catalog.clone();
    let launcher = options.launcher.unwrap_or_else(|| {
        Arc::new(DaemonLauncher {
            inner: RealLauncher::new(LogPolicy::default()),
            catalog: catalog_api.clone(),
        })
    });
    let registry = FolderRegistry::start(RegistryDeps {
        catalog: catalog_api.clone(),
        published: published.clone(),
        status: hub.clone(),
        launcher,
        clock: clock.clone(),
        limits,
    });
    let installation = catalog.snapshot().installation.clone();
    let started_at_ms = now_ms();
    let ctx = Arc::new(Ctx {
        installation: installation.clone(),
        config: Arc::new(config),
        catalog: catalog_api,
        folders: registry.clone(),
        published,
        status: hub.clone(),
        status_source: hub.clone(),
        network: OnceLock::new(),
        legacy: OnceLock::new(),
        clock,
        shutdown,
        stopping: Arc::new(AtomicBool::new(false)),
        started_at_ms,
        endpoints: Snapshot::new(endpoints.clone()),
    });
    let bridge = LegacyBridge::start(ctx.clone());
    let _ = ctx.network.set(bridge.clone());
    let _ = ctx.legacy.set(bridge.clone());
    hub.set_daemon(DaemonStatus {
        installation,
        version: crate::worker::version().to_owned(),
        pid: std::process::id(),
        started_at_ms,
        stopping: false,
        control: endpoints.control,
        network: endpoints.network,
        log_path: endpoints.log_path,
        errors: Vec::new(),
    });
    let _ = extras(&ctx).hub.set(hub.clone());
    spawn_slot_ticker(&ctx);
    let _ = stop_ticker.disarm();
    Ok(Daemon {
        ctx,
        control,
        network,
        bridge,
        hub,
        catalog,
        registry,
    })
}

/// Cancels a token when dropped (unless disarmed).
struct CancelOnDrop(Option<Cancel>);

impl CancelOnDrop {
    fn disarm(mut self) -> Option<Cancel> {
        self.0.take()
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(cancel) = self.0.take() {
            cancel.cancel();
        }
    }
}

/// Re-sends `Tick` to folders waiting for a worker slot (`WORKER_LIMIT`
/// observed, desired, idle), so a lost slot wake-up heals within
/// [`SLOT_TICK`]. `Tick` only acts on a folder that is waiting.
fn spawn_slot_ticker(ctx: &Arc<Ctx>) {
    let weak: Weak<Ctx> = Arc::downgrade(ctx);
    let cancel = ctx.shutdown.child();
    let spawned = std::thread::Builder::new()
        .name("folder-slots".into())
        .spawn(move || {
            while !cancel.sleep(SLOT_TICK) {
                let Some(ctx) = weak.upgrade() else {
                    return;
                };
                if ctx.stopping.load(Ordering::Acquire) {
                    return;
                }
                for view in ctx.folders.all() {
                    let waiting = matches!(view.phase, FolderPhase::Stopped)
                        && view.record.desired_running
                        && view.last_error.as_ref().is_some_and(|record| {
                            !record.is_resolved() && record.error.code == "WORKER_LIMIT"
                        });
                    if waiting {
                        let _ = ctx.folders.send(&view.record.state, FolderCmd::Tick);
                    }
                }
            }
        });
    if let Err(error) = spawned {
        event!(warn, FOLDER_UNAVAILABLE, {
            "detail": "cannot start the folder-slots thread",
            "error": error.to_string(),
        });
    }
}

// ---------------------------------------------------------------------------
// The launcher adapter (CCR-WP1.3-2)

/// `RealLauncher`, plus the `stderr_tail` of adopted workers: the launcher
/// cannot read an adopted worker's log (it is not its child, and
/// `WorkerLauncher::adopt` is not given the state directory), so the
/// `Exited` event's empty tail is filled from `STATE/worker.log`, found
/// through the catalog by the record's repository and device.
pub struct DaemonLauncher {
    pub inner: Arc<RealLauncher>,
    pub catalog: Arc<dyn CatalogApi>,
}

impl WorkerLauncher for DaemonLauncher {
    fn launch(
        &self,
        spec: &LaunchSpec,
        events: ProcessSink,
    ) -> Result<Box<dyn WorkerProcess>, ErrorWire> {
        self.inner.launch(spec, events)
    }

    fn adopt(
        &self,
        record: &WorkerRecord,
        events: ProcessSink,
    ) -> Result<Option<Box<dyn WorkerProcess>>, ErrorWire> {
        let log = self
            .catalog
            .snapshot()
            .folder_by_repo(&record.repo)
            .filter(|folder| folder.device == record.device)
            .map(|folder| folder.directory.join("worker.log"));
        let events: ProcessSink = match log {
            Some(log) => Arc::new(move |event| match event {
                ProcessEvent::Exited {
                    code,
                    fatal,
                    stderr_tail,
                } if stderr_tail.is_empty() => events(ProcessEvent::Exited {
                    code,
                    fatal,
                    stderr_tail: log_tail(&log, STDERR_TAIL_BYTES),
                }),
                other => events(other),
            }),
            None => events,
        };
        self.inner.adopt(record, events)
    }

    fn client(&self, endpoint: &WorkerEndpoint) -> Arc<dyn WorkerClient> {
        self.inner.client(endpoint)
    }
}

// ---------------------------------------------------------------------------
// Daemon extras (CCR-WP2.1-1)

/// What a daemon keeps beside its `Ctx`, which is frozen (§3.1) and has no
/// slot for it. Keyed by the `Ctx` allocation; an entry goes when its `Ctx`
/// is dropped (pruned on the next lookup).
#[derive(Default)]
pub struct Extras {
    hub: OnceLock<Arc<StatusHub>>,
    pub(crate) shutdown: shutdown::Latch,
    /// A management write recorded a daemon-level `CATALOG_UNAVAILABLE`
    /// that no successful write has resolved yet (`manage::apply`).
    pub(crate) catalog_error: AtomicBool,
}

impl Extras {
    /// The concrete hub behind `ctx.status`, when the daemon was built by
    /// [`build`].
    pub fn hub(&self) -> Option<&Arc<StatusHub>> {
        self.hub.get()
    }
}

type ExtrasTable = Vec<(Weak<Ctx>, Arc<Extras>)>;
static EXTRAS: Mutex<ExtrasTable> = Mutex::new(Vec::new());

/// The extras of `ctx` (created on first use).
pub fn extras(ctx: &Arc<Ctx>) -> Arc<Extras> {
    let mut table = lock(&EXTRAS);
    table.retain(|(weak, _)| weak.strong_count() > 0);
    if let Some((_, extras)) = table
        .iter()
        .find(|(weak, _)| std::ptr::eq(weak.as_ptr(), Arc::as_ptr(ctx)))
    {
        return extras.clone();
    }
    let extras = Arc::new(Extras::default());
    table.push((Arc::downgrade(ctx), extras.clone()));
    extras
}
