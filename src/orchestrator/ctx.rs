//! Owned by WP2.1 (Wave 2) and WP3.1 (Wave 3).
//!
//! Construction of the daemon `Ctx` (`model::orchestrator::Ctx`, REVAMP_PLAN
//! §3.1, §14 WP2.1 task 1, WP3.1 task 1).
//!
//! [`build`] wires the actors, in this order:
//!
//! 1. `Catalog::open` (takes `orchestrator.lock`: a second daemon on the same
//!    data directory fails here, before anything else starts); with
//!    [`BuildOptions::install_log`] (`run`), `DATA/logs/orchestrator.log`
//!    becomes the process log right after (CCR-WP4.2-1, so a daemon that
//!    fails to start never touches the running one's log) →
//!    `StatusHub::new` with its 1 Hz `hub-tick` thread → `PublishedCatalog`;
//! 2. the control and network listeners (a busy port fails the start before
//!    any worker is started or adopted), and the control endpoint's local
//!    token (`api::install_control_token`, CCR-WP4.1-3: written before
//!    `orchestrator.json` names the endpoint, required by the control server
//!    on every upgrade);
//! 3. `FolderRegistry::start` with the real launcher and the limits derived
//!    from `Config` (the worker version is `worker::version()`,
//!    CCR-WP1.2-2): every desired folder adopts or starts its worker;
//! 4. `Ctx`;
//! 5. the v3 network (WP3.1's cutover): [`HubLegacyView`] (the hub's §7.3
//!    projection) is `ctx.legacy`, and [`DaemonNetwork`] over
//!    `PeerManager::start` is `ctx.network`. `run::serve` starts the inbound
//!    server (`inbound::start`) on the network listener and hands it to the
//!    `DaemonNetwork`, whose `shutdown` stops both sides.
//!
//! The v2 network (the legacy bridge, its lanes, its peering and its
//! `LinkStatus`) is deleted (§14 WP3.1): a 6298d0b peer fails closed with
//! `PEER_NEEDS_UPGRADE` on both sides (§6.2, invariant 20).
//!
//! Small adapters live here, on this side of frozen contracts:
//!
//! * [`DaemonLauncher`] wraps `RealLauncher` and fills an adopted worker's
//!   empty `stderr_tail` from `STATE/worker.log` (CCR-WP1.3-2);
//! * [`extras`] keeps what a daemon holds beside its frozen `Ctx` (the
//!   concrete hub for `set_stopping`, the shutdown latch) in the context's
//!   extension slot (`Ctx::extensions`, CCR-WP2.1-1 applied by WP3.1; it was
//!   a side table keyed by the `Ctx` allocation);
//! * [`DaemonNetwork`] is the daemon's `NetworkApi`: the v3 `PeerManager`
//!   plus the inbound server's shutdown, and the dialer-side status of a
//!   pairing that a legacy acceptor refused (see its docs).
//!
//! A `folder-slots` thread also re-sends `Tick` to folders that wait for a
//! `maximum_running` slot every 2 s (WP1.2 review: a slot wake-up can be lost
//! while the registry is still inserting a new controller).
use super::{
    api::{self, ControlToken},
    catalog::Catalog,
    compat::HubLegacyView,
    config::Config,
    folder::LifecycleLimits,
    hub::{HubConfig, StatusHub},
    inbound::{self, InboundServer},
    peer::PeerManager,
    process::{LogPolicy, RealLauncher, log_tail},
    published::PublishedCatalog,
    registry::{FolderRegistry, RegistryDeps},
    shutdown,
};
use crate::{
    model::{
        errors::{ErrorClass, ErrorWire, Scope},
        ipc::WorkerRecord,
        orchestrator::{
            CatalogApi, Ctx, DaemonEndpoints, FolderCmd, FolderPhase, LaunchSpec, NetworkApi,
            PairResult, ProcessEvent, ProcessSink, PublishedEntry, StatusSink, WorkerClient,
            WorkerEndpoint, WorkerLauncher, WorkerProcess,
        },
        status::{DaemonStatus, StatusEvent},
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
    any::Any,
    collections::VecDeque,
    net::TcpListener,
    path::PathBuf,
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
    thread,
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
    /// The control endpoint's local token (WP4.1, CCR-WP4.1-3): the control
    /// server requires it on every upgrade.
    pub control_token: ControlToken,
    pub network: Option<TcpListener>,
    /// `ctx.network`: the v3 peer stack; `run` starts the inbound server on
    /// `network` and hands it over ([`DaemonNetwork::serve_inbound`]).
    pub net: Arc<DaemonNetwork>,
    /// `ctx.legacy`.
    pub legacy: Arc<HubLegacyView>,
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
    /// Installs `DATA/logs/orchestrator.log` as this process's rotating log
    /// once the catalog holds `orchestrator.lock`, and publishes it as
    /// `log_path` (CCR-WP4.2-1, applied by WP4.INT). `run` sets it;
    /// in-process daemons of the tests leave it off (the first install of a
    /// process wins, and their events go to the harness's stderr).
    pub install_log: bool,
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
    if options.install_log {
        install_log(&config.data_directory);
    }
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
    let mut endpoints = DaemonEndpoints {
        control: format!("ws://{}", control.local_addr()?),
        network: network
            .as_ref()
            .map(|listener| listener.local_addr().map(|a| format!("ws://{a}")))
            .transpose()?,
        log_path: None,
    };
    let control_token = api::install_control_token(&catalog, &endpoints.control);
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
    // The process log: the one installed above, or the desktop's adopted by
    // `RealLauncher::new` (WP4.2), or none.
    endpoints.log_path = crate::util::log::file_log().map(|log| log.path.display().to_string());
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
        catalog: catalog_api.clone(),
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
        extensions: OnceLock::new(),
    });
    let _ = extras(&ctx).hub.set(hub.clone());
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
    let legacy = HubLegacyView::new(hub.clone(), catalog_api);
    let _ = ctx.legacy.set(legacy.clone());
    let net = DaemonNetwork::start(&ctx);
    let _ = ctx.network.set(net.clone());
    spawn_slot_ticker(&ctx);
    let _ = stop_ticker.disarm();
    Ok(Daemon {
        ctx,
        control,
        control_token,
        network,
        net,
        legacy,
        hub,
        catalog,
        registry,
    })
}

/// CCR-WP4.2-1: `DATA/logs/orchestrator.log` becomes this process's log. A
/// daemon the desktop started (its standard streams appended to that file)
/// adopts it first, as `RealLauncher::new` did before (WP4.2); any other
/// daemon installs it, and keeps teeing its events to a terminal, pipe or
/// other file on stderr. The path is the one the user knows (no `\?\`
/// verbatim prefix: `Config::load` canonicalizes the data directory). A log
/// that cannot be installed is logged; events then go to stderr only.
fn install_log(data_directory: &std::path::Path) {
    use crate::util::log;
    if log::adopt_desktop_log().is_some() {
        return;
    }
    let path = log::daemon_log_path(&plain_path(data_directory));
    if let Err(error) = log::install_file(&path, log::Rotation::from_env()) {
        event!(warn, UNKNOWN_ERROR, {
            "detail": "cannot install the daemon log; events go to standard error only",
            "path": path.display().to_string(),
            "error": error.to_string(),
        });
    }
}

/// `path` without the Windows verbatim prefix (`\?\C:\…` becomes `C:\…`,
/// `\?\UNC\host\share` becomes `\host\share`).
fn plain_path(path: &std::path::Path) -> PathBuf {
    let text = path.to_string_lossy();
    if let Some(rest) = text.strip_prefix(r"\?\UNC\") {
        PathBuf::from(format!(r"\{rest}"))
    } else if let Some(rest) = text.strip_prefix(r"\?\") {
        PathBuf::from(rest)
    } else {
        path.to_owned()
    }
}

// Cancels a token when dropped (unless disarmed).
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
// Daemon extras (CCR-WP2.1-1, applied by WP3.1)

/// What a daemon keeps beside its `Ctx`, which is frozen (§3.1): held in its
/// extension slot (`Ctx::extensions`).
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

/// The extras of `ctx` (created in its extension slot on first use; a
/// context built by a test starts with an empty slot). The slot only ever
/// holds `Extras`; were it to hold anything else, each call would get fresh
/// extras (no shared hub, no shared latch) rather than fail.
pub fn extras(ctx: &Arc<Ctx>) -> Arc<Extras> {
    let slot = ctx
        .extensions
        .get_or_init(|| Arc::new(Extras::default()) as Arc<dyn Any + Send + Sync>);
    slot.clone()
        .downcast::<Extras>()
        .unwrap_or_else(|_| Arc::new(Extras::default()))
}

// ---------------------------------------------------------------------------
// The daemon's network (WP3.1)

/// Endpoints whose refused pairing is shown at most (the oldest is
/// resolved beyond this).
pub const MAX_REFUSED_PAIRINGS: usize = 16;

/// The daemon's `NetworkApi` after the cutover (§3.1 `ctx.network`): the v3
/// [`PeerManager`] (pair, remote list, unpair, reconfigure), plus
///
/// * the inbound server's lifecycle: `run::serve` starts it on the network
///   listener and hands it over ([`serve_inbound`](Self::serve_inbound)), and
///   [`NetworkApi::shutdown`] stops the outbound and inbound sides together,
///   within one deadline (§3.6 step 2: GOAWAY to every session, lanes
///   cancelled and joined, inbound sessions sent away);
/// * the dialer-side status of a refused pairing. A `Pair` with a legacy
///   (6298d0b) acceptor fails with `PEER_NEEDS_UPGRADE` before anything is
///   sent (§6.2), and there is no catalog peer, hence no lane, to show it:
///   the refusal is recorded as an error scoped to the endpoint
///   (`Scope::Peer{installation: <endpoint>}`, the only identity a peer we
///   never authenticated has), so Status shows it until that endpoint pairs
///   (or a later pairing with it fails otherwise). A paired peer that turns
///   out to be legacy shows on its lanes (`Incompatible`), as any peer state.
///
/// Both stacks hold the context (`Arc<Ctx>`), and the context holds this:
/// `shutdown` lets go of them, which breaks that cycle once the daemon
/// stopped.
pub struct DaemonNetwork {
    status: Arc<dyn StatusSink>,
    peers: Mutex<Option<Arc<PeerManager>>>,
    inbound: Mutex<Option<Arc<InboundServer>>>,
    closed: AtomicBool,
    /// Endpoints with an open refused-pairing error, oldest first.
    refused: Mutex<VecDeque<String>>,
}

impl DaemonNetwork {
    /// Starts the v3 peer stack (`PeerManager::start`): every catalog peer is
    /// supervised from now on.
    pub fn start(ctx: &Arc<Ctx>) -> Arc<DaemonNetwork> {
        Self::with_peers(ctx, PeerManager::start(ctx.clone()))
    }

    /// A `DaemonNetwork` over an already started `PeerManager` (tests start
    /// one with shorter timing).
    pub fn with_peers(ctx: &Arc<Ctx>, peers: Arc<PeerManager>) -> Arc<DaemonNetwork> {
        Arc::new(DaemonNetwork {
            status: ctx.status.clone(),
            peers: Mutex::new(Some(peers)),
            inbound: Mutex::new(None),
            closed: AtomicBool::new(false),
            refused: Mutex::new(VecDeque::new()),
        })
    }

    /// Starts the inbound v3 server (§3.8) on `listener` and keeps it for
    /// [`NetworkApi::shutdown`]. `None` once the network is shut down (the
    /// listener is dropped: nothing is served).
    pub fn serve_inbound(
        &self,
        ctx: &Arc<Ctx>,
        listener: TcpListener,
    ) -> Option<Arc<InboundServer>> {
        let mut slot = lock(&self.inbound);
        if self.closed.load(Ordering::Acquire) {
            return None;
        }
        let server = inbound::start(ctx.clone(), listener);
        *slot = Some(server.clone());
        Some(server)
    }

    /// The outbound peer stack (until shutdown).
    pub fn peers(&self) -> Option<Arc<PeerManager>> {
        lock(&self.peers).clone()
    }

    /// The inbound server (once started, until shutdown).
    pub fn inbound(&self) -> Option<Arc<InboundServer>> {
        lock(&self.inbound).clone()
    }

    fn stopping() -> ErrorWire {
        ErrorWire::coded("ORCHESTRATOR_STOPPING", "the orchestrator is shutting down")
    }

    fn manager(&self) -> Result<Arc<PeerManager>, ErrorWire> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Self::stopping());
        }
        self.peers().ok_or_else(Self::stopping)
    }

    fn refusal_scope(endpoint: &str) -> Scope {
        Scope::Peer {
            installation: endpoint.to_owned(),
        }
    }

    /// Records a pairing `endpoint` refused as incompatible (dialer side of
    /// invariant 20).
    fn record_refusal(&self, endpoint: &str, error: &ErrorWire) {
        let evicted = {
            let mut refused = lock(&self.refused);
            refused.retain(|known| known != endpoint);
            refused.push_back(endpoint.to_owned());
            if refused.len() > MAX_REFUSED_PAIRINGS {
                refused.pop_front()
            } else {
                None
            }
        };
        if let Some(evicted) = evicted {
            self.status.record(StatusEvent::Resolved {
                scope: Self::refusal_scope(&evicted),
                code: String::new(),
            });
        }
        let shown = ErrorWire::coded(
            error.code.as_str(),
            format!(
                "pairing with {endpoint} refused: {}",
                error
                    .message
                    .trim_start_matches(&format!("{}: ", error.code))
            ),
        );
        self.status.record(StatusEvent::Error {
            scope: Self::refusal_scope(endpoint),
            error: shown,
        });
    }

    /// Resolves the refused-pairing error of `endpoint`, if one is open.
    fn clear_refusal(&self, endpoint: &str) {
        let was = {
            let mut refused = lock(&self.refused);
            let before = refused.len();
            refused.retain(|known| known != endpoint);
            refused.len() != before
        };
        if was {
            self.status.record(StatusEvent::Resolved {
                scope: Self::refusal_scope(endpoint),
                code: String::new(),
            });
        }
    }
}

impl std::fmt::Debug for DaemonNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DaemonNetwork")
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .field("inbound", &self.inbound().is_some())
            .finish_non_exhaustive()
    }
}

impl NetworkApi for DaemonNetwork {
    fn pair(
        &self,
        endpoint: &str,
        code: &str,
        connections: Option<u32>,
    ) -> Result<PairResult, ErrorWire> {
        let result = self.manager()?.pair(endpoint, code, connections);
        match &result {
            Ok(_) => self.clear_refusal(endpoint),
            Err(error) if error.class == ErrorClass::Incompatible => {
                self.record_refusal(endpoint, error);
            }
            // Another answer replaces an earlier refusal (the user's answer
            // is now that error).
            Err(_) => self.clear_refusal(endpoint),
        }
        result
    }

    fn remote_list(&self, endpoint: &str) -> Result<Vec<PublishedEntry>, ErrorWire> {
        self.manager()?.remote_list(endpoint)
    }

    fn unpair(&self, endpoint: &str) -> Result<(), ErrorWire> {
        self.clear_refusal(endpoint);
        self.manager()?.unpair(endpoint)
    }

    fn reconfigure(&self) {
        if let Ok(peers) = self.manager() {
            peers.reconfigure();
        }
    }

    /// Stops the outbound peer stack and the inbound server together, each
    /// within `deadline`, then lets go of both. Idempotent.
    fn shutdown(&self, deadline: Duration) {
        self.closed.store(true, Ordering::Release);
        let peers = lock(&self.peers).take();
        let inbound = lock(&self.inbound).take();
        let inbound = inbound.map(|server| {
            let stopper = server.clone();
            match thread::Builder::new()
                .name("net-shutdown".into())
                .spawn(move || stopper.shutdown(deadline))
            {
                Ok(handle) => Err(handle),
                Err(_) => Ok(server),
            }
        });
        if let Some(peers) = peers {
            peers.shutdown(deadline);
        }
        match inbound {
            // No thread: stop it here, after the outbound side.
            Some(Ok(server)) => server.shutdown(deadline),
            Some(Err(handle)) => {
                if handle.join().is_err() {
                    event!(warn, UNKNOWN_ERROR, {
                        "detail": "the inbound server's shutdown panicked",
                    });
                }
            }
            None => {}
        }
    }
}
