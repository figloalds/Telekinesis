//! One mount worker: local write staging and loopback WebSocket control.
//!
//! WP4.1: the worker's endpoint requires its local token (`STATE/runtime.token`);
//! [`rpc`], [`RpcLane`] and [`connect_worker`] present it.
use crate::util::lock::{PoisonPolicy, poison_policy};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    ops::{Deref, DerefMut},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, TryLockError, Weak},
    time::{Duration, Instant},
};

pub mod content;
pub mod content_rpc;
pub mod control;
pub mod ctx;
pub mod engine;
pub mod export;
pub mod notify;
pub mod objects_rpc;
pub mod published;
pub mod server;
pub mod sync_rpc;

pub use content::prepare_content;
pub use engine::{Engine, EngineVitals, Handle, Session};
pub use notify::{deliver_notifications, namespace_notifications};
pub use server::{RpcServer, start_rpc, start_rpc_ctx};

pub type Shared = Arc<Mutex<Engine>>;
pub const MAX_FRAME: usize = 64 * 1024 * 1024;
/// Format 6 separates durable metadata from verified object availability.
/// Never negotiate down or acknowledge events a peer cannot project.
pub use crate::core::PEER_FORMAT;
/// `STATE/runtime.json`: where a worker listens. Unchanged by WP4.1 (no
/// token in it, §6.1 P2): the worker's token is in `STATE/runtime.token`
/// next to it, owner-only ([`worker_token`]).
#[derive(Debug, Clone, Serialize)]
pub struct Discovery {
    pub format: u32,
    pub address: String,
    pub state: String,
    pub mount: Option<String>,
    pub repo: String,
    pub device: String,
}

#[derive(Deserialize)]
struct DiscoveryWire {
    format: u32,
    address: String,
    state: String,
    mount: Option<String>,
    repo: String,
    device: String,
}

/// Reading a worker's discovery is how a local client finds the worker: it
/// also lets this process present the worker's token to that address, when
/// the discovery is the worker's own published record (see
/// [`worker_token`]). So a client that reads `runtime.json` and then dials
/// the address it names (`ws::connect`, `session::dial_loopback` with no
/// token) keeps working with mandatory worker tokens (the adapter of
/// CCR-WP4.1-6). Parsing reads no file and resolves no name: it notes where
/// the token is (`ws::tokens::note_source`, an IP-literal loopback address
/// only), and the files are read when a client dials that address, outside
/// any lock of the caller (`runtime::export` parses under the engine lock).
impl<'de> Deserialize<'de> for Discovery {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = DiscoveryWire::deserialize(deserializer)?;
        let discovery = Discovery {
            format: wire.format,
            address: wire.address,
            state: wire.state,
            mount: wire.mount,
            repo: wire.repo,
            device: wire.device,
        };
        if !discovery.state.is_empty() {
            let addresses = crate::ws::tokens::literal_addresses(&discovery.address);
            if !addresses.is_empty() {
                let published = discovery.clone();
                let source: crate::ws::tokens::Source =
                    Arc::new(move || published_token(&published));
                for address in addresses {
                    crate::ws::tokens::note_source(address, source.clone());
                }
            }
        }
        Ok(discovery)
    }
}

/// The token of the worker `info` describes (§14 WP4.1): `STATE/runtime.token`
/// when `STATE/runtime.json` (owner-only, written by the worker) publishes
/// `info.address`, else the one this process already knows for the address
/// (`ws::tokens::lookup`: a worker server in this process, an endpoint the
/// orchestrator was given). The address check means a discovery from
/// elsewhere (a `.tkfs-runtime.json` found by walking up from a directory)
/// can never make this process send a worker's token to an address that
/// worker did not publish. `None`: a legacy worker (no token file) or an
/// unknown address; the call then goes out without a token.
pub fn worker_token(info: &Discovery) -> Option<String> {
    published_token(info).or_else(|| crate::ws::tokens::lookup_endpoint(&info.address))
}

/// `STATE/runtime.token` when `STATE/runtime.json` publishes `info.address`
/// both before and after the token is read (WP4.INT re-review). A new worker
/// instance removes a stale `runtime.json` before it writes its token
/// (`server::start_rpc_server`) and publishes its own after, so a read that
/// finds the new token is followed by a read that finds no discovery or the
/// new instance's: the token is never handed out for the address of the
/// instance before it (a freed port another local process may hold).
fn published_token(info: &Discovery) -> Option<String> {
    if info.state.is_empty() {
        return None;
    }
    let state = std::path::Path::new(&info.state);
    let publishes = || -> Option<()> {
        let published: Value =
            serde_json::from_slice(&std::fs::read(state.join("runtime.json")).ok()?).ok()?;
        (published.get("address").and_then(Value::as_str) == Some(info.address.as_str()))
            .then_some(())
    };
    publishes()?;
    let token = crate::worker::record::read_token(&crate::worker::record::token_path(state))?;
    publishes()?;
    Some(token)
}

/// A JSON RPC connection to the worker `info` describes, presenting its token
/// ([`worker_token`]). A worker that refuses it answers `ACCESS_DENIED`.
pub fn connect_worker(info: &Discovery) -> Result<crate::ws::Socket> {
    crate::ws::connect_authorized(&info.address, worker_token(info).as_deref())
}

/// One worker JSON RPC request on a fresh connection ([`connect_worker`]).
pub fn rpc(info: &Discovery, request: &str, payload: Value) -> Result<Value> {
    let mut socket = connect_worker(info)?;
    crate::ws::call(&mut socket, &json!({"request":request,"payload":payload}))
}
/// One worker connection per peer lane. Never share a socket between bulk and
/// control work, and never replay a request after an uncertain transport error.
#[derive(Default)]
pub struct RpcLane {
    connection: Option<(String, crate::ws::Socket)>,
    last_used: Option<std::time::Instant>,
}
impl RpcLane {
    pub fn call(&mut self, info: &Discovery, request: &str, payload: Value) -> Result<Value> {
        if self
            .connection
            .as_ref()
            .is_none_or(|(address, _)| address != &info.address)
            || self
                .last_used
                .is_some_and(|at| at.elapsed() >= Duration::from_secs(10))
        {
            self.connection = None;
            self.connection = Some((info.address.clone(), connect_worker(info)?));
        }
        let result = crate::ws::call(
            &mut self.connection.as_mut().unwrap().1,
            &json!({"request":request,"payload":payload}),
        );
        self.last_used = Some(std::time::Instant::now());
        if result
            .as_ref()
            .is_err_and(|error| error.downcast_ref::<crate::ws::RemoteError>().is_none())
        {
            self.connection = None;
        }
        result
    }
}

// ---------------------------------------------------------------------------
// Engine lock seam (WP1.4, REVAMP_PLAN §4.2): one poison policy for every
// engine lock, and engine-lock gauges kept outside the lock.

/// Workers whose engine lock reports gauges: the engine mutex address and the
/// worker context that owns it (registered by the RPC server). A worker
/// process has one; in-process tests may have several.
static WORKERS: RwLock<Vec<(usize, Weak<ctx::WorkerCtx>)>> = RwLock::new(Vec::new());

fn engine_key(engine: &Shared) -> usize {
    Arc::as_ptr(engine) as usize
}

/// Makes `ctx.engine`'s lock report into `ctx.gauges` (waits through
/// [`lock_engine`] and [`lock_engine_tracked`], holds through the tracked guard).
/// Replaces an earlier registration of the same engine.
pub fn register_worker(ctx: &Arc<ctx::WorkerCtx>) {
    let key = engine_key(&ctx.engine);
    let mut workers = WORKERS.write().unwrap_or_else(PoisonError::into_inner);
    workers.retain(|(other, worker)| *other != key && worker.strong_count() > 0);
    workers.push((key, Arc::downgrade(ctx)));
}

/// The registered worker of `engine`, if any.
fn worker_of(engine: &Shared) -> Option<Arc<ctx::WorkerCtx>> {
    let key = engine_key(engine);
    WORKERS
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .find(|(other, _)| *other == key)
        .and_then(|(_, worker)| worker.upgrade())
}

fn elapsed_ms(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Locks the engine under the process poison policy (§4.2). Since WP1.INT
/// (CCR-WP1.4-2) it returns the tracked [`EngineGuard`], like
/// [`lock_engine_tracked`]: the wait and the hold are reported to the
/// registered worker's gauges, so filesystem callbacks (the main holders)
/// count toward `engine.stalled`, and the guard's drop refreshes the
/// engine's [`EngineVitals`].
///
/// A poisoned engine (a thread panicked while holding it):
/// * [`PoisonPolicy::Panic`] (the default; tests): panics with `ENGINE_POISONED`.
/// * [`PoisonPolicy::Exit`] (set by `worker::run`): `into_inner`, then
///   [`Engine::persist_after_poison`] (roll back an open transaction, then
///   `retain_all_dirty`), then a fatal `ENGINE_POISONED` log line, then
///   `exit(70)`. The recovered guard is held until the process exits, so no
///   other thread sees the inconsistent engine.
pub fn lock_engine(e: &Shared) -> EngineGuard<'_> {
    lock_engine_tracked(e)
}

/// [`lock_engine`] plus hold gauges: the hold starts when the lock is taken
/// and is reported (`record_hold`) when the guard drops, on the holding
/// thread. Dropping the guard also refreshes the engine's [`EngineVitals`].
pub fn lock_engine_tracked(e: &Shared) -> EngineGuard<'_> {
    let worker = worker_of(e);
    let guard = acquire(e, worker.as_deref());
    EngineGuard::new(guard, worker)
}

/// A non-blocking [`lock_engine_tracked`]: `None` while another thread holds
/// the engine. A poisoned engine follows the poison policy.
pub fn try_lock_engine(e: &Shared) -> Option<EngineGuard<'_>> {
    match e.try_lock() {
        Ok(guard) => Some(EngineGuard::new(guard, worker_of(e))),
        Err(TryLockError::WouldBlock) => None,
        Err(TryLockError::Poisoned(poisoned)) => engine_poisoned(poisoned),
    }
}

/// Applies the poison policy if `e` is poisoned; returns otherwise. Used after
/// a contained panic, so a poisoned engine never waits for its next user.
pub fn check_engine_poison(e: &Shared) {
    if e.is_poisoned() {
        drop(lock_engine(e));
    }
}

fn acquire<'a>(e: &'a Shared, worker: Option<&ctx::WorkerCtx>) -> MutexGuard<'a, Engine> {
    let started = Instant::now();
    let result = {
        let _waiting = worker.map(|worker| worker.gauges.begin_wait());
        e.lock()
    };
    if let Some(worker) = worker {
        worker.gauges.record_wait(elapsed_ms(started));
    }
    match result {
        Ok(guard) => guard,
        Err(poisoned) => engine_poisoned(poisoned),
    }
}

fn engine_poisoned(poisoned: PoisonError<MutexGuard<'_, Engine>>) -> ! {
    match poison_policy() {
        PoisonPolicy::Panic => {
            panic!("ENGINE_POISONED: a thread panicked while holding the engine lock")
        }
        PoisonPolicy::Exit => {
            let mut engine = poisoned.into_inner();
            let outcome = catch_unwind(AssertUnwindSafe(|| engine.persist_after_poison()));
            let (retained, error, hook_panicked) = match &outcome {
                Ok(Ok(retained)) => (Some(*retained), None, false),
                Ok(Err(error)) => (None, Some(format!("{error:#}")), false),
                Err(_) => (None, None, true),
            };
            crate::util::log::event!(fatal, ENGINE_POISONED, {
                "retained_dirty": retained,
                "error": error,
                "hook_panicked": hook_panicked,
                "exit_code": crate::model::ipc::EXIT_INTERNAL,
            });
            // Exit while still holding the recovered guard.
            std::process::exit(crate::model::ipc::EXIT_INTERNAL)
        }
    }
}

/// A tracked engine lock ([`lock_engine_tracked`]). Dereferences to the
/// [`Engine`]; on drop it refreshes the engine vitals and reports the hold to
/// the registered worker's gauges, on the holding thread and while the lock
/// is still held.
pub struct EngineGuard<'a> {
    guard: MutexGuard<'a, Engine>,
    worker: Option<Arc<ctx::WorkerCtx>>,
    since: Instant,
}

impl<'a> EngineGuard<'a> {
    fn new(guard: MutexGuard<'a, Engine>, worker: Option<Arc<ctx::WorkerCtx>>) -> Self {
        if let Some(worker) = &worker {
            worker.gauges.begin_hold();
        }
        Self {
            guard,
            worker,
            since: Instant::now(),
        }
    }
}

impl Deref for EngineGuard<'_> {
    type Target = Engine;
    fn deref(&self) -> &Engine {
        &self.guard
    }
}

impl DerefMut for EngineGuard<'_> {
    fn deref_mut(&mut self) -> &mut Engine {
        &mut self.guard
    }
}

impl Drop for EngineGuard<'_> {
    fn drop(&mut self) {
        // While unwinding, the engine is about to be poisoned and may be
        // inconsistent: touch only the gauges (a second panic would abort).
        if !std::thread::panicking() {
            self.guard.refresh_vitals();
        }
        if let Some(worker) = &self.worker {
            worker.gauges.record_hold(elapsed_ms(self.since));
        }
    }
}
