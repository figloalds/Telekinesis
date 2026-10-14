//! One mount worker: local write staging and loopback WebSocket control.
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Discovery {
    pub format: u32,
    pub address: String,
    pub state: String,
    pub mount: Option<String>,
    pub repo: String,
    pub device: String,
}
pub fn rpc(info: &Discovery, request: &str, payload: Value) -> Result<Value> {
    let mut socket = crate::ws::connect(&info.address)?;
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
            self.connection = Some((info.address.clone(), crate::ws::connect(&info.address)?));
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

/// Locks the engine under the process poison policy (§4.2). The signature is
/// the Wave 0 seam and stays a plain `MutexGuard`; the wait is reported to the
/// registered worker's gauges, but the hold is not (a `MutexGuard` cannot
/// report its release). Prefer [`lock_engine_tracked`].
///
/// A poisoned engine (a thread panicked while holding it):
/// * [`PoisonPolicy::Panic`] (the default; tests): panics with `ENGINE_POISONED`.
/// * [`PoisonPolicy::Exit`] (set by `worker::run`): `into_inner`, then
///   [`Engine::persist_after_poison`] (roll back an open transaction, then
///   `retain_all_dirty`), then a fatal `ENGINE_POISONED` log line, then
///   `exit(70)`. The recovered guard is held until the process exits, so no
///   other thread sees the inconsistent engine.
pub fn lock_engine(e: &Shared) -> MutexGuard<'_, Engine> {
    let worker = worker_of(e);
    acquire(e, worker.as_deref())
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
