//! Owned by WP0.INT (frozen; WP1.4 constructs) — stub landed by WP0.INT
//!
//! The worker context (REVAMP_PLAN §14 WP0.INT, §4): what every worker thread
//! shares besides the engine lock. WP1.4 builds one `Arc<WorkerCtx>` per worker
//! process and hands it to the RPC server, heartbeat and maintenance threads;
//! WP2.3 adds the activator. `WorkerCtx`, `SyncStorePool`, `PooledStore`,
//! `ObjectLease` and `EngineGaugeCell` are frozen: a change needs a CCR.
use crate::{
    core::Store,
    model::{health::EngineGauges, ipc::timing::ENGINE_STALLED_AFTER},
    util::{lock::lock, time::now_ms},
};
use anyhow::Result;
use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
};

/// Everything a worker thread needs besides the engine lock (§14 WP0.INT).
pub struct WorkerCtx {
    pub engine: crate::runtime::Shared,
    /// The store root (`Store::root`).
    pub root: PathBuf,
    /// The folder state directory the worker was started with.
    pub state: PathBuf,
    pub repo: String,
    pub device: String,
    /// Per-process instance id (uuid), as in `worker.json`.
    pub instance: String,
    pub started_at_ms: u64,
    /// Started with `--supervised` (the orchestrator attaches the mount).
    pub supervised: bool,
    pub version: String,
    /// The previous `worker.json`, if any (FUSE recovery, §4.1 step 2).
    pub previous: Option<crate::model::ipc::WorkerRecord>,
    /// Per-object install locks; the engine's `transfers`.
    pub locks: Arc<crate::core::objects::ObjectLocks>,
    pub mount: Arc<dyn crate::model::health::MountControl>,
    /// Poked when inbox rows become ready (WP2.3 activator).
    pub activation: Arc<crate::util::notify::Notify>,
    pub activator_running: std::sync::atomic::AtomicBool,
    /// Bumped by mutating paths; the status cell is stale while it is older.
    pub status_generation: std::sync::atomic::AtomicU64,
    pub heartbeat: crate::util::snapshot::Snapshot<crate::model::ipc::WorkerHeartbeat>,
    pub status_cell: crate::util::snapshot::Snapshot<serde_json::Value>,
    /// Per-peer feed counters, keyed by the peer replica (WP2.5).
    pub sync_obs: std::sync::Mutex<BTreeMap<String, crate::model::health::PeerFeedCounters>>,
    /// Secondary store connections for lock-free RPCs.
    pub sync_stores: SyncStorePool,
    /// Object leases by hash (§6.4, WP3.2).
    pub inflight_objects: std::sync::Mutex<BTreeMap<String, ObjectLease>>,
    pub stopping: Arc<std::sync::atomic::AtomicBool>,
    /// Last `hello`/`watch` contact (supervised idle exit, §3.3).
    pub last_contact_ms: std::sync::atomic::AtomicU64,
    pub gauges: EngineGaugeCell,
}

/// One object lease: `session` may transfer `hash` until `expires_ms` (60 s
/// TTL renewed on progress); `offset` is the durable spool offset.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ObjectLease {
    pub session: String,
    pub expires_ms: u64,
    pub offset: u64,
}

/// A pool of secondary store connections (`Store::open_secondary`) for the RPC
/// paths that never take the engine lock (`feed-*`, `objects.*`, `status`).
///
/// `checkout` reuses an idle connection or opens a new one, so it never
/// blocks on other checkouts. A returned connection is kept for reuse while
/// fewer than [`SyncStorePool::MAX_IDLE`] are idle and it has no open
/// transaction; otherwise it is closed (which rolls back anything left open).
pub struct SyncStorePool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    root: PathBuf,
    repo: String,
    device: String,
    idle: Mutex<Vec<Store>>,
}

impl SyncStorePool {
    /// Most idle connections kept for reuse.
    pub const MAX_IDLE: usize = 8;

    pub fn new(
        root: impl Into<PathBuf>,
        repo: impl Into<String>,
        device: impl Into<String>,
    ) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                root: root.into(),
                repo: repo.into(),
                device: device.into(),
                idle: Mutex::new(Vec::new()),
            }),
        }
    }

    /// An idle connection, or a new one (`Store::open_secondary`).
    pub fn checkout(&self) -> Result<PooledStore> {
        let reused = lock(&self.inner.idle).pop();
        let store = match reused {
            Some(store) => store,
            None => Store::open_secondary(&self.inner.root, &self.inner.repo, &self.inner.device)?,
        };
        Ok(PooledStore {
            store: Some(store),
            pool: self.inner.clone(),
        })
    }

    /// Connections currently idle in the pool.
    pub fn idle(&self) -> usize {
        lock(&self.inner.idle).len()
    }
}

impl fmt::Debug for SyncStorePool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SyncStorePool")
            .field("root", &self.inner.root)
            .field("idle", &self.idle())
            .finish()
    }
}

/// A checked-out secondary store; returns to its pool on drop.
pub struct PooledStore {
    store: Option<Store>,
    pool: Arc<PoolInner>,
}

impl Deref for PooledStore {
    type Target = Store;
    fn deref(&self) -> &Store {
        self.store
            .as_ref()
            .expect("PooledStore holds a store until dropped")
    }
}

impl DerefMut for PooledStore {
    fn deref_mut(&mut self) -> &mut Store {
        self.store
            .as_mut()
            .expect("PooledStore holds a store until dropped")
    }
}

impl Drop for PooledStore {
    fn drop(&mut self) {
        let Some(store) = self.store.take() else {
            return;
        };
        if !store.db.is_autocommit() {
            return;
        }
        let mut idle = lock(&self.pool.idle);
        if idle.len() < SyncStorePool::MAX_IDLE {
            idle.push(store);
        }
    }
}

/// Engine-lock gauges kept in atomics, so the heartbeat reads them without the
/// engine lock (§4.2, §4.5). The lock seam reports every wait and hold:
/// `begin_wait` while blocked, `record_wait` once acquired, `begin_hold` while
/// held and `record_hold` on release.
#[derive(Default)]
pub struct EngineGaugeCell {
    waiters: AtomicU32,
    holds: AtomicU64,
    longest_hold_ms: AtomicU64,
    /// Start (wall-clock ms) of the hold in progress; 0 when unlocked.
    hold_since_ms: AtomicU64,
    /// The most recent waits (ms), for `wait_p99_ms`.
    waits: Mutex<VecDeque<u64>>,
}

impl EngineGaugeCell {
    /// Waits kept for the p99.
    pub const WAIT_SAMPLES: usize = 1024;

    pub fn new() -> Self {
        Self::default()
    }

    /// A completed wait for the engine lock.
    pub fn record_wait(&self, ms: u64) {
        let mut waits = lock(&self.waits);
        if waits.len() == Self::WAIT_SAMPLES {
            waits.pop_front();
        }
        waits.push_back(ms);
    }

    /// A completed hold of the engine lock; ends the hold in progress.
    pub fn record_hold(&self, ms: u64) {
        self.holds.fetch_add(1, Ordering::Relaxed);
        self.longest_hold_ms.fetch_max(ms, Ordering::Relaxed);
        self.hold_since_ms.store(0, Ordering::Relaxed);
    }

    /// Counts the calling thread as a waiter until the guard drops.
    pub fn begin_wait(&self) -> WaitGuard<'_> {
        self.waiters.fetch_add(1, Ordering::Relaxed);
        WaitGuard { cell: self }
    }

    /// Marks a hold in progress from now (for `current_hold_ms`/`stalled`).
    pub fn begin_hold(&self) {
        self.hold_since_ms.store(now_ms().max(1), Ordering::Relaxed);
    }

    /// Threads currently waiting for the engine lock.
    pub fn waiters(&self) -> u32 {
        self.waiters.load(Ordering::Relaxed)
    }

    pub fn snapshot(&self) -> EngineGauges {
        let since = self.hold_since_ms.load(Ordering::Relaxed);
        let current_hold_ms = if since == 0 {
            0
        } else {
            now_ms().saturating_sub(since)
        };
        let mut waits: Vec<u64> = lock(&self.waits).iter().copied().collect();
        waits.sort_unstable();
        let wait_p99_ms = match waits.len() {
            0 => 0,
            n => waits[(n * 99).div_ceil(100) - 1],
        };
        EngineGauges {
            waiters: self.waiters(),
            holds: self.holds.load(Ordering::Relaxed),
            wait_p99_ms,
            longest_hold_ms: self
                .longest_hold_ms
                .load(Ordering::Relaxed)
                .max(current_hold_ms),
            current_hold_ms,
            stalled: u128::from(current_hold_ms) > ENGINE_STALLED_AFTER.as_millis(),
        }
    }
}

impl fmt::Debug for EngineGaugeCell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.snapshot(), f)
    }
}

/// Returned by [`EngineGaugeCell::begin_wait`]; uncounts the waiter on drop.
pub struct WaitGuard<'a> {
    cell: &'a EngineGaugeCell,
}

impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        self.cell.waiters.fetch_sub(1, Ordering::Relaxed);
    }
}
