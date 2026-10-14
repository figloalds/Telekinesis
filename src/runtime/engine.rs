//! Engine state: open sessions and handles, pending-save recovery, and the
//! filesystem/session operations used by the mount adapters and peer receive.
//!
//! WP2.3 (§4.4, §4.5):
//! * [`Engine::after_activation`] runs after every activation (the background
//!   activator's slices and slow commits, a legacy receive, `revalidate`): it
//!   refreshes only the clean, non-writer sessions whose entity's content
//!   revision changed, so a verified read handle survives unrelated receives
//!   (the old receive path re-referenced every clean session, which re-hashed
//!   it on its next read); and, while mounted, it queues the filesystem
//!   notifications of the activated entities, computed from the `before`
//!   projection (captured only while mounted, on Windows).
//! * While a background activator runs (`background_activation`), a legacy
//!   receive only stages (budget 0); the activator commits.
//! * [`EngineVitals`] carries everything the lock-free status reply and the
//!   heartbeat need from the engine: handles, dirty sessions, health,
//!   notification state, the last sync observations, and a store revision
//!   that moves whenever the engine's connection changed the database.
use super::notify::{NotifyQueue, delta_notifications};
use crate::core::{inbox::SliceBudget, *};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

pub struct Session {
    pub entry: Entry,
    pub bytes: crate::staging::Staged,
    pub content_base: Option<String>,
    pub alive_base: Option<String>,
    pub dirty: bool,
    pub references: usize,
    pub preserve_times: u8,
}
pub struct Handle {
    pub entity: String,
    pub generation: u64,
    pub cleaned: bool,
    pub writable: bool,
    pub time_update_disabled: u8,
    directory: Option<Vec<(Entry, usize)>>,
}
#[derive(Serialize, Deserialize)]
struct PendingSave {
    branch: String,
    entry: Entry,
    stage: String,
    content_base: Option<String>,
    alive_base: Option<String>,
    preserve_times: u8,
}
pub struct Engine {
    pub store: Store,
    pub sessions: BTreeMap<String, Session>,
    pub handles: BTreeMap<u64, Handle>,
    pub next_handle: u64,
    pub discovery: Vec<u8>,
    pub health: Option<String>,
    pub observation: Arc<Mutex<Value>>,
    recovery_health: bool,
    pub switching: bool,
    pub mounted: bool,
    pub last_sync: Option<Value>,
    pub transfers: Arc<crate::bulk_sync::ObjectLocks>,
    pub sync_peers: BTreeSet<String>,
    pub peer_sync: BTreeMap<String, Value>,
    pub stopping: Arc<std::sync::atomic::AtomicBool>,
    pub notifications: Vec<(String, u32)>,
    pub notification_error: Option<String>,
    /// Counters readable without the engine lock (heartbeat, status).
    pub vitals: Arc<EngineVitals>,
    /// Set while a background activator runs (WP2.3): a legacy receive then
    /// stages only (budget 0) and the activator commits.
    pub background_activation: bool,
    /// The bounded, coalescing queue the notifier thread delivers (§4.4,
    /// WP2.3 task 7). [`Engine::queue_notifications`] fills it while mounted.
    pub notify_queue: Arc<NotifyQueue>,
    /// Per-entity backoff of [`Engine::retry_pending_due`].
    retry_state: BTreeMap<String, RetryState>,
}

/// What one activation committed, for [`Engine::after_activation`].
#[derive(Debug, Clone, Default)]
pub struct ActivatedDelta {
    /// Committed (or found already committed) event ids, in order.
    pub ids: Vec<String>,
    /// Every entity those events change.
    pub entities: BTreeSet<String>,
    /// Some event changes an entity's `location`, `alive` or `kind`.
    pub namespace_changed: bool,
    /// The active projection before the activation; captured only while
    /// mounted (and only where notifications are delivered: Windows).
    pub before: Option<Projection>,
}

/// The last sync observations of the legacy lanes (`Engine::last_sync`,
/// `peer_sync`, `sync_peers`), published for the lock-free status reply.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SyncObservation {
    pub last_sync: Option<Value>,
    pub peers: BTreeMap<String, Value>,
    pub sync_peers: BTreeSet<String>,
}

/// Per-entity retry backoff of a pending save (1 s → 60 s).
#[derive(Debug, Clone, Copy)]
struct RetryState {
    failures: u32,
    next_at: Instant,
}
/// Pending-save retry backoff (WP2.3 task 5).
const RETRY_BASE: Duration = Duration::from_secs(1);
const RETRY_CAP: Duration = Duration::from_secs(60);

/// Engine counters published for readers that must not take the engine lock
/// (the heartbeat and the lock-free `status` reply, §4.5). Refreshed by
/// `publish_observation`, by every tracked engine guard on release
/// (`runtime::lock_engine_tracked`), and, for `pending_saves`, whenever a
/// `pending_saves` row is written or deleted.
#[derive(Debug, Default)]
pub struct EngineVitals {
    open_handles: AtomicU32,
    unflushed_files: AtomicU32,
    pending_saves: AtomicU32,
    mounted: AtomicBool,
    health: Mutex<Option<String>>,
    /// The engine connection's `total_changes` at the last refresh.
    store_changes: AtomicU64,
    /// Bumped whenever a refresh sees `total_changes` moved.
    store_revision: AtomicU64,
    /// Queued filesystem notifications (engine list plus notifier queue).
    pending_notifications: AtomicU32,
    notification_error: Mutex<Option<String>>,
    /// The last sync observations and a cheap fingerprint of them.
    sync: Mutex<(SyncFingerprint, Arc<SyncObservation>)>,
    /// Clean sessions whose bytes were re-referenced after an activation
    /// changed their content (each one is verified again on its next read).
    session_reloads: AtomicU64,
    /// The background activator holds the engine (a bounded slice).
    activator_holding: AtomicBool,
    /// `Engine::notifications` (control requests' notifications not yet
    /// moved to the notifier's queue): the notifier takes the engine only
    /// when some are waiting.
    control_notifications: AtomicU32,
    /// Completed engine holds of the worker's own background threads
    /// (maintenance, notifier), marked by [`EngineVitals::background_hold`]:
    /// the activator does not mistake them for filesystem or RPC load.
    background_holds: AtomicU64,
    /// A background thread holds the engine right now.
    background_holding: AtomicBool,
    /// `before` projections captured for notifications (an O(entries) clone
    /// under the engine lock; only while mounted, on Windows).
    captures: AtomicU64,
    /// Background activator cycles (slice plus slow commit) begun and
    /// finished; a cycle begins before it takes the engine.
    activator_cycles_begun: AtomicU64,
    activator_cycles_finished: AtomicU64,
}
/// `(last_sync.at, last_sync.caught_up, peers, Σ peers.at, sync peers)`.
type SyncFingerprint = (Option<u64>, bool, usize, u64, usize);
impl EngineVitals {
    pub fn open_handles(&self) -> u32 {
        self.open_handles.load(Ordering::Relaxed)
    }
    pub fn unflushed_files(&self) -> u32 {
        self.unflushed_files.load(Ordering::Relaxed)
    }
    pub fn pending_saves(&self) -> u32 {
        self.pending_saves.load(Ordering::Relaxed)
    }
    pub fn mounted(&self) -> bool {
        self.mounted.load(Ordering::Relaxed)
    }
    /// The engine's `health` string (`None` when healthy).
    pub fn health(&self) -> Option<String> {
        crate::util::lock::lock(&self.health).clone()
    }
    /// Typed store health: any engine health message (a pending save that
    /// could not be committed or recovered) degrades the store.
    pub fn store_health(&self) -> crate::model::health::StoreHealth {
        use crate::model::{errors::ErrorWire, health::StoreHealth};
        match self.health() {
            None => StoreHealth::Healthy,
            Some(message) => StoreHealth::Degraded {
                error: ErrorWire::from_error(&anyhow::anyhow!(message)),
            },
        }
    }
    /// Moves (by at least one) whenever the engine's connection changed the
    /// database since the previous refresh: activation, local writes through
    /// the mount, control requests, receives. Feeds `generations.sync`.
    pub fn store_revision(&self) -> u64 {
        self.store_revision.load(Ordering::Relaxed)
    }
    /// Filesystem notifications not yet delivered.
    pub fn pending_notifications(&self) -> u32 {
        self.pending_notifications.load(Ordering::Relaxed)
    }
    /// The last notification delivery failure, if any.
    pub fn notification_error(&self) -> Option<String> {
        crate::util::lock::lock(&self.notification_error).clone()
    }
    /// The last sync observations of the legacy lanes.
    pub fn sync_observation(&self) -> Arc<SyncObservation> {
        crate::util::lock::lock(&self.sync).1.clone()
    }
    /// Sessions re-referenced (and so re-verified on their next read) after
    /// an activation changed their entity's content.
    pub fn session_reloads(&self) -> u64 {
        self.session_reloads.load(Ordering::Relaxed)
    }
    /// Whether the background activator holds the engine right now (one
    /// bounded slice or slow commit).
    pub fn activator_holding(&self) -> bool {
        self.activator_holding.load(Ordering::Relaxed)
    }
    /// Marks the activator's hold until the returned guard drops.
    pub fn activator_hold(self: &Arc<Self>) -> ActivatorHold {
        self.activator_holding.store(true, Ordering::Relaxed);
        ActivatorHold {
            vitals: self.clone(),
        }
    }
    /// Control requests' notifications waiting in `Engine::notifications`.
    pub fn control_notifications(&self) -> u32 {
        self.control_notifications.load(Ordering::Relaxed)
    }
    /// Completed holds marked by [`EngineVitals::background_hold`].
    pub fn background_holds(&self) -> u64 {
        self.background_holds.load(Ordering::Relaxed)
    }
    /// Whether a background thread (maintenance, notifier) holds the engine.
    pub fn background_holding(&self) -> bool {
        self.background_holding.load(Ordering::Relaxed)
    }
    /// Marks a hold of the engine by one of the worker's own background
    /// threads (maintenance, notifier) until the returned guard drops. Take
    /// it right after the engine and drop it **before** the engine guard: a
    /// hold is then counted here before the lock gauges count it, so
    /// `gauges.holds - background_holds` never moves for it (the activator's
    /// measure of other users, `worker::activator`).
    pub fn background_hold(self: &Arc<Self>) -> BackgroundHold {
        self.background_holding.store(true, Ordering::Relaxed);
        BackgroundHold {
            vitals: self.clone(),
        }
    }
    /// `before` projections captured so far ([`Engine::capture_before`]).
    pub fn captures(&self) -> u64 {
        self.captures.load(Ordering::Relaxed)
    }
    /// Marks a background activator cycle as begun until the returned guard
    /// drops (finished). Begun before the cycle takes the engine, so a cycle
    /// numbered past [`EngineVitals::activator_cycles`]`().0` sees every row
    /// committed before that read.
    pub fn activator_cycle(self: &Arc<Self>) -> ActivatorCycle {
        self.activator_cycles_begun.fetch_add(1, Ordering::SeqCst);
        ActivatorCycle {
            vitals: self.clone(),
        }
    }
    /// `(begun, finished)` background activator cycles.
    pub fn activator_cycles(&self) -> (u64, u64) {
        (
            self.activator_cycles_begun.load(Ordering::SeqCst),
            self.activator_cycles_finished.load(Ordering::SeqCst),
        )
    }
    /// After a notifier round, which no longer takes the engine when no
    /// control notification waits: the queue's length and delivery error
    /// (`Engine::notification_error` is only ever cleared, so the queue's
    /// error is the whole of it).
    pub fn refresh_notify_queue(&self, queue: &NotifyQueue) {
        self.pending_notifications.store(
            count(self.control_notifications() as usize + queue.len()),
            Ordering::Relaxed,
        );
        let error = queue.error();
        let mut current = crate::util::lock::lock(&self.notification_error);
        if *current != error {
            *current = error;
        }
    }
}

/// Returned by [`EngineVitals::activator_cycle`]: counts the cycle finished
/// on drop (also on an error or a panic).
pub struct ActivatorCycle {
    vitals: Arc<EngineVitals>,
}
impl Drop for ActivatorCycle {
    fn drop(&mut self) {
        self.vitals
            .activator_cycles_finished
            .fetch_add(1, Ordering::SeqCst);
    }
}

/// Returned by [`EngineVitals::background_hold`]: counts the hold on drop.
pub struct BackgroundHold {
    vitals: Arc<EngineVitals>,
}
impl Drop for BackgroundHold {
    fn drop(&mut self) {
        self.vitals.background_holds.fetch_add(1, Ordering::Relaxed);
        self.vitals
            .background_holding
            .store(false, Ordering::Relaxed);
    }
}

/// Returned by [`EngineVitals::activator_hold`]: clears the mark on drop.
pub struct ActivatorHold {
    vitals: Arc<EngineVitals>,
}
impl Drop for ActivatorHold {
    fn drop(&mut self) {
        self.vitals
            .activator_holding
            .store(false, Ordering::Relaxed);
    }
}
fn count(n: usize) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}
/// Re-counts `pending_saves` rows into [`EngineVitals`]; called after every
/// write or delete of a row. A failed count keeps the last value.
fn count_pending_saves(store: &Store, vitals: &EngineVitals) {
    if let Ok(rows) = store
        .db
        .query_row("SELECT count(*) FROM pending_saves", [], |row| {
            row.get::<_, i64>(0)
        })
    {
        vitals.pending_saves.store(
            u32::try_from(rows.max(0)).unwrap_or(u32::MAX),
            Ordering::Relaxed,
        );
    }
}
impl Engine {
    pub fn new(store: Store) -> Self {
        let mut engine = Self {
            store,
            sessions: BTreeMap::new(),
            handles: BTreeMap::new(),
            next_handle: 1,
            discovery: vec![],
            health: None,
            observation: Arc::new(Mutex::new(Value::Null)),
            recovery_health: false,
            switching: false,
            mounted: false,
            last_sync: None,
            transfers: Arc::new(crate::bulk_sync::ObjectLocks::default()),
            sync_peers: BTreeSet::new(),
            peer_sync: BTreeMap::new(),
            stopping: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            notifications: vec![],
            notification_error: None,
            vitals: Arc::new(EngineVitals::default()),
            background_activation: false,
            notify_queue: Arc::new(NotifyQueue::default()),
            retry_state: BTreeMap::new(),
        };
        if let Err(error) = engine.load_pending() {
            engine.health = Some(format!("RECOVERY_REQUIRED: {error:#}"));
        }
        engine.publish_observation();
        engine
    }
    pub fn publish_observation(&self) {
        self.refresh_vitals();
        let branch = self.store.branch(&self.store.active).ok();
        *crate::util::lock::lock(&self.observation) = json!({"repo":self.store.repo,"device":self.store.device,
            "branch":branch,"generation":self.store.generation,"mounted":self.mounted,
            "open_handles":self.handles.len(),"unflushed_files":self.sessions.values().filter(|s|s.dirty).count(),
            "health":self.health,"sampled_at_ms":time(),"caught_up":false});
    }
    /// Copies the cheap counters into [`EngineVitals`] (no SQL: the store
    /// revision compares the connection's `total_changes`, the sync
    /// observations are cloned only when their fingerprint moved).
    pub fn refresh_vitals(&self) {
        let v = &self.vitals;
        v.open_handles
            .store(count(self.handles.len()), Ordering::Relaxed);
        v.unflushed_files.store(
            count(self.sessions.values().filter(|s| s.dirty).count()),
            Ordering::Relaxed,
        );
        v.mounted.store(self.mounted, Ordering::Relaxed);
        {
            let mut health = crate::util::lock::lock(&v.health);
            if *health != self.health {
                health.clone_from(&self.health);
            }
        }
        let changes = self.store.db.total_changes();
        if v.store_changes.swap(changes, Ordering::Relaxed) != changes {
            v.store_revision.fetch_add(1, Ordering::Relaxed);
        }
        v.pending_notifications.store(
            count(self.notifications.len() + self.notify_queue.len()),
            Ordering::Relaxed,
        );
        v.control_notifications
            .store(count(self.notifications.len()), Ordering::Relaxed);
        {
            let error = self
                .notification_error
                .clone()
                .or_else(|| self.notify_queue.error());
            let mut current = crate::util::lock::lock(&v.notification_error);
            if *current != error {
                *current = error;
            }
        }
        let fingerprint: SyncFingerprint = (
            self.last_sync.as_ref().and_then(|s| s["at"].as_u64()),
            self.last_sync
                .as_ref()
                .is_some_and(|s| s["caught_up"] == true),
            self.peer_sync.len(),
            self.peer_sync
                .values()
                .filter_map(|s| s["at"].as_u64())
                .fold(0u64, u64::wrapping_add),
            self.sync_peers.len(),
        );
        let mut sync = crate::util::lock::lock(&v.sync);
        if sync.0 != fingerprint {
            *sync = (
                fingerprint,
                Arc::new(SyncObservation {
                    last_sync: self.last_sync.clone(),
                    peers: self.peer_sync.clone(),
                    sync_peers: self.sync_peers.clone(),
                }),
            );
        }
    }
    /// The active projection before an activation, when notifications will
    /// be computed from it: mounted, on Windows (FUSE serves with zero TTLs
    /// and needs none, §4.4). Only its entries are captured (all the
    /// notification diff reads).
    pub fn capture_before(&self) -> Option<Projection> {
        if !(cfg!(windows) && self.mounted) {
            return None;
        }
        self.vitals.captures.fetch_add(1, Ordering::Relaxed);
        self.store.entries_snapshot().ok()
    }
    /// The [`ActivatedDelta`] of `ids` (point lookups of the committed
    /// payloads; ids not in the journal are skipped).
    pub fn activation_delta(
        &self,
        ids: &[String],
        before: Option<Projection>,
    ) -> Result<ActivatedDelta> {
        #[derive(Deserialize)]
        struct Light {
            changes: Vec<LightChange>,
        }
        #[derive(Deserialize)]
        struct LightChange {
            entity: String,
            field: String,
        }
        let mut delta = ActivatedDelta {
            ids: ids.to_vec(),
            before,
            ..ActivatedDelta::default()
        };
        let mut statement = self
            .store
            .db
            .prepare_cached("SELECT payload FROM events WHERE id=?")?;
        for id in ids {
            let payload: Option<String> =
                rusqlite::OptionalExtension::optional(statement.query_row([id], |r| r.get(0)))?;
            let Some(payload) = payload else { continue };
            let light: Light = serde_json::from_str(&payload)?;
            for change in light.changes {
                delta.namespace_changed |=
                    matches!(change.field.as_str(), "location" | "alive" | "kind");
                delta.entities.insert(change.entity);
            }
        }
        Ok(delta)
    }
    /// After an activation (WP2.3 task 3): refresh only the clean, non-writer
    /// sessions whose entity's content revision changed (metadata follows the
    /// canonical register for every activated entity's session); while
    /// mounted, queue the notifications of the activated entities, computed
    /// from `delta.before`. Never fails: the events are already committed, so
    /// a session that cannot be re-referenced keeps its pinned bytes and the
    /// failure is logged.
    pub fn after_activation(&mut self, delta: &ActivatedDelta) {
        if delta.entities.is_empty() {
            return;
        }
        // Existing writer and mapping bases are deliberately pinned. Even a
        // clean writer may have kernel-buffered writes not yet delivered here.
        let writers: BTreeSet<&String> = self
            .handles
            .values()
            .filter(|h| h.writable)
            .map(|h| &h.entity)
            .collect();
        for entity in &delta.entities {
            let Some(session) = self.sessions.get_mut(entity) else {
                continue;
            };
            let Ok(entry) = self.store.entry(entity) else {
                continue;
            };
            if !entry.alive {
                continue;
            }
            // Metadata follows the canonical register without rebasing a
            // writer's pinned content buffer or existence/content ancestry.
            session.entry.basic = entry.basic;
            if session.dirty || writers.contains(entity) {
                continue;
            }
            let content = entry.revisions.get("content").cloned();
            if content != session.content_base {
                match crate::staging::Staged::from_object(&self.store, entry.content.as_deref()) {
                    Ok(bytes) => {
                        session.bytes = bytes;
                        session.content_base = content;
                        self.vitals.session_reloads.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(error) => {
                        let wire = crate::model::errors::ErrorWire::from_error(&error);
                        crate::util::log::event!(warn, (wire.code.as_str()), {
                            "site": "engine.after_activation",
                            "entity": entity,
                            "message": wire.message,
                        });
                        continue;
                    }
                }
            }
            session.alive_base = entry.revisions.get("alive").cloned();
        }
        if let Some(before) = &delta.before {
            self.notify_activation(before, &delta.entities, delta.namespace_changed);
        }
    }
    /// Queues the notifications of activated `entities` while mounted,
    /// computed from `before` (the active projection before those
    /// activations; its entries are all that is read) and the current view.
    pub fn notify_activation(
        &mut self,
        before: &Projection,
        entities: &BTreeSet<String>,
        namespace_changed: bool,
    ) {
        if !self.mounted || entities.is_empty() {
            return;
        }
        let notifications = delta_notifications(before, &self.store, entities, namespace_changed);
        self.queue_notifications(notifications);
    }
    /// Queues filesystem notifications for the notifier (coalesced, bounded;
    /// overflow becomes one root invalidation). Dropped while not mounted.
    pub fn queue_notifications(&mut self, notifications: Vec<(String, u32)>) {
        if notifications.is_empty() {
            return;
        }
        if self.mounted && !self.switching {
            self.notify_queue.push(notifications);
        }
    }
    /// Activates ready inbox rows under the caller's lock in bounded slices
    /// (`max_events` attempts, `max_ms` in total; slow events inline), then
    /// runs [`Engine::after_activation`]. Returns the activated ids. Used by
    /// `revalidate`, which re-queues and then activates what is ready in the
    /// same request (the legacy receive budget, WP1.INT task 1).
    pub fn activate_bounded(&mut self, max_events: usize, max_ms: u64) -> Result<Vec<String>> {
        let before = self.capture_before();
        let started = Instant::now();
        let mut activated = vec![];
        let mut attempts = 0usize;
        loop {
            let spent = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            let slice = self.store.activate_slice(SliceBudget {
                max_events: max_events.saturating_sub(attempts),
                max_ms: max_ms.saturating_sub(spent),
                inline_slow: true,
            })?;
            let tried = slice.activated.len() + slice.quarantined.len();
            attempts += tried;
            activated.extend(slice.activated);
            if !slice.more
                || tried == 0
                || attempts >= max_events
                || started.elapsed() >= Duration::from_millis(max_ms)
            {
                break;
            }
        }
        if !activated.is_empty() {
            let delta = self.activation_delta(&activated, before)?;
            self.after_activation(&delta);
        }
        Ok(activated)
    }
    fn refresh_pending_saves(&self) {
        count_pending_saves(&self.store, &self.vitals);
    }
    fn load_pending(&mut self) -> Result<()> {
        self.store.db.execute_batch("CREATE TABLE IF NOT EXISTS pending_saves(entity TEXT PRIMARY KEY,payload TEXT NOT NULL)")?;
        let records = self
            .store
            .db
            .prepare("SELECT payload FROM pending_saves")?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        self.refresh_pending_saves();
        for record in records {
            crate::startup::progress("recovering pending saves");
            // Qualification: slows recovery down to observe its progress.
            crate::util::fault::hang("engine.recover_pending");
            let pending: PendingSave = serde_json::from_str(&record)?;
            ensure!(
                pending.branch == self.store.active,
                "RECOVERY_BRANCH_MISMATCH"
            );
            let bytes = crate::staging::Staged::recover(&self.store.root, &pending.stage)?;
            self.sessions.insert(
                pending.entry.id.clone(),
                Session {
                    entry: pending.entry,
                    bytes,
                    content_base: pending.content_base,
                    alive_base: pending.alive_base,
                    preserve_times: pending.preserve_times,
                    dirty: true,
                    references: 0,
                },
            );
        }
        if !self.sessions.is_empty() {
            self.health = Some("PENDING_SAVES: recovery in progress".into());
            self.recovery_health = true;
        }
        Ok(())
    }
    pub(crate) fn retain_pending(&mut self, entity: &str, error: &anyhow::Error) -> Result<()> {
        self.recovery_health = self.health.is_none() || self.recovery_health;
        if self.recovery_health {
            self.health = Some(format!("PENDING_SAVE: {error:#}"));
        }
        let session = self.sessions.get_mut(entity).context("INVALID_HANDLE")?;
        if !session.dirty {
            return Ok(());
        }
        let stage = session.bytes.retain(&self.store.root)?;
        let record = PendingSave {
            branch: self.store.active.clone(),
            entry: session.entry.clone(),
            stage,
            content_base: session.content_base.clone(),
            alive_base: session.alive_base.clone(),
            preserve_times: session.preserve_times,
        };
        self.store.db.execute("INSERT INTO pending_saves VALUES(?,?) ON CONFLICT(entity) DO UPDATE SET payload=excluded.payload",
            rusqlite::params![entity,serde_json::to_string(&record)?])?;
        self.refresh_pending_saves();
        Ok(())
    }
    /// Persist every dirty session into `pending_saves` without committing it,
    /// so a force detach or a poison exit never loses unsaved bytes
    /// (invariant 8). Sessions stay in memory and dirty; a later flush or
    /// `retry_pending` commits them and clears the row. Every dirty session is
    /// attempted even if an earlier one fails; the first failure is returned.
    pub fn retain_all_dirty(&mut self) -> Result<usize> {
        let dirty: Vec<_> = self
            .sessions
            .iter()
            .filter(|(_, s)| s.dirty)
            .map(|(id, _)| id.clone())
            .collect();
        let reason = anyhow::anyhow!("DIRTY_SESSION_RETAINED");
        let mut retained = 0;
        let mut failure = None;
        for entity in &dirty {
            match self.retain_pending(entity, &reason) {
                Ok(()) => retained += 1,
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        match failure {
            Some(error) => Err(error.context(format!(
                "PENDING_SAVE_PERSIST_FAILED: {} of {} dirty sessions not retained",
                dirty.len() - retained,
                dirty.len()
            ))),
            None => Ok(retained),
        }
    }
    /// The poison hook (`runtime::lock_engine` under `PoisonPolicy::Exit`):
    /// a thread panicked while holding the engine, possibly inside a
    /// transaction. First roll back whatever transaction the connection still
    /// has open (and every savepoint in it), so the `pending_saves` rows below
    /// commit on their own instead of joining a transaction nobody will commit;
    /// then [`Engine::retain_all_dirty`] (invariant 8).
    ///
    /// If that `ROLLBACK` fails, the half-done work of the panicked request is
    /// never committed (it may break the store's own invariants): the
    /// connection is closed instead, which discards its transaction, and a
    /// fresh connection to the same database writes the rows. The rollback
    /// failure is logged, and reported with any later failure. Only when no
    /// usable connection remains is nothing retained (rows written into the
    /// open transaction would be rolled back at exit anyway).
    pub fn persist_after_poison(&mut self) -> Result<usize> {
        let mut failed_rollback = None;
        if !self.store.db.is_autocommit() {
            let rolled_back = crate::util::fault::fail("engine.poison_rollback")
                .and_then(|()| Ok(self.store.db.execute_batch("ROLLBACK")?));
            if let Err(error) = rolled_back {
                let error = error.context("cannot roll back the open transaction");
                let wire = crate::model::errors::ErrorWire::from_error(&error);
                crate::util::log::event!(warn, (wire.code.as_str()), {
                    "site": "engine.poison",
                    "message": wire.message,
                });
                self.replace_connection().with_context(|| {
                    format!("PENDING_SAVE_PERSIST_FAILED: after a failed rollback ({error:#})")
                })?;
                failed_rollback = Some(error);
            }
        }
        let retained = self.retain_all_dirty();
        match failed_rollback {
            Some(rollback) => {
                retained.with_context(|| format!("after a failed rollback ({rollback:#})"))
            }
            None => retained,
        }
    }
    /// Swaps the store's connection for a fresh one to the same database and
    /// closes the old one, which rolls back whatever transaction it had open.
    fn replace_connection(&mut self) -> Result<()> {
        let fresh = rusqlite::Connection::open(self.store.root.join("metadata.sqlite"))?;
        fresh.busy_timeout(std::time::Duration::from_secs(5))?;
        fresh.execute_batch("PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;")?;
        let old = std::mem::replace(&mut self.store.db, fresh);
        old.close()
            .map_err(|(_, error)| error)
            .context("cannot close the connection")
    }
    fn flush_entity(&mut self, entity: &str) -> Result<()> {
        let session = self.sessions.get_mut(entity).context("INVALID_HANDLE")?;
        if !session.dirty {
            return Ok(());
        }
        let pending: bool = self.store.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM pending_saves WHERE entity=?)",
            [entity],
            |r| r.get(0),
        )?;
        // A retained save commits its write and the removal of its row
        // together. From autocommit that transaction begins IMMEDIATE, taking
        // SQLite's writer slot before the write reads the projection (as
        // `Engine::control` does): a deferred read-then-write transaction fails
        // at once with SQLITE_BUSY/SQLITE_BUSY_SNAPSHOT when another connection
        // (the bulk transfer view, a pooled secondary store) writes meanwhile.
        let outer = if pending {
            Some(begin_staged_save(&self.store.db)?)
        } else {
            None
        };
        let result = (|| {
            let revision = match &mut session.bytes {
                crate::staging::Staged::Memory(bytes) => self.store.write_with_base_and_times(
                    entity,
                    session.content_base.clone(),
                    session.alive_base.clone(),
                    bytes,
                    session.preserve_times,
                )?,
                staged => {
                    let mut source = staged.reader()?;
                    self.store.write_stream_with_base_and_times(
                        entity,
                        session.content_base.clone(),
                        session.alive_base.clone(),
                        &mut source,
                        session.preserve_times,
                    )?
                }
            };
            if let Some(outer) = outer {
                self.store
                    .db
                    .execute("DELETE FROM pending_saves WHERE entity=?", [entity])?;
                commit_staged_save(&self.store.db, outer)?;
            }
            Ok(revision)
        })();
        // `pending_saves` is re-counted once the transaction is resolved
        // (committed here, rolled back below), never inside it.
        match result {
            Ok(revision) => {
                if pending {
                    count_pending_saves(&self.store, &self.vitals);
                }
                session.content_base = Some(revision.clone());
                session.alive_base = Some(revision);
                session.dirty = false;
                session.bytes.release();
                session.entry = self.store.entry(entity)?;
                if self.recovery_health && !self.sessions.values().any(|s| s.dirty) {
                    self.health = None;
                    self.recovery_health = false;
                }
                Ok(())
            }
            Err(error) => {
                if let Some(outer) = outer {
                    rollback_staged_save(&self.store.db, outer);
                    count_pending_saves(&self.store, &self.vitals);
                }
                self.retain_pending(entity, &error)
                    .context("PENDING_SAVE_PERSIST_FAILED")?;
                Err(error)
            }
        }
    }
    pub fn retry_pending(&mut self) -> Result<()> {
        let entities: Vec<_> = self
            .sessions
            .iter()
            .filter(|(_, s)| s.dirty && s.references == 0)
            .map(|(id, _)| id.clone())
            .collect();
        for entity in entities {
            self.flush_entity(&entity)?;
            self.sessions.remove(&entity);
        }
        Ok(())
    }
    /// [`Engine::retry_pending`] with a per-entity backoff (1 s → 60 s, WP2.3
    /// task 5): each dirty, unreferenced session is tried when it is due; a
    /// failure (the session stays retained) delays only that entity, and the
    /// others are still tried. Returns how many were committed.
    pub fn retry_pending_due(&mut self, now: Instant) -> usize {
        self.retry_state
            .retain(|entity, _| self.sessions.get(entity).is_some_and(|s| s.dirty));
        let due: Vec<_> = self
            .sessions
            .iter()
            .filter(|(_, s)| s.dirty && s.references == 0)
            .map(|(id, _)| id.clone())
            .filter(|id| self.retry_state.get(id).is_none_or(|s| s.next_at <= now))
            .collect();
        let mut committed = 0;
        for entity in due {
            match self.flush_entity(&entity) {
                Ok(()) => {
                    self.sessions.remove(&entity);
                    self.retry_state.remove(&entity);
                    committed += 1;
                }
                Err(error) => {
                    let state = self
                        .retry_state
                        .entry(entity.clone())
                        .or_insert(RetryState {
                            failures: 0,
                            next_at: now,
                        });
                    state.failures = state.failures.saturating_add(1);
                    let delay = RETRY_BASE
                        .saturating_mul(1u32 << state.failures.saturating_sub(1).min(6))
                        .min(RETRY_CAP);
                    state.next_at = now + delay;
                    crate::util::log::event!(warn, PENDING_SAVE, {
                        "site": "engine.retry_pending",
                        "entity": entity,
                        "failures": state.failures,
                        "retry_in_ms": delay.as_millis() as u64,
                        "message": format!("{error:#}"),
                    });
                }
            }
        }
        committed
    }
    pub fn open(
        &mut self,
        path: &str,
        create: Option<&str>,
        writable: bool,
    ) -> Result<(u64, Entry)> {
        self.open_with_attributes(path, create, writable, None)
    }
    pub fn open_with_attributes(
        &mut self,
        path: &str,
        create: Option<&str>,
        writable: bool,
        attributes: Option<u32>,
    ) -> Result<(u64, Entry)> {
        ensure!(!self.switching, "BUSY_VIEW");
        let special = path
            .replace('\\', "/")
            .trim_matches('/')
            .eq_ignore_ascii_case(".tkfs-runtime.json");
        let entry = if special {
            ensure!(create.is_none() && !writable, "PERMISSION_DENIED");
            Entry {
                id: "control".into(),
                name: ".tkfs-runtime.json".into(),
                kind: "file".into(),
                parent: ROOT.into(),
                alive: true,
                ..Default::default()
            }
        } else if let Some(kind) = create {
            self.store.create_with_attributes(path, kind, attributes)?
        } else {
            self.store.lookup(path)?
        };
        ensure!(
            create.is_some()
                || !writable
                || entry.kind == "directory"
                || entry.basic_info().attributes & 1 == 0,
            "ACCESS_DENIED: readonly file"
        );
        if !self.sessions.contains_key(&entry.id) {
            let bytes = if special {
                crate::staging::Staged::memory(self.discovery.clone())
            } else {
                crate::staging::Staged::from_object(&self.store, entry.content.as_deref())?
            };
            self.sessions.insert(
                entry.id.clone(),
                Session {
                    content_base: entry.revisions.get("content").cloned(),
                    alive_base: entry.revisions.get("alive").cloned(),
                    entry: entry.clone(),
                    bytes,
                    dirty: false,
                    references: 0,
                    preserve_times: 0,
                },
            );
        }
        let session = self.sessions.get_mut(&entry.id).unwrap();
        session.references += 1;
        let h = self.next_handle;
        self.next_handle += 1;
        self.handles.insert(
            h,
            Handle {
                entity: entry.id.clone(),
                generation: self.store.generation,
                cleaned: false,
                writable,
                time_update_disabled: 0,
                directory: None,
            },
        );
        Ok((h, entry))
    }
    pub fn session(&self, h: u64) -> Result<&Session> {
        let handle = self.handles.get(&h).context("INVALID_HANDLE")?;
        ensure!(handle.generation == self.store.generation, "STALE_VIEW");
        self.sessions.get(&handle.entity).context("INVALID_HANDLE")
    }
    /// One directory cut per enumeration, retained through marker continuations.
    /// A rewind sees later mutations; close releases the cursor. Generation
    /// validation applies even to an already populated cursor.
    pub fn directory_entry(
        &mut self,
        h: u64,
        ordinal: usize,
        restart: bool,
    ) -> Result<Option<(Entry, usize)>> {
        let directory = self.session(h)?.entry.clone();
        ensure!(directory.kind == "directory", "NOT_DIRECTORY");
        if restart || self.handles[&h].directory.is_none() {
            let mut children = self.store.children(&directory.id)?;
            children.sort_by_key(|entry| entry.name.to_lowercase());
            let mut parent = self
                .store
                .entry(&directory.parent)
                .or_else(|_| self.store.lookup(""))?;
            parent.name = "..".into();
            let mut directory = directory;
            directory.name = ".".into();
            let mut cut = Vec::with_capacity(children.len() + 2);
            cut.push((directory, 0));
            cut.push((parent, 0));
            for entry in children {
                let size = entry
                    .content
                    .as_ref()
                    .map(|object| self.store.object_size(object))
                    .transpose()?
                    .unwrap_or(0);
                cut.push((entry, usize::try_from(size)?));
            }
            self.handles.get_mut(&h).unwrap().directory = Some(cut);
        }
        Ok(self.handles[&h]
            .directory
            .as_ref()
            .unwrap()
            .get(ordinal)
            .cloned())
    }
    pub fn write(
        &mut self,
        h: u64,
        offset: u64,
        bytes: &[u8],
        append: bool,
        constrained: bool,
    ) -> Result<usize> {
        let handle = self.handles.get(&h).context("INVALID_HANDLE")?;
        ensure!(
            handle.writable && !handle.cleaned && handle.generation == self.store.generation,
            "STALE_VIEW_OR_ACCESS_DENIED"
        );
        let session = self.sessions.get_mut(&handle.entity).unwrap();
        ensure!(
            session.entry.kind == "file" && session.entry.id != "control",
            "IS_DIRECTORY"
        );
        let start = if append {
            session.bytes.len()
        } else {
            usize::try_from(offset)?
        };
        let mut count = bytes.len();
        if constrained {
            count = count.min(session.bytes.len().saturating_sub(start));
        }
        if count == 0 {
            return Ok(0);
        }
        let end = start.checked_add(count).context("FILE_TOO_LARGE")?;
        ensure!(end as u64 <= MAX_CONTENT, "FILE_TOO_LARGE");
        if end > session.bytes.len() {
            session.bytes.resize(&self.store.root, end)?;
        }
        session
            .bytes
            .write_at(&self.store.root, start, &bytes[..count])?;
        session.preserve_times = handle.time_update_disabled;
        session.dirty = true;
        Ok(count)
    }
    pub fn truncate(&mut self, h: u64, size: u64, allocation: bool) -> Result<()> {
        let handle = self.handles.get(&h).context("INVALID_HANDLE")?;
        ensure!(handle.writable && !handle.cleaned, "PERMISSION_DENIED");
        let session = self.sessions.get_mut(&handle.entity).unwrap();
        ensure!(session.entry.kind == "file", "IS_DIRECTORY");
        ensure!(size <= MAX_CONTENT, "FILE_TOO_LARGE");
        if !allocation || size < (session.bytes.len() as u64) {
            session.bytes.resize(&self.store.root, size as usize)?;
            session.preserve_times = handle.time_update_disabled;
            session.dirty = true;
        }
        Ok(())
    }
    pub fn flush(&mut self, h: u64) -> Result<()> {
        if h == 0 {
            let entities: Vec<_> = self.sessions.keys().cloned().collect();
            for entity in entities {
                self.flush_entity(&entity)?;
            }
            // A volume flush can commit a retained closed session before the
            // periodic retry. Retire it before another view can reuse its ID.
            self.sessions
                .retain(|_, session| session.references > 0 || session.dirty);
            return Ok(());
        }
        let entity = self
            .handles
            .get(&h)
            .context("INVALID_HANDLE")?
            .entity
            .clone();
        self.flush_entity(&entity)
    }
    /// Metadata-only durability never publishes an unrelated dirty content buffer.
    pub fn set_basic(&mut self, h: u64, attributes: u32, times: [u64; 4]) -> Result<()> {
        let entry = self.session(h)?.entry.clone();
        ensure!(entry.id != "control", "PERMISSION_DENIED");
        ensure!(entry.id != ROOT, "UNSUPPORTED_FS_OPERATION: root metadata");
        let current = self.store.entry(&entry.id)?;
        let mut basic = current.basic_info();
        if attributes != u32::MAX {
            basic.attributes = BasicInfo::attributes(&entry.kind, attributes)?;
        }
        let mut disabled = self.handles[&h].time_update_disabled;
        for (i, value) in times.into_iter().enumerate() {
            let bit = if i == 0 { 0 } else { 1 << (i - 1) };
            match value {
                0 => {}
                u64::MAX if i != 0 => disabled |= bit,
                v if v == u64::MAX - 1 && i != 0 => disabled &= !bit,
                v if v <= i64::MAX as u64 => match i {
                    0 => basic.creation_time = v,
                    1 => basic.access_time = v,
                    2 => basic.write_time = v,
                    _ => basic.change_time = v,
                },
                _ => bail!("INVALID_TIMESTAMP"),
            }
        }
        self.store.set_basic(&entry.id, basic)?;
        self.handles.get_mut(&h).unwrap().time_update_disabled = disabled;
        let s = self.sessions.get_mut(&entry.id).unwrap();
        s.entry.basic = Some(basic);
        if s.dirty {
            if times[2] != 0 {
                s.preserve_times |= 2;
            }
            if times[3] != 0 {
                s.preserve_times |= 4;
            }
        }
        Ok(())
    }
    pub fn cleanup(&mut self, h: u64, delete: bool) -> Result<()> {
        self.flush(h)?;
        if delete {
            let entity = self
                .handles
                .get(&h)
                .context("INVALID_HANDLE")?
                .entity
                .clone();
            self.store.delete(&entity)?;
        }
        self.handles.get_mut(&h).context("INVALID_HANDLE")?.cleaned = true;
        Ok(())
    }
    pub fn close(&mut self, h: u64) -> Result<()> {
        let result = self.flush(h);
        if let Some(handle) = self.handles.remove(&h) {
            let s = self.sessions.get_mut(&handle.entity).unwrap();
            s.references -= 1;
            if s.references == 0 && !s.dirty {
                self.sessions.remove(&handle.entity);
            }
        }
        result
    }
    pub fn quiet(&self) -> Result<()> {
        ensure!(
            self.handles.is_empty()
                && self.health.is_none()
                && !self.sessions.values().any(|s| s.dirty),
            "BUSY_VIEW: {} open contexts, {} unsaved files; {}",
            self.handles.len(),
            self.sessions.values().filter(|s| s.dirty).count(),
            self.health.as_deref().unwrap_or("healthy")
        );
        Ok(())
    }
    pub fn receive_peer(
        &mut self,
        bundle: Bundle,
        allowed: &BTreeSet<String>,
    ) -> Result<Vec<String>> {
        self.receive_peer_budget(bundle, allowed, None)
    }
    /// A legacy receive: stage the bundle, activate up to `budget` events
    /// (`None`: until nothing is ready), then [`Engine::after_activation`] for
    /// what was activated. While a background activator runs
    /// (`background_activation`), it only stages (budget 0): the activator
    /// commits, off this caller's request (WP2.3 task 8).
    pub(crate) fn receive_peer_budget(
        &mut self,
        bundle: Bundle,
        allowed: &BTreeSet<String>,
        budget: Option<usize>,
    ) -> Result<Vec<String>> {
        let budget = if self.background_activation {
            Some(0)
        } else {
            budget
        };
        let before = if budget == Some(0) {
            None
        } else {
            self.capture_before()
        };
        let accepted = self.store.receive_with_budget(bundle, allowed, budget)?;
        // Staging alone commits nothing: the accepted ids are then only the
        // events that were already committed, and no session can change.
        if budget != Some(0) && !accepted.is_empty() {
            let delta = self.activation_delta(&accepted, before)?;
            self.after_activation(&delta);
        }
        Ok(accepted)
    }
}

/// Opens the transaction that commits a retained save (`SAVEPOINT staged_save`)
/// and reports whether it is the outermost one: from autocommit it begins with
/// `BEGIN IMMEDIATE` (CCR-WP0.INT-5). Inside a caller's transaction only the
/// savepoint is opened, as before.
fn begin_staged_save(db: &rusqlite::Connection) -> Result<bool> {
    let outer = db.is_autocommit();
    if outer {
        db.execute_batch("BEGIN IMMEDIATE")?;
    }
    if let Err(error) = db.execute_batch("SAVEPOINT staged_save") {
        if outer {
            let _ = db.execute_batch("ROLLBACK");
        }
        return Err(error.into());
    }
    Ok(outer)
}

/// Commits the save and the removal of its `pending_saves` row together.
fn commit_staged_save(db: &rusqlite::Connection, outer: bool) -> Result<()> {
    db.execute_batch("RELEASE staged_save")?;
    if outer {
        db.execute_batch("COMMIT")?;
    }
    Ok(())
}

/// Discards the save's writes, including after a failed `COMMIT`; the caller
/// then retains the session again.
fn rollback_staged_save(db: &rusqlite::Connection, outer: bool) {
    if !outer {
        let _ = db.execute_batch("ROLLBACK TO staged_save; RELEASE staged_save");
    } else if !db.is_autocommit() {
        // SQLite may already have rolled the transaction back (for example
        // after an I/O error); then there is nothing left to discard.
        let _ = db.execute_batch("ROLLBACK");
    }
}
