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
//!
//! **Pending saves that cannot be restored (WP4.INT re-review).** Each
//! `pending_saves` record is restored on its own: one whose staging file is
//! missing or unreadable (an antivirus scanner holding it at startup), whose
//! record does not parse or that belongs to another branch is kept, reported
//! (`unrecovered`: the store is degraded with `RECOVERY_REQUIRED`, naming the
//! entity) and retried with the pending-save backoff, while the others load.
//! It no longer writes `Engine.health`, so it never gates `quiet()`, Stop,
//! Shutdown or control requests (it used to, for good, with a misleading
//! `BUSY_VIEW: 0 open contexts, 0 unsaved files`). The `pending-saves`
//! control op lists the records, and `discard-pending-save{entity}` drops one
//! that cannot be restored (README runbook). A later save of the same file on
//! the same branch supersedes such a record.
//!
//! **A pending save is never lost to another branch (WP4.INT confirmation
//! review).** `pending_saves` holds one record per entity (a positional table,
//! §9: its key cannot change), and each record belongs to the branch it was
//! saved on. So:
//! * a checkout is refused while a record that could belong to the active
//!   branch is not restored ([`Engine::checkout_allowed`],
//!   `RECOVERY_REQUIRED`): the record is retried, or discarded by the user,
//!   first. A restored one is a dirty session, which `quiet()` refuses
//!   already. This build therefore never leaves a record of the active branch
//!   behind a checkout;
//! * a record of another branch (left by an earlier build) is never
//!   superseded: a save of the same entity commits without touching it
//!   (`flush_entity`), a retain refuses to overwrite it (`retain_pending`),
//!   and the entity cannot be opened for writing on this branch
//!   (`ACCESS_DENIED`) until that branch is checked out, which restores the
//!   record, or the record is discarded. It is not retried while its branch
//!   is not active; a checkout makes every unrestored record due at once.
//!
//! **Saves off the engine lock (WP5.2, CCR-WP4.INT-5).** A save has three
//! phases:
//! 1. [`Engine::begin_save`], under the lock: snapshots the session's bytes
//!    (a copy of in-memory bytes, at most 256 KiB, or a second handle to its
//!    staging file) and marks it `saving`;
//! 2. [`store_snapshot`], without the lock: hashes the bytes, writes them to
//!    `objects/<id>.part`, `fsync`s it and installs it under its hash with a
//!    write-through rename (invariant 1), or verifies the copy already there;
//! 3. [`Engine::finish_save`], under the lock: commits the object as the
//!    session's content with the bases of the snapshot (the base check: the
//!    branch, the generation and the session's bases are those of the
//!    snapshot; saves of one entity never overlap), together with the
//!    removal (or the rebase) of a `pending_saves` record of it.
//!
//! While a save reads a staging file nothing writes it: writers wait for the
//! save without the lock ([`lock_for_write`]), and an unsynchronized
//! [`Engine::write`] is refused (`BUSY_VIEW`). In-memory bytes may change
//! during the save; the session then stays dirty and saves again. Every
//! failure of any phase retains the session (`pending_saves`), as a failed
//! save always did, and a crash at any step leaves the last committed bytes
//! (an orphaned `.part` is removed at the next startup). Dirty sessions keep
//! surviving detach, stop and restart (invariant 8): a save in progress is a
//! dirty session until its commit, so `quiet()` refuses it, a force detach
//! or a poison exit retains it, and its commit then removes the record.
//!
//! The adapters save this way ([`save_handle`] for the durability points,
//! `FlushFileBuffers` and fsync(2) (`FUSE_FSYNC`), never write-behind, and
//! for renames, deletes on close and a lone close), and during a burst of
//! closes ([`write_behind`]) a closed file is saved in the background
//! ([`save_handle_in_background`], the WinFsp cleanup and the FUSE flush of a
//! close(2)): a bounded pool of saver threads commits it
//! shortly after the close, giving the engine to filesystem callbacks first
//! ([`foreground`]); with their queue full a close saves before it returns
//! (backpressure: closed files not committed yet are at most
//! [`MAX_QUEUED_SAVES`] + [`MAX_BACKGROUND_SAVES`]); a delete or a rename over
//! a closed file waits for its save ([`settle_entity`], [`lock_settled`]),
//! and `quiet()` counts it as unsaved until it is committed. The
//! availability rows of saved objects are written on a connection of their
//! own, in batches ([`AvailabilityNotes`]).
//!
//! **Deferred creates (WP5.2).** A file created through the WinFsp mount
//! ([`Engine::create_deferred`]) is not committed when it is created: its
//! first save commits one event that creates it with its content (it used to
//! be two commits, each `fsync`ing the journal under the engine lock, the
//! first on the application's create). Until then it is a dirty session the
//! mount shows like any file ([`Engine::lookup_visible`],
//! [`Engine::children_visible`]; a second create of the name is refused,
//! basic information set on it stays in the session), and every rule of
//! dirty sessions holds: `quiet()` refuses it, a force detach or a poison
//! exit retains it (its record holds its location, and a restart shows it
//! again), and a crash before its first commit loses the new file only, as
//! it loses any unsaved bytes. Anything that would change or replace its
//! name commits it first ([`settle_entity`]: a rename of it saves it, a
//! rename over it or a delete of it settles it). A directory, a FUSE create
//! and every create outside the mount commit at once; `TKFS_DEFERRED_CREATE=0`
//! turns deferred creates off. A remote create of the same name in the
//! meantime makes a conflict, as two concurrent creates do.
use super::{
    Shared,
    notify::{NotifyQueue, delta_notifications},
};
use crate::core::{inbox::SliceBudget, *};
use anyhow::{Context, Result, bail, ensure};
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::io::Write;
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
    /// Bumped by every change of `bytes` (write, truncate): a save commits
    /// the snapshot it took, and leaves the session dirty when it moved since.
    pub edits: u64,
    /// A save of this session runs ([`Engine::begin_save`] to
    /// [`Engine::finish_save`]): no second save starts, and while the bytes
    /// are a staging file nothing writes them ([`Engine::write_blocked`]).
    pub saving: bool,
    /// A background saver has the save of this session queued
    /// ([`save_handle_in_background`]); cleared when a save of it begins or
    /// ends.
    pub queued: bool,
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
    /// `pending_saves` records not restored into a session, by entity (module
    /// docs); retried by [`Engine::retry_pending_due`].
    unrecovered: BTreeMap<String, Unrecovered>,
    /// Moves whenever a save ends (module docs, *Saves off the engine lock*).
    saves: Arc<SaveSignal>,
    /// The availability rows of objects saved off the engine lock.
    notes: Arc<AvailabilityNotes>,
    /// What `observation` was last built from ([`Engine::publish_observation`]).
    observed: Mutex<Option<Observed>>,
    /// Files created through the mount whose creation is not committed yet
    /// (module docs, *Deferred creates*): `(parent, name key)` → entity.
    pending_creates: BTreeMap<(String, String), String>,
    /// The shared engine this engine is (set by the mount adapters'
    /// `bind`): the pending-save retry hands staging-file saves to a thread
    /// that stores them without the engine lock.
    shared: Option<std::sync::Weak<Mutex<Engine>>>,
    /// Entities whose save [`Engine::retry_pending_due`] handed to the
    /// background savers, until it commits.
    retry_handed: BTreeSet<String>,
    /// Saves of `retry_handed` entities the savers committed since the last
    /// [`Engine::retry_pending_due`] (which reports them).
    retry_commits: usize,
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

/// A `pending_saves` record that could not be restored into a session (see
/// the module docs), retried with the pending-save backoff.
#[derive(Debug, Clone)]
struct Unrecovered {
    error: String,
    failures: u32,
    next_at: Instant,
    /// The branch the record belongs to (`None`: the record does not parse).
    branch: Option<String>,
}

/// The branch of a `pending_saves` record (`None`: it does not parse).
fn record_branch(record: &str) -> Option<String> {
    serde_json::from_str::<PendingSave>(record)
        .ok()
        .map(|pending| pending.branch)
}

/// What [`Engine::publish_observation`] last built the observation from.
struct Observed {
    /// `(generation, mounted, open handles, unflushed files)`.
    key: (u64, bool, usize, usize),
    health: Option<String>,
    active: String,
    branch: Value,
}

/// The delay before retry number `failures + 1` (1 s → 60 s).
fn retry_delay(failures: u32) -> Duration {
    RETRY_BASE
        .saturating_mul(1u32 << failures.saturating_sub(1).min(6))
        .min(RETRY_CAP)
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
    /// Background activator cycles (slice plus slow commit) begun (each
    /// numbered by it; a cycle begins before it takes the engine), and the
    /// number of the latest one that finished as an activation pass.
    activator_cycles_begun: AtomicU64,
    activator_cycles_finished: AtomicU64,
    /// `pending_saves` records not restored into a session (module docs).
    unrecovered_saves: AtomicU32,
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
    /// `pending_saves` records not restored into a session (WP4.INT
    /// re-review; the maintenance thread retries them).
    pub fn unrecovered_saves(&self) -> u32 {
        self.unrecovered_saves.load(Ordering::Relaxed)
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
    /// Begins a background activator cycle, numbered one past every cycle
    /// begun before it; it finishes when the returned guard drops, unless
    /// [`ActivatorCycle::abandon`]ed. Begun before the cycle takes the engine,
    /// so a cycle numbered past [`EngineVitals::activator_cycles`]`().0` sees
    /// every row committed before that read.
    pub fn activator_cycle(self: &Arc<Self>) -> ActivatorCycle {
        let number = self.activator_cycles_begun.fetch_add(1, Ordering::SeqCst) + 1;
        ActivatorCycle {
            vitals: self.clone(),
            number,
            pass: true,
        }
    }
    /// `(begun, finished)`: the background activator cycles begun, and the
    /// number of the latest cycle that finished as an activation pass. A
    /// cycle that began after a read of `begun` ran a whole pass once
    /// `finished > begun` (cycles run one at a time, on the activator
    /// thread); an abandoned cycle never satisfies that, nor delays it.
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

/// Returned by [`EngineVitals::activator_cycle`]: records the cycle as the
/// latest finished pass on drop (also on an error or a panic), unless it was
/// abandoned.
pub struct ActivatorCycle {
    vitals: Arc<EngineVitals>,
    number: u64,
    pass: bool,
}
impl ActivatorCycle {
    /// The cycle did nothing (its slice found SQLite's writer held by
    /// another connection and is retried): it is not an activation pass, so
    /// a `revalidate` waiting for one keeps waiting.
    pub fn abandon(mut self) {
        self.pass = false;
    }
}
impl Drop for ActivatorCycle {
    fn drop(&mut self) {
        if self.pass {
            self.vitals
                .activator_cycles_finished
                .fetch_max(self.number, Ordering::SeqCst);
        }
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
/// Removes `STATE/staging/<name>` when `name` is a staging file's name (a
/// uuid with `.stage`); anything else is left alone.
fn remove_stage(root: &std::path::Path, name: &str) {
    if name.ends_with(".stage") && uuid::Uuid::parse_str(name.trim_end_matches(".stage")).is_ok() {
        let _ = std::fs::remove_file(root.join("staging").join(name));
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
            unrecovered: BTreeMap::new(),
            saves: Arc::new(SaveSignal::default()),
            notes: Arc::new(AvailabilityNotes::new(&store)),
            shared: None,
            retry_handed: BTreeSet::new(),
            retry_commits: 0,
            observed: Mutex::new(None),
            pending_creates: BTreeMap::new(),
            store,
        };
        if let Err(error) = engine.load_pending() {
            engine.health = Some(format!("RECOVERY_REQUIRED: {error:#}"));
        }
        engine.publish_observation();
        engine
    }
    /// Refreshes the vitals and the observation. Every filesystem callback
    /// calls it (WP5.2: it was a fifth of a cheap callback's cost): the
    /// observation is rebuilt only when what it shows changed, and the
    /// active branch's descriptor is read again only after a checkout.
    pub fn publish_observation(&self) {
        self.refresh_vitals();
        let unflushed = self.sessions.values().filter(|s| s.dirty).count();
        let health = self.reported_health();
        let mut observed = crate::util::lock::lock(&self.observed);
        let key = (
            self.store.generation,
            self.mounted,
            self.handles.len(),
            unflushed,
        );
        if let Some(known) = observed.as_ref()
            && known.key == key
            && known.health == health
            && known.active == self.store.active
        {
            return;
        }
        let branch = match observed.take() {
            Some(known)
                if known.active == self.store.active && known.key.0 == self.store.generation =>
            {
                known.branch
            }
            _ => serde_json::to_value(self.store.branch(&self.store.active).ok())
                .unwrap_or(Value::Null),
        };
        *crate::util::lock::lock(&self.observation) = json!({"repo":self.store.repo,"device":self.store.device,
            "branch":branch,"generation":self.store.generation,"mounted":self.mounted,
            "open_handles":self.handles.len(),"unflushed_files":unflushed,
            "health":health,"sampled_at_ms":time(),"caught_up":false});
        *observed = Some(Observed {
            key,
            health,
            active: self.store.active.clone(),
            branch,
        });
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
        v.unrecovered_saves
            .store(count(self.unrecovered.len()), Ordering::Relaxed);
        {
            let reported = self.reported_health();
            let mut health = crate::util::lock::lock(&v.health);
            if *health != reported {
                *health = reported;
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
    /// Restores every `pending_saves` record into a dirty session, each on
    /// its own: one that cannot be restored is kept, reported and retried
    /// (module docs) and the others still load. Only a failure to read the
    /// table itself is an error (`Engine::new` then reports
    /// `RECOVERY_REQUIRED` as the engine's health, as before).
    fn load_pending(&mut self) -> Result<()> {
        self.store.db.execute_batch("CREATE TABLE IF NOT EXISTS pending_saves(entity TEXT PRIMARY KEY,payload TEXT NOT NULL)")?;
        let records = self.pending_records()?;
        self.refresh_pending_saves();
        let now = Instant::now();
        for (entity, record) in records {
            crate::startup::progress("recovering pending saves");
            // Qualification: slows recovery down to observe its progress.
            crate::util::fault::hang("engine.recover_pending");
            if let Err(error) = self.restore_pending(&record) {
                self.note_unrecovered(&entity, &record, &error, now);
            }
        }
        if self.sessions.values().any(|s| s.dirty) {
            self.health = Some("PENDING_SAVES: recovery in progress".into());
            self.recovery_health = true;
        }
        Ok(())
    }
    /// Every `pending_saves` row: `(entity, payload)`.
    fn pending_records(&self) -> Result<Vec<(String, String)>> {
        Ok(self
            .store
            .db
            .prepare("SELECT entity,payload FROM pending_saves ORDER BY entity")?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()?)
    }
    /// Restores one `pending_saves` record into a dirty session.
    fn restore_pending(&mut self, record: &str) -> Result<()> {
        let pending: PendingSave = serde_json::from_str(record)
            .context("RECOVERY_REQUIRED: the pending-save record cannot be read")?;
        ensure!(
            pending.branch == self.store.active,
            "RECOVERY_BRANCH_MISMATCH: the unsaved file belongs to branch {}, not the active branch",
            pending.branch
        );
        let bytes = crate::staging::Staged::recover(&self.store.root, &pending.stage)
            .with_context(|| {
                format!(
                    "RECOVERY_REQUIRED: the staging file {} of the unsaved file cannot be opened",
                    pending.stage
                )
            })?;
        if !self.is_committed(&pending.entry.id)? {
            // A deferred create retained before its first commit (module
            // docs, *Deferred creates*): visible again, committed by its save.
            let key = name_key(&pending.entry.name)
                .context("RECOVERY_REQUIRED: the unsaved new file's name is not valid")?;
            self.pending_creates.insert(
                (pending.entry.parent.clone(), key),
                pending.entry.id.clone(),
            );
        }
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
                edits: 0,
                saving: false,
                queued: false,
            },
        );
        Ok(())
    }
    /// Whether the active view has `entity`. `Ok(false)` only when it does
    /// not: a failure to read the view is an error, never taken for a
    /// deferred create (which would commit an existing entity's kind,
    /// location and content again, with no parents).
    fn is_committed(&self, entity: &str) -> Result<bool> {
        match self.store.entry(entity) {
            Ok(_) => Ok(true),
            // `Store::entry`'s own answer for an entity the view lacks.
            Err(error) if error.chain().count() == 1 && error.to_string() == "FILE_NOT_FOUND" => {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }
    /// Records that `entity`'s pending save (`record`) could not be restored
    /// (again).
    fn note_unrecovered(
        &mut self,
        entity: &str,
        record: &str,
        error: &anyhow::Error,
        now: Instant,
    ) {
        let failures = self
            .unrecovered
            .get(entity)
            .map_or(1, |known| known.failures.saturating_add(1));
        let message = format!("{error:#}");
        let first = self
            .unrecovered
            .get(entity)
            .is_none_or(|known| known.error != message);
        if first {
            let wire = crate::model::errors::ErrorWire::from_error(error);
            crate::util::log::event!(warn, (wire.code.as_str()), {
                "site": "engine.recover_pending",
                "entity": entity,
                "failures": failures,
                "message": wire.message,
            });
        }
        self.unrecovered.insert(
            entity.to_owned(),
            Unrecovered {
                error: message,
                failures,
                next_at: now + retry_delay(failures),
                branch: record_branch(record),
            },
        );
    }
    /// Forgets an unrestored record of `entity` that a newer save of the same
    /// entity replaced or committed (its row is gone or rewritten), and
    /// removes its staging file. `keep` is the newer record's staging file.
    fn forget_unrecovered(&mut self, entity: &str, record: Option<&str>, keep: Option<&str>) {
        if self.unrecovered.remove(entity).is_none() {
            return;
        }
        if let Some(stage) = record
            .and_then(|record| serde_json::from_str::<PendingSave>(record).ok())
            .map(|pending| pending.stage)
            .filter(|stage| Some(stage.as_str()) != keep)
        {
            remove_stage(&self.store.root, &stage);
        }
    }
    /// The health this engine reports: [`Engine::health`], else the
    /// `pending_saves` records it could not restore (`RECOVERY_REQUIRED`,
    /// a degraded store that gates only a checkout, [`Engine::checkout_allowed`]).
    pub fn reported_health(&self) -> Option<String> {
        if self.health.is_some() {
            return self.health.clone();
        }
        let (entity, first) = self.unrecovered.iter().next()?;
        let count = self.unrecovered.len();
        Some(format!(
            "RECOVERY_REQUIRED: {count} unsaved file{} could not be restored (entity {entity}: {}); \
             they are retried (one of another branch once that branch is checked out); \
             `tkfs pending-saves` lists them and `tkfs discard-pending-save` drops one",
            if count == 1 { "" } else { "s" },
            first.error
        ))
    }
    /// The `pending-saves` control op: every record, restored or not.
    pub(crate) fn pending_saves_report(&self) -> Result<Value> {
        let records = self.pending_records()?;
        let saves: Vec<Value> = records
            .iter()
            .map(|(entity, record)| {
                let pending = serde_json::from_str::<PendingSave>(record).ok();
                let size = pending.as_ref().and_then(|pending| {
                    std::fs::metadata(self.store.root.join("staging").join(&pending.stage))
                        .ok()
                        .map(|metadata| metadata.len())
                });
                json!({
                    "entity": entity,
                    "name": pending.as_ref().map(|pending| pending.entry.name.clone()),
                    "branch": pending.as_ref().map(|pending| pending.branch.clone()),
                    "size": size,
                    "restored": self.sessions.get(entity).is_some_and(|s| s.dirty),
                    "error": self.unrecovered.get(entity).map(|u| u.error.clone()),
                })
            })
            .collect();
        Ok(json!({"pending_saves": saves}))
    }
    /// The `discard-pending-save{entity}` control op: drops a record that
    /// could not be restored (its row and staging file). A restored one is a
    /// dirty session being retried and is refused (`PENDING_SAVE_RESTORED`);
    /// an entity without a record answers `discarded: false`.
    pub(crate) fn discard_pending_save(&mut self, entity: &str) -> Result<Value> {
        ensure!(
            !self.sessions.get(entity).is_some_and(|s| s.dirty),
            "PENDING_SAVE_RESTORED: the unsaved file {entity} was restored and is being saved; it cannot be discarded"
        );
        let record: Option<String> = self
            .store
            .db
            .query_row(
                "SELECT payload FROM pending_saves WHERE entity=?",
                [entity],
                |row| row.get(0),
            )
            .optional()?;
        let Some(record) = record else {
            self.unrecovered.remove(entity);
            self.refresh_vitals();
            return Ok(json!({"entity": entity, "discarded": false}));
        };
        self.store
            .db
            .execute("DELETE FROM pending_saves WHERE entity=?", [entity])?;
        self.refresh_pending_saves();
        self.unrecovered.remove(entity);
        if let Ok(pending) = serde_json::from_str::<PendingSave>(&record) {
            remove_stage(&self.store.root, &pending.stage);
        }
        crate::util::log::event!(warn, PENDING_SAVE, {
            "site": "engine.discard_pending_save",
            "entity": entity,
            "detail": "an unsaved file that could not be restored was discarded",
        });
        self.refresh_vitals();
        Ok(json!({"entity": entity, "discarded": true}))
    }
    /// Removes the staging files (`STATE/staging/*.stage`) no
    /// `pending_saves` row names and no session of this engine uses, and the
    /// temporary object files (`STATE/objects/*.part`) of interrupted object
    /// writes (WP4.INT re-review: a crash, a kill or a power loss left them,
    /// and nothing removed them). Only the worker that holds `owner.lock`
    /// calls this, right after `Engine::new`. Returns the files and bytes
    /// removed.
    pub fn remove_orphaned_spools(&self) -> Result<(u64, u64)> {
        let mut named: BTreeSet<String> = self
            .pending_records()?
            .into_iter()
            .filter_map(|(_, record)| serde_json::from_str::<PendingSave>(&record).ok())
            .map(|pending| pending.stage)
            .collect();
        named.extend(self.sessions.values().filter_map(|session| {
            match &session.bytes {
                crate::staging::Staged::Disk { path, .. } => path
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned()),
                _ => None,
            }
        }));
        let mut removed = (0u64, 0u64);
        let mut sweep = |folder: std::path::PathBuf, orphan: &dyn Fn(&str) -> bool| {
            let Ok(entries) = std::fs::read_dir(&folder) else {
                return;
            };
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if !orphan(&name) {
                    continue;
                }
                let size = entry.metadata().map_or(0, |metadata| metadata.len());
                if std::fs::remove_file(entry.path()).is_ok() {
                    removed.0 += 1;
                    removed.1 += size;
                }
            }
        };
        sweep(self.store.root.join("staging"), &|name| {
            name.ends_with(".stage") && !named.contains(name)
        });
        sweep(self.store.root.join("objects"), &|name| {
            name.ends_with(".part") && uuid::Uuid::parse_str(name.trim_end_matches(".part")).is_ok()
        });
        Ok(removed)
    }
    pub(crate) fn retain_pending(&mut self, entity: &str, error: &anyhow::Error) -> Result<()> {
        if !self.sessions.get(entity).context("INVALID_HANDLE")?.dirty {
            return Ok(());
        }
        let existing: Option<String> = self
            .store
            .db
            .query_row(
                "SELECT payload FROM pending_saves WHERE entity=?",
                [entity],
                |row| row.get(0),
            )
            .optional()?;
        // A record of another branch is never overwritten (module docs; one
        // row per entity): its unsaved bytes would be lost. The entity cannot
        // be opened for writing on this branch while it waits, so this is
        // reached only by a session older than that record.
        if let Some(existing) = existing.as_deref()
            && self.of_other_branch(existing)
        {
            bail!(
                "RECOVERY_BRANCH_MISMATCH: an unsaved version of {entity} on branch {} waits to be \
                 restored there; this branch's unsaved changes to it are not retained over it",
                record_branch(existing).unwrap_or_default()
            );
        }
        self.recovery_health = self.health.is_none() || self.recovery_health;
        if self.recovery_health {
            self.health = Some(format!("PENDING_SAVE: {error:#}"));
        }
        let session = self.sessions.get_mut(entity).context("INVALID_HANDLE")?;
        let stage = session.bytes.retain(&self.store.root)?;
        let record = PendingSave {
            branch: self.store.active.clone(),
            entry: session.entry.clone(),
            stage,
            content_base: session.content_base.clone(),
            alive_base: session.alive_base.clone(),
            preserve_times: session.preserve_times,
        };
        let replaced = existing.filter(|_| self.unrecovered.contains_key(entity));
        self.store.db.execute("INSERT INTO pending_saves VALUES(?,?) ON CONFLICT(entity) DO UPDATE SET payload=excluded.payload",
            rusqlite::params![entity,serde_json::to_string(&record)?])?;
        self.refresh_pending_saves();
        // A newer save of the entity replaces a record that was not restored.
        self.forget_unrecovered(entity, replaced.as_deref(), Some(&record.stage));
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
    /// Saves `entity`'s dirty session under the caller's engine lock: the
    /// three phases of an off-lock save ([`save_entity`]) run back to back.
    /// While another save of the entity runs (off the lock), this one does
    /// nothing: that save commits what it took, and edits made since leave
    /// the session dirty for the next save.
    fn flush_entity(&mut self, entity: &str) -> Result<()> {
        match self.begin_save(entity)? {
            SaveStart::Clean | SaveStart::Busy(_) => Ok(()),
            SaveStart::Started(job) => {
                let stored = store_snapshot(&job);
                self.finish_save(job, stored)
            }
        }
    }
    /// Phase 1 of a save (CCR-WP4.INT-5), under the engine lock: snapshots the
    /// dirty session's bytes ([`crate::staging::Staged::snapshot`]) and marks
    /// it `saving`. `Clean`: nothing to save; `Busy`: a save of the entity
    /// runs, wait for [`SaveSignal`] to move past the returned generation.
    /// A snapshot that fails retains the session (`pending_saves`).
    pub fn begin_save(&mut self, entity: &str) -> Result<SaveStart> {
        let generation = self.saves.generation();
        let session = self.sessions.get(entity).context("INVALID_HANDLE")?;
        if !session.dirty {
            return Ok(SaveStart::Clean);
        }
        if session.saving {
            return Ok(SaveStart::Busy(generation));
        }
        let snapshot = crate::util::fault::trigger("engine.save.snapshot")
            .and_then(|()| session.bytes.snapshot());
        let bytes = match snapshot {
            Ok(bytes) => bytes,
            Err(error) => {
                self.retain_pending(entity, &error)
                    .context("PENDING_SAVE_PERSIST_FAILED")?;
                return Err(error);
            }
        };
        let job = SaveJob {
            entity: entity.to_owned(),
            edits: session.edits,
            content_base: session.content_base.clone(),
            alive_base: session.alive_base.clone(),
            branch: self.store.active.clone(),
            generation: self.store.generation,
            bytes,
            root: self.store.root.clone(),
            notes: self.notes.clone(),
        };
        let session = self.sessions.get_mut(entity).unwrap();
        session.saving = true;
        session.queued = false;
        Ok(SaveStart::Started(job))
    }
    /// Phase 3 of a save, under the engine lock: commits the object phase 2
    /// stored ([`store_snapshot`]) as the session's new content, with the
    /// bases the snapshot was taken at, and ends the save (writers and other
    /// saves of the entity may go on; [`SaveSignal`] moves). A retained
    /// save's `pending_saves` row goes in the same transaction: removed when
    /// the session was not edited since the snapshot (its bytes are the ones
    /// committed), else rebased onto the new revision (it holds later bytes).
    /// The session stays dirty when it was edited since the snapshot. Any
    /// failure, also one of phase 2, retains the session, as a failed save
    /// always did.
    pub fn finish_save(&mut self, job: SaveJob, stored: Result<StoredObject>) -> Result<()> {
        let result = self.finish_save_inner(&job, stored);
        let entity = job.entity.as_str();
        note_save_end();
        if let Some(session) = self.sessions.get_mut(entity) {
            session.saving = false;
            session.queued = false;
            if !session.dirty && session.references == 0 {
                // Closed while it was saved: nothing keeps it now.
                self.sessions.remove(entity);
            }
        }
        self.saves.bump();
        result
    }
    fn finish_save_inner(&mut self, job: &SaveJob, stored: Result<StoredObject>) -> Result<()> {
        let entity = job.entity.as_str();
        ensure!(self.sessions.contains_key(entity), "INVALID_HANDLE");
        let stored = match stored {
            Ok(stored) => stored,
            Err(error) => {
                if let Some(corrupt) = error.downcast_ref::<CorruptCopy>() {
                    // Recorded as `Store::put_object`'s dedupe records it: the
                    // store's own verification marks the object `CORRUPT` and
                    // stops serving it (its `note_corrupt`), so the inventory
                    // no longer advertises the bad copy. That verification
                    // hashes the copy again, under the engine lock (only when
                    // a copy is corrupt; CCR-WP5.2-7 asks for a hook that
                    // records it without the hash).
                    let _ = self.store.verify_object(&corrupt.object);
                }
                self.retain_pending(entity, &error)
                    .context("PENDING_SAVE_PERSIST_FAILED")?;
                return Err(error);
            }
        };
        // The base check. A checkout or a branch switch needs a quiet engine,
        // and saves of one entity never overlap, so neither can move under a
        // running save; refuse rather than commit onto another view. The
        // session stays dirty (and is retained by a stop as usual).
        let session = &self.sessions[entity];
        ensure!(
            job.branch == self.store.active
                && job.generation == self.store.generation
                && job.content_base == session.content_base
                && job.alive_base == session.alive_base,
            "STALE_VIEW: the file's view changed while it was saved; it stays unsaved and is saved again"
        );
        let row: Option<String> = self
            .store
            .db
            .query_row(
                "SELECT payload FROM pending_saves WHERE entity=?",
                [entity],
                |row| row.get(0),
            )
            .optional()?;
        // A record of another branch is not this save's (module docs): the
        // save commits without touching it. A record of this entity on this
        // branch is: this session's own retained save, or one that was not
        // restored, which this save supersedes (its row goes with the commit
        // below).
        let pending = row
            .as_deref()
            .is_some_and(|record| !self.of_other_branch(record));
        let replaced = row
            .clone()
            .filter(|_| pending && self.unrecovered.contains_key(entity));
        let unedited = self.sessions[entity].edits == job.edits;
        // A retained save commits its write and its row together. From
        // autocommit that transaction begins IMMEDIATE, taking SQLite's writer
        // slot before the write reads the projection (as `Engine::control`
        // does): a deferred read-then-write transaction fails at once with
        // SQLITE_BUSY/SQLITE_BUSY_SNAPSHOT when another connection writes
        // meanwhile. The bytes are already stored (phase 2), so the slot is
        // held for the commit only.
        let outer = if pending {
            Some(begin_staged_save(&self.store.db)?)
        } else {
            None
        };
        let preserve_times = self.sessions[entity].preserve_times;
        // A deferred create's first save (registered by `create_deferred` and
        // `restore_pending`), never an entity whose view could not be read.
        let deferred = self.is_deferred(entity);
        let result = (|| {
            crate::util::fault::trigger("engine.save.commit")?;
            let revision = if deferred {
                self.commit_deferred_create(entity, &stored.object, preserve_times)?
            } else {
                self.store.write_object_with_base_and_times(
                    entity,
                    job.content_base.clone(),
                    job.alive_base.clone(),
                    stored.object.clone(),
                    preserve_times,
                )?
            };
            if let Some(outer) = outer {
                match row.as_deref().filter(|_| !unedited) {
                    None => {
                        self.store
                            .db
                            .execute("DELETE FROM pending_saves WHERE entity=?", [entity])?;
                    }
                    Some(record) => {
                        // A retain after the snapshot recorded later bytes:
                        // they stay retained, on top of this commit.
                        let mut pending: PendingSave = serde_json::from_str(record)
                            .context("RECOVERY_REQUIRED: the pending-save record cannot be read")?;
                        pending.content_base = Some(revision.clone());
                        pending.alive_base = Some(revision.clone());
                        self.store.db.execute(
                            "UPDATE pending_saves SET payload=? WHERE entity=?",
                            rusqlite::params![serde_json::to_string(&pending)?, entity],
                        )?;
                    }
                }
                commit_staged_save(&self.store.db, outer)?;
            }
            Ok(revision)
        })();
        // `pending_saves` is re-counted once the transaction is resolved
        // (committed here, rolled back below), never inside it.
        match result {
            Ok(revision) => {
                // Qualification: a crash right after the commit.
                crate::util::fault::exit("engine.save.committed");
                if pending {
                    count_pending_saves(&self.store, &self.vitals);
                }
                if deferred {
                    self.pending_creates.retain(|_, pending| pending != entity);
                }
                if self.retry_handed.remove(entity) {
                    // A pending save the retry handed to a saver: reported by
                    // the next retry (`retry_pending_due`).
                    self.retry_commits += 1;
                }
                let entry = self.store.entry(entity);
                let session = self.sessions.get_mut(entity).context("INVALID_HANDLE")?;
                session.content_base = Some(revision.clone());
                session.alive_base = Some(revision);
                if unedited {
                    session.dirty = false;
                    session.bytes.release();
                }
                session.entry = entry?;
                if pending {
                    self.forget_unrecovered(entity, replaced.as_deref(), None);
                }
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
    /// Commits a deferred create (module docs, *Deferred creates*) with its
    /// first content, `object`: one event that creates the file at its
    /// session's location, with that content and its basic information (the
    /// write and change times of a save, unless preserved, as
    /// `Store::write_object_with_base_and_times` sets them). Another entry
    /// that took the name meanwhile (a remote create) makes a conflict, as
    /// concurrent creates do.
    fn commit_deferred_create(
        &mut self,
        entity: &str,
        object: &str,
        preserve_times: u8,
    ) -> Result<String> {
        let entry = self.sessions[entity].entry.clone();
        let branch = self.store.branch(&self.store.active)?;
        let change = |field: &str, value: Value| Change {
            entity: entity.to_owned(),
            field: field.into(),
            parents: vec![],
            value,
        };
        let changes = vec![
            change("kind", json!(entry.kind)),
            change(
                "location",
                json!({"parent": entry.parent, "name": entry.name}),
            ),
            change("alive", json!(true)),
            change("content", json!(object)),
        ];
        let mut event = self.store.event(&branch, changes);
        if entry.basic.is_some() || preserve_times != 0 {
            let now = filetime(event.created_ms);
            let mut basic = entry.basic_info();
            if preserve_times & 2 == 0 {
                basic.write_time = now;
            }
            if preserve_times & 4 == 0 {
                basic.change_time = now;
            }
            basic.attributes = BasicInfo::attributes(&entry.kind, basic.attributes | 0x20)?;
            event.changes.push(change("basic", json!(basic)));
        }
        let revision = event.id.clone();
        self.store.commit(event, true)?;
        Ok(revision)
    }
    /// Creates the file `path` through the mount without committing its
    /// creation (module docs, *Deferred creates*): the file is visible at
    /// once ([`Engine::lookup_visible`], directory listings), and its first
    /// save commits it together with its content. `TKFS_DEFERRED_CREATE=0`
    /// turns it off (the create commits at once, as before).
    pub fn create_deferred(
        &mut self,
        path: &str,
        writable: bool,
        attributes: Option<u32>,
    ) -> Result<(u64, Entry)> {
        if !deferred_creates() {
            return self.open_with_attributes(path, Some("file"), writable, attributes);
        }
        ensure!(!self.switching, "BUSY_VIEW");
        let normalized = path.replace('\\', "/");
        ensure!(
            !normalized
                .trim_matches('/')
                .eq_ignore_ascii_case(".tkfs-runtime.json"),
            "PERMISSION_DENIED"
        );
        let (parent, name) = split_path(path)?;
        let key = name_key(&name)?;
        let dir = self.store.lookup(&parent)?;
        ensure!(dir.kind == "directory", "NOT_DIRECTORY");
        ensure!(self.lookup_visible(path).is_err(), "NAME_COLLISION");
        let now = time();
        let basic = attributes
            .map(|attributes| -> Result<BasicInfo> {
                let stamp = filetime(now);
                Ok(BasicInfo {
                    posix_mode: None,
                    attributes: BasicInfo::attributes("file", attributes)?,
                    creation_time: stamp,
                    access_time: stamp,
                    write_time: stamp,
                    change_time: stamp,
                })
            })
            .transpose()?;
        let entry = Entry {
            id: id(),
            kind: "file".into(),
            parent: dir.id.clone(),
            name,
            content: Some(hash(b"")),
            alive: true,
            revisions: BTreeMap::new(),
            modified_ms: now,
            created_ms: now,
            basic,
        };
        self.pending_creates.insert((dir.id, key), entry.id.clone());
        self.sessions.insert(
            entry.id.clone(),
            Session {
                entry: entry.clone(),
                bytes: crate::staging::Staged::memory(vec![]),
                content_base: None,
                alive_base: None,
                dirty: true,
                references: 1,
                preserve_times: 0,
                edits: 0,
                saving: false,
                queued: false,
            },
        );
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
    /// Commits, inline, the deferred create that holds `path` (a rename's
    /// target, module docs, *Deferred creates*), so the rename sees it. One a
    /// background save is committing right now refuses the rename
    /// (`ACCESS_DENIED`, as a rename over an open file does).
    pub fn commit_created_at(&mut self, path: &str) -> Result<()> {
        let Ok(entry) = self.lookup_visible(path) else {
            return Ok(());
        };
        if !self.is_deferred(&entry.id) {
            return Ok(());
        }
        self.flush_entity(&entry.id)?;
        ensure!(
            !self.is_deferred(&entry.id),
            "ACCESS_DENIED: the file at the target is being saved"
        );
        Ok(())
    }
    /// Whether `entity` is a deferred create not committed yet.
    pub fn is_deferred(&self, entity: &str) -> bool {
        self.pending_creates
            .values()
            .any(|pending| pending == entity)
    }
    /// [`Store::lookup`], and the deferred creates (module docs, *Deferred
    /// creates*): the entry the mount shows at `path`.
    pub fn lookup_visible(&self, path: &str) -> Result<Entry> {
        let found = self.store.lookup(path);
        if found.is_ok() || self.pending_creates.is_empty() {
            return found;
        }
        let Ok((parent, name)) = split_path(path) else {
            return found;
        };
        let (Ok(dir), Ok(key)) = (self.store.lookup(&parent), name_key(&name)) else {
            return found;
        };
        match self
            .pending_creates
            .get(&(dir.id, key))
            .and_then(|entity| self.sessions.get(entity))
        {
            Some(session) => Ok(session.entry.clone()),
            None => found,
        }
    }
    /// The entries the mount shows in `directory`: [`Store::children`] and
    /// the deferred creates in it.
    pub fn children_visible(&self, directory: &str) -> Result<Vec<Entry>> {
        let mut children = self.store.children(directory)?;
        children.extend(
            self.pending_creates
                .range((directory.to_owned(), String::new())..)
                .take_while(|((parent, _), _)| parent == directory)
                .filter_map(|(_, entity)| self.sessions.get(entity))
                .map(|session| session.entry.clone()),
        );
        Ok(children)
    }
    /// Whether a deferred create not committed yet is in `directory`.
    fn has_deferred_children(&self, directory: &str) -> bool {
        self.pending_creates
            .range((directory.to_owned(), String::new())..)
            .next()
            .is_some_and(|((parent, _), _)| parent == directory)
    }
    /// The size the mount shows for `entry` (lookups, directory listings):
    /// an unsaved session's bytes (a deferred create, a file being written,
    /// a closed file still being saved in the background), else its
    /// committed object's size. The times shown stay the committed ones
    /// until the save commits (its write time is the commit's).
    pub fn visible_size(&self, entry: &Entry) -> Result<usize> {
        if let Some(session) = self.sessions.get(&entry.id).filter(|s| s.dirty) {
            return Ok(session.bytes.len());
        }
        Ok(entry
            .content
            .as_ref()
            .map(|object| self.store.object_size(object))
            .transpose()?
            .map_or(0, |size| size as usize))
    }
    /// Whether a write or truncate through handle `h` must wait: a save of
    /// its session runs and reads the staging file (module docs, *Saves off
    /// the engine lock*). `Some(generation)`: wait for [`SaveSignal`] to move
    /// past it, without the engine lock, then try again.
    pub fn write_blocked(&self, h: u64) -> Option<u64> {
        let session = self.session(h).ok()?;
        (session.saving && session.bytes.is_file()).then(|| self.saves.generation())
    }
    /// The signal saves move when they end ([`Engine::finish_save`]).
    pub fn save_signal(&self) -> Arc<SaveSignal> {
        self.saves.clone()
    }
    pub fn retry_pending(&mut self) -> Result<()> {
        let entities: Vec<_> = self
            .sessions
            .iter()
            .filter(|(_, s)| s.dirty && s.references == 0 && !s.saving)
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
    /// others are still tried. Returns how many saves changed the store's
    /// content for the status (the maintenance thread then marks the status
    /// content wanted, `status_cell::note_mutation`): those it committed, those
    /// it handed to the background savers (counted at once: by the time such
    /// a save commits nothing may be left for the next tick to retry), and
    /// those the savers committed for it since its previous call (reported
    /// again, so a status refresh that ran before that commit is followed by
    /// another one).
    pub fn retry_pending_due(&mut self, now: Instant) -> usize {
        self.retry_unrecovered_due(now);
        self.retry_state
            .retain(|entity, _| self.sessions.get(entity).is_some_and(|s| s.dirty));
        self.retry_handed
            .retain(|entity| self.sessions.get(entity).is_some_and(|s| s.dirty));
        let due: Vec<_> = self
            .sessions
            .iter()
            .filter(|(_, s)| s.dirty && s.references == 0 && !s.saving)
            .map(|(id, _)| id.clone())
            .filter(|id| self.retry_state.get(id).is_none_or(|s| s.next_at <= now))
            .collect();
        let mut committed = std::mem::take(&mut self.retry_commits);
        for entity in due {
            // Saved without the engine lock when the shared engine is known:
            // a saver saves it and reports a failure back (`retry_failed`).
            // With the queue full the savers are busy and their sweep
            // (`orphan_dirty`) picks it up: never saved here then, under the
            // lock (a backlog of closed files saved one after the other here
            // held the engine for seconds). With no saver at all (none could
            // be started) it is saved here.
            if let Some(shared) = self.shared.as_ref().and_then(std::sync::Weak::upgrade) {
                if schedule(&shared, entity.clone()) {
                    if let Some(session) = self.sessions.get_mut(&entity) {
                        session.queued = true;
                    }
                    // Counted once (it is due again while it waits).
                    if self.retry_handed.insert(entity) {
                        committed += 1;
                    }
                    continue;
                }
                if saver_threads() > 0 {
                    continue;
                }
            }
            match self.flush_entity(&entity) {
                Ok(()) => {
                    self.sessions.remove(&entity);
                    self.retry_state.remove(&entity);
                    committed += 1;
                }
                Err(error) => self.retry_failed(&entity, now, &error),
            }
        }
        committed
    }
    /// Records a failed pending-save retry of `entity` (its backoff grows).
    fn retry_failed(&mut self, entity: &str, now: Instant, error: &anyhow::Error) {
        let state = self
            .retry_state
            .entry(entity.to_owned())
            .or_insert(RetryState {
                failures: 0,
                next_at: now,
            });
        state.failures = state.failures.saturating_add(1);
        let delay = retry_delay(state.failures);
        state.next_at = now + delay;
        crate::util::log::event!(warn, PENDING_SAVE, {
            "site": "engine.retry_pending",
            "entity": entity,
            "failures": state.failures,
            "retry_in_ms": delay.as_millis() as u64,
            "message": format!("{error:#}"),
        });
    }
    /// Makes `engine` known to itself (the mount adapters' `bind`): the
    /// pending-save retry then saves staging files without the engine lock.
    pub fn adopt_shared(engine: &Shared) {
        super::lock_engine(engine).shared = Some(Arc::downgrade(engine));
    }
    /// Tries again to restore the due records that could not be restored
    /// (module docs): a staging file a scanner held at startup, a record of
    /// a branch that is active again. A restored one becomes a dirty session
    /// that [`Engine::retry_pending_due`] commits. A record of an entity with
    /// a session (the file is open) waits for it to go, and one of a branch
    /// that is not active waits for a checkout of it.
    fn retry_unrecovered_due(&mut self, now: Instant) {
        let due: Vec<String> = self
            .unrecovered
            .iter()
            .filter(|(entity, unrecovered)| {
                unrecovered.next_at <= now
                    && !self.sessions.contains_key(*entity)
                    && unrecovered
                        .branch
                        .as_ref()
                        .is_none_or(|branch| *branch == self.store.active)
            })
            .map(|(entity, _)| entity.clone())
            .collect();
        if due.is_empty() {
            return;
        }
        let records: BTreeMap<String, String> = match self.pending_records() {
            Ok(records) => records.into_iter().collect(),
            Err(_) => return,
        };
        for entity in due {
            let Some(record) = records.get(&entity) else {
                // The row is gone (discarded, or superseded elsewhere).
                self.unrecovered.remove(&entity);
                continue;
            };
            match self.restore_pending(record) {
                Ok(()) => {
                    self.unrecovered.remove(&entity);
                    crate::util::log::event!(info, PENDING_SAVE, {
                        "site": "engine.recover_pending",
                        "entity": entity,
                        "detail": "an unsaved file that could not be restored before was restored",
                    });
                    if self.health.is_none() {
                        self.health = Some("PENDING_SAVES: recovery in progress".into());
                        self.recovery_health = true;
                    }
                }
                Err(error) => self.note_unrecovered(&entity, record, &error, now),
            }
        }
        self.refresh_vitals();
    }
    /// Whether `record` belongs to a branch other than the active one (a
    /// record that does not parse might be the active branch's: `false`).
    fn of_other_branch(&self, record: &str) -> bool {
        record_branch(record).is_some_and(|branch| branch != self.store.active)
    }
    /// Refuses a checkout while a `pending_saves` record that could belong to
    /// the active branch is not restored (module docs; WP4.INT confirmation
    /// review): after the checkout it could not be restored, and before this
    /// a later save of the same file on the other branch superseded it, so its
    /// unsaved bytes were lost. A restored record is a dirty session, which
    /// `quiet()` refuses already; records of other branches do not block.
    pub fn checkout_allowed(&self) -> Result<()> {
        // Files closed through the mount and still being saved: a checkout
        // waits for them (WP5.2; `quiet` refuses them first).
        let unsaved = self.sessions.values().filter(|s| s.dirty).count();
        ensure!(
            unsaved == 0,
            "BUSY_VIEW: {unsaved} unsaved files are being saved; the checkout waits for them"
        );
        let blocking: Vec<String> = self
            .pending_records()?
            .into_iter()
            .filter(|(entity, record)| {
                !self.of_other_branch(record) && !self.sessions.get(entity).is_some_and(|s| s.dirty)
            })
            .map(|(entity, _)| entity)
            .collect();
        let Some(first) = blocking.first() else {
            return Ok(());
        };
        let count = blocking.len();
        let reason = self
            .unrecovered
            .get(first)
            .map_or("not restored yet", |unrecovered| unrecovered.error.as_str());
        bail!(
            "RECOVERY_REQUIRED: checkout refused: {count} unsaved file{} of this branch could not be \
             restored yet (entity {first}: {reason}) and would be left behind; they are retried and \
             saved on this branch once restored; `tkfs pending-saves` lists them and \
             `tkfs discard-pending-save ENTITY` drops one",
            if count == 1 { "" } else { "s" }
        )
    }
    /// After a checkout: every record not restored is due at once (those of
    /// the branch now active can be restored now).
    pub(crate) fn unrecovered_due_now(&mut self, now: Instant) {
        for unrecovered in self.unrecovered.values_mut() {
            unrecovered.failures = 0;
            unrecovered.next_at = now;
        }
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
            ensure!(self.lookup_visible(path).is_err(), "NAME_COLLISION");
            self.store.create_with_attributes(path, kind, attributes)?
        } else {
            self.lookup_visible(path)?
        };
        ensure!(
            create.is_some()
                || !writable
                || entry.kind == "directory"
                || entry.basic_info().attributes & 1 == 0,
            "ACCESS_DENIED: readonly file"
        );
        // An unsaved version of this file on another branch waits to be
        // restored there (module docs): no unsaved changes of this branch may
        // compete with it for its one `pending_saves` row.
        if writable
            && let Some(branch) = self
                .unrecovered
                .get(&entry.id)
                .and_then(|unrecovered| unrecovered.branch.as_ref())
                .filter(|branch| **branch != self.store.active)
        {
            bail!(
                "ACCESS_DENIED: an unsaved version of this file on branch {branch} waits to be \
                 restored (RECOVERY_BRANCH_MISMATCH); check out that branch to restore it, or \
                 `tkfs discard-pending-save {}` drops it",
                entry.id
            );
        }
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
                    edits: 0,
                    saving: false,
                    queued: false,
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
    /// [`Engine::session`], mutable.
    pub fn session_mut(&mut self, h: u64) -> Result<&mut Session> {
        let handle = self.handles.get(&h).context("INVALID_HANDLE")?;
        ensure!(handle.generation == self.store.generation, "STALE_VIEW");
        let entity = handle.entity.clone();
        self.sessions.get_mut(&entity).context("INVALID_HANDLE")
    }
    /// Removes handle `h` after an off-lock save ([`save_handle`]) without
    /// saving again (it would hold the lock for a save that failed a moment
    /// ago); the session goes when nothing keeps it. A dirty session stays
    /// (its failed save retained it; the pending-save retry commits it).
    pub fn release_handle(&mut self, h: u64) -> Result<()> {
        let handle = self.handles.remove(&h).context("INVALID_HANDLE")?;
        if let Some(s) = self.sessions.get_mut(&handle.entity) {
            s.references = s.references.saturating_sub(1);
            if s.references == 0 && !s.dirty && !s.saving {
                self.sessions.remove(&handle.entity);
            }
        }
        Ok(())
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
            let mut children = self.children_visible(&directory.id)?;
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
                let size = self.visible_size(&entry)?;
                cut.push((entry, size));
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
        // A save reads this staging file without the lock: the adapters wait
        // for it first (`lock_for_write`), so only a caller that did not gets
        // here, and is refused instead of tearing the save's snapshot.
        ensure!(
            !(session.saving && session.bytes.is_file()),
            "BUSY_VIEW: the file is being saved"
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
        session.edits += 1;
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
            // As in `write`: the adapters wait for a save of a staging file.
            ensure!(
                !(session.saving && session.bytes.is_file()),
                "BUSY_VIEW: the file is being saved"
            );
            session.edits += 1;
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
        // A deferred create keeps its basic information in its session until
        // its first save commits it (module docs, *Deferred creates*).
        let deferred = self.is_deferred(&entry.id);
        let current = if deferred {
            entry.clone()
        } else {
            self.store.entry(&entry.id)?
        };
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
        if deferred {
            // `Store::set_basic`'s checks, without its commit.
            basic.validate()?;
            ensure!(
                basic.attributes == BasicInfo::attributes(&entry.kind, basic.attributes)?,
                "INVALID_ATTRIBUTES"
            );
        } else {
            self.store.set_basic(&entry.id, basic)?;
        }
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
    /// Marks handle `h` cleaned up (the application closed it: no more
    /// writes through it) without saving its session; the mount adapters
    /// save it next, without the engine lock ([`save_handle_in_background`]).
    pub fn cleanup_unsaved(&mut self, h: u64) -> Result<()> {
        self.handles.get_mut(&h).context("INVALID_HANDLE")?.cleaned = true;
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
            // `Store::delete` sees only committed children; a file created
            // in the directory and not committed yet (module docs, *Deferred
            // creates*) keeps it too, or its first save would commit it into
            // a deleted directory.
            ensure!(!self.has_deferred_children(&entity), "DIRECTORY_NOT_EMPTY");
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
    /// No open handle, no unsaved session and no pending-save health
    /// (`PENDING_SAVES`/`PENDING_SAVE`, set only while sessions are unsaved).
    /// A `pending_saves` record that could not be restored does not count
    /// (module docs): its row stays and is retried; only a checkout waits for
    /// it ([`Engine::checkout_allowed`]).
    /// Files closed through the mount and still being saved in the
    /// background ([`Engine::settling`]) are unsaved files too: the stop, the
    /// checkout and the control requests that need a quiet engine are refused
    /// (`BUSY_VIEW`, retried) until they are committed, and a fork or a delete
    /// never runs ahead of them. Saves are made in the background only during
    /// a burst of closes ([`write_behind`]), so a lone close is committed
    /// when it returns, as before.
    pub fn quiet(&self) -> Result<()> {
        let open = self.handles.len();
        let dirty = self.sessions.values().filter(|s| s.dirty).count();
        if open == 0 && dirty == 0 {
            if let Some(health) = &self.health {
                bail!("BUSY_VIEW: {health}");
            }
            return Ok(());
        }
        bail!(
            "BUSY_VIEW: {open} open contexts, {dirty} unsaved files; {}",
            self.health.as_deref().unwrap_or("healthy")
        )
    }
    /// Dirty sessions whose save runs or a background saver has queued and
    /// that the application no longer writes (every handle of it is cleaned
    /// up, a WinFsp handle that only awaits its `Close` too, or none is
    /// left): files closed through the mount, being saved.
    pub fn settling(&self) -> usize {
        self.sessions
            .iter()
            .filter(|(entity, s)| {
                s.dirty
                    && (s.saving || s.queued)
                    && !self
                        .handles
                        .values()
                        .any(|h| &h.entity == *entity && !h.cleaned)
            })
            .count()
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

/// How [`Engine::begin_save`] answered.
pub enum SaveStart {
    /// The session is clean: nothing to save.
    Clean,
    /// A save of the entity runs: wait until [`SaveSignal`] moves past this
    /// generation (without the engine lock), then begin again.
    Busy(u64),
    /// Phase 1 is done: store the snapshot ([`store_snapshot`], without the
    /// engine lock), then [`Engine::finish_save`].
    Started(SaveJob),
}

/// A save between its snapshot (phase 1) and its commit (phase 3); see the
/// module docs, *Saves off the engine lock*.
pub struct SaveJob {
    entity: String,
    edits: u64,
    content_base: Option<String>,
    alive_base: Option<String>,
    branch: String,
    generation: u64,
    bytes: crate::staging::StageSnapshot,
    root: std::path::PathBuf,
    notes: Arc<AvailabilityNotes>,
}
impl SaveJob {
    /// The entity being saved.
    pub fn entity(&self) -> &str {
        &self.entity
    }
    /// The size of the snapshot being saved.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

/// The object phase 2 of a save stored ([`store_snapshot`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredObject {
    pub object: String,
    pub size: u64,
}

/// Moves whenever a save ends ([`Engine::finish_save`]), always under the
/// engine lock; a writer or a save that must wait for a running save reads
/// the generation under the engine lock, releases it, and waits for the
/// generation to move ([`SaveSignal::wait_past`]).
#[derive(Debug, Default)]
pub struct SaveSignal {
    generation: Mutex<u64>,
    changed: std::sync::Condvar,
}
impl SaveSignal {
    pub fn generation(&self) -> u64 {
        *crate::util::lock::lock(&self.generation)
    }
    fn bump(&self) {
        *crate::util::lock::lock(&self.generation) += 1;
        self.changed.notify_all();
    }
    /// Waits until the generation moved past `seen`, at most `timeout`;
    /// whether it moved.
    pub fn wait_past(&self, seen: u64, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut generation = crate::util::lock::lock(&self.generation);
        while *generation == seen {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            generation = self
                .changed
                .wait_timeout(generation, deadline - now)
                .map(|(guard, _)| guard)
                .unwrap_or_else(|poison| poison.into_inner().0);
        }
        true
    }
}

/// The availability-index rows of objects saved off the engine lock,
/// written on a connection of their own (module docs): a write through the
/// engine's connection outside a commit would make the engine's projection
/// view stale, and its next operation would reload the whole projection
/// (about 10 ms per save at 2K files; it was most of a small file's save
/// before WP5.2). Another connection's commit that leaves the projection
/// tables alone keeps the view (`Store::adopt_foreign_commits`).
///
/// The rows are written in batches, one transaction every
/// [`NOTES_INTERVAL`], by a thread that runs only while rows wait:
/// one row per transaction made the saves of a burst of small files wait on
/// each other's commits (SQLite's busy handler sleeps). Best effort, like
/// the index itself: until its row is written an object counts as present by
/// the listing of `objects/` (never as corrupt), and one whose row a crash
/// lost is verified again at the next startup.
pub struct AvailabilityNotes {
    root: std::path::PathBuf,
    repo: String,
    device: String,
    state: Mutex<NotesState>,
}
#[derive(Default)]
struct NotesState {
    /// `(object, size, mtime_ns, verified_ms)` not yet written.
    queue: Vec<(String, u64, i64, u64)>,
    /// A flushing thread runs.
    flusher: bool,
}
/// How long noted rows wait before the flushing thread writes them. Every
/// batch is a commit of another connection: an engine commit that overlaps
/// one cannot patch its projection view and drops it (the next operation
/// reloads the whole projection, about 25 ms at 2K files), so batches are
/// rare.
pub const NOTES_INTERVAL: Duration = Duration::from_secs(1);
impl AvailabilityNotes {
    fn new(store: &Store) -> Self {
        Self {
            root: store.root.clone(),
            repo: store.repo.clone(),
            device: store.device.clone(),
            state: Mutex::new(NotesState::default()),
        }
    }
    /// Records `object` (`size` bytes, mtime `mtime_ns`) as available and
    /// verified now, in the next batch.
    fn note(self: &Arc<Self>, object: &str, size: u64, mtime_ns: i64) {
        let mut state = crate::util::lock::lock(&self.state);
        state
            .queue
            .push((object.to_owned(), size, mtime_ns, time()));
        if state.flusher {
            return;
        }
        let notes = self.clone();
        let spawned = std::thread::Builder::new()
            .name("availability-notes".into())
            .spawn(move || notes.flush_loop());
        state.flusher = spawned.is_ok();
        if !state.flusher {
            // No thread: written now (the rows are best effort either way).
            let batch = std::mem::take(&mut state.queue);
            drop(state);
            self.write(batch);
        }
    }
    /// Rows noted and not yet written.
    pub fn queued(&self) -> usize {
        crate::util::lock::lock(&self.state).queue.len()
    }
    /// Writes every queued row now.
    pub fn flush(&self) {
        let batch = std::mem::take(&mut crate::util::lock::lock(&self.state).queue);
        self.write(batch);
    }
    fn flush_loop(&self) {
        loop {
            std::thread::sleep(NOTES_INTERVAL);
            let batch = {
                let mut state = crate::util::lock::lock(&self.state);
                if state.queue.is_empty() {
                    state.flusher = false;
                    return;
                }
                std::mem::take(&mut state.queue)
            };
            self.write(batch);
        }
    }
    /// One transaction on a connection opened for it (none stays open).
    fn write(&self, batch: Vec<(String, u64, i64, u64)>) {
        if batch.is_empty() {
            return;
        }
        let written = (|| -> Result<()> {
            crate::util::fault::trigger("engine.save.notes")?;
            let store = Store::open_secondary(&self.root, &self.repo, &self.device)?;
            store.db.execute_batch("BEGIN IMMEDIATE")?;
            for (object, size, mtime_ns, verified_ms) in &batch {
                store.note_available(object, *size, *mtime_ns, *verified_ms);
            }
            store.db.execute_batch("COMMIT")?;
            Ok(())
        })();
        if let Err(error) = written {
            crate::util::log::event!(debug, OBJECT_IO_ERROR, {
                "site": "engine.save.notes",
                "rows": batch.len(),
                "message": format!("{error:#}"),
            });
        }
    }
}
impl Drop for AvailabilityNotes {
    fn drop(&mut self) {
        self.flush();
    }
}

/// Phase 2 of a save, without the engine lock: hashes the snapshot, writes it
/// to a temporary file in `objects/`, makes it durable and installs it under
/// its hash (invariant 1: whole-object SHA-256, `fsync`, write-through
/// rename), or verifies the installed copy when the object exists already.
/// Then records its availability ([`AvailabilityNotes`]). A crash at any
/// step leaves at most an unreferenced object or a `.part` file (removed at
/// the next startup, `Engine::remove_orphaned_spools`); the session and the
/// store are as before the save until its commit (phase 3).
pub fn store_snapshot(job: &SaveJob) -> Result<StoredObject> {
    use crate::staging::StageSnapshot;
    crate::util::fault::trigger("engine.save.store")?;
    let objects = job.root.join("objects");
    let (stored, mtime) = match &job.bytes {
        StageSnapshot::Object { object, size } => {
            return Ok(StoredObject {
                object: object.clone(),
                size: *size as u64,
            });
        }
        StageSnapshot::Bytes(bytes) => {
            let object = hash(bytes);
            let size = bytes.len() as u64;
            match verified_existing(&objects, &object, size)? {
                Some(mtime) => (StoredObject { object, size }, mtime),
                None => {
                    let mtime = install_new(&objects, &object, |file| {
                        file.write_all(bytes)?;
                        Ok(())
                    })?;
                    (StoredObject { object, size }, mtime)
                }
            }
        }
        StageSnapshot::File { file, size } => store_file(&objects, file, *size)?,
    };
    job.notes.note(&stored.object, stored.size, mtime);
    Ok(stored)
}

/// The mtime (read before hashing) of the installed copy of `object` when it
/// exists and hashes to it; `None` when there is none; `CORRUPT_OBJECT`
/// ([`CorruptCopy`]) when the installed bytes do not hash to it (as
/// `Store::put_object` refuses).
fn verified_existing(objects: &std::path::Path, object: &str, size: u64) -> Result<Option<i64>> {
    let path = objects.join(object);
    let Ok(before) = std::fs::metadata(&path) else {
        return Ok(None);
    };
    let mut file = std::fs::File::open(&path).map_err(anyhow::Error::from)?;
    if before.len() != size || crate::core::stream_hash(&mut file)? != object {
        return Err(CorruptCopy {
            object: object.to_owned(),
        }
        .into());
    }
    Ok(Some(crate::core::objects::mtime_ns(&before)))
}

/// Phase 2 of a save found the installed copy of `object` corrupt (its bytes
/// do not hash to it): `CORRUPT_OBJECT`. Phase 3 records it in the store's
/// index ([`Engine::finish_save`]), as `Store::put_object`'s dedupe does.
#[derive(Debug)]
pub struct CorruptCopy {
    pub object: String,
}
impl std::fmt::Display for CorruptCopy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CORRUPT_OBJECT: {}", self.object)
    }
}
impl std::error::Error for CorruptCopy {}

/// Writes a temporary file in `objects/` with `write`, makes it durable and
/// installs it as `object`; returns its mtime. When the object appears
/// meanwhile (another save of the same bytes), the installed copy is
/// verified and the temporary file removed.
fn install_new(
    objects: &std::path::Path,
    object: &str,
    write: impl FnOnce(&mut std::fs::File) -> Result<()>,
) -> Result<i64> {
    let temp = objects.join(format!("{}.part", id()));
    let dest = objects.join(object);
    let result = (|| -> Result<i64> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        write(&mut file)?;
        file.sync_all()?;
        let mtime = crate::core::objects::stamp(&file)?;
        drop(file);
        // The legacy crash points of `Store::put_object` (`TKFS_FAULT`).
        crate::core::fault("object_flushed");
        crate::util::fault::trigger("engine.save.flushed")?;
        match crate::core::objects::install_object(&temp, &dest) {
            Ok(()) => {}
            Err(error) => {
                let size = std::fs::metadata(&temp)?.len();
                match verified_existing(objects, object, size)? {
                    Some(mtime) => {
                        std::fs::remove_file(&temp)?;
                        return Ok(mtime);
                    }
                    None => return Err(error),
                }
            }
        }
        crate::core::fault("object_installed");
        crate::util::fault::trigger("engine.save.installed")?;
        Ok(mtime)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

/// Phase 2 for a staging file: streams it (positional reads, the session's
/// handle keeps its own position) into a new object, hashing it on the way.
fn store_file(
    objects: &std::path::Path,
    source: &std::fs::File,
    size: usize,
) -> Result<(StoredObject, i64)> {
    ensure!(size as u64 <= MAX_CONTENT, "FILE_TOO_LARGE");
    let temp_name = format!("{}.part", id());
    let temp = objects.join(&temp_name);
    let result = (|| -> Result<(StoredObject, i64)> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        let mut digest = <sha2::Sha256 as sha2::Digest>::new();
        let mut buffer = vec![0; IO_CHUNK.min(size.max(1))];
        let mut offset = 0usize;
        while offset < size {
            let n = buffer.len().min(size - offset);
            crate::staging::read_file_at(source, offset as u64, &mut buffer[..n])?;
            sha2::Digest::update(&mut digest, &buffer[..n]);
            file.write_all(&buffer[..n])?;
            offset += n;
        }
        let object = hex::encode(sha2::Digest::finalize(digest));
        let stored = StoredObject {
            object: object.clone(),
            size: size as u64,
        };
        if let Some(mtime) = verified_existing(objects, &object, size as u64)? {
            drop(file);
            std::fs::remove_file(&temp)?;
            return Ok((stored, mtime));
        }
        file.sync_all()?;
        let mtime = crate::core::objects::stamp(&file)?;
        drop(file);
        crate::core::fault("object_flushed");
        crate::util::fault::trigger("engine.save.flushed")?;
        let dest = objects.join(&object);
        if let Err(error) = crate::core::objects::install_object(&temp, &dest) {
            match verified_existing(objects, &object, size as u64)? {
                Some(mtime) => {
                    std::fs::remove_file(&temp)?;
                    return Ok((stored, mtime));
                }
                None => return Err(error),
            }
        }
        crate::core::fault("object_installed");
        crate::util::fault::trigger("engine.save.installed")?;
        Ok((stored, mtime))
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

/// Saves `entity`'s dirty session without holding the engine lock while its
/// bytes are hashed and stored (CCR-WP4.INT-5; module docs): phase 1 under
/// the lock, phase 2 without it, phase 3 under it. Waits (without the lock)
/// for a save of the same entity that runs. A panic in phase 2 still ends
/// the save (the session is retained).
pub fn save_entity(engine: &Shared, entity: &str) -> Result<()> {
    save_entity_as(engine, entity, false)
}

/// [`save_entity`]; `background` (the background savers): before each
/// phase that takes the engine lock, give it to the filesystem callbacks in
/// progress first ([`foreground`], at most [`YIELD_MAX`] each time); commit
/// [`SETTLE`] after the bytes are stored; and leave an entity a save of
/// which runs to that save (edits made since stay dirty for the savers'
/// sweep, [`orphan_dirty`]).
fn save_entity_as(engine: &Shared, entity: &str, background: bool) -> Result<()> {
    loop {
        if background {
            yield_to_foreground();
        }
        let mut e = super::lock_engine(engine);
        if !e.sessions.contains_key(entity) {
            // Saved and closed meanwhile (a background save ended it).
            return Ok(());
        }
        let job = match e.begin_save(entity)? {
            SaveStart::Clean => return Ok(()),
            SaveStart::Busy(_) if background => return Ok(()),
            SaveStart::Busy(seen) => {
                let signal = e.save_signal();
                drop(e);
                signal.wait_past(seen, SAVE_WAIT_SLICE);
                continue;
            }
            SaveStart::Started(job) => job,
        };
        drop(e);
        let stored =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| store_snapshot(&job)))
                .unwrap_or_else(|_| Err(anyhow::anyhow!("UNKNOWN_ERROR: storing a save panicked")));
        if background {
            std::thread::sleep(SETTLE);
            yield_to_foreground();
        }
        return super::lock_engine(engine).finish_save(job, stored);
    }
}

/// [`save_entity`] for the session of handle `h`; `h == 0`: every dirty
/// session (a volume flush). An unknown handle is left to the caller.
pub fn save_handle(engine: &Shared, h: u64) -> Result<()> {
    let entities: Vec<String> = {
        let e = super::lock_engine(engine);
        if h == 0 {
            e.sessions
                .iter()
                .filter(|(_, s)| s.dirty)
                .map(|(entity, _)| entity.clone())
                .collect()
        } else {
            match e.handles.get(&h) {
                Some(handle) if e.sessions.get(&handle.entity).is_some_and(|s| s.dirty) => {
                    vec![handle.entity.clone()]
                }
                _ => vec![],
            }
        }
    };
    let mut first = Ok(());
    for entity in entities {
        let saved = save_entity(engine, &entity);
        if first.is_ok() {
            first = saved;
        }
    }
    first
}

/// Waits (without the engine lock) until a write through handle `h` no
/// longer waits for a save ([`Engine::write_blocked`]), then returns the
/// engine locked. Bounded only by the saves themselves.
pub fn lock_for_write(engine: &Shared, h: u64) -> super::EngineGuard<'_> {
    loop {
        let e = super::lock_engine(engine);
        let Some(seen) = e.write_blocked(h) else {
            return e;
        };
        let signal = e.save_signal();
        drop(e);
        signal.wait_past(seen, SAVE_WAIT_SLICE);
    }
}

/// Waits (without the engine lock) until no save runs of handle `source`'s
/// file nor of the entry at `target` (a rename's target, a removed entry's
/// path), then returns the engine locked, for a namespace operation that
/// deletes, renames or replaces them: a delete on close, a rename, an
/// unlink. Under that lock no save of them can begin, so a save never
/// commits its content (and `alive`) after the delete, which would bring
/// the file back or make a conflict of it, and a deferred create is
/// committed by the caller's inline save ([`Engine::flush`],
/// [`Engine::commit_created_at`]) before its name changes (module docs,
/// *Deferred creates*). The caller settled them first ([`settle_entity`]);
/// this closes the window in which another close starts a save after that.
/// Bounded only by the saves themselves.
pub fn lock_settled<'a>(
    engine: &'a Shared,
    source: Option<u64>,
    target: Option<&str>,
) -> super::EngineGuard<'a> {
    loop {
        let e = super::lock_engine(engine);
        let saving = |entity: &str| e.sessions.get(entity).is_some_and(|s| s.saving);
        let busy = source
            .and_then(|h| e.handles.get(&h))
            .is_some_and(|handle| saving(&handle.entity))
            || target
                .and_then(|path| e.lookup_visible(path).ok())
                .is_some_and(|entry| saving(&entry.id));
        if !busy {
            return e;
        }
        let seen = e.saves.generation();
        let signal = e.save_signal();
        drop(e);
        signal.wait_past(seen, SAVE_WAIT_SLICE);
    }
}

/// The longest single wait for a save's end before the waiter looks again.
const SAVE_WAIT_SLICE: Duration = Duration::from_millis(100);

/// Background saver threads at most, in this process
/// ([`save_handle_in_background`], a close, the pending-save retry).
pub const MAX_BACKGROUND_SAVES: usize = 8;
/// Closed files waiting for a background saver at most. With the queue full
/// a close saves its file before it returns, as a lone close does
/// (backpressure: [`save_handle_in_background`], [`close_handle`]), so the
/// files a close has returned for and that are not committed yet are the
/// queued ones and those the savers are saving: at most
/// `MAX_QUEUED_SAVES + MAX_BACKGROUND_SAVES`. That bounds what a hard crash
/// (power loss, a killed process) can lose of closed files (a failed save is
/// retained in `pending_saves`, which a crash does not lose).
pub const MAX_QUEUED_SAVES: usize = 64;
/// A background save commits at least this long after it stored the bytes:
/// an on-access scanner scans the object file just written, and the
/// commit's verification of newly authored bytes (`check_commit`) waited
/// for that scan under the engine lock (measured on the reference machine:
/// about 3 ms of a 32 KiB file's 5.5 ms commit). Waiting before taking the
/// lock costs only latency.
pub const SETTLE: Duration = Duration::from_millis(10);
/// How long an idle background saver thread waits for work before it ends.
const SAVER_IDLE: Duration = Duration::from_secs(1);

/// The background savers: a queue of `(engine, entity)` and up to
/// [`MAX_BACKGROUND_SAVES`] threads that serve it (started as work comes,
/// ending after [`SAVER_IDLE`]).
struct Savers {
    state: Mutex<SaversState>,
    wake: std::sync::Condvar,
}
#[derive(Default)]
struct SaversState {
    jobs: std::collections::VecDeque<(Shared, String)>,
    threads: usize,
    idle: usize,
    running: usize,
}
fn savers() -> &'static Savers {
    static SAVERS: std::sync::OnceLock<Savers> = std::sync::OnceLock::new();
    SAVERS.get_or_init(|| Savers {
        state: Mutex::new(SaversState::default()),
        wake: std::sync::Condvar::new(),
    })
}

/// Whether the mount defers the commit of a file's creation to its first save
/// (module docs, *Deferred creates*); `TKFS_DEFERRED_CREATE=0` turns it off.
pub fn deferred_creates() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("TKFS_DEFERRED_CREATE").as_deref() != Ok("0"))
}

/// How long after a save ended the next close still counts as part of a
/// burst of closes ([`write_behind`]).
pub const BURST_WINDOW: Duration = Duration::from_millis(100);

/// Milliseconds after this process's first save at which the latest save
/// ended (0: none yet).
static LAST_SAVE_END: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn process_ms() -> u64 {
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    u64::try_from(START.get_or_init(Instant::now).elapsed().as_millis())
        .unwrap_or(u64::MAX)
        .saturating_add(1)
}

fn note_save_end() {
    LAST_SAVE_END.store(process_ms(), Ordering::Relaxed);
}

/// Whether a close should leave its save to the background savers (WP5.2):
/// during a burst of closes, that is while background saves run or wait, or
/// within [`BURST_WINDOW`] of the end of a save. A lone close is saved before
/// it returns, as before, so a command that follows it (a stop, a fork, a
/// delete) never waits for it; in a burst (a copy, a build) the saves overlap
/// the application's next files, and what follows the burst may find files
/// still being saved for a few milliseconds (refused `BUSY_VIEW`, retried).
/// A close in a burst that finds the savers' queue full saves before it
/// returns, like a lone close ([`save_handle_in_background`]).
pub fn write_behind() -> bool {
    if background_saves() > 0 {
        return true;
    }
    let last = LAST_SAVE_END.load(Ordering::Relaxed);
    last != 0 && process_ms().saturating_sub(last) < BURST_WINDOW.as_millis() as u64
}

/// Background saver threads alive now.
pub fn saver_threads() -> usize {
    crate::util::lock::lock(&savers().state).threads
}

/// Saves running or waiting in the background now.
pub fn background_saves() -> usize {
    let state = crate::util::lock::lock(&savers().state);
    state.jobs.len() + state.running
}

/// Queues the save of `entity` for a background saver; `false` when the
/// queue is full: a close then saves the file itself ([`queue_or_save`]),
/// the pending-save retry leaves it to the savers' sweep ([`orphan_dirty`]).
/// Also `false` when no saver runs and none can be started (the thread
/// could not be created): nothing would serve the queue, so the caller
/// saves the file itself (a close without the engine lock, the pending-save
/// retry under it). A save of `entity` queued already counts as queued, and
/// a saver is started for it too when none is idle (one whose start failed
/// before is tried again).
fn schedule(engine: &Shared, entity: String) -> bool {
    let savers = savers();
    let mut state = crate::util::lock::lock(&savers.state);
    let queued = state
        .jobs
        .iter()
        .any(|(queued, name)| Arc::ptr_eq(queued, engine) && *name == entity);
    if !queued {
        if state.jobs.len() >= MAX_QUEUED_SAVES {
            return false;
        }
        state.jobs.push_back((engine.clone(), entity));
    }
    if state.idle == 0 && state.threads < MAX_BACKGROUND_SAVES && spawn_saver() {
        state.threads += 1;
    }
    if state.threads == 0 {
        // Not even one saver: the job just queued goes again (one queued
        // before stays: a saver started later finds it saved, a no-op).
        if !queued {
            state.jobs.pop_back();
        }
        return false;
    }
    drop(state);
    savers.wake.notify_one();
    true
}

/// Starts one background saver thread; `false` when it could not be
/// created (fault point `engine.saver.spawn`: the qualification of that).
fn spawn_saver() -> bool {
    crate::util::fault::fail("engine.saver.spawn").is_ok()
        && std::thread::Builder::new()
            .name("save".into())
            .spawn(saver_loop)
            .is_ok()
}

/// One background saver: serves the queue, then sweeps the closed files of
/// that engine left unsaved, until it is idle for [`SAVER_IDLE`].
fn saver_loop() {
    let savers = savers();
    loop {
        let (engine, entity) = {
            let mut state = crate::util::lock::lock(&savers.state);
            loop {
                if let Some(job) = state.jobs.pop_front() {
                    state.running += 1;
                    break job;
                }
                state.idle += 1;
                let (next, waited) = savers
                    .wake
                    .wait_timeout(state, SAVER_IDLE)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state = next;
                state.idle -= 1;
                if waited.timed_out() && state.jobs.is_empty() {
                    state.threads -= 1;
                    return;
                }
            }
        };
        // A panic (a poisoned engine under the test policy) ends this job,
        // never the saver or its counts.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut next = Some(entity);
            while let Some(entity) = next {
                if let Err(error) = save_entity_as(&engine, &entity, true) {
                    let wire = crate::model::errors::ErrorWire::from_error(&error);
                    crate::util::log::event!(warn, (wire.code.as_str()), {
                        "site": "engine.background_save",
                        "entity": entity,
                        "message": wire.message,
                    });
                    super::lock_engine(&engine).retry_failed(&entity, Instant::now(), &error);
                }
                next = orphan_dirty(&super::lock_engine(&engine), Instant::now());
            }
        }));
        drop(engine);
        crate::util::lock::lock(&savers.state).running -= 1;
    }
}

/// Filesystem callbacks in progress that take the engine lock for the
/// application ([`foreground`]); background saves let them go first.
static FOREGROUND: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
/// The longest a background save defers one engine-lock phase to
/// [`foreground`] callbacks.
pub const YIELD_MAX: Duration = Duration::from_millis(20);

/// Marks a filesystem callback that takes the engine lock for the
/// application until the returned guard drops (the mount adapters).
pub fn foreground() -> Foreground {
    FOREGROUND.fetch_add(1, Ordering::SeqCst);
    Foreground(())
}

/// Returned by [`foreground`].
pub struct Foreground(());
impl Drop for Foreground {
    fn drop(&mut self) {
        FOREGROUND.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Waits while [`foreground`] callbacks run, at most [`YIELD_MAX`].
fn yield_to_foreground() {
    let deadline = Instant::now() + YIELD_MAX;
    while FOREGROUND.load(Ordering::SeqCst) > 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_micros(200));
    }
}

/// The save of a closed file (WinFsp cleanup, FUSE flush): when the session
/// of handle `h` is dirty and no save of it runs, it is queued for a
/// background saver and this returns at once, so the application's close
/// does not wait for the hashing, copying and commits (module docs, *Saves
/// off the engine lock*). With the queue full ([`MAX_QUEUED_SAVES`]) the
/// file is saved here, without the engine lock, before this returns
/// (backpressure: an application closing files faster than the savers
/// commit them waits for its saves, and the closed files not committed yet
/// stay bounded). Until it is committed the session is a dirty session like
/// any other:
/// reads see its bytes, a graceful stop or detach waits for it (`quiet`), a
/// forced one or a poison exit retains it (invariant 8), and a flush
/// (`FlushFileBuffers`, `fsync`) waits for it ([`save_handle`]). A failed
/// background save is retained (`pending_saves`) and retried with the
/// pending-save backoff.
pub fn save_handle_in_background(engine: &Shared, h: u64) -> Result<()> {
    let entity = {
        let mut e = super::lock_engine(engine);
        let Some(entity) = e.handles.get(&h).map(|handle| handle.entity.clone()) else {
            return Ok(());
        };
        match e.sessions.get_mut(&entity) {
            Some(session) if session.dirty && !session.saving => session.queued = true,
            _ => return Ok(()),
        }
        entity
    };
    queue_or_save(engine, entity)
}

/// [`schedule`]s the save of `entity`, marked `queued` by the caller. With
/// the queue full the mark is cleared and the file is saved now, by the
/// caller, without the engine lock ([`save_entity`]; backpressure, see
/// [`MAX_QUEUED_SAVES`]).
fn queue_or_save(engine: &Shared, entity: String) -> Result<()> {
    if schedule(engine, entity.clone()) {
        return Ok(());
    }
    if let Some(session) = super::lock_engine(engine).sessions.get_mut(&entity) {
        session.queued = false;
    }
    save_entity(engine, &entity)
}

/// A dirty session no save covers and the application no longer writes:
/// every handle of it is cleaned up (closed), or none is left, and its
/// pending-save backoff (if any) is due.
fn orphan_dirty(e: &Engine, now: Instant) -> Option<String> {
    e.sessions
        .iter()
        .filter(|(_, s)| s.dirty && !s.saving)
        .map(|(entity, _)| entity)
        .find(|entity| {
            e.retry_state.get(*entity).is_none_or(|s| s.next_at <= now)
                && !e
                    .handles
                    .values()
                    .any(|handle| &handle.entity == *entity && !handle.cleaned)
        })
        .cloned()
}

/// Makes `entity`'s content final before a namespace operation removes or
/// replaces the entity (a delete, a rename over it): waits until no save of
/// it runs, and saves what a closed file of it still holds unsaved (no
/// handle writes it any more). A background save then never commits after
/// the delete, which would bring the file back or make a conflict of it; the
/// last bytes are committed first, as when a close saved before it returned.
/// A session that an open handle still writes is left alone (as before).
pub fn settle_entity(engine: &Shared, entity: &str) -> Result<()> {
    loop {
        let (seen, signal, orphan) = {
            let e = super::lock_engine(engine);
            let Some(session) = e.sessions.get(entity) else {
                return Ok(());
            };
            if !session.saving && e.is_deferred(entity) {
                // Its name must be in the store before anything replaces
                // or removes it: saved even while a handle writes it.
                drop(e);
                save_entity(engine, entity)?;
                continue;
            }
            if !session.saving {
                let writing = e
                    .handles
                    .values()
                    .any(|handle| handle.entity == entity && !handle.cleaned);
                if !session.dirty || writing {
                    return Ok(());
                }
            }
            (
                e.saves.generation(),
                e.save_signal(),
                !e.sessions[entity].saving,
            )
        };
        if orphan {
            save_entity(engine, entity)?;
        } else {
            signal.wait_past(seen, SAVE_WAIT_SLICE);
        }
    }
}

/// [`settle_entity`] for the entity at `path`, if one is there (a deferred
/// create too).
pub fn settle_path(engine: &Shared, path: &str) -> Result<()> {
    let entity = super::lock_engine(engine)
        .lookup_visible(path)
        .ok()
        .map(|entry| entry.id);
    match entity {
        Some(entity) => settle_entity(engine, &entity),
        None => Ok(()),
    }
}

/// [`settle_entity`] for the session of handle `h`.
pub fn settle_handle(engine: &Shared, h: u64) -> Result<()> {
    let entity = super::lock_engine(engine)
        .handles
        .get(&h)
        .map(|handle| handle.entity.clone());
    match entity {
        Some(entity) => settle_entity(engine, &entity),
        None => Ok(()),
    }
}

/// Closes handle `h` (WinFsp close, FUSE release): removes the handle and,
/// when its session is dirty, no save of it runs and its pending-save
/// backoff is due, queues its save for a background saver. The close saves
/// only when that queue is full (backpressure, [`MAX_QUEUED_SAVES`]): a
/// WinFsp close holds the application's `CloseHandle` until it answers
/// (measured: a save in it cost the application the whole save).
pub fn close_handle(engine: &Shared, h: u64) -> Result<()> {
    let unsaved = {
        let mut e = super::lock_engine(engine);
        let now = Instant::now();
        let unsaved = e
            .handles
            .get(&h)
            .and_then(|handle| e.sessions.get(&handle.entity).map(|s| (handle, s)))
            .filter(|(handle, s)| {
                s.dirty
                    && !s.saving
                    && e.retry_state
                        .get(&handle.entity)
                        .is_none_or(|state| state.next_at <= now)
            })
            .map(|(handle, _)| handle.entity.clone());
        e.release_handle(h)?;
        if let Some(session) = unsaved
            .as_ref()
            .and_then(|entity| e.sessions.get_mut(entity))
        {
            session.queued = true;
        }
        unsaved
    };
    match unsaved {
        Some(entity) => queue_or_save(engine, entity),
        None => Ok(()),
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
