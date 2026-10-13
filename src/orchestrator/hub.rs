//! Owned by WP1.11.
//!
//! The `StatusHub` behind [`StatusSink`] and [`StatusSource`] (REVAMP_PLAN
//! §3.4, §7): one cheap, versioned owner of everything the UI shows.
//!
//! * **Passive.** All state lives in one `Mutex<HubState>`. [`record`] applies
//!   an event in memory (map updates only: no I/O, no waiting, no durable
//!   write; p99 < 50 µs) and never blocks on a producer (invariant 30: status
//!   never blocks or writes).
//! * **Lazy snapshots.** [`snapshot`] returns an `Arc<StatusSnapshot>` that is
//!   rebuilt only when something changed, through a token bucket that allows
//!   4 rebuilds per second sustained (`policy::SNAPSHOT_MIN_INTERVAL`) and a
//!   burst of 2, so an isolated event is published at once while an event
//!   storm costs at most 4 rebuilds a second. [`wait_newer`] serves
//!   `subscribe` long polls.
//! * **Tick.** [`tick`] (the caller runs it at `HubConfig::interval`, 1 Hz,
//!   see [`StatusHub::spawn_ticker`]) folds the EWMA rates (10 s and 60 s),
//!   runs the stall detector, applies the time-based error display policy
//!   (Contention after 30 s) and expires stale inbound-incompatible notices.
//! * **Pure rules** live in [`super::hub_rules`].
//! * **Legacy projection.** [`legacy_folder_sync`] and [`legacy_peers`] build
//!   the §7.3 List/Inspect `sync` member and the Peers reply from the typed
//!   events, for use after the cutover (WP3.1).
//! * **Redaction.** No event carries a pairing code, and the legacy
//!   projection, which receives `PeerRecord`s, never serializes their `code`
//!   and scrubs it from any string it emits (invariant 21).
//!
//! State maps are keyed by folder state, by (repo, peer installation) pair,
//! by (peer, lane direction, lane) and by inbound session (peer, lane).
//!
//! [`record`]: StatusSink::record
//! [`snapshot`]: StatusSource::snapshot
//! [`wait_newer`]: StatusSource::wait_newer
//! [`tick`]: StatusHub::tick
//! [`legacy_folder_sync`]: StatusHub::legacy_folder_sync
//! [`legacy_peers`]: StatusHub::legacy_peers
use super::hub_rules::{
    self as rules, ErrorFlags, Ewma, FolderInputs, PeerInputs, StallInput, Streak, SummaryFacts,
    Work,
};
use crate::{
    model::{
        errors::{ErrorClass, ErrorRecord, ErrorWire, Scope, code_in},
        health::{ContentCounters, MountIntent, MountState, MountStatus, StoreHealth},
        ipc::WorkerHeartbeat,
        orchestrator::{
            ExitRecord, FolderPhase, FolderRecord, FolderView, GateState, PeerRecord,
            PublishedEntry, StatusSink, StatusSource, StopStage, limits,
        },
        protocol::{LaneRole, PROTOCOL_NAME, PROTOCOL_VERSION, feed::ExchangeSummary},
        status::{
            ActivationStatus, ActivityEntry, DaemonStatus, Direction, FolderPeerSync, FolderStatus,
            FolderSync, GateKind, GateStatus, InflightOp, Intents, LaneState, LaneStatus,
            LifecyclePhase, LifecycleStatus, MetaProgress, PeerConnState, PeerStatus,
            STATUS_SCHEMA, StartupProgress, StatusEvent, StatusSnapshot, StoreStatus,
            TransferProgress, policy,
        },
    },
    util::{cancel::Cancel, lock::lock, time::Clock},
};
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

/// Environment override of the stall threshold, in milliseconds (§7.1).
pub const STALL_AFTER_ENV: &str = "TKFS_STALL_AFTER_MS";
/// Open Object/Event errors kept per folder; older ones are dropped (they
/// stay in the worker's own `object_error_state` and inbox rows).
pub const MAX_ITEM_ERRORS: usize = 64;
/// `LegacyLink` passthrough values kept.
pub const MAX_LEGACY_LINKS: usize = 4096;
/// In-flight ops listed per lane (an `ExchangeStarted` never matched by its
/// `ExchangeFinished` cannot grow the list without bound).
pub const MAX_INFLIGHT: usize = 64;
/// Snapshot rebuild burst (see the module docs).
pub const SNAPSHOT_BURST: f64 = 2.0;
/// An inbound-incompatible notice is dropped after this long without a new
/// attempt from that address (the §8 Incompatible recheck period).
pub const INBOUND_INCOMPATIBLE_TTL: Duration = limits::INCOMPATIBLE_RECHECK;

/// Hub settings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HubConfig {
    /// Stall threshold: `max(60 s, 20 × sync_interval)` or `TKFS_STALL_AFTER_MS`.
    pub stall_after: Duration,
    /// Tick period of the caller's ticker (1 s); also the nominal elapsed
    /// time of the first tick.
    pub interval: Duration,
    /// Activity ring capacity (500).
    pub activity: usize,
}

impl HubConfig {
    /// The defaults for a daemon whose lanes exchange every `sync_interval`,
    /// honouring `TKFS_STALL_AFTER_MS`.
    pub fn for_sync_interval(sync_interval: Duration) -> Self {
        Self {
            stall_after: stall_after_for(
                sync_interval,
                std::env::var(STALL_AFTER_ENV).ok().as_deref(),
            ),
            interval: policy::TICK,
            activity: policy::ACTIVITY_RING,
        }
    }
}

impl Default for HubConfig {
    /// The defaults for the default 500 ms sync interval.
    fn default() -> Self {
        Self::for_sync_interval(Duration::from_millis(500))
    }
}

/// The stall threshold for `sync_interval`, or the override (milliseconds,
/// as `TKFS_STALL_AFTER_MS` gives it) when that parses as a positive number.
pub fn stall_after_for(sync_interval: Duration, override_ms: Option<&str>) -> Duration {
    override_ms
        .and_then(|text| text.trim().parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .map(Duration::from_millis)
        .unwrap_or_else(|| crate::model::status::stall_after(sync_interval))
}

/// The status owner (§3.4). Create it with [`StatusHub::new`], give it to
/// producers as `Arc<dyn StatusSink>` and to readers as `Arc<dyn StatusSource>`.
pub struct StatusHub {
    cfg: HubConfig,
    clock: Arc<dyn Clock>,
    state: Mutex<HubState>,
    changed: Condvar,
}

impl std::fmt::Debug for StatusHub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = lock(&self.state);
        f.debug_struct("StatusHub")
            .field("cfg", &self.cfg)
            .field("version", &state.version)
            .field("generation", &state.cached.generation)
            .field("folders", &state.folders.len())
            .field("peers", &state.peers.len())
            .finish_non_exhaustive()
    }
}

impl StatusHub {
    pub fn new(cfg: HubConfig, clock: Arc<dyn Clock>) -> Arc<StatusHub> {
        let now = clock.now_ms();
        let mut state = HubState::new(&cfg, now);
        state.cached = Arc::new(state.build(now, &cfg, 0));
        Arc::new(StatusHub {
            cfg,
            clock,
            state: Mutex::new(state),
            changed: Condvar::new(),
        })
    }

    pub fn config(&self) -> &HubConfig {
        &self.cfg
    }

    /// Sets the daemon identity shown in `StatusSnapshot.daemon` (its
    /// `errors` member is ignored: the hub owns it).
    pub fn set_daemon(&self, daemon: DaemonStatus) {
        self.mutate(|state, _, _| {
            state.daemon = DaemonStatus {
                errors: Vec::new(),
                ..daemon
            };
            true
        });
    }

    /// Marks the daemon as stopping (shutdown in progress).
    pub fn set_stopping(&self, stopping: bool) {
        self.mutate(|state, _, _| {
            let changed = state.daemon.stopping != stopping;
            state.daemon.stopping = stopping;
            changed
        });
    }

    /// One hub tick: rates, stall detection, time-based error visibility.
    /// The caller runs it every `HubConfig::interval`.
    pub fn tick(&self) {
        let instant = self.clock.instant();
        self.mutate(|state, now, cfg| {
            let elapsed = state
                .last_tick
                .map(|last| instant.saturating_duration_since(last))
                .unwrap_or(cfg.interval);
            state.last_tick = Some(instant);
            state.tick(now, elapsed, cfg)
        });
    }

    /// Spawns the `hub-tick` thread, which calls [`tick`](Self::tick) every
    /// `interval` until `cancel` fires.
    pub fn spawn_ticker(self: &Arc<Self>, cancel: Cancel) -> std::thread::JoinHandle<()> {
        let hub = Arc::clone(self);
        std::thread::Builder::new()
            .name("hub-tick".into())
            .spawn(move || {
                while !cancel.sleep(hub.cfg.interval) {
                    hub.tick();
                }
            })
            .expect("spawn hub-tick")
    }

    /// Rebuilds a snapshot no matter how many rebuilds happened recently.
    /// Readers use [`StatusSource::snapshot`]; this is for one-shot callers
    /// that must observe their own writes (and for tests).
    pub fn snapshot_now(&self) -> Arc<StatusSnapshot> {
        let mut state = lock(&self.state);
        if state.version != state.built_version {
            self.rebuild(&mut state);
        }
        state.cached.clone()
    }

    /// Sets (or clears) a peer's display name, the catalog's
    /// `peer_meta.display_name`, shown as `label` on its `PeerStatus` and
    /// `FolderPeerSync` rows. No status event carries it (CCR-WP1.11-1);
    /// setting a label never makes a peer appear in the status by itself.
    pub fn set_peer_label(&self, installation: &str, label: Option<String>) {
        self.mutate(|state, _, _| match label {
            Some(label) => {
                state.labels.insert(installation.to_owned(), label.clone()) != Some(label)
            }
            None => state.labels.remove(installation).is_some(),
        });
    }

    /// The `LegacyLink` passthrough values recorded so far (pre-cutover
    /// legacy `LinkStatus` JSON, keyed as the producer keyed it).
    pub fn legacy_links(&self) -> BTreeMap<String, Value> {
        lock(&self.state).legacy_links.clone()
    }

    /// How many snapshots have been built (diagnostics and tests).
    pub fn rebuilds(&self) -> u64 {
        lock(&self.state).rebuilds
    }

    /// The §7.3 `sync` member of List/Inspect for `folder` after the cutover:
    /// one entry per (outbound) peer in `peers`, in order, with the legacy
    /// keys `endpoint`, `connected`, `remote_available`, `error`, `metadata`
    /// and `lanes[{lane, role, connected, transport, parallel_bulk, error,
    /// repository}]`, where `metadata` and `repository` carry every legacy
    /// `RepositoryStatus` key.
    pub fn legacy_folder_sync(&self, folder: &FolderRecord, peers: &[PeerRecord]) -> Value {
        let state = lock(&self.state);
        let mut out = Vec::with_capacity(peers.len());
        for record in peers {
            let peer = state.find_peer(record);
            let pool = record.connections.max(limits::MIN_POOL);
            let lane0 = peer.and_then(|peer| peer.lanes.get(&LaneKey::outbound(0)));
            let connected = lane0.is_some_and(|lane| lane.state == LaneState::Ready);
            let remote_available = if connected {
                peer.and_then(|peer| peer.catalog.as_ref())
                    .map(|catalog| catalog.iter().any(|entry| entry.repo == folder.repo))
            } else {
                None
            };
            let lane_error =
                |lane: u32| peer.and_then(|peer| state.lane_error_text(&peer.installation, lane));
            let repository = |lane: u32| {
                peer.and_then(|peer| peer.lanes.get(&LaneKey::outbound(lane)))
                    .and_then(|entry| entry.repos.get(&folder.repo))
                    .map(|repo| legacy_repo_value(repo, lane, pool))
            };
            let lanes: Vec<Value> = (0..pool)
                .map(|lane| {
                    let entry = peer.and_then(|peer| peer.lanes.get(&LaneKey::outbound(lane)));
                    json!({
                        "lane": lane,
                        "role": legacy_role(lane),
                        "connected": entry.is_some_and(|entry| entry.state == LaneState::Ready),
                        "transport": transport(),
                        "parallel_bulk": true,
                        "error": lane_error(lane),
                        "repository": repository(lane),
                    })
                })
                .collect();
            out.push(json!({
                "endpoint": record.endpoint,
                "connected": connected,
                "remote_available": remote_available,
                "error": lane_error(0),
                "metadata": repository(0),
                "lanes": lanes,
            }));
        }
        let mut value = Value::Array(out);
        scrub_codes(&mut value, peers);
        value
    }

    /// The §7.3 Peers reply after the cutover: one entry per (outbound) peer
    /// in `peers`, `{endpoint, installation, connections, configured_connections,
    /// reserved_connections: 1, lanes: [{role, connected, exchanges,
    /// last_success_ms, error, transport, parallel_bulk, repositories}]}`.
    pub fn legacy_peers(&self, peers: &[PeerRecord]) -> Value {
        let state = lock(&self.state);
        let mut out = Vec::with_capacity(peers.len());
        for record in peers {
            let peer = state.find_peer(record);
            let pool = record.connections.max(limits::MIN_POOL);
            let lanes: Vec<Value> = (0..pool)
                .map(|lane| {
                    let entry = peer.and_then(|peer| peer.lanes.get(&LaneKey::outbound(lane)));
                    let repositories: Map<String, Value> = entry
                        .map(|entry| {
                            entry
                                .repos
                                .iter()
                                .map(|(repo, status)| {
                                    (repo.clone(), legacy_repo_value(status, lane, pool))
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    json!({
                        "role": legacy_role(lane),
                        "connected": entry.is_some_and(|entry| entry.state == LaneState::Ready),
                        "exchanges": entry.map_or(0, |entry| entry.exchanges),
                        "last_success_ms": entry.and_then(|entry| entry.last_success_ms),
                        "error": peer.and_then(|peer| state.lane_error_text(&peer.installation, lane)),
                        "transport": transport(),
                        "parallel_bulk": true,
                        "repositories": repositories,
                    })
                })
                .collect();
            out.push(json!({
                "endpoint": record.endpoint,
                "installation": record.installation,
                "connections": pool,
                "configured_connections": record.connections,
                "reserved_connections": 1,
                "lanes": lanes,
            }));
        }
        let mut value = Value::Array(out);
        scrub_codes(&mut value, peers);
        value
    }

    /// Applies `change` under the state lock; when it reports a change, the
    /// state version moves and waiters are woken if the hub was clean.
    fn mutate(&self, change: impl FnOnce(&mut HubState, u64, &HubConfig) -> bool) {
        let now = self.clock.now_ms();
        let mut state = lock(&self.state);
        let was_clean = state.version == state.built_version;
        if change(&mut state, now, &self.cfg) {
            state.version = state.version.wrapping_add(1);
            drop(state);
            if was_clean {
                self.changed.notify_all();
            }
        }
    }

    /// Rebuilds the cached snapshot if the state changed and the bucket has a
    /// token; otherwise returns how long until the next token (`None` when
    /// nothing changed or the rebuild happened).
    fn refresh(&self, state: &mut HubState) -> Option<Duration> {
        if state.version == state.built_version {
            return None;
        }
        match state.throttle.take(self.clock.instant()) {
            Ok(()) => {
                self.rebuild(state);
                None
            }
            Err(wait) => Some(wait),
        }
    }

    fn rebuild(&self, state: &mut HubState) {
        let now = self.clock.now_ms();
        let generation = state.cached.generation.wrapping_add(1);
        let snapshot = state.build(now, &self.cfg, generation);
        state.cached = Arc::new(snapshot);
        state.built_version = state.version;
        state.rebuilds = state.rebuilds.wrapping_add(1);
    }
}

impl StatusSink for StatusHub {
    fn record(&self, event: StatusEvent) {
        self.mutate(|state, now, _| {
            state.apply(event, now);
            true
        });
    }
}

impl StatusSource for StatusHub {
    fn snapshot(&self) -> Arc<StatusSnapshot> {
        let mut state = lock(&self.state);
        self.refresh(&mut state);
        state.cached.clone()
    }

    /// Returns as soon as a snapshot newer than `generation` exists, or the
    /// current one when `timeout` elapses. A `generation` ahead of the hub
    /// (from a previous daemon run) returns the current snapshot at once.
    fn wait_newer(&self, generation: u64, timeout: Duration) -> Arc<StatusSnapshot> {
        let deadline = Instant::now().checked_add(timeout);
        let mut state = lock(&self.state);
        loop {
            let throttled = self.refresh(&mut state);
            let current = state.cached.generation;
            // Newer, or a generation from a previous daemon run (ahead of us).
            if current != generation {
                return state.cached.clone();
            }
            let remaining = match deadline {
                Some(deadline) => deadline.saturating_duration_since(Instant::now()),
                None => Duration::from_secs(3600),
            };
            if remaining.is_zero() {
                return state.cached.clone();
            }
            let wait = throttled
                .map_or(remaining, |wait| wait.min(remaining))
                .max(Duration::from_millis(1));
            state = self
                .changed
                .wait_timeout(state, wait)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
    }
}

// ---------------------------------------------------------------------------
// State

/// Token bucket for snapshot rebuilds.
#[derive(Debug)]
struct Throttle {
    tokens: f64,
    at: Option<Instant>,
}

impl Throttle {
    fn new() -> Self {
        Self {
            tokens: SNAPSHOT_BURST,
            at: None,
        }
    }

    fn take(&mut self, now: Instant) -> Result<(), Duration> {
        let period = policy::SNAPSHOT_MIN_INTERVAL.as_secs_f64();
        if let Some(at) = self.at {
            let refill = now.saturating_duration_since(at).as_secs_f64() / period;
            self.tokens = (self.tokens + refill).min(SNAPSHOT_BURST);
        }
        self.at = Some(now);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            Ok(())
        } else {
            Err(Duration::from_secs_f64((1.0 - self.tokens) * period))
        }
    }
}

/// The activity ring.
#[derive(Debug, Default)]
struct Ring {
    cap: usize,
    entries: VecDeque<ActivityEntry>,
}

impl Ring {
    fn push_entry(&mut self, entry: ActivityEntry) {
        if self.cap == 0 {
            return;
        }
        while self.entries.len() >= self.cap {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
    }

    /// Like [`push`](Self::push), but replaces the newest entry when it has the
    /// same kind, folder and peer (a long transfer is one entry, not 500).
    fn coalesce(
        &mut self,
        at_ms: u64,
        kind: &str,
        folder: Option<&str>,
        peer: Option<&str>,
        message: String,
    ) {
        if let Some(last) = self.entries.back_mut()
            && last.kind == kind
            && last.folder.as_deref() == folder
            && last.peer.as_deref() == peer
        {
            last.at_ms = at_ms;
            last.message = message;
            return;
        }
        self.push(at_ms, kind, folder, peer, message);
    }

    fn push(
        &mut self,
        at_ms: u64,
        kind: &str,
        folder: Option<&str>,
        peer: Option<&str>,
        message: String,
    ) {
        self.push_entry(ActivityEntry {
            at_ms,
            kind: kind.to_owned(),
            folder: folder.map(str::to_owned),
            peer: peer.map(str::to_owned),
            message,
        });
    }
}

/// An open error with its display streak.
#[derive(Clone, Debug)]
struct OpenError {
    record: ErrorRecord,
    streak: Streak,
    /// Shown at least once (sticky until resolved); only shown errors reach
    /// the history.
    shown: bool,
}

impl OpenError {
    fn visible(&self, now: u64) -> bool {
        self.shown
            || rules::error_visible(
                self.record.error.class,
                &self.record.error.code,
                self.streak,
                now,
            )
    }
}

/// `(scope, slot)`: one open error per slot. Generic `Error` events use the
/// empty slot; exchanges use their op; mirrored records use a fixed name.
type SlotKey = (Scope, String);

/// Open errors and the resolved history of one owner (the daemon, or one
/// folder).
#[derive(Debug, Default)]
struct ErrorBook {
    open: BTreeMap<SlotKey, OpenError>,
    history: VecDeque<ErrorRecord>,
}

impl ErrorBook {
    /// Records one occurrence (deduplicated per slot).
    fn observe(&mut self, key: SlotKey, error: ErrorWire, now: u64, ring: &mut Ring) {
        let previous = self.open.remove(&key);
        let streak = previous
            .as_ref()
            .map_or(Streak::start(now), |open| open.streak.bump());
        let was_shown = previous.as_ref().is_some_and(|open| open.shown);
        let (record, superseded) =
            rules::dedupe(previous.map(|open| open.record), error, key.0.clone(), now);
        if let Some(old) = superseded
            && was_shown
        {
            self.push_history(old);
        }
        let same_code_shown = was_shown && record.count > 1;
        let mut open = OpenError {
            record,
            streak,
            shown: same_code_shown,
        };
        show_if_visible(&mut open, now, ring);
        self.open.insert(key, open);
    }

    /// Mirrors a record owned by someone else (a FolderView error, a mount or
    /// store-health error): `None` resolves the slot.
    fn mirror(&mut self, key: SlotKey, record: Option<ErrorRecord>, now: u64, ring: &mut Ring) {
        let Some(record) = record else {
            self.resolve(|slot, _| *slot == key, now, ring);
            return;
        };
        if record.is_resolved() {
            self.resolve(|slot, _| *slot == key, now, ring);
            return;
        }
        let previous = self.open.remove(&key);
        let mut shown = false;
        if let Some(previous) = previous {
            if previous.record.error.code == record.error.code {
                shown = previous.shown;
            } else if previous.shown {
                let mut old = previous.record;
                old.resolve(now);
                self.push_history(old);
            }
        }
        let streak = Streak {
            failures: record.count.max(1),
            since_ms: record.first_at_ms,
        };
        let mut open = OpenError {
            record,
            streak,
            shown,
        };
        show_if_visible(&mut open, now, ring);
        self.open.insert(key, open);
    }

    /// Resolves every open slot matching `pred`; shown ones move to history.
    fn resolve(
        &mut self,
        pred: impl Fn(&SlotKey, &OpenError) -> bool,
        now: u64,
        ring: &mut Ring,
    ) -> bool {
        let keys: Vec<SlotKey> = self
            .open
            .iter()
            .filter(|(key, open)| pred(key, open))
            .map(|(key, _)| key.clone())
            .collect();
        for key in &keys {
            if let Some(open) = self.open.remove(key)
                && open.shown
            {
                let mut record = open.record;
                record.resolve(now);
                ring.push(
                    now,
                    "resolved",
                    scope_folder(&record.scope),
                    scope_peer(&record.scope),
                    format!("{} resolved", record.error.code),
                );
                self.push_history(record);
            }
        }
        !keys.is_empty()
    }

    /// Marks errors that became visible with time; true if any did.
    fn refresh(&mut self, now: u64, ring: &mut Ring) -> bool {
        let mut changed = false;
        for open in self.open.values_mut() {
            if !open.shown && open.visible(now) {
                announce(open, now, ring);
                changed = true;
            }
        }
        changed
    }

    /// Drops the oldest open errors of item scopes (Object, Event) beyond
    /// [`MAX_ITEM_ERRORS`].
    fn cap_items(&mut self) {
        let mut items: Vec<(u64, SlotKey)> = self
            .open
            .iter()
            .filter(|((scope, _), _)| matches!(scope, Scope::Object { .. } | Scope::Event { .. }))
            .map(|(key, open)| (open.record.last_at_ms, key.clone()))
            .collect();
        if items.len() <= MAX_ITEM_ERRORS {
            return;
        }
        items.sort();
        let excess = items.len() - MAX_ITEM_ERRORS;
        for (_, key) in items.into_iter().take(excess) {
            self.open.remove(&key);
        }
    }

    fn push_history(&mut self, record: ErrorRecord) {
        while self.history.len() >= policy::ERROR_HISTORY {
            self.history.pop_front();
        }
        self.history.push_back(record);
    }

    /// Visible open errors (newest first), then the history (newest first).
    fn list(&self, now: u64, filter: impl Fn(&Scope) -> bool) -> Vec<ErrorRecord> {
        let mut open: Vec<&ErrorRecord> = self
            .open
            .iter()
            .filter(|((scope, _), open)| filter(scope) && open.visible(now))
            .map(|(_, open)| &open.record)
            .collect();
        open.sort_by_key(|record| std::cmp::Reverse(record.last_at_ms));
        open.into_iter()
            .cloned()
            .chain(
                self.history
                    .iter()
                    .rev()
                    .filter(|record| filter(&record.scope))
                    .cloned(),
            )
            .collect()
    }

    /// The open errors of slots matching `pred`.
    fn open_matching<'a>(
        &'a self,
        pred: impl Fn(&SlotKey) -> bool + 'a,
    ) -> impl Iterator<Item = &'a OpenError> + 'a {
        self.open
            .iter()
            .filter(move |(key, _)| pred(key))
            .map(|(_, open)| open)
    }
}

fn show_if_visible(open: &mut OpenError, now: u64, ring: &mut Ring) {
    if !open.shown && open.visible(now) {
        announce(open, now, ring);
    }
}

fn announce(open: &mut OpenError, now: u64, ring: &mut Ring) {
    open.shown = true;
    let record = &open.record;
    ring.push(
        now,
        "error",
        scope_folder(&record.scope),
        scope_peer(&record.scope),
        record.error.to_string(),
    );
}

fn scope_folder(scope: &Scope) -> Option<&str> {
    match scope {
        Scope::Folder { state }
        | Scope::FolderPeer { state, .. }
        | Scope::Object { state, .. }
        | Scope::Event { state, .. }
        | Scope::Mount { state } => Some(state),
        Scope::Daemon | Scope::Peer { .. } | Scope::Lane { .. } => None,
    }
}

fn scope_peer(scope: &Scope) -> Option<&str> {
    match scope {
        Scope::Peer { installation }
        | Scope::Lane { installation, .. }
        | Scope::FolderPeer { installation, .. } => Some(installation),
        _ => None,
    }
}

// Mirrored-record slot names.
const SLOT_LIFECYCLE: &str = "lifecycle";
const SLOT_PUBLISHED: &str = "published";
const SLOT_MOUNT: &str = "mount";
const SLOT_STORE: &str = "store";
// Lane slots by direction.
const SLOT_OUT: &str = "out";
const SLOT_IN: &str = "in";

#[derive(Debug, Default)]
struct FolderEntry {
    view: Option<Box<FolderView>>,
    phase: LifecyclePhase,
    phase_since_ms: u64,
    heartbeat: Option<Arc<WorkerHeartbeat>>,
    mount: MountStatus,
    last_exit: Option<ExitRecord>,
    errors: ErrorBook,
}

impl FolderEntry {
    fn repo(&self) -> Option<&str> {
        self.view.as_ref().map(|view| view.record.repo.as_str())
    }

    fn content(&self) -> Option<&ContentCounters> {
        self.heartbeat.as_ref().and_then(|hb| hb.content.as_ref())
    }

    fn running(&self) -> bool {
        self.phase == LifecyclePhase::Running
    }
}

/// `(inbound, lane)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct LaneKey {
    inbound: bool,
    lane: u32,
}

impl LaneKey {
    fn outbound(lane: u32) -> Self {
        Self {
            inbound: false,
            lane,
        }
    }

    fn of(direction: Direction, lane: u32) -> Self {
        Self {
            inbound: direction == Direction::Inbound,
            lane,
        }
    }
}

/// The legacy `RepositoryStatus` of one repo on one outbound lane (§7.3).
#[derive(Clone, Debug, Default, Serialize)]
struct LegacyRepo {
    last_success_ms: Option<u64>,
    caught_up: bool,
    metadata_caught_up: bool,
    metadata_progress: Option<Value>,
    transfer: Option<Value>,
    object_inventory: Option<Value>,
    last_progress_ms: Option<u64>,
    total_sent_object_bytes: u64,
    total_received_object_bytes: u64,
    total_sent_wire_bytes: u64,
    total_received_wire_bytes: u64,
    error: Option<String>,
    consecutive_failures: u32,
    retry_at_ms: Option<u64>,
}

#[derive(Debug)]
struct LaneEntry {
    lane: u32,
    role: LaneRole,
    direction: Direction,
    state: LaneState,
    since_ms: Option<u64>,
    inflight: Vec<InflightOp>,
    bytes_up: u64,
    bytes_down: u64,
    up: Ewma,
    down: Ewma,
    reconnects: u32,
    ever_ready: bool,
    backoff_until_ms: Option<u64>,
    rtt_ms: Option<u64>,
    /// Open inbound sessions on this (inbound) lane.
    sessions: u32,
    exchanges: u64,
    last_success_ms: Option<u64>,
    repos: BTreeMap<String, LegacyRepo>,
}

impl LaneEntry {
    fn new(lane: u32, role: LaneRole, direction: Direction) -> Self {
        Self {
            lane,
            role,
            direction,
            state: LaneState::Idle,
            since_ms: None,
            inflight: Vec::new(),
            bytes_up: 0,
            bytes_down: 0,
            up: Ewma::new(),
            down: Ewma::new(),
            reconnects: 0,
            ever_ready: false,
            backoff_until_ms: None,
            rtt_ms: None,
            sessions: 0,
            exchanges: 0,
            last_success_ms: None,
            repos: BTreeMap::new(),
        }
    }

    /// Moves to `state`, keeping the lane counters; returns whether it changed.
    fn set_state(&mut self, state: LaneState, now: u64) -> bool {
        if self.state == state {
            return false;
        }
        if state == LaneState::Ready {
            if self.ever_ready {
                self.reconnects = self.reconnects.saturating_add(1);
            }
            self.ever_ready = true;
        } else {
            self.inflight.clear();
        }
        self.state = state;
        self.since_ms = Some(now);
        true
    }
}

#[derive(Debug, Default)]
struct PeerEntry {
    installation: String,
    endpoint: Option<String>,
    /// Configured here (we dial it).
    outbound: bool,
    /// It dialed us at least once.
    inbound: bool,
    /// Its published catalog as last listed (`None`: never listed).
    catalog: Option<Vec<PublishedEntry>>,
    lanes: BTreeMap<LaneKey, LaneEntry>,
    conn: PeerConnState,
    conn_since_ms: Option<u64>,
    up: Ewma,
    down: Ewma,
}

impl PeerEntry {
    fn new(installation: &str) -> Self {
        Self {
            installation: installation.to_owned(),
            ..Self::default()
        }
    }

    fn inbound_sessions(&self) -> u32 {
        self.lanes
            .iter()
            .filter(|(key, _)| key.inbound)
            .map(|(_, lane)| lane.sessions)
            .sum()
    }

    fn catalog_entry(&self, repo: &str) -> Option<&PublishedEntry> {
        self.catalog
            .as_ref()
            .and_then(|catalog| catalog.iter().find(|entry| entry.repo == repo))
    }

    fn direction(&self) -> Direction {
        match (self.outbound, self.inbound) {
            (true, true) => Direction::Both,
            (false, true) => Direction::Inbound,
            _ => Direction::Outbound,
        }
    }

    fn rtt_ms(&self) -> Option<u64> {
        self.lanes
            .get(&LaneKey::outbound(0))
            .and_then(|lane| lane.rtt_ms)
            .or_else(|| self.lanes.values().find_map(|lane| lane.rtt_ms))
    }
}

/// One (repo, peer installation) pair.
#[derive(Debug, Default)]
struct PairEntry {
    outbound_seen: bool,
    inbound_seen: bool,
    summary: Option<ExchangeSummary>,
    last_meta_exchange_ms: Option<u64>,
    up: Ewma,
    down: Ewma,
    last_progress_ms: Option<u64>,
    last_down_ms: Option<u64>,
    last_up_ms: Option<u64>,
    /// Remaining work at the last observation (progress = a decrease).
    work: Option<Work>,
    eligible_since_ms: Option<u64>,
    stalled_since_ms: Option<u64>,
    /// Object bytes moved in the current transfer episode (reset when the
    /// remaining work reaches zero); `total_bytes = episode + remaining`.
    down_episode: u64,
    up_episode: u64,
}

type PairKey = (String, String);

struct HubState {
    version: u64,
    built_version: u64,
    cached: Arc<StatusSnapshot>,
    throttle: Throttle,
    rebuilds: u64,
    last_tick: Option<Instant>,
    daemon: DaemonStatus,
    daemon_errors: ErrorBook,
    folders: BTreeMap<String, FolderEntry>,
    /// repo → folder state.
    repos: BTreeMap<String, String>,
    peers: BTreeMap<String, PeerEntry>,
    pairs: BTreeMap<PairKey, PairEntry>,
    ring: Ring,
    legacy_links: BTreeMap<String, Value>,
    /// Peer display names (catalog `peer_meta.display_name`), by installation.
    labels: BTreeMap<String, String>,
}

/// Everything derived for one (folder, peer) pair at one instant.
struct PairFacts {
    inputs: PeerInputs,
    work: Work,
    meta: MetaProgress,
    direction: Direction,
    stalled_since_ms: Option<u64>,
    backoff_until_ms: Option<u64>,
    error: Option<ErrorRecord>,
    culprit: Option<ErrorRecord>,
}

impl HubState {
    fn new(cfg: &HubConfig, now: u64) -> Self {
        Self {
            version: 0,
            built_version: 0,
            cached: Arc::new(StatusSnapshot::default()),
            throttle: Throttle::new(),
            rebuilds: 0,
            last_tick: None,
            // The daemon sets its identity (installation, version, endpoints)
            // through `StatusHub::set_daemon`.
            daemon: DaemonStatus {
                pid: std::process::id(),
                started_at_ms: now,
                ..DaemonStatus::default()
            },
            daemon_errors: ErrorBook::default(),
            folders: BTreeMap::new(),
            repos: BTreeMap::new(),
            peers: BTreeMap::new(),
            pairs: BTreeMap::new(),
            ring: Ring {
                cap: cfg.activity,
                entries: VecDeque::new(),
            },
            legacy_links: BTreeMap::new(),
            labels: BTreeMap::new(),
        }
    }

    fn find_peer(&self, record: &PeerRecord) -> Option<&PeerEntry> {
        (!record.installation.is_empty())
            .then(|| self.peers.get(&record.installation))
            .flatten()
            .or_else(|| {
                self.peers
                    .values()
                    .find(|peer| peer.endpoint.as_deref() == Some(record.endpoint.as_str()))
            })
    }

    fn peer_mut(&mut self, installation: &str) -> &mut PeerEntry {
        self.peers
            .entry(installation.to_owned())
            .or_insert_with(|| PeerEntry::new(installation))
    }

    /// The legacy error string of an outbound lane: its open error, shown or
    /// not (legacy status showed the last error as is).
    fn lane_error_text(&self, installation: &str, lane: u32) -> Option<String> {
        let key = (
            Scope::Lane {
                installation: installation.to_owned(),
                lane,
            },
            SLOT_OUT.to_owned(),
        );
        self.daemon_errors
            .open
            .get(&key)
            .map(|open| open.record.error.to_string())
    }

    // -- event application --------------------------------------------------

    fn apply(&mut self, event: StatusEvent, now: u64) {
        match event {
            StatusEvent::FolderLifecycle(view) => self.on_view(*view, now),
            StatusEvent::WorkerHeartbeat { state, hb } => self.on_heartbeat(&state, hb, now, true),
            StatusEvent::FolderRemoved { state } => self.on_folder_removed(&state, now),
            StatusEvent::PeerConfigured {
                installation,
                endpoint,
            } => {
                let peer = self.peer_mut(&installation);
                let new = !peer.outbound;
                peer.outbound = true;
                peer.endpoint = Some(endpoint.clone());
                if new {
                    self.ring.push(
                        now,
                        "peer",
                        None,
                        Some(&installation),
                        format!("peer configured: {endpoint}"),
                    );
                }
            }
            StatusEvent::PeerRemoved { installation } => self.on_peer_removed(&installation, now),
            StatusEvent::LaneState {
                installation,
                lane,
                role,
                direction,
                state,
                error,
                backoff_until_ms,
                rtt_ms,
            } => self.on_lane_state(
                &installation,
                lane,
                role,
                direction,
                state,
                error,
                backoff_until_ms,
                rtt_ms,
                now,
            ),
            StatusEvent::ExchangeStarted {
                installation,
                repo,
                lane,
                op,
                at_ms,
            } => {
                let peer = self.peer_mut(&installation);
                let role = legacy_lane_role(lane);
                let entry = peer
                    .lanes
                    .entry(LaneKey::outbound(lane))
                    .or_insert_with(|| LaneEntry::new(lane, role, Direction::Outbound));
                if entry.inflight.len() >= MAX_INFLIGHT {
                    entry.inflight.remove(0);
                }
                entry.inflight.push(InflightOp {
                    op,
                    repo: repo.clone(),
                    since_ms: at_ms,
                    bytes: 0,
                });
                let pair = self.pairs.entry((repo, installation)).or_default();
                pair.outbound_seen = true;
            }
            StatusEvent::ExchangeFinished {
                installation,
                repo,
                lane,
                op,
                direction,
                ok,
                error,
                summary,
                bytes_up,
                bytes_down,
                objects_up,
                objects_down,
                duration_ms,
            } => self.on_exchange_finished(
                Exchange {
                    installation,
                    repo,
                    lane,
                    op,
                    direction,
                    ok,
                    error,
                    summary,
                    bytes_up,
                    bytes_down,
                    objects_up,
                    objects_down,
                    duration_ms,
                },
                now,
            ),
            StatusEvent::PeerCatalog {
                installation,
                entries,
            } => {
                let peer = self.peer_mut(&installation);
                peer.outbound = true;
                peer.catalog = Some(entries);
            }
            StatusEvent::InboundSession {
                installation,
                lane,
                role,
                opened,
            } => {
                let peer = self.peer_mut(&installation);
                peer.inbound = true;
                let entry = peer
                    .lanes
                    .entry(LaneKey::of(Direction::Inbound, lane))
                    .or_insert_with(|| LaneEntry::new(lane, role, Direction::Inbound));
                entry.role = role;
                if opened {
                    entry.sessions = entry.sessions.saturating_add(1);
                } else {
                    entry.sessions = entry.sessions.saturating_sub(1);
                }
                let state = if entry.sessions > 0 {
                    LaneState::Ready
                } else {
                    LaneState::Closed
                };
                entry.set_state(state, now);
                self.update_conn(&installation, now);
            }
            StatusEvent::InboundIncompatible { address, detail } => {
                let code = code_in(&detail).unwrap_or("PEER_NEEDS_UPGRADE").to_owned();
                let message = format!("inbound connection from {address}: {detail}");
                let key = (Scope::Daemon, format!("inbound:{address}"));
                self.daemon_errors.observe(
                    key,
                    ErrorWire::new(code, message.clone()),
                    now,
                    &mut self.ring,
                );
                self.ring
                    .push(now, "inbound_incompatible", None, None, message);
            }
            StatusEvent::ObjectError { state, hash, error } => {
                let folder = self.folders.entry(state.clone()).or_default();
                let key = (Scope::Object { state, hash }, String::new());
                folder.errors.observe(key, error, now, &mut self.ring);
                folder.errors.cap_items();
            }
            StatusEvent::LegacyLink { key, value } => {
                if !self.legacy_links.contains_key(&key)
                    && self.legacy_links.len() >= MAX_LEGACY_LINKS
                {
                    self.legacy_links.pop_first();
                }
                self.legacy_links.insert(key, value);
            }
            StatusEvent::Error { scope, error } => self.on_error(scope, error, now),
            StatusEvent::Resolved { scope, code } => self.on_resolved(&scope, &code, now),
            StatusEvent::Activity(entry) => self.ring.push_entry(entry),
        }
    }

    fn on_view(&mut self, view: FolderView, now: u64) {
        let state = view.record.state.clone();
        let repo = view.record.repo.clone();
        let phase = view.phase.lifecycle();
        let entry = self.folders.entry(state.clone()).or_default();
        if let Some(old) = entry.repo()
            && old != repo
        {
            let old = old.to_owned();
            self.repos.remove(&old);
        }
        self.repos.insert(repo, state.clone());
        let entry = self.folders.get_mut(&state).expect("inserted above");
        let first = entry.view.is_none();
        let phase_changed = first || entry.phase != phase;
        if phase_changed {
            entry.phase_since_ms = phase_since(&view.phase).unwrap_or(now);
            entry.phase = phase;
            self.ring.push(
                now,
                "lifecycle",
                Some(&state),
                None,
                lifecycle_message(&view.phase),
            );
            if phase == LifecyclePhase::Running {
                // A new worker is serving: a success in the folder scope.
                let folder_scope = Scope::Folder {
                    state: state.clone(),
                };
                entry.errors.resolve(
                    |(scope, slot), _| *scope == folder_scope && slot.is_empty(),
                    now,
                    &mut self.ring,
                );
            }
        }
        if let FolderPhase::Backoff { last_exit, .. } = &view.phase {
            entry.last_exit = Some(last_exit.clone());
        }
        let gate_changed = entry
            .view
            .as_ref()
            .is_none_or(|old| old.gate.kind() != view.gate.kind());
        if gate_changed && !(first && view.gate == GateState::None) {
            self.ring
                .push(now, "gate", Some(&state), None, gate_message(&view.gate));
        }
        // Mirrored records owned by the controller.
        let lifecycle_error = match &view.phase {
            FolderPhase::Failed { error } => Some(error.clone()),
            _ => view.last_error.clone(),
        };
        let folder_scope = Scope::Folder {
            state: state.clone(),
        };
        entry.errors.mirror(
            (folder_scope.clone(), SLOT_LIFECYCLE.into()),
            lifecycle_error,
            now,
            &mut self.ring,
        );
        entry.errors.mirror(
            (folder_scope, SLOT_PUBLISHED.into()),
            view.published_error.clone(),
            now,
            &mut self.ring,
        );
        let heartbeat = view.heartbeat.clone();
        entry.mount = view.mount.clone();
        entry.view = Some(Box::new(view));
        self.mirror_mount(&state, now);
        if let Some(hb) = heartbeat {
            self.on_heartbeat(&state, hb, now, false);
        }
    }

    /// A heartbeat, from a `WorkerHeartbeat` event (`take_mount`: its mount
    /// state is the freshest) or carried by a FolderView (whose own mount
    /// state, set by the controller, wins).
    fn on_heartbeat(&mut self, state: &str, hb: Arc<WorkerHeartbeat>, now: u64, take_mount: bool) {
        let entry = self.folders.entry(state.to_owned()).or_default();
        if let Some(old) = &entry.heartbeat
            && (Arc::ptr_eq(old, &hb) || (old.instance == hb.instance && old.seq >= hb.seq))
        {
            return;
        }
        let previous = entry.heartbeat.replace(Arc::clone(&hb));
        if take_mount {
            entry.mount = hb.mount.clone();
        }
        // Store health: a mirrored Folder-scope record.
        let store_error = hb.store_health.error().map(|error| {
            let first = entry
                .errors
                .open
                .get(&(
                    Scope::Folder {
                        state: state.into(),
                    },
                    SLOT_STORE.to_owned(),
                ))
                .filter(|open| open.record.error.code == error.code)
                .map_or(now, |open| open.record.first_at_ms);
            let mut record = ErrorRecord::new(
                error.clone(),
                Scope::Folder {
                    state: state.into(),
                },
                first,
            );
            record.last_at_ms = now;
            record
        });
        entry.errors.mirror(
            (
                Scope::Folder {
                    state: state.into(),
                },
                SLOT_STORE.into(),
            ),
            store_error,
            now,
            &mut self.ring,
        );
        // Implicit successes for item scopes.
        if hb
            .content
            .is_some_and(|content| rules::missing(&content) == (0, 0))
        {
            entry.errors.resolve(
                |(scope, _), _| matches!(scope, Scope::Object { .. }),
                now,
                &mut self.ring,
            );
        }
        if hb
            .inbox
            .is_some_and(|inbox| inbox.blocked == 0 && inbox.quarantined == 0)
        {
            entry.errors.resolve(
                |(scope, _), _| matches!(scope, Scope::Event { .. }),
                now,
                &mut self.ring,
            );
        }
        // Progress the worker made on its own: downloads and activation.
        let downloaded = match (previous.as_ref().and_then(|old| old.content), hb.content) {
            (Some(before), Some(after)) => {
                let (bo, bb) = rules::missing(&before);
                let (ao, ab) = rules::missing(&after);
                ao < bo || ab < bb || after.partial_bytes > before.partial_bytes
            }
            _ => false,
        };
        let activated = match (
            previous.as_ref().and_then(|old| old.activation),
            hb.activation,
        ) {
            (Some(before), Some(after)) => after.activated > before.activated,
            _ => false,
        };
        self.mirror_mount(state, now);
        if (downloaded || activated)
            && let Some(repo) = self
                .folders
                .get(state)
                .and_then(|f| f.repo().map(str::to_owned))
        {
            for ((pair_repo, _), pair) in self.pairs.iter_mut() {
                if *pair_repo != repo {
                    continue;
                }
                let work = pair.work.unwrap_or_default();
                if (downloaded && (work.down_bytes > 0 || work.down_objects > 0))
                    || (activated && work.meta > 0)
                {
                    pair.last_progress_ms = Some(now);
                    if downloaded {
                        pair.last_down_ms = Some(now);
                    }
                }
            }
        }
    }

    /// Mirrors the folder's current mount state into the Mount scope.
    fn mirror_mount(&mut self, state: &str, now: u64) {
        let Some(entry) = self.folders.get_mut(state) else {
            return;
        };
        let scope = Scope::Mount {
            state: state.to_owned(),
        };
        let key = (scope.clone(), SLOT_MOUNT.to_owned());
        let mount = entry.mount.clone();
        match &mount.state {
            MountState::AttachFailed {
                error,
                attempt,
                next_retry_at_ms,
            } => {
                let first = entry
                    .errors
                    .open
                    .get(&key)
                    .filter(|open| open.record.error.code == error.code)
                    .map_or(now, |open| open.record.first_at_ms);
                let mut record = ErrorRecord::new(error.clone(), scope, first);
                record.last_at_ms = now;
                record.count = u64::from((*attempt).max(1));
                record.next_retry_at_ms = *next_retry_at_ms;
                entry.errors.mirror(key, Some(record), now, &mut self.ring);
            }
            MountState::Lost { error } => {
                let first = entry
                    .errors
                    .open
                    .get(&key)
                    .filter(|open| open.record.error.code == error.code)
                    .map_or(now, |open| open.record.first_at_ms);
                let mut record = ErrorRecord::new(error.clone(), scope, first);
                record.last_at_ms = now;
                entry.errors.mirror(key, Some(record), now, &mut self.ring);
            }
            MountState::Attached { .. } => {
                // Success in the mount scope clears every mount slot.
                entry.errors.resolve(
                    |(slot_scope, _), _| *slot_scope == scope,
                    now,
                    &mut self.ring,
                );
            }
            _ if mount.intent != MountIntent::Attach => {
                entry.errors.resolve(
                    |(slot_scope, _), _| *slot_scope == scope,
                    now,
                    &mut self.ring,
                );
            }
            // Attaching, retrying: the last failure stays until it resolves.
            _ => {}
        }
    }

    fn on_folder_removed(&mut self, state: &str, now: u64) {
        let Some(entry) = self.folders.remove(state) else {
            return;
        };
        if let Some(repo) = entry.repo() {
            let repo = repo.to_owned();
            self.repos.remove(&repo);
            self.pairs.retain(|(pair_repo, _), _| *pair_repo != repo);
            for peer in self.peers.values_mut() {
                for lane in peer.lanes.values_mut() {
                    lane.repos.remove(&repo);
                }
            }
        }
        self.ring
            .push(now, "lifecycle", Some(state), None, "folder removed".into());
    }

    fn on_peer_removed(&mut self, installation: &str, now: u64) {
        let Some(peer) = self.peers.get_mut(installation) else {
            return;
        };
        peer.outbound = false;
        peer.catalog = None;
        peer.endpoint = None;
        peer.lanes.retain(|key, _| key.inbound);
        let keep = peer.inbound_sessions() > 0;
        if !keep {
            self.peers.remove(installation);
            self.pairs.retain(|(_, inst), _| inst != installation);
        } else {
            self.pairs
                .retain(|(_, inst), pair| inst != installation || pair.inbound_seen);
        }
        let of_peer = |scope: &Scope| scope_peer(scope) == Some(installation);
        self.daemon_errors
            .resolve(|(scope, _), _| of_peer(scope), now, &mut self.ring);
        for folder in self.folders.values_mut() {
            folder
                .errors
                .resolve(|(scope, _), _| of_peer(scope), now, &mut self.ring);
        }
        self.ring
            .push(now, "peer", None, Some(installation), "peer removed".into());
        if keep {
            self.update_conn(installation, now);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_lane_state(
        &mut self,
        installation: &str,
        lane: u32,
        role: LaneRole,
        direction: Direction,
        state: LaneState,
        error: Option<ErrorWire>,
        backoff_until_ms: Option<u64>,
        rtt_ms: Option<u64>,
        now: u64,
    ) {
        let peer = self.peer_mut(installation);
        if direction == Direction::Inbound {
            peer.inbound = true;
        } else {
            peer.outbound = true;
        }
        let key = LaneKey::of(direction, lane);
        let entry = peer
            .lanes
            .entry(key)
            .or_insert_with(|| LaneEntry::new(lane, role, direction));
        entry.role = role;
        entry.direction = direction;
        entry.set_state(state, now);
        entry.backoff_until_ms = backoff_until_ms;
        if rtt_ms.is_some() {
            entry.rtt_ms = rtt_ms;
        }
        let slot = if key.inbound { SLOT_IN } else { SLOT_OUT };
        let lane_scope = Scope::Lane {
            installation: installation.to_owned(),
            lane,
        };
        match error {
            Some(mut error) => {
                if error.retry_after_ms.is_none()
                    && let Some(until) = backoff_until_ms
                {
                    error.retry_after_ms = Some(until.saturating_sub(now));
                }
                self.daemon_errors
                    .observe((lane_scope, slot.into()), error, now, &mut self.ring);
            }
            None if state == LaneState::Ready => {
                let peer_scope = Scope::Peer {
                    installation: installation.to_owned(),
                };
                self.daemon_errors.resolve(
                    |(scope, slot_name), _| {
                        (*scope == lane_scope && slot_name == slot)
                            || (*scope == peer_scope && slot_name.is_empty())
                    },
                    now,
                    &mut self.ring,
                );
            }
            None => {}
        }
        self.update_conn(installation, now);
    }

    fn on_exchange_finished(&mut self, x: Exchange, now: u64) {
        let objects_op = is_objects_op(&x.op);
        let folder_state = self.repos.get(&x.repo).cloned();
        let content = folder_state
            .as_ref()
            .and_then(|state| self.folders.get(state))
            .and_then(|folder| folder.content().copied());
        // Lane and peer accounting.
        let peer = self.peer_mut(&x.installation);
        match x.direction {
            Direction::Inbound => peer.inbound = true,
            Direction::Outbound => peer.outbound = true,
            Direction::Both => {
                peer.inbound = true;
                peer.outbound = true;
            }
        }
        peer.up.add(x.bytes_up);
        peer.down.add(x.bytes_down);
        let remote_content = peer.catalog_entry(&x.repo).and_then(|entry| entry.content);
        let key = LaneKey::of(x.direction, x.lane);
        let role = legacy_lane_role(x.lane);
        let lane = peer
            .lanes
            .entry(key)
            .or_insert_with(|| LaneEntry::new(x.lane, role, x.direction));
        if let Some(index) = lane
            .inflight
            .iter()
            .position(|op| op.op == x.op && op.repo == x.repo)
        {
            lane.inflight.remove(index);
        }
        lane.bytes_up = lane.bytes_up.saturating_add(x.bytes_up);
        lane.bytes_down = lane.bytes_down.saturating_add(x.bytes_down);
        lane.up.add(x.bytes_up);
        lane.down.add(x.bytes_down);
        lane.exchanges = lane.exchanges.saturating_add(1);
        if x.ok {
            lane.last_success_ms = Some(now);
        }
        // Pair accounting.
        let pair = self
            .pairs
            .entry((x.repo.clone(), x.installation.clone()))
            .or_default();
        match x.direction {
            Direction::Inbound => pair.inbound_seen = true,
            Direction::Outbound => pair.outbound_seen = true,
            Direction::Both => {
                pair.inbound_seen = true;
                pair.outbound_seen = true;
            }
        }
        pair.up.add(x.bytes_up);
        pair.down.add(x.bytes_down);
        let mut progress = x.objects_up > 0 || x.objects_down > 0;
        if objects_op {
            pair.down_episode = pair.down_episode.saturating_add(x.bytes_down);
            pair.up_episode = pair.up_episode.saturating_add(x.bytes_up);
            if x.bytes_down >= rules::PROGRESS_MIN_BYTES || x.objects_down > 0 {
                pair.last_down_ms = Some(now);
                progress = true;
            }
            if x.bytes_up >= rules::PROGRESS_MIN_BYTES || x.objects_up > 0 {
                pair.last_up_ms = Some(now);
                progress = true;
            }
        }
        if let Some(summary) = &x.summary {
            let moved = summary.sent_events > 0
                || summary.received_events > 0
                || summary.staged > 0
                || summary.acked > 0
                || summary.metadata_advanced;
            let before = pair.summary.as_ref().map(meta_work);
            let after = meta_work(summary);
            progress |= moved || before.is_some_and(|before| after < before);
            pair.summary = Some(summary.clone());
            pair.last_meta_exchange_ms = Some(now);
        }
        // A failed metadata exchange withdraws the caught-up claim until the
        // next success, except under Contention, which never resets it (§8).
        if !x.ok
            && !objects_op
            && x.error
                .as_ref()
                .is_some_and(|e| e.class.counts_as_failure())
            && let Some(summary) = pair.summary.as_mut()
        {
            summary.caught_up = false;
        }
        if progress {
            pair.last_progress_ms = Some(now);
            if pair.stalled_since_ms.take().is_some() {
                self.ring.push(
                    now,
                    "stall",
                    folder_state.as_deref(),
                    Some(&x.installation),
                    "stall cleared".into(),
                );
            }
        }
        // Errors: per (peer, repo, op) slot of the folder book.
        if let Some(state) = &folder_state
            && let Some(folder) = self.folders.get_mut(state)
        {
            let scope = Scope::FolderPeer {
                state: state.clone(),
                installation: x.installation.clone(),
            };
            match (&x.error, x.ok) {
                (_, true) => {
                    folder.errors.resolve(
                        |(slot_scope, slot), _| {
                            *slot_scope == scope && (slot.is_empty() || *slot == x.op)
                        },
                        now,
                        &mut self.ring,
                    );
                }
                (Some(error), false) => {
                    folder.errors.observe(
                        (scope, x.op.clone()),
                        error.clone(),
                        now,
                        &mut self.ring,
                    );
                }
                (None, false) => {}
            }
        }
        if progress && let Some(state) = &folder_state {
            self.ring.coalesce(
                now,
                "exchange",
                Some(state),
                Some(&x.installation),
                exchange_message(&x),
            );
        }
        // Legacy RepositoryStatus (outbound lanes only, §7.3).
        if x.direction != Direction::Inbound {
            let peer = self.peers.get_mut(&x.installation).expect("created above");
            let lane = peer
                .lanes
                .get_mut(&LaneKey::outbound(x.lane))
                .expect("created above");
            let repo = lane.repos.entry(x.repo.clone()).or_default();
            update_legacy_repo(repo, &x, objects_op, progress, content, remote_content, now);
        }
    }

    fn on_error(&mut self, scope: Scope, error: ErrorWire, now: u64) {
        match &scope {
            Scope::Daemon => {
                self.daemon_errors
                    .observe((scope, String::new()), error, now, &mut self.ring);
            }
            Scope::Peer { installation } => {
                let installation = installation.clone();
                self.daemon_errors
                    .observe((scope, String::new()), error, now, &mut self.ring);
                self.update_conn(&installation, now);
            }
            Scope::Lane { installation, .. } => {
                let installation = installation.clone();
                self.daemon_errors
                    .observe((scope, SLOT_OUT.into()), error, now, &mut self.ring);
                self.update_conn(&installation, now);
            }
            Scope::Folder { state }
            | Scope::Mount { state }
            | Scope::Object { state, .. }
            | Scope::Event { state, .. }
            | Scope::FolderPeer { state, .. } => {
                let folder = self.folders.entry(state.clone()).or_default();
                folder
                    .errors
                    .observe((scope, String::new()), error, now, &mut self.ring);
                folder.errors.cap_items();
            }
        }
    }

    fn on_resolved(&mut self, scope: &Scope, code: &str, now: u64) {
        let matches = |(slot_scope, _): &SlotKey, open: &OpenError| {
            slot_scope == scope && (code.is_empty() || open.record.error.code == code)
        };
        match scope {
            Scope::Daemon | Scope::Peer { .. } | Scope::Lane { .. } => {
                self.daemon_errors.resolve(matches, now, &mut self.ring);
                if let Some(installation) = scope_peer(scope) {
                    let installation = installation.to_owned();
                    self.update_conn(&installation, now);
                }
            }
            _ => {
                if let Some(state) = scope_folder(scope)
                    && let Some(folder) = self.folders.get_mut(state)
                {
                    folder.errors.resolve(matches, now, &mut self.ring);
                }
            }
        }
    }

    /// Recomputes a peer's connection badge; records transitions.
    fn update_conn(&mut self, installation: &str, now: u64) {
        let revoked = self
            .daemon_errors
            .open_matching(|(scope, _)| scope_peer(scope) == Some(installation))
            .any(|open| open.record.error.code == "PEER_REVOKED");
        let Some(peer) = self.peers.get_mut(installation) else {
            return;
        };
        let lanes: Vec<LaneState> = peer
            .lanes
            .iter()
            .filter(|(key, _)| !key.inbound)
            .map(|(_, lane)| lane.state)
            .collect();
        let conn = rules::peer_conn_state(&lanes, peer.inbound_sessions(), revoked);
        if conn != peer.conn || peer.conn_since_ms.is_none() {
            let changed = conn != peer.conn;
            peer.conn = conn;
            peer.conn_since_ms = Some(now);
            if changed {
                self.ring.push(
                    now,
                    "peer",
                    None,
                    Some(installation),
                    format!("peer {}", conn_name(conn)),
                );
            }
        }
    }

    // -- tick -----------------------------------------------------------------

    fn tick(&mut self, now: u64, elapsed: Duration, cfg: &HubConfig) -> bool {
        let mut changed = false;
        let mut fold = |ewma: &mut Ewma| {
            let before = (ewma.short_bps(), ewma.long_bps());
            ewma.tick(elapsed);
            changed |= before != (ewma.short_bps(), ewma.long_bps());
        };
        for peer in self.peers.values_mut() {
            fold(&mut peer.up);
            fold(&mut peer.down);
            for lane in peer.lanes.values_mut() {
                fold(&mut lane.up);
                fold(&mut lane.down);
            }
        }
        for pair in self.pairs.values_mut() {
            fold(&mut pair.up);
            fold(&mut pair.down);
        }
        // Stall detection and remaining-work tracking per (folder, peer).
        let mut updates = Vec::new();
        for (state, folder) in &self.folders {
            let Some(view) = &folder.view else { continue };
            for (installation, peer) in &self.peers {
                let key = (view.record.repo.clone(), installation.clone());
                let pair = self.pairs.get(&key);
                let facts = self.pair_facts(state, folder, view, peer, pair, now, cfg);
                updates.push((key, facts.inputs, facts.work));
            }
        }
        for (key, inputs, work) in updates {
            let eligible = inputs.can_progress() && inputs.work_remaining();
            if !eligible && !self.pairs.contains_key(&key) {
                continue;
            }
            let pair = self.pairs.entry(key.clone()).or_default();
            if let Some(before) = pair.work
                && rules::progressed(&before, &work)
            {
                pair.last_progress_ms = Some(now);
            }
            pair.work = Some(work);
            if work.down_bytes == 0 && work.down_objects == 0 {
                pair.down_episode = 0;
            }
            if work.up_bytes == 0 && work.up_objects == 0 {
                pair.up_episode = 0;
            }
            pair.eligible_since_ms = if eligible {
                Some(pair.eligible_since_ms.unwrap_or(now))
            } else {
                None
            };
            let stalled = rules::stall(&StallInput {
                work_remaining: inputs.work_remaining(),
                connected: inputs.can_progress(),
                eligible_since_ms: pair.eligible_since_ms,
                last_progress_ms: pair.last_progress_ms,
                now_ms: now,
                stall_after: cfg.stall_after,
            });
            if stalled.is_some() != pair.stalled_since_ms.is_some() {
                changed = true;
                let state = self.repos.get(&key.0).cloned();
                let message = match stalled {
                    Some(_) => format!(
                        "stalled: no progress for {}",
                        rules::format_elapsed(rules::ms(cfg.stall_after))
                    ),
                    None => "stall cleared".to_owned(),
                };
                self.ring
                    .push(now, "stall", state.as_deref(), Some(&key.1), message);
            }
            pair.stalled_since_ms = stalled;
        }
        // Time-based error visibility; expiry of inbound-incompatible notices.
        changed |= self.daemon_errors.refresh(now, &mut self.ring);
        for folder in self.folders.values_mut() {
            changed |= folder.errors.refresh(now, &mut self.ring);
        }
        let ttl = rules::ms(INBOUND_INCOMPATIBLE_TTL);
        changed |= self.daemon_errors.resolve(
            |(scope, slot), open| {
                *scope == Scope::Daemon
                    && slot.starts_with("inbound:")
                    && now.saturating_sub(open.record.last_at_ms) >= ttl
            },
            now,
            &mut self.ring,
        );
        changed
    }

    // -- derivation -----------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    fn pair_facts(
        &self,
        state: &str,
        folder: &FolderEntry,
        view: &FolderView,
        peer: &PeerEntry,
        pair: Option<&PairEntry>,
        now: u64,
        cfg: &HubConfig,
    ) -> PairFacts {
        let repo = &view.record.repo;
        let entry = peer.catalog_entry(repo);
        let gate_source = match &view.gate {
            GateState::Awaiting { source, .. } | GateState::Blocked { source, .. } => {
                Some(source.as_str())
            }
            _ => None,
        };
        let inbound_seen = pair.is_some_and(|pair| pair.inbound_seen);
        let outbound_seen = pair.is_some_and(|pair| pair.outbound_seen);
        let shared = entry.is_some()
            || inbound_seen
            || (peer.catalog.is_none() && outbound_seen)
            || gate_source == Some(peer.installation.as_str());
        let scope = Scope::FolderPeer {
            state: state.to_owned(),
            installation: peer.installation.clone(),
        };
        let pair_errors: Vec<&OpenError> = folder
            .errors
            .open_matching(|(slot_scope, _)| *slot_scope == scope)
            .collect();
        let flags = pair_errors
            .iter()
            .fold(ErrorFlags::default(), |flags, open| {
                flags.with(
                    open.record.error.class,
                    &open.record.error.code,
                    open.visible(now),
                )
            });
        // Metadata: the last exchange summary, refreshed by the worker's
        // per-peer feed counters when they are at least as recent.
        let mut meta = MetaProgress::default();
        let mut meta_known = false;
        if let Some(summary) = pair.and_then(|pair| pair.summary.as_ref()) {
            meta_known = true;
            meta.to_send = summary.to_send;
            meta.to_receive = summary.to_receive;
            meta.local_pending = summary.local_pending;
            meta.remote_pending = summary.remote_pending;
            meta.blocked = summary.blocked;
            meta.quarantined = summary.quarantined;
            meta.caught_up = summary.caught_up;
            meta.cursor_seq = summary.cursor_in;
            meta.remote_tip = summary.remote_tip;
            meta.last_exchange_ms = pair.and_then(|pair| pair.last_meta_exchange_ms);
        }
        let counters = entry.and_then(|entry| {
            folder
                .heartbeat
                .as_ref()
                .and_then(|hb| hb.peers.iter().find(|c| c.replica == entry.replica))
        });
        if let Some(c) = counters
            && c.last_exchange_ms >= meta.last_exchange_ms
        {
            meta_known = true;
            meta.to_send = c.to_send;
            meta.to_receive = c.to_receive;
            meta.local_pending = c.staged_pending;
            meta.blocked = c.blocked;
            meta.quarantined = c.quarantined;
            meta.rejected = c.rejected;
            meta.caught_up = c.caught_up;
            meta.cursor_seq = c.cursor_seq;
            meta.remote_tip = c.remote_tip;
            meta.last_exchange_ms = c.last_exchange_ms;
        }
        if meta.remote_tip == 0
            && let Some(tip) = entry.and_then(|entry| entry.tip.as_ref())
        {
            meta.remote_tip = tip.seq;
        }
        // Object work exists only with a peer that shares the repo.
        let local_content = folder.content().filter(|_| shared);
        let remote_content = entry.and_then(|entry| entry.content.as_ref());
        let (down_objects, down_bytes) = local_content
            .map(|local| rules::download_work(local, remote_content))
            .unwrap_or_default();
        let (up_objects, up_bytes) = remote_content
            .map(|remote| rules::upload_work(folder.content(), remote))
            .unwrap_or_default();
        let work = Work {
            meta: meta_work_of(&meta),
            down_bytes,
            down_objects,
            up_bytes,
            up_objects,
        };
        let mut inputs = PeerInputs {
            shared,
            sync_enabled: view.record.sync_enabled,
            local_running: folder.running(),
            conn: peer.conn,
            remote: entry.map(|entry| entry.state),
            errors: flags,
            stalled: false,
            meta_known,
            meta_work: work.meta,
            down_objects,
            down_bytes,
            up_objects,
            up_bytes,
            blocked: meta
                .blocked
                .saturating_add(meta.quarantined)
                .saturating_add(meta.rejected),
        };
        let eligible = inputs.can_progress() && inputs.work_remaining();
        let stalled_since_ms = pair.and_then(|pair| {
            rules::stall(&StallInput {
                work_remaining: inputs.work_remaining(),
                connected: inputs.can_progress(),
                eligible_since_ms: if eligible {
                    pair.eligible_since_ms
                } else {
                    None
                },
                last_progress_ms: pair.last_progress_ms,
                now_ms: now,
                stall_after: cfg.stall_after,
            })
        });
        inputs.stalled = stalled_since_ms.is_some();
        let backoff_until_ms = pair_errors
            .iter()
            .filter(|open| open.record.error.class.retries_automatically())
            .filter_map(|open| open.record.next_retry_at_ms)
            .max()
            .or_else(|| {
                (peer.conn == PeerConnState::Backoff)
                    .then(|| {
                        peer.lanes
                            .values()
                            .filter_map(|lane| lane.backoff_until_ms)
                            .min()
                    })
                    .flatten()
            });
        let error = pair_errors
            .iter()
            .filter(|open| open.visible(now))
            .max_by_key(|open| open.record.last_at_ms)
            .map(|open| open.record.clone());
        let culprit = if inputs.stalled {
            pair_errors
                .iter()
                .max_by_key(|open| open.record.last_at_ms)
                .map(|open| open.record.clone())
                .or_else(|| {
                    folder
                        .errors
                        .open
                        .iter()
                        .filter(|((scope, _), _)| {
                            matches!(scope, Scope::Object { .. } | Scope::Event { .. })
                        })
                        .max_by_key(|(_, open)| open.record.last_at_ms)
                        .map(|(_, open)| open.record.clone())
                })
        } else {
            None
        };
        let direction = match (
            outbound_seen || (peer.outbound && entry.is_some()),
            inbound_seen,
        ) {
            (true, true) => Direction::Both,
            (false, true) => Direction::Inbound,
            (true, false) => Direction::Outbound,
            (false, false) => peer.direction(),
        };
        PairFacts {
            inputs,
            work,
            meta,
            direction,
            stalled_since_ms,
            backoff_until_ms,
            error,
            culprit,
        }
    }

    fn build(&self, now: u64, cfg: &HubConfig, generation: u64) -> StatusSnapshot {
        let mut folders = Vec::with_capacity(self.folders.len());
        for (state, folder) in &self.folders {
            if let Some(view) = &folder.view {
                folders.push(self.build_folder(state, folder, view, now, cfg));
            }
        }
        let peers = self
            .peers
            .values()
            .map(|peer| self.build_peer(peer, now))
            .collect();
        StatusSnapshot {
            schema: STATUS_SCHEMA,
            generation,
            at_ms: now,
            daemon: DaemonStatus {
                errors: self.daemon_errors.list(now, |_| true),
                ..self.daemon.clone()
            },
            folders,
            peers,
            activity: self.ring.entries.iter().cloned().collect(),
        }
    }

    fn build_folder(
        &self,
        state: &str,
        folder: &FolderEntry,
        view: &FolderView,
        now: u64,
        cfg: &HubConfig,
    ) -> FolderStatus {
        let record = &view.record;
        let hb = folder.heartbeat.as_deref();
        let facts: Vec<(&PeerEntry, Option<&PairEntry>, PairFacts)> = self
            .peers
            .values()
            .map(|peer| {
                let pair = self
                    .pairs
                    .get(&(record.repo.clone(), peer.installation.clone()));
                let facts = self.pair_facts(state, folder, view, peer, pair, now, cfg);
                (peer, pair, facts)
            })
            .collect();
        let down_rate: u64 = facts
            .iter()
            .filter(|(_, _, facts)| facts.inputs.shared)
            .filter_map(|(_, pair, _)| pair.map(|pair| pair.down.short_bps()))
            .sum();
        let mut peers = Vec::with_capacity(facts.len());
        let mut summary = SummaryFacts::default();
        for (peer, pair, facts) in &facts {
            let peer_state = rules::peer_state(&facts.inputs);
            let shared = facts.inputs.shared;
            let up_rate = pair.map_or(0, |pair| pair.up.short_bps());
            let in_flight = objects_in_flight(peer, &record.repo);
            if shared {
                summary.meta_left = summary.meta_left.max(facts.work.meta);
                summary.down_bytes_left = summary.down_bytes_left.max(facts.work.down_bytes);
                summary.up_bytes_left = summary.up_bytes_left.max(facts.work.up_bytes);
                summary.up_objects_left = summary.up_objects_left.max(facts.work.up_objects);
                summary.up_rate_bps = summary.up_rate_bps.saturating_add(up_rate);
                summary.blocked = summary.blocked.max(facts.meta.blocked);
                summary.quarantined = summary.quarantined.max(facts.meta.quarantined);
                summary.rejected = summary.rejected.max(facts.meta.rejected);
                if let Some(since) = facts.stalled_since_ms {
                    let stalled_for = now
                        .saturating_sub(since)
                        .saturating_add(rules::ms(cfg.stall_after));
                    summary.stalled_for_ms =
                        Some(summary.stalled_for_ms.unwrap_or(0).max(stalled_for));
                    if summary.culprit.is_none() {
                        summary.culprit = facts.culprit.as_ref().map(culprit_text);
                    }
                }
            }
            let error = match (&facts.error, &facts.culprit) {
                (Some(error), _) => Some(error.clone()),
                (None, Some(culprit)) => Some(culprit.clone()),
                (None, None) => None,
            };
            peers.push(FolderPeerSync {
                installation: peer.installation.clone(),
                endpoint: peer.endpoint.clone(),
                label: self.labels.get(&peer.installation).cloned(),
                direction: facts.direction,
                state: peer_state,
                metadata: facts.meta.clone(),
                objects_down: TransferProgress {
                    objects_remaining: facts.work.down_objects,
                    bytes_remaining: facts.work.down_bytes,
                    in_flight: in_flight.0,
                    rate_bps: pair.map_or(0, |pair| pair.down.short_bps()),
                    eta_s: rules::eta(facts.work.down_bytes, down_rate),
                    total_bytes: pair
                        .map_or(0, |pair| pair.down_episode)
                        .saturating_add(facts.work.down_bytes),
                    last_progress_ms: pair.and_then(|pair| pair.last_down_ms),
                },
                objects_up: TransferProgress {
                    objects_remaining: facts.work.up_objects,
                    bytes_remaining: facts.work.up_bytes,
                    in_flight: in_flight.1,
                    rate_bps: up_rate,
                    eta_s: rules::eta(facts.work.up_bytes, up_rate),
                    total_bytes: pair
                        .map_or(0, |pair| pair.up_episode)
                        .saturating_add(facts.work.up_bytes),
                    last_progress_ms: pair.and_then(|pair| pair.last_up_ms),
                },
                last_progress_ms: pair.and_then(|pair| pair.last_progress_ms),
                stalled_since_ms: facts.stalled_since_ms,
                backoff_until_ms: facts.backoff_until_ms,
                error,
            });
        }
        summary.down_rate_bps = down_rate;
        let inbox = hb.and_then(|hb| hb.inbox).unwrap_or_default();
        let folder_scope_errors = folder.errors.open.iter().filter(|((scope, _), open)| {
            matches!(scope, Scope::Folder { .. } | Scope::Mount { .. }) && open.visible(now)
        });
        let attention_error = folder_scope_errors
            .clone()
            .filter(|(_, open)| {
                matches!(
                    open.record.error.class,
                    ErrorClass::NeedsUserAction | ErrorClass::Fatal
                )
            })
            .max_by_key(|(_, open)| open.record.last_at_ms)
            .map(|(_, open)| open.record.error.to_string());
        let gate_kind = view.gate.kind();
        let inputs = FolderInputs {
            lifecycle: folder.phase,
            desired_running: record.desired_running,
            sync_enabled: record.sync_enabled,
            needs_attention: rules::needs_attention(
                folder.phase,
                gate_kind,
                &folder.mount.state,
                folder_scope_errors.map(|(_, open)| open.record.error.class),
            ),
            gate: gate_kind,
            peers: peers.iter().map(|peer| peer.state).collect(),
            local_blocked: inbox.blocked.saturating_add(inbox.quarantined),
        };
        let sync_state = rules::folder_state(&inputs);
        summary.lifecycle =
            (folder.phase != LifecyclePhase::Running).then(|| lifecycle_summary(&view.phase));
        summary.attention = attention_error.or_else(|| match &view.phase {
            FolderPhase::Failed { error } => Some(error.error.to_string()),
            _ => match &view.gate {
                GateState::Blocked { reason, .. } => {
                    Some(format!("initial sync blocked: {reason}"))
                }
                _ => folder.mount.state.error().map(ErrorWire::to_string),
            },
        });
        if let GateState::Awaiting {
            source, waiting_on, ..
        } = &view.gate
        {
            summary.gate_source = Some(
                self.peers
                    .get(source)
                    .and_then(|peer| peer.endpoint.clone())
                    .unwrap_or_else(|| source.clone()),
            );
            summary.waiting_on = waiting_on.clone();
        }
        summary.blocked = summary.blocked.max(inbox.blocked);
        summary.quarantined = summary.quarantined.max(inbox.quarantined);
        let text = rules::folder_summary(sync_state, &inputs, &summary);
        FolderStatus {
            state: record.state.clone(),
            label: record.label.clone(),
            repo: record.repo.clone(),
            device: record.device.clone(),
            lifecycle: lifecycle_status(folder, view, hb),
            intents: Intents {
                desired_running: record.desired_running,
                sync_enabled: record.sync_enabled,
                mount_enabled: record.mount_enabled,
            },
            mount: folder.mount.clone(),
            gate: gate_status(&view.gate),
            store: StoreStatus {
                health: hb.map_or(StoreHealth::Healthy, |hb| hb.store_health.clone()),
                pending_saves: hb.map_or(0, |hb| hb.pending_saves),
                unflushed_files: hb.map_or(0, |hb| hb.unflushed_files),
                open_handles: hb.map_or(0, |hb| hb.open_handles),
            },
            activation: ActivationStatus {
                inbox,
                counters: hb.and_then(|hb| hb.activation).unwrap_or_default(),
            },
            content: hb.and_then(|hb| hb.content).unwrap_or_default(),
            journal: hb.and_then(|hb| hb.journal.clone()).unwrap_or_default(),
            sync: FolderSync {
                state: sync_state,
                summary: text,
                peers,
            },
            errors: folder.errors.list(now, |_| true),
        }
    }

    fn build_peer(&self, peer: &PeerEntry, now: u64) -> PeerStatus {
        let lanes: Vec<LaneStatus> = peer
            .lanes
            .iter()
            .map(|(key, lane)| {
                let slot = if key.inbound { SLOT_IN } else { SLOT_OUT };
                let error_key = (
                    Scope::Lane {
                        installation: peer.installation.clone(),
                        lane: lane.lane,
                    },
                    slot.to_owned(),
                );
                LaneStatus {
                    lane: lane.lane,
                    role: lane.role,
                    direction: lane.direction,
                    state: lane.state,
                    since_ms: lane.since_ms,
                    inflight: lane.inflight.clone(),
                    bytes_up: lane.bytes_up,
                    bytes_down: lane.bytes_down,
                    rate_up_bps: lane.up.short_bps(),
                    rate_down_bps: lane.down.short_bps(),
                    reconnects: lane.reconnects,
                    backoff_until_ms: lane.backoff_until_ms,
                    error: self
                        .daemon_errors
                        .open
                        .get(&error_key)
                        .filter(|open| open.visible(now))
                        .map(|open| open.record.clone()),
                }
            })
            .collect();
        let peer_errors = self
            .daemon_errors
            .open_matching(|(scope, _)| scope_peer(scope) == Some(peer.installation.as_str()))
            .filter(|open| open.visible(now));
        let error = peer_errors
            .max_by_key(|open| {
                (
                    matches!(open.record.scope, Scope::Peer { .. }),
                    open.record.last_at_ms,
                )
            })
            .map(|open| open.record.clone());
        let shared_folders = self
            .folders
            .values()
            .filter_map(|folder| folder.view.as_ref())
            .filter(|view| {
                let pair = self
                    .pairs
                    .get(&(view.record.repo.clone(), peer.installation.clone()));
                peer.catalog_entry(&view.record.repo).is_some()
                    || pair.is_some_and(|pair| {
                        pair.inbound_seen || (peer.catalog.is_none() && pair.outbound_seen)
                    })
            })
            .map(|view| view.record.repo.clone())
            .collect();
        PeerStatus {
            installation: peer.installation.clone(),
            endpoint: peer.endpoint.clone(),
            label: self.labels.get(&peer.installation).cloned(),
            direction: peer.direction(),
            state: peer.conn,
            protocol: None,
            since_ms: peer.conn_since_ms,
            rtt_ms: peer.rtt_ms(),
            rate_up_bps: peer.up.short_bps(),
            rate_down_bps: peer.down.short_bps(),
            lanes,
            inbound_sessions: peer.inbound_sessions(),
            shared_folders,
            error,
        }
    }
}

fn lifecycle_status(
    folder: &FolderEntry,
    view: &FolderView,
    hb: Option<&WorkerHeartbeat>,
) -> LifecycleStatus {
    let (pid, instance, adopted) = match &view.phase {
        FolderPhase::Running {
            pid,
            instance,
            adopted,
            ..
        } => (Some(*pid), Some(instance.clone()), *adopted),
        FolderPhase::Starting { adopting, .. } => (
            view.worker.as_ref().map(|worker| worker.pid),
            view.worker.as_ref().map(|worker| worker.instance.clone()),
            *adopting,
        ),
        _ => (
            view.worker.as_ref().map(|worker| worker.pid),
            view.worker.as_ref().map(|worker| worker.instance.clone()),
            false,
        ),
    };
    LifecycleStatus {
        phase: folder.phase,
        detail: lifecycle_detail(&view.phase),
        since_ms: folder.phase_since_ms,
        pid,
        instance,
        adopted,
        restarts: view.restarts,
        next_retry_at_ms: match &view.phase {
            FolderPhase::Backoff { until_ms, .. } => Some(*until_ms),
            _ => None,
        },
        last_exit: folder.last_exit.clone(),
        degraded: view.degraded || hb.is_some_and(|hb| hb.engine.stalled),
        startup: match &view.phase {
            FolderPhase::Starting {
                phase,
                progress_seq,
                ..
            } => Some(StartupProgress {
                phase: phase.clone(),
                seq: *progress_seq,
            }),
            _ => None,
        },
    }
}

/// `ExchangeFinished` fields.
struct Exchange {
    installation: String,
    repo: String,
    lane: u32,
    op: String,
    direction: Direction,
    ok: bool,
    error: Option<ErrorWire>,
    summary: Option<ExchangeSummary>,
    bytes_up: u64,
    bytes_down: u64,
    objects_up: u64,
    objects_down: u64,
    duration_ms: u64,
}

fn meta_work(summary: &ExchangeSummary) -> u64 {
    summary
        .to_send
        .saturating_add(summary.to_receive)
        .saturating_add(summary.local_pending)
        .saturating_add(summary.remote_pending)
}

fn meta_work_of(meta: &MetaProgress) -> u64 {
    meta.to_send
        .saturating_add(meta.to_receive)
        .saturating_add(meta.local_pending)
        .saturating_add(meta.remote_pending)
}

/// Whether `op` moves object bytes (v3 `objects.*`, the interim
/// `objects.exchange`, or a legacy bulk op).
fn is_objects_op(op: &str) -> bool {
    op.starts_with("objects") || op.contains("bulk")
}

/// Objects ops in flight on a peer's outbound lanes for `repo`, as
/// `(down, up)` counts (derived from the lanes, so a dropped lane can never
/// leave a stale count behind).
fn objects_in_flight(peer: &PeerEntry, repo: &str) -> (u32, u32) {
    peer.lanes
        .iter()
        .filter(|(key, _)| !key.inbound)
        .flat_map(|(_, lane)| lane.inflight.iter())
        .filter(|op| op.repo == repo)
        .fold((0, 0), |(down, up), op| {
            let (d, u) = object_directions(&op.op);
            (down + u32::from(d), up + u32::from(u))
        })
}

/// `(down, up)`: which directions an objects op moves bytes in.
fn object_directions(op: &str) -> (bool, bool) {
    if !is_objects_op(op) {
        return (false, false);
    }
    if op.ends_with("pull") {
        (true, false)
    } else if op.ends_with("push") {
        (false, true)
    } else {
        (true, true)
    }
}

fn legacy_lane_role(lane: u32) -> LaneRole {
    if lane == 0 {
        LaneRole::Control
    } else {
        LaneRole::Bulk
    }
}

fn legacy_role(lane: u32) -> &'static str {
    legacy_lane_role(lane).as_str()
}

/// `tkfs-peer/3`.
fn transport() -> String {
    format!("{PROTOCOL_NAME}/{PROTOCOL_VERSION}")
}

fn update_legacy_repo(
    repo: &mut LegacyRepo,
    x: &Exchange,
    objects_op: bool,
    progress: bool,
    content: Option<ContentCounters>,
    remote_content: Option<ContentCounters>,
    now: u64,
) {
    let (sent_object_bytes, received_object_bytes) = if objects_op {
        (x.bytes_up, x.bytes_down)
    } else {
        (0, 0)
    };
    repo.transfer = Some(json!({
        "offer_ms": null,
        "remote_exchange_ms": null,
        "apply_ms": null,
        "total_ms": x.duration_ms,
        "sent_wire_bytes": x.bytes_up,
        "received_wire_bytes": x.bytes_down,
        "sent_object_bytes": sent_object_bytes,
        "received_object_bytes": received_object_bytes,
        "transport": transport(),
    }));
    repo.total_sent_wire_bytes = repo.total_sent_wire_bytes.saturating_add(x.bytes_up);
    repo.total_received_wire_bytes = repo.total_received_wire_bytes.saturating_add(x.bytes_down);
    repo.total_sent_object_bytes = repo
        .total_sent_object_bytes
        .saturating_add(sent_object_bytes);
    repo.total_received_object_bytes = repo
        .total_received_object_bytes
        .saturating_add(received_object_bytes);
    if objects_op {
        let local = content.unwrap_or_default();
        let remote = remote_content.unwrap_or_default();
        repo.object_inventory = Some(json!({
            "local_available": local.shared_available,
            "local_only": local.shared_available.saturating_sub(remote.shared_available),
            "local_partial": local.downloading,
            "remote_available": remote.shared_available,
            "remote_only": remote.shared_available.saturating_sub(local.shared_available),
            "remote_partial": remote.downloading,
        }));
    }
    if x.ok {
        repo.last_success_ms = Some(now);
        repo.error = None;
        repo.consecutive_failures = 0;
        repo.retry_at_ms = None;
        match &x.summary {
            Some(summary) => {
                repo.caught_up = summary.caught_up;
                repo.metadata_caught_up = summary.caught_up;
                repo.metadata_progress = Some(json!({
                    "local_events": summary.local_tip,
                    "remote_events": summary.remote_tip,
                    "local_only_events": summary.to_send,
                    "remote_only_events": summary.to_receive,
                    "local_pending": summary.local_pending,
                    "remote_pending": summary.remote_pending,
                }));
            }
            None if objects_op => {
                repo.metadata_caught_up = false;
                repo.caught_up = x.objects_up == 0 && x.objects_down == 0;
            }
            None => {}
        }
        if progress {
            repo.last_progress_ms = Some(now);
        }
    } else if let Some(error) = &x.error {
        // Contention is never a failure and never resets caught_up (§8).
        if error.class.counts_as_failure() {
            repo.consecutive_failures = repo.consecutive_failures.saturating_add(1);
            repo.caught_up = false;
            repo.metadata_caught_up = false;
        }
        repo.error = Some(error.to_string());
        repo.retry_at_ms = error.retry_after_ms.map(|ms| now.saturating_add(ms));
    }
}

/// A legacy repository status as JSON; a bulk lane's transfer names its
/// object lane `{index, count}` like the legacy bulk exchange did.
fn legacy_repo_value(repo: &LegacyRepo, lane: u32, pool: u32) -> Value {
    let mut value = serde_json::to_value(repo).unwrap_or(Value::Null);
    if lane > 0
        && let Some(transfer) = value.get_mut("transfer").and_then(Value::as_object_mut)
    {
        transfer.insert(
            "object_lane".into(),
            json!({"index": lane - 1, "count": pool.saturating_sub(1)}),
        );
    }
    value
}

/// Replaces every occurrence of a peer's pairing code in any string of
/// `value` (defence in depth: no producer should ever send one).
fn scrub_codes(value: &mut Value, peers: &[PeerRecord]) {
    let codes: Vec<&str> = peers
        .iter()
        .map(|peer| peer.code.as_str())
        .filter(|code| code.len() >= 4)
        .collect();
    if codes.is_empty() {
        return;
    }
    fn walk(value: &mut Value, codes: &[&str]) {
        match value {
            Value::String(text) => {
                for code in codes {
                    if text.contains(code) {
                        *text = text.replace(code, "<redacted>");
                    }
                }
            }
            Value::Array(items) => items.iter_mut().for_each(|item| walk(item, codes)),
            Value::Object(map) => map.values_mut().for_each(|item| walk(item, codes)),
            _ => {}
        }
    }
    walk(value, &codes);
}

fn gate_status(gate: &GateState) -> GateStatus {
    match gate {
        GateState::None => GateStatus::default(),
        GateState::Awaiting {
            source,
            since_ms,
            waiting_on,
        } => GateStatus {
            state: GateKind::Awaiting,
            source: Some(source.clone()),
            since_ms: Some(*since_ms),
            waiting_on: waiting_on.clone(),
        },
        GateState::Blocked { source, reason } => GateStatus {
            state: GateKind::Blocked,
            source: Some(source.clone()),
            since_ms: None,
            waiting_on: vec![reason.clone()],
        },
        GateState::Satisfied { at_ms } => GateStatus {
            state: GateKind::Satisfied,
            source: None,
            since_ms: Some(*at_ms),
            waiting_on: Vec::new(),
        },
        GateState::Overridden { at_ms } => GateStatus {
            state: GateKind::Overridden,
            source: None,
            since_ms: Some(*at_ms),
            waiting_on: Vec::new(),
        },
    }
}

fn phase_since(phase: &FolderPhase) -> Option<u64> {
    match phase {
        FolderPhase::Starting { since_ms, .. }
        | FolderPhase::Running { since_ms, .. }
        | FolderPhase::StopBlocked { since_ms, .. }
        | FolderPhase::Stopping { since_ms, .. } => Some(*since_ms),
        FolderPhase::Failed { error } => Some(error.last_at_ms),
        FolderPhase::Stopped | FolderPhase::Backoff { .. } => None,
    }
}

fn stop_stage(stage: StopStage) -> &'static str {
    match stage {
        StopStage::Requested => "stop requested",
        StopStage::AwaitExit => "waiting for exit",
        StopStage::Killing => "killing",
    }
}

/// Time-independent detail of a lifecycle phase (the UI computes countdowns
/// from `next_retry_at_ms`).
fn lifecycle_detail(phase: &FolderPhase) -> Option<String> {
    match phase {
        FolderPhase::Stopped => None,
        FolderPhase::Starting {
            phase, adopting, ..
        } => Some(if *adopting {
            "adopting the running worker".to_owned()
        } else {
            phase.clone()
        }),
        FolderPhase::Running { adopted, .. } => adopted.then(|| "adopted".to_owned()),
        FolderPhase::StopBlocked {
            reason,
            open_handles,
            unsaved,
            ..
        } => Some(format!(
            "stop blocked: {open_handles} open files, {unsaved} unsaved ({reason})"
        )),
        FolderPhase::Stopping { stage, force, .. } => Some(format!(
            "{}{}",
            stop_stage(*stage),
            if *force { " (force)" } else { "" }
        )),
        FolderPhase::Backoff {
            attempt, last_exit, ..
        } => Some(match (&last_exit.fatal, last_exit.code) {
            (Some(fatal), _) => format!("restart attempt {attempt} after {}", fatal.code),
            (None, Some(code)) => format!("restart attempt {attempt} after exit code {code}"),
            (None, None) => format!("restart attempt {attempt}"),
        }),
        FolderPhase::Failed { error } => Some(error.error.to_string()),
    }
}

fn lifecycle_summary(phase: &FolderPhase) -> String {
    match phase {
        FolderPhase::Stopped => "worker stopped".into(),
        FolderPhase::Starting { phase, .. } => format!("worker starting ({phase})"),
        FolderPhase::Running { .. } => "worker running".into(),
        FolderPhase::StopBlocked { .. } => "stop blocked by open files".into(),
        FolderPhase::Stopping { .. } => "worker stopping".into(),
        FolderPhase::Backoff { attempt, .. } => {
            format!("worker restarting (attempt {attempt})")
        }
        FolderPhase::Failed { error } => error.error.to_string(),
    }
}

fn lifecycle_message(phase: &FolderPhase) -> String {
    match lifecycle_detail(phase) {
        Some(detail) => format!("{}: {detail}", phase_name(phase.lifecycle())),
        None => phase_name(phase.lifecycle()).to_owned(),
    }
}

fn phase_name(phase: LifecyclePhase) -> &'static str {
    match phase {
        LifecyclePhase::Stopped => "stopped",
        LifecyclePhase::Starting => "starting",
        LifecyclePhase::Running => "running",
        LifecyclePhase::StopBlocked => "stop blocked",
        LifecyclePhase::Stopping => "stopping",
        LifecyclePhase::Backoff => "restarting",
        LifecyclePhase::Failed => "failed",
    }
}

fn gate_message(gate: &GateState) -> String {
    match gate {
        GateState::None => "gate cleared".into(),
        GateState::Awaiting { source, .. } => format!("waiting for initial sync from {source}"),
        GateState::Blocked { reason, .. } => format!("initial sync blocked: {reason}"),
        GateState::Satisfied { .. } => "initial sync complete".into(),
        GateState::Overridden { .. } => "initial sync overridden".into(),
    }
}

fn conn_name(conn: PeerConnState) -> &'static str {
    match conn {
        PeerConnState::Online => "online",
        PeerConnState::Connecting => "connecting",
        PeerConnState::Offline => "offline",
        PeerConnState::Backoff => "in backoff",
        PeerConnState::Incompatible => "needs upgrade",
        PeerConnState::Unauthorized => "unauthorized",
        PeerConnState::Revoked => "revoked",
    }
}

fn exchange_message(x: &Exchange) -> String {
    let mut parts = Vec::new();
    if let Some(summary) = &x.summary {
        if summary.received_events > 0 {
            parts.push(format!("received {} events", summary.received_events));
        }
        if summary.sent_events > 0 {
            parts.push(format!("sent {} events", summary.sent_events));
        }
        if summary.staged > 0 {
            parts.push(format!("staged {}", summary.staged));
        }
        if summary.rejected > 0 {
            parts.push(format!("rejected {}", summary.rejected));
        }
    }
    if x.objects_down > 0 {
        parts.push(format!("downloaded {} objects", x.objects_down));
    }
    if x.objects_up > 0 {
        parts.push(format!("uploaded {} objects", x.objects_up));
    }
    if parts.is_empty() {
        parts.push(format!(
            "{} ({} down, {} up)",
            x.op,
            rules::format_bytes(x.bytes_down),
            rules::format_bytes(x.bytes_up)
        ));
    }
    parts.join(", ")
}

/// `CODE subject` for a stall culprit: the object hash or event id when the
/// error is scoped to one.
fn culprit_text(record: &ErrorRecord) -> String {
    match &record.scope {
        Scope::Object { hash, .. } => format!("{} {hash}", record.error.code),
        Scope::Event { id, .. } => format!("{} event {id}", record.error.code),
        _ => record.error.to_string(),
    }
}
