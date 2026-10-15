//! Owned by WP1.11.
//!
//! The `StatusHub` behind [`StatusSink`] and [`StatusSource`] (REVAMP_PLAN
//! §3.4, §7): one cheap, versioned owner of everything the UI shows.
//!
//! * **Passive, two locks.** The derived state lives in a `Mutex<HubState>`
//!   (the *state* lock). [`record`] never takes it: it appends the event,
//!   stamped with the time it was recorded, to a small queue under a second
//!   lock (the *inbox*) that is never held across any real work. So a
//!   producer never waits for a snapshot rebuild or a tick, whatever the
//!   number of folders and peers (p99 < 50 µs; no I/O, no waiting, no durable
//!   write: invariant 30, status never blocks or writes). Whoever next needs
//!   the state (a reader, the tick, a setter, or `record` itself once
//!   [`MAX_PENDING_EVENTS`] are queued) applies the queued events in order,
//!   each at its recorded time, under the state lock.
//! * **Lazy snapshots.** [`snapshot`] returns an `Arc<StatusSnapshot>` that is
//!   rebuilt only when something changed, at most 4 times in any 1 s window
//!   (`policy::SNAPSHOT_MIN_INTERVAL` on average) and at most 2 back to back,
//!   so an isolated event is published at once while an event storm costs at
//!   most 4 rebuilds a second. [`wait_newer`] serves `subscribe` long polls;
//!   an unchanged hub serves the published snapshot from the inbox lock alone.
//! * **Tick.** [`tick`] (the caller runs it at `HubConfig::interval`, 1 Hz,
//!   see [`StatusHub::spawn_ticker`]) folds the EWMA rates (10 s and 60 s:
//!   per lane and per peer from every exchange's bytes, per (repo, peer)
//!   pair from objects ops only, as those are the object transfer rates and
//!   ETA; each exchange's bytes are spread over its duration,
//!   `rules::TransferRate`), runs the stall detector, applies the time-based
//!   error display policy (Contention after 30 s), resolves pair errors whose
//!   work is gone, expires stale inbound-incompatible notices and prunes
//!   state of folders that were removed or never registered.
//! * **Two clocks.** Reported `*_ms` fields are wall-clock time; every
//!   duration the hub measures (stall, Contention visibility, notice expiry,
//!   orphan grace) runs on a monotonic timeline (`Clock::instant`), so a
//!   system clock jump neither fires nor suppresses one (see `Now`).
//! * **Pure rules** live in [`super::hub_rules`].
//! * **Legacy projection.** [`legacy_folder_sync`] and [`legacy_peers`] build
//!   the §7.3 List/Inspect `sync` member and the Peers reply from the typed
//!   events, for use after the cutover (WP3.1).
//! * **Redaction** (invariant 21). No pairing code reaches the hub: no
//!   `StatusEvent` field carries one, and the typed snapshot is built from
//!   events only, so keeping codes out of error messages is the producers'
//!   duty. The legacy projection, which receives `PeerRecord`s, never
//!   serializes their `code`, never keeps them, and scrubs every code from
//!   any string it emits (defence in depth).
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
    self as rules, ErrorFlags, FolderInputs, PeerInputs, StallInput, Streak, SummaryFacts,
    TransferRate, Work,
};
use crate::{
    model::{
        errors::{ErrorClass, ErrorRecord, ErrorWire, Scope, code_in},
        health::{ContentCounters, MountIntent, MountState, MountStatus, StoreHealth},
        ipc::WorkerHeartbeat,
        orchestrator::{
            EntryState, ExitRecord, FolderPhase, FolderRecord, FolderView, GateState, PeerRecord,
            PublishedEntry, StatusSink, StatusSource, StopStage, limits,
        },
        protocol::{LaneRole, PROTOCOL_NAME, PROTOCOL_VERSION, feed::ExchangeSummary},
        status::{
            ActivationStatus, ActivityEntry, DaemonStatus, Direction, FolderPeerSync, FolderStatus,
            FolderSync, GateKind, GateStatus, InflightOp, Intents, LaneState, LaneStatus,
            LifecyclePhase, LifecycleStatus, MetaProgress, ObjectSource, PeerConnState, PeerStatus,
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
    net::{IpAddr, Ipv6Addr, SocketAddr},
    sync::{Arc, Condvar, Mutex, MutexGuard},
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
/// Snapshot rebuilds allowed in any window of [`SNAPSHOT_WINDOW`].
pub const SNAPSHOT_MAX_PER_WINDOW: usize = 4;
/// The sliding window of [`SNAPSHOT_MAX_PER_WINDOW`] (§3.4: at most 4 a second).
pub const SNAPSHOT_WINDOW: Duration = Duration::from_secs(1);
/// An inbound-incompatible notice is dropped after this long without a new
/// attempt from that host (the §8 Incompatible recheck period).
pub const INBOUND_INCOMPATIBLE_TTL: Duration = limits::INCOMPATIBLE_RECHECK;
/// Open inbound-incompatible notices kept (one per remote host); the least
/// recently seen is dropped beyond this. The notices come from
/// unauthenticated pre-auth traffic, so they must stay bounded (invariant 22).
pub const MAX_INBOUND_NOTICES: usize = 16;
/// Longest remote host name kept in an inbound notice slot.
pub const MAX_NOTICE_HOST: usize = 128;
/// Longest inbound-incompatible message kept.
pub const MAX_NOTICE_MESSAGE: usize = 512;
/// State of a folder the hub never saw registered (no `FolderLifecycle`), or
/// pair data of a repo without a registered folder, is dropped after this
/// long without news (it can only be shown once its folder is registered).
pub const ORPHAN_GRACE: Duration = Duration::from_secs(10);
/// Removed folder states remembered so that late events for them are
/// dropped instead of re-creating the folder.
pub const MAX_TOMBSTONES: usize = 64;
/// Recorded events queued before `record` applies them itself. Readers and
/// the 1 Hz tick normally apply them long before; this only bounds the queue
/// of a hub that nobody reads or ticks.
pub const MAX_PENDING_EVENTS: usize = 4096;
/// Initial capacity of the two event queues that alternate between the inbox
/// and the state (they keep what they grow to).
const QUEUE_CAPACITY: usize = 256;
/// A (folder, peer) error whose work is gone is resolved once it has not
/// been reported for this long (an implicit success, see `clear_idle_errors`):
/// an objects op with no work left in its direction, or any error of a peer
/// that no longer shares the folder. The wait covers the heartbeat's lag
/// behind the worker (content counters are refreshed at 1 Hz).
pub const WORK_GONE_GRACE: Duration = Duration::from_secs(10);
/// At most one `inbound_incompatible` activity entry per this period; the
/// next one says how many more hosts were seen meanwhile. The notices come
/// from unauthenticated traffic and must not flood the activity ring.
pub const NOTICE_ACTIVITY_INTERVAL: Duration = Duration::from_secs(60);

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
///
/// Lock order: `state`, then `inbox`; `inbox` is never held across a build, a
/// tick or an event application.
pub struct StatusHub {
    cfg: HubConfig,
    clock: Arc<dyn Clock>,
    /// The wall-clock time and the instant at construction: the origin of
    /// the monotonic timeline (`Now::mono`).
    origin: (u64, Instant),
    /// The derived state (the slow lock).
    state: Mutex<HubState>,
    /// Queued events, the dirty flag and the published snapshot (the fast
    /// lock).
    inbox: Mutex<Inbox>,
    /// Paired with `inbox`. When the hub turns dirty, one waiter is woken
    /// (it applies the events and rebuilds); a publish wakes them all.
    changed: Condvar,
}

/// What `record` and the readers share without the state lock.
#[derive(Debug)]
struct Inbox {
    /// Recorded events not applied yet, in record order, each with the time
    /// it was recorded.
    events: Vec<(Now, StatusEvent)>,
    /// Something changed since `published` was built (queued events count).
    dirty: bool,
    published: Arc<StatusSnapshot>,
    /// `wait_newer` callers blocked on `changed` (or woken and not yet back
    /// under the lock).
    waiting: usize,
}

/// One reading of the hub's two clocks.
///
/// * `ms` is wall-clock time. Every reported `*_ms` field uses it.
/// * `mono` is a monotonic millisecond timeline: the wall-clock time at hub
///   start plus the `Clock::instant` time elapsed since. Every duration the
///   hub measures uses it (stall, Contention visibility, notice expiry,
///   orphan grace), so a forward system clock jump (NTP, resume) cannot fire
///   a stall and a backward one cannot suppress it.
///
/// A timer kept on the monotonic timeline is reported as wall-clock time by
/// its age (`Now::wall_of`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Now {
    ms: u64,
    mono: u64,
}

impl Now {
    /// The wall-clock time of the monotonic instant `mono`: now minus its age.
    fn wall_of(self, mono: u64) -> u64 {
        self.ms.saturating_sub(self.mono.saturating_sub(mono))
    }

    /// The monotonic instant of the wall-clock time `ms`: now minus its age.
    fn mono_of(self, ms: u64) -> u64 {
        self.mono.saturating_sub(self.ms.saturating_sub(ms))
    }
}

impl std::fmt::Debug for StatusHub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = lock(&self.state);
        let (pending, dirty) = {
            let inbox = lock(&self.inbox);
            (inbox.events.len(), inbox.dirty)
        };
        f.debug_struct("StatusHub")
            .field("cfg", &self.cfg)
            .field("generation", &state.generation)
            .field("dirty", &dirty)
            .field("pending", &pending)
            .field("folders", &state.folders.len())
            .field("peers", &state.peers.len())
            .finish_non_exhaustive()
    }
}

impl StatusHub {
    pub fn new(cfg: HubConfig, clock: Arc<dyn Clock>) -> Arc<StatusHub> {
        let origin = (clock.now_ms(), clock.instant());
        let now = Now {
            ms: origin.0,
            mono: origin.0,
        };
        let state = HubState::new(&cfg, now);
        let published = Arc::new(state.build(now, &cfg, 0));
        Arc::new(StatusHub {
            cfg,
            clock,
            origin,
            state: Mutex::new(state),
            inbox: Mutex::new(Inbox {
                events: Vec::with_capacity(QUEUE_CAPACITY),
                dirty: false,
                published,
                waiting: 0,
            }),
            changed: Condvar::new(),
        })
    }

    pub fn config(&self) -> &HubConfig {
        &self.cfg
    }

    /// Reads both clocks (see [`Now`]).
    fn now(&self) -> Now {
        let elapsed = self
            .clock
            .instant()
            .saturating_duration_since(self.origin.1);
        Now {
            ms: self.clock.now_ms(),
            mono: self.origin.0.saturating_add(rules::ms(elapsed)),
        }
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
        let dirty = lock(&self.inbox).dirty;
        if dirty {
            self.rebuild(&mut state);
        }
        drop(state);
        self.published()
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
        let mut state = lock(&self.state);
        self.drain(&mut state);
        state.legacy_links.clone()
    }

    /// How many snapshots have been built (diagnostics and tests).
    pub fn rebuilds(&self) -> u64 {
        lock(&self.state).rebuilds
    }

    /// Recorded events not applied yet (diagnostics and tests).
    pub fn pending(&self) -> usize {
        lock(&self.inbox).events.len()
    }

    /// The §7.3 `sync` member of List/Inspect for `folder` after the cutover:
    /// one entry per (outbound) peer in `peers`, in order, with the legacy
    /// keys `endpoint`, `connected`, `remote_available`, `error`, `metadata`
    /// and `lanes[{lane, role, connected, transport, parallel_bulk, error,
    /// repository}]`, where `metadata` and `repository` carry every legacy
    /// `RepositoryStatus` key.
    pub fn legacy_folder_sync(&self, folder: &FolderRecord, peers: &[PeerRecord]) -> Value {
        let mut state = lock(&self.state);
        self.drain(&mut state);
        let mut out = Vec::with_capacity(peers.len());
        for record in peers {
            let peer = state.find_peer(record);
            let pool = record.connections.max(limits::MIN_POOL);
            let lane0 = peer.and_then(|peer| peer.lanes.get(&LaneKey::outbound(0)));
            let connected = lane0.is_some_and(|lane| lane.state == LaneState::Ready);
            // Legacy published lists held only Running workers, so a remote
            // folder that is starting, paused or unavailable reads `false`.
            let remote_available = if connected {
                peer.and_then(|peer| peer.catalog.as_ref()).map(|catalog| {
                    catalog.iter().any(|entry| {
                        entry.repo == folder.repo && entry.state == EntryState::Running
                    })
                })
            } else {
                None
            };
            let lane_error =
                |lane: u32| peer.and_then(|peer| state.lane_error_text(&peer.installation, lane));
            let live = state.legacy_live(&folder.repo, peer);
            let repository = |lane: u32| {
                let entry = peer.and_then(|peer| peer.lanes.get(&LaneKey::outbound(lane)))?;
                match entry.repos.get(&folder.repo) {
                    Some(repo) => Some(legacy_repo_value(repo, lane, pool, &live)),
                    None if lane > 0 && live.shared => Some(legacy_repo_value(
                        &LegacyRepo::idle_bulk(),
                        lane,
                        pool,
                        &live,
                    )),
                    None => None,
                }
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
        let mut state = lock(&self.state);
        self.drain(&mut state);
        let mut out = Vec::with_capacity(peers.len());
        for record in peers {
            let peer = state.find_peer(record);
            let pool = record.connections.max(limits::MIN_POOL);
            let lanes: Vec<Value> = (0..pool)
                .map(|lane| {
                    let entry = peer.and_then(|peer| peer.lanes.get(&LaneKey::outbound(lane)));
                    let mut repositories: Map<String, Value> = entry
                        .map(|entry| {
                            entry
                                .repos
                                .iter()
                                .map(|(repo, status)| {
                                    let live = state.legacy_live(repo, peer);
                                    (repo.clone(), legacy_repo_value(status, lane, pool, &live))
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    // A bulk lane serves every repository both sides hold,
                    // whether or not objects moved on it yet (6298d0b's bulk
                    // lanes exchanged each one every round).
                    if lane > 0
                        && entry.is_some()
                        && let Some(catalog) = peer.and_then(|peer| peer.catalog.as_ref())
                    {
                        for published in catalog {
                            if repositories.contains_key(&published.repo)
                                || !state.repos.contains_key(&published.repo)
                            {
                                continue;
                            }
                            let live = state.legacy_live(&published.repo, peer);
                            repositories.insert(
                                published.repo.clone(),
                                legacy_repo_value(&LegacyRepo::idle_bulk(), lane, pool, &live),
                            );
                        }
                    }
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

    /// Applies the queued events, then `change`, under the state lock; when
    /// `change` reports a change, the hub turns dirty.
    fn mutate(&self, change: impl FnOnce(&mut HubState, Now, &HubConfig) -> bool) {
        let mut state = lock(&self.state);
        self.drain(&mut state);
        let now = self.now();
        if change(&mut state, now, &self.cfg) {
            self.mark_dirty();
        }
    }

    /// Marks the published snapshot stale; wakes a waiter if it was current.
    fn mark_dirty(&self) {
        let wake = {
            let mut inbox = lock(&self.inbox);
            !std::mem::replace(&mut inbox.dirty, true) && inbox.waiting > 0
        };
        if wake {
            self.changed.notify_one();
        }
    }

    /// The published snapshot, for a `wait_newer` caller that returns. The
    /// hub wakes a single waiter when it turns dirty; if it is still dirty,
    /// that wake-up may have been this caller's, so it is passed on.
    fn leave(&self, inbox: MutexGuard<'_, Inbox>) -> Arc<StatusSnapshot> {
        let snapshot = inbox.published.clone();
        let pass = inbox.dirty && inbox.waiting > 0;
        drop(inbox);
        if pass {
            self.changed.notify_one();
        }
        snapshot
    }

    /// The published snapshot.
    fn published(&self) -> Arc<StatusSnapshot> {
        lock(&self.inbox).published.clone()
    }

    /// Applies the queued events in record order, each at its recorded time.
    /// The caller holds the state lock, so only one thread drains at a time.
    fn drain(&self, state: &mut HubState) {
        self.take_events(state, false);
        state.apply_pending();
    }

    /// Moves the queued events into `state.pending` (an empty buffer that
    /// keeps its capacity, so `record` does not reallocate in steady state);
    /// with `clean`, also clears `dirty` in the same critical section.
    fn take_events(&self, state: &mut HubState, clean: bool) {
        let mut inbox = lock(&self.inbox);
        if clean {
            inbox.dirty = false;
        }
        std::mem::swap(&mut inbox.events, &mut state.pending);
    }

    /// Applies the queued events; then, if the hub is dirty and the throttle
    /// has a slot, rebuilds and publishes the snapshot. Returns how long
    /// until the next slot when it had none (`None` when the hub was clean or
    /// the rebuild happened).
    fn refresh(&self, state: &mut HubState) -> Option<Duration> {
        self.drain(state);
        let dirty = lock(&self.inbox).dirty;
        if !dirty {
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

    /// Builds and publishes a snapshot. Only the state lock is held while
    /// building, so `record` never waits for it.
    fn rebuild(&self, state: &mut HubState) {
        // Taking the queued events and clearing `dirty` together: anything
        // recorded from here on makes the hub dirty again.
        self.take_events(state, true);
        state.apply_pending();
        let now = self.now();
        let generation = state.generation.wrapping_add(1);
        state.generation = generation;
        let snapshot = Arc::new(state.build(now, &self.cfg, generation));
        state.rebuilds = state.rebuilds.wrapping_add(1);
        let previous = std::mem::replace(&mut lock(&self.inbox).published, snapshot);
        self.changed.notify_all();
        // The previous snapshot may be the last reference: free it outside
        // the inbox lock.
        drop(previous);
    }
}

impl StatusSink for StatusHub {
    /// Queues the event with the time it was recorded: only the inbox lock is
    /// taken, so this never waits for a rebuild, a tick or another reader.
    fn record(&self, event: StatusEvent) {
        let mut inbox = lock(&self.inbox);
        // Read under the lock, so queued events are in time order.
        let now = self.now();
        inbox.events.push((now, event));
        let wake = !std::mem::replace(&mut inbox.dirty, true) && inbox.waiting > 0;
        let full = inbox.events.len() >= MAX_PENDING_EVENTS;
        drop(inbox);
        if wake {
            // One waiter is enough: it applies the events and rebuilds, and
            // the publish wakes the others. Waking every subscriber here
            // would cost the producer one wake-up per subscriber.
            self.changed.notify_one();
        }
        if full {
            // Nobody applied the queue for a while (no reader, no tick):
            // apply it here so that it stays bounded.
            let mut state = lock(&self.state);
            self.drain(&mut state);
        }
    }
}

impl StatusSource for StatusHub {
    fn snapshot(&self) -> Arc<StatusSnapshot> {
        {
            let inbox = lock(&self.inbox);
            if !inbox.dirty {
                return inbox.published.clone();
            }
        }
        let mut state = lock(&self.state);
        self.refresh(&mut state);
        drop(state);
        self.published()
    }

    /// Returns as soon as a snapshot newer than `generation` exists, or the
    /// current one when `timeout` elapses. A `generation` ahead of the hub
    /// (from a previous daemon run) returns the current snapshot at once.
    ///
    /// No wake-up is lost:
    ///
    /// * the dirty flag and the published generation are checked under the
    ///   inbox lock right before waiting on `changed` (paired with it), and
    ///   whoever changes them does so under that lock and signals after;
    /// * when the hub turns dirty, one waiter is woken (if any waits). It
    ///   applies the events and rebuilds, or waits for the throttle's slot
    ///   on a timer and rebuilds then, and the publish wakes everyone. A
    ///   waiter that returns while the hub is still dirty passes the wake-up
    ///   on (`leave`), so some waiter always owns a dirty spell.
    fn wait_newer(&self, generation: u64, timeout: Duration) -> Arc<StatusSnapshot> {
        let deadline = Instant::now().checked_add(timeout);
        let mut throttled: Option<Duration> = None;
        loop {
            let mut inbox = lock(&self.inbox);
            // Newer, or a generation from a previous daemon run (ahead of us).
            if inbox.published.generation != generation {
                return self.leave(inbox);
            }
            if inbox.dirty && throttled.is_none() {
                // Something changed: apply it and rebuild, or learn how long
                // the throttle wants us to wait.
                drop(inbox);
                let mut state = lock(&self.state);
                throttled = self.refresh(&mut state);
                continue;
            }
            let remaining = match deadline {
                Some(deadline) => deadline.saturating_duration_since(Instant::now()),
                None => Duration::from_secs(3600),
            };
            if remaining.is_zero() {
                return self.leave(inbox);
            }
            // While dirty, the throttle's slot is the deadline (`record` does
            // not signal a hub that is already dirty); otherwise wait for a
            // change or a publish.
            let wait = match throttled.take() {
                Some(wait) if inbox.dirty => wait.min(remaining),
                _ => remaining,
            }
            .max(Duration::from_millis(1));
            inbox.waiting += 1;
            let (mut inbox, _) = self
                .changed
                .wait_timeout(inbox, wait)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            inbox.waiting -= 1;
        }
    }
}

// ---------------------------------------------------------------------------
// State

/// Snapshot rebuild limiter: a token bucket (one token per
/// `policy::SNAPSHOT_MIN_INTERVAL`, burst [`SNAPSHOT_BURST`]) under a sliding
/// window that allows at most [`SNAPSHOT_MAX_PER_WINDOW`] rebuilds in any
/// [`SNAPSHOT_WINDOW`] (§3.4: at most 4 a second).
#[derive(Debug)]
struct Throttle {
    tokens: f64,
    at: Option<Instant>,
    /// The latest rebuilds, oldest first (at most `SNAPSHOT_MAX_PER_WINDOW`).
    recent: VecDeque<Instant>,
}

impl Throttle {
    fn new() -> Self {
        Self {
            tokens: SNAPSHOT_BURST,
            at: None,
            recent: VecDeque::with_capacity(SNAPSHOT_MAX_PER_WINDOW + 1),
        }
    }

    /// Takes a rebuild slot, or says how long until the next one.
    fn take(&mut self, now: Instant) -> Result<(), Duration> {
        let period = policy::SNAPSHOT_MIN_INTERVAL.as_secs_f64();
        if let Some(at) = self.at {
            let refill = now.saturating_duration_since(at).as_secs_f64() / period;
            self.tokens = (self.tokens + refill).min(SNAPSHOT_BURST);
        }
        self.at = Some(now);
        if self.tokens < 1.0 {
            return Err(Duration::from_secs_f64((1.0 - self.tokens) * period));
        }
        if self.recent.len() >= SNAPSHOT_MAX_PER_WINDOW
            && let Some(oldest) = self.recent.front()
        {
            let age = now.saturating_duration_since(*oldest);
            if age < SNAPSHOT_WINDOW {
                return Err(SNAPSHOT_WINDOW - age);
            }
        }
        self.tokens -= 1.0;
        self.recent.push_back(now);
        while self.recent.len() > SNAPSHOT_MAX_PER_WINDOW {
            self.recent.pop_front();
        }
        Ok(())
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
    /// The display streak, on the monotonic timeline (`Now::mono`).
    streak: Streak,
    /// Shown at least once (sticky until resolved); only shown errors reach
    /// the history.
    shown: bool,
    /// When this slot was last reported, on the monotonic timeline.
    seen: u64,
}

impl OpenError {
    fn visible(&self, now: Now) -> bool {
        self.shown
            || rules::error_visible(
                self.record.error.class,
                &self.record.error.code,
                self.streak,
                now.mono,
            )
    }
}

/// `(scope, slot)`: one open error per slot. Generic `Error` events use the
/// empty slot; exchanges use their op (`in:op` when the peer initiated it,
/// see `exchange_slot`); mirrored records use a fixed name.
type SlotKey = (Scope, String);

/// Slot-name prefix of the inbound-incompatible notices (Daemon scope).
const INBOUND_SLOT: &str = "inbound:";

/// Open errors and the resolved history of one owner (the daemon, or one
/// folder).
#[derive(Debug, Default)]
struct ErrorBook {
    open: BTreeMap<SlotKey, OpenError>,
    history: VecDeque<Past>,
}

/// A history entry.
#[derive(Debug)]
struct Past {
    record: ErrorRecord,
    /// An expired inbound-incompatible notice: at most one is kept (see
    /// `ErrorBook::expire_notices`).
    notice: bool,
}

impl ErrorBook {
    /// Records one occurrence (deduplicated per slot).
    fn observe(&mut self, key: SlotKey, error: ErrorWire, now: Now, ring: &mut Ring) {
        let previous = self.open.remove(&key);
        let streak = previous
            .as_ref()
            .map_or(Streak::start(now.mono), |open| open.streak.bump());
        let was_shown = previous.as_ref().is_some_and(|open| open.shown);
        let (record, superseded) = rules::dedupe(
            previous.map(|open| open.record),
            error,
            key.0.clone(),
            now.ms,
        );
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
            seen: now.mono,
        };
        show_if_visible(&mut open, now, ring);
        self.open.insert(key, open);
    }

    /// The record of a persistent condition its owner re-reports as a state
    /// rather than as counted attempts (store health, a lost mount): the same
    /// open code is one deduplicated record whose `count` rises with every
    /// report, so the display policy counts reports like attempts (a
    /// Transient condition shows from its 3rd report on).
    fn condition(&self, key: &SlotKey, error: &ErrorWire, now: Now) -> ErrorRecord {
        match self.open.get(key) {
            Some(open) if open.record.error.code == error.code => {
                let mut record = open.record.clone();
                record.observe(error.clone(), now.ms);
                record
            }
            _ => ErrorRecord::new(error.clone(), key.0.clone(), now.ms),
        }
    }

    /// Mirrors a record owned by someone else (a FolderView error, a mount or
    /// store-health error): `None` resolves the slot.
    fn mirror(&mut self, key: SlotKey, record: Option<ErrorRecord>, now: Now, ring: &mut Ring) {
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
        let mut since = None;
        if let Some(previous) = previous {
            if previous.record.error.code == record.error.code {
                shown = previous.shown;
                since = Some(previous.streak.since_ms);
            } else if previous.shown {
                let mut old = previous.record;
                old.resolve(now.ms);
                self.push_history(old);
            }
        }
        // The owner's wall-clock `first_at_ms` enters the monotonic timeline
        // by its age.
        let streak = Streak {
            failures: record.count.max(1),
            since_ms: since.unwrap_or_else(|| now.mono_of(record.first_at_ms)),
        };
        let mut open = OpenError {
            record,
            streak,
            shown,
            seen: now.mono,
        };
        show_if_visible(&mut open, now, ring);
        self.open.insert(key, open);
    }

    /// Resolves every open slot matching `pred`; shown ones move to history.
    fn resolve(
        &mut self,
        pred: impl Fn(&SlotKey, &OpenError) -> bool,
        now: Now,
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
                record.resolve(now.ms);
                ring.push(
                    now.ms,
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
    fn refresh(&mut self, now: Now, ring: &mut Ring) -> bool {
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

    /// The open inbound-incompatible notice slots (a key range, so this
    /// never walks the other daemon errors).
    fn inbound_notices(&self) -> impl Iterator<Item = (&SlotKey, &OpenError)> {
        self.open
            .range((Scope::Daemon, INBOUND_SLOT.to_owned())..)
            .take_while(|((scope, slot), _)| {
                *scope == Scope::Daemon && slot.starts_with(INBOUND_SLOT)
            })
    }

    /// Drops the least recently seen inbound notices beyond
    /// [`MAX_INBOUND_NOTICES`] (not into the history: they are noise from
    /// unauthenticated traffic, and the history is for real errors).
    fn cap_inbound_notices(&mut self) {
        loop {
            let mut count = 0;
            let mut oldest: Option<(u64, &SlotKey)> = None;
            for (key, open) in self.inbound_notices() {
                count += 1;
                if oldest.is_none_or(|(seen, _)| open.seen < seen) {
                    oldest = Some((open.seen, key));
                }
            }
            match oldest {
                Some((_, key)) if count > MAX_INBOUND_NOTICES => {
                    let key = key.clone();
                    self.open.remove(&key);
                }
                _ => return,
            }
        }
    }

    /// Drops the inbound notices whose host has not tried again for `ttl`
    /// (monotonic). This is not a resolution: no `resolved` activity entry,
    /// and only the newest shown one is kept in the history, replacing the
    /// notice kept there before, so unauthenticated traffic holds at most
    /// one history entry.
    fn expire_notices(&mut self, now: Now, ttl: u64) -> bool {
        let expired: Vec<SlotKey> = self
            .inbound_notices()
            .filter(|(_, open)| now.mono.saturating_sub(open.seen) >= ttl)
            .map(|(key, _)| key.clone())
            .collect();
        let mut newest: Option<ErrorRecord> = None;
        for key in &expired {
            if let Some(open) = self.open.remove(key)
                && open.shown
                && newest
                    .as_ref()
                    .is_none_or(|kept| open.record.last_at_ms >= kept.last_at_ms)
            {
                newest = Some(open.record);
            }
        }
        if let Some(mut record) = newest {
            record.resolve(now.ms);
            self.history.retain(|past| !past.notice);
            self.push_past(Past {
                record,
                notice: true,
            });
        }
        !expired.is_empty()
    }

    fn push_history(&mut self, record: ErrorRecord) {
        self.push_past(Past {
            record,
            notice: false,
        });
    }

    fn push_past(&mut self, past: Past) {
        while self.history.len() >= policy::ERROR_HISTORY {
            self.history.pop_front();
        }
        self.history.push_back(past);
    }

    /// Visible open errors (newest first), then the history (newest first).
    fn list(&self, now: Now, filter: impl Fn(&Scope) -> bool) -> Vec<ErrorRecord> {
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
                    .map(|past| &past.record)
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

fn show_if_visible(open: &mut OpenError, now: Now, ring: &mut Ring) {
    if !open.shown && open.visible(now) {
        announce(open, now, ring);
    }
}

fn announce(open: &mut OpenError, now: Now, ring: &mut Ring) {
    open.shown = true;
    let record = &open.record;
    ring.push(
        now.ms,
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
    /// Objects whose transfer failed (`ObjectError` with a source, WP3.INT):
    /// hash → when it last failed (`Now::ms`). Their errors are resolved by
    /// the lanes (`Resolved` once installed or pushed); the heartbeat's
    /// implicit clear (`on_heartbeat`) reaches them only once the folder has
    /// no staged work and [`SOURCED_GRACE_MS`] passed since the failure, so a
    /// heartbeat whose counters predate the object's event never clears it.
    sourced: BTreeMap<String, u64>,
    /// When the entry was created (monotonic): a folder that is never
    /// registered (no view) is pruned [`ORPHAN_GRACE`] later.
    created: u64,
}

/// See [`FolderEntry::sourced`]: objects flow before activation (§6.4), so a
/// transfer can fail while the content counters (committed references only,
/// refreshed in the background, §4.5) still say nothing is missing.
pub const SOURCED_GRACE_MS: u64 = 10_000;

impl FolderEntry {
    fn new(now: Now) -> Self {
        Self {
            created: now.mono,
            ..Self::default()
        }
    }

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
    /// Objects whose transfer on this lane failed (`ObjectError` with a
    /// source, CCR-WP3.INT-1). The projection shows them as the repository's
    /// `error`, `consecutive_failures` and `retry_at_ms` (as 6298d0b's bulk
    /// lanes reported a failed object) while the folder still has an open
    /// error for the object (`legacy_repo_value`).
    #[serde(skip)]
    objects: BTreeMap<String, LegacyObjectFailure>,
}

impl LegacyRepo {
    /// The status of a repository a bulk lane serves but has not moved
    /// objects of yet (WP3.INT): v3 bulk lanes report an exchange only when
    /// objects move, where 6298d0b's exchanged every repository each round.
    fn idle_bulk() -> Self {
        Self {
            transfer: Some(json!({
                "offer_ms": null,
                "remote_exchange_ms": null,
                "apply_ms": null,
                "total_ms": null,
                "sent_wire_bytes": 0,
                "received_wire_bytes": 0,
                "sent_object_bytes": 0,
                "received_object_bytes": 0,
                "transport": transport(),
            })),
            object_inventory: Some(json!({
                "local_available": 0,
                "local_only": 0,
                "local_partial": 0,
                "remote_available": 0,
                "remote_only": 0,
                "remote_partial": 0,
            })),
            ..Self::default()
        }
    }
}

/// One failed object of a [`LegacyRepo`].
#[derive(Clone, Debug)]
struct LegacyObjectFailure {
    text: String,
    attempts: u32,
    at_ms: u64,
    retry_at_ms: Option<u64>,
}

/// Failed objects a [`LegacyRepo`] keeps (the oldest goes first).
const MAX_LEGACY_OBJECT_FAILURES: usize = 64;

/// What the legacy projection of one repository reads at projection time
/// rather than from the exchange that booked it: the folder (its content
/// counters and its open object errors) and the peer's published content.
struct LegacyLive<'a> {
    state: &'a str,
    folder: Option<&'a FolderEntry>,
    remote: Option<ContentCounters>,
    /// The peer's catalog lists the repository and we hold a folder of it.
    shared: bool,
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
    up: TransferRate,
    down: TransferRate,
    reconnects: u32,
    ever_ready: bool,
    backoff_until_ms: Option<u64>,
    rtt_ms: Option<u64>,
    /// The negotiated protocol while `Ready` (`LaneState.protocol`,
    /// CCR-WP1.11-2); cleared when the lane leaves `Ready`.
    protocol: Option<String>,
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
            up: TransferRate::new(),
            down: TransferRate::new(),
            reconnects: 0,
            ever_ready: false,
            backoff_until_ms: None,
            rtt_ms: None,
            protocol: None,
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

/// The members of one `StatusEvent::LaneState`.
struct LaneUpdate {
    lane: u32,
    role: LaneRole,
    direction: Direction,
    state: LaneState,
    error: Option<ErrorWire>,
    backoff_until_ms: Option<u64>,
    rtt_ms: Option<u64>,
    protocol: Option<String>,
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
    up: TransferRate,
    down: TransferRate,
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

    /// The negotiated protocol (CCR-WP1.11-2): the outbound control lane's
    /// while it is `Ready`, else that of any `Ready` lane.
    fn protocol(&self) -> Option<String> {
        let ready = |lane: &&LaneEntry| lane.state == LaneState::Ready && lane.protocol.is_some();
        self.lanes
            .get(&LaneKey::outbound(0))
            .filter(ready)
            .or_else(|| self.lanes.values().find(ready))
            .and_then(|lane| lane.protocol.clone())
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
    /// When the summary's exchange was recorded (its end), and when it began
    /// (the end minus its duration), wall-clock.
    last_meta_exchange_ms: Option<u64>,
    meta_exchange_started_ms: Option<u64>,
    /// Object transfer rates (bytes of objects ops only), shown as
    /// `objects_up`/`objects_down` rates and used for their ETA.
    up: TransferRate,
    down: TransferRate,
    /// Last progress (monotonic; reported by its age).
    last_progress: Option<u64>,
    last_down_ms: Option<u64>,
    last_up_ms: Option<u64>,
    /// Remaining work at the last observation (progress = a decrease).
    work: Option<Work>,
    /// Since when the pair can progress and has work (monotonic).
    eligible_since: Option<u64>,
    /// When the current stall began (monotonic).
    stalled_since: Option<u64>,
    /// Object bytes moved in the current transfer episode (reset when the
    /// remaining work reaches zero); `total_bytes = episode + remaining`.
    down_episode: u64,
    up_episode: u64,
    /// The pair's last exchange event (monotonic): a pair whose repo has no
    /// registered folder is pruned [`ORPHAN_GRACE`] later.
    touched: u64,
    /// The last exchange in which the peer moved objects to us (monotonic):
    /// the evidence that a peer without a published entry supplies
    /// (`rules::may_supply`).
    supplied: Option<u64>,
    /// Download work was attributed to the peer at the last tick (a change
    /// of attribution is not progress).
    supplying: bool,
}

type PairKey = (String, String);

struct HubState {
    /// The generation of the last snapshot built.
    generation: u64,
    /// Events taken from the inbox, being applied; empty otherwise. It swaps
    /// with the inbox queue, so both keep their capacity.
    pending: Vec<(Now, StatusEvent)>,
    throttle: Throttle,
    rebuilds: u64,
    last_tick: Option<Instant>,
    /// The monotonic time the rates were last folded up to (the previous
    /// tick), so each tick folds exactly the stretch since.
    rates_at: Option<u64>,
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
    /// Recently removed folder states (oldest first): late events for them
    /// are dropped instead of re-creating the folder.
    removed: VecDeque<String>,
    /// When the last `inbound_incompatible` activity entry was added
    /// (monotonic), and how many new notices were not logged since.
    notice_logged: Option<u64>,
    notices_unlogged: u64,
}

/// Everything derived for one (folder, peer) pair at one instant.
struct PairFacts {
    inputs: PeerInputs,
    work: Work,
    /// Download work may be attributed to the peer (`rules::may_supply`).
    supplies: bool,
    meta: MetaProgress,
    direction: Direction,
    /// When the stall began (monotonic).
    stalled_since: Option<u64>,
    backoff_until_ms: Option<u64>,
    error: Option<ErrorRecord>,
    culprit: Option<ErrorRecord>,
}

impl HubState {
    fn new(cfg: &HubConfig, now: Now) -> Self {
        Self {
            generation: 0,
            pending: Vec::with_capacity(QUEUE_CAPACITY),
            throttle: Throttle::new(),
            rebuilds: 0,
            last_tick: None,
            rates_at: None,
            // The daemon sets its identity (installation, version, endpoints)
            // through `StatusHub::set_daemon`.
            daemon: DaemonStatus {
                pid: std::process::id(),
                started_at_ms: now.ms,
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
            removed: VecDeque::new(),
            notice_logged: None,
            notices_unlogged: 0,
        }
    }

    /// Applies the events taken from the inbox, in order, keeping the
    /// buffer's capacity.
    fn apply_pending(&mut self) {
        let mut pending = std::mem::take(&mut self.pending);
        for (now, event) in pending.drain(..) {
            self.apply(event, now);
        }
        self.pending = pending;
    }

    /// The (repo, peer) pair, marked as touched now.
    fn pair_mut(&mut self, repo: String, installation: String, now: Now) -> &mut PairEntry {
        let pair = self.pairs.entry((repo, installation)).or_default();
        pair.touched = now.mono;
        pair
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

    /// Books a failed object transfer on its lane's legacy repository status
    /// (CCR-WP3.INT-1). Contention is never a failure (§8); a peer the hub
    /// no longer lists is not recreated.
    fn legacy_object_failed(
        &mut self,
        source: &ObjectSource,
        repo: &str,
        hash: &str,
        error: &ErrorWire,
        now: Now,
    ) {
        if !error.class.counts_as_failure() {
            return;
        }
        let Some(peer) = self.peers.get_mut(&source.installation) else {
            return;
        };
        let lane = peer
            .lanes
            .entry(LaneKey::of(source.direction, source.lane))
            .or_insert_with(|| {
                LaneEntry::new(source.lane, legacy_lane_role(source.lane), source.direction)
            });
        let legacy = lane.repos.entry(repo.to_owned()).or_default();
        let attempts = legacy
            .objects
            .get(hash)
            .map_or(0, |failure| failure.attempts);
        if attempts == 0
            && legacy.objects.len() >= MAX_LEGACY_OBJECT_FAILURES
            && let Some(oldest) = legacy
                .objects
                .iter()
                .min_by_key(|(_, failure)| failure.at_ms)
                .map(|(hash, _)| hash.clone())
        {
            legacy.objects.remove(&oldest);
        }
        legacy.objects.insert(
            hash.to_owned(),
            LegacyObjectFailure {
                text: error.to_string(),
                attempts: attempts.saturating_add(1),
                at_ms: now.ms,
                retry_at_ms: error.retry_after_ms.map(|ms| now.ms.saturating_add(ms)),
            },
        );
    }

    /// The live inputs of `repo`'s legacy status for a peer (`LegacyLive`).
    fn legacy_live<'a>(&'a self, repo: &str, peer: Option<&PeerEntry>) -> LegacyLive<'a> {
        let state = self.repos.get(repo).map_or("", String::as_str);
        let published = peer.and_then(|peer| peer.catalog_entry(repo));
        LegacyLive {
            state,
            folder: self.folders.get(state),
            remote: published.and_then(|entry| entry.content),
            shared: published.is_some() && self.repos.contains_key(repo),
        }
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

    fn apply(&mut self, event: StatusEvent, now: Now) {
        match event {
            StatusEvent::FolderLifecycle(view) => self.on_view(*view, now),
            StatusEvent::WorkerHeartbeat { state, hb } => {
                self.on_heartbeat(&state, hb, now, true);
            }
            StatusEvent::FolderRemoved { state } => self.on_folder_removed(&state, now),
            StatusEvent::PeerConfigured {
                installation,
                endpoint,
                display_name,
            } => {
                // CCR-WP1.11-1: the catalog's display name labels the peer;
                // `None` keeps a label set through `set_peer_label`.
                if let Some(name) = display_name {
                    self.labels.insert(installation.clone(), name);
                }
                let peer = self.peer_mut(&installation);
                let new = !peer.outbound;
                peer.outbound = true;
                peer.endpoint = Some(endpoint.clone());
                if new {
                    self.ring.push(
                        now.ms,
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
                protocol,
            } => self.on_lane_state(
                &installation,
                LaneUpdate {
                    lane,
                    role,
                    direction,
                    state,
                    error,
                    backoff_until_ms,
                    rtt_ms,
                    protocol,
                },
                now,
            ),
            StatusEvent::ExchangeStarted {
                installation,
                repo,
                lane,
                op,
                at_ms,
                direction,
            } => {
                // CCR-WP2.8-3: an acceptor's start is booked on its inbound
                // lane and never makes the pair look outbound.
                let inbound = direction == Direction::Inbound;
                let lane_direction = if inbound {
                    Direction::Inbound
                } else {
                    Direction::Outbound
                };
                let peer = self.peer_mut(&installation);
                if inbound {
                    peer.inbound = true;
                }
                let role = legacy_lane_role(lane);
                let entry = peer
                    .lanes
                    .entry(LaneKey::of(lane_direction, lane))
                    .or_insert_with(|| LaneEntry::new(lane, role, lane_direction));
                if entry.inflight.len() >= MAX_INFLIGHT {
                    entry.inflight.remove(0);
                }
                entry.inflight.push(InflightOp {
                    op,
                    repo: repo.clone(),
                    since_ms: at_ms,
                    bytes: 0,
                });
                let pair = self.pair_mut(repo, installation, now);
                match direction {
                    Direction::Inbound => pair.inbound_seen = true,
                    Direction::Outbound => pair.outbound_seen = true,
                    Direction::Both => {
                        pair.inbound_seen = true;
                        pair.outbound_seen = true;
                    }
                }
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
            } => self.on_catalog(&installation, entries, now),
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
                entry.set_state(state, now.ms);
                self.update_conn(&installation, now);
            }
            StatusEvent::InboundIncompatible { address, detail } => {
                self.on_inbound_incompatible(&address, &detail, now);
            }
            StatusEvent::ObjectError {
                state,
                hash,
                error,
                source,
            } => {
                let mut repo = None;
                if let Some(folder) =
                    folder_for_event(&mut self.folders, &self.removed, &state, now)
                {
                    repo = folder.repo().map(str::to_owned);
                    let key = (
                        Scope::Object {
                            state,
                            hash: hash.clone(),
                        },
                        String::new(),
                    );
                    folder
                        .errors
                        .observe(key, error.clone(), now, &mut self.ring);
                    folder.errors.cap_items();
                    if source.is_some() {
                        if !folder.sourced.contains_key(&hash)
                            && folder.sourced.len() >= MAX_ITEM_ERRORS
                            && let Some(oldest) = folder
                                .sourced
                                .iter()
                                .min_by_key(|(_, at)| **at)
                                .map(|(hash, _)| hash.clone())
                        {
                            folder.sourced.remove(&oldest);
                        }
                        folder.sourced.insert(hash.clone(), now.ms);
                    }
                }
                if let (Some(source), Some(repo)) = (source, repo) {
                    self.legacy_object_failed(&source, &repo, &hash, &error, now);
                }
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

    fn on_view(&mut self, view: FolderView, now: Now) {
        let state = view.record.state.clone();
        let repo = view.record.repo.clone();
        let phase = view.phase.lifecycle();
        // A FolderLifecycle registers the folder (again, after a removal).
        self.removed.retain(|removed| *removed != state);
        let entry = self
            .folders
            .entry(state.clone())
            .or_insert_with(|| FolderEntry::new(now));
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
            entry.phase_since_ms = phase_since(&view.phase).unwrap_or(now.ms);
            entry.phase = phase;
            self.ring.push(
                now.ms,
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
                .push(now.ms, "gate", Some(&state), None, gate_message(&view.gate));
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
        // A newer heartbeat mirrors the mount itself; either way the mount is
        // mirrored once per event (a lost mount's report count rises by one).
        let applied = heartbeat.is_some_and(|hb| self.on_heartbeat(&state, hb, now, false));
        if !applied {
            self.mirror_mount(&state, now);
        }
    }

    /// A heartbeat, from a `WorkerHeartbeat` event (`take_mount`: its mount
    /// state is the freshest) or carried by a FolderView (whose own mount
    /// state, set by the controller, wins). Returns whether it was applied
    /// (it was newer than the last one, and the folder was not removed).
    fn on_heartbeat(
        &mut self,
        state: &str,
        hb: Arc<WorkerHeartbeat>,
        now: Now,
        take_mount: bool,
    ) -> bool {
        let Some(entry) = folder_for_event(&mut self.folders, &self.removed, state, now) else {
            return false;
        };
        if let Some(old) = &entry.heartbeat
            && (Arc::ptr_eq(old, &hb) || (old.instance == hb.instance && old.seq >= hb.seq))
        {
            return false;
        }
        let previous = entry.heartbeat.replace(Arc::clone(&hb));
        if take_mount {
            entry.mount = hb.mount.clone();
        }
        // Store health: a persistent condition, mirrored into a Folder slot.
        let store_key = (
            Scope::Folder {
                state: state.into(),
            },
            SLOT_STORE.to_owned(),
        );
        let store_error = hb
            .store_health
            .error()
            .map(|error| entry.errors.condition(&store_key, error, now));
        entry
            .errors
            .mirror(store_key, store_error, now, &mut self.ring);
        // Implicit successes for item scopes. A failed transfer's object
        // (`sourced`) clears only with no staged work left and after a grace:
        // objects of staged events are transferred before the counters see
        // them (WP3.INT).
        if hb
            .content
            .is_some_and(|content| rules::missing(&content) == (0, 0))
        {
            let idle = hb.inbox.is_none_or(|inbox| inbox.pending_active() == 0);
            let sourced = &entry.sourced;
            entry.errors.resolve(
                |(scope, _), _| match scope {
                    Scope::Object { hash, .. } => sourced
                        .get(hash)
                        .is_none_or(|at| idle && now.ms >= at.saturating_add(SOURCED_GRACE_MS)),
                    _ => false,
                },
                now,
                &mut self.ring,
            );
            let errors = &entry.errors;
            entry.sourced.retain(|hash, _| {
                errors.open.keys().any(
                    |(scope, _)| matches!(scope, Scope::Object { hash: open, .. } if open == hash),
                )
            });
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
        // Download progress the worker made on its own (objects completed,
        // or a partial spool grew). Activation is not credited here: rows
        // the worker activates count only for the pairs whose own backlog
        // shrinks, which the tick sees as a decrease of their metadata work
        // (their feed counters, or their next summary). Crediting any
        // activation to every pair of the repo kept a pair whose own work
        // never moved from ever stalling.
        let downloaded = match (previous.as_ref().and_then(|old| old.content), hb.content) {
            (Some(before), Some(after)) => {
                let (bo, bb) = rules::missing(&before);
                let (ao, ab) = rules::missing(&after);
                ao < bo || ab < bb || after.partial_bytes > before.partial_bytes
            }
            _ => false,
        };
        self.mirror_mount(state, now);
        if downloaded
            && let Some(repo) = self
                .folders
                .get(state)
                .and_then(|f| f.repo().map(str::to_owned))
        {
            for ((pair_repo, _), pair) in self.pairs.iter_mut() {
                if *pair_repo != repo {
                    continue;
                }
                if pair.work.is_some_and(|work| work.has_down_work()) {
                    pair.last_progress = Some(now.mono);
                    pair.last_down_ms = Some(now.ms);
                }
            }
        }
        true
    }

    /// Mirrors the folder's current mount state into the Mount scope.
    fn mirror_mount(&mut self, state: &str, now: Now) {
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
                // The mount supervisor owns the attempt count.
                let mut record = entry.errors.condition(&key, error, now);
                record.count = u64::from((*attempt).max(1));
                record.next_retry_at_ms = *next_retry_at_ms;
                entry.errors.mirror(key, Some(record), now, &mut self.ring);
            }
            MountState::Lost { error } => {
                // A state, not an attempt: every report counts.
                let record = entry.errors.condition(&key, error, now);
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

    fn on_folder_removed(&mut self, state: &str, now: Now) {
        // Late events for the state are dropped from now on.
        if !self.removed.iter().any(|removed| removed == state) {
            if self.removed.len() >= MAX_TOMBSTONES {
                self.removed.pop_front();
            }
            self.removed.push_back(state.to_owned());
        }
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
        self.ring.push(
            now.ms,
            "lifecycle",
            Some(state),
            None,
            "folder removed".into(),
        );
    }

    /// A legacy (v2) peer dialed us (§6.2). This is unauthenticated pre-auth
    /// traffic, so everything it can grow is bounded (invariant 22):
    ///
    /// * one notice per remote host (an IPv6 host by its /64): its count
    ///   rises with every attempt, each reconnect using a new ephemeral port;
    /// * at most [`MAX_INBOUND_NOTICES`] hosts are kept (the least recently
    ///   seen is dropped, not moved to the history);
    /// * a notice adds no generic `error` activity entry, and at most one
    ///   `inbound_incompatible` entry per [`NOTICE_ACTIVITY_INTERVAL`], which
    ///   counts the hosts seen since the previous one;
    /// * an expired notice is not a resolution: the history keeps only the
    ///   newest one (`ErrorBook::expire_notices`).
    fn on_inbound_incompatible(&mut self, address: &str, detail: &str, now: Now) {
        let code = code_in(detail).unwrap_or("PEER_NEEDS_UPGRADE").to_owned();
        let message = truncate(
            format!("inbound connection from {address}: {detail}"),
            MAX_NOTICE_MESSAGE,
        );
        let key = (
            Scope::Daemon,
            format!("{INBOUND_SLOT}{}", remote_host(address)),
        );
        let new = !self.daemon_errors.open.contains_key(&key);
        // A ring with no capacity records nothing: the notice's own entry
        // below replaces the generic `error` one.
        self.daemon_errors.observe(
            key,
            ErrorWire::new(code, message.clone()),
            now,
            &mut Ring::default(),
        );
        self.daemon_errors.cap_inbound_notices();
        if !new {
            return;
        }
        let due = self
            .notice_logged
            .is_none_or(|at| now.mono.saturating_sub(at) >= rules::ms(NOTICE_ACTIVITY_INTERVAL));
        if !due {
            self.notices_unlogged = self.notices_unlogged.saturating_add(1);
            return;
        }
        let message = match self.notices_unlogged {
            0 => message,
            1 => format!("{message} (and 1 more host since the last notice)"),
            more => format!("{message} (and {more} more hosts since the last notice)"),
        };
        self.ring
            .push(now.ms, "inbound_incompatible", None, None, message);
        self.notice_logged = Some(now.mono);
        self.notices_unlogged = 0;
    }

    /// A peer's published catalog. A remote folder that turned Running (it
    /// was not Running, or not listed, in the previous catalog) serves
    /// again: what the remote answered while it did not
    /// (`REMOTE_WORKER_STARTING`, `FOLDER_SYNC_PAUSED`) is resolved. An
    /// unchanged Running entry proves nothing (the catalog may predate the
    /// answer), so only the transition counts.
    fn on_catalog(&mut self, installation: &str, entries: Vec<PublishedEntry>, now: Now) {
        let peer = self.peer_mut(installation);
        peer.outbound = true;
        let started: Vec<String> = entries
            .iter()
            .filter(|entry| {
                entry.state == EntryState::Running
                    && peer
                        .catalog_entry(&entry.repo)
                        .is_none_or(|before| before.state != EntryState::Running)
            })
            .map(|entry| entry.repo.clone())
            .collect();
        let listed: Vec<String> = entries.iter().map(|entry| entry.repo.clone()).collect();
        peer.catalog = Some(entries);
        // A known catalog decides what the peer shares: a repo it no longer
        // lists stops counting, whatever was exchanged before, and what was
        // learnt about it (the last summary) goes with it, so it is not
        // shown again if the peer publishes the repo later.
        for ((repo, inst), pair) in self.pairs.iter_mut() {
            if inst == installation && !listed.contains(repo) {
                pair.inbound_seen = false;
                pair.summary = None;
                pair.last_meta_exchange_ms = None;
                pair.meta_exchange_started_ms = None;
            }
        }
        for repo in started {
            let Some(state) = self.repos.get(&repo) else {
                continue;
            };
            let Some(folder) = self.folders.get_mut(state) else {
                continue;
            };
            let scope = Scope::FolderPeer {
                state: state.clone(),
                installation: installation.to_owned(),
            };
            folder.errors.resolve(
                |(slot_scope, _), open| {
                    *slot_scope == scope && rules::remote_waiting_code(&open.record.error.code)
                },
                now,
                &mut self.ring,
            );
        }
    }

    fn on_peer_removed(&mut self, installation: &str, now: Now) {
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
        self.ring.push(
            now.ms,
            "peer",
            None,
            Some(installation),
            "peer removed".into(),
        );
        if keep {
            self.update_conn(installation, now);
        }
    }

    fn on_lane_state(&mut self, installation: &str, update: LaneUpdate, now: Now) {
        let LaneUpdate {
            lane,
            role,
            direction,
            state,
            error,
            backoff_until_ms,
            rtt_ms,
            protocol,
        } = update;
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
        entry.set_state(state, now.ms);
        entry.backoff_until_ms = backoff_until_ms;
        if rtt_ms.is_some() {
            entry.rtt_ms = rtt_ms;
        }
        if state != LaneState::Ready {
            entry.protocol = None;
        } else if protocol.is_some() {
            entry.protocol = protocol;
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
                    error.retry_after_ms = Some(until.saturating_sub(now.ms));
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

    fn on_exchange_finished(&mut self, x: Exchange, now: Now) {
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
        peer.up.record(x.bytes_up, x.duration_ms, now.mono);
        peer.down.record(x.bytes_down, x.duration_ms, now.mono);
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
        lane.up.record(x.bytes_up, x.duration_ms, now.mono);
        lane.down.record(x.bytes_down, x.duration_ms, now.mono);
        lane.exchanges = lane.exchanges.saturating_add(1);
        if x.ok {
            lane.last_success_ms = Some(now.ms);
        }
        // Pair accounting.
        let pair = self
            .pairs
            .entry((x.repo.clone(), x.installation.clone()))
            .or_default();
        pair.touched = now.mono;
        match x.direction {
            Direction::Inbound => pair.inbound_seen = true,
            Direction::Outbound => pair.outbound_seen = true,
            Direction::Both => {
                pair.inbound_seen = true;
                pair.outbound_seen = true;
            }
        }
        // Progress is a completed object (of a successful exchange), not
        // bytes on the wire: a pull that times out mid-stream, or whose
        // writes the worker rejects, moves bytes and keeps nothing. Partial
        // progress on a large object shows in the content counters (its spool
        // grows, `on_heartbeat`; a peer's published counters, `tick`).
        let completed_down = x.ok && x.objects_down > 0;
        let completed_up = x.ok && x.objects_up > 0;
        let mut progress = completed_down || completed_up;
        if objects_op {
            // The pair's rates are its object transfer rates (and ETA): only
            // objects ops feed them. Metadata exchange bytes count in the
            // peer's and the lane's rates above.
            pair.up.record(x.bytes_up, x.duration_ms, now.mono);
            pair.down.record(x.bytes_down, x.duration_ms, now.mono);
            pair.down_episode = pair.down_episode.saturating_add(x.bytes_down);
            pair.up_episode = pair.up_episode.saturating_add(x.bytes_up);
            if x.bytes_down >= rules::PROGRESS_MIN_BYTES || x.objects_down > 0 {
                // Evidence that the peer supplies objects (`rules::may_supply`).
                pair.supplied = Some(now.mono);
            }
            if completed_down {
                pair.last_down_ms = Some(now.ms);
            }
            if completed_up {
                pair.last_up_ms = Some(now.ms);
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
            pair.last_meta_exchange_ms = Some(now.ms);
            pair.meta_exchange_started_ms = Some(now.ms.saturating_sub(x.duration_ms));
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
            pair.last_progress = Some(now.mono);
            if pair.stalled_since.take().is_some() {
                self.ring.push(
                    now.ms,
                    "stall",
                    folder_state.as_deref(),
                    Some(&x.installation),
                    "stall cleared".into(),
                );
            }
        }
        // Errors: per (peer, repo, op, direction) slot of the folder book.
        if let Some(state) = &folder_state
            && let Some(folder) = self.folders.get_mut(state)
        {
            let scope = Scope::FolderPeer {
                state: state.clone(),
                installation: x.installation.clone(),
            };
            let slot = exchange_slot(&x.op, x.direction);
            match (&x.error, x.ok) {
                (_, true) => {
                    // A success clears its own key and the generic slot; and
                    // any success proves that the remote folder serves, so
                    // what it answered while it did not no longer holds.
                    folder.errors.resolve(
                        |(slot_scope, name), open| {
                            *slot_scope == scope
                                && (name.is_empty()
                                    || *name == slot
                                    || rules::remote_waiting_code(&open.record.error.code))
                        },
                        now,
                        &mut self.ring,
                    );
                }
                (Some(error), false) => {
                    folder
                        .errors
                        .observe((scope, slot), error.clone(), now, &mut self.ring);
                }
                (None, false) => {}
            }
        }
        if progress && let Some(state) = &folder_state {
            self.ring.coalesce(
                now.ms,
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
            update_legacy_repo(
                repo,
                &x,
                objects_op,
                progress,
                content,
                remote_content,
                now.ms,
            );
        }
    }

    fn on_error(&mut self, scope: Scope, error: ErrorWire, now: Now) {
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
                let state = state.clone();
                if let Some(folder) =
                    folder_for_event(&mut self.folders, &self.removed, &state, now)
                {
                    folder
                        .errors
                        .observe((scope, String::new()), error, now, &mut self.ring);
                    folder.errors.cap_items();
                }
            }
        }
    }

    fn on_resolved(&mut self, scope: &Scope, code: &str, now: Now) {
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
    fn update_conn(&mut self, installation: &str, now: Now) {
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
            peer.conn_since_ms = Some(now.ms);
            if changed {
                self.ring.push(
                    now.ms,
                    "peer",
                    None,
                    Some(installation),
                    format!("peer {}", conn_name(conn)),
                );
            }
        }
    }

    // -- tick -----------------------------------------------------------------

    fn tick(&mut self, now: Now, elapsed: Duration, cfg: &HubConfig) -> bool {
        let mut changed = false;
        let from = self
            .rates_at
            .unwrap_or_else(|| now.mono.saturating_sub(rules::ms(elapsed)))
            .min(now.mono);
        self.rates_at = Some(now.mono);
        let mut fold = |rate: &mut TransferRate| {
            let before = (rate.short_bps(), rate.long_bps());
            rate.advance(from, now.mono);
            changed |= before != (rate.short_bps(), rate.long_bps());
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
        // Stall detection and remaining-work tracking per (folder, peer), on
        // the monotonic timeline.
        let mut updates = Vec::new();
        // (sharing, remaining work) of every (repo, peer) of a registered
        // folder, for the implicit clears below.
        let mut pair_work: BTreeMap<PairKey, (bool, Work)> = BTreeMap::new();
        for (state, folder) in &self.folders {
            let Some(view) = &folder.view else { continue };
            for (installation, peer) in &self.peers {
                let key = (view.record.repo.clone(), installation.clone());
                let pair = self.pairs.get(&key);
                let facts = self.pair_facts(state, folder, view, peer, pair, now, cfg);
                pair_work.insert(key.clone(), (facts.inputs.shared, facts.work));
                updates.push((key, facts.inputs, facts.work, facts.supplies));
            }
        }
        for (key, inputs, work, supplies) in updates {
            let eligible = inputs.can_progress() && inputs.work_remaining();
            if !eligible && !self.pairs.contains_key(&key) {
                continue;
            }
            let pair = self.pairs.entry(key.clone()).or_default();
            if let Some(mut before) = pair.work {
                if pair.supplying && !supplies {
                    // The download work left with its attribution, not
                    // because anything was downloaded.
                    before.down_bytes = 0;
                    before.down_objects = 0;
                }
                if rules::progressed(&before, &work) {
                    // Shown as the pair's `last_progress_ms`, and per
                    // direction as the transfers' `last_progress_ms`.
                    pair.last_progress = Some(now.mono);
                    if work.down_bytes < before.down_bytes
                        || work.down_objects < before.down_objects
                    {
                        pair.last_down_ms = Some(now.ms);
                    }
                    if work.up_bytes < before.up_bytes || work.up_objects < before.up_objects {
                        pair.last_up_ms = Some(now.ms);
                    }
                    changed = true;
                }
            }
            pair.supplying = supplies;
            pair.work = Some(work);
            if work.down_bytes == 0 && work.down_objects == 0 && pair.down_episode != 0 {
                pair.down_episode = 0;
                changed = true;
            }
            if work.up_bytes == 0 && work.up_objects == 0 && pair.up_episode != 0 {
                pair.up_episode = 0;
                changed = true;
            }
            pair.eligible_since = if eligible {
                Some(pair.eligible_since.unwrap_or(now.mono))
            } else {
                None
            };
            let stalled = rules::stall(&StallInput {
                work_remaining: inputs.work_remaining(),
                connected: inputs.can_progress(),
                eligible_since_ms: pair.eligible_since,
                last_progress_ms: pair.last_progress,
                now_ms: now.mono,
                stall_after: cfg.stall_after,
            });
            if stalled.is_some() != pair.stalled_since.is_some() {
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
                    .push(now.ms, "stall", state.as_deref(), Some(&key.1), message);
            }
            pair.stalled_since = stalled;
        }
        changed |= self.clear_idle_errors(&pair_work, now);
        // Time-based error visibility; expiry of inbound-incompatible notices.
        changed |= self.daemon_errors.refresh(now, &mut self.ring);
        for folder in self.folders.values_mut() {
            changed |= folder.errors.refresh(now, &mut self.ring);
        }
        changed |= self
            .daemon_errors
            .expire_notices(now, rules::ms(INBOUND_INCOMPATIBLE_TTL));
        self.prune_orphans(now);
        changed
    }

    /// Implicit successes for (folder, peer) errors whose work is gone. An
    /// error clears on the next success of its own key (its op and
    /// direction); these cover the keys that will never run again to clear
    /// it. Once an error has not been reported for [`WORK_GONE_GRACE`]:
    ///
    /// * every error of a peer that does not share the folder (any more), or
    ///   that the hub does not know, resolves: nothing is exchanged with it
    ///   for this folder;
    /// * an objects-op error resolves when the pair has no work left in the
    ///   op's direction(s) and no exchange of that op is in flight: the
    ///   object became unreferenced, arrived from another peer, or the peer
    ///   got it elsewhere, so the op does not run again (the worker only
    ///   pulls or pushes what is wanted).
    ///
    /// Errors of other ops wait for their op's next success: the metadata
    /// exchange runs again on its own (anti-entropy every sync interval) as
    /// long as the pair shares the folder and both folders serve. Remote
    /// folder answers (`REMOTE_WORKER_STARTING`, `FOLDER_SYNC_PAUSED`) are
    /// resolved by any success with the peer for the repo, or by its catalog
    /// entry turning Running (see `on_exchange_finished`, `on_catalog`).
    fn clear_idle_errors(&mut self, pair_work: &BTreeMap<PairKey, (bool, Work)>, now: Now) -> bool {
        let grace = rules::ms(WORK_GONE_GRACE);
        let HubState {
            folders,
            peers,
            ring,
            ..
        } = self;
        let mut changed = false;
        for folder in folders.values_mut() {
            let Some(repo) = folder.repo().map(str::to_owned) else {
                continue;
            };
            changed |= folder.errors.resolve(
                |(scope, slot), open| {
                    let Scope::FolderPeer { installation, .. } = scope else {
                        return false;
                    };
                    if now.mono.saturating_sub(open.seen) < grace {
                        return false;
                    }
                    let Some((shared, work)) = pair_work.get(&(repo.clone(), installation.clone()))
                    else {
                        return true;
                    };
                    if !*shared {
                        return true;
                    }
                    let (down, up) = slot_directions(slot);
                    if !down && !up {
                        return false;
                    }
                    let work_left = (down && work.has_down_work()) || (up && work.has_up_work());
                    let in_flight = !slot.starts_with(INBOUND_OP)
                        && peers
                            .get(installation)
                            .is_some_and(|peer| op_in_flight(peer, &repo, slot));
                    !work_left && !in_flight
                },
                now,
                ring,
            );
        }
        changed
    }

    /// Drops what can never be shown once [`ORPHAN_GRACE`] has passed without
    /// news: the entry of a folder that was never registered (an event raced
    /// a registration that never came) and the pair data of a repo without a
    /// registered folder (with its legacy lane repos). None of it is in a
    /// snapshot, so this is not a change.
    fn prune_orphans(&mut self, now: Now) {
        let grace = rules::ms(ORPHAN_GRACE);
        let fresh = |since: u64| now.mono.saturating_sub(since) < grace;
        self.folders
            .retain(|_, folder| folder.view.is_some() || fresh(folder.created));
        let repos = &self.repos;
        self.pairs
            .retain(|(repo, _), pair| repos.contains_key(repo) || fresh(pair.touched));
        let pairs = &self.pairs;
        for (installation, peer) in self.peers.iter_mut() {
            for lane in peer.lanes.values_mut() {
                lane.repos.retain(|repo, _| {
                    repos.contains_key(repo)
                        || pairs.contains_key(&(repo.clone(), installation.clone()))
                });
            }
        }
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
        now: Now,
        cfg: &HubConfig,
    ) -> PairFacts {
        let repo = &view.record.repo;
        let entry = peer.catalog_entry(repo);
        let gate_source = gate_source(&view.gate);
        let inbound_seen = pair.is_some_and(|pair| pair.inbound_seen);
        let outbound_seen = pair.is_some_and(|pair| pair.outbound_seen);
        // A configured peer whose catalog is unknown (catalogs are not kept
        // across restarts, §3.7) may share the repo: while it cannot be
        // reached it counts, so the folder waits for it (Offline, Backoff…)
        // instead of reading "not shared". Once online, its catalog (listed
        // every 2 s) decides.
        let maybe = peer.outbound && peer.catalog.is_none() && peer.conn != PeerConnState::Online;
        let shared = shares(peer, pair, repo, gate_source) || maybe;
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
        // per-peer feed counters when they are at least as recent: the
        // worker stamps them (`last_exchange_ms`) when it applies an
        // exchange, which is before the orchestrator records that exchange's
        // summary, so counters stamped at or after the summary's exchange
        // began reflect it (or a later one).
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
        let summary_began = pair.and_then(|pair| {
            pair.summary
                .as_ref()
                .and(pair.meta_exchange_started_ms.or(pair.last_meta_exchange_ms))
        });
        if let Some(c) = counters {
            // Only the counters know rejected events.
            meta.rejected = c.rejected;
            if c.last_exchange_ms >= summary_began {
                meta_known = true;
                meta.to_send = c.to_send;
                meta.to_receive = c.to_receive;
                meta.local_pending = c.staged_pending;
                meta.blocked = c.blocked;
                meta.quarantined = c.quarantined;
                meta.caught_up = c.caught_up;
                meta.cursor_seq = c.cursor_seq;
                meta.remote_tip = c.remote_tip;
                meta.last_exchange_ms = c.last_exchange_ms;
            }
        }
        if meta.remote_tip == 0
            && let Some(tip) = entry.and_then(|entry| entry.tip.as_ref())
        {
            meta.remote_tip = tip.seq;
        }
        // Object work exists only with a peer that shares the repo. Download
        // work goes to a peer that publishes the repo, or to one that
        // demonstrably pushes objects to us (inbound-only peers).
        let supplies = rules::may_supply(
            entry.is_some(),
            pair.and_then(|pair| pair.supplied),
            now.mono,
            cfg.stall_after,
        );
        let local_content = folder.content().filter(|_| shared && supplies);
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
        let stalled_since = pair.and_then(|pair| {
            rules::stall(&StallInput {
                work_remaining: inputs.work_remaining(),
                connected: inputs.can_progress(),
                eligible_since_ms: if eligible { pair.eligible_since } else { None },
                last_progress_ms: pair.last_progress,
                now_ms: now.mono,
                stall_after: cfg.stall_after,
            })
        });
        inputs.stalled = stalled_since.is_some();
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
            supplies,
            meta,
            direction,
            stalled_since,
            backoff_until_ms,
            error,
            culprit,
        }
    }

    fn build(&self, now: Now, cfg: &HubConfig, generation: u64) -> StatusSnapshot {
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
            at_ms: now.ms,
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
        now: Now,
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
                if let Some(since) = facts.stalled_since {
                    let stalled_for = now
                        .mono
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
                last_progress_ms: pair
                    .and_then(|pair| pair.last_progress)
                    .map(|mono| now.wall_of(mono)),
                stalled_since_ms: facts.stalled_since.map(|mono| now.wall_of(mono)),
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

    fn build_peer(&self, peer: &PeerEntry, now: Now) -> PeerStatus {
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
        // Folders it is known to share (the rule of `pair_facts`, without the
        // configured peer whose catalog is unknown, which only may).
        let shared_folders = self
            .folders
            .values()
            .filter_map(|folder| folder.view.as_ref())
            .filter(|view| {
                let pair = self
                    .pairs
                    .get(&(view.record.repo.clone(), peer.installation.clone()));
                shares(peer, pair, &view.record.repo, gate_source(&view.gate))
            })
            .map(|view| view.record.repo.clone())
            .collect();
        PeerStatus {
            installation: peer.installation.clone(),
            endpoint: peer.endpoint.clone(),
            label: self.labels.get(&peer.installation).cloned(),
            direction: peer.direction(),
            state: peer.conn,
            protocol: peer.protocol(),
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

/// The source of a folder's initial-sync gate, if it has one.
fn gate_source(gate: &GateState) -> Option<&str> {
    match gate {
        GateState::Awaiting { source, .. } | GateState::Blocked { source, .. } => {
            Some(source.as_str())
        }
        _ => None,
    }
}

/// Whether `peer` is known to share `repo`: its catalog lists it (a known
/// catalog decides, whatever was exchanged before), or, with no catalog (an
/// inbound-only peer, or one not listed yet), it exchanged the repo with us;
/// or it is the source of the folder's initial-sync gate.
fn shares(
    peer: &PeerEntry,
    pair: Option<&PairEntry>,
    repo: &str,
    gate_source: Option<&str>,
) -> bool {
    let exchanged = pair.is_some_and(|pair| pair.inbound_seen || pair.outbound_seen);
    peer.catalog_entry(repo).is_some()
        || (peer.catalog.is_none() && exchanged)
        || gate_source == Some(peer.installation.as_str())
}

/// The folder entry a folder-scoped event (heartbeat, error) applies to:
/// `None` for a recently removed folder, whose late events are dropped. An
/// unknown folder gets an entry, which is pruned if it is never registered
/// (events can race the first `FolderLifecycle`).
fn folder_for_event<'a>(
    folders: &'a mut BTreeMap<String, FolderEntry>,
    removed: &VecDeque<String>,
    state: &str,
    now: Now,
) -> Option<&'a mut FolderEntry> {
    if removed.iter().any(|removed| removed == state) {
        return None;
    }
    Some(
        folders
            .entry(state.to_owned())
            .or_insert_with(|| FolderEntry::new(now)),
    )
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

/// Slot-name prefix of an exchange the peer initiated (inbound).
const INBOUND_OP: &str = "in:";

/// The error slot of an exchange: its op, or `in:op` for an exchange the
/// peer initiated, so the two directions of one op are separate keys (an
/// inbound `objects.push` is a download for us, an outbound one an upload).
fn exchange_slot(op: &str, direction: Direction) -> String {
    match direction {
        Direction::Inbound => format!("{INBOUND_OP}{op}"),
        Direction::Outbound | Direction::Both => op.to_owned(),
    }
}

/// `(down, up)` seen from here: which directions the objects op of an
/// exchange slot moves bytes in (`(false, false)` for any other op).
fn slot_directions(slot: &str) -> (bool, bool) {
    match slot.strip_prefix(INBOUND_OP) {
        // The peer pulls from us (an upload) or pushes to us (a download).
        Some(op) => {
            let (down, up) = object_directions(op);
            (up, down)
        }
        None => object_directions(slot),
    }
}

/// Whether an outbound exchange of `op` for `repo` is in flight with `peer`.
fn op_in_flight(peer: &PeerEntry, repo: &str, op: &str) -> bool {
    peer.lanes
        .iter()
        .filter(|(key, _)| !key.inbound)
        .flat_map(|(_, lane)| lane.inflight.iter())
        .any(|inflight| inflight.op == op && inflight.repo == repo)
}

/// `(down, up)`: which directions an objects op moves bytes in, seen from the
/// side that initiated it.
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
                // CCR-WP3.1-2 (applied by WP3.INT): 6298d0b reported a
                // repository whose source still had events to activate (a
                // gated received folder) as not caught up; the feed's
                // `caught_up` is about our cursor only (§6.3.6).
                let caught_up = summary.caught_up && summary.remote_pending == 0;
                repo.caught_up = caught_up;
                repo.metadata_caught_up = caught_up;
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
                // CCR-WP3.1-1 (applied by WP3.INT): a bulk lane never claims
                // `caught_up` (its exchanges carry objects only; the claim is
                // the control lane's), as 6298d0b's bulk lanes did.
                repo.metadata_caught_up = false;
                repo.caught_up = false;
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
///
/// Read at projection time (`live`, WP3.INT): v3 bulk lanes report an
/// exchange only when objects move, so the `object_inventory` booked with
/// the last one would go stale once a transfer is done; it is recomputed from
/// the folder's and the peer's content counters now. The lane's failed
/// objects (CCR-WP3.INT-1) whose folder error is still open are its `error`
/// (the newest), `consecutive_failures` (the most attempts of one object, on
/// top of failed exchanges) and `retry_at_ms`; once none is open (installed,
/// or the folder has nothing missing) they no longer show.
fn legacy_repo_value(repo: &LegacyRepo, lane: u32, pool: u32, live: &LegacyLive<'_>) -> Value {
    let mut value = serde_json::to_value(repo).unwrap_or(Value::Null);
    if lane > 0
        && repo.object_inventory.is_some()
        && let Some(local) = live.folder.and_then(FolderEntry::content)
    {
        let remote = live.remote.unwrap_or_default();
        value["object_inventory"] = json!({
            "local_available": local.shared_available,
            "local_only": local.shared_available.saturating_sub(remote.shared_available),
            "local_partial": local.downloading,
            "remote_available": remote.shared_available,
            "remote_only": remote.shared_available.saturating_sub(local.shared_available),
            "remote_partial": remote.downloading,
        });
    }
    let open = |hash: &str| {
        live.folder.is_some_and(|folder| {
            folder.errors.open.contains_key(&(
                Scope::Object {
                    state: live.state.to_owned(),
                    hash: hash.to_owned(),
                },
                String::new(),
            ))
        })
    };
    let failing: Vec<&LegacyObjectFailure> = repo
        .objects
        .iter()
        .filter(|(hash, _)| open(hash))
        .map(|(_, failure)| failure)
        .collect();
    if let Some(newest) = failing.iter().max_by_key(|failure| failure.at_ms) {
        let attempts = failing.iter().map(|failure| failure.attempts).max();
        value["error"] = json!(newest.text);
        value["consecutive_failures"] = json!(
            repo.consecutive_failures
                .saturating_add(attempts.unwrap_or(0))
        );
        value["retry_at_ms"] = json!(newest.retry_at_ms.or(repo.retry_at_ms));
        value["caught_up"] = json!(false);
    }
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

/// The remote host of a socket address without its port (`ip:port`,
/// `[v6]:port` or `name:port`), so every connection from one host shares
/// one notice slot; at most [`MAX_NOTICE_HOST`] bytes.
fn remote_host(address: &str) -> String {
    let address = address.trim();
    if let Ok(socket) = address.parse::<SocketAddr>() {
        return notice_host(socket.ip());
    }
    if let Ok(ip) = address
        .trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<IpAddr>()
    {
        return notice_host(ip);
    }
    let host = match address.rsplit_once(':') {
        Some((host, port))
            if !host.is_empty() && !host.contains(':') && port.parse::<u16>().is_ok() =>
        {
            host
        }
        _ => address,
    };
    truncate(host.to_owned(), MAX_NOTICE_HOST)
}

/// The notice key of an IP address: an IPv4 address (IPv4-mapped IPv6
/// included) as is, an IPv6 address by its /64. One IPv6 host can source
/// from any address of its /64 (privacy addresses, or trivially when it
/// wants to rotate notices), so the /64 is the host.
fn notice_host(ip: IpAddr) -> String {
    match ip.to_canonical() {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => {
            let mut segments = v6.segments();
            segments[4..].fill(0);
            format!("{}/64", Ipv6Addr::from(segments))
        }
    }
}

/// `text` cut to at most `max` bytes, on a character boundary.
fn truncate(mut text: String, max: usize) -> String {
    if text.len() > max {
        let mut end = max;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
    text
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
