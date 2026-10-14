//! Owned by WP2.6 — the outbound peer stack over v3 sessions (REVAMP_PLAN
//! §3.7, §14 WP2.6): [`PeerManager`] (the v3 [`NetworkApi`]), one supervisor
//! per catalog peer, its lanes and the remote-catalog client.
//!
//! **Manager.** [`PeerManager::start`] runs one supervisor per catalog peer,
//! keyed by installation, and follows the catalog: a peer whose record
//! changes (endpoint, code, connections or revision; a re-pair gets a new
//! revision) is stopped (GOAWAY, cancel, join ≤ 5 s) and respawned, a peer
//! that left the catalog is stopped and reported `PeerRemoved`. Reconciling
//! runs on the `peer-manager` thread (catalog generation changes) and
//! synchronously in [`NetworkApi::pair`], [`NetworkApi::unpair`] and
//! [`NetworkApi::reconfigure`], serialized by one mutex that only reconcilers
//! take. If several records name the same installation (a legacy catalog),
//! the first by endpoint is supervised; `pair` replaces the other endpoints
//! of the installation it pairs.
//!
//! **Lanes.** A peer's pool is `max(connections, 2)` sessions: lane 0 is a
//! control session, lanes 1..N-1 are bulk sessions with
//! `LaneSpec{index: lane - 1, count: N - 1}` (the legacy numbering). Each lane
//! has its own thread and state machine:
//!
//! | State | Entered | Leaves |
//! |---|---|---|
//! | `Dialing` | a dial starts (every resolved address, 3 s each) | `Handshaking` once the server's `HELLO_ACK` arrives |
//! | `Handshaking` | the HMAC step (the dialer derives its key) | `Ready`, or a failure below |
//! | `Ready` | an authenticated session | `Backoff` when the session closes |
//! | `Backoff` | a transient failure or a closed session | `Dialing` after 1 s → 60 s (±20 %); the attempt count resets after 30 s `Ready` |
//! | `Incompatible` | `PEER_NEEDS_UPGRADE`, `PEER_PROTOCOL_UNSUPPORTED`, any Incompatible-class refusal | rechecked after 10 min, on a change of the peer's record (a respawn), or on [`NetworkApi::reconfigure`] |
//! | `Unauthorized` | a wrong or rotated code, `PEER_INSTALLATION_CHANGED`, any NeedsUserAction refusal | rechecked after 60 s, on a code change (a re-pair respawns the peer), or on [`NetworkApi::reconfigure`] |
//! | `Closed` | the lane thread ended (unpair, respawn, shutdown) | |
//!
//! `Incompatible` and `Unauthorized` describe the peer, not one session, so
//! they are a peer-level *block*: every lane that is not connected waits for
//! it, and when it expires one lane alone re-dials (the probe) while the
//! others wait for that probe's outcome. A legacy or unauthorized peer
//! therefore sees one dial per lane, then one dial per recheck interval, and
//! never a tight loop. A lane that reaches `Ready` clears the block.
//!
//! On `Ready` the lane hands its [`Conn`] to the lane runner on its own thread
//! with a per-session child [`Cancel`]: `scheduler::run_control` on lane 0
//! (`ctl-<installation>`), `objects::run_bulk_lane` on bulk lanes
//! (`bulk-<installation>-<lane>`). The lane thread watches the session; when
//! it closes, the runner is cancelled, joined (≤ 1 s, then detached) and the
//! lane backs off and re-dials with a fresh session. A runner that returns
//! while its session is open (or panics) gets its session closed, so the
//! lane restarts it after a backoff (crash-only, §2.2 rule 6). Runners must
//! therefore return only when their token is cancelled or their session is
//! gone.
//!
//! **Catalog client.** Lane 0 lists the peer's published catalog
//! (`catalog.list{since}`) when it becomes Ready (`since: None`) and every
//! 2 s, into `PeerContext.catalog`. The copy survives reconnects, and a
//! respawn for the same installation starts from it, so bulk lanes never
//! depend on lane 0 being up (RC4). `PeerCatalog` is emitted on the first
//! listing of each supervisor and on every change. The request travels in the
//! request header; the reply is read from the body when there is one,
//! otherwise from the reply header.
//!
//! **Status.** `PeerConfigured` when a supervisor starts, `LaneState` on every
//! lane transition (`Ready` carries the RTT from [`ConnHealth`], refreshed
//! when it moves by a quarter or more and by at least 5 ms), `PeerCatalog`,
//! and `PeerRemoved` when a peer leaves the catalog. The control lane records
//! `peer_meta` on session establishment (seen, incompatibility cleared) and
//! when a peer turns out incompatible; the writes are queued for the
//! `peer-meta` thread, so a slow or wedged catalog never holds a lane (§2.3).
//!
//! **Stopping** (unpair, respawn, shutdown): the supervisor's token is
//! cancelled, whose per-session hooks send `GOAWAY` at once (`PEER_NOT_PAIRED`,
//! `WS_CLOSED` for a respawn, `ORCHESTRATOR_STOPPING`). A v3 server closes a
//! session once it is idle after a GOAWAY, which ends the lane; sessions
//! still open one second before the deadline are closed outright. Lane
//! threads that have not ended by the deadline (a lane inside a dial, which
//! cannot be interrupted, CCR-WP2.6-2) are detached and logged; they close
//! their new session as soon as the dial returns.
//!
//! Invariants: 16 (a probe and every lane session are fresh requests, nothing
//! is replayed), 20 (a legacy endpoint is `Incompatible`, the code is never
//! sent), 21 (the code is only ever a dial credential: no status event, log
//! line or error carries it; installation checks, self-pair refusal), 29
//! (every wait is interruptible by the supervisor's token and bounded).
//!
//! [`ConnHealth`]: crate::session::ConnHealth
use crate::{
    model::{
        errors::{ErrorClass, ErrorWire},
        orchestrator::{
            CatalogApi, CatalogCmd, CatalogSnapshot, Ctx, NetworkApi, PairResult, PeerContext,
            PeerRecord, PublishedEntry, RemoteCatalog, limits,
        },
        protocol::{CatalogListReply, CatalogListRequest, LaneRole, LaneSpec, deadlines, ops},
        status::{Direction, LaneState, StatusEvent},
    },
    orchestrator::{objects, scheduler},
    session::{self, ClientAuth, CodeAuth, Conn, ConnOptions, DialError, Priority, Reply},
    util::{
        backoff::Backoff,
        cancel::Cancel,
        lock::{Tracked, lock},
        log::event,
        notify::Notify,
        snapshot::Snapshot,
    },
};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        mpsc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// How long the `peer-manager` thread waits for a catalog change before it
/// re-checks its cancellation (the folder registry uses the same period).
const CATALOG_WAIT: Duration = Duration::from_millis(500);
/// A wait with nothing to time out on (a block being probed by another lane);
/// every change of interest pokes the waiter.
const IDLE_WAIT: Duration = Duration::from_secs(3600);
/// RTT changes smaller than this are not re-reported.
const RTT_MIN_DELTA_MS: u64 = 5;

/// Timing of the peer stack. [`Default`] is the §3.7 policy; tests shorten it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PeerOptions {
    /// Lane reconnect backoff: 1 s → 60 s.
    pub lane_backoff_base: Duration,
    pub lane_backoff_cap: Duration,
    /// ±20 %.
    pub lane_backoff_jitter: f64,
    /// The attempt count resets after this long `Ready` (30 s).
    pub lane_reset_after: Duration,
    /// `Incompatible` peers are re-dialed after this (10 min).
    pub incompatible_recheck: Duration,
    /// `Unauthorized` peers are re-dialed after this (60 s).
    pub unauthorized_recheck: Duration,
    /// `catalog.list` period on lane 0 (2 s).
    pub catalog_poll: Duration,
    /// Unpair, respawn and shutdown join a peer's lanes within this (5 s).
    pub join: Duration,
    /// Sessions still open this long before a join deadline are closed
    /// outright instead of waiting for the peer to close after GOAWAY.
    pub force_close_margin: Duration,
    /// After its session ends, a lane waits this long for its runner.
    pub runner_join: Duration,
    /// How often a Ready lane looks at its session RTT.
    pub rtt_refresh: Duration,
    /// Options of every outbound session (heartbeat 5 s, dead after 20 s).
    pub conn: ConnOptions,
}

impl Default for PeerOptions {
    fn default() -> Self {
        Self {
            lane_backoff_base: limits::LANE_BACKOFF_BASE,
            lane_backoff_cap: limits::LANE_BACKOFF_CAP,
            lane_backoff_jitter: limits::BACKOFF_JITTER,
            lane_reset_after: limits::LANE_RESET_AFTER,
            incompatible_recheck: limits::INCOMPATIBLE_RECHECK,
            unauthorized_recheck: limits::UNAUTHORIZED_RECHECK,
            catalog_poll: limits::CATALOG_POLL,
            join: limits::PEER_JOIN,
            force_close_margin: Duration::from_secs(1),
            runner_join: Duration::from_secs(1),
            rtt_refresh: Duration::from_secs(5),
            conn: ConnOptions::default(),
        }
    }
}

impl PeerOptions {
    fn lane_backoff(&self) -> Backoff {
        Backoff::new(
            self.lane_backoff_base,
            self.lane_backoff_cap,
            self.lane_backoff_jitter,
            self.lane_reset_after,
        )
    }
}

/// One lane as its supervisor last saw it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LaneView {
    /// Pool index: 0 is the control lane.
    pub lane: u32,
    pub role: LaneRole,
    /// The bulk lane spec (`None` for the control lane).
    pub spec: Option<LaneSpec>,
    pub state: LaneState,
    /// Wall-clock ms of the last transition.
    pub since_ms: u64,
    pub error: Option<ErrorWire>,
    /// Backoff end, or the recheck time of a block.
    pub backoff_until_ms: Option<u64>,
    /// Consecutive failed attempts (the backoff attempt count).
    pub attempt: u32,
    /// Dials started.
    pub dials: u64,
    /// Sessions that reached `Ready`.
    pub connects: u64,
    pub rtt_ms: Option<u64>,
    /// The negotiated protocol while `Ready` (`"tkfs-peer/3"`). The producer
    /// side of CCR-WP1.11-2: `LaneState.protocol` once the field exists.
    pub protocol: Option<String>,
}

/// One supervised peer.
#[derive(Clone, Debug)]
pub struct PeerView {
    pub installation: String,
    /// `peer_meta.display_name`. The producer side of CCR-WP1.11-1:
    /// `PeerConfigured.display_name` once the field exists.
    pub label: Option<String>,
    pub endpoint: String,
    pub revision: String,
    pub pool: u32,
    pub lanes: Vec<LaneView>,
    /// The peer's last good published catalog.
    pub catalog: Arc<RemoteCatalog>,
}

/// The v3 outbound peer stack (§3.7): one supervisor per catalog peer.
pub struct PeerManager {
    ctx: Arc<Ctx>,
    options: PeerOptions,
    /// Child of `ctx.shutdown`; every supervisor's token is a child of it.
    cancel: Cancel,
    /// Set by `shutdown`; nothing is spawned afterwards.
    closed: AtomicBool,
    peers: Tracked<BTreeMap<String, PeerHandle>>,
    /// Serializes reconcilers (they may wait up to `options.join` for lanes).
    reconciling: Mutex<()>,
    /// The catalog generation last reconciled.
    reconciled: AtomicU64,
    watcher: Mutex<Option<JoinHandle<()>>>,
    /// Poked when the `peer-manager` thread ends.
    watcher_done: Arc<Notify>,
    /// `peer_meta` writes, off the lane threads.
    meta: Arc<MetaWriter>,
}

struct PeerHandle {
    record: PeerRecord,
    supervisor: Arc<Supervisor>,
}

/// Why a supervisor stops: the code of the GOAWAY its sessions receive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StopReason {
    Removed,
    Reconfigured,
    Shutdown,
}

impl StopReason {
    fn goaway(self) -> (&'static str, &'static str) {
        match self {
            StopReason::Removed => ("PEER_NOT_PAIRED", "this installation unpaired the peer"),
            StopReason::Reconfigured => (
                "WS_CLOSED",
                "the peer's configuration changed; the session is replaced",
            ),
            StopReason::Shutdown => ("ORCHESTRATOR_STOPPING", "the orchestrator is stopping"),
        }
    }
}

impl PeerManager {
    /// Starts a supervisor for every catalog peer and the `peer-manager`
    /// thread that follows catalog changes, with the §3.7 timing.
    pub fn start(ctx: Arc<Ctx>) -> Arc<PeerManager> {
        Self::start_with(ctx, PeerOptions::default())
    }

    /// [`start`](Self::start) with explicit timing (tests).
    pub fn start_with(ctx: Arc<Ctx>, options: PeerOptions) -> Arc<PeerManager> {
        let cancel = ctx.shutdown.child();
        let ctx_catalog = ctx.catalog.clone();
        let manager = Arc::new(PeerManager {
            ctx,
            options,
            cancel,
            closed: AtomicBool::new(false),
            peers: Tracked::new("peer-manager.peers", BTreeMap::new()),
            reconciling: Mutex::new(()),
            reconciled: AtomicU64::new(0),
            watcher: Mutex::new(None),
            watcher_done: Arc::new(Notify::new()),
            meta: MetaWriter::start(ctx_catalog),
        });
        let generation = manager.reconcile();
        let weak = Arc::downgrade(&manager);
        let catalog = manager.ctx.catalog.clone();
        let cancel = manager.cancel.clone();
        let done = manager.watcher_done.clone();
        match thread::Builder::new()
            .name("peer-manager".into())
            .spawn(move || {
                watch_catalog(weak, catalog, cancel, generation);
                done.poke();
            }) {
            Ok(handle) => *lock(&manager.watcher) = Some(handle),
            Err(error) => {
                event!(error, CATALOG_UNAVAILABLE, {
                    "detail": "cannot start the peer-manager thread; catalog changes are followed only on pair, unpair and reconfigure",
                    "error": error.to_string(),
                });
            }
        }
        manager
    }

    /// The timing this manager runs with.
    pub fn options(&self) -> &PeerOptions {
        &self.options
    }

    /// The shared context of a supervised peer (its remote catalog survives
    /// reconnects).
    pub fn context(&self, installation: &str) -> Option<Arc<PeerContext>> {
        self.peers
            .lock()
            .get(installation)
            .map(|handle| handle.supervisor.context.clone())
    }

    /// Every supervised peer, by installation.
    pub fn peers(&self) -> Vec<PeerView> {
        let supervisors: Vec<Arc<Supervisor>> = self
            .peers
            .lock()
            .values()
            .map(|handle| handle.supervisor.clone())
            .collect();
        supervisors.iter().map(|s| s.view()).collect()
    }

    /// One supervised peer.
    pub fn peer(&self, installation: &str) -> Option<PeerView> {
        let supervisor = self
            .peers
            .lock()
            .get(installation)
            .map(|handle| handle.supervisor.clone())?;
        Some(supervisor.view())
    }

    /// The catalog generation last reconciled.
    pub fn reconciled_generation(&self) -> u64 {
        self.reconciled.load(Ordering::Acquire)
    }

    fn stopping_error() -> ErrorWire {
        ErrorWire::coded("ORCHESTRATOR_STOPPING", "the peer stack is shut down")
    }

    /// Brings the supervisors in line with the catalog and returns the
    /// generation reconciled. The snapshot is taken under the serializing
    /// lock, so a reconciler never applies an older catalog than the one
    /// before it (a caller's own change is always in it: `apply` publishes
    /// before it returns).
    fn reconcile(&self) -> u64 {
        let _serial = lock(&self.reconciling);
        let snapshot = self.ctx.catalog.snapshot();
        let desired = desired_peers(&snapshot, &self.ctx.installation);
        let (stopping, starting) = {
            let mut peers = self.peers.lock();
            let mut stopping: Vec<(PeerHandle, StopReason)> = Vec::new();
            let current: Vec<String> = peers.keys().cloned().collect();
            for installation in current {
                let reason = match desired.get(&installation) {
                    None => StopReason::Removed,
                    Some(record) if *record != peers[&installation].record => {
                        StopReason::Reconfigured
                    }
                    Some(_) => continue,
                };
                if let Some(handle) = peers.remove(&installation) {
                    stopping.push((handle, reason));
                }
            }
            let starting: Vec<PeerRecord> = desired
                .values()
                .filter(|record| !peers.contains_key(&record.installation))
                .cloned()
                .collect();
            (stopping, starting)
        };
        // Stop every outgoing supervisor first (their sessions wind down in
        // parallel), then wait for them under one deadline.
        let deadline = Instant::now() + self.options.join;
        for (handle, reason) in &stopping {
            handle.supervisor.begin_stop(*reason);
        }
        let mut previous: BTreeMap<String, Arc<RemoteCatalog>> = BTreeMap::new();
        for (handle, reason) in stopping {
            handle.supervisor.finish_stop(deadline);
            let installation = handle.record.installation.clone();
            if reason == StopReason::Removed {
                self.ctx
                    .status
                    .record(StatusEvent::PeerRemoved { installation });
            } else {
                previous.insert(installation, handle.supervisor.context.catalog.load());
            }
        }
        if !self.closed.load(Ordering::Acquire) {
            for record in starting {
                let catalog = previous.remove(&record.installation);
                self.spawn(record, catalog);
            }
        }
        self.reconciled
            .fetch_max(snapshot.generation, Ordering::AcqRel);
        snapshot.generation
    }

    fn spawn(&self, record: PeerRecord, catalog: Option<Arc<RemoteCatalog>>) {
        let installation = record.installation.clone();
        let endpoint = record.endpoint.clone();
        let supervisor = Supervisor::new(
            self.ctx.clone(),
            self.options,
            record.clone(),
            self.cancel.child(),
            catalog,
            self.meta.clone(),
        );
        {
            // Checked under the map lock: `shutdown` sets `closed` before it
            // takes the map, so a supervisor is either taken by it or never
            // inserted (and its token, a child of the cancelled manager
            // token, would end its lanes at once).
            let mut peers = self.peers.lock();
            if self.closed.load(Ordering::Acquire) {
                return;
            }
            peers.insert(
                installation.clone(),
                PeerHandle {
                    record,
                    supervisor: supervisor.clone(),
                },
            );
        }
        self.ctx.status.record(StatusEvent::PeerConfigured {
            installation,
            endpoint,
        });
        supervisor.start_lanes();
    }

    /// A short-lived `probe` session (pairing and `remote_list`).
    fn probe(
        &self,
        endpoint: &str,
        code: &str,
        expected: Option<&str>,
        pool: u32,
    ) -> Result<(Conn, session::ServerHello), ErrorWire> {
        let hello = session::client_hello(&self.ctx.installation, LaneRole::Probe, None, pool);
        session::dial_expecting(
            endpoint,
            hello,
            &CodeAuth {
                code: code.to_owned(),
            },
            expected,
            None,
            self.options.conn,
        )
        .map_err(|error| error.to_wire())
    }
}

impl NetworkApi for PeerManager {
    /// A probe session learns the remote installation (and refuses this
    /// installation, `CANNOT_PAIR_WITH_SELF`), then the peer is stored with a
    /// new revision (its lanes restart) and supervised. Other endpoints that
    /// were paired with the same installation are replaced.
    fn pair(
        &self,
        endpoint: &str,
        code: &str,
        connections: Option<u32>,
    ) -> Result<PairResult, ErrorWire> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Self::stopping_error());
        }
        let count = match connections {
            Some(count) => count,
            None => u32::try_from(self.ctx.config.network.connections).unwrap_or(u32::MAX),
        };
        if !(1..=16).contains(&count) {
            return Err(ErrorWire::coded(
                "INVALID_CONNECTION_COUNT",
                format!("{count} connections; 1 to 16 are allowed"),
            ));
        }
        let pool = count.max(limits::MIN_POOL);
        let (conn, server) = self.probe(endpoint, code, None, pool)?;
        conn.close();
        let installation = server.installation;
        if installation == self.ctx.installation {
            return Err(ErrorWire::coded(
                "CANNOT_PAIR_WITH_SELF",
                "the endpoint is this installation",
            ));
        }
        let snapshot = self.ctx.catalog.apply(CatalogCmd::UpsertPeer {
            record: PeerRecord {
                endpoint: endpoint.to_owned(),
                code: code.to_owned(),
                installation: installation.clone(),
                connections: count,
                revision: uuid::Uuid::new_v4().to_string(),
            },
        })?;
        let replaced: Vec<String> = snapshot
            .peers
            .iter()
            .filter(|peer| peer.installation == installation && peer.endpoint != endpoint)
            .map(|peer| peer.endpoint.clone())
            .collect();
        for old in replaced {
            self.ctx
                .catalog
                .apply(CatalogCmd::RemovePeer { endpoint: old })?;
        }
        self.reconcile();
        Ok(PairResult {
            endpoint: endpoint.to_owned(),
            installation,
            connections: pool,
            configured_connections: count,
            reserved_connections: 1,
        })
    }

    /// A probe session to a paired endpoint (`PEER_INSTALLATION_CHANGED` when
    /// it is no longer the paired installation) and one `catalog.list`.
    fn remote_list(&self, endpoint: &str) -> Result<Vec<PublishedEntry>, ErrorWire> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Self::stopping_error());
        }
        let snapshot = self.ctx.catalog.snapshot();
        let record = snapshot
            .peer_by_endpoint(endpoint)
            .cloned()
            .ok_or_else(|| {
                ErrorWire::coded("PEER_NOT_PAIRED", format!("{endpoint} is not paired"))
            })?;
        let expected = (!record.installation.is_empty()).then_some(record.installation.as_str());
        let pool = record.connections.max(limits::MIN_POOL);
        let (conn, _) = self.probe(endpoint, &record.code, expected, pool)?;
        let listed = list_catalog(&conn, None);
        conn.close();
        listed
            .map(|reply| reply.entries)
            .map_err(|error| error.to_wire())
    }

    /// Removes the peer, then stops and joins its lanes (≤ 5 s). Unpairing an
    /// endpoint that is not paired succeeds (the legacy `Unpair` did).
    fn unpair(&self, endpoint: &str) -> Result<(), ErrorWire> {
        self.ctx.catalog.apply(CatalogCmd::RemovePeer {
            endpoint: endpoint.to_owned(),
        })?;
        self.reconcile();
        Ok(())
    }

    /// Reconciles with the catalog, and rechecks every `Incompatible` or
    /// `Unauthorized` peer now (§8: "recheck … on config change"): one lane
    /// of each re-dials at once. Automatic reconciles (catalog generation
    /// changes, which our own `peer_meta` writes also cause) never do this.
    fn reconfigure(&self) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        self.reconcile();
        let supervisors: Vec<Arc<Supervisor>> = self
            .peers
            .lock()
            .values()
            .map(|handle| handle.supervisor.clone())
            .collect();
        for supervisor in supervisors {
            supervisor.recheck_now();
        }
    }

    /// GOAWAY to every session, cancel every lane, join within `deadline`.
    fn shutdown(&self, deadline: Duration) {
        let until = Instant::now() + deadline;
        self.closed.store(true, Ordering::Release);
        let handles: Vec<PeerHandle> = {
            let mut peers = self.peers.lock();
            std::mem::take(&mut *peers).into_values().collect()
        };
        for handle in &handles {
            handle.supervisor.begin_stop(StopReason::Shutdown);
        }
        self.cancel.cancel();
        for handle in handles {
            handle.supervisor.finish_stop(until);
        }
        self.meta.close();
        let watcher = lock(&self.watcher).take();
        if let Some(watcher) = watcher {
            // The watcher re-checks its token every CATALOG_WAIT and pokes
            // `watcher_done` as it ends.
            loop {
                let seen = self.watcher_done.count();
                let now = Instant::now();
                if seen > 0 || watcher.is_finished() || now >= until {
                    break;
                }
                self.watcher_done.wait(seen, until - now);
            }
            // Once it has poked, the thread only returns: the join is short.
            if self.watcher_done.count() > 0 || watcher.is_finished() {
                let _ = watcher.join();
            }
        }
    }
}

impl std::fmt::Debug for PeerManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerManager")
            .field("installation", &self.ctx.installation)
            .field("peers", &self.peers.lock().keys().collect::<Vec<_>>())
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// The record supervised for each installation: the first record by endpoint
/// (the catalog lists peers by endpoint), skipping this installation.
fn desired_peers(snapshot: &CatalogSnapshot, local: &str) -> BTreeMap<String, PeerRecord> {
    let mut desired: BTreeMap<String, PeerRecord> = BTreeMap::new();
    let mut records: Vec<&PeerRecord> = snapshot.peers.iter().collect();
    records.sort_by(|a, b| a.endpoint.cmp(&b.endpoint));
    for record in records {
        if record.installation.is_empty() || record.installation == local {
            continue;
        }
        desired
            .entry(record.installation.clone())
            .or_insert_with(|| record.clone());
    }
    desired
}

/// The `peer-manager` thread: follows the catalog generation.
fn watch_catalog(
    manager: Weak<PeerManager>,
    catalog: Arc<dyn CatalogApi>,
    cancel: Cancel,
    mut generation: u64,
) {
    while !cancel.is_cancelled() {
        let started = Instant::now();
        let snapshot = catalog.wait_newer(generation, CATALOG_WAIT);
        if cancel.is_cancelled() {
            break;
        }
        if snapshot.generation == generation {
            // A catalog that answers at once without a change (its thread
            // died): do not spin.
            if started.elapsed() < CATALOG_WAIT / 10 && cancel.sleep(CATALOG_WAIT) {
                break;
            }
            continue;
        }
        generation = snapshot.generation;
        let Some(manager) = manager.upgrade() else {
            break;
        };
        if manager.closed.load(Ordering::Acquire) {
            break;
        }
        if manager.reconciled_generation() < generation {
            manager.reconcile();
        }
    }
}

// ---------------------------------------------------------------------------
// Supervisor

/// A peer-level refusal every unconnected lane waits for.
#[derive(Clone, Debug)]
struct Block {
    state: LaneState,
    error: ErrorWire,
    until: Instant,
    until_ms: u64,
    generation: u64,
}

#[derive(Debug, Default)]
struct Gate {
    block: Option<Block>,
    /// A lane is re-dialing after the block expired.
    probing: bool,
    generation: u64,
}

/// How a failed dial is handled.
enum Failure {
    /// Peer-level: `Incompatible` or `Unauthorized`, rechecked on a timer.
    Block(LaneState, ErrorWire),
    /// Lane-level: back off and re-dial.
    Retry(ErrorWire),
}

fn classify_dial(error: &DialError) -> Failure {
    let wire = error.to_wire();
    match error {
        DialError::NeedsUpgrade { .. } | DialError::Unsupported { .. } => {
            Failure::Block(LaneState::Incompatible, wire)
        }
        DialError::Unauthorized(_) => Failure::Block(LaneState::Unauthorized, wire),
        _ => match wire.class {
            ErrorClass::Incompatible => Failure::Block(LaneState::Incompatible, wire),
            ErrorClass::NeedsUserAction | ErrorClass::Fatal => {
                Failure::Block(LaneState::Unauthorized, wire)
            }
            ErrorClass::Contention | ErrorClass::Transient => Failure::Retry(wire),
        },
    }
}

struct Supervisor {
    ctx: Arc<Ctx>,
    options: PeerOptions,
    record: PeerRecord,
    context: Arc<PeerContext>,
    pool: u32,
    cancel: Cancel,
    stop_reason: Tracked<StopReason>,
    gate: Tracked<Gate>,
    gate_changed: Notify,
    lanes: Tracked<Vec<LaneView>>,
    conns: Tracked<BTreeMap<u32, Conn>>,
    live: AtomicU32,
    exited: Notify,
    threads: Mutex<Vec<JoinHandle<()>>>,
    /// Whether this supervisor has emitted a `PeerCatalog` yet.
    catalog_emitted: AtomicBool,
    meta: Arc<MetaWriter>,
}

impl Supervisor {
    fn new(
        ctx: Arc<Ctx>,
        options: PeerOptions,
        record: PeerRecord,
        cancel: Cancel,
        catalog: Option<Arc<RemoteCatalog>>,
        meta: Arc<MetaWriter>,
    ) -> Arc<Supervisor> {
        let pool = record.connections.clamp(limits::MIN_POOL, 16);
        let context = Arc::new(PeerContext {
            installation: record.installation.clone(),
            endpoint: record.endpoint.clone(),
            local_installation: ctx.installation.clone(),
            pool,
            catalog: Snapshot::from_arc(catalog.unwrap_or_default()),
        });
        let now_ms = ctx.clock.now_ms();
        let lanes = (0..pool)
            .map(|lane| LaneView {
                lane,
                role: role_of(lane),
                spec: spec_of(lane, pool),
                state: LaneState::Idle,
                since_ms: now_ms,
                error: None,
                backoff_until_ms: None,
                attempt: 0,
                dials: 0,
                connects: 0,
                rtt_ms: None,
                protocol: None,
            })
            .collect();
        let supervisor = Arc::new(Supervisor {
            ctx,
            options,
            record,
            context,
            pool,
            cancel,
            stop_reason: Tracked::new("peer.stop_reason", StopReason::Shutdown),
            gate: Tracked::new("peer.gate", Gate::default()),
            gate_changed: Notify::new(),
            lanes: Tracked::new("peer.lanes", lanes),
            conns: Tracked::new("peer.conns", BTreeMap::new()),
            live: AtomicU32::new(0),
            exited: Notify::new(),
            threads: Mutex::new(Vec::new()),
            catalog_emitted: AtomicBool::new(false),
            meta,
        });
        // Lanes parked on the gate wake when the supervisor is cancelled. The
        // hook holds a weak reference: the token outlives nothing it needs.
        let weak = Arc::downgrade(&supervisor);
        supervisor.cancel.on_cancel(Box::new(move || {
            if let Some(supervisor) = weak.upgrade() {
                supervisor.gate_changed.poke();
            }
        }));
        supervisor
    }

    fn installation(&self) -> &str {
        &self.record.installation
    }

    fn start_lanes(self: &Arc<Self>) {
        for lane in 0..self.pool {
            let supervisor = self.clone();
            let name = if lane == 0 {
                thread_name("peer", self.installation(), None)
            } else {
                thread_name("lane", self.installation(), Some(lane))
            };
            self.live.fetch_add(1, Ordering::AcqRel);
            match thread::Builder::new()
                .name(name)
                .spawn(move || run_lane(supervisor, lane))
            {
                Ok(handle) => lock(&self.threads).push(handle),
                Err(error) => {
                    self.live.fetch_sub(1, Ordering::AcqRel);
                    self.exited.poke();
                    event!(error, CONNECTION_LOST, {
                        "peer": self.installation(),
                        "lane": lane,
                        "detail": "cannot start the lane thread",
                        "error": error.to_string(),
                    });
                }
            }
        }
    }

    fn view(&self) -> PeerView {
        PeerView {
            installation: self.record.installation.clone(),
            label: self
                .ctx
                .catalog
                .snapshot()
                .peer_meta
                .get(&self.record.installation)
                .and_then(|meta| meta.display_name.clone()),
            endpoint: self.record.endpoint.clone(),
            revision: self.record.revision.clone(),
            pool: self.pool,
            lanes: self.lanes.lock().clone(),
            catalog: self.context.catalog.load(),
        }
    }

    /// Cancels every lane; the per-session hooks send `GOAWAY{reason}`.
    fn begin_stop(&self, reason: StopReason) {
        *self.stop_reason.lock() = reason;
        self.cancel.cancel();
    }

    /// Waits for the lanes to end by `deadline`, closing sessions outright
    /// `force_close_margin` before it; detaches lanes still running then.
    fn finish_stop(&self, deadline: Instant) {
        let force_at = deadline
            .checked_sub(self.options.force_close_margin)
            .unwrap_or(deadline);
        self.wait_lanes(force_at);
        if self.live.load(Ordering::Acquire) > 0 {
            let conns: Vec<Conn> = self.conns.lock().values().cloned().collect();
            for conn in conns {
                conn.close();
            }
            self.wait_lanes(deadline);
        }
        let handles: Vec<JoinHandle<()>> = std::mem::take(&mut *lock(&self.threads));
        let mut detached = 0;
        for handle in handles {
            if handle.is_finished() {
                let _ = handle.join();
            } else {
                detached += 1;
            }
        }
        if detached > 0 {
            event!(warn, REQUEST_TIMEOUT, {
                "peer": self.installation(),
                "detail": "lane threads did not end by the join deadline; detached",
                "lanes": detached,
            });
        }
    }

    fn wait_lanes(&self, until: Instant) {
        loop {
            let seen = self.exited.count();
            if self.live.load(Ordering::Acquire) == 0 {
                return;
            }
            let now = Instant::now();
            if now >= until {
                return;
            }
            self.exited.wait(seen, until - now);
        }
    }

    // -- lane state ---------------------------------------------------------

    /// Records a lane transition and emits `LaneState`.
    fn set_lane(
        &self,
        lane: u32,
        state: LaneState,
        error: Option<ErrorWire>,
        backoff_until_ms: Option<u64>,
        rtt_ms: Option<u64>,
    ) {
        let now_ms = self.ctx.clock.now_ms();
        let role = role_of(lane);
        {
            let mut lanes = self.lanes.lock();
            if let Some(view) = lanes.get_mut(lane as usize) {
                if view.state != state {
                    view.since_ms = now_ms;
                }
                if state == LaneState::Dialing {
                    view.dials += 1;
                }
                view.state = state;
                view.error = error.clone();
                view.backoff_until_ms = backoff_until_ms;
                if rtt_ms.is_some() || state != LaneState::Ready {
                    view.rtt_ms = rtt_ms;
                }
                if state != LaneState::Ready {
                    view.protocol = None;
                }
            }
        }
        self.ctx.status.record(StatusEvent::LaneState {
            installation: self.record.installation.clone(),
            lane,
            role,
            direction: Direction::Outbound,
            state,
            error,
            backoff_until_ms,
            rtt_ms,
        });
    }

    fn update_lane(&self, lane: u32, update: impl FnOnce(&mut LaneView)) {
        if let Some(view) = self.lanes.lock().get_mut(lane as usize) {
            update(view);
        }
    }

    // -- the peer-level gate ------------------------------------------------

    /// Waits until `lane` may dial: `Some(false)` with no block, `Some(true)`
    /// when this lane probes an expired block, `None` when cancelled. While
    /// it waits, the lane shows the block's state once per block.
    fn admit(&self, lane: u32, announced: &mut Option<u64>) -> Option<bool> {
        loop {
            if self.cancel.is_cancelled() {
                return None;
            }
            let seen = self.gate_changed.count();
            let (block, probing) = {
                let gate = self.gate.lock();
                (gate.block.clone(), gate.probing)
            };
            let Some(block) = block else {
                return Some(false);
            };
            let now = Instant::now();
            if now >= block.until && !probing {
                let mut gate = self.gate.lock();
                if !gate.probing
                    && gate
                        .block
                        .as_ref()
                        .is_some_and(|current| current.generation == block.generation)
                {
                    gate.probing = true;
                    return Some(true);
                }
                continue;
            }
            if *announced != Some(block.generation) {
                *announced = Some(block.generation);
                self.set_lane(
                    lane,
                    block.state,
                    Some(block.error.clone()),
                    Some(block.until_ms),
                    None,
                );
            }
            let wait = if now >= block.until {
                IDLE_WAIT
            } else {
                block.until - now
            };
            self.gate_changed.wait(seen, wait);
        }
    }

    /// A lane connected: the peer accepts us, any block is lifted.
    fn gate_success(&self) {
        let changed = {
            let mut gate = self.gate.lock();
            let changed = gate.block.is_some() || gate.probing;
            gate.block = None;
            gate.probing = false;
            if changed {
                gate.generation += 1;
            }
            changed
        };
        if changed {
            self.gate_changed.poke();
        }
    }

    /// A lane met a peer-level refusal: block every lane until the recheck.
    fn gate_block(&self, state: LaneState, error: ErrorWire, probe: bool) {
        let recheck = if state == LaneState::Incompatible {
            self.options.incompatible_recheck
        } else {
            self.options.unauthorized_recheck
        };
        let until_ms = self
            .ctx
            .clock
            .now_ms()
            .saturating_add(u64::try_from(recheck.as_millis()).unwrap_or(u64::MAX));
        let newly_incompatible = {
            let mut gate = self.gate.lock();
            let newly = state == LaneState::Incompatible
                && gate.block.as_ref().is_none_or(|old| {
                    old.state != LaneState::Incompatible || old.error.code != error.code
                });
            gate.generation += 1;
            gate.block = Some(Block {
                state,
                error: error.clone(),
                until: Instant::now() + recheck,
                until_ms,
                generation: gate.generation,
            });
            if probe {
                gate.probing = false;
            }
            newly
        };
        self.gate_changed.poke();
        event!(warn, (error.code.as_str()), {
            "peer": self.installation(),
            "endpoint": self.record.endpoint,
            "state": state,
            "recheck_ms": u64::try_from(recheck.as_millis()).unwrap_or(u64::MAX),
            "message": error.message,
        });
        if newly_incompatible {
            self.record_seen(Some(error.code));
        }
    }

    /// Expires the current block (if no probe runs yet): one lane re-dials.
    fn recheck_now(&self) {
        let now_ms = self.ctx.clock.now_ms();
        let expired = {
            let mut gate = self.gate.lock();
            let probing = gate.probing;
            match gate.block.as_mut() {
                Some(block) if !probing => {
                    block.until = Instant::now();
                    block.until_ms = now_ms;
                    true
                }
                _ => false,
            }
        };
        if expired {
            self.gate_changed.poke();
        }
    }

    /// A probe failed transiently: the peer's class is unknown again, so each
    /// lane falls back to its own backoff.
    fn gate_transient(&self, probe: bool) {
        if !probe {
            return;
        }
        {
            let mut gate = self.gate.lock();
            gate.block = None;
            gate.probing = false;
            gate.generation += 1;
        }
        self.gate_changed.poke();
    }

    /// `peer_meta`: seen now; `incompatible` replaces the stored code.
    /// Queued for the `peer-meta` thread: the lane never waits for the
    /// catalog.
    fn record_seen(&self, incompatible: Option<String>) {
        self.meta.record(PeerSeen {
            installation: self.record.installation.clone(),
            at_ms: self.ctx.clock.now_ms(),
            incompatible,
        });
    }

    // -- dialing ------------------------------------------------------------

    fn dial(&self, lane: u32) -> Result<(Conn, session::ServerHello), DialError> {
        let role = role_of(lane);
        let hello = session::client_hello(
            &self.ctx.installation,
            role,
            spec_of(lane, self.pool),
            self.pool,
        );
        let code = CodeAuth {
            code: self.record.code.clone(),
        };
        let handshaking = || self.set_lane(lane, LaneState::Handshaking, None, None, None);
        let auth = AnnounceHandshake {
            inner: &code,
            on_key: &handshaking,
        };
        session::dial_expecting(
            &self.record.endpoint,
            hello,
            &auth,
            Some(&self.record.installation),
            None,
            self.options.conn,
        )
    }

    // -- a Ready lane -------------------------------------------------------

    /// Runs one established session until it closes (`Some(reason)`) or the
    /// supervisor is cancelled (`None`). Always leaves the session closed or
    /// closing and its runner cancelled.
    fn serve(self: &Arc<Self>, lane: u32, conn: &Conn) -> Option<ErrorWire> {
        let session_cancel = self.cancel.child();
        {
            // GOAWAY as soon as the supervisor stops (a no-op once closed).
            let weak = Arc::downgrade(self);
            let conn = conn.clone();
            session_cancel.on_cancel(Box::new(move || {
                let reason = weak.upgrade().map_or(StopReason::Shutdown, |supervisor| {
                    *supervisor.stop_reason.lock()
                });
                let (code, message) = reason.goaway();
                conn.goaway(code, message);
            }));
        }
        self.conns.lock().insert(lane, conn.clone());
        let health = conn.health();
        self.update_lane(lane, |view| view.connects += 1);
        self.set_lane(lane, LaneState::Ready, None, None, health.rtt_ms);
        if lane == 0 {
            self.record_seen(None);
        }
        let runner = self.spawn_runner(lane, conn, &session_cancel);
        let ended = if lane == 0 {
            self.control_loop(lane, conn, &session_cancel)
        } else {
            self.bulk_loop(lane, conn, &session_cancel)
        };
        session_cancel.cancel();
        if ended.is_none() {
            // Stopping: the peer closes once idle after our GOAWAY; the
            // manager closes sessions that outlast the join deadline.
            if conn.wait_closed(self.options.join).is_none() {
                conn.close();
            }
        }
        if let Some((done, handle)) = runner {
            match done.recv_timeout(self.options.runner_join) {
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    event!(warn, REQUEST_TIMEOUT, {
                        "peer": self.installation(),
                        "lane": lane,
                        "detail": "the lane runner did not end after its session; detached",
                    });
                }
                _ => {
                    let _ = handle.join();
                }
            }
        }
        self.conns.lock().remove(&lane);
        ended
    }

    /// Starts `scheduler::run_control` (lane 0) or `objects::run_bulk_lane`
    /// on its own thread. The receiver disconnects when the runner returns.
    fn spawn_runner(
        self: &Arc<Self>,
        lane: u32,
        conn: &Conn,
        cancel: &Cancel,
    ) -> Option<(mpsc::Receiver<()>, JoinHandle<()>)> {
        let (done_tx, done_rx) = mpsc::channel::<()>();
        let ctx = self.ctx.clone();
        let peer = self.context.clone();
        let spec = spec_of(lane, self.pool);
        let owned = conn.clone();
        let cancel = cancel.clone();
        let installation = self.record.installation.clone();
        let name = match spec {
            None => thread_name("ctl", &installation, None),
            Some(_) => thread_name("bulk", &installation, Some(lane)),
        };
        let spawned = thread::Builder::new().name(name).spawn(move || {
            let _done = done_tx;
            let session = owned.clone();
            let token = cancel.clone();
            let outcome = catch_unwind(AssertUnwindSafe(move || match spec {
                None => scheduler::run_control(ctx, peer, session, token),
                Some(spec) => objects::run_bulk_lane(ctx, peer, spec, session, token),
            }));
            if !cancel.is_cancelled() && owned.closed().is_none() {
                // The runner gave up on a live session: close it, so the lane
                // backs off and restarts the runner on a fresh session.
                event!(warn, WS_CLOSED, {
                    "peer": installation,
                    "lane": lane,
                    "detail": "the lane runner ended while its session was open; reconnecting",
                    "panicked": outcome.is_err(),
                });
                owned.close();
            }
        });
        match spawned {
            Ok(handle) => Some((done_rx, handle)),
            Err(error) => {
                event!(error, CONNECTION_LOST, {
                    "peer": self.installation(),
                    "lane": lane,
                    "detail": "cannot start the lane runner; reconnecting",
                    "error": error.to_string(),
                });
                conn.close();
                None
            }
        }
    }

    /// Lane 0: the catalog client and the RTT refresh until the session ends.
    fn control_loop(&self, lane: u32, conn: &Conn, cancel: &Cancel) -> Option<ErrorWire> {
        let mut since: Option<u64> = None;
        let mut failures = 0u32;
        let mut next_poll = Instant::now();
        let mut next_rtt = Instant::now() + self.options.rtt_refresh;
        let mut rtt = conn.health().rtt_ms;
        loop {
            if cancel.is_cancelled() {
                return None;
            }
            if let Some(reason) = conn.closed() {
                return Some(reason);
            }
            if Instant::now() >= next_poll {
                self.poll_catalog(conn, &mut since, &mut failures);
                next_poll = Instant::now() + self.options.catalog_poll;
            }
            if Instant::now() >= next_rtt {
                self.refresh_rtt(lane, conn, &mut rtt);
                next_rtt = Instant::now() + self.options.rtt_refresh;
            }
            let wait = next_poll
                .min(next_rtt)
                .saturating_duration_since(Instant::now());
            if let Some(reason) = conn.wait_closed(wait) {
                return (!cancel.is_cancelled()).then_some(reason);
            }
        }
    }

    /// A bulk lane: the RTT refresh until the session ends.
    fn bulk_loop(&self, lane: u32, conn: &Conn, cancel: &Cancel) -> Option<ErrorWire> {
        let mut rtt = conn.health().rtt_ms;
        loop {
            if cancel.is_cancelled() {
                return None;
            }
            if let Some(reason) = conn.wait_closed(self.options.rtt_refresh) {
                return (!cancel.is_cancelled()).then_some(reason);
            }
            if cancel.is_cancelled() {
                return None;
            }
            self.refresh_rtt(lane, conn, &mut rtt);
        }
    }

    /// Re-emits `Ready` when the session RTT moved noticeably.
    fn refresh_rtt(&self, lane: u32, conn: &Conn, last: &mut Option<u64>) {
        let Some(now) = conn.health().rtt_ms else {
            return;
        };
        let moved = match *last {
            None => true,
            Some(previous) => {
                let delta = now.abs_diff(previous);
                delta >= RTT_MIN_DELTA_MS && delta.saturating_mul(4) >= previous
            }
        };
        if moved && conn.closed().is_none() {
            *last = Some(now);
            self.set_lane(lane, LaneState::Ready, None, None, Some(now));
        }
    }

    /// One `catalog.list`. The first listing of each session asks for the
    /// whole catalog (`since: None`).
    fn poll_catalog(&self, conn: &Conn, since: &mut Option<u64>, failures: &mut u32) {
        match list_catalog(conn, *since) {
            Ok(reply) => {
                *failures = 0;
                // `unchanged` carries no entries: keep the copy we have.
                if reply.unchanged {
                    return;
                }
                *since = Some(reply.generation);
                self.store_catalog(reply);
            }
            Err(error) => {
                *failures = failures.saturating_add(1);
                if conn.closed().is_some() {
                    return;
                }
                let wire = error.to_wire();
                event!(warn, (wire.code.as_str()), {
                    "peer": self.installation(),
                    "op": ops::CATALOG_LIST,
                    "failures": *failures,
                    "message": wire.message,
                });
            }
        }
    }

    fn store_catalog(&self, reply: CatalogListReply) {
        let current = self.context.catalog.load();
        let first = !self.catalog_emitted.swap(true, Ordering::AcqRel);
        let changed = current.entries != reply.entries;
        if changed || current.generation != reply.generation || current.at_ms == 0 {
            self.context.catalog.store(RemoteCatalog {
                generation: reply.generation,
                at_ms: self.ctx.clock.now_ms(),
                entries: reply.entries.clone(),
            });
        }
        if changed || first {
            self.ctx.status.record(StatusEvent::PeerCatalog {
                installation: self.record.installation.clone(),
                entries: reply.entries,
            });
        }
    }
}

/// Queued `peer_meta` writes, applied by the `peer-meta` thread, so a slow or
/// wedged catalog actor never holds a lane (§2.3: a catalog fault stops
/// management writes, never lanes). The queue is bounded; a write that does
/// not fit is dropped and logged (the next session establishment records the
/// peer again).
struct MetaWriter {
    tx: Mutex<Option<mpsc::SyncSender<PeerSeen>>>,
}

/// One `CatalogCmd::RecordPeerSeen`.
struct PeerSeen {
    installation: String,
    at_ms: u64,
    incompatible: Option<String>,
}

/// Queued `peer_meta` writes before new ones are dropped.
const META_QUEUE: usize = 64;

impl MetaWriter {
    fn start(catalog: Arc<dyn CatalogApi>) -> Arc<MetaWriter> {
        let (tx, rx) = mpsc::sync_channel::<PeerSeen>(META_QUEUE);
        let spawned = thread::Builder::new()
            .name("peer-meta".into())
            .spawn(move || {
                while let Ok(seen) = rx.recv() {
                    let cmd = CatalogCmd::RecordPeerSeen {
                        installation: seen.installation,
                        at_ms: seen.at_ms,
                        display_name: None,
                        incompatible: seen.incompatible,
                    };
                    if let Err(error) = catalog.apply(cmd) {
                        event!(warn, (error.code.as_str()), {
                            "detail": "cannot record a peer in peer_meta",
                            "message": error.message,
                        });
                    }
                }
            });
        let tx = match spawned {
            Ok(_) => Some(tx),
            Err(error) => {
                event!(error, CATALOG_UNAVAILABLE, {
                    "detail": "cannot start the peer-meta thread; peer_meta is not updated",
                    "error": error.to_string(),
                });
                None
            }
        };
        Arc::new(MetaWriter { tx: Mutex::new(tx) })
    }

    fn record(&self, seen: PeerSeen) {
        let sent = lock(&self.tx).as_ref().map(|tx| tx.try_send(seen));
        if let Some(Err(mpsc::TrySendError::Full(_))) = sent {
            event!(warn, CATALOG_UNAVAILABLE, {
                "detail": "the peer_meta queue is full; a peer_meta update was dropped",
            });
        }
    }

    /// No more writes; the thread ends once the queue is drained.
    fn close(&self) {
        lock(&self.tx).take();
    }
}

/// Emits `Handshaking` when the handshake reaches the authentication step
/// (the server's `HELLO_ACK` arrived and the dialer derives its key).
struct AnnounceHandshake<'a> {
    inner: &'a CodeAuth,
    on_key: &'a (dyn Fn() + Sync),
}

impl ClientAuth for AnnounceHandshake<'_> {
    fn key(&self, salt: &str) -> Result<[u8; 32], ErrorWire> {
        (self.on_key)();
        self.inner.key(salt)
    }
}

/// Ends a lane thread: `Closed`, one less live lane.
struct LaneExit {
    supervisor: Arc<Supervisor>,
    lane: u32,
}

impl Drop for LaneExit {
    fn drop(&mut self) {
        let supervisor = &self.supervisor;
        supervisor.conns.lock().remove(&self.lane);
        supervisor.set_lane(self.lane, LaneState::Closed, None, None, None);
        supervisor.live.fetch_sub(1, Ordering::AcqRel);
        supervisor.exited.poke();
    }
}

/// One lane's thread: gate, dial, serve, back off, until cancelled.
fn run_lane(supervisor: Arc<Supervisor>, lane: u32) {
    let _exit = LaneExit {
        supervisor: supervisor.clone(),
        lane,
    };
    let mut backoff = supervisor.options.lane_backoff();
    let mut announced: Option<u64> = None;
    loop {
        let Some(probe) = supervisor.admit(lane, &mut announced) else {
            return;
        };
        supervisor.set_lane(lane, LaneState::Dialing, None, None, None);
        let dialed = supervisor.dial(lane);
        if supervisor.cancel.is_cancelled() {
            if let Ok((conn, _)) = dialed {
                conn.close();
            }
            return;
        }
        let error = match dialed {
            Ok((conn, server)) => {
                supervisor.gate_success();
                backoff.success(Instant::now());
                supervisor.update_lane(lane, |view| view.attempt = backoff.attempt());
                let protocol = format!("{}/{}", server.protocol, server.version);
                supervisor.update_lane(lane, |view| view.protocol = Some(protocol));
                match supervisor.serve(lane, &conn) {
                    None => return,
                    Some(reason) => reason,
                }
            }
            Err(error) => match classify_dial(&error) {
                Failure::Block(state, wire) => {
                    supervisor.gate_block(state, wire, probe);
                    continue;
                }
                Failure::Retry(wire) => {
                    supervisor.gate_transient(probe);
                    wire
                }
            },
        };
        let delay = backoff.failure(Instant::now());
        let until_ms = supervisor
            .ctx
            .clock
            .now_ms()
            .saturating_add(u64::try_from(delay.as_millis()).unwrap_or(u64::MAX));
        supervisor.update_lane(lane, |view| view.attempt = backoff.attempt());
        supervisor.set_lane(lane, LaneState::Backoff, Some(error), Some(until_ms), None);
        if supervisor.cancel.sleep(delay) {
            return;
        }
    }
}

/// `<prefix>-<installation>[-<lane>]`, without control characters (a thread
/// name may not hold a NUL) and with the installation cut to 64 characters.
fn thread_name(prefix: &str, installation: &str, lane: Option<u32>) -> String {
    let installation: String = installation
        .chars()
        .filter(|c| !c.is_control())
        .take(64)
        .collect();
    match lane {
        Some(lane) => format!("{prefix}-{installation}-{lane}"),
        None => format!("{prefix}-{installation}"),
    }
}

fn role_of(lane: u32) -> LaneRole {
    if lane == 0 {
        LaneRole::Control
    } else {
        LaneRole::Bulk
    }
}

/// Bulk lane `i` of a pool of `pool` sessions is `{index: i - 1, count: pool - 1}`.
fn spec_of(lane: u32, pool: u32) -> Option<LaneSpec> {
    (lane > 0).then(|| LaneSpec::new(lane - 1, pool.saturating_sub(1)))
}

/// One `catalog.list{since}` on `conn` (10 s).
pub fn list_catalog(
    conn: &Conn,
    since: Option<u64>,
) -> Result<CatalogListReply, session::CallError> {
    let header = serde_json::to_value(CatalogListRequest { since }).unwrap_or(Value::Null);
    let reply = conn.call(
        ops::CATALOG_LIST,
        header,
        Vec::new(),
        Priority::Control,
        deadlines::CATALOG_LIST,
    )?;
    parse_catalog_reply(&reply).map_err(session::CallError::Remote)
}

/// A `catalog.list` reply: the body when there is one, else the header.
pub fn parse_catalog_reply(reply: &Reply) -> Result<CatalogListReply, ErrorWire> {
    let parsed = if reply.body.is_empty() {
        serde_json::from_value::<CatalogListReply>(reply.header.clone())
    } else {
        serde_json::from_slice::<CatalogListReply>(&reply.body)
    };
    parsed.map_err(|error| {
        ErrorWire::coded(
            "INVALID_REPLY",
            format!("{} reply does not parse: {error}", ops::CATALOG_LIST),
        )
    })
}
