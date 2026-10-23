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
//! | `Handshaking` | the ordinary credential exchange | `Ready`, or a failure below |
//! | `Ready` | an authenticated session | `Backoff` when the session closes |
//! | `Backoff` | a transient failure or a closed session | `Dialing` after 1 s → 60 s (±20 %), or on the peer's shared cadence while its network is down (below); at once when a sibling lane connects, unless the lane is backing off after a session the peer ended (below); the attempt count resets once the lane was `Ready` 5 s (healthy), and only then |
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
//! **Network loss** (WP5.3). The lab's 45 s Wi-Fi outage reconnected 36 s
//! after the restore: each lane had doubled its own backoff through the
//! outage (1, 2, 4, …, 32 s), and redialed on its own timer. Now:
//!
//! * A lane that was healthy (`Ready` for [`LANE_HEALTHY_AFTER`], 5 s) and
//!   loses its session by the transport (`CONNECTION_LOST`,
//!   `TRANSPORT_DEAD`, `WS_CLOSED`; not a `GOAWAY`, whose code says why,
//!   and not a close of this side's own: a lane runner that gave up)
//!   starts the peer's *network loss*. Until a lane connects again, every
//!   lane whose dial reaches nothing (`WS_CONNECT_FAILED`, a handshake step
//!   without an answer) or whose session the transport loses redials on one
//!   cadence shared by the peer's lanes: the first redial a base delay (1 s)
//!   after the loss, then one per *spacing*, the outage's age over
//!   [`NETWORK_SPACING_RATIO`] (30) between the base and the cap (1 s for
//!   the first 30 s, 1.5 s at 45 s, 4 s at 2 min, 60 s from 30 min on), each
//!   ±20 %. A short outage is found within a spacing of its end, whatever
//!   kind it is (a refused dial fails at once; a dial into a silent link
//!   hangs until its handshake step times out, and the next slot goes to
//!   another lane meanwhile), while a long one costs one dial per peer per
//!   spacing instead of one per lane per backoff. Paced redials do not
//!   escalate the lane's own backoff.
//! * When a lane connects, the peer is reachable: its network loss ends,
//!   every sibling waiting after a dial that failed (it reached no session)
//!   or on the down peer's cadence dials at once, and every sibling dial
//!   that is stale (begun before this one, no `HELLO_ACK` yet, running
//!   500 ms longer than this one took) is interrupted and redialed at once.
//!   A dial that fails while a sibling connects retries at once too. A
//!   sibling backing off after a session that ended otherwise (one the peer
//!   closed before it was healthy) keeps its delay: lanes of a flapping
//!   acceptor that woke each other on every connect would redial it at one
//!   session per dial time for ever, while their own backoffs escalate.
//! * Everything else keeps the per-lane backoff: a peer never reached since
//!   the supervisor started, a session the peer closes before it was
//!   healthy (a flapping acceptor escalates), a `GOAWAY`, a refusal.
//!
//! Every redial is a fresh session (invariant 16): nothing in flight on a
//! lost session is replayed; the lane runners start afresh on the new one.
//!
//! On `Ready` the lane hands its [`Conn`] to the lane runner on its own thread
//! with a per-session child [`Cancel`]: `scheduler::run_control` on lane 0
//! (`ctl-<installation>`), `objects::run_bulk_lane` on bulk lanes
//! (`bulk-<installation>-<lane>`). The lane thread watches the session; when
//! it closes, the runner is cancelled, joined (≤ 1 s, then detached) and the
//! lane backs off and re-dials with a fresh session. A runner that returns
//! while its session is open (or panics) gets its session closed, so the
//! lane restarts it after a backoff (crash-only, §2.2 rule 6); that close
//! is this side's, never a network loss. Runners must
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
//! session once it is idle after a GOAWAY, which ends the lane. Every
//! supervisor stopped together shares one deadline and the stop runs in
//! phases across all of them: wait for their lanes until one second before
//! the deadline, close every session still open, wait until the deadline,
//! then *retire* each supervisor. Retiring reports `Closed` for every lane
//! still running and mutes the supervisor: no status event of it is recorded
//! afterwards. A reconciler retires a supervisor before it records
//! `PeerRemoved` or spawns the replacement, so a late lane can neither
//! recreate an unpaired peer in the hub nor overwrite its successor's lanes.
//! A lane inside a dial is interrupted by the supervisor's token
//! (`session::dial_cancellable`, CCR-WP2.6-2 applied by WP3.1): its socket is
//! shut down at once, and only a TCP connect in progress (≤ 3 s per address)
//! runs to its end. Lane threads that have still not ended by the deadline
//! are detached, counted ([`PeerManager::detached_lanes`]) and logged; they
//! close their new session as soon as the dial returns.
//!
//! Application credentials are sent over the selected TLS/WSS or VPN channel.
//! Pairing and a code-authenticated lane can enroll an ordinary per-peer
//! credential. Reconnection, revocation and catalog writes use the existing
//! bounded lane/catalog lifecycle; no custom transcript or enrollment cipher
//! is involved. Unpair stops enrollment before removing/revoking the record.
//!
//! [`ConnHealth`]: crate::session::ConnHealth
use crate::{
    model::{
        errors::{ErrorClass, ErrorWire},
        orchestrator::{
            CatalogApi, CatalogCmd, CatalogSnapshot, CredentialSide, Credentials, Ctx, NetworkApi,
            PairResult, PeerContext, PeerRecord, PublishedEntry, RemoteCatalog, limits,
        },
        protocol::{CatalogListReply, CatalogListRequest, LaneRole, LaneSpec, deadlines, ops},
        status::{Direction, LaneState, StatusEvent},
    },
    orchestrator::{objects, scheduler},
    session::{
        self, ClientAuth, CodeAuth, Conn, ConnOptions, DialError, Priority, Reply,
        auth::{self as peer_auth, EnrollReply, PeerAuth},
    },
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
        Arc, Mutex, PoisonError, RwLock, RwLockReadGuard, Weak,
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
/// The furthest deadline [`deadline_in`] computes (a year): a caller's
/// `Duration::MAX` means "no practical limit", not an overflow.
const FAR_FUTURE: Duration = Duration::from_secs(365 * 24 * 3600);
/// How long [`NetworkApi::unpair`] spends at most on its best-effort
/// `peer.revoke` through a probe session (lane 0, when up, answers within
/// `REVOKE_DEADLINE`).
pub const REVOKE_BUDGET: Duration = Duration::from_secs(3);
/// Queued credential jobs before new ones are dropped (the next session
/// establishment queues an enrollment again).
const CREDENTIAL_QUEUE: usize = 64;
/// A lane is healthy once Ready this long (WP5.3: "reset the backoff once the
/// lane is healthy"): a later loss starts over from the base delay, and a
/// loss by the transport begins a network loss of the peer (module docs).
/// A session the peer closes sooner (a flapping acceptor) keeps escalating.
/// `limits::LANE_RESET_AFTER` (5 s since CCR-WP5.3-1; §3.7 said 30 s).
pub const LANE_HEALTHY_AFTER: Duration = limits::LANE_RESET_AFTER;
/// While a peer's network is down its lanes redial on one shared cadence
/// whose spacing is the outage's age divided by this (WP5.3; module docs,
/// "Network loss"), between the lane backoff's base and its cap: 1 s for
/// the first 30 s of an outage, 1.5 s at 45 s, 4 s at 2 min, 60 s from
/// 30 min on. `limits::NETWORK_SPACING_RATIO` (CCR-WP5.3-1).
pub const NETWORK_SPACING_RATIO: u32 = limits::NETWORK_SPACING_RATIO;
/// A sibling's dial is stale (and is interrupted when a lane connects)
/// once it has run this much longer than the dial that just succeeded,
/// without an answer from the peer.
const STALE_DIAL_MARGIN: Duration = Duration::from_millis(500);

/// `now + after`, capped at [`FAR_FUTURE`] instead of overflowing `Instant`.
fn deadline_in(after: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(after.min(FAR_FUTURE))
        .or_else(|| now.checked_add(after.min(Duration::from_secs(3600))))
        .unwrap_or(now)
}

/// Timing of the peer stack. [`Default`] is the §3.7 policy; tests shorten it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PeerOptions {
    /// Lane reconnect backoff: 1 s → 60 s.
    pub lane_backoff_base: Duration,
    pub lane_backoff_cap: Duration,
    /// ±20 %.
    pub lane_backoff_jitter: f64,
    /// A lane is healthy once `Ready` this long ([`LANE_HEALTHY_AFTER`],
    /// 5 s): its backoff resets, and its loss by the transport is a network
    /// loss of the peer.
    pub lane_reset_after: Duration,
    /// While the peer's network is down, its lanes share one redial cadence
    /// whose spacing is the outage's age over this ratio, between
    /// `lane_backoff_base` and `lane_backoff_cap` ([`NETWORK_SPACING_RATIO`],
    /// 30).
    pub network_spacing_ratio: u32,
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
            lane_reset_after: LANE_HEALTHY_AFTER,
            network_spacing_ratio: NETWORK_SPACING_RATIO,
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
    /// A lane's backoff. Its own healthy-period reset is unused (no
    /// `success` call): `run_lane` resets it once a session was `Ready` for
    /// `lane_reset_after`, and only then.
    fn lane_backoff(&self) -> Backoff {
        Backoff::new(
            self.lane_backoff_base,
            self.lane_backoff_cap,
            self.lane_backoff_jitter,
            Duration::MAX,
        )
    }

    /// The spacing of a down peer's redials `age` into its outage (before
    /// jitter): `age / network_spacing_ratio`, between the backoff's base
    /// and its cap.
    pub fn network_spacing(&self, age: Duration) -> Duration {
        let cap = self.lane_backoff_cap.max(self.lane_backoff_base);
        (age / self.network_spacing_ratio.max(1)).clamp(self.lane_backoff_base, cap)
    }

    /// `delay` with the lane backoff's jitter (±20 %).
    fn jittered(&self, delay: Duration) -> Duration {
        let jitter = if self.lane_backoff_jitter.is_finite() {
            self.lane_backoff_jitter.clamp(0.0, 1.0)
        } else {
            0.0
        };
        if jitter == 0.0 || delay.is_zero() {
            return delay;
        }
        let factor = rand::Rng::gen_range(&mut rand::thread_rng(), (1.0 - jitter)..=(1.0 + jitter));
        Duration::try_from_secs_f64(delay.as_secs_f64() * factor).unwrap_or(delay)
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
    peers: Tracked<Peers>,
    /// Serializes reconcilers (they may wait up to `options.join` for lanes).
    reconciling: Mutex<()>,
    /// Serializes `pair`'s catalog writes (its record and the removal of the
    /// installation's other endpoints), so two concurrent pairs of one
    /// installation leave exactly one record.
    pairing: Mutex<()>,
    /// The catalog generation last reconciled.
    reconciled: AtomicU64,
    /// Lane threads detached at a stop deadline, in total.
    detached: AtomicU64,
    watcher: Mutex<Option<JoinHandle<()>>>,
    /// Poked when the `peer-manager` thread ends.
    watcher_done: Arc<Notify>,
    /// `peer_meta` writes, off the lane threads.
    meta: Arc<MetaWriter>,
    /// The per-peer credentials (WP4.1) through the catalog contract;
    /// `None` when the catalog keeps none (a test's fake), which keeps the
    /// code-only behaviour.
    catalog: Option<Arc<CredentialCatalog>>,
    /// Enrollments and forgotten credentials, off the lane threads.
    credentials: Arc<CredentialWriter>,
}

#[derive(Default)]
struct Peers {
    /// The supervised peers, by installation.
    active: BTreeMap<String, PeerHandle>,
    /// Supervisors a reconciler took out of `active` and is stopping; they
    /// leave this list once retired. `shutdown` stops and waits for them too.
    stopping: Vec<Arc<Supervisor>>,
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
        let catalog = CredentialCatalog::of(&ctx.catalog);
        let credentials = CredentialWriter::start(catalog.clone());
        let manager = Arc::new(PeerManager {
            ctx,
            options,
            cancel,
            closed: AtomicBool::new(false),
            peers: Tracked::new("peer-manager.peers", Peers::default()),
            reconciling: Mutex::new(()),
            pairing: Mutex::new(()),
            reconciled: AtomicU64::new(0),
            detached: AtomicU64::new(0),
            watcher: Mutex::new(None),
            watcher_done: Arc::new(Notify::new()),
            meta: MetaWriter::start(ctx_catalog),
            catalog,
            credentials,
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
            .active
            .get(installation)
            .map(|handle| handle.supervisor.context.clone())
    }

    /// Every supervised peer, by installation.
    pub fn peers(&self) -> Vec<PeerView> {
        let supervisors: Vec<Arc<Supervisor>> = self
            .peers
            .lock()
            .active
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
            .active
            .get(installation)
            .map(|handle| handle.supervisor.clone())?;
        Some(supervisor.view())
    }

    /// The catalog generation last reconciled.
    pub fn reconciled_generation(&self) -> u64 {
        self.reconciled.load(Ordering::Acquire)
    }

    /// Lane threads that had not ended by their stop deadline and were
    /// detached (a lane inside a TCP connect, which a cancelled dial still
    /// waits for, CCR-WP2.6-2), since the start. A lane that ended in time was
    /// joined and is not counted.
    pub fn detached_lanes(&self) -> u64 {
        self.detached.load(Ordering::Acquire)
    }

    fn stopping_error() -> ErrorWire {
        ErrorWire::coded("ORCHESTRATOR_STOPPING", "the peer stack is shut down")
    }

    /// Stops `supervisors` (already cancelled) under one `deadline`, in
    /// phases across all of them, so one slow supervisor never eats another
    /// one's time: wait for every lane until `force_close_margin` before the
    /// deadline, close every session still open, wait until the deadline,
    /// then retire each supervisor (lanes still running are reported
    /// `Closed`, detached and muted).
    fn stop_all(&self, supervisors: &[Arc<Supervisor>], deadline: Instant) {
        let force_at = deadline
            .checked_sub(self.options.force_close_margin)
            .unwrap_or(deadline);
        for supervisor in supervisors {
            supervisor.wait_lanes(force_at);
        }
        for supervisor in supervisors {
            supervisor.force_close();
        }
        for supervisor in supervisors {
            supervisor.wait_lanes(deadline);
        }
        for supervisor in supervisors {
            let detached = supervisor.retire();
            self.detached.fetch_add(detached, Ordering::AcqRel);
        }
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
            let current: Vec<String> = peers.active.keys().cloned().collect();
            for installation in current {
                let reason = match desired.get(&installation) {
                    None => StopReason::Removed,
                    Some(record) if *record != peers.active[&installation].record => {
                        StopReason::Reconfigured
                    }
                    Some(_) => continue,
                };
                if let Some(handle) = peers.active.remove(&installation) {
                    // Visible to `shutdown` until retired.
                    peers.stopping.push(handle.supervisor.clone());
                    stopping.push((handle, reason));
                }
            }
            let starting: Vec<PeerRecord> = desired
                .values()
                .filter(|record| !peers.active.contains_key(&record.installation))
                .cloned()
                .collect();
            (stopping, starting)
        };
        // Stop every outgoing supervisor first (their sessions wind down in
        // parallel), then wait for them under one deadline. They are retired
        // (muted) before `PeerRemoved` is recorded or a successor is spawned.
        if !stopping.is_empty() {
            let deadline = deadline_in(self.options.join);
            for (handle, reason) in &stopping {
                handle.supervisor.begin_stop(*reason, deadline);
            }
            let supervisors: Vec<Arc<Supervisor>> = stopping
                .iter()
                .map(|(handle, _)| handle.supervisor.clone())
                .collect();
            self.stop_all(&supervisors, deadline);
            self.peers.lock().stopping.retain(|supervisor| {
                !supervisors
                    .iter()
                    .any(|stopped| Arc::ptr_eq(stopped, supervisor))
            });
        }
        let mut previous: BTreeMap<String, Arc<RemoteCatalog>> = BTreeMap::new();
        for (handle, reason) in stopping {
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
            Credentialing {
                catalog: self.catalog.clone(),
                writer: self.credentials.clone(),
            },
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
            peers.active.insert(
                installation.clone(),
                PeerHandle {
                    record,
                    supervisor: supervisor.clone(),
                },
            );
        }
        // CCR-WP1.11-1: the catalog's display name, if any.
        let display_name = self
            .ctx
            .catalog
            .snapshot()
            .peer_meta
            .get(&installation)
            .and_then(|meta| meta.display_name.clone());
        self.ctx.status.record(StatusEvent::PeerConfigured {
            installation,
            endpoint,
            display_name,
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
        self.probe_with(
            endpoint,
            &CodeAuth {
                code: code.to_owned(),
            },
            expected,
            pool,
            &self.cancel,
        )
        .map_err(|error| error.to_wire())
    }

    fn probe_with(
        &self,
        endpoint: &str,
        auth: &dyn ClientAuth,
        expected: Option<&str>,
        pool: u32,
        cancel: &Cancel,
    ) -> Result<(Conn, session::ServerHello), DialError> {
        let hello = session::client_hello(&self.ctx.installation, LaneRole::Probe, None, pool);
        // A shutdown interrupts a probe in progress (CCR-WP2.6-2).
        session::dial_cancellable(
            endpoint,
            hello,
            auth,
            expected,
            None,
            self.options.conn,
            cancel,
        )
    }

    /// A probe session to a paired peer with our outbound credential (WP4.1)
    /// when we hold one, else the code; a `PEER_REVOKED` refusal of the
    /// credential is retried once with the code.
    fn probe_paired(
        &self,
        record: &PeerRecord,
        cancel: &Cancel,
    ) -> Result<(Conn, session::ServerHello), DialError> {
        let expected = (!record.installation.is_empty()).then_some(record.installation.as_str());
        let pool = record.connections.max(limits::MIN_POOL);
        let code = CodeAuth {
            code: record.code.clone(),
        };
        let Some(key) = self.outbound_key(&record.installation) else {
            return self.probe_with(&record.endpoint, &code, expected, pool, cancel);
        };
        let auth = PeerAuth { key };
        match self.probe_with(&record.endpoint, &auth, expected, pool, cancel) {
            Err(error) if error.to_wire().code == "PEER_REVOKED" => {
                self.probe_with(&record.endpoint, &code, expected, pool, cancel)
            }
            other => other,
        }
    }

    /// Our active outbound credential for `installation`.
    fn outbound_key(&self, installation: &str) -> Option<[u8; 32]> {
        self.catalog
            .as_ref()?
            .credentials()
            .active(installation, CredentialSide::Outbound)
    }

    /// The best-effort `peer.revoke` of an unpair (WP4.1): on the peer's
    /// lane 0 when it is up, else on a probe session, within about
    /// [`REVOKE_BUDGET`]. A lane-0 call that fails (refused `BUSY` by an
    /// acceptor whose exchange slots its lane-0 exchanges hold, or lost) is
    /// sent again on a probe session within what is left of the budget
    /// (WP4.INT re-review: it was only logged, and the acceptor kept our
    /// credential). A failure is logged; the unpair goes on.
    fn revoke_remote(&self, record: &PeerRecord) {
        let started = std::time::Instant::now();
        let live = self
            .peers
            .lock()
            .active
            .get(&record.installation)
            .and_then(|handle| handle.supervisor.conns.lock().get(&0).cloned())
            .filter(|conn| conn.closed().is_none());
        let outcome = match live.map(|conn| call_revoke(&conn)) {
            Some(Ok(())) => Ok(()),
            Some(Err(error)) => {
                let left = REVOKE_BUDGET.saturating_sub(started.elapsed());
                event!(debug, (error.code.as_str()), {
                    "peer": record.installation,
                    "detail": "peer.revoke on lane 0 failed; sending it on a probe session",
                    "message": error.message,
                });
                if left.is_zero() {
                    Err(error)
                } else {
                    self.revoke_on_probe(record, left)
                }
            }
            None => self.revoke_on_probe(record, REVOKE_BUDGET),
        };
        if let Err(error) = outcome {
            event!(info, (error.code.as_str()), {
                "peer": record.installation,
                "endpoint": record.endpoint,
                "detail": "peer.revoke was not delivered; the peer keeps our credential until it rotates its pairing code",
                "message": error.message,
            });
        }
    }
}

impl PeerManager {
    /// `peer.revoke` on a probe session of its own, bounded by `budget`: a
    /// timer cancels the probe at the budget (a TCP connect in progress runs
    /// to its end).
    fn revoke_on_probe(&self, record: &PeerRecord, budget: Duration) -> Result<(), ErrorWire> {
        let cancel = self.cancel.child();
        let done = Cancel::new();
        let timer = {
            let (cancel, done) = (cancel.clone(), done.clone());
            thread::Builder::new()
                .name("peer-revoke".into())
                .spawn(move || {
                    if !done.sleep(budget) {
                        cancel.cancel();
                    }
                })
        };
        let outcome = match self.probe_paired(record, &cancel) {
            Ok((conn, _)) => {
                let outcome = call_revoke(&conn);
                conn.close();
                outcome
            }
            Err(error) => Err(error.to_wire()),
        };
        done.cancel();
        if let Ok(timer) = timer {
            let _ = timer.join();
        }
        outcome
    }
}

/// The pauses before each retry of a lane-0 enrollment the acceptor refused
/// with a code it may lift (`run_credential_job`).
const ENROLL_RETRIES: [Duration; 4] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
];

/// One `peer.revoke` on `conn`.
fn call_revoke(conn: &Conn) -> Result<(), ErrorWire> {
    conn.call(
        peer_auth::ops::PEER_REVOKE,
        Value::Object(serde_json::Map::new()),
        Vec::new(),
        Priority::Control,
        peer_auth::REVOKE_DEADLINE,
    )
    .map(drop)
    .map_err(|error| error.to_wire())
}

/// Sends a fresh credential enrollment over the selected TLS/WSS or VPN channel.
fn enroll_on(conn: &Conn) -> Result<[u8; 32], ErrorWire> {
    let secret = peer_auth::new_secret();
    let request = peer_auth::enrollment_request(&secret);
    let header = serde_json::to_value(&request)
        .map_err(|error| ErrorWire::coded("INVALID_REQUEST", error))?;
    let reply = conn
        .call(
            peer_auth::ops::PEER_ENROLL,
            header,
            Vec::new(),
            Priority::Control,
            peer_auth::ENROLL_DEADLINE,
        )
        .map_err(|error| error.to_wire())?;
    match serde_json::from_value::<EnrollReply>(reply.header) {
        Ok(EnrollReply { enrolled: true }) => Ok(peer_auth::peer_key(&secret)),
        _ => Err(ErrorWire::coded(
            "INVALID_REPLY",
            "peer.enroll did not confirm the enrollment",
        )),
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
        let installation = server.installation.clone();
        if installation == self.ctx.installation {
            conn.close();
            return Err(ErrorWire::coded(
                "CANNOT_PAIR_WITH_SELF",
                "the endpoint is this installation",
            ));
        }
        // WP4.1: a per-peer credential, enrolled on the code-authenticated
        // probe session. An acceptor that does not know `peer.enroll` keeps
        // the pairing code-only (its lane 0 tries again later).
        let enrolled = match &self.catalog {
            Some(_) => match enroll_on(&conn) {
                Ok(key) => Some(key),
                Err(error) => {
                    event!(info, (error.code.as_str()), {
                        "peer": installation,
                        "endpoint": endpoint,
                        "detail": "no per-peer credential was enrolled while pairing; the pairing code is used",
                        "message": error.message,
                    });
                    None
                }
            },
            None => None,
        };
        conn.close();
        {
            // The record and the removal of the installation's other
            // endpoints are one step for concurrent pairs: without this, two
            // pairs of one installation at two endpoints could each remove
            // the other's fresh record and leave none. The last pair wins.
            let _pairing = lock(&self.pairing);
            // The credential is stored before the record: a supervisor the
            // record starts (the `peer-manager` thread may reconcile before
            // this call does) presents it at once and never enrolls a second
            // one on its lane 0.
            let stored = match (enrolled, &self.catalog) {
                (Some(key), Some(catalog)) => {
                    match catalog.enroll(&installation, CredentialSide::Outbound, key) {
                        Ok(()) => true,
                        Err(error) => {
                            // The lanes use the code; lane 0 enrolls again.
                            event!(warn, (error.code.as_str()), {
                                "peer": installation,
                                "detail": "the per-peer credential could not be stored",
                                "message": error.message,
                            });
                            false
                        }
                    }
                }
                _ => false,
            };
            let upserted = self.ctx.catalog.apply(CatalogCmd::UpsertPeer {
                record: PeerRecord {
                    endpoint: endpoint.to_owned(),
                    code: code.to_owned(),
                    installation: installation.clone(),
                    connections: count,
                    revision: uuid::Uuid::new_v4().to_string(),
                },
            });
            let snapshot = match upserted {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    // No record: no credential for it either (best effort).
                    if let (true, Some(catalog)) = (stored, &self.catalog)
                        && !self
                            .ctx
                            .catalog
                            .snapshot()
                            .peers
                            .iter()
                            .any(|peer| peer.installation == installation)
                    {
                        let _ = catalog.forget(&installation, CredentialSide::Outbound);
                    }
                    return Err(error);
                }
            };
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
        let (conn, _) = self
            .probe_paired(&record, &self.cancel)
            .map_err(|error| error.to_wire())?;
        let listed = list_catalog(&conn, None);
        conn.close();
        listed
            .map(|reply| reply.entries)
            .map_err(|error| error.to_wire())
    }

    /// Stops the peer's enrollments and tells the peer (best effort, WP4.1
    /// `peer.revoke`), removes the peer (with our outbound credential), then
    /// stops and joins its lanes (≤ 5 s). Unpairing an endpoint that is not
    /// paired succeeds (the legacy `Unpair` did).
    fn unpair(&self, endpoint: &str) -> Result<(), ErrorWire> {
        let snapshot = self.ctx.catalog.snapshot();
        // The installation's last record: the peer is unpaired, not moved.
        let last = snapshot.peer_by_endpoint(endpoint).filter(|record| {
            !record.installation.is_empty()
                && !snapshot.peers.iter().any(|other| {
                    other.installation == record.installation && other.endpoint != endpoint
                })
        });
        // No enrollment of this peer starts or is stored from now on: one
        // racing the revoke would leave an active credential at the peer.
        let unpairing = last.and_then(|record| {
            let supervisor = self
                .peers
                .lock()
                .active
                .get(&record.installation)
                .map(|handle| handle.supervisor.clone())?;
            supervisor.unpairing.store(true, Ordering::Release);
            Some(supervisor)
        });
        if let Some(record) = last
            && !self.closed.load(Ordering::Acquire)
            && self.outbound_key(&record.installation).is_some()
        {
            // Only a credential we enrolled has anything to revoke there.
            self.revoke_remote(record);
        }
        let removed = self.ctx.catalog.apply(CatalogCmd::RemovePeer {
            endpoint: endpoint.to_owned(),
        });
        if removed.is_err()
            && let Some(supervisor) = &unpairing
        {
            // Still paired: its lane 0 may enroll again.
            supervisor.unpairing.store(false, Ordering::Release);
        }
        removed?;
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
            .active
            .values()
            .map(|handle| handle.supervisor.clone())
            .collect();
        for supervisor in supervisors {
            supervisor.recheck_now();
        }
    }

    /// GOAWAY to every session, cancel every lane, join within `deadline`.
    /// Supervisors a concurrent `pair`/`unpair`/catalog reconcile is still
    /// stopping are waited for as well (under this deadline).
    fn shutdown(&self, deadline: Duration) {
        let until = deadline_in(deadline);
        self.closed.store(true, Ordering::Release);
        let (handles, stopping): (Vec<PeerHandle>, Vec<Arc<Supervisor>>) = {
            let mut peers = self.peers.lock();
            let active = std::mem::take(&mut peers.active);
            (active.into_values().collect(), peers.stopping.clone())
        };
        for handle in &handles {
            handle.supervisor.begin_stop(StopReason::Shutdown, until);
        }
        self.cancel.cancel();
        let mut supervisors: Vec<Arc<Supervisor>> = handles
            .iter()
            .map(|handle| handle.supervisor.clone())
            .collect();
        supervisors.extend(stopping);
        self.stop_all(&supervisors, until);
        self.meta.close();
        self.credentials.close();
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
            .field(
                "peers",
                &self.peers.lock().active.keys().cloned().collect::<Vec<_>>(),
            )
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
    /// The lane re-dialing after the block expired (the probe). A lane
    /// thread that ends while it holds the probe releases it (`LaneExit`),
    /// so a lane that dies mid-probe never leaves the others parked.
    probe: Option<u32>,
    generation: u64,
}

/// A down peer's shared redial cadence (module docs, "Network loss").
#[derive(Debug)]
struct NetLoss {
    /// When the loss began (`None`: the peer is up, or was never reached).
    since: Option<Instant>,
    /// The next redial slot of any of the peer's lanes.
    next: Instant,
    /// Redial slots handed out since the loss began.
    dials: u64,
    /// The code of the session loss that began it (the reconnect is logged
    /// under it).
    cause: String,
}

/// What a lane waits on during one attempt (a dial, or the wait before the
/// next one): its token, a child of the supervisor's, is cancelled when a
/// sibling lane connects and the attempt is one a connect cuts short
/// (module docs, "Network loss").
struct Waker {
    id: u64,
    token: Cancel,
    attempt: Attempt,
}

/// A lane's current attempt, as [`Supervisor::lane_connected`] judges it.
#[derive(Clone, Copy, Debug)]
enum Attempt {
    /// A dial in progress: when it started, and whether the peer answered
    /// (its `HELLO_ACK` arrived: the dial is not stale).
    Dial { since: Instant, answered: bool },
    /// The wait before the next dial. `wakeable`: a sibling that connects
    /// ends it, because the lane reached no session (its dial failed) or a
    /// down peer paces it. A wait after a session that ended otherwise (a
    /// session the peer closed before it was healthy: a flapping acceptor)
    /// keeps its escalating delay: lanes that woke each other on every
    /// connect would redial a flapping peer at one session per dial time
    /// for ever (the WP5.3 review's blocker).
    Wait { wakeable: bool },
}

/// One attempt's registration in [`Supervisor::wakers`]; removed on drop.
struct Armed<'a> {
    supervisor: &'a Supervisor,
    lane: u32,
    id: u64,
    token: Cancel,
}

impl Drop for Armed<'_> {
    fn drop(&mut self) {
        let mut wakers = self.supervisor.wakers.lock();
        if wakers
            .get(&self.lane)
            .is_some_and(|waker| waker.id == self.id)
        {
            wakers.remove(&self.lane);
        }
    }
}

/// Whether a dial failure is the network's (the transport could not reach
/// the peer, or a handshake step got no answer): such failures of a down
/// peer redial on its shared cadence (module docs, "Network loss").
fn dial_lost(error: &DialError) -> bool {
    matches!(error, DialError::Connect(_) | DialError::Timeout)
}

/// How an established session ended ([`Supervisor::serve`]).
struct Ended {
    reason: ErrorWire,
    /// This side closed it (its lane runner gave up on it, or could not
    /// start): never a loss by the transport, although its code is
    /// `WS_CLOSED` ("closed locally") like a peer's close without a
    /// `GOAWAY` (the WP5.3 review's minor).
    local: bool,
}

/// Whether a session ended by the transport (reset, closed without a
/// `GOAWAY`, or silent past its liveness deadline) rather than by a
/// `GOAWAY` of either side, whose code says why, or by this side.
fn session_lost(ended: &Ended) -> bool {
    !ended.local && transport_lost(&ended.reason)
}

/// Whether `reason` is one of the transport's (see [`session_lost`]).
fn transport_lost(reason: &ErrorWire) -> bool {
    matches!(
        reason.code.as_str(),
        "CONNECTION_LOST" | "TRANSPORT_DEAD" | "WS_CLOSED"
    )
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
    /// The earliest stop deadline this supervisor was given.
    stop_deadline: Tracked<Option<Instant>>,
    gate: Tracked<Gate>,
    gate_changed: Notify,
    lanes: Tracked<Vec<LaneView>>,
    conns: Tracked<BTreeMap<u32, Conn>>,
    live: AtomicU32,
    exited: Notify,
    /// Each lane thread, by lane.
    threads: Mutex<Vec<(u32, JoinHandle<()>)>>,
    /// Whether this supervisor has emitted a `PeerCatalog` yet.
    catalog_emitted: AtomicBool,
    /// Per-peer credentials (WP4.1).
    credentialing: Credentialing,
    /// An outbound key the peer refused (`PEER_REVOKED`): never presented
    /// again by this supervisor. The catalog keeps it until a lane-0
    /// enrollment (a code-authenticated session) replaces it: a refusal is
    /// not authenticated.
    refused_key: Mutex<Option<[u8; 32]>>,
    /// An enrollment on lane 0 is queued or running.
    enrolling: Arc<AtomicBool>,
    /// Set by an unpair of this peer before its `peer.revoke`: no
    /// enrollment starts, and none running is stored.
    unpairing: Arc<AtomicBool>,
    /// The peer's network loss and its shared redial cadence (WP5.3).
    net: Tracked<NetLoss>,
    /// Each lane's current attempt, so a lane that connects wakes the
    /// others (WP5.3).
    wakers: Tracked<BTreeMap<u32, Waker>>,
    next_waker: AtomicU64,
    /// Sessions established, by any lane (a dial that fails while a sibling
    /// connects retries at once).
    connects: AtomicU64,
    /// `true` once retired: nothing of this supervisor reaches
    /// `ctx.status` any more. Every status event is recorded under the read
    /// side and `retire` flips it under the write side, so no event recorded
    /// after the retirement can slip in (see [`Supervisor::retire`]).
    retired: RwLock<bool>,
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
        credentialing: Credentialing,
    ) -> Arc<Supervisor> {
        let pool = record.connections.clamp(limits::MIN_POOL, 16);
        let context = Arc::new(PeerContext {
            installation: record.installation.clone(),
            endpoint: record.endpoint.clone(),
            local_installation: ctx.installation.clone(),
            pool,
            catalog: Snapshot::from_arc(catalog.unwrap_or_default()),
        });
        if let Err(error) = super::lease_clock::register_peer(&ctx, &context) {
            event!(warn, CONTENT_RESOURCE_LIMIT, {"site":"peer.pool_generation", "message":error.to_string()});
        }
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
            stop_deadline: Tracked::new("peer.stop_deadline", None),
            gate: Tracked::new("peer.gate", Gate::default()),
            gate_changed: Notify::new(),
            lanes: Tracked::new("peer.lanes", lanes),
            conns: Tracked::new("peer.conns", BTreeMap::new()),
            live: AtomicU32::new(0),
            exited: Notify::new(),
            threads: Mutex::new(Vec::new()),
            catalog_emitted: AtomicBool::new(false),
            retired: RwLock::new(false),
            meta,
            credentialing,
            refused_key: Mutex::new(None),
            enrolling: Arc::new(AtomicBool::new(false)),
            unpairing: Arc::new(AtomicBool::new(false)),
            net: Tracked::new(
                "peer.net",
                NetLoss {
                    since: None,
                    next: Instant::now(),
                    dials: 0,
                    cause: String::new(),
                },
            ),
            wakers: Tracked::new("peer.wakers", BTreeMap::new()),
            next_waker: AtomicU64::new(0),
            connects: AtomicU64::new(0),
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
                Ok(handle) => lock(&self.threads).push((lane, handle)),
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

    /// Cancels every lane; the per-session hooks send `GOAWAY{reason}`. The
    /// lanes close their sessions themselves `force_close_margin` before
    /// `deadline` if the peer has not closed them by then.
    fn begin_stop(&self, reason: StopReason, deadline: Instant) {
        *self.stop_reason.lock() = reason;
        {
            let mut stop = self.stop_deadline.lock();
            if stop.is_none_or(|current| deadline < current) {
                *stop = Some(deadline);
            }
        }
        self.cancel.cancel();
    }

    /// When a stopping lane stops waiting for the peer to close its session:
    /// `force_close_margin` before the stop deadline (`None` when the token
    /// was cancelled without a stop, e.g. by `ctx.shutdown`).
    fn force_close_at(&self) -> Option<Instant> {
        let deadline = (*self.stop_deadline.lock())?;
        Some(
            deadline
                .checked_sub(self.options.force_close_margin)
                .unwrap_or(deadline),
        )
    }

    /// Closes every session still open (a lane in a call the peer never
    /// answers, a peer that does not close after GOAWAY).
    fn force_close(&self) {
        if self.live.load(Ordering::Acquire) == 0 {
            return;
        }
        let conns: Vec<Conn> = self.conns.lock().values().cloned().collect();
        for conn in conns {
            conn.close();
        }
    }

    /// Ends this supervisor's reporting, once; returns how many lane
    /// threads were detached. Every lane not yet `Closed` (it is still
    /// running: inside a dial, CCR-WP2.6-2, or finishing at the deadline) is
    /// reported `Closed` now, then the supervisor is muted: those lanes'
    /// later transitions (`Handshaking` from a dial, their own `Closed`,
    /// a catalog listing) never reach `ctx.status`. The manager calls this
    /// before it records `PeerRemoved` or spawns a successor, so a late lane
    /// can neither recreate an unpaired peer in the hub nor overwrite the
    /// lanes of its successor for the same installation. Lane threads that
    /// ended are joined; the others are detached and logged.
    fn retire(&self) -> u64 {
        let running: Vec<u32> = {
            let mut retired = self.retired.write().unwrap_or_else(PoisonError::into_inner);
            if *retired {
                return 0;
            }
            let now_ms = self.ctx.clock.now_ms();
            let running: Vec<u32> = {
                let mut lanes = self.lanes.lock();
                lanes
                    .iter_mut()
                    .filter(|view| view.state != LaneState::Closed)
                    .map(|view| {
                        view.state = LaneState::Closed;
                        view.since_ms = now_ms;
                        view.error = None;
                        view.backoff_until_ms = None;
                        view.rtt_ms = None;
                        view.protocol = None;
                        view.lane
                    })
                    .collect()
            };
            for lane in &running {
                self.ctx.status.record(self.lane_event(
                    *lane,
                    LaneState::Closed,
                    None,
                    None,
                    None,
                    None,
                ));
            }
            *retired = true;
            running
        };
        // Outside the write side: a lane that recorded its own `Closed` is
        // past its last status event and only returns, so joining it is
        // short; joining under the write side could wait for a lane that
        // waits for the read side.
        let handles: Vec<(u32, JoinHandle<()>)> = std::mem::take(&mut *lock(&self.threads));
        let mut detached = 0u64;
        for (lane, handle) in handles {
            if !running.contains(&lane) || handle.is_finished() {
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
        detached
    }

    /// The read side of `retired`: hold it across a status record.
    fn reporting(&self) -> RwLockReadGuard<'_, bool> {
        self.retired.read().unwrap_or_else(PoisonError::into_inner)
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

    fn lane_event(
        &self,
        lane: u32,
        state: LaneState,
        error: Option<ErrorWire>,
        backoff_until_ms: Option<u64>,
        rtt_ms: Option<u64>,
        protocol: Option<String>,
    ) -> StatusEvent {
        StatusEvent::LaneState {
            installation: self.record.installation.clone(),
            lane,
            role: role_of(lane),
            direction: Direction::Outbound,
            state,
            error,
            backoff_until_ms,
            rtt_ms,
            protocol,
        }
    }

    /// Records a lane transition and emits `LaneState` (unless retired).
    fn set_lane(
        &self,
        lane: u32,
        state: LaneState,
        error: Option<ErrorWire>,
        backoff_until_ms: Option<u64>,
        rtt_ms: Option<u64>,
    ) {
        let now_ms = self.ctx.clock.now_ms();
        // Held across the view update and the record: `retire` sees either
        // both or neither.
        let retired = self.reporting();
        // CCR-WP1.11-2: a `Ready` lane reports the protocol its session
        // negotiated (set on the view before `serve`).
        let mut protocol = None;
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
                protocol = view.protocol.clone();
            }
        }
        if !*retired {
            self.ctx.status.record(self.lane_event(
                lane,
                state,
                error,
                backoff_until_ms,
                rtt_ms,
                protocol,
            ));
        }
    }

    fn update_lane(&self, lane: u32, update: impl FnOnce(&mut LaneView)) {
        if let Some(view) = self.lanes.lock().get_mut(lane as usize) {
            update(view);
        }
    }

    // -- the peer-level gate ------------------------------------------------

    /// Waits until `lane` may dial: `true` with no block, or when this lane
    /// takes the probe of an expired block; `false` when cancelled. While it
    /// waits, the lane shows the block's state once per block.
    fn admit(&self, lane: u32, announced: &mut Option<u64>) -> bool {
        loop {
            if self.cancel.is_cancelled() {
                return false;
            }
            let seen = self.gate_changed.count();
            let (block, probing) = {
                let gate = self.gate.lock();
                (gate.block.clone(), gate.probe.is_some())
            };
            let Some(block) = block else {
                return true;
            };
            let now = Instant::now();
            if now >= block.until && !probing {
                let mut gate = self.gate.lock();
                if gate.probe.is_none()
                    && gate
                        .block
                        .as_ref()
                        .is_some_and(|current| current.generation == block.generation)
                {
                    gate.probe = Some(lane);
                    return true;
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
            let changed = gate.block.is_some() || gate.probe.is_some();
            gate.block = None;
            gate.probe = None;
            if changed {
                gate.generation += 1;
            }
            changed
        };
        if changed {
            self.gate_changed.poke();
        }
    }

    /// `lane` met a peer-level refusal: block every lane until the recheck
    /// (and release the probe if `lane` held it).
    fn gate_block(&self, lane: u32, state: LaneState, error: ErrorWire) {
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
                until: deadline_in(recheck),
                until_ms,
                generation: gate.generation,
            });
            if gate.probe == Some(lane) {
                gate.probe = None;
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
            let probing = gate.probe.is_some();
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

    /// A dial of `lane` failed transiently. When `lane` was the probe, the
    /// peer's class is unknown again: the block is dropped and each lane
    /// falls back to its own backoff.
    fn gate_transient(&self, lane: u32) {
        {
            let mut gate = self.gate.lock();
            if gate.probe != Some(lane) {
                return;
            }
            gate.block = None;
            gate.probe = None;
            gate.generation += 1;
        }
        self.gate_changed.poke();
    }

    /// `lane` ends: if it held the probe (it was cancelled or panicked
    /// mid-probe), another lane may take it.
    fn release_probe(&self, lane: u32) {
        {
            let mut gate = self.gate.lock();
            if gate.probe != Some(lane) {
                return;
            }
            gate.probe = None;
        }
        self.gate_changed.poke();
    }

    /// `peer_meta`: seen now; `incompatible` replaces the stored code.
    /// Queued for the `peer-meta` thread: the lane never waits for the
    /// catalog. A stopping supervisor records nothing (its peer may be
    /// unpaired by now).
    fn record_seen(&self, incompatible: Option<String>) {
        if self.cancel.is_cancelled() {
            return;
        }
        self.meta.record(PeerSeen {
            installation: self.record.installation.clone(),
            at_ms: self.ctx.clock.now_ms(),
            incompatible,
        });
    }

    // -- dialing ------------------------------------------------------------

    /// Our outbound credential for this peer, unless the peer refused it.
    fn outbound_key(&self) -> Option<[u8; 32]> {
        let key = self
            .credentialing
            .catalog
            .as_ref()?
            .credentials()
            .active(self.installation(), CredentialSide::Outbound)?;
        (*lock(&self.refused_key) != Some(key)).then_some(key)
    }

    /// The peer refused `key` (`PEER_REVOKED`): this supervisor stops
    /// presenting it. The catalog keeps it: the refusal is not
    /// authenticated (anyone answering at the peer's address can send it),
    /// so only a code-authenticated session replaces the credential (lane
    /// 0's enrollment, [`Supervisor::enroll_later`]).
    fn credential_refused(&self, key: [u8; 32]) {
        *lock(&self.refused_key) = Some(key);
        event!(info, PEER_REVOKED, {
            "peer": self.installation(),
            "detail": "the peer refused our per-peer credential; dialing with the pairing code",
        });
    }

    /// Lane 0 authenticated with the pairing code (no credential of ours,
    /// one the peer refused, or an acceptor without the scheme): enroll one
    /// on this session, off the lane thread, once at a time. Never once an
    /// unpair of this peer began.
    fn enroll_later(&self, conn: &Conn, server: &session::ServerHello) {
        if self.credentialing.catalog.is_none()
            || self.record.code.is_empty()
            || self.unpairing.load(Ordering::Acquire)
            || self.enrolling.swap(true, Ordering::AcqRel)
        {
            return;
        }
        let posted = self.credentialing.writer.post(CredentialJob::Enroll {
            conn: conn.clone(),
            server: Box::new(server.clone()),
            replaces: *lock(&self.refused_key),
            cancel: self.cancel.clone(),
            unpairing: self.unpairing.clone(),
            done: self.enrolling.clone(),
        });
        if !posted {
            self.enrolling.store(false, Ordering::Release);
        }
    }

    /// Dials `lane`, presenting `credential` (our outbound key, WP4.1) when
    /// given, else the pairing code.
    fn dial(
        &self,
        lane: u32,
        credential: Option<[u8; 32]>,
        attempt: &Armed<'_>,
    ) -> Result<(Conn, session::ServerHello), DialError> {
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
        let credential = credential.map(|key| PeerAuth { key });
        let handshaking = || {
            self.answered(lane, attempt.id);
            self.set_lane(lane, LaneState::Handshaking, None, None, None);
        };
        let auth = AnnounceHandshake {
            inner: match &credential {
                Some(peer) => peer,
                None => &code,
            },
            on_key: &handshaking,
        };
        // CCR-WP2.6-2 (applied by WP3.1): stopping the supervisor (unpair,
        // respawn, shutdown) interrupts a dial in progress, and so does a
        // sibling that connected while this dial got no answer (WP5.3): the
        // attempt's token is a child of the supervisor's.
        session::dial_cancellable(
            &self.record.endpoint,
            hello,
            &auth,
            Some(&self.record.installation),
            None,
            self.options.conn,
            &attempt.token,
        )
    }

    // -- reconnecting (WP5.3; module docs, "Network loss") -------------------

    /// Registers `lane`'s attempt: its token is a child of the
    /// supervisor's, and a sibling that connects cancels it when the attempt
    /// is a stale dial or a wakeable wait ([`Supervisor::lane_connected`]).
    fn arm(&self, lane: u32, attempt: Attempt) -> Armed<'_> {
        let id = self.next_waker.fetch_add(1, Ordering::Relaxed);
        let token = self.cancel.child();
        self.wakers.lock().insert(
            lane,
            Waker {
                id,
                token: token.clone(),
                attempt,
            },
        );
        Armed {
            supervisor: self,
            lane,
            id,
            token,
        }
    }

    /// The peer answered `lane`'s dial `id` (its `HELLO_ACK`): not stale.
    fn answered(&self, lane: u32, id: u64) {
        if let Some(waker) = self
            .wakers
            .lock()
            .get_mut(&lane)
            .filter(|waker| waker.id == id)
            && let Attempt::Dial { answered, .. } = &mut waker.attempt
        {
            *answered = true;
        }
    }

    /// `lane` connected with a dial that started at `started` and took
    /// `took`: the peer is reachable now. Its network loss ends; every
    /// sibling whose wait is wakeable (it reached no session, or the down
    /// peer paced it: [`Attempt::Wait`]) dials at once, and every sibling
    /// dial that is stale (started before this one, no answer from the peer,
    /// and running [`STALE_DIAL_MARGIN`] longer than this one took: a dial
    /// into a link that was down) is interrupted and redialed at once. A
    /// sibling backing off after a session the peer ended keeps its delay.
    fn lane_connected(&self, lane: u32, started: Instant, took: Duration) {
        self.connects.fetch_add(1, Ordering::AcqRel);
        let ended = {
            let mut net = self.net.lock();
            net.since
                .take()
                .map(|since| (since.elapsed(), net.dials, std::mem::take(&mut net.cause)))
        };
        if let Some((down, dials, cause)) = ended {
            event!(info, (cause.as_str()), {
                "peer": self.installation(),
                "lane": lane,
                "detail": "the peer is reachable again: its network loss ended",
                "down_ms": u64::try_from(down.as_millis()).unwrap_or(u64::MAX),
                "redials": dials,
            });
        }
        let now = Instant::now();
        let woken: Vec<Cancel> = self
            .wakers
            .lock()
            .iter()
            .filter(|(other, _)| **other != lane)
            .filter(|(_, waker)| match waker.attempt {
                Attempt::Wait { wakeable } => wakeable,
                Attempt::Dial { since, answered } => {
                    !answered
                        && since < started
                        && now.saturating_duration_since(since) >= took + STALE_DIAL_MARGIN
                }
            })
            .map(|(_, waker)| waker.token.clone())
            .collect();
        for token in woken {
            token.cancel();
        }
    }

    /// `lane` lost a healthy session by the transport: the peer's network
    /// loss begins, unless one is under way. Its lanes then redial on one
    /// cadence ([`Supervisor::network_slot`]), the first one a base delay
    /// (jittered) from now.
    fn network_lost(&self, lane: u32, reason: &ErrorWire) {
        let began = {
            let mut net = self.net.lock();
            if net.since.is_some() {
                false
            } else {
                let now = Instant::now();
                net.since = Some(now);
                net.next = now + self.options.jittered(self.options.lane_backoff_base);
                net.dials = 0;
                net.cause = reason.code.clone();
                true
            }
        };
        if began {
            event!(info, (reason.code.as_str()), {
                "peer": self.installation(),
                "lane": lane,
                "detail": "a healthy session was lost by the transport: the peer's lanes redial on one cadence until one connects",
                "message": reason.message,
            });
        }
    }

    /// The next redial slot while the peer's network is down (`None` while
    /// it is up): one slot per spacing for all its lanes, the spacing
    /// following the outage's age ([`PeerOptions::network_spacing`],
    /// jittered).
    fn network_slot(&self, now: Instant) -> Option<Instant> {
        let mut net = self.net.lock();
        let since = net.since?;
        let slot = net.next.max(now);
        let spacing = self
            .options
            .network_spacing(slot.saturating_duration_since(since));
        net.next = slot + self.options.jittered(spacing);
        net.dials += 1;
        Some(slot)
    }

    // -- a Ready lane -------------------------------------------------------

    /// Runs one established session until it closes (`Some`, with why and
    /// whether this side closed it) or the supervisor is cancelled (`None`).
    /// Always leaves the session closed or closing and its runner cancelled.
    fn serve(self: &Arc<Self>, lane: u32, conn: &Conn) -> Option<Ended> {
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
        let closed_here = Arc::new(AtomicBool::new(false));
        let runner = self.spawn_runner(lane, conn, &session_cancel, &closed_here);
        let ended = if lane == 0 {
            self.control_loop(lane, conn, &session_cancel)
        } else {
            self.bulk_loop(lane, conn, &session_cancel)
        };
        session_cancel.cancel();
        if ended.is_none() {
            // Stopping: the peer closes once idle after our GOAWAY. The lane
            // closes the session itself `force_close_margin` before the stop
            // deadline (the manager does too, for a lane stuck in a call).
            let wait = match self.force_close_at() {
                Some(at) => at.saturating_duration_since(Instant::now()),
                None => self.options.join,
            };
            if conn.wait_closed(wait).is_none() {
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
        ended.map(|reason| Ended {
            reason,
            local: closed_here.load(Ordering::Acquire),
        })
    }

    /// Starts `scheduler::run_control` (lane 0) or `objects::run_bulk_lane`
    /// on its own thread. The receiver disconnects when the runner returns.
    /// `closed_here` is set when the session is closed because the runner
    /// ended on it (or could not start).
    fn spawn_runner(
        self: &Arc<Self>,
        lane: u32,
        conn: &Conn,
        cancel: &Cancel,
        closed_here: &Arc<AtomicBool>,
    ) -> Option<(mpsc::Receiver<()>, JoinHandle<()>)> {
        let (done_tx, done_rx) = mpsc::channel::<()>();
        let ctx = self.ctx.clone();
        let peer = self.context.clone();
        let spec = spec_of(lane, self.pool);
        let owned = conn.clone();
        let cancel = cancel.clone();
        let closing = closed_here.clone();
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
                closing.store(true, Ordering::Release);
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
                closed_here.store(true, Ordering::Release);
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
        let mut next_rtt = deadline_in(self.options.rtt_refresh);
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
                next_poll = deadline_in(self.options.catalog_poll);
            }
            if Instant::now() >= next_rtt {
                self.refresh_rtt(lane, conn, &mut rtt);
                next_rtt = deadline_in(self.options.rtt_refresh);
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
            let retired = self.reporting();
            if !*retired {
                self.ctx.status.record(StatusEvent::PeerCatalog {
                    installation: self.record.installation.clone(),
                    entries: reply.entries,
                });
            }
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
    inner: &'a dyn ClientAuth,
    on_key: &'a (dyn Fn() + Sync),
}

impl ClientAuth for AnnounceHandshake<'_> {
    fn key(&self, salt: &str) -> Result<[u8; 32], ErrorWire> {
        self.inner.key(salt)
    }

    fn peer_key(&self) -> Option<[u8; 32]> {
        self.inner.peer_key()
    }

    fn begin_auth(&self) {
        (self.on_key)();
        self.inner.begin_auth();
    }
}

/// What a supervisor needs for per-peer credentials (WP4.1).
#[derive(Clone)]
struct Credentialing {
    catalog: Option<Arc<CredentialCatalog>>,
    writer: Arc<CredentialWriter>,
}

/// The per-peer credentials of a catalog that keeps them, through the
/// catalog contract (CCR-WP4.1-1, applied by WP5.4; its last callers moved
/// here by CCR-WP5.4-1, WP5.INT, which removed `Catalog::of`): the
/// published [`Credentials`] and the `*Credential` commands of
/// [`CatalogCmd`], with the semantics the catalog actor gives them.
struct CredentialCatalog(Arc<dyn CatalogApi>);

impl CredentialCatalog {
    /// `None` when `catalog` keeps no per-peer credentials.
    fn of(catalog: &Arc<dyn CatalogApi>) -> Option<Arc<CredentialCatalog>> {
        catalog
            .credentials()
            .is_some()
            .then(|| Arc::new(CredentialCatalog(catalog.clone())))
    }

    fn snapshot(&self) -> Arc<CatalogSnapshot> {
        self.0.snapshot()
    }

    fn credentials(&self) -> Arc<Credentials> {
        self.0.credentials().unwrap_or_default()
    }

    /// Stores `key` as the active credential of `installation` on `side`;
    /// with `paired_only`, only while a peer record names it (checked in
    /// the command's transaction).
    fn enroll_with(
        &self,
        installation: &str,
        side: CredentialSide,
        key: [u8; 32],
        paired_only: bool,
    ) -> Result<(), ErrorWire> {
        self.0
            .apply(CatalogCmd::EnrollCredential {
                installation: installation.to_owned(),
                side,
                key,
                paired_only,
                generation: None,
            })
            .map(drop)
    }

    fn enroll(
        &self,
        installation: &str,
        side: CredentialSide,
        key: [u8; 32],
    ) -> Result<(), ErrorWire> {
        self.enroll_with(installation, side, key, false)
    }

    fn enroll_paired(
        &self,
        installation: &str,
        side: CredentialSide,
        key: [u8; 32],
    ) -> Result<(), ErrorWire> {
        self.enroll_with(installation, side, key, true)
    }

    fn forget(&self, installation: &str, side: CredentialSide) -> Result<(), ErrorWire> {
        self.0
            .apply(CatalogCmd::ForgetCredential {
                installation: installation.to_owned(),
                side,
            })
            .map(drop)
    }
}

/// A credential job of the `peer-credentials` thread.
enum CredentialJob {
    /// Enroll on lane 0's session (authenticated with `code`, whose server
    /// hello is `server`), then store our outbound credential unless the
    /// supervisor stopped, its peer is being unpaired, or no record names
    /// the peer any more. An active credential other than `replaces` (the
    /// one the peer refused, if any) means one was stored meanwhile: nothing
    /// is enrolled. `done` is cleared at the end.
    Enroll {
        conn: Conn,
        server: Box<session::ServerHello>,
        replaces: Option<[u8; 32]>,
        cancel: Cancel,
        unpairing: Arc<AtomicBool>,
        done: Arc<AtomicBool>,
    },
}

/// Credential enrollments, off the lane threads (a slow or wedged catalog
/// never holds a lane, §2.3). The queue is bounded; a job that does not fit
/// is dropped (the next session establishment queues an enrollment again).
struct CredentialWriter {
    tx: Mutex<Option<mpsc::SyncSender<CredentialJob>>>,
}

impl CredentialWriter {
    fn start(catalog: Option<Arc<CredentialCatalog>>) -> Arc<CredentialWriter> {
        let Some(catalog) = catalog else {
            return Arc::new(CredentialWriter {
                tx: Mutex::new(None),
            });
        };
        let (tx, rx) = mpsc::sync_channel::<CredentialJob>(CREDENTIAL_QUEUE);
        let spawned = thread::Builder::new()
            .name("peer-credentials".into())
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    run_credential_job(&catalog, job);
                }
            });
        let tx = match spawned {
            Ok(_) => Some(tx),
            Err(error) => {
                event!(error, CATALOG_UNAVAILABLE, {
                    "detail": "cannot start the peer-credentials thread; peers keep their pairing codes",
                    "error": error.to_string(),
                });
                None
            }
        };
        Arc::new(CredentialWriter { tx: Mutex::new(tx) })
    }

    /// Queues `job`; `false` when it was dropped.
    fn post(&self, job: CredentialJob) -> bool {
        match lock(&self.tx).as_ref().map(|tx| tx.try_send(job)) {
            Some(Ok(())) => true,
            Some(Err(mpsc::TrySendError::Full(_))) => {
                event!(warn, CATALOG_UNAVAILABLE, {
                    "detail": "the peer-credentials queue is full; a credential job was dropped",
                });
                false
            }
            _ => false,
        }
    }

    /// No more jobs; the thread ends once the queue is drained.
    fn close(&self) {
        lock(&self.tx).take();
    }
}

fn run_credential_job(catalog: &CredentialCatalog, job: CredentialJob) {
    match job {
        CredentialJob::Enroll {
            conn,
            server,
            replaces,
            cancel,
            unpairing,
            done,
        } => {
            let installation = server.installation.clone();
            let stopped = || cancel.is_cancelled() || unpairing.load(Ordering::Acquire);
            let paired = || {
                catalog
                    .snapshot()
                    .peers
                    .iter()
                    .any(|peer| peer.installation == installation)
            };
            // `pair` (or an earlier job) stored one since this was queued.
            let active = catalog
                .credentials()
                .active(&installation, CredentialSide::Outbound);
            if stopped() || !paired() || active.is_some_and(|key| Some(key) != replaces) {
                done.store(false, Ordering::Release);
                return;
            }
            // A refusal the acceptor may lift (`BUSY`, a catalog it could
            // not write) is retried on the same session while it stays up
            // (WP4.INT re-review: it waited for lane 0 to reconnect, which
            // can be days on a stable link).
            let mut outcome = enroll_on(&conn);
            for delay in ENROLL_RETRIES {
                let retryable = outcome.as_ref().is_err_and(|error| {
                    matches!(
                        crate::model::errors::classify(&error.code),
                        crate::model::errors::ErrorClass::Contention
                            | crate::model::errors::ErrorClass::Transient
                    )
                });
                if !retryable || conn.closed().is_some() || stopped() || cancel.sleep(delay) {
                    break;
                }
                if conn.closed().is_some() || stopped() {
                    break;
                }
                outcome = enroll_on(&conn);
            }
            match outcome {
                // Stored only while a record names the peer, checked in the
                // catalog's transaction: an unpair's `RemovePeer` either
                // runs first (refused) or deletes it with the record.
                Ok(key) if !stopped() => {
                    if let Err(error) =
                        catalog.enroll_paired(&installation, CredentialSide::Outbound, key)
                    {
                        event!(warn, (error.code.as_str()), {
                            "peer": installation,
                            "detail": "the per-peer credential could not be stored",
                            "message": error.message,
                        });
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    event!(info, (error.code.as_str()), {
                        "peer": installation,
                        "detail": "no per-peer credential was enrolled; the pairing code stays in use",
                        "message": error.message,
                    });
                }
            }
            done.store(false, Ordering::Release);
        }
    }
}

/// Ends a lane thread (also when it panics): the probe released, `Closed`,
/// one less live lane.
struct LaneExit {
    supervisor: Arc<Supervisor>,
    lane: u32,
}

impl Drop for LaneExit {
    fn drop(&mut self) {
        let supervisor = &self.supervisor;
        supervisor.conns.lock().remove(&self.lane);
        supervisor.release_probe(self.lane);
        supervisor.set_lane(self.lane, LaneState::Closed, None, None, None);
        supervisor.live.fetch_sub(1, Ordering::AcqRel);
        supervisor.exited.poke();
    }
}

/// One lane's thread: gate, dial, serve, back off, until cancelled (module
/// docs: the lane table and "Network loss").
fn run_lane(supervisor: Arc<Supervisor>, lane: u32) {
    let _exit = LaneExit {
        supervisor: supervisor.clone(),
        lane,
    };
    let options = supervisor.options;
    let mut backoff = options.lane_backoff();
    let mut announced: Option<u64> = None;
    // Consecutive failed attempts since the lane was last Ready (the view's
    // `attempt`; a down peer's paced redials do not escalate `backoff`).
    let mut failures = 0u32;
    loop {
        // WP4.INT (the WP4.1 review's minor): while the peer is being
        // unpaired its lanes do not dial. The acceptor was just told to
        // revoke us and sends our sessions away; a re-dial (with the code,
        // once the credential is refused) would be served again until
        // `RemovePeer` and the reconcile stop this supervisor. A failed
        // `RemovePeer` clears the mark and the lanes go on.
        while supervisor.unpairing.load(Ordering::Acquire) {
            if supervisor.cancel.sleep(UNPAIRING_PARK) {
                return;
            }
        }
        if !supervisor.admit(lane, &mut announced) {
            return;
        }
        // The connects of siblings seen when this dial starts (or, below,
        // when its session ends): one that connects later shows the peer
        // reachable.
        let mut seen = supervisor.connects.load(Ordering::Acquire);
        let attempt = supervisor.arm(
            lane,
            Attempt::Dial {
                since: Instant::now(),
                answered: false,
            },
        );
        supervisor.set_lane(lane, LaneState::Dialing, None, None, None);
        let presented = supervisor.outbound_key();
        let started = Instant::now();
        let dialed = supervisor.dial(lane, presented, &attempt);
        let took = started.elapsed();
        // A sibling connected while this dial got no answer (WP5.3).
        let interrupted = attempt.token.is_cancelled();
        drop(attempt);
        if supervisor.cancel.is_cancelled() {
            if let Ok((conn, _)) = dialed {
                conn.close();
            }
            return;
        }
        // WP4.1: the peer refused our credential: drop it and re-dial with
        // the code at once (the next dial no longer presents it).
        if let (Err(error), Some(key)) = (&dialed, presented)
            && error.to_wire().code == "PEER_REVOKED"
        {
            supervisor.credential_refused(key);
            continue;
        }
        // `lost`: the network's failure (a dial that reached nothing, a
        // session lost by the transport); `healthy`: a session that was
        // Ready long enough to count (`lane_reset_after`).
        let (error, lost, healthy, dial_failed) = match dialed {
            Ok((conn, server)) => {
                supervisor.gate_success();
                supervisor.lane_connected(lane, started, took);
                if lane == 0 && presented.is_none() {
                    supervisor.enroll_later(&conn, &server);
                }
                // No `backoff.success`: the attempt count resets only once
                // the session proves healthy (below), never through
                // `Backoff`'s own healthy period, which a redial the down
                // peer paces (no `failure`) would carry past an unhealthy
                // session (the WP5.3 review's minor).
                failures = 0;
                supervisor.update_lane(lane, |view| view.attempt = 0);
                let protocol = format!("{}/{}", server.protocol, server.version);
                supervisor.update_lane(lane, |view| view.protocol = Some(protocol));
                let ready = Instant::now();
                match supervisor.serve(lane, &conn) {
                    None => return,
                    Some(ended) => {
                        seen = supervisor.connects.load(Ordering::Acquire);
                        let lost = session_lost(&ended);
                        let healthy = ready.elapsed() >= options.lane_reset_after;
                        (ended.reason, lost, healthy, false)
                    }
                }
            }
            // A stale dial a connected sibling interrupted: redial at once.
            Err(_) if interrupted => continue,
            Err(error) => match classify_dial(&error) {
                Failure::Block(state, wire) => {
                    supervisor.gate_block(lane, state, wire);
                    continue;
                }
                Failure::Retry(wire) => {
                    supervisor.gate_transient(lane);
                    (wire, dial_lost(&error), false, true)
                }
            },
        };
        failures = failures.saturating_add(1);
        let now = Instant::now();
        if healthy {
            // A healthy lane starts over from the base delay; its loss by
            // the transport is the peer's network loss.
            backoff.reset();
            if lost {
                supervisor.network_lost(lane, &error);
            }
        }
        let paced = if lost {
            supervisor.network_slot(now)
        } else {
            None
        };
        // A sibling that connects ends the wait only when this lane reached
        // no session (its dial failed) or the down peer paces it; after a
        // session that ended otherwise (one the peer closed before it was
        // healthy: a flapping acceptor) the lane keeps its own escalating
        // delay, so lanes never wake each other into a loop. Armed before
        // the connects are checked: a sibling that connects from now on
        // cuts a wakeable wait short.
        let wakeable = dial_failed || paced.is_some();
        let wait = supervisor.arm(lane, Attempt::Wait { wakeable });
        let mut until = paced.unwrap_or_else(|| now + backoff.failure(now));
        if wakeable && supervisor.connects.load(Ordering::Acquire) != seen {
            // A sibling connected since this dial began (or this session
            // ended): the peer is reachable, try again now.
            until = now;
        }
        let delay = until.saturating_duration_since(now);
        let until_ms = supervisor
            .ctx
            .clock
            .now_ms()
            .saturating_add(u64::try_from(delay.as_millis()).unwrap_or(u64::MAX));
        supervisor.update_lane(lane, |view| view.attempt = failures);
        supervisor.set_lane(lane, LaneState::Backoff, Some(error), Some(until_ms), None);
        if wait.token.sleep(delay) && supervisor.cancel.is_cancelled() {
            return;
        }
    }
}

/// How often a lane parked by an unpair in progress rechecks (`run_lane`).
const UNPAIRING_PARK: Duration = Duration::from_millis(20);

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

#[cfg(test)]
mod tests {
    use super::*;

    fn ended(code: &str, message: &str, local: bool) -> Ended {
        Ended {
            reason: ErrorWire::coded(code, message),
            local,
        }
    }

    /// A session this side closed (a lane runner that gave up on it, or
    /// could not start) is never a network loss, although its code is the
    /// transport's `WS_CLOSED`; the same code from the peer's side is, and
    /// a `GOAWAY`'s code never is (the WP5.3 review's minor).
    #[test]
    fn only_the_transports_closes_are_network_losses() {
        assert!(!session_lost(&ended(
            "WS_CLOSED",
            "the connection was closed locally",
            true
        )));
        assert!(session_lost(&ended(
            "WS_CLOSED",
            "the peer closed the connection",
            false
        )));
        for code in ["CONNECTION_LOST", "TRANSPORT_DEAD"] {
            assert!(session_lost(&ended(code, "lost", false)), "{code}");
            assert!(!session_lost(&ended(code, "lost", true)), "{code}");
        }
        assert!(!session_lost(&ended(
            "ORCHESTRATOR_STOPPING",
            "the peer is stopping",
            false
        )));
    }
}
