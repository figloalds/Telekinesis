//! Owned by WP2.7.
//!
//! The MetaScheduler of a peer's control lane (lane 0; REVAMP_PLAN §3.7 and
//! §14 WP2.7), and the scheduling engine it shares with the interim bulk
//! lanes ([`super::objects`]).
//!
//! # Keys and slots
//!
//! A scheduler serves one peer and one op: `meta.exchange` on lane 0,
//! `objects.exchange` on each bulk lane. Its keys are the repositories that
//! the peer's catalog lists and a local folder holds, so a key is one
//! (peer, repo, op). At most `slots` exchanges run at once (4 on lane 0, 2 on
//! a bulk lane) and at most one per key. Every exchange has its own
//! deadlines (30 s for the peer call of `meta.exchange`). A slow or failing
//! key holds one slot and backs off on its own, while the other keys keep
//! their cadence on the same connection (RC5). "One per key" also holds
//! across the schedulers of this process, for a lane respawned on a new
//! session while an exchange of the old one is still ending.
//!
//! # When a key is due
//!
//! * **Triggers** (due now): the local feed tip moved; the remote catalog's
//!   tip of the repository moved; the local heartbeat's `generations.sync`
//!   (bulk lanes: `generations.content`, and the remote's content counters)
//!   moved while no local exchange of that repository ran or had ended
//!   within `absorb` (a bump in that window is the exchange's own echo, and
//!   counting it would make exchanges trigger each other forever). A trigger
//!   that arrives during an exchange makes the key due as soon as it ends.
//! * **Resets** (due now, retry state cleared): the key became ready, or the
//!   local worker instance or the remote replica changed.
//! * **Anti-entropy:** `interval` (`sync_interval_ms`) after the last
//!   exchange ended.
//! * **Continuation:** an exchange that moved data and has more to move runs
//!   again at once (bulk lanes: after 10 ms). An exchange that only
//!   recovered a cursor runs again at once, at most [`QUICK_LIMIT`] times in
//!   a row.
//! * **Retry** after a failure (below).
//!
//! # Not ready is not a failure
//!
//! `WaitingForWorker` (the local folder is not Running), `WaitingForRemote`
//! (the remote entry is not Running) and `Paused` (sync is disabled) keep a
//! key from running without counting anything. The key is due as soon as it
//! is ready again. Answers that say the same thing ([`WAITING_CODES`]:
//! `REMOTE_WORKER_STARTING`, `FOLDER_SYNC_PAUSED`, a local worker that cannot
//! be reached) are rechecked after `waiting_recheck`, again without counting.
//! The StatusHub reads the remote ones as WaitingForRemote or Paused; a local
//! one ([`LOCAL_WAITING_CODES`]) reaches it without an error, since the
//! folder's lifecycle already says WaitingForWorker.
//!
//! # Failures (§8, by class)
//!
//! | Class | Retry | Counted |
//! |---|---|---|
//! | Contention | 50 ms → 1 s | no |
//! | Transient | 1 s → 60 s | yes |
//! | NeedsUserAction | recheck after 60 s, or on a reset | yes |
//! | Incompatible, Fatal | recheck after 10 min, or on a reset | yes |
//!
//! A remote `retry_after_ms` lengthens the delay, but only up to
//! [`MAX_RETRY_AFTER`] (60 s, the Transient cap): a peer's hint never holds a
//! key longer than that or the key's own recheck. A `Timeout` is uncertain:
//! the next attempt is a fresh request, because the worker builds a new
//! offer with a new id, and a request id is never sent twice (invariant 16).
//! A session that is lost or going away (`ConnectionLost`, or the session
//! refusing a call) charges no key. The scheduler then stops, and the
//! PeerSupervisor redials.
//!
//! # Stopping
//!
//! While the daemon stops (`ctx.stopping`) nothing new is dispatched. A
//! cancelled lane returns within [`EXIT_WAIT`] (+ ~50 ms), well inside the
//! PeerSupervisor's 1 s runner join; exchanges still blocked in a worker call
//! then end on their own and release their keys themselves.
use super::{
    Config,
    meta_exchange::{self, ExchangeOptions},
};
use crate::{
    model::{
        errors::{ErrorClass, ErrorWire, Scope},
        health::ContentCounters,
        orchestrator::{
            CallError, Ctx, EntryState, FeedTip, FolderPhase, FolderView, PeerContext,
            PublishedEntry, limits,
        },
        protocol::{LaneRole, LaneSpec, feed::ExchangeSummary, ops},
        status::{Direction, StatusEvent},
    },
    session::Conn,
    util::{backoff::Backoff, cancel::Cancel, lock::lock, log::event, snapshot::Snapshot},
};
use serde::Serialize;
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

/// The status lane number of the control session.
pub const CONTROL_LANE: u32 = 0;
/// A local generation bump this soon after a local exchange of the same
/// repository ended is that exchange's own echo, not a trigger. A worker
/// publishes a bump in its next heartbeat (1 s); under mutual pairing the
/// peer's answering exchange shows up one more heartbeat later. Three
/// heartbeat periods absorb both, so two daemons can never keep each other
/// exchanging on generation bumps alone.
pub const ABSORB: Duration = Duration::from_secs(3);
/// Recheck of a key whose folder, local or remote, answered that it is not
/// serving (the remote catalog is polled this often too).
pub const WAITING_RECHECK: Duration = limits::CATALOG_POLL;
/// Recheck of a key parked by a NeedsUserAction answer (§8: "recheck 60 s").
pub const RECHECK_USER_ACTION: Duration = limits::UNAUTHORIZED_RECHECK;
/// Recheck of a key parked by an Incompatible answer (§8: 10 min).
pub const RECHECK_INCOMPATIBLE: Duration = limits::INCOMPATIBLE_RECHECK;
/// Recheck of a key parked by a Fatal answer ("no retry in scope"; a reset
/// of the key, such as a new worker or a new remote replica, retries sooner).
pub const RECHECK_FATAL: Duration = limits::INCOMPATIBLE_RECHECK;
/// How long a stopping scheduler waits for its exchanges in flight before it
/// leaves them to finish on their own. A closed session fails peer calls at
/// once, so this only matters for a worker call in flight (up to 10 s, or
/// 30 s on a bulk lane), which cannot be interrupted. The PeerSupervisor
/// (WP2.6) joins a lane runner for 1 s after cancelling it and then detaches
/// it, so the whole exit (this wait, plus the slot and watcher joins, which
/// overlap it) stays well inside that (CCR-WP2.7-4).
pub const EXIT_WAIT: Duration = Duration::from_millis(500);
/// The longest delay a peer's `retry_after_ms` can impose on a key: the cap
/// of the Transient backoff (§8). A larger (or absurd) value is clamped: the
/// Contention, Backoff and Parked holds ignore triggers, so an unbounded hint
/// could keep a key from running until a reset.
pub const MAX_RETRY_AFTER: Duration = Duration::from_secs(60);
/// Immediate re-runs in a row of exchanges that moved nothing (cursor
/// recoveries) before the key waits for its interval.
pub const QUICK_LIMIT: u32 = 2;
/// Answer codes that say a folder (local or remote) is not serving: not a
/// failure of the key (WaitingForWorker, WaitingForRemote, Paused).
pub const WAITING_CODES: &[&str] = &[
    "REMOTE_WORKER_STARTING",
    "FOLDER_SYNC_PAUSED",
    "LOCAL_WORKER_UNAVAILABLE",
    "WORKER_NOT_RUNNING",
    "WORKER_STOPPING",
    "WORKER_EXITED",
];
/// The [`WAITING_CODES`] that say the *local* folder is not serving when a
/// local worker call answers them: the folder's own WaitingForWorker state
/// (§3.7), which its lifecycle shows. Such an exchange is reported to the
/// StatusHub as not completed but without an error (see
/// [`ExchangeError::local_waiting`]).
pub const LOCAL_WAITING_CODES: &[&str] = &[
    "LOCAL_WORKER_UNAVAILABLE",
    "WORKER_NOT_RUNNING",
    "WORKER_STOPPING",
    "WORKER_EXITED",
];

/// Upper bound of one wait of the scheduling loop.
const MAX_WAIT: Duration = Duration::from_secs(1);
/// The watcher's wait slice; the remote catalog and the session are sampled
/// this often.
const WATCH_SLICE: Duration = Duration::from_millis(250);
/// A key claimed by another scheduler of this process is rechecked this often.
const CLAIM_RETRY: Duration = Duration::from_millis(100);
/// Request ids remembered by the replay guard.
const SENT_IDS: usize = 4096;
/// Entries of the activity registry before idle ones are pruned.
const ACTIVITY_PRUNE: usize = 4096;

// ---------------------------------------------------------------------------
// Public model

/// Whether a key may run now; none of the other states is a failure.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Readiness {
    #[default]
    Ready,
    /// The local folder is not Running (or has no worker endpoint).
    WaitingForWorker,
    /// The remote catalog entry is starting, unavailable or paused.
    WaitingForRemote,
    /// Sync is disabled for the local folder.
    Paused,
}

/// The readiness of a (local folder, remote entry) pair.
pub fn readiness(folder: &FolderView, remote: &PublishedEntry) -> Readiness {
    if !folder.record.sync_enabled {
        Readiness::Paused
    } else if !matches!(folder.phase, FolderPhase::Running { .. }) || folder.worker.is_none() {
        Readiness::WaitingForWorker
    } else if remote.state != EntryState::Running {
        Readiness::WaitingForRemote
    } else {
        Readiness::Ready
    }
}

/// How the scheduler treats a failed exchange (§8, plus [`WAITING_CODES`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    /// Retried after 50 ms → 1 s; never counted.
    Contention,
    /// A folder is not serving; rechecked, never counted.
    Waiting,
    /// Counted; exponential backoff 1 s → 60 s.
    Transient,
    /// Counted; parked until a reset or the 60 s recheck.
    NeedsUserAction,
    /// Counted; parked until a reset or the 10 min recheck.
    Incompatible,
    /// Counted; parked until a reset or the 10 min recheck.
    Fatal,
}

impl FailureKind {
    /// Whether an occurrence counts as a failure of its key.
    pub fn counted(self) -> bool {
        !matches!(self, FailureKind::Contention | FailureKind::Waiting)
    }
}

/// The [`FailureKind`] of an error.
pub fn failure_kind(error: &ErrorWire) -> FailureKind {
    if WAITING_CODES.contains(&error.code.as_str()) {
        return FailureKind::Waiting;
    }
    match error.class {
        ErrorClass::Contention => FailureKind::Contention,
        ErrorClass::Transient => FailureKind::Transient,
        ErrorClass::NeedsUserAction => FailureKind::NeedsUserAction,
        ErrorClass::Incompatible => FailureKind::Incompatible,
        ErrorClass::Fatal => FailureKind::Fatal,
    }
}

/// Which side of an exchange failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Step {
    /// A worker call, or a check of what the worker returned.
    Local,
    /// The peer call, or a check of what the peer returned.
    Peer,
}

/// A failed exchange: the error and the side it came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExchangeError {
    pub step: Step,
    pub error: CallError,
}

impl ExchangeError {
    pub fn local(error: CallError) -> Self {
        Self {
            step: Step::Local,
            error,
        }
    }

    pub fn peer(error: CallError) -> Self {
        Self {
            step: Step::Peer,
            error,
        }
    }

    /// The peer session was lost (`ConnectionLost`, uncertain) or refused the
    /// call because it is closing or going away (`Unavailable`, nothing was
    /// sent). Neither is the key's failure.
    pub fn session_lost(&self) -> bool {
        self.step == Step::Peer
            && matches!(
                self.error,
                CallError::ConnectionLost(_) | CallError::Unavailable(_)
            )
    }

    /// A local worker call answered that the local folder is not serving
    /// ([`LOCAL_WAITING_CODES`]): WaitingForWorker, not a failure. The key
    /// rechecks without counting it ([`FailureKind::Waiting`]), and the
    /// StatusHub gets the exchange without an error, because the hub's
    /// display policy would show these Transient codes as a failing pair
    /// ("Backoff") from the third recheck on.
    pub fn local_waiting(&self) -> bool {
        self.step == Step::Local && LOCAL_WAITING_CODES.contains(&self.error.code().as_str())
    }

    pub fn wire(&self) -> ErrorWire {
        self.error.to_wire()
    }
}

impl fmt::Display for ExchangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for ExchangeError {}

impl From<ExchangeError> for CallError {
    fn from(error: ExchangeError) -> Self {
        error.error
    }
}

/// What a key is doing (the scheduler view).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyPhase {
    /// Due: it runs as soon as a slot is free.
    Due,
    Running,
    /// Waiting for its anti-entropy interval or a trigger.
    Idle,
    /// Retrying after Contention (not counted).
    Contention,
    /// Retrying after a counted failure.
    Backoff,
    /// Rechecking a folder that answered it is not serving (not counted).
    Waiting,
    /// Parked after a NeedsUserAction, Incompatible or Fatal answer.
    Parked,
    /// Not ready: see the key's [`Readiness`].
    NotReady,
}

/// One key of a scheduler.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct KeyView {
    pub repo: String,
    pub readiness: Readiness,
    pub phase: KeyPhase,
    /// Counted failures since the last success (Contention and waiting
    /// answers excluded).
    pub failures: u32,
    /// Contention answers since the last success or counted failure.
    pub contention: u32,
    /// Exchanges started.
    pub exchanges: u64,
    /// Exchanges that succeeded.
    pub succeeded: u64,
    /// Time until the key is due (`None` while running or not ready).
    pub due_in_ms: Option<u64>,
    pub last_error: Option<ErrorWire>,
}

/// The state of one scheduler, published after each turn when
/// [`DriveOptions::view`] is set (diagnostics and tests).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct SchedulerView {
    pub op: String,
    pub lane: u32,
    pub slots: usize,
    pub in_flight: usize,
    /// The most exchanges ever in flight at once.
    pub peak_in_flight: usize,
    pub started: u64,
    pub finished: u64,
    /// Sorted by repository.
    pub keys: Vec<KeyView>,
    /// The scheduler ended (cancelled, or its session closed or went away).
    pub stopped: bool,
}

impl SchedulerView {
    pub fn key(&self, repo: &str) -> Option<&KeyView> {
        self.keys.iter().find(|key| key.repo == repo)
    }
}

/// Tuning of the scheduling engine.
#[derive(Clone, Debug)]
pub struct DriveOptions {
    /// Exchanges in flight at once (at most one per key in any case).
    pub slots: usize,
    /// Idle anti-entropy interval.
    pub interval: Duration,
    /// See [`ABSORB`].
    pub absorb: Duration,
    /// See [`WAITING_RECHECK`].
    pub waiting_recheck: Duration,
    pub recheck_user_action: Duration,
    pub recheck_incompatible: Duration,
    pub recheck_fatal: Duration,
    /// See [`EXIT_WAIT`].
    pub exit_wait: Duration,
    /// See [`MAX_RETRY_AFTER`].
    pub max_retry_after: Duration,
    /// Where to publish the [`SchedulerView`] (none by default).
    pub view: Option<Arc<Snapshot<SchedulerView>>>,
}

impl DriveOptions {
    pub fn new(slots: usize, interval: Duration) -> Self {
        Self {
            slots,
            interval,
            absorb: ABSORB,
            waiting_recheck: WAITING_RECHECK,
            recheck_user_action: RECHECK_USER_ACTION,
            recheck_incompatible: RECHECK_INCOMPATIBLE,
            recheck_fatal: RECHECK_FATAL,
            exit_wait: EXIT_WAIT,
            max_retry_after: MAX_RETRY_AFTER,
            view: None,
        }
    }

    fn policy(&self) -> Policy {
        Policy {
            waiting: self.waiting_recheck,
            user_action: self.recheck_user_action,
            incompatible: self.recheck_incompatible,
            fatal: self.recheck_fatal,
            max_asked: self.max_retry_after,
        }
    }
}

/// The anti-entropy interval of a configuration (`sync_interval_ms`, at
/// least 10 ms).
pub fn sync_interval(config: &Config) -> Duration {
    Duration::from_millis(config.network.sync_interval_ms.max(10))
}

/// Options of the control-lane scheduler.
#[derive(Clone, Debug)]
pub struct ControlOptions {
    pub drive: DriveOptions,
    pub exchange: ExchangeOptions,
}

impl ControlOptions {
    /// 4 slots, the configured interval, the frozen deadlines.
    pub fn for_config(config: &Config) -> Self {
        Self {
            drive: DriveOptions::new(limits::EXCHANGE_SLOTS, sync_interval(config)),
            exchange: ExchangeOptions::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// Control lane

/// The MetaScheduler of a peer's control lane: runs until `cancel` fires or
/// the session closes or goes away.
pub fn run_control(ctx: Arc<Ctx>, peer: Arc<PeerContext>, conn: Conn, cancel: Cancel) {
    let options = ControlOptions::for_config(&ctx.config);
    run_control_with(ctx, peer, conn, cancel, options);
}

/// [`run_control`] with explicit options.
pub fn run_control_with(
    ctx: Arc<Ctx>,
    peer: Arc<PeerContext>,
    conn: Conn,
    cancel: Cancel,
    options: ControlOptions,
) {
    let name = format!("ctl-{}", short(&peer.installation));
    if let Err(error) = check_session(&peer, &conn, LaneRole::Control, None) {
        refuse(
            &ctx,
            &peer,
            &conn,
            &cancel,
            CONTROL_LANE,
            error,
            &options.drive,
        );
        return;
    }
    let job = Arc::new(MetaJob {
        ctx: ctx.clone(),
        peer: peer.clone(),
        conn: conn.clone(),
        options: options.exchange,
    });
    drive(
        &ctx,
        &peer,
        &conn,
        &cancel,
        job,
        &options.drive,
        CONTROL_LANE,
        &name,
    );
}

struct MetaJob {
    ctx: Arc<Ctx>,
    peer: Arc<PeerContext>,
    conn: Conn,
    options: ExchangeOptions,
}

impl Job for MetaJob {
    fn op(&self) -> &str {
        ops::META_EXCHANGE
    }

    fn scope(&self) -> String {
        ops::META_EXCHANGE.to_owned()
    }

    fn class(&self) -> &'static str {
        "meta"
    }

    fn triggered(&self, before: &Signals, after: &Signals, absorbing: bool) -> bool {
        before.tip != after.tip
            || before.remote_tip != after.remote_tip
            || (before.sync != after.sync && !absorbing)
    }

    fn run(
        &self,
        target: &Target,
        retry_after: &mut dyn FnMut(&ExchangeError) -> Option<Duration>,
    ) -> Result<Next, ExchangeError> {
        let outcome = meta_exchange::exchange_with(
            &self.ctx,
            &self.conn,
            &self.peer,
            &target.folder,
            &target.remote,
            &self.options,
            retry_after,
        )?;
        let moved = outcome.moved();
        Ok(if moved && outcome.more {
            Next {
                after: Some(Duration::ZERO),
                progress: true,
            }
        } else if outcome.recovered() {
            Next {
                after: Some(Duration::ZERO),
                progress: false,
            }
        } else {
            Next {
                after: None,
                progress: moved,
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Session checks

/// The checks every lane makes on the session it is handed: the
/// authenticated installation is the peer's (I12), and the role (and bulk
/// lane) is the one the lane carries (invariant 18).
pub(crate) fn check_session(
    peer: &PeerContext,
    conn: &Conn,
    role: LaneRole,
    lane: Option<LaneSpec>,
) -> Result<(), ErrorWire> {
    let info = conn.peer();
    if !info.installation.is_empty() && info.installation != peer.installation {
        return Err(ErrorWire::coded(
            "PEER_INSTALLATION_CHANGED",
            format!(
                "the session is authenticated as {}, not {}",
                info.installation, peer.installation
            ),
        ));
    }
    if info.role != role {
        let code = match role {
            LaneRole::Control => "METADATA_ON_BULK_LANE",
            LaneRole::Bulk | LaneRole::Probe => "BULK_ON_CONTROL_LANE",
        };
        return Err(ErrorWire::coded(
            code,
            format!(
                "a {} lane was handed a {} session",
                role.as_str(),
                info.role.as_str()
            ),
        ));
    }
    if let (Some(lane), Some(session)) = (lane, info.lane)
        && lane != session
    {
        return Err(ErrorWire::coded(
            "BULK_LANE_MISMATCH",
            format!(
                "lane {}/{} was handed the session of lane {}/{}",
                lane.index, lane.count, session.index, session.count
            ),
        ));
    }
    Ok(())
}

/// A lane that cannot run on its session: reports why, then holds the lane
/// (as the PeerSupervisor expects) until it is cancelled or the session
/// closes.
pub(crate) fn refuse(
    ctx: &Ctx,
    peer: &PeerContext,
    conn: &Conn,
    cancel: &Cancel,
    lane: u32,
    error: ErrorWire,
    options: &DriveOptions,
) {
    event!(error, (error.code.as_str()), {
        "peer": peer.installation,
        "lane": lane,
        "message": error.message,
    });
    ctx.status.record(StatusEvent::Error {
        scope: Scope::Lane {
            installation: peer.installation.clone(),
            lane,
        },
        error,
    });
    if let Some(view) = &options.view {
        view.store(SchedulerView {
            lane,
            slots: options.slots,
            stopped: true,
            ..SchedulerView::default()
        });
    }
    while conn.closed().is_none() && !cancel.sleep(WATCH_SLICE) {}
}

/// The first 8 characters of an installation, for thread names.
pub(crate) fn short(installation: &str) -> &str {
    installation
        .char_indices()
        .nth(8)
        .map_or(installation, |(end, _)| &installation[..end])
}

// ---------------------------------------------------------------------------
// Status reporting shared by the lanes

/// What a successful exchange reports (`ExchangeFinished`).
#[derive(Clone, Debug, Default)]
pub(crate) struct Finished {
    pub summary: Option<ExchangeSummary>,
    pub bytes_up: u64,
    pub bytes_down: u64,
    /// Completed objects only (installed here, or acknowledged by the peer).
    pub objects_up: u64,
    pub objects_down: u64,
}

/// One outbound exchange as the StatusHub sees it: `ExchangeStarted` when it
/// begins, `ExchangeFinished` with its real duration when it ends.
pub(crate) struct Report<'a> {
    ctx: &'a Ctx,
    installation: &'a str,
    repo: &'a str,
    lane: u32,
    op: &'a str,
    started: Instant,
}

impl<'a> Report<'a> {
    pub fn start(
        ctx: &'a Ctx,
        installation: &'a str,
        repo: &'a str,
        lane: u32,
        op: &'a str,
    ) -> Self {
        ctx.status.record(StatusEvent::ExchangeStarted {
            installation: installation.to_owned(),
            repo: repo.to_owned(),
            lane,
            op: op.to_owned(),
            at_ms: ctx.clock.now_ms(),
            direction: Direction::Outbound,
        });
        Self {
            ctx,
            installation,
            repo,
            lane,
            op,
            started: Instant::now(),
        }
    }

    /// Records `ExchangeFinished`. A failure carries the delay before the
    /// key's next attempt as `retry_after_ms` (the hub's `backoff_until_ms`):
    /// the delay actually used, which already honours the peer's own
    /// `retry_after_ms` up to [`MAX_RETRY_AFTER`]. A local folder that is not
    /// serving ([`ExchangeError::local_waiting`]) is reported as not
    /// completed (`ok: false`) with no error: the hub records no failure for
    /// it, and the folder's lifecycle shows why.
    pub fn finish(
        self,
        result: Result<&Finished, &ExchangeError>,
        retry_after: &mut dyn FnMut(&ExchangeError) -> Option<Duration>,
    ) -> u64 {
        let duration_ms = ms(self.started.elapsed());
        let (ok, error, done) = match result {
            Ok(done) => (true, None, done.clone()),
            Err(failure) => {
                let mut error = failure.wire();
                // Asked in every case: it decides the key's next attempt.
                if let Some(delay) = retry_after(failure) {
                    error.retry_after_ms = Some(ms(delay));
                }
                let error = (!failure.local_waiting()).then_some(error);
                (false, error, Finished::default())
            }
        };
        self.ctx.status.record(StatusEvent::ExchangeFinished {
            installation: self.installation.to_owned(),
            repo: self.repo.to_owned(),
            lane: self.lane,
            op: self.op.to_owned(),
            direction: Direction::Outbound,
            ok,
            error,
            summary: done.summary,
            bytes_up: done.bytes_up,
            bytes_down: done.bytes_down,
            objects_up: done.objects_up,
            objects_down: done.objects_down,
            duration_ms,
        });
        duration_ms
    }
}

pub(crate) fn ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Remembers a request id about to be sent; `false` if it was sent before
/// (invariant 16: an uncertain request is never replayed, so the same id must
/// never go out twice, even from a faulty worker).
pub(crate) fn first_send(request: &str) -> bool {
    struct Sent {
        order: VecDeque<String>,
        set: HashSet<String>,
    }
    static SENT: OnceLock<Mutex<Sent>> = OnceLock::new();
    let mut sent = lock(SENT.get_or_init(|| {
        Mutex::new(Sent {
            order: VecDeque::new(),
            set: HashSet::new(),
        })
    }));
    if !sent.set.insert(request.to_owned()) {
        return false;
    }
    sent.order.push_back(request.to_owned());
    while sent.order.len() > SENT_IDS {
        if let Some(old) = sent.order.pop_front() {
            sent.set.remove(&old);
        }
    }
    true
}

// ---------------------------------------------------------------------------
// The engine

/// What a key exchanges: the local folder and the peer's entry for its repo.
#[derive(Clone, Debug)]
pub(crate) struct Target {
    pub repo: String,
    pub folder: Arc<FolderView>,
    pub remote: PublishedEntry,
}

/// What the scheduler watches of a key between exchanges.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Signals {
    pub readiness: Readiness,
    pub instance: Option<String>,
    pub sync: u64,
    pub content: u64,
    pub tip: Option<FeedTip>,
    pub remote_replica: String,
    pub remote_tip: Option<FeedTip>,
    pub remote_content: Option<ContentCounters>,
}

impl Signals {
    fn of(folder: &FolderView, remote: &PublishedEntry) -> Self {
        let heartbeat = folder.heartbeat.as_deref();
        Self {
            readiness: readiness(folder, remote),
            instance: folder.worker.as_ref().map(|worker| worker.instance.clone()),
            sync: heartbeat.map_or(0, |hb| hb.generations.sync),
            content: heartbeat.map_or(0, |hb| hb.generations.content),
            tip: heartbeat.and_then(|hb| hb.tip.clone()),
            remote_replica: remote.replica.clone(),
            remote_tip: remote.tip.clone(),
            remote_content: remote.content,
        }
    }
}

/// What a successful exchange asks for next.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Next {
    /// Run again after this (`None`: wait for the interval).
    pub after: Option<Duration>,
    /// The exchange moved something (an immediate re-run is then unbounded).
    pub progress: bool,
}

/// The op-specific half of a lane.
pub(crate) trait Job: Send + Sync + 'static {
    /// The status op name.
    fn op(&self) -> &str;
    /// The claim scope: the op, plus the bulk lane where several run.
    fn scope(&self) -> String;
    /// The activity class whose local exchanges absorb generation bumps.
    fn class(&self) -> &'static str;
    /// Whether a change between two observations of a ready key calls for
    /// an exchange now.
    fn triggered(&self, before: &Signals, after: &Signals, absorbing: bool) -> bool;
    /// One exchange, on a slot thread. `retry_after` is asked once for the
    /// delay reported with a failure.
    fn run(
        &self,
        target: &Target,
        retry_after: &mut dyn FnMut(&ExchangeError) -> Option<Duration>,
    ) -> Result<Next, ExchangeError>;
    /// The key went away or was reset: drop per-key caches.
    fn forget(&self, _repo: &str) {}
}

#[derive(Clone, Copy, Debug)]
struct Policy {
    waiting: Duration,
    user_action: Duration,
    incompatible: Duration,
    fatal: Duration,
    /// The bound of a peer's `retry_after_ms`.
    max_asked: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verdict {
    Contention(Duration),
    Waiting(Duration),
    Failure(Duration),
    Parked(Duration),
}

impl Verdict {
    fn delay(self) -> Duration {
        match self {
            Verdict::Contention(d)
            | Verdict::Waiting(d)
            | Verdict::Failure(d)
            | Verdict::Parked(d) => d,
        }
    }
}

/// Per-key retry state: the Transient backoff (counted) and the Contention
/// backoff (never counted).
#[derive(Clone, Debug)]
struct Retry {
    transient: Backoff,
    contention: Backoff,
}

impl Retry {
    fn new() -> Self {
        Self {
            transient: Backoff::transient(),
            contention: Backoff::contention(),
        }
    }

    fn success(&mut self, now: Instant) {
        self.transient.success(now);
        self.contention.success(now);
    }

    fn verdict(&mut self, error: &ErrorWire, now: Instant, policy: &Policy) -> Verdict {
        // A peer's hint lengthens the delay, within bounds: holds other than
        // Due, Idle and Waiting ignore triggers until they expire.
        let asked = Duration::from_millis(error.retry_after_ms.unwrap_or(0)).min(policy.max_asked);
        match failure_kind(error) {
            FailureKind::Contention => Verdict::Contention(self.contention.failure(now).max(asked)),
            FailureKind::Waiting => Verdict::Waiting(policy.waiting.max(asked)),
            FailureKind::Transient => {
                self.contention.reset();
                Verdict::Failure(self.transient.failure(now).max(asked))
            }
            FailureKind::NeedsUserAction => Verdict::Parked(policy.user_action.max(asked)),
            FailureKind::Incompatible => Verdict::Parked(policy.incompatible.max(asked)),
            FailureKind::Fatal => Verdict::Parked(policy.fatal.max(asked)),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Hold {
    Due,
    Idle,
    Contention,
    Backoff,
    Waiting,
    Parked,
}

struct Key {
    target: Target,
    signals: Signals,
    order: u64,
    due: Instant,
    hold: Hold,
    running: bool,
    trigger_pending: bool,
    reset_pending: bool,
    retry: Retry,
    failures: u32,
    contention: u32,
    quick: u32,
    exchanges: u64,
    succeeded: u64,
    last_error: Option<ErrorWire>,
}

impl Key {
    fn new(target: Target, signals: Signals, order: u64, now: Instant) -> Self {
        Self {
            target,
            signals,
            order,
            due: now,
            hold: Hold::Due,
            running: false,
            trigger_pending: false,
            reset_pending: false,
            retry: Retry::new(),
            failures: 0,
            contention: 0,
            quick: 0,
            exchanges: 0,
            succeeded: 0,
            last_error: None,
        }
    }

    fn ready(&self) -> bool {
        self.signals.readiness == Readiness::Ready
    }

    fn trigger(&mut self, now: Instant) {
        if self.running {
            self.trigger_pending = true;
        } else if matches!(self.hold, Hold::Due | Hold::Idle | Hold::Waiting) {
            self.due = self.due.min(now);
            self.hold = Hold::Due;
        }
    }

    fn reset(&mut self, now: Instant) {
        self.retry = Retry::new();
        self.failures = 0;
        self.contention = 0;
        self.quick = 0;
        self.due = now;
        self.hold = Hold::Due;
        if self.running {
            self.reset_pending = true;
        }
    }

    fn view(&self, now: Instant) -> KeyView {
        let phase = if self.running {
            KeyPhase::Running
        } else if !self.ready() {
            KeyPhase::NotReady
        } else if self.due <= now {
            KeyPhase::Due
        } else {
            match self.hold {
                Hold::Due => KeyPhase::Due,
                Hold::Idle => KeyPhase::Idle,
                Hold::Contention => KeyPhase::Contention,
                Hold::Backoff => KeyPhase::Backoff,
                Hold::Waiting => KeyPhase::Waiting,
                Hold::Parked => KeyPhase::Parked,
            }
        };
        KeyView {
            repo: self.target.repo.clone(),
            readiness: self.signals.readiness,
            phase,
            failures: self.failures,
            contention: self.contention,
            exchanges: self.exchanges,
            succeeded: self.succeeded,
            due_in_ms: (!self.running && self.ready())
                .then(|| ms(self.due.saturating_duration_since(now))),
            last_error: self.last_error.clone(),
        }
    }
}

enum Wake {
    Done(Box<Done>),
    Changed,
    Stop,
}

struct Done {
    repo: String,
    /// The incarnation of the key that ran (`Key::order`).
    order: u64,
    result: Result<Next, ExchangeError>,
    retry: Retry,
    verdict: Option<Verdict>,
}

/// The loop state of one scheduler.
struct Engine {
    ctx: Arc<Ctx>,
    local: String,
    peer: String,
    scope: String,
    class: &'static str,
    lane: u32,
    op: String,
    options: DriveOptions,
    policy: Policy,
    keys: BTreeMap<String, Key>,
    order: u64,
    in_flight: usize,
    peak: usize,
    started: u64,
    finished: u64,
    session_gone: bool,
    last_view: Option<SchedulerView>,
}

impl Engine {
    /// Re-reads the remote catalog and the local folders: creates, updates
    /// (triggers, resets) and drops keys.
    fn refresh(&mut self, ctx: &Ctx, peer: &PeerContext, job: &dyn Job, now: Instant) {
        let catalog = peer.catalog.load();
        let mut seen = HashSet::new();
        for remote in &catalog.entries {
            if !seen.insert(remote.repo.clone()) {
                continue;
            }
            let Some(folder) = ctx.folders.by_repo(&remote.repo) else {
                seen.remove(&remote.repo);
                continue;
            };
            let signals = Signals::of(&folder, remote);
            let target = Target {
                repo: remote.repo.clone(),
                folder,
                remote: remote.clone(),
            };
            let activity_key = self.activity_key(&remote.repo);
            let absorb = self.options.absorb;
            match self.keys.get_mut(&remote.repo) {
                None => {
                    // A new key (or one removed and back) starts afresh, even
                    // if an exchange of its previous incarnation still runs.
                    job.forget(&remote.repo);
                    self.order += 1;
                    self.keys.insert(
                        remote.repo.clone(),
                        Key::new(target, signals, self.order, now),
                    );
                }
                Some(key) => {
                    let before = std::mem::replace(&mut key.signals, signals);
                    key.target = target;
                    let after = &key.signals;
                    if after.readiness != Readiness::Ready {
                        continue;
                    }
                    if before.readiness != Readiness::Ready
                        || before.instance != after.instance
                        || before.remote_replica != after.remote_replica
                    {
                        key.reset(now);
                        job.forget(&remote.repo);
                    } else {
                        let absorbing = absorbing(&activity_key, now, absorb);
                        if job.triggered(&before, after, absorbing) {
                            key.trigger(now);
                        }
                    }
                }
            }
        }
        let gone: Vec<String> = self
            .keys
            .keys()
            .filter(|repo| !seen.contains(*repo))
            .cloned()
            .collect();
        for repo in gone {
            self.keys.remove(&repo);
            job.forget(&repo);
        }
    }

    /// Starts due keys while slots are free, oldest due first.
    fn dispatch(
        &mut self,
        job: &Arc<dyn Job>,
        pool: &mut Pool,
        tx: &mpsc::Sender<Wake>,
        now: Instant,
    ) {
        let free = self.options.slots.max(1).saturating_sub(self.in_flight);
        if free == 0 {
            return;
        }
        let mut due: Vec<(Instant, u64, String)> = self
            .keys
            .iter()
            .filter(|(_, key)| !key.running && key.ready() && key.due <= now)
            .map(|(repo, key)| (key.due, key.order, repo.clone()))
            .collect();
        due.sort();
        let mut started = 0;
        for (_, _, repo) in due {
            if started == free {
                break;
            }
            let Some(claim) = Claim::take(self.claim_key(&repo)) else {
                if let Some(key) = self.keys.get_mut(&repo) {
                    key.due = now + CLAIM_RETRY;
                }
                continue;
            };
            let activity_key = self.activity_key(&repo);
            let Some(key) = self.keys.get_mut(&repo) else {
                continue;
            };
            let activity = ActivityGuard::begin(activity_key);
            let order = key.order;
            let target = key.target.clone();
            let retry = key.retry.clone();
            let policy = self.policy;
            let task_job = job.clone();
            let task_tx = tx.clone();
            let ctx_report = ReportCtx {
                ctx: self.ctx.clone(),
                installation: self.peer.clone(),
                lane: self.lane,
                op: self.op.clone(),
                started: Instant::now(),
            };
            let task: Task = Box::new(move || {
                let mut retry = retry;
                let mut verdict = None;
                let result = catch_unwind(AssertUnwindSafe(|| {
                    task_job.run(&target, &mut |failure: &ExchangeError| {
                        if failure.session_lost() {
                            return None;
                        }
                        let decided = retry.verdict(&failure.wire(), Instant::now(), &policy);
                        verdict = Some(decided);
                        Some(decided.delay())
                    })
                }));
                let result = match result {
                    Ok(result) => result,
                    Err(_) => {
                        let failure = ExchangeError::local(CallError::Remote(ErrorWire::coded(
                            "UNKNOWN_ERROR",
                            format!("the {} exchange of {} panicked", ctx_report.op, target.repo),
                        )));
                        ctx_report.panicked(&target.repo, &failure);
                        Err(failure)
                    }
                };
                drop(activity);
                drop(claim);
                let _ = task_tx.send(Wake::Done(Box::new(Done {
                    repo: target.repo.clone(),
                    order,
                    result,
                    retry,
                    verdict,
                })));
            });
            if !pool.submit(task) {
                key.due = now + MAX_WAIT;
                continue;
            }
            key.running = true;
            key.exchanges += 1;
            started += 1;
            self.in_flight += 1;
            self.peak = self.peak.max(self.in_flight);
            self.started += 1;
        }
    }

    fn finish(&mut self, done: Done, now: Instant) {
        self.in_flight = self.in_flight.saturating_sub(1);
        self.finished += 1;
        if done.result.as_ref().is_err_and(ExchangeError::session_lost) {
            // Whichever key it was: no request can go out on this session.
            self.session_gone = true;
        }
        // An exchange of a key that went away (or of an earlier incarnation of
        // a key that came back) has nothing left to update.
        let Some(key) = self
            .keys
            .get_mut(&done.repo)
            .filter(|key| key.order == done.order)
        else {
            return;
        };
        key.running = false;
        let reset = std::mem::take(&mut key.reset_pending);
        let trigger = std::mem::take(&mut key.trigger_pending);
        match done.result {
            Ok(next) => {
                key.retry.success(now);
                key.failures = 0;
                key.contention = 0;
                key.last_error = None;
                key.succeeded += 1;
                let quick = match next.after {
                    Some(after) if next.progress => {
                        key.quick = 0;
                        Some(after)
                    }
                    Some(after) if key.quick < QUICK_LIMIT => {
                        key.quick += 1;
                        Some(after)
                    }
                    _ => {
                        key.quick = 0;
                        None
                    }
                };
                if reset || trigger {
                    key.due = now;
                    key.hold = Hold::Due;
                } else if let Some(after) = quick {
                    key.due = now + after;
                    key.hold = Hold::Due;
                } else {
                    key.due = now + self.options.interval;
                    key.hold = Hold::Idle;
                }
            }
            Err(failure) => {
                let error = failure.wire();
                key.last_error = Some(error.clone());
                if failure.session_lost() {
                    // Not the key's failure; the next session starts afresh.
                    key.due = now;
                    key.hold = Hold::Due;
                    return;
                }
                if reset {
                    key.due = now;
                    key.hold = Hold::Due;
                    return;
                }
                key.retry = done.retry;
                let verdict = done
                    .verdict
                    .unwrap_or_else(|| key.retry.verdict(&error, now, &self.policy));
                let (hold, delay) = match verdict {
                    Verdict::Contention(delay) => {
                        key.contention = key.contention.saturating_add(1);
                        (Hold::Contention, delay)
                    }
                    Verdict::Waiting(delay) => (Hold::Waiting, delay),
                    Verdict::Failure(delay) => {
                        key.failures = key.failures.saturating_add(1);
                        key.contention = 0;
                        (Hold::Backoff, delay)
                    }
                    Verdict::Parked(delay) => {
                        key.failures = key.failures.saturating_add(1);
                        key.contention = 0;
                        (Hold::Parked, delay)
                    }
                };
                event!(debug, (error.code.as_str()), {
                    "peer": self.peer,
                    "repo": done.repo,
                    "op": self.op,
                    "lane": self.lane,
                    "retry_ms": ms(delay),
                    "failures": key.failures,
                    "message": error.message,
                });
                key.hold = hold;
                key.due = now + delay;
                if trigger && hold == Hold::Waiting {
                    key.due = now;
                    key.hold = Hold::Due;
                }
            }
        }
    }

    /// How long the loop may sleep before a key is due.
    fn next_wake(&self, now: Instant) -> Duration {
        if self.in_flight >= self.options.slots.max(1) {
            return MAX_WAIT;
        }
        self.keys
            .values()
            .filter(|key| !key.running && key.ready())
            .map(|key| key.due.saturating_duration_since(now))
            .min()
            .unwrap_or(MAX_WAIT)
            .min(MAX_WAIT)
    }

    fn publish(&mut self, now: Instant, stopped: bool) {
        let Some(target) = &self.options.view else {
            return;
        };
        let view = SchedulerView {
            op: self.op.clone(),
            lane: self.lane,
            slots: self.options.slots,
            in_flight: self.in_flight,
            peak_in_flight: self.peak,
            started: self.started,
            finished: self.finished,
            keys: self.keys.values().map(|key| key.view(now)).collect(),
            stopped,
        };
        // Skip views where only the countdowns moved.
        let same = self.last_view.as_ref().is_some_and(|last| {
            let countdown_free = |view: &SchedulerView| SchedulerView {
                keys: view
                    .keys
                    .iter()
                    .map(|key| KeyView {
                        due_in_ms: None,
                        ..key.clone()
                    })
                    .collect(),
                ..view.clone()
            };
            countdown_free(last) == countdown_free(&view)
        });
        if !same {
            target.store(view.clone());
            self.last_view = Some(view);
        }
    }

    fn claim_key(&self, repo: &str) -> String {
        format!(
            "{}\u{1f}{}\u{1f}{}\u{1f}{repo}",
            self.local, self.peer, self.scope
        )
    }

    fn activity_key(&self, repo: &str) -> String {
        format!("{}\u{1f}{}\u{1f}{repo}", self.local, self.class)
    }
}

/// What a slot needs to report an exchange that panicked (so the hub does
/// not keep it in flight).
struct ReportCtx {
    ctx: Arc<Ctx>,
    installation: String,
    lane: u32,
    op: String,
    started: Instant,
}

impl ReportCtx {
    fn panicked(&self, repo: &str, failure: &ExchangeError) {
        event!(error, UNKNOWN_ERROR, {
            "peer": self.installation,
            "repo": repo,
            "op": self.op,
            "message": failure.to_string(),
        });
        self.ctx.status.record(StatusEvent::ExchangeFinished {
            installation: self.installation.clone(),
            repo: repo.to_owned(),
            lane: self.lane,
            op: self.op.clone(),
            direction: Direction::Outbound,
            ok: false,
            error: Some(failure.wire()),
            summary: None,
            bytes_up: 0,
            bytes_down: 0,
            objects_up: 0,
            objects_down: 0,
            duration_ms: ms(self.started.elapsed()),
        });
    }
}

/// Runs the scheduling loop of one lane until `cancel` fires, the session
/// closes, or a call finds the session lost or going away.
#[allow(clippy::too_many_arguments)]
pub(crate) fn drive(
    ctx: &Arc<Ctx>,
    peer: &Arc<PeerContext>,
    conn: &Conn,
    cancel: &Cancel,
    job: Arc<dyn Job>,
    options: &DriveOptions,
    lane: u32,
    name: &str,
) {
    let (tx, rx) = mpsc::channel::<Wake>();
    // Woken on cancel through a child token, so the hook goes away with it.
    let token = cancel.child();
    {
        let tx = Mutex::new(tx.clone());
        token.on_cancel(Box::new(move || {
            let _ = lock(&tx).send(Wake::Stop);
        }));
    }
    let watching = Arc::new(AtomicBool::new(true));
    let watcher = spawn_watcher(ctx, peer, conn, &watching, &tx, name);
    let mut pool = Pool::new(options.slots.max(1), name);
    let mut engine = Engine {
        ctx: ctx.clone(),
        local: ctx.installation.clone(),
        peer: peer.installation.clone(),
        scope: job.scope(),
        class: job.class(),
        lane,
        op: job.op().to_owned(),
        options: options.clone(),
        policy: options.policy(),
        keys: BTreeMap::new(),
        order: 0,
        in_flight: 0,
        peak: 0,
        started: 0,
        finished: 0,
        session_gone: false,
        last_view: None,
    };
    'run: loop {
        while let Ok(wake) = rx.try_recv() {
            match wake {
                Wake::Done(done) => engine.finish(*done, Instant::now()),
                Wake::Changed => {}
                Wake::Stop => break 'run,
            }
        }
        if cancel.is_cancelled() || conn.closed().is_some() || engine.session_gone {
            break;
        }
        let now = Instant::now();
        engine.refresh(ctx, peer, &*job, now);
        let stopping = ctx.stopping.load(Ordering::Relaxed);
        if !stopping {
            engine.dispatch(&job, &mut pool, &tx, now);
        }
        engine.publish(now, false);
        // While the daemon stops nothing is dispatched, so a due key must not
        // shorten the wait (it would spin): wait for a wake-up or `MAX_WAIT`.
        let wait = if stopping {
            MAX_WAIT
        } else {
            engine.next_wake(Instant::now())
        };
        match rx.recv_timeout(wait) {
            Ok(Wake::Done(done)) => engine.finish(*done, Instant::now()),
            Ok(Wake::Changed) | Err(mpsc::RecvTimeoutError::Timeout) => {}
            Ok(Wake::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    // Stop: no new exchanges; wait a bounded time for those in flight (a
    // closed session fails them at once), then leave the rest to end on
    // their own (they release their claims themselves).
    watching.store(false, Ordering::Release);
    let stop_at = Instant::now();
    let until = stop_at + options.exit_wait;
    while engine.in_flight > 0 {
        let left = until.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break;
        }
        match rx.recv_timeout(left) {
            Ok(Wake::Done(done)) => engine.finish(*done, Instant::now()),
            Ok(_) => {}
            Err(_) => break,
        }
    }
    pool.stop(until.max(Instant::now() + Duration::from_millis(50)));
    if let Some(watcher) = watcher {
        // It ends within one slice of `watching` going false; this wait
        // overlaps the exit wait above.
        let until = stop_at + WATCH_SLICE * 2;
        while !watcher.is_finished() && Instant::now() < until {
            thread::sleep(Duration::from_millis(5));
        }
        if watcher.is_finished() {
            let _ = watcher.join();
        }
    }
    engine.publish(Instant::now(), true);
    drop(token);
}

/// Wakes the loop when a folder view or the remote catalog changes, and stops
/// it when the session closes.
fn spawn_watcher(
    ctx: &Arc<Ctx>,
    peer: &Arc<PeerContext>,
    conn: &Conn,
    watching: &Arc<AtomicBool>,
    tx: &mpsc::Sender<Wake>,
    name: &str,
) -> Option<thread::JoinHandle<()>> {
    let (ctx, peer, conn, watching, tx) = (
        ctx.clone(),
        peer.clone(),
        conn.clone(),
        watching.clone(),
        tx.clone(),
    );
    thread::Builder::new()
        .name(format!("{name}-watch"))
        .spawn(move || {
            let mut folders = ctx.folders.generation();
            let mut catalog = peer.catalog.generation();
            while watching.load(Ordering::Acquire) {
                let began = Instant::now();
                let next = ctx.folders.wait_newer(folders, WATCH_SLICE);
                let mut changed = next != folders;
                folders = next;
                let generation = peer.catalog.generation();
                if generation != catalog {
                    catalog = generation;
                    changed = true;
                }
                if conn.closed().is_some() {
                    let _ = tx.send(Wake::Stop);
                    return;
                }
                if changed {
                    if tx.send(Wake::Changed).is_err() {
                        return;
                    }
                } else if let Some(rest) = WATCH_SLICE.checked_sub(began.elapsed()) {
                    // A directory whose wait returned early without a change.
                    thread::sleep(rest);
                }
            }
        })
        .ok()
}

type Task = Box<dyn FnOnce() + Send>;

/// The slot threads of one scheduler, started on demand (at most `max`).
///
/// A submitted task never waits behind another one (RC5): the pool counts
/// its outstanding tasks (submitted and not yet finished, queued or running)
/// and starts a thread whenever it has fewer threads than outstanding tasks.
/// The engine never has more than `max` exchanges in flight, so every queued
/// task has a thread that is not running another task. (Spawning by the
/// count of idle threads is racy: a thread counts as idle until it has
/// dequeued, so several submits in one turn could all see one idle thread,
/// start none, and queue behind whichever task it took, a hung one too.)
/// A task counts as finished only after it has returned, so a submit that
/// races a task's end may start a thread more than needed, never fewer.
struct Pool {
    tx: Option<mpsc::Sender<Task>>,
    rx: Arc<Mutex<mpsc::Receiver<Task>>>,
    outstanding: Arc<AtomicUsize>,
    threads: Vec<thread::JoinHandle<()>>,
    spawned: usize,
    max: usize,
    name: String,
}

impl Pool {
    fn new(max: usize, name: &str) -> Self {
        let (tx, rx) = mpsc::channel::<Task>();
        Self {
            tx: Some(tx),
            rx: Arc::new(Mutex::new(rx)),
            outstanding: Arc::new(AtomicUsize::new(0)),
            threads: Vec::new(),
            spawned: 0,
            max: max.max(1),
            name: name.to_owned(),
        }
    }

    /// Hands `task` to a slot thread; `false` (nothing runs) if no thread is
    /// free for it and none can be started.
    fn submit(&mut self, task: Task) -> bool {
        let Some(tx) = self.tx.as_ref() else {
            return false;
        };
        // A slot thread ends only with the pool; one that ended anyway no
        // longer serves.
        self.threads.retain(|handle| !handle.is_finished());
        let outstanding = self.outstanding.fetch_add(1, Ordering::AcqRel) + 1;
        if self.threads.len() < outstanding.min(self.max) {
            let (rx, count) = (self.rx.clone(), self.outstanding.clone());
            let spawned = thread::Builder::new()
                .name(format!("{}-s{}", self.name, self.spawned))
                .spawn(move || {
                    loop {
                        let task = lock(&rx).recv();
                        let Ok(task) = task else {
                            return;
                        };
                        // A task reports its own panics; this only keeps the
                        // thread and the count right.
                        let _ = catch_unwind(AssertUnwindSafe(task));
                        count.fetch_sub(1, Ordering::AcqRel);
                    }
                });
            match spawned {
                Ok(handle) => {
                    self.spawned += 1;
                    self.threads.push(handle);
                }
                Err(_) => {
                    // Queued, the task would wait behind a running one.
                    self.outstanding.fetch_sub(1, Ordering::AcqRel);
                    return false;
                }
            }
        }
        if tx.send(task).is_ok() {
            true
        } else {
            self.outstanding.fetch_sub(1, Ordering::AcqRel);
            false
        }
    }

    /// Ends the slot threads: joins those that finish by `until`, leaves the
    /// others (an exchange that outlives the wait) to end on their own.
    fn stop(mut self, until: Instant) {
        self.tx = None;
        for handle in self.threads.drain(..) {
            while !handle.is_finished() && Instant::now() < until {
                thread::sleep(Duration::from_millis(5));
            }
            if handle.is_finished() {
                let _ = handle.join();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Process-wide registries

/// One exchange per (local installation, peer, op scope, repo) at a time in
/// this process, also across scheduler instances (a respawned lane).
struct Claim(String);

impl Claim {
    /// `None` while another exchange holds `key`. (The guard is released
    /// before a `Claim` exists: dropping one locks the registry.)
    fn take(key: String) -> Option<Claim> {
        let taken = lock(claims()).insert(key.clone());
        taken.then(|| Claim(key))
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        lock(claims()).remove(&self.0);
    }
}

fn claims() -> &'static Mutex<HashSet<String>> {
    static CLAIMS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    CLAIMS.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Local exchanges per (local installation, class, repo), across peers: a
/// generation bump while one runs, or within `absorb` after one ended, is
/// that exchange's own echo.
#[derive(Clone, Copy, Debug, Default)]
struct Activity {
    running: u32,
    last_end: Option<Instant>,
}

fn activities() -> &'static Mutex<HashMap<String, Activity>> {
    static ACTIVITIES: OnceLock<Mutex<HashMap<String, Activity>>> = OnceLock::new();
    ACTIVITIES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn absorbing(key: &str, now: Instant, window: Duration) -> bool {
    lock(activities()).get(key).is_some_and(|activity| {
        activity.running > 0
            || activity
                .last_end
                .is_some_and(|end| now.saturating_duration_since(end) < window)
    })
}

struct ActivityGuard(String);

impl ActivityGuard {
    fn begin(key: String) -> Self {
        let mut map = lock(activities());
        if map.len() >= ACTIVITY_PRUNE {
            let now = Instant::now();
            map.retain(|_, activity| {
                activity.running > 0
                    || activity
                        .last_end
                        .is_some_and(|end| now.saturating_duration_since(end) < ABSORB * 30)
            });
        }
        map.entry(key.clone()).or_default().running += 1;
        Self(key)
    }
}

impl Drop for ActivityGuard {
    fn drop(&mut self) {
        let mut map = lock(activities());
        let activity = map.entry(self.0.clone()).or_default();
        activity.running = activity.running.saturating_sub(1);
        activity.last_end = Some(Instant::now());
    }
}
