//! The orchestrator's sync driver (spec §7.3–§7.5): the metadata scheduler,
//! the object pump and the peer clock probe. It only originates requests on
//! this side's outbound lanes ([`Lanes`], the [`Network`]); serving peers is
//! `PeerService`'s.
//!
//! Threads (all end once the driver's [`Cancel`] fires; [`Driver::stop`]):
//! * **scheduler** — looks at each paired peer with a Ready control lane × each
//!   Running, unpaused folder when poked and every [`DUE_CHECK`] at most, and
//!   dispatches an exchange for a (peer, repo) that is due, to a pool of
//!   [`MAX_EXCHANGES`] exchange threads (at most that many in flight overall).
//!   A pair is due when it is marked (a local change, a peer's metadata notify),
//!   when its control session or worker is new, or when its poll elapsed:
//!   `backup_poll_ms` on a session that gets notified, else `sync_interval_ms`.
//!   A failed exchange polls again within the shorter of the two. An exchange is
//!   `feed.outgoing {after: sent}` on the worker → `meta.exchange {repo}`
//!   (`FeedRequest {after: received, events}`) on the peer's control lane →
//!   `feed.apply` of the reply's events on the worker; then both cursors
//!   advance, and it repeats at once while either side has `more`. Cursors
//!   live in memory per (peer, repo) ([`SyncJob`]) and start over when the
//!   folder's worker or the peer's control session changes.
//! * **object pump** — one thread for every Running, unpaused folder. Each
//!   round has two passes (spec §7.8: demand never waits behind background
//!   traffic). Pass 1 supplies every folder's pending `watch` demand; pass 2
//!   prefetches `objects.missing {limit: OBJECT_BATCH, exclude, after}` per
//!   folder — a resumable scan: a page not wholly stored moves the folder's
//!   cursor past it, so every missing object is eventually listed — and is
//!   abandoned before its next call as soon as a demand wake-up (the
//!   wake [`Seq`] the folders' watch threads bump) arrives, so the next
//!   round's pass 1 waits at most for one call in flight (≤
//!   [`FETCH_DEADLINE_MAX`]). Objects are asked of each peer in turn, per
//!   owning Ready bulk lane: manifests (size unknown) in batches of ≤
//!   [`MANIFEST_BATCH`], then chunks in batches of ≤ [`OBJECT_BATCH`], each
//!   within a deadline that grows with what it asks ([`fetch_deadline`]),
//!   and written to the worker. A fetch that timed out halves that folder's
//!   batch for that peer (down to 1); each answered one grows it back by a
//!   quarter. Of a reply the peer cut to its byte bounds (`truncated_at`),
//!   the rest is asked again at once. A digest every asked
//!   peer omitted is excluded for a backoff (5 s doubling to 5 min), which
//!   spares re-asking it when the scan comes around again. A round that stored nothing sleeps
//!   `sync_interval_ms` unless woken. A peer whose fetch failed sits out the
//!   next round. The pump never blocks the scheduler. After each pass it also
//!   pushes ([`push`]), so a round runs: this side's demand, the listeners'
//!   demand, background pull, background push. The background push stops
//!   before its next call on a demand wake-up, as pass 2 does. The push steps
//!   run only for a pair that is due, so an idle pair asks its listener nothing:
//!   the demand step on a peer's demand notify, the background step on a local
//!   change or an exchange that moved events, and both on the pair's poll. A
//!   background step stays due until a page comes back empty.
//! * **clock probes** — a short thread per probe, at most one per peer: at
//!   every new control session (a Ready transition) and every
//!   [`CLOCK_PROBE_EVERY`] scheduler rounds. A round is one `sync_interval_ms`,
//!   however often the scheduler is poked. The smoothed offset goes to the
//!   catalog (`Catalog::set_clock`); one past `WARN_OFFSET_MS` logs
//!   `CLOCK_OFFSET_LARGE`.
use super::{
    catalog::Catalog,
    config::SyncProfile,
    demand::Seq,
    network::{Network, NotifySink, ReadyPeer},
    push::{self, PushMode, PushState},
    sync::{FeedCursor, FeedPage, FeedReply, FeedRequest, Fetched, MAX_EXCLUDE},
    worker_client::{CALL_DEADLINE, WorkerLink},
};
use crate::{
    clock::{self, Offset, Sample, WARN_OFFSET_MS},
    content::Kind,
    model::{
        content::Digest,
        orchestrator::CallError,
        protocol::frame::{Notify, NotifyReason},
    },
    session::{Conn, Priority},
    util::{cancel::Cancel, fault, lock::lock, log::event, time::now_ms},
};
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver, Sender},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// Exchanges in flight at once, over every (peer, repo).
pub const MAX_EXCHANGES: usize = 4;
/// Objects per `objects.fetch` / `objects.write` (the worker refuses more).
pub const OBJECT_BATCH: usize = super::sync::OBJECT_BATCH;
/// A peer's clock is probed again every this many scheduler rounds.
pub const CLOCK_PROBE_EVERY: u64 = 60;
/// Deadline of one request on a peer lane (an `objects.fetch` gets more:
/// [`fetch_deadline`]).
pub const PEER_DEADLINE: Duration = Duration::from_secs(10);
/// Objects of unknown size (manifests) per `objects.fetch`.
pub const MANIFEST_BATCH: usize = 4;
/// The longest `objects.fetch` deadline.
pub const FETCH_DEADLINE_MAX: Duration = Duration::from_secs(120);
/// What a fetch deadline counts for a requested chunk (a full one)…
const CHUNK_GUESS: u64 = 64 * 1024;
/// …and for an object of unknown size (a manifest).
const UNKNOWN_GUESS: u64 = 1024 * 1024;
/// A fetch deadline grows by 1 s per this many requested bytes.
const DEADLINE_STEP_BYTES: u64 = 256 * 1024;
/// A digest no asked peer had is excluded this long at first…
pub const UNAVAILABLE_FIRST: Duration = Duration::from_secs(5);
/// …doubling each time to at most this.
pub const UNAVAILABLE_MAX: Duration = Duration::from_secs(300);
/// The scheduler looks for what came due at least this often, and so does the
/// pump while a push step stays due: a new control session and a failed step
/// send no poke.
const DUE_CHECK: Duration = Duration::from_millis(500);
/// Most marks kept per kind. A mark names a repo a paired peer chose, so it is
/// bounded; past this one is not kept, and the pair's poll covers that.
const MAX_MARKS: usize = 4096;
/// Fault point: the background push steps do not run, so a test can hold bytes
/// on this side until a reader on the other side asks for them.
pub const FAULT_BACKGROUND_SKIP: &str = "sync.push.background.skip";
/// A demanded digest just written is not fetched again for this long (the
/// worker's demand list catches up at its next `watch`).
const DEMAND_SUPPLIED: Duration = super::worker_client::WATCH_DEADLINE;

pub(super) type Wanted = Vec<(Digest, Kind)>;

/// How this orchestrator keeps its folders in sync.
#[derive(Clone, Copy, Debug)]
pub struct SyncPolicy {
    pub profile: SyncProfile,
    /// The poll of a session that gets no notify, and the length of a round.
    pub interval: Duration,
    /// The poll of a session that gets notified.
    pub backup_poll: Duration,
}

impl SyncPolicy {
    /// The poll of a session.
    fn poll(&self, notify: bool) -> Duration {
        if notify {
            self.backup_poll
        } else {
            self.interval
        }
    }

    /// The shorter poll: nothing comes due by time sooner than this.
    fn shortest(&self) -> Duration {
        self.interval.min(self.backup_poll)
    }
}

/// (endpoint, repo).
type Pair = (String, String);

/// What is due before its poll says so.
#[derive(Default)]
struct Due {
    exchange: BTreeSet<Pair>,
    /// The demand push step.
    demand: BTreeSet<Pair>,
    /// The background push step.
    background: BTreeSet<Pair>,
}

fn mark(marks: &mut BTreeSet<Pair>, pair: Pair) {
    if marks.len() < MAX_MARKS {
        marks.insert(pair);
    }
}

/// The peer lanes the driver uses: the [`Network`] (tests stub it).
pub trait Lanes: Send + Sync {
    /// Every peer with a Ready control lane.
    fn ready_peers(&self) -> Vec<ReadyPeer>;
    /// The endpoints with at least one Ready bulk lane.
    fn bulk_endpoints(&self) -> Vec<String>;
    /// The Ready bulk lane to `endpoint` that owns `digest`.
    fn bulk_lane(&self, endpoint: &str, digest: &Digest) -> Option<Conn>;
    /// Any Ready bulk lane to `endpoint`.
    fn any_bulk_lane(&self, endpoint: &str) -> Option<Conn>;
}

impl Lanes for Network {
    fn ready_peers(&self) -> Vec<ReadyPeer> {
        Network::ready_peers(self)
    }
    fn bulk_endpoints(&self) -> Vec<String> {
        Network::bulk_endpoints(self)
    }
    fn bulk_lane(&self, endpoint: &str, digest: &Digest) -> Option<Conn> {
        Network::bulk_lane(self, endpoint, digest)
    }
    fn any_bulk_lane(&self, endpoint: &str) -> Option<Conn> {
        Network::any_bulk_lane(self, endpoint)
    }
}

/// A folder the driver syncs: Running, unpaused, with its worker link.
pub struct SyncFolder {
    pub id: String,
    pub repo: String,
    pub link: Arc<WorkerLink>,
    /// The objects the worker's readers wait for (its last `watch`).
    pub demand: Vec<(Digest, Kind)>,
}

/// The folders to sync (implemented over the daemon's folder slots).
pub trait SyncFolders: Send + Sync {
    /// Every Running, unpaused folder with a worker link.
    fn syncing(&self) -> Vec<SyncFolder>;
}

/// The metadata exchange state of one (peer, repo).
pub struct SyncJob {
    /// The peer's endpoint.
    pub peer: String,
    pub repo: String,
    pub cursor: FeedCursor,
    pub last_exchange_ms: u64,
    pub error: Option<String>,
}

/// A job with what its cursors belong to.
struct Slot {
    job: SyncJob,
    /// The worker link the cursors were taken on.
    link: Weak<WorkerLink>,
    /// The peer control session the cursors were taken on.
    session: String,
    running: bool,
    /// When its poll comes due.
    next_poll: Option<Instant>,
}

/// One dispatched exchange.
struct Task {
    key: (String, String),
    link: Arc<WorkerLink>,
    conn: Conn,
    session: String,
    cursor: FeedCursor,
}

#[derive(Default)]
struct ProbeState {
    /// The control session last probed.
    session: String,
    /// The scheduler round of that probe.
    round: u64,
    running: bool,
}

/// The object pump's memory across rounds.
#[derive(Default)]
struct PumpState {
    round: u64,
    /// Peers sitting out: endpoint → the first round they are asked again.
    resting: BTreeMap<String, u64>,
    /// The last error logged per peer endpoint (fetch) or folder id
    /// (`objects.missing`): logged again only when it changes.
    logged: BTreeMap<String, String>,
    /// Per folder id: the digests no asked peer had, and until when they are
    /// excluded.
    unavailable: BTreeMap<String, BTreeMap<Digest, Unavailable>>,
    /// Per folder id: where its `objects.missing` scan resumes (`None`: from
    /// the start).
    scan: BTreeMap<String, Option<String>>,
    /// (folder id, digest) written lately: not fetched again for a stale
    /// demand.
    supplied: BTreeMap<(String, Digest), Instant>,
    /// (folder id, endpoint) → the objects asked per `objects.fetch`, while
    /// below [`OBJECT_BATCH`]: halved by a timeout, grown back by replies.
    batch: BTreeMap<(String, String), usize>,
    /// The push steps' memory.
    push: PushState,
    /// When each pair's push poll comes due; a pair not here is due.
    polls: BTreeMap<Pair, Instant>,
}

/// A batch of `asked` objects timed out: half that, at least 1.
pub(super) fn halved_batch(size: usize, asked: usize) -> usize {
    (size.min(asked) / 2).max(1)
}

/// A batch was answered: a quarter more (at least 1), up to [`OBJECT_BATCH`].
pub(super) fn grown_batch(size: usize) -> usize {
    (size + (size / 4).max(1)).min(OBJECT_BATCH)
}

impl PumpState {
    /// The objects asked of `endpoint` per fetch for folder `id`.
    fn batch_size(&self, id: &str, endpoint: &str) -> usize {
        self.batch
            .get(&(id.to_owned(), endpoint.to_owned()))
            .copied()
            .unwrap_or(OBJECT_BATCH)
    }

    /// A fetch of `asked` objects timed out: half that, at least 1.
    fn shrink(&mut self, id: &str, endpoint: &str, asked: usize) {
        let size = halved_batch(self.batch_size(id, endpoint), asked);
        self.batch
            .insert((id.to_owned(), endpoint.to_owned()), size);
        event!(info, SYNC_BATCH_SHRUNK, {
            "peer": endpoint,
            "folder": id,
            "batch": size,
        });
    }

    /// A fetch was answered: a quarter more (at least 1), up to
    /// [`OBJECT_BATCH`].
    fn grow(&mut self, id: &str, endpoint: &str) {
        let key = (id.to_owned(), endpoint.to_owned());
        if let Some(size) = self.batch.get(&key).copied() {
            let size = grown_batch(size);
            if size >= OBJECT_BATCH {
                self.batch.remove(&key);
            } else {
                self.batch.insert(key, size);
            }
        }
    }
}

pub(super) struct Unavailable {
    pub(super) until: Instant,
    pub(super) backoff: Duration,
}

/// What one [`Driver::supply`] did.
#[derive(Default)]
struct Supplied {
    stored: usize,
    /// It stopped early (a wake-up, cancel or a failed write).
    interrupted: bool,
}

/// Why an exchange or a fetch did not complete.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Failure {
    /// The peer lane went away (closed, GOAWAY, cancelled): not a repo error.
    LaneLost,
    Error(String),
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Failure::LaneLost => f.write_str("LANE_LOST: the peer lane went away"),
            Failure::Error(error) => f.write_str(error),
        }
    }
}

pub(super) fn peer_failure(op: &str, error: CallError) -> Failure {
    match error {
        CallError::ConnectionLost(_) | CallError::Unavailable(_) | CallError::Cancelled => {
            Failure::LaneLost
        }
        error => Failure::Error(format!("{op}: {error}")),
    }
}

pub(super) fn worker_failure(op: &str, error: impl std::fmt::Display) -> Failure {
    Failure::Error(format!("{op}: {error}"))
}

pub struct Driver {
    lanes: Arc<dyn Lanes>,
    catalog: Arc<Catalog>,
    folders: Arc<dyn SyncFolders>,
    /// Bumped by folder watch threads when demand changes, and by whatever
    /// marks a push step due; wakes the pump.
    wake: Arc<Seq>,
    /// Bumped by what marks an exchange due; wakes the scheduler.
    poke: Arc<Seq>,
    due: Mutex<Due>,
    policy: SyncPolicy,
    cancel: Cancel,
    jobs: Mutex<BTreeMap<(String, String), Slot>>,
    busy: AtomicUsize,
    /// Shared with probe threads, which clear their `running` flag.
    probes: Arc<Mutex<BTreeMap<String, ProbeState>>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    /// The deadline of an `objects.fetch` ([`fetch_deadline`]; tests
    /// shorten it).
    fetch_deadline: fn(&[(Digest, Kind)]) -> Duration,
}

/// Starts the scheduler, its exchange pool and the pump. `cancel` (a child
/// of the daemon's stopping token) stops them.
pub fn start(
    lanes: Arc<dyn Lanes>,
    catalog: Arc<Catalog>,
    folders: Arc<dyn SyncFolders>,
    wake: Arc<Seq>,
    policy: SyncPolicy,
    cancel: Cancel,
) -> Result<Arc<Driver>> {
    let driver = Driver::with_policy(lanes, catalog, folders, wake, policy, cancel);
    let (tasks, queue) = mpsc::channel::<Task>();
    let queue = Arc::new(Mutex::new(queue));
    let mut threads = Vec::new();
    let mut spawn = |name: String, body: Box<dyn FnOnce() + Send>| -> Result<()> {
        match thread::Builder::new().name(name).spawn(body) {
            Ok(handle) => {
                threads.push(handle);
                Ok(())
            }
            Err(error) => {
                // The threads already running end with the driver.
                driver.cancel.cancel();
                Err(error).context("SYNC_THREAD_FAILED")
            }
        }
    };
    for index in 0..MAX_EXCHANGES {
        let (exchanges, queue) = (driver.clone(), queue.clone());
        spawn(
            format!("tkfs-sync-exchange-{index}"),
            Box::new(move || exchanges.exchanges(&queue)),
        )?;
    }
    let scheduler = driver.clone();
    spawn(
        "tkfs-sync-scheduler".into(),
        Box::new(move || scheduler.schedule(tasks)),
    )?;
    let pump = driver.clone();
    spawn("tkfs-sync-pump".into(), Box::new(move || pump.pump()))?;
    *lock(&driver.threads) = threads;
    Ok(driver)
}

impl Driver {
    /// An active driver with no threads yet, polling every `interval`.
    #[cfg(test)]
    fn new(
        lanes: Arc<dyn Lanes>,
        catalog: Arc<Catalog>,
        folders: Arc<dyn SyncFolders>,
        wake: Arc<Seq>,
        interval: Duration,
        cancel: Cancel,
    ) -> Arc<Self> {
        let policy = SyncPolicy {
            profile: SyncProfile::Active,
            interval,
            backup_poll: interval,
        };
        Self::with_policy(lanes, catalog, folders, wake, policy, cancel)
    }

    /// A driver with no threads yet (see [`start`]).
    fn with_policy(
        lanes: Arc<dyn Lanes>,
        catalog: Arc<Catalog>,
        folders: Arc<dyn SyncFolders>,
        wake: Arc<Seq>,
        policy: SyncPolicy,
        cancel: Cancel,
    ) -> Arc<Self> {
        Arc::new(Driver {
            lanes,
            catalog,
            folders,
            wake,
            poke: Seq::new(),
            due: Mutex::new(Due::default()),
            policy,
            cancel,
            jobs: Mutex::new(BTreeMap::new()),
            busy: AtomicUsize::new(0),
            probes: Arc::new(Mutex::new(BTreeMap::new())),
            threads: Mutex::new(Vec::new()),
            fetch_deadline,
        })
    }

    /// Stops every thread: cancels, wakes the pump and waits at most
    /// `budget` for them (a thread still in a bounded worker call is left to
    /// end on its own and reported).
    pub fn stop(&self, budget: Duration) {
        self.cancel.cancel();
        self.wake.bump();
        self.poke.bump();
        let end = Instant::now() + budget;
        let threads = std::mem::take(&mut *lock(&self.threads));
        while Instant::now() < end && !threads.iter().all(JoinHandle::is_finished) {
            thread::sleep(Duration::from_millis(10));
        }
        let left = threads.iter().filter(|t| !t.is_finished()).count();
        event!(info, SYNC_DRIVER_STOPPED, { "threads_left": left });
    }

    /// `{endpoint: {sent, received, last_exchange_ms, error}}` of `repo`.
    pub fn view(&self, repo: &str) -> Value {
        let jobs = lock(&self.jobs);
        let map: serde_json::Map<String, Value> = jobs
            .values()
            .filter(|slot| slot.job.repo == repo)
            .map(|slot| {
                let job = &slot.job;
                (
                    job.peer.clone(),
                    json!({
                        "sent": job.cursor.sent,
                        "received": job.cursor.received,
                        "last_exchange_ms": job.last_exchange_ms,
                        "error": job.error,
                    }),
                )
            })
            .collect();
        Value::Object(map)
    }

    /// `repo` changed on this side: every peer's pair of it is due.
    pub fn local_change(&self, repo: &str) {
        let peers = self.lanes.ready_peers();
        {
            let mut due = lock(&self.due);
            for peer in &peers {
                let pair = (peer.endpoint.clone(), repo.to_owned());
                mark(&mut due.exchange, pair.clone());
                mark(&mut due.background, pair);
            }
        }
        self.poke.bump();
        self.wake.bump();
    }

    // ---- scheduler -------------------------------------------------------

    fn schedule(&self, tasks: Sender<Task>) {
        let look = self.policy.shortest().min(DUE_CHECK);
        let (mut round, mut turn): (u64, u64) = (1, 0);
        let mut next_round = Instant::now() + self.policy.interval;
        while !self.cancel.is_cancelled() {
            let seen = self.poke.current();
            let now = Instant::now();
            if now >= next_round {
                round += 1;
                next_round = now + self.policy.interval;
            }
            turn += 1;
            let peers = self.lanes.ready_peers();
            self.probe_clocks(round, &peers);
            self.dispatch(turn, &peers, &tasks);
            self.poke.wait_past(seen, look);
        }
        // Dropping `tasks` ends the exchange threads once they are idle.
    }

    /// Dispatches an exchange for every idle (peer, repo) that is due, within
    /// [`MAX_EXCHANGES`]; the pairs are rotated each `turn` so none starves.
    fn dispatch(&self, turn: u64, peers: &[ReadyPeer], tasks: &Sender<Task>) {
        let folders = self.folders.syncing();
        let mut pairs: Vec<(&ReadyPeer, &SyncFolder)> = peers
            .iter()
            .flat_map(|peer| folders.iter().map(move |folder| (peer, folder)))
            .collect();
        if pairs.is_empty() {
            return;
        }
        let turn = (turn as usize) % pairs.len();
        pairs.rotate_left(turn);
        let now = Instant::now();
        let mut jobs = lock(&self.jobs);
        for (peer, folder) in pairs {
            if self.busy.load(Ordering::Acquire) >= MAX_EXCHANGES {
                return;
            }
            let key = (peer.endpoint.clone(), folder.repo.clone());
            let slot = jobs.entry(key.clone()).or_insert_with(|| Slot {
                job: SyncJob {
                    peer: peer.endpoint.clone(),
                    repo: folder.repo.clone(),
                    cursor: FeedCursor::default(),
                    last_exchange_ms: 0,
                    error: None,
                },
                link: Weak::new(),
                session: String::new(),
                running: false,
                next_poll: None,
            });
            if slot.running {
                continue;
            }
            // A restarted worker or a new control session: start over.
            let new = !Weak::ptr_eq(&slot.link, &Arc::downgrade(&folder.link))
                || slot.session != peer.session;
            if new {
                slot.link = Arc::downgrade(&folder.link);
                slot.session = peer.session.clone();
                slot.job.cursor = FeedCursor::default();
            }
            let polled = slot.next_poll.is_none_or(|at| at <= now);
            let marked = lock(&self.due).exchange.remove(&key);
            if !(new || polled || marked) {
                continue;
            }
            slot.next_poll = Some(now + self.policy.poll(peer.notify));
            let task = Task {
                key,
                link: folder.link.clone(),
                conn: peer.control.clone(),
                session: peer.session.clone(),
                cursor: slot.job.cursor,
            };
            slot.running = true;
            self.busy.fetch_add(1, Ordering::AcqRel);
            if tasks.send(task).is_err() {
                slot.running = false;
                self.busy.fetch_sub(1, Ordering::AcqRel);
                return;
            }
        }
    }

    /// One exchange thread: runs tasks until the scheduler is gone.
    fn exchanges(&self, queue: &Mutex<Receiver<Task>>) {
        loop {
            let task = match lock(queue).recv() {
                Ok(task) => task,
                Err(_) => return,
            };
            let mut cursor = task.cursor;
            let result = exchange(
                &task.link,
                &task.conn,
                &task.key.1,
                &mut cursor,
                &self.cancel,
            );
            self.finish(&task, cursor, result);
        }
    }

    /// Records an exchange's outcome on its job, unless the job started over
    /// meanwhile.
    fn finish(&self, task: &Task, cursor: FeedCursor, result: Result<(), Failure>) {
        self.record_finish(task, cursor, result);
        // What came due while it ran, or waited for a free exchange.
        self.poke.bump();
    }

    fn record_finish(&self, task: &Task, cursor: FeedCursor, result: Result<(), Failure>) {
        let mut jobs = lock(&self.jobs);
        self.busy.fetch_sub(1, Ordering::AcqRel);
        let Some(slot) = jobs.get_mut(&task.key) else {
            return;
        };
        slot.running = false;
        let current =
            slot.session == task.session && Weak::ptr_eq(&slot.link, &Arc::downgrade(&task.link));
        if !current {
            return;
        }
        // Pages the peer and the worker both took stay taken.
        slot.job.cursor = cursor;
        // Events moved: the objects they name may now be wanted, either way.
        if cursor != task.cursor {
            mark(&mut lock(&self.due).background, task.key.clone());
            self.wake.bump();
        }
        // What this exchange carried is not across yet: no waiting for the
        // backup poll.
        if result.is_err() {
            let retry = Instant::now() + self.policy.shortest();
            slot.next_poll = Some(slot.next_poll.map_or(retry, |at| at.min(retry)));
        }
        match result {
            Ok(()) => {
                slot.job.last_exchange_ms = now_ms();
                slot.job.error = None;
            }
            Err(Failure::LaneLost) => {}
            Err(Failure::Error(error)) => {
                if slot.job.error.as_deref() != Some(error.as_str()) {
                    event!(warn, SYNC_EXCHANGE_FAILED, {
                        "peer": task.key.0.clone(),
                        "repo": task.key.1.clone(),
                        "error": error.clone(),
                    });
                }
                slot.job.error = Some(error);
            }
        }
    }

    // ---- clock -----------------------------------------------------------

    /// Starts a probe of every peer whose control session is new or whose
    /// last probe is [`CLOCK_PROBE_EVERY`] rounds old.
    fn probe_clocks(&self, round: u64, peers: &[ReadyPeer]) {
        let mut probes = lock(&self.probes);
        for peer in peers {
            let state = probes.entry(peer.endpoint.clone()).or_default();
            let due = state.session != peer.session
                || round.saturating_sub(state.round) >= CLOCK_PROBE_EVERY;
            if state.running || !due {
                continue;
            }
            state.session = peer.session.clone();
            state.round = round;
            state.running = true;
            let (catalog, endpoint) = (self.catalog.clone(), peer.endpoint.clone());
            let (installation, conn) = (peer.installation.clone(), peer.control.clone());
            let cancel = self.cancel.clone();
            let done = Arc::downgrade(&self.probes);
            let spawned = thread::Builder::new()
                .name("tkfs-clock-probe".into())
                .spawn(move || {
                    if let Err(error) = probe(&catalog, &installation, &conn, &cancel) {
                        event!(info, CLOCK_PROBE_FAILED, {
                            "peer": endpoint.clone(),
                            "error": format!("{error:#}"),
                        });
                    }
                    if let Some(done) = done.upgrade()
                        && let Some(state) = lock(&done).get_mut(&endpoint)
                    {
                        state.running = false;
                    }
                });
            if spawned.is_err() {
                state.running = false;
            }
        }
    }

    // ---- pump ------------------------------------------------------------

    fn pump(&self) {
        let mut state = PumpState::default();
        while !self.cancel.is_cancelled() {
            let seen = self.wake.current();
            let stored = self.pump_round(&mut state, seen);
            if self.cancel.is_cancelled() {
                break;
            }
            if stored == 0 {
                // Returns at once when a demand wake-up came meanwhile.
                self.wake.wait_past(seen, self.pump_wait());
            }
        }
    }

    /// How long a round that stored nothing rests: until a poll may come due,
    /// or [`DUE_CHECK`] while a push step of a syncing folder stays due. Marks
    /// of a repo not synced here wait for it and do not keep the pump awake.
    fn pump_wait(&self) -> Duration {
        let repos: BTreeSet<String> = self
            .folders
            .syncing()
            .into_iter()
            .map(|folder| folder.repo)
            .collect();
        let due = lock(&self.due);
        let pending = due
            .demand
            .iter()
            .chain(&due.background)
            .any(|(_, repo)| repos.contains(repo));
        if pending {
            self.policy.shortest().min(DUE_CHECK)
        } else {
            self.policy.shortest()
        }
    }

    /// Marks both push steps due for the pairs whose poll came up, and drops
    /// the marks of a peer with no Ready lane left. When it returns, its new
    /// control session starts an exchange that moves events again.
    fn mark_polls(
        &self,
        state: &mut PumpState,
        endpoints: &[String],
        peers: &[ReadyPeer],
        folders: &[SyncFolder],
    ) {
        state
            .polls
            .retain(|(endpoint, _), _| endpoints.contains(endpoint));
        let notified = |endpoint: &str| {
            peers
                .iter()
                .any(|peer| peer.notify && peer.endpoint == endpoint)
        };
        let now = Instant::now();
        let mut due = lock(&self.due);
        let lanes = |endpoint: &String| {
            endpoints.contains(endpoint) || peers.iter().any(|peer| peer.endpoint == *endpoint)
        };
        due.demand.retain(|(endpoint, _)| lanes(endpoint));
        due.background.retain(|(endpoint, _)| lanes(endpoint));
        for folder in folders {
            for endpoint in endpoints {
                let pair = (endpoint.clone(), folder.repo.clone());
                if state.push.rests(&folder.id, endpoint, now)
                    || state.polls.get(&pair).is_some_and(|at| *at > now)
                {
                    continue;
                }
                let poll = self.policy.poll(notified(endpoint));
                state.polls.insert(pair.clone(), now + poll);
                mark(&mut due.demand, pair.clone());
                mark(&mut due.background, pair);
            }
        }
    }

    /// Whether `folder` to `endpoint` has its step in `marks` due and can run
    /// it now. A pair at rest keeps its mark.
    fn take_due(
        &self,
        state: &PumpState,
        folder: &SyncFolder,
        endpoint: &str,
        marks: impl Fn(&mut Due) -> &mut BTreeSet<Pair>,
    ) -> bool {
        !state.push.rests(&folder.id, endpoint, Instant::now())
            && marks(&mut lock(&self.due)).remove(&(endpoint.to_owned(), folder.repo.clone()))
    }

    /// One round, in a fixed order. Pass 1 supplies every folder's pending
    /// demand, then the push pass serves the listeners' demand; pass 2 is the
    /// background `objects.missing` work, then the background push. The push
    /// steps run only for the pairs that are due. Both background passes are
    /// abandoned before their next call as soon as the wake moves past `seen`
    /// (the next round starts with pass 1). Pull's rests hold back only the
    /// pull passes: a failure of one folder's fetch says nothing of another's
    /// push, and what says the lane is unusable rests the push steps by itself
    /// ([`Self::supply`]). The number of objects stored, here or on a listener.
    fn pump_round(&self, state: &mut PumpState, seen: u64) -> usize {
        state.round += 1;
        let round = state.round;
        state.resting.retain(|_, at| *at > round);
        let mut all = self.lanes.bulk_endpoints();
        let peers = self.lanes.ready_peers();
        if all.is_empty() {
            self.mark_polls(state, &all, &peers, &[]);
            return 0;
        }
        let turn = (round as usize) % all.len();
        all.rotate_left(turn);
        let mut endpoints = all.clone();
        endpoints.retain(|endpoint| !state.resting.contains_key(endpoint));
        let folders = self.folders.syncing();
        let syncing = |id: &String| folders.iter().any(|folder| folder.id == *id);
        state.unavailable.retain(|id, _| syncing(id));
        state.scan.retain(|id, _| syncing(id));
        state.batch.retain(|(id, _), _| syncing(id));
        self.mark_polls(state, &all, &peers, &folders);
        let now = Instant::now();
        state
            .supplied
            .retain(|_, at| now.saturating_duration_since(*at) < DEMAND_SUPPLIED);
        let mut stored = 0;
        // Pass 1: what readers wait for. A digest just written may still be
        // listed until the worker's next `watch`: it is not fetched again.
        for folder in &folders {
            let demand: Vec<(Digest, Kind)> = folder
                .demand
                .iter()
                .filter(|(digest, _)| !state.supplied.contains_key(&(folder.id.clone(), *digest)))
                .copied()
                .collect();
            if !demand.is_empty() && !endpoints.is_empty() && !self.cancel.is_cancelled() {
                stored += self.supply(state, &endpoints, folder, demand, None).stored;
            }
        }
        // What the listeners' readers wait for. A failure needs no mark: their
        // notify repeats while they wait.
        for folder in &folders {
            for endpoint in &all {
                if self.cancel.is_cancelled() {
                    break;
                }
                if self.take_due(state, folder, endpoint, |due| &mut due.demand) {
                    stored += self
                        .push(state, endpoint, folder, PushMode::Demand, None)
                        .pushed;
                }
            }
        }
        // Pass 2: background prefetch. With every endpoint resting a page would
        // be passed over unasked.
        for folder in &folders {
            if self.cancel.is_cancelled() || self.wake.current() != seen || endpoints.is_empty() {
                break;
            }
            let Some((wanted, cursor)) = self.background(state, folder) else {
                continue;
            };
            let supplied = if wanted.is_empty() {
                Supplied::default()
            } else {
                self.supply(state, &endpoints, folder, wanted, Some(seen))
            };
            stored += supplied.stored;
            if supplied.interrupted {
                break;
            }
            // A page wholly stored is listed afresh from the same position
            // (what was stored is no longer missing); any other moves on, and
            // a scan at its end starts over.
            if supplied.stored < OBJECT_BATCH {
                state.scan.insert(folder.id.clone(), cursor);
            }
        }
        // What the listeners lack.
        if fault::hit(FAULT_BACKGROUND_SKIP).is_some() {
            return stored;
        }
        for folder in &folders {
            for endpoint in &all {
                if self.cancel.is_cancelled() || self.wake.current() != seen {
                    return stored;
                }
                if !self.take_due(state, folder, endpoint, |due| &mut due.background) {
                    continue;
                }
                let outcome = self.push(state, endpoint, folder, PushMode::Background, Some(seen));
                stored += outcome.pushed;
                // A page that listed something is not done. A listing that
                // failed lists nothing: it waits for the pair's next trigger, so
                // a hung listener does not hold this pump again at once.
                if outcome.interrupted || outcome.pushed + outcome.remaining > 0 {
                    let pair = (endpoint.clone(), folder.repo.clone());
                    mark(&mut lock(&self.due).background, pair);
                }
                if outcome.interrupted {
                    return stored;
                }
            }
        }
        stored
    }

    /// A push step of `folder` to `endpoint`.
    fn push(
        &self,
        state: &mut PumpState,
        endpoint: &String,
        folder: &SyncFolder,
        mode: PushMode,
        interrupt: Option<u64>,
    ) -> push::PushOutcome {
        push::run(
            self.lanes.as_ref(),
            &mut state.push,
            std::slice::from_ref(endpoint),
            folder,
            mode,
            interrupt.map(|seen| (self.wake.as_ref(), seen)),
            &self.cancel,
        )
    }

    /// The folder's background work: the next `objects.missing` page of its
    /// scan without the digests no peer had lately (passed as `exclude`, and
    /// filtered here too), with the cursor after that page. `None` when the
    /// worker failed.
    fn background(
        &self,
        state: &mut PumpState,
        folder: &SyncFolder,
    ) -> Option<(Wanted, Option<String>)> {
        let now = Instant::now();
        let unavailable = state.unavailable.entry(folder.id.clone()).or_default();
        let exclude: Vec<Digest> = unavailable
            .iter()
            .filter(|(_, entry)| entry.until > now)
            .map(|(digest, _)| *digest)
            .take(MAX_EXCLUDE)
            .collect();
        let after = state.scan.get(&folder.id).cloned().flatten();
        let (missing, cursor) = match folder
            .link
            .missing(OBJECT_BATCH, &exclude, after.as_deref())
        {
            Ok(page) => {
                state.logged.remove(&folder.id);
                (page.missing, page.cursor)
            }
            Err(error) => {
                let error = format!("objects.missing: {error}");
                if state.logged.get(&folder.id) != Some(&error) {
                    event!(warn, SYNC_MISSING_FAILED, {
                        "folder": folder.id.clone(),
                        "error": error.clone(),
                    });
                    state.logged.insert(folder.id.clone(), error);
                }
                return None;
            }
        };
        // An expired backoff is kept while its object is still missing (the
        // next one doubles); one no longer missing is forgotten.
        let listed: BTreeSet<Digest> = missing.iter().map(|(digest, _)| *digest).collect();
        unavailable.retain(|digest, entry| entry.until > now || listed.contains(digest));
        let wanted = missing
            .into_iter()
            .filter(|(digest, _)| {
                unavailable
                    .get(digest)
                    .is_none_or(|entry| entry.until <= now)
            })
            .collect();
        Some((wanted, cursor))
    }

    /// Fetches `wanted` for `folder`: each endpoint in turn is asked, per
    /// bulk lane and at most [`OBJECT_BATCH`] per call, for what is still
    /// missing, and what arrives is written to the worker. A digest that
    /// every asked peer omitted (none failing) is unavailable for a backoff
    /// (5 s doubling to 5 min). A failing peer sits out the next round. With
    /// `interrupt`, stops before the next call once the wake moved past it.
    fn supply(
        &self,
        state: &mut PumpState,
        endpoints: &[String],
        folder: &SyncFolder,
        wanted: Vec<(Digest, Kind)>,
        interrupt: Option<u64>,
    ) -> Supplied {
        let mut remaining = wanted;
        let mut asked = BTreeSet::new();
        let mut failed = BTreeSet::new();
        let mut result = Supplied::default();
        'peers: for endpoint in endpoints {
            if remaining.is_empty() {
                break;
            }
            if state.resting.contains_key(endpoint) {
                continue;
            }
            for (conn, batch) in lanes_for(self.lanes.as_ref(), endpoint, &remaining) {
                // Objects of unknown size (manifests) first, in their own
                // small batches; then the chunks.
                let mut queue: VecDeque<(Digest, Kind)> = batch
                    .iter()
                    .filter(|(_, kind)| *kind != Kind::Chunk)
                    .chain(batch.iter().filter(|(_, kind)| *kind == Kind::Chunk))
                    .copied()
                    .collect();
                while let Some(&(_, kind)) = queue.front() {
                    let woken = interrupt.is_some_and(|seen| self.wake.current() != seen);
                    if self.cancel.is_cancelled() || woken {
                        result.interrupted = true;
                        break 'peers;
                    }
                    let chunks = kind == Kind::Chunk;
                    let size = state.batch_size(&folder.id, endpoint);
                    let limit = size.min(if chunks { OBJECT_BATCH } else { MANIFEST_BATCH });
                    let mut chunk = Vec::with_capacity(limit);
                    while chunk.len() < limit
                        && queue
                            .front()
                            .is_some_and(|(_, next)| (*next == Kind::Chunk) == chunks)
                    {
                        chunk.extend(queue.pop_front());
                    }
                    let deadline = (self.fetch_deadline)(&chunk);
                    let fetched = match fetch(&conn, &folder.repo, &chunk, deadline, &self.cancel) {
                        Ok(fetched) => {
                            state.grow(&folder.id, endpoint);
                            fetched
                        }
                        Err(FetchFailure { failure, timed_out }) => {
                            failed.extend(chunk.iter().map(|(digest, _)| *digest));
                            if timed_out {
                                state.shrink(&folder.id, endpoint, chunk.len());
                            }
                            state.resting.insert(endpoint.clone(), state.round + 2);
                            // The lane is unusable, for pushes too. A coded
                            // failure is this folder's alone.
                            if timed_out || failure == Failure::LaneLost {
                                state.push.rest_endpoint(endpoint);
                            }
                            let error = failure.to_string();
                            if state.logged.get(endpoint) != Some(&error) {
                                event!(warn, SYNC_FETCH_FAILED, {
                                    "peer": endpoint.clone(),
                                    "repo": folder.repo.clone(),
                                    "error": error.clone(),
                                    "batch": chunk.len(),
                                });
                                state.logged.insert(endpoint.clone(), error);
                            }
                            continue 'peers;
                        }
                    };
                    // The peer cut its reply to its byte bounds: what it did
                    // not look at is asked again next. It looked at one object
                    // at least, so the queue always shrinks.
                    let looked = fetched
                        .truncated_at
                        .map_or(chunk.len(), |at| at.clamp(1, chunk.len()));
                    for object in chunk[looked..].iter().rev() {
                        queue.push_front(*object);
                    }
                    let chunk = &chunk[..looked];
                    asked.extend(chunk.iter().map(|(digest, _)| *digest));
                    // Never sent (no reply can hold it): logged once, then
                    // treated as absent like any object the peer lacks.
                    for digest in &fetched.oversized {
                        let key = format!("{}/{digest}", folder.id);
                        state.logged.entry(key).or_insert_with(|| {
                            event!(warn, SYNC_OBJECT_TOO_LARGE, {
                                "peer": endpoint.clone(),
                                "repo": folder.repo.clone(),
                                "digest": digest.to_string(),
                            });
                            String::new()
                        });
                    }
                    // Only what was asked (the worker takes ≤ OBJECT_BATCH).
                    let mut objects = fetched.objects;
                    objects.retain(|object| chunk.iter().any(|(d, _)| *d == object.digest));
                    if objects.is_empty() {
                        continue;
                    }
                    let received: BTreeSet<Digest> = objects.iter().map(|o| o.digest).collect();
                    match folder.link.write(objects) {
                        Ok(count) => {
                            result.stored += count;
                            state.logged.remove(endpoint);
                            let now = Instant::now();
                            for digest in &received {
                                state.supplied.insert((folder.id.clone(), *digest), now);
                            }
                            if let Some(unavailable) = state.unavailable.get_mut(&folder.id) {
                                unavailable.retain(|digest, _| !received.contains(digest));
                            }
                            remaining.retain(|(digest, _)| !received.contains(digest));
                        }
                        Err(error) => {
                            event!(warn, SYNC_WRITE_FAILED, {
                                "folder": folder.id.clone(),
                                "error": error.to_string(),
                            });
                            result.interrupted = true;
                            break 'peers;
                        }
                    }
                }
            }
        }
        if !result.interrupted {
            let now = Instant::now();
            let unavailable = state.unavailable.entry(folder.id.clone()).or_default();
            for (digest, _) in &remaining {
                if asked.contains(digest) && !failed.contains(digest) {
                    let backoff = unavailable
                        .get(digest)
                        .map_or(UNAVAILABLE_FIRST, |entry| entry.backoff * 2)
                        .min(UNAVAILABLE_MAX);
                    unavailable.insert(
                        *digest,
                        Unavailable {
                            until: now + backoff,
                            backoff,
                        },
                    );
                }
            }
        }
        result
    }
}

impl NotifySink for Driver {
    /// Runs on the lane's I/O thread: only marks and bumps.
    fn notified(&self, endpoint: &str, notify: &Notify) {
        let pair = (endpoint.to_owned(), notify.repo.clone());
        match notify.reason {
            NotifyReason::Metadata => {
                mark(&mut lock(&self.due).exchange, pair);
                self.poke.bump();
            }
            NotifyReason::Demand => {
                mark(&mut lock(&self.due).demand, pair);
                self.wake.bump();
            }
        }
    }
}

/// `wanted` grouped by the Ready bulk lane of `endpoint` that owns each
/// digest (digests no Ready lane owns are left out).
pub(super) fn lanes_for(
    lanes: &dyn Lanes,
    endpoint: &str,
    wanted: &[(Digest, Kind)],
) -> Vec<(Conn, Wanted)> {
    let mut owners: BTreeMap<String, (Conn, Wanted)> = BTreeMap::new();
    for object in wanted {
        if let Some(conn) = lanes.bulk_lane(endpoint, &object.0) {
            owners
                .entry(conn.peer().session.clone())
                .or_insert_with(|| (conn, Vec::new()))
                .1
                .push(*object);
        }
    }
    owners.into_values().collect()
}

/// One exchange of `repo` with the peer behind `conn`, repeated while either
/// side has more. `cursor` advances only past pages both sides took.
fn exchange(
    link: &WorkerLink,
    conn: &Conn,
    repo: &str,
    cursor: &mut FeedCursor,
    cancel: &Cancel,
) -> Result<(), Failure> {
    loop {
        if cancel.is_cancelled() || conn.closed().is_some() {
            return Err(Failure::LaneLost);
        }
        let page: FeedPage = link
            .call_json("feed.outgoing", json!({"after": cursor.sent}))
            .map_err(|error| worker_failure("feed.outgoing", error))
            .and_then(|value| {
                serde_json::from_value(value).map_err(|e| worker_failure("feed.outgoing", e))
            })?;
        let more = page.more;
        let body = serde_json::to_vec(&FeedRequest {
            after: cursor.received,
            events: page.events,
        })
        .map_err(|error| worker_failure("meta.exchange", error))?;
        let reply = conn
            .call_cancellable(
                "meta.exchange",
                json!({"repo": repo}),
                body,
                Priority::Control,
                PEER_DEADLINE,
                &|| cancel.is_cancelled(),
            )
            .map_err(|error| peer_failure("meta.exchange", error))?;
        let reply: FeedReply = serde_json::from_slice(&reply.body)
            .map_err(|error| Failure::Error(format!("meta.exchange: malformed reply: {error}")))?;
        if !reply.events.is_empty() {
            let events = serde_json::to_vec(&reply.events)
                .map_err(|error| worker_failure("feed.apply", error))?;
            link.call("feed.apply", json!({}), events, CALL_DEADLINE)
                .map_err(|error| worker_failure("feed.apply", error))?;
        }
        cursor.sent = page.cursor;
        cursor.received = reply.cursor;
        if !(more || reply.more) {
            return Ok(());
        }
    }
}

/// Why a fetch failed, and whether its deadline passed.
pub(super) struct FetchFailure {
    pub(super) failure: Failure,
    pub(super) timed_out: bool,
}

impl From<Failure> for FetchFailure {
    fn from(failure: Failure) -> Self {
        Self {
            failure,
            timed_out: false,
        }
    }
}

/// The deadline of an `objects.fetch` of `wanted`: [`PEER_DEADLINE`] plus
/// 1 s per 256 KiB requested (a chunk counts 64 KiB, an object of unknown
/// size such as a manifest 1 MiB), at most [`FETCH_DEADLINE_MAX`].
pub fn fetch_deadline(wanted: &[(Digest, Kind)]) -> Duration {
    let requested: u64 = wanted
        .iter()
        .map(|(_, kind)| match kind {
            Kind::Chunk => CHUNK_GUESS,
            _ => UNKNOWN_GUESS,
        })
        .sum();
    let extra = Duration::from_millis(requested.saturating_mul(1000) / DEADLINE_STEP_BYTES);
    (PEER_DEADLINE + extra).min(FETCH_DEADLINE_MAX)
}

/// `objects.fetch` of ≤ [`OBJECT_BATCH`] objects on a peer bulk lane, within
/// `deadline`.
fn fetch(
    conn: &Conn,
    repo: &str,
    wanted: &[(Digest, Kind)],
    deadline: Duration,
    cancel: &Cancel,
) -> Result<Fetched, FetchFailure> {
    let body = serde_json::to_vec(wanted).map_err(|e| worker_failure("objects.fetch", e))?;
    let reply = conn
        .call_cancellable(
            "objects.fetch",
            json!({"repo": repo}),
            body,
            Priority::Data,
            deadline,
            &|| cancel.is_cancelled(),
        )
        .map_err(|error| FetchFailure {
            timed_out: matches!(error, CallError::Timeout { .. }),
            failure: peer_failure("objects.fetch", error),
        })?;
    Fetched::from_reply(&reply.header, &reply.body).map_err(|error| {
        Failure::Error(format!("objects.fetch: malformed reply: {error:#}")).into()
    })
}

/// One clock probe on a control lane: stores the smoothed offset of
/// `installation` and warns past [`WARN_OFFSET_MS`].
fn probe(catalog: &Catalog, installation: &str, conn: &Conn, cancel: &Cancel) -> Result<Offset> {
    let t1 = now_ms();
    let reply = conn
        .call_cancellable(
            "clock.probe",
            json!({"t1": t1}),
            Vec::new(),
            Priority::Control,
            PEER_DEADLINE,
            &|| cancel.is_cancelled(),
        )
        .map_err(|error| anyhow::anyhow!("clock.probe: {error}"))?;
    let t4 = now_ms();
    let times: Value = serde_json::from_slice(&reply.body).context("clock.probe reply")?;
    let time = |key: &str| {
        times[key]
            .as_u64()
            .with_context(|| format!("clock.probe: no {key}"))
    };
    let sample = Sample {
        t1,
        t2: time("t2")?,
        t3: time("t3")?,
        t4,
    }
    .offset();
    let previous = catalog
        .clock(installation)?
        .map(|(offset, _)| offset.offset_ms);
    let offset = Offset {
        offset_ms: clock::smooth(previous, sample.offset_ms),
        rtt_ms: sample.rtt_ms,
    };
    catalog.set_clock(installation, offset, t4)?;
    if offset.offset_ms.abs() > WARN_OFFSET_MS {
        event!(warn, CLOCK_OFFSET_LARGE, {
            "installation": installation,
            "offset_ms": offset.offset_ms,
            "rtt_ms": offset.rtt_ms,
        });
    }
    Ok(offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::{
        network::{Refused, StoreReply},
        sync::{MissingPage, Object, WantedPage},
    };
    use crate::{
        model::{
            errors::ErrorWire,
            protocol::{LaneRole, features},
        },
        session::{ConnOptions, Handler, Incoming, PeerInfo, Side},
    };
    use std::net::{TcpListener, TcpStream};
    use tungstenite::stream::MaybeTlsStream;

    #[test]
    fn the_driver_bounds_match_the_worker_and_the_brief() {
        assert_eq!(MAX_EXCHANGES, 4);
        assert_eq!(OBJECT_BATCH, 32);
        assert_eq!(CLOCK_PROBE_EVERY, 60);
    }

    #[test]
    fn a_lost_lane_is_not_a_repo_error() {
        for error in [
            CallError::ConnectionLost("x".into()),
            CallError::Unavailable("x".into()),
            CallError::Cancelled,
        ] {
            assert_eq!(peer_failure("meta.exchange", error), Failure::LaneLost);
        }
        let remote = peer_failure(
            "meta.exchange",
            CallError::Remote(ErrorWire::coded("REPO_NOT_PUBLISHED", "paused")),
        );
        assert!(
            matches!(&remote, Failure::Error(e) if e.contains("REPO_NOT_PUBLISHED")),
            "{remote:?}"
        );
        assert!(matches!(
            peer_failure("objects.fetch", CallError::Timeout { after_ms: 1 }),
            Failure::Error(_)
        ));
    }

    /// Answers `clock.probe` as a peer whose clock runs `skew_ms` ahead.
    struct SkewedPeer {
        skew_ms: Mutex<i64>,
    }

    impl Handler for SkewedPeer {
        fn handle(&self, req: Incoming) {
            let t1 = req.header["t1"].as_u64().unwrap();
            let peer_now = (now_ms() as i64 + *lock(&self.skew_ms)) as u64;
            let body = json!({"t1": t1, "t2": peer_now, "t3": peer_now + 1});
            req.responder
                .reply(json!({}), serde_json::to_vec(&body).unwrap());
        }
    }

    fn info_as(role: LaneRole) -> PeerInfo {
        PeerInfo {
            installation: "peer-b".into(),
            role,
            lane: None,
            session: "session".into(),
            verifier_generation: 1,
            features: Vec::new(),
        }
    }

    /// A client `Conn` whose server side is served by `handler`.
    fn served(handler: Arc<dyn Handler>) -> (Conn, Conn) {
        served_as(LaneRole::Control, handler)
    }

    /// [`served`] as a session of `role`.
    fn served_as(role: LaneRole, handler: Arc<dyn Handler>) -> (Conn, Conn) {
        served_info(info_as(role), handler)
    }

    /// [`served`] with the client seeing `info`.
    fn served_info(info: PeerInfo, handler: Arc<dyn Handler>) -> (Conn, Conn) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            tungstenite::accept(MaybeTlsStream::Plain(stream)).unwrap()
        });
        let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        let client = tungstenite::client(
            format!("ws://127.0.0.1:{port}/"),
            MaybeTlsStream::Plain(stream),
        )
        .unwrap()
        .0;
        let role = info.role;
        let client =
            Conn::from_established(client, Side::Client, info, ConnOptions::default()).unwrap();
        let server = Conn::from_established(
            server.join().unwrap(),
            Side::Server,
            info_as(role),
            ConnOptions::default(),
        )
        .unwrap();
        server.serve(handler);
        (client, server)
    }

    #[test]
    fn a_probe_stores_the_smoothed_offset_and_the_round_trip() {
        let root = tempfile::tempdir().unwrap();
        let catalog = Catalog::open(root.path()).unwrap();
        let peer = Arc::new(SkewedPeer {
            skew_ms: Mutex::new(8000),
        });
        let (client, server) = served(peer.clone());
        let cancel = Cancel::new();
        let before = now_ms();
        // The first sample is taken as it is (and is past WARN_OFFSET_MS).
        let first = probe(&catalog, "peer-b", &client, &cancel).unwrap();
        assert!((first.offset_ms - 8000).abs() <= 1000, "{first:?}");
        assert!(first.rtt_ms < 1000, "{first:?}");
        let (stored, measured_at) = catalog.clock("peer-b").unwrap().unwrap();
        assert_eq!(stored, first);
        assert!(measured_at >= before);
        // The next moves a quarter of the way toward the new sample.
        *lock(&peer.skew_ms) = 0;
        let second = probe(&catalog, "peer-b", &client, &cancel).unwrap();
        assert!((second.offset_ms - 6000).abs() <= 1000, "{second:?}");
        assert_eq!(catalog.clock("peer-b").unwrap().unwrap().0, second);
        client.close();
        server.close();
    }

    // ---- pump ------------------------------------------------------------

    const TOKEN: &str = "pump-test-token";

    struct FnHandler<F>(F);

    impl<F: Fn(Incoming) + Send + Sync> Handler for FnHandler<F> {
        fn handle(&self, req: Incoming) {
            (self.0)(req)
        }
    }

    /// `count` distinct objects (bytes and digest).
    fn objects(tag: &str, count: usize) -> Vec<Object> {
        (0..count)
            .map(|i| {
                let bytes = format!("{tag}-{i}").into_bytes();
                Object {
                    digest: Digest::bytes(&bytes),
                    kind: Kind::Chunk,
                    bytes,
                }
            })
            .collect()
    }

    fn wanted_of(objects: &[Object]) -> Vec<(Digest, Kind)> {
        objects.iter().map(|o| (o.digest, o.kind)).collect()
    }

    /// A worker standing in for one folder: `objects.missing {limit,
    /// exclude}` lists `missing` in order minus what was written and what is
    /// excluded; `objects.write` records each digest with its time.
    #[derive(Default)]
    struct FakeFolder {
        missing: Vec<(Digest, Kind)>,
        written: Mutex<Vec<(Digest, Instant)>>,
        excludes: Mutex<Vec<usize>>,
        /// What `objects.fetch` reads, as the worker does: in request order,
        /// absent ones left out, with the kind asked.
        held: Vec<Object>,
        /// `objects.fetch` stops after this many objects (`truncated_at`).
        read_cut: Option<usize>,
        /// Bumped by every `objects.fetch`, as a reader's demand arriving
        /// during the read.
        wake_on_read: Option<Arc<Seq>>,
    }

    impl FakeFolder {
        fn written(&self) -> Vec<Digest> {
            lock(&self.written)
                .iter()
                .map(|(digest, _)| *digest)
                .collect()
        }

        fn written_at(&self, digest: &Digest) -> Option<Instant> {
            lock(&self.written)
                .iter()
                .find(|(d, _)| d == digest)
                .map(|(_, at)| *at)
        }

        fn serve(&self, req: Incoming) {
            let reply = match req.op.as_str() {
                "objects.missing" => {
                    let limit = req.header["limit"].as_u64().unwrap() as usize;
                    let exclude: BTreeSet<Digest> =
                        serde_json::from_value(req.header["exclude"].clone()).unwrap();
                    lock(&self.excludes).push(exclude.len());
                    let start: usize = req.header["after"]
                        .as_str()
                        .map_or(0, |after| after.parse().unwrap());
                    let written = self.written();
                    let mut page = MissingPage::default();
                    for (index, object) in self.missing.iter().enumerate().skip(start) {
                        if !exclude.contains(&object.0) && !written.contains(&object.0) {
                            page.missing.push(*object);
                        }
                        if page.missing.len() == limit {
                            page.cursor = Some((index + 1).to_string());
                            break;
                        }
                    }
                    serde_json::to_vec(&page).unwrap()
                }
                "objects.write" => {
                    let objects: Vec<Object> = serde_json::from_slice(&req.body).unwrap();
                    let now = Instant::now();
                    lock(&self.written).extend(objects.iter().map(|o| (o.digest, now)));
                    serde_json::to_vec(&json!({"stored": objects.len()})).unwrap()
                }
                "objects.fetch" => {
                    if let Some(wake) = &self.wake_on_read {
                        wake.bump();
                    }
                    let wanted: Vec<(Digest, Kind)> = serde_json::from_slice(&req.body).unwrap();
                    let mut fetched = Fetched::default();
                    for (index, (digest, kind)) in wanted.into_iter().enumerate() {
                        let Some(object) = self.held.iter().find(|o| o.digest == digest) else {
                            continue;
                        };
                        if self.read_cut == Some(fetched.objects.len()) {
                            fetched.truncated_at = Some(index);
                            break;
                        }
                        fetched.objects.push(Object {
                            kind,
                            ..object.clone()
                        });
                    }
                    let body = serde_json::to_vec(&fetched.objects).unwrap();
                    req.responder.reply(fetched.header(), body);
                    return;
                }
                op => {
                    req.responder
                        .error(ErrorWire::coded("UNSUPPORTED_OPERATION", op));
                    return;
                }
            };
            req.responder.reply(json!({}), reply);
        }
    }

    /// A loopback worker serving `folder`, and its link.
    fn fake_worker(folder: Arc<FakeFolder>) -> Arc<WorkerLink> {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("ws://{}", listener.local_addr().unwrap());
        let handler: Arc<dyn Handler> = Arc::new(FnHandler(move |req: Incoming| folder.serve(req)));
        thread::spawn(move || {
            let mut conns = Vec::new();
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                if let Ok(conn) = crate::session::accept_loopback(
                    stream,
                    Some(TOKEN),
                    ConnOptions::for_role(LaneRole::Control),
                ) {
                    conn.serve(handler.clone());
                    conns.push(conn);
                }
            }
        });
        let ready = super::super::worker::Ready {
            ready: true,
            repo: "repo-1".into(),
            device: "device-1".into(),
            instance: "instance-1".into(),
            pid: 1,
            address,
            token: TOKEN.into(),
            version: "test".into(),
            ipc: 1,
            mounts: Vec::new(),
        };
        WorkerLink::connect(&ready, 2).unwrap()
    }

    /// A peer bulk lane that holds `held`; a fetch naming one of `slow`
    /// takes `delay`, and so does one of more than `stall_above` objects.
    /// With `cut`, a reply carries at most that many objects and names the
    /// first digest not looked at (`truncated_at`), as a peer at its byte
    /// bounds does. Counts the fetches and records each request's kinds.
    struct Peer {
        held: BTreeMap<Digest, Object>,
        slow: BTreeSet<Digest>,
        delay: Duration,
        stall_above: Option<usize>,
        cut: Option<usize>,
        fetches: Mutex<u32>,
        requests: Mutex<Vec<Vec<Kind>>>,
    }

    impl Peer {
        fn new(held: &[Object], slow: &[Object], delay: Duration) -> Arc<Self> {
            Arc::new(Self::plain(held, slow, delay))
        }

        fn plain(held: &[Object], slow: &[Object], delay: Duration) -> Self {
            Self {
                held: held.iter().map(|o| (o.digest, o.clone())).collect(),
                slow: slow.iter().map(|o| o.digest).collect(),
                delay,
                stall_above: None,
                cut: None,
                fetches: Mutex::new(0),
                requests: Mutex::new(Vec::new()),
            }
        }

        fn lane(self: &Arc<Self>) -> Conn {
            let peer = self.clone();
            let (client, server) = served_as(
                LaneRole::Bulk,
                Arc::new(FnHandler(move |req: Incoming| {
                    *lock(&peer.fetches) += 1;
                    let wanted: Vec<(Digest, Kind)> = serde_json::from_slice(&req.body).unwrap();
                    lock(&peer.requests).push(wanted.iter().map(|(_, kind)| *kind).collect());
                    if wanted.iter().any(|(d, _)| peer.slow.contains(d))
                        || peer.stall_above.is_some_and(|most| wanted.len() > most)
                    {
                        thread::sleep(peer.delay);
                    }
                    let mut header = json!({});
                    let mut objects: Vec<&Object> = Vec::new();
                    for (index, (digest, _)) in wanted.iter().enumerate() {
                        let Some(object) = peer.held.get(digest) else {
                            continue;
                        };
                        if peer.cut.is_some_and(|most| objects.len() == most) {
                            header = json!({"truncated_at": index});
                            break;
                        }
                        objects.push(object);
                    }
                    req.responder
                        .reply(header, serde_json::to_vec(&objects).unwrap());
                })),
            );
            std::mem::forget(server);
            client
        }
    }

    /// One peer endpoint whose every digest goes to `conn`.
    struct OneLane(Conn);

    impl Lanes for OneLane {
        fn ready_peers(&self) -> Vec<ReadyPeer> {
            Vec::new()
        }
        fn bulk_endpoints(&self) -> Vec<String> {
            vec!["ws://peer".into()]
        }
        fn bulk_lane(&self, _: &str, _: &Digest) -> Option<Conn> {
            Some(self.0.clone())
        }
        fn any_bulk_lane(&self, _: &str) -> Option<Conn> {
            Some(self.0.clone())
        }
    }

    /// Running folders with settable demand.
    struct Folders(Vec<(String, Arc<WorkerLink>, Mutex<Wanted>)>);

    impl SyncFolders for Folders {
        fn syncing(&self) -> Vec<SyncFolder> {
            self.0
                .iter()
                .map(|(id, link, demand)| SyncFolder {
                    id: id.clone(),
                    repo: "repo-1".into(),
                    link: link.clone(),
                    demand: lock(demand).clone(),
                })
                .collect()
        }
    }

    fn driver(
        lanes: OneLane,
        folders: Arc<dyn SyncFolders>,
        wake: Arc<Seq>,
    ) -> (Arc<Driver>, tempfile::TempDir) {
        let root = tempfile::tempdir().unwrap();
        let catalog = Catalog::open(root.path()).unwrap();
        let driver = Driver::new(
            Arc::new(lanes),
            catalog,
            folders,
            wake,
            Duration::from_millis(50),
            Cancel::new(),
        );
        (driver, root)
    }

    #[test]
    fn the_pump_moves_past_objects_no_peer_has_within_two_rounds() {
        // 40 missing; the peer has none of the first 32 (the whole first
        // `objects.missing` page) and all of the last 8.
        let all = objects("object", 40);
        let folder = Arc::new(FakeFolder {
            missing: wanted_of(&all),
            ..FakeFolder::default()
        });
        let link = fake_worker(folder.clone());
        let peer = Peer::new(&all[32..], &[], Duration::ZERO);
        let folders = Arc::new(Folders(vec![("f1".into(), link, Mutex::new(Vec::new()))]));
        let (driver, _root) = driver(OneLane(peer.lane()), folders, Seq::new());
        let mut state = PumpState::default();
        let seen = driver.wake.current();
        assert_eq!(driver.pump_round(&mut state, seen), 0);
        let unavailable = &state.unavailable["f1"];
        assert_eq!(unavailable.len(), 32);
        assert!(unavailable.values().all(|u| u.backoff == UNAVAILABLE_FIRST));
        assert_eq!(driver.pump_round(&mut state, seen), 8);
        let expected: Vec<Digest> = all[32..].iter().map(|o| o.digest).collect();
        assert_eq!(folder.written(), expected);
        // The second `objects.missing` excluded the 32 no peer had.
        assert_eq!(lock(&folder.excludes).as_slice(), &[0, 32]);
        // An object asked again after its backoff and still absent doubles it.
        for entry in state.unavailable.get_mut("f1").unwrap().values_mut() {
            entry.until = Instant::now();
        }
        assert_eq!(driver.pump_round(&mut state, seen), 0);
        assert!(
            state.unavailable["f1"]
                .values()
                .all(|u| u.backoff == UNAVAILABLE_FIRST * 2 && u.until > Instant::now())
        );
    }

    #[test]
    fn the_scan_pages_past_a_long_run_no_peer_has() {
        // 300 head digests no peer has (more than MAX_EXCLUDE + one page),
        // then one the peer has.
        let all = objects("head", 301);
        let folder = Arc::new(FakeFolder {
            missing: wanted_of(&all),
            ..FakeFolder::default()
        });
        let link = fake_worker(folder.clone());
        let peer = Peer::new(&all[300..], &[], Duration::ZERO);
        let folders = Arc::new(Folders(vec![("f1".into(), link, Mutex::new(Vec::new()))]));
        let (driver, _root) = driver(OneLane(peer.lane()), folders, Seq::new());
        let mut state = PumpState::default();
        let seen = driver.wake.current();
        let bound = 300_usize.div_ceil(OBJECT_BATCH) + 2;
        let mut rounds = 0;
        while folder.written().is_empty() {
            rounds += 1;
            assert!(
                rounds <= bound,
                "the 301st object not fetched in {bound} rounds"
            );
            driver.pump_round(&mut state, seen);
        }
        assert_eq!(folder.written(), vec![all[300].digest]);
        assert!(
            rounds <= 300_usize.div_ceil(OBJECT_BATCH) + 1,
            "{rounds} rounds"
        );
    }

    #[test]
    fn demand_is_served_before_background_batches_of_other_folders() {
        // Folder 1: 64 objects behind slow fetches (two background rounds);
        // folder 3: 32 more; folder 2: nothing missing, one demanded object.
        let first = objects("first", 64);
        let third = objects("third", 32);
        let demanded = objects("demanded", 1);
        let delay = Duration::from_millis(500);
        let held: Vec<Object> = first
            .iter()
            .chain(&third)
            .chain(&demanded)
            .cloned()
            .collect();
        let slow: Vec<Object> = first.iter().chain(&third).cloned().collect();
        let peer = Peer::new(&held, &slow, delay);
        let fakes: Vec<Arc<FakeFolder>> = [wanted_of(&first), Vec::new(), wanted_of(&third)]
            .into_iter()
            .map(|missing| {
                Arc::new(FakeFolder {
                    missing,
                    ..FakeFolder::default()
                })
            })
            .collect();
        let folders = Arc::new(Folders(
            fakes
                .iter()
                .enumerate()
                .map(|(i, fake)| {
                    (
                        format!("f{}", i + 1),
                        fake_worker(fake.clone()),
                        Mutex::new(Vec::new()),
                    )
                })
                .collect(),
        ));
        let wake = Seq::new();
        let root = tempfile::tempdir().unwrap();
        let driver = start(
            Arc::new(OneLane(peer.lane())),
            Catalog::open(root.path()).unwrap(),
            folders.clone(),
            wake.clone(),
            active(Duration::from_millis(50), Duration::from_millis(50)),
            Cancel::new(),
        )
        .unwrap();
        // Folder 1's first background batch is in flight: a reader of
        // folder 2 starts waiting.
        let end = Instant::now() + Duration::from_secs(10);
        while *lock(&peer.fetches) == 0 {
            assert!(Instant::now() < end, "no background fetch started");
            thread::sleep(Duration::from_millis(5));
        }
        *lock(&folders.0[1].2) = wanted_of(&demanded);
        wake.bump();
        while fakes[0].written().len() < 64 || fakes[2].written().len() < 32 {
            assert!(
                Instant::now() < end,
                "not everything written: {} / {} / {}",
                fakes[0].written().len(),
                fakes[1].written().len(),
                fakes[2].written().len()
            );
            thread::sleep(Duration::from_millis(10));
        }
        driver.stop(Duration::from_secs(2));
        let served = fakes[1]
            .written_at(&demanded[0].digest)
            .expect("the demanded object was never written");
        let last_first = fakes[0].written_at(&first[63].digest).unwrap();
        let first_third = fakes[2].written_at(&third[0].digest).unwrap();
        assert!(
            served < last_first,
            "demand waited for folder 1's second background batch"
        );
        assert!(
            served < first_third,
            "demand waited for folder 3's background batch"
        );
    }

    #[test]
    fn the_fetch_deadline_grows_with_what_is_asked_and_is_capped() {
        let chunk = |n: u8| (Digest([n; 32]), Kind::Chunk);
        let manifest = |n: u8| (Digest([n; 32]), Kind::Manifest);
        assert_eq!(fetch_deadline(&[]), PEER_DEADLINE);
        // 1 s per 256 KiB: a 64 KiB chunk adds 250 ms…
        assert_eq!(
            fetch_deadline(&[chunk(1)]),
            PEER_DEADLINE + Duration::from_millis(250)
        );
        // …a full chunk batch (2 MiB) 8 s…
        let chunks: Vec<_> = (0..OBJECT_BATCH as u8).map(chunk).collect();
        assert_eq!(
            fetch_deadline(&chunks),
            PEER_DEADLINE + Duration::from_secs(8)
        );
        // …a manifest (size unknown, taken as 1 MiB) 4 s.
        let manifests: Vec<_> = (0..MANIFEST_BATCH as u8).map(manifest).collect();
        assert_eq!(
            fetch_deadline(&manifests),
            PEER_DEADLINE + Duration::from_secs(16)
        );
        // At most FETCH_DEADLINE_MAX.
        let many: Vec<_> = (0..=255).map(manifest).collect();
        assert_eq!(fetch_deadline(&many), FETCH_DEADLINE_MAX);
        assert_eq!(FETCH_DEADLINE_MAX, Duration::from_secs(120));
    }

    /// `count` distinct objects of kind Manifest.
    fn manifests(tag: &str, count: usize) -> Vec<Object> {
        objects(tag, count)
            .into_iter()
            .map(|object| Object {
                kind: Kind::Manifest,
                ..object
            })
            .collect()
    }

    /// Pump rounds until `folder` has `count` objects written, at most
    /// `bound` of time.
    fn pump_until(
        driver: &Driver,
        state: &mut PumpState,
        folder: &FakeFolder,
        count: usize,
        bound: Duration,
    ) -> u32 {
        let end = Instant::now() + bound;
        let seen = driver.wake.current();
        let mut rounds = 0;
        while folder.written().len() < count {
            assert!(
                Instant::now() < end,
                "{} of {count} written after {rounds} rounds",
                folder.written().len()
            );
            driver.pump_round(state, seen);
            rounds += 1;
        }
        rounds
    }

    #[test]
    fn a_timed_out_batch_is_halved_and_the_pump_completes() {
        // A slow link: a fetch of more than 16 objects outlasts the deadline,
        // one of 16 or fewer is answered at once.
        let all = objects("slow-link", OBJECT_BATCH);
        let folder = Arc::new(FakeFolder {
            missing: wanted_of(&all),
            ..FakeFolder::default()
        });
        let link = fake_worker(folder.clone());
        let mut peer = Peer::plain(&all, &[], Duration::from_millis(900));
        peer.stall_above = Some(16);
        let peer = Arc::new(peer);
        let folders = Arc::new(Folders(vec![("f1".into(), link, Mutex::new(Vec::new()))]));
        let (mut driver, _root) = driver(OneLane(peer.lane()), folders, Seq::new());
        Arc::get_mut(&mut driver).unwrap().fetch_deadline = |_| Duration::from_millis(300);
        let mut state = PumpState::default();
        let seen = driver.wake.current();
        // The full batch times out: nothing stored, the batch halves, and
        // the timed-out digests are not taken as missing on the peer.
        assert_eq!(driver.pump_round(&mut state, seen), 0);
        assert_eq!(state.batch_size("f1", "ws://peer"), OBJECT_BATCH / 2);
        assert!(state.unavailable["f1"].is_empty());
        assert_eq!(lock(&peer.requests)[0].len(), OBJECT_BATCH);
        pump_until(
            &driver,
            &mut state,
            &folder,
            OBJECT_BATCH,
            Duration::from_secs(30),
        );
        let mut written = folder.written();
        written.sort();
        let mut expected: Vec<Digest> = all.iter().map(|o| o.digest).collect();
        expected.sort();
        assert_eq!(written, expected);
        // Every batch after the timeout was within the halved size, and the
        // answered ones grew it back.
        let requests = lock(&peer.requests).clone();
        assert!(
            requests[1..].iter().all(|kinds| kinds.len() <= 16),
            "{:?}",
            requests.iter().map(Vec::len).collect::<Vec<_>>()
        );
        assert!(state.batch_size("f1", "ws://peer") > OBJECT_BATCH / 2);
    }

    #[test]
    fn a_batch_shrinks_to_one_and_grows_back_to_the_full_batch() {
        let mut state = PumpState::default();
        for expected in [16, 8, 4, 2, 1, 1] {
            let asked = state.batch_size("f", "p");
            state.shrink("f", "p", asked);
            assert_eq!(state.batch_size("f", "p"), expected);
        }
        let mut sizes = Vec::new();
        while state.batch_size("f", "p") < OBJECT_BATCH {
            state.grow("f", "p");
            sizes.push(state.batch_size("f", "p"));
            assert!(sizes.len() < 32, "{sizes:?}");
        }
        assert_eq!(sizes[..4], [2, 3, 4, 5]);
        assert!(state.batch.is_empty());
        // Growing a full batch keeps it full.
        state.grow("f", "p");
        assert_eq!(state.batch_size("f", "p"), OBJECT_BATCH);
    }

    #[test]
    fn manifests_are_asked_in_their_own_batches_of_four_before_chunks() {
        let manifests = manifests("manifest", 6);
        let chunks = objects("chunk", 10);
        // Listed mixed, as an `objects.missing` page may be.
        let all: Vec<Object> = chunks[..5]
            .iter()
            .chain(&manifests)
            .chain(&chunks[5..])
            .cloned()
            .collect();
        let folder = Arc::new(FakeFolder {
            missing: wanted_of(&all),
            ..FakeFolder::default()
        });
        let link = fake_worker(folder.clone());
        let peer = Peer::new(&all, &[], Duration::ZERO);
        let folders = Arc::new(Folders(vec![("f1".into(), link, Mutex::new(Vec::new()))]));
        let (driver, _root) = driver(OneLane(peer.lane()), folders, Seq::new());
        let mut state = PumpState::default();
        assert_eq!(driver.pump_round(&mut state, driver.wake.current()), 16);
        let requests = lock(&peer.requests).clone();
        assert_eq!(
            requests,
            vec![
                vec![Kind::Manifest; 4],
                vec![Kind::Manifest; 2],
                vec![Kind::Chunk; 10],
            ]
        );
    }

    #[test]
    fn a_reply_cut_to_the_byte_bounds_has_the_rest_asked_again() {
        // The peer answers at most 3 objects per reply and says where it
        // stopped; the 2 objects it does not hold are still found absent.
        let all = objects("cut", 12);
        let held: Vec<Object> = all[..10].to_vec();
        let folder = Arc::new(FakeFolder {
            missing: wanted_of(&all),
            ..FakeFolder::default()
        });
        let link = fake_worker(folder.clone());
        let mut peer = Peer::plain(&held, &[], Duration::ZERO);
        peer.cut = Some(3);
        let peer = Arc::new(peer);
        let folders = Arc::new(Folders(vec![("f1".into(), link, Mutex::new(Vec::new()))]));
        let (driver, _root) = driver(OneLane(peer.lane()), folders, Seq::new());
        let mut state = PumpState::default();
        // One round stores everything the peer holds.
        assert_eq!(driver.pump_round(&mut state, driver.wake.current()), 10);
        let expected: Vec<Digest> = held.iter().map(|o| o.digest).collect();
        assert_eq!(folder.written(), expected);
        // 12, then what was not looked at: 9 (from the 4th), 6, 3 (two
        // absent, held objects first).
        let sizes: Vec<usize> = lock(&peer.requests).iter().map(Vec::len).collect();
        assert_eq!(sizes, [12, 9, 6, 3]);
        // Only the two the peer lacks are unavailable; nothing failed.
        let unavailable: BTreeSet<Digest> = state.unavailable["f1"].keys().copied().collect();
        let absent: BTreeSet<Digest> = all[10..].iter().map(|o| o.digest).collect();
        assert_eq!(unavailable, absent);
        assert!(state.resting.is_empty());
    }

    // ---- push ------------------------------------------------------------

    /// A listener's bulk lane: lists `demand` and pages `missing` as the
    /// worker does, less what was stored and what is excluded. A store of more
    /// than `stall_above` objects outlasts its deadline and stores nothing. A
    /// digest in `refuse` is refused. With `unpublished`, that repo answers
    /// `REPO_NOT_PUBLISHED`. Also serves `objects.fetch` from `held`, for the
    /// pull passes. `calls` records every request in order.
    #[derive(Default)]
    struct Listener {
        demand: Wanted,
        missing: Wanted,
        held: Vec<Object>,
        refuse: BTreeSet<Digest>,
        stall_above: Option<usize>,
        /// An `objects.fetch` outlasts its deadline.
        stall_fetch: bool,
        unpublished: Option<String>,
        /// Bumped by every store, as a reader's demand arriving meanwhile.
        wake_on_store: Option<Arc<Seq>>,
        stored: Mutex<Vec<(Digest, Kind)>>,
        calls: Mutex<Vec<String>>,
        store_sizes: Mutex<Vec<usize>>,
        excludes: Mutex<Vec<Vec<Digest>>>,
    }

    impl Listener {
        fn calls(&self) -> Vec<String> {
            lock(&self.calls).clone()
        }

        fn stored(&self) -> Vec<Digest> {
            lock(&self.stored)
                .iter()
                .map(|(digest, _)| *digest)
                .collect()
        }

        fn serve(&self, req: Incoming) {
            if self.unpublished.as_deref() == req.header["repo"].as_str() {
                lock(&self.calls).push(req.op.clone());
                req.responder
                    .error(ErrorWire::coded("REPO_NOT_PUBLISHED", "paused"));
                return;
            }
            match req.op.as_str() {
                "objects.wanted" => self.wanted(req),
                "objects.store" => self.store(req),
                "objects.fetch" => {
                    lock(&self.calls).push("fetch".into());
                    if self.stall_fetch {
                        thread::sleep(Duration::from_millis(900));
                        return;
                    }
                    let wanted: Vec<(Digest, Kind)> = serde_json::from_slice(&req.body).unwrap();
                    let objects: Vec<&Object> = wanted
                        .iter()
                        .filter_map(|(digest, _)| self.held.iter().find(|o| o.digest == *digest))
                        .collect();
                    req.responder
                        .reply(json!({}), serde_json::to_vec(&objects).unwrap());
                }
                op => {
                    let error = ErrorWire::coded("UNSUPPORTED_OPERATION", op);
                    req.responder.error(error)
                }
            }
        }

        fn wanted(&self, req: Incoming) {
            let limit = req.header["limit"].as_u64().unwrap() as usize;
            lock(&self.calls).push(format!("wanted/{limit}"));
            let exclude: Vec<Digest> =
                serde_json::from_value(req.header["exclude"].clone()).unwrap();
            let start: usize = req.header["after"]
                .as_str()
                .map_or(0, |after| after.parse().unwrap());
            let stored = self.stored();
            let mut page = WantedPage {
                demand: self
                    .demand
                    .iter()
                    .filter(|(digest, _)| !stored.contains(digest))
                    .copied()
                    .collect(),
                ..WantedPage::default()
            };
            if limit > 0 {
                for (index, object) in self.missing.iter().enumerate().skip(start) {
                    if !exclude.contains(&object.0) && !stored.contains(&object.0) {
                        page.missing.push(*object);
                    }
                    if page.missing.len() == limit {
                        page.cursor = Some((index + 1).to_string());
                        break;
                    }
                }
            }
            lock(&self.excludes).push(exclude);
            req.responder
                .reply(json!({}), serde_json::to_vec(&page).unwrap());
        }

        fn store(&self, req: Incoming) {
            lock(&self.calls).push("store".into());
            let objects: Vec<Object> = serde_json::from_slice(&req.body).unwrap();
            lock(&self.store_sizes).push(objects.len());
            if self.stall_above.is_some_and(|most| objects.len() > most) {
                thread::sleep(Duration::from_millis(900));
                return;
            }
            let mut reply = StoreReply {
                stored: Vec::new(),
                refused: Vec::new(),
            };
            for object in objects {
                if self.refuse.contains(&object.digest) {
                    reply.refused.push(Refused {
                        digest: object.digest,
                        code: "OBJECT_NOT_WANTED".into(),
                    });
                } else {
                    lock(&self.stored).push((object.digest, object.kind));
                    reply.stored.push(object.digest);
                }
            }
            if let Some(wake) = &self.wake_on_store {
                wake.bump();
            }
            req.responder
                .reply(json!({}), serde_json::to_vec(&reply).unwrap());
        }

        /// A bulk lane to this listener whose peer advertises `features`.
        fn lane_with(self: &Arc<Self>, session: &str, features: &[&str]) -> Conn {
            let listener = self.clone();
            let info = PeerInfo {
                session: session.into(),
                features: features.iter().map(|f| f.to_string()).collect(),
                ..info_as(LaneRole::Bulk)
            };
            let (client, server) = served_info(
                info,
                Arc::new(FnHandler(move |req: Incoming| listener.serve(req))),
            );
            std::mem::forget(server);
            client
        }

        /// A bulk lane to this listener, which takes pushes.
        fn lane(self: &Arc<Self>) -> OneLane {
            OneLane(self.lane_with("session", &[features::CONTENT_PUSH]))
        }
    }

    /// A folder of `repo` whose worker holds `held`.
    fn pushing_folder(repo: &str, held: &[Object]) -> SyncFolder {
        let fake = Arc::new(FakeFolder {
            held: held.to_vec(),
            ..FakeFolder::default()
        });
        SyncFolder {
            id: format!("folder-{repo}"),
            repo: repo.into(),
            link: fake_worker(fake),
            demand: Vec::new(),
        }
    }

    const ENDPOINT: &str = "ws://peer";

    fn push_once(
        lanes: &dyn Lanes,
        state: &mut PushState,
        folder: &SyncFolder,
        mode: PushMode,
    ) -> push::PushOutcome {
        let endpoints = [ENDPOINT.to_owned()];
        push::run(lanes, state, &endpoints, folder, mode, None, &Cancel::new())
    }

    fn push_peer<'a>(state: &'a mut PushState, folder: &SyncFolder) -> &'a mut push::PushPeer {
        state
            .peers
            .get_mut(&(folder.id.clone(), ENDPOINT.to_owned()))
            .unwrap()
    }

    #[test]
    fn the_pump_runs_demand_pull_then_demand_push_then_background() {
        let wanted_here = objects("wanted-here", 1);
        let missing_here = objects("missing-here", 1);
        let wanted_there = objects("wanted-there", 1);
        let missing_there = objects("missing-there", 1);
        let folder = Arc::new(FakeFolder {
            missing: wanted_of(&missing_here),
            held: [wanted_there.clone(), missing_there.clone()].concat(),
            ..FakeFolder::default()
        });
        let listener = Arc::new(Listener {
            demand: wanted_of(&wanted_there),
            missing: wanted_of(&missing_there),
            held: [wanted_here.clone(), missing_here.clone()].concat(),
            ..Listener::default()
        });
        let link = fake_worker(folder.clone());
        let folders = Arc::new(Folders(vec![(
            "f1".into(),
            link,
            Mutex::new(wanted_of(&wanted_here)),
        )]));
        let (driver, _root) = driver(listener.lane(), folders, Seq::new());
        let mut state = PumpState::default();
        // Two pulled, two pushed.
        assert_eq!(driver.pump_round(&mut state, driver.wake.current()), 4);
        assert_eq!(
            listener.calls(),
            ["fetch", "wanted/0", "store", "fetch", "wanted/32", "store"]
        );
        assert_eq!(
            listener.stored(),
            [wanted_there[0].digest, missing_there[0].digest]
        );
    }

    #[test]
    fn a_push_round_stops_before_its_next_call_when_demand_arrives() {
        // Two stores' worth, and demand here arrives during the first.
        let all = objects("object", 2 * OBJECT_BATCH);
        let wake = Seq::new();
        let listener = Arc::new(Listener {
            demand: wanted_of(&all[..OBJECT_BATCH]),
            missing: wanted_of(&all[OBJECT_BATCH..]),
            wake_on_store: Some(wake.clone()),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let folder = pushing_folder("repo-1", &all);
        let mut state = PushState::default();
        let endpoints = [ENDPOINT.to_owned()];
        let cancel = Cancel::new();
        let seen = wake.current();
        let outcome = push::run(
            &lanes,
            &mut state,
            &endpoints,
            &folder,
            PushMode::Background,
            Some((wake.as_ref(), seen)),
            &cancel,
        );
        assert!(outcome.interrupted);
        assert!(outcome.error.is_none());
        assert_eq!(outcome.pushed, OBJECT_BATCH);
        assert_eq!(outcome.remaining, OBJECT_BATCH);
        assert_eq!(listener.calls(), ["wanted/32", "store"]);
        // The page was not finished: the scan stays on it.
        assert_eq!(push_peer(&mut state, &folder).scan, None);
        // A wake that already moved stops it before any call.
        let outcome = push::run(
            &lanes,
            &mut state,
            &endpoints,
            &folder,
            PushMode::Background,
            Some((wake.as_ref(), seen)),
            &cancel,
        );
        assert!(outcome.interrupted);
        assert_eq!(listener.calls().len(), 2);
        // The next round finishes the job.
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, OBJECT_BATCH);
        assert_eq!(listener.stored().len(), 2 * OBJECT_BATCH);
    }

    #[test]
    fn a_listener_without_content_push_is_never_pushed_to() {
        let all = objects("object", 2);
        let listener = Arc::new(Listener {
            demand: wanted_of(&all[..1]),
            missing: wanted_of(&all[1..]),
            ..Listener::default()
        });
        let lanes = OneLane(listener.lane_with("session", &[features::NOTIFY]));
        let folder = pushing_folder("repo-1", &all);
        let mut state = PushState::default();
        for mode in [PushMode::Demand, PushMode::Background] {
            let outcome = push_once(&lanes, &mut state, &folder, mode);
            assert_eq!(outcome.pushed, 0);
            assert!(outcome.error.is_none());
        }
        assert!(listener.calls().is_empty(), "{:?}", listener.calls());
    }

    #[test]
    fn a_digest_this_side_lacks_is_backed_off_and_not_an_error() {
        let all = objects("object", 3);
        let listener = Arc::new(Listener {
            demand: wanted_of(&all[..1]),
            missing: wanted_of(&all[1..]),
            ..Listener::default()
        });
        let lanes = listener.lane();
        // This side holds only the middle one: part of what is wanted.
        let folder = pushing_folder("repo-1", &all[1..2]);
        let mut state = PushState::default();
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, 1);
        assert_eq!(outcome.remaining, 2);
        assert!(outcome.error.is_none());
        assert_eq!(listener.stored(), [all[1].digest]);
        let peer = push_peer(&mut state, &folder);
        assert!(peer.counters.last_error.is_none());
        let lacks: BTreeSet<Digest> = peer.lacks.keys().copied().collect();
        let expected: BTreeSet<Digest> = [all[0].digest, all[2].digest].into();
        assert_eq!(lacks, expected);
        assert!(peer.lacks.values().all(|u| u.backoff == UNAVAILABLE_FIRST));

        // Meanwhile they are excluded from the listing, and not read: demand
        // is not excluded by the listener, so it is skipped here.
        let calls = listener.calls().len();
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, 0);
        assert!(outcome.error.is_none());
        assert_eq!(lock(&listener.excludes).last().unwrap().len(), 2);
        assert_eq!(listener.calls().len(), calls + 1, "{:?}", listener.calls());

        // Lacked again after the backoff: it doubles.
        let peer = push_peer(&mut state, &folder);
        for entry in peer.lacks.values_mut() {
            entry.until = Instant::now();
        }
        push_once(&lanes, &mut state, &folder, PushMode::Background);
        let peer = push_peer(&mut state, &folder);
        assert_eq!(peer.lacks.len(), 2);
        assert!(
            peer.lacks
                .values()
                .all(|u| u.backoff == UNAVAILABLE_FIRST * 2)
        );
        assert!(peer.lacks.values().all(|u| u.until > Instant::now()));
        // Doubling stops at the bound.
        for entry in peer.lacks.values_mut() {
            entry.until = Instant::now();
            entry.backoff = UNAVAILABLE_MAX;
        }
        push_once(&lanes, &mut state, &folder, PushMode::Background);
        let peer = push_peer(&mut state, &folder);
        assert!(peer.lacks.values().all(|u| u.backoff == UNAVAILABLE_MAX));
    }

    #[test]
    fn a_timed_out_store_halves_the_batch_and_answers_grow_it() {
        let all = objects("slow-link", OBJECT_BATCH);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            stall_above: Some(16),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let folder = pushing_folder("repo-1", &all);
        let mut state = PushState {
            deadline: |_| Duration::from_millis(300),
            ..PushState::default()
        };
        // The full batch times out: nothing is stored, the batch halves, and
        // the whole endpoint rests (the lane is not answering).
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, 0);
        assert_eq!(outcome.remaining, OBJECT_BATCH);
        assert!(outcome.error.is_some());
        assert_eq!(push_peer(&mut state, &folder).batch, OBJECT_BATCH / 2);
        assert!(push_peer(&mut state, &folder).lacks.is_empty());
        assert!(state.resting.contains_key(ENDPOINT));
        // While it rests, nothing is asked. Uncertain: not replayed.
        let calls = listener.calls().len();
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert!(outcome.error.is_none());
        assert_eq!(listener.calls().len(), calls);

        let end = Instant::now() + Duration::from_secs(30);
        while listener.stored().len() < OBJECT_BATCH {
            assert!(Instant::now() < end, "{:?}", listener.calls());
            state.resting.clear();
            let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
            assert!(outcome.error.is_none(), "{:?}", outcome.error);
        }
        // Every store after the timeout was within the halved size, and the
        // answered ones grew it back.
        let sizes = lock(&listener.store_sizes).clone();
        assert_eq!(sizes[0], OBJECT_BATCH);
        assert_eq!(sizes[1..], [16, 16], "{sizes:?}");
        assert_eq!(push_peer(&mut state, &folder).batch, 16 + 4 + 5);
    }

    #[test]
    fn a_read_cut_to_its_bounds_is_one_store_and_the_rest_is_read_next() {
        let all = objects("cut", 10);
        let fake = Arc::new(FakeFolder {
            held: all[..8].to_vec(),
            read_cut: Some(3),
            ..FakeFolder::default()
        });
        let folder = SyncFolder {
            id: "f1".into(),
            repo: "repo-1".into(),
            link: fake_worker(fake),
            demand: Vec::new(),
        };
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let mut state = PushState::default();
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, 8);
        assert_eq!(*lock(&listener.store_sizes), [3, 3, 2]);
        // Only what was looked at and found absent is lacked.
        let lacks: BTreeSet<Digest> = push_peer(&mut state, &folder)
            .lacks
            .keys()
            .copied()
            .collect();
        let absent: BTreeSet<Digest> = all[8..].iter().map(|o| o.digest).collect();
        assert_eq!(lacks, absent);
    }

    #[test]
    fn a_refused_digest_is_dropped_from_the_round_and_listed_again() {
        let all = objects("object", 3);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            refuse: [all[1].digest].into(),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let folder = pushing_folder("repo-1", &all);
        let mut state = PushState::default();
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, 2);
        assert_eq!(outcome.remaining, 1);
        assert!(outcome.error.is_none());
        assert_eq!(*lock(&listener.store_sizes), [3]);
        let peer = push_peer(&mut state, &folder);
        assert!(peer.lacks.is_empty());
        assert_eq!(peer.counters.pushed, 2);
        assert_eq!(peer.counters.refused, 1);
        // Still wanted: listed and offered again next round.
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, 0);
        assert_eq!(outcome.remaining, 1);
        assert_eq!(*lock(&listener.store_sizes), [3, 1]);
    }

    /// Digests go to the lane their first byte's parity names.
    struct TwoLanes(Conn, Conn);

    impl Lanes for TwoLanes {
        fn ready_peers(&self) -> Vec<ReadyPeer> {
            Vec::new()
        }
        fn bulk_endpoints(&self) -> Vec<String> {
            vec![ENDPOINT.into()]
        }
        fn bulk_lane(&self, _: &str, digest: &Digest) -> Option<Conn> {
            Some(
                if digest.0[0].is_multiple_of(2) {
                    &self.0
                } else {
                    &self.1
                }
                .clone(),
            )
        }
        fn any_bulk_lane(&self, _: &str) -> Option<Conn> {
            Some(self.0.clone())
        }
    }

    #[test]
    fn a_push_stores_each_digest_on_its_lane_with_the_listed_kind_and_demand_first() {
        let mut all = objects("object", 12);
        all[0].kind = Kind::Manifest;
        let owned = |lane: u8, of: &[Object]| -> Vec<Digest> {
            of.iter()
                .map(|o| o.digest)
                .filter(|digest| digest.0[0] % 2 == lane)
                .collect()
        };
        let lists = Arc::new(Listener {
            demand: wanted_of(&all[..4]),
            missing: wanted_of(&all[4..]),
            ..Listener::default()
        });
        // Each lane records its own stores; only the first is asked to list.
        let other = Arc::new(Listener::default());
        let lanes = TwoLanes(
            lists.lane_with("lane-0", &[features::CONTENT_PUSH]),
            other.lane_with("lane-1", &[features::CONTENT_PUSH]),
        );
        let folder = pushing_folder("repo-1", &all);
        let mut state = PushState::default();
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, 12);
        // Demand went first on each lane, in listed order.
        let expected = |lane| [owned(lane, &all[..4]), owned(lane, &all[4..])].concat();
        assert_eq!(lists.stored(), expected(0));
        assert_eq!(other.stored(), expected(1));
        // The kind the listing gave, not one assumed from the read.
        let kinds: BTreeMap<Digest, Kind> = lock(&lists.stored)
            .iter()
            .chain(lock(&other.stored).iter())
            .copied()
            .collect();
        assert_eq!(kinds[&all[0].digest], Kind::Manifest);
        assert_eq!(kinds[&all[1].digest], Kind::Chunk);
    }

    #[test]
    fn a_repo_the_listener_does_not_publish_rests_alone_and_is_logged_once() {
        let all = objects("object", 1);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            unpublished: Some("paused-repo".into()),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let paused = pushing_folder("paused-repo", &all);
        let open = pushing_folder("repo-1", &all);
        let mut state = PushState::default();
        for mode in [PushMode::Demand, PushMode::Background] {
            let outcome = push_once(&lanes, &mut state, &paused, mode);
            // The first call rests it; the second is not even sent.
            assert_eq!(outcome.error.is_some(), mode == PushMode::Demand);
        }
        assert_eq!(listener.calls(), ["objects.wanted"]);
        let peer = push_peer(&mut state, &paused);
        let logged = peer.counters.last_error.clone().unwrap();
        assert!(logged.contains("REPO_NOT_PUBLISHED"), "{logged}");
        assert!(!state.resting.contains_key(ENDPOINT));

        // The other folder of that endpoint is not kept from pushing.
        let outcome = push_once(&lanes, &mut state, &open, PushMode::Background);
        assert_eq!(outcome.pushed, 1);

        // Its rest ended, it fails alike: the error stays as logged.
        push_peer(&mut state, &paused).rest_until = None;
        let outcome = push_once(&lanes, &mut state, &paused, PushMode::Demand);
        assert_eq!(outcome.error.as_ref(), Some(&logged));
    }

    #[test]
    fn a_failing_store_is_not_cleared_by_the_clean_listing_of_a_later_round() {
        let all = objects("object", 1);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            stall_above: Some(0),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let folder = pushing_folder("repo-1", &all);
        let mut state = PushState {
            deadline: |_| Duration::from_millis(100),
            ..PushState::default()
        };
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        let logged = outcome.error.unwrap();
        state.resting.clear();
        // A Demand pass with nothing to do lists cleanly.
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Demand);
        assert!(outcome.error.is_none());
        let peer = push_peer(&mut state, &folder);
        assert_eq!(peer.counters.last_error.as_ref(), Some(&logged));
    }

    #[test]
    fn what_a_push_remembers_outlasts_a_busy_round_and_not_long_idleness() {
        let all = objects("object", 1);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let lacking = pushing_folder("lacking", &[]);
        let other = pushing_folder("other", &all);
        let mut state = PushState::default();
        push_once(&lanes, &mut state, &lacking, PushMode::Background);
        // Another folder's pass, with this one absent, keeps its backoff.
        push_once(&lanes, &mut state, &other, PushMode::Background);
        assert_eq!(push_peer(&mut state, &lacking).lacks.len(), 1);
        // Unused for long, it goes.
        let long_ago = Instant::now()
            .checked_sub(UNAVAILABLE_MAX * 2 + Duration::from_secs(1))
            .expect("the monotonic clock starts early enough");
        push_peer(&mut state, &lacking).touched = long_ago;
        push_once(&lanes, &mut state, &other, PushMode::Background);
        assert!(state.counters(&lacking.id, ENDPOINT).is_none());
        assert!(state.counters(&other.id, ENDPOINT).is_some());
    }

    #[test]
    fn a_wake_during_the_read_stops_a_background_push_before_its_store() {
        let all = objects("object", 2);
        let wake = Seq::new();
        let fake = Arc::new(FakeFolder {
            held: all.clone(),
            wake_on_read: Some(wake.clone()),
            ..FakeFolder::default()
        });
        let folder = SyncFolder {
            id: "f1".into(),
            repo: "repo-1".into(),
            link: fake_worker(fake),
            demand: Vec::new(),
        };
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let mut state = PushState::default();
        let endpoints = [ENDPOINT.to_owned()];
        let seen = wake.current();
        let outcome = push::run(
            &lanes,
            &mut state,
            &endpoints,
            &folder,
            PushMode::Background,
            Some((wake.as_ref(), seen)),
            &Cancel::new(),
        );
        assert!(outcome.interrupted);
        assert!(outcome.error.is_none());
        assert_eq!(outcome.pushed, 0);
        assert_eq!(outcome.remaining, 2);
        assert_eq!(listener.calls(), ["wanted/32"]);
        // This side does hold them, and they stay missing: listed again.
        let peer = push_peer(&mut state, &folder);
        assert!(peer.lacks.is_empty());
        assert_eq!(peer.scan, None);
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, 2);
    }

    /// Running folders, each of its own repo (its id here), with set demand.
    struct RepoFolders(Vec<(String, Arc<WorkerLink>, Wanted)>);

    impl SyncFolders for RepoFolders {
        fn syncing(&self) -> Vec<SyncFolder> {
            self.0
                .iter()
                .map(|(repo, link, demand)| SyncFolder {
                    id: repo.clone(),
                    repo: repo.clone(),
                    link: link.clone(),
                    demand: demand.clone(),
                })
                .collect()
        }
    }

    const UNPUBLISHED: &str = "unpublished-repo";

    /// A folder of [`UNPUBLISHED`] that lists `missing` and whose readers wait
    /// for `demand`, then one of `repo-1` that holds `held`.
    fn unpublished_then_open(
        missing: &[Object],
        demand: &[Object],
        held: &[Object],
    ) -> Arc<RepoFolders> {
        let unpublished = Arc::new(FakeFolder {
            missing: wanted_of(missing),
            ..FakeFolder::default()
        });
        let open = Arc::new(FakeFolder {
            held: held.to_vec(),
            ..FakeFolder::default()
        });
        Arc::new(RepoFolders(vec![
            (
                UNPUBLISHED.into(),
                fake_worker(unpublished),
                wanted_of(demand),
            ),
            ("repo-1".into(), fake_worker(open), Vec::new()),
        ]))
    }

    #[test]
    fn a_coded_pull_failure_of_one_folder_does_not_starve_the_pushes_of_the_others() {
        // More than a page to push, so a second round has work.
        let there = objects("there", OBJECT_BATCH + 8);
        let listener = Arc::new(Listener {
            missing: wanted_of(&there),
            unpublished: Some(UNPUBLISHED.into()),
            ..Listener::default()
        });
        let folders = unpublished_then_open(&objects("here", 1), &[], &there);
        let (driver, _root) = driver(listener.lane(), folders, Seq::new());
        let mut state = PumpState::default();
        let seen = driver.wake.current();
        // The pull of the first folder fails: the pull passes rest, the second
        // folder's background push goes on in this very round.
        assert_eq!(driver.pump_round(&mut state, seen), OBJECT_BATCH);
        assert!(state.resting.contains_key(ENDPOINT));
        assert!(!state.push.resting.contains_key(ENDPOINT));
        // The round pull sits out holds no call of it, and still pushes.
        assert_eq!(driver.pump_round(&mut state, seen), 8);
        assert_eq!(listener.stored().len(), there.len());
        let fetches = listener
            .calls()
            .iter()
            .filter(|call| *call == "objects.fetch")
            .count();
        assert_eq!(fetches, 1, "{:?}", listener.calls());
    }

    #[test]
    fn a_blocked_reader_of_one_folder_does_not_skip_the_demand_push_of_the_others() {
        let wanted_there = objects("wanted-there", 1);
        let listener = Arc::new(Listener {
            demand: wanted_of(&wanted_there),
            unpublished: Some(UNPUBLISHED.into()),
            ..Listener::default()
        });
        let folders = unpublished_then_open(&[], &objects("here", 1), &wanted_there);
        let (driver, _root) = driver(listener.lane(), folders, Seq::new());
        let mut state = PumpState::default();
        assert_eq!(driver.pump_round(&mut state, driver.wake.current()), 1);
        assert_eq!(listener.stored(), [wanted_there[0].digest]);
        assert_eq!(
            listener.calls(),
            [
                "objects.fetch",
                "objects.wanted",
                "wanted/0",
                "store",
                "wanted/32"
            ]
        );
    }

    #[test]
    fn a_pull_that_timed_out_rests_the_push_steps_too() {
        let here = objects("here", 1);
        let there = objects("there", 1);
        let listener = Arc::new(Listener {
            missing: wanted_of(&there),
            stall_fetch: true,
            ..Listener::default()
        });
        let fake = Arc::new(FakeFolder {
            missing: wanted_of(&here),
            held: there.clone(),
            ..FakeFolder::default()
        });
        let folders = Arc::new(RepoFolders(vec![(
            "repo-1".into(),
            fake_worker(fake),
            Vec::new(),
        )]));
        let (mut driver, _root) = driver(listener.lane(), folders, Seq::new());
        Arc::get_mut(&mut driver).unwrap().fetch_deadline = |_| Duration::from_millis(100);
        let mut state = PumpState::default();
        let seen = driver.wake.current();
        // The lane is not answering: no more calls to it in this round, nor in
        // the next, whichever step would make them.
        assert_eq!(driver.pump_round(&mut state, seen), 0);
        assert_eq!(listener.calls(), ["wanted/0", "fetch"]);
        assert!(state.push.resting.contains_key(ENDPOINT));
        assert_eq!(driver.pump_round(&mut state, seen), 0);
        assert_eq!(listener.calls(), ["wanted/0", "fetch"]);
    }

    #[test]
    fn the_push_scan_stays_on_a_stored_page_and_moves_past_any_other() {
        let all = objects("object", OBJECT_BATCH + 2);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            ..Listener::default()
        });
        let lanes = listener.lane();
        // The first page is held whole.
        let folder = pushing_folder("repo-1", &all[..OBJECT_BATCH]);
        let mut state = PushState::default();
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, OBJECT_BATCH);
        assert_eq!(push_peer(&mut state, &folder).scan, None);
        // The next page is short and lacked: the scan reached its end.
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, 0);
        assert_eq!(outcome.remaining, 2);
        assert_eq!(push_peer(&mut state, &folder).scan, None);
        assert_eq!(push_peer(&mut state, &folder).lacks.len(), 2);

        // A full page not wholly stored moves the scan on.
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let folder = pushing_folder("repo-1", &all[1..OBJECT_BATCH]);
        let mut state = PushState::default();
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, OBJECT_BATCH - 1);
        assert_eq!(
            push_peer(&mut state, &folder).scan.as_deref(),
            Some(OBJECT_BATCH.to_string().as_str())
        );
    }

    // ---- what is due -----------------------------------------------------

    const HOUR: Duration = Duration::from_secs(3600);
    /// Longer than the [`SHORT`] interval, by a margin no scheduler hiccup in
    /// these tests needs to beat: nothing here waits for an exact moment.
    const PAST_SHORT: Duration = Duration::from_millis(40);
    const SHORT: Duration = Duration::from_millis(10);

    fn active(interval: Duration, backup_poll: Duration) -> SyncPolicy {
        SyncPolicy {
            profile: SyncProfile::Active,
            interval,
            backup_poll,
        }
    }

    /// A control lane no test sends anything on.
    fn idle_control() -> Conn {
        let (client, server) = served(Arc::new(SkewedPeer {
            skew_ms: Mutex::new(0),
        }));
        std::mem::forget(server);
        client
    }

    fn ready_peer(endpoint: &str, session: &str, notify: bool, control: &Conn) -> ReadyPeer {
        ReadyPeer {
            endpoint: endpoint.into(),
            installation: "peer-b".into(),
            session: session.into(),
            control: control.clone(),
            notify,
        }
    }

    /// The (endpoint, session, notify) peers with a Ready control lane, and one
    /// bulk lane all of them use.
    struct ReadyLanes {
        peers: Vec<(String, String, bool)>,
        control: Conn,
        bulk: Option<Conn>,
    }

    impl ReadyLanes {
        fn one(notify: bool, bulk: Conn) -> Self {
            Self {
                peers: vec![(ENDPOINT.into(), "control".into(), notify)],
                control: idle_control(),
                bulk: Some(bulk),
            }
        }
    }

    impl Lanes for ReadyLanes {
        fn ready_peers(&self) -> Vec<ReadyPeer> {
            self.peers
                .iter()
                .map(|(endpoint, session, notify)| {
                    ready_peer(endpoint, session, *notify, &self.control)
                })
                .collect()
        }
        fn bulk_endpoints(&self) -> Vec<String> {
            self.bulk
                .iter()
                .flat_map(|_| self.peers.iter().map(|(endpoint, _, _)| endpoint.clone()))
                .collect()
        }
        fn bulk_lane(&self, _: &str, _: &Digest) -> Option<Conn> {
            self.bulk.clone()
        }
        fn any_bulk_lane(&self, _: &str) -> Option<Conn> {
            self.bulk.clone()
        }
    }

    fn policy_driver(
        lanes: impl Lanes + 'static,
        repos: &[&str],
        policy: SyncPolicy,
    ) -> (Arc<Driver>, tempfile::TempDir) {
        let folders = repos
            .iter()
            .map(|repo| {
                let fake = Arc::new(FakeFolder::default());
                ((*repo).to_owned(), fake_worker(fake), Vec::new())
            })
            .collect();
        let root = tempfile::tempdir().unwrap();
        let driver = Driver::with_policy(
            Arc::new(lanes),
            Catalog::open(root.path()).unwrap(),
            Arc::new(RepoFolders(folders)),
            Seq::new(),
            policy,
            Cancel::new(),
        );
        (driver, root)
    }

    /// One dispatch: the pairs it started, each finished with nothing moved.
    fn dispatched(driver: &Driver, peers: &[ReadyPeer]) -> Vec<Pair> {
        let (tasks, queue) = mpsc::channel();
        driver.dispatch(1, peers, &tasks);
        let mut pairs: Vec<Pair> = queue
            .try_iter()
            .map(|task| {
                driver.finish(&task, task.cursor, Ok(()));
                task.key
            })
            .collect();
        pairs.sort();
        pairs
    }

    fn pair(endpoint: &str, repo: &str) -> Pair {
        (endpoint.to_owned(), repo.to_owned())
    }

    fn metadata(repo: &str) -> Notify {
        Notify {
            repo: repo.into(),
            reason: NotifyReason::Metadata,
        }
    }

    fn demand_of(repo: &str) -> Notify {
        Notify {
            repo: repo.into(),
            reason: NotifyReason::Demand,
        }
    }

    /// A driver of one repo with no bulk lane, for what `dispatch` does.
    fn dispatching(policy: SyncPolicy) -> (Arc<Driver>, tempfile::TempDir, Conn) {
        let control = idle_control();
        let lanes = ReadyLanes {
            peers: Vec::new(),
            control: control.clone(),
            bulk: None,
        };
        let (driver, root) = policy_driver(lanes, &["repo-1"], policy);
        (driver, root, control)
    }

    #[test]
    fn a_notify_session_waits_for_the_backup_poll_when_nothing_changed() {
        let (driver, _root, control) = dispatching(active(SHORT, HOUR));
        let peers = [ready_peer(ENDPOINT, "control", true, &control)];
        // New: due once.
        assert_eq!(dispatched(&driver, &peers), [pair(ENDPOINT, "repo-1")]);
        assert!(dispatched(&driver, &peers).is_empty());
        // The sync interval passing is not its poll.
        thread::sleep(PAST_SHORT);
        assert!(dispatched(&driver, &peers).is_empty());
        // The backup poll is.
        let key = pair(ENDPOINT, "repo-1");
        lock(&driver.jobs).get_mut(&key).unwrap().next_poll = Some(Instant::now());
        assert_eq!(dispatched(&driver, &peers), [pair(ENDPOINT, "repo-1")]);
        assert!(dispatched(&driver, &peers).is_empty());
    }

    #[test]
    fn a_session_without_notify_keeps_the_sync_interval() {
        let (driver, _root, control) = dispatching(active(SHORT, HOUR));
        let peers = [ready_peer(ENDPOINT, "control", false, &control)];
        assert_eq!(dispatched(&driver, &peers).len(), 1);
        thread::sleep(PAST_SHORT);
        assert_eq!(dispatched(&driver, &peers).len(), 1);
        thread::sleep(PAST_SHORT);
        assert_eq!(dispatched(&driver, &peers).len(), 1);
    }

    #[test]
    fn a_notify_marks_only_its_peer_and_repo_due() {
        let control = idle_control();
        let lanes = ReadyLanes {
            peers: Vec::new(),
            control: control.clone(),
            bulk: None,
        };
        let (driver, _root) = policy_driver(lanes, &["repo-1", "repo-2"], active(HOUR, HOUR));
        let peers = [
            ready_peer("ws://one", "control-1", true, &control),
            ready_peer("ws://two", "control-2", true, &control),
        ];
        assert_eq!(dispatched(&driver, &peers).len(), 4);
        assert!(dispatched(&driver, &peers).is_empty());

        let (poke, wake) = (driver.poke.current(), driver.wake.current());
        driver.notified("ws://one", &metadata("repo-2"));
        assert!(driver.poke.current() > poke);
        assert_eq!(driver.wake.current(), wake);
        assert_eq!(dispatched(&driver, &peers), [pair("ws://one", "repo-2")]);
        assert!(dispatched(&driver, &peers).is_empty());

        // Demand is the pump's business, not the scheduler's.
        driver.notified("ws://two", &demand_of("repo-1"));
        assert!(driver.wake.current() > wake);
        assert!(dispatched(&driver, &peers).is_empty());
        let due = lock(&driver.due);
        assert_eq!(
            due.demand.iter().cloned().collect::<Vec<_>>(),
            [pair("ws://two", "repo-1")]
        );
        assert!(due.background.is_empty());
    }

    #[test]
    fn a_new_control_session_is_always_due() {
        let (driver, _root, control) = dispatching(active(HOUR, HOUR));
        let first = [ready_peer(ENDPOINT, "control", true, &control)];
        assert_eq!(dispatched(&driver, &first).len(), 1);
        let key = pair(ENDPOINT, "repo-1");
        lock(&driver.jobs).get_mut(&key).unwrap().job.cursor = FeedCursor {
            sent: 5,
            received: 7,
        };
        assert!(dispatched(&driver, &first).is_empty());
        // The lanes came back: due, from the start.
        let second = [ready_peer(ENDPOINT, "control-again", true, &control)];
        let (tasks, queue) = mpsc::channel();
        driver.dispatch(1, &second, &tasks);
        let task = queue.try_recv().expect("the new session was not due");
        assert_eq!(task.cursor, FeedCursor::default());
        driver.finish(&task, task.cursor, Ok(()));
        assert!(dispatched(&driver, &second).is_empty());
    }

    #[test]
    fn a_mark_that_finds_the_pair_busy_or_away_waits_for_it() {
        let (driver, _root, control) = dispatching(active(HOUR, HOUR));
        let peers = [ready_peer(ENDPOINT, "control", true, &control)];
        let (tasks, queue) = mpsc::channel();
        driver.dispatch(1, &peers, &tasks);
        let task = queue.try_recv().unwrap();
        // It arrives during the exchange: the exchange may be past it already.
        driver.notified(ENDPOINT, &metadata("repo-1"));
        driver.dispatch(1, &peers, &tasks);
        assert!(queue.try_recv().is_err());
        let poke = driver.poke.current();
        driver.finish(&task, task.cursor, Ok(()));
        assert!(driver.poke.current() > poke);
        assert_eq!(dispatched(&driver, &peers).len(), 1);
        // It arrives with no Ready peer: the next one to show up is told.
        driver.notified(ENDPOINT, &metadata("repo-1"));
        assert!(dispatched(&driver, &[]).is_empty());
        assert_eq!(dispatched(&driver, &peers).len(), 1);
    }

    #[test]
    fn a_failed_exchange_does_not_wait_for_the_backup_poll() {
        let (driver, _root, control) = dispatching(active(SHORT, HOUR));
        let peers = [ready_peer(ENDPOINT, "control", true, &control)];
        let (tasks, queue) = mpsc::channel();
        driver.dispatch(1, &peers, &tasks);
        let task = queue.try_recv().unwrap();
        driver.finish(&task, task.cursor, Err(Failure::LaneLost));
        thread::sleep(PAST_SHORT);
        assert_eq!(dispatched(&driver, &peers).len(), 1);
        // Once it works the backup poll applies again.
        thread::sleep(PAST_SHORT);
        assert!(dispatched(&driver, &peers).is_empty());
    }

    #[test]
    fn a_local_change_marks_that_repo_for_every_peer() {
        let control = idle_control();
        let lanes = ReadyLanes {
            peers: vec![
                ("ws://one".into(), "control-1".into(), true),
                ("ws://two".into(), "control-2".into(), true),
            ],
            control: control.clone(),
            bulk: None,
        };
        let peers = lanes.ready_peers();
        let (driver, _root) = policy_driver(lanes, &["repo-1", "repo-2"], active(HOUR, HOUR));
        assert_eq!(dispatched(&driver, &peers).len(), 4);
        let (poke, wake) = (driver.poke.current(), driver.wake.current());
        driver.local_change("repo-1");
        assert!(driver.poke.current() > poke);
        assert!(driver.wake.current() > wake);
        let expected = [pair("ws://one", "repo-1"), pair("ws://two", "repo-1")];
        assert_eq!(dispatched(&driver, &peers), expected);
        assert_eq!(
            lock(&driver.due)
                .background
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            expected
        );
    }

    /// One pump round, and the listener's calls it made.
    fn calls_of_round(driver: &Driver, state: &mut PumpState, listener: &Listener) -> Vec<String> {
        let before = listener.calls().len();
        driver.pump_round(state, driver.wake.current());
        listener.calls()[before..].to_vec()
    }

    /// A driver of one repo over a bulk lane to `listener`.
    fn pushing_driver(
        listener: &Arc<Listener>,
        notify: bool,
        policy: SyncPolicy,
    ) -> (Arc<Driver>, tempfile::TempDir) {
        let bulk = listener.lane_with("session", &[features::CONTENT_PUSH]);
        policy_driver(ReadyLanes::one(notify, bulk), &["repo-1"], policy)
    }

    #[test]
    fn an_idle_pair_asks_nothing_until_something_marks_it_due() {
        let listener = Arc::new(Listener::default());
        let (driver, _root) = pushing_driver(&listener, true, active(SHORT, HOUR));
        let mut state = PumpState::default();
        // New: its first poll.
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["wanted/0", "wanted/32"]
        );
        // Idle, past the sync interval: nothing.
        for _ in 0..3 {
            thread::sleep(PAST_SHORT);
            assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
        }
        // A metadata notify is for the scheduler.
        driver.notified(ENDPOINT, &metadata("repo-1"));
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
        // A demand notify runs only the demand step.
        driver.notified(ENDPOINT, &demand_of("repo-1"));
        assert_eq!(calls_of_round(&driver, &mut state, &listener), ["wanted/0"]);
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
        // A local change runs only the background step.
        driver.local_change("repo-1");
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["wanted/32"]
        );
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
        // So does an exchange, once it moved events.
        let peers = driver.lanes.ready_peers();
        lock(&driver.due).exchange.insert(pair(ENDPOINT, "repo-1"));
        let (tasks, queue) = mpsc::channel();
        driver.dispatch(1, &peers, &tasks);
        let task = queue.try_recv().unwrap();
        driver.finish(&task, task.cursor, Ok(()));
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
        lock(&driver.due).exchange.insert(pair(ENDPOINT, "repo-1"));
        driver.dispatch(1, &peers, &tasks);
        let task = queue.try_recv().unwrap();
        let wake = driver.wake.current();
        let moved = FeedCursor {
            sent: task.cursor.sent + 1,
            ..task.cursor
        };
        driver.finish(&task, moved, Ok(()));
        assert!(driver.wake.current() > wake);
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["wanted/32"]
        );
        // And its poll runs both.
        state.polls.insert(pair(ENDPOINT, "repo-1"), Instant::now());
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["wanted/0", "wanted/32"]
        );
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
    }

    #[test]
    fn a_session_without_notify_keeps_the_sync_interval_for_its_pushes_too() {
        let listener = Arc::new(Listener::default());
        let (driver, _root) = pushing_driver(&listener, false, active(SHORT, HOUR));
        let mut state = PumpState::default();
        assert_eq!(calls_of_round(&driver, &mut state, &listener).len(), 2);
        thread::sleep(PAST_SHORT);
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["wanted/0", "wanted/32"]
        );
    }

    #[test]
    fn a_pair_stays_due_until_a_page_lists_nothing() {
        // This side lacks what is listed: it goes in the next listing's exclude.
        let lacked = objects("lacked", 1);
        let listener = Arc::new(Listener {
            missing: wanted_of(&lacked),
            ..Listener::default()
        });
        let (driver, _root) = pushing_driver(&listener, true, active(HOUR, HOUR));
        let mut state = PumpState::default();
        assert_eq!(calls_of_round(&driver, &mut state, &listener).len(), 2);
        assert!(
            lock(&driver.due)
                .background
                .contains(&pair(ENDPOINT, "repo-1"))
        );
        assert_eq!(driver.pump_wait(), DUE_CHECK);
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["wanted/32"]
        );
        assert!(lock(&driver.due).background.is_empty());
        assert_eq!(driver.pump_wait(), HOUR);
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
    }

    #[test]
    fn a_listing_that_failed_waits_for_the_next_trigger() {
        let listener = Arc::new(Listener {
            unpublished: Some("repo-1".into()),
            ..Listener::default()
        });
        let (driver, _root) = pushing_driver(&listener, true, active(HOUR, HOUR));
        let mut state = PumpState::default();
        let key = ("repo-1".to_owned(), ENDPOINT.to_owned());
        // The demand step fails and rests the pair: the background step keeps
        // its mark, and nothing is asked until the rest ends.
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["objects.wanted"]
        );
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
        assert!(!lock(&driver.due).background.is_empty());
        // Then it fails too, and that ends it: no page, nothing left to do.
        state.push.peers.get_mut(&key).unwrap().rest_until = None;
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["objects.wanted"]
        );
        assert!(lock(&driver.due).background.is_empty());
        state.push.peers.get_mut(&key).unwrap().rest_until = None;
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
        // Until something marks it due again.
        driver.local_change("repo-1");
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["objects.wanted"]
        );
    }

    /// A driver of one repo whose worker is `fake`, over a bulk lane to
    /// `listener` of a notified session, that never polls again.
    fn holding_driver(
        listener: &Arc<Listener>,
        fake: FakeFolder,
        wake: Arc<Seq>,
    ) -> (Arc<Driver>, tempfile::TempDir) {
        let root = tempfile::tempdir().unwrap();
        let bulk = listener.lane_with("session", &[features::CONTENT_PUSH]);
        let driver = Driver::with_policy(
            Arc::new(ReadyLanes::one(true, bulk)),
            Catalog::open(root.path()).unwrap(),
            Arc::new(RepoFolders(vec![(
                "repo-1".into(),
                fake_worker(Arc::new(fake)),
                Vec::new(),
            )])),
            wake,
            active(HOUR, HOUR),
            Cancel::new(),
        );
        (driver, root)
    }

    fn background_due(driver: &Driver) -> bool {
        lock(&driver.due)
            .background
            .contains(&pair(ENDPOINT, "repo-1"))
    }

    #[test]
    fn a_store_that_failed_stays_due_and_waits_out_its_rest() {
        let all = objects("object", 1);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            stall_above: Some(0),
            ..Listener::default()
        });
        let fake = FakeFolder {
            held: all,
            ..FakeFolder::default()
        };
        let (driver, _root) = holding_driver(&listener, fake, Seq::new());
        let mut state = PumpState::default();
        state.push.deadline = |_| Duration::from_millis(100);
        // The store outlasts its deadline: what it held stays to do.
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["wanted/0", "wanted/32", "store"]
        );
        assert!(background_due(&driver));
        // While the endpoint rests the mark waits.
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
        assert!(background_due(&driver));
        // A wake that already moved stops the step before it takes its mark.
        state.push.resting.clear();
        let before = listener.calls().len();
        driver.pump_round(&mut state, driver.wake.current().wrapping_add(1));
        assert_eq!(listener.calls().len(), before);
        assert!(background_due(&driver));
    }

    #[test]
    fn a_pass_a_wake_stopped_stays_due() {
        let all = objects("object", 1);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            ..Listener::default()
        });
        let wake = Seq::new();
        let fake = FakeFolder {
            held: all,
            wake_on_read: Some(wake.clone()),
            ..FakeFolder::default()
        };
        let (driver, _root) = holding_driver(&listener, fake, wake);
        let mut state = PumpState::default();
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["wanted/0", "wanted/32"]
        );
        assert!(background_due(&driver));
    }

    #[test]
    fn a_mark_of_a_repo_not_here_does_not_keep_the_pump_awake() {
        let listener = Arc::new(Listener::default());
        let (driver, _root) = pushing_driver(&listener, true, active(HOUR, HOUR));
        driver.notified(ENDPOINT, &demand_of("repo-elsewhere"));
        assert_eq!(driver.pump_wait(), HOUR);
        driver.notified(ENDPOINT, &demand_of("repo-1"));
        assert_eq!(driver.pump_wait(), DUE_CHECK);
    }

    #[test]
    fn the_marks_a_peer_can_cause_are_bounded() {
        let (driver, _root, _control) = dispatching(active(HOUR, HOUR));
        for index in 0..MAX_MARKS + 10 {
            driver.notified(ENDPOINT, &metadata(&format!("repo-{index}")));
        }
        assert_eq!(lock(&driver.due).exchange.len(), MAX_MARKS);
    }

    #[test]
    fn the_marks_of_a_peer_with_no_lane_left_are_dropped() {
        let listener = Arc::new(Listener::default());
        let (driver, _root) = pushing_driver(&listener, true, active(HOUR, HOUR));
        let mut state = PumpState::default();
        driver.pump_round(&mut state, driver.wake.current());
        driver.notified("ws://gone", &demand_of("repo-1"));
        lock(&driver.due)
            .background
            .insert(pair("ws://gone", "repo-1"));
        driver.pump_round(&mut state, driver.wake.current());
        let due = lock(&driver.due);
        assert!(due.demand.is_empty());
        assert!(due.background.is_empty());
    }
}
