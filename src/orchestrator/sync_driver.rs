//! The orchestrator's sync driver (spec §7.3–§7.5): the metadata scheduler,
//! the object pump and the peer clock probe. It only originates requests on
//! this side's outbound lanes ([`Lanes`], the [`Network`]); serving peers is
//! `PeerService`'s.
//!
//! Threads (all end once the driver's [`Cancel`] fires; [`Driver::stop`]):
//! * **scheduler** — every `sync_interval_ms`, for each paired peer with a
//!   Ready control lane × each Running, unpaused folder, dispatches one
//!   exchange per (peer, repo) to a pool of [`MAX_EXCHANGES`] exchange
//!   threads (at most that many in flight overall). An exchange is
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
//!   next round. The pump never blocks the scheduler.
//! * **clock probes** — a short thread per probe, at most one per peer: at
//!   every new control session (a Ready transition) and every
//!   [`CLOCK_PROBE_EVERY`] scheduler rounds. The smoothed offset goes to the
//!   catalog (`Catalog::set_clock`); one past `WARN_OFFSET_MS` logs
//!   `CLOCK_OFFSET_LARGE`.
use super::{
    catalog::Catalog,
    demand::Seq,
    network::{Network, ReadyPeer},
    sync::{FeedCursor, FeedPage, FeedReply, FeedRequest, Fetched, MAX_EXCLUDE},
    worker_client::{CALL_DEADLINE, WorkerLink},
};
use crate::{
    clock::{self, Offset, Sample, WARN_OFFSET_MS},
    content::Kind,
    model::{content::Digest, orchestrator::CallError},
    session::{Conn, Priority},
    util::{cancel::Cancel, lock::lock, log::event, time::now_ms},
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
/// A demanded digest just written is not fetched again for this long (the
/// worker's demand list catches up at its next `watch`).
const DEMAND_SUPPLIED: Duration = super::worker_client::WATCH_DEADLINE;

type Wanted = Vec<(Digest, Kind)>;

/// The peer lanes the driver uses: the [`Network`] (tests stub it).
pub trait Lanes: Send + Sync {
    /// Every peer with a Ready control lane.
    fn ready_peers(&self) -> Vec<ReadyPeer>;
    /// The endpoints with at least one Ready bulk lane.
    fn bulk_endpoints(&self) -> Vec<String>;
    /// The Ready bulk lane to `endpoint` that owns `digest`.
    fn bulk_lane(&self, endpoint: &str, digest: &Digest) -> Option<Conn>;
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
        let size = (self.batch_size(id, endpoint).min(asked) / 2).max(1);
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
            let size = size + (size / 4).max(1);
            if size >= OBJECT_BATCH {
                self.batch.remove(&key);
            } else {
                self.batch.insert(key, size);
            }
        }
    }
}

struct Unavailable {
    until: Instant,
    backoff: Duration,
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
enum Failure {
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

fn peer_failure(op: &str, error: CallError) -> Failure {
    match error {
        CallError::ConnectionLost(_) | CallError::Unavailable(_) | CallError::Cancelled => {
            Failure::LaneLost
        }
        error => Failure::Error(format!("{op}: {error}")),
    }
}

fn worker_failure(op: &str, error: impl std::fmt::Display) -> Failure {
    Failure::Error(format!("{op}: {error}"))
}

pub struct Driver {
    lanes: Arc<dyn Lanes>,
    catalog: Arc<Catalog>,
    folders: Arc<dyn SyncFolders>,
    /// Bumped by folder watch threads when demand changes; wakes the pump.
    wake: Arc<Seq>,
    interval: Duration,
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
    interval: Duration,
    cancel: Cancel,
) -> Result<Arc<Driver>> {
    let driver = Driver::new(lanes, catalog, folders, wake, interval, cancel);
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
    /// A driver with no threads yet (see [`start`]).
    fn new(
        lanes: Arc<dyn Lanes>,
        catalog: Arc<Catalog>,
        folders: Arc<dyn SyncFolders>,
        wake: Arc<Seq>,
        interval: Duration,
        cancel: Cancel,
    ) -> Arc<Self> {
        Arc::new(Driver {
            lanes,
            catalog,
            folders,
            wake,
            interval,
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

    // ---- scheduler -------------------------------------------------------

    fn schedule(&self, tasks: Sender<Task>) {
        let mut round: u64 = 0;
        while !self.cancel.is_cancelled() {
            round += 1;
            let peers = self.lanes.ready_peers();
            self.probe_clocks(round, &peers);
            self.dispatch(round, &peers, &tasks);
            if self.cancel.sleep(self.interval) {
                break;
            }
        }
        // Dropping `tasks` ends the exchange threads once they are idle.
    }

    /// Dispatches an exchange for every idle (peer, repo), within
    /// [`MAX_EXCHANGES`]; the pairs are rotated each round so none starves.
    fn dispatch(&self, round: u64, peers: &[ReadyPeer], tasks: &Sender<Task>) {
        let folders = self.folders.syncing();
        let mut pairs: Vec<(&ReadyPeer, &SyncFolder)> = peers
            .iter()
            .flat_map(|peer| folders.iter().map(move |folder| (peer, folder)))
            .collect();
        if pairs.is_empty() {
            return;
        }
        let turn = (round as usize) % pairs.len();
        pairs.rotate_left(turn);
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
            });
            if slot.running {
                continue;
            }
            // A restarted worker or a new control session: start over.
            if !Weak::ptr_eq(&slot.link, &Arc::downgrade(&folder.link))
                || slot.session != peer.session
            {
                slot.link = Arc::downgrade(&folder.link);
                slot.session = peer.session.clone();
                slot.job.cursor = FeedCursor::default();
            }
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
                self.wake.wait_past(seen, self.interval);
            }
        }
    }

    /// One round. Pass 1 supplies every folder's pending demand; pass 2 the
    /// background `objects.missing` work, abandoned before its next call as
    /// soon as the wake moves past `seen` (the next round starts with pass
    /// 1). The number of objects stored.
    fn pump_round(&self, state: &mut PumpState, seen: u64) -> usize {
        state.round += 1;
        let round = state.round;
        state.resting.retain(|_, at| *at > round);
        let mut endpoints = self.lanes.bulk_endpoints();
        endpoints.retain(|endpoint| !state.resting.contains_key(endpoint));
        if endpoints.is_empty() {
            return 0;
        }
        let turn = (round as usize) % endpoints.len();
        endpoints.rotate_left(turn);
        let folders = self.folders.syncing();
        let syncing = |id: &String| folders.iter().any(|folder| folder.id == *id);
        state.unavailable.retain(|id, _| syncing(id));
        state.scan.retain(|id, _| syncing(id));
        state.batch.retain(|(id, _), _| syncing(id));
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
            if !demand.is_empty() && !self.cancel.is_cancelled() {
                stored += self.supply(state, &endpoints, folder, demand, None).stored;
            }
        }
        // Pass 2: background prefetch.
        for folder in &folders {
            if self.cancel.is_cancelled() || self.wake.current() != seen {
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
        stored
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
            for (conn, batch) in self.lanes_for(endpoint, &remaining) {
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

    /// `wanted` grouped by the Ready bulk lane of `endpoint` that owns each
    /// digest (digests no Ready lane owns are left out).
    fn lanes_for(&self, endpoint: &str, wanted: &[(Digest, Kind)]) -> Vec<(Conn, Wanted)> {
        let mut lanes: BTreeMap<String, (Conn, Wanted)> = BTreeMap::new();
        for object in wanted {
            if let Some(conn) = self.lanes.bulk_lane(endpoint, &object.0) {
                lanes
                    .entry(conn.peer().session.clone())
                    .or_insert_with(|| (conn, Vec::new()))
                    .1
                    .push(*object);
            }
        }
        lanes.into_values().collect()
    }
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
struct FetchFailure {
    failure: Failure,
    timed_out: bool,
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
    use crate::orchestrator::sync::{MissingPage, Object};
    use crate::{
        model::{errors::ErrorWire, protocol::LaneRole},
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
        }
    }

    /// A client `Conn` whose server side is served by `handler`.
    fn served(handler: Arc<dyn Handler>) -> (Conn, Conn) {
        served_as(LaneRole::Control, handler)
    }

    /// [`served`] as a session of `role`.
    fn served_as(role: LaneRole, handler: Arc<dyn Handler>) -> (Conn, Conn) {
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
        let client =
            Conn::from_established(client, Side::Client, info_as(role), ConnOptions::default())
                .unwrap();
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
        folders: Arc<Folders>,
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
            Duration::from_millis(50),
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
}
