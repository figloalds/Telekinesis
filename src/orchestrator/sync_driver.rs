//! The orchestrator's sync driver (spec §7.3–§7.5): the metadata scheduler,
//! the object pump and the peer clock probe. It only originates requests on
//! this side's outbound lanes ([`Network`]); serving peers is `PeerService`'s.
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
//! * **object pump** — one thread for every Running, unpaused folder: each
//!   round, `wanted` = the worker's last `watch` demand ∪ `objects.missing
//!   {limit: OBJECT_BATCH}`, grouped by the Ready bulk lane that owns each
//!   digest (across peers), fetched in batches of ≤ [`OBJECT_BATCH`] and
//!   written to the worker. A round that stored nothing sleeps
//!   `sync_interval_ms`, cut short by a new demand (the wake [`Seq`] the
//!   folders' watch threads bump). A peer whose fetch failed sits out the
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
    sync::{FeedCursor, FeedPage, FeedReply, FeedRequest, Object},
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
    collections::{BTreeMap, BTreeSet},
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
/// Deadline of one request on a peer lane.
pub const PEER_DEADLINE: Duration = Duration::from_secs(10);

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

/// The objects one peer bulk lane is asked for.
struct Group {
    endpoint: String,
    conn: Conn,
    batch: Vec<(Digest, Kind)>,
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
    network: Arc<Network>,
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
}

/// Starts the scheduler, its exchange pool and the pump. `cancel` (a child
/// of the daemon's stopping token) stops them.
pub fn start(
    network: Arc<Network>,
    catalog: Arc<Catalog>,
    folders: Arc<dyn SyncFolders>,
    wake: Arc<Seq>,
    interval: Duration,
    cancel: Cancel,
) -> Result<Arc<Driver>> {
    let driver = Arc::new(Driver {
        network,
        catalog,
        folders,
        wake,
        interval,
        cancel,
        jobs: Mutex::new(BTreeMap::new()),
        busy: AtomicUsize::new(0),
        probes: Arc::new(Mutex::new(BTreeMap::new())),
        threads: Mutex::new(Vec::new()),
    });
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
            let peers = self.network.ready_peers();
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
        let mut round: u64 = 0;
        // Peers sitting out: endpoint → the first round they are asked again.
        let mut resting: BTreeMap<String, u64> = BTreeMap::new();
        // The last error logged per peer endpoint (fetch) or folder id
        // (`objects.missing`): logged again only when it changes.
        let mut logged: BTreeMap<String, String> = BTreeMap::new();
        while !self.cancel.is_cancelled() {
            round += 1;
            let seen = self.wake.current();
            let stored = self.pump_round(round, &mut resting, &mut logged);
            if self.cancel.is_cancelled() {
                break;
            }
            if stored == 0 {
                self.wake.wait_past(seen, self.interval);
            }
        }
    }

    /// One pass over every syncing folder; the number of objects stored.
    fn pump_round(
        &self,
        round: u64,
        resting: &mut BTreeMap<String, u64>,
        logged: &mut BTreeMap<String, String>,
    ) -> usize {
        let mut endpoints: Vec<String> = self
            .network
            .bulk_endpoints()
            .into_iter()
            .filter(|endpoint| resting.get(endpoint).is_none_or(|at| *at <= round))
            .collect();
        if endpoints.is_empty() {
            return 0;
        }
        let turn = (round as usize) % endpoints.len();
        endpoints.rotate_left(turn);
        let mut stored = 0;
        for folder in self.folders.syncing() {
            if self.cancel.is_cancelled() {
                break;
            }
            let missing = match folder.link.missing(OBJECT_BATCH) {
                Ok(missing) => {
                    logged.remove(&folder.id);
                    missing
                }
                Err(error) => {
                    let error = format!("objects.missing: {error}");
                    if logged.get(&folder.id) != Some(&error) {
                        event!(warn, SYNC_MISSING_FAILED, {
                            "folder": folder.id.clone(),
                            "error": error.clone(),
                        });
                        logged.insert(folder.id.clone(), error);
                    }
                    Vec::new()
                }
            };
            let wanted = wanted(&folder.demand, missing);
            for Group {
                endpoint,
                conn,
                batch,
            } in self.group(&endpoints, &wanted)
            {
                if resting.get(&endpoint).is_some_and(|at| *at > round) {
                    continue;
                }
                for chunk in batch.chunks(OBJECT_BATCH) {
                    match fetch(&conn, &folder.repo, chunk, &self.cancel) {
                        Ok(objects) if objects.is_empty() => {}
                        Ok(objects) => match folder.link.write(objects) {
                            Ok(count) => {
                                stored += count;
                                logged.remove(&endpoint);
                            }
                            Err(error) => {
                                event!(warn, SYNC_WRITE_FAILED, {
                                    "folder": folder.id.clone(),
                                    "error": error.to_string(),
                                });
                                break;
                            }
                        },
                        Err(failure) => {
                            resting.insert(endpoint.clone(), round + 2);
                            let error = failure.to_string();
                            if logged.get(&endpoint) != Some(&error) {
                                event!(warn, SYNC_FETCH_FAILED, {
                                    "peer": endpoint.clone(),
                                    "repo": folder.repo.clone(),
                                    "error": error.clone(),
                                });
                                logged.insert(endpoint.clone(), error);
                            }
                            break;
                        }
                    }
                }
            }
        }
        resting.retain(|_, at| *at > round);
        stored
    }

    /// `wanted` grouped by the Ready bulk lane that owns each digest: per
    /// digest, the first of `endpoints` with such a lane.
    fn group(&self, endpoints: &[String], wanted: &[(Digest, Kind)]) -> Vec<Group> {
        // Keyed by (endpoint, session): one group per lane.
        let mut groups: BTreeMap<(String, String), Group> = BTreeMap::new();
        for object in wanted {
            let lane = endpoints.iter().find_map(|endpoint| {
                Some((endpoint, self.network.bulk_lane(endpoint, &object.0)?))
            });
            if let Some((endpoint, conn)) = lane {
                let key = (endpoint.clone(), conn.peer().session.clone());
                groups
                    .entry(key)
                    .or_insert_with(|| Group {
                        endpoint: endpoint.clone(),
                        conn,
                        batch: Vec::new(),
                    })
                    .batch
                    .push(*object);
            }
        }
        groups.into_values().collect()
    }
}

/// Demand first, then `missing`, without duplicates.
fn wanted(demand: &[(Digest, Kind)], missing: Vec<(Digest, Kind)>) -> Vec<(Digest, Kind)> {
    let mut seen = BTreeSet::new();
    demand
        .iter()
        .copied()
        .chain(missing)
        .filter(|(digest, _)| seen.insert(*digest))
        .collect()
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

/// `objects.fetch` of ≤ [`OBJECT_BATCH`] objects on a peer bulk lane.
fn fetch(
    conn: &Conn,
    repo: &str,
    wanted: &[(Digest, Kind)],
    cancel: &Cancel,
) -> Result<Vec<Object>, Failure> {
    let body = serde_json::to_vec(wanted).map_err(|e| worker_failure("objects.fetch", e))?;
    let reply = conn
        .call_cancellable(
            "objects.fetch",
            json!({"repo": repo}),
            body,
            Priority::Data,
            PEER_DEADLINE,
            &|| cancel.is_cancelled(),
        )
        .map_err(|error| peer_failure("objects.fetch", error))?;
    serde_json::from_slice(&reply.body)
        .map_err(|error| Failure::Error(format!("objects.fetch: malformed reply: {error}")))
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
    fn wanted_puts_demand_first_without_duplicates() {
        let digest = |n: u8| Digest::bytes(&[n]);
        let demand = vec![(digest(2), Kind::Chunk), (digest(1), Kind::Manifest)];
        let missing = vec![
            (digest(1), Kind::Manifest),
            (digest(3), Kind::Chunk),
            (digest(2), Kind::Chunk),
        ];
        assert_eq!(
            wanted(&demand, missing),
            vec![
                (digest(2), Kind::Chunk),
                (digest(1), Kind::Manifest),
                (digest(3), Kind::Chunk),
            ]
        );
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

    fn info() -> PeerInfo {
        PeerInfo {
            installation: "peer-b".into(),
            role: LaneRole::Control,
            lane: None,
            session: "session".into(),
            verifier_generation: 1,
        }
    }

    /// A client `Conn` whose server side is served by `handler`.
    fn served(handler: Arc<dyn Handler>) -> (Conn, Conn) {
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
            Conn::from_established(client, Side::Client, info(), ConnOptions::default()).unwrap();
        let server = Conn::from_established(
            server.join().unwrap(),
            Side::Server,
            info(),
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
}
