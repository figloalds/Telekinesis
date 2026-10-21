//! Fixed per-daemon preparation/network pools. A slow frame never owns the
//! foreground probe thread or the old two-repository lane gate.
use super::{
    content_transport::{self, ContentDaemon, FrameTask, Preparation},
    scheduler::{Readiness, readiness},
};
use crate::{
    model::{
        content_wire as wire,
        errors::ErrorWire,
        orchestrator::{CallError, Ctx, PeerContext, WorkerEndpoint},
        protocol::LaneSpec,
    },
    paired_sync::SyncRoute,
    session::Conn,
    util::{Cancel, lock::lock},
};
use std::{
    array,
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{
        Arc, Condvar, Mutex, TryLockError, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;

type LaneKey = (String, u32, u32);
const GROUP_PAGE: usize = 512;
const GROUP_OWNERS: usize = 1024;
struct Binding {
    legacy: Option<crate::model::lease::PullRun>,
    id: Uuid,
    peer: Arc<PeerContext>,
    lane: LaneSpec,
    conn: Conn,
    cancel: Cancel,
}
impl Binding {
    fn live(&self) -> bool {
        !self.cancel.is_cancelled() && self.conn.closed().is_none()
    }
}
struct Source {
    peer: String,
    replica: String,
    lanes: Vec<Arc<Binding>>,
}
struct Group {
    repo: String,
    device: String,
    endpoint: WorkerEndpoint,
    sources: Vec<Source>,
    state: Arc<GroupState>,
}
struct PollCache {
    until: Instant,
    activity: wire::Activity,
    candidates: Vec<String>,
}
struct GroupState {
    cancel: Cancel,
    effects: Mutex<()>,
    busy: [AtomicBool; 3],
    due: [Mutex<Instant>; 3],
    source: [AtomicUsize; 3],
    empty: [AtomicUsize; 3],
    polling: AtomicBool,
    poll: Mutex<Option<PollCache>>,
    poll_turn: AtomicUsize,
    legacy_turn: AtomicUsize,
    last_error: Mutex<Option<ErrorWire>>,
}
impl GroupState {
    fn new(cancel: Cancel) -> Arc<Self> {
        let state = Arc::new(Self {
            cancel,
            effects: Mutex::new(()),
            busy: array::from_fn(|_| AtomicBool::new(false)),
            due: array::from_fn(|_| Mutex::new(Instant::now())),
            source: array::from_fn(|_| AtomicUsize::new(0)),
            empty: array::from_fn(|_| AtomicUsize::new(0)),
            polling: AtomicBool::new(false),
            poll: Mutex::new(None),
            poll_turn: AtomicUsize::new(0),
            legacy_turn: AtomicUsize::new(0),
            last_error: Mutex::new(None),
        });
        let weak = Arc::downgrade(&state);
        state.cancel.on_cancel(Box::new(move || {
            if let Some(state) = weak.upgrade() {
                // Cancellation returning is a barrier for the short counter/
                // error projections, never for network or cleanup I/O.
                let _effects = lock(&state.effects);
            }
        }));
        state
    }
}

#[derive(Default)]
struct GroupRegistry {
    owners: Vec<Weak<Group>>,
}
impl GroupRegistry {
    fn live(&mut self, valid: impl Fn(&Group) -> bool) -> Vec<Arc<Group>> {
        let mut live = Vec::new();
        self.owners.retain(|weak| {
            let Some(group) = weak.upgrade() else {
                return false;
            };
            if group.state.cancel.is_cancelled() {
                return false;
            }
            if !valid(&group) {
                group.state.cancel.cancel();
                return false;
            }
            live.push(group);
            true
        });
        live
    }

    fn retain(&mut self, group: &Arc<Group>) -> bool {
        if self.owners.len() >= GROUP_OWNERS {
            return false;
        }
        self.owners.push(Arc::downgrade(group));
        true
    }
}
struct Job {
    task: Task,
    group: Arc<Group>,
    class: usize,
}
enum Task {
    Typed(Box<FrameTask>),
    Legacy(Box<super::legacy_demand::Task>),
}
impl Task {
    fn perform(self) -> Result<bool, CallError> {
        match self {
            Self::Typed(task) => task.perform(),
            Self::Legacy(task) => task.perform(),
        }
    }
}

pub struct Dispatcher {
    ctx: Weak<Ctx>,
    transport: Weak<ContentDaemon>,
    cancel: Cancel,
    lanes: Mutex<BTreeMap<LaneKey, Arc<Binding>>>,
    groups: Mutex<Arc<Vec<Arc<Group>>>>,
    group_owners: Mutex<GroupRegistry>,
    refreshing: Mutex<()>,
    refresh_at: Mutex<Instant>,
    group_turn: AtomicUsize,
    deferred: AtomicUsize,
    queue: Mutex<VecDeque<Job>>,
    ready: Condvar,
    cursor: [AtomicUsize; 3],
    preparing: AtomicUsize,
    running: AtomicUsize,
    peak_queue: AtomicUsize,
    admitted: [AtomicU64; 3],
    started: [AtomicU64; 3],
    completed: [AtomicU64; 3],
    failed: [AtomicU64; 3],
    retried: [AtomicU64; 3],
}

#[derive(Clone, Debug)]
pub struct DispatchIdle {
    pub repo: String,
    pub device: String,
    /// Consecutive empty preparations, capped at seven for diagnostics.
    pub polls: [usize; 3],
    /// Remaining delay at this snapshot; not an admission guarantee.
    pub due_ms: [u64; 3],
}

#[derive(Clone, Debug)]
pub struct DispatchView {
    pub lanes: usize,
    pub groups: usize,
    pub deferred: usize,
    pub preparing: usize,
    pub queued: usize,
    pub running: usize,
    pub peak_queue: usize,
    /// Foreground, structural, background. Admission is before remote I/O.
    pub admitted: [u64; 3],
    /// A network executor accepted the job, separately from queue admission.
    pub started: [u64; 3],
    pub completed: [u64; 3],
    pub failed: [u64; 3],
    pub retried: [u64; 3],
    pub idle: Vec<DispatchIdle>,
    pub errors: Vec<(String, ErrorWire)>,
}

impl Dispatcher {
    fn start(ctx: &Arc<Ctx>, transport: &Arc<ContentDaemon>) -> anyhow::Result<Arc<Self>> {
        let state = Arc::new(Self {
            ctx: Arc::downgrade(ctx),
            transport: Arc::downgrade(transport),
            cancel: transport.cancel.child(),
            lanes: Mutex::new(BTreeMap::new()),
            groups: Mutex::new(Arc::new(vec![])),
            group_owners: Mutex::new(GroupRegistry::default()),
            refreshing: Mutex::new(()),
            refresh_at: Mutex::new(Instant::now()),
            group_turn: AtomicUsize::new(0),
            deferred: AtomicUsize::new(0),
            queue: Mutex::new(VecDeque::new()),
            ready: Condvar::new(),
            cursor: array::from_fn(|_| AtomicUsize::new(0)),
            preparing: AtomicUsize::new(0),
            running: AtomicUsize::new(0),
            peak_queue: AtomicUsize::new(0),
            admitted: array::from_fn(|_| AtomicU64::new(0)),
            started: array::from_fn(|_| AtomicU64::new(0)),
            completed: array::from_fn(|_| AtomicU64::new(0)),
            failed: array::from_fn(|_| AtomicU64::new(0)),
            retried: array::from_fn(|_| AtomicU64::new(0)),
        });
        let weak = Arc::downgrade(&state);
        state.cancel.on_cancel(Box::new(move || {
            if let Some(state) = weak.upgrade() {
                let lanes: Vec<_> = lock(&state.lanes).values().cloned().collect();
                for lane in lanes {
                    lane.cancel.cancel();
                    lane.conn.close();
                }
                let queued = std::mem::take(&mut *lock(&state.queue));
                state.ready.notify_all();
                drop(queued);
            }
        }));
        for (index, class) in [0, 0, 1, 2].into_iter().enumerate() {
            let weak = Arc::downgrade(&state);
            if let Err(e) = thread::Builder::new()
                .name(format!("content-prepare-{index}"))
                .spawn(move || {
                    while let Some(state) = weak.upgrade() {
                        if state.cancel.is_cancelled() {
                            break;
                        }
                        let worked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            state.prepare(class)
                        }))
                        .unwrap_or(false);
                        let cancel = state.cancel.clone();
                        drop(state);
                        cancel.sleep(Duration::from_millis(if worked { 1 } else { 2 }));
                    }
                })
            {
                state.cancel.cancel();
                return Err(anyhow::anyhow!("CONTENT_RESOURCE_LIMIT: {e}"));
            }
        }
        for index in 0..4 {
            let weak = Arc::downgrade(&state);
            if let Err(e) = thread::Builder::new()
                .name(format!("content-frame-{index}"))
                .spawn(move || {
                    while let Some(state) = weak.upgrade() {
                        let job = {
                            let mut queue = lock(&state.queue);
                            while queue.is_empty() && !state.cancel.is_cancelled() {
                                queue = state
                                    .ready
                                    .wait_timeout(queue, Duration::from_millis(50))
                                    .unwrap_or_else(|e| e.into_inner())
                                    .0;
                                if Arc::strong_count(&state) == 1 {
                                    return;
                                }
                            }
                            if state.cancel.is_cancelled() {
                                return;
                            }
                            queue.pop_front()
                        };
                        if let Some(job) = job {
                            if !state.start_job(&job.group, job.class) {
                                drop(job);
                                continue;
                            }
                            state.running.fetch_add(1, Ordering::Relaxed);
                            let result =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    job.task.perform()
                                }));
                            state.running.fetch_sub(1, Ordering::Relaxed);
                            if job.group.state.cancel.is_cancelled() {
                                continue;
                            }
                            match result {
                                Ok(Ok(_)) => {
                                    state.finish(&job.group, job.class);
                                }
                                Ok(Err(e)) => state.error(&job.group, job.class, &e),
                                Err(_) => state.error(
                                    &job.group,
                                    job.class,
                                    &CallError::Remote(ErrorWire::coded(
                                        "UNKNOWN_ERROR",
                                        "content frame panicked",
                                    )),
                                ),
                            }
                        }
                    }
                })
            {
                state.cancel.cancel();
                return Err(anyhow::anyhow!("CONTENT_RESOURCE_LIMIT: {e}"));
            }
        }
        Ok(state)
    }

    pub fn view(&self) -> DispatchView {
        let groups = lock(&self.groups).clone();
        let now = Instant::now();
        DispatchView {
            lanes: lock(&self.lanes).len(),
            groups: groups.len(),
            deferred: self.deferred.load(Ordering::Relaxed),
            preparing: self.preparing.load(Ordering::Relaxed),
            queued: lock(&self.queue).len(),
            running: self.running.load(Ordering::Relaxed),
            peak_queue: self.peak_queue.load(Ordering::Relaxed),
            admitted: array::from_fn(|i| self.admitted[i].load(Ordering::Relaxed)),
            started: array::from_fn(|i| self.started[i].load(Ordering::Relaxed)),
            completed: array::from_fn(|i| self.completed[i].load(Ordering::Relaxed)),
            failed: array::from_fn(|i| self.failed[i].load(Ordering::Relaxed)),
            retried: array::from_fn(|i| self.retried[i].load(Ordering::Relaxed)),
            idle: groups
                .iter()
                .map(|g| DispatchIdle {
                    repo: g.repo.clone(),
                    device: g.device.clone(),
                    polls: array::from_fn(|i| g.state.empty[i].load(Ordering::Relaxed).min(7)),
                    due_ms: array::from_fn(|i| {
                        lock(&g.state.due[i])
                            .saturating_duration_since(now)
                            .as_millis()
                            .min(u64::MAX as u128) as u64
                    }),
                })
                .collect(),
            errors: groups
                .iter()
                .filter_map(|g| {
                    lock(&g.state.last_error)
                        .clone()
                        .map(|e| (g.repo.clone(), e))
                })
                .collect(),
        }
    }

    fn refresh(&self) {
        // Only one snapshot builder may publish at once. Other preparers keep
        // using their current page instead of waiting behind catalog work.
        let _refreshing = match self.refreshing.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
            Err(TryLockError::WouldBlock) => return,
        };
        {
            let mut due = lock(&self.refresh_at);
            if Instant::now() < *due {
                return;
            }
            *due = Instant::now() + Duration::from_millis(100);
        }
        let Some(ctx) = self.ctx.upgrade() else {
            self.cancel.cancel();
            return;
        };
        // Weakly retain groups still owned by queued/running/preparing work,
        // including groups outside this rotation page. Absence from the page
        // is not withdrawal. Validate against the complete folder snapshot.
        let tracked = lock(&self.group_owners).live(|_| true);
        let folders: BTreeMap<_, _> = ctx
            .folders
            .all()
            .into_iter()
            .map(|f| (f.record.repo.clone(), f))
            .collect();
        let first = if folders.len() > GROUP_PAGE {
            self.group_turn.fetch_add(GROUP_PAGE, Ordering::Relaxed) % folders.len()
        } else {
            0
        };
        let selected: BTreeSet<_> = folders
            .keys()
            .cycle()
            .skip(first)
            .take(folders.len().min(GROUP_PAGE))
            .cloned()
            .collect();
        let wanted: BTreeSet<_> = selected
            .iter()
            .cloned()
            .chain(tracked.iter().map(|g| g.repo.clone()))
            .collect();
        let bindings: Vec<_> = lock(&self.lanes)
            .values()
            .filter(|l| l.live())
            .cloned()
            .collect();
        let mut peers: BTreeMap<String, Vec<Arc<Binding>>> = BTreeMap::new();
        for binding in bindings {
            peers
                .entry(binding.peer.installation.clone())
                .or_default()
                .push(binding);
        }
        let mut sources: BTreeMap<String, Vec<Source>> = BTreeMap::new();
        for (peer, lanes) in peers {
            for remote in &lanes[0].peer.catalog.load().entries {
                if !wanted.contains(&remote.repo) {
                    continue;
                }
                let Some(folder) = folders.get(&remote.repo) else {
                    continue;
                };
                if readiness(folder, remote) != Readiness::Ready
                    || folder.record.device == remote.replica
                {
                    continue;
                }
                let entries = sources.entry(remote.repo.clone()).or_default();
                if !entries.iter().any(|s| s.peer == peer) {
                    entries.push(Source {
                        peer: peer.clone(),
                        replica: remote.replica.clone(),
                        lanes: lanes.clone(),
                    });
                }
            }
        }
        let mut registry = lock(&self.group_owners);
        let current = registry.live(|group| {
            folders.get(&group.repo).is_some_and(|folder| {
                folder.record.device == group.device
                    && folder.worker.as_ref() == Some(&group.endpoint)
                    && sources
                        .get(&group.repo)
                        .is_some_and(|sources| same_sources(&group.sources, sources))
            })
        });
        // The first upgrade only pins the validation candidates through this
        // snapshot; it must not make otherwise dead owners permanent.
        drop(tracked);
        let mut page = Vec::new();
        let mut refused = 0;
        for repo in selected {
            let Some(entries) = sources.remove(&repo) else {
                continue;
            };
            if let Some(previous) = current.iter().find(|g| g.repo == repo) {
                page.push(previous.clone());
                continue;
            }
            let folder = &folders[&repo];
            let group = Arc::new(Group {
                repo,
                device: folder.record.device.clone(),
                endpoint: folder.worker.clone().unwrap(), // Ready above
                sources: entries,
                state: GroupState::new(self.cancel.child()),
            });
            if registry.retain(&group) {
                page.push(group);
            } else {
                refused += 1;
            }
        }
        drop(registry);
        self.deferred.store(
            folders.len().saturating_sub(GROUP_PAGE) + refused,
            Ordering::Relaxed,
        );
        *lock(&self.groups) = Arc::new(page);
    }

    fn activity(
        &self,
        group: &Group,
        transport: &ContentDaemon,
    ) -> Result<Option<wire::Activity>, CallError> {
        if let Some(cache) = lock(&group.state.poll).as_ref().filter(|p| {
            p.until > Instant::now()
                && p.candidates
                    .iter()
                    .all(|id| group.sources.iter().any(|s| &s.replica == id))
        }) {
            return Ok(Some(cache.activity.clone()));
        }
        if group.state.polling.swap(true, Ordering::AcqRel) {
            return Ok(None);
        }
        let _polling = Flag(&group.state.polling);
        let sources: Vec<_> = group.sources.iter().map(|s| s.replica.clone()).collect();
        if sources.is_empty() {
            return Ok(None);
        }
        let first = group.state.poll_turn.fetch_add(32, Ordering::Relaxed) % sources.len();
        let candidates: Vec<_> = sources
            .iter()
            .cycle()
            .skip(first)
            .take(sources.len().min(32))
            .cloned()
            .collect();
        let route = SyncRoute {
            peer: group.sources[0].peer.clone(),
            repo: group.repo.clone(),
            local_replica: group.device.clone(),
            remote_replica: group.sources[0].replica.clone(),
        };
        let client = transport.planner(&group.endpoint)?;
        let (activity, _): (wire::Activity, _) = content_transport::worker_call_with_deadline(
            &client,
            wire::POLL,
            &wire::Poll {
                route,
                sources: candidates.clone(),
            },
            vec![],
            Duration::from_millis(50),
        )?;
        let ttl = if activity.active || activity.legacy_demands.is_some() {
            Duration::from_millis(20)
        } else {
            Duration::from_secs(2)
        };
        if group.state.cancel.is_cancelled() {
            return Err(CallError::Cancelled);
        }
        *lock(&group.state.poll) = Some(PollCache {
            until: Instant::now() + ttl,
            activity: activity.clone(),
            candidates,
        });
        Ok(Some(activity))
    }

    fn prepare(&self, class: usize) -> bool {
        self.refresh();
        let group = {
            let groups = lock(&self.groups);
            if groups.is_empty() {
                return false;
            }
            groups[self.cursor[class].fetch_add(1, Ordering::Relaxed) % groups.len()].clone()
        };
        if group.state.cancel.is_cancelled()
            || Instant::now() < *lock(&group.state.due[class])
            || group.state.busy[class].swap(true, Ordering::AcqRel)
        {
            return false;
        }
        let _busy = Flag(&group.state.busy[class]);
        let Some(transport) = self.transport.upgrade() else {
            self.cancel.cancel();
            return false;
        };
        self.preparing.fetch_add(1, Ordering::Relaxed);
        let _count = Count(&self.preparing);
        let result = (|| -> Result<bool, CallError> {
            let Some(activity) = self.activity(&group, &transport)? else {
                return Ok(false);
            };
            if group.state.cancel.is_cancelled() {
                return Err(CallError::Cancelled);
            }
            if class == 0
                && let Some(hashes) = activity.legacy_demands.as_ref().filter(|h| !h.is_empty())
            {
                let turn = group.state.legacy_turn.fetch_add(1, Ordering::Relaxed);
                // One bounded legacy preparation per slice; alternate with
                // typed demand when both representations have waiting readers.
                if !activity.active || activity.demand_sources.is_empty() || turn.is_multiple_of(2)
                {
                    let source = &group.sources[turn % group.sources.len()];
                    let hash = &hashes[(turn / group.sources.len()) % hashes.len()];
                    if let Some(lane) = source
                        .lanes
                        .iter()
                        .find(|l| l.live() && l.legacy.is_some() && l.lane.owns(hash))
                    {
                        let Some(ctx) = self.ctx.upgrade() else {
                            return Err(CallError::Cancelled);
                        };
                        let target = super::legacy_demand::Target {
                            ctx,
                            state: transport.clone(),
                            endpoint: group.endpoint.clone(),
                            conn: lane.conn.clone(),
                            cancel: lane.cancel.linked(&group.state.cancel),
                            generation: lane.legacy.unwrap(),
                            route: SyncRoute {
                                peer: source.peer.clone(),
                                repo: group.repo.clone(),
                                local_replica: group.device.clone(),
                                remote_replica: source.replica.clone(),
                            },
                            scope: wire::PeerScope {
                                repo: group.repo.clone(),
                                sender: group.device.clone(),
                                receiver: source.replica.clone(),
                                lane: lane.lane,
                            },
                        };
                        if let Some(task) = super::legacy_demand::prepare(target, hash)? {
                            return Ok(self.enqueue(
                                group.clone(),
                                class,
                                Task::Legacy(Box::new(task)),
                            ));
                        }
                    }
                }
            }
            if !activity.active {
                return Ok(false);
            }
            let sources: Vec<_> = group
                .sources
                .iter()
                .filter(|s| class != 0 || activity.demand_sources.contains(&s.replica))
                .filter(|s| s.lanes.iter().any(|l| l.live()))
                .collect();
            if sources.is_empty() {
                return Ok(false);
            }
            let source =
                sources[group.state.source[class].fetch_add(1, Ordering::Relaxed) % sources.len()];
            let client = transport.planner(&group.endpoint)?;
            let route = SyncRoute {
                peer: source.peer.clone(),
                repo: group.repo.clone(),
                local_replica: group.device.clone(),
                remote_replica: source.replica.clone(),
            };
            let mode = [
                wire::PrepareMode::Foreground,
                wire::PrepareMode::Structural,
                wire::PrepareMode::Background,
            ][class];
            let owner = source
                .lanes
                .iter()
                .find(|l| l.live())
                .map(|l| l.id)
                .ok_or(CallError::Cancelled)?;
            match content_transport::prepare_offer(
                &transport,
                client,
                route,
                owner,
                mode,
                LaneSpec::new(0, 1),
            )? {
                Preparation::Capacity => Ok(false),
                Preparation::Progress => {
                    group.state.empty[class].store(0, Ordering::Relaxed);
                    Ok(true)
                }
                Preparation::Idle => {
                    let empty = group.state.empty[class]
                        .fetch_add(1, Ordering::Relaxed)
                        .min(6);
                    *lock(&group.state.due[class]) = Instant::now()
                        + Duration::from_millis(match class {
                            0 => 10,
                            // A missing COW page can introduce new structural
                            // demand after background traversal is already idle.
                            // Preserve its reserved admission latency without
                            // accelerating background byte polling.
                            1 => 50,
                            _ => (10_u64 << empty).min(1000),
                        });
                    Ok(false)
                }
                Preparation::Ready(local) => {
                    let lane = source
                        .lanes
                        .iter()
                        .find(|l| l.live() && l.lane.owns(&local.offer.edge.key.digest.to_string()))
                        .ok_or(CallError::Cancelled)?;
                    let scope = wire::PeerScope {
                        repo: group.repo.clone(),
                        sender: group.device.clone(),
                        receiver: source.replica.clone(),
                        lane: lane.lane,
                    };
                    let Some(task) = FrameTask::bind(
                        &transport,
                        *local,
                        lane.conn.clone(),
                        scope,
                        lane.cancel.linked(&group.state.cancel),
                    )?
                    else {
                        return Ok(false);
                    };
                    if self.cancel.is_cancelled() {
                        return Ok(false);
                    }
                    let queued = self.enqueue(group.clone(), class, Task::Typed(Box::new(task)));
                    group.state.empty[class].store(0, Ordering::Relaxed);
                    Ok(queued)
                }
            }
        })();
        match result {
            Ok(worked) => worked,
            Err(e) => {
                self.error(&group, class, &e);
                false
            }
        }
    }

    fn enqueue(&self, group: Arc<Group>, class: usize, task: Task) -> bool {
        let owner = group.state.clone();
        let effects = lock(&owner.effects);
        let mut queue = lock(&self.queue);
        if queue.len() >= 4 || self.cancel.is_cancelled() || group.state.cancel.is_cancelled() {
            drop(queue);
            drop(effects);
            drop(task);
            return false;
        }
        queue.push_back(Job { task, group, class });
        self.peak_queue.fetch_max(queue.len(), Ordering::Relaxed);
        self.admitted[class].fetch_add(1, Ordering::Relaxed);
        drop(queue);
        drop(effects);
        self.ready.notify_one();
        true
    }

    fn error(&self, group: &Group, class: usize, error: &CallError) {
        let _effects = lock(&group.state.effects);
        if group.state.cancel.is_cancelled()
            || matches!(error, CallError::Cancelled | CallError::ConnectionLost(_))
        {
            return;
        }
        let mut wire = error.to_wire();
        // This object-engine slice uses a zero-wait SQLite connection. A
        // refused writer is readiness, including receipt indexing after CAS
        // installation; it does not count as a source or dispatch failure.
        // Keep the historical global/wire SQLITE_BUSY classification intact.
        if wire.code == "SQLITE_BUSY" {
            wire.class = crate::model::errors::ErrorClass::Contention;
        }
        let ms = match wire.code.as_str() {
            "CONTENT_CANCELLED"
            | "CANCELLED"
            | "BUSY"
            | "SQLITE_BUSY"
            | "CONTENT_RESOURCE_LIMIT" => 25,
            "PEER_NEEDS_UPGRADE" | "UNSUPPORTED_OPERATION" => 2000,
            _ => 250,
        };
        if wire.code == "UNSUPPORTED_OPERATION" {
            *lock(&group.state.poll) = Some(PollCache {
                until: Instant::now() + Duration::from_secs(30),
                activity: wire::Activity {
                    active: false,
                    demand_sources: vec![],
                    legacy_demands: None,
                },
                candidates: vec![],
            });
        }
        *lock(&group.state.due[class]) = Instant::now() + Duration::from_millis(ms);
        if wire.class.counts_as_failure() {
            self.failed[class].fetch_add(1, Ordering::Relaxed);
        } else {
            self.retried[class].fetch_add(1, Ordering::Relaxed);
        }
        *lock(&group.state.last_error) = Some(wire);
    }

    fn finish(&self, group: &Group, class: usize) {
        let _effects = lock(&group.state.effects);
        if !group.state.cancel.is_cancelled() {
            self.completed[class].fetch_add(1, Ordering::Relaxed);
            *lock(&group.state.last_error) = None;
        }
    }

    fn start_job(&self, group: &Group, class: usize) -> bool {
        let _effects = lock(&group.state.effects);
        if group.state.cancel.is_cancelled() {
            return false;
        }
        self.started[class].fetch_add(1, Ordering::Relaxed);
        true
    }
}

fn same_sources(a: &[Source], b: &[Source]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(a, b)| {
            a.peer == b.peer
                && a.replica == b.replica
                && a.lanes.len() == b.lanes.len()
                && a.lanes.iter().zip(&b.lanes).all(|(a, b)| a.id == b.id)
        })
}

struct Flag<'a>(&'a AtomicBool);
impl Drop for Flag<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
struct Count<'a>(&'a AtomicUsize);
impl Drop for Count<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct Registration {
    state: Weak<Dispatcher>,
    key: LaneKey,
    binding: Arc<Binding>,
}
impl Drop for Registration {
    fn drop(&mut self) {
        self.binding.cancel.cancel();
        self.binding.conn.close();
        if let Some(state) = self.state.upgrade() {
            let mut lanes = lock(&state.lanes);
            if lanes
                .get(&self.key)
                .is_some_and(|l| l.id == self.binding.id)
            {
                lanes.remove(&self.key);
            }
            drop(lanes);
            *lock(&state.refresh_at) = Instant::now();
        }
    }
}

pub fn register(
    ctx: &Arc<Ctx>,
    peer: Arc<PeerContext>,
    lane: LaneSpec,
    conn: Conn,
    cancel: &Cancel,
) -> anyhow::Result<Registration> {
    register_with_legacy(ctx, peer, lane, conn, cancel, None)
}

pub(crate) fn register_with_legacy(
    ctx: &Arc<Ctx>,
    peer: Arc<PeerContext>,
    lane: LaneSpec,
    conn: Conn,
    cancel: &Cancel,
    legacy: Option<crate::model::lease::PullRun>,
) -> anyhow::Result<Registration> {
    super::scheduler::check_session(
        &peer,
        &conn,
        crate::model::protocol::LaneRole::Bulk,
        Some(lane),
    )
    .map_err(anyhow::Error::new)?;
    let transport = content_transport::daemon(ctx)?;
    let state = {
        let mut slot = lock(&transport.dispatch);
        if slot.is_none() {
            *slot = Some(Dispatcher::start(ctx, &transport)?);
        }
        slot.as_ref().unwrap().clone()
    };
    let key = (peer.installation.clone(), lane.index, lane.count);
    let binding = Arc::new(Binding {
        legacy,
        id: Uuid::new_v4(),
        peer,
        lane,
        conn,
        cancel: cancel.child(),
    });
    let old = {
        let mut lanes = lock(&state.lanes);
        anyhow::ensure!(
            lanes.len() < 256 || lanes.contains_key(&key),
            "CONTENT_RESOURCE_LIMIT"
        );
        lanes.insert(key.clone(), binding.clone())
    };
    if let Some(old) = old {
        old.cancel.cancel();
        old.conn.close();
    }
    *lock(&state.refresh_at) = Instant::now();
    Ok(Registration {
        state: Arc::downgrade(&state),
        key,
        binding,
    })
}

pub fn view(ctx: &Arc<Ctx>) -> Option<DispatchView> {
    let extras = ctx.extensions.get()?.downcast_ref::<super::ctx::Extras>()?;
    let transport = lock(&extras.content).clone()?;
    let state = lock(&transport.dispatch).clone()?;
    Some(state.view())
}

#[cfg(test)]
mod group_lifetime_tests {
    use super::*;

    fn group(repo: usize) -> Arc<Group> {
        Arc::new(Group {
            repo: repo.to_string(),
            device: "local".into(),
            endpoint: WorkerEndpoint::default(),
            sources: vec![],
            state: GroupState::new(Cancel::new()),
        })
    }

    #[test]
    fn a_deferred_live_group_keeps_its_owner_until_authority_is_withdrawn() {
        let mut registry = GroupRegistry::default();
        let old = group(0);
        assert!(registry.retain(&old));
        let lane = Cancel::new();
        let task = lane.linked(&old.state.cancel);
        // Several complete page rotations cannot retire work merely because
        // its repository is not selected. Only the authority predicate may.
        for page in 1..5 {
            let selected = group(page);
            assert!(registry.retain(&selected));
            assert!(registry.live(|_| true).iter().any(|g| Arc::ptr_eq(g, &old)));
            assert!(!task.is_cancelled());
        }
        let remaining = registry.live(|g| g.repo != old.repo);
        assert!(remaining.is_empty());
        assert!(task.is_cancelled() && old.state.cancel.is_cancelled());
        assert!(!lane.is_cancelled());
        let replacement = group(0);
        assert!(registry.retain(&replacement));
        assert!(!replacement.state.cancel.is_cancelled());
        assert_eq!(registry.live(|_| true).len(), 1);
        assert!(task.is_cancelled());
    }

    #[test]
    fn group_registry_does_not_retain_completed_work_or_dead_pages() {
        let mut registry = GroupRegistry::default();
        for repo in 0..GROUP_PAGE {
            assert!(registry.retain(&group(repo)));
        }
        assert!(registry.live(|_| true).is_empty());
        assert!(registry.owners.is_empty());
    }

    #[test]
    fn group_cancellation_waits_for_the_short_effect_projection_barrier() {
        let state = GroupState::new(Cancel::new());
        let projection = lock(&state.effects);
        let (sent, returned) = std::sync::mpsc::channel();
        let token = state.cancel.clone();
        let cancelling = thread::spawn(move || {
            token.cancel();
            sent.send(()).unwrap();
        });
        let until = Instant::now() + Duration::from_secs(1);
        while !state.cancel.is_cancelled() && Instant::now() < until {
            thread::yield_now();
        }
        assert!(state.cancel.is_cancelled());
        assert_eq!(
            returned.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        );
        drop(projection);
        returned.recv_timeout(Duration::from_secs(1)).unwrap();
        cancelling.join().unwrap();
    }

    #[test]
    fn full_group_registry_refuses_new_owners_without_evicting_live_tasks() {
        let mut registry = GroupRegistry::default();
        let mut owners: Vec<_> = (0..GROUP_OWNERS).map(group).collect();
        for owner in &owners {
            assert!(registry.retain(owner));
        }
        assert!(!registry.retain(&group(GROUP_OWNERS)));
        assert!(owners.iter().all(|g| !g.state.cancel.is_cancelled()));
        // A withdrawn owner can be untracked while its cancelled Task drains.
        let draining = owners.pop().unwrap();
        let live = registry.live(|g| g.repo != draining.repo);
        assert_eq!(live.len(), GROUP_OWNERS - 1);
        assert!(draining.state.cancel.is_cancelled());
        assert!(owners.iter().all(|g| !g.state.cancel.is_cancelled()));
        assert!(registry.retain(&group(GROUP_OWNERS)));
        assert_eq!(registry.owners.len(), GROUP_OWNERS);
    }
}
