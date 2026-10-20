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
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Condvar, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;

type LaneKey = (String, u32, u32);
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
    fn new() -> Self {
        Self {
            busy: array::from_fn(|_| AtomicBool::new(false)),
            due: array::from_fn(|_| Mutex::new(Instant::now())),
            source: array::from_fn(|_| AtomicUsize::new(0)),
            empty: array::from_fn(|_| AtomicUsize::new(0)),
            polling: AtomicBool::new(false),
            poll: Mutex::new(None),
            poll_turn: AtomicUsize::new(0),
            legacy_turn: AtomicUsize::new(0),
            last_error: Mutex::new(None),
        }
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
    completed: [AtomicU64; 3],
    failed: [AtomicU64; 3],
    retried: [AtomicU64; 3],
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
    pub completed: [u64; 3],
    pub failed: [u64; 3],
    pub retried: [u64; 3],
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
                            state.running.fetch_add(1, Ordering::Relaxed);
                            let result =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    job.task.perform()
                                }));
                            state.running.fetch_sub(1, Ordering::Relaxed);
                            match result {
                                Ok(Ok(_)) => {
                                    state.completed[job.class].fetch_add(1, Ordering::Relaxed);
                                    *lock(&job.group.state.last_error) = None;
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
        DispatchView {
            lanes: lock(&self.lanes).len(),
            groups: groups.len(),
            deferred: self.deferred.load(Ordering::Relaxed),
            preparing: self.preparing.load(Ordering::Relaxed),
            queued: lock(&self.queue).len(),
            running: self.running.load(Ordering::Relaxed),
            peak_queue: self.peak_queue.load(Ordering::Relaxed),
            admitted: array::from_fn(|i| self.admitted[i].load(Ordering::Relaxed)),
            completed: array::from_fn(|i| self.completed[i].load(Ordering::Relaxed)),
            failed: array::from_fn(|i| self.failed[i].load(Ordering::Relaxed)),
            retried: array::from_fn(|i| self.retried[i].load(Ordering::Relaxed)),
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
        let old = lock(&self.groups).clone();
        let old: BTreeMap<_, _> = old
            .iter()
            .map(|g| ((g.repo.clone(), g.endpoint.instance.clone()), g.clone()))
            .collect();
        let mut folders: BTreeMap<_, _> = ctx
            .folders
            .all()
            .into_iter()
            .map(|f| (f.record.repo.clone(), f))
            .collect();
        self.deferred
            .store(folders.len().saturating_sub(512), Ordering::Relaxed);
        if folders.len() > 512 {
            let first = self.group_turn.fetch_add(512, Ordering::Relaxed) % folders.len();
            let keys: std::collections::BTreeSet<_> = folders
                .keys()
                .cycle()
                .skip(first)
                .take(512)
                .cloned()
                .collect();
            folders.retain(|key, _| keys.contains(key));
        }
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
        let mut groups: BTreeMap<String, Group> = BTreeMap::new();
        for (peer, lanes) in peers {
            for remote in &lanes[0].peer.catalog.load().entries {
                let Some(folder) = folders.get(&remote.repo) else {
                    continue;
                };
                if readiness(folder, remote) != Readiness::Ready
                    || folder.record.device == remote.replica
                {
                    continue;
                }
                let Some(endpoint) = folder.worker.clone() else {
                    continue;
                };
                let g = groups.entry(remote.repo.clone()).or_insert_with(|| Group {
                    repo: remote.repo.clone(),
                    device: folder.record.device.clone(),
                    state: old
                        .get(&(remote.repo.clone(), endpoint.instance.clone()))
                        .map(|g| g.state.clone())
                        .unwrap_or_else(|| Arc::new(GroupState::new())),
                    endpoint,
                    sources: vec![],
                });
                if !g.sources.iter().any(|s| s.peer == peer) {
                    g.sources.push(Source {
                        peer: peer.clone(),
                        replica: remote.replica.clone(),
                        lanes: lanes.clone(),
                    });
                }
            }
        }
        *lock(&self.groups) = Arc::new(
            groups
                .into_values()
                .map(|mut g| {
                    if let Some(previous) = old.get(&(g.repo.clone(), g.endpoint.instance.clone()))
                        && !same_sources(&previous.sources, &g.sources)
                    {
                        g.state = Arc::new(GroupState::new());
                    }
                    Arc::new(g)
                })
                .collect(),
        );
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
        *lock(&group.state.poll) = Some(PollCache {
            until: Instant::now() + ttl,
            activity: activity.clone(),
            candidates,
        });
        Ok(Some(activity))
    }

    fn prepare(&self, class: usize) -> bool {
        self.refresh();
        let groups = lock(&self.groups).clone();
        if groups.is_empty() {
            return false;
        }
        let group =
            groups[self.cursor[class].fetch_add(1, Ordering::Relaxed) % groups.len()].clone();
        if Instant::now() < *lock(&group.state.due[class])
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
                            cancel: lane.cancel.child(),
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
                        + Duration::from_millis(if class == 0 {
                            10
                        } else {
                            (10_u64 << empty).min(1000)
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
                        lane.cancel.child(),
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
        let mut queue = lock(&self.queue);
        if queue.len() >= 4 || self.cancel.is_cancelled() {
            drop(queue);
            drop(task);
            return false;
        }
        queue.push_back(Job { task, group, class });
        self.peak_queue.fetch_max(queue.len(), Ordering::Relaxed);
        self.admitted[class].fetch_add(1, Ordering::Relaxed);
        drop(queue);
        self.ready.notify_one();
        true
    }

    fn error(&self, group: &Group, class: usize, error: &CallError) {
        if matches!(error, CallError::Cancelled | CallError::ConnectionLost(_)) {
            return;
        }
        let wire = error.to_wire();
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
