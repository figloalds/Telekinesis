//! WP2.7 acceptance: the MetaScheduler (`orchestrator::scheduler`), one
//! metadata exchange (`orchestrator::meta_exchange`) and the interim bulk
//! lane (`orchestrator::objects`); REVAMP_PLAN §3.7, §6.5 and §14 WP2.7.
//!
//! Every test runs a lane against a real authenticated v3 session
//! (`session::dial` ↔ `session::accept`). The server side is a scripted
//! handler standing in for the peer, and the local worker is a
//! `FakeWorker` behind a fake folder directory.
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    net::{SocketAddr, TcpListener},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tkfs::{
    bulk_sync::{self, Page},
    core::ObjectPart,
    model::{
        errors::{ErrorClass, ErrorWire, Scope},
        health::Generations,
        ipc::WorkerHeartbeat,
        orchestrator::{
            CallError, CatalogApi, CatalogCmd, CatalogSnapshot, Ctx, DaemonEndpoints, EntryState,
            FeedTip, FolderCmd, FolderDirectory, FolderPhase, FolderRecord, FolderView,
            GateEvidence, GateState, PeerContext, PublishedCatalogApi, PublishedEntry,
            RemoteCatalog, ShutdownOutcome, StatusSink, StatusSource, WorkerClient, WorkerEndpoint,
        },
        protocol::{
            LaneRole, LaneSpec,
            feed::{
                Cursor, ExchangeSummary, FEED_FORMAT, FeedApplyResult, FeedPage, FeedState,
                GateView, MetaExchangeReply, MetaExchangeRequest, PageStatus,
            },
            frame::{ChunkHeader, ClientHello},
            ops,
        },
        status::{
            Direction, FolderPeerState, FolderSyncState, LaneState, StatusEvent, StatusSnapshot,
        },
    },
    orchestrator::{
        Config, Network,
        hub::{HubConfig, StatusHub},
        meta_exchange::{self, ExchangeOptions},
        objects::{self, BulkOptions},
        scheduler::{
            self, ControlOptions, DriveOptions, FailureKind, KeyPhase, Readiness, SchedulerView,
        },
    },
    paired_sync::SyncRoute,
    pairing_code::Verifier,
    session::{self, AcceptPolicy, CodeAuth, Conn, ConnOptions, Handler, Incoming, Priority},
    util::{cancel::Cancel, snapshot::Snapshot, time::SystemClock},
};

const PEER: &str = "peer-installation-b";
const REMOTE_EPOCH: &str = "remote-epoch";
/// Lane 1 of 2 owns hashes whose first 16 hex digits are odd.
const H_UP: &str = "0000000000000001aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const H_DOWN: &str = "0000000000000003bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn sleep_ms(ms: u64) {
    thread::sleep(Duration::from_millis(ms));
}

/// Polls `check` every 5 ms until it holds or `limit` passes.
fn wait_until(limit: Duration, mut check: impl FnMut() -> bool) -> bool {
    let until = Instant::now() + limit;
    loop {
        if check() {
            return true;
        }
        if Instant::now() >= until {
            return false;
        }
        sleep_ms(5);
    }
}

// ---------------------------------------------------------------------------
// The local side: folders, worker, status

#[derive(Clone, Debug)]
struct WorkerCall {
    at: Instant,
    op: String,
    repo: String,
    payload: Value,
}

type Script = Arc<dyn Fn(&str, &Value) -> Option<Result<Value, CallError>> + Send + Sync>;

/// The local worker: `feed-offer` builds a fresh request per call,
/// `feed-apply` answers `apply_result`, and the interim bulk ops play a
/// two-object scenario (we want `H_DOWN`, the peer wants `H_UP`). A script
/// can override any call.
struct FakeWorker {
    calls: Mutex<Vec<WorkerCall>>,
    script: Mutex<Option<Script>>,
    apply_result: Mutex<FeedApplyResult>,
    installed: Mutex<HashSet<String>>,
}

impl FakeWorker {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            script: Mutex::new(None),
            apply_result: Mutex::new(FeedApplyResult {
                summary: ExchangeSummary {
                    caught_up: true,
                    ..ExchangeSummary::default()
                },
                gate: GateView {
                    caught_up: true,
                    ..GateView::default()
                },
            }),
            installed: Mutex::new(HashSet::new()),
        })
    }

    fn script(
        &self,
        script: impl Fn(&str, &Value) -> Option<Result<Value, CallError>> + Send + Sync + 'static,
    ) {
        *self.script.lock().unwrap() = Some(Arc::new(script));
    }

    fn calls(&self, op: &str, repo: &str) -> Vec<WorkerCall> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| call.op == op && call.repo == repo)
            .cloned()
            .collect()
    }

    fn count(&self, op: &str, repo: &str) -> usize {
        self.calls(op, repo).len()
    }

    fn reply(&self, op: &str, payload: &Value) -> Result<Value, CallError> {
        let route: SyncRoute = serde_json::from_value(payload["route"].clone()).unwrap();
        match op {
            "feed-offer" => Ok(json!(offer(&route, &uuid()))),
            "feed-apply" => {
                let reply: MetaExchangeReply =
                    serde_json::from_value(payload["reply"].clone()).unwrap();
                assert_eq!(payload["request_id"], json!(reply.reply_to));
                Ok(json!(*self.apply_result.lock().unwrap()))
            }
            "published-bulk-offer" => {
                let lane: bulk_sync::Lane =
                    serde_json::from_value(payload["lane"].clone()).unwrap();
                let inventory: Option<Page> =
                    serde_json::from_value(payload["inventory"].clone()).unwrap();
                if let Some(inventory) = &inventory {
                    check_route(inventory, &route, lane)?;
                }
                let installed = self.installed.lock().unwrap().contains(&route.repo);
                let mut page = page(
                    &route.repo,
                    &route.local_replica,
                    &route.remote_replica,
                    lane,
                );
                if !installed {
                    page.wanted_objects.insert(H_DOWN.into());
                }
                if inventory.is_some_and(|inventory| inventory.wanted_objects.contains(H_UP)) {
                    page.object_parts.insert(H_UP.into(), part("abcd"));
                }
                Ok(json!(page))
            }
            "published-bulk-apply" => {
                let lane: bulk_sync::Lane =
                    serde_json::from_value(payload["lane"].clone()).unwrap();
                let reply: Page = serde_json::from_value(payload["page"].clone()).unwrap();
                check_route(&reply, &route, lane)?;
                assert_eq!(payload["request"], json!(reply.reply_to.clone().unwrap()));
                if reply.object_parts.contains_key(H_DOWN) {
                    self.installed.lock().unwrap().insert(route.repo.clone());
                }
                Ok(json!({"received_object_bytes": reply.object_bytes()}))
            }
            other => Err(CallError::Remote(ErrorWire::coded(
                "UNSUPPORTED_OPERATION",
                other,
            ))),
        }
    }
}

impl WorkerClient for FakeWorker {
    fn instance(&self) -> &str {
        "fake-worker"
    }

    fn call(&self, op: &str, payload: Value, _: Duration) -> Result<Value, CallError> {
        let repo = payload["route"]["repo"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        self.calls.lock().unwrap().push(WorkerCall {
            at: Instant::now(),
            op: op.to_owned(),
            repo,
            payload: payload.clone(),
        });
        let script = self.script.lock().unwrap().clone();
        if let Some(script) = script
            && let Some(result) = script(op, &payload)
        {
            return result;
        }
        self.reply(op, &payload)
    }

    fn call_bytes(
        &self,
        op: &str,
        _: Value,
        _: &[u8],
        _: Duration,
    ) -> Result<(Value, Vec<u8>), CallError> {
        Err(CallError::Unavailable(format!(
            "BINARY_OPS_REQUIRE_SESSION: {op}"
        )))
    }
}

/// The real worker's check of a peer page (an inventory or a reply) against
/// the route and lane of the call (`bulk_sync::Page::validate`): it comes
/// from the remote replica, to us. `BULK_ROUTE_MISMATCH` is Fatal.
fn check_route(page: &Page, route: &SyncRoute, lane: bulk_sync::Lane) -> Result<(), CallError> {
    if page.lane != lane {
        return Err(CallError::Remote(ErrorWire::coded(
            "INCOMPATIBLE_BULK_LANE",
            format!("page lane {:?}, call lane {lane:?}", page.lane),
        )));
    }
    if page.repo != route.repo
        || page.sender != route.remote_replica
        || page.receiver != route.local_replica
    {
        return Err(CallError::Remote(ErrorWire::coded(
            "BULK_ROUTE_MISMATCH",
            format!(
                "page {} -> {} for {}, route {} -> {} for {}",
                page.sender,
                page.receiver,
                page.repo,
                route.remote_replica,
                route.local_replica,
                route.repo
            ),
        )));
    }
    Ok(())
}

fn offer(route: &SyncRoute, request: &str) -> MetaExchangeRequest {
    MetaExchangeRequest {
        format: FEED_FORMAT,
        request: request.to_owned(),
        sender: route.local_replica.clone(),
        receiver: route.remote_replica.clone(),
        repo: route.repo.clone(),
        page: FeedPage {
            epoch: "local-epoch".into(),
            after: None,
            reset: false,
            entries: vec![],
            through: Cursor::start("local-epoch"),
            tip: 0,
            more: false,
        },
        cursor: None,
        need: vec![],
        state: FeedState::default(),
    }
}

fn page(repo: &str, sender: &str, receiver: &str, lane: bulk_sync::Lane) -> Page {
    Page {
        format: bulk_sync::PROTOCOL,
        repo: repo.into(),
        sender: sender.into(),
        receiver: receiver.into(),
        request: uuid(),
        reply_to: None,
        lane,
        known_objects: BTreeSet::new(),
        partial_objects: BTreeMap::new(),
        wanted_objects: BTreeSet::new(),
        object_parts: BTreeMap::new(),
    }
}

fn part(hex: &str) -> ObjectPart {
    ObjectPart {
        offset: 0,
        total: (hex.len() / 2) as u64,
        hex: hex.into(),
    }
}

#[derive(Clone, Debug)]
struct Fin {
    at: Instant,
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

/// The folder directory, catalog, published catalog and status sink of one
/// test daemon.
struct World {
    folders: Snapshot<BTreeMap<String, Arc<FolderView>>>,
    worker: Arc<FakeWorker>,
    events: Mutex<Vec<(Instant, StatusEvent)>>,
    commands: Mutex<Vec<(String, FolderCmd)>>,
    /// A real StatusHub the events are also recorded into, when set.
    hub: OnceLock<Arc<StatusHub>>,
    /// `FolderDirectory::by_repo` calls (one per key per scheduler turn).
    lookups: AtomicUsize,
}

impl World {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            folders: Snapshot::new(BTreeMap::new()),
            worker: FakeWorker::new(),
            events: Mutex::new(Vec::new()),
            commands: Mutex::new(Vec::new()),
            hub: OnceLock::new(),
            lookups: AtomicUsize::new(0),
        })
    }

    fn put(&self, view: FolderView) {
        self.folders.update(|folders| {
            let mut folders = folders.clone();
            folders.insert(view.record.state.clone(), Arc::new(view.clone()));
            folders
        });
    }

    fn edit(&self, state: &str, edit: impl FnOnce(&mut FolderView)) {
        self.folders.update(|folders| {
            let mut folders = folders.clone();
            let mut view = (*folders[state]).clone();
            edit(&mut view);
            view.generation += 1;
            folders.insert(state.to_owned(), Arc::new(view));
            folders
        });
    }

    fn bump_sync(&self, state: &str) {
        self.edit(state, |view| {
            let mut heartbeat = (**view.heartbeat.as_ref().unwrap()).clone();
            heartbeat.seq += 1;
            heartbeat.generations.sync += 1;
            view.heartbeat = Some(Arc::new(heartbeat));
        });
    }

    fn bump_tip(&self, state: &str) {
        self.edit(state, |view| {
            let mut heartbeat = (**view.heartbeat.as_ref().unwrap()).clone();
            heartbeat.seq += 1;
            let tip = heartbeat.tip.get_or_insert_with(FeedTip::default);
            tip.seq += 1;
            view.heartbeat = Some(Arc::new(heartbeat));
        });
    }

    fn finished(&self) -> Vec<Fin> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(at, event)| match event {
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
                } => {
                    assert_eq!(installation, PEER);
                    Some(Fin {
                        at: *at,
                        repo: repo.clone(),
                        lane: *lane,
                        op: op.clone(),
                        direction: *direction,
                        ok: *ok,
                        error: error.clone(),
                        summary: summary.clone(),
                        bytes_up: *bytes_up,
                        bytes_down: *bytes_down,
                        objects_up: *objects_up,
                        objects_down: *objects_down,
                        duration_ms: *duration_ms,
                    })
                }
                _ => None,
            })
            .collect()
    }

    fn finished_of(&self, repo: &str) -> Vec<Fin> {
        self.finished()
            .into_iter()
            .filter(|fin| fin.repo == repo)
            .collect()
    }

    fn errors(&self) -> Vec<(Scope, ErrorWire)> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(_, event)| match event {
                StatusEvent::Error { scope, error } => Some((scope.clone(), error.clone())),
                _ => None,
            })
            .collect()
    }

    fn evidence(&self) -> Vec<(String, GateEvidence)> {
        self.commands
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(state, cmd)| match cmd {
                FolderCmd::GateEvidence(evidence) => Some((state.clone(), evidence.clone())),
                _ => None,
            })
            .collect()
    }
}

impl FolderDirectory for World {
    fn view(&self, state: &str) -> Option<Arc<FolderView>> {
        self.folders.load().get(state).cloned()
    }
    fn by_repo(&self, repo: &str) -> Option<Arc<FolderView>> {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        self.folders
            .load()
            .values()
            .find(|view| view.record.repo == repo)
            .cloned()
    }
    fn all(&self) -> Vec<Arc<FolderView>> {
        self.folders.load().values().cloned().collect()
    }
    fn send(&self, state: &str, cmd: FolderCmd) -> Result<(), ErrorWire> {
        self.commands.lock().unwrap().push((state.to_owned(), cmd));
        Ok(())
    }
    fn request(
        &self,
        state: &str,
        cmd: FolderCmd,
        _: Duration,
    ) -> Result<Arc<FolderView>, ErrorWire> {
        self.send(state, cmd)?;
        self.view(state)
            .ok_or_else(|| ErrorWire::new("FOLDER_NOT_FOUND", state))
    }
    fn client(&self, state: &str) -> Option<Arc<dyn WorkerClient>> {
        let view = self.view(state)?;
        matches!(view.phase, FolderPhase::Running { .. })
            .then(|| self.worker.clone() as Arc<dyn WorkerClient>)
    }
    fn generation(&self) -> u64 {
        self.folders.generation()
    }
    fn wait_newer(&self, generation: u64, timeout: Duration) -> u64 {
        self.folders.wait_newer(generation, timeout)
    }
    fn shutdown(&self, _: Duration) -> Vec<ShutdownOutcome> {
        vec![]
    }
}

impl CatalogApi for World {
    fn snapshot(&self) -> Arc<CatalogSnapshot> {
        Arc::new(CatalogSnapshot::default())
    }
    fn apply(&self, cmd: CatalogCmd) -> Result<Arc<CatalogSnapshot>, ErrorWire> {
        Err(ErrorWire::new("CATALOG_UNAVAILABLE", cmd.name()))
    }
    fn wait_newer(&self, _: u64, _: Duration) -> Arc<CatalogSnapshot> {
        CatalogApi::snapshot(self)
    }
}

impl PublishedCatalogApi for World {
    fn update(&self, _: &str, _: Option<PublishedEntry>) {}
    fn list(&self) -> (u64, Arc<Vec<PublishedEntry>>) {
        (0, Arc::new(vec![]))
    }
    fn wait_newer(&self, generation: u64, _: Duration) -> u64 {
        generation
    }
}

impl StatusSink for World {
    fn record(&self, event: StatusEvent) {
        if let Some(hub) = self.hub.get() {
            hub.record(event.clone());
        }
        self.events.lock().unwrap().push((Instant::now(), event));
    }
}

impl StatusSource for World {
    fn snapshot(&self) -> Arc<StatusSnapshot> {
        Arc::new(StatusSnapshot::default())
    }
    fn wait_newer(&self, _: u64, _: Duration) -> Arc<StatusSnapshot> {
        StatusSource::snapshot(self)
    }
}

fn ctx(world: &Arc<World>, interval_ms: u64) -> Arc<Ctx> {
    Arc::new(Ctx {
        // Unique per test: the scheduler's process-wide registries key on it.
        installation: format!("local-{}", uuid()),
        config: Arc::new(Config {
            format_version: 2,
            data_directory: "data".into(),
            default_mount_directory: "mounts".into(),
            maximum_running: 8,
            network: Network {
                listen: None,
                connections: 4,
                sync_interval_ms: interval_ms,
            },
        }),
        catalog: world.clone(),
        folders: world.clone(),
        published: world.clone(),
        status: world.clone(),
        status_source: world.clone(),
        network: OnceLock::new(),
        legacy: OnceLock::new(),
        clock: Arc::new(SystemClock),
        shutdown: Cancel::new(),
        stopping: Arc::new(AtomicBool::new(false)),
        started_at_ms: 0,
        endpoints: Snapshot::new(DaemonEndpoints::default()),
    })
}

fn state_of(repo: &str) -> String {
    format!("state-{repo}")
}

fn device_of(repo: &str) -> String {
    format!("local-device-{repo}")
}

fn replica_of(repo: &str) -> String {
    format!("remote-replica-{repo}")
}

/// A Running folder for `repo` with sync enabled and a heartbeat.
fn folder(repo: &str) -> FolderView {
    FolderView {
        generation: 1,
        record: FolderRecord {
            state: state_of(repo),
            label: repo.into(),
            repo: repo.into(),
            device: device_of(repo),
            directory: format!("data/{repo}").into(),
            mount: None,
            desired_running: true,
            sync_enabled: true,
            mount_enabled: false,
        },
        phase: FolderPhase::Running {
            since_ms: 1,
            instance: "worker-instance-1".into(),
            pid: 1,
            address: "ws://127.0.0.1:1".into(),
            adopted: false,
        },
        worker: Some(WorkerEndpoint {
            instance: "worker-instance-1".into(),
            pid: 1,
            address: "ws://127.0.0.1:1".into(),
            version: "test".into(),
            token: None,
        }),
        heartbeat: Some(Arc::new(WorkerHeartbeat {
            instance: "worker-instance-1".into(),
            seq: 1,
            generations: Generations::default(),
            tip: Some(FeedTip {
                epoch: "local-epoch".into(),
                seq: 1,
            }),
            ..WorkerHeartbeat::default()
        })),
        ..FolderView::default()
    }
}

fn starting(view: &mut FolderView) {
    view.phase = FolderPhase::Starting {
        since_ms: 1,
        attempt: 1,
        phase: "opening store".into(),
        progress_seq: 1,
        adopting: false,
    };
    view.worker = None;
}

fn entry(repo: &str, state: EntryState) -> PublishedEntry {
    PublishedEntry {
        repo: repo.into(),
        replica: replica_of(repo),
        label: repo.into(),
        branches: vec![],
        state,
        tip: Some(FeedTip {
            epoch: REMOTE_EPOCH.into(),
            seq: 1,
        }),
        sync_generation: 0,
        content: None,
        error: None,
        updated_at_ms: 0,
    }
}

fn peer_ctx(ctx: &Ctx, entries: Vec<PublishedEntry>) -> Arc<PeerContext> {
    Arc::new(PeerContext {
        installation: PEER.into(),
        endpoint: "ws://peer.test".into(),
        local_installation: ctx.installation.clone(),
        pool: 4,
        catalog: Snapshot::new(RemoteCatalog {
            generation: 1,
            at_ms: 0,
            entries,
        }),
    })
}

fn set_entry(peer: &PeerContext, entry: PublishedEntry) {
    peer.catalog.update(|catalog| {
        let mut catalog = catalog.clone();
        catalog.generation += 1;
        catalog.entries.retain(|e| e.repo != entry.repo);
        catalog.entries.push(entry);
        catalog
    });
}

// ---------------------------------------------------------------------------
// The peer side: real sessions with scripted handlers

struct TestPolicy {
    installation: String,
    verifier: Verifier,
}

impl AcceptPolicy for TestPolicy {
    fn installation(&self) -> String {
        self.installation.clone()
    }
    fn verifier(&self) -> Option<(Verifier, u64)> {
        Some((self.verifier.clone(), 1))
    }
    fn admit(&self, _: &ClientHello) -> Result<(), ErrorWire> {
        Ok(())
    }
    fn legacy_attempt(&self, _: SocketAddr, _: &Value) {}
}

/// A dialed (client, server) session pair: the client authenticates as
/// `local` to a server that is `remote`; the server serves `handler`.
fn session(
    local: &str,
    remote: &str,
    role: LaneRole,
    lane: Option<LaneSpec>,
    handler: Arc<dyn Handler>,
) -> (Conn, Conn) {
    let (code, verifier) = Verifier::generate();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let policy = Arc::new(TestPolicy {
        installation: remote.into(),
        verifier,
    });
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let options = ConnOptions {
            handlers: 16,
            ..ConnOptions::for_role(role)
        };
        let (conn, _) = session::accept(stream, policy, options).unwrap();
        conn.serve(handler);
        conn
    });
    let hello = session::client_hello(local, role, lane, 4);
    let (client, _) = session::dial(
        &endpoint,
        hello,
        &CodeAuth { code },
        None,
        ConnOptions::for_role(role),
    )
    .unwrap();
    (client, server.join().unwrap())
}

#[derive(Clone, Debug)]
enum Behave {
    Reply,
    /// Reply with `page.more` (catch-up in progress).
    More,
    /// Reply `CursorMismatch` (nothing moves).
    Mismatch,
    Delay(Duration),
    /// Never reply; give up when the client cancels.
    Hang,
    Fail(ErrorWire),
    /// Close the session on receipt.
    Close,
}

#[derive(Clone, Debug)]
struct Seen {
    at: Instant,
    repo: String,
    request: String,
    body_len: usize,
}

/// The peer's `meta.exchange` handler: one behaviour per request, from the
/// repo's plan, else the repo's fallback, else `Reply`.
#[derive(Default)]
struct MetaPeer {
    plans: Mutex<HashMap<String, VecDeque<Behave>>>,
    fallback: Mutex<HashMap<String, Behave>>,
    seen: Mutex<Vec<Seen>>,
    reply_lens: Mutex<HashMap<String, usize>>,
    active: AtomicUsize,
    peak: AtomicUsize,
    per_repo: Mutex<HashMap<String, (usize, usize)>>,
    cancelled: AtomicUsize,
    server: OnceLock<Conn>,
}

impl MetaPeer {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn plan(&self, repo: &str, plan: impl IntoIterator<Item = Behave>) {
        self.plans
            .lock()
            .unwrap()
            .insert(repo.into(), plan.into_iter().collect());
    }

    fn always(&self, repo: &str, behave: Behave) {
        self.fallback.lock().unwrap().insert(repo.into(), behave);
    }

    fn seen(&self, repo: &str) -> Vec<Seen> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|seen| seen.repo == repo)
            .cloned()
            .collect()
    }

    fn next(&self, repo: &str) -> Behave {
        if let Some(behave) = self
            .plans
            .lock()
            .unwrap()
            .get_mut(repo)
            .and_then(VecDeque::pop_front)
        {
            return behave;
        }
        self.fallback
            .lock()
            .unwrap()
            .get(repo)
            .cloned()
            .unwrap_or(Behave::Reply)
    }

    fn handler(self: &Arc<Self>) -> Arc<dyn Handler> {
        Arc::new(MetaHandler(self.clone()))
    }
}

struct MetaHandler(Arc<MetaPeer>);

impl Handler for MetaHandler {
    fn handle(&self, req: Incoming) {
        let peer = &self.0;
        assert_eq!(req.op, ops::META_EXCHANGE, "only meta.exchange on lane 0");
        let request: MetaExchangeRequest = serde_json::from_slice(&req.body).unwrap();
        assert_eq!(req.header, json!({"repo": request.repo}));
        peer.seen.lock().unwrap().push(Seen {
            at: Instant::now(),
            repo: request.repo.clone(),
            request: request.request.clone(),
            body_len: req.body.len(),
        });
        let active = peer.active.fetch_add(1, Ordering::SeqCst) + 1;
        peer.peak.fetch_max(active, Ordering::SeqCst);
        {
            let mut per_repo = peer.per_repo.lock().unwrap();
            let slot = per_repo.entry(request.repo.clone()).or_default();
            slot.0 += 1;
            slot.1 = slot.1.max(slot.0);
        }
        let behave = peer.next(&request.repo);
        let respond = |more: bool, status: PageStatus, req: Incoming| {
            let reply = serde_json::to_vec(&reply_to(&request, more, status)).unwrap();
            peer.reply_lens
                .lock()
                .unwrap()
                .insert(request.request.clone(), reply.len());
            req.responder.reply(json!({}), reply);
        };
        match behave {
            Behave::Reply => respond(false, PageStatus::Accepted, req),
            Behave::More => respond(true, PageStatus::Accepted, req),
            Behave::Mismatch => respond(
                false,
                PageStatus::CursorMismatch {
                    expected: Cursor::start("local-epoch"),
                },
                req,
            ),
            Behave::Delay(delay) => {
                thread::sleep(delay);
                respond(false, PageStatus::Accepted, req);
            }
            Behave::Hang => {
                while !req.responder.cancelled() {
                    sleep_ms(5);
                }
                peer.cancelled.fetch_add(1, Ordering::SeqCst);
            }
            Behave::Fail(error) => req.responder.error(error),
            Behave::Close => {
                if let Some(conn) = peer.server.get() {
                    conn.close();
                }
            }
        }
        peer.active.fetch_sub(1, Ordering::SeqCst);
        peer.per_repo
            .lock()
            .unwrap()
            .get_mut(&request.repo)
            .unwrap()
            .0 -= 1;
    }
}

fn reply_to(request: &MetaExchangeRequest, more: bool, status: PageStatus) -> MetaExchangeReply {
    MetaExchangeReply {
        format: FEED_FORMAT,
        reply_to: request.request.clone(),
        sender: request.receiver.clone(),
        receiver: request.sender.clone(),
        repo: request.repo.clone(),
        page_status: status,
        cursor: Cursor::start("local-epoch"),
        page: FeedPage {
            epoch: REMOTE_EPOCH.into(),
            after: request.cursor.clone(),
            reset: false,
            entries: vec![],
            through: Cursor::start(REMOTE_EPOCH),
            tip: 1,
            more,
        },
        staged: 0,
        rejected: vec![],
        need_served: vec![],
        unknown: vec![],
        state: FeedState {
            epoch: REMOTE_EPOCH.into(),
            tip: 1,
            ..FeedState::default()
        },
    }
}

// ---------------------------------------------------------------------------
// Running lanes

struct Lane {
    cancel: Cancel,
    handle: Option<thread::JoinHandle<()>>,
    view: Arc<Snapshot<SchedulerView>>,
    client: Conn,
    server: Conn,
}

impl Lane {
    fn view(&self) -> Arc<SchedulerView> {
        self.view.load()
    }

    fn key(&self, repo: &str) -> scheduler::KeyView {
        self.view()
            .key(repo)
            .cloned()
            .unwrap_or_else(|| panic!("no key {repo} in {:#?}", self.view()))
    }

    fn finished(&self) -> bool {
        self.handle
            .as_ref()
            .is_none_or(thread::JoinHandle::is_finished)
    }

    /// Cancels the lane and waits up to `limit` for it to return; how long it took.
    fn cancel_and_join(&mut self, limit: Duration) -> Duration {
        let started = Instant::now();
        self.cancel.cancel();
        assert!(
            wait_until(limit, || self.finished()),
            "the lane did not stop within {limit:?}"
        );
        if let Some(handle) = self.handle.take() {
            handle.join().unwrap();
        }
        started.elapsed()
    }

    fn stop(mut self) {
        self.client.close();
        self.server.close();
        self.cancel_and_join(Duration::from_secs(10));
    }
}

fn control_options(interval_ms: u64, deadline: Duration) -> ControlOptions {
    let mut options = ControlOptions {
        drive: DriveOptions::new(4, Duration::from_millis(interval_ms)),
        exchange: ExchangeOptions {
            offer: deadline,
            exchange: deadline,
            apply: deadline,
        },
    };
    options.drive.view = Some(Arc::new(Snapshot::new(SchedulerView::default())));
    options
}

/// Starts the control lane of `peer` on a fresh session served by `meta`.
fn start_control(
    ctx: &Arc<Ctx>,
    peer: &Arc<PeerContext>,
    meta: &Arc<MetaPeer>,
    options: ControlOptions,
) -> Lane {
    let (client, server) = session(
        &ctx.installation,
        PEER,
        LaneRole::Control,
        None,
        meta.handler(),
    );
    let _ = meta.server.set(server.clone());
    start_control_on(ctx, peer, client, server, options)
}

fn start_control_on(
    ctx: &Arc<Ctx>,
    peer: &Arc<PeerContext>,
    client: Conn,
    server: Conn,
    options: ControlOptions,
) -> Lane {
    let view = options.drive.view.clone().unwrap();
    let cancel = Cancel::new();
    let handle = {
        let (ctx, peer, conn, cancel) = (ctx.clone(), peer.clone(), client.clone(), cancel.clone());
        thread::spawn(move || scheduler::run_control_with(ctx, peer, conn, cancel, options))
    };
    Lane {
        cancel,
        handle: Some(handle),
        view,
        client,
        server,
    }
}

fn gaps(times: &[Instant]) -> Vec<Duration> {
    times.windows(2).map(|pair| pair[1] - pair[0]).collect()
}

// ---------------------------------------------------------------------------
// Tests

/// RC5: a hung remote handler for repo A delays repo B's exchanges by less
/// than 2 s; A's timeouts are charged only to A; the session stays usable.
#[test]
fn a_hung_repository_never_delays_the_others() {
    let world = World::new();
    world.put(folder("repo-a"));
    world.put(folder("repo-b"));
    let ctx = ctx(&world, 100);
    let peer = peer_ctx(
        &ctx,
        vec![
            entry("repo-a", EntryState::Running),
            entry("repo-b", EntryState::Running),
        ],
    );
    let meta = MetaPeer::new();
    meta.always("repo-a", Behave::Hang);
    let lane = start_control(
        &ctx,
        &peer,
        &meta,
        control_options(100, Duration::from_secs(2)),
    );
    let began = Instant::now();
    sleep_ms(5_500);

    let b_offers: Vec<Instant> = world
        .worker
        .calls("feed-offer", "repo-b")
        .iter()
        .map(|c| c.at)
        .collect();
    assert!(
        b_offers.len() >= 15,
        "repo B exchanged only {} times in 5.5 s",
        b_offers.len()
    );
    let worst = gaps(&b_offers).into_iter().max().unwrap();
    assert!(
        worst < Duration::from_secs(2),
        "repo B waited {worst:?} between exchanges"
    );
    // B's first exchange did not wait for A either.
    assert!(b_offers[0] - began < Duration::from_secs(1));
    assert!(world.finished_of("repo-b").iter().all(|fin| fin.ok));

    let a_finished = world.finished_of("repo-a");
    assert!(!a_finished.is_empty(), "repo A never timed out");
    for fin in &a_finished {
        assert!(!fin.ok);
        assert_eq!(fin.error.as_ref().unwrap().code, "REQUEST_TIMEOUT");
        assert!(fin.duration_ms >= 1_900, "{fin:?}");
        assert!(fin.error.as_ref().unwrap().retry_after_ms.is_some());
    }
    assert!(
        meta.cancelled.load(Ordering::SeqCst) >= 1,
        "the peer saw no CANCEL"
    );
    let key_a = lane.key("repo-a");
    let key_b = lane.key("repo-b");
    assert!(key_a.failures >= 1, "{key_a:?}");
    assert_eq!(key_a.last_error.as_ref().unwrap().code, "REQUEST_TIMEOUT");
    assert_eq!(key_b.failures, 0, "{key_b:?}");
    assert!(key_b.last_error.is_none());
    // The connection is still Ready: open, and serving calls.
    assert!(lane.client.closed().is_none());
    let pong = lane
        .client
        .call(
            ops::PING,
            json!({}),
            vec![],
            Priority::Control,
            Duration::from_secs(5),
        )
        .unwrap();
    assert_eq!(pong.header["alive"], true);
    assert!(!lane.view().stopped);
    lane.stop();
}

/// RC5 with a warm slot pool (fix round 2): when one turn dispatches more
/// keys than there are idle slot threads, every key still gets a thread at
/// once. Here one earlier exchange left one idle slot thread; then one
/// catalog update makes two keys due in the same turn, and the first hangs.
/// The second must not wait for the first one's deadline (5 s here, 30 s by
/// default), and the view's `in_flight` counts only exchanges that really run.
#[test]
fn keys_due_in_one_turn_never_queue_behind_a_hung_one() {
    let world = World::new();
    world.put(folder("repo-a"));
    world.put(folder("repo-b"));
    let ctx = ctx(&world, 60_000);
    let peer = peer_ctx(
        &ctx,
        vec![
            entry("repo-a", EntryState::Running),
            entry("repo-b", EntryState::Starting),
        ],
    );
    let meta = MetaPeer::new();
    let lane = start_control(
        &ctx,
        &peer,
        &meta,
        control_options(60_000, Duration::from_secs(5)),
    );
    // A exchanges once on its own: the lane now has one slot thread, idle.
    assert!(wait_until(Duration::from_secs(3), || world
        .finished_of("repo-a")
        .len()
        == 1));
    sleep_ms(300);
    assert_eq!(lane.view().in_flight, 0);
    assert_eq!(lane.key("repo-b").readiness, Readiness::WaitingForRemote);

    // From now on A hangs at the peer. One catalog update moves A's remote
    // tip (a trigger) and makes B's entry Running (a reset): both keys are
    // due in the same turn, A first.
    meta.always("repo-a", Behave::Hang);
    let changed = Instant::now();
    peer.catalog.update(|catalog| {
        let mut catalog = catalog.clone();
        catalog.generation += 1;
        for entry in &mut catalog.entries {
            entry.state = EntryState::Running;
            if entry.repo == "repo-a" {
                entry.tip.as_mut().unwrap().seq += 1;
            }
        }
        catalog
    });
    assert!(
        wait_until(Duration::from_secs(8), || world
            .worker
            .count("feed-apply", "repo-b")
            >= 1),
        "repo B never exchanged"
    );
    let b_started = world.worker.calls("feed-offer", "repo-b")[0].at - changed;
    assert!(
        b_started < Duration::from_secs(1),
        "repo B started {b_started:?} after it became due (behind the hung repo A)"
    );
    // A's second exchange really runs (it hangs at the peer), and it is the
    // only exchange the view counts in flight once B has finished.
    assert!(wait_until(Duration::from_secs(2), || meta
        .seen("repo-a")
        .len()
        == 2));
    assert!(
        wait_until(Duration::from_secs(2), || {
            let view = lane.view();
            view.in_flight == 1
                && view.finished == 2
                && view.key("repo-a").map(|key| key.phase) == Some(KeyPhase::Running)
        }),
        "{:#?}",
        lane.view()
    );
    assert_eq!(meta.active.load(Ordering::SeqCst), 1);
    assert_eq!(lane.key("repo-b").failures, 0);
    lane.stop();
}

/// Invariant 16: a request whose outcome is uncertain is never sent again;
/// every attempt is a fresh offer with a new id.
#[test]
fn uncertain_requests_are_never_replayed() {
    let world = World::new();
    world.put(folder("repo-a"));
    let ctx = ctx(&world, 100);
    let peer = peer_ctx(&ctx, vec![entry("repo-a", EntryState::Running)]);
    let meta = MetaPeer::new();
    meta.plan("repo-a", [Behave::Hang, Behave::Hang, Behave::Reply]);
    let mut options = control_options(60_000, Duration::from_millis(400));
    options.drive.interval = Duration::from_secs(60);
    let lane = start_control(&ctx, &peer, &meta, options);
    assert!(
        wait_until(Duration::from_secs(10), || world
            .worker
            .count("feed-apply", "repo-a")
            == 1),
        "repo A never completed an exchange"
    );
    let seen = meta.seen("repo-a");
    assert_eq!(seen.len(), 3, "{seen:#?}");
    let ids: HashSet<&str> = seen.iter().map(|seen| seen.request.as_str()).collect();
    assert_eq!(ids.len(), 3, "a request id was sent twice: {seen:#?}");
    // Every request on the wire came from its own offer.
    assert_eq!(world.worker.count("feed-offer", "repo-a"), 3);
    let apply = &world.worker.calls("feed-apply", "repo-a")[0];
    assert_eq!(apply.payload["request_id"], json!(seen[2].request));
    let codes: Vec<String> = world
        .finished_of("repo-a")
        .iter()
        .map(|fin| fin.error.as_ref().map_or("ok".into(), |e| e.code.clone()))
        .collect();
    assert_eq!(codes, ["REQUEST_TIMEOUT", "REQUEST_TIMEOUT", "ok"]);
    lane.stop();
}

/// A worker that hands out the same request id twice never gets it sent
/// twice (the replay guard), and the refusal is a counted failure.
#[test]
fn a_request_id_is_never_sent_twice() {
    let world = World::new();
    world.put(folder("repo-a"));
    let ctx = ctx(&world, 100);
    let peer = peer_ctx(&ctx, vec![entry("repo-a", EntryState::Running)]);
    let fixed = uuid();
    {
        let fixed = fixed.clone();
        world.worker.script(move |op, payload| {
            (op == "feed-offer").then(|| {
                let route: SyncRoute = serde_json::from_value(payload["route"].clone()).unwrap();
                Ok(json!(offer(&route, &fixed)))
            })
        });
    }
    let meta = MetaPeer::new();
    let lane = start_control(
        &ctx,
        &peer,
        &meta,
        control_options(100, Duration::from_secs(2)),
    );
    assert!(wait_until(Duration::from_secs(5), || {
        world.finished_of("repo-a").iter().any(|fin| {
            fin.error
                .as_ref()
                .is_some_and(|e| e.code == "REQUEST_ID_REUSED")
        })
    }));
    assert_eq!(
        meta.seen("repo-a").len(),
        1,
        "the repeated id went out again"
    );
    assert_eq!(meta.seen("repo-a")[0].request, fixed);
    assert!(lane.key("repo-a").failures >= 1);
    lane.stop();
}

/// A lost session is not the key's failure: the scheduler stops (the
/// PeerSupervisor redials), and the next session starts with a fresh request.
#[test]
fn a_lost_session_stops_the_lane_without_charging_the_key() {
    let world = World::new();
    world.put(folder("repo-a"));
    let ctx = ctx(&world, 100);
    let peer = peer_ctx(&ctx, vec![entry("repo-a", EntryState::Running)]);
    let meta = MetaPeer::new();
    meta.plan("repo-a", [Behave::Close]);
    let mut lane = start_control(
        &ctx,
        &peer,
        &meta,
        control_options(100, Duration::from_secs(5)),
    );
    assert!(
        wait_until(Duration::from_secs(5), || lane.finished()),
        "the lane kept running on a closed session"
    );
    lane.cancel_and_join(Duration::from_secs(1));
    let view = lane.view();
    assert!(view.stopped);
    assert_eq!(view.key("repo-a").unwrap().failures, 0, "{view:#?}");
    let first = meta.seen("repo-a");
    assert_eq!(first.len(), 1);
    let lost = world.finished_of("repo-a");
    assert_eq!(lost.len(), 1);
    assert!(
        lost[0].error.as_ref().unwrap().code == "CONNECTION_LOST",
        "{lost:?}"
    );
    assert!(lost[0].error.as_ref().unwrap().retry_after_ms.is_none());

    // The respawned lane: a new session and a fresh request.
    let meta2 = MetaPeer::new();
    let lane2 = start_control(
        &ctx,
        &peer,
        &meta2,
        control_options(100, Duration::from_secs(5)),
    );
    assert!(wait_until(Duration::from_secs(5), || world
        .worker
        .count("feed-apply", "repo-a")
        >= 1));
    let second = meta2.seen("repo-a");
    assert_ne!(second[0].request, first[0].request);
    lane2.stop();
}

/// WaitingForWorker, WaitingForRemote, Paused and Contention are never
/// counted as failures; a Transient failure after a Contention streak backs
/// off from its first attempt. The real StatusHub shows none of the waiting
/// answers as an error either: a local worker that cannot be reached while
/// its folder still says Running (repo E) is reported without an error, and
/// the remote's "starting" (repo B) is the pair's WaitingForRemote.
#[test]
fn waiting_states_and_contention_are_not_counted() {
    let world = World::new();
    let hub = StatusHub::new(HubConfig::default(), Arc::new(SystemClock));
    let _ = world.hub.set(hub.clone());
    let mut a = folder("repo-a");
    starting(&mut a);
    world.put(a);
    world.put(folder("repo-b"));
    let mut c = folder("repo-c");
    c.record.sync_enabled = false;
    world.put(c);
    world.put(folder("repo-d"));
    world.put(folder("repo-e"));
    for view in world.all() {
        hub.record(StatusEvent::FolderLifecycle(Box::new((*view).clone())));
    }
    let ctx = ctx(&world, 200);
    let entries = vec![
        entry("repo-a", EntryState::Running),
        entry("repo-b", EntryState::Running),
        entry("repo-c", EntryState::Running),
        entry("repo-d", EntryState::Starting),
        entry("repo-e", EntryState::Running),
    ];
    hub.record(StatusEvent::PeerConfigured {
        installation: PEER.into(),
        endpoint: "ws://peer.test".into(),
        display_name: None,
    });
    hub.record(StatusEvent::LaneState {
        installation: PEER.into(),
        lane: 0,
        role: LaneRole::Control,
        direction: Direction::Outbound,
        state: LaneState::Ready,
        error: None,
        backoff_until_ms: None,
        rtt_ms: Some(1),
        protocol: None,
    });
    hub.record(StatusEvent::PeerCatalog {
        installation: PEER.into(),
        entries: entries.clone(),
    });
    let peer = peer_ctx(&ctx, entries);
    let meta = MetaPeer::new();
    meta.always(
        "repo-b",
        Behave::Fail(ErrorWire::coded("REMOTE_WORKER_STARTING", "starting")),
    );
    let busy = Arc::new(AtomicUsize::new(0));
    {
        let busy = busy.clone();
        world.worker.script(move |op, payload| {
            if payload["route"]["repo"] == "repo-e" {
                // The folder view still says Running, but its worker is gone.
                return Some(Err(CallError::Unavailable(
                    "LOCAL_WORKER_UNAVAILABLE: worker gone".into(),
                )));
            }
            if op != "feed-offer" || payload["route"]["repo"] != "repo-a" {
                return None;
            }
            match busy.fetch_add(1, Ordering::SeqCst) {
                0..5 => Some(Err(CallError::Remote(ErrorWire::coded(
                    "WORKER_BUSY",
                    "engine busy",
                )))),
                5 => None,
                6 => Some(Err(CallError::Remote(ErrorWire::coded(
                    "INVALID_REPLY",
                    "garbled",
                )))),
                _ => None,
            }
        });
    }
    let mut options = control_options(200, Duration::from_secs(2));
    options.drive.waiting_recheck = Duration::from_millis(300);
    let lane = start_control(&ctx, &peer, &meta, options);
    sleep_ms(600);
    // Not ready: nothing runs, nothing counts.
    assert_eq!(world.worker.count("feed-offer", "repo-a"), 0);
    assert_eq!(world.worker.count("feed-offer", "repo-c"), 0);
    assert_eq!(world.worker.count("feed-offer", "repo-d"), 0);
    for (repo, readiness) in [
        ("repo-a", Readiness::WaitingForWorker),
        ("repo-c", Readiness::Paused),
        ("repo-d", Readiness::WaitingForRemote),
    ] {
        let key = lane.key(repo);
        assert_eq!(
            (key.readiness, key.phase, key.failures),
            (readiness, KeyPhase::NotReady, 0)
        );
    }
    // The remote answers "starting": rechecked, never counted.
    assert!(world.finished_of("repo-b").len() >= 2);
    let key_b = lane.key("repo-b");
    assert_eq!(key_b.failures, 0, "{key_b:?}");
    for fin in world.finished_of("repo-b") {
        assert_eq!(fin.error.as_ref().unwrap().retry_after_ms, Some(300));
    }

    // The local folder becomes Running: it exchanges at once.
    let running = Instant::now();
    world.edit(&state_of("repo-a"), |view| *view = folder("repo-a"));
    assert!(wait_until(Duration::from_secs(1), || world
        .worker
        .count("feed-offer", "repo-a")
        >= 1));
    let first = world.worker.calls("feed-offer", "repo-a")[0].at;
    assert!(
        first - running < Duration::from_millis(150),
        "{:?}",
        first - running
    );
    // Five Contention answers, a success, then one Transient failure.
    assert!(wait_until(Duration::from_secs(10), || busy
        .load(Ordering::SeqCst)
        >= 8));
    let fins = world.finished_of("repo-a");
    let codes: Vec<&str> = fins
        .iter()
        .map(|fin| fin.error.as_ref().map_or("ok", |e| e.code.as_str()))
        .collect();
    assert_eq!(
        &codes[..7],
        [
            "WORKER_BUSY",
            "WORKER_BUSY",
            "WORKER_BUSY",
            "WORKER_BUSY",
            "WORKER_BUSY",
            "ok",
            "INVALID_REPLY"
        ]
    );
    for fin in &fins[..5] {
        let retry = fin.error.as_ref().unwrap().retry_after_ms.unwrap();
        assert!(retry <= 1_200, "Contention retried after {retry} ms");
    }
    // The five Contention retries took about 50+100+200+400+800 ms, not a
    // Transient backoff.
    assert!(fins[5].at - fins[0].at < Duration::from_secs(3));
    let retry = fins[6].error.as_ref().unwrap().retry_after_ms.unwrap();
    assert!(
        (800..=1_200).contains(&retry),
        "first Transient retry after {retry} ms"
    );
    let offers = world.worker.calls("feed-offer", "repo-a");
    assert!(offers[7].at - offers[6].at >= Duration::from_millis(750));
    // Only the Transient failure counted.
    assert_eq!(lane.key("repo-b").failures, 0);

    // A local worker that cannot be reached (repo E): rechecked, never
    // counted, and never an error at the hub however often it is rechecked
    // (a Transient code would be shown from the 3rd consecutive failure on).
    assert!(wait_until(Duration::from_secs(3), || world
        .finished_of("repo-e")
        .len()
        >= 4));
    let key_e = lane.key("repo-e");
    assert_eq!(key_e.failures, 0, "{key_e:?}");
    assert_eq!(
        key_e.last_error.as_ref().unwrap().code,
        "LOCAL_WORKER_UNAVAILABLE"
    );
    for fin in world.finished_of("repo-e") {
        assert!(!fin.ok && fin.error.is_none(), "{fin:?}");
    }
    let snapshot = hub.snapshot_now();
    let pair = |repo: &str| {
        snapshot
            .folders
            .iter()
            .find(|folder| folder.repo == repo)
            .unwrap()
            .sync
            .peers
            .iter()
            .find(|pair| pair.installation == PEER)
            .cloned()
            .unwrap()
    };
    let e = pair("repo-e");
    assert!(e.error.is_none(), "{e:#?}");
    assert_ne!(e.state, FolderPeerState::Backoff, "{e:#?}");
    let b = pair("repo-b");
    assert_eq!(b.state, FolderPeerState::WaitingForRemote, "{b:#?}");

    // The remote entry becomes Running: it exchanges.
    set_entry(&peer, entry("repo-d", EntryState::Running));
    assert!(wait_until(Duration::from_secs(2), || world
        .worker
        .count("feed-apply", "repo-d")
        >= 1));
    lane.stop();
}

/// A peer's `retry_after_ms` lengthens a key's delay only up to
/// `DriveOptions::max_retry_after` (`MAX_RETRY_AFTER`, 60 s): an absurd value
/// on a Contention, Transient or Fatal answer cannot hold the key for an hour
/// (those holds ignore triggers). The reported `retry_after_ms` is the delay
/// actually used.
#[test]
fn a_peer_retry_after_is_bounded() {
    assert_eq!(scheduler::MAX_RETRY_AFTER, Duration::from_secs(60));
    assert_eq!(
        DriveOptions::new(4, Duration::from_secs(1)).max_retry_after,
        scheduler::MAX_RETRY_AFTER
    );
    let world = World::new();
    let repos = ["repo-a", "repo-b", "repo-c"];
    for repo in repos {
        world.put(folder(repo));
    }
    let ctx = ctx(&world, 60_000);
    let peer = peer_ctx(
        &ctx,
        repos
            .iter()
            .map(|repo| entry(repo, EntryState::Running))
            .collect(),
    );
    let meta = MetaPeer::new();
    meta.always(
        "repo-a",
        Behave::Fail(ErrorWire::coded("BUSY", "handler queue full").with_retry_after(3_600_000)),
    );
    meta.always(
        "repo-b",
        Behave::Fail(
            ErrorWire::coded("SQLITE_BUSY", "database is locked").with_retry_after(u64::MAX),
        ),
    );
    meta.always(
        "repo-c",
        Behave::Fail(
            ErrorWire::coded("UNBOUND_PEER_RESPONSE", "stale reply").with_retry_after(u64::MAX),
        ),
    );
    let mut options = control_options(60_000, Duration::from_secs(2));
    options.drive.max_retry_after = Duration::from_millis(300);
    options.drive.recheck_fatal = Duration::from_millis(500);
    let lane = start_control(&ctx, &peer, &meta, options);
    assert!(wait_until(Duration::from_secs(3), || {
        world.finished_of("repo-a").len() >= 4
            && world.finished_of("repo-b").len() >= 2
            && world.finished_of("repo-c").len() >= 3
    }));
    let retry = |fin: &Fin| fin.error.as_ref().unwrap().retry_after_ms.unwrap();
    // Contention: max(backoff 50 ms → 1 s, the 300 ms bound).
    let a = world.finished_of("repo-a");
    assert_eq!(
        a[..3].iter().map(retry).collect::<Vec<_>>(),
        [300, 300, 300],
        "{a:#?}"
    );
    assert!(a.iter().all(|fin| (300..=1_000).contains(&retry(fin))));
    // Transient: the first backoff (1 s ± 20 %).
    let b = world.finished_of("repo-b");
    assert!((800..=1_200).contains(&retry(&b[0])), "{b:#?}");
    let gap = b[1].at - b[0].at;
    assert!(
        gap >= Duration::from_millis(700) && gap < Duration::from_millis(1_600),
        "{gap:?}"
    );
    // Fatal: parked for its recheck (here 500 ms), not for the peer's value.
    let c = world.finished_of("repo-c");
    assert!(c.iter().all(|fin| retry(fin) == 500), "{c:#?}");
    assert_eq!(lane.key("repo-a").failures, 0);
    assert!(lane.key("repo-a").contention >= 3);
    assert!(lane.key("repo-b").failures >= 1);
    assert!(lane.key("repo-c").failures >= 2);
    lane.stop();
}

/// ≤ 4 exchanges in flight per peer, ≤ 1 per repository.
#[test]
fn at_most_four_exchanges_run_at_once_and_one_per_repo() {
    let world = World::new();
    let repos: Vec<String> = (0..8).map(|i| format!("repo-{i}")).collect();
    for repo in &repos {
        world.put(folder(repo));
    }
    let ctx = ctx(&world, 50);
    let peer = peer_ctx(
        &ctx,
        repos
            .iter()
            .map(|repo| entry(repo, EntryState::Running))
            .collect(),
    );
    let meta = MetaPeer::new();
    for repo in &repos {
        meta.always(repo, Behave::Delay(Duration::from_millis(250)));
    }
    let lane = start_control(
        &ctx,
        &peer,
        &meta,
        control_options(50, Duration::from_secs(5)),
    );
    sleep_ms(3_000);
    assert_eq!(
        meta.peak.load(Ordering::SeqCst),
        4,
        "peak exchanges at the peer"
    );
    assert_eq!(lane.view().peak_in_flight, 4);
    for (repo, (_, peak)) in meta.per_repo.lock().unwrap().iter() {
        assert_eq!(*peak, 1, "{repo} ran twice at once");
    }
    for repo in &repos {
        assert!(meta.seen(repo).len() >= 2, "{repo} starved");
    }
    lane.stop();
}

/// A heartbeat `generations.sync` change, a local tip change and a remote
/// tip change each trigger an exchange (the generation within 100 ms).
#[test]
fn a_heartbeat_generation_change_triggers_an_exchange_within_100ms() {
    let world = World::new();
    world.put(folder("repo-a"));
    let ctx = ctx(&world, 60_000);
    let peer = peer_ctx(&ctx, vec![entry("repo-a", EntryState::Running)]);
    let meta = MetaPeer::new();
    let mut options = control_options(60_000, Duration::from_secs(5));
    options.drive.absorb = Duration::from_millis(50);
    let lane = start_control(&ctx, &peer, &meta, options);
    assert!(wait_until(Duration::from_secs(2), || world
        .worker
        .count("feed-apply", "repo-a")
        == 1));
    let state = state_of("repo-a");
    let mut latencies = vec![];
    for round in 0..5 {
        sleep_ms(200);
        let before = world.worker.count("feed-offer", "repo-a");
        let bumped = Instant::now();
        world.bump_sync(&state);
        assert!(
            wait_until(Duration::from_secs(1), || world
                .worker
                .count("feed-offer", "repo-a")
                > before),
            "round {round}"
        );
        latencies.push(world.worker.calls("feed-offer", "repo-a")[before].at - bumped);
        assert!(wait_until(Duration::from_secs(1), || world
            .worker
            .count("feed-apply", "repo-a")
            == before + 1));
    }
    let worst = latencies.iter().max().unwrap();
    assert!(*worst < Duration::from_millis(100), "{latencies:?}");

    // The local feed tip moved.
    sleep_ms(200);
    let before = world.worker.count("feed-offer", "repo-a");
    world.bump_tip(&state);
    assert!(wait_until(Duration::from_millis(500), || world
        .worker
        .count("feed-offer", "repo-a")
        > before));
    // The remote tip moved (catalog changes are seen within 250 ms).
    sleep_ms(200);
    let before = world.worker.count("feed-offer", "repo-a");
    let mut moved = entry("repo-a", EntryState::Running);
    moved.tip = Some(FeedTip {
        epoch: REMOTE_EPOCH.into(),
        seq: 9,
    });
    set_entry(&peer, moved);
    assert!(wait_until(Duration::from_millis(800), || world
        .worker
        .count("feed-offer", "repo-a")
        > before));
    // Nothing else ran: 1 + 5 + 2 exchanges, the interval is 60 s.
    sleep_ms(300);
    assert_eq!(world.worker.count("feed-offer", "repo-a"), 8);
    lane.stop();
}

/// A generation bump right after our own exchange is its echo (absorbed); a
/// bump after the window triggers; a tip change always triggers.
#[test]
fn the_own_echo_of_an_exchange_is_absorbed() {
    let world = World::new();
    world.put(folder("repo-a"));
    let ctx = ctx(&world, 60_000);
    let peer = peer_ctx(&ctx, vec![entry("repo-a", EntryState::Running)]);
    let meta = MetaPeer::new();
    let mut options = control_options(60_000, Duration::from_secs(5));
    options.drive.absorb = Duration::from_secs(1);
    let lane = start_control(&ctx, &peer, &meta, options);
    let state = state_of("repo-a");
    assert!(wait_until(Duration::from_secs(2), || world
        .finished_of("repo-a")
        .len()
        == 1));
    let ended = world.finished_of("repo-a")[0].at;
    // Inside the window: absorbed.
    sleep_ms(100);
    world.bump_sync(&state);
    sleep_ms(500);
    assert_eq!(
        world.worker.count("feed-offer", "repo-a"),
        1,
        "the echo triggered an exchange"
    );
    // A tip change inside a window still triggers.
    world.bump_tip(&state);
    assert!(wait_until(Duration::from_millis(500), || world
        .finished_of("repo-a")
        .len()
        == 2));
    let ended2 = world.finished_of("repo-a")[1].at;
    assert!(ended2 - ended < Duration::from_secs(1));
    // After the window: a generation bump triggers.
    let quiet = ended2 + Duration::from_millis(1_300);
    thread::sleep(quiet.saturating_duration_since(Instant::now()));
    world.bump_sync(&state);
    assert!(wait_until(Duration::from_millis(300), || world
        .worker
        .count("feed-offer", "repo-a")
        == 3));
    lane.stop();
}

/// The trigger latency with the default `ABSORB` (3 s), pinned as the notes
/// state it: a `generations.sync` bump on a repository with no local exchange
/// in the last 3 s triggers within 100 ms; a bump inside the window (with the
/// default 500 ms interval every bump is: an anti-entropy exchange ended less
/// than 500 ms before) is taken for an echo, and the next anti-entropy
/// exchange carries it, within the interval.
#[test]
fn with_the_default_absorb_window_a_bump_triggers_only_on_a_quiet_repository() {
    assert_eq!(scheduler::ABSORB, Duration::from_secs(3));
    // Quiet: one exchange at the start, then nothing (interval 60 s).
    let quiet_world = World::new();
    quiet_world.put(folder("repo-a"));
    let quiet_ctx = ctx(&quiet_world, 60_000);
    let quiet_peer = peer_ctx(&quiet_ctx, vec![entry("repo-a", EntryState::Running)]);
    let quiet_meta = MetaPeer::new();
    let options = control_options(60_000, Duration::from_secs(5));
    assert_eq!(options.drive.absorb, scheduler::ABSORB);
    let quiet = start_control(&quiet_ctx, &quiet_peer, &quiet_meta, options);
    // Busy: the default configuration (`sync_interval_ms` 500).
    let busy_world = World::new();
    busy_world.put(folder("repo-a"));
    let busy_ctx = ctx(&busy_world, Network::default().sync_interval_ms);
    let busy_peer = peer_ctx(&busy_ctx, vec![entry("repo-a", EntryState::Running)]);
    let busy_meta = MetaPeer::new();
    let mut options = ControlOptions::for_config(&busy_ctx.config);
    assert_eq!(options.drive.interval, Duration::from_millis(500));
    assert_eq!(options.drive.absorb, scheduler::ABSORB);
    options.drive.view = Some(Arc::new(Snapshot::new(SchedulerView::default())));
    let busy = start_control(&busy_ctx, &busy_peer, &busy_meta, options);
    let state = state_of("repo-a");

    // Busy, three rounds: a bump right after an exchange ends waits for the
    // next anti-entropy exchange.
    for round in 0..3 {
        let done = busy_world.finished_of("repo-a").len();
        assert!(wait_until(Duration::from_secs(2), || busy_world
            .finished_of("repo-a")
            .len()
            > done));
        let ended = busy_world.finished_of("repo-a")[done].at;
        busy_world.bump_sync(&state);
        let bumped = Instant::now();
        assert!(wait_until(Duration::from_secs(2), || busy_world
            .worker
            .count("feed-offer", "repo-a")
            >= done + 2));
        let next = busy_world.worker.calls("feed-offer", "repo-a")[done + 1].at;
        assert!(
            next - bumped >= Duration::from_millis(300),
            "round {round}: the echo window let the bump trigger after {:?}",
            next - bumped
        );
        assert!(
            next - ended < Duration::from_millis(800),
            "round {round}: anti-entropy came {:?} after the exchange",
            next - ended
        );
    }

    // Quiet: more than ABSORB after its only exchange, a bump triggers at once.
    assert_eq!(quiet_world.finished_of("repo-a").len(), 1);
    let ended = quiet_world.finished_of("repo-a")[0].at;
    let calm = ended + scheduler::ABSORB + Duration::from_millis(200);
    thread::sleep(calm.saturating_duration_since(Instant::now()));
    assert_eq!(quiet_world.worker.count("feed-offer", "repo-a"), 1);
    let bumped = Instant::now();
    quiet_world.bump_sync(&state);
    assert!(wait_until(Duration::from_secs(1), || quiet_world
        .worker
        .count("feed-offer", "repo-a")
        == 2));
    let latency = quiet_world.worker.calls("feed-offer", "repo-a")[1].at - bumped;
    assert!(latency < Duration::from_millis(100), "{latency:?}");
    quiet.stop();
    busy.stop();
}

/// An idle key exchanges once per `sync_interval_ms`.
#[test]
fn idle_keys_follow_the_anti_entropy_interval() {
    let world = World::new();
    world.put(folder("repo-a"));
    let ctx = ctx(&world, 300);
    let peer = peer_ctx(&ctx, vec![entry("repo-a", EntryState::Running)]);
    let meta = MetaPeer::new();
    // The default options read the interval from the configuration.
    let mut options = ControlOptions::for_config(&ctx.config);
    assert_eq!(options.drive.interval, Duration::from_millis(300));
    assert_eq!(options.drive.slots, 4);
    assert_eq!(options.exchange, ExchangeOptions::default());
    assert_eq!(options.exchange.exchange, Duration::from_secs(30));
    options.drive.view = Some(Arc::new(Snapshot::new(SchedulerView::default())));
    let lane = start_control(&ctx, &peer, &meta, options);
    sleep_ms(2_200);
    let offers: Vec<Instant> = world
        .worker
        .calls("feed-offer", "repo-a")
        .iter()
        .map(|c| c.at)
        .collect();
    assert!(
        (5..=9).contains(&offers.len()),
        "{} exchanges in 2.2 s at 300 ms",
        offers.len()
    );
    for gap in gaps(&offers) {
        assert!(
            gap >= Duration::from_millis(280),
            "exchanged again after {gap:?}"
        );
    }
    lane.stop();
}

/// Catch-up pages run back to back; cursor recoveries re-run at most
/// `QUICK_LIMIT` times in a row.
#[test]
fn catch_up_runs_back_to_back_and_recoveries_are_bounded() {
    let world = World::new();
    world.put(folder("repo-a"));
    world.put(folder("repo-b"));
    *world.worker.apply_result.lock().unwrap() = FeedApplyResult {
        summary: ExchangeSummary {
            received_events: 100,
            metadata_advanced: true,
            to_receive: 400,
            ..ExchangeSummary::default()
        },
        gate: GateView::default(),
    };
    let ctx = ctx(&world, 60_000);
    let peer = peer_ctx(
        &ctx,
        vec![
            entry("repo-a", EntryState::Running),
            entry("repo-b", EntryState::Running),
        ],
    );
    let meta = MetaPeer::new();
    meta.plan(
        "repo-a",
        [
            Behave::More,
            Behave::More,
            Behave::More,
            Behave::More,
            Behave::More,
        ],
    );
    meta.always("repo-b", Behave::Mismatch);
    let lane = start_control(
        &ctx,
        &peer,
        &meta,
        control_options(60_000, Duration::from_secs(5)),
    );
    sleep_ms(1_500);
    // Five pages with `more`, then the last one: six exchanges, at once.
    let a = meta.seen("repo-a");
    assert_eq!(a.len(), 6, "{a:#?}");
    assert!(a[5].at - a[0].at < Duration::from_secs(1));
    // Mismatches: the first exchange and QUICK_LIMIT re-runs, then the interval.
    assert_eq!(
        meta.seen("repo-b").len(),
        1 + scheduler::QUICK_LIMIT as usize
    );
    let key = lane.key("repo-b");
    assert_eq!(key.phase, KeyPhase::Idle);
    lane.stop();
}

/// `ExchangeStarted`/`ExchangeFinished` carry the real duration, the wire
/// bytes and the summary; a gated folder receives `GateEvidence`.
#[test]
fn an_exchange_reports_its_duration_bytes_and_gate_evidence() {
    let world = World::new();
    let mut gated = folder("repo-a");
    gated.gate = GateState::Awaiting {
        source: PEER.into(),
        since_ms: 1,
        waiting_on: vec![],
    };
    world.put(gated);
    world.put(folder("repo-b"));
    *world.worker.apply_result.lock().unwrap() = FeedApplyResult {
        summary: ExchangeSummary {
            caught_up: true,
            ..ExchangeSummary::default()
        },
        gate: GateView {
            caught_up: false,
            waiting_on: vec!["2 events quarantined".into()],
            blocked: true,
        },
    };
    let ctx = ctx(&world, 60_000);
    let peer = peer_ctx(
        &ctx,
        vec![
            entry("repo-a", EntryState::Running),
            entry("repo-b", EntryState::Running),
        ],
    );
    let meta = MetaPeer::new();
    meta.always("repo-a", Behave::Delay(Duration::from_millis(300)));
    let lane = start_control(
        &ctx,
        &peer,
        &meta,
        control_options(60_000, Duration::from_secs(5)),
    );
    assert!(wait_until(Duration::from_secs(3), || world
        .finished()
        .len()
        == 2));
    let events = world.events.lock().unwrap().clone();
    let started = events
        .iter()
        .position(
            |(_, e)| matches!(e, StatusEvent::ExchangeStarted { repo, .. } if repo == "repo-a"),
        )
        .unwrap();
    let ended = events
        .iter()
        .position(
            |(_, e)| matches!(e, StatusEvent::ExchangeFinished { repo, .. } if repo == "repo-a"),
        )
        .unwrap();
    assert!(started < ended);
    let StatusEvent::ExchangeStarted {
        lane: started_lane,
        op,
        installation,
        ..
    } = &events[started].1
    else {
        unreachable!()
    };
    assert_eq!(
        (started_lane, op.as_str(), installation.as_str()),
        (&0, "meta.exchange", PEER)
    );
    let fin = &world.finished_of("repo-a")[0];
    assert!(fin.ok);
    assert_eq!(
        (fin.lane, fin.op.as_str(), fin.direction),
        (0, "meta.exchange", Direction::Outbound)
    );
    assert!(fin.duration_ms >= 300 && fin.duration_ms < 2_000, "{fin:?}");
    let seen = &meta.seen("repo-a")[0];
    assert_eq!(fin.bytes_up, seen.body_len as u64);
    assert_eq!(
        fin.bytes_down,
        meta.reply_lens.lock().unwrap()[&seen.request] as u64
    );
    assert_eq!((fin.objects_up, fin.objects_down), (0, 0));
    let summary = fin.summary.as_ref().unwrap();
    assert_eq!(
        (summary.bytes_up, summary.bytes_down),
        (fin.bytes_up, fin.bytes_down)
    );
    assert!(world.finished_of("repo-b")[0].duration_ms < 300);
    // Only the gated folder hears about it, as "caught up, but blocked".
    let evidence = world.evidence();
    assert_eq!(evidence.len(), 1, "{evidence:?}");
    assert_eq!(evidence[0].0, state_of("repo-a"));
    let evidence = &evidence[0].1;
    assert_eq!(evidence.installation, PEER);
    assert!(evidence.caught_up && evidence.blocked);
    assert_eq!(evidence.waiting_on, ["2 events quarantined"]);
    lane.stop();

    // The plain `exchange` entry point, on a fresh session.
    let meta = MetaPeer::new();
    let (client, server) = session(
        &ctx.installation,
        PEER,
        LaneRole::Control,
        None,
        meta.handler(),
    );
    let view = world.view(&state_of("repo-b")).unwrap();
    let summary = meta_exchange::exchange(
        &ctx,
        &client,
        &peer,
        &view,
        &entry("repo-b", EntryState::Running),
    )
    .unwrap();
    assert!(summary.caught_up);
    assert_eq!(meta.seen("repo-b").len(), 1);
    client.close();
    server.close();
}

/// Cancelling a lane with an exchange still in flight returns within its
/// exit wait; a respawned lane does not start that repository again until the
/// old exchange has ended (one per key across instances), while the other
/// repositories run at once.
#[test]
fn a_cancelled_lane_stops_promptly_and_its_successor_waits_for_the_old_exchange() {
    let world = World::new();
    world.put(folder("repo-a"));
    world.put(folder("repo-b"));
    let ctx = ctx(&world, 100);
    let peer = peer_ctx(
        &ctx,
        vec![
            entry("repo-a", EntryState::Running),
            entry("repo-b", EntryState::Running),
        ],
    );
    let meta = MetaPeer::new();
    meta.plan("repo-a", [Behave::Hang]);
    let mut options = control_options(100, Duration::from_secs(30));
    options.drive.exit_wait = Duration::from_millis(500);
    let mut lane = start_control(&ctx, &peer, &meta, options);
    assert!(wait_until(Duration::from_secs(2), || meta
        .seen("repo-a")
        .len()
        == 1));
    let took = lane.cancel_and_join(Duration::from_secs(5));
    assert!(
        took < Duration::from_millis(1_500),
        "the lane took {took:?} to stop"
    );
    assert!(lane.view().stopped);

    // The successor, on a new session, while the old exchange still hangs.
    let meta2 = MetaPeer::new();
    let lane2 = start_control(
        &ctx,
        &peer,
        &meta2,
        control_options(100, Duration::from_secs(5)),
    );
    sleep_ms(800);
    assert!(meta2.seen("repo-b").len() >= 2);
    assert!(meta2.seen("repo-a").is_empty(), "repo A ran twice at once");
    // The old session goes away: its exchange ends, the successor takes over.
    lane.client.close();
    lane.server.close();
    assert!(wait_until(Duration::from_secs(2), || !meta2
        .seen("repo-a")
        .is_empty()));
    lane2.stop();
}

/// While the daemon stops (`ctx.stopping`), nothing new is dispatched, and
/// the loop sleeps instead of spinning on its due keys; cancellation still
/// ends it at once.
#[test]
fn a_stopping_daemon_dispatches_nothing_and_the_lane_does_not_spin() {
    let world = World::new();
    world.put(folder("repo-a"));
    world.put(folder("repo-b"));
    let ctx = ctx(&world, 100);
    ctx.stopping.store(true, Ordering::SeqCst);
    let peer = peer_ctx(
        &ctx,
        vec![
            entry("repo-a", EntryState::Running),
            entry("repo-b", EntryState::Running),
        ],
    );
    let meta = MetaPeer::new();
    let mut lane = start_control(
        &ctx,
        &peer,
        &meta,
        control_options(100, Duration::from_secs(5)),
    );
    sleep_ms(1_200);
    assert!(world.worker.calls.lock().unwrap().is_empty());
    assert!(meta.seen("repo-a").is_empty() && meta.seen("repo-b").is_empty());
    assert_eq!(lane.key("repo-a").phase, KeyPhase::Due);
    // Each turn reads both folders once. A turn per second (and per change)
    // is a few lookups; a spinning loop makes hundreds of thousands.
    let lookups = world.lookups.load(Ordering::SeqCst);
    assert!(lookups <= 20, "{lookups} folder lookups in 1.2 s");
    let took = lane.cancel_and_join(Duration::from_secs(3));
    assert!(
        took < Duration::from_secs(1),
        "the lane took {took:?} to stop"
    );
    lane.client.close();
    lane.server.close();
}

/// CCR-WP2.7-4: with the default exit wait, a cancelled lane returns within
/// WP2.6's 1 s runner join even while a worker call (which cannot be
/// interrupted) is in flight. That exchange then ends on its own.
#[test]
fn a_cancelled_lane_returns_within_the_supervisor_join_budget() {
    assert!(scheduler::EXIT_WAIT <= Duration::from_millis(500));
    let world = World::new();
    world.put(folder("repo-a"));
    world.worker.script(|op, payload| {
        if op == "feed-offer" && payload["route"]["repo"] == "repo-a" {
            sleep_ms(2_000);
        }
        None
    });
    let ctx = ctx(&world, 60_000);
    let peer = peer_ctx(&ctx, vec![entry("repo-a", EntryState::Running)]);
    let meta = MetaPeer::new();
    let options = control_options(60_000, Duration::from_secs(5));
    assert_eq!(options.drive.exit_wait, scheduler::EXIT_WAIT);
    let mut lane = start_control(&ctx, &peer, &meta, options);
    assert!(wait_until(Duration::from_secs(2), || world
        .worker
        .count("feed-offer", "repo-a")
        == 1));
    let took = lane.cancel_and_join(Duration::from_secs(5));
    assert!(
        took < Duration::from_millis(900),
        "the lane took {took:?} to stop"
    );
    assert!(lane.view().stopped);
    // The supervisor closes the session; the detached exchange ends on its
    // own once its worker call returns, without reaching the peer.
    lane.client.close();
    lane.server.close();
    assert!(wait_until(Duration::from_secs(4), || world
        .finished_of("repo-a")
        .len()
        == 1));
    assert!(!world.finished_of("repo-a")[0].ok);
    assert!(meta.seen("repo-a").is_empty());
}

/// A lane checks its session: the authenticated peer, the role, the bulk lane.
#[test]
fn lanes_refuse_a_session_of_the_wrong_role_or_peer() {
    let world = World::new();
    world.put(folder("repo-a"));
    let ctx = ctx(&world, 100);
    let peer = peer_ctx(&ctx, vec![entry("repo-a", EntryState::Running)]);
    let meta = MetaPeer::new();
    let lane2of2 = LaneSpec::new(1, 2);
    type Run = Box<dyn Fn(Conn, Cancel) + Send + Sync>;
    let cases: Vec<(&str, Run, LaneRole, Option<LaneSpec>, &str)> = vec![
        (
            "control lane on a bulk session",
            Box::new({
                let (ctx, peer) = (ctx.clone(), peer.clone());
                move |conn, cancel| scheduler::run_control(ctx.clone(), peer.clone(), conn, cancel)
            }),
            LaneRole::Bulk,
            Some(lane2of2),
            "METADATA_ON_BULK_LANE",
        ),
        (
            "bulk lane on a control session",
            Box::new({
                let (ctx, peer) = (ctx.clone(), peer.clone());
                move |conn, cancel| {
                    objects::run_bulk_lane(ctx.clone(), peer.clone(), lane2of2, conn, cancel)
                }
            }),
            LaneRole::Control,
            None,
            "BULK_ON_CONTROL_LANE",
        ),
        (
            "bulk lane on another lane's session",
            Box::new({
                let (ctx, peer) = (ctx.clone(), peer.clone());
                move |conn, cancel| {
                    objects::run_bulk_lane(ctx.clone(), peer.clone(), lane2of2, conn, cancel)
                }
            }),
            LaneRole::Bulk,
            Some(LaneSpec::new(0, 2)),
            "BULK_LANE_MISMATCH",
        ),
        (
            "invalid bulk lane",
            Box::new({
                let (ctx, peer) = (ctx.clone(), peer.clone());
                move |conn, cancel| {
                    objects::run_bulk_lane(
                        ctx.clone(),
                        peer.clone(),
                        LaneSpec::new(3, 2),
                        conn,
                        cancel,
                    )
                }
            }),
            LaneRole::Bulk,
            Some(LaneSpec::new(1, 2)),
            "INVALID_BULK_LANE",
        ),
    ];
    for (name, run, role, lane, code) in cases {
        let (client, server) = session(&ctx.installation, PEER, role, lane, meta.handler());
        let cancel = Cancel::new();
        let before = world.errors().len();
        thread::scope(|scope| {
            let handle = scope.spawn(|| run(client.clone(), cancel.clone()));
            sleep_ms(300);
            assert!(
                !handle.is_finished(),
                "{name}: the lane returned before it was cancelled"
            );
            cancel.cancel();
            assert!(
                wait_until(Duration::from_secs(1), || handle.is_finished()),
                "{name}"
            );
        });
        let errors = world.errors();
        assert_eq!(errors.len(), before + 1, "{name}: {errors:?}");
        assert_eq!(errors[before].1.code, code, "{name}");
        assert!(matches!(errors[before].0, Scope::Lane { .. }));
        client.close();
        server.close();
    }
    assert!(world.worker.calls.lock().unwrap().is_empty());

    // A session authenticated as another installation.
    let (client, server) = session(
        &ctx.installation,
        "someone-else",
        LaneRole::Control,
        None,
        meta.handler(),
    );
    let cancel = Cancel::new();
    let handle = {
        let (ctx, peer, client, cancel) =
            (ctx.clone(), peer.clone(), client.clone(), cancel.clone());
        thread::spawn(move || scheduler::run_control(ctx, peer, client, cancel))
    };
    sleep_ms(300);
    cancel.cancel();
    handle.join().unwrap();
    assert_eq!(
        world.errors().last().unwrap().1.code,
        "PEER_INSTALLATION_CHANGED"
    );
    assert!(world.worker.calls.lock().unwrap().is_empty());
    client.close();
    server.close();
}

/// The peer's `objects.exchange` handler: answers with the parts we want
/// (`H_DOWN`), lists what it holds, and wants `H_UP` until it has it.
#[derive(Default)]
struct BulkPeer {
    seen: Mutex<Vec<(Value, Page)>>,
    holds_up: Mutex<HashSet<String>>,
    /// The hex of `H_DOWN` (default `"0102"`).
    down_hex: Mutex<Option<String>>,
    /// Stream the reply as 64 KiB body chunks instead of one body.
    stream: AtomicBool,
    /// While set, requests wait (until cleared or cancelled) before replying.
    hold: AtomicBool,
    /// Requests waiting on `hold` now.
    holding: AtomicUsize,
}

impl Handler for BulkPeer {
    fn handle(&self, req: Incoming) {
        assert_eq!(req.op, ops::OBJECTS_EXCHANGE);
        let page: Page = serde_json::from_slice(&req.body).unwrap();
        self.seen
            .lock()
            .unwrap()
            .push((req.header.clone(), page.clone()));
        if self.hold.load(Ordering::SeqCst) {
            self.holding.fetch_add(1, Ordering::SeqCst);
            while self.hold.load(Ordering::SeqCst) && !req.responder.cancelled() {
                sleep_ms(5);
            }
            self.holding.fetch_sub(1, Ordering::SeqCst);
        }
        let mut holds = self.holds_up.lock().unwrap();
        if page.object_parts.contains_key(H_UP) {
            holds.insert(page.repo.clone());
        }
        let mut reply = Page {
            request: uuid(),
            reply_to: Some(page.request.clone()),
            ..self::page(&page.repo, &page.receiver, &page.sender, page.lane)
        };
        if holds.contains(&page.repo) {
            reply.known_objects.insert(H_UP.into());
        } else {
            reply.wanted_objects.insert(H_UP.into());
        }
        if page.wanted_objects.contains(H_DOWN) {
            let hex = self.down_hex.lock().unwrap().clone();
            reply
                .object_parts
                .insert(H_DOWN.into(), part(hex.as_deref().unwrap_or("0102")));
        }
        let body = serde_json::to_vec(&reply).unwrap();
        if !self.stream.load(Ordering::SeqCst) {
            req.responder.reply(json!({}), body);
            return;
        }
        let pieces: Vec<&[u8]> = body.chunks(64 * 1024).collect();
        let mut offset = 0;
        for (index, piece) in pieces.iter().enumerate() {
            let header = ChunkHeader {
                id: 0,
                hash: None,
                offset,
                total: body.len() as u64,
                last: index + 1 == pieces.len(),
            };
            req.responder.chunk(header, piece).unwrap();
            offset += piece.len() as u64;
        }
        req.responder.reply(json!({"streamed": true}), vec![]);
    }
}

/// §6.5: the interim bulk lane runs offer → `objects.exchange` → apply for
/// every shared repository, with the lane's `Lane{index, count}`, the peer's
/// inventory passed into the next offer, and completed objects reported.
#[test]
fn interim_bulk_pages_are_routed() {
    let world = World::new();
    world.put(folder("repo-a"));
    world.put(folder("repo-b"));
    let mut c = folder("repo-c");
    c.record.sync_enabled = false;
    world.put(c);
    let ctx = ctx(&world, 60_000);
    let peer = peer_ctx(
        &ctx,
        vec![
            entry("repo-a", EntryState::Running),
            entry("repo-b", EntryState::Running),
            entry("repo-c", EntryState::Running),
            entry("repo-x", EntryState::Running),
        ],
    );
    let lane = LaneSpec::new(1, 2);
    let bulk = Arc::new(BulkPeer::default());
    let (client, server) = session(
        &ctx.installation,
        PEER,
        LaneRole::Bulk,
        Some(lane),
        bulk.clone(),
    );
    let mut options = BulkOptions::for_config(&ctx.config);
    assert_eq!(options.drive.slots, 2);
    assert_eq!(options.exchange, Duration::from_secs(60));
    let view = Arc::new(Snapshot::new(SchedulerView::default()));
    options.drive.view = Some(view.clone());
    let cancel = Cancel::new();
    let handle = {
        let (ctx, peer, client, cancel) =
            (ctx.clone(), peer.clone(), client.clone(), cancel.clone());
        thread::spawn(move || objects::run_bulk_lane_with(ctx, peer, lane, client, cancel, options))
    };
    // Three exchanges per repository, back to back: download, upload, idle.
    assert!(wait_until(Duration::from_secs(5), || {
        world.finished_of("repo-a").len() == 3 && world.finished_of("repo-b").len() == 3
    }));
    sleep_ms(300);
    for repo in ["repo-a", "repo-b"] {
        let fins = world.finished_of(repo);
        assert_eq!(
            fins.len(),
            3,
            "{repo}: the third exchange moved nothing, so the key idles"
        );
        for fin in &fins {
            assert!(fin.ok, "{fin:?}");
            assert_eq!((fin.lane, fin.op.as_str()), (2, "objects.exchange"));
            assert!(fin.summary.is_none());
        }
        let moved: Vec<(u64, u64, u64, u64)> = fins
            .iter()
            .map(|fin| {
                (
                    fin.objects_down,
                    fin.bytes_down,
                    fin.objects_up,
                    fin.bytes_up,
                )
            })
            .collect();
        assert_eq!(moved, [(1, 2, 0, 0), (0, 0, 1, 2), (0, 0, 0, 0)], "{repo}");
        assert!(fins[1].at - fins[0].at < Duration::from_millis(500));

        // The worker saw the lane, the route and the peer's inventory.
        let offers = world.worker.calls("published-bulk-offer", repo);
        assert_eq!(offers.len(), 3);
        for offer in &offers {
            assert_eq!(offer.payload["lane"], json!({"index": 1, "count": 2}));
            assert_eq!(
                offer.payload["route"],
                json!({"peer": PEER, "repo": repo, "local_replica": device_of(repo), "remote_replica": replica_of(repo)})
            );
        }
        assert_eq!(offers[0].payload["inventory"], Value::Null);
        let inventory: Page =
            serde_json::from_value(offers[1].payload["inventory"].clone()).unwrap();
        assert!(inventory.object_parts.is_empty());
        assert!(inventory.wanted_objects.contains(H_UP));
        let applies = world.worker.calls("published-bulk-apply", repo);
        assert_eq!(applies.len(), 3);
        let seen: Vec<(Value, Page)> = bulk
            .seen
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, page)| page.repo == repo)
            .cloned()
            .collect();
        assert_eq!(seen.len(), 3);
        for ((header, page), apply) in seen.iter().zip(&applies) {
            assert_eq!(
                header,
                &json!({"repo": repo, "lane": {"index": 1, "count": 2}})
            );
            assert_eq!(page.sender, device_of(repo));
            assert_eq!(page.receiver, replica_of(repo));
            assert_eq!(apply.payload["request"], json!(page.request));
            assert_eq!(apply.payload["page"]["reply_to"], json!(page.request));
        }
    }
    // Paused and unshared repositories are not exchanged.
    assert_eq!(world.worker.count("published-bulk-offer", "repo-c"), 0);
    assert!(view.load().key("repo-x").is_none());
    assert_eq!(
        view.load().key("repo-c").unwrap().readiness,
        Readiness::Paused
    );
    cancel.cancel();
    handle.join().unwrap();
    client.close();
    server.close();
}

/// Bulk failures are per key too: one repository's failing worker never
/// stalls another's transfers on the same lane.
#[test]
fn a_failing_bulk_repository_never_stalls_the_others() {
    let world = World::new();
    world.put(folder("repo-a"));
    world.put(folder("repo-b"));
    world.worker.script(|op, payload| {
        (op == "published-bulk-offer" && payload["route"]["repo"] == "repo-a").then(|| {
            Err(CallError::Remote(ErrorWire::coded(
                "OBJECT_IO_ERROR",
                "disk",
            )))
        })
    });
    let ctx = ctx(&world, 60_000);
    let peer = peer_ctx(
        &ctx,
        vec![
            entry("repo-a", EntryState::Running),
            entry("repo-b", EntryState::Running),
        ],
    );
    let lane = LaneSpec::new(1, 2);
    let bulk = Arc::new(BulkPeer::default());
    let (client, server) = session(
        &ctx.installation,
        PEER,
        LaneRole::Bulk,
        Some(lane),
        bulk.clone(),
    );
    let mut options = BulkOptions::for_config(&ctx.config);
    let view = Arc::new(Snapshot::new(SchedulerView::default()));
    options.drive.view = Some(view.clone());
    let cancel = Cancel::new();
    let handle = {
        let (ctx, peer, client, cancel) =
            (ctx.clone(), peer.clone(), client.clone(), cancel.clone());
        thread::spawn(move || objects::run_bulk_lane_with(ctx, peer, lane, client, cancel, options))
    };
    assert!(wait_until(Duration::from_secs(5), || world
        .finished_of("repo-b")
        .len()
        == 3));
    let a = view.load().key("repo-a").cloned().unwrap();
    assert!(
        a.failures >= 1 && a.last_error.as_ref().unwrap().code == "OBJECT_IO_ERROR",
        "{a:?}"
    );
    assert_eq!(view.load().key("repo-b").unwrap().failures, 0);
    assert!(
        bulk.seen
            .lock()
            .unwrap()
            .iter()
            .all(|(_, page)| page.repo == "repo-b")
    );
    cancel.cancel();
    handle.join().unwrap();
    client.close();
    server.close();
}

/// A key reset while its exchange runs (the peer's replica changed) drops the
/// cached inventory: the running exchange does not put the old replica's
/// inventory back, so the next offer, to the new replica, is never refused
/// (`BULK_ROUTE_MISMATCH` is Fatal and would park the key for 10 min). A
/// repository removed and re-added while its exchange runs starts without an
/// inventory as well. The fake worker checks every inventory and reply
/// against the call's route, as `bulk_sync::worker_control` does.
#[test]
fn a_key_reset_during_a_bulk_exchange_drops_the_stale_inventory() {
    let world = World::new();
    world.put(folder("repo-a"));
    let ctx = ctx(&world, 300);
    let peer = peer_ctx(&ctx, vec![entry("repo-a", EntryState::Running)]);
    let lane = LaneSpec::new(1, 2);
    let bulk = Arc::new(BulkPeer::default());
    bulk.hold.store(true, Ordering::SeqCst);
    let (client, server) = session(
        &ctx.installation,
        PEER,
        LaneRole::Bulk,
        Some(lane),
        bulk.clone(),
    );
    let mut options = BulkOptions::for_config(&ctx.config);
    let view = Arc::new(Snapshot::new(SchedulerView::default()));
    options.drive.view = Some(view.clone());
    let cancel = Cancel::new();
    let handle = {
        let (ctx, peer, client, cancel) =
            (ctx.clone(), peer.clone(), client.clone(), cancel.clone());
        thread::spawn(move || objects::run_bulk_lane_with(ctx, peer, lane, client, cancel, options))
    };
    let offers = || world.worker.calls("published-bulk-offer", "repo-a");
    let key = || view.load().key("repo-a").cloned();

    // 1. The peer's replica changes while the first exchange (to the first
    //    replica) is in flight.
    assert!(wait_until(Duration::from_secs(2), || bulk
        .holding
        .load(Ordering::SeqCst)
        == 1));
    let replica2 = "remote-replica-2";
    let mut moved = entry("repo-a", EntryState::Running);
    moved.replica = replica2.into();
    set_entry(&peer, moved.clone());
    // The lane samples the catalog every 250 ms.
    sleep_ms(700);
    assert_eq!(key().unwrap().phase, KeyPhase::Running);
    bulk.hold.store(false, Ordering::SeqCst);
    // At once a fresh offer to the new replica, without the old replica's
    // inventory (it learns the new one), then after the interval an offer
    // with the new replica's inventory, which uploads `H_UP`, then idle.
    assert!(wait_until(Duration::from_secs(5), || world
        .finished_of("repo-a")
        .len()
        >= 4));
    let fins = world.finished_of("repo-a");
    assert!(fins.iter().all(|fin| fin.ok), "{fins:#?}");
    let moved_objects: Vec<(u64, u64)> = fins[..4]
        .iter()
        .map(|fin| (fin.objects_down, fin.objects_up))
        .collect();
    assert_eq!(moved_objects, [(1, 0), (0, 0), (0, 1), (0, 0)]);
    let sent = offers();
    assert_eq!(
        sent[0].payload["route"]["remote_replica"],
        json!(replica_of("repo-a"))
    );
    for offer in &sent[1..] {
        assert_eq!(offer.payload["route"]["remote_replica"], json!(replica2));
    }
    assert_eq!(
        sent[1].payload["inventory"],
        Value::Null,
        "the old replica's inventory was reused"
    );
    let inventory: Page = serde_json::from_value(sent[2].payload["inventory"].clone()).unwrap();
    assert_eq!(inventory.sender, replica2);
    let receivers: Vec<String> = bulk
        .seen
        .lock()
        .unwrap()
        .iter()
        .map(|(_, page)| page.receiver.clone())
        .collect();
    assert_eq!(receivers[0], replica_of("repo-a"));
    assert!(receivers[1..].iter().all(|receiver| receiver == replica2));
    assert_eq!(key().unwrap().failures, 0);

    // 2. The repository is removed and re-added (same replica) while an
    //    exchange runs: the re-added key starts without an inventory.
    bulk.hold.store(true, Ordering::SeqCst);
    assert!(wait_until(Duration::from_secs(2), || bulk
        .holding
        .load(Ordering::SeqCst)
        == 1));
    let held = offers().len();
    assert!(!offers()[held - 1].payload["inventory"].is_null());
    peer.catalog.update(|catalog| {
        let mut catalog = catalog.clone();
        catalog.generation += 1;
        catalog.entries.retain(|entry| entry.repo != "repo-a");
        catalog
    });
    assert!(wait_until(Duration::from_secs(2), || key().is_none()));
    set_entry(&peer, moved.clone());
    assert!(wait_until(Duration::from_secs(2), || key().is_some()));
    // The old exchange still holds the repository: no second one at once.
    sleep_ms(200);
    assert_eq!(offers().len(), held);
    bulk.hold.store(false, Ordering::SeqCst);
    assert!(wait_until(Duration::from_secs(3), || offers().len() > held));
    assert_eq!(
        offers()[held].payload["inventory"],
        Value::Null,
        "the removed key's exchange put its inventory back"
    );
    sleep_ms(200);
    let fins = world.finished_of("repo-a");
    assert!(fins.iter().all(|fin| fin.ok), "{fins:#?}");
    assert_eq!(key().unwrap().failures, 0);
    cancel.cancel();
    handle.join().unwrap();

    // 3. The exchange itself never hands the worker an inventory of another
    //    route or lane, whatever its caller passes.
    let route = SyncRoute {
        peer: PEER.into(),
        repo: "repo-a".into(),
        local_replica: device_of("repo-a"),
        remote_replica: replica2.into(),
    };
    let page_lane = objects::page_lane(lane);
    assert!(objects::fits(&inventory, &route, page_lane));
    let stale = Page {
        sender: replica_of("repo-a"),
        ..inventory.clone()
    };
    assert!(!objects::fits(&stale, &route, page_lane));
    let reversed = Page {
        receiver: replica2.into(),
        ..inventory.clone()
    };
    assert!(!objects::fits(&reversed, &route, page_lane));
    let other_repo = Page {
        repo: "repo-b".into(),
        ..inventory.clone()
    };
    assert!(!objects::fits(&other_repo, &route, page_lane));
    let other_lane = bulk_sync::Lane { index: 0, count: 2 };
    assert!(!objects::fits(&inventory, &route, other_lane));
    let before = offers().len();
    let view_a = world.view(&state_of("repo-a")).unwrap();
    objects::bulk_exchange(
        &ctx,
        &client,
        &peer,
        lane,
        &view_a,
        &moved,
        Some(&stale),
        &BulkOptions::for_config(&ctx.config),
        &mut |_| None,
    )
    .unwrap();
    assert_eq!(offers()[before].payload["inventory"], Value::Null);
    client.close();
    server.close();
}

/// The StatusHub reads the lane's reports as intended: a failing key is its
/// pair's Backoff (with its retry time), Contention stays hidden, and the
/// other repository of the same peer is up to date.
#[test]
fn the_status_hub_reads_the_lane_reports() {
    let world = World::new();
    let hub = StatusHub::new(HubConfig::default(), Arc::new(SystemClock));
    let _ = world.hub.set(hub.clone());
    let repos = ["repo-a", "repo-b", "repo-c"];
    for repo in repos {
        world.put(folder(repo));
        hub.record(StatusEvent::FolderLifecycle(Box::new(folder(repo))));
    }
    let entries: Vec<PublishedEntry> = repos
        .iter()
        .map(|repo| entry(repo, EntryState::Running))
        .collect();
    hub.record(StatusEvent::PeerConfigured {
        installation: PEER.into(),
        endpoint: "ws://peer.test".into(),
        display_name: None,
    });
    hub.record(StatusEvent::LaneState {
        installation: PEER.into(),
        lane: 0,
        role: LaneRole::Control,
        direction: Direction::Outbound,
        state: LaneState::Ready,
        error: None,
        backoff_until_ms: None,
        rtt_ms: Some(1),
        protocol: None,
    });
    hub.record(StatusEvent::PeerCatalog {
        installation: PEER.into(),
        entries: entries.clone(),
    });
    let ctx = ctx(&world, 100);
    let peer = peer_ctx(&ctx, entries);
    let meta = MetaPeer::new();
    meta.always(
        "repo-a",
        Behave::Fail(ErrorWire::coded("SQLITE_BUSY", "database is locked")),
    );
    meta.always(
        "repo-c",
        Behave::Fail(ErrorWire::coded("BUSY", "handler queue full")),
    );
    let lane = start_control(
        &ctx,
        &peer,
        &meta,
        control_options(100, Duration::from_secs(2)),
    );
    // Three Transient failures of repo A: 0 s, ~1 s, ~3 s.
    assert!(wait_until(Duration::from_secs(8), || world
        .finished_of("repo-a")
        .len()
        >= 3));
    let snapshot = hub.snapshot_now();
    let pair = |repo: &str| {
        snapshot
            .folders
            .iter()
            .find(|folder| folder.repo == repo)
            .unwrap()
            .sync
            .peers
            .iter()
            .find(|pair| pair.installation == PEER)
            .cloned()
            .unwrap()
    };
    let a = pair("repo-a");
    assert_eq!(a.state, FolderPeerState::Backoff, "{a:#?}");
    assert_eq!(a.error.as_ref().unwrap().error.code, "SQLITE_BUSY");
    assert!(a.backoff_until_ms.is_some(), "{a:#?}");
    let b = pair("repo-b");
    assert_eq!(b.state, FolderPeerState::UpToDate, "{b:#?}");
    assert!(b.metadata.caught_up && b.metadata.last_exchange_ms.is_some());
    assert!(b.error.is_none());
    // Contention is retried quietly (hidden for 30 s).
    let c = pair("repo-c");
    assert!(c.error.is_none(), "{c:#?}");
    assert!(world.finished_of("repo-c").len() >= 3);
    let b_folder = snapshot
        .folders
        .iter()
        .find(|folder| folder.repo == "repo-b")
        .unwrap();
    assert_eq!(b_folder.sync.state, FolderSyncState::UpToDate);
    // The peer and its control lane are online, with nothing stuck in flight.
    let peer_status = snapshot
        .peers
        .iter()
        .find(|p| p.installation == PEER)
        .unwrap();
    assert!(
        peer_status.lanes[0]
            .inflight
            .iter()
            .all(|op| op.repo != "repo-b")
    );
    lane.stop();
}

/// A reply streamed as body chunks has no body limit (a 1.5 MiB object);
/// a request page over the op's 1 MiB body limit fails only its own key
/// (`WS_MESSAGE_TOO_LARGE`) until CCR-WP2.7-1.
#[test]
fn large_bulk_replies_stream_and_oversize_pages_fail_only_their_key() {
    let world = World::new();
    world.put(folder("repo-a"));
    world.put(folder("repo-big"));
    let big_hex = "ab".repeat(600 * 1024);
    {
        let big_hex = big_hex.clone();
        world.worker.script(move |op, payload| {
            if op != "published-bulk-offer" || payload["route"]["repo"] != "repo-big" {
                return None;
            }
            let route: SyncRoute = serde_json::from_value(payload["route"].clone()).unwrap();
            let lane: bulk_sync::Lane = serde_json::from_value(payload["lane"].clone()).unwrap();
            let mut page = page(
                &route.repo,
                &route.local_replica,
                &route.remote_replica,
                lane,
            );
            page.object_parts.insert(H_UP.into(), part(&big_hex));
            Some(Ok(json!(page)))
        });
    }
    let ctx = ctx(&world, 60_000);
    let peer = peer_ctx(
        &ctx,
        vec![
            entry("repo-a", EntryState::Running),
            entry("repo-big", EntryState::Running),
        ],
    );
    let lane = LaneSpec::new(1, 2);
    let bulk = Arc::new(BulkPeer::default());
    bulk.stream.store(true, Ordering::SeqCst);
    *bulk.down_hex.lock().unwrap() = Some("cd".repeat(1536 * 1024));
    let (client, server) = session(
        &ctx.installation,
        PEER,
        LaneRole::Bulk,
        Some(lane),
        bulk.clone(),
    );
    let mut options = BulkOptions::for_config(&ctx.config);
    let view = Arc::new(Snapshot::new(SchedulerView::default()));
    options.drive.view = Some(view.clone());
    let cancel = Cancel::new();
    let handle = {
        let (ctx, peer, client, cancel) =
            (ctx.clone(), peer.clone(), client.clone(), cancel.clone());
        thread::spawn(move || objects::run_bulk_lane_with(ctx, peer, lane, client, cancel, options))
    };
    assert!(wait_until(Duration::from_secs(10), || world
        .finished_of("repo-a")
        .len()
        == 3));
    let fins = world.finished_of("repo-a");
    assert!(fins.iter().all(|fin| fin.ok), "{fins:?}");
    assert_eq!((fins[0].objects_down, fins[0].bytes_down), (1, 1536 * 1024));
    let apply = &world.worker.calls("published-bulk-apply", "repo-a")[0];
    assert_eq!(
        apply.payload["page"]["object_parts"][H_DOWN]["total"],
        json!(1536 * 1024)
    );
    let big = world.finished_of("repo-big");
    assert!(!big.is_empty());
    assert!(
        big.iter()
            .all(|fin| !fin.ok && fin.error.as_ref().unwrap().code == "WS_MESSAGE_TOO_LARGE"),
        "{big:?}"
    );
    assert!(view.load().key("repo-big").unwrap().failures >= 1);
    assert_eq!(view.load().key("repo-a").unwrap().failures, 0);
    // Nothing of the oversize page reached the peer, and the session is fine.
    assert!(
        bulk.seen
            .lock()
            .unwrap()
            .iter()
            .all(|(_, page)| page.repo == "repo-a")
    );
    assert!(client.closed().is_none());
    cancel.cancel();
    handle.join().unwrap();
    client.close();
    server.close();
}

/// §8 classes, plus the answers that mean "not serving".
#[test]
fn failure_kinds_follow_the_error_classes() {
    let kind = |code: &str| scheduler::failure_kind(&ErrorWire::coded(code, "x"));
    assert_eq!(kind("WORKER_BUSY"), FailureKind::Contention);
    assert_eq!(kind("BUSY"), FailureKind::Contention);
    assert_eq!(kind("REQUEST_TIMEOUT"), FailureKind::Transient);
    assert_eq!(kind("CONNECTION_LOST"), FailureKind::Transient);
    assert_eq!(kind("SOMETHING_UNKNOWN"), FailureKind::Transient);
    assert_eq!(kind("REMOTE_WORKER_STARTING"), FailureKind::Waiting);
    assert_eq!(kind("FOLDER_SYNC_PAUSED"), FailureKind::Waiting);
    assert_eq!(kind("LOCAL_WORKER_UNAVAILABLE"), FailureKind::Waiting);
    assert_eq!(kind("INVALID_PAIRING_CODE"), FailureKind::NeedsUserAction);
    assert_eq!(kind("PEER_NEEDS_UPGRADE"), FailureKind::Incompatible);
    assert_eq!(kind("UNBOUND_PEER_RESPONSE"), FailureKind::Fatal);
    for (code, counted) in [
        ("BUSY", false),
        ("REMOTE_WORKER_STARTING", false),
        ("REQUEST_TIMEOUT", true),
        ("UNBOUND_PEER_RESPONSE", true),
    ] {
        assert_eq!(kind(code).counted(), counted, "{code}");
    }
    assert_eq!(
        scheduler::failure_kind(&ErrorWire::coded("BUSY", "x")).counted(),
        ErrorClass::Contention.counts_as_failure()
    );
    // Readiness of a (folder, entry) pair.
    let mut view = folder("repo-a");
    assert_eq!(
        scheduler::readiness(&view, &entry("repo-a", EntryState::Running)),
        Readiness::Ready
    );
    assert_eq!(
        scheduler::readiness(&view, &entry("repo-a", EntryState::Paused)),
        Readiness::WaitingForRemote
    );
    assert_eq!(
        scheduler::readiness(&view, &entry("repo-a", EntryState::Unavailable)),
        Readiness::WaitingForRemote
    );
    starting(&mut view);
    assert_eq!(
        scheduler::readiness(&view, &entry("repo-a", EntryState::Running)),
        Readiness::WaitingForWorker
    );
    view.record.sync_enabled = false;
    assert_eq!(
        scheduler::readiness(&view, &entry("repo-a", EntryState::Running)),
        Readiness::Paused
    );
}

/// §14 WP2.7: the anti-entropy interval holds at `sync_interval_ms = 30000`
/// with no reconnect over 90 s (heartbeats keep the idle session alive).
/// Release, ignored: `cargo test --release --test meta_scheduler -- --ignored`.
#[test]
#[ignore = "WP2.7 release scenario (95 s)"]
fn anti_entropy_at_30_s_keeps_one_session_for_90_s() {
    let world = World::new();
    world.put(folder("repo-a"));
    let ctx = ctx(&world, 30_000);
    let peer = peer_ctx(&ctx, vec![entry("repo-a", EntryState::Running)]);
    let meta = MetaPeer::new();
    let mut options = ControlOptions::for_config(&ctx.config);
    options.drive.view = Some(Arc::new(Snapshot::new(SchedulerView::default())));
    let lane = start_control(&ctx, &peer, &meta, options);
    let since = lane.client.health().since_ms;
    sleep_ms(95_000);
    let offers: Vec<Instant> = world
        .worker
        .calls("feed-offer", "repo-a")
        .iter()
        .map(|c| c.at)
        .collect();
    println!(
        "QUAL {}",
        json!({"scenario": "wp2_7_anti_entropy_30s", "exchanges": offers.len(),
        "gaps_ms": gaps(&offers).iter().map(|g| g.as_millis() as u64).collect::<Vec<_>>()})
    );
    assert_eq!(offers.len(), 4, "exchanges at 0, 30, 60 and 90 s");
    for gap in gaps(&offers) {
        assert!(
            gap >= Duration::from_millis(29_900) && gap < Duration::from_millis(31_000),
            "{gap:?}"
        );
    }
    assert!(lane.client.closed().is_none(), "the session closed");
    assert_eq!(lane.client.health().since_ms, since);
    assert!(!lane.view().stopped);
    assert!(world.finished().iter().all(|fin| fin.ok));
    lane.stop();
}
