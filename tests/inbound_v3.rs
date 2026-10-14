//! WP2.8: the inbound v3 server (REVAMP_PLAN §3.8), built standalone.
//!
//! Each test runs a real `inbound::start_with` on a loopback listener, with a
//! fake catalog (settable verifier, generation, folders, revocation), fake
//! folder controllers and scripted worker clients, a real
//! `PublishedCatalog` and a real `StatusHub` (behind a recorder that keeps
//! every event). Dialers are real v3 sessions (`session::dial`).
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc, Condvar, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicIsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tkfs::model::orchestrator::StatusSink;
use tkfs::{
    bulk_sync::{Lane, Page},
    core::transfer::ObjectPart,
    model::{
        errors::{ErrorClass, ErrorWire},
        ipc::{FeedExchangeReply, FeedExchangeRequest},
        orchestrator::{
            CallError, CatalogApi, CatalogCmd, CatalogSnapshot, Ctx, DaemonEndpoints, EntryState,
            FolderCmd, FolderDirectory, FolderPhase, FolderRecord, FolderView, GateEvidence,
            PeerMeta, PublishedCatalogApi, PublishedEntry, ShutdownOutcome, WorkerClient,
        },
        protocol::{
            CatalogListReply, LEGACY_NEEDS_UPGRADE_REPLY, LaneRole, LaneSpec,
            feed::{
                Cursor, ExchangeSummary, FeedApplyResult, FeedPage, FeedState, GateView,
                MetaExchangeReply, MetaExchangeRequest, PageStatus,
            },
        },
        status::{Direction, LaneState, StatusEvent, StatusSnapshot},
    },
    orchestrator::{
        Config, Network,
        hub::{HubConfig, StatusHub},
        inbound::{self, InboundOptions, InboundServer},
        published::PublishedCatalog,
    },
    pairing_code::Verifier,
    session::{self, CodeAuth, Conn, ConnOptions, DialError, Priority, Reply},
    util::{cancel::Cancel, snapshot::Snapshot, time::SystemClock},
    ws,
};

const SERVER: &str = "server-installation";
const CLIENT: &str = "client-installation";
const OTHER_CLIENT: &str = "other-client-installation";
const GENERATION: u64 = 5;

const STATE_A: &str = "state-a";
const REPO_A: &str = "repo-a";
const DEVICE_A: &str = "replica-a";
const STATE_B: &str = "state-b";
const REPO_B: &str = "repo-b";
const DEVICE_B: &str = "replica-b";
/// The dialer's replica of the shared repositories.
const REMOTE_REPLICA: &str = "replica-remote";

// ---------------------------------------------------------------------------
// Fakes

/// A catalog whose snapshot tests edit directly; every edit bumps the
/// generation and wakes `wait_newer`.
struct FakeCatalog {
    snapshot: Snapshot<CatalogSnapshot>,
}

impl FakeCatalog {
    fn new(verifier: Verifier) -> Arc<FakeCatalog> {
        Arc::new(FakeCatalog {
            snapshot: Snapshot::new(CatalogSnapshot {
                generation: 1,
                installation: SERVER.into(),
                verifier: Some(verifier),
                verifier_generation: GENERATION,
                ..CatalogSnapshot::default()
            }),
        })
    }

    fn edit(&self, edit: impl FnOnce(&mut CatalogSnapshot)) {
        self.snapshot.update(|old| {
            let mut next = old.clone();
            edit(&mut next);
            next.generation = old.generation + 1;
            next
        });
    }

    /// A new pairing code (`SetVerifier`): the generation moves on.
    fn rotate(&self) -> String {
        let (code, verifier) = Verifier::generate();
        self.apply(CatalogCmd::SetVerifier { verifier }).unwrap();
        code
    }

    fn set_folder(&self, record: FolderRecord) {
        self.edit(|snapshot| {
            snapshot
                .folders
                .retain(|folder| folder.state != record.state);
            snapshot.folders.push(record);
        });
    }
}

impl CatalogApi for FakeCatalog {
    fn snapshot(&self) -> Arc<CatalogSnapshot> {
        self.snapshot.load()
    }

    fn apply(&self, cmd: CatalogCmd) -> Result<Arc<CatalogSnapshot>, ErrorWire> {
        match cmd {
            CatalogCmd::SetVerifier { verifier } => self.edit(|snapshot| {
                snapshot.verifier = Some(verifier);
                snapshot.verifier_generation += 1;
            }),
            CatalogCmd::SetIntents {
                state,
                sync_enabled,
                ..
            } => self.edit(|snapshot| {
                for folder in &mut snapshot.folders {
                    if folder.state == state
                        && let Some(enabled) = sync_enabled
                    {
                        folder.sync_enabled = enabled;
                    }
                }
            }),
            other => panic!("unexpected catalog command {other:?}"),
        }
        Ok(self.snapshot())
    }

    fn wait_newer(&self, generation: u64, timeout: Duration) -> Arc<CatalogSnapshot> {
        let (snapshot, seen) = self.snapshot.load_versioned();
        if snapshot.generation > generation {
            return snapshot;
        }
        self.snapshot.wait_newer(seen, timeout);
        self.snapshot.load()
    }
}

type Script = dyn Fn(&str, &Value) -> Result<Value, CallError> + Send + Sync;

/// A worker client answering from a script and recording every call.
struct FakeWorker {
    script: Box<Script>,
    calls: Mutex<Vec<(String, Value)>>,
}

impl FakeWorker {
    fn new(
        script: impl Fn(&str, &Value) -> Result<Value, CallError> + Send + Sync + 'static,
    ) -> Arc<FakeWorker> {
        Arc::new(FakeWorker {
            script: Box::new(script),
            calls: Mutex::new(Vec::new()),
        })
    }

    fn calls(&self) -> Vec<(String, Value)> {
        self.calls.lock().unwrap().clone()
    }
}

impl WorkerClient for FakeWorker {
    fn instance(&self) -> &str {
        "fake-worker"
    }

    fn call(&self, op: &str, payload: Value, _deadline: Duration) -> Result<Value, CallError> {
        self.calls
            .lock()
            .unwrap()
            .push((op.to_owned(), payload.clone()));
        (self.script)(op, &payload)
    }

    fn call_bytes(
        &self,
        op: &str,
        _header: Value,
        _body: &[u8],
        _deadline: Duration,
    ) -> Result<(Value, Vec<u8>), CallError> {
        Err(CallError::Unavailable(format!("no binary op {op} here")))
    }
}

/// Folder controllers as far as the inbound server sees them.
#[derive(Default)]
struct FakeFolders {
    views: Mutex<BTreeMap<String, Arc<FolderView>>>,
    clients: Mutex<BTreeMap<String, Arc<dyn WorkerClient>>>,
    sent: Mutex<Vec<(String, FolderCmd)>>,
}

impl FakeFolders {
    fn set(&self, view: FolderView, client: Option<Arc<dyn WorkerClient>>) {
        let state = view.record.state.clone();
        self.views
            .lock()
            .unwrap()
            .insert(state.clone(), Arc::new(view));
        match client {
            Some(client) => {
                self.clients.lock().unwrap().insert(state, client);
            }
            None => {
                self.clients.lock().unwrap().remove(&state);
            }
        }
    }

    fn evidence(&self) -> Vec<(String, GateEvidence)> {
        self.sent
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

impl FolderDirectory for FakeFolders {
    fn view(&self, state: &str) -> Option<Arc<FolderView>> {
        self.views.lock().unwrap().get(state).cloned()
    }

    fn by_repo(&self, repo: &str) -> Option<Arc<FolderView>> {
        self.views
            .lock()
            .unwrap()
            .values()
            .find(|view| view.record.repo == repo)
            .cloned()
    }

    fn all(&self) -> Vec<Arc<FolderView>> {
        self.views.lock().unwrap().values().cloned().collect()
    }

    fn send(&self, state: &str, cmd: FolderCmd) -> Result<(), ErrorWire> {
        self.sent.lock().unwrap().push((state.to_owned(), cmd));
        Ok(())
    }

    fn request(
        &self,
        _state: &str,
        _cmd: FolderCmd,
        _wait: Duration,
    ) -> Result<Arc<FolderView>, ErrorWire> {
        Err(ErrorWire::coded("NOT_IMPLEMENTED", "fake"))
    }

    fn client(&self, state: &str) -> Option<Arc<dyn WorkerClient>> {
        self.clients.lock().unwrap().get(state).cloned()
    }

    fn generation(&self) -> u64 {
        0
    }

    fn wait_newer(&self, generation: u64, timeout: Duration) -> u64 {
        thread::sleep(timeout.min(Duration::from_millis(10)));
        generation
    }

    fn shutdown(&self, _deadline: Duration) -> Vec<ShutdownOutcome> {
        Vec::new()
    }
}

/// Keeps every status event and forwards it to a real hub.
struct Recorder {
    hub: Arc<StatusHub>,
    events: Mutex<Vec<StatusEvent>>,
}

impl StatusSink for Recorder {
    fn record(&self, event: StatusEvent) {
        self.events.lock().unwrap().push(event.clone());
        self.hub.record(event);
    }
}

impl Recorder {
    fn events(&self) -> Vec<StatusEvent> {
        self.events.lock().unwrap().clone()
    }

    fn finished(&self) -> Vec<StatusEvent> {
        self.events()
            .into_iter()
            .filter(|event| matches!(event, StatusEvent::ExchangeFinished { .. }))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// A node: a daemon's inbound side

struct Node {
    catalog: Arc<FakeCatalog>,
    folders: Arc<FakeFolders>,
    published: Arc<PublishedCatalog>,
    hub: Arc<StatusHub>,
    recorder: Arc<Recorder>,
    ctx: Arc<Ctx>,
    server: Arc<InboundServer>,
    endpoint: String,
    code: Mutex<String>,
}

impl Node {
    fn start() -> Node {
        Node::start_with(InboundOptions::default())
    }

    fn start_with(options: InboundOptions) -> Node {
        let (code, verifier) = Verifier::generate();
        let catalog = FakeCatalog::new(verifier);
        let folders = Arc::new(FakeFolders::default());
        let published = PublishedCatalog::with_base(1_000);
        let hub = StatusHub::new(HubConfig::default(), Arc::new(SystemClock));
        let recorder = Arc::new(Recorder {
            hub: hub.clone(),
            events: Mutex::new(Vec::new()),
        });
        let data = std::env::temp_dir().join("tkfs-inbound-v3");
        let ctx = Arc::new(Ctx {
            installation: SERVER.into(),
            config: Arc::new(Config {
                format_version: 2,
                data_directory: data.join("data"),
                default_mount_directory: data.join("mounts"),
                maximum_running: 8,
                network: Network::default(),
            }),
            catalog: catalog.clone(),
            folders: folders.clone(),
            published: published.clone(),
            status: recorder.clone(),
            status_source: hub.clone(),
            network: OnceLock::new(),
            legacy: OnceLock::new(),
            clock: Arc::new(SystemClock),
            shutdown: Cancel::new(),
            stopping: Arc::new(AtomicBool::new(false)),
            started_at_ms: 1,
            endpoints: Snapshot::new(DaemonEndpoints::default()),
        });
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = inbound::start_with(ctx.clone(), listener, options);
        Node {
            catalog,
            folders,
            published,
            hub,
            recorder,
            ctx,
            server,
            endpoint,
            code: Mutex::new(code),
        }
    }

    fn code(&self) -> String {
        self.code.lock().unwrap().clone()
    }

    fn rotate(&self) -> String {
        let code = self.catalog.rotate();
        *self.code.lock().unwrap() = code.clone();
        code
    }

    /// Adds a folder (catalog record and controller view) served by `worker`.
    fn folder(&self, state: &str, repo: &str, device: &str, worker: Option<Arc<FakeWorker>>) {
        let record = FolderRecord {
            state: state.into(),
            label: format!("label of {state}"),
            repo: repo.into(),
            device: device.into(),
            directory: std::env::temp_dir().join(state),
            mount: None,
            desired_running: true,
            sync_enabled: true,
            mount_enabled: false,
        };
        self.catalog.set_folder(record.clone());
        let phase = if worker.is_some() {
            FolderPhase::Running {
                since_ms: 1,
                instance: "instance".into(),
                pid: 1,
                address: "ws://127.0.0.1:1".into(),
                adopted: false,
            }
        } else {
            FolderPhase::Stopped
        };
        let view = FolderView {
            record,
            phase,
            ..FolderView::default()
        };
        self.hub
            .record(StatusEvent::FolderLifecycle(Box::new(view.clone())));
        self.folders
            .set(view, worker.map(|worker| worker as Arc<dyn WorkerClient>));
    }

    fn dial(&self, role: LaneRole, lane: Option<LaneSpec>) -> Result<Client, DialError> {
        self.dial_as(CLIENT, &self.code(), role, lane)
    }

    fn dial_as(
        &self,
        installation: &str,
        code: &str,
        role: LaneRole,
        lane: Option<LaneSpec>,
    ) -> Result<Client, DialError> {
        let hello = session::client_hello(installation, role, lane, 4);
        session::dial(
            &self.endpoint,
            hello,
            &CodeAuth { code: code.into() },
            None,
            ConnOptions::for_role(role),
        )
        .map(|(conn, server)| {
            assert_eq!(server.installation, SERVER);
            Client(conn)
        })
    }

    fn control(&self) -> Client {
        self.dial(LaneRole::Control, None).unwrap()
    }

    fn bulk(&self, index: u32, count: u32) -> Client {
        self.dial(LaneRole::Bulk, Some(LaneSpec::new(index, count)))
            .unwrap()
    }

    /// The hub snapshot once `ready` holds (the hub rebuilds lazily).
    fn status_until(
        &self,
        what: &str,
        ready: impl Fn(&StatusSnapshot) -> bool,
    ) -> Arc<StatusSnapshot> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let snapshot = tkfs::model::orchestrator::StatusSource::snapshot(&*self.hub);
            if ready(&snapshot) {
                return snapshot;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {what}: {snapshot:#?}"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.server.shutdown(Duration::from_secs(5));
    }
}

/// A dialer's session; closed when dropped (a `Conn` does not close itself).
struct Client(Conn);

impl Client {
    fn call(&self, op: &str, header: Value, body: Vec<u8>) -> Result<Reply, CallError> {
        self.0
            .call(op, header, body, Priority::Control, Duration::from_secs(30))
    }

    fn call_code(&self, op: &str, header: Value, body: Vec<u8>) -> String {
        match self.call(op, header, body) {
            Ok(reply) => panic!("{op} succeeded: {:?}", reply.header),
            Err(CallError::Remote(error)) => error.code,
            Err(other) => panic!("{op} failed locally: {other:?}"),
        }
    }

    fn ping(&self) {
        let reply = self.call("ping", json!({}), Vec::new()).unwrap();
        assert_eq!(reply.header["alive"], true);
    }

    fn catalog(&self, since: Option<u64>) -> CatalogListReply {
        let reply = self
            .call("catalog.list", json!({ "since": since }), Vec::new())
            .unwrap();
        if reply.body.is_empty() {
            serde_json::from_value(reply.header).unwrap()
        } else {
            assert_eq!(reply.header["in_body"], true);
            serde_json::from_slice(&reply.body).unwrap()
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.0.close();
    }
}

fn wait_until(what: &str, timeout: Duration, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !ready() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(10));
    }
}

// ---------------------------------------------------------------------------
// Scripted workers

fn meta_request(repo: &str, receiver: &str) -> MetaExchangeRequest {
    MetaExchangeRequest {
        format: 7,
        request: uuid::Uuid::new_v4().to_string(),
        sender: REMOTE_REPLICA.into(),
        receiver: receiver.into(),
        repo: repo.into(),
        page: FeedPage::default(),
        cursor: None,
        need: Vec::new(),
        state: FeedState::default(),
    }
}

fn meta_body(request: &MetaExchangeRequest) -> Vec<u8> {
    serde_json::to_vec(request).unwrap()
}

/// The acceptor's `feed-exchange` answer to `request`.
fn feed_reply(request: &FeedExchangeRequest, caught_up: bool) -> FeedExchangeReply {
    FeedExchangeReply {
        reply: MetaExchangeReply {
            format: 7,
            reply_to: request.request.request.clone(),
            sender: request.route.local_replica.clone(),
            receiver: request.request.sender.clone(),
            repo: request.request.repo.clone(),
            page_status: PageStatus::Accepted,
            cursor: Cursor::start("epoch"),
            page: FeedPage {
                epoch: "epoch".into(),
                tip: 12,
                ..FeedPage::default()
            },
            staged: 2,
            rejected: Vec::new(),
            need_served: Vec::new(),
            unknown: Vec::new(),
            state: FeedState::default(),
        },
        local: FeedApplyResult {
            summary: ExchangeSummary {
                received_events: 2,
                staged: 2,
                to_receive: 3,
                remote_tip: 12,
                caught_up,
                metadata_advanced: true,
                ..ExchangeSummary::default()
            },
            gate: GateView {
                caught_up,
                waiting_on: if caught_up {
                    Vec::new()
                } else {
                    vec!["3 events remaining at source".into()]
                },
                blocked: false,
            },
        },
    }
}

/// A worker serving `feed-exchange` (and, for bulk pages, echoing an empty
/// offer).
fn feed_worker(caught_up: bool) -> Arc<FakeWorker> {
    FakeWorker::new(move |op, payload| match op {
        "feed-exchange" => {
            let request: FeedExchangeRequest = serde_json::from_value(payload.clone()).unwrap();
            Ok(serde_json::to_value(feed_reply(&request, caught_up)).unwrap())
        }
        "published-bulk-exchange" => {
            let page: Page = serde_json::from_value(payload["page"].clone()).unwrap();
            Ok(serde_json::to_value(offer(&page, Vec::new())).unwrap())
        }
        other => Err(CallError::Remote(ErrorWire::coded(
            "UNSUPPORTED_OPERATION",
            other,
        ))),
    })
}

/// A hash owned by `lane` (its first 16 hex digits modulo the count).
fn hash_on(lane: Lane, salt: u64) -> String {
    let count = lane.count as u64;
    let mut prefix = salt * count + lane.index as u64;
    prefix %= u64::MAX / 2;
    format!("{prefix:016x}{}", "ab".repeat(24))
}

fn bulk_page(lane: Lane, receiver: &str, parts: BTreeMap<String, ObjectPart>) -> Page {
    Page {
        format: 1,
        repo: REPO_A.into(),
        sender: REMOTE_REPLICA.into(),
        receiver: receiver.into(),
        request: uuid::Uuid::new_v4().to_string(),
        reply_to: None,
        lane,
        known_objects: BTreeSet::new(),
        partial_objects: BTreeMap::new(),
        wanted_objects: BTreeSet::new(),
        object_parts: parts,
    }
}

/// The worker's offer in answer to `page`, carrying `parts`.
fn offer(page: &Page, parts: Vec<(String, ObjectPart)>) -> Page {
    Page {
        format: 1,
        repo: page.repo.clone(),
        sender: page.receiver.clone(),
        receiver: page.sender.clone(),
        request: uuid::Uuid::new_v4().to_string(),
        reply_to: Some(page.request.clone()),
        lane: page.lane,
        known_objects: BTreeSet::new(),
        partial_objects: BTreeMap::new(),
        wanted_objects: BTreeSet::new(),
        object_parts: parts.into_iter().collect(),
    }
}

fn part(offset: u64, total: u64, bytes: usize) -> ObjectPart {
    ObjectPart {
        offset,
        total,
        hex: "5a".repeat(bytes),
    }
}

/// A gate that hung workers wait on until released (or 30 s).
#[derive(Default)]
struct Release {
    open: Mutex<bool>,
    changed: Condvar,
}

impl Release {
    fn wait(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut open = self.open.lock().unwrap();
        while !*open && Instant::now() < deadline {
            open = self
                .changed
                .wait_timeout(open, Duration::from_millis(100))
                .unwrap()
                .0;
        }
    }

    fn open(&self) {
        *self.open.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

// ---------------------------------------------------------------------------
// Bounds

/// §14 WP2.8 acceptance: the 129th session is refused in under 100 ms, with
/// `TOO_MANY_SESSIONS`; a slot frees when a session closes.
#[test]
fn the_129th_session_is_refused_quickly() {
    let node = Node::start();
    let mut clients = Vec::new();
    for index in 0..128u32 {
        let lane = LaneSpec::new(index % 15, 15);
        let client = node
            .dial(LaneRole::Bulk, Some(lane))
            .unwrap_or_else(|error| panic!("session {index}: {error:?}"));
        clients.push(client);
    }
    // A dial returns once AUTH_OK is read; the server registers the session
    // just after (its slot is reserved meanwhile, and counts).
    assert_eq!(node.server.stats().sessions, 128);
    wait_until("128 registered sessions", Duration::from_secs(5), || {
        node.server.sessions().len() == 128
    });
    let started = Instant::now();
    let refused = node.dial(LaneRole::Control, None);
    let elapsed = started.elapsed();
    match refused {
        Err(DialError::Rejected(error)) => {
            assert_eq!(error.code, "TOO_MANY_SESSIONS", "{error}");
        }
        other => panic!("the 129th session was not refused: {:?}", other.map(|_| ())),
    }
    println!("the 129th session was refused after {elapsed:?}");
    assert!(
        elapsed < Duration::from_millis(100),
        "refused after {elapsed:?}"
    );
    let stats = node.server.stats();
    assert_eq!(stats.refused_sessions, 1);
    assert_eq!(
        stats.sessions, 128,
        "the refused handshake gave its slot back"
    );
    assert!(stats.sessions_peak <= 128, "{stats:?}");
    // Every session still works.
    clients[0].ping();
    clients[127].ping();
    // Closing one frees one slot.
    drop(clients.pop());
    wait_until("a slot to free", Duration::from_secs(5), || {
        node.server.stats().sessions == 127
    });
    let again = node.control();
    again.ping();
    assert_eq!(node.server.stats().sessions, 128);
}

/// Live heap bytes and their peak, counted by this test binary's allocator
/// (for [`preauth_flood_keeps_memory_flat`], which runs alone in a child
/// process).
static LIVE_HEAP: AtomicIsize = AtomicIsize::new(0);
static PEAK_HEAP: AtomicIsize = AtomicIsize::new(0);

struct CountingAllocator;

fn counted(delta: isize) {
    let live = LIVE_HEAP.fetch_add(delta, Ordering::Relaxed) + delta;
    PEAK_HEAP.fetch_max(live, Ordering::Relaxed);
}

// SAFETY: every call is forwarded to the system allocator unchanged; only the
// live byte count is updated around it.
unsafe impl std::alloc::GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        // SAFETY: the caller's contract is forwarded.
        let ptr = unsafe { std::alloc::System.alloc(layout) };
        if !ptr.is_null() {
            counted(layout.size() as isize);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: std::alloc::Layout) -> *mut u8 {
        // SAFETY: the caller's contract is forwarded.
        let ptr = unsafe { std::alloc::System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            counted(layout.size() as isize);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        // SAFETY: the caller's contract is forwarded.
        unsafe { std::alloc::System.dealloc(ptr, layout) };
        LIVE_HEAP.fetch_sub(layout.size() as isize, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller's contract is forwarded.
        let new = unsafe { std::alloc::System.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            counted(new_size as isize - layout.size() as isize);
        }
        new
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Child-process switch of the tests that must run alone in their process.
const CHILD_ENV: &str = "TKFS_INBOUND_V3_CHILD";
const MIB: usize = 1024 * 1024;

/// §14 WP2.8 acceptance (invariant 22): 100 sockets each sending 1 MiB of
/// pre-auth garbage (half of them random bytes, half an endless HTTP token,
/// which only tungstenite's 64 KiB upgrade bound stops) leave the live heap
/// flat, never hold more than 32 pre-auth slots, and a legitimate dial
/// started with the flood succeeds within 1 s. Runs alone in a child
/// process: it measures the live heap of the whole process.
#[test]
fn preauth_flood_keeps_memory_flat() {
    if std::env::var(CHILD_ENV).as_deref() == Ok("flood") {
        flood_child();
        return;
    }
    if std::env::var(CHILD_ENV).is_ok() {
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "preauth_flood_keeps_memory_flat",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_ENV, "flood")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stdout}\n{stderr}");
    assert!(stdout.contains("FLOOD peak="), "{stdout}");
}

fn flood_child() {
    const SOCKETS: usize = 100;
    let node = Node::start();
    // Warm up: one full session, so lazily built state does not count.
    node.control().ping();
    let garbage_random: Arc<Vec<u8>> = Arc::new(
        (0..MIB as u64)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect(),
    );
    let garbage_token: Arc<Vec<u8>> = Arc::new(vec![b'A'; MIB]);
    let address: SocketAddr = node.endpoint.trim_start_matches("ws://").parse().unwrap();
    thread::sleep(Duration::from_millis(200));
    let baseline = LIVE_HEAP.load(Ordering::SeqCst);
    PEAK_HEAP.store(baseline, Ordering::SeqCst);
    let started = Instant::now();
    let flood: Vec<_> = (0..SOCKETS)
        .map(|index| {
            let garbage = if index % 2 == 0 {
                garbage_random.clone()
            } else {
                garbage_token.clone()
            };
            thread::spawn(move || {
                let Ok(mut stream) = TcpStream::connect(address) else {
                    return;
                };
                let _ = stream.set_write_timeout(Some(Duration::from_secs(10)));
                let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
                let _ = stream.write_all(&garbage);
                let mut sink = [0u8; 1024];
                while matches!(stream.read(&mut sink), Ok(n) if n > 0) {}
            })
        })
        .collect();
    // The legitimate dialer starts with the flood and retries while every
    // slot is taken (it is closed at once, not held).
    let mut attempts = 0;
    let legit = loop {
        attempts += 1;
        match node.dial(LaneRole::Control, None) {
            Ok(client) => break client,
            Err(error) => {
                assert!(
                    started.elapsed() < Duration::from_secs(1),
                    "no legitimate session within 1 s ({attempts} attempts): {error:?}"
                );
                thread::sleep(Duration::from_millis(10));
            }
        }
    };
    let dialed = started.elapsed();
    assert!(
        dialed < Duration::from_secs(1),
        "legitimate dial took {dialed:?}"
    );
    legit.ping();
    for thread in flood {
        thread.join().unwrap();
    }
    let stats = node.server.stats();
    assert!(stats.preauth_peak <= 32, "{stats:?}");
    wait_until(
        "the pre-auth slots to drain",
        Duration::from_secs(10),
        || node.server.stats().preauth == 0,
    );
    let peak = PEAK_HEAP.load(Ordering::SeqCst) - baseline;
    drop(legit);
    println!(
        "FLOOD peak={peak} dialed_ms={} attempts={attempts} stats={stats:?}",
        dialed.as_millis()
    );
    assert!(
        peak < 16 * MIB as isize,
        "{SOCKETS} MiB of pre-auth garbage grew the live heap by {peak} bytes"
    );
    assert!(
        stats.handshake_failures + stats.refused_preauth >= SOCKETS as u64 / 2,
        "{stats:?}"
    );
}

/// A handshake that was admitted (its session slot reserved at `HELLO`) and
/// then failed (wrong code, or the dialer gave up) gives the slot back. It
/// does so when the server's handshake ends: after its `GOAWAY`, the server
/// drains for the dialer's close (≤ 1 s), so the dialer may learn of the
/// failure a moment before the slot is free again.
#[test]
fn a_failed_handshake_gives_its_session_slot_back() {
    let node = Node::start_with(InboundOptions {
        max_sessions: 1,
        ..InboundOptions::default()
    });
    for _ in 0..5 {
        match node.dial_as(CLIENT, "not-the-code", LaneRole::Control, None) {
            Err(DialError::Unauthorized(error)) => assert_eq!(error.code, "INVALID_PAIRING_CODE"),
            other => panic!("{:?}", other.map(|_| ())),
        }
        wait_until(
            "the reservation to be given back",
            Duration::from_secs(5),
            || node.server.stats().sessions == 0,
        );
    }
    assert_eq!(node.server.stats().refused_sessions, 0);
    let client = node.control();
    client.ping();
    match node.dial(LaneRole::Control, None) {
        Err(DialError::Rejected(error)) => assert_eq!(error.code, "TOO_MANY_SESSIONS"),
        other => panic!("{:?}", other.map(|_| ())),
    }
    let stats = node.server.stats();
    assert_eq!(
        (stats.sessions, stats.refused_sessions),
        (1, 1),
        "{stats:?}"
    );
}

/// A connection that never says anything holds its pre-auth slot for the
/// 5 s step budget only, and extra connections are closed at once.
#[test]
fn silent_and_extra_preauth_connections_are_bounded() {
    let node = Node::start_with(InboundOptions {
        max_preauth: 2,
        ..InboundOptions::default()
    });
    let address: SocketAddr = node.endpoint.trim_start_matches("ws://").parse().unwrap();
    let silent: Vec<TcpStream> = (0..2)
        .map(|_| TcpStream::connect(address).unwrap())
        .collect();
    wait_until("two pre-auth slots", Duration::from_secs(5), || {
        node.server.stats().preauth == 2
    });
    // The third is closed at once: it reads end-of-stream (or a reset).
    let mut extra = TcpStream::connect(address).unwrap();
    extra
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let started = Instant::now();
    let mut byte = [0u8; 1];
    let read = extra.read(&mut byte);
    assert!(
        matches!(read, Ok(0) | Err(_)) && started.elapsed() < Duration::from_secs(1),
        "{read:?} after {:?}",
        started.elapsed()
    );
    assert!(node.server.stats().refused_preauth >= 1);
    // The silent ones are dropped by the 5 s step budget.
    wait_until(
        "the silent handshakes to time out",
        Duration::from_secs(8),
        || node.server.stats().preauth == 0,
    );
    assert!(node.server.stats().handshake_failures >= 2);
    drop(silent);
    node.control().ping();
}

// ---------------------------------------------------------------------------
// Rotation

/// §14 WP2.8 acceptance: a verifier rotation sends
/// `GOAWAY{PAIRING_CODE_CHANGED}` to every older session within 1 s; the old
/// code is refused, the new one admitted and left alone.
#[test]
fn rotation_sends_goaway_to_older_sessions_within_1s() {
    let node = Node::start();
    let old_code = node.code();
    let sessions = vec![
        node.control(),
        node.bulk(0, 2),
        node.bulk(1, 2),
        node.dial(LaneRole::Probe, None).unwrap(),
        node.dial_as(OTHER_CLIENT, &old_code, LaneRole::Control, None)
            .unwrap(),
    ];
    for client in &sessions {
        client.ping();
    }
    let rotated = Instant::now();
    let new_code = node.rotate();
    for client in &sessions {
        let left = Duration::from_secs(1).saturating_sub(rotated.elapsed());
        let closed = client.0.wait_closed(left).unwrap_or_else(|| {
            panic!(
                "a session was still open {:?} after the rotation",
                rotated.elapsed()
            )
        });
        assert_eq!(closed.code, "PAIRING_CODE_CHANGED", "{closed}");
    }
    assert!(rotated.elapsed() < Duration::from_secs(1));
    assert_eq!(node.server.stats().rotation_goaways, sessions.len() as u64);
    match node.dial_as(CLIENT, &old_code, LaneRole::Control, None) {
        Err(DialError::Unauthorized(error)) => assert_eq!(error.code, "INVALID_PAIRING_CODE"),
        other => panic!("the old code was not refused: {:?}", other.map(|_| ())),
    }
    let fresh = node
        .dial_as(CLIENT, &new_code, LaneRole::Control, None)
        .unwrap();
    fresh.catalog(None);
    thread::sleep(Duration::from_millis(300));
    fresh.ping();
    assert!(fresh.0.closed().is_none());
    wait_until(
        "the old sessions to unregister",
        Duration::from_secs(10),
        || node.server.sessions().len() == 1,
    );
}

/// Invariant 21: no session authenticated under a rotated generation stays,
/// including one whose handshake completes while the verifier rotates (it is
/// checked when it registers, under the lock the watcher scans with).
#[test]
fn no_session_survives_a_rotation_during_its_handshake() {
    let node = Node::start_with(InboundOptions {
        max_sessions: 1024,
        ..InboundOptions::default()
    });
    for round in 0..3 {
        let code = node.code();
        let stop = Arc::new(AtomicBool::new(false));
        let dialers: Vec<_> = (0..8)
            .map(|_| {
                let endpoint = node.endpoint.clone();
                let code = code.clone();
                let stop = stop.clone();
                thread::spawn(move || {
                    // The last few sessions each dialer got: the ones
                    // closest to the rotation.
                    let mut kept = std::collections::VecDeque::new();
                    while !stop.load(Ordering::Acquire) {
                        let dialed = session::dial(
                            &endpoint,
                            session::client_hello(CLIENT, LaneRole::Control, None, 2),
                            &CodeAuth { code: code.clone() },
                            None,
                            ConnOptions::default(),
                        );
                        if let Ok((conn, _)) = dialed {
                            kept.push_back(Client(conn));
                            if kept.len() > 4 {
                                kept.pop_front();
                            }
                        }
                    }
                    kept
                })
            })
            .collect();
        thread::sleep(Duration::from_millis(150));
        node.rotate();
        thread::sleep(Duration::from_millis(150));
        stop.store(true, Ordering::Release);
        let clients: Vec<Client> = dialers
            .into_iter()
            .flat_map(|dialer| dialer.join().unwrap())
            .collect();
        assert!(!clients.is_empty(), "round {round}: no session was dialed");
        for client in &clients {
            let closed = client
                .0
                .wait_closed(Duration::from_secs(2))
                .unwrap_or_else(|| panic!("round {round}: an old-generation session stayed open"));
            assert_eq!(
                closed.code, "PAIRING_CODE_CHANGED",
                "round {round}: {closed}"
            );
        }
        let current = node.catalog.snapshot().verifier_generation;
        wait_until("only current sessions", Duration::from_secs(10), || {
            node.server
                .sessions()
                .iter()
                .all(|session| session.verifier_generation == current)
        });
    }
}

/// A revoked peer (`peer_meta.revoked_ms`, WP4.1) is refused at the
/// handshake and its live sessions are sent away.
#[test]
fn revoked_peers_are_refused_and_sent_away() {
    let node = Node::start();
    let other = node
        .dial_as(OTHER_CLIENT, &node.code(), LaneRole::Control, None)
        .unwrap();
    let client = node.control();
    node.catalog.edit(|snapshot| {
        snapshot.peer_meta.insert(
            CLIENT.into(),
            PeerMeta {
                installation: CLIENT.into(),
                revoked_ms: Some(1),
                ..PeerMeta::default()
            },
        );
    });
    let closed = client.0.wait_closed(Duration::from_secs(2)).unwrap();
    assert_eq!(closed.code, "PEER_REVOKED");
    match node.dial(LaneRole::Control, None) {
        Err(DialError::Rejected(error)) => assert_eq!(error.code, "PEER_REVOKED"),
        other => panic!("a revoked peer was admitted: {:?}", other.map(|_| ())),
    }
    other.ping();
    assert!(other.0.closed().is_none());
}

// ---------------------------------------------------------------------------
// catalog.list

/// §14 WP2.8 acceptance: `catalog.list` answers in under 100 ms while a hung
/// worker holds every exchange slot of the same session (it never calls a
/// worker, and exchanges never hold a handler thread); a fifth exchange is
/// refused `BUSY` at once.
#[test]
fn catalog_list_is_fast_with_a_hung_worker() {
    let node = Node::start();
    let release = Arc::new(Release::default());
    let hung = {
        let release = release.clone();
        FakeWorker::new(move |_, _| {
            release.wait();
            Err(CallError::Timeout { after_ms: 25_000 })
        })
    };
    node.folder(STATE_A, REPO_A, DEVICE_A, Some(hung.clone()));
    node.folder(STATE_B, REPO_B, DEVICE_B, Some(feed_worker(true)));
    for (state, repo, replica) in [(STATE_A, REPO_A, DEVICE_A), (STATE_B, REPO_B, DEVICE_B)] {
        node.published.update(
            state,
            Some(PublishedEntry {
                repo: repo.into(),
                replica: replica.into(),
                label: format!("label {state}"),
                state: EntryState::Running,
                ..PublishedEntry::default()
            }),
        );
    }
    let client = Arc::new(node.control());
    let hung_calls: Vec<_> = (0..4)
        .map(|_| {
            let client = client.clone();
            thread::spawn(move || {
                let request = meta_request(REPO_A, DEVICE_A);
                client.call(
                    "meta.exchange",
                    json!({ "repo": REPO_A }),
                    meta_body(&request),
                )
            })
        })
        .collect();
    wait_until("four hung worker calls", Duration::from_secs(5), || {
        hung.calls().len() == 4
    });
    let request = meta_request(REPO_B, DEVICE_B);
    let error = client
        .call(
            "meta.exchange",
            json!({ "repo": REPO_B }),
            meta_body(&request),
        )
        .unwrap_err();
    match &error {
        CallError::Remote(wire) => {
            assert_eq!(wire.code, "BUSY");
            assert_eq!(wire.class, ErrorClass::Contention);
            assert!(wire.retry_after_ms.is_some());
        }
        other => panic!("{other:?}"),
    }
    let mut slowest = Duration::ZERO;
    let (generation, _) = node.published.list();
    for _ in 0..20 {
        let started = Instant::now();
        let reply = client.catalog(None);
        slowest = slowest.max(started.elapsed());
        assert_eq!(reply.generation, generation);
        assert!(!reply.unchanged);
        let repos: Vec<_> = reply
            .entries
            .iter()
            .map(|entry| entry.repo.as_str())
            .collect();
        assert_eq!(repos, [REPO_A, REPO_B]);
    }
    println!("slowest catalog.list {slowest:?}");
    assert!(
        slowest < Duration::from_millis(100),
        "slowest catalog.list {slowest:?}"
    );
    let unchanged = client.catalog(Some(generation));
    assert!(unchanged.unchanged && unchanged.entries.is_empty());
    assert_eq!(unchanged.generation, generation);
    release.open();
    for call in hung_calls {
        match call.join().unwrap() {
            Err(CallError::Remote(error)) => assert_eq!(error.code, "REQUEST_TIMEOUT"),
            other => panic!("{other:?}"),
        }
    }
    // The slots are free again.
    let request = meta_request(REPO_B, DEVICE_B);
    client
        .call(
            "meta.exchange",
            json!({ "repo": REPO_B }),
            meta_body(&request),
        )
        .unwrap();
    assert_eq!(node.server.stats().busy_refusals, 1);
}

/// Exchanges are capped server-wide too, and an exchange keeps its slot
/// until its worker call returns, also after its session is gone: redialing
/// cannot pile up worker calls (invariant 22). Shutdown waits for them.
#[test]
fn exchanges_are_capped_server_wide_across_session_churn() {
    let node = Node::start_with(InboundOptions {
        max_exchanges: 3,
        ..InboundOptions::default()
    });
    let release = Arc::new(Release::default());
    let hung = {
        let release = release.clone();
        FakeWorker::new(move |_, _| {
            release.wait();
            Err(CallError::Timeout { after_ms: 25_000 })
        })
    };
    node.folder(STATE_A, REPO_A, DEVICE_A, Some(hung.clone()));
    // Two sessions take the three server-wide slots, then go away.
    let mut pending = Vec::new();
    let mut churned = Vec::new();
    for calls in [2, 1] {
        let client = Arc::new(node.control());
        churned.push(client.clone());
        for _ in 0..calls {
            let client = client.clone();
            pending.push(thread::spawn(move || {
                let request = meta_request(REPO_A, DEVICE_A);
                client.call(
                    "meta.exchange",
                    json!({ "repo": REPO_A }),
                    meta_body(&request),
                )
            }));
        }
    }
    wait_until("three hung worker calls", Duration::from_secs(5), || {
        hung.calls().len() == 3
    });
    assert_eq!(node.server.stats().exchanges, 3);
    // A new session finds the server full, not its own slots.
    let fresh = node.control();
    let request = meta_request(REPO_A, DEVICE_A);
    match fresh.call(
        "meta.exchange",
        json!({ "repo": REPO_A }),
        meta_body(&request),
    ) {
        Err(CallError::Remote(error)) => {
            assert_eq!(error.code, "BUSY");
            assert!(error.message.contains("on this orchestrator"), "{error}");
        }
        other => panic!("{other:?}"),
    }
    fresh.catalog(None);
    // The dialers give up and close; the worker calls still hold the slots.
    for client in &churned {
        client.0.close();
    }
    for call in pending {
        assert!(call.join().unwrap().is_err());
    }
    wait_until("the old sessions to close", Duration::from_secs(10), || {
        node.server.sessions().len() == 1
    });
    assert_eq!(node.server.stats().exchanges, 3);
    match fresh.call(
        "meta.exchange",
        json!({ "repo": REPO_A }),
        meta_body(&request),
    ) {
        Err(CallError::Remote(error)) => assert_eq!(error.code, "BUSY"),
        other => panic!("{other:?}"),
    }
    assert_eq!(hung.calls().len(), 3, "no worker call beyond the cap");
    // Shutdown waits for the worker calls (bounded by its deadline).
    let releaser = {
        let release = release.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(300));
            release.open();
        })
    };
    let started = Instant::now();
    node.server.shutdown(Duration::from_secs(5));
    let waited = started.elapsed();
    releaser.join().unwrap();
    assert!(
        waited >= Duration::from_millis(250) && waited < Duration::from_secs(5),
        "{waited:?}"
    );
    assert_eq!(node.server.stats().exchanges, 0);
}

/// A catalog too large for a response header travels as the body.
#[test]
fn a_large_catalog_travels_in_the_body() {
    let node = Node::start();
    for index in 0..200 {
        let state = format!("state-{index:03}");
        node.published.update(
            &state,
            Some(PublishedEntry {
                repo: format!("repo-{index:03}"),
                replica: format!("replica-{index:03}"),
                label: "x".repeat(400),
                state: EntryState::Running,
                ..PublishedEntry::default()
            }),
        );
    }
    let client = node.dial(LaneRole::Probe, None).unwrap();
    let reply = client.call("catalog.list", json!({}), Vec::new()).unwrap();
    assert_eq!(reply.header["in_body"], true);
    // A reader that parses only the header fails rather than read an empty
    // catalog.
    assert!(reply.header.get("entries").is_none(), "{}", reply.header);
    assert!(serde_json::from_value::<CatalogListReply>(reply.header.clone()).is_err());
    let catalog: CatalogListReply = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(catalog.entries.len(), 200);
    assert_eq!(catalog.generation, node.published.list().0);
    assert_eq!(client.catalog(None).entries.len(), 200);
}

// ---------------------------------------------------------------------------
// Roles and routes

/// §14 WP2.8 acceptance (invariants 18, 19): role and route violations keep
/// their codes, never reach a worker, and leave the session usable
/// (invariant 17).
#[test]
fn role_violations_and_route_violations_keep_their_codes() {
    let node = Node::start();
    let worker = feed_worker(true);
    node.folder(STATE_A, REPO_A, DEVICE_A, Some(worker.clone()));
    let control = node.control();
    let bulk = node.bulk(1, 3);
    let probe = node.dial(LaneRole::Probe, None).unwrap();
    let request = meta_request(REPO_A, DEVICE_A);
    let page = bulk_page(Lane { index: 1, count: 3 }, DEVICE_A, BTreeMap::new());
    let page_body = serde_json::to_vec(&page).unwrap();

    // Roles (invariant 18).
    let meta = |client: &Client| {
        client.call_code(
            "meta.exchange",
            json!({ "repo": REPO_A }),
            meta_body(&request),
        )
    };
    assert_eq!(meta(&bulk), "METADATA_ON_BULK_LANE");
    assert_eq!(meta(&probe), "METADATA_ON_BULK_LANE");
    assert_eq!(
        bulk.call_code("catalog.list", json!({}), Vec::new()),
        "METADATA_ON_BULK_LANE"
    );
    for op in [
        "objects.exchange",
        "objects.pull",
        "objects.push",
        "objects.wants",
        "objects.report",
    ] {
        assert_eq!(
            control.call_code(op, json!({}), page_body.clone()),
            "BULK_ON_CONTROL_LANE"
        );
        assert_eq!(
            probe.call_code(op, json!({}), page_body.clone()),
            "BULK_ON_CONTROL_LANE"
        );
    }
    assert_eq!(probe.catalog(None).entries.len(), 0, "probes may list");
    assert_eq!(
        control.call_code("objects.rebuild", json!({}), Vec::new()),
        "BULK_ON_CONTROL_LANE"
    );
    assert_eq!(
        control.call_code("catalog.drop", json!({}), Vec::new()),
        "UNKNOWN_PEER_OPERATION"
    );
    // The lane a bulk page names must be the session's (invariant 19).
    let wrong_lane = bulk_page(Lane { index: 2, count: 3 }, DEVICE_A, BTreeMap::new());
    assert_eq!(
        bulk.call_code(
            "objects.exchange",
            json!({}),
            serde_json::to_vec(&wrong_lane).unwrap()
        ),
        "BULK_LANE_MISMATCH"
    );
    let wrong_count = bulk_page(Lane { index: 1, count: 4 }, DEVICE_A, BTreeMap::new());
    assert_eq!(
        bulk.call_code(
            "objects.exchange",
            json!({}),
            serde_json::to_vec(&wrong_count).unwrap()
        ),
        "BULK_LANE_MISMATCH"
    );
    assert_eq!(
        bulk.call_code("objects.pull", json!({}), Vec::new()),
        "UNSUPPORTED_OPERATION"
    );

    // Routes.
    assert_eq!(
        control.call_code(
            "meta.exchange",
            json!({ "repo": REPO_B }),
            meta_body(&request)
        ),
        "REPOSITORY_MISMATCH"
    );
    assert_eq!(
        control.call_code("meta.exchange", json!({}), meta_body(&request)),
        "INVALID_REQUEST"
    );
    assert_eq!(
        control.call_code(
            "meta.exchange",
            json!({ "repo": REPO_A }),
            b"{not json".to_vec()
        ),
        "INVALID_REQUEST"
    );
    let unknown = meta_request(REPO_B, DEVICE_B);
    assert_eq!(
        control.call_code(
            "meta.exchange",
            json!({ "repo": REPO_B }),
            meta_body(&unknown)
        ),
        "REPOSITORY_NOT_MANAGED"
    );
    let misbound = meta_request(REPO_A, "some-other-replica");
    assert_eq!(
        control.call_code(
            "meta.exchange",
            json!({ "repo": REPO_A }),
            meta_body(&misbound)
        ),
        "LOCAL_REPLICA_BINDING_MISMATCH"
    );
    assert!(worker.calls().is_empty(), "{:?}", worker.calls());

    // Every refusal left every session usable.
    control.ping();
    bulk.ping();
    probe.ping();
    control
        .call(
            "meta.exchange",
            json!({ "repo": REPO_A }),
            meta_body(&request),
        )
        .unwrap();
    bulk.call("objects.exchange", json!({}), page_body).unwrap();
    assert_eq!(worker.calls().len(), 2);
}

/// A folder that is paused, stopped or whose worker cannot be reached is
/// answered without a worker call (`FOLDER_SYNC_PAUSED`,
/// `REMOTE_WORKER_STARTING`).
#[test]
fn paused_and_starting_folders_answer_without_a_worker() {
    let node = Node::start();
    let worker = feed_worker(true);
    node.folder(STATE_A, REPO_A, DEVICE_A, Some(worker.clone()));
    node.folder(STATE_B, REPO_B, DEVICE_B, None);
    let control = node.control();
    let bulk = node.bulk(0, 1);
    let request = meta_request(REPO_B, DEVICE_B);
    assert_eq!(
        control.call_code(
            "meta.exchange",
            json!({ "repo": REPO_B }),
            meta_body(&request)
        ),
        "REMOTE_WORKER_STARTING"
    );
    node.catalog
        .apply(CatalogCmd::SetIntents {
            state: STATE_A.into(),
            sync_enabled: Some(false),
            mount_enabled: None,
        })
        .unwrap();
    let request = meta_request(REPO_A, DEVICE_A);
    assert_eq!(
        control.call_code(
            "meta.exchange",
            json!({ "repo": REPO_A }),
            meta_body(&request)
        ),
        "FOLDER_SYNC_PAUSED"
    );
    let page = bulk_page(Lane { index: 0, count: 1 }, DEVICE_A, BTreeMap::new());
    assert_eq!(
        bulk.call_code(
            "objects.exchange",
            json!({}),
            serde_json::to_vec(&page).unwrap()
        ),
        "FOLDER_SYNC_PAUSED"
    );
    assert!(worker.calls().is_empty());
    assert!(
        node.recorder.finished().is_empty(),
        "refusals are not exchanges"
    );
    // A worker that cannot be reached is, to the peer, a remote folder that
    // is starting; the hub records the local reason.
    node.catalog
        .apply(CatalogCmd::SetIntents {
            state: STATE_A.into(),
            sync_enabled: Some(true),
            mount_enabled: None,
        })
        .unwrap();
    let unreachable = FakeWorker::new(|_, _| {
        Err(CallError::Unavailable(
            "LOCAL_WORKER_UNAVAILABLE: connection refused".into(),
        ))
    });
    node.folder(STATE_A, REPO_A, DEVICE_A, Some(unreachable));
    assert_eq!(
        control.call_code(
            "meta.exchange",
            json!({ "repo": REPO_A }),
            meta_body(&request)
        ),
        "REMOTE_WORKER_STARTING"
    );
    let finished = node.recorder.finished();
    match finished.as_slice() {
        [
            StatusEvent::ExchangeFinished {
                ok: false,
                error: Some(error),
                direction,
                ..
            },
        ] => {
            assert_eq!(error.code, "LOCAL_WORKER_UNAVAILABLE");
            assert_eq!(*direction, Direction::Inbound);
        }
        other => panic!("{other:#?}"),
    }
}

/// §14 WP2.8 acceptance (invariant 15): the route of every inbound exchange
/// names the **authenticated** installation of the session, whatever the
/// request says, plus this folder's replica and the request's sender.
#[test]
fn route_binding_uses_the_authenticated_installation() {
    let node = Node::start();
    let worker = feed_worker(true);
    node.folder(STATE_A, REPO_A, DEVICE_A, Some(worker.clone()));
    let code = node.code();
    for installation in [CLIENT, OTHER_CLIENT] {
        let control = node
            .dial_as(installation, &code, LaneRole::Control, None)
            .unwrap();
        let request = meta_request(REPO_A, DEVICE_A);
        // Header fields beyond `repo` are ignored, whatever they claim.
        control
            .call(
                "meta.exchange",
                json!({ "repo": REPO_A, "peer": "someone-else", "installation": "someone-else" }),
                meta_body(&request),
            )
            .unwrap();
        let bulk = node
            .dial_as(
                installation,
                &code,
                LaneRole::Bulk,
                Some(LaneSpec::new(0, 2)),
            )
            .unwrap();
        let page = bulk_page(Lane { index: 0, count: 2 }, DEVICE_A, BTreeMap::new());
        bulk.call(
            "objects.exchange",
            json!({}),
            serde_json::to_vec(&page).unwrap(),
        )
        .unwrap();
        let calls = worker.calls();
        let (op, payload) = &calls[calls.len() - 2];
        assert_eq!(op, "feed-exchange");
        let sent: FeedExchangeRequest = serde_json::from_value(payload.clone()).unwrap();
        assert_eq!(sent.route.peer, installation);
        assert_eq!(sent.route.repo, REPO_A);
        assert_eq!(sent.route.local_replica, DEVICE_A);
        assert_eq!(sent.route.remote_replica, REMOTE_REPLICA);
        assert_eq!(sent.request, request);
        let (op, payload) = &calls[calls.len() - 1];
        assert_eq!(op, "published-bulk-exchange");
        assert_eq!(payload["route"]["peer"], installation);
        assert_eq!(payload["route"]["local_replica"], DEVICE_A);
        assert_eq!(payload["route"]["remote_replica"], REMOTE_REPLICA);
        assert_eq!(payload["lane"], json!({ "index": 0, "count": 2 }));
        assert_eq!(payload["page"]["request"], page.request);
    }
    // Gate evidence names the authenticated installation too.
    let evidence = node.folders.evidence();
    let sources: Vec<_> = evidence
        .iter()
        .map(|(_, e)| e.installation.as_str())
        .collect();
    assert_eq!(sources, [CLIENT, OTHER_CLIENT]);
}

// ---------------------------------------------------------------------------
// Exchanges

/// `meta.exchange` reaches the worker's `feed-exchange` and answers its
/// `MetaExchangeReply`; worker errors keep their codes and the session.
#[test]
fn meta_exchange_serves_feed_exchange() {
    let node = Node::start();
    node.folder(STATE_A, REPO_A, DEVICE_A, Some(feed_worker(false)));
    let control = node.control();
    let request = meta_request(REPO_A, DEVICE_A);
    let reply = control
        .call(
            "meta.exchange",
            json!({ "repo": REPO_A }),
            meta_body(&request),
        )
        .unwrap();
    assert_eq!(reply.header["repo"], REPO_A);
    let answered: MetaExchangeReply = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(answered.reply_to, request.request);
    assert_eq!(answered.sender, DEVICE_A);
    assert_eq!(answered.staged, 2);
    // The acceptor's own view went to the hub and to the folder's gate.
    match node.recorder.finished().as_slice() {
        [
            StatusEvent::ExchangeFinished {
                installation,
                repo,
                lane,
                op,
                direction,
                ok: true,
                summary: Some(summary),
                bytes_up,
                bytes_down,
                ..
            },
        ] => {
            assert_eq!((installation.as_str(), repo.as_str()), (CLIENT, REPO_A));
            assert_eq!(
                (*lane, op.as_str(), *direction),
                (0, "meta.exchange", Direction::Inbound)
            );
            assert_eq!(summary.to_receive, 3);
            assert_eq!(*bytes_up, reply.body.len() as u64);
            assert_eq!(*bytes_down, meta_body(&request).len() as u64);
            assert_eq!(summary.bytes_up, *bytes_up);
        }
        other => panic!("{other:#?}"),
    }
    match node.folders.evidence().as_slice() {
        [(state, evidence)] => {
            assert_eq!(state, STATE_A);
            assert_eq!(evidence.installation, CLIENT);
            assert!(!evidence.caught_up && !evidence.blocked);
            assert_eq!(evidence.waiting_on, ["3 events remaining at source"]);
        }
        other => panic!("{other:#?}"),
    }

    // A worker Remote error passes through and keeps the session.
    let failing = FakeWorker::new(|_, _| {
        Err(CallError::Remote(ErrorWire::coded(
            "EVENT_ID_REUSED",
            "event e1 was staged with another payload",
        )))
    });
    node.folder(STATE_A, REPO_A, DEVICE_A, Some(failing));
    let error = control
        .call(
            "meta.exchange",
            json!({ "repo": REPO_A }),
            meta_body(&request),
        )
        .unwrap_err();
    assert!(
        matches!(&error, CallError::Remote(wire) if wire.code == "EVENT_ID_REUSED" && wire.class == ErrorClass::Fatal),
        "{error:?}"
    );
    control.ping();

    // A reply that is not bound to the request never reaches the peer.
    let unbound = FakeWorker::new(|_, payload| {
        let mut request: FeedExchangeRequest = serde_json::from_value(payload.clone()).unwrap();
        request.request.request = "another-request".into();
        Ok(serde_json::to_value(feed_reply(&request, true)).unwrap())
    });
    node.folder(STATE_A, REPO_A, DEVICE_A, Some(unbound));
    assert_eq!(
        control.call_code(
            "meta.exchange",
            json!({ "repo": REPO_A }),
            meta_body(&request)
        ),
        "UNBOUND_PEER_RESPONSE"
    );
    // So does a reply the worker garbled.
    let garbled = FakeWorker::new(|_, _| Ok(json!({ "reply": 3 })));
    node.folder(STATE_A, REPO_A, DEVICE_A, Some(garbled));
    assert_eq!(
        control.call_code(
            "meta.exchange",
            json!({ "repo": REPO_A }),
            meta_body(&request)
        ),
        "INVALID_REPLY"
    );
    let failures: Vec<_> = node
        .recorder
        .finished()
        .into_iter()
        .filter_map(|event| match event {
            StatusEvent::ExchangeFinished {
                ok: false, error, ..
            } => error.map(|e| e.code),
            _ => None,
        })
        .collect();
    assert_eq!(
        failures,
        ["EVENT_ID_REUSED", "UNBOUND_PEER_RESPONSE", "INVALID_REPLY"]
    );
    control.catalog(None);
}

/// The interim `objects.exchange` (§6.5) reaches `published-bulk-exchange`,
/// counts the objects the page completed, and trims an offer above the
/// session's 1 MiB body bound to the parts that fit.
#[test]
fn objects_exchange_serves_the_interim_bulk_path() {
    let node = Node::start();
    let lane = Lane { index: 1, count: 2 };
    let big = Arc::new(Mutex::new(Vec::<(String, ObjectPart)>::new()));
    let worker = {
        let big = big.clone();
        FakeWorker::new(move |op, payload| {
            assert_eq!(op, "published-bulk-exchange");
            let page: Page = serde_json::from_value(payload["page"].clone()).unwrap();
            Ok(serde_json::to_value(offer(&page, big.lock().unwrap().clone())).unwrap())
        })
    };
    node.folder(STATE_A, REPO_A, DEVICE_A, Some(worker.clone()));
    let bulk = node.bulk(1, 2);
    // Two parts arrive: one completes its object, one does not.
    let parts: BTreeMap<String, ObjectPart> = [
        (hash_on(lane, 1), part(0, 1000, 1000)),
        (hash_on(lane, 2), part(0, 5000, 1000)),
    ]
    .into_iter()
    .collect();
    let page = bulk_page(lane, DEVICE_A, parts);
    let reply = bulk
        .call(
            "objects.exchange",
            json!({ "repo": REPO_A }),
            serde_json::to_vec(&page).unwrap(),
        )
        .unwrap();
    assert_eq!(reply.header["repo"], REPO_A);
    let answered: Page = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(answered.reply_to.as_deref(), Some(page.request.as_str()));
    match node.recorder.finished().as_slice() {
        [
            StatusEvent::ExchangeFinished {
                lane: status_lane,
                op,
                direction,
                ok: true,
                bytes_down,
                bytes_up,
                objects_down,
                objects_up,
                summary: None,
                ..
            },
        ] => {
            assert_eq!(*status_lane, 2, "bulk lane index 1 is status lane 2");
            assert_eq!(
                (op.as_str(), *direction),
                ("objects.exchange", Direction::Inbound)
            );
            assert_eq!((*bytes_down, *objects_down), (2000, 1));
            assert_eq!((*bytes_up, *objects_up), (0, 0));
        }
        other => panic!("{other:#?}"),
    }

    // An offer of 3 × 3 MiB (18 MiB of hex) is trimmed under the 16 MiB
    // `objects.exchange` bound (WP2.INT: CCR-WP2.8-1 raised it from 1 MiB,
    // where this case was 3 × 400 KiB trimmed to one part).
    *big.lock().unwrap() = (1..=3)
        .map(|salt| (hash_on(lane, 10 + salt), part(0, 3 * MIB as u64, 3 * MIB)))
        .collect();
    let page = bulk_page(lane, DEVICE_A, BTreeMap::new());
    let reply = bulk
        .call(
            "objects.exchange",
            json!({}),
            serde_json::to_vec(&page).unwrap(),
        )
        .unwrap();
    assert!(reply.body.len() <= 16 * MIB, "{} bytes", reply.body.len());
    let answered: Page = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(answered.object_parts.len(), 2, "two 6 MiB parts fit");
    let first = hash_on(lane, 11);
    assert!(answered.object_parts.contains_key(&first));
    match node.recorder.finished().last() {
        Some(StatusEvent::ExchangeFinished { bytes_up, .. }) => {
            assert_eq!(*bytes_up, 2 * 3 * MIB as u64)
        }
        other => panic!("{other:#?}"),
    }
    // A page sent as the request header (no body) is accepted too.
    *big.lock().unwrap() = Vec::new();
    let small = bulk_page(lane, DEVICE_A, BTreeMap::new());
    bulk.call(
        "objects.exchange",
        serde_json::to_value(&small).unwrap(),
        Vec::new(),
    )
    .unwrap();
    assert_eq!(worker.calls().len(), 3);
}

// ---------------------------------------------------------------------------
// Legacy and visibility

/// §14 WP2.8 acceptance: a legacy (v2) hello is answered in the legacy
/// format with `PEER_NEEDS_UPGRADE` and recorded as `InboundIncompatible`
/// (the pairing code is never recorded).
#[test]
fn a_legacy_hello_gets_needs_upgrade_and_is_recorded() {
    let node = Node::start();
    let mut socket = ws::connect(&node.endpoint).unwrap();
    let local = match socket.get_ref() {
        tungstenite::stream::MaybeTlsStream::Plain(stream) => stream.local_addr().unwrap(),
        _ => unreachable!(),
    };
    let secret = node.code();
    let error = ws::call(
        &mut socket,
        &json!({"op":"pair","version":2,"code":secret,"installation":"legacy-peer\u{7}",
                "lane":"control","bulk_protocol":1,"bulk_lane":null}),
    )
    .unwrap_err();
    let remote = error
        .downcast_ref::<ws::RemoteError>()
        .expect("a legacy-format error reply");
    let expected: Value = serde_json::from_str(LEGACY_NEEDS_UPGRADE_REPLY).unwrap();
    assert_eq!(remote.0, expected["error"].as_str().unwrap());
    wait_until(
        "the legacy attempt to be counted",
        Duration::from_secs(5),
        || node.server.stats().legacy_refused == 1,
    );
    let incompatible: Vec<_> = node
        .recorder
        .events()
        .into_iter()
        .filter_map(|event| match event {
            StatusEvent::InboundIncompatible { address, detail } => Some((address, detail)),
            _ => None,
        })
        .collect();
    match incompatible.as_slice() {
        [(address, detail)] => {
            assert_eq!(*address, local.to_string());
            assert!(detail.starts_with("PEER_NEEDS_UPGRADE: "), "{detail}");
            assert!(
                detail.contains("legacy-peer\\u{7}"),
                "control characters escaped: {detail}"
            );
            assert!(!detail.contains(&secret));
        }
        other => panic!("{other:#?}"),
    }
    let snapshot = node.status_until("the notice", |snapshot| !snapshot.daemon.errors.is_empty());
    let notice = &snapshot.daemon.errors[0];
    assert_eq!(notice.error.code, "PEER_NEEDS_UPGRADE");
    assert!(!serde_json::to_string(&*snapshot).unwrap().contains(&secret));
    assert!(node.server.sessions().is_empty());
}

/// RC12: the serving side has status. Inbound sessions show as an
/// inbound-only peer with Ready lanes; served exchanges show in the folder's
/// per-peer sync; closing the sessions clears them.
#[test]
fn the_serving_side_reports_status() {
    let node = Node::start();
    node.folder(STATE_A, REPO_A, DEVICE_A, Some(feed_worker(false)));
    let control = node.control();
    let bulk = node.bulk(0, 3);
    let probe = node.dial(LaneRole::Probe, None).unwrap();
    probe.catalog(None);
    let snapshot = node.status_until("two inbound sessions", |snapshot| {
        snapshot
            .peers
            .iter()
            .any(|peer| peer.installation == CLIENT && peer.inbound_sessions == 2)
    });
    let peer = snapshot
        .peers
        .iter()
        .find(|peer| peer.installation == CLIENT)
        .unwrap();
    assert_eq!(peer.direction, Direction::Inbound);
    let mut lanes: Vec<_> = peer
        .lanes
        .iter()
        .map(|lane| (lane.lane, lane.role, lane.direction, lane.state))
        .collect();
    lanes.sort_by_key(|lane| lane.0);
    assert_eq!(
        lanes,
        [
            (0, LaneRole::Control, Direction::Inbound, LaneState::Ready),
            (1, LaneRole::Bulk, Direction::Inbound, LaneState::Ready),
        ],
        "probe sessions are not lanes"
    );
    let request = meta_request(REPO_A, DEVICE_A);
    control
        .call(
            "meta.exchange",
            json!({ "repo": REPO_A }),
            meta_body(&request),
        )
        .unwrap();
    let snapshot = node.status_until("the folder's inbound peer", |snapshot| {
        snapshot.folders.iter().any(|folder| {
            folder
                .sync
                .peers
                .iter()
                .any(|peer| peer.installation == CLIENT && peer.metadata.to_receive == 3)
        })
    });
    let folder = snapshot
        .folders
        .iter()
        .find(|f| f.state == STATE_A)
        .unwrap();
    let pair = folder
        .sync
        .peers
        .iter()
        .find(|peer| peer.installation == CLIENT)
        .unwrap();
    assert_eq!(pair.direction, Direction::Inbound);
    assert!(pair.metadata.last_exchange_ms.is_some());
    let events = node.recorder.events();
    let opened = events
        .iter()
        .filter(|event| matches!(event, StatusEvent::InboundSession { opened: true, .. }))
        .count();
    assert_eq!(opened, 2);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, StatusEvent::ExchangeStarted { .. })),
        "ExchangeStarted would read as outbound work"
    );
    drop(control);
    drop(bulk);
    drop(probe);
    node.status_until("no inbound sessions", |snapshot| {
        snapshot
            .peers
            .iter()
            .filter(|peer| peer.installation == CLIENT)
            .all(|peer| peer.inbound_sessions == 0)
    });
    let closed = node
        .recorder
        .events()
        .iter()
        .filter(|event| matches!(event, StatusEvent::InboundSession { opened: false, .. }))
        .count();
    assert_eq!(closed, 2);
}

// ---------------------------------------------------------------------------
// Shutdown

/// Shutdown sends `GOAWAY{ORCHESTRATOR_STOPPING}`, waits for the sessions to
/// end and stops accepting; a daemon-level cancel does the same.
#[test]
fn shutdown_sends_sessions_away_and_stops_accepting() {
    let node = Node::start();
    let control = node.control();
    let bulk = node.bulk(0, 1);
    let started = Instant::now();
    node.server.shutdown(Duration::from_secs(5));
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    for client in [&control, &bulk] {
        let closed = client.0.wait_closed(Duration::from_secs(2)).unwrap();
        assert_eq!(closed.code, "ORCHESTRATOR_STOPPING", "{closed}");
    }
    assert!(node.server.sessions().is_empty());
    assert!(node.dial(LaneRole::Control, None).is_err());
    // Idempotent.
    node.server.shutdown(Duration::from_secs(1));

    let node = Node::start();
    let control = node.control();
    node.ctx.shutdown.cancel();
    let closed = control.0.wait_closed(Duration::from_secs(3)).unwrap();
    assert_eq!(closed.code, "ORCHESTRATOR_STOPPING");
    let stopped = Instant::now();
    wait_until("the listener to close", Duration::from_secs(3), || {
        node.dial(LaneRole::Control, None).is_err()
    });
    assert!(stopped.elapsed() < Duration::from_secs(3));
}

/// Requests on a daemon that is stopping are refused, and requests on a
/// session whose generation was rotated are refused even before its
/// `GOAWAY` lands.
#[test]
fn stopping_and_rotated_sessions_refuse_requests() {
    let node = Node::start();
    node.folder(STATE_A, REPO_A, DEVICE_A, Some(feed_worker(true)));
    let control = node.control();
    node.ctx.stopping.store(true, Ordering::Release);
    assert_eq!(
        control.call_code("catalog.list", json!({}), Vec::new()),
        "ORCHESTRATOR_STOPPING"
    );
    node.ctx.stopping.store(false, Ordering::Release);
    control.catalog(None);
    // The rotation and the next request race the watcher's GOAWAY: either
    // refusal is correct, and nothing is served on the old generation.
    node.rotate();
    match control.call("catalog.list", json!({}), Vec::new()) {
        Err(CallError::Remote(error)) => {
            assert!(
                ["PAIRING_CODE_CHANGED", "WS_CLOSED"].contains(&error.code.as_str()),
                "{error}"
            );
        }
        Err(CallError::Unavailable(detail)) => {
            assert!(detail.contains("PAIRING_CODE_CHANGED"), "{detail}");
        }
        other => panic!("served on a rotated generation: {other:?}"),
    }
}
