//! Real authenticated peer + loopback sessions and real SQLite/CAS workers.
//! Catalog/folder actors are fixed fixtures; no object RPC is faked.
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    net::TcpListener,
    sync::{Arc, Mutex, OnceLock, atomic::AtomicBool},
    thread,
    time::{Duration, Instant},
};
use tkfs::{
    content::{ContentProvider, LocalContent, ReadContext},
    core::{Bundle, Change, Store, admission::reference_id},
    model::{
        content::{Digest, FileContentRef, ObjectKey, ObjectKind},
        content_wire as wire,
        errors::ErrorWire,
        ipc::StopMode,
        orchestrator::*,
        protocol::{LaneRole, LaneSpec},
        status::{StatusEvent, StatusSnapshot},
    },
    orchestrator::{
        Config, Network,
        content_transport::{self, pull_one, worker_call},
        inbound::{self, InboundOptions, InboundServer},
        worker_client::SessionWorkerClient,
    },
    paired_sync::SyncRoute,
    pairing_code::Verifier,
    runtime::{self, Engine, ctx::WorkerCtx, server::RpcServer},
    session::{self, CodeAuth, Conn, ConnOptions, Priority},
    util::{Cancel, snapshot::Snapshot, time::SystemClock},
};
use uuid::Uuid;
const LANE: LaneSpec = LaneSpec { index: 0, count: 1 };
fn context() -> ReadContext {
    ReadContext {
        deadline: Instant::now() + Duration::from_secs(30),
        cancel: Cancel::new(),
    }
}
fn store(path: &std::path::Path, repo: &str) -> Store {
    let s = Store::initialize(path, repo, &tkfs::core::id()).unwrap();
    s.db.execute("INSERT INTO config VALUES('fixture:content_v1','1')", [])
        .unwrap();
    s
}
fn publish(store: &mut Store, provider: &LocalContent, bytes: &[u8]) -> (String, FileContentRef) {
    let entry = store.create("file", "file").unwrap();
    let root = provider
        .import(&mut &bytes[..], bytes.len() as u64, &context())
        .unwrap();
    let event = store.event(
        &store.branch(&store.active).unwrap(),
        vec![Change {
            entity: entry.id.clone(),
            field: "content".into(),
            parents: entry
                .revisions
                .get("content")
                .cloned()
                .into_iter()
                .collect(),
            value: serde_json::to_value(&root).unwrap(),
        }],
    );
    let reference = reference_id(&event.id, &entry.id);
    store.commit(event, true).unwrap();
    (reference, root)
}
struct Worker {
    ctx: Arc<WorkerCtx>,
    server: RpcServer,
    endpoint: WorkerEndpoint,
}
impl Worker {
    fn new(store: Store) -> Self {
        let root = store.root.clone();
        let ctx =
            tkfs::worker::build_ctx(Arc::new(Mutex::new(Engine::new(store))), &root, false, None);
        let server = runtime::server::start_rpc_server(ctx.clone()).unwrap();
        let endpoint = WorkerEndpoint {
            instance: ctx.instance.clone(),
            pid: std::process::id(),
            address: server.discovery().address.clone(),
            version: ctx.version.clone(),
            token: Some(server.token().into()),
        };
        Self {
            ctx,
            server,
            endpoint,
        }
    }
    fn client(&self) -> Arc<SessionWorkerClient> {
        SessionWorkerClient::connect(&self.endpoint).unwrap()
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.server.stop(StopMode::Force);
    }
}

struct World {
    catalog: Snapshot<CatalogSnapshot>,
    views: Vec<Arc<FolderView>>,
    clients: BTreeMap<String, Arc<dyn WorkerClient>>,
}
impl World {
    fn new(worker: &Worker, verifier: Option<Verifier>) -> Arc<Self> {
        Self::from_workers(&[worker], verifier)
    }
    fn from_workers(workers: &[&Worker], verifier: Option<Verifier>) -> Arc<Self> {
        let mut records = vec![];
        let mut views = vec![];
        let mut clients = BTreeMap::new();
        for worker in workers {
            let record = FolderRecord {
                state: worker.ctx.root.to_string_lossy().into_owned(),
                label: "content".into(),
                repo: worker.ctx.repo.clone(),
                device: worker.ctx.device.clone(),
                directory: worker.ctx.root.clone(),
                mount: None,
                desired_running: true,
                sync_enabled: true,
                mount_enabled: false,
            };
            let view = FolderView {
                record: record.clone(),
                phase: FolderPhase::Running {
                    since_ms: 1,
                    instance: worker.ctx.instance.clone(),
                    pid: std::process::id(),
                    address: worker.endpoint.address.clone(),
                    adopted: false,
                },
                worker: Some(worker.endpoint.clone()),
                ..Default::default()
            };
            clients.insert(
                record.state.clone(),
                worker.client() as Arc<dyn WorkerClient>,
            );
            records.push(record);
            views.push(Arc::new(view));
        }
        Arc::new(Self {
            catalog: Snapshot::new(CatalogSnapshot {
                installation: Uuid::new_v4().to_string(),
                generation: 1,
                folders: records,
                verifier,
                verifier_generation: 1,
                ..Default::default()
            }),
            views,
            clients,
        })
    }
    fn ctx(self: &Arc<Self>) -> Arc<Ctx> {
        Arc::new(Ctx {
            installation: self.catalog.load().installation.clone(),
            config: Arc::new(Config {
                format_version: 2,
                data_directory: std::env::temp_dir().join("tkfs-content-test"),
                default_mount_directory: std::env::temp_dir().join("tkfs-content-test-mounts"),
                maximum_running: 8,
                network: Network {
                    listen: None,
                    connections: 2,
                    sync_interval_ms: 100,
                },
            }),
            catalog: self.clone(),
            folders: self.clone(),
            published: self.clone(),
            status: self.clone(),
            status_source: self.clone(),
            network: OnceLock::new(),
            legacy: OnceLock::new(),
            extensions: OnceLock::new(),
            clock: Arc::new(SystemClock),
            shutdown: Cancel::new(),
            stopping: Arc::new(AtomicBool::new(false)),
            started_at_ms: 1,
            endpoints: Snapshot::new(DaemonEndpoints::default()),
        })
    }
}
impl CatalogApi for World {
    fn snapshot(&self) -> Arc<CatalogSnapshot> {
        self.catalog.load()
    }
    fn apply(&self, _: CatalogCmd) -> Result<Arc<CatalogSnapshot>, ErrorWire> {
        Err(ErrorWire::coded("CATALOG_UNAVAILABLE", "fixture"))
    }
    fn wait_newer(&self, _: u64, wait: Duration) -> Arc<CatalogSnapshot> {
        thread::sleep(wait.min(Duration::from_millis(25)));
        self.catalog.load()
    }
}
impl FolderDirectory for World {
    fn view(&self, state: &str) -> Option<Arc<FolderView>> {
        self.views.iter().find(|v| v.record.state == state).cloned()
    }
    fn by_repo(&self, repo: &str) -> Option<Arc<FolderView>> {
        self.views.iter().find(|v| v.record.repo == repo).cloned()
    }
    fn all(&self) -> Vec<Arc<FolderView>> {
        self.views.clone()
    }
    fn send(&self, _: &str, _: FolderCmd) -> Result<(), ErrorWire> {
        Ok(())
    }
    fn request(
        &self,
        state: &str,
        _: FolderCmd,
        _: Duration,
    ) -> Result<Arc<FolderView>, ErrorWire> {
        self.view(state)
            .ok_or_else(|| ErrorWire::coded("FOLDER_NOT_FOUND", state))
    }
    fn client(&self, state: &str) -> Option<Arc<dyn WorkerClient>> {
        self.clients.get(state).cloned()
    }
    fn generation(&self) -> u64 {
        1
    }
    fn wait_newer(&self, g: u64, wait: Duration) -> u64 {
        thread::sleep(wait.min(Duration::from_millis(25)));
        g
    }
    fn shutdown(&self, _: Duration) -> Vec<ShutdownOutcome> {
        vec![]
    }
}
impl PublishedCatalogApi for World {
    fn update(&self, _: &str, _: Option<PublishedEntry>) {}
    fn list(&self) -> (u64, Arc<Vec<PublishedEntry>>) {
        (0, Arc::new(vec![]))
    }
    fn wait_newer(&self, g: u64, _: Duration) -> u64 {
        g
    }
}
impl StatusSink for World {
    fn record(&self, _: StatusEvent) {}
}
impl StatusSource for World {
    fn snapshot(&self) -> Arc<StatusSnapshot> {
        Arc::new(StatusSnapshot::default())
    }
    fn wait_newer(&self, _: u64, _: Duration) -> Arc<StatusSnapshot> {
        StatusSource::snapshot(self)
    }
}
struct Pair {
    _dir: tempfile::TempDir,
    a: Worker,
    b: Worker,
    ap: LocalContent,
    bp: LocalContent,
    source: Arc<Ctx>,
    dest: Arc<Ctx>,
    server: Arc<InboundServer>,
    conn: Conn,
    address: String,
    code: String,
    reference: String,
    root: FileContentRef,
    route: SyncRoute,
    scope: wire::PeerScope,
}
impl Pair {
    fn peer(&self, local: &Arc<Ctx>) -> Arc<PeerContext> {
        Arc::new(PeerContext {
            installation: self.source.installation.clone(),
            endpoint: self.address.clone(),
            local_installation: local.installation.clone(),
            pool: 2,
            catalog: Snapshot::new(RemoteCatalog {
                generation: 1,
                at_ms: 1,
                entries: vec![PublishedEntry {
                    repo: self.scope.repo.clone(),
                    replica: self.scope.receiver.clone(),
                    state: EntryState::Running,
                    ..Default::default()
                }],
            }),
        })
    }
    fn dial_for(&self, local: &Arc<Ctx>) -> Conn {
        session::dial(
            &self.address,
            session::client_hello(&local.installation, LaneRole::Bulk, Some(LANE), 2),
            &CodeAuth {
                code: self.code.clone(),
            },
            None,
            ConnOptions::for_role(LaneRole::Bulk),
        )
        .unwrap()
        .0
    }
    fn new(bytes: &[u8]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let repo = tkfs::core::id();
        let mut a = store(&dir.path().join("a"), &repo);
        let mut b = store(&dir.path().join("b"), &repo);
        let ap = LocalContent::open(&a.root).unwrap();
        let bp = LocalContent::open(&b.root).unwrap();
        let (reference, root) = publish(&mut a, &ap, bytes);
        b.receive(
            Bundle {
                repo: repo.clone(),
                branches: vec![],
                objects: BTreeMap::new(),
                events: a.events().unwrap(),
            },
            &BTreeSet::from([a.device.clone()]),
        )
        .unwrap();
        let a = Worker::new(a);
        let b = Worker::new(b);
        let (code, verifier) = Verifier::generate();
        let source = World::new(&a, Some(verifier)).ctx();
        let dest = World::new(&b, None).ctx();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("ws://{}", listener.local_addr().unwrap());
        let server = inbound::start_with(source.clone(), listener, InboundOptions::default());
        let hello = session::client_hello(&dest.installation, LaneRole::Bulk, Some(LANE), 2);
        let (conn, _) = session::dial(
            &address,
            hello,
            &CodeAuth { code: code.clone() },
            None,
            ConnOptions::for_role(LaneRole::Bulk),
        )
        .unwrap();
        let route = SyncRoute {
            peer: source.installation.clone(),
            repo: repo.clone(),
            local_replica: b.ctx.device.clone(),
            remote_replica: a.ctx.device.clone(),
        };
        let scope = wire::PeerScope {
            repo,
            sender: b.ctx.device.clone(),
            receiver: a.ctx.device.clone(),
            lane: LANE,
        };
        Self {
            _dir: dir,
            a,
            b,
            ap,
            bp,
            source,
            dest,
            server,
            conn,
            address,
            code,
            reference,
            root,
            route,
            scope,
        }
    }
    fn position(&self) -> wire::Position {
        wire::Position {
            reference: self.reference.clone(),
            edge: tkfs::content::ContentEdge {
                key: ObjectKey::root(&self.root),
                offset: 0,
                len: self.root.total_len,
                level: if self.root.kind == tkfs::model::content::ContentKind::Manifest {
                    Some(
                        tkfs::model::content::ManifestNode::root_level(self.root.total_len)
                            .unwrap(),
                    )
                } else {
                    None
                },
            },
        }
    }
    fn read(&self, conn: &Conn, position: wire::Position) -> Result<session::Reply, CallError> {
        conn.call(
            wire::READ,
            serde_json::to_value(wire::PeerRead {
                scope: self.scope.clone(),
                position,
                priority: tkfs::model::workspace::DemandPriority::Foreground,
            })
            .unwrap(),
            vec![],
            Priority::Control,
            Duration::from_secs(5),
        )
    }
    fn caps(&self, conn: &Conn) {
        let reply = conn
            .call(
                wire::CAPS,
                serde_json::to_value(wire::PeerCaps {
                    scope: self.scope.clone(),
                    version: 1,
                })
                .unwrap(),
                vec![],
                Priority::Control,
                Duration::from_secs(5),
            )
            .unwrap();
        assert!(
            serde_json::from_value::<wire::Capabilities>(reply.header)
                .unwrap()
                .active
        );
    }
    fn ack(&self, conn: &Conn, token: wire::Token) -> Result<session::Reply, CallError> {
        conn.call(
            wire::ACK,
            serde_json::to_value(wire::PeerAck {
                scope: self.scope.clone(),
                token,
            })
            .unwrap(),
            vec![],
            Priority::Control,
            Duration::from_secs(2),
        )
    }
}
impl Drop for Pair {
    fn drop(&mut self) {
        self.conn.close();
        self.source.shutdown.cancel();
        self.dest.shutdown.cancel();
        self.server.shutdown(Duration::from_secs(2));
    }
}

// An authenticated inbound receiver backed by the actual metadata/CAS worker.
// Pair's original direction has its bytes at A; these tests push them to B.
struct LegacyPushReceiver {
    ctx: Arc<Ctx>,
    server: Arc<InboundServer>,
    address: String,
    code: String,
    installation: String,
    scope: wire::PeerScope,
}
impl LegacyPushReceiver {
    fn new(p: &Pair) -> Self {
        for worker in [&p.a, &p.b] {
            worker
                .ctx
                .sync_stores
                .checkout()
                .unwrap()
                .db
                .execute(
                    "UPDATE config SET value='0' WHERE key='fixture:content_v1'",
                    [],
                )
                .unwrap();
        }
        let (code, verifier) = Verifier::generate();
        let ctx = World::new(&p.b, Some(verifier)).ctx();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("ws://{}", listener.local_addr().unwrap());
        let server = inbound::start_with(ctx.clone(), listener, InboundOptions::default());
        Self {
            ctx,
            server,
            address,
            code,
            installation: p.source.installation.clone(),
            scope: wire::PeerScope {
                repo: p.scope.repo.clone(),
                sender: p.a.ctx.device.clone(),
                receiver: p.b.ctx.device.clone(),
                lane: LANE,
            },
        }
    }
    fn dial(&self) -> Conn {
        self.dial_lane(LANE)
    }
    fn dial_lane(&self, lane: LaneSpec) -> Conn {
        session::dial(
            &self.address,
            session::client_hello(
                &self.installation,
                LaneRole::Bulk,
                Some(lane),
                lane.count + 1,
            ),
            &CodeAuth {
                code: self.code.clone(),
            },
            None,
            ConnOptions::for_role(LaneRole::Bulk),
        )
        .unwrap()
        .0
    }
    fn wants_header(&self) -> Value {
        serde_json::json!({"repo":self.scope.repo,"sender":self.scope.sender,"receiver":self.scope.receiver,"lane":LANE,"max_items":256,"max_bytes":16*1024*1024})
    }
    fn wants(&self, conn: &Conn) -> Vec<tkfs::model::protocol::objects::WantItem> {
        let reply = conn
            .call(
                "objects.wants",
                self.wants_header(),
                vec![],
                Priority::Control,
                Duration::from_secs(5),
            )
            .unwrap();
        let reply: tkfs::model::protocol::objects::ObjectsWantsReply = if reply.body.is_empty() {
            serde_json::from_value(reply.header).unwrap()
        } else {
            serde_json::from_slice(&reply.body).unwrap()
        };
        reply.items
    }
    fn push(&self, conn: &Conn, item: tkfs::model::protocol::objects::WantItem, bytes: &[u8]) {
        self.push_lane(conn, item, bytes, LANE);
    }
    fn push_lane(
        &self,
        conn: &Conn,
        item: tkfs::model::protocol::objects::WantItem,
        bytes: &[u8],
        lane: LaneSpec,
    ) {
        let reply = self.send_lane(conn, item, bytes, lane).unwrap();
        assert_eq!(reply.results.len(), 1);
        assert_eq!(
            reply.results[0].outcome,
            tkfs::model::protocol::objects::ObjectOutcome::Completed
        );
    }
    fn send_lane(
        &self,
        conn: &Conn,
        item: tkfs::model::protocol::objects::WantItem,
        bytes: &[u8],
        lane: LaneSpec,
    ) -> Result<tkfs::model::protocol::objects::ObjectsResult, CallError> {
        use tkfs::model::protocol::{
            frame::ChunkHeader,
            objects::{ObjectsPush, ObjectsResult},
        };
        let request = ObjectsPush {
            repo: self.scope.repo.clone(),
            sender: self.scope.sender.clone(),
            receiver: self.scope.receiver.clone(),
            lane,
            items: vec![item.clone()],
        };
        let mut upload = conn.upload(
            "objects.push",
            serde_json::to_value(request).unwrap(),
            Priority::Control,
            Duration::from_secs(5),
        )?;
        for (index, chunk) in bytes[item.offset as usize..].chunks(65536).enumerate() {
            let offset = item.offset + (index * 65536) as u64;
            upload.chunk(
                ChunkHeader {
                    id: 0,
                    hash: Some(item.hash.clone()),
                    offset,
                    total: bytes.len() as u64,
                    last: offset + chunk.len() as u64 == bytes.len() as u64,
                },
                chunk,
            )?;
        }
        let reply = upload.finish()?;
        let reply: ObjectsResult = if reply.body.is_empty() {
            serde_json::from_value(reply.header)
                .map_err(|e| CallError::Remote(ErrorWire::coded("INVALID_REPLY", e)))?
        } else {
            serde_json::from_slice(&reply.body)
                .map_err(|e| CallError::Remote(ErrorWire::coded("INVALID_REPLY", e)))?
        };
        Ok(reply)
    }
}
impl Drop for LegacyPushReceiver {
    fn drop(&mut self) {
        self.ctx.shutdown.cancel();
        self.server.shutdown(Duration::from_secs(2));
    }
}

#[test]
fn native_closed_offer_cleanup_is_bounded_during_connection_churn_and_drains_queued_work() {
    use tkfs::{
        orchestrator::inbound_objects,
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let _reset = Reset;
    let bytes = vec![117; 150_000];
    let p = Pair::new(&bytes);
    let receiver = LegacyPushReceiver::new(&p);
    let hash = p.root.digest.to_string();
    let mut config = FaultConfig::new("worker.rpc.objects.release", FaultAction::Hang);
    config.ms = Some(1500);
    config.count = Some(2);
    fault::configure(Some(config));
    // Occupy both fixed release workers in actual native loopback handlers.
    for expected in 1..=2 {
        let conn = receiver.dial();
        assert!(receiver.wants(&conn).iter().any(|item| item.hash == hash));
        conn.close();
        let until = Instant::now() + Duration::from_secs(1);
        while fault::fired() < expected && Instant::now() < until {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(fault::fired(), expected);
    }
    let began = Instant::now();
    let mut admission_peak = Duration::ZERO;
    for _ in 0..20 {
        let conn = receiver.dial();
        let started = Instant::now();
        assert!(receiver.wants(&conn).iter().any(|item| item.hash == hash));
        admission_peak = admission_peak.max(started.elapsed());
        conn.close();
        let view = inbound_objects::offer_cleanup_view();
        assert_eq!(view.worker_threads, inbound_objects::OFFER_RELEASE_WORKERS);
        assert!(view.active <= 2 && view.peak <= 2, "{view:?}");
    }
    let churn = began.elapsed();
    let until = Instant::now() + Duration::from_millis(250);
    while receiver.server.stats().sessions != 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(1));
    }
    let saturated = inbound_objects::offer_cleanup_view();
    assert_eq!(
        saturated.active, 2,
        "release workers were not still blocked: {saturated:?}"
    );
    assert!(
        saturated.closed_pending > 0,
        "pending close was lost: {saturated:?}"
    );
    assert!(
        p.b.ctx
            .inflight_objects
            .lock()
            .unwrap()
            .leases
            .contains_key(&hash)
    );
    // Manual sweeps share the same bound; they cannot add another RPC.
    assert_eq!(inbound_objects::reap_offers(Instant::now()), 0);
    assert_eq!(inbound_objects::offer_cleanup_view().active, 2);
    let drain = Instant::now();
    let until = Instant::now() + Duration::from_secs(3);
    while (p
        .b
        .ctx
        .inflight_objects
        .lock()
        .unwrap()
        .leases
        .contains_key(&hash)
        || inbound_objects::offer_cleanup_view().active != 0
        || inbound_objects::offer_cleanup_view().closed_pending != 0)
        && Instant::now() < until
    {
        thread::sleep(Duration::from_millis(2));
    }
    let drained = inbound_objects::offer_cleanup_view();
    assert_eq!(drained.active, 0, "{drained:?}");
    assert_eq!(drained.closed_pending, 0, "{drained:?}");
    assert!(
        !p.b.ctx
            .inflight_objects
            .lock()
            .unwrap()
            .leases
            .contains_key(&hash)
    );
    assert!(runtime::objects_rpc::source_retry_at(&p.b.ctx, &p.a.ctx.device, &hash).is_none());
    fault::configure(None);
    let conn = receiver.dial();
    let item = receiver
        .wants(&conn)
        .into_iter()
        .find(|item| item.hash == hash)
        .unwrap();
    receiver.push(&conn, item, &bytes);
    conn.close();
    let mut out = vec![0; bytes.len()];
    p.bp.read_range(&p.bp.pin(&p.root).unwrap(), 0, &mut out, &context())
        .unwrap();
    assert_eq!(out, bytes);
    assert!(
        churn < Duration::from_secs(1),
        "close churn waited {churn:?}"
    );
    assert!(admission_peak < Duration::from_millis(250));
    println!(
        "WAVE6_CLEANUP {}",
        serde_json::json!({"scenario":"native_close_churn",
        "connections":22,"stalled_release_ms":1500,"stalled_releases":2,
        "worker_threads":drained.worker_threads,"peak_active":drained.peak,
        "queued_while_saturated":saturated.closed_pending,"churn_ms":churn.as_secs_f64()*1000.0,
        "admission_peak_ms":admission_peak.as_secs_f64()*1000.0,"admission_bound_ms":250,
        "drain_ms":drain.elapsed().as_secs_f64()*1000.0,"typed_activation":false})
    );
}

#[test]
fn native_closed_session_cannot_publish_an_offer_after_its_close_cleanup_ran() {
    use tkfs::util::fault::{self, FaultAction, FaultConfig};
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let _reset = Reset;
    let bytes = vec![119; 150_000];
    let p = Pair::new(&bytes);
    let receiver = LegacyPushReceiver::new(&p);
    let conn = receiver.dial();
    let hash = p.root.digest.to_string();
    let mut config = FaultConfig::new("inbound.objects.before_offer_publish", FaultAction::Hang);
    config.ms = Some(1500);
    config.count = Some(1);
    fault::configure(Some(config));
    let (request, payload) = (conn.clone(), receiver.wants_header());
    let running = thread::spawn(move || {
        request.call(
            "objects.wants",
            payload,
            vec![],
            Priority::Control,
            Duration::from_secs(3),
        )
    });
    let until = Instant::now() + Duration::from_secs(1);
    while fault::fired() == 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(fault::fired(), 1);
    assert!(
        p.b.ctx
            .inflight_objects
            .lock()
            .unwrap()
            .leases
            .contains_key(&hash)
    );
    let began = Instant::now();
    conn.close();
    assert!(running.join().unwrap().is_err());
    let until = Instant::now() + Duration::from_secs(1);
    while receiver.server.stats().sessions != 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(receiver.server.stats().sessions, 0);
    // No replacement session exists to supersede the old frame for us.
    // The close itself must prevent the late publication and release its lease.
    let until = Instant::now() + Duration::from_secs(3);
    while (receiver.server.stats().exchanges != 0
        || p.b
            .ctx
            .inflight_objects
            .lock()
            .unwrap()
            .leases
            .contains_key(&hash))
        && Instant::now() < until
    {
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(receiver.server.stats().exchanges, 0);
    assert!(
        !p.b.ctx
            .inflight_objects
            .lock()
            .unwrap()
            .leases
            .contains_key(&hash),
        "late closed offer kept its lease"
    );
    assert!(began.elapsed() < tkfs::orchestrator::inbound_objects::OFFER_GRACE);
    fault::configure(None);
    let new = receiver.dial();
    let item = receiver
        .wants(&new)
        .into_iter()
        .find(|item| item.hash == hash)
        .unwrap();
    receiver.push(&new, item, &bytes);
    new.close();
    println!(
        "WAVE6_CLEANUP {}",
        serde_json::json!({"scenario":"late_offer_after_close",
        "publication_stall_ms":1500,"replacement_before_cleanup":false,
        "close_to_drain_ms":began.elapsed().as_secs_f64()*1000.0,"typed_activation":false})
    );
}

#[test]
fn native_closed_push_drains_its_admitted_prefix_and_releases_its_exact_lease() {
    use tkfs::{
        model::protocol::{frame::ChunkHeader, objects::ObjectsPush},
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let _reset = Reset;
    let bytes = vec![121; 150_000];
    let p = Pair::new(&bytes);
    let receiver = LegacyPushReceiver::new(&p);
    let conn = receiver.dial();
    let hash = p.root.digest.to_string();
    let item = receiver
        .wants(&conn)
        .into_iter()
        .find(|item| item.hash == hash)
        .unwrap();
    let mut config = FaultConfig::new("inbound.objects.push.after_admit", FaultAction::Hang);
    config.ms = Some(300);
    config.count = Some(1);
    fault::configure(Some(config));
    let request = ObjectsPush {
        repo: receiver.scope.repo.clone(),
        sender: receiver.scope.sender.clone(),
        receiver: receiver.scope.receiver.clone(),
        lane: LANE,
        items: vec![item],
    };
    let mut upload = conn
        .upload(
            "objects.push",
            serde_json::to_value(request).unwrap(),
            Priority::Control,
            Duration::from_secs(3),
        )
        .unwrap();
    upload
        .chunk(
            ChunkHeader {
                id: 0,
                hash: Some(hash.clone()),
                offset: 0,
                total: bytes.len() as u64,
                last: false,
            },
            &bytes[..65536],
        )
        .unwrap();
    let until = Instant::now() + Duration::from_secs(1);
    while fault::fired() == 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(fault::fired(), 1);
    let probe = Store::open_secondary(&p.b.ctx.root, &p.b.ctx.repo, &p.b.ctx.device).unwrap();
    assert!(
        probe.spool_state(&hash).unwrap().is_none(),
        "prefix must still be buffered at the close"
    );
    conn.close();
    drop(upload);
    let until = Instant::now() + Duration::from_secs(2);
    while (receiver.server.stats().sessions != 0 || receiver.server.stats().exchanges != 0)
        && Instant::now() < until
    {
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(receiver.server.stats().sessions, 0);
    assert_eq!(receiver.server.stats().exchanges, 0);
    assert_eq!(
        probe.spool_state(&hash).unwrap().map(|state| state.durable),
        Some(65536)
    );
    assert!(
        !p.b.ctx
            .inflight_objects
            .lock()
            .unwrap()
            .leases
            .contains_key(&hash)
    );
    assert!(runtime::objects_rpc::source_retry_at(&p.b.ctx, &p.a.ctx.device, &hash).is_none());
    fault::configure(None);
    let new = receiver.dial();
    let item = receiver
        .wants(&new)
        .into_iter()
        .find(|item| item.hash == hash)
        .unwrap();
    assert_eq!(item.offset, 65536);
    receiver.push(&new, item, &bytes);
    new.close();
    let mut out = vec![0; bytes.len()];
    p.bp.read_range(&p.bp.pin(&p.root).unwrap(), 0, &mut out, &context())
        .unwrap();
    assert_eq!(out, bytes);
    println!(
        "WAVE6_CLEANUP {}",
        serde_json::json!({"scenario":"closed_push_prefix",
        "admitted_prefix_bytes":65536,"durable_prefix_bytes":65536,"verified_file_bytes":bytes.len(),
        "admit_stall_ms":300,"typed_activation":false})
    );
}

#[test]
fn native_legacy_push_reconnect_admits_before_old_release_and_late_cleanup_cannot_release_it() {
    use tkfs::util::fault::{self, FaultAction, FaultConfig};
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let bytes = vec![101; 150_000];
    let p = Pair::new(&bytes);
    let receiver = LegacyPushReceiver::new(&p);
    let old = receiver.dial();
    let hash = p.root.digest.to_string();
    assert!(receiver.wants(&old).iter().any(|item| item.hash == hash));
    let old_id = p.b.ctx.inflight_objects.lock().unwrap().leases[&hash]
        .session
        .clone();
    let mut config = FaultConfig::new("worker.rpc.objects.release", FaultAction::Hang);
    config.ms = Some(2000);
    config.count = Some(1);
    fault::configure(Some(config));
    let _reset = Reset;
    old.close();
    let until = Instant::now() + Duration::from_secs(3);
    while fault::fired() == 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(fault::fired(), 1, "actual old release must be stalled");
    let new = receiver.dial();
    let started = Instant::now();
    let item = receiver
        .wants(&new)
        .into_iter()
        .find(|item| item.hash == hash)
        .unwrap();
    let admission = started.elapsed();
    let lease = p.b.ctx.inflight_objects.lock().unwrap().leases[&hash].clone();
    let old_owner = tkfs::model::lease::parse(&old_id).unwrap().unwrap();
    let new_owner = tkfs::model::lease::parse(&lease.session).unwrap().unwrap();
    assert!(new_owner.generation > old_owner.generation);
    assert_eq!(old_owner.layout, new_owner.layout);
    thread::sleep(Duration::from_millis(2150));
    let after = p.b.ctx.inflight_objects.lock().unwrap().leases[&hash].clone();
    let no_backoff =
        runtime::objects_rpc::source_retry_at(&p.b.ctx, &p.a.ctx.device, &hash).is_none();
    receiver.push(&new, item, &bytes);
    let mut out = vec![0; bytes.len()];
    p.bp.read_range(&p.bp.pin(&p.root).unwrap(), 0, &mut out, &context())
        .unwrap();
    new.close();
    assert!(
        admission < Duration::from_millis(250),
        "replacement waited {admission:?}"
    );
    assert_eq!(after, lease);
    assert!(no_backoff);
    assert_eq!(out, bytes);
    println!(
        "WAVE6_PUSH {}",
        serde_json::json!({"scenario":"native_reconnect_behind_old_release","admission_ms":admission.as_secs_f64()*1000.0,"bound_ms":250,"old_release_stall_ms":2000,"typed_activation":false})
    );
}

#[test]
fn native_late_push_offer_reply_cannot_replace_a_newer_offer_on_the_same_session() {
    use tkfs::util::fault::{self, FaultAction, FaultConfig};
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let bytes = vec![102; 150_000];
    let p = Pair::new(&bytes);
    let receiver = LegacyPushReceiver::new(&p);
    let conn = receiver.dial();
    let mut config = FaultConfig::new("inbound.objects.before_offer_publish", FaultAction::Hang);
    config.ms = Some(2000);
    config.count = Some(1);
    fault::configure(Some(config));
    let _reset = Reset;
    let old_conn = conn.clone();
    let header = receiver.wants_header();
    let old = thread::spawn(move || {
        old_conn.call(
            "objects.wants",
            header,
            vec![],
            Priority::Control,
            Duration::from_secs(5),
        )
    });
    let until = Instant::now() + Duration::from_secs(3);
    while fault::fired() == 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(fault::fired(), 1);
    let started = Instant::now();
    let hash = p.root.digest.to_string();
    let item = receiver
        .wants(&conn)
        .into_iter()
        .find(|item| item.hash == hash)
        .unwrap();
    let admission = started.elapsed();
    let owner = p.b.ctx.inflight_objects.lock().unwrap().leases[&hash]
        .session
        .clone();
    assert_eq!(
        old.join().unwrap().unwrap_err().code(),
        "IN_FLIGHT_ELSEWHERE"
    );
    assert_eq!(
        p.b.ctx.inflight_objects.lock().unwrap().leases[&hash].session,
        owner
    );
    assert!(runtime::objects_rpc::source_retry_at(&p.b.ctx, &p.a.ctx.device, &hash).is_none());
    receiver.push(&conn, item, &bytes);
    let mut out = vec![0; bytes.len()];
    p.bp.read_range(&p.bp.pin(&p.root).unwrap(), 0, &mut out, &context())
        .unwrap();
    conn.close();
    assert!(
        admission < Duration::from_millis(250),
        "new offer waited {admission:?}"
    );
    assert_eq!(out, bytes);
    println!(
        "WAVE6_PUSH {}",
        serde_json::json!({"scenario":"native_late_offer_publication","admission_ms":admission.as_secs_f64()*1000.0,"bound_ms":250,"old_offer_stall_ms":2000,"typed_activation":false})
    );
}

#[test]
fn native_inbound_push_pool_resize_fences_an_old_still_open_session() {
    use tkfs::model::protocol::objects::ObjectsWantsReply;
    let bytes = vec![103; 150_000];
    let p = Pair::new(&bytes);
    let receiver = LegacyPushReceiver::new(&p);
    let old = receiver.dial();
    let hash = p.root.digest.to_string();
    assert!(receiver.wants(&old).iter().any(|item| item.hash == hash));
    let old_id = p.b.ctx.inflight_objects.lock().unwrap().leases[&hash]
        .session
        .clone();
    let lane = (0..2)
        .map(|index| LaneSpec::new(index, 2))
        .find(|lane| lane.owns(&hash))
        .unwrap();
    let new = receiver.dial_lane(lane);
    let mut header = receiver.wants_header();
    header["lane"] = serde_json::to_value(lane).unwrap();
    let started = Instant::now();
    let reply = new
        .call(
            "objects.wants",
            header,
            vec![],
            Priority::Control,
            Duration::from_secs(5),
        )
        .unwrap();
    let offered: ObjectsWantsReply = serde_json::from_value(reply.header).unwrap();
    let item = offered
        .items
        .into_iter()
        .find(|item| item.hash == hash)
        .unwrap();
    let admission = started.elapsed();
    let current = p.b.ctx.inflight_objects.lock().unwrap().leases[&hash].clone();
    assert!(
        tkfs::model::lease::parse(&current.session)
            .unwrap()
            .unwrap()
            .layout
            > tkfs::model::lease::parse(&old_id).unwrap().unwrap().layout
    );
    assert_eq!(
        old.call(
            "objects.wants",
            receiver.wants_header(),
            vec![],
            Priority::Control,
            Duration::from_secs(5)
        )
        .unwrap_err()
        .code(),
        "IN_FLIGHT_ELSEWHERE"
    );
    old.close();
    thread::sleep(Duration::from_millis(100));
    assert_eq!(
        p.b.ctx.inflight_objects.lock().unwrap().leases[&hash],
        current
    );
    receiver.push_lane(&new, item, &bytes, lane);
    let mut out = vec![0; bytes.len()];
    p.bp.read_range(&p.bp.pin(&p.root).unwrap(), 0, &mut out, &context())
        .unwrap();
    new.close();
    assert_eq!(out, bytes);
    assert!(
        admission < Duration::from_millis(250),
        "push pool resize waited {admission:?}"
    );
    println!(
        "WAVE6_PUSH {}",
        serde_json::json!({"scenario":"native_push_pool_resize","admission_ms":admission.as_secs_f64()*1000.0,"bound_ms":250,"old_lane_count":1,"new_lane_count":2,"typed_activation":false})
    );
}

#[test]
fn native_push_backoff_is_fenced_at_mutation_after_a_superseding_transfer_completes() {
    use tkfs::{
        model::protocol::objects::ObjectOutcome,
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let bytes = vec![104; 150_000];
    let wrong = vec![105; bytes.len()];
    let p = Pair::new(&bytes);
    let receiver = LegacyPushReceiver::new(&p);
    let old = receiver.dial();
    let hash = p.root.digest.to_string();
    let item = receiver
        .wants(&old)
        .into_iter()
        .find(|item| item.hash == hash)
        .unwrap();
    let mut config = FaultConfig::new("objects.write.before_source_backoff", FaultAction::Hang);
    config.ms = Some(2000);
    config.count = Some(1);
    fault::configure(Some(config));
    let _reset = Reset;
    thread::scope(|scope| {
        let pending = scope.spawn(|| receiver.send_lane(&old, item, &wrong, LANE));
        let until = Instant::now() + Duration::from_secs(3);
        while fault::fired() == 0 && Instant::now() < until {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            fault::fired(),
            1,
            "old source verdict must already be collected before its projection stalls"
        );
        let new = receiver.dial();
        let started = Instant::now();
        let item = receiver
            .wants(&new)
            .into_iter()
            .find(|item| item.hash == hash)
            .unwrap();
        receiver.push(&new, item, &bytes);
        let availability = started.elapsed();
        let previous = pending.join().unwrap();
        match previous {
            Ok(reply) => assert_eq!(reply.results[0].outcome, ObjectOutcome::CorruptAtSource),
            Err(error) => assert_eq!(error.code(), "IN_FLIGHT_ELSEWHERE"),
        }
        let mut out = vec![0; bytes.len()];
        p.bp.read_range(&p.bp.pin(&p.root).unwrap(), 0, &mut out, &context())
            .unwrap();
        assert!(
            runtime::objects_rpc::source_retry_at(&p.b.ctx, &p.a.ctx.device, &hash).is_none(),
            "old verdict applied a backoff to the completed replacement"
        );
        assert_eq!(out, bytes);
        assert!(
            availability < Duration::from_secs(1),
            "replacement waited {availability:?}"
        );
        new.close();
        println!(
            "WAVE6_PUSH {}",
            serde_json::json!({"scenario":"native_backoff_after_replacement","availability_ms":availability.as_secs_f64()*1000.0,"bound_ms":1000,"old_projection_stall_ms":2000,"typed_activation":false})
        );
    });
    old.close();
}

#[test]
fn native_local_failure_cannot_recreate_sql_retry_after_a_replacement_push_completes() {
    use tkfs::util::fault::{self, FaultAction, FaultConfig};
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let _reset = Reset;
    let bytes = vec![141; 150_000];
    let p = Pair::new(&bytes);
    let receiver = LegacyPushReceiver::new(&p);
    let old = receiver.dial();
    let hash = p.root.digest.to_string();
    let item = receiver
        .wants(&old)
        .into_iter()
        .find(|item| item.hash == hash)
        .unwrap();
    let spool = p.b.ctx.root.join("incoming-objects").join(&hash);
    std::fs::create_dir_all(&spool).unwrap();
    let mut config = FaultConfig::new("objects.write.before_local_backoff", FaultAction::Hang);
    config.ms = Some(2000);
    config.count = Some(1);
    fault::configure(Some(config));
    thread::scope(|scope| {
        let pending = scope.spawn(|| receiver.send_lane(&old, item, &bytes, LANE));
        let until = Instant::now() + Duration::from_secs(3);
        while fault::fired() == 0 && Instant::now() < until {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            fault::fired(),
            1,
            "actual native local failure did not reach SQL projection"
        );
        std::fs::remove_dir(&spool).unwrap();
        let new = receiver.dial();
        let began = Instant::now();
        let item = receiver
            .wants(&new)
            .into_iter()
            .find(|item| item.hash == hash)
            .unwrap();
        receiver.push(&new, item, &bytes);
        let availability = began.elapsed();
        let _ = pending.join().unwrap();
        let probe = p.b.ctx.sync_stores.checkout().unwrap();
        assert!(probe.object_backoff(&hash).unwrap().is_none());
        let rows: u64 = probe
            .db
            .query_row(
                "SELECT count(*) FROM object_error_state WHERE hash=?",
                [&hash],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            rows, 0,
            "old native failure recreated retry after good install"
        );
        let mut out = vec![0; bytes.len()];
        p.bp.read_range(&p.bp.pin(&p.root).unwrap(), 0, &mut out, &context())
            .unwrap();
        assert_eq!(out, bytes);
        assert!(availability < Duration::from_secs(1));
        new.close();
        println!(
            "WAVE6_EFFECT {}",
            serde_json::json!({"scenario":"native-local-retry-after-replacement",
            "availability_ms":availability.as_secs_f64()*1000.0,"bound_ms":1000,"projection_stall_ms":2000,
            "verified_bytes":bytes.len(),"stale_sql_retry":false,"typed_activation":false})
        );
    });
    old.close();
}

#[test]
fn bounded_frames_hydrate_through_real_peer_and_workers_without_semantic_engine_locks() {
    let bytes: Vec<u8> = (0..150_000)
        .map(|i| ((i / 65536) * 71 + i % 251) as u8)
        .collect();
    let p = Pair::new(&bytes);
    let client = p.b.client();
    let owner = Uuid::new_v4();
    let mut wrong = p.route.clone();
    wrong.peer = Uuid::new_v4().to_string();
    assert_eq!(
        pull_one(
            &p.dest,
            &p.conn,
            &client,
            &wrong,
            &p.scope,
            owner,
            wire::PrepareMode::Background
        )
        .unwrap_err()
        .code(),
        "PEER_INSTALLATION_CHANGED"
    );
    assert_eq!(
        pull_one(
            &p.dest,
            &p.conn,
            &client,
            &p.route,
            &p.scope,
            owner,
            wire::PrepareMode::Mixed
        )
        .unwrap_err()
        .code(),
        "INVALID_REQUEST"
    );
    let mut installed = 0;
    let _source_engine = p.a.ctx.engine.lock().unwrap();
    let _dest_engine = p.b.ctx.engine.lock().unwrap();
    for _ in 0..16 {
        for mode in [wire::PrepareMode::Structural, wire::PrepareMode::Background] {
            installed += usize::from(
                pull_one(&p.dest, &p.conn, &client, &p.route, &p.scope, owner, mode).unwrap(),
            );
        }
        let mut out = vec![0; bytes.len()];
        if p.bp
            .read_range(&p.bp.pin(&p.root).unwrap(), 0, &mut out, &context())
            .is_ok()
        {
            assert_eq!(out, bytes);
            break;
        }
    }
    assert_eq!(installed, 4, "root plus three data chunks");
    let mut out = vec![0; bytes.len()];
    p.bp.read_range(&p.bp.pin(&p.root).unwrap(), 0, &mut out, &context())
        .unwrap();
    assert_eq!(out, bytes);
    assert_eq!(
        content_transport::daemon(&p.source)
            .unwrap()
            .pending_frames(),
        0
    );
    assert_eq!(
        content_transport::daemon(&p.dest)
            .unwrap()
            .scheduler
            .usage()
            .0,
        0
    );
    assert_eq!(
        runtime::content_rpc::state(&p.b.ctx)
            .unwrap()
            .scheduler
            .usage()
            .0,
        0
    );
}

#[test]
fn capability_and_membership_are_checked_before_any_private_bytes_are_sent() {
    let p = Pair::new(&vec![1; 150_000]);
    assert_eq!(
        p.read(&p.conn, p.position()).unwrap_err().code(),
        "PEER_NEEDS_UPGRADE"
    );
    p.caps(&p.conn);
    let secret = vec![7; 65536];
    let key = ObjectKey {
        kind: ObjectKind::Bytes,
        digest: Digest::bytes(&secret),
    };
    p.ap.install(key, &secret, &context()).unwrap();
    let forged = wire::Position {
        reference: p.reference.clone(),
        edge: tkfs::content::ContentEdge {
            key,
            offset: 0,
            len: 65536,
            level: None,
        },
    };
    assert_eq!(
        p.read(&p.conn, forged).unwrap_err().code(),
        "UNAUTHORIZED_OBJECT"
    );
    let mut bare = p.position();
    bare.reference = key.digest.to_string();
    assert_eq!(
        p.read(&p.conn, bare).unwrap_err().code(),
        "UNAUTHORIZED_OBJECT"
    );
    assert_eq!(
        content_transport::daemon(&p.source)
            .unwrap()
            .pending_frames(),
        0
    );
    assert_eq!(
        content_transport::daemon(&p.source)
            .unwrap()
            .scheduler
            .usage()
            .0,
        0
    );
}

#[test]
fn ack_ownership_and_session_close_preserve_newer_frame_credit() {
    let p = Pair::new(b"frame bytes");
    p.caps(&p.conn);
    let old: wire::Frame =
        serde_json::from_value(p.read(&p.conn, p.position()).unwrap().header).unwrap();
    let state = content_transport::daemon(&p.source).unwrap();
    assert_eq!(state.pending_frames(), 1);
    p.ack(&p.conn, old.token.unwrap()).unwrap();
    let newer: wire::Frame =
        serde_json::from_value(p.read(&p.conn, p.position()).unwrap().header).unwrap();
    p.ack(&p.conn, old.token.unwrap()).unwrap();
    assert_eq!(state.pending_frames(), 1);
    let hello = session::client_hello(&p.dest.installation, LaneRole::Bulk, Some(LANE), 2);
    let (other, _) = session::dial(
        &p.address,
        hello,
        &CodeAuth {
            code: p.code.clone(),
        },
        None,
        ConnOptions::for_role(LaneRole::Bulk),
    )
    .unwrap();
    assert_eq!(
        p.ack(&other, newer.token.unwrap()).unwrap_err().code(),
        "UNAUTHORIZED_OBJECT"
    );
    other.close();
    p.conn.close();
    let until = Instant::now() + Duration::from_secs(1);
    while state.pending_frames() != 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(state.pending_frames(), 0);
    assert_eq!(state.scheduler.usage().0, 0);
}

#[test]
fn stale_worker_offer_cannot_release_or_complete_a_new_owner() {
    let p = Pair::new(b"frame bytes");
    let client = p.b.client();
    let owner = Uuid::new_v4();
    let prepare = || {
        worker_call::<wire::Prepared>(
            &client,
            wire::PREPARE,
            &wire::Prepare {
                route: p.route.clone(),
                owner,
                lane: LANE,
                mode: wire::PrepareMode::Mixed,
            },
            vec![],
        )
        .unwrap()
        .0
        .offer
        .unwrap()
    };
    let old = prepare();
    let finish = |token| wire::Finish {
        route: p.route.clone(),
        owner,
        token,
    };
    worker_call::<Value>(
        &client,
        wire::RELEASE,
        &wire::Release {
            transfer: finish(old.token),
            failed_source: false,
        },
        vec![],
    )
    .unwrap();
    let newer = prepare();
    assert_ne!(old.token, newer.token);
    worker_call::<Value>(
        &client,
        wire::RELEASE,
        &wire::Release {
            transfer: finish(old.token),
            failed_source: false,
        },
        vec![],
    )
    .unwrap();
    assert_eq!(
        runtime::content_rpc::state(&p.b.ctx)
            .unwrap()
            .scheduler
            .usage()
            .0,
        1
    );
    assert_eq!(
        worker_call::<Value>(
            &client,
            wire::COMPLETE,
            &finish(old.token),
            b"frame bytes".to_vec()
        )
        .unwrap_err()
        .code(),
        "STALE_ADMISSION_GENERATION"
    );
    let mut wrong = finish(newer.token);
    wrong.owner = Uuid::new_v4();
    assert_eq!(
        worker_call::<Value>(&client, wire::COMPLETE, &wrong, b"frame bytes".to_vec())
            .unwrap_err()
            .code(),
        "UNAUTHORIZED_OBJECT"
    );
    worker_call::<Value>(
        &client,
        wire::COMPLETE,
        &finish(newer.token),
        b"frame bytes".to_vec(),
    )
    .unwrap();
    assert_eq!(
        runtime::content_rpc::state(&p.b.ctx)
            .unwrap()
            .scheduler
            .usage()
            .0,
        0
    );
}

#[test]
fn object_worker_writer_contention_returns_promptly_and_preserves_metadata_progress() {
    let p = Pair::new(b"frame bytes");
    let client = p.b.client();
    let _ = runtime::content_rpc::state(&p.b.ctx).unwrap();
    let store = Store::open_secondary(&p.b.ctx.root, &p.b.ctx.repo, &p.b.ctx.device).unwrap();
    store.db.execute_batch("BEGIN IMMEDIATE").unwrap();
    let started = Instant::now();
    let err = worker_call::<wire::Prepared>(
        &client,
        wire::PREPARE,
        &wire::Prepare {
            route: p.route.clone(),
            owner: Uuid::new_v4(),
            lane: LANE,
            mode: wire::PrepareMode::Mixed,
        },
        vec![],
    )
    .unwrap_err();
    assert_eq!(err.code(), "SQLITE_BUSY");
    assert!(started.elapsed() < Duration::from_millis(250));
    store.db.execute_batch("ROLLBACK").unwrap();
    assert!(
        worker_call::<wire::Prepared>(
            &client,
            wire::PREPARE,
            &wire::Prepare {
                route: p.route.clone(),
                owner: Uuid::new_v4(),
                lane: LANE,
                mode: wire::PrepareMode::Mixed
            },
            vec![]
        )
        .unwrap()
        .0
        .offer
        .is_some()
    );
}

#[test]
fn outbound_background_cannot_take_foreground_or_inbound_service_capacity() {
    use tkfs::model::workspace::DemandPriority as P;
    let p = Pair::new(b"frame bytes");
    let state = content_transport::daemon(&p.dest).unwrap();
    let one = state.outgoing("one", P::Background).unwrap();
    let two = state.outgoing("two", P::Background).unwrap();
    assert!(state.outgoing("three", P::Background).is_none());
    let foreground = state.outgoing("three", P::Foreground).unwrap();
    let structural = state.outgoing("four", P::Structural).unwrap();
    assert!(state.outgoing("five", P::Foreground).is_none());
    drop((one, two, foreground, structural));
    assert!(state.outgoing("one", P::Foreground).is_some());
}

#[test]
fn unacknowledged_frame_expiry_closes_the_session_before_returning_byte_credit() {
    let p = Pair::new(b"frame bytes");
    p.caps(&p.conn);
    let _ = p.read(&p.conn, p.position()).unwrap();
    let state = content_transport::daemon(&p.source).unwrap();
    assert_eq!(state.pending_frames(), 1);
    assert!(
        p.conn
            .wait_closed(Duration::from_millis(wire::OFFER_MS + 1500))
            .is_some()
    );
    let until = Instant::now() + Duration::from_secs(1);
    while state.scheduler.usage().0 != 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(state.pending_frames(), 0);
    assert_eq!(state.scheduler.usage().0, 0);
}

#[test]
fn foreground_peer_frame_uses_the_buffer_reserve_while_save_producers_are_stalled() {
    use std::{
        io::{self, Read},
        sync::{Barrier, Condvar},
    };
    struct Slow {
        barrier: Arc<Barrier>,
        release: Arc<(Mutex<bool>, Condvar)>,
        waited: bool,
    }
    impl Read for Slow {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            if !self.waited {
                self.waited = true;
                self.barrier.wait();
                let mut ready = self.release.0.lock().unwrap();
                while !*ready {
                    ready = self.release.1.wait(ready).unwrap();
                }
            }
            out.fill(8);
            Ok(out.len())
        }
    }
    let p = Pair::new(b"frame bytes");
    p.caps(&p.conn);
    let barrier = Arc::new(Barrier::new(3));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let mut jobs = vec![];
    for _ in 0..2 {
        let provider = LocalContent::open(&p.a.ctx.root).unwrap();
        let barrier = barrier.clone();
        let release = release.clone();
        jobs.push(thread::spawn(move || {
            provider
                .import(
                    &mut Slow {
                        barrier,
                        release,
                        waited: false,
                    },
                    65536,
                    &context(),
                )
                .unwrap()
        }));
    }
    barrier.wait();
    let started = Instant::now();
    let frame = p.read(&p.conn, p.position());
    let elapsed = started.elapsed();
    *release.0.lock().unwrap() = true;
    release.1.notify_all();
    for job in jobs {
        job.join().unwrap();
    }
    let frame = frame.unwrap();
    assert_eq!(frame.body, b"frame bytes");
    assert!(elapsed < Duration::from_millis(250));
    let frame: wire::Frame = serde_json::from_value(frame.header).unwrap();
    p.ack(&p.conn, frame.token.unwrap()).unwrap();
}

#[test]
fn stalled_legacy_bulk_handlers_leave_v1_frame_and_ack_service_available() {
    use tkfs::{
        model::protocol::objects::{ObjectsPull, WantItem, WantPriority},
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let p = Pair::new(b"frame bytes");
    p.caps(&p.conn);
    let hello = session::client_hello(&p.dest.installation, LaneRole::Bulk, Some(LANE), 2);
    let (other, _) = session::dial(
        &p.address,
        hello,
        &CodeAuth {
            code: p.code.clone(),
        },
        None,
        ConnOptions::for_role(LaneRole::Bulk),
    )
    .unwrap();
    let mut config = FaultConfig::new("worker.rpc.objects.read", FaultAction::Hang);
    config.ms = Some(2000);
    config.count = Some(8);
    fault::configure(Some(config));
    let _reset = Reset;
    thread::scope(|scope| {
        let mut jobs = vec![];
        for i in 0..8 {
            let conn = if i < 4 { p.conn.clone() } else { other.clone() };
            let header = serde_json::to_value(ObjectsPull {
                repo: p.scope.repo.clone(),
                sender: p.scope.sender.clone(),
                receiver: p.scope.receiver.clone(),
                lane: LANE,
                items: vec![WantItem {
                    hash: p.root.digest.to_string(),
                    size: p.root.total_len,
                    offset: 0,
                    priority: WantPriority::Current,
                }],
                budget_bytes: 65536,
            })
            .unwrap();
            jobs.push(scope.spawn(move || {
                conn.call_streaming(
                    "objects.pull",
                    header,
                    vec![],
                    Priority::Control,
                    Some(Duration::from_secs(5)),
                    Duration::from_secs(5),
                    &mut |_, _| Ok(()),
                )
            }));
        }
        let until = Instant::now() + Duration::from_secs(1);
        while p.server.stats().exchanges < 8 && Instant::now() < until {
            thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(
            p.server.stats().exchanges,
            8,
            "legacy bulk did not occupy both sessions' slots"
        );
        let started = Instant::now();
        let frame = p.read(&p.conn, p.position()).unwrap();
        let frame: wire::Frame = serde_json::from_value(frame.header).unwrap();
        p.ack(&p.conn, frame.token.unwrap()).unwrap();
        assert!(started.elapsed() < Duration::from_millis(250));
        assert!(
            p.server.stats().exchanges >= 8,
            "legacy calls were no longer stalled"
        );
        assert_eq!(
            content_transport::daemon(&p.source)
                .unwrap()
                .pending_frames(),
            0
        );
        for job in jobs {
            job.join().unwrap().unwrap();
        }
    });
    other.close();
}

#[test]
fn bulk_runner_automatically_dispatches_typed_content() {
    automatic_typed_dispatch(false);
}

#[test]
fn bulk_runner_recovers_a_receipt_rejected_after_verified_cas_installation() {
    automatic_typed_dispatch(true);
}

fn automatic_typed_dispatch(force_receipt_contention: bool) {
    use tkfs::content::receipt::ReceiptRecovery;
    use tkfs::model::workspace::ByteRange;
    use tkfs::orchestrator::{content_dispatch, objects};
    use tkfs::util::fault::{self, FaultAction, FaultConfig};
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let _reset = Reset;
    let bytes = vec![4; 150_000];
    let p = Pair::new(&bytes);
    let mut probe = Store::open_secondary(&p.b.ctx.root, &p.b.ctx.repo, &p.b.ctx.device).unwrap();
    probe.db.busy_timeout(Duration::ZERO).unwrap();
    if force_receipt_contention {
        let mut config = FaultConfig::new("content.receipt.installed", FaultAction::Hang);
        config.count = Some(1);
        config.ms = Some(300);
        fault::configure(Some(config));
    }
    let cancel = Cancel::new();
    let token = cancel.clone();
    let (ctx, peer, conn) = (p.dest.clone(), p.peer(&p.dest), p.conn.clone());
    let running = thread::spawn(move || objects::run_bulk_lane(ctx, peer, LANE, conn, token));
    let until = Instant::now() + Duration::from_secs(5);
    let mut out = vec![0; bytes.len()];
    let mut available = false;
    let mut recovered = 0;
    let mut contended = false;
    // Retain bounded diagnostics before another successful class clears last_error.
    let mut observed_errors = Vec::new();
    let mut observe_errors = |view: &content_dispatch::DispatchView| {
        for error in view
            .errors
            .iter()
            .filter(|(_, wire)| wire.class.counts_as_failure())
        {
            let detail = format!("{error:?}");
            if observed_errors.len() < 16 && !observed_errors.contains(&detail) {
                observed_errors.push(detail);
            }
        }
    };
    if force_receipt_contention {
        while fault::fired() == 0 && Instant::now() < until {
            if let Some(view) = content_dispatch::view(&p.dest) {
                observe_errors(&view);
            }
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(fault::fired(), 1);
        // The first manifest is installed, but its receipt index has not been
        // written. Hold the real WAL writer until that COMPLETE fails BUSY.
        loop {
            match probe.db.execute_batch("BEGIN IMMEDIATE") {
                Ok(()) => break,
                Err(error) => {
                    let error = anyhow::Error::new(error);
                    assert!(
                        tkfs::model::errors::code_of(&error) == "SQLITE_BUSY"
                            && Instant::now() < until,
                        "receipt contention setup: {error:#}"
                    );
                    thread::sleep(Duration::from_millis(1));
                }
            }
        }
        while Instant::now() < until {
            let view = content_dispatch::view(&p.dest).unwrap();
            observe_errors(&view);
            if view.running == 0 && view.retried[1] > 0 {
                contended = true;
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        probe.db.execute_batch("ROLLBACK").unwrap();
        fault::configure(None);
    }
    let range = ByteRange {
        offset: 0,
        len: bytes.len() as u64,
    };
    let mut durable = false;
    while Instant::now() < until {
        if !available
            && p.bp
                .read_range(&p.bp.pin(&p.root).unwrap(), 0, &mut out, &context())
                .is_ok()
        {
            available = true;
        }
        let Some(view) = content_dispatch::view(&p.dest) else {
            thread::sleep(Duration::from_millis(5));
            continue;
        };
        observe_errors(&view);
        // This in-process RPC fixture does not spawn worker maintenance.
        // Drive its same bounded recovery component while no frame is running.
        // CAS publication can precede a BUSY receipt-index write; callback
        // success counters alone therefore cannot prove logical completion.
        if view.running == 0
            && view.queued == 0
            && let Some(mut recovery) = ReceiptRecovery::open(&p.bp).unwrap()
        {
            match recovery.step(
                &mut probe,
                &p.bp,
                16,
                &ReadContext {
                    deadline: until,
                    cancel: Cancel::new(),
                },
            ) {
                Ok((count, _)) => recovered += count,
                Err(error) if tkfs::model::errors::code_of(&error) == "SQLITE_BUSY" => {}
                Err(error) => panic!("receipt recovery: {error:#}"),
            }
        }
        let all_receipts:bool=probe.db.query_row("SELECT count(*)>=3 AND NOT EXISTS(
            SELECT 1 FROM content_object_jobs j LEFT JOIN content_public_receipts r
            ON j.kind=r.kind AND j.digest=r.digest
            WHERE j.verified=0 OR r.digest IS NULL OR (j.kind=1 AND j.expanded=0)) FROM content_object_jobs",[],|row|row.get(0)).unwrap();
        durable = all_receipts && probe.content_range_indexed(&p.reference, range).unwrap();
        if available && durable && view.admitted[1] >= 1 && view.admitted[2] >= 1 {
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    let view = content_dispatch::view(&p.dest).unwrap();
    cancel.cancel();
    running.join().unwrap();
    assert!(available, "automatic content did not hydrate: {view:?}");
    assert_eq!(out, bytes);
    assert!(
        durable,
        "automatic content lacks verified receipt/index evidence: {view:?}"
    );
    assert!(view.admitted[1] >= 1 && view.admitted[2] >= 1, "{view:?}");
    if force_receipt_contention {
        assert!(contended);
        assert!(recovered >= 1);
        assert!(
            view.retried[1] >= 1,
            "receipt contention was not observed: {view:?}"
        );
        assert_eq!(
            view.failed, [0; 3],
            "SQLite readiness counted as a failure: {view:?}; observed errors: {observed_errors:?}"
        );
    }
    assert!(view.peak_queue <= 4);
    println!(
        "WAVE6_DISPATCH {}",
        serde_json::json!({"scenario":if force_receipt_contention {"automatic_receipt_busy_recovery"}else{"automatic_typed_bootstrap"},
        "bytes_verified":bytes.len(),"durable_receipts":durable,"recovered_receipts":recovered,"forced_contention":contended,
        "admitted":view.admitted,"rpc_completed":view.completed,"failed":view.failed,"retried":view.retried,"bound_ms":5000})
    );
}

#[test]
fn foreground_dispatch_admits_within_250ms_while_two_other_repositories_are_stalled() {
    use tkfs::{
        model::workspace::AdmissionAction,
        orchestrator::content_dispatch,
        runtime::{content::ContentWait, content_reads},
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let a = Pair::new(b"background one");
    let b = Pair::new(b"background two");
    let c = Pair::new(b"foreground");
    let mut metadata =
        Store::open_secondary(&c.b.ctx.root, &c.b.ctx.repo, &c.b.ctx.device).unwrap();
    metadata
        .set_content_admission(&c.reference, 1, AdmissionAction::Withdraw)
        .unwrap();
    let dest = World::from_workers(&[&a.b, &b.b, &c.b], None).ctx();
    let token = Cancel::new();
    let mut config = FaultConfig::new("content.read.background", FaultAction::Hang);
    config.ms = Some(3000);
    config.count = Some(2);
    fault::configure(Some(config));
    let _reset = Reset;
    let registrations: Vec<_> = [&a, &b, &c]
        .iter()
        .map(|p| {
            content_dispatch::register(&dest, p.peer(&dest), LANE, p.dial_for(&dest), &token)
                .unwrap()
        })
        .collect();
    let until = Instant::now() + Duration::from_secs(3);
    while fault::fired() < 2 && Instant::now() < until {
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(
        fault::fired(),
        2,
        "both background source reads must actually be stalled"
    );
    let before = content_dispatch::view(&dest).unwrap();
    assert_eq!(before.completed[2], 0);
    assert!(before.running >= 2);
    metadata
        .set_content_admission(&c.reference, 2, AdmissionAction::Grant)
        .unwrap();
    let shared = c.b.ctx.engine.clone();
    let h = runtime::lock_engine(&shared)
        .open("file", None, false)
        .unwrap()
        .0;
    content_reads::prepare_open(&shared, h, ContentWait::fixed(Duration::from_secs(1))).unwrap();
    let len = c.root.total_len as usize;
    let started = Instant::now();
    let read = thread::spawn(move || {
        content_reads::read_mount_vec(
            &shared,
            h,
            0,
            len,
            ContentWait::fixed(Duration::from_secs(1)),
        )
    });
    let mut admitted = false;
    while started.elapsed() < Duration::from_millis(250) {
        if content_dispatch::view(&dest).unwrap().admitted[0] > before.admitted[0] {
            admitted = true;
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    let elapsed = started.elapsed();
    let view = content_dispatch::view(&dest).unwrap();
    println!(
        "WAVE6_DISPATCH {}",
        serde_json::json!({
            "scenario": "foreground_behind_two_stalled_repositories",
            "admission_ms": elapsed.as_secs_f64() * 1000.0,
            "admitted": admitted, "stalled_background_completions": view.completed[2],
            "bound_ms": 250,
            "adapter_read": true,
        })
    );
    let mut out = vec![0; c.root.total_len as usize];
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut available = false;
    while Instant::now() < deadline {
        if c.bp
            .read_range(&c.bp.pin(&c.root).unwrap(), 0, &mut out, &context())
            .is_ok()
        {
            available = true;
            break;
        }
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(read.join().unwrap().unwrap().unwrap(), b"foreground");
    runtime::engine::close_handle(&c.b.ctx.engine, h).unwrap();
    token.cancel();
    drop(registrations);
    dest.shutdown.cancel();
    assert!(admitted, "foreground admission took {elapsed:?}: {view:?}");
    assert!(available);
    assert_eq!(out, b"foreground");
    assert_eq!(
        view.completed[2], 0,
        "the foreground result must precede the stalled background completion"
    );
}

#[test]
fn replacing_a_lane_fences_old_work_and_late_registration_drop() {
    use tkfs::{
        orchestrator::content_dispatch,
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let p = Pair::new(b"replacement bytes");
    let peer = p.peer(&p.dest);
    let cancel = Cancel::new();
    let mut config = FaultConfig::new("content.read.background", FaultAction::Hang);
    config.ms = Some(2000);
    config.count = Some(1);
    fault::configure(Some(config));
    let _reset = Reset;
    let old =
        content_dispatch::register(&p.dest, peer.clone(), LANE, p.conn.clone(), &cancel).unwrap();
    let until = Instant::now() + Duration::from_secs(2);
    while fault::fired() < 1 && Instant::now() < until {
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(fault::fired(), 1);
    let new_conn = p.dial_for(&p.dest);
    let new = content_dispatch::register(&p.dest, peer, LANE, new_conn, &cancel).unwrap();
    drop(old);
    let started = Instant::now();
    let until = started + Duration::from_secs(1);
    let mut out = vec![0; p.root.total_len as usize];
    let mut available = false;
    while Instant::now() < until {
        if p.bp
            .read_range(&p.bp.pin(&p.root).unwrap(), 0, &mut out, &context())
            .is_ok()
        {
            available = true;
            break;
        }
        thread::sleep(Duration::from_millis(2));
    }
    let elapsed = started.elapsed();
    let view = content_dispatch::view(&p.dest).unwrap();
    println!(
        "WAVE6_DISPATCH {}",
        serde_json::json!({
            "scenario": "replace_stalled_lane", "available_ms": elapsed.as_secs_f64() * 1000.0,
            "available": available, "bound_ms": 1000,
        })
    );
    cancel.cancel();
    drop(new);
    assert!(available, "replacement waited for old work: {view:?}");
    assert_eq!(view.lanes, 1);
    assert_eq!(out, b"replacement bytes");
}

#[test]
fn cancellation_releases_inflight_dispatch_without_waiting_for_stalled_source_io() {
    use tkfs::{
        orchestrator::content_dispatch,
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    for stop_daemon in [false, true] {
        let p = Pair::new(b"cancelled frame");
        assert!(p.dest.extensions.get().is_none());
        assert!(content_dispatch::view(&p.dest).is_none());
        assert!(
            p.dest.extensions.get().is_none(),
            "observing status started content state"
        );
        let cancel = Cancel::new();
        let mut config = FaultConfig::new("content.read.background", FaultAction::Hang);
        config.ms = Some(2000);
        config.count = Some(1);
        fault::configure(Some(config));
        let _reset = Reset;
        let registration =
            content_dispatch::register(&p.dest, p.peer(&p.dest), LANE, p.conn.clone(), &cancel)
                .unwrap();
        let until = Instant::now() + Duration::from_secs(2);
        while fault::fired() < 1 && Instant::now() < until {
            thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(fault::fired(), 1, "the source must really be stalled");
        let source = content_transport::daemon(&p.source).unwrap();
        let dest = content_transport::daemon(&p.dest).unwrap();
        assert_eq!(dest.scheduler.usage().0, 1);
        assert_eq!(source.scheduler.usage().0, 1);
        let started = Instant::now();
        if stop_daemon {
            p.dest.shutdown.cancel();
        } else {
            cancel.cancel();
        }
        let mut drained = false;
        while started.elapsed() < Duration::from_millis(250) {
            let view = content_dispatch::view(&p.dest).unwrap();
            if view.running == 0
                && view.queued == 0
                && dest.scheduler.usage().0 == 0
                && source.scheduler.usage().0 == 0
                && runtime::content_rpc::state(&p.b.ctx)
                    .unwrap()
                    .scheduler
                    .usage()
                    .0
                    == 0
            {
                drained = true;
                break;
            }
            thread::sleep(Duration::from_millis(2));
        }
        println!(
            "WAVE6_DISPATCH {}",
            serde_json::json!({
                "scenario": if stop_daemon { "daemon_cancel" } else { "lane_cancel" },
                "drain_ms": started.elapsed().as_secs_f64() * 1000.0,
                "drained": drained, "bound_ms": 250,
            })
        );
        assert!(
            drained,
            "cancellation retained work: {:?}",
            content_dispatch::view(&p.dest)
        );
        if !stop_daemon {
            assert!(
                p.conn.closed().is_none(),
                "cancelling one request closed its shared session"
            );
            p.caps(&p.conn);
        }
        // Keep the registration alive until after the assertions: its Drop
        // closes the socket and would hide a missing request-cancel bridge.
        drop(registration);
    }
}

#[test]
fn background_dispatch_progresses_while_a_foreground_source_is_stalled() {
    use tkfs::{
        model::workspace::{
            AdmissionAction, ByteRange, Demand, DemandPriority, FileRevision, WorkspaceId,
        },
        orchestrator::content_dispatch,
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let a = Pair::new(b"stalled foreground");
    let b = Pair::new(b"background must advance");
    let mut metadata =
        Store::open_secondary(&b.b.ctx.root, &b.b.ctx.repo, &b.b.ctx.device).unwrap();
    metadata
        .set_content_admission(&b.reference, 1, AdmissionAction::Withdraw)
        .unwrap();
    let entity: String = Store::open_secondary(&a.b.ctx.root, &a.b.ctx.repo, &a.b.ctx.device)
        .unwrap()
        .db
        .query_row(
            "SELECT entity FROM content_root_metadata WHERE reference=?1",
            [&a.reference],
            |r| r.get(0),
        )
        .unwrap();
    let guard = runtime::content_rpc::demand(
        &a.b.ctx,
        Demand {
            waiter: Uuid::new_v4(),
            reference: a.reference.clone(),
            revision: FileRevision {
                workspace: WorkspaceId {
                    repository: a.scope.repo.clone(),
                    workspace: Uuid::new_v4(),
                },
                entity,
                generation: 1,
                content: a.root.clone(),
            },
            range: ByteRange {
                offset: 0,
                len: a.root.total_len,
            },
            priority: DemandPriority::Foreground,
            eligible_peers: vec![a.a.ctx.device.clone()],
        },
        &context(),
    )
    .unwrap();
    let dest = World::from_workers(&[&a.b, &b.b], None).ctx();
    let cancel = Cancel::new();
    let mut config = FaultConfig::new("content.read.foreground", FaultAction::Hang);
    config.ms = Some(2000);
    config.count = Some(1);
    fault::configure(Some(config));
    let _reset = Reset;
    let registrations: Vec<_> = [&a, &b]
        .iter()
        .map(|p| {
            content_dispatch::register(&dest, p.peer(&dest), LANE, p.dial_for(&dest), &cancel)
                .unwrap()
        })
        .collect();
    let until = Instant::now() + Duration::from_secs(2);
    while fault::fired() < 1 && Instant::now() < until {
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(fault::fired(), 1);
    let before = content_dispatch::view(&dest).unwrap();
    assert_eq!(before.completed[0], 0);
    assert!(before.running >= 1);
    metadata
        .set_content_admission(&b.reference, 2, AdmissionAction::Grant)
        .unwrap();
    let started = Instant::now();
    let until = started + Duration::from_secs(1);
    let mut out = vec![0; b.root.total_len as usize];
    let mut available = false;
    while Instant::now() < until {
        if b.bp
            .read_range(&b.bp.pin(&b.root).unwrap(), 0, &mut out, &context())
            .is_ok()
        {
            available = true;
            break;
        }
        thread::sleep(Duration::from_millis(2));
    }
    let elapsed = started.elapsed();
    let view = content_dispatch::view(&dest).unwrap();
    println!(
        "WAVE6_DISPATCH {}",
        serde_json::json!({
            "scenario": "background_while_foreground_stalled", "available_ms": elapsed.as_secs_f64() * 1000.0,
            "available": available, "foreground_completions": view.completed[0], "bound_ms": 1000,
        })
    );
    cancel.cancel();
    drop(guard);
    drop(registrations);
    dest.shutdown.cancel();
    assert!(available, "background waited for foreground IO: {view:?}");
    assert_eq!(out, b"background must advance");
    assert_eq!(view.completed[0], 0, "foreground IO was no longer stalled");
}

#[test]
fn a_real_legacy_bulk_reconnect_resumes_under_a_new_generation_before_old_io_finishes() {
    legacy_reconnect(1, "native_legacy_reconnect");
}

#[test]
fn a_real_legacy_pool_resize_resumes_before_old_io_and_fences_old_pool_cleanup() {
    legacy_reconnect(2, "native_legacy_pool_resize");
}

fn legacy_reconnect(new_count: u32, scenario: &str) {
    use tkfs::{
        orchestrator::objects,
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let bytes = vec![73; 150_000];
    let p = Pair::new(&bytes);
    // Ensure the injected stall is the actual nonempty file, rather than
    // its empty historical revision.
    runtime::objects_rpc::dispatch(
        &p.b.ctx,
        "objects.write",
        &serde_json::json!({
            "route":p.route,"session":"fixture-empty-receipt","items":[{
                "hash":tkfs::core::hash(&[]),"offset":0,"total":0,"len":0,"last":true
            }]
        }),
        &[],
    )
    .unwrap();
    let mut cfg = FaultConfig::new("worker.rpc.objects.read", FaultAction::Hang);
    cfg.ms = Some(2000);
    cfg.count = Some(1);
    fault::configure(Some(cfg));
    let _reset = Reset;
    let old_cancel = Cancel::new();
    let (ctx, peer, conn, token) = (
        p.dest.clone(),
        p.peer(&p.dest),
        p.conn.clone(),
        old_cancel.clone(),
    );
    let old = thread::spawn(move || objects::run_bulk_lane(ctx, peer, LANE, conn, token));
    let until = Instant::now() + Duration::from_secs(3);
    while fault::fired() == 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(
        fault::fired(),
        1,
        "the old whole-file source read must really be in flight"
    );
    let old_id =
        p.b.ctx
            .inflight_objects
            .lock()
            .unwrap()
            .leases
            .get(&p.root.digest.to_string())
            .unwrap()
            .session
            .clone();
    let old_owned = tkfs::model::lease::parse(&old_id).unwrap().unwrap();
    let mut probe = vec![0; bytes.len()];
    assert!(
        p.bp.read_range(&p.bp.pin(&p.root).unwrap(), 0, &mut probe, &context())
            .is_err(),
        "old file must still be unavailable while its read is stalled"
    );
    let new_lane = (0..new_count)
        .map(|index| LaneSpec::new(index, new_count))
        .find(|lane| lane.owns(&p.root.digest.to_string()))
        .unwrap();
    let mut new_peer = p.peer(&p.dest);
    Arc::get_mut(&mut new_peer).unwrap().pool = new_count + 1;
    let new_conn = session::dial(
        &p.address,
        session::client_hello(
            &p.dest.installation,
            LaneRole::Bulk,
            Some(new_lane),
            new_count + 1,
        ),
        &CodeAuth {
            code: p.code.clone(),
        },
        None,
        ConnOptions::for_role(LaneRole::Bulk),
    )
    .unwrap()
    .0;
    let new_cancel = Cancel::new();
    let (ctx, peer, conn, token) = (p.dest.clone(), new_peer, new_conn, new_cancel.clone());
    let started = Instant::now();
    let newer = thread::spawn(move || objects::run_bulk_lane(ctx, peer, new_lane, conn, token));
    let mut out = vec![0; bytes.len()];
    let mut available = false;
    while started.elapsed() < Duration::from_secs(1) {
        if p.bp
            .read_range(&p.bp.pin(&p.root).unwrap(), 0, &mut out, &context())
            .is_ok()
        {
            available = true;
            break;
        }
        thread::sleep(Duration::from_millis(2));
    }
    let elapsed = started.elapsed();
    let new_scope = objects::pull_holder(&p.source.installation, new_lane);
    let generation =
        p.b.ctx
            .inflight_objects
            .lock()
            .unwrap()
            .lease_runs
            .get(&new_scope)
            .copied();
    old_cancel.cancel();
    new_cancel.cancel();
    old.join().unwrap();
    newer.join().unwrap();
    assert!(
        available,
        "a new legacy run waited for old IO/lease expiry ({elapsed:?})"
    );
    assert_eq!(out, bytes);
    assert!(generation.unwrap().generation > old_owned.generation);
    assert!(generation.unwrap().layout > old_owned.layout);
    assert_eq!(
        p.b.ctx
            .inflight_objects
            .lock()
            .unwrap()
            .lease_runs
            .get(&new_scope)
            .copied(),
        generation,
        "old runner cleanup changed the newer run's fence"
    );
    println!(
        "WAVE6_LEASE {}",
        serde_json::json!({"scenario":scenario,
        "available_ms":elapsed.as_secs_f64()*1000.0,"bound_ms":1000,"old_source_stall_ms":2000,
        "old_lane_count":1,"new_lane_count":new_count,"old_layout":old_owned.layout,"new_layout":generation.unwrap().layout})
    );
}

#[test]
fn native_timed_out_release_cannot_release_or_hold_the_next_pull_round() {
    use tkfs::{
        model::lease::{self, Generation, PullRun},
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let _reset = Reset;
    let bytes = vec![121; 150_000];
    let p = Pair::new(&bytes);
    let conn = session::dial_loopback(
        &p.b.endpoint.address,
        p.b.endpoint.token.as_deref(),
        ConnOptions::default(),
    )
    .unwrap();
    let run = PullRun {
        generation: Generation {
            incarnation: 91,
            run: 2,
        },
        layout: 1,
    };
    let id = |round| {
        lease::qualify_pull_round(&format!("pull:{}:0/1:a", p.route.peer), run, round).unwrap()
    };
    let old = id(3);
    let new = id(4);
    let hash = p.root.digest.to_string();
    let want = |session: &str| {
        conn.call(
            "objects.want",
            serde_json::json!({"route":p.route,"lane":LANE,
        "session":session,"max_items":256,"max_bytes":1024*1024}),
            vec![],
            Priority::Control,
            Duration::from_secs(2),
        )
        .unwrap()
    };
    assert!(
        want(&old).header["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["hash"] == hash)
    );
    let mut config = FaultConfig::new("worker.rpc.objects.release", FaultAction::Hang);
    config.ms = Some(2500);
    config.count = Some(1);
    fault::configure(Some(config));
    let (request, header) = (
        conn.clone(),
        serde_json::json!({"route":p.route,"session":old,"hashes":[hash],"hold_ms":60_000}),
    );
    let release = thread::spawn(move || {
        request.call(
            "objects.release",
            header,
            vec![],
            Priority::Control,
            Duration::from_secs(2),
        )
    });
    let until = Instant::now() + Duration::from_secs(1);
    while fault::fired() == 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(fault::fired(), 1);
    assert!(
        release.join().unwrap().is_err(),
        "the original cleanup did not time out"
    );
    let began = Instant::now();
    assert!(
        want(&new).header["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["hash"] == hash)
    );
    let admission = began.elapsed();
    assert!(admission < Duration::from_millis(250));
    let before =
        p.b.ctx
            .inflight_objects
            .lock()
            .unwrap()
            .leases
            .get(&hash)
            .unwrap()
            .clone();
    let mut config = FaultConfig::new("objects.release.stale", FaultAction::Hang);
    config.ms = Some(0);
    config.count = Some(1);
    fault::configure(Some(config));
    let until = Instant::now() + Duration::from_secs(2);
    while fault::fired() == 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        fault::fired(),
        1,
        "timed-out handler did not reject stale release authorization"
    );
    // The hook runs after the stale verdict, with the state mutex released.
    // A synchronous stale release also gives us its completed result.
    assert_eq!(
        conn.call(
            "objects.release",
            serde_json::json!({"route":p.route,"session":old,"hashes":[hash],"hold_ms":60_000}),
            vec![],
            Priority::Control,
            Duration::from_secs(1)
        )
        .unwrap()
        .header["released"],
        0
    );
    assert_eq!(
        p.b.ctx.inflight_objects.lock().unwrap().leases.get(&hash),
        Some(&before)
    );
    assert!(
        runtime::objects_rpc::source_retry_at(&p.b.ctx, &p.route.remote_replica, &hash).is_none()
    );
    let write = conn
        .call(
            "objects.write",
            serde_json::json!({"route":p.route,"session":new,
        "items":[{"hash":hash,"offset":0,"total":bytes.len(),"len":bytes.len(),"last":true}]}),
            bytes.clone(),
            Priority::Control,
            Duration::from_secs(2),
        )
        .unwrap();
    assert_eq!(write.header["results"][0]["outcome"], "completed");
    println!(
        "WAVE6_ROUND {}",
        serde_json::json!({"scenario":"native_timed_out_release","release_deadline_ms":2000,"handler_stall_ms":2500,
        "next_round_admission_ms":admission.as_secs_f64()*1000.0,"old_release_reached_effect_check":true,"new_lease_preserved":true,"source_backoff_added":false,"bytes_verified":bytes.len()})
    );
    conn.close();
}

#[test]
fn native_download_rounds_use_fresh_ids_and_resume_without_an_idle_interval() {
    use tkfs::{
        orchestrator::objects,
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let _reset = Reset;
    let bytes = vec![122; 300_000];
    let p = Pair::new(&bytes);
    runtime::objects_rpc::dispatch(
        &p.b.ctx,
        "objects.write",
        &serde_json::json!({"route":p.route,"session":"fixture-empty",
        "items":[{"hash":tkfs::core::hash(&[]),"offset":0,"total":0,"len":0,"last":true}]}),
        &[],
    )
    .unwrap();
    let mut config = FaultConfig::new("worker.rpc.objects.read", FaultAction::Hang);
    config.ms = Some(30);
    config.count = Some(10);
    fault::configure(Some(config));
    let cancel = Cancel::new();
    let stats = Arc::new(objects::LaneStats::default());
    let mut options = objects::LaneOptions::for_config(&p.dest.config);
    options.round = Duration::from_millis(20);
    options.window_bytes = 128 * 1024;
    options.stats = Some(stats.clone());
    options.download.interval = Duration::from_secs(60);
    let (ctx, peer, conn, token) = (
        p.dest.clone(),
        p.peer(&p.dest),
        p.conn.clone(),
        cancel.clone(),
    );
    let runner =
        thread::spawn(move || objects::run_lane_with(ctx, peer, LANE, conn, token, options));
    let mut ids = BTreeSet::new();
    let hash = p.root.digest.to_string();
    let mut out = vec![0; bytes.len()];
    let began = Instant::now();
    let mut available = false;
    while began.elapsed() < Duration::from_secs(5) {
        if let Some(held) = p.b.ctx.inflight_objects.lock().unwrap().leases.get(&hash) {
            ids.insert(held.session.clone());
        }
        if p.bp
            .read_range(&p.bp.pin(&p.root).unwrap(), 0, &mut out, &context())
            .is_ok()
        {
            available = true;
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    let elapsed = began.elapsed();
    cancel.cancel();
    runner.join().unwrap();
    assert!(available, "short rounds waited for the 60-second heartbeat");
    assert_eq!(out, bytes);
    assert!(
        ids.len() >= 2,
        "no actual round identity transition was observed: {ids:?}"
    );
    let mut parents = BTreeSet::new();
    let mut nonces = BTreeSet::new();
    for id in &ids {
        let owned = tkfs::model::lease::parse(id).unwrap().unwrap();
        assert!(owned.round_slot.is_some());
        parents.insert((owned.generation, owned.layout));
        nonces.insert(owned.frame);
    }
    assert_eq!(
        parents.len(),
        1,
        "a reconnect substituted for the round transition"
    );
    assert!(nonces.len() >= 2);
    assert!(objects::LaneStats::get(&stats.v3_rounds) >= 2);
    assert!(
        runtime::objects_rpc::source_retry_at(&p.b.ctx, &p.route.remote_replica, &hash).is_none()
    );
    println!(
        "WAVE6_ROUND {}",
        serde_json::json!({"scenario":"native_short_download_rounds","bytes_verified":bytes.len(),"round_ids_observed":ids,
        "parent_runs":parents.len(),"available_ms":elapsed.as_secs_f64()*1000.0,"heartbeat_ms":60000,"source_backoff_added":false})
    );
}

#[test]
fn native_cancelled_foreground_verdict_cannot_back_off_a_replacement_copy() {
    use tkfs::{
        orchestrator::{content_dispatch, objects},
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let _reset = Reset;
    let bytes = vec![137; 150_000];
    let p = Pair::new(&bytes);
    let hash = p.root.digest.to_string();
    runtime::objects_rpc::dispatch(
        &p.b.ctx,
        "objects.write",
        &serde_json::json!({"route":p.route,"session":"fixture-empty",
        "items":[{"hash":tkfs::core::hash(&[]),"offset":0,"total":0,"len":0,"last":true}]}),
        &[],
    )
    .unwrap();
    let mut config = FaultConfig::new("worker.rpc.objects.read", FaultAction::Hang);
    config.ms = Some(2500);
    config.count = Some(1);
    fault::configure(Some(config));
    let old_cancel = Cancel::new();
    let (ctx, peer, conn, token) = (
        p.dest.clone(),
        p.peer(&p.dest),
        p.conn.clone(),
        old_cancel.clone(),
    );
    let old = thread::spawn(move || objects::run_bulk_lane(ctx, peer, LANE, conn, token));
    let until = Instant::now() + Duration::from_secs(2);
    while fault::fired() == 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(fault::fired(), 1);
    let path = p.a.ctx.root.join("objects").join(&hash);
    std::fs::remove_file(&path).unwrap();
    let mut config = FaultConfig::new("legacy.foreground.before_source_backoff", FaultAction::Hang);
    config.ms = Some(1500);
    config.count = Some(1);
    fault::configure(Some(config));
    Store::open_secondary(&p.b.ctx.root, &p.b.ctx.repo, &p.b.ctx.device)
        .unwrap()
        .request_object(&hash)
        .unwrap();
    let until = Instant::now() + Duration::from_secs(1);
    while fault::fired() == 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        fault::fired(),
        1,
        "foreground source verdict was not pending"
    );
    assert!(content_dispatch::view(&p.dest).unwrap().running >= 1);
    assert!(
        objects::object_backoff_for(
            &p.dest.installation,
            &p.source.installation,
            &p.route.repo,
            false,
            &hash
        )
        .is_none()
    );
    old_cancel.cancel();
    std::fs::write(&path, &bytes).unwrap();
    let source = Store::open_secondary(&p.a.ctx.root, &p.a.ctx.repo, &p.a.ctx.device).unwrap();
    assert_eq!(source.read_object_range(&hash, 0, 1).unwrap().1, bytes[..1]);
    let conn = session::dial(
        &p.address,
        session::client_hello(&p.dest.installation, LaneRole::Bulk, Some(LANE), 2),
        &CodeAuth {
            code: p.code.clone(),
        },
        None,
        ConnOptions::for_role(LaneRole::Bulk),
    )
    .unwrap()
    .0;
    let new_cancel = Cancel::new();
    let (ctx, peer, token) = (p.dest.clone(), p.peer(&p.dest), new_cancel.clone());
    let began = Instant::now();
    let new = thread::spawn(move || objects::run_bulk_lane(ctx, peer, LANE, conn, token));
    let mut out = vec![0; bytes.len()];
    let mut available = false;
    while began.elapsed() < Duration::from_secs(1) {
        if p.bp
            .read_range(&p.bp.pin(&p.root).unwrap(), 0, &mut out, &context())
            .is_ok()
        {
            available = true;
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    let availability = began.elapsed();
    assert!(
        available,
        "replacement waited for obsolete foreground verdict"
    );
    assert_eq!(out, bytes);
    let state = content_transport::daemon(&p.dest).unwrap();
    let until = Instant::now() + Duration::from_secs(3);
    while state.scheduler.usage().0 != 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(
        state.scheduler.usage().0,
        0,
        "obsolete foreground task did not retire"
    );
    assert!(
        objects::object_backoff_for(
            &p.dest.installation,
            &p.source.installation,
            &p.route.repo,
            false,
            &hash
        )
        .is_none()
    );
    assert!(
        runtime::objects_rpc::source_retry_at(&p.b.ctx, &p.route.remote_replica, &hash).is_none()
    );
    new_cancel.cancel();
    old.join().unwrap();
    new.join().unwrap();
    println!(
        "WAVE6_FOREGROUND {}",
        serde_json::json!({"scenario":"cancelled_verdict_after_replacement","verdict_stall_ms":1500,"replacement_ms":availability.as_secs_f64()*1000.0,
        "replacement_bound_ms":1000,"bytes_verified":bytes.len(),"daemon_backoff_added":false,"worker_backoff_added":false,"foreground_budget_drained":true})
    );
}

#[test]
fn native_catalog_withdrawal_cancels_a_legacy_verdict_without_closing_its_lane() {
    use tkfs::{
        orchestrator::{content_dispatch, objects},
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let _reset = Reset;
    let bytes = vec![129; 150_000];
    let p = Pair::new(&bytes);
    let hash = p.root.digest.to_string();
    runtime::objects_rpc::dispatch(
        &p.b.ctx,
        "objects.write",
        &serde_json::json!({"route":p.route,"session":"fixture-empty",
        "items":[{"hash":tkfs::core::hash(&[]),"offset":0,"total":0,"len":0,"last":true}]}),
        &[],
    )
    .unwrap();
    let peer = p.peer(&p.dest);
    let original = peer.catalog.load();
    let cancel = Cancel::new();
    let mut config = FaultConfig::new("worker.rpc.objects.read", FaultAction::Hang);
    config.ms = Some(1500);
    config.count = Some(1);
    fault::configure(Some(config));
    let (ctx, remote, conn, token) = (p.dest.clone(), peer.clone(), p.conn.clone(), cancel.clone());
    let lane = thread::spawn(move || objects::run_bulk_lane(ctx, remote, LANE, conn, token));
    let until = Instant::now() + Duration::from_secs(2);
    while fault::fired() == 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(fault::fired(), 1);
    let path = p.a.ctx.root.join("objects").join(&hash);
    std::fs::remove_file(&path).unwrap();
    let mut config = FaultConfig::new("legacy.foreground.before_source_backoff", FaultAction::Hang);
    config.ms = Some(1000);
    config.count = Some(1);
    fault::configure(Some(config));
    Store::open_secondary(&p.b.ctx.root, &p.b.ctx.repo, &p.b.ctx.device)
        .unwrap()
        .request_object(&hash)
        .unwrap();
    let until = Instant::now() + Duration::from_secs(1);
    while fault::fired() == 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        fault::fired(),
        1,
        "actual foreground verdict must be pending"
    );
    let state = content_transport::daemon(&p.dest).unwrap();
    assert_eq!(state.scheduler.joined_usage().0, 1);
    peer.catalog.store(RemoteCatalog {
        generation: 2,
        at_ms: 2,
        entries: vec![],
    });
    let began = Instant::now();
    while began.elapsed() < Duration::from_millis(250)
        && content_dispatch::view(&p.dest).unwrap().groups != 0
    {
        thread::sleep(Duration::from_millis(1));
    }
    let withdraw_ms = began.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(content_dispatch::view(&p.dest).unwrap().groups, 0);
    assert!(withdraw_ms < 250.0);
    assert!(p.conn.closed().is_none() && !cancel.is_cancelled());
    // A synthetic disk stall is not interruptible. Its reservation must stay
    // held until that old Task actually unwinds, even though its group is gone.
    assert_eq!(state.scheduler.joined_usage().0, 1);
    let until = Instant::now() + Duration::from_secs(2);
    while state.scheduler.joined_usage().0 != 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(state.scheduler.joined_usage().0, 0);
    let old = content_dispatch::view(&p.dest).unwrap();
    assert_eq!(old.failed[0], 0);
    assert_eq!(old.completed[0], 0);
    assert!(
        objects::object_backoff_for(
            &p.dest.installation,
            &p.source.installation,
            &p.route.repo,
            false,
            &hash
        )
        .is_none()
    );
    assert!(
        runtime::objects_rpc::source_retry_at(&p.b.ctx, &p.route.remote_replica, &hash).is_none()
    );
    std::fs::write(&path, &bytes).unwrap();
    Store::open_secondary(&p.a.ctx.root, &p.a.ctx.repo, &p.a.ctx.device)
        .unwrap()
        .read_object_range(&hash, 0, 1)
        .unwrap();
    peer.catalog.store_arc(original);
    let began = Instant::now();
    let mut out = vec![0; bytes.len()];
    let mut available = false;
    while began.elapsed() < Duration::from_secs(1) {
        if p.bp
            .read_range(&p.bp.pin(&p.root).unwrap(), 0, &mut out, &context())
            .is_ok()
        {
            available = true;
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    let replacement_ms = began.elapsed().as_secs_f64() * 1000.0;
    assert!(available, "restored catalog waited for stale source hold");
    assert_eq!(out, bytes);
    assert!(p.conn.closed().is_none());
    cancel.cancel();
    lane.join().unwrap();
    println!(
        "WAVE6_GROUP {}",
        serde_json::json!({"scenario":"legacy_catalog_withdrawal",
        "withdraw_ms":withdraw_ms,"withdraw_bound_ms":250,"old_verdict_stall_ms":1000,
        "replacement_ms":replacement_ms,"replacement_bound_ms":1000,"bytes_verified":bytes.len(),
        "connection_preserved":true,"cancelled_task_failed":false,"cancelled_task_completed":false,
        "source_backoff_added":false,"budget_retained_until_task_unwind":true})
    );
}

#[test]
fn native_catalog_withdrawal_cancels_typed_work_and_reuses_the_same_connection() {
    use tkfs::{
        orchestrator::content_dispatch,
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let _reset = Reset;
    let bytes = b"same connection after group replacement";
    let p = Pair::new(bytes);
    let peer = p.peer(&p.dest);
    let original = peer.catalog.load();
    let cancel = Cancel::new();
    let mut config = FaultConfig::new("content.read.background", FaultAction::Hang);
    config.ms = Some(1500);
    config.count = Some(1);
    fault::configure(Some(config));
    let registration =
        content_dispatch::register(&p.dest, peer.clone(), LANE, p.conn.clone(), &cancel).unwrap();
    let until = Instant::now() + Duration::from_secs(2);
    while fault::fired() == 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(fault::fired(), 1);
    let state = content_transport::daemon(&p.dest).unwrap();
    assert_eq!(state.scheduler.usage().0, 1);
    peer.catalog.store(RemoteCatalog {
        generation: 2,
        at_ms: 2,
        entries: vec![],
    });
    let began = Instant::now();
    let mut drained = false;
    while began.elapsed() < Duration::from_millis(250) {
        let view = content_dispatch::view(&p.dest).unwrap();
        if view.groups == 0
            && view.running == 0
            && view.queued == 0
            && state.scheduler.usage().0 == 0
            && runtime::content_rpc::state(&p.b.ctx)
                .unwrap()
                .scheduler
                .usage()
                .0
                == 0
        {
            drained = true;
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    let drain_ms = began.elapsed().as_secs_f64() * 1000.0;
    assert!(
        drained,
        "withdrawal retained typed work: {:?}",
        content_dispatch::view(&p.dest)
    );
    let old = content_dispatch::view(&p.dest).unwrap();
    assert_eq!(old.failed, [0; 3]);
    assert_eq!(old.completed, [0; 3]);
    assert!(p.conn.closed().is_none() && !cancel.is_cancelled());
    peer.catalog.store_arc(original);
    let began = Instant::now();
    let mut out = vec![0; bytes.len()];
    let mut available = false;
    while began.elapsed() < Duration::from_secs(1) {
        if p.bp
            .read_range(&p.bp.pin(&p.root).unwrap(), 0, &mut out, &context())
            .is_ok()
        {
            available = true;
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    let replacement_ms = began.elapsed().as_secs_f64() * 1000.0;
    assert!(available, "new group could not use the original connection");
    assert_eq!(out, bytes);
    assert!(p.conn.closed().is_none());
    cancel.cancel();
    drop(registration);
    println!(
        "WAVE6_GROUP {}",
        serde_json::json!({"scenario":"typed_catalog_withdrawal",
        "drain_ms":drain_ms,"drain_bound_ms":250,"old_source_stall_ms":1500,
        "replacement_ms":replacement_ms,"replacement_bound_ms":1000,"bytes_verified":bytes.len(),
        "connection_preserved":true,"cancelled_task_failed":false,"cancelled_task_completed":false})
    );
}

#[test]
fn native_legacy_streams_share_global_capacity_and_leave_foreground_service() {
    use tkfs::{
        model::workspace::{ByteRange, Demand, DemandPriority, FileRevision, WorkspaceId},
        orchestrator::{content_dispatch, objects},
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let _reset = Reset;
    let background: Vec<_> = (0..6).map(|i| Pair::new(&vec![150 + i; 300_000])).collect();
    for p in &background {
        runtime::objects_rpc::dispatch(
            &p.b.ctx,
            "objects.write",
            &serde_json::json!({"route":p.route,"session":"fixture-empty",
            "items":[{"hash":tkfs::core::hash(&[]),"offset":0,"total":0,"len":0,"last":true}]}),
            &[],
        )
        .unwrap();
    }
    let foreground = Pair::new(b"foreground through the shared stream budget");
    let workers: Vec<_> = background
        .iter()
        .map(|p| &p.b)
        .chain(std::iter::once(&foreground.b))
        .collect();
    let dest = World::from_workers(&workers, None).ctx();
    let token = Cancel::new();
    let mut config = FaultConfig::new("worker.rpc.objects.read", FaultAction::Hang);
    config.ms = Some(1500);
    fault::configure(Some(config));
    let lanes: Vec<_> = background
        .iter()
        .map(|p| {
            let (ctx, peer, conn, cancel) = (
                dest.clone(),
                p.peer(&dest),
                p.dial_for(&dest),
                token.clone(),
            );
            thread::spawn(move || objects::run_bulk_lane(ctx, peer, LANE, conn, cancel))
        })
        .collect();
    let state = content_transport::daemon(&dest).unwrap();
    let began = Instant::now();
    let mut peak_jobs = 0;
    let mut peak_bytes = 0;
    while began.elapsed() < Duration::from_secs(1) {
        let (jobs, bytes, _, _) = state.scheduler.usage();
        peak_jobs = peak_jobs.max(jobs);
        peak_bytes = peak_bytes.max(bytes);
        assert!(jobs <= 8 && bytes <= 8 * 1024 * 1024);
        assert!(state.scheduler.stream_usage().0 <= 6);
        if fault::fired() >= 6 && state.scheduler.stream_usage().0 == 6 {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(state.scheduler.stream_usage(), (6, 6 * 1024 * 1024));
    assert_eq!(
        fault::fired(),
        6,
        "only globally admitted streams may reach the source"
    );
    let store = Store::open_secondary(
        &foreground.b.ctx.root,
        &foreground.b.ctx.repo,
        &foreground.b.ctx.device,
    )
    .unwrap();
    let entity = store
        .db
        .query_row(
            "SELECT entity FROM content_root_metadata WHERE reference=?1",
            [&foreground.reference],
            |r| r.get::<_, String>(0),
        )
        .unwrap();
    let demand = runtime::content_rpc::demand(
        &foreground.b.ctx,
        Demand {
            waiter: Uuid::new_v4(),
            reference: foreground.reference.clone(),
            revision: FileRevision {
                workspace: WorkspaceId {
                    repository: foreground.scope.repo.clone(),
                    workspace: Uuid::new_v4(),
                },
                entity,
                generation: 1,
                content: foreground.root.clone(),
            },
            range: ByteRange {
                offset: 0,
                len: foreground.root.total_len,
            },
            priority: DemandPriority::Foreground,
            eligible_peers: vec![foreground.a.ctx.device.clone()],
        },
        &context(),
    )
    .unwrap();
    let began = Instant::now();
    let registration = content_dispatch::register(
        &dest,
        foreground.peer(&dest),
        LANE,
        foreground.dial_for(&dest),
        &token,
    )
    .unwrap();
    let mut execution_ms = None;
    let mut out = vec![0; foreground.root.total_len as usize];
    let mut available = false;
    while began.elapsed() < Duration::from_millis(250) {
        let (jobs, bytes, _, _) = state.scheduler.usage();
        peak_jobs = peak_jobs.max(jobs);
        peak_bytes = peak_bytes.max(bytes);
        assert!(jobs <= 8 && bytes <= 8 * 1024 * 1024);
        if content_dispatch::view(&dest).is_some_and(|v| v.started[0] > 0) && execution_ms.is_none()
        {
            execution_ms = Some(began.elapsed().as_secs_f64() * 1000.0);
        }
        if foreground
            .bp
            .read_range(
                &foreground.bp.pin(&foreground.root).unwrap(),
                0,
                &mut out,
                &context(),
            )
            .is_ok()
        {
            available = true;
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    let available_ms = began.elapsed().as_secs_f64() * 1000.0;
    assert!(
        available,
        "foreground lacked actual service: {:?}",
        content_dispatch::view(&dest)
    );
    assert!(execution_ms.is_some());
    assert_eq!(out, b"foreground through the shared stream budget");
    assert_eq!(
        fault::fired(),
        6,
        "new background streams borrowed a reserved class slot"
    );
    drop(demand);
    token.cancel();
    drop(registration);
    for lane in lanes {
        lane.join().unwrap();
    }
    dest.shutdown.cancel();
    assert_eq!(state.scheduler.usage().0, 0);
    println!(
        "WAVE6_STREAM {}",
        serde_json::json!({"scenario":"native_global_legacy_budget","stalled_streams":6,
        "source_stall_ms":1500,"execution_ms":execution_ms,"available_ms":available_ms,"bound_ms":250,
        "peak_jobs":peak_jobs,"peak_reserved_bytes":peak_bytes,"foreground_bytes_verified":out.len(),"all_reservations_drained":true})
    );
}

#[test]
fn native_pending_legacy_writes_continue_the_same_download_round() {
    native_legacy_drain(false);
}

#[test]
fn native_empty_legacy_pull_finishes_only_after_its_actual_local_write() {
    use tkfs::{
        orchestrator::objects,
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    #[derive(Default)]
    struct Events(Mutex<Vec<StatusEvent>>);
    impl StatusSink for Events {
        fn record(&self, event: StatusEvent) {
            self.0.lock().unwrap().push(event);
        }
    }
    let _reset = Reset;
    let mut p = Pair::new(&[]);
    let events = Arc::new(Events::default());
    Arc::get_mut(&mut p.dest).unwrap().status = events.clone();
    let token = Cancel::new();
    let stats = Arc::new(objects::LaneStats::default());
    let mut options = objects::LaneOptions::for_config(&p.dest.config);
    options.stats = Some(stats.clone());
    let mut config = FaultConfig::new("worker.rpc.objects.write", FaultAction::Hang);
    config.ms = Some(600);
    config.count = Some(1);
    fault::configure(Some(config));
    let (ctx, peer, conn, cancel) = (
        p.dest.clone(),
        p.peer(&p.dest),
        p.conn.clone(),
        token.clone(),
    );
    let lane =
        thread::spawn(move || objects::run_lane_with(ctx, peer, LANE, conn, cancel, options));
    let finished = || {
        events
            .0
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                StatusEvent::ExchangeFinished {
                    op,
                    ok,
                    bytes_down,
                    objects_down,
                    ..
                } if op == "objects.pull" => Some((*ok, *bytes_down, *objects_down)),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    let until = Instant::now() + Duration::from_secs(1);
    while Instant::now() < until
        && (fault::fired() == 0 || objects::LaneStats::get(&stats.pulls_in_flight) != 0)
    {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(fault::fired(), 1);
    assert_eq!(objects::LaneStats::get(&stats.pulls_in_flight), 0);
    thread::sleep(Duration::from_millis(40));
    let premature = finished();
    let until = Instant::now() + Duration::from_secs(2);
    while finished().is_empty() && Instant::now() < until {
        thread::sleep(Duration::from_millis(1));
    }
    token.cancel();
    lane.join().unwrap();
    let outcomes = finished();
    assert!(
        premature.is_empty(),
        "empty stream reported completion before its write: {premature:?}"
    );
    assert_eq!(outcomes, vec![(true, 0, 1)]);
    assert_eq!(objects::LaneStats::get(&stats.objects_down), 1);
    println!(
        "WAVE6_BOOKKEEPING {}",
        serde_json::json!({"scenario":"native_empty_pull_completion","write_stall_ms":600,
        "premature_completions":premature.len(),"final_completions":outcomes.len(),"objects_completed":outcomes[0].2,"bytes_completed":outcomes[0].1})
    );
}

#[test]
fn native_pending_legacy_credit_retirement_continues_the_same_download_round() {
    native_legacy_drain(true);
}

fn native_legacy_drain(credit: bool) {
    native_legacy_drain_with_race(credit, false, false);
}

#[test]
fn native_capacity_return_between_refusal_and_pending_snapshot_keeps_round_authority() {
    native_legacy_drain_with_race(true, true, false);
}

#[test]
fn native_local_family_effect_wait_preserves_the_same_download_round() {
    native_legacy_drain_with_race(false, false, true);
}

fn native_legacy_drain_with_race(credit: bool, race: bool, family: bool) {
    use tkfs::{
        orchestrator::objects,
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let _reset = Reset;
    let bytes = vec![
        151;
        if credit {
            1024 * 1024 + 7
        } else {
            4 * 1024 * 1024
        }
    ];
    #[derive(Default)]
    struct Events(Mutex<Vec<StatusEvent>>);
    impl StatusSink for Events {
        fn record(&self, event: StatusEvent) {
            self.0.lock().unwrap().push(event);
        }
    }
    let mut p = Pair::new(&bytes);
    let events = Arc::new(Events::default());
    Arc::get_mut(&mut p.dest).unwrap().status = events.clone();
    let hash = p.root.digest.to_string();
    runtime::objects_rpc::dispatch(
        &p.b.ctx,
        "objects.write",
        &serde_json::json!({"route":p.route,"session":"fixture-empty",
        "items":[{"hash":tkfs::core::hash(&[]),"offset":0,"total":0,"len":0,"last":true}]}),
        &[],
    )
    .unwrap();
    let stall_ms = if family {
        200
    } else if race {
        600
    } else if credit {
        150
    } else {
        250
    };
    let mut config = FaultConfig::new(
        if credit {
            "legacy.sink.before_capacity_return"
        } else {
            "worker.rpc.objects.write"
        },
        FaultAction::Hang,
    );
    config.ms = Some(stall_ms);
    config.count = if race {
        Some(2)
    } else if credit {
        None
    } else {
        Some(1)
    };
    fault::configure(Some(config));
    let cancel = Cancel::new();
    let stats = Arc::new(objects::LaneStats::default());
    let mut options = objects::LaneOptions::for_config(&p.dest.config);
    options.stats = Some(stats.clone());
    options.download.interval = Duration::from_secs(60);
    let (ctx, peer, conn, token) = (
        p.dest.clone(),
        p.peer(&p.dest),
        p.conn.clone(),
        cancel.clone(),
    );
    let lane = thread::spawn(move || objects::run_lane_with(ctx, peer, LANE, conn, token, options));
    let family_want = if family {
        let until = Instant::now() + Duration::from_secs(3);
        while fault::fired() == 0 && Instant::now() < until {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(fault::fired(), 1, "the actual write must reach admission");
        let session =
            p.b.ctx
                .inflight_objects
                .lock()
                .unwrap()
                .leases
                .get(&hash)
                .unwrap()
                .session
                .clone();
        let mut held = FaultConfig::new("objects.want.family_effect_held", FaultAction::Hang);
        held.ms = Some(350);
        held.count = Some(1);
        fault::configure(Some(held));
        let ctx = p.b.ctx.clone();
        let route = p.route.clone();
        let wanting = thread::spawn(move || {
            runtime::objects_rpc::dispatch(
                &ctx,
                "objects.want",
                &serde_json::json!({"route":route,"session":session,"lane":LANE,"max_items":256,"max_bytes":4*1024*1024,"exclude":[]}),
                &[],
            )
        });
        let until = Instant::now() + Duration::from_secs(3);
        while fault::fired() == 0 && Instant::now() < until {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(fault::fired(), 1, "the real want must own the family gate");
        let mut waiting = FaultConfig::new("objects.write.family_waiting", FaultAction::Hang);
        waiting.ms = Some(0);
        waiting.count = Some(1);
        fault::configure(Some(waiting));
        Some(wanting)
    } else {
        None
    };
    if race {
        let until = Instant::now() + Duration::from_secs(3);
        while fault::fired() < 2 && Instant::now() < until {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            fault::fired(),
            2,
            "both native credits must actually be retiring"
        );
        let mut refused = FaultConfig::new("legacy.round.capacity_refused", FaultAction::Hang);
        refused.ms = Some(850);
        refused.count = Some(1);
        fault::configure(Some(refused));
    }
    let began = Instant::now();
    let mut nonces = BTreeSet::new();
    let mut out = vec![0; bytes.len()];
    let pin = p.bp.pin(&p.root).unwrap();
    let mut available = false;
    while began.elapsed() < Duration::from_secs(5) {
        if let Some(held) = p.b.ctx.inflight_objects.lock().unwrap().leases.get(&hash) {
            let owner = tkfs::model::lease::parse(&held.session).unwrap().unwrap();
            assert!(owner.round_slot.is_some());
            nonces.insert(owner.frame);
        }
        if out.chunks_mut(1024 * 1024).enumerate().all(|(i, chunk)| {
            p.bp.read_range(&pin, (i * 1024 * 1024) as u64, chunk, &context())
                .is_ok()
        }) {
            available = true;
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    let elapsed = began.elapsed();
    let stalled = fault::fired();
    cancel.cancel();
    lane.join().unwrap();
    if let Some(wanting) = family_want
        && let Err(error) = wanting.join().unwrap()
    {
        assert_eq!(error.code, "WORKER_BUSY");
    }
    if race {
        assert_eq!(stalled, 1, "the refusal-to-pending race was not forced");
    } else if credit {
        assert!(stalled >= 2);
    } else {
        assert_eq!(stalled, 1);
    }
    assert!(available, "local-write pressure waited for the heartbeat");
    assert_eq!(out, bytes);
    let exchanges:Vec<_>=events.0.lock().unwrap().iter().filter_map(|event|match event {
        StatusEvent::ExchangeFinished {op,ok,bytes_down,objects_down,error,..} if op=="objects.pull" =>Some(serde_json::json!({"ok":ok,"bytes":bytes_down,"objects":objects_down,"error":error})),_=>None,
    }).collect();
    assert_eq!(
        nonces.len(),
        1,
        "own pending writes churned round authority: {nonces:?}; exchanges={exchanges:?}; pulls={}; rounds={}; bytes_written={}",
        objects::LaneStats::get(&stats.pulls),
        objects::LaneStats::get(&stats.v3_rounds),
        objects::LaneStats::get(&stats.bytes_written)
    );
    assert!(
        runtime::objects_rpc::source_retry_at(&p.b.ctx, &p.route.remote_replica, &hash).is_none()
    );
    println!(
        "WAVE6_DRAIN {}",
        serde_json::json!({"scenario":if family {"native_family_gate_same_round"}else if race {"native_credit_return_race_same_round"}else if credit {"native_credit_retirement_same_round"}else{"native_pending_writes_same_round"},"bytes_verified":bytes.len(),
        "available_ms":elapsed.as_secs_f64()*1000.0,"bound_ms":5000,"stall_ms":stall_ms,"stall_hits":stalled,"round_nonces":nonces,
        "capacity_refusal_stall_ms":if race {850}else{0},
        "family_gate_stall_ms":if family {350}else{0},
        "rounds_observed":objects::LaneStats::get(&stats.v3_rounds),"heartbeat_ms":60000,"source_backoff_added":false})
    );
}

#[test]
fn native_legacy_stream_budget_survives_stream_end_and_cancel_until_worker_write_returns() {
    use tkfs::{
        orchestrator::objects,
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let _reset = Reset;
    // Include canonical empty data: zero payload still owns an actual write.
    for bytes in [vec![], vec![127; 16000]] {
        let p = Pair::new(&bytes);
        if !bytes.is_empty() {
            runtime::objects_rpc::dispatch(
                &p.b.ctx,
                "objects.write",
                &serde_json::json!({"route":p.route,"session":"fixture-empty",
                "items":[{"hash":tkfs::core::hash(&[]),"offset":0,"total":0,"len":0,"last":true}]}),
                &[],
            )
            .unwrap();
        }
        let token = Cancel::new();
        let stats = Arc::new(objects::LaneStats::default());
        let mut options = objects::LaneOptions::for_config(&p.dest.config);
        options.stats = Some(stats.clone());
        let mut config = FaultConfig::new("worker.rpc.objects.write", FaultAction::Hang);
        config.ms = Some(750);
        config.count = Some(1);
        fault::configure(Some(config));
        let (ctx, peer, conn, cancel) = (
            p.dest.clone(),
            p.peer(&p.dest),
            p.conn.clone(),
            token.clone(),
        );
        let lane =
            thread::spawn(move || objects::run_lane_with(ctx, peer, LANE, conn, cancel, options));
        let state = content_transport::daemon(&p.dest).unwrap();
        let began = Instant::now();
        while began.elapsed() < Duration::from_secs(1)
            && (fault::fired() == 0 || objects::LaneStats::get(&stats.pulls_in_flight) != 0)
        {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(fault::fired(), 1);
        assert_eq!(objects::LaneStats::get(&stats.pulls_in_flight), 0);
        assert_eq!(
            state.scheduler.stream_usage(),
            (1, 1024 * 1024),
            "ended stream lent out a pending write's budget"
        );
        token.cancel();
        thread::sleep(Duration::from_millis(30));
        assert_eq!(
            state.scheduler.stream_usage().0,
            1,
            "cancel lent out a still-running write's budget"
        );
        lane.join().unwrap();
        let retained_after_lane_join = state.scheduler.stream_usage().0 != 0;
        // The historical runner has a 500 ms exit wait; its still-running
        // round may outlive join. That is precisely when the stream proof
        // must remain retained. Observe physical retirement separately.
        let until = Instant::now() + Duration::from_secs(2);
        while state.scheduler.stream_usage().0 != 0 && Instant::now() < until {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(state.scheduler.stream_usage().0, 0);
        println!(
            "WAVE6_STREAM {}",
            serde_json::json!({"scenario":"native_pending_legacy_write","bytes":bytes.len(),"write_stall_ms":750,
            "stream_ended_before_write":true,"reservation_held_after_cancel":true,
            "retained_after_lane_join":retained_after_lane_join,"all_reservations_drained":true})
        );
        fault::configure(None);
    }
}

#[test]
fn native_partial_foreground_receipt_preserves_source_history_until_whole_file_verification() {
    use tkfs::{
        orchestrator::{content_dispatch, objects},
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let _reset = Reset;
    let bytes = vec![139; wire::LEGACY_BYTES * 2 + 17];
    let p = Pair::new(&bytes);
    let hash = p.root.digest.to_string();
    runtime::objects_rpc::dispatch(
        &p.b.ctx,
        "objects.write",
        &serde_json::json!({"route":p.route,"session":"fixture-empty",
        "items":[{"hash":tkfs::core::hash(&[]),"offset":0,"total":0,"len":0,"last":true}]}),
        &[],
    )
    .unwrap();
    let mut config = FaultConfig::new("worker.rpc.objects.read", FaultAction::Hang);
    config.ms = Some(1500);
    config.count = Some(1);
    fault::configure(Some(config));
    let cancel = Cancel::new();
    let (ctx, peer, conn, token) = (
        p.dest.clone(),
        p.peer(&p.dest),
        p.conn.clone(),
        cancel.clone(),
    );
    let lane = thread::spawn(move || objects::run_bulk_lane(ctx, peer, LANE, conn, token));
    let until = Instant::now() + Duration::from_secs(2);
    while fault::fired() == 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(fault::fired(), 1);
    let mut config = FaultConfig::new("legacy.foreground.before_success", FaultAction::Hang);
    config.ms = Some(2500);
    config.count = Some(1);
    fault::configure(Some(config));
    let probe = Store::open_secondary(&p.b.ctx.root, &p.b.ctx.repo, &p.b.ctx.device).unwrap();
    probe.request_object(&hash).unwrap();
    let until = Instant::now() + Duration::from_secs(1);
    while fault::fired() == 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        fault::fired(),
        1,
        "foreground fragment did not finish before source loss"
    );
    let durable: u64 = probe
        .db
        .query_row(
            "SELECT durable FROM spool_state WHERE hash=?",
            [&hash],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(durable, wire::LEGACY_BYTES as u64);
    std::fs::remove_file(p.a.ctx.root.join("objects").join(&hash)).unwrap();
    let until = Instant::now() + Duration::from_secs(2);
    while objects::object_backoff_for(
        &p.dest.installation,
        &p.source.installation,
        &p.route.repo,
        false,
        &hash,
    )
    .is_none()
        && Instant::now() < until
    {
        thread::sleep(Duration::from_millis(1));
    }
    assert!(
        objects::object_backoff_for(
            &p.dest.installation,
            &p.source.installation,
            &p.route.repo,
            false,
            &hash
        )
        .is_some(),
        "background source failure was not recorded"
    );
    let until = Instant::now() + Duration::from_secs(2);
    while content_dispatch::view(&p.dest).unwrap().completed[0] == 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(2));
    }
    assert!(content_dispatch::view(&p.dest).unwrap().completed[0] >= 1);
    assert!(
        objects::object_backoff_for(
            &p.dest.installation,
            &p.source.installation,
            &p.route.repo,
            false,
            &hash
        )
        .is_some(),
        "unverified prefix cleared whole-file source history"
    );
    let mut out = vec![0; bytes.len()];
    assert!(
        p.bp.read_range(&p.bp.pin(&p.root).unwrap(), 0, &mut out, &context())
            .is_err()
    );
    cancel.cancel();
    lane.join().unwrap();
    println!(
        "WAVE6_FOREGROUND {}",
        serde_json::json!({"scenario":"partial_receipt_retains_history","durable_prefix_bytes":durable,"whole_file_bytes":bytes.len(),
        "source_history_preserved":true,"whole_file_available":false,"completion_stall_ms":2500})
    );
}

#[test]
fn native_legacy_offers_check_deadline_disconnect_and_shutdown_before_late_grants() {
    use tkfs::{
        model::lease::{self, Generation, PullRun},
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let _reset = Reset;
    for replacement in [false, true] {
        for operation in ["objects.want", wire::LEGACY_PREPARE] {
            for phase in ["entry", "grant"] {
                for stop in ["deadline", "disconnect", "shutdown"] {
                    let p = Pair::new(&vec![131; 100_000]);
                    let hash = p.root.digest.to_string();
                    let probe =
                        Store::open_secondary(&p.b.ctx.root, &p.b.ctx.repo, &p.b.ctx.device)
                            .unwrap();
                    probe.request_object(&hash).unwrap();
                    let dial = || {
                        session::dial_loopback(
                            &p.b.endpoint.address,
                            p.b.endpoint.token.as_deref(),
                            ConnOptions::default(),
                        )
                        .unwrap()
                    };
                    let conn = dial();
                    let run = PullRun {
                        generation: Generation {
                            incarnation: 91,
                            run: 2,
                        },
                        layout: 1,
                    };
                    let id = |n| {
                        if operation == "objects.want" {
                            lease::qualify_pull_round(
                                &format!("pull:{}:0/1:a", p.route.peer),
                                run,
                                n,
                            )
                            .unwrap()
                        } else {
                            lease::qualify_pull(
                                &format!("pull:{}:0/1:c{n:016x}", p.route.peer),
                                run,
                            )
                            .unwrap()
                        }
                    };
                    let old = id(3);
                    let new = id(4);
                    let offer = |session: &str| {
                        if operation == "objects.want" {
                            serde_json::json!({"route":p.route,"lane":LANE,"session":session,
                            "max_items":1,"max_bytes":wire::LEGACY_BYTES})
                        } else {
                            serde_json::json!({"route":p.route,"lane":LANE,"session":session,"hash":hash})
                        }
                    };
                    let point = if phase == "entry" {
                        format!("worker.rpc.{operation}")
                    } else {
                        "objects.want.before_grant".into()
                    };
                    let mut config = FaultConfig::new(&point, FaultAction::Hang);
                    config.count = Some(1);
                    config.ms = Some(500);
                    fault::configure(Some(config));
                    let (request, payload) = (conn.clone(), offer(&old));
                    let running = thread::spawn(move || {
                        request.call(
                            operation,
                            payload,
                            vec![],
                            Priority::Control,
                            if stop == "deadline" {
                                Duration::from_millis(200)
                            } else {
                                Duration::from_secs(2)
                            },
                        )
                    });
                    let until = Instant::now() + Duration::from_secs(1);
                    while fault::fired() == 0 && Instant::now() < until {
                        thread::sleep(Duration::from_millis(1));
                    }
                    assert_eq!(
                        fault::fired(),
                        1,
                        "{operation}/{phase}/{stop} did not enter the stall"
                    );
                    let mut config = FaultConfig::new(
                        if operation == "objects.want" {
                            "objects.want.stopped"
                        } else {
                            "legacy.prepare.stopped"
                        },
                        FaultAction::Hang,
                    );
                    config.count = None;
                    config.ms = Some(0);
                    fault::configure(Some(config));
                    let began = Instant::now();
                    match stop {
                        "disconnect" => conn.close(),
                        "shutdown" => {
                            p.b.server.stop(StopMode::Force).unwrap();
                        }
                        _ => {}
                    }
                    assert!(
                        running.join().unwrap().is_err(),
                        "{phase}/{stop} returned a live offer"
                    );
                    let current = if replacement && stop != "shutdown" {
                        let current = dial();
                        let released = current
                            .call(
                                "objects.release",
                                serde_json::json!({"route":p.route,"session":old,"hashes":[hash]}),
                                vec![],
                                Priority::Control,
                                Duration::from_secs(1),
                            )
                            .unwrap();
                        assert_eq!(released.header["released"], 0);
                        let reply = current
                            .call(
                                operation,
                                offer(&new),
                                vec![],
                                Priority::Control,
                                Duration::from_secs(1),
                            )
                            .unwrap();
                        assert_eq!(reply.header["items"].as_array().unwrap().len(), 1);
                        Some((
                            current,
                            p.b.ctx
                                .inflight_objects
                                .lock()
                                .unwrap()
                                .leases
                                .get(&hash)
                                .unwrap()
                                .clone(),
                        ))
                    } else {
                        None
                    };
                    let until = Instant::now() + Duration::from_secs(1);
                    while fault::fired() == 0 && Instant::now() < until {
                        thread::sleep(Duration::from_millis(1));
                    }
                    assert_eq!(fault::fired(), 1, "{phase}/{stop} handler did not retire");
                    let elapsed = began.elapsed();
                    {
                        let state = p.b.ctx.inflight_objects.lock().unwrap();
                        if let Some((_, expected)) = &current {
                            assert_eq!(state.leases.get(&hash), Some(expected));
                        } else {
                            assert!(!state.leases.contains_key(&hash));
                        }
                        assert!(state.source_backoff.is_empty());
                        assert!(!state.leases.values().any(|held| held.session == old));
                    }
                    assert!(elapsed < Duration::from_secs(1));
                    println!(
                        "WAVE6_FOREGROUND {}",
                        serde_json::json!({"scenario":"cancelled_offer","operation":operation,"phase":phase,"stop":stop,"replacement_requested":replacement,
                "stall_ms":500,"drain_ms":elapsed.as_secs_f64()*1000.0,"bound_ms":1000,"late_old_lease":false,
                "replacement_preserved":current.is_some(),"source_backoff_added":false})
                    );
                    if let Some((current, _)) = current {
                        current.close();
                    }
                    conn.close();
                    fault::configure(None);
                }
            }
        }
    }
}

#[test]
fn legacy_foreground_admits_within_250ms_behind_two_other_repositories_without_a_heartbeat() {
    use tkfs::{
        orchestrator::objects,
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let a = Pair::new(&vec![81; 150_000]);
    let b = Pair::new(&vec![82; 150_000]);
    let c_bytes = vec![83; 1024 * 1024 + 23];
    let c = Pair::new(&c_bytes);
    let fresh = Worker::new(store(
        &c._dir.path().join("fresh-destination"),
        &c.scope.repo,
    ));
    // Do not spend the two fault injections on a zero-byte historical
    // revision: establish its actual verified shared receipt during setup.
    for p in [&a, &b] {
        let (reply, _) = runtime::objects_rpc::dispatch(
            &p.b.ctx,
            "objects.write",
            &serde_json::json!({
                "route":p.route,"session":"fixture-empty-receipt","items":[{
                    "hash":tkfs::core::hash(&[]),"offset":0,"total":0,"len":0,"last":true
                }]
            }),
            &[],
        )
        .unwrap();
        assert_eq!(reply["results"][0]["outcome"], "completed");
    }
    // Exercise ordinary legacy repositories, with typed activation disabled.
    for worker in [&a.a, &b.a, &c.a, &a.b, &b.b, &fresh] {
        worker
            .ctx
            .sync_stores
            .checkout()
            .unwrap()
            .db
            .execute(
                "UPDATE config SET value='0' WHERE key='fixture:content_v1'",
                [],
            )
            .unwrap();
    }
    let (code, verifier) = Verifier::generate();
    let source = World::from_workers(&[&a.a, &b.a, &c.a], Some(verifier)).ctx();
    let dest = World::from_workers(&[&a.b, &b.b, &fresh], None).ctx();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("ws://{}", listener.local_addr().unwrap());
    let server = inbound::start_with(source.clone(), listener, InboundOptions::default());
    let peer = Arc::new(PeerContext {
        installation: source.installation.clone(),
        endpoint: address.clone(),
        local_installation: dest.installation.clone(),
        pool: 2,
        catalog: Snapshot::new(RemoteCatalog {
            generation: 1,
            at_ms: 1,
            entries: [&a, &b, &c]
                .iter()
                .map(|p| PublishedEntry {
                    repo: p.scope.repo.clone(),
                    replica: p.a.ctx.device.clone(),
                    state: EntryState::Running,
                    ..Default::default()
                })
                .collect(),
        }),
    });
    let (conn, _) = session::dial(
        &address,
        session::client_hello(&dest.installation, LaneRole::Bulk, Some(LANE), 2),
        &CodeAuth { code },
        None,
        ConnOptions::for_role(LaneRole::Bulk),
    )
    .unwrap();
    let mut config = FaultConfig::new("worker.rpc.objects.read", FaultAction::Hang);
    config.ms = Some(2000);
    config.count = Some(2);
    fault::configure(Some(config));
    let _reset = Reset;
    let token = Cancel::new();
    let stats = Arc::new(objects::LaneStats::default());
    let mut options = objects::LaneOptions::for_config(&dest.config);
    options.stats = Some(stats.clone());
    options.download.interval = Duration::from_secs(60);
    let (ctx, connection, cancel) = (dest.clone(), conn.clone(), token.clone());
    let running =
        thread::spawn(move || objects::run_lane_with(ctx, peer, LANE, connection, cancel, options));
    let until = Instant::now() + Duration::from_secs(3);
    while fault::fired() < 2 && Instant::now() < until {
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(
        fault::fired(),
        2,
        "both other repositories must actually be stalled"
    );
    assert_eq!(objects::LaneStats::get(&stats.repos), 2);
    let stalled_at = Instant::now();
    // Make the third repository's legacy content eligible only now. Its
    // published FolderView is deliberately unchanged throughout the test.
    let mut metadata =
        Store::open_secondary(&fresh.ctx.root, &fresh.ctx.repo, &fresh.ctx.device).unwrap();
    let source_metadata =
        Store::open_secondary(&c.a.ctx.root, &c.a.ctx.repo, &c.a.ctx.device).unwrap();
    metadata
        .receive(
            Bundle {
                repo: c.scope.repo.clone(),
                branches: vec![],
                objects: BTreeMap::new(),
                events: source_metadata.events().unwrap(),
            },
            &BTreeSet::from([c.a.ctx.device.clone()]),
        )
        .unwrap();
    for p in [&a, &b] {
        let mut probe = vec![0; 150_000];
        assert!(
            p.bp.read_range(&p.bp.pin(&p.root).unwrap(), 0, &mut probe, &context())
                .is_err(),
            "background repository finished before demand; setup elapsed {:?}",
            stalled_at.elapsed()
        );
        assert!(
            p.b.ctx
                .inflight_objects
                .lock()
                .unwrap()
                .leases
                .contains_key(&p.root.digest.to_string()),
            "the stalled background must retain actual file transfer ownership"
        );
    }
    let provider = LocalContent::open(&fresh.ctx.root).unwrap();
    let started = Instant::now();
    metadata.request_object(&c.root.digest.to_string()).unwrap();
    let mut out = vec![0; c_bytes.len()];
    let mut read_available = || {
        let Ok(pin) = provider.pin(&c.root) else {
            return false;
        };
        out.chunks_mut(1024 * 1024)
            .enumerate()
            .all(|(index, bytes)| {
                provider
                    .read_range(&pin, (index * 1024 * 1024) as u64, bytes, &context())
                    .is_ok()
            })
    };
    let mut admitted = false;
    let mut available = false;
    while started.elapsed() < Duration::from_millis(250) {
        available = read_available();
        if available
            || fresh
                .ctx
                .inflight_objects
                .lock()
                .unwrap()
                .leases
                .contains_key(&c.root.digest.to_string())
        {
            admitted = true;
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    let admission = started.elapsed();
    while !available && started.elapsed() < Duration::from_secs(1) {
        available = read_available();
        if !available {
            thread::sleep(Duration::from_millis(2));
        }
    }
    let availability = started.elapsed();
    // Installed bytes become readable before the final frame's job records
    // its completion. Observe that acknowledgement separately, within the
    // same overall bound, before cancelling the dispatcher.
    let mut dispatch = tkfs::orchestrator::content_dispatch::view(&dest);
    while started.elapsed() < Duration::from_secs(1)
        && dispatch.as_ref().is_none_or(|d| d.completed[0] < 5)
    {
        thread::sleep(Duration::from_millis(1));
        dispatch = tkfs::orchestrator::content_dispatch::view(&dest);
    }
    let completion = started.elapsed();
    let spool = metadata.spool_state(&c.root.digest.to_string()).unwrap();
    token.cancel();
    conn.close();
    running.join().unwrap();
    dest.shutdown.cancel();
    source.shutdown.cancel();
    server.shutdown(Duration::from_secs(2));
    assert!(
        admitted,
        "legacy foreground waited {admission:?} behind unrelated rounds"
    );
    assert!(
        available,
        "legacy foreground bytes waited {availability:?}; dispatch={dispatch:?}; spool={spool:?}"
    );
    assert_eq!(out, c_bytes);
    assert!(
        objects::LaneStats::get(&stats.repos_peak) <= 2,
        "foreground admission exceeded the existing repository budget"
    );
    assert!(
        dispatch.as_ref().is_some_and(|d| d.completed[0] >= 5),
        "multi-frame foreground transfer was not exercised: {dispatch:?}"
    );
    println!(
        "WAVE6_LEGACY_DEMAND {}",
        serde_json::json!({"admission_ms":admission.as_secs_f64()*1000.0,
        "availability_ms":availability.as_secs_f64()*1000.0,"admission_bound_ms":250,"availability_bound_ms":1000,
        "completion_ms":completion.as_secs_f64()*1000.0,"completion_bound_ms":1000,
        "background_stall_ms":2000,"heartbeat_changed":false,"typed_activation":false,"bytes":c_bytes.len(),
        "foreground_frames_completed":dispatch.unwrap().completed[0]})
    );
}

#[test]
fn legacy_source_credit_is_exact_single_use_and_retained_through_an_active_ack() {
    use tkfs::{
        model::protocol::objects::{ObjectsPull, WantItem, WantPriority},
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let bytes = vec![91; 300_000];
    let p = Pair::new(&bytes);
    let request = ObjectsPull {
        repo: p.scope.repo.clone(),
        sender: p.scope.sender.clone(),
        receiver: p.scope.receiver.clone(),
        lane: LANE,
        items: vec![WantItem {
            hash: p.root.digest.to_string(),
            size: bytes.len() as u64,
            offset: 0,
            priority: WantPriority::Demand,
        }],
        budget_bytes: wire::LEGACY_BYTES as u64,
    };
    let begin = || -> wire::Token {
        serde_json::from_value(
            p.conn
                .call(
                    wire::LEGACY_BEGIN,
                    serde_json::to_value(&request).unwrap(),
                    vec![],
                    Priority::Control,
                    Duration::from_secs(2),
                )
                .unwrap()
                .header,
        )
        .unwrap()
    };
    let state = content_transport::daemon(&p.source).unwrap();
    let abandoned = begin();
    assert_eq!(state.scheduler.usage().0, 1);
    p.ack(&p.conn, abandoned).unwrap();
    assert_eq!(state.scheduler.usage().0, 0);
    assert_eq!(
        p.conn
            .call(
                wire::LEGACY_PULL,
                serde_json::to_value(wire::LegacyPull {
                    request: request.clone(),
                    token: abandoned
                })
                .unwrap(),
                vec![],
                Priority::Control,
                Duration::from_secs(2)
            )
            .unwrap_err()
            .code(),
        "CONTENT_CANCELLED"
    );
    let token = begin();
    // An old ACK cannot return a newer owner's credit.
    p.ack(&p.conn, abandoned).unwrap();
    assert_eq!(state.scheduler.usage().0, 1);
    let mut config = FaultConfig::new(
        &format!("worker.rpc.{}", wire::LEGACY_READ),
        FaultAction::Hang,
    );
    config.ms = Some(300);
    config.count = Some(1);
    fault::configure(Some(config));
    let _reset = Reset;
    let connection = p.conn.clone();
    let payload = serde_json::to_value(wire::LegacyPull {
        request: request.clone(),
        token,
    })
    .unwrap();
    let running = thread::spawn(move || {
        let mut received = Vec::new();
        let reply = connection.call_streaming(
            wire::LEGACY_PULL,
            payload,
            vec![],
            Priority::Control,
            Some(Duration::from_secs(2)),
            Duration::from_secs(2),
            &mut |_, chunk| {
                received.extend_from_slice(chunk);
                Ok(())
            },
        );
        (reply, received)
    });
    let until = Instant::now() + Duration::from_secs(1);
    while fault::fired() == 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(fault::fired(), 1);
    p.ack(&p.conn, token).unwrap();
    assert_eq!(
        state.scheduler.usage().0,
        1,
        "ACK during active streaming must retain the byte budget"
    );
    assert_eq!(
        p.conn
            .call(
                wire::LEGACY_PULL,
                serde_json::to_value(wire::LegacyPull { request, token }).unwrap(),
                vec![],
                Priority::Control,
                Duration::from_secs(2)
            )
            .unwrap_err()
            .code(),
        "UNAUTHORIZED_OBJECT"
    );
    let (reply, received) = running.join().unwrap();
    let reply: wire::LegacyReply = serde_json::from_value(reply.unwrap().header).unwrap();
    assert_eq!(reply.token, token);
    assert_eq!(reply.result.results[0].bytes, wire::LEGACY_BYTES as u64);
    assert_eq!(received, &bytes[..wire::LEGACY_BYTES]);
    let until = Instant::now() + Duration::from_secs(1);
    while state.scheduler.usage().0 != 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(state.scheduler.usage().0, 0);
}

#[test]
fn native_legacy_hashing_stops_on_request_deadline_disconnect_and_worker_shutdown() {
    use tkfs::util::fault::{self, FaultAction, FaultConfig};
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let _reset = Reset;
    let bytes = vec![67; 3 * 1024 * 1024 + 13];
    let hash = tkfs::core::hash(&bytes);
    for op in ["objects.read", wire::LEGACY_READ] {
        for stop in ["deadline", "disconnect", "shutdown"] {
            let dir = tempfile::tempdir().unwrap();
            let repo = tkfs::core::id();
            let mut s = Store::initialize(dir.path(), &repo, &tkfs::core::id()).unwrap();
            let entry = s.create("large", "file").unwrap();
            s.write_revision(&entry.id, entry.revisions.get("content").cloned(), &bytes)
                .unwrap();
            let route = serde_json::json!({"peer":"remote", "repo":repo,
                "local_replica":s.device, "remote_replica":"source"});
            let path = dir.path().join("objects").join(&hash);
            // Installed objects have trusted markers. Change the actual file's
            // mtime so this request must enter whole-file verification.
            std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .set_modified(std::time::SystemTime::now() + Duration::from_secs(3600))
                .unwrap();
            let marker =
                |store: &Store| -> (i64, i64, i64) {
                    store.db.query_row(
                    "SELECT state,mtime_ns,verified_ms FROM object_availability WHERE hash=?",
                    [&hash], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
                ).unwrap()
                };
            let before = marker(&s);
            let worker = Worker::new(s);
            let conn = session::dial_loopback(
                &worker.endpoint.address,
                worker.endpoint.token.as_deref(),
                ConnOptions::default(),
            )
            .unwrap();
            let header = serde_json::json!({"route":route,"hash":hash,"offset":0,"len":65536});
            let mut config = FaultConfig::new("core.hash.after_chunk", FaultAction::Hang);
            config.ms = Some(250);
            config.count = Some(1);
            fault::configure(Some(config));
            let (request, payload) = (conn.clone(), header.clone());
            let running = thread::spawn(move || {
                request.call(
                    op,
                    payload,
                    vec![],
                    Priority::Control,
                    if stop == "deadline" {
                        Duration::from_millis(80)
                    } else {
                        Duration::from_secs(2)
                    },
                )
            });
            let until = Instant::now() + Duration::from_secs(1);
            while fault::fired() == 0 && Instant::now() < until {
                thread::sleep(Duration::from_millis(1));
            }
            assert_eq!(fault::fired(), 1, "{op}/{stop} did not enter hash I/O");
            let began = Instant::now();
            match stop {
                "disconnect" => conn.close(),
                "shutdown" => {
                    worker.server.stop(StopMode::Force).unwrap();
                }
                _ => {}
            }
            assert!(
                running.join().unwrap().is_err(),
                "{op}/{stop} served cancelled bytes"
            );
            // A returned caller alone does not prove the hash worker stopped.
            // Observe its checked-out connection and foreground buffers return.
            let until = Instant::now() + Duration::from_secs(1);
            while worker.ctx.sync_stores.idle() == 0 && Instant::now() < until {
                thread::sleep(Duration::from_millis(2));
            }
            assert!(
                worker.ctx.sync_stores.idle() > 0,
                "hash handler did not drain"
            );
            assert_eq!(LocalContent::open(dir.path()).unwrap().buffer_usage().0, 0);
            let elapsed = began.elapsed();
            fault::configure(None);
            let probe =
                Store::open_secondary(dir.path(), &worker.ctx.repo, &worker.ctx.device).unwrap();
            assert_eq!(marker(&probe), before, "partial hash became trusted");
            assert!(path.is_file(), "cancellation quarantined a good object");
            let errors: u64 = probe
                .db
                .query_row(
                    "SELECT count(*) FROM object_error_state WHERE hash=?",
                    [&hash],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(errors, 0, "cancellation created an object retry");
            // A fresh reader must still verify. Fail inside that next hash,
            // then retry normally: neither SQL nor the process cache was trusted.
            let mut config = FaultConfig::new("core.hash.after_chunk", FaultAction::Fail);
            config.count = Some(1);
            fault::configure(Some(config));
            assert_eq!(
                probe.read_object_range(&hash, 0, 65536).unwrap_err().code,
                "NOT_AVAILABLE"
            );
            assert_eq!(
                fault::fired(),
                1,
                "cancelled hash populated a trusted cache"
            );
            fault::configure(None);
            assert_eq!(
                probe.read_object_range(&hash, 0, 65536).unwrap().1,
                bytes[..65536]
            );
            assert!(elapsed < Duration::from_secs(1));
            println!(
                "WAVE6_HASH {}",
                serde_json::json!({"scenario":stop,"op":op,
                "bytes":bytes.len(),"cancel_to_drain_ms":elapsed.as_secs_f64()*1000.0,
                "io_stall_ms":250,"bound_ms":1000,"typed_activation":false,
                "partial_verification_trusted":false})
            );
            conn.close();
        }
    }
}

#[test]
fn native_background_read_stream_cancels_pending_calls_and_keeps_the_worker_session_usable() {
    use tkfs::{
        orchestrator::worker_client::{ReadFlow, ReadRange},
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let _reset = Reset;
    // Exercise both an externally cancelled read and an existing read_stream
    // consumer that stops after the first range while a later one is hashing.
    for external in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Store::initialize(dir.path(), &tkfs::core::id(), &tkfs::core::id()).unwrap();
        let mut hashes = Vec::new();
        for (name, bytes) in [
            ("small", vec![19; 17]),
            ("large", vec![71; 3 * 1024 * 1024 + 17]),
        ] {
            let entry = s.create(name, "file").unwrap();
            s.write_revision(&entry.id, entry.revisions.get("content").cloned(), &bytes)
                .unwrap();
            hashes.push(tkfs::core::hash(&bytes));
        }
        let large_path = s.root.join("objects").join(&hashes[1]);
        std::fs::OpenOptions::new()
            .write(true)
            .open(large_path)
            .unwrap()
            .set_modified(std::time::SystemTime::now() + Duration::from_secs(3600))
            .unwrap();
        let before: i64 =
            s.db.query_row(
                "SELECT mtime_ns FROM object_availability WHERE hash=?",
                [&hashes[1]],
                |row| row.get(0),
            )
            .unwrap();
        let route = SyncRoute {
            peer: "peer".into(),
            repo: s.repo.clone(),
            local_replica: s.device.clone(),
            remote_replica: "remote".into(),
        };
        let worker = Worker::new(s);
        let client = worker.client();
        let ranges = if external {
            vec![ReadRange {
                hash: hashes[1].clone(),
                offset: 0,
                len: 65536,
            }]
        } else {
            hashes
                .iter()
                .map(|hash| ReadRange {
                    hash: hash.clone(),
                    offset: 0,
                    len: if hash == &hashes[0] { 17 } else { 65536 },
                })
                .collect()
        };
        let mut config = FaultConfig::new("core.hash.after_chunk", FaultAction::Hang);
        config.count = Some(1);
        config.ms = Some(400);
        fault::configure(Some(config));
        let cancel = Cancel::new();
        let (task_client, token, route) = (client.clone(), cancel.clone(), route.clone());
        let task = thread::spawn(move || {
            let consume = &mut |_: &ReadRange, result: Result<(u64, Vec<u8>), CallError>| {
                assert!(!external, "cancelled range reached consumer");
                assert_eq!(result.unwrap().1, vec![19; 17]);
                let until = Instant::now() + Duration::from_secs(1);
                while fault::fired() == 0 && Instant::now() < until {
                    thread::sleep(Duration::from_millis(1));
                }
                assert_eq!(fault::fired(), 1, "later hash was not in flight");
                ReadFlow::Stop
            };
            let began = Instant::now();
            if external {
                task_client.read_stream_cancellable(
                    &route,
                    &ranges,
                    2,
                    Duration::from_secs(5),
                    &|| token.is_cancelled(),
                    consume,
                );
            } else {
                task_client.read_stream(&route, &ranges, 2, Duration::from_secs(5), consume);
            }
            began.elapsed()
        });
        let until = Instant::now() + Duration::from_secs(1);
        while fault::fired() == 0 && Instant::now() < until {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(fault::fired(), 1);
        let began = Instant::now();
        cancel.cancel();
        let duration = task.join().unwrap();
        assert!(
            began.elapsed() < Duration::from_millis(250),
            "local readers waited for stalled hash"
        );
        assert!(duration < Duration::from_millis(300));
        // The abandoned call sends CANCEL for that request, keeping unrelated
        // loopback users connected. A destructive socket close would fail here.
        assert_eq!(
            client
                .call("hello", serde_json::json!({}), Duration::from_secs(1))
                .unwrap()["device"],
            worker.ctx.device
        );
        // Park the already-idle fast reader's connection. The next returned
        // connection then belongs to the slow hash, even if the pool reused a
        // single handle for both requests.
        let parked: Vec<_> = (0..worker.ctx.sync_stores.idle())
            .map(|_| worker.ctx.sync_stores.checkout().unwrap())
            .collect();
        let until = Instant::now() + Duration::from_secs(1);
        while worker.ctx.sync_stores.idle() == 0 && Instant::now() < until {
            thread::sleep(Duration::from_millis(2));
        }
        assert!(
            worker.ctx.sync_stores.idle() > 0,
            "abandoned native hash did not drain"
        );
        drop(parked);
        fault::configure(None);
        let probe =
            Store::open_secondary(dir.path(), &worker.ctx.repo, &worker.ctx.device).unwrap();
        let recorded_mtime: i64 = probe
            .db
            .query_row(
                "SELECT mtime_ns FROM object_availability WHERE hash=?",
                [&hashes[1]],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            recorded_mtime, before,
            "abandoned verification became trusted"
        );
        println!(
            "WAVE6_HASH {}",
            serde_json::json!({"scenario":if external {"background_external_cancel"} else {"background_consumer_stop"},
            "local_stream_ms":duration.as_secs_f64()*1000.0,"io_stall_ms":400,"bound_ms":300,
            "worker_session_usable":true,"partial_verification_trusted":false,"typed_activation":false})
        );
    }
}

#[test]
fn native_legacy_read_rechecks_sharing_after_first_serve_verification() {
    use tkfs::util::fault::{self, FaultAction, FaultConfig};
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let _reset = Reset;
    let dir = tempfile::tempdir().unwrap();
    let mut s = Store::initialize(dir.path(), &tkfs::core::id(), &tkfs::core::id()).unwrap();
    let bytes = vec![29; 2 * 1024 * 1024 + 7];
    let hash = tkfs::core::hash(&bytes);
    let entry = s.create("shared", "file").unwrap();
    s.write_revision(&entry.id, entry.revisions.get("content").cloned(), &bytes)
        .unwrap();
    let path = s.root.join("objects").join(&hash);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_modified(std::time::SystemTime::now() + Duration::from_secs(3600))
        .unwrap();
    let route = serde_json::json!({"peer":"remote","repo":s.repo,
        "local_replica":s.device,"remote_replica":"receiver"});
    let worker = Worker::new(s);
    let probe = Store::open_secondary(dir.path(), &worker.ctx.repo, &worker.ctx.device).unwrap();
    let conn = session::dial_loopback(
        &worker.endpoint.address,
        worker.endpoint.token.as_deref(),
        ConnOptions::default(),
    )
    .unwrap();
    let header = serde_json::json!({"route":route,"hash":hash,"offset":0,"len":65536});
    let mut config = FaultConfig::new("core.hash.after_chunk", FaultAction::Hang);
    config.count = Some(1);
    config.ms = Some(250);
    fault::configure(Some(config));
    let (request, payload) = (conn.clone(), header.clone());
    let task = thread::spawn(move || {
        request.call(
            wire::LEGACY_READ,
            payload,
            vec![],
            Priority::Control,
            Duration::from_secs(2),
        )
    });
    let until = Instant::now() + Duration::from_secs(1);
    while fault::fired() == 0 && Instant::now() < until {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(fault::fired(), 1);
    let shared = || -> bool {
        probe.db.query_row(
        "SELECT EXISTS(SELECT 1 FROM published_objects WHERE hash=?1 UNION ALL SELECT 1 FROM received_shared_objects WHERE hash=?1)",
        [&hash], |row|row.get(0),
    ).unwrap()
    };
    assert!(shared());
    probe
        .db
        .execute("DELETE FROM published_objects WHERE hash=?", [&hash])
        .unwrap();
    probe
        .db
        .execute("DELETE FROM received_shared_objects WHERE hash=?", [&hash])
        .unwrap();
    assert!(!shared());
    assert_eq!(task.join().unwrap().unwrap_err().code(), "NOT_AVAILABLE");
    fault::configure(None);
    assert!(path.is_file());
    assert_eq!(
        conn.call(
            "hello",
            serde_json::json!({}),
            vec![],
            Priority::Control,
            Duration::from_secs(1)
        )
        .unwrap()
        .header["device"],
        worker.ctx.device
    );
    assert_eq!(
        conn.call(
            wire::LEGACY_READ,
            header,
            vec![],
            Priority::Control,
            Duration::from_secs(1)
        )
        .unwrap_err()
        .code(),
        "NOT_AVAILABLE"
    );
    println!(
        "WAVE6_HASH {}",
        serde_json::json!({"scenario":"sharing_withdrawn_during_hash",
        "bytes":bytes.len(),"bytes_served":0,"io_stall_ms":250,"worker_session_usable":true,
        "typed_activation":false})
    );
    conn.close();
}

fn partial_source() -> (Pair, Vec<u8>) {
    let bytes: Vec<_> = (0..3 * tkfs::model::content::CHUNK_SIZE as usize)
        .map(|i| ((i / 65_536) * 71 + i % 251) as u8)
        .collect();
    let p = Pair::new(&bytes);
    std::fs::remove_file(
        p.a.ctx
            .root
            .join("objects")
            .join(Digest::bytes(&bytes[131_072..]).to_string()),
    )
    .unwrap();
    (p, bytes)
}
fn wait_for_mounted_demand(p: &Pair) -> tkfs::content::scheduler::DemandSnapshot {
    let state = runtime::content_rpc::state(&p.b.ctx).unwrap();
    let until = Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(demand) = state
            .scheduler
            .snapshots(state.provider.identity(), &p.scope.repo)
            .into_iter()
            .next()
        {
            return demand;
        }
        assert!(
            Instant::now() < until,
            "mounted read did not register demand"
        );
        thread::sleep(Duration::from_millis(1));
    }
}

fn native_writer(p: &Pair) -> (Arc<runtime::content_writes::NativeWrites>, u64) {
    let shared = &p.b.ctx.engine;
    let writes = runtime::content_writes::NativeWrites::install(shared, &context()).unwrap();
    let h = runtime::lock_engine(shared)
        .open("file", None, true)
        .unwrap()
        .0;
    runtime::content_reads::prepare_open(
        shared,
        h,
        runtime::content::ContentWait::fixed(Duration::from_secs(5)),
    )
    .unwrap();
    (writes, h)
}
fn register_native_source(
    p: &Pair,
    cancel: &Cancel,
) -> tkfs::orchestrator::content_dispatch::Registration {
    tkfs::orchestrator::content_dispatch::register(
        &p.dest,
        p.peer(&p.dest),
        LANE,
        p.conn.clone(),
        cancel,
    )
    .unwrap()
}
fn local_native_structure(p: &Pair) {
    let key = ObjectKey::root(&p.root);
    p.bp.install(
        key,
        &p.ap.read_blob(key, None, &context()).unwrap(),
        &context(),
    )
    .unwrap();
}
fn native_private_file(p: &Pair, h: u64) -> Arc<runtime::content_saves::SaveFile> {
    match &runtime::lock_engine(&p.b.ctx.engine)
        .session(h)
        .unwrap()
        .bytes
    {
        tkfs::staging::Staged::Cow(file) => file.clone(),
        _ => panic!("native file missing"),
    }
}
fn native_saved_fixture(p: &Pair, h: u64) {
    // A preexisting Saved overlay (including earlier-format sessions) is not
    // proof that its required base bytes are already available in this process.
    runtime::content_writes::write(
        &p.b.ctx.engine,
        h,
        0,
        &[],
        false,
        false,
        runtime::content::ContentWait::fixed(Duration::from_secs(1)),
    )
    .unwrap();
    native_private_file(p, h)
        .write_at(13, b"changed", &context())
        .unwrap();
    runtime::content_writes::retain_dirty(&p.b.ctx.engine, &context()).unwrap();
}

#[test]
fn native_cow_structural_repair_admits_within_250ms_after_idle_behind_stalled_background() {
    use tkfs::{
        orchestrator::content_dispatch,
        runtime::{content::ContentWait, content_writes},
        util::fault::{self, FaultAction, FaultConfig},
    };
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let a = Pair::new(b"stalled background one");
    let b = Pair::new(b"stalled background two");
    let c = Pair::new(&vec![37; 3 * 65536]);
    let (writes, h) = native_writer(&c);
    let dest = World::from_workers(&[&a.b, &b.b, &c.b], None).ctx();
    let token = Cancel::new();
    let mut config = FaultConfig::new("content.read.background", FaultAction::Hang);
    config.ms = Some(4000);
    config.count = Some(2);
    fault::configure(Some(config));
    let _reset = Reset;
    let mut registrations: Vec<_> = [&a, &b]
        .iter()
        .map(|p| {
            content_dispatch::register(&dest, p.peer(&dest), LANE, p.dial_for(&dest), &token)
                .unwrap()
        })
        .collect();
    let until = Instant::now() + Duration::from_secs(3);
    while fault::fired() < 2 && Instant::now() < until {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        fault::fired(),
        2,
        "two actual background source reads must stall"
    );
    registrations.push(
        content_dispatch::register(&dest, c.peer(&dest), LANE, c.dial_for(&dest), &token).unwrap(),
    );
    // Observe this repository's actual empty structural preparations; neither
    // elapsed sleep nor another repository's counter proves its idle phase.
    let until = Instant::now() + Duration::from_secs(3);
    let idle = loop {
        let view = content_dispatch::view(&dest).unwrap();
        if let Some(idle) = view.idle.into_iter().find(|i| i.repo == c.scope.repo)
            && idle.polls[1] >= 7
        {
            break idle;
        }
        assert!(
            Instant::now() < until,
            "structural dispatcher never became idle"
        );
        thread::sleep(Duration::from_millis(1));
    };
    let key = ObjectKey::root(&c.root);
    assert_eq!(key.kind, ObjectKind::Manifest);
    c.bp.read_blob(key, None, &context()).unwrap();
    // Repair an actually lost local page, retaining the authenticated revision.
    std::fs::remove_file(
        c.b.ctx
            .root
            .join("manifests-v1")
            .join(key.digest.to_string()),
    )
    .unwrap();
    let before = content_dispatch::view(&dest).unwrap();
    let shared = c.b.ctx.engine.clone();
    let began = Instant::now();
    let writer = thread::spawn(move || {
        content_writes::write(
            &shared,
            h,
            13,
            b"changed",
            false,
            false,
            ContentWait::fixed(Duration::from_secs(2)),
        )
    });
    let mut admitted = false;
    while began.elapsed() < Duration::from_millis(250) {
        if content_dispatch::view(&dest).unwrap().admitted[1] > before.admitted[1] {
            admitted = true;
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    let admission = began.elapsed();
    let measured = content_dispatch::view(&dest).unwrap();
    let result = writer.join().unwrap();
    let write_elapsed = began.elapsed();
    token.cancel();
    drop(registrations);
    dest.shutdown.cancel();
    assert_eq!(result.unwrap(), Some(7));
    content_writes::flush(
        &c.b.ctx.engine,
        h,
        ContentWait::fixed(Duration::from_secs(2)),
    )
    .unwrap();
    let mut out = [0; 7];
    native_private_file(&c, h)
        .read_range(13, &mut out, &context())
        .unwrap();
    assert_eq!(&out, b"changed");
    runtime::engine::close_handle(&c.b.ctx.engine, h).unwrap();
    writes.shutdown(&context()).unwrap();
    println!(
        "WAVE6_STRUCTURAL_IDLE {}",
        serde_json::json!({"scenario":"native_cow_repair_after_idle_behind_two_stalled_repositories",
            "idle_polls":idle.polls[1],"idle_due_ms":idle.due_ms[1],
            "admitted":admitted,"admission_ms":admission.as_secs_f64()*1000.0,"bound_ms":250,
            "write_ms":write_elapsed.as_secs_f64()*1000.0,"background_stall_ms":4000,
            "background_completions_before":before.completed[2],"background_completions_after":measured.completed[2],
            "lost_page_repaired":true,"private_bytes_equal":true,"local_fsync":true})
    );
    assert_eq!(
        measured.completed[2], before.completed[2],
        "background completed before admission"
    );
    assert!(
        admitted,
        "new structural demand missed the original 250ms gate: {admission:?}; {idle:?}"
    );
}

#[test]
fn native_cow_read_hydrates_only_the_needed_base_range_from_an_authenticated_peer() {
    use runtime::{content::ContentWait, content_reads, content_writes};
    let (p, bytes) = partial_source();
    local_native_structure(&p);
    let (writes, h) = native_writer(&p);
    content_writes::write(
        &p.b.ctx.engine,
        h,
        65536,
        &vec![177; 65536],
        false,
        false,
        ContentWait::fixed(Duration::from_secs(5)),
    )
    .unwrap();
    let shared = p.b.ctx.engine.clone();
    let read = thread::spawn(move || {
        content_reads::read_mount_vec(
            &shared,
            h,
            13,
            257,
            ContentWait::fixed(Duration::from_secs(5)),
        )
    });
    let demand = wait_for_mounted_demand(&p).demand;
    assert_eq!(demand.reference, p.reference);
    assert_eq!(
        demand.range,
        tkfs::model::workspace::ByteRange {
            offset: 13,
            len: 257
        }
    );
    let began = Instant::now();
    assert!(
        runtime::lock_engine(&p.b.ctx.engine)
            .store
            .lookup("file")
            .is_ok()
    );
    assert!(began.elapsed() < Duration::from_millis(300));
    let cancel = Cancel::new();
    let registration = register_native_source(&p, &cancel);
    assert_eq!(read.join().unwrap().unwrap().unwrap(), bytes[13..270]);
    assert!(
        p.bp.read_range(&p.bp.pin(&p.root).unwrap(), 131072, &mut [0; 1], &context())
            .is_err()
    );
    runtime::engine::close_handle(&p.b.ctx.engine, h).unwrap();
    writes.shutdown(&context()).unwrap();
    cancel.cancel();
    drop(registration);
    println!(
        "WAVE6_COW_BASE {}",
        serde_json::json!({"scenario":"native_range", "authenticated_peer":true,"range_bytes":257,"unrelated_source_chunk_absent":true,"engine_available_during_wait":true})
    );
}

#[test]
fn native_cow_partial_write_prepares_base_then_fsync_succeeds_without_a_peer() {
    use runtime::{content::ContentWait, content_writes};
    let (p, mut bytes) = partial_source();
    let (writes, h) = native_writer(&p);
    bytes[13..20].copy_from_slice(b"changed");
    let shared = p.b.ctx.engine.clone();
    let writer = thread::spawn(move || {
        content_writes::write(
            &shared,
            h,
            13,
            b"changed",
            false,
            false,
            ContentWait::fixed(Duration::from_secs(5)),
        )
    });
    assert_eq!(wait_for_mounted_demand(&p).demand.reference, p.reference);
    let independent = ReadContext {
        deadline: Instant::now() + Duration::from_millis(300),
        cancel: Cancel::new(),
    };
    let began = Instant::now();
    assert_eq!(
        native_private_file(&p, h).len(&independent).unwrap(),
        bytes.len() as u64
    );
    assert!(
        p.bp.buffer_usage().0 <= 1,
        "preflight retained builder buffers while waiting"
    );
    p.bp.import(&mut &[17; 1024][..], 1024, &independent)
        .unwrap();
    let independent_ms = began.elapsed().as_secs_f64() * 1000.0;
    let cancel = Cancel::new();
    let registration = register_native_source(&p, &cancel);
    assert_eq!(writer.join().unwrap().unwrap(), Some(7));
    assert!(
        tkfs::content::write::WriteJournal::open(&p.b.ctx.root)
            .unwrap()
            .pending(None, 64, &context())
            .unwrap()
            .is_empty(),
        "ordinary write forced Saved"
    );
    cancel.cancel();
    drop(registration);
    p.conn.close();
    assert!(
        content_writes::flush(
            &p.b.ctx.engine,
            h,
            ContentWait::fixed(Duration::from_secs(2))
        )
        .unwrap()
    );
    let reference = runtime::lock_engine(&p.b.ctx.engine)
        .store
        .lookup("file")
        .unwrap()
        .content_ref
        .unwrap();
    let mut actual = vec![0; 65536];
    p.bp.read_range(&p.bp.pin(&reference).unwrap(), 0, &mut actual, &context())
        .unwrap();
    assert_eq!(actual, bytes[..65536]);
    assert!(
        p.bp.read_range(
            &p.bp.pin(&reference).unwrap(),
            131072,
            &mut [0; 1],
            &context()
        )
        .is_err()
    );
    assert!(
        runtime::lock_engine(&p.b.ctx.engine)
            .store
            .verify_derived_state()
            .unwrap()
            .is_empty()
    );
    runtime::engine::close_handle(&p.b.ctx.engine, h).unwrap();
    writes.shutdown(&context()).unwrap();
    println!(
        "WAVE6_COW_BASE {}",
        serde_json::json!({"scenario":"native_partial_flush","authenticated_peer":true,"changed_bytes":7,"verified_chunk_bytes":65536,"unrelated_source_chunk_absent":true,"peer_closed_before_fsync":true,"ordinary_write_without_full_checkpoint":true,"independent_preparation_ms":independent_ms,"independent_bound_ms":300,"file_state_and_journal_available":true})
    );
}

#[test]
fn native_cow_full_chunk_write_prepares_only_structure_then_fsync_is_local() {
    use runtime::{content::ContentWait, content_writes};
    let (p, bytes) = partial_source();
    std::fs::remove_file(
        p.a.ctx
            .root
            .join("objects")
            .join(Digest::bytes(&bytes[..65536]).to_string()),
    )
    .unwrap();
    let (writes, h) = native_writer(&p);
    let shared = p.b.ctx.engine.clone();
    let writer = thread::spawn(move || {
        content_writes::write(
            &shared,
            h,
            0,
            &vec![178; 65536],
            false,
            false,
            ContentWait::fixed(Duration::from_secs(5)),
        )
    });
    let demand = wait_for_mounted_demand(&p).demand;
    assert_eq!(demand.reference, p.reference);
    assert_eq!(
        demand.priority,
        tkfs::model::workspace::DemandPriority::Structural
    );
    assert_eq!(demand.range.len, 1);
    let cancel = Cancel::new();
    let registration = register_native_source(&p, &cancel);
    assert_eq!(writer.join().unwrap().unwrap(), Some(65536));
    cancel.cancel();
    drop(registration);
    p.conn.close();
    assert!(
        content_writes::flush(
            &p.b.ctx.engine,
            h,
            ContentWait::fixed(Duration::from_secs(2))
        )
        .unwrap()
    );
    let reference = runtime::lock_engine(&p.b.ctx.engine)
        .store
        .lookup("file")
        .unwrap()
        .content_ref
        .unwrap();
    let mut actual = vec![0; 65536];
    p.bp.read_range(&p.bp.pin(&reference).unwrap(), 0, &mut actual, &context())
        .unwrap();
    assert_eq!(actual, vec![178; 65536]);
    assert!(
        !p.b.ctx
            .root
            .join("objects")
            .join(Digest::bytes(&bytes[..65536]).to_string())
            .exists()
    );
    runtime::engine::close_handle(&p.b.ctx.engine, h).unwrap();
    writes.shutdown(&context()).unwrap();
    println!(
        "WAVE6_COW_BASE {}",
        serde_json::json!({"scenario":"native_full_overwrite","authenticated_peer":true,"changed_bytes":65536,"structural_demand_bytes":1,"overwritten_base_absent_at_both_peers":true,"peer_closed_before_fsync":true})
    );
}

#[test]
fn native_cow_base_wait_deadline_withdrawal_and_stop_keep_saved_bytes_and_release_demand() {
    use runtime::{content::ContentWait, content_writes};
    for outcome in ["deadline", "withdraw", "regrant", "stop", "cancel"] {
        let (p, _) = partial_source();
        let (writes, h) = native_writer(&p);
        native_saved_fixture(&p, h);
        let shared = p.b.ctx.engine.clone();
        let writer = thread::spawn(move || {
            content_writes::write(
                &shared,
                h,
                13,
                b"NOPE",
                false,
                false,
                ContentWait::fixed(Duration::from_millis(500)),
            )
        });
        wait_for_mounted_demand(&p);
        let state = runtime::content_rpc::state(&p.b.ctx).unwrap();
        let began = Instant::now();
        if outcome == "withdraw" || outcome == "regrant" {
            let mut store = Store::open_existing(&p.b.ctx.root).unwrap();
            store
                .set_content_admission(
                    &p.reference,
                    1,
                    tkfs::model::workspace::AdmissionAction::Withdraw,
                )
                .unwrap();
            if outcome == "regrant" {
                store
                    .set_content_admission(
                        &p.reference,
                        2,
                        tkfs::model::workspace::AdmissionAction::Grant,
                    )
                    .unwrap();
            }
        } else if outcome == "stop" {
            p.b.ctx
                .stopping
                .store(true, std::sync::atomic::Ordering::Release);
        } else if outcome == "cancel" {
            p.b.ctx
                .inflight_objects
                .lock()
                .unwrap()
                .legacy_cancel
                .cancel();
        }
        let error = writer.join().unwrap().unwrap_err();
        let expected = match outcome {
            "deadline" => "CONTENT_DEADLINE",
            "withdraw" | "regrant" => "STALE_ADMISSION_GENERATION",
            _ => "CONTENT_CANCELLED",
        };
        assert!(
            format!("{error:#}").contains(expected),
            "{outcome}: {error:#}"
        );
        assert!(began.elapsed() < Duration::from_secs(1));
        assert!(
            state
                .scheduler
                .snapshots(state.provider.identity(), &p.scope.repo)
                .is_empty()
        );
        let journal = tkfs::content::write::WriteJournal::open(&p.b.ctx.root).unwrap();
        assert_eq!(journal.pending(None, 64, &context()).unwrap().len(), 1);
        let file = match &runtime::lock_engine(&p.b.ctx.engine)
            .session(h)
            .unwrap()
            .bytes
        {
            tkfs::staging::Staged::Cow(file) => file.clone(),
            _ => panic!("native session lost its private file"),
        };
        let mut private = [0; 7];
        file.read_range(13, &mut private, &context()).unwrap();
        assert_eq!(&private, b"changed");
        writes.shutdown(&context()).unwrap();
        println!(
            "WAVE6_COW_BASE {}",
            serde_json::json!({"scenario":outcome,"saved_retained":true,"private_bytes_visible":true,"waiter_released":true,"unacknowledged_mutation_not_applied":true})
        );
    }
}

#[test]
fn native_cow_saved_reopen_restores_authenticated_base_hydration() {
    use runtime::{content::ContentWait, content_writes};
    let (mut p, mut bytes) = partial_source();
    let (writes, h) = native_writer(&p);
    native_saved_fixture(&p, h);
    let error = content_writes::flush(
        &p.b.ctx.engine,
        h,
        ContentWait::fixed(Duration::from_millis(100)),
    )
    .unwrap_err();
    assert!(format!("{error:#}").contains("CONTENT_UNAVAILABLE"));
    writes.shutdown(&context()).unwrap();
    drop(writes);
    p.conn.close();
    p.dest.shutdown.cancel();
    let root = p.b.ctx.root.clone();
    p.b = Worker::new(Store::open_existing(&root).unwrap());
    p.dest = World::new(&p.b, None).ctx();
    p.conn = p.dial_for(&p.dest);
    let (writes, h) = native_writer(&p);
    assert_eq!(
        runtime::content_reads::read_mount_vec(
            &p.b.ctx.engine,
            h,
            13,
            7,
            ContentWait::fixed(Duration::from_secs(1))
        )
        .unwrap()
        .unwrap(),
        b"changed"
    );
    let shared = p.b.ctx.engine.clone();
    let writer = thread::spawn(move || {
        content_writes::write(
            &shared,
            h,
            65536,
            &vec![187; 65536],
            false,
            false,
            ContentWait::fixed(Duration::from_secs(5)),
        )
    });
    assert_eq!(wait_for_mounted_demand(&p).demand.reference, p.reference);
    let cancel = Cancel::new();
    let registration = register_native_source(&p, &cancel);
    assert_eq!(writer.join().unwrap().unwrap(), Some(65536));
    cancel.cancel();
    drop(registration);
    p.conn.close();
    assert!(
        content_writes::flush(
            &p.b.ctx.engine,
            h,
            ContentWait::fixed(Duration::from_secs(2))
        )
        .unwrap()
    );
    bytes[13..20].copy_from_slice(b"changed");
    let mut engine = runtime::lock_engine(&p.b.ctx.engine);
    let reference = engine.store.lookup("file").unwrap().content_ref.unwrap();
    assert_eq!(
        engine
            .store
            .db
            .query_row("SELECT count(*) FROM content_save_history", [], |r| r
                .get::<_, usize>(0))
            .unwrap(),
        1
    );
    assert!(engine.store.verify_derived_state().unwrap().is_empty());
    drop(engine);
    let mut out = vec![0; 65536];
    p.bp.read_range(&p.bp.pin(&reference).unwrap(), 0, &mut out, &context())
        .unwrap();
    assert_eq!(out, bytes[..65536]);
    runtime::engine::close_handle(&p.b.ctx.engine, h).unwrap();
    writes.shutdown(&context()).unwrap();
    println!(
        "WAVE6_COW_BASE {}",
        serde_json::json!({"scenario":"saved_reopen","authenticated_peer":true,"private_bytes_visible_before_retry":true,"publications":1,"peer_closed_before_fsync":true})
    );
}

#[test]
#[ignore = "explicit real kernel COW flush plus authenticated peer; isolated stores only"]
fn isolated_kernel_cow_write_prepares_base_then_fsync_succeeds_offline() {
    use std::io::{Read, Seek, SeekFrom, Write};
    if let Some(result_root) = std::env::var_os("TKFS_COW_BASE_KERNEL_CHILD") {
        let (p, mut bytes) = partial_source();
        let writes =
            runtime::content_writes::NativeWrites::install(&p.b.ctx.engine, &context()).unwrap();
        let point = p._dir.path().join("cow-base-mount");
        tkfs::mount::bind(&p.b.ctx.engine).unwrap();
        tkfs::mount::attach(&point).unwrap();
        struct Mounted;
        impl Drop for Mounted {
            fn drop(&mut self) {
                let _ = tkfs::mount::detach(true);
            }
        }
        let mounted = Mounted;
        let path = point.join("file");
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let writer = thread::spawn(move || {
            file.seek(SeekFrom::Start(13)).unwrap();
            file.write_all(b"changed").unwrap();
            file
        });
        assert_eq!(wait_for_mounted_demand(&p).demand.reference, p.reference);
        let cancel = Cancel::new();
        let registration = register_native_source(&p, &cancel);
        let mut file = writer.join().unwrap();
        cancel.cancel();
        drop(registration);
        p.conn.close();
        file.sync_all().unwrap();
        assert_eq!(file.metadata().unwrap().len(), bytes.len() as u64);
        bytes[13..20].copy_from_slice(b"changed");
        let mut out = vec![0; 65536];
        file.seek(SeekFrom::Start(0)).unwrap();
        file.read_exact(&mut out).unwrap();
        assert_eq!(out, bytes[..65536]);
        drop(file);
        let mut reopened = std::fs::File::open(&path).unwrap();
        reopened.read_exact(&mut out).unwrap();
        assert_eq!(out, bytes[..65536]);
        drop(reopened);
        assert!(
            p.bp.read_range(&p.bp.pin(&p.root).unwrap(), 131072, &mut [0; 1], &context())
                .is_err()
        );
        assert!(
            runtime::lock_engine(&p.b.ctx.engine)
                .store
                .verify_derived_state()
                .unwrap()
                .is_empty()
        );
        tkfs::mount::detach(false).unwrap();
        drop(mounted);
        writes.shutdown(&context()).unwrap();
        let result = serde_json::json!({"scenario":"kernel_cow_partial_flush","platform":std::env::consts::OS,"real_kernel_mount":true,"authenticated_peer":true,"changed_bytes":7,"verified_range_bytes":65536,"unrelated_source_chunk_absent":true,"reopen_equal":true,"peer_closed_before_fsync":true});
        std::fs::write(
            std::path::PathBuf::from(result_root).join("result.json"),
            serde_json::to_vec(&result).unwrap(),
        )
        .unwrap();
        return;
    }
    let result = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "isolated_kernel_cow_write_prepares_base_then_fsync_succeeds_offline",
            "--ignored",
            "--nocapture",
        ])
        .env("TKFS_COW_BASE_KERNEL_CHILD", result.path())
        .env("TKFS_CONTENT_STALL_MS", "1000")
        .env("TKFS_CONTENT_BOUND_MS", "5000")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{:?}\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value =
        serde_json::from_slice(&std::fs::read(result.path().join("result.json")).unwrap()).unwrap();
    println!("WAVE6_COW_BASE_KERNEL {value}");
}

#[test]
fn native_cow_fsync_missing_local_base_never_demands_and_retains_saved() {
    use runtime::{content::ContentWait, content_writes};
    use tkfs::util::fault::{self, FaultAction, FaultConfig};
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let _reset = Reset;
    let (p, bytes) = partial_source();
    local_native_structure(&p);
    let key = ObjectKey {
        kind: ObjectKind::Bytes,
        digest: Digest::bytes(&bytes[..65536]),
    };
    let body = p.ap.read_blob(key, None, &context()).unwrap();
    p.bp.install(key, &body, &context()).unwrap();
    let (writes, h) = native_writer(&p);
    content_writes::write(
        &p.b.ctx.engine,
        h,
        13,
        b"changed",
        false,
        false,
        ContentWait::fixed(Duration::from_secs(2)),
    )
    .unwrap();
    std::fs::remove_file(p.b.ctx.root.join("objects").join(key.digest.to_string())).unwrap();
    let mut config = FaultConfig::new("content.base.before_demand", FaultAction::Hang);
    config.ms = Some(0);
    config.count = None;
    fault::configure(Some(config));
    let began = Instant::now();
    let error = content_writes::flush(
        &p.b.ctx.engine,
        h,
        ContentWait::fixed(Duration::from_secs(5)),
    )
    .unwrap_err();
    let elapsed = began.elapsed();
    assert!(
        format!("{error:#}").contains("CONTENT_UNAVAILABLE"),
        "{error:#}"
    );
    assert!(
        elapsed < Duration::from_millis(300),
        "local failure waited for a peer: {elapsed:?}"
    );
    assert_eq!(fault::fired(), 0, "fsync invoked remote base recovery");
    let state = runtime::content_rpc::state(&p.b.ctx).unwrap();
    assert!(
        state
            .scheduler
            .snapshots(state.provider.identity(), &p.scope.repo)
            .is_empty()
    );
    assert_eq!(
        tkfs::content::write::WriteJournal::open(&p.b.ctx.root)
            .unwrap()
            .pending(None, 64, &context())
            .unwrap()
            .len(),
        1
    );
    let mut private = [0; 7];
    native_private_file(&p, h)
        .read_range(13, &mut private, &context())
        .unwrap();
    assert_eq!(&private, b"changed");
    // The authenticated peer is still live; it was deliberately not asked.
    p.caps(&p.conn);
    p.bp.install(key, &body, &context()).unwrap();
    assert!(
        content_writes::flush(
            &p.b.ctx.engine,
            h,
            ContentWait::fixed(Duration::from_secs(2))
        )
        .unwrap()
    );
    assert_eq!(fault::fired(), 0);
    runtime::engine::close_handle(&p.b.ctx.engine, h).unwrap();
    writes.shutdown(&context()).unwrap();
    println!(
        "WAVE6_LOCAL_FSYNC {}",
        serde_json::json!({"scenario":"missing_local_base","failure_ms":elapsed.as_secs_f64()*1000.0,"bound_ms":300,"demand_calls":0,"saved_retained":true,"private_bytes_visible":true,"retry_local_after_restore":true})
    );
}

#[test]
fn native_cow_stale_preflight_never_overwrites_a_concurrent_private_mutation() {
    use runtime::{content::ContentWait, content_writes};
    let (p, _) = partial_source();
    let (writes, h) = native_writer(&p);
    let shared = p.b.ctx.engine.clone();
    let writer = thread::spawn(move || {
        content_writes::write(
            &shared,
            h,
            13,
            b"stale!!",
            false,
            false,
            ContentWait::fixed(Duration::from_secs(5)),
        )
    });
    wait_for_mounted_demand(&p);
    native_private_file(&p, h)
        .write_at(0, &vec![188; 65536], &context())
        .unwrap();
    let cancel = Cancel::new();
    let registration = register_native_source(&p, &cancel);
    let error = writer.join().unwrap().unwrap_err();
    assert!(
        format!("{error:#}").contains("STALE_WRITE_CHECKPOINT"),
        "{error:#}"
    );
    let mut private = [0; 7];
    native_private_file(&p, h)
        .read_range(13, &mut private, &context())
        .unwrap();
    assert_eq!(private, [188; 7]);
    cancel.cancel();
    drop(registration);
    p.conn.close();
    assert!(
        content_writes::flush(
            &p.b.ctx.engine,
            h,
            ContentWait::fixed(Duration::from_secs(2))
        )
        .unwrap()
    );
    runtime::engine::close_handle(&p.b.ctx.engine, h).unwrap();
    writes.shutdown(&context()).unwrap();
    println!(
        "WAVE6_LOCAL_FSYNC {}",
        serde_json::json!({"scenario":"stale_preflight","stale_bytes_applied":false,"concurrent_bytes_preserved":true,"local_fsync":true})
    );
}

#[test]
fn native_cow_shrink_grow_preflight_preserves_zeroes_and_fsync_is_local() {
    use runtime::{content::ContentWait, content_reads, content_writes};
    let (p, bytes) = partial_source();
    let (writes, h) = native_writer(&p);
    let shared = p.b.ctx.engine.clone();
    let resize = thread::spawn(move || {
        content_writes::truncate(
            &shared,
            h,
            65537,
            false,
            ContentWait::fixed(Duration::from_secs(5)),
        )
    });
    assert_eq!(wait_for_mounted_demand(&p).demand.reference, p.reference);
    let cancel = Cancel::new();
    let registration = register_native_source(&p, &cancel);
    assert!(resize.join().unwrap().unwrap());
    assert_eq!(native_private_file(&p, h).len(&context()).unwrap(), 65537);
    cancel.cancel();
    drop(registration);
    p.conn.close();
    assert!(
        content_writes::truncate(
            &p.b.ctx.engine,
            h,
            200000,
            false,
            ContentWait::fixed(Duration::from_secs(2))
        )
        .unwrap()
    );
    assert!(
        tkfs::content::write::WriteJournal::open(&p.b.ctx.root)
            .unwrap()
            .pending(None, 64, &context())
            .unwrap()
            .is_empty()
    );
    assert!(
        content_writes::flush(
            &p.b.ctx.engine,
            h,
            ContentWait::fixed(Duration::from_secs(2))
        )
        .unwrap()
    );
    let out = content_reads::read_mount_vec(
        &p.b.ctx.engine,
        h,
        65536,
        200000 - 65536,
        ContentWait::fixed(Duration::from_secs(2)),
    )
    .unwrap()
    .unwrap();
    assert_eq!(out.len(), 200000 - 65536);
    assert_eq!(out[0], bytes[65536]);
    assert!(out[1..].iter().all(|b| *b == 0));
    runtime::engine::close_handle(&p.b.ctx.engine, h).unwrap();
    writes.shutdown(&context()).unwrap();
    println!(
        "WAVE6_LOCAL_FSYNC {}",
        serde_json::json!({"scenario":"shrink_grow","shrink_bytes":65537,"grow_bytes":200000,"retained_tail_byte":true,"gap_zero":true,"peer_closed_before_grow_and_fsync":true,"ordinary_resize_without_full_checkpoint":true})
    );
}
#[test]
fn adapter_range_read_fetches_real_peer_data_without_an_unrelated_source_chunk() {
    use tkfs::runtime::{content::ContentWait, content_reads};
    let (p, bytes) = partial_source();
    let shared = p.b.ctx.engine.clone();
    let h = runtime::lock_engine(&shared)
        .open("file", None, false)
        .unwrap()
        .0;
    content_reads::prepare_open(&shared, h, ContentWait::fixed(Duration::from_secs(5))).unwrap();
    let read = thread::spawn(move || {
        content_reads::read_mount_vec(
            &shared,
            h,
            13,
            257,
            ContentWait::fixed(Duration::from_secs(5)),
        )
    });
    let demand = wait_for_mounted_demand(&p).demand;
    assert_eq!(demand.reference, p.reference);
    assert_eq!(
        demand.range,
        tkfs::model::workspace::ByteRange {
            offset: 13,
            len: 257
        }
    );
    let cancel = Cancel::new();
    let began = Instant::now();
    let registration = tkfs::orchestrator::content_dispatch::register(
        &p.dest,
        p.peer(&p.dest),
        LANE,
        p.conn.clone(),
        &cancel,
    )
    .unwrap();
    let mut admitted = false;
    while began.elapsed() < Duration::from_millis(250) {
        if tkfs::orchestrator::content_dispatch::view(&p.dest)
            .unwrap()
            .admitted[0]
            > 0
        {
            admitted = true;
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    let admission_ms = began.elapsed().as_secs_f64() * 1000.0;
    let out = read.join().unwrap().unwrap().unwrap();
    assert_eq!(out, bytes[13..270]);
    assert!(admitted, "range demand was not admitted within 250ms");
    assert!(
        p.bp.read_range(
            &p.bp.pin(&p.root).unwrap(),
            131_072,
            &mut [0; 1],
            &context()
        )
        .is_err()
    );
    runtime::engine::close_handle(&p.b.ctx.engine, h).unwrap();
    cancel.cancel();
    drop(registration);
    println!(
        "WAVE6_MOUNT_DEMAND {}",
        serde_json::json!({"scenario":"real_peer_adapter_range", "bytes":out.len(),"admission_ms":admission_ms,"admission_bound_ms":250,"unrelated_chunk_absent_at_source":true,"real_kernel_mount":false})
    );
}

#[test]
fn control_preview_fetches_only_its_referenced_range_over_real_peer_sessions() {
    let (p, bytes) = partial_source();
    let client = p.b.client();
    let reader = client.clone();
    let reference = p.reference.clone();
    let read = thread::spawn(move || {
        reader.call(
            "preview-content",
            serde_json::json!({"reference":reference}),
            Duration::from_secs(5),
        )
    });
    let demand = wait_for_mounted_demand(&p).demand;
    assert_eq!(demand.reference, p.reference);
    assert_eq!(
        demand.range,
        tkfs::model::workspace::ByteRange {
            offset: 0,
            len: 65_536
        }
    );
    let cancel = Cancel::new();
    let registration = tkfs::orchestrator::content_dispatch::register(
        &p.dest,
        p.peer(&p.dest),
        LANE,
        p.conn.clone(),
        &cancel,
    )
    .unwrap();
    let preview = read.join().unwrap().unwrap();
    assert_eq!(preview["size"], bytes.len() as u64);
    assert_eq!(preview["truncated"], true);
    assert_eq!(preview["text"], "Binary content; text preview unavailable");
    let mut out = vec![0; 65_536];
    p.bp.read_range(&p.bp.pin(&p.root).unwrap(), 0, &mut out, &context())
        .unwrap();
    assert_eq!(out, bytes[..65_536]);
    assert!(
        p.bp.read_range(
            &p.bp.pin(&p.root).unwrap(),
            131_072,
            &mut [0; 1],
            &context()
        )
        .is_err()
    );
    assert!(
        runtime::content_rpc::state(&p.b.ctx)
            .unwrap()
            .scheduler
            .snapshots(p.bp.identity(), &p.scope.repo)
            .is_empty()
    );
    cancel.cancel();
    drop(registration);
    client.close();
}

#[test]
fn explicit_control_range_prefetch_drains_over_real_peer_sessions_and_releases_its_owner() {
    let (p, bytes) = partial_source();
    let client = p.b.client();
    let queued = client
        .call(
            "request-content",
            serde_json::json!({"reference":p.reference,"offset":65_536,"len":65_536}),
            Duration::from_secs(5),
        )
        .unwrap();
    assert_eq!(queued["queued"], true);
    let demand = wait_for_mounted_demand(&p).demand;
    assert_eq!(
        demand.range,
        tkfs::model::workspace::ByteRange {
            offset: 65_536,
            len: 65_536
        }
    );
    let cancel = Cancel::new();
    let registration = tkfs::orchestrator::content_dispatch::register(
        &p.dest,
        p.peer(&p.dest),
        LANE,
        p.conn.clone(),
        &cancel,
    )
    .unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    let mut out = vec![0; 65_536];
    loop {
        if p.bp
            .read_range(&p.bp.pin(&p.root).unwrap(), 65_536, &mut out, &context())
            .is_ok()
        {
            break;
        }
        assert!(
            Instant::now() < end,
            "explicit range prefetch did not complete"
        );
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(out, bytes[65_536..131_072]);
    loop {
        runtime::content_rpc::reap(&p.b.ctx);
        if runtime::content_rpc::state(&p.b.ctx)
            .unwrap()
            .scheduler
            .snapshots(p.bp.identity(), &p.scope.repo)
            .is_empty()
        {
            break;
        }
        assert!(
            Instant::now() < end,
            "completed prefetch retained its waiter"
        );
        thread::sleep(Duration::from_millis(5));
    }
    assert!(
        runtime::content_rpc::state(&p.b.ctx)
            .unwrap()
            .scheduler
            .snapshots(p.bp.identity(), &p.scope.repo)
            .is_empty()
    );
    assert_eq!(
        client
            .call(
                "cancel-content-request",
                serde_json::json!({"token":queued["token"]}),
                Duration::from_secs(2)
            )
            .unwrap()["cancelled"],
        false
    );
    assert!(
        p.bp.read_range(
            &p.bp.pin(&p.root).unwrap(),
            131_072,
            &mut [0; 1],
            &context()
        )
        .is_err()
    );
    cancel.cancel();
    drop(registration);
    client.close();
}

#[test]
#[ignore = "explicit real kernel mount plus authenticated peer qualification; isolated stores only"]
fn isolated_kernel_mount_fetches_a_missing_range_from_a_real_peer() {
    use std::io::{Read, Seek, SeekFrom};
    if let Some(result_root) = std::env::var_os("TKFS_DEMAND_MOUNT_CHILD") {
        let (p, bytes) = partial_source();
        let point = p._dir.path().join("kernel-mount");
        tkfs::mount::bind(&p.b.ctx.engine).unwrap();
        tkfs::mount::attach(&point).unwrap();
        struct Mounted;
        impl Drop for Mounted {
            fn drop(&mut self) {
                let _ = tkfs::mount::detach(true);
            }
        }
        let mounted = Mounted;
        let mut file = std::fs::File::open(point.join("file")).unwrap();
        assert_eq!(file.metadata().unwrap().len(), bytes.len() as u64);
        let read = thread::spawn(move || {
            file.seek(SeekFrom::Start(13)).unwrap();
            let mut out = [0; 257];
            file.read_exact(&mut out).unwrap();
            (file, out)
        });
        let demand = wait_for_mounted_demand(&p).demand;
        assert_eq!(demand.reference, p.reference);
        let cancel = Cancel::new();
        let registration = tkfs::orchestrator::content_dispatch::register(
            &p.dest,
            p.peer(&p.dest),
            LANE,
            p.conn.clone(),
            &cancel,
        )
        .unwrap();
        let (file, out) = read.join().unwrap();
        assert_eq!(out.as_slice(), &bytes[13..270]);
        assert_eq!(file.metadata().unwrap().len(), bytes.len() as u64);
        assert!(
            p.bp.read_range(
                &p.bp.pin(&p.root).unwrap(),
                131_072,
                &mut [0; 1],
                &context()
            )
            .is_err()
        );
        drop(file);
        cancel.cancel();
        drop(registration);
        assert!(runtime::lock_engine(&p.b.ctx.engine).health.is_none());
        tkfs::mount::detach(false).unwrap();
        drop(mounted);
        let result = serde_json::json!({"scenario":"real_peer_kernel_range","platform":std::env::consts::OS,"real_kernel_mount":true,"authenticated_peer":true,"descriptor_bytes":bytes.len(),"ready_range_bytes":out.len(),"unrelated_chunk_absent_at_source":true,"typed_activation":"isolated fixture only"});
        std::fs::write(
            std::path::PathBuf::from(result_root).join("result.json"),
            serde_json::to_vec(&result).unwrap(),
        )
        .unwrap();
        return;
    }
    let result = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "isolated_kernel_mount_fetches_a_missing_range_from_a_real_peer",
            "--ignored",
            "--nocapture",
        ])
        .env("TKFS_DEMAND_MOUNT_CHILD", result.path())
        .env("TKFS_CONTENT_STALL_MS", "1000")
        .env("TKFS_CONTENT_BOUND_MS", "5000")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "kernel peer child failed {:?}\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value =
        serde_json::from_slice(&std::fs::read(result.path().join("result.json")).unwrap()).unwrap();
    println!("WAVE6_MOUNT_DEMAND {result}");
}
