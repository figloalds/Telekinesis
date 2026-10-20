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
        Self::with_legacy(bytes, false)
    }
    fn with_legacy(bytes: &[u8], legacy: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let repo = tkfs::core::id();
        let mut a = store(&dir.path().join("a"), &repo);
        let mut b = store(&dir.path().join("b"), &repo);
        let ap = LocalContent::open(&a.root).unwrap();
        let bp = LocalContent::open(&b.root).unwrap();
        let (reference, root) = if legacy {
            let entry = a.create("file", "file").unwrap();
            a.write_revision(&entry.id, entry.revisions.get("content").cloned(), bytes)
                .unwrap();
            (
                "legacy-test".into(),
                FileContentRef::new(
                    tkfs::model::content::ContentKind::Legacy,
                    Digest::bytes(bytes),
                    bytes.len() as u64,
                )
                .unwrap(),
            )
        } else {
            publish(&mut a, &ap, bytes)
        };
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
    let p = Pair::with_legacy(&bytes, true);
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
    let p = Pair::with_legacy(&bytes, true);
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
fn native_legacy_push_reconnect_admits_before_old_release_and_late_cleanup_cannot_release_it() {
    use tkfs::util::fault::{self, FaultAction, FaultConfig};
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }
    let bytes = vec![101; 150_000];
    let p = Pair::with_legacy(&bytes, true);
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
    let p = Pair::with_legacy(&bytes, true);
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
    let p = Pair::with_legacy(&bytes, true);
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
    let p = Pair::with_legacy(&bytes, true);
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
    use tkfs::orchestrator::{content_dispatch, objects};
    let bytes = vec![4; 150_000];
    let p = Pair::new(&bytes);
    let cancel = Cancel::new();
    let token = cancel.clone();
    let (ctx, peer, conn) = (p.dest.clone(), p.peer(&p.dest), p.conn.clone());
    let running = thread::spawn(move || objects::run_bulk_lane(ctx, peer, LANE, conn, token));
    let until = Instant::now() + Duration::from_secs(5);
    let mut out = vec![0; bytes.len()];
    let mut available = false;
    while Instant::now() < until {
        if p.bp
            .read_range(&p.bp.pin(&p.root).unwrap(), 0, &mut out, &context())
            .is_ok()
        {
            available = true;
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    let view = content_dispatch::view(&p.dest).unwrap();
    cancel.cancel();
    running.join().unwrap();
    assert!(available, "automatic content did not hydrate: {view:?}");
    assert_eq!(out, bytes);
    assert!(view.completed[1] >= 1 && view.completed[2] >= 1);
    assert!(view.peak_queue <= 4);
}

#[test]
fn foreground_dispatch_admits_within_250ms_while_two_other_repositories_are_stalled() {
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
    let entity: String = metadata
        .db
        .query_row(
            "SELECT entity FROM content_root_metadata WHERE reference=?1",
            [&c.reference],
            |r| r.get(0),
        )
        .unwrap();
    let started = Instant::now();
    let guard = runtime::content_rpc::demand(
        &c.b.ctx,
        Demand {
            waiter: Uuid::new_v4(),
            reference: c.reference.clone(),
            revision: FileRevision {
                workspace: WorkspaceId {
                    repository: c.scope.repo.clone(),
                    workspace: Uuid::new_v4(),
                },
                entity,
                generation: 1,
                content: c.root.clone(),
            },
            range: ByteRange {
                offset: 0,
                len: c.root.total_len,
            },
            priority: DemandPriority::Foreground,
            eligible_peers: vec![c.a.ctx.device.clone()],
        },
        &context(),
    )
    .unwrap();
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
    drop(guard);
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
    let p = Pair::with_legacy(&bytes, true);
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
    let a = Pair::with_legacy(&vec![81; 150_000], true);
    let b = Pair::with_legacy(&vec![82; 150_000], true);
    let c_bytes = vec![83; 1024 * 1024 + 23];
    let c = Pair::with_legacy(&c_bytes, true);
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
    let dispatch = tkfs::orchestrator::content_dispatch::view(&dest);
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
    let p = Pair::with_legacy(&bytes, true);
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
