//! WP3.3 acceptance: objects v3, orchestrator side (REVAMP_PLAN §6.4,
//! §14 WP3.3).
//!
//! Every test runs the real dialer lane (`objects::run_lane_with`) against
//! the real inbound server (`inbound::start_with` → `inbound_objects`) over
//! an authenticated v3 bulk session, optionally through a `FaultProxy`. Both
//! daemons' workers are `FakeWorker`s: loopback session servers
//! (`session::accept_loopback`) that implement the frozen W0 worker op
//! contract (`objects.want/read/write/report`, `model::protocol::objects`)
//! the way §6.4 specifies the worker side (WP3.2 builds the real one in
//! parallel): durable offsets advanced only by a write, appends only at the
//! durable offset, a whole-object SHA-256 check on completion
//! (`CorruptAtSource`), leases per session, wanted-only writes. A worker can
//! keep its spools on disk with one `sync_data` per write call and a full
//! re-read hash on completion, which the throughput scenarios use.
//!
//! The ignored `qual_*` scenarios measure throughput (release:
//! `cargo test --release --test objects_v3_lanes -- --ignored --test-threads=1`)
//! and print `QUAL {json}` lines.
mod support;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs,
    io::{Read, Seek, SeekFrom, Write},
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use support::FaultProxy;
use tkfs::{
    bulk_sync::{self, Page},
    model::{
        errors::{ErrorWire, Scope},
        health::Generations,
        ipc::WorkerHeartbeat,
        orchestrator::{
            CallError, CatalogApi, CatalogCmd, CatalogSnapshot, Ctx, DaemonEndpoints, EntryState,
            FeedTip, FolderCmd, FolderDirectory, FolderPhase, FolderRecord, FolderView,
            PeerContext, PublishedCatalogApi, PublishedEntry, RemoteCatalog, ShutdownOutcome,
            StatusSink, StatusSource, WorkerClient, WorkerEndpoint,
        },
        protocol::{
            LaneRole, LaneSpec,
            frame::{ChunkHeader, ClientHello},
            objects::{
                ObjectOutcome, ObjectsPull, ObjectsResult, ReadHeader, ReadReply, ReportHeader,
                WantHeader, WantItem, WantPriority, WantReply, WriteHeader, WriteItem, WriteReply,
                WriteResult,
            },
        },
        status::{Direction, StatusEvent, StatusSnapshot},
    },
    orchestrator::{
        Config, Network,
        inbound::{self, InboundOptions, InboundServer},
        objects::{self, LaneOptions, LaneStats},
        worker_client::{self, SessionWorkerClient},
    },
    paired_sync::SyncRoute,
    pairing_code::Verifier,
    session::{self, AcceptPolicy, CodeAuth, Conn, ConnOptions, Handler, Incoming, Priority},
    util::{cancel::Cancel, snapshot::Snapshot, time::SystemClock},
};

const MIB: u64 = 1024 * 1024;
const REPO: &str = "repo-objects";
/// The dialer's (A) and the acceptor's (B) replicas of `REPO`.
const REPLICA_A: &str = "replica-a";
const REPLICA_B: &str = "replica-b";
const STATE_A: &str = "state-a";
const STATE_B: &str = "state-b";
/// The one bulk lane of a pool of 2 (it owns every object).
const LANE: LaneSpec = LaneSpec { index: 0, count: 1 };

fn uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn sleep_ms(ms: u64) {
    thread::sleep(Duration::from_millis(ms));
}

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

fn sha(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Deterministic pseudo-random bytes (xorshift64*).
fn content(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        out.extend_from_slice(&state.wrapping_mul(0x2545_F491_4F6C_DD1D).to_le_bytes());
    }
    out.truncate(len);
    out
}

// ---------------------------------------------------------------------------
// The fake worker: a loopback session server implementing the W0 op contract

#[derive(Default)]
struct Faults {
    /// Every write call sleeps this long first (a slow disk).
    write_delay: Option<Duration>,
    /// Writes wait while this is set (a stalled worker).
    stall_writes: bool,
    /// Reads of these objects serve one flipped byte (a corrupt copy).
    corrupt: HashSet<String>,
    /// The next N write calls are refused `BUSY`.
    busy_writes: usize,
    /// The next write call sleeps this long before it answers.
    hang_next_write: Option<Duration>,
}

enum Held {
    Memory(Arc<Vec<u8>>),
    File(PathBuf, u64),
}

impl Held {
    fn len(&self) -> u64 {
        match self {
            Held::Memory(bytes) => bytes.len() as u64,
            Held::File(_, len) => *len,
        }
    }

    fn read(&self, offset: u64, len: u64) -> Vec<u8> {
        match self {
            Held::Memory(bytes) => bytes[offset as usize..(offset + len) as usize].to_vec(),
            Held::File(path, _) => {
                let mut file = fs::File::open(path).unwrap();
                file.seek(SeekFrom::Start(offset)).unwrap();
                let mut out = vec![0; len as usize];
                file.read_exact(&mut out).unwrap();
                out
            }
        }
    }
}

/// A wanted object's spool.
struct Spool {
    size: u64,
    durable: u64,
    data: Vec<u8>,
    file: Option<(PathBuf, fs::File)>,
}

#[derive(Default)]
struct WorkerState {
    held: HashMap<String, Arc<Held>>,
    wanted: HashMap<String, Arc<Mutex<Spool>>>,
    leases: HashMap<String, (String, Instant)>,
    /// Objects that may not be served (private).
    private: HashSet<String>,
    completed: Vec<String>,
    reports: Vec<(String, String)>,
}

struct FakeWorker {
    device: String,
    instance: String,
    address: String,
    disk: Option<PathBuf>,
    state: Mutex<WorkerState>,
    faults: Mutex<Faults>,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    write_calls: AtomicU64,
    want_calls: AtomicU64,
    /// Every read: (hash, offset, len).
    read_log: Mutex<Vec<(String, u64, u64)>>,
    /// Every write item: (session, hash, offset, len).
    write_log: Mutex<Vec<(String, String, u64, u64)>>,
    conns: Mutex<Vec<Conn>>,
    stopped: AtomicBool,
}

impl FakeWorker {
    fn start(device: &str) -> Arc<FakeWorker> {
        Self::start_in(device, None)
    }

    /// A worker whose spools and installed objects live under `disk`.
    fn start_in(device: &str, disk: Option<PathBuf>) -> Arc<FakeWorker> {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("ws://{}", listener.local_addr().unwrap());
        let worker = Arc::new(FakeWorker {
            device: device.into(),
            instance: format!("worker-{}", uuid()),
            address,
            disk,
            state: Mutex::new(WorkerState::default()),
            faults: Mutex::new(Faults::default()),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            write_calls: AtomicU64::new(0),
            want_calls: AtomicU64::new(0),
            read_log: Mutex::new(Vec::new()),
            write_log: Mutex::new(Vec::new()),
            conns: Mutex::new(Vec::new()),
            stopped: AtomicBool::new(false),
        });
        let serving = worker.clone();
        thread::spawn(move || {
            for stream in listener.incoming() {
                if serving.stopped.load(Ordering::Relaxed) {
                    return;
                }
                let Ok(stream) = stream else { continue };
                let worker = serving.clone();
                thread::spawn(move || {
                    let options = ConnOptions {
                        handlers: 8,
                        handler_queue: 64,
                        ..ConnOptions::default()
                    };
                    if let Ok(conn) = session::accept_loopback(stream, None, options) {
                        conn.serve(Arc::new(WorkerHandler(worker.clone())));
                        worker.conns.lock().unwrap().push(conn);
                    }
                });
            }
        });
        worker
    }

    fn endpoint(&self) -> WorkerEndpoint {
        WorkerEndpoint {
            instance: self.instance.clone(),
            pid: 1,
            address: self.address.clone(),
            version: "test".into(),
            token: None,
        }
    }

    fn shutdown(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        for conn in self.conns.lock().unwrap().drain(..) {
            conn.close();
        }
        worker_client::drop_session(&self.instance);
        let address: SocketAddr = self.address.trim_start_matches("ws://").parse().unwrap();
        let _ = std::net::TcpStream::connect_timeout(&address, Duration::from_millis(200));
    }

    /// Holds `bytes` (servable); returns its hash.
    fn hold(&self, bytes: Vec<u8>) -> String {
        let hash = sha(&bytes);
        self.state
            .lock()
            .unwrap()
            .held
            .insert(hash.clone(), Arc::new(Held::Memory(Arc::new(bytes))));
        hash
    }

    /// Holds a file of `len` pseudo-random bytes (disk workers); its hash.
    fn hold_file(&self, seed: u64, len: u64) -> String {
        let dir = self.disk.as_ref().expect("a disk worker");
        let path = dir.join(format!("source-{seed}"));
        let mut file = fs::File::create(&path).unwrap();
        let mut hasher = Sha256::new();
        let mut written = 0u64;
        let mut block = 0u64;
        while written < len {
            let take = (len - written).min(4 * MIB);
            let bytes = content(
                seed.wrapping_mul(1_000_003).wrapping_add(block),
                take as usize,
            );
            hasher.update(&bytes);
            file.write_all(&bytes).unwrap();
            written += take;
            block += 1;
        }
        file.sync_all().unwrap();
        let hash: String = hasher
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let held = path.with_file_name(format!("held-{hash}"));
        fs::rename(&path, &held).unwrap();
        self.state
            .lock()
            .unwrap()
            .held
            .insert(hash.clone(), Arc::new(Held::File(held, len)));
        hash
    }

    /// Wants `hash` (`size` bytes), as if a committed or staged event
    /// referenced it.
    fn want(&self, hash: &str, size: u64) {
        let file = self.disk.as_ref().map(|dir| {
            let path = dir.join(format!("spool-{hash}"));
            let file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .unwrap();
            (path, file)
        });
        self.state.lock().unwrap().wanted.insert(
            hash.into(),
            Arc::new(Mutex::new(Spool {
                size,
                durable: 0,
                data: Vec::new(),
                file,
            })),
        );
    }

    fn has(&self, hash: &str) -> bool {
        self.state.lock().unwrap().held.contains_key(hash)
    }

    fn held_bytes(&self, hash: &str) -> Option<Vec<u8>> {
        let held = self.state.lock().unwrap().held.get(hash).cloned()?;
        Some(held.read(0, held.len()))
    }

    fn durable(&self, hash: &str) -> Option<u64> {
        let spool = self.state.lock().unwrap().wanted.get(hash).cloned()?;
        Some(spool.lock().unwrap().durable)
    }

    fn reports(&self) -> Vec<(String, String)> {
        self.state.lock().unwrap().reports.clone()
    }

    fn reads(&self) -> Vec<(String, u64, u64)> {
        self.read_log.lock().unwrap().clone()
    }

    fn faults(&self) -> std::sync::MutexGuard<'_, Faults> {
        self.faults.lock().unwrap()
    }

    fn route_check(&self, route: &SyncRoute) -> Result<(), ErrorWire> {
        if route.local_replica != self.device {
            return Err(ErrorWire::coded(
                "LOCAL_REPLICA_BINDING_MISMATCH",
                format!(
                    "route for {}, worker is {}",
                    route.local_replica, self.device
                ),
            ));
        }
        Ok(())
    }

    fn op_want(&self, header: Value) -> Result<(Value, Vec<u8>), ErrorWire> {
        let header: WantHeader = parse(header)?;
        self.route_check(&header.route)?;
        self.want_calls.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        // Spools are locked without the state lock (a completing write
        // holds its spool, then takes the state).
        let wanted: Vec<(String, Arc<Mutex<Spool>>)> = {
            let state = self.state.lock().unwrap();
            state
                .wanted
                .iter()
                .filter(|(hash, _)| !state.held.contains_key(*hash) && header.lane.owns(hash))
                .filter(|(hash, _)| {
                    state
                        .leases
                        .get(*hash)
                        .is_none_or(|(session, until)| *session == header.session || now >= *until)
                })
                .map(|(hash, spool)| (hash.clone(), spool.clone()))
                .collect()
        };
        let mut candidates: Vec<(u64, String, u64)> = wanted
            .into_iter()
            .map(|(hash, spool)| {
                let spool = spool.lock().unwrap();
                (spool.size, hash, spool.durable)
            })
            .collect();
        candidates.sort();
        let mut state = self.state.lock().unwrap();
        let mut items = Vec::new();
        let mut bytes = 0u64;
        for (size, hash, durable) in candidates {
            let remaining = size - durable;
            if items.len() >= header.max_items as usize
                || (!items.is_empty() && bytes + remaining > header.max_bytes)
            {
                break;
            }
            bytes += remaining;
            state.leases.insert(
                hash.clone(),
                (header.session.clone(), now + Duration::from_secs(60)),
            );
            items.push(WantItem {
                hash,
                size,
                offset: durable,
                priority: WantPriority::History,
            });
        }
        Ok((
            serde_json::to_value(WantReply { items }).unwrap(),
            Vec::new(),
        ))
    }

    fn op_read(&self, header: Value) -> Result<(Value, Vec<u8>), ErrorWire> {
        let header: ReadHeader = parse(header)?;
        self.route_check(&header.route)?;
        let held = {
            let state = self.state.lock().unwrap();
            match state.held.get(&header.hash) {
                Some(held) => held.clone(),
                None if state.private.contains(&header.hash) => {
                    return Err(ErrorWire::coded("NOT_PERMITTED", &header.hash));
                }
                None => return Err(ErrorWire::coded("NOT_AVAILABLE", &header.hash)),
            }
        };
        let total = held.len();
        if header.offset > total {
            return Err(ErrorWire::coded("INVALID_OBJECT_OFFSET", header.offset));
        }
        let len = header.len.min(total - header.offset);
        let mut bytes = held.read(header.offset, len);
        if self.faults().corrupt.contains(&header.hash)
            && let Some(first) = bytes.first_mut()
        {
            *first ^= 0xff;
        }
        self.bytes_read.fetch_add(len, Ordering::Relaxed);
        self.read_log
            .lock()
            .unwrap()
            .push((header.hash.clone(), header.offset, len));
        Ok((serde_json::to_value(ReadReply { total }).unwrap(), bytes))
    }

    fn op_write(&self, header: Value, body: Vec<u8>) -> Result<(Value, Vec<u8>), ErrorWire> {
        let header: WriteHeader = parse(header)?;
        self.route_check(&header.route)?;
        let (delay, hang) = {
            let mut faults = self.faults();
            if faults.busy_writes > 0 {
                faults.busy_writes -= 1;
                return Err(ErrorWire::coded("BUSY", "test: the handler queue is full"));
            }
            (faults.write_delay, faults.hang_next_write.take())
        };
        while self.faults().stall_writes {
            sleep_ms(10);
        }
        if let Some(delay) = delay {
            thread::sleep(delay);
        }
        if let Some(hang) = hang {
            thread::sleep(hang);
        }
        self.write_calls.fetch_add(1, Ordering::Relaxed);
        let mut at = 0usize;
        let mut results = Vec::new();
        for item in &header.items {
            let end = at + item.len as usize;
            if end > body.len() {
                return Err(ErrorWire::coded(
                    "INVALID_REQUEST",
                    "body shorter than its items",
                ));
            }
            results.push(self.write_item(&header.session, item, &body[at..end]));
            at = end;
        }
        Ok((
            serde_json::to_value(WriteReply { results }).unwrap(),
            Vec::new(),
        ))
    }

    fn write_item(&self, session: &str, item: &WriteItem, bytes: &[u8]) -> WriteResult {
        self.write_log.lock().unwrap().push((
            session.into(),
            item.hash.clone(),
            item.offset,
            item.len,
        ));
        let result = |outcome, durable_offset| WriteResult {
            hash: item.hash.clone(),
            outcome,
            durable_offset,
        };
        let spool = {
            let mut state = self.state.lock().unwrap();
            if state.held.contains_key(&item.hash) {
                return result(ObjectOutcome::Skipped, item.total);
            }
            let Some(spool) = state.wanted.get(&item.hash).cloned() else {
                return result(ObjectOutcome::Unsolicited, 0);
            };
            let now = Instant::now();
            if let Some((holder, until)) = state.leases.get(&item.hash)
                && holder != session
                && now < *until
            {
                return result(ObjectOutcome::InFlightElsewhere, 0);
            }
            state.leases.insert(
                item.hash.clone(),
                (session.into(), now + Duration::from_secs(60)),
            );
            spool
        };
        let mut spool = spool.lock().unwrap();
        if item.total != spool.size {
            return result(ObjectOutcome::SizeMismatch, spool.durable);
        }
        if item.offset != spool.durable {
            return result(
                ObjectOutcome::Failed(ErrorWire::coded(
                    "RESUME_OFFSET_AHEAD",
                    format!("append at {} but durable is {}", item.offset, spool.durable),
                )),
                spool.durable,
            );
        }
        match &mut spool.file {
            Some((_, file)) => {
                file.write_all(bytes).unwrap();
                file.sync_data().unwrap();
            }
            None => spool.data.extend_from_slice(bytes),
        }
        spool.durable += bytes.len() as u64;
        self.bytes_written
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        if spool.durable < spool.size {
            return result(
                ObjectOutcome::Progressed {
                    durable_offset: spool.durable,
                },
                spool.durable,
            );
        }
        // Complete: a full re-read hash, then install.
        let (hash, held) = match &spool.file {
            Some((path, _)) => {
                let mut file = fs::File::open(path).unwrap();
                let mut hasher = Sha256::new();
                let mut buffer = vec![0; 4 * MIB as usize];
                loop {
                    let n = file.read(&mut buffer).unwrap();
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buffer[..n]);
                }
                let hash: String = hasher
                    .finalize()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect();
                (hash, Held::File(path.clone(), spool.size))
            }
            None => (sha(&spool.data), Held::Memory(Arc::new(Vec::new()))),
        };
        let held = match held {
            Held::Memory(_) if hash == item.hash => {
                Held::Memory(Arc::new(std::mem::take(&mut spool.data)))
            }
            other => other,
        };
        if hash != item.hash {
            spool.durable = 0;
            spool.data.clear();
            if let Some((path, file)) = &mut spool.file {
                *file = fs::File::create(&*path).unwrap();
            }
            return result(ObjectOutcome::CorruptAtSource, 0);
        }
        let mut state = self.state.lock().unwrap();
        state.held.insert(item.hash.clone(), Arc::new(held));
        state.wanted.remove(&item.hash);
        state.leases.remove(&item.hash);
        state.completed.push(item.hash.clone());
        result(ObjectOutcome::Completed, spool.size)
    }

    fn op_report(&self, header: Value) -> Result<(Value, Vec<u8>), ErrorWire> {
        let header: ReportHeader = parse(header)?;
        self.route_check(&header.route)?;
        self.state
            .lock()
            .unwrap()
            .reports
            .push((header.hash, header.code));
        Ok((json!({ "action": "verified_ok" }), Vec::new()))
    }
}

fn parse<T: serde::de::DeserializeOwned>(header: Value) -> Result<T, ErrorWire> {
    serde_json::from_value(header).map_err(|error| ErrorWire::coded("INVALID_REQUEST", error))
}

struct WorkerHandler(Arc<FakeWorker>);

impl Handler for WorkerHandler {
    fn handle(&self, req: Incoming) {
        let Incoming {
            op,
            header,
            body,
            responder,
            ..
        } = req;
        let worker = &self.0;
        let result = match op.as_str() {
            "objects.want" => worker.op_want(header),
            "objects.read" => worker.op_read(header),
            "objects.write" => worker.op_write(header, body),
            "objects.report" => worker.op_report(header),
            other => Err(ErrorWire::coded("UNSUPPORTED_OPERATION", other)),
        };
        match result {
            Ok((header, body)) => responder.reply(header, body),
            Err(error) => responder.error(error),
        }
    }
}

// ---------------------------------------------------------------------------
// A daemon's side: catalog, folders, status

type Script = dyn Fn(&str, &Value) -> Result<Value, CallError> + Send + Sync;

/// A JSON worker client answering from a script (the interim path's ops).
struct ScriptClient(Box<Script>);

impl WorkerClient for ScriptClient {
    fn instance(&self) -> &str {
        "script"
    }
    fn call(&self, op: &str, payload: Value, _: Duration) -> Result<Value, CallError> {
        (self.0)(op, &payload)
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

/// One daemon's catalog, folder directory and status sink.
struct World {
    installation: String,
    catalog: Snapshot<CatalogSnapshot>,
    views: Snapshot<BTreeMap<String, Arc<FolderView>>>,
    clients: Mutex<HashMap<String, Arc<dyn WorkerClient>>>,
    events: Mutex<Vec<(Instant, StatusEvent)>>,
}

impl World {
    fn new(installation: &str, verifier: Option<Verifier>) -> Arc<World> {
        Arc::new(World {
            installation: installation.into(),
            catalog: Snapshot::new(CatalogSnapshot {
                generation: 1,
                installation: installation.into(),
                verifier,
                verifier_generation: 1,
                ..CatalogSnapshot::default()
            }),
            views: Snapshot::new(BTreeMap::new()),
            clients: Mutex::new(HashMap::new()),
            events: Mutex::new(Vec::new()),
        })
    }

    /// A Running folder of `REPO` served by `worker`.
    fn folder(&self, state: &str, device: &str, worker: &FakeWorker) {
        self.folder_of(state, REPO, device, worker);
    }

    /// A Running folder of `repo` served by `worker`.
    fn folder_of(&self, state: &str, repo: &str, device: &str, worker: &FakeWorker) {
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
        self.catalog.update(|snapshot| {
            let mut next = snapshot.clone();
            next.folders.retain(|folder| folder.state != state);
            next.folders.push(record.clone());
            next.generation += 1;
            next
        });
        let endpoint = worker.endpoint();
        let view = FolderView {
            generation: 1,
            record,
            phase: FolderPhase::Running {
                since_ms: 1,
                instance: endpoint.instance.clone(),
                pid: 1,
                address: endpoint.address.clone(),
                adopted: false,
            },
            worker: Some(endpoint.clone()),
            heartbeat: Some(Arc::new(WorkerHeartbeat {
                instance: endpoint.instance.clone(),
                seq: 1,
                generations: Generations::default(),
                tip: Some(FeedTip {
                    epoch: "epoch".into(),
                    seq: 1,
                }),
                ..WorkerHeartbeat::default()
            })),
            ..FolderView::default()
        };
        self.views.update(|views| {
            let mut views = views.clone();
            views.insert(state.into(), Arc::new(view));
            views
        });
        let client: Arc<dyn WorkerClient> = match worker_client::session_for(&endpoint) {
            Ok(client) => client,
            Err(error) => panic!("the fake worker's session: {error}"),
        };
        self.clients.lock().unwrap().insert(state.into(), client);
    }

    fn set_client(&self, state: &str, client: Arc<dyn WorkerClient>) {
        self.clients.lock().unwrap().insert(state.into(), client);
    }

    /// The folder's heartbeat `generations.sync` moves (events were staged).
    fn bump_sync(&self, state: &str) {
        self.views.update(|views| {
            let mut views = views.clone();
            let mut view = (*views[state]).clone();
            let mut heartbeat = (**view.heartbeat.as_ref().unwrap()).clone();
            heartbeat.seq += 1;
            heartbeat.generations.sync += 1;
            view.heartbeat = Some(Arc::new(heartbeat));
            view.generation += 1;
            views.insert(state.into(), Arc::new(view));
            views
        });
    }

    fn events(&self) -> Vec<StatusEvent> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .map(|(_, event)| event.clone())
            .collect()
    }

    /// `(op, direction, ok, bytes_up, bytes_down, objects_up, objects_down)`
    /// of every `ExchangeFinished`.
    fn finished(&self) -> Vec<Fin> {
        self.events()
            .into_iter()
            .filter_map(|event| match event {
                StatusEvent::ExchangeFinished {
                    op,
                    direction,
                    ok,
                    error,
                    bytes_up,
                    bytes_down,
                    objects_up,
                    objects_down,
                    duration_ms,
                    lane,
                    ..
                } => Some(Fin {
                    op,
                    direction,
                    ok,
                    error,
                    bytes_up,
                    bytes_down,
                    objects_up,
                    objects_down,
                    duration_ms,
                    lane,
                }),
                _ => None,
            })
            .collect()
    }

    fn object_errors(&self) -> Vec<(String, String)> {
        self.events()
            .into_iter()
            .filter_map(|event| match event {
                StatusEvent::ObjectError { hash, error, .. } => Some((hash, error.code)),
                _ => None,
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct Fin {
    op: String,
    direction: Direction,
    ok: bool,
    error: Option<ErrorWire>,
    bytes_up: u64,
    bytes_down: u64,
    objects_up: u64,
    objects_down: u64,
    duration_ms: u64,
    lane: u32,
}

impl CatalogApi for World {
    fn snapshot(&self) -> Arc<CatalogSnapshot> {
        self.catalog.load()
    }
    fn apply(&self, cmd: CatalogCmd) -> Result<Arc<CatalogSnapshot>, ErrorWire> {
        Err(ErrorWire::new("CATALOG_UNAVAILABLE", cmd.name()))
    }
    fn wait_newer(&self, _: u64, timeout: Duration) -> Arc<CatalogSnapshot> {
        thread::sleep(timeout.min(Duration::from_millis(50)));
        self.catalog.load()
    }
}

impl FolderDirectory for World {
    fn view(&self, state: &str) -> Option<Arc<FolderView>> {
        self.views.load().get(state).cloned()
    }
    fn by_repo(&self, repo: &str) -> Option<Arc<FolderView>> {
        self.views
            .load()
            .values()
            .find(|view| view.record.repo == repo)
            .cloned()
    }
    fn all(&self) -> Vec<Arc<FolderView>> {
        self.views.load().values().cloned().collect()
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
            .ok_or_else(|| ErrorWire::new("FOLDER_NOT_FOUND", state))
    }
    fn client(&self, state: &str) -> Option<Arc<dyn WorkerClient>> {
        self.clients.lock().unwrap().get(state).cloned()
    }
    fn generation(&self) -> u64 {
        self.views.generation()
    }
    fn wait_newer(&self, generation: u64, timeout: Duration) -> u64 {
        self.views.wait_newer(generation, timeout)
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
    fn wait_newer(&self, generation: u64, _: Duration) -> u64 {
        generation
    }
}

impl StatusSink for World {
    fn record(&self, event: StatusEvent) {
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
        installation: world.installation.clone(),
        config: Arc::new(Config {
            format_version: 2,
            data_directory: std::env::temp_dir().join("tkfs-objects-v3").join("data"),
            default_mount_directory: std::env::temp_dir().join("tkfs-objects-v3").join("mounts"),
            maximum_running: 8,
            network: Network {
                listen: None,
                connections: 2,
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
        started_at_ms: 1,
        endpoints: Snapshot::new(DaemonEndpoints::default()),
    })
}

// ---------------------------------------------------------------------------
// Two daemons: A dials B

/// The acceptor: a real inbound server with B's folder and worker.
struct Acceptor {
    world: Arc<World>,
    worker: Arc<FakeWorker>,
    server: Arc<InboundServer>,
    endpoint: String,
    code: String,
}

impl Acceptor {
    fn start(worker: Arc<FakeWorker>) -> Acceptor {
        let (code, verifier) = Verifier::generate();
        let world = World::new(&format!("b-{}", uuid()), Some(verifier));
        world.folder(STATE_B, REPLICA_B, &worker);
        let ctx = ctx(&world, 1000);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = inbound::start_with(ctx, listener, InboundOptions::default());
        Acceptor {
            world,
            worker,
            server,
            endpoint,
            code,
        }
    }

    fn installation(&self) -> String {
        self.world.installation.clone()
    }
}

impl Drop for Acceptor {
    fn drop(&mut self) {
        self.server.shutdown(Duration::from_secs(5));
        self.worker.shutdown();
    }
}

/// The dialer: A's folder and worker, and B's catalog entry.
struct Dialer {
    world: Arc<World>,
    worker: Arc<FakeWorker>,
    ctx: Arc<Ctx>,
    peer: Arc<PeerContext>,
}

impl Dialer {
    fn start(worker: Arc<FakeWorker>, remote: &str, endpoint: &str, interval_ms: u64) -> Dialer {
        let world = World::new(&format!("a-{}", uuid()), None);
        world.folder(STATE_A, REPLICA_A, &worker);
        let ctx = ctx(&world, interval_ms);
        let peer = Arc::new(PeerContext {
            installation: remote.into(),
            endpoint: endpoint.into(),
            local_installation: ctx.installation.clone(),
            pool: 2,
            catalog: Snapshot::new(RemoteCatalog {
                generation: 1,
                at_ms: 0,
                entries: vec![entry()],
            }),
        });
        Dialer {
            world,
            worker,
            ctx,
            peer,
        }
    }

    /// A bulk session of lane `lane` to `endpoint`, authenticated with `code`.
    fn dial(&self, endpoint: &str, code: &str, lane: LaneSpec) -> Conn {
        let hello = session::client_hello(&self.ctx.installation, LaneRole::Bulk, Some(lane), 2);
        let (conn, server) = session::dial(
            endpoint,
            hello,
            &CodeAuth { code: code.into() },
            None,
            ConnOptions::for_role(LaneRole::Bulk),
        )
        .unwrap();
        assert_eq!(server.installation, self.peer.installation);
        conn
    }

    fn options(&self) -> (LaneOptions, Arc<LaneStats>) {
        let mut options = LaneOptions::for_config(&self.ctx.config);
        let stats = Arc::new(LaneStats::default());
        options.stats = Some(stats.clone());
        (options, stats)
    }

    fn run(&self, conn: Conn, options: LaneOptions) -> RunningLane {
        let cancel = Cancel::new();
        let handle = {
            let (ctx, peer, conn, cancel) = (
                self.ctx.clone(),
                self.peer.clone(),
                conn.clone(),
                cancel.clone(),
            );
            thread::spawn(move || objects::run_lane_with(ctx, peer, LANE, conn, cancel, options))
        };
        RunningLane {
            cancel,
            conn,
            handle: Some(handle),
        }
    }
}

impl Drop for Dialer {
    fn drop(&mut self) {
        self.worker.shutdown();
    }
}

fn entry() -> PublishedEntry {
    entry_of(REPO, REPLICA_B)
}

fn entry_of(repo: &str, replica: &str) -> PublishedEntry {
    PublishedEntry {
        repo: repo.into(),
        replica: replica.into(),
        label: "remote".into(),
        branches: vec![],
        state: EntryState::Running,
        tip: Some(FeedTip {
            epoch: "remote-epoch".into(),
            seq: 1,
        }),
        sync_generation: 0,
        content: None,
        error: None,
        updated_at_ms: 0,
    }
}

/// A lane running on its own thread; cancelled, closed and joined on drop.
struct RunningLane {
    cancel: Cancel,
    conn: Conn,
    handle: Option<thread::JoinHandle<()>>,
}

impl RunningLane {
    fn stop(&mut self) -> Duration {
        let began = Instant::now();
        self.cancel.cancel();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        self.conn.close();
        began.elapsed()
    }

    fn finished(&self) -> bool {
        self.handle
            .as_ref()
            .is_none_or(|handle| handle.is_finished())
    }
}

impl Drop for RunningLane {
    fn drop(&mut self) {
        self.stop();
    }
}

/// A and B with fresh in-memory workers.
fn pair(interval_ms: u64) -> (Dialer, Acceptor) {
    let acceptor = Acceptor::start(FakeWorker::start(REPLICA_B));
    let dialer = Dialer::start(
        FakeWorker::start(REPLICA_A),
        &acceptor.installation(),
        &acceptor.endpoint,
        interval_ms,
    );
    (dialer, acceptor)
}

// ---------------------------------------------------------------------------
// Acceptance

/// §14 WP3.3: both directions complete with per-object results; one
/// `CorruptAtSource` object does not stop the others (it is reported to the
/// source and backs off); `ExchangeFinished` per pull and push with real
/// bytes, completed objects and durations.
#[test]
fn both_directions_complete_with_per_object_results() {
    let (a, b) = pair(100);
    // B → A: five objects of mixed sizes, one of them corrupt at B, plus an
    // empty one; A → B: three objects.
    let sizes = [3 * MIB as usize + 17, 4096, 1, 700 * 1024, 2 * MIB as usize];
    let mut down = Vec::new();
    for (index, size) in sizes.iter().enumerate() {
        let hash = b.worker.hold(content(index as u64 + 1, *size));
        a.worker.want(&hash, *size as u64);
        down.push(hash);
    }
    let empty = b.worker.hold(Vec::new());
    a.worker.want(&empty, 0);
    let corrupt = down[3].clone();
    b.worker.faults().corrupt.insert(corrupt.clone());
    let mut up = Vec::new();
    for (index, size) in [5 * MIB as usize, 10, 300_000].iter().enumerate() {
        let hash = a.worker.hold(content(100 + index as u64, *size));
        b.worker.want(&hash, *size as u64);
        up.push(hash);
    }

    let conn = a.dial(&b.endpoint, &b.code, LANE);
    let (options, stats) = a.options();
    let mut lane = a.run(conn, options);
    let complete = || {
        down.iter()
            .filter(|hash| **hash != corrupt)
            .all(|hash| a.worker.has(hash))
            && a.worker.has(&empty)
            && up.iter().all(|hash| b.worker.has(hash))
    };
    assert!(
        wait_until(Duration::from_secs(30), complete),
        "not complete: down {:?}, up {:?}",
        down.iter().map(|h| a.worker.has(h)).collect::<Vec<_>>(),
        up.iter().map(|h| b.worker.has(h)).collect::<Vec<_>>()
    );
    // The bytes are the source's.
    for (index, hash) in down.iter().enumerate() {
        if *hash != corrupt {
            assert_eq!(
                a.worker.held_bytes(hash).unwrap(),
                content(index as u64 + 1, sizes[index])
            );
        }
    }
    // The corrupt object: never installed, reported to B, an ObjectError.
    assert!(
        wait_until(Duration::from_secs(10), || b
            .worker
            .reports()
            .contains(&(corrupt.clone(), "CORRUPT_AT_SOURCE".into()))),
        "{:?}",
        b.worker.reports()
    );
    assert!(!a.worker.has(&corrupt));
    assert!(
        a.world
            .object_errors()
            .contains(&(corrupt.clone(), "CORRUPT_AT_SOURCE".into())),
        "{:?}",
        a.world.object_errors()
    );
    lane.stop();

    // Status: pulls and pushes with completed objects and bytes.
    let finished = a.world.finished();
    let pulls: Vec<&Fin> = finished.iter().filter(|f| f.op == "objects.pull").collect();
    let pushes: Vec<&Fin> = finished.iter().filter(|f| f.op == "objects.push").collect();
    assert!(!pulls.is_empty() && !pushes.is_empty(), "{finished:?}");
    assert!(
        finished
            .iter()
            .all(|f| f.lane == 1 && f.direction == Direction::Outbound)
    );
    let down_objects: u64 = pulls.iter().filter(|f| f.ok).map(|f| f.objects_down).sum();
    let up_objects: u64 = pushes.iter().filter(|f| f.ok).map(|f| f.objects_up).sum();
    assert_eq!(
        down_objects, 5,
        "4 good objects and the empty one: {pulls:?}"
    );
    assert_eq!(up_objects, 3, "{pushes:?}");
    let down_bytes: u64 = pulls.iter().map(|f| f.bytes_down).sum();
    let good: u64 = sizes.iter().map(|size| *size as u64).sum::<u64>();
    assert!(down_bytes >= good, "{down_bytes} < {good}");
    let up_bytes: u64 = pushes.iter().map(|f| f.bytes_up).sum();
    assert_eq!(up_bytes, 5 * MIB + 10 + 300_000);
    assert!(pulls.iter().all(|f| f.bytes_up == 0 && f.objects_up == 0));
    assert!(
        pushes
            .iter()
            .all(|f| f.bytes_down == 0 && f.objects_down == 0)
    );
    assert_eq!(LaneStats::get(&stats.objects_down), 5);
    assert_eq!(LaneStats::get(&stats.objects_up), 3);
    assert!(LaneStats::get(&stats.reports) >= 1);
    assert_eq!(LaneStats::get(&stats.interim_exchanges), 0);

    // B served them: inbound pulls and pushes on its lane 1.
    let served = b.world.finished();
    assert!(
        served
            .iter()
            .any(|f| f.op == "objects.pull" && f.direction == Direction::Inbound && f.bytes_up > 0)
    );
    let pushed: u64 = served
        .iter()
        .filter(|f| f.op == "objects.push" && f.direction == Direction::Inbound)
        .map(|f| f.objects_down)
        .sum();
    assert_eq!(pushed, 3);
}

/// A push whose bytes fail the peer's hash check (`CorruptAtSource` in the
/// peer's results) makes our worker re-verify its copy (`objects.report`);
/// the object backs off on both sides instead of being pushed in a loop,
/// and the other objects of the push complete.
#[test]
fn a_corrupt_push_is_reported_to_our_worker_and_backs_off() {
    let (a, b) = pair(100);
    let bad = a.worker.hold(content(60, 300_000));
    let good = a.worker.hold(content(61, 200_000));
    a.worker.faults().corrupt.insert(bad.clone());
    b.worker.want(&bad, 300_000);
    b.worker.want(&good, 200_000);
    let conn = a.dial(&b.endpoint, &b.code, LANE);
    let (options, _) = a.options();
    let lane = a.run(conn, options);
    assert!(wait_until(Duration::from_secs(20), || b.worker.has(&good)));
    assert!(
        wait_until(Duration::from_secs(10), || a
            .worker
            .reports()
            .contains(&(bad.clone(), "CORRUPT_AT_SOURCE".into()))),
        "our worker was not asked to re-verify: {:?}",
        a.worker.reports()
    );
    // Rounds keep running (interval 100 ms), but the object is not pushed
    // again within its backoff.
    sleep_ms(1500);
    drop(lane);
    assert!(!b.worker.has(&bad));
    let completing_writes = b
        .worker
        .write_log
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, hash, offset, len)| *hash == bad && offset + len == 300_000)
        .count();
    assert_eq!(completing_writes, 1, "the corrupt object was pushed again");
    assert!(
        a.world
            .object_errors()
            .contains(&(bad.clone(), "CORRUPT_AT_SOURCE".into())),
        "{:?}",
        a.world.object_errors()
    );
    assert!(
        b.world
            .object_errors()
            .contains(&(bad.clone(), "CORRUPT_AT_SOURCE".into())),
        "{:?}",
        b.world.object_errors()
    );
}

/// A zero-size object is one empty chunk, pulled or pushed.
#[test]
fn zero_size_objects_travel_both_ways() {
    for up in [false, true] {
        let (a, b) = pair(100);
        let (holder, wanter) = if up {
            (&a.worker, &b.worker)
        } else {
            (&b.worker, &a.worker)
        };
        let empty = holder.hold(Vec::new());
        let small = holder.hold(content(50, 3));
        wanter.want(&empty, 0);
        wanter.want(&small, 3);
        let conn = a.dial(&b.endpoint, &b.code, LANE);
        let (options, _) = a.options();
        let lane = a.run(conn, options);
        assert!(
            wait_until(Duration::from_secs(20), || wanter.has(&empty)
                && wanter.has(&small)),
            "up: {up}"
        );
        assert_eq!(wanter.held_bytes(&empty).unwrap(), Vec::<u8>::new());
        drop(lane);
    }
}

/// Invariant 18: object bytes never travel on the control session, and
/// `objects.*` on a control session is refused.
#[test]
fn control_carries_no_object_bytes() {
    let (a, b) = pair(100);
    let mut hashes = Vec::new();
    for index in 0..4 {
        let hash = b.worker.hold(content(200 + index, 2 * MIB as usize));
        a.worker.want(&hash, 2 * MIB);
        hashes.push(hash);
    }
    let hello = session::client_hello(&a.ctx.installation, LaneRole::Control, None, 2);
    let (control, _) = session::dial(
        &b.endpoint,
        hello,
        &CodeAuth {
            code: b.code.clone(),
        },
        None,
        ConnOptions::default(),
    )
    .unwrap();
    let before = control.health();
    let bulk = a.dial(&b.endpoint, &b.code, LANE);
    let (options, _) = a.options();
    let lane = a.run(bulk.clone(), options);
    assert!(wait_until(Duration::from_secs(30), || hashes
        .iter()
        .all(|hash| a.worker.has(hash))));
    let control_bytes = {
        let now = control.health();
        (now.bytes_up - before.bytes_up) + (now.bytes_down - before.bytes_down)
    };
    let bulk_bytes = bulk.health().bytes_down;
    assert!(bulk_bytes >= 8 * MIB, "bulk carried {bulk_bytes}");
    assert!(control_bytes < 64 * 1024, "control carried {control_bytes}");
    drop(lane);
    // The acceptor refuses objects ops on a control session.
    let pull = ObjectsPull {
        repo: REPO.into(),
        sender: REPLICA_A.into(),
        receiver: REPLICA_B.into(),
        lane: LANE,
        items: vec![],
        budget_bytes: MIB,
    };
    let error = control
        .call(
            "objects.pull",
            serde_json::to_value(&pull).unwrap(),
            Vec::new(),
            Priority::Control,
            Duration::from_secs(5),
        )
        .unwrap_err();
    assert_eq!(error.code(), "BULK_ON_CONTROL_LANE");
    control.close();
}

/// §14 WP3.3: a stalled push does not block pulls on the same lane.
#[test]
fn a_stalled_push_does_not_block_pulls_on_the_same_lane() {
    let (a, b) = pair(100);
    let up = a.worker.hold(content(300, 4 * MIB as usize));
    b.worker.want(&up, 4 * MIB);
    // B's worker stalls every write: A's push cannot finish.
    b.worker.faults().stall_writes = true;
    let conn = a.dial(&b.endpoint, &b.code, LANE);
    let (options, _) = a.options();
    let lane = a.run(conn, options);
    assert!(wait_until(Duration::from_secs(10), || a
        .world
        .events()
        .iter()
        .any(
            |event| matches!(event, StatusEvent::ExchangeStarted { op, .. } if op == "objects.push")
        )));
    // Meanwhile A pulls six objects from B on the same lane.
    let mut down = Vec::new();
    for index in 0..6 {
        let hash = b.worker.hold(content(310 + index, 1536 * 1024));
        a.worker.want(&hash, 1536 * 1024);
        down.push(hash);
    }
    a.world.bump_sync(STATE_A);
    assert!(
        wait_until(Duration::from_secs(20), || down
            .iter()
            .all(|hash| a.worker.has(hash))),
        "the pulls waited behind the stalled push"
    );
    assert!(!b.worker.has(&up), "the push was not stalled");
    assert!(
        !a.world.finished().iter().any(|f| f.op == "objects.push"),
        "the push ended before the pulls did"
    );
    // Once B's worker writes again, the push completes.
    b.worker.faults().stall_writes = false;
    assert!(wait_until(Duration::from_secs(30), || b.worker.has(&up)));
    drop(lane);
}

/// §14 WP3.3: ≤ 16 MiB in flight per lane (measured): object bytes the
/// source has read for this lane's pulls and the local worker has not yet
/// written durably never exceed the window, whether the local disk or the
/// link is the bottleneck. When the link is, the lane re-pulls at half the
/// window, so two pulls are in flight.
#[test]
fn at_most_16_mib_in_flight_per_lane() {
    for link_bound in [false, true] {
        let (a, b) = pair(100);
        let (count, size) = if link_bound {
            (3, 8 * MIB)
        } else {
            (3, 24 * MIB)
        };
        let mut hashes = Vec::new();
        for index in 0..count {
            let hash = b.worker.hold(content(400 + index, size as usize));
            a.worker.want(&hash, size);
            hashes.push(hash);
        }
        let proxy = FaultProxy::start(&b.endpoint);
        if link_bound {
            proxy.set_latency(Duration::from_millis(2));
            proxy.set_bandwidth(20 * MIB);
        } else {
            // A slow local disk: the window, not the link, sets the pace.
            a.worker.faults().write_delay = Some(Duration::from_millis(3));
        }
        let conn = a.dial(&proxy.endpoint(), &b.code, LANE);
        let (options, stats) = a.options();
        let lane = a.run(conn, options);
        let mut most = 0u64;
        let done = wait_until(Duration::from_secs(60), || {
            let read = b.worker.bytes_read.load(Ordering::Relaxed);
            let written = a.worker.bytes_written.load(Ordering::Relaxed);
            most = most.max(read.saturating_sub(written));
            hashes.iter().all(|hash| a.worker.has(hash))
        });
        assert!(
            done,
            "the transfer did not finish (link bound: {link_bound})"
        );
        drop(lane);
        assert!(most <= 16 * MIB, "{most} bytes in flight");
        let peak = LaneStats::get(&stats.window_peak);
        assert!(peak <= 16 * MIB, "window peak {peak}");
        assert!(most >= 8 * MIB, "the window was not used: {most}");
        assert!(LaneStats::get(&stats.peer_calls_peak) <= objects::PEER_CALLS as u64);
        if link_bound {
            assert_eq!(
                LaneStats::get(&stats.pulls_peak),
                2,
                "pulls were never pipelined"
            );
        }
    }
}

/// §14 WP3.3: after a connection loss the transfer resumes from the
/// worker's durable offset: the next pull starts exactly there (no blind
/// replay of what was durable, nothing skipped), and the object completes
/// with the source's bytes.
#[test]
fn a_connection_loss_resumes_from_the_durable_offset() {
    let (a, b) = pair(100);
    let size = 40 * MIB;
    let bytes = content(500, size as usize);
    let hash = b.worker.hold(bytes.clone());
    a.worker.want(&hash, size);
    a.worker.faults().write_delay = Some(Duration::from_millis(2));
    let proxy = FaultProxy::start(&b.endpoint);
    proxy.reset_after(12 * MIB);
    let conn = a.dial(&proxy.endpoint(), &b.code, LANE);
    let (options, _) = a.options();
    let mut first = a.run(conn, options);
    assert!(
        wait_until(Duration::from_secs(30), || first.finished()),
        "the lane did not end with its session"
    );
    first.stop();
    let durable = a.worker.durable(&hash).expect("still wanted");
    assert!(durable > 0 && durable < size, "durable {durable}");
    let reads_before = b.worker.reads().len();

    // A new session: the first read B serves starts at the durable offset.
    proxy.clear();
    let conn = a.dial(&proxy.endpoint(), &b.code, LANE);
    let (options, _) = a.options();
    let second = a.run(conn, options);
    assert!(wait_until(Duration::from_secs(30), || a.worker.has(&hash)));
    drop(second);
    // (B reads up to 4 ranges at once: its log is not in offset order.)
    let resumed = &b.worker.reads()[reads_before..];
    assert_eq!(
        resumed.iter().map(|(_, offset, _)| *offset).min(),
        Some(durable),
        "{:?}",
        &resumed[..resumed.len().min(4)]
    );
    assert_eq!(a.worker.held_bytes(&hash).unwrap(), bytes);
    // The writes never went back below what was durable.
    let mut end = 0;
    for (_, written, offset, len) in a.worker.write_log.lock().unwrap().iter() {
        if *written == hash {
            assert_eq!(*offset, end, "a write that is not an append");
            end += len;
        }
    }
    assert_eq!(end, size);
}

/// Objects of staged (not yet activated) events flow at once: staging moves
/// the heartbeat's `generations.sync` (not the tip, not the content
/// counters), and that alone starts a download, with no anti-entropy round
/// to wait for (the interval here is 60 s).
#[test]
fn staging_alone_starts_a_download() {
    let (a, b) = pair(60_000);
    let conn = a.dial(&b.endpoint, &b.code, LANE);
    let (options, _) = a.options();
    let lane = a.run(conn, options);
    // The first rounds find nothing to do.
    assert!(wait_until(Duration::from_secs(5), || a
        .worker
        .want_calls
        .load(Ordering::Relaxed)
        > 0));
    sleep_ms(300);
    let hash = b.worker.hold(content(600, 256 * 1024));
    a.worker.want(&hash, 256 * 1024);
    let staged = Instant::now();
    a.world.bump_sync(STATE_A);
    assert!(
        wait_until(Duration::from_secs(5), || a.worker.has(&hash)),
        "a staged event's object waited for anti-entropy"
    );
    assert!(staged.elapsed() < Duration::from_secs(5));
    drop(lane);
}

/// The session client: a refused request (`BUSY`) is sent again; an
/// uncertain one (no reply within its deadline) is never replayed
/// (invariant 16).
#[test]
fn the_session_client_retries_refusals_and_never_replays_uncertain_calls() {
    let worker = FakeWorker::start(REPLICA_A);
    let hash = sha(b"hello");
    worker.want(&hash, 5);
    let client = SessionWorkerClient::connect(&worker.endpoint()).unwrap();
    let route = SyncRoute {
        peer: "peer".into(),
        repo: REPO.into(),
        local_replica: REPLICA_A.into(),
        remote_replica: REPLICA_B.into(),
    };
    let item = |offset: u64, len: u64| WriteItem {
        hash: hash.clone(),
        offset,
        total: 5,
        len,
        last: offset + len == 5,
    };
    worker.faults().busy_writes = 2;
    let results = client
        .write_batch(
            &route,
            "session",
            vec![item(0, 2)],
            b"he".to_vec(),
            Duration::from_secs(10),
        )
        .unwrap();
    assert_eq!(
        results[0].outcome,
        ObjectOutcome::Progressed { durable_offset: 2 }
    );
    assert_eq!(worker.write_calls.load(Ordering::Relaxed), 1);
    // Uncertain: the worker answers after the deadline.
    worker.faults().hang_next_write = Some(Duration::from_millis(1500));
    let error = client
        .write_batch(
            &route,
            "session",
            vec![item(2, 3)],
            b"llo".to_vec(),
            Duration::from_millis(400),
        )
        .unwrap_err();
    assert!(error.is_uncertain(), "{error:?}");
    sleep_ms(1600);
    assert_eq!(
        worker.write_calls.load(Ordering::Relaxed),
        2,
        "the uncertain write was sent again"
    );
    // The late reply was applied by the worker: the object is complete.
    assert!(worker.has(&hash));
    worker.shutdown();
}

/// `session_for` remembers a worker without loopback sessions (a Wave 2
/// worker): `UNSUPPORTED_OPERATION`, and no second dial.
#[test]
fn a_worker_without_loopback_sessions_is_remembered() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("ws://{}", listener.local_addr().unwrap());
    let accepted = Arc::new(AtomicUsize::new(0));
    {
        let accepted = accepted.clone();
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                accepted.fetch_add(1, Ordering::SeqCst);
                // A plain (pre-v3) WebSocket endpoint: no `X-Tkfs-Sync` echo.
                thread::spawn(move || {
                    if let Ok(mut socket) = tkfs::ws::accept(stream) {
                        let _ = tkfs::ws::receive::<Value>(&mut socket);
                    }
                });
            }
        });
    }
    let endpoint = WorkerEndpoint {
        instance: format!("legacy-{}", uuid()),
        pid: 1,
        address,
        version: "6298d0b".into(),
        token: None,
    };
    let error = worker_client::session_for(&endpoint).unwrap_err();
    assert_eq!(error.code(), "UNSUPPORTED_OPERATION", "{error}");
    let error = worker_client::session_for(&endpoint).unwrap_err();
    assert_eq!(error.code(), "UNSUPPORTED_OPERATION");
    assert_eq!(accepted.load(Ordering::SeqCst), 1, "dialed again");
    // A JSON client's binary ops say why.
    let json = tkfs::orchestrator::worker_client::JsonWorkerClient::new(endpoint);
    let error = json
        .call_bytes("objects.read", json!({}), b"", Duration::from_secs(1))
        .unwrap_err();
    assert_eq!(error.code(), "BINARY_OPS_REQUIRE_SESSION");
}

/// The acceptor's checks keep their codes and never reach the worker; per
/// item: a hash another lane owns is `OBJECT_ON_WRONG_LANE`, a hash-only
/// request for an object the worker does not serve gets no bytes.
#[test]
fn inbound_checks_keep_their_codes() {
    let (a, b) = pair(100);
    let lane_of_two = LaneSpec::new(1, 2);
    let held = b.worker.hold(content(700, 2048));
    let private = sha(b"private");
    b.worker
        .state
        .lock()
        .unwrap()
        .private
        .insert(private.clone());
    let conn = a.dial(&b.endpoint, &b.code, lane_of_two);
    let pull = |repo: &str, receiver: &str, lane: LaneSpec, items: Vec<WantItem>| {
        let request = ObjectsPull {
            repo: repo.into(),
            sender: REPLICA_A.into(),
            receiver: receiver.into(),
            lane,
            items,
            budget_bytes: MIB,
        };
        let mut chunks = Vec::new();
        conn.call_streaming(
            "objects.pull",
            serde_json::to_value(&request).unwrap(),
            Vec::new(),
            Priority::Control,
            None,
            Duration::from_secs(10),
            &mut |chunk: ChunkHeader, bytes: &[u8]| {
                chunks.push((chunk, bytes.len()));
                Ok(())
            },
        )
        .map(|reply| {
            let result: ObjectsResult = if reply.body.is_empty() {
                serde_json::from_value(reply.header).unwrap()
            } else {
                serde_json::from_slice(&reply.body).unwrap()
            };
            (result, chunks)
        })
    };
    let item = |hash: &str, size: u64| WantItem {
        hash: hash.into(),
        size,
        offset: 0,
        priority: WantPriority::Demand,
    };
    let code = |result: Result<(ObjectsResult, Vec<(ChunkHeader, usize)>), CallError>| {
        result.unwrap_err().code()
    };
    let reads = b.worker.reads().len();
    assert_eq!(
        code(pull(REPO, REPLICA_B, LANE, vec![])),
        "BULK_LANE_MISMATCH"
    );
    assert_eq!(
        code(pull("other-repo", REPLICA_B, lane_of_two, vec![])),
        "REPOSITORY_NOT_MANAGED"
    );
    assert_eq!(
        code(pull(REPO, "someone-else", lane_of_two, vec![])),
        "LOCAL_REPLICA_BINDING_MISMATCH"
    );
    let malformed = conn
        .call(
            "objects.pull",
            json!({ "repo": REPO }),
            Vec::new(),
            Priority::Control,
            Duration::from_secs(5),
        )
        .unwrap_err();
    assert_eq!(malformed.code(), "UNSUPPORTED_OPERATION");
    assert_eq!(
        b.worker.reads().len(),
        reads,
        "a refusal reached the worker"
    );

    // Per item.
    let mine = (0..64u64)
        .map(|seed| content(800 + seed, 64))
        .map(|bytes| (sha(&bytes), bytes))
        .find(|(hash, _)| lane_of_two.owns(hash))
        .unwrap();
    let mine_hash = b.worker.hold(mine.1.clone());
    let wrong_lane = if lane_of_two.owns(&held) {
        (0..64u64)
            .map(|seed| b.worker.hold(content(900 + seed, 64)))
            .find(|hash| !lane_of_two.owns(hash))
            .unwrap()
    } else {
        held.clone()
    };
    let missing = (0..64u64)
        .map(|seed| sha(&content(1000 + seed, 8)))
        .find(|hash| lane_of_two.owns(hash))
        .unwrap();
    let private_owned = lane_of_two.owns(&private);
    let mut items = vec![
        item(&wrong_lane, 64),
        item(&missing, 8),
        item(&mine_hash, 64),
    ];
    if private_owned {
        items.push(item(&private, 7));
    }
    let (result, chunks) = pull(REPO, REPLICA_B, lane_of_two, items).unwrap();
    let outcome = |hash: &str| {
        result
            .results
            .iter()
            .find(|result| result.hash == hash)
            .unwrap()
            .outcome
            .clone()
    };
    assert_eq!(outcome(&wrong_lane).code(), Some("OBJECT_ON_WRONG_LANE"));
    assert_eq!(outcome(&missing), ObjectOutcome::NotAvailable);
    assert_eq!(outcome(&mine_hash), ObjectOutcome::Completed);
    if private_owned {
        assert_eq!(outcome(&private), ObjectOutcome::NotPermitted);
    }
    assert_eq!(
        chunks.len(),
        1,
        "only the served object streams: {chunks:?}"
    );
    assert_eq!(chunks[0].0.hash.as_deref(), Some(mine_hash.as_str()));
    assert!(chunks[0].0.last);
    conn.close();
}

/// A peer that answers `UNSUPPORTED_OPERATION` to objects v3 (a Wave 2
/// build) gets the interim `objects.exchange` instead, and the lane does
/// not try v3 again until the key resets.
#[test]
fn a_peer_without_objects_v3_gets_the_interim_path() {
    // B: a scripted v3 server whose objects ops are the interim ones only.
    struct InterimPeer {
        v3_calls: AtomicUsize,
        exchanges: AtomicUsize,
    }
    impl Handler for InterimPeer {
        fn handle(&self, req: Incoming) {
            match req.op.as_str() {
                "objects.exchange" => {
                    self.exchanges.fetch_add(1, Ordering::SeqCst);
                    let page: Page = serde_json::from_slice(&req.body).unwrap();
                    let mut reply = page.clone();
                    reply.sender = page.receiver.clone();
                    reply.receiver = page.sender.clone();
                    reply.reply_to = Some(page.request.clone());
                    reply.request = uuid();
                    reply.object_parts.clear();
                    req.responder
                        .reply(json!({ "repo": REPO }), serde_json::to_vec(&reply).unwrap());
                }
                op => {
                    self.v3_calls.fetch_add(1, Ordering::SeqCst);
                    req.responder.error(ErrorWire::coded(
                        "UNSUPPORTED_OPERATION",
                        format!("{op} is served from objects v3 (Wave 3)"),
                    ));
                }
            }
        }
    }
    struct Policy(String, Verifier);
    impl AcceptPolicy for Policy {
        fn installation(&self) -> String {
            self.0.clone()
        }
        fn verifier(&self) -> Option<(Verifier, u64)> {
            Some((self.1.clone(), 1))
        }
        fn admit(&self, _: &ClientHello) -> Result<(), ErrorWire> {
            Ok(())
        }
        fn legacy_attempt(&self, _: SocketAddr, _: &Value) {}
    }
    let remote = format!("b-{}", uuid());
    let (code, verifier) = Verifier::generate();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let peer = Arc::new(InterimPeer {
        v3_calls: AtomicUsize::new(0),
        exchanges: AtomicUsize::new(0),
    });
    let server = {
        let (peer, remote) = (peer.clone(), remote.clone());
        thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let (conn, _) = session::accept(
                stream,
                Arc::new(Policy(remote, verifier)),
                ConnOptions::for_role(LaneRole::Bulk),
            )
            .unwrap();
            conn.serve(peer);
            conn
        })
    };
    let a = Dialer::start(FakeWorker::start(REPLICA_A), &remote, &endpoint, 100);
    // A's JSON worker client plays the interim worker ops.
    a.world.set_client(
        STATE_A,
        Arc::new(ScriptClient(Box::new(|op, payload| match op {
            "published-bulk-offer" => {
                let route: SyncRoute = serde_json::from_value(payload["route"].clone()).unwrap();
                let page = Page {
                    format: bulk_sync::PROTOCOL,
                    repo: route.repo.clone(),
                    sender: route.local_replica.clone(),
                    receiver: route.remote_replica.clone(),
                    request: uuid(),
                    reply_to: None,
                    lane: objects::page_lane(LANE),
                    known_objects: BTreeSet::new(),
                    partial_objects: BTreeMap::new(),
                    wanted_objects: BTreeSet::new(),
                    object_parts: BTreeMap::new(),
                };
                Ok(serde_json::to_value(page).unwrap())
            }
            "published-bulk-apply" => Ok(json!({})),
            other => Err(CallError::Remote(ErrorWire::coded(
                "UNSUPPORTED_OPERATION",
                other,
            ))),
        }))),
    );
    // Something to want, so the download round reaches the peer.
    a.worker.want(&sha(b"wanted"), 6);
    let conn = a.dial(&endpoint, &code, LANE);
    let (options, stats) = a.options();
    let lane = a.run(conn, options);
    assert!(
        wait_until(Duration::from_secs(10), || peer
            .exchanges
            .load(Ordering::SeqCst)
            >= 3),
        "no interim exchanges"
    );
    let v3 = peer.v3_calls.load(Ordering::SeqCst);
    sleep_ms(500);
    assert_eq!(
        peer.v3_calls.load(Ordering::SeqCst),
        v3,
        "v3 was tried again on every round"
    );
    assert!(v3 <= 2, "{v3} v3 calls before the fallback");
    assert!(LaneStats::get(&stats.interim_exchanges) >= 3);
    assert!(
        a.world
            .finished()
            .iter()
            .any(|f| f.op == "objects.exchange" && f.ok)
    );
    drop(lane);
    server.join().unwrap().close();
}

/// §14 WP3.3 task 3: at most 2 repositories share a lane at once (both
/// directions of one count once); a third one waits its turn and completes.
#[test]
fn at_most_two_repositories_share_a_lane() {
    let (code, verifier) = Verifier::generate();
    let b_world = World::new(&format!("b-{}", uuid()), Some(verifier));
    let a_world = World::new(&format!("a-{}", uuid()), None);
    let mut workers = Vec::new();
    let mut entries = Vec::new();
    let mut wanted = Vec::new();
    for index in 0..3u64 {
        let repo = format!("repo-{index}");
        let (replica_a, replica_b) = (format!("replica-a-{index}"), format!("replica-b-{index}"));
        let worker_a = FakeWorker::start(&replica_a);
        let worker_b = FakeWorker::start(&replica_b);
        // A slow local disk: rounds last long enough to overlap.
        worker_a.faults().write_delay = Some(Duration::from_millis(20));
        a_world.folder_of(&format!("state-a-{index}"), &repo, &replica_a, &worker_a);
        b_world.folder_of(&format!("state-b-{index}"), &repo, &replica_b, &worker_b);
        for object in 0..6 {
            let hash = worker_b.hold(content(2000 + index * 100 + object, 512 * 1024));
            worker_a.want(&hash, 512 * 1024);
            wanted.push((worker_a.clone(), hash));
        }
        entries.push(entry_of(&repo, &replica_b));
        workers.push((worker_a, worker_b));
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let server = inbound::start_with(ctx(&b_world, 1000), listener, InboundOptions::default());
    let a_ctx = ctx(&a_world, 100);
    let peer = Arc::new(PeerContext {
        installation: b_world.installation.clone(),
        endpoint: endpoint.clone(),
        local_installation: a_ctx.installation.clone(),
        pool: 2,
        catalog: Snapshot::new(RemoteCatalog {
            generation: 1,
            at_ms: 0,
            entries,
        }),
    });
    let dialer = Dialer {
        world: a_world.clone(),
        worker: workers[0].0.clone(),
        ctx: a_ctx,
        peer,
    };
    let conn = dialer.dial(&endpoint, &code, LANE);
    let (mut options, stats) = dialer.options();
    options.round = Duration::from_millis(500);
    let lane = dialer.run(conn, options);
    assert!(
        wait_until(Duration::from_secs(60), || wanted
            .iter()
            .all(|(worker, hash)| worker.has(hash))),
        "the repositories did not all complete"
    );
    drop(lane);
    assert_eq!(
        LaneStats::get(&stats.repos_peak),
        2,
        "two repositories at once"
    );
    server.shutdown(Duration::from_secs(5));
    for (a, b) in workers {
        a.shutdown();
        b.shutdown();
    }
}

// ---------------------------------------------------------------------------
// Qualification (release, ignored): throughput of the data plane

/// Two daemons whose workers keep their spools on disk (one `sync_data`
/// per written object, a full re-read SHA-256 on completion), or in memory
/// (`root: None`: the orchestrator's data plane alone).
fn qual_pair(root: Option<&Path>) -> (Dialer, Acceptor) {
    let disk = |name: &str| {
        root.map(|root| {
            let dir = root.join(name);
            fs::create_dir_all(&dir).unwrap();
            dir
        })
    };
    let acceptor = Acceptor::start(FakeWorker::start_in(REPLICA_B, disk("b")));
    let dialer = Dialer::start(
        FakeWorker::start_in(REPLICA_A, disk("a")),
        &acceptor.installation(),
        &acceptor.endpoint,
        500,
    );
    (dialer, acceptor)
}

struct Link {
    rtt_ms: u64,
    bytes_per_s: u64,
}

/// Moves `objects` (B → A) over `link` (through a FaultProxy unless
/// `None`); returns the seconds from the lane's start to the last install,
/// and to the last byte written (before the worker's final verification).
fn transfer(
    a: &Dialer,
    b: &Acceptor,
    hashes: &[String],
    total: u64,
    link: Option<&Link>,
) -> (f64, f64, Arc<LaneStats>) {
    let proxy = link.map(|link| {
        let proxy = FaultProxy::start(&b.endpoint);
        proxy.set_latency(Duration::from_millis(link.rtt_ms / 2));
        proxy.set_bandwidth(link.bytes_per_s);
        proxy
    });
    let endpoint = proxy
        .as_ref()
        .map_or_else(|| b.endpoint.clone(), FaultProxy::endpoint);
    let conn = a.dial(&endpoint, &b.code, LANE);
    let (options, stats) = a.options();
    let began = Instant::now();
    let lane = a.run(conn, options);
    let mut data_seconds = None;
    let done = wait_until(Duration::from_secs(1800), || {
        if data_seconds.is_none() && a.worker.bytes_written.load(Ordering::Relaxed) >= total {
            data_seconds = Some(began.elapsed().as_secs_f64());
        }
        hashes.iter().all(|hash| a.worker.has(hash))
    });
    let seconds = began.elapsed().as_secs_f64();
    drop(lane);
    assert!(done, "the transfer did not finish");
    (seconds, data_seconds.unwrap_or(seconds), stats)
}

fn record(scenario: &str, metric: &str, value: f64, unit: &str, extra: Value) {
    support::qual(scenario, metric, value, unit, extra);
}

fn large_object(link: Option<Link>, scenario: &str, disk: bool) {
    let size = std::env::var("TKFS_QUAL_LARGE_MIB")
        .ok()
        .and_then(|mib| mib.parse::<u64>().ok())
        .unwrap_or(1024)
        * MIB;
    let root = tempfile::tempdir().unwrap();
    let (a, b) = qual_pair(disk.then(|| root.path()));
    let hash = if disk {
        b.worker.hold_file(1, size)
    } else {
        b.worker.hold(content(1, size as usize))
    };
    a.worker.want(&hash, size);
    let (seconds, data_seconds, stats) =
        transfer(&a, &b, std::slice::from_ref(&hash), size, link.as_ref());
    let mib_s = size as f64 / MIB as f64 / seconds;
    let mut extra = json!({
        "bytes": size,
        "seconds": seconds,
        "data_seconds": data_seconds,
        "data_mib_per_s": size as f64 / MIB as f64 / data_seconds,
        "pulls": LaneStats::get(&stats.pulls),
        "pulls_peak": LaneStats::get(&stats.pulls_peak),
        "window_peak": LaneStats::get(&stats.window_peak),
        "worker": if disk {
            "fake (disk spool, sync_data per write, full re-read SHA-256)"
        } else {
            "fake (memory spool, SHA-256 on completion)"
        },
    });
    if let Some(link) = &link {
        let link_mib_s = link.bytes_per_s as f64 / MIB as f64;
        extra["rtt_ms"] = json!(link.rtt_ms);
        extra["link_bytes_per_s"] = json!(link.bytes_per_s);
        if link.bytes_per_s > 0 {
            record(
                scenario,
                "link_utilization",
                mib_s / link_mib_s,
                "ratio",
                extra.clone(),
            );
        }
    }
    record(scenario, "mib_per_s", mib_s, "MiB/s", extra);
}

fn tiny_files(link: Option<Link>, scenario: &str, disk: bool) {
    let count = std::env::var("TKFS_QUAL_TINY_COUNT")
        .ok()
        .and_then(|count| count.parse::<usize>().ok())
        .unwrap_or(10_000);
    let root = tempfile::tempdir().unwrap();
    let (a, b) = qual_pair(disk.then(|| root.path()));
    let hashes: Vec<String> = (0..count)
        .map(|index| {
            let hash = b.worker.hold(content(10_000 + index as u64, 4096));
            a.worker.want(&hash, 4096);
            hash
        })
        .collect();
    let (seconds, _, stats) = transfer(&a, &b, &hashes, count as u64 * 4096, link.as_ref());
    let mut extra = json!({
        "files": count,
        "file_bytes": 4096,
        "seconds": seconds,
        "pulls": LaneStats::get(&stats.pulls),
        "write_calls": a.worker.write_calls.load(Ordering::Relaxed),
        "worker": if disk {
            "fake (disk spool per object, sync_data per object, re-read SHA-256)"
        } else {
            "fake (memory spool, SHA-256 on completion)"
        },
    });
    if let Some(link) = &link {
        extra["rtt_ms"] = json!(link.rtt_ms);
        extra["link_bytes_per_s"] = json!(link.bytes_per_s);
    }
    record(
        scenario,
        "files_per_s",
        count as f64 / seconds,
        "files/s",
        extra,
    );
}

#[test]
#[ignore = "WP3.3 qualification (release)"]
fn qual_large_object_loopback() {
    large_object(None, "objects_v3_lanes::large_object_loopback", true);
}

#[test]
#[ignore = "WP3.3 qualification (release)"]
fn qual_large_object_loopback_memory() {
    large_object(
        None,
        "objects_v3_lanes::large_object_loopback_memory",
        false,
    );
}

#[test]
#[ignore = "WP3.3 qualification (release)"]
fn qual_large_object_rtt50_100mbit() {
    large_object(
        Some(Link {
            rtt_ms: 50,
            bytes_per_s: 12_500_000,
        }),
        "objects_v3_lanes::large_object_rtt50_100mbit",
        true,
    );
}

#[test]
#[ignore = "WP3.3 qualification (release)"]
fn qual_large_object_rtt50() {
    large_object(
        Some(Link {
            rtt_ms: 50,
            bytes_per_s: 0,
        }),
        "objects_v3_lanes::large_object_rtt50",
        true,
    );
}

#[test]
#[ignore = "WP3.3 qualification (release)"]
fn qual_tiny_files_10k_loopback() {
    tiny_files(None, "objects_v3_lanes::tiny_files_10k_loopback", true);
}

#[test]
#[ignore = "WP3.3 qualification (release)"]
fn qual_tiny_files_10k_loopback_memory() {
    tiny_files(
        None,
        "objects_v3_lanes::tiny_files_10k_loopback_memory",
        false,
    );
}

#[test]
#[ignore = "WP3.3 qualification (release)"]
fn qual_tiny_files_10k_rtt50() {
    tiny_files(
        Some(Link {
            rtt_ms: 50,
            bytes_per_s: 0,
        }),
        "objects_v3_lanes::tiny_files_10k_rtt50",
        true,
    );
}

#[test]
#[ignore = "WP3.3 qualification (release)"]
fn qual_tiny_files_10k_rtt50_100mbit() {
    tiny_files(
        Some(Link {
            rtt_ms: 50,
            bytes_per_s: 12_500_000,
        }),
        "objects_v3_lanes::tiny_files_10k_rtt50_100mbit",
        true,
    );
}

/// Keeps `Scope` imported for status assertions in future scenarios.
#[allow(dead_code)]
fn object_scope(hash: &str) -> Scope {
    Scope::Object {
        state: STATE_A.into(),
        hash: hash.into(),
    }
}
