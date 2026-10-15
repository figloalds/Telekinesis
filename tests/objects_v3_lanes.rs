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
        health::{ContentCounters, Generations},
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
                ObjectOutcome, ObjectResult, ObjectsPull, ObjectsPush, ObjectsReport,
                ObjectsResult, ObjectsWantsReply, ReadHeader, ReadReply, ReportHeader, WantHeader,
                WantItem, WantPriority, WantReply, WriteHeader, WriteItem, WriteReply, WriteResult,
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
    /// The next N write calls are handed on, then answered `WORKER_BUSY`
    /// (the worker's own answer: the write may run).
    worker_busy_writes: usize,
    /// The next write call sleeps this long before it answers.
    hang_next_write: Option<Duration>,
    /// Every write call fails as a whole with this error (a store outage).
    fail_writes: Option<ErrorWire>,
    /// The next N want replies report offset 0 whatever is durable (a
    /// stale offset: another transfer advanced the spool meanwhile; WP3.INT).
    stale_want_offsets: usize,
    /// Reads of objects wait while this is set (a source verifying a large
    /// object on its first serve, a slow disk; WP3.INT re-review).
    stall_reads: bool,
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
    /// 0 with `size_unknown`: not known (an event without a size descriptor);
    /// the first write's total defines it.
    size: u64,
    size_unknown: bool,
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
                size_unknown: false,
                durable: 0,
                data: Vec::new(),
                file,
            })),
        );
    }

    /// Wants `hash` without knowing its size (its event carries no size
    /// descriptor): wanted with size 0 until a write's total defines it.
    fn want_unsized(&self, hash: &str) {
        self.want(hash, 0);
        let spool = self.state.lock().unwrap().wanted[hash].clone();
        spool.lock().unwrap().size_unknown = true;
    }

    /// Stops wanting `hash` (its event was quarantined): writes of it are
    /// `Unsolicited` from now on.
    fn unwant(&self, hash: &str) {
        self.state.lock().unwrap().wanted.remove(hash);
    }

    fn has(&self, hash: &str) -> bool {
        self.state.lock().unwrap().held.contains_key(hash)
    }

    /// Makes `bytes` the durable start of a wanted object's spool, as a
    /// transfer of another session would have (no lease is left behind).
    fn advance_spool(&self, hash: &str, bytes: &[u8]) {
        let spool = self.state.lock().unwrap().wanted[hash].clone();
        let mut spool = spool.lock().unwrap();
        assert!(spool.file.is_none(), "a memory spool");
        spool.data.extend_from_slice(bytes);
        spool.durable += bytes.len() as u64;
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

    /// The session holding a live lease on `hash`.
    fn lease_of(&self, hash: &str) -> Option<String> {
        let state = self.state.lock().unwrap();
        state
            .leases
            .get(hash)
            .filter(|(_, until)| Instant::now() < *until)
            .map(|(session, _)| session.clone())
    }

    /// The sessions of every write of `hash`.
    fn write_sessions(&self, hash: &str) -> BTreeSet<String> {
        self.write_log
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, written, _, _)| written == hash)
            .map(|(session, ..)| session.clone())
            .collect()
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
        let stale = {
            let mut faults = self.faults();
            let stale = faults.stale_want_offsets > 0;
            faults.stale_want_offsets = faults.stale_want_offsets.saturating_sub(1);
            stale
        };
        let mut state = self.state.lock().unwrap();
        let mut items = Vec::new();
        let mut bytes = 0u64;
        for (size, hash, durable) in candidates {
            let durable = if stale { 0 } else { durable };
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
        while self.faults().stall_reads && !self.stopped.load(Ordering::Relaxed) {
            sleep_ms(10);
        }
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
            if faults.worker_busy_writes > 0 {
                faults.worker_busy_writes -= 1;
                self.write_calls.fetch_add(1, Ordering::Relaxed);
                return Err(ErrorWire::coded(
                    "WORKER_BUSY",
                    "test: the write was handed on and is still running",
                ));
            }
            if let Some(error) = &faults.fail_writes {
                return Err(error.clone());
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
        if spool.size_unknown {
            // Any total completes an object of unknown size (the whole-object
            // hash decides), as the real worker's write does.
            spool.size = item.total;
            spool.size_unknown = false;
        }
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
        extensions: OnceLock::new(),
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
        Self::start_as(worker, REPLICA_A, remote, endpoint, interval_ms)
    }

    /// A dialer whose folder (served by `worker`) is replica `replica`.
    fn start_as(
        worker: Arc<FakeWorker>,
        replica: &str,
        remote: &str,
        endpoint: &str,
        interval_ms: u64,
    ) -> Dialer {
        let world = World::new(&format!("a-{}", uuid()), None);
        world.folder(STATE_A, replica, &worker);
        let ctx = ctx(&world, interval_ms);
        let peer = Self::peer_context(&ctx, remote, endpoint);
        Dialer {
            world,
            worker,
            ctx,
            peer,
        }
    }

    fn peer_context(ctx: &Ctx, remote: &str, endpoint: &str) -> Arc<PeerContext> {
        Arc::new(PeerContext {
            installation: remote.into(),
            endpoint: endpoint.into(),
            local_installation: ctx.installation.clone(),
            pool: 2,
            catalog: Snapshot::new(RemoteCatalog {
                generation: 1,
                at_ms: 0,
                entries: vec![entry()],
            }),
        })
    }

    /// The same daemon (world, folder, worker) facing another peer.
    fn to_peer(&self, remote: &str, endpoint: &str) -> Dialer {
        Dialer {
            world: self.world.clone(),
            worker: self.worker.clone(),
            ctx: self.ctx.clone(),
            peer: Self::peer_context(&self.ctx, remote, endpoint),
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

/// A write the worker answers with a resume point (`RESUME_OFFSET_AHEAD`,
/// `OBJECT_PART_CHANGED`) is no failure of the object (CCR-WP3.2-5): the
/// object resumes from the durable offset the worker answered, without a
/// per-object backoff or an `ObjectError` (WP3.INT; the WP3.3 review's
/// minor on the fake worker's parity with WP3.2's).
#[test]
fn a_resume_point_resynchronizes_the_object_without_a_backoff() {
    let (a, b) = pair(100);
    let data = content(4500, 3 * MIB as usize);
    let hash = b.worker.hold(data.clone());
    a.worker.want(&hash, 3 * MIB);
    // Another transfer already made the first MiB durable; the lane's
    // first want still reports offset 0, so its first write is refused.
    a.worker.advance_spool(&hash, &data[..MIB as usize]);
    a.worker.faults().stale_want_offsets = 1;
    let started = Instant::now();
    let conn = a.dial(&b.endpoint, &b.code, LANE);
    let (options, _) = a.options();
    let lane = a.run(conn, options);
    assert!(
        wait_until(Duration::from_secs(20), || a.worker.has(&hash)),
        "not installed"
    );
    let took = started.elapsed();
    drop(lane);
    assert_eq!(a.worker.held_bytes(&hash).unwrap(), data);
    assert!(
        took < Duration::from_secs(10),
        "resumed only after {took:?} (a per-object backoff is 24-36 s)"
    );
    let errors = a.world.object_errors();
    assert!(errors.is_empty(), "{errors:?}");
    // The refused write started at 0; the next writes resumed from 1 MiB.
    let writes: Vec<u64> = a
        .worker
        .write_log
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, written, _, _)| *written == hash)
        .map(|(_, _, offset, _)| *offset)
        .collect();
    assert_eq!(writes.first(), Some(&0), "{writes:?}");
    assert!(
        writes[1..].iter().all(|offset| *offset >= MIB),
        "{writes:?}"
    );
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
/// uncertain one (no reply within its deadline) is never replayed, nor is
/// one the worker answered `WORKER_BUSY` (its own answer, possibly after it
/// handed the request on) (invariant 16).
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
    // `WORKER_BUSY` is returned, not sent again.
    worker.faults().worker_busy_writes = 1;
    let error = client
        .write_batch(
            &route,
            "session",
            vec![item(0, 5)],
            b"hello".to_vec(),
            Duration::from_secs(10),
        )
        .unwrap_err();
    assert_eq!(error.code(), "WORKER_BUSY", "{error}");
    assert_eq!(
        worker.write_calls.load(Ordering::Relaxed),
        3,
        "a WORKER_BUSY write was sent again"
    );
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
/// request for an object the worker does not serve gets no bytes. A report
/// names an object of the lane that was served to the peer through its end
/// (`OBJECT_ON_WRONG_LANE`, `UNSOLICITED_OBJECT` otherwise).
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
    // CCR-WP3.3-3 (WP3.INT): a malformed v3 request is INVALID_REQUEST.
    assert_eq!(malformed.code(), "INVALID_REQUEST");
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

    // `objects.report`: only for an object of the lane that B served this
    // peer through its end; the others never reach the worker.
    let report = |hash: &str| {
        let report = ObjectsReport {
            repo: REPO.into(),
            sender: REPLICA_A.into(),
            hash: hash.into(),
            code: "CORRUPT_AT_SOURCE".into(),
        };
        conn.call(
            "objects.report",
            serde_json::to_value(&report).unwrap(),
            Vec::new(),
            Priority::Control,
            Duration::from_secs(5),
        )
    };
    assert_eq!(
        report(&wrong_lane).unwrap_err().code(),
        "OBJECT_ON_WRONG_LANE"
    );
    assert_eq!(report(&missing).unwrap_err().code(), "UNSOLICITED_OBJECT");
    assert!(b.worker.reports().is_empty(), "{:?}", b.worker.reports());
    report(&mine_hash).unwrap();
    assert_eq!(
        b.worker.reports(),
        vec![(mine_hash.clone(), "CORRUPT_AT_SOURCE".to_owned())]
    );
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

/// Multi-source liveness, push side (a hub): B wants Y; A1 cannot serve it,
/// C can. A1's lane is offered Y first (B's worker leases Y to A1's push
/// session, and A1's lane keeps renewing that lease with every
/// `objects.wants`); A1 does not send it, so B backs Y off for A1 (no
/// error), parks it under A1's lease and offers it to C's lane, whose push
/// installs it under that lease: well inside the lease's 60 s, with A1's
/// lane still running.
#[test]
fn a_source_that_cannot_serve_hands_its_push_lease_on() {
    let b = Acceptor::start(FakeWorker::start(REPLICA_B));
    let bytes = content(3000, 1536 * 1024);
    let hash = sha(&bytes);
    b.worker.want(&hash, bytes.len() as u64);
    let a1 = Dialer::start(
        FakeWorker::start(REPLICA_A),
        &b.installation(),
        &b.endpoint,
        100,
    );
    let c = Dialer::start_as(
        FakeWorker::start("replica-c"),
        "replica-c",
        &b.installation(),
        &b.endpoint,
        100,
    );
    c.worker.hold(bytes.clone());
    let (options, _) = a1.options();
    let lane_a1 = a1.run(a1.dial(&b.endpoint, &b.code, LANE), options);
    let a1_lease = objects::push_lease(&a1.ctx.installation, LANE);
    assert!(
        wait_until(Duration::from_secs(10), || b
            .worker
            .lease_of(&hash)
            .is_some_and(|lease| lease == a1_lease)),
        "A1 was never offered Y: lease {:?}",
        b.worker.lease_of(&hash)
    );
    // A1 tried and could not read it: its push went through without Y.
    // Not serving an object is no error of A1's (WP3.INT re-review: a
    // peer's wants may name any hash; this sync point was A1's
    // `NOT_AVAILABLE` ObjectError).
    assert!(
        wait_until(Duration::from_secs(10), || a1
            .world
            .finished()
            .iter()
            .any(|f| f.op == "objects.push" && f.ok)),
        "{:?}",
        a1.world.finished()
    );
    assert!(
        !a1.world
            .object_errors()
            .iter()
            .any(|(object, _)| *object == hash),
        "{:?}",
        a1.world.object_errors()
    );
    let began = Instant::now();
    let (options, _) = c.options();
    let lane_c = c.run(c.dial(&b.endpoint, &b.code, LANE), options);
    assert!(
        wait_until(Duration::from_secs(20), || b.worker.has(&hash)),
        "B never got Y while A1's lane held its lease ({:?})",
        b.worker.lease_of(&hash)
    );
    assert!(began.elapsed() < Duration::from_secs(30));
    assert_eq!(b.worker.held_bytes(&hash).unwrap(), bytes);
    // C's push wrote it under A1's lease (the handoff), and A1's lane was
    // still running (renewing that lease) the whole time.
    assert_eq!(
        b.worker.write_sessions(&hash),
        BTreeSet::from([a1_lease.clone()])
    );
    assert!(!lane_a1.finished());
    // A source that does not serve an object is no error at B.
    assert!(
        !b.world
            .object_errors()
            .iter()
            .any(|(object, _)| *object == hash),
        "{:?}",
        b.world.object_errors()
    );
    drop(lane_c);
    drop(lane_a1);
}

/// Multi-source liveness, pull side: D wants X from B (which lacks it) and
/// C (which has it). D's lane to B leases X first (D's worker), its pull
/// fails `NOT_AVAILABLE`, and the lane keeps renewing the lease with its
/// wants; D parks X under that lease, and its lane to C takes it over and
/// installs it under that lease, well inside the lease's 60 s.
#[test]
fn a_source_that_cannot_serve_hands_its_pull_lease_on() {
    let b = Acceptor::start(FakeWorker::start(REPLICA_B));
    let c = Acceptor::start(FakeWorker::start(REPLICA_B));
    let bytes = content(3100, 1536 * 1024);
    let hash = c.worker.hold(bytes.clone());
    let to_b = Dialer::start(
        FakeWorker::start(REPLICA_A),
        &b.installation(),
        &b.endpoint,
        100,
    );
    to_b.worker.want(&hash, bytes.len() as u64);
    let to_c = to_b.to_peer(&c.installation(), &c.endpoint);
    let (options, _) = to_b.options();
    let lane_b = to_b.run(to_b.dial(&b.endpoint, &b.code, LANE), options);
    let b_lanes = objects::pull_holder(&b.installation(), LANE);
    assert!(
        wait_until(Duration::from_secs(10), || to_b
            .world
            .object_errors()
            .iter()
            .any(|(object, code)| *object == hash && code == "NOT_AVAILABLE")),
        "{:?}",
        to_b.world.object_errors()
    );
    assert!(
        to_b.worker
            .lease_of(&hash)
            .is_some_and(|lease| lease.starts_with(&b_lanes)),
        "{:?}",
        to_b.worker.lease_of(&hash)
    );
    let began = Instant::now();
    let (options, _) = to_c.options();
    let lane_c = to_c.run(to_c.dial(&c.endpoint, &c.code, LANE), options);
    assert!(
        wait_until(Duration::from_secs(20), || to_b.worker.has(&hash)),
        "D never got X while its lane to B held the lease ({:?})",
        to_b.worker.lease_of(&hash)
    );
    assert!(began.elapsed() < Duration::from_secs(30));
    assert_eq!(to_b.worker.held_bytes(&hash).unwrap(), bytes);
    let sessions = to_b.worker.write_sessions(&hash);
    assert!(
        sessions.len() == 1 && sessions.iter().all(|lease| lease.starts_with(&b_lanes)),
        "{sessions:?}"
    );
    assert!(!lane_b.finished());
    // Installed: B's error for it is resolved too.
    assert!(
        wait_until(Duration::from_secs(5), || to_b.world.events().iter().any(
            |event| matches!(event, StatusEvent::Resolved { scope, code }
                if *scope == object_scope(&hash) && code == "NOT_AVAILABLE")
        )),
        "the NOT_AVAILABLE error from B was not resolved"
    );
    drop(lane_c);
    drop(lane_b);
}

/// Invariant 22 on the acceptor's push: a chunk of an object without a
/// share of the push, out of order (a gap), or past its share of the push
/// budget (the allotment rule) ends the push `PROTOCOL_VIOLATION`, before
/// anything of it is buffered or written; the session stays usable.
#[test]
fn a_push_outside_its_shares_is_refused() {
    let (a, b) = pair(100);
    let size = 3 * MIB;
    let data = content(3200, size as usize);
    let hash = sha(&data);
    b.worker.want(&hash, size);
    let conn = a.dial(&b.endpoint, &b.code, LANE);
    let push = |items: Vec<WantItem>, chunks: Vec<(String, u64, u64, Vec<u8>)>| {
        let request = ObjectsPush {
            repo: REPO.into(),
            sender: REPLICA_A.into(),
            receiver: REPLICA_B.into(),
            lane: LANE,
            items,
        };
        let mut upload = conn.upload(
            "objects.push",
            serde_json::to_value(&request).unwrap(),
            Priority::Control,
            Duration::from_secs(10),
        )?;
        for (hash, offset, total, bytes) in chunks {
            let header = ChunkHeader {
                id: 0,
                hash: Some(hash),
                offset,
                total,
                last: offset + bytes.len() as u64 == total,
            };
            if let Err(error) = upload.chunk(header, &bytes) {
                // The acceptor may already have answered (and ended it).
                return Err(upload.finish().err().unwrap_or(error));
            }
        }
        upload.finish().map(|reply| -> ObjectsResult {
            if reply.body.is_empty() {
                serde_json::from_value(reply.header).unwrap()
            } else {
                serde_json::from_slice(&reply.body).unwrap()
            }
        })
    };
    let item = |hash: &str, size: u64| WantItem {
        hash: hash.into(),
        size,
        offset: 0,
        priority: WantPriority::History,
    };
    let part = |at: u64, len: u64| data[at as usize..(at + len) as usize].to_vec();
    let violation = |result: Result<ObjectsResult, CallError>| {
        let error = result.unwrap_err();
        assert_eq!(error.code(), "PROTOCOL_VIOLATION", "{error}");
    };
    // A gap: the second MiB first.
    violation(push(
        vec![item(&hash, size)],
        vec![(hash.clone(), MIB, size, part(MIB, MIB))],
    ));
    // An object that is not in the push.
    let other = sha(b"not in the push");
    violation(push(
        vec![item(&hash, size)],
        vec![(other, 0, 10, vec![7; 10])],
    ));
    // Past the object's share: a claimed 32 MiB object gets 16 MiB of the
    // push budget; its 17th MiB is refused.
    let big = sha(b"a claimed large object");
    let chunks = (0..17)
        .map(|index| (big.clone(), index * MIB, 32 * MIB, vec![1; MIB as usize]))
        .collect();
    violation(push(vec![item(&big, 32 * MIB)], chunks));
    // Nothing of the refused chunks reached the worker.
    assert!(
        b.worker
            .write_log
            .lock()
            .unwrap()
            .iter()
            .all(|(_, written, offset, _)| *written != hash
                && (*written != big || *offset < 16 * MIB)),
        "{:?}",
        b.worker.write_log.lock().unwrap()
    );
    // The session is still usable: a push in order completes.
    let chunks = (0..3)
        .map(|index| (hash.clone(), index * MIB, size, part(index * MIB, MIB)))
        .collect();
    let result = push(vec![item(&hash, size)], chunks).unwrap();
    assert_eq!(result.results[0].outcome, ObjectOutcome::Completed);
    assert_eq!(b.worker.held_bytes(&hash).unwrap(), data);
    conn.close();
}

/// §2.3: a write call that fails as a whole at the acceptor (its worker's
/// store is down) fails the push, not its objects: no `ObjectError` and no
/// per-object backoff on either side (a backoff would hold them 30 s); once
/// the store is back, the next push completes them.
#[test]
fn a_failed_worker_call_fails_the_push_not_its_objects() {
    let (a, b) = pair(100);
    let hashes: Vec<String> = [300_000usize, 200_000]
        .iter()
        .enumerate()
        .map(|(index, size)| {
            let hash = a.worker.hold(content(3300 + index as u64, *size));
            b.worker.want(&hash, *size as u64);
            hash
        })
        .collect();
    b.worker.faults().fail_writes = Some(ErrorWire::coded("SQLITE_BUSY", "test: store down"));
    let (options, _) = a.options();
    let lane = a.run(a.dial(&b.endpoint, &b.code, LANE), options);
    let failed_push = || {
        b.world.finished().iter().any(|f| {
            f.op == "objects.push"
                && f.direction == Direction::Inbound
                && !f.ok
                && f.error
                    .as_ref()
                    .is_some_and(|error| error.code == "SQLITE_BUSY")
        })
    };
    assert!(
        wait_until(Duration::from_secs(10), failed_push),
        "{:?}",
        b.world.finished()
    );
    assert!(
        a.world
            .finished()
            .iter()
            .any(|f| f.op == "objects.push" && !f.ok),
        "the dialer's push did not fail"
    );
    b.worker.faults().fail_writes = None;
    let healed = Instant::now();
    assert!(
        wait_until(Duration::from_secs(20), || hashes
            .iter()
            .all(|hash| b.worker.has(hash))),
        "the objects did not complete once the store was back"
    );
    assert!(healed.elapsed() < Duration::from_secs(25));
    for world in [&a.world, &b.world] {
        assert!(
            !world
                .object_errors()
                .iter()
                .any(|(hash, _)| hashes.contains(hash)),
            "{:?}",
            world.object_errors()
        );
    }
    drop(lane);
}

/// §2.2 rule 2: the session client dials without holding its lock. While a
/// dial hangs (a worker that accepts TCP but never answers the upgrade),
/// `is_open` answers at once, and concurrent callers share that one dial's
/// failure instead of each dialing in turn.
#[test]
fn the_session_client_dials_without_its_lock() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("ws://{}", listener.local_addr().unwrap());
    let accepted = Arc::new(AtomicUsize::new(0));
    {
        let accepted = accepted.clone();
        thread::spawn(move || {
            let mut sessions = Vec::new();
            let mut hanging = Vec::new();
            for (index, stream) in listener.incoming().flatten().enumerate() {
                if index == 0 {
                    // The first dial gets a session (closed below).
                    if let Ok(conn) = session::accept_loopback(stream, None, ConnOptions::default())
                    {
                        sessions.push(conn);
                    }
                    continue;
                }
                // Later dials hang: accepted, never answered.
                accepted.fetch_add(1, Ordering::SeqCst);
                hanging.push(stream);
            }
        });
    }
    let endpoint = WorkerEndpoint {
        instance: format!("hanging-{}", uuid()),
        pid: 1,
        address,
        version: "test".into(),
        token: None,
    };
    let client = SessionWorkerClient::connect(&endpoint).unwrap();
    client.close();
    let began = Instant::now();
    let callers: Vec<_> = (0..6)
        .map(|_| {
            let client = client.clone();
            thread::spawn(move || client.conn().map(|_| ()))
        })
        .collect();
    assert!(wait_until(Duration::from_secs(5), || accepted
        .load(Ordering::SeqCst)
        >= 1));
    let asked = Instant::now();
    assert!(!client.is_open());
    assert!(
        asked.elapsed() < Duration::from_millis(500),
        "is_open waited for the dial: {:?}",
        asked.elapsed()
    );
    for caller in callers {
        assert!(caller.join().unwrap().is_err());
    }
    // One dial (rarely two: a caller that came after the first failed),
    // not one per caller.
    assert!(
        accepted.load(Ordering::SeqCst) <= 2,
        "{} dials",
        accepted.load(Ordering::SeqCst)
    );
    assert!(
        began.elapsed() < Duration::from_secs(12),
        "{:?}",
        began.elapsed()
    );
}

/// Multi-source liveness when the transfer that took an object over goes
/// away (pull side): D wants X; B lacks it (D parks X under its lane to B's
/// lease, which that lane keeps renewing); D's lane to C takes X over and
/// moves part of it, then C's session resets and C stays away; E holds X.
/// C's claim is given back when its round fails, so E's lane takes X over
/// and installs it, with B's lane still running.
#[test]
fn a_holder_that_goes_away_hands_the_pulled_object_on() {
    let b = Acceptor::start(FakeWorker::start(REPLICA_B));
    let c = Acceptor::start(FakeWorker::start(REPLICA_B));
    let e = Acceptor::start(FakeWorker::start(REPLICA_B));
    let size = 8 * MIB;
    let bytes = content(3400, size as usize);
    let hash = c.worker.hold(bytes.clone());
    e.worker.hold(bytes.clone());
    let to_b = Dialer::start(
        FakeWorker::start(REPLICA_A),
        &b.installation(),
        &b.endpoint,
        100,
    );
    to_b.worker.want(&hash, size);
    to_b.worker.faults().write_delay = Some(Duration::from_millis(20));
    let to_c = to_b.to_peer(&c.installation(), &c.endpoint);
    let to_e = to_b.to_peer(&e.installation(), &e.endpoint);
    let (options, _) = to_b.options();
    let lane_b = to_b.run(to_b.dial(&b.endpoint, &b.code, LANE), options);
    let b_lanes = objects::pull_holder(&b.installation(), LANE);
    assert!(
        wait_until(Duration::from_secs(10), || to_b
            .world
            .object_errors()
            .iter()
            .any(|(object, code)| *object == hash && code == "NOT_AVAILABLE")),
        "{:?}",
        to_b.world.object_errors()
    );
    // C takes X over, and its session resets mid-transfer.
    let proxy = FaultProxy::start(&c.endpoint);
    proxy.reset_after(2 * MIB);
    let (options, _) = to_c.options();
    let mut lane_c = to_c.run(to_c.dial(&proxy.endpoint(), &c.code, LANE), options);
    assert!(
        wait_until(Duration::from_secs(20), || lane_c.finished()),
        "C's lane did not end with its session"
    );
    lane_c.stop();
    assert!(!to_b.worker.has(&hash), "C delivered X before it went away");
    assert!(
        to_b.worker
            .write_sessions(&hash)
            .iter()
            .all(|lease| lease.starts_with(&b_lanes)),
        "{:?}",
        to_b.worker.write_sessions(&hash)
    );
    // E takes it over (from where C left it) while B's lane keeps renewing
    // the lease.
    let began = Instant::now();
    let (options, _) = to_e.options();
    let lane_e = to_e.run(to_e.dial(&e.endpoint, &e.code, LANE), options);
    assert!(
        wait_until(Duration::from_secs(20), || to_b.worker.has(&hash)),
        "E never delivered X: lease {:?}, durable {:?}",
        to_b.worker.lease_of(&hash),
        to_b.worker.durable(&hash)
    );
    assert!(began.elapsed() < Duration::from_secs(25));
    assert_eq!(to_b.worker.held_bytes(&hash).unwrap(), bytes);
    let sessions = to_b.worker.write_sessions(&hash);
    assert!(
        sessions.len() == 1 && sessions.iter().all(|lease| lease.starts_with(&b_lanes)),
        "{sessions:?}"
    );
    assert!(!lane_b.finished(), "B's lane stopped renewing the lease");
    drop(lane_e);
    drop(lane_b);
}

/// The same on the push side (a hub): B wants Y; A1 cannot serve it (B
/// parks Y under A1's push lease, which A1's lane keeps renewing); C takes
/// Y over and pushes part of it, then C's session resets and C stays away;
/// E holds Y. C's push ends without its end, so B gives C's claim back, and
/// E's push installs Y under A1's lease, with A1's lane still running.
#[test]
fn a_holder_that_goes_away_hands_the_pushed_object_on() {
    let b = Acceptor::start(FakeWorker::start(REPLICA_B));
    let size = 8 * MIB;
    let bytes = content(3500, size as usize);
    let hash = sha(&bytes);
    b.worker.want(&hash, size);
    b.worker.faults().write_delay = Some(Duration::from_millis(20));
    let a1 = Dialer::start(
        FakeWorker::start(REPLICA_A),
        &b.installation(),
        &b.endpoint,
        100,
    );
    let c = Dialer::start_as(
        FakeWorker::start("replica-c"),
        "replica-c",
        &b.installation(),
        &b.endpoint,
        100,
    );
    c.worker.hold(bytes.clone());
    let e = Dialer::start_as(
        FakeWorker::start("replica-e"),
        "replica-e",
        &b.installation(),
        &b.endpoint,
        100,
    );
    e.worker.hold(bytes.clone());
    let (options, _) = a1.options();
    let lane_a1 = a1.run(a1.dial(&b.endpoint, &b.code, LANE), options);
    let a1_lease = objects::push_lease(&a1.ctx.installation, LANE);
    // A1 pushed without Y (it cannot read it; no error of A1's, WP3.INT
    // re-review: this sync point was A1's `NOT_AVAILABLE` ObjectError).
    assert!(
        wait_until(Duration::from_secs(10), || a1
            .world
            .finished()
            .iter()
            .any(|f| f.op == "objects.push" && f.ok)),
        "{:?}",
        a1.world.finished()
    );
    // C takes Y over, and its session resets mid-push.
    let proxy = FaultProxy::start(&b.endpoint);
    proxy.reset_after(3 * MIB);
    let (options, _) = c.options();
    let mut lane_c = c.run(c.dial(&proxy.endpoint(), &b.code, LANE), options);
    assert!(
        wait_until(Duration::from_secs(20), || lane_c.finished()),
        "C's lane did not end with its session"
    );
    lane_c.stop();
    assert!(!b.worker.has(&hash), "C delivered Y before it went away");
    assert!(
        wait_until(Duration::from_secs(5), || b
            .worker
            .write_sessions(&hash)
            .contains(&a1_lease)),
        "C never pushed Y under A1's lease: {:?}",
        b.worker.write_sessions(&hash)
    );
    let began = Instant::now();
    let (options, _) = e.options();
    let lane_e = e.run(e.dial(&b.endpoint, &b.code, LANE), options);
    assert!(
        wait_until(Duration::from_secs(20), || b.worker.has(&hash)),
        "E never delivered Y: lease {:?}, durable {:?}",
        b.worker.lease_of(&hash),
        b.worker.durable(&hash)
    );
    assert!(began.elapsed() < Duration::from_secs(25));
    assert_eq!(b.worker.held_bytes(&hash).unwrap(), bytes);
    assert_eq!(
        b.worker.write_sessions(&hash),
        BTreeSet::from([a1_lease.clone()])
    );
    assert!(!lane_a1.finished(), "A1's lane stopped renewing the lease");
    drop(lane_e);
    drop(lane_a1);
}

/// A large object spanning many short rounds resumes at the start of every
/// round: no round finds it hidden under the lane's other lease id and
/// idles for the anti-entropy interval (here 30 s).
#[test]
fn a_large_object_never_idles_between_rounds() {
    let (a, b) = pair(30_000);
    let size = 96 * MIB;
    let bytes = content(3600, size as usize);
    let hash = b.worker.hold(bytes.clone());
    a.worker.want(&hash, size);
    a.worker.faults().write_delay = Some(Duration::from_millis(20));
    let conn = a.dial(&b.endpoint, &b.code, LANE);
    let (mut options, stats) = a.options();
    options.round = Duration::from_millis(300);
    let began = Instant::now();
    let lane = a.run(conn, options);
    let done = wait_until(Duration::from_secs(60), || a.worker.has(&hash));
    let took = began.elapsed();
    drop(lane);
    assert!(done, "not complete after 60 s");
    assert!(
        took < Duration::from_secs(15),
        "a round idled between rounds: {took:?} for 96 MiB"
    );
    assert!(
        LaneStats::get(&stats.v3_rounds) >= 2,
        "the object did not span several rounds: {} rounds",
        LaneStats::get(&stats.v3_rounds)
    );
    assert_eq!(a.worker.held_bytes(&hash).unwrap(), bytes);
}

/// The acceptor reads a push no faster than its worker writes, counting
/// only bytes that drain without more input: a dialer that sends part of
/// each of many objects (tails that are not whole writes, more than
/// `PUSH_BUFFERED_BYTES` in all) never stalls its push; each tail is written
/// when the next object starts.
#[test]
fn a_push_of_partial_objects_never_stalls_the_acceptor() {
    let (a, b) = pair(100);
    let conn = a.dial(&b.endpoint, &b.code, LANE);
    let size = MIB + 1;
    let part = 900 * 1024u64;
    let objects: Vec<(String, Vec<u8>)> = (0..12)
        .map(|index| {
            let data = content(3700 + index, size as usize);
            (sha(&data), data)
        })
        .collect();
    for (hash, _) in &objects {
        b.worker.want(hash, size);
    }
    let request = ObjectsPush {
        repo: REPO.into(),
        sender: REPLICA_A.into(),
        receiver: REPLICA_B.into(),
        lane: LANE,
        items: objects
            .iter()
            .map(|(hash, _)| WantItem {
                hash: hash.clone(),
                size,
                offset: 0,
                priority: WantPriority::History,
            })
            .collect(),
    };
    let began = Instant::now();
    let mut upload = conn
        .upload(
            "objects.push",
            serde_json::to_value(&request).unwrap(),
            Priority::Control,
            Duration::from_secs(10),
        )
        .unwrap();
    for (hash, data) in &objects {
        let mut at = 0u64;
        while at < part {
            let len = (part - at).min(300 * 1024);
            let header = ChunkHeader {
                id: 0,
                hash: Some(hash.clone()),
                offset: at,
                total: size,
                last: false,
            };
            upload
                .chunk(header, &data[at as usize..(at + len) as usize])
                .unwrap();
            at += len;
        }
    }
    let reply = upload.finish().unwrap();
    let result: ObjectsResult = if reply.body.is_empty() {
        serde_json::from_value(reply.header).unwrap()
    } else {
        serde_json::from_slice(&reply.body).unwrap()
    };
    assert!(
        began.elapsed() < Duration::from_secs(8),
        "the push stalled: {:?}",
        began.elapsed()
    );
    for (hash, _) in &objects {
        let outcome = &result
            .results
            .iter()
            .find(|result| result.hash == *hash)
            .unwrap()
            .outcome;
        assert_eq!(
            *outcome,
            ObjectOutcome::Progressed {
                durable_offset: part
            }
        );
        assert_eq!(b.worker.durable(hash), Some(part));
    }
    conn.close();
}

/// A scripted v3 peer (its own bulk session server): `handler` answers the
/// dialer's requests. Returns the peer's installation, pairing code and
/// endpoint, and the thread that serves one session.
fn scripted_peer(handler: Arc<dyn Handler>) -> (String, String, String, thread::JoinHandle<Conn>) {
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
    let server = {
        let remote = remote.clone();
        thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let (conn, _) = session::accept(
                stream,
                Arc::new(Policy(remote, verifier)),
                ConnOptions::for_role(LaneRole::Bulk),
            )
            .unwrap();
            conn.serve(handler);
            conn
        })
    };
    (remote, code, endpoint, server)
}

/// Invariant 22 at the dialer: a pull stream must start each object at the
/// start of its span and stay contiguous. A peer that skips the start of a
/// span ends the pull `PROTOCOL_VIOLATION`, and nothing of it is written.
#[test]
fn a_pull_stream_must_start_each_object_at_its_span() {
    struct SkippingPeer {
        hash: String,
        data: Vec<u8>,
        pulls: AtomicUsize,
    }
    impl Handler for SkippingPeer {
        fn handle(&self, req: Incoming) {
            match req.op.as_str() {
                "objects.pull" => {
                    self.pulls.fetch_add(1, Ordering::SeqCst);
                    let size = self.data.len() as u64;
                    // The span is [0, size); the stream starts at 1 MiB.
                    let header = ChunkHeader {
                        id: 0,
                        hash: Some(self.hash.clone()),
                        offset: MIB,
                        total: size,
                        last: true,
                    };
                    let _ = req.responder.chunk(header, &self.data[MIB as usize..]);
                    let result = ObjectsResult {
                        results: vec![ObjectResult {
                            hash: self.hash.clone(),
                            bytes: size - MIB,
                            outcome: ObjectOutcome::Completed,
                        }],
                    };
                    req.responder
                        .reply(serde_json::to_value(result).unwrap(), Vec::new());
                }
                "objects.wants" => req.responder.reply(
                    serde_json::to_value(ObjectsWantsReply { items: vec![] }).unwrap(),
                    Vec::new(),
                ),
                op => req.responder.error(ErrorWire::coded(
                    "UNSUPPORTED_OPERATION",
                    format!("{op}: not scripted"),
                )),
            }
        }
    }
    let data = content(3800, 2 * MIB as usize);
    let hash = sha(&data);
    let peer = Arc::new(SkippingPeer {
        hash: hash.clone(),
        data,
        pulls: AtomicUsize::new(0),
    });
    let (remote, code, endpoint, server) = scripted_peer(peer.clone());
    let a = Dialer::start(FakeWorker::start(REPLICA_A), &remote, &endpoint, 100);
    a.worker.want(&hash, 2 * MIB);
    let (options, _) = a.options();
    let lane = a.run(a.dial(&endpoint, &code, LANE), options);
    let refused = || {
        a.world.finished().iter().any(|f| {
            f.op == "objects.pull"
                && !f.ok
                && f.error
                    .as_ref()
                    .is_some_and(|error| error.code == "PROTOCOL_VIOLATION")
        })
    };
    assert!(
        wait_until(Duration::from_secs(10), refused),
        "{:?}",
        a.world.finished()
    );
    drop(lane);
    assert!(peer.pulls.load(Ordering::SeqCst) >= 1);
    assert!(!a.worker.has(&hash));
    assert!(
        a.worker.write_sessions(&hash).is_empty(),
        "bytes past a skipped span start were written"
    );
    server.join().unwrap().close();
}

/// Invariant 22 at the dialer (WP3.INT; the WP3.3 review's major finding): a
/// peer's per-object results count only for the objects the request listed,
/// each once. Results naming other objects never back anything off, never
/// record an `ObjectError`, never count as uploaded, and never make our
/// worker re-verify (`objects.report`) an object; nor does a
/// `CorruptAtSource` for an object the push did not send through its end.
/// An object the peer wants that we may not serve gets no `ObjectError`
/// either (a silent backoff, as the acceptor does).
#[test]
fn a_peer_reply_counts_only_for_the_objects_it_was_asked_for() {
    struct LyingPeer {
        wanted: Vec<WantItem>,
        bogus: Vec<String>,
        pulls: AtomicUsize,
        pushes: AtomicUsize,
    }
    impl LyingPeer {
        fn lies(&self, outcome: &ObjectOutcome) -> Vec<ObjectResult> {
            self.bogus
                .iter()
                .flat_map(|hash| {
                    [ObjectOutcome::CorruptAtSource, outcome.clone()]
                        .into_iter()
                        .map(|outcome| ObjectResult {
                            hash: hash.clone(),
                            outcome,
                            bytes: 0,
                        })
                })
                .collect()
        }
    }
    impl Handler for LyingPeer {
        fn handle(&self, req: Incoming) {
            match req.op.as_str() {
                "objects.pull" => {
                    self.pulls.fetch_add(1, Ordering::SeqCst);
                    let request: ObjectsPull = serde_json::from_value(req.header).unwrap();
                    // Nothing of what was asked is served; every other
                    // object fails or "completes".
                    let mut results: Vec<ObjectResult> = request
                        .items
                        .iter()
                        .map(|item| ObjectResult {
                            hash: item.hash.clone(),
                            outcome: ObjectOutcome::NotAvailable,
                            bytes: 0,
                        })
                        .collect();
                    results.extend(self.lies(&ObjectOutcome::NotPermitted));
                    req.responder.reply(
                        serde_json::to_value(ObjectsResult { results }).unwrap(),
                        Vec::new(),
                    );
                }
                "objects.wants" => req.responder.reply(
                    serde_json::to_value(ObjectsWantsReply {
                        items: self.wanted.clone(),
                    })
                    .unwrap(),
                    Vec::new(),
                ),
                "objects.push" => {
                    self.pushes.fetch_add(1, Ordering::SeqCst);
                    let request: ObjectsPush = serde_json::from_value(req.header).unwrap();
                    for _ in req.chunks.into_iter().flatten() {}
                    // Every listed object "completes" except the large one,
                    // which this push cannot have sent through its end; the
                    // reply names other objects too, and the small one twice.
                    let mut results: Vec<ObjectResult> = request
                        .items
                        .iter()
                        .map(|item| ObjectResult {
                            hash: item.hash.clone(),
                            outcome: if item.size > objects::PUSH_BUDGET_BYTES {
                                ObjectOutcome::CorruptAtSource
                            } else {
                                ObjectOutcome::Completed
                            },
                            bytes: 0,
                        })
                        .collect();
                    results.extend(results.clone());
                    results.extend(self.lies(&ObjectOutcome::Completed));
                    req.responder.reply(
                        serde_json::to_value(ObjectsResult { results }).unwrap(),
                        Vec::new(),
                    );
                }
                op => req.responder.error(ErrorWire::coded(
                    "UNSUPPORTED_OPERATION",
                    format!("{op}: not scripted"),
                )),
            }
        }
    }
    let lane = LANE;
    let owned = |seed: u64, len: usize| {
        (seed..)
            .map(|seed| content(seed, len))
            .find(|bytes| lane.owns(&sha(bytes)))
            .unwrap()
    };
    let worker = FakeWorker::start(REPLICA_A);
    let small = worker.hold(owned(4100, 4096));
    let large_len = objects::PUSH_BUDGET_BYTES + MIB;
    let large = worker.hold(owned(4200, large_len as usize));
    let missing = sha(&owned(4300, 1000));
    worker.want(&missing, 1000);
    // An object the peer wants that we may not serve (private): not serving
    // it is no error of ours (WP3.INT), as on the acceptor's side.
    let private = sha(&owned(4350, 64));
    worker.state.lock().unwrap().private.insert(private.clone());
    let bogus: Vec<String> = (0..3).map(|i| sha(&owned(4400 + i * 7, 64))).collect();
    let item = |hash: &str, size: u64| WantItem {
        hash: hash.into(),
        size,
        offset: 0,
        priority: WantPriority::Current,
    };
    let peer = Arc::new(LyingPeer {
        wanted: vec![
            item(&small, 4096),
            item(&large, large_len),
            item(&private, 64),
        ],
        bogus: bogus.clone(),
        pulls: AtomicUsize::new(0),
        pushes: AtomicUsize::new(0),
    });
    let (remote, code, endpoint, server) = scripted_peer(peer.clone());
    let a = Dialer::start(worker, &remote, &endpoint, 100);
    let (options, _) = a.options();
    let running = a.run(a.dial(&endpoint, &code, lane), options);
    let pushed = || {
        a.world
            .finished()
            .iter()
            .any(|f| f.op == "objects.push" && f.ok)
    };
    let pulled = || {
        a.world
            .finished()
            .iter()
            .any(|f| f.op == "objects.pull" && f.ok)
    };
    assert!(
        wait_until(Duration::from_secs(20), || pushed() && pulled()),
        "{:?}",
        a.world.finished()
    );
    drop(running);
    assert!(peer.pulls.load(Ordering::SeqCst) >= 1);
    assert!(peer.pushes.load(Ordering::SeqCst) >= 1);
    // Errors only for what was asked: the pulled object, the large one.
    let errors = a.world.object_errors();
    assert!(
        errors.iter().any(|(hash, _)| *hash == missing),
        "{errors:?}"
    );
    for (hash, code) in &errors {
        assert!(
            *hash == missing || *hash == large,
            "an ObjectError for {hash} ({code}), which was never asked for"
        );
    }
    // The first push counted one object up (the small one, once).
    let first = a
        .world
        .finished()
        .into_iter()
        .find(|f| f.op == "objects.push" && f.ok)
        .unwrap();
    assert_eq!(first.objects_up, 1, "{first:?}");
    // Our worker re-verified nothing: the large object was not sent through
    // its end, the others were never pushed.
    assert!(a.worker.reports().is_empty(), "{:?}", a.worker.reports());
    server.join().unwrap().close();
}

// ---------------------------------------------------------------------------
// WP3.INT post-integration re-review

/// The reply of a raw `objects.push` (header or body).
fn push_result(reply: session::Reply) -> ObjectsResult {
    if reply.body.is_empty() {
        serde_json::from_value(reply.header).unwrap()
    } else {
        serde_json::from_slice(&reply.body).unwrap()
    }
}

/// A push may list any hash (WP3.3 re-review major): one the acceptor never
/// offered this dialer (its worker does not want it: `UNSOLICITED_OBJECT`)
/// is answered in the push's result alone, with neither an `ObjectError`
/// nor a per-object backoff at the acceptor, so a peer cannot push the
/// acceptor's genuine object errors out of its status with hashes of its
/// choosing.
#[test]
fn a_push_of_objects_never_offered_records_no_errors() {
    let (a, b) = pair(100);
    let conn = a.dial(&b.endpoint, &b.code, LANE);
    for round in 0..4u64 {
        let hashes: Vec<String> = (0..64)
            .map(|index| sha(format!("never wanted {round} {index}").as_bytes()))
            .collect();
        let request = ObjectsPush {
            repo: REPO.into(),
            sender: REPLICA_A.into(),
            receiver: REPLICA_B.into(),
            lane: LANE,
            items: hashes
                .iter()
                .map(|hash| WantItem {
                    hash: hash.clone(),
                    size: 0,
                    offset: 0,
                    priority: WantPriority::History,
                })
                .collect(),
        };
        let mut upload = conn
            .upload(
                "objects.push",
                serde_json::to_value(&request).unwrap(),
                Priority::Control,
                Duration::from_secs(10),
            )
            .unwrap();
        for hash in &hashes {
            let header = ChunkHeader {
                id: 0,
                hash: Some(hash.clone()),
                offset: 0,
                total: 0,
                last: true,
            };
            upload.chunk(header, &[]).unwrap();
        }
        let result = push_result(upload.finish().unwrap());
        assert_eq!(result.results.len(), hashes.len());
        assert!(
            result
                .results
                .iter()
                .all(|result| result.outcome == ObjectOutcome::Unsolicited),
            "{:?}",
            result.results.first()
        );
    }
    sleep_ms(200);
    let errors = b.world.object_errors();
    assert!(
        errors.is_empty(),
        "{} ObjectErrors for objects never offered, e.g. {:?}",
        errors.len(),
        errors.first()
    );
    conn.close();
}

/// The same on the dialer (WP3.3 re-review major): an acceptor's
/// `objects.wants` may name any hash; one our worker refuses to read (it
/// is not available here, or not ours to serve) is not sent, with neither an
/// `ObjectError` nor a backoff of ours (the acceptor backs off what it
/// offered and we did not send).
#[test]
fn wants_for_objects_we_cannot_serve_record_no_errors() {
    struct WantingPeer {
        wanted: Vec<WantItem>,
        pushes: AtomicUsize,
    }
    impl Handler for WantingPeer {
        fn handle(&self, req: Incoming) {
            match req.op.as_str() {
                "objects.wants" => req.responder.reply(
                    serde_json::to_value(ObjectsWantsReply {
                        items: self.wanted.clone(),
                    })
                    .unwrap(),
                    Vec::new(),
                ),
                "objects.push" => {
                    self.pushes.fetch_add(1, Ordering::SeqCst);
                    let request: ObjectsPush = serde_json::from_value(req.header).unwrap();
                    for _ in req.chunks.into_iter().flatten() {}
                    let results = request
                        .items
                        .iter()
                        .map(|item| ObjectResult {
                            hash: item.hash.clone(),
                            outcome: ObjectOutcome::Skipped,
                            bytes: 0,
                        })
                        .collect();
                    req.responder.reply(
                        serde_json::to_value(ObjectsResult { results }).unwrap(),
                        Vec::new(),
                    );
                }
                "objects.pull" => req.responder.reply(
                    serde_json::to_value(ObjectsResult { results: vec![] }).unwrap(),
                    Vec::new(),
                ),
                op => req.responder.error(ErrorWire::coded(
                    "UNSUPPORTED_OPERATION",
                    format!("{op}: not scripted"),
                )),
            }
        }
    }
    let worker = FakeWorker::start(REPLICA_A);
    let private = sha(b"a private object of ours");
    worker.state.lock().unwrap().private.insert(private.clone());
    let mut wanted: Vec<WantItem> = (0..32)
        .map(|index| WantItem {
            hash: sha(format!("not held here {index}").as_bytes()),
            size: 1000,
            offset: 0,
            priority: WantPriority::History,
        })
        .collect();
    wanted.push(WantItem {
        hash: private,
        size: 64,
        offset: 0,
        priority: WantPriority::History,
    });
    let peer = Arc::new(WantingPeer {
        wanted,
        pushes: AtomicUsize::new(0),
    });
    let (remote, code, endpoint, server) = scripted_peer(peer.clone());
    let a = Dialer::start(worker, &remote, &endpoint, 100);
    let (options, _) = a.options();
    let lane = a.run(a.dial(&endpoint, &code, LANE), options);
    assert!(
        wait_until(Duration::from_secs(20), || peer
            .pushes
            .load(Ordering::SeqCst)
            >= 2),
        "{:?}",
        a.world.finished()
    );
    drop(lane);
    let errors = a.world.object_errors();
    assert!(
        errors.is_empty(),
        "{} ObjectErrors: {errors:?}",
        errors.len()
    );
    server.join().unwrap().close();
}

/// §14 WP3.3, with two repositories (WP3.3 re-review major): pushes leave a
/// peer slot to pulls and reports. Both repositories push to a peer whose
/// workers stall every write (the peer keeps each push alive with PROGRESS
/// until its 60 s write deadline); pulls on the same lane still complete.
#[test]
fn stalled_pushes_of_two_repositories_leave_a_slot_to_pulls() {
    let (code, verifier) = Verifier::generate();
    let b_world = World::new(&format!("b-{}", uuid()), Some(verifier));
    let a_world = World::new(&format!("a-{}", uuid()), None);
    let mut workers = Vec::new();
    let mut entries = Vec::new();
    for index in 0..2u64 {
        let repo = format!("repo-{index}");
        let (replica_a, replica_b) = (format!("replica-a-{index}"), format!("replica-b-{index}"));
        let worker_a = FakeWorker::start(&replica_a);
        let worker_b = FakeWorker::start(&replica_b);
        a_world.folder_of(&format!("state-a-{index}"), &repo, &replica_a, &worker_a);
        b_world.folder_of(&format!("state-b-{index}"), &repo, &replica_b, &worker_b);
        // A holds a 4 MiB object B wants; B's worker stalls its writes.
        let up = worker_a.hold(content(5000 + index, 4 * MIB as usize));
        worker_b.want(&up, 4 * MIB);
        worker_b.faults().stall_writes = true;
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
    let (options, stats) = dialer.options();
    let lane = dialer.run(dialer.dial(&endpoint, &code, LANE), options);
    assert!(wait_until(Duration::from_secs(10), || a_world
        .events()
        .iter()
        .any(
            |event| matches!(event, StatusEvent::ExchangeStarted { op, .. } if op == "objects.push")
        )));
    // (Time for a second push to take the other slot, as it did.)
    sleep_ms(500);
    let (worker_a, worker_b) = &workers[0];
    let mut down = Vec::new();
    for index in 0..6 {
        let hash = worker_b.hold(content(5100 + index, 1536 * 1024));
        worker_a.want(&hash, 1536 * 1024);
        down.push(hash);
    }
    a_world.bump_sync("state-a-0");
    let pulled = wait_until(Duration::from_secs(20), || {
        down.iter().all(|hash| worker_a.has(hash))
    });
    let peak = LaneStats::get(&stats.peer_calls_peak);
    for (_, worker_b) in &workers {
        worker_b.faults().stall_writes = false;
    }
    assert!(
        pulled,
        "the pulls waited behind two stalled pushes (pulls {}, peer calls peak {peak})",
        LaneStats::get(&stats.pulls)
    );
    assert!(peak <= objects::PEER_CALLS as u64);
    drop(lane);
    server.shutdown(Duration::from_secs(5));
    for (a, b) in workers {
        a.shutdown();
        b.shutdown();
    }
}

/// An object that settles in a round's sink (here a resume point: the
/// worker's durable offset is ahead of the want's) is no longer pulled in
/// that round (WP3.3 re-review minor): the rest of it would be read at the
/// source, sent and dropped. It resumes from the durable offset.
#[test]
fn a_settled_object_is_not_pulled_again_in_its_round() {
    let (a, b) = pair(100);
    let size = 48 * MIB;
    let data = content(5200, size as usize);
    let hash = b.worker.hold(data.clone());
    a.worker.want(&hash, size);
    a.worker.advance_spool(&hash, &data[..MIB as usize]);
    a.worker.faults().stale_want_offsets = 1;
    let (options, _) = a.options();
    let lane = a.run(a.dial(&b.endpoint, &b.code, LANE), options);
    assert!(
        wait_until(Duration::from_secs(60), || a.worker.has(&hash)),
        "not installed"
    );
    drop(lane);
    assert_eq!(a.worker.held_bytes(&hash).unwrap(), data);
    let read = b.worker.bytes_read.load(Ordering::Relaxed);
    assert!(
        read <= size + 24 * MIB,
        "the source read {} MiB for a {} MiB object",
        read / MIB,
        size / MIB
    );
}

/// Invariant 22 at the dialer (WP3.3 re-review minor): a pull stream's
/// empty chunk is accepted only as the one chunk of an empty span (a
/// zero-size object). Empty chunks carry no bytes but reset the call's idle
/// deadline, so a peer streaming them could hold the pull, its slot and the
/// lane's window forever; the first one ends the pull `PROTOCOL_VIOLATION`.
#[test]
fn empty_chunks_cannot_hold_a_pull() {
    struct DrippingPeer {
        hash: String,
        data: Vec<u8>,
    }
    impl Handler for DrippingPeer {
        fn handle(&self, req: Incoming) {
            match req.op.as_str() {
                "objects.pull" => {
                    let size = self.data.len() as u64;
                    let chunk = |offset: u64, last: bool| ChunkHeader {
                        id: 0,
                        hash: Some(self.hash.clone()),
                        offset,
                        total: size,
                        last,
                    };
                    for _ in 0..30 {
                        if req.responder.cancelled()
                            || req.responder.chunk(chunk(0, false), &[]).is_err()
                        {
                            return;
                        }
                        sleep_ms(200);
                    }
                    if req.responder.chunk(chunk(0, true), &self.data).is_err() {
                        return;
                    }
                    let result = ObjectsResult {
                        results: vec![ObjectResult {
                            hash: self.hash.clone(),
                            bytes: size,
                            outcome: ObjectOutcome::Completed,
                        }],
                    };
                    req.responder
                        .reply(serde_json::to_value(result).unwrap(), Vec::new());
                }
                "objects.wants" => req.responder.reply(
                    serde_json::to_value(ObjectsWantsReply { items: vec![] }).unwrap(),
                    Vec::new(),
                ),
                op => req.responder.error(ErrorWire::coded(
                    "UNSUPPORTED_OPERATION",
                    format!("{op}: not scripted"),
                )),
            }
        }
    }
    let data = content(5300, 100_000);
    let hash = sha(&data);
    let (remote, code, endpoint, server) = scripted_peer(Arc::new(DrippingPeer {
        hash: hash.clone(),
        data,
    }));
    let a = Dialer::start(FakeWorker::start(REPLICA_A), &remote, &endpoint, 100);
    a.worker.want(&hash, 100_000);
    let (mut options, _) = a.options();
    options.idle = Duration::from_secs(2);
    let lane = a.run(a.dial(&endpoint, &code, LANE), options);
    let ended = || {
        a.world
            .finished()
            .into_iter()
            .find(|f| f.op == "objects.pull")
    };
    assert!(
        wait_until(Duration::from_secs(10), || ended().is_some()),
        "no pull ended"
    );
    drop(lane);
    let first = ended().unwrap();
    assert!(
        !first.ok
            && first
                .error
                .as_ref()
                .is_some_and(|error| error.code == "PROTOCOL_VIOLATION"),
        "{first:?}"
    );
    assert!(first.duration_ms < 2000, "{first:?}");
    assert!(!a.worker.has(&hash));
    server.join().unwrap().close();
}

/// §2.3 (v3 network re-review major): one repository whose source stalls
/// (here every read of its worker waits, as a first-serve verify of a large
/// object does) never stops another repository's downloads on the same
/// lane: a pull reserves at most its key's share of the lane window.
#[test]
fn a_stalled_source_leaves_the_window_to_other_repositories() {
    let (code, verifier) = Verifier::generate();
    let b_world = World::new(&format!("b-{}", uuid()), Some(verifier));
    let a_world = World::new(&format!("a-{}", uuid()), None);
    let mut workers = Vec::new();
    let mut entries = Vec::new();
    for index in 0..2u64 {
        let repo = format!("repo-{index}");
        let (replica_a, replica_b) = (format!("replica-a-{index}"), format!("replica-b-{index}"));
        let worker_a = FakeWorker::start(&replica_a);
        let worker_b = FakeWorker::start(&replica_b);
        a_world.folder_of(&format!("state-a-{index}"), &repo, &replica_a, &worker_a);
        b_world.folder_of(&format!("state-b-{index}"), &repo, &replica_b, &worker_b);
        entries.push(entry_of(&repo, &replica_b));
        workers.push((worker_a, worker_b));
    }
    // Repository 0: a 24 MiB object whose source stalls every read.
    let (x_a, x_b) = (workers[0].0.clone(), workers[0].1.clone());
    let large = x_b.hold(content(5400, 24 * MIB as usize));
    x_a.want(&large, 24 * MIB);
    x_b.faults().stall_reads = true;
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
        worker: x_a.clone(),
        ctx: a_ctx,
        peer,
    };
    let (options, stats) = dialer.options();
    let lane = dialer.run(dialer.dial(&endpoint, &code, LANE), options);
    assert!(
        wait_until(Duration::from_secs(10), || {
            a_world.events().iter().any(|event| matches!(
            event,
            StatusEvent::ExchangeStarted { op, repo, .. } if op == "objects.pull" && repo == "repo-0"
        ))
        }),
        "repository 0 never pulled"
    );
    sleep_ms(300);
    // Repository 1 wants three small objects from a healthy source.
    let (y_a, y_b) = (workers[1].0.clone(), workers[1].1.clone());
    let small: Vec<String> = (0..3)
        .map(|index| {
            let hash = y_b.hold(content(5500 + index, 256 * 1024));
            y_a.want(&hash, 256 * 1024);
            hash
        })
        .collect();
    let began = Instant::now();
    a_world.bump_sync("state-a-1");
    let arrived = wait_until(Duration::from_secs(10), || {
        small.iter().all(|hash| y_a.has(hash))
    });
    let took = began.elapsed();
    let window_peak = LaneStats::get(&stats.window_peak);
    x_b.faults().stall_reads = false;
    assert!(
        arrived && took < Duration::from_secs(5),
        "repository 1 waited {took:?} behind repository 0's stalled pull (window peak {window_peak})"
    );
    assert!(window_peak <= 16 * MIB);
    drop(lane);
    server.shutdown(Duration::from_secs(5));
    for (a, b) in workers {
        a.shutdown();
        b.shutdown();
    }
}

/// An object whose size nothing records (a legacy event without a size
/// descriptor; the worker wants it with size 0) moves both ways (objects
/// re-review major): its sender's one empty chunk (a pull) or our worker's
/// read (a push) gives its size, and the receiving worker accepts any total
/// when it knows none.
#[test]
fn objects_of_unknown_size_travel_both_ways() {
    for up in [false, true] {
        let (a, b) = pair(100);
        let (holder, wanter) = if up {
            (&a.worker, &b.worker)
        } else {
            (&b.worker, &a.worker)
        };
        let data = content(5600, 300_000);
        let hash = holder.hold(data.clone());
        wanter.want_unsized(&hash);
        let (options, _) = a.options();
        let lane = a.run(a.dial(&b.endpoint, &b.code, LANE), options);
        assert!(
            wait_until(Duration::from_secs(20), || wanter.has(&hash)),
            "up: {up}: not installed; errors {:?} {:?}",
            a.world.object_errors(),
            b.world.object_errors()
        );
        drop(lane);
        assert_eq!(wanter.held_bytes(&hash).unwrap(), data);
        assert!(
            a.world.object_errors().is_empty(),
            "{:?}",
            a.world.object_errors()
        );
        assert!(
            b.world.object_errors().is_empty(),
            "{:?}",
            b.world.object_errors()
        );
    }
}

/// A source that could not serve an object is asked again as soon as its
/// published content grows (v3 network re-review major, a relay): not after
/// its per-object backoff (30 s, doubling up to 1 h).
#[test]
fn a_source_whose_content_grows_is_asked_again_at_once() {
    let (a, b) = pair(100);
    let data = content(5700, 200_000);
    let hash = sha(&data);
    a.worker.want(&hash, 200_000);
    let (options, _) = a.options();
    let lane = a.run(a.dial(&b.endpoint, &b.code, LANE), options);
    // B lacks it: A's pull fails and backs the object off for B.
    assert!(
        wait_until(Duration::from_secs(10), || a
            .world
            .object_errors()
            .iter()
            .any(|(object, code)| *object == hash && code == "NOT_AVAILABLE")),
        "{:?}",
        a.world.object_errors()
    );
    sleep_ms(300);
    // B gets it (a relay that received it), and its catalog entry says so.
    b.worker.hold(data.clone());
    let began = Instant::now();
    a.peer.catalog.update(|catalog| {
        let mut next = catalog.clone();
        next.generation += 1;
        for entry in &mut next.entries {
            entry.content = Some(ContentCounters {
                shared_objects: 1,
                shared_available: 1,
                ..ContentCounters::default()
            });
        }
        next
    });
    assert!(
        wait_until(Duration::from_secs(10), || a.worker.has(&hash)),
        "the object waited for its backoff after B got it"
    );
    assert!(began.elapsed() < Duration::from_secs(10));
    drop(lane);
    assert_eq!(a.worker.held_bytes(&hash).unwrap(), data);
}

/// An object our worker stops wanting while it moves (its staged event was
/// quarantined: the write answers `Unsolicited`) settles without an
/// `ObjectError` or a backoff (objects re-review minor): it is nobody's
/// failure.
#[test]
fn an_object_no_longer_wanted_settles_without_an_error() {
    let (a, b) = pair(100);
    let hash = b.worker.hold(content(5800, 2 * MIB as usize));
    a.worker.want(&hash, 2 * MIB);
    a.worker.faults().stall_writes = true;
    let (options, _) = a.options();
    let lane = a.run(a.dial(&b.endpoint, &b.code, LANE), options);
    assert!(
        wait_until(Duration::from_secs(10), || b
            .worker
            .reads()
            .iter()
            .any(|(read, _, _)| *read == hash)),
        "never pulled"
    );
    a.worker.unwant(&hash);
    a.worker.faults().stall_writes = false;
    assert!(
        wait_until(Duration::from_secs(10), || a
            .worker
            .write_log
            .lock()
            .unwrap()
            .iter()
            .any(|(_, written, _, _)| *written == hash)),
        "never written"
    );
    sleep_ms(1000);
    drop(lane);
    let errors = a.world.object_errors();
    assert!(errors.is_empty(), "{errors:?}");
}

/// Direction coupling on a slow uplink (v3 network re-review major): a pull's
/// request travels up the link a push saturates; it waits only behind what
/// the session handed the socket (its send buffer follows the link's drain
/// rate, and object bytes go out in 256 KiB frames), not behind megabytes, so
/// the pull completes in seconds, not tens of seconds.
#[test]
fn a_saturating_push_does_not_delay_pulls_on_a_slow_uplink() {
    let (a, b) = pair(100);
    let proxy = FaultProxy::start(&b.endpoint);
    proxy.set_bandwidth(256 * 1024);
    let (options, _) = a.options();
    let lane = a.run(a.dial(&proxy.endpoint(), &b.code, LANE), options);
    let pull = |seed: u64| {
        let hash = b.worker.hold(content(seed, 64 * 1024));
        a.worker.want(&hash, 64 * 1024);
        let began = Instant::now();
        a.world.bump_sync(STATE_A);
        let done = wait_until(Duration::from_secs(60), || a.worker.has(&hash));
        (done, began.elapsed())
    };
    let (done, idle) = pull(6000);
    assert!(done, "the idle pull never completed");
    // A saturates its uplink: three 6 MiB objects B wants.
    let mut up = Vec::new();
    for index in 0..3 {
        let hash = a.worker.hold(content(6100 + index, 6 * MIB as usize));
        b.worker.want(&hash, 6 * MIB);
        up.push(hash);
    }
    assert!(
        wait_until(Duration::from_secs(10), || proxy.bytes_up() > MIB),
        "the push never started"
    );
    // (The uplink's buffers are full by now.)
    sleep_ms(1500);
    let (done, loaded) = pull(6200);
    assert!(
        up.iter().any(|hash| !b.worker.has(hash)),
        "the push ended before the pull: the test proves nothing"
    );
    drop(lane);
    assert!(done, "the pull under the push never completed");
    // 256 KiB/s: the proxy itself queues about 2 MiB (8 s); with a fixed
    // 4 MiB send buffer and 1 MiB frames the pull took 18-19 s.
    assert!(
        loaded < Duration::from_secs(12),
        "a 64 KiB pull took {loaded:?} under a push (idle: {idle:?})"
    );
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

/// The §15.1 W3 floors these scenarios check (at their default sizes; an
/// override of `TKFS_QUAL_LARGE_MIB` / `TKFS_QUAL_TINY_COUNT` only records).
const FLOOR_LOOPBACK_MIB_S: f64 = 40.0;
const FLOOR_LINK_UTILIZATION: f64 = 0.80;
const FLOOR_TINY_FILES_S: f64 = 300.0;

/// What a large-object scenario measured.
struct Large {
    mib_s: f64,
    link_utilization: Option<f64>,
    /// The default 1 GiB (the size the floors are defined for).
    default_size: bool,
}

fn large_object(link: Option<Link>, scenario: &str, disk: bool) -> Large {
    let configured = std::env::var("TKFS_QUAL_LARGE_MIB")
        .ok()
        .and_then(|mib| mib.parse::<u64>().ok());
    let size = configured.unwrap_or(1024) * MIB;
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
    let mut link_utilization = None;
    if let Some(link) = &link {
        let link_mib_s = link.bytes_per_s as f64 / MIB as f64;
        extra["rtt_ms"] = json!(link.rtt_ms);
        extra["link_bytes_per_s"] = json!(link.bytes_per_s);
        if link.bytes_per_s > 0 {
            let ratio = mib_s / link_mib_s;
            link_utilization = Some(ratio);
            record(scenario, "link_utilization", ratio, "ratio", extra.clone());
        }
    }
    record(scenario, "mib_per_s", mib_s, "MiB/s", extra);
    Large {
        mib_s,
        link_utilization,
        default_size: configured.is_none(),
    }
}

/// What a tiny-files scenario measured: files/s, and whether it ran the
/// default 10K files.
fn tiny_files(link: Option<Link>, scenario: &str, disk: bool) -> (f64, bool) {
    let configured = std::env::var("TKFS_QUAL_TINY_COUNT")
        .ok()
        .and_then(|count| count.parse::<usize>().ok());
    let count = configured.unwrap_or(10_000);
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
    let files_s = count as f64 / seconds;
    record(scenario, "files_per_s", files_s, "files/s", extra);
    (files_s, configured.is_none())
}

#[test]
#[ignore = "WP3.3 qualification (release)"]
fn qual_large_object_loopback() {
    let large = large_object(None, "objects_v3_lanes::large_object_loopback", true);
    if large.default_size {
        assert!(
            large.mib_s >= FLOOR_LOOPBACK_MIB_S,
            "1 GiB on loopback at {:.1} MiB/s, below the {FLOOR_LOOPBACK_MIB_S} MiB/s floor",
            large.mib_s
        );
    }
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

/// §14 WP3.3 acceptance: through FaultProxy (50 ms RTT, 100 Mbit) ≥ 80 % of
/// the link rate.
#[test]
#[ignore = "WP3.3 qualification (release)"]
fn qual_large_object_rtt50_100mbit() {
    let large = large_object(
        Some(Link {
            rtt_ms: 50,
            bytes_per_s: 12_500_000,
        }),
        "objects_v3_lanes::large_object_rtt50_100mbit",
        true,
    );
    if large.default_size {
        let ratio = large.link_utilization.unwrap_or(0.0);
        assert!(
            ratio >= FLOOR_LINK_UTILIZATION,
            "1 GiB through 50 ms / 100 Mbit at {ratio:.3} of the link rate, below the \
             {FLOOR_LINK_UTILIZATION} floor"
        );
    }
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
    let (files_s, default_count) =
        tiny_files(None, "objects_v3_lanes::tiny_files_10k_loopback", true);
    if default_count {
        assert!(
            files_s >= FLOOR_TINY_FILES_S,
            "10K × 4 KiB on loopback at {files_s:.0} files/s, below the {FLOOR_TINY_FILES_S} \
             files/s floor"
        );
    }
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

/// The status scope of `hash` in A's folder.
fn object_scope(hash: &str) -> Scope {
    Scope::Object {
        state: STATE_A.into(),
        hash: hash.into(),
    }
}
