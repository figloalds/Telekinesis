//! WP2.6 acceptance (REVAMP_PLAN §14 WP2.6): the v3 `PeerManager`, its
//! per-peer supervisors, lanes and catalog client, against fake v3 servers
//! built from `session::accept` with a test handler, an emulated legacy (v2)
//! server, and a listener that drops every connection. The local side runs a
//! real `Catalog` (temporary data directory) and a real `StatusHub`.
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc, Barrier, Condvar, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tkfs::{
    model::{
        errors::{ErrorClass, ErrorWire},
        orchestrator::{
            CatalogApi, CatalogCmd, CatalogSnapshot, Ctx, DaemonEndpoints, EntryState, FolderCmd,
            FolderDirectory, FolderView, NetworkApi, PeerRecord, PublishedEntry, ShutdownOutcome,
            StatusSink, WorkerClient,
        },
        protocol::{CatalogListReply, LaneRole, LaneSpec, frame::ClientHello},
        status::{LaneState, PeerConnState, StatusEvent},
    },
    orchestrator::{
        Config, Network,
        catalog::Catalog,
        hub::{HubConfig, StatusHub},
        peer::{LaneView, PeerManager, PeerOptions, parse_catalog_reply},
        published::PublishedCatalog,
    },
    pairing_code::Verifier,
    session::{self, AcceptPolicy, Conn, ConnOptions, Handler, Incoming, PeerInfo, Reply},
    util::{cancel::Cancel, snapshot::Snapshot, time::SystemClock},
    ws,
};

const SERVER: &str = "peer-server-installation";

// ---------------------------------------------------------------------------
// Local side: a real catalog and hub, no folders.

/// Records every status event (with when it arrived) and forwards it to a
/// real hub.
struct Recorder {
    hub: Arc<StatusHub>,
    events: Mutex<Vec<(Instant, u64, StatusEvent)>>,
    /// When set, the next `LaneState{Dialing}` panics (once): a lane thread
    /// that dies right after it took the probe.
    panic_on_dialing: AtomicBool,
}

impl StatusSink for Recorder {
    fn record(&self, event: StatusEvent) {
        if matches!(
            event,
            StatusEvent::LaneState {
                state: LaneState::Dialing,
                ..
            }
        ) && self.panic_on_dialing.swap(false, Ordering::SeqCst)
        {
            panic!("injected: the status sink panics on a Dialing lane");
        }
        self.events.lock().unwrap().push((
            Instant::now(),
            tkfs::util::time::now_ms(),
            event.clone(),
        ));
        self.hub.record(event);
    }
}

impl Recorder {
    fn events(&self) -> Vec<(Instant, u64, StatusEvent)> {
        self.events.lock().unwrap().clone()
    }

    /// `(at, wall ms, state, error, backoff_until_ms)` of one lane's
    /// `LaneState` events, in order.
    #[allow(clippy::type_complexity)]
    fn lane(
        &self,
        installation: &str,
        lane: u32,
    ) -> Vec<(Instant, u64, LaneState, Option<ErrorWire>, Option<u64>)> {
        self.events()
            .into_iter()
            .filter_map(|(at, ms, event)| match event {
                StatusEvent::LaneState {
                    installation: i,
                    lane: l,
                    state,
                    error,
                    backoff_until_ms,
                    ..
                } if i == installation && l == lane => {
                    Some((at, ms, state, error, backoff_until_ms))
                }
                _ => None,
            })
            .collect()
    }

    fn count(&self, matches: impl Fn(&StatusEvent) -> bool) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, _, event)| matches(event))
            .count()
    }

    /// The index of the (single) `PeerRemoved` of `installation`.
    fn removed_at(&self, installation: &str) -> usize {
        let events = self.events();
        let removed: Vec<usize> = events
            .iter()
            .enumerate()
            .filter(|(_, (_, _, event))| {
                matches!(event, StatusEvent::PeerRemoved { installation: i } if i == installation)
            })
            .map(|(index, _)| index)
            .collect();
        assert_eq!(removed.len(), 1, "one PeerRemoved for {installation}");
        removed[0]
    }

    /// Every event of `installation` (lanes, catalog, configuration) from
    /// index `from` on.
    fn of_peer_from(&self, installation: &str, from: usize) -> Vec<StatusEvent> {
        self.events()
            .into_iter()
            .skip(from)
            .map(|(_, _, event)| event)
            .filter(|event| match event {
                StatusEvent::LaneState {
                    installation: i, ..
                }
                | StatusEvent::PeerCatalog {
                    installation: i, ..
                }
                | StatusEvent::PeerConfigured {
                    installation: i, ..
                } => i == installation,
                _ => false,
            })
            .collect()
    }

    /// The last `LaneState` of each lane of `installation` among the first
    /// `until` events.
    fn last_lane_states(&self, installation: &str, pool: u32, until: usize) -> Vec<LaneState> {
        let events = self.events();
        (0..pool)
            .map(|lane| {
                events[..until]
                    .iter()
                    .rev()
                    .find_map(|(_, _, event)| match event {
                        StatusEvent::LaneState {
                            installation: i,
                            lane: l,
                            state,
                            ..
                        } if i == installation && *l == lane => Some(*state),
                        _ => None,
                    })
                    .unwrap_or_else(|| panic!("lane {lane} of {installation} never reported"))
            })
            .collect()
    }
}

/// No folders: the peer stack needs none.
struct NoFolders;

impl FolderDirectory for NoFolders {
    fn view(&self, _: &str) -> Option<Arc<FolderView>> {
        None
    }
    fn by_repo(&self, _: &str) -> Option<Arc<FolderView>> {
        None
    }
    fn all(&self) -> Vec<Arc<FolderView>> {
        Vec::new()
    }
    fn send(&self, state: &str, _: FolderCmd) -> Result<(), ErrorWire> {
        Err(ErrorWire::coded("FOLDER_NOT_FOUND", state))
    }
    fn request(
        &self,
        state: &str,
        _: FolderCmd,
        _: Duration,
    ) -> Result<Arc<FolderView>, ErrorWire> {
        Err(ErrorWire::coded("FOLDER_NOT_FOUND", state))
    }
    fn client(&self, _: &str) -> Option<Arc<dyn WorkerClient>> {
        None
    }
    fn generation(&self) -> u64 {
        0
    }
    fn wait_newer(&self, generation: u64, timeout: Duration) -> u64 {
        thread::sleep(timeout.min(Duration::from_millis(50)));
        generation
    }
    fn shutdown(&self, _: Duration) -> Vec<ShutdownOutcome> {
        Vec::new()
    }
}

struct Local {
    _dir: tempfile::TempDir,
    catalog: Arc<Catalog>,
    ctx: Arc<Ctx>,
    recorder: Arc<Recorder>,
    hub: Arc<StatusHub>,
    installation: String,
}

fn local() -> Local {
    local_with(|catalog| catalog)
}

/// [`local`] whose `ctx.catalog` is `wrap(real catalog)`.
fn local_with(wrap: impl FnOnce(Arc<Catalog>) -> Arc<dyn CatalogApi>) -> Local {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        format_version: 2,
        data_directory: dir.path().join("data"),
        default_mount_directory: dir.path().join("mounts"),
        maximum_running: 8,
        network: Network {
            listen: None,
            connections: 2,
            sync_interval_ms: 500,
        },
    };
    let catalog = Catalog::open(&config).unwrap();
    let installation = catalog.snapshot().installation.clone();
    assert!(!installation.is_empty());
    let hub = StatusHub::new(HubConfig::default(), Arc::new(SystemClock));
    let recorder = Arc::new(Recorder {
        hub: hub.clone(),
        events: Mutex::new(Vec::new()),
        panic_on_dialing: AtomicBool::new(false),
    });
    let ctx = Arc::new(Ctx {
        installation: installation.clone(),
        config: Arc::new(config),
        catalog: wrap(catalog.clone()),
        folders: Arc::new(NoFolders),
        published: PublishedCatalog::new(),
        status: recorder.clone(),
        status_source: hub.clone(),
        network: OnceLock::new(),
        legacy: OnceLock::new(),
        clock: Arc::new(SystemClock),
        shutdown: Cancel::new(),
        stopping: Arc::new(AtomicBool::new(false)),
        started_at_ms: tkfs::util::time::now_ms(),
        endpoints: Snapshot::new(DaemonEndpoints::default()),
        extensions: OnceLock::new(),
    });
    Local {
        _dir: dir,
        catalog,
        ctx,
        recorder,
        hub,
        installation,
    }
}

impl Local {
    /// Stores a peer record directly (what a successful pair stores).
    fn add_peer(&self, endpoint: &str, code: &str, installation: &str, connections: u32) {
        self.catalog
            .apply(CatalogCmd::UpsertPeer {
                record: PeerRecord {
                    endpoint: endpoint.into(),
                    code: code.into(),
                    installation: installation.into(),
                    connections,
                    revision: String::new(),
                },
            })
            .unwrap();
    }

    fn peer_state(&self, installation: &str) -> Option<PeerConnState> {
        self.hub
            .snapshot_now()
            .peers
            .iter()
            .find(|peer| peer.installation == installation)
            .map(|peer| peer.state)
    }

    /// Every recorded event and the hub snapshot as text (redaction checks).
    fn everything_reported(&self) -> String {
        let events: Vec<Value> = self
            .recorder
            .events()
            .into_iter()
            .map(|(_, _, event)| serde_json::to_value(event).unwrap())
            .collect();
        format!(
            "{}\n{}",
            serde_json::to_string(&events).unwrap(),
            serde_json::to_string(&*self.hub.snapshot_now()).unwrap()
        )
    }
}

/// Short timers so the state machines are observable within a test.
fn fast() -> PeerOptions {
    let mut options = PeerOptions {
        lane_backoff_base: Duration::from_millis(100),
        lane_backoff_cap: Duration::from_millis(800),
        incompatible_recheck: Duration::from_secs(2),
        unauthorized_recheck: Duration::from_millis(1500),
        catalog_poll: Duration::from_millis(100),
        rtt_refresh: Duration::from_millis(200),
        ..PeerOptions::default()
    };
    options.conn.heartbeat = Duration::from_millis(200);
    options
}

fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if condition() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn lanes(manager: &PeerManager, installation: &str) -> Vec<LaneView> {
    manager
        .peer(installation)
        .map(|peer| peer.lanes)
        .unwrap_or_default()
}

fn all_lanes(manager: &PeerManager, installation: &str, state: LaneState) -> bool {
    let lanes = lanes(manager, installation);
    !lanes.is_empty() && lanes.iter().all(|lane| lane.state == state)
}

fn entry(repo: &str, generation: u64) -> PublishedEntry {
    PublishedEntry {
        repo: repo.into(),
        replica: format!("{repo}-replica"),
        label: repo.into(),
        branches: Vec::new(),
        state: EntryState::Running,
        tip: None,
        sync_generation: generation,
        content: None,
        error: None,
        updated_at_ms: 1,
    }
}

// ---------------------------------------------------------------------------
// A fake v3 peer: `session::accept` plus a test handler.

struct Policy {
    installation: Mutex<String>,
    verifier: Mutex<Option<(Verifier, u64)>>,
    hellos: Mutex<Vec<ClientHello>>,
}

impl AcceptPolicy for Policy {
    fn installation(&self) -> String {
        self.installation.lock().unwrap().clone()
    }
    fn verifier(&self) -> Option<(Verifier, u64)> {
        self.verifier.lock().unwrap().clone()
    }
    fn admit(&self, hello: &ClientHello) -> Result<(), ErrorWire> {
        self.hellos.lock().unwrap().push(hello.clone());
        Ok(())
    }
    fn legacy_attempt(&self, _: SocketAddr, _: &Value) {}
}

#[derive(Default)]
struct ServerState {
    tcp: AtomicUsize,
    handshake_failures: AtomicUsize,
    sessions: Mutex<Vec<(PeerInfo, Conn)>>,
    catalog: Mutex<(u64, Vec<PublishedEntry>)>,
    /// `since` of every `catalog.list`, per session id.
    since: Mutex<Vec<(String, Option<u64>)>>,
    reply_in_header: AtomicBool,
    hang_catalog: AtomicBool,
    /// Sessions to close right after they are established.
    close_next: AtomicUsize,
    stopped: AtomicBool,
}

struct FakePeer {
    endpoint: String,
    addr: SocketAddr,
    policy: Arc<Policy>,
    state: Arc<ServerState>,
    accept: Option<JoinHandle<()>>,
}

struct CatalogHandler {
    state: Arc<ServerState>,
    session: String,
}

impl Handler for CatalogHandler {
    fn handle(&self, req: Incoming) {
        match req.op.as_str() {
            "catalog.list" => {
                let since = req.header.get("since").and_then(Value::as_u64);
                self.state
                    .since
                    .lock()
                    .unwrap()
                    .push((self.session.clone(), since));
                // Hung while `hang_catalog` is set (until the request is
                // cancelled or 30 s pass), then answered normally.
                let until = Instant::now() + Duration::from_secs(30);
                while self.state.hang_catalog.load(Ordering::SeqCst)
                    && !req.responder.cancelled()
                    && Instant::now() < until
                {
                    thread::sleep(Duration::from_millis(20));
                }
                if req.responder.cancelled() {
                    return;
                }
                let (generation, entries) = self.state.catalog.lock().unwrap().clone();
                let unchanged = since == Some(generation);
                let reply = CatalogListReply {
                    generation,
                    unchanged,
                    entries: if unchanged { Vec::new() } else { entries },
                };
                if self.state.reply_in_header.load(Ordering::SeqCst) {
                    req.responder
                        .reply(serde_json::to_value(reply).unwrap(), Vec::new());
                } else {
                    req.responder
                        .reply(json!({}), serde_json::to_vec(&reply).unwrap());
                }
            }
            "ping" => req.responder.reply(json!({"alive": true}), Vec::new()),
            op => req.responder.error(ErrorWire::coded(
                "UNKNOWN_PEER_OPERATION",
                format!("the fake peer does not serve {op}"),
            )),
        }
    }
}

fn server_options() -> ConnOptions {
    ConnOptions {
        heartbeat: Duration::from_millis(200),
        ..ConnOptions::default()
    }
}

impl FakePeer {
    fn start(installation: &str) -> (FakePeer, String) {
        let (code, verifier) = Verifier::generate();
        (Self::with_verifier(installation, Some((verifier, 1))), code)
    }

    fn with_verifier(installation: &str, verifier: Option<(Verifier, u64)>) -> FakePeer {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let policy = Arc::new(Policy {
            installation: Mutex::new(installation.into()),
            verifier: Mutex::new(verifier),
            hellos: Mutex::new(Vec::new()),
        });
        let state = Arc::new(ServerState::default());
        let accept = {
            let policy = policy.clone();
            let state = state.clone();
            thread::spawn(move || {
                for stream in listener.incoming() {
                    if state.stopped.load(Ordering::SeqCst) {
                        break;
                    }
                    let Ok(stream) = stream else { continue };
                    state.tcp.fetch_add(1, Ordering::SeqCst);
                    let policy: Arc<dyn AcceptPolicy> = policy.clone();
                    let state = state.clone();
                    thread::spawn(move || {
                        match session::accept(stream, policy, server_options()) {
                            Ok((conn, peer)) => {
                                conn.serve(Arc::new(CatalogHandler {
                                    state: state.clone(),
                                    session: peer.session.clone(),
                                }));
                                let close = state
                                    .close_next
                                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                                        n.checked_sub(1)
                                    })
                                    .is_ok();
                                state.sessions.lock().unwrap().push((peer, conn.clone()));
                                if close {
                                    conn.close();
                                }
                            }
                            Err(_) => {
                                state.handshake_failures.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    });
                }
            })
        };
        FakePeer {
            endpoint: format!("ws://{addr}"),
            addr,
            policy,
            state,
            accept: Some(accept),
        }
    }

    fn set_catalog(&self, generation: u64, entries: Vec<PublishedEntry>) {
        *self.state.catalog.lock().unwrap() = (generation, entries);
    }

    /// Open sessions of `role`.
    fn open(&self, role: LaneRole) -> Vec<PeerInfo> {
        self.state
            .sessions
            .lock()
            .unwrap()
            .iter()
            .filter(|(peer, conn)| peer.role == role && conn.closed().is_none())
            .map(|(peer, _)| peer.clone())
            .collect()
    }

    fn open_count(&self) -> usize {
        self.state
            .sessions
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, conn)| conn.closed().is_none())
            .count()
    }

    fn established(&self) -> usize {
        self.state.sessions.lock().unwrap().len()
    }

    /// Closes the open sessions of `role`.
    fn close(&self, role: LaneRole) {
        for (peer, conn) in self.state.sessions.lock().unwrap().iter() {
            if peer.role == role {
                conn.close();
            }
        }
    }

    fn goaway_all(&self, code: &str) {
        for (_, conn) in self.state.sessions.lock().unwrap().iter() {
            conn.goaway(code, "the fake peer says goaway");
        }
    }

    /// Close reasons of the sessions that have closed.
    fn close_reasons(&self) -> Vec<String> {
        self.state
            .sessions
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(_, conn)| conn.closed().map(|reason| reason.code))
            .collect()
    }

    fn stop(&mut self) {
        self.state.stopped.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.addr);
        if let Some(accept) = self.accept.take() {
            let _ = accept.join();
        }
        for (_, conn) in self.state.sessions.lock().unwrap().iter() {
            conn.close();
        }
    }
}

impl Drop for FakePeer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// A listener that accepts and drops every connection, recording when.
struct DropListener {
    endpoint: String,
    accepts: Arc<Mutex<Vec<Instant>>>,
}

fn drop_listener() -> DropListener {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let accepts = Arc::new(Mutex::new(Vec::new()));
    let seen = accepts.clone();
    thread::spawn(move || {
        for stream in listener.incoming() {
            seen.lock().unwrap().push(Instant::now());
            drop(stream);
        }
    });
    DropListener { endpoint, accepts }
}

/// An emulated 6298d0b endpoint: plain `ws::accept` (no `X-Tkfs-Sync`
/// echo), recording connections and every data message received.
struct LegacyServer {
    endpoint: String,
    accepts: Arc<AtomicUsize>,
    received: Arc<Mutex<Vec<String>>>,
}

fn legacy_server() -> LegacyServer {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let accepts = Arc::new(AtomicUsize::new(0));
    let received = Arc::new(Mutex::new(Vec::new()));
    let (count, messages) = (accepts.clone(), received.clone());
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            count.fetch_add(1, Ordering::SeqCst);
            let messages = messages.clone();
            thread::spawn(move || {
                let Ok(mut socket) = ws::accept(stream) else {
                    return;
                };
                loop {
                    match socket.read() {
                        Ok(tungstenite::Message::Close(_)) | Err(_) => break,
                        Ok(tungstenite::Message::Ping(_) | tungstenite::Message::Pong(_)) => {}
                        Ok(message) => messages.lock().unwrap().push(message.to_string()),
                    }
                }
            });
        }
    });
    LegacyServer {
        endpoint,
        accepts,
        received,
    }
}

/// A TCP proxy in front of `target` that holds every server-to-client chunk
/// for `delay`: each handshake step of a dial through it takes `delay`, so a
/// dial takes about three times `delay` (upgrade, `HELLO_ACK`, `AUTH_OK`),
/// and it cannot be interrupted (CCR-WP2.6-2). End-of-stream is forwarded
/// both ways.
fn slow_proxy(target: SocketAddr, delay: Duration) -> String {
    delay_proxy(target, Duration::ZERO, delay)
}

/// A TCP proxy in front of `target` that holds every client-to-server chunk
/// for `up` and every server-to-client chunk for `down`.
fn delay_proxy(target: SocketAddr, up: Duration, down: Duration) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    thread::spawn(move || {
        for client in listener.incoming() {
            let Ok(client) = client else { continue };
            let Ok(server) = TcpStream::connect(target) else {
                continue;
            };
            let (Ok(client_out), Ok(server_in)) = (client.try_clone(), server.try_clone()) else {
                continue;
            };
            pump(client, server_in, up);
            pump(server, client_out, down);
        }
    });
    endpoint
}

fn pump(mut from: TcpStream, mut to: TcpStream, delay: Duration) {
    thread::spawn(move || {
        let mut buf = [0u8; 16 * 1024];
        loop {
            match from.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    thread::sleep(delay);
                    if to.write_all(&buf[..n]).is_err() {
                        break;
                    }
                }
            }
        }
        let _ = to.shutdown(Shutdown::Write);
        let _ = from.shutdown(Shutdown::Read);
    });
}

/// Every lane of `installation` in the hub's snapshot (outbound).
fn hub_lanes(node: &Local, installation: &str) -> Vec<LaneState> {
    node.hub
        .snapshot_now()
        .peers
        .iter()
        .find(|peer| peer.installation == installation)
        .map(|peer| peer.lanes.iter().map(|lane| lane.state).collect())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Lanes

#[test]
fn all_lanes_reach_ready_with_their_roles_and_lanes() {
    let node = local();
    let (server, code) = FakePeer::start(SERVER);
    server.set_catalog(5, vec![entry("repo-a", 1)]);
    node.add_peer(&server.endpoint, &code, SERVER, 4);
    let manager = PeerManager::start_with(node.ctx.clone(), fast());

    assert!(
        wait_until(Duration::from_secs(10), || all_lanes(
            &manager,
            SERVER,
            LaneState::Ready
        )),
        "{:?}",
        lanes(&manager, SERVER)
    );
    let view = manager.peer(SERVER).unwrap();
    assert_eq!(view.pool, 4);
    assert_eq!(view.endpoint, server.endpoint);
    assert_eq!(view.label, None, "no display name is known");
    assert!(
        view.lanes
            .iter()
            .all(|lane| lane.connects == 1 && lane.dials == 1)
    );
    assert!(
        view.lanes
            .iter()
            .all(|lane| lane.protocol.as_deref() == Some("tkfs-peer/3")),
        "{:?}",
        view.lanes
    );
    assert_eq!(view.lanes[0].role, LaneRole::Control);
    assert_eq!(view.lanes[0].spec, None);
    assert_eq!(view.lanes[3].spec, Some(LaneSpec::new(2, 3)));

    // One control session and three bulk sessions, lanes {0..3, 3}, pool 4.
    assert!(wait_until(Duration::from_secs(5), || server.open_count() == 4));
    assert_eq!(server.open(LaneRole::Control).len(), 1);
    let control = &server.open(LaneRole::Control)[0];
    assert_eq!(control.lane, None);
    assert_eq!(control.installation, node.installation);
    let mut bulk: Vec<LaneSpec> = server
        .open(LaneRole::Bulk)
        .iter()
        .map(|peer| peer.lane.unwrap())
        .collect();
    bulk.sort();
    assert_eq!(
        bulk,
        (0..3)
            .map(|index| LaneSpec::new(index, 3))
            .collect::<Vec<_>>()
    );
    for hello in server.policy.hellos.lock().unwrap().iter() {
        assert_eq!(hello.pool, 4);
        assert_eq!(hello.installation, node.installation);
    }

    // Every lane went Dialing → Handshaking → Ready, with its role.
    for lane in 0..4 {
        let states: Vec<LaneState> = node
            .recorder
            .lane(SERVER, lane)
            .into_iter()
            .map(|(_, _, state, _, _)| state)
            .collect();
        assert_eq!(
            &states[..3],
            &[LaneState::Dialing, LaneState::Handshaking, LaneState::Ready],
            "lane {lane}: {states:?}"
        );
    }
    assert_eq!(
        node.recorder.count(|event| matches!(
            event,
            StatusEvent::LaneState {
                lane: 0,
                role: LaneRole::Control,
                ..
            }
        )),
        node.recorder
            .count(|event| matches!(event, StatusEvent::LaneState { lane: 0, .. }))
    );
    assert_eq!(
        node.recorder.count(|event| matches!(
            event,
            StatusEvent::PeerConfigured { installation, endpoint, .. }
                if installation == SERVER && *endpoint == server.endpoint
        )),
        1
    );

    // The hub sees the peer online; the RTT arrives from the heartbeats.
    assert!(wait_until(Duration::from_secs(5), || node
        .peer_state(SERVER)
        == Some(PeerConnState::Online)));
    assert!(
        wait_until(Duration::from_secs(5), || node
            .hub
            .snapshot_now()
            .peers
            .iter()
            .any(|peer| peer.installation == SERVER && peer.rtt_ms.is_some())),
        "RTT reported"
    );
    // WP2.INT (CCR-WP1.11-2): the negotiated protocol reaches the hub.
    assert!(
        node.hub
            .snapshot_now()
            .peers
            .iter()
            .any(|peer| peer.installation == SERVER
                && peer.protocol.as_deref() == Some("tkfs-peer/3")),
        "protocol reported"
    );

    // Lane 0 recorded the peer as seen (off the lane thread) and listed its
    // catalog.
    assert!(wait_until(Duration::from_secs(5), || node
        .catalog
        .snapshot()
        .peer_meta
        .get(SERVER)
        .is_some_and(
            |meta| meta.last_seen_ms.is_some() && meta.incompatible.is_none()
        )));
    assert!(wait_until(Duration::from_secs(5), || {
        manager.context(SERVER).unwrap().catalog.load().entries == vec![entry("repo-a", 1)]
    }));
    assert_eq!(
        manager.context(SERVER).unwrap().catalog.load().generation,
        5
    );

    manager.shutdown(Duration::from_secs(5));
}

#[test]
fn catalog_client_polls_and_survives_a_control_reconnect() {
    let node = local();
    let (server, code) = FakePeer::start(SERVER);
    server.set_catalog(10, vec![entry("repo-a", 1), entry("repo-b", 1)]);
    node.add_peer(&server.endpoint, &code, SERVER, 3);
    let manager = PeerManager::start_with(node.ctx.clone(), fast());
    let context = || manager.context(SERVER).unwrap();
    let catalogs = || {
        node.recorder.count(
            |event| matches!(event, StatusEvent::PeerCatalog { installation, .. } if installation == SERVER),
        )
    };

    assert!(wait_until(Duration::from_secs(10), || context()
        .catalog
        .load()
        .entries
        .len()
        == 2));
    assert!(wait_until(Duration::from_secs(5), || all_lanes(
        &manager,
        SERVER,
        LaneState::Ready
    )));
    // Unchanged polls (`since` = the generation) emit nothing new.
    assert!(wait_until(Duration::from_secs(5), || server
        .state
        .since
        .lock()
        .unwrap()
        .len()
        >= 6));
    assert_eq!(catalogs(), 1);
    {
        let since = server.state.since.lock().unwrap().clone();
        assert_eq!(
            since[0].1, None,
            "a session's first listing asks for everything"
        );
        assert!(
            since[1..].iter().all(|(_, since)| *since == Some(10)),
            "{since:?}"
        );
    }

    // The control session drops: the last good catalog stays, the bulk lanes
    // stay up, lane 0 backs off and reconnects, and the new session's first
    // listing asks for the whole catalog again.
    let first_control = server.open(LaneRole::Control)[0].session.clone();
    server.close(LaneRole::Control);
    assert!(wait_until(Duration::from_secs(5), || lanes(
        &manager, SERVER
    )[0]
    .state
        != LaneState::Ready));
    assert_eq!(context().catalog.load().entries.len(), 2);
    assert!(wait_until(Duration::from_secs(10), || {
        let lanes = lanes(&manager, SERVER);
        lanes[0].state == LaneState::Ready && lanes[0].connects == 2
    }));
    assert!(
        lanes(&manager, SERVER)[1..]
            .iter()
            .all(|lane| lane.connects == 1 && lane.state == LaneState::Ready),
        "bulk lanes never depend on lane 0"
    );
    let states: Vec<LaneState> = node
        .recorder
        .lane(SERVER, 0)
        .into_iter()
        .map(|(_, _, state, _, _)| state)
        .collect();
    assert!(states.contains(&LaneState::Backoff), "{states:?}");
    assert!(wait_until(Duration::from_secs(5), || {
        server
            .state
            .since
            .lock()
            .unwrap()
            .iter()
            .any(|(session, since)| *session != first_control && since.is_none())
    }));
    assert_eq!(context().catalog.load().entries.len(), 2);

    // A change arrives at the next poll (the reply is read from the header
    // as well as from the body).
    server.state.reply_in_header.store(true, Ordering::SeqCst);
    server.set_catalog(11, vec![entry("repo-a", 2)]);
    assert!(wait_until(Duration::from_secs(2), || context()
        .catalog
        .load()
        .entries
        == vec![entry("repo-a", 2)]));
    assert_eq!(context().catalog.load().generation, 11);
    assert!(wait_until(Duration::from_secs(2), || catalogs() >= 2));
    let last = node
        .recorder
        .events()
        .into_iter()
        .rev()
        .find_map(|(_, _, event)| match event {
            StatusEvent::PeerCatalog { entries, .. } => Some(entries),
            _ => None,
        })
        .unwrap();
    assert_eq!(last, vec![entry("repo-a", 2)]);
    manager.shutdown(Duration::from_secs(5));
}

#[test]
fn catalog_replies_parse_from_the_body_or_the_header() {
    let reply = CatalogListReply {
        generation: 3,
        unchanged: false,
        entries: vec![entry("r", 1)],
    };
    let body = Reply {
        header: json!({}),
        body: serde_json::to_vec(&reply).unwrap(),
    };
    let header = Reply {
        header: serde_json::to_value(&reply).unwrap(),
        body: Vec::new(),
    };
    assert_eq!(parse_catalog_reply(&body).unwrap(), reply);
    assert_eq!(parse_catalog_reply(&header).unwrap(), reply);
    let broken = parse_catalog_reply(&Reply {
        header: json!({"generation": "x"}),
        body: Vec::new(),
    })
    .unwrap_err();
    assert_eq!(broken.code, "INVALID_REPLY");
}

/// `(event wall ms, backoff_until_ms)` → the declared delay in ms.
fn declared_delay(ms: u64, until: Option<u64>) -> u64 {
    until.unwrap().saturating_sub(ms)
}

#[test]
fn reconnect_backoff_stays_within_jitter_bounds() {
    let node = local();
    let target = drop_listener();
    node.add_peer(&target.endpoint, "some-code", SERVER, 2);
    let options = PeerOptions {
        lane_backoff_base: Duration::from_millis(100),
        lane_backoff_cap: Duration::from_millis(800),
        lane_backoff_jitter: 0.2,
        ..fast()
    };
    let manager = PeerManager::start_with(node.ctx.clone(), options);
    assert!(wait_until(Duration::from_secs(15), || {
        node.recorder
            .lane(SERVER, 0)
            .iter()
            .filter(|(_, _, state, _, _)| *state == LaneState::Backoff)
            .count()
            >= 7
    }));
    manager.shutdown(Duration::from_secs(5));
    assert!(target.accepts.lock().unwrap().len() >= 7);

    for lane in 0..2 {
        let events = node.recorder.lane(SERVER, lane);
        let mut attempt = 0u32;
        for (index, (at, ms, state, error, until)) in events.iter().enumerate() {
            if *state != LaneState::Backoff {
                continue;
            }
            attempt += 1;
            let error = error.as_ref().unwrap();
            assert_eq!(error.code, "WS_CONNECT_FAILED", "{error:?}");
            assert_eq!(error.class, ErrorClass::Transient);
            let nominal = (100u64 << (attempt - 1).min(10)).min(800);
            let delay = declared_delay(*ms, *until);
            assert!(
                delay + 2 >= nominal * 8 / 10 && delay <= nominal * 12 / 10 + 2,
                "lane {lane} attempt {attempt}: {delay} ms outside {nominal} ms ± 20 %"
            );
            // The next dial waits for the declared delay (and not much longer).
            if let Some((next, _, _, _, _)) = events[index + 1..]
                .iter()
                .find(|(_, _, state, _, _)| *state == LaneState::Dialing)
            {
                let waited = next.duration_since(*at).as_millis() as u64;
                assert!(
                    waited + 20 >= delay && waited <= delay + 500,
                    "lane {lane} attempt {attempt}: redialed after {waited} ms, declared {delay} ms"
                );
            }
        }
        assert!(attempt >= 6, "lane {lane}: {attempt} backoffs");
    }
}

#[test]
fn backoff_resets_after_a_healthy_session() {
    let node = local();
    let (server, code) = FakePeer::start(SERVER);
    // The first three sessions close as soon as they are established.
    server.state.close_next.store(3, Ordering::SeqCst);
    node.add_peer(&server.endpoint, &code, SERVER, 2);
    let options = PeerOptions {
        lane_backoff_base: Duration::from_millis(200),
        lane_backoff_cap: Duration::from_secs(5),
        lane_reset_after: Duration::from_millis(400),
        ..fast()
    };
    let manager = PeerManager::start_with(node.ctx.clone(), options);
    // Three flaps in total across both lanes, then both lanes stay up.
    assert!(wait_until(Duration::from_secs(15), || {
        server.established() >= 5 && all_lanes(&manager, SERVER, LaneState::Ready)
    }));
    let flapping: Vec<u64> = (0..2)
        .flat_map(|lane| node.recorder.lane(SERVER, lane))
        .filter(|(_, _, state, _, _)| *state == LaneState::Backoff)
        .map(|(_, ms, _, _, until)| declared_delay(ms, until))
        .collect();
    assert_eq!(flapping.len(), 3, "{flapping:?}");
    // A lane that flapped twice in a row escalated: some delay is ≥ 2 × base.
    let per_lane: Vec<Vec<u64>> = (0..2)
        .map(|lane| {
            node.recorder
                .lane(SERVER, lane)
                .into_iter()
                .filter(|(_, _, state, _, _)| *state == LaneState::Backoff)
                .map(|(_, ms, _, _, until)| declared_delay(ms, until))
                .collect()
        })
        .collect();
    for delays in &per_lane {
        for (attempt, delay) in delays.iter().enumerate() {
            let nominal = 200u64 << attempt;
            assert!(
                *delay + 2 >= nominal * 8 / 10 && *delay <= nominal * 12 / 10 + 2,
                "{per_lane:?}"
            );
        }
    }

    // Healthy for longer than `lane_reset_after`, then dropped: the next
    // delay starts from the base again.
    thread::sleep(Duration::from_millis(700));
    let before = node.recorder.lane(SERVER, 0).len();
    server.close(LaneRole::Control);
    assert!(wait_until(Duration::from_secs(5), || {
        node.recorder.lane(SERVER, 0)[before..]
            .iter()
            .any(|(_, _, state, _, _)| *state == LaneState::Backoff)
    }));
    let (_, ms, _, error, until) = node.recorder.lane(SERVER, 0)[before..]
        .iter()
        .find(|(_, _, state, _, _)| *state == LaneState::Backoff)
        .cloned()
        .unwrap();
    let delay = declared_delay(ms, until);
    assert!(
        (158..=242).contains(&delay),
        "after a healthy session the delay restarts at the base: {delay} ms"
    );
    assert!(error.is_some(), "the close reason is reported");
    assert!(wait_until(Duration::from_secs(5), || all_lanes(
        &manager,
        SERVER,
        LaneState::Ready
    )));
    manager.shutdown(Duration::from_secs(5));
}

#[test]
fn legacy_peer_is_incompatible_without_redial_before_the_recheck() {
    let node = local();
    let legacy = legacy_server();
    let code = "a-code-that-must-never-be-sent";
    node.add_peer(&legacy.endpoint, code, SERVER, 2);
    let options = PeerOptions {
        incompatible_recheck: Duration::from_secs(2),
        ..fast()
    };
    let manager = PeerManager::start_with(node.ctx.clone(), options);
    assert!(
        wait_until(Duration::from_secs(10), || all_lanes(
            &manager,
            SERVER,
            LaneState::Incompatible
        )),
        "{:?}",
        lanes(&manager, SERVER)
    );
    let blocked_at = Instant::now();
    for lane in lanes(&manager, SERVER) {
        let error = lane.error.unwrap();
        assert_eq!(error.code, "PEER_NEEDS_UPGRADE");
        assert_eq!(error.class, ErrorClass::Incompatible);
        assert!(lane.backoff_until_ms.is_some(), "the recheck time is shown");
    }
    let dials = legacy.accepts.load(Ordering::SeqCst);
    assert!(
        (1..=2).contains(&dials),
        "one dial per lane at most: {dials}"
    );
    assert!(wait_until(Duration::from_secs(5), || node
        .catalog
        .snapshot()
        .peer_meta
        .get(SERVER)
        .and_then(|meta| meta.incompatible.clone())
        .as_deref()
        == Some("PEER_NEEDS_UPGRADE")));
    assert!(wait_until(Duration::from_secs(5), || node
        .peer_state(SERVER)
        == Some(PeerConnState::Incompatible)));

    // No redial before the recheck interval.
    thread::sleep(Duration::from_millis(1500).saturating_sub(blocked_at.elapsed()));
    assert_eq!(legacy.accepts.load(Ordering::SeqCst), dials);

    // At the recheck, exactly one lane re-dials, and the peer stays blocked.
    assert!(wait_until(Duration::from_secs(4), || legacy
        .accepts
        .load(Ordering::SeqCst)
        == dials + 1));
    thread::sleep(Duration::from_millis(400));
    assert_eq!(legacy.accepts.load(Ordering::SeqCst), dials + 1);
    assert!(all_lanes(&manager, SERVER, LaneState::Incompatible));

    // An explicit reconfigure rechecks at once, with one dial.
    manager.reconfigure();
    assert!(wait_until(Duration::from_secs(2), || legacy
        .accepts
        .load(Ordering::SeqCst)
        == dials + 2));
    thread::sleep(Duration::from_millis(400));
    assert_eq!(legacy.accepts.load(Ordering::SeqCst), dials + 2);
    assert!(all_lanes(&manager, SERVER, LaneState::Incompatible));
    assert!(
        legacy.received.lock().unwrap().is_empty(),
        "a v3 dialer never sends anything to a legacy server"
    );
    assert!(!node.everything_reported().contains(code));
    manager.shutdown(Duration::from_secs(5));
}

#[test]
fn wrong_code_is_unauthorized_without_a_tight_retry_loop() {
    let node = local();
    let (server, code) = FakePeer::start(SERVER);
    let wrong = format!("{code}-wrong");
    node.add_peer(&server.endpoint, &wrong, SERVER, 3);
    let options = PeerOptions {
        unauthorized_recheck: Duration::from_secs(1),
        ..fast()
    };
    let manager = PeerManager::start_with(node.ctx.clone(), options);
    assert!(
        wait_until(Duration::from_secs(10), || all_lanes(
            &manager,
            SERVER,
            LaneState::Unauthorized
        )),
        "{:?}",
        lanes(&manager, SERVER)
    );
    for lane in lanes(&manager, SERVER) {
        assert_eq!(lane.error.unwrap().code, "INVALID_PAIRING_CODE");
    }
    assert!(wait_until(Duration::from_secs(5), || node
        .peer_state(SERVER)
        == Some(PeerConnState::Unauthorized)));
    let dials = || -> u64 { lanes(&manager, SERVER).iter().map(|lane| lane.dials).sum() };
    let started = Instant::now();
    let initial = dials();
    assert!((1..=3).contains(&initial), "{initial}");
    thread::sleep(Duration::from_millis(3500));
    let attempts = (dials() - initial) as usize;
    let rechecks = started.elapsed().as_secs() + 1;
    assert!(
        (1..=rechecks as usize).contains(&attempts),
        "one probe per recheck interval, not a loop: {attempts} in {:?}",
        started.elapsed()
    );
    assert_eq!(server.established(), 0);
    // The server saw exactly those handshakes fail (its count lags a little).
    assert!(wait_until(Duration::from_secs(2), || server
        .state
        .handshake_failures
        .load(Ordering::SeqCst)
        as u64
        == dials()));

    // Re-pairing with the right code restarts the lanes at once.
    let repaired = Instant::now();
    let paired = manager.pair(&server.endpoint, &code, Some(3)).unwrap();
    assert_eq!(paired.installation, SERVER);
    assert!(wait_until(Duration::from_secs(5), || all_lanes(
        &manager,
        SERVER,
        LaneState::Ready
    )));
    assert!(repaired.elapsed() < Duration::from_secs(3));
    let reported = node.everything_reported();
    assert!(!reported.contains(&code) && !reported.contains(&wrong));
    assert!(!format!("{manager:?}").contains(&code));
    manager.shutdown(Duration::from_secs(5));
}

#[test]
fn rotation_goaway_closes_sessions_and_the_peer_becomes_unauthorized() {
    let node = local();
    let (server, code) = FakePeer::start(SERVER);
    node.add_peer(&server.endpoint, &code, SERVER, 2);
    let manager = PeerManager::start_with(node.ctx.clone(), fast());
    assert!(wait_until(Duration::from_secs(10), || all_lanes(
        &manager,
        SERVER,
        LaneState::Ready
    )));
    // The fake server lists a session only after `accept` returns, which
    // can lag the client's Ready: wait until both are listed, or the GOAWAY
    // below would miss one.
    assert!(wait_until(Duration::from_secs(5), || server.open_count() == 2));
    let (_, rotated) = Verifier::generate();
    *server.policy.verifier.lock().unwrap() = Some((rotated, 2));
    server.goaway_all("PAIRING_CODE_CHANGED");
    assert!(
        wait_until(Duration::from_secs(10), || all_lanes(
            &manager,
            SERVER,
            LaneState::Unauthorized
        )),
        "{:#?}\n{:#?}",
        lanes(&manager, SERVER),
        node.recorder.of_peer_from(SERVER, 0)
    );
    for lane in 0..2 {
        let events = node.recorder.lane(SERVER, lane);
        assert!(
            events
                .iter()
                .any(|(_, _, state, error, _)| *state == LaneState::Backoff
                    && error
                        .as_ref()
                        .is_some_and(|e| e.code == "PAIRING_CODE_CHANGED")),
            "lane {lane}: the session closed with the server's reason"
        );
    }
    assert_eq!(
        lanes(&manager, SERVER)[0].error.as_ref().unwrap().code,
        "INVALID_PAIRING_CODE"
    );
    manager.shutdown(Duration::from_secs(5));
}

/// The real catalog, except that `RecordPeerSeen` blocks until released: a
/// wedged catalog actor, as far as `peer_meta` writes are concerned.
struct StuckMeta {
    inner: Arc<Catalog>,
    released: Mutex<bool>,
    release: Condvar,
    attempts: AtomicUsize,
}

impl StuckMeta {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.release.notify_all();
    }
}

impl CatalogApi for StuckMeta {
    fn snapshot(&self) -> Arc<CatalogSnapshot> {
        self.inner.snapshot()
    }
    fn apply(&self, cmd: CatalogCmd) -> Result<Arc<CatalogSnapshot>, ErrorWire> {
        if matches!(cmd, CatalogCmd::RecordPeerSeen { .. }) {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.release.wait(released).unwrap();
            }
        }
        self.inner.apply(cmd)
    }
    fn wait_newer(&self, generation: u64, timeout: Duration) -> Arc<CatalogSnapshot> {
        self.inner.wait_newer(generation, timeout)
    }
}

#[test]
fn a_wedged_catalog_write_never_holds_a_lane() {
    let stuck: Arc<Mutex<Option<Arc<StuckMeta>>>> = Arc::new(Mutex::new(None));
    let slot = stuck.clone();
    let node = local_with(move |catalog| {
        let wrapped = Arc::new(StuckMeta {
            inner: catalog,
            released: Mutex::new(false),
            release: Condvar::new(),
            attempts: AtomicUsize::new(0),
        });
        *slot.lock().unwrap() = Some(wrapped.clone());
        wrapped
    });
    let stuck = stuck.lock().unwrap().clone().unwrap();
    let (server, code) = FakePeer::start(SERVER);
    server.set_catalog(3, vec![entry("repo-a", 1)]);
    node.add_peer(&server.endpoint, &code, SERVER, 2);
    let manager = PeerManager::start_with(node.ctx.clone(), fast());

    // The `peer_meta` write of lane 0 hangs; the lanes, the catalog client
    // and RTT reporting carry on (§2.3: a catalog fault never stops lanes).
    assert!(wait_until(Duration::from_secs(10), || all_lanes(
        &manager,
        SERVER,
        LaneState::Ready
    )));
    assert!(wait_until(Duration::from_secs(5), || stuck
        .attempts
        .load(Ordering::SeqCst)
        >= 1));
    assert!(wait_until(Duration::from_secs(5), || !manager
        .context(SERVER)
        .unwrap()
        .catalog
        .load()
        .entries
        .is_empty()));
    let polls = server.state.since.lock().unwrap().len();
    assert!(wait_until(Duration::from_secs(5), || server
        .state
        .since
        .lock()
        .unwrap()
        .len()
        > polls + 3));
    // A reconnect with the write still pending is not held either.
    server.close(LaneRole::Control);
    assert!(wait_until(Duration::from_secs(10), || {
        let lanes = lanes(&manager, SERVER);
        lanes[0].connects == 2 && lanes[0].state == LaneState::Ready
    }));
    assert!(!node.catalog.snapshot().peer_meta.contains_key(SERVER));

    // Shutdown does not wait for the wedged write either.
    let started = Instant::now();
    manager.shutdown(Duration::from_secs(5));
    assert!(started.elapsed() < Duration::from_secs(5));
    // Once the catalog recovers, the queued writes land.
    stuck.release();
    assert!(wait_until(Duration::from_secs(5), || node
        .catalog
        .snapshot()
        .peer_meta
        .get(SERVER)
        .is_some_and(|meta| meta.last_seen_ms.is_some())));
}

// ---------------------------------------------------------------------------
// NetworkApi

#[test]
fn pair_probes_stores_a_new_revision_and_supervises() {
    let node = local();
    let (server, code) = FakePeer::start(SERVER);
    server.set_catalog(1, vec![entry("repo-a", 1)]);
    let manager = PeerManager::start_with(node.ctx.clone(), fast());
    let paired = manager.pair(&server.endpoint, &code, Some(3)).unwrap();
    assert_eq!(paired.endpoint, server.endpoint);
    assert_eq!(paired.installation, SERVER);
    assert_eq!(
        (
            paired.connections,
            paired.configured_connections,
            paired.reserved_connections
        ),
        (3, 3, 1)
    );
    let probes: Vec<ClientHello> = server
        .policy
        .hellos
        .lock()
        .unwrap()
        .iter()
        .filter(|hello| hello.role == LaneRole::Probe)
        .cloned()
        .collect();
    assert_eq!(probes.len(), 1);
    assert_eq!(probes[0].lane, None);
    let record = node
        .catalog
        .snapshot()
        .peer_by_endpoint(&server.endpoint)
        .cloned()
        .unwrap();
    assert_eq!(
        (record.installation.as_str(), record.connections),
        (SERVER, 3)
    );
    assert!(!record.revision.is_empty());
    assert!(wait_until(Duration::from_secs(10), || all_lanes(
        &manager,
        SERVER,
        LaneState::Ready
    )));
    assert_eq!(lanes(&manager, SERVER).len(), 3);
    assert!(wait_until(Duration::from_secs(5), || !manager
        .context(SERVER)
        .unwrap()
        .catalog
        .load()
        .entries
        .is_empty()));

    // Pairing again: a new revision replaces the supervisor; the remote
    // catalog carries over.
    server.set_catalog(1, Vec::new());
    server.state.hang_catalog.store(true, Ordering::SeqCst);
    let again = manager.pair(&server.endpoint, &code, Some(2)).unwrap();
    assert_eq!(again.connections, 2);
    let renewed = node
        .catalog
        .snapshot()
        .peer_by_endpoint(&server.endpoint)
        .cloned()
        .unwrap();
    assert_ne!(renewed.revision, record.revision);
    assert_eq!(manager.peer(SERVER).unwrap().revision, renewed.revision);
    assert_eq!(
        manager.context(SERVER).unwrap().catalog.load().entries,
        vec![entry("repo-a", 1)],
        "the last good catalog survives the respawn"
    );
    assert!(wait_until(Duration::from_secs(10), || {
        all_lanes(&manager, SERVER, LaneState::Ready) && server.open_count() == 2
    }));
    assert_eq!(
        node.recorder
            .count(|event| matches!(event, StatusEvent::PeerRemoved { .. })),
        0,
        "a respawn is not a removal"
    );

    // Another endpoint of the same installation replaces the first.
    server.state.hang_catalog.store(false, Ordering::SeqCst);
    let (second, second_code) = FakePeer::start(SERVER);
    manager.pair(&second.endpoint, &second_code, None).unwrap();
    let peers = node.catalog.snapshot().peers.clone();
    assert_eq!(peers.len(), 1, "{peers:?}");
    assert_eq!(peers[0].endpoint, second.endpoint);
    assert_eq!(peers[0].connections, 2, "the configured default");
    assert!(wait_until(Duration::from_secs(10), || {
        all_lanes(&manager, SERVER, LaneState::Ready) && second.open_count() == 2
    }));
    assert!(wait_until(Duration::from_secs(5), || server.open_count() == 0));
    manager.shutdown(Duration::from_secs(5));
}

#[test]
fn pair_refusals_store_nothing() {
    let node = local();
    let manager = PeerManager::start_with(node.ctx.clone(), fast());

    // To ourselves: the remote is this installation.
    let (myself, my_code) = FakePeer::start(&node.installation);
    let error = manager.pair(&myself.endpoint, &my_code, None).unwrap_err();
    assert_eq!(error.code, "CANNOT_PAIR_WITH_SELF");
    assert_eq!(myself.established(), 0);

    // A wrong code.
    let (server, code) = FakePeer::start(SERVER);
    let error = manager
        .pair(&server.endpoint, &format!("{code}x"), None)
        .unwrap_err();
    assert_eq!(error.code, "INVALID_PAIRING_CODE");
    assert_eq!(error.class, ErrorClass::NeedsUserAction);

    // Connection counts outside 1..=16.
    for count in [0, 17] {
        let error = manager
            .pair(&server.endpoint, &code, Some(count))
            .unwrap_err();
        assert_eq!(error.code, "INVALID_CONNECTION_COUNT");
    }

    // Nothing listens properly.
    let dropping = drop_listener();
    let error = manager.pair(&dropping.endpoint, &code, None).unwrap_err();
    assert_eq!(error.code, "WS_CONNECT_FAILED");

    // A legacy peer: incompatible, and it never receives the code.
    let legacy = legacy_server();
    let error = manager.pair(&legacy.endpoint, &code, None).unwrap_err();
    assert_eq!(error.code, "PEER_NEEDS_UPGRADE");
    assert_eq!(error.class, ErrorClass::Incompatible);
    thread::sleep(Duration::from_millis(100));
    assert!(legacy.received.lock().unwrap().is_empty());

    assert!(node.catalog.snapshot().peers.is_empty());
    assert!(manager.peers().is_empty());
    assert!(!node.everything_reported().contains(&code));
    manager.shutdown(Duration::from_secs(5));
}

#[test]
fn remote_list_lists_the_catalog_and_checks_the_installation() {
    let node = local();
    let (server, code) = FakePeer::start(SERVER);
    server.set_catalog(7, vec![entry("repo-a", 3), entry("repo-b", 4)]);
    let manager = PeerManager::start_with(node.ctx.clone(), fast());

    let error = manager.remote_list(&server.endpoint).unwrap_err();
    assert_eq!(error.code, "PEER_NOT_PAIRED");

    node.add_peer(&server.endpoint, &code, SERVER, 2);
    let entries = manager.remote_list(&server.endpoint).unwrap();
    assert_eq!(entries, vec![entry("repo-a", 3), entry("repo-b", 4)]);
    server.state.reply_in_header.store(true, Ordering::SeqCst);
    assert_eq!(manager.remote_list(&server.endpoint).unwrap().len(), 2);
    let probes = server
        .policy
        .hellos
        .lock()
        .unwrap()
        .iter()
        .filter(|hello| hello.role == LaneRole::Probe)
        .count();
    assert_eq!(probes, 2);
    assert!(wait_until(Duration::from_secs(10), || all_lanes(
        &manager,
        SERVER,
        LaneState::Ready
    )));

    // The endpoint now serves another installation.
    *server.policy.installation.lock().unwrap() = "someone-else".into();
    let error = manager.remote_list(&server.endpoint).unwrap_err();
    assert_eq!(error.code, "PEER_INSTALLATION_CHANGED");
    assert_eq!(error.class, ErrorClass::NeedsUserAction);
    // Live sessions keep their authenticated peer; reconnecting finds the
    // change and the peer is refused until re-paired. (Both lane sessions
    // must be listed by the fake server before they can be closed.)
    assert!(wait_until(Duration::from_secs(5), || server.open_count() == 2));
    server.close(LaneRole::Control);
    server.close(LaneRole::Bulk);
    assert!(wait_until(Duration::from_secs(10), || all_lanes(
        &manager,
        SERVER,
        LaneState::Unauthorized
    )));
    assert_eq!(
        lanes(&manager, SERVER)[0].error.as_ref().unwrap().code,
        "PEER_INSTALLATION_CHANGED"
    );
    manager.shutdown(Duration::from_secs(5));
}

#[test]
fn unpair_cancels_and_joins_every_lane_within_5s() {
    let node = local();
    let (server, code) = FakePeer::start(SERVER);
    node.add_peer(&server.endpoint, &code, SERVER, 4);
    let manager = PeerManager::start_with(node.ctx.clone(), fast());
    assert!(wait_until(Duration::from_secs(10), || all_lanes(
        &manager,
        SERVER,
        LaneState::Ready
    )));
    assert!(wait_until(Duration::from_secs(5), || server.open_count() == 4));

    let started = Instant::now();
    manager.unpair(&server.endpoint).unwrap();
    let took = started.elapsed();
    assert!(took < Duration::from_secs(5), "unpair took {took:?}");
    assert!(manager.peer(SERVER).is_none());
    assert!(node.catalog.snapshot().peers.is_empty());
    // Every lane ended and was joined (none detached), each reported Closed
    // before PeerRemoved, and the peer closed every session after our
    // GOAWAY.
    assert_eq!(manager.detached_lanes(), 0);
    let removed = node.recorder.removed_at(SERVER);
    assert_eq!(
        node.recorder.last_lane_states(SERVER, 4, removed),
        vec![LaneState::Closed; 4]
    );
    for lane in 0..4 {
        assert_eq!(
            node.recorder.lane(SERVER, lane).last().unwrap().2,
            LaneState::Closed
        );
    }
    assert!(wait_until(Duration::from_secs(2), || server.open_count() == 0));
    assert!(
        server
            .close_reasons()
            .iter()
            .all(|code| code == "PEER_NOT_PAIRED"),
        "{:?}",
        server.close_reasons()
    );
    assert_eq!(
        node.recorder.count(|event| matches!(
            event,
            StatusEvent::PeerRemoved { installation } if installation == SERVER
        )),
        1
    );
    assert!(wait_until(Duration::from_secs(5), || node
        .peer_state(SERVER)
        .is_none()));
    // Nothing dials again.
    let tcp = server.state.tcp.load(Ordering::SeqCst);
    thread::sleep(Duration::from_millis(500));
    assert_eq!(server.state.tcp.load(Ordering::SeqCst), tcp);
    // Unpairing again is harmless.
    manager.unpair(&server.endpoint).unwrap();
    manager.shutdown(Duration::from_secs(5));
}

#[test]
fn unpair_joins_within_5s_while_the_peer_holds_a_request() {
    let node = local();
    let (server, code) = FakePeer::start(SERVER);
    node.add_peer(&server.endpoint, &code, SERVER, 2);
    let manager = PeerManager::start_with(node.ctx.clone(), fast());
    assert!(wait_until(Duration::from_secs(10), || all_lanes(
        &manager,
        SERVER,
        LaneState::Ready
    )));
    // The peer stops answering `catalog.list`: lane 0 sits in a call and the
    // peer is never idle, so it does not close after our GOAWAY.
    server.state.hang_catalog.store(true, Ordering::SeqCst);
    let calls = server.state.since.lock().unwrap().len();
    assert!(wait_until(Duration::from_secs(5), || server
        .state
        .since
        .lock()
        .unwrap()
        .len()
        > calls));
    thread::sleep(Duration::from_millis(200));

    let started = Instant::now();
    manager.unpair(&server.endpoint).unwrap();
    let took = started.elapsed();
    // The peer never closes lane 0 after our GOAWAY: the session is closed
    // outright one second before the 5 s deadline, which ends lane 0's call
    // and the lane, and unpair returns well within 5 s.
    assert!(
        took >= Duration::from_millis(3500) && took < Duration::from_millis(4750),
        "unpair took {took:?} (sessions are force-closed at 4 s)"
    );
    assert!(manager.peer(SERVER).is_none());
    // Both lanes were joined, not detached at the deadline, and reported
    // Closed before PeerRemoved.
    assert_eq!(manager.detached_lanes(), 0, "every lane was joined");
    let removed = node.recorder.removed_at(SERVER);
    assert_eq!(
        node.recorder.last_lane_states(SERVER, 2, removed),
        vec![LaneState::Closed; 2]
    );
    assert!(node.recorder.of_peer_from(SERVER, removed + 1).is_empty());
    assert!(wait_until(Duration::from_secs(1), || server.open_count() == 0));
    manager.shutdown(Duration::from_secs(5));
}

/// CCR-WP2.6-2 (applied by WP3.1; this test pinned its adapter before):
/// lanes inside a dial are interrupted by the unpair instead of being
/// detached at the join deadline. Nothing they dialed ever becomes a session,
/// and nothing of them reaches the status sink after `PeerRemoved`.
#[test]
fn a_lane_inside_a_dial_is_interrupted_and_reports_nothing_after_peer_removed() {
    let node = local();
    let (server, code) = FakePeer::start(SERVER);
    // Every dial through the proxy takes about 4.5 s: longer than the join.
    let proxy = slow_proxy(server.addr, Duration::from_millis(1500));
    node.add_peer(&proxy, &code, SERVER, 2);
    let options = PeerOptions {
        join: Duration::from_secs(2),
        force_close_margin: Duration::from_millis(500),
        ..fast()
    };
    let manager = PeerManager::start_with(node.ctx.clone(), options);
    assert!(wait_until(Duration::from_secs(5), || all_lanes(
        &manager,
        SERVER,
        LaneState::Dialing
    )));

    // Both lanes are inside their dial: the unpair interrupts them (their
    // sockets are shut down), joins them before the 2 s deadline, and they
    // are reported Closed before PeerRemoved. An interrupted dial still waits
    // out the slow proxy's current 1.5 s chunk delay (on Windows the proxy
    // keeps the client side open after its FIN until that chunk is written),
    // so the bound has a margin over it (CCR-WP4.1-10, applied by WP4.INT:
    // it was the delay itself and failed at 1.501-1.578 s on loaded runs);
    // an uninterrupted dial takes about 4.5 s and the join would detach it.
    let started = Instant::now();
    manager.unpair(&proxy).unwrap();
    let took = started.elapsed();
    assert!(
        took < Duration::from_millis(2500),
        "unpair took {took:?} (the dials are interrupted at once)"
    );
    assert_eq!(manager.detached_lanes(), 0, "every lane was joined");
    let removed = node.recorder.removed_at(SERVER);
    assert_eq!(
        node.recorder.last_lane_states(SERVER, 2, removed),
        vec![LaneState::Closed; 2]
    );
    assert!(wait_until(Duration::from_secs(2), || node
        .peer_state(SERVER)
        .is_none()));

    // Longer than an uninterrupted dial takes: none of them completed, and
    // nothing reached the status sink, so the hub does not bring the
    // unpaired peer back.
    thread::sleep(Duration::from_secs(6));
    assert_eq!(server.established(), 0, "an interrupted dial completed");
    assert_eq!(server.open_count(), 0);
    let late = node.recorder.of_peer_from(SERVER, removed + 1);
    assert!(late.is_empty(), "status after PeerRemoved: {late:?}");
    assert_eq!(node.peer_state(SERVER), None, "no ghost peer in the hub");
    assert!(node.hub.snapshot_now().peers.is_empty());
    manager.shutdown(Duration::from_secs(5));
}

#[test]
fn a_respawn_is_not_overwritten_by_the_old_supervisors_late_lanes() {
    let node = local();
    let (server, code) = FakePeer::start(SERVER);
    let proxy = slow_proxy(server.addr, Duration::from_millis(1500));
    node.add_peer(&proxy, &code, SERVER, 2);
    let options = PeerOptions {
        join: Duration::from_secs(2),
        force_close_margin: Duration::from_millis(500),
        ..fast()
    };
    let manager = PeerManager::start_with(node.ctx.clone(), options);
    assert!(wait_until(Duration::from_secs(5), || all_lanes(
        &manager,
        SERVER,
        LaneState::Dialing
    )));

    // Re-pairing the installation at its direct endpoint replaces the
    // proxied supervisor (respawn) while its lanes are inside their dial:
    // they are interrupted and joined (CCR-WP2.6-2, applied by WP3.1; they
    // were detached at the deadline before), and the successor runs the
    // same (installation, lane) pairs.
    manager.pair(&server.endpoint, &code, Some(2)).unwrap();
    assert_eq!(manager.detached_lanes(), 0);
    assert_eq!(manager.peer(SERVER).unwrap().endpoint, server.endpoint);
    let respawned = node
        .recorder
        .events()
        .iter()
        .rposition(|(_, _, event)| {
            matches!(event, StatusEvent::PeerConfigured { installation, .. } if installation == SERVER)
        })
        .unwrap();
    assert!(wait_until(Duration::from_secs(5), || all_lanes(
        &manager,
        SERVER,
        LaneState::Ready
    )));

    // The old dials never complete (they were interrupted): the server
    // established the probe of the pairing and the successor's two lanes
    // only, also after the time an uninterrupted dial takes. Nothing late of
    // the old supervisor reaches the status sink: since the respawn every
    // lane event is the successor's, and the hub still shows both lanes
    // Ready.
    assert!(wait_until(Duration::from_secs(10), || server.established()
        == 3
        && server.open_count() == 2));
    thread::sleep(Duration::from_secs(6));
    assert_eq!(server.established(), 3, "an interrupted dial completed");
    let since: Vec<LaneState> = node
        .recorder
        .of_peer_from(SERVER, respawned + 1)
        .into_iter()
        .filter_map(|event| match event {
            StatusEvent::LaneState { state, .. } => Some(state),
            _ => None,
        })
        .collect();
    assert!(
        !since.contains(&LaneState::Closed) && !since.contains(&LaneState::Backoff),
        "{since:?}"
    );
    assert_eq!(hub_lanes(&node, SERVER), vec![LaneState::Ready; 2]);
    assert_eq!(node.peer_state(SERVER), Some(PeerConnState::Online));
    assert!(all_lanes(&manager, SERVER, LaneState::Ready));
    manager.shutdown(Duration::from_secs(5));
}

#[test]
fn a_slow_supervisor_does_not_take_another_ones_stop_time() {
    let node = local();
    // "peer-a-slow" sorts first: its lanes are inside a dial (6 s through
    // the proxy) for the whole shutdown.
    let (slow, slow_code) = FakePeer::start("peer-a-slow");
    let (holding, holding_code) = FakePeer::start("peer-b-holding");
    let proxy = slow_proxy(slow.addr, Duration::from_secs(2));
    node.add_peer(&proxy, &slow_code, "peer-a-slow", 2);
    node.add_peer(&holding.endpoint, &holding_code, "peer-b-holding", 2);
    let manager = PeerManager::start_with(node.ctx.clone(), fast());
    assert!(wait_until(Duration::from_secs(10), || all_lanes(
        &manager,
        "peer-b-holding",
        LaneState::Ready
    )));
    // The second peer stops answering `catalog.list`: its lane 0 sits in a
    // call, so it is never idle and needs the forced close.
    holding.state.hang_catalog.store(true, Ordering::SeqCst);
    let calls = holding.state.since.lock().unwrap().len();
    assert!(wait_until(Duration::from_secs(5), || holding
        .state
        .since
        .lock()
        .unwrap()
        .len()
        > calls));
    assert!(all_lanes(&manager, "peer-a-slow", LaneState::Dialing));

    // One deadline, in phases: the holding peer is force-closed one second
    // before it and joined; the slow peer's lanes, inside their dial, are
    // interrupted and joined too (CCR-WP2.6-2, applied by WP3.1: they were
    // the ones detached at the deadline before).
    let started = Instant::now();
    manager.shutdown(Duration::from_secs(3));
    let took = started.elapsed();
    assert!(took < Duration::from_millis(3500), "shutdown took {took:?}");
    assert_eq!(manager.detached_lanes(), 0, "every lane was joined");
    assert!(wait_until(Duration::from_secs(1), || holding.open_count() == 0));
    let all = node.recorder.events().len();
    assert_eq!(
        node.recorder.last_lane_states("peer-b-holding", 2, all),
        vec![LaneState::Closed; 2]
    );
}

#[test]
fn shutdown_also_stops_a_peer_an_unpair_is_stopping() {
    let node = local();
    let (server, code) = FakePeer::start(SERVER);
    node.add_peer(&server.endpoint, &code, SERVER, 2);
    let manager = PeerManager::start_with(node.ctx.clone(), fast());
    assert!(wait_until(Duration::from_secs(10), || all_lanes(
        &manager,
        SERVER,
        LaneState::Ready
    )));
    server.state.hang_catalog.store(true, Ordering::SeqCst);
    let calls = server.state.since.lock().unwrap().len();
    assert!(wait_until(Duration::from_secs(5), || server
        .state
        .since
        .lock()
        .unwrap()
        .len()
        > calls));

    // An unpair takes the supervisor out and waits (up to 5 s) for lane 0,
    // which sits in a call the peer never answers.
    let unpairing = {
        let manager = manager.clone();
        let endpoint = server.endpoint.clone();
        thread::spawn(move || manager.unpair(&endpoint))
    };
    assert!(wait_until(Duration::from_secs(2), || manager
        .peer(SERVER)
        .is_none()));

    // A shutdown meanwhile stops that supervisor under its own deadline:
    // the session is closed one second before it and the lanes are joined,
    // long before the unpair's own deadline.
    let started = Instant::now();
    manager.shutdown(Duration::from_secs(2));
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(800) && took < Duration::from_millis(2500),
        "shutdown took {took:?}"
    );
    assert!(wait_until(Duration::from_millis(500), || server
        .open_count()
        == 0));
    assert_eq!(manager.detached_lanes(), 0);
    unpairing.join().unwrap().unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "the unpair ended too"
    );
    let removed = node.recorder.removed_at(SERVER);
    assert_eq!(
        node.recorder.last_lane_states(SERVER, 2, removed),
        vec![LaneState::Closed; 2]
    );
    assert!(node.catalog.snapshot().peers.is_empty());
}

#[test]
fn shutdown_accepts_an_unbounded_deadline() {
    let node = local();
    let (server, code) = FakePeer::start(SERVER);
    node.add_peer(&server.endpoint, &code, SERVER, 2);
    let manager = PeerManager::start_with(node.ctx.clone(), fast());
    assert!(wait_until(Duration::from_secs(10), || all_lanes(
        &manager,
        SERVER,
        LaneState::Ready
    )));
    let started = Instant::now();
    manager.shutdown(Duration::MAX);
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(manager.peers().is_empty());
    assert!(wait_until(Duration::from_secs(2), || server.open_count() == 0));
}

/// The real catalog, except that every `RemovePeer` waits 300 ms first.
struct SlowRemove {
    inner: Arc<Catalog>,
}

impl CatalogApi for SlowRemove {
    fn snapshot(&self) -> Arc<CatalogSnapshot> {
        self.inner.snapshot()
    }
    fn apply(&self, cmd: CatalogCmd) -> Result<Arc<CatalogSnapshot>, ErrorWire> {
        if matches!(cmd, CatalogCmd::RemovePeer { .. }) {
            thread::sleep(Duration::from_millis(300));
        }
        self.inner.apply(cmd)
    }
    fn wait_newer(&self, generation: u64, timeout: Duration) -> Arc<CatalogSnapshot> {
        self.inner.wait_newer(generation, timeout)
    }
}

#[test]
fn concurrent_pairs_of_one_installation_keep_one_record() {
    let node = local_with(|catalog| Arc::new(SlowRemove { inner: catalog }));
    let (first, first_code) = FakePeer::start(SERVER);
    let (second, second_code) = FakePeer::start(SERVER);
    // A legacy catalog with the installation at both endpoints.
    node.add_peer(&first.endpoint, &first_code, SERVER, 2);
    node.add_peer(&second.endpoint, &second_code, SERVER, 2);
    let manager = PeerManager::start_with(node.ctx.clone(), fast());

    // Pairing it at both endpoints at once: each pair stores its record and
    // removes the installation's other endpoint. Unserialized, both stores
    // land before both (slow) removals and each removes the other's fresh
    // record; serialized, the last pair wins.
    let barrier = Barrier::new(2);
    let (a, b) = thread::scope(|scope| {
        let a = scope.spawn(|| {
            barrier.wait();
            manager.pair(&first.endpoint, &first_code, Some(2))
        });
        let b = scope.spawn(|| {
            barrier.wait();
            manager.pair(&second.endpoint, &second_code, Some(2))
        });
        (a.join().unwrap(), b.join().unwrap())
    });
    a.unwrap();
    b.unwrap();
    let peers = node.catalog.snapshot().peers.clone();
    assert_eq!(peers.len(), 1, "{peers:?}");
    let kept = peers[0].endpoint.clone();
    assert!(kept == first.endpoint || kept == second.endpoint);
    assert!(wait_until(Duration::from_secs(10), || {
        manager
            .peer(SERVER)
            .is_some_and(|peer| peer.endpoint == kept)
            && all_lanes(&manager, SERVER, LaneState::Ready)
    }));
    manager.shutdown(Duration::from_secs(5));
}

#[test]
fn a_lane_that_dies_holding_the_probe_hands_it_on() {
    let node = local();
    let legacy = legacy_server();
    node.add_peer(&legacy.endpoint, "some-code", SERVER, 2);
    let options = PeerOptions {
        incompatible_recheck: Duration::from_secs(1),
        ..fast()
    };
    let manager = PeerManager::start_with(node.ctx.clone(), options);
    assert!(wait_until(Duration::from_secs(10), || all_lanes(
        &manager,
        SERVER,
        LaneState::Incompatible
    )));
    let dials = legacy.accepts.load(Ordering::SeqCst);

    // At the recheck, the lane that takes the probe dies (its `Dialing`
    // report panics) before it dials. The probe passes to the other lane,
    // which dials at once instead of waiting for a respawn.
    node.recorder.panic_on_dialing.store(true, Ordering::SeqCst);
    assert!(wait_until(Duration::from_secs(4), || {
        !node.recorder.panic_on_dialing.load(Ordering::SeqCst)
            && legacy.accepts.load(Ordering::SeqCst) > dials
    }));
    let states: Vec<LaneState> = lanes(&manager, SERVER)
        .iter()
        .map(|lane| lane.state)
        .collect();
    assert_eq!(
        states.iter().filter(|s| **s == LaneState::Closed).count(),
        1,
        "{states:?}"
    );
    // The surviving lane keeps probing at every recheck.
    let after = legacy.accepts.load(Ordering::SeqCst);
    assert!(wait_until(Duration::from_secs(4), || legacy
        .accepts
        .load(Ordering::SeqCst)
        > after));
    manager.shutdown(Duration::from_secs(5));
}

#[test]
fn catalog_changes_respawn_or_stop_supervisors() {
    let node = local();
    let (server, code) = FakePeer::start(SERVER);
    let (other, other_code) = FakePeer::start("peer-other-installation");
    node.add_peer(&server.endpoint, &code, SERVER, 2);
    let manager = PeerManager::start_with(node.ctx.clone(), fast());
    assert!(wait_until(Duration::from_secs(10), || all_lanes(
        &manager,
        SERVER,
        LaneState::Ready
    )));

    // A new peer appears in the catalog: supervised without any call.
    node.add_peer(&other.endpoint, &other_code, "peer-other-installation", 2);
    assert!(wait_until(Duration::from_secs(10), || all_lanes(
        &manager,
        "peer-other-installation",
        LaneState::Ready
    )));
    // More connections: the supervisor is respawned with a bigger pool.
    let before = manager.peer(SERVER).unwrap().revision;
    node.add_peer(&server.endpoint, &code, SERVER, 3);
    assert!(wait_until(Duration::from_secs(10), || {
        manager
            .peer(SERVER)
            .is_some_and(|peer| peer.revision != before && peer.pool == 3)
            && all_lanes(&manager, SERVER, LaneState::Ready)
    }));
    assert!(wait_until(Duration::from_secs(5), || server.open_count() == 3));
    // Removed from the catalog: stopped and reported removed; the other peer
    // is untouched.
    let other_connects: Vec<u64> = lanes(&manager, "peer-other-installation")
        .iter()
        .map(|lane| lane.connects)
        .collect();
    node.catalog
        .apply(CatalogCmd::RemovePeer {
            endpoint: server.endpoint.clone(),
        })
        .unwrap();
    assert!(wait_until(Duration::from_secs(10), || manager
        .peer(SERVER)
        .is_none()));
    assert!(wait_until(Duration::from_secs(5), || server.open_count() == 0));
    // WP2.INT: the catalog watcher records `PeerRemoved` once the lanes are
    // joined, and since WP2.7 a lane joins its real `run_control` runner
    // (its watcher slice, up to ~0.5 s) after the sessions closed, so wait
    // for the event before counting it.
    let removed = || {
        node.recorder.count(|event| {
            matches!(
                event,
                StatusEvent::PeerRemoved { installation } if installation == SERVER
            )
        })
    };
    assert!(wait_until(Duration::from_secs(5), || removed() >= 1));
    assert_eq!(removed(), 1);
    assert_eq!(
        lanes(&manager, "peer-other-installation")
            .iter()
            .map(|lane| lane.connects)
            .collect::<Vec<_>>(),
        other_connects
    );
    // `reconfigure` with nothing changed is a no-op.
    manager.reconfigure();
    assert_eq!(manager.peers().len(), 1);
    manager.shutdown(Duration::from_secs(5));
}

#[test]
fn shutdown_sends_goaway_and_joins_every_peer() {
    let node = local();
    let (first, first_code) = FakePeer::start(SERVER);
    let (second, second_code) = FakePeer::start("peer-second-installation");
    node.add_peer(&first.endpoint, &first_code, SERVER, 3);
    node.add_peer(
        &second.endpoint,
        &second_code,
        "peer-second-installation",
        2,
    );
    let manager = PeerManager::start_with(node.ctx.clone(), fast());
    assert!(wait_until(Duration::from_secs(10), || {
        all_lanes(&manager, SERVER, LaneState::Ready)
            && all_lanes(&manager, "peer-second-installation", LaneState::Ready)
    }));
    let started = Instant::now();
    manager.shutdown(Duration::from_secs(5));
    let took = started.elapsed();
    assert!(took < Duration::from_secs(5), "shutdown took {took:?}");
    assert!(manager.peers().is_empty());
    assert_eq!(manager.detached_lanes(), 0, "every lane was joined");
    for server in [&first, &second] {
        assert!(wait_until(Duration::from_secs(2), || server.open_count() == 0));
        let reasons = server.close_reasons();
        assert!(
            reasons.iter().all(|code| code == "ORCHESTRATOR_STOPPING"),
            "{reasons:?}"
        );
    }
    // Peers stay configured (no removal), nothing dials again, and the
    // manager refuses new work.
    assert_eq!(
        node.recorder
            .count(|event| matches!(event, StatusEvent::PeerRemoved { .. })),
        0
    );
    let tcp = first.state.tcp.load(Ordering::SeqCst);
    thread::sleep(Duration::from_millis(500));
    assert_eq!(first.state.tcp.load(Ordering::SeqCst), tcp);
    let error = manager
        .pair(&first.endpoint, &first_code, None)
        .unwrap_err();
    assert_eq!(error.code, "ORCHESTRATOR_STOPPING");
    assert_eq!(
        manager.remote_list(&first.endpoint).unwrap_err().code,
        "ORCHESTRATOR_STOPPING"
    );
    assert_eq!(node.catalog.snapshot().peers.len(), 2);
}

#[test]
fn shutdown_of_the_daemon_token_stops_every_lane() {
    let node = local();
    let (server, code) = FakePeer::start(SERVER);
    node.add_peer(&server.endpoint, &code, SERVER, 2);
    let manager = PeerManager::start_with(node.ctx.clone(), fast());
    assert!(wait_until(Duration::from_secs(10), || all_lanes(
        &manager,
        SERVER,
        LaneState::Ready
    )));
    // `ctx.shutdown` is the manager's parent token (invariant 29).
    node.ctx.shutdown.cancel();
    assert!(wait_until(Duration::from_secs(5), || all_lanes(
        &manager,
        SERVER,
        LaneState::Closed
    )));
    assert!(wait_until(Duration::from_secs(5), || server.open_count() == 0));
    manager.shutdown(Duration::from_secs(5));
}

#[test]
fn the_default_policy_matches_section_3_7() {
    let options = PeerOptions::default();
    assert_eq!(options.lane_backoff_base, Duration::from_secs(1));
    assert_eq!(options.lane_backoff_cap, Duration::from_secs(60));
    assert_eq!(options.lane_backoff_jitter, 0.2);
    // WP5.3 ("reset the backoff once the lane is healthy"): a lane is
    // healthy after 5 s Ready (§3.7 said 30 s; CCR-WP5.3-1), and a down
    // peer's lanes share one redial cadence (the outage's age / 30).
    assert_eq!(options.lane_reset_after, Duration::from_secs(5));
    assert_eq!(options.network_spacing_ratio, 30);
    assert_eq!(options.incompatible_recheck, Duration::from_secs(600));
    assert_eq!(options.unauthorized_recheck, Duration::from_secs(60));
    assert_eq!(options.catalog_poll, Duration::from_secs(2));
    assert_eq!(options.join, Duration::from_secs(5));
    assert_eq!(options.conn, ConnOptions::default());
}

// ---------------------------------------------------------------------------
// WP5.3: network loss (REVAMP_PLAN §14.5b, "Lane reconnect")

/// How a [`Link`] is down.
const LINK_UP: usize = 0;
/// A clean close: every connection is shut down, new ones at accept.
const LINK_REFUSED: usize = 1;
/// A silent link: connections stay open, every byte is dropped (new
/// connections' too), and an end of stream is not passed on.
const LINK_SILENT: usize = 2;

/// A relay in front of `target` whose link can go down (WP5.3).
struct Link {
    endpoint: String,
    state: Arc<LinkState>,
}

#[derive(Default)]
struct LinkState {
    mode: AtomicUsize,
    live: Mutex<Vec<TcpStream>>,
    /// When each connection was accepted.
    accepts: Mutex<Vec<Instant>>,
}

fn link(target: SocketAddr) -> Link {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let state = Arc::new(LinkState::default());
    let shared = state.clone();
    thread::spawn(move || {
        for client in listener.incoming() {
            let Ok(client) = client else { continue };
            shared.accepts.lock().unwrap().push(Instant::now());
            if shared.mode.load(Ordering::SeqCst) == LINK_REFUSED {
                let _ = client.shutdown(Shutdown::Both);
                continue;
            }
            let Ok(server) = TcpStream::connect(target) else {
                continue;
            };
            let (Ok(a), Ok(b), Ok(c), Ok(d)) = (
                client.try_clone(),
                server.try_clone(),
                client.try_clone(),
                server.try_clone(),
            ) else {
                continue;
            };
            shared.live.lock().unwrap().extend([a, b]);
            link_pump(client, server, shared.clone());
            link_pump(d, c, shared.clone());
        }
    });
    Link { endpoint, state }
}

fn link_pump(mut from: TcpStream, mut to: TcpStream, state: Arc<LinkState>) {
    thread::spawn(move || {
        let mut buf = [0u8; 16 * 1024];
        loop {
            match from.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if state.mode.load(Ordering::SeqCst) == LINK_SILENT {
                        continue;
                    }
                    if to.write_all(&buf[..n]).is_err() {
                        break;
                    }
                }
            }
        }
        if state.mode.load(Ordering::SeqCst) != LINK_SILENT {
            let _ = to.shutdown(Shutdown::Write);
        }
    });
}

impl Link {
    fn down(&self, mode: usize) {
        self.state.mode.store(mode, Ordering::SeqCst);
        if mode == LINK_REFUSED {
            for stream in self.state.live.lock().unwrap().drain(..) {
                let _ = stream.shutdown(Shutdown::Both);
            }
        }
    }

    fn up(&self) {
        self.state.mode.store(LINK_UP, Ordering::SeqCst);
    }

    fn accepts_between(&self, from: Instant, to: Instant) -> Vec<Instant> {
        self.state
            .accepts
            .lock()
            .unwrap()
            .iter()
            .copied()
            .filter(|at| *at >= from && *at <= to)
            .collect()
    }
}

/// When each lane of `installation` first became `Ready` at or after `since`.
fn ready_times(
    node: &Local,
    installation: &str,
    pool: u32,
    since: Instant,
) -> Vec<Option<Instant>> {
    (0..pool)
        .map(|lane| {
            node.recorder
                .lane(installation, lane)
                .into_iter()
                .filter(|(at, _, state, _, _)| *at >= since && *state == LaneState::Ready)
                .map(|(at, ..)| at)
                .next()
        })
        .collect()
}

/// The spacing of a down peer's redials follows the outage's age: 1 s for
/// the first 30 s, the age / 30 after, at most the 60 s cap.
#[test]
fn a_down_peers_redial_spacing_follows_the_outage_age() {
    let options = PeerOptions::default();
    let secs = |s: u64| Duration::from_secs(s);
    assert_eq!(options.network_spacing(Duration::ZERO), secs(1));
    assert_eq!(options.network_spacing(secs(30)), secs(1));
    assert_eq!(
        options.network_spacing(secs(45)),
        Duration::from_millis(1500)
    );
    assert_eq!(options.network_spacing(secs(120)), secs(4));
    assert_eq!(options.network_spacing(secs(1800)), secs(60));
    assert_eq!(options.network_spacing(secs(7200)), secs(60));
}

/// The lab's fault (a clean close through a tunnel): the peer's lanes were
/// healthy, the link closes, redials are refused. Through the outage the
/// peer is dialed on one cadence (one dial per spacing, not one per lane
/// per doubling backoff), and once the link is back every lane is `Ready`
/// within about a spacing.
#[test]
fn a_lost_peer_redials_on_one_cadence_and_is_back_within_a_spacing() {
    let node = local();
    let (server, code) = FakePeer::start(SERVER);
    let link = link(server.addr);
    node.add_peer(&link.endpoint, &code, SERVER, 4);
    let options = PeerOptions {
        lane_backoff_base: Duration::from_millis(200),
        lane_backoff_cap: Duration::from_secs(3),
        lane_reset_after: Duration::from_millis(300),
        ..fast()
    };
    let manager = PeerManager::start_with(node.ctx.clone(), options);
    assert!(wait_until(Duration::from_secs(10), || all_lanes(
        &manager,
        SERVER,
        LaneState::Ready
    )));
    // Healthy (`lane_reset_after`), then the link goes down.
    thread::sleep(Duration::from_millis(600));
    let down = Instant::now();
    link.down(LINK_REFUSED);
    thread::sleep(Duration::from_secs(4));
    let restored = Instant::now();
    link.up();
    assert!(
        wait_until(Duration::from_secs(5), || {
            ready_times(&node, SERVER, 4, restored)
                .iter()
                .all(Option::is_some)
        }),
        "{:?}",
        lanes(&manager, SERVER)
    );
    let back = ready_times(&node, SERVER, 4, restored)
        .into_iter()
        .map(|at| at.unwrap().duration_since(restored))
        .max()
        .unwrap();
    // The spacing 4 s into the outage is the base (200 ms, ±20 %).
    assert!(
        back <= Duration::from_millis(1500),
        "every lane Ready {back:?} after the restore"
    );
    // One cadence for the peer: the redials during the outage never bunch
    // (each lane's own doubling backoff would dial all four at once, at
    // 200, 600, 1400 ms, … after the loss) and come about every spacing.
    let dials = link.accepts_between(down, restored);
    assert!(dials.len() >= 8, "{} redials in 4 s", dials.len());
    for pair in dials.windows(2) {
        let gap = pair[1].duration_since(pair[0]);
        assert!(
            gap >= Duration::from_millis(100),
            "redials {gap:?} apart: {:?}",
            dials
                .iter()
                .map(|at| at.duration_since(down).as_millis())
                .collect::<Vec<_>>()
        );
    }
    manager.shutdown(Duration::from_secs(5));
}

/// When one lane connects, its siblings backing off after a failed dial
/// dial at once instead of waiting for their own timers (eight lanes whose
/// 3 s ±20 % timers would spread over more than a second).
#[test]
fn lanes_in_backoff_redial_at_once_when_a_sibling_connects() {
    let node = local();
    let (server, code) = FakePeer::start(SERVER);
    let link = link(server.addr);
    link.down(LINK_REFUSED);
    node.add_peer(&link.endpoint, &code, SERVER, 8);
    let options = PeerOptions {
        lane_backoff_base: Duration::from_secs(3),
        lane_backoff_cap: Duration::from_secs(3),
        ..fast()
    };
    let manager = PeerManager::start_with(node.ctx.clone(), options);
    // Every lane failed its first dial (never connected: its own backoff).
    assert!(wait_until(Duration::from_secs(5), || all_lanes(
        &manager,
        SERVER,
        LaneState::Backoff
    )));
    let restored = Instant::now();
    link.up();
    assert!(
        wait_until(Duration::from_secs(10), || {
            ready_times(&node, SERVER, 8, restored)
                .iter()
                .all(Option::is_some)
        }),
        "{:?}",
        lanes(&manager, SERVER)
    );
    let ready: Vec<Instant> = ready_times(&node, SERVER, 8, restored)
        .into_iter()
        .flatten()
        .collect();
    let first = *ready.iter().min().unwrap();
    let last = *ready.iter().max().unwrap();
    assert!(
        last.duration_since(first) <= Duration::from_millis(400),
        "the last lane was Ready {:?} after the first",
        last.duration_since(first)
    );
    manager.shutdown(Duration::from_secs(5));
}

/// A silent link: sessions die by their liveness deadline and redials hang
/// in their handshake. When the link is back, the first lane that connects
/// interrupts the siblings' stale dials (begun into the silent link), which
/// redial at once instead of waiting out their handshake timeouts (5 s).
#[test]
fn a_dial_into_a_silent_link_is_interrupted_when_a_sibling_connects() {
    let node = local();
    let (server, code) = FakePeer::start(SERVER);
    let link = link(server.addr);
    node.add_peer(&link.endpoint, &code, SERVER, 4);
    let mut options = PeerOptions {
        lane_backoff_base: Duration::from_secs(1),
        lane_backoff_cap: Duration::from_secs(3),
        lane_reset_after: Duration::from_millis(300),
        ..fast()
    };
    options.conn.dead_after = Duration::from_secs(1);
    let manager = PeerManager::start_with(node.ctx.clone(), options);
    assert!(wait_until(Duration::from_secs(10), || all_lanes(
        &manager,
        SERVER,
        LaneState::Ready
    )));
    thread::sleep(Duration::from_millis(600));
    link.down(LINK_SILENT);
    // The sessions die (1 s), then the peer's cadence starts one dial per
    // second, each hanging in its handshake (about 3 s): three lanes hang at
    // once, begun about a second apart.
    assert!(
        wait_until(Duration::from_secs(15), || {
            lanes(&manager, SERVER)
                .iter()
                .filter(|lane| lane.state == LaneState::Dialing && lane.connects == 1)
                .count()
                >= 3
        }),
        "{:?}",
        lanes(&manager, SERVER)
    );
    let restored = Instant::now();
    link.up();
    assert!(
        wait_until(Duration::from_secs(10), || {
            ready_times(&node, SERVER, 4, restored)
                .iter()
                .all(Option::is_some)
        }),
        "{:?}",
        lanes(&manager, SERVER)
    );
    let ready: Vec<Instant> = ready_times(&node, SERVER, 4, restored)
        .into_iter()
        .flatten()
        .collect();
    let first = *ready.iter().min().unwrap();
    let last = *ready.iter().max().unwrap();
    // Waiting out each hang would spread them over about two seconds.
    assert!(
        last.duration_since(first) <= Duration::from_millis(700),
        "the last lane was Ready {:?} after the first",
        last.duration_since(first)
    );
    manager.shutdown(Duration::from_secs(5));
}

/// A peer that closes every session as soon as it is established (a
/// flapping acceptor: a lane runner that gives up on every session, a
/// protocol-violation close), through a link with a real round trip. A
/// lane's session then ends before a sibling's dial does, so lanes that
/// woke each other on every connect would redial at one session per dial
/// time for as long as the peer flaps (the WP5.3 review's blocker). Each
/// lane keeps its own escalating backoff instead: no lane redials before
/// the delay it declared after a session the peer ended, and the session
/// rate decays.
#[test]
fn a_flapping_peer_keeps_escalating_over_a_slow_link() {
    let node = local();
    let (server, code) = FakePeer::start(SERVER);
    server.state.close_next.store(1_000_000, Ordering::SeqCst);
    // Every chunk is held 30 ms each way (a 60 ms round trip).
    let endpoint = delay_proxy(
        server.addr,
        Duration::from_millis(30),
        Duration::from_millis(30),
    );
    node.add_peer(&endpoint, &code, SERVER, 4);
    let options = PeerOptions {
        lane_backoff_base: Duration::from_millis(200),
        lane_backoff_cap: Duration::from_secs(5),
        ..fast()
    };
    let start = Instant::now();
    let manager = PeerManager::start_with(node.ctx.clone(), options);
    let established_at = |at: Duration| {
        thread::sleep((start + at).saturating_duration_since(Instant::now()));
        server.established()
    };
    let early = established_at(Duration::from_secs(3));
    let before_late = established_at(Duration::from_secs(6));
    let total = established_at(Duration::from_secs(9));
    manager.shutdown(Duration::from_secs(5));
    let late = total - before_late;
    // Escalating (200 ms, 400 ms, … up to 5 s, ±20 %, plus a dial each
    // time), each lane connects about four times in the first 3 s and once
    // or twice from 6 s to 9 s; lanes waking each other connect at a
    // constant rate (one session per dial time and lane pair).
    assert!(early >= 4, "{early} sessions in the first 3 s");
    assert!(
        late * 2 < early,
        "the session rate does not decay: {early} sessions in the first 3 s, {late} from 6 s to 9 s ({total} in all)"
    );
    for lane in 0..4 {
        let events = node.recorder.lane(SERVER, lane);
        for (index, (at, ms, state, _, until)) in events.iter().enumerate() {
            // A backoff after a session (Ready before it), not after a dial.
            if *state != LaneState::Backoff || index == 0 || events[index - 1].2 != LaneState::Ready
            {
                continue;
            }
            let delay = declared_delay(*ms, *until);
            if let Some((next, ..)) = events[index + 1..]
                .iter()
                .find(|(_, _, state, _, _)| *state == LaneState::Dialing)
            {
                let waited = next.duration_since(*at).as_millis() as u64;
                assert!(
                    waited + 20 >= delay,
                    "lane {lane}: redialed {waited} ms into the {delay} ms backoff after a session the peer closed"
                );
            }
        }
    }
}
