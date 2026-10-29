//! The orchestrator's peer network (spec §7.2, §7.10): outbound lanes to
//! paired peers, inbound sessions on the `[network] listen` address, and
//! pairing.
//!
//! **Outbound.** Per paired peer (a catalog `peers` row whose installation is
//! still in `paired`), a pool of `max(connections, 2)` lanes, one thread
//! each: lane 0 is `control`, lanes `1..pool` are `bulk` with
//! `LaneSpec { index: i − 1, count: pool − 1 }`. Every lane runs the state
//! machine of [`LaneState`]: `Dialing` (a dial is bounded by the session
//! layer: [`DIAL_TIMEOUT`] per TCP connect, 5 s per handshake step) →
//! `Ready`; a failed dial or a lost session → `Backoff` (1 s doubling to
//! [`LANE_BACKOFF_MAX`], ±20 %), the attempt count reset by a session that
//! stayed Ready for [`READY_RESET`] (it redials at once); a version mismatch
//! → `Incompatible` for [`INCOMPATIBLE_HOLD`]; a refused credential
//! (`ACCESS_DENIED`, `PEER_REVOKED`, `PAIRING_CODE_CHANGED`,
//! `INVALID_PAIRING_CODE`) → `Unauthorized` for [`UNAUTHORIZED_HOLD`].
//! `clock::check()` runs before every dial (`CLOCK_INVALID` → Backoff).
//!
//! **Inbound.** The accept loop runs every handshake on its own thread, at
//! most [`PRE_AUTH_CAP`] at once (more are closed unanswered), and admits at
//! most [`SESSION_CAP`] sessions (`GOAWAY{TOO_MANY_SESSIONS}`). Each session
//! is served by a [`PeerService`] and kept so a pairing-code reset can send
//! it `GOAWAY{PAIRING_CODE_CHANGED}` and shutdown can close it.
//!
//! **Pairing.** `pair` dials a probe lane with the code, enrolls a fresh
//! secret (`peer.enroll`, header `{"endpoint": advertise}`, `null` on a
//! dial-only host, whose peer then stores no endpoint for it), saves the peer
//! and opens its lanes. A reset of this side's code revokes every inbound
//! credential (`paired` is cleared), sends the GOAWAY above and closes every
//! outbound lane; the `peers` rows stay (listed with `"paired": false`, no
//! lanes) so the owner can re-pair by endpoint.
use super::{catalog::Catalog, catalog::Peer, config::Config, worker_client::WorkerLink};
use crate::{
    clock,
    content::Kind,
    model::{
        content::Digest,
        errors::{ErrorClass, ErrorWire},
        protocol::{
            LaneRole, LaneSpec,
            frame::{ClientHello, Limits},
            ops::{PEER_ENROLL, PEER_REVOKE},
        },
    },
    pairing_code::Verifier,
    session::{
        self, AcceptPolicy, CodeAuth, Conn, ConnHealth, ConnOptions, Handler, Incoming, PeerInfo,
        Priority, Side,
        auth::{self, PeerAuth},
    },
    util::{
        cancel::Cancel,
        lock::lock,
        log::{event, register_secret},
        time::now_ms,
    },
};
use anyhow::{Context, Result, anyhow, ensure};
use rand::Rng;
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

pub const DIAL_TIMEOUT: Duration = Duration::from_secs(3);
pub const LANE_BACKOFF_MAX: Duration = Duration::from_secs(60);
pub const READY_RESET: Duration = Duration::from_secs(5);
pub const INCOMPATIBLE_HOLD: Duration = Duration::from_secs(600);
pub const UNAUTHORIZED_HOLD: Duration = Duration::from_secs(60);
pub const PRE_AUTH_CAP: usize = 32;
pub const SESSION_CAP: usize = 128;
/// Largest pool `[network] connections` / `pair --connections` allows.
pub const MAX_POOL: usize = 16;
/// Deadline of one request this side originates (`catalog.list`, …).
const REQUEST_DEADLINE: Duration = Duration::from_secs(10);
/// How often a Ready lane checks whether its session ended.
const LANE_POLL: Duration = Duration::from_millis(250);
/// Jitter bound of a lane backoff (±20 %).
const JITTER: f64 = 0.2;
/// Refusals of the credential itself: the lane waits [`UNAUTHORIZED_HOLD`].
const AUTH_REFUSALS: &[&str] = &[
    "ACCESS_DENIED",
    "PEER_REVOKED",
    "PAIRING_CODE_CHANGED",
    "INVALID_PAIRING_CODE",
];

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "state")]
pub enum LaneState {
    Idle,
    Dialing {
        since_ms: u64,
    },
    Ready {
        since_ms: u64,
        health: ConnHealth,
    },
    Backoff {
        until_ms: u64,
        attempt: u32,
        error: String,
    },
    Incompatible {
        until_ms: u64,
    },
    Unauthorized {
        until_ms: u64,
    },
}

/// The folders the network serves to peers (implemented over the daemon's
/// folder slots).
pub trait FolderDirectory: Send + Sync {
    /// The worker link of a Running, unpaused folder of `repo`.
    fn link_by_repo(&self, repo: &str) -> Option<Arc<WorkerLink>>;
    /// `[{"repo","label"}]` of every folder, paused ones included.
    fn published(&self) -> Vec<Value>;
}

/// The backoff after `attempt` failures in a row: `1 s · 2^(attempt−1)`
/// capped at [`LANE_BACKOFF_MAX`], then `× (1 + jitter)` with `jitter`
/// clamped to ±0.2 (NaN → 0).
pub fn lane_backoff(attempt: u32, jitter: f64) -> Duration {
    let exponent = attempt.saturating_sub(1).min(31);
    let nominal_ms = (1000u64 << exponent).min(LANE_BACKOFF_MAX.as_millis() as u64);
    let jitter = if jitter.is_finite() {
        jitter.clamp(-JITTER, JITTER)
    } else {
        0.0
    };
    Duration::from_millis((nominal_ms as f64 * (1.0 + jitter)).round() as u64)
}

/// What a refused dial or a closed session means for a lane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Refusal {
    Incompatible,
    Unauthorized,
    Transient,
}

fn refusal(error: &ErrorWire) -> Refusal {
    if AUTH_REFUSALS.contains(&error.code.as_str()) {
        Refusal::Unauthorized
    } else if error.class == ErrorClass::Incompatible {
        Refusal::Incompatible
    } else {
        Refusal::Transient
    }
}

/// One lane's state, shared between its thread and readers.
struct Lane {
    index: usize,
    role: LaneRole,
    spec: Option<LaneSpec>,
    slot: Mutex<LaneSlot>,
}

struct LaneSlot {
    state: LaneState,
    conn: Option<Conn>,
    last_error: Option<String>,
}

impl Lane {
    fn new(index: usize, pool: usize) -> Arc<Self> {
        let (role, spec) = if index == 0 {
            (LaneRole::Control, None)
        } else {
            (
                LaneRole::Bulk,
                Some(LaneSpec::new(index as u32 - 1, pool as u32 - 1)),
            )
        };
        Arc::new(Self {
            index,
            role,
            spec,
            slot: Mutex::new(LaneSlot {
                state: LaneState::Idle,
                conn: None,
                last_error: None,
            }),
        })
    }

    fn set(&self, state: LaneState) {
        lock(&self.slot).state = state;
    }

    fn fail(&self, state: LaneState, error: String) {
        let mut slot = lock(&self.slot);
        slot.state = state;
        slot.last_error = Some(error);
    }

    /// The session, while it is Ready and open.
    fn ready(&self) -> Option<Conn> {
        let slot = lock(&self.slot);
        matches!(slot.state, LaneState::Ready { .. })
            .then(|| slot.conn.clone())
            .flatten()
            .filter(|conn| conn.closed().is_none())
    }

    fn close(&self) {
        if let Some(conn) = &lock(&self.slot).conn {
            conn.close();
        }
    }

    /// `{lane, role, spec, state…, last_error}` with live health.
    fn view(&self) -> Value {
        let slot = lock(&self.slot);
        let mut state = slot.state.clone();
        if let (LaneState::Ready { health, .. }, Some(conn)) = (&mut state, &slot.conn) {
            *health = conn.health();
        }
        let mut view = serde_json::to_value(&state).unwrap_or_else(|_| json!({}));
        view["lane"] = json!(self.index);
        view["role"] = json!(self.role.as_str());
        view["spec"] = json!(self.spec);
        view["last_error"] = json!(slot.last_error);
        view
    }
}

/// Where a peer's lanes dial and with which credential. `Debug` is not
/// derived: `key` is a secret.
struct Target {
    /// The clock check before every dial (`clock::check`; tests swap it).
    check_clock: fn() -> Result<()>,
    endpoint: String,
    /// The peer's installation (`dial_expecting`).
    remote: String,
    key: [u8; 32],
    /// This orchestrator's installation.
    local: String,
    pool: usize,
}

/// The lanes of one outbound peer.
struct PeerLanes {
    installation: String,
    lanes: Vec<Arc<Lane>>,
    cancel: Cancel,
    threads: Vec<JoinHandle<()>>,
}

impl PeerLanes {
    fn stop(&self) {
        self.cancel.cancel();
        self.lanes.iter().for_each(|lane| lane.close());
    }

    fn finished(&self) -> bool {
        self.threads.iter().all(JoinHandle::is_finished)
    }
}

/// A peer with a Ready control lane.
pub struct ReadyPeer {
    pub endpoint: String,
    pub installation: String,
    /// The control session's id (a new one is a Ready transition).
    pub session: String,
    pub control: Conn,
}

pub struct Network {
    catalog: Arc<Catalog>,
    installation: String,
    /// Default lanes per peer (`Config::pool`).
    pool: usize,
    /// The endpoint peers are told in `peer.enroll`.
    advertise: Option<String>,
    folders: Arc<dyn FolderDirectory>,
    peers: Mutex<BTreeMap<String, PeerLanes>>,
    /// Admitted inbound sessions (closed ones are pruned on use).
    inbound: Mutex<Vec<Conn>>,
    pre_auth: AtomicUsize,
    stopping: Cancel,
}

/// Starts the network: the accept loop on `listener` (none: dial only) and
/// the lanes of every paired peer. `stopping` (the daemon's) also stops it.
pub fn start(
    config: &Config,
    catalog: Arc<Catalog>,
    folders: Arc<dyn FolderDirectory>,
    stopping: Cancel,
    listener: Option<TcpListener>,
) -> Result<Arc<Network>> {
    let installation = catalog.installation()?;
    let listen = listener
        .as_ref()
        .map(TcpListener::local_addr)
        .transpose()
        .context("LISTEN_FAILED")?;
    let advertise = config
        .advertise()
        .map(str::to_owned)
        .or_else(|| listen.map(|address| format!("ws://{address}")));
    let network = Arc::new(Network {
        catalog,
        installation,
        pool: config.pool().min(MAX_POOL),
        advertise,
        folders,
        peers: Mutex::new(BTreeMap::new()),
        inbound: Mutex::new(Vec::new()),
        pre_auth: AtomicUsize::new(0),
        stopping: stopping.child(),
    });
    if let Some(listener) = listener {
        let weak = Arc::downgrade(&network);
        thread::Builder::new()
            .name("tkfs-peer-accept".into())
            .spawn(move || accept_loop(weak, listener))
            .context("PEER_ACCEPT_THREAD_FAILED")?;
    }
    for peer in network.catalog.peers()? {
        if network.catalog.paired_key(&peer.installation).is_ok() {
            network.open_lanes_or_log(&peer);
        }
    }
    Ok(network)
}

impl Network {
    /// Pairs with `endpoint` using its pairing `code`; returns its
    /// installation. Nothing is saved unless the peer enrolled our secret.
    pub fn pair(&self, endpoint: &str, code: &str, connections: Option<usize>) -> Result<String> {
        crate::ws::endpoint_uri(endpoint)?;
        if let Some(connections) = connections {
            ensure!(
                (1..=MAX_POOL).contains(&connections),
                "INVALID_REQUEST: connections must be 1..={MAX_POOL}"
            );
        }
        // A dial-only host (no listener, no advertise) tells the peer no
        // endpoint: the peer keeps the pairing inbound only.
        let advertise = self.advertise.clone();
        register_secret(code);
        clock::check()?;
        let hello = session::client_hello(&self.installation, LaneRole::Probe, None, 1);
        let (conn, server) = session::dial_cancellable(
            endpoint,
            hello,
            &CodeAuth {
                code: code.to_owned(),
            },
            None,
            None,
            ConnOptions::for_role(LaneRole::Probe),
            &self.stopping,
        )
        .map_err(|error| anyhow!("PAIRING_FAILED: {}", error.to_wire()))?;
        // Paired the other way already (it enrolled with us, or we with
        // it): reuse that credential when the peer still accepts it, so the
        // two directions keep one key. Otherwise enroll a fresh one.
        let reused = self
            .catalog
            .paired_key(&server.installation)
            .ok()
            .filter(|key| self.accepts(endpoint, &server.installation, *key));
        let key = match reused {
            Some(key) => {
                conn.close();
                key
            }
            None => {
                let secret = auth::new_secret();
                let enrolled = serde_json::to_vec(&auth::enrollment_request(&secret))
                    .map_err(|error| anyhow!("PAIRING_FAILED: {error}"))
                    .and_then(|body| {
                        conn.call(
                            PEER_ENROLL,
                            json!({"endpoint": advertise}),
                            body,
                            Priority::Control,
                            auth::ENROLL_DEADLINE,
                        )
                        .map_err(|error| anyhow!("PAIRING_FAILED: {error}"))
                    });
                conn.close();
                enrolled?;
                auth::peer_key(&secret)
            }
        };
        self.catalog
            .save_peer(endpoint, &server.installation, key, connections)?;
        // Every row of this installation now carries `key`.
        self.reopen(&server.installation);
        event!(info, PEER_PAIRED, {
            "endpoint": endpoint,
            "installation": server.installation.clone(),
            "connections": connections,
            "enrolled": reused.is_none(),
        });
        Ok(server.installation)
    }

    /// Whether `endpoint` (installation `installation`) accepts `key`: a
    /// probe session with it authenticates.
    fn accepts(&self, endpoint: &str, installation: &str, key: [u8; 32]) -> bool {
        let hello = session::client_hello(&self.installation, LaneRole::Probe, None, 1);
        match session::dial_cancellable(
            endpoint,
            hello,
            &PeerAuth { key },
            Some(installation),
            None,
            ConnOptions::for_role(LaneRole::Probe),
            &self.stopping,
        ) {
            Ok((conn, _)) => {
                conn.close();
                true
            }
            Err(_) => false,
        }
    }

    /// (Re)opens the lanes of every paired `peers` row of `installation`
    /// with the key the catalog now holds.
    fn reopen(&self, installation: &str) {
        if self.catalog.paired_key(installation).is_err() {
            return;
        }
        for peer in self.catalog.peers().unwrap_or_default() {
            if peer.installation == installation {
                self.open_lanes_or_log(&peer);
            }
        }
    }

    fn open_lanes_or_log(&self, peer: &Peer) {
        if let Err(error) = self.open_lanes(peer) {
            event!(warn, PEER_LANES_SKIPPED, {
                "endpoint": peer.endpoint.clone(),
                "installation": peer.installation.clone(),
                "error": format!("{error:#}"),
            });
        }
    }

    /// Forgets `installation`: best-effort `peer.revoke` on its control lane,
    /// then the catalog rows, its lanes and its inbound sessions.
    pub fn unpair(&self, installation: &str) -> Result<()> {
        let outbound: Vec<Peer> = self
            .catalog
            .peers()?
            .into_iter()
            .filter(|peer| peer.installation == installation)
            .collect();
        ensure!(
            !outbound.is_empty() || self.catalog.paired_key(installation).is_ok(),
            "PEER_NOT_FOUND: no pairing with installation {installation:?}"
        );
        let revoked = outbound.iter().find_map(|peer| match self.revoke(peer) {
            Ok(()) => Some(peer.endpoint.clone()),
            Err(error) => {
                event!(warn, PEER_REVOKE_FAILED, {
                    "endpoint": peer.endpoint.clone(),
                    "error": format!("{error:#}"),
                });
                None
            }
        });
        self.catalog.unpair(installation)?;
        self.forget(installation, "PEER_REVOKED");
        event!(info, PEER_UNPAIRED, {
            "installation": installation,
            "revoked_at": revoked,
        });
        Ok(())
    }

    fn revoke(&self, peer: &Peer) -> Result<()> {
        let (conn, temporary) = match self.control_lane(&peer.endpoint) {
            Some(conn) => (conn, false),
            None => (self.dial(peer, LaneRole::Control)?, true),
        };
        let result = conn.call(
            PEER_REVOKE,
            json!({}),
            Vec::new(),
            Priority::Control,
            auth::REVOKE_DEADLINE,
        );
        if temporary {
            conn.close();
        }
        result.map(drop).map_err(|error| anyhow!("{error}"))
    }

    /// Closes the lanes to `installation` and sends its inbound sessions a
    /// `GOAWAY{code}`.
    fn forget(&self, installation: &str, code: &str) {
        let removed: Vec<PeerLanes> = {
            let mut peers = lock(&self.peers);
            let endpoints: Vec<String> = peers
                .iter()
                .filter(|(_, lanes)| lanes.installation == installation)
                .map(|(endpoint, _)| endpoint.clone())
                .collect();
            endpoints
                .iter()
                .filter_map(|endpoint| peers.remove(endpoint))
                .collect()
        };
        removed.iter().for_each(PeerLanes::stop);
        for conn in self.sessions() {
            if conn.peer().installation == installation {
                conn.goaway(code, "this installation is no longer paired");
            }
        }
    }

    /// A new pairing code: every inbound credential is revoked, every inbound
    /// session gets `GOAWAY{PAIRING_CODE_CHANGED}` and every outbound lane is
    /// closed (the `peers` rows stay for a re-pair).
    pub fn reset_pairing_code(&self) -> Result<String> {
        let code = self.catalog.reset_code()?;
        register_secret(&code);
        {
            let mut inbound = lock(&self.inbound);
            inbound.retain(|conn| conn.closed().is_none());
            for conn in inbound.iter() {
                conn.goaway("PAIRING_CODE_CHANGED", "the pairing code was reset");
            }
        }
        let closed = std::mem::take(&mut *lock(&self.peers));
        closed.values().for_each(PeerLanes::stop);
        event!(info, PAIRING_CODE_RESET, { "outbound_closed": closed.len() });
        Ok(code)
    }

    /// The folders `endpoint` publishes (`catalog.list`), over its control
    /// lane when Ready, else a probe lane with the stored credential.
    pub fn remote_list(&self, endpoint: &str) -> Result<Vec<Value>> {
        let reply = match self.control_lane(endpoint) {
            Some(conn) => conn.call(
                "catalog.list",
                json!({}),
                Vec::new(),
                Priority::Control,
                REQUEST_DEADLINE,
            ),
            None => {
                let peer = self
                    .catalog
                    .peer(endpoint)
                    .ok()
                    .filter(|peer| self.catalog.paired_key(&peer.installation).is_ok())
                    .with_context(|| {
                        format!("PAIRING_CODE_REQUIRED: {endpoint} is not paired; pair it first")
                    })?;
                let conn = self.dial(&peer, LaneRole::Probe)?;
                let reply = conn.call(
                    "catalog.list",
                    json!({}),
                    Vec::new(),
                    Priority::Control,
                    REQUEST_DEADLINE,
                );
                conn.close();
                reply
            }
        };
        let reply = reply.map_err(|error| anyhow!("{error}"))?;
        serde_json::from_slice(&reply.body).context("PROTOCOL_VIOLATION: catalog.list reply")
    }

    /// A one-off session to a saved peer with its credential.
    fn dial(&self, peer: &Peer, role: LaneRole) -> Result<Conn> {
        clock::check()?;
        let hello = session::client_hello(&self.installation, role, None, self.pool as u32);
        let (conn, _) = session::dial_cancellable(
            &peer.endpoint,
            hello,
            &PeerAuth { key: peer.key()? },
            Some(&peer.installation),
            None,
            ConnOptions::for_role(role),
            &self.stopping,
        )
        .map_err(|error| anyhow!("{}", error.to_wire()))?;
        Ok(conn)
    }

    /// `[{"endpoint","installation","direction","paired","connections",
    /// "lanes":[…],"inbound","clock"}]`: every outbound peer row, then every
    /// paired installation with no row (an inbound pairing, `endpoint` null).
    pub fn peers(&self) -> Value {
        let outbound = self.catalog.peers().unwrap_or_default();
        let paired: BTreeSet<String> = self
            .catalog
            .paired_installations()
            .unwrap_or_default()
            .into_iter()
            .collect();
        let mut sessions: BTreeMap<String, usize> = BTreeMap::new();
        for conn in self.sessions() {
            *sessions
                .entry(conn.peer().installation.clone())
                .or_default() += 1;
        }
        let clock = |installation: &str| match self.catalog.clock(installation) {
            Ok(Some((offset, measured_at))) => json!({
                "offset_ms": offset.offset_ms,
                "rtt_ms": offset.rtt_ms,
                "measured_at": measured_at,
            }),
            _ => Value::Null,
        };
        let lanes = lock(&self.peers);
        let mut list: Vec<Value> = outbound
            .iter()
            .map(|peer| {
                let views: Vec<Value> = lanes
                    .get(&peer.endpoint)
                    .map(|lanes| lanes.lanes.iter().map(|lane| lane.view()).collect())
                    .unwrap_or_default();
                json!({
                    "endpoint": peer.endpoint,
                    "installation": peer.installation,
                    "direction": "outbound",
                    "paired": paired.contains(&peer.installation),
                    "connections": peer.connections,
                    "lanes": views,
                    "inbound": sessions.get(&peer.installation).copied().unwrap_or(0),
                    "clock": clock(&peer.installation),
                })
            })
            .collect();
        drop(lanes);
        let known: BTreeSet<&str> = outbound
            .iter()
            .map(|peer| peer.installation.as_str())
            .collect();
        for installation in paired.iter().filter(|i| !known.contains(i.as_str())) {
            list.push(json!({
                "endpoint": Value::Null,
                "installation": installation,
                "direction": "inbound",
                "paired": true,
                "connections": Value::Null,
                "lanes": [],
                "inbound": sessions.get(installation).copied().unwrap_or(0),
                "clock": clock(installation),
            }));
        }
        Value::Array(list)
    }

    /// Every outbound peer whose control lane is Ready and open, with that
    /// session (the sync driver's scheduler and clock probes).
    pub fn ready_peers(&self) -> Vec<ReadyPeer> {
        lock(&self.peers)
            .iter()
            .filter_map(|(endpoint, lanes)| {
                let control = lanes.lanes.first()?.ready()?;
                Some(ReadyPeer {
                    endpoint: endpoint.clone(),
                    installation: lanes.installation.clone(),
                    session: control.peer().session.clone(),
                    control,
                })
            })
            .collect()
    }

    /// The endpoints with at least one Ready bulk lane (the object pump).
    pub fn bulk_endpoints(&self) -> Vec<String> {
        lock(&self.peers)
            .iter()
            .filter(|(_, lanes)| {
                lanes
                    .lanes
                    .iter()
                    .any(|lane| lane.role == LaneRole::Bulk && lane.ready().is_some())
            })
            .map(|(endpoint, _)| endpoint.clone())
            .collect()
    }

    /// The Ready control lane to `endpoint`.
    pub fn control_lane(&self, endpoint: &str) -> Option<Conn> {
        lock(&self.peers).get(endpoint)?.lanes.first()?.ready()
    }

    /// The Ready bulk lane to `endpoint` that owns `digest`.
    pub fn bulk_lane(&self, endpoint: &str, digest: &Digest) -> Option<Conn> {
        let hash = digest.to_string();
        lock(&self.peers)
            .get(endpoint)?
            .lanes
            .iter()
            .find(|lane| lane.spec.is_some_and(|spec| spec.owns(&hash)))?
            .ready()
    }

    /// Closes every lane and inbound session, waiting at most `budget` for
    /// lane threads to end and sessions to close.
    pub fn shutdown(&self, budget: Duration) {
        let started = Instant::now();
        self.stopping.cancel();
        let peers = std::mem::take(&mut *lock(&self.peers));
        peers.values().for_each(PeerLanes::stop);
        let inbound = std::mem::take(&mut *lock(&self.inbound));
        for conn in &inbound {
            conn.goaway("ORCHESTRATOR_STOPPING", "the orchestrator is shutting down");
        }
        let end = started + budget;
        while Instant::now() < end
            && !(peers.values().all(PeerLanes::finished)
                && inbound.iter().all(|conn| conn.closed().is_some()))
        {
            thread::sleep(Duration::from_millis(20));
        }
        inbound.iter().for_each(Conn::close);
        event!(info, PEER_NETWORK_STOPPED, {
            "elapsed_ms": started.elapsed().as_millis() as u64,
            "peers": peers.len(),
            "lane_threads_left": peers.values().filter(|lanes| !lanes.finished()).count(),
            "inbound": inbound.len(),
        });
    }

    /// Starts (or restarts) the lanes of `peer`.
    fn open_lanes(&self, peer: &Peer) -> Result<()> {
        let pool = peer.connections.unwrap_or(self.pool).clamp(2, MAX_POOL);
        let target = Arc::new(Target {
            check_clock: clock::check,
            endpoint: peer.endpoint.clone(),
            remote: peer.installation.clone(),
            key: peer.key()?,
            local: self.installation.clone(),
            pool,
        });
        let cancel = self.stopping.child();
        let lanes: Vec<Arc<Lane>> = (0..pool).map(|index| Lane::new(index, pool)).collect();
        let mut threads = Vec::with_capacity(pool);
        for lane in &lanes {
            let (lane, target, token) = (lane.clone(), target.clone(), cancel.clone());
            let spawned = thread::Builder::new()
                .name(format!("tkfs-lane-{}", lane.index))
                .spawn(move || run_lane(&lane, &target, &token));
            match spawned {
                Ok(handle) => threads.push(handle),
                Err(error) => {
                    cancel.cancel();
                    return Err(error).context("LANE_THREAD_FAILED");
                }
            }
        }
        let previous = lock(&self.peers).insert(
            peer.endpoint.clone(),
            PeerLanes {
                installation: peer.installation.clone(),
                lanes,
                cancel,
                threads,
            },
        );
        if let Some(previous) = previous {
            previous.stop();
        }
        Ok(())
    }

    /// Open inbound sessions (closed ones are pruned).
    fn sessions(&self) -> Vec<Conn> {
        let mut inbound = lock(&self.inbound);
        inbound.retain(|conn| conn.closed().is_none());
        inbound.clone()
    }

    fn session_count(&self) -> usize {
        self.sessions().len()
    }

    /// Runs one inbound handshake and, when admitted, serves the session.
    fn accept_session(self: &Arc<Self>, stream: TcpStream) {
        let policy = Admission(self.clone());
        let (ws, peer) = match session::accept_v3(stream, &policy, Limits::default()) {
            Ok(accepted) => accepted,
            Err(error) => {
                event!(info, PEER_SESSION_REFUSED, { "error": error.to_string() });
                return;
            }
        };
        let conn = match Conn::from_established(
            ws,
            Side::Server,
            peer.clone(),
            ConnOptions::for_role(peer.role),
        ) {
            Ok(conn) => conn,
            Err(error) => {
                event!(warn, PEER_SESSION_FAILED, { "error": error.to_string() });
                return;
            }
        };
        {
            let mut inbound = lock(&self.inbound);
            inbound.retain(|conn| conn.closed().is_none());
            let generation = self.catalog.verifier().map(|(_, g)| g).unwrap_or(0);
            if self.stopping.is_cancelled() {
                conn.goaway("ORCHESTRATOR_STOPPING", "the orchestrator is shutting down");
                return;
            }
            if inbound.len() >= SESSION_CAP {
                conn.goaway("TOO_MANY_SESSIONS", "the session limit is reached");
                return;
            }
            // A reset that ran while this handshake finished.
            if peer.verifier_generation != generation {
                conn.goaway("PAIRING_CODE_CHANGED", "the pairing code was reset");
                return;
            }
            inbound.push(conn.clone());
        }
        conn.serve(Arc::new(PeerService {
            network: self.clone(),
            peer,
        }));
    }
}

/// Accepts peer connections until the network is gone; each handshake runs
/// on its own thread, at most [`PRE_AUTH_CAP`] at once.
fn accept_loop(network: Weak<Network>, listener: TcpListener) {
    for stream in listener.incoming() {
        let Some(network) = network.upgrade() else {
            return;
        };
        let stream = match stream {
            Ok(stream) => stream,
            Err(_) => {
                // A persistent accept error must not spin.
                thread::sleep(Duration::from_millis(50));
                continue;
            }
        };
        if network.stopping.is_cancelled() {
            continue;
        }
        if network.pre_auth.fetch_add(1, Ordering::AcqRel) >= PRE_AUTH_CAP {
            network.pre_auth.fetch_sub(1, Ordering::AcqRel);
            event!(warn, PEER_PRE_AUTH_FULL, { "limit": PRE_AUTH_CAP });
            continue;
        }
        let worker = network.clone();
        let spawned = thread::Builder::new()
            .name("tkfs-peer-session".into())
            .spawn(move || {
                let _guard = PreAuth(&worker.pre_auth);
                worker.accept_session(stream);
            });
        if spawned.is_err() {
            network.pre_auth.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// Releases a pre-auth slot.
struct PreAuth<'a>(&'a AtomicUsize);

impl Drop for PreAuth<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The catalog's policy plus the session cap.
struct Admission(Arc<Network>);

impl AcceptPolicy for Admission {
    fn installation(&self) -> String {
        AcceptPolicy::installation(&*self.0.catalog)
    }
    fn verifier(&self) -> Option<(Verifier, u64)> {
        AcceptPolicy::verifier(&*self.0.catalog)
    }
    fn admit(&self, hello: &ClientHello) -> Result<(), ErrorWire> {
        if self.0.session_count() >= SESSION_CAP {
            return Err(ErrorWire::coded(
                "TOO_MANY_SESSIONS",
                format!("{SESSION_CAP} peer sessions are open"),
            ));
        }
        self.0.catalog.admit(hello)
    }
    fn legacy_attempt(&self, addr: SocketAddr, _hello: &Value) {
        // The legacy hello may carry a pairing code: never logged.
        event!(warn, PEER_LEGACY_ATTEMPT, { "address": addr.to_string() });
    }
    fn peer_key(&self, installation: &str) -> Result<[u8; 32], ErrorWire> {
        AcceptPolicy::peer_key(&*self.0.catalog, installation)
    }
}

/// One outbound lane: dial, hold the session while Ready, back off.
fn run_lane(lane: &Lane, target: &Target, cancel: &Cancel) {
    let mut attempt: u32 = 0;
    while !cancel.is_cancelled() {
        lane.set(LaneState::Dialing { since_ms: now_ms() });
        let dialed = (target.check_clock)()
            .map_err(|error| ErrorWire::new("CLOCK_INVALID", format!("{error:#}")))
            .and_then(|()| {
                let hello =
                    session::client_hello(&target.local, lane.role, lane.spec, target.pool as u32);
                session::dial_cancellable(
                    &target.endpoint,
                    hello,
                    &PeerAuth { key: target.key },
                    Some(&target.remote),
                    None,
                    ConnOptions::for_role(lane.role),
                    cancel,
                )
                .map(|(conn, _)| conn)
                .map_err(|error| error.to_wire())
            });
        if cancel.is_cancelled() {
            if let Ok(conn) = &dialed {
                conn.close();
            }
            break;
        }
        let error = match dialed {
            Ok(conn) => {
                let ready_at = Instant::now();
                {
                    let mut slot = lock(&lane.slot);
                    slot.state = LaneState::Ready {
                        since_ms: now_ms(),
                        health: conn.health(),
                    };
                    slot.conn = Some(conn.clone());
                }
                while conn.closed().is_none() && !cancel.sleep(LANE_POLL) {}
                conn.close();
                lock(&lane.slot).conn = None;
                if cancel.is_cancelled() {
                    break;
                }
                let reason = conn
                    .closed()
                    .unwrap_or_else(|| ErrorWire::coded("WS_CLOSED", "the session ended"));
                if refusal(&reason) == Refusal::Transient && ready_at.elapsed() >= READY_RESET {
                    // A session that stayed up redials at once.
                    attempt = 0;
                    lane.fail(LaneState::Idle, reason.to_string());
                    continue;
                }
                reason
            }
            Err(error) => error,
        };
        let hold = match refusal(&error) {
            Refusal::Unauthorized => {
                lane.fail(
                    LaneState::Unauthorized {
                        until_ms: now_ms() + UNAUTHORIZED_HOLD.as_millis() as u64,
                    },
                    error.to_string(),
                );
                UNAUTHORIZED_HOLD
            }
            Refusal::Incompatible => {
                lane.fail(
                    LaneState::Incompatible {
                        until_ms: now_ms() + INCOMPATIBLE_HOLD.as_millis() as u64,
                    },
                    error.to_string(),
                );
                INCOMPATIBLE_HOLD
            }
            Refusal::Transient => {
                attempt = attempt.saturating_add(1);
                let delay = lane_backoff(attempt, rand::thread_rng().gen_range(-JITTER..=JITTER));
                lane.fail(
                    LaneState::Backoff {
                        until_ms: now_ms() + delay.as_millis() as u64,
                        attempt,
                        error: error.to_string(),
                    },
                    error.to_string(),
                );
                delay
            }
        };
        if cancel.sleep(hold) {
            break;
        }
    }
    lane.set(LaneState::Idle);
}

/// Serves one inbound session. Ops by the session's role: `objects.*` on
/// bulk lanes only, `meta.exchange` on the control lane only, `catalog.list`,
/// `peer.enroll`, `peer.revoke`, `ping` and `clock.probe` on probe and
/// control lanes; anything else is refused with `OP_NOT_ALLOWED`. The
/// session layer enforces the same policy (`protocol::op_allowed`).
pub struct PeerService {
    network: Arc<Network>,
    peer: PeerInfo,
}

impl PeerService {
    pub fn new(network: Arc<Network>, peer: PeerInfo) -> Self {
        Self { network, peer }
    }

    fn allowed(&self, op: &str) -> Result<(), ErrorWire> {
        let role = self.peer.role;
        let allowed = match op {
            _ if op.starts_with("objects.") => role == LaneRole::Bulk,
            "meta.exchange" => role == LaneRole::Control,
            "catalog.list" | "peer.enroll" | "peer.revoke" | "ping" | "clock.probe" => {
                matches!(role, LaneRole::Probe | LaneRole::Control)
            }
            _ => false,
        };
        if allowed {
            Ok(())
        } else {
            Err(ErrorWire::coded(
                "OP_NOT_ALLOWED",
                format!("{op} is not allowed on a {} lane", role.as_str()),
            ))
        }
    }

    /// Sync ops need a paired (not merely code-authenticated) peer.
    fn require_paired(&self) -> Result<(), ErrorWire> {
        self.network
            .catalog
            .paired_key(&self.peer.installation)
            .map(drop)
            .map_err(|_| ErrorWire::coded("PEER_REVOKED", "this installation is not paired"))
    }

    fn link(&self, header: &Value) -> Result<Arc<WorkerLink>, ErrorWire> {
        let repo = header["repo"]
            .as_str()
            .ok_or_else(|| ErrorWire::coded("INVALID_REQUEST", "repo is required"))?;
        self.network.folders.link_by_repo(repo).ok_or_else(|| {
            ErrorWire::coded(
                "REPO_NOT_PUBLISHED",
                format!("no running, unpaused folder of repo {repo}"),
            )
        })
    }

    fn serve(
        &self,
        op: &str,
        header: &Value,
        body: Vec<u8>,
        deadline: Instant,
    ) -> Result<(Value, Vec<u8>), ErrorWire> {
        let catalog = &self.network.catalog;
        match op {
            "catalog.list" => {
                let published = self.network.folders.published();
                Ok((json!({}), encode(&published)?))
            }
            PEER_ENROLL => {
                let request: auth::EnrollRequest = serde_json::from_slice(&body)
                    .map_err(|error| ErrorWire::coded("INVALID_REQUEST", error))?;
                let secret = auth::enrollment_secret(&request)?;
                let updated = catalog
                    .save_paired(&self.peer.installation, auth::peer_key(&secret))
                    .map_err(|error| ErrorWire::from_error(&error))?;
                // The rows we dial this installation with took the new key:
                // their lanes redial with it.
                if updated > 0 {
                    self.network.reopen(&self.peer.installation);
                }
                event!(info, PEER_ENROLLED, {
                    "installation": self.peer.installation.clone(),
                    "endpoint": header["endpoint"].clone(),
                    "outbound_rows": updated,
                });
                Ok((json!({}), encode(&auth::EnrollReply { enrolled: true })?))
            }
            PEER_REVOKE => {
                catalog
                    .unpair(&self.peer.installation)
                    .map_err(|error| ErrorWire::from_error(&error))?;
                event!(info, PEER_REVOKED_BY_PEER, {
                    "installation": self.peer.installation.clone(),
                });
                Ok((json!({}), encode(&auth::RevokeReply { revoked: true })?))
            }
            "ping" => Ok((json!({"alive": true}), Vec::new())),
            "clock.probe" => {
                let t2 = now_ms();
                let t1 = header["t1"].as_u64().unwrap_or(0);
                Ok((
                    json!({}),
                    encode(&json!({"t1": t1, "t2": t2, "t3": now_ms()}))?,
                ))
            }
            "meta.exchange" => {
                self.require_paired()?;
                let link = self.link(header)?;
                let left = deadline.saturating_duration_since(Instant::now());
                let reply = link
                    .call("meta.exchange", header.clone(), body, left)
                    .map_err(|error| error.to_wire())?;
                Ok((reply.header, reply.body))
            }
            "objects.fetch" => {
                self.require_paired()?;
                let link = self.link(header)?;
                let wanted: Vec<(Digest, Kind)> = serde_json::from_slice(&body)
                    .map_err(|error| ErrorWire::coded("INVALID_REQUEST", error))?;
                // The worker's cut (`truncated_at`, `oversized`) goes along.
                let fetched = link.fetch_reply(&wanted).map_err(|error| error.to_wire())?;
                Ok((fetched.header(), encode(&fetched.objects)?))
            }
            _ => Err(ErrorWire::coded(
                "UNSUPPORTED_OPERATION",
                format!("{op} is not served to peers"),
            )),
        }
    }
}

/// A reply body as JSON.
fn encode(value: &impl Serialize) -> Result<Vec<u8>, ErrorWire> {
    serde_json::to_vec(value).map_err(|error| ErrorWire::coded("INTERNAL_ERROR", error))
}

impl Handler for PeerService {
    fn handle(&self, req: Incoming) {
        let Incoming {
            op,
            header,
            body,
            deadline,
            responder,
            ..
        } = req;
        let result = self
            .allowed(&op)
            .and_then(|()| self.serve(&op, &header, body, deadline));
        let revoked = op == PEER_REVOKE && result.is_ok();
        match result {
            Ok((header, body)) => responder.reply(header, body),
            Err(error) => responder.error(error),
        }
        if revoked {
            // After the reply: the GOAWAY lets it complete.
            self.network.forget(&self.peer.installation, "PEER_REVOKED");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{model::orchestrator::CallError, orchestrator::catalog::Catalog};
    use tungstenite::{WebSocket, stream::MaybeTlsStream};

    type Ws = WebSocket<MaybeTlsStream<TcpStream>>;

    struct NoFolders;

    impl FolderDirectory for NoFolders {
        fn link_by_repo(&self, _: &str) -> Option<Arc<WorkerLink>> {
            None
        }
        fn published(&self) -> Vec<Value> {
            vec![json!({"repo": "r1", "label": "Shared"})]
        }
    }

    fn network(root: &std::path::Path) -> Arc<Network> {
        let mut config = Config::initialize(&root.join("orchestrator.toml")).unwrap();
        config.network.listen = String::new();
        let catalog = Catalog::open(&config.data_directory).unwrap();
        start(&config, catalog, Arc::new(NoFolders), Cancel::new(), None).unwrap()
    }

    fn ws_pair() -> (Ws, Ws) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            tungstenite::accept(MaybeTlsStream::Plain(stream)).unwrap()
        });
        let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let client = tungstenite::client(
            format!("ws://127.0.0.1:{port}/"),
            MaybeTlsStream::Plain(stream),
        )
        .unwrap()
        .0;
        (client, server.join().unwrap())
    }

    fn info(role: LaneRole) -> PeerInfo {
        PeerInfo {
            installation: "peer-a".into(),
            role,
            lane: (role == LaneRole::Bulk).then_some(LaneSpec::new(0, 1)),
            session: "session".into(),
            verifier_generation: 1,
        }
    }

    /// A client connected to a `PeerService` whose session has role
    /// `session` and whose service believes the peer's role is `service`
    /// (differing only to reach the service's own check past the session
    /// layer's).
    fn serve(network: &Arc<Network>, session: LaneRole, service: LaneRole) -> (Conn, Conn) {
        let (client, server) = ws_pair();
        let client =
            Conn::from_established(client, Side::Client, info(session), ConnOptions::default())
                .unwrap();
        let server =
            Conn::from_established(server, Side::Server, info(session), ConnOptions::default())
                .unwrap();
        server.serve(Arc::new(PeerService::new(network.clone(), info(service))));
        (client, server)
    }

    fn code(client: &Conn, op: &str, body: &[u8]) -> String {
        match client.call(
            op,
            json!({"repo": "r1"}),
            body.to_vec(),
            Priority::Control,
            Duration::from_secs(10),
        ) {
            Ok(reply) => format!("OK {}", String::from_utf8_lossy(&reply.body)),
            Err(CallError::Remote(error)) => error.code,
            Err(other) => panic!("{op}: {other}"),
        }
    }

    #[test]
    fn peer_service_refuses_objects_on_control_and_meta_on_bulk() {
        let root = tempfile::tempdir().unwrap();
        let network = network(root.path());
        let wanted = br#"[]"#;
        let cases = [
            // Matching roles: the session layer refuses first.
            (
                LaneRole::Control,
                LaneRole::Control,
                "objects.fetch",
                "OP_NOT_ALLOWED",
            ),
            (
                LaneRole::Bulk,
                LaneRole::Bulk,
                "meta.exchange",
                "OP_NOT_ALLOWED",
            ),
            // Past the session layer: the service's own check refuses.
            (
                LaneRole::Bulk,
                LaneRole::Control,
                "objects.fetch",
                "OP_NOT_ALLOWED",
            ),
            (
                LaneRole::Control,
                LaneRole::Bulk,
                "meta.exchange",
                "OP_NOT_ALLOWED",
            ),
            (
                LaneRole::Control,
                LaneRole::Bulk,
                "catalog.list",
                "OP_NOT_ALLOWED",
            ),
            // Allowed, but the peer is not paired yet.
            (
                LaneRole::Control,
                LaneRole::Control,
                "meta.exchange",
                "PEER_REVOKED",
            ),
            (
                LaneRole::Bulk,
                LaneRole::Bulk,
                "objects.fetch",
                "PEER_REVOKED",
            ),
        ];
        for (session, service, op, expected) in cases {
            let (client, server) = serve(&network, session, service);
            assert_eq!(
                code(&client, op, wanted),
                expected,
                "{op} on a {session:?} session served as {service:?}"
            );
            client.close();
            server.close();
        }
        // Paired: allowed ops reach the folder directory.
        network.catalog.save_paired("peer-a", [7; 32]).unwrap();
        let (control, _server) = serve(&network, LaneRole::Control, LaneRole::Control);
        assert_eq!(code(&control, "meta.exchange", b"{}"), "REPO_NOT_PUBLISHED");
        assert_eq!(
            code(&control, "catalog.list", b""),
            r#"OK [{"label":"Shared","repo":"r1"}]"#
        );
        let (bulk, _server) = serve(&network, LaneRole::Bulk, LaneRole::Bulk);
        assert_eq!(code(&bulk, "objects.fetch", wanted), "REPO_NOT_PUBLISHED");
        control.close();
        bulk.close();
    }

    #[test]
    fn a_bad_peer_row_is_skipped_and_the_network_still_starts() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::initialize(&root.path().join("orchestrator.toml")).unwrap();
        config.network.listen = String::new();
        let catalog = Catalog::open(&config.data_directory).unwrap();
        catalog
            .save_peer("ws://127.0.0.1:9", "broken", [1; 32], None)
            .unwrap();
        rusqlite::Connection::open(config.data_directory.join("folders.sqlite"))
            .unwrap()
            .execute("UPDATE peers SET credential='not hex'", [])
            .unwrap();
        let network = start(&config, catalog, Arc::new(NoFolders), Cancel::new(), None).unwrap();
        let peers = network.peers();
        assert_eq!(peers[0]["installation"], "broken", "{peers}");
        assert_eq!(peers[0]["lanes"], json!([]), "{peers}");
        network.shutdown(Duration::from_secs(1));
    }

    #[test]
    fn lane_backoff_doubles_to_a_minute_with_bounded_jitter() {
        assert_eq!(lane_backoff(1, 0.0), Duration::from_secs(1));
        assert_eq!(lane_backoff(2, 0.0), Duration::from_secs(2));
        assert_eq!(lane_backoff(7, 0.0), LANE_BACKOFF_MAX);
        assert_eq!(lane_backoff(40, 0.0), LANE_BACKOFF_MAX);
        assert_eq!(lane_backoff(1, 0.5), Duration::from_millis(1200));
        assert_eq!(lane_backoff(1, -0.5), Duration::from_millis(800));
        assert_eq!(lane_backoff(1, f64::NAN), Duration::from_secs(1));
        assert_eq!(DIAL_TIMEOUT, crate::model::protocol::frame::DIAL_ATTEMPT);
    }

    #[test]
    fn refusals_map_to_lane_states() {
        let wire = |code: &str| ErrorWire::coded(code, "x");
        for code in AUTH_REFUSALS {
            assert_eq!(refusal(&wire(code)), Refusal::Unauthorized, "{code}");
        }
        for code in ["PEER_NEEDS_UPGRADE", "PEER_PROTOCOL_UNSUPPORTED"] {
            assert_eq!(refusal(&wire(code)), Refusal::Incompatible, "{code}");
        }
        for code in ["WS_CONNECT_FAILED", "CLOCK_INVALID", "TOO_MANY_SESSIONS"] {
            assert_eq!(refusal(&wire(code)), Refusal::Transient, "{code}");
        }
    }

    /// A peer daemon with an invalid clock never starts, so the dial side of
    /// `CLOCK_INVALID` is checked here: the lane backs off before any dial.
    #[test]
    fn a_lane_with_an_invalid_clock_backs_off_without_dialing() {
        fn invalid() -> Result<()> {
            clock::check_ms(1_000_000_000_000)
        }
        // Nothing listens on port 9: a dial would fail with WS_CONNECT_FAILED.
        let target = Target {
            check_clock: invalid,
            endpoint: "ws://127.0.0.1:9".into(),
            remote: "peer-b".into(),
            key: [7; 32],
            local: "peer-a".into(),
            pool: 2,
        };
        let lanes: Vec<Arc<Lane>> = (0..2).map(|index| Lane::new(index, 2)).collect();
        let cancel = Cancel::new();
        let target = Arc::new(target);
        let threads: Vec<_> = lanes
            .iter()
            .map(|lane| {
                let (lane, target, cancel) = (lane.clone(), target.clone(), cancel.clone());
                thread::spawn(move || run_lane(&lane, &target, &cancel))
            })
            .collect();
        let end = Instant::now() + Duration::from_secs(10);
        for lane in &lanes {
            loop {
                let view = lane.view();
                if view["state"] == "Backoff" {
                    let error = view["error"].as_str().unwrap_or_default();
                    assert!(error.contains("CLOCK_INVALID"), "{view}");
                    assert_eq!(view["attempt"], 1, "{view}");
                    break;
                }
                assert!(Instant::now() < end, "lane not in Backoff: {view}");
                thread::sleep(Duration::from_millis(10));
            }
        }
        cancel.cancel();
        for thread in threads {
            thread.join().unwrap();
        }
        assert!(lanes.iter().all(|lane| lane.view()["state"] == "Idle"));
    }

    #[test]
    fn lanes_are_one_control_then_sharded_bulk() {
        let lanes: Vec<Arc<Lane>> = (0..4).map(|index| Lane::new(index, 4)).collect();
        assert_eq!(lanes[0].role, LaneRole::Control);
        assert_eq!(lanes[0].spec, None);
        for (i, lane) in lanes.iter().enumerate().skip(1) {
            assert_eq!(lane.role, LaneRole::Bulk);
            assert_eq!(lane.spec, Some(LaneSpec::new(i as u32 - 1, 3)));
        }
        let view = lanes[1].view();
        assert_eq!(view["state"], "Idle");
        assert_eq!(view["role"], "bulk");
    }
}
