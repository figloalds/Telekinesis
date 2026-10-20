//! Content-v1 bulk transport. One frame retains its byte/job credit through a
//! receiver ACK, so queuing a reply cannot return its budget prematurely.
use super::{inbound, worker_client::SessionWorkerClient};
use crate::{
    content::scheduler::{ContentScheduler, TransferKey, TransferLease},
    model::{
        content::{MAX_NODE_BYTES, ObjectKind},
        content_wire as wire,
        errors::ErrorWire,
        orchestrator::{CallError, Ctx, WorkerEndpoint},
        protocol::LaneRole,
        workspace::DemandPriority,
    },
    paired_sync::SyncRoute,
    session::{self, Conn, Incoming, Priority},
    util::{Cancel, lock::lock},
};
use anyhow::{Result, ensure};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;

const CALL: Duration = Duration::from_secs(5);
const CAPS_FOR: Duration = Duration::from_secs(60);
struct Negotiated {
    instance: String,
    until: Instant,
}
struct Credit {
    legacy: bool,
    started: bool,
    active: bool,
    acknowledged: bool,
    connection: Conn,
    session: String,
    scope: wire::PeerScope,
    token: wire::Token,
    until: Instant,
    _lease: Arc<TransferLease>,
}
type OutKey = (String, u8);

pub struct ContentDaemon {
    pub(crate) cancel: Cancel,
    pub(crate) dispatch: Mutex<Option<Arc<super::content_dispatch::Dispatcher>>>,
    pub scheduler: Arc<ContentScheduler>,
    negotiated: Mutex<BTreeMap<(String, String), Negotiated>>,
    credits: Mutex<BTreeMap<Uuid, Credit>>,
    workers: Mutex<BTreeMap<String, Arc<SessionWorkerClient>>>,
    planners: Mutex<BTreeMap<String, Arc<SessionWorkerClient>>>,
    outgoing: Mutex<BTreeMap<OutKey, Uuid>>,
    out_caps: Mutex<BTreeMap<(String, String), Instant>>,
}

impl ContentDaemon {
    fn new(cancel: Cancel) -> Result<Arc<Self>> {
        let state = Arc::new(Self {
            cancel: cancel.clone(),
            dispatch: Mutex::new(None),
            out_caps: Mutex::new(BTreeMap::new()),
            scheduler: Arc::new(ContentScheduler::default()),
            negotiated: Mutex::new(BTreeMap::new()),
            credits: Mutex::new(BTreeMap::new()),
            workers: Mutex::new(BTreeMap::new()),
            planners: Mutex::new(BTreeMap::new()),
            outgoing: Mutex::new(BTreeMap::new()),
        });
        let weak = Arc::downgrade(&state);
        thread::Builder::new()
            .name("content-credits".into())
            .spawn(move || {
                loop {
                    let Some(state) = weak.upgrade() else {
                        break;
                    };
                    if cancel.is_cancelled() {
                        let credits = std::mem::take(&mut *lock(&state.credits));
                        for (_, credit) in credits {
                            credit.connection.close();
                        }
                        let workers: Vec<_> = lock(&state.workers).values().cloned().collect();
                        for worker in workers {
                            worker.close();
                        }
                        lock(&state.negotiated).clear();
                        break;
                    }
                    state.reap();
                    drop(state);
                    thread::sleep(Duration::from_millis(50));
                }
            })
            .map_err(|e| anyhow::anyhow!("CONTENT_RESOURCE_LIMIT: {e}"))?;
        Ok(state)
    }

    fn reap(&self) {
        let now = Instant::now();
        let expired: Vec<Credit> = {
            let mut credits = lock(&self.credits);
            let ids: Vec<Uuid> = credits
                .iter()
                .filter(|(_, c)| c.until <= now)
                .map(|(id, _)| *id)
                .collect();
            ids.into_iter()
                .filter_map(|id| credits.remove(&id))
                .collect()
        };
        // A timed-out reply may still occupy a socket queue. Fence that
        // session instead of lending the same byte credit to another frame.
        for credit in expired {
            credit.connection.close();
        }
        lock(&self.negotiated).retain(|_, c| c.until > now);
        lock(&self.out_caps).retain(|_, until| *until > now);
    }

    pub fn worker(
        &self,
        endpoint: &WorkerEndpoint,
    ) -> std::result::Result<Arc<SessionWorkerClient>, CallError> {
        Self::cached_worker(&self.workers, endpoint)
    }
    pub(crate) fn planner(
        &self,
        endpoint: &WorkerEndpoint,
    ) -> std::result::Result<Arc<SessionWorkerClient>, CallError> {
        Self::cached_worker(&self.planners, endpoint)
    }
    fn cached_worker(
        cache: &Mutex<BTreeMap<String, Arc<SessionWorkerClient>>>,
        endpoint: &WorkerEndpoint,
    ) -> std::result::Result<Arc<SessionWorkerClient>, CallError> {
        if let Some(client) = lock(cache)
            .get(&endpoint.instance)
            .filter(|c| {
                c.endpoint().address == endpoint.address && c.endpoint().token == endpoint.token
            })
            .cloned()
        {
            return Ok(client);
        }
        // Dedicated content loopback sessions keep legacy bulk handlers from
        // occupying every handler used for foreground content and completion.
        let client = SessionWorkerClient::connect_with(
            endpoint,
            session::ConnOptions {
                max_chunk: wire::FRAME_BYTES,
                data_queue_bytes: 1024 * 1024,
                control_queue_bytes: 1024 * 1024,
                ..Default::default()
            },
        )?;
        let mut workers = lock(cache);
        if let Some(current) = workers.get(&endpoint.instance).filter(|c| {
            c.endpoint().address == endpoint.address && c.endpoint().token == endpoint.token
        }) {
            return Ok(current.clone());
        }
        if workers.len() >= 64
            && let Some(first) = workers.keys().next().cloned()
        {
            workers.remove(&first);
        }
        workers.insert(endpoint.instance.clone(), client.clone());
        Ok(client)
    }

    /// Four outbound frames leave global service for inbound replies. At most
    /// one outgoing frame per peer/class prevents symmetric peers from filling
    /// both of a class's per-peer slots while waiting for each other's reads.
    pub fn outgoing(self: &Arc<Self>, peer: &str, priority: DemandPriority) -> Option<Outgoing> {
        let key = (peer.to_owned(), rank(priority));
        let mut active = lock(&self.outgoing);
        let mut present = [false; 3];
        for (_, class) in active.keys() {
            present[*class as usize] = true;
        }
        let reserved = (0..3)
            .filter(|class| *class != rank(priority) as usize && !present[*class])
            .count();
        if active.len() >= 4 - reserved || active.contains_key(&key) {
            return None;
        }
        let owner = Uuid::new_v4();
        active.insert(key.clone(), owner);
        Some(Outgoing {
            state: Arc::downgrade(self),
            key,
            owner,
        })
    }

    pub fn pending_frames(&self) -> usize {
        lock(&self.credits).len()
    }

    pub(crate) fn retain_credit(
        &self,
        peer: &session::AcceptedPeer,
        scope: wire::PeerScope,
        connection: Conn,
        lease: TransferLease,
    ) -> wire::Token {
        let token = wire::Token {
            owner: lease.token().owner,
            generation: lease.token().generation,
        };
        lock(&self.credits).insert(
            token.owner,
            Credit {
                legacy: true,
                started: false,
                active: false,
                acknowledged: false,
                connection,
                session: peer.session.clone(),
                scope,
                token,
                until: Instant::now() + Duration::from_millis(wire::OFFER_MS),
                _lease: Arc::new(lease),
            },
        );
        token
    }

    pub(crate) fn start_legacy(
        self: &Arc<Self>,
        peer: &session::AcceptedPeer,
        scope: &wire::PeerScope,
        token: wire::Token,
        object: crate::model::content::ObjectKey,
    ) -> Result<LegacyCredit> {
        let mut credits = lock(&self.credits);
        let credit = credits
            .get_mut(&token.owner)
            .ok_or_else(|| anyhow::anyhow!("CONTENT_CANCELLED"))?;
        ensure!(
            credit.legacy
                && credit.token == token
                && credit.session == peer.session
                && credit.scope == *scope
                && credit._lease.key().object == object
                && !credit.started
                && credit.until > Instant::now(),
            "UNAUTHORIZED_OBJECT"
        );
        credit.started = true;
        credit.active = true;
        Ok(LegacyCredit {
            state: self.clone(),
            owner: token.owner,
            _lease: credit._lease.clone(),
        })
    }
}

pub(crate) struct LegacyCredit {
    state: Arc<ContentDaemon>,
    owner: Uuid,
    // Closing/expiring a session removes its map entry, but active worker
    // or socket work still owns the budget until its handler unwinds.
    _lease: Arc<TransferLease>,
}
impl Drop for LegacyCredit {
    fn drop(&mut self) {
        let mut credits = lock(&self.state.credits);
        if let Some(credit) = credits.get_mut(&self.owner) {
            credit.active = false;
            if credit.acknowledged {
                credits.remove(&self.owner);
            }
        }
    }
}

pub struct Outgoing {
    state: Weak<ContentDaemon>,
    key: OutKey,
    owner: Uuid,
}
impl Drop for Outgoing {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            let mut active = lock(&state.outgoing);
            if active.get(&self.key) == Some(&self.owner) {
                active.remove(&self.key);
            }
        }
    }
}
fn rank(priority: DemandPriority) -> u8 {
    match priority {
        DemandPriority::Foreground => 0,
        DemandPriority::Structural => 1,
        DemandPriority::Background => 2,
    }
}

pub fn daemon(ctx: &Arc<Ctx>) -> Result<Arc<ContentDaemon>> {
    let extras = super::ctx::extras(ctx);
    let mut state = lock(&extras.content);
    if let Some(state) = state.as_ref() {
        return Ok(state.clone());
    }
    let created = ContentDaemon::new(ctx.shutdown.child())?;
    *state = Some(created.clone());
    Ok(created)
}

pub fn session_closed(ctx: &Arc<Ctx>, session: &str) {
    let state = lock(&super::ctx::extras(ctx).content).clone();
    if let Some(state) = state {
        lock(&state.credits).retain(|_, c| c.session != session);
        lock(&state.negotiated).retain(|(s, _), _| s != session);
    }
}

fn encode(value: &impl Serialize) -> std::result::Result<Value, CallError> {
    serde_json::to_value(value)
        .map_err(|e| CallError::Remote(ErrorWire::coded("INVALID_REQUEST", e)))
}
fn parse<T: DeserializeOwned>(header: Value) -> std::result::Result<T, CallError> {
    serde_json::from_value(header)
        .map_err(|e| CallError::Remote(ErrorWire::coded("INVALID_REPLY", e)))
}
pub fn worker_call<T: DeserializeOwned>(
    client: &SessionWorkerClient,
    op: &str,
    header: &impl Serialize,
    body: Vec<u8>,
) -> std::result::Result<(T, Vec<u8>), CallError> {
    worker_call_with_deadline(client, op, header, body, CALL)
}

pub(crate) fn worker_call_with_deadline<T: DeserializeOwned>(
    client: &SessionWorkerClient,
    op: &str,
    header: &impl Serialize,
    body: Vec<u8>,
    deadline: Duration,
) -> std::result::Result<(T, Vec<u8>), CallError> {
    let priority = if body.is_empty() {
        Priority::Control
    } else {
        Priority::Data
    };
    let reply = client.request(op, encode(header)?, body, priority, deadline)?;
    Ok((parse(reply.header)?, reply.body))
}

pub(crate) fn bind(
    ctx: &Arc<Ctx>,
    peer: &session::AcceptedPeer,
    scope: &wire::PeerScope,
) -> std::result::Result<(SyncRoute, WorkerEndpoint), ErrorWire> {
    if peer.role != LaneRole::Bulk {
        return Err(ErrorWire::coded(
            "BULK_ON_CONTROL_LANE",
            "content frames require bulk",
        ));
    }
    if !scope.lane.is_valid() || peer.lane != Some(scope.lane) {
        return Err(ErrorWire::coded("BULK_LANE_MISMATCH", "content frame lane"));
    }
    let served = inbound::resolve(ctx, peer, &scope.repo, &scope.sender)?;
    if scope.receiver != served.route.local_replica {
        return Err(ErrorWire::coded(
            "LOCAL_REPLICA_BINDING_MISMATCH",
            "content frame receiver",
        ));
    }
    let endpoint = served
        .view
        .worker
        .clone()
        .ok_or_else(|| ErrorWire::coded("REMOTE_WORKER_STARTING", "content worker"))?;
    Ok((served.route, endpoint))
}

fn request<T: DeserializeOwned>(header: &Value, body: &[u8]) -> Result<T> {
    ensure!(body.is_empty(), "INVALID_REQUEST");
    serde_json::from_value(header.clone()).map_err(|e| anyhow::anyhow!("INVALID_REQUEST: {e}"))
}

pub fn handle(ctx: &Arc<Ctx>, peer: &session::AcceptedPeer, req: Incoming) {
    if matches!(req.op.as_str(), wire::LEGACY_PULL | wire::LEGACY_BEGIN) {
        super::legacy_demand::serve(ctx, peer, req);
        return;
    }
    let Incoming {
        op,
        header,
        body,
        responder,
        ..
    } = req;
    let result = (|| -> Result<(Value, Vec<u8>)> {
        let state = daemon(ctx)?;
        match op.as_str() {
            wire::CAPS => {
                let req: wire::PeerCaps = request(&header, &body)?;
                ensure!(req.version == wire::VERSION, "PEER_NEEDS_UPGRADE");
                let (route, endpoint) = bind(ctx, peer, &req.scope).map_err(anyhow::Error::new)?;
                let client = state.worker(&endpoint).map_err(anyhow::Error::new)?;
                let (caps, _): (wire::Capabilities, _) = worker_call(
                    &client,
                    wire::CAPS,
                    &wire::WorkerCaps {
                        route,
                        version: wire::VERSION,
                    },
                    vec![],
                )
                .map_err(anyhow::Error::new)?;
                ensure!(caps.supports(), "PEER_NEEDS_UPGRADE");
                let mut negotiated = lock(&state.negotiated);
                if negotiated.len() >= 4096 {
                    negotiated.retain(|_, c| c.until > Instant::now());
                    if negotiated.len() >= 4096
                        && let Some(first) = negotiated.keys().next().cloned()
                    {
                        negotiated.remove(&first);
                    }
                }
                if caps.active {
                    negotiated.insert(
                        (peer.session.clone(), req.scope.repo),
                        Negotiated {
                            instance: endpoint.instance,
                            until: Instant::now() + CAPS_FOR,
                        },
                    );
                }
                Ok((serde_json::to_value(caps)?, vec![]))
            }
            wire::READ => {
                let req: wire::PeerRead = request(&header, &body)?;
                let (route, endpoint) = bind(ctx, peer, &req.scope).map_err(anyhow::Error::new)?;
                let authorized = lock(&state.negotiated)
                    .get(&(peer.session.clone(), req.scope.repo.clone()))
                    .is_some_and(|n| n.until > Instant::now() && n.instance == endpoint.instance);
                ensure!(authorized, "PEER_NEEDS_UPGRADE");
                ensure!(
                    req.scope
                        .lane
                        .owns(&req.position.edge.key.digest.to_string()),
                    "OBJECT_ON_WRONG_LANE"
                );
                let cost = if req.position.edge.key.kind == ObjectKind::Manifest {
                    MAX_NODE_BYTES
                } else {
                    usize::try_from(req.position.edge.len).unwrap_or(usize::MAX)
                };
                ensure!(cost <= wire::FRAME_BYTES, "CONTENT_BUFFER_LIMIT");
                let provider = Uuid::parse_str(&endpoint.instance)
                    .map_err(|e| anyhow::anyhow!("INVALID_REQUEST: {e}"))?;
                let lease = state
                    .scheduler
                    .try_acquire(
                        TransferKey {
                            provider,
                            object: req.position.edge.key,
                        },
                        &req.scope.repo,
                        &peer.installation,
                        req.priority,
                        1024 * 1024,
                    )?
                    .ok_or_else(|| anyhow::anyhow!("CONTENT_RESOURCE_LIMIT"))?;
                let token = wire::Token {
                    owner: lease.token().owner,
                    generation: lease.token().generation,
                };
                let client = state.worker(&endpoint).map_err(anyhow::Error::new)?;
                let worker_reply = client
                    .request_cancellable(
                        wire::READ,
                        encode(&wire::WorkerRead {
                            route,
                            position: req.position,
                            priority: req.priority,
                        })
                        .map_err(anyhow::Error::new)?,
                        vec![],
                        CALL,
                        &|| responder.cancelled() || state.cancel.is_cancelled(),
                    )
                    .map_err(anyhow::Error::new)?;
                let (mut frame, bytes): (wire::Frame, _) = (
                    parse(worker_reply.header).map_err(anyhow::Error::new)?,
                    worker_reply.body,
                );
                ensure!(
                    bytes.len() == frame.bytes && bytes.len() <= wire::FRAME_BYTES,
                    "INVALID_REPLY"
                );
                ensure!(!responder.cancelled(), "CONTENT_CANCELLED");
                frame.token = Some(token);
                lock(&state.credits).insert(
                    token.owner,
                    Credit {
                        legacy: false,
                        started: false,
                        active: false,
                        acknowledged: false,
                        connection: responder.connection(),
                        session: peer.session.clone(),
                        scope: req.scope,
                        token,
                        until: Instant::now() + Duration::from_millis(wire::OFFER_MS),
                        _lease: Arc::new(lease),
                    },
                );
                Ok((serde_json::to_value(frame)?, bytes))
            }
            wire::ACK => {
                let req: wire::PeerAck = request(&header, &body)?;
                ensure!(
                    peer.role == LaneRole::Bulk && peer.lane == Some(req.scope.lane),
                    "BULK_LANE_MISMATCH"
                );
                let mut credits = lock(&state.credits);
                if let Some(credit) = credits.get_mut(&req.token.owner) {
                    ensure!(
                        credit.token == req.token
                            && credit.session == peer.session
                            && credit.scope.repo == req.scope.repo
                            && credit.scope.sender == req.scope.sender
                            && credit.scope.receiver == req.scope.receiver
                            && credit.scope.lane == req.scope.lane,
                        "UNAUTHORIZED_OBJECT"
                    );
                    if credit.active {
                        credit.acknowledged = true;
                    } else {
                        credits.remove(&req.token.owner);
                    }
                }
                Ok((json!({"released":true}), vec![]))
            }
            _ => anyhow::bail!("UNSUPPORTED_OPERATION"),
        }
    })();
    match result {
        Ok((header, body)) => responder.reply(header, body),
        Err(e) => responder.error(ErrorWire::from_error(&e)),
    }
}

/// A worker offer is independently owned before any remote network wait.
/// Its destructor returns only that token, outside scheduler/dispatcher locks.
pub(crate) struct LocalOffer {
    pub offer: wire::Offer,
    pub client: Arc<SessionWorkerClient>,
    pub finish: wire::Finish,
    state: Arc<ContentDaemon>,
    turn: Option<Outgoing>,
    armed: bool,
}
impl LocalOffer {
    fn release(&mut self, failed_source: bool) {
        if !self.armed {
            return;
        }
        self.armed = false;
        if self.state.cancel.is_cancelled() && !self.client.is_open() {
            return;
        }
        if let Ok(header) = encode(&wire::Release {
            transfer: self.finish.clone(),
            failed_source,
        }) {
            let _ = self.client.request(
                wire::RELEASE,
                header,
                vec![],
                Priority::Control,
                Duration::from_millis(250),
            );
        }
    }
}
impl Drop for LocalOffer {
    fn drop(&mut self) {
        drop(self.turn.take());
        self.release(false);
    }
}

pub(crate) fn mode_priority(mode: wire::PrepareMode) -> DemandPriority {
    match mode {
        wire::PrepareMode::Structural => DemandPriority::Structural,
        wire::PrepareMode::Background => DemandPriority::Background,
        _ => DemandPriority::Foreground,
    }
}

pub(crate) enum Preparation {
    Ready(Box<LocalOffer>),
    Progress,
    Idle,
    Capacity,
}
pub(crate) fn prepare_offer(
    state: &Arc<ContentDaemon>,
    client: Arc<SessionWorkerClient>,
    route: SyncRoute,
    owner: Uuid,
    mode: wire::PrepareMode,
    lane: crate::model::protocol::LaneSpec,
) -> std::result::Result<Preparation, CallError> {
    if state.cancel.is_cancelled() {
        return Err(CallError::Cancelled);
    }
    if mode == wire::PrepareMode::Mixed {
        return Err(CallError::Remote(ErrorWire::coded(
            "INVALID_REQUEST",
            "one content priority class required",
        )));
    }
    let Some(turn) = state.outgoing(&route.peer, mode_priority(mode)) else {
        return Ok(Preparation::Capacity);
    };
    let (prepared, _): (wire::Prepared, _) = worker_call(
        &client,
        wire::PREPARE,
        &wire::Prepare {
            route: route.clone(),
            owner,
            lane,
            mode,
        },
        vec![],
    )?;
    let Some(offer) = prepared.offer else {
        return Ok(if prepared.progress {
            Preparation::Progress
        } else {
            Preparation::Idle
        });
    };
    let local = LocalOffer {
        finish: wire::Finish {
            route,
            owner,
            token: offer.token,
        },
        offer,
        client,
        state: state.clone(),
        turn: Some(turn),
        armed: true,
    };
    if local.offer.priority != mode_priority(mode) {
        return Err(CallError::Remote(ErrorWire::coded(
            "INVALID_REPLY",
            "content offer priority",
        )));
    }
    Ok(Preparation::Ready(Box::new(local)))
}

pub(crate) struct FrameTask {
    state: Arc<ContentDaemon>,
    conn: Conn,
    scope: wire::PeerScope,
    cancel: Cancel,
    budget: Option<TransferLease>,
    local: LocalOffer,
}
impl Drop for FrameTask {
    fn drop(&mut self) {
        drop(self.budget.take());
    }
}
impl FrameTask {
    pub(crate) fn bind(
        state: &Arc<ContentDaemon>,
        local: LocalOffer,
        conn: Conn,
        scope: wire::PeerScope,
        cancel: Cancel,
    ) -> std::result::Result<Option<Self>, CallError> {
        validate_route(&conn, &local.finish.route, &scope)?;
        if cancel.is_cancelled() || state.cancel.is_cancelled() {
            return Err(CallError::Cancelled);
        }
        let provider = Uuid::parse_str(&local.client.endpoint().instance)
            .map_err(|e| CallError::Remote(ErrorWire::coded("INVALID_REPLY", e)))?;
        let cost = if local.offer.edge.key.kind == ObjectKind::Manifest {
            MAX_NODE_BYTES
        } else {
            usize::try_from(local.offer.edge.len).unwrap_or(usize::MAX)
        };
        if cost > wire::FRAME_BYTES || !scope.lane.owns(&local.offer.edge.key.digest.to_string()) {
            return Err(CallError::Remote(ErrorWire::coded(
                "INVALID_REPLY",
                "content offer bounds/lane",
            )));
        }
        let budget = state
            .scheduler
            .try_acquire(
                TransferKey {
                    provider,
                    object: local.offer.edge.key,
                },
                &scope.repo,
                &local.finish.route.peer,
                local.offer.priority,
                1024 * 1024,
            )
            .map_err(|e| CallError::Remote(ErrorWire::from_error(&e)))?;
        Ok(budget.map(|budget| Self {
            state: state.clone(),
            conn,
            scope,
            cancel,
            budget: Some(budget),
            local,
        }))
    }
    fn check(&self) -> std::result::Result<(), CallError> {
        if self.cancel.is_cancelled() || self.state.cancel.is_cancelled() {
            Err(CallError::Cancelled)
        } else {
            Ok(())
        }
    }
    fn negotiate(&self) -> std::result::Result<(), CallError> {
        let key = (self.conn.peer().session.clone(), self.scope.repo.clone());
        if lock(&self.state.out_caps)
            .get(&key)
            .is_some_and(|until| *until > Instant::now())
        {
            return Ok(());
        }
        let reply = self.conn.call_cancellable(
            wire::CAPS,
            encode(&wire::PeerCaps {
                scope: self.scope.clone(),
                version: wire::VERSION,
            })?,
            vec![],
            Priority::Control,
            CALL,
            &|| self.cancel.is_cancelled() || self.state.cancel.is_cancelled(),
        )?;
        let caps: wire::Capabilities = parse(reply.header)?;
        if !caps.active || !caps.supports() {
            return Err(CallError::Remote(ErrorWire::coded(
                "PEER_NEEDS_UPGRADE",
                "remote content worker",
            )));
        }
        let mut cache = lock(&self.state.out_caps);
        if cache.len() >= 4096
            && let Some(first) = cache.keys().next().cloned()
        {
            cache.remove(&first);
        }
        cache.insert(key, Instant::now() + CAPS_FOR);
        Ok(())
    }
    pub(crate) fn perform(mut self) -> std::result::Result<bool, CallError> {
        self.check()?;
        self.negotiate()?;
        self.check()?;
        let read = self.conn.call_cancellable(
            wire::READ,
            encode(&wire::PeerRead {
                scope: self.scope.clone(),
                position: wire::Position {
                    reference: self.local.offer.reference.clone(),
                    edge: self.local.offer.edge.clone(),
                },
                priority: self.local.offer.priority,
            })?,
            vec![],
            Priority::Control,
            CALL,
            &|| self.cancel.is_cancelled() || self.state.cancel.is_cancelled(),
        );
        let reply = match read {
            Ok(reply) => reply,
            Err(e) => {
                if e.code() == "PEER_NEEDS_UPGRADE" {
                    lock(&self.state.out_caps)
                        .remove(&(self.conn.peer().session.clone(), self.scope.repo.clone()));
                }
                if matches!(
                    e.code().as_str(),
                    "CONTENT_UNAVAILABLE"
                        | "CORRUPT_CONTENT"
                        | "UNAUTHORIZED_OBJECT"
                        | "NOT_AVAILABLE"
                        | "NOT_PERMITTED"
                ) {
                    self.local.release(true);
                }
                return Err(e);
            }
        };
        let frame: wire::Frame = match parse(reply.header) {
            Ok(frame) => frame,
            Err(e) => {
                self.local.release(true);
                return Err(e);
            }
        };
        let ack = |conn: &Conn| {
            if let Some(token) = frame.token {
                let _ = conn.call(
                    wire::ACK,
                    encode(&wire::PeerAck {
                        scope: self.scope.clone(),
                        token,
                    })?,
                    vec![],
                    Priority::Control,
                    Duration::from_millis(250),
                )?;
            }
            Ok::<_, CallError>(())
        };
        if frame.bytes != reply.body.len()
            || frame.bytes > wire::FRAME_BYTES
            || frame.token.is_none()
            || frame.generation == 0
        {
            let _ = ack(&self.conn);
            self.local.release(true);
            return Err(CallError::Remote(ErrorWire::coded(
                "INVALID_REPLY",
                "content frame length/token",
            )));
        }
        if let Err(e) = self.check() {
            let _ = ack(&self.conn);
            return Err(e);
        }
        let data_client = self.state.worker(self.local.client.endpoint())?;
        // Cancellation abandons this request without closing a shared worker
        // session. A completion already executing remains protected by its
        // exact owner/generation and durable receipt protocol.
        let installed = data_client.request_cancellable(
            wire::COMPLETE,
            encode(&self.local.finish)?,
            reply.body,
            CALL,
            &|| self.cancel.is_cancelled() || self.state.cancel.is_cancelled(),
        );
        let _ = ack(&self.conn);
        installed?;
        self.local.armed = false;
        Ok(true)
    }
}

pub(crate) fn validate_route(
    conn: &Conn,
    route: &SyncRoute,
    scope: &wire::PeerScope,
) -> std::result::Result<(), CallError> {
    let error = |code, detail| CallError::Remote(ErrorWire::coded(code, detail));
    if conn.peer().role != LaneRole::Bulk {
        return Err(error("BULK_ON_CONTROL_LANE", "content pull"));
    }
    if conn.peer().lane != Some(scope.lane) {
        return Err(error("BULK_LANE_MISMATCH", "content pull"));
    }
    if conn.peer().installation != route.peer {
        return Err(error("PEER_INSTALLATION_CHANGED", "content pull"));
    }
    if scope.repo != route.repo
        || scope.sender != route.local_replica
        || scope.receiver != route.remote_replica
    {
        return Err(error("INVALID_REQUEST", "content route"));
    }
    Ok(())
}

/// An explicit one-class exchange; continuous dispatch uses the same separated
/// offer and frame phases, with bounded preparation and network worker pools.
pub fn pull_one(
    ctx: &Arc<Ctx>,
    conn: &Conn,
    client: &Arc<SessionWorkerClient>,
    route: &SyncRoute,
    scope: &wire::PeerScope,
    owner: Uuid,
    mode: wire::PrepareMode,
) -> std::result::Result<bool, CallError> {
    validate_route(conn, route, scope)?;
    if mode == wire::PrepareMode::Mixed {
        return Err(CallError::Remote(ErrorWire::coded(
            "INVALID_REQUEST",
            "one content priority class required",
        )));
    }
    let state = daemon(ctx).map_err(|e| CallError::Remote(ErrorWire::from_error(&e)))?;
    let (caps, _): (wire::Capabilities, _) = worker_call(
        client,
        wire::CAPS,
        &wire::WorkerCaps {
            route: route.clone(),
            version: wire::VERSION,
        },
        vec![],
    )?;
    if !caps.active {
        return Ok(false);
    }
    if !caps.supports() {
        return Err(CallError::Remote(ErrorWire::coded(
            "PEER_NEEDS_UPGRADE",
            "local content worker",
        )));
    }
    let Preparation::Ready(local) = prepare_offer(
        &state,
        client.clone(),
        route.clone(),
        owner,
        mode,
        scope.lane,
    )?
    else {
        return Ok(false);
    };
    let Some(task) = FrameTask::bind(
        &state,
        *local,
        conn.clone(),
        scope.clone(),
        ctx.shutdown.child(),
    )?
    else {
        return Ok(false);
    };
    task.perform()
}
