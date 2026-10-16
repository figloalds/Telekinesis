//! Owned by WP2.8 and WP3.3 (WP4.5: the interim path removed, leases given
//! up at the worker) — inbound `objects.*` (REVAMP_PLAN §3.8, §6.4).
//!
//! Requests on an authenticated **bulk** session; the acceptor's half of
//! objects v3 (the dialer's is [`super::objects`], which also holds the
//! shared data plane: [`Sink`], the allotment rule, the lease ids). Every
//! error is a Remote error: the session stays usable (invariant 17). Each
//! request runs on an exchange thread of its own (`inbound`), so a slow one
//! never holds the session's handlers.
//!
//! | op | handling |
//! |---|---|
//! | `objects.pull` | our worker's `objects.read` (≤ 4 in flight, ≤ 1 MiB each) → CHUNK `{hash, offset, total, last}` stream → `ObjectsResult` |
//! | `objects.wants` | our worker's `objects.want`, leased to [`push_lease`] (peer, lane), excluding what that dialer cannot serve now → `ObjectsWantsReply` |
//! | `objects.push` | the CHUNK stream, checked against each object's share → our worker's `objects.write` (≤ 4 in flight, one per object, under the dialer lane's push lease) → `ObjectsResult` |
//! | `objects.report` | for an object of the lane that we served this peer through its end lately: our worker's `objects.report` (it re-verifies and, if corrupt, quarantines its copy) → `{}` |
//!
//! Any other `objects.*` op is answered `UNSUPPORTED_OPERATION`, among them
//! the Wave 2 interim `objects.exchange` (removed by WP4.5, CCR-WP3.3-2: every
//! peer of this protocol serves objects v3).
//!
//! **Checks, in order** (none of the refusals reaches a worker):
//! 1. the session is a bulk session (`BULK_ON_CONTROL_LANE`; the inbound
//!    router checks this first too);
//! 2. the request parses (a v3 request that does not is answered
//!    `INVALID_REQUEST`, CCR-WP3.3-3 applied by WP3.INT) and names the session's own
//!    lane, the one the handshake authenticated (`BULK_LANE_MISMATCH`), so
//!    one lane never carries another lane's objects (invariant 19); items
//!    the lane does not own are answered `OBJECT_ON_WRONG_LANE` one by one;
//! 3. the repository is managed (`REPOSITORY_NOT_MANAGED`), its sync is not
//!    paused (`FOLDER_SYNC_PAUSED`) and its worker is Running
//!    (`REMOTE_WORKER_STARTING`);
//! 4. the route is `{peer: authenticated installation, repo, local_replica:
//!    folder device, remote_replica: request.sender}` (invariant 15), and
//!    `request.receiver` is this folder (`LOCAL_REPLICA_BINDING_MISMATCH`).
//!    The worker checks the rest (authorization triad on reads, wanted and
//!    leased on writes, invariant 13).
//!
//! A folder whose worker cannot be reached over a loopback session is, to
//! the peer, a folder that is starting (`REMOTE_WORKER_STARTING`).
//!
//! **Pull.** An item whose size the dialer does not know (size 0, not the
//! empty object) gets one empty chunk carrying our total, and the result
//! `Skipped`: the dialer pulls it next with that size (WP3.INT re-review).
//! The items cover the bytes of the allotment rule
//! ([`super::objects::allot`]): an item that fails sends nothing, and its
//! share is not given to later items. Per item the result is `Completed`
//! (sent through its end), `Progressed{durable_offset}` (sent up to there,
//! the budget ended), `Skipped` (no bytes), or the read's failure
//! (`NotAvailable`, `NotPermitted`, `SizeMismatch`, …). A PROGRESS goes to
//! the dialer every [`KEEPALIVE`] while our worker reads.
//!
//! **Push.** Each object of the push has its share of the push budget (the
//! allotment rule, 16 MiB): a chunk of an object without a share, outside
//! its share, not contiguous with its last chunk, or of another size than
//! the push declared ends the push `PROTOCOL_VIOLATION` (invariant 22), so
//! at most the budget arrives, in order. Bytes are written as they arrive
//! (what the dialer sent of an object is written once it moves on to the
//! next one, whole write or not); the stream is read no faster than our
//! worker writes (at most [`PUSH_BUFFERED_BYTES`] wait that the writers
//! take without more input), so the session holds the dialer back. A
//! PROGRESS follows every write, so the dialer's idle deadline measures our
//! progress, not its queue. Objects of our offer that failed here (for
//! example `CorruptAtSource`) back off for that peer: `objects.wants` does
//! not list them until then. A push may list any hash: one we never offered
//! this dialer (not wanted: `UNSOLICITED_OBJECT`; not this lane's:
//! `OBJECT_ON_WRONG_LANE`) is answered in the result alone, with neither a
//! backoff nor an `ObjectError` (WP3.INT re-review). A write call that fails as a whole (our worker timed
//! out, went away, refused or erred for the call) fails the push with that
//! error, not its objects: no per-object backoff or `ObjectError` (§2.3).
//!
//! **Leases given up** ([`super::objects`] module docs; CCR-WP3.3-4,
//! WP4.5). Our worker leases what `objects.wants` returns to the dialer's
//! lane ([`push_lease`]). What we offered (the *offer*: each object, and
//! whether a push of the whole offer would give it a share) is compared with
//! what the dialer then does on the same session: an object with a share it
//! did not send (its push ended normally without a byte of it, or did not
//! list it, or no push came before its next `objects.wants`) is one it
//! cannot or will not serve. It backs off for that peer (no `ObjectError`:
//! not serving is no error here), and its lease is given up at our worker
//! (`objects.release`), so the next `objects.wants` of another dialer is
//! offered it at once; that dialer's own wants exclude it while it backs
//! off. An object of our offer whose push failed here, or that the dialer
//! abandoned (its push failed or ended early, its session went away), is
//! given up the same way (without a backoff when the object did not fail).
//!
//! **Report.** A report makes our worker re-read a whole object, so it is
//! taken only for an object of the session's lane (`OBJECT_ON_WRONG_LANE`)
//! that a pull of ours sent this peer through its end in the last hour
//! (`UNSOLICITED_OBJECT`; the record is bounded).
//!
//! **Status.** `ExchangeStarted`/`ExchangeFinished{direction: inbound}` for
//! every pull and push served (bytes; `objects_down`: objects a push
//! completed here; `objects_up` is 0, only the dialer knows what it
//! installed); `ObjectError` for objects a push failed here, `Resolved` for
//! every source's errors of an object a push installed.
use super::{
    inbound::{
        Finished, deadline_error, invalid_request, peer_error, record_finished, resolve,
        status_lane, worker_deadline,
    },
    objects::{
        KEEPALIVE, ObjectKey, PUSH_BUDGET_BYTES, PUSH_BUFFERED_BYTES, Sink, SinkEvent, SinkOptions,
        SinkResult, allot, blocked_objects, covered, is_failure, object_blocked, object_failed,
        object_installed, object_key, object_unserved, outcome_error, push_lease, read_outcome,
        read_ranges, release_leases, size_unknown, want_excluding, wire_pieces,
    },
    worker_client::{self, ReadFlow, SessionWorkerClient},
};
use crate::{
    model::{
        errors::{ErrorWire, Scope},
        orchestrator::{CallError, Ctx},
        protocol::{
            self, LaneSpec, deadlines,
            frame::ChunkHeader,
            objects::{
                INFLIGHT_READS, MAX_RELEASE_ITEMS, MAX_WANT_ITEMS, ObjectOutcome, ObjectResult,
                ObjectsPull, ObjectsPush, ObjectsReport, ObjectsResult, ObjectsWantsReply,
                ObjectsWantsRequest, PULL_BUDGET_BYTES, WantHeader, WantItem,
            },
            ops,
        },
        status::{Direction, ObjectSource, StatusEvent},
    },
    paired_sync::SyncRoute,
    session::{self, Responder},
    util::{cancel::Cancel, lock::lock},
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet, VecDeque, hash_map::RandomState},
    hash::BuildHasher,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
        mpsc::RecvTimeoutError,
    },
    thread,
    time::{Duration, Instant},
};

/// A JSON reply above this many bytes goes in the body (the response
/// header bound is 64 KiB, framing included).
const HEADER_REPLY_MAX: usize = protocol::frame::MAX_HEADER - 1024;
/// How often a waiting exchange looks at its request (cancelled, chunks).
const POLL: Duration = Duration::from_millis(100);

pub fn handle(ctx: &Arc<Ctx>, peer: &session::AcceptedPeer, req: session::Incoming) {
    if peer.role != protocol::LaneRole::Bulk {
        let error = ErrorWire::coded(
            "BULK_ON_CONTROL_LANE",
            format!(
                "{} needs a bulk session; this is a {} session",
                req.op,
                peer.role.as_str()
            ),
        );
        req.responder.error(error);
        return;
    }
    match req.op.as_str() {
        ops::OBJECTS_PULL => pull(ctx, peer, req),
        ops::OBJECTS_WANTS => wants(ctx, peer, req),
        ops::OBJECTS_PUSH => push(ctx, peer, req),
        ops::OBJECTS_REPORT => report(ctx, peer, req),
        _ => {
            let session::Incoming { op, responder, .. } = req;
            responder.error(ErrorWire::coded(
                "UNSUPPORTED_OPERATION",
                format!("{op} is not an objects operation of tkfs peer protocol 3"),
            ));
        }
    }
}

// ---------------------------------------------------------------------------
// Objects v3

/// A request of `op`: its JSON body, or (no body) its header. One that does
/// not parse is not a v3 request this acceptor serves (module docs).
fn request_of<T: DeserializeOwned>(op: &str, header: &Value, body: &[u8]) -> Result<T, ErrorWire> {
    let parsed = if body.is_empty() {
        serde_json::from_value(header.clone())
    } else {
        serde_json::from_slice(body)
    };
    // CCR-WP3.3-3 (applied by WP3.INT): a malformed objects v3 request is
    // an invalid request, never "not served".
    parsed.map_err(|error| invalid_request(op, &error))
}

/// The JSON reply of an op: the header when it fits, else the body.
fn reply_json(responder: Responder, value: &impl Serialize) {
    match serde_json::to_vec(value) {
        Ok(encoded) if encoded.len() <= HEADER_REPLY_MAX => {
            match serde_json::from_slice::<Value>(&encoded) {
                Ok(header) => responder.reply(header, Vec::new()),
                Err(error) => responder.error(ErrorWire::coded("UNKNOWN_ERROR", error)),
            }
        }
        Ok(encoded) => responder.reply(json!({ "in_body": true }), encoded),
        Err(error) => responder.error(ErrorWire::coded("UNKNOWN_ERROR", error)),
    }
}

/// A folder that serves this objects request: the route bound to the
/// authenticated peer, the folder's state and its worker's loopback session.
struct Bound {
    state: String,
    route: SyncRoute,
    lane: LaneSpec,
    client: Arc<SessionWorkerClient>,
}

/// Checks 2–4 of the module docs.
fn bind(
    ctx: &Ctx,
    peer: &session::AcceptedPeer,
    lane: LaneSpec,
    repo: &str,
    sender: &str,
    receiver: &str,
) -> Result<Bound, ErrorWire> {
    let Some(session_lane) = peer.lane.filter(|session| *session == lane) else {
        return Err(ErrorWire::coded(
            "BULK_LANE_MISMATCH",
            format!(
                "the request is for lane {}/{}; this session is lane {}",
                lane.index,
                lane.count,
                peer.lane.map_or_else(
                    || "none".to_owned(),
                    |lane| format!("{}/{}", lane.index, lane.count)
                )
            ),
        ));
    };
    let served = resolve(ctx, peer, repo, sender)?;
    if receiver != served.route.local_replica {
        return Err(ErrorWire::coded(
            "LOCAL_REPLICA_BINDING_MISMATCH",
            format!(
                "the request is for replica {receiver:?}; this folder is replica {}",
                served.route.local_replica
            ),
        ));
    }
    let Some(endpoint) = served.view.worker.as_ref() else {
        return Err(ErrorWire::coded(
            "REMOTE_WORKER_STARTING",
            format!(
                "folder {} has no worker endpoint yet",
                served.view.record.label
            ),
        ));
    };
    let client = worker_client::session_for(endpoint).map_err(|error| peer_error(&error))?;
    Ok(Bound {
        state: served.view.record.state.clone(),
        route: served.route,
        lane: session_lane,
        client,
    })
}

/// The answer to the peer for a failed worker call of a v3 op.
fn worker_failure(error: &CallError) -> ErrorWire {
    peer_error(error)
}

fn started(ctx: &Ctx, peer: &session::AcceptedPeer, repo: &str, op: &str) {
    ctx.status.record(StatusEvent::ExchangeStarted {
        installation: peer.installation.clone(),
        repo: repo.to_owned(),
        lane: status_lane(peer),
        op: op.to_owned(),
        at_ms: ctx.clock.now_ms(),
        direction: Direction::Inbound,
    });
}

/// Sends a PROGRESS every [`KEEPALIVE`] (and, with `wrote`, whenever it
/// moves) until `done` fires; it returns as soon as it does, so the
/// exchange's reply never waits for it.
fn keepalive(responder: &Responder, done: &Cancel, wrote: Option<&AtomicU64>) {
    let mut last = Instant::now();
    let mut seen = 0;
    while !done.sleep(POLL) && !responder.cancelled() {
        let now = wrote.map_or(0, |wrote| wrote.load(Ordering::Relaxed));
        if now != seen || last.elapsed() >= KEEPALIVE {
            seen = now;
            last = Instant::now();
            responder.progress();
        }
    }
}

/// `objects.pull` (module docs, "Pull").
fn pull(ctx: &Arc<Ctx>, peer: &session::AcceptedPeer, req: session::Incoming) {
    let began = Instant::now();
    let op = ops::OBJECTS_PULL;
    let session::Incoming {
        header,
        body,
        responder,
        ..
    } = req;
    let request: ObjectsPull = match request_of(op, &header, &body) {
        Ok(request) => request,
        Err(error) => return responder.error(error),
    };
    if request.items.len() > MAX_WANT_ITEMS {
        return responder.error(invalid_request(
            op,
            &format!("{} items (at most {MAX_WANT_ITEMS})", request.items.len()),
        ));
    }
    let bound = match bind(
        ctx,
        peer,
        request.lane,
        &request.repo,
        &request.sender,
        &request.receiver,
    ) {
        Ok(bound) => bound,
        Err(error) => return responder.error(error),
    };
    started(ctx, peer, &request.repo, op);
    let budget = request.budget_bytes.min(PULL_BUDGET_BYTES);
    let spans = allot(&request.items, budget);
    let mut outcomes: HashMap<String, ObjectOutcome> = HashMap::new();
    let mut seen = HashSet::new();
    let mut served = Vec::new();
    let mut served_spans = Vec::new();
    for (item, span) in request.items.iter().zip(&spans) {
        if !seen.insert(item.hash.clone()) {
            continue;
        }
        if !bound.lane.owns(&item.hash) {
            outcomes.insert(
                item.hash.clone(),
                ObjectOutcome::Failed(ErrorWire::coded(
                    "OBJECT_ON_WRONG_LANE",
                    format!(
                        "{} is not owned by lane {}/{}",
                        item.hash, bound.lane.index, bound.lane.count
                    ),
                )),
            );
            continue;
        }
        if covered(item, *span) {
            served.push(item.clone());
            served_spans.push(*span);
        }
    }
    let sizes: HashMap<String, u64> = served
        .iter()
        .map(|item| (item.hash.clone(), item.size))
        .collect();
    // Objects whose size the dialer does not know: their one empty read
    // learns our total (module docs, "Pull").
    let unknown: HashSet<String> = served
        .iter()
        .filter(|item| size_unknown(item))
        .map(|item| item.hash.clone())
        .collect();
    let ranges = read_ranges(&served, &served_spans);
    let mut sent_to: HashMap<String, u64> = HashMap::new();
    let mut sent = 0u64;
    let mut failure: Option<ErrorWire> = None;
    let done = Cancel::new();
    // A first read of an object may verify it whole (verify-on-first-serve).
    let largest = sizes.values().copied().max().unwrap_or(0);
    let read_deadline =
        worker_client::deadlines::with_verify(worker_client::deadlines::READ, largest);
    thread::scope(|scope| {
        scope.spawn(|| keepalive(&responder, &done, None));
        bound.client.read_stream(
            &bound.route,
            &ranges,
            INFLIGHT_READS,
            read_deadline,
            &mut |range, result| {
                if responder.cancelled() {
                    return ReadFlow::Stop;
                }
                match result {
                    Ok((total, bytes)) if unknown.contains(&range.hash) && bytes.is_empty() => {
                        // The dialer does not know this object's size: one
                        // empty chunk carries our total (WP3.INT re-review);
                        // it pulls the bytes next with that size.
                        let chunk = ChunkHeader {
                            id: 0,
                            hash: Some(range.hash.clone()),
                            offset: 0,
                            total,
                            last: total == 0,
                        };
                        if responder.chunk(chunk, &[]).is_err() {
                            return ReadFlow::Stop;
                        }
                        ReadFlow::SkipObject
                    }
                    Ok((total, bytes)) => {
                        let size = sizes.get(&range.hash).copied().unwrap_or(total);
                        if total != size || bytes.len() as u64 != range.len {
                            outcomes.insert(range.hash.clone(), ObjectOutcome::SizeMismatch);
                            return ReadFlow::SkipObject;
                        }
                        for (chunk, piece) in wire_pieces(&range.hash, range.offset, total, &bytes)
                        {
                            if responder.chunk(chunk, piece).is_err() {
                                return ReadFlow::Stop;
                            }
                        }
                        sent += range.len;
                        sent_to.insert(range.hash.clone(), range.offset + range.len);
                        ReadFlow::Continue
                    }
                    Err(error @ (CallError::Unavailable(_) | CallError::ConnectionLost(_))) => {
                        failure = Some(worker_failure(&error));
                        ReadFlow::Stop
                    }
                    Err(error) => {
                        outcomes.insert(range.hash.clone(), read_outcome(&error));
                        ReadFlow::SkipObject
                    }
                }
            },
        );
        done.cancel();
    });
    if failure.is_none() && responder.cancelled() {
        failure = Some(ErrorWire::coded(
            "CANCELLED",
            "the dialer cancelled the objects.pull",
        ));
    }
    // Objects sent through their end: the dialer may report them corrupt.
    note_served(
        &peer.installation,
        &request.repo,
        sent_to
            .iter()
            .filter(|(hash, end)| sizes.get(hash.as_str()) == Some(*end))
            .map(|(hash, _)| hash.as_str()),
    );
    let results: Vec<ObjectResult> = request
        .items
        .iter()
        .filter(|item| seen.remove(&item.hash))
        .map(|item| {
            let end = sent_to.get(&item.hash).copied();
            let sent_outcome = match end {
                // A size probe sends no bytes of the object.
                _ if unknown.contains(&item.hash) => ObjectOutcome::Skipped,
                Some(end) if end == item.size => ObjectOutcome::Completed,
                Some(end) => ObjectOutcome::Progressed {
                    durable_offset: end,
                },
                None => ObjectOutcome::Skipped,
            };
            let outcome = outcomes.remove(&item.hash).unwrap_or(sent_outcome);
            ObjectResult {
                hash: item.hash.clone(),
                bytes: end.map_or(0, |end| end.saturating_sub(item.offset)),
                outcome,
            }
        })
        .collect();
    let result = match &failure {
        Some(error) => Err(error.clone()),
        None => Ok(()),
    };
    record_finished(
        ctx,
        Finished {
            peer,
            repo: &request.repo,
            op,
            result,
            summary: None,
            bytes_up: sent,
            bytes_down: 0,
            objects_up: 0,
            objects_down: 0,
            started: began,
        },
    );
    match failure {
        Some(error) => responder.error(error),
        None => reply_json(responder, &ObjectsResult { results }),
    }
}

// ---------------------------------------------------------------------------
// Offers and leases given up (module docs, "Leases given up")

/// What we offered a dialer's lane in its last `objects.wants`, until its
/// push (or its next `objects.wants`) shows what it moved.
struct Offer {
    /// The bulk session it was offered on.
    session: String,
    at: Instant,
    items: Vec<Offered>,
}

#[derive(Clone)]
struct Offered {
    /// As our worker gave it: its offset is where our worker stands.
    item: WantItem,
    /// Inside the allotment of a push of the whole offer: a dialer that can
    /// serve it sends some of it.
    covered: bool,
}

/// Offers per (worker instance, dialer lane's lease).
fn offers() -> &'static Mutex<HashMap<String, Offer>> {
    static OFFERS: OnceLock<Mutex<HashMap<String, Offer>>> = OnceLock::new();
    OFFERS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Offers remembered at most, and for at most this long.
const MAX_OFFERS: usize = 4096;
const OFFER_AGE: Duration = Duration::from_secs(600);

fn offer_key(instance: &str, lease: &str) -> String {
    format!("{instance}\u{1f}{lease}")
}

fn take_offer(key: &str) -> Option<Offer> {
    lock(offers()).remove(key)
}

fn put_offer(key: String, offer: Offer) {
    let mut map = lock(offers());
    if map.len() >= MAX_OFFERS {
        map.retain(|_, offer| offer.at.elapsed() < OFFER_AGE);
        if map.len() >= MAX_OFFERS {
            return;
        }
    }
    map.insert(key, offer);
}

/// Where one dialer's lane stands in our worker's leases: its lease session
/// and its per-object backoff keys.
struct Source<'a> {
    ctx: &'a Ctx,
    peer: &'a str,
    repo: &'a str,
    lane: LaneSpec,
    instance: String,
    lease: String,
}

impl<'a> Source<'a> {
    fn new(
        ctx: &'a Ctx,
        peer: &'a session::AcceptedPeer,
        repo: &'a str,
        bound: &Bound,
    ) -> Source<'a> {
        Source {
            ctx,
            peer: &peer.installation,
            repo,
            lane: bound.lane,
            instance: bound.client.endpoint().instance.clone(),
            lease: push_lease(&peer.installation, bound.lane),
        }
    }

    fn key(&self, hash: &str) -> ObjectKey {
        object_key(&self.ctx.installation, self.peer, self.repo, false, hash)
    }

    fn blocked(&self, hash: &str) -> bool {
        object_blocked(&self.key(hash))
    }

    /// What this dialer's wants exclude: the objects of its lane in their
    /// backoff for it.
    fn excluded(&self) -> Vec<String> {
        blocked_objects(
            &self.ctx.installation,
            self.peer,
            self.repo,
            false,
            MAX_RELEASE_ITEMS,
            &|hash| self.lane.owns(hash),
        )
    }

    /// The dialer did not send an object it was offered (it cannot read it,
    /// or it backs it off): no error here, but a backoff for this source
    /// (its lease is given up by the caller).
    fn not_served(&self, hash: &str) {
        object_unserved(&self.key(hash));
    }

    /// An offer the dialer's next request settles: `pushed` (what its push
    /// listed of it) or nothing (it pushed none of it). Called only when the
    /// dialer ended its push normally (or never started one). Returns what
    /// it did not send, to give up.
    fn settle_unpushed(&self, offer: &Offer, pushed: &HashSet<String>) -> Vec<String> {
        offer
            .items
            .iter()
            .filter(|offered| offered.covered && !pushed.contains(&offered.item.hash))
            .map(|offered| {
                self.not_served(&offered.item.hash);
                offered.item.hash.clone()
            })
            .collect()
    }
}

/// `objects.wants`: what our worker wants of the dialer, leased to the
/// dialer's lane ([`push_lease`]) for its next push, without what that lane
/// cannot serve now (module docs, "Leases given up").
fn wants(ctx: &Arc<Ctx>, peer: &session::AcceptedPeer, req: session::Incoming) {
    let op = ops::OBJECTS_WANTS;
    let session::Incoming {
        header,
        body,
        deadline,
        responder,
        ..
    } = req;
    let request: ObjectsWantsRequest = match request_of(op, &header, &body) {
        Ok(request) => request,
        Err(error) => return responder.error(error),
    };
    let bound = match bind(
        ctx,
        peer,
        request.lane,
        &request.repo,
        &request.sender,
        &request.receiver,
    ) {
        Ok(bound) => bound,
        Err(error) => return responder.error(error),
    };
    let Some(wait) = worker_deadline(deadline, worker_client::deadlines::WANT) else {
        return responder.error(deadline_error(op));
    };
    let source = Source::new(ctx, peer, &request.repo, &bound);
    let key = offer_key(&source.instance, &source.lease);
    // The last offer on this session was followed by no push: the dialer
    // took none of it. Its objects back off for this dialer, and the want
    // below excludes them first, which gives up their leases.
    let mut exclude = Vec::new();
    if let Some(offer) = take_offer(&key)
        && offer.session == peer.session
    {
        exclude = source.settle_unpushed(&offer, &HashSet::new());
    }
    for hash in source.excluded() {
        if !exclude.contains(&hash) {
            exclude.push(hash);
        }
    }
    exclude.truncate(MAX_RELEASE_ITEMS);
    let max_items = request.max_items.clamp(1, MAX_WANT_ITEMS as u32);
    let max_bytes = request.max_bytes.clamp(1, PULL_BUDGET_BYTES);
    let want = WantHeader {
        route: bound.route.clone(),
        lane: bound.lane,
        max_items,
        max_bytes,
        session: source.lease.clone(),
    };
    let items = match want_excluding(&bound.client, &want, exclude, wait) {
        Ok(items) => items,
        Err(error) => return responder.error(worker_failure(&error)),
    };
    // One in its backoff for this dialer although the want excluded what it
    // could (the list is bounded) is given up at once.
    let mut refused = Vec::new();
    let mut offered = Vec::new();
    for item in items {
        if !bound.lane.owns(&item.hash) {
            continue;
        }
        if source.blocked(&item.hash) {
            refused.push(item.hash);
        } else {
            offered.push(item);
        }
    }
    if responder.cancelled() {
        // The dialer will never see this offer.
        refused.extend(offered.drain(..).map(|item| item.hash));
    }
    if !refused.is_empty() {
        release_leases(&bound.client, &bound.route, &source.lease, refused);
    }
    if responder.cancelled() {
        return;
    }
    let spans = allot(&offered, PUSH_BUDGET_BYTES);
    let offer = Offer {
        session: peer.session.clone(),
        at: Instant::now(),
        items: offered
            .iter()
            .zip(spans)
            .map(|(item, span)| Offered {
                covered: covered(item, span),
                item: item.clone(),
            })
            .collect(),
    };
    if !offer.items.is_empty() {
        put_offer(key, offer);
    }
    reply_json(responder, &ObjectsWantsReply { items: offered });
}

/// One pushed object's share of the push (the allotment rule) and how far
/// its chunks got.
struct Share {
    size: u64,
    end: u64,
    /// Where the next chunk must start.
    next: u64,
    /// A chunk of it arrived.
    got: bool,
}

/// `objects.push` (module docs, "Push").
fn push(ctx: &Arc<Ctx>, peer: &session::AcceptedPeer, req: session::Incoming) {
    let began = Instant::now();
    let op = ops::OBJECTS_PUSH;
    let session::Incoming {
        header,
        chunks,
        responder,
        ..
    } = req;
    let request: ObjectsPush = match request_of(op, &header, &[]) {
        Ok(request) => request,
        Err(error) => return responder.error(error),
    };
    if request.items.len() > MAX_WANT_ITEMS {
        return responder.error(invalid_request(
            op,
            &format!("{} items (at most {MAX_WANT_ITEMS})", request.items.len()),
        ));
    }
    let Some(chunks) = chunks else {
        return responder.error(invalid_request(op, &"objects.push streams its objects"));
    };
    let bound = match bind(
        ctx,
        peer,
        request.lane,
        &request.repo,
        &request.sender,
        &request.receiver,
    ) {
        Ok(bound) => bound,
        Err(error) => return responder.error(error),
    };
    started(ctx, peer, &request.repo, op);
    let source = Source::new(ctx, peer, &request.repo, &bound);
    // What we offered this lane on this session.
    let offer = take_offer(&offer_key(&source.instance, &source.lease))
        .filter(|offer| offer.session == peer.session);
    let offered: HashMap<&str, &Offered> = offer
        .iter()
        .flat_map(|offer| &offer.items)
        .map(|offered| (offered.item.hash.as_str(), offered))
        .collect();
    let wrote = Arc::new(AtomicU64::new(0));
    let sink = {
        let wrote = wrote.clone();
        Sink::start(
            bound.client.clone(),
            bound.route.clone(),
            SinkOptions::default(),
            Box::new(move |events: Vec<SinkEvent>| {
                let written = events
                    .iter()
                    .filter(|event| matches!(event, SinkEvent::Released { written: true, .. }))
                    .count();
                wrote.fetch_add(written as u64, Ordering::Relaxed);
            }),
        )
    };
    // Each object's share of the push budget (the allotment rule, as the
    // dialer computes it): the only bytes it may send (invariant 22).
    let spans = allot(&request.items, PUSH_BUDGET_BYTES);
    let mut outcomes: HashMap<String, ObjectOutcome> = HashMap::new();
    let mut shares: HashMap<String, Share> = HashMap::new();
    let mut seen = HashSet::new();
    for (item, span) in request.items.iter().zip(&spans) {
        if !seen.insert(item.hash.clone()) {
            continue;
        }
        if !bound.lane.owns(&item.hash) {
            outcomes.insert(
                item.hash.clone(),
                ObjectOutcome::Failed(ErrorWire::coded(
                    "OBJECT_ON_WRONG_LANE",
                    format!(
                        "{} is not owned by lane {}/{}",
                        item.hash, bound.lane.index, bound.lane.count
                    ),
                )),
            );
            continue;
        }
        if covered(item, *span) {
            sink.expect(&item.hash, &source.lease, item.size, span.0, 0);
            shares.insert(
                item.hash.clone(),
                Share {
                    size: item.size,
                    end: span.1,
                    next: span.0,
                    got: false,
                },
            );
        }
    }
    let mut received = 0u64;
    let mut failure: Option<ErrorWire> = None;
    let done = Cancel::new();
    let mut results = HashMap::new();
    thread::scope(|scope| {
        // PROGRESS after writes (and every KEEPALIVE): the dialer's idle
        // deadline then measures our progress.
        scope.spawn(|| keepalive(&responder, &done, Some(&wrote)));
        let stop = || responder.cancelled();
        let mut last: Option<String> = None;
        loop {
            if stop() {
                break;
            }
            match chunks.recv_timeout(POLL) {
                Ok((chunk, bytes)) => {
                    if let Err(error) = admit(&mut shares, &chunk, bytes.len() as u64) {
                        failure = Some(error);
                        break;
                    }
                    let Some(hash) = chunk.hash.as_deref() else {
                        break;
                    };
                    received += bytes.len() as u64;
                    // The dialer moved on to another object: what it sent of
                    // the last one is written now, whole write or not (an
                    // in-order sender has sent its share of it).
                    if let Some(previous) = last.take_if(|previous| previous != hash) {
                        sink.flush(&[previous]);
                    }
                    last.get_or_insert_with(|| hash.to_owned());
                    // A refused part (the object settled, the worker gone)
                    // shows in the object's result.
                    let _ = sink.offer(hash, chunk.offset, chunk.total, &bytes, 0);
                    sink.wait_below(PUSH_BUFFERED_BYTES, &stop);
                    if let Some(error) = sink.fatal() {
                        failure = Some(worker_failure(&error));
                        break;
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        // What arrived is valid and contiguous: written even when the
        // dialer went away, so its next push resumes after it.
        results = sink.finish(failure.is_none());
        done.cancel();
    });
    // The dialer sent its push through to the end: what it did not send of
    // an object with a share, it could not (or would not) send.
    let whole = failure.is_none() && !responder.cancelled();
    // A whole write call that failed (our worker timed out, went away,
    // refused or erred for the call) fails the push, not its objects
    // (§2.3): no per-object backoff or error for it, here or at the dialer.
    if failure.is_none()
        && let Some(error) = results.values().find_map(|result| result.call.clone())
    {
        failure = Some(worker_failure(&error));
    }
    let mut objects_down = 0;
    let mut answer = Vec::new();
    let mut pushed = HashSet::new();
    // The leases this push gives up at our worker (module docs).
    let mut give_up = Vec::new();
    for item in &request.items {
        if !seen.remove(&item.hash) {
            continue;
        }
        pushed.insert(item.hash.clone());
        let result = results.get(&item.hash);
        let (outcome, written) = match outcomes.remove(&item.hash) {
            Some(outcome) => (outcome, 0),
            None => match result {
                Some(result) => (
                    result.outcome.clone().unwrap_or(if result.written > 0 {
                        ObjectOutcome::Progressed {
                            durable_offset: result.durable,
                        }
                    } else {
                        ObjectOutcome::Skipped
                    }),
                    result.written,
                ),
                None => (ObjectOutcome::Skipped, 0),
            },
        };
        let call = result.is_some_and(|result| result.call.is_some());
        // Only an object we offered this dialer can fail here (WP3.INT
        // re-review): a push may list any hash, and one we never offered
        // (not wanted, `UNSOLICITED_OBJECT`; not this lane's,
        // `OBJECT_ON_WRONG_LANE`) is answered in the result alone, with
        // neither a backoff nor an `ObjectError`, so a peer cannot fill our
        // status or our backoff table with hashes of its choosing.
        let ours = offered.contains_key(item.hash.as_str());
        match &outcome {
            ObjectOutcome::Completed => {
                objects_down += 1;
                // Installed: the errors of every source of it are resolved
                // (one that could not serve it may have given it up).
                for code in object_installed(&source.key(&item.hash)) {
                    ctx.status.record(StatusEvent::Resolved {
                        scope: Scope::Object {
                            state: bound.state.clone(),
                            hash: item.hash.clone(),
                        },
                        code,
                    });
                }
            }
            other if is_failure(other) && !call && ours => {
                let detail = format!("pushed by peer {}", peer.installation);
                if let Some(mut error) = outcome_error(&item.hash, other, &detail) {
                    let delay = object_failed(&source.key(&item.hash), &error.code);
                    error.retry_after_ms =
                        Some(u64::try_from(delay.as_millis()).unwrap_or(u64::MAX));
                    ctx.status.record(StatusEvent::ObjectError {
                        state: bound.state.clone(),
                        hash: item.hash.clone(),
                        error,
                        source: Some(ObjectSource {
                            installation: peer.installation.clone(),
                            lane: status_lane(peer),
                            direction: Direction::Inbound,
                        }),
                    });
                }
            }
            _ => {}
        }
        if ours {
            let unsent = whole && shares.get(&item.hash).is_some_and(|share| !share.got);
            if gives_up(&source, &item.hash, result, &outcome, unsent, whole) {
                give_up.push(item.hash.clone());
            }
        }
        answer.push(ObjectResult {
            hash: item.hash.clone(),
            bytes: written,
            outcome,
        });
    }
    if let Some(offer) = &offer {
        if whole {
            give_up.extend(source.settle_unpushed(offer, &pushed));
        } else {
            // The push failed or the dialer went away: what we offered it and
            // it did not list is given up too, for another source.
            give_up.extend(
                offer
                    .items
                    .iter()
                    .filter(|offered| !pushed.contains(&offered.item.hash))
                    .map(|offered| offered.item.hash.clone()),
            );
        }
    }
    if !give_up.is_empty() {
        release_leases(&bound.client, &bound.route, &source.lease, give_up);
    }
    let result = match &failure {
        Some(error) => Err(error.clone()),
        None => Ok(()),
    };
    record_finished(
        ctx,
        Finished {
            peer,
            repo: &request.repo,
            op,
            result,
            summary: None,
            bytes_up: 0,
            bytes_down: received,
            objects_up: 0,
            objects_down,
            started: began,
        },
    );
    match failure {
        Some(error) => responder.error(error),
        None => reply_json(responder, &ObjectsResult { results: answer }),
    }
}

/// Checks one pushed chunk against the shares (invariant 22): an object of
/// the push, inside its share, contiguous with its last chunk, of the size
/// the push declared; a zero-size object is one empty chunk. So at most the
/// push budget arrives, in order, and nothing waits ahead of a gap.
fn admit(
    shares: &mut HashMap<String, Share>,
    chunk: &ChunkHeader,
    len: u64,
) -> Result<(), ErrorWire> {
    let violation = |detail: String| ErrorWire::coded("PROTOCOL_VIOLATION", detail);
    let Some(hash) = chunk.hash.as_deref() else {
        return Err(violation(
            "an objects.push stream carried a chunk without an object".into(),
        ));
    };
    let Some(share) = shares.get_mut(hash) else {
        return Err(violation(format!(
            "objects.push streamed {hash}, which has no share of this push"
        )));
    };
    let end = chunk.offset.saturating_add(len);
    let in_order = chunk.offset == share.next && end <= share.end && chunk.total == share.size;
    let sized = len > 0 || (share.size == 0 && !share.got);
    if !in_order || !sized {
        return Err(violation(format!(
            "objects.push streamed {hash} [{}, {end}) of {}; its share continues at {} up to {} of {}",
            chunk.offset, chunk.total, share.next, share.end, share.size
        )));
    }
    share.next = end;
    share.got = true;
    Ok(())
}

/// Whether the lease of one pushed object of our offer is given up after the
/// push (module docs, "Leases given up"): not when it settled for good here
/// (installed, already ours, held by another session, no longer wanted);
/// yes when it had a share the dialer did not send (`unsent`, a push that
/// went through: not served, backed off for this dialer), when it failed
/// here (backed off already), and when the push did not go through
/// (`whole` false: the dialer abandoned it). An object still moving under a
/// push that went through keeps its lease (the dialer continues it).
fn gives_up(
    source: &Source<'_>,
    hash: &str,
    result: Option<&SinkResult>,
    outcome: &ObjectOutcome,
    unsent: bool,
    whole: bool,
) -> bool {
    let settled = result.and_then(|result| result.outcome.as_ref());
    if matches!(
        settled,
        Some(
            ObjectOutcome::Completed
                | ObjectOutcome::Skipped
                | ObjectOutcome::InFlightElsewhere
                | ObjectOutcome::Unsolicited
        )
    ) {
        return false;
    }
    if unsent && settled.is_none() {
        source.not_served(hash);
        return true;
    }
    is_failure(outcome) || !whole
}

// ---------------------------------------------------------------------------
// Objects served through their end (what a peer may report)

/// Objects pulls sent through their end, per (peer, repository): the last
/// [`SERVED_MAX`], for at most [`SERVED_AGE`]. Keys are keyed hashes (a
/// per-process random key), so the record stays small.
struct Served {
    keys: RandomState,
    at: HashMap<u64, Instant>,
    order: VecDeque<(u64, Instant)>,
}

const SERVED_MAX: usize = 256 * 1024;
const SERVED_AGE: Duration = Duration::from_secs(3600);

fn served() -> &'static Mutex<Served> {
    static SERVED: OnceLock<Mutex<Served>> = OnceLock::new();
    SERVED.get_or_init(|| {
        Mutex::new(Served {
            keys: RandomState::new(),
            at: HashMap::new(),
            order: VecDeque::new(),
        })
    })
}

impl Served {
    fn key(&self, peer: &str, repo: &str, hash: &str) -> u64 {
        self.keys.hash_one((peer, repo, hash))
    }
}

/// `peer` got `hashes` of `repo` through their end.
fn note_served<'a>(peer: &str, repo: &str, hashes: impl Iterator<Item = &'a str>) {
    let now = Instant::now();
    let mut served = lock(served());
    for hash in hashes {
        let key = served.key(peer, repo, hash);
        served.at.insert(key, now);
        served.order.push_back((key, now));
    }
    while served.order.len() > SERVED_MAX
        || served
            .order
            .front()
            .is_some_and(|(_, at)| at.elapsed() >= SERVED_AGE)
    {
        let Some((key, at)) = served.order.pop_front() else {
            break;
        };
        if served.at.get(&key) == Some(&at) {
            served.at.remove(&key);
        }
    }
}

/// Whether `peer` got `hash` of `repo` through its end lately.
fn was_served(peer: &str, repo: &str, hash: &str) -> bool {
    let served = lock(served());
    let key = served.key(peer, repo, hash);
    served
        .at
        .get(&key)
        .is_some_and(|at| at.elapsed() < SERVED_AGE)
}

/// `objects.report{repo, sender, hash, code}`: our worker re-verifies its
/// copy (and quarantines it if it is corrupt); the reply is `{}`. Only for
/// an object of the session's lane that we served this peer through its
/// end lately (`OBJECT_ON_WRONG_LANE`, `UNSOLICITED_OBJECT`).
fn report(ctx: &Arc<Ctx>, peer: &session::AcceptedPeer, req: session::Incoming) {
    let op = ops::OBJECTS_REPORT;
    let session::Incoming {
        header,
        body,
        deadline,
        responder,
        ..
    } = req;
    let request: ObjectsReport = match request_of(op, &header, &body) {
        Ok(request) => request,
        Err(error) => return responder.error(error),
    };
    let Some(lane) = peer.lane else {
        return responder.error(ErrorWire::coded(
            "BULK_LANE_MISMATCH",
            "objects.report needs a bulk session with a lane",
        ));
    };
    // Only an object this lane carries, and that we served this peer
    // through its end lately: a report makes our worker re-read the whole
    // object, which a peer may not ask for any object it likes.
    if !lane.owns(&request.hash) {
        return responder.error(ErrorWire::coded(
            "OBJECT_ON_WRONG_LANE",
            format!(
                "{} is not owned by lane {}/{}",
                request.hash, lane.index, lane.count
            ),
        ));
    }
    if !was_served(&peer.installation, &request.repo, &request.hash) {
        return responder.error(ErrorWire::coded(
            "UNSOLICITED_OBJECT",
            format!(
                "objects.report for {}, which this acceptor did not serve to {} lately",
                request.hash, peer.installation
            ),
        ));
    }
    // The report names no receiver: the folder of the repository is it.
    let served = match resolve(ctx, peer, &request.repo, &request.sender) {
        Ok(served) => served,
        Err(error) => return responder.error(error),
    };
    let receiver = served.route.local_replica.clone();
    let bound = match bind(ctx, peer, lane, &request.repo, &request.sender, &receiver) {
        Ok(bound) => bound,
        Err(error) => return responder.error(error),
    };
    let Some(wait) = worker_deadline(deadline, deadlines::OBJECTS_REPORT) else {
        return responder.error(deadline_error(op));
    };
    match bound
        .client
        .report(&bound.route, &request.hash, &request.code, wait)
    {
        Ok(_) => responder.reply(json!({}), Vec::new()),
        Err(error) => responder.error(worker_failure(&error)),
    }
}
