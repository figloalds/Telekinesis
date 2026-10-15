//! Owned by WP2.8 and WP3.3 — inbound `objects.*` (REVAMP_PLAN §3.8, §6.4,
//! §6.5).
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
//! | `objects.wants` | what the dialer's lane continues or takes over (the handoff table), then our worker's `objects.want`, leased to [`push_lease`] (peer, lane) → `ObjectsWantsReply` |
//! | `objects.push` | the CHUNK stream, checked against each object's share → our worker's `objects.write` (≤ 4 in flight, one per object, with the lease it was offered under) → `ObjectsResult` |
//! | `objects.report` | for an object of the lane that we served this peer through its end lately: our worker's `objects.report` (it re-verifies and, if corrupt, quarantines its copy) → `{}` |
//! | `objects.exchange` | the interim page (§6.5) → worker `published-bulk-exchange` |
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
//! A folder whose worker serves no objects v3 (no loopback sessions, or
//! `UNSUPPORTED_OPERATION` from its objects ops) is answered
//! `UNSUPPORTED_OPERATION` too: the dialer then takes the interim path.
//!
//! **Pull.** The items cover the bytes of the allotment rule
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
//! progress, not its queue. Objects that failed here (for example
//! `CorruptAtSource`) back off for that peer: `objects.wants` does not list
//! them until then. A write call that fails as a whole (our worker timed
//! out, went away, refused or erred for the call) fails the push with that
//! error, not its objects: no per-object backoff or `ObjectError` (§2.3).
//!
//! **Lease handoff** ([`super::objects`] module docs). Our worker leases
//! what `objects.wants` returns to the dialer's lane, and that lane's next
//! `objects.wants` renews the lease. What we offered (the *offer*: each
//! object, its lease, and whether a push of the whole offer would give it a
//! share) is compared with what the dialer then does on the same session:
//! an object with a share it did not send (its push ended normally without
//! a byte of it, or did not list it, or no push came before its next
//! `objects.wants`) is one it cannot or will not serve. It backs off for
//! that peer (no `ObjectError`: not serving is no error here) and is
//! *parked* under its lease, and the next dialer of another source whose
//! lane owns it is offered it first, to push under that lease. That
//! dialer's claim (its lane's push lease and its bulk session) stands while
//! its pushes go through. When a push fails or the dialer abandons it (its
//! session ends), or an `objects.wants` fails, every claim of that session
//! goes back to the table, so another source takes the object over; the
//! claims of a dialer that vanishes between requests lapse
//! ([`super::objects::CLAIM_TTL`]).
//!
//! **Report.** A report makes our worker re-read a whole object, so it is
//! taken only for an object of the session's lane (`OBJECT_ON_WRONG_LANE`)
//! that a pull of ours sent this peer through its end in the last hour
//! (`UNSOLICITED_OBJECT`; the record is bounded).
//!
//! **Interim** `objects.exchange` (§6.5): the dialer's legacy
//! `bulk_sync::Page` (bulk protocol 1, v6 JSON) goes to the folder's worker
//! as `published-bulk-exchange` (route checked by the worker:
//! `BULK_ROUTE_MISMATCH`, `OBJECT_ON_WRONG_LANE`, …), and the worker's page
//! is the reply: the header `{repo}` and the page as the JSON body, trimmed
//! to `protocol::max_body` (object parts are dropped, the last ones first;
//! offers are stateless, so they are offered again later).
//!
//! **Status.** `ExchangeStarted`/`ExchangeFinished{direction: inbound}` for
//! every pull and push served (bytes; `objects_down`: objects a push
//! completed here; `objects_up` is 0, only the dialer knows what it
//! installed) and for every interim exchange; `ObjectError` for objects a
//! push failed here, `Resolved` for every source's errors of an object a
//! push installed.
use super::{
    inbound::{
        Failed, Finished, deadline_error, invalid_request, peer_error, record_finished, resolve,
        status_lane, worker_deadline,
    },
    objects::{
        KEEPALIVE, ObjectKey, PUSH_BUDGET_BYTES, PUSH_BUFFERED_BYTES, Renewal, Sink, SinkEvent,
        SinkOptions, SinkResult, allot, covered, handoff_park, handoff_progress, handoff_release,
        handoff_take, handoff_unclaim, handoff_unclaim_all, handoff_wanted, is_failure,
        is_unsupported, object_blocked, object_failed, object_installed, object_key, outcome_error,
        push_lease, read_outcome, read_ranges, size_unknown, wire_pieces,
    },
    worker_client::{self, ReadFlow, SessionWorkerClient},
};
use crate::{
    bulk_sync::Page,
    model::{
        errors::{ErrorWire, Scope},
        orchestrator::{CallError, Ctx},
        protocol::{
            self, LaneRole, LaneSpec, deadlines,
            frame::ChunkHeader,
            objects::{
                INFLIGHT_READS, MAX_WANT_ITEMS, ObjectOutcome, ObjectResult, ObjectsPull,
                ObjectsPush, ObjectsReport, ObjectsResult, ObjectsWantsReply, ObjectsWantsRequest,
                PULL_BUDGET_BYTES, WantHeader, WantItem,
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

/// The worker op that serves an inbound interim bulk page.
const BULK_EXCHANGE: &str = "published-bulk-exchange";
/// A JSON reply above this many bytes goes in the body (the response
/// header bound is 64 KiB, framing included).
const HEADER_REPLY_MAX: usize = protocol::frame::MAX_HEADER - 1024;
/// How often a waiting exchange looks at its request (cancelled, chunks).
const POLL: Duration = Duration::from_millis(100);

pub fn handle(ctx: &Arc<Ctx>, peer: &session::AcceptedPeer, req: session::Incoming) {
    if peer.role != LaneRole::Bulk {
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
        ops::OBJECTS_EXCHANGE => exchange(ctx, peer, req),
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
    // an invalid request, never "not served" (which would send a buggy v3
    // dialer to the interim path without a trace).
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
    let client = worker_client::session_for(endpoint).map_err(|error| {
        if is_unsupported(&error) {
            error.to_wire()
        } else {
            peer_error(&error)
        }
    })?;
    Ok(Bound {
        state: served.view.record.state.clone(),
        route: served.route,
        lane: session_lane,
        client,
    })
}

/// The answer to the peer for a failed worker call of a v3 op.
fn worker_failure(error: &CallError) -> ErrorWire {
    if is_unsupported(error) {
        ErrorWire::coded(
            "UNSUPPORTED_OPERATION",
            format!("this folder's worker serves no objects v3: {error}"),
        )
    } else {
        peer_error(error)
    }
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
                    Err(error) if is_unsupported(&error) => {
                        failure = Some(worker_failure(&error));
                        ReadFlow::Stop
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
// Offers and lease handoff (module docs, "Lease handoff")

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
    /// As our worker (or the handoff table) gave it: its offset is where our
    /// worker stands.
    item: WantItem,
    /// The lease its writes use.
    lease: String,
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

/// Where one dialer's lane stands in our worker's leases: its lease session,
/// its claimant in the handoff table (that lease and its bulk session: one
/// session gives back only its own claims), and its per-object backoff keys.
struct Source<'a> {
    ctx: &'a Ctx,
    peer: &'a str,
    repo: &'a str,
    instance: String,
    lease: String,
    claimant: String,
}

impl<'a> Source<'a> {
    fn new(
        ctx: &'a Ctx,
        peer: &'a session::AcceptedPeer,
        repo: &'a str,
        bound: &Bound,
    ) -> Source<'a> {
        let lease = push_lease(&peer.installation, bound.lane);
        Source {
            ctx,
            peer: &peer.installation,
            repo,
            instance: bound.client.endpoint().instance.clone(),
            claimant: format!("{lease}@{}", peer.session),
            lease,
        }
    }

    fn key(&self, hash: &str) -> ObjectKey {
        object_key(&self.ctx.installation, self.peer, self.repo, false, hash)
    }

    fn blocked(&self, hash: &str) -> bool {
        object_blocked(&self.key(hash))
    }

    /// `item` (at the durable offset our worker gave) cannot move under
    /// `lease` through this dialer: parked for another source. One under
    /// this dialer's own lease is parked anew; one it took over is given
    /// back to the table (module docs, "Lease handoff").
    fn park(&self, item: &WantItem, lease: &str) {
        if lease == self.lease {
            handoff_park(&self.instance, item, lease);
        } else {
            handoff_unclaim(
                &self.instance,
                &item.hash,
                lease,
                &self.claimant,
                Some(item),
            );
        }
    }

    /// The dialer did not send an object it was offered (it cannot read it,
    /// or it backs it off): no error here, but a backoff for this source,
    /// and the object is parked under its lease for another source.
    fn not_served(&self, offered: &Offered) {
        object_failed(&self.key(&offered.item.hash), "");
        self.park(&offered.item, &offered.lease);
    }

    /// The dialer's request failed or was cancelled: what it claimed goes
    /// back to the table.
    fn unclaim_all(&self) {
        handoff_unclaim_all(Some(&self.instance), &self.claimant);
    }

    /// An offer the dialer's next request settles: `pushed` (what its push
    /// moved of it) or nothing (it pushed none of it). Called only when the
    /// dialer ended its push normally (or never started one).
    fn settle_unpushed(&self, offer: &Offer, pushed: &HashSet<String>) {
        for offered in &offer.items {
            if offered.covered && !pushed.contains(&offered.item.hash) {
                self.not_served(offered);
            }
        }
    }
}

/// `objects.wants`: what our worker wants of the dialer, leased to the
/// dialer's lane ([`push_lease`]) for its next push, after what that lane
/// continues or takes over under another lease (module docs, "Lease
/// handoff").
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
    // took none of it.
    if let Some(offer) = take_offer(&key)
        && offer.session == peer.session
    {
        source.settle_unpushed(&offer, &HashSet::new());
    }
    let max_items = request.max_items.clamp(1, MAX_WANT_ITEMS as u32);
    let max_bytes = request.max_bytes.clamp(1, PULL_BUDGET_BYTES);
    let mut offered = handoff_take(
        &source.instance,
        &source.claimant,
        max_items as usize,
        max_bytes,
        &|hash| bound.lane.owns(hash) && !source.blocked(hash),
    );
    let want = WantHeader {
        route: bound.route.clone(),
        lane: bound.lane,
        max_items,
        max_bytes,
        session: source.lease.clone(),
    };
    match bound.client.want(&want, wait) {
        Ok(items) => {
            for item in items {
                if !bound.lane.owns(&item.hash) {
                    continue;
                }
                if let Some(index) = offered
                    .iter()
                    .position(|(taken, _)| taken.hash == item.hash)
                {
                    // The other lease lapsed and this want took it: our
                    // worker's word is the newer one.
                    let (_, stale) = offered.remove(index);
                    handoff_release(&source.instance, &item.hash, &stale);
                }
                // An object in its backoff for this source is parked under
                // this lease, for another source; one another source
                // continues under this lease is left to it.
                let blocked = source.blocked(&item.hash);
                if handoff_wanted(
                    &source.instance,
                    &item,
                    &source.lease,
                    &source.claimant,
                    blocked,
                ) {
                    offered.push((item, source.lease.clone()));
                }
            }
        }
        Err(error) => {
            // What this request took from the handoff table goes back.
            source.unclaim_all();
            return responder.error(worker_failure(&error));
        }
    }
    if responder.cancelled() {
        // The dialer will never see this offer.
        source.unclaim_all();
        return;
    }
    offered.truncate(max_items as usize);
    let items: Vec<WantItem> = offered.iter().map(|(item, _)| item.clone()).collect();
    let spans = allot(&items, PUSH_BUDGET_BYTES);
    let offer = Offer {
        session: peer.session.clone(),
        at: Instant::now(),
        items: offered
            .into_iter()
            .zip(spans)
            .map(|((item, lease), span)| Offered {
                covered: covered(&item, span),
                item,
                lease,
            })
            .collect(),
    };
    if !offer.items.is_empty() {
        put_offer(key, offer);
    }
    reply_json(responder, &ObjectsWantsReply { items });
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
    // What we offered this lane on this session: each object's lease (one
    // taken over from another source is written under that source's lease).
    let offer = take_offer(&offer_key(&source.instance, &source.lease))
        .filter(|offer| offer.session == peer.session);
    let offered: HashMap<&str, &Offered> = offer
        .iter()
        .flat_map(|offer| &offer.items)
        .map(|offered| (offered.item.hash.as_str(), offered))
        .collect();
    let lease_of = |hash: &str| {
        offered
            .get(hash)
            .map_or_else(|| source.lease.clone(), |offered| offered.lease.clone())
    };
    // What this dialer took over stays claimed while the push runs (module
    // docs, "Lease handoff").
    let renewal = Renewal::new(&source.instance, &source.claimant);
    if offered
        .values()
        .any(|offered| offered.lease != source.lease)
    {
        renewal.activate();
    }
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
            sink.expect(&item.hash, &lease_of(&item.hash), item.size, span.0, 0);
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
        let stop = || {
            renewal.tick(false);
            responder.cancelled()
        };
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
        renewal.tick(true);
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
    for item in &request.items {
        if !seen.remove(&item.hash) {
            continue;
        }
        pushed.insert(item.hash.clone());
        let lease = lease_of(&item.hash);
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
                // (one that could not serve it may have handed it on).
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
        let unsent = whole && shares.get(&item.hash).is_some_and(|share| !share.got);
        if !ours {
            answer.push(ObjectResult {
                hash: item.hash.clone(),
                bytes: written,
                outcome,
            });
            continue;
        }
        settle_pushed(
            &source,
            item,
            &lease,
            offered.get(item.hash.as_str()).copied(),
            result,
            &outcome,
            unsent,
            whole,
        );
        answer.push(ObjectResult {
            hash: item.hash.clone(),
            bytes: written,
            outcome,
        });
    }
    if whole && let Some(offer) = &offer {
        source.settle_unpushed(offer, &pushed);
    }
    if !whole {
        // The push failed or the dialer went away: what it took over and did
        // not finish goes back to the table, for another source.
        source.unclaim_all();
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

/// The handoff table after one pushed object (module docs, "Lease
/// handoff"): installed or another session's → forgotten; not sent at all
/// though it had a share (`unsent`) → not served by this dialer; failed →
/// parked for another source; taken over and still moving → still this
/// dialer's when its push went through (`whole`), given back otherwise.
#[allow(clippy::too_many_arguments)]
fn settle_pushed(
    source: &Source<'_>,
    item: &WantItem,
    lease: &str,
    offered: Option<&Offered>,
    result: Option<&SinkResult>,
    outcome: &ObjectOutcome,
    unsent: bool,
    whole: bool,
) {
    let own = lease == source.lease;
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
        if !own {
            handoff_release(&source.instance, &item.hash, lease);
        }
        return;
    }
    // Where our worker stands: a write's word, or (nothing written) the
    // offset our own want gave; the dialer's claim is not that.
    let known = match result {
        Some(result) if result.call.is_some() => None,
        Some(result) if result.written > 0 || settled.is_some() => Some(result.durable),
        _ => offered.map(|offered| offered.item.offset),
    };
    let base = offered.map_or(item, |offered| &offered.item);
    let at = known.map(|offset| WantItem {
        offset,
        ..base.clone()
    });
    match at {
        Some(at) if unsent && settled.is_none() => source.not_served(&Offered {
            item: at,
            lease: lease.to_owned(),
            covered: true,
        }),
        Some(at) if is_failure(outcome) => source.park(&at, lease),
        Some(at) if !own && whole => {
            handoff_progress(&source.instance, &at, lease, &source.claimant);
        }
        Some(at) if !own => {
            handoff_unclaim(
                &source.instance,
                &item.hash,
                lease,
                &source.claimant,
                Some(&at),
            );
        }
        None if !own => handoff_release(&source.instance, &item.hash, lease),
        _ if unsent && settled.is_none() => {
            // Our own lease, where our worker stands not known: a backoff
            // for this source; our worker's next want parks it.
            object_failed(&source.key(&item.hash), "");
        }
        _ => {}
    }
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

// ---------------------------------------------------------------------------
// Interim path (§6.5)

/// The interim `objects.exchange` (§6.5).
fn exchange(ctx: &Arc<Ctx>, peer: &session::AcceptedPeer, req: session::Incoming) {
    let started = Instant::now();
    let op = ops::OBJECTS_EXCHANGE;
    let session::Incoming {
        header,
        body,
        deadline,
        responder,
        ..
    } = req;
    let parsed = if body.is_empty() {
        serde_json::from_value::<Page>(header)
    } else {
        serde_json::from_slice::<Page>(&body)
    };
    let page = match parsed {
        Ok(page) => page,
        Err(error) => {
            responder.error(invalid_request(op, &error));
            return;
        }
    };
    let lane_matches = peer.lane.is_some_and(|lane| {
        u32::try_from(page.lane.index).is_ok_and(|index| index == lane.index)
            && u32::try_from(page.lane.count).is_ok_and(|count| count == lane.count)
    });
    if !lane_matches {
        responder.error(ErrorWire::coded(
            "BULK_LANE_MISMATCH",
            format!(
                "the page is for lane {}/{}; this session is lane {}",
                page.lane.index,
                page.lane.count,
                peer.lane.map_or_else(
                    || "none".to_owned(),
                    |lane| format!("{}/{}", lane.index, lane.count)
                )
            ),
        ));
        return;
    }
    let served = match resolve(ctx, peer, &page.repo, &page.sender) {
        Ok(served) => served,
        Err(error) => {
            responder.error(error);
            return;
        }
    };
    let Some(wait) = worker_deadline(deadline, deadlines::OBJECTS_EXCHANGE) else {
        responder.error(deadline_error(op));
        return;
    };
    let bytes_down = page.object_bytes() as u64;
    let completed_down = completed_parts(&page);
    let repo = page.repo.clone();
    let payload = json!({ "route": served.route, "lane": page.lane, "page": page });
    if responder.cancelled() {
        return;
    }
    let result = served
        .client
        .call(BULK_EXCHANGE, payload, wait)
        .map_err(|error| Failed::call(&error))
        .and_then(|value| {
            let page = parse_reply(value).map_err(Failed::both)?;
            fit_page(page, protocol::max_body(op)).map_err(Failed::both)
        });
    let finish = |outcome: Result<(), ErrorWire>, bytes_up: u64| {
        let objects_down = if outcome.is_ok() { completed_down } else { 0 };
        record_finished(
            ctx,
            Finished {
                peer,
                repo: &repo,
                op,
                result: outcome,
                summary: None,
                bytes_up,
                bytes_down,
                objects_up: 0,
                objects_down,
                started,
            },
        );
    };
    match result {
        Ok((body, sent_bytes)) => {
            // Status first: once the reply is out, the dialer may reuse
            // this exchange's slot.
            finish(Ok(()), sent_bytes);
            responder.reply(json!({ "repo": repo }), body);
        }
        Err(failed) => {
            finish(Err(failed.local), 0);
            responder.error(failed.peer);
        }
    }
}

/// Object parts of `page` that end their object (the worker installs and
/// registers each one, or fails the whole call).
fn completed_parts(page: &Page) -> u64 {
    page.object_parts
        .values()
        .filter(|part| {
            part.offset
                .checked_add((part.hex.len() / 2) as u64)
                .is_some_and(|end| end == part.total)
        })
        .count() as u64
}

fn parse_reply(value: Value) -> Result<Page, ErrorWire> {
    serde_json::from_value(value).map_err(|error| {
        ErrorWire::coded(
            "INVALID_REPLY",
            format!("the worker's {BULK_EXCHANGE} reply is not a bulk page: {error}"),
        )
    })
}

/// Encodes `page` within `limit` bytes, dropping object parts (the last ones
/// first) while it does not fit. Returns the body and the object bytes it
/// carries. A page whose inventories alone exceed the bound is refused
/// (`BULK_PAGE_TOO_LARGE`).
fn fit_page(mut page: Page, limit: usize) -> Result<(Vec<u8>, u64), ErrorWire> {
    loop {
        let body =
            serde_json::to_vec(&page).map_err(|error| ErrorWire::coded("UNKNOWN_ERROR", error))?;
        if body.len() <= limit {
            return Ok((body, page.object_bytes() as u64));
        }
        if page.object_parts.is_empty() {
            return Err(ErrorWire::coded(
                "BULK_PAGE_TOO_LARGE",
                format!(
                    "the worker's page is {} bytes without object parts, above the {limit}-byte \
                     objects.exchange bound",
                    body.len()
                ),
            ));
        }
        // Drop parts from the end until the estimate fits, then re-encode.
        let mut over = body.len() - limit;
        while over > 0 {
            let Some((hash, part)) = page.object_parts.pop_last() else {
                break;
            };
            // `"hash":{"offset":…,"total":…,"hex":"…"},` with some slack.
            let size = hash.len() + part.hex.len() + 64;
            over = over.saturating_sub(size);
        }
    }
}
