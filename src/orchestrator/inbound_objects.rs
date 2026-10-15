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
//! | `objects.wants` | our worker's `objects.want`, leased to [`push_lease`] (peer, lane) → `ObjectsWantsReply` |
//! | `objects.push` | the CHUNK stream → our worker's `objects.write` (≤ 4 in flight, one per object, with that lease) → `ObjectsResult` |
//! | `objects.report` | our worker's `objects.report` (it re-verifies and, if corrupt, quarantines its copy) → `{}` |
//! | `objects.exchange` | the interim page (§6.5) → worker `published-bulk-exchange` |
//!
//! **Checks, in order** (none of the refusals reaches a worker):
//! 1. the session is a bulk session (`BULK_ON_CONTROL_LANE`; the inbound
//!    router checks this first too);
//! 2. the request parses (a v3 request that does not is answered
//!    `UNSUPPORTED_OPERATION`, the answer of a pre-v3 acceptor, so a dialer
//!    treats both alike; see the WP3.3 notes) and names the session's own
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
//! **Push.** Bytes are written as they arrive; the stream is read no faster
//! than our worker writes (at most [`PUSH_BUFFERED_BYTES`] wait), so the
//! session holds the dialer back. A PROGRESS follows every write, so the
//! dialer's idle deadline measures our progress, not its queue. Objects
//! that failed here (for example `CorruptAtSource`) back off for that peer:
//! `objects.wants` does not list them until then.
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
//! installed) and for every interim exchange; `ObjectError`/`Resolved` for
//! objects a push failed or later completed here.
use super::{
    inbound::{
        Failed, Finished, deadline_error, invalid_request, peer_error, record_finished, resolve,
        status_lane, worker_deadline,
    },
    objects::{
        KEEPALIVE, PUSH_BUFFERED_BYTES, Sink, SinkEvent, SinkOptions, allot, covered, is_failure,
        is_unsupported, object_blocked, object_completed, object_failed, object_key, outcome_error,
        push_lease, read_outcome, read_ranges,
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
                PULL_BUDGET_BYTES, WantHeader,
            },
            ops,
        },
        status::{Direction, StatusEvent},
    },
    paired_sync::SyncRoute,
    session::{self, Responder},
    util::cancel::Cancel,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
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
    parsed.map_err(|error| {
        ErrorWire::coded(
            "UNSUPPORTED_OPERATION",
            format!("{op}: not an objects v3 request this acceptor serves: {error}"),
        )
    })
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
                    Ok((total, bytes)) => {
                        let size = sizes.get(&range.hash).copied().unwrap_or(total);
                        if total != size || bytes.len() as u64 != range.len {
                            outcomes.insert(range.hash.clone(), ObjectOutcome::SizeMismatch);
                            return ReadFlow::SkipObject;
                        }
                        let chunk = ChunkHeader {
                            id: 0,
                            hash: Some(range.hash.clone()),
                            offset: range.offset,
                            total,
                            last: range.offset + range.len == total,
                        };
                        if responder.chunk(chunk, &bytes).is_err() {
                            return ReadFlow::Stop;
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
    let results: Vec<ObjectResult> = request
        .items
        .iter()
        .filter(|item| seen.remove(&item.hash))
        .map(|item| {
            let end = sent_to.get(&item.hash).copied();
            let sent_outcome = match end {
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

/// `objects.wants`: what our worker wants of the dialer, leased to the
/// dialer's lane ([`push_lease`]) for its next push.
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
    let want = WantHeader {
        route: bound.route.clone(),
        lane: bound.lane,
        max_items: request.max_items.clamp(1, MAX_WANT_ITEMS as u32),
        max_bytes: request.max_bytes.clamp(1, PULL_BUDGET_BYTES),
        session: push_lease(&peer.installation, bound.lane),
    };
    match bound.client.want(&want, wait) {
        Ok(items) => {
            // Objects in their backoff for this source are not asked for.
            let items = items
                .into_iter()
                .filter(|item| {
                    bound.lane.owns(&item.hash)
                        && !object_blocked(&object_key(
                            &ctx.installation,
                            &peer.installation,
                            &request.repo,
                            false,
                            &item.hash,
                        ))
                })
                .collect();
            reply_json(responder, &ObjectsWantsReply { items });
        }
        Err(error) => responder.error(worker_failure(&error)),
    }
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
    let lease = push_lease(&peer.installation, bound.lane);
    let wrote = Arc::new(AtomicU64::new(0));
    let sink = {
        let wrote = wrote.clone();
        Sink::start(
            bound.client.clone(),
            bound.route.clone(),
            SinkOptions::default(),
            Box::new(move |event| {
                if let SinkEvent::Released { written: true, .. } = event {
                    wrote.fetch_add(1, Ordering::Relaxed);
                }
            }),
        )
    };
    let mut outcomes: HashMap<String, ObjectOutcome> = HashMap::new();
    let mut seen = HashSet::new();
    for item in &request.items {
        if !seen.insert(item.hash.clone()) {
            continue;
        }
        if bound.lane.owns(&item.hash) {
            sink.expect(&item.hash, &lease, item.size, item.offset, 0);
        } else {
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
        loop {
            if responder.cancelled() {
                break;
            }
            match chunks.recv_timeout(POLL) {
                Ok((chunk, bytes)) => {
                    let Some(hash) = chunk.hash.as_deref() else {
                        failure = Some(ErrorWire::coded(
                            "PROTOCOL_VIOLATION",
                            "an objects.push stream carried a chunk without an object",
                        ));
                        break;
                    };
                    received += bytes.len() as u64;
                    if let Err(outcome) = sink.offer(hash, chunk.offset, chunk.total, &bytes, 0)
                        && !seen.contains(hash)
                    {
                        outcomes.insert(hash.to_owned(), outcome);
                    }
                    sink.wait_below(PUSH_BUFFERED_BYTES, &|| responder.cancelled());
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
    let mut objects_down = 0;
    let mut answer = Vec::new();
    for item in &request.items {
        if !seen.remove(&item.hash) {
            continue;
        }
        let key = object_key(
            &ctx.installation,
            &peer.installation,
            &request.repo,
            false,
            &item.hash,
        );
        let (outcome, written) = match outcomes.remove(&item.hash) {
            Some(outcome) => (outcome, 0),
            None => match results.get(&item.hash) {
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
        match &outcome {
            ObjectOutcome::Completed => {
                objects_down += 1;
                if let Some(code) = object_completed(&key) {
                    ctx.status.record(StatusEvent::Resolved {
                        scope: Scope::Object {
                            state: bound.state.clone(),
                            hash: item.hash.clone(),
                        },
                        code,
                    });
                }
            }
            other if is_failure(other) => {
                let detail = format!("pushed by peer {}", peer.installation);
                if let Some(error) = outcome_error(&item.hash, other, &detail) {
                    object_failed(&key, &error.code);
                    ctx.status.record(StatusEvent::ObjectError {
                        state: bound.state.clone(),
                        hash: item.hash.clone(),
                        error,
                    });
                }
            }
            _ => {}
        }
        answer.push(ObjectResult {
            hash: item.hash.clone(),
            bytes: written,
            outcome,
        });
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

/// `objects.report{repo, sender, hash, code}`: our worker re-verifies its
/// copy (and quarantines it if it is corrupt); the reply is `{}`.
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
