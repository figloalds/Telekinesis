//! Owned by WP2.7.
//!
//! One metadata exchange of this daemon (the dialer D) with a peer A for one
//! repository (REVAMP_PLAN §3.7, §6.3.5):
//!
//! 1. worker `feed-offer{route}` (10 s) builds the `MetaExchangeRequest`;
//! 2. peer `meta.exchange` (30 s) on the control session: header `{repo}`,
//!    body the JSON request; the reply body is the JSON `MetaExchangeReply`;
//! 3. worker `feed-apply{route, request_id, reply}` (10 s) stages the reply;
//! 4. `ExchangeStarted`/`ExchangeFinished` for the StatusHub, and
//!    `GateEvidence` to a folder whose mount gate is pending (the controller
//!    keeps only the gate source's evidence).
//!
//! The route comes from the local folder record and the authenticated peer's
//! catalog entry (I12): `{peer: A, repo, local_replica: D's device,
//! remote_replica: A's replica}`. The reply must answer this request
//! (`reply_to`, invariant 10); the worker checks it again. A request id never
//! goes out twice (invariant 16): a retry is a fresh `feed-offer`.
//!
//! Large replies (need serving, up to 16 MiB) travel as session body chunks
//! on the control connection only (invariant 18); the request and reply
//! bodies use the response queue, so `catalog.list` and heartbeats keep
//! priority.
use super::scheduler::{ExchangeError, Finished, Report, first_send};
use crate::{
    model::{
        errors::ErrorWire,
        ipc::{self, FeedApplyRequest, FeedOfferRequest},
        orchestrator::{
            CallError, Ctx, FolderCmd, FolderView, GateEvidence, GateState, PeerContext,
            PublishedEntry, limits,
        },
        protocol::{
            MetaExchangeHeader,
            feed::{
                ExchangeSummary, FEED_FORMAT, FeedApplyResult, GateView, MetaExchangeReply,
                MetaExchangeRequest, PageStatus,
            },
            frame::ChunkHeader,
            ops,
        },
    },
    paired_sync::SyncRoute,
    session::{Conn, Priority, Reply},
};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::time::Duration;

/// The status lane number of `meta.exchange` (the control session).
pub const LANE: u32 = super::scheduler::CONTROL_LANE;

/// The longest a `meta.exchange` call may take while its peer keeps
/// sending (its reply's frames): see [`ExchangeOptions::exchange`].
pub const EXCHANGE_CAP: Duration = Duration::from_secs(30 * 60);

/// Deadlines of the three steps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExchangeOptions {
    /// Worker `feed-offer` (10 s).
    pub offer: Duration,
    /// Peer `meta.exchange` (30 s): how long the peer may send nothing (an
    /// idle deadline: every frame of its reply restarts it; WP3.INT
    /// re-review), within [`EXCHANGE_CAP`].
    pub exchange: Duration,
    /// Worker `feed-apply` (10 s).
    pub apply: Duration,
}

impl Default for ExchangeOptions {
    fn default() -> Self {
        Self {
            offer: ipc::deadlines::FEED_OFFER,
            exchange: limits::EXCHANGE_DEADLINE,
            apply: ipc::deadlines::FEED_APPLY,
        }
    }
}

/// A completed exchange.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExchangeOutcome {
    /// The request id (`MetaExchangeRequest.request`).
    pub request: String,
    /// The worker's summary, with the wire bytes and the event counts this
    /// side saw filled in.
    pub summary: ExchangeSummary,
    /// The local worker's gate view of the peer.
    pub gate: GateView,
    /// How the peer took our page.
    pub page_status: PageStatus,
    /// A page continues on either side (`page.more`), or the acceptor asked
    /// for events our next request serves (`reply.need`).
    pub more: bool,
    /// The peer's page restarted its feed from the start.
    pub reset: bool,
    pub duration_ms: u64,
}

impl ExchangeOutcome {
    /// Something moved: events sent, received, staged or acknowledged, or a
    /// cursor advanced.
    pub fn moved(&self) -> bool {
        let s = &self.summary;
        s.sent_events > 0
            || s.received_events > 0
            || s.staged > 0
            || s.acked > 0
            || s.metadata_advanced
    }

    /// A cursor was corrected (mismatch or reset): the next exchange
    /// continues from the corrected position.
    pub fn recovered(&self) -> bool {
        self.page_status != PageStatus::Accepted || self.reset
    }
}

/// The route of an exchange of `folder` with `peer`'s `remote` entry.
pub fn route(peer: &PeerContext, folder: &FolderView, remote: &PublishedEntry) -> SyncRoute {
    SyncRoute {
        peer: peer.installation.clone(),
        repo: folder.record.repo.clone(),
        local_replica: folder.record.device.clone(),
        remote_replica: remote.replica.clone(),
    }
}

/// One metadata exchange with the frozen deadlines (§14 WP2.7 "Provides").
pub fn exchange(
    ctx: &Ctx,
    conn: &Conn,
    peer: &PeerContext,
    folder: &FolderView,
    remote: &PublishedEntry,
) -> Result<ExchangeSummary, CallError> {
    exchange_with(
        ctx,
        conn,
        peer,
        folder,
        remote,
        &ExchangeOptions::default(),
        &mut |_| None,
    )
    .map(|outcome| outcome.summary)
    .map_err(CallError::from)
}

/// [`exchange`] with explicit deadlines and the full outcome. `retry_after`
/// is asked for the delay to report with a failure (`retry_after_ms`).
pub fn exchange_with(
    ctx: &Ctx,
    conn: &Conn,
    peer: &PeerContext,
    folder: &FolderView,
    remote: &PublishedEntry,
    options: &ExchangeOptions,
    retry_after: &mut dyn FnMut(&ExchangeError) -> Option<Duration>,
) -> Result<ExchangeOutcome, ExchangeError> {
    let repo = folder.record.repo.clone();
    let report = Report::start(ctx, &peer.installation, &repo, LANE, ops::META_EXCHANGE);
    let mut finished = Finished::default();
    let result = run(ctx, conn, peer, folder, remote, options, &mut finished);
    let duration_ms = report.finish(result.as_ref().map(|_| &finished), retry_after);
    let mut outcome = result?;
    outcome.duration_ms = duration_ms;
    if matches!(
        folder.gate,
        GateState::Awaiting { .. } | GateState::Blocked { .. }
    ) {
        let evidence = gate_evidence(&peer.installation, ctx.clock.now_ms(), &outcome);
        let _ = ctx
            .folders
            .send(&folder.record.state, FolderCmd::GateEvidence(evidence));
    }
    Ok(outcome)
}

/// The `GateEvidence` of an exchange with `installation`. The controller
/// reads `caught_up && !blocked` as satisfied and `caught_up && blocked` as
/// the Blocked gate ("caught up, but blocked, quarantined or rejected rows
/// remain"), so `caught_up` here is the gate view's claim, or, when the gate
/// view is blocked, the metadata claim ([`evidence_of`]).
pub fn gate_evidence(installation: &str, at_ms: u64, outcome: &ExchangeOutcome) -> GateEvidence {
    evidence_of(
        installation,
        at_ms,
        &outcome.gate,
        outcome.summary.caught_up,
    )
}

/// The `GateEvidence` a feed exchange's gate view stands for, in either
/// direction: the dialer's `feed-apply` result ([`gate_evidence`]) and the
/// acceptor's own `feed-exchange` `local` view (the inbound server). A
/// `GateView`'s `caught_up` is the full gate claim (§6.3.6), so it is false
/// whenever `blocked` holds; the controller's Blocked gate is `caught_up &&
/// blocked`, so `caught_up` here is the gate claim, or, when the view is
/// blocked, `metadata_caught_up` (the summary's `caught_up`). One helper for
/// both sides (post-integration re-review, WP2.INT: the inbound server
/// forwarded the raw view, so an acceptor-side gate never reached Blocked).
pub fn evidence_of(
    installation: &str,
    at_ms: u64,
    gate: &GateView,
    metadata_caught_up: bool,
) -> GateEvidence {
    GateEvidence {
        installation: installation.to_owned(),
        at_ms,
        caught_up: gate.caught_up || (gate.blocked && metadata_caught_up),
        blocked: gate.blocked,
        waiting_on: gate.waiting_on.clone(),
    }
}

fn run(
    ctx: &Ctx,
    conn: &Conn,
    peer: &PeerContext,
    folder: &FolderView,
    remote: &PublishedEntry,
    options: &ExchangeOptions,
    finished: &mut Finished,
) -> Result<ExchangeOutcome, ExchangeError> {
    let local = ExchangeError::local;
    let route = route(peer, folder, remote);
    if remote.repo != route.repo {
        return Err(local(invalid(
            "REPOSITORY_MISMATCH",
            format!("the peer entry is for {}, not {}", remote.repo, route.repo),
        )));
    }
    let client = ctx.folders.client(&folder.record.state).ok_or_else(|| {
        local(CallError::Unavailable(format!(
            "LOCAL_WORKER_UNAVAILABLE: folder {} has no running worker",
            folder.record.state
        )))
    })?;

    // 1. The local offer.
    let offer = client
        .call(
            ipc::ops::FEED_OFFER,
            to_value(&FeedOfferRequest {
                route: route.clone(),
            }),
            options.offer,
        )
        .map_err(local)?;
    let request: MetaExchangeRequest = parse(offer, ipc::ops::FEED_OFFER).map_err(local)?;
    check_request(&request, &route).map_err(local)?;
    if !first_send(&request.request) {
        return Err(local(invalid(
            "REQUEST_ID_REUSED",
            format!(
                "feed-offer returned request {}, which was already sent; it is never sent twice",
                request.request
            ),
        )));
    }

    // 2. The peer.
    let body = serde_json::to_vec(&request)
        .map_err(|error| local(invalid("INVALID_SYNC_REQUEST", error)))?;
    finished.bytes_up = body.len() as u64;
    let header = to_value(&MetaExchangeHeader {
        repo: route.repo.clone(),
    });
    // `options.exchange` bounds how long the peer may stay silent, not the
    // whole exchange (WP3.INT re-review): a large event's payload, served in
    // the reply (up to `MAX_EVENT`, 8 MiB; `need` up to 16 MiB), may take
    // minutes on a slow link, and an absolute deadline failed every attempt,
    // each sending the same bytes again. Every frame of the reply resets the
    // idle deadline; [`EXCHANGE_CAP`] bounds the whole call.
    let mut no_stream = |chunk: ChunkHeader, _: &[u8]| -> Result<(), ErrorWire> {
        Err(ErrorWire::coded(
            "PROTOCOL_VIOLATION",
            format!("meta.exchange request {} received a stream chunk", chunk.id),
        ))
    };
    let reply = conn
        .call_streaming(
            ops::META_EXCHANGE,
            header,
            body,
            Priority::Response,
            Some(EXCHANGE_CAP.max(options.exchange)),
            options.exchange,
            &mut no_stream,
        )
        .map_err(ExchangeError::peer)?;
    finished.bytes_down = reply.body.len() as u64;
    let reply: MetaExchangeReply =
        body_of(&reply, ops::META_EXCHANGE).map_err(ExchangeError::peer)?;
    if reply.reply_to != request.request {
        return Err(ExchangeError::peer(invalid(
            "UNBOUND_PEER_RESPONSE",
            format!(
                "the meta.exchange reply answers {}, not {}",
                reply.reply_to, request.request
            ),
        )));
    }
    if reply.repo != route.repo {
        return Err(ExchangeError::peer(invalid(
            "REPOSITORY_MISMATCH",
            format!(
                "the meta.exchange reply is for {}, not {}",
                reply.repo, route.repo
            ),
        )));
    }
    let sent = payload_entries(&request.page.entries);
    let received =
        payload_entries(&reply.page.entries).saturating_add(len_u32(reply.need_served.len()));
    let page_status = reply.page_status.clone();
    // A need of the acceptor's is more work too (CCR-WP2.5-2, applied by
    // WP3.1): its events go in our next request (`need_served`), so the
    // scheduler books that request now instead of after the idle interval.
    // No loop: an id we do not hold comes back `unknown` and is not asked
    // again for `NEED_RETRY_MS`, and an exchange that moves nothing books
    // no follow-up whatever `more` says.
    let more = request.page.more || reply.page.more || !reply.need.is_empty();
    let reset = reply.page.reset;

    // 3. The local apply.
    let applied = client
        .call(
            ipc::ops::FEED_APPLY,
            to_value(&FeedApplyRequest {
                route,
                request_id: request.request.clone(),
                reply,
            }),
            options.apply,
        )
        .map_err(local)?;
    let applied: FeedApplyResult = parse(applied, ipc::ops::FEED_APPLY).map_err(local)?;
    let mut summary = applied.summary;
    summary.bytes_up = finished.bytes_up;
    summary.bytes_down = finished.bytes_down;
    summary.sent_events = summary.sent_events.max(sent);
    summary.received_events = summary.received_events.max(received);
    finished.summary = Some(summary.clone());
    Ok(ExchangeOutcome {
        request: request.request,
        summary,
        gate: applied.gate,
        page_status,
        more,
        reset,
        duration_ms: 0,
    })
}

/// The offer must be a format 7 request for this route with a uuid id.
fn check_request(request: &MetaExchangeRequest, route: &SyncRoute) -> Result<(), CallError> {
    if request.format != FEED_FORMAT {
        return Err(invalid(
            "INVALID_SYNC_REQUEST",
            format!(
                "feed-offer returned format {}, not {FEED_FORMAT}",
                request.format
            ),
        ));
    }
    if request.repo != route.repo
        || request.sender != route.local_replica
        || request.receiver != route.remote_replica
    {
        return Err(invalid(
            "LOCAL_REPLICA_BINDING_MISMATCH",
            format!(
                "feed-offer returned a request from {} to {} for {}, not from {} to {} for {}",
                request.sender,
                request.receiver,
                request.repo,
                route.local_replica,
                route.remote_replica,
                route.repo
            ),
        ));
    }
    if uuid::Uuid::parse_str(&request.request).is_err() {
        return Err(invalid(
            "INVALID_SYNC_REQUEST_ID",
            format!("feed-offer returned request id {:?}", request.request),
        ));
    }
    Ok(())
}

fn payload_entries(entries: &[crate::model::protocol::feed::FeedEntry]) -> u32 {
    len_u32(entries.iter().filter(|entry| entry.event.is_some()).count())
}

fn len_u32(n: usize) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}

/// A Remote error with `code` (the connection, or worker, stays usable).
pub(crate) fn invalid(code: &str, detail: impl std::fmt::Display) -> CallError {
    CallError::Remote(ErrorWire::coded(code, detail))
}

pub(crate) fn to_value<T: serde::Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

/// A worker reply as `T`.
pub(crate) fn parse<T: DeserializeOwned>(value: Value, op: &str) -> Result<T, CallError> {
    serde_json::from_value(value).map_err(|error| {
        invalid(
            "INVALID_REPLY",
            format!("{op} returned an unexpected reply: {error}"),
        )
    })
}

/// A peer reply as `T`: the JSON body, or (a body-less reply) the header.
pub(crate) fn body_of<T: DeserializeOwned>(reply: &Reply, op: &str) -> Result<T, CallError> {
    let parsed = if reply.body.is_empty() {
        serde_json::from_value(reply.header.clone())
    } else {
        serde_json::from_slice(&reply.body)
    };
    parsed.map_err(|error| {
        invalid(
            "INVALID_REPLY",
            format!("{op} returned an unexpected reply: {error}"),
        )
    })
}
