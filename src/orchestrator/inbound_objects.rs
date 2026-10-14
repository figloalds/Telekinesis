//! Owned by WP2.8 and WP3.3 — inbound `objects.*` (REVAMP_PLAN §3.8, §6.5).
//!
//! Requests on an authenticated **bulk** session. Wave 2 serves the interim
//! `objects.exchange` (§6.5): the dialer's legacy `bulk_sync::Page` (bulk
//! protocol 1, v6 JSON) goes to the folder's worker as
//! `published-bulk-exchange`, and the worker's offer page is the reply.
//! `objects.pull`, `objects.wants`, `objects.push` and `objects.report`
//! answer `UNSUPPORTED_OPERATION` until WP3.3 (objects v3). Every error is a
//! Remote error: the session stays usable (invariant 17).
//!
//! **Checks, in order** (none of the refusals reaches a worker):
//! 1. the session is a bulk session (`BULK_ON_CONTROL_LANE`; the inbound
//!    router checks this first too);
//! 2. the page parses (`INVALID_REQUEST`) and its lane is the session's own
//!    lane, the one the handshake authenticated (`BULK_LANE_MISMATCH`), so
//!    one lane never carries another lane's objects (invariant 19);
//! 3. the repository is managed (`REPOSITORY_NOT_MANAGED`), its sync is not
//!    paused (`FOLDER_SYNC_PAUSED`) and its worker is Running
//!    (`REMOTE_WORKER_STARTING`);
//! 4. the route is `{peer: authenticated installation, repo, local_replica:
//!    folder device, remote_replica: page.sender}` (invariant 15); the worker
//!    checks the page against it (`BULK_ROUTE_MISMATCH`, `OBJECT_ON_WRONG_LANE`, …).
//!
//! **Wire shape.** The request page is the JSON body (or, when the body is
//! empty, the request header); the reply is the header `{repo}` and the
//! worker's page as the JSON body. The session bounds an `objects.exchange`
//! body at `protocol::max_body` (16 MiB since WP2.INT applied CCR-WP2.8-1;
//! it was 1 MiB, below the 4 MiB pages `published-bulk-*` may build), so a
//! reply page above the bound is trimmed: object parts are dropped (the last
//! ones first) until it fits. Offers are stateless, so a dropped part is
//! simply offered again in a later round.
//!
//! **Status.** Every exchange a worker served records `ExchangeFinished
//! {op: objects.exchange, direction: inbound}`: the object bytes received
//! (`bytes_down`) and sent (`bytes_up`), and the objects the request page
//! completed (`objects_down`; the worker installs and registers each one or
//! fails the call). Objects sent are not counted as completed (`objects_up:
//! 0`): only the peer knows whether it installed them.
use super::inbound::{
    Failed, Finished, deadline_error, invalid_request, record_finished, resolve, worker_deadline,
};
use crate::{
    bulk_sync::Page,
    model::{
        errors::ErrorWire,
        orchestrator::Ctx,
        protocol::{self, LaneRole, deadlines, ops},
    },
    session,
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Instant};

/// The worker op that serves an inbound interim bulk page.
const BULK_EXCHANGE: &str = "published-bulk-exchange";

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
        _ => {
            let session::Incoming { op, responder, .. } = req;
            responder.error(ErrorWire::coded(
                "UNSUPPORTED_OPERATION",
                format!("{op} is served from objects v3 (Wave 3)"),
            ));
        }
    }
}

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
