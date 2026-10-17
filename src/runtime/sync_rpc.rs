//! Owned by WP2.5.
//!
//! Worker `feed-offer`, `feed-exchange` and `feed-apply` (REVAMP_PLAN §6.1,
//! §6.3.5): the worker half of the format 7 metadata feed. Each op checks the
//! route against this worker (codes as in `paired_sync::binding`), runs on a
//! pooled secondary store (`ctx.sync_stores`) and never waits for the engine
//! lock; afterwards it refreshes the peer's `ctx.sync_obs` counters, bumps
//! `ctx.status_generation` (when the exchange moved anything, WP4.INT) and
//! pokes `ctx.activation`.
//!
//! Replies are the typed `model` messages as JSON, with the `"event": null`
//! and `"large": null` members of feed entries left out (both are
//! `#[serde(default)]`): an elided entry is then about 90 bytes (seq, id and
//! digest) instead of about 115 (CCR-WP2.5-3 asks the model to skip them).
//!
//! **Stand-in activation.** Staging never activates; WP2.3's background
//! activator does, and sets `ctx.activator_running`. While no activator runs
//! (a Wave 1 worker, or the in-process `start_rpc` wrapper), a feed exchange
//! that left activatable rows, and nothing more to receive from the peer for
//! now, activates them in one bounded pass, as the legacy lanes do: only if
//! the engine lock is free (`try_lock_engine`, never a wait), at most
//! [`STAND_IN_BUDGET`], and paused like the activator while its fault point
//! (`worker.activator.slice`) is armed.
use super::{ctx::WorkerCtx, try_lock_engine};
use crate::{
    core::{Bundle, Store, time},
    model::{
        errors::code_of,
        health::PeerFeedCounters,
        ipc::{FeedApplyRequest, FeedExchangeReply, FeedExchangeRequest, FeedOfferRequest, ops},
        protocol::feed::{FeedApplyResult, MetaExchangeReply, MetaExchangeRequest},
    },
    paired_sync::SyncRoute,
    util::{fault, lock::lock},
};
use anyhow::{Result, anyhow, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

/// The activator's fault point (named by WP2.3); armed, it also pauses the
/// stand-in activation.
const ACTIVATOR_POINT: &str = "worker.activator.slice";
/// Activation work one feed RPC may do while no activator runs.
pub const STAND_IN_BUDGET: Duration = Duration::from_millis(100);
/// Attempts per stand-in slice (the legacy lanes' bound).
const STAND_IN_SLICE: usize = 64;

pub fn dispatch(ctx: &WorkerCtx, op: &str, payload: &Value) -> Result<Value> {
    match op {
        ops::FEED_OFFER => {
            let FeedOfferRequest { route } = request(op, payload)?;
            bind(ctx, &route)?;
            let store = ctx.sync_stores.checkout()?;
            wire(&store.build_exchange_request(&route)?)
        }
        ops::FEED_EXCHANGE => {
            let FeedExchangeRequest { route, request } = request(op, payload)?;
            bind(ctx, &route)?;
            let (reply, local) = {
                let store = ctx.sync_stores.checkout()?;
                let result = store.handle_exchange_request(&route, &request)?;
                observe(ctx, &store, &route, &result.1);
                result
            };
            after_exchange(ctx, &local);
            wire(&FeedExchangeReply { reply, local })
        }
        ops::FEED_APPLY => {
            let FeedApplyRequest {
                route,
                request_id,
                reply,
            } = request(op, payload)?;
            bind(ctx, &route)?;
            let result = {
                let store = ctx.sync_stores.checkout()?;
                let result = store.apply_exchange_reply(&route, &request_id, &reply)?;
                observe(ctx, &store, &route, &result);
                result
            };
            after_exchange(ctx, &result);
            Ok(json!(result))
        }
        _ => bail!("UNSUPPORTED_OPERATION: {op}"),
    }
}

/// The op's typed request; a payload that does not parse is answered with
/// [`malformed`]'s code.
fn request<'a, T: Deserialize<'a>>(op: &str, payload: &'a Value) -> Result<T> {
    T::deserialize(payload).map_err(|error| {
        anyhow!(
            "{}: {op}: malformed request: {error}",
            malformed(op, payload)
        )
    })
}

/// The code of a feed op payload that does not parse:
/// - `PROTOCOL_VIOLATION` when only the peer's message does not parse (the
///   `request` of `feed-exchange`, the `reply` of `feed-apply`);
/// - `INVALID_REQUEST` otherwise (the orchestrator's own members), also for
///   a `feed-offer` whose `route` does not parse (CCR-WP2.5-1, applied by
///   WP2.INT: the Wave 0 stub's `UNSUPPORTED_OPERATION` is gone).
fn malformed(op: &str, payload: &Value) -> &'static str {
    // Whether member `key` is present and `check` holds for it.
    let member = |key: &str, check: fn(&Value) -> bool| payload.get(key).is_some_and(check);
    let route = member("route", |v| SyncRoute::deserialize(v).is_ok());
    let peer_message_bad = match op {
        ops::FEED_EXCHANGE => member("request", |v| MetaExchangeRequest::deserialize(v).is_err()),
        ops::FEED_APPLY => {
            member("request_id", Value::is_string)
                && member("reply", |v| MetaExchangeReply::deserialize(v).is_err())
        }
        _ => false,
    };
    if route && peer_message_bad {
        "PROTOCOL_VIOLATION"
    } else {
        "INVALID_REQUEST"
    }
}

/// The route must name this worker (`paired_sync::binding`'s codes).
fn bind(ctx: &WorkerCtx, route: &SyncRoute) -> Result<()> {
    ensure!(route.repo == ctx.repo, "REPOSITORY_MISMATCH");
    ensure!(
        route.local_replica == ctx.device,
        "LOCAL_REPLICA_BINDING_MISMATCH"
    );
    ensure!(
        route.local_replica != route.remote_replica,
        "DUPLICATE_REPLICA_IDENTITY"
    );
    Ok(())
}

/// `value` as JSON with null `event`/`large` entry members left out.
pub fn wire<T: Serialize>(value: &T) -> Result<Value> {
    let mut value = serde_json::to_value(value)?;
    compact(&mut value);
    Ok(value)
}

/// Drops `"event": null` and `"large": null` from the feed entries of an
/// exchange message: a request (`page`), a reply (`page`) or a
/// `feed-exchange` answer (`reply.page`). Both members default to `None`.
pub fn compact(value: &mut Value) {
    compact_page(value.get_mut("page"));
    compact_page(value.pointer_mut("/reply/page"));
}

fn compact_page(page: Option<&mut Value>) {
    let Some(entries) = page
        .and_then(|page| page.get_mut("entries"))
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    for entry in entries.iter_mut().filter_map(Value::as_object_mut) {
        for key in ["event", "large"] {
            if entry.get(key).is_some_and(Value::is_null) {
                entry.remove(key);
            }
        }
    }
}

/// Refresh the peer's counters in `ctx.sync_obs` (from the exchange result,
/// stamped now) and, when the exchange moved anything, mark the status cell
/// stale (`status_generation`, which moves `generations.sync`). A failure
/// here never fails the exchange.
///
/// An exchange that moved nothing (no event sent, received, staged, rejected
/// or acknowledged, the metadata not advanced, the peer's counters equal but
/// for their time stamp) bumps nothing (CCR-WP4.2-5, applied by WP4.INT): an
/// idle replica's feed exchanges every sync interval would otherwise move
/// `generations.sync` every time, and each move triggers a download round on
/// every bulk lane (objects of staged events are wanted before activation),
/// an O(objects) want at this worker each. The counters' new time stamp
/// still goes out with the next regular heartbeat (1 s).
fn observe(ctx: &WorkerCtx, store: &Store, route: &SyncRoute, result: &FeedApplyResult) {
    let summary = &result.summary;
    let mut moved = summary.sent_events > 0
        || summary.received_events > 0
        || summary.staged > 0
        || summary.rejected > 0
        || summary.acked > 0
        || summary.metadata_advanced;
    match store.feed_counters_after(&route.remote_replica, result) {
        Ok(mut counters) => {
            counters.last_exchange_ms = Some(time());
            let previous =
                lock(&ctx.sync_obs).insert(route.remote_replica.clone(), counters.clone());
            moved |= previous.is_none_or(|previous| {
                PeerFeedCounters {
                    last_exchange_ms: None,
                    ..previous
                } != PeerFeedCounters {
                    last_exchange_ms: None,
                    ..counters
                }
            });
        }
        Err(error) => {
            moved = true;
            crate::util::log::event!(warn, (code_of(&error).as_str()), {
                "site": "worker.feed.counters",
                "peer": route.remote_replica,
                "message": format!("{error:#}"),
            });
        }
    }
    if moved {
        ctx.status_generation.fetch_add(1, Ordering::SeqCst);
    }
}

fn after_exchange(ctx: &WorkerCtx, result: &FeedApplyResult) {
    ctx.activation.poke();
    // While the peer's feed still streams (a multi-page catch-up), staging
    // goes first; the stand-in activates once nothing is left to receive.
    if !ctx.activator_running.load(Ordering::SeqCst) && result.summary.to_receive == 0 {
        stand_in_activation(ctx, result.summary.staged > 0);
    }
}

/// Activate ready rows while no background activator runs (module docs):
/// slices of [`STAND_IN_SLICE`] attempts through the engine (which also
/// refreshes clean sessions and queues namespace notifications), for at
/// most [`STAND_IN_BUDGET`].
fn stand_in_activation(ctx: &WorkerCtx, staged: bool) {
    if !staged && !activatable(ctx) {
        return;
    }
    if fault::hit(ACTIVATOR_POINT).is_some() {
        return;
    }
    let Some(mut engine) = try_lock_engine(&ctx.engine) else {
        return;
    };
    let deadline = Instant::now() + STAND_IN_BUDGET;
    loop {
        let empty = Bundle {
            repo: engine.store.repo.clone(),
            branches: vec![],
            events: vec![],
            objects: Default::default(),
        };
        match engine.receive_peer_budget(empty, &BTreeSet::new(), Some(STAND_IN_SLICE)) {
            Ok(activated) if !activated.is_empty() && Instant::now() < deadline => {}
            Ok(_) => break,
            Err(error) => {
                crate::util::log::event!(warn, (code_of(&error).as_str()), {
                    "site": "worker.feed.activation",
                    "message": format!("{error:#}"),
                });
                break;
            }
        }
    }
}

/// Whether a ready (or due retry) inbox row exists (a point query).
fn activatable(ctx: &WorkerCtx) -> bool {
    let Ok(store) = ctx.sync_stores.checkout() else {
        return false;
    };
    store
        .db
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM incoming_events WHERE state='ready')
                OR EXISTS(SELECT 1 FROM incoming_events WHERE state='retry' AND retry_at_ms<=?)",
            [time() as i64],
            |r| r.get::<_, bool>(0),
        )
        .unwrap_or(false)
}
