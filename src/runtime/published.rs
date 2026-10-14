//! Legacy v6 `published-*` worker operations: bounded metadata exchange plus
//! object parts installed outside the semantic engine lock.
//!
//! WP2.3 (task 8):
//! * the engine is taken with `try_lock`, retried for up to
//!   [`ENGINE_RETRY`] of waiting behind other holders before `WORKER_BUSY`.
//!   The call counts as an engine waiter, so the background activator yields
//!   to it before its next slice, and time spent behind an activator slice
//!   (bounded, `EngineVitals::activator_holding`) is not charged: with the
//!   activator running, a legacy exchange is not refused because of it (up
//!   to [`ACTIVATOR_PATIENCE`]);
//! * while the background activator runs, a page's events are only staged
//!   (`Engine::background_activation`, budget 0) and the activator is poked;
//!   without it (the in-process `start_rpc` wrapper) the legacy bounded
//!   activation is unchanged (≤ 32 events per metadata exchange);
//! * every call that changed the store refreshes the status cell before it
//!   replies (read-your-writes; `status_generation` is bumped, and the content
//!   must catch up when the journal moved);
//! * CCR-WP1.6-4: a `published-apply` result keeps the `content` member that
//!   `paired_sync::worker_control` built (`{incoming, outgoing_unacknowledged}`,
//!   current after the parts are registered: neither counter depends on
//!   object installation), instead of being overwritten with an
//!   O(repository) `content_status()` scan of the legacy shape. Only the local
//!   lane reads it (`orchestrator::status::RepositoryStatus::succeeded` reads
//!   neither shape); it never travels to a peer.
use super::{Engine, EngineGuard, EngineVitals, ctx::WorkerCtx};
use crate::core::*;
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::Ordering,
    thread,
    time::{Duration, Instant},
};

/// How long a `published-*` call retries `try_lock` behind holders other
/// than the activator before `WORKER_BUSY`.
pub const ENGINE_RETRY: Duration = Duration::from_millis(100);
/// The longest a `published-*` call waits in all (activator slices included).
pub const ACTIVATOR_PATIENCE: Duration = Duration::from_secs(2);

/// The engine for a `published-*` call (module docs), else `WORKER_BUSY`
/// (Contention: the lane retries).
fn engine<'a>(ctx: &'a WorkerCtx, vitals: &EngineVitals) -> Result<EngineGuard<'a>> {
    let started = Instant::now();
    let _waiting = ctx.gauges.begin_wait();
    let mut charged = Duration::ZERO;
    let mut last = started;
    loop {
        if let Some(guard) = super::try_lock_engine(&ctx.engine) {
            return Ok(guard);
        }
        let now = Instant::now();
        if !vitals.activator_holding() {
            charged += now - last;
        }
        last = now;
        if charged >= ENGINE_RETRY || now - started >= ACTIVATOR_PATIENCE {
            anyhow::bail!(
                "WORKER_BUSY: the engine stayed busy for {} ms",
                (now - started).as_millis()
            );
        }
        thread::sleep(Duration::from_millis(1));
    }
}

/// The journal tip seen through the engine's connection.
fn tip(engine: &Engine) -> Result<i64> {
    Ok(engine
        .store
        .db
        .prepare_cached("SELECT coalesce(max(seq),0) FROM journal_sequence")?
        .query_row([], |r| r.get(0))?)
}

/// After a call that may have staged or committed: wake the activator, then
/// refresh the status cell (the content too when the journal moved).
fn changed(ctx: &WorkerCtx, vitals: &EngineVitals, journal_moved: bool) {
    if ctx.activator_running.load(Ordering::SeqCst) {
        ctx.activation.poke();
    }
    crate::worker::status_cell::note_mutation(ctx, vitals, journal_moved);
}

/// Metadata commits and acknowledgements never wait for object installation.
/// Bulk hashing/fsync uses a separate connection and lock; filesystem metadata
/// and control-lane calls remain able to acquire the semantic engine.
pub(super) fn published_rpc(
    ctx: &WorkerCtx,
    vitals: &EngineVitals,
    payload: &Value,
) -> Result<Value> {
    let mut metadata = payload.clone();
    // Bound semantic work per control exchange; queued events remain durable
    // and unacknowledged until later slices activate them.
    metadata["bounded_metadata"] = json!(payload["metadata_only"] == true);
    let mut parts = BTreeMap::new();
    if let Some(page) = metadata.get_mut("page") {
        let originals: crate::paired_sync::SyncPage = serde_json::from_value(page.clone())?;
        if payload["metadata_only"] == true {
            ensure!(
                originals.object_parts.is_empty() && originals.bundle.objects.is_empty(),
                "BULK_ON_CONTROL_LANE"
            );
        }
        parts = originals.object_parts;
        for (object, bytes) in originals.bundle.objects {
            let total = hex::decode(&bytes)?.len() as u64;
            parts.insert(
                object,
                ObjectPart {
                    offset: 0,
                    total,
                    hex: bytes,
                },
            );
        }
        page["object_parts"] = json!({});
        page["bundle"]["objects"] = json!({});
    }
    // An offer (no page) reads only.
    let writes = metadata.get("page").is_some();
    let (result, transfer, lock, moved) = {
        let mut e = engine(ctx, vitals)?;
        let before = tip(&e)?;
        let result = crate::paired_sync::worker_control(&mut e, &metadata);
        let moved = tip(&e).map_or(true, |after| after != before);
        if parts.is_empty() {
            drop(e);
            if writes {
                changed(ctx, vitals, moved);
            }
            return result;
        }
        let result = result?;
        (result, e.store.transfer_view()?, e.transfers.clone(), moved)
    };
    // The metadata is applied: visible to `status` before the (possibly
    // long) object installation below.
    changed(ctx, vitals, moved);
    let objects: Vec<_> = parts.keys().cloned().collect();
    let deadline = std::time::Instant::now() + crate::bulk_sync::RECEIVE_BUDGET;
    let received =
        transfer.receive_parts_until(parts, &[], &BTreeSet::new(), Some(&lock), Some(deadline));
    let outcomes = match received {
        Ok(outcomes) => outcomes,
        Err(error) => {
            // Page-level failure: nothing was received.
            let e = super::lock_engine(&ctx.engine);
            for object in objects {
                e.store.db.execute("INSERT INTO object_errors VALUES(?,?,?) ON CONFLICT(hash) DO UPDATE SET error=excluded.error,at=excluded.at", rusqlite::params![object, format!("{error:#}"), time()])?;
            }
            return Err(error);
        }
    };
    let (completed, failed) = crate::core::transfer::split_outcomes(&outcomes);
    {
        // Completed objects are registered first, even when another part
        // failed; then each failed object gets its error and backoff.
        let e = super::lock_engine(&ctx.engine);
        e.store.register_received(&completed)?;
        for (object, error) in &failed {
            e.store.db.execute("INSERT INTO object_errors VALUES(?,?,?) ON CONFLICT(hash) DO UPDATE SET error=excluded.error,at=excluded.at", rusqlite::params![object, error.message, time()])?;
            e.store.record_object_failure(object, error)?;
        }
    }
    // Installed objects reach the content counters at the next maintenance
    // refresh (no content scan per page, RC2); the counters move now.
    crate::worker::status_cell::note_mutation(ctx, vitals, false);
    if let Some((_, error)) = failed.into_iter().next() {
        return Err(error.into_anyhow());
    }
    let mut result = result;
    if result.get("known_objects").is_some() {
        result["known_objects"] = json!(transfer.shared_object_inventory()?);
        result["partial_objects"] = json!(transfer.partial_objects()?);
    }
    Ok(result)
}
