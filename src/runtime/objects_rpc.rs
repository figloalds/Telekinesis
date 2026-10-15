//! Owned by WP3.2.
//!
//! Worker `objects.want`, `objects.read`, `objects.write` and `objects.report`
//! (REVAMP_PLAN §6.4): the worker half of objects v3. Each op checks its route
//! against this worker (the `paired_sync::binding` codes, as the feed ops do),
//! runs on a pooled secondary store (`ctx.sync_stores`) and never takes the
//! engine lock. Bodies are binary (`objects.read` replies with the bytes,
//! `objects.write` carries the parts' bytes concatenated in item order), so
//! `read` and `write` need a v3 loopback session (`runtime::server`); `want`
//! and `report` also answer over the JSON RPC.
//!
//! **Leases** (invariant 19, with `LaneSpec::owns` and the per-object locks):
//! `ctx.inflight_objects` maps a hash to the session that may transfer it,
//! for `LEASE_TTL` (60 s) renewed by every write that makes progress. A want
//! list never offers an object another session holds, and takes a lease on
//! every item it returns; a write for an object another session holds is
//! `InFlightElsewhere` and touches nothing. A write for a free (or expired)
//! lease takes it, so a session whose lease lapsed during a slow transfer
//! continues. Mutual pairing and several peers therefore move each object's
//! bytes once.
//!
//! **Ordered parts:** a session may keep several writes of one object in
//! flight; handler threads can run them out of order. A write whose first
//! part starts past the object's durable offset waits (at most
//! `ORDER_WAIT`) for this session's earlier writes to advance it before the
//! store answers `RESUME_OFFSET_AHEAD`.
//!
//! **Per-source backoff:** a completed spool that does not hash to its object
//! is `CorruptAtSource`; the receiver then backs that object off for that
//! source (the route's remote replica) with the object backoff (30 s → 1 h),
//! while other sources may still supply it. The orchestrator reports it to the
//! source (`objects.report`), which re-verifies and quarantines its copy.
use super::ctx::{ObjectLease, WorkerCtx};
use crate::{
    model::{
        errors::ErrorWire,
        protocol::objects::{
            LEASE_TTL, MAX_READ_LEN, MAX_WANT_ITEMS, ObjectOutcome, ReadHeader, ReadReply,
            ReportHeader, ReportReply, WantHeader, WantReply, WriteHeader, WriteItem, WriteReply,
            WriteResult, ops,
        },
    },
    paired_sync::SyncRoute,
    util::{backoff::Backoff, lock::lock, time::now_ms},
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    path::PathBuf,
    sync::{Condvar, Mutex, atomic::Ordering},
    time::{Duration, Instant},
};

/// How long a write whose part starts past the durable offset waits for this
/// session's earlier writes of the object (see the module docs).
pub const ORDER_WAIT: Duration = Duration::from_secs(2);
/// Per-source backoff entries kept (expired ones are dropped first).
const MAX_SOURCE_BACKOFFS: usize = 16 * 1024;

/// Wakes writes waiting for an object's durable offset (`ORDER_WAIT`): bumped
/// after every write call.
static PROGRESS: (Mutex<u64>, Condvar) = (Mutex::new(0), Condvar::new());

/// `(store root, source replica, hash)` → `(attempts, retry_at_ms)`.
type SourceKey = (PathBuf, String, String);
static SOURCE_BACKOFF: Mutex<BTreeMap<SourceKey, (u32, u64)>> = Mutex::new(BTreeMap::new());

pub fn dispatch(
    ctx: &WorkerCtx,
    op: &str,
    header: &Value,
    body: &[u8],
) -> Result<(Value, Vec<u8>), ErrorWire> {
    match op {
        ops::WANT => want(ctx, parse(op, header)?),
        ops::READ => read(ctx, parse(op, header)?),
        ops::WRITE => write(ctx, parse(op, header)?, body),
        ops::REPORT => report(ctx, parse(op, header)?),
        _ => Err(ErrorWire::coded("UNSUPPORTED_OPERATION", op)),
    }
}

fn parse<T: DeserializeOwned>(op: &str, header: &Value) -> Result<T, ErrorWire> {
    T::deserialize(header).map_err(|error| {
        ErrorWire::coded(
            "INVALID_REQUEST",
            format!("{op}: malformed header: {error}"),
        )
    })
}

/// The route must name this worker (`paired_sync::binding`'s codes).
fn bind(ctx: &WorkerCtx, route: &SyncRoute) -> Result<(), ErrorWire> {
    if route.repo != ctx.repo {
        return Err(ErrorWire::coded(
            "REPOSITORY_MISMATCH",
            format!("route repository {}", route.repo),
        ));
    }
    if route.local_replica != ctx.device {
        return Err(ErrorWire::coded(
            "LOCAL_REPLICA_BINDING_MISMATCH",
            format!("route local replica {}", route.local_replica),
        ));
    }
    if route.local_replica == route.remote_replica {
        return Err(ErrorWire::coded(
            "DUPLICATE_REPLICA_IDENTITY",
            "the route's remote replica is this replica",
        ));
    }
    Ok(())
}

fn session_id(session: &str) -> Result<(), ErrorWire> {
    if session.is_empty() || session.len() > 128 || session.chars().any(char::is_control) {
        return Err(ErrorWire::coded(
            "INVALID_REQUEST",
            "session must be 1..=128 bytes without control characters",
        ));
    }
    Ok(())
}

fn wire(error: anyhow::Error) -> ErrorWire {
    ErrorWire::from_error(&error)
}

fn store(ctx: &WorkerCtx) -> Result<crate::runtime::ctx::PooledStore, ErrorWire> {
    ctx.sync_stores.checkout().map_err(wire)
}

/// Whether `lease` is held by a session other than `session` and still live.
fn held_elsewhere(lease: Option<&ObjectLease>, session: &str, now: u64) -> bool {
    lease.is_some_and(|lease| lease.session != session && lease.expires_ms > now)
}

/// The retry time of `hash` from `source` while it is backed off.
fn source_backoff(ctx: &WorkerCtx, source: &str, hash: &str, now: u64) -> Option<u64> {
    lock(&SOURCE_BACKOFF)
        .get(&(ctx.root.clone(), source.to_owned(), hash.to_owned()))
        .map(|&(_, retry_at)| retry_at)
        .filter(|retry_at| *retry_at > now)
}

/// Backs `hash` off for `source` (30 s → 1 h by attempt). Returns the retry time.
pub fn back_off_source(ctx: &WorkerCtx, source: &str, hash: &str) -> u64 {
    let now = now_ms();
    let mut map = lock(&SOURCE_BACKOFF);
    if map.len() >= MAX_SOURCE_BACKOFFS {
        map.retain(|_, (_, retry_at)| *retry_at > now);
        if map.len() >= MAX_SOURCE_BACKOFFS {
            map.clear();
        }
    }
    let entry = map
        .entry((ctx.root.clone(), source.to_owned(), hash.to_owned()))
        .or_insert((0, now));
    entry.0 = entry.0.saturating_add(1);
    let delay = Backoff::objects().nominal(entry.0);
    entry.1 = now.saturating_add(u64::try_from(delay.as_millis()).unwrap_or(u64::MAX));
    entry.1
}

/// `objects.want{route, lane, max_items, max_bytes, session}` → `{items}`;
/// every returned item is leased to `session`.
fn want(ctx: &WorkerCtx, header: WantHeader) -> Result<(Value, Vec<u8>), ErrorWire> {
    bind(ctx, &header.route)?;
    session_id(&header.session)?;
    if !header.lane.is_valid() {
        return Err(ErrorWire::coded(
            "INVALID_BULK_LANE",
            format!("lane {}/{}", header.lane.index, header.lane.count),
        ));
    }
    let now = now_ms();
    // A snapshot: the store's scan runs without the lease lock.
    let leased: HashSet<String> = lock(&ctx.inflight_objects)
        .iter()
        .filter(|(_, lease)| held_elsewhere(Some(lease), &header.session, now))
        .map(|(hash, _)| hash.clone())
        .collect();
    let backed_off: HashSet<String> = {
        let mut map = lock(&SOURCE_BACKOFF);
        map.retain(|_, (_, retry_at)| *retry_at > now.saturating_sub(3_600_000));
        map.iter()
            .filter(|((root, source, _), (_, retry_at))| {
                *root == ctx.root && *source == header.route.remote_replica && *retry_at > now
            })
            .map(|((_, _, hash), _)| hash.clone())
            .collect()
    };
    let mut items = store(ctx)?
        .want_objects(
            header.lane,
            (header.max_items as usize).min(MAX_WANT_ITEMS),
            header.max_bytes,
            &|hash| leased.contains(hash) || backed_off.contains(hash),
        )
        .map_err(wire)?;
    {
        let now = now_ms();
        let mut leases = lock(&ctx.inflight_objects);
        leases.retain(|_, lease| lease.expires_ms > now);
        items.retain(|item| {
            if held_elsewhere(leases.get(&item.hash), &header.session, now) {
                return false;
            }
            leases.insert(
                item.hash.clone(),
                ObjectLease {
                    session: header.session.clone(),
                    expires_ms: now + LEASE_TTL.as_millis() as u64,
                    offset: item.offset,
                },
            );
            true
        });
    }
    Ok((json!(WantReply { items }), vec![]))
}

/// `objects.read{route, hash, offset, len ≤ 1 MiB}` → `{total}` / bytes.
fn read(ctx: &WorkerCtx, header: ReadHeader) -> Result<(Value, Vec<u8>), ErrorWire> {
    bind(ctx, &header.route)?;
    let len = header.len.min(MAX_READ_LEN) as usize;
    let (total, bytes) = store(ctx)?.read_object_range(&header.hash, header.offset, len)?;
    Ok((json!(ReadReply { total }), bytes))
}

/// `objects.report{route, hash, code}` → `{action}`.
fn report(ctx: &WorkerCtx, header: ReportHeader) -> Result<(Value, Vec<u8>), ErrorWire> {
    bind(ctx, &header.route)?;
    if !matches!(
        header.code.as_str(),
        "CORRUPT_AT_SOURCE" | "CORRUPT_OBJECT" | "SIZE_MISMATCH"
    ) {
        return Err(ErrorWire::coded(
            "INVALID_REQUEST",
            format!("objects.report: unknown code {}", header.code),
        ));
    }
    let action = store(ctx)?.report_corrupt(&header.hash).map_err(wire)?;
    crate::util::log::event!(warn, CORRUPT_AT_SOURCE, {
        "site": "worker.objects.report",
        "hash": header.hash,
        "reporter": header.route.remote_replica,
        "action": action,
    });
    if action == crate::model::protocol::objects::ReportAction::Quarantined {
        crate::worker::status_cell::want_content(ctx);
    }
    Ok((json!(ReportReply { action }), vec![]))
}

/// `objects.write{route, session, items}` + body → `{results}` (one per item,
/// in item order).
fn write(ctx: &WorkerCtx, header: WriteHeader, body: &[u8]) -> Result<(Value, Vec<u8>), ErrorWire> {
    bind(ctx, &header.route)?;
    session_id(&header.session)?;
    let WriteHeader {
        route,
        session,
        items,
    } = header;
    let described = items
        .iter()
        .try_fold(0u64, |sum, item| sum.checked_add(item.len));
    if described != Some(body.len() as u64) {
        return Err(ErrorWire::coded(
            "INVALID_OBJECT_PART",
            format!(
                "the items describe {described:?} bytes, the body has {}",
                body.len()
            ),
        ));
    }
    let store = store(ctx)?;
    // Leases: refuse objects another session holds; take free ones. The
    // committed offsets are read first, never under the lease lock.
    let hashes: BTreeSet<&str> = items.iter().map(|item| item.hash.as_str()).collect();
    let committed: HashMap<&str, u64> = hashes
        .iter()
        .map(|hash| {
            let durable = store
                .spool_state(hash)
                .ok()
                .flatten()
                .map_or(0, |s| s.durable);
            (*hash, durable)
        })
        .collect();
    let mut refused: HashSet<&str> = HashSet::new();
    {
        let now = now_ms();
        let mut leases = lock(&ctx.inflight_objects);
        for hash in &hashes {
            if held_elsewhere(leases.get(*hash), &session, now) {
                refused.insert(hash);
                continue;
            }
            let offset = match leases.get(*hash) {
                Some(lease) if lease.session == session => lease.offset,
                _ => committed.get(hash).copied().unwrap_or(0),
            };
            leases.insert(
                (*hash).to_owned(),
                ObjectLease {
                    session: session.clone(),
                    expires_ms: now + LEASE_TTL.as_millis() as u64,
                    offset,
                },
            );
        }
    }
    // Ordered parts: wait for this session's earlier writes (bounded).
    let mut first: HashMap<&str, u64> = HashMap::new();
    for item in items
        .iter()
        .filter(|item| !refused.contains(item.hash.as_str()))
    {
        first.entry(item.hash.as_str()).or_insert(item.offset);
    }
    wait_for_order(ctx, &session, &first);

    // The items to write, and their bytes (copied only when some are refused).
    let mut accepted: Vec<WriteItem> = Vec::with_capacity(items.len());
    let mut owned_body: Vec<u8> = vec![];
    let mut at = 0usize;
    for item in &items {
        let len = item.len as usize;
        if !refused.contains(item.hash.as_str()) {
            accepted.push(item.clone());
            if !refused.is_empty() {
                owned_body.extend_from_slice(&body[at..at + len]);
            }
        }
        at += len;
    }
    let accepted_body: &[u8] = if refused.is_empty() {
        body
    } else {
        &owned_body
    };
    let written = if accepted.is_empty() {
        vec![]
    } else {
        store
            .write_object_items(&accepted, accepted_body, &ctx.locks)
            .map_err(wire)?
    };
    drop(store);

    // Merge back in item order; update leases; back off corrupt sources.
    let mut written = written.into_iter();
    let mut results = Vec::with_capacity(items.len());
    let mut completed = false;
    {
        let now = now_ms();
        let mut leases = lock(&ctx.inflight_objects);
        for item in &items {
            if refused.contains(item.hash.as_str()) {
                results.push(WriteResult {
                    hash: item.hash.clone(),
                    outcome: ObjectOutcome::InFlightElsewhere,
                    durable_offset: 0,
                });
                continue;
            }
            let Some(result) = written.next() else { break };
            let ours = leases
                .get(&result.hash)
                .is_some_and(|lease| lease.session == session);
            match &result.outcome {
                ObjectOutcome::Progressed { durable_offset } if ours => {
                    if let Some(lease) = leases.get_mut(&result.hash) {
                        lease.offset = *durable_offset;
                        lease.expires_ms = now + LEASE_TTL.as_millis() as u64;
                    }
                }
                ObjectOutcome::Failed(_) if ours => {
                    if let Some(lease) = leases.get_mut(&result.hash) {
                        lease.offset = result.durable_offset;
                    }
                }
                ObjectOutcome::Progressed { .. } | ObjectOutcome::Failed(_) => {}
                outcome => {
                    completed |= matches!(outcome, ObjectOutcome::Completed);
                    if ours {
                        leases.remove(&result.hash);
                    }
                }
            }
            if result.outcome == ObjectOutcome::CorruptAtSource {
                let retry_at = back_off_source(ctx, &route.remote_replica, &result.hash);
                crate::util::log::event!(warn, CORRUPT_AT_SOURCE, {
                    "site": "worker.objects.write",
                    "hash": result.hash,
                    "source": route.remote_replica,
                    "retry_at_ms": retry_at,
                });
            }
            results.push(result);
        }
    }
    {
        let (generation, wake) = &PROGRESS;
        *lock(generation) += 1;
        wake.notify_all();
    }
    if completed {
        crate::worker::status_cell::want_content(ctx);
    }
    Ok((json!(WriteReply { results }), vec![]))
}

/// Waits (at most [`ORDER_WAIT`]) until every object in `first` (hash → the
/// offset of its first part in this write) has a durable offset at or past
/// that part, per this session's lease, or the lease is gone or another
/// session's, or the worker stops.
fn wait_for_order(ctx: &WorkerCtx, session: &str, first: &HashMap<&str, u64>) {
    let ahead = |leases: &BTreeMap<String, ObjectLease>| {
        first.iter().any(|(hash, offset)| {
            leases
                .get(*hash)
                .is_some_and(|lease| lease.session == session && lease.offset < *offset)
        })
    };
    if !ahead(&lock(&ctx.inflight_objects)) {
        return;
    }
    let deadline = Instant::now() + ORDER_WAIT;
    let (generation, wake) = &PROGRESS;
    let mut seen = lock(generation);
    loop {
        if !ahead(&lock(&ctx.inflight_objects)) || ctx.stopping.load(Ordering::Relaxed) {
            return;
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return;
        };
        seen = wake
            .wait_timeout(seen, remaining.min(Duration::from_millis(100)))
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .0;
    }
}

/// The retry time of `hash` from `source`, while that source is backed off
/// for it (`CorruptAtSource`); `None` otherwise.
pub fn source_retry_at(ctx: &WorkerCtx, source: &str, hash: &str) -> Option<u64> {
    source_backoff(ctx, source, hash, now_ms())
}
