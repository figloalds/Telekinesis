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
//! `ctx.inflight_objects` maps a hash to the session that may transfer it.
//! A want list never offers an object another session holds, and leases every
//! item it returns that is free, for `LEASE_TTL` (60 s); a write for an
//! object another session holds is `InFlightElsewhere` and touches nothing.
//! A write for a free (or lapsed) lease takes it, so a session whose lease
//! lapsed during a slow transfer continues. Mutual pairing and several peers
//! therefore move each object's bytes once.
//!
//! Only progress renews a lease (`LEASE_TTL`, "renewed on progress"): a
//! write that makes an object's bytes durable (`Progressed`). A want list
//! that offers an object again to the session holding it, and a write that
//! fails, leave the lease's expiry alone. A lease that reaches its expiry
//! without being released therefore saw no progress for a whole TTL: it
//! *lapses*, and the object is backed off for the source that held it (the
//! route's remote replica) for `STALL_BACKOFF`, so the sessions of other
//! sources are offered it. A source that cannot serve an object (a relay that
//! has the metadata but not yet the bytes) thus holds it for one TTL at a
//! time, never for as long as it keeps asking.
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
//! source (`objects.report`), which re-verifies and quarantines its copy. A
//! `SizeMismatch` (the source claims another size) and an item the store
//! refuses as malformed (a Fatal-class code) back the object off for that
//! source the same way, and a lapsed lease for `STALL_BACKOFF`. A want list
//! for a source skips the objects backed off for it, except those its own
//! session holds a live lease on (a transfer in progress resumes).
//!
//! **Local failures:** an item that fails here for a local reason
//! (`OBJECT_IO_ERROR`, a failed registration, ...) records the object
//! backoff in `object_error_state` (`Store::record_object_failure`, 30 s →
//! 1 h; contention and `SQLITE_BUSY` are never recorded), which every want
//! list honors, and releases the lease. `RESUME_OFFSET_AHEAD` and
//! `OBJECT_PART_CHANGED` are not failures of the object: they tell the
//! writer where to resume, and keep the lease.
use super::ctx::{ObjectLease, WorkerCtx};
use crate::{
    model::{
        errors::{ErrorClass, ErrorWire},
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
/// How long an object whose lease lapsed (no progress for `LEASE_TTL`) is
/// backed off for the source that held it (see the module docs).
pub const STALL_BACKOFF: Duration = LEASE_TTL;
/// Lease holders kept (see `LEASE_HOLDERS`).
const MAX_LEASE_HOLDERS: usize = 64 * 1024;

/// Wakes writes waiting for an object's durable offset (`ORDER_WAIT`): bumped
/// after every write call.
static PROGRESS: (Mutex<u64>, Condvar) = (Mutex::new(0), Condvar::new());

/// `(store root, source replica, hash)` → `(attempts, retry_at_ms)`.
type SourceKey = (PathBuf, String, String);
static SOURCE_BACKOFF: Mutex<BTreeMap<SourceKey, (u32, u64)>> = Mutex::new(BTreeMap::new());

/// Who was granted each lease: `(store root, hash)` → `(session, source
/// replica)`, so a lapsed lease backs its object off for its source
/// (`ObjectLease`, frozen, has no source). Lock order: `ctx.inflight_objects`,
/// then `LEASE_HOLDERS`, then `SOURCE_BACKOFF`.
type HolderKey = (PathBuf, String);
static LEASE_HOLDERS: Mutex<BTreeMap<HolderKey, (String, String)>> = Mutex::new(BTreeMap::new());

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

/// The retry time of `hash` from `source` while it is backed off.
fn source_backoff(ctx: &WorkerCtx, source: &str, hash: &str, now: u64) -> Option<u64> {
    lock(&SOURCE_BACKOFF)
        .get(&(ctx.root.clone(), source.to_owned(), hash.to_owned()))
        .map(|&(_, retry_at)| retry_at)
        .filter(|retry_at| *retry_at > now)
}

/// The `SOURCE_BACKOFF` entry of `(source, hash)`, room made first.
fn source_entry<'a>(
    map: &'a mut BTreeMap<SourceKey, (u32, u64)>,
    ctx: &WorkerCtx,
    source: &str,
    hash: &str,
    now: u64,
) -> &'a mut (u32, u64) {
    if map.len() >= MAX_SOURCE_BACKOFFS {
        map.retain(|_, (_, retry_at)| *retry_at > now);
        if map.len() >= MAX_SOURCE_BACKOFFS {
            map.clear();
        }
    }
    map.entry((ctx.root.clone(), source.to_owned(), hash.to_owned()))
        .or_insert((0, now))
}

/// Backs `hash` off for `source` (30 s → 1 h by attempt). Returns the retry time.
pub fn back_off_source(ctx: &WorkerCtx, source: &str, hash: &str) -> u64 {
    let now = now_ms();
    let mut map = lock(&SOURCE_BACKOFF);
    let entry = source_entry(&mut map, ctx, source, hash, now);
    entry.0 = entry.0.saturating_add(1);
    let delay = Backoff::objects().nominal(entry.0);
    entry.1 = now.saturating_add(u64::try_from(delay.as_millis()).unwrap_or(u64::MAX));
    entry.1
}

/// Backs `hash` off for `source` for `STALL_BACKOFF` (a lapsed lease), never
/// shortening a longer backoff and without counting an attempt.
fn back_off_stalled(ctx: &WorkerCtx, source: &str, hash: &str, now: u64) -> u64 {
    let mut map = lock(&SOURCE_BACKOFF);
    let entry = source_entry(&mut map, ctx, source, hash, now);
    entry.1 = entry
        .1
        .max(now.saturating_add(STALL_BACKOFF.as_millis() as u64));
    entry.1
}

/// Grants `session` (transferring from `source`) a fresh lease on `hash`.
fn grant(
    ctx: &WorkerCtx,
    leases: &mut BTreeMap<String, ObjectLease>,
    lease: (&str, &str, &str, u64),
    now: u64,
) {
    let (hash, session, source, offset) = lease;
    leases.insert(
        hash.to_owned(),
        ObjectLease {
            session: session.to_owned(),
            expires_ms: now + LEASE_TTL.as_millis() as u64,
            offset,
        },
    );
    let mut holders = lock(&LEASE_HOLDERS);
    if holders.len() >= MAX_LEASE_HOLDERS {
        holders.retain(|(root, hash), _| *root != ctx.root || leases.contains_key(hash));
        if holders.len() >= MAX_LEASE_HOLDERS {
            holders.clear();
        }
    }
    holders.insert(
        (ctx.root.clone(), hash.to_owned()),
        (session.to_owned(), source.to_owned()),
    );
}

/// Ends the lease of `hash` (a terminal outcome or a local failure).
fn release(ctx: &WorkerCtx, leases: &mut BTreeMap<String, ObjectLease>, hash: &str) {
    leases.remove(hash);
    lock(&LEASE_HOLDERS).remove(&(ctx.root.clone(), hash.to_owned()));
}

/// The lane set of one of the orchestrator's lease ids (§6.4 "Lease ids":
/// `pull:<peer>:<i>/<n>:<a|b>` for a dialer's pulls, `push:<peer>:<i>/<n>`
/// for the pushes an acceptor serves): its kind, peer and lane count. `None`
/// for any other id.
fn lane_set(session: &str) -> Option<(&str, &str, &str)> {
    let mut parts = session.splitn(4, ':');
    let (kind, peer, lane) = (parts.next()?, parts.next()?, parts.next()?);
    let (_, count) = lane.split_once('/')?;
    let digits = |text: &str| !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit());
    (matches!(kind, "pull" | "push") && !peer.is_empty() && digits(count))
        .then_some((kind, peer, count))
}

/// A lease of another lane set of the same peer that made no progress for
/// this long may be taken over (see [`superseded`]).
const SUPERSEDE_IDLE: Duration = Duration::from_secs(15);

/// Whether `held` is a lease of a lane set that `session`'s replaced: the
/// same kind and peer with another lane count, and no progress for
/// [`SUPERSEDE_IDLE`]. A re-pairing with another `connections` restarts every
/// lane of that peer, so the old ids' sessions are gone; their leases would
/// hide the objects from the new lanes until they lapse (one TTL), and then
/// back them off for a healthy source (WP3.INT re-review). The new lane set
/// takes such a lease over, without a stall backoff, once it has been idle a
/// quarter of a TTL (an old lane still moving it during the switch keeps it).
fn superseded(held: &ObjectLease, session: &str, now: u64) -> bool {
    let idle_since = held.expires_ms.saturating_sub(LEASE_TTL.as_millis() as u64);
    now.saturating_sub(idle_since) >= SUPERSEDE_IDLE.as_millis() as u64
        && matches!(
            (lane_set(&held.session), lane_set(session)),
            (Some((kind, peer, count)), Some((new_kind, new_peer, new_count)))
                if kind == new_kind && peer == new_peer && count != new_count
        )
}

/// Drops every lease that lapsed (reached its expiry without progress or
/// release) and backs each object off for the source that held it, for
/// `STALL_BACKOFF` (module docs).
fn sweep_lapsed(ctx: &WorkerCtx, leases: &mut BTreeMap<String, ObjectLease>, now: u64) {
    let lapsed: Vec<(String, String)> = leases
        .iter()
        .filter(|(_, lease)| lease.expires_ms <= now)
        .map(|(hash, lease)| (hash.clone(), lease.session.clone()))
        .collect();
    if lapsed.is_empty() {
        return;
    }
    let mut holders = lock(&LEASE_HOLDERS);
    for (hash, session) in lapsed {
        leases.remove(&hash);
        let Some((holder, source)) = holders.remove(&(ctx.root.clone(), hash.clone())) else {
            continue;
        };
        if holder != session {
            continue;
        }
        let retry_at = back_off_stalled(ctx, &source, &hash, now);
        crate::util::log::event!(info, NOT_AVAILABLE, {
            "site": "worker.objects.lease_lapsed",
            "hash": hash,
            "session": session,
            "source": source,
            "retry_at_ms": retry_at,
        });
    }
}

/// `objects.want{route, lane, max_items, max_bytes, session}` → `{items}`;
/// every returned item is leased to `session`: a free one gets a fresh lease,
/// one `session` holds already keeps its lease and expiry (only progress
/// renews it, module docs).
fn want(ctx: &WorkerCtx, header: WantHeader) -> Result<(Value, Vec<u8>), ErrorWire> {
    bind(ctx, &header.route)?;
    session_id(&header.session)?;
    if !header.lane.is_valid() {
        return Err(ErrorWire::coded(
            "INVALID_BULK_LANE",
            format!("lane {}/{}", header.lane.index, header.lane.count),
        ));
    }
    let session = header.session.as_str();
    let source = header.route.remote_replica.as_str();
    let now = now_ms();
    // Lapsed leases back their objects off for their sources first; then a
    // snapshot (the store's scan runs without the lease lock): objects other
    // sessions hold, and the ones this session holds.
    let (leased, mine): (HashSet<String>, HashSet<String>) = {
        let mut leases = lock(&ctx.inflight_objects);
        sweep_lapsed(ctx, &mut leases, now);
        let (mine, others): (Vec<_>, Vec<_>) = leases
            .iter()
            .filter(|(_, lease)| !superseded(lease, session, now))
            .partition(|(_, lease)| lease.session == session);
        (
            others.into_iter().map(|(hash, _)| hash.clone()).collect(),
            mine.into_iter().map(|(hash, _)| hash.clone()).collect(),
        )
    };
    let backed_off: HashSet<String> = {
        let mut map = lock(&SOURCE_BACKOFF);
        map.retain(|_, (_, retry_at)| *retry_at > now.saturating_sub(3_600_000));
        map.iter()
            .filter(|((root, from, _), (_, retry_at))| {
                *root == ctx.root && from == source && *retry_at > now
            })
            .map(|((_, _, hash), _)| hash.clone())
            .collect()
    };
    let mut items = store(ctx)?
        .want_objects(
            header.lane,
            (header.max_items as usize).min(MAX_WANT_ITEMS),
            header.max_bytes,
            &|hash| leased.contains(hash) || (backed_off.contains(hash) && !mine.contains(hash)),
        )
        .map_err(wire)?;
    {
        let now = now_ms();
        let mut leases = lock(&ctx.inflight_objects);
        sweep_lapsed(ctx, &mut leases, now);
        items.retain(|item| {
            match leases.get_mut(&item.hash) {
                Some(lease) if superseded(lease, session, now) => {
                    let offset = lease.offset.max(item.offset);
                    grant(ctx, &mut leases, (&item.hash, session, source, offset), now);
                    true
                }
                Some(lease) if lease.session != session => false,
                Some(lease) => {
                    // Ours already: offered again, the expiry untouched. The
                    // scan's offset may predate a write that has since moved
                    // it on: never move it back.
                    lease.offset = lease.offset.max(item.offset);
                    true
                }
                // Lapsed meanwhile (now backed off for this source), or free.
                None if source_backoff(ctx, source, &item.hash, now).is_some() => false,
                None => {
                    grant(
                        ctx,
                        &mut leases,
                        (&item.hash, session, source, item.offset),
                        now,
                    );
                    true
                }
            }
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
    let source = route.remote_replica.as_str();
    let mut refused: HashSet<&str> = HashSet::new();
    {
        let now = now_ms();
        let mut leases = lock(&ctx.inflight_objects);
        // A lapsed lease (ours too) backs its object off for its source and
        // frees it (module docs).
        sweep_lapsed(ctx, &mut leases, now);
        for hash in &hashes {
            match leases.get(*hash) {
                Some(lease) if superseded(lease, &session, now) => {
                    let offset = committed.get(hash).copied().unwrap_or(lease.offset);
                    grant(ctx, &mut leases, (hash, &session, source, offset), now);
                }
                Some(lease) if lease.session != session => {
                    refused.insert(hash);
                }
                // Ours and live: untouched (only progress renews it).
                Some(_) => {}
                None => {
                    let offset = committed.get(hash).copied().unwrap_or(0);
                    grant(ctx, &mut leases, (hash, &session, source, offset), now);
                }
            }
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

    // Merge back in item order; update leases (only progress renews one);
    // back off sources and record local failures (module docs).
    let mut written = written.into_iter();
    let mut results = Vec::with_capacity(items.len());
    let mut completed = false;
    // Per object, once (several items of one object count one attempt).
    let mut blamed: BTreeMap<String, String> = BTreeMap::new();
    let mut local: BTreeMap<String, ErrorWire> = BTreeMap::new();
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
                ObjectOutcome::Progressed { durable_offset } => {
                    if let Some(lease) = leases.get_mut(&result.hash).filter(|_| ours) {
                        // Progress renews the lease: bytes that were already
                        // durable (a re-sent overlap) are none.
                        if *durable_offset > lease.offset {
                            lease.expires_ms = now + LEASE_TTL.as_millis() as u64;
                        }
                        lease.offset = *durable_offset;
                    }
                }
                ObjectOutcome::Failed(error) => match failure_kind(error) {
                    Failure::Resume => {
                        if let Some(lease) = leases.get_mut(&result.hash).filter(|_| ours) {
                            lease.offset = result.durable_offset;
                        }
                    }
                    Failure::Contention => {}
                    Failure::Source => {
                        blamed
                            .entry(result.hash.clone())
                            .or_insert_with(|| error.code.clone());
                        if ours {
                            release(ctx, &mut leases, &result.hash);
                        }
                    }
                    Failure::Local => {
                        local
                            .entry(result.hash.clone())
                            .or_insert_with(|| error.clone());
                        if ours {
                            release(ctx, &mut leases, &result.hash);
                        }
                    }
                },
                outcome => {
                    completed |= matches!(outcome, ObjectOutcome::Completed);
                    if let ObjectOutcome::CorruptAtSource | ObjectOutcome::SizeMismatch = outcome {
                        blamed
                            .entry(result.hash.clone())
                            .or_insert_with(|| outcome.code().unwrap_or_default().to_owned());
                    }
                    if ours {
                        release(ctx, &mut leases, &result.hash);
                    }
                }
            }
            results.push(result);
        }
    }
    for (hash, code) in &blamed {
        let retry_at = back_off_source(ctx, source, hash);
        crate::util::log::event!(warn, (code.as_str()), {
            "site": "worker.objects.write",
            "hash": hash,
            "source": source,
            "retry_at_ms": retry_at,
        });
    }
    if !local.is_empty() {
        // Best effort: the outcome is answered either way.
        if let Ok(store) = self::store(ctx) {
            for (hash, error) in &local {
                let _ = store.record_object_failure(hash, error);
            }
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

/// How `objects.write` treats a `Failed` item (module docs).
enum Failure {
    /// The writer resumes at `durable_offset`; the lease stays.
    Resume,
    /// Contention (busy database, lock): nothing recorded.
    Contention,
    /// The source's item was malformed: backed off for that source.
    Source,
    /// A local failure: the object backoff (`object_error_state`).
    Local,
}

fn failure_kind(error: &ErrorWire) -> Failure {
    match error.code.as_str() {
        "RESUME_OFFSET_AHEAD" | "OBJECT_PART_CHANGED" => Failure::Resume,
        "SQLITE_BUSY" => Failure::Contention,
        "INVALID_OBJECT_PART" | "INVALID_OBJECT_ID" => Failure::Source,
        _ if error.class == ErrorClass::Contention => Failure::Contention,
        _ => Failure::Local,
    }
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
