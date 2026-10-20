//! Owned by WP3.2 (WP4.5: CCR-WP3.2-3, CCR-WP3.3-4).
//!
//! Worker `objects.want`, `objects.read`, `objects.write`, `objects.report`
//! and `objects.release` (REVAMP_PLAN §6.4): the worker half of objects v3.
//! Each op checks its route against this worker (the `paired_sync::binding`
//! codes, as the feed ops do), runs on a pooled secondary store
//! (`ctx.sync_stores`) and never takes the engine lock. Bodies are binary
//! (`objects.read` replies with the bytes, `objects.write` carries the
//! parts' bytes concatenated in item order), so `read` and `write` need a v3
//! loopback session (`runtime::server`); `want`, `report` and `release` also
//! answer over the JSON RPC.
//!
//! **State** (CCR-WP3.2-3, WP4.5): the leases, the source each was granted
//! for, the per-source backoffs and the durable-progress signal live in the
//! worker's own context (`WorkerCtx::inflight_objects`, one lock), not in
//! process-wide statics keyed by the store root.
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
//! sources are offered it.
//!
//! **Giving a lease up** (CCR-WP3.3-4, WP4.5): a session that will not move
//! an object (its source cannot serve it, its transfer failed or stopped)
//! need not wait for the lapse. `objects.release{route, session, hashes}`
//! ends the leases `session` holds on `hashes` at once, and an
//! `objects.want` with `exclude` gives up the session's leases on the
//! excluded hashes and leaves them out of its list (the orchestrator
//! excludes what it backs off for that source). Either way the object is
//! free for the next want of another session, at its durable offset, with
//! no stall backoff: the orchestrator keeps its own per-source backoff.
//!
//! **Demand** (WP5.3): `objects.demand{route, lane?, max_items?}` →
//! `{items:[{hash, requested_ms}]}` lists the objects a reader waits for
//! (`Store::demanded_objects`, newest first; only the lane's own with
//! `lane`). It is read-only: it leases nothing and touches no backoff. A
//! lane busy with other objects asks it every half second and gives the
//! demanded ones its next pull; the want list orders them first anyway.
//! The op name and its types live here until `model::protocol::objects`
//! takes them (CCR-WP5.3-3); a worker without it answers
//! `UNSUPPORTED_OPERATION`, which the lane remembers.
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
use super::ctx::{ObjectLease, ObjectsState, WorkerCtx};
use crate::{
    model::{
        errors::{ErrorClass, ErrorWire},
        protocol::objects::{
            LEASE_TTL, MAX_READ_LEN, MAX_RELEASE_ITEMS, MAX_WANT_ITEMS, ObjectOutcome, ReadHeader,
            ReadReply, ReleaseHeader, ReleaseReply, ReportHeader, ReportReply, WantReply,
            WantRequest, WriteHeader, WriteItem, WriteReply, WriteResult, ops,
        },
    },
    paired_sync::SyncRoute,
    util::{backoff::Backoff, lock::lock, time::now_ms},
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::atomic::Ordering,
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
/// Lease holders kept (see [`ObjectsState::holders`]).
const MAX_LEASE_HOLDERS: usize = 64 * 1024;

/// `objects.demand` (module docs, "Demand"; WP5.3). Its name and types are
/// the model's since CCR-WP5.3-3 (applied by WP5.INT), re-exported here for
/// the callers that named them here.
pub use crate::model::protocol::objects::{
    DemandHeader, DemandItem, DemandReply, MAX_DEMAND_ITEMS, ops::DEMAND,
};

pub fn dispatch(
    ctx: &WorkerCtx,
    op: &str,
    header: &Value,
    body: &[u8],
) -> Result<(Value, Vec<u8>), ErrorWire> {
    if op.starts_with(crate::model::content_wire::PREFIX) {
        return super::content_rpc::dispatch(ctx, op, header, body);
    }
    match op {
        ops::WANT => want(ctx, parse(op, header)?),
        ops::READ => read(ctx, parse(op, header)?),
        ops::WRITE => write(ctx, parse(op, header)?, body),
        ops::REPORT => report(ctx, parse(op, header)?),
        ops::RELEASE => release_op(ctx, parse(op, header)?),
        DEMAND => demand(ctx, parse(op, header)?),
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
pub(super) fn bind(ctx: &WorkerCtx, route: &SyncRoute) -> Result<(), ErrorWire> {
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

/// A newer daemon/run fences both slots of an owned lane, including delayed
/// wants/writes/releases on still-open pooled loopback sessions. The worker
/// never evicts a fence to make an old run admissible again.
fn owned_run<'a>(
    session: &'a str,
    route: &SyncRoute,
) -> Result<Option<crate::model::lease::Owned<'a>>, ErrorWire> {
    let owned = crate::model::lease::parse_transfer(session)?;
    if owned.as_ref().is_some_and(|o| o.peer != route.peer) {
        return Err(ErrorWire::coded(
            "INVALID_REQUEST",
            "lease generation peer does not match route",
        ));
    }
    Ok(owned)
}
fn run_current(state: &ObjectsState, owned: Option<&crate::model::lease::Owned<'_>>) -> bool {
    owned.is_none_or(|o| {
        layout_current(state, o)
            && state
                .lease_runs
                .get(o.scope)
                .is_none_or(|g| g.layout != o.layout || g.generation <= o.generation)
    })
}

fn layout_current(state: &ObjectsState, owned: &crate::model::lease::Owned<'_>) -> bool {
    state
        .lease_layouts
        .get(owned.family)
        .is_none_or(|(seen, count)| {
            owned.layout.is_some_and(|run| {
                let incoming = crate::model::lease::Generation {
                    incarnation: owned.generation.incarnation,
                    run,
                };
                incoming > *seen || (incoming == *seen && owned.lane.count == *count)
            })
        })
}
fn run_owns(state: &ObjectsState, owned: Option<&crate::model::lease::Owned<'_>>) -> bool {
    owned.is_none_or(|o| {
        layout_current(state, o)
            && state.lease_runs.get(o.scope)
                == Some(&crate::model::lease::RunFence {
                    generation: o.generation,
                    layout: o.layout,
                })
            && o.layout.is_none_or(|run| {
                state.lease_layouts.get(o.family)
                    == Some(&(
                        crate::model::lease::Generation {
                            incarnation: o.generation.incarnation,
                            run,
                        },
                        o.lane.count,
                    ))
            })
    })
}
fn claim_run(
    state: &mut ObjectsState,
    owned: Option<&crate::model::lease::Owned<'_>>,
) -> Result<(), ErrorWire> {
    if !run_current(state, owned) {
        return Err(ErrorWire::coded(
            "IN_FLIGHT_ELSEWHERE",
            "object lease run superseded",
        ));
    }
    if let Some(o) = owned {
        if !state.lease_runs.contains_key(o.scope)
            && state.lease_runs.len() >= crate::model::lease::MAX_RUN_FENCES
        {
            return Err(ErrorWire::coded(
                "BUSY",
                "object lease generation fence capacity",
            ));
        }
        if o.layout.is_some()
            && !state.lease_layouts.contains_key(o.family)
            && state.lease_layouts.len() >= crate::model::lease::MAX_RUN_FENCES
        {
            return Err(ErrorWire::coded(
                "BUSY",
                "object lease layout fence capacity",
            ));
        }
        if let Some(run) = o.layout {
            state.lease_layouts.insert(
                o.family.to_owned(),
                (
                    crate::model::lease::Generation {
                        incarnation: o.generation.incarnation,
                        run,
                    },
                    o.lane.count,
                ),
            );
        }
        state.lease_runs.insert(
            o.scope.to_owned(),
            crate::model::lease::RunFence {
                generation: o.generation,
                layout: o.layout,
            },
        );
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
fn source_backoff(state: &ObjectsState, source: &str, hash: &str, now: u64) -> Option<u64> {
    state
        .source_backoff
        .get(&(source.to_owned(), hash.to_owned()))
        .map(|&(_, retry_at)| retry_at)
        .filter(|retry_at| *retry_at > now)
}

/// The backoff entry of `(source, hash)`, room made first.
fn source_entry<'a>(
    state: &'a mut ObjectsState,
    source: &str,
    hash: &str,
    now: u64,
) -> &'a mut (u32, u64) {
    let map = &mut state.source_backoff;
    if map.len() >= MAX_SOURCE_BACKOFFS {
        map.retain(|_, (_, retry_at)| *retry_at > now);
        if map.len() >= MAX_SOURCE_BACKOFFS {
            map.clear();
        }
    }
    map.entry((source.to_owned(), hash.to_owned()))
        .or_insert((0, now))
}

/// Backs `hash` off for `source` (30 s → 1 h by attempt). Returns the retry time.
pub fn back_off_source(ctx: &WorkerCtx, source: &str, hash: &str) -> u64 {
    let now = now_ms();
    let mut state = lock(&ctx.inflight_objects);
    let entry = source_entry(&mut state, source, hash, now);
    entry.0 = entry.0.saturating_add(1);
    let delay = Backoff::objects().nominal(entry.0);
    entry.1 = now.saturating_add(u64::try_from(delay.as_millis()).unwrap_or(u64::MAX));
    entry.1
}

/// Backs `hash` off for `source` for `STALL_BACKOFF` (a lapsed lease), never
/// shortening a longer backoff and without counting an attempt.
fn back_off_stalled(state: &mut ObjectsState, source: &str, hash: &str, now: u64) -> u64 {
    let entry = source_entry(state, source, hash, now);
    entry.1 = entry
        .1
        .max(now.saturating_add(STALL_BACKOFF.as_millis() as u64));
    entry.1
}

/// Grants `session` (transferring from `source`) a fresh lease on `hash`.
fn grant(state: &mut ObjectsState, lease: (&str, &str, &str, u64), now: u64) {
    let (hash, session, source, offset) = lease;
    state.leases.insert(
        hash.to_owned(),
        ObjectLease {
            session: session.to_owned(),
            expires_ms: now + LEASE_TTL.as_millis() as u64,
            offset,
        },
    );
    if state.holders.len() >= MAX_LEASE_HOLDERS {
        let ObjectsState {
            leases, holders, ..
        } = &mut *state;
        holders.retain(|hash, _| leases.contains_key(hash));
        if holders.len() >= MAX_LEASE_HOLDERS {
            holders.clear();
        }
    }
    state
        .holders
        .insert(hash.to_owned(), (session.to_owned(), source.to_owned()));
}

/// Ends the lease of `hash` (a terminal outcome, a local failure, or the
/// session gave it up).
fn release(state: &mut ObjectsState, hash: &str) {
    state.leases.remove(hash);
    state.holders.remove(hash);
}

/// Ends the leases `session` holds on `hashes` (module docs, "Giving a lease
/// up"); returns how many ended. A lease of another session is untouched.
fn give_up(state: &mut ObjectsState, session: &str, hashes: &[String]) -> u32 {
    let mut released = 0;
    for hash in hashes {
        if state
            .leases
            .get(hash)
            .is_some_and(|lease| lease.session == session)
        {
            release(state, hash);
            released += 1;
        }
    }
    released
}

/// At most [`MAX_RELEASE_ITEMS`] hashes (an `exclude` or `release` list).
fn release_list(op: &str, hashes: &[String]) -> Result<(), ErrorWire> {
    if hashes.len() > MAX_RELEASE_ITEMS {
        return Err(ErrorWire::coded(
            "INVALID_REQUEST",
            format!(
                "{op}: {} hashes (at most {MAX_RELEASE_ITEMS})",
                hashes.len()
            ),
        ));
    }
    Ok(())
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
    if let (Ok(Some(old)), Ok(Some(new))) = (
        crate::model::lease::parse_transfer(&held.session),
        crate::model::lease::parse_transfer(session),
    ) {
        if old.family == new.family {
            let layout = |o: &crate::model::lease::Owned<'_>| {
                o.layout.map(|run| crate::model::lease::Generation {
                    incarnation: o.generation.incarnation,
                    run,
                })
            };
            if layout(&old) < layout(&new) {
                return true;
            }
            if layout(&old) > layout(&new) {
                return false;
            }
            if old.scope == new.scope
                && (old.generation < new.generation
                    || (old.generation == new.generation
                        && new.frame.is_some()
                        && old.frame < new.frame))
            {
                return true;
            }
        }
        // Qualified layouts never use the historical idle-time heuristic.
        if old.layout.is_some() || new.layout.is_some() {
            return false;
        }
    }
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
fn sweep_lapsed(state: &mut ObjectsState, now: u64) {
    let lapsed: Vec<(String, String)> = state
        .leases
        .iter()
        .filter(|(_, lease)| lease.expires_ms <= now)
        .map(|(hash, lease)| (hash.clone(), lease.session.clone()))
        .collect();
    for (hash, session) in lapsed {
        state.leases.remove(&hash);
        let Some((holder, source)) = state.holders.remove(&hash) else {
            continue;
        };
        if holder != session {
            continue;
        }
        let retry_at = back_off_stalled(state, &source, &hash, now);
        crate::util::log::event!(info, NOT_AVAILABLE, {
            "site": "worker.objects.lease_lapsed",
            "hash": hash,
            "session": session,
            "source": source,
            "retry_at_ms": retry_at,
        });
    }
}

/// `objects.want{route, lane, max_items, max_bytes, session, exclude?}` →
/// `{items}`; every returned item is leased to `session`: a free one gets a
/// fresh lease, one `session` holds already keeps its lease and expiry (only
/// progress renews it, module docs). `exclude` (CCR-WP3.3-4, WP4.5): those
/// hashes are left out, and the leases `session` holds on them end.
fn want(ctx: &WorkerCtx, request: WantRequest) -> Result<(Value, Vec<u8>), ErrorWire> {
    want_selected(ctx, request, None)
}

pub(super) fn legacy_prepare(
    ctx: &WorkerCtx,
    header: &Value,
) -> Result<(Value, Vec<u8>), ErrorWire> {
    let request: crate::model::content_wire::LegacyPrepare = parse("legacy-prepare", header)?;
    crate::model::content::Digest::parse(&request.hash).map_err(wire)?;
    if crate::model::lease::parse(&request.session)?.is_none_or(|o| o.frame.is_none()) {
        return Err(ErrorWire::coded(
            "INVALID_REQUEST",
            "foreground lease frame required",
        ));
    }
    want_selected(
        ctx,
        WantRequest {
            header: crate::model::protocol::objects::WantHeader {
                route: request.route,
                lane: request.lane,
                max_items: 1,
                max_bytes: crate::model::content_wire::LEGACY_BYTES as u64,
                session: request.session,
            },
            exclude: vec![],
        },
        Some(&request.hash),
    )
}

pub(super) fn legacy_read(ctx: &WorkerCtx, header: &Value) -> Result<(Value, Vec<u8>), ErrorWire> {
    let header: ReadHeader = parse("legacy-read", header)?;
    bind(ctx, &header.route)?;
    if header.len > crate::model::content_wire::FRAME_BYTES as u64 {
        return Err(ErrorWire::coded(
            "CONTENT_BUFFER_LIMIT",
            "legacy foreground read",
        ));
    }
    let context = crate::content::ReadContext {
        deadline: Instant::now() + Duration::from_secs(5),
        cancel: lock(&ctx.inflight_objects).legacy_cancel.child(),
    };
    let provider = crate::content::LocalContent::open(&ctx.root).map_err(wire)?;
    let _buffers = provider.foreground_permit(&context).map_err(wire)?;
    let store = super::content_rpc::connection(ctx).map_err(wire)?;
    let (total, bytes) =
        store.read_object_range(&header.hash, header.offset, header.len as usize)?;
    context.check().map_err(wire)?;
    Ok((json!(ReadReply { total }), bytes))
}

fn want_selected(
    ctx: &WorkerCtx,
    request: WantRequest,
    only: Option<&str>,
) -> Result<(Value, Vec<u8>), ErrorWire> {
    let WantRequest { header, exclude } = request;
    bind(ctx, &header.route)?;
    session_id(&header.session)?;
    let owned = owned_run(&header.session, &header.route)?;
    if owned
        .as_ref()
        .is_some_and(|o| o.generation.run != 0 && o.lane != header.lane)
    {
        return Err(ErrorWire::coded(
            "INVALID_BULK_LANE",
            "lease generation lane does not match want",
        ));
    }
    release_list(ops::WANT, &exclude)?;
    if !header.lane.is_valid() {
        return Err(ErrorWire::coded(
            "INVALID_BULK_LANE",
            format!("lane {}/{}", header.lane.index, header.lane.count),
        ));
    }
    let session = header.session.as_str();
    let source = header.route.remote_replica.as_str();
    let now = now_ms();
    let excluded: HashSet<&str> = exclude.iter().map(String::as_str).collect();
    // Lapsed leases back their objects off for their sources first, and the
    // session gives up what it excludes; then a snapshot (the store's scan
    // runs without the lock): objects other sessions hold, the ones this
    // session holds, and the ones backed off for this source.
    let (leased, mine, backed_off): (HashSet<String>, HashSet<String>, HashSet<String>) = {
        let mut state = lock(&ctx.inflight_objects);
        claim_run(&mut state, owned.as_ref())?;
        sweep_lapsed(&mut state, now);
        give_up(&mut state, session, &exclude);
        let (mine, others): (Vec<_>, Vec<_>) = state
            .leases
            .iter()
            .filter(|(_, lease)| !superseded(lease, session, now))
            .partition(|(_, lease)| lease.session == session);
        let (mine, others) = (
            mine.into_iter().map(|(hash, _)| hash.clone()).collect(),
            others.into_iter().map(|(hash, _)| hash.clone()).collect(),
        );
        state
            .source_backoff
            .retain(|_, (_, retry_at)| *retry_at > now.saturating_sub(3_600_000));
        let backed_off = state
            .source_backoff
            .iter()
            .filter(|((from, _), (_, retry_at))| from == source && *retry_at > now)
            .map(|((_, hash), _)| hash.clone())
            .collect();
        (others, mine, backed_off)
    };
    let mut items = if let Some(hash) = only {
        if excluded.contains(hash)
            || leased.contains(hash)
            || (backed_off.contains(hash) && !mine.contains(hash))
        {
            vec![]
        } else {
            super::content_rpc::connection(ctx)
                .map_err(wire)?
                .demanded_object(hash, header.lane)
                .map_err(wire)?
                .into_iter()
                .collect()
        }
    } else {
        store(ctx)?
            .want_objects(
                header.lane,
                (header.max_items as usize).min(MAX_WANT_ITEMS),
                header.max_bytes,
                &|hash| {
                    only.is_some_and(|selected| selected != hash)
                        || excluded.contains(hash)
                        || leased.contains(hash)
                        || (backed_off.contains(hash) && !mine.contains(hash))
                },
            )
            .map_err(wire)?
    };
    crate::util::fault::trigger("objects.want.before_grant").map_err(wire)?;
    {
        let now = now_ms();
        let mut state = lock(&ctx.inflight_objects);
        claim_run(&mut state, owned.as_ref())?;
        sweep_lapsed(&mut state, now);
        items.retain_mut(|item| {
            let held = state.leases.get(&item.hash).cloned();
            match held {
                Some(lease) if superseded(&lease, session, now) => {
                    let offset = lease.offset.max(item.offset);
                    item.offset = offset;
                    grant(&mut state, (&item.hash, session, source, offset), now);
                    true
                }
                Some(lease) if lease.session != session => false,
                Some(_) => {
                    // Ours already: offered again, the expiry untouched. The
                    // scan's offset may predate a write that has since moved
                    // it on: never move it back.
                    if let Some(lease) = state.leases.get_mut(&item.hash) {
                        lease.offset = lease.offset.max(item.offset);
                    }
                    true
                }
                // Lapsed meanwhile (now backed off for this source), or free.
                None if source_backoff(&state, source, &item.hash, now).is_some() => false,
                None => {
                    grant(&mut state, (&item.hash, session, source, item.offset), now);
                    true
                }
            }
        });
    }
    Ok((json!(WantReply { items }), vec![]))
}

/// `objects.demand{route, lane?, max_items?}` → `{items}` (module docs,
/// "Demand"): read-only.
fn demand(ctx: &WorkerCtx, header: DemandHeader) -> Result<(Value, Vec<u8>), ErrorWire> {
    bind(ctx, &header.route)?;
    if let Some(lane) = header.lane
        && !lane.is_valid()
    {
        return Err(ErrorWire::coded(
            "INVALID_BULK_LANE",
            format!("lane {}/{}", lane.index, lane.count),
        ));
    }
    let limit = header
        .max_items
        .unwrap_or(MAX_DEMAND_ITEMS)
        .clamp(1, MAX_DEMAND_ITEMS) as usize;
    // The lane's share of the demand: ask for more rows than `limit` when
    // filtering, so another lane's objects do not crowd this one's out.
    let rows = store(ctx)?
        .demanded_objects(if header.lane.is_some() {
            limit * 4
        } else {
            limit
        })
        .map_err(wire)?;
    let items = rows
        .into_iter()
        .filter(|(hash, _)| header.lane.is_none_or(|lane| lane.owns(hash)))
        .take(limit)
        .map(|(hash, requested_ms)| DemandItem { hash, requested_ms })
        .collect();
    Ok((json!(DemandReply { items }), vec![]))
}

/// `objects.release{route, session, hashes}` → `{released}` (module docs,
/// "Giving a lease up").
fn release_op(ctx: &WorkerCtx, header: ReleaseHeader) -> Result<(Value, Vec<u8>), ErrorWire> {
    bind(ctx, &header.route)?;
    session_id(&header.session)?;
    let owned = owned_run(&header.session, &header.route)?;
    release_list(ops::RELEASE, &header.hashes)?;
    let released = {
        let mut state = lock(&ctx.inflight_objects);
        if !run_owns(&state, owned.as_ref()) {
            return Ok((json!(ReleaseReply { released: 0 }), vec![]));
        }
        let now = now_ms();
        sweep_lapsed(&mut state, now);
        let held: HashSet<_> = header
            .hashes
            .iter()
            .filter(|hash| {
                state
                    .leases
                    .get(*hash)
                    .is_some_and(|lease| lease.session == header.session)
            })
            .cloned()
            .collect();
        let released = give_up(&mut state, &header.session, &header.hashes);
        // The dialer's own backoff for this source, kept here so later wants
        // of this source leave them out whatever its exclusion list holds
        // (WP4.INT, the WP4.5 review's minor).
        if let Some(hold) = header.hold_ms.filter(|hold| *hold > 0) {
            let source = header.route.remote_replica.as_str();
            for hash in &header.hashes {
                if owned.is_some() && !held.contains(hash) {
                    continue;
                }
                let entry = source_entry(&mut state, source, hash, now);
                entry.1 = entry.1.max(now.saturating_add(hold));
            }
        }
        released
    };
    if released > 0 {
        // A write of this session waiting for its earlier writes (ordered
        // parts) stops waiting for an object it no longer holds.
        progress(ctx).bump();
    }
    Ok((json!(ReleaseReply { released }), vec![]))
}

/// The durable-progress signal of `ctx`'s worker.
fn progress(ctx: &WorkerCtx) -> std::sync::Arc<super::ctx::ProgressSignal> {
    lock(&ctx.inflight_objects).progress.clone()
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
    let owned = owned_run(&session, &route)?;
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
        let mut state = lock(&ctx.inflight_objects);
        claim_run(&mut state, owned.as_ref())?;
        // A lapsed lease (ours too) backs its object off for its source and
        // frees it (module docs).
        sweep_lapsed(&mut state, now);
        for hash in &hashes {
            let held = state.leases.get(*hash).cloned();
            match held {
                Some(lease) if superseded(&lease, &session, now) => {
                    let offset = committed.get(hash).copied().unwrap_or(lease.offset);
                    grant(&mut state, (hash, &session, source, offset), now);
                }
                Some(lease) if lease.session != session => {
                    refused.insert(hash);
                }
                // Ours and live: untouched (only progress renews it).
                Some(_) => {}
                None => {
                    let offset = committed.get(hash).copied().unwrap_or(0);
                    grant(&mut state, (hash, &session, source, offset), now);
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
    if !run_current(&lock(&ctx.inflight_objects), owned.as_ref()) {
        return Err(ErrorWire::coded(
            "IN_FLIGHT_ELSEWHERE",
            "object lease run superseded",
        ));
    }

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
    crate::util::fault::trigger("objects.write.before_lease_completion").map_err(wire)?;

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
        let mut state = lock(&ctx.inflight_objects);
        if !run_owns(&state, owned.as_ref()) {
            return Ok((
                json!(WriteReply {
                    results: items
                        .iter()
                        .map(|item| WriteResult {
                            hash: item.hash.clone(),
                            outcome: ObjectOutcome::InFlightElsewhere,
                            durable_offset: 0,
                        })
                        .collect()
                }),
                vec![],
            ));
        }
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
            let ours = state
                .leases
                .get(&result.hash)
                .is_some_and(|lease| lease.session == session);
            if owned.is_some() && !ours {
                results.push(WriteResult {
                    hash: result.hash.clone(),
                    outcome: ObjectOutcome::InFlightElsewhere,
                    durable_offset: result.durable_offset,
                });
                continue;
            }
            match &result.outcome {
                ObjectOutcome::Progressed { durable_offset } => {
                    if let Some(lease) = state.leases.get_mut(&result.hash).filter(|_| ours) {
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
                        if let Some(lease) = state.leases.get_mut(&result.hash).filter(|_| ours) {
                            lease.offset = result.durable_offset;
                        }
                    }
                    Failure::Contention => {}
                    Failure::Source => {
                        blamed
                            .entry(result.hash.clone())
                            .or_insert_with(|| error.code.clone());
                        if ours {
                            release(&mut state, &result.hash);
                        }
                    }
                    Failure::Local => {
                        local
                            .entry(result.hash.clone())
                            .or_insert_with(|| error.clone());
                        if ours {
                            release(&mut state, &result.hash);
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
                        release(&mut state, &result.hash);
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
    progress(ctx).bump();
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
    let ahead = |state: &ObjectsState| {
        first.iter().any(|(hash, offset)| {
            state
                .leases
                .get(*hash)
                .is_some_and(|lease| lease.session == session && lease.offset < *offset)
        })
    };
    let signal = {
        let state = lock(&ctx.inflight_objects);
        if !ahead(&state) {
            return;
        }
        state.progress.clone()
    };
    let deadline = Instant::now() + ORDER_WAIT;
    let (generation, wake) = (&signal.generation, &signal.wake);
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
    source_backoff(&lock(&ctx.inflight_objects), source, hash, now_ms())
}
