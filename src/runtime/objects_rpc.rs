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
use super::objects_effects::{self, Gate};
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
        ops::WANT => want_request(
            ctx,
            header,
            Instant::now() + Duration::from_secs(60),
            &|| false,
        ),
        ops::READ => read_request(
            ctx,
            op,
            header,
            Instant::now() + Duration::from_secs(60),
            &|| false,
        ),
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
    // Before the extension, every valid string was opaque. Recognize only
    // the classic IDs actually belonging to this route for old-run fencing.
    if owned
        .as_ref()
        .is_some_and(|o| o.generation.run == 0 && o.peer != route.peer)
    {
        return Ok(None);
    }
    if owned.as_ref().is_some_and(|o| o.peer != route.peer) {
        return Err(ErrorWire::coded(
            "INVALID_REQUEST",
            "lease generation peer does not match route",
        ));
    }
    Ok(owned)
}

fn effect_family(
    ctx: &WorkerCtx,
    owned: Option<&crate::model::lease::Owned<'_>>,
    session: &str,
) -> Result<Gate, ErrorWire> {
    let key = owned
        .map(|owner| owner.family.to_owned())
        .unwrap_or_else(|| format!("opaque:{session}"));
    lock(&ctx.inflight_objects).effects.family(&key)
}
fn run_current(state: &ObjectsState, owned: Option<&crate::model::lease::Owned<'_>>) -> bool {
    owned.is_none_or(|o| {
        layout_current(state, o)
            && round_current(state, o)
            && state.lease_runs.get(o.scope).is_none_or(|g| {
                g.generation.incarnation < o.generation.incarnation
                    || (g.generation.incarnation == o.generation.incarnation
                        && (g.layout != o.layout
                            || g.generation < o.generation
                            || (g.generation == o.generation
                                && (!o.family.starts_with("push:") || g.frame <= o.frame))))
            })
    })
}

fn layout_current(state: &ObjectsState, owned: &crate::model::lease::Owned<'_>) -> bool {
    state
        .lease_layouts
        .get(owned.family)
        .is_none_or(|(seen, count)| {
            let incoming = pool_generation(owned);
            incoming > *seen
                || (incoming == *seen && (owned.layout.is_none() || owned.lane.count == *count))
        })
}
fn round_fence(owned: &crate::model::lease::Owned<'_>) -> crate::model::lease::RunFence {
    crate::model::lease::RunFence {
        generation: owned.generation,
        layout: owned.layout,
        frame: owned.frame,
    }
}
fn round_current(state: &ObjectsState, owned: &crate::model::lease::Owned<'_>) -> bool {
    owned.round_slot.is_none_or(|slot| {
        state
            .lease_rounds
            .get(owned.scope)
            .and_then(|slots| slots[slot])
            .is_none_or(|g| {
                g.generation != owned.generation
                    || g.layout != owned.layout
                    || g.frame <= owned.frame
            })
    })
}
fn run_owns(state: &ObjectsState, owned: Option<&crate::model::lease::Owned<'_>>) -> bool {
    owned.is_none_or(|o| {
        layout_current(state, o)
            && o.round_slot.is_none_or(|slot| {
                state
                    .lease_rounds
                    .get(o.scope)
                    .and_then(|slots| slots[slot])
                    == Some(round_fence(o))
            })
            && state.lease_runs.get(o.scope)
                == Some(&crate::model::lease::RunFence {
                    generation: o.generation,
                    layout: o.layout,
                    frame: fence_frame(o),
                })
            && (o.generation.run == 0
                || state.lease_layouts.get(o.family)
                    == Some(&(pool_generation(o), o.layout.map_or(0, |_| o.lane.count))))
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
        if o.generation.run > 0
            && !state.lease_layouts.contains_key(o.family)
            && state.lease_layouts.len() >= crate::model::lease::MAX_RUN_FENCES
        {
            return Err(ErrorWire::coded(
                "BUSY",
                "object lease layout fence capacity",
            ));
        }
        if o.round_slot.is_some()
            && !state.lease_rounds.contains_key(o.scope)
            && state.lease_rounds.len() >= crate::model::lease::MAX_RUN_FENCES
        {
            return Err(ErrorWire::coded(
                "CONTENT_RESOURCE_LIMIT",
                "object lease round fence capacity",
            ));
        }
        if let Some(slot) = o.round_slot {
            state.lease_rounds.entry(o.scope.to_owned()).or_default()[slot] = Some(round_fence(o));
        }
        if o.generation.run > 0 {
            state.lease_layouts.insert(
                o.family.to_owned(),
                (pool_generation(o), o.layout.map_or(0, |_| o.lane.count)),
            );
        }
        state.lease_runs.insert(
            o.scope.to_owned(),
            crate::model::lease::RunFence {
                generation: o.generation,
                layout: o.layout,
                frame: fence_frame(o),
            },
        );
    }
    Ok(())
}

fn pool_generation(owned: &crate::model::lease::Owned<'_>) -> crate::model::lease::Generation {
    crate::model::lease::Generation {
        incarnation: owned.generation.incarnation,
        run: owned.layout.unwrap_or(0),
    }
}

fn fence_frame(owned: &crate::model::lease::Owned<'_>) -> Option<u64> {
    owned
        .family
        .starts_with("push:")
        .then_some(owned.frame)
        .flatten()
}

fn superseded_lease(state: &ObjectsState, held: &ObjectLease, session: &str, now: u64) -> bool {
    crate::model::lease::parse_transfer(&held.session)
        .ok()
        .flatten()
        .is_some_and(|owned| !run_current(state, Some(&owned)))
        || superseded(held, session, now)
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
    back_off_source_locked(&mut state, source, hash, now)
}

fn back_off_source_locked(state: &mut ObjectsState, source: &str, hash: &str, now: u64) -> u64 {
    let entry = source_entry(state, source, hash, now);
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
fn grant(
    state: &mut ObjectsState,
    lease: (&str, &str, &str, u64),
    now: u64,
    family: &Gate,
) -> Result<(), ErrorWire> {
    let (hash, session, source, offset) = lease;
    if !state.leases.contains_key(hash) && state.leases.len() >= MAX_LEASE_HOLDERS {
        return Err(ErrorWire::coded(
            "CONTENT_RESOURCE_LIMIT",
            "legacy object lease capacity",
        ));
    }
    if !state.holders.contains_key(hash) && state.holders.len() >= MAX_LEASE_HOLDERS {
        let ObjectsState {
            leases, holders, ..
        } = &mut *state;
        holders.retain(|hash, (session, _)| {
            leases
                .get(hash)
                .is_some_and(|lease| lease.session == *session)
        });
        if holders.len() >= MAX_LEASE_HOLDERS {
            return Err(ErrorWire::coded(
                "CONTENT_RESOURCE_LIMIT",
                "legacy source owner capacity",
            ));
        }
    }
    state.effects.grant(hash, session, family.clone())?;
    state.leases.insert(
        hash.to_owned(),
        ObjectLease {
            session: session.to_owned(),
            expires_ms: now + LEASE_TTL.as_millis() as u64,
            offset,
        },
    );
    state
        .holders
        .insert(hash.to_owned(), (session.to_owned(), source.to_owned()));
    Ok(())
}

/// One pressure scan per bounded grant batch. Retire only leases whose ordered
/// run/pool/frame is already fenced; opaque or current live owners are kept.
/// Capture at most one request page of keys, not the whole lease table.
fn reclaim_fenced_leases<'a>(state: &mut ObjectsState, candidates: impl Iterator<Item = &'a str>) {
    let new: BTreeSet<_> = candidates
        .filter(|hash| !state.leases.contains_key(*hash))
        .collect();
    let needed = state
        .leases
        .len()
        .saturating_add(new.len())
        .saturating_sub(MAX_LEASE_HOLDERS)
        .min(MAX_WANT_ITEMS);
    if needed == 0 {
        return;
    }
    let stale: Vec<_> = state
        .leases
        .iter()
        .filter(|(_, lease)| {
            crate::model::lease::parse_transfer(&lease.session)
                .ok()
                .flatten()
                .is_some_and(|owned| !run_current(state, Some(&owned)))
        })
        .take(needed)
        .map(|(hash, _)| hash.clone())
        .collect();
    for hash in stale {
        if let Some(lease) = state.leases.get(&hash) {
            state.effects.cancel(&hash, &lease.session.clone());
        }
        release(state, &hash);
    }
}

/// Ends the lease of `hash` (a terminal outcome, a local failure, or the
/// session gave it up).
fn release(state: &mut ObjectsState, hash: &str) {
    state.leases.remove(hash);
    state.holders.remove(hash);
    state.effects.ended(hash);
}

/// Ends the leases `session` holds on `hashes` (module docs, "Giving a lease
/// up"); returns how many ended. A lease of another session is untouched.
fn give_up(state: &mut ObjectsState, session: &str, hashes: &[String]) -> u32 {
    let mut released = 0;
    for hash in hashes {
        state.effects.cancel(hash, session);
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
            if pool_generation(&old) < pool_generation(&new) {
                return true;
            }
            if pool_generation(&old) > pool_generation(&new) {
                return false;
            }
            // Background advances only its own slot. Foreground may promote
            // either background slot, independently of their round counters.
            let newer_frame = new.frame.is_some()
                && if old.round_slot == new.round_slot {
                    old.frame < new.frame
                } else {
                    new.round_slot.is_none() && new.family.starts_with("pull:")
                };
            if old.scope == new.scope
                && (old.generation < new.generation
                    || (old.generation == new.generation && newer_frame))
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
        state.effects.cancel(&hash, &session);
        state.leases.remove(&hash);
        let Some((holder, source)) = state.holders.remove(&hash) else {
            continue;
        };
        if holder != session {
            continue;
        }
        // A replacement pool/run is already authoritative. Expiry of its
        // predecessor is cleanup, not evidence that the current source failed.
        if crate::model::lease::parse_transfer(&session)
            .ok()
            .flatten()
            .is_some_and(|owned| !run_current(state, Some(&owned)))
        {
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
pub(super) fn want_request(
    ctx: &WorkerCtx,
    header: &Value,
    deadline: Instant,
    cancelled: &dyn Fn() -> bool,
) -> Result<(Value, Vec<u8>), ErrorWire> {
    let stopping = lock(&ctx.inflight_objects).legacy_cancel.clone();
    let check = || {
        if cancelled() || stopping.is_cancelled() || ctx.stopping.load(Ordering::Relaxed) {
            return Err(ErrorWire::coded(
                "CONTENT_CANCELLED",
                "objects.want cancelled",
            ));
        }
        if Instant::now() >= deadline {
            return Err(ErrorWire::coded(
                "CONTENT_DEADLINE",
                "objects.want deadline",
            ));
        }
        Ok(())
    };
    let result = (|| {
        check()?;
        want_selected(ctx, parse(ops::WANT, header)?, None, &check)
    })();
    if result.as_ref().is_err_and(|error| {
        matches!(
            error.code.as_str(),
            "CONTENT_CANCELLED" | "CONTENT_DEADLINE"
        )
    }) {
        crate::util::fault::trigger("objects.want.stopped").map_err(wire)?;
    }
    result
}

pub(super) fn legacy_prepare(
    ctx: &WorkerCtx,
    header: &Value,
) -> Result<(Value, Vec<u8>), ErrorWire> {
    legacy_prepare_request(
        ctx,
        header,
        Instant::now() + Duration::from_secs(5),
        &|| false,
    )
}

pub(super) fn legacy_prepare_request(
    ctx: &WorkerCtx,
    header: &Value,
    deadline: Instant,
    cancelled: &dyn Fn() -> bool,
) -> Result<(Value, Vec<u8>), ErrorWire> {
    let stopping = lock(&ctx.inflight_objects).legacy_cancel.clone();
    let check = || {
        if cancelled() || stopping.is_cancelled() {
            return Err(ErrorWire::coded(
                "CONTENT_CANCELLED",
                "legacy prepare cancelled",
            ));
        }
        if Instant::now() >= deadline {
            return Err(ErrorWire::coded(
                "CONTENT_DEADLINE",
                "legacy prepare deadline",
            ));
        }
        Ok(())
    };
    let result = (|| {
        check()?;
        let request: crate::model::content_wire::LegacyPrepare = parse("legacy-prepare", header)?;
        crate::model::content::Digest::parse(&request.hash).map_err(wire)?;
        if crate::model::lease::parse(&request.session)?.is_none_or(|o| {
            o.frame.is_none() || o.round_slot.is_some() || !o.family.starts_with("pull:")
        }) {
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
            &check,
        )
    })();
    if result.as_ref().is_err_and(|error| {
        matches!(
            error.code.as_str(),
            "CONTENT_CANCELLED" | "CONTENT_DEADLINE"
        )
    }) {
        crate::util::fault::trigger("legacy.prepare.stopped").map_err(wire)?;
    }
    result
}

pub(super) fn legacy_read(ctx: &WorkerCtx, header: &Value) -> Result<(Value, Vec<u8>), ErrorWire> {
    read_request(
        ctx,
        crate::model::content_wire::LEGACY_READ,
        header,
        Instant::now() + Duration::from_secs(5),
        &|| false,
    )
}

/// Loopback request cancellation reaches first-serve hashing. The responder's
/// predicate is borrowed for this call; no hook/thread survives the request.
pub(super) fn read_request(
    ctx: &WorkerCtx,
    op: &str,
    header: &Value,
    deadline: Instant,
    cancelled: &dyn Fn() -> bool,
) -> Result<(Value, Vec<u8>), ErrorWire> {
    let foreground = op == crate::model::content_wire::LEGACY_READ;
    let header: ReadHeader = parse(op, header)?;
    bind(ctx, &header.route)?;
    if foreground && header.len > crate::model::content_wire::FRAME_BYTES as u64 {
        return Err(ErrorWire::coded(
            "CONTENT_BUFFER_LIMIT",
            "legacy foreground read",
        ));
    }
    let context = crate::content::ReadContext {
        deadline: if foreground {
            deadline.min(Instant::now() + Duration::from_secs(5))
        } else {
            deadline
        },
        cancel: lock(&ctx.inflight_objects).legacy_cancel.child(),
    };
    let check = || {
        context.check()?;
        anyhow::ensure!(
            !cancelled() && !ctx.stopping.load(Ordering::Relaxed),
            "CONTENT_CANCELLED"
        );
        Ok(())
    };
    check().map_err(wire)?;
    let provider = foreground
        .then(|| crate::content::LocalContent::open(&ctx.root))
        .transpose()
        .map_err(wire)?;
    let _buffers = provider
        .as_ref()
        .map(|p| p.foreground_permit_checked(&context, &check))
        .transpose()
        .map_err(wire)?;
    let store = super::content_rpc::connection(ctx).map_err(wire)?;
    let (total, bytes) = store.read_object_range_checked(
        &header.hash,
        header.offset,
        header.len.min(MAX_READ_LEN) as usize,
        check,
    )?;
    check().map_err(wire)?;
    Ok((json!(ReadReply { total }), bytes))
}

fn want_selected(
    ctx: &WorkerCtx,
    request: WantRequest,
    only: Option<&str>,
    check: &dyn Fn() -> Result<(), ErrorWire>,
) -> Result<(Value, Vec<u8>), ErrorWire> {
    check()?;
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
    let family = effect_family(ctx, owned.as_ref(), session)?;
    let admission_deadline = Instant::now() + ORDER_WAIT;
    let source = header.route.remote_replica.as_str();
    let now = now_ms();
    let excluded: HashSet<&str> = exclude.iter().map(String::as_str).collect();
    // Lapsed leases back their objects off for their sources first, and the
    // session gives up what it excludes; then a snapshot (the store's scan
    // runs without the lock): objects other sessions hold, the ones this
    // session holds, and the ones backed off for this source.
    let (leased, mine, backed_off): (HashSet<String>, HashSet<String>, HashSet<String>) = {
        let _effect = wait_family_gate(
            ctx,
            &family,
            owned.as_ref(),
            admission_deadline,
            check,
            "objects.want.family_waiting",
        )?;
        crate::util::fault::hang("objects.want.family_effect_held");
        let mut state = lock(&ctx.inflight_objects);
        check()?;
        claim_run(&mut state, owned.as_ref())?;
        sweep_lapsed(&mut state, now);
        // A fresh offer frame also retires excluded predecessor leases.
        let retired: Vec<_> = exclude
            .iter()
            .filter_map(|hash| {
                state
                    .leases
                    .get(hash)
                    .filter(|lease| superseded_lease(&state, lease, session, now))
                    .map(|lease| (hash.clone(), lease.session.clone()))
            })
            .collect();
        give_up(&mut state, session, &exclude);
        for (hash, prior) in retired {
            give_up(&mut state, &prior, &[hash]);
        }
        let (mine, others): (Vec<_>, Vec<_>) = state
            .leases
            .iter()
            .filter(|(_, lease)| !superseded_lease(&state, lease, session, now))
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
        let _effect = wait_family_gate(
            ctx,
            &family,
            owned.as_ref(),
            admission_deadline,
            check,
            "objects.want.family_waiting",
        )?;
        let mut state = lock(&ctx.inflight_objects);
        check()?;
        claim_run(&mut state, owned.as_ref())?;
        sweep_lapsed(&mut state, now);
        let mut deferred = None;
        let mut newly_granted = Vec::new();
        reclaim_fenced_leases(&mut state, items.iter().map(|item| item.hash.as_str()));
        items.retain_mut(|item| {
            let held = state.leases.get(&item.hash).cloned();
            match held {
                Some(lease) if superseded_lease(&state, &lease, session, now) => {
                    let offset = lease.offset.max(item.offset);
                    item.offset = offset;
                    let granted = grant(
                        &mut state,
                        (&item.hash, session, source, offset),
                        now,
                        &family,
                    );
                    if let Err(error) = &granted {
                        deferred = Some(error.clone());
                    } else {
                        newly_granted.push(item.hash.clone());
                    }
                    granted.is_ok()
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
                    let granted = grant(
                        &mut state,
                        (&item.hash, session, source, item.offset),
                        now,
                        &family,
                    );
                    if let Err(error) = &granted {
                        deferred = Some(error.clone());
                    } else {
                        newly_granted.push(item.hash.clone());
                    }
                    granted.is_ok()
                }
            }
        });
        if let Err(error) = check() {
            // Still under the grant lock: only this request's fresh grants
            // may be undone. A cancelled retry must keep previously offered
            // leases and their in-flight writers alive.
            give_up(&mut state, session, &newly_granted);
            return Err(error);
        }
        if items.is_empty()
            && let Some(error) = deferred
        {
            return Err(error);
        }
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
    let family = effect_family(ctx, owned.as_ref(), &header.session)?;
    let _effect = objects_effects::try_gate(&family)?;
    let released = {
        let mut state = lock(&ctx.inflight_objects);
        if !run_owns(&state, owned.as_ref()) {
            drop(state);
            crate::util::fault::trigger("objects.release.stale").map_err(wire)?;
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
    let family = effect_family(ctx, owned.as_ref(), &session)?;
    let admission_deadline = Instant::now() + ORDER_WAIT;
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
    let mut admission_errors: HashMap<&str, ErrorWire> = HashMap::new();
    let mut work = HashMap::new();
    {
        let _effect = write_family_gate(ctx, &family, owned.as_ref(), admission_deadline)?;
        let now = now_ms();
        let mut state = lock(&ctx.inflight_objects);
        claim_run(&mut state, owned.as_ref())?;
        // A lapsed lease (ours too) backs its object off for its source and
        // frees it (module docs).
        sweep_lapsed(&mut state, now);
        reclaim_fenced_leases(&mut state, hashes.iter().copied());
        for hash in &hashes {
            let held = state.leases.get(*hash).cloned();
            match held {
                Some(lease) if superseded_lease(&state, &lease, &session, now) => {
                    let offset = committed.get(hash).copied().unwrap_or(lease.offset);
                    if let Err(error) =
                        grant(&mut state, (hash, &session, source, offset), now, &family)
                    {
                        refused.insert(hash);
                        admission_errors.insert(hash, error);
                    }
                }
                Some(lease) if lease.session != session => {
                    refused.insert(hash);
                }
                // Ours and live: untouched (only progress renews it).
                Some(_) => {}
                None => {
                    let offset = committed.get(hash).copied().unwrap_or(0);
                    if let Err(error) =
                        grant(&mut state, (hash, &session, source, offset), now, &family)
                    {
                        refused.insert(hash);
                        admission_errors.insert(hash, error);
                    }
                }
            }
            if !refused.contains(hash) {
                match state.effects.ensure(hash, &session, family.clone()) {
                    Ok(owned) => {
                        work.insert((*hash).to_owned(), owned);
                    }
                    Err(error) => {
                        refused.insert(hash);
                        admission_errors.insert(hash, error);
                    }
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
    wait_for_order(ctx, &session, &first, admission_deadline);
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
                    outcome: admission_errors
                        .get(item.hash.as_str())
                        .cloned()
                        .map_or(ObjectOutcome::InFlightElsewhere, ObjectOutcome::Failed),
                    durable_offset: 0,
                });
                continue;
            }
            let Some(result) = written.next() else { break };
            let ours = state
                .leases
                .get(&result.hash)
                .is_some_and(|lease| lease.session == session);
            if work.get(&result.hash).is_none_or(|work| !work.current())
                || (owned.is_some() && !ours)
            {
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
        crate::util::fault::trigger("objects.write.before_source_backoff").map_err(wire)?;
        let retry_at = {
            let mut state = lock(&ctx.inflight_objects);
            if !run_owns(&state, owned.as_ref())
                || work.get(hash).is_none_or(|work| !work.current())
            {
                continue;
            }
            back_off_source_locked(&mut state, source, hash, now_ms())
        };
        crate::util::log::event!(warn, (code.as_str()), {
            "site": "worker.objects.write",
            "hash": hash,
            "source": source,
            "retry_at_ms": retry_at,
        });
    }
    if !local.is_empty() {
        // Best effort: the outcome is answered either way.
        if let Ok(store) = super::content_rpc::connection(ctx) {
            for (hash, error) in &local {
                crate::util::fault::trigger("objects.write.before_local_backoff").map_err(wire)?;
                let Some(work) = work.get(hash) else { continue };
                let Ok(_family) = objects_effects::try_gate(&work.family) else {
                    continue;
                };
                let Ok(_object) = work.object_gate() else {
                    continue;
                };
                {
                    let state = lock(&ctx.inflight_objects);
                    if !run_owns(&state, owned.as_ref()) || !work.current() {
                        continue;
                    }
                }
                let _ = store.record_unavailable_object_failure(hash, error);
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
fn wait_for_order(ctx: &WorkerCtx, session: &str, first: &HashMap<&str, u64>, deadline: Instant) {
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

/// Short effect publication in this family is readiness, not a failed stream.
/// Wait without Engine, the lease-state mutex or any object gate; claim_run
/// still revalidates under the family gate before any byte work can begin.
fn write_family_gate<'a>(
    ctx: &WorkerCtx,
    family: &'a Gate,
    owned: Option<&crate::model::lease::Owned<'_>>,
    deadline: Instant,
) -> Result<std::sync::MutexGuard<'a, ()>, ErrorWire> {
    wait_family_gate(
        ctx,
        family,
        owned,
        deadline,
        &|| Ok(()),
        "objects.write.family_waiting",
    )
}
fn wait_family_gate<'a>(
    ctx: &WorkerCtx,
    family: &'a Gate,
    owned: Option<&crate::model::lease::Owned<'_>>,
    deadline: Instant,
    check: &dyn Fn() -> Result<(), ErrorWire>,
    point: &str,
) -> Result<std::sync::MutexGuard<'a, ()>, ErrorWire> {
    let cancelled = lock(&ctx.inflight_objects).legacy_cancel.clone();
    loop {
        if cancelled.is_cancelled() || ctx.stopping.load(Ordering::Relaxed) {
            // Preserve the request owner's specific stop/cancel outcome before
            // the worker-level fallback, including an uncontended admission.
            check()?;
            return Err(ErrorWire::coded(
                "CANCELLED",
                "object family admission cancelled",
            ));
        }
        match objects_effects::try_gate(family) {
            Ok(guard) => return Ok(guard),
            Err(error) if error.code == "WORKER_BUSY" => {
                check()?;
                if !run_current(&lock(&ctx.inflight_objects), owned) {
                    return Err(ErrorWire::coded(
                        "IN_FLIGHT_ELSEWHERE",
                        "object family run superseded",
                    ));
                }
                if Instant::now() >= deadline {
                    return Err(error);
                }
                crate::util::fault::hang(point);
                cancelled.sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod write_admission_tests {
    use super::*;
    #[test]
    fn family_wait_deadline_and_worker_stop_do_not_claim_or_mutate_work() {
        let dir = tempfile::tempdir().unwrap();
        let shared = std::sync::Arc::new(std::sync::Mutex::new(crate::runtime::Engine::new(
            crate::core::Store::initialize(dir.path(), "gate-wait", &crate::core::id()).unwrap(),
        )));
        let ctx = crate::worker::build_ctx(shared, dir.path(), false, None);
        let family = std::sync::Arc::new(std::sync::Mutex::new(()));
        let held = family.lock().unwrap();
        let began = Instant::now();
        assert_eq!(
            write_family_gate(&ctx, &family, None, began + Duration::from_millis(35))
                .unwrap_err()
                .code,
            "WORKER_BUSY"
        );
        assert!(began.elapsed() < Duration::from_millis(250));
        let waiting_ctx = ctx.clone();
        let waiting_family = family.clone();
        let waiter = std::thread::spawn(move || {
            write_family_gate(
                &waiting_ctx,
                &waiting_family,
                None,
                Instant::now() + ORDER_WAIT,
            )
            .map(drop)
            .map_err(|e| e.code)
        });
        std::thread::sleep(Duration::from_millis(10));
        let stopped = Instant::now();
        ctx.stopping.store(true, Ordering::Release);
        assert_eq!(waiter.join().unwrap().unwrap_err(), "CANCELLED");
        assert!(stopped.elapsed() < Duration::from_millis(250));
        assert!(lock(&ctx.inflight_objects).leases.is_empty());
        assert!(lock(&ctx.inflight_objects).lease_runs.is_empty());
        drop(held);
    }
}

/// The retry time of `hash` from `source`, while that source is backed off
/// for it (`CorruptAtSource`); `None` otherwise.
pub fn source_retry_at(ctx: &WorkerCtx, source: &str, hash: &str) -> Option<u64> {
    source_backoff(&lock(&ctx.inflight_objects), source, hash, now_ms())
}

#[cfg(test)]
mod lease_capacity_tests {
    use super::*;
    #[test]
    fn want_waits_for_both_family_phases_and_cancellation_does_not_grant_work() {
        use crate::{
            core::Store,
            runtime::Engine,
            util::fault::{self, FaultAction, FaultConfig},
        };
        use std::sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize},
        };
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                fault::configure(None);
            }
        }
        let _reset = Reset;
        for phase in ["snapshot", "grant"] {
            for cancel in [false, true] {
                fault::configure(None);
                let dir = tempfile::tempdir().unwrap();
                let mut store =
                    Store::initialize(dir.path(), &crate::core::id(), &crate::core::id()).unwrap();
                let entry = store.create("file", "file").unwrap();
                let bytes = vec![219; 100];
                store
                    .write_revision(&entry.id, entry.revisions.get("content").cloned(), &bytes)
                    .unwrap();
                let hash = crate::core::hash(&bytes);
                std::fs::remove_file(store.root.join("objects").join(&hash)).unwrap();
                store
                    .db
                    .execute("DELETE FROM published_objects WHERE hash=?", [&hash])
                    .unwrap();
                store
                    .db
                    .execute("DELETE FROM object_availability WHERE hash=?", [&hash])
                    .unwrap();
                let route = SyncRoute {
                    peer: "peer".into(),
                    repo: store.repo.clone(),
                    local_replica: store.device.clone(),
                    remote_replica: "remote".into(),
                };
                let ctx = crate::worker::build_ctx(
                    Arc::new(Mutex::new(Engine::new(store))),
                    dir.path(),
                    false,
                    None,
                );
                let family = effect_family(&ctx, None, "waiting-want").unwrap();
                let mut held = if phase == "snapshot" {
                    Some(family.lock().unwrap())
                } else {
                    let mut config =
                        FaultConfig::new("objects.want.before_grant", FaultAction::Hang);
                    config.ms = Some(200);
                    config.count = Some(1);
                    fault::configure(Some(config));
                    None
                };
                let cancelled = Arc::new(AtomicBool::new(false));
                let checks = Arc::new(AtomicUsize::new(0));
                let (worker, stop, observed) = (ctx.clone(), cancelled.clone(), checks.clone());
                let wanting = std::thread::spawn(move || {
                    want_selected(
                        &worker,
                        WantRequest {
                            header: crate::model::protocol::objects::WantHeader {
                                route,
                                session: "waiting-want".into(),
                                lane: crate::model::protocol::LaneSpec::new(0, 1),
                                max_items: 16,
                                max_bytes: 1024,
                            },
                            exclude: vec![crate::core::hash(&[])],
                        },
                        None,
                        &|| {
                            observed.fetch_add(1, Ordering::Release);
                            if stop.load(Ordering::Acquire) {
                                Err(ErrorWire::coded("CANCELLED", "want cancelled"))
                            } else {
                                Ok(())
                            }
                        },
                    )
                });
                let until = Instant::now() + Duration::from_secs(2);
                if phase == "grant" {
                    while fault::fired() == 0 {
                        assert!(Instant::now() < until);
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    held = Some(family.lock().unwrap());
                }
                let expected = if phase == "snapshot" { 2 } else { 3 };
                while checks.load(Ordering::Acquire) < expected {
                    assert!(Instant::now() < until);
                    std::thread::sleep(Duration::from_millis(1));
                }
                assert!(lock(&ctx.inflight_objects).leases.is_empty());
                assert!(ctx.engine.try_lock().is_ok());
                if cancel {
                    let began = Instant::now();
                    cancelled.store(true, Ordering::Release);
                    assert_eq!(wanting.join().unwrap().unwrap_err().code, "CANCELLED");
                    assert!(began.elapsed() < Duration::from_millis(250));
                    assert!(lock(&ctx.inflight_objects).leases.is_empty());
                    drop(held);
                } else {
                    drop(held);
                    let reply = wanting.join().unwrap().unwrap().0;
                    assert!(
                        reply["items"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|item| item["hash"] == hash)
                    );
                    assert_eq!(
                        lock(&ctx.inflight_objects).leases[&hash].session,
                        "waiting-want"
                    );
                }
            }
        }
    }
    #[test]
    fn cancelled_want_rolls_back_fresh_grants_and_keeps_preexisting_work() {
        use crate::{core::Store, runtime::Engine};
        use std::{
            cell::Cell,
            sync::{Arc, Mutex},
        };
        let dir = tempfile::tempdir().unwrap();
        let mut store =
            Store::initialize(dir.path(), &crate::core::id(), &crate::core::id()).unwrap();
        let mut hashes = Vec::new();
        for (name, bytes) in [("a", vec![21; 100]), ("b", vec![22; 200])] {
            let entry = store.create(name, "file").unwrap();
            store
                .write_revision(&entry.id, entry.revisions.get("content").cloned(), &bytes)
                .unwrap();
            let hash = crate::core::hash(&bytes);
            std::fs::remove_file(store.root.join("objects").join(&hash)).unwrap();
            store
                .db
                .execute("DELETE FROM published_objects WHERE hash=?", [&hash])
                .unwrap();
            store
                .db
                .execute("DELETE FROM object_availability WHERE hash=?", [&hash])
                .unwrap();
            hashes.push(hash);
        }
        let route = SyncRoute {
            peer: "peer".into(),
            repo: store.repo.clone(),
            local_replica: store.device.clone(),
            remote_replica: "remote".into(),
        };
        let ctx = crate::worker::build_ctx(
            Arc::new(Mutex::new(Engine::new(store))),
            dir.path(),
            false,
            None,
        );
        let request = |max_items| WantRequest {
            header: crate::model::protocol::objects::WantHeader {
                route: route.clone(),
                lane: crate::model::protocol::LaneSpec::new(0, 1),
                max_items,
                max_bytes: 1024,
                session: "retained-session".into(),
            },
            exclude: vec![],
        };
        let reply = want_selected(&ctx, request(1), None, &|| Ok(())).unwrap();
        assert_eq!(reply.0["items"].as_array().unwrap().len(), 1);
        let (prior, ticket) = {
            let state = lock(&ctx.inflight_objects);
            (
                state.leases[&hashes[0]].clone(),
                state.effects.work(&hashes[0]).unwrap(),
            )
        };
        let checks = Cell::new(0);
        let error = want_selected(&ctx, request(2), None, &|| {
            checks.set(checks.get() + 1);
            // Entry, snapshot, grant lock, then post-grant under that lock.
            if checks.get() == 4 {
                Err(ErrorWire::coded(
                    "CONTENT_CANCELLED",
                    "post-grant cancellation",
                ))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
        assert_eq!(error.code, "CONTENT_CANCELLED");
        assert_eq!(checks.get(), 4);
        let state = lock(&ctx.inflight_objects);
        assert_eq!(state.leases.len(), 1);
        assert_eq!(state.leases[&hashes[0]], prior);
        assert!(
            ticket.current(),
            "cancelled retry retired its previous writer"
        );
        assert!(!state.holders.contains_key(&hashes[1]));
        assert!(state.effects.work(&hashes[1]).is_none());
        assert!(state.source_backoff.is_empty());
    }
    fn filled(session: &str) -> (ObjectsState, Gate) {
        let mut state = ObjectsState::default();
        let family = state.effects.family("capacity").unwrap();
        for n in 0..MAX_LEASE_HOLDERS {
            grant(
                &mut state,
                (&format!("{n:064x}"), session, "source", 0),
                100,
                &family,
            )
            .unwrap();
        }
        (state, family)
    }
    #[test]
    fn full_lease_tables_preserve_existing_sources_and_refuse_new_live_keys() {
        let (mut state, family) = filled("capacity");
        let first = format!("{:064x}", 0);
        let other = format!("{:064x}", MAX_LEASE_HOLDERS - 1);
        let old = state.effects.work(&first).unwrap();
        let retained = state.effects.work(&other).unwrap();
        grant(
            &mut state,
            (&first, "replacement", "new-source", 23),
            200,
            &family,
        )
        .unwrap();
        assert_eq!(state.leases.len(), MAX_LEASE_HOLDERS);
        assert_eq!(state.holders.len(), MAX_LEASE_HOLDERS);
        assert_eq!(state.holders[&other], ("capacity".into(), "source".into()));
        assert!(!old.current() && retained.current());
        let extra = format!("{:064x}", MAX_LEASE_HOLDERS);
        let began = Instant::now();
        reclaim_fenced_leases(&mut state, std::iter::once(extra.as_str()));
        assert_eq!(
            grant(&mut state, (&extra, "new", "another", 0), 200, &family)
                .unwrap_err()
                .code,
            "CONTENT_RESOURCE_LIMIT"
        );
        let denied = began.elapsed();
        assert_eq!(state.holders.len(), MAX_LEASE_HOLDERS);
        assert_eq!(
            give_up(&mut state, "replacement", std::slice::from_ref(&first)),
            1
        );
        // A delayed writer still holds the old slot: do not evict that fence.
        assert_eq!(
            grant(&mut state, (&extra, "new", "another", 0), 200, &family)
                .unwrap_err()
                .code,
            "CONTENT_RESOURCE_LIMIT"
        );
        drop(old);
        grant(&mut state, (&extra, "new", "another", 0), 200, &family).unwrap();
        assert!(retained.current());
        assert_eq!(state.leases.len(), MAX_LEASE_HOLDERS);
        assert_eq!(state.holders.len(), MAX_LEASE_HOLDERS);
        println!(
            "WAVE6_CAPACITY {}",
            json!({"scenario":"full-live-owner-tables","entries":MAX_LEASE_HOLDERS,
            "new_key_refused":true,"existing_sources_preserved":true,"delayed_writer_fence_preserved":true,
            "pressure_scan_ms":denied.as_secs_f64()*1000.0})
        );
    }
    #[test]
    fn holder_pressure_removes_only_orphan_or_mismatched_provenance() {
        let mut state = ObjectsState::default();
        let family = state.effects.family("capacity").unwrap();
        let live = format!("{:064x}", 0);
        let mismatched = format!("{:064x}", 1);
        state.leases.insert(
            live.clone(),
            ObjectLease {
                session: "live".into(),
                expires_ms: 1000,
                offset: 0,
            },
        );
        state.leases.insert(
            mismatched.clone(),
            ObjectLease {
                session: "correct".into(),
                expires_ms: 1000,
                offset: 0,
            },
        );
        for n in 0..MAX_LEASE_HOLDERS {
            state
                .holders
                .insert(format!("{n:064x}"), ("orphan".into(), "wrong".into()));
        }
        state
            .holders
            .insert(live.clone(), ("live".into(), "kept-source".into()));
        let extra = format!("{:064x}", MAX_LEASE_HOLDERS);
        grant(&mut state, (&extra, "new", "new-source", 0), 100, &family).unwrap();
        assert_eq!(state.holders[&live], ("live".into(), "kept-source".into()));
        assert!(!state.holders.contains_key(&mismatched));
        assert!(state.leases.contains_key(&mismatched));
        assert_eq!(state.holders.len(), 2);
        assert_eq!(state.leases.len(), 3);
    }
    #[test]
    fn lease_pressure_reclaims_only_already_fenced_owners_without_source_backoff() {
        use crate::model::lease::{self, Generation};
        let old = lease::qualify(
            "pull:capacity:0/1:a",
            Generation {
                incarnation: 1,
                run: 1,
            },
        )
        .unwrap();
        let new = lease::qualify(
            "pull:capacity:0/1:a",
            Generation {
                incarnation: 1,
                run: 2,
            },
        )
        .unwrap();
        let (mut state, family) = filled(&old);
        let first = format!("{:064x}", 0);
        let delayed = state.effects.work(&first).unwrap();
        claim_run(&mut state, lease::parse(&new).unwrap().as_ref()).unwrap();
        let extra = format!("{:064x}", MAX_LEASE_HOLDERS);
        let began = Instant::now();
        reclaim_fenced_leases(&mut state, std::iter::once(extra.as_str()));
        assert_eq!(state.leases.len(), MAX_LEASE_HOLDERS - 1);
        assert_eq!(state.holders.len(), MAX_LEASE_HOLDERS - 1);
        assert!(!delayed.current());
        assert!(state.source_backoff.is_empty());
        drop(delayed);
        grant(&mut state, (&extra, &new, "source", 0), 200, &family).unwrap();
        assert_eq!(state.leases.len(), MAX_LEASE_HOLDERS);
        assert_eq!(state.holders.len(), MAX_LEASE_HOLDERS);
        println!(
            "WAVE6_CAPACITY {}",
            json!({"scenario":"fenced-owner-pressure","entries":MAX_LEASE_HOLDERS,
            "reclaimed":1,"source_backoff_added":false,"pressure_and_grant_ms":began.elapsed().as_secs_f64()*1000.0})
        );
    }
}
