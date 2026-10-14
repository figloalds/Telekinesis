//! Owned by WP2.3.
//!
//! The worker status cell (REVAMP_PLAN §4.5, WP2.3 task 4): `ctx.status_cell`
//! holds the legacy status JSON plus typed members, so the `status` RPC and
//! the heartbeat never take the engine lock (invariant 30).
//!
//! Writers refresh it on a pooled secondary store (`ctx.sync_stores`), never
//! under the engine lock:
//!
//! * [`refresh_cheap`]: identity and the cheap counters
//!   (`Store::status_counters`: inbox, outbox, conflicts, tip). Called
//!   synchronously by every mutating RPC path ([`note_mutation`]) and after
//!   every activator slice;
//! * [`refresh_full`]: everything, including the content scan (the legacy
//!   `content` member and `content_counters`), the pending causal events, the
//!   journal counters and the per-peer inventory match. Run by maintenance
//!   (about 1 Hz) and by a `status` call that finds the content older than a
//!   client mutation that changed it.
//!
//! Each refresh records the `ctx.status_generation` it read before reading the
//! store; a merge never replaces newer data with an older computation. A
//! mutating path bumps `status_generation` *before* replying, so a `status`
//! call that follows it finds the cell older and recomputes first
//! (read-your-writes). A refresh that knows the engine vitals also records
//! the engine's store revision it read before the store
//! (`CellMeta::store_revision`): writes through the mount move that
//! revision, not `status_generation`, so a `status` call that finds the
//! revision moved recomputes the cheap counters too (the outbox of a file
//! just written and closed is never reported acknowledged). A mutation that
//! changed the journal or the demand also records that the content must
//! catch up ([`note_mutation`] with `content`). Background changes
//! (activator commits, object installs, writes through the mount) reach the
//! content at the next maintenance refresh.
//!
//! [`reply`] composes the `status` reply: the cell, plus the engine's
//! lock-free vitals (`mounted`, `open_handles`, `unflushed_files`, `health`,
//! notifications, the last sync observations) and the values derived from
//! them at reply time (`caught_up`, `metadata_caught_up`, `content_complete`,
//! `upload_state`). `caught_up` follows the legacy rule except that staged
//! rows count through `incoming_active` (`pending_active`): quarantined and
//! blocked rows no longer hold it false forever (CCR-WP1.6-3). Its per-peer
//! inventory match is recomputed when an exchange was observed or the
//! journal tip moved since it was computed. It never sets
//! `busy`; `stale` only when the cell is older than `STATUS_CELL_STALE` and
//! could not be refreshed.
use crate::{
    core::{
        Store,
        status::{InventoryMatch, StatusCounters, merge_object},
    },
    model::{
        health::{ActivationCounters, ContentCounters, InboxCounts, JournalCounters},
        ipc::timing::STATUS_CELL_STALE,
        orchestrator::FeedTip,
    },
    runtime::{EngineVitals, ctx::WorkerCtx},
    util::time::now_ms,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

/// How long a peer's last sync observation counts for `caught_up` (legacy).
const SYNC_FRESH_MS: u64 = 15_000;

/// The cell's own bookkeeping, stored under `"cell"` (stripped from replies).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CellMeta {
    /// The `status_generation` the counters reflect.
    pub generation: u64,
    /// The `status_generation` the content members reflect.
    pub content_generation: u64,
    /// The content must reflect at least this `status_generation`.
    pub content_wanted: u64,
    pub refreshed_at_ms: u64,
    pub content_at_ms: u64,
    /// `generations.branches`: bumped when the branch list changes.
    pub branches: u64,
    pub branches_hash: String,
    /// `generations.content`: bumped when the content counters change.
    pub content: u64,
    /// Duration of the last full refresh.
    pub full_ms: u64,
    /// When the per-peer inventory match was computed.
    pub inventory_at_ms: u64,
    /// The journal tip the per-peer inventory match was computed at (read
    /// before it): a new committed event can change the match.
    pub inventory_tip: i64,
    /// The engine's store revision (`EngineVitals::store_revision`, read
    /// before the store) that the cheap counters reflect at least; `None`
    /// when the refresh did not know it. A `status` reply recomputes the
    /// counters when the engine's connection changed the database since
    /// (writes through the mount move no `status_generation`).
    pub store_revision: Option<u64>,
}

impl CellMeta {
    pub fn of(cell: &Value) -> Self {
        serde_json::from_value(cell["cell"].clone()).unwrap_or_default()
    }
}

/// The typed members of the cell (for the heartbeat).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CellView {
    pub inbox: Option<InboxCounts>,
    pub activation: Option<ActivationCounters>,
    pub content: Option<ContentCounters>,
    pub journal: Option<JournalCounters>,
    pub tip: Option<FeedTip>,
    pub meta: CellMeta,
}

/// Reads the typed members of `cell`.
pub fn view(cell: &Value) -> CellView {
    fn typed<T: serde::de::DeserializeOwned>(value: &Value) -> Option<T> {
        if value.is_null() {
            return None;
        }
        serde_json::from_value(value.clone()).ok()
    }
    CellView {
        inbox: typed(&cell["inbox"]),
        activation: typed(&cell["activation"]),
        content: typed(&cell["content_counters"]),
        journal: typed(&cell["journal"]),
        tip: typed(&cell["tip"]),
        meta: CellMeta::of(cell),
    }
}

/// `generations.sync` (§6.1): moves after activation slices, feed and legacy
/// exchanges and every other mutation, whether it went through an RPC
/// (`status_generation`) or the engine's connection (the store revision:
/// local writes through the mount included).
pub fn sync_generation(ctx: &WorkerCtx, vitals: &EngineVitals) -> u64 {
    ctx.status_generation
        .load(Ordering::SeqCst)
        .wrapping_add(vitals.store_revision())
}

/// The cheap part of a refresh.
struct Cheap {
    target: u64,
    /// The engine's store revision read before the store, if known.
    revision: Option<u64>,
    identity: Value,
    counters: StatusCounters,
    /// The feed tip (shared events only) and epoch (WP2.5): the heartbeat's
    /// `tip`, the MetaScheduler's trigger (CCR-WP2.5-5, wired by WP2.INT).
    feed: FeedTip,
    branches_hash: String,
}

fn compute_cheap(store: &Store, target: u64, revision: Option<u64>) -> Result<Cheap> {
    let identity = store.status_identity()?;
    let counters = store.status_counters()?;
    let feed = FeedTip {
        epoch: store.feed_epoch()?,
        seq: store.feed_tip()?,
    };
    let branches_hash = crate::core::hash(&serde_json::to_vec(&store.branches()?)?);
    Ok(Cheap {
        target,
        revision,
        identity,
        counters,
        feed,
        branches_hash,
    })
}

/// The content part of a refresh.
struct Full {
    content: ContentCounters,
    legacy: Value,
    pending: Vec<String>,
    journal: JournalCounters,
    inventory: InventoryMatch,
    inventory_at_ms: u64,
}

fn compute_full(store: &Store) -> Result<Full> {
    let (content, legacy) = store.content_report()?;
    // The content scan loaded the view: the pending list is a borrow of it.
    let pending = store.pending_causal_events()?;
    let inventory_at_ms = now_ms();
    Ok(Full {
        content,
        legacy,
        pending,
        journal: store.journal_counters()?,
        inventory: store.inventory_match()?,
        inventory_at_ms,
    })
}

/// Recomputes the per-peer inventory match (`caught_up`'s last check).
fn refresh_inventory(ctx: &WorkerCtx) -> Result<()> {
    let at_ms = now_ms();
    let (tip, inventory) = {
        let store = ctx.sync_stores.checkout()?;
        // The tip first: an event committed meanwhile leaves it older.
        let tip: i64 = store
            .db
            .prepare_cached("SELECT coalesce(max(seq),0) FROM journal_sequence")?
            .query_row([], |r| r.get(0))?;
        (tip, store.inventory_match()?)
    };
    ctx.status_cell.update(|prev| {
        let mut next = object_or_empty(prev);
        let mut meta = CellMeta::of(prev);
        if at_ms >= meta.inventory_at_ms {
            next["inventory"] = json!(inventory);
            meta.inventory_at_ms = at_ms;
            meta.inventory_tip = tip;
        }
        next["cell"] = json!(meta);
        next
    });
    Ok(())
}

fn object_or_empty(prev: &Value) -> Value {
    if prev.is_object() {
        prev.clone()
    } else {
        json!({})
    }
}

/// Overlays a cheap computation unless the cell already reflects a newer
/// generation.
fn merge_cheap(next: &mut Value, meta: &mut CellMeta, cheap: &Cheap) {
    if cheap.target < meta.generation {
        return;
    }
    merge_object(next, cheap.identity.clone());
    merge_object(next, cheap.counters.legacy_json());
    next["tip"] = json!(cheap.feed);
    let mut journal: JournalCounters =
        serde_json::from_value(next["journal"].clone()).unwrap_or_default();
    journal.tip = cheap.counters.tip;
    journal.epoch = Some(cheap.feed.epoch.clone());
    journal.outgoing_unacknowledged = cheap.counters.outgoing_unacknowledged;
    journal.unresolved_conflicts = cheap.counters.unresolved_conflicts;
    next["journal"] = json!(journal);
    if meta.branches_hash != cheap.branches_hash {
        if !meta.branches_hash.is_empty() {
            meta.branches += 1;
        }
        meta.branches_hash.clone_from(&cheap.branches_hash);
    }
    meta.generation = cheap.target;
    // Unknown resets it: the next reply then recomputes rather than trust
    // counters that may be older than the revision recorded before.
    meta.store_revision = cheap.revision;
    meta.refreshed_at_ms = now_ms();
}

fn refresh_cheap_with(
    ctx: &WorkerCtx,
    vitals: Option<&EngineVitals>,
    activation: Option<ActivationCounters>,
) -> Result<()> {
    let target = ctx.status_generation.load(Ordering::SeqCst);
    let revision = vitals.map(EngineVitals::store_revision);
    let cheap = {
        let store = ctx.sync_stores.checkout()?;
        compute_cheap(&store, target, revision)?
    };
    ctx.status_cell.update(|prev| {
        let mut next = object_or_empty(prev);
        let mut meta = CellMeta::of(prev);
        merge_cheap(&mut next, &mut meta, &cheap);
        if let Some(activation) = activation {
            next["activation"] = json!(activation);
        }
        next["cell"] = json!(meta);
        next
    });
    Ok(())
}

/// Refreshes the cheap members (module docs) on a pooled secondary store.
pub fn refresh_cheap(ctx: &WorkerCtx) -> Result<()> {
    refresh_cheap_with(ctx, None, None)
}

/// [`refresh_cheap`], recording the engine's store revision it reflects.
pub fn refresh_cheap_at(ctx: &WorkerCtx, vitals: &EngineVitals) -> Result<()> {
    refresh_cheap_with(ctx, Some(vitals), None)
}

/// Refreshes every member (module docs) on a pooled secondary store.
pub fn refresh_full(ctx: &WorkerCtx) -> Result<()> {
    refresh_full_with(ctx, None)
}

/// [`refresh_full`], recording the engine's store revision it reflects
/// when `vitals` are given.
pub fn refresh_full_with(ctx: &WorkerCtx, vitals: Option<&EngineVitals>) -> Result<()> {
    let target = ctx.status_generation.load(Ordering::SeqCst);
    let revision = vitals.map(EngineVitals::store_revision);
    let started = Instant::now();
    let (cheap, full) = {
        let store = ctx.sync_stores.checkout()?;
        (
            compute_cheap(&store, target, revision)?,
            compute_full(&store)?,
        )
    };
    let full_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    ctx.status_cell.update(|prev| {
        let mut next = object_or_empty(prev);
        let mut meta = CellMeta::of(prev);
        merge_cheap(&mut next, &mut meta, &cheap);
        if target >= meta.content_generation {
            let previous: Option<ContentCounters> =
                serde_json::from_value(next["content_counters"].clone()).ok();
            if previous.is_some_and(|previous| previous != full.content) {
                meta.content += 1;
            }
            next["content"] = full.legacy.clone();
            next["content_counters"] = json!(full.content);
            next["pending_causal_events"] = json!(full.pending);
            let mut journal = full.journal.clone();
            if target < meta.generation {
                // Newer cheap counters are already merged: keep them.
                let current: JournalCounters =
                    serde_json::from_value(next["journal"].clone()).unwrap_or_default();
                journal.tip = current.tip;
                journal.outgoing_unacknowledged = current.outgoing_unacknowledged;
                journal.unresolved_conflicts = current.unresolved_conflicts;
            }
            next["journal"] = json!(journal);
            next["inventory"] = json!(full.inventory);
            meta.inventory_at_ms = full.inventory_at_ms;
            meta.inventory_tip = cheap.counters.tip;
            meta.content_generation = target;
            meta.content_at_ms = now_ms();
            meta.full_ms = full_ms;
        }
        next["cell"] = json!(meta);
        next
    });
    Ok(())
}

/// Records the activator's counters and refreshes the cheap members.
pub fn record_activation(
    ctx: &WorkerCtx,
    vitals: &EngineVitals,
    counters: ActivationCounters,
) -> Result<()> {
    refresh_cheap_with(ctx, Some(vitals), Some(counters))
}

/// A mutating path changed the store (WP2.3 task 4): bump
/// `status_generation`, record that the content must catch up when the
/// journal or the demand changed (`content`), and refresh the cheap members
/// synchronously. A refresh failure is logged: the next `status` call sees
/// the cell behind and recomputes.
pub fn note_mutation(ctx: &WorkerCtx, vitals: &EngineVitals, content: bool) {
    let generation = ctx.status_generation.fetch_add(1, Ordering::SeqCst) + 1;
    if content {
        ctx.status_cell.update(|prev| {
            let mut next = object_or_empty(prev);
            let mut meta = CellMeta::of(prev);
            meta.content_wanted = meta.content_wanted.max(generation);
            next["cell"] = json!(meta);
            next
        });
    }
    if let Err(error) = refresh_cheap_at(ctx, vitals) {
        let wire = crate::model::errors::ErrorWire::from_error(&error);
        crate::util::log::event!(warn, (wire.code.as_str()), {
            "site": "worker.status_cell",
            "message": wire.message,
        });
    }
}

/// Marks the content as wanted by the next `status` call (startup: the
/// first reply computes it if maintenance has not yet).
pub fn want_content(ctx: &WorkerCtx) {
    // A new generation, so the refresh that satisfies it can record it.
    let generation = ctx.status_generation.fetch_add(1, Ordering::SeqCst) + 1;
    ctx.status_cell.update(|prev| {
        let mut next = object_or_empty(prev);
        let mut meta = CellMeta::of(prev);
        meta.content_wanted = meta.content_wanted.max(generation);
        next["cell"] = json!(meta);
        next
    });
}

/// Whether the background activator runs and has been idle for `quiet`: no
/// ready row and no activation since then (maintenance scrubs only then).
pub fn activator_idle(ctx: &WorkerCtx, quiet: Duration) -> bool {
    if !ctx.activator_running.load(Ordering::SeqCst) {
        return false;
    }
    let cell = view(&ctx.status_cell.load());
    let ready = cell.inbox.is_none_or(|inbox| inbox.ready == 0);
    let last = cell
        .activation
        .and_then(|a| a.last_activation_at_ms)
        .unwrap_or(0);
    ready && now_ms().saturating_sub(last) >= u64::try_from(quiet.as_millis()).unwrap_or(0)
}

/// The `status` RPC reply (module docs). Recomputes first when the cell is
/// behind a mutation (`status_generation`) or behind the engine's store
/// revision (writes through the mount), when a mutation asked for the
/// content, or when the cell is older than `STATUS_CELL_STALE`.
pub fn reply(ctx: &WorkerCtx, vitals: &EngineVitals) -> Result<Value> {
    let target = ctx.status_generation.load(Ordering::SeqCst);
    let revision = vitals.store_revision();
    let mut cell = ctx.status_cell.load();
    let mut meta = CellMeta::of(&cell);
    let stale_ms = u64::try_from(STATUS_CELL_STALE.as_millis()).unwrap_or(u64::MAX);
    let mut failure = None;
    if !cell.is_object()
        || meta.generation < target
        || meta.store_revision != Some(revision)
        || now_ms().saturating_sub(meta.refreshed_at_ms) > stale_ms
    {
        if let Err(error) = refresh_cheap_at(ctx, vitals) {
            failure = Some(error);
        }
        cell = ctx.status_cell.load();
        meta = CellMeta::of(&cell);
    }
    if meta.content_generation < meta.content_wanted || cell.get("content").is_none() {
        if let Err(error) = refresh_full_with(ctx, Some(vitals)) {
            failure.get_or_insert(error);
        }
        cell = ctx.status_cell.load();
        meta = CellMeta::of(&cell);
    }
    if cell.get("repo").is_none() {
        return Err(failure.unwrap_or_else(|| anyhow::anyhow!("ENGINE_NOT_READY: no status yet")));
    }
    // `caught_up`'s inventory check reads `peer_known`, which only an
    // exchange changes, and the shared committed events: recompute it when
    // every cheaper condition holds and an exchange was observed or the
    // journal moved since it was computed.
    let sync = vitals.sync_observation();
    let latest_exchange = sync
        .last_sync
        .iter()
        .chain(sync.peers.values())
        .filter_map(|observation| observation["at"].as_u64())
        .max()
        .unwrap_or(0);
    let tip = cell["tip"]["seq"].as_i64().unwrap_or(0);
    if (latest_exchange >= meta.inventory_at_ms || tip != meta.inventory_tip)
        && cheap_caught_up(&cell, vitals)
    {
        if let Err(error) = refresh_inventory(ctx) {
            failure.get_or_insert(error);
        }
        cell = ctx.status_cell.load();
        meta = CellMeta::of(&cell);
    }
    let stale = now_ms().saturating_sub(meta.refreshed_at_ms) > stale_ms;
    Ok(compose(&cell, &meta, ctx, vitals, stale))
}

/// Every `caught_up` condition except the per-peer inventory match.
fn cheap_caught_up(cell: &Value, vitals: &EngineVitals) -> bool {
    let now = now_ms();
    let sync = vitals.sync_observation();
    let fresh = |observation: &Value| {
        observation["at"]
            .as_u64()
            .is_some_and(|at| now.saturating_sub(at) < SYNC_FRESH_MS)
    };
    sync.last_sync
        .as_ref()
        .is_some_and(|v| v["caught_up"] == true && fresh(v))
        && cell["outgoing_unacknowledged"] == 0
        && cell["incoming_active"] == 0
        && cell["pending_causal_events"] == json!([])
        && vitals.unflushed_files() == 0
        && vitals.health().is_none()
        && !sync.sync_peers.is_empty()
        && sync
            .sync_peers
            .iter()
            .all(|peer| sync.peers.get(peer).is_some_and(fresh))
}

/// The reply from a cell: the engine overlay and the derived members.
fn compose(
    cell: &Value,
    meta: &CellMeta,
    ctx: &WorkerCtx,
    vitals: &EngineVitals,
    stale: bool,
) -> Value {
    let mut s = cell.clone();
    let inventory: InventoryMatch =
        serde_json::from_value(s["inventory"].clone()).unwrap_or_default();
    if let Some(object) = s.as_object_mut() {
        object.remove("cell");
        object.remove("inventory");
    }
    let now = now_ms();
    let sync = vitals.sync_observation();
    let unflushed = vitals.unflushed_files();
    let health = vitals.health();
    s["mounted"] = json!(vitals.mounted());
    s["open_handles"] = json!(vitals.open_handles());
    s["unflushed_files"] = json!(unflushed);
    s["health"] = json!(health);
    s["notification_error"] = json!(vitals.notification_error());
    s["pending_notifications"] = json!(vitals.pending_notifications());
    s["last_sync"] = json!(sync.last_sync);
    s["peers"] = json!(sync.peers);
    s["sampled_at_ms"] = json!(now);
    let caught_up =
        cheap_caught_up(cell, vitals) && sync.sync_peers.iter().all(|peer| inventory.matches(peer));
    s["caught_up"] = json!(caught_up);
    s["metadata_caught_up"] = json!(caught_up);
    s["content_complete"] = s["content"]["complete"].clone();
    s["upload_state"] = json!(if s["branch"]["shared"] != true {
        "local-only"
    } else if sync.sync_peers.is_empty() {
        "peer-not-configured"
    } else if s["outgoing_unacknowledged"] != 0 {
        "queued"
    } else {
        "peer-acknowledged"
    });
    let mut activation: ActivationCounters =
        serde_json::from_value(s["activation"].clone()).unwrap_or_default();
    activation.running = ctx.activator_running.load(Ordering::SeqCst);
    s["activation"] = json!(activation);
    let mut feed: Vec<_> = crate::util::lock::lock(&ctx.sync_obs)
        .values()
        .cloned()
        .collect();
    feed.sort_by(|a, b| a.replica.cmp(&b.replica));
    s["feed_peers"] = json!(feed);
    s["status_generation"] = json!(meta.generation);
    if stale {
        s["stale"] = json!(true);
    }
    s
}
