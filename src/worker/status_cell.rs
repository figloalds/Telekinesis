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
//!   journal counters and the per-peer inventory match (the last two cached
//!   per connection until the database changes, `core::status::Cache`). Run
//!   by maintenance (about 1 Hz) and by the first `status` call. A `status`
//!   call that finds the content older than a client mutation that changed
//!   it has the content and cheap members refreshed on a background thread
//!   (`status-content`, one at a time per store): it waits for that refresh
//!   only when the last one took at most [`READ_YOUR_WRITES_BUDGET`] (then
//!   it reads its writes); otherwise it answers from the cell at once, with
//!   `content_pending: true`, and the refresh lands in the cell for the next
//!   call. The refresh after another connection's commit reloads the
//!   projection (cold, O(projection): about 2 s at 50K events), so it never
//!   runs on the caller (WP2.INT's post-integration re-review and its
//!   confirmation review).
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
//! `upload_state`, `content_pending`). `caught_up` follows the legacy rule except that staged
//! rows count through `incoming_active` (`pending_active`): quarantined and
//! blocked rows no longer hold it false forever (CCR-WP1.6-3). Its per-peer
//! inventory match is recomputed when an exchange was observed or the feed
//! tip (shared events only, like the cell's `tip`) moved since it was
//! computed. It never sets
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
    util::{lock::lock, time::now_ms},
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Condvar, Mutex, atomic::Ordering},
    thread,
    time::{Duration, Instant},
};

/// How long a peer's last sync observation counts for `caught_up` (legacy).
const SYNC_FRESH_MS: u64 = 15_000;
/// A `status` call after a client mutation waits for the background content
/// refresh when one is expected to take at most this long (a cold content
/// scan: 17 ms at 2K entries, 47 ms at 5K, 220 ms at 20K, release), so it
/// reads its writes; a longer one is not waited for (`content_pending`).
pub const READ_YOUR_WRITES_BUDGET: Duration = Duration::from_millis(100);
/// On a store whose refresh is expected to take longer than
/// [`READ_YOUR_WRITES_BUDGET`], a background content refresh starts only
/// this many times the last one's duration after it ended (at most about
/// half a core while a client mutates a large store and polls `status`;
/// maintenance also refreshes).
const SLOW_REFRESH_SPACING: u32 = 1;

/// Fault points of the status cell (`util::fault`).
pub mod faults {
    /// `<CONTENT_REFRESH>.<repo>`: at the start of a background content
    /// refresh (tests: a slow refresh; the repo keeps it to one in-process
    /// worker).
    pub const CONTENT_REFRESH: &str = "worker.status_cell.content";
}

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
    /// Duration of the cell's first full refresh: cold (a new process's
    /// connections have no view loaded), so what a content refresh after a
    /// commit is expected to cost before one ran in the background
    /// (`catch_up_content`). The later ones read cached scans while the
    /// store is idle.
    pub first_full_ms: Option<u64>,
    /// When the per-peer inventory match was computed.
    pub inventory_at_ms: u64,
    /// The feed tip (shared events only, like the cell's `tip`) the per-peer
    /// inventory match was computed at (read before it): a new shared
    /// committed event can change the match, a private one cannot. It was
    /// the journal tip, which never equals the cell's feed tip once the
    /// newest event is private, so every `status` recomputed the match
    /// (post-integration re-review, WP2.INT).
    pub inventory_tip: i64,
    /// The feed epoch of `inventory_tip`.
    pub inventory_epoch: String,
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

/// The content part of a refresh; the journal counters and the inventory
/// match only in a full one (`totals`).
struct Full {
    content: ContentCounters,
    legacy: Value,
    pending: Vec<String>,
    totals: Option<(JournalCounters, InventoryMatch, u64)>,
}

/// `totals`: also the journal counters and the per-peer inventory match
/// (both cached per connection until the database changes,
/// `core::status::Cache`).
fn compute_full(store: &Store, totals: bool) -> Result<Full> {
    let (content, legacy) = store.content_report()?;
    // The content scan loaded the view: the pending list is a borrow of it.
    let pending = store.pending_causal_events()?;
    let totals = if totals {
        let inventory_at_ms = now_ms();
        Some((
            store.journal_counters()?,
            store.inventory_match()?,
            inventory_at_ms,
        ))
    } else {
        None
    };
    Ok(Full {
        content,
        legacy,
        pending,
        totals,
    })
}

/// Recomputes the per-peer inventory match (`caught_up`'s last check).
fn refresh_inventory(ctx: &WorkerCtx) -> Result<()> {
    let at_ms = now_ms();
    let (feed, inventory) = {
        let store = ctx.sync_stores.checkout()?;
        // The tip first: an event committed meanwhile leaves it older. The
        // feed tip, as the cell's `tip`: the match counts shared events only.
        let feed = FeedTip {
            epoch: store.feed_epoch()?,
            seq: store.feed_tip()?,
        };
        (feed, store.inventory_match()?)
    };
    ctx.status_cell.update(|prev| {
        let mut next = object_or_empty(prev);
        let mut meta = CellMeta::of(prev);
        if at_ms >= meta.inventory_at_ms {
            next["inventory"] = json!(inventory);
            meta.inventory_at_ms = at_ms;
            meta.inventory_tip = feed.seq;
            meta.inventory_epoch.clone_from(&feed.epoch);
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
    refresh_content_with(ctx, vitals, true)
}

/// The content members (and the cheap ones) for a `status` call that must
/// read a client's writes: no journal counters and no inventory match (the
/// maintenance refresh brings those), so a mutation never makes the next
/// `status` wait for them (post-integration re-review, WP2.INT).
fn refresh_content(ctx: &WorkerCtx, vitals: &EngineVitals) -> Result<()> {
    refresh_content_with(ctx, Some(vitals), false)
}

fn refresh_content_with(
    ctx: &WorkerCtx,
    vitals: Option<&EngineVitals>,
    totals: bool,
) -> Result<()> {
    let target = ctx.status_generation.load(Ordering::SeqCst);
    let revision = vitals.map(EngineVitals::store_revision);
    let started = Instant::now();
    let (cheap, full) = {
        let store = ctx.sync_stores.checkout()?;
        (
            compute_cheap(&store, target, revision)?,
            compute_full(&store, totals)?,
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
            if let Some((journal, inventory, inventory_at_ms)) = &full.totals {
                let mut journal = journal.clone();
                if target < meta.generation {
                    // Newer cheap counters are already merged: keep them.
                    let current: JournalCounters =
                        serde_json::from_value(next["journal"].clone()).unwrap_or_default();
                    journal.tip = current.tip;
                    journal.outgoing_unacknowledged = current.outgoing_unacknowledged;
                    journal.unresolved_conflicts = current.unresolved_conflicts;
                }
                next["journal"] = json!(journal);
                if *inventory_at_ms >= meta.inventory_at_ms {
                    next["inventory"] = json!(inventory);
                    meta.inventory_at_ms = *inventory_at_ms;
                    // The feed tip, as the cell's `tip` (`inventory_tip`).
                    meta.inventory_tip = cheap.feed.seq;
                    meta.inventory_epoch.clone_from(&cheap.feed.epoch);
                }
                meta.full_ms = full_ms;
                meta.first_full_ms.get_or_insert(full_ms);
            }
            meta.content_generation = target;
            meta.content_at_ms = now_ms();
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

/// The `status` RPC reply (module docs). Recomputes the cheap members first
/// when the cell is behind a mutation (`status_generation`) or behind the
/// engine's store revision (writes through the mount), or when the cell is
/// older than `STATUS_CELL_STALE`; when a mutation asked for the content,
/// has it refreshed in the background ([`catch_up_content`]).
pub fn reply(ctx: &Arc<WorkerCtx>, vitals: &Arc<EngineVitals>) -> Result<Value> {
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
    if cell.get("content").is_none() {
        // The very first reply computes everything once (the legacy shape
        // has a `content` member), one computation for concurrent callers.
        let job = content_job(ctx);
        let _first = lock(&job.first);
        if ctx.status_cell.load().get("content").is_none()
            && let Err(error) = refresh_full_with(ctx, Some(vitals.as_ref()))
        {
            failure.get_or_insert(error);
        }
        cell = ctx.status_cell.load();
        meta = CellMeta::of(&cell);
    } else if meta.content_generation < meta.content_wanted {
        catch_up_content(ctx, vitals, &meta);
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
    // Like with like: the cell's `tip` is the feed tip, as `inventory_tip`.
    let tip = cell["tip"]["seq"].as_i64().unwrap_or(0);
    let epoch = cell["tip"]["epoch"].as_str().unwrap_or_default();
    if (latest_exchange >= meta.inventory_at_ms
        || tip != meta.inventory_tip
        || epoch != meta.inventory_epoch)
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

/// One worker store's read-your-writes content refresh: at most one runs at
/// a time, on its own thread, and every `status` call that wants it shares
/// it. Keyed by the store root (`WorkerCtx` is frozen and has no slot).
#[derive(Default)]
struct ContentJob {
    state: Mutex<JobState>,
    /// Signalled when a refresh ends (its result is in the cell by then).
    ended: Condvar,
    /// Serializes the very first reply's full computation.
    first: Mutex<()>,
}

#[derive(Default)]
struct JobState {
    running: bool,
    /// How long the last background refresh took, and when it ended.
    last: Option<(Duration, Instant)>,
    /// What a refresh is expected to cost: a decaying maximum of their
    /// durations, seeded with the cell's first full refresh. A maximum,
    /// because a refresh that finds a pooled connection's view already
    /// loaded after the commit (maintenance's) is fast without saying
    /// anything about the next one, which reloads it; decaying (by a
    /// quarter per refresh), so a one-off slow one is forgotten.
    expected: Option<Duration>,
}

fn content_job(ctx: &WorkerCtx) -> Arc<ContentJob> {
    static JOBS: Mutex<BTreeMap<PathBuf, Arc<ContentJob>>> = Mutex::new(BTreeMap::new());
    lock(&JOBS).entry(ctx.root.clone()).or_default().clone()
}

/// Brings the content up to `meta.content_wanted` without computing
/// it on the caller (WP2.INT's confirmation review: after another
/// connection's commit the content scan reloads the projection, O(projection),
/// 1.6–2.3 s at 50K events). The refresh runs on a `status-content` thread,
/// one at a time per store; this waits for it only when a refresh is
/// expected to take at most [`READ_YOUR_WRITES_BUDGET`] (`JobState::expected`;
/// bounded by `STATUS_CELL_STALE`). Otherwise the caller answers from the
/// cell (`content_pending`), and a refresh starts only
/// [`SLOW_REFRESH_SPACING`] times the last one's duration after it ended (a
/// later `status` call, or maintenance, catches the content up meanwhile).
fn catch_up_content(ctx: &Arc<WorkerCtx>, vitals: &Arc<EngineVitals>, meta: &CellMeta) {
    let wanted = meta.content_wanted;
    let caught_up = || CellMeta::of(&ctx.status_cell.load()).content_generation >= wanted;
    let job = content_job(ctx);
    let mut state = lock(&job.state);
    let expected = *state.expected.get_or_insert(Duration::from_millis(
        meta.first_full_ms.unwrap_or(meta.full_ms),
    ));
    let cheap = expected <= READ_YOUR_WRITES_BUDGET;
    let deadline = Instant::now()
        + if cheap {
            STATUS_CELL_STALE
        } else {
            Duration::ZERO
        };
    loop {
        // Under the job lock: a refresh that ended has merged its result.
        if caught_up() {
            return;
        }
        if !state.running {
            let spaced = cheap
                || state.last.is_none_or(|(took, ended)| {
                    ended.elapsed() >= took.saturating_mul(SLOW_REFRESH_SPACING)
                });
            if !spaced || ctx.stopping.load(Ordering::SeqCst) {
                return;
            }
            if let Err(error) = spawn_content_refresh(ctx, vitals, &job) {
                crate::util::log::event!(warn, UNKNOWN_ERROR, {
                    "site": "worker.status_cell",
                    "message": format!("cannot start a content refresh: {error}"),
                });
                return;
            }
            state.running = true;
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return;
        }
        state = job
            .ended
            .wait_timeout(state, left)
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .0;
    }
}

/// Starts one background content refresh (`refresh_content` on a pooled
/// store); its end, however it ends, is recorded in `job` and signalled.
fn spawn_content_refresh(
    ctx: &Arc<WorkerCtx>,
    vitals: &Arc<EngineVitals>,
    job: &Arc<ContentJob>,
) -> std::io::Result<()> {
    /// Records the refresh's end, also when it panics.
    struct Ended {
        job: Arc<ContentJob>,
        started: Instant,
    }
    impl Drop for Ended {
        fn drop(&mut self) {
            let took = self.started.elapsed();
            let mut state = lock(&self.job.state);
            state.running = false;
            state.last = Some((took, Instant::now()));
            state.expected = Some(state.expected.map_or(took, |e| took.max(e * 3 / 4)));
            drop(state);
            self.job.ended.notify_all();
        }
    }
    let (ctx, vitals, job) = (ctx.clone(), vitals.clone(), job.clone());
    thread::Builder::new()
        .name("status-content".into())
        .spawn(move || {
            let _ended = Ended {
                job,
                started: Instant::now(),
            };
            let point = format!("{}.{}", faults::CONTENT_REFRESH, ctx.repo);
            let refreshed =
                crate::util::fault::trigger(&point).and_then(|()| refresh_content(&ctx, &vitals));
            if let Err(error) = refreshed {
                let wire = crate::model::errors::ErrorWire::from_error(&error);
                crate::util::log::event!(warn, (wire.code.as_str()), {
                    "site": "worker.status_cell.content",
                    "message": wire.message,
                });
            }
        })
        .map(drop)
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
    if meta.content_generation < meta.content_wanted {
        // The content members predate a client mutation that changed them:
        // their refresh runs in the background (`catch_up_content`).
        s["content_pending"] = json!(true);
    }
    s
}
