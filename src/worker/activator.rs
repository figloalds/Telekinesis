//! Owned by WP2.3.
//!
//! The background inbox activator (REVAMP_PLAN §4.2, §4.4, WP2.3 task 2):
//! staged events are committed off the request paths, in slices that hold the
//! engine lock for at most 64 events or 25 ms, so the filesystem, sync and
//! status never wait behind a whole backlog.
//!
//! One loop, on the `activator` thread:
//!
//! 1. wait for `ctx.activation` (poked when rows are staged or re-queued) or
//!    `ACTIVATOR_IDLE_WAIT` (due retries), unless the last slice left more.
//!    After a wait, a cheap probe on a pooled secondary store ([`has_work`]:
//!    a ready, unclassified or due row) decides whether there is anything to
//!    do: an idle wakeup (the timeout, a poke after an empty exchange) never
//!    takes the engine;
//! 2. yield to the engine's other users (bounded by [`MAX_YIELD`]): while a
//!    thread waits for the engine, holds it, or took it in the last
//!    [`WAITER_GRACE`], the activator does not start a slice, so it does not
//!    slip one between the callbacks of a filesystem operation. The worker's
//!    own background holds (maintenance, notifier; marked through
//!    `EngineVitals::background_hold`) do not count;
//! 3. under the engine lock: `activate_slice(SliceBudget::ACTIVATOR)` (slow
//!    events are not committed inline: the slice stops and returns a
//!    `SlowTicket`), then `Engine::after_activation` (session refresh). The
//!    commit that crosses the startup-snapshot interval seals the snapshot
//!    of its own event's branch (since WP4.4 the rows changed since the
//!    previous seal, or a projection of at most 1 MiB, `core::snapshot`):
//!    the activator commits that event alone, in its own slice, and stops
//!    the slice before it short of that event ([`seal_forecast`]: the
//!    branches of the rows the slice will pop, each with the store's own
//!    cadence, `Store::seal_due_in`), so no seal hold also carries a
//!    slice's worth of other events. While mounted,
//!    notifications are computed per batch of slices: the `before`
//!    projection (entries only) is captured when a batch begins and diffed
//!    against the view for every entity the batch activated when it ends
//!    (after [`NOTIFY_BATCH`], or twenty times the capture's own cost if
//!    longer, or when no work is left), so the O(entries) capture is not paid
//!    on every slice. A slice that finds SQLite's writer held by another
//!    connection gives up at once and is retried a millisecond later, so the
//!    activator never waits for another writer while holding the engine
//!    (`fail_busy`, up to [`WRITER_PATIENCE`]);
//! 4. a `SlowTicket` is prepared off-lock (`Store::prepare_slow` on a pooled
//!    `SyncStore`) and committed under the lock
//!    (`commit_prepared_unless_busy`). Like a slice, the commit and the
//!    re-queues around it never wait for another connection's writer under
//!    the engine lock (up to [`WRITER_PATIENCE`]): a busy writer hands the
//!    preparation back, and it is committed after [`WRITER_RETRY`]
//!    (WP2.INT's post-integration re-review). A `Stale` ticket is re-queued
//!    with a per-event backoff (250 ms → 30 s); after [`STALE_ESCALATE`]
//!    consecutive `Stale` outcomes the event is committed inline, alone, the
//!    next time it comes up (`Prepared::Inline`: one O(history) hold for that
//!    event instead of starving behind a local writer that commits more
//!    often than a preparation takes; CCR-WP2.INT-1, §4.4).
//!    `activation.starved_since_ms` is the first `Stale` time of the oldest
//!    event still starving: it clears when that event commits, is
//!    quarantined or leaves the inbox, never on unrelated activations. A
//!    ticket whose preparation (or commit) fails is re-queued with the same
//!    per-event backoff, so one bad event never stalls the others behind it;
//!    only a store-wide failure (no secondary connection, a failing
//!    re-queue, a failing slice) backs the whole loop off;
//! 5. record the counters, bump `ctx.status_generation` and refresh the
//!    status cell (which wakes the heartbeat: `generations.sync` moves);
//! 6. after a seal (and every [`CHECKPOINT_EVERY`] committing slices) the
//!    WAL is checkpointed off-lock (`maintenance::checkpoint_wal`), and while
//!    the activator runs the engine connection's automatic checkpoint
//!    threshold is raised ([`ENGINE_WAL_AUTOCHECKPOINT`]): a seal writes
//!    megabytes, and SQLite otherwise copies the WAL into the database inside
//!    the commit that crosses 1,000 pages, under the engine lock. The engine
//!    connection also gets a page cache of [`ENGINE_CACHE_KIB`] while a
//!    backlog drains (no mid-transaction cache spill in a seal) and a busy
//!    handler that polls every millisecond (no backoff overshoot behind a
//!    secondary connection's staging), see `tune_engine_connection`.
//!
//! While it runs (`ctx.activator_running`, and the engine's
//! `background_activation`), legacy receives only stage. Errors are logged
//! and retried with backoff; a panic in a cycle or an idle step is contained
//! (a poisoned engine follows the poison policy), and both flags are cleared
//! however the loop ends (a drop guard). The loop ends when the worker is
//! stopping.
use crate::{
    core::{
        Store,
        inbox::{CommitOutcome, SliceBudget, SlowTicket},
    },
    model::{
        errors::{ErrorClass, ErrorWire, code_of},
        health::{ActivationCounters, EngineGauges},
        ipc::timing::{ACTIVATION_SLICE_EVENTS, ACTIVATION_SLICE_MS, ACTIVATOR_IDLE_WAIT},
    },
    runtime::{
        EngineVitals, check_engine_poison,
        ctx::{EngineGaugeCell, WorkerCtx},
        lock_engine, try_lock_engine,
    },
    util::{backoff::Backoff, time::now_ms},
    worker::{maintenance, status_cell},
};
use anyhow::Result;
use std::{
    collections::BTreeMap,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, atomic::Ordering},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// The longest the activator yields to the engine's other users before it
/// takes the engine anyway (it must progress under a constant stream of
/// calls).
pub const MAX_YIELD: Duration = Duration::from_millis(50);
/// How long the activator keeps yielding after another thread last used (or
/// waited for) the engine.
pub const WAITER_GRACE: Duration = Duration::from_millis(3);
/// The shortest notification batch while mounted (see the module docs).
pub const NOTIFY_BATCH: Duration = Duration::from_millis(250);
/// Another thread used the engine this recently: it is busy.
pub const BUSY_WINDOW: Duration = Duration::from_millis(250);
/// The slice while the engine is busy with other users: fewer events, so a
/// filesystem callback waits less behind it, but the same time budget, so a
/// slice that first reloads the view (a legacy exchange invalidated it)
/// still commits a useful batch (measured: 8 ms slices cut a legacy-fed
/// drain's throughput by half or more).
pub const BUSY_SLICE: SliceBudget = SliceBudget {
    max_events: 16,
    max_ms: 25,
    inline_slow: false,
};
/// The shortest journal distance between two periodic startup-snapshot
/// seals of a branch (`core::snapshot::MIN_SEAL_INTERVAL`); the forecast
/// reads each branch's own interval, `core::snapshot::seal_interval` of its
/// sealed payload (CCR-WP2.4-3 and the read side of CCR-WP2.3-5, applied by
/// WP3.INT), with the query the store decides with.
pub const SEAL_INTERVAL: i64 = crate::core::MIN_SEAL_INTERVAL;
/// At most this many head rows are looked at by [`seal_forecast`].
pub const FORECAST_ROWS: usize = 8;
/// [`seal_forecast`] parses a head row's payload only up to this size.
pub const FORECAST_PAYLOAD: i64 = 64 * 1024;
/// How long the activator's drop guard retries the engine to clear
/// `background_activation` when the loop ends while the worker is still
/// running (a panic outside a contained step).
const RELEASE_PATIENCE: Duration = Duration::from_secs(1);
/// The engine connection's `wal_autocheckpoint` (pages) while the activator
/// runs: the activator and maintenance checkpoint off-lock, so SQLite's own
/// checkpoint inside an engine commit is only a backstop (64 MiB of WAL at
/// 4 KiB pages).
pub const ENGINE_WAL_AUTOCHECKPOINT: i64 = 16_384;
/// SQLite's default `wal_autocheckpoint`, restored when the activator stops.
const DEFAULT_WAL_AUTOCHECKPOINT: i64 = 1000;
/// The engine connection's page cache while the activator has work (KiB;
/// see `set_cache_size`).
pub const ENGINE_CACHE_KIB: i64 = 32 * 1024;
/// SQLite's default page cache (KiB), restored when the activator drained
/// its backlog or stops.
const DEFAULT_CACHE_KIB: i64 = 2000;
/// The store's busy timeout (`Store::open`), which the activator's busy
/// handler keeps and restores.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
/// Committing slices between two off-lock WAL checkpoints (besides the one
/// after every seal).
pub const CHECKPOINT_EVERY: u32 = 16;
/// The pause before a slice that found SQLite's writer busy is retried.
const WRITER_RETRY: Duration = Duration::from_millis(1);
/// How long slices give up at once on a busy writer before they wait for it
/// under the engine lock like any other engine user (`fail_busy`).
pub const WRITER_PATIENCE: Duration = Duration::from_secs(2);
/// Per-event re-queue backoff (`Stale` tickets, failed preparations).
const EVENT_BASE: Duration = Duration::from_millis(250);
const EVENT_CAP: Duration = Duration::from_secs(30);
/// Consecutive `Stale` outcomes after which an event is committed inline
/// (CCR-WP2.INT-1; about 3.75 s of re-queue backoff before it).
pub const STALE_ESCALATE: u32 = 4;
/// How often the starving events are checked against the inbox (an event
/// that left it no longer starves).
const STARVING_CHECK: Duration = Duration::from_secs(1);
/// Events whose failure count is remembered (oldest forgotten first).
const EVENTS_TRACKED: usize = 1024;
/// The pause after a slice that left more rows but committed nothing.
const IDLE_SLICE_PAUSE: Duration = Duration::from_millis(5);

/// Starts the activator thread (panics if the thread cannot be created, as
/// `std::thread::spawn` does; see [`try_spawn`]).
pub fn spawn(ctx: Arc<WorkerCtx>) -> JoinHandle<()> {
    try_spawn(ctx).expect("cannot create the activator thread")
}

/// Starts the activator thread.
pub fn try_spawn(ctx: Arc<WorkerCtx>) -> std::io::Result<JoinHandle<()>> {
    thread::Builder::new()
        .name("activator".into())
        .spawn(move || run(&ctx))
}

fn run(ctx: &WorkerCtx) {
    // Set before the engine's flag (a legacy page that finds the engine flag
    // without this one clears it: `runtime::published`), and cleared with it
    // however the loop ends.
    ctx.activator_running.store(true, Ordering::SeqCst);
    let _running = Running { ctx };
    let vitals = {
        let mut engine = lock_engine(&ctx.engine);
        engine.background_activation = true;
        tune_engine_connection(&engine.store, true);
        engine.vitals.clone()
    };
    let mut activator = Activator::new(vitals);
    activator.counters.running = true;
    activator.publish(ctx);
    let mut errors = Backoff::inbox();
    let mut seen = ctx.activation.count();
    // Drain whatever startup left staged at once.
    let mut more = true;
    while !ctx.stopping.load(Ordering::Relaxed) {
        if !more {
            seen = ctx.activation.wait(seen, ACTIVATOR_IDLE_WAIT);
            if ctx.stopping.load(Ordering::Relaxed) {
                break;
            }
            // Idle: nothing ready, unclassified or due. A failed probe
            // falls through to a slice, which reports the failure.
            if matches!(has_work(ctx), Ok(false)) {
                // Contained as a cycle is: `idle` takes the engine to end a
                // notification batch.
                if catch_unwind(AssertUnwindSafe(|| activator.idle(ctx))).is_err() {
                    panicked(ctx, &mut errors);
                }
                continue;
            }
        }
        match catch_unwind(AssertUnwindSafe(|| activator.cycle(ctx))) {
            Ok(Ok(next)) => {
                more = next;
                errors.success(Instant::now());
            }
            Ok(Err(error)) => {
                let wire = ErrorWire::from_error(&error);
                crate::util::log::event!(warn, (wire.code.as_str()), {
                    "site": "worker.activator",
                    "message": wire.message,
                });
                more = false;
                pause(ctx, errors.failure(Instant::now()));
            }
            Err(_) => {
                panicked(ctx, &mut errors);
                more = false;
            }
        }
    }
}

/// After a contained panic: a panic under the engine lock poisoned it (the
/// policy runs), else the loop backs off.
fn panicked(ctx: &WorkerCtx, errors: &mut Backoff) {
    check_engine_poison(&ctx.engine);
    crate::util::log::event!(error, UNKNOWN_ERROR, {
        "site": "worker.activator",
        "message": "the activator panicked",
    });
    pause(ctx, errors.failure(Instant::now()));
}

/// Clears `ctx.activator_running` and the engine's `background_activation`
/// (and restores the engine connection's tuning) when [`run`] ends, by a
/// stop or by a panic outside its contained steps: with no activator thread
/// left, legacy pages must activate inline again and `revalidate` must not
/// wait for it. Never panics (it may run while unwinding).
struct Running<'a> {
    ctx: &'a WorkerCtx,
}

impl Drop for Running<'_> {
    fn drop(&mut self) {
        let ctx = self.ctx;
        ctx.activator_running.store(false, Ordering::SeqCst);
        // While the worker runs on, the engine is retried for a bounded
        // time (a filesystem callback may hold it); a stopping worker does
        // not wait. A poisoned engine is left to the poison policy (its
        // `try_lock` would run it here, a panic while unwinding aborts).
        let patience = if ctx.stopping.load(Ordering::Relaxed) {
            Duration::ZERO
        } else {
            RELEASE_PATIENCE
        };
        let until = Instant::now() + patience;
        loop {
            if ctx.engine.is_poisoned() {
                return;
            }
            if let Some(mut engine) = try_lock_engine(&ctx.engine) {
                engine.background_activation = false;
                tune_engine_connection(&engine.store, false);
                return;
            }
            if Instant::now() >= until {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
    }
}

/// Tunes the engine connection while the activator runs (`running`), or
/// restores SQLite's defaults and the store's 5 s busy timeout (and the
/// default page cache). Best effort: a failure keeps the current setting.
/// While running:
///
/// * `wal_autocheckpoint` is raised to [`ENGINE_WAL_AUTOCHECKPOINT`]: the
///   activator and maintenance checkpoint off-lock;
/// * SQLite's busy handler polls every millisecond (still giving up after
///   5 s, as the store's `busy_timeout`): staging on secondary connections
///   (feed exchanges, `stage_incoming`) holds SQLite's writer for a few
///   milliseconds, and the default handler's backoff (1, 2, 5, 10, 15, 20,
///   25 ms…) overshoots each release by up to 25 ms, under the engine lock.
///
/// (On Windows the engine store already keeps a commit's savepoint
/// sub-journal in memory, from its open on: `inbox::startup_drain`, WP4.4.)
fn tune_engine_connection(store: &Store, running: bool) {
    let pages = if running {
        ENGINE_WAL_AUTOCHECKPOINT
    } else {
        set_cache_size(store, DEFAULT_CACHE_KIB);
        DEFAULT_WAL_AUTOCHECKPOINT
    };
    let _ = store.db.query_row(
        &format!("PRAGMA wal_autocheckpoint={pages}"),
        [],
        |_| Ok(()),
    );
    let _ = if running {
        store.db.busy_handler(Some(poll_busy))
    } else {
        store.db.busy_timeout(BUSY_TIMEOUT)
    };
}

/// Sets the engine connection's page cache (best effort). While the
/// activator has work it is [`ENGINE_CACHE_KIB`]: a periodic seal reads the
/// stored projection back and rewrites megabytes in one transaction, and
/// with SQLite's default 2 MiB cache it re-reads pages and spills dirty pages
/// into the WAL in the middle of that transaction, under the engine lock
/// (measured at 2K events: seal holds of 14–52 ms became 8–33 ms). Once the
/// backlog is drained it is the default again, which frees the excess pages.
fn set_cache_size(store: &Store, kib: i64) {
    let _ = store.db.execute_batch(&format!("PRAGMA cache_size=-{kib}"));
}

/// The engine connection's busy handler while the activator runs: retry
/// every millisecond until [`BUSY_TIMEOUT`] (measured from the first retry
/// of this wait), as the store's `busy_timeout` would, without its backoff.
fn poll_busy(attempts: i32) -> bool {
    thread_local! {
        static SINCE: std::cell::Cell<Option<Instant>> = const { std::cell::Cell::new(None) };
    }
    let since = SINCE.with(|since| {
        if attempts == 0 || since.get().is_none() {
            since.set(Some(Instant::now()));
        }
        since.get().unwrap_or_else(Instant::now)
    });
    if since.elapsed() >= BUSY_TIMEOUT {
        return false;
    }
    thread::sleep(Duration::from_millis(1));
    true
}

/// The engine connection's busy handler during the activator's own slice:
/// give up at once. A slice's only statement that can find SQLite's writer
/// busy is its opening `BEGIN IMMEDIATE` (later statements run inside the
/// write transaction), so a busy slice changed nothing and is simply retried
/// after [`WRITER_RETRY`], with the engine free for the filesystem meanwhile,
/// instead of polling for the writer while holding it (another connection's
/// staging transaction and fsync can take tens of milliseconds on a busy
/// disk). After [`WRITER_PATIENCE`] of busy slices the slice waits for the
/// writer as every other engine user does (the store's 5 s budget).
fn fail_busy(_attempts: i32) -> bool {
    false
}

/// Whether `error` is SQLite's `SQLITE_BUSY` (the writer is held by another
/// connection).
fn writer_busy(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<rusqlite::Error>(),
            Some(rusqlite::Error::SqliteFailure(failure, _))
                if failure.code == rusqlite::ErrorCode::DatabaseBusy
        )
    })
}

/// Whether a slice could do anything: a `ready` row, an unclassified (legacy
/// or freshly staged) row, or a `retry` row that is due. Read on a pooled
/// secondary store, without the engine lock (staging commits before it pokes
/// the activator, so a poke's rows are visible here).
pub fn has_work(ctx: &WorkerCtx) -> Result<bool> {
    let store = ctx.sync_stores.checkout()?;
    let now = i64::try_from(now_ms()).unwrap_or(i64::MAX);
    Ok(store
        .db
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM incoming_events WHERE state='ready')
                 OR EXISTS(SELECT 1 FROM incoming_events WHERE state IS NULL)
                 OR EXISTS(SELECT 1 FROM incoming_events WHERE state='retry' AND coalesce(retry_at_ms,0)<=?1)",
        )?
        .query_row([now], |r| r.get(0))?)
}

/// What [`seal_forecast`] read before a slice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SealForecast {
    /// The journal tip before the slice: a startup snapshot whose cursor is
    /// past it was sealed by the slice ([`sealed_past`]).
    pub tip: i64,
    /// The first attempt (1-based) that may seal: `Some(1)` when the
    /// slice's first attempt seals if it commits; `Some(d)` when no attempt
    /// before the `d`-th seals the branch of any head row; `None` when no
    /// head row's branch is due within the lookahead (a branch due only
    /// behind the head is not counted, see [`seal_forecast`]).
    pub at: Option<i64>,
}

/// Forecasts the slice's sealing commit. A commit seals the startup snapshot
/// of **the committing event's branch** when the journal tip, that commit
/// included, is the branch's interval (at least [`SEAL_INTERVAL`]) past its
/// sealed cursor, or when the branch has no snapshot yet; the distance is
/// the store's own (`Store::seal_due_in`, CCR-WP2.3-5 applied by WP4.4). The tip
/// moves with every commit, whatever its branch, so a branch `b` sealed at
/// `saved` seals at the slice's commit number `d(b) = SEAL_INTERVAL - (tip -
/// saved)` if one of its events commits there, else at its first commit
/// after it (`d(b) <= 0`: at its next commit; no snapshot counts as 0).
///
/// The branches that the slice commits are those of its head rows: the
/// oldest `ready` and due `retry` rows (received order, then rowid, as
/// `Store::slice` pops them; their staged descendants, released as they
/// commit, are on the same branches), read with each row's branch through
/// the state index, with one cursor read per branch. So (see
/// [`forecast_at`]): the first row's branch due (`d <= 1`) → `Some(1)` (that
/// row is the slice's first attempt: it seals if it commits, or it leaves the
/// head); otherwise the smallest `d >= 2` among the head rows' branches (a
/// cap of `d - 1` attempts commits at most `d - 1` events, so it never
/// seals). A branch that is due but not at the head is left out: where its
/// next commit falls is unknown, and capping every slice for it would cut
/// them to one event until it commits. The first round read the active
/// branch's cursor only: with the backlog on another shared branch (a peer
/// working there) it predicted, after ~127 events, a seal on every slice
/// that never came (review WP2.3 fix round 2).
///
/// Read on the engine's own connection under the engine lock, so only the
/// slice that follows moves the journal. A forecast is exact for one
/// branch's backlog; with several branches a seal can still fall inside a
/// normal slice (a due branch not at the head, rows the slice itself
/// classifies), which costs that slice's hold, never a run of short slices.
/// `None` when it cannot be read (the slice then keeps its budget).
///
/// Bounded under the lock (WP2.3 review major, fixed by WP2.INT): it looks
/// at no more than [`FORECAST_ROWS`] head rows, and parses the payload of a
/// row only when it is at most [`FORECAST_PAYLOAD`] bytes (`octet_length`
/// reads no payload). A larger event's branch is not read: that row counts
/// as one whose branch is unknown (it caps nothing, and it is never the
/// one-event slice), so its seal can fall inside a normal slice, the cost of
/// that one slice. Before, the forecast parsed up to 2 x 64 head payloads
/// (peer-supplied events up to 8 MiB) before every slice.
pub fn seal_forecast(store: &Store, lookahead: usize) -> Option<SealForecast> {
    forecast(store, lookahead).ok()
}

fn forecast(store: &Store, lookahead: usize) -> Result<SealForecast> {
    let tip: i64 = store
        .db
        .prepare_cached("SELECT coalesce(max(seq),0) FROM journal_sequence")?
        .query_row([], |r| r.get(0))?;
    if lookahead == 0 {
        return Ok(SealForecast { tip, at: None });
    }
    let lookahead = lookahead.min(FORECAST_ROWS);
    let limit = i64::try_from(lookahead).unwrap_or(i64::MAX);
    let now = i64::try_from(now_ms()).unwrap_or(i64::MAX);
    let key = |r: &rusqlite::Row<'_>| -> rusqlite::Result<(i64, i64, Option<String>)> {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
    };
    // The slice's own window queries (`Store::slice`), with the branch of
    // each row whose payload is small enough to parse under the lock.
    let mut head: Vec<(i64, i64, Option<String>)> = store
        .db
        .prepare_cached(
            "SELECT coalesce(received_ms,0),rowid,
                    CASE WHEN octet_length(payload)<=?2 THEN json_extract(payload,'$.branch.id') END
             FROM incoming_events WHERE state='ready' ORDER BY received_ms,rowid LIMIT ?1",
        )?
        .query_map(rusqlite::params![limit, FORECAST_PAYLOAD], key)?
        .collect::<rusqlite::Result<_>>()?;
    head.extend(
        store
            .db
            .prepare_cached(
                "SELECT coalesce(received_ms,0),rowid,
                        CASE WHEN octet_length(payload)<=?3 THEN json_extract(payload,'$.branch.id') END
                 FROM incoming_events WHERE state='retry' AND coalesce(retry_at_ms,0)<=?1
                 ORDER BY received_ms,rowid LIMIT ?2",
            )?
            .query_map(rusqlite::params![now, limit, FORECAST_PAYLOAD], key)?
            .collect::<rusqlite::Result<Vec<_>>>()?,
    );
    head.sort();
    head.truncate(lookahead);
    let mut distances: BTreeMap<String, i64> = BTreeMap::new();
    let mut rows = Vec::with_capacity(head.len());
    for (_, _, branch) in head {
        // A row without a branch read (quarantined by its attempt, or too
        // large to parse here) is left out of the forecast.
        let Some(branch) = branch else {
            rows.push(None);
            continue;
        };
        let distance = match distances.get(&branch) {
            Some(distance) => *distance,
            None => {
                // The store's own cadence (CCR-WP2.3-5, WP4.4): commits until
                // the branch's next periodic seal; no snapshot counts as 0.
                let distance = store.seal_due_in(&branch)?;
                distances.insert(branch, distance);
                distance
            }
        };
        rows.push(Some(distance));
    }
    Ok(SealForecast {
        tip,
        at: forecast_at(rows),
    })
}

/// [`SealForecast::at`] from the head rows in pop order, each with its
/// branch's seal distance `d` (`None`: a row without a branch, which commits
/// nothing). See [`seal_forecast`].
pub fn forecast_at(rows: impl IntoIterator<Item = Option<i64>>) -> Option<i64> {
    let mut at: Option<i64> = None;
    for (index, distance) in rows.into_iter().enumerate() {
        let Some(distance) = distance else { continue };
        if index == 0 && distance <= 1 {
            return Some(1);
        }
        if distance >= 2 {
            at = Some(at.map_or(distance, |at| at.min(distance)));
        }
    }
    at
}

/// Whether a startup snapshot was sealed since the journal tip was `tip`
/// (its cursor, the sealed branch's last journal sequence, is past it).
/// Read on the engine's own connection under the engine lock, right after a
/// slice: exact, whichever branch sealed. `false` when it cannot be read.
pub fn sealed_past(store: &Store, tip: i64) -> bool {
    store
        .db
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM startup_snapshots WHERE cursor>?1)")
        .and_then(|mut statement| statement.query_row([tip], |r| r.get(0)))
        .unwrap_or(false)
}

/// Sleeps `delay`, waking early when the worker stops.
fn pause(ctx: &WorkerCtx, delay: Duration) {
    let until = Instant::now() + delay;
    while !ctx.stopping.load(Ordering::Relaxed) {
        let left = until.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break;
        }
        thread::sleep(left.min(Duration::from_millis(100)));
    }
}

/// The engine's other users (filesystem callbacks, RPCs), as the activator
/// sees them through the lock gauges. The worker's own background threads
/// (maintenance, notifier) mark their holds (`EngineVitals::background_hold`)
/// and are not counted: otherwise their periodic `try_lock`s would keep the
/// activator on its busy slices with no filesystem or RPC load at all.
#[derive(Default)]
struct Others {
    /// Other users' completed holds right after the activator's own last
    /// release.
    holds_after_own: u64,
    /// When another thread was last seen using (or waiting for) the engine.
    active_at: Option<Instant>,
}

impl Others {
    /// The gauges and the completed holds of threads other than the worker's
    /// background ones. The gauges are read first: a background hold that
    /// completes in between is then subtracted without having been counted,
    /// which can only lower the count, never fake activity.
    fn holds(gauges: &EngineGaugeCell, vitals: &EngineVitals) -> (EngineGauges, u64) {
        let snapshot = gauges.snapshot();
        let holds = snapshot.holds.wrapping_sub(vitals.background_holds());
        (snapshot, holds)
    }

    /// Records the activator's own release (call right after it).
    fn released(&mut self, gauges: &EngineGaugeCell, vitals: &EngineVitals) {
        self.holds_after_own = Self::holds(gauges, vitals).1;
    }

    /// Whether another thread used the engine within [`BUSY_WINDOW`].
    fn busy(&self) -> bool {
        self.active_at.is_some_and(|at| at.elapsed() < BUSY_WINDOW)
    }

    /// Lets the engine's other users go first, for at most [`MAX_YIELD`].
    /// Returns how long it yielded.
    ///
    /// A filesystem operation is several callbacks, each taking the engine
    /// briefly and rarely blocking for long, so waiters alone do not reveal
    /// it: a callback that queued behind the last slice ran as soon as the
    /// activator released the engine. So the activator also counts the
    /// engine's holds since its own last release, watches the current holder,
    /// and starts a slice only once nobody waited for, held or took the
    /// engine for [`WAITER_GRACE`]. With no other user it does not wait.
    fn yield_to(&mut self, gauges: &EngineGaugeCell, vitals: &EngineVitals) -> Duration {
        let started = Instant::now();
        let mut seen = self.holds_after_own;
        loop {
            let (snapshot, holds) = Self::holds(gauges, vitals);
            let holding = snapshot.current_hold_ms > 0 && !vitals.background_holding();
            // Wrapping: a count that went down (see `holds`) is no activity.
            let took = (holds.wrapping_sub(seen) as i64) > 0;
            if snapshot.waiters > 0 || holding || took {
                self.active_at = Some(Instant::now());
                if took {
                    seen = holds;
                }
            }
            let quiet = self.active_at.is_none_or(|at| at.elapsed() >= WAITER_GRACE);
            if quiet || started.elapsed() >= MAX_YIELD {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        started.elapsed()
    }

    /// The slice budget: §4.4's 64 events / 25 ms, or the smaller
    /// [`BUSY_SLICE`] while the engine's other users are active, so a
    /// filesystem callback that arrives during a slice waits less (each
    /// slice also pays its own commit).
    fn budget(&self) -> SliceBudget {
        if self.busy() {
            BUSY_SLICE
        } else {
            SliceBudget {
                max_events: ACTIVATION_SLICE_EVENTS,
                max_ms: ACTIVATION_SLICE_MS,
                inline_slow: false,
            }
        }
    }
}

/// `budget`, bounded so that the slice never seals a startup snapshot
/// together with other events: short of the forecast sealing attempt `at`
/// ([`SealForecast::at`]), or that attempt alone.
pub fn seal_budget(mut budget: SliceBudget, at: Option<i64>) -> SliceBudget {
    match at {
        Some(at) if at <= 1 => budget.max_events = 1,
        Some(at) => {
            let short = usize::try_from(at - 1).unwrap_or(usize::MAX);
            budget.max_events = budget.max_events.min(short);
        }
        None => {}
    }
    budget
}

/// The notifications of a batch of slices (mounted only, see the module
/// docs).
struct NotifyBatchState {
    /// The active projection's entries when the batch began.
    before: crate::core::Projection,
    entities: std::collections::BTreeSet<String>,
    namespace_changed: bool,
    began: Instant,
    /// The batch ends after this long.
    length: Duration,
}

struct Activator {
    counters: ActivationCounters,
    /// The engine's lock-free vitals (background holds).
    vitals: Arc<EngineVitals>,
    /// Consecutive `Stale` outcomes or failed preparations per event
    /// (re-queue backoff).
    failures: BTreeMap<String, u32>,
    /// The notification batch in progress (mounted only).
    batch: Option<NotifyBatchState>,
    /// The engine's other users.
    others: Others,
    /// A seal was committed since the last off-lock WAL checkpoint.
    sealed: bool,
    /// Committing slices since the last off-lock WAL checkpoint.
    since_checkpoint: u32,
    /// The engine connection's page cache is raised (a backlog is draining).
    cache_raised: bool,
    /// Since when slices keep finding SQLite's writer held by another
    /// connection.
    writer_busy_since: Option<Instant>,
    /// Events whose last slow commit was `Stale`: when they first went
    /// stale (`now_ms`) and how many times in a row (`starved_since_ms`,
    /// [`STALE_ESCALATE`]).
    starving: BTreeMap<String, Starving>,
    /// When `starving` was last checked against the inbox.
    starving_checked: Option<Instant>,
    /// Phase timings at the last dump (`dump_phases`).
    phases: BTreeMap<String, (u64, u64)>,
    phases_at: Option<Instant>,
}

/// One event whose slow commit keeps going `Stale`.
#[derive(Clone, Copy, Debug)]
struct Starving {
    since_ms: u64,
    stale: u32,
}

impl Activator {
    fn new(vitals: Arc<EngineVitals>) -> Self {
        Self {
            counters: ActivationCounters::default(),
            vitals,
            failures: BTreeMap::new(),
            batch: None,
            others: Others::default(),
            sealed: false,
            since_checkpoint: 0,
            cache_raised: false,
            writer_busy_since: None,
            starving: BTreeMap::new(),
            starving_checked: None,
            phases: BTreeMap::new(),
            phases_at: None,
        }
    }

    /// `activation.starved_since_ms`: the first `Stale` time of the oldest
    /// event still starving.
    fn refresh_starved(&mut self) {
        self.counters.starved_since_ms = self.starving.values().map(|s| s.since_ms).min();
    }

    /// Events that committed or were quarantined no longer starve.
    fn settled(&mut self, ids: impl IntoIterator<Item = impl AsRef<str>>) {
        if self.starving.is_empty() {
            return;
        }
        for id in ids {
            self.starving.remove(id.as_ref());
        }
        self.refresh_starved();
    }

    /// Drops starving events that left the inbox (committed by another
    /// path, revalidated away, deleted), at most every [`STARVING_CHECK`].
    /// Point lookups on a pooled secondary store.
    fn check_starving(&mut self, ctx: &WorkerCtx) {
        if self.starving.is_empty()
            || self
                .starving_checked
                .is_some_and(|at| at.elapsed() < STARVING_CHECK)
        {
            return;
        }
        self.starving_checked = Some(Instant::now());
        let Ok(store) = ctx.sync_stores.checkout() else {
            return;
        };
        let gone: Vec<String> = self
            .starving
            .keys()
            .filter(|id| {
                store
                    .db
                    .prepare_cached(
                        "SELECT EXISTS(SELECT 1 FROM incoming_events WHERE id=?1 AND state IN ('ready','retry'))",
                    )
                    .and_then(|mut statement| statement.query_row([id.as_str()], |r| r.get(0)))
                    .is_ok_and(|pending: bool| !pending)
            })
            .cloned()
            .collect();
        self.settled(gone);
    }

    /// Nothing to do: a notification batch still open (its last slice
    /// failed) is ended, so its notifications are not held back.
    fn idle(&mut self, ctx: &WorkerCtx) {
        // Fault point `worker.activator.idle.<repo>` (tests: a panic while
        // idle is contained; the repo keeps it to one in-process worker).
        let _ = crate::util::fault::trigger(&format!("worker.activator.idle.{}", ctx.repo));
        if self.batch.is_some() {
            let mut engine = lock_engine(&ctx.engine);
            let _holding = engine.vitals.activator_hold();
            self.end_batch(&mut engine);
        }
    }

    /// One slice, plus the slow commit it may hand over. Returns whether more
    /// work is (probably) ready.
    fn cycle(&mut self, ctx: &WorkerCtx) -> Result<bool> {
        // Counted finished on drop (`revalidate` waits for a cycle that
        // began after its re-queue), unless abandoned (writer busy, below).
        let cycle = self.vitals.activator_cycle();
        self.others.yield_to(&ctx.gauges, &self.vitals);
        let started = Instant::now();
        let (slice, sealed) = {
            let mut engine = lock_engine(&ctx.engine);
            let _holding = engine.vitals.activator_hold();
            let budget = self.others.budget();
            let forecast = seal_forecast(&engine.store, budget.max_events);
            let budget = seal_budget(budget, forecast.and_then(|forecast| forecast.at));
            if !self.cache_raised {
                set_cache_size(&engine.store, ENGINE_CACHE_KIB);
                self.cache_raised = true;
            }
            self.begin_batch(&engine);
            // While another connection holds SQLite's writer, the slice's
            // `BEGIN IMMEDIATE` fails at once instead of waiting for it
            // under the engine lock (see `writer_busy`).
            let fail_fast = self
                .writer_busy_since
                .is_none_or(|since| since.elapsed() < WRITER_PATIENCE);
            if fail_fast {
                let _ = engine.store.db.busy_handler(Some(fail_busy));
            }
            let slice = engine.store.activate_slice(budget);
            if fail_fast {
                let _ = engine.store.db.busy_handler(Some(poll_busy));
            }
            let mut sealed = false;
            if let Ok(slice) = &slice {
                // Rows the slice moved to `retry` (CCR-WP4.2-2).
                self.counters.retried += u64::from(slice.retried);
                // Exact, whichever branch sealed (the WAL checkpoint below).
                sealed = slice.fast + slice.slow > 0
                    && forecast.is_some_and(|forecast| sealed_past(&engine.store, forecast.tip));
                self.activated(&mut engine, &slice.activated);
                if !slice.more && slice.slow_ticket.is_none() {
                    // Drained: notifications out, the page cache back down.
                    self.end_batch(&mut engine);
                    set_cache_size(&engine.store, DEFAULT_CACHE_KIB);
                    self.cache_raised = false;
                }
            }
            (slice, sealed)
        };
        self.others.released(&ctx.gauges, &self.vitals);
        let slice = match slice {
            Err(error) if writer_busy(&error) => {
                // Nothing was done: retry soon, without the engine meanwhile.
                // Not an activation pass: `revalidate` keeps waiting.
                cycle.abandon();
                self.writer_busy_since.get_or_insert_with(Instant::now);
                thread::sleep(WRITER_RETRY);
                return Ok(true);
            }
            slice => {
                self.writer_busy_since = None;
                slice?
            }
        };
        let changed = !slice.activated.is_empty() || !slice.quarantined.is_empty();
        self.counters.slices += 1;
        self.counters.last_slice_ms =
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.counters.activated += slice.activated.len() as u64;
        self.counters.fast += u64::from(slice.fast);
        self.counters.slow += u64::from(slice.slow);
        self.counters.quarantined += slice.quarantined.len() as u64;
        if !slice.activated.is_empty() {
            self.counters.last_activation_at_ms = Some(now_ms());
        }
        // Only the starving events themselves end their starvation.
        self.settled(slice.activated.iter());
        self.settled(slice.quarantined.iter().map(|(id, _)| id));
        self.check_starving(ctx);
        let journaled = slice.fast + slice.slow > 0;
        self.sealed |= sealed;
        let mut more = slice.more;
        let mut slow_changed = false;
        if let Some(ticket) = slice.slow_ticket {
            slow_changed = self.slow(ctx, ticket)?;
            more = true;
        }
        if changed || slow_changed {
            ctx.status_generation.fetch_add(1, Ordering::SeqCst);
            self.publish(ctx);
        } else if more {
            // More rows, none committed (classification, deferrals): do not
            // spin on the engine.
            thread::sleep(IDLE_SLICE_PAUSE);
        }
        self.checkpoint(ctx, journaled);
        self.dump_phases();
        Ok(more)
    }

    /// Profiling aid (temporary): with `TKFS_DERIVE_PHASES=1`, the activator
    /// thread's derivation phase timings since the last dump, every 60 s.
    fn dump_phases(&mut self) {
        static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        if !*ENABLED.get_or_init(|| std::env::var("TKFS_DERIVE_PHASES").as_deref() == Ok("1")) {
            return;
        }
        if self
            .phases_at
            .is_some_and(|at| at.elapsed() < Duration::from_secs(60))
        {
            return;
        }
        self.phases_at = Some(Instant::now());
        let now = crate::core::derive::thread_phase_micros();
        let mut diff: Vec<(String, u64, u64)> = now
            .iter()
            .map(|(k, (c, m))| {
                let (c0, m0) = self.phases.get(*k).copied().unwrap_or((0, 0));
                (k.to_string(), c - c0, (m - m0) / 1000)
            })
            .filter(|(_, c, _)| *c > 0)
            .collect();
        diff.sort_by(|a, b| b.2.cmp(&a.2));
        eprintln!(
            "DERIVE_PHASES {} {}",
            now_ms(),
            serde_json::to_string(&diff).unwrap_or_default()
        );
        self.phases = now.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
    }

    /// The off-lock WAL checkpoint after a seal, or after
    /// [`CHECKPOINT_EVERY`] committing slices.
    fn checkpoint(&mut self, ctx: &WorkerCtx, journaled: bool) {
        if journaled {
            self.since_checkpoint += 1;
        }
        if self.sealed || self.since_checkpoint >= CHECKPOINT_EVERY {
            maintenance::checkpoint_wal(ctx);
            self.sealed = false;
            self.since_checkpoint = 0;
        }
    }

    /// The re-queue delay of `id` after one more `Stale` outcome or failed
    /// preparation (250 ms doubling to 30 s).
    fn event_backoff(&mut self, id: &str) -> u64 {
        let failures = self.failures.entry(id.to_owned()).or_insert(0);
        *failures = failures.saturating_add(1);
        let delay = EVENT_BASE
            .saturating_mul(1u32 << (*failures - 1).min(7))
            .min(EVENT_CAP);
        if self.failures.len() > EVENTS_TRACKED
            && let Some(oldest) = self.failures.keys().next().cloned()
        {
            self.failures.remove(&oldest);
        }
        u64::try_from(delay.as_millis()).unwrap_or(u64::MAX)
    }

    /// Prepares `ticket` off-lock and commits it under the lock. Returns
    /// whether the store changed. A failed preparation or commit re-queues
    /// the ticket's event with its own backoff (the other events go on);
    /// only a store-wide failure (no secondary connection, a failing
    /// re-queue) fails the cycle. An event that went `Stale`
    /// [`STALE_ESCALATE`] times in a row is committed inline instead
    /// (`Prepared::Inline`, alone; CCR-WP2.INT-1).
    fn slow(&mut self, ctx: &WorkerCtx, ticket: SlowTicket) -> Result<bool> {
        let escalated = self
            .starving
            .get(&ticket.id)
            .is_some_and(|starving| starving.stale >= STALE_ESCALATE);
        let prepared = if escalated {
            crate::util::log::event!(warn, STALE, {
                "site": "worker.activator",
                "event": ticket.id,
                "branch": ticket.branch,
                "detail": "the event keeps going stale behind local writes: committed inline",
                "stale": self.starving.get(&ticket.id).map_or(0, |s| s.stale),
            });
            Ok(crate::core::inbox::Prepared::Inline(ticket.clone()))
        } else {
            let read = ctx.sync_stores.checkout()?;
            // Fault point `worker.activator.prepare.<repo>` (tests): scoped
            // to one in-process worker's repository, so arming it never
            // fails another test's activator (WP2.3 review minor, WP2.INT).
            crate::util::fault::fail(&format!("worker.activator.prepare.{}", ctx.repo))
                .and_then(|()| Store::prepare_slow(&read, &ticket))
        };
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                self.requeue_failed(ctx, &ticket, &error)?;
                return Ok(true);
            }
        };
        let Some(outcome) = self.commit(ctx, prepared) else {
            // Stopping: the row stays staged for the next start.
            return Ok(false);
        };
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                let delay = self.event_backoff(&ticket.id);
                self.requeue(ctx, &ticket.id, delay)?;
                self.counters.retried += 1;
                log_event_failure(&ticket, "commit", &error);
                return Ok(true);
            }
        };
        Ok(match outcome {
            CommitOutcome::Committed { slow, .. } => {
                self.failures.remove(&ticket.id);
                self.settled([&ticket.id]);
                self.counters.activated += 1;
                if slow {
                    self.counters.slow += 1;
                } else {
                    self.counters.fast += 1;
                }
                self.counters.last_activation_at_ms = Some(now_ms());
                // Every slow-path commit seals the snapshot (REVAMP_PLAN P4).
                self.sealed = true;
                true
            }
            CommitOutcome::Stale => {
                let delay = self.event_backoff(&ticket.id);
                self.requeue(ctx, &ticket.id, delay)?;
                self.counters.stale += 1;
                let starving = self.starving.entry(ticket.id.clone()).or_insert(Starving {
                    since_ms: now_ms(),
                    stale: 0,
                });
                starving.stale = starving.stale.saturating_add(1);
                self.refresh_starved();
                crate::util::log::event!(info, STALE, {
                    "site": "worker.activator",
                    "event": ticket.id,
                    "branch": ticket.branch,
                });
                true
            }
            CommitOutcome::Failed(error) => {
                self.failures.remove(&ticket.id);
                self.settled([&ticket.id]);
                if error.class == ErrorClass::Fatal || error.class == ErrorClass::NeedsUserAction {
                    self.counters.quarantined += 1;
                } else {
                    self.counters.retried += 1;
                }
                true
            }
        })
    }

    /// Commits `prepared` under the engine lock without waiting there for
    /// another connection's writer: within [`WRITER_PATIENCE`] the commit's
    /// `BEGIN IMMEDIATE` gives up at once (`fail_busy`), the preparation
    /// comes back and the commit is retried after [`WRITER_RETRY`] with the
    /// engine free meanwhile (as a slice is; WP2.INT's post-integration
    /// re-review: the commit used to poll for the writer for up to 5 s under
    /// the lock). A busy attempt is neither `Stale` nor a failure. `None`
    /// when the worker is stopping.
    fn commit(
        &mut self,
        ctx: &WorkerCtx,
        mut prepared: crate::core::inbox::Prepared,
    ) -> Option<Result<CommitOutcome>> {
        let since = Instant::now();
        loop {
            self.others.yield_to(&ctx.gauges, &self.vitals);
            let attempt = {
                let mut engine = lock_engine(&ctx.engine);
                let _holding = engine.vitals.activator_hold();
                self.begin_batch(&engine);
                let fail_fast = since.elapsed() < WRITER_PATIENCE;
                if fail_fast {
                    let _ = engine.store.db.busy_handler(Some(fail_busy));
                }
                let attempt = engine.store.commit_prepared_unless_busy(prepared);
                if fail_fast {
                    let _ = engine.store.db.busy_handler(Some(poll_busy));
                }
                if let Ok(Ok(CommitOutcome::Committed { id, .. })) = &attempt {
                    self.activated(&mut engine, std::slice::from_ref(id));
                }
                attempt
            };
            self.others.released(&ctx.gauges, &self.vitals);
            match attempt {
                Ok(Err(back)) => {
                    if ctx.stopping.load(Ordering::Relaxed) {
                        return None;
                    }
                    prepared = back;
                    thread::sleep(WRITER_RETRY);
                }
                Ok(Ok(outcome)) => return Some(Ok(outcome)),
                Err(error) => return Some(Err(error)),
            }
        }
    }

    /// Re-queues `id` (due in `delay_ms`) under the engine lock, without
    /// waiting there for another connection's writer within
    /// [`WRITER_PATIENCE`] (as [`commit`](Self::commit)). A failure other
    /// than a busy writer is store-wide: it fails the cycle.
    fn requeue(&mut self, ctx: &WorkerCtx, id: &str, delay_ms: u64) -> Result<()> {
        let since = Instant::now();
        loop {
            let attempt = {
                let mut engine = lock_engine(&ctx.engine);
                let _holding = engine.vitals.activator_hold();
                let fail_fast = since.elapsed() < WRITER_PATIENCE;
                if fail_fast {
                    let _ = engine.store.db.busy_handler(Some(fail_busy));
                }
                let attempt = engine.store.requeue(id, delay_ms);
                if fail_fast {
                    let _ = engine.store.db.busy_handler(Some(poll_busy));
                }
                attempt
            };
            self.others.released(&ctx.gauges, &self.vitals);
            match attempt {
                Err(error) if writer_busy(&error) => {
                    if ctx.stopping.load(Ordering::Relaxed) {
                        return Ok(());
                    }
                    thread::sleep(WRITER_RETRY);
                }
                attempt => return attempt,
            }
        }
    }

    /// A ticket whose preparation failed: its event goes back to `retry`
    /// with its own backoff, so the rows behind it keep activating (no
    /// head-of-line stall); counted as `retried` and logged. A failing
    /// re-queue is store-wide: it fails the cycle.
    fn requeue_failed(
        &mut self,
        ctx: &WorkerCtx,
        ticket: &SlowTicket,
        error: &anyhow::Error,
    ) -> Result<()> {
        let delay = self.event_backoff(&ticket.id);
        self.requeue(ctx, &ticket.id, delay)?;
        self.counters.retried += 1;
        log_event_failure(ticket, "prepare", error);
        Ok(())
    }

    /// Starts a notification batch while mounted (captures `before`), and
    /// drops one that outlived the mount.
    fn begin_batch(&mut self, engine: &crate::runtime::Engine) {
        if !engine.mounted {
            self.batch = None;
            return;
        }
        if self.batch.is_some() {
            return;
        }
        let capture = Instant::now();
        if let Some(before) = engine.capture_before() {
            self.batch = Some(NotifyBatchState {
                before,
                entities: Default::default(),
                namespace_changed: false,
                began: Instant::now(),
                length: NOTIFY_BATCH.max(capture.elapsed().saturating_mul(20)),
            });
        }
    }

    /// After committed `ids`: the session refresh now, their entities into
    /// the notification batch (ended once it is long enough).
    fn activated(&mut self, engine: &mut crate::runtime::Engine, ids: &[String]) {
        if ids.is_empty() {
            return;
        }
        match engine.activation_delta(ids, None) {
            Ok(delta) => {
                engine.after_activation(&delta);
                if let Some(batch) = &mut self.batch {
                    batch.entities.extend(delta.entities);
                    batch.namespace_changed |= delta.namespace_changed;
                }
            }
            // Committed already: only the session refresh is lost.
            Err(error) => log_after(&error),
        }
        if self
            .batch
            .as_ref()
            .is_some_and(|batch| batch.began.elapsed() >= batch.length)
        {
            self.end_batch(engine);
        }
    }

    /// Ends the notification batch: queues its notifications.
    fn end_batch(&mut self, engine: &mut crate::runtime::Engine) {
        if let Some(batch) = self.batch.take() {
            engine.notify_activation(&batch.before, &batch.entities, batch.namespace_changed);
        }
    }

    /// Records the counters in the status cell (cheap refresh).
    fn publish(&self, ctx: &WorkerCtx) {
        if let Err(error) = status_cell::record_activation(ctx, &self.vitals, self.counters) {
            let wire = ErrorWire::from_error(&error);
            crate::util::log::event!(warn, (wire.code.as_str()), {
                "site": "worker.status_cell",
                "message": wire.message,
            });
        }
    }
}

fn log_after(error: &anyhow::Error) {
    crate::util::log::event!(warn, (code_of(error)), {
        "site": "worker.activator.after",
        "message": format!("{error:#}"),
    });
}

fn log_event_failure(ticket: &SlowTicket, step: &str, error: &anyhow::Error) {
    crate::util::log::event!(warn, (code_of(error)), {
        "site": "worker.activator.slow",
        "step": step,
        "event": ticket.id,
        "branch": ticket.branch,
        "message": format!("{error:#}"),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tracked hold of the engine as the lock seam reports it.
    fn hold(gauges: &EngineGaugeCell) {
        gauges.begin_hold();
        gauges.record_hold(0);
    }

    /// Holds of the worker's own background threads (maintenance, notifier)
    /// are not filesystem or RPC load: the activator keeps §4.4's full slice.
    /// Any other hold makes it yield and use the busy slice.
    #[test]
    fn background_holds_do_not_make_the_engine_busy() {
        let gauges = EngineGaugeCell::new();
        let vitals = Arc::new(EngineVitals::default());
        let mut others = Others::default();
        hold(&gauges); // the activator's own slice
        others.released(&gauges, &vitals);
        for _ in 0..3 {
            // As maintenance and the notifier hold: the mark is dropped
            // before the engine guard.
            gauges.begin_hold();
            let mark = vitals.background_hold();
            assert!(vitals.background_holding());
            drop(mark);
            gauges.record_hold(0);
        }
        others.yield_to(&gauges, &vitals);
        assert!(!others.busy(), "background holds counted as load");
        assert_eq!(others.budget().max_events, ACTIVATION_SLICE_EVENTS);
        hold(&gauges); // a filesystem callback
        others.yield_to(&gauges, &vitals);
        assert!(others.busy(), "a filesystem hold was not seen");
        assert_eq!(others.budget(), BUSY_SLICE);
    }

    /// `SQLITE_BUSY` (through any context) is the writer-busy retry, other
    /// errors are not.
    #[test]
    fn writer_busy_detects_sqlite_busy_only() {
        let busy = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
            None,
        );
        assert!(writer_busy(
            &anyhow::Error::new(busy).context("inbox_slice")
        ));
        let full = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_FULL),
            None,
        );
        assert!(!writer_busy(&anyhow::Error::new(full)));
        assert!(!writer_busy(&anyhow::anyhow!("SQLITE_BUSY in a message")));
    }

    /// The sealing commit is alone in its slice, and the slice before it
    /// stops short of it.
    #[test]
    fn seal_budget_isolates_the_sealing_commit() {
        let full = SliceBudget::ACTIVATOR;
        assert_eq!(seal_budget(full, None), full);
        assert_eq!(seal_budget(full, Some(SEAL_INTERVAL)).max_events, 64);
        assert_eq!(
            seal_budget(full, Some(10)),
            SliceBudget {
                max_events: 9,
                ..full
            }
        );
        assert_eq!(seal_budget(full, Some(2)).max_events, 1);
        assert_eq!(
            seal_budget(full, Some(1)),
            SliceBudget {
                max_events: 1,
                ..full
            }
        );
        // Never below one attempt (a forecast is 1-based; defensive).
        assert_eq!(seal_budget(full, Some(0)).max_events, 1);
        assert_eq!(seal_budget(full, Some(-5)).max_events, 1);
    }

    /// The forecast follows the branches of the head rows, not the active
    /// branch (review WP2.3 fix round 2): a due branch at the head seals the
    /// first attempt; a due branch elsewhere does not cap the slice (its
    /// next commit's place is unknown); otherwise the nearest seal among the
    /// head rows' branches bounds it.
    #[test]
    fn forecast_at_follows_the_head_rows_branches() {
        // One branch's backlog: its distance, then its sealing commit alone.
        assert_eq!(forecast_at([Some(40)]), Some(40));
        assert_eq!(forecast_at([Some(1)]), Some(1));
        // Overdue (a missed seal) or no snapshot (0) at the head.
        assert_eq!(forecast_at([Some(-300), Some(90)]), Some(1));
        assert_eq!(forecast_at([Some(0)]), Some(1));
        // Several branches: the nearest one bounds the slice.
        assert_eq!(forecast_at([Some(90), Some(30), Some(60)]), Some(30));
        // A due branch not at the head (here 0 and -5) caps nothing.
        assert_eq!(forecast_at([Some(90), Some(0), Some(-5)]), Some(90));
        assert_eq!(forecast_at([Some(127), Some(1)]), Some(127));
        // A head row without a branch commits nothing.
        assert_eq!(forecast_at([None, Some(1), Some(50)]), Some(50));
        assert_eq!(forecast_at(Vec::<Option<i64>>::new()), None);
        assert_eq!(forecast_at([None]), None);
    }

    /// An abandoned cycle (its slice found SQLite's writer busy and did
    /// nothing) is not an activation pass: `revalidate`'s wait for a cycle
    /// begun after its re-queue is satisfied only by one that ran, and an
    /// abandoned cycle never delays a later wait (review WP2.3 fix round 2).
    #[test]
    fn an_abandoned_cycle_is_not_an_activation_pass() {
        let vitals = Arc::new(EngineVitals::default());
        let (begun, _) = vitals.activator_cycles();
        vitals.activator_cycle().abandon();
        vitals.activator_cycle().abandon();
        assert!(
            vitals.activator_cycles().1 <= begun,
            "abandoned cycles counted"
        );
        drop(vitals.activator_cycle());
        assert!(
            vitals.activator_cycles().1 > begun,
            "a pass was not counted"
        );
        // No drift: after abandoned cycles, the next pass alone satisfies a
        // new wait.
        let (begun, _) = vitals.activator_cycles();
        vitals.activator_cycle().abandon();
        drop(vitals.activator_cycle());
        assert!(vitals.activator_cycles().1 > begun);
        // A pass in progress when a wait begins does not satisfy it.
        let running = vitals.activator_cycle();
        let (begun, _) = vitals.activator_cycles();
        drop(running);
        assert!(vitals.activator_cycles().1 <= begun);
        drop(vitals.activator_cycle());
        assert!(vitals.activator_cycles().1 > begun);
    }
}
