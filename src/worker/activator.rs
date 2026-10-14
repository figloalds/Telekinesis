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
//!    (an O(history) hash plus a projection serialization, `core::snapshot`):
//!    the activator commits that event alone, in its own slice, and stops the
//!    slice before it short of that event ([`seal_distance`]), so no seal
//!    hold also carries a slice's worth of other events. While mounted,
//!    notifications are computed per batch of slices: the `before`
//!    projection (entries only) is captured when a batch begins and diffed
//!    against the view for every entity the batch activated when it ends
//!    (after [`NOTIFY_BATCH`], or twenty times the capture's own cost if
//!    longer, or when no work is left), so the O(entries) capture is not paid
//!    on every slice;
//! 4. a `SlowTicket` is prepared off-lock (`Store::prepare_slow` on a pooled
//!    `SyncStore`) and committed under the lock (`commit_prepared`;
//!    `Prepared::Inline` runs the slow path there until WP2.4's off-lock
//!    preparation lands). A `Stale` ticket is re-queued with a per-event
//!    backoff (250 ms → 30 s), never committed inline; while tickets keep
//!    going stale, `activation.starved_since_ms` is set. A ticket whose
//!    preparation (or commit) fails is re-queued with the same per-event
//!    backoff, so one bad event never stalls the others behind it; only a
//!    store-wide failure (no secondary connection, a failing re-queue, a
//!    failing slice) backs the whole loop off;
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
//! and retried with backoff; a panic is contained (a poisoned engine follows
//! the poison policy). The loop ends when the worker is stopping.
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
/// Journal sequences between two periodic startup-snapshot seals. Mirrors
/// `core::snapshot::INTERVAL` (private to `core`, CCR-WP2.3-5): if the two
/// ever differ, the seal is only no longer isolated in its own slice.
pub const SEAL_INTERVAL: i64 = 128;
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
/// Per-event re-queue backoff (`Stale` tickets, failed preparations).
const EVENT_BASE: Duration = Duration::from_millis(250);
const EVENT_CAP: Duration = Duration::from_secs(30);
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
    let vitals = {
        let mut engine = lock_engine(&ctx.engine);
        engine.background_activation = true;
        tune_engine_connection(&engine.store, true);
        engine.vitals.clone()
    };
    ctx.activator_running.store(true, Ordering::SeqCst);
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
                activator.idle(ctx);
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
                // A panic under the engine lock poisoned it: the policy runs.
                check_engine_poison(&ctx.engine);
                crate::util::log::event!(error, UNKNOWN_ERROR, {
                    "site": "worker.activator",
                    "message": "the activator panicked",
                });
                more = false;
                pause(ctx, errors.failure(Instant::now()));
            }
        }
    }
    ctx.activator_running.store(false, Ordering::SeqCst);
    if let Some(mut engine) = try_lock_engine(&ctx.engine) {
        engine.background_activation = false;
        tune_engine_connection(&engine.store, false);
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

/// Journal commits until (and including) the one that seals the active
/// branch's startup snapshot (§4.4, `core::snapshot::maybe_persist`: a
/// commit seals when the journal tip is [`SEAL_INTERVAL`] past the branch's
/// sealed cursor, or when the branch has no snapshot yet). Read on the
/// engine's own connection, under the engine lock: only the slice that
/// follows can move the journal. `None` when it cannot be read.
pub fn seal_distance(store: &Store) -> Option<i64> {
    let (tip, saved): (i64, Option<i64>) = store
        .db
        .prepare_cached(
            "SELECT (SELECT coalesce(max(seq),0) FROM journal_sequence),
                    (SELECT cursor FROM startup_snapshots WHERE branch=?1)",
        )
        .ok()?
        .query_row([&store.active], |r| Ok((r.get(0)?, r.get(1)?)))
        .ok()?;
    Some(saved.map_or(1, |saved| SEAL_INTERVAL - (tip - saved)))
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

/// `budget`, bounded so that the slice never seals the startup snapshot
/// together with other events: short of the sealing commit, or that commit
/// alone. Returns the budget and whether the slice may seal.
fn seal_budget(mut budget: SliceBudget, distance: Option<i64>) -> (SliceBudget, bool) {
    match distance {
        Some(distance) if distance <= 1 => {
            budget.max_events = 1;
            (budget, true)
        }
        Some(distance) => {
            // Attempts bound commits (a quarantined or retried attempt
            // commits nothing), so `distance - 1` attempts never seal.
            let short = usize::try_from(distance - 1).unwrap_or(usize::MAX);
            budget.max_events = budget.max_events.min(short);
            (budget, false)
        }
        None => (budget, false),
    }
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
        }
    }

    /// Nothing to do: a notification batch still open (its last slice
    /// failed) is ended, so its notifications are not held back.
    fn idle(&mut self, ctx: &WorkerCtx) {
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
        // began after its re-queue).
        let _cycle = self.vitals.activator_cycle();
        self.others.yield_to(&ctx.gauges, &self.vitals);
        let started = Instant::now();
        let (slice, sealing) = {
            let mut engine = lock_engine(&ctx.engine);
            let _holding = engine.vitals.activator_hold();
            let (budget, sealing) = seal_budget(self.others.budget(), seal_distance(&engine.store));
            if !self.cache_raised {
                set_cache_size(&engine.store, ENGINE_CACHE_KIB);
                self.cache_raised = true;
            }
            self.begin_batch(&engine);
            let slice = engine.store.activate_slice(budget);
            if let Ok(slice) = &slice {
                self.activated(&mut engine, &slice.activated);
                if !slice.more && slice.slow_ticket.is_none() {
                    // Drained: notifications out, the page cache back down.
                    self.end_batch(&mut engine);
                    set_cache_size(&engine.store, DEFAULT_CACHE_KIB);
                    self.cache_raised = false;
                }
            }
            (slice, sealing)
        };
        self.others.released(&ctx.gauges, &self.vitals);
        let slice = slice?;
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
            self.counters.starved_since_ms = None;
        }
        // Journal rows written: the sealing slice sealed.
        let journaled = slice.fast + slice.slow > 0;
        self.sealed |= sealing && journaled;
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
        Ok(more)
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
    /// re-queue) fails the cycle.
    fn slow(&mut self, ctx: &WorkerCtx, ticket: SlowTicket) -> Result<bool> {
        let prepared = {
            let read = ctx.sync_stores.checkout()?;
            crate::util::fault::fail("worker.activator.prepare")
                .and_then(|()| Store::prepare_slow(&read, &ticket))
        };
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                self.requeue_failed(ctx, &ticket, &error)?;
                return Ok(true);
            }
        };
        self.others.yield_to(&ctx.gauges, &self.vitals);
        let outcome = {
            let mut engine = lock_engine(&ctx.engine);
            let _holding = engine.vitals.activator_hold();
            self.begin_batch(&engine);
            let outcome = engine.store.commit_prepared(prepared);
            match &outcome {
                Ok(CommitOutcome::Committed { id, .. }) => {
                    self.activated(&mut engine, std::slice::from_ref(id));
                }
                Ok(CommitOutcome::Stale) | Err(_) => {
                    let delay = self.event_backoff(&ticket.id);
                    engine.store.requeue(&ticket.id, delay)?;
                }
                Ok(CommitOutcome::Failed(_)) => {}
            }
            outcome
        };
        self.others.released(&ctx.gauges, &self.vitals);
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                // Re-queued above, under the same lock.
                self.counters.retried += 1;
                log_event_failure(&ticket, "commit", &error);
                return Ok(true);
            }
        };
        Ok(match outcome {
            CommitOutcome::Committed { slow, .. } => {
                self.failures.remove(&ticket.id);
                self.counters.activated += 1;
                if slow {
                    self.counters.slow += 1;
                } else {
                    self.counters.fast += 1;
                }
                self.counters.last_activation_at_ms = Some(now_ms());
                self.counters.starved_since_ms = None;
                // Every slow-path commit seals the snapshot (REVAMP_PLAN P4).
                self.sealed = true;
                true
            }
            CommitOutcome::Stale => {
                self.counters.stale += 1;
                self.counters.starved_since_ms.get_or_insert_with(now_ms);
                crate::util::log::event!(info, STALE, {
                    "site": "worker.activator",
                    "event": ticket.id,
                    "branch": ticket.branch,
                });
                true
            }
            CommitOutcome::Failed(error) => {
                self.failures.remove(&ticket.id);
                if error.class == ErrorClass::Fatal || error.class == ErrorClass::NeedsUserAction {
                    self.counters.quarantined += 1;
                } else {
                    self.counters.retried += 1;
                }
                true
            }
        })
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
        {
            let mut engine = lock_engine(&ctx.engine);
            let _holding = engine.vitals.activator_hold();
            engine.store.requeue(&ticket.id, delay)?;
        }
        self.others.released(&ctx.gauges, &self.vitals);
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

    /// The sealing commit is alone in its slice, and the slice before it
    /// stops short of it.
    #[test]
    fn seal_budget_isolates_the_sealing_commit() {
        let full = SliceBudget::ACTIVATOR;
        assert_eq!(seal_budget(full, None), (full, false));
        assert_eq!(seal_budget(full, Some(SEAL_INTERVAL)).0.max_events, 64);
        assert_eq!(
            seal_budget(full, Some(10)),
            (
                SliceBudget {
                    max_events: 9,
                    ..full
                },
                false
            )
        );
        assert_eq!(seal_budget(full, Some(2)).0.max_events, 1);
        assert_eq!(
            seal_budget(full, Some(1)),
            (
                SliceBudget {
                    max_events: 1,
                    ..full
                },
                true
            )
        );
        // Overdue (a branch without a snapshot, or a missed seal).
        assert_eq!(seal_budget(full, Some(-5)).0.max_events, 1);
        assert!(seal_budget(full, Some(0)).1);
    }
}
