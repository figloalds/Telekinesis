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
//!    `ACTIVATOR_IDLE_WAIT` (due retries), unless the last slice left more;
//! 2. yield to the engine's other users (bounded by [`MAX_YIELD`]): while a
//!    thread waits for the engine, holds it, or took it in the last
//!    [`WAITER_GRACE`], the activator does not start a slice, so it does not
//!    slip one between the callbacks of a filesystem operation;
//! 3. under the engine lock: `activate_slice(SliceBudget::ACTIVATOR)` (slow
//!    events are not committed inline: the slice stops and returns a
//!    `SlowTicket`), then `Engine::after_activation` (session refresh).
//!    While mounted, notifications are computed per batch of slices: the
//!    `before` projection (entries only) is captured when a batch begins and
//!    diffed against the view for every entity the batch activated when it
//!    ends (after [`NOTIFY_BATCH`], or twenty times the capture's own cost if
//!    longer, or when no work is left), so the O(entries) capture is not
//!    paid on every slice;
//! 4. a `SlowTicket` is prepared off-lock (`Store::prepare_slow` on a pooled
//!    `SyncStore`) and committed under the lock (`commit_prepared`;
//!    `Prepared::Inline` runs the slow path there until WP2.4's off-lock
//!    preparation lands). A `Stale` ticket is re-queued with a per-event
//!    backoff (250 ms → 30 s), never committed inline; while tickets keep
//!    going stale, `activation.starved_since_ms` is set;
//! 5. record the counters, bump `ctx.status_generation` and refresh the
//!    status cell (which wakes the heartbeat: `generations.sync` moves).
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
        health::ActivationCounters,
        ipc::timing::{ACTIVATION_SLICE_EVENTS, ACTIVATION_SLICE_MS, ACTIVATOR_IDLE_WAIT},
    },
    runtime::{check_engine_poison, ctx::WorkerCtx, lock_engine, try_lock_engine},
    util::{backoff::Backoff, time::now_ms},
    worker::status_cell,
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
/// `Stale` re-queue backoff, per event.
const STALE_BASE: Duration = Duration::from_millis(250);
const STALE_CAP: Duration = Duration::from_secs(30);
/// Events whose stale count is remembered (oldest forgotten first).
const STALE_TRACKED: usize = 1024;
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
    lock_engine(&ctx.engine).background_activation = true;
    ctx.activator_running.store(true, Ordering::SeqCst);
    let mut activator = Activator::default();
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
    }
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
/// sees them through the lock gauges.
#[derive(Default)]
struct Others {
    /// The engine's hold count right after the activator's own last release.
    holds_after_own: u64,
    /// When another thread was last seen using (or waiting for) the engine.
    active_at: Option<Instant>,
}

impl Others {
    /// Records the activator's own release (call right after it).
    fn released(&mut self, ctx: &WorkerCtx) {
        self.holds_after_own = ctx.gauges.snapshot().holds;
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
    fn yield_to(&mut self, ctx: &WorkerCtx) -> Duration {
        let started = Instant::now();
        let mut holds = self.holds_after_own;
        loop {
            let gauges = ctx.gauges.snapshot();
            if gauges.waiters > 0 || gauges.current_hold_ms > 0 || gauges.holds != holds {
                self.active_at = Some(Instant::now());
                holds = gauges.holds;
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

#[derive(Default)]
struct Activator {
    counters: ActivationCounters,
    /// Consecutive `Stale` outcomes per event (re-queue backoff).
    stale: BTreeMap<String, u32>,
    /// The notification batch in progress (mounted only).
    batch: Option<NotifyBatchState>,
    /// The engine's other users.
    others: Others,
}

impl Activator {
    /// One slice, plus the slow commit it may hand over. Returns whether more
    /// work is (probably) ready.
    fn cycle(&mut self, ctx: &WorkerCtx) -> Result<bool> {
        self.others.yield_to(ctx);
        let started = Instant::now();
        let budget = self.others.budget();
        let slice = {
            let mut engine = lock_engine(&ctx.engine);
            let _holding = engine.vitals.activator_hold();
            self.begin_batch(&engine);
            let slice = engine.store.activate_slice(budget);
            if let Ok(slice) = &slice {
                self.activated(&mut engine, &slice.activated);
                if !slice.more && slice.slow_ticket.is_none() {
                    self.end_batch(&mut engine);
                }
            }
            slice
        };
        self.others.released(ctx);
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
        Ok(more)
    }

    /// Prepares `ticket` off-lock and commits it under the lock. Returns
    /// whether the store changed.
    fn slow(&mut self, ctx: &WorkerCtx, ticket: SlowTicket) -> Result<bool> {
        let prepared = {
            let read = ctx.sync_stores.checkout()?;
            Store::prepare_slow(&read, &ticket)?
        };
        self.others.yield_to(ctx);
        let outcome = {
            let mut engine = lock_engine(&ctx.engine);
            let _holding = engine.vitals.activator_hold();
            self.begin_batch(&engine);
            let outcome = engine.store.commit_prepared(prepared)?;
            match &outcome {
                CommitOutcome::Committed { id, .. } => {
                    self.activated(&mut engine, std::slice::from_ref(id));
                }
                CommitOutcome::Stale => {
                    let failures = self.stale.entry(ticket.id.clone()).or_insert(0);
                    *failures = failures.saturating_add(1);
                    let delay = STALE_BASE
                        .saturating_mul(1u32 << (*failures - 1).min(7))
                        .min(STALE_CAP);
                    engine.store.requeue(
                        &ticket.id,
                        u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                    )?;
                }
                CommitOutcome::Failed(_) => {}
            }
            outcome
        };
        self.others.released(ctx);
        Ok(match outcome {
            CommitOutcome::Committed { slow, .. } => {
                self.stale.remove(&ticket.id);
                self.counters.activated += 1;
                if slow {
                    self.counters.slow += 1;
                } else {
                    self.counters.fast += 1;
                }
                self.counters.last_activation_at_ms = Some(now_ms());
                self.counters.starved_since_ms = None;
                true
            }
            CommitOutcome::Stale => {
                self.counters.stale += 1;
                self.counters.starved_since_ms.get_or_insert_with(now_ms);
                if self.stale.len() > STALE_TRACKED
                    && let Some(oldest) = self.stale.keys().next().cloned()
                {
                    self.stale.remove(&oldest);
                }
                crate::util::log::event!(info, STALE, {
                    "site": "worker.activator",
                    "event": ticket.id,
                    "branch": ticket.branch,
                });
                true
            }
            CommitOutcome::Failed(error) => {
                self.stale.remove(&ticket.id);
                if error.class == ErrorClass::Fatal || error.class == ErrorClass::NeedsUserAction {
                    self.counters.quarantined += 1;
                } else {
                    self.counters.retried += 1;
                }
                true
            }
        })
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
        if let Err(error) = status_cell::record_activation(ctx, self.counters) {
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
