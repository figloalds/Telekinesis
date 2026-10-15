//! Owned by WP2.3.
//!
//! The 1 Hz maintenance thread (REVAMP_PLAN §2.5, §4.5, WP2.3 task 5), which
//! replaces the RPC server's 250 ms recovery loop:
//!
//! * **pending saves**: `Engine::retry_pending_due` under `try_lock` (a busy
//!   engine skips the tick), with a per-entity backoff of 1 s → 60 s; then
//!   `publish_observation`. On platforms without filesystem notifications it
//!   also empties the engine's notification list (as the old loop did). The
//!   engine is taken only when its lock-free vitals show such work (a dirty
//!   session, or control notifications to drop), and that hold is marked as a
//!   background hold, so the activator does not take it for filesystem load;
//! * **WAL checkpoint**: a `PASSIVE` checkpoint on a pooled secondary store
//!   ([`checkpoint_wal`]), so the engine's commits seldom run SQLite's
//!   automatic checkpoint under the engine lock. Not while the activator
//!   drains a backlog: it checkpoints between its own slices then, and a
//!   checkpoint here (a disk flush) would run concurrently with its commits.
//!   A large WAL that was copied in full is truncated ([`checkpoint_db`]);
//! * **status cell**: `status_cell::refresh_full` about once a second, spaced
//!   to at least four times its own duration (a cold content scan of a large
//!   repository during active sync costs hundreds of ms; the spacing caps its
//!   CPU share at about 20 %). On an idle store it reads back what the
//!   connection computed before: the content scan, the journal counters and
//!   the inventory match are cached until the database changes (WP2.INT's
//!   post-integration re-review; the last two were recomputed every tick,
//!   about a fifth of a core per idle folder of 20K+ events);
//! * **scrubber**: `Store::scrub_step` at most [`SCRUB_RATE`] bytes per
//!   second, only while the background activator runs and has been idle for
//!   [`SCRUB_QUIET`], and only when some available object is unverified or
//!   was last verified more than [`SCRUB_REVERIFY_AFTER`] ago. Objects this
//!   store installed were hashed when written, so a fresh repository has
//!   nothing to scrub; the scrubber verifies `Lazy` startups' rows and re-reads
//!   old objects for bit rot.
//!
//! Every step runs on a pooled secondary store or under a brief `try_lock`:
//! maintenance never waits for the engine. Failures are logged.
use crate::{
    model::{errors::ErrorWire, ipc::timing::MAINTENANCE_INTERVAL},
    runtime::{EngineVitals, ctx::WorkerCtx, lock_engine, try_lock_engine},
    util::time::now_ms,
    worker::status_cell,
};
use std::{
    sync::{Arc, atomic::Ordering},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// Scrubber rate cap (§4.5: ≤ 32 MiB/s).
pub const SCRUB_RATE: u64 = 32 * 1024 * 1024;
/// The activator must have been idle this long before the scrubber runs.
pub const SCRUB_QUIET: Duration = Duration::from_secs(5);
/// An available object verified longer ago than this is due again.
pub const SCRUB_REVERIFY_AFTER: Duration = Duration::from_secs(7 * 24 * 3600);
/// Maintenance checkpoints the WAL only once the activator has been idle
/// this long (during a backlog the activator checkpoints itself).
const CHECKPOINT_QUIET: Duration = Duration::from_secs(2);
/// The status cell's full refresh is spaced at least this many times its own
/// last duration.
const FULL_REFRESH_SPACING: u32 = 4;
/// A WAL of at least this many frames (64 MiB at 4 KiB pages) that a
/// checkpoint copied in full is truncated ([`checkpoint_db`]).
pub const WAL_TRUNCATE_FRAMES: i64 = 16_384;
/// How long that truncation waits for readers still reading the WAL. It holds
/// SQLite's writer lock meanwhile, so writers wait at most as long.
pub const WAL_TRUNCATE_WAIT: Duration = Duration::from_millis(20);

/// Starts the `maintenance` thread; it ends when the worker is stopping.
pub fn spawn(ctx: Arc<WorkerCtx>) -> std::io::Result<JoinHandle<()>> {
    thread::Builder::new()
        .name("maintenance".into())
        .spawn(move || run(&ctx))
}

fn run(ctx: &WorkerCtx) {
    let mut maintenance = Maintenance {
        vitals: lock_engine(&ctx.engine).vitals.clone(),
        next_full: None,
        next_scrub: None,
    };
    let mut errors = crate::util::backoff::Backoff::inbox();
    while !ctx.stopping.load(Ordering::Relaxed) {
        let started = Instant::now();
        // A panic in one tick (under the engine: `retry_pending_due`) never
        // ends the thread: the poison policy runs, the panic is logged and
        // the next tick waits for a growing delay, as the activator's loop
        // and the recovery loop this thread replaced do (WP2.3 review minor,
        // fixed by WP2.INT).
        let tick = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            maintenance.tick(ctx, started);
        }));
        let mut next = started + MAINTENANCE_INTERVAL;
        if tick.is_err() {
            crate::runtime::check_engine_poison(&ctx.engine);
            crate::util::log::event!(error, UNKNOWN_ERROR, {
                "site": "worker.maintenance",
                "message": "a maintenance tick panicked",
            });
            next = Instant::now() + errors.failure(Instant::now());
        } else {
            errors.success(Instant::now());
        }
        while !ctx.stopping.load(Ordering::Relaxed) {
            let left = next.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break;
            }
            thread::sleep(left.min(Duration::from_millis(100)));
        }
    }
}

struct Maintenance {
    /// The engine's lock-free vitals: whether the engine is worth taking.
    vitals: Arc<EngineVitals>,
    /// No full refresh before this.
    next_full: Option<Instant>,
    /// No scrub step before this (the rate cap).
    next_scrub: Option<Instant>,
}

impl Maintenance {
    fn tick(&mut self, ctx: &WorkerCtx, now: Instant) {
        // Fault point `worker.maintenance.tick.<repo>` (tests: a panicking
        // tick is contained; the repo keeps it to one in-process worker).
        let _ = crate::util::fault::trigger(&format!("worker.maintenance.tick.{}", ctx.repo));
        self.pending_saves(ctx, now);
        // While a backlog drains, the activator checkpoints between its own
        // slices; a checkpoint here would flush the disk concurrently with
        // an engine commit.
        let draining = ctx.activator_running.load(Ordering::SeqCst)
            && !status_cell::activator_idle(ctx, CHECKPOINT_QUIET);
        if !draining {
            checkpoint_wal(ctx);
        }
        if self.next_full.is_none_or(|at| now >= at) {
            let started = Instant::now();
            match status_cell::refresh_full_with(ctx, Some(&self.vitals)) {
                Ok(()) => {
                    let spacing = started.elapsed().saturating_mul(FULL_REFRESH_SPACING);
                    self.next_full = Some(started + spacing.max(MAINTENANCE_INTERVAL));
                }
                Err(error) => {
                    log("worker.maintenance.status", &error);
                    self.next_full = Some(started + MAINTENANCE_INTERVAL);
                }
            }
        }
        if self.next_scrub.is_none_or(|at| now >= at)
            && status_cell::activator_idle(ctx, SCRUB_QUIET)
        {
            self.scrub(ctx);
        }
    }

    /// Retries due pending saves and publishes the observation, without
    /// waiting for the engine, and only when the vitals show work: a dirty
    /// session (a save to retry) or, without filesystem notifications,
    /// control notifications to drop. An idle worker's maintenance never
    /// takes the engine (every other vital is refreshed by the engine guards
    /// themselves).
    fn pending_saves(&mut self, ctx: &WorkerCtx, now: Instant) {
        let work = self.vitals.unflushed_files() > 0
            || (!cfg!(windows) && self.vitals.control_notifications() > 0);
        if !work {
            return;
        }
        let Some(mut engine) = try_lock_engine(&ctx.engine) else {
            return;
        };
        // Not filesystem load for the activator (dropped before `engine`).
        let hold = self.vitals.background_hold();
        let committed = engine.retry_pending_due(now);
        engine.publish_observation();
        if !cfg!(windows) {
            // FUSE serves with zero TTLs: nothing to deliver.
            engine.notifications.clear();
            engine.notification_error = None;
        }
        drop(hold);
        drop(engine);
        if committed > 0 {
            status_cell::note_mutation(ctx, &self.vitals, true);
        }
    }

    /// One rate-capped scrub step, when some available object is due.
    fn scrub(&mut self, ctx: &WorkerCtx) {
        let outcome = (|| -> anyhow::Result<Option<crate::core::ScrubOutcome>> {
            let store = ctx.sync_stores.checkout()?;
            let cutoff = now_ms().saturating_sub(
                u64::try_from(SCRUB_REVERIFY_AFTER.as_millis()).unwrap_or(u64::MAX),
            );
            let due: bool = store.db.query_row(
                "SELECT EXISTS(SELECT 1 FROM object_availability WHERE state=?1 AND (verified_ms=0 OR verified_ms<?2))",
                rusqlite::params![crate::core::objects::AVAILABLE, cutoff as i64],
                |r| r.get(0),
            )?;
            if !due {
                return Ok(None);
            }
            Ok(Some(store.scrub_step(SCRUB_RATE)?))
        })();
        match outcome {
            Ok(None) => self.next_scrub = Some(Instant::now() + SCRUB_QUIET),
            Ok(Some(outcome)) => {
                // Rate cap: the bytes just read, at `SCRUB_RATE`.
                let seconds = outcome.bytes as f64 / SCRUB_RATE as f64;
                self.next_scrub = Some(
                    Instant::now() + Duration::from_secs_f64(seconds).max(MAINTENANCE_INTERVAL),
                );
                if outcome.corrupt > 0 {
                    status_cell::note_mutation(ctx, &self.vitals, true);
                }
            }
            Err(error) => {
                log("worker.maintenance.scrub", &error);
                self.next_scrub = Some(Instant::now() + SCRUB_QUIET);
            }
        }
    }
}

/// Copies the committed WAL frames into the database on a pooled secondary
/// connection, off the engine lock: `PRAGMA wal_checkpoint(PASSIVE)` never
/// waits for readers or writers, and the next write restarts a fully copied
/// WAL. So the engine connection's commits (a startup-snapshot seal writes
/// megabytes) rarely cross SQLite's automatic checkpoint threshold under the
/// engine lock (CCR-WP2.4-5). Best effort: returns the WAL's frames and the
/// frames copied, or `None`.
pub(crate) fn checkpoint_wal(ctx: &WorkerCtx) -> Option<(i64, i64)> {
    let store = ctx.sync_stores.checkout().ok()?;
    checkpoint_db(&store.db)
}

/// [`checkpoint_wal`] on `db`: a `PASSIVE` checkpoint, then, when it copied
/// a WAL of at least [`WAL_TRUNCATE_FRAMES`] frames in full, a `TRUNCATE`
/// checkpoint that waits at most [`WAL_TRUNCATE_WAIT`] for readers still on
/// the WAL (WP3.INT). Under a sustained catch-up the passive checkpoints keep
/// up, but SQLite restarts the WAL only at a write that began after a full
/// copy while no reader is still on the WAL: with commits every few
/// milliseconds and readers of seconds (the status cell's full refresh of a
/// large repository) that moment never came, and the WAL file of a
/// 150K-event catch-up grew past 9 GiB (and kept that size on disk). The
/// truncation restarts it at an idle moment instead and gives the space back.
/// Best effort; returns the `PASSIVE` checkpoint's WAL frames and frames
/// copied, or `None`.
pub fn checkpoint_db(db: &rusqlite::Connection) -> Option<(i64, i64)> {
    let (log, copied): (i64, i64) = db
        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
            Ok((row.get(1)?, row.get(2)?))
        })
        .ok()?;
    if log >= WAL_TRUNCATE_FRAMES && copied >= log {
        truncate_wal(db);
    }
    Some((log, copied))
}

/// `PRAGMA wal_checkpoint(TRUNCATE)` with the connection's busy timeout
/// lowered to [`WAL_TRUNCATE_WAIT`] for the attempt. A writer or a reader
/// that outlasts it makes the attempt give up (`SQLITE_BUSY`, ignored).
fn truncate_wal(db: &rusqlite::Connection) {
    let Ok(timeout) = db.query_row("PRAGMA busy_timeout", [], |row| row.get::<_, u64>(0)) else {
        return;
    };
    if db.busy_timeout(WAL_TRUNCATE_WAIT).is_err() {
        return;
    }
    let _ = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()));
    let _ = db.busy_timeout(Duration::from_millis(timeout));
}

fn log(site: &str, error: &anyhow::Error) {
    let wire = ErrorWire::from_error(error);
    crate::util::log::event!(warn, (wire.code.as_str()), {
        "site": site,
        "message": wire.message,
    });
}
