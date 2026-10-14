//! Owned by WP2.3.
//!
//! The 1 Hz maintenance thread (REVAMP_PLAN §2.5, §4.5, WP2.3 task 5), which
//! replaces the RPC server's 250 ms recovery loop:
//!
//! * **pending saves**: `Engine::retry_pending_due` under `try_lock` (a busy
//!   engine skips the tick), with a per-entity backoff of 1 s → 60 s; then
//!   `publish_observation`. On platforms without filesystem notifications it
//!   also empties the engine's notification list (as the old loop did);
//! * **status cell**: `status_cell::refresh_full` about once a second, spaced
//!   to at least four times its own duration (a cold content scan of a large
//!   repository during active sync costs hundreds of ms; the spacing caps its
//!   CPU share at about 20 %);
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
    runtime::{ctx::WorkerCtx, try_lock_engine},
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
/// The status cell's full refresh is spaced at least this many times its own
/// last duration.
const FULL_REFRESH_SPACING: u32 = 4;

/// Starts the `maintenance` thread; it ends when the worker is stopping.
pub fn spawn(ctx: Arc<WorkerCtx>) -> std::io::Result<JoinHandle<()>> {
    thread::Builder::new()
        .name("maintenance".into())
        .spawn(move || run(&ctx))
}

fn run(ctx: &WorkerCtx) {
    let mut maintenance = Maintenance::default();
    while !ctx.stopping.load(Ordering::Relaxed) {
        let started = Instant::now();
        maintenance.tick(ctx, started);
        let next = started + MAINTENANCE_INTERVAL;
        while !ctx.stopping.load(Ordering::Relaxed) {
            let left = next.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break;
            }
            thread::sleep(left.min(Duration::from_millis(100)));
        }
    }
}

#[derive(Default)]
struct Maintenance {
    /// No full refresh before this.
    next_full: Option<Instant>,
    /// No scrub step before this (the rate cap).
    next_scrub: Option<Instant>,
}

impl Maintenance {
    fn tick(&mut self, ctx: &WorkerCtx, now: Instant) {
        self.pending_saves(ctx, now);
        if self.next_full.is_none_or(|at| now >= at) {
            let started = Instant::now();
            match status_cell::refresh_full(ctx) {
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
    /// waiting for the engine.
    fn pending_saves(&mut self, ctx: &WorkerCtx, now: Instant) {
        let Some(mut engine) = try_lock_engine(&ctx.engine) else {
            return;
        };
        let committed = engine.retry_pending_due(now);
        engine.publish_observation();
        if !cfg!(windows) {
            // FUSE serves with zero TTLs: nothing to deliver.
            engine.notifications.clear();
            engine.notification_error = None;
        }
        drop(engine);
        if committed > 0 {
            status_cell::note_mutation(ctx, true);
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
                    status_cell::note_mutation(ctx, true);
                }
            }
            Err(error) => {
                log("worker.maintenance.scrub", &error);
                self.next_scrub = Some(Instant::now() + SCRUB_QUIET);
            }
        }
    }
}

fn log(site: &str, error: &anyhow::Error) {
    let wire = ErrorWire::from_error(error);
    crate::util::log::event!(warn, (wire.code.as_str()), {
        "site": site,
        "message": wire.message,
    });
}
