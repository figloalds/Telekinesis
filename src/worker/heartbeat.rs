//! Owned by WP1.4 (basic heartbeat); WP2.3 adds the full groups.
//!
//! The worker heartbeat (REVAMP_PLAN §4.5, §6.1): a [`WorkerHeartbeat`]
//! published into `ctx.heartbeat` by the `heartbeat` thread, at least once per
//! `HEARTBEAT_INTERVAL` and promptly (at most every
//! [`PROMPT_BEAT_INTERVAL`]) after the status cell changed, and on demand
//! after a mount transition. It is built from atomics, the status cell and
//! `MountControl::status()` only: the engine vitals and lock gauges, the
//! cell's inbox, activation, content and journal counters, the feed tip,
//! `generations` (sync from `status_generation` plus the engine's store
//! revision, branches and content from the cell, mount from the controller)
//! and the per-peer feed counters of `ctx.sync_obs`. It never takes the
//! engine lock, so a wedged engine still has a live heartbeat whose
//! `engine.current_hold_ms` / `engine.stalled` say so. The `watch` RPC waits
//! on `ctx.heartbeat.wait_newer`.
use crate::{
    model::ipc::{WorkerHeartbeat, timing::HEARTBEAT_INTERVAL},
    runtime::{EngineVitals, ctx::WorkerCtx},
    util::time::now_ms,
    worker::status_cell,
};
use std::{
    sync::{Arc, atomic::Ordering},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// Prompt beats (after a status cell change) are at least this far apart, so
/// an activation burst wakes `watch` callers about ten times a second, not
/// once per slice.
pub const PROMPT_BEAT_INTERVAL: Duration = Duration::from_millis(100);

/// Publishes the next heartbeat (`seq` + 1) and returns it.
///
/// `generations.mount` is the mount controller's own status generation
/// (`MountControl::generation`, CCR-WP1.5-2), so every state change counts,
/// also several between two beats; a controller that does not count (0)
/// falls back to counting the changes seen between beats. The mount status
/// is read inside the snapshot update, so concurrent beats (the heartbeat
/// thread and a mount transition) publish their `seq` in the order they read
/// the mount: a later `seq` never carries an older mount state (WP1.4 review
/// finding, fixed by WP1.INT). The generation is read before the status, so
/// the published status is always at least as new as the published
/// generation: a transition between the two reads shows its new status at
/// once and its generation on the next beat, never a new generation with
/// the old status (which a consumer keying on `generations.mount` would
/// take as seen). Both reads are lock-free snapshot loads.
///
/// Every generation is kept monotonic across beats (`max` with the previous
/// heartbeat's).
pub fn beat(ctx: &WorkerCtx, vitals: &EngineVitals) -> Arc<WorkerHeartbeat> {
    let engine = ctx.gauges.snapshot();
    let store_health = vitals.store_health();
    let mount_errors = crate::mount::mount_errors();
    let cell = status_cell::view(&ctx.status_cell.load());
    let sync = status_cell::sync_generation(ctx, vitals);
    let mut peers: Vec<_> = crate::util::lock::lock(&ctx.sync_obs)
        .values()
        .cloned()
        .collect();
    peers.sort_by(|a, b| a.replica.cmp(&b.replica));
    let at_ms = now_ms();
    ctx.heartbeat.update(|previous| {
        let counted = ctx.mount.generation();
        let mount = ctx.mount.status();
        let mut generations = previous.generations;
        match counted {
            0 if previous.seq > 0 && previous.mount != mount => generations.mount += 1,
            0 => {}
            counted => generations.mount = generations.mount.max(counted),
        }
        generations.sync = generations.sync.max(sync);
        generations.branches = generations.branches.max(cell.meta.branches);
        generations.content = generations.content.max(cell.meta.content);
        WorkerHeartbeat {
            instance: ctx.instance.clone(),
            seq: previous.seq + 1,
            at_ms,
            uptime_ms: at_ms.saturating_sub(ctx.started_at_ms),
            mount,
            engine,
            store_health: store_health.clone(),
            pending_saves: vitals.pending_saves(),
            open_handles: vitals.open_handles(),
            unflushed_files: vitals.unflushed_files(),
            generations,
            tip: cell.tip.clone(),
            inbox: cell.inbox,
            activation: cell.activation.map(|mut activation| {
                activation.running = ctx.activator_running.load(Ordering::SeqCst);
                activation
            }),
            content: cell.content,
            journal: cell.journal.clone(),
            peers: peers.clone(),
            mount_errors,
        }
    });
    ctx.heartbeat.load()
}

/// Whether the cell or the sync generation moved away from what `published`
/// carries (a refresh that only re-stamped the cell does not count).
fn moved(ctx: &WorkerCtx, vitals: &EngineVitals, published: &WorkerHeartbeat) -> bool {
    let cell = status_cell::view(&ctx.status_cell.load());
    cell.inbox != published.inbox
        || cell
            .activation
            .map(|a| (a.activated, a.quarantined, a.slices, a.stale))
            != published
                .activation
                .map(|a| (a.activated, a.quarantined, a.slices, a.stale))
        || cell.content != published.content
        || cell.tip != published.tip
        || cell.meta.branches > published.generations.branches
        || cell.meta.content > published.generations.content
        || status_cell::sync_generation(ctx, vitals) > published.generations.sync
}

/// Starts the `heartbeat` thread: one [`beat`] per `HEARTBEAT_INTERVAL`, and
/// one soon after the status cell's counters change (at most every
/// [`PROMPT_BEAT_INTERVAL`]), until the worker is stopping.
pub fn spawn(ctx: Arc<WorkerCtx>, vitals: Arc<EngineVitals>) -> std::io::Result<JoinHandle<()>> {
    thread::Builder::new()
        .name("heartbeat".into())
        .spawn(move || {
            let mut seen = ctx.status_cell.generation();
            while !ctx.stopping.load(Ordering::Relaxed) {
                let last = Instant::now();
                let published = beat(&ctx, &vitals);
                // Wait for the next period, or a counter change; then keep
                // the prompt beats apart.
                let deadline = last + HEARTBEAT_INTERVAL;
                loop {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() || ctx.stopping.load(Ordering::Relaxed) {
                        break;
                    }
                    let generation = ctx.status_cell.wait_newer(seen, remaining);
                    if generation == seen {
                        break;
                    }
                    seen = generation;
                    if moved(&ctx, &vitals, &published) {
                        let earliest = last + PROMPT_BEAT_INTERVAL;
                        let pause = earliest.saturating_duration_since(Instant::now());
                        if !pause.is_zero() {
                            thread::sleep(pause);
                        }
                        seen = seen.max(ctx.status_cell.generation());
                        break;
                    }
                }
            }
        })
}
