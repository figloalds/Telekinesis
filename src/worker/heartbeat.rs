//! Owned by WP1.4 (basic heartbeat); WP2.3 adds the full groups.
//!
//! The worker heartbeat (REVAMP_PLAN §4.5, §6.1): a [`WorkerHeartbeat`]
//! published into `ctx.heartbeat` at 1 Hz by the `heartbeat` thread, and on
//! demand after a mount transition. It is built from atomics only: the engine
//! vitals, the engine-lock gauges, `MountControl::status()`. It never takes
//! the engine lock, so a wedged engine still has a live heartbeat whose
//! `engine.current_hold_ms` / `engine.stalled` say so. The `watch` RPC waits
//! on `ctx.heartbeat.wait_newer`.
use crate::{
    model::ipc::{WorkerHeartbeat, timing::HEARTBEAT_INTERVAL},
    runtime::{EngineVitals, ctx::WorkerCtx},
    util::time::now_ms,
};
use std::{
    sync::{Arc, atomic::Ordering},
    thread::{self, JoinHandle},
};

/// Publishes the next heartbeat (`seq` + 1) and returns it.
/// `generations.mount` counts mount status changes between beats.
pub fn beat(ctx: &WorkerCtx, vitals: &EngineVitals) -> Arc<WorkerHeartbeat> {
    let mount = ctx.mount.status();
    let engine = ctx.gauges.snapshot();
    let store_health = vitals.store_health();
    let at_ms = now_ms();
    ctx.heartbeat.update(|previous| {
        let mut generations = previous.generations;
        if previous.seq > 0 && previous.mount != mount {
            generations.mount += 1;
        }
        WorkerHeartbeat {
            instance: ctx.instance.clone(),
            seq: previous.seq + 1,
            at_ms,
            uptime_ms: at_ms.saturating_sub(ctx.started_at_ms),
            mount: mount.clone(),
            engine,
            store_health: store_health.clone(),
            pending_saves: vitals.pending_saves(),
            open_handles: vitals.open_handles(),
            unflushed_files: vitals.unflushed_files(),
            generations,
            ..WorkerHeartbeat::default()
        }
    });
    ctx.heartbeat.load()
}

/// Starts the `heartbeat` thread: one [`beat`] per `HEARTBEAT_INTERVAL` until
/// the worker is stopping.
pub fn spawn(ctx: Arc<WorkerCtx>, vitals: Arc<EngineVitals>) -> std::io::Result<JoinHandle<()>> {
    thread::Builder::new()
        .name("heartbeat".into())
        .spawn(move || {
            while !ctx.stopping.load(Ordering::Relaxed) {
                beat(&ctx, &vitals);
                thread::sleep(HEARTBEAT_INTERVAL);
            }
        })
}
