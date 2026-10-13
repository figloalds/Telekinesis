//! Owned by WP1.5 — stub landed by WP0.INT
//!
//! The worker's `MountControl` (REVAMP_PLAN §4.3). This stub keeps today's
//! semantics by delegating to `mount::start` and `mount::stop`, exactly as the
//! worker's startup and `stop` op do:
//!
//! - `attach(path)` calls `mount::start`. On WinFsp a second attach in the same
//!   process still fails with `ONE_MOUNT_PER_RUNTIME`, and a failed path check
//!   is reported as `AttachFailed` (classified by `model::errors::classify`).
//! - `detach(force)` calls `mount::stop` when the engine is mounted (graceful
//!   refusal is `BUSY_VIEW`). `force` has no separate path yet: WP1.5 adds the
//!   fence, drain and `retain_all_dirty` sequence.
//! - `status()` reads a published snapshot and never waits for an attach or
//!   detach in progress.
//!
//! WP1.5 replaces the internals (preflight, re-attach, force detach, FUSE
//! recovery with `previous`) behind the same constructor and trait.
use crate::{
    model::{
        errors::ErrorWire,
        health::{MountControl, MountIntent, MountState, MountStatus},
        ipc::WorkerRecord,
    },
    runtime::{Shared, lock_engine},
    util::{lock::lock, snapshot::Snapshot, time::now_ms},
};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

pub struct MountController {
    engine: Shared,
    previous: Option<WorkerRecord>,
    status: Snapshot<MountStatus>,
    /// Serializes attach and detach; counts consecutive failed attaches.
    attempts: Mutex<u32>,
}

impl MountController {
    pub fn new(engine: Shared, previous: Option<WorkerRecord>) -> Arc<Self> {
        Arc::new(Self {
            engine,
            previous,
            status: Snapshot::new(MountStatus {
                intent: MountIntent::Headless,
                path: None,
                state: MountState::Detached,
            }),
            attempts: Mutex::new(0),
        })
    }

    /// The previous `worker.json` this controller was built with.
    pub fn previous(&self) -> Option<&WorkerRecord> {
        self.previous.as_ref()
    }

    fn publish(&self, status: MountStatus) -> MountStatus {
        self.status.store(status.clone());
        status
    }
}

impl MountControl for MountController {
    fn attach(&self, path: &Path) -> Result<MountStatus, ErrorWire> {
        let mut attempts = lock(&self.attempts);
        let current = self.status();
        if current.state.is_attached() && current.path.as_deref() == Some(path) {
            return Ok(current);
        }
        match crate::mount::start(self.engine.clone(), path) {
            Ok(()) => {
                *attempts = 0;
                Ok(self.publish(MountStatus {
                    intent: MountIntent::Attach,
                    path: Some(path.to_owned()),
                    state: MountState::Attached { since_ms: now_ms() },
                }))
            }
            Err(error) => {
                let error = ErrorWire::from_error(&error);
                // A refused second attach leaves the existing mount in place.
                if !current.state.is_attached() {
                    *attempts = attempts.saturating_add(1);
                    self.publish(MountStatus {
                        intent: MountIntent::Attach,
                        path: Some(path.to_owned()),
                        state: MountState::AttachFailed {
                            error: error.clone(),
                            attempt: *attempts,
                            next_retry_at_ms: None,
                        },
                    });
                }
                Err(error)
            }
        }
    }

    fn detach(&self, force: bool) -> Result<MountStatus, ErrorWire> {
        let _serialized = lock(&self.attempts);
        let _ = force;
        let mounted = lock_engine(&self.engine).mounted;
        if mounted {
            crate::mount::stop(&self.engine).map_err(|error| ErrorWire::from_error(&error))?;
        }
        let current = self.status();
        Ok(self.publish(MountStatus {
            intent: MountIntent::Detach,
            path: current.path,
            state: MountState::Detached,
        }))
    }

    fn status(&self) -> MountStatus {
        (*self.status.load()).clone()
    }
}
