//! Owned by WP2.1.
//!
//! The shutdown sequence shared by every trigger (REVAMP_PLAN §3.6): the
//! `Shutdown` action, Ctrl-C and console close on Windows
//! (`SetConsoleCtrlHandler`), SIGINT and SIGTERM on Unix.
//!
//! 1. `stopping` is set: new actions get `ORCHESTRATOR_STOPPING`;
//! 2. `network.shutdown(5 s)`: lanes and inbound sessions are cancelled and
//!    joined;
//! 3. `folders.shutdown(deadline)`: every folder in parallel, one outcome each
//!    (`stopped`, `left_running`, `killed`, `not_running`); a busy or silent
//!    worker is left running for adoption, `desired_running` never changes;
//! 4. the reply `{stopped:true, folders:[{state, outcome, error}]}` is built
//!    (the control connection that asked flushes it before the daemon exits);
//! 5. `run` joins the control threads (2 s) and drops `orchestrator.json`.
//!
//! [`run`] is idempotent per daemon: the first caller runs the sequence, a
//! later one (a signal after the action, or the main thread after either)
//! waits for that result.
//!
//! Signals: [`install`] registers the daemon's `ctx.shutdown`; a signal
//! cancels every registered token. On Unix the handler only writes a byte to
//! a self-pipe (async-signal-safe) and a watcher thread does the cancelling;
//! on Windows the console handler runs on a system thread and cancels
//! directly, and for console close, logoff and system shutdown it holds the
//! process until the sequence completes (Windows ends the process when the
//! handler returns).
use super::ctx;
use crate::{
    model::orchestrator::{Ctx, ShutdownOutcome, limits},
    util::{cancel::Cancel, fault, lock::lock, log::event},
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Condvar, Mutex, Once, atomic::Ordering},
    time::{Duration, Instant},
};

/// Fault points of the shutdown sequence (`util::fault`).
pub mod faults {
    /// After step 1 (`stopping` is set), before the network and the folders.
    pub const SEQUENCE: &str = "orchestrator.shutdown";
}

/// The once-per-daemon shutdown result.
#[derive(Default)]
pub(crate) struct Latch {
    state: Mutex<LatchState>,
    done: Condvar,
}

#[derive(Default)]
enum LatchState {
    #[default]
    Idle,
    Running,
    Done(Value),
}

impl Latch {
    /// `true` for the caller that runs the sequence.
    fn begin(&self) -> bool {
        let mut state = lock(&self.state);
        if matches!(*state, LatchState::Idle) {
            *state = LatchState::Running;
            true
        } else {
            false
        }
    }

    fn finish(&self, reply: Value) {
        *lock(&self.state) = LatchState::Done(reply);
        self.done.notify_all();
    }

    /// The result of the run in progress (or done), waiting at most `timeout`.
    fn wait(&self, timeout: Duration) -> Option<Value> {
        let deadline = Instant::now() + timeout;
        let mut state = lock(&self.state);
        loop {
            if let LatchState::Done(reply) = &*state {
                return Some(reply.clone());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return None;
            }
            state = self
                .done
                .wait_timeout(state, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
    }
}

/// Finishes a latch whose sequence unwound (a panic) so later callers do not
/// wait for a result that never comes; disarmed by a normal finish.
struct FinishOnUnwind<'a> {
    latch: &'a Latch,
    armed: bool,
}

impl Drop for FinishOnUnwind<'_> {
    fn drop(&mut self) {
        if self.armed {
            event!(error, UNKNOWN_ERROR, {
                "detail": "the shutdown sequence panicked; the daemon still exits",
            });
            self.latch.finish(json!({
                "stopped": true,
                "folders": [],
                "error": {
                    "code": "UNKNOWN_ERROR",
                    "message": "the shutdown sequence panicked",
                },
            }));
        }
    }
}

/// The Shutdown reply for a set of outcomes.
pub fn reply(outcomes: &[ShutdownOutcome]) -> Value {
    json!({ "stopped": true, "folders": outcomes })
}

/// Runs steps 1–4 of §3.6 and returns the reply. `deadline` is the folders'
/// budget (step 3; the network has its own 5 s). A second caller waits for
/// the first caller's result.
pub fn run(ctx: &Arc<Ctx>, deadline: Duration) -> Value {
    let extras = ctx::extras(ctx);
    if !extras.shutdown.begin() {
        let patience = deadline + limits::SHUTDOWN_NETWORK + Duration::from_secs(5);
        return extras
            .shutdown
            .wait(patience)
            .unwrap_or_else(|| json!({ "stopped": true, "folders": [] }));
    }
    let mut unwinding = FinishOnUnwind {
        latch: &extras.shutdown,
        armed: true,
    };
    let started = Instant::now();
    // 1. stopping: every new action is refused.
    ctx.stopping.store(true, Ordering::Release);
    if let Some(hub) = extras.hub() {
        hub.set_stopping(true);
    }
    event!(info, ORCHESTRATOR_STOPPING, {
        "detail": "shutdown started",
    });
    if let Err(error) = fault::trigger(faults::SEQUENCE) {
        event!(warn, ORCHESTRATOR_STOPPING, {
            "detail": "fault injected into the shutdown sequence",
            "error": error.to_string(),
        });
    }
    // 2. the network: lanes and inbound sessions.
    if let Ok(network) = ctx.network() {
        network.shutdown(limits::SHUTDOWN_NETWORK);
    }
    // 3. the folders, in parallel; desired_running is never changed.
    let outcomes = ctx.folders.shutdown(deadline);
    for outcome in &outcomes {
        if outcome.outcome != crate::model::orchestrator::outcomes::STOPPED
            && outcome.outcome != crate::model::orchestrator::outcomes::NOT_RUNNING
        {
            event!(info, ORCHESTRATOR_STOPPING, {
                "folder": outcome.state,
                "outcome": outcome.outcome,
                "error": outcome.error.as_ref().map(|e| e.code.clone()),
            });
        }
    }
    let reply = reply(&outcomes);
    event!(info, ORCHESTRATOR_STOPPING, {
        "detail": "folders stopped",
        "folders": outcomes.len(),
        "elapsed_ms": u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
    });
    unwinding.armed = false;
    extras.shutdown.finish(reply.clone());
    reply
}

// ---------------------------------------------------------------------------
// Signals

/// The `ctx.shutdown` tokens of the daemons in this process.
static TARGETS: Mutex<Vec<Cancel>> = Mutex::new(Vec::new());
/// Set by [`completed`] when a daemon finished its sequence (Windows console
/// close waits for it).
static COMPLETED: (Mutex<bool>, Condvar) = (Mutex::new(false), Condvar::new());
static INSTALL: Once = Once::new();

/// Registers `ctx.shutdown` as the target of SIGINT/SIGTERM (Unix) and of
/// Ctrl-C, Ctrl-Break, console close, logoff and system shutdown (Windows).
/// The OS handlers are installed once per process.
pub fn install(ctx: &Arc<Ctx>) {
    {
        let mut targets = lock(&TARGETS);
        targets.retain(|target| !target.is_cancelled());
        targets.push(ctx.shutdown.clone());
    }
    INSTALL.call_once(platform::install);
}

/// What a signal does: cancel every registered daemon's `shutdown` token.
fn signalled() {
    let targets: Vec<Cancel> = lock(&TARGETS).clone();
    for target in targets {
        target.cancel();
    }
}

/// Marks this process's shutdown as complete (`run.rs`, last step).
pub(crate) fn completed() {
    *lock(&COMPLETED.0) = true;
    COMPLETED.1.notify_all();
}

/// Waits at most `timeout` for [`completed`].
#[cfg_attr(not(windows), allow(dead_code))]
fn wait_completed(timeout: Duration) {
    let deadline = Instant::now() + timeout;
    let mut done = lock(&COMPLETED.0);
    while !*done {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return;
        }
        done = COMPLETED
            .1
            .wait_timeout(done, remaining)
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .0;
    }
}

#[cfg(windows)]
mod platform {
    use std::time::Duration;

    const CTRL_CLOSE_EVENT: u32 = 2;
    const CTRL_LOGOFF_EVENT: u32 = 5;
    const CTRL_SHUTDOWN_EVENT: u32 = 6;
    /// Windows ends the process about 5 s after a close event; leave time to
    /// remove `orchestrator.json`.
    const CLOSE_PATIENCE: Duration = Duration::from_millis(4500);

    type Handler = unsafe extern "system" fn(u32) -> i32;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn SetConsoleCtrlHandler(handler: Option<Handler>, add: i32) -> i32;
    }

    unsafe extern "system" fn on_console(kind: u32) -> i32 {
        super::signalled();
        if matches!(
            kind,
            CTRL_CLOSE_EVENT | CTRL_LOGOFF_EVENT | CTRL_SHUTDOWN_EVENT
        ) {
            // The process ends when this returns: let the sequence finish.
            super::wait_completed(CLOSE_PATIENCE);
        }
        1
    }

    pub(super) fn install() {
        // SAFETY: registers a handler with the documented signature; the
        // handler only touches thread-safe statics.
        let installed = unsafe { SetConsoleCtrlHandler(Some(on_console), 1) };
        if installed == 0 {
            crate::util::log::event!(warn, ORCHESTRATOR_STOPPING, {
                "detail": "SetConsoleCtrlHandler failed; Ctrl-C ends the daemon without the shutdown sequence",
                "error": std::io::Error::last_os_error().to_string(),
            });
        }
    }
}

#[cfg(unix)]
mod platform {
    use std::sync::atomic::{AtomicI32, Ordering};

    /// The write end of the self-pipe (`-1` before `install`).
    static PIPE: AtomicI32 = AtomicI32::new(-1);

    extern "C" fn on_signal(_: libc::c_int) {
        let fd = PIPE.load(Ordering::Relaxed);
        if fd >= 0 {
            let byte = 1u8;
            // SAFETY: write(2) is async-signal-safe; a full pipe drops the byte
            // (a wake-up is already pending).
            unsafe {
                libc::write(fd, (&byte as *const u8).cast(), 1);
            }
        }
    }

    pub(super) fn install() {
        let mut fds = [0 as libc::c_int; 2];
        // SAFETY: pipe(2) on a valid two-element array.
        if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
            crate::util::log::event!(warn, ORCHESTRATOR_STOPPING, {
                "detail": "cannot create the signal pipe; signals end the daemon without the shutdown sequence",
                "error": std::io::Error::last_os_error().to_string(),
            });
            return;
        }
        for fd in fds {
            // Workers are spawned by this process: never let them inherit it.
            // SAFETY: fcntl on a descriptor this function owns.
            unsafe {
                libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
            }
        }
        let read = fds[0];
        let watcher = std::thread::Builder::new()
            .name("shutdown-signal".into())
            .spawn(move || {
                loop {
                    let mut byte = 0u8;
                    // SAFETY: read(2) into a one-byte buffer this thread owns.
                    let n = unsafe { libc::read(read, (&mut byte as *mut u8).cast(), 1) };
                    if n == 1 {
                        super::signalled();
                    } else if n < 0
                        && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
                    {
                        continue;
                    } else {
                        return;
                    }
                }
            });
        if watcher.is_err() {
            return;
        }
        PIPE.store(fds[1], Ordering::Relaxed);
        // SAFETY: installs a handler that only performs an async-signal-safe
        // write(2).
        unsafe {
            libc::signal(libc::SIGINT, on_signal as libc::sighandler_t);
            libc::signal(libc::SIGTERM, on_signal as libc::sighandler_t);
        }
    }
}

#[cfg(not(any(windows, unix)))]
mod platform {
    pub(super) fn install() {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    #[test]
    fn a_sequence_that_unwinds_still_finishes_the_latch() {
        let latch = Latch::default();
        assert!(latch.begin());
        let unwound = catch_unwind(AssertUnwindSafe(|| {
            let _guard = FinishOnUnwind {
                latch: &latch,
                armed: true,
            };
            panic!("the sequence panicked");
        }));
        assert!(unwound.is_err());
        // A later caller gets a result at once instead of its full patience.
        let reply = latch.wait(Duration::from_millis(100)).expect("finished");
        assert_eq!(reply["stopped"], true);
        assert_eq!(reply["error"]["code"], "UNKNOWN_ERROR");
        assert!(!latch.begin(), "the sequence runs once");
    }

    #[test]
    fn a_disarmed_guard_leaves_the_result_to_finish() {
        let latch = Latch::default();
        assert!(latch.begin());
        {
            let mut guard = FinishOnUnwind {
                latch: &latch,
                armed: true,
            };
            guard.armed = false;
        }
        assert!(latch.wait(Duration::from_millis(20)).is_none());
        latch.finish(reply(&[]));
        assert_eq!(
            latch.wait(Duration::from_millis(20)),
            Some(json!({"stopped": true, "folders": []}))
        );
    }
}
