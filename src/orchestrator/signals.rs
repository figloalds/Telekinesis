//! SIGINT/SIGTERM (Unix) and Ctrl-C, Ctrl-Break, console close, logoff and
//! system shutdown (Windows) as the orchestrator's `manage shutdown`: each
//! cancels the token [`install`] registered; the daemon's main loop then runs
//! the shutdown sequence (`hub`).
//!
//! On Unix the handler only writes a byte to a self-pipe (async-signal-safe)
//! and a watcher thread does the cancelling. On Windows the console handler
//! runs on a system thread and cancels directly; for close, logoff and
//! shutdown it then holds the process until the daemon calls [`completed`]
//! (Windows ends the process when the handler returns).
use crate::util::{cancel::Cancel, lock::lock};
use anyhow::Result;
use std::{
    sync::{Condvar, Mutex},
    time::{Duration, Instant},
};

/// The tokens a signal cancels.
static TARGETS: Mutex<Vec<Cancel>> = Mutex::new(Vec::new());
/// Set by [`completed`]; a Windows close event waits for it.
static COMPLETED: (Mutex<bool>, Condvar) = (Mutex::new(false), Condvar::new());
static INSTALLED: Mutex<bool> = Mutex::new(false);

/// Makes SIGINT/SIGTERM (Unix) and the console control events (Windows)
/// cancel `cancel`. The OS handlers are installed once per process.
pub fn install(cancel: Cancel) -> Result<()> {
    {
        let mut targets = lock(&TARGETS);
        targets.retain(|target| !target.is_cancelled());
        targets.push(cancel);
    }
    let mut installed = lock(&INSTALLED);
    if !*installed {
        platform::install()?;
        *installed = true;
    }
    Ok(())
}

/// Marks the shutdown as complete: a held console handler returns.
pub fn completed() {
    *lock(&COMPLETED.0) = true;
    COMPLETED.1.notify_all();
}

fn signalled() {
    let targets: Vec<Cancel> = lock(&TARGETS).clone();
    for target in targets {
        target.cancel();
    }
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
    use anyhow::Result;
    use std::time::Duration;

    const CTRL_CLOSE_EVENT: u32 = 2;
    const CTRL_LOGOFF_EVENT: u32 = 5;
    const CTRL_SHUTDOWN_EVENT: u32 = 6;
    /// The longest a close event holds the process: the whole shutdown
    /// sequence plus its cleanup (Windows may end the process sooner).
    const CLOSE_PATIENCE: Duration =
        Duration::from_secs(super::super::hub::SHUTDOWN_BOUND.as_secs() + 5);

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

    pub(super) fn install() -> Result<()> {
        // SAFETY: registers a handler with the documented signature; the
        // handler only touches thread-safe statics.
        let installed = unsafe { SetConsoleCtrlHandler(Some(on_console), 1) };
        anyhow::ensure!(
            installed != 0,
            "SIGNAL_HANDLER_FAILED: SetConsoleCtrlHandler: {}",
            std::io::Error::last_os_error()
        );
        Ok(())
    }
}

#[cfg(unix)]
mod platform {
    use anyhow::{Context, Result};
    use std::sync::atomic::{AtomicI32, Ordering};

    /// The write end of the self-pipe (`-1` before `install`).
    static PIPE: AtomicI32 = AtomicI32::new(-1);

    extern "C" fn on_signal(_: libc::c_int) {
        wake(PIPE.load(Ordering::Relaxed));
    }

    /// One byte into the self-pipe's non-blocking write end (a full pipe
    /// drops it: a wake-up is already pending), restoring `errno`.
    fn wake(fd: libc::c_int) {
        if fd < 0 {
            return;
        }
        let saved = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        let byte = 1u8;
        // SAFETY: write(2) is async-signal-safe, on a non-blocking descriptor
        // with a one-byte buffer that lives for the call.
        unsafe {
            libc::write(fd, (&byte as *const u8).cast(), 1);
        }
        errno::set(saved);
    }

    mod errno {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        pub(super) fn set(value: libc::c_int) {
            // SAFETY: the calling thread's errno slot.
            unsafe { *libc::__errno_location() = value }
        }
        #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
        pub(super) fn set(value: libc::c_int) {
            // SAFETY: the calling thread's errno slot.
            unsafe { *libc::__error() = value }
        }
        #[cfg(not(any(
            target_os = "linux",
            target_os = "android",
            target_os = "macos",
            target_os = "ios",
            target_os = "freebsd"
        )))]
        pub(super) fn set(_: libc::c_int) {}
    }

    /// Both ends close-on-exec (workers must never inherit them), the write
    /// end non-blocking (the handler must never block).
    fn signal_pipe() -> std::io::Result<[libc::c_int; 2]> {
        let mut fds = [0 as libc::c_int; 2];
        #[cfg(any(target_os = "linux", target_os = "android"))]
        // SAFETY: pipe2(2) on a valid two-element array.
        let created = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) };
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        // SAFETY: pipe(2) on a valid two-element array.
        let created = unsafe { libc::pipe(fds.as_mut_ptr()) };
        if created != 0 {
            return Err(std::io::Error::last_os_error());
        }
        let fail = |fds: [libc::c_int; 2]| {
            let error = std::io::Error::last_os_error();
            for fd in fds {
                // SAFETY: closes descriptors this function created.
                unsafe {
                    libc::close(fd);
                }
            }
            Err(error)
        };
        for fd in fds {
            // SAFETY: fcntl on a descriptor this function owns.
            if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
                return fail(fds);
            }
        }
        // SAFETY: fcntl on a descriptor this function owns.
        let flags = unsafe { libc::fcntl(fds[1], libc::F_GETFL) };
        // SAFETY: as above.
        if flags == -1
            || unsafe { libc::fcntl(fds[1], libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1
        {
            return fail(fds);
        }
        Ok(fds)
    }

    pub(super) fn install() -> Result<()> {
        let [read, write] = signal_pipe().context("SIGNAL_HANDLER_FAILED: signal pipe")?;
        std::thread::Builder::new()
            .name("tkfs-signals".into())
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
            })
            .context("SIGNAL_HANDLER_FAILED: watcher thread")?;
        PIPE.store(write, Ordering::Relaxed);
        // SAFETY: installs a handler that only performs an async-signal-safe
        // write(2).
        unsafe {
            libc::signal(libc::SIGINT, on_signal as *const () as libc::sighandler_t);
            libc::signal(libc::SIGTERM, on_signal as *const () as libc::sighandler_t);
        }
        Ok(())
    }
}

#[cfg(not(any(windows, unix)))]
mod platform {
    pub(super) fn install() -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_signal_cancels_every_live_target_and_completed_releases_a_wait() {
        let first = Cancel::new();
        let second = Cancel::new();
        lock(&TARGETS).extend([first.clone(), second.clone()]);
        signalled();
        assert!(first.is_cancelled() && second.is_cancelled());
        lock(&TARGETS).retain(|target| !target.is_cancelled());

        let waiter = std::thread::spawn(|| {
            let start = Instant::now();
            wait_completed(Duration::from_secs(10));
            start.elapsed()
        });
        std::thread::sleep(Duration::from_millis(50));
        completed();
        assert!(waiter.join().unwrap() < Duration::from_secs(5));
    }
}
