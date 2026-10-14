//! Startup progress comes from the recovering thread, never a timer thread.
//! Also the worker's other stdout startup lines (Ready, Fatal; §6.1).
use std::{
    cell::RefCell,
    io::Write,
    time::{Duration, Instant},
};

pub const LEEWAY: Duration = Duration::from_secs(30);
thread_local! {
    static REPORTER: RefCell<Option<Reporter>> = const { RefCell::new(None) };
}
struct Reporter {
    last: Option<Instant>,
    sequence: u64,
}
pub struct Reporting;
impl Reporting {
    pub fn begin() -> Self {
        REPORTER.with(|r| {
            *r.borrow_mut() = Some(Reporter {
                last: None,
                sequence: 0,
            })
        });
        progress("opening state");
        Self
    }
}
impl Drop for Reporting {
    fn drop(&mut self) {
        REPORTER.with(|r| *r.borrow_mut() = None);
    }
}

/// Call at completed units of recovery work. Throttled to one message/second.
pub fn progress(phase: &'static str) {
    REPORTER.with(|slot| {
        let mut slot = slot.borrow_mut();
        let Some(r) = slot.as_mut() else { return };
        let now = Instant::now();
        if r.last
            .is_some_and(|last| now.duration_since(last) < Duration::from_secs(1))
        {
            return;
        }
        r.sequence += 1;
        r.last = Some(now);
        // A broken supervisor pipe must not panic inside filesystem recovery.
        let _ = writeln!(
            std::io::stdout().lock(),
            "{}",
            serde_json::json!({
                "startup_progress": r.sequence, "phase": phase
            })
        );
    });
}

/// Prints one stdout startup line (a `ReadyLine` or a `FatalLine`, §6.1). A
/// closed stdout (the supervisor went away) is ignored, never a panic.
pub fn print_line<T: serde::Serialize>(line: &T) {
    if let Ok(text) = serde_json::to_string(line) {
        let mut out = std::io::stdout().lock();
        let _ = writeln!(out, "{text}");
        let _ = out.flush();
    }
}

/// A failure before Ready: prints the `FatalLine` on stdout, logs it on
/// stderr (the worker log) and exits with its `exit_code`.
pub fn fatal_exit(fatal: &crate::model::ipc::FatalLine) -> ! {
    print_line(fatal);
    crate::util::log::event!(fatal, (fatal.code.as_str()), {
        "class": fatal.class,
        "message": fatal.message,
        "exit_code": fatal.exit_code,
    });
    std::process::exit(fatal.exit_code)
}

pub(crate) struct Watchdog {
    last: Instant,
    sequence: u64,
    pub phase: String,
}
impl Watchdog {
    pub fn new(now: Instant) -> Self {
        Self {
            last: now,
            sequence: 0,
            phase: "launching worker".into(),
        }
    }
    pub fn observe(&mut self, sequence: u64, phase: &str, now: Instant) {
        if sequence > self.sequence {
            self.sequence = sequence;
            self.phase = phase.into();
            self.last = now;
        }
    }
    /// The legacy supervisor's rule (deleted by WP2.INT); the unit tests
    /// still pin it as `expired_after(now, LEEWAY)`.
    #[cfg(test)]
    pub fn expired(&self, now: Instant) -> bool {
        self.expired_after(now, LEEWAY)
    }
    /// Whether `silence` has passed without progress (a supervisor's own
    /// startup-silence limit; [`Self::expired`] uses [`LEEWAY`]). CCR-WP1.2-4.
    pub fn expired_after(&self, now: Instant, silence: Duration) -> bool {
        now.saturating_duration_since(self.last) >= silence
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn advancing_work_can_run_indefinitely_but_silence_expires() {
        let start = Instant::now();
        let mut w = Watchdog::new(start);
        assert!(!w.expired(start + Duration::from_secs(29)));
        assert!(w.expired(start + LEEWAY));
        for sequence in 1..=100 {
            let now = start + Duration::from_secs(sequence * 20);
            w.observe(sequence, "replaying history", now);
            assert!(!w.expired(now + Duration::from_secs(29)));
        }
        let last = start + Duration::from_secs(2000);
        w.observe(100, "duplicate", last + Duration::from_secs(29));
        w.observe(99, "old", last + Duration::from_secs(29));
        assert!(w.expired(last + LEEWAY));
        assert_eq!(w.phase, "replaying history");
    }
}
