//! Startup progress comes from the recovering thread, never a timer thread.
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
    pub fn expired(&self, now: Instant) -> bool {
        now.duration_since(self.last) >= LEEWAY
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
