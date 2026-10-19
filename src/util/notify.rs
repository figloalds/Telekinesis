//! A wake-up counter: producers `poke`, consumers wait for the count to pass the
//! value they last saw. Pokes are never lost and coalesce naturally.
use std::{
    sync::{Condvar, Mutex, PoisonError},
    time::{Duration, Instant},
};

#[derive(Debug, Default)]
pub struct Notify {
    count: Mutex<u64>,
    poked: Condvar,
}

impl Notify {
    pub fn new() -> Self {
        Self::default()
    }

    /// Increments the counter and wakes every waiter.
    pub fn poke(&self) {
        let mut count = self.count.lock().unwrap_or_else(PoisonError::into_inner);
        *count += 1;
        drop(count);
        self.poked.notify_all();
    }

    /// Blocks until the counter exceeds `seen` or `timeout` elapses; returns the
    /// counter at that moment (equal to `seen` on a quiet timeout).
    pub fn wait(&self, seen: u64, timeout: Duration) -> u64 {
        let deadline = Instant::now().checked_add(timeout);
        let mut count = self.count.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            if *count > seen {
                return *count;
            }
            let remaining = match deadline {
                Some(deadline) => match deadline.checked_duration_since(Instant::now()) {
                    Some(remaining) if !remaining.is_zero() => remaining,
                    _ => return *count,
                },
                None => Duration::from_secs(3600),
            };
            count = self
                .poked
                .wait_timeout(count, remaining)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    pub fn count(&self) -> u64 {
        *self.count.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
