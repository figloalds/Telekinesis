//! Low-overhead cumulative diagnostics. Nested scopes are inclusive, not additive.
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

pub(crate) struct Counter {
    calls: AtomicU64,
    nanos: AtomicU64,
}
impl Counter {
    pub const fn new() -> Self {
        Self {
            calls: AtomicU64::new(0),
            nanos: AtomicU64::new(0),
        }
    }
    pub fn time(&self) -> Timer<'_> {
        Timer {
            counter: self,
            started: Instant::now(),
        }
    }
    pub fn json(&self, stage: &str) -> serde_json::Value {
        serde_json::json!({"stage":stage, "calls":self.calls.load(Ordering::Relaxed),
            "ms":self.nanos.load(Ordering::Relaxed) as f64 / 1e6})
    }
}
pub(crate) struct Timer<'a> {
    counter: &'a Counter,
    started: Instant,
}
impl Drop for Timer<'_> {
    fn drop(&mut self) {
        self.counter.calls.fetch_add(1, Ordering::Relaxed);
        self.counter
            .nanos
            .fetch_add(self.started.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }
}
