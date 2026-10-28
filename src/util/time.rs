//! Clocks. Components take `Arc<dyn Clock>` so state machines with timers
//! (backoff, breakers, stall detection) are testable with [`ManualClock`].
use std::{
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub trait Clock: Send + Sync {
    /// Wall-clock milliseconds since the Unix epoch.
    fn now_ms(&self) -> u64;
    /// A monotonic instant for deadlines and durations.
    fn instant(&self) -> Instant;
}

/// Wall-clock milliseconds since the Unix epoch (0 if the clock is before it).
///
/// Test hook: debug builds return `TKFS_FAKE_NOW_MS` when it parses as `u64`
/// (read once per process). Release builds never read it.
pub fn now_ms() -> u64 {
    if cfg!(debug_assertions) {
        static FAKE: std::sync::OnceLock<Option<u64>> = std::sync::OnceLock::new();
        if let Some(fake) = *FAKE.get_or_init(|| {
            std::env::var("TKFS_FAKE_NOW_MS")
                .ok()
                .and_then(|value| value.trim().parse().ok())
        }) {
            return fake;
        }
    }
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        now_ms()
    }
    fn instant(&self) -> Instant {
        Instant::now()
    }
}

/// A clock that only moves when told to. Both readings advance together.
#[derive(Debug)]
pub struct ManualClock {
    start_ms: u64,
    start: Instant,
    offset: Mutex<Duration>,
}

impl ManualClock {
    pub fn new(start_ms: u64) -> Self {
        Self {
            start_ms,
            start: Instant::now(),
            offset: Mutex::new(Duration::ZERO),
        }
    }

    pub fn advance(&self, by: Duration) {
        let mut offset = crate::util::lock::lock(&self.offset);
        *offset = offset.saturating_add(by);
    }

    /// Time advanced since construction.
    pub fn elapsed(&self) -> Duration {
        *crate::util::lock::lock(&self.offset)
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new(1_790_000_000_000)
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        let offset = u64::try_from(self.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.start_ms.saturating_add(offset)
    }
    fn instant(&self) -> Instant {
        self.start + self.elapsed()
    }
}
