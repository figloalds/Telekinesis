//! Exponential backoff with a cap, symmetric jitter and a healthy-period reset.
//!
//! The nominal delay of attempt `n` (1-based) is `min(cap, base · 2^(n-1))`.
//! A sampled delay lies in `[nominal · (1 - jitter), min(cap, nominal · (1 + jitter))]`.
//! `success(now)` starts (or continues) a healthy period; once it has lasted
//! `reset_after`, the attempt counter returns to zero (checked on the next
//! `success` or `failure`).
use rand::Rng;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct Backoff {
    pub base: Duration,
    pub cap: Duration,
    /// Fraction in `[0, 1]`; 0.2 means ±20 %. A value set directly is
    /// sanitized when used (NaN or infinite → 0, else clamped to `[0, 1]`).
    pub jitter: f64,
    pub reset_after: Duration,
    attempt: u32,
    healthy_since: Option<Instant>,
}

impl Backoff {
    pub fn new(base: Duration, cap: Duration, jitter: f64, reset_after: Duration) -> Self {
        Self {
            base,
            cap: cap.max(base),
            jitter: sanitize_jitter(jitter),
            reset_after,
            attempt: 0,
            healthy_since: None,
        }
    }

    /// Worker restarts (§3.3): 1 s → 300 s, ±20 %, reset after 10 min running.
    pub fn worker_restart() -> Self {
        Self::new(
            Duration::from_secs(1),
            Duration::from_secs(300),
            0.2,
            Duration::from_secs(600),
        )
    }

    /// Peer lanes and connections (§3.7, §8): 1 s → 60 s, ±20 %, reset after 30 s Ready.
    pub fn lane() -> Self {
        Self::new(
            Duration::from_secs(1),
            Duration::from_secs(60),
            0.2,
            Duration::from_secs(30),
        )
    }

    /// Generic Transient class (§8): 1 s → 60 s, ±20 %, reset on success.
    pub fn transient() -> Self {
        Self::new(
            Duration::from_secs(1),
            Duration::from_secs(60),
            0.2,
            Duration::ZERO,
        )
    }

    /// Contention class (§8): 50 ms → 1 s, ±20 %, reset on success.
    pub fn contention() -> Self {
        Self::new(
            Duration::from_millis(50),
            Duration::from_millis(1000),
            0.2,
            Duration::ZERO,
        )
    }

    /// Mount attach retries (§3.3): 2 s → 120 s, ±20 %, reset on success.
    pub fn mount_retry() -> Self {
        Self::new(
            Duration::from_secs(2),
            Duration::from_secs(120),
            0.2,
            Duration::ZERO,
        )
    }

    /// Inbox retries (§4.4): 1 s → 5 min, ±20 %, reset on success.
    pub fn inbox() -> Self {
        Self::new(
            Duration::from_secs(1),
            Duration::from_secs(300),
            0.2,
            Duration::ZERO,
        )
    }

    /// Per-object retries (§8): 30 s → 1 h, ±20 %, reset on success.
    pub fn objects() -> Self {
        Self::new(
            Duration::from_secs(30),
            Duration::from_secs(3600),
            0.2,
            Duration::ZERO,
        )
    }

    /// Records a failure at `now` and returns how long to wait before retrying.
    pub fn failure(&mut self, now: Instant) -> Duration {
        if self.healthy_long_enough(now) {
            self.attempt = 0;
        }
        self.healthy_since = None;
        self.attempt = self.attempt.saturating_add(1);
        sample(self.nominal(self.attempt), self.cap, self.jitter)
    }

    /// Records a success at `now`. With `reset_after == 0` the counter resets
    /// immediately; otherwise once the healthy period has lasted `reset_after`.
    pub fn success(&mut self, now: Instant) {
        if self.healthy_since.is_none() {
            self.healthy_since = Some(now);
        }
        if self.healthy_long_enough(now) {
            self.attempt = 0;
        }
    }

    /// Consecutive failures since the last reset.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
        self.healthy_since = None;
    }

    /// The un-jittered delay for a 1-based attempt number (0 is treated as 1).
    pub fn nominal(&self, attempt: u32) -> Duration {
        let exponent = attempt.saturating_sub(1).min(63);
        let nanos = self
            .base
            .as_nanos()
            .saturating_mul(1u128 << exponent)
            .min(self.cap.as_nanos());
        let secs = u64::try_from(nanos / 1_000_000_000).unwrap_or(u64::MAX);
        Duration::new(secs, (nanos % 1_000_000_000) as u32)
    }

    /// Inclusive bounds of a sampled delay for a 1-based attempt number.
    pub fn bounds(&self, attempt: u32) -> (Duration, Duration) {
        let nominal = self.nominal(attempt);
        let jitter = sanitize_jitter(self.jitter);
        (
            scale(nominal, 1.0 - jitter, self.cap),
            scale(nominal, 1.0 + jitter, self.cap),
        )
    }

    fn healthy_long_enough(&self, now: Instant) -> bool {
        self.healthy_since
            .is_some_and(|since| now.saturating_duration_since(since) >= self.reset_after)
    }
}

/// `jitter` as a usable fraction: NaN or infinite → 0, else clamped to
/// `[0, 1]` (CCR-WP0.INT-4 item 6: the field is public, so `new` alone
/// cannot guarantee it, and `gen_range` panics on a NaN or empty range).
fn sanitize_jitter(jitter: f64) -> f64 {
    if jitter.is_finite() {
        jitter.clamp(0.0, 1.0)
    } else {
        0.0
    }
}

fn sample(nominal: Duration, cap: Duration, jitter: f64) -> Duration {
    let jitter = sanitize_jitter(jitter);
    if jitter <= 0.0 || nominal.is_zero() {
        return nominal;
    }
    let factor = rand::thread_rng().gen_range((1.0 - jitter)..=(1.0 + jitter));
    scale(nominal, factor, cap)
}

fn scale(nominal: Duration, factor: f64, cap: Duration) -> Duration {
    Duration::try_from_secs_f64(nominal.as_secs_f64() * factor)
        .unwrap_or(cap)
        .min(cap)
}
