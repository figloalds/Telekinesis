//! Lock helpers: a process-wide poison policy, poison-tolerant locking, and
//! `Tracked<T>`, a mutex that measures how long it is held (§2.2 rule 2).
//!
//! * [`lock`] never panics on poison: shared state guarded this way must stay
//!   valid after any panic (orchestrator maps, status, registries).
//! * [`lock_or_exit`] is for state that may be inconsistent after a panic (the
//!   worker engine): under [`PoisonPolicy::Exit`] it runs the hook on the
//!   recovered value (for example `retain_all_dirty`), logs a fatal event and
//!   exits with code 70; under [`PoisonPolicy::Panic`] (the default, used by
//!   tests) it panics.
//! * [`Tracked`] records every hold longer than its limit (50 ms by default)
//!   in a process-wide ring that tests and diagnostics read.
use std::{
    collections::VecDeque,
    fmt,
    ops::{Deref, DerefMut},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Mutex, MutexGuard, PoisonError,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

/// Exit code used by [`lock_or_exit`] (sysexits `EX_SOFTWARE`).
pub const POISON_EXIT_CODE: i32 = crate::model::ipc::EXIT_INTERNAL;
/// Default hold limit of a [`Tracked`] lock.
pub const TRACKED_HOLD_LIMIT: Duration = Duration::from_millis(50);
/// How many hold violations the process-wide ring keeps.
pub const HOLD_VIOLATIONS_KEPT: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PoisonPolicy {
    /// Panic on a poisoned `lock_or_exit` (tests; the default).
    Panic,
    /// Run the hook, log `LOCK_POISONED` at fatal level, `exit(70)` (workers).
    Exit,
}

static POLICY: AtomicU8 = AtomicU8::new(0);

pub fn set_poison_policy(policy: PoisonPolicy) {
    POLICY.store(
        match policy {
            PoisonPolicy::Panic => 0,
            PoisonPolicy::Exit => 1,
        },
        Ordering::SeqCst,
    );
}

pub fn poison_policy() -> PoisonPolicy {
    match POLICY.load(Ordering::SeqCst) {
        1 => PoisonPolicy::Exit,
        _ => PoisonPolicy::Panic,
    }
}

/// Poison-tolerant lock: a poisoned mutex yields its inner guard.
pub fn lock<T: ?Sized>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Locks `mutex`; on poison follows the process [`PoisonPolicy`].
///
/// Under `Exit`: `into_inner`, then `on_poison(&mut value)` (a panicking hook is
/// contained), then a fatal `LOCK_POISONED` log line, then `exit(70)`.
pub fn lock_or_exit<'a, T: ?Sized>(
    mutex: &'a Mutex<T>,
    on_poison: &dyn Fn(&mut T),
) -> MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => match poison_policy() {
            PoisonPolicy::Panic => {
                panic!("LOCK_POISONED: a thread panicked while holding this lock")
            }
            PoisonPolicy::Exit => {
                let mut guard = poisoned.into_inner();
                let hook = catch_unwind(AssertUnwindSafe(|| on_poison(&mut guard)));
                crate::util::log::event!(fatal, LOCK_POISONED, {
                    "type": std::any::type_name::<T>(),
                    "hook_panicked": hook.is_err(),
                    "exit_code": POISON_EXIT_CODE,
                });
                std::process::exit(POISON_EXIT_CODE)
            }
        },
    }
}

/// One hold of a [`Tracked`] lock longer than its limit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HoldViolation {
    pub name: &'static str,
    pub held_ms: u64,
    pub limit_ms: u64,
    pub at_ms: u64,
}

static VIOLATIONS: Mutex<VecDeque<HoldViolation>> = Mutex::new(VecDeque::new());

/// Every recorded violation still in the ring (oldest first).
pub fn hold_violations() -> Vec<HoldViolation> {
    lock(&VIOLATIONS).iter().cloned().collect()
}

/// Drains the ring.
pub fn take_hold_violations() -> Vec<HoldViolation> {
    lock(&VIOLATIONS).drain(..).collect()
}

/// A poison-tolerant mutex that measures every hold. Holds longer than the
/// limit are counted, kept in the process-wide ring and, in debug builds,
/// logged as `LOCK_HOLD_EXCEEDED` (rate limited like every log code).
pub struct Tracked<T> {
    name: &'static str,
    limit: Duration,
    holds: AtomicU64,
    violations: AtomicU64,
    longest_ns: AtomicU64,
    inner: Mutex<T>,
}

impl<T> Tracked<T> {
    pub fn new(name: &'static str, value: T) -> Self {
        Self::with_limit(name, value, TRACKED_HOLD_LIMIT)
    }

    pub fn with_limit(name: &'static str, value: T, limit: Duration) -> Self {
        Self {
            name,
            limit,
            holds: AtomicU64::new(0),
            violations: AtomicU64::new(0),
            longest_ns: AtomicU64::new(0),
            inner: Mutex::new(value),
        }
    }

    pub fn lock(&self) -> TrackedGuard<'_, T> {
        let guard = lock(&self.inner);
        TrackedGuard {
            owner: self,
            since: Instant::now(),
            guard: Some(guard),
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn limit(&self) -> Duration {
        self.limit
    }

    /// Completed holds.
    pub fn holds(&self) -> u64 {
        self.holds.load(Ordering::Relaxed)
    }

    /// Completed holds longer than the limit.
    pub fn violations(&self) -> u64 {
        self.violations.load(Ordering::Relaxed)
    }

    pub fn longest_hold(&self) -> Duration {
        Duration::from_nanos(self.longest_ns.load(Ordering::Relaxed))
    }

    pub fn into_inner(self) -> T {
        self.inner
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn released(&self, held: Duration) {
        self.holds.fetch_add(1, Ordering::Relaxed);
        let nanos = u64::try_from(held.as_nanos()).unwrap_or(u64::MAX);
        self.longest_ns.fetch_max(nanos, Ordering::Relaxed);
        if held <= self.limit {
            return;
        }
        self.violations.fetch_add(1, Ordering::Relaxed);
        let violation = HoldViolation {
            name: self.name,
            held_ms: u64::try_from(held.as_millis()).unwrap_or(u64::MAX),
            limit_ms: u64::try_from(self.limit.as_millis()).unwrap_or(u64::MAX),
            at_ms: crate::util::time::now_ms(),
        };
        if cfg!(debug_assertions) {
            crate::util::log::event!(warn, LOCK_HOLD_EXCEEDED, {
                "lock": violation.name,
                "held_ms": violation.held_ms,
                "limit_ms": violation.limit_ms,
            });
        }
        let mut ring = lock(&VIOLATIONS);
        if ring.len() == HOLD_VIOLATIONS_KEPT {
            ring.pop_front();
        }
        ring.push_back(violation);
    }
}

impl<T: Default> Default for Tracked<T> {
    fn default() -> Self {
        Self::new("unnamed", T::default())
    }
}

impl<T> fmt::Debug for Tracked<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tracked")
            .field("name", &self.name)
            .field("limit", &self.limit)
            .field("holds", &self.holds())
            .field("violations", &self.violations())
            .finish()
    }
}

pub struct TrackedGuard<'a, T> {
    owner: &'a Tracked<T>,
    since: Instant,
    /// `Some` until `drop`, which releases it before reporting.
    guard: Option<MutexGuard<'a, T>>,
}

impl<T> TrackedGuard<'_, T> {
    /// How long this guard has been held so far.
    pub fn held(&self) -> Duration {
        self.since.elapsed()
    }
}

impl<T> Deref for TrackedGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.guard
            .as_deref()
            .expect("a TrackedGuard holds its lock until drop")
    }
}

impl<T> DerefMut for TrackedGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.guard
            .as_deref_mut()
            .expect("a TrackedGuard holds its lock until drop")
    }
}

impl<T> Drop for TrackedGuard<'_, T> {
    fn drop(&mut self) {
        // Measured before the inner guard is released, reported after it
        // (CCR-WP0.INT-4 item 4): the accounting is atomics plus the violation
        // ring and a log line, none of which may run under the tracked lock.
        let held = self.since.elapsed();
        drop(self.guard.take());
        self.owner.released(held);
    }
}
