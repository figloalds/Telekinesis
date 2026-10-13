//! Shared immutable state with change signalling (REVAMP_PLAN §2.2 rule 2).
//!
//! Readers take an `Arc<T>` and never hold a lock while they use it. Writers
//! replace the whole value and bump a generation counter; waiters block on a
//! condvar until the generation passes the one they last saw.
use std::{
    fmt,
    sync::{Arc, Condvar, Mutex, PoisonError, RwLock},
    time::{Duration, Instant},
};

/// `RwLock<Arc<T>>` plus a generation counter and a condvar.
///
/// The generation starts at 0 and increases by exactly one per `store`/`update`.
/// A value observed after `wait_newer` returned `g` is at least as new as `g`.
pub struct Snapshot<T> {
    value: RwLock<Arc<T>>,
    generation: Mutex<u64>,
    changed: Condvar,
}

impl<T> Snapshot<T> {
    pub fn new(value: T) -> Self {
        Self::from_arc(Arc::new(value))
    }

    pub fn from_arc(value: Arc<T>) -> Self {
        Self {
            value: RwLock::new(value),
            generation: Mutex::new(0),
            changed: Condvar::new(),
        }
    }

    /// The current value. Never blocks behind a waiter; blocks only for the
    /// duration of a concurrent `store` pointer swap or `update` closure.
    pub fn load(&self) -> Arc<T> {
        self.value
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The current value together with the generation it was published at.
    pub fn load_versioned(&self) -> (Arc<T>, u64) {
        let value = self.value.read().unwrap_or_else(PoisonError::into_inner);
        let generation = *self
            .generation
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        (value.clone(), generation)
    }

    /// Replaces the value and returns the new generation.
    pub fn store(&self, value: T) -> u64 {
        self.store_arc(Arc::new(value))
    }

    /// Replaces the value with an existing `Arc` and returns the new generation.
    pub fn store_arc(&self, value: Arc<T>) -> u64 {
        let mut slot = self.value.write().unwrap_or_else(PoisonError::into_inner);
        *slot = value;
        self.bump_locked()
    }

    /// Atomically derives the next value from the current one (the closure runs
    /// under the write lock, so it must be short) and returns the new generation.
    pub fn update(&self, next: impl FnOnce(&T) -> T) -> u64 {
        let mut slot = self.value.write().unwrap_or_else(PoisonError::into_inner);
        let value = next(&slot);
        *slot = Arc::new(value);
        self.bump_locked()
    }

    pub fn generation(&self) -> u64 {
        *self
            .generation
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Blocks until the generation is greater than `generation` or `timeout`
    /// elapses, and returns the generation current at that moment (which equals
    /// the argument on timeout when nothing changed).
    pub fn wait_newer(&self, generation: u64, timeout: Duration) -> u64 {
        let deadline = Instant::now().checked_add(timeout);
        let mut current = self
            .generation
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        loop {
            if *current > generation {
                return *current;
            }
            let remaining = match deadline {
                Some(deadline) => match deadline.checked_duration_since(Instant::now()) {
                    Some(remaining) if !remaining.is_zero() => remaining,
                    _ => return *current,
                },
                None => Duration::from_secs(3600),
            };
            current = self
                .changed
                .wait_timeout(current, remaining)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    // Called with the value write lock held, so (value, generation) pairs read
    // under the read lock are always consistent.
    fn bump_locked(&self) -> u64 {
        let mut generation = self
            .generation
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *generation += 1;
        let published = *generation;
        drop(generation);
        self.changed.notify_all();
        published
    }
}

impl<T: Default> Default for Snapshot<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: fmt::Debug> fmt::Debug for Snapshot<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (value, generation) = self.load_versioned();
        f.debug_struct("Snapshot")
            .field("generation", &generation)
            .field("value", &value)
            .finish()
    }
}
