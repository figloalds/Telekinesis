//! Cooperative cancellation (invariant 29: waits stay interruptible).
//!
//! A `Cancel` is a cheap cloneable handle. `sleep` returns early on cancel;
//! `on_cancel` hooks ("killers") run exactly once, on the cancelling thread,
//! for example to shut down a socket a blocked reader is parked on. `child()`
//! derives a token that is cancelled with its parent but can also be
//! cancelled on its own.
use std::{
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Condvar, Mutex, PoisonError, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

type Hook = Box<dyn Fn() + Send + Sync>;

#[derive(Clone, Default)]
pub struct Cancel {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    cancelled: AtomicBool,
    state: Mutex<State>,
    changed: Condvar,
}

#[derive(Default)]
struct State {
    hooks: Vec<Hook>,
    children: Vec<Weak<Inner>>,
}

impl Cancel {
    pub fn new() -> Self {
        Self::default()
    }

    /// Cancels this token and its children. The first call runs every
    /// registered hook (a panicking hook does not stop the others); later
    /// calls do nothing.
    pub fn cancel(&self) {
        Inner::cancel(&self.inner);
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    /// Sleeps for `duration` unless cancelled first. Returns `true` when the
    /// token is (or becomes) cancelled, `false` when the full duration passed.
    pub fn sleep(&self, duration: Duration) -> bool {
        let deadline = Instant::now().checked_add(duration);
        let mut state = self.lock();
        loop {
            if self.is_cancelled() {
                return true;
            }
            let remaining = match deadline {
                Some(deadline) => match deadline.checked_duration_since(Instant::now()) {
                    Some(remaining) if !remaining.is_zero() => remaining,
                    _ => return false,
                },
                None => Duration::from_secs(3600),
            };
            state = self
                .inner
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    /// Blocks until cancelled.
    pub fn wait(&self) {
        while !self.sleep(Duration::from_secs(3600)) {}
    }

    /// Registers a hook that runs once when the token is cancelled. If it is
    /// already cancelled, the hook runs immediately on the calling thread.
    pub fn on_cancel(&self, hook: Hook) {
        let mut state = self.lock();
        if !self.is_cancelled() {
            state.hooks.push(hook);
            return;
        }
        drop(state);
        run_hook(&hook);
    }

    /// A token cancelled together with this one; cancelling the child does not
    /// cancel the parent.
    pub fn child(&self) -> Cancel {
        let child = Cancel::new();
        let mut state = self.lock();
        if self.is_cancelled() {
            drop(state);
            child.cancel();
            return child;
        }
        state.children.retain(|weak| weak.strong_count() > 0);
        state.children.push(Arc::downgrade(&child.inner));
        child
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl Inner {
    fn cancel(this: &Arc<Inner>) {
        if this.cancelled.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut state = this.state.lock().unwrap_or_else(PoisonError::into_inner);
        let hooks = std::mem::take(&mut state.hooks);
        let children = std::mem::take(&mut state.children);
        this.changed.notify_all();
        drop(state);
        for hook in &hooks {
            run_hook(hook);
        }
        for child in children.iter().filter_map(Weak::upgrade) {
            Inner::cancel(&child);
        }
    }
}

fn run_hook(hook: &Hook) {
    let _ = catch_unwind(AssertUnwindSafe(hook));
}

impl fmt::Debug for Cancel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cancel")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}
