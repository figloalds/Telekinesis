//! Cooperative cancellation (invariant 29: waits stay interruptible).
//!
//! A `Cancel` is a cheap cloneable handle. `sleep` returns early on cancel;
//! `on_cancel` hooks ("killers") run exactly once, on the cancelling thread,
//! for example to shut down a socket a blocked reader is parked on. `child()`
//! derives a token that is cancelled with its parent but can also be
//! cancelled on its own.
//!
//! Cancelling a token cancels its children (and their hooks run) **before**
//! its own hooks run, so a parent's hook never observes a live child.
//!
//! Hooks cannot be deregistered: they live as long as the token they were
//! registered on. Register per-connection or per-request killers on a
//! `child()` token that is dropped when that connection or request ends
//! (a parent keeps only a weak reference to its children), never directly on
//! a long-lived token, where they would accumulate. A hook should not hold a
//! clone of the token it is registered on (that is a reference cycle).
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

    /// Cancels this token and its children. The first call cancels the
    /// children first, then runs every hook registered on this token (a
    /// panicking hook does not stop the others); later calls do nothing.
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
    /// The hook is kept until the token is dropped: register short-lived
    /// killers on a `child()` token (see the module docs).
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
        self.attach_child(&child);
        child
    }

    /// A child of both parents. Either parent cancels it; cancelling the child
    /// affects neither parent. Parents retain only weak links, without hooks
    /// that accumulate across completed requests.
    pub fn linked(&self, other: &Cancel) -> Cancel {
        let child = self.child();
        other.attach_child(&child);
        child
    }

    fn attach_child(&self, child: &Cancel) {
        let mut state = self.lock();
        if self.is_cancelled() {
            drop(state);
            child.cancel();
            return;
        }
        state.children.retain(|weak| weak.strong_count() > 0);
        state.children.push(Arc::downgrade(&child.inner));
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
        // Children first (CCR-WP0.INT-4 item 3): a hook of this token may
        // rely on everything derived from it being cancelled already.
        for child in children.iter().filter_map(Weak::upgrade) {
            Inner::cancel(&child);
        }
        for hook in &hooks {
            run_hook(hook);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Barrier, thread};

    #[test]
    fn linked_cancellation_has_two_weak_parents_and_independent_siblings() {
        for first in [true, false] {
            let a = Cancel::new();
            let b = Cancel::new();
            let sibling = a.child();
            let child = a.linked(&b);
            let grandchild = child.child();
            if first {
                a.cancel();
            } else {
                b.cancel();
            }
            assert!(child.is_cancelled() && grandchild.is_cancelled());
            assert_eq!(sibling.is_cancelled(), first);
            assert_eq!(a.is_cancelled(), first);
            assert_eq!(b.is_cancelled(), !first);
        }
        let a = Cancel::new();
        let b = Cancel::new();
        a.linked(&b).cancel();
        assert!(!a.is_cancelled() && !b.is_cancelled());
        b.cancel();
        assert!(a.linked(&b).is_cancelled());
        assert!(b.linked(&a).is_cancelled());
        let parent = Cancel::new();
        let child = parent.linked(&parent);
        parent.cancel();
        assert!(child.is_cancelled());
    }

    #[test]
    fn completed_linked_requests_leave_no_hooks_or_strong_parent_links() {
        let a = Cancel::new();
        let b = Cancel::new();
        for _ in 0..4096 {
            drop(a.linked(&b));
        }
        for parent in [&a, &b] {
            let state = parent.lock();
            assert!(state.hooks.is_empty());
            assert_eq!(state.children.len(), 1);
            assert_eq!(state.children[0].strong_count(), 0);
            assert_eq!(Arc::strong_count(&parent.inner), 1);
        }
    }

    #[test]
    fn linking_races_cancellation_without_missing_either_parent() {
        for index in 0..64 {
            let a = Cancel::new();
            let b = Cancel::new();
            let barrier = Arc::new(Barrier::new(2));
            let parent = if index % 2 == 0 { a.clone() } else { b.clone() };
            let start = barrier.clone();
            let cancelling = thread::spawn(move || {
                start.wait();
                parent.cancel();
            });
            barrier.wait();
            let child = a.linked(&b);
            cancelling.join().unwrap();
            assert!(child.is_cancelled());
        }
    }
}
