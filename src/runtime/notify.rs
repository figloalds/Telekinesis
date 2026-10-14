//! Namespace change notifications for the mounted filesystem.
//!
//! WP2.3 task 7 (§2.5, §4.4): notifications leave the RPC path. Producers
//! queue them in the engine's [`NotifyQueue`] (bounded at
//! [`NOTIFY_QUEUE_PATHS`] distinct `(path, action)` pairs, duplicates
//! coalesced; an overflow replaces the queue with one root invalidation),
//! and the `notifier` thread ([`spawn_notifier`], Windows) delivers them
//! outside the engine lock, only while the mount is attached. While detached
//! it drops what is queued and remembers to invalidate the root on the next
//! attach (`needs_refresh`). Control requests still append to
//! `Engine::notifications`; the notifier moves those into the queue.
//!
//! After an activation, [`delta_notifications`] computes the notifications of
//! the activated entities only (plus the descendants of directories whose
//! location changed), from the `before` projection and the store's current
//! view: the same `(path, action)` pairs [`namespace_notifications`] yields
//! for those paths, without building every visible path of both projections.
use super::{Shared, ctx::WorkerCtx};
use crate::core::*;
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::{Arc, Condvar, Mutex, PoisonError, atomic::Ordering},
    thread::{self, JoinHandle},
    time::Duration,
};

/// Distinct `(path, action)` pairs the queue holds before it overflows into
/// one root invalidation.
pub const NOTIFY_QUEUE_PATHS: usize = 4096;
/// WinFsp path of the volume root.
pub const ROOT_PATH: &str = "\\";
/// `FILE_ACTION_ADDED`, `FILE_ACTION_REMOVED`, `FILE_ACTION_MODIFIED`.
pub const ACTION_ADDED: u32 = 1;
pub const ACTION_REMOVED: u32 = 2;
pub const ACTION_MODIFIED: u32 = 3;
/// The notifier's wait between deliveries when nothing is queued.
const NOTIFIER_TICK: Duration = Duration::from_millis(250);
/// How long the notifier lets a burst accumulate before delivering it.
const NOTIFIER_COALESCE: Duration = Duration::from_millis(20);
/// The pause before retrying notifications refused during a mount transition.
const NOTIFIER_RETRY: Duration = Duration::from_millis(100);

pub(super) fn visible_paths(p: &Projection) -> BTreeMap<String, Entry> {
    let mut result = BTreeMap::new();
    for e in p.entries.values().filter(|e| e.alive) {
        let mut parts = vec![e.name.clone()];
        let mut parent = e.parent.clone();
        let mut seen = BTreeSet::new();
        while parent != ROOT && seen.insert(parent.clone()) {
            let Some(dir) = p.entries.get(&parent) else {
                break;
            };
            parts.push(dir.name.clone());
            parent = dir.parent.clone();
        }
        parts.reverse();
        result.insert(format!("\\{}", parts.join("\\")), e.clone());
    }
    result
}
pub fn namespace_notifications(before: &Projection, after: &Projection) -> Vec<(String, u32)> {
    diff_paths(&visible_paths(before), &visible_paths(after))
}

/// The notifications between two `path → entry` maps (the rules of
/// [`namespace_notifications`]).
fn diff_paths(a: &BTreeMap<String, Entry>, b: &BTreeMap<String, Entry>) -> Vec<(String, u32)> {
    let mut out = vec![];
    for (path, old) in a {
        match b.get(path) {
            None => out.push((path.clone(), ACTION_REMOVED)),
            Some(new) if old.id != new.id => {
                out.push((path.clone(), ACTION_REMOVED));
                out.push((path.clone(), ACTION_ADDED));
            }
            Some(new)
                if old.content != new.content
                    || old.modified_ms != new.modified_ms
                    || old.basic_info() != new.basic_info() =>
            {
                out.push((path.clone(), ACTION_MODIFIED))
            }
            _ => {}
        }
    }
    for path in b.keys() {
        if !a.contains_key(path) {
            out.push((path.clone(), ACTION_ADDED));
        }
    }
    out
}

/// The visible path of `entity` given an entry lookup, or `None` when it or
/// an ancestor is not alive (a cycle or a missing ancestor ends the walk, as
/// in `visible_paths`).
fn path_of(entity: &str, lookup: &mut dyn FnMut(&str) -> Option<Entry>) -> Option<(String, Entry)> {
    let entry = lookup(entity).filter(|e| e.alive)?;
    let mut parts = vec![entry.name.clone()];
    let mut parent = entry.parent.clone();
    let mut seen = BTreeSet::new();
    while parent != ROOT && seen.insert(parent.clone()) {
        let Some(dir) = lookup(&parent) else {
            break;
        };
        parts.push(dir.name.clone());
        parent = dir.parent.clone();
    }
    parts.reverse();
    Some((format!("\\{}", parts.join("\\")), entry))
}

/// The notifications of an activation that changed `entities`: their paths
/// before (`before`) and now (the store's view), plus, when the namespace
/// changed, the descendants of every directory among them (whose paths move
/// with it). Restricted to those entities, the result equals
/// [`namespace_notifications`] over the whole projections.
pub fn delta_notifications(
    before: &Projection,
    store: &Store,
    entities: &BTreeSet<String>,
    namespace_changed: bool,
) -> Vec<(String, u32)> {
    let mut cache: BTreeMap<String, Option<Entry>> = BTreeMap::new();
    let mut now = |id: &str| -> Option<Entry> {
        cache
            .entry(id.to_owned())
            .or_insert_with(|| store.entry(id).ok())
            .clone()
    };
    let mut affected: BTreeSet<String> = entities.clone();
    if namespace_changed {
        // Directories among the entities, before or now: their descendants'
        // paths change with them.
        let directories: BTreeSet<String> = entities
            .iter()
            .filter(|id| {
                before
                    .entries
                    .get(*id)
                    .is_some_and(|e| e.kind == "directory")
                    || now(id).is_some_and(|e| e.kind == "directory")
            })
            .cloned()
            .collect();
        if !directories.is_empty() {
            for (id, entry) in &before.entries {
                let mut parent = entry.parent.clone();
                let mut seen = BTreeSet::new();
                while parent != ROOT && seen.insert(parent.clone()) {
                    if directories.contains(&parent) {
                        affected.insert(id.clone());
                        break;
                    }
                    let Some(dir) = before.entries.get(&parent) else {
                        break;
                    };
                    parent = dir.parent.clone();
                }
            }
            let mut pending: Vec<String> = directories.into_iter().collect();
            let mut visited = BTreeSet::new();
            while let Some(dir) = pending.pop() {
                if !visited.insert(dir.clone()) {
                    continue;
                }
                for child in store.children(&dir).unwrap_or_default() {
                    affected.insert(child.id.clone());
                    if child.kind == "directory" {
                        pending.push(child.id);
                    }
                }
            }
        }
    }
    let mut old = BTreeMap::new();
    let mut new = BTreeMap::new();
    let mut then = |id: &str| before.entries.get(id).cloned();
    for id in &affected {
        if let Some((path, entry)) = path_of(id, &mut then) {
            old.insert(path, entry);
        }
        if let Some((path, entry)) = path_of(id, &mut now) {
            new.insert(path, entry);
        }
    }
    diff_paths(&old, &new)
}

/// The bounded, coalescing notification queue (module docs).
#[derive(Debug, Default)]
pub struct NotifyQueue {
    state: Mutex<QueueState>,
    wake: Condvar,
}

#[derive(Debug, Default)]
struct QueueState {
    items: Vec<(String, u32)>,
    index: HashSet<(String, u32)>,
    /// More than `NOTIFY_QUEUE_PATHS` distinct pairs were queued: deliver one
    /// root invalidation instead.
    overflow: bool,
    /// Queued notifications were dropped while detached: invalidate the root
    /// once attached again.
    needs_refresh: bool,
    error: Option<String>,
    overflows: u64,
    delivered: u64,
}

/// What [`NotifyQueue::take`] hands to the deliverer.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct NotifyBatch {
    pub items: Vec<(String, u32)>,
}

impl NotifyQueue {
    fn state(&self) -> std::sync::MutexGuard<'_, QueueState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
    /// Queues `items`, coalescing duplicates; past [`NOTIFY_QUEUE_PATHS`]
    /// distinct pairs the queue becomes one root invalidation.
    pub fn push(&self, items: impl IntoIterator<Item = (String, u32)>) {
        let mut state = self.state();
        let mut added = false;
        for item in items {
            if state.overflow {
                break;
            }
            if state.index.contains(&item) {
                continue;
            }
            if state.items.len() >= NOTIFY_QUEUE_PATHS {
                state.items.clear();
                state.index.clear();
                state.overflow = true;
                state.overflows += 1;
                added = true;
                break;
            }
            state.index.insert(item.clone());
            state.items.push(item);
            added = true;
        }
        drop(state);
        if added {
            self.wake.notify_all();
        }
    }
    /// Queued pairs (a pending root invalidation counts as one).
    pub fn len(&self) -> usize {
        let state = self.state();
        state.items.len() + usize::from(state.overflow || state.needs_refresh)
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Times the queue overflowed into a root invalidation.
    pub fn overflows(&self) -> u64 {
        self.state().overflows
    }
    /// Notifications delivered so far.
    pub fn delivered(&self) -> u64 {
        self.state().delivered
    }
    /// The last delivery failure, if any.
    pub fn error(&self) -> Option<String> {
        self.state().error.clone()
    }
    /// The mount is detached: drop everything queued and invalidate the root
    /// on the next attach.
    pub fn detached(&self) {
        let mut state = self.state();
        if !state.items.is_empty() || state.overflow {
            state.needs_refresh = true;
        }
        state.items.clear();
        state.index.clear();
        state.overflow = false;
    }
    /// Takes everything queued for delivery: one root invalidation after an
    /// overflow or a detach, else the queued pairs in order.
    pub fn take(&self) -> NotifyBatch {
        let mut state = self.state();
        let root = std::mem::take(&mut state.overflow) | std::mem::take(&mut state.needs_refresh);
        let mut items = std::mem::take(&mut state.items);
        state.index.clear();
        if root {
            items = vec![(ROOT_PATH.to_owned(), ACTION_MODIFIED)];
        }
        NotifyBatch { items }
    }
    /// Puts back pairs whose delivery must be retried (a mount transition
    /// was in progress), ahead of anything queued since.
    fn requeue(&self, items: Vec<(String, u32)>) {
        let mut state = self.state();
        let queued = std::mem::take(&mut state.items);
        state.index.clear();
        for item in items.into_iter().chain(queued) {
            if state.items.len() >= NOTIFY_QUEUE_PATHS {
                state.items.clear();
                state.index.clear();
                state.overflow = true;
                state.overflows += 1;
                break;
            }
            if state.index.insert(item.clone()) {
                state.items.push(item);
            }
        }
    }
    fn delivered_batch(&self, delivered: u64, error: Option<String>) {
        let mut state = self.state();
        state.delivered += delivered;
        if error.is_some() || delivered > 0 {
            state.error = error;
        }
    }
    /// Waits until something is queued or `timeout` elapses.
    pub fn wait(&self, timeout: Duration) {
        let state = self.state();
        if !state.items.is_empty() || state.overflow {
            return;
        }
        let _ = self
            .wake
            .wait_timeout(state, timeout)
            .unwrap_or_else(PoisonError::into_inner);
    }
}

/// One delivery round for `engine`: moves the control requests'
/// `Engine::notifications` into the queue, then delivers the queue outside
/// the engine lock while `attached` (Windows); detached, drops it (root
/// invalidation on the next attach). Linux and other platforms drop
/// everything (FUSE serves with zero TTLs). Never waits for the engine.
pub fn deliver(engine: &Shared, queue: &NotifyQueue, attached: bool) {
    if let Some(mut e) = super::try_lock_engine(engine) {
        let legacy = std::mem::take(&mut e.notifications);
        let mounted = e.mounted && !e.switching;
        e.notification_error = None;
        drop(e);
        if attached && mounted {
            queue.push(legacy);
        }
    }
    if !(cfg!(windows) && attached) {
        queue.detached();
        if !cfg!(windows) {
            // Nothing is ever delivered here: no refresh is owed either.
            let _ = queue.take();
        }
        return;
    }
    let batch = queue.take();
    if batch.items.is_empty() {
        return;
    }
    let mut retry = vec![];
    let mut delivered = 0u64;
    let mut error = None;
    for (path, action) in batch.items {
        match notify_native(&path, action) {
            Ok(()) => delivered += 1,
            Err(failure) => {
                let message = format!("{failure:#}");
                if crate::model::errors::code_in(&message) == Some("BUSY_VIEW") {
                    // A mount transition is in progress: retried next round.
                    retry.push((path, action));
                } else {
                    error = Some(message);
                }
            }
        }
    }
    if let Some(message) = &error {
        crate::util::log::event!(warn, NOTIFICATION_FAILED, {
            "site": "worker.notifier",
            "message": message,
        });
    }
    if !retry.is_empty() {
        queue.requeue(retry);
    }
    queue.delivered_batch(delivered, error);
}

#[cfg(windows)]
fn notify_native(path: &str, action: u32) -> anyhow::Result<()> {
    crate::mount::notify(path, action)
}
#[cfg(not(windows))]
fn notify_native(_path: &str, _action: u32) -> anyhow::Result<()> {
    Ok(())
}

/// Delivers pending notifications once, synchronously (the legacy entry
/// point; the notifier thread calls [`deliver`] in a loop).
pub fn deliver_notifications(engine: &Shared) {
    let Some(e) = super::try_lock_engine(engine) else {
        return;
    };
    let queue = e.notify_queue.clone();
    let attached = e.mounted && !e.switching;
    drop(e);
    deliver(engine, &queue, attached);
}

/// Starts the `notifier` thread (§2.5): delivers the engine's queue while
/// the mount is attached, drops it while detached, until the worker stops.
/// On platforms without notifications it only keeps the queues empty.
pub fn spawn_notifier(ctx: Arc<WorkerCtx>) -> std::io::Result<JoinHandle<()>> {
    let queue = super::lock_engine(&ctx.engine).notify_queue.clone();
    thread::Builder::new()
        .name("notifier".into())
        .spawn(move || {
            while !ctx.stopping.load(Ordering::Relaxed) {
                queue.wait(NOTIFIER_TICK);
                // Coalesce a burst (one activation slice queues many paths).
                thread::sleep(NOTIFIER_COALESCE);
                let attached = ctx.mount.status().state.is_attached();
                deliver(&ctx.engine, &queue, attached);
                if !queue.is_empty() {
                    // Retries wait for a mount transition: do not spin.
                    thread::sleep(NOTIFIER_RETRY);
                }
            }
        })
}
