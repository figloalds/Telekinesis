//! On-demand content hydration and verified object fetches.
//!
//! **Reads of content still downloading (WP5.2, CCR-WP4.INT-2).** A read of
//! a file whose object is not here registers demand (`request_object`, kept
//! fresh while it waits) and waits while the download progresses: the wait
//! ([`ContentWait`]) ends with the verified object, or fails when no byte
//! arrived for `stall`, or when the whole wait reached `bound` (below the
//! adapter's own request timeout), or when the mount is being detached. It
//! used to wait 15 s for the whole object whatever the progress, which a
//! 512 MiB download never met. Progress is the length of the object's spool
//! (`incoming-objects/<hash>`, written durably by the receiving lane), read
//! without any lock. Only verified bytes are ever served: the wait ends when
//! the installed object hashes to its id. A failure is `CONTENT_UNAVAILABLE`,
//! which the adapters report as a device that is not ready (WinFsp) or an
//! I/O error (FUSE), never as an invalid argument.
//!
//! **The first write to a large file (WP5.2, CCR-WP4.INT-5).**
//! [`prepare_writable`] makes the writable copy of a referenced object larger
//! than the in-memory limit without the engine lock (it used to be copied
//! whole under it, about 2 s per 256 MiB): the copy is verified while it is
//! made and installed only if the session still references that object.
use super::Shared;
use crate::core::*;
use anyhow::{Result, ensure};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

/// How long a read may wait for content that is downloading.
#[derive(Clone, Copy)]
pub struct ContentWait {
    /// Fail when no byte of the object arrived for this long.
    pub stall: Duration,
    /// Fail when the whole wait took this long (below the adapter's request
    /// timeout).
    pub bound: Duration,
    /// At most this many reads wait at once (`None`: no limit); a read that
    /// finds every slot taken fails at once instead of occupying an adapter
    /// thread (the mount's other operations never run out of threads).
    pub slots: Option<&'static WaitSlots>,
    /// The wait ends (`CONTENT_UNAVAILABLE`) as soon as this is true: a
    /// detach or checkout fenced the mount.
    pub cancelled: fn() -> bool,
}

impl ContentWait {
    /// A wait bounded by `timeout` alone (no progress rule, no slot limit).
    pub fn fixed(timeout: Duration) -> Self {
        Self {
            stall: timeout,
            bound: timeout,
            slots: None,
            cancelled: never,
        }
    }
}

fn never() -> bool {
    false
}

/// A bound on concurrent content waits ([`ContentWait::slots`]).
pub struct WaitSlots {
    used: AtomicUsize,
    max: AtomicUsize,
}

impl WaitSlots {
    pub const fn new(max: usize) -> Self {
        Self {
            used: AtomicUsize::new(0),
            max: AtomicUsize::new(max),
        }
    }
    /// Sets the limit (for example from the adapter's thread count).
    pub fn set_max(&self, max: usize) {
        self.max.store(max.max(1), Ordering::Relaxed);
    }
    /// Waits running now.
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }
    fn take(&'static self) -> Option<WaitSlot> {
        let max = self.max.load(Ordering::Relaxed);
        self.used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                (used < max).then_some(used + 1)
            })
            .ok()
            .map(|_| WaitSlot(self))
    }
}

/// One taken [`WaitSlots`] slot, given back on drop.
struct WaitSlot(&'static WaitSlots);

impl Drop for WaitSlot {
    fn drop(&mut self) {
        self.0.used.fetch_sub(1, Ordering::SeqCst);
    }
}

/// How often a waiting read looks for its object.
const WAIT_POLL: Duration = Duration::from_millis(50);
/// How often a waiting read renews its demand.
const DEMAND_REFRESH: Duration = Duration::from_secs(10);

/// Hydrate only the requested handle. Waiting and verification happen outside
/// the engine mutex. All peers see the durable request on their next exchange.
/// Waits at most `timeout` ([`ContentWait::fixed`]).
pub fn prepare_content(engine: &Shared, handle: u64, timeout: Duration) -> Result<()> {
    prepare_content_waiting(engine, handle, ContentWait::fixed(timeout))
}

/// [`prepare_content`] with a progress-based wait (module docs).
pub fn prepare_content_waiting(engine: &Shared, handle: u64, wait: ContentWait) -> Result<()> {
    let started = Instant::now();
    loop {
        let reference = {
            let e = super::lock_engine(engine);
            e.session(handle)?
                .bytes
                .reference()
                .map(|(_, object, size)| (object.to_owned(), size))
        };
        let Some((object, size)) = reference else {
            return Ok(());
        };
        let left = ContentWait {
            bound: wait.bound.saturating_sub(started.elapsed()),
            ..wait
        };
        let file = fetch_verified_waiting(engine, &object, left)?;
        let e = super::lock_engine(engine);
        let session = e.session(handle)?;
        if session
            .bytes
            .reference()
            .is_some_and(|(_, h, n)| h == object && n == size)
        {
            session.bytes.prime(file);
            return Ok(());
        }
        ensure!(
            started.elapsed() < wait.bound,
            "CONTENT_UNAVAILABLE: revision changed during download"
        );
    }
}

/// Makes the writable copy a write (`resize: None`) or a resize to
/// `Some((size, allocation))` through `handle` needs of a referenced object
/// larger than the in-memory limit, without the engine lock (module docs).
/// Call after [`prepare_content`] (the object is then here and verified). A
/// no-op for any other session and for a resize that copies nothing (to
/// zero, or an allocation that does not shrink); a copy that lost a race
/// (the session changed meanwhile) is dropped.
pub fn prepare_writable(engine: &Shared, handle: u64, resize: Option<(u64, bool)>) -> Result<()> {
    let (reference, root) = {
        let e = super::lock_engine(engine);
        let session = e.session(handle)?;
        let copies = match resize {
            None => true,
            Some((size, allocation)) => {
                size != 0 && (!allocation || size < session.bytes.len() as u64)
            }
        };
        (
            session.bytes.large_reference().filter(|_| copies),
            e.store.root.clone(),
        )
    };
    let Some((path, object, size)) = reference else {
        return Ok(());
    };
    crate::util::fault::trigger("engine.writable_copy")?;
    let copy = crate::staging::writable_copy(&root, &path, &object, size)?;
    let mut e = super::lock_engine(engine);
    let session = e.session_mut(handle)?;
    session.bytes.install_writable(&object, size, copy);
    Ok(())
}

/// A verified copy of `object`, opened for reading: the installed file when
/// its bytes hash to `object`; otherwise its download is queued
/// (`request_object`) and the installed file is awaited until `deadline`.
///
/// Corrupt bytes at the object's path are quarantined (CCR-WP3.2-1, applied
/// by WP4.5): renamed aside (`<hash>.corrupt-<id>`) under the object's
/// install lock, after a re-check there (a concurrent download may have
/// repaired the path), and then, with the install lock released, recorded
/// through the store (`Store::quarantine_renamed`: `CORRUPT`, the legacy
/// `object_errors` row, the backoff). The engine lock is never taken under
/// the install lock: the legacy part receiver takes install locks under the
/// engine lock, so the opposite order could deadlock. Before WP4.5 only the
/// rename happened, and the index learned of it from `request_object`'s
/// reconciliation (`MISSING`).
pub fn fetch_verified(
    engine: &Shared,
    object: &str,
    deadline: std::time::Instant,
) -> Result<std::fs::File> {
    let left = deadline.saturating_duration_since(Instant::now());
    fetch_verified_waiting(engine, object, ContentWait::fixed(left))
}

/// [`fetch_verified`] with a progress-based wait (module docs).
pub fn fetch_verified_waiting(
    engine: &Shared,
    object: &str,
    wait: ContentWait,
) -> Result<std::fs::File> {
    let (root, path, size, lock, stopping) = {
        let e = super::lock_engine(engine);
        valid_object(object)?;
        (
            e.store.root.clone(),
            e.store.root.join("objects").join(object),
            e.store.object_size(object)?,
            e.transfers.clone(),
            e.stopping.clone(),
        )
    };
    let spool = root.join("incoming-objects").join(object);
    let started = Instant::now();
    let mut requested: Option<Instant> = None;
    let mut slot = None;
    // The spool's length when it last grew, and when that was.
    let mut progress = (0u64, started);
    loop {
        ensure!(
            !stopping.load(std::sync::atomic::Ordering::Relaxed),
            "WORKER_STOPPING"
        );
        if path.is_file() {
            let mut file = std::fs::File::open(&path)?;
            if file.metadata()?.len() == size && stream_hash(&mut file)? == object {
                return Ok(file);
            }
            drop(file);
            // `Some(length)`: corrupt bytes of that length were renamed aside.
            let renamed = {
                let object_lock = lock.for_object(object);
                let _guard = object_lock.lock().unwrap();
                if path.is_file() {
                    // Recheck after acquiring the install lock: another request
                    // may have repaired the path while this reader verified old
                    // bytes.
                    let mut current = std::fs::File::open(&path)?;
                    let length = current.metadata()?.len();
                    if length == size && stream_hash(&mut current)? == object {
                        return Ok(current);
                    }
                    drop(current);
                    std::fs::rename(&path, path.with_extension(format!("corrupt-{}", id())))?;
                    Some(length)
                } else {
                    None
                }
            };
            if let Some(length) = renamed {
                // The install lock is released: the store call may take the
                // engine lock (the lock order of the module docs).
                let error = anyhow::anyhow!(
                    "CORRUPT_OBJECT: the installed bytes of {object} do not hash to it \
                     ({length} bytes, {size} expected); quarantined by a verified read"
                );
                let recorded = super::lock_engine(engine)
                    .store
                    .quarantine_renamed(object, length, &error);
                if let Err(failed) = recorded {
                    // The rename stands; the index heals at the next read.
                    crate::util::log::event!(warn, CORRUPT_OBJECT, {
                        "site": "worker.content.quarantine",
                        "object": object,
                        "detail": format!("recording the quarantine failed: {failed:#}"),
                    });
                }
            }
        }
        let now = Instant::now();
        if requested.is_none_or(|at| now.duration_since(at) >= DEMAND_REFRESH) {
            // Registered first, refreshed while waiting: the newest demand is
            // served first.
            super::lock_engine(engine).store.request_object(object)?;
            requested = Some(now);
        }
        let received = std::fs::metadata(&spool).map_or(0, |m| m.len());
        if received > progress.0 {
            progress = (received, now);
        }
        let waited = now.duration_since(started);
        ensure!(
            waited < wait.bound && now.duration_since(progress.1) < wait.stall,
            "CONTENT_UNAVAILABLE: {object}; download queued, no verified copy after {} ms \
             ({} of {size} bytes received, the last {} ms ago)",
            waited.as_millis(),
            progress.0,
            now.duration_since(progress.1).as_millis()
        );
        ensure!(
            !(wait.cancelled)(),
            "CONTENT_UNAVAILABLE: {object}; the mount is being detached"
        );
        if slot.is_none()
            && let Some(slots) = wait.slots
        {
            slot = Some(slots.take().ok_or_else(|| {
                anyhow::anyhow!(
                    "CONTENT_UNAVAILABLE: {object}; {} reads already wait for downloads",
                    slots.used()
                )
            })?);
        }
        std::thread::sleep(WAIT_POLL.min(wait.bound.saturating_sub(waited)));
    }
}
