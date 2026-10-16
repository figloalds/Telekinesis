//! On-demand content hydration and verified object fetches.
use super::Shared;
use crate::core::*;
use anyhow::{Result, ensure};
use std::time::Duration;

/// Hydrate only the requested handle. Waiting and verification happen outside
/// the engine mutex. All peers see the durable request on their next exchange.
pub fn prepare_content(engine: &Shared, handle: u64, timeout: Duration) -> Result<()> {
    let deadline = std::time::Instant::now() + timeout;
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
        let file = fetch_verified(engine, &object, deadline)?;
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
            std::time::Instant::now() < deadline,
            "CONTENT_UNAVAILABLE: revision changed during download"
        );
    }
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
    let (path, size, lock, stopping) = {
        let e = super::lock_engine(engine);
        valid_object(object)?;
        (
            e.store.root.join("objects").join(object),
            e.store.object_size(object)?,
            e.transfers.clone(),
            e.stopping.clone(),
        )
    };
    let mut queued = false;
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
        if !queued {
            super::lock_engine(engine).store.request_object(object)?;
            queued = true;
        }
        ensure!(
            std::time::Instant::now() < deadline,
            "CONTENT_UNAVAILABLE: {object}; download queued, no verified copy available before deadline"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}
