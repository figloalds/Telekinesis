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
            let e = engine.lock().unwrap();
            e.session(handle)?
                .bytes
                .reference()
                .map(|(_, object, size)| (object.to_owned(), size))
        };
        let Some((object, size)) = reference else {
            return Ok(());
        };
        let file = fetch_verified(engine, &object, deadline)?;
        let e = engine.lock().unwrap();
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
pub(super) fn fetch_verified(
    engine: &Shared,
    object: &str,
    deadline: std::time::Instant,
) -> Result<std::fs::File> {
    let (path, size, lock, stopping) = {
        let e = engine.lock().unwrap();
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
            let object_lock = lock.for_object(object);
            let _guard = object_lock.lock().unwrap();
            if path.is_file() {
                // Recheck after acquiring the install lock: another request may
                // have repaired the path while this reader verified old bytes.
                let mut current = std::fs::File::open(&path)?;
                if current.metadata()?.len() == size && stream_hash(&mut current)? == object {
                    return Ok(current);
                }
                drop(current);
                std::fs::rename(&path, path.with_extension(format!("corrupt-{}", id())))?;
            }
        }
        if !queued {
            engine.lock().unwrap().store.request_object(object)?;
            queued = true;
        }
        ensure!(
            std::time::Instant::now() < deadline,
            "CONTENT_UNAVAILABLE: {object}; download queued, no verified copy available before deadline"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}
