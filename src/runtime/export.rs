//! Export of authorized shared data to an explicit local test bucket.
use super::{Discovery, ctx::WorkerCtx};
use crate::content::ReadContext;
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::{
    path::Path,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

static EXPORTS: super::content::WaitSlots = super::content::WaitSlots::new(2);

pub(super) fn export_bucket(
    worker: &WorkerCtx,
    payload: &Value,
    deadline: Instant,
    cancelled: &dyn Fn() -> bool,
) -> Result<Value> {
    use crate::backend::DirectoryBackend;
    let _slot = EXPORTS.take().context("CONTENT_RESOURCE_LIMIT")?;
    let context = ReadContext {
        deadline: deadline.min(Instant::now() + Duration::from_secs(240)),
        cancel: crate::util::lock::lock(&worker.inflight_objects)
            .legacy_cancel
            .child(),
    };
    let interrupted = || cancelled() || worker.stopping.load(Ordering::Acquire);
    ensure!(!interrupted(), "CONTENT_CANCELLED");
    context.check()?;
    let requested = std::path::absolute(Path::new(
        payload["directory"].as_str().context("MISSING_DIRECTORY")?,
    ))?;
    let (state, mount) = {
        let e = super::lock_engine(&worker.engine);
        let info: Discovery = serde_json::from_slice(&e.discovery)?;
        (e.store.root.clone(), info.mount)
    };
    let normalized = |p: &std::path::Path| {
        p.to_string_lossy()
            .trim_start_matches("\\\\?\\")
            .replace('/', "\\")
            .trim_end_matches('\\')
            .to_uppercase()
    };
    let overlaps = |a: &std::path::Path, b: &std::path::Path| {
        let a = normalized(a);
        let b = normalized(b);
        a == b || a.starts_with(&(b.clone() + "\\")) || b.starts_with(&(a + "\\"))
    };
    ensure!(
        !overlaps(&requested, &state),
        "BUCKET_MUST_BE_OUTSIDE_STATE"
    );
    if let Some(mount) = &mount {
        ensure!(
            !overlaps(&requested, Path::new(mount)),
            "BUCKET_MUST_BE_OUTSIDE_MOUNT"
        );
    }
    let mut ancestor = requested.as_path();
    let mut tail = vec![];
    while !ancestor.exists() {
        tail.push(
            ancestor
                .file_name()
                .context("INVALID_DIRECTORY")?
                .to_owned(),
        );
        ancestor = ancestor.parent().context("INVALID_DIRECTORY")?;
    }
    let mut destination =
        std::fs::canonicalize(ancestor).context("BUCKET_REQUIRES_NATIVE_DIRECTORY")?;
    for name in tail.into_iter().rev() {
        destination.push(name);
    }
    let state = std::fs::canonicalize(state)?;
    ensure!(
        !destination.starts_with(&state) && !state.starts_with(&destination),
        "BUCKET_MUST_BE_OUTSIDE_STATE"
    );
    if let Some(mount) = mount {
        let mount = Path::new(&mount);
        let mount = std::fs::canonicalize(mount.parent().context("FOLDER_MOUNT_REQUIRED")?)?
            .join(mount.file_name().context("FOLDER_MOUNT_REQUIRED")?);
        ensure!(
            !destination.starts_with(&mount) && !mount.starts_with(&destination),
            "BUCKET_MUST_BE_OUTSIDE_MOUNT"
        );
    }
    let backend = DirectoryBackend::new(&destination)?;
    let store = worker.sync_stores.checkout_nowait()?;
    let export = store.prepare_shared_export(&context, &interrupted)?;
    drop(store);
    export.write(&backend, &context, &interrupted)
}
