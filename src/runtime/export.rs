//! Export of authorized shared data to an explicit local test bucket.
use super::{Discovery, Shared};
use crate::core::*;
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{collections::BTreeSet, path::Path};

pub(super) fn export_bucket(engine: &Shared, payload: &Value) -> Result<Value> {
    use crate::backend::{DirectoryBackend, ObjectBackend};
    let requested = std::path::absolute(Path::new(
        payload["directory"].as_str().context("MISSING_DIRECTORY")?,
    ))?;
    let (state, mount) = {
        let e = super::lock_engine(engine);
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
    // Capture only authorized immutable data under the lock; backend I/O outside.
    let (repo, branches, events, objects) = {
        let e = super::lock_engine(engine);
        let events: Vec<_> = e
            .store
            .events()?
            .into_iter()
            .filter(|e| e.branch.shared)
            .collect();
        let objects: BTreeSet<_> = events.iter().flat_map(Event::objects).collect();
        (
            e.store.repo.clone(),
            e.store
                .branches()?
                .into_iter()
                .filter(|b| b.shared)
                .collect::<Vec<_>>(),
            events,
            objects,
        )
    };
    for object in &objects {
        let mut file = {
            let e = super::lock_engine(engine);
            e.store.open_object(object)?
        };
        ensure!(
            backend.put_stream(&mut file)? == *object,
            "BACKEND_HASH_MISMATCH"
        );
    }
    let manifest = json!({"format":"tkfs-shared-export-1","repo":repo,"branches":branches,"events":events,"objects":objects});
    let hash = backend.put_verified(&serde_json::to_vec(&manifest)?)?;
    Ok(
        json!({"backend":"local-directory-test-bucket","manifest":hash,"objects":objects.len(),"remote_validated":false}),
    )
}
