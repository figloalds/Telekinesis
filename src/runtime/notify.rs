//! Namespace change notifications for the mounted filesystem.
use super::Shared;
use crate::core::*;
use std::collections::{BTreeMap, BTreeSet};

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
    let a = visible_paths(before);
    let b = visible_paths(after);
    let mut out = vec![];
    for (path, old) in &a {
        match b.get(path) {
            None => out.push((path.clone(), 2)),
            Some(new) if old.id != new.id => {
                out.push((path.clone(), 2));
                out.push((path.clone(), 1));
            }
            Some(new)
                if old.content != new.content
                    || old.modified_ms != new.modified_ms
                    || old.basic_info() != new.basic_info() =>
            {
                out.push((path.clone(), 3))
            }
            _ => {}
        }
    }
    for path in b.keys() {
        if !a.contains_key(path) {
            out.push((path.clone(), 1));
        }
    }
    out
}
pub fn deliver_notifications(engine: &Shared) {
    #[cfg(windows)]
    {
        let pending = {
            let Some(mut e) = super::try_lock_engine(engine) else {
                return;
            };
            if !e.mounted || e.switching {
                return;
            }
            std::mem::take(&mut e.notifications)
        };
        if pending.is_empty() {
            return;
        }
        let mut retry = vec![];
        let mut error = None;
        for (path, action) in pending {
            if let Err(e) = crate::mount::notify(&path, action) {
                error = Some(format!("{e:#}"));
                retry.push((path, action));
            }
        }
        let mut e = super::lock_engine(engine);
        e.notifications.extend(retry);
        e.notification_error = error;
    }
    #[cfg(target_os = "linux")]
    {
        // The Linux adapter uses zero entry/attribute TTLs and direct file I/O.
        if let Some(mut e) = super::try_lock_engine(engine) {
            e.notifications.clear();
            e.notification_error = None;
        }
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = engine;
    }
}
