//! Legacy v6 `published-*` worker operations: bounded metadata exchange plus
//! object parts installed outside the semantic engine lock.
use super::Shared;
use crate::core::*;
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

/// Metadata commits and acknowledgements never wait for object installation.
/// Bulk hashing/fsync uses a separate connection and lock; filesystem metadata
/// and control-lane calls remain able to acquire the semantic engine.
pub(super) fn published_rpc(engine: &Shared, payload: &Value) -> Result<Value> {
    let mut metadata = payload.clone();
    // Bound semantic work per control exchange; queued events remain durable
    // and unacknowledged until later slices activate them.
    metadata["bounded_metadata"] = json!(payload["metadata_only"] == true);
    let mut parts = BTreeMap::new();
    if let Some(page) = metadata.get_mut("page") {
        let originals: crate::paired_sync::SyncPage = serde_json::from_value(page.clone())?;
        if payload["metadata_only"] == true {
            ensure!(
                originals.object_parts.is_empty() && originals.bundle.objects.is_empty(),
                "BULK_ON_CONTROL_LANE"
            );
        }
        parts = originals.object_parts;
        for (object, bytes) in originals.bundle.objects {
            let total = hex::decode(&bytes)?.len() as u64;
            parts.insert(
                object,
                ObjectPart {
                    offset: 0,
                    total,
                    hex: bytes,
                },
            );
        }
        page["object_parts"] = json!({});
        page["bundle"]["objects"] = json!({});
    }
    if parts.is_empty() {
        let mut e = super::try_lock_engine(engine).ok_or_else(|| anyhow::anyhow!("WORKER_BUSY"))?;
        return crate::paired_sync::worker_control(&mut e, &metadata);
    }
    let (mut result, transfer, lock) = {
        let mut e = super::try_lock_engine(engine).ok_or_else(|| anyhow::anyhow!("WORKER_BUSY"))?;
        let result = crate::paired_sync::worker_control(&mut e, &metadata)?;
        (result, e.store.transfer_view()?, e.transfers.clone())
    };
    if !parts.is_empty() {
        let objects: Vec<_> = parts.keys().cloned().collect();
        let deadline = std::time::Instant::now() + crate::bulk_sync::RECEIVE_BUDGET;
        let received =
            transfer.receive_parts_until(parts, &[], &BTreeSet::new(), Some(&lock), Some(deadline));
        let outcomes = match received {
            Ok(outcomes) => outcomes,
            Err(error) => {
                // Page-level failure: nothing was received.
                let e = super::lock_engine(engine);
                for object in objects {
                    e.store.db.execute("INSERT INTO object_errors VALUES(?,?,?) ON CONFLICT(hash) DO UPDATE SET error=excluded.error,at=excluded.at", rusqlite::params![object, format!("{error:#}"), time()])?;
                }
                return Err(error);
            }
        };
        let (completed, failed) = crate::core::transfer::split_outcomes(&outcomes);
        {
            // Completed objects are registered first, even when another part
            // failed; then each failed object gets its error and backoff.
            let e = super::lock_engine(engine);
            e.store.register_received(&completed)?;
            for (object, error) in &failed {
                e.store.db.execute("INSERT INTO object_errors VALUES(?,?,?) ON CONFLICT(hash) DO UPDATE SET error=excluded.error,at=excluded.at", rusqlite::params![object, error.message, time()])?;
                e.store.record_object_failure(object, error)?;
            }
        }
        if let Some((_, error)) = failed.into_iter().next() {
            return Err(error.into_anyhow());
        }
        if result.get("known_objects").is_some() {
            result["known_objects"] = json!(transfer.shared_object_inventory()?);
            result["partial_objects"] = json!(transfer.partial_objects()?);
        } else {
            result["content"] = transfer.content_status()?;
        }
    }
    Ok(result)
}
