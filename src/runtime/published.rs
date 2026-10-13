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
        let mut e = engine
            .try_lock()
            .map_err(|_| anyhow::anyhow!("WORKER_BUSY"))?;
        return crate::paired_sync::worker_control(&mut e, &metadata);
    }
    let (mut result, transfer, lock) = {
        let mut e = engine
            .try_lock()
            .map_err(|_| anyhow::anyhow!("WORKER_BUSY"))?;
        let result = crate::paired_sync::worker_control(&mut e, &metadata)?;
        (result, e.store.transfer_view()?, e.transfers.clone())
    };
    if !parts.is_empty() {
        let objects: Vec<_> = parts.keys().cloned().collect();
        let received =
            transfer.receive_parts_data_locked(parts, &[], &BTreeSet::new(), Some(&lock));
        let completed = match received {
            Ok(completed) => completed,
            Err(error) => {
                let e = engine.lock().unwrap();
                for object in objects {
                    e.store.db.execute("INSERT INTO object_errors VALUES(?,?,?) ON CONFLICT(hash) DO UPDATE SET error=excluded.error,at=excluded.at", rusqlite::params![object, format!("{error:#}"), time()])?;
                }
                return Err(error);
            }
        };
        engine.lock().unwrap().store.register_received(&completed)?;
        if result.get("known_objects").is_some() {
            result["known_objects"] = json!(transfer.shared_object_inventory()?);
            result["partial_objects"] = json!(transfer.partial_objects()?);
        } else {
            result["content"] = transfer.content_status()?;
        }
    }
    Ok(result)
}
