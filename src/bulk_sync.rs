//! Independent bounded object lanes. Metadata/causal activation stays on control.
use crate::{
    core::{transfer::split_outcomes, *},
    paired_sync::SyncRoute,
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

pub const PROTOCOL: u32 = 1;
const PAGE_BUDGET: usize = 4 * 1024 * 1024;
/// How long one page may spend installing objects. Well inside the 15 s RPC
/// deadline of the legacy lanes; parts left over are re-sent by the peer.
pub const RECEIVE_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lane {
    pub index: usize,
    pub count: usize,
}
impl Lane {
    pub fn validate(self) -> Result<()> {
        ensure!(
            (1..=15).contains(&self.count) && self.index < self.count,
            "INVALID_BULK_LANE"
        );
        Ok(())
    }
    pub fn owns(self, object: &str) -> bool {
        u64::from_str_radix(object.get(..16).unwrap_or(""), 16)
            .is_ok_and(|hash| self.count > 0 && hash % self.count as u64 == self.index as u64)
    }
}

pub use crate::core::objects::ObjectLocks;

#[derive(Clone)]
pub(crate) struct Worker {
    pub root: PathBuf,
    pub repo: String,
    pub device: String,
    pub locks: Arc<ObjectLocks>,
}
impl Worker {
    pub fn store(&self) -> Result<Store> {
        Store::open_transfer(&self.root, &self.repo, &self.device)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Page {
    pub format: u32,
    pub repo: String,
    pub sender: String,
    pub receiver: String,
    pub request: String,
    pub reply_to: Option<String>,
    pub lane: Lane,
    pub known_objects: BTreeSet<String>,
    pub partial_objects: BTreeMap<String, u64>,
    pub wanted_objects: BTreeSet<String>,
    pub object_parts: BTreeMap<String, ObjectPart>,
}
impl Page {
    pub fn object_bytes(&self) -> usize {
        self.object_parts
            .values()
            .map(|part| part.hex.len() / 2)
            .sum()
    }
    pub fn inventory(mut self) -> Self {
        self.object_parts.clear();
        self
    }
    fn validate(&self, route: &SyncRoute, lane: Lane) -> Result<()> {
        lane.validate()?;
        ensure!(
            self.format == PROTOCOL && self.lane == lane,
            "INCOMPATIBLE_BULK_LANE"
        );
        ensure!(
            self.repo == route.repo
                && self.sender == route.remote_replica
                && self.receiver == route.local_replica,
            "BULK_ROUTE_MISMATCH"
        );
        uuid::Uuid::parse_str(&self.request)?;
        if let Some(reply) = &self.reply_to {
            uuid::Uuid::parse_str(reply)?;
        }
        ensure!(
            self.known_objects.len() <= 100000
                && self.partial_objects.len() <= 10000
                && self.wanted_objects.len() <= 100000
                && self.object_parts.len() <= 10000,
            "INVENTORY_TOO_LARGE"
        );
        for object in self
            .known_objects
            .iter()
            .chain(self.partial_objects.keys())
            .chain(self.wanted_objects.iter())
            .chain(self.object_parts.keys())
        {
            valid_object(object)?;
            ensure!(lane.owns(object), "OBJECT_ON_WRONG_LANE");
        }
        ensure!(
            self.partial_objects
                .values()
                .all(|offset| *offset <= MAX_CONTENT),
            "INVALID_OBJECT_OFFSET"
        );
        ensure!(
            self.object_bytes() <= PAGE_BUDGET / 2,
            "BULK_PAGE_TOO_LARGE"
        );
        for part in self.object_parts.values() {
            ensure!(
                part.total <= MAX_CONTENT
                    && part.hex.len() <= 8 * IO_CHUNK
                    && part.hex.len() % 2 == 0
                    && part.hex.bytes().all(|byte| byte.is_ascii_hexdigit())
                    && (!part.hex.is_empty() || part.total == 0 && part.offset == 0)
                    && part
                        .offset
                        .checked_add((part.hex.len() / 2) as u64)
                        .is_some_and(|end| end <= part.total),
                "INVALID_OBJECT_PART"
            );
        }
        Ok(())
    }
}

fn offer(store: &Store, route: &SyncRoute, lane: Lane, inventory: Option<&Page>) -> Result<Page> {
    // One consistent committed metadata view, without the semantic engine lock.
    store.db.execute_batch("BEGIN")?;
    let mut failures = vec![];
    let result = (|| {
        let mut parts = BTreeMap::new();
        if let Some(inventory) = inventory {
            // Objects of shared events the peer has committed (an index join).
            let permitted = store
                .objects_of_events_known_by(&route.remote_replica)?
                .into_iter()
                .filter(|hash| lane.owns(hash))
                .collect();
            let empty = Bundle {
                repo: store.repo.clone(),
                branches: vec![],
                events: vec![],
                objects: BTreeMap::new(),
            };
            // At most `MAX_PAGE_PARTS` parts with exact overhead accounting;
            // objects in their failure backoff are skipped.
            parts = store.transfer_parts_collect(
                &empty,
                &inventory.known_objects,
                &inventory.partial_objects,
                PAGE_BUDGET,
                &permitted,
                &inventory.wanted_objects,
                &mut failures,
            )?;
        }
        Ok(Page {
            format: PROTOCOL,
            repo: store.repo.clone(),
            sender: route.local_replica.clone(),
            receiver: route.remote_replica.clone(),
            request: id(),
            reply_to: inventory.map(|page| page.request.clone()),
            lane,
            known_objects: store
                .shared_object_inventory()?
                .into_iter()
                .filter(|hash| lane.owns(hash))
                .collect(),
            partial_objects: store
                .partial_objects()?
                .into_iter()
                .filter(|(hash, _)| lane.owns(hash))
                .collect(),
            wanted_objects: store
                .wanted_objects()?
                .into_iter()
                .filter(|hash| lane.owns(hash))
                .collect(),
            object_parts: parts,
        })
    })();
    store.db.execute_batch("ROLLBACK")?; // End the read snapshot before any later write.
    // Unreadable objects heal the index and back off (never fail the page).
    store.note_send_failures(&failures);
    result
}

/// Receive a page's parts object by object. Completed objects are registered
/// even when a later part fails; each failure records its object backoff.
/// The page still answers with the first failure (wire behaviour unchanged).
fn receive(worker: &Worker, store: &Store, page: &Page) -> Result<()> {
    let outcomes = store.receive_parts_until(
        page.object_parts.clone(),
        &[],
        &BTreeSet::new(),
        Some(&worker.locks),
        Some(std::time::Instant::now() + RECEIVE_BUDGET),
    )?;
    let (completed, failed) = split_outcomes(&outcomes);
    if !completed.is_empty() || !failed.is_empty() {
        // Acquire SQLite's writer slot before descriptor reads, avoiding a WAL
        // snapshot-upgrade race with concurrent causal commits/other bulk lanes.
        store.db.execute_batch("BEGIN IMMEDIATE")?;
        let registered = (|| {
            store.register_received(&completed)?;
            for (object, error) in &failed {
                store.record_object_failure(object, error)?;
            }
            Ok::<_, anyhow::Error>(())
        })();
        match registered {
            Ok(()) => store.db.execute_batch("COMMIT")?,
            Err(error) => {
                let _ = store.db.execute_batch("ROLLBACK");
                return Err(error);
            }
        }
    }
    match failed.into_iter().next() {
        Some((_, error)) => Err(error.into_anyhow()),
        None => Ok(()),
    }
}

pub(crate) fn worker_control(worker: &Worker, payload: &Value) -> Result<Value> {
    let route: SyncRoute = serde_json::from_value(payload["route"].clone())?;
    let lane: Lane = serde_json::from_value(payload["lane"].clone())?;
    lane.validate()?;
    ensure!(
        route.repo == worker.repo
            && route.local_replica == worker.device
            && route.remote_replica != worker.device,
        "BULK_ROUTE_MISMATCH"
    );
    let store = worker.store()?;
    match payload["op"].as_str() {
        Some("published-bulk-offer") => {
            let inventory: Option<Page> = serde_json::from_value(payload["inventory"].clone())?;
            if let Some(page) = &inventory {
                page.validate(&route, lane)?;
            }
            let mut page = offer(&store, &route, lane, inventory.as_ref())?;
            page.reply_to = None;
            Ok(json!(page))
        }
        Some(op @ ("published-bulk-exchange" | "published-bulk-apply")) => {
            let page: Page = serde_json::from_value(payload["page"].clone())?;
            page.validate(&route, lane)?;
            if op == "published-bulk-exchange" {
                ensure!(page.reply_to.is_none(), "INVALID_SYNC_REQUEST");
            } else {
                let request = payload["request"]
                    .as_str()
                    .context("SYNC_REQUEST_REQUIRED")?;
                uuid::Uuid::parse_str(request)?;
                ensure!(
                    page.reply_to.as_deref() == Some(request),
                    "UNBOUND_PEER_RESPONSE"
                );
            }
            receive(worker, &store, &page)?;
            if op == "published-bulk-exchange" {
                Ok(json!(offer(&store, &route, lane, Some(&page))?))
            } else {
                Ok(json!({"received_object_bytes":page.object_bytes()}))
            }
        }
        _ => anyhow::bail!("UNKNOWN_BULK_OPERATION"),
    }
}
