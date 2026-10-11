//! Bounded replication pages, proxied by orchestrators to local workers.
use crate::{
    core::*,
    runtime::{Engine, MAX_FRAME},
};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRoute {
    pub peer: String,
    pub repo: String,
    pub local_replica: String,
    pub remote_replica: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishedState {
    pub repo: String,
    pub replica: String,
    pub label: String,
    pub branches: Vec<Branch>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncPage {
    pub format: u32,
    pub repo: String,
    pub sender: String,
    pub receiver: String,
    pub request: String,
    pub reply_to: Option<String>,
    pub known: BTreeSet<String>,
    pub known_objects: BTreeSet<String>,
    pub partial_objects: BTreeMap<String, u64>,
    pub object_parts: BTreeMap<String, ObjectPart>,
    pub bundle: Bundle,
    pub ack: Vec<String>,
    pub pending_events: usize,
}
fn binding(engine: &Engine, route: &SyncRoute) -> Result<()> {
    ensure!(engine.store.repo == route.repo, "REPOSITORY_MISMATCH");
    ensure!(
        engine.store.device == route.local_replica,
        "LOCAL_REPLICA_BINDING_MISMATCH"
    );
    ensure!(
        route.local_replica != route.remote_replica,
        "DUPLICATE_REPLICA_IDENTITY"
    );
    Ok(())
}
pub fn summary(engine: &Engine) -> Result<PublishedState> {
    Ok(PublishedState {
        repo: engine.store.repo.clone(),
        replica: engine.store.device.clone(),
        label: engine.store.repo.clone(),
        branches: engine
            .store
            .branches()?
            .into_iter()
            .filter(|branch| branch.shared)
            .collect(),
    })
}
pub fn offer(engine: &Engine, route: &SyncRoute, inventory: Option<&SyncPage>) -> Result<SyncPage> {
    offer_bounded(engine, route, inventory, MAX_FRAME)
}
pub(crate) fn offer_bounded(
    engine: &Engine,
    route: &SyncRoute,
    inventory: Option<&SyncPage>,
    maximum: usize,
) -> Result<SyncPage> {
    binding(engine, route)?;
    let known = inventory
        .map(|page| Ok(page.known.clone()))
        .unwrap_or_else(|| engine.store.known_by_peer(&route.remote_replica))?;
    let objects = inventory
        .map(|page| Ok(page.known_objects.clone()))
        .unwrap_or_else(|| engine.store.known_objects_by_peer(&route.remote_replica))?;
    let offsets = inventory
        .map(|page| Ok(page.partial_objects.clone()))
        .unwrap_or_else(|| engine.store.peer_partial_objects(&route.remote_replica))?;
    let bundle = engine.store.shared_page(
        &known,
        &objects,
        if maximum == MAX_FRAME {
            MAX_FRAME - 12 * 1024 * 1024
        } else {
            maximum / 4
        },
    )?;
    let parts = engine.store.shared_parts(
        &bundle,
        &objects,
        &offsets,
        if maximum == MAX_FRAME {
            MAX_FRAME - 8 * 1024 * 1024
        } else {
            maximum / 2
        },
    )?;
    Ok(SyncPage {
        format: PEER_FORMAT,
        repo: engine.store.repo.clone(),
        sender: route.local_replica.clone(),
        receiver: route.remote_replica.clone(),
        request: id(),
        reply_to: inventory.map(|page| page.request.clone()),
        known: engine.store.shared_event_inventory()?,
        known_objects: engine.store.shared_object_inventory()?,
        partial_objects: engine.store.partial_objects()?,
        object_parts: parts,
        bundle,
        ack: vec![],
        pending_events: engine.store.db.query_row(
            "SELECT count(*) FROM incoming_events",
            [],
            |row| row.get(0),
        )?,
    })
}
pub fn receive(engine: &mut Engine, route: &SyncRoute, page: &SyncPage) -> Result<Vec<String>> {
    binding(engine, route)?;
    ensure!(page.format == PEER_FORMAT, "INCOMPATIBLE_PEER_PROTOCOL");
    ensure!(
        page.repo == route.repo && page.bundle.repo == route.repo,
        "REPOSITORY_MISMATCH"
    );
    ensure!(
        page.sender == route.remote_replica && page.receiver == route.local_replica,
        "REMOTE_REPLICA_BINDING_MISMATCH"
    );
    uuid::Uuid::parse_str(&page.request).context("INVALID_SYNC_REQUEST_ID")?;
    ensure!(
        page.known.len() <= 100000
            && page.known_objects.len() <= 100000
            && page.partial_objects.len() <= 10000,
        "INVENTORY_TOO_LARGE"
    );
    // Orchestrators relay events from any replica. Retain the immutable local
    // event check so a relayed page cannot rewrite this replica's history.
    let existing: BTreeMap<_, _> = engine
        .store
        .events()?
        .into_iter()
        .map(|event| (event.id.clone(), event))
        .collect();
    for event in &page.bundle.events {
        ensure!(
            event.device != route.local_replica
                || existing.get(&event.id).is_some_and(|known| known == event),
            "FORGED_EVENT_ORIGIN"
        );
    }
    let mut allowed: BTreeSet<_> = page
        .bundle
        .events
        .iter()
        .map(|e| e.device.clone())
        .collect();
    allowed.insert(route.local_replica.clone());
    allowed.insert(route.remote_replica.clone());
    engine.sync_peers.insert(route.remote_replica.clone());
    ensure!(
        page.bundle.branches.iter().all(|branch| branch.shared),
        "PRIVATE_BRANCH_REJECTED"
    );
    engine
        .store
        .receive_parts(page.object_parts.clone(), &page.bundle.events, &allowed)?;
    let ack = engine.receive_peer(page.bundle.clone(), &allowed)?;
    // Advertised inventory means committed durable events, never pending
    // manifests. Discard unknown IDs; private events cannot alter the outbox.
    let shared = engine.store.shared_event_inventory()?;
    let known = page.known.intersection(&shared).cloned().collect();
    engine
        .store
        .remember_peer_inventory(&route.remote_replica, &known)?;
    engine
        .store
        .remember_peer_objects(&route.remote_replica, &page.known_objects)?;
    engine
        .store
        .remember_peer_partials(&route.remote_replica, &page.partial_objects)?;
    Ok(ack)
}
pub fn worker_control(engine: &mut Engine, payload: &Value) -> Result<Value> {
    let op = payload["op"].as_str().context("INVALID_REQUEST")?;
    if op == "published-summary" {
        return Ok(json!(summary(engine)?));
    }
    let route: SyncRoute = serde_json::from_value(payload["route"].clone())?;
    binding(engine, &route)?;
    let maximum = payload["max_frame"].as_u64().unwrap_or(MAX_FRAME as u64) as usize;
    ensure!(
        (1024 * 1024..=MAX_FRAME).contains(&maximum),
        "INVALID_PAGE_BUDGET"
    );
    match op {
        "published-offer" => Ok(json!(offer_bounded(engine, &route, None, maximum)?)),
        "published-exchange" => {
            let page: SyncPage = serde_json::from_value(payload["page"].clone())?;
            ensure!(page.reply_to.is_none(), "INVALID_SYNC_REQUEST");
            let ack = receive(engine, &route, &page)?;
            let mut response = offer_bounded(engine, &route, Some(&page), maximum)?;
            response.ack = ack;
            Ok(json!(response))
        }
        "published-apply" => {
            let page: SyncPage = serde_json::from_value(payload["page"].clone())?;
            ensure!(
                page.reply_to.as_deref() == payload["request"].as_str(),
                "UNBOUND_PEER_RESPONSE"
            );
            let sent: BTreeSet<String> = serde_json::from_value(payload["sent"].clone())?;
            ensure!(
                page.ack
                    .iter()
                    .all(|ack| sent.contains(ack) && page.known.contains(ack)),
                "UNBOUND_OR_EARLY_ACK"
            );
            receive(engine, &route, &page)?;
            engine.store.acknowledge(&page.ack)?;
            let pending: i64 =
                engine
                    .store
                    .db
                    .query_row("SELECT count(*) FROM incoming_events", [], |row| row.get(0))?;
            let status = json!({"caught_up":engine.store.shared_event_inventory()?==page.known&&pending==0&&page.pending_events==0&&engine.store.outgoing_unacknowledged()?==0&&engine.health.is_none()&&!engine.sessions.values().any(|session|session.dirty),"peer":route.peer,"replica":route.remote_replica,"durability":"peer SQLite FULL + verified objects","at":time()});
            engine.last_sync = Some(status.clone());
            Ok(status)
        }
        _ => bail!("UNKNOWN_PUBLISHED_OPERATION"),
    }
}
