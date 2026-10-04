//! Owner-authenticated worker bridge. The network service supplies a locally
//! persisted grant; remote callers never select runtime paths or management ops.
use crate::{
    core::*,
    pairing::{PublishedState, SyncConfiguration},
    runtime::{Engine, MAX_FRAME},
};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

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
fn binding(engine: &Engine, grant: &SyncConfiguration) -> Result<()> {
    ensure!(engine.store.repo == grant.repo, "REPOSITORY_MISMATCH");
    ensure!(
        engine.store.device == grant.local_replica,
        "LOCAL_REPLICA_BINDING_MISMATCH"
    );
    ensure!(
        grant.local_replica != grant.remote_replica,
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
pub fn offer(
    engine: &Engine,
    grant: &SyncConfiguration,
    inventory: Option<&SyncPage>,
) -> Result<SyncPage> {
    offer_bounded(engine, grant, inventory, MAX_FRAME)
}
pub(crate) fn offer_bounded(
    engine: &Engine,
    grant: &SyncConfiguration,
    inventory: Option<&SyncPage>,
    maximum: usize,
) -> Result<SyncPage> {
    binding(engine, grant)?;
    let known = inventory
        .map(|page| Ok(page.known.clone()))
        .unwrap_or_else(|| engine.store.known_by_peer(&grant.remote_replica))?;
    let objects = inventory
        .map(|page| Ok(page.known_objects.clone()))
        .unwrap_or_else(|| engine.store.known_objects_by_peer(&grant.remote_replica))?;
    let offsets = inventory
        .map(|page| Ok(page.partial_objects.clone()))
        .unwrap_or_else(|| engine.store.peer_partial_objects(&grant.remote_replica))?;
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
        sender: grant.local_replica.clone(),
        receiver: grant.remote_replica.clone(),
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
pub fn receive(
    engine: &mut Engine,
    grant: &SyncConfiguration,
    page: &SyncPage,
) -> Result<Vec<String>> {
    binding(engine, grant)?;
    ensure!(page.format == PEER_FORMAT, "INCOMPATIBLE_PEER_PROTOCOL");
    ensure!(
        page.repo == grant.repo && page.bundle.repo == grant.repo,
        "REPOSITORY_MISMATCH"
    );
    ensure!(
        page.sender == grant.remote_replica && page.receiver == grant.local_replica,
        "REMOTE_REPLICA_BINDING_MISMATCH"
    );
    uuid::Uuid::parse_str(&page.request).context("INVALID_SYNC_REQUEST_ID")?;
    ensure!(
        page.known.len() <= 100000
            && page.known_objects.len() <= 100000
            && page.partial_objects.len() <= 10000,
        "INVENTORY_TOO_LARGE"
    );
    // A peer may echo known local events, but may never invent events authored
    // by this replica. Validate before CAS ingestion or journal staging.
    let existing: BTreeMap<_, _> = engine
        .store
        .events()?
        .into_iter()
        .map(|event| (event.id.clone(), event))
        .collect();
    for event in &page.bundle.events {
        ensure!(
            event.device == grant.remote_replica
                || (event.device == grant.local_replica
                    && existing.get(&event.id).is_some_and(|known| known == event)),
            "FORGED_EVENT_ORIGIN"
        );
    }
    let allowed = BTreeSet::from([grant.local_replica.clone(), grant.remote_replica.clone()]);
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
        .remember_peer_inventory(&grant.remote_replica, &known)?;
    engine
        .store
        .remember_peer_objects(&grant.remote_replica, &page.known_objects)?;
    engine
        .store
        .remember_peer_partials(&grant.remote_replica, &page.partial_objects)?;
    Ok(ack)
}
pub fn worker_control(engine: &mut Engine, payload: &Value) -> Result<Value> {
    let op = payload["op"].as_str().context("INVALID_REQUEST")?;
    if op == "published-summary" {
        return Ok(json!(summary(engine)?));
    }
    let grant: SyncConfiguration = serde_json::from_value(payload["grant"].clone())?;
    binding(engine, &grant)?;
    let maximum = payload["max_frame"].as_u64().unwrap_or(MAX_FRAME as u64) as usize;
    ensure!(
        (1024 * 1024..=MAX_FRAME).contains(&maximum),
        "INVALID_PAGE_BUDGET"
    );
    match op {
        "published-offer" => Ok(json!(offer_bounded(engine, &grant, None, maximum)?)),
        "published-exchange" => {
            let page: SyncPage = serde_json::from_value(payload["page"].clone())?;
            ensure!(page.reply_to.is_none(), "INVALID_SYNC_REQUEST");
            let ack = receive(engine, &grant, &page)?;
            let mut response = offer_bounded(engine, &grant, Some(&page), maximum)?;
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
            receive(engine, &grant, &page)?;
            engine.store.acknowledge(&page.ack)?;
            let pending: i64 =
                engine
                    .store
                    .db
                    .query_row("SELECT count(*) FROM incoming_events", [], |row| row.get(0))?;
            let status = json!({"caught_up":engine.store.shared_event_inventory()?==page.known&&pending==0&&page.pending_events==0&&engine.store.outgoing_unacknowledged()?==0&&engine.health.is_none()&&!engine.sessions.values().any(|session|session.dirty),"peer":grant.peer,"replica":grant.remote_replica,"durability":"peer SQLite FULL + verified objects","at":time()});
            engine.last_sync = Some(status.clone());
            Ok(status)
        }
        _ => bail!("UNKNOWN_PUBLISHED_OPERATION"),
    }
}
