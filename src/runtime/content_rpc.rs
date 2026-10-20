//! Content-v1 loopback adapter. No semantic engine lock spans these calls.
//! Offers are bounded, expiring and bound to their exact route/run/token.
use super::ctx::{PooledStore, WorkerCtx};
use crate::{
    content::{
        LocalContent, ReadContext,
        scheduler::{ContentScheduler, DemandLease},
        work::{ContentTransfer, TransferSelection, prepare_transfer_slice},
    },
    model::{content_wire as wire, errors::ErrorWire, workspace::Demand},
    util::{Cancel, lock::lock},
};
use anyhow::{Result, ensure};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use uuid::Uuid;

struct Pending {
    offer: wire::Offer,
    route: crate::paired_sync::SyncRoute,
    owner: Uuid,
    until: Instant,
    transfer: ContentTransfer,
}

pub struct ContentWorker {
    pub provider: Arc<LocalContent>,
    pub scheduler: Arc<ContentScheduler>,
    pending: Mutex<BTreeMap<Uuid, Pending>>,
    cancel: Cancel,
}

impl std::fmt::Debug for ContentWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContentWorker")
            .field("pending", &lock(&self.pending).len())
            .finish_non_exhaustive()
    }
}

pub(super) struct Connection {
    store: PooledStore,
    timeout: u64,
}
impl Deref for Connection {
    type Target = crate::core::Store;
    fn deref(&self) -> &Self::Target {
        &self.store
    }
}
impl DerefMut for Connection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.store
    }
}
impl Drop for Connection {
    fn drop(&mut self) {
        let _ = self
            .store
            .db
            .busy_timeout(Duration::from_millis(self.timeout));
    }
}

pub(super) fn connection(ctx: &WorkerCtx) -> Result<Connection> {
    let store = ctx.sync_stores.checkout()?;
    let timeout = store
        .db
        .query_row("PRAGMA busy_timeout", [], |r| r.get(0))?;
    store.db.busy_timeout(Duration::ZERO)?;
    Ok(Connection { store, timeout })
}

pub fn capabilities(store: &crate::core::Store) -> Result<wire::Capabilities> {
    let active = store.db.query_row(
        "SELECT EXISTS(SELECT 1 FROM config WHERE key='fixture:content_v1' AND value='1')",
        [],
        |r| r.get(0),
    )?;
    Ok(wire::Capabilities {
        version: wire::VERSION,
        active,
        frame_bytes: wire::FRAME_BYTES,
    })
}

pub fn state(ctx: &WorkerCtx) -> Result<Arc<ContentWorker>> {
    ensure!(
        !ctx.stopping.load(std::sync::atomic::Ordering::Relaxed),
        "CONTENT_CANCELLED"
    );
    ensure!(
        capabilities(&*connection(ctx)?)?.active,
        "PEER_NEEDS_UPGRADE"
    );
    if let Some(state) = lock(&ctx.inflight_objects).content.clone() {
        return Ok(state);
    }
    // Opening the immutable provider can touch disk; do not hold ctx's lock.
    let created = Arc::new(ContentWorker {
        provider: Arc::new(LocalContent::open(&ctx.root)?),
        scheduler: Arc::new(ContentScheduler::default()),
        pending: Mutex::new(BTreeMap::new()),
        cancel: Cancel::new(),
    });
    Ok(lock(&ctx.inflight_objects)
        .content
        .get_or_insert(created)
        .clone())
}

pub fn demand(ctx: &WorkerCtx, demand: Demand, context: &ReadContext) -> Result<DemandLease> {
    connection(ctx)?.validate_content_demand(&demand)?;
    let state = state(ctx)?;
    state
        .scheduler
        .demand_on(state.provider.identity(), demand, context)
}

pub fn demand_missing(
    ctx: &WorkerCtx,
    demand: Demand,
    key: crate::model::content::ObjectKey,
    context: &ReadContext,
) -> Result<DemandLease> {
    context.check()?;
    let mut store = connection(ctx)?;
    store.validate_content_demand(&demand)?;
    store.observe_content_unavailable(key)?;
    drop(store);
    self::demand(ctx, demand, context)
}

pub fn reap(ctx: &WorkerCtx) {
    let state = lock(&ctx.inflight_objects).content.clone();
    if let Some(state) = state {
        lock(&state.pending).retain(|_, item| item.until > Instant::now());
    }
}

pub fn stop(ctx: &WorkerCtx) {
    lock(&ctx.inflight_objects).legacy_cancel.cancel();
    let state = lock(&ctx.inflight_objects).content.clone();
    if let Some(state) = state {
        state.cancel.cancel();
        lock(&state.pending).clear();
    }
}

fn parse<T: DeserializeOwned>(header: &Value) -> Result<T> {
    serde_json::from_value(header.clone()).map_err(|e| anyhow::anyhow!("INVALID_REQUEST: {e}"))
}

fn context(state: &ContentWorker, ms: u64) -> ReadContext {
    ReadContext {
        deadline: Instant::now() + Duration::from_millis(ms),
        cancel: state.cancel.child(),
    }
}

fn take(state: &ContentWorker, finish: &wire::Finish) -> Result<Pending> {
    let mut pending = lock(&state.pending);
    pending.retain(|_, p| p.until > Instant::now());
    let item = pending
        .get(&finish.token.owner)
        .ok_or_else(|| anyhow::anyhow!("STALE_ADMISSION_GENERATION"))?;
    ensure!(
        item.offer.token == finish.token
            && item.owner == finish.owner
            && item.route == finish.route,
        "UNAUTHORIZED_OBJECT"
    );
    Ok(pending
        .remove(&finish.token.owner)
        .expect("checked under the same lock"))
}

pub fn dispatch(
    ctx: &WorkerCtx,
    op: &str,
    header: &Value,
    body: &[u8],
) -> std::result::Result<(Value, Vec<u8>), ErrorWire> {
    dispatch_inner(ctx, op, header, body).map_err(|e| ErrorWire::from_error(&e))
}

fn dispatch_inner(
    ctx: &WorkerCtx,
    op: &str,
    header: &Value,
    body: &[u8],
) -> Result<(Value, Vec<u8>)> {
    ensure!(body.len() <= wire::FRAME_BYTES, "CONTENT_BUFFER_LIMIT");
    ensure!(op == wire::COMPLETE || body.is_empty(), "INVALID_REQUEST");
    match op {
        wire::LEGACY_READ => {
            super::objects_rpc::legacy_read(ctx, header).map_err(anyhow::Error::new)
        }
        wire::LEGACY_PREPARE => {
            super::objects_rpc::legacy_prepare(ctx, header).map_err(anyhow::Error::new)
        }
        wire::POLL => {
            let req: wire::Poll = parse(header)?;
            super::objects_rpc::bind(ctx, &req.route).map_err(anyhow::Error::new)?;
            ensure!(
                req.sources.len() <= 32
                    && req.sources.iter().all(|s| !s.is_empty() && s.len() <= 128),
                "INVALID_REQUEST"
            );
            let store = connection(ctx)?;
            let active = capabilities(&store)?.active;
            let legacy_demands = Some(
                store
                    .demanded_objects(32)?
                    .into_iter()
                    .map(|(hash, _)| hash)
                    .collect(),
            );
            let state = lock(&ctx.inflight_objects).content.clone();
            let demand_sources = if active {
                state
                    .map(|s| {
                        s.scheduler
                            .eligible_sources(s.provider.identity(), &ctx.repo, &req.sources)
                    })
                    .unwrap_or_default()
            } else {
                vec![]
            };
            Ok((
                serde_json::to_value(wire::Activity {
                    active,
                    demand_sources,
                    legacy_demands,
                })?,
                vec![],
            ))
        }
        wire::CAPS => {
            let req: wire::WorkerCaps = parse(header)?;
            super::objects_rpc::bind(ctx, &req.route).map_err(anyhow::Error::new)?;
            ensure!(req.version == wire::VERSION, "PEER_NEEDS_UPGRADE");
            Ok((
                serde_json::to_value(capabilities(&*connection(ctx)?)?)?,
                vec![],
            ))
        }
        wire::PREPARE => {
            let req: wire::Prepare = parse(header)?;
            super::objects_rpc::bind(ctx, &req.route).map_err(anyhow::Error::new)?;
            let state = state(ctx)?;
            reap(ctx);
            let mut store = connection(ctx)?;
            // Lost notifications cannot block a root that the object consumer
            // has not yet seen. Replay is one bounded SQL page, without bytes.
            let consumed = store.consume_content_admission("objects-v1", 32)?;
            let selection = TransferSelection {
                mode: req.mode,
                lane: req.lane,
            };
            let (transfer, progress) = prepare_transfer_slice(
                &mut store,
                &state.provider,
                &state.scheduler,
                &req.route.remote_replica,
                &selection,
                &context(&state, 100),
            )?;
            let offer = transfer.map(|transfer| {
                let offer = wire::Offer {
                    token: transfer.token(),
                    reference: transfer.item.reference.clone(),
                    generation: transfer.item.generation,
                    edge: transfer.item.edge.clone(),
                    priority: transfer.priority,
                };
                lock(&state.pending).insert(
                    offer.token.owner,
                    Pending {
                        offer: offer.clone(),
                        route: req.route,
                        owner: req.owner,
                        until: Instant::now() + Duration::from_millis(wire::OFFER_MS),
                        transfer,
                    },
                );
                offer
            });
            Ok((
                serde_json::to_value(wire::Prepared {
                    offer,
                    progress: consumed > 0 || progress,
                })?,
                vec![],
            ))
        }
        wire::READ => {
            let req: wire::WorkerRead = parse(header)?;
            super::objects_rpc::bind(ctx, &req.route).map_err(anyhow::Error::new)?;
            let state = state(ctx)?;
            let class = match req.priority {
                crate::model::workspace::DemandPriority::Foreground => "foreground",
                crate::model::workspace::DemandPriority::Structural => "structural",
                crate::model::workspace::DemandPriority::Background => "background",
            };
            crate::util::fault::trigger(&format!("content.read.{class}"))?;
            let mut store = connection(ctx)?;
            let result = store.read_content_frame_prioritized(
                &state.provider,
                &req.position.reference,
                &req.position.edge,
                req.priority,
                &context(&state, 5000),
            );
            if let Err(error) = &result
                && let Some(key) = LocalContent::unavailable_key(error)
            {
                let _ = store.observe_content_unavailable(key);
            }
            let (generation, bytes) = result?;
            ensure!(bytes.len() <= wire::FRAME_BYTES, "CONTENT_BUFFER_LIMIT");
            Ok((
                serde_json::to_value(wire::Frame {
                    generation,
                    bytes: bytes.len(),
                    token: None,
                })?,
                bytes,
            ))
        }
        wire::COMPLETE => {
            let req: wire::Finish = parse(header)?;
            super::objects_rpc::bind(ctx, &req.route).map_err(anyhow::Error::new)?;
            let state = state(ctx)?;
            let item = take(&state, &req)?;
            let mut store = connection(ctx)?;
            let result =
                item.transfer
                    .complete(&mut store, &state.provider, body, &context(&state, 5000));
            if let Err(error) = &result
                && matches!(
                    crate::model::errors::code_of(error).as_str(),
                    "CORRUPT_CONTENT" | "MANIFEST_ROOT_MISMATCH" | "INVALID_MANIFEST_ENCODING"
                )
            {
                store.backoff_content_source(item.offer.edge.key, &req.route.remote_replica)?;
            }
            result?;
            Ok((json!({"installed":true}), vec![]))
        }
        wire::RELEASE => {
            let req: wire::Release = parse(header)?;
            super::objects_rpc::bind(ctx, &req.transfer.route).map_err(anyhow::Error::new)?;
            let state = state(ctx)?;
            let item = match take(&state, &req.transfer) {
                Ok(item) => Some(item),
                Err(e) if crate::model::errors::code_of(&e) == "STALE_ADMISSION_GENERATION" => None,
                Err(e) => return Err(e),
            };
            let released = item.is_some();
            if let Some(item) = item
                && req.failed_source
            {
                item.transfer.failed(&mut *connection(ctx)?)?;
            }
            Ok((json!({"released":released}), vec![]))
        }
        _ => anyhow::bail!("UNSUPPORTED_OPERATION"),
    }
}
