//! Local control consumers of immutable content references. Provider work and
//! range waits never hold Engine or a metadata transaction.
use super::{
    content::{WaitSlot, WaitSlots},
    content_reads::{WAITERS, retry_sql},
    content_rpc::{ContentWorker, state_for_read},
    ctx::WorkerCtx,
};
use crate::{
    content::{ContentProvider, LocalContent, PinnedContent, ReadContext, scheduler::DemandLease},
    core::Store,
    model::workspace::{ByteRange, Demand, DemandPriority, FileRevision, limits},
    util::{Cancel, lock::lock},
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use uuid::Uuid;

pub const PREVIEW_BYTES: usize = 65_536;
// Explicit prefetches leave most foreground waiter slots for filesystem reads.
const REQUESTS: usize = limits::DEMAND_WAITERS / 4;
static REQUEST_SLOTS: WaitSlots = WaitSlots::new(REQUESTS);

struct CallSlot<'a>(&'a WorkerCtx);
impl<'a> CallSlot<'a> {
    fn take(worker: &'a WorkerCtx) -> Result<Self> {
        let mut state = lock(&worker.inflight_objects);
        ensure!(state.inspection_calls < 2, "CONTENT_RESOURCE_LIMIT");
        state.inspection_calls += 1;
        Ok(Self(worker))
    }
}
impl Drop for CallSlot<'_> {
    fn drop(&mut self) {
        lock(&self.0.inflight_objects).inspection_calls -= 1;
    }
}

#[derive(Clone)]
struct Selection {
    reference: String,
    admission: u64,
    branch: String,
    revision: FileRevision,
}

impl Selection {
    fn validate(&self, store: &Store) -> Result<()> {
        let (generation, root, granted) = store.content_root(&self.reference)?;
        ensure!(
            granted && generation == self.admission && root == self.revision.content,
            "STALE_ADMISSION_GENERATION"
        );
        Ok(())
    }
}

struct Queued {
    selection: Selection,
    range: ByteRange,
    deadline: Instant,
    cancel: Cancel,
    _lease: DemandLease,
    _pin: PinnedContent,
    _slot: WaitSlot,
}

#[derive(Default)]
pub(super) struct Requests {
    items: BTreeMap<Uuid, Queued>,
}

impl Requests {
    pub(super) fn clear(&mut self) {
        self.items.clear();
    }
}

struct Request<'a> {
    worker: &'a WorkerCtx,
    context: ReadContext,
    cancelled: &'a dyn Fn() -> bool,
}

impl Request<'_> {
    fn check(&self) -> Result<()> {
        if (self.cancelled)() {
            self.context.cancel.cancel();
        }
        self.context.check()?;
        ensure!(
            !self
                .worker
                .stopping
                .load(std::sync::atomic::Ordering::Acquire),
            "CONTENT_CANCELLED"
        );
        Ok(())
    }

    fn view(&self) -> Result<(String, u64)> {
        loop {
            self.check()?;
            if let Some(engine) = super::try_lock_engine(&self.worker.engine) {
                ensure!(
                    !engine.switching && engine.store.root == self.worker.root,
                    "STALE_VIEW"
                );
                return Ok((engine.store.active.clone(), engine.store.generation));
            }
            self.context.cancel.sleep(Duration::from_millis(5));
        }
    }

    fn selected(&self, selection: &Selection, store: &Store) -> Result<()> {
        ensure!(
            self.view()? == (selection.branch.clone(), selection.revision.generation),
            "STALE_VIEW"
        );
        self.sql(|| selection.validate(store))
    }

    fn sql<T>(&self, mut operation: impl FnMut() -> Result<T>) -> Result<T> {
        retry_sql(&self.context, || {
            self.check()?;
            operation()
        })
    }

    fn capture(&self, store: &mut Store, payload: &Value) -> Result<Selection> {
        let (branch, generation) = self.view()?;
        if let Some(requested) = payload["generation"].as_u64() {
            ensure!(requested == generation, "STALE_VIEW");
        }
        let reference = payload["reference"].as_str().context("MISSING_ARGUMENT")?;
        crate::model::content::Digest::parse(reference).context("INVALID_DEMAND_SCOPE")?;
        let (admission, root, granted) = self.sql(|| store.content_root(reference))?;
        ensure!(granted, "UNAUTHORIZED_OBJECT");
        let entity = self.sql(|| {
            Ok(store.db.query_row(
                "SELECT entity FROM content_root_metadata WHERE reference=?1",
                [reference],
                |r| r.get(0),
            )?)
        })?;
        let workspace = self.sql(|| store.content_workspace_id(&self.context))?;
        let selection = Selection {
            reference: reference.into(),
            admission,
            branch,
            revision: FileRevision {
                workspace,
                entity,
                generation,
                content: root,
            },
        };
        self.selected(&selection, store)?;
        Ok(selection)
    }

    fn demand(&self, selection: &Selection, range: ByteRange, waiter: Uuid) -> Demand {
        Demand {
            waiter,
            reference: selection.reference.clone(),
            revision: selection.revision.clone(),
            range,
            priority: DemandPriority::Foreground,
            eligible_peers: vec![],
        }
    }
}

/// JSON control uses the existing 15s preview budget. Session callers supply
/// their shorter absolute deadline and cancellation predicate.
pub fn dispatch(
    worker: &WorkerCtx,
    op: &str,
    payload: &Value,
    deadline: Instant,
    cancelled: &dyn Fn() -> bool,
) -> Result<Value> {
    let cancel = lock(&worker.inflight_objects).legacy_cancel.child();
    let request = Request {
        worker,
        cancelled,
        context: ReadContext {
            deadline: deadline.min(Instant::now() + Duration::from_secs(15)),
            cancel,
        },
    };
    request.check()?;
    ensure!(
        matches!(
            op,
            "preview-content" | "request-content" | "cancel-content-request"
        ),
        "UNKNOWN_OPERATION"
    );
    if op == "cancel-content-request" {
        let token: Uuid = payload["token"]
            .as_str()
            .context("MISSING_ARGUMENT")?
            .parse()
            .context("INVALID_REQUEST")?;
        let state = lock(&worker.inflight_objects).content.clone();
        let cancelled =
            state.is_some_and(|state| lock(&state.requests).items.remove(&token).is_some());
        return Ok(json!({"token":token,"cancelled":cancelled}));
    }
    let _call = CallSlot::take(worker)?;
    let slot = WAITERS.take().context("CONTENT_RESOURCE_LIMIT")?;
    let mut store = request.sql(|| worker.sync_stores.checkout_nowait())?;
    let selection = request.capture(&mut store, payload)?;
    let state = request.sql(|| state_for_read(worker, &store))?;
    let pin = state.provider.pin(&selection.revision.content)?;
    let offset = if op == "preview-content" {
        0
    } else {
        payload["offset"].as_u64().unwrap_or(0)
    };
    let maximum = if op == "preview-content" {
        PREVIEW_BYTES
    } else {
        limits::READ_BYTES
    } as u64;
    let range = ByteRange {
        offset,
        len: if op == "preview-content" {
            selection.revision.content.total_len.min(maximum)
        } else {
            payload["len"].as_u64().unwrap_or_else(|| {
                selection
                    .revision
                    .content
                    .total_len
                    .saturating_sub(offset)
                    .min(maximum)
            })
        },
    };
    range.validate(selection.revision.content.total_len)?;
    ensure!(range.len <= maximum, "CONTENT_BUFFER_LIMIT");
    let mut bytes = vec![0; range.len as usize];
    crate::util::fault::trigger("content.inspection.before_read")?;
    request.check()?;
    let initial = state
        .provider
        .read_range(&pin, range.offset, &mut bytes, &request.context);
    request.selected(&selection, &store)?;
    if op == "request-content" {
        return queue(
            &request,
            &mut store,
            &state,
            selection,
            range,
            (pin, slot),
            initial,
        );
    }
    ensure!(op == "preview-content", "UNKNOWN_OPERATION");
    if let Err(error) = initial {
        let Some(mut key) = LocalContent::unavailable_key(&error) else {
            return Err(error);
        };
        request.sql(|| store.observe_content_unavailable(key))?;
        if !request
            .sql(|| store.content_reference_authorized(&selection.reference, selection.admission))?
        {
            return Err(error);
        }
        let mut lease = None;
        let mut observed = None;
        loop {
            request.selected(&selection, &store)?;
            ensure!(
                request.sql(|| store
                    .content_reference_authorized(&selection.reference, selection.admission))?,
                "STALE_ADMISSION_GENERATION"
            );
            if observed != Some(key)
                || request.sql(|| store.content_object_verified(key))?
                || lease.as_ref().is_some_and(|lease: &DemandLease| {
                    !state.scheduler.waiter_pending(lease.waiter())
                })
            {
                request.sql(|| store.observe_content_unavailable(key))?;
                drop(lease.take());
                lease = Some(state.scheduler.demand_on(
                    state.provider.identity(),
                    request.demand(&selection, range, Uuid::new_v4()),
                    &request.context,
                )?);
                observed = Some(key);
            }
            request.context.cancel.sleep(Duration::from_millis(10));
            request.selected(&selection, &store)?;
            match state
                .provider
                .read_range(&pin, range.offset, &mut bytes, &request.context)
            {
                Ok(()) => break,
                Err(error) => key = LocalContent::unavailable_key(&error).ok_or(error)?,
            }
        }
    }
    request.selected(&selection, &store)?;
    let binary = bytes.contains(&0) || std::str::from_utf8(&bytes).is_err();
    Ok(
        json!({"reference":selection.reference,"descriptor":selection.revision.content,
        "size":selection.revision.content.total_len,"truncated":range.len < selection.revision.content.total_len,
        "text":if binary { "Binary content; text preview unavailable".to_owned() } else { String::from_utf8_lossy(&bytes).into_owned() }}),
    )
}

fn queue(
    request: &Request<'_>,
    store: &mut Store,
    state: &Arc<ContentWorker>,
    selection: Selection,
    range: ByteRange,
    owned: (PinnedContent, WaitSlot),
    initial: Result<()>,
) -> Result<Value> {
    let (pin, _read_slot) = owned;
    let error = match initial {
        Ok(()) => {
            return Ok(
                json!({"reference":selection.reference,"range":range,"queued":false,"available":true}),
            );
        }
        Err(error) => error,
    };
    let Some(key) = LocalContent::unavailable_key(&error) else {
        return Err(error);
    };
    request.sql(|| store.observe_content_unavailable(key))?;
    if !request
        .sql(|| store.content_reference_authorized(&selection.reference, selection.admission))?
    {
        return Err(error);
    }
    reap(request.worker, state);
    let joined = {
        lock(&state.requests)
            .items
            .iter()
            .find_map(|(token, item)| {
                (item.selection.reference == selection.reference
                    && item.selection.admission == selection.admission
                    && item.selection.revision.generation == selection.revision.generation
                    && item.range == range)
                    .then_some(*token)
            })
    };
    if let Some(token) = joined {
        request.selected(&selection, store)?;
        return Ok(
            json!({"reference":selection.reference,"range":range,"queued":true,"token":token}),
        );
    }
    let slot = REQUEST_SLOTS.take().context("CONTENT_RESOURCE_LIMIT")?;
    let token = Uuid::new_v4();
    let lifetime = Duration::from_millis(crate::core::demand::DEMAND_TTL_MS);
    let context = ReadContext {
        deadline: Instant::now() + lifetime,
        cancel: lock(&request.worker.inflight_objects).legacy_cancel.child(),
    };
    let lease = state.scheduler.demand_on(
        state.provider.identity(),
        request.demand(&selection, range, token),
        &context,
    )?;
    request.selected(&selection, store)?;
    let reference = selection.reference.clone();
    let mut requests = lock(&state.requests);
    // A racing retry joins the first owner's request; its temporary lease/pin
    // are dropped, never replacing that owner's cancellation identity.
    if let Some(existing) = requests.items.iter().find_map(|(token, item)| {
        (item.selection.reference == selection.reference
            && item.selection.admission == selection.admission
            && item.selection.revision.generation == selection.revision.generation
            && item.range == range)
            .then_some(*token)
    }) {
        return Ok(json!({"reference":reference,"range":range,"queued":true,"token":existing}));
    }
    ensure!(requests.items.len() < REQUESTS, "CONTENT_RESOURCE_LIMIT");
    request.check()?;
    requests.items.insert(
        token,
        Queued {
            selection,
            range,
            deadline: context.deadline,
            cancel: context.cancel,
            _lease: lease,
            _pin: pin,
            _slot: slot,
        },
    );
    Ok(
        json!({"reference":reference,"range":range,"queued":true,"token":token,"expires_after_ms":lifetime.as_millis()}),
    )
}

pub(super) fn reap(worker: &WorkerCtx, state: &ContentWorker) {
    let now = Instant::now();
    let snapshots: Vec<_> = {
        let mut requests = lock(&state.requests);
        requests
            .items
            .retain(|_, item| item.deadline > now && !item.cancel.is_cancelled());
        requests
            .items
            .iter()
            .map(|(token, item)| (*token, item.selection.clone(), item.range))
            .collect()
    };
    if snapshots.is_empty() {
        return;
    }
    let Ok(store) = worker.sync_stores.checkout_nowait() else {
        return;
    };
    for (token, selection, range) in snapshots {
        let result = (|| -> Result<bool> {
            if store.active != selection.branch || store.generation != selection.revision.generation
            {
                return Ok(true);
            }
            if let Err(error) = selection.validate(&store) {
                if matches!(
                    crate::model::errors::code_of(&error).as_str(),
                    "STALE_ADMISSION_GENERATION" | "UNAUTHORIZED_OBJECT"
                ) {
                    return Ok(true);
                }
                return Err(error);
            }
            store.content_range_indexed(&selection.reference, range)
        })();
        if matches!(result, Ok(true)) {
            lock(&state.requests).items.remove(&token);
        }
    }
}
