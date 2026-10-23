//! Authenticated hydration of native private-session bases. No Engine, file
//! state, journal or preflight buffer permit is held while waiting for a peer.
//! Canonical preparation/flush never invokes this network recovery hook.
use super::{Shared, ctx::WorkerCtx};
use crate::{
    content::{
        LocalContent, ReadContext,
        write::{BaseRequest, BaseWait, WriteSession},
    },
    model::{
        content::ObjectKind,
        workspace::{Demand, DemandPriority},
    },
};
use anyhow::{Context, Result, ensure};
use std::{
    sync::{Arc, Weak},
    time::{Duration, Instant},
};
use uuid::Uuid;

struct NativeBase {
    worker: Weak<WorkerCtx>,
    generation: u64,
}
pub(super) fn attach(shared: &Shared, generation: u64, session: WriteSession) -> WriteSession {
    match super::worker_of(shared) {
        Some(worker) => session.with_base_wait(Arc::new(NativeBase {
            worker: Arc::downgrade(&worker),
            generation,
        })),
        None => session,
    }
}
impl NativeBase {
    fn check(
        &self,
        worker: &WorkerCtx,
        request: &BaseRequest<'_>,
        context: &ReadContext,
    ) -> Result<()> {
        loop {
            context.check()?;
            ensure!(
                request.limits.is_none_or(|limits| !(limits.cancelled)()),
                "CONTENT_CANCELLED"
            );
            ensure!(
                !worker.stopping.load(std::sync::atomic::Ordering::Acquire),
                "CONTENT_CANCELLED"
            );
            if let Some(engine) = super::try_lock_engine(&worker.engine) {
                ensure!(
                    !engine.switching
                        && engine.store.root == worker.root
                        && engine.store.active == request.intent.branch
                        && engine.store.generation == self.generation
                        && engine
                            .sessions
                            .get(&request.intent.base.entity)
                            .and_then(|session| session.bytes.cow_file())
                            .is_some_and(|file| file.operation() == request.intent.operation),
                    "STALE_CONTENT_VIEW"
                );
                return Ok(());
            }
            context.cancel.sleep(Duration::from_millis(5));
        }
    }
}
impl BaseWait for NativeBase {
    fn wait(
        &self,
        request: BaseRequest<'_>,
        error: anyhow::Error,
        context: &ReadContext,
    ) -> Result<()> {
        let Some(key) = LocalContent::unavailable_key(&error) else {
            return Err(error);
        };
        let Some(worker) = self.worker.upgrade() else {
            return Err(error);
        };
        crate::util::fault::trigger("content.base.before_demand")?;
        self.check(&worker, &request, context)?;
        let _permit = super::content_reads::WAITERS
            .take()
            .context("CONTENT_RESOURCE_LIMIT")?;
        let mut store = super::content_reads::retry_sql(context, || {
            self.check(&worker, &request, context)?;
            worker.sync_stores.checkout_nowait()
        })?;
        let state = super::content_reads::retry_sql(context, || {
            self.check(&worker, &request, context)?;
            super::content_rpc::state_for_read(&worker, &store)
        })?;
        if request.pin.provider() != state.provider.identity() {
            return Err(error);
        }
        ensure!(
            request.pin.reference() == &request.intent.base.content,
            "CONTENT_PROVIDER_MISMATCH"
        );
        let reference = crate::core::admission::reference_id(
            request
                .intent
                .content_parent
                .as_ref()
                .context("MISSING_CONTENT_DESCRIPTOR")?,
            &request.intent.base.entity,
        );
        let (generation, root, granted) = super::content_reads::retry_sql(context, || {
            self.check(&worker, &request, context)?;
            store.content_root(&reference)
        })?;
        ensure!(
            granted && root == request.intent.base.content,
            "INVALID_DEMAND_SCOPE"
        );
        ensure!(
            super::content_reads::retry_sql(context, || {
                self.check(&worker, &request, context)?;
                store.content_workspace_id(context)
            })? == request.intent.base.workspace,
            "INVALID_DEMAND_SCOPE"
        );
        let priority = if key.kind == ObjectKind::Manifest {
            DemandPriority::Structural
        } else {
            DemandPriority::Foreground
        };
        let demand = Demand {
            waiter: Uuid::new_v4(),
            reference,
            revision: crate::model::workspace::FileRevision {
                workspace: request.intent.base.workspace.clone(),
                entity: request.intent.base.entity.clone(),
                generation: self.generation,
                content: root,
            },
            range: request.range,
            priority,
            eligible_peers: vec![],
        };
        let validate = |store: &crate::core::Store| -> Result<()> {
            self.check(&worker, &request, context)?;
            super::content_reads::retry_sql(context, || {
                self.check(&worker, &request, context)?;
                let (current, root, granted) = store.content_root(&demand.reference)?;
                ensure!(
                    granted
                        && current == generation
                        && root == demand.revision.content
                        && store.content_reference_authorized(&demand.reference, generation)?,
                    "STALE_ADMISSION_GENERATION"
                );
                store.validate_content_demand(&demand)
            })
        };
        validate(&store)?;
        super::content_reads::retry_sql(context, || {
            self.check(&worker, &request, context)?;
            store.observe_content_unavailable(key)
        })?;
        let _lease =
            state
                .scheduler
                .demand_on(state.provider.identity(), demand.clone(), context)?;
        let began = Instant::now();
        loop {
            validate(&store)?;
            match state
                .provider
                .read_blob_for(key, None, priority, context)
                .map(drop)
            {
                Ok(()) => {
                    validate(&store)?;
                    return Ok(());
                }
                Err(error) if LocalContent::unavailable_key(&error) == Some(key) => {}
                Err(error) => return Err(error),
            }
            ensure!(
                request
                    .limits
                    .is_none_or(|limits| began.elapsed() < limits.stall),
                "CONTENT_UNAVAILABLE"
            );
            context.cancel.sleep(Duration::from_millis(10));
        }
    }
}
