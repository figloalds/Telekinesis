//! Exact unlink replies and retained unnamed saves, outside Engine.
use super::*;
use crate::core::content_saves::{NamespaceDestination, NamespaceTarget};

#[derive(Clone)]
pub(super) struct PendingUnlink {
    destination: NamespaceDestination,
    mutation: Uuid,
    target: NamespaceTarget,
    durable: bool,
}
impl SaveExecutor {
    fn clear_unlink(&self, file: &SaveFile, mutation: Uuid, context: &ReadContext) -> Result<()> {
        let mut state = lock(&file.state, context)?;
        if state
            .pending_unlink
            .as_ref()
            .is_some_and(|p| p.mutation == mutation)
        {
            state.pending_unlink = None;
        }
        Ok(())
    }
    pub(crate) fn reconcile_unlink(
        &self,
        file: &Arc<SaveFile>,
        context: &ReadContext,
    ) -> Result<bool> {
        let Some(pending) = lock(&file.state, context)?.pending_unlink.clone() else {
            return Ok(false);
        };
        self.wait_idle(file, context)?;
        let (receipt, view) = {
            let (mut store, view) = connection_capture(
                &self.pool,
                &pending.destination.intent.branch,
                context,
                false,
            )?;
            let mut receipt = store.content_checkpoint_unlink_receipt(
                pending.destination.clone(),
                pending.mutation,
                &pending.target,
                context,
            )?;
            if receipt.is_none() && pending.durable {
                // This is replay of a separately durable, already authorized
                // disposition, not a retry of an unrecorded failed unlink.
                ensure!(
                    store
                        .content_delete_requested(pending.destination.intent.operation, context)?,
                    "CONTENT_METADATA_RECEIPT_LOST"
                );
                receipt = Some(store.apply_content_checkpoint_unlink(
                    pending.destination.clone(),
                    pending.mutation,
                    &pending.target,
                    context,
                )?);
            }
            (receipt, view)
        };
        let Some(receipt) = receipt else {
            self.clear_unlink(file, pending.mutation, context)?;
            return Ok(false);
        };
        // Adoption changes only private ownership. It never publishes bytes or
        // retries the namespace event, including after an expired/lost reply.
        self.reconcile_retirement(file, context)?;
        let shared = self
            .pool
            .engine
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("CONTENT_CANCELLED"))?;
        let mut e = engine(&shared, context)?;
        ensure!(
            e.store.root == self.pool.root
                && e.store.active == view.branch
                && e.store.generation == view.generation,
            "STALE_CONTENT_VIEW"
        );
        e.note_secondary_commit();
        let delta = e.activation_delta(&[receipt.event], None)?;
        e.after_activation(&delta);
        if cfg!(windows) && e.mounted {
            e.queue_notifications(vec![(
                super::super::notify::ROOT_PATH.to_owned(),
                super::super::notify::ACTION_MODIFIED,
            )]);
        }
        e.publish_observation();
        drop(e);
        self.clear_unlink(file, pending.mutation, context)?;
        Ok(true)
    }
    /// Caller owns the entity and adapter mutation gates. FULL Saved precedes
    /// the metadata-only unlink; later writes keep the entity unnamed.
    pub(crate) fn unlink_file(
        &self,
        file: Arc<SaveFile>,
        target: &NamespaceTarget,
        durable: bool,
        context: &ReadContext,
    ) -> Result<()> {
        let context = ReadContext {
            deadline: context.deadline,
            cancel: context.cancel.linked(&self.pool.stop),
        };
        self.reconcile_basic(&file, &context)?;
        self.reconcile_rename(&file, &context)?;
        if self.reconcile_unlink(&file, &context)? {
            return Ok(());
        }
        self.reconcile_retirement(&file, &context)?;
        while self.adopt_committed(file.clone(), &context)? {
            context.check()?;
        }
        if durable {
            let branch = lock(&file.state, &context)?
                .session
                .intent(&context)?
                .branch;
            ensure!(
                connection_capture(&self.pool, &branch, &context, false)?
                    .0
                    .content_delete_requested(file.operation(), &context)?,
                "RECOVERY_REQUIRED: missing native delete disposition"
            );
        }
        let pending = {
            let mut state = lock(&file.state, &context)?;
            let checkpoint = state.session.checkpoint(&context)?;
            file.checkpointed();
            let pending = PendingUnlink {
                destination: NamespaceDestination {
                    intent: state.session.intent(&context)?,
                    checkpoint,
                },
                mutation: Uuid::new_v4(),
                target: target.clone(),
                durable,
            };
            state.pending_unlink = Some(pending.clone());
            file.check_namespace_before_mutation();
            pending
        };
        let result = (|| {
            let (mut store, _) = connection_capture(
                &self.pool,
                &pending.destination.intent.branch,
                &context,
                false,
            )?;
            store.apply_content_checkpoint_unlink(
                pending.destination,
                pending.mutation,
                &pending.target,
                &context,
            )?;
            drop(store);
            ensure!(
                self.reconcile_unlink(&file, &context)?,
                "CONTENT_METADATA_RECEIPT_LOST"
            );
            Ok(())
        })();
        match result {
            Ok(()) => Ok(()),
            Err(error) => match self.reconcile_unlink(&file, &context) {
                Ok(true) => Ok(()),
                Ok(false) => Err(error),
                Err(error) => Err(error),
            },
        }
    }
}
