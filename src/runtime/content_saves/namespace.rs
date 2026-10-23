//! Reconcile namespace receipts without retrying uncommitted operations.
use super::*;
use crate::{
    content::write::SaveIntent,
    core::content_saves::{NamespaceDestination, NamespaceTarget},
};

#[derive(Clone)]
pub(super) struct PendingRename {
    intent: SaveIntent,
    mutation: Uuid,
    checkpoint: Option<SaveCheckpoint>,
    target: NamespaceTarget,
    destination: Option<NamespaceDestination>,
}
fn observe(
    pool: &SavePool,
    file: &Arc<SaveFile>,
    pending: &PendingRename,
    event: &str,
    view: MetadataView,
    context: &ReadContext,
) -> Result<()> {
    let shared = pool
        .engine
        .upgrade()
        .ok_or_else(|| anyhow::anyhow!("CONTENT_CANCELLED"))?;
    let mut e = engine(&shared, context)?;
    ensure!(
        e.store.root == pool.root
            && e.store.active == view.branch
            && e.store.generation == view.generation,
        "STALE_CONTENT_VIEW"
    );
    e.note_secondary_commit();
    let delta = e.activation_delta(&[event.to_owned()], None)?;
    e.after_activation(&delta);
    let entity = &pending.intent.base.entity;
    let entry = e.store.entry(entity)?;
    ensure!(entry.alive, "STALE_CONTENT_VIEW");
    if let Some(session) = e.sessions.get_mut(entity)
        && session
            .bytes
            .cow_file()
            .is_some_and(|current| Arc::ptr_eq(&current, file))
    {
        session.entry.parent = entry.parent;
        session.entry.name = entry.name;
        if let Some(location) = entry.revisions.get("location") {
            session
                .entry
                .revisions
                .insert("location".to_owned(), location.clone());
        }
        session.content_base = pending.intent.content_parent.clone();
        session.alive_base = pending.intent.alive_parent.clone();
    }
    if let Some(destination) = &pending.destination {
        let entry = e.store.entry(&destination.intent.base.entity)?;
        if let Some(session) = e.sessions.get_mut(&entry.id)
            && let Some(file) = session.bytes.cow_file()
            && file.operation() == destination.intent.operation
        {
            file.check_namespace_before_mutation();
            session.entry.alive = entry.alive;
        }
    }
    if cfg!(windows) && e.mounted {
        // Without a retained pre-commit directory snapshot, invalidate both
        // names conservatively through the existing bounded root fallback.
        e.queue_notifications(vec![(
            super::super::notify::ROOT_PATH.to_owned(),
            super::super::notify::ACTION_MODIFIED,
        )]);
    }
    e.publish_observation();
    Ok(())
}
impl SaveExecutor {
    fn clear_rename(
        &self,
        file: &SaveFile,
        mutation: Uuid,
        committed: bool,
        context: &ReadContext,
    ) -> Result<()> {
        let mut state = lock(&file.state, context)?;
        if state
            .pending_rename
            .as_ref()
            .is_some_and(|p| p.mutation == mutation)
        {
            state.pending_rename = None;
            if committed {
                state.pending = None;
            }
        }
        Ok(())
    }
    pub(crate) fn reconcile_rename(
        &self,
        file: &Arc<SaveFile>,
        context: &ReadContext,
    ) -> Result<bool> {
        let Some(pending) = lock(&file.state, context)?.pending_rename.clone() else {
            return Ok(false);
        };
        self.wait_idle(file, context)?;
        let (receipt, view) = {
            let (store, view) =
                connection_capture(&self.pool, &pending.intent.branch, context, false)?;
            let receipt = store.content_checkpoint_rename_at_receipt(
                &pending.intent,
                pending.mutation,
                pending.checkpoint.as_ref(),
                &pending.target,
                pending.destination.clone(),
                context,
            )?;
            (receipt, view)
        };
        let Some(receipt) = receipt else {
            self.clear_rename(file, pending.mutation, false, context)?;
            return Ok(false);
        };
        observe(&self.pool, file, &pending, &receipt.event, view, context)?;
        self.clear_rename(file, pending.mutation, true, context)?;
        Ok(true)
    }
    /// Caller holds source/destination adapter gates in entity order.
    pub(crate) fn rename_file(
        &self,
        file: Arc<SaveFile>,
        target: &NamespaceTarget,
        destination: Option<Arc<SaveFile>>,
        context: &ReadContext,
    ) -> Result<()> {
        let context = ReadContext {
            deadline: context.deadline,
            cancel: context.cancel.linked(&self.pool.stop),
        };
        self.reconcile_basic(&file, &context)?;
        self.reconcile_rename(&file, &context)?;
        self.reconcile_retirement(&file, &context)?;
        while self.adopt_committed(file.clone(), &context)? {
            context.check()?;
        }
        let destination = if let Some(destination) = destination {
            self.reconcile_basic(&destination, &context)?;
            self.reconcile_rename(&destination, &context)?;
            self.reconcile_retirement(&destination, &context)?;
            while self.adopt_committed(destination.clone(), &context)? {
                context.check()?;
            }
            let state = lock(&destination.state, &context)?;
            let checkpoint = state.session.checkpoint(&context)?;
            destination.checkpointed();
            let request = NamespaceDestination {
                intent: state.session.intent(&context)?,
                checkpoint,
            };
            destination.check_namespace_before_mutation();
            Some(request)
        } else {
            None
        };
        let pending = {
            let mut state = lock(&file.state, &context)?;
            let checkpoint = state.session.checkpoint(&context)?;
            file.checkpointed();
            let pending = PendingRename {
                intent: state.session.intent(&context)?,
                mutation: Uuid::new_v4(),
                checkpoint,
                target: target.clone(),
                destination,
            };
            state.pending_rename = Some(pending.clone());
            pending
        };
        let result = (|| {
            let (mut store, view) =
                connection_capture(&self.pool, &pending.intent.branch, &context, false)?;
            let receipt = store.apply_content_checkpoint_rename_at(
                &pending.intent,
                pending.mutation,
                pending.checkpoint.as_ref(),
                &pending.target,
                pending.destination.clone(),
                &context,
            )?;
            drop(store);
            observe(&self.pool, &file, &pending, &receipt.event, view, &context)?;
            self.clear_rename(&file, pending.mutation, true, &context)
        })();
        match result {
            Ok(()) => Ok(()),
            Err(error) => match self.reconcile_rename(&file, &context) {
                Ok(true) => Ok(()),
                Ok(false) => Err(error),
                Err(error) => Err(error),
            },
        }
    }
}
