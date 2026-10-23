//! Native metadata ordering without canonicalizing unrelated private bytes.
use super::*;
use crate::{content::write::SaveIntent, core::content_saves::ContentBasicUpdate};

#[derive(Clone)]
pub(super) struct PendingBasic {
    intent: SaveIntent,
    mutation: Uuid,
    checkpoint: Option<SaveCheckpoint>,
    fields: ContentBasicUpdate,
    handle: u64,
    disabled: u8,
}

fn observe_basic(
    pool: &SavePool,
    file: &Arc<SaveFile>,
    pending: &PendingBasic,
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
    let basic = e
        .store
        .content_save_basic_info(file.operation(), file.basic_state(), context)?;
    if let Some(session) = e.sessions.get_mut(entity)
        && session
            .bytes
            .cow_file()
            .is_some_and(|current| Arc::ptr_eq(&current, file))
    {
        session.entry.basic = Some(basic);
        session.content_base = pending.intent.content_parent.clone();
        session.alive_base = pending.intent.alive_parent.clone();
    }
    if e.sessions
        .get(entity)
        .and_then(|s| s.bytes.cow_file())
        .is_some_and(|current| Arc::ptr_eq(&current, file))
        && let Some(handle) = e.handles.get_mut(&pending.handle)
        && handle.entity == *entity
    {
        handle.time_update_disabled = pending.disabled;
    }
    if cfg!(windows) && e.mounted {
        let notification = super::super::notify::metadata_notification(&e.store, entity);
        e.queue_notifications(vec![notification]);
    }
    e.publish_observation();
    Ok(())
}

impl SaveExecutor {
    fn clear_basic(
        &self,
        file: &SaveFile,
        mutation: Uuid,
        committed: bool,
        context: &ReadContext,
    ) -> Result<()> {
        let mut state = lock(&file.state, context)?;
        if state
            .pending_basic
            .as_ref()
            .is_some_and(|p| p.mutation == mutation)
        {
            state.pending_basic = None;
            if committed {
                state.pending = None;
            }
        }
        Ok(())
    }
    /// Complete a known metadata receipt, or discard an uncommitted failed
    /// request. Absence never invokes the mutating Core command.
    pub(crate) fn reconcile_basic(
        &self,
        file: &Arc<SaveFile>,
        context: &ReadContext,
    ) -> Result<bool> {
        let pending = lock(&file.state, context)?.pending_basic.clone();
        let Some(pending) = pending else {
            return Ok(false);
        };
        self.wait_idle(file, context)?;
        let (receipt, view) = {
            let (store, view) =
                connection_capture(&self.pool, &pending.intent.branch, context, false)?;
            (
                store.content_checkpoint_metadata_receipt(
                    &pending.intent,
                    pending.mutation,
                    pending.checkpoint.as_ref(),
                    &pending.fields,
                    context,
                )?,
                view,
            )
        };
        let Some(receipt) = receipt else {
            self.clear_basic(file, pending.mutation, false, context)?;
            return Ok(false);
        };
        observe_basic(&self.pool, file, &pending, &receipt.event, view, context)?;
        self.clear_basic(file, pending.mutation, true, context)?;
        Ok(true)
    }
    /// Caller owns the file's native mutation gate. Only metadata is published;
    /// a FULL private checkpoint anchors the bytes it describes.
    pub(crate) fn update_basic(
        &self,
        file: Arc<SaveFile>,
        handle: u64,
        fields: ContentBasicUpdate,
        disabled: u8,
        context: &ReadContext,
    ) -> Result<()> {
        let context = ReadContext {
            deadline: context.deadline,
            cancel: context.cancel.linked(&self.pool.stop),
        };
        self.reconcile_basic(&file, &context)?;
        while self.adopt_committed(file.clone(), &context)? {
            context.check()?;
        }
        let pending = {
            let mut state = lock(&file.state, &context)?;
            let checkpoint = state.session.checkpoint(&context)?;
            file.checkpointed();
            let pending = PendingBasic {
                intent: state.session.intent(&context)?,
                mutation: Uuid::new_v4(),
                checkpoint,
                fields,
                handle,
                disabled,
            };
            state.pending_basic = Some(pending.clone());
            pending
        };
        let result = (|| {
            let (mut store, view) =
                connection_capture(&self.pool, &pending.intent.branch, &context, false)?;
            let receipt = store.apply_content_checkpoint_basic(
                &pending.intent,
                pending.mutation,
                pending.checkpoint.as_ref(),
                pending.fields.clone(),
                &context,
            )?;
            drop(store);
            observe_basic(&self.pool, &file, &pending, &receipt.event, view, &context)?;
            self.clear_basic(&file, pending.mutation, true, &context)
        })();
        match result {
            Ok(()) => Ok(()),
            Err(error) => match self.reconcile_basic(&file, &context) {
                Ok(true) => Ok(()),
                Ok(false) => Err(error),
                Err(reconcile) => Err(reconcile),
            },
        }
    }
}
