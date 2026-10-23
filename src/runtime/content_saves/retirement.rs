//! Reconcile a retired open file without repeating a namespace mutation.
use super::*;

impl SaveExecutor {
    pub(crate) fn retired_file(&self, file: &Arc<SaveFile>, context: &ReadContext) -> Result<bool> {
        while self.adopt_committed(file.clone(), context)? {
            context.check()?;
        }
        file.check_namespace_before_mutation();
        self.reconcile_retirement(file, context)?;
        let intent = lock(&file.state, context)?.session.intent(context)?;
        connection_capture(&self.pool, &intent.branch, context, false)?
            .0
            .content_save_is_retired(&intent, context)
    }
    /// Caller owns this file's adapter or byte-read gate. Only files marked by an attempted
    /// replacement pay for this read-only lookup on their next mutation/save.
    pub(crate) fn reconcile_retirement(
        &self,
        file: &Arc<SaveFile>,
        context: &ReadContext,
    ) -> Result<()> {
        if !file.namespace_check.load(Ordering::Acquire) {
            return Ok(());
        }
        let _retirement = lock(&file.retirement_gate, context)?;
        if !file.namespace_check.load(Ordering::Acquire) {
            return Ok(());
        }
        self.wait_idle(file, context)?;
        let (session, intent, pending) = {
            let state = lock(&file.state, context)?;
            let pending = state.pending_retirement.clone();
            let intent = match &pending {
                Some(intent) => intent.clone(),
                None => state.session.intent(context)?,
            };
            (state.session.clone(), intent, pending.is_some())
        };
        let (handoff, view) = {
            let (store, view) = connection_capture(&self.pool, &intent.branch, context, false)?;
            (
                store.content_namespace_continuation(&intent, context)?,
                view,
            )
        };
        let Some(handoff) = handoff else {
            ensure!(
                !pending,
                "CORRUPT_CONTENT_SAVE: namespace handoff disappeared"
            );
            file.namespace_check.store(false, Ordering::Release);
            return Ok(());
        };
        {
            let mut state = lock(&file.state, context)?;
            ensure!(Arc::ptr_eq(&state.session, &session), "STALE_WRITE_SESSION");
            state.pending_retirement = Some(intent);
            if session.intent(context).ok().as_ref() != Some(handoff.intent()) {
                let next = session.adopt_namespace_continuation(&handoff, context)?;
                state.session = Arc::new(next);
                *crate::util::lock::lock(&file.read_session) = state.session.clone();
                file.mirror(&state.session);
            }
            state.pending = None;
            // Keep the flag until the session's namespace observation succeeds.
            // If the reply expires now, the new journal intent needs no rebase.
        }
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
        let entry = e.store.entry(&handoff.intent().base.entity)?;
        if let Some(session) = e.sessions.get_mut(&entry.id)
            && session
                .bytes
                .cow_file()
                .is_some_and(|f| Arc::ptr_eq(&f, file))
        {
            session.entry.alive = entry.alive;
            session.content_base = handoff.intent().content_parent.clone();
            session.alive_base = handoff.intent().alive_parent.clone();
            if let Some(revision) = entry.revisions.get("alive") {
                session
                    .entry
                    .revisions
                    .insert("alive".to_owned(), revision.clone());
            }
        }
        e.publish_observation();
        drop(e);
        crate::util::lock::lock(&file.state).pending_retirement = None;
        file.namespace_check.store(false, Ordering::Release);
        Ok(())
    }
}
