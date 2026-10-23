//! FULL namespace handoff. No base substitution, patch pruning or byte ACK.
use super::*;

impl WriteSession {
    pub fn adopt_namespace_continuation(
        &self,
        handoff: &crate::core::content_saves::NamespaceContinuation,
        context: &ReadContext,
    ) -> Result<WriteSession> {
        context.check()?;
        let previous = handoff.previous();
        let intent = handoff.intent();
        previous.validate()?;
        intent.validate()?;
        ensure!(
            self.journal.root == handoff.store_root(),
            "CONTENT_SAVE_WRONG_STORE"
        );
        ensure!(
            previous.operation == self.operation
                && intent.operation == self.operation
                && intent.branch == previous.branch
                && intent.base.workspace == previous.base.workspace
                && intent.base.entity == previous.base.entity
                && intent.base.content == previous.base.content
                && previous.base.generation.checked_add(2) == Some(intent.base.generation)
                && intent.content_parent == previous.content_parent
                && intent.alive_parent.is_some()
                && intent.alive_parent != previous.alive_parent,
            "CONTENT_SAVE_INTENT_MISMATCH"
        );
        let key = Digest::bytes(&serde_json::to_vec(&(
            "tkfs:namespace-adoption:v1",
            previous,
            intent,
            handoff.checkpoint(),
        ))?);
        let mut db = self.journal.lock(context)?;
        db.execute_batch("PRAGMA synchronous=FULL")?;
        let result = (|| -> Result<(Working, Option<SaveCheckpoint>)> {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let (mut working, saved) = state(&tx, self.operation)?;
            if working.owner != self.owner {
                ensure!(
                    working.intent == *intent
                        && working
                            .namespace
                            .as_ref()
                            .is_some_and(|n| n.from_owner == self.owner && n.previous == key),
                    "STALE_WRITE_SESSION"
                );
                context.check()?;
                tx.commit()?;
                return Ok((working, saved));
            }
            ensure!(
                working.intent == *previous
                    && saved.as_ref().filter(|saved| saved.needs_publication())
                        == handoff.checkpoint(),
                "STALE_WRITE_CHECKPOINT"
            );
            if let Some(saved) = &saved {
                ensure!(
                    recorded_checkpoint(&tx, checkpoint_key(saved)?)? == *saved,
                    "CORRUPT_WRITE_JOURNAL"
                );
            }
            crate::util::fault::trigger("content.write.before_namespace_adoption")?;
            working.intent = intent.clone();
            working.owner = Uuid::new_v4();
            working.namespace = Some(NamespaceAdoption {
                previous: key,
                from_owner: self.owner,
            });
            let next_saved = saved.map(|mut saved| {
                saved.intent = intent.clone();
                saved
            });
            if let Some(saved) = &next_saved {
                remember_checkpoint(&tx, saved)?;
            }
            persist(&tx, &working, next_saved.as_ref(), false)?;
            crate::util::fault::trigger("content.write.namespace_adoption_written")?;
            context.check()?;
            tx.commit()?;
            Ok((working, next_saved))
        })();
        db.progress_handler(0, None::<fn() -> bool>);
        let normal = db.execute_batch("PRAGMA synchronous=NORMAL");
        drop(db);
        context.check()?;
        let (working, saved) = result?;
        normal?;
        crate::util::fault::trigger("content.write.namespace_adopted")?;
        context.check()?;
        Ok(WriteSession {
            journal: self.journal.clone(),
            base_wait: self.base_wait.clone(),
            provider: self.provider.clone(),
            base: self.base.clone(),
            visible_len: AtomicU64::new(working.length),
            visible_basic: Mutex::new(NativeBasicState::from_working(&working, saved.as_ref())),
            visible_sequence: AtomicU64::new(working.sequence),
            operation: self.operation,
            owner: working.owner,
            adoption_work: None,
        })
    }
}
