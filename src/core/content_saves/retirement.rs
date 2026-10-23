//! A namespace removal changes ownership without claiming any byte publication.
//! Every record is committed with its namespace event; the journal is separate.
use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceDestination {
    pub intent: SaveIntent,
    pub checkpoint: Option<SaveCheckpoint>,
}
impl NamespaceDestination {
    pub(super) fn validate(&self, source: &SaveIntent) -> Result<()> {
        self.intent.validate()?;
        ensure!(
            self.intent.operation != source.operation
                && self.intent.base.entity != source.base.entity
                && self.intent.branch == source.branch
                && self.intent.base.workspace == source.base.workspace,
            "CONTENT_SAVE_INTENT_MISMATCH"
        );
        if let Some(checkpoint) = &self.checkpoint {
            checkpoint.validate()?;
            ensure!(
                checkpoint.intent == self.intent,
                "CONTENT_SAVE_INTENT_MISMATCH"
            );
        }
        Ok(())
    }
}

/// Opaque proof of a namespace-only handoff. It authorizes no byte retirement,
/// base substitution or patch pruning. Recover through Core after a lost reply.
#[derive(Clone, Debug)]
pub struct NamespaceContinuation {
    previous: SaveIntent,
    checkpoint: Option<SaveCheckpoint>,
    intent: SaveIntent,
    root: PathBuf,
}
impl NamespaceContinuation {
    pub fn previous(&self) -> &SaveIntent {
        &self.previous
    }
    pub fn checkpoint(&self) -> Option<&SaveCheckpoint> {
        self.checkpoint.as_ref()
    }
    pub fn intent(&self) -> &SaveIntent {
        &self.intent
    }
    pub fn store_root(&self) -> &Path {
        &self.root
    }
}

pub(super) struct Retirement {
    previous: Capture,
    next: Capture,
    checkpoint: Option<SaveCheckpoint>,
    mutation: Uuid,
}
impl Store {
    /// Historical retirement evidence, not a current publication grant. A local
    /// restore can still fence this intent; its private bytes must be retained.
    pub fn content_save_is_retired(
        &self,
        intent: &SaveIntent,
        context: &ReadContext,
    ) -> Result<bool> {
        context.check()?;
        self.content_save_usable()?;
        intent.validate()?;
        ensure!(
            self.db.is_autocommit(),
            "CONTENT_SAVE_REQUIRES_OUTER_TRANSACTION"
        );
        let tx = self.db.unchecked_transaction()?;
        let (capture, _, receipt) = load(&self.db, intent.operation)?;
        let (capture, receipt) = if capture.intent == *intent {
            (capture, receipt)
        } else {
            let key: String = record_sql(self.db.query_row(
                "SELECT checkpoint FROM content_save_history WHERE operation=? AND generation=?",
                params![intent.operation.to_string(), intent.base.generation],
                |r| checkpoint_text(r, 0),
            ))?;
            let (capture, _, receipt) =
                history(&self.db, intent.operation, &key)?.context("CORRUPT_CONTENT_SAVE")?;
            ensure!(capture.intent == *intent, "CONTENT_SAVE_INTENT_MISMATCH");
            (capture, Some(receipt))
        };
        ensure!(
            intent.base.workspace.repository == self.repo
                && intent.base.workspace.workspace == workspace(&self.db)?,
            "STALE_CONTENT_WORKSPACE"
        );
        if let Some(receipt) = receipt {
            self.verify_content_save_receipt(&capture, &receipt)?;
        }
        self.verify_capture_retirement(&capture)?;
        context.check()?;
        tx.commit()?;
        Ok(capture.retirement.is_some())
    }
    pub(super) fn is_namespace_head(&self, capture: &Capture) -> Result<bool> {
        let Some(event) = &capture.retirement else {
            return Ok(false);
        };
        Ok(self.db.query_row("SELECT EXISTS(SELECT 1 FROM content_save_namespace_continuations WHERE operation=? AND mutation=? AND generation=?)", params![capture.intent.operation.to_string(),event,capture.intent.base.generation.saturating_sub(2)], |r| r.get(0))?)
    }
    pub(super) fn verify_obsolete_successor(&self, successor: &Capture) -> Result<()> {
        if let Some(retirement) = self.load_namespace_retirement(&successor.intent)? {
            ensure!(retirement.previous == *successor, "CORRUPT_CONTENT_SAVE");
            return self.verify_namespace_retirement_event(&retirement);
        }
        let child_key: Option<String> = record_sql(self.db.query_row("SELECT checkpoint FROM content_save_history WHERE operation=? AND generation=?", params![successor.intent.operation.to_string(),successor.intent.base.generation], |r| checkpoint_text(r,0)).optional())?;
        let child = history(
            &self.db,
            successor.intent.operation,
            &child_key.context("CORRUPT_CONTENT_SAVE")?,
        )?
        .context("CORRUPT_CONTENT_SAVE")?;
        ensure!(child.0 == *successor, "CORRUPT_CONTENT_SAVE");
        self.verify_content_save_receipt(&child.0, &child.2)
    }
    pub(super) fn check_namespace_retirement_current(&self, retirement: &Retirement) -> Result<()> {
        current(self, &retirement.next)?;
        let selected = retirement
            .checkpoint
            .as_ref()
            .filter(|c| c.needs_publication())
            .map(|c| {
                let mut c = c.clone();
                c.intent = retirement.next.intent.clone();
                c
            });
        let (capture, stored, receipt) = load(&self.db, retirement.next.intent.operation)?;
        ensure!(
            capture == retirement.next && stored == selected && receipt.is_none(),
            "CONTENT_METADATA_CONTINUATION_LOST"
        );
        Ok(())
    }
    pub(super) fn prepare_namespace_retirement(
        &mut self,
        destination: &NamespaceDestination,
        mutation: Uuid,
    ) -> Result<Retirement> {
        let (previous, selected, receipt) = load(&self.db, destination.intent.operation)?;
        ensure!(
            previous.intent == destination.intent,
            "CONTENT_SAVE_INTENT_MISMATCH"
        );
        ensure!(receipt.is_none(), "CONTENT_METADATA_SAVE_IN_PROGRESS");
        current(self, &previous)?;
        ensure!(
            previous.retirement.is_none() && mutation.to_string() != previous.event.id,
            "CONTENT_METADATA_OWNER_MISMATCH"
        );
        if let Some(prior) = selected {
            let checkpoint = destination
                .checkpoint
                .as_ref()
                .context("CONTENT_METADATA_SAVE_IN_PROGRESS")?;
            ensure!(
                checkpoint.needs_publication()
                    && (checkpoint.sequence > prior.sequence || checkpoint == &prior),
                "STALE_WRITE_CHECKPOINT"
            );
        }
        let sequence = destination.checkpoint.as_ref().map_or(0, |c| c.sequence);
        ensure!(
            previous
                .basic_policy
                .as_ref()
                .is_none_or(|p| sequence >= p.sequence),
            "STALE_WRITE_CHECKPOINT"
        );
        let entry = self.entry(&destination.intent.base.entity)?;
        ensure!(
            entry.alive && entry.kind == "file",
            "STALE_CONTENT_DESTINATION"
        );
        if let Some(checkpoint) = destination
            .checkpoint
            .as_ref()
            .filter(|c| c.needs_publication())
        {
            self.select_content_checkpoint_inner(checkpoint)?;
        }
        let previous = load(&self.db, destination.intent.operation)?.0;
        let mut next = previous.clone();
        next.intent.base.generation = next
            .intent
            .base
            .generation
            .checked_add(2)
            .filter(|g| *g <= i64::MAX as u64)
            .context("WRITE_GENERATION_EXHAUSTED")?;
        next.intent.alive_parent = Some(mutation.to_string());
        next.retirement = Some(mutation.to_string());
        next.event.id = capture_event_id(
            next.intent.operation,
            next.intent.base.generation,
            next.predecessor.as_deref(),
            next.retirement.as_deref(),
        );
        next.event.dependencies = vec![mutation.to_string()];
        next.event.changes[1].parents = vec![mutation.to_string()];
        next.event.changes[1].value = json!(false);
        next.validate(next.intent.operation)?;
        Ok(Retirement {
            previous,
            next,
            checkpoint: destination.checkpoint.clone(),
            mutation,
        })
    }
    pub(super) fn commit_namespace_retirement(
        &self,
        retirement: &Retirement,
        event: &Event,
    ) -> Result<()> {
        let Retirement {
            previous,
            next,
            checkpoint,
            mutation,
        } = retirement;
        ensure!(
            event.id == mutation.to_string()
                && event
                    .changes
                    .last()
                    .is_some_and(|c| c.entity == previous.intent.base.entity
                        && c.field == "alive"
                        && c.value == json!(false)),
            "CONTENT_METADATA_OWNER_MISMATCH"
        );
        // observe_event fenced the old target. Only this exact successor may
        // claim that one transition; ordinary metadata never bypasses fencing.
        let fenced = previous
            .intent
            .base
            .generation
            .checked_add(1)
            .context("WRITE_GENERATION_EXHAUSTED")?;
        let changed = self.db.execute("UPDATE content_save_entities SET generation=?,owner=? WHERE branch=? AND entity=? AND generation=? AND owner IS NULL", params![next.intent.base.generation, next.intent.operation.to_string(), next.intent.branch, next.intent.base.entity, fenced])?;
        ensure!(changed == 1, "CONTENT_METADATA_CONTINUATION_LOST");
        let selected = checkpoint
            .as_ref()
            .filter(|c| c.needs_publication())
            .map(|c| {
                let mut c = c.clone();
                c.intent = next.intent.clone();
                c
            });
        let changed = self.db.execute("UPDATE content_save_operations SET capture=?,selected=?,receipt=NULL WHERE operation=? AND capture=? AND receipt IS NULL", params![encode("capture:v1",next)?, selected.as_ref().map(|c| encode("selection:v1",c)).transpose()?, next.intent.operation.to_string(), encode("capture:v1",previous)?])?;
        ensure!(changed == 1, "CONTENT_METADATA_CONTINUATION_LOST");
        self.db.execute("INSERT INTO content_save_namespace_continuations(operation,generation,mutation,previous,successor,selected) VALUES(?,?,?,?,?,?)", params![previous.intent.operation.to_string(),previous.intent.base.generation,mutation.to_string(),encode("capture:v1",previous)?,encode("capture:v1",next)?,checkpoint.as_ref().map(|c| encode("selection:v1",c)).transpose()?])?;
        let stored = self
            .load_namespace_retirement(&previous.intent)?
            .context("CONTENT_METADATA_CONTINUATION_LOST")?;
        ensure!(
            stored.previous == *previous
                && stored.next == *next
                && stored.checkpoint == *checkpoint
                && stored.mutation == *mutation,
            "CONTENT_METADATA_CONTINUATION_LOST"
        );
        let (capture, selection, receipt) = load(&self.db, next.intent.operation)?;
        ensure!(
            capture == *next && selection == selected && receipt.is_none(),
            "CONTENT_METADATA_CONTINUATION_LOST"
        );
        let (generation, owner): (u64, Option<String>) = self.db.query_row(
            "SELECT generation,owner FROM content_save_entities WHERE branch=? AND entity=?",
            params![next.intent.branch, next.intent.base.entity],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        ensure!(
            generation == next.intent.base.generation
                && owner == Some(next.intent.operation.to_string()),
            "CONTENT_METADATA_CONTINUATION_LOST"
        );
        self.consume_content_deletes(&previous.intent)?;
        Ok(())
    }
    fn load_namespace_retirement(&self, intent: &SaveIntent) -> Result<Option<Retirement>> {
        let raw = record_sql(self.db.query_row("SELECT mutation,previous,successor,selected FROM content_save_namespace_continuations WHERE operation=? AND generation=?", params![intent.operation.to_string(),intent.base.generation], |r| Ok((r.get::<_,String>(0)?,blob(r,1)?,blob(r,2)?,if r.get_ref(3)?.data_type() == rusqlite::types::Type::Null {None} else {Some(blob(r,3)?)}))).optional())?;
        let Some((mutation, previous, next, checkpoint)) = raw else {
            return Ok(None);
        };
        let previous: Capture = decode("capture:v1", &previous)?;
        let next: Capture = decode("capture:v1", &next)?;
        let checkpoint: Option<SaveCheckpoint> = checkpoint
            .as_deref()
            .map(|b| decode("selection:v1", b))
            .transpose()?;
        previous.validate(intent.operation)?;
        next.validate(intent.operation)?;
        ensure!(
            previous.intent == *intent && previous.retirement.is_none(),
            "CORRUPT_CONTENT_SAVE"
        );
        let mutation = Uuid::parse_str(&mutation).context("CORRUPT_CONTENT_SAVE")?;
        let mut expected = previous.clone();
        expected.intent.base.generation = intent
            .base
            .generation
            .checked_add(2)
            .context("CORRUPT_CONTENT_SAVE")?;
        expected.intent.alive_parent = Some(mutation.to_string());
        expected.retirement = Some(mutation.to_string());
        expected.event.id = capture_event_id(
            intent.operation,
            expected.intent.base.generation,
            expected.predecessor.as_deref(),
            expected.retirement.as_deref(),
        );
        expected.event.dependencies = vec![mutation.to_string()];
        expected.event.changes[1].parents = vec![mutation.to_string()];
        expected.event.changes[1].value = json!(false);
        ensure!(next == expected, "CORRUPT_CONTENT_SAVE");
        if let Some(checkpoint) = &checkpoint {
            checkpoint.validate()?;
            ensure!(checkpoint.intent == *intent, "CORRUPT_CONTENT_SAVE");
        }
        Ok(Some(Retirement {
            previous,
            next,
            checkpoint,
            mutation,
        }))
    }
    fn verify_namespace_retirement_event(&self, retirement: &Retirement) -> Result<()> {
        let raw = record_sql(self.db.query_row(
            "SELECT record FROM content_save_namespace_changes WHERE mutation=?",
            [retirement.mutation.to_string()],
            |r| blob(r, 0),
        ))?;
        if let Ok(record) = decode::<unlink::UnlinkRecord>("unlink:v1", &raw) {
            self.verify_unlink_record(&record)?;
            ensure!(
                record.request.destination
                    == NamespaceDestination {
                        intent: retirement.previous.intent.clone(),
                        checkpoint: retirement.checkpoint.clone()
                    },
                "CORRUPT_CONTENT_SAVE"
            );
        } else {
            let record: namespace::RenameRecord = decode("rename:v1", &raw)?;
            self.verify_rename_record(&record)?;
            ensure!(
                record.request.destination.as_ref()
                    == Some(&NamespaceDestination {
                        intent: retirement.previous.intent.clone(),
                        checkpoint: retirement.checkpoint.clone()
                    }),
                "CORRUPT_CONTENT_SAVE"
            );
        }
        Ok(())
    }
    pub(super) fn verify_capture_retirement(&self, capture: &Capture) -> Result<()> {
        let Some(event) = &capture.retirement else {
            return Ok(());
        };
        let generation: u64 = record_sql(self.db.query_row("SELECT generation FROM content_save_namespace_continuations WHERE operation=? AND mutation=?", params![capture.intent.operation.to_string(),event], |r| r.get(0)))?;
        let mut previous = capture.intent.clone();
        previous.base.generation = generation;
        // Read the original identity rather than infer it from later bases.
        let raw = record_sql(self.db.query_row("SELECT previous FROM content_save_namespace_continuations WHERE operation=? AND generation=?",params![previous.operation.to_string(),generation],|r|blob(r,0)))?;
        previous = decode::<Capture>("capture:v1", &raw)?.intent;
        let retirement = self
            .load_namespace_retirement(&previous)?
            .context("CORRUPT_CONTENT_SAVE")?;
        self.verify_namespace_retirement_event(&retirement)?;
        ensure!(
            capture.retirement == retirement.next.retirement
                && capture.intent.operation == previous.operation
                && capture.intent.branch == previous.branch
                && capture.intent.base.entity == previous.base.entity
                && capture.intent.base.workspace == previous.base.workspace
                && capture.intent.base.generation >= retirement.next.intent.base.generation,
            "CORRUPT_CONTENT_SAVE"
        );
        Ok(())
    }
    /// Read-only recovery of the exact retired destination. Never creates or
    /// repeats a namespace mutation, and does not require the owner still live.
    pub fn content_namespace_continuation(
        &self,
        previous: &SaveIntent,
        context: &ReadContext,
    ) -> Result<Option<NamespaceContinuation>> {
        context.check()?;
        self.content_save_usable()?;
        previous.validate()?;
        ensure!(
            self.db.is_autocommit(),
            "CONTENT_SAVE_REQUIRES_OUTER_TRANSACTION"
        );
        let tx = self.db.unchecked_transaction()?;
        let Some(retirement) = self.load_namespace_retirement(previous)? else {
            context.check()?;
            tx.commit()?;
            return Ok(None);
        };
        self.verify_namespace_retirement_event(&retirement)?;
        ensure!(
            self.repo == previous.base.workspace.repository
                && workspace(&self.db)? == previous.base.workspace.workspace,
            "STALE_CONTENT_WORKSPACE"
        );
        context.check()?;
        tx.commit()?;
        Ok(Some(NamespaceContinuation {
            previous: previous.clone(),
            checkpoint: retirement.checkpoint,
            intent: retirement.next.intent,
            root: self.root.canonicalize()?,
        }))
    }
}
