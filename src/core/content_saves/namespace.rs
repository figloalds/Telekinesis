//! Owned file renames preserve byte-save ownership and exact retry identity.
use super::*;

/// The observed parent identity is part of the exact namespace request. A path
/// can resolve to a different directory while private checkpoint I/O runs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceTarget {
    pub path: String,
    pub parent: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RenameRequest {
    intent: SaveIntent,
    mutation: Uuid,
    checkpoint: Option<SaveCheckpoint>,
    target: String,
    replace: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expected_parent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) destination: Option<NamespaceDestination>,
}
impl RenameRequest {
    fn validate(&self) -> Result<()> {
        self.intent.validate()?;
        ensure!(self.target.len() <= 32_768, "CONTENT_SAVE_CAPTURE_LIMIT");
        split_path(&self.target)?;
        if let Some(parent) = &self.expected_parent {
            ensure!(
                !parent.is_empty() && parent.len() <= 128 && !parent.chars().any(char::is_control),
                "CONTENT_SAVE_INTENT_MISMATCH"
            );
        }
        if let Some(checkpoint) = &self.checkpoint {
            checkpoint.validate()?;
            ensure!(
                checkpoint.intent == self.intent,
                "CONTENT_SAVE_INTENT_MISMATCH"
            );
        }
        if let Some(destination) = &self.destination {
            destination.validate(&self.intent)?;
            ensure!(self.replace, "CONTENT_SAVE_INTENT_MISMATCH");
        }
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RenameRecord {
    pub(super) request: RenameRequest,
    pub(super) event: Event,
}
impl Store {
    fn rename_receipt(&self, request: &RenameRequest) -> Result<Option<ContentMetadataReceipt>> {
        let raw = record_sql(
            self.db
                .query_row(
                    "SELECT record FROM content_save_namespace_changes WHERE mutation=?",
                    [request.mutation.to_string()],
                    |r| blob(r, 0),
                )
                .optional(),
        )?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let record: RenameRecord = decode("rename:v1", &raw)?;
        ensure!(
            record.request == *request,
            "CONTENT_METADATA_MUTATION_REUSED"
        );
        self.verify_rename_record(&record).map(Some)
    }
    pub(super) fn verify_rename_record(
        &self,
        record: &RenameRecord,
    ) -> Result<ContentMetadataReceipt> {
        let request = &record.request;
        request.validate()?;
        let event = &record.event;
        let name = split_path(&request.target)?.1;
        ensure!(
            event.id == request.mutation.to_string()
                && event.repo == self.repo
                && event.repo == request.intent.base.workspace.repository
                && event.branch.id == request.intent.branch
                && (1..=2).contains(&event.changes.len()),
            "CORRUPT_CONTENT_SAVE"
        );
        let location = &event.changes[0];
        ensure!(
            location.entity == request.intent.base.entity
                && location.field == "location"
                && location.value.get("name").and_then(Value::as_str) == Some(name.as_str())
                && location
                    .value
                    .get("parent")
                    .and_then(Value::as_str)
                    .is_some(),
            "CORRUPT_CONTENT_SAVE"
        );
        ensure!(
            request
                .expected_parent
                .as_ref()
                .is_none_or(
                    |parent| location.value.get("parent").and_then(Value::as_str)
                        == Some(parent.as_str())
                ),
            "CORRUPT_CONTENT_SAVE"
        );
        if let Some(replaced) = event.changes.get(1) {
            ensure!(
                request.replace
                    && replaced.entity != request.intent.base.entity
                    && replaced.field == "alive"
                    && replaced.value == json!(false),
                "CORRUPT_CONTENT_SAVE"
            );
        }
        if let Some(destination) = &request.destination {
            ensure!(
                event
                    .changes
                    .get(1)
                    .is_some_and(|c| c.entity == destination.intent.base.entity),
                "CORRUPT_CONTENT_SAVE"
            );
        }
        let payload: String = record_sql(self.db.query_row(
            "SELECT payload FROM events WHERE id=?",
            [&event.id],
            |r| r.get(0),
        ))?;
        ensure!(
            payload == serde_json::to_string(event)?,
            "CORRUPT_CONTENT_SAVE"
        );
        Ok(ContentMetadataReceipt {
            operation: request.intent.operation,
            mutation: request.mutation,
            event: event.id.clone(),
        })
    }
    /// Read-only exact receipt lookup. Absence never retries a failed rename.
    pub fn content_checkpoint_rename_receipt(
        &self,
        intent: &SaveIntent,
        mutation: Uuid,
        checkpoint: Option<&SaveCheckpoint>,
        target: &str,
        replace: bool,
        context: &ReadContext,
    ) -> Result<Option<ContentMetadataReceipt>> {
        context.check()?;
        self.content_save_usable()?;
        let request = RenameRequest {
            intent: intent.clone(),
            mutation,
            checkpoint: checkpoint.cloned(),
            target: target.to_owned(),
            replace,
            expected_parent: None,
            destination: None,
        };
        request.validate()?;
        self.rename_receipt(&request)
    }
    /// Publish only the namespace event, anchored to the caller's FULL Saved
    /// revision (or clean operation). No provider/private byte I/O occurs here.
    pub fn apply_content_checkpoint_rename(
        &mut self,
        intent: &SaveIntent,
        mutation: Uuid,
        checkpoint: Option<&SaveCheckpoint>,
        target: &str,
        replace: bool,
        context: &ReadContext,
    ) -> Result<ContentMetadataReceipt> {
        let request = RenameRequest {
            intent: intent.clone(),
            mutation,
            checkpoint: checkpoint.cloned(),
            target: target.to_owned(),
            replace,
            expected_parent: None,
            destination: None,
        };
        self.apply_checkpoint_rename_request(request, context)
    }
    /// Replace an exact open destination and retain its unpublished Saved bytes.
    /// Both adapters must hold their entity gates before checkpointing/calling.
    pub fn apply_content_checkpoint_replace(
        &mut self,
        intent: &SaveIntent,
        mutation: Uuid,
        checkpoint: Option<&SaveCheckpoint>,
        target: &str,
        destination: NamespaceDestination,
        context: &ReadContext,
    ) -> Result<ContentMetadataReceipt> {
        self.apply_checkpoint_rename_request(
            RenameRequest {
                intent: intent.clone(),
                mutation,
                checkpoint: checkpoint.cloned(),
                target: target.to_owned(),
                replace: true,
                expected_parent: None,
                destination: Some(destination),
            },
            context,
        )
    }
    pub fn content_checkpoint_replace_receipt(
        &self,
        intent: &SaveIntent,
        mutation: Uuid,
        checkpoint: Option<&SaveCheckpoint>,
        target: &str,
        destination: NamespaceDestination,
        context: &ReadContext,
    ) -> Result<Option<ContentMetadataReceipt>> {
        context.check()?;
        self.content_save_usable()?;
        let request = RenameRequest {
            intent: intent.clone(),
            mutation,
            checkpoint: checkpoint.cloned(),
            target: target.to_owned(),
            replace: true,
            expected_parent: None,
            destination: Some(destination),
        };
        request.validate()?;
        self.rename_receipt(&request)
    }
    /// Exact observed-parent rename, optionally retiring an owned destination.
    pub fn apply_content_checkpoint_rename_at(
        &mut self,
        intent: &SaveIntent,
        mutation: Uuid,
        checkpoint: Option<&SaveCheckpoint>,
        target: &NamespaceTarget,
        destination: Option<NamespaceDestination>,
        context: &ReadContext,
    ) -> Result<ContentMetadataReceipt> {
        self.apply_checkpoint_rename_request(
            RenameRequest {
                intent: intent.clone(),
                mutation,
                checkpoint: checkpoint.cloned(),
                target: target.path.clone(),
                replace: destination.is_some(),
                expected_parent: Some(target.parent.clone()),
                destination,
            },
            context,
        )
    }
    /// Read-only lookup binds a lost reply to the same parent observation.
    pub fn content_checkpoint_rename_at_receipt(
        &self,
        intent: &SaveIntent,
        mutation: Uuid,
        checkpoint: Option<&SaveCheckpoint>,
        target: &NamespaceTarget,
        destination: Option<NamespaceDestination>,
        context: &ReadContext,
    ) -> Result<Option<ContentMetadataReceipt>> {
        context.check()?;
        self.content_save_usable()?;
        let request = RenameRequest {
            intent: intent.clone(),
            mutation,
            checkpoint: checkpoint.cloned(),
            target: target.path.clone(),
            replace: destination.is_some(),
            expected_parent: Some(target.parent.clone()),
            destination,
        };
        request.validate()?;
        self.rename_receipt(&request)
    }
    fn apply_checkpoint_rename_request(
        &mut self,
        request: RenameRequest,
        context: &ReadContext,
    ) -> Result<ContentMetadataReceipt> {
        request.validate()?;
        let intent = &request.intent;
        let checkpoint = request.checkpoint.as_ref();
        let mutation = request.mutation;
        let target = request.target.as_str();
        let replace = request.replace;
        self.content_save_write(context, |store| {
            if let Some(receipt) = store.rename_receipt(&request)? { return Ok(receipt); }
            let (capture, selected, receipt) = load(&store.db, intent.operation)?;
            ensure!(capture.intent == *intent, "CONTENT_SAVE_INTENT_MISMATCH");
            ensure!(receipt.is_none(), "CONTENT_METADATA_SAVE_IN_PROGRESS");
            current(store, &capture)?;
            if let Some(prior) = &selected {
                let checkpoint = checkpoint.context("CONTENT_METADATA_SAVE_IN_PROGRESS")?;
                ensure!(checkpoint.needs_publication() && (checkpoint.sequence > prior.sequence || checkpoint == prior), "STALE_WRITE_CHECKPOINT");
            }
            let sequence = checkpoint.map_or(0, |c| c.sequence);
            ensure!(capture.basic_policy.as_ref().is_none_or(|p| sequence >= p.sequence), "STALE_WRITE_CHECKPOINT");
            if let Some(checkpoint) = checkpoint.filter(|c| c.needs_publication()) {
                store.select_content_checkpoint_inner(checkpoint)?;
            }
            let (capture, selected, _) = load(&store.db, intent.operation)?;
            let retirement = request.destination.as_ref().map(|destination| store.prepare_namespace_retirement(destination, mutation)).transpose()?;
            let source = store.entry(&intent.base.entity)?;
            ensure!(source.alive && source.kind == "file", "FILE_NOT_FOUND");
            let (parent, name) = split_path(target)?;
            let directory = store.lookup(&parent)?;
            ensure!(directory.alive && directory.kind == "directory", "NOT_DIRECTORY");
            store.ensure_directory_accepts_children(&directory.id,context)?;
            ensure!(request.expected_parent.as_ref().is_none_or(|parent| *parent == directory.id), "STALE_CONTENT_DESTINATION");
            let mut changes = vec![store.current_change(&source.id, "location", json!({"parent":directory.id,"name":name}))?];
            let mut replaced = None;
            match store.lookup(target) {
                Ok(existing) if existing.id != source.id => {
                    ensure!(replace && existing.kind == "file", "NAME_COLLISION");
                    ensure!(request.destination.as_ref().is_none_or(|d| d.intent.base.entity == existing.id), "STALE_CONTENT_DESTINATION");
                    changes.push(store.current_change(&existing.id, "alive", json!(false))?);
                    replaced = Some(existing.id);
                }
                Ok(_) => {ensure!(retirement.is_none(), "STALE_CONTENT_DESTINATION");}
                Err(error) if crate::model::errors::code_of(&error) == "FILE_NOT_FOUND" => {ensure!(retirement.is_none(), "STALE_CONTENT_DESTINATION");}
                Err(error) => return Err(error),
            }
            let mut event = store.event(&store.branch(&intent.branch)?, changes);
            event.id = mutation.to_string();
            ensure!(event.dependencies.len() <= FRONTIER_LIMIT, "CONTENT_SAVE_CAPTURE_LIMIT");
            let owner = MetadataOwner::rename(&capture, selected.as_ref(), &event, replaced)?;
            crate::util::fault::trigger("content.save.before_rename")?;
            store.commit_owned_metadata(event.clone(), &owner)?;
            if let Some(retirement) = &retirement {
                store.commit_namespace_retirement(retirement, &event)?;
            }
            let mut next = capture.clone();
            next.event.dependencies = vec![event.id.clone()];
            store.replace_pending_capture(&capture, &next, selected.as_ref())?;
            let target_selection = checkpoint.filter(|c| c.needs_publication());
            let changed = store.db.execute(
                "UPDATE content_save_operations SET selected=? WHERE operation=? AND capture=? AND selected IS ? AND receipt IS NULL",
                params![target_selection.map(|c| encode("selection:v1", c)).transpose()?, intent.operation.to_string(), encode("capture:v1", &next)?, selected.as_ref().map(|c| encode("selection:v1", c)).transpose()?]
            )?;
            ensure!(changed == 1, "CONTENT_METADATA_CONTINUATION_LOST");
            let record = RenameRecord { request: request.clone(), event };
            let raw = encode("rename:v1", &record)?;
            let changed = store.db.execute("INSERT INTO content_save_namespace_changes(mutation,record) VALUES(?,?)", params![mutation.to_string(), &raw])?;
            let stored = record_sql(store.db.query_row("SELECT record FROM content_save_namespace_changes WHERE mutation=?", [mutation.to_string()], |r| blob(r, 0)).optional())?;
            ensure!(changed == 1 && stored.as_ref() == Some(&raw), "CONTENT_METADATA_RECEIPT_LOST");
            if let Some(retirement) = &retirement {
                store.check_namespace_retirement_current(retirement)?;
            }
            current(store, &next)?;
            let (stored, selection, byte_receipt) = load(&store.db, intent.operation)?;
            ensure!(stored == next && selection.as_ref() == target_selection && byte_receipt.is_none(), "CONTENT_METADATA_CONTINUATION_LOST");
            let receipt = store.verify_rename_record(&record)?;
            crate::util::fault::trigger("content.save.rename_written")?;
            Ok(receipt)
        }).and_then(|receipt| {
            crate::util::fault::trigger("content.save.rename_committed")?;
            Ok(receipt)
        })
    }
}
