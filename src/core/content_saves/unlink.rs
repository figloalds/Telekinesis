//! Namespace-only removal retains an exact open writer as an unnamed entity.
use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct UnlinkRequest {
    pub(super) destination: NamespaceDestination,
    mutation: Uuid,
    target: NamespaceTarget,
}
impl UnlinkRequest {
    fn validate(&self) -> Result<()> {
        self.destination.intent.validate()?;
        if let Some(checkpoint) = &self.destination.checkpoint {
            checkpoint.validate()?;
            ensure!(
                checkpoint.intent == self.destination.intent,
                "CONTENT_SAVE_INTENT_MISMATCH"
            );
        }
        ensure!(
            self.target.path.len() <= 32_768,
            "CONTENT_SAVE_CAPTURE_LIMIT"
        );
        split_path(&self.target.path)?;
        ensure!(
            !self.target.parent.is_empty()
                && self.target.parent.len() <= 128
                && !self.target.parent.chars().any(char::is_control),
            "CONTENT_SAVE_INTENT_MISMATCH"
        );
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct UnlinkRecord {
    pub(super) request: UnlinkRequest,
    event: Event,
}
impl Store {
    pub(super) fn verify_unlink_record(
        &self,
        record: &UnlinkRecord,
    ) -> Result<ContentMetadataReceipt> {
        record.request.validate()?;
        let request = &record.request;
        let intent = &request.destination.intent;
        let event = &record.event;
        ensure!(
            event.id == request.mutation.to_string()
                && event.repo == self.repo
                && event.repo == intent.base.workspace.repository
                && event.branch.id == intent.branch
                && event.changes.len() == 1
                && event.changes[0].entity == intent.base.entity
                && event.changes[0].field == "alive"
                && event.changes[0].value == json!(false),
            "CORRUPT_CONTENT_SAVE"
        );
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
            operation: intent.operation,
            mutation: request.mutation,
            event: event.id.clone(),
        })
    }
    fn unlink_receipt(&self, request: &UnlinkRequest) -> Result<Option<ContentMetadataReceipt>> {
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
        let record: UnlinkRecord = decode("unlink:v1", &raw)?;
        ensure!(
            record.request == *request,
            "CONTENT_METADATA_MUTATION_REUSED"
        );
        self.verify_unlink_record(&record).map(Some)
    }
    /// Read-only exact receipt lookup after an uncertain namespace reply.
    pub fn content_checkpoint_unlink_receipt(
        &self,
        destination: NamespaceDestination,
        mutation: Uuid,
        target: &NamespaceTarget,
        context: &ReadContext,
    ) -> Result<Option<ContentMetadataReceipt>> {
        context.check()?;
        self.content_save_usable()?;
        let request = UnlinkRequest {
            destination,
            mutation,
            target: target.clone(),
        };
        request.validate()?;
        self.unlink_receipt(&request)
    }
    /// Commit a tombstone and the retained writer's successor together. The
    /// private checkpoint must already be FULL; no bytes are published here.
    pub fn apply_content_checkpoint_unlink(
        &mut self,
        destination: NamespaceDestination,
        mutation: Uuid,
        target: &NamespaceTarget,
        context: &ReadContext,
    ) -> Result<ContentMetadataReceipt> {
        let request = UnlinkRequest {
            destination,
            mutation,
            target: target.clone(),
        };
        request.validate()?;
        self.content_save_write(context, |store| {
            if let Some(receipt) = store.unlink_receipt(&request)? {
                return Ok(receipt);
            }
            let entry = store.lookup(&request.target.path)?;
            ensure!(
                entry.id == request.destination.intent.base.entity
                    && entry.parent == request.target.parent
                    && entry.alive
                    && entry.kind == "file",
                "STALE_CONTENT_DESTINATION"
            );
            let retirement = store.prepare_namespace_retirement(&request.destination, mutation)?;
            let mut event = store.event(
                &store.branch(&request.destination.intent.branch)?,
                vec![store.current_change(&entry.id, "alive", json!(false))?],
            );
            event.id = mutation.to_string();
            ensure!(
                event.dependencies.len() <= FRONTIER_LIMIT,
                "CONTENT_SAVE_CAPTURE_LIMIT"
            );
            crate::util::fault::trigger("content.save.before_unlink")?;
            // The ordinary mutation fences this writer; only the exact retained
            // successor can claim the one fenced generation in this transaction.
            store.commit(event.clone(), true)?;
            store.commit_namespace_retirement(&retirement, &event)?;
            let record = UnlinkRecord {
                request: request.clone(),
                event,
            };
            let raw = encode("unlink:v1", &record)?;
            ensure!(
                store.db.execute(
                    "INSERT INTO content_save_namespace_changes(mutation,record) VALUES(?,?)",
                    params![mutation.to_string(), &raw]
                )? == 1,
                "CONTENT_METADATA_RECEIPT_LOST"
            );
            let stored = record_sql(store.db.query_row(
                "SELECT record FROM content_save_namespace_changes WHERE mutation=?",
                [mutation.to_string()],
                |r| blob(r, 0),
            ))?;
            ensure!(stored == raw, "CONTENT_METADATA_RECEIPT_LOST");
            store.check_namespace_retirement_current(&retirement)?;
            let receipt = store.verify_unlink_record(&record)?;
            crate::util::fault::trigger("content.save.unlink_written")?;
            Ok(receipt)
        })
        .and_then(|receipt| {
            crate::util::fault::trigger("content.save.unlink_committed")?;
            Ok(receipt)
        })
    }
}
