//! Owned basic-info changes retain an unselected byte operation. Ordinary
//! metadata commits still fence it. No provider or private journal I/O here.
use super::*;

/// Constructible only here, under the outer writer that updates the capture.
pub(in crate::core) struct MetadataOwner {
    capture: Capture,
    payload: String,
}
impl MetadataOwner {
    pub(super) fn check(&self, db: &Connection, event: &Event) -> Result<()> {
        let (capture, selected, receipt) = load(db, self.capture.intent.operation)?;
        let intent = &capture.intent;
        ensure!(
            capture == self.capture
                && selected.is_none()
                && receipt.is_none()
                && active(db)? == (intent.branch.clone(), capture.view_generation)
                && event.branch.id == intent.branch
                && event.repo == intent.base.workspace.repository
                && event.id != capture.event.id
                && event.changes.len() == 1
                && event.changes[0].entity == intent.base.entity
                && event.changes[0].field == "basic"
                && hash(&serde_json::to_vec(event)?) == self.payload,
            "CONTENT_METADATA_OWNER_MISMATCH"
        );
        let (generation, owner): (u64, Option<String>) = record_sql(db.query_row(
            "SELECT generation,owner FROM content_save_entities WHERE branch=? AND entity=?",
            params![intent.branch, intent.base.entity],
            |r| Ok((r.get(0)?, r.get(1)?)),
        ))?;
        ensure!(
            generation == intent.base.generation && owner == Some(intent.operation.to_string()),
            "STALE_CONTENT_SAVE"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataRequest {
    intent: SaveIntent,
    mutation: Uuid,
    basic: BasicInfo,
    policy: BasicPolicy,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataRecord {
    request: MetadataRequest,
    event: Event,
}

/// An observed local metadata commit, not authority to retire private bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContentMetadataReceipt {
    pub operation: Uuid,
    pub mutation: Uuid,
    pub event: String,
}

impl Store {
    /// Update basic info without superseding this writer's byte operation.
    /// Only an unselected capture may be rebased; ordinary set_basic still
    /// fences old saves. A later private sequence expires time retention.
    /// The caller owns entity ordering. This owns the FULL WAL transaction
    /// and performs no journal, provider or canonical byte work.
    pub fn apply_content_save_basic(
        &mut self,
        intent: &SaveIntent,
        mutation: Uuid,
        basic: BasicInfo,
        sequence: u64,
        preserve_times: u8,
        context: &ReadContext,
    ) -> Result<ContentMetadataReceipt> {
        intent.validate()?;
        basic.validate()?;
        ensure!(preserve_times & !6 == 0, "INVALID_TIMESTAMP_POLICY");
        let request = MetadataRequest {
            intent: intent.clone(),
            mutation,
            basic,
            policy: BasicPolicy {
                sequence,
                preserve_times,
            },
        };
        self.content_save_write(context, |store| {
            let raw = record_sql(
                store
                    .db
                    .query_row(
                        "SELECT record FROM content_save_metadata_changes WHERE mutation=?",
                        [mutation.to_string()],
                        |r| blob(r, 0),
                    )
                    .optional(),
            )?;
            if let Some(raw) = raw {
                let record: MetadataRecord = decode("metadata:v1", &raw)?;
                ensure!(
                    record.request == request,
                    "CONTENT_METADATA_MUTATION_REUSED"
                );
                return store.verify_metadata_record(&record);
            }
            let (capture, selected, receipt) = load(&store.db, intent.operation)?;
            ensure!(capture.intent == *intent, "CONTENT_SAVE_INTENT_MISMATCH");
            current(store, &capture)?;
            ensure!(
                selected.is_none() && receipt.is_none(),
                "CONTENT_METADATA_SAVE_IN_PROGRESS"
            );
            let entry = store.entry(&intent.base.entity)?;
            ensure!(entry.alive && entry.kind == "file", "FILE_NOT_FOUND");
            ensure!(
                basic.attributes == BasicInfo::attributes(&entry.kind, basic.attributes)?,
                "INVALID_ATTRIBUTES"
            );
            let mut event = store.event(
                &store.branch(&intent.branch)?,
                vec![store.current_change(&entry.id, "basic", json!(basic))?],
            );
            event.id = mutation.to_string();
            ensure!(
                event.dependencies.len() <= FRONTIER_LIMIT,
                "CONTENT_SAVE_CAPTURE_LIMIT"
            );
            let owner = MetadataOwner {
                capture: capture.clone(),
                payload: hash(&serde_json::to_vec(&event)?),
            };
            crate::util::fault::trigger("content.save.before_basic")?;
            store.commit_owned_metadata(event.clone(), &owner)?;
            let mut next = capture.clone();
            // The acknowledged metadata event causally includes the current
            // frontier; byte/alive parents and private operation stay exact.
            next.event.dependencies = vec![event.id.clone()];
            let mut future = basic;
            future.attributes = BasicInfo::attributes("file", future.attributes | 0x20)?;
            next.event.changes.truncate(2);
            next.event.changes.push(Change {
                entity: entry.id,
                field: "basic".into(),
                parents: vec![event.id.clone()],
                value: json!(future),
            });
            next.basic_policy = Some(request.policy.clone());
            store.replace_pending_capture(&capture, &next, None)?;
            let record = MetadataRecord {
                request: request.clone(),
                event,
            };
            store.db.execute(
                "INSERT INTO content_save_metadata_changes(mutation,record) VALUES(?,?)",
                params![mutation.to_string(), encode("metadata:v1", &record)?],
            )?;
            current(store, &next)?;
            let receipt = store.verify_metadata_record(&record)?;
            crate::util::fault::trigger("content.save.basic_written")?;
            Ok(receipt)
        })
        .and_then(|receipt| {
            crate::util::fault::trigger("content.save.basic_committed")?;
            Ok(receipt)
        })
    }
    fn verify_metadata_record(&self, record: &MetadataRecord) -> Result<ContentMetadataReceipt> {
        let request = &record.request;
        request.intent.validate()?;
        request.basic.validate()?;
        let event = &record.event;
        ensure!(
            request.policy.preserve_times & !6 == 0
                && event.id == request.mutation.to_string()
                && event.repo == request.intent.base.workspace.repository
                && event.repo == self.repo
                && event.branch.id == request.intent.branch
                && event.changes.len() == 1
                && event.changes[0].entity == request.intent.base.entity
                && event.changes[0].field == "basic"
                && event.changes[0].value == json!(request.basic),
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
            operation: request.intent.operation,
            mutation: request.mutation,
            event: event.id.clone(),
        })
    }
    pub(super) fn replace_pending_capture(
        &self,
        previous: &Capture,
        next: &Capture,
        selected: Option<&SaveCheckpoint>,
    ) -> Result<()> {
        previous.validate(previous.intent.operation)?;
        next.validate(previous.intent.operation)?;
        ensure!(
            previous.intent == next.intent
                && previous.view_generation == next.view_generation
                && previous.predecessor == next.predecessor,
            "CONTENT_SAVE_INTENT_MISMATCH"
        );
        let old = encode("capture:v1", previous)?;
        ensure!(
            !self.db.query_row(
                "SELECT EXISTS(SELECT 1 FROM events WHERE id=?)",
                [&previous.event.id],
                |r| r.get::<_, bool>(0)
            )?,
            "CONTENT_SAVE_ALREADY_COMMITTED"
        );
        if let Some(key) = &previous.predecessor {
            let changed = self.db.execute(
                "UPDATE content_save_successors SET capture=? WHERE operation=? AND checkpoint=? AND capture=?",
                params![encode("successor:v1", next)?, previous.intent.operation.to_string(), key, encode("successor:v1", previous)?],
            )?;
            ensure!(changed == 1, "CONTENT_METADATA_CONTINUATION_LOST");
        }
        let changed = self.db.execute(
            "UPDATE content_save_operations SET capture=? WHERE operation=? AND capture=? AND selected IS ? AND receipt IS NULL",
            params![encode("capture:v1", next)?, previous.intent.operation.to_string(), old,
                selected.map(|s| encode("selection:v1", s)).transpose()?],
        )?;
        ensure!(changed == 1, "CONTENT_METADATA_CONTINUATION_LOST");
        Ok(())
    }
}
