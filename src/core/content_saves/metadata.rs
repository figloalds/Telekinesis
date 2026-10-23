//! Owned basic-info changes retain a validated byte operation. Ordinary
//! metadata commits still fence it. No provider or private journal I/O here.
use super::*;
mod update;
pub use update::ContentBasicUpdate;

/// Constructible only here, under the outer writer that updates the capture.
pub(in crate::core) struct MetadataOwner {
    capture: Capture,
    selected: Option<SaveCheckpoint>,
    payload: String,
    kind: MetadataKind,
}
enum MetadataKind {
    Basic,
    Rename { replaced: Option<String> },
}
impl MetadataOwner {
    pub(super) fn rename(
        capture: &Capture,
        selected: Option<&SaveCheckpoint>,
        event: &Event,
        replaced: Option<String>,
    ) -> Result<Self> {
        Ok(Self {
            capture: capture.clone(),
            selected: selected.cloned(),
            payload: hash(&serde_json::to_vec(event)?),
            kind: MetadataKind::Rename { replaced },
        })
    }
    pub(super) fn entity(&self) -> &str {
        &self.capture.intent.base.entity
    }
    pub(super) fn check(&self, db: &Connection, event: &Event) -> Result<()> {
        let (capture, selected, receipt) = load(db, self.capture.intent.operation)?;
        let intent = &capture.intent;
        ensure!(
            capture == self.capture
                && selected == self.selected
                && receipt.is_none()
                && active(db)? == (intent.branch.clone(), capture.view_generation)
                && event.branch.id == intent.branch
                && event.repo == intent.base.workspace.repository
                && event.id != capture.event.id
                && event
                    .changes
                    .first()
                    .is_some_and(|change| change.entity == intent.base.entity)
                && hash(&serde_json::to_vec(event)?) == self.payload,
            "CONTENT_METADATA_OWNER_MISMATCH"
        );
        match &self.kind {
            MetadataKind::Basic => ensure!(
                event.changes.len() == 1 && event.changes[0].field == "basic",
                "CONTENT_METADATA_OWNER_MISMATCH"
            ),
            MetadataKind::Rename { replaced } => {
                ensure!(
                    event.changes.len() == 1 + usize::from(replaced.is_some())
                        && event.changes[0].field == "location",
                    "CONTENT_METADATA_OWNER_MISMATCH"
                );
                if let Some(entity) = replaced {
                    let change = &event.changes[1];
                    ensure!(
                        entity != &intent.base.entity
                            && &change.entity == entity
                            && change.field == "alive"
                            && change.value == json!(false),
                        "CONTENT_METADATA_OWNER_MISMATCH"
                    );
                }
            }
        }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    update: Option<update::CheckpointMetadataUpdate>,
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
            update: None,
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
            store.commit_basic_change(&capture, None, &request)
        })
        .and_then(|receipt| {
            crate::util::fault::trigger("content.save.basic_committed")?;
            Ok(receipt)
        })
    }
    fn commit_basic_change(
        &mut self,
        capture: &Capture,
        selected: Option<&SaveCheckpoint>,
        request: &MetadataRequest,
    ) -> Result<ContentMetadataReceipt> {
        let store = self;
        let intent = &request.intent;
        let mutation = request.mutation;
        let basic = request.basic;
        let entry = store.entry(&intent.base.entity)?;
        ensure!(
            (entry.alive || capture.retirement.is_some()) && entry.kind == "file",
            "FILE_NOT_FOUND"
        );
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
            selected: selected.cloned(),
            payload: hash(&serde_json::to_vec(&event)?),
            kind: MetadataKind::Basic,
        };
        crate::util::fault::trigger("content.save.before_basic")?;
        store.commit_owned_metadata(event.clone(), &owner)?;
        let mut next = capture.clone();
        // The acknowledged metadata event causally includes the current
        // frontier; byte/alive parents and private operation stay exact.
        next.event.dependencies = vec![event.id.clone()];
        let mut future = basic;
        if request.update.is_none() {
            future.attributes = BasicInfo::attributes("file", future.attributes | 0x20)?;
        }
        next.event.changes.truncate(2);
        next.event.changes.push(Change {
            entity: entry.id,
            field: "basic".into(),
            parents: vec![event.id.clone()],
            value: json!(future),
        });
        next.basic_policy = Some(request.policy.clone());
        store.replace_pending_capture(capture, &next, selected)?;
        let target = request
            .update
            .as_ref()
            .and_then(|update| update.checkpoint.as_ref())
            .filter(|checkpoint| checkpoint.needs_publication());
        if request.update.is_some() {
            let changed = store.db.execute(
                "UPDATE content_save_operations SET selected=? WHERE operation=? AND capture=? AND selected IS ? AND receipt IS NULL",
                params![target.map(|cp| encode("selection:v1", cp)).transpose()?,
                    intent.operation.to_string(), encode("capture:v1", &next)?,
                    selected.map(|cp| encode("selection:v1", cp)).transpose()?],
            )?;
            ensure!(changed == 1, "CONTENT_METADATA_CONTINUATION_LOST");
        }
        let record = MetadataRecord {
            request: request.clone(),
            event,
        };
        let raw = encode("metadata:v1", &record)?;
        let changed = store.db.execute(
            "INSERT INTO content_save_metadata_changes(mutation,record) VALUES(?,?)",
            params![mutation.to_string(), &raw],
        )?;
        let stored = record_sql(
            store
                .db
                .query_row(
                    "SELECT record FROM content_save_metadata_changes WHERE mutation=?",
                    [mutation.to_string()],
                    |r| blob(r, 0),
                )
                .optional(),
        )?;
        ensure!(
            changed == 1 && stored.as_ref() == Some(&raw),
            "CONTENT_METADATA_RECEIPT_LOST"
        );
        current(store, &next)?;
        let (stored, selection, byte_receipt) = load(&store.db, intent.operation)?;
        ensure!(
            stored == next && selection.as_ref() == target && byte_receipt.is_none(),
            "CONTENT_METADATA_CONTINUATION_LOST"
        );
        let receipt = store.verify_metadata_record(&record)?;
        crate::util::fault::trigger("content.save.basic_written")?;
        Ok(receipt)
    }
    fn verify_metadata_record(&self, record: &MetadataRecord) -> Result<ContentMetadataReceipt> {
        let request = &record.request;
        request.intent.validate()?;
        request.basic.validate()?;
        if let Some(update) = &request.update {
            update.validate(&request.intent)?;
            ensure!(
                request.policy.sequence == update.sequence()
                    && request.policy.preserve_times & update.fields.preserve_times()
                        == update.fields.preserve_times()
                    && update.fields.apply(request.basic)? == request.basic,
                "CORRUPT_CONTENT_SAVE"
            );
        }
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
                && previous.predecessor == next.predecessor
                && previous.retirement == next.retirement,
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
        if let Some(key) = &previous.predecessor
            && !self.is_namespace_head(previous)?
        {
            let changed = self.db.execute(
                "UPDATE content_save_successors SET capture=? WHERE operation=? AND checkpoint=? AND capture=?",
                params![encode("successor:v1", next)?, previous.intent.operation.to_string(), key, encode("successor:v1", previous)?],
            )?;
            ensure!(changed == 1, "CONTENT_METADATA_CONTINUATION_LOST");
            let raw = record_sql(self.db.query_row(
                "SELECT capture FROM content_save_successors WHERE operation=? AND checkpoint=?",
                params![previous.intent.operation.to_string(), key],
                |r| blob(r, 0),
            ))?;
            ensure!(
                decode::<Capture>("successor:v1", &raw)? == *next,
                "CONTENT_METADATA_CONTINUATION_LOST"
            );
        }
        let changed = self.db.execute(
            "UPDATE content_save_operations SET capture=? WHERE operation=? AND capture=? AND selected IS ? AND receipt IS NULL",
            params![encode("capture:v1", next)?, previous.intent.operation.to_string(), old,
                selected.map(|s| encode("selection:v1", s)).transpose()?],
        )?;
        ensure!(changed == 1, "CONTENT_METADATA_CONTINUATION_LOST");
        let (stored, selection, receipt) = load(&self.db, previous.intent.operation)?;
        ensure!(
            stored == *next && selection.as_ref() == selected && receipt.is_none(),
            "CONTENT_METADATA_CONTINUATION_LOST"
        );
        Ok(())
    }
}
