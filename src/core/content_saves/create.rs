//! Durable typed empty creation and its initial byte owner share one writer.
use super::*;
use crate::model::content::ContentKind;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentCreateRequest {
    pub mutation: Uuid,
    pub entity: Uuid,
    pub operation: Uuid,
    pub branch: String,
    pub view_generation: u64,
    pub target: NamespaceTarget,
    pub basic: BasicInfo,
}
impl ContentCreateRequest {
    fn validate(&self) -> Result<()> {
        ensure!(
            !self.mutation.is_nil()
                && !self.entity.is_nil()
                && !self.operation.is_nil()
                && self.mutation != self.entity
                && self.mutation != self.operation
                && self.entity != self.operation,
            "CONTENT_SAVE_INTENT_MISMATCH"
        );
        ensure!(
            [&self.branch, &self.target.parent]
                .iter()
                .all(|s| !s.is_empty() && s.len() <= 128 && !s.chars().any(char::is_control))
                && self.target.path.len() <= 32_768,
            "CONTENT_SAVE_CAPTURE_LIMIT"
        );
        split_path(&self.target.path)?;
        self.basic.validate()?;
        ensure!(
            self.basic.attributes == BasicInfo::attributes("file", self.basic.attributes)?,
            "INVALID_ATTRIBUTES"
        );
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateRecord {
    request: ContentCreateRequest,
    event: Event,
    intent: SaveIntent,
}
fn empty() -> Result<FileContentRef> {
    FileContentRef::new(ContentKind::Direct, ContentDigest::bytes(&[]), 0)
}
impl Store {
    fn verify_create_record(&self, record: &CreateRecord) -> Result<SaveIntent> {
        record.request.validate()?;
        record.intent.validate()?;
        let request = &record.request;
        let event = &record.event;
        let intent = &record.intent;
        let entity = request.entity.to_string();
        let mutation = request.mutation.to_string();
        let name = split_path(&request.target.path)?.1;
        let expected = [
            ("kind", json!("file")),
            (
                "location",
                json!({"parent":request.target.parent,"name":name}),
            ),
            ("alive", json!(true)),
            ("content", json!(empty()?)),
            ("basic", json!(request.basic)),
        ];
        ensure!(
            event.id == mutation
                && event.repo == self.repo
                && event.branch.id == request.branch
                && event.changes.len() == expected.len()
                && event.reviewed.is_empty()
                && event.object_sizes.is_empty()
                && event.dependencies.len() <= FRONTIER_LIMIT
                && event
                    .changes
                    .iter()
                    .zip(expected)
                    .all(|(change, (field, value))| change.entity == entity
                        && change.field == field
                        && change.value == value
                        && change.parents.is_empty())
                && intent.operation == request.operation
                && intent.branch == request.branch
                && intent.base.entity == entity
                && intent.base.content == empty()?
                && intent.base.generation == 1
                && intent.base.workspace.repository == self.repo
                && intent.base.workspace.workspace == workspace(&self.db)?
                && intent.content_parent.as_deref() == Some(mutation.as_str())
                && intent.alive_parent.as_deref() == Some(mutation.as_str()),
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
        Ok(intent.clone())
    }
    /// Read-only exact receipt; this does not grant current byte ownership.
    pub fn content_create_receipt(
        &self,
        request: &ContentCreateRequest,
        context: &ReadContext,
    ) -> Result<Option<SaveIntent>> {
        context.check()?;
        self.content_save_usable()?;
        request.validate()?;
        let raw = record_sql(
            self.db
                .query_row(
                    "SELECT record FROM content_save_create_changes WHERE mutation=?",
                    [request.mutation.to_string()],
                    |r| blob(r, 0),
                )
                .optional(),
        )?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let record: CreateRecord = decode("create:v1", &raw)?;
        ensure!(
            record.request == *request,
            "CONTENT_METADATA_MUTATION_REUSED"
        );
        self.verify_create_record(&record).map(Some)
    }
    /// The caller installs/pins the canonical empty object before this command.
    /// No file byte I/O or private-journal work occurs under this writer.
    pub fn apply_content_create(
        &mut self,
        request: &ContentCreateRequest,
        context: &ReadContext,
    ) -> Result<SaveIntent> {
        request.validate()?;
        self.content_save_write(context, |store| {
            if let Some(intent) = store.content_create_receipt(request, context)? { return Ok(intent); }
            ensure!(active(&store.db)? == (request.branch.clone(), request.view_generation)
                && store.active == request.branch, "STALE_CONTENT_VIEW");
            let (parent, name) = split_path(&request.target.path)?;
            let directory = store.lookup(&parent)?;
            ensure!(directory.alive && directory.kind == "directory", "NOT_DIRECTORY");
            ensure!(directory.id == request.target.parent, "STALE_CONTENT_DESTINATION");
            store.ensure_directory_accepts_children(&directory.id,context)?;
            match store.lookup(&request.target.path) {
                Ok(_) => bail!("NAME_COLLISION"),
                Err(error) if crate::model::errors::code_of(&error) == "FILE_NOT_FOUND" => {},
                Err(error) => return Err(error),
            }
            let entity = request.entity.to_string();
            match store.entry(&entity) {
                Ok(_) => bail!("CONTENT_METADATA_MUTATION_REUSED"),
                Err(error) if crate::model::errors::code_of(&error) == "FILE_NOT_FOUND" => {},
                Err(error) => return Err(error),
            }
            ensure!(!store.db.query_row("SELECT EXISTS(SELECT 1 FROM events WHERE id IN (?,?)) OR EXISTS(SELECT 1 FROM content_save_operations WHERE operation=?)",
                params![request.mutation.to_string(),request.operation.to_string(),request.operation.to_string()], |r| r.get::<_, bool>(0))?, "CONTENT_METADATA_MUTATION_REUSED");
            let content = empty()?;
            let values = [
                ("kind", json!("file")), ("location", json!({"parent":directory.id,"name":name})),
                ("alive", json!(true)), ("content", json!(content)), ("basic", json!(request.basic)),
            ];
            let changes = values.into_iter().map(|(field,value)| Change {entity:entity.clone(),field:field.into(),parents:vec![],value}).collect();
            let mut event = store.event(&store.branch(&request.branch)?, changes);
            event.id = request.mutation.to_string();
            ensure!(event.dependencies.len() <= FRONTIER_LIMIT, "CONTENT_SAVE_CAPTURE_LIMIT");
            crate::util::fault::trigger("content.create.before_event")?;
            store.commit(event.clone(), true)?;
            crate::util::fault::trigger("content.create.event_written")?;
            let entry = store.entry(&entity)?;
            ensure!(entry.alive && entry.kind == "file" && entry.parent == request.target.parent
                && entry.name == name && entry.content_ref.as_ref() == Some(&content)
                && entry.basic_info() == request.basic, "CONTENT_METADATA_CONTINUATION_LOST");
            let selected = ContentSaveSelection {
                entity, branch: request.branch.clone(), generation: request.view_generation,
                content, content_parent:entry.revisions.get("content").cloned(),
                alive_parent:entry.revisions.get("alive").cloned(),
            };
            let initial_capture = store.begin_content_save_inner(&selected.entity, Some(&selected), request.operation)?;
            let record = CreateRecord { request: request.clone(), event, intent:initial_capture.intent.clone() };
            let raw = encode("create:v1", &record)?;
            let changed = store.db.execute("INSERT INTO content_save_create_changes(mutation,record) VALUES(?,?)", params![request.mutation.to_string(),&raw])?;
            let stored = record_sql(store.db.query_row("SELECT record FROM content_save_create_changes WHERE mutation=?", [request.mutation.to_string()], |r| blob(r, 0)).optional())?;
            ensure!(changed == 1 && stored.as_ref() == Some(&raw), "CONTENT_METADATA_RECEIPT_LOST");
            let (capture, selected, receipt) = load(&store.db, request.operation)?;
            ensure!(capture == initial_capture && selected.is_none() && receipt.is_none(), "CONTENT_METADATA_CONTINUATION_LOST");
            current(store, &capture)?;
            let intent = store.verify_create_record(&record)?;
            crate::util::fault::trigger("content.create.written")?;
            Ok(intent)
        }).and_then(|intent| {
            crate::util::fault::trigger("content.create.committed")?;
            Ok(intent)
        })
    }
    pub(crate) fn check_content_create_owner(
        &self,
        intent: &SaveIntent,
        context: &ReadContext,
    ) -> Result<()> {
        context.check()?;
        self.content_save_usable()?;
        let (capture, selected, receipt) = load(&self.db, intent.operation)?;
        ensure!(
            capture.intent == *intent && selected.is_none() && receipt.is_none(),
            "STALE_CONTENT_SAVE"
        );
        current(self, &capture)
    }
}
