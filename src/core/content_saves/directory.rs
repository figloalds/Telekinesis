//! Directory namespace durability has no private byte owner or save checkpoint.
use super::*;
mod changes;
mod deletion;
pub use changes::{
    DirectoryMutation, DirectoryMutationReceipt, DirectoryMutationRequest, DirectorySnapshot,
};
pub use deletion::DirectoryDeleteIntent;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectoryCreateRequest {
    pub mutation: Uuid,
    pub entity: Uuid,
    pub workspace: WorkspaceId,
    pub branch: String,
    pub view_generation: u64,
    pub target: NamespaceTarget,
    pub basic: BasicInfo,
}
impl DirectoryCreateRequest {
    fn validate(&self) -> Result<()> {
        ensure!(
            !self.mutation.is_nil()
                && !self.entity.is_nil()
                && self.mutation != self.entity
                && !self.workspace.workspace.is_nil(),
            "CONTENT_SAVE_INTENT_MISMATCH"
        );
        ensure!(
            [
                &self.workspace.repository,
                &self.branch,
                &self.target.parent
            ]
            .iter()
            .all(|s| !s.is_empty() && s.len() <= 128 && !s.chars().any(char::is_control))
                && self.target.path.len() <= 32_768
                && self.view_generation <= i64::MAX as u64,
            "CONTENT_SAVE_CAPTURE_LIMIT"
        );
        split_path(&self.target.path)?;
        self.basic.validate()?;
        ensure!(
            self.basic.attributes == BasicInfo::attributes("directory", self.basic.attributes)?,
            "INVALID_ATTRIBUTES"
        );
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectoryCreateReceipt {
    pub mutation: Uuid,
    pub entity: String,
    pub event: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectoryCreateRecord {
    request: DirectoryCreateRequest,
    event: Event,
}
fn values(request: &DirectoryCreateRequest) -> Result<[(&'static str, Value); 4]> {
    let name = split_path(&request.target.path)?.1;
    Ok([
        ("kind", json!("directory")),
        (
            "location",
            json!({"parent":request.target.parent,"name":name}),
        ),
        ("alive", json!(true)),
        ("basic", json!(request.basic)),
    ])
}
impl Store {
    fn directory_create_record(
        &self,
        request: &DirectoryCreateRequest,
    ) -> Result<Option<DirectoryCreateRecord>> {
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
        let record: DirectoryCreateRecord = decode("directory-create:v1", &raw)?;
        ensure!(
            record.request == *request,
            "CONTENT_METADATA_MUTATION_REUSED"
        );
        Ok(Some(record))
    }
    fn verify_directory_create(
        &self,
        record: &DirectoryCreateRecord,
    ) -> Result<DirectoryCreateReceipt> {
        let request = &record.request;
        request.validate()?;
        ensure!(
            request.workspace.repository == self.repo
                && request.workspace.workspace == workspace(&self.db)?,
            "STALE_CONTENT_WORKSPACE"
        );
        let entity = request.entity.to_string();
        let event = &record.event;
        ensure!(
            event.id == request.mutation.to_string()
                && event.repo == self.repo
                && event.branch.id == request.branch
                && event.changes.len() == 4
                && event.reviewed.is_empty()
                && event.object_sizes.is_empty()
                && event.dependencies.len() <= FRONTIER_LIMIT
                && event.changes.iter().zip(values(request)?).all(
                    |(change, (field, value))| change.entity == entity
                        && change.field == field
                        && change.value == value
                        && change.parents.is_empty()
                ),
            "CORRUPT_CONTENT_SAVE"
        );
        let payload: String = record_sql(self.db.query_row(
            "SELECT payload FROM events WHERE id=?",
            [&event.id],
            |r| {
                let text = r.get_ref(0)?.as_str()?;
                if text.len() > RECORD_BYTES {
                    return Err(rusqlite::Error::InvalidQuery);
                }
                Ok(text.to_owned())
            },
        ))?;
        ensure!(
            payload == serde_json::to_string(event)?,
            "CORRUPT_CONTENT_SAVE"
        );
        Ok(DirectoryCreateReceipt {
            mutation: request.mutation,
            entity,
            event: event.id.clone(),
        })
    }
    /// Historical exact proof, never a grant to create or reopen a later name.
    pub fn directory_create_receipt(
        &self,
        request: &DirectoryCreateRequest,
        context: &ReadContext,
    ) -> Result<Option<DirectoryCreateReceipt>> {
        context.check()?;
        self.content_save_usable()?;
        request.validate()?;
        let _snapshot = if self.db.is_autocommit() {
            Some(self.db.unchecked_transaction()?)
        } else {
            None
        };
        let receipt = self
            .directory_create_record(request)?
            .as_ref()
            .map(|record| self.verify_directory_create(record))
            .transpose()?;
        context.check()?;
        Ok(receipt)
    }
    /// Revalidate current authority before acknowledging a newly created handle.
    pub fn validate_directory_create(
        &self,
        request: &DirectoryCreateRequest,
        context: &ReadContext,
    ) -> Result<Entry> {
        context.check()?;
        self.content_save_usable()?;
        request.validate()?;
        let _snapshot = if self.db.is_autocommit() {
            Some(self.db.unchecked_transaction()?)
        } else {
            None
        };
        let record = self
            .directory_create_record(request)?
            .context("CONTENT_METADATA_RECEIPT_LOST")?;
        self.verify_directory_create(&record)?;
        ensure!(
            active(&self.db)? == (request.branch.clone(), request.view_generation)
                && self.active == request.branch,
            "STALE_CONTENT_VIEW"
        );
        let entry = self.entry(&request.entity.to_string())?;
        ensure!(
            entry.alive
                && entry.kind == "directory"
                && entry.content.is_none()
                && entry.content_ref.is_none()
                && entry.parent == request.target.parent
                && entry.name == split_path(&request.target.path)?.1
                && entry.basic_info() == request.basic
                && ["kind", "location", "alive", "basic"]
                    .iter()
                    .all(|field| entry.revisions.get(*field) == Some(&record.event.id))
                && self.lookup(&request.target.path)?.id == entry.id,
            "STALE_CONTENT_VIEW"
        );
        context.check()?;
        Ok(entry)
    }
    /// One FULL metadata commit; there is no file content or journal operation.
    pub fn apply_directory_create(
        &mut self,
        request: &DirectoryCreateRequest,
        context: &ReadContext,
    ) -> Result<DirectoryCreateReceipt> {
        request.validate()?;
        self.content_save_write(context, |store| {
            if let Some(receipt) = store.directory_create_receipt(request,context)? {return Ok(receipt);}
            ensure!(request.workspace.repository == store.repo
                && request.workspace.workspace == workspace(&store.db)?, "STALE_CONTENT_WORKSPACE");
            ensure!(active(&store.db)? == (request.branch.clone(),request.view_generation)
                && store.active == request.branch, "STALE_CONTENT_VIEW");
            let parent=store.lookup(&split_path(&request.target.path)?.0)?;
            ensure!(parent.alive && parent.kind == "directory", "NOT_DIRECTORY");
            ensure!(parent.id == request.target.parent, "STALE_CONTENT_DESTINATION");
            store.ensure_directory_accepts_children(&parent.id,context)?;
            match store.lookup(&request.target.path) {
                Ok(_) => bail!("NAME_COLLISION"),
                Err(error) if crate::model::errors::code_of(&error) == "FILE_NOT_FOUND" => {},
                Err(error) => return Err(error),
            }
            let entity=request.entity.to_string();
            match store.entry(&entity) {
                Ok(_) => bail!("CONTENT_METADATA_MUTATION_REUSED"),
                Err(error) if crate::model::errors::code_of(&error) == "FILE_NOT_FOUND" => {},
                Err(error) => return Err(error),
            }
            let mutation=request.mutation.to_string();
            ensure!(!store.db.query_row("SELECT EXISTS(SELECT 1 FROM events WHERE id=?) OR EXISTS(SELECT 1 FROM content_save_operations WHERE operation=?) OR EXISTS(SELECT 1 FROM content_delete_intents WHERE request=?) OR EXISTS(SELECT 1 FROM content_save_namespace_changes WHERE mutation=?) OR EXISTS(SELECT 1 FROM content_save_metadata_changes WHERE mutation=?)",
                params![mutation,mutation,mutation,mutation,mutation], |r| r.get::<_,bool>(0))?, "CONTENT_METADATA_MUTATION_REUSED");
            let changes=values(request)?.into_iter().map(|(field,value)| Change {entity:entity.clone(),field:field.into(),parents:vec![],value}).collect();
            let mut event=store.event(&store.branch(&request.branch)?,changes);
            event.id=mutation.clone();
            ensure!(event.dependencies.len()<=FRONTIER_LIMIT,"CONTENT_SAVE_CAPTURE_LIMIT");
            crate::util::fault::trigger("content.directory.before_event")?;
            store.commit(event.clone(),true)?;
            crate::util::fault::trigger("content.directory.event_written")?;
            let record=DirectoryCreateRecord {request:request.clone(),event};
            let raw=encode("directory-create:v1",&record)?;
            ensure!(store.db.execute("INSERT INTO content_save_create_changes(mutation,record) VALUES(?,?)",params![mutation,&raw])?==1,"CONTENT_METADATA_RECEIPT_LOST");
            let stored=record_sql(store.db.query_row("SELECT record FROM content_save_create_changes WHERE mutation=?",[mutation],|r|blob(r,0)).optional())?;
            ensure!(stored.as_ref()==Some(&raw),"CONTENT_METADATA_RECEIPT_LOST");
            let receipt=store.verify_directory_create(&record)?;
            store.validate_directory_create(request,context)?;
            crate::util::fault::trigger("content.directory.written")?;
            Ok(receipt)
        }).and_then(|receipt| {crate::util::fault::trigger("content.directory.committed")?;Ok(receipt)})
    }
}
