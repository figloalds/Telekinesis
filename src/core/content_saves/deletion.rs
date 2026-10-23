//! Durable file dispositions are separate from the later namespace transaction.
use super::*;

type DeleteRow = (String, String, String, String, Vec<u8>);
fn delete_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DeleteRow> {
    let text = |column, limit| -> rusqlite::Result<String> {
        let value = row.get_ref(column)?.as_str()?;
        if value.len() > limit {
            return Err(rusqlite::Error::InvalidQuery);
        }
        Ok(value.to_owned())
    };
    Ok((
        text(0, 36)?,
        text(1, 128)?,
        text(2, 128)?,
        text(3, 36)?,
        blob(row, 4)?,
    ))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentDeleteIntent {
    pub request: Uuid,
    pub workspace: WorkspaceId,
    pub branch: String,
    pub view_generation: u64,
    pub entity: String,
    pub operation: Uuid,
}
impl ContentDeleteIntent {
    fn validate(&self) -> Result<()> {
        ensure!(
            !self.request.is_nil()
                && !self.operation.is_nil()
                && !self.workspace.workspace.is_nil()
                && !self.workspace.repository.is_empty()
                && self.workspace.repository.len() <= 128
                && !self.workspace.repository.chars().any(char::is_control)
                && !self.branch.is_empty()
                && self.branch.len() <= 128
                && !self.branch.chars().any(char::is_control)
                && !self.entity.is_empty()
                && self.entity.len() <= 128
                && !self.entity.chars().any(char::is_control)
                && self.view_generation <= i64::MAX as u64,
            "CORRUPT_CONTENT_SAVE"
        );
        Ok(())
    }
}
impl Store {
    fn delete_intent_row(&self, row: DeleteRow) -> Result<ContentDeleteIntent> {
        let record: ContentDeleteIntent = decode("delete-intent:v1", &row.4)?;
        record.validate()?;
        ensure!(
            row.0 == record.request.to_string()
                && row.1 == record.branch
                && row.2 == record.entity
                && row.3 == record.operation.to_string()
                && record.workspace.repository == self.repo
                && record.workspace.workspace == workspace(&self.db)?,
            "CORRUPT_CONTENT_SAVE"
        );
        Ok(record)
    }
    fn delete_intent(&self, request: Uuid) -> Result<Option<ContentDeleteIntent>> {
        let row=record_sql(self.db.query_row("SELECT request,branch,entity,operation,record FROM content_delete_intents WHERE request=?",
            [request.to_string()],delete_row).optional())?;
        row.map(|r| self.delete_intent_row(r)).transpose()
    }
    pub fn content_delete_intent(
        &self,
        request: Uuid,
        context: &ReadContext,
    ) -> Result<Option<ContentDeleteIntent>> {
        context.check()?;
        self.content_save_usable()?;
        let _snapshot = if self.db.is_autocommit() {
            Some(self.db.unchecked_transaction()?)
        } else {
            None
        };
        self.delete_intent(request)
    }
    pub fn content_delete_target(
        &self,
        record: &ContentDeleteIntent,
        context: &ReadContext,
    ) -> Result<NamespaceTarget> {
        let _snapshot = if self.db.is_autocommit() {
            Some(self.db.unchecked_transaction()?)
        } else {
            None
        };
        let intent = self.validate_content_delete(record, context)?;
        let mut entry = self.entry(&intent.base.entity)?;
        ensure!(
            entry.alive && entry.kind == "file",
            "STALE_CONTENT_DESTINATION"
        );
        let parent = entry.parent.clone();
        let mut names = Vec::new();
        let mut seen = BTreeSet::new();
        let mut length = 0usize;
        for _ in 0..4096 {
            context.check()?;
            if entry.id == ROOT {
                names.reverse();
                return Ok(NamespaceTarget {
                    path: names.join("/"),
                    parent,
                });
            }
            ensure!(
                entry.alive && seen.insert(entry.id.clone()),
                "STALE_CONTENT_DESTINATION"
            );
            length = length
                .checked_add(entry.name.len() + 1)
                .context("CONTENT_SAVE_CAPTURE_LIMIT")?;
            ensure!(length <= 32_768, "CONTENT_SAVE_CAPTURE_LIMIT");
            names.push(entry.name);
            if entry.parent == ROOT {
                names.reverse();
                return Ok(NamespaceTarget {
                    path: names.join("/"),
                    parent,
                });
            }
            entry = self.entry(&entry.parent)?;
            ensure!(entry.kind == "directory", "NOT_DIRECTORY");
        }
        bail!("CONTENT_SAVE_CAPTURE_LIMIT")
    }
    /// Record disposition before its fallible acknowledgement. This does not
    /// remove a name, select bytes, or publish a save; the handle can cancel it.
    pub fn stage_content_delete(
        &mut self,
        request: Uuid,
        operation: Uuid,
        context: &ReadContext,
    ) -> Result<Option<ContentDeleteIntent>> {
        self.content_save_write(context,|store| {
            let (capture,selected,receipt)=load(&store.db,operation)?;
            current(store,&capture)?;
            ensure!(receipt.is_none(),"CONTENT_METADATA_SAVE_IN_PROGRESS");
            let intent=&capture.intent;
            if capture.retirement.is_some() {return Ok(None);}
            let entry=store.entry(&intent.base.entity)?;
            ensure!(entry.alive && entry.kind=="file","FILE_NOT_FOUND");
            ensure!(entry.basic_info().attributes & 1==0,"ACCESS_DENIED: readonly file");
            let record=ContentDeleteIntent{request,workspace:intent.base.workspace.clone(),branch:intent.branch.clone(),
                view_generation:capture.view_generation,entity:intent.base.entity.clone(),operation};
            record.validate()?;
            ensure!(!store.db.query_row("SELECT EXISTS(SELECT 1 FROM directory_delete_intents WHERE request=?)",[request.to_string()],|r|r.get::<_,bool>(0))?,"CONTENT_METADATA_MUTATION_REUSED");
            ensure!(request.to_string()!=capture.event.id
                && !store.db.query_row("SELECT EXISTS(SELECT 1 FROM content_save_namespace_changes WHERE mutation=?)",[request.to_string()],|r|r.get::<_,bool>(0))?,"CONTENT_METADATA_MUTATION_REUSED");
            if let Some(stored)=store.delete_intent(request)? {
                ensure!(stored==record,"CONTENT_METADATA_MUTATION_REUSED");return Ok(Some(stored));
            }
            let count=store.native_delete_count()?;
            ensure!(count<crate::model::workspace::limits::SAVE_QUEUE as u64,"CONTENT_RESOURCE_LIMIT");
            store.db.execute("INSERT INTO content_delete_intents VALUES(?,?,?,?,?)",params![request.to_string(),record.branch,record.entity,operation.to_string(),encode("delete-intent:v1",&record)?])?;
            ensure!(store.delete_intent(request)?.as_ref()==Some(&record),"CONTENT_METADATA_RECEIPT_LOST");
            store.validate_content_delete_stage(&record,context)?;
            ensure!(load(&store.db,operation)?==(capture,selected,None),"CONTENT_METADATA_RECEIPT_LOST");
            crate::util::fault::trigger("content.save.delete_intent_written")?;
            Ok(Some(record))
        }).and_then(|record| {crate::util::fault::trigger("content.save.delete_intent_committed")?;Ok(record)})
    }
    /// Clear only this handle's exact request before acknowledging cancellation.
    /// A continuation/fence of the byte writer does not prevent safe cancellation.
    pub fn cancel_content_delete(
        &mut self,
        request: Uuid,
        operation: Uuid,
        context: &ReadContext,
    ) -> Result<()> {
        self.content_save_write(context, |store| {
            if let Some(record) = store.delete_intent(request)? {
                ensure!(
                    record.operation == operation,
                    "CONTENT_SAVE_INTENT_MISMATCH"
                );
                ensure!(
                    store.db.execute(
                        "DELETE FROM content_delete_intents WHERE request=? AND record=?",
                        params![request.to_string(), encode("delete-intent:v1", &record)?]
                    )? == 1,
                    "CONTENT_METADATA_RECEIPT_LOST"
                );
            }
            ensure!(
                store.delete_intent(request)?.is_none(),
                "CONTENT_METADATA_RECEIPT_LOST"
            );
            crate::util::fault::trigger("content.save.delete_cancel_written")?;
            Ok(())
        })
        .and_then(|()| {
            crate::util::fault::trigger("content.save.delete_cancel_committed")?;
            Ok(())
        })
    }
    /// Bounded authority read. No history scan, synthesized request or repair.
    pub fn content_delete_intents(
        &self,
        after: Option<Uuid>,
        limit: usize,
        context: &ReadContext,
    ) -> Result<Vec<ContentDeleteIntent>> {
        context.check()?;
        self.content_save_usable()?;
        let _snapshot = if self.db.is_autocommit() {
            Some(self.db.unchecked_transaction()?)
        } else {
            None
        };
        let count = self.native_delete_count()?;
        ensure!(
            count <= crate::model::workspace::limits::SAVE_QUEUE as u64,
            "CORRUPT_CONTENT_SAVE: disposition capacity"
        );
        ensure!((1..=16).contains(&limit), "CONTENT_SAVE_CAPTURE_LIMIT");
        let mut query=self.db.prepare("SELECT request,branch,entity,operation,record FROM content_delete_intents WHERE request>? ORDER BY request LIMIT ?")?;
        let rows = record_sql(
            query
                .query_map(
                    params![
                        after.map_or_else(String::new, |id| id.to_string()),
                        limit as u64
                    ],
                    delete_row,
                )?
                .collect::<rusqlite::Result<Vec<_>>>(),
        )?;
        rows.into_iter()
            .map(|r| self.delete_intent_row(r))
            .collect()
    }
    pub fn inspect_content_delete(
        &self,
        record: &ContentDeleteIntent,
        context: &ReadContext,
    ) -> Result<SaveIntent> {
        self.check_content_delete(record, true, context)
    }
    pub fn validate_content_delete(
        &self,
        record: &ContentDeleteIntent,
        context: &ReadContext,
    ) -> Result<SaveIntent> {
        self.check_content_delete(record, false, context)
    }
    /// Resolve a lost staging reply only if the exact request remains eligible
    /// for acknowledgement in the same read snapshot.
    pub fn validate_content_delete_stage(
        &self,
        record: &ContentDeleteIntent,
        context: &ReadContext,
    ) -> Result<()> {
        let _snapshot = if self.db.is_autocommit() {
            Some(self.db.unchecked_transaction()?)
        } else {
            None
        };
        self.validate_content_delete(record, context)?;
        let (capture, _, _) = load(&self.db, record.operation)?;
        ensure!(capture.retirement.is_none(), "STALE_CONTENT_DESTINATION");
        let entry = self.entry(&record.entity)?;
        ensure!(entry.alive && entry.kind == "file", "FILE_NOT_FOUND");
        ensure!(
            entry.basic_info().attributes & 1 == 0,
            "ACCESS_DENIED: readonly file"
        );
        context.check()
    }
    fn check_content_delete(
        &self,
        record: &ContentDeleteIntent,
        published: bool,
        context: &ReadContext,
    ) -> Result<SaveIntent> {
        context.check()?;
        self.content_save_usable()?;
        record.validate()?;
        let _snapshot = if self.db.is_autocommit() {
            Some(self.db.unchecked_transaction()?)
        } else {
            None
        };
        ensure!(
            self.delete_intent(record.request)?.as_ref() == Some(record),
            "CONTENT_METADATA_RECEIPT_LOST"
        );
        ensure!(
            record.workspace.repository == self.repo
                && record.workspace.workspace == workspace(&self.db)?
                && active(&self.db)? == (record.branch.clone(), record.view_generation),
            "STALE_CONTENT_VIEW"
        );
        let (capture, _, receipt) = load(&self.db, record.operation)?;
        ensure!(
            (published || receipt.is_none())
                && capture.intent.base.entity == record.entity
                && capture.intent.base.workspace == record.workspace
                && capture.intent.branch == record.branch
                && capture.view_generation == record.view_generation,
            "STALE_CONTENT_SAVE"
        );
        if let Some(receipt) = receipt {
            self.verify_content_save_receipt(&capture, &receipt)?;
            let (generation, owner): (u64, Option<String>) = record_sql(self.db.query_row(
                "SELECT generation,owner FROM content_save_entities WHERE branch=? AND entity=?",
                params![record.branch, record.entity],
                |r| Ok((r.get(0)?, r.get(1)?)),
            ))?;
            ensure!(
                Some(generation) == capture.intent.base.generation.checked_add(1)
                    && owner.is_none(),
                "STALE_CONTENT_SAVE"
            );
        } else {
            current(self, &capture)?;
        }
        Ok(capture.intent)
    }
    pub fn content_delete_requested(&self, operation: Uuid, context: &ReadContext) -> Result<bool> {
        context.check()?;
        self.content_save_usable()?;
        let _snapshot = if self.db.is_autocommit() {
            Some(self.db.unchecked_transaction()?)
        } else {
            None
        };
        let (capture, _, _) = load(&self.db, operation)?;
        let mut query=self.db.prepare("SELECT request,branch,entity,operation,record FROM content_delete_intents WHERE branch=? AND entity=? ORDER BY request LIMIT 65")?;
        let rows = record_sql(
            query
                .query_map(
                    params![capture.intent.branch, capture.intent.base.entity],
                    delete_row,
                )?
                .collect::<rusqlite::Result<Vec<_>>>(),
        )?;
        ensure!(
            rows.len() <= crate::model::workspace::limits::SAVE_QUEUE,
            "CONTENT_SAVE_CAPTURE_LIMIT"
        );
        let found = !rows.is_empty();
        for row in rows {
            let record = self.delete_intent_row(row)?;
            ensure!(record.operation == operation, "STALE_CONTENT_SAVE");
            self.validate_content_delete(&record, context)?;
        }
        Ok(found)
    }
    pub(super) fn consume_content_deletes(&self, intent: &SaveIntent) -> Result<()> {
        let mut q=self.db.prepare("SELECT request,branch,entity,operation,record FROM content_delete_intents WHERE branch=? AND entity=? ORDER BY request LIMIT 65")?;
        let rows = record_sql(
            q.query_map(params![intent.branch, intent.base.entity], delete_row)?
                .collect::<rusqlite::Result<Vec<_>>>(),
        )?;
        ensure!(
            rows.len() <= crate::model::workspace::limits::SAVE_QUEUE,
            "CONTENT_SAVE_CAPTURE_LIMIT"
        );
        let mut requests = Vec::new();
        for row in rows {
            let record = self.delete_intent_row(row)?;
            ensure!(record.operation == intent.operation, "STALE_CONTENT_SAVE");
            requests.push(record.request);
        }
        for request in requests {
            ensure!(
                self.db.execute(
                    "DELETE FROM content_delete_intents WHERE request=?",
                    [request.to_string()]
                )? == 1,
                "CONTENT_METADATA_RECEIPT_LOST"
            );
        }
        let left: bool = self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM content_delete_intents WHERE branch=? AND entity=?)",
            params![intent.branch, intent.base.entity],
            |r| r.get(0),
        )?;
        ensure!(!left, "CONTENT_METADATA_RECEIPT_LOST");
        Ok(())
    }
}
