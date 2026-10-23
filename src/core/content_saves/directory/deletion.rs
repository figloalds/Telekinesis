//! Bounded per-handle directory dispositions, independent of private byte saves.
use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectoryDeleteIntent {
    pub request: Uuid,
    pub snapshot: DirectorySnapshot,
}
impl DirectoryDeleteIntent {
    fn validate(&self) -> Result<()> {
        ensure!(
            !self.request.is_nil() && self.snapshot.entry.alive,
            "CORRUPT_CONTENT_SAVE"
        );
        self.snapshot.validate()
    }
}
type Row = (String, String, String, Vec<u8>);
fn row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Row> {
    let text = |i, limit| -> rusqlite::Result<String> {
        let s = r.get_ref(i)?.as_str()?;
        if s.len() > limit {
            return Err(rusqlite::Error::InvalidQuery);
        }
        Ok(s.to_owned())
    };
    Ok((text(0, 36)?, text(1, 128)?, text(2, 128)?, blob(r, 3)?))
}
impl Store {
    pub(in crate::core::content_saves) fn native_delete_count(&self) -> Result<u64> {
        let count:u64=self.db.query_row("SELECT count(*) FROM(SELECT request FROM content_delete_intents UNION ALL SELECT request FROM directory_delete_intents LIMIT 65)",[],|r|r.get(0))?;
        ensure!(
            count <= crate::model::workspace::limits::SAVE_QUEUE as u64,
            "CORRUPT_CONTENT_SAVE: disposition capacity"
        );
        Ok(count)
    }
    fn decode_directory_delete(&self, r: Row) -> Result<DirectoryDeleteIntent> {
        let record: DirectoryDeleteIntent = decode("directory-delete:v1", &r.3)?;
        record.validate()?;
        ensure!(
            r.0 == record.request.to_string()
                && r.1 == record.snapshot.branch
                && r.2 == record.snapshot.entry.id
                && record.snapshot.workspace.repository == self.repo
                && record.snapshot.workspace.workspace == workspace(&self.db)?,
            "CORRUPT_CONTENT_SAVE"
        );
        Ok(record)
    }
    fn directory_delete_row(&self, request: Uuid) -> Result<Option<DirectoryDeleteIntent>> {
        let r=record_sql(self.db.query_row("SELECT request,branch,entity,record FROM directory_delete_intents WHERE request=?",[request.to_string()],row).optional())?;
        r.map(|r| self.decode_directory_delete(r)).transpose()
    }
    pub fn directory_delete_intent(
        &self,
        request: Uuid,
        context: &ReadContext,
    ) -> Result<Option<DirectoryDeleteIntent>> {
        context.check()?;
        self.content_save_usable()?;
        let _tx = if self.db.is_autocommit() {
            Some(self.db.unchecked_transaction()?)
        } else {
            None
        };
        let r = self.directory_delete_row(request)?;
        context.check()?;
        Ok(r)
    }
    pub fn directory_delete_intents(
        &self,
        after: Option<Uuid>,
        limit: usize,
        context: &ReadContext,
    ) -> Result<Vec<DirectoryDeleteIntent>> {
        context.check()?;
        self.content_save_usable()?;
        ensure!((1..=16).contains(&limit), "CONTENT_SAVE_CAPTURE_LIMIT");
        let _tx = if self.db.is_autocommit() {
            Some(self.db.unchecked_transaction()?)
        } else {
            None
        };
        self.native_delete_count()?;
        let mut statement=self.db.prepare("SELECT request,branch,entity,record FROM directory_delete_intents WHERE request>? ORDER BY request LIMIT ?")?;
        let rows = record_sql(
            statement
                .query_map(
                    params![
                        after.map_or_else(String::new, |id| id.to_string()),
                        limit as u64
                    ],
                    row,
                )?
                .collect::<rusqlite::Result<Vec<_>>>(),
        )?;
        let result = rows
            .into_iter()
            .map(|r| self.decode_directory_delete(r))
            .collect::<Result<Vec<_>>>()?;
        context.check()?;
        Ok(result)
    }
    fn directory_pending_rows(&self, entity: &str) -> Result<Vec<DirectoryDeleteIntent>> {
        let mut statement=self.db.prepare("SELECT request,branch,entity,record FROM directory_delete_intents WHERE branch=? AND entity=? ORDER BY request LIMIT 65")?;
        let rows = record_sql(
            statement
                .query_map(params![self.active, entity], row)?
                .collect::<rusqlite::Result<Vec<_>>>(),
        )?;
        ensure!(
            rows.len() <= crate::model::workspace::limits::SAVE_QUEUE,
            "CONTENT_SAVE_CAPTURE_LIMIT"
        );
        rows.into_iter()
            .map(|r| self.decode_directory_delete(r))
            .collect()
    }
    pub(super) fn directory_pending_for(
        &self,
        snapshot: &DirectorySnapshot,
        context: &ReadContext,
    ) -> Result<Vec<DirectoryDeleteIntent>> {
        context.check()?;
        let rows = self.directory_pending_rows(&snapshot.entry.id)?;
        for r in &rows {
            ensure!(r.snapshot == *snapshot, "STALE_CONTENT_DIRECTORY");
        }
        Ok(rows)
    }
    pub(in crate::core::content_saves) fn ensure_directory_accepts_children(
        &self,
        entity: &str,
        context: &ReadContext,
    ) -> Result<()> {
        context.check()?;
        let rows = self.directory_pending_rows(entity)?;
        if !rows.is_empty() {
            let snapshot = self.directory_snapshot(entity, context)?;
            ensure!(
                rows.iter().all(|r| r.snapshot == snapshot),
                "STALE_CONTENT_DIRECTORY"
            );
            bail!("ACCESS_DENIED: directory deletion is pending");
        }
        Ok(())
    }
    pub fn validate_directory_delete(
        &self,
        record: &DirectoryDeleteIntent,
        context: &ReadContext,
    ) -> Result<()> {
        context.check()?;
        self.content_save_usable()?;
        record.validate()?;
        let _tx = if self.db.is_autocommit() {
            Some(self.db.unchecked_transaction()?)
        } else {
            None
        };
        ensure!(
            self.directory_delete_row(record.request)?.as_ref() == Some(record),
            "CONTENT_METADATA_RECEIPT_LOST"
        );
        self.check_directory_snapshot(&record.snapshot, context)?;
        self.check_directory_empty(&record.snapshot.entry.id)?;
        context.check()
    }
    pub fn stage_directory_delete(
        &mut self,
        request: Uuid,
        snapshot: &DirectorySnapshot,
        context: &ReadContext,
    ) -> Result<DirectoryDeleteIntent> {
        let record = DirectoryDeleteIntent {
            request,
            snapshot: snapshot.clone(),
        };
        record.validate()?;
        self.content_save_write(context,|store| {
            store.check_directory_snapshot(snapshot,context)?;store.check_directory_empty(&snapshot.entry.id)?;
            let mut pending=store.directory_pending_for(snapshot,context)?;
            if let Some(old)=store.directory_delete_row(request)? {ensure!(old==record,"CONTENT_METADATA_MUTATION_REUSED");return Ok(old);}
            let id=request.to_string();
            ensure!(!store.db.query_row("SELECT EXISTS(SELECT 1 FROM events WHERE id=?) OR EXISTS(SELECT 1 FROM content_save_operations WHERE operation=?) OR EXISTS(SELECT 1 FROM content_save_create_changes WHERE mutation=?) OR EXISTS(SELECT 1 FROM content_save_metadata_changes WHERE mutation=?) OR EXISTS(SELECT 1 FROM content_save_namespace_changes WHERE mutation=?) OR EXISTS(SELECT 1 FROM content_delete_intents WHERE request=?)",params![id,id,id,id,id,id],|r|r.get::<_,bool>(0))?,"CONTENT_METADATA_MUTATION_REUSED");
            ensure!(store.native_delete_count()?<crate::model::workspace::limits::SAVE_QUEUE as u64,"CONTENT_RESOURCE_LIMIT");
            let raw=encode("directory-delete:v1",&record)?;
            ensure!(store.db.execute("INSERT INTO directory_delete_intents VALUES(?,?,?,?)",params![id,snapshot.branch,snapshot.entry.id,raw])?==1,"CONTENT_METADATA_RECEIPT_LOST");
            store.validate_directory_delete(&record,context)?;
            store.native_delete_count()?;
            pending.push(record.clone());pending.sort_by_key(|r|r.request);
            ensure!(store.directory_pending_for(snapshot,context)?==pending,"CONTENT_METADATA_RECEIPT_LOST");
            crate::util::fault::trigger("content.directory.delete_written")?;Ok(record.clone())
        }).and_then(|r|{crate::util::fault::trigger("content.directory.delete_committed")?;Ok(r)})
    }
    pub fn cancel_directory_delete(
        &mut self,
        request: Uuid,
        entity: &str,
        context: &ReadContext,
    ) -> Result<()> {
        self.content_save_write(context, |store| {
            if let Some(record) = store.directory_delete_row(request)? {
                ensure!(record.snapshot.entry.id == entity, "INVALID_HANDLE");
                ensure!(
                    store.db.execute(
                        "DELETE FROM directory_delete_intents WHERE request=? AND record=?",
                        params![request.to_string(), encode("directory-delete:v1", &record)?]
                    )? == 1,
                    "CONTENT_METADATA_RECEIPT_LOST"
                );
            }
            ensure!(
                store.directory_delete_row(request)?.is_none(),
                "CONTENT_METADATA_RECEIPT_LOST"
            );
            crate::util::fault::trigger("content.directory.cancel_written")?;
            Ok(())
        })
        .and_then(|()| {
            crate::util::fault::trigger("content.directory.cancel_committed")?;
            Ok(())
        })
    }
    pub(super) fn advance_directory_pending(
        &self,
        rows: &[DirectoryDeleteIntent],
        after: &Entry,
        remove: bool,
        context: &ReadContext,
    ) -> Result<()> {
        for row in rows {
            context.check()?;
            let old = encode("directory-delete:v1", row)?;
            let changed = if remove {
                self.db.execute(
                    "DELETE FROM directory_delete_intents WHERE request=? AND record=?",
                    params![row.request.to_string(), old],
                )?
            } else {
                let mut next = row.clone();
                next.snapshot.entry = after.clone();
                next.validate()?;
                self.db.execute(
                    "UPDATE directory_delete_intents SET record=? WHERE request=? AND record=?",
                    params![
                        encode("directory-delete:v1", &next)?,
                        row.request.to_string(),
                        old
                    ],
                )?
            };
            ensure!(changed == 1, "CONTENT_METADATA_RECEIPT_LOST");
        }
        Ok(())
    }
    pub(super) fn check_directory_pending_after(
        &self,
        before: &DirectorySnapshot,
        rows: &[DirectoryDeleteIntent],
        after: &Entry,
        remove: bool,
        context: &ReadContext,
    ) -> Result<()> {
        let mut scope = before.clone();
        scope.entry = after.clone();
        let actual = self.directory_pending_for(&scope, context)?;
        let expected = if remove {
            vec![]
        } else {
            rows.iter()
                .map(|row| {
                    let mut next = row.clone();
                    next.snapshot = scope.clone();
                    next
                })
                .collect::<Vec<_>>()
        };
        ensure!(actual == expected, "CONTENT_METADATA_RECEIPT_LOST");
        Ok(())
    }
}
