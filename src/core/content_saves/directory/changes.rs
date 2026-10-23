//! Exact directory metadata commands, with no byte-save or journal authority.
use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectorySnapshot {
    pub workspace: WorkspaceId,
    pub branch: String,
    pub view_generation: u64,
    pub entry: Entry,
}
impl DirectorySnapshot {
    pub(super) fn validate(&self) -> Result<()> {
        let e = &self.entry;
        ensure!(
            !self.workspace.workspace.is_nil()
                && self.view_generation <= i64::MAX as u64
                && [&self.workspace.repository, &self.branch, &e.id, &e.parent]
                    .iter()
                    .all(|s| !s.is_empty() && s.len() <= 128 && !s.chars().any(char::is_control))
                && e.id != ROOT
                && e.kind == "directory"
                && e.content.is_none()
                && e.content_ref.is_none()
                && e.revisions.len() <= 4
                && ["kind", "location", "alive"]
                    .iter()
                    .all(|k| e.revisions.contains_key(*k))
                && e.revisions
                    .iter()
                    .all(
                        |(k, v)| ["kind", "location", "alive", "basic"].contains(&k.as_str())
                            && !v.is_empty()
                            && v.len() <= 128
                            && !v.chars().any(char::is_control)
                    ),
            "CORRUPT_CONTENT_SAVE"
        );
        name_key(&e.name)?;
        if let Some(b) = e.basic {
            b.validate()?;
            ensure!(
                b.attributes == BasicInfo::attributes("directory", b.attributes)?,
                "INVALID_ATTRIBUTES"
            );
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum DirectoryMutation {
    Basic(ContentBasicUpdate),
    Rename {
        source: Option<NamespaceTarget>,
        target: NamespaceTarget,
        destination: Option<Box<DirectorySnapshot>>,
    },
    Remove {
        source: Option<NamespaceTarget>,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectoryMutationRequest {
    pub mutation: Uuid,
    pub before: DirectorySnapshot,
    pub action: DirectoryMutation,
}
impl DirectoryMutationRequest {
    fn validate(&self) -> Result<()> {
        self.before.validate()?;
        ensure!(!self.mutation.is_nil(), "CONTENT_SAVE_INTENT_MISMATCH");
        let target = |t: &NamespaceTarget| -> Result<()> {
            ensure!(
                t.path.len() <= 32_768
                    && !t.parent.is_empty()
                    && t.parent.len() <= 128
                    && !t.parent.chars().any(char::is_control),
                "CONTENT_SAVE_CAPTURE_LIMIT"
            );
            split_path(&t.path)?;
            Ok(())
        };
        match &self.action {
            DirectoryMutation::Basic(fields) => {
                fields.validate()?;
            }
            DirectoryMutation::Remove { source } => {
                if let Some(s) = source {
                    target(s)?;
                }
            }
            DirectoryMutation::Rename {
                source,
                target: t,
                destination,
            } => {
                if let Some(s) = source {
                    target(s)?;
                }
                target(t)?;
                if let Some(d) = destination {
                    d.validate()?;
                    ensure!(
                        d.entry.id != self.before.entry.id
                            && d.entry.alive
                            && d.workspace == self.before.workspace
                            && d.branch == self.before.branch
                            && d.view_generation == self.before.view_generation,
                        "CONTENT_SAVE_INTENT_MISMATCH"
                    );
                }
            }
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryMutationReceipt {
    pub mutation: Uuid,
    pub event: String,
    pub entries: Vec<Entry>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DirectoryChangeRecord {
    pub request: DirectoryMutationRequest,
    pub event: Event,
    pub after: Vec<Entry>,
}
impl Store {
    pub fn directory_snapshot(
        &self,
        entity: &str,
        context: &ReadContext,
    ) -> Result<DirectorySnapshot> {
        context.check()?;
        self.content_save_usable()?;
        let _tx = if self.db.is_autocommit() {
            Some(self.db.unchecked_transaction()?)
        } else {
            None
        };
        let (branch, view_generation) = active(&self.db)?;
        ensure!(branch == self.active, "STALE_CONTENT_VIEW");
        let snapshot = DirectorySnapshot {
            workspace: WorkspaceId {
                repository: self.repo.clone(),
                workspace: workspace(&self.db)?,
            },
            branch,
            view_generation,
            entry: self.entry(entity)?,
        };
        snapshot.validate()?;
        context.check()?;
        Ok(snapshot)
    }
    pub(super) fn check_directory_snapshot(
        &self,
        s: &DirectorySnapshot,
        context: &ReadContext,
    ) -> Result<()> {
        s.validate()?;
        ensure!(
            self.directory_snapshot(&s.entry.id, context)? == *s,
            "STALE_CONTENT_DIRECTORY"
        );
        Ok(())
    }
    pub(super) fn check_directory_empty(&self, entity: &str) -> Result<()> {
        let view = self.view(&self.active)?;
        ensure!(
            view.slots
                .range((entity.to_owned(), String::new())..)
                .next()
                .is_none_or(|((parent, _), _)| parent != entity),
            "DIRECTORY_NOT_EMPTY"
        );
        Ok(())
    }
    fn directory_changes(request: &DirectoryMutationRequest) -> Result<Vec<Change>> {
        let before = &request.before.entry;
        let change = |e: &Entry, field: &str, value: Value| Change {
            entity: e.id.clone(),
            field: field.to_owned(),
            parents: e.revisions.get(field).cloned().into_iter().collect(),
            value,
        };
        Ok(match &request.action {
            DirectoryMutation::Basic(fields) => vec![change(
                before,
                "basic",
                json!(fields.apply_kind(before.basic_info(), "directory")?),
            )],
            DirectoryMutation::Remove { .. } => vec![change(before, "alive", json!(false))],
            DirectoryMutation::Rename {
                target,
                destination,
                ..
            } => {
                let mut changes = vec![change(
                    before,
                    "location",
                    json!({"parent":target.parent,"name":split_path(&target.path)?.1}),
                )];
                if let Some(d) = destination {
                    changes.push(change(&d.entry, "alive", json!(false)));
                }
                changes
            }
        })
    }
    fn directory_after(&self, before: &Entry, event: &Event) -> Result<Entry> {
        let mut after = before.clone();
        for c in event.changes.iter().filter(|c| c.entity == before.id) {
            after.revisions.insert(c.field.clone(), event.id.clone());
            match c.field.as_str() {
                "basic" => after.basic = Some(serde_json::from_value(c.value.clone())?),
                "alive" => after.alive = c.value.as_bool().context("CORRUPT_CONTENT_SAVE")?,
                "location" => {
                    after.parent = c.value["parent"]
                        .as_str()
                        .context("CORRUPT_CONTENT_SAVE")?
                        .into();
                    after.name = c.value["name"]
                        .as_str()
                        .context("CORRUPT_CONTENT_SAVE")?
                        .into();
                }
                _ => bail!("CORRUPT_CONTENT_SAVE"),
            }
        }
        let mut modified = 0;
        for field in ["kind", "alive", "location"] {
            let id = after.revisions.get(field).context("CORRUPT_CONTENT_SAVE")?;
            let time: u64 = record_sql(self.db.query_row(
                "SELECT json_extract(payload,'$.created_ms') FROM events WHERE id=?",
                [id],
                |r| r.get(0),
            ))?;
            modified = modified.max(time);
        }
        after.modified_ms = modified;
        Ok(after)
    }
    fn verify_directory_change(
        &self,
        r: &DirectoryChangeRecord,
    ) -> Result<DirectoryMutationReceipt> {
        let q = &r.request;
        q.validate()?;
        let event = &r.event;
        ensure!(
            q.before.workspace.repository == self.repo
                && q.before.workspace.workspace == workspace(&self.db)?,
            "STALE_CONTENT_WORKSPACE"
        );
        ensure!(
            event.id == q.mutation.to_string()
                && event.repo == self.repo
                && event.branch.id == q.before.branch
                && event.changes == Self::directory_changes(q)?
                && event.dependencies.len() <= FRONTIER_LIMIT
                && event.object_sizes.is_empty()
                && event.reviewed.is_empty(),
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
        let mut expected = vec![self.directory_after(&q.before.entry, event)?];
        if let DirectoryMutation::Rename {
            destination: Some(d),
            ..
        } = &q.action
        {
            expected.push(self.directory_after(&d.entry, event)?);
        }
        ensure!(r.after == expected, "CORRUPT_CONTENT_SAVE");
        Ok(DirectoryMutationReceipt {
            mutation: q.mutation,
            event: event.id.clone(),
            entries: r.after.clone(),
        })
    }
    fn directory_change(&self, mutation: Uuid) -> Result<Option<DirectoryChangeRecord>> {
        let raw = record_sql(
            self.db
                .query_row(
                    "SELECT record FROM content_save_namespace_changes WHERE mutation=?",
                    [mutation.to_string()],
                    |r| blob(r, 0),
                )
                .optional(),
        )?;
        raw.map(|b| decode("directory-change:v1", &b)).transpose()
    }
    pub fn directory_mutation_receipt(
        &self,
        q: &DirectoryMutationRequest,
        context: &ReadContext,
    ) -> Result<Option<DirectoryMutationReceipt>> {
        context.check()?;
        self.content_save_usable()?;
        q.validate()?;
        let _tx = if self.db.is_autocommit() {
            Some(self.db.unchecked_transaction()?)
        } else {
            None
        };
        let Some(r) = self.directory_change(q.mutation)? else {
            return Ok(None);
        };
        ensure!(r.request == *q, "CONTENT_METADATA_MUTATION_REUSED");
        let receipt = self.verify_directory_change(&r)?;
        context.check()?;
        Ok(Some(receipt))
    }
    pub fn directory_is_removed(&self, entity: &str, context: &ReadContext) -> Result<bool> {
        context.check()?;
        self.content_save_usable()?;
        let _tx = if self.db.is_autocommit() {
            Some(self.db.unchecked_transaction()?)
        } else {
            None
        };
        let current = self.directory_snapshot(entity, context)?;
        let removed = self.directory_removal(&current)?.is_some();
        context.check()?;
        Ok(removed)
    }
    fn directory_removal(
        &self,
        current: &DirectorySnapshot,
    ) -> Result<Option<(DirectoryMutationRequest, DirectoryMutationReceipt)>> {
        if current.entry.alive {
            return Ok(None);
        }
        let head = current
            .entry
            .revisions
            .get("alive")
            .context("CORRUPT_CONTENT_SAVE")?;
        let Ok(id) = Uuid::parse_str(head) else {
            return Ok(None);
        };
        let Some(record) = self.directory_change(id)? else {
            return Ok(None);
        };
        let receipt = self.verify_directory_change(&record)?;
        ensure!(
            record.request.before.workspace == current.workspace
                && record.request.before.branch == current.branch
                && record.request.before.view_generation == current.view_generation,
            "STALE_CONTENT_VIEW"
        );
        if !receipt
            .entries
            .iter()
            .any(|e| e.id == current.entry.id && !e.alive && e.revisions.get("alive") == Some(head))
        {
            return Ok(None);
        }
        Ok(Some((record.request, receipt)))
    }
    /// Recover an unobserved local removal only for its exact prior incarnation.
    /// A foreign tombstone or a restored directory cannot advance an old handle.
    pub fn directory_handle_removal(
        &self,
        before: &Entry,
        context: &ReadContext,
    ) -> Result<Option<(DirectoryMutationRequest, DirectoryMutationReceipt)>> {
        context.check()?;
        self.content_save_usable()?;
        let _tx = if self.db.is_autocommit() {
            Some(self.db.unchecked_transaction()?)
        } else {
            None
        };
        let current = self.directory_snapshot(&before.id, context)?;
        let Some((request, receipt)) = self.directory_removal(&current)? else {
            return Ok(None);
        };
        let original = match &request.action {
            DirectoryMutation::Remove { .. } if request.before.entry.id == before.id => {
                &request.before.entry
            }
            DirectoryMutation::Rename {
                destination: Some(destination),
                ..
            } if destination.entry.id == before.id => &destination.entry,
            _ => return Ok(None),
        };
        let matches = before.kind == "directory"
            && original.revisions.get("alive") == before.revisions.get("alive");
        context.check()?;
        Ok(matches.then_some((request, receipt)))
    }
    pub fn apply_directory_mutation(
        &mut self,
        q: &DirectoryMutationRequest,
        context: &ReadContext,
    ) -> Result<DirectoryMutationReceipt> {
        q.validate()?;
        self.content_save_write(context,|store| {
            if let Some(r)=store.directory_mutation_receipt(q,context)? {return Ok(r);}
            store.check_directory_snapshot(&q.before,context)?;
            if !q.before.entry.alive {
                ensure!(matches!(q.action,DirectoryMutation::Basic(_)) && store.directory_is_removed(&q.before.entry.id,context)?,"FILE_NOT_FOUND");
            }
            let observed=match &q.action {DirectoryMutation::Rename{source,..}|DirectoryMutation::Remove{source}=>source.as_ref(),_=>None};
            if let Some(source)=observed {let entry=store.lookup(&source.path)?;ensure!(entry.id==q.before.entry.id && entry.parent==source.parent,"STALE_CONTENT_DESTINATION");}
            match &q.action {
                DirectoryMutation::Basic(_)=>{},
                DirectoryMutation::Remove{..}=>store.check_directory_empty(&q.before.entry.id)?,
                DirectoryMutation::Rename{target,destination,..}=>{
                    let parent=store.lookup(&split_path(&target.path)?.0)?;
                    ensure!(parent.alive && parent.kind=="directory" && parent.id==target.parent,"STALE_CONTENT_DESTINATION");
                    store.ensure_directory_accepts_children(&parent.id,context)?;
                    let mut ancestor=parent.id;let mut seen=BTreeSet::new();
                    for pass in 0..=4096 {
                        if ancestor==ROOT {break;}
                        ensure!(pass<4096,"CONTENT_SAVE_CAPTURE_LIMIT");context.check()?;
                        ensure!(ancestor!=q.before.entry.id && seen.insert(ancestor.clone()),"DIRECTORY_CYCLE");
                        let e=store.entry(&ancestor)?;ensure!(e.alive && e.kind=="directory","NOT_DIRECTORY");ancestor=e.parent;
                    }
                    match (store.lookup(&target.path),destination) {
                        (Ok(e),None) if e.id==q.before.entry.id=>{},
                        (Ok(e),Some(d))=>{ensure!(e.id==d.entry.id,"STALE_CONTENT_DESTINATION");store.check_directory_snapshot(d,context)?;store.check_directory_empty(&e.id)?;},
                        (Err(e),None) if crate::model::errors::code_of(&e)=="FILE_NOT_FOUND"=>{},
                        (Err(e),_)=>return Err(e),
                        _=>bail!("NAME_COLLISION")
                    }
                }
            }
            let mutation=q.mutation.to_string();
            if let Some(pending)=store.directory_delete_intent(q.mutation,context)? {
                ensure!(matches!(q.action,DirectoryMutation::Remove{..}) && pending.snapshot==q.before,"CONTENT_METADATA_MUTATION_REUSED");
            }
            ensure!(!store.db.query_row("SELECT EXISTS(SELECT 1 FROM events WHERE id=?) OR EXISTS(SELECT 1 FROM content_save_operations WHERE operation=?) OR EXISTS(SELECT 1 FROM content_save_create_changes WHERE mutation=?) OR EXISTS(SELECT 1 FROM content_save_metadata_changes WHERE mutation=?) OR EXISTS(SELECT 1 FROM content_delete_intents WHERE request=?)",params![mutation,mutation,mutation,mutation,mutation],|r|r.get::<_,bool>(0))?,"CONTENT_METADATA_MUTATION_REUSED");
            let pending=store.directory_pending_for(&q.before,context)?;
            let replaced=if let DirectoryMutation::Rename{destination:Some(d),..}=&q.action {store.directory_pending_for(d,context)?}else{vec![]};
            let mut event=store.event(&store.branch(&q.before.branch)?,Self::directory_changes(q)?);event.id=mutation.clone();
            ensure!(event.dependencies.len()<=FRONTIER_LIMIT,"CONTENT_SAVE_CAPTURE_LIMIT");
            crate::util::fault::trigger("content.directory.change_before_event")?;
            store.commit(event.clone(),true)?;
            crate::util::fault::trigger("content.directory.change_event_written")?;
            let mut after=vec![store.directory_after(&q.before.entry,&event)?];
            if let DirectoryMutation::Rename{destination:Some(d),..}=&q.action {after.push(store.directory_after(&d.entry,&event)?);}
            for entry in &after {ensure!(store.entry(&entry.id)?==*entry,"CONTENT_METADATA_CONTINUATION_LOST");}
            store.advance_directory_pending(&pending,&after[0],matches!(q.action,DirectoryMutation::Remove{..}),context)?;
            if after.len()==2 {store.advance_directory_pending(&replaced,&after[1],true,context)?;}
            let record=DirectoryChangeRecord {request:q.clone(),event,after};let raw=encode("directory-change:v1",&record)?;
            ensure!(store.db.execute("INSERT INTO content_save_namespace_changes VALUES(?,?)",params![mutation,&raw])?==1,"CONTENT_METADATA_RECEIPT_LOST");
            let stored=record_sql(store.db.query_row("SELECT record FROM content_save_namespace_changes WHERE mutation=?",[mutation],|r|blob(r,0)))?;
            ensure!(stored==raw,"CONTENT_METADATA_RECEIPT_LOST");
            for entry in &record.after {
                let expected=DirectorySnapshot{entry:entry.clone(),..q.before.clone()};
                ensure!(store.directory_snapshot(&entry.id,context)?==expected,"STALE_CONTENT_DIRECTORY");
            }
            store.check_directory_pending_after(&q.before,&pending,&record.after[0],matches!(q.action,DirectoryMutation::Remove{..}),context)?;
            if let DirectoryMutation::Rename{destination:Some(d),..}=&q.action {
                store.check_directory_pending_after(d,&replaced,&record.after[1],true,context)?;
            }
            let receipt=store.verify_directory_change(&record)?;
            crate::util::fault::trigger("content.directory.change_written")?;Ok(receipt)
        }).and_then(|r|{crate::util::fault::trigger("content.directory.change_committed")?;Ok(r)})
    }
}
