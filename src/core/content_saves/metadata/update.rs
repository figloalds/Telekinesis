//! Partial owned metadata commands, ordered with durable private checkpoints.
use super::*;

/// Unspecified fields retain the fresh value read under the Core writer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentBasicUpdate {
    pub attributes: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readonly: Option<bool>,
    pub posix_mode: Option<u32>,
    pub creation_time: Option<u64>,
    pub access_time: Option<u64>,
    pub write_time: Option<u64>,
    pub change_time: Option<u64>,
}
impl ContentBasicUpdate {
    pub fn validate(&self) -> Result<()> {
        self.apply(BasicInfo {
            posix_mode: None,
            attributes: 0x80,
            creation_time: 0,
            access_time: 0,
            write_time: 0,
            change_time: 0,
        })?;
        Ok(())
    }
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }
    pub(super) fn preserve_times(&self) -> u8 {
        (u8::from(self.write_time.is_some()) * 2) | (u8::from(self.change_time.is_some()) * 4)
    }
    pub(super) fn apply(&self, basic: BasicInfo) -> Result<BasicInfo> {
        self.apply_kind(basic, "file")
    }
    pub(in crate::core::content_saves) fn apply_kind(
        &self,
        mut basic: BasicInfo,
        kind: &str,
    ) -> Result<BasicInfo> {
        if let Some(attributes) = self.attributes {
            basic.attributes = BasicInfo::attributes(kind, attributes)?;
        }
        if let Some(readonly) = self.readonly {
            basic.attributes =
                BasicInfo::attributes(kind, (basic.attributes & !1) | u32::from(readonly))?;
        }
        if let Some(mode) = self.posix_mode {
            basic.posix_mode = Some(mode);
        }
        if let Some(time) = self.creation_time {
            basic.creation_time = time;
        }
        if let Some(time) = self.access_time {
            basic.access_time = time;
        }
        if let Some(time) = self.write_time {
            basic.write_time = time;
        }
        if let Some(time) = self.change_time {
            basic.change_time = time;
        }
        basic.validate()?;
        Ok(basic)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CheckpointMetadataUpdate {
    pub(super) checkpoint: Option<SaveCheckpoint>,
    pub(super) fields: ContentBasicUpdate,
}
impl CheckpointMetadataUpdate {
    pub(super) fn sequence(&self) -> u64 {
        self.checkpoint.as_ref().map_or(0, |cp| cp.sequence)
    }
    pub(super) fn validate(&self, intent: &SaveIntent) -> Result<()> {
        intent.validate()?;
        if let Some(checkpoint) = &self.checkpoint {
            checkpoint.validate()?;
            ensure!(checkpoint.intent == *intent, "CONTENT_SAVE_INTENT_MISMATCH");
        }
        self.fields.validate()?;
        Ok(())
    }
}

impl Store {
    /// Read-only receipt lookup; absence never re-applies a failed command.
    pub fn content_checkpoint_metadata_receipt(
        &self,
        intent: &SaveIntent,
        mutation: Uuid,
        checkpoint: Option<&SaveCheckpoint>,
        fields: &ContentBasicUpdate,
        context: &ReadContext,
    ) -> Result<Option<ContentMetadataReceipt>> {
        context.check()?;
        self.content_save_usable()?;
        let update = CheckpointMetadataUpdate {
            checkpoint: checkpoint.cloned(),
            fields: fields.clone(),
        };
        update.validate(intent)?;
        ensure!(
            self.db.is_autocommit(),
            "CONTENT_SAVE_REQUIRES_OUTER_TRANSACTION"
        );
        let tx = self.db.unchecked_transaction()?;
        let receipt = self.find_checkpoint_metadata_receipt(intent, mutation, &update)?;
        context.check()?;
        tx.commit()?;
        Ok(receipt)
    }
    fn find_checkpoint_metadata_receipt(
        &self,
        intent: &SaveIntent,
        mutation: Uuid,
        update: &CheckpointMetadataUpdate,
    ) -> Result<Option<ContentMetadataReceipt>> {
        let raw = record_sql(
            self.db
                .query_row(
                    "SELECT record FROM content_save_metadata_changes WHERE mutation=?",
                    [mutation.to_string()],
                    |r| blob(r, 0),
                )
                .optional(),
        )?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let record: MetadataRecord = decode("metadata:v1", &raw)?;
        ensure!(
            record.request.intent == *intent
                && record.request.mutation == mutation
                && record.request.update.as_ref() == Some(update),
            "CONTENT_METADATA_MUTATION_REUSED"
        );
        Ok(Some(self.verify_metadata_record(&record)?))
    }
    /// Merge the current canonical fields with an observed private byte revision.
    /// This grants no writer ownership or authority to retire private bytes.
    pub fn content_save_basic_info(
        &self,
        operation: Uuid,
        state: crate::content::write::NativeBasicState,
        context: &ReadContext,
    ) -> Result<BasicInfo> {
        context.check()?;
        self.content_save_usable()?;
        let tx = if self.db.is_autocommit() {
            Some(self.db.unchecked_transaction()?)
        } else {
            None
        };
        let (capture, selected, receipt) = load(&self.db, operation)?;
        let mut published_floor = if receipt.is_some() {
            selected.as_ref().context("CORRUPT_CONTENT_SAVE")?.sequence
        } else {
            0
        };
        if let Some(predecessor) = &capture.predecessor {
            let (_, saved, _) =
                history(&self.db, operation, predecessor)?.context("CORRUPT_CONTENT_SAVE")?;
            published_floor = published_floor.max(saved.sequence);
        }
        let mut basic = self.entry(&capture.intent.base.entity)?.basic_info();
        if state.has_changes && state.sequence > published_floor {
            let policy = capture
                .basic_policy
                .as_ref()
                .filter(|p| p.sequence == state.sequence);
            let preserve = policy.map_or(0, |p| p.preserve_times);
            if let Some(times) = state.times {
                if preserve & 2 == 0 {
                    basic.write_time = times.write_time;
                }
                if preserve & 4 == 0 {
                    basic.change_time = times.change_time;
                }
            }
            if policy.is_none() {
                basic.attributes = BasicInfo::attributes("file", basic.attributes | 0x20)?;
            }
        }
        basic.validate()?;
        context.check()?;
        if let Some(tx) = tx {
            tx.commit()?;
        }
        Ok(basic)
    }
    /// Apply only requested fields without publishing the private byte buffer.
    /// The caller orders this with its byte writer and supplies its FULL Saved
    /// checkpoint, or None when no unpublished byte revision exists. A failed
    /// selected save may be rebound; an already committed save needs adoption
    /// first. This transaction performs no private journal or provider work.
    pub fn apply_content_checkpoint_basic(
        &mut self,
        intent: &SaveIntent,
        mutation: Uuid,
        checkpoint: Option<&SaveCheckpoint>,
        fields: ContentBasicUpdate,
        context: &ReadContext,
    ) -> Result<ContentMetadataReceipt> {
        let update = CheckpointMetadataUpdate {
            checkpoint: checkpoint.cloned(),
            fields,
        };
        update.validate(intent)?;
        self.content_save_write(context, |store| {
            if let Some(receipt) =
                store.find_checkpoint_metadata_receipt(intent, mutation, &update)?
            {
                return Ok(receipt);
            }
            let (capture, selected, receipt) = load(&store.db, intent.operation)?;
            ensure!(capture.intent == *intent, "CONTENT_SAVE_INTENT_MISMATCH");
            ensure!(receipt.is_none(), "CONTENT_METADATA_SAVE_IN_PROGRESS");
            current(store, &capture)?;
            if let Some(prior) = &selected {
                let checkpoint = checkpoint.context("CONTENT_METADATA_SAVE_IN_PROGRESS")?;
                ensure!(
                    checkpoint.needs_publication()
                        && (checkpoint.sequence > prior.sequence || checkpoint == prior),
                    "STALE_WRITE_CHECKPOINT"
                );
            }
            let sequence = update.sequence();
            ensure!(
                capture
                    .basic_policy
                    .as_ref()
                    .is_none_or(|p| sequence >= p.sequence),
                "STALE_WRITE_CHECKPOINT"
            );
            let entry = store.entry(&intent.base.entity)?;
            ensure!(
                (entry.alive || capture.retirement.is_some()) && entry.kind == "file",
                "FILE_NOT_FOUND"
            );
            let prior_policy = capture
                .basic_policy
                .as_ref()
                .filter(|p| p.sequence == sequence);
            let prior_retention = prior_policy.map_or(0, |p| p.preserve_times);
            let mut basic = entry.basic_info();
            if let Some(checkpoint) = checkpoint.filter(|cp| cp.needs_publication()) {
                if let Some(times) = checkpoint.native_times() {
                    if prior_retention & 2 == 0 {
                        basic.write_time = times.write_time;
                    }
                    if prior_retention & 4 == 0 {
                        basic.change_time = times.change_time;
                    }
                }
                if prior_policy.is_none() {
                    basic.attributes = BasicInfo::attributes("file", basic.attributes | 0x20)?;
                }
            }
            basic = update.fields.apply(basic)?;
            let request = MetadataRequest {
                intent: intent.clone(),
                mutation,
                basic,
                policy: BasicPolicy {
                    sequence,
                    preserve_times: prior_retention | update.fields.preserve_times(),
                },
                update: Some(update.clone()),
            };
            store.commit_basic_change(&capture, selected.as_ref(), &request)
        })
        .and_then(|receipt| {
            crate::util::fault::trigger("content.save.basic_committed")?;
            Ok(receipt)
        })
    }
}
