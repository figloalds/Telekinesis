//! Private recovery roots and immutable retained reads. These APIs grant no
//! metadata publication, whole-store reachability or byte-deletion authority.
use super::*;

pub const RETAINED_READ_LIMIT: usize = 64;
const ROOT_PAGE_LIMIT: usize = 64;

#[derive(Clone, Copy, Debug)]
enum Source {
    Session(Uuid),
    Retained(Uuid),
}

/// Opaque source identity from a verified header, not a serialized capability.
#[derive(Clone, Debug)]
pub struct RecoveryRoot {
    journal: Uuid,
    source: Source,
    operation: Uuid,
    base: FileRevision,
    seal: Digest,
}
impl RecoveryRoot {
    pub fn operation(&self) -> Uuid {
        self.operation
    }
    pub fn base(&self) -> &FileRevision {
        &self.base
    }
    pub fn archive(&self) -> Option<Uuid> {
        match self.source {
            Source::Session(_) => None,
            Source::Retained(id) => Some(id),
        }
    }
}
/// Volatile, bounded source audit. Discard all partial results on any error.
pub struct RecoveryRootsCursor {
    journal: Uuid,
    token: (i64, u64),
    expected: Inventory,
    seen: Inventory,
    after: Option<Uuid>,
    archives: bool,
}
/// Consistency fence for this private source only. Other root sources and a
/// deletion barrier are required before any collector can delete native bytes.
pub struct RecoveryRootsFence {
    journal: Uuid,
    token: (i64, u64),
    inventory: Inventory,
}
pub struct RecoveryRootsPage {
    pub examined: usize,
    pub roots: Vec<RecoveryRoot>,
    pub next: Option<RecoveryRootsCursor>,
    pub fence: Option<RecoveryRootsFence>,
}

fn root_source(db: &Connection, journal: Uuid, source: Source) -> Result<RecoveryRoot> {
    let (working, seal) = match source {
        Source::Session(op) => {
            let (working, _) = state(db, op)?;
            let seal = journal_sql(db.query_row(
                "SELECT seal FROM sessions WHERE id=?",
                [op.to_string()],
                |r| blob(r, 0, 32),
            ))?;
            let seal = Digest(
                seal.try_into()
                    .map_err(|_| anyhow::anyhow!("CORRUPT_WRITE_JOURNAL"))?,
            );
            (working, seal)
        }
        Source::Retained(id) => {
            let (working, _, seal) = retained(db, id)?;
            (working, seal)
        }
    };
    Ok(RecoveryRoot {
        journal,
        source,
        operation: working.intent.operation,
        base: working.intent.base,
        seal,
    })
}
impl WriteJournal {
    /// Enumerate every current session (including unsaved ones) and retained
    /// predecessor, one base per scope. A missing scope fails the final inventory
    /// check. Own or foreign writes invalidate the cursor instead of mixing pages.
    pub fn recovery_roots(
        &self,
        cursor: Option<RecoveryRootsCursor>,
        limit: usize,
        context: &ReadContext,
    ) -> Result<RecoveryRootsPage> {
        ensure!(
            (1..=ROOT_PAGE_LIMIT).contains(&limit),
            "CONTENT_BUFFER_LIMIT"
        );
        let db = self.lock(context)?;
        let token = change_token(&db)?;
        let mut cursor = match cursor {
            Some(cursor) => {
                ensure!(
                    cursor.journal == self.identity && cursor.token == token,
                    "STALE_WRITE_CHECKPOINT"
                );
                cursor
            }
            None => RecoveryRootsCursor {
                journal: self.identity,
                token,
                expected: inventory(&db)?,
                seen: Inventory {
                    count: 0,
                    xor: Digest([0; 32]),
                },
                after: None,
                archives: false,
            },
        };
        let mut roots = Vec::with_capacity(limit);
        while roots.len() < limit {
            context.check()?;
            let table = if cursor.archives {
                "retained_overlays"
            } else {
                "sessions"
            };
            let id = db
                .query_row(
                    &format!("SELECT id FROM {table} WHERE id>? ORDER BY id LIMIT 1"),
                    [cursor.after.map(|v| v.to_string()).unwrap_or_default()],
                    uuid_text,
                )
                .optional()?;
            let Some(id) = id else {
                if !cursor.archives {
                    cursor.archives = true;
                    cursor.after = None;
                    continue;
                }
                ensure!(
                    cursor.seen == cursor.expected && inventory(&db)? == cursor.expected,
                    "CORRUPT_WRITE_JOURNAL"
                );
                ensure!(change_token(&db)? == token, "STALE_WRITE_CHECKPOINT");
                context.check()?;
                let fence = RecoveryRootsFence {
                    journal: self.identity,
                    token,
                    inventory: cursor.expected,
                };
                return Ok(RecoveryRootsPage {
                    examined: roots.len(),
                    roots,
                    next: None,
                    fence: Some(fence),
                });
            };
            let id = Uuid::parse_str(&id)?;
            let source = if cursor.archives {
                Source::Retained(id)
            } else {
                Source::Session(id)
            };
            let root = root_source(&db, self.identity, source)?;
            cursor.seen.count = cursor
                .seen
                .count
                .checked_add(1)
                .context("CORRUPT_WRITE_JOURNAL")?;
            xor_into(&mut cursor.seen.xor, root.seal);
            cursor.after = Some(id);
            roots.push(root);
        }
        ensure!(change_token(&db)? == token, "STALE_WRITE_CHECKPOINT");
        context.check()?;
        Ok(RecoveryRootsPage {
            examined: roots.len(),
            roots,
            next: Some(cursor),
            fence: None,
        })
    }
    pub fn validate_recovery_roots(
        &self,
        fence: &RecoveryRootsFence,
        context: &ReadContext,
    ) -> Result<()> {
        let db = self.lock(context)?;
        ensure!(
            fence.journal == self.identity && change_token(&db)? == fence.token,
            "STALE_WRITE_CHECKPOINT"
        );
        ensure!(inventory(&db)? == fence.inventory, "CORRUPT_WRITE_JOURNAL");
        context.check()
    }
    fn check_recovery_root(&self, root: &RecoveryRoot, context: &ReadContext) -> Result<()> {
        ensure!(root.journal == self.identity, "STALE_WRITE_CHECKPOINT");
        let db = self.lock(context)?;
        let current = root_source(&db, self.identity, root.source)?;
        ensure!(
            current.seal == root.seal
                && current.operation == root.operation
                && current.base == root.base,
            "STALE_WRITE_CHECKPOINT"
        );
        context.check()
    }
    /// Pin outside the journal mutex, then revalidate the source. Caller retains
    /// the pin through its native walk/read; availability is a separate concern.
    pub fn pin_native_recovery_root(
        &self,
        root: &RecoveryRoot,
        native: &LocalContent,
        context: &ReadContext,
    ) -> Result<PinnedContent> {
        self.check_recovery_root(root, context)?;
        ensure!(
            native.identity() == LocalContent::open(&self.root)?.identity(),
            "CONTENT_SAVE_WRONG_STORE"
        );
        let pin = native.pin(&root.base.content)?;
        self.check_recovery_root(root, context)?;
        Ok(pin)
    }
    /// Read the immutable Saved view of a retained predecessor. This neither
    /// resumes its owner nor creates a PreparedSave publication ticket.
    pub fn open_retained_checkpoint(
        self: &Arc<Self>,
        archive: Uuid,
        provider: Arc<dyn ContentProvider>,
        context: &ReadContext,
    ) -> Result<RetainedRead> {
        let (checkpoint, seal, lease) = {
            let db = self.lock(context)?;
            let (_, checkpoint, seal) = retained(&db, archive)?;
            verify_saved(&db, &checkpoint, context)?;
            let mut readers = crate::util::lock::lock(&self.retained_readers);
            ensure!(
                readers.values().sum::<usize>() < RETAINED_READ_LIMIT,
                "CONTENT_RESOURCE_LIMIT"
            );
            *readers.entry(archive).or_default() += 1;
            let lease = ReadLease {
                journal: self.clone(),
                archive,
            };
            (checkpoint, seal, lease)
        };
        let base = provider.pin(&checkpoint.intent.base.content)?;
        ensure!(
            base.reference() == &checkpoint.intent.base.content,
            "CONTENT_SAVE_WRONG_STORE"
        );
        context.check()?;
        let reader = RetainedRead {
            journal: self.clone(),
            checkpoint,
            archive,
            seal,
            provider,
            base,
            converted: Mutex::new(None),
            _lease: lease,
        };
        reader.check(context)?;
        Ok(reader)
    }
    /// A future retirement transaction must inspect this while holding the
    /// journal mutex; acquisition registers under that same mutex.
    pub fn retained_reader_count(&self, archive: Uuid) -> usize {
        crate::util::lock::lock(&self.retained_readers)
            .get(&archive)
            .copied()
            .unwrap_or(0)
    }
}
struct ReadLease {
    journal: Arc<WriteJournal>,
    archive: Uuid,
}
impl Drop for ReadLease {
    fn drop(&mut self) {
        let mut readers = crate::util::lock::lock(&self.journal.retained_readers);
        if let Some(count) = readers.get_mut(&self.archive) {
            *count -= 1;
            if *count == 0 {
                readers.remove(&self.archive);
            }
        }
    }
}
pub struct RetainedRead {
    journal: Arc<WriteJournal>,
    checkpoint: SaveCheckpoint,
    archive: Uuid,
    seal: Digest,
    provider: Arc<dyn ContentProvider>,
    base: PinnedContent,
    converted: Mutex<Option<PinnedContent>>,
    _lease: ReadLease,
}
impl RetainedRead {
    pub fn checkpoint(&self) -> &SaveCheckpoint {
        &self.checkpoint
    }
    pub fn archive(&self) -> Uuid {
        self.archive
    }
    fn check(&self, context: &ReadContext) -> Result<()> {
        let db = self.journal.lock(context)?;
        let (_, saved, seal) = retained(&db, self.archive)?;
        ensure!(
            seal == self.seal && saved == self.checkpoint,
            "CORRUPT_WRITE_JOURNAL"
        );
        context.check()
    }
    /// Output must be discarded on error, as with live session reads.
    pub fn read_range(&self, offset: u64, out: &mut [u8], context: &ReadContext) -> Result<()> {
        self.read_using(offset, out, None, context)
    }
    fn read_using(
        &self,
        offset: u64,
        out: &mut [u8],
        override_base: Option<(&dyn ContentProvider, &PinnedContent)>,
        context: &ReadContext,
    ) -> Result<()> {
        ensure!(out.len() <= limits::READ_BYTES, "CONTENT_BUFFER_LIMIT");
        let end = offset
            .checked_add(out.len() as u64)
            .context("RANGE_OVERFLOW")?;
        ensure!(end <= self.checkpoint.length, "RANGE_OUT_OF_BOUNDS");
        self.check(context)?;
        let mut from = offset;
        while from < end {
            context.check()?;
            let until = end.min((from / CHUNK_SIZE + 1) * CHUNK_SIZE);
            let patch = {
                let db = self.journal.lock(context)?;
                let (_, saved, seal) = retained(&db, self.archive)?;
                ensure!(
                    seal == self.seal && saved == self.checkpoint,
                    "CORRUPT_WRITE_JOURNAL"
                );
                chunk_meta(
                    &db,
                    saved.roots.patches,
                    saved.intent.operation,
                    from / CHUNK_SIZE,
                    saved.roots.floor,
                    context,
                )?
                .map(|c| {
                    let bytes = payload(&db, &c)?;
                    Ok::<_, anyhow::Error>((c, bytes))
                })
                .transpose()?
            };
            let (provider, pin) = override_base.unwrap_or((&*self.provider, &self.base));
            read_overlay_chunk(
                provider,
                pin,
                self.checkpoint.base_visible_len,
                from,
                &mut out[(from - offset) as usize..(until - offset) as usize],
                patch.as_ref(),
                context,
            )?;
            from = until;
        }
        self.check(context)
    }
    fn needs_legacy_base(&self, context: &ReadContext) -> Result<bool> {
        let mut offset = 0;
        while offset < self.checkpoint.base_visible_len {
            self.check(context)?;
            let db = self.journal.lock(context)?;
            let c = chunk_meta(
                &db,
                self.checkpoint.roots.patches,
                self.checkpoint.intent.operation,
                offset / CHUNK_SIZE,
                self.checkpoint.roots.floor,
                context,
            )?;
            let Some(c) = c else {
                return Ok(true);
            };
            let count = (self.checkpoint.base_visible_len - offset).min(CHUNK_SIZE) as usize;
            if c.valid < count {
                return Ok(true);
            }
            let bytes = payload(&db, &c)?;
            if (0..count).any(|i| bytes[CHUNK_SIZE as usize + i / 8] & (1 << (i % 8)) == 0) {
                return Ok(true);
            }
            offset += count as u64;
        }
        Ok(false)
    }
    fn converted_base(
        &self,
        target: &LocalContent,
        context: &ReadContext,
    ) -> Result<PinnedContent> {
        loop {
            self.check(context)?;
            match self.converted.try_lock() {
                Ok(mut cache) => {
                    if let Some(pin) = cache.as_ref().filter(|p| p.provider() == target.identity())
                    {
                        return Ok(pin.clone());
                    }
                    let pin = target.convert_legacy(
                        &self.checkpoint.intent.base.content,
                        context,
                        &|| self.check(context),
                    )?;
                    *cache = Some(pin.clone());
                    return Ok(pin);
                }
                Err(TryLockError::Poisoned(_)) => anyhow::bail!("WRITE_CONVERSION_POISONED"),
                Err(TryLockError::WouldBlock) => {
                    context.cancel.sleep(Duration::from_millis(5));
                }
            }
        }
    }
    /// Reconstruct/pin canonical Saved bytes, with normal COW/legacy rules.
    /// Returns content only; no active owner or metadata publication authority.
    pub fn restore_native(
        &self,
        target: &LocalContent,
        context: &ReadContext,
    ) -> Result<PinnedContent> {
        self.check(context)?;
        let legacy = self.checkpoint.intent.base.content.kind == ContentKind::Legacy;
        let converted = if legacy && self.needs_legacy_base(context)? {
            Some(self.converted_base(target, context)?)
        } else {
            None
        };
        let base = if legacy {
            converted.as_ref().map(PinnedContent::reference)
        } else {
            Some(&self.checkpoint.intent.base.content)
        };
        let mut source = RetainedEdit {
            reader: self,
            converted: converted
                .as_ref()
                .map(|pin| (target as &dyn ContentProvider, pin)),
        };
        let content = target.rebuild(base, self.checkpoint.length, &mut source, context)?;
        self.check(context)?;
        let pin = target.pin(&content)?;
        self.check(context)?;
        Ok(pin)
    }
}
struct RetainedEdit<'a> {
    reader: &'a RetainedRead,
    converted: Option<(&'a dyn ContentProvider, &'a PinnedContent)>,
}
impl EditSource for RetainedEdit<'_> {
    fn check(&self, context: &ReadContext) -> Result<()> {
        self.reader.check(context)
    }
    fn span(&mut self, offset: u64, len: u64, context: &ReadContext) -> Result<EditSpan> {
        self.reader.check(context)?;
        let end = offset.checked_add(len).context("RANGE_OVERFLOW")?;
        ensure!(end <= self.reader.checkpoint.length, "RANGE_OUT_OF_BOUNDS");
        let db = self.reader.journal.lock(context)?;
        let cp = &self.reader.checkpoint;
        let changed = map(&db, cp.intent.operation, MapKind::Patches).has_range(
            cp.roots.patches,
            offset / CHUNK_SIZE,
            end.div_ceil(CHUNK_SIZE),
            cp.roots.floor,
            &mut MapWork::default(),
            context,
        )?;
        Ok(if changed {
            EditSpan::Changed
        } else if end <= cp.base_visible_len {
            EditSpan::Unchanged
        } else if offset >= cp.base_visible_len {
            EditSpan::Zero
        } else {
            EditSpan::Changed
        })
    }
    fn read(&mut self, offset: u64, out: &mut [u8], context: &ReadContext) -> Result<()> {
        self.reader.read_using(offset, out, self.converted, context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{model::workspace::WorkspaceId, util::Cancel};
    use std::time::Instant;
    fn context() -> ReadContext {
        ReadContext {
            deadline: Instant::now() + Duration::from_secs(30),
            cancel: Cancel::new(),
        }
    }
    #[test]
    fn retained_legacy_restoration_caches_conversion_and_full_replacement_needs_no_base() {
        for full in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let native = Arc::new(LocalContent::open(dir.path()).unwrap());
            let journal = WriteJournal::open(dir.path()).unwrap();
            let original: Vec<u8> = (0..3 * CHUNK_SIZE)
                .map(|i| ((i * 13 + 9) % 251) as u8)
                .collect();
            let digest = Digest::bytes(&original);
            let legacy =
                FileContentRef::new(ContentKind::Legacy, digest, original.len() as u64).unwrap();
            let path = dir.path().join("objects").join(digest.to_string());
            if !full {
                std::fs::write(&path, &original).unwrap();
            }
            let intent = SaveIntent {
                operation: Uuid::new_v4(),
                branch: "branch".into(),
                content_parent: None,
                alive_parent: None,
                base: FileRevision {
                    workspace: WorkspaceId {
                        repository: "repo".into(),
                        workspace: Uuid::new_v4(),
                    },
                    entity: "file".into(),
                    generation: 1,
                    content: legacy,
                },
            };
            let session = journal.create(intent, native.clone(), &context()).unwrap();
            let mut expected = original;
            if full {
                expected.fill(31);
                session.write_at(0, &expected, &context()).unwrap();
            } else {
                expected[70000..70004].copy_from_slice(b"edit");
                session.write_at(70000, b"edit", &context()).unwrap();
            }
            session.checkpoint(&context()).unwrap();
            let archive = Uuid::new_v4();
            // Trusted archive fixture exercises legacy recovery independently
            // of metadata's continuation/capture protocol.
            {
                let mut db = journal.lock(&context()).unwrap();
                db.execute_batch("PRAGMA synchronous=FULL").unwrap();
                let tx = db.transaction().unwrap();
                let (w, saved) = session.owned(&tx).unwrap();
                retain(&tx, archive, &w, &saved.unwrap()).unwrap();
                tx.commit().unwrap();
                db.execute_batch("PRAGMA synchronous=NORMAL").unwrap();
            }
            let reader = journal
                .open_retained_checkpoint(archive, native.clone(), &context())
                .unwrap();
            let restored = reader.restore_native(&native, &context()).unwrap();
            let oracle = native
                .import(&mut expected.as_slice(), expected.len() as u64, &context())
                .unwrap();
            assert_eq!(restored.reference(), &oracle);
            if !full {
                std::fs::remove_file(&path).unwrap();
            }
            let again = reader.restore_native(&native, &context()).unwrap();
            assert_eq!(again.reference(), restored.reference());
            assert_eq!(native.buffer_usage().0, 0);
        }
    }
}
