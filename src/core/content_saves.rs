//! Durable ownership and publication of prepared file saves. No file byte work or
//! journal lock is held in these metadata transactions. Mount/session rollover
//! and receipt-owned retirement are separate consumers, not implied by a receipt.
use super::*;
use crate::{
    content::{
        ContentProvider, LocalContent, PinnedContent, ReadContext,
        write::{PreparedSave, SaveCheckpoint, SaveIntent},
    },
    model::{
        content::{ContentKind, Digest as ContentDigest, FileContentRef},
        workspace::{FileRevision, WorkspaceId},
    },
};
use serde::de::DeserializeOwned;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::time::Duration;

const RECORD_BYTES: usize = 65_536;
const FRONTIER_LIMIT: usize = 256;

/// The descriptor and parents selected by an open native writer. Checked
/// inside the metadata writer transaction before superseding an operation.
pub(crate) struct ContentSaveSelection {
    pub entity: String,
    pub branch: String,
    pub generation: u64,
    pub content: FileContentRef,
    pub content_parent: Option<String>,
    pub alive_parent: Option<String>,
}

pub(super) fn ensure_schema(db: &Connection) -> Result<()> {
    db.execute_batch("SAVEPOINT content_save_schema;
        CREATE TABLE IF NOT EXISTS content_save_entities(
            branch TEXT NOT NULL,entity TEXT NOT NULL,generation INTEGER NOT NULL
            CHECK(typeof(generation)='integer' AND generation>0),
            owner TEXT CHECK(owner IS NULL OR length(owner)=36),PRIMARY KEY(branch,entity));
        CREATE TABLE IF NOT EXISTS content_save_operations(
            operation TEXT PRIMARY KEY,capture BLOB NOT NULL CHECK(length(capture)<=65536),
            selected BLOB CHECK(length(selected)<=65536),receipt BLOB CHECK(length(receipt)<=65536));
        CREATE TABLE IF NOT EXISTS content_save_history(
            operation TEXT NOT NULL,checkpoint TEXT NOT NULL,generation INTEGER NOT NULL CHECK(generation>0),
            capture BLOB NOT NULL CHECK(length(capture)<=65536),
            selected BLOB NOT NULL CHECK(length(selected)<=65536),receipt BLOB NOT NULL CHECK(length(receipt)<=65536),
            PRIMARY KEY(operation,checkpoint),UNIQUE(operation,generation));
        CREATE TABLE IF NOT EXISTS content_save_successors(
            operation TEXT NOT NULL,checkpoint TEXT NOT NULL,capture BLOB NOT NULL CHECK(length(capture)<=65536),
            PRIMARY KEY(operation,checkpoint));
        CREATE TABLE IF NOT EXISTS content_save_metadata_changes(
            mutation TEXT PRIMARY KEY CHECK(length(mutation)=36),
            record BLOB NOT NULL CHECK(length(record)<=65536));
        RELEASE content_save_schema;")?;
    Ok(())
}

/// Every ordinary local file-register mutation supersedes pending
/// publication tickets for that entity. Remote changes remain causal competitors;
/// they must not be silently parented or discarded by an old open writer.
pub(super) fn observe_event(
    db: &Connection,
    event: &Event,
    local: bool,
    owner: Option<&MetadataOwner>,
) -> Result<()> {
    if let Some(owner) = owner {
        ensure!(local, "CONTENT_METADATA_OWNER_MISMATCH");
        return owner.check(db, event);
    }
    if local {
        let mut update = db.prepare_cached(
            "UPDATE content_save_entities SET generation=generation+1,owner=NULL WHERE branch=? AND entity=?",
        )?;
        for entity in event
            .changes
            .iter()
            .filter(|c| matches!(c.field.as_str(), "content" | "alive" | "location" | "basic"))
            .map(|c| &c.entity)
            .collect::<BTreeSet<_>>()
        {
            update.execute(params![event.branch.id, entity])?;
        }
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope<T> {
    value: T,
    checksum: String,
}
fn encode<T: Serialize>(domain: &str, value: &T) -> Result<Vec<u8>> {
    let checksum = hash(&serde_json::to_vec(&(domain, value))?);
    let bytes = serde_json::to_vec(&Envelope { value, checksum })?;
    ensure!(bytes.len() <= RECORD_BYTES, "CONTENT_SAVE_CAPTURE_LIMIT");
    Ok(bytes)
}
fn decode<T: Serialize + DeserializeOwned>(domain: &str, bytes: &[u8]) -> Result<T> {
    ensure!(bytes.len() <= RECORD_BYTES, "CORRUPT_CONTENT_SAVE");
    let record: Envelope<T> = serde_json::from_slice(bytes).context("CORRUPT_CONTENT_SAVE")?;
    ensure!(
        record.checksum == hash(&serde_json::to_vec(&(domain, &record.value))?),
        "CORRUPT_CONTENT_SAVE"
    );
    Ok(record.value)
}
fn blob(row: &rusqlite::Row<'_>, column: usize) -> rusqlite::Result<Vec<u8>> {
    let bytes = row.get_ref(column)?.as_blob()?;
    if bytes.len() > RECORD_BYTES {
        return Err(rusqlite::Error::InvalidQuery);
    }
    Ok(bytes.to_vec())
}
fn checkpoint_text(row: &rusqlite::Row<'_>, column: usize) -> rusqlite::Result<String> {
    let key = row.get_ref(column)?.as_str()?;
    if key.len() != 64 {
        return Err(rusqlite::Error::InvalidQuery);
    }
    Ok(key.to_owned())
}

/// Missing/badly typed required records are integrity failures. Database
/// contention, interruption, I/O and authorization errors keep their real cause;
/// never relabel an unavailable SQLite read as corrupt saved work.
fn record_sql<T>(result: rusqlite::Result<T>) -> Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(
            error @ (rusqlite::Error::QueryReturnedNoRows
            | rusqlite::Error::InvalidQuery
            | rusqlite::Error::InvalidColumnType(..)
            | rusqlite::Error::FromSqlConversionFailure(..)
            | rusqlite::Error::IntegralValueOutOfRange(..)
            | rusqlite::Error::Utf8Error(..)),
        ) => Err(error).context("CORRUPT_CONTENT_SAVE"),
        Err(error) => Err(error.into()),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Capture {
    intent: SaveIntent,
    view_generation: u64,
    /// Immutable local event template, including the captured frontier and clocks.
    /// Only its content value is filled from the selected prepared checkpoint.
    event: Event,
    /// Absent fields remain absent when serializing pre-continuation records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    predecessor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    basic_policy: Option<BasicPolicy>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BasicPolicy {
    sequence: u64,
    preserve_times: u8,
}
impl Capture {
    fn validate(&self, operation: Uuid) -> Result<()> {
        self.intent.validate()?;
        let event = &self.event;
        let entity = &self.intent.base.entity;
        ensure!(
            self.intent.operation == operation
                && self.basic_policy.as_ref().is_none_or(|p| {
                    p.preserve_times & !6 == 0 && event.changes.len() == 3
                })
                && event.id
                    == capture_event_id(
                        operation,
                        self.intent.base.generation,
                        self.predecessor.as_deref()
                    )
                && event.repo == self.intent.base.workspace.repository
                && event.branch.id == self.intent.branch
                && !event.device.is_empty()
                && event.device.len() <= 128
                && event.reviewed.is_empty()
                && event.object_sizes.is_empty()
                && event.dependencies.len() <= FRONTIER_LIMIT
                && event
                    .dependencies
                    .iter()
                    .all(|d| !d.is_empty() && d.len() <= 128)
                && (2..=3).contains(&event.changes.len()),
            "CORRUPT_CONTENT_SAVE"
        );
        if let Some(parent) = &self.predecessor {
            ContentDigest::parse(parent).context("CORRUPT_CONTENT_SAVE")?;
        }
        let content = &event.changes[0];
        let alive = &event.changes[1];
        ensure!(
            &content.entity == entity
                && content.field == "content"
                && content.parents
                    == self
                        .intent
                        .content_parent
                        .clone()
                        .into_iter()
                        .collect::<Vec<_>>()
                && content.value == json!(self.intent.base.content)
                && &alive.entity == entity
                && alive.field == "alive"
                && alive.value == json!(true)
                && alive.parents
                    == self
                        .intent
                        .alive_parent
                        .clone()
                        .into_iter()
                        .collect::<Vec<_>>(),
            "CORRUPT_CONTENT_SAVE"
        );
        if let Some(basic) = event.changes.get(2) {
            ensure!(
                &basic.entity == entity && basic.field == "basic" && basic.parents.len() <= 1,
                "CORRUPT_CONTENT_SAVE"
            );
            serde_json::from_value::<BasicInfo>(basic.value.clone())?.validate()?;
        }
        Ok(())
    }
}
fn capture_event_id(operation: Uuid, generation: u64, predecessor: Option<&str>) -> String {
    match predecessor {
        None => operation.to_string(),
        Some(parent) => hash(
            &serde_json::to_vec(&("tkfs:save-event:v2", operation, generation, parent)).unwrap(),
        ),
    }
}

/// A verified metadata handoff to the next save generation. This is not
/// authority to delete journal patches or proof of current byte availability.
/// The session coordinator must adopt it atomically and retain newer work.
#[derive(Clone)]
pub struct SaveContinuation {
    previous: SaveCheckpoint,
    intent: SaveIntent,
    root: PathBuf,
    base: PinnedContent,
}
impl std::fmt::Debug for SaveContinuation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaveContinuation")
            .field("previous", &self.previous)
            .field("intent", &self.intent)
            .finish()
    }
}
impl SaveContinuation {
    pub fn checkpoint(&self) -> &SaveCheckpoint {
        &self.previous
    }
    pub fn intent(&self) -> &SaveIntent {
        &self.intent
    }
    pub fn base_pin(&self) -> &PinnedContent {
        &self.base
    }
    pub fn store_root(&self) -> &Path {
        &self.root
    }
}

/// Evidence of an outer FULL metadata commit, not a transferable authorization
/// to delete journal data. Recovery must query the store, not trust a JSON token.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SaveReceipt {
    operation: Uuid,
    sequence: u64,
    checkpoint: String,
    event: String,
    content: FileContentRef,
}
impl SaveReceipt {
    pub fn operation(&self) -> Uuid {
        self.operation
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn event(&self) -> &str {
        &self.event
    }
    pub fn content(&self) -> &FileContentRef {
        &self.content
    }
}

fn load(
    db: &Connection,
    operation: Uuid,
) -> Result<(Capture, Option<SaveCheckpoint>, Option<SaveReceipt>)> {
    let (capture, selected, receipt) = record_sql(db.query_row(
        "SELECT capture,selected,receipt FROM content_save_operations WHERE operation=?",
        [operation.to_string()],
        |r| {
            Ok((
                blob(r, 0)?,
                if r.get_ref(1)?.data_type() == rusqlite::types::Type::Null {
                    None
                } else {
                    Some(blob(r, 1)?)
                },
                if r.get_ref(2)?.data_type() == rusqlite::types::Type::Null {
                    None
                } else {
                    Some(blob(r, 2)?)
                },
            ))
        },
    ))?;
    decode_state(operation, &capture, selected.as_deref(), receipt.as_deref())
}
fn decode_state(
    operation: Uuid,
    capture: &[u8],
    selected: Option<&[u8]>,
    receipt: Option<&[u8]>,
) -> Result<(Capture, Option<SaveCheckpoint>, Option<SaveReceipt>)> {
    let capture: Capture = decode("capture:v1", capture)?;
    capture
        .validate(operation)
        .context("CORRUPT_CONTENT_SAVE")?;
    let selected: Option<SaveCheckpoint> =
        selected.map(|b| decode("selection:v1", b)).transpose()?;
    if let Some(checkpoint) = &selected {
        checkpoint.validate().context("CORRUPT_CONTENT_SAVE")?;
        ensure!(checkpoint.intent == capture.intent, "CORRUPT_CONTENT_SAVE");
    }
    let receipt: Option<SaveReceipt> = receipt.map(|b| decode("receipt:v1", b)).transpose()?;
    if let Some(receipt) = &receipt {
        let selected = selected.as_ref().context("CORRUPT_CONTENT_SAVE")?;
        ensure!(
            receipt.operation == operation
                && receipt.sequence == selected.sequence
                && receipt.event == capture.event.id
                && receipt.checkpoint == checkpoint_id(selected)?,
            "CORRUPT_CONTENT_SAVE"
        );
        receipt.content.validate().context("CORRUPT_CONTENT_SAVE")?;
    }
    Ok((capture, selected, receipt))
}
type PublishedSave = (Capture, SaveCheckpoint, SaveReceipt);
fn history(db: &Connection, operation: Uuid, key: &str) -> Result<Option<PublishedSave>> {
    let raw=record_sql(db.query_row("SELECT capture,selected,receipt,generation FROM content_save_history WHERE operation=? AND checkpoint=?",
        params![operation.to_string(),key],|r|Ok((blob(r,0)?,blob(r,1)?,blob(r,2)?,r.get::<_,u64>(3)?))).optional())?;
    raw.map(|(c, s, r, g)| {
        let (capture, selected, receipt) = decode_state(operation, &c, Some(&s), Some(&r))?;
        let selected = selected.context("CORRUPT_CONTENT_SAVE")?;
        ensure!(
            checkpoint_id(&selected)? == key && selected.intent.base.generation == g,
            "CORRUPT_CONTENT_SAVE"
        );
        Ok((capture, selected, receipt.context("CORRUPT_CONTENT_SAVE")?))
    })
    .transpose()
}
fn checkpoint_id(checkpoint: &SaveCheckpoint) -> Result<String> {
    Ok(hash(&serde_json::to_vec(&(
        "tkfs:save-selection:v1",
        checkpoint,
    ))?))
}
fn active(db: &Connection) -> Result<(String, u64)> {
    let branch: String = record_sql(db.query_row(
        "SELECT value FROM config WHERE key='active'",
        [],
        |r| r.get(0),
    ))?;
    let generation: String = record_sql(db.query_row(
        "SELECT value FROM config WHERE key='generation'",
        [],
        |r| r.get(0),
    ))?;
    Ok((branch, generation.parse().context("CORRUPT_CONTENT_SAVE")?))
}
fn workspace(db: &Connection) -> Result<Uuid> {
    let id: String = record_sql(db.query_row(
        "SELECT value FROM config WHERE key='content_save_workspace'",
        [],
        |r| r.get(0),
    ))?;
    Uuid::parse_str(&id).context("CORRUPT_CONTENT_SAVE")
}
impl Store {
    /// One durable local mutable-workspace identity shared by native readers
    /// and save intents. Allocate only once, outside the semantic Engine lock.
    pub fn content_workspace_id(&mut self, context: &ReadContext) -> Result<WorkspaceId> {
        context.check()?;
        ensure!(
            self.db.is_autocommit(),
            "CONTENT_SAVE_REQUIRES_OUTER_TRANSACTION"
        );
        let existing: Option<String> = self
            .db
            .query_row(
                "SELECT value FROM config WHERE key='content_save_workspace'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        let workspace = if let Some(id) = existing {
            Uuid::parse_str(&id).context("CORRUPT_CONTENT_SAVE")?
        } else {
            self.with_write("content_workspace_identity", |store| {
                context.check()?;
                store.content_save_enabled()?;
                store.db.execute(
                    "INSERT OR IGNORE INTO config(key,value) VALUES('content_save_workspace',?)",
                    [Uuid::new_v4().to_string()],
                )?;
                workspace(&store.db)
            })?
        };
        context.check()?;
        Ok(WorkspaceId {
            repository: self.repo.clone(),
            workspace,
        })
    }
}
fn current(store: &Store, capture: &Capture) -> Result<()> {
    let intent = &capture.intent;
    ensure!(
        intent.base.workspace.repository == store.repo
            && workspace(&store.db)? == intent.base.workspace.workspace,
        "STALE_CONTENT_WORKSPACE"
    );
    ensure!(
        active(&store.db)? == (intent.branch.clone(), capture.view_generation),
        "STALE_CONTENT_VIEW"
    );
    let (generation, owner): (u64, Option<String>) = record_sql(store.db.query_row(
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

impl Store {
    /// Locate a committed parent of this exact private generation after a
    /// runtime crash/lost reply. Later Saved bytes may already supersede its
    /// private map; callers must adopt this parent before selecting those bytes.
    pub fn committed_content_checkpoint(
        &self,
        intent: &SaveIntent,
        context: &ReadContext,
    ) -> Result<Option<SaveCheckpoint>> {
        context.check()?;
        self.content_save_usable()?;
        intent.validate()?;
        let key: Option<String> = record_sql(self.db.query_row(
            "SELECT checkpoint FROM content_save_history WHERE operation=? AND generation=?",
            params![intent.operation.to_string(), intent.base.generation],
            |r| checkpoint_text(r, 0),
        ).optional())?;
        let Some(key) = key else {
            return Ok(None);
        };
        let (capture, checkpoint, receipt) =
            history(&self.db, intent.operation, &key)?.context("CORRUPT_CONTENT_SAVE")?;
        ensure!(&capture.intent == intent, "CONTENT_SAVE_INTENT_MISMATCH");
        self.verify_content_save_receipt(&capture, &receipt)?;
        context.check()?;
        Ok(Some(checkpoint))
    }
    fn published_checkpoint(&self, checkpoint: &SaveCheckpoint) -> Result<Option<PublishedSave>> {
        let key = checkpoint_id(checkpoint)?;
        if let Some(published) = history(&self.db, checkpoint.intent.operation, &key)? {
            ensure!(&published.1 == checkpoint, "CORRUPT_CONTENT_SAVE");
            self.verify_content_save_receipt(&published.0, &published.2)?;
            return Ok(Some(published));
        }
        let exists: bool = self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM content_save_operations WHERE operation=?)",
            [checkpoint.intent.operation.to_string()],
            |r| r.get(0),
        )?;
        if !exists {
            return Ok(None);
        }
        let (capture, selected, receipt) = load(&self.db, checkpoint.intent.operation)?;
        if selected.as_ref() == Some(checkpoint)
            && let Some(receipt) = receipt
        {
            self.verify_content_save_receipt(&capture, &receipt)?;
            return Ok(Some((capture, checkpoint.clone(), receipt)));
        }
        let continued:bool=self.db.query_row("SELECT EXISTS(SELECT 1 FROM content_save_successors WHERE operation=? AND checkpoint=?)",
            params![checkpoint.intent.operation.to_string(),key],|r|r.get(0))?;
        ensure!(
            !continued && capture.predecessor.as_deref() != Some(&key),
            "CORRUPT_CONTENT_SAVE"
        );
        Ok(None)
    }
    fn archive_publication(
        &self,
        capture: &Capture,
        checkpoint: &SaveCheckpoint,
        receipt: &SaveReceipt,
    ) -> Result<()> {
        let existing:Option<String>=record_sql(self.db.query_row("SELECT checkpoint FROM content_save_history WHERE operation=? AND generation=?",
            params![receipt.operation.to_string(),checkpoint.intent.base.generation],|r|checkpoint_text(r,0)).optional())?;
        ensure!(
            existing.is_none_or(|key| key == receipt.checkpoint),
            "CORRUPT_CONTENT_SAVE"
        );
        self.db.execute("INSERT INTO content_save_history(operation,checkpoint,generation,capture,selected,receipt) VALUES(?,?,?,?,?,?) ON CONFLICT(operation,checkpoint) DO NOTHING",
            params![receipt.operation.to_string(),receipt.checkpoint,checkpoint.intent.base.generation,
                encode("capture:v1",capture)?,encode("selection:v1",checkpoint)?,encode("receipt:v1",receipt)?])?;
        let stored = history(&self.db, receipt.operation, &receipt.checkpoint)?;
        ensure!(
            stored.as_ref() == Some(&(capture.clone(), checkpoint.clone(), receipt.clone())),
            "CONTENT_SAVE_PUBLICATION_LOST"
        );
        self.verify_content_save_receipt(capture, receipt)
    }
    /// A failed rollback invalidates this whole Store handle, including its
    /// derived caches. A pool must discard it and recover via a fresh Store.
    pub fn content_save_connection_failed(&self) -> bool {
        self.save_connection_failed
    }
    fn content_save_usable(&self) -> Result<()> {
        ensure!(
            !self.save_connection_failed,
            "CONTENT_SAVE_CONNECTION_FAILED"
        );
        Ok(())
    }
    fn quarantine_content_save_connection(&mut self) -> Result<()> {
        self.save_connection_failed = true;
        self.view_cache.borrow_mut().take();
        self.caches = StoreCaches::default();
        // The empty replacement cannot perform metadata operations. Closing
        // the old handle also abandons an unrollbackable transaction, releasing
        // its real writer. Do not silently reconnect/retry an ambiguous save.
        let empty = Connection::open_in_memory().context("CONTENT_SAVE_CONNECTION_FAILED")?;
        let old = std::mem::replace(&mut self.db, empty);
        old.close()
            .map_err(|(connection, error)| {
                drop(connection);
                error
            })
            .context("CONTENT_SAVE_CONNECTION_FAILED")
    }
    fn content_save_enabled(&self) -> Result<()> {
        ensure!(
            self.db.query_row(
                "SELECT EXISTS(SELECT 1 FROM config WHERE key='fixture:content_v1' AND value='1')",
                [],
                |r| r.get::<_, bool>(0)
            )?,
            "PEER_NEEDS_UPGRADE: content-v1 is not active for this repository"
        );
        Ok(())
    }
    /// Own the outer writer transaction. A receipt can never escape a caller's
    /// later rollback. Writer acquisition polls the actual deadline/cancel; no
    /// content I/O and no private-journal lock occurs while holding the writer.
    fn content_save_write<T>(
        &mut self,
        context: &ReadContext,
        f: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        context.check()?;
        self.content_save_usable()?;
        ensure!(
            self.db.is_autocommit(),
            "CONTENT_SAVE_REQUIRES_OUTER_TRANSACTION"
        );
        self.content_save_enabled()?;
        let synchronous: i64 = self.db.query_row("PRAGMA synchronous", [], |r| r.get(0))?;
        let mode: String = self.db.query_row("PRAGMA journal_mode", [], |r| r.get(0))?;
        ensure!(
            synchronous >= 2 && mode.eq_ignore_ascii_case("wal"),
            "CONTENT_SAVE_REQUIRES_FULL_WAL"
        );
        let timeout: u64 = self.db.query_row("PRAGMA busy_timeout", [], |r| r.get(0))?;
        self.db.busy_timeout(Duration::from_millis(1))?;
        let acquired = loop {
            if let Err(error) = context.check() {
                break Err(error);
            }
            match self.db.execute_batch("BEGIN IMMEDIATE") {
                Ok(()) => break Ok(()),
                Err(rusqlite::Error::SqliteFailure(error, _))
                    if matches!(
                        error.code,
                        rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
                    ) =>
                {
                    context.cancel.sleep(Duration::from_millis(1));
                }
                Err(error) => break Err(error.into()),
            }
        };
        let restored = self.db.busy_timeout(Duration::from_millis(timeout));
        if let Err(error) = acquired {
            restored?;
            return Err(error);
        }
        let cancel = context.cancel.clone();
        let deadline = context.deadline;
        self.db.progress_handler(
            1000,
            Some(move || cancel.is_cancelled() || std::time::Instant::now() >= deadline),
        );
        let result = catch_unwind(AssertUnwindSafe(|| {
            restored?;
            self.drop_rolled_back_view();
            let value = f(self)?;
            context.check()?;
            self.db.execute_batch("COMMIT")?;
            Ok(value)
        }));
        self.db.progress_handler(0, None::<fn() -> bool>);
        if !matches!(&result, Ok(Ok(_))) {
            self.view_cache.borrow_mut().take();
            if !self.db.is_autocommit()
                && let Err(rollback) = self.db.execute_batch("ROLLBACK")
            {
                let quarantined = self.quarantine_content_save_connection();
                let original = match &result {
                    Ok(Err(error)) => format!("{error:#}"),
                    Err(_) => "panic unwinding the publication".to_owned(),
                    Ok(Ok(_)) => unreachable!(),
                };
                let quarantine = quarantined.err().map(|e| format!("{e:#}"));
                let error = anyhow::anyhow!(
                    "CONTENT_SAVE_ROLLBACK_FAILED: {rollback}; original: {original}; quarantine: {quarantine:?}"
                );
                if let Err(payload) = result {
                    crate::util::log::event!(error, CONTENT_SAVE_ROLLBACK_FAILED, {
                        site: "content.save.unwind", state: self.root.display().to_string(),
                        detail: format!("{error:#}")
                    });
                    resume_unwind(payload);
                }
                return Err(error);
            }
        }
        self.settle_event_cache();
        match result {
            Ok(Err(error)) => {
                context.check()?;
                Err(error)
            }
            Ok(Ok(value)) => Ok(value),
            Err(payload) => resume_unwind(payload),
        }
    }

    /// Capture an existing live file, its exact visible content/alive parents,
    /// namespace frontier and clocks. Starting another local operation supersedes
    /// this operation's publication right, without discarding its recovery bytes.
    pub fn begin_content_save(
        &mut self,
        entity: &str,
        context: &ReadContext,
    ) -> Result<SaveIntent> {
        self.begin_content_save_selection(entity, None, context)
    }
    pub(crate) fn begin_selected_content_save(
        &mut self,
        selection: &ContentSaveSelection,
        context: &ReadContext,
    ) -> Result<SaveIntent> {
        self.begin_content_save_selection(&selection.entity, Some(selection), context)
    }
    fn begin_content_save_selection(
        &mut self,
        entity: &str,
        selection: Option<&ContentSaveSelection>,
        context: &ReadContext,
    ) -> Result<SaveIntent> {
        self.content_save_write(context, |store| {
            let (branch, view_generation) = active(&store.db)?;
            ensure!(branch == store.active, "STALE_CONTENT_VIEW");
            let entry = store.entry(entity)?;
            ensure!(entry.kind == "file" && entry.alive, "FILE_NOT_FOUND");
            let content = if let Some(content) = &entry.content_ref { content.clone() } else {
                let object = entry.content.as_ref().context("MISSING_CONTENT_DESCRIPTOR")?;
                FileContentRef::new(ContentKind::Legacy, ContentDigest::parse(object)?, store.object_size(object)?)?
            };
            if let Some(selected) = selection {
                ensure!(branch == selected.branch && view_generation == selected.generation
                    && content == selected.content
                    && entry.revisions.get("content") == selected.content_parent.as_ref()
                    && entry.revisions.get("alive") == selected.alive_parent.as_ref(),
                    "STALE_CONTENT_VIEW");
            }
            let mut q = store.db.prepare("SELECT event FROM event_frontier WHERE branch=? ORDER BY event LIMIT 257")?;
            let dependencies = q.query_map([&branch], |r| {
                let s=r.get_ref(0)?.as_str()?;
                if s.len()>128 {return Err(rusqlite::Error::InvalidQuery);}
                Ok(s.to_owned())
            })?.collect::<rusqlite::Result<Vec<_>>>()?;
            ensure!(dependencies.len() <= FRONTIER_LIMIT, "CONTENT_SAVE_CAPTURE_LIMIT");
            store.db.execute("INSERT OR IGNORE INTO config(key,value) VALUES('content_save_workspace',?)", [Uuid::new_v4().to_string()])?;
            let operation = Uuid::new_v4();
            let generation: u64 = store.db.query_row(
                "INSERT INTO content_save_entities(branch,entity,generation,owner) VALUES(?,?,1,?)
                 ON CONFLICT(branch,entity) DO UPDATE SET generation=generation+1,owner=excluded.owner RETURNING generation",
                params![branch, entity, operation.to_string()], |r| r.get(0))?;
            let intent = SaveIntent {
                operation, branch: branch.clone(),
                content_parent: entry.revisions.get("content").cloned(),
                alive_parent: entry.revisions.get("alive").cloned(),
                base: FileRevision {workspace: WorkspaceId {repository: store.repo.clone(), workspace: workspace(&store.db)?},
                    entity: entity.to_owned(), generation, content},
            };
            let mut event = Event {id: intent.operation.to_string(), repo:store.repo.clone(), branch:store.branch(&branch)?,
                device:store.device.clone(), created_ms:time(), dependencies, reviewed:vec![], object_sizes:BTreeMap::new(),
                changes:vec![Change {entity:entity.into(), field:"content".into(), parents:intent.content_parent.clone().into_iter().collect(), value:json!(intent.base.content)},
                    Change {entity:entity.into(), field:"alive".into(), parents:intent.alive_parent.clone().into_iter().collect(), value:json!(true)}]};
            if entry.basic.is_some() {
                let mut basic=entry.basic_info();
                basic.write_time=filetime(event.created_ms);
                basic.change_time=filetime(event.created_ms);
                basic.attributes=BasicInfo::attributes(&entry.kind,basic.attributes|0x20)?;
                event.changes.push(Change {entity:entity.into(),field:"basic".into(),
                    parents:entry.revisions.get("basic").cloned().into_iter().collect(),value:json!(basic)});
            }
            let capture=Capture {intent:intent.clone(),view_generation,event,predecessor:None,basic_policy:None};
            capture.validate(intent.operation)?;
            store.db.execute("INSERT INTO content_save_operations(operation,capture) VALUES(?,?)",
                params![intent.operation.to_string(),encode("capture:v1", &capture)?])?;
            Ok(intent)
        })
    }

    /// Persist the coordinator's selected durable checkpoint. A newer selection
    /// fences an old preparation before it can publish. This is not a journal
    /// checkpoint acknowledgement: the adapter must order selection with saves.
    pub fn select_content_checkpoint(
        &mut self,
        checkpoint: &SaveCheckpoint,
        context: &ReadContext,
    ) -> Result<()> {
        checkpoint.validate()?;
        ensure!(checkpoint.needs_publication(), "STALE_WRITE_CHECKPOINT");
        self.content_save_write(context, |store| {
            if let Some((capture, _, receipt)) = store.published_checkpoint(checkpoint)? {
                store.archive_publication(&capture, checkpoint, &receipt)?;
                return Ok(());
            }
            let (mut capture, prior, receipt) = load(&store.db, checkpoint.intent.operation)?;
            ensure!(
                capture.intent == checkpoint.intent,
                "CONTENT_SAVE_INTENT_MISMATCH"
            );
            if let Some(receipt) = receipt {
                ensure!(
                    receipt.checkpoint == checkpoint_id(checkpoint)?,
                    "CONTENT_SAVE_ALREADY_COMMITTED"
                );
                store.verify_content_save_receipt(&capture, &receipt)?;
                return Ok(());
            }
            current(store, &capture)?;
            if prior.is_none() && let Some(policy) = capture.basic_policy.clone() {
                let previous = capture.clone();
                capture.event.created_ms = time();
                let change = capture.event.changes.get_mut(2).context("CORRUPT_CONTENT_SAVE")?;
                let mut basic: BasicInfo = serde_json::from_value(change.value.clone())?;
                let preserve = if policy.sequence == checkpoint.sequence { policy.preserve_times } else { 0 };
                if preserve & 2 == 0 { basic.write_time = filetime(capture.event.created_ms); }
                if preserve & 4 == 0 { basic.change_time = filetime(capture.event.created_ms); }
                change.value = json!(basic);
                capture.basic_policy = None;
                store.replace_unselected_capture(&previous, &capture)?;
            }
            if let Some(prior) = prior {
                ensure!(
                    checkpoint.sequence > prior.sequence || checkpoint == &prior,
                    "STALE_WRITE_CHECKPOINT"
                );
            }
            let changed = store.db.execute(
                "UPDATE content_save_operations SET selected=? WHERE operation=?",
                params![
                    encode("selection:v1", checkpoint)?,
                    checkpoint.intent.operation.to_string()
                ],
            )?;
            ensure!(
                changed == 1
                    && load(&store.db, checkpoint.intent.operation)?.1.as_ref() == Some(checkpoint),
                "CONTENT_SAVE_SELECTION_LOST"
            );
            Ok(())
        })
    }

    /// Atomically publish a selected canonical revision, admission outbox and
    /// idempotent receipt. The current local operation/view and selected sequence
    /// are rechecked under the actual SQLite writer, including foreign commits.
    pub fn publish_content_save(
        &mut self,
        prepared: &PreparedSave,
        context: &ReadContext,
    ) -> Result<SaveReceipt> {
        context.check()?;
        self.content_save_usable()?;
        ensure!(
            prepared.content().total_len == prepared.checkpoint().length,
            "CONTENT_SAVE_PREPARATION_MISMATCH"
        );
        // Store identity is checked before the transaction, without reading or
        // rehashing file bytes. A root prepared in another CAS cannot be published.
        ensure!(
            prepared.pin().provider() == LocalContent::open(&self.root)?.identity(),
            "CONTENT_SAVE_WRONG_STORE"
        );
        let checkpoint = prepared.checkpoint();
        self.content_save_write(context, |store| {
            if let Some((capture, _, receipt)) = store.published_checkpoint(checkpoint)? {
                ensure!(
                    &receipt.content == prepared.content(),
                    "CONTENT_SAVE_RECEIPT_MISMATCH"
                );
                store.archive_publication(&capture, checkpoint, &receipt)?;
                return Ok(receipt);
            }
            let (capture, selected, receipt) = load(&store.db, checkpoint.intent.operation)?;
            ensure!(
                selected.as_ref() == Some(checkpoint),
                "STALE_WRITE_CHECKPOINT"
            );
            if let Some(receipt) = receipt {
                ensure!(
                    &receipt.content == prepared.content(),
                    "CONTENT_SAVE_RECEIPT_MISMATCH"
                );
                store.verify_content_save_receipt(&capture, &receipt)?;
                return Ok(receipt);
            }
            current(store, &capture)?;
            let mut event = capture.event;
            event.changes[0].value = json!(prepared.content());
            crate::util::fault::trigger("content.save.before_metadata")?;
            store.commit(event.clone(), true)?;
            crate::util::fault::trigger("content.save.event_written")?;
            let receipt = SaveReceipt {
                operation: checkpoint.intent.operation,
                sequence: checkpoint.sequence,
                checkpoint: checkpoint_id(checkpoint)?,
                event: event.id,
                content: prepared.content().clone(),
            };
            let changed = store.db.execute(
                "UPDATE content_save_operations SET receipt=? WHERE operation=?",
                params![
                    encode("receipt:v1", &receipt)?,
                    receipt.operation.to_string()
                ],
            )?;
            let (capture, selected, stored) = load(&store.db, receipt.operation)?;
            ensure!(
                changed == 1
                    && stored.as_ref() == Some(&receipt)
                    && selected.as_ref() == Some(checkpoint),
                "CONTENT_SAVE_PUBLICATION_LOST"
            );
            store.verify_content_save_receipt(&capture, &receipt)?;
            store.archive_publication(&capture, checkpoint, &receipt)?;
            crate::util::fault::trigger("content.save.archived")?;
            crate::util::fault::trigger("content.save.receipt_written")?;
            Ok(receipt)
        })
        .and_then(|receipt| {
            // A lost reply here leaves an independently queryable receipt.
            crate::util::fault::trigger("content.save.committed")?;
            Ok(receipt)
        })
    }

    /// Exact checkpoint lookup survives later generations of the same journal
    /// operation. Absence is not authority to retire or reconstruct an intent.
    pub fn content_checkpoint_receipt(
        &self,
        checkpoint: &SaveCheckpoint,
        context: &ReadContext,
    ) -> Result<Option<SaveReceipt>> {
        context.check()?;
        self.content_save_usable()?;
        checkpoint.validate()?;
        ensure!(
            self.db.is_autocommit(),
            "CONTENT_SAVE_REQUIRES_OUTER_TRANSACTION"
        );
        let tx = self.db.unchecked_transaction()?;
        let receipt = self.published_checkpoint(checkpoint)?.map(|(_, _, r)| r);
        context.check()?;
        tx.commit()?;
        Ok(receipt)
    }

    /// Persist the next metadata generation, preserving the journal operation
    /// key. The returned handoff does not modify or retire that journal.
    pub fn continue_content_save(
        &mut self,
        checkpoint: &SaveCheckpoint,
        context: &ReadContext,
    ) -> Result<SaveContinuation> {
        context.check()?;
        self.content_save_usable()?;
        checkpoint.validate()?;
        let root = self.root.canonicalize()?;
        let local = LocalContent::open(&root)?;
        let (intent,content)=self.content_save_write(context,|store| {
            let (previous,selected,receipt)=store.published_checkpoint(checkpoint)?.context("CONTENT_SAVE_NOT_COMMITTED")?;
            ensure!(&selected==checkpoint,"CORRUPT_CONTENT_SAVE");
            store.archive_publication(&previous,checkpoint,&receipt)?;
            let key=checkpoint_id(checkpoint)?;
            let operation=checkpoint.intent.operation;
            let raw=record_sql(store.db.query_row("SELECT capture FROM content_save_successors WHERE operation=? AND checkpoint=?",
                params![operation.to_string(),key],|r|blob(r,0)).optional())?;
            if let Some(raw)=raw {
                let successor:Capture=decode("successor:v1",&raw)?;
                store.verify_successor(&previous,checkpoint,&receipt,&successor)?;
                let current=load(&store.db,operation)?.0;
                if current.intent.base.generation!=successor.intent.base.generation {
                    let child_key:Option<String>=record_sql(store.db.query_row("SELECT checkpoint FROM content_save_history WHERE operation=? AND generation=?",
                        params![operation.to_string(),successor.intent.base.generation],|r|checkpoint_text(r,0)).optional())?;
                    let child=history(&store.db,operation,&child_key.context("CORRUPT_CONTENT_SAVE")?)?.context("CORRUPT_CONTENT_SAVE")?;
                    ensure!(child.0==successor,"CORRUPT_CONTENT_SAVE");
                    store.verify_content_save_receipt(&child.0,&child.2)?;
                } else {ensure!(current==successor,"CORRUPT_CONTENT_SAVE");}
                return Ok((successor.intent,receipt.content));
            }
            let current_capture=load(&store.db,operation)?.0;
            ensure!(current_capture==previous,"CORRUPT_CONTENT_SAVE");
            ensure!(active(&store.db)?==(previous.intent.branch.clone(),previous.view_generation),"STALE_CONTENT_VIEW");
            ensure!(workspace(&store.db)?==previous.intent.base.workspace.workspace && store.repo==previous.intent.base.workspace.repository,"STALE_CONTENT_WORKSPACE");
            let (generation,owner):(u64,Option<String>)=record_sql(store.db.query_row("SELECT generation,owner FROM content_save_entities WHERE branch=? AND entity=?",
                params![previous.intent.branch,previous.intent.base.entity],|r|Ok((r.get(0)?,r.get(1)?))))?;
            let expected=previous.intent.base.generation.checked_add(1).context("WRITE_GENERATION_EXHAUSTED")?;
            ensure!(generation==expected && owner.is_none(),"STALE_CONTENT_SAVE");
            let next=generation.checked_add(1).filter(|n|*n<=i64::MAX as u64).context("WRITE_GENERATION_EXHAUSTED")?;
            crate::util::fault::trigger("content.save.before_successor")?;
            let mut intent=previous.intent.clone();
            intent.base.generation=next; intent.base.content=receipt.content.clone();
            intent.content_parent=Some(receipt.event.clone()); intent.alive_parent=Some(receipt.event.clone());
            let successor=store.successor_capture(intent,previous.view_generation,key.clone())?;
            let changed=store.db.execute("UPDATE content_save_entities SET generation=?,owner=? WHERE branch=? AND entity=? AND generation=? AND owner IS NULL",
                params![next,operation.to_string(),successor.intent.branch,successor.intent.base.entity,generation])?;
            ensure!(changed==1,"CONTENT_SAVE_CONTINUATION_LOST");
            let changed=store.db.execute("UPDATE content_save_operations SET capture=?,selected=NULL,receipt=NULL WHERE operation=?",
                params![encode("capture:v1",&successor)?,operation.to_string()])?;
            ensure!(changed==1,"CONTENT_SAVE_CONTINUATION_LOST");
            store.db.execute("INSERT INTO content_save_successors(operation,checkpoint,capture) VALUES(?,?,?)",
                params![operation.to_string(),key,encode("successor:v1",&successor)?])?;
            let raw=record_sql(store.db.query_row("SELECT capture FROM content_save_successors WHERE operation=? AND checkpoint=?",
                params![operation.to_string(),key],|r|blob(r,0)))?;
            ensure!(decode::<Capture>("successor:v1",&raw)?==successor && load(&store.db,operation)?.0==successor,"CONTENT_SAVE_CONTINUATION_LOST");
            current(store,&successor)?;
            store.verify_successor(&previous,checkpoint,&receipt,&successor)?;
            ensure!(store.published_checkpoint(checkpoint)?.as_ref()==Some(&(previous,checkpoint.clone(),receipt.clone())),"CONTENT_SAVE_CONTINUATION_LOST");
            crate::util::fault::trigger("content.save.successor_written")?;
            Ok((successor.intent,receipt.content))
        })?;
        crate::util::fault::trigger("content.save.successor_committed")?;
        let base = local.pin(&content)?;
        context.check()?;
        Ok(SaveContinuation {
            previous: checkpoint.clone(),
            intent,
            root,
            base,
        })
    }

    fn verify_successor(
        &self,
        previous: &Capture,
        checkpoint: &SaveCheckpoint,
        receipt: &SaveReceipt,
        next: &Capture,
    ) -> Result<()> {
        next.validate(checkpoint.intent.operation)?;
        ensure!(
            next.predecessor.as_deref() == Some(receipt.checkpoint.as_str())
                && next.intent.operation == previous.intent.operation
                && next.intent.branch == previous.intent.branch
                && next.intent.base.workspace == previous.intent.base.workspace
                && next.intent.base.entity == previous.intent.base.entity
                && next.intent.base.generation
                    == previous
                        .intent
                        .base
                        .generation
                        .checked_add(2)
                        .context("CORRUPT_CONTENT_SAVE")?
                && next.intent.base.content == receipt.content
                && next.intent.content_parent.as_deref() == Some(receipt.event.as_str())
                && next.intent.alive_parent.as_deref() == Some(receipt.event.as_str())
                && next.view_generation == previous.view_generation,
            "CORRUPT_CONTENT_SAVE"
        );
        Ok(())
    }

    fn successor_capture(
        &self,
        intent: SaveIntent,
        view_generation: u64,
        predecessor: String,
    ) -> Result<Capture> {
        let mut q = self
            .db
            .prepare("SELECT event FROM event_frontier WHERE branch=? ORDER BY event LIMIT 257")?;
        let dependencies = q
            .query_map([&intent.branch], |r| {
                let s = r.get_ref(0)?.as_str()?;
                if s.is_empty() || s.len() > 128 {
                    return Err(rusqlite::Error::InvalidQuery);
                }
                Ok(s.to_owned())
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        ensure!(
            dependencies.len() <= FRONTIER_LIMIT,
            "CONTENT_SAVE_CAPTURE_LIMIT"
        );
        let mut event = Event {
            id: capture_event_id(intent.operation, intent.base.generation, Some(&predecessor)),
            repo: self.repo.clone(),
            branch: self.branch(&intent.branch)?,
            device: self.device.clone(),
            created_ms: time(),
            dependencies,
            reviewed: vec![],
            object_sizes: BTreeMap::new(),
            changes: vec![
                Change {
                    entity: intent.base.entity.clone(),
                    field: "content".into(),
                    parents: intent.content_parent.clone().into_iter().collect(),
                    value: json!(intent.base.content),
                },
                Change {
                    entity: intent.base.entity.clone(),
                    field: "alive".into(),
                    parents: intent.alive_parent.clone().into_iter().collect(),
                    value: json!(true),
                },
            ],
        };
        let entry = self
            .view(&intent.branch)?
            .projection
            .entries
            .get(&intent.base.entity)
            .cloned()
            .context("FILE_NOT_FOUND")?;
        ensure!(entry.kind == "file", "INVALID_WRITE_INTENT");
        if entry.basic.is_some() {
            let mut basic = entry.basic_info();
            basic.write_time = filetime(event.created_ms);
            basic.change_time = filetime(event.created_ms);
            basic.attributes = BasicInfo::attributes(&entry.kind, basic.attributes | 0x20)?;
            event.changes.push(Change {
                entity: entry.id,
                field: "basic".into(),
                parents: entry.revisions.get("basic").cloned().into_iter().collect(),
                value: json!(basic),
            });
        }
        let capture = Capture {
            intent,
            view_generation,
            event,
            predecessor: Some(predecessor),
            basic_policy: None,
        };
        capture.validate(capture.intent.operation)?;
        Ok(capture)
    }

    fn verify_content_save_receipt(&self, capture: &Capture, receipt: &SaveReceipt) -> Result<()> {
        let mut event = capture.event.clone();
        event.changes[0].value = json!(receipt.content);
        let payload: String = record_sql(self.db.query_row(
            "SELECT payload FROM events WHERE id=?",
            [&receipt.event],
            |r| {
                let text = r.get_ref(0)?.as_str()?;
                if text.len() > RECORD_BYTES {
                    return Err(rusqlite::Error::InvalidQuery);
                }
                Ok(text.to_owned())
            },
        ))?;
        ensure!(
            payload == serde_json::to_string(&event)?,
            "CORRUPT_CONTENT_SAVE"
        );
        let reference = admission::reference_id(&event.id, &capture.intent.base.entity);
        let root: bool = self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM content_root_metadata WHERE reference=? AND event=? AND entity=? AND branch=? AND root=? AND shared=?)",
            params![reference,event.id,capture.intent.base.entity,event.branch.id,receipt.content.encode()?.as_slice(),event.branch.shared], |r| r.get(0))?;
        ensure!(root, "CORRUPT_CONTENT_SAVE");
        let present: bool=self.db.query_row("SELECT EXISTS(SELECT 1 FROM content_root_outbox WHERE reference=? AND generation=1 AND action='grant' AND root=? AND repository=? AND branch=? AND shared=? AND publishable=?)",
            params![reference,receipt.content.encode()?.as_slice(),event.repo,event.branch.id,event.branch.shared,event.branch.shared],|r| r.get(0))?;
        ensure!(present, "CORRUPT_CONTENT_SAVE");
        Ok(())
    }

    /// Query durable publication after a lost reply/restart, independently of
    /// current checkout/operation generation. Never retire a journal on absence.
    pub fn content_save_receipt(
        &self,
        operation: Uuid,
        context: &ReadContext,
    ) -> Result<Option<SaveReceipt>> {
        context.check()?;
        self.content_save_usable()?;
        ensure!(
            self.db.is_autocommit(),
            "CONTENT_SAVE_REQUIRES_OUTER_TRANSACTION"
        );
        let tx = self.db.unchecked_transaction()?;
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM content_save_operations WHERE operation=?)",
            [operation.to_string()],
            |r| r.get(0),
        )?;
        if !exists {
            context.check()?;
            tx.commit()?;
            return Ok(None);
        }
        let (capture, _, receipt) = load(&tx, operation)?;
        if let Some(receipt) = &receipt {
            self.verify_content_save_receipt(&capture, receipt)?;
        }
        context.check()?;
        tx.commit()?;
        Ok(receipt)
    }
}
