//! Private chunk overlays and durable save intents. This is not a mounted
//! flush implementation or a metadata publication surface. Only checkpoints
//! are recoverable promises; ordinary writes use SQLite NORMAL transactions.
use super::{
    ContentProvider, LocalContent, PinnedContent, ReadContext,
    local::{EditSource, EditSpan},
    overlay_map::{MapKind, MapRoot, MapValue, MapWalk, MapWork, OverlayMap},
};
use crate::model::{
    content::{CHUNK_SIZE, ContentKind, Digest, FileContentRef, MAX_FILE_SIZE},
    workspace::{ByteRange, FileRevision, limits},
};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest as _, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use uuid::Uuid;

const MASK: usize = CHUNK_SIZE as usize / 8;
pub const PATCH_BYTES: usize = CHUNK_SIZE as usize + MASK;
const RECORD_BYTES: usize = 4096;
const APPLICATION_ID: i64 = 0x544b5753;
const JOURNAL_VERSION: i64 = 4;

mod retention;
pub use retention::{
    RETAINED_READ_LIMIT, RecoveryRoot, RecoveryRootsCursor, RecoveryRootsFence, RecoveryRootsPage,
    RetainedRead,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SaveIntent {
    pub operation: Uuid,
    pub branch: String,
    pub base: FileRevision,
    /// Captured causal parents, never reconstructed from a newer live view
    /// during recovery. Namespace/create intents are owned by the adapter.
    pub content_parent: Option<String>,
    pub alive_parent: Option<String>,
}

/// Process-local optional recovery for native immutable bases. Neither the
/// provider interface nor durable save records contain network authority.
#[derive(Clone, Copy)]
pub(crate) struct BaseWaitLimits {
    pub stall: Duration,
    pub cancelled: fn() -> bool,
}
pub(crate) struct BaseRequest<'a> {
    pub intent: &'a SaveIntent,
    pub pin: &'a PinnedContent,
    pub range: ByteRange,
    pub limits: Option<BaseWaitLimits>,
}
pub(crate) trait BaseWait: Send + Sync {
    fn wait(
        &self,
        request: BaseRequest<'_>,
        error: anyhow::Error,
        context: &ReadContext,
    ) -> Result<()>;
}

#[derive(Clone, Copy)]
enum BaseRead {
    Local,
    Waiting(Option<BaseWaitLimits>),
}
pub(crate) struct PreflightContext<'a> {
    pub target: &'a LocalContent,
    pub limits: Option<BaseWaitLimits>,
    pub context: &'a ReadContext,
}
pub(crate) struct MutationSelection {
    working: Working,
    length: u64,
    base_visible_len: u64,
    overwrite: ByteRange,
    all_changes: bool,
    limits: Option<BaseWaitLimits>,
}
impl MutationSelection {
    pub(crate) fn check(&self, context: &ReadContext) -> Result<()> {
        context.check()?;
        ensure!(
            self.limits.is_none_or(|limits| !(limits.cancelled)()),
            "CONTENT_CANCELLED"
        );
        Ok(())
    }
}
struct MutationSource<'a> {
    session: &'a WriteSession,
    selection: &'a MutationSelection,
}
impl EditSource for MutationSource<'_> {
    fn check(&self, context: &ReadContext) -> Result<()> {
        self.session.validate_mutation(self.selection, context)
    }
    fn span(&mut self, offset: u64, len: u64, context: &ReadContext) -> Result<EditSpan> {
        self.check(context)?;
        let end = offset.checked_add(len).context("RANGE_OVERFLOW")?;
        ensure!(end <= self.selection.length, "RANGE_OUT_OF_BOUNDS");
        let incoming = self.selection.overwrite;
        let mut changed = incoming.len > 0 && offset < incoming.end()? && end > incoming.offset;
        if self.selection.all_changes && !changed {
            let db = self.session.journal.lock(context)?;
            let current = self.session.owned(&db)?.0;
            ensure!(current == self.selection.working, "STALE_WRITE_CHECKPOINT");
            let patches = map(&db, self.session.operation, MapKind::Patches);
            changed = patches.has_range(
                current.pending,
                offset / CHUNK_SIZE,
                end.div_ceil(CHUNK_SIZE),
                0,
                &mut MapWork::default(),
                context,
            )? || patches.has_range(
                current.roots.patches,
                offset / CHUNK_SIZE,
                end.div_ceil(CHUNK_SIZE),
                current.roots.floor,
                &mut MapWork::default(),
                context,
            )?;
        }
        Ok(if changed {
            EditSpan::Changed
        } else if end <= self.selection.base_visible_len {
            EditSpan::Unchanged
        } else if offset >= self.selection.base_visible_len {
            EditSpan::Zero
        } else {
            EditSpan::Changed
        })
    }
    fn read(&mut self, offset: u64, out: &mut [u8], context: &ReadContext) -> Result<()> {
        self.check(context)?;
        out.fill(0);
        let end = (offset + out.len() as u64).min(self.selection.working.length);
        let incoming = self.selection.overwrite;
        let mut from = offset;
        while from < end {
            if incoming.len > 0 && from >= incoming.offset && from < incoming.end()? {
                from = end.min(incoming.end()?);
                continue;
            }
            let until = if incoming.len > 0 && from < incoming.offset {
                end.min(incoming.offset)
            } else {
                end
            };
            self.session.read_using(
                None,
                from,
                &mut out[(from - offset) as usize..(until - offset) as usize],
                None,
                false,
                BaseRead::Local,
                context,
            )?;
            from = until;
        }
        self.check(context)
    }
}

impl SaveIntent {
    pub(crate) fn validate(&self) -> Result<()> {
        self.base.content.validate()?;
        ensure!(
            self.base.content.total_len <= MAX_FILE_SIZE
                && self.base.generation > 0
                && [
                    &self.branch,
                    &self.base.entity,
                    &self.base.workspace.repository
                ]
                .into_iter()
                .chain(self.content_parent.iter())
                .chain(self.alive_parent.iter())
                .all(|s| !s.is_empty() && s.len() <= 128 && !s.chars().any(char::is_control)),
            "INVALID_WRITE_INTENT"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SaveCheckpoint {
    pub intent: SaveIntent,
    pub sequence: u64,
    pub length: u64,
    pub base_visible_len: u64,
    pub patch_count: u64,
    pub patch_map: Digest,
    roots: OverlayRoots,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OverlayRoots {
    patches: MapRoot,
    cuts: MapRoot,
    floor: u64,
}
impl OverlayRoots {
    fn empty(op: Uuid) -> Self {
        Self {
            patches: MapRoot::empty(op, MapKind::Patches),
            cuts: MapRoot::empty(op, MapKind::Truncations),
            floor: 0,
        }
    }
    fn validate(&self, op: Uuid, sequence: u64, length: u64) -> Result<()> {
        self.patches.validate(op, MapKind::Patches)?;
        self.cuts.validate(op, MapKind::Truncations)?;
        ensure!(
            self.floor <= sequence
                && self.patches.max_sequence() <= sequence
                && self.cuts.max_sequence() <= sequence
                && self.patches.entries() <= length.div_ceil(CHUNK_SIZE),
            "CORRUPT_WRITE_JOURNAL"
        );
        Ok(())
    }
}
impl SaveCheckpoint {
    /// A rebased baseline remains recoverable but does not publish a new edit.
    pub fn needs_publication(&self) -> bool {
        self.sequence > self.roots.floor
    }
    pub(crate) fn validate(&self) -> Result<()> {
        self.intent.validate()?;
        self.roots
            .validate(self.intent.operation, self.sequence, self.length)?;
        ensure!(
            self.sequence > 0
                && self.length <= MAX_FILE_SIZE
                && self.base_visible_len <= self.length
                && self.base_visible_len <= self.intent.base.content.total_len,
            "CORRUPT_WRITE_JOURNAL"
        );
        ensure!(
            self.patch_count == self.roots.patches.entries()
                && self.patch_map == self.roots.patches.digest(),
            "CORRUPT_WRITE_JOURNAL"
        );
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Working {
    intent: SaveIntent,
    owner: Uuid,
    sequence: u64,
    length: u64,
    base_visible_len: u64,
    dirty: bool,
    roots: OverlayRoots,
    pending: MapRoot,
    adoption: Option<Adoption>,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Adoption {
    previous: Digest,
    from_owner: Uuid,
    archive: Uuid,
}
impl Working {
    fn validate(&self) -> Result<()> {
        self.intent.validate()?;
        self.roots
            .validate(self.intent.operation, self.sequence, self.length)?;
        self.pending
            .validate(self.intent.operation, MapKind::Patches)?;
        ensure!(
            self.length <= MAX_FILE_SIZE
                && self.base_visible_len <= self.length
                && self.base_visible_len <= self.intent.base.content.total_len,
            "CORRUPT_WRITE_JOURNAL"
        );
        ensure!(
            self.pending.entries() <= self.length.div_ceil(CHUNK_SIZE)
                && self.pending.entries() <= (limits::DIRTY_BYTES / PATCH_BYTES) as u64
                && self.pending.max_sequence() <= self.sequence,
            "CORRUPT_WRITE_JOURNAL"
        );
        ensure!(
            self.dirty || self.pending.entries() == 0,
            "CORRUPT_WRITE_JOURNAL"
        );
        if let Some(adoption) = &self.adoption {
            ensure!(
                adoption.from_owner != self.owner
                    && !adoption.archive.is_nil()
                    && adoption.previous != Digest([0; 32])
                    && self.roots.floor > 0,
                "CORRUPT_WRITE_JOURNAL"
            );
        }
        Ok(())
    }
    fn advance(&mut self) -> Result<()> {
        self.sequence = self
            .sequence
            .checked_add(1)
            .context("WRITE_GENERATION_EXHAUSTED")?;
        self.dirty = true;
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record<T> {
    value: T,
    checksum: Digest,
}
fn encode<T: Serialize>(value: T) -> Result<Vec<u8>> {
    let checksum = Digest::bytes(&serde_json::to_vec(&value)?);
    let bytes = serde_json::to_vec(&Record { value, checksum })?;
    ensure!(bytes.len() <= RECORD_BYTES, "INVALID_WRITE_INTENT");
    Ok(bytes)
}
fn decode<T: Serialize + DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    ensure!(bytes.len() <= RECORD_BYTES, "CORRUPT_WRITE_JOURNAL");
    let record: Record<T> = serde_json::from_slice(bytes).context("CORRUPT_WRITE_JOURNAL")?;
    ensure!(
        record.checksum == Digest::bytes(&serde_json::to_vec(&record.value)?),
        "CORRUPT_WRITE_JOURNAL"
    );
    Ok(record.value)
}
fn blob(row: &rusqlite::Row<'_>, column: usize, max: usize) -> rusqlite::Result<Vec<u8>> {
    let bytes = row.get_ref(column)?.as_blob()?;
    if bytes.len() > max {
        return Err(rusqlite::Error::InvalidQuery);
    }
    Ok(bytes.to_vec())
}
fn uuid_text(row: &rusqlite::Row<'_>) -> rusqlite::Result<String> {
    let id = row.get_ref(0)?.as_str()?;
    if id.len() != 36 {
        return Err(rusqlite::Error::InvalidQuery);
    }
    Ok(id.to_owned())
}
fn journal_sql<T>(result: rusqlite::Result<T>) -> Result<T> {
    match result {
        Err(
            error @ (rusqlite::Error::QueryReturnedNoRows
            | rusqlite::Error::InvalidQuery
            | rusqlite::Error::InvalidColumnType(..)
            | rusqlite::Error::FromSqlConversionFailure(..)),
        ) => Err(error).context("CORRUPT_WRITE_JOURNAL"),
        other => Ok(other?),
    }
}
fn state(db: &Connection, op: Uuid) -> Result<(Working, Option<SaveCheckpoint>)> {
    let (working, saved, dirty, queued, seal, publish) = journal_sql(db.query_row(
        "SELECT working,saved,dirty,queued,seal,publish FROM sessions WHERE id=?",
        [op.to_string()],
        |r| {
            let dirty = r.get::<_, u8>(2)?;
            let publish = r.get::<_, u8>(5)?;
            if dirty > 1 || publish > 1 {
                return Err(rusqlite::Error::InvalidQuery);
            }
            Ok((
                blob(r, 0, RECORD_BYTES)?,
                if r.get_ref(1)?.data_type() == rusqlite::types::Type::Null {
                    None
                } else {
                    Some(blob(r, 1, RECORD_BYTES)?)
                },
                dirty == 1,
                r.get::<_, u64>(3)?,
                blob(r, 4, 32)?,
                publish == 1,
            ))
        },
    ))?;
    ensure!(
        seal == scope_seal(op, &working, saved.as_deref(), dirty).0,
        "CORRUPT_WRITE_JOURNAL"
    );
    let working: Working = decode(&working)?;
    working.validate()?;
    ensure!(
        working.intent.operation == op
            && working.dirty == dirty
            && working.pending.entries() == queued,
        "CORRUPT_WRITE_JOURNAL"
    );
    let saved: Option<SaveCheckpoint> = saved.map(|b| decode(&b)).transpose()?;
    ensure!(
        publish
            == saved
                .as_ref()
                .is_some_and(SaveCheckpoint::needs_publication),
        "CORRUPT_WRITE_JOURNAL"
    );
    if let Some(saved) = &saved {
        saved.validate()?;
        ensure!(
            saved.intent == working.intent && saved.sequence <= working.sequence,
            "CORRUPT_WRITE_JOURNAL"
        );
    }
    Ok((working, saved))
}
fn saved_state(db: &Connection, op: Uuid) -> Result<SaveCheckpoint> {
    let (bytes, publish) = journal_sql(db.query_row(
        "SELECT saved,publish FROM sessions WHERE id=? AND saved IS NOT NULL",
        [op.to_string()],
        |r| Ok((blob(r, 0, RECORD_BYTES)?, r.get::<_, u8>(1)?)),
    ))?;
    let saved: SaveCheckpoint = decode(&bytes)?;
    saved.validate()?;
    ensure!(
        saved.intent.operation == op && u8::from(saved.needs_publication()) == publish,
        "CORRUPT_WRITE_JOURNAL"
    );
    Ok(saved)
}
fn map(db: &Connection, op: Uuid, kind: MapKind) -> OverlayMap<'_> {
    OverlayMap::new(db, op, kind)
}
fn checkpoint_key(checkpoint: &SaveCheckpoint) -> Result<Digest> {
    Ok(Digest::bytes(&serde_json::to_vec(&(
        "tkfs:journal-checkpoint:v1",
        checkpoint,
    ))?))
}
fn recorded_checkpoint(db: &Connection, key: Digest) -> Result<SaveCheckpoint> {
    let (op, raw) = journal_sql(db.query_row(
        "SELECT op,record FROM checkpoint_records WHERE key=?",
        [key.to_string()],
        |r| Ok((uuid_text(r)?, blob(r, 1, RECORD_BYTES)?)),
    ))?;
    let checkpoint: SaveCheckpoint = decode(&raw)?;
    checkpoint.validate()?;
    ensure!(
        checkpoint.intent.operation == Uuid::parse_str(&op)? && checkpoint_key(&checkpoint)? == key,
        "CORRUPT_WRITE_JOURNAL"
    );
    Ok(checkpoint)
}
fn remember_checkpoint(db: &Connection, checkpoint: &SaveCheckpoint) -> Result<()> {
    checkpoint.validate()?;
    let key = checkpoint_key(checkpoint)?;
    db.execute(
        "INSERT INTO checkpoint_records(key,op,record) VALUES(?,?,?) ON CONFLICT(key) DO NOTHING",
        params![
            key.to_string(),
            checkpoint.intent.operation.to_string(),
            encode(checkpoint)?
        ],
    )?;
    ensure!(
        recorded_checkpoint(db, key)? == *checkpoint,
        "CORRUPT_WRITE_JOURNAL"
    );
    Ok(())
}
fn retained_seal(id: Uuid, op: Uuid, working: &[u8], saved: &[u8]) -> Digest {
    let mut h = Sha256::new();
    h.update(b"tkfs:retained-overlay:v1\0");
    h.update(id.as_bytes());
    h.update(op.as_bytes());
    h.update((working.len() as u64).to_le_bytes());
    h.update(working);
    h.update((saved.len() as u64).to_le_bytes());
    h.update(saved);
    Digest(h.finalize().into())
}
fn retained(db: &Connection, id: Uuid) -> Result<(Working, SaveCheckpoint, Digest)> {
    let (op, raw, saved, seal) = journal_sql(db.query_row(
        "SELECT op,working,saved,seal FROM retained_overlays WHERE id=?",
        [id.to_string()],
        |r| {
            Ok((
                uuid_text(r)?,
                blob(r, 1, RECORD_BYTES)?,
                blob(r, 2, RECORD_BYTES)?,
                blob(r, 3, 32)?,
            ))
        },
    ))?;
    let op = Uuid::parse_str(&op)?;
    let expected = retained_seal(id, op, &raw, &saved);
    ensure!(seal == expected.0, "CORRUPT_WRITE_JOURNAL");
    let working: Working = decode(&raw)?;
    let saved: SaveCheckpoint = decode(&saved)?;
    working.validate()?;
    saved.validate()?;
    ensure!(
        working.intent.operation == op
            && saved.intent == working.intent
            && saved.sequence <= working.sequence,
        "CORRUPT_WRITE_JOURNAL"
    );
    Ok((working, saved, expected))
}
fn retain(db: &Connection, id: Uuid, working: &Working, saved: &SaveCheckpoint) -> Result<()> {
    let raw = encode(working)?;
    let saved_raw = encode(saved)?;
    let seal = retained_seal(id, working.intent.operation, &raw, &saved_raw);
    db.execute(
        "INSERT INTO retained_overlays(id,op,working,saved,seal) VALUES(?,?,?,?,?)",
        params![
            id.to_string(),
            working.intent.operation.to_string(),
            raw,
            saved_raw,
            seal.0.as_slice()
        ],
    )?;
    let (actual, actual_saved, actual_seal) = retained(db, id)?;
    ensure!(
        actual == *working && actual_saved == *saved && actual_seal == seal,
        "CORRUPT_WRITE_JOURNAL"
    );
    adjust_inventory(db, None, seal)
}
fn verify_saved(db: &Connection, saved: &SaveCheckpoint, context: &ReadContext) -> Result<()> {
    saved.validate()?;
    let op = saved.intent.operation;
    let mut work = MapWork::default();
    map(db, op, MapKind::Patches).verify_root(saved.roots.patches, &mut work, context)?;
    map(db, op, MapKind::Truncations).verify_root(saved.roots.cuts, &mut work, context)
}
fn verify_maps(
    db: &Connection,
    working: &Working,
    saved: Option<&SaveCheckpoint>,
    context: &ReadContext,
) -> Result<()> {
    working.validate()?;
    let op = working.intent.operation;
    let mut work = MapWork::default();
    let patches = map(db, op, MapKind::Patches);
    patches.verify_root(working.roots.patches, &mut work, context)?;
    patches.verify_root(working.pending, &mut work, context)?;
    map(db, op, MapKind::Truncations).verify_root(working.roots.cuts, &mut work, context)?;
    if let Some(saved) = saved {
        verify_saved(db, saved, context)?;
    }
    Ok(())
}
fn persist(
    db: &Connection,
    value: &Working,
    saved: Option<&SaveCheckpoint>,
    insert: bool,
) -> Result<()> {
    value.validate()?;
    if let Some(saved) = saved {
        saved.validate()?;
        ensure!(
            saved.intent == value.intent && saved.sequence <= value.sequence,
            "CORRUPT_WRITE_JOURNAL"
        );
    }
    let op = value.intent.operation;
    let old = if insert {
        None
    } else {
        let bytes = journal_sql(db.query_row(
            "SELECT seal FROM sessions WHERE id=?",
            [op.to_string()],
            |r| blob(r, 0, 32),
        ))?;
        Some(Digest(
            bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("CORRUPT_WRITE_JOURNAL"))?,
        ))
    };
    let bytes = encode(value)?;
    let publish = saved.is_some_and(SaveCheckpoint::needs_publication);
    let saved = saved.map(encode).transpose()?;
    let seal = scope_seal(op, &bytes, saved.as_deref(), value.dirty);
    if insert {
        db.execute(
            "INSERT INTO sessions(id,working,saved,dirty,queued,seal,publish) VALUES(?,?,?,?,?,?,?)",
            params![
                op.to_string(),
                bytes,
                saved,
                value.dirty,
                value.pending.entries(),
                seal.0.as_slice()
                ,publish
            ],
        )?;
    } else {
        db.execute(
            "UPDATE sessions SET working=?,saved=?,dirty=?,queued=?,seal=?,publish=? WHERE id=?",
            params![
                bytes,
                saved,
                value.dirty,
                value.pending.entries(),
                seal.0.as_slice(),
                publish,
                op.to_string()
            ],
        )?;
    }
    let (actual, actual_saved) = state(db, op)?;
    ensure!(
        actual == *value
            && actual_saved.as_ref()
                == saved
                    .as_ref()
                    .map(|b| decode::<SaveCheckpoint>(b))
                    .transpose()?
                    .as_ref(),
        "CORRUPT_WRITE_JOURNAL"
    );
    adjust_inventory(db, old, seal)?;
    Ok(())
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Inventory {
    count: u64,
    xor: Digest,
}
fn inventory(db: &Connection) -> Result<Inventory> {
    let raw = journal_sql(
        db.query_row("SELECT record FROM inventory WHERE id=0", [], |r| {
            blob(r, 0, RECORD_BYTES)
        }),
    )?;
    decode(&raw)
}
fn scope_seal(op: Uuid, working: &[u8], saved: Option<&[u8]>, dirty: bool) -> Digest {
    let mut h = Sha256::new();
    h.update(b"tkfs:write-scope:v4\0");
    h.update(op.as_bytes());
    h.update((working.len() as u64).to_le_bytes());
    h.update(working);
    h.update([u8::from(dirty)]);
    h.update([u8::from(saved.is_some())]);
    if let Some(saved) = saved {
        h.update((saved.len() as u64).to_le_bytes());
        h.update(saved);
    }
    Digest(h.finalize().into())
}
fn xor_into(left: &mut Digest, right: Digest) {
    for (a, b) in left.0.iter_mut().zip(right.0) {
        *a ^= b;
    }
}
fn adjust_inventory(db: &Connection, old: Option<Digest>, new: Digest) -> Result<()> {
    let mut value = inventory(db)?;
    if let Some(old) = old {
        value.count = value
            .count
            .checked_sub(1)
            .context("CORRUPT_WRITE_JOURNAL")?;
        xor_into(&mut value.xor, old);
    }
    value.count = value
        .count
        .checked_add(1)
        .context("WRITE_GENERATION_EXHAUSTED")?;
    xor_into(&mut value.xor, new);
    db.execute(
        "UPDATE inventory SET record=? WHERE id=0",
        [encode(&value)?],
    )?;
    ensure!(inventory(db)? == value, "CORRUPT_WRITE_JOURNAL");
    Ok(())
}
#[derive(Clone)]
struct Chunk {
    index: u64,
    hash: Digest,
    valid: usize,
    sequence: u64,
}
fn chunk_meta(
    db: &Connection,
    root: MapRoot,
    op: Uuid,
    index: u64,
    floor: u64,
    context: &ReadContext,
) -> Result<Option<Chunk>> {
    map(db, op, MapKind::Patches)
        .get(root, index, floor, &mut MapWork::default(), context)?
        .map(|value| {
            let MapValue::Patch {
                hash,
                valid,
                sequence,
            } = value
            else {
                anyhow::bail!("CORRUPT_WRITE_JOURNAL")
            };
            Ok(Chunk {
                index,
                hash,
                valid: valid as usize,
                sequence,
            })
        })
        .transpose()
}
fn put_chunk(
    db: &Connection,
    working: &mut Working,
    chunk: &Chunk,
    context: &ReadContext,
) -> Result<()> {
    let patches = map(db, working.intent.operation, MapKind::Patches);
    let value = MapValue::Patch {
        hash: chunk.hash,
        valid: chunk.valid as u32,
        sequence: working.sequence,
    };
    let mut work = MapWork::default();
    working.roots.patches = patches.set(
        working.roots.patches,
        chunk.index,
        value,
        working.roots.floor,
        &mut work,
        context,
    )?;
    working.pending = patches.set(working.pending, chunk.index, value, 0, &mut work, context)?;
    Ok(())
}
fn payload(db: &Connection, chunk: &Chunk) -> Result<Vec<u8>> {
    let bytes=journal_sql(db.query_row("SELECT CASE WHEN typeof(data)='blob' AND length(data)=73728 THEN data END FROM payloads WHERE hash=?",
        [chunk.hash.to_string()],|r|blob(r,0,PATCH_BYTES)))?;
    ensure!(
        bytes.len() == PATCH_BYTES && Digest::bytes(&bytes) == chunk.hash,
        "CORRUPT_WRITE_JOURNAL"
    );
    Ok(bytes)
}
fn clip(bytes: &mut [u8], valid: usize) {
    for i in valid..CHUNK_SIZE as usize {
        bytes[i] = 0;
        bytes[CHUNK_SIZE as usize + i / 8] &= !(1 << (i % 8));
    }
}
fn overlay(
    db: &Connection,
    working: &Working,
    selected: Option<&SaveCheckpoint>,
    index: u64,
    context: &ReadContext,
) -> Result<Option<(Chunk, Vec<u8>)>> {
    let roots = selected.map_or(&working.roots, |c| &c.roots);
    chunk_meta(
        db,
        roots.patches,
        working.intent.operation,
        index,
        roots.floor,
        context,
    )?
    .map(|c| {
        let bytes = payload(db, &c)?;
        Ok((c, bytes))
    })
    .transpose()
}

fn read_overlay_chunk(
    provider: &dyn ContentProvider,
    pin: &PinnedContent,
    base_len: u64,
    from: u64,
    target: &mut [u8],
    patch: Option<&(Chunk, Vec<u8>)>,
    context: &ReadContext,
) -> Result<()> {
    let index = from / CHUNK_SIZE;
    let until = from
        .checked_add(target.len() as u64)
        .context("RANGE_OVERFLOW")?;
    ensure!(until <= (index + 1) * CHUNK_SIZE, "INVALID_WRITE_INTENT");
    target.fill(0);
    let covered = |position: u64| {
        patch.is_some_and(|(c, b)| {
            let i = (position - index * CHUNK_SIZE) as usize;
            i < c.valid && b[CHUNK_SIZE as usize + i / 8] & (1 << (i % 8)) != 0
        })
    };
    let base_end = until.min(base_len);
    if (from..base_end).any(|position| !covered(position)) {
        provider.read_range(
            pin,
            from,
            &mut target[..(base_end - from) as usize],
            context,
        )?;
    }
    if let Some((c, b)) = patch {
        for position in from..until {
            let i = (position - index * CHUNK_SIZE) as usize;
            if i < c.valid && b[CHUNK_SIZE as usize + i / 8] & (1 << (i % 8)) != 0 {
                target[(position - from) as usize] = b[i];
            }
        }
    }
    context.check()
}

/// One private journal per worker. Share its Arc among sessions. No metadata
/// engine lock may be held by its caller. The existing worker store lock must
/// exclude another worker process opening this journal for the same store.
pub struct WriteJournal {
    root: PathBuf,
    db: Mutex<Connection>,
    maintenance_cursor: Mutex<Option<ReclaimCursor>>,
    identity: Uuid,
    reclamation_error: Mutex<Option<String>>,
    connection_failed: AtomicBool,
    retained_readers: Mutex<std::collections::BTreeMap<Uuid, usize>>,
    staging: AtomicUsize,
    checkpoint_work: Mutex<CheckpointWork>,
}
/// Logical committed FULL checkpoint transactions and dirty sessions advanced.
/// These counters exclude adoption/metadata commits and physical I/O.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CheckpointWork {
    pub commits: u64,
    pub sessions: u64,
}
struct StagingLease {
    journal: Arc<WriteJournal>,
}
impl Drop for StagingLease {
    fn drop(&mut self) {
        self.journal.staging.fetch_sub(1, Ordering::AcqRel);
    }
}
struct JournalDb<'a>(MutexGuard<'a, Connection>, &'a AtomicBool);
impl std::ops::Deref for JournalDb<'_> {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        &self.0
    }
}
impl std::ops::DerefMut for JournalDb<'_> {
    fn deref_mut(&mut self) -> &mut Connection {
        &mut self.0
    }
}
impl Drop for JournalDb<'_> {
    fn drop(&mut self) {
        self.0.progress_handler(0, None::<fn() -> bool>);
        if !self.0.is_autocommit() {
            // A failed RAII rollback must never expose tentative adoption state
            // as a committed lost-reply marker on the next request.
            self.1.store(true, Ordering::Release);
            let _ = self.0.execute_batch("ROLLBACK");
            if let Ok(empty) = Connection::open_in_memory() {
                let failed = std::mem::replace(&mut *self.0, empty);
                if let Err((failed, _)) = failed.close() {
                    drop(failed);
                }
            }
        }
    }
}
pub struct ReclaimPage {
    /// Payload rows deleted/examined; node rows are reported separately.
    pub removed: usize,
    pub examined: usize,
    /// Inventory records and map traversal units, including verified nodes.
    pub validated: usize,
    pub nodes_removed: usize,
    pub nodes_examined: usize,
    /// In-flight adoption may still need nodes not installed in a durable root.
    pub nodes_deferred: bool,
    pub next: Option<ReclaimCursor>,
}
pub struct ReclaimCursor {
    journal: Uuid,
    token: (i64, u64),
    phase: ReclaimPhase,
    epoch: Uuid,
}
enum ReclaimPhase {
    Audit(Box<InventoryAudit>),
    Payload(Option<Digest>),
    Nodes(Option<Digest>),
}
struct InventoryAudit {
    expected: Inventory,
    seen: Inventory,
    after: Option<Uuid>,
    archives: bool,
    scope: Option<ScopeAudit>,
}
struct ScopeAudit {
    id: Uuid,
    op: Uuid,
    maps: Vec<MapAudit>,
}
struct MapAudit {
    kind: MapKind,
    length: u64,
    sequence: u64,
    walk: MapWalk,
}
fn change_token(db: &Connection) -> Result<(i64, u64)> {
    Ok((
        db.query_row("PRAGMA data_version", [], |r| r.get(0))?,
        db.total_changes(),
    ))
}
fn audit_start(db: &Connection) -> Result<ReclaimPhase> {
    Ok(ReclaimPhase::Audit(Box::new(InventoryAudit {
        expected: inventory(db)?,
        seen: Inventory {
            count: 0,
            xor: Digest([0; 32]),
        },
        after: None,
        archives: false,
        scope: None,
    })))
}
impl WriteJournal {
    pub(crate) fn store_root(&self) -> &Path {
        &self.root
    }
    pub fn connection_failed(&self) -> bool {
        self.connection_failed.load(Ordering::Acquire)
    }
    fn prune_for_adoption(
        &self,
        op: Uuid,
        root: MapRoot,
        kind: MapKind,
        floor: u64,
        work: &mut AdoptionWork,
        context: &ReadContext,
    ) -> Result<MapRoot> {
        root.validate(op, kind)?;
        context.check()?;
        if root.entries() == 0 {
            return Ok(root);
        }
        let mut cursor = {
            let db = self.lock(context)?;
            map(&db, op, kind).prune(root, floor)?
        };
        loop {
            let mut db = self.lock(context)?;
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let page = map(&tx, op, kind).prune_page(&mut cursor, 64, &mut work.map, context)?;
            context.check()?;
            tx.commit()?;
            drop(db);
            work.pages += 1;
            crate::util::fault::trigger("content.write.adoption_page")?;
            if let Some(root) = page.root {
                return Ok(root);
            }
        }
    }
    pub fn open(root: &Path) -> Result<Arc<Self>> {
        ensure!(root.is_dir(), "CONTENT_STORE_NOT_INITIALIZED");
        let root = root.canonicalize()?;
        let db = Connection::open(root.join("write-sessions-v1.sqlite"))?;
        db.busy_timeout(Duration::ZERO)?;
        let app: i64 = db.query_row("PRAGMA application_id", [], |r| r.get(0))?;
        let version: i64 = db.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        let tables: usize = db.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table'",
            [],
            |r| r.get(0),
        )?;
        ensure!(
            (app == APPLICATION_ID && version == JOURNAL_VERSION)
                || (app == 0 && version == 0 && tables == 0),
            "WRITE_JOURNAL_NEEDS_UPGRADE"
        );
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA cache_size=-1536; PRAGMA temp_store=FILE; PRAGMA wal_autocheckpoint=512; PRAGMA journal_size_limit=8388608;")?;
        if app == 0 {
            db.execute_batch("BEGIN IMMEDIATE;
                CREATE TABLE inventory(id INTEGER PRIMARY KEY CHECK(id=0),record BLOB NOT NULL);
                CREATE TABLE sessions(id TEXT PRIMARY KEY,working BLOB NOT NULL,saved BLOB,dirty INTEGER NOT NULL CHECK(dirty IN (0,1)),queued INTEGER NOT NULL CHECK(queued>=0),publish INTEGER NOT NULL CHECK(publish IN (0,1)),seal BLOB NOT NULL CHECK(length(seal)=32));
                CREATE INDEX pending_sessions ON sessions(id) WHERE publish=1;
                CREATE TABLE checkpoint_records(key TEXT PRIMARY KEY,op TEXT NOT NULL,record BLOB NOT NULL CHECK(length(record)<=4096));
                CREATE TABLE retained_overlays(id TEXT PRIMARY KEY,op TEXT NOT NULL,working BLOB NOT NULL CHECK(length(working)<=4096),saved BLOB NOT NULL CHECK(length(saved)<=4096),seal BLOB NOT NULL CHECK(length(seal)=32));
                CREATE INDEX dirty_sessions ON sessions(id) WHERE dirty=1;
                CREATE TABLE payloads(hash TEXT PRIMARY KEY,data BLOB NOT NULL CHECK(length(data)=73728));
                CREATE TABLE gc_marks(hash TEXT PRIMARY KEY,epoch TEXT NOT NULL) WITHOUT ROWID;
                PRAGMA application_id=1414223699; PRAGMA user_version=4;")?;
            OverlayMap::ensure_schema(&db)?;
            db.execute(
                "INSERT INTO inventory VALUES(0,?)",
                [encode(Inventory {
                    count: 0,
                    xor: Digest([0; 32]),
                })?],
            )?;
            db.execute_batch("COMMIT")?;
            crate::core::objects::sync_dir(&root)?;
        }
        inventory(&db)?;
        db.execute_batch("CREATE TEMP TABLE gc_node_marks(hash BLOB PRIMARY KEY CHECK(length(hash)=32),epoch TEXT NOT NULL) WITHOUT ROWID; PRAGMA temp.cache_size=-512;")?;
        db.execute_batch("PRAGMA synchronous=NORMAL;")?;
        Ok(Arc::new(Self {
            root,
            db: Mutex::new(db),
            maintenance_cursor: Mutex::new(None),
            identity: Uuid::new_v4(),
            reclamation_error: Mutex::new(None),
            connection_failed: AtomicBool::new(false),
            retained_readers: Mutex::new(std::collections::BTreeMap::new()),
            staging: AtomicUsize::new(0),
            checkpoint_work: Mutex::new(CheckpointWork::default()),
        }))
    }
    fn lock(&self, context: &ReadContext) -> Result<JournalDb<'_>> {
        loop {
            context.check()?;
            ensure!(!self.connection_failed(), "WRITE_JOURNAL_CONNECTION_FAILED");
            match self.db.try_lock() {
                Ok(db) => {
                    let cancel = context.cancel.clone();
                    let deadline = context.deadline;
                    db.progress_handler(
                        1000,
                        Some(move || {
                            cancel.is_cancelled() || std::time::Instant::now() >= deadline
                        }),
                    );
                    ensure!(!self.connection_failed(), "WRITE_JOURNAL_CONNECTION_FAILED");
                    return Ok(JournalDb(db, &self.connection_failed));
                }
                Err(TryLockError::Poisoned(_)) => anyhow::bail!("WRITE_JOURNAL_POISONED"),
                Err(TryLockError::WouldBlock) => std::thread::sleep(Duration::from_millis(1)),
            }
        }
    }
    pub fn create(
        self: &Arc<Self>,
        intent: SaveIntent,
        provider: Arc<dyn ContentProvider>,
        context: &ReadContext,
    ) -> Result<WriteSession> {
        intent.validate()?;
        context.check()?;
        let base = provider.pin(&intent.base.content)?;
        let working = Working {
            roots: OverlayRoots::empty(intent.operation),
            pending: MapRoot::empty(intent.operation, MapKind::Patches),
            adoption: None,
            length: intent.base.content.total_len,
            base_visible_len: intent.base.content.total_len,
            intent,
            owner: Uuid::new_v4(),
            sequence: 0,
            dirty: false,
        };
        let mut db = self.lock(context)?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        persist(&tx, &working, None, true)?;
        verify_maps(&tx, &working, None, context)?;
        tx.commit()?;
        context.check()?;
        Ok(WriteSession {
            journal: self.clone(),
            base_wait: None,
            provider,
            base,
            operation: working.intent.operation,
            visible_len: AtomicU64::new(working.length),
            owner: working.owner,
            converted_base: Mutex::new(None),
            adoption_work: None,
        })
    }
    /// Return at most one bounded page. Recovery/retention consumers restart
    /// enumeration on journal changes; this UUID cursor is not a change journal.
    pub fn pending(
        &self,
        after: Option<Uuid>,
        limit: usize,
        context: &ReadContext,
    ) -> Result<Vec<SaveCheckpoint>> {
        let db = self.lock(context)?;
        let mut query =
            db.prepare("SELECT id FROM sessions WHERE publish=1 AND id>? ORDER BY id LIMIT ?")?;
        let ids: Vec<String> = query
            .query_map(
                params![
                    after.map(|u| u.to_string()).unwrap_or_default(),
                    limit.clamp(1, 256)
                ],
                uuid_text,
            )?
            .collect::<rusqlite::Result<_>>()?;
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            context.check()?;
            out.push(saved_state(&db, Uuid::parse_str(&id)?)?);
        }
        context.check()?;
        Ok(out)
    }
    /// Resume only the last durable checkpoint. A fresh owner fences all old
    /// handles, even on this same journal Arc. Uncheckpointed edits are discarded.
    pub fn resume(
        self: &Arc<Self>,
        operation: Uuid,
        provider: Arc<dyn ContentProvider>,
        context: &ReadContext,
    ) -> Result<WriteSession> {
        let checkpoint = saved_state(&*self.lock(context)?, operation)?;
        let base = provider.pin(&checkpoint.intent.base.content)?;
        let mut db = self.lock(context)?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let saved = saved_state(&tx, operation)?;
        ensure!(saved == checkpoint, "STALE_WRITE_CHECKPOINT");
        verify_saved(&tx, &checkpoint, context)?;
        // A damaged newer buffered header cannot make the older promised
        // checkpoint unrecoverable. A new UUID fences its previous owner.
        let sequence = state(&tx, operation)
            .ok()
            .map_or(checkpoint.sequence, |s| s.0.sequence)
            .checked_add(1)
            .context("WRITE_GENERATION_EXHAUSTED")?;
        let working = Working {
            intent: checkpoint.intent.clone(),
            owner: Uuid::new_v4(),
            sequence,
            length: checkpoint.length,
            base_visible_len: checkpoint.base_visible_len,
            dirty: false,
            roots: checkpoint.roots.clone(),
            pending: MapRoot::empty(operation, MapKind::Patches),
            adoption: None,
        };
        persist(&tx, &working, Some(&checkpoint), false)?;
        tx.commit()?;
        context.check()?;
        Ok(WriteSession {
            journal: self.clone(),
            base_wait: None,
            provider,
            base,
            operation,
            owner: working.owner,
            visible_len: AtomicU64::new(working.length),
            converted_base: Mutex::new(None),
            adoption_work: None,
        })
    }
    fn needs_checkpoint(db: &Connection, working: &Working, additional: usize) -> Result<bool> {
        let (chunks, dirty): (usize, usize) = db.query_row(
            "SELECT coalesce(sum(queued),0),count(*) FROM sessions WHERE dirty=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        ensure!(
            chunks <= limits::DIRTY_BYTES / PATCH_BYTES
                && dirty <= limits::SAVE_QUEUE
                && additional <= limits::READ_BYTES / CHUNK_SIZE as usize + 1,
            "CORRUPT_WRITE_JOURNAL"
        );
        Ok((chunks + additional) * PATCH_BYTES > limits::DIRTY_BYTES
            || (!working.dirty && dirty >= limits::SAVE_QUEUE))
    }
    fn checkpoint(
        &self,
        only: Option<Uuid>,
        initiator: (Uuid, Uuid),
        context: &ReadContext,
    ) -> Result<()> {
        self.checkpoint_inner(only, initiator, None, context)
            .map(|_| ())
    }
    pub fn checkpoint_work(&self) -> CheckpointWork {
        *crate::util::lock::lock(&self.checkpoint_work)
    }
    /// Advance only these same-journal owners in one FULL transaction. Results
    /// preserve input order. A failure acknowledges none; a lost reply can leave
    /// all selected Saved headers committed. Ordinary Working is not discarded.
    pub fn checkpoint_group(
        &self,
        sessions: &[&WriteSession],
        context: &ReadContext,
    ) -> Result<Vec<Option<SaveCheckpoint>>> {
        context.check()?;
        ensure!(
            sessions.len() <= limits::SAVE_QUEUE,
            "CONTENT_RESOURCE_LIMIT"
        );
        if sessions.is_empty() {
            return Ok(Vec::new());
        }
        let mut seen = std::collections::BTreeSet::new();
        let mut owners = Vec::with_capacity(sessions.len());
        for session in sessions {
            ensure!(
                std::ptr::eq(self, session.journal.as_ref()),
                "CONTENT_SAVE_WRONG_STORE"
            );
            ensure!(seen.insert(session.operation), "CONTENT_RESOURCE_LIMIT");
            owners.push((session.operation, session.owner));
        }
        self.checkpoint_inner(None, owners[0], Some(&owners), context)
    }
    fn checkpoint_inner(
        &self,
        only: Option<Uuid>,
        initiator: (Uuid, Uuid),
        batch: Option<&[(Uuid, Uuid)]>,
        context: &ReadContext,
    ) -> Result<Vec<Option<SaveCheckpoint>>> {
        context.check()?;
        crate::util::fault::trigger("content.write.before_checkpoint")?;
        let mut db = self.lock(context)?;
        db.execute_batch("PRAGMA synchronous=FULL;")?;
        let result = (|| -> Result<Vec<Option<SaveCheckpoint>>> {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            ensure!(
                state(&tx, initiator.0)?.0.owner == initiator.1,
                "STALE_WRITE_SESSION"
            );
            let ids: Vec<Uuid> = if let Some(batch) = batch {
                let mut ids = Vec::new();
                for &(operation, owner) in batch {
                    context.check()?;
                    let (working, saved) = state(&tx, operation)?;
                    ensure!(working.owner == owner, "STALE_WRITE_SESSION");
                    verify_maps(&tx, &working, saved.as_ref(), context)?;
                    if working.dirty {
                        ids.push(operation);
                    }
                }
                ids
            } else {
                let mut q=tx.prepare("SELECT id FROM sessions WHERE dirty=1 AND (?1 IS NULL OR id=?1) ORDER BY id LIMIT 65")?;
                q.query_map([only.map(|o| o.to_string())], uuid_text)?
                    .map(|id| Ok(Uuid::parse_str(&id?)?))
                    .collect::<Result<_>>()?
            };
            ensure!(ids.len() <= limits::SAVE_QUEUE, "CORRUPT_WRITE_JOURNAL");
            let dirty_count = ids.len();
            for op in ids {
                context.check()?;
                let (mut w, prior) = state(&tx, op)?;
                verify_maps(&tx, &w, prior.as_ref(), context)?;
                let patches = map(&tx, op, MapKind::Patches);
                let mut walk = patches.walk(w.pending, 0)?;
                loop {
                    let page =
                        patches.walk_page(&mut walk, 256, &mut MapWork::default(), context)?;
                    for (index, value) in page.values {
                        let MapValue::Patch {
                            hash,
                            valid,
                            sequence,
                        } = value
                        else {
                            anyhow::bail!("CORRUPT_WRITE_JOURNAL")
                        };
                        ensure!(
                            sequence <= w.sequence
                                && index < w.length.div_ceil(CHUNK_SIZE)
                                && u64::from(valid)
                                    <= (w.length - index * CHUNK_SIZE).min(CHUNK_SIZE),
                            "CORRUPT_WRITE_JOURNAL"
                        );
                        let c =
                            chunk_meta(&tx, w.roots.patches, op, index, w.roots.floor, context)?
                                .context("CORRUPT_WRITE_JOURNAL")?;
                        ensure!(
                            c.hash == hash && c.valid == valid as usize && c.sequence == sequence,
                            "CORRUPT_WRITE_JOURNAL"
                        );
                        payload(&tx, &c)?;
                    }
                    if page.complete {
                        break;
                    }
                }
                let saved = SaveCheckpoint {
                    intent: w.intent.clone(),
                    sequence: w.sequence,
                    length: w.length,
                    base_visible_len: w.base_visible_len,
                    patch_count: w.roots.patches.entries(),
                    patch_map: w.roots.patches.digest(),
                    roots: w.roots.clone(),
                };
                w.dirty = false;
                w.pending = MapRoot::empty(op, MapKind::Patches);
                remember_checkpoint(&tx, &saved)?;
                persist(&tx, &w, Some(&saved), false)?;
            }
            let selected = batch
                .map(|owners| {
                    owners
                        .iter()
                        .map(|&(operation, _)| {
                            Ok(state(&tx, operation)?
                                .1
                                .filter(SaveCheckpoint::needs_publication))
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?
                .unwrap_or_default();
            crate::util::fault::trigger("content.write.before_checkpoint_commit")?;
            context.check()?;
            tx.commit()?;
            let mut work = crate::util::lock::lock(&self.checkpoint_work);
            work.commits = work.commits.saturating_add(1);
            work.sessions = work.sessions.saturating_add(dirty_count as u64);
            Ok(selected)
        })();
        db.progress_handler(0, None::<fn() -> bool>);
        let normal = db.execute_batch("PRAGMA synchronous=NORMAL;");
        drop(db);
        context.check()?;
        let selected = result?;
        normal?;
        crate::util::fault::trigger("content.write.checkpointed")?;
        self.maintain(context)?;
        context.check()?;
        Ok(selected)
    }
    pub fn queued_patch_bytes(&self, context: &ReadContext) -> Result<usize> {
        let count: usize = self.lock(context)?.query_row(
            "SELECT coalesce(sum(queued),0) FROM sessions WHERE dirty=1",
            [],
            |r| r.get(0),
        )?;
        ensure!(
            count <= limits::DIRTY_BYTES / PATCH_BYTES,
            "CORRUPT_WRITE_JOURNAL"
        );
        context.check()?;
        Ok(count * PATCH_BYTES)
    }
    /// Conservative bounded orphan cleanup, independent of metadata retention.
    /// Saved, current and archived overlay nodes/payloads are always retained.
    /// Each page audits/examines at most 256 records/traversal units; SQL mark
    /// writes and their verification are additional work within those units.
    pub fn reclaim_orphans(
        &self,
        cursor: Option<ReclaimCursor>,
        context: &ReadContext,
    ) -> Result<ReclaimPage> {
        let result = self.reclaim_page(cursor, context);
        match &result {
            Ok(page) if page.next.is_none() => {
                *crate::util::lock::lock(&self.reclamation_error) = None
            }
            Err(error) if context.check().is_ok() => {
                let mut episode = crate::util::lock::lock(&self.reclamation_error);
                if episode.is_none() {
                    *episode = Some(error.to_string().chars().take(1024).collect());
                }
            }
            _ => {}
        }
        result
    }
    /// Maintenance failure is independent of an already committed healthy
    /// mutation. The runtime must expose this state and keep driving audits.
    pub fn reclamation_error(&self) -> Option<String> {
        crate::util::lock::lock(&self.reclamation_error).clone()
    }
    fn reclaim_page(
        &self,
        cursor: Option<ReclaimCursor>,
        context: &ReadContext,
    ) -> Result<ReclaimPage> {
        let mut db = self.lock(context)?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let token = change_token(&tx)?;
        let (mut phase, epoch) = match cursor {
            Some(cursor) if cursor.journal == self.identity && cursor.token == token => {
                (cursor.phase, cursor.epoch)
            }
            _ => (audit_start(&tx)?, Uuid::new_v4()),
        };
        let mut remaining = 256usize;
        let mut validated = 0;
        loop {
            context.check()?;
            let ReclaimPhase::Audit(audit) = &mut phase else {
                break;
            };
            if remaining == 0 {
                break;
            }
            if let Some(scope) = &mut audit.scope {
                let Some(root) = scope.maps.last_mut() else {
                    audit.after = Some(scope.id);
                    audit.scope = None;
                    continue;
                };
                let page = map(&tx, scope.op, root.kind).walk_page(
                    &mut root.walk,
                    remaining,
                    &mut MapWork::default(),
                    context,
                )?;
                remaining -= page.processed;
                validated += page.processed;
                for hash in page.nodes {
                    context.check()?;
                    tx.execute("INSERT INTO temp.gc_node_marks(hash,epoch) VALUES(?,?) ON CONFLICT(hash) DO UPDATE SET epoch=excluded.epoch",
                        params![hash.0.as_slice(),epoch.to_string()])?;
                    let marked: bool = journal_sql(tx.query_row(
                        "SELECT epoch=?1 FROM temp.gc_node_marks WHERE hash=?2",
                        params![epoch.to_string(), hash.0.as_slice()],
                        |r| r.get(0),
                    ))?;
                    ensure!(marked, "CORRUPT_WRITE_JOURNAL");
                }
                for (index, value) in page.values {
                    context.check()?;
                    ensure!(value.sequence() <= root.sequence, "CORRUPT_WRITE_JOURNAL");
                    if let MapValue::Patch { hash, valid, .. } = value {
                        ensure!(
                            index < root.length.div_ceil(CHUNK_SIZE)
                                && u64::from(valid)
                                    <= (root.length - index * CHUNK_SIZE).min(CHUNK_SIZE),
                            "CORRUPT_WRITE_JOURNAL"
                        );
                        let hash = hash.to_string();
                        tx.execute("INSERT INTO gc_marks(hash,epoch) VALUES(?,?) ON CONFLICT(hash) DO UPDATE SET epoch=excluded.epoch",params![hash,epoch.to_string()])?;
                        let marked: String = journal_sql(tx.query_row(
                            "SELECT epoch FROM gc_marks WHERE hash=?",
                            [hash],
                            |r| r.get(0),
                        ))?;
                        ensure!(marked == epoch.to_string(), "CORRUPT_WRITE_JOURNAL");
                    }
                }
                if page.complete {
                    scope.maps.pop();
                }
                continue;
            }
            let table = if audit.archives {
                "retained_overlays"
            } else {
                "sessions"
            };
            let id = tx
                .query_row(
                    &format!("SELECT id FROM {table} WHERE id>? ORDER BY id LIMIT 1"),
                    [audit.after.map(|u| u.to_string()).unwrap_or_default()],
                    uuid_text,
                )
                .optional()?;
            let Some(id) = id else {
                if !audit.archives {
                    audit.archives = true;
                    audit.after = None;
                    continue;
                }
                ensure!(audit.seen == audit.expected, "CORRUPT_WRITE_JOURNAL");
                phase = ReclaimPhase::Payload(None);
                break;
            };
            let id = Uuid::parse_str(&id)?;
            let (working, saved, seal) = if audit.archives {
                let (working, saved, seal) = retained(&tx, id)?;
                (working, Some(saved), seal.0.to_vec())
            } else {
                let (working, saved) = state(&tx, id)?;
                let seal = journal_sql(tx.query_row(
                    "SELECT seal FROM sessions WHERE id=?",
                    [id.to_string()],
                    |r| blob(r, 0, 32),
                ))?;
                (working, saved, seal)
            };
            let op = working.intent.operation;
            ensure!(seal.len() == 32, "CORRUPT_WRITE_JOURNAL");
            xor_into(&mut audit.seen.xor, Digest(seal.try_into().unwrap()));
            audit.seen.count = audit
                .seen
                .count
                .checked_add(1)
                .context("CORRUPT_WRITE_JOURNAL")?;
            let mut roots = vec![
                (
                    MapKind::Patches,
                    working.length,
                    working.sequence,
                    working.roots.patches,
                ),
                (
                    MapKind::Truncations,
                    working.length,
                    working.sequence,
                    working.roots.cuts,
                ),
                (
                    MapKind::Patches,
                    working.length,
                    working.sequence,
                    working.pending,
                ),
            ];
            if let Some(saved) = saved {
                roots.push((
                    MapKind::Patches,
                    saved.length,
                    saved.sequence,
                    saved.roots.patches,
                ));
                roots.push((
                    MapKind::Truncations,
                    saved.length,
                    saved.sequence,
                    saved.roots.cuts,
                ));
            }
            let mut unique = Vec::new();
            for root in roots {
                if !unique.contains(&root) {
                    unique.push(root);
                }
            }
            let maps = unique
                .into_iter()
                .map(|(kind, length, sequence, root)| {
                    Ok(MapAudit {
                        kind,
                        length,
                        sequence,
                        walk: map(&tx, op, kind).walk(root, 0)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            audit.scope = Some(ScopeAudit { id, op, maps });
            remaining -= 1;
            validated += 1;
        }
        let mut removed = 0;
        let mut examined = 0;
        let mut complete = false;
        if let ReclaimPhase::Payload(after) = &mut phase
            && remaining > 0
        {
            let hashes: Vec<String> = {
                let mut q =
                    tx.prepare("SELECT hash FROM payloads WHERE hash>? ORDER BY hash LIMIT ?")?;
                q.query_map(
                    params![after.map(|h| h.to_string()).unwrap_or_default(), remaining],
                    |r| {
                        let hash = r.get_ref(0)?.as_str()?;
                        if hash.len() != 64 {
                            return Err(rusqlite::Error::InvalidQuery);
                        }
                        Ok(hash.to_owned())
                    },
                )?
                .collect::<rusqlite::Result<_>>()?
            };
            examined = hashes.len();
            for hash in &hashes {
                context.check()?;
                Digest::parse(hash)?;
                removed+=tx.execute("DELETE FROM payloads WHERE hash=?1 AND NOT EXISTS(SELECT 1 FROM gc_marks WHERE hash=?1 AND epoch=?2)",params![hash,epoch.to_string()])?;
                tx.execute("DELETE FROM gc_marks WHERE hash=?1 AND NOT EXISTS(SELECT 1 FROM payloads WHERE hash=?1)",[hash])?;
            }
            let payload_done = examined < remaining;
            remaining -= examined;
            if let Some(hash) = hashes.last() {
                *after = Some(Digest::parse(hash)?);
            }
            if payload_done {
                phase = ReclaimPhase::Nodes(None);
            }
        }
        let mut nodes_removed = 0;
        let mut nodes_examined = 0;
        let mut nodes_deferred = false;
        if let ReclaimPhase::Nodes(after) = &mut phase
            && remaining > 0
        {
            // Admission and this test share the journal mutex. A lease spans
            // every staging commit through the final root transaction/error.
            nodes_deferred = self.staging.load(Ordering::Acquire) > 0;
            if !nodes_deferred {
                let hashes: Vec<Digest> = {
                    let mut q=tx.prepare("SELECT hash FROM private_overlay_nodes_v1 WHERE hash>? ORDER BY hash LIMIT ?")?;
                    let lower = after.map(|h| h.0.to_vec()).unwrap_or_default();
                    q.query_map(params![lower, remaining], |r| {
                        let hash = r.get_ref(0)?.as_blob()?;
                        if hash.len() != 32 {
                            return Err(rusqlite::Error::InvalidQuery);
                        }
                        Ok(Digest(hash.try_into().unwrap()))
                    })?
                    .collect::<rusqlite::Result<_>>()?
                };
                nodes_examined = hashes.len();
                for hash in &hashes {
                    context.check()?;
                    nodes_removed+=tx.execute("DELETE FROM private_overlay_nodes_v1 WHERE hash=?1 AND NOT EXISTS(SELECT 1 FROM temp.gc_node_marks WHERE hash=?1 AND epoch=?2)",
                        params![hash.0.as_slice(),epoch.to_string()])?;
                    tx.execute("DELETE FROM temp.gc_node_marks WHERE hash=?1 AND NOT EXISTS(SELECT 1 FROM private_overlay_nodes_v1 WHERE hash=?1)",[hash.0.as_slice()])?;
                }
                complete = nodes_examined < remaining;
                if let Some(hash) = hashes.last() {
                    *after = Some(*hash);
                }
            }
        }
        let token = change_token(&tx)?;
        context.check()?;
        tx.commit()?;
        context.check()?;
        Ok(ReclaimPage {
            removed,
            examined,
            validated,
            nodes_removed,
            nodes_examined,
            nodes_deferred,
            next: if complete {
                None
            } else {
                Some(ReclaimCursor {
                    journal: self.identity,
                    token,
                    phase,
                    epoch,
                })
            },
        })
    }
    fn maintain(&self, context: &ReadContext) -> Result<()> {
        let mut cursor = match self.maintenance_cursor.try_lock() {
            Ok(cursor) => cursor,
            Err(TryLockError::WouldBlock) => return Ok(()),
            Err(TryLockError::Poisoned(error)) => {
                let mut cursor = error.into_inner();
                *cursor = None;
                cursor
            }
        };
        if let Ok(page) = self.reclaim_orphans(cursor.take(), context) {
            *cursor = page.next;
        }
        context.check()
    }
}

pub struct WriteSession {
    journal: Arc<WriteJournal>,
    base_wait: Option<Arc<dyn BaseWait>>,
    provider: Arc<dyn ContentProvider>,
    base: PinnedContent,
    visible_len: AtomicU64,
    operation: Uuid,
    owner: Uuid,
    converted_base: Mutex<Option<PinnedContent>>,
    adoption_work: Option<AdoptionWork>,
}
/// Staging map work only; excludes preflight root checks, SQLite headers,
/// WAL/device I/O and payloads.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct AdoptionWork {
    pub map: MapWork,
    pub pages: usize,
    pub replay: bool,
}

/// Installed and pinned canonical content for a particular durable selection.
/// It does not acknowledge a metadata commit or retire the recovery journal.
#[derive(Clone)]
pub struct PreparedSave {
    checkpoint: SaveCheckpoint,
    owner: Uuid,
    content: PinnedContent,
    _converted_base: Option<PinnedContent>,
}
impl std::fmt::Debug for PreparedSave {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedSave")
            .field("checkpoint", &self.checkpoint)
            .field("content", self.content.reference())
            .finish()
    }
}
impl PreparedSave {
    pub fn checkpoint(&self) -> &SaveCheckpoint {
        &self.checkpoint
    }
    pub fn content(&self) -> &FileContentRef {
        self.content.reference()
    }
    pub fn pin(&self) -> &PinnedContent {
        &self.content
    }
}
struct CheckpointEdit<'a> {
    session: &'a WriteSession,
    checkpoint: &'a SaveCheckpoint,
    converted: Option<(&'a dyn ContentProvider, &'a PinnedContent)>,
}
impl EditSource for CheckpointEdit<'_> {
    fn check(&self, context: &ReadContext) -> Result<()> {
        self.session
            .selected(&*self.session.journal.lock(context)?, self.checkpoint)?;
        context.check()
    }
    fn span(&mut self, offset: u64, len: u64, context: &ReadContext) -> Result<EditSpan> {
        let end = offset.checked_add(len).context("RANGE_OVERFLOW")?;
        ensure!(end <= self.checkpoint.length, "RANGE_OUT_OF_BOUNDS");
        let db = self.session.journal.lock(context)?;
        self.session.selected(&db, self.checkpoint)?;
        let changed = map(&db, self.session.operation, MapKind::Patches).has_range(
            self.checkpoint.roots.patches,
            offset / CHUNK_SIZE,
            end.div_ceil(CHUNK_SIZE),
            self.checkpoint.roots.floor,
            &mut MapWork::default(),
            context,
        )?;
        context.check()?;
        Ok(if changed {
            EditSpan::Changed
        } else if end <= self.checkpoint.base_visible_len {
            EditSpan::Unchanged
        } else if offset >= self.checkpoint.base_visible_len {
            EditSpan::Zero
        } else {
            EditSpan::Changed
        })
    }
    fn read(&mut self, offset: u64, out: &mut [u8], context: &ReadContext) -> Result<()> {
        self.session.read_using(
            Some(self.checkpoint),
            offset,
            out,
            self.converted,
            false,
            BaseRead::Local,
            context,
        )
    }
}
impl WriteSession {
    pub(crate) fn validate_mutation(
        &self,
        selection: &MutationSelection,
        context: &ReadContext,
    ) -> Result<()> {
        selection.check(context)?;
        ensure!(
            self.owned(&*self.journal.lock(context)?)?.0 == selection.working,
            "STALE_WRITE_CHECKPOINT"
        );
        selection.check(context)
    }
    pub(crate) fn preflight_mutation(
        &self,
        overwrite: ByteRange,
        resize: Option<u64>,
        all_changes: bool,
        preflight: &PreflightContext<'_>,
    ) -> Result<MutationSelection> {
        let context = preflight.context;
        let working = self.owned(&*self.journal.lock(context)?)?.0;
        let length = resize.unwrap_or(working.length.max(overwrite.end()?));
        ensure!(length <= MAX_FILE_SIZE, "FILE_TOO_LARGE");
        let selection = MutationSelection {
            base_visible_len: working.base_visible_len.min(length),
            working,
            length,
            overwrite,
            all_changes,
            limits: preflight.limits,
        };
        if selection.working.intent.base.content.kind == ContentKind::Legacy {
            // A first legacy conversion may require a whole-file verification.
            // Full replacement and truncation to zero need none of its bytes.
            if self.legacy_base_needed(&selection, context)? {
                preflight.target.read_range(
                    &preflight
                        .target
                        .pin(&selection.working.intent.base.content)?,
                    0,
                    &mut [0; 1],
                    context,
                )?;
            }
        } else {
            let mut source = MutationSource {
                session: self,
                selection: &selection,
            };
            loop {
                match preflight.target.preflight(
                    &selection.working.intent.base.content,
                    length,
                    &mut source,
                    context,
                ) {
                    Ok(()) => break,
                    Err(error) => {
                        let Some(mut range) = LocalContent::rebuild_range(&error) else {
                            return Err(error);
                        };
                        range.len = range.len.min(
                            selection
                                .working
                                .intent
                                .base
                                .content
                                .total_len
                                .saturating_sub(range.offset),
                        );
                        // The visitor and all its buffers/guards have returned.
                        self.recover_base(
                            &selection.working.intent,
                            range,
                            error,
                            preflight.limits,
                            context,
                        )?;
                        self.validate_mutation(&selection, context)?;
                    }
                }
            }
        }
        self.validate_mutation(&selection, context)?;
        Ok(selection)
    }
    fn legacy_base_needed(
        &self,
        selection: &MutationSelection,
        context: &ReadContext,
    ) -> Result<bool> {
        let mut from = 0;
        let incoming = selection.overwrite;
        let incoming_end = incoming.end()?;
        while from < selection.base_visible_len {
            selection.check(context)?;
            let index = from / CHUNK_SIZE;
            let until = selection.base_visible_len.min((index + 1) * CHUNK_SIZE);
            if incoming.len > 0 && incoming.offset <= from && incoming_end >= until {
                from = until;
                continue;
            }
            let patch = {
                let db = self.journal.lock(context)?;
                let current = self.owned(&db)?.0;
                ensure!(current == selection.working, "STALE_WRITE_CHECKPOINT");
                overlay(&db, &current, None, index, context)?
            };
            for position in from..until {
                if incoming.len > 0 && position >= incoming.offset && position < incoming_end {
                    continue;
                }
                let at = (position - index * CHUNK_SIZE) as usize;
                if patch.as_ref().is_none_or(|(meta, bytes)| {
                    at >= meta.valid || bytes[CHUNK_SIZE as usize + at / 8] & (1 << (at % 8)) == 0
                }) {
                    return Ok(true);
                }
            }
            from = until;
        }
        self.validate_mutation(selection, context)?;
        Ok(false)
    }
    pub(crate) fn with_base_wait(mut self, wait: Arc<dyn BaseWait>) -> Self {
        self.base_wait = Some(wait);
        self
    }
    fn recover_base(
        &self,
        intent: &SaveIntent,
        range: ByteRange,
        error: anyhow::Error,
        limits: Option<BaseWaitLimits>,
        context: &ReadContext,
    ) -> Result<()> {
        match &self.base_wait {
            Some(wait) => wait.wait(
                BaseRequest {
                    intent,
                    pin: &self.base,
                    range,
                    limits,
                },
                error,
                context,
            ),
            None => Err(error),
        }
    }
    pub(crate) fn uses_journal(&self, journal: &Arc<WriteJournal>) -> bool {
        Arc::ptr_eq(&self.journal, journal)
    }
    pub fn adoption_work(&self) -> Option<AdoptionWork> {
        self.adoption_work
    }

    /// Adopt a verified metadata continuation, retaining all predecessor
    /// recovery roots. Canonical availability/receipt-owned deletion is separate.
    /// Concurrent mutations during staging return stale; retry from current state.
    pub fn adopt_continuation(
        &self,
        handoff: &crate::core::content_saves::SaveContinuation,
        provider: Arc<dyn ContentProvider>,
        context: &ReadContext,
    ) -> Result<WriteSession> {
        context.check()?;
        let parent = handoff.checkpoint();
        let intent = handoff.intent();
        parent.validate()?;
        intent.validate()?;
        ensure!(
            self.journal.root == handoff.store_root()
                && handoff.base_pin().reference() == &intent.base.content,
            "CONTENT_SAVE_WRONG_STORE"
        );
        let provider_pin = provider.pin(&intent.base.content)?;
        context.check()?;
        ensure!(
            provider_pin.provider() == handoff.base_pin().provider(),
            "CONTENT_SAVE_WRONG_STORE"
        );
        ensure!(
            parent.needs_publication()
                && parent.intent.operation == self.operation
                && intent.operation == self.operation
                && intent.branch == parent.intent.branch
                && intent.base.entity == parent.intent.base.entity
                && intent.base.workspace == parent.intent.base.workspace
                && intent.base.content.total_len == parent.length
                && parent.intent.base.generation.checked_add(2) == Some(intent.base.generation),
            "CONTENT_SAVE_INTENT_MISMATCH"
        );
        let key = checkpoint_key(parent)?;
        let mut work = AdoptionWork::default();
        let (snapshot, saved, _staging) = {
            let db = self.journal.lock(context)?;
            let (w, saved) = state(&db, self.operation)?;
            if w.owner != self.owner {
                let marker = w
                    .adoption
                    .as_ref()
                    .filter(|m| m.previous == key && m.from_owner == self.owner)
                    .context("STALE_WRITE_SESSION")?;
                ensure!(w.intent == *intent, "STALE_WRITE_SESSION");
                ensure!(
                    recorded_checkpoint(&db, key)? == *parent,
                    "CORRUPT_WRITE_JOURNAL"
                );
                let (old, old_saved, _) = retained(&db, marker.archive)?;
                ensure!(
                    old.owner == self.owner
                        && old.intent == parent.intent
                        && old_saved.sequence >= parent.sequence,
                    "CORRUPT_WRITE_JOURNAL"
                );
                work.replay = true;
                context.check()?;
                return Ok(self.adopted_handle(w.owner, w.length, handoff, provider, work));
            }
            ensure!(w.intent == parent.intent, "STALE_WRITE_SESSION");
            let saved = saved.context("STALE_WRITE_CHECKPOINT")?;
            ensure!(
                saved.sequence >= parent.sequence
                    && w.sequence >= saved.sequence
                    && w.roots.floor == parent.roots.floor,
                "STALE_WRITE_CHECKPOINT"
            );
            ensure!(
                recorded_checkpoint(&db, key)? == *parent,
                "CORRUPT_WRITE_JOURNAL"
            );
            verify_maps(&db, &w, Some(&saved), context)?;
            // Register while holding the same mutex as a node sweep's check.
            // Pruning may commit nodes not yet reachable from a durable root.
            ensure!(
                self.journal.staging.load(Ordering::Acquire) < 4,
                "CONTENT_RESOURCE_LIMIT"
            );
            self.journal.staging.fetch_add(1, Ordering::AcqRel);
            (
                w,
                saved,
                StagingLease {
                    journal: self.journal.clone(),
                },
            )
        };
        crate::util::fault::trigger("content.write.before_adoption")?;
        let visible =
            |roots: &OverlayRoots, through: u64, length: u64, work: &mut MapWork| -> Result<u64> {
                let db = self.journal.lock(context)?;
                let cut = map(&db, self.operation, MapKind::Truncations).minimum(
                    roots.cuts,
                    parent.sequence,
                    through,
                    work,
                    context,
                )?;
                Ok(parent.length.min(cut.unwrap_or(parent.length)).min(length))
            };
        let working_visible = visible(
            &snapshot.roots,
            snapshot.sequence,
            snapshot.length,
            &mut work.map,
        )?;
        let saved_visible = visible(&saved.roots, saved.sequence, saved.length, &mut work.map)?;
        let mut memo = Vec::<(MapRoot, MapRoot)>::new();
        let mut prune = |root: MapRoot, kind: MapKind| -> Result<MapRoot> {
            if let Some((_, result)) = memo.iter().find(|(source, _)| *source == root) {
                return Ok(*result);
            }
            let result = self.journal.prune_for_adoption(
                self.operation,
                root,
                kind,
                parent.sequence,
                &mut work,
                context,
            )?;
            memo.push((root, result));
            Ok(result)
        };
        let working_roots = OverlayRoots {
            patches: prune(snapshot.roots.patches, MapKind::Patches)?,
            cuts: prune(snapshot.roots.cuts, MapKind::Truncations)?,
            floor: parent.sequence,
        };
        let saved_roots = OverlayRoots {
            patches: prune(saved.roots.patches, MapKind::Patches)?,
            cuts: prune(saved.roots.cuts, MapKind::Truncations)?,
            floor: parent.sequence,
        };
        crate::util::fault::trigger("content.write.adoption_staged")?;
        let mut next = snapshot.clone();
        next.intent = intent.clone();
        next.owner = Uuid::new_v4();
        next.base_visible_len = working_visible;
        next.roots = working_roots;
        next.adoption = Some(Adoption {
            previous: key,
            from_owner: self.owner,
            archive: Uuid::new_v4(),
        });
        let next_saved = SaveCheckpoint {
            intent: intent.clone(),
            sequence: saved.sequence,
            length: saved.length,
            base_visible_len: saved_visible,
            patch_count: saved_roots.patches.entries(),
            patch_map: saved_roots.patches.digest(),
            roots: saved_roots,
        };
        let mut db = self.journal.lock(context)?;
        db.execute_batch("PRAGMA synchronous=FULL")?;
        let result = (|| -> Result<()> {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let (current, current_saved) = state(&tx, self.operation)?;
            ensure!(current.owner == self.owner, "STALE_WRITE_SESSION");
            ensure!(
                current == snapshot && current_saved.as_ref() == Some(&saved),
                "STALE_WRITE_CHECKPOINT"
            );
            ensure!(
                recorded_checkpoint(&tx, key)? == *parent,
                "CORRUPT_WRITE_JOURNAL"
            );
            retain(
                &tx,
                next.adoption.as_ref().unwrap().archive,
                &snapshot,
                &saved,
            )?;
            crate::util::fault::trigger("content.write.adoption_archived")?;
            remember_checkpoint(&tx, &next_saved)?;
            persist(&tx, &next, Some(&next_saved), false)?;
            crate::util::fault::trigger("content.write.adoption_written")?;
            context.check()?;
            tx.commit()?;
            Ok(())
        })();
        db.progress_handler(0, None::<fn() -> bool>);
        let normal = db.execute_batch("PRAGMA synchronous=NORMAL");
        drop(db);
        context.check()?;
        result?;
        normal?;
        crate::util::fault::trigger("content.write.adopted")?;
        context.check()?;
        Ok(self.adopted_handle(next.owner, next.length, handoff, provider, work))
    }
    fn adopted_handle(
        &self,
        owner: Uuid,
        length: u64,
        handoff: &crate::core::content_saves::SaveContinuation,
        provider: Arc<dyn ContentProvider>,
        work: AdoptionWork,
    ) -> WriteSession {
        WriteSession {
            journal: self.journal.clone(),
            base_wait: self.base_wait.clone(),
            provider,
            base: handoff.base_pin().clone(),
            visible_len: AtomicU64::new(length),
            operation: self.operation,
            owner,
            converted_base: Mutex::new(None),
            adoption_work: Some(work),
        }
    }
    pub fn operation(&self) -> Uuid {
        self.operation
    }
    /// Last committed mutable length, for metadata without journal I/O.
    /// Byte operations still validate the durable owner and sequence.
    pub(crate) fn visible_len(&self) -> u64 {
        self.visible_len.load(Ordering::Acquire)
    }
    pub fn intent(&self, context: &ReadContext) -> Result<SaveIntent> {
        let db = self.journal.lock(context)?;
        let (working, saved) = self.owned(&db)?;
        verify_maps(&db, &working, saved.as_ref(), context)?;
        context.check()?;
        Ok(working.intent)
    }
    fn owned(&self, db: &Connection) -> Result<(Working, Option<SaveCheckpoint>)> {
        let state = state(db, self.operation)?;
        ensure!(state.0.owner == self.owner, "STALE_WRITE_SESSION");
        Ok(state)
    }
    fn selected(&self, db: &Connection, checkpoint: &SaveCheckpoint) -> Result<()> {
        ensure!(
            self.owned(db)?.1.as_ref() == Some(checkpoint),
            "STALE_WRITE_CHECKPOINT"
        );
        Ok(())
    }
    fn verified_selection(&self, checkpoint: &SaveCheckpoint, context: &ReadContext) -> Result<()> {
        let db = self.journal.lock(context)?;
        self.selected(&db, checkpoint)?;
        ensure!(checkpoint.needs_publication(), "STALE_WRITE_CHECKPOINT");
        verify_saved(&db, checkpoint, context)
    }
    fn needs_base(&self, checkpoint: &SaveCheckpoint, context: &ReadContext) -> Result<bool> {
        let mut offset = 0;
        while offset < checkpoint.base_visible_len {
            context.check()?;
            let db = self.journal.lock(context)?;
            self.selected(&db, checkpoint)?;
            let Some(c) = chunk_meta(
                &db,
                checkpoint.roots.patches,
                self.operation,
                offset / CHUNK_SIZE,
                checkpoint.roots.floor,
                context,
            )?
            else {
                return Ok(true);
            };
            let count = (checkpoint.base_visible_len - offset).min(CHUNK_SIZE) as usize;
            if c.valid < count {
                return Ok(true);
            }
            let bytes = payload(&db, &c)?;
            if (0..count).any(|i| bytes[CHUNK_SIZE as usize + i / 8] & (1 << (i % 8)) == 0) {
                return Ok(true);
            }
            offset += count as u64;
        }
        self.selected(&*self.journal.lock(context)?, checkpoint)?;
        context.check()?;
        Ok(false)
    }
    fn convert_base(
        &self,
        target: &LocalContent,
        checkpoint: &SaveCheckpoint,
        context: &ReadContext,
    ) -> Result<PinnedContent> {
        loop {
            context.check()?;
            self.selected(&*self.journal.lock(context)?, checkpoint)?;
            match self.converted_base.try_lock() {
                Ok(mut cached) => {
                    if let Some(pin) = cached
                        .as_ref()
                        .filter(|p| p.provider() == target.identity())
                    {
                        return Ok(pin.clone());
                    }
                    self.selected(&*self.journal.lock(context)?, checkpoint)?;
                    let pin =
                        target.convert_legacy(&checkpoint.intent.base.content, context, &|| {
                            self.selected(&*self.journal.lock(context)?, checkpoint)
                        })?;
                    *cached = Some(pin.clone());
                    context.check()?;
                    return Ok(pin);
                }
                Err(TryLockError::Poisoned(_)) => anyhow::bail!("WRITE_CONVERSION_POISONED"),
                Err(TryLockError::WouldBlock) => {
                    context.cancel.sleep(Duration::from_millis(5));
                }
            }
        }
    }
    /// Construct canonical objects without publishing a revision. Intact
    /// subtrees may remain unhydrated. The object engine supplies missing
    /// structural pages; their absence never requests a whole-base copy here.
    pub fn prepare(
        &self,
        target: &LocalContent,
        checkpoint: &SaveCheckpoint,
        context: &ReadContext,
    ) -> Result<PreparedSave> {
        self.verified_selection(checkpoint, context)?;
        let legacy = checkpoint.intent.base.content.kind == ContentKind::Legacy;
        // Conversion must precede the builder's buffer permit: nested save
        // permits could deadlock two simultaneous legacy preparations.
        let converted = if legacy && self.needs_base(checkpoint, context)? {
            Some(self.convert_base(target, checkpoint, context)?)
        } else {
            None
        };
        let base = if legacy {
            converted.as_ref().map(PinnedContent::reference)
        } else {
            Some(&checkpoint.intent.base.content)
        };
        let content = {
            let mut source = CheckpointEdit {
                session: self,
                checkpoint,
                converted: converted
                    .as_ref()
                    .map(|p| (target as &dyn ContentProvider, p)),
            };
            target.rebuild(base, checkpoint.length, &mut source, context)?
        };
        self.verified_selection(checkpoint, context)?;
        let pin = target.pin(&content)?;
        context.check()?;
        Ok(PreparedSave {
            checkpoint: checkpoint.clone(),
            owner: self.owner,
            content: pin,
            _converted_base: converted,
        })
    }
    /// Read-only preparation validation; metadata publication still requires
    /// the coordinator's atomic entity/generation and causal-parent fence.
    pub fn validate_prepared(&self, prepared: &PreparedSave, context: &ReadContext) -> Result<()> {
        ensure!(prepared.owner == self.owner, "STALE_WRITE_SESSION");
        self.verified_selection(&prepared.checkpoint, context)?;
        context.check()
    }
    pub fn len(&self, context: &ReadContext) -> Result<u64> {
        let length = self.owned(&*self.journal.lock(context)?)?.0.length;
        context.check()?;
        Ok(length)
    }
    pub fn is_empty(&self, context: &ReadContext) -> Result<bool> {
        Ok(self.len(context)? == 0)
    }
    pub fn write_at(&self, offset: u64, bytes: &[u8], context: &ReadContext) -> Result<()> {
        ensure!(bytes.len() <= limits::READ_BYTES, "CONTENT_BUFFER_LIMIT");
        let end = offset
            .checked_add(bytes.len() as u64)
            .context("RANGE_OVERFLOW")?;
        ensure!(end <= MAX_FILE_SIZE, "FILE_TOO_LARGE");
        loop {
            let mut db = self.journal.lock(context)?;
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let (mut w, saved) = self.owned(&tx)?;
            verify_maps(&tx, &w, saved.as_ref(), context)?;
            if bytes.is_empty() {
                return context.check();
            }
            let first = offset / CHUNK_SIZE;
            let last = (end - 1) / CHUNK_SIZE;
            let mut additional = 0;
            for index in first..=last {
                if chunk_meta(&tx, w.pending, self.operation, index, 0, context)?.is_none() {
                    additional += 1;
                }
            }
            if WriteJournal::needs_checkpoint(&tx, &w, additional)? {
                drop(tx);
                drop(db);
                self.journal
                    .checkpoint(None, (self.operation, self.owner), context)?;
                continue;
            }
            let mut collect = false;
            w.advance()?;
            for index in first..=last {
                context.check()?;
                let prior = chunk_meta(&tx, w.pending, self.operation, index, 0, context)?;
                let old = overlay(&tx, &w, None, index, context)?;
                let (valid, mut patch) = old
                    .map(|(c, b)| (c.valid, b))
                    .unwrap_or_else(|| (0, vec![0; PATCH_BYTES]));
                clip(&mut patch, valid);
                let from = offset.max(index * CHUNK_SIZE);
                let until = end.min((index + 1) * CHUNK_SIZE);
                let start = (from - index * CHUNK_SIZE) as usize;
                let stop = (until - index * CHUNK_SIZE) as usize;
                patch[start..stop]
                    .copy_from_slice(&bytes[(from - offset) as usize..(until - offset) as usize]);
                for i in start..stop {
                    patch[CHUNK_SIZE as usize + i / 8] |= 1 << (i % 8);
                }
                let hash = Digest::bytes(&patch);
                collect |= prior.is_some_and(|prior| prior.hash != hash);
                tx.execute("INSERT INTO payloads(hash,data) VALUES(?,?) ON CONFLICT(hash) DO UPDATE SET data=excluded.data",params![hash.to_string(),patch])?;
                let c = Chunk {
                    index,
                    hash,
                    valid: valid.max(stop),
                    sequence: w.sequence,
                };
                payload(&tx, &c)?;
                put_chunk(&tx, &mut w, &c, context)?;
            }
            w.length = w.length.max(end);
            persist(&tx, &w, saved.as_ref(), false)?;
            context.check()?;
            tx.commit()?;
            self.visible_len.store(w.length, Ordering::Release);
            drop(db);
            crate::util::fault::trigger("content.write.buffered")?;
            if collect {
                self.journal.maintain(context)?;
            }
            return context.check();
        }
    }
    pub fn set_len(&self, length: u64, context: &ReadContext) -> Result<()> {
        ensure!(length <= MAX_FILE_SIZE, "FILE_TOO_LARGE");
        loop {
            let mut db = self.journal.lock(context)?;
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let (mut w, saved) = self.owned(&tx)?;
            verify_maps(&tx, &w, saved.as_ref(), context)?;
            if length == w.length {
                return context.check();
            }
            // Clipping a previously Saved boundary creates a pending patch too.
            let additional = if length < w.length && !length.is_multiple_of(CHUNK_SIZE) {
                let index = length / CHUNK_SIZE;
                let clips = chunk_meta(
                    &tx,
                    w.roots.patches,
                    self.operation,
                    index,
                    w.roots.floor,
                    context,
                )?
                .is_some_and(|c| c.valid > (length % CHUNK_SIZE) as usize);
                usize::from(
                    clips
                        && chunk_meta(&tx, w.pending, self.operation, index, 0, context)?.is_none(),
                )
            } else {
                0
            };
            if WriteJournal::needs_checkpoint(&tx, &w, additional)? {
                drop(tx);
                drop(db);
                self.journal
                    .checkpoint(None, (self.operation, self.owner), context)?;
                continue;
            }
            w.advance()?;
            if length < w.length {
                let patches = map(&tx, self.operation, MapKind::Patches);
                let mut work = MapWork::default();
                w.roots.patches = patches.truncate(
                    w.roots.patches,
                    length.div_ceil(CHUNK_SIZE),
                    w.roots.floor,
                    &mut work,
                    context,
                )?;
                w.pending = patches.truncate(
                    w.pending,
                    length.div_ceil(CHUNK_SIZE),
                    0,
                    &mut work,
                    context,
                )?;
                if !length.is_multiple_of(CHUNK_SIZE)
                    && let Some(mut c) = chunk_meta(
                        &tx,
                        w.roots.patches,
                        self.operation,
                        length / CHUNK_SIZE,
                        w.roots.floor,
                        context,
                    )?
                {
                    let valid = c.valid.min((length % CHUNK_SIZE) as usize);
                    if c.valid != valid {
                        c.valid = valid;
                        put_chunk(&tx, &mut w, &c, context)?;
                    }
                }
                w.roots.cuts = map(&tx, self.operation, MapKind::Truncations).set(
                    w.roots.cuts,
                    w.sequence,
                    MapValue::Truncate {
                        sequence: w.sequence,
                        length,
                    },
                    w.roots.floor,
                    &mut work,
                    context,
                )?;
                w.base_visible_len = w.base_visible_len.min(length);
            }
            w.length = length;
            persist(&tx, &w, saved.as_ref(), false)?;
            context.check()?;
            tx.commit()?;
            self.visible_len.store(length, Ordering::Release);
            return context.check();
        }
    }
    pub fn checkpoint(&self, context: &ReadContext) -> Result<Option<SaveCheckpoint>> {
        {
            let db = self.journal.lock(context)?;
            let (w, saved) = self.owned(&db)?;
            verify_maps(&db, &w, saved.as_ref(), context)?;
        }
        self.journal
            .checkpoint(Some(self.operation), (self.operation, self.owner), context)?;
        let saved = self.owned(&*self.journal.lock(context)?)?.1;
        context.check()?;
        Ok(saved.filter(SaveCheckpoint::needs_publication))
    }
    /// Reads of the mutable view fail if a concurrent mutation changes its
    /// captured sequence. Callers must discard output on every error.
    pub fn read_range(&self, offset: u64, out: &mut [u8], context: &ReadContext) -> Result<()> {
        self.read(None, offset, out, context)
    }
    /// A later checkpoint supersedes this selection: stale preparations fail,
    /// rather than mixing its overlay with a newer checkpoint's chunk map.
    pub fn read_checkpoint(
        &self,
        checkpoint: &SaveCheckpoint,
        offset: u64,
        out: &mut [u8],
        context: &ReadContext,
    ) -> Result<()> {
        self.read(Some(checkpoint), offset, out, context)
    }
    fn read(
        &self,
        selected: Option<&SaveCheckpoint>,
        offset: u64,
        out: &mut [u8],
        context: &ReadContext,
    ) -> Result<()> {
        self.read_using(
            selected,
            offset,
            out,
            None,
            true,
            BaseRead::Waiting(None),
            context,
        )
    }
    pub(crate) fn read_range_waiting(
        &self,
        offset: u64,
        out: &mut [u8],
        limits: Option<BaseWaitLimits>,
        context: &ReadContext,
    ) -> Result<()> {
        self.read_using(
            None,
            offset,
            out,
            None,
            true,
            BaseRead::Waiting(limits),
            context,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn read_using(
        &self,
        selected: Option<&SaveCheckpoint>,
        offset: u64,
        out: &mut [u8],
        base_override: Option<(&dyn ContentProvider, &PinnedContent)>,
        verify_map: bool,
        recovery: BaseRead,
        context: &ReadContext,
    ) -> Result<()> {
        ensure!(out.len() <= limits::READ_BYTES, "CONTENT_BUFFER_LIMIT");
        let (w, saved) = self.owned(&*self.journal.lock(context)?)?;
        if let Some(c) = selected {
            ensure!(saved.as_ref() == Some(c), "STALE_WRITE_CHECKPOINT");
        }
        if verify_map {
            let db = self.journal.lock(context)?;
            let current = self.owned(&db)?;
            ensure!(current.0.sequence == w.sequence, "STALE_WRITE_CHECKPOINT");
            if let Some(c) = selected {
                self.selected(&db, c)?;
                verify_saved(&db, c, context)?;
            } else {
                verify_maps(&db, &current.0, current.1.as_ref(), context)?;
            }
        }
        let length = selected.map_or(w.length, |c| c.length);
        let base_len = selected.map_or(w.base_visible_len, |c| c.base_visible_len);
        let end = offset
            .checked_add(out.len() as u64)
            .context("RANGE_OVERFLOW")?;
        ensure!(end <= length, "RANGE_OUT_OF_BOUNDS");
        let fresh = |current: &(Working, Option<SaveCheckpoint>)| -> Result<()> {
            ensure!(
                if let Some(c) = selected {
                    current.1.as_ref() == Some(c)
                } else {
                    current.0.sequence == w.sequence
                },
                "STALE_WRITE_CHECKPOINT"
            );
            Ok(())
        };
        let mut from = offset;
        while from < end {
            context.check()?;
            let index = from / CHUNK_SIZE;
            let until = end.min((index + 1) * CHUNK_SIZE);
            let patch = {
                let db = self.journal.lock(context)?;
                let current = self.owned(&db)?;
                fresh(&current)?;
                overlay(&db, &current.0, selected, index, context)?
            };
            let target = &mut out[(from - offset) as usize..(until - offset) as usize];
            let (provider, pin) = base_override.unwrap_or((&*self.provider, &self.base));
            loop {
                match read_overlay_chunk(
                    provider,
                    pin,
                    base_len,
                    from,
                    target,
                    patch.as_ref(),
                    context,
                ) {
                    Ok(()) => break,
                    Err(error)
                        if base_override.is_none() && matches!(recovery, BaseRead::Waiting(_)) =>
                    {
                        let BaseRead::Waiting(limits) = recovery else {
                            unreachable!()
                        };
                        self.recover_base(
                            &w.intent,
                            ByteRange {
                                offset: from,
                                len: target.len() as u64,
                            },
                            error,
                            limits,
                            context,
                        )?;
                        fresh(&self.owned(&*self.journal.lock(context)?)?)?;
                    }
                    Err(error) => return Err(error),
                }
            }
            from = until;
        }
        {
            let db = self.journal.lock(context)?;
            let current = self.owned(&db)?;
            fresh(&current)?;
            if verify_map {
                if let Some(c) = selected {
                    verify_saved(&db, c, context)?;
                } else {
                    verify_maps(&db, &current.0, current.1.as_ref(), context)?;
                }
            }
        }
        context.check()
    }
}

#[cfg(test)]
mod root_journal_tests {
    use super::*;
    use crate::{model::workspace::WorkspaceId, util::Cancel};
    use std::time::Instant;

    fn context() -> ReadContext {
        ReadContext {
            deadline: Instant::now() + Duration::from_secs(120),
            cancel: Cancel::new(),
        }
    }
    #[test]
    fn legacy_mutation_preflight_uses_private_replacement_when_original_is_absent() {
        let dir = tempfile::tempdir().unwrap();
        let native = Arc::new(LocalContent::open(dir.path()).unwrap());
        let original = Digest::bytes(&[199; 32]);
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
                content: FileContentRef::new(ContentKind::Legacy, original, 32).unwrap(),
            },
        };
        let session = WriteJournal::open(dir.path())
            .unwrap()
            .create(intent, native.clone(), &context())
            .unwrap();
        session.write_at(0, &[15; 32], &context()).unwrap();
        let context = context();
        let cancelled = BaseWaitLimits {
            stall: Duration::from_secs(1),
            cancelled: || true,
        };
        let error = session
            .preflight_mutation(
                ByteRange { offset: 0, len: 0 },
                Some(16),
                true,
                &PreflightContext {
                    target: &native,
                    limits: Some(cancelled),
                    context: &context,
                },
            )
            .err()
            .unwrap();
        assert!(format!("{error:#}").contains("CONTENT_CANCELLED"));
        assert_eq!(session.len(&context).unwrap(), 32);
        let mut selection = session
            .preflight_mutation(
                ByteRange { offset: 0, len: 0 },
                Some(16),
                true,
                &PreflightContext {
                    target: &native,
                    limits: None,
                    context: &context,
                },
            )
            .unwrap();
        selection.limits = Some(cancelled);
        assert!(
            format!(
                "{:#}",
                session.validate_mutation(&selection, &context).unwrap_err()
            )
            .contains("CONTENT_CANCELLED")
        );
        selection.limits = None;
        session.validate_mutation(&selection, &context).unwrap();
        session.set_len(16, &context).unwrap();
        let checkpoint = session.checkpoint(&context).unwrap().unwrap();
        let prepared = session.prepare(&native, &checkpoint, &context).unwrap();
        let mut out = [0; 16];
        native
            .read_range(prepared.pin(), 0, &mut out, &context)
            .unwrap();
        assert_eq!(out, [15; 16]);
        assert!(
            !dir.path()
                .join("objects")
                .join(original.to_string())
                .exists()
        );
    }
    #[test]
    fn discarded_node_marks_cannot_authorize_a_sweep_and_cache_budget_is_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let native = Arc::new(LocalContent::open(dir.path()).unwrap());
        let journal = WriteJournal::open(dir.path()).unwrap();
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
                content: FileContentRef::new(ContentKind::Direct, Digest::bytes(b""), 0).unwrap(),
            },
        };
        let session = journal.create(intent, native, &context()).unwrap();
        session.write_at(0, b"safe", &context()).unwrap();
        session.checkpoint(&context()).unwrap();
        let orphan = Digest([99; 32]);
        {
            let db = journal.lock(&context()).unwrap();
            assert_eq!(
                db.query_row("PRAGMA main.cache_size", [], |r| r.get::<_, i64>(0))
                    .unwrap(),
                -1536
            );
            assert_eq!(
                db.query_row("PRAGMA temp.cache_size", [], |r| r.get::<_, i64>(0))
                    .unwrap(),
                -512
            );
            db.execute(
                "INSERT INTO private_overlay_nodes_v1(hash,body) VALUES(?,x'00')",
                [orphan.0.as_slice()],
            )
            .unwrap();
            db.execute_batch("CREATE TEMP TRIGGER discard_node_mark BEFORE INSERT ON gc_node_marks BEGIN SELECT RAISE(IGNORE); END").unwrap();
        }
        assert!(
            journal
                .reclaim_orphans(None, &context())
                .err()
                .unwrap()
                .to_string()
                .contains("CORRUPT_WRITE_JOURNAL")
        );
        {
            let db = journal.lock(&context()).unwrap();
            assert_eq!(
                db.query_row(
                    "SELECT count(*) FROM private_overlay_nodes_v1 WHERE hash=?",
                    [orphan.0.as_slice()],
                    |r| r.get::<_, usize>(0)
                )
                .unwrap(),
                1
            );
            db.execute_batch("DROP TRIGGER discard_node_mark").unwrap();
        }
        let mut cursor = None;
        let mut removed = 0;
        loop {
            let page = journal.reclaim_orphans(cursor, &context()).unwrap();
            assert!(page.validated + page.examined + page.nodes_examined <= 256);
            removed += page.nodes_removed;
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(removed, 1);
        let mut bytes = [0; 4];
        session.read_range(0, &mut bytes, &context()).unwrap();
        assert_eq!(&bytes, b"safe");
    }
    struct Zeros;
    impl EditSource for Zeros {
        fn check(&self, context: &ReadContext) -> Result<()> {
            context.check()
        }
        fn span(&mut self, _offset: u64, _len: u64, context: &ReadContext) -> Result<EditSpan> {
            context.check()?;
            Ok(EditSpan::Zero)
        }
        fn read(&mut self, _offset: u64, out: &mut [u8], context: &ReadContext) -> Result<()> {
            context.check()?;
            out.fill(0);
            Ok(())
        }
    }
    #[test]
    fn adoption_staging_is_bounded_after_five_and_fifty_thousand_published_patches() {
        use crate::core::{Store, id};
        for entries in [5_000u64, 50_000] {
            for iteration in 0..6 {
                let dir = tempfile::tempdir().unwrap();
                let mut store = Store::initialize(dir.path(), "adoption-probe", &id()).unwrap();
                store
                    .db
                    .execute("INSERT INTO config VALUES('fixture:content_v1','1')", [])
                    .unwrap();
                let entry = store.create("file", "file").unwrap();
                let local = Arc::new(LocalContent::open(dir.path()).unwrap());
                let journal = WriteJournal::open(dir.path()).unwrap();
                let session = journal
                    .create(
                        store.begin_content_save(&entry.id, &context()).unwrap(),
                        local.clone(),
                        &context(),
                    )
                    .unwrap();
                let length = entries * CHUNK_SIZE;
                let canonical = local.rebuild(None, length, &mut Zeros, &context()).unwrap();
                // Fixture seeds a complete Saved root and its independently built
                // zero DAG. Initial edit/preparation cost is deliberately excluded.
                let parent = {
                    let mut db = journal.lock(&context()).unwrap();
                    db.execute_batch("PRAGMA synchronous=FULL").unwrap();
                    let tx = db.transaction().unwrap();
                    let (mut w, _) = session.owned(&tx).unwrap();
                    let mut bytes = vec![0; PATCH_BYTES];
                    bytes[CHUNK_SIZE as usize..].fill(255);
                    let hash = Digest::bytes(&bytes);
                    tx.execute(
                        "INSERT INTO payloads(hash,data) VALUES(?,?)",
                        params![hash.to_string(), bytes],
                    )
                    .unwrap();
                    w.roots.patches = map(&tx, session.operation, MapKind::Patches)
                        .build_sorted(
                            (0..entries).map(|key| {
                                (
                                    key,
                                    MapValue::Patch {
                                        hash,
                                        valid: CHUNK_SIZE as u32,
                                        sequence: 1,
                                    },
                                )
                            }),
                            &mut MapWork::default(),
                            &context(),
                        )
                        .unwrap();
                    w.length = length;
                    w.sequence = 1;
                    let saved = SaveCheckpoint {
                        intent: w.intent.clone(),
                        sequence: 1,
                        length,
                        base_visible_len: 0,
                        patch_count: entries,
                        patch_map: w.roots.patches.digest(),
                        roots: w.roots.clone(),
                    };
                    remember_checkpoint(&tx, &saved).unwrap();
                    persist(&tx, &w, Some(&saved), false).unwrap();
                    tx.commit().unwrap();
                    db.execute_batch("PRAGMA synchronous=NORMAL").unwrap();
                    saved
                };
                let prepared = PreparedSave {
                    checkpoint: parent.clone(),
                    owner: session.owner,
                    content: local.pin(&canonical).unwrap(),
                    _converted_base: None,
                };
                store
                    .select_content_checkpoint(&parent, &context())
                    .unwrap();
                store.publish_content_save(&prepared, &context()).unwrap();
                let offset = entries / 2 * CHUNK_SIZE;
                session.write_at(offset + 3, b"later", &context()).unwrap();
                session.checkpoint(&context()).unwrap();
                session
                    .write_at(offset + 8192, b"more", &context())
                    .unwrap();
                let handoff = store.continue_content_save(&parent, &context()).unwrap();
                let before = journal.lock(&context()).unwrap().total_changes();
                let start = Instant::now();
                let next = session
                    .adopt_continuation(&handoff, local.clone(), &context())
                    .unwrap();
                let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                let changed = journal.lock(&context()).unwrap().total_changes() - before;
                let work = next.adoption_work().unwrap();
                assert!(
                    work.map.nodes_written <= 16 && work.map.nodes_read <= 18 && changed <= 32,
                    "{work:?} {changed}"
                );
                let saved = journal.pending(None, 256, &context()).unwrap().remove(0);
                assert_eq!(saved.patch_count, 1);
                let mut bytes = [0; 5];
                next.read_checkpoint(&saved, offset + 3, &mut bytes, &context())
                    .unwrap();
                assert_eq!(&bytes, b"later");
                let mut bytes = [0; 4];
                next.read_range(offset + 8192, &mut bytes, &context())
                    .unwrap();
                assert_eq!(&bytes, b"more");
                let mut bytes = [1; 4];
                next.read_range(0, &mut bytes, &context()).unwrap();
                assert_eq!(bytes, [0; 4]);
                let rebuilt = next.prepare(&local, &saved, &context()).unwrap();
                assert_ne!(rebuilt.content(), &canonical);
                println!(
                    "WAVE6_ADOPTION {}",
                    serde_json::json!({"entries":entries,"warmup":iteration==0,"adoption_ms":elapsed,
                "changed_sql_rows":changed,"work":work,"saved_patches":saved.patch_count,"staging_budget":64,
                "full_wal":true,"seed_and_canonical_preparation_excluded":true,"retained_predecessor":true})
                );
            }
        }
    }
    #[test]
    fn failed_adoption_rollback_never_exposes_a_tentative_replay_marker() {
        use crate::{
            core::{Store, id},
            util::fault::{self, FaultAction, FaultConfig},
        };
        use rusqlite::hooks::{AuthAction, AuthContext, Authorization, TransactionOperation};
        for panic in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut store = Store::initialize(dir.path(), "adoption-rollback", &id()).unwrap();
            store
                .db
                .execute("INSERT INTO config VALUES('fixture:content_v1','1')", [])
                .unwrap();
            let entry = store.create("file", "file").unwrap();
            let local = Arc::new(LocalContent::open(dir.path()).unwrap());
            let journal = WriteJournal::open(dir.path()).unwrap();
            let session = journal
                .create(
                    store.begin_content_save(&entry.id, &context()).unwrap(),
                    local.clone(),
                    &context(),
                )
                .unwrap();
            session.write_at(0, b"retained", &context()).unwrap();
            let checkpoint = session.checkpoint(&context()).unwrap().unwrap();
            let prepared = session.prepare(&local, &checkpoint, &context()).unwrap();
            store
                .select_content_checkpoint(&checkpoint, &context())
                .unwrap();
            store.publish_content_save(&prepared, &context()).unwrap();
            let handoff = store
                .continue_content_save(&checkpoint, &context())
                .unwrap();
            journal
                .lock(&context())
                .unwrap()
                .authorizer(Some(|auth: AuthContext<'_>| {
                    if matches!(
                        auth.action,
                        AuthAction::Transaction {
                            operation: TransactionOperation::Rollback
                        }
                    ) {
                        Authorization::Deny
                    } else {
                        Authorization::Allow
                    }
                }));
            fault::configure(Some(FaultConfig::new(
                "content.write.adoption_written",
                if panic {
                    FaultAction::Panic
                } else {
                    FaultAction::Fail
                },
            )));
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                session.adopt_continuation(&handoff, local.clone(), &context())
            }));
            fault::configure(None);
            if panic {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().is_err());
            }
            assert!(journal.connection_failed());
            assert!(
                session
                    .adopt_continuation(&handoff, local.clone(), &context())
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("WRITE_JOURNAL_CONNECTION_FAILED")
            );
            let fresh = WriteJournal::open(dir.path()).unwrap();
            assert_eq!(
                fresh.pending(None, 256, &context()).unwrap(),
                vec![checkpoint]
            );
            let recovered = fresh
                .resume(session.operation(), local.clone(), &context())
                .unwrap();
            let next = recovered
                .adopt_continuation(&handoff, local, &context())
                .unwrap();
            let mut bytes = [0; 8];
            next.read_range(0, &mut bytes, &context()).unwrap();
            assert_eq!(&bytes, b"retained");
        }
    }
    #[test]
    fn journal_point_and_full_checkpoint_work_stays_bounded_after_large_saved_maps() {
        for entries in [5_000u64, 50_000] {
            let dir = tempfile::tempdir().unwrap();
            let provider = Arc::new(LocalContent::open(dir.path()).unwrap());
            let journal = WriteJournal::open(dir.path()).unwrap();
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
                    entity: "entity".into(),
                    generation: 1,
                    content: FileContentRef::new(ContentKind::Direct, Digest::bytes(b""), 0)
                        .unwrap(),
                },
            };
            let session = journal.create(intent, provider, &context()).unwrap();
            // Seed one committed complete Saved root, without timing 50,000
            // ordinary mutations or pretending that their history is free.
            {
                let mut db = journal.lock(&context()).unwrap();
                db.execute_batch("PRAGMA synchronous=FULL").unwrap();
                let tx = db.transaction().unwrap();
                let (mut w, _) = session.owned(&tx).unwrap();
                let mut bytes = vec![0; PATCH_BYTES];
                bytes[0] = 7;
                bytes[CHUNK_SIZE as usize] = 1;
                let hash = Digest::bytes(&bytes);
                tx.execute(
                    "INSERT INTO payloads(hash,data) VALUES(?,?)",
                    params![hash.to_string(), bytes],
                )
                .unwrap();
                let mut work = MapWork::default();
                w.roots.patches = map(&tx, session.operation, MapKind::Patches)
                    .build_sorted(
                        (0..entries).map(|index| {
                            (
                                index,
                                MapValue::Patch {
                                    hash,
                                    valid: 1,
                                    sequence: 1,
                                },
                            )
                        }),
                        &mut work,
                        &context(),
                    )
                    .unwrap();
                w.length = entries * CHUNK_SIZE;
                w.sequence = 1;
                let saved = SaveCheckpoint {
                    intent: w.intent.clone(),
                    sequence: 1,
                    length: w.length,
                    base_visible_len: 0,
                    patch_count: entries,
                    patch_map: w.roots.patches.digest(),
                    roots: w.roots.clone(),
                };
                persist(&tx, &w, Some(&saved), false).unwrap();
                tx.commit().unwrap();
                db.execute_batch("PRAGMA synchronous=NORMAL").unwrap();
            }
            for iteration in 0..6 {
                let before = journal.lock(&context()).unwrap().total_changes();
                let offset = entries / 2 * CHUNK_SIZE;
                let mut byte = [0];
                let started = Instant::now();
                session.read_range(offset, &mut byte, &context()).unwrap();
                let read_ms = started.elapsed().as_secs_f64() * 1000.0;
                assert_eq!(byte, [if iteration == 0 { 7 } else { 9 }]);
                let started = Instant::now();
                session.write_at(offset, &[9], &context()).unwrap();
                let saved = session.checkpoint(&context()).unwrap().unwrap();
                let save_ms = started.elapsed().as_secs_f64() * 1000.0;
                assert_eq!(saved.patch_count, entries);
                let changes = journal.lock(&context()).unwrap().total_changes() - before;
                assert!(
                    changes <= 600,
                    "historical map rebuilt: {entries} entries, {changes} changed rows"
                );
                println!(
                    "WAVE6_ROOT_JOURNAL {}",
                    serde_json::json!({"scenario":"point_checkpoint","entries":entries,
                    "warmup":iteration==0,"read_ms":read_ms,"write_full_checkpoint_ms":save_ms,"changed_sql_rows":changes,
                    "payload_bytes":PATCH_BYTES,"maintenance_node_budget":256,"full_wal":true,"seed_excluded":true})
                );
            }
        }
    }
}
