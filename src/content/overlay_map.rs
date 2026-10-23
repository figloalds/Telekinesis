//! Immutable authenticated sparse maps for private overlay generations.
//!
//! This is not the canonical content format or the active v2 journal. A caller
//! seals/persists returned roots with its own intent transaction and owns their
//! lifetime. Nodes are immutable; this module performs no collection. Mutations
//! require an existing SQLite transaction, and every byte buffer is <= 1 KiB.
//! Returned roots become durable only with the caller's intent transaction.
use crate::{
    content::ReadContext,
    model::content::{CHUNK_SIZE, Digest, MAX_FILE_SIZE},
};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::iter::Peekable;
use uuid::Uuid;

const FANOUT: usize = 16;
pub const NODE_BYTES: usize = 1024;
pub const WALK_BUDGET: usize = 256;
const ZERO: Digest = Digest([0; 32]);
const MAGIC: &[u8] = b"TKOM\x01";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MapKind {
    Patches,
    Truncations,
}
impl MapKind {
    fn level(self) -> u8 {
        match self {
            Self::Patches => 8,
            Self::Truncations => 16,
        }
    }
    fn tag(self) -> u8 {
        match self {
            Self::Patches => 0,
            Self::Truncations => 1,
        }
    }
    fn count_limit(self) -> u64 {
        match self {
            Self::Patches => MAX_FILE_SIZE / CHUNK_SIZE,
            Self::Truncations => u64::MAX,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MapValue {
    Patch {
        hash: Digest,
        valid: u32,
        sequence: u64,
    },
    Truncate {
        sequence: u64,
        length: u64,
    },
}
impl MapValue {
    pub fn sequence(self) -> u64 {
        match self {
            Self::Patch { sequence, .. } | Self::Truncate { sequence, .. } => sequence,
        }
    }
    fn validate(self, kind: MapKind, key: u64) -> Result<()> {
        ensure!(self.sequence() > 0, "INVALID_WRITE_INTENT");
        match (kind, self) {
            (MapKind::Patches, Self::Patch { valid, .. }) => ensure!(
                key < MAX_FILE_SIZE / CHUNK_SIZE && valid > 0 && u64::from(valid) <= CHUNK_SIZE,
                "INVALID_WRITE_INTENT"
            ),
            (MapKind::Truncations, Self::Truncate { sequence, length }) => ensure!(
                key == sequence && length <= MAX_FILE_SIZE,
                "INVALID_WRITE_INTENT"
            ),
            _ => anyhow::bail!("INVALID_WRITE_INTENT"),
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Edge {
    hash: Digest,
    count: u64,
    max_sequence: u64,
    minimum: u64,
}
impl Edge {
    const EMPTY: Self = Self {
        hash: ZERO,
        count: 0,
        max_sequence: 0,
        minimum: u64::MAX,
    };
    fn validate(self, kind: MapKind) -> Result<()> {
        if self.count == 0 {
            ensure!(self == Self::EMPTY, "CORRUPT_WRITE_JOURNAL");
        } else {
            ensure!(
                self.hash != ZERO && self.count <= kind.count_limit() && self.max_sequence > 0,
                "CORRUPT_WRITE_JOURNAL"
            );
            ensure!(
                match kind {
                    MapKind::Patches => self.minimum == u64::MAX,
                    MapKind::Truncations => self.minimum <= MAX_FILE_SIZE,
                },
                "CORRUPT_WRITE_JOURNAL"
            );
        }
        Ok(())
    }
    fn validate_at(self, kind: MapKind, level: u8) -> Result<()> {
        self.validate(kind)?;
        ensure!(
            level <= kind.level() && u128::from(self.count) <= (1u128 << (u32::from(level) * 4)),
            "CORRUPT_WRITE_JOURNAL"
        );
        Ok(())
    }
}

/// Store this root in a bounded checksummed private intent. It is not a wire
/// capability or an independently authenticated substitute for that intent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MapRoot {
    format: u8,
    scope: Uuid,
    kind: MapKind,
    edge: Edge,
}
impl MapRoot {
    pub(crate) fn validate(self, scope: Uuid, kind: MapKind) -> Result<()> {
        ensure!(self.format == 1, "UNSUPPORTED_WRITE_JOURNAL");
        ensure!(
            self.scope == scope && self.kind == kind,
            "CORRUPT_WRITE_JOURNAL"
        );
        self.edge.validate_at(kind, kind.level())
    }
    pub fn empty(scope: Uuid, kind: MapKind) -> Self {
        Self {
            format: 1,
            scope,
            kind,
            edge: Edge::EMPTY,
        }
    }
    pub fn digest(self) -> Digest {
        self.edge.hash
    }
    /// Referenced leaves, including entries hidden by a caller's sequence floor.
    pub fn entries(self) -> u64 {
        self.edge.count
    }
    pub fn max_sequence(self) -> u64 {
        self.edge.max_sequence
    }
}

/// Successful SQLite node work, independent of logical file size. SQLite/WAL/
/// device amplification and the caller's payload hashing are separate counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct MapWork {
    pub nodes_read: u64,
    pub nodes_written: u64,
    /// Bounded readbacks that verified installed nodes (separate from traversal).
    pub node_checks: u64,
    pub encoded_bytes: u64,
}

// The branch is bounded to 896 bytes; inline storage avoids a heap allocation
// for every verified path step.
#[allow(clippy::large_enum_variant)]
enum Node {
    Leaf(MapValue),
    Branch([Edge; FANOUT]),
}
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self.at.checked_add(len).context("CORRUPT_WRITE_JOURNAL")?;
        let result = self
            .bytes
            .get(self.at..end)
            .context("CORRUPT_WRITE_JOURNAL")?;
        self.at = end;
        Ok(result)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn digest(&mut self) -> Result<Digest> {
        Ok(Digest(self.take(32)?.try_into().unwrap()))
    }
}
fn stats(node: &Node) -> Result<(u64, u64, u64)> {
    match node {
        Node::Leaf(value) => Ok((
            1,
            value.sequence(),
            match value {
                MapValue::Patch { .. } => u64::MAX,
                MapValue::Truncate { length, .. } => *length,
            },
        )),
        Node::Branch(children) => {
            let mut count = 0u64;
            let mut maximum = 0;
            let mut minimum = u64::MAX;
            for child in children {
                count = count
                    .checked_add(child.count)
                    .context("CORRUPT_WRITE_JOURNAL")?;
                maximum = maximum.max(child.max_sequence);
                minimum = minimum.min(child.minimum);
            }
            Ok((count, maximum, minimum))
        }
    }
}
fn node_hash(bytes: &[u8]) -> Digest {
    let mut hash = Sha256::new();
    hash.update(b"tkfs:private-overlay-node:v1\0");
    hash.update(bytes);
    Digest(hash.finalize().into())
}
fn range(prefix: u64, level: u8) -> (u128, u128) {
    let start = u128::from(prefix) << (u32::from(level) * 4);
    (start, start + (1u128 << (u32::from(level) * 4)))
}
fn child_prefix(prefix: u64, digit: usize) -> Result<u64> {
    prefix
        .checked_mul(16)
        .and_then(|n| n.checked_add(digit as u64))
        .context("CORRUPT_WRITE_JOURNAL")
}

pub struct OverlayMap<'a> {
    db: &'a Connection,
    scope: Uuid,
    kind: MapKind,
}
impl<'a> OverlayMap<'a> {
    pub fn ensure_schema(db: &Connection) -> Result<()> {
        db.execute_batch("CREATE TABLE IF NOT EXISTS private_overlay_nodes_v1(hash BLOB PRIMARY KEY CHECK(length(hash)=32),body BLOB NOT NULL CHECK(length(body)<=1024)) WITHOUT ROWID")?;
        Ok(())
    }
    pub fn new(db: &'a Connection, scope: Uuid, kind: MapKind) -> Self {
        Self { db, scope, kind }
    }
    fn root(&self, root: MapRoot) -> Result<Edge> {
        root.validate(self.scope, self.kind)?;
        Ok(root.edge)
    }
    /// Verify the root body before trusting its summaries; descendants remain
    /// demand-verified or can be audited in bounded walk pages.
    pub fn verify_root(
        &self,
        root: MapRoot,
        work: &mut MapWork,
        context: &ReadContext,
    ) -> Result<()> {
        context.check()?;
        let edge = self.root(root)?;
        if edge.count > 0 {
            self.load(edge, self.kind.level(), 0, work, context)?;
        }
        Ok(())
    }
    /// Does [start,end) contain an active patch? Fully covered authenticated
    /// children can answer without enumerating their leaves.
    pub fn has_range(
        &self,
        root: MapRoot,
        start: u64,
        end: u64,
        floor: u64,
        work: &mut MapWork,
        context: &ReadContext,
    ) -> Result<bool> {
        ensure!(
            self.kind == MapKind::Patches && start <= end && end <= MAX_FILE_SIZE / CHUNK_SIZE,
            "RANGE_OUT_OF_BOUNDS"
        );
        let edge = self.root(root)?;
        context.check()?;
        if edge.count == 0 || start == end {
            return Ok(false);
        }
        let node = self.load(edge, self.kind.level(), 0, work, context)?;
        self.has_range_at(
            edge,
            Some(node),
            self.kind.level(),
            0,
            start,
            end,
            floor,
            work,
            context,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn has_range_at(
        &self,
        edge: Edge,
        cached: Option<Node>,
        level: u8,
        prefix: u64,
        start: u64,
        end: u64,
        floor: u64,
        work: &mut MapWork,
        context: &ReadContext,
    ) -> Result<bool> {
        context.check()?;
        let (low, high) = range(prefix, level);
        if edge.count == 0
            || edge.max_sequence <= floor
            || low >= u128::from(end)
            || high <= u128::from(start)
        {
            return Ok(false);
        }
        if u128::from(start) <= low && high <= u128::from(end) {
            return Ok(true);
        }
        let node = match cached {
            Some(node) => node,
            None => self.load(edge, level, prefix, work, context)?,
        };
        match node {
            Node::Branch(children) => {
                for (digit, child) in children.into_iter().enumerate() {
                    if self.has_range_at(
                        child,
                        None,
                        level - 1,
                        child_prefix(prefix, digit)?,
                        start,
                        end,
                        floor,
                        work,
                        context,
                    )? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Node::Leaf(_) => Ok(true),
        }
    }
    fn make_root(&self, edge: Edge) -> MapRoot {
        MapRoot {
            format: 1,
            scope: self.scope,
            kind: self.kind,
            edge,
        }
    }
    fn writable(&self) -> Result<()> {
        ensure!(
            !self.db.is_autocommit(),
            "WRITE_JOURNAL_TRANSACTION_REQUIRED"
        );
        Ok(())
    }
    fn encode(&self, node: &Node, level: u8, prefix: u64) -> Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(NODE_BYTES);
        bytes.extend(MAGIC);
        bytes.extend(self.scope.as_bytes());
        bytes.push(self.kind.tag());
        bytes.push(level);
        bytes.extend(prefix.to_le_bytes());
        match node {
            Node::Leaf(value) => {
                ensure!(level == 0, "CORRUPT_WRITE_JOURNAL");
                value.validate(self.kind, prefix)?;
                match value {
                    MapValue::Patch {
                        hash,
                        valid,
                        sequence,
                    } => {
                        bytes.extend(hash.0);
                        bytes.extend(valid.to_le_bytes());
                        bytes.extend(sequence.to_le_bytes());
                    }
                    MapValue::Truncate { sequence, length } => {
                        bytes.extend(sequence.to_le_bytes());
                        bytes.extend(length.to_le_bytes());
                    }
                }
            }
            Node::Branch(children) => {
                ensure!(level > 0, "CORRUPT_WRITE_JOURNAL");
                for child in children {
                    child.validate_at(self.kind, level - 1)?;
                    bytes.extend(child.hash.0);
                    bytes.extend(child.count.to_le_bytes());
                    bytes.extend(child.max_sequence.to_le_bytes());
                    bytes.extend(child.minimum.to_le_bytes());
                }
            }
        }
        ensure!(bytes.len() <= NODE_BYTES, "CORRUPT_WRITE_JOURNAL");
        Ok(bytes)
    }
    fn install(
        &self,
        node: &Node,
        level: u8,
        prefix: u64,
        work: &mut MapWork,
        context: &ReadContext,
    ) -> Result<Edge> {
        context.check()?;
        let (count, max_sequence, minimum) = stats(node)?;
        if count == 0 {
            return Ok(Edge::EMPTY);
        }
        let bytes = self.encode(node, level, prefix)?;
        let edge = Edge {
            hash: node_hash(&bytes),
            count,
            max_sequence,
            minimum,
        };
        edge.validate_at(self.kind, level)?;
        self.db.prepare_cached("INSERT INTO private_overlay_nodes_v1(hash,body) VALUES(?,?) ON CONFLICT(hash) DO UPDATE SET body=excluded.body")?.execute(params![edge.hash.0.as_slice(),bytes])?;
        // A trigger can ignore the INSERT or remove its result. Success means
        // the exact node remains present, even when a matching row preexisted.
        let installed = self.db.prepare_cached(
            "SELECT length(body),CASE WHEN typeof(body)='blob' AND length(body)<=1024 THEN body END FROM private_overlay_nodes_v1 WHERE hash=?")?.query_row(
                [edge.hash.0.as_slice()],
                |row| Ok(row.get::<_, i64>(0)? == bytes.len() as i64
                    && row.get_ref(1)?.as_blob()? == bytes.as_slice()),
            );
        match installed {
            Ok(true) => (),
            Ok(false) => anyhow::bail!("CORRUPT_WRITE_JOURNAL"),
            Err(
                error @ (rusqlite::Error::QueryReturnedNoRows
                | rusqlite::Error::InvalidColumnType(..)
                | rusqlite::Error::FromSqlConversionFailure(..)),
            ) => {
                return Err(error).context("CORRUPT_WRITE_JOURNAL");
            }
            Err(error) => return Err(error.into()),
        }
        work.nodes_written += 1;
        work.node_checks += 1;
        work.encoded_bytes += bytes.len() as u64;
        context.check()?;
        Ok(edge)
    }
    fn load(
        &self,
        edge: Edge,
        level: u8,
        prefix: u64,
        work: &mut MapWork,
        context: &ReadContext,
    ) -> Result<Node> {
        context.check()?;
        edge.validate_at(self.kind, level)?;
        ensure!(edge.count > 0, "CORRUPT_WRITE_JOURNAL");
        let raw = self.db.prepare_cached(
            "SELECT length(body),CASE WHEN typeof(body)='blob' AND length(body)<=1024 THEN body END FROM private_overlay_nodes_v1 WHERE hash=?")?.query_row(
            [edge.hash.0.as_slice()],
            |r| {
                let length:i64=r.get(0)?;
                if !(0..=NODE_BYTES as i64).contains(&length) {
                    return Err(rusqlite::Error::InvalidQuery);
                }
                let body = r.get_ref(1)?.as_blob()?;
                Ok(body.to_vec())
            },
        );
        let bytes = match raw {
            Err(
                error @ (rusqlite::Error::QueryReturnedNoRows
                | rusqlite::Error::InvalidQuery
                | rusqlite::Error::InvalidColumnType(..)
                | rusqlite::Error::FromSqlConversionFailure(..)),
            ) => return Err(error).context("CORRUPT_WRITE_JOURNAL"),
            other => other?,
        };
        ensure!(node_hash(&bytes) == edge.hash, "CORRUPT_WRITE_JOURNAL");
        let mut r = Reader {
            bytes: &bytes,
            at: 0,
        };
        ensure!(
            r.take(MAGIC.len())? == MAGIC
                && r.take(16)? == self.scope.as_bytes()
                && r.byte()? == self.kind.tag()
                && r.byte()? == level
                && r.u64()? == prefix,
            "CORRUPT_WRITE_JOURNAL"
        );
        let node = if level == 0 {
            let value = match self.kind {
                MapKind::Patches => MapValue::Patch {
                    hash: r.digest()?,
                    valid: r.u32()?,
                    sequence: r.u64()?,
                },
                MapKind::Truncations => MapValue::Truncate {
                    sequence: r.u64()?,
                    length: r.u64()?,
                },
            };
            value
                .validate(self.kind, prefix)
                .context("CORRUPT_WRITE_JOURNAL")?;
            Node::Leaf(value)
        } else {
            let mut children = [Edge::EMPTY; FANOUT];
            for child in &mut children {
                *child = Edge {
                    hash: r.digest()?,
                    count: r.u64()?,
                    max_sequence: r.u64()?,
                    minimum: r.u64()?,
                };
                child.validate_at(self.kind, level - 1)?;
            }
            Node::Branch(children)
        };
        ensure!(
            r.at == bytes.len() && stats(&node)? == (edge.count, edge.max_sequence, edge.minimum),
            "CORRUPT_WRITE_JOURNAL"
        );
        work.nodes_read += 1;
        context.check()?;
        Ok(node)
    }
    pub fn get(
        &self,
        root: MapRoot,
        key: u64,
        floor: u64,
        work: &mut MapWork,
        context: &ReadContext,
    ) -> Result<Option<MapValue>> {
        context.check()?;
        if self.kind == MapKind::Patches {
            ensure!(key < MAX_FILE_SIZE / CHUNK_SIZE, "RANGE_OUT_OF_BOUNDS");
        }
        let mut edge = self.root(root)?;
        let mut prefix = 0;
        let mut level = self.kind.level();
        while edge.count > 0 {
            let node = self.load(edge, level, prefix, work, context)?;
            if edge.max_sequence <= floor {
                return Ok(None);
            }
            match node {
                Node::Leaf(value) => return Ok(Some(value)),
                Node::Branch(children) => {
                    let digit = ((key >> (u32::from(level - 1) * 4)) & 15) as usize;
                    edge = children[digit];
                    prefix = child_prefix(prefix, digit)?;
                    level -= 1;
                    // Authenticated child summaries permit skipping old epochs.
                    if edge.max_sequence <= floor {
                        return Ok(None);
                    }
                }
            }
        }
        Ok(None)
    }
    pub fn set(
        &self,
        root: MapRoot,
        key: u64,
        value: MapValue,
        floor: u64,
        work: &mut MapWork,
        context: &ReadContext,
    ) -> Result<MapRoot> {
        context.check()?;
        self.writable()?;
        value.validate(self.kind, key)?;
        ensure!(value.sequence() > floor, "STALE_WRITE_CHECKPOINT");
        let edge = self.root(root)?;
        let edge = self.set_at(edge, self.kind.level(), 0, key, value, floor, work, context)?;
        Ok(self.make_root(edge))
    }
    #[allow(clippy::too_many_arguments)]
    fn set_at(
        &self,
        edge: Edge,
        level: u8,
        prefix: u64,
        key: u64,
        value: MapValue,
        floor: u64,
        work: &mut MapWork,
        context: &ReadContext,
    ) -> Result<Edge> {
        context.check()?;
        let existing = if edge.count == 0 {
            None
        } else {
            Some(self.load(edge, level, prefix, work, context)?)
        };
        let existing = if edge.max_sequence <= floor {
            None
        } else {
            existing
        };
        if level == 0 {
            if let Some(Node::Leaf(old)) = existing {
                ensure!(value.sequence() >= old.sequence(), "STALE_WRITE_CHECKPOINT");
                if old == value {
                    return Ok(edge);
                }
            }
            return self.install(&Node::Leaf(value), level, prefix, work, context);
        }
        let mut children = match existing {
            Some(Node::Branch(children)) => children,
            None => [Edge::EMPTY; FANOUT],
            _ => anyhow::bail!("CORRUPT_WRITE_JOURNAL"),
        };
        let digit = ((key >> (u32::from(level - 1) * 4)) & 15) as usize;
        let previous = children[digit];
        children[digit] = self.set_at(
            children[digit],
            level - 1,
            child_prefix(prefix, digit)?,
            key,
            value,
            floor,
            work,
            context,
        )?;
        if children[digit] == previous {
            return Ok(edge);
        }
        self.install(&Node::Branch(children), level, prefix, work, context)
    }
    /// Stream a strictly increasing complete map using O(depth * fanout) stack
    /// space. It is not a resumable migration; the caller owns its transaction.
    pub fn build_sorted<I>(
        &self,
        entries: I,
        work: &mut MapWork,
        context: &ReadContext,
    ) -> Result<MapRoot>
    where
        I: IntoIterator<Item = (u64, MapValue)>,
    {
        self.writable()?;
        context.check()?;
        let mut entries = entries.into_iter().peekable();
        let edge = self.build_at(self.kind.level(), 0, &mut entries, work, context)?;
        ensure!(entries.next().is_none(), "INVALID_WRITE_INTENT");
        Ok(self.make_root(edge))
    }
    fn build_at<I>(
        &self,
        level: u8,
        prefix: u64,
        entries: &mut Peekable<I>,
        work: &mut MapWork,
        context: &ReadContext,
    ) -> Result<Edge>
    where
        I: Iterator<Item = (u64, MapValue)>,
    {
        context.check()?;
        let (start, end) = range(prefix, level);
        let Some((key, _)) = entries.peek() else {
            return Ok(Edge::EMPTY);
        };
        ensure!(u128::from(*key) >= start, "INVALID_WRITE_INTENT");
        if u128::from(*key) >= end {
            return Ok(Edge::EMPTY);
        }
        if level == 0 {
            let (key, value) = entries.next().unwrap();
            value.validate(self.kind, key)?;
            return self.install(&Node::Leaf(value), level, prefix, work, context);
        }
        let mut children = [Edge::EMPTY; FANOUT];
        for (digit, child) in children.iter_mut().enumerate() {
            *child = self.build_at(
                level - 1,
                child_prefix(prefix, digit)?,
                entries,
                work,
                context,
            )?;
        }
        self.install(&Node::Branch(children), level, prefix, work, context)
    }
    pub fn truncate(
        &self,
        root: MapRoot,
        end: u64,
        floor: u64,
        work: &mut MapWork,
        context: &ReadContext,
    ) -> Result<MapRoot> {
        self.writable()?;
        context.check()?;
        ensure!(
            self.kind == MapKind::Patches && end <= MAX_FILE_SIZE / CHUNK_SIZE,
            "INVALID_WRITE_INTENT"
        );
        let edge = self.truncate_at(
            self.root(root)?,
            self.kind.level(),
            0,
            end,
            floor,
            work,
            context,
        )?;
        Ok(self.make_root(edge))
    }
    #[allow(clippy::too_many_arguments)]
    fn truncate_at(
        &self,
        edge: Edge,
        level: u8,
        prefix: u64,
        end: u64,
        floor: u64,
        work: &mut MapWork,
        context: &ReadContext,
    ) -> Result<Edge> {
        context.check()?;
        if edge.count == 0 {
            return Ok(edge);
        }
        let (start, stop) = range(prefix, level);
        if start >= u128::from(end) {
            return Ok(Edge::EMPTY);
        }
        if stop <= u128::from(end) {
            return Ok(if edge.max_sequence <= floor {
                Edge::EMPTY
            } else {
                edge
            });
        }
        let Node::Branch(mut children) = self.load(edge, level, prefix, work, context)? else {
            anyhow::bail!("CORRUPT_WRITE_JOURNAL")
        };
        let original = children;
        for (digit, child) in children.iter_mut().enumerate() {
            *child = self.truncate_at(
                *child,
                level - 1,
                child_prefix(prefix, digit)?,
                end,
                floor,
                work,
                context,
            )?;
        }
        if original == children {
            return Ok(edge);
        }
        self.install(&Node::Branch(children), level, prefix, work, context)
    }
    /// Minimum truncation length for (after, through]. This preserves shrink/
    /// grow tombstones relative to an independently published older checkpoint.
    pub fn minimum(
        &self,
        root: MapRoot,
        after: u64,
        through: u64,
        work: &mut MapWork,
        context: &ReadContext,
    ) -> Result<Option<u64>> {
        context.check()?;
        ensure!(self.kind == MapKind::Truncations, "INVALID_WRITE_INTENT");
        let edge = self.root(root)?;
        if edge.count == 0 || after >= through {
            return Ok(None);
        }
        // Verify the externally persisted root summaries before skipping nodes.
        let node = self.load(edge, self.kind.level(), 0, work, context)?;
        self.minimum_at(
            edge,
            Some(node),
            self.kind.level(),
            0,
            u128::from(after) + 1,
            u128::from(through) + 1,
            work,
            context,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn minimum_at(
        &self,
        edge: Edge,
        cached: Option<Node>,
        level: u8,
        prefix: u64,
        start: u128,
        end: u128,
        work: &mut MapWork,
        context: &ReadContext,
    ) -> Result<Option<u64>> {
        context.check()?;
        let (low, high) = range(prefix, level);
        if edge.count == 0 || low >= end || high <= start {
            return Ok(None);
        }
        if start <= low && high <= end {
            return Ok(Some(edge.minimum));
        }
        let node = match cached {
            Some(node) => node,
            None => self.load(edge, level, prefix, work, context)?,
        };
        match node {
            Node::Leaf(MapValue::Truncate { length, .. }) => Ok(Some(length)),
            Node::Branch(children) => {
                let mut result: Option<u64> = None;
                for (digit, child) in children.into_iter().enumerate() {
                    if let Some(value) = self.minimum_at(
                        child,
                        None,
                        level - 1,
                        child_prefix(prefix, digit)?,
                        start,
                        end,
                        work,
                        context,
                    )? {
                        result = Some(result.map_or(value, |old| old.min(value)));
                    }
                }
                Ok(result)
            }
            _ => anyhow::bail!("CORRUPT_WRITE_JOURNAL"),
        }
    }
    pub fn walk(&self, root: MapRoot, floor: u64) -> Result<MapWalk> {
        let edge = self.root(root)?;
        Ok(MapWalk {
            scope: self.scope,
            kind: self.kind,
            floor,
            failed: false,
            stack: if edge.count == 0 {
                vec![]
            } else {
                vec![Task {
                    edge,
                    level: self.kind.level(),
                    prefix: 0,
                    root: true,
                }]
            },
        })
    }
    /// Prepare removal of published map generations without rewriting retained
    /// historical roots. The embedding journal must pin the source and recheck
    /// its live owner/selection before atomically adopting the resulting root.
    pub fn prune(&self, root: MapRoot, floor: u64) -> Result<MapPrune> {
        let edge = self.root(root)?;
        Ok(MapPrune {
            scope: self.scope,
            kind: self.kind,
            floor,
            failed: false,
            frames: if edge.count == 0 {
                vec![]
            } else {
                vec![PruneFrame {
                    edge,
                    level: self.kind.level(),
                    prefix: 0,
                    children: None,
                    original: None,
                    next: 0,
                }]
            },
            result: if edge.count == 0 { Some(edge) } else { None },
        })
    }
    /// At most `budget` node reads/installs; each install includes one bounded
    /// readback (counted separately in `MapWork::node_checks`). Unchanged subtrees
    /// reuse their hash. Commit staged nodes between pages if needed; no new
    /// root is returned until the complete source has been resolved.
    /// If a page's transaction rolls back or its commit fails, discard this
    /// volatile cursor and restart from the pinned source root.
    pub fn prune_page(
        &self,
        prune: &mut MapPrune,
        budget: usize,
        work: &mut MapWork,
        context: &ReadContext,
    ) -> Result<PrunePage> {
        context.check()?;
        self.writable()?;
        ensure!((1..=WALK_BUDGET).contains(&budget), "CONTENT_BUFFER_LIMIT");
        ensure!(
            prune.scope == self.scope && prune.kind == self.kind && !prune.failed,
            "CORRUPT_WRITE_JOURNAL"
        );
        prune.failed = true;
        let mut processed = 0;
        while processed < budget && !prune.frames.is_empty() {
            context.check()?;
            let frame = prune.frames.last_mut().unwrap();
            if frame.children.is_none() {
                processed += 1;
                let node = self.load(frame.edge, frame.level, frame.prefix, work, context)?;
                if frame.edge.max_sequence <= prune.floor {
                    prune.finish(Edge::EMPTY);
                    continue;
                }
                match node {
                    Node::Leaf(_) => {
                        let edge = frame.edge;
                        prune.finish(edge);
                    }
                    Node::Branch(children) => {
                        frame.original = Some(children);
                        frame.children = Some(children);
                    }
                }
                continue;
            }
            if frame.next < FANOUT {
                let digit = frame.next;
                frame.next += 1;
                let child = &mut frame.children.as_mut().unwrap()[digit];
                if child.count == 0 || child.max_sequence <= prune.floor {
                    *child = Edge::EMPTY;
                    continue;
                }
                let child = PruneFrame {
                    edge: *child,
                    level: frame.level - 1,
                    prefix: child_prefix(frame.prefix, digit)?,
                    children: None,
                    original: None,
                    next: 0,
                };
                prune.frames.push(child);
                ensure!(
                    prune.frames.len() <= usize::from(self.kind.level()) + 1,
                    "CORRUPT_WRITE_JOURNAL"
                );
                continue;
            }
            let children = frame.children.unwrap();
            let edge = if frame.original.as_ref() == Some(&children) {
                frame.edge
            } else {
                processed += 1;
                self.install(
                    &Node::Branch(children),
                    frame.level,
                    frame.prefix,
                    work,
                    context,
                )?
            };
            prune.finish(edge);
        }
        context.check()?;
        prune.failed = false;
        Ok(PrunePage {
            processed,
            root: prune.result.map(|edge| self.make_root(edge)),
        })
    }
    pub fn walk_page(
        &self,
        walk: &mut MapWalk,
        budget: usize,
        work: &mut MapWork,
        context: &ReadContext,
    ) -> Result<MapPage> {
        context.check()?;
        ensure!((1..=WALK_BUDGET).contains(&budget), "CONTENT_BUFFER_LIMIT");
        ensure!(
            walk.scope == self.scope && walk.kind == self.kind,
            "CORRUPT_WRITE_JOURNAL"
        );
        ensure!(!walk.failed, "CORRUPT_WRITE_JOURNAL");
        walk.failed = true;
        let mut processed = 0;
        let mut values = Vec::new();
        let mut nodes = Vec::new();
        while processed < budget {
            let Some(task) = walk.stack.pop() else {
                break;
            };
            processed += 1;
            if !task.root && task.edge.max_sequence <= walk.floor {
                continue;
            }
            let node = self.load(task.edge, task.level, task.prefix, work, context)?;
            nodes.push(task.edge.hash);
            if task.edge.max_sequence <= walk.floor {
                continue;
            }
            match node {
                Node::Leaf(value) => values.push((task.prefix, value)),
                Node::Branch(children) => {
                    for (digit, edge) in children.into_iter().enumerate().rev() {
                        if edge.count > 0 && edge.max_sequence > walk.floor {
                            walk.stack.push(Task {
                                edge,
                                level: task.level - 1,
                                prefix: child_prefix(task.prefix, digit)?,
                                root: false,
                            });
                        }
                    }
                }
            }
            ensure!(
                walk.stack.len() <= 1 + 15 * usize::from(self.kind.level()),
                "CORRUPT_WRITE_JOURNAL"
            );
        }
        context.check()?;
        walk.failed = false;
        Ok(MapPage {
            processed,
            values,
            nodes,
            complete: walk.stack.is_empty(),
        })
    }
}

struct Task {
    edge: Edge,
    level: u8,
    prefix: u64,
    root: bool,
}
/// Opaque, volatile cursor for one immutable root. The embedding collector must
/// additionally fence its root set/generation and retain readers across pages.
pub struct MapWalk {
    scope: Uuid,
    kind: MapKind,
    floor: u64,
    failed: bool,
    stack: Vec<Task>,
}
pub struct MapPage {
    pub processed: usize,
    pub values: Vec<(u64, MapValue)>,
    /// Hashes of nodes whose bodies were verified on this page. A collector
    /// walks with floor zero and fences its root set before using these marks.
    pub nodes: Vec<Digest>,
    pub complete: bool,
}

struct PruneFrame {
    edge: Edge,
    level: u8,
    prefix: u64,
    children: Option<[Edge; FANOUT]>,
    original: Option<[Edge; FANOUT]>,
    next: usize,
}
/// Volatile immutable-root work; failed/cancelled partial work must be restarted.
pub struct MapPrune {
    scope: Uuid,
    kind: MapKind,
    floor: u64,
    failed: bool,
    frames: Vec<PruneFrame>,
    result: Option<Edge>,
}
impl MapPrune {
    fn finish(&mut self, edge: Edge) {
        self.frames.pop();
        if let Some(parent) = self.frames.last_mut() {
            parent.children.as_mut().unwrap()[parent.next - 1] = edge;
        } else {
            self.result = Some(edge);
        }
    }
}
pub struct PrunePage {
    pub processed: usize,
    pub root: Option<MapRoot>,
}
