//! Object-owned graph and verified availability. All provider work runs outside
//! SQL transactions; durable effects revalidate the admitting generation.
use super::*;
use crate::{
    content::{ContentEdge, ContentProvider, LocalContent, ReadContext},
    model::content::{
        CHUNK_SIZE, ContentKind, FileContentRef, ManifestNode, ObjectKey, ObjectKind,
    },
};

pub const EDGE_PAGE: usize = 256;

#[derive(Clone, Debug)]
pub struct AdmittedEdge {
    pub reference: String,
    pub generation: u64,
    pub edge: ContentEdge,
}

mod graph;

pub(super) fn ensure_schema(db: &Connection) -> Result<()> {
    db.execute_batch("SAVEPOINT content_objects_schema;
        CREATE TABLE IF NOT EXISTS content_public_receipts(kind INTEGER NOT NULL,digest TEXT NOT NULL,
            PRIMARY KEY(kind,digest));
        CREATE TABLE IF NOT EXISTS content_source_backoff(kind INTEGER NOT NULL,digest TEXT NOT NULL,
            source TEXT NOT NULL,attempts INTEGER NOT NULL,retry_at_ms INTEGER NOT NULL,
            PRIMARY KEY(kind,digest,source));
        INSERT OR REPLACE INTO config VALUES('content:graph_cursor','');
        DELETE FROM config WHERE key='content:graph_reconciled';")?;
    graph::schema(db)?;
    db.execute_batch("RELEASE content_objects_schema;")?;
    Ok(())
}

pub(super) fn seed_root(
    db: &Connection,
    reference: &str,
    generation: u64,
    root: &FileContentRef,
) -> Result<()> {
    graph::seed(db, reference, generation, root)
}

fn kind(value: ObjectKind) -> i64 {
    if value == ObjectKind::Bytes { 0 } else { 1 }
}
fn level(value: Option<u8>) -> i64 {
    value.map_or(-1, i64::from)
}

fn validate_blob(edge: &ContentEdge, bytes: &[u8]) -> Result<()> {
    match edge.key.kind {
        ObjectKind::Bytes => ensure!(
            bytes.len() as u64 == edge.len
                && bytes.len() <= CHUNK_SIZE as usize
                && crate::model::content::Digest::bytes(bytes) == edge.key.digest,
            "CORRUPT_CONTENT"
        ),
        ObjectKind::Manifest => {
            let node = ManifestNode::decode(bytes)?;
            ensure!(
                Some(node.level) == edge.level
                    && node.total_len == edge.len
                    && node.digest()? == edge.key.digest,
                "MANIFEST_ROOT_MISMATCH"
            );
        }
    }
    Ok(())
}

impl Store {
    pub fn reconcile_content_roots(&mut self, limit: usize) -> Result<usize> {
        ensure!((1..=EDGE_PAGE).contains(&limit), "ADMISSION_PAGE_LIMIT");
        let done: bool = self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM config
            WHERE key='content:graph_reconciled' AND value='1')",
            [],
            |row| row.get(0),
        )?;
        if done {
            return Ok(0);
        }
        self.with_write("content_reconcile_roots", |store| {
            let after: String = store.db.query_row("SELECT value FROM config WHERE key='content:graph_cursor'",
                [],|row|row.get(0)).optional()?.unwrap_or_default();
            let roots: Vec<(String,u64,Vec<u8>,bool)> = store.db.prepare_cached(
                "SELECT reference,generation,root,granted FROM content_root_metadata WHERE reference>?1
                 ORDER BY reference LIMIT ?2")?.query_map(params![after,limit as i64],|row|
                    Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)))?.collect::<rusqlite::Result<_>>()?;
            for (reference,generation,root,granted) in &roots {
                if *granted { seed_root(&store.db,reference,*generation,&FileContentRef::decode(root)?)?; }
            }
            if let Some(last) = roots.last() {
                store.db.execute("INSERT OR REPLACE INTO config VALUES('content:graph_cursor',?1)",[&last.0])?;
            }
            if roots.len() < limit {
                store.db.execute("INSERT OR REPLACE INTO config VALUES('content:graph_reconciled','1')",[])?;
            }
            Ok(roots.len())
        })
    }

    pub fn content_source_ready(&self, key: ObjectKey, source: &str) -> Result<bool> {
        let until: Option<u64> = self
            .db
            .query_row(
                "SELECT retry_at_ms FROM content_source_backoff
            WHERE kind=?1 AND digest=?2 AND source=?3",
                params![kind(key.kind), key.digest.to_string(), source],
                |row| row.get(0),
            )
            .optional()?;
        Ok(until.is_none_or(|until| until <= time()))
    }

    pub fn backoff_content_source(&mut self, key: ObjectKey, source: &str) -> Result<()> {
        self.with_write("content_source_backoff", |store| {
            store.db.execute("DELETE FROM content_source_backoff WHERE rowid IN
                (SELECT rowid FROM content_source_backoff WHERE retry_at_ms<=?1 LIMIT 256)", [time()])?;
            let attempts: u32 = store.db.query_row("SELECT attempts FROM content_source_backoff
                WHERE kind=?1 AND digest=?2 AND source=?3", params![kind(key.kind),key.digest.to_string(),source],
                |row| row.get(0)).optional()?.unwrap_or(0);
            let count: usize = store.db.query_row("SELECT count(*) FROM content_source_backoff",[],|row|row.get(0))?;
            if count >= 16_384 {
                store.db.execute("DELETE FROM content_source_backoff WHERE rowid IN
                    (SELECT rowid FROM content_source_backoff ORDER BY retry_at_ms LIMIT 1)",[])?;
            }
            let delay = 30_000_u64.saturating_mul(1_u64 << attempts.min(7)).min(3_600_000);
            store.db.execute("INSERT INTO content_source_backoff VALUES(?1,?2,?3,?4,?5)
                ON CONFLICT(kind,digest,source) DO UPDATE SET attempts=excluded.attempts,retry_at_ms=excluded.retry_at_ms",
                params![kind(key.kind),key.digest.to_string(),source,attempts.saturating_add(1),time().saturating_add(delay)])?;
            Ok(())
        })
    }

    pub fn validate_content_demand(&self, demand: &crate::model::workspace::Demand) -> Result<()> {
        demand.validate()?;
        ensure!(
            demand.revision.workspace.repository == self.repo,
            "REPOSITORY_MISMATCH"
        );
        let (root, entity, granted): (Vec<u8>, String, bool) = self.db.query_row(
            "SELECT root,entity,granted FROM content_root_metadata WHERE reference=?",
            [&demand.reference],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        ensure!(
            granted
                && entity == demand.revision.entity
                && FileContentRef::decode(&root)? == demand.revision.content,
            "INVALID_DEMAND_SCOPE"
        );
        Ok(())
    }

    pub fn content_root(&self, reference: &str) -> Result<(u64, FileContentRef, bool)> {
        let (generation, root, granted): (u64, Vec<u8>, bool) = self
            .db
            .query_row(
                "SELECT generation,root,granted FROM content_root_metadata WHERE reference=?",
                [reference],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?
            .context("UNAUTHORIZED_OBJECT")?;
        Ok((generation, FileContentRef::decode(&root)?, granted))
    }

    pub fn seed_content_reference(&mut self, reference: &str) -> Result<()> {
        self.with_write("content_seed_reference", |store| {
            let (generation, bytes): (u64, Vec<u8>) = store.db.query_row(
                "SELECT generation,root FROM content_root_metadata WHERE reference=? AND granted=1",
                [reference],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            seed_root(
                &store.db,
                reference,
                generation,
                &FileContentRef::decode(&bytes)?,
            )
        })
    }

    pub fn read_content_frame(
        &self,
        provider: &LocalContent,
        reference: &str,
        key: ObjectKey,
        context: &ReadContext,
    ) -> Result<(u64, Vec<u8>)> {
        let (generation, root, granted) = self.content_root(reference)?;
        ensure!(granted, "UNAUTHORIZED_OBJECT");
        let edge = if key == ObjectKey::root(&root) {
            ContentEdge::root(&provider.pin(&root)?)?
        } else {
            self.content_edge(reference, generation, key)?.edge
        };
        self.read_content_frame_at(provider, reference, &edge, context)
    }

    /// The peer route authenticates repository/branch access before calling.
    /// A position lets a sender prove membership even if its derived graph
    /// cursor lags behind metadata. No descendant traversal is required first.
    pub fn read_content_frame_at(
        &self,
        provider: &LocalContent,
        reference: &str,
        edge: &ContentEdge,
        context: &ReadContext,
    ) -> Result<(u64, Vec<u8>)> {
        self.read_content_frame_prioritized(
            provider,
            reference,
            edge,
            crate::model::workspace::DemandPriority::Foreground,
            context,
        )
    }

    pub fn read_content_frame_prioritized(
        &self,
        provider: &LocalContent,
        reference: &str,
        edge: &ContentEdge,
        priority: crate::model::workspace::DemandPriority,
        context: &ReadContext,
    ) -> Result<(u64, Vec<u8>)> {
        let (generation,publishable,root): (u64,bool,Vec<u8>) = self.db.query_row(
            "SELECT generation,publishable,root FROM content_root_metadata WHERE reference=? AND granted=1 AND shared=1",
            [reference], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?))).optional()?.context("UNAUTHORIZED_OBJECT")?;
        let root = FileContentRef::decode(&root)?;
        ensure!(root.kind != ContentKind::Legacy, "UNSUPPORTED_CONTENT_KIND");
        let pin = provider.pin(&root)?;
        let key = edge.key;
        let received: bool = self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM content_public_receipts
            WHERE kind=?1 AND digest=?2)",
            params![kind(key.kind), key.digest.to_string()],
            |row| row.get(0),
        )?;
        ensure!(publishable || received, "UNAUTHORIZED_OBJECT");
        provider.verify_member_for(&pin, edge, priority, context)?;
        let bytes = provider.read_blob_for(
            key,
            (key.kind == ObjectKind::Bytes).then_some(edge.len),
            priority,
            context,
        )?;
        validate_blob(edge, &bytes)?;
        ensure!(
            self.content_reference_authorized(reference, generation)?,
            "STALE_ADMISSION_GENERATION"
        );
        context.check()?;
        Ok((generation, bytes))
    }

    pub(crate) fn record_content_receipt(
        &mut self,
        reference: &str,
        generation: u64,
        key: ObjectKey,
    ) -> Result<()> {
        self.with_write("content_receipt", |store| {
            store.content_edge(reference, generation, key)?;
            ensure!(
                store.content_reference_authorized(reference, generation)?,
                "UNAUTHORIZED_OBJECT"
            );
            store.db.execute(
                "INSERT OR IGNORE INTO content_public_receipts VALUES(?1,?2)",
                params![kind(key.kind), key.digest.to_string()],
            )?;
            store.db.execute(
                "UPDATE content_object_jobs SET verified=1,error=NULL,retry_at_ms=0
                WHERE kind=?1 AND digest=?2",
                params![kind(key.kind), key.digest.to_string()],
            )?;
            Ok(())
        })
    }

    pub fn validate_content_frame(
        &self,
        reference: &str,
        generation: u64,
        key: ObjectKey,
        bytes: &[u8],
    ) -> Result<AdmittedEdge> {
        ensure!(
            self.content_reference_authorized(reference, generation)?,
            "UNAUTHORIZED_OBJECT"
        );
        let edge = self.content_edge(reference, generation, key)?;
        validate_blob(&edge.edge, bytes)?;
        Ok(edge)
    }
}
