//! One immutable adjacency list and availability row per typed object. Root
//! witnesses are disposable locators, never authority. Every use proves a path
//! from current metadata; revocation does not require descendant rewrites.
use super::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct Witness {
    reference: String,
    generation: u64,
    edge: ContentEdge,
    ancestors: Vec<ContentEdge>,
}

fn alias(db: &Connection, item: &AdmittedEdge) -> Result<()> {
    db.execute(
        "INSERT OR IGNORE INTO content_page_aliases(kind,digest,reference,generation,offset)
        VALUES(?1,?2,?3,?4,?5)",
        params![
            kind(item.edge.key.kind),
            item.edge.key.digest.to_string(),
            item.reference,
            item.generation,
            item.edge.offset
        ],
    )?;
    Ok(())
}

fn admit(db: &Connection, item: &AdmittedEdge, ancestors: &[ContentEdge]) -> Result<()> {
    ensure!(
        ancestors.len() <= crate::model::content::MAX_DEPTH as usize,
        "MANIFEST_DEPTH"
    );
    let witness = serde_json::to_string(&Witness {
        reference: item.reference.clone(),
        generation: item.generation,
        edge: item.edge.clone(),
        ancestors: ancestors.to_vec(),
    })?;
    db.execute(
        "INSERT INTO content_object_jobs(kind,digest,expanded,witness) VALUES(?1,?2,?3,?4)
        ON CONFLICT(kind,digest) DO UPDATE SET witness=excluded.witness",
        params![
            kind(item.edge.key.kind),
            item.edge.key.digest.to_string(),
            item.edge.level.is_none(),
            witness
        ],
    )?;
    if item.edge.level.is_some() || ancestors.is_empty() {
        alias(db, item)?;
    }
    Ok(())
}

pub(super) fn seed(
    db: &Connection,
    reference: &str,
    generation: u64,
    root: &FileContentRef,
) -> Result<()> {
    // An outbox replay must not replace a newer root witness with an old grant.
    let valid: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM content_root_metadata
        WHERE reference=?1 AND generation=?2 AND granted=1)",
        params![reference, generation],
        |r| r.get(0),
    )?;
    if !valid {
        return Ok(());
    }
    let edge = ContentEdge {
        key: ObjectKey::root(root),
        offset: 0,
        len: root.total_len,
        level: if root.kind == ContentKind::Manifest {
            Some(ManifestNode::root_level(root.total_len)?)
        } else {
            None
        },
    };
    admit(
        db,
        &AdmittedEdge {
            reference: reference.into(),
            generation,
            edge,
        },
        &[],
    )
}

impl Store {
    pub fn content_object_verified(&self, key: ObjectKey) -> Result<bool> {
        Ok(self
            .db
            .query_row(
                "SELECT verified FROM content_object_jobs WHERE kind=?1 AND digest=?2",
                params![kind(key.kind), key.digest.to_string()],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(false))
    }

    /// Trusted local readers/scrubbers invalidate only the object they actually
    /// observed missing/corrupt. Permission and receipt evidence are unchanged.
    pub fn observe_content_unavailable(&mut self, key: ObjectKey) -> Result<()> {
        self.with_write("content_observe_missing",|store| {
            store.db.execute("UPDATE content_object_jobs SET verified=0,retry_at_ms=0 WHERE kind=?1 AND digest=?2 AND verified<>0",
                params![kind(key.kind),key.digest.to_string()])?;Ok(())
        })
    }

    pub fn content_range_indexed(
        &self,
        reference: &str,
        range: crate::model::workspace::ByteRange,
    ) -> Result<bool> {
        let mut covered = range.offset;
        let path = self.content_range_candidates(reference, range)?;
        for item in &path {
            if !self.content_object_verified(item.edge.key)? {
                return Ok(false);
            }
        }
        for item in path.iter().filter(|item| item.edge.level.is_none()) {
            if item.edge.offset > covered {
                return Ok(false);
            }
            covered = (item.edge.offset + item.edge.len).min(range.end()?);
        }
        Ok(covered == range.end()?)
    }
    /// Per-source/lane keyset scans inspect a fixed number of jobs, including
    /// backed-off or differently sharded ones. A prefix of unavailable objects
    /// can therefore never hide all later work behind a filtered LIMIT.
    pub fn content_transfer_candidates(
        &mut self,
        source: &str,
        lane: crate::model::protocol::LaneSpec,
        mode: crate::model::content_wire::PrepareMode,
    ) -> Result<Vec<AdmittedEdge>> {
        use crate::model::content_wire::PrepareMode;
        ensure!(
            !source.is_empty() && source.len() <= 128 && lane.is_valid(),
            "INVALID_DEMAND_SCOPE"
        );
        let kinds: &[i64] = match mode {
            PrepareMode::Structural => &[1],
            PrepareMode::Background => &[0],
            PrepareMode::Mixed => &[1, 0],
            PrepareMode::Foreground => &[],
        };
        self.with_write("content_transfer_scan",|store| {
            let mut items=vec![];
            for tag in kinds {
                let cursor:Option<String>=store.db.query_row("SELECT after_digest FROM content_transfer_cursor
                    WHERE source=?1 AND lane_index=?2 AND lane_count=?3 AND kind=?4",
                    params![source,lane.index,lane.count,tag],|r|r.get(0)).optional()?;
                if cursor.is_none() {
                    let count:u64=store.db.query_row("SELECT count(*) FROM content_transfer_cursor",[],|r|r.get(0))?;
                    if count>=4096 {store.db.execute("DELETE FROM content_transfer_cursor WHERE rowid IN (SELECT rowid FROM content_transfer_cursor LIMIT 1)",[])?;}
                }
                let sql=if *tag==1 {"SELECT digest,retry_at_ms,witness FROM content_object_jobs
                    WHERE kind=?1 AND expanded=0 AND digest>?2 ORDER BY digest LIMIT 16"} else {
                    "SELECT digest,retry_at_ms,witness FROM content_object_jobs WHERE verified=0 AND kind=?1 AND digest>?2 ORDER BY digest LIMIT 16"};
                let rows:Vec<(String,u64,String)>=store.db.prepare_cached(sql)?
                    .query_map(params![tag,cursor.as_deref().unwrap_or_default()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?.collect::<rusqlite::Result<_>>()?;
                let next=if rows.len()==16 {rows.last().map_or("",|r|r.0.as_str())} else {""};
                if cursor.as_deref()!=Some(next) {store.db.execute("INSERT INTO content_transfer_cursor VALUES(?1,?2,?3,?4,?5)
                    ON CONFLICT(source,lane_index,lane_count,kind) DO UPDATE SET after_digest=excluded.after_digest",
                    params![source,lane.index,lane.count,tag,next])?;}
                for (digest,retry,json) in &rows {
                    if *retry>time() || !lane.owns(digest) {continue;}
                    ensure!(json.len()<=16_384,"INVALID_CONTENT_EDGE");
                    let witness:Witness=serde_json::from_str(json)?;
                    ensure!(witness.ancestors.len()<=crate::model::content::MAX_DEPTH as usize,"MANIFEST_DEPTH");
                    if !store.content_source_ready(witness.edge.key,source)? {continue;}
                    if let Some(item)=store.resolve_content_witness(&witness,None,true)? {items.push(item);}
                    else {
                        store.db.execute("UPDATE content_object_jobs SET retry_at_ms=?1 WHERE kind=?2 AND digest=?3 AND witness=?4",
                            params![time().saturating_add(1000),tag,digest,json])?;
                    }
                }
            }
            Ok(items)
        })
    }
    /// Follow canonical coordinates with at most one indexed child lookup per
    /// level. Missing adjacency stops the path; no subtree scan is attempted.
    fn content_trace(
        &self,
        reference: &str,
        generation: u64,
        offset: u64,
        target: Option<u8>,
    ) -> Result<Option<Vec<ContentEdge>>> {
        let row: Option<(u64, Vec<u8>, bool)> = self
            .db
            .query_row(
                "SELECT generation,root,granted FROM content_root_metadata WHERE reference=?1",
                [reference],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let Some((current, bytes, granted)) = row else {
            return Ok(None);
        };
        let root = FileContentRef::decode(&bytes)?;
        if !granted || current != generation {
            return Ok(None);
        }
        let mut edge = ContentEdge {
            key: ObjectKey::root(&root),
            offset: 0,
            len: root.total_len,
            level: if root.kind == ContentKind::Manifest {
                Some(ManifestNode::root_level(root.total_len)?)
            } else {
                None
            },
        };
        let mut path = vec![];
        for _ in 0..=crate::model::content::MAX_DEPTH {
            path.push(edge.clone());
            if edge.offset == offset && edge.level == target {
                return Ok(Some(path));
            }
            let Some(node_level) = edge.level else {
                return Ok(None);
            };
            if offset < edge.offset || offset >= edge.offset + edge.len {
                return Ok(None);
            }
            let capacity = ManifestNode::child_capacity(node_level)?;
            let ordinal = (offset - edge.offset) / capacity;
            let row:Option<(i64,String,u64,i64)>=self.db.query_row(
                "SELECT kind,digest,len,level FROM content_manifest_edges WHERE parent=?1 AND ordinal=?2",
                params![edge.key.digest.to_string(),ordinal],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
            let Some((tag, digest, len, child_level)) = row else {
                return Ok(None);
            };
            ensure!(
                tag == i64::from(node_level > 0)
                    && child_level == level(node_level.checked_sub(1))
                    && len == (edge.len - ordinal * capacity).min(capacity),
                "INVALID_CONTENT_EDGE"
            );
            edge = ContentEdge {
                key: ObjectKey {
                    kind: if tag == 0 {
                        ObjectKind::Bytes
                    } else {
                        ObjectKind::Manifest
                    },
                    digest: crate::model::content::Digest::parse(&digest)?,
                },
                offset: edge.offset + ordinal * capacity,
                len,
                level: node_level.checked_sub(1),
            };
        }
        bail!("MANIFEST_DEPTH")
    }

    fn validate_admitted_edge(&self, item: &AdmittedEdge) -> Result<()> {
        let path = self.content_trace(
            &item.reference,
            item.generation,
            item.edge.offset,
            item.edge.level,
        )?;
        ensure!(
            path.as_ref().and_then(|p| p.last()) == Some(&item.edge),
            "UNAUTHORIZED_OBJECT"
        );
        Ok(())
    }

    pub fn content_edge_at(
        &self,
        reference: &str,
        generation: u64,
        offset: u64,
        node_level: Option<u8>,
    ) -> Result<AdmittedEdge> {
        let path = self
            .content_trace(reference, generation, offset, node_level)?
            .context("UNAUTHORIZED_OBJECT")?;
        Ok(AdmittedEdge {
            reference: reference.into(),
            generation,
            edge: path.last().context("UNAUTHORIZED_OBJECT")?.clone(),
        })
    }

    fn content_witness(&self, key: ObjectKey) -> Result<Witness> {
        let json: String = self
            .db
            .query_row(
                "SELECT witness FROM content_object_jobs WHERE kind=?1 AND digest=?2",
                params![kind(key.kind), key.digest.to_string()],
                |r| r.get(0),
            )
            .optional()?
            .context("UNAUTHORIZED_OBJECT")?;
        ensure!(json.len() <= 16_384, "INVALID_CONTENT_EDGE");
        let witness: Witness = serde_json::from_str(&json)?;
        ensure!(
            witness.ancestors.len() <= crate::model::content::MAX_DEPTH as usize
                && witness.edge.key == key,
            "INVALID_CONTENT_EDGE"
        );
        Ok(witness)
    }

    /// Reuse a page-level alias when the original witness is withdrawn. Inspect
    /// at most 256 aliases per call; persist a cursor so stale history never
    /// becomes an unbounded join or prevents eventual discovery of older roots.
    fn resolve_content_witness(
        &self,
        witness: &Witness,
        only: Option<(&str, u64)>,
        shared: bool,
    ) -> Result<Option<AdmittedEdge>> {
        if only.is_none_or(|(r, g)| r == witness.reference && g == witness.generation) {
            let item = AdmittedEdge {
                reference: witness.reference.clone(),
                generation: witness.generation,
                edge: witness.edge.clone(),
            };
            let path = self.content_trace(
                &item.reference,
                item.generation,
                item.edge.offset,
                item.edge.level,
            )?;
            if path.as_ref().and_then(|p| p.last()) == Some(&item.edge)
                && (!shared
                    || self.content_reference_authorized(&item.reference, item.generation)?)
            {
                return Ok(Some(item));
            }
        }
        let mut remaining = EDGE_PAGE;
        for ancestor in witness
            .ancestors
            .iter()
            .chain(std::iter::once(&witness.edge))
            .rev()
        {
            if remaining == 0 {
                break;
            }
            let key = ancestor.key;
            let cursor: u64 =
                if only.is_some() {
                    0
                } else {
                    self.db.query_row(
                "SELECT before_id FROM content_alias_cursor WHERE kind=?1 AND digest=?2",
                params![kind(key.kind),key.digest.to_string()],|r|r.get(0)).optional()?.unwrap_or(0)
                };
            let aliases: Vec<(u64, String, u64, u64)> = if let Some((reference, generation)) = only
            {
                self.db.prepare_cached("SELECT id,reference,generation,offset FROM content_page_aliases
                    WHERE kind=?1 AND digest=?2 AND reference=?3 AND generation=?4 ORDER BY offset LIMIT ?5")?
                    .query_map(params![kind(key.kind),key.digest.to_string(),reference,generation,remaining as i64],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?
                    .collect::<rusqlite::Result<_>>()?
            } else {
                self.db
                    .prepare_cached(
                        "SELECT id,reference,generation,offset FROM content_page_aliases
                    WHERE kind=?1 AND digest=?2 AND id<?3 ORDER BY id DESC LIMIT ?4",
                    )?
                    .query_map(
                        params![
                            kind(key.kind),
                            key.digest.to_string(),
                            if cursor == 0 { i64::MAX as u64 } else { cursor },
                            remaining as i64
                        ],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                    )?
                    .collect::<rusqlite::Result<_>>()?
            };
            for (_, reference, generation, offset) in &aliases {
                if shared && !self.content_reference_authorized(reference, *generation)? {
                    continue;
                }
                let Some(path) =
                    self.content_trace(reference, *generation, *offset, ancestor.level)?
                else {
                    continue;
                };
                if path
                    .last()
                    .is_none_or(|e| e.key != key || e.len != ancestor.len)
                {
                    continue;
                }
                let Some(relative) = witness.edge.offset.checked_sub(ancestor.offset) else {
                    continue;
                };
                let Some(offset) = offset.checked_add(relative) else {
                    continue;
                };
                let Some(path) =
                    self.content_trace(reference, *generation, offset, witness.edge.level)?
                else {
                    continue;
                };
                let Some(edge) = path.last() else {
                    continue;
                };
                if edge.key == witness.edge.key && edge.len == witness.edge.len {
                    return Ok(Some(AdmittedEdge {
                        reference: reference.clone(),
                        generation: *generation,
                        edge: edge.clone(),
                    }));
                }
            }
            if only.is_none() {
                let next = if aliases.len() == remaining {
                    aliases.last().map_or(0, |a| a.0)
                } else {
                    0
                };
                self.db.execute(
                    "INSERT INTO content_alias_cursor VALUES(?1,?2,?3)
                    ON CONFLICT(kind,digest) DO UPDATE SET before_id=excluded.before_id",
                    params![kind(key.kind), key.digest.to_string(), next],
                )?;
            }
            remaining -= aliases.len();
        }
        Ok(None)
    }

    pub fn content_edge(
        &self,
        reference: &str,
        generation: u64,
        key: ObjectKey,
    ) -> Result<AdmittedEdge> {
        let witness = self.content_witness(key)?;
        self.resolve_content_witness(&witness, Some((reference, generation)), false)?
            .context("UNAUTHORIZED_OBJECT")
    }

    pub fn content_edge_expanded(&self, item: &AdmittedEdge) -> Result<bool> {
        self.validate_admitted_edge(item)?;
        Ok(self.db.query_row(
            "SELECT expanded FROM content_object_jobs WHERE kind=?1 AND digest=?2",
            params![kind(item.edge.key.kind), item.edge.key.digest.to_string()],
            |r| r.get(0),
        )?)
    }

    pub fn content_range_candidates(
        &self,
        reference: &str,
        range: crate::model::workspace::ByteRange,
    ) -> Result<Vec<AdmittedEdge>> {
        let (generation, root, granted) = self.content_root(reference)?;
        ensure!(granted, "UNAUTHORIZED_OBJECT");
        range.validate(root.total_len)?;
        ensure!(
            range.len <= crate::model::workspace::limits::READ_BYTES as u64,
            "CONTENT_BUFFER_LIMIT"
        );
        if range.len == 0 {
            return Ok(vec![]);
        }
        let top = if root.kind == ContentKind::Manifest {
            i64::from(ManifestNode::root_level(root.total_len)?)
        } else {
            -1
        };
        let mut items = vec![];
        for node_level in (-1..=top).rev() {
            let span = if node_level < 0 {
                CHUNK_SIZE
            } else {
                ManifestNode::child_capacity(node_level as u8)? * 256
            };
            let mut offset = range.offset / span * span;
            while offset < range.end()? {
                if let Some(path) = self.content_trace(
                    reference,
                    generation,
                    offset,
                    (node_level >= 0).then_some(node_level as u8),
                )? {
                    items.push(AdmittedEdge {
                        reference: reference.into(),
                        generation,
                        edge: path.last().context("UNAUTHORIZED_OBJECT")?.clone(),
                    });
                }
                offset = offset.checked_add(span).context("MANIFEST_OVERFLOW")?;
            }
        }
        Ok(items)
    }

    pub fn content_candidates(
        &self,
        limit: usize,
        manifests_only: bool,
    ) -> Result<Vec<AdmittedEdge>> {
        ensure!((1..=EDGE_PAGE).contains(&limit), "ADMISSION_PAGE_LIMIT");
        let sql = if manifests_only {
            "SELECT witness FROM content_object_jobs WHERE kind=1 AND expanded=0 AND retry_at_ms<=?1 ORDER BY retry_at_ms,digest LIMIT ?2"
        } else {
            "SELECT witness FROM content_object_jobs WHERE verified=0 AND retry_at_ms<=?1 ORDER BY retry_at_ms,kind,digest LIMIT ?2"
        };
        let rows: Vec<String> = self
            .db
            .prepare_cached(sql)?
            .query_map(params![time(), limit as i64], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        let mut items = vec![];
        for json in rows {
            ensure!(json.len() <= 16_384, "INVALID_CONTENT_EDGE");
            let witness: Witness = serde_json::from_str(&json)?;
            ensure!(
                witness.ancestors.len() <= crate::model::content::MAX_DEPTH as usize,
                "MANIFEST_DEPTH"
            );
            if let Some(item) = self.resolve_content_witness(&witness, None, !manifests_only)? {
                items.push(item);
            } else {
                self.db.execute(
                    "UPDATE content_object_jobs SET retry_at_ms=?1 WHERE kind=?2 AND digest=?3",
                    params![
                        time().saturating_add(1000),
                        kind(witness.edge.key.kind),
                        witness.edge.key.digest.to_string()
                    ],
                )?;
            }
        }
        Ok(items)
    }

    pub fn verify_content_edge(
        &mut self,
        provider: &LocalContent,
        item: &AdmittedEdge,
        context: &ReadContext,
    ) -> Result<()> {
        self.verify_content_edge_prioritized(
            provider,
            item,
            crate::model::workspace::DemandPriority::Background,
            context,
        )
    }

    pub fn verify_content_edge_prioritized(
        &mut self,
        provider: &LocalContent,
        item: &AdmittedEdge,
        priority: crate::model::workspace::DemandPriority,
        context: &ReadContext,
    ) -> Result<()> {
        let bytes = provider.read_blob_for(
            item.edge.key,
            (item.edge.key.kind == ObjectKind::Bytes).then_some(item.edge.len),
            priority,
            context,
        )?;
        validate_blob(&item.edge, &bytes)?;
        self.with_write("content_verify_edge",|store| {
            store.validate_admitted_edge(item)?;
            store.db.execute("UPDATE content_object_jobs SET verified=1,retry_at_ms=0,error=NULL WHERE kind=?1 AND digest=?2",
                params![kind(item.edge.key.kind),item.edge.key.digest.to_string()])?;
            Ok(())
        })
    }

    pub fn expand_content_edge(
        &mut self,
        provider: &LocalContent,
        item: &AdmittedEdge,
        context: &ReadContext,
    ) -> Result<usize> {
        self.validate_admitted_edge(item)?;
        if self.content_edge_expanded(item)? {
            return Ok(0);
        }
        let children = provider.verified_children(&item.edge, context)?;
        self.with_write("content_expand_edge",|store| {
            store.validate_admitted_edge(item)?;
            let ancestors=store.content_trace(&item.reference,item.generation,item.edge.offset,item.edge.level)?
                .context("UNAUTHORIZED_OBJECT")?;
            ensure!(ancestors.last()==Some(&item.edge),"UNAUTHORIZED_OBJECT");
            // The trace already includes this parent, so every child receives
            // a bounded root-to-parent witness without descendant replay.
            for (ordinal,child) in children.iter().enumerate() {
                store.db.execute("INSERT OR IGNORE INTO content_manifest_edges VALUES(?1,?2,?3,?4,?5,?6)",
                    params![item.edge.key.digest.to_string(),ordinal as u64,kind(child.key.kind),child.key.digest.to_string(),child.len,level(child.level)])?;
                admit(&store.db,&AdmittedEdge {reference:item.reference.clone(),generation:item.generation,edge:child.clone()},&ancestors)?;
            }
            store.db.execute("UPDATE content_object_jobs SET expanded=1,verified=1,error=NULL,retry_at_ms=0 WHERE kind=?1 AND digest=?2",
                params![kind(item.edge.key.kind),item.edge.key.digest.to_string()])?;
            Ok(children.len())
        })
    }

    pub fn defer_content_edge(
        &mut self,
        item: &AdmittedEdge,
        error: &anyhow::Error,
        delay_ms: u64,
    ) -> Result<()> {
        let wire = crate::model::errors::ErrorWire::from_error(error);
        self.with_write("content_defer_edge",|store| {
            store.validate_admitted_edge(item)?;
            store.db.execute("UPDATE content_object_jobs SET verified=0,error=?1,retry_at_ms=?2 WHERE kind=?3 AND digest=?4",
                params![wire.code,time().saturating_add(delay_ms.min(60_000)),kind(item.edge.key.kind),item.edge.key.digest.to_string()])?;
            Ok(())
        })
    }
}
