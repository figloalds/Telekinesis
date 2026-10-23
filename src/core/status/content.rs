//! Read-only summaries of the current typed graph. No CAS reads or file hashes.
//! A page query holds at most FANOUT children; only page summaries are memoized.
use super::*;
use crate::model::content::{
    ChildKind, ContentKind, Digest, FANOUT, FileContentRef, ManifestChild, ManifestNode, ObjectKey,
    ObjectKind,
};

#[derive(Clone, Copy, Default)]
struct Summary {
    missing_bytes: u64,
    public_missing_bytes: u64,
    missing: bool,
    indexing: bool,
    corrupt: bool,
}

impl Summary {
    fn unavailable(len: u64, state: &str) -> Self {
        Self {
            missing_bytes: len,
            public_missing_bytes: len,
            missing: state == "missing",
            indexing: state == "indexing",
            corrupt: state == "corrupt",
        }
    }

    fn state(self) -> &'static str {
        if self.corrupt {
            "corrupt"
        } else if self.missing {
            "missing"
        } else if self.indexing {
            "indexing"
        } else {
            "available"
        }
    }

    fn available(self) -> bool {
        self.state() == "available"
    }
}

#[derive(Default)]
struct Evidence {
    verified: bool,
    expanded: bool,
    corrupt: bool,
    public: bool,
}

fn evidence(row: &rusqlite::Row<'_>, start: usize) -> rusqlite::Result<Evidence> {
    Ok(Evidence {
        verified: row.get::<_, Option<bool>>(start)?.unwrap_or(false),
        expanded: row.get::<_, Option<bool>>(start + 1)?.unwrap_or(false),
        corrupt: row
            .get::<_, Option<String>>(start + 2)?
            .is_some_and(|error| error.starts_with("CORRUPT_CONTENT")),
        public: row.get(start + 3)?,
    })
}

struct Scan<'a> {
    store: &'a Store,
    bytes: &'a Presence,
    manifests: &'a Presence,
    pages: HashMap<(Digest, u64, u8), Summary>,
}

impl Scan<'_> {
    fn root(&mut self, root: &FileContentRef) -> Result<Summary> {
        root.validate()?;
        if root.total_len == 0 {
            return Ok(Summary::default());
        }
        let key = ObjectKey::root(root);
        let evidence = self
            .store
            .db
            .prepare_cached(
                "SELECT j.verified,j.expanded,j.error,
                 EXISTS(SELECT 1 FROM content_public_receipts r WHERE r.kind=?1 AND r.digest=?2)
                 FROM content_object_jobs j WHERE j.kind=?1 AND j.digest=?2",
            )?
            .query_row(
                params![
                    if key.kind == ObjectKind::Bytes { 0 } else { 1 },
                    key.digest.to_string()
                ],
                |row| evidence(row, 0),
            )
            .optional()?
            .unwrap_or_default();
        let level = if root.kind == ContentKind::Manifest {
            Some(ManifestNode::root_level(root.total_len)?)
        } else {
            None
        };
        self.object(key, root.total_len, level, evidence)
    }

    fn object(
        &mut self,
        key: ObjectKey,
        len: u64,
        level: Option<u8>,
        evidence: Evidence,
    ) -> Result<Summary> {
        let digest = key.digest.to_string();
        if evidence.corrupt {
            return Ok(Summary::unavailable(len, "corrupt"));
        }
        let presence = if level.is_some() {
            self.manifests
        } else {
            self.bytes
        };
        let physical = presence.files.get(&digest).map(|entry| entry.0);
        if physical.is_none() || (level.is_none() && physical != Some(len)) {
            return Ok(Summary::unavailable(len, "missing"));
        }
        if !evidence.verified || (level.is_some() && !evidence.expanded) {
            return Ok(Summary::unavailable(len, "indexing"));
        }
        let Some(level) = level else {
            return Ok(Summary {
                public_missing_bytes: if evidence.public { 0 } else { len },
                ..Summary::default()
            });
        };
        let memo_key = (key.digest, len, level);
        if let Some(summary) = self.pages.get(&memo_key) {
            return Ok(*summary);
        }
        // The persisted index is derived. Incomplete or inconsistent adjacency
        // means indexing is pending, never permission to declare a file complete.
        let mut statement = self.store.db.prepare_cached(
            "SELECT e.ordinal,e.kind,e.digest,e.len,e.level,j.verified,j.expanded,j.error,
             EXISTS(SELECT 1 FROM content_public_receipts r WHERE r.kind=e.kind AND r.digest=e.digest)
             FROM content_manifest_edges e LEFT JOIN content_object_jobs j
             ON j.kind=e.kind AND j.digest=e.digest WHERE e.parent=?1 ORDER BY e.ordinal LIMIT ?2",
        )?;
        let mut rows = statement.query(params![digest, FANOUT + 1])?;
        let mut children = Vec::new();
        let mut child_evidence = Vec::new();
        let mut valid = true;
        while let Some(row) = rows.next()? {
            let ordinal: usize = row.get(0)?;
            let kind: i64 = row.get(1)?;
            let child_level: i64 = row.get(4)?;
            let child_digest = Digest::parse(row.get_ref(2)?.as_str()?);
            if ordinal != children.len()
                || kind != i64::from(level != 0)
                || child_level != i64::from(level) - 1
                || child_digest.is_err()
            {
                valid = false;
                break;
            }
            children.push(ManifestChild {
                kind: if level == 0 {
                    ChildKind::Chunk
                } else {
                    ChildKind::Node
                },
                digest: child_digest?,
                len: row.get(3)?,
            });
            child_evidence.push(evidence_from_row(row)?);
        }
        drop(rows);
        drop(statement);
        let node = ManifestNode {
            level,
            total_len: len,
            children,
        };
        if !valid
            || node.digest().ok() != Some(key.digest)
            || physical != Some((16 + 41 * node.children.len()) as u64)
        {
            return Ok(Summary::unavailable(len, "indexing"));
        }
        let mut summary = Summary::default();
        for (child, evidence) in node.children.into_iter().zip(child_evidence) {
            let child = self.object(
                ObjectKey {
                    kind: if level == 0 {
                        ObjectKind::Bytes
                    } else {
                        ObjectKind::Manifest
                    },
                    digest: child.digest,
                },
                child.len,
                level.checked_sub(1),
                evidence,
            )?;
            summary.missing_bytes += child.missing_bytes;
            summary.public_missing_bytes += child.public_missing_bytes;
            summary.missing |= child.missing;
            summary.indexing |= child.indexing;
            summary.corrupt |= child.corrupt;
        }
        if !evidence.public {
            summary.public_missing_bytes = len;
        }
        self.pages.insert(memo_key, summary);
        Ok(summary)
    }
}

fn evidence_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Evidence> {
    evidence(row, 5)
}

impl Store {
    pub(super) fn scan_content(
        &self,
        bytes: &Presence,
        manifests: &Presence,
    ) -> Result<(ContentCounters, Vec<Value>)> {
        let view = self.view(&self.active)?;
        let mut current = BTreeMap::new();
        for entry in view
            .projection
            .entries
            .values()
            .filter(|e| e.alive && e.kind == "file")
        {
            let root = entry
                .content_ref
                .as_ref()
                .context("MISSING_CONTENT_DESCRIPTOR")?;
            current.entry(root.encode()?).or_insert(root);
        }
        let shared: Vec<(Vec<u8>, bool)> = self.db.prepare_cached(
            "SELECT root,max(publishable) FROM content_root_metadata WHERE granted=1 AND shared=1 GROUP BY root",
        )?.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?.collect::<rusqlite::Result<_>>()?;
        let mut scan = Scan {
            store: self,
            bytes,
            manifests,
            pages: HashMap::new(),
        };
        let mut states = BTreeMap::new();
        let mut counters = ContentCounters {
            current_objects: current.len() as u64,
            ..ContentCounters::default()
        };
        for (encoded, root) in current {
            let summary = scan.root(root)?;
            counters.current_available += u64::from(summary.available());
            counters.current_missing_bytes = counters
                .current_missing_bytes
                .saturating_add(summary.missing_bytes);
            states.insert(encoded, summary);
        }
        counters.current_missing = counters.current_objects - counters.current_available;
        for (encoded, publishable) in shared {
            let root = FileContentRef::decode(&encoded)?;
            let summary = match states.get(&root.encode()?) {
                Some(summary) => *summary,
                None => scan.root(&root)?,
            };
            let missing = if publishable {
                summary.missing_bytes
            } else {
                summary.public_missing_bytes
            };
            counters.shared_objects += 1;
            counters.shared_available += u64::from(missing == 0 && summary.available());
            counters.shared_missing_bytes = counters.shared_missing_bytes.saturating_add(missing);
        }
        counters.complete = counters.shared_available == counters.shared_objects;
        let mut missing = Vec::new();
        for entry in view
            .projection
            .entries
            .values()
            .filter(|e| e.alive && e.kind == "file")
        {
            let root = entry
                .content_ref
                .as_ref()
                .context("MISSING_CONTENT_DESCRIPTOR")?;
            let summary = states[&root.encode()?];
            if summary.available() {
                continue;
            }
            let mut parts = vec![entry.name.clone()];
            let mut parent = &entry.parent;
            let mut seen = BTreeSet::new();
            while parent != ROOT && seen.insert(parent.clone()) {
                let Some(directory) = view.projection.entries.get(parent) else {
                    break;
                };
                parts.push(directory.name.clone());
                parent = &directory.parent;
            }
            parts.reverse();
            let revision = entry.revisions.get("content");
            missing.push(json!({
                "entity":entry.id,"revision":revision,
                "reference":revision.map(|revision| super::super::admission::reference_id(revision,&entry.id)),
                "descriptor":root,"path":parts.join("/"),"available":false,
                "state":summary.state(),"missing_bytes":summary.missing_bytes
            }));
            if missing.len() == CONTENT_ITEMS {
                break;
            }
        }
        Ok((counters, missing))
    }
}
