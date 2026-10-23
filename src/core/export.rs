//! One current export path for shared metadata and pinned typed content.
use super::*;
use crate::{
    backend::ObjectBackend,
    content::{ContentEdge, ContentProvider, LocalContent, PinnedContent, ReadContext},
    model::content::{FileContentRef, ObjectKey, ObjectKind},
};
use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
};

pub struct SharedExport {
    root: PathBuf,
    repo: String,
    device: String,
    branches: Vec<Branch>,
    events: Vec<Event>,
    provider: LocalContent,
    roots: Vec<ExportRoot>,
}

struct ExportRoot {
    pin: PinnedContent,
    reference: String,
    generation: u64,
    publishable: bool,
}

fn check(context: &ReadContext, cancelled: &dyn Fn() -> bool) -> Result<()> {
    ensure!(!cancelled(), "CONTENT_CANCELLED");
    context.check()
}

impl Store {
    /// Capture one metadata snapshot and pin its current content roots. Call
    /// this on a secondary connection, outside Engine, for worker exports.
    pub fn prepare_shared_export(
        &self,
        context: &ReadContext,
        cancelled: &dyn Fn() -> bool,
    ) -> Result<SharedExport> {
        check(context, cancelled)?;
        ensure!(self.db.is_autocommit(), "CONTENT_EXPORT_REQUIRES_SNAPSHOT");
        let provider = LocalContent::open(&self.root)?;
        let tx = self.db.unchecked_transaction()?;
        let branches: Vec<_> = self.branches()?.into_iter().filter(|b| b.shared).collect();
        let mut events = Vec::new();
        let mut roots: BTreeMap<[u8; 48], Option<ExportRoot>> = BTreeMap::new();
        let mut statement = self.db.prepare(
            "SELECT e.payload FROM events e JOIN branches b ON b.id=e.branch WHERE b.shared=1 ORDER BY e.id",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            check(context, cancelled)?;
            let event: Event = serde_json::from_str(row.get_ref(0)?.as_str()?)?;
            ensure!(
                event.branch.shared && event.repo == self.repo,
                "CORRUPT_EXPORT_METADATA"
            );
            for change in event.changes.iter().filter(|c| c.field == "content") {
                let reference: FileContentRef =
                    serde_json::from_value(change.value.clone()).context("INVALID_CONTENT")?;
                let key = reference.encode()?;
                let identity = super::admission::reference_id(&event.id, &change.entity);
                let admitted: Option<(u64, bool)> = self.db.query_row(
                    "SELECT generation,publishable FROM content_root_metadata WHERE reference=?1 AND root=?2 AND granted=1 AND shared=1",
                    params![identity, key.as_slice()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                ).optional()?;
                let selected = roots.entry(key).or_insert(None);
                if let Some((generation, publishable)) = admitted
                    && selected.as_ref().is_none_or(|old| publishable && !old.publishable)
                {
                    *selected = Some(ExportRoot {
                        pin: provider.pin(&reference)?,
                        reference: identity,
                        generation,
                        publishable,
                    });
                }
            }
            events.push(event);
        }
        drop(rows);
        drop(statement);
        check(context, cancelled)?;
        let roots = roots.into_values().map(|root| root.context("UNAUTHORIZED_OBJECT")).collect::<Result<Vec<_>>>()?;
        tx.commit()?;
        Ok(SharedExport {
            root: self.root.clone(),
            repo: self.repo.clone(),
            device: self.device.clone(),
            branches,
            events,
            provider,
            roots,
        })
    }

    /// Explicit local test export; authority remains rooted in shared events.
    pub fn export_shared_objects(&self, backend: &dyn ObjectBackend) -> Result<Value> {
        let context = super::content::import_context();
        self.prepare_shared_export(&context, &|| false)?
            .write(backend, &context, &|| false)
    }
}

/// The streamed export index is a generic temporary artifact, not staged file
/// bytes or a mutable write-session snapshot. Drop removes it on every outcome.
struct IndexFile {
    file: Option<File>,
    path: PathBuf,
}
impl IndexFile {
    fn new(root: &Path) -> Result<Self> {
        let path = root.join("objects").join(format!("export-{}.part", id()));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        Ok(Self {
            file: Some(file),
            path,
        })
    }
}
impl Drop for IndexFile {
    fn drop(&mut self) {
        drop(self.file.take());
        let _ = fs::remove_file(&self.path);
    }
}

struct CheckedReader<'a> {
    file: &'a mut File,
    context: &'a ReadContext,
    cancelled: &'a dyn Fn() -> bool,
}
impl Read for CheckedReader<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        check(self.context, self.cancelled).map_err(std::io::Error::other)?;
        self.file.read(bytes)
    }
}

struct ObjectWriter<'a> {
    backend: &'a dyn ObjectBackend,
    index: &'a mut File,
    objects: BTreeMap<ObjectKey, u64>,
    context: &'a ReadContext,
    cancelled: &'a dyn Fn() -> bool,
}

impl SharedExport {
    /// Bytes and backend I/O run after the capture transaction has ended. The
    /// index records raw backend blob hashes separately from typed identities:
    /// manifest digests have a domain and are not hashes of raw file bytes.
    pub fn write(
        &self,
        backend: &dyn ObjectBackend,
        context: &ReadContext,
        cancelled: &dyn Fn() -> bool,
    ) -> Result<Value> {
        check(context, cancelled)?;
        let source = Store::open_transfer_nowait(&self.root, &self.repo, &self.device)?;
        let mut temporary = IndexFile::new(&self.root)?;
        let file = temporary.file.as_mut().unwrap();
        file.write_all(b"{\"format\":\"tkfs-shared-export-2\",\"repo\":")?;
        serde_json::to_writer(&mut *file, &self.repo)?;
        file.write_all(b",\"branches\":")?;
        serde_json::to_writer(&mut *file, &self.branches)?;
        file.write_all(b",\"events\":[")?;
        for (i, event) in self.events.iter().enumerate() {
            check(context, cancelled)?;
            if i != 0 {
                file.write_all(b",")?;
            }
            serde_json::to_writer(&mut *file, event)?;
        }
        file.write_all(b"],\"objects\":[")?;
        let mut writer = ObjectWriter {
            backend,
            index: file,
            objects: BTreeMap::new(),
            context,
            cancelled,
        };
        for root in &self.roots {
            check(context, cancelled)?;
            ensure!(source.content_reference_authorized(&root.reference, root.generation)?, "STALE_ADMISSION_GENERATION");
            let mut walk = self.provider.walk(&root.pin)?;
            while let Some(batch) = self.provider.walk_step(&mut walk, context)? {
                writer.copy_object(&source, &self.provider, root, &batch.visited)?;
                for child in batch
                    .children
                    .iter()
                    .filter(|e| e.key.kind == ObjectKind::Bytes)
                {
                    writer.copy_object(&source, &self.provider, root, child)?;
                }
            }
        }
        let objects = writer.objects.len();
        drop(writer);
        check(context, cancelled)?;
        file.write_all(b"]}")?;
        file.seek(SeekFrom::Start(0))?;
        let manifest = backend.put_stream(&mut CheckedReader {
            file,
            context,
            cancelled,
        })?;
        check(context, cancelled)?;
        Ok(
            json!({"backend":"local-directory-test-bucket","manifest":manifest,"objects":objects,"events":self.events.len(),"remote_validated":false}),
        )
    }
}

impl ObjectWriter<'_> {
    fn copy_object(&mut self, source: &Store, provider: &LocalContent, root: &ExportRoot, edge: &ContentEdge) -> Result<()> {
        check(self.context, self.cancelled)?;
        if let Some(size) = self.objects.get(&edge.key) {
            ensure!(
                edge.key.kind != ObjectKind::Bytes || *size == edge.len,
                "CORRUPT_CONTENT"
            );
            return Ok(());
        }
        crate::util::fault::trigger("content.export.before_object")?;
        check(self.context, self.cancelled)?;
        let (generation, bytes) = source.read_content_frame_prioritized(provider, &root.reference, edge, crate::model::workspace::DemandPriority::Background, self.context)?;
        ensure!(generation == root.generation, "STALE_ADMISSION_GENERATION");
        let object = hash(&bytes);
        ensure!(
            self.backend.put_verified(&bytes)? == object,
            "BACKEND_HASH_MISMATCH"
        );
        check(self.context, self.cancelled)?;
        if !self.objects.is_empty() {
            self.index.write_all(b",")?;
        }
        serde_json::to_writer(
            &mut *self.index,
            &json!({"key":edge.key,"object":object,"bytes":bytes.len()}),
        )?;
        self.objects.insert(edge.key, bytes.len() as u64);
        Ok(())
    }
}
