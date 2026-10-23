//! Verified 64 KiB CAS and indexed manifests. All byte work is independent of
//! metadata locks and SQLite. This module is not a network authorization surface.
use super::provider::{Availability, ContentProvider, PinnedContent, ReadContext};
use super::walk::{ContentEdge, ContentWalk, WalkBatch};
use crate::{
    model::{content::*, workspace::ByteRange},
    util::{
        lock::lock,
        phases::{Phase, PhaseCounters},
    },
};
use anyhow::{Context, Result, ensure};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
};
use uuid::Uuid;

pub use crate::model::content::{ObjectKey, ObjectKind};

/// Verified immutable import, pinned until its metadata publication finishes.
#[derive(Clone)]
pub struct PreparedContent {
    pin: PinnedContent,
}
impl PreparedContent {
    pub fn content(&self) -> &FileContentRef {
        self.pin.reference()
    }
    pub fn pin(&self) -> &PinnedContent {
        &self.pin
    }
    pub(crate) fn check_store(&self, root: &Path) -> Result<()> {
        ensure!(
            self.pin.provider() == LocalContent::open(root)?.identity(),
            "CONTENT_SAVE_WRONG_STORE"
        );
        Ok(())
    }
}
impl std::fmt::Debug for PreparedContent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedContent")
            .field("content", self.content())
            .finish()
    }
}

#[derive(Debug)]
enum ContentError {
    Missing(ObjectKey),
    Corrupt(ObjectKey),
}

impl std::fmt::Display for ContentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing(key) => write!(f, "CONTENT_UNAVAILABLE: {}", key.digest),
            Self::Corrupt(key) => write!(f, "CORRUPT_CONTENT: {}", key.digest),
        }
    }
}
impl std::error::Error for ContentError {}

#[derive(Default)]
struct Lifetimes {
    generation: u64,
    pins: BTreeMap<[u8; 48], (FileContentRef, usize)>,
}

struct PinLease {
    owner: Arc<ContentOwner>,
    key: [u8; 48],
}

impl Drop for PinLease {
    fn drop(&mut self) {
        let mut state = lock(&self.owner.lifetimes);
        if let Some((_, count)) = state.pins.get_mut(&self.key) {
            *count -= 1;
            if *count == 0 {
                state.pins.remove(&self.key);
            }
        }
        state.generation = state.generation.wrapping_add(1);
    }
}

struct ContentOwner {
    id: Uuid,
    lifetimes: Mutex<Lifetimes>,
    install_locks: crate::core::objects::ObjectLocks,
    buffers: Mutex<BufferState>,
    counters: Arc<PhaseCounters>,
}

#[derive(Clone, Copy)]
enum BufferClass {
    Foreground,
    Background,
    Structural,
}

#[derive(Default)]
struct BufferState {
    foreground: usize,
    background: usize,
    structural: bool,
    peak: usize,
}

static OWNERS: Mutex<BTreeMap<PathBuf, Weak<ContentOwner>>> = Mutex::new(BTreeMap::new());

/// Handles to one canonical store share installation gates, budgets and pins.
/// Pins keep that owner alive even after the last LocalContent handle drops.
/// The existing worker store lock must exclude other writing processes.
/// This constructor does not open or migrate a metadata database.
pub struct LocalContent {
    root: PathBuf,
    owner: Arc<ContentOwner>,
    pub counters: Arc<PhaseCounters>,
}

/// Private save preparation. The caller retains and revalidates the selected
/// checkpoint; these classifications confer neither publication nor permission.
pub(crate) enum EditSpan {
    Unchanged,
    Zero,
    Changed,
}
pub(crate) trait EditSource {
    fn check(&self, context: &ReadContext) -> Result<()>;
    fn span(&mut self, offset: u64, len: u64, context: &ReadContext) -> Result<EditSpan>;
    fn read(&mut self, offset: u64, out: &mut [u8], context: &ReadContext) -> Result<()>;
}

#[derive(Debug)]
struct RebuildRange(ByteRange);
impl std::fmt::Display for RebuildRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "immutable base path at {}", self.0.offset)
    }
}
impl std::error::Error for RebuildRange {}

/// One authenticated path, not a cache proportional to the base file. Repeated
/// sequential lookups keep one page at each level, including on depth changes.
struct BaseCursor<'a> {
    provider: &'a LocalContent,
    root: Option<ContentEdge>,
    pages: Vec<Option<(ContentEdge, ManifestNode)>>,
}
impl BaseCursor<'_> {
    fn find(
        &mut self,
        offset: u64,
        level: Option<u8>,
        context: &ReadContext,
    ) -> Result<Option<ContentEdge>> {
        let Some(mut edge) = self.root.clone() else {
            return Ok(None);
        };
        if offset >= edge.len {
            return Ok(None);
        }
        loop {
            context.check()?;
            if edge.level == level {
                return Ok((edge.offset == offset).then_some(edge));
            }
            if edge.level < level {
                return Ok(None);
            }
            let old_level = edge.level.context("INVALID_MANIFEST_ENCODING")?;
            let slot = &mut self.pages[old_level as usize];
            if slot.as_ref().is_none_or(|(cached, _)| cached != &edge) {
                let node = self
                    .provider
                    .node(edge.key.digest, edge.len, old_level, context)
                    .map_err(|error| error.context(RebuildRange(ByteRange { offset, len: 1 })))?;
                *slot = Some((edge.clone(), node));
            }
            let node = &slot.as_ref().unwrap().1;
            let capacity = ManifestNode::child_capacity(old_level)?;
            let index = ((offset - edge.offset) / capacity) as usize;
            let child = node.children.get(index).context("MANIFEST_COVERAGE")?;
            edge = ContentEdge {
                key: ObjectKey {
                    kind: if old_level == 0 {
                        ObjectKind::Bytes
                    } else {
                        ObjectKind::Manifest
                    },
                    digest: child.digest,
                },
                offset: edge.offset + index as u64 * capacity,
                len: child.len,
                level: old_level.checked_sub(1),
            };
        }
    }
}

impl LocalContent {
    pub(crate) fn rebuild_range(error: &anyhow::Error) -> Option<ByteRange> {
        error.downcast_ref::<RebuildRange>().map(|range| range.0)
    }
    pub fn unavailable_key(error: &anyhow::Error) -> Option<ObjectKey> {
        error
            .chain()
            .find_map(|cause| match cause.downcast_ref::<ContentError>() {
                Some(ContentError::Missing(key) | ContentError::Corrupt(key)) => Some(*key),
                None => None,
            })
    }
    pub(crate) fn receipt_directory(&self) -> PathBuf {
        self.root.join("content-receipts")
    }
    pub fn identity(&self) -> Uuid {
        self.owner.id
    }

    /// Bounded local byte access for the object engine. Network callers must
    /// additionally prove reference authorization and disclosure provenance.
    pub fn read_blob(
        &self,
        key: ObjectKey,
        expected_len: Option<u64>,
        context: &ReadContext,
    ) -> Result<Vec<u8>> {
        self.read_blob_for(
            key,
            expected_len,
            crate::model::workspace::DemandPriority::Background,
            context,
        )
    }

    pub fn read_blob_for(
        &self,
        key: ObjectKey,
        expected_len: Option<u64>,
        priority: crate::model::workspace::DemandPriority,
        context: &ReadContext,
    ) -> Result<Vec<u8>> {
        let _permit = self.permit(
            context,
            1,
            if priority == crate::model::workspace::DemandPriority::Foreground {
                BufferClass::Foreground
            } else if key.kind == ObjectKind::Manifest {
                BufferClass::Structural
            } else {
                BufferClass::Background
            },
        )?;
        self.verified_blob(key, expected_len, context)
    }
    pub fn open(root: &Path) -> Result<Self> {
        ensure!(root.is_dir(), "CONTENT_STORE_NOT_INITIALIZED");
        let root = root.canonicalize()?;
        fs::create_dir_all(root.join("objects"))?;
        fs::create_dir_all(root.join("manifests-v1"))?;
        let mut owners = lock(&OWNERS);
        owners.retain(|_, owner| owner.strong_count() != 0);
        let owner = match owners.get(&root).and_then(Weak::upgrade) {
            Some(owner) => owner,
            None => {
                let owner = Arc::new(ContentOwner {
                    id: Uuid::new_v4(),
                    lifetimes: Mutex::new(Lifetimes::default()),
                    install_locks: Default::default(),
                    counters: Default::default(),
                    buffers: Mutex::default(),
                });
                owners.insert(root.clone(), Arc::downgrade(&owner));
                owner
            }
        };
        Ok(Self {
            root,
            counters: owner.counters.clone(),
            owner,
        })
    }

    fn permit(
        &self,
        context: &ReadContext,
        buffers: usize,
        class: BufferClass,
    ) -> Result<BufferPermit<'_>> {
        self.permit_checked(context, buffers, class, &|| Ok(()))
    }

    fn permit_checked(
        &self,
        context: &ReadContext,
        buffers: usize,
        class: BufferClass,
        check: &dyn Fn() -> Result<()>,
    ) -> Result<BufferPermit<'_>> {
        loop {
            context.check()?;
            check()?;
            {
                let mut state = lock(&self.owner.buffers);
                let total = state.foreground + state.background;
                let allowed = match class {
                    BufferClass::Structural => !state.structural,
                    BufferClass::Foreground => {
                        total + buffers
                            <= crate::model::workspace::limits::VERIFY_BUFFERS
                                - if state.background == 0 { 3 } else { 0 }
                    }
                    BufferClass::Background => {
                        total + buffers
                            <= crate::model::workspace::limits::VERIFY_BUFFERS
                                - if state.foreground == 0 { 2 } else { 0 }
                    }
                };
                if allowed {
                    match class {
                        BufferClass::Structural => state.structural = true,
                        BufferClass::Foreground => state.foreground += buffers,
                        BufferClass::Background => state.background += buffers,
                    }
                    state.peak = state
                        .peak
                        .max(state.foreground + state.background + usize::from(state.structural));
                    return Ok(BufferPermit {
                        owner: &self.owner.buffers,
                        buffers,
                        class,
                    });
                }
            }
            context.cancel.sleep(std::time::Duration::from_millis(5));
        }
    }

    pub fn buffer_usage(&self) -> (usize, bool, usize) {
        let state = lock(&self.owner.buffers);
        (
            state.foreground + state.background,
            state.structural,
            state.peak * CHUNK_SIZE as usize,
        )
    }

    pub(crate) fn foreground_permit_checked(
        &self,
        context: &ReadContext,
        check: &dyn Fn() -> Result<()>,
    ) -> Result<BufferPermit<'_>> {
        self.permit_checked(context, 2, BufferClass::Foreground, check)
    }

    /// Authenticate an indexed locator against the immutable root. At most one
    /// page per level is read; derived-index corruption cannot add a private key.
    pub fn verify_member(
        &self,
        pin: &PinnedContent,
        member: &ContentEdge,
        context: &ReadContext,
    ) -> Result<()> {
        self.verify_member_for(
            pin,
            member,
            crate::model::workspace::DemandPriority::Structural,
            context,
        )
    }

    pub fn verify_member_for(
        &self,
        pin: &PinnedContent,
        member: &ContentEdge,
        priority: crate::model::workspace::DemandPriority,
        context: &ReadContext,
    ) -> Result<()> {
        ensure!(pin.provider() == self.owner.id, "CONTENT_PROVIDER_MISMATCH");
        let _permit = self.permit(
            context,
            1,
            if priority == crate::model::workspace::DemandPriority::Foreground {
                BufferClass::Foreground
            } else {
                BufferClass::Structural
            },
        )?;
        let mut current = ContentEdge::root(pin)?;
        for _ in 0..=MAX_DEPTH {
            context.check()?;
            if current.offset == member.offset && current.level == member.level {
                ensure!(
                    current.key == member.key && current.len == member.len,
                    "UNAUTHORIZED_OBJECT"
                );
                return Ok(());
            }
            let level = current.level.context("UNAUTHORIZED_OBJECT")?;
            ensure!(
                member.offset >= current.offset && member.offset < current.offset + current.len,
                "UNAUTHORIZED_OBJECT"
            );
            let node = self.node(current.key.digest, current.len, level, context)?;
            let capacity = ManifestNode::child_capacity(level)?;
            let index = (member.offset - current.offset) / capacity;
            let child = node
                .children
                .get(index as usize)
                .context("UNAUTHORIZED_OBJECT")?;
            current = ContentEdge {
                key: ObjectKey {
                    kind: if level == 0 {
                        ObjectKind::Bytes
                    } else {
                        ObjectKind::Manifest
                    },
                    digest: child.digest,
                },
                offset: current.offset + index * capacity,
                len: child.len,
                level: level.checked_sub(1),
            };
        }
        anyhow::bail!("MANIFEST_DEPTH")
    }

    fn path(&self, key: ObjectKey) -> PathBuf {
        self.root
            .join(match key.kind {
                ObjectKind::Bytes => "objects",
                ObjectKind::Manifest => "manifests-v1",
            })
            .join(key.digest.to_string())
    }

    fn verified_blob(
        &self,
        key: ObjectKey,
        expected: Option<u64>,
        context: &ReadContext,
    ) -> Result<Vec<u8>> {
        context.check()?;
        let mut file = File::open(self.path(key)).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                anyhow::Error::new(ContentError::Missing(key))
            } else {
                error.into()
            }
        })?;
        let len = file.metadata()?.len();
        let cap = match key.kind {
            ObjectKind::Bytes => CHUNK_SIZE,
            ObjectKind::Manifest => MAX_NODE_BYTES as u64,
        };
        ensure!(
            len <= cap && expected.is_none_or(|expected| expected == len),
            ContentError::Corrupt(key)
        );
        let _read = self.counters.begin(
            if key.kind == ObjectKind::Manifest {
                Phase::ManifestRead
            } else {
                Phase::Read
            },
            len,
        );
        let mut bytes = vec![0; len as usize];
        file.read_exact(&mut bytes)?;
        ensure!(file.read(&mut [0])? == 0, ContentError::Corrupt(key));
        {
            let _hash = self.counters.begin(Phase::Hash, len);
            let digest = match key.kind {
                ObjectKind::Bytes => Digest::bytes(&bytes),
                ObjectKind::Manifest => ManifestNode::decode(&bytes)
                    .and_then(|node| node.digest())
                    .map_err(|_| ContentError::Corrupt(key))?,
            };
            ensure!(digest == key.digest, ContentError::Corrupt(key));
        }
        context.check()?;
        Ok(bytes)
    }

    /// Install only verified bytes, with data and rename durability. Joined
    /// installations serialize by typed content identity, outside engine locks.
    pub fn install(&self, key: ObjectKey, bytes: &[u8], context: &ReadContext) -> Result<bool> {
        let _permit = self.permit(
            context,
            1,
            if key.kind == ObjectKind::Manifest {
                BufferClass::Structural
            } else {
                BufferClass::Background
            },
        )?;
        self.install_inner(key, bytes, context)
    }

    fn install_inner(&self, key: ObjectKey, bytes: &[u8], context: &ReadContext) -> Result<bool> {
        context.check()?;
        let digest = {
            let _hash = self.counters.begin(Phase::Hash, bytes.len() as u64);
            match key.kind {
                ObjectKind::Bytes => {
                    ensure!(bytes.len() <= CHUNK_SIZE as usize, "CONTENT_BUFFER_LIMIT");
                    Digest::bytes(bytes)
                }
                ObjectKind::Manifest => ManifestNode::decode(bytes)?.digest()?,
            }
        };
        ensure!(digest == key.digest, ContentError::Corrupt(key));
        let gate = self
            .owner
            .install_locks
            .for_object(&format!("{:?}:{}", key.kind, key.digest));
        // No network or unrelated content identity is blocked by this gate.
        let _guard = loop {
            context.check()?;
            match gate.try_lock() {
                Ok(guard) => break guard,
                Err(std::sync::TryLockError::WouldBlock) => {}
                Err(std::sync::TryLockError::Poisoned(_)) => anyhow::bail!("CONTENT_LOCK_POISONED"),
            }
            context.cancel.sleep(std::time::Duration::from_millis(5));
        };
        match self.verified_blob(key, Some(bytes.len() as u64), context) {
            Ok(_) => return Ok(false),
            Err(error)
                if matches!(
                    error.downcast_ref::<ContentError>(),
                    Some(ContentError::Missing(_))
                ) => {}
            Err(error)
                if matches!(
                    error.downcast_ref::<ContentError>(),
                    Some(ContentError::Corrupt(_))
                ) =>
            {
                let quarantine = self.root.join("quarantine-content");
                match fs::create_dir(&quarantine) {
                    Ok(()) => crate::core::objects::sync_dir(&self.root)?,
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error.into()),
                }
                let retained =
                    quarantine.join(format!("{:?}-{}-{}", key.kind, key.digest, Uuid::new_v4()));
                crate::core::objects::install_object(&self.path(key), &retained)?;
            }
            Err(error) => return Err(error),
        }
        let dest = self.path(key);
        let temp = dest.with_file_name(format!("{}.part", Uuid::new_v4()));
        let result = (|| {
            let _write = self.counters.begin(
                if key.kind == ObjectKind::Manifest {
                    Phase::ManifestWrite
                } else {
                    Phase::Write
                },
                bytes.len() as u64,
            );
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)?;
            file.write_all(bytes)?;
            context.check()?;
            {
                let _sync = self.counters.begin(Phase::FileSync, bytes.len() as u64);
                file.sync_all()?;
            }
            drop(file);
            crate::util::fault::trigger("content.install.flushed")?;
            crate::core::objects::install_object(&temp, &dest)?;
            crate::util::fault::trigger("content.install.installed")?;
            context.check()?;
            Ok(true)
        })();
        if result.is_err() {
            let _ = fs::remove_file(temp);
        }
        result
    }

    fn put_bytes(&self, bytes: &[u8], context: &ReadContext) -> Result<Digest> {
        let digest = {
            let _hash = self.counters.begin(Phase::Hash, bytes.len() as u64);
            Digest::bytes(bytes)
        };
        self.install_inner(
            ObjectKey {
                kind: ObjectKind::Bytes,
                digest,
            },
            bytes,
            context,
        )?;
        Ok(digest)
    }

    fn put_node(&self, node: &ManifestNode, context: &ReadContext) -> Result<Digest> {
        let digest = node.digest()?;
        self.install_inner(
            ObjectKey {
                kind: ObjectKind::Manifest,
                digest,
            },
            &node.encode()?,
            context,
        )?;
        Ok(digest)
    }

    /// Streaming construction uses at most one chunk plus one page per depth.
    /// `len` bytes are consumed exactly; trailing input belongs to the caller.
    pub fn import(
        &self,
        reader: &mut dyn Read,
        len: u64,
        context: &ReadContext,
    ) -> Result<FileContentRef> {
        context.check()?;
        ensure!(len <= MAX_FILE_SIZE, "FILE_TOO_LARGE");
        let _permit = self.permit(context, 3, BufferClass::Background)?;
        self.import_inner(reader, len, context)
    }
    /// Prepare current direct/manifest bytes before taking a metadata writer.
    pub fn prepare_import(
        &self,
        reader: &mut dyn Read,
        len: u64,
        context: &ReadContext,
    ) -> Result<PreparedContent> {
        let content = self.import(reader, len, context)?;
        Ok(PreparedContent {
            pin: self.pin(&content)?,
        })
    }
    fn import_inner(
        &self,
        reader: &mut dyn Read,
        len: u64,
        context: &ReadContext,
    ) -> Result<FileContentRef> {
        context.check()?;
        ensure!(len <= MAX_FILE_SIZE, "FILE_TOO_LARGE");
        if len <= CHUNK_SIZE {
            let mut bytes = vec![0; len as usize];
            reader.read_exact(&mut bytes)?;
            let digest = self.put_bytes(&bytes, context)?;
            return FileContentRef::new(ContentKind::Direct, digest, len);
        }
        let digest = self.import_node(reader, len, ManifestNode::root_level(len)?, context)?;
        FileContentRef::new(ContentKind::Manifest, digest, len)
    }

    fn import_node(
        &self,
        reader: &mut dyn Read,
        len: u64,
        level: u8,
        context: &ReadContext,
    ) -> Result<Digest> {
        context.check()?;
        let capacity = ManifestNode::child_capacity(level)?;
        let mut node = ManifestNode {
            level,
            total_len: len,
            children: Vec::new(),
        };
        let mut remaining = len;
        while remaining != 0 {
            let child_len = remaining.min(capacity);
            let (kind, digest) = if level == 0 {
                let mut bytes = vec![0; child_len as usize];
                reader.read_exact(&mut bytes)?;
                (ChildKind::Chunk, self.put_bytes(&bytes, context)?)
            } else {
                (
                    ChildKind::Node,
                    self.import_node(reader, child_len, level - 1, context)?,
                )
            };
            node.children.push(ManifestChild {
                kind,
                len: child_len,
                digest,
            });
            remaining -= child_len;
        }
        self.put_node(&node, context)
    }

    fn node(
        &self,
        digest: Digest,
        len: u64,
        level: u8,
        context: &ReadContext,
    ) -> Result<ManifestNode> {
        let key = ObjectKey {
            kind: ObjectKind::Manifest,
            digest,
        };
        let bytes = self.verified_blob(key, None, context)?;
        let node = ManifestNode::decode(&bytes)?;
        ensure!(
            node.total_len == len && node.level == level,
            ContentError::Corrupt(key)
        );
        Ok(node)
    }

    /// Verify exactly one parent before returning any child admission evidence.
    /// This local API confers no network authorization.
    pub fn verified_children(
        &self,
        edge: &ContentEdge,
        context: &ReadContext,
    ) -> Result<Vec<ContentEdge>> {
        context.check()?;
        ensure!(edge.key.kind == ObjectKind::Manifest, "EXPECTED_MANIFEST");
        let level = edge.level.context("EXPECTED_MANIFEST_LEVEL")?;
        ensure!(
            edge.offset
                .checked_add(edge.len)
                .is_some_and(|end| end <= MAX_FILE_SIZE),
            "MANIFEST_RANGE"
        );
        let _permit = self.permit(context, 1, BufferClass::Structural)?;
        let node = self.node(edge.key.digest, edge.len, level, context)?;
        let mut offset = edge.offset;
        let children = node
            .children
            .into_iter()
            .map(|child| {
                let result = ContentEdge {
                    key: ObjectKey {
                        kind: if level == 0 {
                            ObjectKind::Bytes
                        } else {
                            ObjectKind::Manifest
                        },
                        digest: child.digest,
                    },
                    offset,
                    len: child.len,
                    level: level.checked_sub(1),
                };
                offset += child.len; // node and containing range were validated
                result
            })
            .collect();
        context.check()?;
        Ok(children)
    }

    pub fn walk(&self, pin: &PinnedContent) -> Result<ContentWalk> {
        ensure!(pin.provider() == self.owner.id, "CONTENT_PROVIDER_MISMATCH");
        ContentWalk::new(pin)
    }

    /// A failure retains the same frontier for retry. The walk owns a root pin
    /// until dropped, including while a missing manifest prevents completion.
    pub fn walk_step(
        &self,
        walk: &mut ContentWalk,
        context: &ReadContext,
    ) -> Result<Option<WalkBatch>> {
        context.check()?;
        ensure!(
            walk.pin.provider() == self.owner.id,
            "CONTENT_PROVIDER_MISMATCH"
        );
        let Some(edge) = walk.frontier.last().cloned() else {
            return Ok(None);
        };
        let children = if edge.key.kind == ObjectKind::Manifest {
            self.verified_children(&edge, context)?
        } else {
            vec![]
        };
        context.check()?;
        walk.advance(&children)?;
        Ok(Some(WalkBatch {
            visited: edge,
            children,
            complete: walk.complete(),
        }))
    }

    fn read_node(
        &self,
        digest: Digest,
        len: u64,
        level: u8,
        offset: u64,
        out: &mut [u8],
        context: &ReadContext,
    ) -> Result<()> {
        let node = self.node(digest, len, level, context)?;
        let end = offset
            .checked_add(out.len() as u64)
            .context("RANGE_OVERFLOW")?;
        ensure!(end <= len, "RANGE_OUT_OF_BOUNDS");
        let mut start = 0;
        for child in node.children {
            let child_end = start + child.len; // validated exact coverage
            let from = offset.max(start);
            let until = end.min(child_end);
            if from < until {
                let target = &mut out[(from - offset) as usize..(until - offset) as usize];
                if level == 0 {
                    let bytes = self.verified_blob(
                        ObjectKey {
                            kind: ObjectKind::Bytes,
                            digest: child.digest,
                        },
                        Some(child.len),
                        context,
                    )?;
                    target
                        .copy_from_slice(&bytes[(from - start) as usize..(until - start) as usize]);
                } else {
                    self.read_node(
                        child.digest,
                        child.len,
                        level - 1,
                        from - start,
                        target,
                        context,
                    )?;
                }
            }
            start = child_end;
            if start >= end {
                break;
            }
        }
        Ok(())
    }

    /// Canonical rebuilding walks only changed paths and shape boundaries.
    /// Unchanged data can remain absent. Structural pages needed for reuse must
    /// be locally verified; absence never falls back to a whole-file copy.
    pub(crate) fn rebuild(
        &self,
        base: Option<&FileContentRef>,
        len: u64,
        source: &mut dyn EditSource,
        context: &ReadContext,
    ) -> Result<FileContentRef> {
        context.check()?;
        ensure!(len <= MAX_FILE_SIZE, "FILE_TOO_LARGE");
        let base_pin = base.map(|r| self.pin(r)).transpose()?;
        let mut cursor = BaseCursor {
            provider: self,
            root: base_pin.as_ref().map(ContentEdge::root).transpose()?,
            pages: (0..MAX_DEPTH).map(|_| None).collect(),
        };
        let _permit = self.permit_checked(context, 3, BufferClass::Background, &|| {
            source.check(context)
        })?;
        let mut zeros = BTreeMap::new();
        let level = if len <= CHUNK_SIZE {
            None
        } else {
            Some(ManifestNode::root_level(len)?)
        };
        let digest = self.rebuild_part(0, len, level, source, &mut cursor, &mut zeros, context)?;
        context.check()?;
        FileContentRef::new(
            if level.is_some() {
                ContentKind::Manifest
            } else {
                ContentKind::Direct
            },
            digest,
            len,
        )
    }
    /// Read-only mutation preflight. Existing inputs are verified, but no new
    /// content hashes, CAS installation or durability boundary are introduced.
    /// Errors leave this stack (and its permits) before
    /// the coordinator is allowed to wait for an immutable base object.
    pub(crate) fn preflight(
        &self,
        base: &FileContentRef,
        len: u64,
        source: &mut dyn EditSource,
        context: &ReadContext,
    ) -> Result<()> {
        context.check()?;
        ensure!(len <= MAX_FILE_SIZE, "FILE_TOO_LARGE");
        if len == 0 {
            return source.check(context);
        }
        let pin = self.pin(base)?;
        let mut cursor = BaseCursor {
            provider: self,
            root: Some(ContentEdge::root(&pin)?),
            pages: (0..MAX_DEPTH).map(|_| None).collect(),
        };
        let _permit = self.permit_checked(context, 3, BufferClass::Background, &|| {
            source.check(context)
        })?;
        let level = if len <= CHUNK_SIZE {
            None
        } else {
            Some(ManifestNode::root_level(len)?)
        };
        Self::preflight_part(0, len, level, source, &mut cursor, context)?;
        source.check(context)
    }
    fn preflight_part(
        offset: u64,
        len: u64,
        level: Option<u8>,
        source: &mut dyn EditSource,
        cursor: &mut BaseCursor<'_>,
        context: &ReadContext,
    ) -> Result<()> {
        context.check()?;
        match source.span(offset, len, context)? {
            EditSpan::Unchanged => {
                if let Some(edge) = cursor.find(offset, level, context)?
                    && edge.len == len
                {
                    return Ok(());
                }
            }
            EditSpan::Zero => return Ok(()),
            EditSpan::Changed => {}
        }
        if let Some(level) = level {
            let capacity = ManifestNode::child_capacity(level)?;
            let mut position = 0;
            while position < len {
                let child = capacity.min(len - position);
                Self::preflight_part(
                    offset + position,
                    child,
                    level.checked_sub(1),
                    source,
                    cursor,
                    context,
                )?;
                position += child;
            }
            Ok(())
        } else {
            let mut scratch = vec![0; len as usize];
            source
                .read(offset, &mut scratch, context)
                .map_err(|error| error.context(RebuildRange(ByteRange { offset, len })))
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn rebuild_part(
        &self,
        offset: u64,
        len: u64,
        level: Option<u8>,
        source: &mut dyn EditSource,
        cursor: &mut BaseCursor<'_>,
        zeros: &mut BTreeMap<(Option<u8>, u64), Digest>,
        context: &ReadContext,
    ) -> Result<Digest> {
        context.check()?;
        match source.span(offset, len, context)? {
            EditSpan::Unchanged => {
                if let Some(edge) = cursor.find(offset, level, context)?
                    && edge.len == len
                {
                    return Ok(edge.key.digest);
                }
            }
            EditSpan::Zero => return self.zero_part(len, level, zeros, context),
            EditSpan::Changed => {}
        }
        if let Some(level) = level {
            let capacity = ManifestNode::child_capacity(level)?;
            let mut node = ManifestNode {
                level,
                total_len: len,
                children: Vec::new(),
            };
            let mut position = 0;
            while position < len {
                context.check()?;
                let child_len = capacity.min(len - position);
                let digest = self.rebuild_part(
                    offset + position,
                    child_len,
                    level.checked_sub(1),
                    source,
                    cursor,
                    zeros,
                    context,
                )?;
                node.children.push(ManifestChild {
                    kind: if level == 0 {
                        ChildKind::Chunk
                    } else {
                        ChildKind::Node
                    },
                    len: child_len,
                    digest,
                });
                position += child_len;
            }
            self.put_node(&node, context)
        } else {
            let mut bytes = vec![0; len as usize];
            source.read(offset, &mut bytes, context)?;
            context.check()?;
            self.put_bytes(&bytes, context)
        }
    }
    fn zero_part(
        &self,
        len: u64,
        level: Option<u8>,
        cache: &mut BTreeMap<(Option<u8>, u64), Digest>,
        context: &ReadContext,
    ) -> Result<Digest> {
        context.check()?;
        if let Some(digest) = cache.get(&(level, len)) {
            return Ok(*digest);
        }
        let digest = if let Some(level) = level {
            let capacity = ManifestNode::child_capacity(level)?;
            let mut node = ManifestNode {
                level,
                total_len: len,
                children: Vec::new(),
            };
            let mut remaining = len;
            while remaining > 0 {
                context.check()?;
                let child_len = remaining.min(capacity);
                let digest = self.zero_part(child_len, level.checked_sub(1), cache, context)?;
                node.children.push(ManifestChild {
                    kind: if level == 0 {
                        ChildKind::Chunk
                    } else {
                        ChildKind::Node
                    },
                    len: child_len,
                    digest,
                });
                remaining -= child_len;
            }
            self.put_node(&node, context)?
        } else {
            self.put_bytes(&vec![0; len as usize], context)?
        };
        ensure!(
            cache.len() < 2 * MAX_DEPTH as usize + 2,
            "CONTENT_BUFFER_LIMIT"
        );
        cache.insert((level, len), digest);
        Ok(digest)
    }

    /// Constant-length aligned-chunk COW update. Unchanged subtrees retain their
    /// identities. A fully covered chunk requires no read of its old bytes.
    /// Resize is an explicit operation owned by write sessions.
    pub fn overwrite(
        &self,
        pin: &PinnedContent,
        offset: u64,
        bytes: &[u8],
        context: &ReadContext,
    ) -> Result<FileContentRef> {
        ensure!(pin.provider() == self.owner.id, "CONTENT_PROVIDER_MISMATCH");
        let base = pin.reference();
        context.validate(base, offset, bytes.len())?;
        if bytes.is_empty() {
            return Ok(base.clone());
        }
        let _permit = self.permit(context, 3, BufferClass::Background)?;
        let digest = if base.kind == ContentKind::Direct {
            let chunk = self.patch_chunk(base.digest, base.total_len, offset, bytes, context)?;
            chunk.digest
        } else {
            self.patch_node(
                base.digest,
                base.total_len,
                ManifestNode::root_level(base.total_len)?,
                offset,
                bytes,
                context,
            )?
        };
        FileContentRef::new(base.kind, digest, base.total_len)
    }

    fn patch_chunk(
        &self,
        digest: Digest,
        len: u64,
        offset: u64,
        bytes: &[u8],
        context: &ReadContext,
    ) -> Result<ManifestChild> {
        let digest = if offset == 0 && bytes.len() as u64 == len {
            self.put_bytes(bytes, context)?
        } else {
            let mut data = self.verified_blob(
                ObjectKey {
                    kind: ObjectKind::Bytes,
                    digest,
                },
                Some(len),
                context,
            )?;
            data[offset as usize..offset as usize + bytes.len()].copy_from_slice(bytes);
            self.put_bytes(&data, context)?
        };
        Ok(ManifestChild {
            kind: ChildKind::Chunk,
            len,
            digest,
        })
    }

    fn patch_node(
        &self,
        digest: Digest,
        len: u64,
        level: u8,
        offset: u64,
        bytes: &[u8],
        context: &ReadContext,
    ) -> Result<Digest> {
        let mut node = self.node(digest, len, level, context)?;
        let end = offset
            .checked_add(bytes.len() as u64)
            .context("RANGE_OVERFLOW")?;
        ensure!(end <= len, "RANGE_OUT_OF_BOUNDS");
        let mut start = 0;
        for child in &mut node.children {
            let child_end = start + child.len;
            let from = start.max(offset);
            let until = child_end.min(end);
            if from < until {
                let patch = &bytes[(from - offset) as usize..(until - offset) as usize];
                if level == 0 {
                    *child =
                        self.patch_chunk(child.digest, child.len, from - start, patch, context)?;
                } else {
                    child.digest = self.patch_node(
                        child.digest,
                        child.len,
                        level - 1,
                        from - start,
                        patch,
                        context,
                    )?;
                }
            }
            start = child_end;
            if start >= end {
                break;
            }
        }
        self.put_node(&node, context)
    }

    /// Captures only current explicit pins. This is not a complete reachability
    /// inventory: metadata histories/recovery records must be added by retention.
    pub fn pinned_roots(&self) -> (u64, Vec<FileContentRef>) {
        let state = lock(&self.owner.lifetimes);
        (
            state.generation,
            state.pins.values().map(|(root, _)| root.clone()).collect(),
        )
    }

    fn read_range_inner(
        &self,
        pin: &PinnedContent,
        offset: u64,
        out: &mut [u8],
        context: &ReadContext,
    ) -> Result<()> {
        let reference = pin.reference();
        if out.is_empty() {
            return context.check();
        }
        match reference.kind {
            ContentKind::Direct => {
                let bytes = self.verified_blob(
                    ObjectKey::root(reference),
                    Some(reference.total_len),
                    context,
                )?;
                out.copy_from_slice(&bytes[offset as usize..offset as usize + out.len()]);
            }
            ContentKind::Manifest => self.read_node(
                reference.digest,
                reference.total_len,
                ManifestNode::root_level(reference.total_len)?,
                offset,
                out,
                context,
            )?,
        }
        context.check()
    }
}

pub(crate) struct BufferPermit<'a> {
    owner: &'a Mutex<BufferState>,
    buffers: usize,
    class: BufferClass,
}
impl Drop for BufferPermit<'_> {
    fn drop(&mut self) {
        let mut state = lock(self.owner);
        match self.class {
            BufferClass::Structural => state.structural = false,
            BufferClass::Foreground => state.foreground -= self.buffers,
            BufferClass::Background => state.background -= self.buffers,
        }
    }
}

impl ContentProvider for LocalContent {
    fn pin(&self, reference: &FileContentRef) -> Result<PinnedContent> {
        let key = reference.encode()?;
        let mut state = lock(&self.owner.lifetimes);
        state
            .pins
            .entry(key)
            .or_insert_with(|| (reference.clone(), 0))
            .1 += 1;
        state.generation = state.generation.wrapping_add(1);
        drop(state);
        PinnedContent::new(
            reference.clone(),
            self.owner.id,
            Arc::new(PinLease {
                owner: self.owner.clone(),
                key,
            }),
        )
    }

    fn availability(
        &self,
        pin: &PinnedContent,
        range: ByteRange,
        context: &ReadContext,
    ) -> Result<Availability> {
        let len = usize::try_from(range.len).context("CONTENT_BUFFER_LIMIT")?;
        context.validate(pin.reference(), range.offset, len)?;
        ensure!(pin.provider() == self.owner.id, "CONTENT_PROVIDER_MISMATCH");
        let _permit = self.permit(context, 2, BufferClass::Foreground)?;
        let result = (|| -> Result<()> {
            if len == 0 {
                return context.check();
            }
            let mut offset = range.offset;
            let end = range.end()?;
            while offset < end {
                self.read_range_inner(pin, offset, &mut [0], context)?;
                offset = ((offset / CHUNK_SIZE + 1) * CHUNK_SIZE).min(end);
            }
            Ok(())
        })();
        match result {
            Ok(()) => Ok(Availability::Available),
            Err(error) => match error.downcast_ref::<ContentError>() {
                Some(ContentError::Missing(_)) => Ok(Availability::Missing),
                Some(ContentError::Corrupt(_)) => Ok(Availability::Corrupt),
                None => Err(error),
            },
        }
    }

    fn read_range(
        &self,
        pin: &PinnedContent,
        offset: u64,
        out: &mut [u8],
        context: &ReadContext,
    ) -> Result<()> {
        ensure!(pin.provider() == self.owner.id, "CONTENT_PROVIDER_MISMATCH");
        let reference = pin.reference();
        context.validate(reference, offset, out.len())?;
        if out.is_empty() {
            return context.check();
        }
        let _permit = self.permit(context, 2, BufferClass::Foreground)?;
        self.read_range_inner(pin, offset, out, context)
    }
}
