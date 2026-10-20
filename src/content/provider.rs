//! Provider-independent immutable ranges and lifetimes.
use crate::{
    model::{
        content::FileContentRef,
        workspace::{ByteRange, WorkspaceId, limits},
    },
    util::Cancel,
};
use anyhow::{Result, ensure};
use std::{collections::BTreeMap, sync::Arc, time::Instant};
use uuid::Uuid;

pub struct ReadContext {
    pub deadline: Instant,
    pub cancel: Cancel,
}

impl ReadContext {
    pub fn check(&self) -> Result<()> {
        ensure!(!self.cancel.is_cancelled(), "CONTENT_CANCELLED");
        ensure!(Instant::now() < self.deadline, "CONTENT_DEADLINE");
        Ok(())
    }

    pub fn validate(&self, reference: &FileContentRef, offset: u64, len: usize) -> Result<()> {
        self.check()?;
        reference.validate()?;
        ensure!(len <= limits::READ_BYTES, "CONTENT_BUFFER_LIMIT");
        ByteRange {
            offset,
            len: len as u64,
        }
        .validate(reference.total_len)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Availability {
    Available,
    Missing,
    Corrupt,
}

/// Owns an immutable version and a provider-specific lease. Clones share that
/// lease. The provider must keep its reachability pin until the last clone drops.
#[derive(Clone)]
pub struct PinnedContent {
    reference: FileContentRef,
    provider: Uuid,
    _lease: Arc<dyn Send + Sync>,
}

impl PinnedContent {
    /// For provider implementors. `lease` must protect this reference's lifetime.
    pub fn new(
        reference: FileContentRef,
        provider: Uuid,
        lease: Arc<dyn Send + Sync>,
    ) -> Result<Self> {
        reference.validate()?;
        Ok(Self {
            reference,
            provider,
            _lease: lease,
        })
    }

    pub fn reference(&self) -> &FileContentRef {
        &self.reference
    }

    pub fn provider(&self) -> Uuid {
        self.provider
    }
}

pub trait ContentProvider: Send + Sync {
    fn pin(&self, reference: &FileContentRef) -> Result<PinnedContent>;

    /// Verify the requested range. No whole-file availability requirement.
    fn availability(
        &self,
        pin: &PinnedContent,
        range: ByteRange,
        context: &ReadContext,
    ) -> Result<Availability>;

    /// Fill precisely `out.len()` bytes or fail. Unrelated missing chunks must
    /// not block a verified range. Callers must discard the buffer on error.
    fn read_range(
        &self,
        pin: &PinnedContent,
        offset: u64,
        out: &mut [u8],
        context: &ReadContext,
    ) -> Result<()>;
}

/// Internal immutable selected view, independent of startup snapshot files.
/// Pins have already been acquired by the caller, so construction performs no I/O.
#[derive(Clone)]
pub struct SnapshotRootHandle {
    workspace: WorkspaceId,
    generation: u64,
    entries: Arc<BTreeMap<String, PinnedContent>>,
}

impl SnapshotRootHandle {
    pub fn new(
        workspace: WorkspaceId,
        generation: u64,
        entries: BTreeMap<String, PinnedContent>,
    ) -> Self {
        Self {
            workspace,
            generation,
            entries: Arc::new(entries),
        }
    }

    pub fn workspace(&self) -> &WorkspaceId {
        &self.workspace
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn get(&self, entity: &str) -> Option<&PinnedContent> {
        self.entries.get(entity)
    }
    pub fn entries(&self) -> &BTreeMap<String, PinnedContent> {
        &self.entries
    }
}
