//! Wave 6 coordination contracts. These are not added to legacy wire messages.
use super::content::FileContentRef;
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Stable identity of one selected mutable view; a filesystem path is not identity.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceId {
    pub repository: String,
    pub workspace: Uuid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ByteRange {
    pub offset: u64,
    pub len: u64,
}

impl ByteRange {
    pub fn end(self) -> Result<u64> {
        self.offset
            .checked_add(self.len)
            .ok_or_else(|| anyhow::anyhow!("RANGE_OVERFLOW"))
    }

    pub fn validate(self, total_len: u64) -> Result<()> {
        ensure!(self.end()? <= total_len, "RANGE_OUT_OF_BOUNDS");
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileRevision {
    pub workspace: WorkspaceId,
    pub entity: String,
    pub generation: u64,
    pub content: FileContentRef,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionAction {
    Grant,
    Withdraw,
}

/// A cursor is repository-local and durable. Reference and generation scope
/// grants independently of content deduplication.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionChange {
    pub cursor: u64,
    pub repository: String,
    pub branch: String,
    pub reference: String,
    pub root: FileContentRef,
    pub generation: u64,
    pub action: AdmissionAction,
    pub shared: bool,
    /// Explicit local authorship/publication. Incoming references alone do not
    /// authorize serving an identically hashed private local copy.
    pub publishable: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DemandPriority {
    Structural,
    Foreground,
    Background,
}

/// Serializable request identity; deadlines/cancellation are owned locally by
/// each waiter, not serialized as process-relative monotonic clock values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Demand {
    pub waiter: Uuid,
    pub revision: FileRevision,
    pub reference: String,
    pub range: ByteRange,
    pub priority: DemandPriority,
    pub eligible_peers: Vec<String>,
}

impl Demand {
    pub fn validate(&self) -> Result<()> {
        self.revision.content.validate()?;
        self.range.validate(self.revision.content.total_len)?;
        ensure!(
            !self.reference.is_empty()
                && self.reference.len() <= 128
                && !self.revision.entity.is_empty()
                && self.revision.entity.len() <= 128
                && !self.revision.workspace.repository.is_empty()
                && self.revision.workspace.repository.len() <= 128
                && self.eligible_peers.len() <= 256
                && self.eligible_peers.iter().all(|peer| !peer.is_empty()
                    && peer.len() <= 128
                    && !peer.chars().any(char::is_control)),
            "INVALID_DEMAND_SCOPE"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeCursor {
    pub workspace: WorkspaceId,
    pub epoch: Uuid,
    pub sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceChange {
    pub cursor: ChangeCursor,
    pub generation: u64,
    pub entries: Vec<String>,
    pub entities: Vec<String>,
    pub directories: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum ChangePage {
    Changes {
        changes: Vec<WorkspaceChange>,
        next: ChangeCursor,
    },
    RescanRequired {
        current: ChangeCursor,
    },
}

/// Explicit, independently visible local save states. Peer availability is
/// separate: committing never waits for a peer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SaveState {
    Buffered,
    RecoverablePending,
    Committed,
}

/// Default limits are counted in bytes as well as jobs. Implementations must
/// charge before allocation/queuing and return generation-owned permits.
pub mod limits {
    pub const DIRTY_BYTES: usize = 64 * 1024 * 1024;
    pub const SAVE_WORKERS: usize = 4;
    pub const SAVE_QUEUE: usize = 64;
    pub const VERIFY_BUFFERS: usize = 8;
    pub const READ_BYTES: usize = 1024 * 1024;
    pub const EXPANSION_EDGES: usize = 256;
    pub const SQL_QUEUE: usize = 128;
    pub const SQL_RESERVED: usize = 16;
    pub const DEMAND_WAITERS: usize = 64;
    pub const TRANSFER_JOBS: usize = 8;
    pub const PEER_JOBS: usize = 4;
    pub const REPOSITORY_JOBS: usize = 4;
    pub const JOURNAL_RECORDS: usize = 65_536;
    pub const CANCEL_POLL_MS: u64 = 50;
    pub const SHUTDOWN_DRAIN_MS: u64 = 5_000;
}
