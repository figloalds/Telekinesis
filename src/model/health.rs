//! Worker-side health and counter types (§4.3, §4.4, §4.5). Frozen in Wave 0.
//!
//! These are produced by the worker (heartbeat, status cell, `mount-*` RPCs)
//! and consumed by the orchestrator (FolderController, StatusHub).
use crate::model::errors::ErrorWire;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// The worker's mount, driven by the MountController (WP1.5).
///
/// `status()` is lock-free: it reads a published snapshot and never waits for
/// an attach or detach in progress.
pub trait MountControl: Send + Sync {
    fn attach(&self, path: &Path) -> Result<MountStatus, ErrorWire>;
    fn detach(&self, force: bool) -> Result<MountStatus, ErrorWire>;
    fn status(&self) -> MountStatus;
}

/// What the controller wants the mount to be.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MountIntent {
    /// No mount requested (headless folder, or no mount path configured).
    #[default]
    Headless,
    /// A mount at `path` is requested.
    Attach,
    /// The mount was explicitly detached (user Detach, stop in progress).
    Detach,
}

/// Mount state machine (§4.3). Serialized flattened into [`MountStatus`], so
/// a status reads `{"intent":…, "path":…, "state":"attach_failed", "error":…}`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum MountState {
    /// Mounting is not available or not enabled for this worker.
    #[default]
    Disabled,
    Detached,
    Preflight,
    Attaching,
    Attached {
        since_ms: u64,
    },
    Detaching,
    AttachFailed {
        error: ErrorWire,
        attempt: u32,
        #[serde(default)]
        next_retry_at_ms: Option<u64>,
    },
    /// The mount went away underneath us (dispatch death, failed remount).
    Lost {
        error: ErrorWire,
    },
}

impl MountState {
    /// The snake_case name of the state (`"attach_failed"`, …).
    pub fn name(&self) -> &'static str {
        match self {
            MountState::Disabled => "disabled",
            MountState::Detached => "detached",
            MountState::Preflight => "preflight",
            MountState::Attaching => "attaching",
            MountState::Attached { .. } => "attached",
            MountState::Detaching => "detaching",
            MountState::AttachFailed { .. } => "attach_failed",
            MountState::Lost { .. } => "lost",
        }
    }

    pub fn is_attached(&self) -> bool {
        matches!(self, MountState::Attached { .. })
    }

    pub fn error(&self) -> Option<&ErrorWire> {
        match self {
            MountState::AttachFailed { error, .. } | MountState::Lost { error } => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MountStatus {
    pub intent: MountIntent,
    #[serde(default)]
    pub path: Option<PathBuf>,
    #[serde(flatten)]
    pub state: MountState,
}

/// Store health. Mount errors never write it (§4.3).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum StoreHealth {
    #[default]
    Healthy,
    /// Usable, but something needs attention (for example `RECOVERY_REQUIRED`
    /// for a pending save that could not be restored).
    Degraded { error: ErrorWire },
    /// Not usable for writes.
    Failed { error: ErrorWire },
}

impl StoreHealth {
    pub fn is_healthy(&self) -> bool {
        matches!(self, StoreHealth::Healthy)
    }

    pub fn error(&self) -> Option<&ErrorWire> {
        match self {
            StoreHealth::Healthy => None,
            StoreHealth::Degraded { error } | StoreHealth::Failed { error } => Some(error),
        }
    }
}

/// Inbox rows by state (§4.4). `pending_active` excludes quarantined and
/// blocked rows, which are reported separately.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboxCounts {
    pub ready: u64,
    pub waiting: u64,
    pub retry: u64,
    pub blocked: u64,
    pub quarantined: u64,
    /// Legacy rows (`state IS NULL`) not yet classified.
    pub unclassified: u64,
}

impl InboxCounts {
    /// `ready + waiting + retry + unclassified`.
    pub fn pending_active(&self) -> u64 {
        self.ready
            .saturating_add(self.waiting)
            .saturating_add(self.retry)
            .saturating_add(self.unclassified)
    }

    /// Every staged row in any state.
    pub fn total(&self) -> u64 {
        self.pending_active()
            .saturating_add(self.blocked)
            .saturating_add(self.quarantined)
    }
}

/// Background activator counters (§4.4, WP2.3).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivationCounters {
    /// Whether a background activator runs in this worker.
    pub running: bool,
    pub activated: u64,
    pub fast: u64,
    pub slow: u64,
    pub quarantined: u64,
    pub retried: u64,
    pub stale: u64,
    pub slices: u64,
    /// Duration of the last slice.
    pub last_slice_ms: u64,
    #[serde(default)]
    pub last_activation_at_ms: Option<u64>,
    /// Set while the activator cannot make progress (slow path starved).
    #[serde(default)]
    pub starved_since_ms: Option<u64>,
}

/// Local content completeness (the `content.complete` claim of invariant 30).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentCounters {
    pub current_objects: u64,
    pub current_available: u64,
    pub current_missing: u64,
    pub current_missing_bytes: u64,
    pub shared_objects: u64,
    pub shared_available: u64,
    pub shared_missing_bytes: u64,
    /// Objects with a partial spool.
    pub downloading: u64,
    pub partial_bytes: u64,
    /// Objects in the local demand queue.
    pub requested: u64,
    /// Every object referenced by shared events is available.
    pub complete: bool,
}

/// Journal counters; `outgoing_unacknowledged` is the remote-backup claim of
/// invariant 30, separate from metadata and content.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalCounters {
    pub events: u64,
    pub shared_events: u64,
    pub branches: u64,
    /// `journal_sequence` tip (0 for an empty journal).
    pub tip: i64,
    /// Feed epoch (`journal_epoch`); `None` before WP2.5.
    #[serde(default)]
    pub epoch: Option<String>,
    pub outgoing_unacknowledged: u64,
    pub unresolved_conflicts: u64,
}

/// Change counters the orchestrator watches instead of polling.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Generations {
    /// Bumped after activation slices and feed exchanges (drives the MetaScheduler).
    pub sync: u64,
    /// Bumped when the branch list or a shared branch descriptor changes
    /// (drives `published-summary`).
    pub branches: u64,
    /// Bumped when local content availability changes.
    pub content: u64,
    /// Bumped on every mount state change.
    pub mount: u64,
}

/// Engine-lock gauges, maintained from atomics by `lock_engine` (never by
/// taking the lock).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineGauges {
    /// Threads currently waiting for the engine lock.
    pub waiters: u32,
    /// Completed engine-lock holds.
    pub holds: u64,
    pub wait_p99_ms: u64,
    pub longest_hold_ms: u64,
    /// Age of the hold in progress (0 when unlocked).
    pub current_hold_ms: u64,
    /// The current hold has lasted longer than `ENGINE_STALLED_AFTER` (120 s).
    pub stalled: bool,
}

/// Per-peer metadata feed counters (§6.3, WP2.5), keyed by the peer replica.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerFeedCounters {
    pub replica: String,
    /// Our cursor of the peer's feed (`feed_in(peer).seq`).
    pub cursor_seq: i64,
    /// The peer's reported tip.
    pub remote_tip: i64,
    pub to_receive: u64,
    pub to_send: u64,
    /// Staged rows sourced from this peer that are not yet activated.
    pub staged_pending: u64,
    pub blocked: u64,
    pub quarantined: u64,
    pub rejected: u64,
    pub caught_up: bool,
    #[serde(default)]
    pub last_exchange_ms: Option<u64>,
}
