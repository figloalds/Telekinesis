//! Status model v2 (§7.1), schema 2. Frozen in Wave 0.
//!
//! [`StatusSnapshot`] is what `Action::Status` returns and what the GUI and
//! `tkfs manage status` render. [`StatusEvent`] is what producers record into
//! the StatusHub (`StatusSink::record`). Invariant 30: `metadata.caught_up`
//! ([`MetaProgress::caught_up`]), `content.complete`
//! ([`ContentCounters::complete`]) and the outbox acks
//! ([`JournalCounters::outgoing_unacknowledged`]) are separate fields.
use crate::model::{
    errors::{ErrorRecord, ErrorWire, Scope},
    health::{
        ActivationCounters, ContentCounters, InboxCounts, JournalCounters, MountStatus, StoreHealth,
    },
    ipc::WorkerHeartbeat,
    orchestrator::{FolderView, PublishedEntry},
    protocol::feed::ExchangeSummary,
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};

pub use crate::model::protocol::LaneRole;

pub const STATUS_SCHEMA: u32 = 2;

/// Status timing and display policy (§3.4, §7.1).
pub mod policy {
    use std::time::Duration;
    /// Stall: work remains, the peer is connected, no progress for
    /// `max(STALL_MIN, STALL_INTERVALS × sync_interval)` (`TKFS_STALL_AFTER_MS` overrides).
    pub const STALL_MIN: Duration = Duration::from_secs(60);
    pub const STALL_INTERVALS: u32 = 20;
    /// Contention errors are hidden until they have lasted this long.
    pub const CONTENTION_VISIBLE_AFTER: Duration = Duration::from_secs(30);
    /// Transient errors are shown from this attempt on.
    pub const TRANSIENT_VISIBLE_ATTEMPTS: u64 = 3;
    /// Error history entries kept (with `resolved_at_ms`).
    pub const ERROR_HISTORY: usize = 50;
    /// Activity ring entries.
    pub const ACTIVITY_RING: usize = 500;
    /// The snapshot is rebuilt lazily at most this often.
    pub const SNAPSHOT_MIN_INTERVAL: Duration = Duration::from_millis(250);
    /// Hub tick (rates, ETA, stall detection).
    pub const TICK: Duration = Duration::from_secs(1);
    /// EWMA windows for rates; ETA uses the short one.
    pub const RATE_SHORT: Duration = Duration::from_secs(10);
    pub const RATE_LONG: Duration = Duration::from_secs(60);
    /// `Subscribe.wait_ms` upper bound.
    pub const SUBSCRIBE_MAX_WAIT_MS: u32 = 25_000;
}

/// The stall threshold for a sync interval (without the env override).
pub fn stall_after(sync_interval: Duration) -> Duration {
    policy::STALL_MIN.max(sync_interval.saturating_mul(policy::STALL_INTERVALS))
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusSnapshot {
    pub schema: u32,
    pub generation: u64,
    pub at_ms: u64,
    pub daemon: DaemonStatus,
    pub folders: Vec<FolderStatus>,
    pub peers: Vec<PeerStatus>,
    pub activity: Vec<ActivityEntry>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonStatus {
    pub installation: String,
    pub version: String,
    pub pid: u32,
    pub started_at_ms: u64,
    pub stopping: bool,
    pub control: String,
    #[serde(default)]
    pub network: Option<String>,
    #[serde(default)]
    pub log_path: Option<String>,
    #[serde(default)]
    pub errors: Vec<ErrorRecord>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FolderStatus {
    pub state: String,
    pub label: String,
    pub repo: String,
    pub device: String,
    pub lifecycle: LifecycleStatus,
    pub intents: Intents,
    pub mount: MountStatus,
    pub gate: GateStatus,
    pub store: StoreStatus,
    pub activation: ActivationStatus,
    pub content: ContentCounters,
    pub journal: JournalCounters,
    pub sync: FolderSync,
    #[serde(default)]
    pub errors: Vec<ErrorRecord>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecyclePhase {
    #[default]
    Stopped,
    Starting,
    Running,
    StopBlocked,
    Stopping,
    Backoff,
    Failed,
}

/// Startup progress as reported by `startup_progress` lines.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartupProgress {
    pub phase: String,
    pub seq: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleStatus {
    pub phase: LifecyclePhase,
    #[serde(default)]
    pub detail: Option<String>,
    pub since_ms: u64,
    #[serde(default)]
    pub pid: Option<u32>,
    #[serde(default)]
    pub instance: Option<String>,
    pub adopted: bool,
    pub restarts: u32,
    #[serde(default)]
    pub next_retry_at_ms: Option<u64>,
    #[serde(default)]
    pub last_exit: Option<crate::model::orchestrator::ExitRecord>,
    pub degraded: bool,
    #[serde(default)]
    pub startup: Option<StartupProgress>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Intents {
    pub desired_running: bool,
    pub sync_enabled: bool,
    pub mount_enabled: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateKind {
    #[default]
    None,
    Awaiting,
    Blocked,
    Satisfied,
    Overridden,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateStatus {
    pub state: GateKind,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub since_ms: Option<u64>,
    #[serde(default)]
    pub waiting_on: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreStatus {
    pub health: StoreHealth,
    pub pending_saves: u32,
    pub unflushed_files: u32,
    pub open_handles: u32,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivationStatus {
    pub inbox: InboxCounts,
    pub counters: ActivationCounters,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FolderSync {
    pub state: FolderSyncState,
    /// Human summary, e.g. "2 of 3 peers".
    pub summary: String,
    pub peers: Vec<FolderPeerSync>,
}

/// Folder aggregate (§7.1 precedence table; first match wins in `hub_rules`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FolderSyncState {
    #[default]
    Stopped,
    NotShared,
    Paused,
    Offline,
    InitialSync,
    Metadata,
    Downloading,
    Uploading,
    Syncing,
    Blocked,
    Backoff,
    Stalled,
    NeedsAttention,
    UpToDate,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    #[default]
    Outbound,
    Inbound,
    Both,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FolderPeerSync {
    pub installation: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
    pub direction: Direction,
    pub state: FolderPeerState,
    pub metadata: MetaProgress,
    pub objects_down: TransferProgress,
    pub objects_up: TransferProgress,
    #[serde(default)]
    pub last_progress_ms: Option<u64>,
    #[serde(default)]
    pub stalled_since_ms: Option<u64>,
    #[serde(default)]
    pub backoff_until_ms: Option<u64>,
    #[serde(default)]
    pub error: Option<ErrorRecord>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FolderPeerState {
    #[default]
    NotShared,
    Offline,
    Connecting,
    Incompatible,
    Unauthorized,
    WaitingForWorker,
    WaitingForRemote,
    Paused,
    Metadata,
    Downloading,
    Uploading,
    Syncing,
    Blocked,
    Backoff,
    Stalled,
    UpToDate,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaProgress {
    pub to_send: u64,
    pub to_receive: u64,
    pub local_pending: u64,
    pub remote_pending: u64,
    pub blocked: u64,
    pub quarantined: u64,
    pub rejected: u64,
    pub caught_up: bool,
    pub cursor_seq: i64,
    pub remote_tip: i64,
    #[serde(default)]
    pub last_exchange_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferProgress {
    pub objects_remaining: u64,
    pub bytes_remaining: u64,
    pub in_flight: u32,
    pub rate_bps: u64,
    #[serde(default)]
    pub eta_s: Option<u64>,
    pub total_bytes: u64,
    #[serde(default)]
    pub last_progress_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerConnState {
    Online,
    Connecting,
    #[default]
    Offline,
    Backoff,
    Incompatible,
    Unauthorized,
    Revoked,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerStatus {
    pub installation: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
    pub direction: Direction,
    pub state: PeerConnState,
    /// Negotiated protocol, e.g. `"tkfs-peer/3"`.
    #[serde(default)]
    pub protocol: Option<String>,
    #[serde(default)]
    pub since_ms: Option<u64>,
    #[serde(default)]
    pub rtt_ms: Option<u64>,
    pub rate_up_bps: u64,
    pub rate_down_bps: u64,
    pub lanes: Vec<LaneStatus>,
    pub inbound_sessions: u32,
    pub shared_folders: Vec<String>,
    #[serde(default)]
    pub error: Option<ErrorRecord>,
}

/// Lane state machine (§3.7); details live in the sibling fields.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LaneState {
    #[default]
    Idle,
    Dialing,
    Handshaking,
    Ready,
    Backoff,
    Incompatible,
    Unauthorized,
    Closed,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InflightOp {
    pub op: String,
    pub repo: String,
    pub since_ms: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaneStatus {
    pub lane: u32,
    pub role: LaneRole,
    pub direction: Direction,
    pub state: LaneState,
    #[serde(default)]
    pub since_ms: Option<u64>,
    #[serde(default)]
    pub inflight: Vec<InflightOp>,
    pub bytes_up: u64,
    pub bytes_down: u64,
    pub rate_up_bps: u64,
    pub rate_down_bps: u64,
    pub reconnects: u32,
    #[serde(default)]
    pub backoff_until_ms: Option<u64>,
    #[serde(default)]
    pub error: Option<ErrorRecord>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityEntry {
    pub at_ms: u64,
    pub kind: String,
    #[serde(default)]
    pub folder: Option<String>,
    #[serde(default)]
    pub peer: Option<String>,
    pub message: String,
}

/// Everything a producer can tell the StatusHub. `record()` is in-memory only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum StatusEvent {
    FolderLifecycle(Box<FolderView>),
    WorkerHeartbeat {
        state: String,
        hb: Arc<WorkerHeartbeat>,
    },
    FolderRemoved {
        state: String,
    },
    PeerConfigured {
        installation: String,
        endpoint: String,
    },
    PeerRemoved {
        installation: String,
    },
    LaneState {
        installation: String,
        lane: u32,
        role: LaneRole,
        direction: Direction,
        state: LaneState,
        #[serde(default)]
        error: Option<ErrorWire>,
        #[serde(default)]
        backoff_until_ms: Option<u64>,
        #[serde(default)]
        rtt_ms: Option<u64>,
    },
    ExchangeStarted {
        installation: String,
        repo: String,
        lane: u32,
        op: String,
        at_ms: u64,
    },
    ExchangeFinished {
        installation: String,
        repo: String,
        lane: u32,
        op: String,
        direction: Direction,
        ok: bool,
        #[serde(default)]
        error: Option<ErrorWire>,
        #[serde(default)]
        summary: Option<ExchangeSummary>,
        bytes_up: u64,
        bytes_down: u64,
        objects_up: u64,
        objects_down: u64,
        duration_ms: u64,
    },
    PeerCatalog {
        installation: String,
        entries: Vec<PublishedEntry>,
    },
    InboundSession {
        installation: String,
        lane: u32,
        role: LaneRole,
        opened: bool,
    },
    InboundIncompatible {
        address: String,
        detail: String,
    },
    ObjectError {
        state: String,
        hash: String,
        error: ErrorWire,
    },
    /// Pre-cutover passthrough of legacy `LinkStatus` JSON (Wave 2).
    LegacyLink {
        key: String,
        value: serde_json::Value,
    },
    Error {
        scope: Scope,
        error: ErrorWire,
    },
    Resolved {
        scope: Scope,
        code: String,
    },
    Activity(ActivityEntry),
}
