//! Orchestrator seams (§3.1, §3.3, WP0.4 interfaces). Frozen in Wave 0.
//!
//! Every orchestrator component receives `Arc<Ctx>` and talks to the others
//! only through the traits below, so packages can be built and tested
//! standalone against fakes and wired later (WP2.1, WP3.1).
use crate::{
    core::Branch,
    model::{
        errors::{ErrorClass, ErrorRecord, ErrorWire, classify, code_in},
        health::{ContentCounters, MountStatus},
        ipc::{FatalLine, StartupLine, StopMode, WorkerHeartbeat, WorkerRecord},
        status::{StatusEvent, StatusSnapshot},
    },
    pairing_code::Verifier,
    util::{cancel::Cancel, snapshot::Snapshot, time::Clock},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fmt,
    path::PathBuf,
    sync::{Arc, OnceLock, atomic::AtomicBool},
    time::Duration,
};

/// Orchestrator lifecycle, scheduling and server limits (§3).
pub mod limits {
    use std::time::Duration;
    /// A Starting worker with no `startup_progress` for this long is killed.
    pub const STARTUP_SILENCE: Duration = Duration::from_secs(30);
    pub const STOP_GRACEFUL: Duration = Duration::from_secs(5);
    pub const STOP_EXIT_WAIT: Duration = Duration::from_secs(10);
    /// StopBlocked retries the graceful stop this often.
    pub const STOP_BLOCKED_RETRY: Duration = Duration::from_secs(2);
    pub const RESTART_BACKOFF_BASE: Duration = Duration::from_secs(1);
    pub const RESTART_BACKOFF_CAP: Duration = Duration::from_secs(300);
    pub const RESTART_BACKOFF_RESET_AFTER: Duration = Duration::from_secs(600);
    pub const BACKOFF_JITTER: f64 = 0.2;
    /// Circuit breaker: this many failures within the window → `CRASH_LOOP`.
    pub const BREAKER_FAILURES: u32 = 5;
    pub const BREAKER_WINDOW: Duration = Duration::from_secs(600);
    /// No heartbeat for this long (or 3 failed watches) → `hello` probe.
    pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(8);
    pub const WATCH_FAILURES: u32 = 3;
    pub const HELLO_TIMEOUT: Duration = Duration::from_secs(2);
    /// Adoption: alive but silent for this long → `PREVIOUS_WORKER_UNRESPONSIVE`.
    pub const ADOPT_PATIENCE: Duration = Duration::from_secs(30);
    /// Adoption with a version mismatch waits this long for `owner.lock`.
    pub const ADOPT_LOCK_WAIT: Duration = Duration::from_secs(10);
    /// Adopted workers are probed for liveness this often.
    pub const ADOPTED_PID_PROBE: Duration = Duration::from_secs(1);
    pub const ATTACH_DEADLINE: Duration = Duration::from_secs(60);
    pub const MOUNT_RETRY_BASE: Duration = Duration::from_secs(2);
    pub const MOUNT_RETRY_CAP: Duration = Duration::from_secs(120);
    /// An `unavailable` published entry is refreshed this often.
    pub const PUBLISHED_RETRY: Duration = Duration::from_secs(60);
    /// Management `Stop` waits this long for `Stopped|StopBlocked`.
    pub const API_STOP_WAIT: Duration = Duration::from_secs(15);
    /// Shutdown budget for folders / network / control threads.
    pub const SHUTDOWN_FOLDERS: Duration = Duration::from_secs(30);
    pub const SHUTDOWN_NETWORK: Duration = Duration::from_secs(5);
    pub const SHUTDOWN_CONTROL_JOIN: Duration = Duration::from_secs(2);
    /// Peer supervisors join their lanes within this on unpair or respawn.
    pub const PEER_JOIN: Duration = Duration::from_secs(5);
    /// Concurrent control connections.
    pub const MAX_CONTROL_CONNECTIONS: usize = 64;
    /// Inbound: concurrent pre-auth handshakes and sessions.
    pub const MAX_PREAUTH: usize = 32;
    pub const MAX_SESSIONS: usize = 128;
    /// Inbound handler pools: threads per control / bulk session, queue depth.
    pub const CONTROL_HANDLERS: usize = 4;
    pub const BULK_HANDLERS: usize = 2;
    pub const HANDLER_QUEUE: usize = 16;
    /// Older sessions receive `GOAWAY{PAIRING_CODE_CHANGED}` within this.
    pub const ROTATION_GOAWAY: Duration = Duration::from_secs(1);
    /// Lane reconnect backoff and its healthy reset.
    pub const LANE_BACKOFF_BASE: Duration = Duration::from_secs(1);
    pub const LANE_BACKOFF_CAP: Duration = Duration::from_secs(60);
    pub const LANE_RESET_AFTER: Duration = Duration::from_secs(30);
    pub const INCOMPATIBLE_RECHECK: Duration = Duration::from_secs(600);
    pub const UNAUTHORIZED_RECHECK: Duration = Duration::from_secs(60);
    /// Remote catalog polling on lane 0.
    pub const CATALOG_POLL: Duration = Duration::from_secs(2);
    /// MetaScheduler slots.
    pub const EXCHANGE_SLOTS: usize = 4;
    pub const EXCHANGES_PER_REPO: usize = 1;
    pub const EXCHANGE_DEADLINE: Duration = Duration::from_secs(30);
    /// Legacy gate rule: `metadata_caught_up` within this window (Wave 2;
    /// unused since WP3.1 deleted the legacy rule with the legacy bridge).
    pub const LEGACY_GATE_FRESHNESS: Duration = Duration::from_secs(15);
    /// Pool size is `max(connections, MIN_POOL)`; lane 0 is control.
    pub const MIN_POOL: u32 = 2;
}

/// Shared context every orchestrator component receives (§3.1).
pub struct Ctx {
    pub installation: String,
    pub config: Arc<crate::orchestrator::Config>,
    pub catalog: Arc<dyn CatalogApi>,
    pub folders: Arc<dyn FolderDirectory>,
    pub published: Arc<dyn PublishedCatalogApi>,
    pub status: Arc<dyn StatusSink>,
    pub status_source: Arc<dyn StatusSource>,
    /// Legacy implementation (Wave 2) → v3 implementation (Wave 3).
    pub network: OnceLock<Arc<dyn NetworkApi>>,
    /// Legacy `LinkStatus` (Wave 2) → hub projection (Wave 3).
    pub legacy: OnceLock<Arc<dyn LegacyNetworkView>>,
    pub clock: Arc<dyn Clock>,
    pub shutdown: Cancel,
    pub stopping: Arc<AtomicBool>,
    pub started_at_ms: u64,
    /// Control/network URLs and the log path.
    pub endpoints: Snapshot<DaemonEndpoints>,
    /// The daemon's private parts beside this frozen context (CCR-WP2.1-1,
    /// applied by WP3.1): `orchestrator::ctx::extras` keeps its `Extras`
    /// here (the concrete hub, the shutdown latch). A context built by a
    /// test leaves it empty (`OnceLock::new()`); the first `extras` call
    /// fills it.
    pub extensions: OnceLock<Arc<dyn std::any::Any + Send + Sync>>,
}

impl Ctx {
    /// The network implementation, or `NETWORK_NOT_READY` before it is set.
    pub fn network(&self) -> Result<&Arc<dyn NetworkApi>, ErrorWire> {
        self.network.get().ok_or_else(network_not_ready)
    }

    /// The legacy network view, or `NETWORK_NOT_READY` before it is set.
    pub fn legacy(&self) -> Result<&Arc<dyn LegacyNetworkView>, ErrorWire> {
        self.legacy.get().ok_or_else(network_not_ready)
    }
}

fn network_not_ready() -> ErrorWire {
    ErrorWire::new(
        "NETWORK_NOT_READY",
        "NETWORK_NOT_READY: the network stack has not started yet",
    )
}

impl fmt::Debug for Ctx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ctx")
            .field("installation", &self.installation)
            .field("network_ready", &self.network.get().is_some())
            .field("legacy_ready", &self.legacy.get().is_some())
            .field("shutdown", &self.shutdown)
            .field("started_at_ms", &self.started_at_ms)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Catalog

/// The single owner of `folders.sqlite` (WP1.1). `snapshot()` is O(1) and
/// never waits for a command in progress.
pub trait CatalogApi: Send + Sync {
    fn snapshot(&self) -> Arc<CatalogSnapshot>;
    /// Runs one command in one transaction; returns the snapshot that includes it.
    fn apply(&self, cmd: CatalogCmd) -> Result<Arc<CatalogSnapshot>, ErrorWire>;
    /// Blocks until the generation exceeds `generation` or `timeout` elapses.
    fn wait_newer(&self, generation: u64, timeout: Duration) -> Arc<CatalogSnapshot>;
}

/// An immutable catalog view. Debug output never shows the verifier or codes.
#[derive(Clone, Default)]
pub struct CatalogSnapshot {
    pub generation: u64,
    pub installation: String,
    pub folders: Vec<FolderRecord>,
    pub peers: Vec<PeerRecord>,
    /// Salted pairing verifier; HMAC-key-equivalent to the code (§11.15).
    pub verifier: Option<Verifier>,
    pub verifier_generation: u64,
    /// Keyed by folder state.
    pub gates: BTreeMap<String, GateRecord>,
    /// Keyed by peer installation.
    pub peer_meta: BTreeMap<String, PeerMeta>,
}

impl CatalogSnapshot {
    pub fn folder(&self, state: &str) -> Option<&FolderRecord> {
        self.folders.iter().find(|folder| folder.state == state)
    }

    pub fn folder_by_repo(&self, repo: &str) -> Option<&FolderRecord> {
        self.folders.iter().find(|folder| folder.repo == repo)
    }

    pub fn peer_by_endpoint(&self, endpoint: &str) -> Option<&PeerRecord> {
        self.peers.iter().find(|peer| peer.endpoint == endpoint)
    }

    pub fn peer_by_installation(&self, installation: &str) -> Option<&PeerRecord> {
        self.peers
            .iter()
            .find(|peer| peer.installation == installation)
    }
}

impl fmt::Debug for CatalogSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogSnapshot")
            .field("generation", &self.generation)
            .field("installation", &self.installation)
            .field("folders", &self.folders)
            .field("peers", &self.peers)
            .field("verifier", &self.verifier.as_ref().map(|_| "<redacted>"))
            .field("verifier_generation", &self.verifier_generation)
            .field("gates", &self.gates)
            .field("peer_meta", &self.peer_meta)
            .finish()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FolderRecord {
    pub state: String,
    pub label: String,
    pub repo: String,
    pub device: String,
    pub directory: PathBuf,
    #[serde(default)]
    pub mount: Option<PathBuf>,
    pub desired_running: bool,
    pub sync_enabled: bool,
    pub mount_enabled: bool,
}

/// A paired peer. The pairing code is never serialized (invariant 21) and
/// never shown by `Debug`.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerRecord {
    pub endpoint: String,
    #[serde(skip_serializing, default)]
    pub code: String,
    pub installation: String,
    pub connections: u32,
    pub revision: String,
}

impl fmt::Debug for PeerRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerRecord")
            .field("endpoint", &self.endpoint)
            .field("code", &"<redacted>")
            .field("installation", &self.installation)
            .field("connections", &self.connections)
            .field("revision", &self.revision)
            .finish()
    }
}

/// The initial-sync mount gate of a received folder (`mount_gates`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateRecord {
    pub state: String,
    /// The installation the folder was received from (`None` for a legacy row
    /// whose endpoint could not be resolved).
    #[serde(default)]
    pub source_installation: Option<String>,
    pub source_endpoint: String,
    pub created_ms: u64,
    #[serde(default)]
    pub override_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerMeta {
    pub installation: String,
    #[serde(default)]
    pub last_seen_ms: Option<u64>,
    #[serde(default)]
    pub display_name: Option<String>,
    /// Incompatibility code last seen (e.g. `PEER_NEEDS_UPGRADE`).
    #[serde(default)]
    pub incompatible: Option<String>,
    #[serde(default)]
    pub revoked_ms: Option<u64>,
}

/// Catalog mutations; each runs in one transaction. `Debug` redacts secrets.
#[derive(Clone)]
pub enum CatalogCmd {
    CreateFolder {
        record: FolderRecord,
        gate: Option<GateRecord>,
    },
    SetDesired {
        state: String,
        desired: bool,
    },
    SetMount {
        state: String,
        mount: Option<PathBuf>,
    },
    SetIntents {
        state: String,
        sync_enabled: Option<bool>,
        mount_enabled: Option<bool>,
    },
    OverrideGate {
        state: String,
    },
    /// Deletes both `initial_sync` and `mount_gates` rows (after `Attached`).
    ClearGate {
        state: String,
    },
    RetargetGate {
        state: String,
        source_installation: Option<String>,
        source_endpoint: String,
    },
    UpsertPeer {
        record: PeerRecord,
    },
    RemovePeer {
        endpoint: String,
    },
    /// Also bumps `verifier_generation`.
    SetVerifier {
        verifier: Verifier,
    },
    RecordPeerSeen {
        installation: String,
        at_ms: u64,
        display_name: Option<String>,
        incompatible: Option<String>,
    },
}

impl CatalogCmd {
    /// The variant name, for logs and errors.
    pub fn name(&self) -> &'static str {
        match self {
            CatalogCmd::CreateFolder { .. } => "create_folder",
            CatalogCmd::SetDesired { .. } => "set_desired",
            CatalogCmd::SetMount { .. } => "set_mount",
            CatalogCmd::SetIntents { .. } => "set_intents",
            CatalogCmd::OverrideGate { .. } => "override_gate",
            CatalogCmd::ClearGate { .. } => "clear_gate",
            CatalogCmd::RetargetGate { .. } => "retarget_gate",
            CatalogCmd::UpsertPeer { .. } => "upsert_peer",
            CatalogCmd::RemovePeer { .. } => "remove_peer",
            CatalogCmd::SetVerifier { .. } => "set_verifier",
            CatalogCmd::RecordPeerSeen { .. } => "record_peer_seen",
        }
    }
}

impl fmt::Debug for CatalogCmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CatalogCmd::CreateFolder { record, gate } => f
                .debug_struct("CreateFolder")
                .field("record", record)
                .field("gate", gate)
                .finish(),
            CatalogCmd::SetDesired { state, desired } => f
                .debug_struct("SetDesired")
                .field("state", state)
                .field("desired", desired)
                .finish(),
            CatalogCmd::SetMount { state, mount } => f
                .debug_struct("SetMount")
                .field("state", state)
                .field("mount", mount)
                .finish(),
            CatalogCmd::SetIntents {
                state,
                sync_enabled,
                mount_enabled,
            } => f
                .debug_struct("SetIntents")
                .field("state", state)
                .field("sync_enabled", sync_enabled)
                .field("mount_enabled", mount_enabled)
                .finish(),
            CatalogCmd::OverrideGate { state } => f
                .debug_struct("OverrideGate")
                .field("state", state)
                .finish(),
            CatalogCmd::ClearGate { state } => {
                f.debug_struct("ClearGate").field("state", state).finish()
            }
            CatalogCmd::RetargetGate {
                state,
                source_installation,
                source_endpoint,
            } => f
                .debug_struct("RetargetGate")
                .field("state", state)
                .field("source_installation", source_installation)
                .field("source_endpoint", source_endpoint)
                .finish(),
            CatalogCmd::UpsertPeer { record } => f
                .debug_struct("UpsertPeer")
                .field("record", record)
                .finish(),
            CatalogCmd::RemovePeer { endpoint } => f
                .debug_struct("RemovePeer")
                .field("endpoint", endpoint)
                .finish(),
            CatalogCmd::SetVerifier { .. } => f
                .debug_struct("SetVerifier")
                .field("verifier", &"<redacted>")
                .finish(),
            CatalogCmd::RecordPeerSeen {
                installation,
                at_ms,
                display_name,
                incompatible,
            } => f
                .debug_struct("RecordPeerSeen")
                .field("installation", installation)
                .field("at_ms", at_ms)
                .field("display_name", display_name)
                .field("incompatible", incompatible)
                .finish(),
        }
    }
}

// ---------------------------------------------------------------------------
// Folders

/// The FolderRegistry (WP1.2): one controller per folder. `send` never waits
/// for the controller; `request` waits up to `wait` for the resulting view.
pub trait FolderDirectory: Send + Sync {
    fn view(&self, state: &str) -> Option<Arc<FolderView>>;
    fn by_repo(&self, repo: &str) -> Option<Arc<FolderView>>;
    fn all(&self) -> Vec<Arc<FolderView>>;
    fn send(&self, state: &str, cmd: FolderCmd) -> Result<(), ErrorWire>;
    fn request(
        &self,
        state: &str,
        cmd: FolderCmd,
        wait: Duration,
    ) -> Result<Arc<FolderView>, ErrorWire>;
    fn client(&self, state: &str) -> Option<Arc<dyn WorkerClient>>;
    /// Aggregate generation over every folder view.
    fn generation(&self) -> u64;
    fn wait_newer(&self, generation: u64, timeout: Duration) -> u64;
    /// Stops every folder in parallel within `deadline` (§3.3 Shutdown).
    fn shutdown(&self, deadline: Duration) -> Vec<ShutdownOutcome>;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum FolderCmd {
    Start,
    Stop {
        #[serde(default)]
        mode: StopMode,
    },
    ForceStop,
    Restart,
    Attach,
    Detach {
        #[serde(default)]
        force: bool,
    },
    SetSync {
        enabled: bool,
    },
    OverrideGate,
    Retry,
    Revalidate {
        #[serde(default)]
        ids: Option<Vec<String>>,
    },
    CatalogChanged,
    GateEvidence(GateEvidence),
    Tick,
}

/// Evidence about the gate source, from the exchange that involved it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateEvidence {
    pub installation: String,
    pub at_ms: u64,
    pub caught_up: bool,
    pub blocked: bool,
    #[serde(default)]
    pub waiting_on: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopStage {
    #[default]
    Requested,
    AwaitExit,
    Killing,
}

/// FolderController phases (§3.3).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FolderPhase {
    #[default]
    Stopped,
    Starting {
        since_ms: u64,
        attempt: u32,
        phase: String,
        progress_seq: u64,
        adopting: bool,
    },
    Running {
        since_ms: u64,
        instance: String,
        pid: u32,
        address: String,
        adopted: bool,
    },
    StopBlocked {
        since_ms: u64,
        reason: ErrorWire,
        open_handles: u32,
        unsaved: u32,
    },
    Stopping {
        since_ms: u64,
        stage: StopStage,
        deadline_ms: u64,
        force: bool,
    },
    Backoff {
        until_ms: u64,
        attempt: u32,
        last_exit: ExitRecord,
    },
    Failed {
        error: ErrorRecord,
    },
}

impl FolderPhase {
    /// The status phase this controller phase maps to.
    pub fn lifecycle(&self) -> crate::model::status::LifecyclePhase {
        use crate::model::status::LifecyclePhase;
        match self {
            FolderPhase::Stopped => LifecyclePhase::Stopped,
            FolderPhase::Starting { .. } => LifecyclePhase::Starting,
            FolderPhase::Running { .. } => LifecyclePhase::Running,
            FolderPhase::StopBlocked { .. } => LifecyclePhase::StopBlocked,
            FolderPhase::Stopping { .. } => LifecyclePhase::Stopping,
            FolderPhase::Backoff { .. } => LifecyclePhase::Backoff,
            FolderPhase::Failed { .. } => LifecyclePhase::Failed,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GateState {
    #[default]
    None,
    Awaiting {
        source: String,
        since_ms: u64,
        #[serde(default)]
        waiting_on: Vec<String>,
    },
    Blocked {
        source: String,
        reason: String,
    },
    Satisfied {
        at_ms: u64,
    },
    Overridden {
        at_ms: u64,
    },
}

impl GateState {
    pub fn kind(&self) -> crate::model::status::GateKind {
        use crate::model::status::GateKind;
        match self {
            GateState::None => GateKind::None,
            GateState::Awaiting { .. } => GateKind::Awaiting,
            GateState::Blocked { .. } => GateKind::Blocked,
            GateState::Satisfied { .. } => GateKind::Satisfied,
            GateState::Overridden { .. } => GateKind::Overridden,
        }
    }

    /// Whether the gate allows an attach (`None | Satisfied | Overridden`).
    pub fn allows_attach(&self) -> bool {
        matches!(
            self,
            GateState::None | GateState::Satisfied { .. } | GateState::Overridden { .. }
        )
    }
}

/// What a FolderController publishes (`Snapshot<FolderView>`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FolderView {
    pub generation: u64,
    pub record: FolderRecord,
    pub phase: FolderPhase,
    pub gate: GateState,
    pub mount: MountStatus,
    #[serde(default)]
    pub worker: Option<WorkerEndpoint>,
    #[serde(default)]
    pub heartbeat: Option<Arc<WorkerHeartbeat>>,
    #[serde(default)]
    pub last_error: Option<ErrorRecord>,
    pub restarts: u32,
    pub degraded: bool,
    #[serde(default)]
    pub published_error: Option<ErrorRecord>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerEndpoint {
    pub instance: String,
    pub pid: u32,
    pub address: String,
    pub version: String,
    /// Per-instance worker token; `None` until WP4.1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExitRecord {
    #[serde(default)]
    pub code: Option<i32>,
    #[serde(default)]
    pub fatal: Option<FatalLine>,
    pub stderr_tail: String,
    pub at_ms: u64,
}

/// `ShutdownOutcome.outcome` values.
pub mod outcomes {
    pub const STOPPED: &str = "stopped";
    pub const LEFT_RUNNING: &str = "left_running";
    pub const KILLED: &str = "killed";
    pub const NOT_RUNNING: &str = "not_running";
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShutdownOutcome {
    pub state: String,
    /// One of [`outcomes`]: `stopped|left_running|killed|not_running`.
    pub outcome: String,
    #[serde(default)]
    pub error: Option<ErrorWire>,
}

// ---------------------------------------------------------------------------
// Published catalog

/// O(1) cache of this daemon's published folders (WP1.1), served to peers by
/// `catalog.list`.
pub trait PublishedCatalogApi: Send + Sync {
    /// Sets (`Some`) or removes (`None`) the entry of folder `state`.
    fn update(&self, state: &str, entry: Option<PublishedEntry>);
    fn list(&self) -> (u64, Arc<Vec<PublishedEntry>>);
    fn wait_newer(&self, generation: u64, timeout: Duration) -> u64;
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryState {
    Running,
    #[default]
    Starting,
    Paused,
    Unavailable,
}

/// The tip of a replica's feed (`{epoch, seq}`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FeedTip {
    pub epoch: String,
    pub seq: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedEntry {
    pub repo: String,
    pub replica: String,
    pub label: String,
    pub branches: Vec<Branch>,
    pub state: EntryState,
    #[serde(default)]
    pub tip: Option<FeedTip>,
    pub sync_generation: u64,
    #[serde(default)]
    pub content: Option<ContentCounters>,
    #[serde(default)]
    pub error: Option<ErrorWire>,
    pub updated_at_ms: u64,
}

// ---------------------------------------------------------------------------
// Status

pub trait StatusSink: Send + Sync {
    /// In-memory only; never blocks on I/O.
    fn record(&self, event: StatusEvent);
}

pub trait StatusSource: Send + Sync {
    fn snapshot(&self) -> Arc<StatusSnapshot>;
    fn wait_newer(&self, generation: u64, timeout: Duration) -> Arc<StatusSnapshot>;
}

// ---------------------------------------------------------------------------
// Workers

/// A connection to one worker instance (JSON RPC in Waves 1–2; v3 loopback
/// sessions for binary ops from Wave 3).
pub trait WorkerClient: Send + Sync {
    fn instance(&self) -> &str;
    fn call(&self, op: &str, payload: Value, deadline: Duration) -> Result<Value, CallError>;
    fn call_bytes(
        &self,
        op: &str,
        header: Value,
        body: &[u8],
        deadline: Duration,
    ) -> Result<(Value, Vec<u8>), CallError>;
}

/// The failure of one call. `Timeout` and `ConnectionLost` are *uncertain*:
/// the request may have executed and is never replayed (invariant 16).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallError {
    /// A complete error reply; the connection stays usable (invariant 17).
    Remote(ErrorWire),
    Timeout {
        after_ms: u64,
    },
    ConnectionLost(String),
    /// Could not reach the peer or worker (nothing was sent).
    Unavailable(String),
    /// Refused locally because a bounded queue is full (nothing was sent).
    Busy,
    Cancelled,
}

impl CallError {
    /// `Timeout | ConnectionLost`.
    pub fn is_uncertain(&self) -> bool {
        matches!(
            self,
            CallError::Timeout { .. } | CallError::ConnectionLost(_)
        )
    }

    /// The error code: the remote code; `REQUEST_TIMEOUT`; `CONNECTION_LOST`;
    /// the first code in an `Unavailable` message or `LOCAL_WORKER_UNAVAILABLE`;
    /// `BUSY`; `CANCELLED`.
    pub fn code(&self) -> String {
        match self {
            CallError::Remote(error) => error.code.clone(),
            CallError::Timeout { .. } => "REQUEST_TIMEOUT".into(),
            CallError::ConnectionLost(_) => "CONNECTION_LOST".into(),
            CallError::Unavailable(message) => code_in(message)
                .unwrap_or("LOCAL_WORKER_UNAVAILABLE")
                .to_owned(),
            CallError::Busy => "BUSY".into(),
            CallError::Cancelled => "CANCELLED".into(),
        }
    }

    pub fn class(&self) -> ErrorClass {
        match self {
            CallError::Remote(error) => error.class,
            other => classify(&other.code()),
        }
    }

    pub fn to_wire(&self) -> ErrorWire {
        match self {
            CallError::Remote(error) => error.clone(),
            other => ErrorWire::new(other.code(), other.to_string()),
        }
    }
}

impl fmt::Display for CallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallError::Remote(error) => write!(f, "{error}"),
            CallError::Timeout { after_ms } => {
                write!(f, "REQUEST_TIMEOUT: no reply after {after_ms} ms")
            }
            CallError::ConnectionLost(detail) => write!(f, "CONNECTION_LOST: {detail}"),
            CallError::Unavailable(detail) => {
                crate::model::errors::write_coded(f, &self.code(), detail)
            }
            CallError::Busy => f.write_str("BUSY: request queue full"),
            CallError::Cancelled => f.write_str("CANCELLED: call cancelled"),
        }
    }
}

impl std::error::Error for CallError {}

impl From<ErrorWire> for CallError {
    fn from(error: ErrorWire) -> Self {
        CallError::Remote(error)
    }
}

/// How to launch one worker process (WP1.3).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchSpec {
    pub state: String,
    pub directory: PathBuf,
    pub executable: PathBuf,
    pub supervised: bool,
    /// `worker.log`; stdout lines and stderr are appended to it.
    pub log: PathBuf,
    #[serde(default)]
    pub env: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessEvent {
    /// A parsed stdout line.
    Line(StartupLine),
    Exited {
        code: Option<i32>,
        fatal: Option<FatalLine>,
        /// The last 4 KiB of `worker.log`.
        stderr_tail: String,
    },
}

pub type ProcessSink = Arc<dyn Fn(ProcessEvent) + Send + Sync>;

/// A launched or adopted worker process.
///
/// Dropping a `Box<dyn WorkerProcess>` never kills the process and never
/// waits for it: workers outlive the daemon (invariant 8), Shutdown's
/// `left_running` workers and abandoned adoption handles rely on it. Only
/// [`WorkerProcess::kill`] ends a process (CCR-WP1.2-3).
pub trait WorkerProcess: Send {
    fn pid(&self) -> u32;
    /// Kills the process after re-verifying its identity.
    fn kill(&self) -> Result<(), ErrorWire>;
    fn adopted(&self) -> bool;
}

pub trait WorkerLauncher: Send + Sync {
    fn launch(
        &self,
        spec: &LaunchSpec,
        events: ProcessSink,
    ) -> Result<Box<dyn WorkerProcess>, ErrorWire>;
    /// `Ok(None)`: the recorded process is dead or its identity does not match.
    fn adopt(
        &self,
        record: &WorkerRecord,
        events: ProcessSink,
    ) -> Result<Option<Box<dyn WorkerProcess>>, ErrorWire>;
    fn client(&self, endpoint: &WorkerEndpoint) -> Arc<dyn WorkerClient>;
}

// ---------------------------------------------------------------------------
// Network

/// Pairing and peer management: the legacy bridge in Wave 2, the v3
/// PeerManager from Wave 3.
pub trait NetworkApi: Send + Sync {
    fn pair(
        &self,
        endpoint: &str,
        code: &str,
        connections: Option<u32>,
    ) -> Result<PairResult, ErrorWire>;
    fn remote_list(&self, endpoint: &str) -> Result<Vec<PublishedEntry>, ErrorWire>;
    fn unpair(&self, endpoint: &str) -> Result<(), ErrorWire>;
    /// The catalog changed; reconcile peers and lanes.
    fn reconfigure(&self);
    fn shutdown(&self, deadline: Duration);
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairResult {
    pub endpoint: String,
    pub installation: String,
    pub connections: u32,
    pub configured_connections: u32,
    pub reserved_connections: u32,
}

/// The legacy List/Inspect `sync` member and the Peers reply (§7.3).
pub trait LegacyNetworkView: Send + Sync {
    fn folder_sync(&self, folder: &FolderRecord) -> Value;
    fn peers(&self) -> Value;
}

/// A peer's published catalog as last listed on its control lane.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteCatalog {
    pub generation: u64,
    pub at_ms: u64,
    pub entries: Vec<PublishedEntry>,
}

/// Per-peer shared state of the PeerSupervisor (WP2.6). The last good remote
/// catalog survives reconnects.
#[derive(Debug)]
pub struct PeerContext {
    pub installation: String,
    pub endpoint: String,
    pub local_installation: String,
    pub pool: u32,
    pub catalog: Snapshot<RemoteCatalog>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonEndpoints {
    pub control: String,
    #[serde(default)]
    pub network: Option<String>,
    #[serde(default)]
    pub log_path: Option<String>,
}
