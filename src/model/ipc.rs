//! Worker IPC v3 (§6.1): orchestrator ↔ worker, same binary. Frozen in Wave 0.
//!
//! * stdout startup lines ([`StartupLine`]: progress, ready, fatal), one JSON
//!   object per line, parsed by [`parse_line`];
//! * `STATE/worker.json` ([`WorkerRecord`], written atomically, removed on a
//!   clean exit; `runtime.json` / `Discovery` is unchanged);
//! * JSON RPC ops over the loopback endpoint (`{"request": id, "payload":
//!   {"op": …}}`), named in [`ops`], with their request/reply types and
//!   deadlines in [`deadlines`].
use crate::{
    core::Branch,
    model::{
        errors::{ErrorClass, ErrorWire, classify},
        health::{
            ActivationCounters, ContentCounters, EngineGauges, Generations, InboxCounts,
            JournalCounters, MountStatus, PeerFeedCounters, StoreHealth,
        },
        orchestrator::FeedTip,
        protocol::feed::{FeedApplyResult, MetaExchangeReply, MetaExchangeRequest},
    },
    paired_sync::SyncRoute,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{path::PathBuf, time::Duration};

/// IPC version carried by `ReadyLine`, `WorkerRecord` and `HelloReply`.
pub const IPC_VERSION: u32 = 3;
/// `worker.json` format.
pub const WORKER_RECORD_FORMAT: u32 = 1;
/// File name of the worker record inside the state directory.
pub const WORKER_RECORD_FILE: &str = "worker.json";

/// Worker exit codes (sysexits). Failures before Ready print a [`FatalLine`].
pub const EXIT_OK: i32 = 0;
/// Internal error, poisoned engine, wedged accept loop (`EX_SOFTWARE`).
pub const EXIT_INTERNAL: i32 = 70;
/// Store open/recovery failure (sysexits 73, `EX_CANTCREAT`).
pub const EXIT_STORE: i32 = 73;
/// `owner.lock` is held by another process (`EX_TEMPFAIL`): `ALREADY_RUNNING`.
pub const EXIT_LOCKED: i32 = 75;
/// Invalid configuration or arguments (`EX_CONFIG`).
pub const EXIT_CONFIG: i32 = 78;
/// Legacy `core::fault` / `util::fault` exit.
pub const EXIT_FAULT: i32 = 86;

/// Worker timing constants (§4).
pub mod timing {
    use std::time::Duration;
    /// Heartbeat publication period.
    pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
    /// Upper bound of `watch.wait_ms`.
    pub const WATCH_MAX_WAIT_MS: u32 = 5000;
    /// Main waits this long for the stop reply to be flushed before exiting.
    pub const STOP_REPLY_FLUSH: Duration = Duration::from_secs(2);
    /// Pause after an accept error.
    pub const ACCEPT_ERROR_PAUSE: Duration = Duration::from_millis(100);
    /// Consecutive accept errors before `exit(70)`.
    pub const ACCEPT_ERROR_LIMIT: u32 = 50;
    /// An engine-lock hold longer than this marks the engine `stalled`.
    pub const ENGINE_STALLED_AFTER: Duration = Duration::from_secs(120);
    /// Default of `TKFS_UNSUPERVISED_EXIT_S`.
    pub const UNSUPERVISED_EXIT_DEFAULT_S: u64 = 600;
    /// The `status` RPC may report `busy`/`stale` only if the cell is older.
    pub const STATUS_CELL_STALE: Duration = Duration::from_secs(5);
    /// Activator slice budget (§4.2).
    pub const ACTIVATION_SLICE_EVENTS: usize = 64;
    pub const ACTIVATION_SLICE_MS: u64 = 25;
    /// Activator idle wait between pokes.
    pub const ACTIVATOR_IDLE_WAIT: Duration = Duration::from_secs(1);
    /// Inbox retry backoff (§4.4).
    pub const INBOX_RETRY_BASE: Duration = Duration::from_secs(1);
    pub const INBOX_RETRY_CAP: Duration = Duration::from_secs(300);
    /// Maintenance tick.
    pub const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(1);
    /// Force-detach drain limit (§4.3).
    pub const DETACH_DRAIN: Duration = Duration::from_secs(10);
    /// Object lease TTL (§6.4), renewed on progress.
    pub const OBJECT_LEASE_TTL: Duration = Duration::from_secs(60);
    /// Pooled secondary stores per worker.
    pub const SYNC_STORE_POOL: usize = 8;
}

/// Orchestrator-side deadlines for worker RPCs.
pub mod deadlines {
    use std::time::Duration;
    pub const HELLO: Duration = Duration::from_secs(2);
    /// `watch{wait_ms: 2000}` is called with this deadline.
    pub const WATCH: Duration = Duration::from_secs(4);
    pub const WATCH_WAIT_MS: u32 = 2000;
    pub const STOP_GRACEFUL: Duration = Duration::from_secs(5);
    pub const MOUNT_ATTACH: Duration = Duration::from_secs(60);
    pub const MOUNT_DETACH: Duration = Duration::from_secs(15);
    pub const MOUNT_STATUS: Duration = Duration::from_secs(2);
    pub const PUBLISHED_SUMMARY: Duration = Duration::from_secs(3);
    pub const REVALIDATE: Duration = Duration::from_secs(30);
    pub const FEED_OFFER: Duration = Duration::from_secs(10);
    pub const FEED_EXCHANGE: Duration = Duration::from_secs(25);
    pub const FEED_APPLY: Duration = Duration::from_secs(10);
    /// Legacy `Folder` passthrough.
    pub const FOLDER_PASSTHROUGH: Duration = Duration::from_secs(30);
}

/// JSON RPC op names served by the worker.
pub mod ops {
    pub const HELLO: &str = "hello";
    pub const WATCH: &str = "watch";
    pub const STOP: &str = "stop";
    pub const MOUNT_ATTACH: &str = "mount-attach";
    pub const MOUNT_DETACH: &str = "mount-detach";
    pub const MOUNT_STATUS: &str = "mount-status";
    pub const REVALIDATE: &str = "revalidate";
    pub const PUBLISHED_SUMMARY: &str = "published-summary";
    pub const STATUS: &str = "status";
    pub const FEED_OFFER: &str = "feed-offer";
    pub const FEED_EXCHANGE: &str = "feed-exchange";
    pub const FEED_APPLY: &str = "feed-apply";
    /// Prefix routed to `runtime::sync_rpc::dispatch`.
    pub const FEED_PREFIX: &str = "feed-";
    /// Prefix routed to `runtime::objects_rpc::dispatch` (v3 loopback sessions).
    pub const OBJECTS_PREFIX: &str = "objects.";
}

// ---------------------------------------------------------------------------
// stdout startup lines

/// `{"startup_progress": 12, "phase": "verifying available objects"}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgressLine {
    pub startup_progress: u64,
    pub phase: String,
}

/// The Ready line. Legacy keys `ready`, `repo`, `device`, `runtime`, `mounted`
/// are kept; `mounted` is the actual state. A legacy line without the IPC v3
/// fields parses with `ipc == 0`, which the controller rejects with
/// `WORKER_IPC_MISMATCH`.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadyLine {
    pub ready: bool,
    pub repo: String,
    pub device: String,
    /// Path of `STATE/runtime.json`.
    pub runtime: PathBuf,
    pub mounted: bool,
    #[serde(default)]
    pub mount_error: Option<ErrorWire>,
    #[serde(default)]
    pub instance: String,
    #[serde(default)]
    pub pid: u32,
    #[serde(default)]
    pub address: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub ipc: u32,
    /// Per-instance worker token (WP4.1). `Debug` prints `<redacted>`
    /// (CCR-WP4.1-8, applied by WP4.INT): a formatted record never carries
    /// it into a log.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

/// The `Debug` text of a token field: whether there is one, never its value.
fn redacted(token: &Option<String>) -> &'static str {
    match token {
        Some(_) => "Some(<redacted>)",
        None => "None",
    }
}

impl std::fmt::Debug for ReadyLine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadyLine")
            .field("ready", &self.ready)
            .field("repo", &self.repo)
            .field("device", &self.device)
            .field("runtime", &self.runtime)
            .field("mounted", &self.mounted)
            .field("mount_error", &self.mount_error)
            .field("instance", &self.instance)
            .field("pid", &self.pid)
            .field("address", &self.address)
            .field("version", &self.version)
            .field("ipc", &self.ipc)
            .field("token", &format_args!("{}", redacted(&self.token)))
            .finish()
    }
}

/// A failure before Ready: printed once, then the process exits `exit_code`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FatalLine {
    pub fatal: bool,
    pub code: String,
    pub class: ErrorClass,
    pub message: String,
    pub exit_code: i32,
}

impl FatalLine {
    pub fn new(code: &str, message: impl Into<String>, exit_code: i32) -> Self {
        Self {
            fatal: true,
            code: code.to_owned(),
            class: classify(code),
            message: message.into(),
            exit_code,
        }
    }

    pub fn from_error(error: &anyhow::Error, exit_code: i32) -> Self {
        let wire = ErrorWire::from_error(error);
        Self {
            fatal: true,
            code: wire.code,
            class: wire.class,
            message: wire.message,
            exit_code,
        }
    }

    pub fn to_wire(&self) -> ErrorWire {
        ErrorWire {
            code: self.code.clone(),
            class: self.class,
            message: self.message.clone(),
            retry_after_ms: None,
        }
    }
}

/// One parsed worker stdout line. (Lines are rare; the size difference
/// between variants does not matter.)
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StartupLine {
    Progress(ProgressLine),
    Ready(ReadyLine),
    Fatal(FatalLine),
}

/// Parses one stdout line. Lines that are not JSON objects, or that carry none
/// of `startup_progress`, `ready: true`, `fatal: true`, yield `None` (they are
/// still copied to `worker.log` by the launcher).
pub fn parse_line(line: &str) -> Option<StartupLine> {
    let value: Value = serde_json::from_str(line.trim()).ok()?;
    let object = value.as_object()?;
    if object.contains_key("startup_progress") {
        return serde_json::from_value(value)
            .ok()
            .map(StartupLine::Progress);
    }
    if object.get("fatal") == Some(&Value::Bool(true)) {
        return serde_json::from_value(value).ok().map(StartupLine::Fatal);
    }
    if object.get("ready") == Some(&Value::Bool(true)) {
        return serde_json::from_value(value).ok().map(StartupLine::Ready);
    }
    None
}

// ---------------------------------------------------------------------------
// worker.json

/// `STATE/worker.json`, the adoption record (§3.3, §6.1).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerRecord {
    pub format: u32,
    pub pid: u32,
    pub instance: String,
    pub address: String,
    pub version: String,
    pub ipc: u32,
    pub repo: String,
    pub device: String,
    pub started_at_ms: u64,
    /// Windows `GetProcessTimes` creation FILETIME, or Linux `/proc/self/stat`
    /// field 22 (start time in clock ticks).
    pub process_start: u64,
    pub executable: PathBuf,
    pub supervised: bool,
    /// Per-instance worker token (WP4.1). `Debug` prints `<redacted>`
    /// (CCR-WP4.1-8, applied by WP4.INT): a formatted record never carries
    /// it into a log.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

impl std::fmt::Debug for WorkerRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerRecord")
            .field("format", &self.format)
            .field("pid", &self.pid)
            .field("instance", &self.instance)
            .field("address", &self.address)
            .field("version", &self.version)
            .field("ipc", &self.ipc)
            .field("repo", &self.repo)
            .field("device", &self.device)
            .field("started_at_ms", &self.started_at_ms)
            .field("process_start", &self.process_start)
            .field("executable", &self.executable)
            .field("supervised", &self.supervised)
            .field("token", &format_args!("{}", redacted(&self.token)))
            .finish()
    }
}

// ---------------------------------------------------------------------------
// RPC payloads

/// `hello` reply: identity used by adoption.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelloReply {
    pub instance: String,
    pub pid: u32,
    pub repo: String,
    pub device: String,
    pub version: String,
    pub ipc: u32,
    pub started_at_ms: u64,
    pub process_start: u64,
}

/// `watch{after_seq, wait_ms ≤ 5000}`; replies with a [`WorkerHeartbeat`] as
/// soon as `seq > after_seq` (or the current one when `wait_ms` elapses).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchRequest {
    #[serde(default)]
    pub after_seq: u64,
    #[serde(default)]
    pub wait_ms: u32,
}

impl WatchRequest {
    /// `wait_ms` clamped to [`timing::WATCH_MAX_WAIT_MS`].
    pub fn wait(&self) -> Duration {
        Duration::from_millis(u64::from(self.wait_ms.min(timing::WATCH_MAX_WAIT_MS)))
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopMode {
    /// Refuses with `BUSY_VIEW: …` while files are open or sessions are dirty.
    #[default]
    Graceful,
    /// Persists dirty sessions, force-detaches, exits.
    Force,
}

/// `stop{mode}`; no mode means graceful.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StopRequest {
    #[serde(default)]
    pub mode: StopMode,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StopReply {
    pub stopped: bool,
    pub persisted_dirty: u32,
    pub detached: bool,
}

/// `mount-attach{path}`; replies [`MountStatus`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MountAttachRequest {
    pub path: PathBuf,
}

/// `mount-detach{force}`; replies [`MountStatus`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MountDetachRequest {
    #[serde(default)]
    pub force: bool,
}

/// `revalidate{ids?}`: re-queue quarantined (and their blocked) inbox rows.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevalidateRequest {
    #[serde(default)]
    pub ids: Option<Vec<String>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevalidateReply {
    pub requeued: u64,
}

/// `published-summary` (extends the legacy `PublishedState`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedSummary {
    pub repo: String,
    pub replica: String,
    pub label: String,
    pub branches: Vec<Branch>,
    /// Feed tip (`epoch` is empty before WP2.5).
    #[serde(default)]
    pub tip: FeedTip,
    pub sync_generation: u64,
}

/// Typed members the `status` RPC adds to the legacy status JSON (WP2.3).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusExtras {
    pub activation: ActivationCounters,
    pub mount: MountStatus,
    pub store_health: StoreHealth,
    pub inbox: InboxCounts,
    pub heartbeat_seq: u64,
}

/// Published at 1 Hz by the heartbeat thread from atomics, the status cell and
/// `MountControl::status()`; never takes the engine lock. Wave 1 fills the
/// basic fields; the optional groups arrive with WP2.3.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerHeartbeat {
    pub instance: String,
    pub seq: u64,
    pub at_ms: u64,
    pub uptime_ms: u64,
    pub mount: MountStatus,
    pub engine: EngineGauges,
    pub store_health: StoreHealth,
    pub pending_saves: u32,
    pub open_handles: u32,
    pub unflushed_files: u32,
    #[serde(default)]
    pub generations: Generations,
    #[serde(default)]
    pub tip: Option<FeedTip>,
    #[serde(default)]
    pub inbox: Option<InboxCounts>,
    #[serde(default)]
    pub activation: Option<ActivationCounters>,
    #[serde(default)]
    pub content: Option<ContentCounters>,
    #[serde(default)]
    pub journal: Option<JournalCounters>,
    /// Per-peer feed counters, sorted by replica.
    #[serde(default)]
    pub peers: Vec<PeerFeedCounters>,
    /// Mount-scoped callback failures since the worker started (the counter
    /// that replaced the close path's engine-health write, §4.3;
    /// CCR-WP1.5-1).
    #[serde(default)]
    pub mount_errors: u64,
}

/// `feed-offer{route}` → [`MetaExchangeRequest`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedOfferRequest {
    pub route: SyncRoute,
}

/// `feed-exchange{route, request}` → [`FeedExchangeReply`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedExchangeRequest {
    pub route: SyncRoute,
    pub request: MetaExchangeRequest,
}

/// The acceptor's reply plus its own view (inbound gate evidence).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedExchangeReply {
    pub reply: MetaExchangeReply,
    pub local: FeedApplyResult,
}

/// `feed-apply{route, request_id, reply}` → [`FeedApplyResult`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedApplyRequest {
    pub route: SyncRoute,
    pub request_id: String,
    pub reply: MetaExchangeReply,
}
