//! Owned by WP3.4.
//!
//! The GUI view model (`tkfs::ui_model`, REVAMP_PLAN §7.4, §14 WP3.4).
//!
//! Everything a desktop shows is computed here from one [`StatusSnapshot`]
//! and a clock reading ([`Now`]), with no I/O, so it is tested on every
//! platform (`tests/gui_model.rs`); `src/gui.rs` keeps only the Slint glue.
//!
//! * [`folder_rows`], [`peer_rows`], [`folder_detail`], [`daemon_model`],
//!   [`overview`] and [`activity_rows`] build the models; [`chip_text`] is
//!   a folder's sync chip, [`format_eta`] and [`format_bytes`] the shared
//!   number formats.
//! * Every sync state is legible on its own: up to date, syncing metadata
//!   (n of m), downloading or uploading (bytes, rate, ETA), waiting, stalled
//!   since, blocked (why), offline, backoff (next retry), peer needs
//!   upgrade, initial-sync gate, mount failed (with its action), conflicts;
//!   and every open error carries its class, time and a suggested action.
//! * Errors come only from the snapshot being rendered: one that is absent
//!   from a snapshot is gone from every model built from it (§7.4).
//! * Nothing here can show a pairing code: the snapshot never carries one
//!   (invariant 21); the desktop shows a code only from the one-time
//!   `ResetPairingCode` reply.
//!
//! The desktop's two background loops live here too, written against small
//! transport traits so they are tested with fakes: [`Feed`] (one status
//! subscription, reconnect with backoff, daemon respawn) and [`CommandPool`]
//! (commands on their own connections, so a slow subscription or a slow
//! command never delays another command). [`MountSmoother`] is the
//! display-side adapter for CCR-WP2.1-7 (see its documentation).
use crate::{
    model::{
        errors::{ErrorClass, ErrorRecord, Scope, code_in},
        health::{MountIntent, MountState, MountStatus, StoreHealth},
        status::{
            Direction, FolderPeerState, FolderPeerSync, FolderStatus, FolderSyncState, GateKind,
            LaneState, LifecyclePhase, PeerConnState, PeerStatus, StatusSnapshot, TransferProgress,
        },
    },
    orchestrator::Action,
    util::cancel::Cancel,
};
use serde_json::Value;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, MutexGuard, mpsc},
    thread,
    time::{Duration, Instant},
};

pub use crate::orchestrator::hub_rules::{format_bytes, format_duration, format_elapsed};

// ---------------------------------------------------------------------------
// Clock and number formats

/// The moment a model is rendered for: "now" in Unix milliseconds, and the
/// local time zone's offset from UTC (for clock times).
///
/// Relative times ("3 min ago", "retry in 8 s") are measured against `ms`.
/// The desktop passes the local clock (a snapshot can be seconds old while
/// nothing changes); [`Now::of`] uses the snapshot's own `at_ms`, so a model
/// built with it depends on the snapshot alone (tests, the CLI-like
/// functions without an `_at` suffix).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Now {
    pub ms: u64,
    pub utc_offset_min: i32,
}

impl Now {
    /// The snapshot's own time, in UTC.
    pub fn of(snapshot: &StatusSnapshot) -> Self {
        Self::at(snapshot.at_ms)
    }

    /// `ms`, in UTC.
    pub fn at(ms: u64) -> Self {
        Self {
            ms,
            utc_offset_min: 0,
        }
    }

    pub fn with_offset(self, utc_offset_min: i32) -> Self {
        Self {
            utc_offset_min,
            ..self
        }
    }
}

/// `3.4 MiB/s`.
pub fn format_rate(bytes_per_second: u64) -> String {
    format!("{}/s", format_bytes(bytes_per_second))
}

/// `Some("ETA 6 min")`, or `None` when the ETA is unknown.
pub fn format_eta(eta_s: Option<u64>) -> Option<String> {
    eta_s.map(|seconds| format!("ETA {}", format_duration(seconds.saturating_mul(1000))))
}

/// `1,234,567`.
pub fn group(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

/// `1 file`, `1,234 files`.
pub fn count(n: u64, one: &str, many: &str) -> String {
    format!("{} {}", group(n), if n == 1 { one } else { many })
}

/// `just now` (under a second, or a time after `now`: clock skew), else
/// `12 s ago`, `4 min ago`.
pub fn ago(now: Now, then_ms: u64) -> String {
    if then_ms >= now.ms || now.ms - then_ms < 1000 {
        "just now".to_owned()
    } else {
        format!("{} ago", format_elapsed(now.ms - then_ms))
    }
}

/// A clock time in the local zone: `14:03:07` on today's date, else
/// `2026-10-13 14:03`.
pub fn clock(now: Now, ms: u64) -> String {
    let shift = |ms: u64| {
        let shifted = ms as i128 + i128::from(now.utc_offset_min) * 60_000;
        u64::try_from(shifted.max(0)).unwrap_or(0)
    };
    let (date, time) = civil(shift(ms));
    if civil(shift(now.ms)).0 == date {
        time
    } else {
        format!("{date} {}", &time[..5])
    }
}

/// `14:03:07 (3 min ago)`.
pub fn when(now: Now, ms: u64) -> String {
    format!("{} ({})", clock(now, ms), ago(now, ms))
}

/// (`YYYY-MM-DD`, `HH:MM:SS`) of a Unix time in milliseconds.
fn civil(ms: u64) -> (String, String) {
    let seconds = ms / 1000;
    let days = i64::try_from(seconds / 86_400).unwrap_or(i64::MAX / 2);
    let rest = seconds % 86_400;
    // Days since the epoch to a proleptic Gregorian date (H. Hinnant's
    // `civil_from_days`).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (
        format!("{year:04}-{month:02}-{day:02}"),
        format!(
            "{:02}:{:02}:{:02}",
            rest / 3600,
            (rest / 60) % 60,
            rest % 60
        ),
    )
}

/// Messages are cut to this many characters in rows and chips.
pub const MESSAGE_CHARS: usize = 180;

/// One line of at most `max` characters (`...` marks a cut).
pub fn clip(text: &str, max: usize) -> String {
    let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= max {
        return flat;
    }
    let mut cut: String = flat.chars().take(max).collect();
    cut.push_str("...");
    cut
}

/// A path as the user knows it: without the `\\?\` verbatim prefix the
/// daemon may report (`\\?\UNC\host\share` becomes `\\host\share`).
pub fn display_path(path: &std::path::Path) -> String {
    let text = path.display().to_string();
    if let Some(rest) = text.strip_prefix(r"\\?\UNC\") {
        format!(r"\\{rest}")
    } else if let Some(rest) = text.strip_prefix(r"\\?\") {
        rest.to_owned()
    } else {
        text
    }
}

/// The first 8 characters of an id (folder states, installations).
pub fn short(id: &str) -> String {
    id.chars().take(8).collect()
}

fn capitalized(text: &str) -> String {
    let mut chars = text.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

// ---------------------------------------------------------------------------
// Chips, actions and error lines

/// The colour family of a chip or line.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Tone {
    /// Plain information.
    #[default]
    Neutral,
    /// Healthy: up to date, mounted, online.
    Ok,
    /// Work in progress: syncing, starting, connecting.
    Progress,
    /// Degraded, retrying or waiting: backoff, offline, paused by a peer.
    Warn,
    /// Needs the user, or broken: failed, blocked, stalled, incompatible.
    Error,
    /// Off on purpose: stopped, paused, headless, resolved.
    Muted,
}

impl Tone {
    /// The name the Slint UI switches colours on.
    pub fn as_str(self) -> &'static str {
        match self {
            Tone::Neutral => "neutral",
            Tone::Ok => "ok",
            Tone::Progress => "progress",
            Tone::Warn => "warn",
            Tone::Error => "error",
            Tone::Muted => "muted",
        }
    }

    fn of_class(class: ErrorClass) -> Self {
        match class {
            ErrorClass::Contention | ErrorClass::Transient => Tone::Warn,
            ErrorClass::NeedsUserAction | ErrorClass::Incompatible | ErrorClass::Fatal => {
                Tone::Error
            }
        }
    }
}

/// What the user can do to a folder from the desktop (§7.4 "Folder
/// actions"). [`FolderAction::command`] is the management action it sends;
/// `ChangePath` opens a dialog instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FolderAction {
    Start,
    Stop,
    ForceStop,
    /// Override the initial-sync gate (`override-gate`).
    MountNow,
    Attach,
    Detach,
    PauseSync,
    ResumeSync,
    /// Restart a failed or backing-off worker (`retry`).
    Retry,
    /// Re-queue quarantined events (`revalidate`).
    Revalidate,
    ChangePath,
}

impl FolderAction {
    pub const ALL: [FolderAction; 11] = [
        FolderAction::Start,
        FolderAction::Stop,
        FolderAction::ForceStop,
        FolderAction::MountNow,
        FolderAction::Attach,
        FolderAction::Detach,
        FolderAction::PauseSync,
        FolderAction::ResumeSync,
        FolderAction::Retry,
        FolderAction::Revalidate,
        FolderAction::ChangePath,
    ];

    /// The stable id the UI passes back (`"force-stop"`, …).
    pub fn id(self) -> &'static str {
        match self {
            FolderAction::Start => "start",
            FolderAction::Stop => "stop",
            FolderAction::ForceStop => "force-stop",
            FolderAction::MountNow => "mount-now",
            FolderAction::Attach => "attach",
            FolderAction::Detach => "detach",
            FolderAction::PauseSync => "pause-sync",
            FolderAction::ResumeSync => "resume-sync",
            FolderAction::Retry => "retry",
            FolderAction::Revalidate => "revalidate",
            FolderAction::ChangePath => "change-path",
        }
    }

    pub fn from_id(id: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|action| action.id() == id)
    }

    /// Button caption.
    pub fn label(self) -> &'static str {
        match self {
            FolderAction::Start => "Start",
            FolderAction::Stop => "Stop",
            FolderAction::ForceStop => "Force stop",
            FolderAction::MountNow => "Mount now",
            FolderAction::Attach => "Attach",
            FolderAction::Detach => "Detach",
            FolderAction::PauseSync => "Pause sync",
            FolderAction::ResumeSync => "Resume sync",
            FolderAction::Retry => "Retry",
            FolderAction::Revalidate => "Revalidate",
            FolderAction::ChangePath => "Change path",
        }
    }

    /// Caption while the action runs.
    pub fn busy_label(self) -> &'static str {
        match self {
            FolderAction::Start => "Starting",
            FolderAction::Stop => "Stopping",
            FolderAction::ForceStop => "Force stopping",
            FolderAction::MountNow => "Mounting",
            FolderAction::Attach => "Attaching",
            FolderAction::Detach => "Detaching",
            FolderAction::PauseSync => "Pausing",
            FolderAction::ResumeSync => "Resuming",
            FolderAction::Retry => "Retrying",
            FolderAction::Revalidate => "Revalidating",
            FolderAction::ChangePath => "Saving",
        }
    }

    /// Whether the action can stop the folder or its mount (shown in a
    /// warning colour).
    pub fn is_destructive(self) -> bool {
        matches!(self, FolderAction::ForceStop)
    }

    /// The management action sent for this folder (`None`: a dialog).
    pub fn command(self, state: &str) -> Option<Action> {
        let state = state.to_owned();
        Some(match self {
            FolderAction::Start => Action::Start { state },
            FolderAction::Stop => Action::Stop { state, wait: None },
            FolderAction::ForceStop => Action::ForceStop { state },
            FolderAction::MountNow => Action::OverrideGate { state },
            FolderAction::Attach => Action::Attach { state },
            FolderAction::Detach => Action::Detach {
                state,
                force: false,
            },
            FolderAction::PauseSync => Action::PauseSync { state },
            FolderAction::ResumeSync => Action::ResumeSync { state },
            FolderAction::Retry => Action::Retry { state },
            FolderAction::Revalidate => Action::Revalidate { state, ids: None },
            FolderAction::ChangePath => return None,
        })
    }
}

/// The prefix the UI adds to an action id once the user confirmed it.
pub const CONFIRMED_PREFIX: &str = "confirmed-";

/// What a click on a folder action does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActionRoute {
    /// Send the action's command.
    Send(FolderAction),
    /// Ask first (Force stop); the confirmation sends the id again with
    /// [`CONFIRMED_PREFIX`].
    Confirm(FolderAction),
    /// Open the mount path dialog.
    ChangePath,
}

/// Routes an action id from the UI (`None`: not an action).
pub fn route_action(id: &str) -> Option<ActionRoute> {
    let (confirmed, id) = match id.strip_prefix(CONFIRMED_PREFIX) {
        Some(rest) => (true, rest),
        None => (false, id),
    };
    let action = FolderAction::from_id(id)?;
    Some(match action {
        FolderAction::ChangePath => ActionRoute::ChangePath,
        FolderAction::ForceStop if !confirmed => ActionRoute::Confirm(action),
        _ => ActionRoute::Send(action),
    })
}

/// A status chip: text, colour and, when the state has a remedy, the one
/// action the chip's button runs (§7.4: "Mount failed [Retry]").
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Chip {
    pub text: String,
    pub tone: Tone,
    pub action: Option<FolderAction>,
    /// The button caption (may differ from the action's label: a failed
    /// mount's `Attach` reads "Retry").
    pub action_label: String,
}

impl Chip {
    pub fn new(text: impl Into<String>, tone: Tone) -> Self {
        Self {
            text: text.into(),
            tone,
            action: None,
            action_label: String::new(),
        }
    }

    pub fn with_action(mut self, action: FolderAction, label: &str) -> Self {
        self.action = Some(action);
        self.action_label = label.to_owned();
        self
    }
}

/// One error, ready to show: class, code, message, when, how often, the
/// next retry and a suggested action.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ErrorLine {
    pub code: String,
    /// `busy`, `temporary`, `needs action`, `incompatible`, `fatal`.
    pub class: String,
    pub tone: Tone,
    pub message: String,
    /// What it applies to: `mount`, `peer office-nas`, `object 1a2b3c4d5e6f`.
    pub scope: Option<String>,
    /// Last occurrence: `14:03:07 (3 min ago)`.
    pub when: String,
    pub count: u64,
    /// First occurrence, when it happened more than once.
    pub first: Option<String>,
    /// `retry in 8 s`.
    pub next_retry: Option<String>,
    /// `resolved 2 min ago`.
    pub resolved: Option<String>,
    pub open: bool,
    pub suggestion: String,
}

impl ErrorLine {
    /// `[needs action] CODE: message · mount · 14:03:07 (3 min ago) · 4x ·
    /// retry in 8 s`.
    pub fn summary(&self) -> String {
        let mut parts = vec![format!("[{}] {}", self.class, self.message)];
        if let Some(scope) = &self.scope {
            parts.push(scope.clone());
        }
        parts.push(self.when.clone());
        if self.count > 1 {
            parts.push(format!("{}x", group(self.count)));
        }
        if let Some(retry) = &self.next_retry {
            parts.push(retry.clone());
        }
        if let Some(resolved) = &self.resolved {
            parts.push(resolved.clone());
        }
        parts.join(" · ")
    }
}

/// The class as the UI names it.
pub fn class_label(class: ErrorClass) -> &'static str {
    match class {
        ErrorClass::Contention => "busy",
        ErrorClass::Transient => "temporary",
        ErrorClass::NeedsUserAction => "needs action",
        ErrorClass::Incompatible => "incompatible",
        ErrorClass::Fatal => "fatal",
    }
}

/// What the user can do about an error code (§8 classes; specific codes
/// first).
pub fn suggestion(code: &str, class: ErrorClass) -> &'static str {
    match code {
        "MOUNT_PATH_MUST_NOT_EXIST" => {
            "Remove the existing folder at the mount path or choose another path, then retry the mount."
        }
        "MOUNT_OVERLAPS_DATA" | "MOUNT_OVERLAPS_FOLDER" | "MOUNT_OVERLAPS_STATE" => {
            "Choose a mount path outside the data directory and the other folders' mounts."
        }
        "WINFSP_RUNTIME_UNAVAILABLE" => "Install WinFsp (winfsp.dev), then retry the mount.",
        "INVALID_PAIRING_CODE" | "PAIRING_CODE_CHANGED" | "PAIRING_CODE_NOT_SET" => {
            "Pair again with the peer's current pairing code."
        }
        "PEER_INSTALLATION_CHANGED" => "The peer was reinstalled: disconnect it and pair again.",
        "PEER_REVOKED" => "The peer revoked this pairing: pair again.",
        "CANNOT_PAIR_WITH_SELF" => "That address is this computer: pair with another host.",
        "NAMESPACE_HISTORY_LIMIT" => "Resolve the folder's conflicts, then retry.",
        "CRASH_LOOP" => "The worker keeps crashing: check its worker.log, then Retry.",
        "PREVIOUS_WORKER_UNRESPONSIVE" => {
            "An older worker does not answer: Force stop it, then Start."
        }
        "STOP_BLOCKED_BY_OPEN_FILES" | "BUSY_VIEW" => {
            "Close the files open in this folder, or Force stop (unsaved files are kept)."
        }
        "GATE_BLOCKED" => "Revalidate the quarantined events, or Mount now.",
        "ALREADY_RUNNING" => "Another daemon or worker already runs on this data: stop it first.",
        "WORKER_IPC_MISMATCH" => "Restart the folder so its worker matches this version.",
        "FOLDER_SYNC_PAUSED" => "Sync is paused on one side: Resume sync.",
        "REMOTE_WORKER_STARTING" => "The peer's folder is starting; sync resumes on its own.",
        "CORRUPT_OBJECT" => {
            "A received copy failed verification; it is fetched again automatically."
        }
        "CORRUPT_AT_SOURCE" | "SIZE_MISMATCH" | "OBJECT_SIZE_CHANGED" => {
            "The peer's copy is damaged: it is re-verified at the source; fix or restore the file there."
        }
        "UNAUTHORIZED_EVENT"
        | "FORGED_EVENT_ORIGIN"
        | "INVALID_EVENT"
        | "CAUSAL_CYCLE"
        | "INVALID_CAUSAL_PARENT"
        | "INVALID_CAUSAL_DEPENDENCY"
        | "QUARANTINED_CAUSAL_ANCESTOR"
        | "EVENT_ID_REUSED"
        | "PRIVATE_EVENT_REJECTED"
        | "PRIVATE_BRANCH_REJECTED"
        | "EVENT_METADATA_TOO_LARGE"
        | "BRANCH_DESCRIPTOR_CHANGED" => "Quarantined: fix the sending peer, then Revalidate.",
        "STATE_NOT_INITIALIZED"
        | "STORE_IDENTITY_MISMATCH"
        | "INVALID_STORE_METADATA"
        | "ENGINE_POISONED"
        | "WORKER_STATE_MISMATCH"
        | "REPOSITORY_MISMATCH"
        | "INVALID_SELECTED_BRANCH" => {
            "The folder's store needs repair: check its worker.log; Retry restarts the worker."
        }
        "ORCHESTRATOR_NOT_RUNNING" => "The daemon is not running: it is started automatically.",
        _ => match class {
            ErrorClass::Contention => "Busy; retrying automatically.",
            ErrorClass::Transient => "Retrying automatically.",
            ErrorClass::NeedsUserAction => "Needs your action (see the message).",
            ErrorClass::Incompatible => {
                "Upgrade Telekinesis on the peer or on this computer so both speak the same protocol."
            }
            ErrorClass::Fatal => "Stopped in this scope: check the log for details.",
        },
    }
}

/// Display names of folders and peers.
pub struct Names<'a> {
    folders: &'a [FolderStatus],
    peers: &'a [PeerStatus],
}

impl<'a> Names<'a> {
    pub fn of(snapshot: &'a StatusSnapshot) -> Self {
        Self {
            folders: &snapshot.folders,
            peers: &snapshot.peers,
        }
    }

    /// A peer's label, else its endpoint, else `peer 1a2b3c4d`.
    pub fn peer(&self, installation: &str) -> String {
        match self.peers.iter().find(|p| p.installation == installation) {
            Some(peer) => peer_name(
                peer.label.as_deref(),
                peer.endpoint.as_deref(),
                installation,
            ),
            None => peer_name(None, None, installation),
        }
    }

    pub fn folder(&self, state: &str) -> String {
        match self.folders.iter().find(|f| f.state == state) {
            Some(folder) => folder_name(folder),
            None => format!("folder {}", short(state)),
        }
    }

    pub fn repo(&self, repo: &str) -> String {
        match self.folders.iter().find(|f| f.repo == repo) {
            Some(folder) => folder_name(folder),
            None => format!("repo {}", short(repo)),
        }
    }

    fn peer_status(&self, installation: &str) -> Option<&'a PeerStatus> {
        self.peers.iter().find(|p| p.installation == installation)
    }
}

pub fn folder_name(folder: &FolderStatus) -> String {
    if folder.label.trim().is_empty() {
        format!("folder {}", short(&folder.state))
    } else {
        folder.label.clone()
    }
}

pub fn peer_name(label: Option<&str>, endpoint: Option<&str>, installation: &str) -> String {
    match (label.filter(|l| !l.is_empty()), endpoint) {
        (Some(label), _) => label.to_owned(),
        (None, Some(endpoint)) => endpoint
            .trim_start_matches("wss://")
            .trim_start_matches("ws://")
            .to_owned(),
        (None, None) => format!("peer {}", short(installation)),
    }
}

/// An error record as shown.
pub fn error_line(record: &ErrorRecord, names: &Names, now: Now) -> ErrorLine {
    let open = !record.is_resolved();
    ErrorLine {
        code: record.error.code.clone(),
        class: class_label(record.error.class).to_owned(),
        tone: if open {
            Tone::of_class(record.error.class)
        } else {
            Tone::Muted
        },
        message: clip(&record.error.to_string(), MESSAGE_CHARS),
        scope: scope_text(&record.scope, names),
        when: when(now, record.last_at_ms),
        count: record.count,
        first: (record.count > 1).then(|| clock(now, record.first_at_ms)),
        next_retry: record
            .next_retry_at_ms
            .filter(|next| open && *next > now.ms)
            .map(|next| format!("retry in {}", format_duration(next - now.ms))),
        resolved: record
            .resolved_at_ms
            .map(|at| format!("resolved {}", ago(now, at))),
        open,
        suggestion: if open {
            suggestion(&record.error.code, record.error.class).to_owned()
        } else {
            String::new()
        },
    }
}

fn scope_text(scope: &Scope, names: &Names) -> Option<String> {
    match scope {
        Scope::Daemon | Scope::Folder { .. } => None,
        Scope::Mount { .. } => Some("mount".to_owned()),
        Scope::Object { hash, .. } => Some(format!(
            "object {}",
            hash.chars().take(12).collect::<String>()
        )),
        Scope::Event { id, .. } => Some(format!("event {}", short(id))),
        Scope::FolderPeer { installation, .. } | Scope::Peer { installation } => {
            Some(format!("peer {}", names.peer(installation)))
        }
        Scope::Lane { installation, lane } => {
            Some(format!("peer {} lane {lane}", names.peer(installation)))
        }
    }
}

/// The newest open error of a list (records arrive open-first, newest
/// first, from the hub; this does not rely on it).
/// `FOLDER_SYNC_PAUSED` is the Paused state, never shown as an error (§8 †).
fn newest_open(records: &[ErrorRecord]) -> Option<&ErrorRecord> {
    records
        .iter()
        .filter(|record| !record.is_resolved())
        .filter(|record| record.error.code != crate::orchestrator::hub_rules::PAUSED_CODE)
        .max_by_key(|record| (severity(record.error.class), record.last_at_ms))
}

fn severity(class: ErrorClass) -> u8 {
    match class {
        ErrorClass::Contention => 0,
        ErrorClass::Transient => 1,
        ErrorClass::Incompatible => 2,
        ErrorClass::NeedsUserAction => 3,
        ErrorClass::Fatal => 4,
    }
}

// ---------------------------------------------------------------------------
// Names of states

pub fn sync_state_name(state: FolderSyncState) -> &'static str {
    match state {
        FolderSyncState::Stopped => "stopped",
        FolderSyncState::NotShared => "not shared",
        FolderSyncState::Paused => "paused",
        FolderSyncState::Offline => "offline",
        FolderSyncState::InitialSync => "initial sync",
        FolderSyncState::Metadata => "syncing metadata",
        FolderSyncState::Downloading => "downloading",
        FolderSyncState::Uploading => "uploading",
        FolderSyncState::Syncing => "syncing",
        FolderSyncState::Blocked => "blocked",
        FolderSyncState::Backoff => "backing off",
        FolderSyncState::Stalled => "stalled",
        FolderSyncState::NeedsAttention => "needs attention",
        FolderSyncState::UpToDate => "up to date",
    }
}

pub fn direction_name(direction: Direction) -> &'static str {
    match direction {
        Direction::Outbound => "outbound",
        Direction::Inbound => "inbound",
        Direction::Both => "both ways",
    }
}

fn lane_state_name(state: LaneState) -> &'static str {
    match state {
        LaneState::Idle => "idle",
        LaneState::Dialing => "dialing",
        LaneState::Handshaking => "handshaking",
        LaneState::Ready => "ready",
        LaneState::Backoff => "backing off",
        LaneState::Incompatible => "incompatible",
        LaneState::Unauthorized => "unauthorized",
        LaneState::Closed => "closed",
    }
}

// ---------------------------------------------------------------------------
// Transfer arithmetic

/// The download or upload totals of a folder over its sharing peers. Peers
/// may offer the same objects, so the remaining work is the largest any
/// peer reports (as the hub's summary does); rates add up.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Transfer {
    pub objects_left: u64,
    pub bytes_left: u64,
    pub total_bytes: u64,
    pub rate_bps: u64,
    pub in_flight: u32,
}

impl Transfer {
    fn add(&mut self, t: &TransferProgress) {
        self.objects_left = self.objects_left.max(t.objects_remaining);
        self.bytes_left = self.bytes_left.max(t.bytes_remaining);
        self.total_bytes = self.total_bytes.max(t.total_bytes);
        self.rate_bps = self.rate_bps.saturating_add(t.rate_bps);
        self.in_flight = self.in_flight.saturating_add(t.in_flight);
    }

    fn of(t: &TransferProgress) -> Self {
        let mut out = Self::default();
        out.add(t);
        out
    }

    pub fn active(&self) -> bool {
        self.objects_left > 0 || self.bytes_left > 0 || self.in_flight > 0
    }

    /// Bytes moved in the current episode.
    pub fn done_bytes(&self) -> u64 {
        self.total_bytes.saturating_sub(self.bytes_left)
    }

    pub fn eta_s(&self) -> Option<u64> {
        (self.bytes_left > 0 && self.rate_bps > 0).then(|| self.bytes_left.div_ceil(self.rate_bps))
    }

    /// Fraction done in `0..=1`, when the total is known.
    pub fn fraction(&self) -> Option<f32> {
        (self.total_bytes > 0 && self.total_bytes >= self.bytes_left)
            .then(|| self.done_bytes() as f32 / self.total_bytes as f32)
    }

    /// `1.2 GiB of 3.0 GiB · 3.4 MiB/s · ETA 6 min`, or `120 files`.
    fn text(&self) -> String {
        let mut parts = Vec::new();
        if self.bytes_left > 0 && self.total_bytes > self.bytes_left {
            parts.push(format!(
                "{} of {}",
                format_bytes(self.done_bytes()),
                format_bytes(self.total_bytes)
            ));
        } else if self.bytes_left > 0 {
            parts.push(format!("{} left", format_bytes(self.bytes_left)));
            if self.objects_left > 0 {
                parts.push(count(self.objects_left, "file", "files"));
            }
        } else if self.objects_left > 0 {
            parts.push(count(self.objects_left, "file", "files"));
        }
        if self.rate_bps > 0 {
            parts.push(format_rate(self.rate_bps));
        }
        if let Some(eta) = format_eta(self.eta_s()) {
            parts.push(eta);
        }
        if parts.is_empty() && self.in_flight > 0 {
            parts.push(format!("{} in flight", self.in_flight));
        }
        parts.join(" · ")
    }
}

/// Metadata work of a folder over its sharing peers: the peer with the most
/// work left decides the "n of m".
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetaWork {
    /// Events left: to send + to receive + received not yet activated +
    /// pending at the peer (the largest over peers).
    pub left: u64,
    /// The peer feed's position and tip of that peer (`n of m`).
    pub cursor: u64,
    pub tip: u64,
    pub to_send: u64,
    pub to_receive: u64,
}

impl MetaWork {
    fn of(p: &FolderPeerSync) -> Self {
        let m = &p.metadata;
        Self {
            left: m
                .to_send
                .saturating_add(m.to_receive)
                .saturating_add(m.local_pending)
                .saturating_add(m.remote_pending),
            cursor: u64::try_from(m.cursor_seq.max(0)).unwrap_or(0),
            tip: u64::try_from(m.remote_tip.max(0)).unwrap_or(0),
            to_send: m.to_send,
            to_receive: m.to_receive,
        }
    }

    fn merge(self, other: Self) -> Self {
        if other.left > self.left { other } else { self }
    }

    /// `1,588 of 2,000` while receiving from a known tip.
    pub fn n_of_m(&self) -> Option<String> {
        (self.tip > 0 && self.to_receive > 0).then(|| {
            format!(
                "{} of {}",
                group(self.cursor.min(self.tip)),
                group(self.tip)
            )
        })
    }

    pub fn fraction(&self) -> Option<f32> {
        (self.tip > 0 && self.to_receive > 0)
            .then(|| self.cursor.min(self.tip) as f32 / self.tip as f32)
    }

    /// `Syncing metadata · 1,588 of 2,000 · 412 left`.
    fn text(&self, label: &str) -> String {
        let mut parts = vec![label.to_owned()];
        if let Some(n_of_m) = self.n_of_m() {
            parts.push(n_of_m);
        }
        if self.left > 0 {
            parts.push(format!("{} left", group(self.left)));
        } else {
            parts.push("checking peers".to_owned());
        }
        parts.join(" · ")
    }
}

fn sharing(folder: &FolderStatus) -> impl Iterator<Item = &FolderPeerSync> {
    folder
        .sync
        .peers
        .iter()
        .filter(|p| p.state != FolderPeerState::NotShared)
}

fn downloads(folder: &FolderStatus) -> Transfer {
    let mut out = Transfer::default();
    for p in sharing(folder) {
        out.add(&p.objects_down);
    }
    out
}

fn uploads(folder: &FolderStatus) -> Transfer {
    let mut out = Transfer::default();
    for p in sharing(folder) {
        out.add(&p.objects_up);
    }
    out
}

fn meta_work(folder: &FolderStatus) -> MetaWork {
    sharing(folder)
        .map(MetaWork::of)
        .fold(MetaWork::default(), MetaWork::merge)
}

/// Blocked, quarantined and rejected events of a folder: the largest any
/// peer reports, and the local inbox.
fn blocked_counts(folder: &FolderStatus) -> (u64, u64, u64) {
    let inbox = &folder.activation.inbox;
    sharing(folder).fold(
        (inbox.blocked, inbox.quarantined, 0),
        |(blocked, quarantined, rejected), p| {
            (
                blocked.max(p.metadata.blocked),
                quarantined.max(p.metadata.quarantined),
                rejected.max(p.metadata.rejected),
            )
        },
    )
}

fn blocked_text(blocked: u64, quarantined: u64, rejected: u64) -> Option<String> {
    let reasons: Vec<String> = [
        (quarantined, "quarantined"),
        (blocked, "blocked"),
        (rejected, "rejected"),
    ]
    .into_iter()
    .filter(|(n, _)| *n > 0)
    .map(|(n, what)| format!("{} {what}", count(n, "event", "events")))
    .collect();
    (!reasons.is_empty()).then(|| reasons.join(", "))
}

/// `CODE subject` for a stalled pair's culprit.
fn culprit(record: &ErrorRecord) -> String {
    match &record.scope {
        Scope::Object { hash, .. } => format!(
            "{} {}",
            record.error.code,
            hash.chars().take(12).collect::<String>()
        ),
        Scope::Event { id, .. } => format!("{} event {}", record.error.code, short(id)),
        _ => clip(&record.error.to_string(), 80),
    }
}

// ---------------------------------------------------------------------------
// Folder rows

/// One folder row of the main table (§7.4 "Folder row").
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FolderRowModel {
    pub state: String,
    pub label: String,
    pub repo: String,
    /// The mount path, or `""` for a folder without one.
    pub mount_path: String,
    pub sync_state: FolderSyncState,
    /// The sync chip (`Downloading 1.2 GiB of 3.0 GiB · 3.4 MiB/s · ETA 6 min`).
    pub sync: Chip,
    /// A second line: peers, conflicts, missing content, unacknowledged changes.
    pub sync_detail: String,
    /// Progress bar fraction (bytes, or metadata events) while transferring.
    pub progress: Option<f32>,
    pub mount: Chip,
    pub lifecycle: Chip,
    pub conflicts: u64,
    /// The newest open error, the most severe first.
    pub error: Option<ErrorLine>,
    pub open_errors: usize,
    /// Every action that applies now.
    pub actions: Vec<FolderAction>,
    /// The row's main button (Start, Stop or Force stop).
    pub primary: Option<FolderAction>,
    /// The row needs the user (failed, blocked, stalled, mount failed, a
    /// severe error).
    pub attention: bool,
    pub running: bool,
    pub mounted: bool,
}

/// Every folder row, sorted by label.
pub fn folder_rows(snapshot: &StatusSnapshot) -> Vec<FolderRowModel> {
    folder_rows_at(snapshot, Now::of(snapshot))
}

pub fn folder_rows_at(snapshot: &StatusSnapshot, now: Now) -> Vec<FolderRowModel> {
    let names = Names::of(snapshot);
    let mut rows: Vec<FolderRowModel> = snapshot
        .folders
        .iter()
        .map(|folder| folder_row(folder, &names, now))
        .collect();
    rows.sort_by(|a, b| {
        a.label
            .to_lowercase()
            .cmp(&b.label.to_lowercase())
            .then_with(|| a.state.cmp(&b.state))
    });
    rows
}

/// Rows whose label or mount path contains `query` (case-insensitive).
pub fn filter_rows(rows: &[FolderRowModel], query: &str) -> Vec<FolderRowModel> {
    let query = query.trim().to_lowercase();
    rows.iter()
        .filter(|row| {
            query.is_empty()
                || row.label.to_lowercase().contains(&query)
                || row.mount_path.to_lowercase().contains(&query)
        })
        .cloned()
        .collect()
}

pub fn folder_row(folder: &FolderStatus, names: &Names, now: Now) -> FolderRowModel {
    let actions = folder_actions(folder);
    let primary = [
        FolderAction::ForceStop,
        FolderAction::Retry,
        FolderAction::Stop,
        FolderAction::Start,
    ]
    .into_iter()
    .find(|action| actions.contains(action));
    let sync = sync_chip(folder, names, now);
    let mount = mount_chip(folder, now);
    let lifecycle = lifecycle_chip(folder, now);
    let error = newest_open(&folder.errors).map(|record| error_line(record, names, now));
    let open_errors = folder.errors.iter().filter(|r| !r.is_resolved()).count();
    let attention = matches!(
        folder.sync.state,
        FolderSyncState::NeedsAttention | FolderSyncState::Stalled | FolderSyncState::Blocked
    ) || [sync.tone, mount.tone, lifecycle.tone].contains(&Tone::Error)
        || error.as_ref().is_some_and(|e| e.tone == Tone::Error);
    FolderRowModel {
        state: folder.state.clone(),
        label: folder_name(folder),
        repo: folder.repo.clone(),
        mount_path: folder
            .mount
            .path
            .as_deref()
            .map(display_path)
            .unwrap_or_default(),
        sync_state: folder.sync.state,
        sync_detail: sync_detail(folder),
        progress: progress(folder),
        sync,
        mount,
        lifecycle,
        conflicts: folder.journal.unresolved_conflicts,
        error,
        open_errors,
        actions,
        primary,
        attention,
        running: folder.lifecycle.phase == LifecyclePhase::Running,
        mounted: folder.mount.state.is_attached(),
    }
}

/// The text of a folder's sync chip (§7.4).
pub fn chip_text(folder: &FolderStatus, peers: &[PeerStatus], now: Now) -> String {
    let names = Names {
        folders: std::slice::from_ref(folder),
        peers,
    };
    sync_chip(folder, &names, now).text
}

/// The sync chip of a folder (§7.4), from its aggregate state and the
/// structured per-peer progress (not the hub's summary line, so each state
/// shows its own numbers).
pub fn sync_chip(folder: &FolderStatus, names: &Names, now: Now) -> Chip {
    use FolderSyncState as F;
    let text = |text: String, tone: Tone| Chip::new(text, tone);
    match folder.sync.state {
        F::Stopped => {
            if folder.lifecycle.phase == LifecyclePhase::Stopped && !folder.intents.desired_running
            {
                Chip::new("Stopped", Tone::Muted).with_action(FolderAction::Start, "Start")
            } else {
                text(
                    format!("Not syncing — {}", lifecycle_brief(folder, now)),
                    Tone::Muted,
                )
            }
        }
        F::NotShared => text("Not shared with any peer".into(), Tone::Muted),
        F::Paused => {
            Chip::new("Paused", Tone::Muted).with_action(FolderAction::ResumeSync, "Resume")
        }
        F::Offline => text(waiting_text(folder, names, now), Tone::Warn),
        F::InitialSync => {
            let source = folder
                .gate
                .source
                .as_deref()
                .map(|source| names.peer(source))
                .unwrap_or_else(|| "the source peer".into());
            let meta = meta_work(folder);
            let mut parts = vec![format!("Initial sync from {source}")];
            if let Some(n_of_m) = meta.n_of_m() {
                parts.push(format!("{n_of_m} events"));
            } else if meta.left > 0 {
                parts.push(format!("{} left", count(meta.left, "event", "events")));
            }
            let down = downloads(folder);
            if down.active() {
                parts.push(format!("↓ {}", down.text()));
            }
            text(parts.join(" · "), Tone::Progress)
        }
        F::Metadata => text(meta_work(folder).text("Syncing metadata"), Tone::Progress),
        F::Downloading => {
            let down = downloads(folder);
            text(join_nonempty("Downloading", &down.text()), Tone::Progress)
        }
        F::Uploading => {
            let up = uploads(folder);
            text(join_nonempty("Uploading", &up.text()), Tone::Progress)
        }
        F::Syncing => {
            let mut parts = vec!["Syncing".to_owned()];
            let down = downloads(folder);
            if down.active() {
                parts.push(format!("↓ {}", down.text()));
            }
            let up = uploads(folder);
            if up.active() {
                parts.push(format!("↑ {}", up.text()));
            }
            let meta = meta_work(folder);
            if meta.left > 0 {
                parts.push(count(meta.left, "event", "events"));
            }
            text(parts.join(" · "), Tone::Progress)
        }
        F::Blocked => {
            let (blocked, quarantined, rejected) = blocked_counts(folder);
            let reason = blocked_text(blocked, quarantined, rejected).or_else(|| {
                sharing(folder)
                    .filter(|p| p.state == FolderPeerState::Blocked)
                    .find_map(|p| p.error.as_ref().map(|e| e.error.code.clone()))
            });
            let chip = match reason {
                Some(reason) => Chip::new(format!("Blocked — {reason}"), Tone::Error),
                None => Chip::new("Blocked", Tone::Error),
            };
            if quarantined + blocked > 0 && folder.lifecycle.phase == LifecyclePhase::Running {
                chip.with_action(FolderAction::Revalidate, "Revalidate")
            } else {
                chip
            }
        }
        F::Backoff => {
            let next = sharing(folder)
                .filter_map(|p| retry_at(p, names, now))
                .min();
            let code = sharing(folder).find_map(|p| p.error.as_ref().map(|e| e.error.code.clone()));
            let mut out = match next {
                Some(at) => format!("Retrying in {}", format_duration(at - now.ms)),
                None => "Retrying".to_owned(),
            };
            if let Some(code) = code {
                out.push_str(" — ");
                out.push_str(&code);
            }
            text(out, Tone::Warn)
        }
        F::Stalled => {
            let stalled: Vec<&FolderPeerSync> = sharing(folder)
                .filter(|p| p.state == FolderPeerState::Stalled)
                .collect();
            let since = stalled
                .iter()
                .filter_map(|p| p.last_progress_ms.or(p.stalled_since_ms))
                .min();
            let mut out = match since.filter(|since| now.ms >= *since) {
                Some(since) => format!("Stalled {}", format_elapsed(now.ms - since)),
                None => "Stalled".to_owned(),
            };
            if let Some(record) = stalled.iter().find_map(|p| p.error.as_ref()) {
                out.push_str(" — ");
                out.push_str(&culprit(record));
            }
            text(out, Tone::Error)
        }
        F::NeedsAttention => {
            let (reason, action) = attention_reason(folder);
            let chip = Chip::new(format!("Needs attention: {reason}"), Tone::Error);
            match action {
                Some((action, label)) => chip.with_action(action, label),
                None => chip,
            }
        }
        F::UpToDate => {
            let total = sharing(folder).count();
            let away = sharing(folder)
                .filter(|p| p.state != FolderPeerState::UpToDate)
                .count();
            if away > 0 {
                text(
                    format!(
                        "Up to date · {} of {} unreachable",
                        away,
                        count(total as u64, "peer", "peers")
                    ),
                    Tone::Ok,
                )
            } else {
                text("Up to date".into(), Tone::Ok)
            }
        }
    }
}

fn join_nonempty(head: &str, tail: &str) -> String {
    if tail.is_empty() {
        head.to_owned()
    } else {
        format!("{head} {tail}")
    }
}

/// When a pair retries, if that is still ahead of `now`: its own backoff,
/// else its peer's earliest lane backoff that is still ahead, else its
/// error's next retry. A time already past never hides a later one (a lane
/// whose backoff just ended does not hide another lane's retry).
fn retry_at(p: &FolderPeerSync, names: &Names, now: Now) -> Option<u64> {
    let ahead = |at: &u64| *at > now.ms;
    p.backoff_until_ms
        .filter(ahead)
        .or_else(|| {
            names.peer_status(&p.installation).and_then(|peer| {
                peer.lanes
                    .iter()
                    .filter_map(|lane| lane.backoff_until_ms)
                    .filter(ahead)
                    .min()
            })
        })
        .or_else(|| {
            p.error
                .as_ref()
                .and_then(|e| e.next_retry_at_ms)
                .filter(ahead)
        })
}

/// `Waiting for office-nas (offline 5 min)`, or for several peers
/// `Waiting for 2 peers (offline, needs upgrade)`.
fn waiting_text(folder: &FolderStatus, names: &Names, now: Now) -> String {
    let peers: Vec<&FolderPeerSync> = sharing(folder).collect();
    match peers.as_slice() {
        [] => "Waiting for peers".to_owned(),
        [p] => format!(
            "Waiting for {} ({})",
            names.peer(&p.installation),
            peer_wait_reason(p, names, now)
        ),
        many => {
            let mut reasons: Vec<String> = many
                .iter()
                .map(|p| peer_wait_kind(p.state).to_owned())
                .collect();
            reasons.sort();
            reasons.dedup();
            format!(
                "Waiting for {} ({})",
                count(many.len() as u64, "peer", "peers"),
                reasons.join(", ")
            )
        }
    }
}

fn peer_wait_kind(state: FolderPeerState) -> &'static str {
    match state {
        FolderPeerState::Offline => "offline",
        FolderPeerState::Connecting => "connecting",
        FolderPeerState::Backoff => "retrying",
        FolderPeerState::Incompatible => "needs upgrade",
        FolderPeerState::Unauthorized => "re-pair needed",
        FolderPeerState::WaitingForRemote => "its folder is not serving",
        FolderPeerState::WaitingForWorker => "local worker not running",
        _ => "waiting",
    }
}

/// Why one peer cannot be synced with, with how long or when it retries.
fn peer_wait_reason(p: &FolderPeerSync, names: &Names, now: Now) -> String {
    match p.state {
        FolderPeerState::Offline => {
            match names
                .peer_status(&p.installation)
                .and_then(|peer| peer.since_ms)
                .filter(|since| *since != 0 && now.ms >= *since)
            {
                Some(since) => format!("offline {}", format_elapsed(now.ms - since)),
                None => "offline".to_owned(),
            }
        }
        FolderPeerState::Backoff => match retry_at(p, names, now) {
            Some(at) => format!("retry in {}", format_duration(at - now.ms)),
            None => "retrying".to_owned(),
        },
        state => peer_wait_kind(state).to_owned(),
    }
}

/// The reason and remedy of a NeedsAttention folder: a failed worker, a
/// blocked gate, a failed mount, else the most severe open folder error.
fn attention_reason(folder: &FolderStatus) -> (String, Option<(FolderAction, &'static str)>) {
    let l = &folder.lifecycle;
    if l.phase == LifecyclePhase::Failed {
        let code = l
            .detail
            .as_deref()
            .and_then(code_in)
            .unwrap_or("worker failed");
        let action = if code == "PREVIOUS_WORKER_UNRESPONSIVE" {
            (FolderAction::ForceStop, "Force stop")
        } else {
            (FolderAction::Retry, "Retry")
        };
        let text = if code == "CRASH_LOOP" {
            "crash loop".to_owned()
        } else {
            code.to_owned()
        };
        return (text, Some(action));
    }
    if folder.gate.state == GateKind::Blocked {
        let reason = folder
            .gate
            .waiting_on
            .first()
            .map(|reason| clip(reason, 60))
            .unwrap_or_else(|| "events from the source are quarantined".into());
        return (
            format!("initial sync blocked — {reason}"),
            Some((FolderAction::Revalidate, "Revalidate")),
        );
    }
    if let MountState::AttachFailed { error, .. } = &folder.mount.state
        && error.class == ErrorClass::NeedsUserAction
    {
        return (
            format!("mount failed — {}", error.code),
            Some((FolderAction::Attach, "Retry")),
        );
    }
    let record = folder
        .errors
        .iter()
        .filter(|r| !r.is_resolved())
        .filter(|r| {
            matches!(
                r.error.class,
                ErrorClass::NeedsUserAction | ErrorClass::Fatal
            )
        })
        .max_by_key(|r| r.last_at_ms);
    match record {
        Some(record) => (record.error.code.clone(), None),
        None => match folder.store.health.error() {
            Some(error) => (format!("store — {}", error.code), None),
            None => ("see the folder's errors".into(), None),
        },
    }
}

/// The second line of a row: peers, conflicts, missing content and the
/// remote-backup claim (invariant 30: three separate claims).
fn sync_detail(folder: &FolderStatus) -> String {
    let mut parts = Vec::new();
    let peers: Vec<&FolderPeerSync> = sharing(folder).collect();
    if !peers.is_empty() {
        let up = peers
            .iter()
            .filter(|p| p.state == FolderPeerState::UpToDate)
            .count();
        parts.push(if up == peers.len() {
            format!("{} in sync", count(peers.len() as u64, "peer", "peers"))
        } else {
            format!(
                "{up} of {} in sync",
                count(peers.len() as u64, "peer", "peers")
            )
        });
    }
    let c = &folder.content;
    if c.current_missing > 0 {
        parts.push(format!(
            "{} not downloaded",
            count(c.current_missing, "file", "files")
        ));
    }
    if folder.journal.outgoing_unacknowledged > 0 {
        parts.push(format!(
            "{} not yet backed up",
            count(folder.journal.outgoing_unacknowledged, "change", "changes")
        ));
    }
    if folder.journal.unresolved_conflicts > 0 {
        parts.push(count(
            folder.journal.unresolved_conflicts,
            "conflict",
            "conflicts",
        ));
    }
    parts.join(" · ")
}

/// Progress bar fraction: bytes while transferring objects, metadata
/// events while syncing metadata or in the initial sync.
fn progress(folder: &FolderStatus) -> Option<f32> {
    use FolderSyncState as F;
    let combined = |a: Transfer, b: Transfer| {
        let total = a.total_bytes.saturating_add(b.total_bytes);
        let left = a.bytes_left.saturating_add(b.bytes_left);
        (total > 0 && total >= left).then(|| (total - left) as f32 / total as f32)
    };
    match folder.sync.state {
        F::Downloading => downloads(folder).fraction(),
        F::Uploading => uploads(folder).fraction(),
        F::Syncing => {
            combined(downloads(folder), uploads(folder)).or_else(|| meta_work(folder).fraction())
        }
        F::Metadata | F::InitialSync => meta_work(folder).fraction(),
        _ => None,
    }
}

/// The mount chip (§7.4): `Mounted`, `Headless`, `Mount failed [Retry]`,
/// `Waiting for initial sync [Mount now]`, …
pub fn mount_chip(folder: &FolderStatus, now: Now) -> Chip {
    let m = &folder.mount;
    let running = folder.lifecycle.phase == LifecyclePhase::Running;
    let gated = matches!(folder.gate.state, GateKind::Awaiting | GateKind::Blocked);
    let Some(_) = &m.path else {
        return Chip::new("Headless", Tone::Muted);
    };
    // CCR-WP2.1-7 adapter, part 1: a mount the controller still wants
    // attached, with its last failure open, is a failed mount being retried
    // even while a heartbeat shows it detached or attaching.
    let open_failure = folder
        .errors
        .iter()
        .find(|record| !record.is_resolved() && matches!(record.scope, Scope::Mount { .. }));
    match &m.state {
        MountState::Attached { .. } => Chip::new("Mounted", Tone::Ok),
        MountState::AttachFailed {
            next_retry_at_ms, ..
        } => {
            let text = match next_retry_at_ms.filter(|at| *at > now.ms) {
                Some(at) => format!("Mount failed · retry in {}", format_duration(at - now.ms)),
                None => "Mount failed".to_owned(),
            };
            Chip::new(text, Tone::Error).with_action(FolderAction::Attach, "Retry")
        }
        MountState::Lost { .. } => {
            Chip::new("Mount lost", Tone::Error).with_action(FolderAction::Attach, "Mount again")
        }
        MountState::Preflight | MountState::Attaching => match open_failure {
            Some(_) if m.intent == MountIntent::Attach => {
                Chip::new("Mount failed · retrying", Tone::Error)
                    .with_action(FolderAction::Attach, "Retry")
            }
            _ => Chip::new("Mounting…", Tone::Progress),
        },
        MountState::Detaching => Chip::new("Unmounting…", Tone::Progress),
        MountState::Detached | MountState::Disabled => {
            if gated && folder.intents.mount_enabled {
                let chip = Chip::new("Waiting for initial sync", Tone::Progress);
                return if running || folder.lifecycle.phase == LifecyclePhase::Starting {
                    chip.with_action(FolderAction::MountNow, "Mount now")
                } else {
                    chip
                };
            }
            if !folder.intents.mount_enabled {
                return Chip::new("Mount off", Tone::Muted);
            }
            if !running {
                return Chip::new("Not mounted", Tone::Muted);
            }
            if m.intent == MountIntent::Detach {
                return Chip::new("Detached", Tone::Muted)
                    .with_action(FolderAction::Attach, "Attach");
            }
            match open_failure {
                Some(_) if m.intent == MountIntent::Attach => {
                    Chip::new("Mount failed · retrying", Tone::Error)
                        .with_action(FolderAction::Attach, "Retry")
                }
                _ => {
                    Chip::new("Not mounted", Tone::Warn).with_action(FolderAction::Attach, "Attach")
                }
            }
        }
    }
}

/// A short lifecycle text for "Not syncing — …".
fn lifecycle_brief(folder: &FolderStatus, now: Now) -> String {
    let l = &folder.lifecycle;
    match l.phase {
        LifecyclePhase::Stopped => "start pending".to_owned(),
        LifecyclePhase::Starting => "worker starting".to_owned(),
        LifecyclePhase::Running => "running".to_owned(),
        LifecyclePhase::StopBlocked => "stop blocked by open files".to_owned(),
        LifecyclePhase::Stopping => "worker stopping".to_owned(),
        LifecyclePhase::Backoff => match l.next_retry_at_ms.filter(|at| *at > now.ms) {
            Some(at) => format!("worker restarting in {}", format_duration(at - now.ms)),
            None => "worker restarting".to_owned(),
        },
        LifecyclePhase::Failed => "worker failed".to_owned(),
    }
}

/// The restart attempt of a backing-off worker: from the lifecycle detail
/// (`restart attempt 3 after …`), else the restart count.
fn restart_attempt(folder: &FolderStatus) -> u32 {
    let l = &folder.lifecycle;
    l.detail
        .as_deref()
        .and_then(|detail| detail.split("attempt ").nth(1))
        .and_then(|rest| {
            rest.split(|c: char| !c.is_ascii_digit())
                .next()
                .and_then(|n| n.parse().ok())
        })
        .unwrap_or(l.restarts.max(1))
}

/// The lifecycle chip (§7.4): `Starting — verifying objects (seq 37)`,
/// `Running`, `Restarting in 32 s (attempt 3)`, `Crash loop [Retry]`,
/// `Stopping — files open [Force stop]`, `Adopted`.
pub fn lifecycle_chip(folder: &FolderStatus, now: Now) -> Chip {
    let l = &folder.lifecycle;
    match l.phase {
        LifecyclePhase::Stopped => {
            if folder.intents.desired_running {
                Chip::new("Start pending", Tone::Progress)
            } else {
                Chip::new("Stopped", Tone::Muted)
            }
        }
        LifecyclePhase::Starting => {
            let text = match (&l.startup, &l.detail) {
                _ if l.adopted => "Starting — adopting the running worker".to_owned(),
                (Some(progress), _) if !progress.phase.is_empty() => {
                    format!("Starting — {} (seq {})", progress.phase, progress.seq)
                }
                (_, Some(detail)) if !detail.is_empty() => format!("Starting — {detail}"),
                _ => "Starting…".to_owned(),
            };
            Chip::new(text, Tone::Progress)
        }
        LifecyclePhase::Running => {
            if l.degraded {
                Chip::new("Degraded — engine lock stalled", Tone::Warn)
            } else if l.adopted {
                Chip::new("Adopted", Tone::Ok)
            } else {
                Chip::new("Running", Tone::Ok)
            }
        }
        LifecyclePhase::StopBlocked => {
            let files = folder.store.open_handles;
            let text = if files > 0 {
                format!(
                    "Stopping — {} open",
                    count(u64::from(files), "file", "files")
                )
            } else {
                "Stopping — files open".to_owned()
            };
            Chip::new(text, Tone::Warn).with_action(FolderAction::ForceStop, "Force stop")
        }
        LifecyclePhase::Stopping => Chip::new("Stopping…", Tone::Progress)
            .with_action(FolderAction::ForceStop, "Force stop"),
        LifecyclePhase::Backoff => {
            let attempt = restart_attempt(folder);
            let text = match l.next_retry_at_ms.filter(|at| *at > now.ms) {
                Some(at) => format!(
                    "Restarting in {} (attempt {attempt})",
                    format_duration(at - now.ms)
                ),
                None => format!("Restarting (attempt {attempt})"),
            };
            Chip::new(text, Tone::Warn).with_action(FolderAction::Retry, "Retry now")
        }
        LifecyclePhase::Failed => {
            let code = l.detail.as_deref().and_then(code_in);
            match code {
                Some("CRASH_LOOP") => {
                    Chip::new("Crash loop", Tone::Error).with_action(FolderAction::Retry, "Retry")
                }
                Some("PREVIOUS_WORKER_UNRESPONSIVE") => {
                    Chip::new("Failed — previous worker unresponsive", Tone::Error)
                        .with_action(FolderAction::ForceStop, "Force stop")
                }
                Some(code) => Chip::new(format!("Failed — {code}"), Tone::Error)
                    .with_action(FolderAction::Retry, "Retry"),
                None => Chip::new("Failed", Tone::Error).with_action(FolderAction::Retry, "Retry"),
            }
        }
    }
}

/// Every action that applies to the folder now (§7.4 "Folder actions").
pub fn folder_actions(folder: &FolderStatus) -> Vec<FolderAction> {
    let l = &folder.lifecycle;
    let running = l.phase == LifecyclePhase::Running;
    let has_path = folder.mount.path.is_some();
    let gated = matches!(folder.gate.state, GateKind::Awaiting | GateKind::Blocked);
    let failed_code = (l.phase == LifecyclePhase::Failed)
        .then(|| l.detail.as_deref().and_then(code_in))
        .flatten();
    let (blocked, quarantined, _) = blocked_counts(folder);
    let mut out = Vec::new();
    let mut add = |action: FolderAction, when: bool| {
        if when {
            out.push(action);
        }
    };
    add(
        FolderAction::Start,
        l.phase == LifecyclePhase::Stopped && !folder.intents.desired_running,
    );
    add(
        FolderAction::Stop,
        folder.intents.desired_running
            && matches!(
                l.phase,
                LifecyclePhase::Starting
                    | LifecyclePhase::Running
                    | LifecyclePhase::Backoff
                    | LifecyclePhase::Failed
            ),
    );
    add(
        FolderAction::ForceStop,
        matches!(
            l.phase,
            LifecyclePhase::StopBlocked | LifecyclePhase::Stopping
        ) || failed_code == Some("PREVIOUS_WORKER_UNRESPONSIVE"),
    );
    add(
        FolderAction::Retry,
        matches!(l.phase, LifecyclePhase::Failed | LifecyclePhase::Backoff),
    );
    add(
        FolderAction::MountNow,
        gated
            && has_path
            && folder.intents.mount_enabled
            && matches!(l.phase, LifecyclePhase::Running | LifecyclePhase::Starting),
    );
    add(
        FolderAction::Attach,
        running
            && has_path
            && !gated
            && matches!(
                folder.mount.state,
                MountState::Detached
                    | MountState::Disabled
                    | MountState::AttachFailed { .. }
                    | MountState::Lost { .. }
            ),
    );
    add(
        FolderAction::Detach,
        running && folder.mount.state.is_attached(),
    );
    add(FolderAction::PauseSync, folder.intents.sync_enabled);
    add(FolderAction::ResumeSync, !folder.intents.sync_enabled);
    add(
        FolderAction::Revalidate,
        running && (folder.gate.state == GateKind::Blocked || blocked + quarantined > 0),
    );
    add(FolderAction::ChangePath, true);
    out
}

// ---------------------------------------------------------------------------
// Peer rows

/// One lane of a peer (§7.4 "Peer sidebar": role, state, in-flight op, repo,
/// since, bytes).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LaneRowModel {
    pub lane: u32,
    /// `control` or `bulk`, with `inbound` for a lane the peer opened.
    pub role: String,
    pub state: Chip,
    /// `meta.exchange · Photos · 3 s · 1.2 MiB`; several joined by `; `.
    pub inflight: String,
    pub since: String,
    /// `↑ 1.2 MiB · ↓ 3.4 GiB`.
    pub bytes: String,
    pub rate: String,
    pub reconnects: u32,
    pub error: Option<ErrorLine>,
}

/// One peer of the sidebar (§7.4 "Peer sidebar").
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PeerRowModel {
    pub installation: String,
    pub name: String,
    pub endpoint: String,
    pub state: PeerConnState,
    /// `Online`, `Backoff (retry in 12 s)`, `Peer needs upgrade`, …
    pub badge: Chip,
    pub online: bool,
    pub direction: String,
    /// The peer dials us; we have no endpoint for it.
    pub inbound_only: bool,
    pub protocol: String,
    /// `14 ms`.
    pub rtt: String,
    /// `↑ 1.2 MiB/s · ↓ 3.4 MiB/s`.
    pub rates: String,
    /// `3 of 4 lanes ready · 2 requests in flight · 1 inbound session`.
    pub lanes_summary: String,
    pub lanes: Vec<LaneRowModel>,
    /// The shared folders, by name.
    pub shared_folders: Vec<String>,
    pub error: Option<ErrorLine>,
}

/// Every peer: configured peers first, then inbound-only ones, by name.
pub fn peer_rows(snapshot: &StatusSnapshot) -> Vec<PeerRowModel> {
    peer_rows_at(snapshot, Now::of(snapshot))
}

pub fn peer_rows_at(snapshot: &StatusSnapshot, now: Now) -> Vec<PeerRowModel> {
    let names = Names::of(snapshot);
    let mut rows: Vec<PeerRowModel> = snapshot
        .peers
        .iter()
        .map(|peer| peer_row(peer, &names, now))
        .collect();
    rows.sort_by(|a, b| {
        a.inbound_only
            .cmp(&b.inbound_only)
            .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
            .then_with(|| a.installation.cmp(&b.installation))
    });
    rows
}

pub fn peer_row(peer: &PeerStatus, names: &Names, now: Now) -> PeerRowModel {
    let inbound_only = peer.endpoint.is_none() || peer.direction == Direction::Inbound;
    let mut rates = Vec::new();
    if peer.rate_up_bps > 0 || peer.rate_down_bps > 0 {
        rates.push(format!("↑ {}", format_rate(peer.rate_up_bps)));
        rates.push(format!("↓ {}", format_rate(peer.rate_down_bps)));
    }
    PeerRowModel {
        installation: peer.installation.clone(),
        name: peer_name(
            peer.label.as_deref(),
            peer.endpoint.as_deref(),
            &peer.installation,
        ),
        endpoint: peer.endpoint.clone().unwrap_or_default(),
        state: peer.state,
        badge: peer_badge(peer, now),
        online: peer.state == PeerConnState::Online,
        direction: if inbound_only {
            "inbound only".to_owned()
        } else {
            direction_name(peer.direction).to_owned()
        },
        inbound_only,
        protocol: peer.protocol.clone().unwrap_or_default(),
        rtt: peer
            .rtt_ms
            .map(|rtt| format!("{rtt} ms"))
            .unwrap_or_default(),
        rates: rates.join(" · "),
        lanes_summary: lanes_summary(peer),
        lanes: peer
            .lanes
            .iter()
            .map(|lane| lane_row(lane, names, now))
            .collect(),
        shared_folders: peer.shared_folders.iter().map(|r| names.repo(r)).collect(),
        error: peer
            .error
            .as_ref()
            .filter(|record| !record.is_resolved())
            .map(|record| error_line(record, names, now)),
    }
}

/// The connection badge of a peer (§7.4).
pub fn peer_badge(peer: &PeerStatus, now: Now) -> Chip {
    let since = peer
        .since_ms
        .filter(|since| *since != 0 && now.ms >= *since)
        .map(|since| format_elapsed(now.ms - since));
    match peer.state {
        PeerConnState::Online => Chip::new("Online", Tone::Ok),
        PeerConnState::Connecting => Chip::new("Connecting…", Tone::Progress),
        PeerConnState::Offline => Chip::new(
            match since {
                Some(since) => format!("Offline {since}"),
                None => "Offline".to_owned(),
            },
            Tone::Muted,
        ),
        PeerConnState::Backoff => {
            let retry = peer
                .lanes
                .iter()
                .filter_map(|lane| lane.backoff_until_ms)
                .filter(|until| *until > now.ms)
                .min();
            Chip::new(
                match retry {
                    Some(until) => {
                        format!("Backoff (retry in {})", format_duration(until - now.ms))
                    }
                    None => "Backoff".to_owned(),
                },
                Tone::Warn,
            )
        }
        PeerConnState::Incompatible => Chip::new("Peer needs upgrade", Tone::Error),
        PeerConnState::Unauthorized => Chip::new("Unauthorized — re-pair", Tone::Error),
        PeerConnState::Revoked => Chip::new("Revoked", Tone::Error),
    }
}

fn lanes_summary(peer: &PeerStatus) -> String {
    let outbound: Vec<_> = peer
        .lanes
        .iter()
        .filter(|lane| lane.direction != Direction::Inbound)
        .collect();
    let mut parts = Vec::new();
    if !outbound.is_empty() {
        let ready = outbound
            .iter()
            .filter(|lane| lane.state == LaneState::Ready)
            .count();
        parts.push(format!("{ready} of {} lanes ready", outbound.len()));
    }
    let inflight: usize = peer.lanes.iter().map(|lane| lane.inflight.len()).sum();
    if inflight > 0 {
        parts.push(count(
            inflight as u64,
            "request in flight",
            "requests in flight",
        ));
    }
    if peer.inbound_sessions > 0 {
        parts.push(count(
            u64::from(peer.inbound_sessions),
            "inbound session",
            "inbound sessions",
        ));
    }
    parts.join(" · ")
}

fn lane_row(lane: &crate::model::status::LaneStatus, names: &Names, now: Now) -> LaneRowModel {
    let role = lane.role.as_str();
    let tone = match lane.state {
        LaneState::Ready => Tone::Ok,
        LaneState::Dialing | LaneState::Handshaking => Tone::Progress,
        LaneState::Backoff => Tone::Warn,
        LaneState::Incompatible | LaneState::Unauthorized => Tone::Error,
        LaneState::Idle | LaneState::Closed => Tone::Muted,
    };
    let mut state = lane_state_name(lane.state).to_owned();
    if let Some(until) = lane.backoff_until_ms.filter(|until| *until > now.ms) {
        state.push_str(&format!(" (retry in {})", format_duration(until - now.ms)));
    }
    let inflight = lane
        .inflight
        .iter()
        .map(|op| {
            let mut parts = vec![op.op.clone(), names.repo(&op.repo)];
            if op.since_ms != 0 && now.ms >= op.since_ms {
                parts.push(format_elapsed(now.ms - op.since_ms));
            }
            if op.bytes > 0 {
                parts.push(format_bytes(op.bytes));
            }
            parts.join(" · ")
        })
        .collect::<Vec<_>>()
        .join("; ");
    let mut rate = Vec::new();
    if lane.rate_up_bps > 0 || lane.rate_down_bps > 0 {
        rate.push(format!("↑ {}", format_rate(lane.rate_up_bps)));
        rate.push(format!("↓ {}", format_rate(lane.rate_down_bps)));
    }
    LaneRowModel {
        lane: lane.lane,
        role: if lane.direction == Direction::Inbound {
            format!("{role} (inbound)")
        } else {
            role.to_owned()
        },
        state: Chip::new(state, tone),
        inflight,
        since: lane
            .since_ms
            .filter(|since| *since != 0 && now.ms >= *since)
            .map(|since| format!("for {}", format_elapsed(now.ms - since)))
            .unwrap_or_default(),
        bytes: format!(
            "↑ {} · ↓ {}",
            format_bytes(lane.bytes_up),
            format_bytes(lane.bytes_down)
        ),
        rate: rate.join(" · "),
        reconnects: lane.reconnects,
        error: lane
            .error
            .as_ref()
            .filter(|record| !record.is_resolved())
            .map(|record| error_line(record, names, now)),
    }
}

// ---------------------------------------------------------------------------
// Folder detail

/// One `key: value` line of a detail tab.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
    pub tone: Tone,
}

fn kv(key: &str, value: impl Into<String>, tone: Tone) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: value.into(),
        tone,
    }
}

/// One peer of a folder's Sync tab (§7.4: peer, direction, state, to
/// send/receive, local/remote pending, blocked/quarantined/rejected, ↓/↑
/// bytes, rate, ETA, last progress, error with class, message and next
/// retry).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FolderPeerRowModel {
    pub installation: String,
    pub name: String,
    pub direction: String,
    pub state: Chip,
    pub to_send: u64,
    pub to_receive: u64,
    pub local_pending: u64,
    pub remote_pending: u64,
    pub blocked: u64,
    pub quarantined: u64,
    pub rejected: u64,
    pub caught_up: bool,
    /// `1,588 of 2,000 · 412 to receive · 3 to send · caught up`.
    pub metadata: String,
    /// `1.2 GiB of 3.0 GiB · 3.4 MiB/s · ETA 6 min` (or `idle`).
    pub down: String,
    pub up: String,
    pub last_progress: String,
    pub last_exchange: String,
    pub error: Option<ErrorLine>,
}

/// A folder's dialog (§7.4 "Folder dialog tabs").
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FolderDetailModel {
    pub row: FolderRowModel,
    /// Lifecycle, mount, gate, store, the three claims, intents, identity.
    pub overview: Vec<KeyValue>,
    /// The gate's `waiting_on` list.
    pub gate_waiting_on: Vec<String>,
    pub peers: Vec<FolderPeerRowModel>,
    /// Inbox counts, rate, backlog, starvation.
    pub activation: Vec<KeyValue>,
    pub content: Vec<KeyValue>,
    /// Open errors, then the history.
    pub errors: Vec<ErrorLine>,
    pub activity: Vec<ActivityRowModel>,
}

/// The detail of folder `state`, `None` when the snapshot does not list it.
pub fn folder_detail(snapshot: &StatusSnapshot, state: &str) -> Option<FolderDetailModel> {
    folder_detail_at(snapshot, state, Now::of(snapshot), None)
}

/// [`folder_detail`] at `now`; `activation_rate` is the activation rate in
/// events per second ([`RateMeter`]), when known.
pub fn folder_detail_at(
    snapshot: &StatusSnapshot,
    state: &str,
    now: Now,
    activation_rate: Option<f64>,
) -> Option<FolderDetailModel> {
    let folder = snapshot.folders.iter().find(|f| f.state == state)?;
    let names = Names::of(snapshot);
    let mut peers: Vec<FolderPeerRowModel> = sharing(folder)
        .map(|p| folder_peer_row(p, &names, now))
        .collect();
    peers.sort_by_key(|a| a.name.to_lowercase());
    Some(FolderDetailModel {
        row: folder_row(folder, &names, now),
        overview: overview_rows(folder, &names, now),
        gate_waiting_on: folder.gate.waiting_on.clone(),
        peers,
        activation: activation_rows(folder, now, activation_rate),
        content: content_rows(folder),
        errors: folder
            .errors
            .iter()
            .map(|record| error_line(record, &names, now))
            .collect(),
        activity: activity_rows_at(snapshot, now, Some(state), 200),
    })
}

fn overview_rows(f: &FolderStatus, names: &Names, now: Now) -> Vec<KeyValue> {
    let l = &f.lifecycle;
    let mut rows = Vec::new();
    let life = lifecycle_chip(f, now);
    let mut parts = vec![life.text.clone()];
    if l.since_ms != 0 && now.ms >= l.since_ms {
        parts.push(format!("since {}", when(now, l.since_ms)));
    }
    if let Some(pid) = l.pid {
        parts.push(format!("pid {pid}"));
    }
    if l.restarts > 0 {
        parts.push(count(u64::from(l.restarts), "restart", "restarts"));
    }
    rows.push(kv("Lifecycle", parts.join(" · "), life.tone));
    if let Some(detail) = l.detail.as_deref().filter(|_| {
        matches!(
            l.phase,
            LifecyclePhase::Failed | LifecyclePhase::StopBlocked
        )
    }) {
        rows.push(kv("Detail", clip(detail, MESSAGE_CHARS), life.tone));
    }
    if let Some(exit) = l
        .last_exit
        .as_ref()
        .filter(|_| l.phase != LifecyclePhase::Running)
    {
        let mut parts = vec![match exit.code {
            Some(code) => format!("exit code {code}"),
            None => "no exit code".to_owned(),
        }];
        match &exit.fatal {
            Some(fatal) => parts.push(format!(
                "[{}] {}",
                class_label(fatal.class),
                clip(&fatal.message, 120)
            )),
            None => {
                if let Some(tail) = exit
                    .stderr_tail
                    .lines()
                    .rev()
                    .find(|l| !l.trim().is_empty())
                {
                    parts.push(format!("log: {}", clip(tail, 120)));
                }
            }
        }
        if exit.at_ms != 0 {
            parts.push(ago(now, exit.at_ms));
        }
        rows.push(kv("Last exit", parts.join(" · "), Tone::Warn));
    }
    let mount = mount_chip(f, now);
    let mount_value = match &f.mount.path {
        Some(path) => {
            let mut parts = vec![mount.text.clone()];
            if let Some(error) = f.mount.state.error() {
                parts.push(clip(&error.to_string(), 120));
            }
            if let MountState::AttachFailed { attempt, .. } = &f.mount.state {
                parts.push(format!("attempt {attempt}"));
            }
            parts.push(display_path(path));
            parts.join(" · ")
        }
        None => "Headless (no mount path)".to_owned(),
    };
    rows.push(kv("Mount", mount_value, mount.tone));
    match f.gate.state {
        GateKind::None => {}
        GateKind::Awaiting => {
            let source = f
                .gate
                .source
                .as_deref()
                .map(|s| names.peer(s))
                .unwrap_or_else(|| "the source peer".into());
            let mut text = format!("Waiting for the initial sync from {source}");
            if let Some(since) = f.gate.since_ms.filter(|s| *s != 0 && now.ms >= *s) {
                text.push_str(&format!(" for {}", format_elapsed(now.ms - since)));
            }
            rows.push(kv("Initial sync", text, Tone::Progress));
        }
        GateKind::Blocked => rows.push(kv(
            "Initial sync",
            "Blocked: events from the source are quarantined or rejected (Revalidate or Mount now)",
            Tone::Error,
        )),
        GateKind::Satisfied => rows.push(kv("Initial sync", "Complete", Tone::Ok)),
        GateKind::Overridden => rows.push(kv(
            "Initial sync",
            "Overridden: mounted before the initial sync finished",
            Tone::Warn,
        )),
    }
    let s = &f.store;
    let (health, tone) = match &s.health {
        StoreHealth::Healthy => ("Healthy".to_owned(), Tone::Ok),
        StoreHealth::Degraded { error } => (
            format!("Degraded — {}", clip(&error.to_string(), 120)),
            Tone::Warn,
        ),
        StoreHealth::Failed { error } => (
            format!("Failed — {}", clip(&error.to_string(), 120)),
            Tone::Error,
        ),
    };
    rows.push(kv("Store", health, tone));
    rows.push(kv(
        "Files",
        format!(
            "{} · {} · {}",
            count(u64::from(s.open_handles), "open file", "open files"),
            count(u64::from(s.pending_saves), "pending save", "pending saves"),
            count(
                u64::from(s.unflushed_files),
                "unflushed file",
                "unflushed files"
            )
        ),
        if s.pending_saves > 0 {
            Tone::Warn
        } else {
            Tone::Neutral
        },
    ));
    // Invariant 30: three separate claims.
    let peers: Vec<&FolderPeerSync> = sharing(f).collect();
    let caught_up = peers.iter().filter(|p| p.metadata.caught_up).count();
    rows.push(if peers.is_empty() {
        kv("Metadata", "No sharing peer", Tone::Muted)
    } else if caught_up == peers.len() {
        kv("Metadata", "Caught up with every peer", Tone::Ok)
    } else {
        kv(
            "Metadata",
            format!(
                "Caught up with {caught_up} of {}",
                count(peers.len() as u64, "peer", "peers")
            ),
            Tone::Progress,
        )
    });
    let c = &f.content;
    rows.push(if c.complete {
        kv("Content", "Complete: every shared file is here", Tone::Ok)
    } else if c.current_missing > 0 {
        kv(
            "Content",
            format!(
                "{} of {} missing ({})",
                group(c.current_missing),
                count(c.current_objects, "file", "files"),
                format_bytes(c.current_missing_bytes)
            ),
            Tone::Progress,
        )
    } else {
        kv(
            "Content",
            "Current files complete; history incomplete",
            Tone::Neutral,
        )
    });
    let j = &f.journal;
    rows.push(if j.outgoing_unacknowledged > 0 {
        kv(
            "Backed up",
            format!(
                "{} not yet acknowledged by a peer",
                count(j.outgoing_unacknowledged, "change", "changes")
            ),
            Tone::Progress,
        )
    } else if j.shared_events > 0 {
        kv("Backed up", "Every change acknowledged by a peer", Tone::Ok)
    } else {
        kv("Backed up", "Nothing shared yet", Tone::Muted)
    });
    rows.push(kv(
        "Journal",
        format!(
            "{} · {} shared · tip {}{}",
            count(j.events, "event", "events"),
            group(j.shared_events),
            j.tip,
            if j.unresolved_conflicts > 0 {
                format!(
                    " · {}",
                    count(
                        j.unresolved_conflicts,
                        "unresolved conflict",
                        "unresolved conflicts"
                    )
                )
            } else {
                String::new()
            }
        ),
        if j.unresolved_conflicts > 0 {
            Tone::Warn
        } else {
            Tone::Neutral
        },
    ));
    rows.push(kv(
        "Intents",
        format!(
            "{} · sync {} · mount {}",
            if f.intents.desired_running {
                "run"
            } else {
                "stopped"
            },
            if f.intents.sync_enabled {
                "on"
            } else {
                "paused"
            },
            if f.intents.mount_enabled { "on" } else { "off" }
        ),
        Tone::Neutral,
    ));
    rows.push(kv(
        "Identity",
        format!(
            "state {} · repo {} · device {}",
            short(&f.state),
            short(&f.repo),
            short(&f.device)
        ),
        Tone::Muted,
    ));
    rows
}

fn folder_peer_row(p: &FolderPeerSync, names: &Names, now: Now) -> FolderPeerRowModel {
    let m = &p.metadata;
    let work = MetaWork::of(p);
    let mut meta = Vec::new();
    if let Some(n_of_m) = work.n_of_m() {
        meta.push(n_of_m);
    }
    for (n, what) in [
        (m.to_receive, "to receive"),
        (m.to_send, "to send"),
        (m.local_pending, "received, not activated"),
        (m.remote_pending, "pending at the peer"),
    ] {
        if n > 0 {
            meta.push(format!("{} {what}", group(n)));
        }
    }
    meta.push(
        if m.caught_up {
            "caught up"
        } else {
            "not caught up"
        }
        .to_owned(),
    );
    let transfer = |t: &TransferProgress| {
        let t = Transfer::of(t);
        if t.active() {
            t.text()
        } else {
            "idle".to_owned()
        }
    };
    FolderPeerRowModel {
        installation: p.installation.clone(),
        name: match &p.label {
            Some(label) => label.clone(),
            None => names.peer(&p.installation),
        },
        direction: direction_name(p.direction).to_owned(),
        state: folder_peer_chip(p, names, now),
        to_send: m.to_send,
        to_receive: m.to_receive,
        local_pending: m.local_pending,
        remote_pending: m.remote_pending,
        blocked: m.blocked,
        quarantined: m.quarantined,
        rejected: m.rejected,
        caught_up: m.caught_up,
        metadata: meta.join(" · "),
        down: transfer(&p.objects_down),
        up: transfer(&p.objects_up),
        last_progress: p
            .last_progress_ms
            .map(|at| ago(now, at))
            .unwrap_or_else(|| "never".to_owned()),
        last_exchange: m
            .last_exchange_ms
            .map(|at| ago(now, at))
            .unwrap_or_else(|| "never".to_owned()),
        error: p
            .error
            .as_ref()
            .filter(|record| !record.is_resolved())
            .map(|record| error_line(record, names, now)),
    }
}

/// The state chip of one (folder, peer) pair.
pub fn folder_peer_chip(p: &FolderPeerSync, names: &Names, now: Now) -> Chip {
    use FolderPeerState as S;
    match p.state {
        S::NotShared => Chip::new("Not shared", Tone::Muted),
        S::Offline => Chip::new(capitalized(&peer_wait_reason(p, names, now)), Tone::Muted),
        S::Connecting => Chip::new("Connecting…", Tone::Progress),
        S::Incompatible => Chip::new("Peer needs upgrade", Tone::Error),
        S::Unauthorized => Chip::new("Unauthorized — re-pair", Tone::Error),
        S::WaitingForWorker => Chip::new("Waiting for the local worker", Tone::Muted),
        S::WaitingForRemote => Chip::new("Waiting for the peer's folder", Tone::Warn),
        S::Paused => Chip::new("Paused", Tone::Muted),
        S::Metadata => Chip::new(MetaWork::of(p).text("Syncing metadata"), Tone::Progress),
        S::Downloading => Chip::new(
            join_nonempty("Downloading", &Transfer::of(&p.objects_down).text()),
            Tone::Progress,
        ),
        S::Uploading => Chip::new(
            join_nonempty("Uploading", &Transfer::of(&p.objects_up).text()),
            Tone::Progress,
        ),
        S::Syncing => Chip::new("Syncing", Tone::Progress),
        S::Blocked => {
            let m = &p.metadata;
            let reason = blocked_text(m.blocked, m.quarantined, m.rejected)
                .or_else(|| p.error.as_ref().map(|e| e.error.code.clone()));
            Chip::new(
                match reason {
                    Some(reason) => format!("Blocked — {reason}"),
                    None => "Blocked".to_owned(),
                },
                Tone::Error,
            )
        }
        S::Backoff => Chip::new(
            match retry_at(p, names, now) {
                Some(at) => format!("Backoff (retry in {})", format_duration(at - now.ms)),
                None => "Backoff".to_owned(),
            },
            Tone::Warn,
        ),
        S::Stalled => {
            let since = p.last_progress_ms.or(p.stalled_since_ms);
            let mut text = match since.filter(|since| now.ms >= *since) {
                Some(since) => format!("Stalled since {} ago", format_elapsed(now.ms - since)),
                None => "Stalled".to_owned(),
            };
            if let Some(record) = &p.error {
                text.push_str(" — ");
                text.push_str(&culprit(record));
            }
            Chip::new(text, Tone::Error)
        }
        S::UpToDate => Chip::new("Up to date", Tone::Ok),
    }
}

fn activation_rows(f: &FolderStatus, now: Now, rate: Option<f64>) -> Vec<KeyValue> {
    let i = &f.activation.inbox;
    let c = &f.activation.counters;
    let mut rows = Vec::new();
    let pending = i.pending_active();
    rows.push(if pending > 0 {
        let detail: Vec<String> = [
            (i.ready, "ready"),
            (i.waiting, "waiting for parents"),
            (i.retry, "retrying"),
            (i.unclassified, "unclassified"),
        ]
        .into_iter()
        .filter(|(n, _)| *n > 0)
        .map(|(n, what)| format!("{} {what}", group(n)))
        .collect();
        kv(
            "Backlog",
            format!(
                "{} to activate ({})",
                count(pending, "event", "events"),
                detail.join(", ")
            ),
            Tone::Progress,
        )
    } else {
        kv("Backlog", "Nothing to activate", Tone::Ok)
    });
    rows.push(kv(
        "Quarantined",
        group(i.quarantined),
        if i.quarantined > 0 {
            Tone::Error
        } else {
            Tone::Neutral
        },
    ));
    rows.push(kv(
        "Blocked",
        group(i.blocked),
        if i.blocked > 0 {
            Tone::Error
        } else {
            Tone::Neutral
        },
    ));
    rows.push(kv(
        "Activator",
        if c.running { "Running" } else { "Not running" },
        if c.running { Tone::Ok } else { Tone::Muted },
    ));
    rows.push(kv(
        "Activated",
        format!(
            "{} ({} fast, {} slow) · {} retried · {} stale",
            group(c.activated),
            group(c.fast),
            group(c.slow),
            group(c.retried),
            group(c.stale)
        ),
        Tone::Neutral,
    ));
    rows.push(kv(
        "Rate",
        match rate {
            Some(rate) if rate >= 0.05 => format!("{rate:.1} events/s"),
            Some(_) => "idle".to_owned(),
            None => "measuring…".to_owned(),
        },
        Tone::Neutral,
    ));
    rows.push(kv(
        "Slices",
        format!("{} · last {} ms", group(c.slices), c.last_slice_ms),
        Tone::Neutral,
    ));
    if let Some(last) = c.last_activation_at_ms {
        rows.push(kv("Last activation", when(now, last), Tone::Neutral));
    }
    rows.push(match c.starved_since_ms.filter(|since| now.ms >= *since) {
        Some(since) => kv(
            "Starvation",
            format!(
                "The slow path is starved for {}",
                format_elapsed(now.ms - since)
            ),
            Tone::Warn,
        ),
        None => kv("Starvation", "None", Tone::Ok),
    });
    rows
}

fn content_rows(f: &FolderStatus) -> Vec<KeyValue> {
    let c = &f.content;
    vec![
        kv(
            "Current files",
            format!(
                "{} of {} available",
                group(c.current_available),
                count(c.current_objects, "file", "files")
            ),
            if c.current_missing > 0 {
                Tone::Progress
            } else {
                Tone::Ok
            },
        ),
        kv(
            "Missing",
            format!(
                "{} ({})",
                count(c.current_missing, "file", "files"),
                format_bytes(c.current_missing_bytes)
            ),
            if c.current_missing > 0 {
                Tone::Warn
            } else {
                Tone::Neutral
            },
        ),
        kv(
            "Downloading",
            format!(
                "{} ({} so far)",
                count(c.downloading, "file", "files"),
                format_bytes(c.partial_bytes)
            ),
            Tone::Neutral,
        ),
        kv("Requested", group(c.requested), Tone::Neutral),
        kv(
            "Shared history",
            format!(
                "{} of {} objects available ({} missing)",
                group(c.shared_available),
                group(c.shared_objects),
                format_bytes(c.shared_missing_bytes)
            ),
            Tone::Neutral,
        ),
        kv(
            "Complete",
            if c.complete { "Yes" } else { "No" },
            if c.complete { Tone::Ok } else { Tone::Neutral },
        ),
    ]
}

/// One missing item of a folder's Content tab, from the worker's `status`
/// reply (`content.items`: `{path, state, downloaded?, size?, error?}`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MissingItem {
    pub path: String,
    pub state: String,
    /// `12.0 KiB of 2.0 MiB`, or `2.0 MiB`.
    pub progress: String,
    pub error: String,
}

pub fn missing_items(worker_status: &Value) -> Vec<MissingItem> {
    worker_status["content"]["items"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|item| {
            let size = item["size"].as_u64();
            let downloaded = item["downloaded"].as_u64();
            MissingItem {
                path: item["path"].as_str().unwrap_or("file").to_owned(),
                state: item["state"].as_str().unwrap_or("missing").to_owned(),
                progress: match (downloaded, size) {
                    (Some(done), Some(size)) if done > 0 => {
                        format!("{} of {}", format_bytes(done), format_bytes(size))
                    }
                    (_, Some(size)) => format_bytes(size),
                    _ => String::new(),
                },
                error: item["error"]
                    .as_str()
                    .map(|e| clip(e, MESSAGE_CHARS))
                    .unwrap_or_default(),
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Daemon header, overview, activity

/// The header: daemon identity, uptime, log, and the global errors banner.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DaemonModel {
    /// `Telekinesis 0.1.0`.
    pub title: String,
    pub status: Chip,
    pub installation: String,
    pub uptime: String,
    pub pid: String,
    pub control: String,
    pub network: String,
    pub log_path: Option<String>,
    /// Open daemon-level errors (and those of peers the snapshot does not
    /// list): the global banner.
    pub errors: Vec<ErrorLine>,
}

pub fn daemon_model(snapshot: &StatusSnapshot, now: Now) -> DaemonModel {
    let d = &snapshot.daemon;
    let names = Names::of(snapshot);
    let uptime = if d.started_at_ms != 0 && now.ms >= d.started_at_ms {
        format_elapsed(now.ms - d.started_at_ms)
    } else {
        String::new()
    };
    let status = if d.stopping {
        Chip::new("Stopping", Tone::Warn)
    } else if uptime.is_empty() {
        Chip::new("Running", Tone::Ok)
    } else {
        Chip::new(format!("Running · up {uptime}"), Tone::Ok)
    };
    let errors = d
        .errors
        .iter()
        .filter(|record| !record.is_resolved())
        .filter(|record| match &record.scope {
            Scope::Peer { installation }
            | Scope::Lane { installation, .. }
            | Scope::FolderPeer { installation, .. } => !snapshot
                .peers
                .iter()
                .any(|p| &p.installation == installation),
            _ => true,
        })
        .map(|record| error_line(record, &names, now))
        .collect();
    DaemonModel {
        title: if d.version.is_empty() {
            "Telekinesis".to_owned()
        } else {
            format!("Telekinesis {}", d.version)
        },
        status,
        installation: short(&d.installation),
        uptime,
        pid: if d.pid == 0 {
            String::new()
        } else {
            d.pid.to_string()
        },
        control: d.control.clone(),
        network: d
            .network
            .clone()
            .unwrap_or_else(|| "not listening".to_owned()),
        log_path: d.log_path.clone(),
        errors,
    }
}

/// The numbers of the overview cards.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Overview {
    pub folders: usize,
    pub mounted: usize,
    pub up_to_date: usize,
    pub syncing: usize,
    pub attention: usize,
    pub peers: usize,
    pub peers_online: usize,
    pub inbound_only: usize,
    pub rate_down_bps: u64,
    pub rate_up_bps: u64,
    pub conflicts: u64,
}

pub fn overview(snapshot: &StatusSnapshot) -> Overview {
    use FolderSyncState as F;
    let f = &snapshot.folders;
    Overview {
        folders: f.len(),
        mounted: f.iter().filter(|f| f.mount.state.is_attached()).count(),
        up_to_date: f.iter().filter(|f| f.sync.state == F::UpToDate).count(),
        syncing: f
            .iter()
            .filter(|f| {
                matches!(
                    f.sync.state,
                    F::InitialSync | F::Metadata | F::Downloading | F::Uploading | F::Syncing
                )
            })
            .count(),
        attention: f
            .iter()
            .filter(|f| matches!(f.sync.state, F::NeedsAttention | F::Stalled | F::Blocked))
            .count(),
        peers: snapshot.peers.len(),
        peers_online: snapshot
            .peers
            .iter()
            .filter(|p| p.state == PeerConnState::Online)
            .count(),
        inbound_only: snapshot
            .peers
            .iter()
            .filter(|p| p.endpoint.is_none() || p.direction == Direction::Inbound)
            .count(),
        rate_down_bps: snapshot.peers.iter().map(|p| p.rate_down_bps).sum(),
        rate_up_bps: snapshot.peers.iter().map(|p| p.rate_up_bps).sum(),
        conflicts: f.iter().map(|f| f.journal.unresolved_conflicts).sum(),
    }
}

/// One activity entry.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ActivityRowModel {
    pub at_ms: u64,
    /// `14:03:07`.
    pub time: String,
    /// `3 min ago`.
    pub ago: String,
    pub kind: String,
    /// `Photos / office-nas`.
    pub subject: String,
    pub message: String,
    pub tone: Tone,
}

/// The activity ring, newest first, at most `limit` entries, optionally
/// only those of folder `state`.
pub fn activity_rows(snapshot: &StatusSnapshot, folder: Option<&str>) -> Vec<ActivityRowModel> {
    activity_rows_at(snapshot, Now::of(snapshot), folder, 500)
}

pub fn activity_rows_at(
    snapshot: &StatusSnapshot,
    now: Now,
    folder: Option<&str>,
    limit: usize,
) -> Vec<ActivityRowModel> {
    let names = Names::of(snapshot);
    let mut entries: Vec<_> = snapshot
        .activity
        .iter()
        .filter(|entry| folder.is_none_or(|state| entry.folder.as_deref() == Some(state)))
        .collect();
    entries.sort_by_key(|entry| std::cmp::Reverse(entry.at_ms));
    entries
        .into_iter()
        .take(limit)
        .map(|entry| {
            let subject = match (&entry.folder, &entry.peer) {
                (Some(f), Some(p)) => format!("{} / {}", names.folder(f), names.peer(p)),
                (Some(f), None) => names.folder(f),
                (None, Some(p)) => names.peer(p),
                (None, None) => String::new(),
            };
            let lower = entry.kind.to_lowercase();
            let tone = if lower.contains("error") || lower.contains("fail") {
                Tone::Error
            } else if lower.contains("resolved") || lower.contains("ok") {
                Tone::Ok
            } else if lower.contains("warn") || lower.contains("stall") {
                Tone::Warn
            } else {
                Tone::Neutral
            };
            ActivityRowModel {
                at_ms: entry.at_ms,
                time: clock(now, entry.at_ms),
                ago: ago(now, entry.at_ms),
                kind: entry.kind.clone(),
                subject,
                message: clip(&entry.message, MESSAGE_CHARS),
                tone,
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// CCR-WP2.1-7 display adapter

/// Keeps a settled mount state on screen across a stale heartbeat
/// (CCR-WP2.1-7, display side).
///
/// The hub takes a folder's mount from every worker heartbeat, including
/// one requested before the controller's `mount-attach` result, so for up to
/// one heartbeat (1 s) after the controller shows `Attached` or
/// `AttachFailed` the snapshot can say `attaching` or `detached` again. The
/// fix belongs in the hub (prefer the FolderView's mount over an older
/// heartbeat, which needs the mount generation in `FolderView`: CCR-WP3.4-1).
/// Until then the desktop keeps showing the settled state (`Attached`,
/// `AttachFailed`, `Lost`) while a regression to a transitional or detached
/// state is younger than [`MountSmoother::HOLD_MS`], within the same worker
/// instance, with the mount still wanted (`mount_enabled`, a path, no user
/// Detach). A regression that lasts longer is real and shown; a local mount
/// or lifecycle command releases the hold at once ([`MountSmoother::release`]).
#[derive(Clone, Debug, Default)]
pub struct MountSmoother {
    held: HashMap<String, Held>,
}

#[derive(Clone, Debug)]
struct Held {
    shown: MountStatus,
    instance: Option<String>,
    regressed_since_ms: Option<u64>,
}

impl MountSmoother {
    /// A heartbeat interval and a half.
    pub const HOLD_MS: u64 = 1_500;

    fn settled(state: &MountState) -> bool {
        matches!(
            state,
            MountState::Attached { .. } | MountState::AttachFailed { .. } | MountState::Lost { .. }
        )
    }

    /// Rewrites `snapshot`'s mount states in place (idempotent for the same
    /// snapshot and `now_ms`; call it again on every render).
    pub fn smooth(&mut self, snapshot: &mut StatusSnapshot, now_ms: u64) {
        self.held
            .retain(|state, _| snapshot.folders.iter().any(|f| &f.state == state));
        for folder in &mut snapshot.folders {
            let mount = &folder.mount;
            if Self::settled(&mount.state) {
                self.held.insert(
                    folder.state.clone(),
                    Held {
                        shown: mount.clone(),
                        instance: folder.lifecycle.instance.clone(),
                        regressed_since_ms: None,
                    },
                );
                continue;
            }
            let Some(held) = self.held.get_mut(&folder.state) else {
                continue;
            };
            let wanted = folder.lifecycle.phase == LifecyclePhase::Running
                && folder.intents.mount_enabled
                && mount.path.is_some()
                && mount.path == held.shown.path
                && mount.intent != MountIntent::Detach
                && folder.lifecycle.instance == held.instance;
            if !wanted {
                self.held.remove(&folder.state);
                continue;
            }
            let since = *held.regressed_since_ms.get_or_insert(now_ms);
            if now_ms.saturating_sub(since) <= Self::HOLD_MS {
                folder.mount = held.shown.clone();
            } else {
                self.held.remove(&folder.state);
            }
        }
    }

    /// Forgets folder `state`'s held mount (after a local command on it).
    pub fn release(&mut self, state: &str) {
        self.held.remove(state);
    }
}

// ---------------------------------------------------------------------------
// Activation rate

/// The activation rate of each folder, from the `activated` counter across
/// snapshots (a 10 s window; a counter that goes down restarts it).
#[derive(Clone, Debug, Default)]
pub struct RateMeter {
    samples: HashMap<String, VecDeque<(u64, u64)>>,
}

impl RateMeter {
    pub const WINDOW_MS: u64 = 10_000;

    pub fn observe(&mut self, snapshot: &StatusSnapshot) {
        self.samples
            .retain(|state, _| snapshot.folders.iter().any(|f| &f.state == state));
        for folder in &snapshot.folders {
            let value = folder.activation.counters.activated;
            let samples = self.samples.entry(folder.state.clone()).or_default();
            if samples
                .back()
                .is_some_and(|(at, before)| *before > value || *at > snapshot.at_ms)
            {
                samples.clear();
            }
            if samples.back().is_some_and(|(at, _)| *at == snapshot.at_ms) {
                continue;
            }
            samples.push_back((snapshot.at_ms, value));
            while samples.len() > 2
                && samples
                    .front()
                    .is_some_and(|(at, _)| snapshot.at_ms.saturating_sub(*at) > Self::WINDOW_MS)
            {
                samples.pop_front();
            }
        }
    }

    /// Events per second over the window, when two samples exist.
    pub fn per_second(&self, state: &str) -> Option<f64> {
        let samples = self.samples.get(state)?;
        let (first, last) = (samples.front()?, samples.back()?);
        let span = last.0.checked_sub(first.0).filter(|span| *span > 0)?;
        Some(last.1.saturating_sub(first.1) as f64 * 1000.0 / span as f64)
    }
}

// ---------------------------------------------------------------------------
// Background: busy tracking, commands, status subscription

/// What is running per row: at most one command per key (a folder state, a
/// peer, or `"global"`), so a row shows one busy spinner and refuses a
/// second click.
#[derive(Clone, Debug, Default)]
pub struct Busy {
    running: HashMap<String, (u64, String)>,
}

impl Busy {
    /// Marks `key` busy with command `id`; `false` if it already is.
    pub fn begin(&mut self, key: &str, id: u64, label: &str) -> bool {
        if self.running.contains_key(key) {
            return false;
        }
        self.running.insert(key.to_owned(), (id, label.to_owned()));
        true
    }

    /// Clears `key` if command `id` is the one running there.
    pub fn finish(&mut self, key: &str, id: u64) {
        if self
            .running
            .get(key)
            .is_some_and(|(running, _)| *running == id)
        {
            self.running.remove(key);
        }
    }

    /// The caption of the command running for `key`.
    pub fn label(&self, key: &str) -> Option<&str> {
        self.running.get(key).map(|(_, label)| label.as_str())
    }

    pub fn is_busy(&self, key: &str) -> bool {
        self.running.contains_key(key)
    }

    pub fn any(&self) -> bool {
        !self.running.is_empty()
    }
}

/// One connection that runs management actions (the desktop's is a
/// `ControlClient`). Errors are the daemon's message or the transport's.
pub trait CommandTransport: Send {
    fn call(&mut self, action: &Action) -> Result<Value, String>;
}

/// A command for the pool.
#[derive(Clone, Debug)]
pub struct Command {
    pub id: u64,
    /// The busy key (folder state, peer, `"global"`).
    pub key: String,
    pub action: Action,
}

/// A finished command.
#[derive(Debug)]
pub struct CommandOutcome {
    pub command: Command,
    pub result: Result<Value, String>,
    pub elapsed: Duration,
}

/// Makes a fresh command connection (lazily, per worker thread).
pub type Connect = Arc<dyn Fn() -> Box<dyn CommandTransport> + Send + Sync>;
/// Receives each finished command (on a pool thread).
pub type Done = Arc<dyn Fn(CommandOutcome) + Send + Sync>;

/// Commands on their own threads and connections (§7.2: commands never wait
/// behind the subscription). Each worker thread owns one connection, made on
/// first use; `workers` commands run at once, so a 15 s Stop never delays a
/// Start of another folder.
pub struct CommandPool {
    tx: Option<mpsc::Sender<Command>>,
}

impl CommandPool {
    pub fn start(workers: usize, connect: Connect, done: Done) -> Self {
        let (tx, rx) = mpsc::channel::<Command>();
        let rx = Arc::new(Mutex::new(rx));
        for index in 0..workers.max(1) {
            let rx = Arc::clone(&rx);
            let connect = Arc::clone(&connect);
            let done = Arc::clone(&done);
            let _ = thread::Builder::new()
                .name(format!("gui-command-{index}"))
                .spawn(move || {
                    let mut transport: Option<Box<dyn CommandTransport>> = None;
                    loop {
                        let next = {
                            let rx = crate::util::lock::lock(&rx);
                            rx.recv()
                        };
                        let Ok(command) = next else {
                            return;
                        };
                        let started = Instant::now();
                        let transport = transport.get_or_insert_with(|| connect());
                        let result = transport.call(&command.action);
                        done(CommandOutcome {
                            command,
                            result,
                            elapsed: started.elapsed(),
                        });
                    }
                });
        }
        Self { tx: Some(tx) }
    }

    /// Queues `command`; gives it back if the pool is gone.
    pub fn submit(&self, command: Command) -> Result<(), Box<Command>> {
        match &self.tx {
            Some(tx) => tx.send(command).map_err(|e| Box::new(e.0)),
            None => Err(Box::new(command)),
        }
    }
}

impl Drop for CommandPool {
    fn drop(&mut self) {
        // The workers end after their current call (never joined: a call may
        // be a 15 s Stop).
        self.tx.take();
    }
}

/// Why a subscription call failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransportError {
    /// No daemon answers the control endpoint (no discovery file, refused):
    /// a candidate for a respawn.
    DaemonGone(String),
    /// The call failed after reaching a daemon (timeout, lost connection).
    Failed(String),
}

impl TransportError {
    pub fn message(&self) -> &str {
        match self {
            TransportError::DaemonGone(message) | TransportError::Failed(message) => message,
        }
    }
}

/// The subscription connection (`subscribe(since, wait)` with a deadline of
/// `wait` plus slack). After an error the next call reconnects.
pub trait StatusTransport: Send {
    fn subscribe(&mut self, since: u64, wait: Duration) -> Result<StatusSnapshot, TransportError>;
    /// Drops the connection.
    fn disconnect(&mut self) {}
}

/// Starts the daemon (the desktop spawns `tkfs orchestrator` with its output
/// in `DATA/logs/orchestrator.log`).
pub trait DaemonStarter: Send {
    /// Returns where the daemon's output goes.
    fn start(&mut self) -> Result<String, String>;
}

/// The feed is down: why, and when it tries again.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FeedDown {
    pub error: String,
    pub attempt: u32,
    pub retry_in_ms: u64,
    pub daemon_gone: bool,
    /// A daemon was just started (its log path).
    pub started: Option<String>,
    pub start_error: Option<String>,
    /// Respawning is off: after a Shutdown from the desktop, or because the
    /// daemon said it was shutting down and went away (see [`Feed`]).
    pub respawn_off: bool,
    /// The daemon's last word was that it is shutting down (a `stopping`
    /// snapshot, or `ORCHESTRATOR_STOPPING`): someone stopped it on purpose,
    /// so it is not started again until the user asks.
    pub deliberate: bool,
}

#[derive(Clone, Debug)]
pub enum FeedUpdate {
    Snapshot(Arc<StatusSnapshot>),
    Down(FeedDown),
}

/// What a daemon answers while it shuts down (§3.6).
pub const STOPPING_CODE: &str = "ORCHESTRATOR_STOPPING";

/// [`Feed`] timing.
#[derive(Clone, Copy, Debug)]
pub struct FeedOptions {
    /// `subscribe` long-poll wait (§14 WP3.4: 10 s; the deadline adds 5 s).
    pub wait: Duration,
    pub backoff_base: Duration,
    pub backoff_cap: Duration,
    /// At most one daemon start per this interval.
    pub start_every: Duration,
}

impl Default for FeedOptions {
    fn default() -> Self {
        Self {
            wait: Duration::from_secs(10),
            backoff_base: Duration::from_millis(250),
            backoff_cap: Duration::from_secs(5),
            start_every: Duration::from_secs(15),
        }
    }
}

/// Who may start a daemon, shared by the desktop and the feed thread.
#[derive(Debug, Default)]
struct RespawnGate {
    /// The last daemon that answered (pid, start time).
    daemon: Option<(u32, u64)>,
    /// The desktop shut a daemon down ([`Feed::set_respawn`]): the daemon it
    /// shut down (`None`: none had answered yet, so the next one to answer is
    /// taken as it). No daemon is started until the user asks, or until a
    /// different daemon answers (one started elsewhere, which is the one to
    /// look after from then on).
    hold: Option<Option<(u32, u64)>>,
    /// The user asked for a daemon now ([`Feed::start_daemon`]); moot once a
    /// daemon that is not shutting down answers.
    start_now: bool,
}

fn gate(gate: &Mutex<RespawnGate>) -> MutexGuard<'_, RespawnGate> {
    gate.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// The status subscription (§7.2, §14 WP3.4 task 1): one thread long-polls
/// `subscribe(since, wait)` and hands every newer snapshot to `sink`; on a
/// failure it reconnects with backoff (250 ms → 5 s), and when no daemon
/// answers it starts one (at most once per `start_every`).
///
/// It never starts one that was shut down on purpose:
/// * after a Shutdown from the desktop ([`Feed::set_respawn`]`(false)`),
///   until the user asks for one ([`Feed::start_daemon`]) or a different
///   daemon answers (one started from the CLI, say);
/// * once the daemon said it was shutting down (a `stopping` snapshot or an
///   `ORCHESTRATOR_STOPPING` reply: a shutdown from the CLI is not undone by
///   an open window) and went away, until a daemon that is not shutting down
///   answers again or the user asks for one.
///
/// After any failure the next call asks for `since = 0`: the daemon may have
/// restarted with fresh generations. A `sink` that returns `false` ends the
/// thread.
pub struct Feed {
    cancel: Cancel,
    gate: Arc<Mutex<RespawnGate>>,
}

impl Feed {
    pub fn start(
        mut transport: Box<dyn StatusTransport>,
        mut starter: Option<Box<dyn DaemonStarter>>,
        options: FeedOptions,
        mut sink: Box<dyn FnMut(FeedUpdate) -> bool + Send>,
    ) -> Self {
        let cancel = Cancel::new();
        let shared = Arc::new(Mutex::new(RespawnGate::default()));
        let feed = Self {
            cancel: cancel.clone(),
            gate: Arc::clone(&shared),
        };
        let _ = thread::Builder::new()
            .name("gui-subscribe".into())
            .spawn(move || {
                let mut since = 0u64;
                let mut daemon: Option<(u32, u64)> = None;
                let mut attempt = 0u32;
                let mut last_start: Option<Instant> = None;
                // The daemon's last word was that it is shutting down.
                let mut stopping = false;
                // It said so and then went away: it stays down until a
                // daemon answers again or the user asks for one.
                let mut stayed_down = false;
                while !cancel.is_cancelled() {
                    match transport.subscribe(since, options.wait) {
                        Ok(snapshot) => {
                            attempt = 0;
                            stopping = snapshot.daemon.stopping;
                            let identity = (snapshot.daemon.pid, snapshot.daemon.started_at_ms);
                            {
                                let mut g = gate(&shared);
                                match g.hold {
                                    Some(None) => g.hold = Some(Some(identity)),
                                    Some(Some(held)) if held != identity => g.hold = None,
                                    _ => {}
                                }
                                g.daemon = Some(identity);
                                if !stopping {
                                    // A daemon is up: an earlier shutdown is
                                    // over and a pending start is moot.
                                    g.start_now = false;
                                    stayed_down = false;
                                }
                            }
                            let fresh = daemon != Some(identity) || snapshot.generation != since;
                            daemon = Some(identity);
                            since = snapshot.generation;
                            if fresh && !sink(FeedUpdate::Snapshot(Arc::new(snapshot))) {
                                return;
                            }
                        }
                        Err(error) => {
                            transport.disconnect();
                            since = 0;
                            attempt = attempt.saturating_add(1);
                            let gone = matches!(error, TransportError::DaemonGone(_));
                            if error.message().contains(STOPPING_CODE) {
                                stopping = true;
                            }
                            let (forced, held) = {
                                let mut g = gate(&shared);
                                (std::mem::take(&mut g.start_now), g.hold.is_some())
                            };
                            if forced {
                                stopping = false;
                                stayed_down = false;
                            } else if stopping && gone {
                                // Shut down on purpose: stay down.
                                stayed_down = true;
                            }
                            let respawn_on = !held && !stayed_down;
                            let due = forced
                                || last_start.is_none_or(|at| at.elapsed() >= options.start_every);
                            let mut down = FeedDown {
                                error: error.message().to_owned(),
                                attempt,
                                daemon_gone: gone,
                                respawn_off: !respawn_on,
                                deliberate: stopping,
                                ..FeedDown::default()
                            };
                            if gone
                                && respawn_on
                                && due
                                && let Some(starter) = starter.as_mut()
                            {
                                last_start = Some(Instant::now());
                                match starter.start() {
                                    Ok(log) => down.started = Some(log),
                                    Err(error) => down.start_error = Some(error),
                                }
                            }
                            let delay = if down.started.is_some() {
                                options.backoff_base
                            } else {
                                let doubled = options
                                    .backoff_base
                                    .saturating_mul(1u32 << attempt.saturating_sub(1).min(16));
                                doubled.min(options.backoff_cap)
                            };
                            down.retry_in_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
                            if !sink(FeedUpdate::Down(down)) {
                                return;
                            }
                            // Sleep in slices, so `start_daemon` is prompt.
                            let until = Instant::now() + delay;
                            while Instant::now() < until {
                                if gate(&shared).start_now {
                                    break;
                                }
                                let slice = until
                                    .saturating_duration_since(Instant::now())
                                    .min(Duration::from_millis(100));
                                if cancel.sleep(slice) {
                                    return;
                                }
                            }
                        }
                    }
                }
            });
        feed
    }

    /// `false`: the desktop is shutting the daemon down, so a daemon that is
    /// gone is not started again until [`Feed::start_daemon`] or until a
    /// different daemon answers. `true` lifts that (the Shutdown failed).
    pub fn set_respawn(&self, on: bool) {
        let mut g = gate(&self.gate);
        g.hold = if on { None } else { Some(g.daemon) };
    }

    /// Lifts every reason not to start a daemon and starts one at the next
    /// failure, without waiting for the backoff.
    pub fn start_daemon(&self) {
        let mut g = gate(&self.gate);
        g.hold = None;
        g.start_now = true;
    }

    /// Ends the thread at its next wake-up (a long poll in flight finishes
    /// first; its result is dropped).
    pub fn stop(&self) {
        self.cancel.cancel();
    }
}

impl Drop for Feed {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
