//! Owned by WP1.11.
//!
//! Pure status derivation rules used by the [`StatusHub`](super::hub::StatusHub)
//! (REVAMP_PLAN §3.4, §7.1, §8). Nothing here reads a clock, takes a lock or
//! allocates shared state: every function maps plain inputs to an answer, so
//! the rules are table-tested in `tests/status_hub.rs`.
//!
//! * [`peer_state`]: the state of one (folder, peer) pair ([`FolderPeerState`]).
//! * [`folder_state`]: the folder aggregate ([`FolderSyncState`]), first match
//!   wins over the §7.1 precedence table; [`folder_summary`] is its text.
//! * [`peer_conn_state`]: the connection badge of a peer ([`PeerConnState`]).
//! * [`stall`]: the stall detector (§7.1 "Stall").
//! * [`Ewma`], [`TransferRate`] and [`eta`]: transfer rates (10 s and 60 s
//!   windows, each transfer spread over its duration) and ETA.
//! * [`dedupe`] and [`error_visible`]: error deduplication and the display
//!   policy (§7.1 "Error display", §8).
use crate::model::{
    errors::{ErrorClass, ErrorRecord, ErrorWire, Scope},
    health::{ContentCounters, MountState},
    orchestrator::EntryState,
    status::{
        FolderPeerState, FolderSyncState, GateKind, LaneState, LifecyclePhase, PeerConnState,
        policy,
    },
};
use std::time::Duration;

/// `FOLDER_SYNC_PAUSED` is Transient but shown as the `Paused` state, never as
/// an error (§8 †).
pub const PAUSED_CODE: &str = "FOLDER_SYNC_PAUSED";
/// The acceptor answers this while its folder is not Running (§3.8).
pub const REMOTE_STARTING_CODE: &str = "REMOTE_WORKER_STARTING";
/// An objects exchange in which the peer moved at least this many bytes to us
/// (or a completed object) is evidence that the peer supplies objects
/// ([`may_supply`]). It is not progress: only completed objects and the
/// content counters are (partial progress on a large object shows as its
/// spool growing), so a transfer that moves bytes but never completes or
/// keeps anything still stalls.
pub const PROGRESS_MIN_BYTES: u64 = 64 * 1024;

/// Whether `code` says the remote *folder* is not serving
/// (`REMOTE_WORKER_STARTING`, `FOLDER_SYNC_PAUSED`) rather than that one op
/// failed: the pair waits for the remote, and any success with that peer for
/// that repo, whatever the op, proves the remote folder serves again.
pub fn remote_waiting_code(code: &str) -> bool {
    code == REMOTE_STARTING_CODE || code == PAUSED_CODE
}

// ---------------------------------------------------------------------------
// Errors: dedupe and display policy

/// Consecutive failures in one error slot since its last success. A different
/// code in the same slot continues the streak (only a success ends it).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Streak {
    /// Failures counted toward the display policy (Contention included: it is
    /// timed, not counted).
    pub failures: u64,
    /// When the streak began.
    pub since_ms: u64,
}

impl Streak {
    pub fn start(at_ms: u64) -> Self {
        Self {
            failures: 1,
            since_ms: at_ms,
        }
    }

    /// One more failure.
    pub fn bump(self) -> Self {
        Self {
            failures: self.failures.saturating_add(1),
            since_ms: self.since_ms,
        }
    }
}

/// Deduplicates one error occurrence against the slot's open record
/// (`ErrorRecord::observe` semantics, §8): the same open code bumps `count`
/// and `last_at_ms`; a different code starts a new record. Returns the open
/// record and, when a different code replaced it, the previous record marked
/// resolved at `at_ms` (it belongs in the history).
pub fn dedupe(
    current: Option<ErrorRecord>,
    error: ErrorWire,
    scope: Scope,
    at_ms: u64,
) -> (ErrorRecord, Option<ErrorRecord>) {
    let next_retry = error.retry_after_ms.map(|ms| at_ms.saturating_add(ms));
    match current {
        Some(mut open) if open.error.code == error.code && !open.is_resolved() => {
            open.observe(error, at_ms);
            open.scope = scope;
            open.next_retry_at_ms = next_retry;
            (open, None)
        }
        previous => {
            let mut record = ErrorRecord::new(error, scope, at_ms);
            record.next_retry_at_ms = next_retry;
            let superseded = previous.filter(|old| !old.is_resolved()).map(|mut old| {
                old.resolve(at_ms);
                old
            });
            (record, superseded)
        }
    }
}

/// The §7.1 display policy for an open error:
/// * `FOLDER_SYNC_PAUSED` is never shown as an error (it is the Paused state);
/// * Contention is hidden until the streak has lasted 30 s;
/// * Transient is shown from the 3rd consecutive failure on;
/// * NeedsUserAction, Incompatible and Fatal are always shown.
pub fn error_visible(class: ErrorClass, code: &str, streak: Streak, now_ms: u64) -> bool {
    if code == PAUSED_CODE {
        return false;
    }
    match class {
        ErrorClass::Contention => {
            now_ms.saturating_sub(streak.since_ms) >= ms(policy::CONTENTION_VISIBLE_AFTER)
        }
        ErrorClass::Transient => streak.failures >= policy::TRANSIENT_VISIBLE_ATTEMPTS,
        ErrorClass::NeedsUserAction | ErrorClass::Incompatible | ErrorClass::Fatal => true,
    }
}

/// When an open error becomes visible under [`error_visible`] (`None`: only
/// more failures can make it visible, or it never will be).
pub fn visible_at(class: ErrorClass, code: &str, streak: Streak) -> Option<u64> {
    if code == PAUSED_CODE {
        return None;
    }
    match class {
        ErrorClass::Contention => Some(
            streak
                .since_ms
                .saturating_add(ms(policy::CONTENTION_VISIBLE_AFTER)),
        ),
        ErrorClass::Transient => {
            (streak.failures >= policy::TRANSIENT_VISIBLE_ATTEMPTS).then_some(streak.since_ms)
        }
        ErrorClass::NeedsUserAction | ErrorClass::Incompatible | ErrorClass::Fatal => {
            Some(streak.since_ms)
        }
    }
}

/// What the open errors of one (folder, peer) pair say about its state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ErrorFlags {
    /// A visible Contention or Transient error: the key is retrying (backoff).
    pub retrying: bool,
    /// A visible NeedsUserAction or Fatal error: the key does not retry.
    pub hard: bool,
    /// An Incompatible error (always visible).
    pub incompatible: bool,
    /// The remote folder is not serving: `REMOTE_WORKER_STARTING` or
    /// `FOLDER_SYNC_PAUSED` is open (shown or not).
    pub remote_waiting: bool,
}

impl ErrorFlags {
    /// Folds one open error into the flags.
    pub fn with(mut self, class: ErrorClass, code: &str, visible: bool) -> Self {
        if remote_waiting_code(code) {
            self.remote_waiting = true;
            return self;
        }
        match class {
            ErrorClass::Incompatible => self.incompatible = true,
            ErrorClass::Contention | ErrorClass::Transient => self.retrying |= visible,
            ErrorClass::NeedsUserAction | ErrorClass::Fatal => self.hard |= visible,
        }
        self
    }
}

// ---------------------------------------------------------------------------
// Per-(folder, peer) state

/// Inputs of [`peer_state`] for one (folder, peer) pair.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PeerInputs {
    /// The peer shares the repo, or may: its catalog lists it (once known,
    /// the catalog decides), or its catalog is unknown and it exchanged the
    /// repo with us (inbound-only peers have no catalog) or, while it cannot
    /// be reached, it is a configured peer (catalogs are not kept across
    /// restarts), or it is the gate source of a received folder.
    pub shared: bool,
    /// The folder intent `sync_enabled`.
    pub sync_enabled: bool,
    /// The local folder lifecycle is Running.
    pub local_running: bool,
    /// The peer's connection badge.
    pub conn: PeerConnState,
    /// The peer's published entry for the repo, when its catalog is known.
    pub remote: Option<EntryState>,
    pub errors: ErrorFlags,
    /// The stall detector fired for this pair.
    pub stalled: bool,
    /// A metadata exchange summary (or feed counters) exists for this pair.
    /// Without one nothing is known yet, which counts as metadata work.
    pub meta_known: bool,
    /// `to_send + to_receive + local_pending + remote_pending`.
    pub meta_work: u64,
    pub down_objects: u64,
    pub down_bytes: u64,
    pub up_objects: u64,
    pub up_bytes: u64,
    /// `blocked + quarantined`: work that cannot progress without
    /// Revalidate (never counted as remaining work). Rejected events are not
    /// in it: Revalidate does not revive them, and a pair whose only
    /// leftovers are rejected events is up to date (WP3.INT re-review).
    pub blocked: u64,
}

impl PeerInputs {
    pub fn has_meta_work(&self) -> bool {
        !self.meta_known || self.meta_work > 0
    }

    pub fn has_down_work(&self) -> bool {
        self.down_bytes > 0 || self.down_objects > 0
    }

    pub fn has_up_work(&self) -> bool {
        self.up_bytes > 0 || self.up_objects > 0
    }

    /// Any remaining (progressable) work.
    pub fn work_remaining(&self) -> bool {
        self.has_meta_work() || self.has_down_work() || self.has_up_work()
    }

    /// Whether the pair is in a position to make progress, so a lack of
    /// progress means a stall: shared, sync enabled, both folders serving and
    /// the peer connected. (Work remaining is checked separately.)
    pub fn can_progress(&self) -> bool {
        self.shared
            && self.sync_enabled
            && self.local_running
            && self.conn == PeerConnState::Online
            && !remote_waiting(self)
    }
}

fn remote_waiting(i: &PeerInputs) -> bool {
    i.errors.remote_waiting
        || matches!(
            i.remote,
            Some(EntryState::Starting | EntryState::Unavailable | EntryState::Paused)
        )
}

/// The transfer state of a set of work kinds: one kind gives its own state,
/// more than one gives `Syncing`, none gives `None`.
pub fn transfer_state(meta: bool, down: bool, up: bool) -> Option<FolderPeerState> {
    match (meta, down, up) {
        (false, false, false) => None,
        (true, false, false) => Some(FolderPeerState::Metadata),
        (false, true, false) => Some(FolderPeerState::Downloading),
        (false, false, true) => Some(FolderPeerState::Uploading),
        _ => Some(FolderPeerState::Syncing),
    }
}

/// The state of one (folder, peer) pair; first match wins:
///
/// | # | State | Condition |
/// |---|---|---|
/// | 1 | `NotShared` | the peer does not share the repo |
/// | 2 | `Paused` | sync disabled for the folder |
/// | 3 | `Incompatible` | peer incompatible, or an Incompatible error on the pair |
/// | 4 | `Unauthorized` | peer unauthorized or revoked |
/// | 5 | `Offline` | peer offline |
/// | 6 | `Connecting` | peer connecting |
/// | 7 | `Backoff` | peer connection in reconnect backoff |
/// | 8 | `WaitingForWorker` | the local folder is not Running |
/// | 9 | `WaitingForRemote` | the remote entry is starting, unavailable or paused (or the remote answered `REMOTE_WORKER_STARTING`/`FOLDER_SYNC_PAUSED`) |
/// | 10 | `Stalled` | the stall detector fired |
/// | 11 | `Backoff` | a visible Contention/Transient error: the key is retrying |
/// | 12 | `Blocked` | a visible NeedsUserAction/Fatal error: the key does not retry |
/// | 13 | transfer | `Metadata`, `Downloading`, `Uploading`, or `Syncing` if mixed |
/// | 14 | `Blocked` | only blocked or quarantined work remains |
/// | 15 | `UpToDate` | none of the above |
pub fn peer_state(i: &PeerInputs) -> FolderPeerState {
    use FolderPeerState as S;
    if !i.shared {
        return S::NotShared;
    }
    if !i.sync_enabled {
        return S::Paused;
    }
    if i.conn == PeerConnState::Incompatible || i.errors.incompatible {
        return S::Incompatible;
    }
    match i.conn {
        PeerConnState::Unauthorized | PeerConnState::Revoked => return S::Unauthorized,
        PeerConnState::Offline => return S::Offline,
        PeerConnState::Connecting => return S::Connecting,
        PeerConnState::Backoff => return S::Backoff,
        PeerConnState::Online | PeerConnState::Incompatible => {}
    }
    if !i.local_running {
        return S::WaitingForWorker;
    }
    if remote_waiting(i) {
        return S::WaitingForRemote;
    }
    if i.stalled {
        return S::Stalled;
    }
    if i.errors.retrying {
        return S::Backoff;
    }
    if i.errors.hard {
        return S::Blocked;
    }
    if let Some(state) = transfer_state(i.has_meta_work(), i.has_down_work(), i.has_up_work()) {
        return state;
    }
    if i.blocked > 0 {
        return S::Blocked;
    }
    S::UpToDate
}

/// Whether a pair state means "the peer cannot be synced with right now"
/// (rule 10 of [`folder_state`]).
pub fn unreachable(state: FolderPeerState) -> bool {
    use FolderPeerState as S;
    matches!(
        state,
        S::Offline
            | S::Incompatible
            | S::Unauthorized
            | S::Connecting
            | S::WaitingForRemote
            | S::WaitingForWorker
            | S::Backoff
    )
}

/// Whether a pair state is a transfer state.
pub fn is_transfer(state: FolderPeerState) -> bool {
    use FolderPeerState as S;
    matches!(
        state,
        S::Metadata | S::Downloading | S::Uploading | S::Syncing
    )
}

// ---------------------------------------------------------------------------
// Peer connection badge

/// The connection badge of a peer from its lanes and inbound sessions; first
/// match wins: Revoked, Incompatible (any lane), Unauthorized (any lane),
/// Online (any lane Ready or any inbound session), Connecting (any lane
/// dialing or handshaking), Backoff (any lane in backoff), else Offline.
pub fn peer_conn_state(lanes: &[LaneState], inbound_sessions: u32, revoked: bool) -> PeerConnState {
    let any = |state: LaneState| lanes.contains(&state);
    if revoked {
        PeerConnState::Revoked
    } else if any(LaneState::Incompatible) {
        PeerConnState::Incompatible
    } else if any(LaneState::Unauthorized) {
        PeerConnState::Unauthorized
    } else if any(LaneState::Ready) || inbound_sessions > 0 {
        PeerConnState::Online
    } else if any(LaneState::Dialing) || any(LaneState::Handshaking) {
        PeerConnState::Connecting
    } else if any(LaneState::Backoff) {
        PeerConnState::Backoff
    } else {
        PeerConnState::Offline
    }
}

// ---------------------------------------------------------------------------
// Folder aggregate

/// Inputs of [`folder_state`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FolderInputs {
    pub lifecycle: LifecyclePhase,
    pub desired_running: bool,
    pub sync_enabled: bool,
    /// See [`needs_attention`].
    pub needs_attention: bool,
    pub gate: GateKind,
    /// The state of every known peer for this folder; `NotShared` peers are
    /// ignored by every rule.
    pub peers: Vec<FolderPeerState>,
    /// Local inbox rows that cannot activate without Revalidate
    /// (`blocked + quarantined`).
    pub local_blocked: u64,
}

impl FolderInputs {
    /// The states of the peers that share the repo.
    pub fn sharing(&self) -> impl Iterator<Item = FolderPeerState> + '_ {
        self.peers
            .iter()
            .copied()
            .filter(|state| *state != FolderPeerState::NotShared)
    }
}

/// Rule 2 of the §7.1 table: the lifecycle is Failed; a NeedsUserAction or
/// Fatal error is open in the folder's own scopes (Folder, Mount); the mount
/// failed with a NeedsUserAction error; or the gate is Blocked
/// (`GATE_BLOCKED` is NeedsUserAction).
pub fn needs_attention(
    lifecycle: LifecyclePhase,
    gate: GateKind,
    mount: &MountState,
    folder_errors: impl IntoIterator<Item = ErrorClass>,
) -> bool {
    let severe =
        |class: ErrorClass| matches!(class, ErrorClass::NeedsUserAction | ErrorClass::Fatal);
    lifecycle == LifecyclePhase::Failed
        || gate == GateKind::Blocked
        || matches!(mount, MountState::AttachFailed { error, .. } if error.class == ErrorClass::NeedsUserAction)
        || folder_errors.into_iter().any(severe)
}

/// The folder aggregate; first match wins (§7.1, plus rule 3b):
///
/// | # | State | Condition |
/// |---|---|---|
/// | 1 | `Stopped` | lifecycle stopped and not desired |
/// | 2 | `NeedsAttention` | [`needs_attention`] |
/// | 3 | `Paused` | sync disabled |
/// | 3b | `Stopped` | the worker is not Running (starting, restarting, stopping, or stopped while desired): nothing can sync |
/// | 4 | `NotShared` | no peer shares the repo |
/// | 5 | `Stalled` | any sharing peer is stalled |
/// | 6 | `InitialSync` | gate awaiting |
/// | 7 | transfer | any sharing peer transferring: its kind if all agree, else `Syncing` |
/// | 8 | `Blocked` | only blocked, quarantined or rejected work remains (a Blocked peer, or blocked/quarantined local inbox rows) |
/// | 9 | `Backoff` | every sharing peer is in backoff |
/// | 10 | `Offline` | every sharing peer is unreachable ([`unreachable`]) |
/// | 11 | `UpToDate` | none of the above |
///
/// Peers that do not share the repo are ignored.
pub fn folder_state(i: &FolderInputs) -> FolderSyncState {
    use FolderSyncState as F;
    if i.lifecycle == LifecyclePhase::Stopped && !i.desired_running {
        return F::Stopped;
    }
    if i.needs_attention {
        return F::NeedsAttention;
    }
    if !i.sync_enabled {
        return F::Paused;
    }
    if i.lifecycle != LifecyclePhase::Running {
        return F::Stopped;
    }
    let sharing: Vec<FolderPeerState> = i.sharing().collect();
    if sharing.is_empty() {
        return F::NotShared;
    }
    if sharing.contains(&FolderPeerState::Stalled) {
        return F::Stalled;
    }
    if i.gate == GateKind::Awaiting {
        return F::InitialSync;
    }
    let mut transfers = sharing.iter().copied().filter(|s| is_transfer(*s));
    if let Some(first) = transfers.next() {
        let mixed = transfers.any(|state| state != first);
        return match (mixed, first) {
            (false, FolderPeerState::Metadata) => F::Metadata,
            (false, FolderPeerState::Downloading) => F::Downloading,
            (false, FolderPeerState::Uploading) => F::Uploading,
            _ => F::Syncing,
        };
    }
    if sharing.contains(&FolderPeerState::Blocked) || i.local_blocked > 0 {
        return F::Blocked;
    }
    if sharing.iter().all(|s| *s == FolderPeerState::Backoff) {
        return F::Backoff;
    }
    if sharing.iter().all(|s| unreachable(*s)) {
        return F::Offline;
    }
    F::UpToDate
}

/// Facts the folder summary line can mention (all optional).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SummaryFacts {
    /// Lifecycle detail for a folder whose worker is not Running.
    pub lifecycle: Option<String>,
    /// The error that needs attention (`CODE: message`).
    pub attention: Option<String>,
    /// Metadata events left across sharing peers (max over peers).
    pub meta_left: u64,
    pub down_bytes_left: u64,
    pub down_rate_bps: u64,
    pub up_objects_left: u64,
    pub up_bytes_left: u64,
    pub up_rate_bps: u64,
    /// The longest stall among sharing peers.
    pub stalled_for_ms: Option<u64>,
    /// What the stall is blamed on (`CODE subject`).
    pub culprit: Option<String>,
    /// Gate source and its `waiting_on` reasons.
    pub gate_source: Option<String>,
    pub waiting_on: Vec<String>,
    pub blocked: u64,
    pub quarantined: u64,
    pub rejected: u64,
}

/// The one-line summary of a folder aggregate, e.g. `"Downloading · 1 of 2
/// peers · 1.2 GiB left · 3.4 MiB/s · ETA 6 min"`.
pub fn folder_summary(state: FolderSyncState, i: &FolderInputs, f: &SummaryFacts) -> String {
    use FolderSyncState as F;
    let sharing: Vec<FolderPeerState> = i.sharing().collect();
    let total = sharing.len();
    let count =
        |pred: &dyn Fn(FolderPeerState) -> bool| sharing.iter().filter(|s| pred(**s)).count();
    let of = |n: usize| format!("{n} of {total} {}", plural(total as u64, "peer", "peers"));
    match state {
        F::Stopped => match &f.lifecycle {
            Some(detail) => format!("Not syncing: {detail}"),
            None => "Stopped".into(),
        },
        F::NeedsAttention => match &f.attention {
            Some(error) => format!("Needs attention: {error}"),
            None => "Needs attention".into(),
        },
        F::Paused => "Paused".into(),
        F::NotShared => "Not shared with any peer".into(),
        F::Stalled => {
            let mut text = format!(
                "Stalled{} · {}",
                f.stalled_for_ms
                    .map(|ms| format!(" {}", format_elapsed(ms)))
                    .unwrap_or_default(),
                of(count(&|s| s == FolderPeerState::Stalled))
            );
            if let Some(culprit) = &f.culprit {
                text.push_str(" — ");
                text.push_str(culprit);
            }
            text
        }
        F::InitialSync => {
            let mut text = match &f.gate_source {
                Some(source) => format!("Waiting for initial sync from {source}"),
                None => "Waiting for initial sync".into(),
            };
            if !f.waiting_on.is_empty() {
                text.push_str(": ");
                text.push_str(&f.waiting_on.join(", "));
            }
            text
        }
        F::Metadata | F::Downloading | F::Uploading | F::Syncing => {
            let label = match state {
                F::Metadata => "Syncing metadata",
                F::Downloading => "Downloading",
                F::Uploading => "Uploading",
                _ => "Syncing",
            };
            let mut parts = vec![label.to_owned(), of(count(&is_transfer))];
            if f.meta_left > 0 && matches!(state, F::Metadata | F::Syncing) {
                parts.push(format!(
                    "{} left",
                    plural_count(f.meta_left, "event", "events")
                ));
            }
            if f.down_bytes_left > 0 && matches!(state, F::Downloading | F::Syncing) {
                parts.push(format!("{} left", format_bytes(f.down_bytes_left)));
                if f.down_rate_bps > 0 {
                    parts.push(format!("{}/s", format_bytes(f.down_rate_bps)));
                    if let Some(eta) = eta(f.down_bytes_left, f.down_rate_bps) {
                        parts.push(format!("ETA {}", format_duration(eta.saturating_mul(1000))));
                    }
                }
            }
            if (f.up_bytes_left > 0 || f.up_objects_left > 0)
                && matches!(state, F::Uploading | F::Syncing)
            {
                if f.up_objects_left > 0 {
                    parts.push(format!(
                        "{} to upload",
                        plural_count(f.up_objects_left, "file", "files")
                    ));
                } else {
                    parts.push(format!("{} to upload", format_bytes(f.up_bytes_left)));
                }
                if f.up_rate_bps > 0 {
                    parts.push(format!("{}/s up", format_bytes(f.up_rate_bps)));
                }
            }
            parts.join(" · ")
        }
        F::Blocked => {
            let mut reasons = Vec::new();
            if f.quarantined > 0 {
                reasons.push(format!(
                    "{} quarantined",
                    plural_count(f.quarantined, "event", "events")
                ));
            }
            if f.blocked > 0 {
                reasons.push(format!(
                    "{} blocked",
                    plural_count(f.blocked, "event", "events")
                ));
            }
            if f.rejected > 0 {
                reasons.push(format!(
                    "{} rejected",
                    plural_count(f.rejected, "event", "events")
                ));
            }
            if reasons.is_empty() {
                "Blocked".into()
            } else {
                format!("Blocked — {}", reasons.join(", "))
            }
        }
        F::Backoff => format!("Retrying · {} in backoff", of(total)),
        F::Offline => format!("Waiting for peers · {} unreachable", of(total)),
        F::UpToDate => {
            let up = count(&|s| s == FolderPeerState::UpToDate);
            let mut text = format!("Up to date · {}", of(up));
            let away = total - up;
            if away > 0 {
                text.push_str(&format!(" ({away} unreachable)"));
            }
            // Rejected events are no remaining work (they never activate):
            // shown, not counted (WP3.INT re-review).
            if f.rejected > 0 {
                text.push_str(&format!(
                    " · {} rejected",
                    plural_count(f.rejected, "event", "events")
                ));
            }
            text
        }
    }
}

// ---------------------------------------------------------------------------
// Stall detection

/// Inputs of [`stall`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StallInput {
    /// Progressable work remains.
    pub work_remaining: bool,
    /// The pair can make progress ([`PeerInputs::can_progress`]: connected,
    /// both folders serving, sync enabled).
    pub connected: bool,
    /// Since when `work_remaining && connected` has held without a break.
    pub eligible_since_ms: Option<u64>,
    pub last_progress_ms: Option<u64>,
    pub now_ms: u64,
    pub stall_after: Duration,
}

/// The stall detector (§7.1): work remains, the peer is connected, and there
/// has been no progress for `stall_after`, counted from the later of the last
/// progress and the moment the pair became eligible (so a reconnect after a
/// long outage does not stall at once). Returns the time the stall began.
pub fn stall(i: &StallInput) -> Option<u64> {
    if !i.work_remaining || !i.connected {
        return None;
    }
    let reference = i.eligible_since_ms?.max(i.last_progress_ms.unwrap_or(0));
    let stalled_since = reference.saturating_add(ms(i.stall_after));
    (i.now_ms >= stalled_since).then_some(stalled_since)
}

/// The remaining work of a (folder, peer) pair at one observation; used to
/// detect progress as a decrease.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Work {
    pub meta: u64,
    pub down_bytes: u64,
    pub down_objects: u64,
    pub up_bytes: u64,
    pub up_objects: u64,
}

impl Work {
    pub fn is_zero(&self) -> bool {
        *self == Work::default()
    }

    /// Objects or bytes left to download.
    pub fn has_down_work(&self) -> bool {
        self.down_bytes > 0 || self.down_objects > 0
    }

    /// Objects or bytes left to upload.
    pub fn has_up_work(&self) -> bool {
        self.up_bytes > 0 || self.up_objects > 0
    }
}

/// Whether remaining work went down between two observations (any component
/// decreased, without counting new work that arrived meanwhile).
pub fn progressed(before: &Work, after: &Work) -> bool {
    after.meta < before.meta
        || after.down_bytes < before.down_bytes
        || after.down_objects < before.down_objects
        || after.up_bytes < before.up_bytes
        || after.up_objects < before.up_objects
}

/// Objects and bytes a replica is missing, from its content counters: the
/// missing shared objects (`shared_objects - shared_available`) and their
/// bytes minus what partial spools already hold.
pub fn missing(content: &ContentCounters) -> (u64, u64) {
    if content.complete {
        return (0, 0);
    }
    (
        content
            .shared_objects
            .saturating_sub(content.shared_available),
        content
            .shared_missing_bytes
            .saturating_sub(content.partial_bytes),
    )
}

/// Whether `source` can likely supply some of what `target` misses:
///
/// * its counters are unknown (for a download, only a peer that
///   [`may_supply`] gets this far), or it is complete;
/// * it misses strictly fewer objects or fewer bytes: then `target` misses
///   something `source` holds;
/// * it misses at least as many objects and bytes, so the totals cannot say
///   (it may miss a superset of what `target` misses: one object lost on both
///   sides plus what it is still receiving from `target`, WP3.INT re-review
///   2), but it moved objects to `target` lately (`moved`, per-pair
///   evidence), misses something else than `target` does and holds objects.
///   That shows a download while each side misses different objects, as
///   during a large upload one way and a download the other (WP3.INT
///   re-review), without counting phantom work from a peer that only misses
///   more.
///
/// Two replicas missing the same objects (lost everywhere: the same objects
/// and bytes missing) owe each other nothing, so that case never shows as a
/// transfer or a stall.
fn can_supply(source: Option<&ContentCounters>, target: &ContentCounters, moved: bool) -> bool {
    source.is_none_or(|source| {
        let (theirs, ours) = (missing(source), missing(target));
        source.complete
            || theirs.0 < ours.0
            || theirs.1 < ours.1
            || (moved && theirs != ours && source.shared_available > 0)
    })
}

/// Whether something happened within `window` before `now` (`at` and `now`
/// on the same clock, in milliseconds): the per-pair evidence that a peer
/// moved objects (`may_supply`, [`download_work_with_evidence`],
/// [`upload_work_with_evidence`]).
pub fn lately(at: Option<u64>, now: u64, window: Duration) -> bool {
    at.is_some_and(|at| now.saturating_sub(at) < ms(window))
}

/// Whether our missing content may be attributed to a peer as download work
/// at all (before [`download_work`] weighs its counters):
///
/// * a peer that publishes the repo (its catalog lists it) is one we pull
///   from on our own initiative, so it may supply;
/// * a peer without a published entry for the repo (an inbound-only peer has
///   no catalog) only sends what it chooses to push, and its counters are
///   unknown. It supplies only while it demonstrably does: within `window`
///   (the stall threshold) of the last exchange in which it moved objects to
///   us (`last_supplied_ms`).
///
/// Without the second rule, content that such a peer lacks too would read as
/// a download from it and then as a false stall. That pair can never stall on
/// download work alone (its evidence expires as the threshold passes); the
/// dialer, which has our catalog and counters, reports that pair's stalls.
pub fn may_supply(
    published: bool,
    last_supplied_ms: Option<u64>,
    now_ms: u64,
    window: Duration,
) -> bool {
    published || lately(last_supplied_ms, now_ms, window)
}

/// Whether a peer may supply some of what we miss ([`can_supply`]; `supplied`:
/// it moved objects to us lately, the hub's per-pair evidence): download
/// work is attributed to it.
pub fn supplies_download(
    local: &ContentCounters,
    remote: Option<&ContentCounters>,
    supplied: bool,
) -> bool {
    can_supply(remote, local, supplied)
}

/// Whether we may supply some of what a peer misses ([`can_supply`];
/// `served`: we moved objects to it lately): upload work is attributed to the
/// pair.
pub fn supplies_upload(
    local: Option<&ContentCounters>,
    remote: &ContentCounters,
    served: bool,
) -> bool {
    can_supply(local, remote, served)
}

/// What we may still download from a peer: our missing objects and bytes,
/// when the peer (its published content counters) can supply some of them,
/// judged from the counters alone ([`download_work_with_evidence`] without
/// evidence).
pub fn download_work(local: &ContentCounters, remote: Option<&ContentCounters>) -> (u64, u64) {
    download_work_with_evidence(local, remote, false)
}

/// [`download_work`], given whether the peer moved objects to us lately
/// (`supplied`: within the stall threshold, the hub's per-pair evidence),
/// which decides when the counters cannot ([`can_supply`]).
pub fn download_work_with_evidence(
    local: &ContentCounters,
    remote: Option<&ContentCounters>,
    supplied: bool,
) -> (u64, u64) {
    if supplies_download(local, remote, supplied) {
        missing(local)
    } else {
        (0, 0)
    }
}

/// What a peer still lacks that we may upload: its missing objects and
/// bytes (from its published entry), when we can supply some of them,
/// judged from the counters alone ([`upload_work_with_evidence`] without
/// evidence).
pub fn upload_work(local: Option<&ContentCounters>, remote: &ContentCounters) -> (u64, u64) {
    upload_work_with_evidence(local, remote, false)
}

/// [`upload_work`], given whether we moved objects to the peer lately
/// (`served`: within the stall threshold, the hub's per-pair evidence),
/// which decides when the counters cannot ([`can_supply`]).
pub fn upload_work_with_evidence(
    local: Option<&ContentCounters>,
    remote: &ContentCounters,
    served: bool,
) -> (u64, u64) {
    if supplies_upload(local, remote, served) {
        missing(remote)
    } else {
        (0, 0)
    }
}

// ---------------------------------------------------------------------------
// Rates and ETA

/// Exponentially weighted byte rates over a 10 s and a 60 s window
/// (`policy::RATE_SHORT`, `policy::RATE_LONG`), with bias correction so a
/// stream reads its true rate from its first tick instead of ramping up from
/// zero. Bytes are [`add`](Ewma::add)ed as they are reported and folded in on
/// every [`tick`](Ewma::tick) with the elapsed time.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Ewma {
    pending: u64,
    short: f64,
    long: f64,
    short_weight: f64,
    long_weight: f64,
}

impl Ewma {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bytes moved since the last tick.
    pub fn add(&mut self, bytes: u64) {
        self.pending = self.pending.saturating_add(bytes);
    }

    /// Folds the pending bytes in over `elapsed`. A zero `elapsed` keeps them
    /// pending. Once both rates decay below 1 B/s the stream is over and the
    /// next one is bias-corrected afresh.
    pub fn tick(&mut self, elapsed: Duration) {
        if elapsed.is_zero() {
            return;
        }
        let sample = self.pending as f64 / elapsed.as_secs_f64();
        self.pending = 0;
        self.fold(sample, elapsed);
    }

    /// Folds in `elapsed` of a constant `rate` (bytes per second), as
    /// [`tick`](Ewma::tick) does with its pending bytes. An idle EWMA stays
    /// idle over a zero rate, so bias correction counts from the first
    /// non-zero stretch, not from the tick it falls in.
    pub fn fold(&mut self, rate: f64, elapsed: Duration) {
        let seconds = elapsed.as_secs_f64();
        if seconds <= 0.0 || !rate.is_finite() {
            return;
        }
        let sample = rate.max(0.0);
        if sample == 0.0 && self.short_weight == 0.0 && self.long_weight == 0.0 {
            return;
        }
        let fold = |value: &mut f64, weight: &mut f64, window: Duration| {
            let alpha = 1.0 - (-seconds / window.as_secs_f64()).exp();
            *value += alpha * (sample - *value);
            *weight += alpha * (1.0 - *weight);
        };
        fold(&mut self.short, &mut self.short_weight, policy::RATE_SHORT);
        fold(&mut self.long, &mut self.long_weight, policy::RATE_LONG);
        if self.short_bps() == 0 && self.long_bps() == 0 && sample == 0.0 {
            *self = Self {
                pending: self.pending,
                ..Self::default()
            };
        }
    }

    /// The 10 s rate in bytes per second.
    pub fn short_bps(&self) -> u64 {
        corrected(self.short, self.short_weight)
    }

    /// The 60 s rate in bytes per second.
    pub fn long_bps(&self) -> u64 {
        corrected(self.long, self.long_weight)
    }

    /// No stream in progress and nothing pending.
    pub fn is_idle(&self) -> bool {
        self.pending == 0 && self.short_weight == 0.0 && self.long_weight == 0.0
    }
}

/// Shortest stretch a reported transfer is spread over by [`TransferRate`]
/// (one hub tick): a transfer shorter than that counts as the average of the
/// tick it falls in, not as a burst rate.
pub const MIN_RATE_SEGMENT: Duration = policy::TICK;

/// A byte rate fed by finished transfers, whose bytes are only known when
/// they end (an exchange reports `bytes` and `duration_ms` once it is over:
/// §6.4 issues one `ExchangeFinished` per 16 MiB pull or push).
///
/// Each transfer is folded into an [`Ewma`] as a constant-rate segment of
/// `bytes / duration` that starts when it is reported and lasts its duration
/// (at least [`MIN_RATE_SEGMENT`]): the rate a transfer measured is shown
/// over as long as it took. Back-to-back transfers (the next pull starts as
/// the previous one ends) thus read their throughput continuously, and the
/// bias correction starts with the first segment, so the first reading after
/// idle is that transfer's rate. Folding a 16 MiB pull into the one tick in
/// which it ended would read 16 MiB/s whatever the link, then swing with
/// every pull. Times are milliseconds on the caller's (monotonic) timeline;
/// nothing here reads a clock.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TransferRate {
    ewma: Ewma,
    /// Segments not folded in completely: `(start_ms, end_ms, bytes per
    /// ms)`, `start_ms` advanced past what was folded.
    segments: Vec<(u64, u64, f64)>,
}

impl TransferRate {
    pub fn new() -> Self {
        Self::default()
    }

    /// A transfer of `bytes` that took `duration_ms` and was reported at
    /// `at_ms`.
    pub fn record(&mut self, bytes: u64, duration_ms: u64, at_ms: u64) {
        if bytes == 0 {
            return;
        }
        let length = duration_ms.max(ms(MIN_RATE_SEGMENT));
        self.segments.push((
            at_ms,
            at_ms.saturating_add(length),
            bytes as f64 / length as f64,
        ));
    }

    /// Folds in the stretch `(from_ms, to_ms]` (the time since the previous
    /// call). A segment reported before `from_ms` (it was recorded as the
    /// previous call ran) starts at `from_ms` instead, with its length.
    pub fn advance(&mut self, from_ms: u64, to_ms: u64) {
        if to_ms <= from_ms {
            return;
        }
        for segment in &mut self.segments {
            if segment.0 < from_ms {
                let late = from_ms - segment.0;
                segment.0 = from_ms;
                segment.1 = segment.1.saturating_add(late);
            }
        }
        // Every segment boundary inside the stretch splits it; within each
        // piece the rate is constant.
        let mut bounds = vec![from_ms, to_ms];
        for &(start, end, _) in &self.segments {
            bounds.extend(
                [start, end]
                    .into_iter()
                    .filter(|t| *t > from_ms && *t < to_ms),
            );
        }
        bounds.sort_unstable();
        bounds.dedup();
        for piece in bounds.windows(2) {
            let (a, b) = (piece[0], piece[1]);
            let per_ms: f64 = self
                .segments
                .iter()
                .filter(|(start, end, _)| *start <= a && *end >= b)
                .map(|(_, _, rate)| rate)
                .sum();
            self.ewma
                .fold(per_ms * 1000.0, Duration::from_millis(b - a));
        }
        self.segments.retain(|(_, end, _)| *end > to_ms);
        for segment in &mut self.segments {
            segment.0 = segment.0.max(to_ms);
        }
    }

    /// The 10 s rate in bytes per second.
    pub fn short_bps(&self) -> u64 {
        self.ewma.short_bps()
    }

    /// The 60 s rate in bytes per second.
    pub fn long_bps(&self) -> u64 {
        self.ewma.long_bps()
    }

    /// No transfer left to fold in and no rate shown.
    pub fn is_idle(&self) -> bool {
        self.segments.is_empty() && self.ewma.is_idle()
    }
}

fn corrected(value: f64, weight: f64) -> u64 {
    if weight <= 0.0 {
        return 0;
    }
    let rate = value / weight;
    if rate < 1.0 { 0 } else { rate.round() as u64 }
}

/// ETA in seconds: bytes remaining over the (10 s) rate, rounded up. `None`
/// when nothing remains or the rate is zero (unknown).
pub fn eta(bytes_remaining: u64, rate_bps: u64) -> Option<u64> {
    (bytes_remaining > 0 && rate_bps > 0).then(|| bytes_remaining.div_ceil(rate_bps))
}

// ---------------------------------------------------------------------------
// Formatting

/// `1.2 GiB`, `512 B`, `3.4 MiB` (binary units, one decimal from KiB up).
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64 / 1024.0;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

/// An elapsed time, rounded down to the unit shown: `45 s`, `4 min`,
/// `2 h 5 min`, `3 d 4 h` (for "stalled 4 min"; ETAs use
/// [`format_duration`], which rounds up).
pub fn format_elapsed(ms: u64) -> String {
    let seconds = ms / 1000;
    if seconds < 60 {
        format!("{seconds} s")
    } else if seconds < 3600 {
        format!("{} min", seconds / 60)
    } else if seconds < 86_400 {
        match (seconds / 60) % 60 {
            0 => format!("{} h", seconds / 3600),
            rest => format!("{} h {rest} min", seconds / 3600),
        }
    } else {
        match (seconds / 3600) % 24 {
            0 => format!("{} d", seconds / 86_400),
            rest => format!("{} d {rest} h", seconds / 86_400),
        }
    }
}

/// A remaining time, rounded up to the unit shown: `45 s`, `6 min`,
/// `2 h 5 min`, `3 d 4 h` (for ETAs).
pub fn format_duration(ms: u64) -> String {
    let seconds = ms.div_ceil(1000);
    if seconds < 60 {
        format!("{seconds} s")
    } else if seconds < 3600 {
        format!("{} min", seconds.div_ceil(60))
    } else if seconds < 86_400 {
        let minutes = seconds.div_ceil(60);
        match minutes % 60 {
            0 => format!("{} h", minutes / 60),
            rest => format!("{} h {rest} min", minutes / 60),
        }
    } else {
        let hours = seconds.div_ceil(3600);
        match hours % 24 {
            0 => format!("{} d", hours / 24),
            rest => format!("{} d {rest} h", hours / 24),
        }
    }
}

fn plural(n: u64, one: &'static str, many: &'static str) -> &'static str {
    if n == 1 { one } else { many }
}

fn plural_count(n: u64, one: &'static str, many: &'static str) -> String {
    format!("{n} {}", plural(n, one, many))
}

/// A duration in whole milliseconds (saturating).
pub fn ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
