//! Error taxonomy (REVAMP_PLAN §8). Frozen in Wave 0.
//!
//! * [`code_of`] extracts the first SCREAMING_CASE code from an error chain.
//! * [`classify`] maps a code to its [`ErrorClass`]; codes outside the table are
//!   [`ErrorClass::Transient`]. Codes that exist in the tree but are not in the
//!   table are acknowledged in [`TRANSIENT_DEFAULT_CODES`]; `tests/contracts.rs`
//!   greps `src/` and fails on any code literal that is in neither list.
//! * [`ErrorWire`] is the wire shape of one error; [`ErrorRecord`] is its
//!   history in one [`Scope`].
use serde::{Deserialize, Serialize};
use std::fmt;

/// Retry and display class of an error code (§8).
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ErrorClass {
    /// 50 → 1,000 ms; never counted as a failure; never resets `caught_up`;
    /// hidden unless it lasts ≥ 30 s.
    Contention,
    /// Exponential backoff (generic 1 s → 60 s; lanes 1 s → 60 s; objects
    /// 30 s → 1 h; inbox 1 s → 5 min); shown after 3 attempts.
    #[default]
    Transient,
    /// No automatic retry (auth rechecks every 60 s); always shown with an action.
    NeedsUserAction,
    /// Rechecked every 10 min or on configuration change; always shown.
    Incompatible,
    /// Scoped quarantine; no retry in scope; always shown.
    Fatal,
}

impl ErrorClass {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorClass::Contention => "contention",
            ErrorClass::Transient => "transient",
            ErrorClass::NeedsUserAction => "needs_user_action",
            ErrorClass::Incompatible => "incompatible",
            ErrorClass::Fatal => "fatal",
        }
    }

    /// Whether an occurrence counts toward failure counters and backoff
    /// escalation (`false` only for Contention).
    pub fn counts_as_failure(self) -> bool {
        self != ErrorClass::Contention
    }

    /// Whether the scheduler may retry on its own (Contention and Transient).
    pub fn retries_automatically(self) -> bool {
        matches!(self, ErrorClass::Contention | ErrorClass::Transient)
    }
}

impl fmt::Display for ErrorClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// §8 table, Contention row.
pub const CONTENTION_CODES: &[&str] = &["WORKER_BUSY", "BUSY", "BUSY_VIEW", "IN_FLIGHT_ELSEWHERE"];

/// §8 table, Transient row (explicitly classified). `FOLDER_SYNC_PAUSED` is
/// Transient but shown as the `Paused` state, never as an error.
pub const TRANSIENT_CODES: &[&str] = &[
    "REQUEST_TIMEOUT",
    "CONNECTION_LOST",
    "TRANSPORT_DEAD",
    "WS_CONNECT_FAILED",
    "WS_CLOSED",
    "LOCAL_WORKER_UNAVAILABLE",
    "REMOTE_WORKER_STARTING",
    "WORKER_NOT_RUNNING",
    "WORKER_EXITED",
    "WORKER_START_TIMEOUT",
    "WORKER_UNRESPONSIVE",
    "CONTENT_UNAVAILABLE",
    "OFFLINE_OBJECT_MISSING",
    "OBJECT_IO_ERROR",
    "CORRUPT_OBJECT",
    "OBJECT_PART_CHANGED",
    "RESUME_OFFSET_AHEAD",
    "SQLITE_BUSY",
    "CATALOG_UNAVAILABLE",
    "PUBLISHED_SUMMARY_FAILED",
    "NETWORK_NOT_READY",
    "FOLDER_SYNC_PAUSED",
    "NOT_AVAILABLE",
];

/// §8 table, NeedsUserAction row.
pub const NEEDS_USER_ACTION_CODES: &[&str] = &[
    "MOUNT_PATH_MUST_NOT_EXIST",
    "MOUNT_OVERLAPS_DATA",
    "MOUNT_OVERLAPS_FOLDER",
    "MOUNT_OVERLAPS_STATE",
    "WINFSP_RUNTIME_UNAVAILABLE",
    "INVALID_PAIRING_CODE",
    "PAIRING_CODE_CHANGED",
    "PAIRING_CODE_NOT_SET",
    "PEER_INSTALLATION_CHANGED",
    "PEER_REVOKED",
    "CANNOT_PAIR_WITH_SELF",
    "NAMESPACE_HISTORY_LIMIT",
    "CRASH_LOOP",
    "PREVIOUS_WORKER_UNRESPONSIVE",
    "STOP_BLOCKED_BY_OPEN_FILES",
    "GATE_BLOCKED",
    "ALREADY_RUNNING",
];

/// §8 table, Incompatible row.
pub const INCOMPATIBLE_CODES: &[&str] = &[
    "PEER_NEEDS_UPGRADE",
    "PEER_PROTOCOL_UNSUPPORTED",
    "INCOMPATIBLE_ORCHESTRATOR_PROTOCOL",
    "INCOMPATIBLE_PEER_PROTOCOL",
    "RESERVED_LANES_UNSUPPORTED",
    "INCOMPATIBLE_BULK_LANE",
    "WORKER_IPC_MISMATCH",
];

/// §8 table, Fatal row (scoped quarantine).
pub const FATAL_CODES: &[&str] = &[
    "STATE_NOT_INITIALIZED",
    "STORE_IDENTITY_MISMATCH",
    "INVALID_STORE_METADATA",
    "INVALID_SELECTED_BRANCH",
    "REPOSITORY_MISMATCH",
    "ENGINE_POISONED",
    "WORKER_STATE_MISMATCH",
    "EVENT_ID_REUSED",
    "FORGED_EVENT_ORIGIN",
    "UNAUTHORIZED_EVENT",
    "PRIVATE_EVENT_REJECTED",
    "PRIVATE_BRANCH_REJECTED",
    "INVALID_EVENT",
    "CAUSAL_CYCLE",
    "INVALID_CAUSAL_PARENT",
    "INVALID_CAUSAL_DEPENDENCY",
    "QUARANTINED_CAUSAL_ANCESTOR",
    "OBJECT_SIZE_CHANGED",
    "UNREFERENCED_OBJECT_SIZE",
    "BRANCH_DESCRIPTOR_CHANGED",
    "EVENT_METADATA_TOO_LARGE",
    "UNBOUND_PEER_RESPONSE",
    "UNBOUND_OR_EARLY_ACK",
    "BULK_ROUTE_MISMATCH",
    "REMOTE_REPLICA_BINDING_MISMATCH",
    "LOCAL_REPLICA_BINDING_MISMATCH",
    "DUPLICATE_REPLICA_IDENTITY",
    "METADATA_ON_BULK_LANE",
    "BULK_ON_CONTROL_LANE",
    "BULK_LANE_MISMATCH",
    "OBJECT_ON_WRONG_LANE",
    "UNAUTHORIZED_OBJECT",
    "UNSOLICITED_OBJECT",
    "NOT_PERMITTED",
    "CORRUPT_AT_SOURCE",
    "SIZE_MISMATCH",
    "INVENTORY_TOO_LARGE",
    "INVALID_OBJECT_PART",
    "CONFLICT_MODE_MISMATCH",
];

/// Codes that exist in the tree (or are named by REVAMP_PLAN for later waves)
/// but are not in the §8 table: they deliberately take the Transient default.
/// Listing a code here is an acknowledgement, not a classification change.
pub const TRANSIENT_DEFAULT_CODES: &[&str] = &[
    "ACCESS_DENIED",
    "ALREADY_RUNNING_OR_PATH_UNAVAILABLE",
    "AMBIGUOUS_BRANCH",
    "BACKEND_HASH_MISMATCH",
    "BACKEND_REQUIRES_STREAMING",
    "BINARY_OPS_REQUIRE_SESSION",
    "BRANCH_EXISTS",
    "BRANCH_NOT_FOUND",
    "BUCKET_EXPORT_REQUIRES_PATH_VALIDATION",
    "BUCKET_MUST_BE_OUTSIDE_MOUNT",
    "BUCKET_MUST_BE_OUTSIDE_STATE",
    "BUCKET_REQUIRES_NATIVE_DIRECTORY",
    "BUFFER_TOO_SMALL",
    "BULK_LANE_NEGOTIATION_FAILED",
    "BULK_LANE_REQUIRED",
    "BULK_PAGE_TOO_LARGE",
    "BUNDLE_TOO_LARGE",
    "CANCELLED",
    "CAUSAL_FRONTIER_UNAVAILABLE",
    "CAUSAL_PREREQUISITES_PENDING",
    "CHECKPOINT_NOT_FOUND",
    "CONFIG_NOT_FOUND",
    "CONFIG_PARENT_REQUIRED",
    "CONFIG_VERSION_2_REQUIRED",
    "CONFLICT_NOT_FOUND",
    "CONTENT_SIZE_UNKNOWN",
    "CURRENT_STATE_PUBLICATION_REQUIRES_EXPLICIT_FLAG",
    "DESKTOP_WINDOWS_ONLY",
    "DIRECTORY_CYCLE",
    "DIRECTORY_NOT_EMPTY",
    // WP0.2 `Engine::retain_all_dirty` reason (`PENDING_SAVE: DIRTY_SESSION_RETAINED`);
    // registered by WP0.INT under CCR-WP0.4-3.
    "DIRTY_SESSION_RETAINED",
    "DUPLICATE_OR_INVALID_ATTACHMENT",
    "DUPLICATE_REGISTER",
    "EMPTY_DIRECTORY",
    "ENGINE_NOT_READY",
    "ENTRY_NOT_FOUND",
    "EXPLICIT_EXISTENCE_RESOLUTION_REQUIRED",
    "EXPLICIT_REVIEW_REQUIRED",
    "FAULT_INJECTED",
    "FILE_NOT_FOUND",
    "FILE_TOO_LARGE",
    "FOLDER_MOUNT_REQUIRED",
    "FOLDER_NOT_FOUND",
    "FOLDER_UNAVAILABLE",
    "FUSE_CALLBACK_PANIC",
    "FUSE_COMMFD",
    "FUSE_DESCRIPTOR_INVALID",
    "FUSE_DESCRIPTOR_RECEIVE",
    "FUSE_DISPATCH_FAILED",
    "FUSE_MOUNT_FAILED",
    "FUSE_WORKER_PANIC",
    "HEADLESS_MOUNT_CONFLICT",
    "INCOMPLETE_CAUSAL_HISTORY",
    "INCOMPLETE_SNAPSHOT",
    "INSTALLATION_REQUIRED",
    "INVALID_ALIVE",
    "INVALID_ATTACHMENT_LENGTH",
    "INVALID_ATTACHMENT_PATH",
    "INVALID_ATTRIBUTES",
    "INVALID_BINARY_ENVELOPE",
    "INVALID_BRANCH_ID",
    "INVALID_BULK_LANE",
    "INVALID_CONFIG",
    "INVALID_CONFLICT_REVIEW",
    "INVALID_CONNECTION_COUNT",
    "INVALID_CONTENT",
    "INVALID_DIRECTORY",
    "INVALID_ENTITY",
    "INVALID_FIELD",
    "INVALID_FOLDER_LABEL",
    "INVALID_FUSE_PACKET",
    "INVALID_HANDLE",
    "INVALID_INFLATED_LENGTH",
    "INVALID_INTERRUPT",
    "INVALID_KIND",
    "INVALID_LANE_ROLE",
    "INVALID_LOCATION",
    "INVALID_MOUNTINFO",
    "INVALID_MOUNT_FOLDER_NAME",
    "INVALID_NAME",
    "INVALID_NAMESPACE_CYCLE",
    "INVALID_OBJECT_ID",
    "INVALID_OBJECT_OFFSET",
    "INVALID_ORCHESTRATOR_DISCOVERY",
    "INVALID_PAGE_BUDGET",
    "INVALID_PAIR_REPLY",
    "INVALID_PARAMETER",
    "INVALID_RECOVERY_PATH",
    "INVALID_REPLY",
    "INVALID_REQUEST",
    "INVALID_REQUEST_ID",
    "INVALID_SNAPSHOT",
    "INVALID_STORE_IDENTITY",
    "INVALID_SYNC_INTERVAL",
    "INVALID_SYNC_REPLY_ID",
    "INVALID_SYNC_REQUEST",
    "INVALID_SYNC_REQUEST_ID",
    "INVALID_TIMESTAMP",
    "INVALID_WORKER_LIMIT",
    "INVALID_WS_ENDPOINT",
    "INVALID_WS_MESSAGE",
    "INVALID_WS_PORT",
    "INVALID_WS_REPLY",
    "IS_DIRECTORY",
    "LOCAL_BULK_APPLY_FAILED",
    "LOCAL_BULK_OFFER_FAILED",
    "LOCAL_SYNC_APPLY_FAILED",
    "LOCAL_SYNC_OFFER_FAILED",
    "LOCK_HOLD_EXCEEDED",
    "LOCK_POISONED",
    "MISSING_ARGUMENT",
    "MISSING_DIRECTORY",
    "MOUNT_NAME_REQUIRED",
    "MOUNT_NOT_RUNNING",
    "MOUNT_PARENT_REQUIRED",
    "MOUNT_REBIND_REQUIRED",
    "NAMESPACE_RESOLUTION_REQUIRES_RENAME_OR_DELETE",
    "NAME_COLLISION",
    "NOTIFICATION_FAILED",
    "NOT_DIRECTORY",
    "NOT_EMPTY",
    "NOT_IMPLEMENTED",
    "OBJECT_REQUIRES_STREAMING",
    "ONE_MOUNT_PER_RUNTIME",
    "ORCHESTRATOR_NOT_RUNNING",
    "ORCHESTRATOR_STOPPING",
    "PAIRING_CODE_REQUIRED",
    "PARTIAL_INVENTORY_TOO_LARGE",
    "PEER_NOT_PAIRED",
    "PENDING_SAVE",
    "PENDING_SAVES",
    "PENDING_SAVE_PERSIST_FAILED",
    "PENDING_SAVE_RESTORED",
    "PERMISSION_DENIED",
    "PREVIOUS_WORKER_STOP_TIMEOUT",
    "PROTOCOL_VIOLATION",
    "RECOVERY_BRANCH_MISMATCH",
    "RECOVERY_NOT_VISIBLE",
    "RECOVERY_REQUIRED",
    "REMOTE_BULK_EXCHANGE_FAILED",
    "REMOTE_FOLDER_NOT_FOUND",
    "REMOTE_SYNC_EXCHANGE_FAILED",
    "REMOUNT_FAILED",
    "REPOSITORY_ALREADY_MANAGED",
    "REPOSITORY_NOT_MANAGED",
    "REQUEST_ID_REUSED",
    "REVIEW_HAS_NO_ENTRY_LOCATION",
    "REVIEW_SCOPE_MISMATCH",
    "REVISION_NOT_IN_REVIEW",
    "SNAPSHOT_CHECKSUM",
    "SNAPSHOT_CURSOR",
    "SNAPSHOT_EVENT_IDENTITY",
    "SNAPSHOT_JOURNAL_CHANGED",
    "SNAPSHOT_NON_TOPOLOGICAL_SUFFIX",
    "SNAPSHOT_VERSION_OR_IDENTITY",
    "STAGING_TRUNCATED",
    "STALE",
    "STALE_CONFLICT_REVIEW",
    "STALE_VIEW",
    "STALE_VIEW_OR_ACCESS_DENIED",
    "STOP_FOLDER_BEFORE_CONFIGURING",
    "SYNC_REQUEST_REQUIRED",
    "TOO_MANY_SESSIONS",
    "TRAILING_ATTACHMENT_DATA",
    "TRAILING_GZIP_DATA",
    "UNKNOWN_BULK_OPERATION",
    "UNKNOWN_ERROR",
    "UNKNOWN_OPERATION",
    "UNKNOWN_PEER_OPERATION",
    "UNKNOWN_PUBLISHED_OPERATION",
    "UNKNOWN_WIRE_COMPRESSION",
    "UNREFERENCED_OBJECT",
    "UNSUPPORTED_ATTRIBUTES",
    "UNSUPPORTED_FOLDER_ACTION",
    "UNSUPPORTED_FS_OPERATION",
    "UNSUPPORTED_FUSE_OPCODE",
    "UNSUPPORTED_FUSE_PROTOCOL",
    "UNSUPPORTED_OPERATION",
    "UNSUPPORTED_POSIX_KIND",
    "UNSUPPORTED_POSIX_MODE",
    "UNSUPPORTED_POSIX_OPERATION",
    "UNSUPPORTED_POSIX_OWNERSHIP",
    "UNSUPPORTED_RENAME_FLAGS",
    // A handshake watchdog could not shut its socket down (WP1.10, logged
    // only; found by CCR-WP0.INT-4 item 7's `event!` identifier scan).
    "WATCHDOG_SHUTDOWN_FAILED",
    "WINFSP_MOUNT_FAILED",
    "WORKER_LIMIT",
    "WORKER_START",
    "WORKER_STOPPING",
    "WORKER_STOP_TIMEOUT",
    "WS_HOST_REQUIRED",
    "WS_MESSAGE_TOO_LARGE",
    "WS_OR_WSS_ENDPOINT_REQUIRED",
    "WS_USERINFO_NOT_SUPPORTED",
];

/// The code [`code_of`] returns when an error chain carries none.
pub const UNKNOWN_CODE: &str = "UNKNOWN_ERROR";

/// CCR-WP6.2-1: content and admission scopes extend the historical §8 table.
/// Cancellation/stale ownership are readiness outcomes; malformed authenticated
/// content is fatal only to its object/reference, not unrelated engine work.
pub const WAVE6_CODES: &[(&str, ErrorClass)] = &[
    // CCR-WP6.4-3: superseded save work is readiness, not engine failure.
    ("STALE_WRITE_SESSION", ErrorClass::Contention),
    ("STALE_WRITE_CHECKPOINT", ErrorClass::Contention),
    ("STALE_CONTENT_SAVE", ErrorClass::Contention),
    ("STALE_CONTENT_VIEW", ErrorClass::Contention),
    ("STALE_CONTENT_WORKSPACE", ErrorClass::Contention),
    ("CONTENT_SAVE_ALREADY_COMMITTED", ErrorClass::Contention),
    ("CONTENT_METADATA_SAVE_IN_PROGRESS", ErrorClass::Contention),
    // A stable capture/sequence bound cannot be cured by retrying this intent.
    ("CONTENT_SAVE_CAPTURE_LIMIT", ErrorClass::NeedsUserAction),
    ("WRITE_GENERATION_EXHAUSTED", ErrorClass::NeedsUserAction),
    ("WRITE_JOURNAL_NEEDS_UPGRADE", ErrorClass::Incompatible),
    // These apply to the pending save/journal (or broken connection), never to
    // another engine's network/readiness state. Retain recovery bytes on error.
    ("CORRUPT_WRITE_JOURNAL", ErrorClass::Fatal),
    ("CORRUPT_CONTENT_SAVE", ErrorClass::Fatal),
    ("WRITE_JOURNAL_POISONED", ErrorClass::Fatal),
    ("WRITE_CONVERSION_POISONED", ErrorClass::Fatal),
    ("INVALID_WRITE_INTENT", ErrorClass::Fatal),
    ("MISSING_CONTENT_DESCRIPTOR", ErrorClass::Fatal),
    ("CONTENT_SAVE_INTENT_MISMATCH", ErrorClass::Fatal),
    ("CONTENT_SAVE_PREPARATION_MISMATCH", ErrorClass::Fatal),
    ("CONTENT_SAVE_RECEIPT_MISMATCH", ErrorClass::Fatal),
    ("CONTENT_SAVE_SELECTION_LOST", ErrorClass::Fatal),
    ("CONTENT_SAVE_PUBLICATION_LOST", ErrorClass::Fatal),
    ("CONTENT_SAVE_WRONG_STORE", ErrorClass::Fatal),
    ("CONTENT_SAVE_REQUIRES_FULL_WAL", ErrorClass::Fatal),
    ("CONTENT_SAVE_REQUIRES_OUTER_TRANSACTION", ErrorClass::Fatal),
    ("CONTENT_SAVE_REQUIRES_OFFLOCK_RETAIN", ErrorClass::Fatal),
    ("CONTENT_SAVE_ROLLBACK_FAILED", ErrorClass::Fatal),
    ("CONTENT_SAVE_CONNECTION_FAILED", ErrorClass::Fatal),
    ("CONTENT_SAVE_CONTINUATION_LOST", ErrorClass::Fatal),
    // Scoped owned-metadata integrity/input failures retain the byte save.
    ("CONTENT_METADATA_CONTINUATION_LOST", ErrorClass::Fatal),
    ("CONTENT_METADATA_EVENT_REUSED", ErrorClass::Fatal),
    ("CONTENT_METADATA_MUTATION_REUSED", ErrorClass::Fatal),
    ("CONTENT_METADATA_OWNER_MISMATCH", ErrorClass::Fatal),
    ("CONTENT_METADATA_RECEIPT_LOST", ErrorClass::Fatal),
    ("INVALID_TIMESTAMP_POLICY", ErrorClass::Fatal),
    ("CONTENT_SAVE_NOT_COMMITTED", ErrorClass::Contention),
    ("WRITE_JOURNAL_TRANSACTION_REQUIRED", ErrorClass::Fatal),
    ("WRITE_JOURNAL_CONNECTION_FAILED", ErrorClass::Fatal),
    ("CONTENT_RESOURCE_LIMIT", ErrorClass::Contention),
    ("DEMAND_ID_REUSED", ErrorClass::Fatal),
    ("INVALID_CONTENT_EDGE", ErrorClass::Fatal),
    ("CORRUPT_CONTENT_RECEIPT", ErrorClass::Fatal),
    ("CONTENT_CANCELLED", ErrorClass::Contention),
    ("STALE_ADMISSION_GENERATION", ErrorClass::Contention),
    ("CONTENT_DEADLINE", ErrorClass::Transient),
    ("CORRUPT_CONTENT", ErrorClass::Transient),
    ("STALE_METADATA_PREPARATION", ErrorClass::Transient),
    ("LEGACY_CONVERSION_REQUIRED", ErrorClass::Incompatible),
    ("UNSUPPORTED_CONTENT_DESCRIPTOR", ErrorClass::Incompatible),
    ("UNSUPPORTED_CONTENT_KIND", ErrorClass::Incompatible),
    ("ADMISSION_OUTSIDE_METADATA_TRANSACTION", ErrorClass::Fatal),
    ("ADMISSION_PAGE_LIMIT", ErrorClass::Fatal),
    ("CONTENT_BUFFER_LIMIT", ErrorClass::Fatal),
    ("CONTENT_LOCK_POISONED", ErrorClass::Fatal),
    ("CONTENT_PROVIDER_MISMATCH", ErrorClass::Fatal),
    ("CONTENT_STORE_NOT_INITIALIZED", ErrorClass::Fatal),
    ("EXPECTED_MANIFEST", ErrorClass::Fatal),
    ("EXPECTED_MANIFEST_LEVEL", ErrorClass::Fatal),
    ("INVALID_ADMISSION_ACTION", ErrorClass::Fatal),
    ("INVALID_ADMISSION_CONSUMER", ErrorClass::Fatal),
    ("INVALID_ADMISSION_CURSOR", ErrorClass::Fatal),
    ("INVALID_ADMISSION_GENERATION", ErrorClass::Fatal),
    ("INVALID_CONTENT_DIGEST", ErrorClass::Fatal),
    ("INVALID_DEMAND_SCOPE", ErrorClass::Fatal),
    ("INVALID_DIRECT_DESCRIPTOR", ErrorClass::Fatal),
    ("INVALID_LEGACY_DESCRIPTOR", ErrorClass::Fatal),
    ("INVALID_MANIFEST_DESCRIPTOR", ErrorClass::Fatal),
    ("INVALID_MANIFEST_ENCODING", ErrorClass::Fatal),
    ("MANIFEST_CHILD_KIND", ErrorClass::Fatal),
    ("MANIFEST_COVERAGE", ErrorClass::Fatal),
    ("MANIFEST_DEPTH", ErrorClass::Fatal),
    ("MANIFEST_FANOUT", ErrorClass::Fatal),
    ("MANIFEST_FRONTIER_LIMIT", ErrorClass::Fatal),
    ("MANIFEST_LENGTH", ErrorClass::Fatal),
    ("MANIFEST_OVERFLOW", ErrorClass::Fatal),
    ("MANIFEST_RANGE", ErrorClass::Fatal),
    ("MANIFEST_ROOT_MISMATCH", ErrorClass::Fatal),
    ("NONCANONICAL_EMPTY_CONTENT", ErrorClass::Fatal),
    ("RANGE_OUT_OF_BOUNDS", ErrorClass::Fatal),
    ("RANGE_OVERFLOW", ErrorClass::Fatal),
];

/// The class of `code` (§8). Codes outside the table are Transient.
pub fn classify(code: &str) -> ErrorClass {
    explicit_class(code).unwrap_or(ErrorClass::Transient)
}

/// The class from the §8 table only (`None` for Transient-default codes).
pub fn explicit_class(code: &str) -> Option<ErrorClass> {
    [
        (CONTENTION_CODES, ErrorClass::Contention),
        (TRANSIENT_CODES, ErrorClass::Transient),
        (NEEDS_USER_ACTION_CODES, ErrorClass::NeedsUserAction),
        (INCOMPATIBLE_CODES, ErrorClass::Incompatible),
        (FATAL_CODES, ErrorClass::Fatal),
    ]
    .into_iter()
    .find_map(|(codes, class)| codes.contains(&code).then_some(class))
    .or_else(|| {
        WAVE6_CODES
            .iter()
            .find_map(|(known, class)| (*known == code).then_some(*class))
    })
}

/// Whether `code` is in the §8 table or acknowledged as Transient-default.
pub fn is_known(code: &str) -> bool {
    explicit_class(code).is_some() || TRANSIENT_DEFAULT_CODES.contains(&code)
}

/// The first SCREAMING_CASE code in `text`.
///
/// A candidate is a maximal run of `[A-Za-z0-9_]` that consists only of
/// `[A-Z0-9_]`, starts with `A-Z` and is at least 4 characters long. It is a
/// code if it contains `_` or is a known single-word code (for example `BUSY`),
/// so words like `HTTP` or `UUID` in messages are skipped.
pub fn code_in(text: &str) -> Option<&str> {
    let bytes = text.as_bytes();
    let mut start = 0;
    while start < bytes.len() {
        if !is_word(bytes[start]) {
            start += 1;
            continue;
        }
        let mut end = start;
        while end < bytes.len() && is_word(bytes[end]) {
            end += 1;
        }
        let token = &text[start..end];
        if is_code_token(token) {
            return Some(token);
        }
        start = end;
    }
    None
}

fn is_word(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn is_code_token(token: &str) -> bool {
    let bytes = token.as_bytes();
    bytes.len() >= 4
        && bytes[0].is_ascii_uppercase()
        && bytes
            .iter()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || *b == b'_')
        && (token.contains('_') || is_known(token))
}

/// The code of an error: the first code found walking the chain from the
/// outermost context to the root cause. Typed errors ([`ErrorWire`],
/// [`crate::model::orchestrator::CallError`]) contribute their own code, a
/// SQLite busy/locked failure contributes `SQLITE_BUSY`, and any other layer
/// contributes the first SCREAMING_CASE token of its message. Returns
/// [`UNKNOWN_CODE`] when nothing matches.
pub fn code_of(error: &anyhow::Error) -> String {
    for cause in error.chain() {
        if let Some(wire) = cause.downcast_ref::<ErrorWire>() {
            return wire.code.clone();
        }
        if let Some(call) = cause.downcast_ref::<crate::model::orchestrator::CallError>() {
            return call.code();
        }
        if let Some(rusqlite::Error::SqliteFailure(failure, _)) = cause.downcast_ref()
            && matches!(
                failure.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            )
        {
            return "SQLITE_BUSY".into();
        }
        if let Some(code) = code_in(&cause.to_string()) {
            return code.to_owned();
        }
    }
    UNKNOWN_CODE.to_owned()
}

/// One error on the wire: `{code, class, message, retry_after_ms?}`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ErrorWire {
    pub code: String,
    pub class: ErrorClass,
    pub message: String,
    /// Omitted from JSON when absent (`retry_after_ms?` in §8).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

impl ErrorWire {
    /// An error with the class from [`classify`].
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        let code = code.into();
        Self {
            class: classify(&code),
            code,
            message: message.into(),
            retry_after_ms: None,
        }
    }

    /// `code` with the message `<code>: <detail>`, the conventional anyhow shape.
    pub fn coded(code: &str, detail: impl fmt::Display) -> Self {
        Self::new(code, format!("{code}: {detail}"))
    }

    /// Code and class from [`code_of`]/[`classify`]; message is the full chain.
    pub fn from_error(error: &anyhow::Error) -> Self {
        Self::new(code_of(error), format!("{error:#}"))
    }

    pub fn with_retry_after(mut self, ms: u64) -> Self {
        self.retry_after_ms = Some(ms);
        self
    }

    /// Converts into an `anyhow::Error` whose `code_of` is this code.
    pub fn into_anyhow(self) -> anyhow::Error {
        anyhow::Error::new(self)
    }
}

impl fmt::Display for ErrorWire {
    /// `CODE: message`, without repeating a code the message already starts with.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_coded(f, &self.code, &self.message)
    }
}

/// Writes `code: detail` unless `detail` is empty (just `code`) or already
/// starts with `code` followed by `:` or the end.
pub(crate) fn write_coded(f: &mut fmt::Formatter<'_>, code: &str, detail: &str) -> fmt::Result {
    let prefixed = detail
        .strip_prefix(code)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with(':'));
    if detail.is_empty() {
        f.write_str(code)
    } else if prefixed {
        f.write_str(detail)
    } else {
        write!(f, "{code}: {detail}")
    }
}

impl std::error::Error for ErrorWire {}

impl From<&anyhow::Error> for ErrorWire {
    fn from(error: &anyhow::Error) -> Self {
        Self::from_error(error)
    }
}

/// Where an error applies. Errors clear on the next success in the same scope.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Scope {
    Daemon,
    Folder {
        state: String,
    },
    Peer {
        installation: String,
    },
    Lane {
        installation: String,
        lane: u32,
    },
    FolderPeer {
        state: String,
        installation: String,
    },
    Object {
        state: String,
        hash: String,
    },
    Event {
        state: String,
        id: String,
    },
    Mount {
        state: String,
    },
    /// An endpoint that is not a paired peer (CCR-WP3.1-3, applied by
    /// WP4.INT): a pairing the endpoint refused, before any installation is
    /// known (it was `Peer{installation: <endpoint>}`). Additive: older
    /// readers of the status JSON never see it from older daemons.
    Endpoint {
        endpoint: String,
    },
}

/// The history of one error code in one scope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorRecord {
    pub error: ErrorWire,
    pub scope: Scope,
    pub first_at_ms: u64,
    pub last_at_ms: u64,
    pub count: u64,
    #[serde(default)]
    pub next_retry_at_ms: Option<u64>,
    #[serde(default)]
    pub resolved_at_ms: Option<u64>,
}

impl ErrorRecord {
    pub fn new(error: ErrorWire, scope: Scope, at_ms: u64) -> Self {
        Self {
            error,
            scope,
            first_at_ms: at_ms,
            last_at_ms: at_ms,
            count: 1,
            next_retry_at_ms: None,
            resolved_at_ms: None,
        }
    }

    /// Records another occurrence. A different code, or a resolved record,
    /// restarts the record; the same open code bumps the count.
    pub fn observe(&mut self, error: ErrorWire, at_ms: u64) {
        if error.code != self.error.code || self.resolved_at_ms.is_some() {
            *self = Self::new(error, self.scope.clone(), at_ms);
            return;
        }
        self.error = error;
        self.last_at_ms = at_ms;
        self.count = self.count.saturating_add(1);
    }

    pub fn resolve(&mut self, at_ms: u64) {
        self.resolved_at_ms.get_or_insert(at_ms);
        self.next_retry_at_ms = None;
    }

    pub fn is_resolved(&self) -> bool {
        self.resolved_at_ms.is_some()
    }

    pub fn class(&self) -> ErrorClass {
        self.error.class
    }
}
