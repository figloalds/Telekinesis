//! Peer protocol v3 (§6.2–§6.4). Frozen in Wave 0.
//!
//! * top level: protocol identity, upgrade headers, lane roles, peer op names,
//!   per-op deadlines and body limits, `ping` and `catalog.list` payloads;
//! * [`frame`]: frame kinds, flags, bounds and every frame header type;
//! * [`feed`]: metadata feed format 7 (`meta.exchange`);
//! * [`objects`]: objects v3 (peer `objects.*` and worker `objects.*` ops).
use crate::model::orchestrator::PublishedEntry;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// `ClientHello.protocol` / `ServerHello.protocol`.
pub const PROTOCOL_NAME: &str = "tkfs-peer";
/// The peer protocol version negotiated by this build.
pub const PROTOCOL_VERSION: u32 = 3;
/// Versions offered in `ClientHello.versions`.
pub const SUPPORTED_VERSIONS: &[u32] = &[PROTOCOL_VERSION];
/// WebSocket upgrade header; v3 servers echo it, its absence means legacy.
pub const UPGRADE_HEADER: &str = "X-Tkfs-Sync";
/// `X-Tkfs-Sync` value for peer sessions.
pub const UPGRADE_V3: &str = "3";
/// `X-Tkfs-Sync` value for orchestrator ↔ worker loopback sessions.
pub const UPGRADE_LOOPBACK: &str = "3-loopback";
/// Local token header (WP4.1).
pub const TOKEN_HEADER: &str = "X-Tkfs-Token";
/// Domain-separation prefix of the authentication transcript `T` (§6.2).
pub const AUTH_CONTEXT: &str = "tkfs-peer-auth-v3";
/// Text JSON reply a v3 server sends to a legacy (v2) client before closing.
pub const LEGACY_NEEDS_UPGRADE_REPLY: &str = r#"{"ok":false,"error":"PEER_NEEDS_UPGRADE: remote runs tkfs peer protocol 3; upgrade this orchestrator"}"#;

/// The role of a session (`ClientHello.role`).
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum LaneRole {
    /// Lane 0: metadata, catalog, control traffic. Never object bytes.
    #[default]
    Control,
    /// Lanes 1..N-1: objects only. Never activates or acknowledges metadata.
    Bulk,
    /// Short-lived session for pairing and `remote_list`.
    Probe,
}

impl LaneRole {
    pub fn as_str(self) -> &'static str {
        match self {
            LaneRole::Control => "control",
            LaneRole::Bulk => "bulk",
            LaneRole::Probe => "probe",
        }
    }
}

/// A bulk lane `{index, count}` of a pool (`ClientHello.lane`). Objects are
/// sharded across the bulk lanes with [`LaneSpec::owns`], the same rule as
/// the legacy `bulk_sync::Lane::owns` (invariant 19).
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct LaneSpec {
    pub index: u32,
    pub count: u32,
}

impl LaneSpec {
    pub fn new(index: u32, count: u32) -> Self {
        Self { index, count }
    }

    /// `1 ≤ count ≤ 15` and `index < count` (the legacy `INVALID_BULK_LANE` bounds).
    pub fn is_valid(self) -> bool {
        (1..=15).contains(&self.count) && self.index < self.count
    }

    /// Whether this lane owns `hash`: the first 16 hex digits as a u64, modulo
    /// `count`, equals `index`. Malformed hashes are owned by no lane.
    pub fn owns(self, hash: &str) -> bool {
        hash.get(..16)
            .and_then(|prefix| u64::from_str_radix(prefix, 16).ok())
            .is_some_and(|value| {
                self.count > 0 && value % u64::from(self.count) == u64::from(self.index)
            })
    }
}

/// Peer op names (§6.2).
pub mod ops {
    pub const PING: &str = "ping";
    pub const CATALOG_LIST: &str = "catalog.list";
    pub const META_EXCHANGE: &str = "meta.exchange";
    /// Interim bulk path (W2–W3, §6.5): legacy `bulk_sync::Page` JSON.
    pub const OBJECTS_EXCHANGE: &str = "objects.exchange";
    pub const OBJECTS_PULL: &str = "objects.pull";
    pub const OBJECTS_WANTS: &str = "objects.wants";
    pub const OBJECTS_PUSH: &str = "objects.push";
    pub const OBJECTS_REPORT: &str = "objects.report";
}

/// Per-op deadlines (§6.2 table). Streaming ops use an idle deadline.
pub mod deadlines {
    use std::time::Duration;
    pub const PING: Duration = Duration::from_secs(5);
    pub const CATALOG_LIST: Duration = Duration::from_secs(10);
    pub const META_EXCHANGE: Duration = Duration::from_secs(30);
    pub const OBJECTS_EXCHANGE: Duration = Duration::from_secs(60);
    /// Idle deadline of `objects.pull` and `objects.push` (PROGRESS resets it).
    pub const OBJECTS_IDLE: Duration = Duration::from_secs(30);
    pub const OBJECTS_WANTS: Duration = Duration::from_secs(10);
    pub const OBJECTS_REPORT: Duration = Duration::from_secs(10);
}

/// The absolute deadline of a non-streaming op (`None` for streaming ops,
/// which use [`deadlines::OBJECTS_IDLE`], and for unknown ops).
pub fn op_deadline(op: &str) -> Option<Duration> {
    match op {
        ops::PING => Some(deadlines::PING),
        ops::CATALOG_LIST => Some(deadlines::CATALOG_LIST),
        ops::META_EXCHANGE => Some(deadlines::META_EXCHANGE),
        ops::OBJECTS_EXCHANGE => Some(deadlines::OBJECTS_EXCHANGE),
        ops::OBJECTS_WANTS => Some(deadlines::OBJECTS_WANTS),
        ops::OBJECTS_REPORT => Some(deadlines::OBJECTS_REPORT),
        _ => None,
    }
}

/// The role an op requires (`None`: any role). `catalog.list` names
/// `Control`, but `Probe` sessions may use it too: check permissions with
/// [`op_allowed`], not by comparing with this value.
pub fn op_role(op: &str) -> Option<LaneRole> {
    match op {
        ops::PING => None,
        ops::CATALOG_LIST | ops::META_EXCHANGE => Some(LaneRole::Control),
        _ if op.starts_with("objects.") => Some(LaneRole::Bulk),
        _ => None,
    }
}

/// Whether a session of `role` may send `op` (§6.2 op table): ops without a
/// role (`ping`, unknown ops, which the server answers itself) on any
/// session; `catalog.list` on control **or** probe sessions; `meta.exchange`
/// on control sessions; `objects.*` on bulk sessions. (CCR-WP0.INT-4 item 2.)
pub fn op_allowed(op: &str, role: LaneRole) -> bool {
    match op_role(op) {
        None => true,
        Some(_) if op == ops::CATALOG_LIST => {
            matches!(role, LaneRole::Control | LaneRole::Probe)
        }
        Some(required) => required == role,
    }
}

/// Reassembled body limit per op: 16 MiB for `meta.exchange`, 1 MiB otherwise.
pub fn max_body(op: &str) -> usize {
    if op == ops::META_EXCHANGE {
        frame::MAX_BODY_META
    } else {
        frame::MAX_CHUNK
    }
}

/// `ping` reply.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PingReply {
    pub alive: bool,
}

/// `catalog.list{since}`: `unchanged:true` (with no entries) when `since`
/// equals the current generation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogListRequest {
    #[serde(default)]
    pub since: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogListReply {
    pub generation: u64,
    pub unchanged: bool,
    pub entries: Vec<PublishedEntry>,
}

/// `meta.exchange` request header; the body is a JSON `MetaExchangeRequest`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaExchangeHeader {
    pub repo: String,
}

/// Session frames (§6.2). One frame per WebSocket binary message:
///
/// ```text
/// off len  field
/// 0   1    kind (FrameKind)
/// 1   1    flags: bit0 MORE, bit1 LAST (CHUNK only)
/// 2   2    reserved = 0
/// 4   4    header_len u32 LE (≤ 16 KiB pre-auth, ≤ 64 KiB after)
/// 8   n    header: UTF-8 JSON object (types below)
/// 8+n m    body: raw bytes (≤ 1 MiB; empty for HELLO/AUTH/HEARTBEAT/GOAWAY/CANCEL/PROGRESS)
/// ```
pub mod frame {
    use super::{LaneRole, LaneSpec};
    use crate::model::errors::ErrorWire;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use std::time::Duration;

    pub const PREFIX_LEN: usize = 8;
    pub const FLAG_MORE: u8 = 0b01;
    pub const FLAG_LAST: u8 = 0b10;
    pub const KNOWN_FLAGS: u8 = FLAG_MORE | FLAG_LAST;
    /// Header bound before authentication completes.
    pub const MAX_HEADER_PRE_AUTH: usize = 16 * 1024;
    /// Header bound after authentication.
    pub const MAX_HEADER: usize = 64 * 1024;
    /// Body bound of one frame, and the default reassembled body bound.
    pub const MAX_CHUNK: usize = 1024 * 1024;
    /// Reassembled body bound for `meta.exchange`.
    pub const MAX_BODY_META: usize = 16 * 1024 * 1024;
    /// Pre-auth handshake: every message ≤ 16 KiB and ≤ 5 s per step.
    pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
    /// A HEARTBEAT is sent after this much send idleness.
    pub const HEARTBEAT_AFTER: Duration = Duration::from_secs(5);
    /// The connection is dead after this long without any received frame.
    pub const DEAD_AFTER: Duration = Duration::from_secs(20);
    /// Pending-request deadline scan period.
    pub const DEADLINE_SCAN: Duration = Duration::from_millis(250);
    /// Write from the queues while tungstenite's buffer is below this.
    pub const WRITE_HIGH_WATER: usize = 256 * 1024;
    /// Control queue bound (headers, PROGRESS, CANCEL, HEARTBEAT, GOAWAY).
    pub const CONTROL_QUEUE_BYTES: usize = 16 * 1024 * 1024;
    /// Data queue bound (CHUNK frames); producers block when it is full.
    pub const DATA_QUEUE_BYTES: usize = 32 * 1024 * 1024;
    /// After GOAWAY: in-flight requests complete; close after idle or this.
    pub const GOAWAY_GRACE: Duration = Duration::from_secs(5);
    /// Per resolved address when dialing.
    pub const DIAL_ATTEMPT: Duration = Duration::from_secs(3);
    /// Client nonce / server nonce length in bytes (hex-encoded on the wire).
    pub const NONCE_BYTES: usize = 32;

    /// `GOAWAY` codes sent instead of `HELLO_ACK` or `AUTH_OK`.
    pub const GOAWAY_HANDSHAKE_CODES: &[&str] = &[
        "CANNOT_PAIR_WITH_SELF",
        "PAIRING_CODE_NOT_SET",
        "TOO_MANY_SESSIONS",
        "PEER_REVOKED",
        "PEER_PROTOCOL_UNSUPPORTED",
        "INVALID_PAIRING_CODE",
        // The verifier rotated between HELLO_ACK and AUTH (CCR-WP1.10-2).
        "PAIRING_CODE_CHANGED",
    ];

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    #[repr(u8)]
    pub enum FrameKind {
        Hello = 1,
        HelloAck = 2,
        Auth = 3,
        AuthOk = 4,
        Request = 5,
        Response = 6,
        Chunk = 7,
        Progress = 8,
        Cancel = 9,
        Heartbeat = 10,
        Goaway = 11,
    }

    impl FrameKind {
        pub const ALL: [FrameKind; 11] = [
            FrameKind::Hello,
            FrameKind::HelloAck,
            FrameKind::Auth,
            FrameKind::AuthOk,
            FrameKind::Request,
            FrameKind::Response,
            FrameKind::Chunk,
            FrameKind::Progress,
            FrameKind::Cancel,
            FrameKind::Heartbeat,
            FrameKind::Goaway,
        ];

        pub fn from_u8(byte: u8) -> Option<FrameKind> {
            FrameKind::ALL.into_iter().find(|kind| *kind as u8 == byte)
        }

        pub fn as_u8(self) -> u8 {
            self as u8
        }

        /// Kinds that must carry an empty body.
        pub fn body_forbidden(self) -> bool {
            matches!(
                self,
                FrameKind::Hello
                    | FrameKind::HelloAck
                    | FrameKind::Auth
                    | FrameKind::AuthOk
                    | FrameKind::Heartbeat
                    | FrameKind::Goaway
                    | FrameKind::Cancel
                    | FrameKind::Progress
            )
        }

        /// Kinds allowed before authentication completes.
        pub fn pre_auth(self) -> bool {
            matches!(
                self,
                FrameKind::Hello
                    | FrameKind::HelloAck
                    | FrameKind::Auth
                    | FrameKind::AuthOk
                    | FrameKind::Goaway
            )
        }
    }

    /// `HELLO` header (client → server).
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ClientHello {
        /// Always [`super::PROTOCOL_NAME`].
        pub protocol: String,
        pub versions: Vec<u32>,
        pub installation: String,
        pub role: LaneRole,
        #[serde(default)]
        pub lane: Option<LaneSpec>,
        /// Pool size of the dialer (`max(connections, 2)`).
        pub pool: u32,
        /// Session uuid.
        pub session: String,
        /// 32 random bytes, hex.
        pub client_nonce: String,
        #[serde(default)]
        pub features: Vec<String>,
    }

    /// Limits a server announces in `HELLO_ACK`.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct Limits {
        pub max_header: u64,
        pub max_chunk: u64,
        pub max_body: u64,
    }

    impl Default for Limits {
        fn default() -> Self {
            Self {
                max_header: MAX_HEADER as u64,
                max_chunk: MAX_CHUNK as u64,
                max_body: MAX_BODY_META as u64,
            }
        }
    }

    /// `HELLO_ACK` header (server → client).
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ServerHello {
        pub protocol: String,
        pub version: u32,
        pub installation: String,
        pub role: LaneRole,
        #[serde(default)]
        pub lane: Option<LaneSpec>,
        /// Verifier salt; the client derives the key with it.
        pub salt: String,
        /// 32 random bytes, hex.
        pub server_nonce: String,
        pub verifier_generation: u64,
        #[serde(default)]
        pub features: Vec<String>,
        pub limits: Limits,
    }

    /// `AUTH` header: `proof = hex(HMAC-SHA256(K, "client" ‖ T))`.
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct Auth {
        pub proof: String,
    }

    /// `AUTH_OK` header: `server_proof = hex(HMAC-SHA256(K, "server" ‖ T))`.
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct AuthOk {
        pub server_proof: String,
    }

    /// `REQUEST` header. Only clients originate requests; ids are allocated by
    /// the client. With `MORE`, the body continues in CHUNK frames and `total`
    /// gives the full body length.
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct RequestHeader {
        pub id: u64,
        pub op: String,
        /// Remaining time to the absolute deadline, for the server's handler.
        #[serde(default)]
        pub deadline_ms: Option<u64>,
        /// Idle deadline of a streaming op.
        #[serde(default)]
        pub idle_ms: Option<u64>,
        /// Op-specific header object.
        #[serde(default)]
        pub header: Value,
        #[serde(default)]
        pub total: Option<u64>,
    }

    /// `RESPONSE` header. `ok:false` carries `error` and leaves the connection
    /// usable (invariant 17).
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ResponseHeader {
        pub id: u64,
        pub ok: bool,
        #[serde(default)]
        pub header: Value,
        #[serde(default)]
        pub error: Option<ErrorWire>,
        #[serde(default)]
        pub total: Option<u64>,
    }

    /// `CHUNK` header: a body continuation (`hash: None`) or a streamed object
    /// part (`objects.pull` responses, `objects.push` requests). `last` mirrors
    /// the `LAST` flag.
    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ChunkHeader {
        pub id: u64,
        #[serde(default)]
        pub hash: Option<String>,
        pub offset: u64,
        pub total: u64,
        pub last: bool,
    }

    /// `PROGRESS` header: resets the idle deadline of request `id`.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ProgressHeader {
        pub id: u64,
        #[serde(default)]
        pub bytes: Option<u64>,
    }

    /// `CANCEL` header: the client gave up on request `id`.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct CancelHeader {
        pub id: u64,
    }

    /// Echo of the last heartbeat received from the other side; RTT is
    /// `now - echo.at_ms - echo.held_ms` on the side that sent `at_ms`.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct HeartbeatEcho {
        pub seq: u64,
        pub at_ms: u64,
        pub held_ms: u64,
    }

    /// `HEARTBEAT` header, sent after [`HEARTBEAT_AFTER`] of send idleness.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct Heartbeat {
        pub seq: u64,
        pub at_ms: u64,
        #[serde(default)]
        pub echo: Option<HeartbeatEcho>,
    }

    /// `GOAWAY` header: no new requests; in-flight requests complete.
    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct Goaway {
        pub code: String,
        pub message: String,
    }
}

/// Metadata feed, format 7 (§6.3).
pub mod feed {
    use crate::{core::Event, model::errors::ErrorClass};
    use serde::{Deserialize, Serialize};
    use sha2::{Digest, Sha256};

    pub const FEED_FORMAT: u32 = 7;
    /// Entries per page.
    pub const MAX_PAGE_ENTRIES: usize = 4096;
    /// Payload bytes per page.
    pub const MAX_PAGE_BYTES: usize = 1024 * 1024;
    /// Events larger than this are flagged `large` and served only via `need`.
    pub const LARGE_EVENT_BYTES: usize = 256 * 1024;
    /// Ids per `need` list.
    pub const MAX_NEED: usize = 256;
    /// Total payload bytes of `need_served`. A `meta.exchange` reply carries
    /// `need_served` plus a page (≤ [`MAX_PAGE_BYTES`]) plus JSON overhead in
    /// one body of at most `frame::MAX_BODY_META` (16 MiB), so the need budget
    /// leaves room for both: 12 MiB (one 8 MiB event still fits). Was 16 MiB,
    /// which could make a reply serving two 8 MiB events exceed the body
    /// limit forever (CCR-WP0.INT-4 item 1).
    pub const MAX_NEED_BYTES: usize = 12 * 1024 * 1024;
    const _: () =
        assert!(MAX_NEED_BYTES + MAX_PAGE_BYTES + 2 * 1024 * 1024 <= super::frame::MAX_BODY_META);
    /// Rejections shown in a reply (the full set is durable in `feed_rejected`).
    pub const MAX_REJECTED_SHOWN: usize = 256;
    /// Hex characters of an entry digest.
    pub const DIGEST_HEX_LEN: usize = 16;
    /// Cap of exact `to_send`/`to_receive` counts.
    pub const COUNT_CAP: u64 = 100_000;

    /// Payload digest: the first 16 hex characters of SHA-256 of the payload
    /// text (the stored event JSON).
    pub fn digest(payload: &str) -> String {
        let mut hex = hex::encode(Sha256::digest(payload.as_bytes()));
        hex.truncate(DIGEST_HEX_LEN);
        hex
    }

    /// `{epoch, seq, id}`; `seq == 0` with an empty id is the start of a feed.
    #[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub struct Cursor {
        pub epoch: String,
        pub seq: i64,
        pub id: String,
    }

    impl Cursor {
        pub fn start(epoch: &str) -> Self {
            Self {
                epoch: epoch.to_owned(),
                seq: 0,
                id: String::new(),
            }
        }

        pub fn is_start(&self) -> bool {
            self.seq == 0 && self.id.is_empty()
        }
    }

    /// One feed entry. `event: None` means elided (the receiver is known to
    /// hold it) or `large` (fetch it through `need`).
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct FeedEntry {
        pub seq: i64,
        pub id: String,
        pub digest: String,
        #[serde(default)]
        pub event: Option<Event>,
        /// Payload bytes of a large event.
        #[serde(default)]
        pub large: Option<u64>,
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct FeedPage {
        pub epoch: String,
        /// The cursor this page continues from (`None`: probe page).
        #[serde(default)]
        pub after: Option<Cursor>,
        pub reset: bool,
        pub entries: Vec<FeedEntry>,
        /// The last scanned position (the tip when exhausted).
        pub through: Cursor,
        pub tip: i64,
        pub more: bool,
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct FeedState {
        pub epoch: String,
        pub tip: i64,
        pub pending_active: u64,
        pub blocked: u64,
        pub quarantined: u64,
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum PageStatus {
        #[default]
        Accepted,
        CursorMismatch {
            expected: Cursor,
        },
        Reset,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct RejectedEvent {
        pub id: String,
        pub seq: i64,
        pub code: String,
        pub class: ErrorClass,
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct MetaExchangeRequest {
        pub format: u32,
        /// Request uuid; the reply must carry it as `reply_to`.
        pub request: String,
        pub sender: String,
        pub receiver: String,
        pub repo: String,
        /// Sender's feed after the receiver's last reported cursor.
        pub page: FeedPage,
        /// Sender's cursor of the receiver's feed.
        #[serde(default)]
        pub cursor: Option<Cursor>,
        /// ≤ 256 ids wanted from the receiver.
        #[serde(default)]
        pub need: Vec<String>,
        pub state: FeedState,
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct MetaExchangeReply {
        pub format: u32,
        pub reply_to: String,
        pub sender: String,
        pub receiver: String,
        pub repo: String,
        pub page_status: PageStatus,
        /// Receiver's cursor of the sender's feed (after this request).
        pub cursor: Cursor,
        /// Receiver's feed after `request.cursor` (or reset).
        pub page: FeedPage,
        pub staged: u32,
        /// ≤ 256 shown; the full set is durable in `feed_rejected`.
        #[serde(default)]
        pub rejected: Vec<RejectedEvent>,
        #[serde(default)]
        pub need_served: Vec<Event>,
        #[serde(default)]
        pub unknown: Vec<String>,
        pub state: FeedState,
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct GateView {
        pub caught_up: bool,
        #[serde(default)]
        pub waiting_on: Vec<String>,
        pub blocked: bool,
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ExchangeSummary {
        pub sent_events: u32,
        pub received_events: u32,
        pub staged: u32,
        pub rejected: u32,
        pub acked: u32,
        pub local_tip: i64,
        pub remote_tip: i64,
        pub cursor_in: i64,
        pub cursor_out: i64,
        pub to_send: u64,
        pub to_receive: u64,
        pub local_pending: u64,
        pub remote_pending: u64,
        pub blocked: u64,
        pub quarantined: u64,
        pub caught_up: bool,
        pub metadata_advanced: bool,
        pub bytes_up: u64,
        pub bytes_down: u64,
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct FeedApplyResult {
        pub summary: ExchangeSummary,
        pub gate: GateView,
    }
}

/// Objects v3 (§6.4): peer ops on bulk lanes and worker ops on loopback sessions.
pub mod objects {
    use super::LaneSpec;
    use crate::{model::errors::ErrorWire, paired_sync::SyncRoute};
    use serde::{Deserialize, Serialize};
    use std::time::Duration;

    /// Worker op names (loopback sessions, binary bodies).
    pub mod ops {
        pub const WANT: &str = "objects.want";
        pub const READ: &str = "objects.read";
        pub const WRITE: &str = "objects.write";
        pub const REPORT: &str = "objects.report";
    }

    /// Items per want list.
    pub const MAX_WANT_ITEMS: usize = 256;
    /// Bytes per want list and per pull budget.
    pub const PULL_BUDGET_BYTES: u64 = 16 * 1024 * 1024;
    /// Bytes per `objects.read` call.
    pub const MAX_READ_LEN: u64 = 1024 * 1024;
    /// Worker reads / writes in flight per transfer.
    pub const INFLIGHT_READS: usize = 4;
    pub const INFLIGHT_WRITES: usize = 4;
    /// Pulls in flight per lane (the next is issued at half the budget).
    pub const INFLIGHT_PULLS: usize = 2;
    /// Repositories served concurrently per bulk lane.
    pub const REPOS_PER_LANE: usize = 2;
    /// Lease TTL, renewed on progress.
    pub const LEASE_TTL: Duration = Duration::from_secs(60);
    /// Demand requests expire after this.
    pub const DEMAND_EXPIRY: Duration = Duration::from_secs(24 * 3600);

    /// Want priority: demand > current projection > history (declaration
    /// order is priority order).
    #[derive(
        Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
    )]
    #[serde(rename_all = "snake_case")]
    pub enum WantPriority {
        Demand,
        Current,
        #[default]
        History,
    }

    /// One wanted object; `offset` is the receiver's durable spool offset.
    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct WantItem {
        pub hash: String,
        pub size: u64,
        pub offset: u64,
        pub priority: WantPriority,
    }

    /// Peer `objects.pull` request (D pulls from A); reply is a CHUNK stream
    /// plus a final [`ObjectsResult`].
    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ObjectsPull {
        pub repo: String,
        pub sender: String,
        pub receiver: String,
        pub lane: LaneSpec,
        pub items: Vec<WantItem>,
        pub budget_bytes: u64,
    }

    /// Peer `objects.wants` request (D asks what A wants); reply [`ObjectsWantsReply`].
    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ObjectsWantsRequest {
        pub repo: String,
        pub sender: String,
        pub receiver: String,
        pub lane: LaneSpec,
        pub max_items: u32,
        pub max_bytes: u64,
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ObjectsWantsReply {
        pub items: Vec<WantItem>,
    }

    /// Peer `objects.push` request header; the body follows as CHUNK frames.
    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ObjectsPush {
        pub repo: String,
        pub sender: String,
        pub receiver: String,
        pub lane: LaneSpec,
        pub items: Vec<WantItem>,
    }

    /// Peer `objects.report{repo, sender, hash, code}` (e.g. `CORRUPT_AT_SOURCE`);
    /// reply `{}`.
    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ObjectsReport {
        pub repo: String,
        pub sender: String,
        pub hash: String,
        pub code: String,
    }

    /// Per-object outcome of a pull, push or write.
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum ObjectOutcome {
        Completed,
        Progressed { durable_offset: u64 },
        Skipped,
        InFlightElsewhere,
        NotAvailable,
        NotPermitted,
        CorruptAtSource,
        SizeMismatch,
        Unsolicited,
        Failed(ErrorWire),
    }

    impl ObjectOutcome {
        /// The error code of a non-success outcome.
        pub fn code(&self) -> Option<&str> {
            match self {
                ObjectOutcome::Completed
                | ObjectOutcome::Progressed { .. }
                | ObjectOutcome::Skipped => None,
                ObjectOutcome::InFlightElsewhere => Some("IN_FLIGHT_ELSEWHERE"),
                ObjectOutcome::NotAvailable => Some("NOT_AVAILABLE"),
                ObjectOutcome::NotPermitted => Some("NOT_PERMITTED"),
                ObjectOutcome::CorruptAtSource => Some("CORRUPT_AT_SOURCE"),
                ObjectOutcome::SizeMismatch => Some("SIZE_MISMATCH"),
                ObjectOutcome::Unsolicited => Some("UNSOLICITED_OBJECT"),
                ObjectOutcome::Failed(error) => Some(error.code.as_str()),
            }
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ObjectResult {
        pub hash: String,
        pub outcome: ObjectOutcome,
        pub bytes: u64,
    }

    /// Final reply of `objects.pull` / `objects.push`.
    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ObjectsResult {
        pub results: Vec<ObjectResult>,
    }

    /// Worker `objects.want` header; reply [`WantReply`].
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct WantHeader {
        pub route: SyncRoute,
        pub lane: LaneSpec,
        pub max_items: u32,
        pub max_bytes: u64,
        /// Session id that will hold the leases.
        pub session: String,
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct WantReply {
        pub items: Vec<WantItem>,
    }

    /// Worker `objects.read` header; reply header [`ReadReply`], body = bytes.
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ReadHeader {
        pub route: SyncRoute,
        pub hash: String,
        pub offset: u64,
        pub len: u64,
    }

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ReadReply {
        pub total: u64,
    }

    /// One part of an `objects.write` body (bodies are concatenated in order).
    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct WriteItem {
        pub hash: String,
        pub offset: u64,
        pub total: u64,
        pub len: u64,
        pub last: bool,
    }

    /// Worker `objects.write` header; reply [`WriteReply`].
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct WriteHeader {
        pub route: SyncRoute,
        pub session: String,
        pub items: Vec<WriteItem>,
    }

    /// `durable_offset` is advertised only after the spool fsync and the
    /// `spool_state` commit (invariant 2).
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct WriteResult {
        pub hash: String,
        pub outcome: ObjectOutcome,
        pub durable_offset: u64,
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct WriteReply {
        pub results: Vec<WriteResult>,
    }

    /// Worker `objects.report` header; reply [`ReportReply`].
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ReportHeader {
        pub route: SyncRoute,
        pub hash: String,
        pub code: String,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum ReportAction {
        Quarantined,
        VerifiedOk,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ReportReply {
        pub action: ReportAction,
    }
}
