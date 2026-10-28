//! Authenticated session framing and bounded replacement operations.
use serde::{Deserialize, Serialize};
use std::time::Duration;
pub const PROTOCOL_NAME: &str = "tkfs-peer";
pub const PROTOCOL_VERSION: u32 = 4;
pub const IPC_VERSION: u32 = 4;
pub const SUPPORTED_VERSIONS: &[u32] = &[4];
pub const UPGRADE_HEADER: &str = "X-Tkfs-Sync";
pub const UPGRADE_V3: &str = "4";
pub const UPGRADE_LOOPBACK: &str = "4-loopback";
pub const TOKEN_HEADER: &str = "X-Tkfs-Token";
pub const LEGACY_NEEDS_UPGRADE_REPLY: &str =
    r#"{"ok":false,"error":"PEER_NEEDS_UPGRADE: use an authenticated session"}"#;
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

pub mod ops {
    pub const PING: &str = "ping";
    pub const PEER_ENROLL: &str = "peer.enroll";
    pub const PEER_REVOKE: &str = "peer.revoke";
}
pub mod deadlines {
    use std::time::Duration;
    pub const PEER_ENROLL: Duration = Duration::from_secs(10);
    pub const PEER_REVOKE: Duration = Duration::from_secs(10);
}
pub fn op_deadline(_op: &str) -> Option<Duration> {
    Some(Duration::from_secs(10))
}
/// Per-lane op policy: the control lane never carries object bytes, bulk
/// lanes carry only `objects.*` (and `ping`), probe lanes a fixed short list.
pub fn op_allowed(op: &str, role: LaneRole) -> bool {
    match role {
        LaneRole::Probe => matches!(op, "ping" | "peer.enroll" | "catalog.list" | "clock.probe"),
        LaneRole::Bulk => op == "ping" || op.starts_with("objects."),
        LaneRole::Control => !op.starts_with("objects."),
    }
}
pub fn max_body(op: &str) -> usize {
    if op == "objects.fetch" {
        32 * 1024 * 1024
    } else if matches!(
        op,
        "meta.exchange" | "cat" | "tree" | "events" | "desktop-snapshot"
    ) {
        16 * 1024 * 1024
    } else {
        1024 * 1024
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct PingReply {
    pub alive: bool,
}
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
        pub verifier_generation: u64,
        #[serde(default)]
        pub features: Vec<String>,
        pub limits: Limits,
    }

    /// Application credential carried by the selected TLS/WSS or VPN channel.
    #[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Auth {
        pub credential: String,
        #[serde(default)]
        pub peer: bool,
    }
    impl std::fmt::Debug for Auth {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Auth")
                .field("credential", &"<redacted>")
                .field("peer", &self.peer)
                .finish()
        }
    }
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct AuthOk {}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_lane_never_carries_objects_and_bulk_only_does() {
        assert!(!op_allowed("objects.fetch", LaneRole::Control));
        assert!(!op_allowed("objects.write", LaneRole::Control));
        assert!(op_allowed("meta.exchange", LaneRole::Control));
        assert!(op_allowed("clock.probe", LaneRole::Control));
        assert!(op_allowed("objects.fetch", LaneRole::Bulk));
        assert!(!op_allowed("meta.exchange", LaneRole::Bulk));
        assert!(op_allowed("clock.probe", LaneRole::Probe));
        assert!(!op_allowed("meta.exchange", LaneRole::Probe));
    }

    #[test]
    fn protocol_is_version_4() {
        assert_eq!(SUPPORTED_VERSIONS, &[4]);
        assert_eq!(UPGRADE_V3, "4");
        assert_eq!(UPGRADE_LOOPBACK, "4-loopback");
        assert_eq!(IPC_VERSION, 4);
    }

    #[test]
    fn worker_bulk_replies_get_the_metadata_body_bound() {
        assert_eq!(max_body("cat"), 16 * 1024 * 1024);
        assert_eq!(max_body("tree"), max_body("meta.exchange"));
        assert_eq!(max_body("hello"), 1024 * 1024);
    }
}
