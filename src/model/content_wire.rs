//! Versioned bounded content frames. These ops run only on bulk/loopback
//! sessions and require explicit capability negotiation before data transfer.
use crate::{
    content::ContentEdge,
    model::{protocol::LaneSpec, workspace::DemandPriority},
    paired_sync::SyncRoute,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const VERSION: u8 = 1;
pub const FRAME_BYTES: usize = 65_536;
pub const OFFER_MS: u64 = 15_000;
pub const PREFIX: &str = "objects.content-v1.";
pub const CAPS: &str = "objects.content-v1.capabilities";
pub const PREPARE: &str = "objects.content-v1.prepare";
pub const READ: &str = "objects.content-v1.read";
pub const COMPLETE: &str = "objects.content-v1.complete";
pub const RELEASE: &str = "objects.content-v1.release";
pub const ACK: &str = "objects.content-v1.ack";
pub const POLL: &str = "objects.content-v1.poll";
pub const LEGACY_PREPARE: &str = "objects.content-v1.legacy-prepare";
/// Local worker negotiation; independent of typed content activation.
pub const LEGACY_CAPS: &str = "objects.content-v1.legacy-capabilities";
pub const LEGACY_BEGIN: &str = "objects.content-v1.legacy-begin";
pub const LEGACY_READ: &str = "objects.content-v1.legacy-read";
pub const LEGACY_PULL: &str = "objects.content-v1.legacy-pull";
pub const LEGACY_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyCapabilities {
    pub version: u8,
    pub round_fences: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyPrepare {
    pub route: SyncRoute,
    pub lane: LaneSpec,
    pub session: String,
    pub hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyReply {
    pub result: crate::model::protocol::objects::ObjectsResult,
    pub token: Token,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyPull {
    pub request: crate::model::protocol::objects::ObjectsPull,
    pub token: Token,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Poll {
    pub route: SyncRoute,
    pub sources: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Activity {
    pub active: bool,
    pub demand_sources: Vec<String>,
    /// Local legacy whole-file demand, independent of content activation.
    /// None identifies an older local poll implementation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_demands: Option<Vec<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capabilities {
    pub version: u8,
    pub active: bool,
    pub frame_bytes: usize,
}

impl Capabilities {
    pub fn supports(&self) -> bool {
        self.version == VERSION && self.frame_bytes == FRAME_BYTES
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerCaps {
    pub route: SyncRoute,
    pub version: u8,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PrepareMode {
    Foreground,
    Structural,
    Background,
    Mixed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prepare {
    pub route: SyncRoute,
    pub owner: Uuid,
    pub lane: LaneSpec,
    pub mode: PrepareMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Token {
    pub owner: Uuid,
    pub generation: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Offer {
    pub token: Token,
    pub reference: String,
    pub generation: u64,
    pub edge: ContentEdge,
    pub priority: DemandPriority,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prepared {
    pub offer: Option<Offer>,
    #[serde(default)]
    pub progress: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Finish {
    pub route: SyncRoute,
    pub owner: Uuid,
    pub token: Token,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Release {
    pub transfer: Finish,
    pub failed_source: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Position {
    pub reference: String,
    pub edge: ContentEdge,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerRead {
    pub route: SyncRoute,
    pub position: Position,
    pub priority: DemandPriority,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Frame {
    pub generation: u64,
    pub bytes: usize,
    pub token: Option<Token>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PeerScope {
    pub repo: String,
    pub sender: String,
    pub receiver: String,
    pub lane: LaneSpec,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerCaps {
    pub scope: PeerScope,
    pub version: u8,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerRead {
    pub scope: PeerScope,
    pub position: Position,
    pub priority: DemandPriority,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerAck {
    pub scope: PeerScope,
    pub token: Token,
}
