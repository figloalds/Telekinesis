//! Frame codec (§6.2). Stub landed by WP0.4; WP1.9 implements `encode` and
//! `decode` with every bound: trailing bytes, unknown kinds, unknown flags,
//! a non-zero reserved field, oversize headers and bodies, and bodies on
//! kinds that forbid them are protocol errors that close the connection.
use crate::model::protocol::frame::{
    FLAG_LAST, FLAG_MORE, FrameKind, MAX_CHUNK, MAX_HEADER, MAX_HEADER_PRE_AUTH,
};
use serde::{Serialize, de::DeserializeOwned};
use std::fmt;

/// Size bounds applied by the codec.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameLimits {
    pub max_header: usize,
    pub max_body: usize,
}

impl FrameLimits {
    /// Before authentication: 16 KiB headers, no bodies beyond one chunk.
    pub const PRE_AUTH: FrameLimits = FrameLimits {
        max_header: MAX_HEADER_PRE_AUTH,
        max_body: MAX_CHUNK,
    };
    /// After authentication: 64 KiB headers, 1 MiB bodies.
    pub const SESSION: FrameLimits = FrameLimits {
        max_header: MAX_HEADER,
        max_body: MAX_CHUNK,
    };
}

/// One decoded frame. `header` holds the raw UTF-8 JSON object bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub kind: FrameKind,
    pub flags: u8,
    pub header: Vec<u8>,
    pub body: Vec<u8>,
}

impl Frame {
    /// A frame whose header is `header` serialized as JSON.
    pub fn new(
        kind: FrameKind,
        flags: u8,
        header: &impl Serialize,
        body: Vec<u8>,
    ) -> Result<Frame, FrameError> {
        let header = serde_json::to_vec(header)
            .map_err(|error| FrameError::InvalidHeader(error.to_string()))?;
        Ok(Frame {
            kind,
            flags,
            header,
            body,
        })
    }

    /// Parses the header JSON.
    pub fn header_as<T: DeserializeOwned>(&self) -> Result<T, FrameError> {
        serde_json::from_slice(&self.header)
            .map_err(|error| FrameError::InvalidHeader(error.to_string()))
    }

    pub fn more(&self) -> bool {
        self.flags & FLAG_MORE != 0
    }

    pub fn last(&self) -> bool {
        self.flags & FLAG_LAST != 0
    }
}

/// A protocol violation found by the codec.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrameError {
    Truncated,
    TrailingBytes,
    UnknownKind(u8),
    UnknownFlags(u8),
    ReservedNonZero,
    HeaderTooLarge { len: usize, max: usize },
    BodyTooLarge { len: usize, max: usize },
    BodyNotAllowed(FrameKind),
    InvalidHeader(String),
    NotImplemented,
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::Truncated => f.write_str("PROTOCOL_VIOLATION: truncated frame"),
            FrameError::TrailingBytes => f.write_str("PROTOCOL_VIOLATION: trailing bytes"),
            FrameError::UnknownKind(kind) => {
                write!(f, "PROTOCOL_VIOLATION: unknown frame kind {kind}")
            }
            FrameError::UnknownFlags(flags) => {
                write!(f, "PROTOCOL_VIOLATION: unknown frame flags {flags:#04x}")
            }
            FrameError::ReservedNonZero => {
                f.write_str("PROTOCOL_VIOLATION: reserved field is not zero")
            }
            FrameError::HeaderTooLarge { len, max } => {
                write!(f, "PROTOCOL_VIOLATION: header of {len} bytes exceeds {max}")
            }
            FrameError::BodyTooLarge { len, max } => {
                write!(f, "PROTOCOL_VIOLATION: body of {len} bytes exceeds {max}")
            }
            FrameError::BodyNotAllowed(kind) => {
                write!(f, "PROTOCOL_VIOLATION: {kind:?} frames carry no body")
            }
            FrameError::InvalidHeader(detail) => {
                write!(f, "PROTOCOL_VIOLATION: invalid frame header: {detail}")
            }
            FrameError::NotImplemented => {
                f.write_str("NOT_IMPLEMENTED: the frame codec lands in WP1.9")
            }
        }
    }
}

impl std::error::Error for FrameError {}

/// Encodes one frame (one WebSocket binary message). Stub: WP1.9.
pub fn encode(frame: &Frame, limits: FrameLimits) -> Result<Vec<u8>, FrameError> {
    let _ = (frame, limits);
    Err(FrameError::NotImplemented)
}

/// Decodes one frame from one WebSocket binary message. Stub: WP1.9.
pub fn decode(message: &[u8], limits: FrameLimits) -> Result<Frame, FrameError> {
    let _ = (message, limits);
    Err(FrameError::NotImplemented)
}
