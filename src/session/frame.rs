//! Frame codec (§6.2). Owned by WP1.9.
//!
//! One frame per WebSocket binary message:
//!
//! ```text
//! off len  field
//! 0   1    kind (FrameKind)
//! 1   1    flags: bit0 MORE, bit1 LAST
//! 2   2    reserved = 0
//! 4   4    header_len u32 LE
//! 8   n    header: one UTF-8 JSON object
//! 8+n m    body: raw bytes
//! ```
//!
//! Every bound is enforced on both encode and decode. A violation is a
//! [`FrameError`], which the session treats as a protocol error that closes
//! the connection:
//!
//! * a message shorter than the prefix or than its declared header
//!   ([`FrameError::Truncated`]);
//! * an unknown kind, unknown flag bits, or a flag the kind does not allow
//!   ([`FrameError::UnknownKind`], [`FrameError::UnknownFlags`]);
//! * a non-zero reserved field;
//! * a header over the limit (16 KiB before authentication, 64 KiB after) or a
//!   header that is not exactly one JSON object;
//! * a body over the limit (1 MiB), or any body bytes after the header of a
//!   kind that carries none ([`FrameError::TrailingBytes`] on decode,
//!   [`FrameError::BodyNotAllowed`] on encode).
//!
//! Flag rules: `MORE` is valid on REQUEST, RESPONSE and CHUNK (more frames of
//! the same request id follow); `LAST` is valid on CHUNK only and mirrors
//! `ChunkHeader.last`.
use crate::model::protocol::frame::{
    FLAG_LAST, FLAG_MORE, FrameKind, KNOWN_FLAGS, MAX_CHUNK, MAX_HEADER, MAX_HEADER_PRE_AUTH,
    PREFIX_LEN,
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

    /// The largest WebSocket message these limits admit.
    pub fn max_message(self) -> usize {
        PREFIX_LEN + self.max_header + self.max_body
    }
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
        parse_header(&self.header)
    }

    pub fn more(&self) -> bool {
        self.flags & FLAG_MORE != 0
    }

    pub fn last(&self) -> bool {
        self.flags & FLAG_LAST != 0
    }

    /// Borrows this frame as a [`FrameRef`].
    pub fn as_ref(&self) -> FrameRef<'_> {
        FrameRef {
            kind: self.kind,
            flags: self.flags,
            header: &self.header,
            body: &self.body,
        }
    }
}

/// A decoded frame borrowing the message it came from (no copies).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameRef<'a> {
    pub kind: FrameKind,
    pub flags: u8,
    pub header: &'a [u8],
    pub body: &'a [u8],
}

impl<'a> FrameRef<'a> {
    /// Parses the header JSON.
    pub fn header_as<T: DeserializeOwned>(&self) -> Result<T, FrameError> {
        parse_header(self.header)
    }

    pub fn more(&self) -> bool {
        self.flags & FLAG_MORE != 0
    }

    pub fn last(&self) -> bool {
        self.flags & FLAG_LAST != 0
    }

    pub fn to_owned(&self) -> Frame {
        Frame {
            kind: self.kind,
            flags: self.flags,
            header: self.header.to_vec(),
            body: self.body.to_vec(),
        }
    }
}

/// A protocol violation found by the codec.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrameError {
    Truncated,
    TrailingBytes,
    UnknownKind(u8),
    /// Unknown flag bits, or a flag the frame kind does not allow.
    UnknownFlags(u8),
    ReservedNonZero,
    HeaderTooLarge {
        len: usize,
        max: usize,
    },
    BodyTooLarge {
        len: usize,
        max: usize,
    },
    BodyNotAllowed(FrameKind),
    InvalidHeader(String),
    /// Returned only by the Wave 0 stub; the codec never produces it.
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
                write!(
                    f,
                    "PROTOCOL_VIOLATION: frame flags {flags:#04x} are unknown or not allowed for this kind"
                )
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

/// Flag bits `kind` may carry.
pub fn allowed_flags(kind: FrameKind) -> u8 {
    match kind {
        FrameKind::Chunk => FLAG_MORE | FLAG_LAST,
        FrameKind::Request | FrameKind::Response => FLAG_MORE,
        _ => 0,
    }
}

fn check_flags(kind: FrameKind, flags: u8) -> Result<(), FrameError> {
    if flags & !KNOWN_FLAGS != 0 || flags & !allowed_flags(kind) != 0 {
        return Err(FrameError::UnknownFlags(flags));
    }
    Ok(())
}

fn check_header(header: &[u8], limits: FrameLimits) -> Result<(), FrameError> {
    if header.len() > limits.max_header {
        return Err(FrameError::HeaderTooLarge {
            len: header.len(),
            max: limits.max_header,
        });
    }
    // Exactly one JSON object: the first token is `{`, the whole text parses,
    // and nothing follows it (serde rejects trailing characters).
    if header.iter().find(|byte| !byte.is_ascii_whitespace()) != Some(&b'{') {
        return Err(FrameError::InvalidHeader("not a JSON object".into()));
    }
    serde_json::from_slice::<serde::de::IgnoredAny>(header)
        .map(|_| ())
        .map_err(|error| FrameError::InvalidHeader(error.to_string()))
}

fn check_body_len(len: usize, limits: FrameLimits) -> Result<(), FrameError> {
    if len > limits.max_body {
        return Err(FrameError::BodyTooLarge {
            len,
            max: limits.max_body,
        });
    }
    Ok(())
}

fn parse_header<T: DeserializeOwned>(header: &[u8]) -> Result<T, FrameError> {
    serde_json::from_slice(header).map_err(|error| FrameError::InvalidHeader(error.to_string()))
}

fn prefix(kind: FrameKind, flags: u8, header_len: usize) -> [u8; PREFIX_LEN] {
    let len = (header_len as u32).to_le_bytes();
    [kind.as_u8(), flags, 0, 0, len[0], len[1], len[2], len[3]]
}

/// Encodes one frame (one WebSocket binary message).
pub fn encode(frame: &Frame, limits: FrameLimits) -> Result<Vec<u8>, FrameError> {
    encode_parts(frame.kind, frame.flags, &frame.header, &frame.body, limits)
}

/// Encodes a frame from raw header JSON bytes and a body.
pub fn encode_parts(
    kind: FrameKind,
    flags: u8,
    header: &[u8],
    body: &[u8],
    limits: FrameLimits,
) -> Result<Vec<u8>, FrameError> {
    check_flags(kind, flags)?;
    check_header(header, limits)?;
    if kind.body_forbidden() && !body.is_empty() {
        return Err(FrameError::BodyNotAllowed(kind));
    }
    check_body_len(body.len(), limits)?;
    let mut out = Vec::with_capacity(PREFIX_LEN + header.len() + body.len());
    out.extend_from_slice(&prefix(kind, flags, header.len()));
    out.extend_from_slice(header);
    out.extend_from_slice(body);
    Ok(out)
}

/// Encodes a frame whose header is `header` serialized as JSON, writing the
/// header and the body straight into the output (one copy of the body).
pub fn encode_with<T: Serialize + ?Sized>(
    kind: FrameKind,
    flags: u8,
    header: &T,
    body: &[u8],
    limits: FrameLimits,
) -> Result<Vec<u8>, FrameError> {
    check_flags(kind, flags)?;
    if kind.body_forbidden() && !body.is_empty() {
        return Err(FrameError::BodyNotAllowed(kind));
    }
    check_body_len(body.len(), limits)?;
    let mut out = Vec::with_capacity(PREFIX_LEN + 256 + body.len());
    out.extend_from_slice(&[0; PREFIX_LEN]);
    serde_json::to_writer(&mut out, header)
        .map_err(|error| FrameError::InvalidHeader(error.to_string()))?;
    let header_len = out.len() - PREFIX_LEN;
    if header_len > limits.max_header {
        return Err(FrameError::HeaderTooLarge {
            len: header_len,
            max: limits.max_header,
        });
    }
    if out.get(PREFIX_LEN) != Some(&b'{') {
        return Err(FrameError::InvalidHeader("not a JSON object".into()));
    }
    out[..PREFIX_LEN].copy_from_slice(&prefix(kind, flags, header_len));
    out.extend_from_slice(body);
    Ok(out)
}

/// Decodes one frame from one WebSocket binary message.
pub fn decode(message: &[u8], limits: FrameLimits) -> Result<Frame, FrameError> {
    decode_ref(message, limits).map(|frame| frame.to_owned())
}

/// Decodes one frame without copying the header or the body.
pub fn decode_ref(message: &[u8], limits: FrameLimits) -> Result<FrameRef<'_>, FrameError> {
    if message.len() < PREFIX_LEN {
        return Err(FrameError::Truncated);
    }
    let kind = FrameKind::from_u8(message[0]).ok_or(FrameError::UnknownKind(message[0]))?;
    let flags = message[1];
    check_flags(kind, flags)?;
    if message[2] != 0 || message[3] != 0 {
        return Err(FrameError::ReservedNonZero);
    }
    let header_len = u32::from_le_bytes([message[4], message[5], message[6], message[7]]) as usize;
    if header_len > limits.max_header {
        return Err(FrameError::HeaderTooLarge {
            len: header_len,
            max: limits.max_header,
        });
    }
    let header_end = PREFIX_LEN
        .checked_add(header_len)
        .filter(|end| *end <= message.len())
        .ok_or(FrameError::Truncated)?;
    let header = &message[PREFIX_LEN..header_end];
    check_header(header, limits)?;
    let body = &message[header_end..];
    if kind.body_forbidden() && !body.is_empty() {
        return Err(FrameError::TrailingBytes);
    }
    check_body_len(body.len(), limits)?;
    Ok(FrameRef {
        kind,
        flags,
        header,
        body,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::protocol::frame::{CancelHeader, ChunkHeader, Heartbeat, RequestHeader};

    fn request(id: u64) -> RequestHeader {
        RequestHeader {
            id,
            op: "ping".into(),
            deadline_ms: Some(5000),
            idle_ms: None,
            header: serde_json::json!({}),
            total: None,
        }
    }

    #[test]
    fn round_trips_every_kind() {
        for kind in FrameKind::ALL {
            let body = if kind.body_forbidden() {
                vec![]
            } else {
                vec![1, 2, 3]
            };
            let frame = Frame::new(kind, 0, &CancelHeader { id: 7 }, body).unwrap();
            let bytes = encode(&frame, FrameLimits::SESSION).unwrap();
            assert_eq!(decode(&bytes, FrameLimits::SESSION).unwrap(), frame);
            let typed = encode_with(
                kind,
                0,
                &CancelHeader { id: 7 },
                &frame.body,
                FrameLimits::SESSION,
            )
            .unwrap();
            assert_eq!(typed, bytes);
        }
    }

    #[test]
    fn rejects_malformed_prefixes() {
        let good = encode_with(
            FrameKind::Request,
            0,
            &request(1),
            b"x",
            FrameLimits::SESSION,
        )
        .unwrap();
        assert_eq!(
            decode(&good[..7], FrameLimits::SESSION),
            Err(FrameError::Truncated)
        );
        let mut bad = good.clone();
        bad[0] = 0;
        assert_eq!(
            decode(&bad, FrameLimits::SESSION),
            Err(FrameError::UnknownKind(0))
        );
        let mut bad = good.clone();
        bad[0] = 12;
        assert_eq!(
            decode(&bad, FrameLimits::SESSION),
            Err(FrameError::UnknownKind(12))
        );
        let mut bad = good.clone();
        bad[1] = 0b100;
        assert_eq!(
            decode(&bad, FrameLimits::SESSION),
            Err(FrameError::UnknownFlags(0b100))
        );
        let mut bad = good.clone();
        bad[1] = FLAG_LAST;
        assert_eq!(
            decode(&bad, FrameLimits::SESSION),
            Err(FrameError::UnknownFlags(FLAG_LAST))
        );
        let mut bad = good.clone();
        bad[3] = 1;
        assert_eq!(
            decode(&bad, FrameLimits::SESSION),
            Err(FrameError::ReservedNonZero)
        );
        let mut bad = good.clone();
        bad[4..8].copy_from_slice(&(good.len() as u32).to_le_bytes());
        assert_eq!(
            decode(&bad, FrameLimits::SESSION),
            Err(FrameError::Truncated)
        );
    }

    #[test]
    fn enforces_header_and_body_bounds() {
        let big = "x".repeat(MAX_HEADER_PRE_AUTH);
        let header = serde_json::json!({ "pad": big });
        let bytes = encode_with(FrameKind::Hello, 0, &header, &[], FrameLimits::SESSION).unwrap();
        assert!(matches!(
            decode(&bytes, FrameLimits::PRE_AUTH),
            Err(FrameError::HeaderTooLarge { .. })
        ));
        assert!(matches!(
            encode_with(FrameKind::Hello, 0, &header, &[], FrameLimits::PRE_AUTH),
            Err(FrameError::HeaderTooLarge { .. })
        ));
        assert!(decode(&bytes, FrameLimits::SESSION).is_ok());

        let body = vec![0u8; MAX_CHUNK + 1];
        assert!(matches!(
            encode_with(
                FrameKind::Chunk,
                0,
                &ChunkHeader::default(),
                &body,
                FrameLimits::SESSION
            ),
            Err(FrameError::BodyTooLarge { .. })
        ));
        let mut raw = encode_with(
            FrameKind::Chunk,
            0,
            &ChunkHeader::default(),
            &[],
            FrameLimits::SESSION,
        )
        .unwrap();
        raw.extend_from_slice(&body);
        assert!(matches!(
            decode(&raw, FrameLimits::SESSION),
            Err(FrameError::BodyTooLarge { .. })
        ));
    }

    #[test]
    fn body_on_bodiless_kinds_is_rejected() {
        assert_eq!(
            encode_with(
                FrameKind::Heartbeat,
                0,
                &Heartbeat::default(),
                b"x",
                FrameLimits::SESSION
            ),
            Err(FrameError::BodyNotAllowed(FrameKind::Heartbeat))
        );
        let mut raw = encode_with(
            FrameKind::Heartbeat,
            0,
            &Heartbeat::default(),
            &[],
            FrameLimits::SESSION,
        )
        .unwrap();
        raw.push(0);
        assert_eq!(
            decode(&raw, FrameLimits::SESSION),
            Err(FrameError::TrailingBytes)
        );
    }

    #[test]
    fn header_must_be_one_json_object() {
        for header in [
            &b"[1]"[..],
            b"",
            b"{\"a\":1} x",
            b"{\"a\":",
            b"\xff{}",
            b"1",
        ] {
            assert!(
                matches!(
                    encode_parts(FrameKind::Cancel, 0, header, &[], FrameLimits::SESSION),
                    Err(FrameError::InvalidHeader(_))
                ),
                "{header:?}"
            );
            let mut raw = prefix(FrameKind::Cancel, 0, header.len()).to_vec();
            raw.extend_from_slice(header);
            assert!(
                matches!(
                    decode(&raw, FrameLimits::SESSION),
                    Err(FrameError::InvalidHeader(_))
                ),
                "{header:?}"
            );
        }
        assert!(encode_with(FrameKind::Cancel, 0, &5u32, &[], FrameLimits::SESSION).is_err());
        assert!(encode_parts(FrameKind::Cancel, 0, b" {} ", &[], FrameLimits::SESSION).is_ok());
    }

    #[test]
    fn flags_follow_the_kind() {
        for kind in FrameKind::ALL {
            for flags in 0..=3u8 {
                let ok = flags & !allowed_flags(kind) == 0;
                let body: &[u8] = &[];
                let result = encode_with(
                    kind,
                    flags,
                    &CancelHeader::default(),
                    body,
                    FrameLimits::SESSION,
                );
                assert_eq!(result.is_ok(), ok, "{kind:?} {flags}");
            }
        }
        let frame = decode(
            &encode_with(
                FrameKind::Chunk,
                FLAG_MORE | FLAG_LAST,
                &ChunkHeader::default(),
                b"x",
                FrameLimits::SESSION,
            )
            .unwrap(),
            FrameLimits::SESSION,
        )
        .unwrap();
        assert!(frame.more() && frame.last());
    }
}
