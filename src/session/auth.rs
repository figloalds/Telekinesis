//! Client credentials and the HMAC transcript (§6.2 Handshake steps 3–4).
//! Implemented by WP1.10 over `util::hmac` (RFC 2104 HMAC-SHA256).
//!
//! * `K` is the 32-byte verifier digest: the server holds it as
//!   [`Verifier::key`] (the decoded `Verifier.hash`), the client derives it
//!   from the server's salt and its pairing code with
//!   [`Verifier::derive_key`]. The code itself never crosses the wire.
//! * `T` is the transcript of [`Transcript::new`]; the client proves
//!   `HMAC(K, "client" ‖ T)` and the server answers `HMAC(K, "server" ‖ T)`
//!   (mutual authentication). Both nonces are in `T`, so a proof captured on
//!   one connection is useless on any other.
use crate::{
    model::{
        errors::ErrorWire,
        protocol::{
            AUTH_CONTEXT, LaneRole, LaneSpec,
            frame::{ClientHello, NONCE_BYTES, ServerHello},
        },
    },
    pairing_code::Verifier,
    session::handshake::ClientAuth,
    util::hmac::{ct_eq, hmac_sha256_parts},
};
use rand::RngCore;
use std::fmt;

/// Authenticates with the plaintext pairing code the dialer stores today
/// (`peers.code`). The code never crosses the wire: only `K`, derived from it
/// and the server's salt, keys the proofs.
#[derive(Clone)]
pub struct CodeAuth {
    pub code: String,
}

impl ClientAuth for CodeAuth {
    fn key(&self, salt: &str) -> Result<[u8; 32], ErrorWire> {
        Ok(Verifier::derive_key(salt, &self.code))
    }
}

impl fmt::Debug for CodeAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CodeAuth")
            .field("code", &"<redacted>")
            .finish()
    }
}

/// Label prefixed to `T` for the client's proof.
pub const CLIENT_LABEL: &[u8] = b"client";
/// Label prefixed to `T` for the server's proof.
pub const SERVER_LABEL: &[u8] = b"server";
/// Longest installation id or session id accepted in a hello (bytes).
pub const MAX_ID_LEN: usize = 128;

/// A fresh 32-byte nonce, hex-encoded (`client_nonce` / `server_nonce`).
pub fn new_nonce() -> String {
    let mut bytes = [0u8; NONCE_BYTES];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// The raw bytes of a hex nonce; `None` unless it is exactly 32 bytes.
pub fn decode_nonce(nonce: &str) -> Option<[u8; NONCE_BYTES]> {
    let mut bytes = [0u8; NONCE_BYTES];
    (nonce.len() == 2 * NONCE_BYTES && hex::decode_to_slice(nonce, &mut bytes).is_ok())
        .then_some(bytes)
}

/// Whether `id` may appear in the transcript: 1..=128 bytes with no NUL or
/// other control character (NUL separates the variable-length fields of `T`).
pub fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= MAX_ID_LEN && !id.chars().any(char::is_control)
}

/// The authentication transcript `T` (§6.2):
///
/// ```text
/// "tkfs-peer-auth-v3" ‖ 0 ‖ client_inst ‖ 0 ‖ server_inst ‖ 0 ‖ role ‖ 0 ‖
/// lane_index u32 LE ‖ lane_count u32 LE ‖ session ‖ client_nonce (32 raw bytes) ‖
/// server_nonce (32 raw bytes) ‖ verifier_generation u64 LE
/// ```
///
/// A missing lane encodes as `0, 0`. The variable-length fields are either
/// NUL-terminated or followed only by fixed-length fields, so `T` is an
/// injective encoding of its inputs.
#[derive(Clone, PartialEq, Eq)]
pub struct Transcript {
    bytes: Vec<u8>,
}

impl Transcript {
    /// The transcript of one handshake. Fails (`PROTOCOL_VIOLATION`) when an
    /// id is not [`valid_id`] or a nonce is not 32 hex-encoded bytes.
    #[allow(clippy::too_many_arguments)]
    pub fn from_parts(
        client_installation: &str,
        server_installation: &str,
        role: LaneRole,
        lane: Option<LaneSpec>,
        session: &str,
        client_nonce: &str,
        server_nonce: &str,
        verifier_generation: u64,
    ) -> Result<Transcript, ErrorWire> {
        for (name, id) in [
            ("client installation", client_installation),
            ("server installation", server_installation),
            ("session", session),
        ] {
            if !valid_id(id) {
                return Err(ErrorWire::coded(
                    "PROTOCOL_VIOLATION",
                    format!("invalid {name} in the handshake"),
                ));
            }
        }
        let nonce = |name: &str, value: &str| {
            decode_nonce(value).ok_or_else(|| {
                ErrorWire::coded(
                    "PROTOCOL_VIOLATION",
                    format!("{name} must be {NONCE_BYTES} hex-encoded bytes"),
                )
            })
        };
        let client_nonce = nonce("client_nonce", client_nonce)?;
        let server_nonce = nonce("server_nonce", server_nonce)?;
        let lane = lane.unwrap_or(LaneSpec { index: 0, count: 0 });
        let mut bytes = Vec::with_capacity(256);
        bytes.extend_from_slice(AUTH_CONTEXT.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(client_installation.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(server_installation.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(role.as_str().as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(&lane.index.to_le_bytes());
        bytes.extend_from_slice(&lane.count.to_le_bytes());
        bytes.extend_from_slice(session.as_bytes());
        bytes.extend_from_slice(&client_nonce);
        bytes.extend_from_slice(&server_nonce);
        bytes.extend_from_slice(&verifier_generation.to_le_bytes());
        Ok(Transcript { bytes })
    }

    /// The transcript of the handshake that exchanged `client` and `server`.
    /// Role and lane come from the client's hello (the server must echo them;
    /// the handshake checks that separately).
    pub fn new(client: &ClientHello, server: &ServerHello) -> Result<Transcript, ErrorWire> {
        Self::from_parts(
            &client.installation,
            &server.installation,
            client.role,
            client.lane,
            &client.session,
            &client.client_nonce,
            &server.server_nonce,
            server.verifier_generation,
        )
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// `HMAC-SHA256(K, "client" ‖ T)`.
    pub fn client_proof(&self, key: &[u8; 32]) -> [u8; 32] {
        hmac_sha256_parts(key, &[CLIENT_LABEL, &self.bytes])
    }

    /// `HMAC-SHA256(K, "server" ‖ T)`.
    pub fn server_proof(&self, key: &[u8; 32]) -> [u8; 32] {
        hmac_sha256_parts(key, &[SERVER_LABEL, &self.bytes])
    }
}

impl fmt::Debug for Transcript {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transcript")
            .field("len", &self.bytes.len())
            .finish()
    }
}

/// Constant-time check of a hex-encoded proof against the expected MAC.
/// Anything that is not exactly 64 hex digits fails.
pub fn verify_proof(expected: &[u8; 32], presented: &str) -> bool {
    let mut bytes = [0u8; 32];
    let decoded = presented.len() == 64 && hex::decode_to_slice(presented, &mut bytes).is_ok();
    // Compare even when decoding failed, so timing does not depend on where
    // a malformed proof went wrong.
    ct_eq(expected, &bytes) && decoded
}
