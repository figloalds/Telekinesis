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
//!
//! **Per-peer credentials (WP4.1, §14).** After a dialer authenticated with
//! the pairing code, it enrolls a per-peer secret: 32 random bytes `S`, sent
//! once in `peer.enroll` ([`EnrollRequest`]). Both sides keep
//! `K_peer = SHA-256("tkfs-peer-secret-v1" ‖ S)` ([`peer_key`]) in
//! `peer_credentials`, and later sessions authenticate with the scheme
//! [`PEER_HMAC_V1`]: the dialer offers it in `ClientHello.features`, a server
//! holding an active credential for the dialer echoes it in
//! `ServerHello.features`, and both proofs are keyed by `K_peer` over the
//! transcript with its own context ([`PEER_AUTH_CONTEXT`], so a proof of one
//! scheme never verifies under the other). `S` never crosses the wire in the
//! clear: the request carries it XORed with a pad derived from the pairing
//! code's key `K` and a fresh nonce, plus a MAC over it, so a passive
//! observer of a plain `ws://` session learns nothing, and a server whose
//! code rotated since the session opened refuses it.
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
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fmt, time::Duration};

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

/// Authenticates with a per-peer credential `K_peer` ([`PEER_HMAC_V1`]),
/// falling back to the pairing code when the server does not offer the
/// scheme (an older acceptor). Without a code there is no fallback.
#[derive(Clone)]
pub struct PeerAuth {
    pub key: [u8; 32],
    pub code: Option<String>,
}

impl ClientAuth for PeerAuth {
    fn key(&self, salt: &str) -> Result<[u8; 32], ErrorWire> {
        match &self.code {
            Some(code) => Ok(Verifier::derive_key(salt, code)),
            None => Err(ErrorWire::coded(
                "PEER_REVOKED",
                "the server does not accept this peer credential and no pairing code is stored",
            )),
        }
    }

    fn peer_key(&self) -> Option<[u8; 32]> {
        Some(self.key)
    }
}

impl fmt::Debug for PeerAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerAuth")
            .field("key", &"<redacted>")
            .field("code", &self.code.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// The per-peer scheme's name in `ClientHello.features` and
/// `ServerHello.features`.
pub const PEER_HMAC_V1: &str = "peer-hmac-v1";
/// Domain separation of the per-peer transcript (in place of
/// `AUTH_CONTEXT`).
pub const PEER_AUTH_CONTEXT: &str = "tkfs-peer-hmac-v1";
/// Domain separation of `K_peer`.
pub const PEER_SECRET_CONTEXT: &[u8] = b"tkfs-peer-secret-v1";
/// Domain separation of the enrollment pad and MAC.
const ENROLL_CONTEXT: &[u8] = b"tkfs-peer-enroll-v1";

/// The peer ops WP4.1 adds (§14 WP4.1; CCR-WP4.1-2 asks for them in
/// `model::protocol::ops`). Both are allowed on any session role; the server
/// applies them to the session's **authenticated** installation only.
pub mod ops {
    /// `peer.enroll`: header [`EnrollRequest`](super::EnrollRequest), reply
    /// [`EnrollReply`](super::EnrollReply).
    pub const PEER_ENROLL: &str = "peer.enroll";
    /// `peer.revoke`: header `{}`, reply [`RevokeReply`](super::RevokeReply).
    /// The dialer's best-effort notice before it unpairs: the acceptor
    /// revokes the dialer's credential and closes its sessions.
    pub const PEER_REVOKE: &str = "peer.revoke";
}

/// Deadline of `peer.enroll`.
pub const ENROLL_DEADLINE: Duration = Duration::from_secs(10);
/// Deadline of `peer.revoke` (an unpair waits for it at most this long).
pub const REVOKE_DEADLINE: Duration = Duration::from_secs(2);

/// A fresh per-peer secret `S`.
pub fn new_secret() -> [u8; 32] {
    let mut secret = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut secret);
    secret
}

/// `K_peer = SHA-256("tkfs-peer-secret-v1" ‖ S)`.
pub fn peer_key(secret: &[u8; 32]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(PEER_SECRET_CONTEXT);
    digest.update(secret);
    digest.finalize().into()
}

/// `peer.enroll` request header: `S` sealed under the pairing code's key.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrollRequest {
    /// 32 fresh random bytes, hex.
    pub nonce: String,
    /// `S ⊕ HMAC(K, "tkfs-peer-enroll-v1" ‖ 0 ‖ "pad" ‖ 0 ‖ client ‖ 0 ‖ server ‖ 0 ‖ nonce)`, hex.
    pub secret: String,
    /// `HMAC(K, "tkfs-peer-enroll-v1" ‖ 0 ‖ "mac" ‖ 0 ‖ client ‖ 0 ‖ server ‖ 0 ‖ nonce ‖ S)`, hex.
    pub mac: String,
}

impl fmt::Debug for EnrollRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnrollRequest")
            .field("nonce", &self.nonce)
            .field("secret", &"<redacted>")
            .field("mac", &"<redacted>")
            .finish()
    }
}

/// `peer.enroll` reply header.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrollReply {
    pub enrolled: bool,
}

/// `peer.revoke` reply header.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevokeReply {
    pub revoked: bool,
}

fn enroll_mac(
    code_key: &[u8; 32],
    label: &[u8],
    client: &str,
    server: &str,
    nonce: &[u8; NONCE_BYTES],
    tail: &[u8],
) -> [u8; 32] {
    hmac_sha256_parts(
        code_key,
        &[
            ENROLL_CONTEXT,
            &[0],
            label,
            &[0],
            client.as_bytes(),
            &[0],
            server.as_bytes(),
            &[0],
            nonce,
            tail,
        ],
    )
}

/// Seals `secret` for `peer.enroll` from `client` (the dialer's
/// installation) to `server`, under the pairing code's key `code_key`
/// (`Verifier::derive_key(salt, code)`).
pub fn seal_enrollment(
    code_key: &[u8; 32],
    client: &str,
    server: &str,
    secret: &[u8; 32],
) -> EnrollRequest {
    let mut nonce = [0u8; NONCE_BYTES];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    let pad = enroll_mac(code_key, b"pad", client, server, &nonce, &[]);
    let mut sealed = [0u8; 32];
    for (index, byte) in sealed.iter_mut().enumerate() {
        *byte = secret[index] ^ pad[index];
    }
    let mac = enroll_mac(code_key, b"mac", client, server, &nonce, secret);
    EnrollRequest {
        nonce: hex::encode(nonce),
        secret: hex::encode(sealed),
        mac: hex::encode(mac),
    }
}

/// Opens a `peer.enroll` request with the server's verifier key. Fails
/// (`INVALID_PAIRING_CODE`) unless it was sealed under the same key for the
/// same pair of installations; a malformed request is `INVALID_REQUEST`.
pub fn open_enrollment(
    code_key: &[u8; 32],
    client: &str,
    server: &str,
    request: &EnrollRequest,
) -> Result<[u8; 32], ErrorWire> {
    let invalid = || {
        ErrorWire::coded(
            "INVALID_REQUEST",
            "peer.enroll: nonce, secret and mac must be 32 hex-encoded bytes each",
        )
    };
    let nonce = decode_nonce(&request.nonce).ok_or_else(invalid)?;
    let mut sealed = [0u8; 32];
    if request.secret.len() != 64 || hex::decode_to_slice(&request.secret, &mut sealed).is_err() {
        return Err(invalid());
    }
    let pad = enroll_mac(code_key, b"pad", client, server, &nonce, &[]);
    let mut secret = [0u8; 32];
    for (index, byte) in secret.iter_mut().enumerate() {
        *byte = sealed[index] ^ pad[index];
    }
    let expected = enroll_mac(code_key, b"mac", client, server, &nonce, &secret);
    if !verify_proof(&expected, &request.mac) {
        return Err(ErrorWire::coded(
            "INVALID_PAIRING_CODE",
            "peer.enroll was not sealed with this orchestrator's pairing code",
        ));
    }
    Ok(secret)
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
        Self::in_context(
            AUTH_CONTEXT,
            client_installation,
            server_installation,
            role,
            lane,
            session,
            client_nonce,
            server_nonce,
            verifier_generation,
        )
    }

    /// [`Transcript::from_parts`] with another domain-separation prefix (the
    /// per-peer scheme's [`PEER_AUTH_CONTEXT`]).
    #[allow(clippy::too_many_arguments)]
    pub fn in_context(
        context: &str,
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
        bytes.extend_from_slice(context.as_bytes());
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

    /// The transcript of a per-peer ([`PEER_HMAC_V1`]) handshake: the fields
    /// of [`Transcript::new`] under [`PEER_AUTH_CONTEXT`].
    pub fn per_peer(client: &ClientHello, server: &ServerHello) -> Result<Transcript, ErrorWire> {
        Self::in_context(
            PEER_AUTH_CONTEXT,
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

#[cfg(test)]
mod tests {
    use super::*;

    const CLIENT: &str = "client-installation";
    const SERVER: &str = "server-installation";

    #[test]
    fn an_enrollment_opens_only_with_the_same_code_key_and_pair() {
        let (code, verifier) = Verifier::generate();
        let key = Verifier::derive_key(&verifier.salt, &code);
        let secret = new_secret();
        let sealed = seal_enrollment(&key, CLIENT, SERVER, &secret);
        // The secret never travels in the clear.
        assert_ne!(sealed.secret, hex::encode(secret));
        assert!(!format!("{sealed:?}").contains(&sealed.secret));
        assert_eq!(
            open_enrollment(&verifier.key(), CLIENT, SERVER, &sealed).unwrap(),
            secret
        );
        // Another code, another dialer or server, a tampered secret or mac:
        // refused.
        let (_, other) = Verifier::generate();
        let refused = |error: ErrorWire| error.code;
        assert_eq!(
            refused(open_enrollment(&other.key(), CLIENT, SERVER, &sealed).unwrap_err()),
            "INVALID_PAIRING_CODE"
        );
        assert_eq!(
            refused(open_enrollment(&key, "someone-else", SERVER, &sealed).unwrap_err()),
            "INVALID_PAIRING_CODE"
        );
        assert_eq!(
            refused(open_enrollment(&key, CLIENT, "another-server", &sealed).unwrap_err()),
            "INVALID_PAIRING_CODE"
        );
        let mut tampered = sealed.clone();
        let flipped = if tampered.secret.starts_with('0') {
            "1"
        } else {
            "0"
        };
        tampered.secret.replace_range(..1, flipped);
        assert_eq!(
            refused(open_enrollment(&key, CLIENT, SERVER, &tampered).unwrap_err()),
            "INVALID_PAIRING_CODE"
        );
        let mut malformed = sealed.clone();
        malformed.nonce = "zz".into();
        assert_eq!(
            refused(open_enrollment(&key, CLIENT, SERVER, &malformed).unwrap_err()),
            "INVALID_REQUEST"
        );
        // Two seals of one secret differ (fresh nonce, fresh pad).
        let again = seal_enrollment(&key, CLIENT, SERVER, &secret);
        assert_ne!(again.nonce, sealed.nonce);
        assert_ne!(again.secret, sealed.secret);
    }

    #[test]
    fn the_per_peer_key_and_transcript_are_domain_separated() {
        let secret = [7u8; 32];
        let key = peer_key(&secret);
        assert_ne!(key, secret);
        assert_eq!(key, peer_key(&secret));
        assert_ne!(key, peer_key(&[8u8; 32]));
        let nonce = new_nonce();
        let other = new_nonce();
        let parts = |context| {
            Transcript::in_context(
                context,
                CLIENT,
                SERVER,
                LaneRole::Control,
                None,
                "session",
                &nonce,
                &other,
                3,
            )
            .unwrap()
        };
        let code = parts(AUTH_CONTEXT);
        let per_peer = parts(PEER_AUTH_CONTEXT);
        assert_ne!(code, per_peer);
        // A proof of one scheme never verifies under the other.
        assert!(!verify_proof(
            &code.client_proof(&key),
            &hex::encode(per_peer.client_proof(&key))
        ));
        assert_eq!(
            code,
            Transcript::from_parts(
                CLIENT,
                SERVER,
                LaneRole::Control,
                None,
                "session",
                &nonce,
                &other,
                3
            )
            .unwrap()
        );
    }

    #[test]
    fn peer_auth_falls_back_to_the_code_and_redacts_both() {
        let (code, verifier) = Verifier::generate();
        let key = peer_key(&new_secret());
        let auth = PeerAuth {
            key,
            code: Some(code.clone()),
        };
        assert_eq!(auth.peer_key(), Some(key));
        assert_eq!(auth.key(&verifier.salt).unwrap(), verifier.key());
        let printed = format!("{auth:?}");
        assert!(!printed.contains(&code) && !printed.contains(&hex::encode(key)));
        let without = PeerAuth { key, code: None };
        assert_eq!(
            without.key(&verifier.salt).unwrap_err().code,
            "PEER_REVOKED"
        );
    }
}
