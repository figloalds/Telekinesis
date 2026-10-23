//! Application credentials over TLS/WSS or an enclosing VPN.
//! Channel protection belongs to that transport; no custom MAC or encryption.
use crate::{
    model::errors::ErrorWire, pairing_code::Verifier, session::handshake::ClientAuth,
    util::secret::ct_eq,
};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fmt, time::Duration};

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
#[derive(Clone)]
pub struct PeerAuth {
    pub key: [u8; 32],
}
impl ClientAuth for PeerAuth {
    fn key(&self, _salt: &str) -> Result<[u8; 32], ErrorWire> {
        Ok(self.key)
    }
    fn peer_key(&self) -> Option<[u8; 32]> {
        Some(self.key)
    }
}
impl fmt::Debug for PeerAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerAuth")
            .field("key", &"<redacted>")
            .finish()
    }
}

pub mod ops {
    pub use crate::model::protocol::ops::{PEER_ENROLL, PEER_REVOKE};
}
pub const ENROLL_DEADLINE: Duration = crate::model::protocol::deadlines::PEER_ENROLL;
pub const REVOKE_DEADLINE: Duration = crate::model::protocol::deadlines::PEER_REVOKE;
pub fn new_secret() -> [u8; 32] {
    let mut secret = [0; 32];
    rand::rngs::OsRng.fill_bytes(&mut secret);
    secret
}
/// Stable stored credential identity. This is not channel encryption.
pub fn peer_key(secret: &[u8; 32]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"tkfs-peer-secret-v1");
    digest.update(secret);
    digest.finalize().into()
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnrollRequest {
    pub secret: String,
}
impl fmt::Debug for EnrollRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnrollRequest")
            .field("secret", &"<redacted>")
            .finish()
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrollReply {
    pub enrolled: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevokeReply {
    pub revoked: bool,
}
pub fn enrollment_request(secret: &[u8; 32]) -> EnrollRequest {
    EnrollRequest {
        secret: hex::encode(secret),
    }
}
pub fn enrollment_secret(request: &EnrollRequest) -> Result<[u8; 32], ErrorWire> {
    let mut secret = [0; 32];
    if request.secret.len() != 64 || hex::decode_to_slice(&request.secret, &mut secret).is_err() {
        return Err(ErrorWire::coded(
            "INVALID_REQUEST",
            "peer.enroll: secret must contain 32 hex-encoded bytes",
        ));
    }
    Ok(secret)
}
pub const MAX_ID_LEN: usize = 128;
pub fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= MAX_ID_LEN && !id.chars().any(char::is_control)
}
pub fn verify_credential(expected: &[u8; 32], presented: &str) -> bool {
    let mut bytes = [0; 32];
    let decoded = presented.len() == 64 && hex::decode_to_slice(presented, &mut bytes).is_ok();
    ct_eq(expected, &bytes) && decoded
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn enrollment_uses_the_selected_transport_and_redacts_debug_output() {
        let secret = new_secret();
        let request = enrollment_request(&secret);
        assert_eq!(request.secret, hex::encode(secret));
        assert_eq!(enrollment_secret(&request).unwrap(), secret);
        assert!(!format!("{request:?}").contains(&request.secret));
        for value in [
            String::new(),
            "zz".repeat(32),
            "11".repeat(31),
            "11".repeat(33),
        ] {
            assert_eq!(
                enrollment_secret(&EnrollRequest { secret: value })
                    .unwrap_err()
                    .code,
                "INVALID_REQUEST"
            );
        }
    }
    #[test]
    fn ordinary_pairing_and_peer_credentials_are_checked_without_transcripts() {
        let (code, verifier) = Verifier::generate();
        let auth = CodeAuth { code: code.clone() };
        assert_eq!(auth.key(&verifier.salt).unwrap(), verifier.key());
        let key = peer_key(&new_secret());
        let peer = PeerAuth { key };
        assert_eq!(peer.peer_key(), Some(key));
        assert!(verify_credential(&key, &hex::encode(key)));
        assert!(!verify_credential(&key, &hex::encode([0; 32])));
        assert!(!verify_credential(&key, "bad"));
        assert!(!format!("{auth:?}").contains(&code));
        assert!(!format!("{peer:?}").contains(&hex::encode(key)));
    }
}
