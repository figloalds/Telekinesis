//! Client credentials and the HMAC transcript (§6.2 Handshake steps 3–4).
//! Stub landed by WP0.4; WP1.10 implements key derivation
//! (`Verifier::derive_key`) and the transcript/proof helpers over
//! `util::hmac`.
use crate::{
    model::errors::ErrorWire,
    session::{handshake::ClientAuth, not_implemented},
};
use std::fmt;

/// Authenticates with the plaintext pairing code the dialer stores today
/// (`peers.code`). The code never crosses the wire.
#[derive(Clone)]
pub struct CodeAuth {
    pub code: String,
}

impl ClientAuth for CodeAuth {
    /// Stub: WP1.10 returns `Verifier::derive_key(salt, &self.code)`.
    fn key(&self, salt: &str) -> Result<[u8; 32], ErrorWire> {
        let _ = (salt, &self.code);
        Err(not_implemented("session::CodeAuth::key"))
    }
}

impl fmt::Debug for CodeAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CodeAuth")
            .field("code", &"<redacted>")
            .finish()
    }
}
