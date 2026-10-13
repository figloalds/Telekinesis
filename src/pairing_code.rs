//! Pairing has one credential: a code checked against a persisted salted hash.
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Verifier {
    pub salt: String,
    pub hash: String,
}
/// `hash` is HMAC-key-equivalent to the pairing code (REVAMP_PLAN §11.15,
/// invariant 21), so `Debug` never prints it (CCR-WP0.4-2, applied by WP0.INT).
impl std::fmt::Debug for Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Verifier")
            .field("salt", &self.salt)
            .field("hash", &"<redacted>")
            .finish()
    }
}
impl Verifier {
    pub fn generate() -> (String, Self) {
        let mut bytes = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        let code = hex::encode(bytes);
        rand::rngs::OsRng.fill_bytes(&mut bytes);
        let salt = hex::encode(bytes);
        let hash = Self::digest(&salt, &code);
        (code, Self { salt, hash })
    }
    fn digest(salt: &str, code: &str) -> String {
        let mut digest = Sha256::new();
        digest.update(b"tkfs-pairing-v2\0");
        digest.update(salt.as_bytes());
        digest.update(b"\0");
        digest.update(code.as_bytes());
        hex::encode(digest.finalize())
    }
    pub fn verify(&self, code: &str) -> bool {
        Self::digest(&self.salt, code) == self.hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn code_is_random_salted_and_verifier_survives_serialization() {
        let (code, verifier) = Verifier::generate();
        let (other, second) = Verifier::generate();
        assert_eq!(code.len(), 64);
        assert_ne!(code, other);
        assert_ne!(verifier.salt, second.salt);
        let stored = serde_json::to_string(&verifier).unwrap();
        assert!(!stored.contains(&code));
        let restored: Verifier = serde_json::from_str(&stored).unwrap();
        assert!(restored.verify(&code));
        assert!(!restored.verify(&other));
    }
    #[test]
    fn verifier_debug_redacts_the_hash() {
        let (code, verifier) = Verifier::generate();
        let printed = format!("{verifier:?} {verifier:#?}");
        assert!(!printed.contains(&verifier.hash));
        assert!(!printed.contains(&code));
        assert!(printed.contains(&verifier.salt));
        assert!(printed.contains("<redacted>"));
        assert_eq!(verifier.clone(), verifier);
        assert_ne!(verifier, Verifier::generate().1);
    }
}
