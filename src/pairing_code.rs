//! Pairing has one credential: a code checked against a persisted salted hash.
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Verifier {
    pub salt: String,
    pub hash: String,
}
/// `hash` is an application credential, so `Debug` never prints it.
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
        hex::encode(Self::derive_key(salt, code))
    }
    pub fn verify(&self, code: &str) -> bool {
        crate::util::secret::ct_eq(
            Self::digest(&self.salt, code).as_bytes(),
            self.hash.as_bytes(),
        )
    }
    /// The 32-byte credential a dialer derives from the server's `salt` and its
    /// pairing code. It is the raw digest whose hex
    /// form is [`Verifier::hash`], so it equals [`Verifier::key`] exactly when
    /// the code is right. The code itself never crosses the wire.
    pub fn derive_key(salt: &str, code: &str) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"tkfs-pairing-v2\0");
        digest.update(salt.as_bytes());
        digest.update(b"\0");
        digest.update(code.as_bytes());
        digest.finalize().into()
    }
    /// The 32-byte credential of this verifier: [`Verifier::hash`] decoded.
    ///
    /// A malformed stored hash (not 64 hex digits) yields a key that no
    /// pairing code derives (it uses a different domain-separation prefix),
    /// so every handshake against it fails closed with `INVALID_PAIRING_CODE`.
    pub fn key(&self) -> [u8; 32] {
        let mut key = [0u8; 32];
        if self.hash.len() == 64 && hex::decode_to_slice(&self.hash, &mut key).is_ok() {
            return key;
        }
        let mut digest = Sha256::new();
        digest.update(b"tkfs-unusable-verifier\0");
        digest.update(self.hash.as_bytes());
        digest.finalize().into()
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
    fn key_derivation_matches_the_stored_verifier() {
        let (code, verifier) = Verifier::generate();
        assert_eq!(Verifier::derive_key(&verifier.salt, &code), verifier.key());
        assert_eq!(hex::encode(verifier.key()), verifier.hash);
        assert_ne!(
            Verifier::derive_key(&verifier.salt, "wrong"),
            verifier.key()
        );
        assert_ne!(Verifier::derive_key("other-salt", &code), verifier.key());
        let broken = Verifier {
            salt: verifier.salt.clone(),
            hash: "not-hex".into(),
        };
        assert_ne!(broken.key(), Verifier::derive_key(&broken.salt, &code));
        assert_ne!(broken.key(), [0u8; 32]);
        assert!(!broken.verify(&code));
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
