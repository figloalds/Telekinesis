//! RFC 2104 HMAC-SHA256 over `sha2` 0.10 (no `hmac` crate, §11 decision 3),
//! checked against RFC 4231 test cases 1–7 in `tests/contracts.rs`.
use sha2::{Digest, Sha256};

const BLOCK: usize = 64;
const IPAD: u8 = 0x36;
const OPAD: u8 = 0x5c;

/// HMAC-SHA256(key, data).
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    hmac_sha256_parts(key, &[data])
}

/// HMAC-SHA256 of the concatenation of `parts`, without concatenating them.
pub fn hmac_sha256_parts(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut block = [0u8; BLOCK];
    if key.len() > BLOCK {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(block.map(|byte| byte ^ IPAD));
    for part in parts {
        inner.update(part);
    }
    let inner = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(block.map(|byte| byte ^ OPAD));
    outer.update(inner);
    outer.finalize().into()
}

/// Constant-time equality for equal-length inputs (the length itself is not
/// secret). Unequal lengths compare unequal.
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let difference = a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y));
    std::hint::black_box(difference) == 0
}
