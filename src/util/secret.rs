//! Comparison for ordinary application credentials.
/// Constant-time equality for equal-length inputs (the length itself is not
/// secret). Unequal lengths compare unequal.
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let difference = a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y));
    std::hint::black_box(difference) == 0
}
