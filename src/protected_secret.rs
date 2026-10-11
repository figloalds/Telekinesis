//! Encrypted owner-local protocol material. No plaintext fallback.
use anyhow::{Result, ensure};
use zeroize::Zeroizing;

const LIMIT: usize = 64 * 1024;
const PREFIX: &[u8] = b"tkfs-protected-secret-v1\0";

fn validate(context: &str) -> Result<()> {
    ensure!(
        !context.is_empty()
            && context.len() <= 128
            && context
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)),
        "INVALID_CREDENTIAL_NAME"
    );
    Ok(())
}

pub fn seal(context: &str, plaintext: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
    validate(context)?;
    ensure!(plaintext.len() <= LIMIT, "CREDENTIAL_TOO_LARGE");
    let mut bound = Zeroizing::new(Vec::with_capacity(context.len() + plaintext.len() + 1));
    bound.extend_from_slice(context.as_bytes());
    bound.push(0);
    bound.extend_from_slice(plaintext);
    let encrypted = platform(context, &bound, true)?;
    let mut envelope = Zeroizing::new(PREFIX.to_vec());
    envelope.extend_from_slice(&encrypted);
    Ok(envelope)
}

pub fn open(context: &str, envelope: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
    validate(context)?;
    ensure!(
        envelope.len() <= LIMIT * 2 && envelope.starts_with(PREFIX),
        "CREDENTIAL_INVALID"
    );
    let plaintext = platform(context, &envelope[PREFIX.len()..], false)?;
    let mut prefix = context.as_bytes().to_vec();
    prefix.push(0);
    ensure!(
        plaintext.starts_with(&prefix),
        "CREDENTIAL_IDENTITY_MISMATCH"
    );
    ensure!(
        plaintext.len() - prefix.len() <= LIMIT,
        "CREDENTIAL_TOO_LARGE"
    );
    Ok(Zeroizing::new(plaintext[prefix.len()..].to_vec()))
}

#[cfg(windows)]
fn platform(_context: &str, bytes: &[u8], encrypt: bool) -> Result<Zeroizing<Vec<u8>>> {
    crate::pairing::dpapi(bytes, encrypt)
}

#[cfg(target_os = "linux")]
fn platform(context: &str, bytes: &[u8], encrypt: bool) -> Result<Zeroizing<Vec<u8>>> {
    linux::crypt(context, bytes, encrypt)
}

#[cfg(not(any(windows, target_os = "linux")))]
fn platform(_context: &str, _bytes: &[u8], _encrypt: bool) -> Result<Zeroizing<Vec<u8>>> {
    anyhow::bail!("CREDENTIAL_BACKEND_UNAVAILABLE: automatic encrypted storage is unavailable")
}

#[cfg(target_os = "linux")]
mod linux;

#[cfg(all(test, target_os = "linux"))]
pub(crate) fn unavailable_fixture<T>(f: impl FnOnce() -> T) -> T {
    linux::unavailable_fixture(f)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_invalid_envelopes_and_names_without_platform_access() {
        assert!(seal("", b"fixture").is_err());
        assert!(seal("../alias", b"fixture").is_err());
        assert!(open("fixture", b"plaintext").is_err());
        assert!(seal("fixture", &vec![0; LIMIT + 1]).is_err());
    }
    #[cfg(windows)]
    #[test]
    fn encrypted_roundtrip_binds_context_and_rejects_tamper() {
        let secret = b"synthetic protocol material";
        let mut sealed = seal("tkfs-test-one", secret).unwrap();
        assert!(!sealed.windows(secret.len()).any(|w| w == secret));
        assert_eq!(&**open("tkfs-test-one", &sealed).unwrap(), secret);
        assert!(open("tkfs-test-two", &sealed).is_err());
        let end = sealed.len() - 1;
        sealed[end] ^= 1;
        assert!(open("tkfs-test-one", &sealed).is_err());
    }
}
