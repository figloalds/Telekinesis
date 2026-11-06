//! The pairing link a phone scans: `tkfs://pair?endpoint=<encoded>&code=<hex>`.
//!
//! The link contains the pairing code, so it is a secret exactly like the
//! code. It is built to be shown (a QR beside the code, `--qr`) and never
//! logged or put in an event.

#[cfg(not(target_os = "android"))]
use anyhow::{Context, Result};

/// The link for `code`, with `endpoint` when this PC knows an address a phone
/// could dial.
pub fn link(endpoint: Option<&str>, code: &str) -> String {
    match endpoint {
        Some(endpoint) => format!("tkfs://pair?endpoint={}&code={code}", encode(endpoint)),
        None => format!("tkfs://pair?code={code}"),
    }
}

/// Percent-encodes everything but the RFC 3986 unreserved characters.
fn encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len() * 2);
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// The QR of `link` as text, two module rows per line, for a terminal.
#[cfg(not(target_os = "android"))]
pub fn qr_text(link: &str) -> Result<String> {
    use qrcode::{QrCode, render::unicode};
    let code = QrCode::new(link.as_bytes()).context("QR_TOO_LONG")?;
    Ok(code
        .render::<unicode::Dense1x2>()
        .quiet_zone(true)
        .module_dimensions(1, 1)
        .build())
}

/// The QR of `link` as a square of `size` modules per side, row by row, `true`
/// for a dark module, with a quiet zone of four light modules around it.
#[cfg(not(target_os = "android"))]
pub fn qr_modules(link: &str) -> Result<(usize, Vec<bool>)> {
    use qrcode::{Color, QrCode};
    const QUIET: usize = 4;
    let code = QrCode::new(link.as_bytes()).context("QR_TOO_LONG")?;
    let inner = code.width();
    let size = inner + 2 * QUIET;
    let mut modules = vec![false; size * size];
    for (index, color) in code.to_colors().into_iter().enumerate() {
        let (row, column) = (index / inner + QUIET, index % inner + QUIET);
        modules[row * size + column] = color == Color::Dark;
    }
    Ok((size, modules))
}

#[cfg(test)]
mod tests {
    use super::*;

    const CODE: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn link_percent_encodes_the_endpoint_and_carries_the_code() {
        assert_eq!(
            link(Some("ws://192.168.1.20:43180"), CODE),
            format!("tkfs://pair?endpoint=ws%3A%2F%2F192.168.1.20%3A43180&code={CODE}")
        );
        assert_eq!(
            link(Some("wss://h/a b&c=d"), CODE),
            format!("tkfs://pair?endpoint=wss%3A%2F%2Fh%2Fa%20b%26c%3Dd&code={CODE}")
        );
    }

    #[test]
    fn link_without_an_endpoint_has_only_the_code() {
        assert_eq!(link(None, CODE), format!("tkfs://pair?code={CODE}"));
    }

    #[test]
    fn qr_text_renders_a_non_empty_block() {
        let text = qr_text(&link(Some("ws://192.168.1.20:43180"), CODE)).unwrap();
        assert!(text.lines().count() > 10);
        assert!(text.contains('█') || text.contains('▀') || text.contains('▄'));
        let (size, modules) = qr_modules(&link(None, CODE)).unwrap();
        assert_eq!(modules.len(), size * size);
        assert!(modules.iter().any(|dark| *dark));
        assert!(modules[..size * 4].iter().all(|dark| !dark), "quiet zone");
    }
}
