//! Negotiated binary envelopes: JSON metadata plus raw object attachments.
//! Compression is per message, bounded before and after inflation, and optional.
use anyhow::{Context, Result, ensure};
use flate2::{Compression, bufread::GzDecoder, write::GzEncoder};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{Read, Write};

pub const PROTOCOL: &str = "tkfs-binary-gzip-v1";
const MAGIC: &[u8; 4] = b"TKW1";
const HEADER: usize = 9;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    value: Value,
    attachments: Vec<(String, usize)>,
}

fn detach(value: &mut Value, path: &str, candidate: bool, out: &mut Vec<(String, Vec<u8>)>) {
    if candidate
        && let Some(hex) = value.as_str()
        && hex.len() >= 128
        && hex.len() % 2 == 0
        && hex
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        // Restrict to canonical lowercase hex so decoding preserves exact values.
        out.push((path.to_owned(), hex::decode(hex).expect("validated hex")));
        *value = Value::Null;
        return;
    }
    match value {
        Value::Object(fields) => {
            let objects = path.ends_with("/objects");
            for (key, value) in fields {
                let child = format!("{path}/{}", key.replace('~', "~0").replace('/', "~1"));
                detach(value, &child, key == "hex" || objects, out);
            }
        }
        Value::Array(items) => {
            for (index, value) in items.iter_mut().enumerate() {
                detach(value, &format!("{path}/{index}"), false, out);
            }
        }
        _ => {}
    }
}

pub fn encode(mut value: Value, maximum: usize) -> Result<Vec<u8>> {
    let mut attachments = Vec::new();
    detach(&mut value, "", false, &mut attachments);
    let envelope = Envelope {
        value,
        attachments: attachments
            .iter()
            .map(|(path, bytes)| (path.clone(), bytes.len()))
            .collect(),
    };
    let metadata = serde_json::to_vec(&envelope)?;
    let object_bytes: usize = attachments.iter().map(|(_, bytes)| bytes.len()).sum();
    ensure!(
        metadata
            .len()
            .saturating_add(object_bytes.saturating_mul(2))
            <= maximum,
        "WS_MESSAGE_TOO_LARGE"
    );
    let mut raw = Vec::with_capacity(4 + metadata.len() + object_bytes);
    raw.extend_from_slice(&(metadata.len() as u32).to_be_bytes());
    raw.extend_from_slice(&metadata);
    for (_, bytes) in attachments {
        raw.extend_from_slice(&bytes);
    }
    ensure!(raw.len() + HEADER <= maximum, "WS_MESSAGE_TOO_LARGE");
    let mut compressed = Vec::new();
    // Probe a bounded prefix before paying to compress an incompressible page.
    // A final size comparison also catches misleading compressible prefixes.
    if raw.len() >= 1024 {
        let mut probe = GzEncoder::new(Vec::new(), Compression::fast());
        probe.write_all(&raw[..raw.len().min(16 * 1024)])?;
        if probe.finish()?.len() * 100 < raw.len().min(16 * 1024) * 95 {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
            encoder.write_all(&raw)?;
            compressed = encoder.finish()?;
        }
    }
    let gzip = !compressed.is_empty() && compressed.len() + 32 < raw.len();
    let mut frame = Vec::with_capacity(HEADER + if gzip { compressed.len() } else { raw.len() });
    frame.extend_from_slice(MAGIC);
    frame.push(u8::from(gzip));
    frame.extend_from_slice(&(raw.len() as u32).to_be_bytes());
    frame.extend_from_slice(if gzip { &compressed } else { &raw });
    Ok(frame)
}

pub fn decode(frame: &[u8], maximum: usize) -> Result<Value> {
    ensure!(
        frame.len() >= HEADER && frame.len() <= maximum && &frame[..4] == MAGIC,
        "INVALID_BINARY_ENVELOPE"
    );
    let expected = u32::from_be_bytes(frame[5..9].try_into().unwrap()) as usize;
    ensure!(expected <= maximum && expected >= 4, "WS_MESSAGE_TOO_LARGE");
    let raw = match frame[4] {
        0 => frame[HEADER..].to_vec(),
        1 => {
            let mut decoder = GzDecoder::new(&frame[HEADER..]);
            let mut raw = Vec::new();
            decoder
                .by_ref()
                .take(expected as u64 + 1)
                .read_to_end(&mut raw)?;
            ensure!(raw.len() == expected, "INVALID_INFLATED_LENGTH");
            ensure!(decoder.into_inner().is_empty(), "TRAILING_GZIP_DATA");
            raw
        }
        _ => anyhow::bail!("UNKNOWN_WIRE_COMPRESSION"),
    };
    ensure!(raw.len() == expected, "INVALID_INFLATED_LENGTH");
    let metadata_len = u32::from_be_bytes(raw[..4].try_into().unwrap()) as usize;
    let end = 4usize
        .checked_add(metadata_len)
        .context("INVALID_BINARY_ENVELOPE")?;
    ensure!(end <= raw.len(), "INVALID_BINARY_ENVELOPE");
    let mut envelope: Envelope = serde_json::from_slice(&raw[4..end])?;
    ensure!(
        metadata_len.saturating_add((raw.len() - end).saturating_mul(2)) <= maximum,
        "WS_MESSAGE_TOO_LARGE"
    );
    let mut offset = end;
    for (path, length) in envelope.attachments {
        let end = offset
            .checked_add(length)
            .context("INVALID_ATTACHMENT_LENGTH")?;
        ensure!(end <= raw.len(), "INVALID_ATTACHMENT_LENGTH");
        let slot = envelope
            .value
            .pointer_mut(&path)
            .context("INVALID_ATTACHMENT_PATH")?;
        ensure!(slot.is_null(), "DUPLICATE_OR_INVALID_ATTACHMENT");
        *slot = Value::String(hex::encode(&raw[offset..end]));
        offset = end;
    }
    ensure!(offset == raw.len(), "TRAILING_ATTACHMENT_DATA");
    Ok(envelope.value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{RngCore, SeedableRng};
    use serde_json::json;

    #[test]
    fn binary_attachments_and_gzip_preserve_exact_json_values() {
        let value = json!({"page":{"object_parts":{"a/b~c":{"hex":hex::encode(vec![17; 1024*1024]),"offset":37,"total":2000000}}},
            "uppercase":{"hex":"AA".repeat(100)},"ordinary":"hello","null":null,"array":[{"hex":"01".repeat(80)}]});
        let frame = encode(value.clone(), 4 * 1024 * 1024).unwrap();
        assert_eq!(frame[4], 1);
        assert!(frame.len() < 32 * 1024);
        assert_eq!(decode(&frame, 4 * 1024 * 1024).unwrap(), value);
    }

    #[test]
    fn incompressible_objects_use_raw_binary_instead_of_hex_or_gzip_expansion() {
        let mut bytes = vec![0; 256 * 1024];
        rand::rngs::StdRng::seed_from_u64(71).fill_bytes(&mut bytes);
        let value = json!({"bundle":{"objects":{"content":hex::encode(&bytes)}}});
        let frame = encode(value.clone(), 1024 * 1024).unwrap();
        assert_eq!(frame[4], 0);
        assert!(frame.len() < bytes.len() + 1024);
        assert_eq!(decode(&frame, 1024 * 1024).unwrap(), value);
    }

    #[test]
    fn reject_truncated_corrupt_oversized_and_trailing_compressed_frames() {
        let frame = encode(json!({"hex":"01".repeat(32000)}), 100000).unwrap();
        assert_eq!(frame[4], 1);
        for n in [0, 4, 8, 9, frame.len() - 1] {
            assert!(decode(&frame[..n], 100000).is_err());
        }
        let mut bad = frame.clone();
        bad[5..9].copy_from_slice(&100001u32.to_be_bytes());
        assert!(decode(&bad, 100000).is_err());
        bad[5..9].copy_from_slice(&64u32.to_be_bytes());
        assert!(decode(&bad, 100000).is_err());
        let mut bad = frame.clone();
        *bad.last_mut().unwrap() ^= 1;
        assert!(decode(&bad, 100000).is_err());
        let mut bad = frame.clone();
        bad.extend_from_slice(&frame[HEADER..]);
        assert!(decode(&bad, 100000).is_err());
        assert!(encode(json!({"hex":"01".repeat(32000)}), 1000).is_err());
    }

    #[test]
    fn duplicate_attachment_paths_cannot_overwrite_metadata() {
        let metadata = serde_json::to_vec(&Envelope {
            value: serde_json::json!({"hex":null}),
            attachments: vec![("/hex".into(), 1), ("/hex".into(), 1)],
        })
        .unwrap();
        let mut frame = MAGIC.to_vec();
        frame.push(0);
        frame.extend_from_slice(&((4 + metadata.len() + 2) as u32).to_be_bytes());
        frame.extend_from_slice(&(metadata.len() as u32).to_be_bytes());
        frame.extend_from_slice(&metadata);
        frame.extend_from_slice(&[1, 2]);
        assert!(
            decode(&frame, 10000)
                .unwrap_err()
                .to_string()
                .contains("DUPLICATE")
        );
    }

    #[test]
    #[ignore = "opt-in release codec throughput benchmark"]
    fn codec_benchmark() {
        use std::time::Instant;
        let mut random = vec![0; 1024 * 1024];
        rand::rngs::StdRng::seed_from_u64(71).fill_bytes(&mut random);
        for (name, bytes) in [
            (
                "text",
                b"function sync() { return metadata; }\n".repeat(30000),
            ),
            ("random", random),
        ] {
            let value = json!({"object_parts":{"object":{"hex":hex::encode(&bytes),"offset":0,"total":bytes.len()}}});
            let json = serde_json::to_vec(&value).unwrap();
            let start = Instant::now();
            let mut old_gzip = GzEncoder::new(Vec::new(), Compression::fast());
            old_gzip.write_all(&json).unwrap();
            let old_gzip = old_gzip.finish().unwrap();
            let gzip_json_ms = start.elapsed().as_secs_f64() * 1000.0;
            let start = Instant::now();
            let mut frame = Vec::new();
            for _ in 0..5 {
                frame = encode(std::hint::black_box(value.clone()), 8 * 1024 * 1024).unwrap();
            }
            let encode_ms = start.elapsed().as_secs_f64() * 200.0;
            let start = Instant::now();
            for _ in 0..5 {
                assert_eq!(
                    decode(std::hint::black_box(&frame), 8 * 1024 * 1024).unwrap(),
                    value
                );
            }
            let decode_ms = start.elapsed().as_secs_f64() * 200.0;
            println!(
                "{}",
                json!({"dataset":name,"object_bytes":bytes.len(),"json_bytes":json.len(),"gzip_json_bytes":old_gzip.len(),
                "gzip_json_encode_ms":gzip_json_ms,"wire_bytes":frame.len(),"gzip":frame[4]==1,"encode_ms":encode_ms,"decode_ms":decode_ms})
            );
        }
    }
}
