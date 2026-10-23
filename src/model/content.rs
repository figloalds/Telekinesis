//! Immutable direct or flat-manifest file references.
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
pub const CHUNK_SIZE: u64 = 65_536;
pub const MAX_FILE_SIZE: u64 = ((64 * 1024 * 1024 - 13) / 32) * CHUNK_SIZE;
/// A SHA-256 digest with a single, lowercase wire spelling.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Digest(pub [u8; 32]);

impl Digest {
    pub fn bytes(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    pub fn parse(value: &str) -> Result<Self> {
        ensure!(
            value.len() == 64
                && value
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "INVALID_CONTENT_DIGEST"
        );
        let mut digest = [0; 32];
        hex::decode_to_slice(value, &mut digest)?;
        Ok(Self(digest))
    }
}

impl std::fmt::Display for Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl Serialize for Digest {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentKind {
    Direct,
    Manifest,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DigestAlgorithm {
    Sha256,
}

/// A descriptor is distinct from a legacy hash string and from a Git identity.
/// Both JSON and binary decoding validate the full descriptor contract.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FileContentRef {
    pub kind: ContentKind,
    pub format: u8,
    pub algorithm: DigestAlgorithm,
    pub digest: Digest,
    pub total_len: u64,
}

impl<'de> Deserialize<'de> for FileContentRef {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            kind: ContentKind,
            format: u8,
            algorithm: DigestAlgorithm,
            digest: Digest,
            total_len: u64,
        }
        let wire = Wire::deserialize(deserializer)?;
        let result = Self {
            kind: wire.kind,
            format: wire.format,
            algorithm: wire.algorithm,
            digest: wire.digest,
            total_len: wire.total_len,
        };
        result.validate().map_err(serde::de::Error::custom)?;
        Ok(result)
    }
}

impl FileContentRef {
    pub fn new(kind: ContentKind, digest: Digest, total_len: u64) -> Result<Self> {
        let result = Self {
            kind,
            format: 1,
            algorithm: DigestAlgorithm::Sha256,
            digest,
            total_len,
        };
        result.validate()?;
        Ok(result)
    }

    pub fn validate(&self) -> Result<()> {
        match self.kind {
            ContentKind::Direct => {
                ensure!(
                    self.format == 1 && self.total_len <= CHUNK_SIZE,
                    "INVALID_DIRECT_DESCRIPTOR"
                );
                if self.total_len == 0 {
                    ensure!(
                        self.digest == Digest::bytes(&[]),
                        "NONCANONICAL_EMPTY_CONTENT"
                    );
                }
            }
            ContentKind::Manifest => ensure!(
                self.format == 1 && self.total_len > CHUNK_SIZE && self.total_len <= MAX_FILE_SIZE,
                "INVALID_MANIFEST_DESCRIPTOR"
            ),
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<[u8; 48]> {
        self.validate()?;
        let mut bytes = [0; 48];
        bytes[..5].copy_from_slice(b"TKFC\x01");
        bytes[5] = match self.kind {
            ContentKind::Direct => 1,
            ContentKind::Manifest => 2,
        };
        bytes[6] = self.format;
        bytes[7] = 1; // SHA-256
        bytes[8..16].copy_from_slice(&self.total_len.to_le_bytes());
        bytes[16..].copy_from_slice(&self.digest.0);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() == 48 && &bytes[..5] == b"TKFC\x01" && bytes[7] == 1,
            "UNSUPPORTED_CONTENT_DESCRIPTOR"
        );
        let kind = match bytes[5] {
            1 => ContentKind::Direct,
            2 => ContentKind::Manifest,
            _ => anyhow::bail!("UNSUPPORTED_CONTENT_KIND"),
        };
        let result = Self {
            kind,
            format: bytes[6],
            algorithm: DigestAlgorithm::Sha256,
            digest: Digest(bytes[16..48].try_into()?),
            total_len: u64::from_le_bytes(bytes[8..16].try_into()?),
        };
        result.validate()?;
        Ok(result)
    }
}
