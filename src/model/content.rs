//! Wave 6 immutable content format. These types do not enable new-format writes.
//!
//! The byte encoding, limits and digest domains are frozen in
//! `docs/revamp/WAVE_6_CONTRACTS.md`. Decode before allocating from untrusted lengths.
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

pub const CHUNK_SIZE: u64 = 65_536;
pub const FANOUT: usize = 256;
pub const MAX_NODE_BYTES: usize = 65_536;
pub const MAX_DEPTH: u8 = 8;
pub const MAX_FILE_SIZE: u64 = 1 << 48;
const MANIFEST_DOMAIN: &[u8] = b"tkfs:manifest:v1\0";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectKind {
    Bytes,
    Manifest,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectKey {
    pub kind: ObjectKind,
    pub digest: Digest,
}

impl ObjectKey {
    pub fn root(reference: &FileContentRef) -> Self {
        Self {
            kind: if reference.kind == ContentKind::Manifest {
                ObjectKind::Manifest
            } else {
                ObjectKind::Bytes
            },
            digest: reference.digest,
        }
    }
}

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
    Legacy,
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
            format: if kind == ContentKind::Legacy { 0 } else { 1 },
            algorithm: DigestAlgorithm::Sha256,
            digest,
            total_len,
        };
        result.validate()?;
        Ok(result)
    }

    pub fn validate(&self) -> Result<()> {
        match self.kind {
            ContentKind::Legacy => ensure!(
                self.format == 0 && self.total_len <= i64::MAX as u64,
                "INVALID_LEGACY_DESCRIPTOR"
            ),
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
            ContentKind::Legacy => 0,
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
            0 => ContentKind::Legacy,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChildKind {
    Chunk,
    Node,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestChild {
    pub kind: ChildKind,
    pub len: u64,
    pub digest: Digest,
}

/// Offsets are cumulative from zero. Level zero contains data chunks;
/// level n contains nodes of exactly level n-1.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestNode {
    pub level: u8,
    pub total_len: u64,
    pub children: Vec<ManifestChild>,
}

impl ManifestNode {
    pub fn child_capacity(level: u8) -> Result<u64> {
        ensure!(level < MAX_DEPTH, "MANIFEST_DEPTH");
        CHUNK_SIZE
            .checked_mul(
                (FANOUT as u64)
                    .checked_pow(level.into())
                    .ok_or_else(|| anyhow::anyhow!("MANIFEST_OVERFLOW"))?,
            )
            .ok_or_else(|| anyhow::anyhow!("MANIFEST_OVERFLOW"))
    }

    pub fn root_level(len: u64) -> Result<u8> {
        ensure!(len > CHUNK_SIZE && len <= MAX_FILE_SIZE, "MANIFEST_LENGTH");
        let mut level = 0;
        while len.div_ceil(Self::child_capacity(level)?) > FANOUT as u64 {
            level += 1;
        }
        Ok(level)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.total_len > 0 && self.total_len <= MAX_FILE_SIZE,
            "MANIFEST_LENGTH"
        );
        let capacity = Self::child_capacity(self.level)?;
        let expected = self.total_len.div_ceil(capacity);
        ensure!(
            expected <= FANOUT as u64 && self.children.len() as u64 == expected,
            "MANIFEST_FANOUT"
        );
        let mut covered = 0_u64;
        for child in &self.children {
            let expected_kind = if self.level == 0 {
                ChildKind::Chunk
            } else {
                ChildKind::Node
            };
            ensure!(child.kind == expected_kind, "MANIFEST_CHILD_KIND");
            ensure!(
                child.len == (self.total_len - covered).min(capacity),
                "MANIFEST_COVERAGE"
            );
            covered = covered
                .checked_add(child.len)
                .ok_or_else(|| anyhow::anyhow!("MANIFEST_OVERFLOW"))?;
        }
        ensure!(covered == self.total_len, "MANIFEST_COVERAGE");
        Ok(())
    }

    pub fn validate_root(&self, descriptor: &FileContentRef) -> Result<()> {
        descriptor.validate()?;
        self.validate()?;
        ensure!(
            descriptor.kind == ContentKind::Manifest
                && self.level == Self::root_level(descriptor.total_len)?
                && self.total_len == descriptor.total_len
                && self.digest()? == descriptor.digest,
            "MANIFEST_ROOT_MISMATCH"
        );
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut bytes = Vec::with_capacity(16 + 41 * self.children.len());
        bytes.extend_from_slice(b"TKFM\x01");
        bytes.push(self.level);
        bytes.extend_from_slice(&self.total_len.to_le_bytes());
        bytes.extend_from_slice(&(self.children.len() as u16).to_le_bytes());
        for child in &self.children {
            bytes.push(if child.kind == ChildKind::Chunk { 0 } else { 1 });
            bytes.extend_from_slice(&child.len.to_le_bytes());
            bytes.extend_from_slice(&child.digest.0);
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            (16..=MAX_NODE_BYTES).contains(&bytes.len()) && &bytes[..5] == b"TKFM\x01",
            "INVALID_MANIFEST_ENCODING"
        );
        let count = u16::from_le_bytes(bytes[14..16].try_into()?) as usize;
        ensure!(
            count <= FANOUT && bytes.len() == 16 + 41 * count,
            "MANIFEST_FANOUT"
        );
        let mut children = Vec::with_capacity(count);
        for entry in bytes[16..].chunks_exact(41) {
            let kind = match entry[0] {
                0 => ChildKind::Chunk,
                1 => ChildKind::Node,
                _ => anyhow::bail!("MANIFEST_CHILD_KIND"),
            };
            children.push(ManifestChild {
                kind,
                len: u64::from_le_bytes(entry[1..9].try_into()?),
                digest: Digest(entry[9..41].try_into()?),
            });
        }
        let node = Self {
            level: bytes[5],
            total_len: u64::from_le_bytes(bytes[6..14].try_into()?),
            children,
        };
        node.validate()?;
        Ok(node)
    }

    pub fn digest(&self) -> Result<Digest> {
        let mut hash = Sha256::new();
        hash.update(MANIFEST_DOMAIN);
        hash.update(self.encode()?);
        Ok(Digest(hash.finalize().into()))
    }
}
