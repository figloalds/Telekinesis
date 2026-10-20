use tkfs::model::content::*;

fn vectors() -> serde_json::Value {
    serde_json::from_str(include_str!("fixtures/content_v1/vectors.json")).unwrap()
}

fn child(len: u64, digest: Digest) -> ManifestChild {
    ManifestChild {
        kind: ChildKind::Chunk,
        len,
        digest,
    }
}

#[test]
fn independently_encoded_descriptor_goldens() {
    let vectors = vectors();
    for (name, data) in [
        ("empty", vec![]),
        ("direct", b"abc".to_vec()),
        ("boundary", vec![b'x'; CHUNK_SIZE as usize]),
    ] {
        let descriptor =
            FileContentRef::new(ContentKind::Direct, Digest::bytes(&data), data.len() as u64)
                .unwrap();
        let encoded = descriptor.encode().unwrap();
        assert_eq!(
            hex::encode(encoded),
            vectors[name]["descriptor_hex"].as_str().unwrap()
        );
        assert_eq!(FileContentRef::decode(&encoded).unwrap(), descriptor);
        let json = serde_json::to_string(&descriptor).unwrap();
        assert_eq!(
            serde_json::from_str::<FileContentRef>(&json).unwrap(),
            descriptor
        );
    }
    let legacy = FileContentRef::new(ContentKind::Legacy, Digest::bytes(b"abc"), 3).unwrap();
    assert_eq!(
        hex::encode(legacy.encode().unwrap()),
        vectors["legacy"]["descriptor_hex"]
    );
}

#[test]
fn canonical_tail_and_indexed_goldens() {
    let vectors = vectors();
    let x = Digest::bytes(&vec![b'x'; CHUNK_SIZE as usize]);
    let y = Digest::bytes(b"y");
    let leaf = ManifestNode {
        level: 0,
        total_len: CHUNK_SIZE + 1,
        children: vec![child(CHUNK_SIZE, x), child(1, y)],
    };
    assert_eq!(
        hex::encode(leaf.encode().unwrap()),
        vectors["tail"]["node_hex"]
    );
    assert_eq!(
        leaf.digest().unwrap().to_string(),
        vectors["tail"]["digest"]
    );
    let descriptor = FileContentRef::new(
        ContentKind::Manifest,
        leaf.digest().unwrap(),
        leaf.total_len,
    )
    .unwrap();
    assert_eq!(
        hex::encode(descriptor.encode().unwrap()),
        vectors["tail"]["descriptor_hex"]
    );
    leaf.validate_root(&descriptor).unwrap();
    let full = ManifestNode {
        level: 0,
        total_len: CHUNK_SIZE * FANOUT as u64,
        children: vec![child(CHUNK_SIZE, x); FANOUT],
    };
    let tail = ManifestNode {
        level: 0,
        total_len: 1,
        children: vec![child(1, y)],
    };
    assert_eq!(
        full.digest().unwrap().to_string(),
        vectors["indexed"]["full_leaf_digest"]
    );
    assert_eq!(
        tail.digest().unwrap().to_string(),
        vectors["indexed"]["tail_leaf_digest"]
    );
    let root = ManifestNode {
        level: 1,
        total_len: full.total_len + 1,
        children: vec![
            ManifestChild {
                kind: ChildKind::Node,
                len: full.total_len,
                digest: full.digest().unwrap(),
            },
            ManifestChild {
                kind: ChildKind::Node,
                len: 1,
                digest: tail.digest().unwrap(),
            },
        ],
    };
    assert_eq!(
        hex::encode(root.encode().unwrap()),
        vectors["indexed"]["node_hex"]
    );
    assert_eq!(
        root.digest().unwrap().to_string(),
        vectors["indexed"]["digest"]
    );
    assert_eq!(ManifestNode::decode(&root.encode().unwrap()).unwrap(), root);
    assert_ne!(
        root.digest().unwrap(),
        Digest::bytes(&root.encode().unwrap())
    );
}

#[test]
fn rejects_forged_coverage_kinds_depth_lengths_and_empty() {
    let original = hex::decode(vectors()["tail"]["node_hex"].as_str().unwrap()).unwrap();
    for end in 0..original.len() {
        assert!(
            ManifestNode::decode(&original[..end]).is_err(),
            "truncated at {end}"
        );
    }
    let mut trailing = original.clone();
    trailing.push(0);
    assert!(ManifestNode::decode(&trailing).is_err());
    for (offset, value) in [(4, 2), (5, 8), (14, 255), (15, 255), (16, 1), (17, 1)] {
        let mut bad = original.clone();
        bad[offset] = value;
        assert!(
            ManifestNode::decode(&bad).is_err(),
            "mutated offset {offset}"
        );
    }
    let mut overflow = original.clone();
    overflow[6..14].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(ManifestNode::decode(&overflow).is_err());
    assert!(ManifestNode::decode(&vec![0; MAX_NODE_BYTES + 1]).is_err());
    assert!(FileContentRef::new(ContentKind::Direct, Digest::bytes(b"x"), 0).is_err());
    assert!(FileContentRef::new(ContentKind::Direct, Digest::bytes(b"x"), CHUNK_SIZE + 1).is_err());
    assert!(
        FileContentRef::new(
            ContentKind::Manifest,
            Digest::bytes(b"x"),
            MAX_FILE_SIZE + 1
        )
        .is_err()
    );
    assert!(Digest::parse(&"A".repeat(64)).is_err());
    assert!(Digest::parse("../abc").is_err());
    let mut json = serde_json::to_value(
        FileContentRef::new(ContentKind::Direct, Digest::bytes(b"x"), 1).unwrap(),
    )
    .unwrap();
    json["total_len"] = serde_json::json!(CHUNK_SIZE + 1);
    assert!(
        serde_json::from_value::<FileContentRef>(json).is_err(),
        "JSON ingress bypassed descriptor validation"
    );
}

#[test]
fn root_height_is_canonical_and_bounded_at_maximum() {
    assert_eq!(ManifestNode::root_level(CHUNK_SIZE + 1).unwrap(), 0);
    assert_eq!(ManifestNode::root_level(CHUNK_SIZE * 256).unwrap(), 0);
    assert_eq!(ManifestNode::root_level(CHUNK_SIZE * 256 + 1).unwrap(), 1);
    assert_eq!(ManifestNode::root_level(MAX_FILE_SIZE).unwrap(), 3);
    assert!(ManifestNode::root_level(u64::MAX).is_err());
    for level in 6..=255 {
        assert!(ManifestNode::child_capacity(level).is_err());
    }
    let root = ManifestNode {
        level: 1,
        total_len: CHUNK_SIZE + 1,
        children: vec![ManifestChild {
            kind: ChildKind::Node,
            len: CHUNK_SIZE + 1,
            digest: Digest::bytes(b"x"),
        }],
    };
    let descriptor = FileContentRef::new(
        ContentKind::Manifest,
        root.digest().unwrap(),
        root.total_len,
    )
    .unwrap();
    assert!(root.validate_root(&descriptor).is_err());
}

#[test]
fn arbitrary_inputs_never_panic_or_allocate_from_declared_sizes() {
    let mut state = 0x6a09_e667_f3bc_c909_u64;
    for size in 0..2048 {
        let mut bytes = vec![0; size];
        for value in &mut bytes {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *value = state as u8;
        }
        let _ = FileContentRef::decode(&bytes);
        let _ = ManifestNode::decode(&bytes);
        if size >= 16 {
            bytes[..5].copy_from_slice(b"TKFM\x01");
            let _ = ManifestNode::decode(&bytes);
        }
    }
}

#[test]
fn range_overflow_and_phase_accounting_have_explicit_bounds() {
    use tkfs::{
        model::workspace::ByteRange,
        util::phases::{Phase, PhaseCounters},
    };
    assert!(
        ByteRange {
            offset: u64::MAX,
            len: 1
        }
        .end()
        .is_err()
    );
    assert!(ByteRange { offset: 10, len: 0 }.validate(10).is_ok());
    assert!(ByteRange { offset: 11, len: 0 }.validate(10).is_err());
    let counters = PhaseCounters::default();
    for latency in [0, 1, 2, 3, 4, 5, u64::MAX] {
        // Keep sum representable when exercising overflow bucket.
        counters.record(
            Phase::Hash,
            64,
            if latency == u64::MAX {
                1 << 40
            } else {
                latency
            },
        );
    }
    let sample = &counters.snapshot()[Phase::Hash as usize];
    assert_eq!(sample.operations, 7);
    assert_eq!(sample.bytes, 448);
    assert_eq!(&sample.latency_buckets[..4], &[2, 1, 2, 1]);
    assert_eq!(sample.latency_buckets[31], 1);
    assert_eq!(sample.latency_buckets.iter().sum::<u64>(), 7);
}
