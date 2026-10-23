//! Actual SQLite/WAL sparse-map qualification. The active private journal is
//! still v2; these roots do not acknowledge journal adoption or mounted saves.
use rusqlite::Connection;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
use tkfs::{
    content::{
        ReadContext,
        overlay_map::{MapKind, MapRoot, MapValue, MapWork, OverlayMap},
    },
    model::content::{CHUNK_SIZE, Digest, MAX_FILE_SIZE},
    util::Cancel,
};
use uuid::Uuid;

struct Fixture {
    db: Connection,
    _dir: tempfile::TempDir,
}
fn context() -> ReadContext {
    ReadContext {
        deadline: Instant::now() + Duration::from_secs(120),
        cancel: Cancel::new(),
    }
}
fn scope() -> Uuid {
    Uuid::from_u128(1)
}
fn patch(sequence: u64) -> MapValue {
    MapValue::Patch {
        hash: Digest::bytes(&sequence.to_le_bytes()),
        valid: 65536,
        sequence,
    }
}
fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let db = Connection::open(dir.path().join("map.sqlite")).unwrap();
    db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA cache_size=-2048; PRAGMA temp_store=FILE").unwrap();
    assert_eq!(
        db.query_row("PRAGMA journal_mode", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "wal"
    );
    assert_eq!(
        db.query_row("PRAGMA synchronous", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        2
    );
    OverlayMap::ensure_schema(&db).unwrap();
    Fixture { db, _dir: dir }
}
fn error<T: std::fmt::Debug>(result: anyhow::Result<T>, code: &str) {
    let value = format!("{:#}", result.unwrap_err());
    assert!(value.contains(code), "expected {code}: {value}");
}

#[test]
fn canonical_private_node_vector_and_root_survive_wal_reopen() {
    let f = fixture();
    let tx = f.db.unchecked_transaction().unwrap();
    let map = OverlayMap::new(&tx, scope(), MapKind::Patches);
    let value = MapValue::Patch {
        hash: Digest([3; 32]),
        valid: 512,
        sequence: 7,
    };
    let root = map
        .set(
            MapRoot::empty(scope(), MapKind::Patches),
            5,
            value,
            0,
            &mut MapWork::default(),
            &context(),
        )
        .unwrap();
    // Independently encoded with Python hashlib + little-endian struct fields.
    assert_eq!(
        root.digest().to_string(),
        "590ee6e8817f4f228b911aee3049f95dee92b32ea93435b7c8899db121d51538"
    );
    let leaf: Vec<u8> = tx
        .query_row(
            "SELECT body FROM private_overlay_nodes_v1 WHERE length(body)=75",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        hex::encode(leaf),
        "544b4f4d0100000000000000000000000000000001000005000000000000000303030303030303030303030303030303030303030303030303030303030303000200000700000000000000"
    );
    let serialized = serde_json::to_vec(&root).unwrap();
    tx.commit().unwrap();
    let path = f._dir.path().join("map.sqlite");
    drop(f.db);
    let db = Connection::open(path).unwrap();
    let map = OverlayMap::new(&db, scope(), MapKind::Patches);
    let root: MapRoot = serde_json::from_slice(&serialized).unwrap();
    let mut work = MapWork::default();
    assert_eq!(
        map.get(root, 5, 0, &mut work, &context()).unwrap(),
        Some(value)
    );
    assert_eq!(work.nodes_read, 9);
}

#[test]
fn authenticated_patch_ranges_match_a_sequence_filtered_oracle() {
    let f = fixture();
    let tx = f.db.unchecked_transaction().unwrap();
    let map = OverlayMap::new(&tx, scope(), MapKind::Patches);
    let entries: BTreeMap<_, _> = [0, 3, 17, 65535, 65536, 1_000_000, (1u64 << 32) - 1]
        .into_iter()
        .enumerate()
        .map(|(i, key)| (key, patch(i as u64 + 1)))
        .collect();
    let root = map
        .build_sorted(entries.clone(), &mut MapWork::default(), &context())
        .unwrap();
    let bounds = [
        0,
        1,
        3,
        4,
        17,
        18,
        65535,
        65536,
        65537,
        1_000_000,
        1_000_001,
        1u64 << 32,
    ];
    for start in bounds {
        for end in bounds {
            if start > end {
                continue;
            }
            for floor in [0, 1, 3, 6, u64::MAX] {
                let expected = entries
                    .range(start..end)
                    .any(|(_, value)| value.sequence() > floor);
                let mut work = MapWork::default();
                assert_eq!(
                    map.has_range(root, start, end, floor, &mut work, &context())
                        .unwrap(),
                    expected
                );
                assert!(work.nodes_read <= 17, "{work:?}");
            }
        }
    }
    tx.execute(
        "DELETE FROM private_overlay_nodes_v1 WHERE hash=?",
        [root.digest().0.as_slice()],
    )
    .unwrap();
    error(
        map.has_range(root, 0, 1u64 << 32, 0, &mut MapWork::default(), &context()),
        "CORRUPT_WRITE_JOURNAL",
    );
    error(
        map.verify_root(root, &mut MapWork::default(), &context()),
        "CORRUPT_WRITE_JOURNAL",
    );
}

#[test]
fn walk_reports_all_verified_node_hashes_for_collection() {
    use std::collections::BTreeSet;
    let f = fixture();
    let tx = f.db.unchecked_transaction().unwrap();
    let map = OverlayMap::new(&tx, scope(), MapKind::Patches);
    let root = map
        .build_sorted(
            (0..80).map(|key| (key * 65537, patch(key + 1))),
            &mut MapWork::default(),
            &context(),
        )
        .unwrap();
    let expected: BTreeSet<_> = tx
        .prepare("SELECT hash FROM private_overlay_nodes_v1")
        .unwrap()
        .query_map([], |r| {
            let raw: Vec<u8> = r.get(0)?;
            Ok(Digest(raw.try_into().unwrap()))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let mut walk = map.walk(root, 0).unwrap();
    let mut seen = BTreeSet::new();
    let mut leaves = 0;
    loop {
        let page = map
            .walk_page(&mut walk, 3, &mut MapWork::default(), &context())
            .unwrap();
        assert!(page.nodes.len() <= page.processed && page.processed <= 3);
        for hash in page.nodes {
            assert!(seen.insert(hash));
        }
        leaves += page.values.len();
        if page.complete {
            break;
        }
    }
    assert_eq!(seen, expected);
    assert_eq!(leaves, 80);
    let mut expired = map.walk(root, u64::MAX).unwrap();
    let page = map
        .walk_page(&mut expired, 3, &mut MapWork::default(), &context())
        .unwrap();
    assert_eq!(page.nodes, vec![root.digest()]);
    assert!(page.values.is_empty() && page.complete);
}

#[test]
fn a_discarded_node_install_cannot_return_a_successful_root() {
    for (timing, action) in [
        ("BEFORE", "SELECT RAISE(IGNORE);"),
        (
            "AFTER",
            "DELETE FROM private_overlay_nodes_v1 WHERE hash=NEW.hash;",
        ),
        (
            "AFTER",
            "UPDATE private_overlay_nodes_v1 SET body=x'00' WHERE hash=NEW.hash;",
        ),
    ] {
        let f = fixture();
        let tx = f.db.unchecked_transaction().unwrap();
        let map = OverlayMap::new(&tx, scope(), MapKind::Patches);
        tx.execute_batch(&format!("CREATE TEMP TRIGGER discard_node {timing} INSERT ON private_overlay_nodes_v1 BEGIN {action} END")).unwrap();
        error(
            map.set(
                MapRoot::empty(scope(), MapKind::Patches),
                3,
                patch(1),
                0,
                &mut MapWork::default(),
                &context(),
            ),
            "CORRUPT_WRITE_JOURNAL",
        );
        tx.execute_batch("DROP TRIGGER discard_node").unwrap();
        let root = map
            .set(
                MapRoot::empty(scope(), MapKind::Patches),
                3,
                patch(1),
                0,
                &mut MapWork::default(),
                &context(),
            )
            .unwrap();
        assert_eq!(
            map.get(root, 3, 0, &mut MapWork::default(), &context())
                .unwrap(),
            Some(patch(1))
        );
        tx.execute_batch("CREATE TEMP TRIGGER ignore_existing BEFORE INSERT ON private_overlay_nodes_v1 BEGIN SELECT RAISE(IGNORE); END").unwrap();
        let mut verified = MapWork::default();
        let identical = map
            .set(
                MapRoot::empty(scope(), MapKind::Patches),
                3,
                patch(1),
                0,
                &mut verified,
                &context(),
            )
            .unwrap();
        assert_eq!(identical, root);
        assert_eq!(verified.nodes_written, 9);
        assert_eq!(verified.node_checks, 9);
    }
}

#[test]
fn path_copies_truncations_and_immutable_forks_match_an_independent_map() {
    let f = fixture();
    let tx = f.db.unchecked_transaction().unwrap();
    let map = OverlayMap::new(&tx, scope(), MapKind::Patches);
    let mut root = MapRoot::empty(scope(), MapKind::Patches);
    let mut oracle = BTreeMap::new();
    let mut snapshots = vec![];
    let mut random = 0x71691a5bc8235u64;
    for sequence in 1..=320u64 {
        random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
        let key = if sequence % 19 == 0 {
            MAX_FILE_SIZE / CHUNK_SIZE - 1
        } else {
            (random >> 32) % 1024
        };
        let mut work = MapWork::default();
        if sequence % 7 == 0 {
            let end = key;
            root = map.truncate(root, end, 0, &mut work, &context()).unwrap();
            oracle.retain(|k, _| *k < end);
            assert!(work.nodes_read <= 8 && work.nodes_written <= 8, "{work:?}");
        } else {
            root = map
                .set(root, key, patch(sequence), 0, &mut work, &context())
                .unwrap();
            oracle.insert(key, patch(sequence));
            assert!(work.nodes_read <= 9 && work.nodes_written <= 9, "{work:?}");
        }
        assert_eq!(root.entries(), oracle.len() as u64);
        for key in [key, 0, 1023, MAX_FILE_SIZE / CHUNK_SIZE - 1] {
            let mut work = MapWork::default();
            assert_eq!(
                map.get(root, key, 0, &mut work, &context()).unwrap(),
                oracle.get(&key).copied()
            );
            assert!(work.nodes_read <= 9);
        }
        if sequence % 37 == 0 {
            snapshots.push((root, oracle.clone()));
        }
    }
    for (root, values) in snapshots {
        for (key, value) in values {
            assert_eq!(
                map.get(root, key, 0, &mut MapWork::default(), &context())
                    .unwrap(),
                Some(value)
            );
        }
    }
    let mut walk = map.walk(root, 0).unwrap();
    let mut actual = BTreeMap::new();
    loop {
        let page = map
            .walk_page(&mut walk, 7, &mut MapWork::default(), &context())
            .unwrap();
        assert!(page.processed <= 7);
        actual.extend(page.values);
        if page.complete {
            break;
        }
    }
    assert_eq!(actual, oracle);
    tx.commit().unwrap();
}

#[test]
fn published_sequence_prunes_old_paths_without_losing_the_old_root() {
    let f = fixture();
    let tx = f.db.unchecked_transaction().unwrap();
    let map = OverlayMap::new(&tx, scope(), MapKind::Patches);
    let root = map
        .build_sorted(
            (0..512).map(|key| (key, patch(key + 1))),
            &mut MapWork::default(),
            &context(),
        )
        .unwrap();
    let mut work = MapWork::default();
    assert!(
        map.get(root, 34, 512, &mut work, &context())
            .unwrap()
            .is_none()
    );
    assert_eq!(work.nodes_read, 1);
    let mut work = MapWork::default();
    let next = map
        .set(root, 34, patch(513), 512, &mut work, &context())
        .unwrap();
    assert_eq!(work.nodes_read, 1);
    assert_eq!(work.nodes_written, 9);
    assert_eq!(next.entries(), 1);
    assert_eq!(
        map.get(root, 34, 0, &mut MapWork::default(), &context())
            .unwrap(),
        Some(patch(35))
    );
    assert_eq!(
        map.get(next, 34, 512, &mut MapWork::default(), &context())
            .unwrap(),
        Some(patch(513))
    );
    let mut work = MapWork::default();
    assert_eq!(
        map.set(next, 34, patch(513), 512, &mut work, &context())
            .unwrap(),
        next
    );
    assert_eq!(work.nodes_written, 0);
    tx.commit().unwrap();
}

#[test]
fn truncate_watermarks_are_relative_to_the_published_and_saved_sequences() {
    let f = fixture();
    let tx = f.db.unchecked_transaction().unwrap();
    let map = OverlayMap::new(&tx, scope(), MapKind::Truncations);
    let root = map
        .build_sorted(
            [
                (
                    1,
                    MapValue::Truncate {
                        sequence: 1,
                        length: 10,
                    },
                ),
                (
                    3,
                    MapValue::Truncate {
                        sequence: 3,
                        length: 50,
                    },
                ),
                (
                    5,
                    MapValue::Truncate {
                        sequence: 5,
                        length: 0,
                    },
                ),
            ],
            &mut MapWork::default(),
            &context(),
        )
        .unwrap();
    assert_eq!(
        map.minimum(root, 0, 5, &mut MapWork::default(), &context())
            .unwrap(),
        Some(0)
    );
    assert_eq!(
        map.minimum(root, 2, 4, &mut MapWork::default(), &context())
            .unwrap(),
        Some(50)
    );
    assert_eq!(
        map.minimum(root, 2, 5, &mut MapWork::default(), &context())
            .unwrap(),
        Some(0)
    );
    assert_eq!(
        map.minimum(root, 3, 4, &mut MapWork::default(), &context())
            .unwrap(),
        None
    );
    // Original 10-byte low-water mark is insufficient once the published base
    // contains a nonzero append through 100. Later shrink/grow must zero 50..100.
    let mut published = vec![1; 10];
    published.extend([2; 90]);
    let visible = map
        .minimum(root, 2, 4, &mut MapWork::default(), &context())
        .unwrap()
        .unwrap() as usize;
    let mut rebased = vec![0; 100];
    rebased[..visible].copy_from_slice(&published[..visible]);
    assert_eq!(&rebased[..50], &published[..50]);
    assert_eq!(&rebased[50..], &[0; 50]);
    tx.commit().unwrap();
}

#[test]
fn bounded_walks_fail_closed_and_can_restart_after_missing_nodes_are_repaired() {
    let f = fixture();
    let tx = f.db.unchecked_transaction().unwrap();
    let map = OverlayMap::new(&tx, scope(), MapKind::Patches);
    let root = map
        .build_sorted(
            [(1, patch(1)), (17, patch(2)), (300, patch(3))],
            &mut MapWork::default(),
            &context(),
        )
        .unwrap();
    let mut walk = map.walk(root, 0).unwrap();
    assert_eq!(
        map.walk_page(&mut walk, 1, &mut MapWork::default(), &context())
            .unwrap()
            .processed,
        1
    );
    tx.execute("DELETE FROM private_overlay_nodes_v1", [])
        .unwrap();
    assert!(
        map.walk_page(&mut walk, 1, &mut MapWork::default(), &context())
            .is_err()
    );
    let repaired = map
        .build_sorted(
            [(1, patch(1)), (17, patch(2)), (300, patch(3))],
            &mut MapWork::default(),
            &context(),
        )
        .unwrap();
    assert_eq!(root, repaired);
    assert!(
        map.walk_page(&mut walk, 256, &mut MapWork::default(), &context())
            .is_err(),
        "a failed cursor omitted its popped task"
    );
    let mut walk = map.walk(root, 0).unwrap();
    let mut values = vec![];
    loop {
        let mut work = MapWork::default();
        let page = map.walk_page(&mut walk, 2, &mut work, &context()).unwrap();
        assert!(page.processed <= 2 && work.nodes_read <= 2);
        values.extend(page.values);
        if page.complete {
            break;
        }
    }
    assert_eq!(values, vec![(1, patch(1)), (17, patch(2)), (300, patch(3))]);
    tx.commit().unwrap();
}

#[test]
fn storage_corruption_is_never_interpreted_as_an_absent_patch() {
    for fault in ["missing", "bytes", "oversized", "type"] {
        let f = fixture();
        let tx = f.db.unchecked_transaction().unwrap();
        let map = OverlayMap::new(&tx, scope(), MapKind::Patches);
        let root = map
            .set(
                MapRoot::empty(scope(), MapKind::Patches),
                4,
                patch(1),
                0,
                &mut MapWork::default(),
                &context(),
            )
            .unwrap();
        match fault {
            "missing" => {
                tx.execute(
                    "DELETE FROM private_overlay_nodes_v1 WHERE hash=?",
                    [root.digest().0.as_slice()],
                )
                .unwrap();
            }
            "bytes" => {
                tx.execute(
                    "UPDATE private_overlay_nodes_v1 SET body=x'00' WHERE hash=?",
                    [root.digest().0.as_slice()],
                )
                .unwrap();
            }
            "oversized" => {
                tx.execute_batch("PRAGMA ignore_check_constraints=ON")
                    .unwrap();
                tx.execute(
                    "UPDATE private_overlay_nodes_v1 SET body=zeroblob(4000000) WHERE hash=?",
                    [root.digest().0.as_slice()],
                )
                .unwrap();
            }
            "type" => {
                tx.execute(
                    "UPDATE private_overlay_nodes_v1 SET body=23 WHERE hash=?",
                    [root.digest().0.as_slice()],
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        error(
            map.get(root, 4, 0, &mut MapWork::default(), &context()),
            "CORRUPT_WRITE_JOURNAL",
        );
        // No successful empty result is returned even when the requested key
        // differs from the corrupted scope's existing leaf.
        error(
            map.get(root, 90, 0, &mut MapWork::default(), &context()),
            "CORRUPT_WRITE_JOURNAL",
        );
    }
}

#[test]
fn roles_scopes_and_future_roots_cannot_reinterpret_a_patch_tree() {
    let f = fixture();
    let tx = f.db.unchecked_transaction().unwrap();
    let map = OverlayMap::new(&tx, scope(), MapKind::Patches);
    let root = map
        .set(
            MapRoot::empty(scope(), MapKind::Patches),
            0,
            patch(1),
            0,
            &mut MapWork::default(),
            &context(),
        )
        .unwrap();
    error(
        OverlayMap::new(&tx, Uuid::from_u128(2), MapKind::Patches).get(
            root,
            0,
            0,
            &mut MapWork::default(),
            &context(),
        ),
        "CORRUPT_WRITE_JOURNAL",
    );
    let mut value = serde_json::to_value(root).unwrap();
    value["scope"] = serde_json::json!(Uuid::from_u128(2));
    let forged = serde_json::from_value(value).unwrap();
    error(
        OverlayMap::new(&tx, Uuid::from_u128(2), MapKind::Patches).get(
            forged,
            0,
            0,
            &mut MapWork::default(),
            &context(),
        ),
        "CORRUPT_WRITE_JOURNAL",
    );
    let mut value = serde_json::to_value(root).unwrap();
    value["format"] = serde_json::json!(2);
    error(
        map.get(
            serde_json::from_value(value).unwrap(),
            0,
            0,
            &mut MapWork::default(),
            &context(),
        ),
        "UNSUPPORTED_WRITE_JOURNAL",
    );
    error(
        OverlayMap::new(&tx, scope(), MapKind::Truncations).get(
            root,
            0,
            0,
            &mut MapWork::default(),
            &context(),
        ),
        "CORRUPT_WRITE_JOURNAL",
    );
    tx.commit().unwrap();
    let map = OverlayMap::new(&f.db, scope(), MapKind::Patches);
    error(
        map.set(root, 4, patch(2), 0, &mut MapWork::default(), &context()),
        "WRITE_JOURNAL_TRANSACTION_REQUIRED",
    );
}

#[test]
fn stopped_and_invalid_operations_do_not_return_new_roots() {
    let f = fixture();
    let tx = f.db.unchecked_transaction().unwrap();
    let map = OverlayMap::new(&tx, scope(), MapKind::Patches);
    let root = MapRoot::empty(scope(), MapKind::Patches);
    let ctx = context();
    ctx.cancel.cancel();
    error(
        map.set(root, 0, patch(1), 0, &mut MapWork::default(), &ctx),
        "CONTENT_CANCELLED",
    );
    let mut ctx = context();
    ctx.deadline = Instant::now();
    error(
        map.get(root, 0, 0, &mut MapWork::default(), &ctx),
        "CONTENT_DEADLINE",
    );
    error(
        map.set(
            root,
            MAX_FILE_SIZE / CHUNK_SIZE,
            patch(1),
            0,
            &mut MapWork::default(),
            &context(),
        ),
        "INVALID_WRITE_INTENT",
    );
    error(
        map.build_sorted(
            [(2, patch(1)), (1, patch(2))],
            &mut MapWork::default(),
            &context(),
        ),
        "INVALID_WRITE_INTENT",
    );
    error(
        map.build_sorted(
            [(2, patch(1)), (2, patch(2))],
            &mut MapWork::default(),
            &context(),
        ),
        "INVALID_WRITE_INTENT",
    );
    assert_eq!(root.entries(), 0);
}

#[test]
fn fixed_local_work_at_five_and_fifty_thousand_patch_entries() {
    for entries in [5_000u64, 50_000] {
        let f = fixture();
        let map = OverlayMap::new(&f.db, scope(), MapKind::Patches);
        let tx = f.db.unchecked_transaction().unwrap();
        let mut built = MapWork::default();
        let root = map
            .build_sorted(
                (0..entries).map(|k| (k, patch(k + 1))),
                &mut built,
                &context(),
            )
            .unwrap();
        assert_eq!(root.entries(), entries);
        assert!(
            built.nodes_written < entries * 2,
            "bulk construction retained point-update history"
        );
        tx.commit().unwrap();
        for iteration in 0..6 {
            let tx = f.db.unchecked_transaction().unwrap();
            let key = entries / 2;
            let mut read = MapWork::default();
            let started = Instant::now();
            assert_eq!(
                map.get(root, key, 0, &mut read, &context()).unwrap(),
                Some(patch(key + 1))
            );
            let read_ms = started.elapsed().as_secs_f64() * 1000.0;
            let mut written = MapWork::default();
            let started = Instant::now();
            let next = map
                .set(
                    root,
                    key,
                    patch(entries + 1 + iteration),
                    0,
                    &mut written,
                    &context(),
                )
                .unwrap();
            let write_ms = started.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(next.entries(), entries);
            assert_eq!(read.nodes_read, 9);
            assert_eq!(written.nodes_read, 9);
            assert_eq!(written.nodes_written, 9);
            assert_eq!(written.node_checks, written.nodes_written);
            assert!(written.encoded_bytes <= 9 * 1024);
            println!(
                "WAVE6_OVERLAY_MAP {}",
                serde_json::json!({"scenario":"point","entries":entries,"warmup":iteration==0,"read_ms":read_ms,"write_ms":write_ms,"read":read,"write":written,"build_nodes":built.nodes_written})
            );
            let mut cursor = map.prune(next, entries).unwrap();
            let mut work = MapWork::default();
            let mut pages = 0;
            let start = Instant::now();
            let pruned = loop {
                let page = map
                    .prune_page(&mut cursor, 4, &mut work, &context())
                    .unwrap();
                assert!(page.processed <= 4);
                pages += 1;
                if let Some(root) = page.root {
                    break root;
                }
            };
            let elapsed = start.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(pruned.entries(), 1);
            assert_eq!(work.nodes_read, 9);
            assert_eq!(work.nodes_written, 8);
            assert_eq!(work.node_checks, work.nodes_written);
            println!(
                "WAVE6_OVERLAY_MAP {}",
                serde_json::json!({"scenario":"prune","entries":entries,"warmup":iteration==0,"pages":pages,"work":work,"elapsed_ms":elapsed,"source_unchanged":map.get(root,key,0,&mut MapWork::default(),&context()).unwrap()==Some(patch(key+1))})
            );
            tx.commit().unwrap();
        }
    }
}

#[test]
fn paged_pruning_commits_staging_without_early_root_publication() {
    let f = fixture();
    let map = OverlayMap::new(&f.db, scope(), MapKind::Patches);
    let tx = f.db.unchecked_transaction().unwrap();
    let original = map
        .build_sorted(
            (0..5000).map(|k| (k, patch(k + 1))),
            &mut MapWork::default(),
            &context(),
        )
        .unwrap();
    let newer = map
        .set(
            original,
            2500,
            patch(5001),
            0,
            &mut MapWork::default(),
            &context(),
        )
        .unwrap();
    tx.commit().unwrap();
    let mut cursor = map.prune(newer, 5000).unwrap();
    let mut total = MapWork::default();
    let mut pages = 0;
    let pruned = loop {
        let tx = f.db.unchecked_transaction().unwrap();
        let mut work = MapWork::default();
        let page = map
            .prune_page(&mut cursor, 1, &mut work, &context())
            .unwrap();
        assert!(page.processed <= 1 && work.nodes_read + work.nodes_written <= 1);
        assert_eq!(work.node_checks, work.nodes_written);
        total.nodes_read += work.nodes_read;
        total.nodes_written += work.nodes_written;
        total.node_checks += work.node_checks;
        pages += 1;
        tx.commit().unwrap();
        assert_eq!(
            map.get(original, 2500, 0, &mut MapWork::default(), &context())
                .unwrap(),
            Some(patch(2501))
        );
        if let Some(root) = page.root {
            break root;
        }
        assert!(pages < 40);
    };
    assert_eq!(pruned.entries(), 1);
    assert_eq!(total.nodes_read, 9);
    assert_eq!(total.nodes_written, 8);
    assert_eq!(total.node_checks, 8);
    let tx = f.db.unchecked_transaction().unwrap();
    let expected = map
        .set(
            MapRoot::empty(scope(), MapKind::Patches),
            2500,
            patch(5001),
            0,
            &mut MapWork::default(),
            &context(),
        )
        .unwrap();
    assert_eq!(pruned, expected);
    tx.commit().unwrap();
}

#[test]
fn mixed_generation_pruning_matches_an_independent_sequence_filter() {
    for kind in [MapKind::Patches, MapKind::Truncations] {
        let f = fixture();
        let tx = f.db.unchecked_transaction().unwrap();
        let map = OverlayMap::new(&tx, scope(), kind);
        let entries: BTreeMap<_, _> = (1..=64u64)
            .map(|key| {
                let value = match kind {
                    MapKind::Patches => patch((key * 19) % 67 + 1),
                    MapKind::Truncations => MapValue::Truncate {
                        sequence: key,
                        length: key * 7,
                    },
                };
                (
                    key * if kind == MapKind::Patches { 65537 } else { 1 },
                    value,
                )
            })
            .collect();
        let root = map
            .build_sorted(entries.clone(), &mut MapWork::default(), &context())
            .unwrap();
        for floor in [0, 1, 17, 32, 64, u64::MAX] {
            let expected: Vec<_> = entries
                .iter()
                .filter(|(_, v)| v.sequence() > floor)
                .map(|(&k, &v)| (k, v))
                .collect();
            let mut cursor = map.prune(root, floor).unwrap();
            let mut pages = 0;
            let pruned = loop {
                let budget = 1 + pages % 7;
                let mut work = MapWork::default();
                let page = map
                    .prune_page(&mut cursor, budget, &mut work, &context())
                    .unwrap();
                assert!(page.processed <= budget);
                assert!(work.nodes_read + work.nodes_written <= budget as u64);
                assert_eq!(work.node_checks, work.nodes_written);
                pages += 1;
                assert!(pages < 4096);
                if let Some(result) = page.root {
                    break result;
                }
            };
            let rebuilt = map
                .build_sorted(expected.clone(), &mut MapWork::default(), &context())
                .unwrap();
            assert_eq!(pruned, rebuilt);
            assert_eq!(pruned.entries(), expected.len() as u64);
            let mut walk = map.walk(pruned, 0).unwrap();
            let mut actual = Vec::new();
            loop {
                let page = map
                    .walk_page(&mut walk, 3, &mut MapWork::default(), &context())
                    .unwrap();
                actual.extend(page.values);
                if page.complete {
                    break;
                }
            }
            assert_eq!(actual, expected);
        }
        for (key, value) in entries {
            assert_eq!(
                map.get(root, key, 0, &mut MapWork::default(), &context())
                    .unwrap(),
                Some(value)
            );
        }
        tx.commit().unwrap();
    }
}

#[test]
fn failed_pruning_never_resumes_past_the_failed_install() {
    let f = fixture();
    let tx = f.db.unchecked_transaction().unwrap();
    let map = OverlayMap::new(&tx, scope(), MapKind::Patches);
    let root = map
        .build_sorted(
            (0..32).map(|k| (k, patch(k + 1))),
            &mut MapWork::default(),
            &context(),
        )
        .unwrap();
    let root = map
        .set(root, 7, patch(33), 0, &mut MapWork::default(), &context())
        .unwrap();
    let mut cursor = map.prune(root, 32).unwrap();
    tx.execute_batch("CREATE TEMP TRIGGER reject_prune BEFORE INSERT ON private_overlay_nodes_v1 BEGIN SELECT RAISE(ABORT,'failed immutable parent install'); END").unwrap();
    assert!(
        map.prune_page(&mut cursor, 256, &mut MapWork::default(), &context())
            .is_err()
    );
    tx.execute_batch("DROP TRIGGER reject_prune").unwrap();
    assert!(
        map.prune_page(&mut cursor, 256, &mut MapWork::default(), &context())
            .is_err()
    );
    let mut cursor = map.prune(root, 32).unwrap();
    let pruned = map
        .prune_page(&mut cursor, 256, &mut MapWork::default(), &context())
        .unwrap()
        .root
        .unwrap();
    assert_eq!(pruned.entries(), 1);
    assert_eq!(
        map.get(pruned, 7, 0, &mut MapWork::default(), &context())
            .unwrap(),
        Some(patch(33))
    );
    tx.commit().unwrap();
}

#[test]
fn cancellation_inside_a_walk_poisoned_cursor_cannot_omit_its_root() {
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
    let f = fixture();
    let tx = f.db.unchecked_transaction().unwrap();
    let map = OverlayMap::new(&tx, scope(), MapKind::Patches);
    let root = map
        .set(
            MapRoot::empty(scope(), MapKind::Patches),
            3,
            patch(1),
            0,
            &mut MapWork::default(),
            &context(),
        )
        .unwrap();
    let mut walk = map.walk(root, 0).unwrap();
    let ctx = context();
    let cancel = ctx.cancel.clone();
    tx.authorizer(Some(move |auth: AuthContext<'_>| {
        if matches!(
            auth.action,
            AuthAction::Read {
                table_name: "private_overlay_nodes_v1",
                ..
            }
        ) {
            cancel.cancel();
        }
        Authorization::Allow
    }));
    assert!(
        map.walk_page(&mut walk, 1, &mut MapWork::default(), &ctx)
            .is_err()
    );
    tx.authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
    assert!(
        map.walk_page(&mut walk, 256, &mut MapWork::default(), &context())
            .is_err()
    );
}

#[test]
fn truncation_minima_have_fixed_depth_at_fifty_thousand_events_and_u64_tail() {
    let f = fixture();
    let tx = f.db.unchecked_transaction().unwrap();
    let map = OverlayMap::new(&tx, scope(), MapKind::Truncations);
    let root = map
        .build_sorted(
            (1..=50_000u64).map(|sequence| {
                (
                    sequence,
                    MapValue::Truncate {
                        sequence,
                        length: (sequence * 137) % 100_000 + 10,
                    },
                )
            }),
            &mut MapWork::default(),
            &context(),
        )
        .unwrap();
    for (after, through) in [
        (0, 50_000),
        (10_000, 40_000),
        (24_999, 25_000),
        (49_999, 50_000),
        (50_000, 60_000),
    ] {
        let mut work = MapWork::default();
        let got = map
            .minimum(root, after, through, &mut work, &context())
            .unwrap();
        let expected = ((after + 1)..=through.min(50_000))
            .map(|n| (n * 137) % 100_000 + 10)
            .min();
        assert_eq!(got, expected);
        assert!(work.nodes_read <= 34, "{work:?}");
        println!(
            "WAVE6_OVERLAY_MAP {}",
            serde_json::json!({"scenario":"minimum","entries":50_000,"after":after,"through":through,"nodes_read":work.nodes_read})
        );
    }
    let root = map
        .set(
            root,
            u64::MAX,
            MapValue::Truncate {
                sequence: u64::MAX,
                length: 4,
            },
            0,
            &mut MapWork::default(),
            &context(),
        )
        .unwrap();
    assert_eq!(
        map.minimum(
            root,
            u64::MAX - 1,
            u64::MAX,
            &mut MapWork::default(),
            &context()
        )
        .unwrap(),
        Some(4)
    );
    tx.commit().unwrap();
}
