//! Owned by WP3.INT: integration tests of Wave 3 that belong to no package
//! (REVAMP_PLAN §14 WP3.INT). The daemon-level end to end of objects v3 is
//! `tests/orchestrator.rs` (WP3.1's file, run on objects v3 since WP3.INT)
//! and the release scenarios of `tests/qual_wp3_int_e2e.rs`.
use tkfs::core::{MAX_SEAL_INTERVAL, MIN_SEAL_INTERVAL, Store, seal_interval};

/// CCR-WP2.4-3 (applied by WP3.INT): the periodic seal interval grows with
/// the sealed payload, one event per 4 KiB, between 128 and 1024 events.
#[test]
fn the_seal_interval_scales_with_the_sealed_payload() {
    assert_eq!(MIN_SEAL_INTERVAL, 128);
    assert_eq!(MAX_SEAL_INTERVAL, 1024);
    assert_eq!(seal_interval(None), 128, "no seal yet");
    assert_eq!(seal_interval(Some(0)), 128);
    assert_eq!(seal_interval(Some(512 * 1024 - 1)), 128);
    assert_eq!(seal_interval(Some(512 * 1024)), 128);
    assert_eq!(seal_interval(Some(1024 * 1024)), 256);
    assert_eq!(seal_interval(Some(2 * 1024 * 1024)), 512);
    assert_eq!(seal_interval(Some(4 * 1024 * 1024)), 1024);
    assert_eq!(seal_interval(Some(64 * 1024 * 1024)), 1024);
}

fn sealed(store: &Store) -> (i64, i64) {
    store
        .db
        .query_row(
            "SELECT cursor,octet_length(payload) FROM startup_snapshots WHERE branch=?",
            [&store.active],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
}

/// A store follows the scaled rule: each periodic seal falls exactly
/// `seal_interval(previous payload)` journal sequences after the previous
/// one, so a projection of about 1 MiB seals every ~256 events instead of
/// every 128; and it still opens (the replay of a tail longer than 128
/// events resumes from the snapshot or rebuilds, never fails).
#[test]
fn a_large_projection_seals_less_often() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), Some("w3-seal-interval")).unwrap();
    let long = "n".repeat(200);
    let mut seals: Vec<(i64, i64)> = vec![sealed(&store)];
    for index in 0..2600 {
        store.create(&format!("{long}-{index:05}"), "file").unwrap();
        let now = sealed(&store);
        if now.0 != seals.last().unwrap().0 {
            seals.push(now);
        }
    }
    assert!(seals.len() >= 3, "{seals:?}");
    let mut longer = 0;
    for pair in seals.windows(2) {
        let (before, after) = (pair[0], pair[1]);
        let interval = seal_interval(Some(before.1));
        assert_eq!(after.0 - before.0, interval, "{seals:?}");
        if interval > MIN_SEAL_INTERVAL {
            longer += 1;
        }
    }
    assert!(longer > 0, "the payload grew past 512 KiB: {seals:?}");
    let expected = store.projection(&store.active).unwrap();
    drop(store);
    let store = Store::open(dir.path(), Some("w3-seal-interval")).unwrap();
    assert_eq!(store.projection(&store.active).unwrap(), expected);
}
