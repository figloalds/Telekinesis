//! Owned by WP3.INT: integration tests of Wave 3 that belong to no package
//! (REVAMP_PLAN §14 WP3.INT). The daemon-level end to end of objects v3 is
//! `tests/orchestrator.rs` (WP3.1's file, run on objects v3 since WP3.INT)
//! and the release scenarios of `tests/qual_wp3_int_e2e.rs`.
mod support;

use serde_json::{Value, json};
use std::{fs, path::Path, time::Duration};
use support::{Node, content_complete, has_file, mount_supported, wait_until};
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

fn state_of(folder: &Value) -> String {
    folder["state"].as_str().unwrap().to_owned()
}

/// Whether `path` (inside a mount) holds exactly `bytes`.
fn reads(path: &Path, bytes: &[u8]) -> bool {
    fs::read(path).is_ok_and(|read| read == bytes)
}

/// The end to end of Wave 3 (§14 WP3.INT): two real daemons sync two
/// folders mounted on the filesystem (WinFsp on Windows, FUSE on Linux) over
/// v3 sessions and objects v3, both directions: files and directories
/// written through one mount are read back through the other (metadata and
/// content), as are renames and deletions; the lanes are `tkfs-peer/3`.
#[test]
fn two_daemons_sync_mounted_folders_both_ways_over_v3() {
    if !mount_supported() {
        eprintln!("skipped: no mount support on this machine");
        return;
    }
    let a = Node::new();
    let b = Node::new();
    let mounts = tempfile::tempdir().unwrap();
    let (mount_a, mount_b) = (mounts.path().join("a"), mounts.path().join("b"));
    let pa = a.create_mounted("Mounted v3", &mount_a);
    let mounted =
        |node: &Node, state: &str| node.try_inspect(state).is_ok_and(|p| p["mounted"] == true);
    a.wait_for("A mounted", Duration::from_secs(60), || {
        mounted(&a, &state_of(&pa))
    });
    b.pair(&a, &a.code());
    let pb = b.call_ready(
        json!({"op":"receive","endpoint":a.endpoint(),"repo":pa["repo"],
        "label":pa["label"],"mount":mount_b,"headless":false}),
    );
    b.wait_for("B mounted", Duration::from_secs(60), || {
        mounted(&b, &state_of(&pb))
    });
    let big = |seed: u8| -> Vec<u8> {
        (0..3 * 1024 * 1024u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8 ^ seed)
            .collect()
    };
    let (big_a, big_b) = (big(0xa5), big(0x5a));
    let settle = Duration::from_secs(60);

    // A -> B, written through A's mount.
    fs::write(mount_a.join("from-a.txt"), b"written through A's mount").unwrap();
    fs::create_dir(mount_a.join("dir-a")).unwrap();
    fs::write(mount_a.join("dir-a").join("big-a.bin"), &big_a).unwrap();
    b.wait_for("A's files in B's store", settle, || {
        has_file(&pb, "from-a.txt", b"written through A's mount")
            && has_file(&pb, "big-a.bin", &big_a)
            && content_complete(&pb, 2)
    });
    assert!(reads(
        &mount_b.join("from-a.txt"),
        b"written through A's mount"
    ));
    assert!(reads(&mount_b.join("dir-a").join("big-a.bin"), &big_a));

    // B -> A, written through B's mount.
    fs::write(mount_b.join("from-b.txt"), b"written through B's mount").unwrap();
    fs::create_dir(mount_b.join("dir-b")).unwrap();
    fs::write(mount_b.join("dir-b").join("big-b.bin"), &big_b).unwrap();
    a.wait_for("B's files in A's store", settle, || {
        has_file(&pa, "from-b.txt", b"written through B's mount")
            && has_file(&pa, "big-b.bin", &big_b)
            && content_complete(&pa, 4)
    });
    assert!(reads(
        &mount_a.join("from-b.txt"),
        b"written through B's mount"
    ));
    assert!(reads(&mount_a.join("dir-b").join("big-b.bin"), &big_b));

    // Metadata only: a rename through B's mount and a deletion through A's.
    fs::rename(mount_b.join("from-a.txt"), mount_b.join("renamed-on-b.txt")).unwrap();
    fs::remove_file(mount_a.join("from-b.txt")).unwrap();
    assert!(
        wait_until(settle, || {
            reads(
                &mount_a.join("renamed-on-b.txt"),
                b"written through A's mount",
            ) && !mount_a.join("from-a.txt").exists()
                && !mount_b.join("from-b.txt").exists()
        }),
        "the rename and the deletion did not reach the other mount"
    );

    // Over v3 only.
    let peers = b.peers();
    let lanes = peers[0]["lanes"].as_array().unwrap();
    assert!(lanes.len() >= 2, "{peers}");
    for lane in lanes {
        assert_eq!(lane["transport"], "tkfs-peer/3", "{peers}");
        assert_eq!(lane["connected"], true, "{peers}");
    }
    for (node, folder) in [(&a, &pa), (&b, &pb)] {
        let status = node.raw_call(json!({"op":"status"})).unwrap();
        let ours = status["folders"]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["state"] == folder["state"])
            .cloned()
            .unwrap();
        assert_eq!(ours["mount"]["state"], "attached", "{ours}");
    }
}

/// WP3.INT: a handle's cached view survives another connection's commit that
/// changed no projection row (feed staging, object registration, status
/// writes), instead of reloading the whole projection at its next use; a
/// commit that did change the projection is seen at once.
#[test]
fn a_view_survives_foreign_commits_that_leave_the_projection_alone() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Store::open(dir.path(), Some("w3-view-freshness")).unwrap();
    engine.create("first", "file").unwrap();
    let other = rusqlite::Connection::open(dir.path().join("metadata.sqlite")).unwrap();
    other
        .busy_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    // Loaded once.
    engine.lookup("first").unwrap();
    let loads = engine.view_loads();
    engine.lookup("first").unwrap();
    assert_eq!(engine.view_loads(), loads, "a fresh view is reused");
    // A foreign commit to a table outside the projection: kept.
    other
        .execute(
            "INSERT INTO object_requests VALUES('w3-not-a-projection-row', 1)",
            [],
        )
        .unwrap();
    engine.lookup("first").unwrap();
    assert_eq!(
        engine.view_loads(),
        loads,
        "a commit that left the projection alone keeps the view"
    );
    // A foreign commit that changes a projection row: reloaded, and seen.
    let renamed: String = other
        .query_row(
            "SELECT entry FROM current_parts WHERE entry LIKE '%\"first\"%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let renamed = renamed.replace("\"first\"", "\"changed-elsewhere\"");
    other
        .execute(
            "UPDATE current_parts SET entry=? WHERE entry LIKE '%\"first\"%'",
            [&renamed],
        )
        .unwrap();
    assert!(
        engine.lookup("changed-elsewhere").is_ok(),
        "the change is seen"
    );
    assert!(
        engine.view_loads() > loads,
        "reloaded after a projection change"
    );
    // Its own commits keep the view fresh, also across foreign commits.
    let loads = engine.view_loads();
    engine.create("second", "file").unwrap();
    other
        .execute(
            "INSERT INTO object_requests VALUES('w3-another-row', 2)",
            [],
        )
        .unwrap();
    engine.create("third", "file").unwrap();
    engine.lookup("second").unwrap();
    engine.lookup("third").unwrap();
    assert_eq!(
        engine.view_loads(),
        loads,
        "own commits patch the view; a foreign non-projection commit keeps it"
    );
}
