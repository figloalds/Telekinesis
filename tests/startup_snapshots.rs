use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, process::Command};
use tkfs::core::*;

fn open(path: &Path) -> Store {
    Store::open(path, Some("startup-snapshots")).unwrap()
}

fn snapshot(s: &Store) -> (i64, String, String) {
    s.db.query_row(
        "SELECT cursor,payload,checksum FROM startup_snapshots WHERE branch=?",
        [&s.active],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )
    .unwrap()
}

fn set_snapshot(s: &Store, saved: &(i64, String, String)) {
    s.db.execute(
        "UPDATE startup_snapshots SET cursor=?,payload=?,checksum=? WHERE branch=?",
        params![saved.0, saved.1, saved.2, s.active],
    )
    .unwrap();
}

fn frontier(s: &Store) -> Vec<String> {
    s.db.prepare("SELECT event FROM event_frontier ORDER BY event")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

fn save(s: &mut Store, name: &str, bytes: &[u8]) {
    let entry = s.lookup(name).unwrap();
    s.write_revision(&entry.id, entry.revisions.get("content").cloned(), bytes)
        .unwrap();
}

#[test]
fn suffix_replay_restores_damaged_materialization_without_changing_sync_state() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = open(dir.path());
    let base = snapshot(&s);
    let entry = s.create("file", "file").unwrap();
    save(&mut s, "file", b"one");
    s.rename(&entry.id, "renamed", false).unwrap();
    save(&mut s, "renamed", b"two");
    let deleted = s.create("deleted", "file").unwrap();
    s.delete(&deleted.id).unwrap();
    assert_eq!(
        snapshot(&s),
        base,
        "ordinary edits leave a real replay suffix"
    );
    let expected = s.projection(&s.active).unwrap();
    assert_eq!(expected, project(&s.active, &s.events().unwrap()).unwrap());
    let events = s.events().unwrap();
    let heads = frontier(&s);
    s.acknowledge(&events.iter().map(|e| e.id.clone()).collect::<Vec<_>>())
        .unwrap();
    s.db.execute("INSERT INTO peer_known VALUES('peer',?)", [&events[0].id])
        .unwrap();
    s.db.execute("INSERT INTO receipts VALUES('receipt','hash','{}')", [])
        .unwrap();
    s.db.execute_batch("DELETE FROM current_parts; UPDATE current_state SET payload='broken'; DELETE FROM event_frontier;").unwrap();
    drop(s);
    let s = open(dir.path());
    assert_eq!(s.projection(&s.active).unwrap(), expected);
    assert_eq!(s.events().unwrap(), events);
    assert_eq!(frontier(&s), heads);
    assert_eq!(s.lookup("renamed").unwrap().id, entry.id);
    assert_eq!(
        s.read_object(&s.lookup("renamed").unwrap().content.unwrap())
            .unwrap(),
        b"two"
    );
    assert_eq!(s.outgoing_unacknowledged().unwrap(), 0);
    for table in ["peer_known", "receipts"] {
        assert_eq!(
            s.db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }
    assert_eq!(snapshot(&s).0, events.len() as i64);
    let sequence: i64 =
        s.db.query_row(
            "SELECT seq FROM sqlite_sequence WHERE name='journal_sequence'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    drop(s);
    let s = open(dir.path());
    assert_eq!(s.projection(&s.active).unwrap(), expected);
    assert_eq!(
        s.db.query_row(
            "SELECT seq FROM sqlite_sequence WHERE name='journal_sequence'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        sequence
    );
}

#[test]
fn invalid_snapshots_fall_back_and_are_replaced() {
    for mode in [
        "checksum", "version", "identity", "cursor", "journal", "missing",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path());
        s.create("file", "file").unwrap();
        save(&mut s, "file", b"durable");
        let expected = s.projection(&s.active).unwrap();
        drop(s);
        let s = open(dir.path()); // consolidate the suffix
        let saved = snapshot(&s);
        if mode == "missing" {
            s.db.execute("DELETE FROM startup_snapshots", []).unwrap();
        } else {
            let mut value: Value = serde_json::from_str(&saved.1).unwrap();
            match mode {
                "version" => value["format"] = json!(999),
                "identity" => value["repo"] = json!("wrong"),
                "cursor" => value["cursor"] = json!(saved.0 + 99),
                "journal" => value["journal_hash"] = json!("wrong"),
                _ => value["projection"] = json!({}),
            }
            let payload = serde_json::to_string(&value).unwrap();
            let checksum = if mode == "checksum" {
                saved.2
            } else {
                hash(payload.as_bytes())
            };
            set_snapshot(&s, &(saved.0, payload, checksum));
        }
        drop(s);
        let s = open(dir.path());
        assert_eq!(s.projection(&s.active).unwrap(), expected, "{mode}");
        let saved = snapshot(&s);
        assert_eq!(hash(saved.1.as_bytes()), saved.2);
        assert_eq!(
            serde_json::from_str::<Value>(&saved.1).unwrap()["format"],
            1
        );
    }
}

#[test]
fn unsupported_suffix_falls_back_with_identical_historical_conflicts_and_reviews() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = open(dir.path());
    s.create("same", "file").unwrap();
    drop(s);
    let mut s = open(dir.path());
    let base = snapshot(&s);
    save(&mut s, "same", b"intermediate"); // replay applies this before fallback
    let mut competitor = s
        .events()
        .unwrap()
        .into_iter()
        .find(|e| e.changes.iter().any(|c| c.field == "kind"))
        .unwrap();
    competitor.id = id();
    competitor.device = "peer".into();
    for c in &mut competitor.changes {
        c.entity = "competitor".into();
    }
    s.commit(competitor, false).unwrap();
    let conflict = s
        .conflicts()
        .unwrap()
        .into_iter()
        .find(|c| c.field == "namespace")
        .unwrap();
    let winner = s.lookup("same").unwrap().id;
    let loser = s
        .projection(&s.active)
        .unwrap()
        .entries
        .keys()
        .find(|id| **id != winner)
        .unwrap()
        .clone();
    s.recover(&loser, "recovered", &[conflict.id]).unwrap();
    let expected = s.projection(&s.active).unwrap();
    let conflicts = s.conflicts().unwrap();
    assert!(conflicts.iter().any(|c| c.resolved_by.is_some()));
    set_snapshot(&s, &base); // simulate a writer with no snapshot consolidation
    drop(s);
    let s = open(dir.path());
    assert_eq!(s.projection(&s.active).unwrap(), expected);
    assert_eq!(s.conflicts().unwrap(), conflicts);
    drop(s);
    let s = open(dir.path()); // the newly sealed snapshot retains resolved history
    assert_eq!(s.conflicts().unwrap(), conflicts);
}

#[test]
fn branches_missing_content_and_outer_rollback_preserve_snapshot_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = open(dir.path());
    s.create("file", "file").unwrap();
    save(&mut s, "file", b"main");
    let main = s.active.clone();
    let private = s.fork("private", false).unwrap();
    s.checkout(&private.id).unwrap();
    save(&mut s, "file", b"private");
    let expected: BTreeMap<_, _> = s
        .branches()
        .unwrap()
        .into_iter()
        .map(|b| (b.id.clone(), s.projection(&b.id).unwrap()))
        .collect();
    let base = snapshot(&s);
    s.db.execute_batch("SAVEPOINT outer").unwrap();
    for _ in 0..128 {
        save(&mut s, "file", b"rolled back");
    }
    assert!(snapshot(&s).0 > base.0);
    s.db.execute_batch("ROLLBACK TO outer; RELEASE outer")
        .unwrap();
    assert_eq!(snapshot(&s), base);
    std::fs::remove_file(dir.path().join("objects").join(hash(b"main"))).unwrap();
    drop(s);
    let mut s = open(dir.path());
    assert_eq!(s.active, private.id);
    for (branch, projection) in expected {
        assert_eq!(s.projection(&branch).unwrap(), projection);
    }
    s.checkout(&main).unwrap();
    assert!(
        s.read_object(&s.lookup("file").unwrap().content.unwrap())
            .is_err()
    );
    assert_eq!(s.status().unwrap()["content"]["current_missing"], 1);
}

#[test]
fn journal_edit_invalidates_snapshots_and_sequence_never_reuses_deleted_ids() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = open(dir.path());
    s.create("file", "file").unwrap();
    drop(s);
    let s = open(dir.path());
    let seq = snapshot(&s).0;
    s.db.execute("UPDATE events SET payload=payload", [])
        .unwrap();
    assert_eq!(
        s.db.query_row("SELECT count(*) FROM startup_snapshots", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    s.db.execute_batch("DELETE FROM events; DELETE FROM outbox")
        .unwrap();
    drop(s);
    let mut s = open(dir.path());
    assert!(s.lookup("file").is_err());
    s.create("new", "file").unwrap();
    assert!(
        s.db.query_row("SELECT max(seq) FROM journal_sequence", [], |r| r
            .get::<_, i64>(0))
            .unwrap()
            > seq
    );
}

#[test]
fn periodic_seals_bound_replay_and_oversized_legacy_suffix_reconstructs() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = open(dir.path());
    let original = snapshot(&s);
    s.create("file", "file").unwrap();
    for _ in 0..129 {
        save(&mut s, "file", b"latest");
    }
    assert!(snapshot(&s).0 >= 128);
    let expected = s.projection(&s.active).unwrap();
    let count = s.events().unwrap().len();
    set_snapshot(&s, &original);
    drop(s);
    let s = open(dir.path());
    assert_eq!(s.projection(&s.active).unwrap(), expected);
    assert_eq!(snapshot(&s).0, count as i64);
}

#[test]
fn snapshot_fault_worker() {
    let Ok(root) = std::env::var("TKFS_SNAPSHOT_WORKER") else {
        return;
    };
    let mut s = open(Path::new(&root));
    if std::env::var("TKFS_SNAPSHOT_WRITE").is_ok() {
        for _ in 0..128 {
            save(&mut s, "file", b"next");
        }
    }
}

#[test]
fn crashes_during_suffix_and_snapshot_replacement_keep_the_committed_view() {
    for point in [
        "startup_suffix_applied",
        "before_startup_snapshot",
        "startup_snapshot_written",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path());
        s.create("file", "file").unwrap();
        save(&mut s, "file", b"durable");
        let expected = s.projection(&s.active).unwrap();
        let saved = snapshot(&s);
        drop(s);
        let result = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "snapshot_fault_worker", "--nocapture"])
            .env("TKFS_SNAPSHOT_WORKER", dir.path())
            .env("TKFS_FAULT", point)
            .output()
            .unwrap();
        assert_eq!(
            result.status.code(),
            Some(86),
            "{point}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        let db = Connection::open(dir.path().join("metadata.sqlite")).unwrap();
        let after: (i64, String, String) = db
            .query_row(
                "SELECT cursor,payload,checksum FROM startup_snapshots",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            after, saved,
            "{point}: uncommitted snapshot must not escape"
        );
        drop(db);
        let s = open(dir.path());
        assert_eq!(s.projection(&s.active).unwrap(), expected, "{point}");
    }
    // Also crash snapshot replacement inside a live event transaction.
    let dir = tempfile::tempdir().unwrap();
    let mut s = open(dir.path());
    s.create("file", "file").unwrap();
    drop(s);
    drop(open(dir.path()));
    let result = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "snapshot_fault_worker", "--nocapture"])
        .env("TKFS_SNAPSHOT_WORKER", dir.path())
        .env("TKFS_SNAPSHOT_WRITE", "1")
        .env("TKFS_FAULT", "startup_snapshot_written")
        .output()
        .unwrap();
    assert_eq!(result.status.code(), Some(86));
    let s = open(dir.path());
    assert_eq!(
        s.events().unwrap().len(),
        128,
        "snapshot and its 128th suffix event roll back together"
    );
    assert_eq!(
        s.projection(&s.active).unwrap(),
        project(&s.active, &s.events().unwrap()).unwrap()
    );
    assert_eq!(
        Connection::open(dir.path().join("metadata.sqlite"))
            .unwrap()
            .query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
}
