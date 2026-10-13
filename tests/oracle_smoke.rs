//! WP0.3: the differential oracle (`Store::verify_derived_state`) and view-cache
//! freshness on secondary handles.
use std::{collections::BTreeSet, path::Path};
use tempfile::TempDir;
use tkfs::core::*;

const REPO: &str = "oracle-smoke";

fn store(path: &Path) -> Store {
    Store::open(path, Some(REPO)).unwrap()
}

fn replicas() -> (TempDir, Store, TempDir, Store) {
    let a_dir = tempfile::tempdir().unwrap();
    let b_dir = tempfile::tempdir().unwrap();
    let a = store(a_dir.path());
    let b = store(b_dir.path());
    (a_dir, a, b_dir, b)
}

#[track_caller]
fn assert_oracle_empty(s: &mut Store, step: &str) {
    let diff = s.verify_derived_state().unwrap();
    assert!(diff.is_empty(), "{step}: {diff:#?}");
}

/// Deliver every shared event the receiver lacks, with the objects available
/// to the sender. Both replicas are authorized authors.
fn sync(from: &Store, to: &mut Store) {
    let known = to.shared_event_inventory().unwrap();
    let allowed = BTreeSet::from([from.device.clone(), to.device.clone()]);
    to.receive(from.shared_bundle(&known).unwrap(), &allowed)
        .unwrap();
}

fn sync_both(a: &mut Store, b: &mut Store) {
    sync(a, b);
    sync(b, a);
}

fn content(s: &Store, path: &str) -> Vec<u8> {
    s.read_object(&s.lookup(path).unwrap().content.unwrap())
        .unwrap()
}

fn write(s: &mut Store, path: &str, bytes: &[u8]) -> String {
    let entry = s.lookup(path).unwrap();
    s.write_revision(&entry.id, entry.revisions.get("content").cloned(), bytes)
        .unwrap()
}

fn sequential_history() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    assert_oracle_empty(&mut s, "empty store");
    s.create("docs", "directory").unwrap();
    s.create("docs/a.txt", "file").unwrap();
    assert_oracle_empty(&mut s, "creates");
    write(&mut s, "docs/a.txt", b"one");
    write(&mut s, "docs/a.txt", b"two");
    assert_oracle_empty(&mut s, "edits");
    let a = s.lookup("docs/a.txt").unwrap();
    s.rename(&a.id, "docs/b.txt", false).unwrap();
    assert_oracle_empty(&mut s, "rename");
    // Temp-file replacement: one atomic location + tombstone event.
    s.create("docs/b.txt.tmp", "file").unwrap();
    write(&mut s, "docs/b.txt.tmp", b"replacement");
    let temp = s.lookup("docs/b.txt.tmp").unwrap();
    s.rename(&temp.id, "docs/b.txt", true).unwrap();
    assert_eq!(content(&s, "docs/b.txt"), b"replacement");
    assert_oracle_empty(&mut s, "temp replacement");
    let empty = s.create("empty", "directory").unwrap();
    s.delete(&empty.id).unwrap();
    let docs = s.lookup("docs").unwrap();
    s.rename(&docs.id, "archive", false).unwrap();
    let mut basic = s.lookup("archive/b.txt").unwrap().basic_info();
    basic.attributes = BasicInfo::attributes("file", 0x1).unwrap();
    s.set_basic(&s.lookup("archive/b.txt").unwrap().id, basic)
        .unwrap();
    assert_oracle_empty(&mut s, "delete, directory move, basic metadata");
    // Cross a startup-snapshot interval, then resume from it after restart.
    s.create("archive/log.txt", "file").unwrap();
    for round in 0..140 {
        write(
            &mut s,
            "archive/log.txt",
            format!("line {round}").as_bytes(),
        );
    }
    assert_oracle_empty(&mut s, "long linear chain");
    drop(s);
    let mut s = store(dir.path());
    assert_eq!(content(&s, "archive/log.txt"), b"line 139");
    assert_oracle_empty(&mut s, "after restart");
}

fn concurrent_edits() {
    let (_a_dir, mut a, _b_dir, mut b) = replicas();
    a.create("shared.txt", "file").unwrap();
    sync(&a, &mut b);
    write(&mut a, "shared.txt", b"from-a");
    write(&mut b, "shared.txt", b"from-b");
    sync_both(&mut a, &mut b);
    for (s, name) in [(&mut a, "a"), (&mut b, "b")] {
        assert!(
            s.conflicts()
                .unwrap()
                .iter()
                .any(|c| c.field == "content" && c.resolved_by.is_none())
        );
        assert_oracle_empty(s, &format!("concurrent edits on {name}"));
    }
    let conflict = a
        .conflicts()
        .unwrap()
        .into_iter()
        .find(|c| c.field == "content")
        .unwrap();
    let selected = conflict.alternatives[0].revision.clone();
    let review = a
        .resolve(std::slice::from_ref(&conflict.id), &selected)
        .unwrap();
    assert_oracle_empty(&mut a, "resolved on a");
    sync(&a, &mut b);
    let resolved = b
        .conflicts()
        .unwrap()
        .into_iter()
        .find(|c| c.id == conflict.id)
        .unwrap();
    assert_eq!(resolved.resolved_by, Some(review));
    assert_oracle_empty(&mut b, "resolution delivered to b");
}

fn namespace_collision_before_and_after_resolve() {
    let (_a_dir, mut a, _b_dir, mut b) = replicas();
    a.create("same.txt", "file").unwrap();
    b.create("same.txt", "file").unwrap();
    sync_both(&mut a, &mut b);
    for (s, name) in [(&mut a, "a"), (&mut b, "b")] {
        assert!(
            s.conflicts()
                .unwrap()
                .iter()
                .any(|c| c.field == "namespace" && c.resolved_by.is_none())
        );
        assert_oracle_empty(s, &format!("namespace collision on {name}"));
    }
    let conflict = a
        .conflicts()
        .unwrap()
        .into_iter()
        .find(|c| c.field == "namespace")
        .unwrap();
    let visible = a.lookup("same.txt").unwrap().id;
    let loser = conflict
        .entity
        .split(',')
        .find(|e| *e != visible)
        .unwrap()
        .to_owned();
    let review = a
        .recover(&loser, "recovered.txt", std::slice::from_ref(&conflict.id))
        .unwrap();
    assert_eq!(a.lookup("recovered.txt").unwrap().id, loser);
    assert_oracle_empty(&mut a, "namespace resolved on a");
    sync(&a, &mut b);
    assert_eq!(b.lookup("recovered.txt").unwrap().id, loser);
    assert!(
        b.conflicts()
            .unwrap()
            .iter()
            .any(|c| c.id == conflict.id && c.resolved_by.as_deref() == Some(review.as_str()))
    );
    assert_oracle_empty(&mut b, "namespace resolution delivered to b");
}

fn deleted_parent() {
    let (_a_dir, mut a, _b_dir, mut b) = replicas();
    a.create("dir", "directory").unwrap();
    sync(&a, &mut b);
    let dir = a.lookup("dir").unwrap();
    a.delete(&dir.id).unwrap();
    b.create("dir/child.txt", "file").unwrap();
    sync_both(&mut a, &mut b);
    for (s, name) in [(&mut a, "a"), (&mut b, "b")] {
        assert!(s.lookup("dir/child.txt").is_err());
        assert!(
            s.conflicts()
                .unwrap()
                .iter()
                .any(|c| c.reason.contains("deleted/missing parent"))
        );
        assert_oracle_empty(s, &format!("deleted parent on {name}"));
    }
}

fn directory_cycle() {
    let (_a_dir, mut a, _b_dir, mut b) = replicas();
    a.create("left", "directory").unwrap();
    a.create("right", "directory").unwrap();
    sync(&a, &mut b);
    let left = a.lookup("left").unwrap();
    let right = b.lookup("right").unwrap();
    a.rename(&left.id, "right/left", false).unwrap();
    b.rename(&right.id, "left/right", false).unwrap();
    sync_both(&mut a, &mut b);
    for (s, name) in [(&mut a, "a"), (&mut b, "b")] {
        assert!(
            s.conflicts()
                .unwrap()
                .iter()
                .any(|c| c.reason.contains("cycle"))
        );
        assert_oracle_empty(s, &format!("directory cycle on {name}"));
    }
}

#[test]
fn oracle_empty_on_valid_histories() {
    sequential_history();
    concurrent_edits();
    namespace_collision_before_and_after_resolve();
    deleted_parent();
    directory_cycle();
}

/// One store whose history holds a resolved content conflict, an unresolved
/// namespace conflict and a multi-event frontier.
fn conflicted() -> (TempDir, TempDir, Store) {
    let (a_dir, mut a, b_dir, mut b) = replicas();
    a.create("shared.txt", "file").unwrap();
    sync(&a, &mut b);
    write(&mut a, "shared.txt", b"from-a");
    write(&mut b, "shared.txt", b"from-b");
    a.create("same.txt", "file").unwrap();
    b.create("same.txt", "file").unwrap();
    sync_both(&mut a, &mut b);
    let conflict = a
        .conflicts()
        .unwrap()
        .into_iter()
        .find(|c| c.field == "content")
        .unwrap();
    let selected = conflict.alternatives[0].revision.clone();
    a.resolve(std::slice::from_ref(&conflict.id), &selected)
        .unwrap();
    sync(&b, &mut a);
    drop(b);
    assert!(
        a.conflicts()
            .unwrap()
            .iter()
            .any(|c| c.resolved_by.is_some())
    );
    assert!(
        a.conflicts()
            .unwrap()
            .iter()
            .any(|c| c.field == "namespace" && c.resolved_by.is_none())
    );
    assert_oracle_empty(&mut a, "conflicted fixture");
    (a_dir, b_dir, a)
}

#[test]
fn oracle_detects_tampered_conflicts_and_frontier() {
    // A cleared review receipt.
    let (_a, _b, mut s) = conflicted();
    s.db.execute("UPDATE conflicts SET resolved_by=NULL", [])
        .unwrap();
    let diff = s.verify_derived_state().unwrap();
    assert!(!diff.resolved_by_mismatch.is_empty(), "{diff:#?}");
    assert!(diff.conflicts_missing.is_empty() && diff.conflicts_extra.is_empty());
    assert!(diff.frontier_mismatch.is_empty() && diff.projection_mismatch.is_empty());

    // A lost causal frontier.
    let (_a, _b, mut s) = conflicted();
    let frontier: i64 =
        s.db.query_row("SELECT count(*) FROM event_frontier", [], |r| r.get(0))
            .unwrap();
    s.db.execute("DELETE FROM event_frontier", []).unwrap();
    let diff = s.verify_derived_state().unwrap();
    assert_eq!(diff.frontier_mismatch.len() as i64, frontier, "{diff:#?}");
    assert!(
        diff.frontier_mismatch
            .iter()
            .all(|m| m.ends_with(": missing"))
    );
    assert!(diff.resolved_by_mismatch.is_empty() && diff.conflicts_missing.is_empty());

    // A deleted conflict record and a fabricated namespace record.
    let (_a, _b, mut s) = conflicted();
    let (id, branch): (String, String) =
        s.db.query_row(
            "SELECT id,branch FROM conflicts WHERE resolved_by IS NOT NULL",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    s.db.execute("DELETE FROM conflicts WHERE id=?", [&id])
        .unwrap();
    let mut fake = s
        .conflicts()
        .unwrap()
        .into_iter()
        .find(|c| c.field == "namespace")
        .unwrap();
    fake.id = hash(b"fabricated");
    s.db.execute(
        "INSERT INTO conflicts(id,branch,payload) VALUES(?,?,?)",
        rusqlite::params![fake.id, branch, serde_json::to_string(&fake).unwrap()],
    )
    .unwrap();
    let diff = s.verify_derived_state().unwrap();
    assert_eq!(diff.conflicts_missing, vec![format!("{branch}/{id}")]);
    assert_eq!(diff.conflicts_extra, vec![format!("{branch}/{}", fake.id)]);

    // A stored projection that no longer matches the journal.
    let (_a, _b, mut s) = conflicted();
    let active = s.active.clone();
    let mut projection = s.projection(&active).unwrap();
    let entry = projection.entries.values_mut().find(|e| e.alive).unwrap();
    entry.name = "tampered.txt".into();
    let tampered = entry.id.clone();
    s.db.execute("DELETE FROM current_parts", []).unwrap();
    s.db.execute(
        "UPDATE current_state SET payload=? WHERE branch=?",
        rusqlite::params![serde_json::to_string(&projection).unwrap(), active],
    )
    .unwrap();
    let diff = s.verify_derived_state().unwrap();
    assert!(
        diff.projection_mismatch
            .contains(&format!("{active}: entry {tampered}")),
        "{diff:#?}"
    );
}

/// Every row of every table, for byte-for-byte state comparison.
fn dump(db: &rusqlite::Connection) -> Vec<(String, Vec<Vec<rusqlite::types::Value>>)> {
    let tables: Vec<String> = db
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    tables
        .into_iter()
        .map(|table| {
            let mut statement = db
                .prepare(&format!("SELECT * FROM \"{table}\" ORDER BY rowid"))
                .or_else(|_| db.prepare(&format!("SELECT * FROM \"{table}\"")))
                .unwrap();
            let columns = statement.column_count();
            let rows = statement
                .query_map([], |r| {
                    (0..columns)
                        .map(|i| r.get::<_, rusqlite::types::Value>(i))
                        .collect::<rusqlite::Result<Vec<_>>>()
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            (table, rows)
        })
        .collect()
}

#[test]
fn oracle_has_no_side_effects() {
    let (_a, _b, mut s) = conflicted();
    let before = dump(&s.db);
    let projection = s.projection(&s.active).unwrap();
    let conflicts = s.conflicts().unwrap();
    assert!(s.verify_derived_state().unwrap().is_empty());
    assert!(s.db.is_autocommit(), "the oracle savepoint must be closed");
    assert_eq!(dump(&s.db), before);
    assert_eq!(s.projection(&s.active).unwrap(), projection);
    assert_eq!(s.conflicts().unwrap(), conflicts);

    // The store stays fully usable, and the view cache is coherent.
    s.create("after-oracle.txt", "file").unwrap();
    write(&mut s, "after-oracle.txt", b"still writable");
    assert_eq!(content(&s, "after-oracle.txt"), b"still writable");
    assert_oracle_empty(&mut s, "commits after the oracle");

    // A detected difference is reported, never repaired.
    s.db.execute("DELETE FROM event_frontier", []).unwrap();
    let before = dump(&s.db);
    assert!(!s.verify_derived_state().unwrap().is_empty());
    assert_eq!(dump(&s.db), before);
    assert!(s.db.is_autocommit());

    // A rebuild failure inside the oracle is returned and fully rolled back.
    let (_a, _b, mut s) = conflicted();
    let id: String =
        s.db.query_row("SELECT id FROM events ORDER BY id LIMIT 1", [], |r| {
            r.get(0)
        })
        .unwrap();
    s.db.execute("UPDATE events SET payload='{' WHERE id=?", [&id])
        .unwrap();
    let before = dump(&s.db);
    assert!(s.verify_derived_state().is_err());
    assert!(s.db.is_autocommit());
    assert_eq!(dump(&s.db), before);
}

#[test]
fn secondary_handle_sees_engine_commits() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = store(dir.path());
    let first = engine.create("a.txt", "file").unwrap();
    let mut secondary = Store::open_secondary(dir.path(), &engine.repo, &engine.device).unwrap();
    // Warm the secondary view cache before the engine commits.
    assert_eq!(secondary.lookup("a.txt").unwrap().id, first.id);
    assert!(secondary.lookup("b.txt").is_err());

    // Incremental creation, content and namespace commits on the engine.
    let created = engine.create("b.txt", "file").unwrap();
    assert_eq!(secondary.lookup("b.txt").unwrap().id, created.id);
    write(&mut engine, "b.txt", b"engine bytes");
    assert_eq!(content(&secondary, "b.txt"), b"engine bytes");
    engine.rename(&created.id, "c.txt", false).unwrap();
    assert!(secondary.lookup("b.txt").is_err());
    assert_eq!(secondary.lookup("c.txt").unwrap().id, created.id);

    // A full-replay commit: a remote creation collides with a.txt.
    let mut foreign = engine
        .events()
        .unwrap()
        .into_iter()
        .find(|e| e.changes.iter().any(|c| c.entity == first.id))
        .unwrap();
    foreign.id = id();
    foreign.device = "foreign-device".into();
    for change in &mut foreign.changes {
        change.entity = "foreign-entry".into();
    }
    engine.commit(foreign, false).unwrap();
    assert!(
        engine
            .conflicts()
            .unwrap()
            .iter()
            .any(|c| c.field == "namespace")
    );
    assert_eq!(
        secondary.projection(&secondary.active).unwrap(),
        engine.projection(&engine.active).unwrap()
    );
    assert_eq!(
        secondary.lookup("a.txt").unwrap(),
        engine.lookup("a.txt").unwrap()
    );

    // The engine's own cache likewise follows another handle's commit.
    let from_secondary = secondary.create("from-secondary.txt", "file").unwrap();
    assert_eq!(
        engine.lookup("from-secondary.txt").unwrap().id,
        from_secondary.id
    );
    secondary
        .rename(&from_secondary.id, "moved.txt", false)
        .unwrap();
    assert!(engine.lookup("from-secondary.txt").is_err());
    assert_eq!(engine.lookup("moved.txt").unwrap().id, from_secondary.id);
    assert_oracle_empty(&mut engine, "after commits from both handles");
}
