use serde_json::json;
use std::time::{Duration, Instant};
use tkfs::core::*;

fn assert_replay(s: &Store) {
    assert_eq!(
        s.projection(&s.active).unwrap(),
        project(&s.active, &s.events().unwrap()).unwrap()
    );
}

#[test]
fn directory_moves_and_empty_deletes_preserve_descendants_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let mut s = Store::open(root.path(), Some("directory-progress")).unwrap();
    s.create("project", "directory").unwrap();
    let dir = s.create("project/cache", "directory").unwrap();
    let sub = s.create("project/cache/sub", "directory").unwrap();
    let file = s.create("project/cache/sub/file", "file").unwrap();
    s.write_revision(&file.id, file.revisions.get("content").cloned(), b"keep")
        .unwrap();
    let empty = s.create("empty", "directory").unwrap();
    s.rename(&dir.id, "moved", false).unwrap();
    assert_eq!(s.lookup("moved/sub/file").unwrap().id, file.id);
    assert!(s.rename(&dir.id, "moved/sub/cycle", false).is_err());
    assert!(s.delete(&dir.id).is_err());
    assert!(s.rename(&dir.id, "empty", true).is_err());
    assert_replay(&s);
    s.delete(&empty.id).unwrap();
    s.rename(&file.id, "project/file", false).unwrap();
    s.delete(&sub.id).unwrap();
    s.delete(&dir.id).unwrap();
    assert_replay(&s);
    let expected = s.projection(&s.active).unwrap();
    let events = s.events().unwrap();
    drop(s);
    let s = Store::open(root.path(), None).unwrap();
    assert_eq!(s.projection(&s.active).unwrap(), expected);
    assert_eq!(s.events().unwrap(), events);
    assert_eq!(
        s.read_object(&s.lookup("project/file").unwrap().content.unwrap())
            .unwrap(),
        b"keep"
    );
    assert!(s.conflicts().unwrap().is_empty());
    // Also compare against full recovery, not only the new suffix replay path.
    s.db.execute("DELETE FROM startup_snapshots", []).unwrap();
    drop(s);
    let s = Store::open(root.path(), None).unwrap();
    assert_eq!(s.projection(&s.active).unwrap(), expected);
    assert!(s.conflicts().unwrap().is_empty());
}

#[test]
fn direct_directory_commits_that_hide_children_still_record_conflicts() {
    for cycle in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut s = Store::open(root.path(), Some("directory-fallback")).unwrap();
        let dir = s.create("parent", "directory").unwrap();
        let sub = s.create("parent/sub", "directory").unwrap();
        s.create("parent/sub/file", "file").unwrap();
        let branch = s.branch(&s.active).unwrap();
        let change = if cycle {
            s.current_change(&dir.id, "location", json!({"parent":sub.id,"name":"cycle"}))
                .unwrap()
        } else {
            s.current_change(&dir.id, "alive", json!(false)).unwrap()
        };
        s.commit(s.event(&branch, vec![change]), true).unwrap();
        assert_replay(&s);
        let conflicts = s.conflicts().unwrap();
        assert!(conflicts.iter().any(|c| c.field == "namespace"));
        drop(s);
        let s = Store::open(root.path(), None).unwrap();
        assert_eq!(s.conflicts().unwrap(), conflicts);
    }
}

#[test]
fn ide_directory_cleanup_stays_bounded_with_unrelated_history() {
    let root = tempfile::tempdir().unwrap();
    let mut s = Store::open(root.path(), Some("directory-scale")).unwrap();
    s.create("project", "directory").unwrap();
    for i in 0..1000 {
        let f = s.create(&format!("project/file-{i}"), "file").unwrap();
        s.write_revision(&f.id, f.revisions.get("content").cloned(), b"source")
            .unwrap();
    }
    let cache = s.create("project/cache", "directory").unwrap();
    let start = Instant::now();
    s.rename(&cache.id, "project/cache-old", false).unwrap();
    s.delete(&cache.id).unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "ordinary directory cleanup replayed unrelated history: {:?}",
        start.elapsed()
    );
    assert_eq!(
        s.children(&s.lookup("project").unwrap().id).unwrap().len(),
        1000
    );
    assert_replay(&s);
}
