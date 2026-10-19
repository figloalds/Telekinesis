//! WP4.INT post-integration re-review (RC1): the conflict gate of the
//! creation and namespace successors is scoped to what the event changes.
//!
//! One unresolved conflict anywhere on a branch used to send every creation,
//! rename and delete to the full-replay slow path, local filesystem
//! operations included (run inline under the engine lock, O(history) each).
//! Now only a conflict that touches the event's entities, parent directories
//! or slots does; every commit stays equal to a rebuild (the strict oracle
//! after each one), on the replica that makes the change and on the one that
//! receives it.
use std::collections::BTreeSet;
use tempfile::TempDir;
use tkfs::core::*;

const REPO: &str = "scoped-conflict-gate";

struct Replica {
    _dir: TempDir,
    store: Store,
}

impl Replica {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), Some(REPO)).unwrap();
        Self { _dir: dir, store }
    }
}

#[track_caller]
fn assert_exact(store: &mut Store, after: &str) {
    let diff = store.verify_derived_state_strict().unwrap();
    assert!(diff.is_empty(), "after {after}: {diff:#?}");
}

/// Sends `from`'s shared events that `to` lacks; returns how many.
fn sync(from: &mut Replica, to: &mut Replica, allowed: &BTreeSet<String>) -> usize {
    let known = to.store.shared_event_inventory().unwrap();
    let bundle = from.store.shared_bundle(&known).unwrap();
    let count = bundle.events.len();
    to.store.receive(bundle, allowed).unwrap();
    count
}

/// Two replicas with a linear history, then a content conflict on one file
/// (concurrent saves of `conflicted.txt` exchanged both ways), unresolved on
/// both.
fn replicas_with_one_conflict() -> (Replica, Replica, BTreeSet<String>) {
    let mut a = Replica::new();
    let mut b = Replica::new();
    let allowed: BTreeSet<String> = [a.store.device.clone(), b.store.device.clone()].into();
    let conflicted = a.store.create("conflicted.txt", "file").unwrap();
    for i in 0..30 {
        a.store.create(&format!("file-{i:02}.txt"), "file").unwrap();
    }
    a.store.create("dir", "directory").unwrap();
    sync(&mut a, &mut b, &allowed);
    let base = a.store.lookup("conflicted.txt").unwrap();
    a.store
        .write_revision(&conflicted.id, base.revisions.get("content").cloned(), b"a")
        .unwrap();
    b.store
        .write_revision(&conflicted.id, base.revisions.get("content").cloned(), b"b")
        .unwrap();
    sync(&mut a, &mut b, &allowed);
    sync(&mut b, &mut a, &allowed);
    for replica in [&mut a, &mut b] {
        let unresolved = replica
            .store
            .conflicts()
            .unwrap()
            .into_iter()
            .filter(|c| c.resolved_by.is_none())
            .count();
        assert_eq!(unresolved, 1, "one unresolved content conflict");
        assert!(
            replica
                .store
                .branch_has_unresolved_conflicts(&replica.store.active.clone())
                .unwrap()
        );
        assert_exact(&mut replica.store, "the conflict");
    }
    (a, b, allowed)
}

/// The review's scenario: with one unresolved content conflict on the
/// branch, local creations, directory creations, renames, moves and deletes
/// of other files take the fast path (before the fix every one was a full
/// replay: `fast 0, slow N`), and each commit equals a rebuild. A peer that
/// receives them (with the same conflict open) takes the fast path too.
#[test]
fn namespace_operations_beside_an_unrelated_conflict_take_the_fast_path() {
    let (mut a, mut b, allowed) = replicas_with_one_conflict();
    let before = a.store.commit_path_counters();
    let operations = 6;
    a.store.create("new.txt", "file").unwrap();
    assert_exact(&mut a.store, "create");
    a.store.create("made", "directory").unwrap();
    assert_exact(&mut a.store, "mkdir");
    let file = a.store.lookup("file-00.txt").unwrap();
    a.store.rename(&file.id, "dir/moved.txt", false).unwrap();
    assert_exact(&mut a.store, "move");
    let file = a.store.lookup("file-01.txt").unwrap();
    a.store
        .rename(&file.id, "file-01-renamed.txt", false)
        .unwrap();
    assert_exact(&mut a.store, "rename");
    let file = a.store.lookup("file-02.txt").unwrap();
    a.store.delete(&file.id).unwrap();
    assert_exact(&mut a.store, "delete");
    let dir = a.store.lookup("made").unwrap();
    a.store.rename(&dir.id, "dir/made", false).unwrap();
    assert_exact(&mut a.store, "directory move");
    let after = a.store.commit_path_counters();
    assert_eq!(
        (after.fast - before.fast, after.slow - before.slow),
        (operations, 0),
        "local namespace commits beside an unrelated conflict"
    );
    // The receiving replica has the same conflict open.
    let before = b.store.commit_path_counters();
    assert_eq!(sync(&mut a, &mut b, &allowed), operations as usize);
    assert_exact(&mut b.store, "receive");
    let after = b.store.commit_path_counters();
    assert_eq!(
        (after.fast - before.fast, after.slow - before.slow),
        (operations, 0),
        "remote namespace commits beside an unrelated conflict"
    );
    assert!(b.store.lookup("dir/made").is_ok());
    assert!(b.store.lookup("file-02.txt").is_err());
    assert_eq!(
        a.store.projection(&a.store.active.clone()).unwrap(),
        b.store.projection(&b.store.active.clone()).unwrap()
    );
}

/// What the conflict touches still takes the slow path: renaming or deleting
/// the conflicted file, and a name collision that becomes a namespace
/// conflict keeps later operations on that slot and its entities slow; all
/// stay exact.
#[test]
fn namespace_operations_that_touch_a_conflict_take_the_slow_path() {
    let (mut a, mut b, allowed) = replicas_with_one_conflict();
    let before = a.store.commit_path_counters();
    let conflicted = a.store.lookup("conflicted.txt").unwrap();
    a.store
        .rename(&conflicted.id, "dir/conflicted.txt", false)
        .unwrap();
    assert_exact(&mut a.store, "move of the conflicted file");
    let after = a.store.commit_path_counters();
    assert_eq!((after.fast - before.fast, after.slow - before.slow), (0, 1));

    // A namespace conflict: both replicas create `same.txt` concurrently.
    a.store.create("same.txt", "file").unwrap();
    b.store.create("same.txt", "file").unwrap();
    sync(&mut a, &mut b, &allowed);
    sync(&mut b, &mut a, &allowed);
    for replica in [&mut a, &mut b] {
        assert_exact(&mut replica.store, "the name collision");
    }
    // Beside the open name collision (in the same directory), creating and
    // renaming other entries keeps the fast path.
    let before = a.store.commit_path_counters();
    a.store.create("beside-collision.txt", "file").unwrap();
    assert_exact(&mut a.store, "create beside the collision");
    let file = a.store.lookup("file-04.txt").unwrap();
    a.store
        .rename(&file.id, "file-04-renamed.txt", false)
        .unwrap();
    assert_exact(&mut a.store, "rename beside the collision");
    let after = a.store.commit_path_counters();
    assert_eq!((after.fast - before.fast, after.slow - before.slow), (2, 0));
    // Taking the colliding name (another entry into that slot) touches it.
    let before = a.store.commit_path_counters();
    let file = a.store.lookup("file-05.txt").unwrap();
    assert!(a.store.rename(&file.id, "same.txt", false).is_err());
    let after = a.store.commit_path_counters();
    assert_eq!(after.fast, before.fast);
    let shown = a.store.lookup("same.txt").unwrap();
    // Renaming the visible winner of the collision touches it: slow.
    let before = a.store.commit_path_counters();
    a.store
        .rename(&shown.id, "same-renamed.txt", false)
        .unwrap();
    assert_exact(&mut a.store, "rename of a colliding entry");
    let after = a.store.commit_path_counters();
    assert_eq!((after.fast - before.fast, after.slow - before.slow), (0, 1));
    // An unrelated rename is still fast.
    let before = a.store.commit_path_counters();
    let file = a.store.lookup("file-03.txt").unwrap();
    a.store
        .rename(&file.id, "file-03-renamed.txt", false)
        .unwrap();
    assert_exact(&mut a.store, "unrelated rename");
    let after = a.store.commit_path_counters();
    assert_eq!((after.fast - before.fast, after.slow - before.slow), (1, 0));
    sync(&mut a, &mut b, &allowed);
    assert_exact(&mut b.store, "receive");
    assert_eq!(
        a.store.projection(&a.store.active.clone()).unwrap(),
        b.store.projection(&b.store.active.clone()).unwrap()
    );
}
