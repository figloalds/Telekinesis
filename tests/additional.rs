use serde_json::json;
use std::{collections::BTreeSet, path::Path};
use tkfs::{core::*, runtime::Engine};
fn store(path: &Path) -> Store {
    Store::open(path, Some("additional-tests")).unwrap()
}
#[test]
fn review_all_three_pairs_in_one_resolution() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let f = s.create("code", "file").unwrap();
    let base = f.revisions["content"].clone();
    let branch = s.branch(&s.active).unwrap();
    let mut events = vec![];
    for name in ["a", "b", "c"] {
        let mut e = s.event(
            &branch,
            vec![Change {
                entity: f.id.clone(),
                field: "content".into(),
                parents: vec![base.clone()],
                value: json!(s.put_object(name.as_bytes()).unwrap()),
            }],
        );
        e.device = name.into();
        events.push(e);
    }
    let selected = events[0].id.clone();
    for e in events {
        s.commit(e, true).unwrap();
    }
    let reviewed: Vec<_> = s
        .conflicts()
        .unwrap()
        .iter()
        .map(|c| c.id.clone())
        .collect();
    assert_eq!(reviewed.len(), 3);
    s.resolve(&reviewed, &selected).unwrap();
    assert!(s.projection(&s.active).unwrap().conflicts.is_empty());
    assert!(
        s.conflicts()
            .unwrap()
            .iter()
            .all(|c| c.resolved_by.is_some())
    );
}
#[test]
fn correction_before_namespace_collision_has_identical_retained_conflicts() {
    let dir = tempfile::tempdir().unwrap();
    let mut origin = store(dir.path());
    let file = origin.create("same.txt", "file").unwrap();
    let a = origin.events().unwrap()[0].clone();
    let mut b = a.clone();
    b.id = id();
    b.device = "other".into();
    for c in &mut b.changes {
        c.entity = "other-file".into();
        if c.field == "location" {
            c.value["name"] = json!("SAME.txt");
        }
    }
    origin.commit(b.clone(), false).unwrap();
    origin.rename(&file.id, "recovered.txt", false).unwrap();
    let correction = origin
        .events()
        .unwrap()
        .into_iter()
        .find(|e| e.id != a.id && e.id != b.id)
        .unwrap();
    let expected = (
        origin.projection(&origin.active).unwrap(),
        origin.conflicts().unwrap(),
    );
    for order in [
        [a.clone(), b.clone(), correction.clone()],
        [correction.clone(), b.clone(), a.clone()],
        [b.clone(), correction.clone(), a.clone()],
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut s = store(dir.path());
        s.put_object(b"").unwrap();
        for e in order {
            s.commit(e, false).unwrap();
        }
        assert_eq!(
            (s.projection(&s.active).unwrap(), s.conflicts().unwrap()),
            expected
        );
    }
}
#[test]
fn private_conflict_alternatives_are_excluded_from_current_state_publication() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let b = s.fork("private", false).unwrap();
    s.checkout(&b.id).unwrap();
    let f = s.create("code", "file").unwrap();
    let base = f.revisions["content"].clone();
    let mut events = vec![];
    for (device, bytes) in [
        ("a", b"PRIVATE-LOSER".as_slice()),
        ("z", b"VISIBLE-WINNER".as_slice()),
    ] {
        let mut e = s.event(
            &b,
            vec![Change {
                entity: f.id.clone(),
                field: "content".into(),
                parents: vec![base.clone()],
                value: json!(s.put_object(bytes).unwrap()),
            }],
        );
        e.device = device.into();
        events.push(e);
    }
    for e in events {
        s.commit(e, true).unwrap();
    }
    assert!(!s.conflicts().unwrap().is_empty());
    s.fork("public", true).unwrap();
    let bundle = s.shared_bundle(&BTreeSet::new()).unwrap();
    assert!(bundle.objects.contains_key(&hash(b"VISIBLE-WINNER")));
    assert!(!bundle.objects.contains_key(&hash(b"PRIVATE-LOSER")));
    assert_eq!(
        s.read_object(&hash(b"PRIVATE-LOSER")).unwrap(),
        b"PRIVATE-LOSER"
    );
}
#[test]
fn receipt_fault_worker() {
    let Ok(root) = std::env::var("TKFS_RPC_WORKER") else {
        return;
    };
    let mut e = Engine::new(store(Path::new(&root)));
    e.control(
        "durable-request",
        &json!({"op":"import","path":"code","hex":hex::encode(b"with-receipt")}),
    )
    .unwrap();
}
#[test]
fn mutation_and_receipt_have_one_crash_boundary() {
    for (point, committed) in [("sql_committed", false), ("receipt_committed", true)] {
        let dir = tempfile::tempdir().unwrap();
        let mut s = store(dir.path());
        s.create("code", "file").unwrap();
        drop(s);
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "receipt_fault_worker", "--nocapture"])
            .env("TKFS_RPC_WORKER", dir.path())
            .env("TKFS_FAULT", point)
            .output()
            .unwrap();
        assert_eq!(result.status.code(), Some(86));
        let s = store(dir.path());
        let payload = json!({"op":"import","path":"code","hex":hex::encode(b"with-receipt")});
        assert_eq!(
            s.receipt("durable-request", &payload).unwrap().is_some(),
            committed
        );
        assert_eq!(
            s.read_object(&s.lookup("code").unwrap().content.unwrap())
                .unwrap(),
            if committed {
                b"with-receipt".as_slice()
            } else {
                b"".as_slice()
            }
        );
    }
}
