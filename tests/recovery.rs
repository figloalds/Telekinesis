use serde_json::json;
use std::collections::BTreeSet;
use tkfs::{
    backend::{DirectoryBackend, ObjectBackend},
    core::*,
    runtime::{Engine, namespace_notifications},
};
fn collision() -> (tempfile::TempDir, Store, String, String) {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Store::open(dir.path(), Some("recover-tests")).unwrap();
    let a = s.create("same.txt", "file").unwrap();
    let mut b = s.events().unwrap()[0].clone();
    b.id = id();
    b.device = "other-device".into();
    for c in &mut b.changes {
        c.entity = "other-entry".into();
    }
    s.commit(b, false).unwrap();
    (dir, s, a.id, "other-entry".into())
}
#[test]
fn namespace_recovery_preserves_id_bytes_and_exact_review_across_restart() {
    let (dir, mut s, a, b) = collision();
    let conflict = s
        .conflicts()
        .unwrap()
        .into_iter()
        .find(|c| c.field == "namespace")
        .unwrap();
    let loser = if s.lookup("same.txt").unwrap().id == a {
        b
    } else {
        a
    };
    let revision = s
        .recover(&loser, "recovered.txt", std::slice::from_ref(&conflict.id))
        .unwrap();
    assert_eq!(s.lookup("recovered.txt").unwrap().id, loser);
    assert!(s.lookup("same.txt").is_ok());
    assert_eq!(
        s.conflicts()
            .unwrap()
            .iter()
            .find(|c| c.id == conflict.id)
            .unwrap()
            .resolved_by,
        Some(revision)
    );
    let events = s.events().unwrap();
    let expected = s.projection(&s.active).unwrap();
    drop(s);
    let s = Store::open(dir.path(), None).unwrap();
    assert_eq!(s.projection(&s.active).unwrap(), expected);
    let peer = tempfile::tempdir().unwrap();
    let mut p = Store::open(peer.path(), Some("recover-tests")).unwrap();
    p.put_object(b"").unwrap();
    for e in events.iter().rev() {
        p.commit(e.clone(), false).unwrap();
    }
    assert_eq!(p.projection(&p.active).unwrap(), expected);
    assert_eq!(p.conflicts().unwrap(), s.conflicts().unwrap());
}
#[test]
fn namespace_recovery_does_not_review_unseen_third_creation() {
    let (_dir, mut s, a, b) = collision();
    let conflict = s.conflicts().unwrap()[0].clone();
    let mut third = s
        .events()
        .unwrap()
        .into_iter()
        .find(|e| e.changes.iter().any(|c| c.entity == b))
        .unwrap();
    third.id = id();
    third.device = "third-device".into();
    third.dependencies.clear();
    for c in &mut third.changes {
        c.entity = "third-entry".into();
    }
    let loser = if s.lookup("same.txt").unwrap().id == a {
        b
    } else {
        a
    };
    s.recover(&loser, "recovered.txt", std::slice::from_ref(&conflict.id))
        .unwrap();
    s.commit(third, false).unwrap();
    assert!(
        s.conflicts()
            .unwrap()
            .iter()
            .any(|c| c.resolved_by.is_none() && c.entity.contains("third-entry"))
    );
    assert_eq!(s.lookup("recovered.txt").unwrap().id, loser);
}
#[test]
fn recovery_can_restore_child_with_deleted_parent_at_a_free_root_path() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Store::open(dir.path(), Some("recover-tests")).unwrap();
    let parent = s.create("dir", "directory").unwrap();
    let child = s.create("dir/child.txt", "file").unwrap();
    let delete = s.current_change(&parent.id, "alive", json!(false)).unwrap();
    let branch = s.branch(&s.active).unwrap();
    s.commit(s.event(&branch, vec![delete]), true).unwrap();
    let conflict = s
        .conflicts()
        .unwrap()
        .into_iter()
        .find(|c| c.entity == child.id && c.field == "namespace")
        .unwrap();
    s.recover(&child.id, "rescued.txt", &[conflict.id]).unwrap();
    assert_eq!(s.lookup("rescued.txt").unwrap().id, child.id);
}
#[test]
fn checkpoint_restore_is_private_and_preserves_current_and_original_history() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Store::open(dir.path(), Some("recover-tests")).unwrap();
    let f = s.create("code", "file").unwrap();
    s.write_revision(&f.id, f.revisions.get("content").cloned(), b"snapshot")
        .unwrap();
    let cp = s.checkpoint("before").unwrap();
    let f = s.lookup("code").unwrap();
    s.write_revision(&f.id, f.revisions.get("content").cloned(), b"later")
        .unwrap();
    let current = s.active.clone();
    let b = s.restore(&cp, "restored").unwrap();
    assert!(!b.shared);
    assert_eq!(s.active, current);
    assert_eq!(s.history().unwrap()[0]["id"], cp);
    s.checkout(&b.id).unwrap();
    assert_eq!(
        s.read_object(&s.lookup("code").unwrap().content.unwrap())
            .unwrap(),
        b"snapshot"
    );
    s.checkout("main").unwrap();
    assert_eq!(
        s.read_object(&s.lookup("code").unwrap().content.unwrap())
            .unwrap(),
        b"later"
    );
}
#[test]
fn local_bucket_adapter_uploads_only_shared_roots_and_verifies_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Store::open(dir.path(), Some("recover-tests")).unwrap();
    let f = s.create("code", "file").unwrap();
    s.write_revision(&f.id, f.revisions.get("content").cloned(), b"shared")
        .unwrap();
    let private = s.fork("private", false).unwrap();
    s.checkout(&private.id).unwrap();
    let f = s.lookup("code").unwrap();
    s.write_revision(
        &f.id,
        f.revisions.get("content").cloned(),
        b"PRIVATE-CANARY",
    )
    .unwrap();
    let bucket = tempfile::tempdir().unwrap();
    let backend = DirectoryBackend::new(bucket.path()).unwrap();
    let exported = s.export_shared_objects(&backend).unwrap();
    assert_eq!(exported["remote_validated"], false);
    assert_eq!(backend.get_verified(&hash(b"shared")).unwrap(), b"shared");
    assert!(backend.get_verified(&hash(b"PRIVATE-CANARY")).is_err());
    let manifest: serde_json::Value = serde_json::from_slice(
        &backend
            .get_verified(exported["manifest"].as_str().unwrap())
            .unwrap(),
    )
    .unwrap();
    assert!(
        manifest["events"]
            .as_array()
            .unwrap()
            .iter()
            .all(|e| e["branch"]["shared"] == true)
    );
    assert!(
        !serde_json::to_string(&manifest)
            .unwrap()
            .contains(&private.id)
    );
    std::fs::write(bucket.path().join(hash(b"shared")), b"corrupt").unwrap();
    assert!(backend.get_verified(&hash(b"shared")).is_err());
}
#[test]
fn peer_refreshes_readonly_sessions_but_preserves_live_writer_bases() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Store::open(dir.path(), Some("recover-tests")).unwrap();
    let f = s.create("code", "file").unwrap();
    let mut e = Engine::new(s);
    let (reader, _) = e.open("code", None, false).unwrap();
    let base = e.store.lookup("code").unwrap();
    let branch = e.store.branch(&e.store.active).unwrap();
    let mut remote = e.store.event(
        &branch,
        vec![Change {
            entity: f.id.clone(),
            field: "content".into(),
            parents: vec![base.revisions["content"].clone()],
            value: json!(e.store.put_object(b"remote").unwrap()),
        }],
    );
    remote.device = "peer".into();
    let allowed = BTreeSet::from(["peer".into()]);
    let bundle = Bundle {
        repo: e.store.repo.clone(),
        branches: vec![branch],
        events: vec![remote],
        objects: std::collections::BTreeMap::from([(hash(b"remote"), hex::encode(b"remote"))]),
    };
    let before = e.store.projection(&e.store.active).unwrap();
    e.receive_peer(bundle, &allowed).unwrap();
    assert_eq!(e.session(reader).unwrap().bytes, b"remote");
    assert!(
        !namespace_notifications(&before, &e.store.projection(&e.store.active).unwrap()).is_empty()
    );
    e.close(reader).unwrap();
}
