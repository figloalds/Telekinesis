use serde_json::json;
use std::{collections::BTreeSet, path::Path};
use tkfs::{
    core::*,
    runtime::{Engine, decrypt, encrypt},
};

fn store(path: &Path) -> Store {
    Store::open(path, Some("test-repo")).unwrap()
}
fn changes(e: &str, kind: &str, name: &str, content: &str) -> Vec<Change> {
    vec![
        Change {
            entity: e.into(),
            field: "kind".into(),
            parents: vec![],
            value: json!(kind),
        },
        Change {
            entity: e.into(),
            field: "alive".into(),
            parents: vec![],
            value: json!(true),
        },
        Change {
            entity: e.into(),
            field: "location".into(),
            parents: vec![],
            value: json!({"parent":ROOT,"name":name}),
        },
        Change {
            entity: e.into(),
            field: "content".into(),
            parents: vec![],
            value: json!(hash(content.as_bytes())),
        },
    ]
}
fn event(s: &Store, revision: &str, device: &str, cs: Vec<Change>) -> Event {
    let mut e = s.event(&s.branch("main").unwrap(), cs);
    e.id = revision.into();
    e.device = device.into();
    e.created_ms = 123;
    e
}
fn edit(s: &Store, rev: &str, device: &str, base: &str, bytes: &str) -> Event {
    event(
        s,
        rev,
        device,
        vec![Change {
            entity: "file".into(),
            field: "content".into(),
            parents: vec![base.into()],
            value: json!(hash(bytes.as_bytes())),
        }],
    )
}
fn seed(s: &mut Store) -> Event {
    for b in ["base", "a", "b", "c", "d", "successor"] {
        s.put_object(b.as_bytes()).unwrap();
    }
    let e = event(
        s,
        "zz-base",
        "device-z",
        changes("file", "file", "test.txt", "base"),
    );
    s.commit(e.clone(), true).unwrap();
    e
}
fn permutations<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
    if items.is_empty() {
        return vec![vec![]];
    }
    let mut all = vec![];
    for i in 0..items.len() {
        let mut tail = items.to_vec();
        let first = tail.remove(i);
        for mut p in permutations(&tail) {
            p.insert(0, first.clone());
            all.push(p);
        }
    }
    all
}

#[test]
fn causal_successor_wins_even_with_lower_identity() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    seed(&mut s);
    s.commit(
        edit(&s, "00-successor", "device-a", "zz-base", "successor"),
        true,
    )
    .unwrap();
    assert_eq!(
        s.lookup("test.txt").unwrap().content,
        Some(hash(b"successor"))
    );
    assert!(s.projection(&s.active).unwrap().conflicts.is_empty());
}

#[test]
fn three_competitors_converge_in_all_delivery_orders_with_duplicates_and_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let mut origin = store(dir.path());
    let base = seed(&mut origin);
    let events = vec![
        base,
        edit(&origin, "a", "device-a", "zz-base", "a"),
        edit(&origin, "b", "device-b", "zz-base", "b"),
        edit(&origin, "c", "device-c", "zz-base", "c"),
    ];
    let mut expected: Option<(Projection, Vec<Conflict>)> = None;
    for order in permutations(&events) {
        let root = tempfile::tempdir().unwrap();
        let mut s = store(root.path());
        for bytes in ["base", "a", "b", "c"] {
            s.put_object(bytes.as_bytes()).unwrap();
        }
        for e in order {
            s.commit(e.clone(), false).unwrap();
            s.commit(e, false).unwrap();
            drop(s);
            s = store(root.path());
        }
        let result = (s.projection(&s.active).unwrap(), s.conflicts().unwrap());
        assert!(result.0.pending.is_empty());
        assert_eq!(result.0.heads[&register("file", "content")].len(), 3);
        assert_eq!(result.0.conflicts.len(), 3);
        assert_eq!(result.0.entries["file"].content, Some(hash(b"c")));
        if let Some(want) = &expected {
            assert_eq!(&result, want);
        } else {
            expected = Some(result);
        }
    }
}

#[test]
fn resolution_reviews_exact_pairs_and_unseen_concurrency_survives() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    seed(&mut s);
    let a = edit(&s, "a", "device-a", "zz-base", "a");
    let b = edit(&s, "b", "device-b", "zz-base", "b");
    let d = edit(&s, "d", "device-d", "zz-base", "d");
    s.commit(a, true).unwrap();
    s.commit(b, true).unwrap();
    let reviewed = s.conflicts().unwrap()[0].id.clone();
    s.resolve(std::slice::from_ref(&reviewed), "a").unwrap();
    assert!(s.projection(&s.active).unwrap().conflicts.is_empty());
    s.commit(d, false).unwrap();
    assert!(!s.projection(&s.active).unwrap().conflicts.is_empty());
    let conflicts = s.conflicts().unwrap();
    assert!(
        conflicts
            .iter()
            .find(|c| c.id == reviewed)
            .unwrap()
            .resolved_by
            .is_some()
    );
    assert!(
        conflicts
            .iter()
            .any(|c| c.resolved_by.is_none() && c.alternatives.iter().any(|v| v.revision == "d"))
    );
}

#[test]
fn child_before_parent_stays_pending_and_activates_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let base = event(&s, "base", "a", changes("file", "file", "test.txt", "base"));
    s.put_object(b"base").unwrap();
    s.put_object(b"a").unwrap();
    s.commit(edit(&s, "child", "b", "base", "a"), false)
        .unwrap();
    assert!(s.events().unwrap().is_empty());
    assert_eq!(s.status().unwrap()["incoming_pending"], 1);
    assert!(s.lookup("test.txt").is_err());
    s.commit(base, false).unwrap();
    assert!(s.projection(&s.active).unwrap().pending.is_empty());
    assert_eq!(s.lookup("test.txt").unwrap().content, Some(hash(b"a")));
}

#[test]
fn rename_and_offline_edit_combine_on_stable_identity() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    seed(&mut s);
    s.rename("file", "renamed.txt", false).unwrap();
    s.commit(edit(&s, "offline-edit", "b", "zz-base", "b"), false)
        .unwrap();
    assert!(s.lookup("test.txt").is_err());
    let e = s.lookup("renamed.txt").unwrap();
    assert_eq!(e.id, "file");
    assert_eq!(e.content, Some(hash(b"b")));
}

#[test]
fn temp_replace_is_one_atomic_event_and_retains_old_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let old = s.create("target", "file").unwrap();
    s.write_revision(&old.id, old.revisions.get("content").cloned(), b"old")
        .unwrap();
    let temp = s.create("temp", "file").unwrap();
    s.write_revision(&temp.id, temp.revisions.get("content").cloned(), b"new")
        .unwrap();
    let before = s.events().unwrap();
    s.rename(&temp.id, "target", true).unwrap();
    let after = s.events().unwrap();
    assert_eq!(after.len(), before.len() + 1);
    assert_eq!(s.lookup("target").unwrap().id, temp.id);
    assert!(s.lookup("temp").is_err());
    assert_eq!(s.read_object(&hash(b"old")).unwrap(), b"old");
    let replacement = after
        .iter()
        .find(|e| !before.iter().any(|prior| prior.id == e.id))
        .unwrap();
    assert_eq!(replacement.changes.len(), 2);
    assert!(
        replacement.changes.iter().any(|c| {
            c.entity == temp.id && c.field == "location" && c.value["name"] == "target"
        })
    );
    assert!(
        replacement
            .changes
            .iter()
            .any(|c| { c.entity == old.id && c.field == "alive" && c.value == false })
    );
}

#[test]
fn delete_edit_preserves_both_existence_and_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    seed(&mut s);
    s.delete("file").unwrap();
    let e = event(
        &s,
        "edit",
        "offline",
        vec![
            Change {
                entity: "file".into(),
                field: "content".into(),
                parents: vec!["zz-base".into()],
                value: json!(hash(b"a")),
            },
            Change {
                entity: "file".into(),
                field: "alive".into(),
                parents: vec!["zz-base".into()],
                value: json!(true),
            },
        ],
    );
    s.commit(e, false).unwrap();
    assert!(s.conflicts().unwrap().iter().any(|c| c.field == "alive"));
    assert_eq!(s.read_object(&hash(b"a")).unwrap(), b"a");
}

#[test]
fn same_name_and_file_directory_collisions_converge() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    s.put_object(b"base").unwrap();
    let a = event(
        &s,
        "create-a",
        "a",
        changes("entity-a", "file", "ReadMe", "base"),
    );
    let mut cs = changes("entity-b", "directory", "README", "base");
    cs.retain(|c| c.field != "content");
    let b = event(&s, "create-b", "b", cs);
    assert_eq!(
        project(&s.active, &[a.clone(), b.clone()]).unwrap(),
        project(&s.active, &[b.clone(), a.clone()]).unwrap()
    );
    s.commit(a, false).unwrap();
    s.commit(b, false).unwrap();
    assert_eq!(s.lookup("readme").unwrap().kind, "directory");
    assert!(
        s.conflicts()
            .unwrap()
            .iter()
            .any(|c| c.field == "namespace")
    );
}

#[test]
fn concurrent_directory_cycle_is_suppressed_and_preserved() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let a = s.create("a", "directory").unwrap();
    let b = s.create("b", "directory").unwrap();
    assert!(s.rename(&a.id, "a/nested", false).is_err());
    let ea = event(
        &s,
        "move-a",
        "a",
        vec![Change {
            entity: a.id.clone(),
            field: "location".into(),
            parents: vec![a.revisions["location"].clone()],
            value: json!({"parent":b.id,"name":"a"}),
        }],
    );
    let eb = event(
        &s,
        "move-b",
        "b",
        vec![Change {
            entity: b.id.clone(),
            field: "location".into(),
            parents: vec![b.revisions["location"].clone()],
            value: json!({"parent":a.id,"name":"b"}),
        }],
    );
    let mut forward = s.events().unwrap();
    forward.extend([ea.clone(), eb.clone()]);
    let mut reverse = s.events().unwrap();
    reverse.extend([eb, ea]);
    assert_eq!(
        project(&s.active, &forward).unwrap(),
        project(&s.active, &reverse).unwrap()
    );
    s.commit(forward[forward.len() - 2].clone(), false).unwrap();
    s.commit(forward.last().unwrap().clone(), false).unwrap();
    assert!(s.lookup("a").is_err());
    assert!(
        s.conflicts()
            .unwrap()
            .iter()
            .any(|c| c.reason.contains("cycle"))
    );
}

#[test]
fn deleted_parent_and_descendant_creation_is_an_inspectable_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let parent = s.create("dir", "directory").unwrap();
    s.delete(&parent.id).unwrap();
    s.put_object(b"base").unwrap();
    let mut cs = changes("child", "file", "child.txt", "base");
    cs.iter_mut().find(|c| c.field == "location").unwrap().value =
        json!({"parent":parent.id,"name":"child.txt"});
    s.commit(event(&s, "new-child", "offline", cs), false)
        .unwrap();
    assert!(s.lookup("dir/child.txt").is_err());
    assert!(s.conflicts().unwrap().iter().any(|c| c.entity == "child"));
    assert_eq!(s.read_object(&hash(b"base")).unwrap(), b"base");
}

#[test]
fn missing_corrupt_content_cannot_enter_visible_journal() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let e = event(&s, "new", "a", changes("file", "file", "test.txt", "base"));
    assert!(s.commit(e.clone(), false).is_err());
    assert!(s.events().unwrap().is_empty());
    s.put_object(b"base").unwrap();
    std::fs::write(s.root.join("objects").join(hash(b"base")), b"bad").unwrap();
    assert!(s.commit(e, false).is_err());
    assert!(s.events().unwrap().is_empty());
}

#[test]
fn private_branch_upload_is_rooted_in_shared_events_across_restart_and_dedup() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let e = s.create("shared.txt", "file").unwrap();
    s.write_revision(&e.id, e.revisions.get("content").cloned(), b"shared")
        .unwrap();
    let private = s.fork("private", false).unwrap();
    s.checkout(&private.id).unwrap();
    let e = s.create("secret", "file").unwrap();
    s.write_revision(
        &e.id,
        e.revisions.get("content").cloned(),
        b"PRIVATE-CANARY",
    )
    .unwrap();
    drop(s);
    let mut s = store(dir.path());
    let bundle = s.shared_bundle(&BTreeSet::new()).unwrap();
    assert!(!bundle.objects.contains_key(&hash(b"PRIVATE-CANARY")));
    assert!(
        !serde_json::to_string(&bundle)
            .unwrap()
            .contains(&private.id)
    );
    let e = s.lookup("secret").unwrap();
    s.write_revision(
        &e.id,
        e.revisions.get("content").cloned(),
        b"published-visible",
    )
    .unwrap();
    let published = s.fork("public", true).unwrap();
    let bundle = s.shared_bundle(&BTreeSet::new()).unwrap();
    assert!(bundle.objects.contains_key(&hash(b"published-visible")));
    assert!(!bundle.objects.contains_key(&hash(b"PRIVATE-CANARY")));
    let root = bundle
        .events
        .iter()
        .find(|e| e.branch.id == published.id)
        .unwrap();
    assert!(root.changes.iter().all(|c| c.parents.is_empty()));
}

#[test]
fn long_lived_writer_keeps_real_base_and_shared_local_staging() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    seed(&mut s);
    let mut e = Engine::new(s);
    let (h, _) = e.open("test.txt", None, true).unwrap();
    let (reader, _) = e.open("test.txt", None, false).unwrap();
    e.store
        .commit(edit(&e.store, "remote", "z", "zz-base", "b"), false)
        .unwrap();
    e.truncate(h, 0, false).unwrap();
    e.write(h, 0, b"a", false, false).unwrap();
    assert_eq!(e.session(reader).unwrap().bytes, b"a");
    e.flush(h).unwrap();
    assert!(
        !e.store
            .projection(&e.store.active)
            .unwrap()
            .conflicts
            .is_empty()
    );
    e.write(h, 1, b"!", false, false).unwrap();
    e.flush(h).unwrap();
    assert!(
        !e.store
            .projection(&e.store.active)
            .unwrap()
            .conflicts
            .is_empty()
    );
    e.cleanup(h, false).unwrap();
    e.close(h).unwrap();
    e.close(reader).unwrap();
}

#[test]
fn checkout_refusal_preserves_branch_and_retry_receipts_are_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let s = store(dir.path());
    let mut e = Engine::new(s);
    let branch_payload = json!({"op":"branch","name":"other"});
    let b = e.control("branch-request", &branch_payload).unwrap();
    assert_eq!(b, e.control("branch-request", &branch_payload).unwrap());
    assert!(
        e.control("branch-request", &json!({"op":"branch","name":"wrong"}))
            .is_err()
    );
    let original = e.store.active.clone();
    let (h, _) = e.open("", None, false).unwrap();
    assert!(
        e.control("switch", &json!({"op":"checkout","name":"other"}))
            .is_err()
    );
    assert_eq!(e.store.active, original);
    e.close(h).unwrap();
    let p = json!({"op":"checkout","name":"other","generation":0});
    let result = e.control("switch", &p).unwrap();
    assert_eq!(result, e.control("switch", &p).unwrap());
    assert_eq!(e.store.generation, 1);
    let cp = json!({"op":"checkpoint","message":"coherent"});
    let first = e.control("cp", &cp).unwrap();
    assert_eq!(first, e.control("cp", &cp).unwrap());
}

#[test]
fn authenticated_encrypted_transport_rejects_tamper_and_wrong_key() {
    let k = [7; 32];
    let ciphertext = encrypt(&k, b"SOURCE-CANARY").unwrap();
    assert!(!ciphertext.windows(13).any(|w| w == b"SOURCE-CANARY"));
    assert_eq!(decrypt(&k, &ciphertext).unwrap(), b"SOURCE-CANARY");
    assert!(decrypt(&[8; 32], &ciphertext).is_err());
    let mut bad = ciphertext;
    bad[13] ^= 1;
    assert!(decrypt(&k, &bad).is_err());
}

#[test]
fn fault_worker() {
    let Ok(root) = std::env::var("TKFS_TEST_WORKER") else {
        return;
    };
    let mut s = store(Path::new(&root));
    let e = s.lookup("test.txt").unwrap();
    s.write_revision(&e.id, e.revisions.get("content").cloned(), b"acknowledged")
        .unwrap();
}
#[test]
fn process_crash_boundaries_preserve_last_committed_generation() {
    for (point, new_visible) in [
        ("object_flushed", false),
        ("object_installed", false),
        ("before_sql", false),
        ("sql_committed", true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut s = store(dir.path());
        seed(&mut s);
        drop(s);
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "fault_worker", "--nocapture"])
            .env("TKFS_TEST_WORKER", dir.path())
            .env("TKFS_FAULT", point)
            .output()
            .unwrap();
        assert_eq!(
            result.status.code(),
            Some(86),
            "{point}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        let s = store(dir.path());
        let object = s.lookup("test.txt").unwrap().content.unwrap();
        assert_eq!(
            s.read_object(&object).unwrap(),
            if new_visible {
                b"acknowledged".as_slice()
            } else {
                b"base".as_slice()
            }
        );
    }
}
