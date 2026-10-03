use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use tkfs::{core::*, runtime::MAX_FRAME};
fn store(path: &std::path::Path) -> Store {
    Store::open(path, Some("replication-regressions")).unwrap()
}
fn deliver(s: &mut Store, events: Vec<Event>, objects: BTreeMap<String, String>) -> Vec<String> {
    let allowed = events.iter().map(|e| e.device.clone()).collect();
    s.receive(
        Bundle {
            repo: s.repo.clone(),
            branches: vec![],
            events,
            objects,
        },
        &allowed,
    )
    .unwrap()
}
#[test]
fn restart_evacuates_legacy_unvalidated_journal_before_local_writes() {
    let source = tempfile::tempdir().unwrap();
    let mut a = store(source.path());
    let file = a.create("code", "file").unwrap();
    let branch = a.branch(&a.active).unwrap();
    let mut malformed = a.event(
        &branch,
        vec![Change {
            entity: file.id.clone(),
            field: "alive".into(),
            parents: vec!["future-parent".into()],
            value: json!(true),
        }],
    );
    malformed.id = "legacy-child".into();
    let destination = tempfile::tempdir().unwrap();
    let mut b = store(destination.path());
    b.receive(
        a.shared_bundle(&BTreeSet::new()).unwrap(),
        &BTreeSet::from([a.device.clone()]),
    )
    .unwrap();
    // Simulate the prior implementation's incomplete-but-acknowledged journal,
    // including a subsequent local event that inherited the poisoned dependency.
    let mut inherited = b.event(
        &branch,
        vec![
            b.current_change(
                &file.id,
                "content",
                json!(b.put_object(b"legacy-save").unwrap()),
            )
            .unwrap(),
        ],
    );
    inherited.id = "legacy-local".into();
    inherited.dependencies.push(malformed.id.clone());
    for event in [&malformed, &inherited] {
        b.db.execute(
            "INSERT INTO events VALUES(?,?,?)",
            rusqlite::params![
                event.id,
                event.branch.id,
                serde_json::to_string(event).unwrap()
            ],
        )
        .unwrap();
        b.db.execute(
            "INSERT INTO outbox(event,acknowledged) VALUES(?,1)",
            [&event.id],
        )
        .unwrap();
    }
    drop(b);
    let mut b = store(destination.path());
    assert!(
        b.events()
            .unwrap()
            .iter()
            .all(|e| e.id != malformed.id && e.id != inherited.id)
    );
    assert_eq!(b.status().unwrap()["incoming_pending"], 2);
    let mut parent = a.event(
        &branch,
        vec![
            a.current_change(
                &file.id,
                "content",
                json!(a.put_object(b"valid-parent").unwrap()),
            )
            .unwrap(),
        ],
    );
    parent.id = "future-parent".into();
    let ack = deliver(
        &mut b,
        vec![parent.clone()],
        BTreeMap::from([(hash(b"valid-parent"), hex::encode(b"valid-parent"))]),
    );
    assert!(ack.contains(&parent.id));
    assert_eq!(b.status().unwrap()["incoming_quarantined"], 2);
    let base = b.lookup("code").unwrap();
    b.write_revision(
        &base.id,
        base.revisions.get("content").cloned(),
        b"new-valid-save",
    )
    .unwrap();
    assert_eq!(
        b.read_object(&b.lookup("code").unwrap().content.unwrap())
            .unwrap(),
        b"new-valid-save"
    );
    assert_eq!(
        b.read_object(&hash(b"legacy-save")).unwrap(),
        b"legacy-save",
        "legacy acknowledged bytes must remain available for inspection/recovery"
    );
    assert!(
        !b.shared_bundle(&BTreeSet::new())
            .unwrap()
            .objects
            .contains_key(&hash(b"legacy-save"))
    );
}
#[test]
fn namespace_history_budget_refuses_commit_without_erasing_records() {
    let destination = tempfile::tempdir().unwrap();
    let mut s = store(destination.path());
    let anchor = s.create("anchor", "directory").unwrap();
    let dirs: Vec<_> = (0..13)
        .map(|i| s.create(&format!("anchor/dir-{i}"), "directory").unwrap())
        .collect();
    let branch = s.branch(&s.active).unwrap();
    let moves: Vec<_> = dirs
        .iter()
        .enumerate()
        .map(|(i, dir)| {
            let name = "collision";
            let mut event = s.event(
                &branch,
                vec![
                    s.current_change(&dir.id, "location", json!({"parent":anchor.id,"name":name}))
                        .unwrap(),
                ],
            );
            event.id = format!("concurrent-{i}");
            event
        })
        .collect();
    for event in &moves[..12] {
        s.commit(event.clone(), true).unwrap();
    }
    let before = s.events().unwrap();
    let conflicts = s.conflicts().unwrap();
    assert!(conflicts.iter().any(|c| c.field == "namespace"));
    let error = s.commit(moves[12].clone(), true).unwrap_err();
    assert!(error.to_string().contains("NAMESPACE_HISTORY_LIMIT"));
    assert_eq!(s.events().unwrap(), before);
    assert_eq!(s.conflicts().unwrap(), conflicts);
    drop(s);
    let s = store(destination.path());
    assert_eq!(s.events().unwrap(), before);
    assert_eq!(s.conflicts().unwrap(), conflicts);
    // A legacy journal may already exceed the new bound. If startup also needs
    // causal repair, failure must roll back BOTH evacuation and conflict rebuild.
    let mut incomplete = s.event(
        &branch,
        vec![Change {
            entity: dirs[0].id.clone(),
            field: "alive".into(),
            parents: vec!["missing-legacy-parent".into()],
            value: json!(true),
        }],
    );
    incomplete.id = "legacy-incomplete".into();
    for event in [&moves[12], &incomplete] {
        s.db.execute(
            "INSERT INTO events VALUES(?,?,?)",
            rusqlite::params![
                event.id,
                event.branch.id,
                serde_json::to_string(event).unwrap()
            ],
        )
        .unwrap();
        s.db.execute("INSERT INTO outbox(event) VALUES(?)", [&event.id])
            .unwrap();
    }
    let prior_events = s.events().unwrap();
    let prior_projection = s.projection(&branch.id).unwrap();
    drop(s);
    let error = Store::open(destination.path(), Some("replication-regressions"))
        .err()
        .unwrap();
    assert!(error.to_string().contains("NAMESPACE_HISTORY_LIMIT"));
    let db = rusqlite::Connection::open(destination.path().join("metadata.sqlite")).unwrap();
    let events: Vec<Event> = db
        .prepare("SELECT payload FROM events ORDER BY id")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|row| serde_json::from_str(&row.unwrap()).unwrap())
        .collect();
    assert_eq!(events, prior_events);
    let records: Vec<Conflict> = db
        .prepare("SELECT payload FROM conflicts ORDER BY id")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|row| serde_json::from_str(&row.unwrap()).unwrap())
        .collect();
    assert_eq!(records, conflicts);
    let projection: String = db
        .query_row(
            "SELECT payload FROM current_state WHERE branch=?",
            [&branch.id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Projection>(&projection).unwrap(),
        prior_projection
    );
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM incoming_events WHERE id=?",
            [&incomplete.id],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(
        db.query_row(
            "SELECT count(*) FROM outbox WHERE event=?",
            [&incomplete.id],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
}
#[test]
fn malformed_deferred_child_cannot_poison_parent_local_flush_or_other_branches() {
    let source = tempfile::tempdir().unwrap();
    let mut a = store(source.path());
    let file = a.create("code", "file").unwrap();
    let other = a.create("other", "file").unwrap();
    let seed = a.shared_bundle(&BTreeSet::new()).unwrap();
    let branch = a.branch(&a.active).unwrap();
    let mut parent = a.event(
        &branch,
        vec![
            a.current_change(
                &other.id,
                "content",
                json!(a.put_object(b"valid-parent").unwrap()),
            )
            .unwrap(),
        ],
    );
    parent.id = "zz-valid-parent".into();
    let mut child = a.event(
        &branch,
        vec![Change {
            entity: file.id.clone(),
            field: "alive".into(),
            parents: vec![parent.id.clone()],
            value: json!(true),
        }],
    );
    child.id = "00-malformed-child".into();
    let mut descendant = child.clone();
    descendant.id = "01-malicious-descendant".into();
    descendant.dependencies = vec![child.id.clone()];
    descendant.changes[0].parents = vec![child.id.clone()];
    let destination = tempfile::tempdir().unwrap();
    let mut b = store(destination.path());
    b.receive(seed, &BTreeSet::from([a.device.clone()]))
        .unwrap();
    let ack = deliver(
        &mut b,
        vec![descendant.clone(), child.clone()],
        BTreeMap::new(),
    );
    assert!(
        ack.is_empty(),
        "causally unvalidated children must not be acknowledged"
    );
    assert!(
        b.events()
            .unwrap()
            .iter()
            .all(|e| e.id != child.id && e.id != descendant.id)
    );
    drop(b);
    let mut b = store(destination.path());
    a.commit(parent.clone(), true).unwrap();
    let ack = deliver(
        &mut b,
        vec![parent.clone()],
        BTreeMap::from([(hash(b"valid-parent"), hex::encode(b"valid-parent"))]),
    );
    assert!(
        ack.contains(&parent.id),
        "valid parent must commit despite malformed staged child"
    );
    let quarantined: Vec<String> =
        b.db.prepare("SELECT id FROM incoming_events WHERE error IS NOT NULL ORDER BY id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
    assert_eq!(quarantined, vec![child.id.clone(), descendant.id.clone()]);
    let mut engine = tkfs::runtime::Engine::new(b);
    let (writer, _) = engine.open("code", None, true).unwrap();
    engine.truncate(writer, 0, false).unwrap();
    engine
        .write(writer, 0, b"acknowledged-local", false, false)
        .unwrap();
    engine.flush(writer).unwrap();
    engine.close(writer).unwrap();
    let (reader, _) = engine.open("code", None, false).unwrap();
    assert_eq!(engine.session(reader).unwrap().bytes, b"acknowledged-local");
    engine.close(reader).unwrap();
    assert!(
        engine.store.events().unwrap().iter().all(
            |e| !e.dependencies.contains(&child.id) && !e.dependencies.contains(&descendant.id)
        )
    );
    drop(engine);
    let mut b = store(destination.path());
    assert_eq!(
        b.read_object(&b.lookup("code").unwrap().content.unwrap())
            .unwrap(),
        b"acknowledged-local"
    );
    let side = a.fork("independent", true).unwrap();
    a.checkout(&side.id).unwrap();
    a.create("side-file", "file").unwrap();
    let bundle = a.shared_bundle(&BTreeSet::new()).unwrap();
    b.receive(bundle, &BTreeSet::from([a.device.clone()]))
        .unwrap();
    assert!(lookup(&b.projection(&side.id).unwrap(), "side-file").is_ok());
    let replica = tempfile::tempdir().unwrap();
    let mut c = store(replica.path());
    c.receive(
        b.shared_bundle(&BTreeSet::new()).unwrap(),
        &BTreeSet::from([a.device.clone(), b.device.clone()]),
    )
    .unwrap();
    assert_eq!(
        c.read_object(&c.lookup("code").unwrap().content.unwrap())
            .unwrap(),
        b"acknowledged-local"
    );
    assert!(
        c.events()
            .unwrap()
            .iter()
            .all(|e| e.id != child.id && e.id != descendant.id)
    );
}
#[test]
fn three_origin_cycle_survives_ordinary_correction_in_every_delivery_order() {
    let source = tempfile::tempdir().unwrap();
    let mut a = store(source.path());
    let dirs: Vec<_> = ["A", "B", "C"]
        .iter()
        .map(|name| a.create(name, "directory").unwrap())
        .collect();
    let seed = a.events().unwrap();
    let branch = a.branch(&a.active).unwrap();
    let mut moves = vec![];
    for i in 0..3 {
        let mut e = a.event(
            &branch,
            vec![
                a.current_change(
                    &dirs[i].id,
                    "location",
                    json!({"parent":dirs[(i+1)%3].id,"name":dirs[i].name}),
                )
                .unwrap(),
            ],
        );
        e.id = format!("move-{i}");
        e.device = format!("origin-{i}");
        moves.push(e);
    }
    let mut correction = moves[0].clone();
    correction.id = "correction".into();
    correction
        .dependencies
        .extend([moves[0].id.clone(), moves[1].id.clone()]);
    correction.changes[0].parents = vec![moves[0].id.clone()];
    correction.changes[0].value = json!({"parent":ROOT,"name":"A"});
    let baseline_dir = tempfile::tempdir().unwrap();
    let mut baseline = store(baseline_dir.path());
    deliver(&mut baseline, seed.clone(), BTreeMap::new());
    deliver(&mut baseline, moves.clone(), BTreeMap::new());
    let before = baseline.conflicts().unwrap();
    let cycle: Vec<_> = before
        .iter()
        .filter(|c| c.field == "namespace" && c.reason.contains("cycle"))
        .cloned()
        .collect();
    assert_eq!(cycle.len(), 3);
    deliver(&mut baseline, vec![correction.clone()], BTreeMap::new());
    let after = baseline.conflicts().unwrap();
    assert!(
        cycle.iter().all(|record| after.contains(record)),
        "ordinary correction cannot erase inspectable historical cycle records"
    );
    let expected = (baseline.projection(&baseline.active).unwrap(), after);
    let events = [
        moves[0].clone(),
        moves[1].clone(),
        moves[2].clone(),
        correction,
    ];
    for i in 0..4 {
        for j in 0..4 {
            for k in 0..4 {
                for l in 0..4 {
                    let order = [i, j, k, l];
                    if order.iter().copied().collect::<BTreeSet<_>>().len() != 4 {
                        continue;
                    }
                    let destination = tempfile::tempdir().unwrap();
                    let mut b = store(destination.path());
                    deliver(&mut b, seed.clone(), BTreeMap::new());
                    for index in order {
                        deliver(&mut b, vec![events[index].clone()], BTreeMap::new());
                    }
                    drop(b);
                    let b = store(destination.path());
                    assert_eq!(
                        (b.projection(&b.active).unwrap(), b.conflicts().unwrap()),
                        expected,
                        "order {order:?}"
                    );
                }
            }
        }
    }
}
#[test]
fn large_atomic_publication_uses_bounded_pages_and_durable_partial_objects() {
    let source = tempfile::tempdir().unwrap();
    let mut a = store(source.path());
    let private = a.fork("private", false).unwrap();
    a.checkout(&private.id).unwrap();
    for (name, byte) in [("a.bin", 1), ("b.bin", 2)] {
        let f = a.create(name, "file").unwrap();
        a.write_revision(
            &f.id,
            f.revisions.get("content").cloned(),
            &vec![byte; MAX_FILE],
        )
        .unwrap();
    }
    let shared = a.fork("released", true).unwrap();
    let destination = tempfile::tempdir().unwrap();
    let mut b = store(destination.path());
    let allowed = BTreeSet::from([a.device.clone()]);
    let mut pages = 0;
    loop {
        let known: BTreeSet<_> = b.events().unwrap().iter().map(|e| e.id.clone()).collect();
        let objects = b.shared_object_inventory().unwrap();
        let page = a
            .shared_page(&known, &objects, MAX_FRAME - 8 * 1024 * 1024)
            .unwrap();
        if page.events.is_empty() {
            break;
        }
        let bytes = serde_json::to_vec(&page).unwrap();
        assert!(bytes.len() < MAX_FRAME);
        pages += 1;
        b.receive(page, &allowed).unwrap();
        if pages == 1 {
            assert!(
                b.branch(&shared.id).is_err(),
                "partial atomic publication must not expose an empty branch"
            );
            assert_eq!(b.events().unwrap().len(), 0);
            drop(b);
            b = store(destination.path());
        }
        assert!(pages < 5, "backlog must make progress");
    }
    assert_eq!(pages, 2);
    b.checkout(&shared.id).unwrap();
    for (name, byte) in [("a.bin", 1), ("b.bin", 2)] {
        assert_eq!(
            b.read_object(&b.lookup(name).unwrap().content.unwrap())
                .unwrap(),
            vec![byte; MAX_FILE]
        );
    }
}
#[test]
fn paired_peer_cannot_disclose_private_cas_using_a_hash_only_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let private = s.fork("private", false).unwrap();
    s.checkout(&private.id).unwrap();
    let f = s.create("secret", "file").unwrap();
    let secret = b"PRIVATE-KNOWN-HASH-CANARY";
    s.write_revision(&f.id, f.revisions.get("content").cloned(), secret)
        .unwrap();
    let h = hash(secret);
    s.checkout("main").unwrap();
    let main = s.branch("main").unwrap();
    let mut event = s.event(
        &main,
        vec![
            Change {
                entity: "attack".into(),
                field: "kind".into(),
                parents: vec![],
                value: json!("file"),
            },
            Change {
                entity: "attack".into(),
                field: "alive".into(),
                parents: vec![],
                value: json!(true),
            },
            Change {
                entity: "attack".into(),
                field: "location".into(),
                parents: vec![],
                value: json!({"parent":ROOT,"name":"stolen"}),
            },
            Change {
                entity: "attack".into(),
                field: "content".into(),
                parents: vec![],
                value: json!(h),
            },
        ],
    );
    event.device = "paired-peer".into();
    let bundle = Bundle {
        repo: s.repo.clone(),
        branches: vec![main],
        events: vec![event],
        objects: BTreeMap::new(),
    };
    let ack = s
        .receive(bundle, &BTreeSet::from(["paired-peer".into()]))
        .unwrap();
    assert!(ack.is_empty());
    assert!(s.lookup("stolen").is_err());
    assert!(!s.shared_object_inventory().unwrap().contains(&h));
    assert!(
        !s.shared_bundle(&BTreeSet::new())
            .unwrap()
            .objects
            .contains_key(&h)
    );
    drop(s);
    let s = store(dir.path());
    assert!(
        !s.shared_bundle(&BTreeSet::new())
            .unwrap()
            .objects
            .contains_key(&h)
    );
    assert_eq!(s.read_object(&h).unwrap(), secret);
}
#[test]
fn corrected_deleted_parent_conflict_is_identical_in_every_delivery_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut origin = store(dir.path());
    let parent = origin.create("S", "directory").unwrap();
    let seed = origin.events().unwrap()[0].clone();
    let branch = origin.branch(&origin.active).unwrap();
    let delete = origin.event(
        &branch,
        vec![
            origin
                .current_change(&parent.id, "alive", json!(false))
                .unwrap(),
        ],
    );
    let child = origin.create("S/child", "file").unwrap();
    let create = origin
        .events()
        .unwrap()
        .into_iter()
        .find(|e| e.id != seed.id)
        .unwrap();
    origin.rename(&child.id, "child", false).unwrap();
    let rename = origin
        .events()
        .unwrap()
        .into_iter()
        .find(|e| e.id != seed.id && e.id != create.id)
        .unwrap();
    let events = [seed, delete, create, rename];
    let mut expected = None;
    for i in 0..4 {
        for j in 0..4 {
            for k in 0..4 {
                for l in 0..4 {
                    let order = [i, j, k, l];
                    if order.iter().copied().collect::<BTreeSet<_>>().len() != 4 {
                        continue;
                    }
                    let dir = tempfile::tempdir().unwrap();
                    let mut s = store(dir.path());
                    s.put_object(b"").unwrap();
                    for index in order {
                        s.commit(events[index].clone(), false).unwrap();
                    }
                    let result = (s.projection(&s.active).unwrap(), s.conflicts().unwrap());
                    assert!(result.0.entries[&child.id].alive);
                    assert!(
                        result
                            .1
                            .iter()
                            .any(|c| c.entity == child.id && c.reason.contains("parent"))
                    );
                    if let Some(want) = &expected {
                        assert_eq!(&result, want, "order {order:?}");
                    } else {
                        expected = Some(result);
                    }
                }
            }
        }
    }
}
#[test]
fn private_shared_name_collision_and_concurrent_shared_names_do_not_block_main() {
    let adir = tempfile::tempdir().unwrap();
    let bdir = tempfile::tempdir().unwrap();
    let mut a = store(adir.path());
    let mut b = store(bdir.path());
    let private = b.fork("released", false).unwrap();
    let public = a.fork("released", true).unwrap();
    let af = a.create("main-file", "file").unwrap();
    a.write_revision(
        &af.id,
        af.revisions.get("content").cloned(),
        b"main-progress",
    )
    .unwrap();
    b.receive(
        a.shared_bundle(&BTreeSet::new()).unwrap(),
        &BTreeSet::from([a.device.clone()]),
    )
    .unwrap();
    assert!(b.branch("released").is_err());
    assert!(!b.branch(&private.id).unwrap().shared);
    assert!(b.branch(&public.id).unwrap().shared);
    assert_eq!(
        b.read_object(&b.lookup("main-file").unwrap().content.unwrap())
            .unwrap(),
        b"main-progress"
    );
    assert!(
        !serde_json::to_string(&b.shared_bundle(&BTreeSet::new()).unwrap())
            .unwrap()
            .contains(&private.id)
    );
    let x = a.fork("same-public-name", true).unwrap();
    let y = b.fork("same-public-name", true).unwrap();
    a.receive(
        b.shared_bundle(&BTreeSet::new()).unwrap(),
        &BTreeSet::from([a.device.clone(), b.device.clone()]),
    )
    .unwrap();
    b.receive(
        a.shared_bundle(&BTreeSet::new()).unwrap(),
        &BTreeSet::from([a.device.clone(), b.device.clone()]),
    )
    .unwrap();
    assert!(a.branch("same-public-name").is_err());
    assert!(a.branch(&x.id).is_ok() && a.branch(&y.id).is_ok());
    assert!(b.branch(&x.id).is_ok() && b.branch(&y.id).is_ok());
}
#[test]
fn quarantined_branch_descriptor_error_does_not_stop_unrelated_main_event() {
    let adir = tempfile::tempdir().unwrap();
    let bdir = tempfile::tempdir().unwrap();
    let mut a = store(adir.path());
    let mut b = store(bdir.path());
    let f = a.create("ok", "file").unwrap();
    let mut bundle = a.shared_bundle(&BTreeSet::new()).unwrap();
    let mut bad = bundle.events[0].clone();
    bad.id = "00-invalid".into();
    bad.branch.name = "wrong-main-descriptor".into();
    bundle.events.insert(0, bad);
    let ack = b
        .receive(bundle, &BTreeSet::from([a.device.clone()]))
        .unwrap();
    assert!(ack.contains(&f.revisions["content"]));
    assert!(b.lookup("ok").is_ok());
    let count: i64 =
        b.db.query_row(
            "SELECT count(*) FROM incoming_events WHERE error IS NOT NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
}
