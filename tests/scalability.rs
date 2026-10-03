use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};
use tkfs::{core::*, runtime::Engine};
fn store(path: &std::path::Path) -> Store {
    Store::open(path, Some("scalability")).unwrap()
}
fn save(engine: &mut Engine, handle: u64, bytes: &[u8]) {
    engine.truncate(handle, 0, false).unwrap();
    engine.write(handle, 0, bytes, false, false).unwrap();
    engine.flush(handle).unwrap();
}
#[test]
fn incremental_save_matches_full_projection_when_clock_moves_backward() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let branch = s.branch(&s.active).unwrap();
    let mut seed = s.event(
        &branch,
        vec![
            Change {
                entity: "file".into(),
                field: "kind".into(),
                parents: vec![],
                value: json!("file"),
            },
            Change {
                entity: "file".into(),
                field: "location".into(),
                parents: vec![],
                value: json!({"parent":ROOT,"name":"code"}),
            },
            Change {
                entity: "file".into(),
                field: "alive".into(),
                parents: vec![],
                value: json!(true),
            },
            Change {
                entity: "file".into(),
                field: "content".into(),
                parents: vec![],
                value: json!(s.put_object(b"seed").unwrap()),
            },
        ],
    );
    seed.created_ms = 0;
    s.commit(seed, true).unwrap();
    for (clock, bytes) in [(1000, b"forward".as_slice()), (50, b"backward".as_slice())] {
        let mut event = s.event(
            &branch,
            vec![
                s.current_change("file", "content", json!(s.put_object(bytes).unwrap()))
                    .unwrap(),
            ],
        );
        event.created_ms = clock;
        s.commit(event, true).unwrap();
        assert_eq!(
            s.projection(&branch.id).unwrap(),
            project(&branch.id, &s.events().unwrap()).unwrap()
        );
    }
    assert_eq!(s.lookup("code").unwrap().modified_ms, 50);
    assert_eq!(s.lookup("code").unwrap().content, Some(hash(b"backward")));
}
#[test]
fn ten_thousand_durable_content_saves_and_later_delete_undelete() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let file = s.create("code", "file").unwrap();
    let mut engine = Engine::new(s);
    let (handle, _) = engine.open("code", None, true).unwrap();
    let start = Instant::now();
    let mut first_thousand = 0;
    let mut last_start = Instant::now();
    for i in 0..10_000 {
        if i == 9000 {
            last_start = Instant::now();
        }
        save(&mut engine, handle, format!("{i:05}:body").as_bytes());
        if i == 999 {
            first_thousand = start.elapsed().as_millis();
        }
        if (i + 1) % 2500 == 0 {
            println!(
                "sequential durable saves: {} in {} ms",
                i + 1,
                start.elapsed().as_millis()
            );
        }
    }
    let last_thousand = last_start.elapsed().as_millis();
    let saves = start.elapsed().as_millis();
    engine.close(handle).unwrap();
    assert_eq!(
        engine.store.projection(&engine.store.active).unwrap(),
        project(&engine.store.active, &engine.store.events().unwrap()).unwrap()
    );
    assert_eq!(
        engine.store.lookup("code").unwrap().content,
        Some(hash(b"09999:body"))
    );
    assert_eq!(
        engine
            .store
            .db
            .query_row("SELECT count(*) FROM events", [], |r| r.get::<_, u64>(0))
            .unwrap(),
        10_001
    );
    assert!(engine.store.conflicts().unwrap().is_empty());
    let namespace_start = Instant::now();
    engine.store.delete(&file.id).unwrap();
    let deleted = engine
        .store
        .projection(&engine.store.active)
        .unwrap()
        .entries[&file.id]
        .clone();
    assert!(engine.store.lookup("code").is_err());
    engine
        .store
        .write_with_base(
            &file.id,
            deleted.revisions.get("content").cloned(),
            deleted.revisions.get("alive").cloned(),
            b"explicit-undelete",
        )
        .unwrap();
    assert_eq!(
        engine.store.lookup("code").unwrap().content,
        Some(hash(b"explicit-undelete"))
    );
    let namespace_ms = namespace_start.elapsed().as_millis();
    drop(engine);
    let restart = Instant::now();
    let s = store(dir.path());
    let restart_ms = restart.elapsed().as_millis();
    assert_eq!(
        s.read_object(&s.lookup("code").unwrap().content.unwrap())
            .unwrap(),
        b"explicit-undelete"
    );
    assert_eq!(s.read_object(&hash(b"00000:body")).unwrap(), b"00000:body");
    assert!(
        s.events()
            .unwrap()
            .iter()
            .all(|e| e.dependencies.len() <= 1)
    );
    assert_eq!(s.status().unwrap()["incoming_quarantined"], 0);
    println!(
        "SCALABILITY_JSON {}",
        json!({"case":"10000-durable-content-saves","profile":if cfg!(debug_assertions){"debug"}else{"release"},"saves":10000,"saves_ms":saves,"first_1000_ms":first_thousand,"last_1000_ms":last_thousand,"delete_undelete_ms":namespace_ms,"restart_ms":restart_ms,"passed":true,"real_mount":false})
    );
}
#[test]
fn two_replicas_each_make_one_hundred_offline_saves_and_retain_all_pairs() {
    let adir = tempfile::tempdir().unwrap();
    let bdir = tempfile::tempdir().unwrap();
    let mut a = store(adir.path());
    let mut b = store(bdir.path());
    a.create("code", "file").unwrap();
    b.receive(
        a.shared_bundle(&BTreeSet::new()).unwrap(),
        &BTreeSet::from([a.device.clone()]),
    )
    .unwrap();
    let allowed = BTreeSet::from([a.device.clone(), b.device.clone()]);
    let mut a = Engine::new(a);
    let mut b = Engine::new(b);
    let (ah, _) = a.open("code", None, true).unwrap();
    let (bh, _) = b.open("code", None, true).unwrap();
    let start = Instant::now();
    for i in 0..100 {
        save(&mut a, ah, format!("A{i:03}").as_bytes());
        save(&mut b, bh, format!("B{i:03}").as_bytes());
    }
    a.close(ah).unwrap();
    b.close(bh).unwrap();
    let saves_ms = start.elapsed().as_millis();
    let ab = a.store.shared_bundle(&BTreeSet::new()).unwrap();
    let ba = b.store.shared_bundle(&BTreeSet::new()).unwrap();
    let sync = Instant::now();
    a.receive_peer(ba, &allowed).unwrap();
    b.receive_peer(ab, &allowed).unwrap();
    let sync_ms = sync.elapsed().as_millis();
    assert_eq!(
        a.store.projection(&a.store.active).unwrap(),
        b.store.projection(&b.store.active).unwrap()
    );
    assert_eq!(a.store.conflicts().unwrap(), b.store.conflicts().unwrap());
    let conflicts = a.store.conflicts().unwrap();
    assert_eq!(
        conflicts.iter().filter(|c| c.field == "content").count(),
        10_000
    );
    assert!(conflicts.iter().all(|c| c.field == "content"));
    for i in 0..100 {
        for prefix in ["A", "B"] {
            let bytes = format!("{prefix}{i:03}");
            assert_eq!(
                a.store.read_object(&hash(bytes.as_bytes())).unwrap(),
                bytes.as_bytes()
            );
        }
    }
    assert_eq!(a.store.status().unwrap()["incoming_pending"], 0);
    assert_eq!(b.store.status().unwrap()["incoming_pending"], 0);
    drop(a);
    drop(b);
    let restart = Instant::now();
    let a = store(adir.path());
    let b = store(bdir.path());
    assert_eq!(a.conflicts().unwrap(), b.conflicts().unwrap());
    assert_eq!(
        a.projection(&a.active).unwrap(),
        b.projection(&b.active).unwrap()
    );
    println!(
        "SCALABILITY_JSON {}",
        json!({"case":"two-replicas-100-offline-saves-each","profile":if cfg!(debug_assertions){"debug"}else{"release"},"saves_per_replica":100,"historical_content_pairs":10000,"saves_ms":saves_ms,"reconnect_ms":sync_ms,"two_restarts_ms":restart.elapsed().as_millis(),"passed":true,"real_two_computers":false,"processes":1})
    );
}
#[test]
fn unrelated_namespace_chains_do_not_multiply_the_topology_budget() {
    let adir = tempfile::tempdir().unwrap();
    let bdir = tempfile::tempdir().unwrap();
    let mut a = store(adir.path());
    let mut b = store(bdir.path());
    a.create("project", "directory").unwrap();
    let af = a.create("project/A", "file").unwrap();
    let bf = a.create("project/B", "file").unwrap();
    b.receive(
        a.shared_bundle(&BTreeSet::new()).unwrap(),
        &BTreeSet::from([a.device.clone()]),
    )
    .unwrap();
    let allowed = BTreeSet::from([a.device.clone(), b.device.clone()]);
    let start = Instant::now();
    for i in 0..65 {
        a.rename(&af.id, &format!("project/A-{i}"), false).unwrap();
        b.rename(&bf.id, &format!("project/B-{i}"), false).unwrap();
    }
    let ab = a.shared_bundle(&BTreeSet::new()).unwrap();
    let ba = b.shared_bundle(&BTreeSet::new()).unwrap();
    a.receive(ba, &allowed).unwrap();
    b.receive(ab, &allowed).unwrap();
    assert_eq!(
        a.projection(&a.active).unwrap(),
        b.projection(&b.active).unwrap()
    );
    assert!(a.lookup("project/A-64").is_ok() && a.lookup("project/B-64").is_ok());
    assert!(a.conflicts().unwrap().is_empty());
    println!(
        "SCALABILITY_JSON {}",
        json!({"case":"unrelated-65-rename-chains","profile":if cfg!(debug_assertions){"debug"}else{"release"},"renames_per_replica":65,"total_ms":start.elapsed().as_millis(),"passed":true,"real_two_computers":false})
    );
}
#[test]
fn compressed_presence_witnesses_match_exhaustive_causal_cuts() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = store(dir.path());
    let branch = s.branch(&s.active).unwrap();
    let change = |entity: &str, field: &str, parents: Vec<String>, value| Change {
        entity: entity.into(),
        field: field.into(),
        parents,
        value,
    };
    let mut seed = s.event(
        &branch,
        vec![
            change("dir", "kind", vec![], json!("directory")),
            change(
                "dir",
                "location",
                vec![],
                json!({"parent":ROOT,"name":"dir"}),
            ),
            change("dir", "alive", vec![], json!(true)),
            change("file", "kind", vec![], json!("file")),
            change(
                "file",
                "location",
                vec![],
                json!({"parent":"dir","name":"file"}),
            ),
            change("file", "alive", vec![], json!(true)),
            change(
                "file",
                "content",
                vec![],
                json!(s.put_object(b"seed").unwrap()),
            ),
        ],
    );
    seed.id = "seed".into();
    seed.device = "peer".into();
    let mut events = vec![seed.clone()];
    let mut delete = s.event(
        &branch,
        vec![change("file", "alive", vec![seed.id.clone()], json!(false))],
    );
    delete.id = "rev-27".into();
    delete.device = "peer".into();
    delete.dependencies = vec![seed.id.clone()];
    events.push(delete.clone());
    let mut parent_delete = delete.clone();
    parent_delete.id = "parent-delete".into();
    parent_delete.changes[0].entity = "dir".into();
    events.push(parent_delete);
    let mut base = seed.id.clone();
    for id in ["rev-30", "rev-10", "rev-40", "rev-20", "rev-25"] {
        let mut e = s.event(
            &branch,
            vec![
                change("file", "alive", vec![base.clone()], json!(true)),
                change(
                    "file",
                    "content",
                    vec![base.clone()],
                    json!(s.put_object(id.as_bytes()).unwrap()),
                ),
            ],
        );
        e.id = id.into();
        e.device = "peer".into();
        e.dependencies = vec![delete.id.clone(), base.clone()];
        base = e.id.clone();
        events.push(e);
    }
    let mut correction = s.event(
        &branch,
        vec![change(
            "file",
            "location",
            vec![seed.id.clone()],
            json!({"parent":ROOT,"name":"file"}),
        )],
    );
    correction.id = "correction".into();
    correction.dependencies = vec![base];
    events.push(correction);
    let mut expected = BTreeMap::new();
    for mask in 0..(1usize << events.len()) {
        let cut: Vec<_> = events
            .iter()
            .enumerate()
            .filter(|(i, _)| mask & (1 << i) != 0)
            .map(|(_, e)| e.clone())
            .collect();
        let ids: BTreeSet<_> = cut.iter().map(|e| &e.id).collect();
        if !cut.iter().all(|e| {
            e.dependencies
                .iter()
                .chain(e.changes.iter().flat_map(|c| c.parents.iter()))
                .all(|p| ids.contains(p))
        }) {
            continue;
        }
        for c in project(&branch.id, &cut)
            .unwrap()
            .conflicts
            .into_iter()
            .filter(|c| c.field == "namespace")
        {
            expected.insert(c.id.clone(), c);
        }
    }
    for e in &events {
        s.commit(e.clone(), false).unwrap();
    }
    let actual: BTreeMap<_, _> = s
        .conflicts()
        .unwrap()
        .into_iter()
        .filter(|c| c.field == "namespace")
        .map(|c| (c.id.clone(), c))
        .collect();
    assert_eq!(actual, expected);
    assert!(!actual.is_empty());
    let alive: Vec<_> = s
        .conflicts()
        .unwrap()
        .into_iter()
        .filter(|c| c.field == "alive")
        .map(|c| c.id)
        .collect();
    assert_eq!(alive.len(), 5);
    s.resolve(&alive, "rev-25").unwrap();
    assert!(s.lookup("file").is_ok());
    let receiver = tempfile::tempdir().unwrap();
    let mut peer = store(receiver.path());
    for event in s.events().unwrap().into_iter().rev() {
        for object in event.objects() {
            peer.put_object(&s.read_object(&object).unwrap()).unwrap();
        }
        peer.commit(event, false).unwrap();
    }
    assert_eq!(peer.conflicts().unwrap(), s.conflicts().unwrap());
    assert_eq!(
        peer.projection(&peer.active).unwrap(),
        s.projection(&s.active).unwrap()
    );
    drop(s);
    let s = store(dir.path());
    let retained: BTreeMap<_, _> = s
        .conflicts()
        .unwrap()
        .into_iter()
        .filter(|c| c.field == "namespace")
        .map(|c| (c.id.clone(), c))
        .collect();
    assert_eq!(retained, expected);
}
