use serde_json::json;
use std::collections::BTreeSet;
use tkfs::{core::*, runtime::Engine};

#[test]
fn metadata_durable_noop_and_dirty_content_are_independent() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = Engine::new(Store::open(dir.path(), Some("basic-tests")).unwrap());
    let (h, file) = e.open("file", Some("file"), true).unwrap();
    e.write(h, 0, b"unpublished", false, false).unwrap();
    let times = [
        132_000_000_000_000_001,
        132_000_000_000_000_002,
        132_000_000_000_000_003,
        132_000_000_000_000_004,
    ];
    e.set_basic(h, 2, times).unwrap();
    let entry = e.store.lookup("file").unwrap();
    assert_eq!(
        e.store
            .read_object(entry.content.as_ref().unwrap())
            .unwrap(),
        b""
    );
    assert_eq!(entry.basic_info().attributes, 2);
    let count = e.store.events().unwrap().len();
    e.set_basic(h, 2, times).unwrap();
    e.set_basic(h, u32::MAX, [0; 4]).unwrap();
    assert_eq!(e.store.events().unwrap().len(), count);
    e.flush(h).unwrap();
    assert_eq!(
        e.store.lookup("file").unwrap().basic_info().write_time,
        times[2]
    );
    assert_eq!(
        e.store.lookup("file").unwrap().basic_info().change_time,
        times[3]
    );
    e.close(h).unwrap();
    drop(e);
    let s = Store::open(dir.path(), Some("basic-tests")).unwrap();
    let entry = s.lookup("file").unwrap();
    assert_eq!(entry.id, file.id);
    assert_eq!(entry.basic_info().creation_time, times[0]);
    assert_eq!(entry.basic_info().access_time, times[1]);
    assert_eq!(entry.basic_info().write_time, times[2]);
    assert_eq!(
        s.read_object(entry.content.as_ref().unwrap()).unwrap(),
        b"unpublished"
    );
    assert_eq!(
        s.projection(&s.active).unwrap(),
        project(&s.active, &s.events().unwrap()).unwrap()
    );
}

#[test]
fn create_flags_readonly_and_time_sentinels() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = Engine::new(Store::open(dir.path(), None).unwrap());
    let (h, _) = e
        .open_with_attributes("file", Some("file"), true, Some(3))
        .unwrap();
    e.write(h, 0, b"initial", false, false).unwrap();
    e.flush(h).unwrap();
    assert!(e.open("file", None, true).is_err());
    e.set_basic(h, 0x80, [0, 0, 1, 2]).unwrap();
    let count = e.store.events().unwrap().len();
    e.set_basic(h, u32::MAX, [0, u64::MAX, u64::MAX, u64::MAX])
        .unwrap();
    assert_eq!(e.store.events().unwrap().len(), count);
    e.write(h, 0, b"second", false, false).unwrap();
    e.flush(h).unwrap();
    let basic = e.store.lookup("file").unwrap().basic_info();
    assert_eq!((basic.write_time, basic.change_time), (1, 2));
    e.set_basic(h, u32::MAX, [0, u64::MAX - 1, u64::MAX - 1, u64::MAX - 1])
        .unwrap();
    e.write(h, 0, b"third", false, false).unwrap();
    e.flush(h).unwrap();
    assert!(e.store.lookup("file").unwrap().basic_info().write_time > 2);
    assert!(
        e.set_basic(h, 0x400, [0; 4])
            .unwrap_err()
            .to_string()
            .contains("UNSUPPORTED")
    );
    assert!(e.set_basic(h, u32::MAX, [u64::MAX, 0, 0, 0]).is_err());
    let (dh, _) = e
        .open_with_attributes("dir", Some("directory"), false, Some(2))
        .unwrap();
    e.set_basic(dh, 0x80, [0; 4]).unwrap();
    assert_eq!(e.store.lookup("dir").unwrap().basic_info().attributes, 0x10);
}

#[test]
fn metadata_survives_fork_restore_without_private_replication() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Store::open(dir.path(), Some("basic-tests")).unwrap();
    let file = s.create_with_attributes("file", "file", Some(6)).unwrap();
    assert_eq!(file.basic_info().attributes, 6);
    let mut original = file.basic_info();
    original.attributes = 2;
    original.write_time = 42;
    s.set_basic(&file.id, original).unwrap();
    s.rename(&file.id, "renamed", false).unwrap();
    assert_eq!(s.lookup("renamed").unwrap().basic_info(), original);
    let checkpoint = s.checkpoint("metadata").unwrap();
    let private = s.fork("private", false).unwrap();
    s.checkout(&private.id).unwrap();
    assert_eq!(s.lookup("renamed").unwrap().basic_info(), original);
    let mut changed = original;
    changed.attributes = 1;
    s.set_basic(&file.id, changed).unwrap();
    assert!(
        s.shared_bundle(&BTreeSet::new())
            .unwrap()
            .events
            .iter()
            .all(|e| e.branch.shared)
    );
    s.checkout("main").unwrap();
    assert_eq!(s.lookup("renamed").unwrap().basic_info(), original);
    let restored = s.restore(&checkpoint, "restored").unwrap();
    s.checkout(&restored.id).unwrap();
    assert_eq!(s.lookup("renamed").unwrap().basic_info(), original);
    drop(s);
    let s = Store::open(dir.path(), Some("basic-tests")).unwrap();
    assert_eq!(s.lookup("renamed").unwrap().basic_info(), original);
}

#[test]
fn concurrent_metadata_converges_and_remains_reviewable() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Store::open(dir.path(), None).unwrap();
    let file = s.create("file", "file").unwrap();
    let seed = s.events().unwrap();
    let branch = s.branch("main").unwrap();
    let mut events = vec![];
    for (device, attr) in [("a", 2), ("b", 1)] {
        let mut basic = file.basic_info();
        basic.attributes = attr;
        let mut event = s.event(
            &branch,
            vec![Change {
                entity: file.id.clone(),
                field: "basic".into(),
                parents: vec![],
                value: json!(basic),
            }],
        );
        event.device = device.into();
        events.push(event);
    }
    let mut forward = seed.clone();
    forward.extend(events.clone());
    let mut reverse = seed;
    reverse.extend(events.iter().rev().cloned());
    assert_eq!(
        project(&branch.id, &forward).unwrap(),
        project(&branch.id, &reverse).unwrap()
    );
    for event in &events {
        s.commit(event.clone(), true).unwrap();
    }
    let conflicts: Vec<_> = s
        .conflicts()
        .unwrap()
        .iter()
        .filter(|c| c.field == "basic")
        .map(|c| c.id.clone())
        .collect();
    assert_eq!(conflicts.len(), 1);
    s.resolve(&conflicts, &events[0].id).unwrap();
    assert_eq!(s.lookup("file").unwrap().basic_info().attributes, 2);
    assert_eq!(
        s.projection(&s.active).unwrap(),
        project(&s.active, &s.events().unwrap()).unwrap()
    );
}
