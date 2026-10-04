use std::collections::BTreeSet;
use tkfs::{core::*, runtime::Engine};

#[test]
fn posix_mode_survives_windows_style_metadata_edit_fork_restore_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Store::open(dir.path(), None).unwrap();
    let file = s.create("script.sh", "file").unwrap();
    let mut basic = file.basic_info();
    basic.posix_mode = Some(0o755);
    s.set_basic(&file.id, basic).unwrap();
    let mut engine = Engine::new(s);
    let (handle, _) = engine.open("script.sh", None, true).unwrap();
    engine.set_basic(handle, 2, [0; 4]).unwrap();
    engine
        .write(handle, 0, b"#!/bin/sh\n", false, false)
        .unwrap();
    engine.close(handle).unwrap();
    assert_eq!(
        engine
            .store
            .lookup("script.sh")
            .unwrap()
            .basic_info()
            .posix_mode,
        Some(0o755)
    );
    let checkpoint = engine.store.checkpoint("permissions").unwrap();
    let private = engine.store.fork("private", false).unwrap();
    engine.store.checkout(&private.id).unwrap();
    let mut private_basic = engine.store.lookup("script.sh").unwrap().basic_info();
    private_basic.posix_mode = Some(0o700);
    engine.store.set_basic(&file.id, private_basic).unwrap();
    let shared = engine.store.shared_bundle(&BTreeSet::new()).unwrap();
    assert!(shared.events.iter().all(|event| event.branch.shared));
    assert!(
        shared
            .events
            .iter()
            .flat_map(|event| &event.changes)
            .all(|change| change.value["posix_mode"] != 0o700)
    );
    engine.store.checkout("main").unwrap();
    assert_eq!(
        engine
            .store
            .lookup("script.sh")
            .unwrap()
            .basic_info()
            .posix_mode,
        Some(0o755)
    );
    let restored = engine.store.restore(&checkpoint, "restored").unwrap();
    engine.store.checkout(&restored.id).unwrap();
    drop(engine);
    let s = Store::open_existing(dir.path()).unwrap();
    assert_eq!(
        s.lookup("script.sh").unwrap().basic_info().posix_mode,
        Some(0o755)
    );
    let mut invalid = s.lookup("script.sh").unwrap().basic_info();
    invalid.posix_mode = Some(0o4755);
    assert!(invalid.validate().is_err());
}

#[test]
fn competing_execute_modes_keep_causal_conflicts_and_order_independence() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Store::open(dir.path(), None).unwrap();
    let file = s.create("script", "file").unwrap();
    let branch = s.branch("main").unwrap();
    let seed = s.events().unwrap();
    let mut revisions = vec![];
    for (device, mode) in [("device-a", 0o644), ("device-b", 0o755)] {
        let mut basic = file.basic_info();
        basic.posix_mode = Some(mode);
        let mut event = s.event(
            &branch,
            vec![Change {
                entity: file.id.clone(),
                field: "basic".into(),
                parents: vec![],
                value: serde_json::json!(basic),
            }],
        );
        event.device = device.into();
        revisions.push(event);
    }
    let mut forward = seed.clone();
    forward.extend(revisions.clone());
    let mut reverse = seed;
    reverse.extend(revisions.iter().rev().cloned());
    assert_eq!(
        project(&branch.id, &forward).unwrap(),
        project(&branch.id, &reverse).unwrap()
    );
    for event in &revisions {
        s.commit(event.clone(), true).unwrap();
    }
    let conflicts: Vec<_> = s
        .conflicts()
        .unwrap()
        .into_iter()
        .filter(|conflict| conflict.field == "basic")
        .map(|conflict| conflict.id)
        .collect();
    assert_eq!(conflicts.len(), 1);
    s.resolve(&conflicts, &revisions[1].id).unwrap();
    assert_eq!(
        s.lookup("script").unwrap().basic_info().posix_mode,
        Some(0o755)
    );
}

#[test]
fn failed_close_retains_original_base_and_recovers_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::new(Store::open(dir.path(), None).unwrap());
    let (handle, entry) = engine.open("work", Some("file"), true).unwrap();
    let base = engine.sessions[&entry.id].content_base.clone();
    let bytes = b"failed-save recovery";
    engine.write(handle, 0, bytes, false, false).unwrap();
    let destination = dir.path().join("objects").join(hash(bytes));
    std::fs::write(&destination, b"corrupt").unwrap();
    assert!(engine.close(handle).is_err());
    assert!(engine.handles.is_empty());
    drop(engine);
    let mut recovered = Engine::new(Store::open_existing(dir.path()).unwrap());
    assert_eq!(recovered.sessions[&entry.id].content_base, base);
    assert_eq!(recovered.sessions[&entry.id].bytes, bytes);
    assert!(recovered.quiet().is_err());
    std::fs::write(&destination, bytes).unwrap();
    recovered.retry_pending().unwrap();
    recovered.quiet().unwrap();
    assert_eq!(
        recovered
            .store
            .read_object(&recovered.store.lookup("work").unwrap().content.unwrap())
            .unwrap(),
        bytes
    );
}
