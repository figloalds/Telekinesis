use serde_json::json;
#[cfg(windows)]
use std::fs::OpenOptions;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;
use std::{
    collections::BTreeSet,
    fs,
    io::{Read, Seek, SeekFrom},
};
use tkfs::{
    backend::{DirectoryBackend, ObjectBackend},
    core::*,
    runtime::Engine,
    staging::Staged,
};

#[cfg(windows)]
#[test]
fn failed_close_retires_context_and_recovery_preserves_original_bases_and_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::new(Store::open(dir.path(), Some("availability")).unwrap());
    let (handle, entry) = engine.open("work", Some("file"), true).unwrap();
    let base = engine.session(handle).unwrap().content_base.clone();
    let bytes = b"unsaved contents must survive close and restart";
    engine.write(handle, 0, bytes, false, false).unwrap();
    let object = dir.path().join("objects").join(hash(bytes));
    fs::write(&object, bytes).unwrap();
    let lock = OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(&object)
        .unwrap();
    assert!(engine.cleanup(handle, false).is_err());
    assert!(engine.close(handle).is_err());
    assert!(engine.handles.is_empty());
    assert!(engine.sessions[&entry.id].dirty);
    assert_eq!(engine.sessions[&entry.id].content_base, base);
    assert_eq!(engine.sessions[&entry.id].bytes, bytes);
    assert!(engine.quiet().is_err());
    assert_eq!(
        engine.control("status", &json!({"op":"status"})).unwrap()["open_handles"],
        0
    );
    assert!(engine.health.as_ref().unwrap().contains("os error 32"));
    drop(engine);
    drop(lock);
    let mut recovered = Engine::new(Store::open_existing(dir.path()).unwrap());
    assert!(recovered.health.is_some());
    assert_eq!(recovered.sessions[&entry.id].content_base, base);
    assert_eq!(recovered.sessions[&entry.id].bytes, bytes);
    recovered.retry_pending().unwrap();
    assert!(recovered.health.is_none());
    recovered.quiet().unwrap();
    assert_eq!(
        recovered
            .store
            .read_object(&recovered.store.lookup("work").unwrap().content.unwrap())
            .unwrap(),
        bytes
    );
    assert_eq!(
        recovered
            .store
            .db
            .query_row("SELECT count(*) FROM pending_saves", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn permanent_failed_save_stays_degraded_and_retains_staging() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::new(Store::open(dir.path(), Some("availability")).unwrap());
    let (handle, entry) = engine.open("work", Some("file"), true).unwrap();
    engine.write(handle, 0, b"wanted", false, false).unwrap();
    fs::write(dir.path().join("objects").join(hash(b"wanted")), b"corrupt").unwrap();
    assert!(engine.close(handle).is_err());
    assert!(engine.retry_pending().is_err());
    assert!(engine.health.as_ref().unwrap().contains("CORRUPT_OBJECT"));
    assert_eq!(engine.sessions[&entry.id].bytes, b"wanted");
    let (other, _) = engine.open("unrelated", Some("file"), true).unwrap();
    engine.write(other, 0, b"other", false, false).unwrap();
    engine.close(other).unwrap();
    assert!(engine.health.is_some());
    assert!(engine.quiet().is_err());
}
#[cfg(windows)]
#[test]
fn volume_flush_retires_recovered_closed_sessions_before_checkout() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::new(Store::open(dir.path(), Some("availability")).unwrap());
    let (handle, _) = engine.open("work", Some("file"), true).unwrap();
    engine.write(handle, 0, b"retained", false, false).unwrap();
    let object = dir.path().join("objects").join(hash(b"retained"));
    fs::write(&object, b"retained").unwrap();
    let lock = OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(&object)
        .unwrap();
    assert!(engine.close(handle).is_err());
    drop(lock);
    engine.flush(0).unwrap();
    assert!(engine.sessions.is_empty());
    engine.quiet().unwrap();
    let branch = engine.store.fork("other", false).unwrap();
    engine.store.checkout(&branch.id).unwrap();
    let entry = engine.store.lookup("work").unwrap();
    engine
        .store
        .write_revision(
            &entry.id,
            entry.revisions.get("content").cloned(),
            b"other-view",
        )
        .unwrap();
    let (handle, _) = engine.open("work", None, false).unwrap();
    assert_eq!(engine.session(handle).unwrap().bytes, b"other-view");
    engine.close(handle).unwrap();
}

#[test]
fn large_random_access_file_is_disk_staged_durable_and_exportable() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::new(Store::open(dir.path(), Some("availability")).unwrap());
    let (handle, _) = engine.open("large", Some("file"), true).unwrap();
    let block = vec![0x61; 1024 * 1024];
    for i in 0..20 {
        engine
            .write(handle, i * block.len() as u64, &block, false, false)
            .unwrap();
    }
    assert!(matches!(
        engine.session(handle).unwrap().bytes,
        Staged::Disk { .. }
    ));
    engine
        .write(handle, 5 * 1024 * 1024, b"patched", false, false)
        .unwrap();
    engine.truncate(handle, 18 * 1024 * 1024, false).unwrap();
    engine
        .write(handle, 19 * 1024 * 1024, b"tail", false, false)
        .unwrap();
    engine.close(handle).unwrap();
    engine.quiet().unwrap();
    drop(engine);
    let mut store = Store::open_existing(dir.path()).unwrap();
    let object = store.lookup("large").unwrap().content.unwrap();
    assert!(
        store
            .read_object(&object)
            .unwrap_err()
            .to_string()
            .contains("OBJECT_REQUIRES_STREAMING")
    );
    let mut file = store.open_object(&object).unwrap();
    assert_eq!(file.metadata().unwrap().len(), 19 * 1024 * 1024 + 4);
    let mut bytes = [0; 7];
    file.seek(SeekFrom::Start(5 * 1024 * 1024)).unwrap();
    file.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"patched");
    file.seek(SeekFrom::Start(18 * 1024 * 1024)).unwrap();
    file.read_exact(&mut bytes).unwrap();
    assert_eq!(bytes, [0; 7]);
    let checkpoint = store.checkpoint("large preserved").unwrap();
    let restored = store.restore(&checkpoint, "restored").unwrap();
    store.checkout(&restored.id).unwrap();
    assert_eq!(store.lookup("large").unwrap().content, Some(object.clone()));
    let bucket = tempfile::tempdir().unwrap();
    let backend = DirectoryBackend::new(bucket.path()).unwrap();
    store.export_shared_objects(&backend).unwrap();
    assert_eq!(
        fs::metadata(bucket.path().join(&object)).unwrap().len(),
        19 * 1024 * 1024 + 4
    );
    assert!(backend.get_verified(&object).is_err());
}

#[test]
fn large_peer_segments_survive_restart_and_never_ack_partial_or_private_content() {
    let a_dir = tempfile::tempdir().unwrap();
    let b_dir = tempfile::tempdir().unwrap();
    let mut a = Engine::new(Store::open(a_dir.path(), Some("availability")).unwrap());
    let (handle, _) = a.open("large", Some("file"), true).unwrap();
    let block = vec![42; 1024 * 1024];
    for i in 0..18 {
        a.write(handle, i * 1024 * 1024, &block, false, false)
            .unwrap();
    }
    a.close(handle).unwrap();
    let mut b = Store::open(b_dir.path(), Some("availability")).unwrap();
    let allowed = BTreeSet::from([a.store.device.clone()]);
    let object = a.store.lookup("large").unwrap().content.unwrap();
    let mut pages = 0;
    while b.lookup("large").is_err() || b.lookup("large").unwrap().content != Some(object.clone()) {
        pages += 1;
        assert!(pages < 30);
        let page = a
            .store
            .shared_page(
                &b.shared_event_inventory().unwrap(),
                &b.shared_object_inventory().unwrap(),
                3 * 1024 * 1024,
            )
            .unwrap();
        let parts = a
            .store
            .shared_parts(
                &page,
                &b.shared_object_inventory().unwrap(),
                &b.partial_objects().unwrap(),
                3 * 1024 * 1024,
            )
            .unwrap();
        b.receive_parts(parts.clone(), &page.events, &allowed)
            .unwrap();
        b.receive_parts(parts, &page.events, &allowed).unwrap(); // reply-loss replay
        let accepted = b.receive(page, &allowed).unwrap();
        if !b.shared_object_inventory().unwrap().contains(&object) {
            let revision = a
                .store
                .events()
                .unwrap()
                .into_iter()
                .find(|e| e.objects().contains(&object))
                .unwrap()
                .id;
            assert!(!accepted.contains(&revision));
            drop(b);
            b = Store::open_existing(b_dir.path()).unwrap();
        }
    }
    assert!(pages > 1);
    b.verify_object(&object).unwrap();
    assert!(b.partial_objects().unwrap().is_empty());
    // A hash-only page never authorizes private-only CAS, including huge files.
    let private = a.store.fork("private", false).unwrap();
    a.store.checkout(&private.id).unwrap();
    let (h, _) = a.open("secret", Some("file"), true).unwrap();
    a.write(h, 0, b"PRIVATE-CANARY", false, false).unwrap();
    a.close(h).unwrap();
    let secret = a.store.lookup("secret").unwrap().content.unwrap();
    assert!(!a.store.shared_object_inventory().unwrap().contains(&secret));
    assert!(
        a.store
            .receive_parts(
                std::collections::BTreeMap::from([(
                    secret,
                    ObjectPart {
                        offset: 0,
                        total: 14,
                        hex: hex::encode(b"PRIVATE-CANARY")
                    }
                )]),
                &[],
                &allowed
            )
            .is_err()
    );
}

#[cfg(windows)]
#[test]
fn pending_save_keeps_its_base_when_a_peer_revision_arrives() {
    let a_dir = tempfile::tempdir().unwrap();
    let b_dir = tempfile::tempdir().unwrap();
    let mut a = Engine::new(Store::open(a_dir.path(), Some("availability")).unwrap());
    let mut b = Store::open(b_dir.path(), Some("availability")).unwrap();
    let (h, entry) = a.open("work", Some("file"), true).unwrap();
    b.receive(
        a.store.shared_bundle(&BTreeSet::new()).unwrap(),
        &BTreeSet::from([a.store.device.clone()]),
    )
    .unwrap();
    let base = a.session(h).unwrap().content_base.clone();
    a.write(h, 0, b"local", false, false).unwrap();
    let path = a_dir.path().join("objects").join(hash(b"local"));
    fs::write(&path, b"local").unwrap();
    let lock = OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(&path)
        .unwrap();
    assert!(a.close(h).is_err());
    let remote = b.lookup("work").unwrap();
    b.write_revision(
        &remote.id,
        remote.revisions.get("content").cloned(),
        b"remote",
    )
    .unwrap();
    a.receive_peer(
        b.shared_bundle(&BTreeSet::new()).unwrap(),
        &BTreeSet::from([a.store.device.clone(), b.device.clone()]),
    )
    .unwrap();
    assert_eq!(a.sessions[&entry.id].content_base, base);
    drop(lock);
    a.retry_pending().unwrap();
    let conflicts = a.store.conflicts().unwrap();
    assert!(
        conflicts
            .iter()
            .any(|c| c.entity == entry.id && c.field == "content")
    );
    assert_eq!(a.store.read_object(&hash(b"local")).unwrap(), b"local");
    assert_eq!(a.store.read_object(&hash(b"remote")).unwrap(), b"remote");
}
#[test]
fn status_is_authenticated_and_responsive_while_engine_is_busy() {
    use std::sync::{Arc, Mutex};
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Mutex::new(Engine::new(
        Store::open(dir.path(), Some("availability")).unwrap(),
    )));
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let info = tkfs::runtime::start_rpc(engine.clone(), dir.path(), None).unwrap();
    let guard = engine.lock().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let client = std::thread::spawn(move || {
        let result = tkfs::runtime::rpc(&info, "status-busy", json!({"op":"status"}));
        let mut wrong = info;
        wrong.token = "wrong".into();
        assert!(
            tkfs::runtime::rpc(&wrong, "unauthorized", json!({"op":"status"}))
                .unwrap_err()
                .to_string()
                .contains("PERMISSION_DENIED")
        );
        tx.send(result).unwrap();
    });
    let status = rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap()
        .unwrap();
    assert_eq!(status["busy"], true);
    assert_eq!(status["stale"], true);
    assert_eq!(status["caught_up"], false);
    drop(guard);
    client.join().unwrap();
}
#[test]
fn sequential_creates_and_content_deltas_match_full_replay_and_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), Some("availability")).unwrap();
    store.create("folder", "directory").unwrap();
    for i in 0..100 {
        let entry = store
            .create_with_attributes(&format!("folder/file{i}"), "file", Some(0x20))
            .unwrap();
        store
            .write_with_base(
                &entry.id,
                entry.revisions.get("content").cloned(),
                entry.revisions.get("alive").cloned(),
                b"body",
            )
            .unwrap();
    }
    let expected = project(&store.active, &store.events().unwrap()).unwrap();
    assert_eq!(store.projection(&store.active).unwrap(), expected);
    let entry = store.lookup("folder/file0").unwrap();
    store.rename(&entry.id, "renamed", false).unwrap();
    assert_eq!(
        store.projection(&store.active).unwrap(),
        project(&store.active, &store.events().unwrap()).unwrap()
    );
    store.delete(&entry.id).unwrap();
    assert_eq!(
        store.projection(&store.active).unwrap(),
        project(&store.active, &store.events().unwrap()).unwrap()
    );
    let expected = store.projection(&store.active).unwrap();
    store.db.execute_batch("SAVEPOINT rollback_cache").unwrap();
    store.create("rolled-back", "file").unwrap();
    assert!(store.lookup("rolled-back").is_ok());
    store
        .db
        .execute_batch("ROLLBACK TO rollback_cache; RELEASE rollback_cache")
        .unwrap();
    assert!(store.lookup("rolled-back").is_err());
    assert_eq!(store.projection(&store.active).unwrap(), expected);
    drop(store);
    let store = Store::open_existing(dir.path()).unwrap();
    assert_eq!(store.projection(&store.active).unwrap(), expected);
}

#[test]
fn atomic_file_replacements_match_replay_both_slot_orders_and_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), Some("availability")).unwrap();
    let mut orders = BTreeSet::new();
    for i in 0..24 {
        let target = store.create(&format!("index{i}"), "file").unwrap();
        store
            .write_revision(
                &target.id,
                target.revisions.get("content").cloned(),
                b"old index",
            )
            .unwrap();
        let source = store.create(&format!("index{i}.lock"), "file").unwrap();
        store
            .write_revision(
                &source.id,
                source.revisions.get("content").cloned(),
                b"new index",
            )
            .unwrap();
        orders.insert(source.id < target.id);
        let before = store.projection(&store.active).unwrap();
        store
            .db
            .execute_batch("SAVEPOINT replacement_rollback")
            .unwrap();
        store
            .rename(&source.id, &format!("index{i}"), true)
            .unwrap();
        assert_eq!(store.lookup(&format!("index{i}")).unwrap().id, source.id);
        assert!(store.lookup(&format!("index{i}.lock")).is_err());
        assert_eq!(
            store.projection(&store.active).unwrap(),
            project(&store.active, &store.events().unwrap()).unwrap()
        );
        store
            .db
            .execute_batch("ROLLBACK TO replacement_rollback; RELEASE replacement_rollback")
            .unwrap();
        assert_eq!(store.projection(&store.active).unwrap(), before);
        assert_eq!(store.lookup(&format!("index{i}")).unwrap().id, target.id);
        store
            .rename(&source.id, &format!("index{i}"), true)
            .unwrap();
        // Both new and superseded bytes remain immutable and available.
        assert_eq!(
            store.read_object(&hash(b"old index")).unwrap(),
            b"old index"
        );
        assert_eq!(
            store.read_object(&hash(b"new index")).unwrap(),
            b"new index"
        );
    }
    assert_eq!(orders.len(), 2, "exercise both entity ID slot orders");
    let expected = project(&store.active, &store.events().unwrap()).unwrap();
    assert_eq!(store.projection(&store.active).unwrap(), expected);
    drop(store);
    let store = Store::open_existing(dir.path()).unwrap();
    assert_eq!(store.projection(&store.active).unwrap(), expected);
}

#[test]
fn replacement_preserves_concurrent_edit_delete_conflict() {
    let a_dir = tempfile::tempdir().unwrap();
    let b_dir = tempfile::tempdir().unwrap();
    let mut a = Store::open(a_dir.path(), Some("availability")).unwrap();
    let mut b = Store::open(b_dir.path(), Some("availability")).unwrap();
    let target = a.create("index", "file").unwrap();
    a.write_revision(
        &target.id,
        target.revisions.get("content").cloned(),
        b"original",
    )
    .unwrap();
    let allowed = BTreeSet::from([a.device.clone(), b.device.clone()]);
    b.receive(a.shared_bundle(&BTreeSet::new()).unwrap(), &allowed)
        .unwrap();
    let source = a.create("index.lock", "file").unwrap();
    a.write_revision(
        &source.id,
        source.revisions.get("content").cloned(),
        b"replacement",
    )
    .unwrap();
    a.rename(&source.id, "index", true).unwrap();
    let old = b.lookup("index").unwrap();
    b.write_with_base(
        &old.id,
        old.revisions.get("content").cloned(),
        old.revisions.get("alive").cloned(),
        b"offline edit",
    )
    .unwrap();
    a.receive(b.shared_bundle(&BTreeSet::new()).unwrap(), &allowed)
        .unwrap();
    assert_eq!(
        a.projection(&a.active).unwrap(),
        project(&a.active, &a.events().unwrap()).unwrap()
    );
    assert!(!a.conflicts().unwrap().is_empty());
    for bytes in [b"original".as_slice(), b"replacement", b"offline edit"] {
        assert_eq!(a.read_object(&hash(bytes)).unwrap(), bytes);
    }
}

#[test]
fn directory_cursor_keeps_one_cut_across_mutations_and_refreshes_on_rewind() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::new(Store::open(dir.path(), Some("enumeration")).unwrap());
    let folder = engine.store.create("folder", "directory").unwrap();
    let neighbor = engine.store.create("neighbor", "directory").unwrap();
    for i in 0..160 {
        engine
            .store
            .create(&format!("folder/f{i:03}"), "file")
            .unwrap();
        engine
            .store
            .create(&format!("neighbor/n{i:03}"), "file")
            .unwrap();
    }
    let (first, _) = engine.open("folder", None, false).unwrap();
    let mut before = vec![];
    for ordinal in 0..162 {
        before.push(
            engine
                .directory_entry(first, ordinal, ordinal == 0)
                .unwrap()
                .unwrap(),
        );
    }
    assert!(engine.directory_entry(first, 162, false).unwrap().is_none());
    assert_eq!(before[0].0.name, ".");
    assert_eq!(before[1].0.name, "..");
    assert_eq!(engine.store.children(&folder.id).unwrap().len(), 160);
    assert_eq!(engine.store.children(&neighbor.id).unwrap().len(), 160);
    let deleted = engine.store.lookup("folder/f010").unwrap();
    let moved = engine.store.lookup("folder/f040").unwrap();
    let changed = engine.store.lookup("folder/f020").unwrap();
    engine.store.delete(&deleted.id).unwrap();
    engine.store.rename(&moved.id, "folder/zzz", false).unwrap();
    engine.store.create("folder/aaa", "file").unwrap();
    engine
        .store
        .write_revision(
            &changed.id,
            changed.revisions.get("content").cloned(),
            b"new length",
        )
        .unwrap();
    // A nonsequential marker lookup can revisit any ordinal in the same cut.
    for ordinal in (0..162).rev() {
        assert_eq!(
            engine
                .directory_entry(first, ordinal, false)
                .unwrap()
                .unwrap(),
            before[ordinal]
        );
    }
    let (second, _) = engine.open("folder", None, false).unwrap();
    let mut after = vec![];
    for ordinal in 0..162 {
        after.push(
            engine
                .directory_entry(second, ordinal, ordinal == 0)
                .unwrap()
                .unwrap(),
        );
    }
    let names: BTreeSet<_> = after.iter().map(|(e, _)| e.name.as_str()).collect();
    assert!(!names.contains("f010") && !names.contains("f040"));
    assert!(names.contains("aaa") && names.contains("zzz"));
    assert_eq!(after.iter().find(|(e, _)| e.name == "f020").unwrap().1, 10);
    for (ordinal, expected) in after.iter().enumerate() {
        assert_eq!(
            engine
                .directory_entry(first, ordinal, ordinal == 0)
                .unwrap()
                .as_ref(),
            Some(expected)
        );
    }
    engine.close(first).unwrap();
    assert!(engine.directory_entry(first, 0, false).is_err());
    engine.close(second).unwrap();
}

#[test]
fn enumeration_preserves_empty_control_namespace_and_generation_fences() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::new(Store::open(dir.path(), Some("enumeration")).unwrap());
    let (handle, _) = engine.open("", None, false).unwrap();
    assert_eq!(
        engine
            .directory_entry(handle, 0, true)
            .unwrap()
            .unwrap()
            .0
            .name,
        "."
    );
    assert_eq!(
        engine
            .directory_entry(handle, 1, false)
            .unwrap()
            .unwrap()
            .0
            .name,
        ".."
    );
    assert!(engine.directory_entry(handle, 2, false).unwrap().is_none());
    let (control, _) = engine.open(".tkfs-runtime.json", None, false).unwrap();
    assert!(engine.directory_entry(control, 0, true).is_err());
    engine.close(control).unwrap();
    let other = engine.store.fork("other", false).unwrap();
    engine.store.checkout(&other.id).unwrap();
    assert!(
        engine
            .directory_entry(handle, 0, false)
            .unwrap_err()
            .to_string()
            .contains("STALE_VIEW")
    );
    let _ = engine.close(handle);
}

#[test]
fn indexed_enumeration_preserves_concurrent_namespace_alternatives() {
    let a_dir = tempfile::tempdir().unwrap();
    let b_dir = tempfile::tempdir().unwrap();
    let mut a = Store::open(a_dir.path(), Some("enumeration")).unwrap();
    let mut b = Store::open(b_dir.path(), Some("enumeration")).unwrap();
    a.create("same", "file").unwrap();
    b.create("same", "file").unwrap();
    let allowed = BTreeSet::from([a.device.clone(), b.device.clone()]);
    a.receive(b.shared_bundle(&BTreeSet::new()).unwrap(), &allowed)
        .unwrap();
    let full = project(&a.active, &a.events().unwrap()).unwrap();
    assert!(!full.conflicts.is_empty());
    let expected: BTreeSet<_> = full
        .entries
        .values()
        .filter(|e| e.alive && e.parent == ROOT)
        .map(|e| e.id.clone())
        .collect();
    assert!(!expected.is_empty());
    let actual: BTreeSet<_> = a
        .children(ROOT)
        .unwrap()
        .into_iter()
        .map(|e| e.id)
        .collect();
    assert_eq!(actual, expected);
    for parent in full
        .entries
        .values()
        .map(|e| e.parent.clone())
        .collect::<BTreeSet<_>>()
    {
        let expected: BTreeSet<_> = full
            .entries
            .values()
            .filter(|e| e.alive && e.parent == parent)
            .map(|e| e.id.clone())
            .collect();
        let actual: BTreeSet<_> = a
            .children(&parent)
            .unwrap()
            .into_iter()
            .map(|e| e.id)
            .collect();
        assert_eq!(actual, expected);
    }
    assert_eq!(a.projection(&a.active).unwrap(), full);
}
