use crate::{
    content::{seal::*, working::*},
    core::*,
};
use anyhow::Result;
use std::sync::Mutex;

#[test]
fn background_checkpoint_failure_does_not_turn_cached_durability_into_success() -> Result<()> {
    let home = tempfile::tempdir()?;
    let mut store = Store::open(home.path(), None)?;
    store.create("file", EntryKind::File)?;
    store.sync_metadata()?;
    store.checkpoint.inject_error("checkpoint disk failure");
    assert!(
        store
            .sync_metadata()
            .unwrap_err()
            .to_string()
            .contains("checkpoint disk failure")
    );
    assert!(store.commit_due().is_err());
    Ok(())
}

#[test]
fn background_checkpoint_failure_retains_unsealed_bytes_for_restart() -> Result<()> {
    use crate::runtime::engine::Engine;
    let home = tempfile::tempdir()?;
    let engine = Engine::open(home.path(), None)?;
    let handle = engine.open_file("file", true, false, true)?;
    engine.write(handle, 0, b"old bytes", false, false)?;
    engine.flush(Some(handle))?;
    engine.write(handle, 0, b"new bytes", false, false)?;
    engine
        .store
        .lock()
        .unwrap()
        .checkpoint
        .inject_error("checkpoint disk failure");
    assert!(engine.flush(Some(handle)).is_err());
    engine.cleanup(handle, false)?;
    engine.close(handle)?;
    assert!(engine.shutdown().is_err());
    drop(engine);
    let recovered = Engine::open(home.path(), None)?;
    let handle = recovered.open_file("file", false, false, false)?;
    let mut bytes = [0; 9];
    assert_eq!(recovered.read(handle, 0, &mut bytes)?, bytes.len());
    assert_eq!(&bytes, b"new bytes");
    recovered.close(handle)?;
    recovered.shutdown()?;
    Ok(())
}

#[test]
fn explicit_full_metadata_barriers_coexist_with_background_checkpointing() -> Result<()> {
    let home = tempfile::tempdir()?;
    let mut store = Store::open(home.path(), None)?;
    store
        .db
        .execute_batch("CREATE TABLE checkpoint_payload(bytes BLOB);")?;
    for _ in 0..12 {
        store.db.execute(
            "INSERT INTO checkpoint_payload VALUES(zeroblob(4194304))",
            [],
        )?;
        store.checkpoint.request();
        store.sync_metadata()?;
        let (frames, done): (i64, i64) =
            store.db.query_row("PRAGMA wal_checkpoint(NOOP)", [], |r| {
                Ok((r.get(1)?, r.get(2)?))
            })?;
        assert_eq!(
            frames, done,
            "FULL barrier returned before all frames were checkpointed"
        );
    }
    assert_eq!(
        store.db.query_row(
            "SELECT sum(length(bytes)) FROM checkpoint_payload",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        12 * 4194304
    );
    Ok(())
}

#[test]
fn sqlite_version_includes_the_concurrent_wal_reset_fix() {
    // A future background checkpoint must not introduce the WAL-reset data
    // race fixed in SQLite 3.51.3. The bundled engine is part of durability.
    assert!(
        rusqlite::version_number() >= 3_051_003,
        "SQLite {} lacks the required WAL-reset fix",
        rusqlite::version()
    );
}

fn seed_lazy_truncate(store: &mut Store, bytes: &[u8]) -> Result<String> {
    let mut state = store.create("lazy-truncate", EntryKind::File)?;
    let entity = state.id.clone();
    let parent = store.projection().visible[&entity].clone();
    state.content = Some(store.content.import(bytes)?);
    store.content.packs.flush()?;
    let event = store.event(
        &store.active.clone(),
        state,
        vec![parent],
        Operation::Content,
    );
    store.apply(&[event])?;
    store.sync_metadata()?;
    Ok(entity)
}

#[test]
fn truncate_keeps_lazy_immutable_ranges_unhydrated() -> Result<()> {
    let bytes: Vec<_> = (0..crate::model::content::CHUNK_SIZE as usize * 3)
        .map(|index| (index / crate::model::content::CHUNK_SIZE as usize + 1) as u8)
        .collect();
    for keep in [17usize, 0] {
        let root = tempfile::tempdir()?;
        let engine = crate::runtime::Engine::open(root.path(), None)?;
        seed_lazy_truncate(&mut lock(&engine.store), &bytes)?;
        let handle = engine.open_file("lazy-truncate", false, false, true)?;
        let scratch = engine.handle(handle)?.scratch.clone().unwrap();
        let reads = engine.working.content().packs.verified_reads();
        engine.truncate(handle, keep as u64, false)?;
        assert!(
            lock(&scratch).hydrated.is_empty(),
            "truncate copied immutable chunks that remain readable from their base"
        );
        assert_eq!(
            engine.working.content().packs.verified_reads(),
            reads,
            "truncate read discarded base data"
        );
        engine.truncate(handle, bytes.len() as u64, false)?;
        let mut actual = vec![99; bytes.len()];
        assert_eq!(engine.read(handle, 0, &mut actual)?, actual.len());
        let mut expected = vec![0; bytes.len()];
        expected[..keep].copy_from_slice(&bytes[..keep]);
        assert_eq!(actual, expected);
        engine.flush(Some(handle))?;
        engine.close(handle)?;
        engine.shutdown()?;
    }
    Ok(())
}

#[test]
fn truncate_visibility_marker_failure_blocks_later_extension_until_retry() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let bytes = vec![7; crate::model::content::CHUNK_SIZE as usize * 2];
    seed_lazy_truncate(&mut lock(&engine.store), &bytes)?;
    let handle = engine.open_file("lazy-truncate", false, false, true)?;
    let scratch = engine.handle(handle)?.scratch.clone().unwrap();
    // The record goes aside first, so a failure there has cut nothing.
    let aside = lock(&scratch).path.with_extension("json.tmp");
    std::fs::create_dir(&aside)?;
    assert!(engine.truncate(handle, 17, false).is_err());
    assert_eq!(lock(&scratch).len()?, bytes.len() as u64);
    std::fs::remove_dir(&aside)?;
    // A record that cannot replace the old one comes after the cut, which stands: a restart reads
    // a file shorter than its record as cut. It has to be on record before the file can grow.
    let record = {
        let mut draft = lock(&scratch);
        draft.materialize()?;
        draft.path.with_extension("json")
    };
    std::fs::remove_file(&record)?;
    std::fs::create_dir(&record)?;
    engine.truncate(handle, 17, false)?;
    assert_eq!(
        lock(&scratch).len()?,
        17,
        "physical shrink must not require hydration"
    );
    assert!(
        engine.truncate(handle, 17, false).is_err(),
        "same-size retry must retry the failed visibility marker"
    );
    assert!(engine.truncate(handle, bytes.len() as u64, false).is_err());
    assert!(engine.write(handle, 17, b"extend", false, false).is_err());
    assert_eq!(
        lock(&scratch).file()?.metadata()?.len(),
        17,
        "failed visibility publication must prevent re-extension"
    );
    std::fs::remove_dir(&record)?;
    engine.truncate(handle, 17, false)?;
    assert_eq!(lock(&scratch).record.base_visible_len, Some(17));
    engine.truncate(handle, bytes.len() as u64, false)?;
    let mut actual = vec![99; bytes.len()];
    engine.read(handle, 0, &mut actual)?;
    assert_eq!(&actual[..17], &bytes[..17]);
    assert!(actual[17..].iter().all(|byte| *byte == 0));
    engine.flush(Some(handle))?;
    engine.close(handle)?;
    engine.shutdown()?;
    Ok(())
}

#[test]
fn truncate_lazy_prefix_and_reextend_recovers_zero_tail_before_seal() -> Result<()> {
    let root = tempfile::tempdir()?;
    let bytes = vec![9; crate::model::content::CHUNK_SIZE as usize * 3];
    let mut metadata = Store::open(root.path(), None)?;
    let entity = seed_lazy_truncate(&mut metadata, &bytes)?;
    let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
    let scratch = layer.open(metadata.basis(&entity)?, true)?;
    {
        let mut draft = lock(&scratch);
        draft.truncate(17)?;
        draft.truncate(bytes.len() as u64)?;
        draft.file()?.sync_all()?;
    }
    drop(scratch);
    drop(layer);
    drop(metadata);
    let metadata = Mutex::new(Store::open(root.path(), None)?);
    let layer = WorkingLayer::new(root.path(), lock(&metadata).content.clone())?;
    assert_eq!(recover(&metadata, &layer)?, 1);
    let store = lock(&metadata);
    let mut actual = vec![99; bytes.len()];
    store
        .content
        .read_range(&store.basis(&entity)?.content, 0, &mut actual)?;
    assert_eq!(&actual[..17], &bytes[..17]);
    assert!(
        actual[17..].iter().all(|byte| *byte == 0),
        "recovery resurrected discarded immutable bytes"
    );
    Ok(())
}

#[test]
fn seal_keeps_the_visible_write_time_and_honors_explicit_times() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let writer = engine.open_file("file", true, false, true)?;
    engine.write(writer, 0, b"before", false, false)?;
    let explicit = 116_443_872_000_000_000; // 1969-12-31; native times may predate Unix.
    engine.basic(writer, u32::MAX, [0, 0, explicit, 0])?;
    assert_eq!(engine.info_visible(writer)?.0.times.modified, explicit);
    engine.flush_native_file(writer)?;
    assert_eq!(engine.info_visible(writer)?.0.times.modified, explicit);
    engine.write(writer, 0, b"after!", false, false)?;
    let visible = engine.info_visible(writer)?.0.times.modified;
    assert!(
        visible > explicit,
        "new writes must advance the explicit old time"
    );
    std::thread::sleep(std::time::Duration::from_millis(25));
    engine.flush_native_file(writer)?;
    assert_eq!(engine.info_visible(writer)?.0.times.modified, visible);
    assert_eq!(engine.lookup("file")?.times.modified, visible);
    engine.close(writer)?;
    engine.shutdown()?;
    let reopened = Store::open(root.path(), None)?;
    assert_eq!(reopened.lookup("file")?.times.modified, visible);
    Ok(())
}

#[test]
fn native_cleanup_notifies_before_delayed_seal() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let (send, receive) = std::sync::mpsc::channel();
    engine.set_invalidator(std::sync::Arc::new(move |path, action| {
        send.send((path.to_owned(), action))?;
        Ok(())
    }));
    let writer = engine.open_file("coherent", true, false, true)?;
    engine.truncate(writer, 3, false)?;
    engine.write_native(writer, 0, b"one", false, true)?;
    engine.cleanup_native(writer, false, true)?;
    assert_eq!(
        receive.recv_timeout(std::time::Duration::from_millis(500))?,
        ("/coherent".into(), 3)
    );
    let store = lock(&engine.store);
    assert_eq!(store.lookup("coherent")?.content.unwrap().total_len, 0);
    drop(store);
    engine.close(writer)?;
    engine.shutdown()?;
    Ok(())
}

#[test]
fn paging_write_reply_and_cleanup_keep_one_automatic_write_time() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let writer = engine.open_file("clock", true, false, true)?;
    engine.truncate(writer, 3, false)?;
    engine.write_native(writer, 0, b"one", false, true)?;
    let reply_time = engine.info_visible(writer)?.0.times.modified;
    std::thread::sleep(std::time::Duration::from_millis(20));
    engine.cleanup_native(writer, false, true)?;
    assert_eq!(engine.lookup("clock")?.times.modified, reply_time);
    engine.flush_native_file(writer)?;
    assert_eq!(engine.lookup("clock")?.times.modified, reply_time);
    engine.close(writer)?;
    let next = engine.open_file("clock", false, false, true)?;
    std::thread::sleep(std::time::Duration::from_millis(20));
    engine.write_native(next, 0, b"two", false, true)?;
    let next_time = engine.info_visible(next)?.0.times.modified;
    assert!(next_time > reply_time);
    // A file fsync before Cleanup must not make Cleanup invent another time.
    engine.flush_native_file(next)?;
    std::thread::sleep(std::time::Duration::from_millis(20));
    engine.cleanup_native(next, false, true)?;
    assert_eq!(engine.lookup("clock")?.times.modified, next_time);
    engine.close(next)?;
    engine.shutdown()?;
    Ok(())
}

#[test]
fn cached_writeback_keeps_explicit_times_until_native_cleanup_requests_a_new_time() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let writer = engine.open_file("file", true, false, true)?;
    engine.truncate(writer, 3, false)?;
    let explicit = 132_537_600_000_000_000;
    engine.basic(writer, u32::MAX, [0, 0, explicit, 0])?;
    // Cached bytes can arrive after SetBasicInfo. Paging I/O is not a new
    // application write and must not erase the explicit timestamp.
    engine.write_native(writer, 0, b"one", false, true)?;
    engine.cleanup_native(writer, false, false)?;
    engine.flush_native_file(writer)?;
    assert_eq!(engine.lookup("file")?.times.modified, explicit);
    let next = engine.open_file("file", false, false, true)?;
    engine.write_native(next, 0, b"two", false, true)?;
    // The driver sets this flag for another cached application write after
    // SetBasicInfo; its cleanup is the automatic write-time boundary.
    engine.cleanup_native(next, false, true)?;
    let modified = engine.lookup("file")?.times.modified;
    assert!(modified > explicit);
    engine.flush_native_file(next)?;
    assert_eq!(engine.lookup("file")?.times.modified, modified);
    engine.close(next)?;
    engine.close(writer)?;
    engine.shutdown()?;
    Ok(())
}

#[test]
fn native_metadata_open_does_not_fetch_a_remote_manifest_or_create_scratch() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let length = 2 * crate::model::content::CHUNK_SIZE;
    {
        let mut store = lock(&engine.store);
        let mut entry = store.create("remote", EntryKind::File)?;
        let basis = store.basis(&entry.id)?;
        entry.content = Some(crate::model::content::FileContentRef::new(
            crate::model::content::ContentKind::Manifest,
            crate::model::content::Digest::bytes(b"absent remote manifest"),
            length,
        )?);
        let event = store.event(MAIN, entry, vec![basis.version], Operation::Content);
        store.apply_batch(&[event], &[])?;
    }
    let attributes = engine.open_native_file("remote", false, false, false, None, true)?;
    assert!(engine.handle(attributes)?.scratch.is_none());
    assert_eq!(engine.info(attributes)?.1, length);
    assert_eq!(engine.info_visible(attributes)?.1, length);
    engine.close(attributes)?;
    assert!(engine.working.records()?.is_empty());
    // A real data open still demands the manifest instead of silently treating
    // missing bytes as an empty file.
    assert!(engine.open_file("remote", false, false, false).is_err());
    engine.shutdown()?;
    Ok(())
}

#[test]
fn native_metadata_context_still_routes_paging_writes_and_file_flush_to_the_entity() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let attributes = engine.open_native_file("file", true, false, false, None, true)?;
    assert!(engine.handle(attributes)?.scratch.is_none());
    let writer = engine.open_file("file", false, false, true)?;
    engine.write(writer, 0, b"first", false, false)?;
    assert!(
        engine
            .write_native(attributes, 0, b"paged", false, false)
            .is_err()
    );
    assert_eq!(
        engine.write_native(attributes, 0, b"paged", false, true)?,
        5
    );
    engine.flush_native_file(attributes)?;
    let mut bytes = [0; 5];
    engine.read_visible(attributes, 0, &mut bytes)?;
    assert_eq!(&bytes, b"paged");
    assert_eq!(engine.info_visible(attributes)?.1, 5);
    engine.close(writer)?;
    engine.close(attributes)?;
    engine.shutdown()?;
    Ok(())
}

#[test]
fn native_metadata_rename_folds_a_writer_opened_after_the_attribute_handle() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let target = engine.open_file("target", true, false, true)?;
    engine.write(target, 0, b"before", false, false)?;
    engine.flush(Some(target))?;
    let target_id = engine.lookup("target")?.id;
    let attributes = engine.open_native_file("temp", true, false, false, None, true)?;
    assert!(engine.handle(attributes)?.scratch.is_none());
    let writer = engine.open_file("temp", false, false, true)?;
    engine.write(writer, 0, b"saved", false, false)?;
    engine.rename(attributes, "target", true)?;
    assert_eq!(engine.lookup("target")?.id, target_id);
    assert!(engine.lookup("temp").is_err());
    let mut bytes = [0; 5];
    engine.read_visible(attributes, 0, &mut bytes)?;
    assert_eq!(&bytes, b"saved");
    for handle in [target, attributes, writer] {
        engine.close(handle)?;
    }
    engine.shutdown()?;
    Ok(())
}

#[test]
fn metadata_create_keeps_empty_content_live_without_flushing_pending_objects() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut store = Store::open(root.path(), None)?;
    let pending = store
        .content
        .packs
        .put(crate::content::local::Kind::Chunk, b"pending seal")?;
    let empty = store.create("empty", EntryKind::File)?.content.unwrap();
    // File creation must not publish another entity's pending seal objects.
    let index = rusqlite::Connection::open(root.path().join("objects/index.sqlite"))?;
    let committed: u64 = index.query_row(
        "SELECT count(*) FROM objects WHERE digest=?1",
        [&pending.0[..]],
        |row| row.get(0),
    )?;
    assert_eq!(committed, 0);
    // The shared empty reference stays usable even when GC has no roots.
    store.content.packs.collect(&Default::default(), 1.0)?;
    assert!(store.content.packs.contains(empty.digest)?);
    assert!(store.content.packs.get(empty.digest)?.is_empty());
    let another = store.create("another", EntryKind::File)?.content.unwrap();
    assert_eq!(another, empty);
    Ok(())
}

#[test]
fn receipt_recovery_preserves_ranges_appended_after_the_recorded_base() -> Result<()> {
    let root = tempfile::tempdir()?;
    let entity;
    {
        let mut metadata = Store::open(root.path(), None)?;
        entity = metadata.create("receipt-extension", EntryKind::File)?.id;
        let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
        let scratch = layer.open(metadata.basis(&entity)?, true)?;
        let prior = lock(&scratch).record.clone();
        let store = Mutex::new(metadata);
        lock(&scratch).write(layer.content(), 0, b"old", false, false)?;
        seal_batch(&store, &layer, std::slice::from_ref(&scratch))?;
        lock(&scratch).write(layer.content(), 0, b"new bytes", false, false)?;
        // The receipt committed but its sidecar advancement did not survive.
        // Keep the old record format too: recovery must support existing stores.
        let mut legacy = serde_json::to_value(prior)?;
        legacy.as_object_mut().unwrap().remove("base_visible_len");
        std::fs::write(
            lock(&scratch).path.with_extension("json"),
            serde_json::to_vec(&legacy)?,
        )?;
    }
    let store = Mutex::new(Store::open(root.path(), None)?);
    let layer = WorkingLayer::new(root.path(), lock(&store).content.clone())?;
    assert_eq!(recover(&store, &layer)?, 1);
    let metadata = lock(&store);
    let mut bytes = [0; 9];
    metadata
        .content
        .read_range(&metadata.basis(&entity)?.content, 0, &mut bytes)?;
    assert_eq!(&bytes, b"new bytes");
    Ok(())
}

#[test]
fn sealing_releases_scratch_before_durability_and_recovers_a_concurrent_truncate() -> Result<()> {
    use std::sync::{Arc, TryLockError};
    use std::time::{Duration, Instant};
    let root = tempfile::tempdir()?;
    let mut metadata = Store::open(root.path(), None)?;
    let entity = metadata.create("concurrent-seal", EntryKind::File)?.id;
    let basis = metadata.basis(&entity)?;
    let layer = Arc::new(WorkingLayer::new(root.path(), metadata.content.clone())?);
    let scratch = layer.open(basis, true)?;
    let length = 2 * 1024 * 1024;
    lock(&scratch).write(layer.content(), 0, &vec![31; length], false, false)?;
    let store = Arc::new(Mutex::new(metadata));
    // A real index writer holds the pack durability transaction, without a
    // mount or a timing assumption about how long a device fsync takes.
    let blocker = rusqlite::Connection::open(root.path().join("objects/index.sqlite"))?;
    blocker.execute_batch("BEGIN IMMEDIATE")?;
    let held = lock(&store);
    let sealing_store = store.clone();
    let sealing_layer = layer.clone();
    let sealing_scratch = scratch.clone();
    let (send, receive) = std::sync::mpsc::channel();
    let sealer = std::thread::spawn(move || {
        send.send(seal_batch(
            &sealing_store,
            &sealing_layer,
            &[sealing_scratch],
        ))
        .unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(2);
    // Nothing is held across the sleep: a guard kept through it would starve the sealer.
    let entered = loop {
        if matches!(scratch.try_lock(), Err(TryLockError::WouldBlock)) {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    drop(held);
    let deadline = Instant::now() + Duration::from_millis(500);
    let released = loop {
        match scratch.try_lock() {
            Ok(mut state) if entered => {
                // Shrink then re-extend while the prepared reference still
                // contains nonzero bytes. Unmaterialized successor ranges must
                // remain zero after both the first commit and crash recovery.
                state.truncate(3)?;
                state.truncate(length as u64)?;
                state.write(layer.content(), 0, b"new", false, false)?;
                break true;
            }
            _ if Instant::now() >= deadline => break false,
            _ => std::thread::sleep(Duration::from_millis(1)),
        }
    };
    if released {
        layer.close(&scratch, true);
    }
    blocker.execute_batch("COMMIT")?;
    receive.recv_timeout(Duration::from_secs(3))??;
    sealer.join().unwrap();
    assert!(entered, "seal never reached its scratch preparation");
    assert!(released, "scratch remained locked across pack durability");
    assert!(
        lock(&scratch).changed,
        "concurrent write was cleared by the earlier seal"
    );
    let mut expected = vec![0; length];
    expected[..3].copy_from_slice(b"new");
    let mut bytes = vec![0; length];
    lock(&scratch).read(layer.content(), 0, &mut bytes)?;
    assert_eq!(bytes, expected);
    drop(scratch);
    drop(layer);
    drop(store);
    let store = Mutex::new(Store::open(root.path(), None)?);
    let layer = WorkingLayer::new(root.path(), lock(&store).content.clone())?;
    assert_eq!(recover(&store, &layer)?, 1);
    assert_eq!(recover(&store, &layer)?, 0);
    let metadata = lock(&store);
    metadata
        .content
        .read_range(&metadata.basis(&entity)?.content, 0, &mut bytes)?;
    assert_eq!(bytes, expected);
    Ok(())
}

#[test]
fn recovery_and_seal_preserve_unmaterialized_ranges_of_a_lazy_draft() -> Result<()> {
    for seal_first in [false, true] {
        let root = tempfile::tempdir()?;
        let mut expected = vec![23; 3 * 65536 + 7];
        let entity;
        {
            let mut metadata = Store::open(root.path(), None)?;
            let mut entry = metadata.create("lazy", EntryKind::File)?;
            entity = entry.id.clone();
            let parent = metadata.basis(&entity)?.version;
            entry.content = Some(metadata.content.import(&expected)?);
            metadata.content.packs.flush()?;
            let branch = metadata.active.clone();
            let event = metadata.event(&branch, entry, vec![parent], Operation::Content);
            metadata.apply(&[event])?;
            metadata.flush_metadata()?;
            let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
            let scratch = layer.open(metadata.basis(&entity)?, false)?;
            assert!(!lock(&scratch).path.exists());
            // Hydrate only two ranges; the third complete chunk stays in packs.
            lock(&scratch).write(layer.content(), 65534, b"paging", false, true)?;
            expected[65534..65540].copy_from_slice(b"paging");
            let store = Mutex::new(metadata);
            if seal_first {
                seal_batch(&store, &layer, std::slice::from_ref(&scratch))?;
                lock(&scratch).write(layer.content(), 0, b"new", false, true)?;
                expected[..3].copy_from_slice(b"new");
            }
            assert!(!lock(&scratch).hydrated.contains(&2));
        }
        let store = Mutex::new(Store::open(root.path(), None)?);
        let layer = WorkingLayer::new(root.path(), lock(&store).content.clone())?;
        assert_eq!(recover(&store, &layer)?, 1);
        let metadata = lock(&store);
        let mut actual = vec![0; expected.len()];
        metadata
            .content
            .read_range(&metadata.basis(&entity)?.content, 0, &mut actual)?;
        assert_eq!(actual, expected);
    }
    Ok(())
}

#[test]
fn seal_exactly_once_across_restart_and_changed_chunks_only() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut metadata = Store::open(root.path(), None)?;
    let entity = metadata.create("file", EntryKind::File)?.id;
    let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
    let scratch = layer.open(metadata.basis(&entity)?, true)?;
    let store = Mutex::new(metadata);
    lock(&scratch).write(layer.content(), 0, &vec![7; 3 * 65536], false, false)?;
    let stats = seal_batch(&store, &layer, std::slice::from_ref(&scratch))?;
    assert_eq!(stats.hashed_chunks, 3);
    assert_eq!(stats.new_chunks, 1);
    lock(&scratch).write(layer.content(), 65536 + 4096, b"edit", false, false)?;
    let stats = seal_batch(&store, &layer, std::slice::from_ref(&scratch))?;
    assert_eq!(stats.hashed_chunks, 1);
    assert_eq!(stats.new_chunks, 1);
    let before = lock(&store).events_after(0, 4096)?.len();
    // Emulate a crash after commit but before the sidecar is advanced.
    let mut record = lock(&scratch).record.clone();
    record.operation = "11111111-1111-4111-8111-111111111111".into();
    record.id = record.operation.clone();
    let path = lock(&scratch).path.clone();
    // A recovery file name is the operation identity.
    let crash_path = path.with_file_name(format!("{}.scratch", record.operation));
    std::fs::copy(&path, &crash_path)?;
    let stamp = modified_ns(&std::fs::File::open(&crash_path)?)?;
    let event = lock(&scratch).record.basis.version.clone();
    lock(&store).db.execute(
        "INSERT INTO seal_receipts VALUES(?1,?2,?3)",
        rusqlite::params![record.operation, event, stamp.to_string()],
    )?;
    std::fs::write(
        crash_path.with_extension("json"),
        serde_json::to_vec(&record)?,
    )?;
    drop(scratch);
    drop(layer);
    drop(store);
    let store = Mutex::new(Store::open(root.path(), None)?);
    let layer = WorkingLayer::new(root.path(), lock(&store).content.clone())?;
    recover(&store, &layer)?;
    assert_eq!(lock(&store).events_after(0, 4096)?.len(), before);
    assert!(!crash_path.exists());
    drop(layer);
    drop(store);
    let metadata = Store::open(root.path(), None)?;
    let mut bytes = [0; 4];
    metadata
        .content
        .read_range(&metadata.basis(&entity)?.content, 65536 + 4096, &mut bytes)?;
    assert_eq!(&bytes, b"edit");
    Ok(())
}

#[test]
#[ignore = "opt-in native backing seal-cost diagnostic; no filesystem mount"]
fn repeated_chunk_seal_cost_diagnostic() -> Result<()> {
    for lap in 0..3 {
        let root = tempfile::tempdir()?;
        let mut metadata = Store::open(root.path(), None)?;
        let entity = metadata.create("file", EntryKind::File)?.id;
        let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
        let scratch = layer.open(metadata.basis(&entity)?, true)?;
        let store = Mutex::new(metadata);
        let block = vec![7; 1024 * 1024];
        for offset in 0..512 {
            lock(&scratch).write(
                layer.content(),
                offset * block.len() as u64,
                &block,
                false,
                false,
            )?;
        }
        let started = std::time::Instant::now();
        let stats = seal_batch(&store, &layer, std::slice::from_ref(&scratch))?;
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(stats.hashed_chunks, 8192);
        assert_eq!(stats.new_chunks, 1);
        let reference = lock(&store).basis(&entity)?.content;
        assert_eq!(reference.total_len, 512 * 1024 * 1024);
        let mut probe = vec![0; 65536];
        for offset in [0, 255 * 1024 * 1024, reference.total_len - 65536] {
            layer.content().read_range(&reference, offset, &mut probe)?;
            assert!(probe.iter().all(|byte| *byte == 7));
        }
        println!(
            "SEAL_DIGEST_DIAGNOSTIC {}",
            serde_json::json!({"lap":lap,"bytes":reference.total_len,"seal_ms":elapsed_ms,
                "hashed_chunks":stats.hashed_chunks,"new_chunks":stats.new_chunks})
        );
    }
    Ok(())
}

#[test]
#[ignore = "opt-in copied native metadata page-cache diagnostic; no filesystem mount"]
fn copied_metadata_page_cache_diagnostic() -> Result<()> {
    fn status(db: &rusqlite::Connection, op: i32) -> Result<i32> {
        let (mut current, mut high) = (0, 0);
        // The connection stays exclusively borrowed on this test thread. This
        // only reads SQLite's counters; it does not reset or change the pager.
        let rc = unsafe {
            rusqlite::ffi::sqlite3_db_status(db.handle(), op, &mut current, &mut high, 0)
        };
        anyhow::ensure!(rc == rusqlite::ffi::SQLITE_OK, "db_status failed: {rc}");
        Ok(current)
    }
    fn counters(db: &rusqlite::Connection) -> Result<serde_json::Value> {
        use rusqlite::ffi::*;
        Ok(serde_json::json!({
            "hit":status(db, SQLITE_DBSTATUS_CACHE_HIT)?,
            "miss":status(db, SQLITE_DBSTATUS_CACHE_MISS)?,
            "write":status(db, SQLITE_DBSTATUS_CACHE_WRITE)?,
            "spill":status(db, SQLITE_DBSTATUS_CACHE_SPILL)?,
            "used_bytes":status(db, SQLITE_DBSTATUS_CACHE_USED)?,
        }))
    }
    let path = std::path::PathBuf::from(std::env::var("TKFS_METADATA_CACHE_COPY")?);
    // The controller supplies a fresh native copy for each invocation. Never
    // accept a repository root or silently create an empty benchmark database.
    anyhow::ensure!(path.is_absolute() && path.join("metadata.sqlite").is_file());
    let cache_kib: i32 = std::env::var("TKFS_METADATA_CACHE_KIB")?.parse()?;
    anyhow::ensure!([2000, 65536].contains(&cache_kib));
    let mut store = Store::open(&path, None)?;
    let head: u64 = store
        .db
        .query_row("SELECT max(seq) FROM events", [], |r| r.get(0))?;
    assert_eq!(
        head, 125276,
        "diagnostic requires the retained, unedited corpus"
    );
    store.db.pragma_update(None, "cache_size", -cache_kib)?;
    let before = counters(&store.db)?;
    let started = std::time::Instant::now();
    store.create("page-cache-diagnostic", EntryKind::Directory)?;
    let mut created = Vec::new();
    let mut receipts = Vec::new();
    for i in 0..2000 {
        let mut entry = store.create(
            &format!("page-cache-diagnostic/file-{i:05}"),
            EntryKind::File,
        )?;
        // Exercise the real FULL receipt transaction with valid empty content,
        // without including scratch, hashing, driver or client costs.
        entry.times.modified += 1;
        let parent = store.basis(&entry.id)?.version;
        let event = store.event(MAIN, entry.clone(), vec![parent], Operation::Content);
        let receipt = (
            format!("page-cache-diagnostic-{i}"),
            event.id.clone(),
            u128::from(entry.times.modified),
        );
        store.apply_batch(std::slice::from_ref(&event), std::slice::from_ref(&receipt))?;
        created.push(entry);
        receipts.push(receipt);
    }
    store.sync_metadata()?;
    let create_ms = started.elapsed().as_secs_f64() * 1000.0;
    let after_create = counters(&store.db)?;
    let started = std::time::Instant::now();
    for entry in &created {
        store.delete(&entry.id)?;
    }
    store.sync_metadata()?;
    let delete_ms = started.elapsed().as_secs_f64() * 1000.0;
    let after_delete = counters(&store.db)?;
    let synchronous: i64 = store.db.query_row("PRAGMA synchronous", [], |r| r.get(0))?;
    assert_eq!(synchronous, 1, "receipt did not restore NORMAL mode");
    drop(store);
    let mut reopened = Store::open(&path, None)?;
    for (entry, receipt) in created.iter().zip(&receipts) {
        assert!(reopened.entry(&entry.id)?.deleted);
        assert_eq!(
            reopened.receipt(&receipt.0)?,
            Some((receipt.1.clone(), receipt.2))
        );
    }
    let final_head: u64 = reopened
        .db
        .query_row("SELECT max(seq) FROM events", [], |r| r.get(0))?;
    assert_eq!(final_head, head + 6001);
    assert_eq!(
        reopened
            .db
            .query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))?,
        "ok"
    );
    reopened.sync_metadata()?;
    println!(
        "METADATA_PAGE_CACHE_DIAGNOSTIC {}",
        serde_json::json!({
            "cache_kib":cache_kib,"files":created.len(),"initial_head":head,"final_head":final_head,
            "create_receipt_ms":create_ms,"delete_ms":delete_ms,
            "before":before,"after_create":after_create,"after_delete":after_delete,
            "reopen_deleted_and_receipts_verified":true,"integrity_check":"ok",
        })
    );
    Ok(())
}

#[test]
fn seal_retry_with_committed_bytes_advances_the_scratch_without_a_duplicate_event() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut metadata = Store::open(root.path(), None)?;
    let entity = metadata.create("retry", EntryKind::File)?.id;
    let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
    let scratch = layer.open(metadata.basis(&entity)?, true)?;
    let store = Mutex::new(metadata);
    let prior = lock(&scratch).record.clone();
    lock(&scratch).write(layer.content(), 0, b"committed", false, false)?;
    seal_batch(&store, &layer, std::slice::from_ref(&scratch))?;
    let basis = lock(&store).basis(&entity)?;
    let before = lock(&store).events_after(0, 4096)?.len();
    {
        let mut state = lock(&scratch);
        state.record = prior.clone();
        state.changed = true;
        state.dirty.mark(0, 9);
    }
    seal_batch(&store, &layer, std::slice::from_ref(&scratch))?;
    assert_eq!(lock(&store).events_after(0, 4096)?.len(), before);
    let state = lock(&scratch);
    assert!(!state.changed);
    assert_ne!(state.record.operation, prior.operation);
    let saved: ScratchRecord =
        serde_json::from_slice(&std::fs::read(state.path.with_extension("json"))?)?;
    assert_eq!(saved.basis.version, basis.version);
    assert_eq!(saved.basis.content, basis.content);
    assert_eq!(saved.hydrated, state.hydrated);
    assert_eq!(saved.operation, state.record.operation);
    Ok(())
}

#[test]
fn seal_retry_preserves_edits_with_an_older_timestamp_than_its_committed_receipt() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut metadata = Store::open(root.path(), None)?;
    let entity = metadata.create("retry", EntryKind::File)?.id;
    let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
    let scratch = layer.open(metadata.basis(&entity)?, true)?;
    let store = Mutex::new(metadata);
    let prior = lock(&scratch).record.clone();
    lock(&scratch).write(layer.content(), 0, b"old", false, false)?;
    seal_batch(&store, &layer, std::slice::from_ref(&scratch))?;
    let committed = lock(&scratch).record.basis.version.clone();
    let before = lock(&store).events_after(0, 4096)?.len();
    {
        let mut state = lock(&scratch);
        // The commit succeeded, but its in-memory/sidecar advancement failed.
        state.record = prior;
        state.changed = true;
        state.dirty.mark(0, 3);
        state.write(layer.content(), 0, b"new", false, false)?;
        // A coarse or backwards clock must not make the receipt discard this edit.
        let stamp = modified_ns(state.file()?)? + 86_400_000_000_000;
        lock(&store).db.execute(
            "UPDATE seal_receipts SET modified_ns=?1 WHERE operation=?2",
            rusqlite::params![stamp.to_string(), state.record.operation],
        )?;
    }
    seal_batch(&store, &layer, std::slice::from_ref(&scratch))?;
    assert_eq!(lock(&store).events_after(0, 4096)?.len(), before + 1);
    let metadata = lock(&store);
    let basis = metadata.basis(&entity)?;
    let version = metadata.version(&basis.version)?.unwrap();
    assert_eq!(version.parents, vec![committed]);
    let mut bytes = [0; 3];
    metadata.content.read_range(&basis.content, 0, &mut bytes)?;
    assert_eq!(&bytes, b"new");
    Ok(())
}

#[test]
fn recovery_seals_surviving_dirty_scratch_once() -> Result<()> {
    let root = tempfile::tempdir()?;
    {
        let mut store = Store::open(root.path(), None)?;
        let entity = store.create("saved", EntryKind::File)?.id;
        let layer = WorkingLayer::new(root.path(), store.content.clone())?;
        let scratch = layer.open(store.basis(&entity)?, true)?;
        lock(&scratch).write(layer.content(), 0, b"survive", false, false)?;
    }
    let store = Mutex::new(Store::open(root.path(), None)?);
    let layer = WorkingLayer::new(root.path(), lock(&store).content.clone())?;
    assert_eq!(recover(&store, &layer)?, 1);
    assert_eq!(recover(&store, &layer)?, 0);
    let metadata = lock(&store);
    let entry = metadata.lookup("saved")?;
    let mut bytes = [0; 7];
    metadata
        .content
        .read_range(entry.content.as_ref().unwrap(), 0, &mut bytes)?;
    assert_eq!(&bytes, b"survive");
    Ok(())
}

#[test]
fn recovery_does_not_discard_changed_bytes_when_the_scratch_timestamp_is_not_newer() -> Result<()> {
    let root = tempfile::tempdir()?;
    let payload = b"recoverable lifecycle bytes".repeat(4096);
    {
        let mut metadata = Store::open(root.path(), None)?;
        let grown = metadata.create("grown", EntryKind::File)?.id;
        let same = metadata.create("same", EntryKind::File)?.id;
        let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
        let a = layer.open(metadata.basis(&grown)?, true)?;
        let b = layer.open(metadata.basis(&same)?, true)?;
        let store = Mutex::new(metadata);
        lock(&a).write(layer.content(), 0, &payload, false, false)?;
        lock(&b).write(layer.content(), 0, b"old", false, false)?;
        seal_batch(&store, &layer, std::slice::from_ref(&b))?;
        lock(&b).write(layer.content(), 0, b"new", false, false)?;
        for file in [&a, &b] {
            let mut scratch = lock(file);
            // Coarse timestamps or a backwards clock can make dirty bytes
            // look older than their clean marker, even at the same file size.
            scratch.record.modified_ns = modified_ns(scratch.file()?)? + 86_400_000_000_000;
            scratch.persist_record()?;
        }
    }
    let store = Mutex::new(Store::open(root.path(), None)?);
    let layer = WorkingLayer::new(root.path(), lock(&store).content.clone())?;
    assert_eq!(recover(&store, &layer)?, 2);
    assert_eq!(recover(&store, &layer)?, 0);
    for (path, expected) in [("grown", payload.as_slice()), ("same", b"new".as_slice())] {
        let metadata = lock(&store);
        let content = metadata.lookup(path)?.content.unwrap();
        let mut bytes = vec![0; content.total_len as usize];
        metadata.content.read_range(&content, 0, &mut bytes)?;
        assert_eq!(bytes, expected);
    }
    Ok(())
}

#[test]
fn recovery_preserves_partial_initial_records_and_uncommitted_entities() -> Result<()> {
    let root = tempfile::tempdir()?;
    let orphan;
    {
        let mut store = Store::open(root.path(), None)?;
        let entry = store.create("not-yet-committed", EntryKind::File)?;
        let layer = WorkingLayer::new(root.path(), store.content.clone())?;
        let scratch = layer.open(store.basis(&entry.id)?, true)?;
        lock(&scratch).write(layer.content(), 0, b"preserved orphan", false, false)?;
        orphan = lock(&scratch).path.file_name().unwrap().to_owned();
        store.db.execute_batch("ROLLBACK;")?;
    }
    let partial = root
        .path()
        .join("scratch")
        .join(format!("{}.scratch", id()));
    std::fs::write(&partial, b"partial initialization")?;
    std::fs::write(partial.with_extension("json"), b"{\"id\":")?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    assert!(engine.lookup("not-yet-committed").is_err());
    assert_eq!(
        std::fs::read(root.path().join("scratch/quarantine").join(orphan))?,
        b"preserved orphan"
    );
    assert_eq!(
        std::fs::read(
            root.path()
                .join("scratch/quarantine")
                .join(partial.file_name().unwrap())
        )?,
        b"partial initialization"
    );
    engine.shutdown()?;
    Ok(())
}

#[test]
fn snapshot_fork_isolation_and_cached_tree_reopen() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut store = Store::open(root.path(), None)?;
    store.create("directory", EntryKind::Directory)?;
    let entity = store.create("directory/file", EntryKind::File)?.id;
    let snapshot = store.checkpoint()?;
    let branch = store.fork(&snapshot, "fork")?;
    store.rename(&entity, "directory/renamed", false)?;
    store.checkout(&branch)?;
    assert!(store.lookup("directory/file").is_ok());
    assert!(store.lookup("directory/renamed").is_err());
    store.rename(&entity, "fork-only", false)?;
    store.checkout(MAIN)?;
    assert!(store.lookup("directory/renamed").is_ok());
    assert!(store.lookup("fork-only").is_err());
    drop(store);
    let mut store = Store::open(root.path(), None)?;
    store.checkout("fork")?;
    assert!(store.lookup("fork-only").is_ok());
    Ok(())
}

#[test]
fn entity_conflicts_keep_local_head_and_resolution_converges() -> Result<()> {
    let left = tempfile::tempdir()?;
    let right = tempfile::tempdir()?;
    let mut a = Store::open(left.path(), Some("repo"))?;
    let mut b = Store::open(right.path(), Some("repo"))?;
    let entity = a.create("file", EntryKind::File)?.id;
    let create = a.events_after(0, 10)?[0].1.clone();
    b.apply(std::slice::from_ref(&create))?;
    let mut a_state = a.entry(&entity)?;
    a_state.content = Some(a.content.import(b"local")?);
    let local = a.event(MAIN, a_state, vec![create.id.clone()], Operation::Content);
    a.apply(std::slice::from_ref(&local))?;
    let mut b_state = b.entry(&entity)?;
    b_state.content = Some(b.content.import(b"remote")?);
    let remote = b.event(MAIN, b_state, vec![create.id], Operation::Content);
    b.apply(std::slice::from_ref(&remote))?;
    a.apply(std::slice::from_ref(&remote))?;
    b.apply(std::slice::from_ref(&local))?;
    assert_eq!(a.projection().visible[&entity], local.id);
    assert_eq!(b.projection().visible[&entity], remote.id);
    assert_eq!(a.conflicts(false)?[0].heads.len(), 2);
    assert_eq!(a.projection().children(ROOT).len(), 1);
    let resolution = a.resolve(&entity, &remote.id)?;
    b.apply(&[resolution])?;
    assert!(a.conflicts(false)?.is_empty());
    assert!(b.conflicts(false)?.is_empty());
    assert_eq!(a.lookup("file")?, b.lookup("file")?);
    assert_eq!(a.conflicts(true)?[0].heads.len(), 2);
    assert!(
        a.live_objects(32)?
            .contains(&local.state.content.unwrap().digest)
    );
    Ok(())
}

#[test]
fn reopened_conflict_history_distinguishes_current_heads_from_retired_alternatives() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut store = Store::open(root.path(), None)?;
    let entity = store.create("file", EntryKind::File)?.id;
    let parent = store.basis(&entity)?.version;
    let state = store.entry(&entity)?;
    let local = store.event(MAIN, state.clone(), vec![parent.clone()], Operation::Times);
    let mut remote = store.event(MAIN, state.clone(), vec![parent], Operation::Times);
    remote.author = "peer".into();
    store.apply(&[local.clone(), remote])?;
    let resolved = store.resolve(&entity, &local.id)?;
    let next_local = store.event(
        MAIN,
        state.clone(),
        vec![resolved.id.clone()],
        Operation::Times,
    );
    let mut next_remote = store.event(MAIN, state, vec![resolved.id], Operation::Times);
    next_remote.author = "peer".into();
    store.apply(&[next_local.clone(), next_remote.clone()])?;
    let conflict = store.conflicts(true)?.remove(0);
    assert!(!conflict.resolved);
    assert_eq!(conflict.heads.len(), 4);
    assert_eq!(
        conflict.current_heads,
        [next_local.id.clone(), next_remote.id.clone()]
            .into_iter()
            .collect()
    );
    assert!(conflict.heads.iter().any(|event| event.id == local.id));
    assert!(!conflict.current_heads.contains(&local.id));
    assert!(store.resolve(&entity, &local.id).is_err());
    store.resolve(&entity, &next_remote.id)?;
    assert!(
        store
            .conflicts(true)?
            .iter()
            .all(|conflict| conflict.resolved)
    );
    Ok(())
}

#[test]
fn remote_apply_invalidates_cached_paths_without_changing_open_version() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), Some("repo"))?;
    let h = engine.open_file("file", true, false, true)?;
    engine.write(h, 0, b"local", false, false)?;
    engine.flush(Some(h))?;
    let observed = std::sync::Arc::new(Mutex::new(Vec::new()));
    let sink = observed.clone();
    engine.set_invalidator(std::sync::Arc::new(move |path, action| {
        lock(&sink).push((path.to_string(), action));
        Ok(())
    }));
    let event = {
        let mut s = lock(&engine.store);
        let entity = s.lookup("file")?.id;
        let base = s.basis(&entity)?;
        let mut state = s.entry(&entity)?;
        state.content = Some(s.content.import(b"other")?);
        let mut event = s.event(MAIN, state, vec![base.version], Operation::Content);
        event.author = "remote".into();
        event
    };
    engine.apply_remote(&[event])?;
    assert!(lock(&observed).iter().any(|(path, _)| path == "/file"));
    let mut old = [0; 5];
    engine.read(h, 0, &mut old)?;
    assert_eq!(&old, b"local");
    engine.read_visible(h, 0, &mut old)?;
    assert_eq!(&old, b"other");
    assert_eq!(engine.info_visible(h)?.1, 5);
    let new = engine.open_file("file", false, false, false)?;
    engine.read(new, 0, &mut old)?;
    assert_eq!(&old, b"other");
    engine.close(h)?;
    engine.close(new)?;
    engine.shutdown()?;
    Ok(())
}

#[test]
fn resolving_a_conflict_seals_pending_local_edits_before_selecting_the_remote_head() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let settings = crate::orchestrator::settings::Settings {
        engine: engine.clone(),
    };
    let handle = engine.open_file("file", true, false, true)?;
    engine.write(handle, 0, b"base", false, false)?;
    engine.flush(Some(handle))?;
    let (entity, parent) = {
        let store = lock(&engine.store);
        let entity = store.lookup("file")?.id;
        let parent = store.basis(&entity)?.version;
        (entity, parent)
    };
    engine.write(handle, 0, b"mine", false, false)?;
    engine.flush(Some(handle))?;
    let remote = {
        let mut store = lock(&engine.store);
        let mut state = store.entry(&entity)?;
        state.content = Some(store.content.import(b"peer")?);
        let mut event = store.event(MAIN, state, vec![parent], Operation::Content);
        event.author = "peer".into();
        event
    };
    engine.apply_remote(std::slice::from_ref(&remote))?;
    engine.write(handle, 0, b"last", false, false)?;
    crate::orchestrator::control::execute(
        &engine,
        &settings,
        "resolve",
        &serde_json::json!({"entity":entity,"version":remote.id}),
    )?;
    {
        let store = lock(&engine.store);
        assert!(
            store.conflicts(false)?.is_empty(),
            "resolution left a new local conflict"
        );
        assert_eq!(store.lookup("file")?.content, remote.state.content);
        let last = crate::model::content::Digest::bytes(b"last");
        assert!(
            store
                .conflicts(true)?
                .iter()
                .flat_map(|c| &c.heads)
                .any(|e| e.state.content.as_ref().is_some_and(|c| c.digest == last)),
            "resolution lost the pending local alternative"
        );
    }
    engine.close(handle)?;
    engine.shutdown()?;
    drop(settings);
    drop(engine);
    let reopened = crate::runtime::Engine::open(root.path(), None)?;
    assert!(lock(&reopened.store).conflicts(false)?.is_empty());
    assert_eq!(reopened.lookup("file")?.content, remote.state.content);
    reopened.shutdown()?;
    Ok(())
}

#[test]
fn conflict_notifications_follow_the_visible_head_and_invalidate_both_resolution_names()
-> Result<()> {
    for deleted in [false, true] {
        let root = tempfile::tempdir()?;
        let engine = crate::runtime::Engine::open(root.path(), None)?;
        let settings = crate::orchestrator::settings::Settings {
            engine: engine.clone(),
        };
        let handle = engine.open_file("original", true, false, true)?;
        let (entity, parent) = {
            let store = lock(&engine.store);
            let entity = store.lookup("original")?.id;
            let parent = store.basis(&entity)?.version;
            (entity, parent)
        };
        engine.write(handle, 0, b"mine", false, false)?;
        engine.flush(Some(handle))?;
        let remote = {
            let mut store = lock(&engine.store);
            let mut state = store.entry(&entity)?;
            state.deleted = deleted;
            if !deleted {
                state.name = "renamed".into();
            }
            let operation = if deleted {
                Operation::Delete
            } else {
                Operation::Rename
            };
            let mut event = store.event(MAIN, state, vec![parent], operation);
            event.author = "peer".into();
            event
        };
        let observed = std::sync::Arc::new(Mutex::new(Vec::new()));
        let sink = observed.clone();
        engine.set_invalidator(std::sync::Arc::new(move |path, action| {
            lock(&sink).push((path.to_owned(), action));
            Ok(())
        }));
        engine.apply_remote(std::slice::from_ref(&remote))?;
        assert_eq!(engine.lookup("original")?.id, entity);
        assert!(
            !lock(&observed).iter().any(|(_, action)| *action == 2),
            "an unselected delete invalidated the visible local file as removed"
        );
        lock(&observed).clear();
        crate::orchestrator::control::execute(
            &engine,
            &settings,
            "resolve",
            &serde_json::json!({"entity":entity,"version":remote.id}),
        )?;
        assert!(
            lock(&observed).iter().any(|(path, _)| path == "/original"),
            "resolution left the previous pathname cached"
        );
        if deleted {
            assert!(engine.lookup("original").is_err());
            assert!(lock(&observed).contains(&("/original".into(), 2)));
        } else {
            assert!(engine.lookup("original").is_err());
            assert_eq!(engine.lookup("renamed")?.id, entity);
            assert!(lock(&observed).contains(&("/renamed".into(), 3)));
        }
        engine.close(handle)?;
        engine.shutdown()?;
    }
    Ok(())
}

#[test]
fn an_unreadable_import_source_cannot_truncate_or_create_the_destination() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let settings = crate::orchestrator::settings::Settings {
        engine: engine.clone(),
    };
    let handle = engine.open_file("existing", true, false, true)?;
    engine.write(handle, 0, b"keep these bytes", false, false)?;
    engine.flush(Some(handle))?;
    engine.close(handle)?;
    for (path, source) in [
        ("existing", root.path().join("absent")),
        ("new", root.path().join("absent")),
        ("existing", root.path().to_owned()),
        ("new", root.path().to_owned()),
    ] {
        assert!(
            crate::orchestrator::control::execute(
                &engine,
                &settings,
                "import",
                &serde_json::json!({"path":path,"source":source}),
            )
            .is_err()
        );
    }
    assert!(
        engine.lookup("new").is_err(),
        "failed import created an empty destination"
    );
    let handle = engine.open_file("existing", false, false, false)?;
    let mut bytes = [0; 16];
    assert_eq!(engine.read(handle, 0, &mut bytes)?, bytes.len());
    assert_eq!(&bytes, b"keep these bytes");
    engine.close(handle)?;
    engine.shutdown()?;
    Ok(())
}

#[test]
fn a_failed_resolution_flush_leaves_the_conflict_and_edit_intact() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let handle = engine.open_file("file", true, false, true)?;
    engine.write(handle, 0, b"base", false, false)?;
    engine.flush(Some(handle))?;
    let (entity, parent) = {
        let store = lock(&engine.store);
        let entity = store.lookup("file")?.id;
        let parent = store.basis(&entity)?.version;
        (entity, parent)
    };
    engine.write(handle, 0, b"mine", false, false)?;
    engine.flush(Some(handle))?;
    let remote = {
        let mut store = lock(&engine.store);
        let mut state = store.entry(&entity)?;
        state.content = Some(store.content.import(b"peer")?);
        let mut event = store.event(MAIN, state, vec![parent], Operation::Content);
        event.author = "peer".into();
        event
    };
    engine.apply_remote(std::slice::from_ref(&remote))?;
    let heads = lock(&engine.store).projection().heads[&entity].clone();
    engine.write(handle, 0, b"last", false, false)?;
    engine.inject_fault("flush");
    assert!(engine.resolve(&entity, &remote.id).is_err());
    assert_eq!(lock(&engine.store).projection().heads[&entity], heads);
    let mut bytes = [0; 4];
    engine.read(handle, 0, &mut bytes)?;
    assert_eq!(&bytes, b"last");
    engine.resolve(&entity, &remote.id)?;
    assert!(lock(&engine.store).conflicts(false)?.is_empty());
    engine.close(handle)?;
    engine.shutdown()?;
    Ok(())
}

#[test]
fn a_refused_large_cat_releases_its_read_handle() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let settings = crate::orchestrator::settings::Settings {
        engine: engine.clone(),
    };
    let handle = engine.open_file("large", true, false, true)?;
    engine.truncate(handle, 16 * 1024 * 1024 + 1, false)?;
    let scratch = engine.handle(handle)?.scratch.clone().unwrap();
    assert_eq!(lock(&scratch).handles, 1);
    let result = crate::orchestrator::control::execute(
        &engine,
        &settings,
        "cat",
        &serde_json::json!({"path":"large"}),
    );
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("USE_EXPORT_FOR_LARGE_FILES")
    );
    assert_eq!(
        lock(&scratch).handles,
        1,
        "refused cat retained an extra handle"
    );
    engine.close(handle)?;
    engine.shutdown()?;
    Ok(())
}

#[test]
fn failed_delete_cleanup_still_retires_the_writer_and_close_releases_the_handle() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let handle = engine.open_file("file", true, false, true)?;
    engine.write(handle, 0, b"retain on failed delete", false, false)?;
    let scratch = engine.handle(handle)?.scratch.clone().unwrap();
    {
        let mut store = lock(&engine.store);
        store.sync_metadata()?;
        store.db.execute_batch(
            "CREATE TRIGGER refuse_delete BEFORE INSERT ON events
             WHEN json_extract(NEW.event,'$.operation')='delete'
             BEGIN SELECT RAISE(ABORT,'test delete failure'); END;",
        )?;
    }
    engine
        .handle(handle)?
        .delete
        .store(true, std::sync::atomic::Ordering::Release);
    assert!(engine.cleanup(handle, false).is_err());
    assert_eq!(
        lock(&scratch).writers,
        0,
        "failed delete retained the closed writer"
    );
    assert!(engine.close(handle).is_err());
    assert!(
        engine.handle(handle).is_err(),
        "Close kept an unreachable native context"
    );
    assert_eq!(lock(&scratch).handles, 0);
    lock(&engine.store)
        .db
        .execute_batch("DROP TRIGGER refuse_delete")?;
    engine.shutdown()?;
    let entry = engine.lookup("file")?;
    let mut bytes = vec![0; 23];
    engine
        .working
        .content()
        .read_range(entry.content.as_ref().unwrap(), 0, &mut bytes)?;
    assert_eq!(bytes, b"retain on failed delete");
    Ok(())
}

#[test]
fn committed_notifications_retry_without_invalidating_echoed_events() -> Result<()> {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let h = engine.open_file("file", true, false, true)?;
    engine.close(h)?;
    // Drive retries explicitly so the test does not depend on thread timing.
    engine.stopping.store(true, Ordering::Release);
    let ready = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(AtomicUsize::new(0));
    let (r, c) = (ready.clone(), calls.clone());
    engine.set_invalidator(Arc::new(move |_, _| {
        c.fetch_add(1, Ordering::AcqRel);
        anyhow::ensure!(r.load(Ordering::Acquire), "NOTIFY_BUSY");
        Ok(())
    }));
    let event = {
        let mut store = lock(&engine.store);
        let state = store.lookup("file")?;
        let parent = store.basis(&state.id)?.version;
        let mut event = store.event(MAIN, state, vec![parent], Operation::Times);
        event.author = "peer".into();
        event
    };
    assert_eq!(
        engine
            .apply_remote(std::slice::from_ref(&event))?
            .as_slice(),
        std::slice::from_ref(&event.id)
    );
    assert_eq!(calls.load(Ordering::Acquire), 1);
    ready.store(true, Ordering::Release);
    engine.apply_remote(&[])?;
    assert_eq!(calls.load(Ordering::Acquire), 2);
    assert!(engine.apply_remote(&[event])?.is_empty());
    assert_eq!(calls.load(Ordering::Acquire), 2);
    engine.shutdown()?;
    Ok(())
}

#[test]
fn pathname_cache_refills_from_a_recreated_entity() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let first = engine.open_file("file", true, false, true)?;
    engine.write(first, 0, b"old", false, false)?;
    engine.flush(Some(first))?;
    let old_id = engine.lookup("file")?.id;
    lock(&engine.store).delete(&old_id)?;
    let next = engine.open_file("file", true, false, true)?;
    engine.write(next, 0, b"recreated bytes", false, false)?;
    engine.flush(Some(next))?;
    let mut bytes = [0; 15];
    assert_eq!(engine.read_visible(first, 0, &mut bytes)?, 15);
    assert_eq!(&bytes, b"recreated bytes");
    assert_eq!(engine.info_visible(first)?.1, 15);
    assert_ne!(engine.info_visible(first)?.0.id, old_id);
    assert_eq!(engine.read(first, 0, &mut bytes)?, 3);
    assert_eq!(&bytes[..3], b"old");
    engine.close(first)?;
    engine.close(next)?;
    engine.shutdown()?;
    Ok(())
}

#[test]
fn paging_write_on_a_retained_reader_updates_the_recreated_entity() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let first = engine.open_file("file", true, false, true)?;
    engine.write(first, 0, b"old", false, false)?;
    engine.flush(Some(first))?;
    engine.close(first)?;
    let retained = engine.open_file("file", false, false, false)?;
    let old_id = engine.lookup("file")?.id;
    lock(&engine.store).delete(&old_id)?;
    let writer = engine.open_file("file", true, false, true)?;
    let current_id = engine.lookup("file")?.id;
    engine.truncate(writer, 23, false)?;
    // WinFsp extends the new writer's scratch, but Cache Manager delivers the
    // paging write (and may deliver Flush) through the old reader's context.
    assert_eq!(
        engine.write_native(retained, 0, b"edited after recreation", false, true)?,
        23
    );
    engine.flush_native_file(retained)?;
    assert_eq!(
        engine.lookup("file")?.content.unwrap().digest,
        crate::model::content::Digest::bytes(b"edited after recreation")
    );
    assert_eq!(engine.lookup("file")?.id, current_id);
    assert_eq!(engine.info_visible(retained)?.1, 23);
    // Ordinary writes on the retained read-only context remain forbidden.
    assert!(
        engine
            .write_native(retained, 0, b"x", false, false)
            .is_err()
    );
    let mut old = [0; 3];
    engine.read(retained, 0, &mut old)?;
    assert_eq!(&old, b"old");
    engine.close(retained)?;
    engine.close(writer)?;
    engine.shutdown()?;
    Ok(())
}

#[test]
fn native_cache_handles_keep_a_renamed_entity_when_its_old_name_is_reused() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let writer = engine.open_file("before", true, false, true)?;
    engine.write(writer, 0, b"base", false, false)?;
    engine.flush(Some(writer))?;
    let reader = engine.open_file("before", false, false, false)?;
    engine.rename(writer, "after", false)?;
    let other = engine.open_file("before", true, false, true)?;
    engine.write(other, 0, b"new", false, false)?;
    engine.flush(Some(other))?;
    assert_eq!(engine.write_native(reader, 0, b"edit", false, true)?, 4);
    engine.flush_native_file(reader)?;
    assert_eq!(
        engine.lookup("after")?.content.unwrap().digest,
        crate::model::content::Digest::bytes(b"edit")
    );
    assert_eq!(
        engine.lookup("before")?.content.unwrap().digest,
        crate::model::content::Digest::bytes(b"new")
    );
    assert_eq!(
        engine.info_visible(reader)?.0.id,
        engine.lookup("after")?.id
    );
    engine.close(reader)?;
    engine.close(writer)?;
    engine.close(other)?;
    engine.shutdown()?;
    Ok(())
}

#[test]
fn native_paging_writes_refresh_clean_readers_but_preserve_a_local_edit_parent() -> Result<()> {
    for local_edit in [false, true] {
        let root = tempfile::tempdir()?;
        let engine = crate::runtime::Engine::open(root.path(), None)?;
        let initial = engine.open_file("file", true, false, true)?;
        engine.write(initial, 0, b"base", false, false)?;
        engine.flush(Some(initial))?;
        engine.close(initial)?;
        let reader = engine.open_file("file", false, false, false)?;
        let mut writer = None;
        if local_edit {
            let h = engine.open_file("file", false, false, true)?;
            engine.write(h, 0, b"mine", false, false)?;
            writer = Some(h);
        }
        let event = {
            let mut store = lock(&engine.store);
            let mut state = store.lookup("file")?;
            let parent = store.basis(&state.id)?.version;
            state.content = Some(store.content.import(b"peer")?);
            let mut event = store.event(MAIN, state, vec![parent], Operation::Content);
            event.author = "peer".into();
            event
        };
        engine.apply_remote(&[event])?;
        if !local_edit {
            writer = Some(engine.open_file("file", false, false, true)?);
        }
        engine.write_native(reader, 0, b"edit", false, true)?;
        engine.flush_native_file(reader)?;
        if local_edit {
            assert!(!lock(&engine.store).conflicts(false)?.is_empty());
        } else {
            assert_eq!(
                engine.lookup("file")?.content.unwrap().digest,
                crate::model::content::Digest::bytes(b"edit")
            );
            let mut old = [0; 4];
            engine.read(reader, 0, &mut old)?;
            assert_eq!(&old, b"base");
        }
        engine.close(reader)?;
        engine.close(writer.unwrap())?;
        engine.shutdown()?;
    }
    Ok(())
}

#[test]
fn native_file_flush_preserves_an_edit_opened_before_checkout() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let writer = engine.open_file("file", true, false, true)?;
    engine.write(writer, 0, b"base", false, false)?;
    engine.flush(Some(writer))?;
    let branch = {
        let mut store = lock(&engine.store);
        let snapshot = store.checkpoint()?;
        store.fork(&snapshot, "other")?
    };
    engine.write(writer, 0, b"mine", false, false)?;
    lock(&engine.store).checkout(&branch)?;
    engine.flush_native_file(writer)?;
    assert_eq!(
        engine.lookup("file")?.content.unwrap().digest,
        crate::model::content::Digest::bytes(b"base")
    );
    lock(&engine.store).checkout(MAIN)?;
    assert_eq!(
        engine.lookup("file")?.content.unwrap().digest,
        crate::model::content::Digest::bytes(b"mine")
    );
    engine.close(writer)?;
    engine.shutdown()?;
    Ok(())
}

#[test]
fn native_file_flush_finds_a_replacement_draft_without_sealing_other_files() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let target = engine.open_file("target", true, false, true)?;
    engine.write(target, 0, b"base", false, false)?;
    engine.flush(Some(target))?;
    let reader = engine.open_file("target", false, false, false)?;
    engine.close(target)?;
    let unrelated = engine.open_file("unrelated", true, false, true)?;
    engine.write(unrelated, 0, b"private", false, false)?;
    let temp = engine.open_file("temp", true, false, true)?;
    engine.write(temp, 0, b"save", false, false)?;
    engine.rename(temp, "target", true)?;
    engine.write(temp, 0, b"next", false, false)?;
    // Flush may use the retained reader's cache context after replacement.
    engine.flush_native_file(reader)?;
    assert_eq!(
        engine.lookup("target")?.content.unwrap().digest,
        crate::model::content::Digest::bytes(b"next")
    );
    assert_eq!(engine.lookup("unrelated")?.content.unwrap().total_len, 0);
    assert!(lock(engine.handle(unrelated)?.scratch.as_ref().unwrap()).changed);
    engine.close(temp)?;
    engine.close(reader)?;
    engine.close(unrelated)?;
    engine.shutdown()?;
    Ok(())
}

#[test]
fn fsync_uses_a_durable_wal_and_reports_an_incomplete_metadata_barrier() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let settings = crate::orchestrator::settings::Settings {
        engine: engine.clone(),
    };
    lock(&engine.store)
        .db
        .busy_timeout(std::time::Duration::from_millis(25))?;
    engine.flush(None)?;
    let reader = rusqlite::Connection::open(root.path().join("metadata.sqlite"))?;
    reader.execute_batch("BEGIN; SELECT value FROM config WHERE key='clock';")?;
    let writer = engine.open_file("file", true, false, true)?;
    engine.write(writer, 0, b"durable in the WAL", false, false)?;
    // This reader prevents a FULL checkpoint. File fsync must still succeed:
    // the seal commits its event and receipt with FULL WAL synchronization.
    engine.flush_native_file(writer)?;
    engine.flush(None)?;
    reader.execute_batch("ROLLBACK;")?;

    for change in ["namespace", "catalog", "schema"] {
        reader.execute_batch("BEGIN; SELECT value FROM config WHERE key='clock';")?;
        match change {
            "namespace" => {
                let mut store = lock(&engine.store);
                let entity = store.lookup("file")?.id;
                store.rename(&entity, "renamed", false)?;
            }
            "catalog" => settings.set("durability-test", "value")?,
            "schema" => lock(&engine.store)
                .db
                .execute_batch("CREATE TABLE sync_probe(value);")?,
            _ => unreachable!(),
        }
        // NORMAL commits after the last seal still need a durability barrier.
        // SQLite returns a busy result row rather than an SQL error when the
        // reader blocks it; fsync must report failure instead of claiming sync.
        let error = engine.flush(None).unwrap_err();
        assert!(format!("{error:#}").contains("METADATA_SYNC_INCOMPLETE"));
        reader.execute_batch("ROLLBACK;")?;
        engine.flush(None)?;
    }
    assert_eq!(
        engine.lookup("renamed")?.content.unwrap().digest,
        crate::model::content::Digest::bytes(b"durable in the WAL")
    );
    assert_eq!(settings.get("durability-test")?.as_deref(), Some("value"));
    engine.close(writer)?;
    engine.shutdown()?;
    Ok(())
}

#[test]
fn bound_lookup_reuses_its_locally_saved_draft_after_remote_apply() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let h = engine.open_file("file", true, false, true)?;
    engine.write(h, 0, b"first", false, false)?;
    engine.flush(Some(h))?;
    let (entry, basis) = {
        let store = lock(&engine.store);
        let entry = store.lookup("file")?;
        let basis = store.basis(&entry.id)?;
        (entry, basis)
    };
    let alias = engine.open_bound(entry.clone(), basis.clone(), false)?;
    engine.close(alias)?;
    engine.write(h, 0, b"local", false, false)?;
    engine.flush(Some(h))?;
    engine.close(h)?;
    let event = {
        let mut store = lock(&engine.store);
        let mut state = store.lookup("file")?;
        let parent = store.basis(&state.id)?.version;
        state.content = Some(store.content.import(b"other")?);
        let mut event = store.event(MAIN, state, vec![parent], Operation::Content);
        event.author = "peer".into();
        event
    };
    engine.apply_remote(&[event])?;
    let old = engine.open_bound(entry, basis, false)?;
    let new = engine.open_file("file", false, false, false)?;
    let mut bytes = [0; 5];
    engine.read(old, 0, &mut bytes)?;
    assert_eq!(&bytes, b"local");
    engine.read(new, 0, &mut bytes)?;
    assert_eq!(&bytes, b"other");
    engine.close(old)?;
    engine.close(new)?;
    engine.shutdown()?;
    Ok(())
}

#[test]
fn hydrate_missing_chunk_through_worker_demand_and_object_ops() -> Result<()> {
    use crate::content::Kind;
    use crate::orchestrator::{
        demand::{Demand, Seq},
        sync::{FeedPage, Service},
    };
    use std::{
        sync::{Arc, mpsc},
        time::{Duration, Instant},
    };
    let aroot = tempfile::tempdir()?;
    let broot = tempfile::tempdir()?;
    let a = crate::runtime::Engine::open(aroot.path(), Some("repo"))?;
    let b = crate::runtime::Engine::open(broot.path(), Some("repo"))?;
    let h = a.open_file("remote", true, false, true)?;
    let mut payload = vec![9; 65536];
    payload.extend_from_slice(b"tail");
    a.write(h, 0, &payload, false, false)?;
    a.flush(Some(h))?;
    a.close(h)?;
    let service = |engine: &Arc<crate::runtime::Engine>| Service {
        engine: engine.clone(),
        demand: Demand::new(Seq::new(), engine.working.content().packs.clone()),
    };
    let (source, target) = (service(&a), service(&b));
    // The worker's fetcher is its demand, as in `worker::run`.
    b.working.content().set_fetcher(target.demand.clone());
    let page: FeedPage = serde_json::from_slice(&source.handle_request(
        "feed.outgoing",
        &serde_json::json!({"after":0}),
        &[],
    )?)?;
    assert!(!page.more);
    target.handle_request(
        "feed.apply",
        &serde_json::json!({}),
        &serde_json::to_vec(&page.events)?,
    )?;
    let content = b.lookup("remote")?.content.unwrap();
    assert!(!b.working.content().packs.contains(content.digest)?);
    // A reader of the tail blocks on the worker's demand...
    let (sent, received) = mpsc::channel();
    let reader = {
        let b = b.clone();
        std::thread::spawn(move || {
            let result = (|| -> Result<Vec<u8>> {
                let h = b.open_file("remote", false, false, false)?;
                let mut tail = [0; 4];
                let read = b.read(h, 65536, &mut tail);
                b.close(h)?;
                read?;
                Ok(tail.to_vec())
            })();
            let _ = sent.send(result);
        })
    };
    // ...and the orchestrator's part supplies exactly what is demanded.
    let end = Instant::now() + Duration::from_secs(10);
    let mut supplied = Vec::new();
    let tail = loop {
        match received.recv_timeout(Duration::from_millis(10)) {
            Ok(result) => break result?,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(error) => anyhow::bail!("the reader ended without a result: {error}"),
        }
        anyhow::ensure!(
            Instant::now() < end,
            "the tail read was not satisfied in 10 s"
        );
        let pending = target.demand.pending();
        if pending.is_empty() {
            continue;
        }
        let objects = source.handle_request(
            "objects.fetch",
            &serde_json::json!({"repo":"repo"}),
            &serde_json::to_vec(&pending)?,
        )?;
        target.handle_request(
            "objects.write",
            &serde_json::json!({"repo":"repo"}),
            &objects,
        )?;
        supplied.extend(pending);
    };
    reader.join().unwrap();
    assert_eq!(tail, b"tail");
    let chunks = b.working.content().chunks(&content)?;
    assert_eq!(
        supplied,
        vec![(content.digest, Kind::Manifest), (chunks[1], Kind::Chunk)],
        "only the manifest and the read chunk are fetched"
    );
    assert!(!b.working.content().packs.contains(chunks[0])?);
    assert!(b.working.content().packs.contains(chunks[1])?);
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

#[test]
fn metadata_changes_share_one_draft_and_atomic_replace_keeps_target_entity() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let original = engine.open_file("file", true, false, true)?;
    engine.write(original, 0, b"original", false, false)?;
    engine.basic(
        original,
        u32::MAX,
        [0, 0, crate::core::Times::default().modified, 0],
    )?;
    engine.close(original)?;
    let next = engine.open_file("file", false, false, true)?;
    engine.write(next, 3, b"XY", false, false)?;
    engine.flush(Some(next))?;
    engine.close(next)?;
    engine.flush(None)?;
    let current = engine.open_file("file", false, false, false)?;
    let mut bytes = [0; 8];
    engine.read(current, 0, &mut bytes)?;
    assert_eq!(&bytes, b"oriXYnal");
    engine.close(current)?;
    let target = engine.lookup("file")?.id;
    let temp = engine.open_file("temporary", true, false, true)?;
    engine.write(temp, 0, b"replacement", false, false)?;
    engine.cleanup(temp, false)?;
    engine.rename(temp, "file", true)?;
    engine.close(temp)?;
    assert_eq!(engine.lookup("file")?.id, target);
    assert!(engine.lookup("temporary").is_err());
    let read = engine.open_file("file", false, false, false)?;
    let mut bytes = [0; 11];
    engine.read(read, 0, &mut bytes)?;
    assert_eq!(&bytes, b"replacement");
    engine.close(read)?;
    engine.shutdown()?;
    Ok(())
}

#[test]
fn replacing_a_target_with_an_unsealed_draft_keeps_one_local_history() -> Result<()> {
    // git's config writes: the first lock file is renamed into place inside
    // the write-back window, then each later lock file replaces it.
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let first = engine.open_file("config.lock", true, false, true)?;
    engine.write(first, 0, b"[core]\n", false, false)?;
    engine.cleanup(first, false)?;
    engine.rename(first, "config", false)?;
    engine.close(first)?;
    let target = engine.lookup("config")?.id;
    let second = engine.open_file("config.lock", true, false, true)?;
    engine.write(second, 0, b"[core]\n\tbare = false\n", false, false)?;
    engine.cleanup(second, false)?;
    engine.rename(second, "config", true)?;
    engine.close(second)?;
    engine.flush(None)?;
    assert!(lock(&engine.store).conflicts(true)?.is_empty());
    let config = engine.lookup("config")?;
    assert_eq!(config.id, target);
    assert_eq!(
        config.content.unwrap().digest,
        crate::model::content::Digest::bytes(b"[core]\n\tbare = false\n")
    );
    // The replaced bytes stay in the entity's history.
    let replaced = crate::model::content::Digest::bytes(b"[core]\n");
    let store = lock(&engine.store);
    let mut ancestors = vec![store.projection().visible[&target].clone()];
    let mut retained = false;
    while let Some(id) = ancestors.pop() {
        let event = store.version(&id)?.unwrap();
        retained |= event.state.content.is_some_and(|c| c.digest == replaced);
        ancestors.extend(event.parents);
    }
    assert!(retained);
    drop(store);
    engine.shutdown()?;
    Ok(())
}

#[test]
fn local_metadata_after_remote_content_does_not_erase_the_edit_conflict() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let h = engine.open_file("file", true, false, true)?;
    engine.write(h, 0, b"base", false, false)?;
    engine.flush(Some(h))?;
    engine.write(h, 0, b"mine", false, false)?;
    let event = {
        let mut s = lock(&engine.store);
        let e = s.lookup("file")?;
        let basis = s.basis(&e.id)?;
        let mut state = e;
        state.content = Some(s.content.import(b"peer")?);
        let mut event = s.event(MAIN, state, vec![basis.version], Operation::Content);
        event.author = "other-peer".into();
        event
    };
    engine.apply_remote(&[event])?;
    engine.basic(
        h,
        u32::MAX,
        [0, 0, crate::core::Times::default().modified, 0],
    )?;
    engine.flush(Some(h))?;
    assert_eq!(lock(&engine.store).conflicts(false)?.len(), 1);
    engine.close(h)?;
    engine.shutdown()?;
    Ok(())
}

#[test]
fn a_late_seal_of_a_deleted_entity_cannot_remove_a_recreated_name() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut store = Store::open(root.path(), None)?;
    let old = store.create("file", EntryKind::File)?;
    store.delete(&old.id)?;
    let current = store.create("file", EntryKind::File)?;
    let mut old = store.entry(&old.id)?;
    old.content = Some(store.content.import(b"old private bytes")?);
    let parent = store.projection().visible[&old.id].clone();
    let event = store.event(MAIN, old, vec![parent], Operation::Delete);
    store.apply(&[event])?;
    assert_eq!(store.lookup("file")?.id, current.id);
    Ok(())
}

#[test]
fn paging_writes_are_accepted_on_a_read_only_handle() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let writer = engine.open_file("file", true, false, true)?;
    engine.write(writer, 0, b"12345", false, false)?;
    let reader = engine.open_file("file", false, false, false)?;
    // An ordinary write through a read-only handle is still refused.
    let refused = engine.write(reader, 0, b"abc", false, false);
    assert!(format!("{:#}", refused.unwrap_err()).contains("ACCESS_DENIED"));
    // The Cache Manager writes back through whichever file object initialised
    // caching; WinFsp checked access at Create, so paging writes must land.
    assert_eq!(engine.write(reader, 0, b"abc", false, true)?, 3);
    // Constrained writes never extend the file.
    assert_eq!(engine.write(reader, 3, b"XYZW", false, true)?, 2);
    let mut bytes = [0; 8];
    assert_eq!(engine.read(writer, 0, &mut bytes)?, 5);
    assert_eq!(&bytes[..5], b"abcXY");
    engine.close(reader)?;
    engine.close(writer)?;
    engine.shutdown()?;
    Ok(())
}

#[test]
fn a_paging_write_after_cleanup_is_sealed_again() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let h = engine.open_file("file", true, false, true)?;
    engine.write(h, 0, b"a", false, false)?;
    engine.cleanup(h, false)?;
    engine.flush(None)?;
    assert_eq!(
        engine.lookup("file")?.content.unwrap().digest,
        crate::model::content::Digest::bytes(b"a")
    );
    // A mapped section can write back between Cleanup and Close.
    engine.write(h, 0, b"b", false, true)?;
    engine.flush(None)?;
    assert_eq!(
        engine.lookup("file")?.content.unwrap().digest,
        crate::model::content::Digest::bytes(b"b")
    );
    engine.close(h)?;
    engine.shutdown()?;
    Ok(())
}

#[test]
fn a_failed_flush_still_stops_the_daemon_and_keeps_recoverable_scratch() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let settings = crate::orchestrator::settings::Settings {
        engine: engine.clone(),
    };
    let h = engine.open_file("file", true, false, true)?;
    engine.write(h, 0, b"unsaved", false, false)?;
    let scratch_path = lock(engine.handle(h)?.scratch.as_ref().unwrap())
        .path
        .clone();
    engine.inject_fault("flush");
    let result = crate::orchestrator::control::execute(
        &engine,
        &settings,
        "shutdown",
        // `h` is still open for writing; an unforced stop is refused.
        &serde_json::json!({"force": true}),
    );
    let error = format!("{:#}", result.unwrap_err());
    assert!(error.contains("FAULT_INJECTED: flush"), "{error}");
    assert!(engine.stopping.load(std::sync::atomic::Ordering::Acquire));
    assert!(
        *lock(&engine.error) == Some(error.clone()),
        "{:?}",
        lock(&engine.error)
    );
    assert!(scratch_path.exists(), "recoverable scratch was removed");
    // A successful final retry must not turn the failed stop into exit code 0.
    let error = format!("{:#}", engine.shutdown().unwrap_err());
    assert!(error.contains("FAULT_INJECTED: flush"), "{error}");
    assert!(scratch_path.exists(), "recoverable scratch was removed");
    Ok(())
}

#[test]
fn daemon_startup_failure_still_shuts_down_the_engine() -> Result<()> {
    let root = tempfile::tempdir()?;
    let engine = crate::runtime::Engine::open(root.path(), None)?;
    let h = engine.open_file("file", true, false, true)?;
    engine.write(h, 0, b"save on failed startup", false, false)?;
    // A directory where the token file goes makes the runtime files fail.
    std::fs::create_dir(root.path().join("runtime.token"))?;
    std::fs::write(root.path().join("runtime.json"), "stale")?;
    let result = crate::orchestrator::run(
        engine.clone(),
        crate::orchestrator::worker::Options {
            supervised: false,
            mount: None,
            on_ready: None,
        },
    );
    let fatal = result
        .unwrap_err()
        .downcast::<crate::orchestrator::worker::Fatal>()?;
    assert_eq!(fatal.exit, crate::orchestrator::worker::EXIT_STORE);
    assert!(engine.stopping.load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(
        engine.lookup("file")?.content.unwrap().digest,
        crate::model::content::Digest::bytes(b"save on failed startup")
    );
    for file in ["runtime.json", "worker.json", "control.json"] {
        assert!(!root.path().join(file).exists(), "{file}");
    }
    Ok(())
}

#[test]
fn failed_shutdown_retries_preserve_scratch_for_startup_recovery() -> Result<()> {
    let root = tempfile::tempdir()?;
    {
        let engine = crate::runtime::Engine::open(root.path(), None)?;
        let h = engine.open_file("file", true, false, true)?;
        engine.write(h, 0, b"recover after failed shutdown", false, false)?;
        engine.inject_fault("flush-persistent");
        assert!(engine.request_shutdown().is_err());
        assert!(engine.shutdown().is_err());
    }
    // The background sealer holds an Arc briefly; bound owner-lock retries.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let recovered = loop {
        match crate::runtime::Engine::open(root.path(), None) {
            Ok(engine) => break engine,
            Err(error) if std::time::Instant::now() < deadline => {
                assert!(format!("{error:#}").contains("STORE_ALREADY_OPEN"));
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(error) => return Err(error),
        }
    };
    assert_eq!(
        recovered.lookup("file")?.content.unwrap().digest,
        crate::model::content::Digest::bytes(b"recover after failed shutdown")
    );
    recovered.shutdown()?;
    Ok(())
}

/// A file this store knows by name, with its bytes on a peer: only the first `held` of its
/// `chunks` chunks are here, and its manifest only if `manifest` says so. A file of one chunk
/// has no manifest. Returns the entity and the bytes of each chunk.
fn seed_file_on_a_peer(
    store: &mut Store,
    name: &str,
    chunks: u64,
    held: u64,
    manifest: bool,
) -> Result<(String, Vec<Vec<u8>>)> {
    use crate::content::{Kind, Manifest};
    use crate::model::content::{CHUNK_SIZE, ContentKind, Digest, FileContentRef};
    let mut state = store.create(name, EntryKind::File)?;
    let entity = state.id.clone();
    let parent = store.projection().visible[&entity].clone();
    let mut bytes = Vec::new();
    let mut digests = Vec::new();
    for index in 0..chunks {
        let mut chunk = vec![index as u8 + 1; CHUNK_SIZE as usize];
        chunk[..name.len()].copy_from_slice(name.as_bytes());
        digests.push(if index < held {
            store.content.packs.put(Kind::Chunk, &chunk)?
        } else {
            Digest::bytes(&chunk)
        });
        bytes.push(chunk);
    }
    let len = chunks * CHUNK_SIZE;
    let reference = if chunks == 1 {
        FileContentRef::new(ContentKind::Direct, digests[0], len)?
    } else {
        let encoded = Manifest {
            len,
            chunks: digests,
        }
        .encode()?;
        let digest = if manifest {
            store.content.packs.put(Kind::Manifest, &encoded)?
        } else {
            Digest::bytes(&encoded)
        };
        FileContentRef::new(ContentKind::Manifest, digest, len)?
    };
    state.content = Some(reference);
    store.content.packs.flush()?;
    let event = store.event(
        &store.active.clone(),
        state,
        vec![parent],
        Operation::Content,
    );
    store.apply(&[event])?;
    store.sync_metadata()?;
    Ok((entity, bytes))
}

/// What a real restart does: a new store and working layer, and no fetcher, which a worker
/// only gets once its engine is open.
fn recover_after_a_crash(root: &std::path::Path) -> Result<(Mutex<Store>, WorkingLayer, usize)> {
    let store = Mutex::new(Store::open(root, None)?);
    let layer = WorkingLayer::new(root, lock(&store).content.clone())?;
    let recovered = recover(&store, &layer)?;
    Ok((store, layer, recovered))
}

fn file_bytes(store: &Store, name: &str) -> Result<Vec<u8>> {
    let content = store.lookup(name)?.content.unwrap();
    let mut bytes = vec![0; content.total_len as usize];
    store.content.read_range(&content, 0, &mut bytes)?;
    Ok(bytes)
}

/// A phone edits one spot of a large file and dies before the seal. Sealing must not need the
/// chunks it never touched: at this point nothing can fetch them, and a failure here would keep
/// the folder from ever opening again.
#[test]
fn a_draft_over_chunks_that_are_not_here_recovers_without_them() -> Result<()> {
    use crate::model::content::{CHUNK_SIZE, Digest};
    let root = tempfile::tempdir()?;
    let (entity, chunks) = {
        let mut metadata = Store::open(root.path(), None)?;
        let (entity, chunks) = seed_file_on_a_peer(&mut metadata, "big", 4, 1, true)?;
        let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
        let draft = layer.open(metadata.basis(&entity)?, true)?;
        lock(&draft).write(layer.content(), 10, b"edit", false, false)?;
        (entity, chunks)
    };
    let (store, _layer, recovered) = recover_after_a_crash(root.path())?;
    assert_eq!(recovered, 1);
    let metadata = lock(&store);
    let content = metadata.entry(&entity)?.content.unwrap();
    assert_eq!(content.total_len, 4 * CHUNK_SIZE);
    let mut first = chunks[0].clone();
    first[10..14].copy_from_slice(b"edit");
    let digests = metadata.content.chunks(&content)?;
    assert_eq!(digests[0], Digest::bytes(&first));
    for index in 1..4 {
        assert_eq!(
            digests[index],
            Digest::bytes(&chunks[index]),
            "chunk {index} is not the one the file had"
        );
    }
    Ok(())
}

/// `wt` with the PC away: nothing of the old file is needed, and nothing can be fetched.
#[test]
fn replacing_a_file_never_downloaded_asks_for_nothing() -> Result<()> {
    use crate::runtime::Engine;
    for (name, chunks) in [("direct", 1), ("manifest", 4)] {
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), None)?;
        seed_file_on_a_peer(&mut lock(&engine.store), name, chunks, 0, false)?;
        let token = engine.open_to_replace(name)?;
        engine.truncate(token, 0, false)?;
        assert_eq!(engine.info(token)?.1, 0);
        assert_eq!(engine.write(token, 0, b"new", false, false)?, 3);
        engine.flush(Some(token))?;
        engine.write(token, 3, b" bytes", false, false)?;
        engine.flush(Some(token))?;
        engine.close(token)?;
        assert_eq!(file_bytes(&lock(&engine.store), name)?, b"new bytes");
        engine.shutdown()?;
    }
    Ok(())
}

//// The same, dying before the seal, for a new file shorter than, as long as and longer than the
/// old one, and when the draft's timestamp cannot tell it from a clean one. Nothing says its writer
/// finished, so what it wrote goes beside the file, which stays as it was.
#[test]
fn a_replacement_killed_before_its_seal_recovers_beside_the_file() -> Result<()> {
    let old_len = 4 * crate::model::content::CHUNK_SIZE as usize;
    for len in [5, old_len, old_len + 5] {
        for coarse_clock in [false, true] {
            let root = tempfile::tempdir()?;
            let bytes = vec![7; len];
            let before = {
                let mut metadata = Store::open(root.path(), None)?;
                let (entity, _) = seed_file_on_a_peer(&mut metadata, "big", 4, 0, false)?;
                let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
                let basis = metadata.basis(&entity)?;
                let draft = layer.open_to_replace(basis.clone())?;
                let mut draft = lock(&draft);
                draft.truncate(0)?;
                draft.write(layer.content(), 0, &bytes, false, false)?;
                if coarse_clock {
                    draft.record.modified_ns = modified_ns(draft.file()?)? + 86_400_000_000_000;
                    draft.persist_record()?;
                }
                basis
            };
            let (store, layer, recovered) = recover_after_a_crash(root.path())?;
            assert_eq!(recovered, 1, "{len} bytes, coarse clock {coarse_clock}");
            assert_eq!(file_bytes(&lock(&store), "big (recovered)")?, bytes);
            let after = lock(&store).basis(&before.entity)?;
            assert_eq!(after.version, before.version, "the file is as it was");
            assert_eq!(after.content, before.content);
            assert_eq!(drafts_on_disk(root.path())?, 0);
            drop((store, layer));
            let (store, _layer, again) = recover_after_a_crash(root.path())?;
            assert_eq!(again, 0);
            assert_eq!(lock(&store).projection().children(ROOT).len(), 2);
        }
    }
    Ok(())
}

/// Dying inside the first write of a replacement, on either side of the truncate's durable record:
/// the store still opens, the file is as it was, and with nothing written there is no new file.
#[test]
fn a_kill_inside_the_first_write_of_a_replacement_leaves_a_store_that_opens() -> Result<()> {
    for record_written in [true, false] {
        let root = tempfile::tempdir()?;
        let before = {
            let mut metadata = Store::open(root.path(), None)?;
            let (entity, _) = seed_file_on_a_peer(&mut metadata, "big", 4, 0, false)?;
            let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
            let basis = metadata.basis(&entity)?;
            let draft = layer.open_to_replace(basis.clone())?;
            let mut draft = lock(&draft);
            draft.truncate(0)?;
            if !record_written {
                draft.record.base_visible_len = Some(4 * crate::model::content::CHUNK_SIZE);
                draft.persist_record()?;
            }
            basis
        };
        let (store, _layer, recovered) = recover_after_a_crash(root.path())?;
        assert_eq!(recovered, 0, "record written: {record_written}");
        assert_eq!(lock(&store).basis(&before.entity)?.version, before.version);
        assert_eq!(lock(&store).projection().children(ROOT).len(), 1);
        assert_eq!(drafts_on_disk(root.path())?, 0);
    }
    Ok(())
}

/// A name that is taken gets the first free number, and the new file goes where the old one is.
#[test]
fn a_killed_replacement_takes_the_first_free_recovered_name() -> Result<()> {
    let root = tempfile::tempdir()?;
    {
        let mut metadata = Store::open(root.path(), None)?;
        metadata.create("sub", EntryKind::Directory)?;
        let (entity, _) = seed_file_on_a_peer(&mut metadata, "sub/notes.txt", 1, 1, false)?;
        metadata.create("sub/NOTES (RECOVERED).txt", EntryKind::File)?;
        metadata.create("sub/notes (recovered) (1).txt", EntryKind::File)?;
        let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
        let draft = layer.open_to_replace(metadata.basis(&entity)?)?;
        let mut draft = lock(&draft);
        draft.truncate(0)?;
        draft.write(layer.content(), 0, b"half", false, false)?;
    }
    let (store, _layer, recovered) = recover_after_a_crash(root.path())?;
    assert_eq!(recovered, 1);
    assert_eq!(
        file_bytes(&lock(&store), "sub/notes (recovered) (2).txt")?,
        b"half"
    );
    assert_eq!(
        lock(&store)
            .lookup("sub/notes.txt")?
            .content
            .unwrap()
            .total_len,
        crate::model::content::CHUNK_SIZE
    );
    Ok(())
}

/// The file and its directory went while the draft waited: what the writer wrote still has a place.
#[test]
fn a_killed_replacement_of_a_file_that_went_away_still_recovers() -> Result<()> {
    let root = tempfile::tempdir()?;
    {
        let mut metadata = Store::open(root.path(), None)?;
        let directory = metadata.create("sub", EntryKind::Directory)?;
        let (entity, _) = seed_file_on_a_peer(&mut metadata, "sub/notes.txt", 1, 1, false)?;
        let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
        let draft = layer.open_to_replace(metadata.basis(&entity)?)?;
        {
            let mut draft = lock(&draft);
            draft.truncate(0)?;
            draft.write(layer.content(), 0, b"half", false, false)?;
        }
        metadata.delete(&entity)?;
        metadata.delete(&directory.id)?;
        metadata.sync_metadata()?;
    }
    let (store, _layer, recovered) = recover_after_a_crash(root.path())?;
    assert_eq!(recovered, 1);
    assert_eq!(file_bytes(&lock(&store), "notes (recovered).txt")?, b"half");
    Ok(())
}

#[test]
fn a_recovered_name_keeps_the_extension_and_fits() -> Result<()> {
    let free = std::collections::BTreeSet::new();
    for (name, want) in [
        ("notes.txt", "notes (recovered).txt"),
        ("archive.tar.gz", "archive.tar (recovered).gz"),
        ("plain", "plain (recovered)"),
        (".hidden", ".hidden (recovered)"),
        ("ends.", "ends. (recovered)"),
    ] {
        assert_eq!(recovered_name(name, &free)?, want);
    }
    let long = format!("{}.txt", "é".repeat(251));
    let fitted = recovered_name(&long, &free)?;
    assert!(name_key(&fitted).is_ok(), "{fitted}");
    assert!(fitted.ends_with(" (recovered).txt"), "{fitted}");
    let dotted = format!("a.{}", "b".repeat(253));
    assert!(name_key(&recovered_name(&dotted, &free)?).is_ok());
    Ok(())
}

/// A new or empty file has nothing to lose, so its draft recovers under its own name as any other.
#[test]
fn a_killed_replacement_of_an_empty_file_recovers_under_its_name() -> Result<()> {
    let root = tempfile::tempdir()?;
    {
        let mut metadata = Store::open(root.path(), None)?;
        let entry = metadata.create("empty.txt", EntryKind::File)?;
        let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
        let draft = layer.open_to_replace(metadata.basis(&entry.id)?)?;
        let mut draft = lock(&draft);
        draft.truncate(0)?;
        draft.write(layer.content(), 0, b"first bytes", false, false)?;
    }
    let (store, _layer, recovered) = recover_after_a_crash(root.path())?;
    assert_eq!(recovered, 1);
    assert_eq!(file_bytes(&lock(&store), "empty.txt")?, b"first bytes");
    assert_eq!(lock(&store).projection().children(ROOT).len(), 1);
    Ok(())
}

/// Only `open_to_replace` says so: the same cut through an ordinary writer is an ordinary edit, and
/// recovers under its name as it does on a desktop.
#[test]
fn a_cut_through_an_ordinary_writer_recovers_under_its_name() -> Result<()> {
    let root = tempfile::tempdir()?;
    {
        let mut metadata = Store::open(root.path(), None)?;
        let (entity, _) = seed_file_on_a_peer(&mut metadata, "file", 1, 1, false)?;
        let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
        let draft = layer.open(metadata.basis(&entity)?, true)?;
        let mut draft = lock(&draft);
        draft.truncate(0)?;
        draft.write(layer.content(), 0, b"cut", false, false)?;
    }
    let (store, _layer, recovered) = recover_after_a_crash(root.path())?;
    assert_eq!(recovered, 1);
    assert_eq!(file_bytes(&lock(&store), "file")?, b"cut");
    assert_eq!(lock(&store).projection().children(ROOT).len(), 1);
    Ok(())
}

/// A writer that closed has finished: what it wrote is the file, even if nothing has sealed it yet.
#[test]
fn a_replacement_whose_writer_closed_recovers_under_its_name() -> Result<()> {
    let root = tempfile::tempdir()?;
    {
        let mut metadata = Store::open(root.path(), None)?;
        let (entity, _) = seed_file_on_a_peer(&mut metadata, "file", 1, 1, false)?;
        let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
        let draft = layer.open_to_replace(metadata.basis(&entity)?)?;
        {
            let mut draft = lock(&draft);
            draft.truncate(0)?;
            draft.write(layer.content(), 0, b"all of it", false, false)?;
        }
        layer.close(&draft, true);
    }
    let (store, _layer, recovered) = recover_after_a_crash(root.path())?;
    assert_eq!(recovered, 1);
    assert_eq!(file_bytes(&lock(&store), "file")?, b"all of it");
    assert_eq!(lock(&store).projection().children(ROOT).len(), 1);
    Ok(())
}

/// A replacement that sealed and closed leaves an ordinary draft: an edit made through it later and
/// killed is an edit of the new version.
#[test]
fn a_sealed_replacement_is_unaffected_by_a_later_kill() -> Result<()> {
    let root = tempfile::tempdir()?;
    {
        let mut metadata = Store::open(root.path(), None)?;
        let (entity, _) = seed_file_on_a_peer(&mut metadata, "file", 1, 1, false)?;
        let basis = metadata.basis(&entity)?;
        let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
        let store = Mutex::new(metadata);
        let draft = layer.open_to_replace(basis)?;
        {
            let mut draft = lock(&draft);
            draft.truncate(0)?;
            draft.write(layer.content(), 0, b"new bytes", false, false)?;
        }
        seal_batch(&store, &layer, std::slice::from_ref(&draft))?;
        layer.close(&draft, true);
        lock(&draft).write(layer.content(), 0, b"NEW", false, false)?;
    }
    let (store, _layer, recovered) = recover_after_a_crash(root.path())?;
    assert_eq!(recovered, 1);
    assert_eq!(file_bytes(&lock(&store), "file")?, b"NEW bytes");
    assert_eq!(lock(&store).projection().children(ROOT).len(), 1);

    // And with nothing after the seal, there is nothing to recover at all.
    drop((store, _layer));
    let root = tempfile::tempdir()?;
    {
        let mut metadata = Store::open(root.path(), None)?;
        let (entity, _) = seed_file_on_a_peer(&mut metadata, "file", 1, 1, false)?;
        let basis = metadata.basis(&entity)?;
        let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
        let store = Mutex::new(metadata);
        let draft = layer.open_to_replace(basis)?;
        {
            let mut draft = lock(&draft);
            draft.truncate(0)?;
            draft.write(layer.content(), 0, b"new bytes", false, false)?;
        }
        seal_batch(&store, &layer, std::slice::from_ref(&draft))?;
    }
    let (store, _layer, recovered) = recover_after_a_crash(root.path())?;
    assert_eq!(
        recovered, 0,
        "its writer never closed, and all it wrote is sealed"
    );
    assert_eq!(file_bytes(&lock(&store), "file")?, b"new bytes");
    assert_eq!(lock(&store).projection().children(ROOT).len(), 1);
    Ok(())
}

/// An fsync made a version of what came before it, which stands. What the writer went on to write
/// still has no proof it finished.
#[test]
fn a_replacement_killed_after_an_fsync_recovers_the_rest_beside_that_version() -> Result<()> {
    let root = tempfile::tempdir()?;
    {
        let mut metadata = Store::open(root.path(), None)?;
        let (entity, _) = seed_file_on_a_peer(&mut metadata, "file.bin", 1, 1, false)?;
        let basis = metadata.basis(&entity)?;
        let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
        let store = Mutex::new(metadata);
        let draft = layer.open_to_replace(basis)?;
        {
            let mut draft = lock(&draft);
            draft.truncate(0)?;
            draft.write(layer.content(), 0, b"first half", false, false)?;
        }
        seal_batch(&store, &layer, std::slice::from_ref(&draft))?;
        lock(&draft).write(layer.content(), 10, b" and more", false, false)?;
    }
    let (store, _layer, recovered) = recover_after_a_crash(root.path())?;
    assert_eq!(recovered, 1);
    assert_eq!(file_bytes(&lock(&store), "file.bin")?, b"first half");
    assert_eq!(
        file_bytes(&lock(&store), "file (recovered).bin")?,
        b"first half and more"
    );
    Ok(())
}

/// Nothing but its own writer may turn half of a replacement into a version: not whatever asks for
/// everything to be sealed, and not a stop, however orderly. The next start has it, beside the file.
#[test]
fn a_flush_or_a_stop_with_a_replacing_writer_open_leaves_the_file_as_it_was() -> Result<()> {
    use crate::runtime::Engine;
    let home = tempfile::tempdir()?;
    let engine = Engine::open(home.path(), None)?;
    let token = engine.open_file("file.txt", true, false, true)?;
    engine.write(token, 0, b"the good bytes", false, false)?;
    engine.flush(Some(token))?;
    engine.close(token)?;
    let writer = engine.open_to_replace("file.txt")?;
    engine.truncate(writer, 0, false)?;
    engine.write(writer, 0, b"half", false, false)?;
    engine.flush(None)?;
    engine.flush_native_volume()?;
    assert_eq!(
        file_bytes(&lock(&engine.store), "file.txt")?,
        b"the good bytes"
    );
    engine.shutdown()?;
    assert_eq!(
        file_bytes(&lock(&engine.store), "file.txt")?,
        b"the good bytes"
    );
    drop(engine);

    let engine = Engine::open(home.path(), None)?;
    assert_eq!(
        file_bytes(&lock(&engine.store), "file.txt")?,
        b"the good bytes"
    );
    assert_eq!(
        file_bytes(&lock(&engine.store), "file (recovered).txt")?,
        b"half"
    );
    engine.shutdown()?;
    Ok(())
}

/// A draft already on disk, so its record exists before the cut: the record must say what is coming
/// before the file is cut, since a kill right after the cut leaves only that record to go by. The
/// cut is made to fail here, and the record says so all the same.
#[test]
fn a_replacement_is_on_record_before_the_cut() -> Result<()> {
    use crate::model::content::CHUNK_SIZE;
    let root = tempfile::tempdir()?;
    let mut metadata = Store::open(root.path(), None)?;
    let (entity, _) = seed_file_on_a_peer(&mut metadata, "file", 2, 2, true)?;
    let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
    let draft = layer.open_to_replace(metadata.basis(&entity)?)?;
    let mut draft = lock(&draft);
    draft.write(layer.content(), 0, b"edit", false, false)?;
    let on_record = |path: &std::path::Path| -> Result<bool> {
        let record: ScratchRecord =
            serde_json::from_slice(&std::fs::read(path.with_extension("json"))?)?;
        Ok(record.replacing)
    };
    assert!(!on_record(&draft.path)?, "nothing is cut yet");
    let writable = draft.file.take().unwrap();
    draft.file = Some(std::fs::File::open(&draft.path)?);
    assert!(draft.truncate(0).is_err(), "a read-only handle cannot cut");
    assert!(on_record(&draft.path)?);
    assert!(!draft.record.replacing, "and nothing changed here");
    assert_eq!(draft.len()?, 2 * CHUNK_SIZE);
    draft.file = Some(writable);
    draft.truncate(0)?;
    assert!(on_record(&draft.path)?);
    assert!(draft.record.replacing);
    Ok(())
}

/// `rw` with the PC away: writing part of a chunk that is not here fails, the file stays as it
/// was, and nothing is left for the sealer or for a restart to turn into a version.
#[test]
fn a_partial_write_over_a_chunk_not_here_fails_and_changes_nothing() -> Result<()> {
    use crate::model::content::CHUNK_SIZE;
    use crate::runtime::Engine;
    for (name, chunks, manifest) in [("direct", 1, false), ("sparse", 4, true)] {
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), None)?;
        let (entity, _) = seed_file_on_a_peer(&mut lock(&engine.store), name, chunks, 0, manifest)?;
        let before = lock(&engine.store).basis(&entity)?;
        let token = engine.open_file(name, false, false, true)?;
        let error = format!(
            "{:#}",
            engine.write(token, 5, b"part", false, false).unwrap_err()
        );
        assert!(error.starts_with("OBJECT_MISSING"), "{error}");
        assert_eq!(engine.info(token)?.1, chunks * CHUNK_SIZE);
        engine.flush(Some(token))?;
        engine.close(token)?;
        let after = lock(&engine.store).basis(&entity)?;
        assert_eq!(after.version, before.version);
        assert_eq!(after.content, before.content);
        engine.shutdown()?;
        drop(engine);

        // The same failed write, and then no orderly stop.
        let root = tempfile::tempdir()?;
        let entity = {
            let mut metadata = Store::open(root.path(), None)?;
            let (entity, _) = seed_file_on_a_peer(&mut metadata, name, chunks, 0, manifest)?;
            let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
            let draft = layer.open(metadata.basis(&entity)?, true)?;
            assert!(
                lock(&draft)
                    .write(layer.content(), 5, b"part", false, false)
                    .is_err()
            );
            entity
        };
        let (store, _layer, recovered) = recover_after_a_crash(root.path())?;
        assert_eq!(recovered, 0, "{name}");
        assert_eq!(
            lock(&store).entry(&entity)?.content.unwrap().total_len,
            chunks * CHUNK_SIZE
        );
    }
    Ok(())
}

/// The file's manifest is not here, so `rw` cannot start: the open fails, and leaves nothing.
#[test]
fn opening_for_part_of_a_file_whose_manifest_is_not_here_fails_and_leaves_nothing() -> Result<()> {
    use crate::runtime::Engine;
    let home = tempfile::tempdir()?;
    let engine = Engine::open(home.path(), None)?;
    seed_file_on_a_peer(&mut lock(&engine.store), "big", 4, 0, false)?;
    let error = format!(
        "{:#}",
        engine.open_file("big", false, false, true).unwrap_err()
    );
    assert!(error.starts_with("OBJECT_MISSING"), "{error}");
    assert!(engine.writable_tokens().is_empty());
    engine.shutdown()?;
    Ok(())
}

fn drafts_on_disk(root: &std::path::Path) -> Result<usize> {
    let mut drafts = 0;
    for entry in std::fs::read_dir(root.join("scratch"))? {
        drafts += usize::from(entry?.path().extension().is_some_and(|e| e == "json"));
    }
    Ok(drafts)
}

fn wait_until(what: &str, mut done: impl FnMut() -> Result<bool>) -> Result<()> {
    let end = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !done()? {
        anyhow::ensure!(std::time::Instant::now() < end, "not within 30 s: {what}");
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    Ok(())
}

/// A write that covers one whole chunk and part of the next, which is not here, fails. The whole
/// chunk's bytes were never written, so it must not read as written.
#[test]
fn a_failed_spanning_write_leaves_what_the_file_reads_as() -> Result<()> {
    use crate::model::content::CHUNK_SIZE;
    use crate::runtime::Engine;
    let home = tempfile::tempdir()?;
    let engine = Engine::open(home.path(), None)?;
    let (_, chunks) = seed_file_on_a_peer(&mut lock(&engine.store), "big", 3, 1, true)?;
    let token = engine.open_file("big", false, false, true)?;
    let span = vec![9u8; CHUNK_SIZE as usize + 10];
    assert!(engine.write(token, 0, &span, false, false).is_err());
    let mut first = vec![0u8; CHUNK_SIZE as usize];
    engine.read(token, 0, &mut first)?;
    assert!(first == chunks[0], "chunk 0 reads as {:?}", &first[..8]);
    engine.close(token)?;
    engine.shutdown()?;
    Ok(())
}

/// The same failed write, and then a smaller one into the chunk it named, which is not here. That
/// is a partial write over a chunk not here, so it fails too, and no zeros are published.
#[test]
fn a_retry_after_a_failed_spanning_write_publishes_no_zeros() -> Result<()> {
    use crate::model::content::CHUNK_SIZE;
    use crate::runtime::Engine;
    let home = tempfile::tempdir()?;
    let engine = Engine::open(home.path(), None)?;
    let (entity, _) = seed_file_on_a_peer(&mut lock(&engine.store), "big", 3, 0, true)?;
    let before = lock(&engine.store).basis(&entity)?;
    let token = engine.open_file("big", false, false, true)?;
    let span = vec![9u8; CHUNK_SIZE as usize + 10];
    assert!(engine.write(token, 0, &span, false, false).is_err());
    assert!(engine.write(token, 100, b"edit", false, false).is_err());
    engine.flush(Some(token))?;
    engine.close(token)?;
    assert_eq!(lock(&engine.store).basis(&entity)?.version, before.version);
    engine.shutdown()?;
    Ok(())
}

/// The same failed write, and then no orderly stop.
#[test]
fn a_failed_spanning_write_killed_leaves_nothing_to_publish() -> Result<()> {
    use crate::model::content::CHUNK_SIZE;
    let root = tempfile::tempdir()?;
    let (entity, version) = {
        let mut metadata = Store::open(root.path(), None)?;
        let (entity, _) = seed_file_on_a_peer(&mut metadata, "big", 3, 0, true)?;
        let basis = metadata.basis(&entity)?;
        let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
        let draft = layer.open(basis.clone(), true)?;
        let span = vec![9u8; CHUNK_SIZE as usize + 10];
        assert!(
            lock(&draft)
                .write(layer.content(), 0, &span, false, false)
                .is_err()
        );
        (entity, basis.version)
    };
    let (store, _layer, recovered) = recover_after_a_crash(root.path())?;
    assert_eq!(recovered, 0);
    assert_eq!(lock(&store).basis(&entity)?.version, version);
    Ok(())
}

/// A writer replacing a file was told a write failed. What it wrote before that goes, and the file is
/// what it was, for a flush, a stop and a kill alike: a full disk must not leave half a file over a
/// good one.
#[test]
fn a_replacement_abandoned_leaves_the_file_as_it_was() -> Result<()> {
    use crate::runtime::Engine;
    let original = b"the good bytes".to_vec();
    let home = tempfile::tempdir()?;
    let version = {
        let engine = Engine::open(home.path(), None)?;
        let token = engine.open_file("file", true, false, true)?;
        engine.write(token, 0, &original, false, false)?;
        engine.flush(Some(token))?;
        engine.close(token)?;
        let id = engine.lookup("file")?.id;
        let version = lock(&engine.store).basis(&id)?.version;

        let writer = engine.open_to_replace("file")?;
        engine.truncate(writer, 0, false)?;
        engine.write(writer, 0, b"half", false, false)?;
        engine.abandon_draft(writer)?;
        // Whatever else asks for everything to be sealed in the meantime.
        engine.flush(None)?;
        assert_eq!(lock(&engine.store).basis(&id)?.version, version);
        assert_eq!(file_bytes(&lock(&engine.store), "file")?, original);
        engine.close(writer)?;
        engine.flush(None)?;
        assert_eq!(lock(&engine.store).basis(&id)?.version, version);

        // Failed before any byte went in: the same.
        let writer = engine.open_to_replace("file")?;
        engine.truncate(writer, 0, false)?;
        engine.abandon_draft(writer)?;
        engine.close(writer)?;
        engine.flush(None)?;
        assert_eq!(lock(&engine.store).basis(&id)?.version, version);

        // Left open and killed: nothing was left for a start to seal.
        let writer = engine.open_to_replace("file")?;
        engine.truncate(writer, 0, false)?;
        engine.write(writer, 0, b"half", false, false)?;
        engine.abandon_draft(writer)?;
        assert_eq!(drafts_on_disk(home.path())?, 0);
        engine.close(writer)?;
        engine.shutdown()?;
        version
    };
    let (store, _layer, recovered) = recover_after_a_crash(home.path())?;
    assert_eq!(recovered, 0);
    let id = lock(&store).lookup("file")?.id;
    assert_eq!(lock(&store).basis(&id)?.version, version);
    assert_eq!(file_bytes(&lock(&store), "file")?, original);
    Ok(())
}

/// With room for a draft and not for its seal, a save is answered and stays in its draft. The next
/// save opens that same draft. It must get that one sealed before it cuts anything: until it has, it
/// fails and cuts nothing, and once it has, a failure of its own goes back to that save.
#[test]
fn a_failed_replacement_keeps_an_earlier_save_that_was_answered() -> Result<()> {
    use crate::runtime::Engine;
    let home = tempfile::tempdir()?;
    let engine = Engine::open(home.path(), None)?;
    let packs = engine.working.content().packs.clone();
    let token = engine.open_file("file", true, false, true)?;
    engine.write(token, 0, b"the first version", false, false)?;
    engine.flush(Some(token))?;
    engine.close(token)?;

    // Save A: answered, and it cannot seal.
    packs.fail_appends_after(Some(0));
    let token = engine.open_file("file", false, false, true)?;
    engine.write(token, 0, b"THE SECOND VERSION", false, false)?;
    assert!(engine.flush(Some(token)).is_err());
    engine.close(token)?;

    // Save B, as a replacing writer goes about it: the seal fails, so there is no cut.
    let token = engine.open_to_replace("file")?;
    assert!(engine.flush(Some(token)).is_err());
    engine.close(token)?;
    let read = engine.open_file("file", false, false, false)?;
    let mut bytes = [0; 18];
    engine.read(read, 0, &mut bytes)?;
    engine.close(read)?;
    assert_eq!(&bytes, b"THE SECOND VERSION");

    // With room for that seal, B cuts and fails, and goes back to A.
    packs.fail_appends_after(None);
    let token = engine.open_to_replace("file")?;
    engine.flush(Some(token))?;
    engine.truncate(token, 0, false)?;
    engine.write(token, 0, b"half", false, false)?;
    engine.abandon_draft(token)?;
    engine.close(token)?;
    engine.flush(None)?;
    assert_eq!(
        file_bytes(&lock(&engine.store), "file")?,
        b"THE SECOND VERSION"
    );
    engine.shutdown()?;
    Ok(())
}

/// A write of whole chunks that cannot put them on record fails, and they read as they were.
#[test]
fn a_whole_chunk_write_whose_record_fails_leaves_what_the_file_reads_as() -> Result<()> {
    use crate::model::content::CHUNK_SIZE;
    use crate::runtime::Engine;
    let home = tempfile::tempdir()?;
    let engine = Engine::open(home.path(), None)?;
    let (_, chunks) = seed_file_on_a_peer(&mut lock(&engine.store), "big", 3, 3, true)?;
    let token = engine.open_file("big", false, false, true)?;
    let scratch = engine.handle(token)?.scratch.clone().unwrap();
    let blocked = lock(&scratch).path.with_extension("json.tmp");
    std::fs::create_dir(&blocked)?;
    let whole = vec![9u8; CHUNK_SIZE as usize];
    assert!(engine.write(token, 0, &whole, false, false).is_err());
    let mut first = vec![0u8; CHUNK_SIZE as usize];
    engine.read(token, 0, &mut first)?;
    assert!(first == chunks[0], "chunk 0 reads as {:?}", &first[..8]);
    std::fs::remove_dir(&blocked)?;
    engine.write(token, 0, &whole, false, false)?;
    engine.flush(Some(token))?;
    engine.close(token)?;
    assert_eq!(
        &file_bytes(&lock(&engine.store), "big")?[..whole.len()],
        whole
    );
    engine.shutdown()?;
    Ok(())
}

/// A write of whole chunks whose bytes do not reach the draft fails, and they read as they were,
/// now and after a restart.
#[test]
fn a_whole_chunk_write_that_fails_on_disk_leaves_what_the_file_reads_as() -> Result<()> {
    use crate::model::content::CHUNK_SIZE;
    let root = tempfile::tempdir()?;
    let (entity, version, chunks) = {
        let mut metadata = Store::open(root.path(), None)?;
        let (entity, chunks) = seed_file_on_a_peer(&mut metadata, "big", 3, 3, true)?;
        let basis = metadata.basis(&entity)?;
        let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
        let draft = layer.open(basis.clone(), true)?;
        let mut draft = lock(&draft);
        draft.materialize()?;
        let read_only = std::fs::File::open(&draft.path)?;
        draft.file = Some(read_only);
        let whole = vec![9u8; CHUNK_SIZE as usize];
        assert!(
            draft
                .write(layer.content(), 0, &whole, false, false)
                .is_err()
        );
        let mut first = vec![0u8; CHUNK_SIZE as usize];
        draft.read(layer.content(), 0, &mut first)?;
        assert!(first == chunks[0], "chunk 0 reads as {:?}", &first[..8]);
        (entity, basis.version, chunks)
    };
    let (store, _layer, recovered) = recover_after_a_crash(root.path())?;
    assert_eq!(recovered, 0);
    assert_eq!(lock(&store).basis(&entity)?.version, version);
    assert_eq!(file_bytes(&lock(&store), "big")?, chunks.concat());
    Ok(())
}

/// A write past the end that fails leaves the file its length: at a restart the difference would
/// read as written.
#[test]
fn a_growing_write_that_fails_leaves_the_file_its_length() -> Result<()> {
    use crate::model::content::CHUNK_SIZE;
    use crate::runtime::Engine;
    let home = tempfile::tempdir()?;
    let engine = Engine::open(home.path(), None)?;
    seed_short_file(&mut lock(&engine.store), "small", &[7; 100], true)?;
    let token = engine.open_file("small", false, false, true)?;
    let scratch = engine.handle(token)?.scratch.clone().unwrap();
    let blocked = lock(&scratch).path.with_extension("json.tmp");
    std::fs::create_dir(&blocked)?;
    let long = vec![8; 2 * CHUNK_SIZE as usize];
    assert!(engine.write(token, 0, &long, false, false).is_err());
    assert_eq!(engine.info(token)?.1, 100);
    assert_eq!(lock(&scratch).file()?.metadata()?.len(), 100);
    let mut bytes = [0; 100];
    engine.read(token, 0, &mut bytes)?;
    assert_eq!(bytes, [7; 100]);
    std::fs::remove_dir(&blocked)?;
    engine.flush(Some(token))?;
    engine.close(token)?;
    assert_eq!(file_bytes(&lock(&engine.store), "small")?, [7; 100]);
    engine.shutdown()?;
    Ok(())
}

/// A file of `bytes.len()` bytes, one short chunk, of which nothing is here unless `here`.
fn seed_short_file(store: &mut Store, name: &str, bytes: &[u8], here: bool) -> Result<String> {
    use crate::content::Kind;
    use crate::model::content::{ContentKind, Digest, FileContentRef};
    let mut state = store.create(name, EntryKind::File)?;
    let entity = state.id.clone();
    let parent = store.projection().visible[&entity].clone();
    if here {
        store.content.packs.put(Kind::Chunk, bytes)?;
        store.content.packs.flush()?;
    }
    state.content = Some(FileContentRef::new(
        ContentKind::Direct,
        Digest::bytes(bytes),
        bytes.len() as u64,
    )?);
    let event = store.event(
        &store.active.clone(),
        state,
        vec![parent],
        Operation::Content,
    );
    store.apply(&[event])?;
    store.sync_metadata()?;
    Ok(entity)
}

/// `rw` past the end of a file whose last chunk is short and not here. A seal would need that
/// chunk, as it grows into a longer one, so the write fails now rather than the seal for ever.
#[test]
fn a_write_past_a_short_last_chunk_not_here_fails_and_changes_nothing() -> Result<()> {
    use crate::model::content::CHUNK_SIZE;
    use crate::runtime::Engine;
    let home = tempfile::tempdir()?;
    let engine = Engine::open(home.path(), None)?;
    let entity = seed_short_file(&mut lock(&engine.store), "small", &[7; 100], false)?;
    let before = lock(&engine.store).basis(&entity)?;
    let token = engine.open_file("small", false, false, true)?;
    let error = format!(
        "{:#}",
        engine
            .write(token, 2 * CHUNK_SIZE, b"past the end", false, false)
            .unwrap_err()
    );
    assert!(error.starts_with("OBJECT_MISSING"), "{error}");
    assert_eq!(engine.info(token)?.1, 100);
    engine.flush(Some(token))?;
    engine.close(token)?;
    assert_eq!(lock(&engine.store).basis(&entity)?.version, before.version);
    engine.shutdown()?;
    Ok(())
}

/// The same with no orderly stop: the store still opens, and the file is as it was.
#[test]
fn a_write_past_a_short_last_chunk_killed_leaves_a_store_that_opens() -> Result<()> {
    use crate::model::content::CHUNK_SIZE;
    use crate::runtime::Engine;
    let root = tempfile::tempdir()?;
    let (entity, version) = {
        let mut metadata = Store::open(root.path(), None)?;
        let entity = seed_short_file(&mut metadata, "small", &[7; 100], false)?;
        let basis = metadata.basis(&entity)?;
        let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
        let draft = layer.open(basis.clone(), true)?;
        let _ = lock(&draft).write(
            layer.content(),
            2 * CHUNK_SIZE,
            b"past the end",
            false,
            false,
        );
        (entity, basis.version)
    };
    let engine = Engine::open(root.path(), None)?;
    assert_eq!(lock(&engine.store).basis(&entity)?.version, version);
    engine.shutdown()?;
    Ok(())
}

/// With that chunk here the same write works, and keeps it.
#[test]
fn a_write_past_a_short_last_chunk_here_keeps_its_bytes() -> Result<()> {
    use crate::model::content::CHUNK_SIZE;
    use crate::runtime::Engine;
    let home = tempfile::tempdir()?;
    let engine = Engine::open(home.path(), None)?;
    seed_short_file(&mut lock(&engine.store), "small", &[7; 100], true)?;
    let token = engine.open_file("small", false, false, true)?;
    engine.write(token, 2 * CHUNK_SIZE, b"past", false, false)?;
    engine.flush(Some(token))?;
    engine.close(token)?;
    let mut want = vec![0; 2 * CHUNK_SIZE as usize + 4];
    want[..100].fill(7);
    want[2 * CHUNK_SIZE as usize..].copy_from_slice(b"past");
    assert!(file_bytes(&lock(&engine.store), "small")? == want);
    engine.shutdown()?;
    Ok(())
}

/// A write over all of that chunk and past it keeps none of its bytes, so it needs none.
#[test]
fn a_write_over_all_of_a_short_last_chunk_needs_none_of_it() -> Result<()> {
    use crate::runtime::Engine;
    let home = tempfile::tempdir()?;
    let engine = Engine::open(home.path(), None)?;
    seed_short_file(&mut lock(&engine.store), "small", &[7; 100], false)?;
    let token = engine.open_file("small", false, false, true)?;
    engine.write(token, 0, &[8; 200], false, false)?;
    engine.flush(Some(token))?;
    engine.close(token)?;
    assert_eq!(file_bytes(&lock(&engine.store), "small")?, [8; 200]);
    engine.shutdown()?;
    Ok(())
}

/// Two drafts killed before their seals, one of which cannot seal: cut in the middle of a chunk
/// that is not here. Returns the file it is over, and the bytes of that chunk.
fn seed_a_draft_that_cannot_seal(root: &std::path::Path) -> Result<(String, Vec<u8>)> {
    let mut metadata = Store::open(root, None)?;
    let (entity, chunks) = seed_file_on_a_peer(&mut metadata, "big", 4, 0, true)?;
    let other = seed_short_file(&mut metadata, "other", b"other", true)?;
    let layer = WorkingLayer::new(root, metadata.content.clone())?;
    let cut = layer.open(metadata.basis(&entity)?, true)?;
    lock(&cut).truncate(17)?;
    let edited = layer.open(metadata.basis(&other)?, true)?;
    lock(&edited).write(layer.content(), 0, b"OTHER", false, false)?;
    Ok((entity, chunks[0].clone()))
}

/// One draft that cannot seal yet costs that draft a wait, and nothing else: the other seals, the
/// store opens and stops, and the draft is still there at every start.
#[test]
fn a_draft_that_cannot_seal_stays_and_the_store_opens() -> Result<()> {
    use crate::model::content::CHUNK_SIZE;
    use crate::runtime::Engine;
    let root = tempfile::tempdir()?;
    let (entity, _) = seed_a_draft_that_cannot_seal(root.path())?;
    let (store, layer, recovered) = recover_after_a_crash(root.path())?;
    assert_eq!(recovered, 1, "the other draft seals");
    assert_eq!(file_bytes(&lock(&store), "other")?, b"OTHER");
    assert_eq!(
        lock(&store).entry(&entity)?.content.unwrap().total_len,
        4 * CHUNK_SIZE
    );
    assert_eq!(drafts_on_disk(root.path())?, 1);
    drop((store, layer));
    for _ in 0..2 {
        let engine = Engine::open(root.path(), None)?;
        assert_eq!(
            engine.lookup("big")?.content.unwrap().total_len,
            4 * CHUNK_SIZE
        );
        engine.shutdown()?;
        drop(engine);
        wait_until("the store free again", || {
            Ok(match Engine::open(root.path(), None) {
                Ok(engine) => {
                    engine.shutdown()?;
                    true
                }
                Err(_) => false,
            })
        })?;
        assert_eq!(drafts_on_disk(root.path())?, 1);
    }
    Ok(())
}

/// Hands over what it is told to hint, as a peer would once it is reachable.
struct Supplying {
    packs: crate::content::PackStore,
    bytes: Vec<u8>,
    hinted: Mutex<Vec<crate::model::content::Digest>>,
}
impl crate::content::ObjectFetcher for Supplying {
    fn fetch(&self, _: &[(crate::model::content::Digest, crate::content::Kind)]) -> Result<()> {
        anyhow::bail!("OBJECT_MISSING: nothing here waits")
    }
    fn hint(&self, objects: &[(crate::model::content::Digest, crate::content::Kind)]) {
        for (digest, kind) in objects {
            lock(&self.hinted).push(*digest);
            self.packs.put(*kind, &self.bytes).unwrap();
        }
    }
}

/// Once the worker has a fetcher, what the draft waits for is asked for and the draft seals, with
/// nothing else having to happen.
#[test]
fn a_draft_kept_for_a_retry_seals_once_its_objects_arrive() -> Result<()> {
    use crate::model::content::Digest;
    use crate::runtime::Engine;
    let root = tempfile::tempdir()?;
    let (_, chunk) = seed_a_draft_that_cannot_seal(root.path())?;
    let engine = Engine::open(root.path(), None)?;
    let supplying = std::sync::Arc::new(Supplying {
        packs: engine.working.content().packs.clone(),
        bytes: chunk.clone(),
        hinted: Mutex::new(Vec::new()),
    });
    engine.working.content().set_fetcher(supplying.clone());
    wait_until("the draft sealed", || {
        Ok(engine.lookup("big")?.content.unwrap().total_len == 17)
    })?;
    assert_eq!(lock(&supplying.hinted)[0], Digest::bytes(&chunk));
    assert_eq!(file_bytes(&lock(&engine.store), "big")?, chunk[..17]);
    wait_until("the draft gone", || Ok(drafts_on_disk(root.path())? == 0))?;
    engine.shutdown()?;
    Ok(())
}

/// With no fetcher it seals too, if what it needs arrives some other way, as with a burst.
#[test]
fn a_draft_kept_for_a_retry_seals_whichever_way_its_objects_arrive() -> Result<()> {
    use crate::content::Kind;
    use crate::runtime::Engine;
    let root = tempfile::tempdir()?;
    let (_, chunk) = seed_a_draft_that_cannot_seal(root.path())?;
    let engine = Engine::open(root.path(), None)?;
    engine.working.content().packs.put(Kind::Chunk, &chunk)?;
    wait_until("the draft sealed", || {
        Ok(engine.lookup("big")?.content.unwrap().total_len == 17)
    })?;
    assert_eq!(file_bytes(&lock(&engine.store), "big")?, chunk[..17]);
    engine.shutdown()?;
    Ok(())
}

/// A draft a start could not seal is in nothing the working layer holds, and no pin keeps its basis.
/// What of it is here must stay, or the draft would never have all it needs at once. It stays once
/// a peer's edit has moved the file on, too, when no head names it any more.
#[test]
fn evict_keeps_the_basis_of_a_draft_kept_for_a_retry() -> Result<()> {
    use crate::model::content::{CHUNK_SIZE, Digest};
    use crate::runtime::{Engine, Evicted};
    use std::collections::BTreeSet;
    let root = tempfile::tempdir()?;
    let (first, other) = {
        let mut metadata = Store::open(root.path(), None)?;
        // The cut keeps the first chunk as it is, and needs the bytes of the second, not here.
        let (entity, chunks) = seed_file_on_a_peer(&mut metadata, "big", 4, 1, true)?;
        let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
        let cut = layer.open(metadata.basis(&entity)?, true)?;
        lock(&cut).truncate(CHUNK_SIZE + 17)?;
        // In the same segment as the first chunk, so that freeing this rewrites that.
        let other = metadata.content.import(b"the peer's bytes")?;
        metadata.content.packs.flush()?;
        (Digest::bytes(&chunks[0]), other)
    };
    let engine = Engine::open(root.path(), None)?;
    assert_eq!(drafts_on_disk(root.path())?, 1);
    let named = BTreeSet::from([first]);
    assert_eq!(
        engine.evict(&named)?,
        Evicted {
            removed: 0,
            refused: 1
        }
    );
    let content = engine.working.content();
    assert!(content.packs.contains(first)?);
    // No head names it now, and this evict is of something else entirely.
    let entity = engine.lookup("big")?.id;
    {
        let mut store = lock(&engine.store);
        let mut state = store.entry(&entity)?;
        state.content = Some(other.clone());
        let heads = store.projection().heads[&entity].iter().cloned().collect();
        let branch = store.active.clone();
        let mut event = store.event(&branch, state, heads, Operation::Content);
        event.author = "a peer".into();
        store.apply(&[event])?;
    }
    let evicted = engine.evict(&BTreeSet::from([other.digest]))?;
    assert_eq!(evicted.removed, 1, "{evicted:?}");
    assert!(content.packs.contains(first)?, "the draft's basis went");
    assert_eq!(drafts_on_disk(root.path())?, 1);
    engine.shutdown()?;
    Ok(())
}

/// `Engine::collect` goes by the same rule as `evict`. A draft kept for a retry holds no pin, and once
/// a peer's edit has moved the file on no head names what it was made from.
#[test]
fn collect_keeps_the_basis_of_a_draft_kept_for_a_retry() -> Result<()> {
    use crate::model::content::{CHUNK_SIZE, Digest};
    use crate::runtime::Engine;
    let root = tempfile::tempdir()?;
    let (first, other) = {
        let mut metadata = Store::open(root.path(), None)?;
        let (entity, chunks) = seed_file_on_a_peer(&mut metadata, "big", 4, 1, true)?;
        let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
        let cut = layer.open(metadata.basis(&entity)?, true)?;
        lock(&cut).truncate(CHUNK_SIZE + 17)?;
        let other = metadata.content.import(b"the peer's bytes")?;
        // Dead, and most of the segment, so that a collect rewrites it.
        metadata.content.import(&vec![3; 4 * CHUNK_SIZE as usize])?;
        metadata.content.packs.flush()?;
        (Digest::bytes(&chunks[0]), other)
    };
    let engine = Engine::open(root.path(), None)?;
    assert_eq!(drafts_on_disk(root.path())?, 1);
    let entity = engine.lookup("big")?.id;
    {
        let mut store = lock(&engine.store);
        let mut state = store.entry(&entity)?;
        state.content = Some(other);
        let heads = store.projection().heads[&entity].iter().cloned().collect();
        let branch = store.active.clone();
        let mut event = store.event(&branch, state, heads, Operation::Content);
        event.author = "a peer".into();
        store.apply(&[event])?;
    }
    assert!(engine.collect()? > 0, "what is dead around it stayed");
    assert!(
        engine.working.content().packs.contains(first)?,
        "the draft's basis went"
    );
    assert_eq!(drafts_on_disk(root.path())?, 1);
    engine.shutdown()?;
    Ok(())
}

/// A draft whose seal went through just before a crash goes on from that version, which is not the
/// one on its record. That one's chunks stay too.
#[test]
fn evict_keeps_what_a_draft_kept_for_a_retry_goes_on_from() -> Result<()> {
    use crate::content::{Kind, Manifest};
    use crate::model::content::{CHUNK_SIZE, ContentKind, Digest, FileContentRef};
    use crate::runtime::{Engine, Evicted};
    use std::collections::BTreeSet;
    let root = tempfile::tempdir()?;
    let first = {
        let mut metadata = Store::open(root.path(), None)?;
        let (entity, _) = seed_file_on_a_peer(&mut metadata, "big", 4, 0, true)?;
        let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
        let cut = layer.open(metadata.basis(&entity)?, true)?;
        lock(&cut).truncate(CHUNK_SIZE + 17)?;
        let operation = lock(&cut).record.operation.clone();
        // What the seal published: a first chunk that is here and a second that is not.
        let first = metadata
            .content
            .packs
            .put(Kind::Chunk, &vec![1; CHUNK_SIZE as usize])?;
        let second = Digest::bytes(&vec![2; CHUNK_SIZE as usize]);
        let len = 2 * CHUNK_SIZE;
        let encoded = Manifest {
            len,
            chunks: vec![first, second],
        }
        .encode()?;
        let manifest = metadata.content.packs.put(Kind::Manifest, &encoded)?;
        metadata.content.packs.flush()?;
        let mut state = metadata.entry(&entity)?;
        state.content = Some(FileContentRef::new(ContentKind::Manifest, manifest, len)?);
        let heads = metadata.projection().heads[&entity]
            .iter()
            .cloned()
            .collect();
        let branch = metadata.active.clone();
        let event = metadata.event(&branch, state, heads, Operation::Content);
        let receipt = (operation, event.id.clone(), 0);
        metadata.apply_batch(&[event], &[receipt])?;
        metadata.sync_metadata()?;
        first
    };
    let engine = Engine::open(root.path(), None)?;
    assert_eq!(drafts_on_disk(root.path())?, 1, "it does not wait");
    assert_eq!(
        engine.evict(&BTreeSet::from([first]))?,
        Evicted {
            removed: 0,
            refused: 1
        }
    );
    engine.shutdown()?;
    Ok(())
}

/// A draft that waits does not stand in the way of a flush of everything, of other files' seals
/// or of a stop, and a stop does not lose it.
#[test]
fn a_draft_kept_for_a_retry_blocks_nothing() -> Result<()> {
    use crate::runtime::Engine;
    let root = tempfile::tempdir()?;
    seed_a_draft_that_cannot_seal(root.path())?;
    let engine = Engine::open(root.path(), None)?;
    let token = engine.open_file("new", true, false, true)?;
    engine.write(token, 0, b"new", false, false)?;
    engine.flush(None)?;
    engine.close(token)?;
    assert_eq!(file_bytes(&lock(&engine.store), "new")?, b"new");
    engine.shutdown()?;
    drop(engine);
    let mut reopened = None;
    wait_until("the store free again", || {
        reopened = Engine::open(root.path(), None).ok();
        Ok(reopened.is_some())
    })?;
    reopened.unwrap().shutdown()?;
    assert_eq!(drafts_on_disk(root.path())?, 1);
    Ok(())
}

/// A truncate whose record cannot be written fails and changes nothing, in a new draft and in one
/// that holds an edit, so neither a seal nor a restart can publish a cut nobody got.
#[test]
fn a_failed_truncate_leaves_the_file_as_it_was() -> Result<()> {
    use crate::model::content::CHUNK_SIZE;
    use crate::runtime::Engine;
    for edited in [false, true] {
        let home = tempfile::tempdir()?;
        let engine = Engine::open(home.path(), None)?;
        let (entity, _) = seed_file_on_a_peer(&mut lock(&engine.store), "big", 4, 4, true)?;
        let token = engine.open_file("big", false, false, true)?;
        let scratch = engine.handle(token)?.scratch.clone().unwrap();
        if edited {
            engine.write(token, 0, b"edit", false, false)?;
        }
        let want = {
            let mut bytes = vec![0; 4 * CHUNK_SIZE as usize];
            engine.read(token, 0, &mut bytes)?;
            bytes
        };
        let was_changed = lock(&scratch).changed;
        let blocked = lock(&scratch).path.with_extension("json.tmp");
        std::fs::create_dir(&blocked)?;
        assert!(engine.truncate(token, 0, false).is_err());
        assert_eq!(engine.info(token)?.1, 4 * CHUNK_SIZE, "edited: {edited}");
        assert_eq!(lock(&scratch).changed, was_changed);
        let mut bytes = vec![0; 4 * CHUNK_SIZE as usize];
        engine.read(token, 0, &mut bytes)?;
        assert!(bytes == want, "edited: {edited}");
        std::fs::remove_dir(&blocked)?;
        engine.flush(Some(token))?;
        engine.close(token)?;
        assert_eq!(
            lock(&engine.store)
                .entry(&entity)?
                .content
                .unwrap()
                .total_len,
            4 * CHUNK_SIZE
        );
        assert!(file_bytes(&lock(&engine.store), "big")? == want);
        engine.shutdown()?;
    }
    Ok(())
}

/// The same for a handle opened to replace a file never downloaded: what `w` does first.
#[test]
fn a_failed_truncate_of_a_replacing_handle_leaves_the_file_as_it_was() -> Result<()> {
    use crate::model::content::CHUNK_SIZE;
    use crate::runtime::Engine;
    let home = tempfile::tempdir()?;
    let engine = Engine::open(home.path(), None)?;
    let (entity, _) = seed_file_on_a_peer(&mut lock(&engine.store), "big", 4, 0, false)?;
    let before = lock(&engine.store).basis(&entity)?;
    let token = engine.open_to_replace("big")?;
    let scratch = engine.handle(token)?.scratch.clone().unwrap();
    let blocked = lock(&scratch).path.with_extension("json.tmp");
    std::fs::create_dir(&blocked)?;
    assert!(engine.truncate(token, 0, false).is_err());
    std::fs::remove_dir(&blocked)?;
    engine.flush(Some(token))?;
    engine.close(token)?;
    let after = lock(&engine.store).basis(&entity)?;
    assert_eq!(after.version, before.version);
    assert_eq!(after.content.total_len, 4 * CHUNK_SIZE);
    engine.shutdown()?;
    Ok(())
}

/// A handle opened to replace a file changes nothing until its caller cuts it, whether it is then
/// closed or killed.
#[test]
fn a_replacing_handle_changes_nothing_before_it_is_cut() -> Result<()> {
    use crate::runtime::Engine;
    let root = tempfile::tempdir()?;
    let (entity, version) = {
        let mut metadata = Store::open(root.path(), None)?;
        let (entity, _) = seed_file_on_a_peer(&mut metadata, "killed", 4, 0, false)?;
        let basis = metadata.basis(&entity)?;
        let layer = WorkingLayer::new(root.path(), metadata.content.clone())?;
        let _draft = layer.open_to_replace(basis.clone())?;
        (entity, basis.version)
    };
    let (store, layer, recovered) = recover_after_a_crash(root.path())?;
    assert_eq!(recovered, 0);
    assert_eq!(lock(&store).basis(&entity)?.version, version);
    assert_eq!(drafts_on_disk(root.path())?, 0);
    drop((store, layer));

    let engine = Engine::open(root.path(), None)?;
    let token = engine.open_to_replace("killed")?;
    engine.close(token)?;
    engine.flush(None)?;
    assert_eq!(lock(&engine.store).basis(&entity)?.version, version);
    engine.shutdown()?;
    Ok(())
}
