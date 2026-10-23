use crate::{
    content::{seal::*, working::*},
    core::*,
};
use anyhow::Result;
use std::sync::Mutex;

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
fn hydrate_missing_chunk_over_authenticated_session() -> Result<()> {
    use crate::{
        model::{
            errors::ErrorWire,
            protocol::{LaneRole, frame::ClientHello},
        },
        pairing_code::Verifier,
        session,
    };
    use std::sync::Arc;
    struct Policy(Verifier);
    impl session::AcceptPolicy for Policy {
        fn installation(&self) -> String {
            "source-peer".into()
        }
        fn verifier(&self) -> Option<(Verifier, u64)> {
            Some((self.0.clone(), 1))
        }
        fn admit(&self, _: &ClientHello) -> std::result::Result<(), ErrorWire> {
            Ok(())
        }
        fn legacy_attempt(&self, _: std::net::SocketAddr, _: &serde_json::Value) {}
    }
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
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let endpoint = format!("ws://{}", listener.local_addr()?);
    let (code, verifier) = Verifier::generate();
    let serve = a.clone();
    let server = std::thread::spawn(move || -> Result<session::Conn> {
        let (stream, _) = listener.accept()?;
        let (conn, _) = session::accept(
            stream,
            Arc::new(Policy(verifier)),
            session::ConnOptions::default(),
        )
        .map_err(|e| anyhow::anyhow!("{e}"))?;
        conn.serve(Arc::new(crate::orchestrator::sync::Service {
            engine: serve,
        }));
        Ok(conn)
    });
    let (conn, _) = session::dial(
        &endpoint,
        session::client_hello("target-peer", LaneRole::Control, None, 1),
        &session::CodeAuth { code },
        None,
        session::ConnOptions::default(),
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    let server = server.join().unwrap()?;
    let fetch = crate::orchestrator::sync::DigestFetch::new(b.working.content().packs.clone());
    fetch.add(
        "source-peer".into(),
        Arc::new(crate::orchestrator::sync::SessionSource {
            conn: conn.clone(),
            repo: "repo".into(),
        }),
    );
    b.working.content().set_fetcher(fetch);
    let mut cursor = crate::orchestrator::sync::FeedCursor::default();
    crate::orchestrator::sync::exchange(&b, &conn, &mut cursor)?;
    assert!(b.lookup("remote").is_ok());
    let content = b.lookup("remote")?.content.unwrap();
    assert!(!b.working.content().packs.contains(content.digest)?);
    let h = b.open_file("remote", false, false, false)?;
    let mut tail = [0; 4];
    b.read(h, 65536, &mut tail)?;
    assert_eq!(&tail, b"tail");
    let chunks = b.working.content().chunks(&content)?;
    assert!(!b.working.content().packs.contains(chunks[0])?);
    assert!(b.working.content().packs.contains(chunks[1])?);
    b.close(h)?;
    conn.close();
    server.close();
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
    let catalog = crate::orchestrator::catalog::Catalog::open(engine.clone())?;
    let h = engine.open_file("file", true, false, true)?;
    engine.write(h, 0, b"unsaved", false, false)?;
    let scratch_path = lock(engine.handle(h)?.scratch.as_ref().unwrap())
        .path
        .clone();
    engine.inject_fault("flush");
    let result = crate::orchestrator::control::execute(
        &engine,
        &catalog,
        "shutdown",
        &serde_json::json!({}),
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
    Ok(())
}
