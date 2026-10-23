use super::{working::*,store::*,seal::*};
use anyhow::Result;
use std::sync::Mutex;

#[test]
fn seal_exactly_once_across_restart_and_changed_chunks_only()->Result<()> {
    let root=tempfile::tempdir()?;
    let mut metadata=Store::open(root.path(),None)?;
    let entity=metadata.create("file",EntryKind::File)?.id;
    let layer=WorkingLayer::new(root.path(),metadata.content.clone())?;
    let scratch=layer.open(metadata.basis(&entity)?,true)?;
    let store=Mutex::new(metadata);
    lock(&scratch).write(layer.content(),0,&vec![7;3*65536],false,false)?;
    let stats=seal_batch(&store,&layer,std::slice::from_ref(&scratch))?;
    assert_eq!(stats.hashed_chunks,3);assert_eq!(stats.new_chunks,1);
    lock(&scratch).write(layer.content(),65536+4096,b"edit",false,false)?;
    let stats=seal_batch(&store,&layer,std::slice::from_ref(&scratch))?;
    assert_eq!(stats.hashed_chunks,1);assert_eq!(stats.new_chunks,1);
    let before=lock(&store).events_after(0,4096)?.len();
    // Emulate a crash after commit but before the sidecar is advanced.
    let mut record=lock(&scratch).record.clone();
    record.operation="11111111-1111-4111-8111-111111111111".into();
    record.id=record.operation.clone();
    let path=lock(&scratch).path.clone();
    // A recovery file name is the operation identity.
    let crash_path=path.with_file_name(format!("{}.scratch",record.operation));
    std::fs::copy(&path,&crash_path)?;
    let stamp=modified_ns(&std::fs::File::open(&crash_path)?)?;
    let event=lock(&scratch).record.basis.version.clone();
    lock(&store).db.execute("INSERT INTO seal_receipts VALUES(?1,?2,?3)",rusqlite::params![record.operation,event,stamp.to_string()])?;
    std::fs::write(crash_path.with_extension("json"),serde_json::to_vec(&record)?)?;
    drop(scratch);drop(layer);drop(store);
    let store=Mutex::new(Store::open(root.path(),None)?);
    let layer=WorkingLayer::new(root.path(),lock(&store).content.clone())?;
    recover(&store,&layer)?;
    assert_eq!(lock(&store).events_after(0,4096)?.len(),before);
    assert!(!crash_path.exists());
    drop(layer);drop(store);
    let metadata=Store::open(root.path(),None)?;
    let mut bytes=[0;4];metadata.content.read_range(&metadata.basis(&entity)?.content,65536+4096,&mut bytes)?;assert_eq!(&bytes,b"edit");Ok(())
}

#[test]
fn recovery_seals_surviving_dirty_scratch_once()->Result<()> {
    let root=tempfile::tempdir()?;
    {
        let mut store=Store::open(root.path(),None)?;let entity=store.create("saved",EntryKind::File)?.id;
        let layer=WorkingLayer::new(root.path(),store.content.clone())?;let scratch=layer.open(store.basis(&entity)?,true)?;
        lock(&scratch).write(layer.content(),0,b"survive",false,false)?;
    }
    let store=Mutex::new(Store::open(root.path(),None)?);let layer=WorkingLayer::new(root.path(),lock(&store).content.clone())?;
    assert_eq!(recover(&store,&layer)?,1);assert_eq!(recover(&store,&layer)?,0);
    let metadata=lock(&store);let entry=metadata.lookup("saved")?;let mut bytes=[0;7];metadata.content.read_range(entry.content.as_ref().unwrap(),0,&mut bytes)?;assert_eq!(&bytes,b"survive");Ok(())
}

#[test]
fn snapshot_fork_isolation_and_cached_tree_reopen()->Result<()> {
    let root=tempfile::tempdir()?;let mut store=Store::open(root.path(),None)?;
    store.create("directory",EntryKind::Directory)?;let entity=store.create("directory/file",EntryKind::File)?.id;
    let snapshot=store.checkpoint()?;let branch=store.fork(&snapshot,"fork")?;
    store.rename(&entity,"directory/renamed",false)?;
    store.checkout(&branch)?;assert!(store.lookup("directory/file").is_ok());assert!(store.lookup("directory/renamed").is_err());
    store.rename(&entity,"fork-only",false)?;store.checkout(MAIN)?;assert!(store.lookup("directory/renamed").is_ok());assert!(store.lookup("fork-only").is_err());
    drop(store);let mut store=Store::open(root.path(),None)?;store.checkout("fork")?;assert!(store.lookup("fork-only").is_ok());Ok(())
}
