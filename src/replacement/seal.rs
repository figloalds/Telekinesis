//! Delayed, batched seals. Pack durability precedes the atomic event/receipt batch.
use super::{objects::{Kind,Manifest},store::{Operation,Store},working::{Scratch,WorkingLayer,lock,modified_ns}};
use crate::model::content::{CHUNK_SIZE,ContentKind,FileContentRef};
use anyhow::{Context,Result};
use std::{io::{Read,Seek,SeekFrom},sync::{Arc,Mutex}};

#[derive(Default,Debug)]
pub struct SealStats {pub entities:usize,pub hashed_chunks:usize,pub new_chunks:usize}
fn prepare(scratch:&mut Scratch,layer:&WorkingLayer,stats:&mut SealStats)->Result<FileContentRef> {
    let content=layer.content();let old=content.chunks(&scratch.record.basis.content)?;
    let len=scratch.len()?;let mut chunks=Vec::new();
    for index in 0..len.div_ceil(CHUNK_SIZE) {
        let start=index*CHUNK_SIZE;let count=(len-start).min(CHUNK_SIZE);
        let old_count=scratch.record.basis.content.total_len.saturating_sub(start).min(CHUNK_SIZE);
        if !scratch.dirty.intersects(start,count) && old_count==count && index<old.len() as u64 {
            chunks.push(old[index as usize]);continue;
        }
        scratch.hydrate_chunk(content,index)?;
        let mut bytes=vec![0;count as usize];scratch.file.seek(SeekFrom::Start(start))?;scratch.file.read_exact(&mut bytes)?;
        stats.hashed_chunks+=1;
        let digest=crate::model::content::Digest::bytes(&bytes);
        if !content.packs.contains(digest)? {stats.new_chunks+=1;}
        chunks.push(content.packs.put(Kind::Chunk,&bytes)?);
    }
    let (kind,digest)=if len<=CHUNK_SIZE {(ContentKind::Direct,if len==0 {content.packs.put(Kind::Chunk,&[])?}else{chunks[0]})}else{(ContentKind::Manifest,content.packs.put(Kind::Manifest,&Manifest{len,chunks}.encode()?)?)};
    FileContentRef::new(kind,digest,len)
}
pub fn seal_batch(store:&Mutex<Store>,layer:&WorkingLayer,files:&[Arc<Mutex<Scratch>>])->Result<SealStats> {
    let mut stats=SealStats::default();
    // Stable order prevents two simultaneous explicit flushes deadlocking.
    let mut files=files.to_vec();files.sort_by_key(|f|Arc::as_ptr(f) as usize);files.dedup_by(|a,b|Arc::ptr_eq(a,b));
    let mut guards:Vec<_>=files.iter().map(|f|lock(f)).collect();
    let mut prepared=Vec::new();
    for (index,scratch) in guards.iter_mut().enumerate() {
        if !scratch.changed {continue;}
        if let Some((event,stamp))=lock(store).receipt(&scratch.record.operation)? {
            let version=lock(store).version(&event)?.context("SEAL_VERSION_MISSING")?;
            if modified_ns(&scratch.file)?<=stamp {
                scratch.record.basis.version=event;scratch.record.basis.content=version.state.content.context("SEAL_CONTENT_MISSING")?;
                scratch.changed=false;scratch.dirty.clear();continue;
            }
            scratch.record.operation=super::store::id();scratch.record.basis.version=event;
            scratch.record.basis.content=version.state.content.context("SEAL_CONTENT_MISSING")?;
        }
        let reference=prepare(scratch,layer,&mut stats)?;
        prepared.push((index,reference,modified_ns(&scratch.file)?));
    }
    layer.content().packs.flush()?;
    let mut metadata=lock(store);let mut events=Vec::new();let mut receipts=Vec::new();
    for (index,reference,stamp) in &prepared {
        let scratch=&guards[*index];let basis=&scratch.record.basis;
        let base=metadata.version(&basis.version)?.context("BASE_VERSION_MISSING")?;
        let mut state=base.state;let mut parent=basis.version.clone();
        // Local namespace/timestamp changes made during this edit are its own
        // successors. Remote successors deliberately keep the opened base.
        if basis.branch==metadata.active {
            if let Some(visible)=metadata.projection().visible.get(&basis.entity) {
                if let Some(current)=metadata.version(visible)? {
                    if current.author==metadata.device && current.operation!=Operation::Content && current.operation!=Operation::Resolve {
                        state=current.state;parent=current.id;
                    }
                }
            }
        }
        // Unlinked/replaced open files may still flush private bytes, but cannot
        // resurrect the old name. Their retained version remains in history.
        let operation=if state.deleted {Operation::Delete}else{Operation::Content};
        state.content=Some(reference.clone());state.times.modified=super::store::Times::default().modified;
        let event=metadata.event(&basis.branch,state,vec![parent],operation);
        receipts.push((scratch.record.operation.clone(),event.id.clone(),*stamp));events.push(event);
    }
    metadata.apply_batch(&events,&receipts)?;
    drop(metadata);
    for ((index,reference,stamp),event) in prepared.into_iter().zip(events) {
        let scratch=&mut guards[index];
        scratch.record.basis.version=event.id;scratch.record.basis.content=reference.clone();scratch.record.operation=super::store::id();scratch.record.modified_ns=stamp;
        scratch.base_visible_len=reference.total_len;scratch.pin=layer.content().pin(&reference)?;
        scratch.changed=false;scratch.dirty.clear();scratch.closed_at=None;scratch.record.hydrated=scratch.hydrated.clone();
        scratch.persist_record()?;stats.entities+=1;
    }
    Ok(stats)
}

/// Recovery uses the last committed receipt, not wall-clock ordering of peers.
/// Only materialized chunks override the immutable base; sparse remote ranges
/// are rehydrated when the recovered file is sealed or read.
pub fn recover(store:&Mutex<Store>,layer:&WorkingLayer)->Result<usize> {
    let mut recovered=0;
    for (path,mut record) in layer.records()? {
        if !path.exists() {continue;}
        let file=std::fs::OpenOptions::new().read(true).write(true).open(&path)?;
        let stamp=modified_ns(&file)?;
        if let Some((event,committed))=lock(store).receipt(&record.operation)? {
            if stamp<=committed {drop(file);std::fs::remove_file(&path)?;std::fs::remove_file(path.with_extension("json"))?;continue;}
            let version=lock(store).version(&event)?.context("SEAL_VERSION_MISSING")?;
            record.basis.version=event;record.basis.content=version.state.content.context("SEAL_CONTENT_MISSING")?;record.operation=super::store::id();
        } else if stamp<=record.modified_ns {
            drop(file);std::fs::remove_file(&path)?;std::fs::remove_file(path.with_extension("json"))?;continue;
        }
        let mut dirty=super::working::DirtyBitmap::default();dirty.mark(0,file.metadata()?.len());
        let pin=layer.content().pin(&record.basis.content)?;
        let scratch=Arc::new(Mutex::new(Scratch{base_visible_len:record.basis.content.total_len,hydrated:record.hydrated.clone(),record,path:path.clone(),file,dirty,changed:true,writers:0,handles:0,closed_at:None,pin}));
        seal_batch(store,layer,std::slice::from_ref(&scratch))?;
        drop(scratch);std::fs::remove_file(&path)?;std::fs::remove_file(path.with_extension("json"))?;recovered+=1;
    } Ok(recovered)
}
