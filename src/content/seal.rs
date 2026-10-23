//! Delayed, batched seals. Pack durability precedes the atomic event/receipt batch.
use crate::model::content::{CHUNK_SIZE, ContentKind, FileContentRef};
use crate::{
    content::{
        local::{Kind, Manifest},
        working::{Scratch, WorkingLayer, lock, modified_ns},
    },
    core::{Operation, Store},
};
use anyhow::{Context, Result};
use std::{
    io::{Read, Seek, SeekFrom},
    sync::{Arc, Mutex},
};

#[derive(Default, Debug)]
pub struct SealStats {
    pub entities: usize,
    pub hashed_chunks: usize,
    pub new_chunks: usize,
}
fn prepare(
    scratch: &mut Scratch,
    layer: &WorkingLayer,
    stats: &mut SealStats,
) -> Result<FileContentRef> {
    let content = layer.content();
    let old = content.chunks(&scratch.record.basis.content)?;
    let len = scratch.len()?;
    let mut chunks = Vec::new();
    for index in 0..len.div_ceil(CHUNK_SIZE) {
        let start = index * CHUNK_SIZE;
        let count = (len - start).min(CHUNK_SIZE);
        let old_count = scratch
            .record
            .basis
            .content
            .total_len
            .saturating_sub(start)
            .min(CHUNK_SIZE);
        if !scratch.dirty.intersects(start, count) && old_count == count && index < old.len() as u64
        {
            chunks.push(old[index as usize]);
            continue;
        }
        scratch.hydrate_chunk(content, index)?;
        let mut bytes = vec![0; count as usize];
        scratch.file.seek(SeekFrom::Start(start))?;
        scratch.file.read_exact(&mut bytes)?;
        stats.hashed_chunks += 1;
        let digest = crate::model::content::Digest::bytes(&bytes);
        if !content.packs.contains(digest)? {
            stats.new_chunks += 1;
        }
        chunks.push(content.packs.put(Kind::Chunk, &bytes)?);
    }
    let (kind, digest) = if len <= CHUNK_SIZE {
        (
            ContentKind::Direct,
            if len == 0 {
                content.packs.put(Kind::Chunk, &[])?
            } else {
                chunks[0]
            },
        )
    } else {
        (
            ContentKind::Manifest,
            content
                .packs
                .put(Kind::Manifest, &Manifest { len, chunks }.encode()?)?,
        )
    };
    FileContentRef::new(kind, digest, len)
}
pub fn replace_file(
    store: &Mutex<Store>,
    layer: &WorkingLayer,
    file: &Arc<Mutex<Scratch>>,
    target: &str,
) -> Result<()> {
    let mut scratch = lock(file);
    let mut stats = SealStats::default();
    let reference = prepare(&mut scratch, layer, &mut stats)?;
    let pin = layer.content().pin(&reference)?;
    layer.content().packs.flush()?;
    let stamp = modified_ns(&scratch.file)?;
    let mut metadata = lock(store);
    let mut source = metadata.entry(&scratch.record.basis.entity)?;
    let source_parent = metadata.projection().visible[&source.id].clone();
    source.deleted = true;
    let mut destination = metadata.entry(target)?;
    let destination_parent = metadata.projection().visible[target].clone();
    destination.content = Some(reference.clone());
    destination.times.modified = crate::core::Times::default().modified;
    let branch = scratch.record.basis.branch.clone();
    let deleted = metadata.event(&branch, source, vec![source_parent], Operation::Delete);
    let content = metadata.event(
        &branch,
        destination,
        vec![destination_parent],
        Operation::Content,
    );
    metadata.apply_batch(
        &[deleted, content.clone()],
        &[(scratch.record.operation.clone(), content.id.clone(), stamp)],
    )?;
    drop(metadata);
    scratch.record.basis.entity = target.into();
    scratch.record.basis.version = content.id;
    scratch.record.basis.anchor = scratch.record.basis.version.clone();
    scratch.record.basis.content = reference.clone();
    scratch.record.operation = crate::core::id();
    scratch.record.modified_ns = stamp;
    scratch.record.hydrated = scratch.hydrated.clone();
    scratch.base_visible_len = reference.total_len;
    scratch.pin = pin;
    scratch.changed = false;
    scratch.dirty.clear();
    scratch.persist_record()?;
    Ok(())
}
pub fn seal_batch(
    store: &Mutex<Store>,
    layer: &WorkingLayer,
    files: &[Arc<Mutex<Scratch>>],
) -> Result<SealStats> {
    let mut stats = SealStats::default();
    if files.is_empty() {
        return Ok(stats);
    }
    // Stable order prevents two simultaneous explicit flushes deadlocking.
    let mut files = files.to_vec();
    files.sort_by_key(|f| Arc::as_ptr(f) as usize);
    files.dedup_by(|a, b| Arc::ptr_eq(a, b));
    let mut guards: Vec<_> = files.iter().map(|f| lock(f)).collect();
    let mut prepared = Vec::new();
    for (index, scratch) in guards.iter_mut().enumerate() {
        if !scratch.changed {
            continue;
        }
        let receipt = { lock(store).receipt(&scratch.record.operation)? };
        if let Some((event, stamp)) = receipt {
            let version = lock(store)
                .version(&event)?
                .context("SEAL_VERSION_MISSING")?;
            if modified_ns(&scratch.file)? <= stamp {
                scratch.record.basis.version = event;
                scratch.record.basis.anchor = scratch.record.basis.version.clone();
                scratch.record.basis.content =
                    version.state.content.context("SEAL_CONTENT_MISSING")?;
                scratch.changed = false;
                scratch.dirty.clear();
                continue;
            }
            scratch.record.operation = crate::core::id();
            scratch.record.basis.version = event;
            scratch.record.basis.anchor = scratch.record.basis.version.clone();
            scratch.record.basis.content = version.state.content.context("SEAL_CONTENT_MISSING")?;
        }
        let reference = prepare(scratch, layer, &mut stats)?;
        prepared.push((index, reference, modified_ns(&scratch.file)?));
    }
    if prepared.is_empty() {
        return Ok(stats);
    }
    layer.content().packs.flush()?;
    let mut metadata = lock(store);
    let mut events = Vec::new();
    let mut receipts = Vec::new();
    for (index, reference, stamp) in &prepared {
        let scratch = &guards[*index];
        let basis = &scratch.record.basis;
        let base = metadata
            .version(&basis.version)?
            .context("BASE_VERSION_MISSING")?;
        let mut state = base.state;
        let mut parent = basis.version.clone();
        // Local namespace/timestamp changes made during this edit are its own
        // successors. Remote successors deliberately keep the opened base.
        if basis.branch == metadata.active
            && let Some(visible) = metadata.projection().visible.get(&basis.entity)
            && let Some(current) = metadata.version(visible)?
            && current.author == metadata.device
            && current.operation != Operation::Content
            && current.operation != Operation::Resolve
            && metadata.content_anchor(&current.id)? == basis.anchor
        {
            state = current.state;
            parent = current.id;
        }
        // Unlinked/replaced open files may still flush private bytes, but cannot
        // resurrect the old name. Their retained version remains in history.
        let operation = if state.deleted {
            Operation::Delete
        } else {
            Operation::Content
        };
        state.content = Some(reference.clone());
        state.times.modified = crate::core::Times::default().modified;
        let event = metadata.event(&basis.branch, state, vec![parent], operation);
        receipts.push((scratch.record.operation.clone(), event.id.clone(), *stamp));
        events.push(event);
    }
    metadata.apply_batch(&events, &receipts)?;
    drop(metadata);
    for ((index, reference, stamp), event) in prepared.into_iter().zip(events) {
        let scratch = &mut guards[index];
        scratch.record.basis.version = event.id;
        scratch.record.basis.anchor = scratch.record.basis.version.clone();
        scratch.record.basis.content = reference.clone();
        scratch.record.operation = crate::core::id();
        scratch.record.modified_ns = stamp;
        scratch.base_visible_len = reference.total_len;
        scratch.pin = layer.content().pin(&reference)?;
        scratch.changed = false;
        scratch.dirty.clear();
        scratch.closed_at = None;
        scratch.record.hydrated = scratch.hydrated.clone();
        scratch.persist_record()?;
        stats.entities += 1;
    }
    Ok(stats)
}

/// Recovery uses the last committed receipt, not wall-clock ordering of peers.
/// Only materialized chunks override the immutable base; sparse remote ranges
/// are rehydrated when the recovered file is sealed or read.
pub fn recover(store: &Mutex<Store>, layer: &WorkingLayer) -> Result<usize> {
    let mut recovered = 0;
    for (path, mut record) in layer.records()? {
        if !path.exists() {
            continue;
        }
        // Namespace transactions may still be inside the short write-back
        // window. A missing metadata-only successor can rebase to its content
        // anchor; an uncommitted new entity is preserved for manual recovery.
        if lock(store).version(&record.basis.version)?.is_none() {
            if lock(store).version(&record.basis.anchor)?.is_some() {
                record.basis.version = record.basis.anchor.clone();
            } else {
                layer.quarantine(&path)?;
                continue;
            }
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        let stamp = modified_ns(&file)?;
        let receipt = { lock(store).receipt(&record.operation)? };
        if let Some((event, _committed)) = receipt {
            let version = lock(store)
                .version(&event)?
                .context("SEAL_VERSION_MISSING")?;
            let reference = version.state.content.context("SEAL_CONTENT_MISSING")?;
            if scratch_matches(&file, &record, &reference, layer)? {
                drop(file);
                std::fs::remove_file(&path)?;
                std::fs::remove_file(path.with_extension("json"))?;
                continue;
            }
            record.basis.version = event;
            record.basis.anchor = record.basis.version.clone();
            record.basis.content = reference;
            record.operation = crate::core::id();
        } else if stamp <= record.modified_ns {
            drop(file);
            std::fs::remove_file(&path)?;
            std::fs::remove_file(path.with_extension("json"))?;
            continue;
        }
        let mut dirty = crate::content::working::DirtyBitmap::default();
        dirty.mark(0, file.metadata()?.len());
        let pin = layer.content().pin(&record.basis.content)?;
        let mut hydrated = record.hydrated.clone();
        for index in record.basis.content.total_len.div_ceil(CHUNK_SIZE)
            ..file.metadata()?.len().div_ceil(CHUNK_SIZE)
        {
            hydrated.insert(index);
        }
        let scratch = Arc::new(Mutex::new(Scratch {
            length: file.metadata()?.len(),
            base_visible_len: record.basis.content.total_len,
            hydrated,
            record,
            path: path.clone(),
            file,
            dirty,
            changed: true,
            writers: 0,
            handles: 0,
            closed_at: None,
            idle_at: std::time::Instant::now(),
            pin,
        }));
        seal_batch(store, layer, std::slice::from_ref(&scratch))?;
        drop(scratch);
        std::fs::remove_file(&path)?;
        std::fs::remove_file(path.with_extension("json"))?;
        recovered += 1;
    }
    Ok(recovered)
}
fn scratch_matches(
    file: &std::fs::File,
    record: &crate::content::working::ScratchRecord,
    reference: &FileContentRef,
    layer: &WorkingLayer,
) -> Result<bool> {
    if file.metadata()?.len() != reference.total_len {
        return Ok(false);
    }
    let chunks = layer.content().chunks(reference)?;
    for (index, digest) in chunks.iter().enumerate() {
        let offset = index as u64 * CHUNK_SIZE;
        if !record.hydrated.contains(&(index as u64)) && offset < record.basis.content.total_len {
            continue;
        }
        let count = reference.total_len.saturating_sub(offset).min(CHUNK_SIZE) as usize;
        let mut bytes = vec![0; count];
        crate::content::local::read_exact_at(file, &mut bytes, offset)?;
        if crate::model::content::Digest::bytes(&bytes) != *digest {
            return Ok(false);
        }
    }
    Ok(true)
}
