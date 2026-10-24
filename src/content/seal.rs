//! Delayed, batched seals. Pack durability precedes the atomic event/receipt batch.
use crate::model::content::{CHUNK_SIZE, ContentKind, FileContentRef};
use crate::{
    content::{
        local::{Kind, Manifest},
        working::{Scratch, WorkingLayer, file_write_time, lock, modified_ns},
    },
    core::{Operation, Store},
};
use anyhow::{Context, Result};
use std::sync::{Arc, Mutex};

struct Prepared {
    index: usize,
    record: crate::content::working::ScratchRecord,
    reference: FileContentRef,
    stamp: u128,
    revision: u64,
    modified_time: Option<u64>,
    pin: crate::content::local::Pin,
}

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
    scratch.materialize()?;
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
        crate::content::local::read_exact_at(scratch.file()?, &mut bytes, start)?;
        stats.hashed_chunks += 1;
        let (digest, inserted) = content.packs.put_with_status(Kind::Chunk, &bytes)?;
        if inserted {
            stats.new_chunks += 1;
        }
        chunks.push(digest);
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
    let _sealing = layer.seal_guard();
    let mut scratch = lock(file);
    let mut stats = SealStats::default();
    let reference = prepare(&mut scratch, layer, &mut stats)?;
    let pin = layer.content().pin(&reference)?;
    layer.content().packs.flush()?;
    let stamp = modified_ns(scratch.file()?)?;
    let mut metadata = lock(store);
    let mut source = metadata.entry(&scratch.record.basis.entity)?;
    let source_parent = metadata.projection().visible[&source.id].clone();
    source.deleted = true;
    let mut destination = metadata.entry(target)?;
    let destination_parent = metadata.projection().visible[target].clone();
    destination.content = Some(reference.clone());
    destination.times.modified = scratch.modified_time.unwrap_or(source.times.modified);
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
    scratch.record.base_visible_len = Some(scratch.base_visible_len);
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
    // Serialize seal publication, but retain no scratch mutex across pack fsync
    // or the metadata transaction. Close and new writes can proceed meanwhile.
    let _sealing = layer.seal_guard();
    let mut files = files.to_vec();
    files.sort_by_key(|f| Arc::as_ptr(f) as usize);
    files.dedup_by(|a, b| Arc::ptr_eq(a, b));
    let mut prepared = Vec::new();
    for (index, file) in files.iter().enumerate() {
        let mut scratch = lock(file);
        if !scratch.changed {
            continue;
        }
        let receipt = { lock(store).receipt(&scratch.record.operation)? };
        if let Some((event, _stamp)) = receipt {
            let version = lock(store)
                .version(&event)?
                .context("SEAL_VERSION_MISSING")?;
            let reference = version.state.content.context("SEAL_CONTENT_MISSING")?;
            let mut record = scratch.record.clone();
            record.hydrated = scratch.hydrated.clone();
            if scratch_matches(scratch.file()?, &record, &reference, layer)? {
                // The previous commit may have succeeded before advancing the
                // scratch. Timestamp order cannot distinguish that retry from
                // new bytes written under a coarse or backwards clock.
                scratch.pin = layer.content().pin(&reference)?;
                scratch.record.basis.version = event;
                scratch.record.basis.anchor = scratch.record.basis.version.clone();
                scratch.record.basis.content = reference.clone();
                scratch.record.operation = crate::core::id();
                scratch.record.modified_ns = modified_ns(scratch.file()?)?;
                scratch.record.hydrated = scratch.hydrated.clone();
                scratch.base_visible_len = reference.total_len;
                scratch.record.base_visible_len = Some(scratch.base_visible_len);
                scratch.changed = false;
                scratch.dirty.clear();
                scratch.closed_at = None;
                scratch.persist_record()?;
                continue;
            }
            scratch.record.operation = crate::core::id();
            scratch.record.basis.version = event;
            scratch.record.basis.anchor = scratch.record.basis.version.clone();
            scratch.record.basis.content = reference;
        }
        let reference = prepare(&mut scratch, layer, &mut stats)?;
        prepared.push(Prepared {
            index,
            record: scratch.record.clone(),
            pin: layer.content().pin(&reference)?,
            reference,
            stamp: modified_ns(scratch.file()?)?,
            revision: scratch.revision,
            modified_time: scratch.modified_time,
        });
    }
    if prepared.is_empty() {
        return Ok(stats);
    }
    layer.content().packs.flush()?;
    let mut metadata = lock(store);
    let mut events = Vec::new();
    let mut receipts = Vec::new();
    for snapshot in &prepared {
        let basis = &snapshot.record.basis;
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
        state.content = Some(snapshot.reference.clone());
        state.times.modified = snapshot.modified_time.unwrap_or(state.times.modified);
        let event = metadata.event(&basis.branch, state, vec![parent], operation);
        receipts.push((
            snapshot.record.operation.clone(),
            event.id.clone(),
            snapshot.stamp,
        ));
        events.push(event);
    }
    metadata.apply_batch(&events, &receipts)?;
    drop(metadata);
    for (snapshot, event) in prepared.into_iter().zip(events) {
        let file = &files[snapshot.index];
        let mut scratch = lock(file);
        let unchanged = scratch.revision == snapshot.revision;
        scratch.record.basis.version = event.id;
        scratch.record.basis.anchor = scratch.record.basis.version.clone();
        scratch.record.basis.content = snapshot.reference.clone();
        scratch.record.operation = crate::core::id();
        scratch.record.modified_ns = snapshot.stamp;
        if unchanged {
            scratch.base_visible_len = snapshot.reference.total_len;
            scratch.changed = false;
            scratch.dirty.clear();
            scratch.closed_at = None;
        } else {
            // A concurrent shrink/re-extension must keep zero ranges, even
            // though the just-committed reference contains older nonzero bytes.
            scratch.base_visible_len = scratch.base_visible_len.min(snapshot.reference.total_len);
            scratch.changed = true;
        }
        scratch.record.base_visible_len = Some(scratch.base_visible_len);
        let old_pin = std::mem::replace(&mut scratch.pin, snapshot.pin);
        scratch.record.hydrated = scratch.hydrated.clone();
        scratch.persist_record()?;
        drop(scratch);
        drop(old_pin);
        if !unchanged {
            layer.schedule(file);
        }
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
        // Capture the original overlay before rebasing to a committed receipt.
        // Bytes appended beyond that old base are materialized host bytes, not
        // missing ranges to refill from the receipt's older content.
        let mut base_visible_len = record
            .base_visible_len
            .unwrap_or(record.basis.content.total_len);
        record.base_visible_len = Some(base_visible_len);
        for index in record.basis.content.total_len.div_ceil(CHUNK_SIZE)
            ..file.metadata()?.len().div_ceil(CHUNK_SIZE)
        {
            record.hydrated.insert(index);
        }
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
            base_visible_len = base_visible_len.min(reference.total_len);
            record.base_visible_len = Some(base_visible_len);
            record.basis.content = reference;
            record.operation = crate::core::id();
        } else if stamp <= record.modified_ns
            && scratch_matches(&file, &record, &record.basis.content, layer)?
        {
            // A coarse filesystem clock (or a backwards wall-clock change)
            // can give changed bytes the clean marker's timestamp. Only bytes
            // that still match their immutable base may be discarded.
            drop(file);
            std::fs::remove_file(&path)?;
            std::fs::remove_file(path.with_extension("json"))?;
            continue;
        }
        let mut dirty = crate::content::working::DirtyBitmap::default();
        dirty.mark(0, file.metadata()?.len());
        let pin = layer.content().pin(&record.basis.content)?;
        let hydrated = record.hydrated.clone();
        let scratch = Arc::new(Mutex::new(Scratch {
            modified_time: Some(file_write_time(&file)?),
            explicit_modified_time: false,
            automatic_time_current: false,
            length: file.metadata()?.len(),
            base_visible_len,
            hydrated,
            hydration_pending: false,
            record,
            path: path.clone(),
            file: Some(file),
            dirty,
            changed: true,
            revision: 0,
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
        if !record.hydrated.contains(&(index as u64))
            && offset
                < record
                    .base_visible_len
                    .unwrap_or(record.basis.content.total_len)
        {
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
