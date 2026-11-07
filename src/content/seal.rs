//! Delayed, batched seals. Pack durability precedes the atomic event/receipt batch.
use crate::model::content::{CHUNK_SIZE, ContentKind, Digest, FileContentRef};
use crate::{
    content::{
        local::{Kind, LocalContent, Manifest},
        working::{Scratch, ScratchRecord, WorkingLayer, file_write_time, lock, modified_ns},
    },
    core::{Entry, Operation, ROOT, Store, Times, name_key},
    util::log::event,
};
use anyhow::{Context, Result};
use std::{
    collections::BTreeSet,
    path::Path,
    sync::{Arc, Mutex},
};

struct Prepared {
    index: usize,
    record: ScratchRecord,
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
/// Whether a seal keeps this chunk of the draft's base as it is, which takes none of its bytes.
fn keeps_base_chunk(scratch: &Scratch, index: u64, len: u64, base_chunks: u64) -> bool {
    let start = index * CHUNK_SIZE;
    let count = (len - start).min(CHUNK_SIZE);
    let old_count = scratch
        .record
        .basis
        .content
        .total_len
        .saturating_sub(start)
        .min(CHUNK_SIZE);
    !scratch.dirty.intersects(start, count) && old_count == count && index < base_chunks
}
/// What a seal of this draft would have to fetch.
fn objects_a_seal_needs(scratch: &Scratch, content: &LocalContent) -> Result<Vec<(Digest, Kind)>> {
    if scratch.base_visible_len == 0 {
        return Ok(Vec::new());
    }
    let base = &scratch.record.basis.content;
    if base.kind == ContentKind::Manifest && !content.packs.contains(base.digest)? {
        return Ok(vec![(base.digest, Kind::Manifest)]);
    }
    let old = content.chunks(base)?;
    let len = scratch.len()?;
    let mut missing = Vec::new();
    for index in 0..len.div_ceil(CHUNK_SIZE) {
        if keeps_base_chunk(scratch, index, len, old.len() as u64)
            || scratch.hydrated.contains(&index)
            || index * CHUNK_SIZE >= scratch.base_visible_len
        {
            continue;
        }
        if let Some(digest) = old.get(index as usize)
            && !content.packs.contains(*digest)?
        {
            missing.push((*digest, Kind::Chunk));
        }
    }
    Ok(missing)
}
fn prepare(
    scratch: &mut Scratch,
    layer: &WorkingLayer,
    stats: &mut SealStats,
) -> Result<FileContentRef> {
    scratch.materialize()?;
    let content = layer.content();
    // A draft that shows nothing of its base keeps none of its chunks, and the base's
    // manifest may never have been here.
    let old = if scratch.base_visible_len == 0 {
        Vec::new()
    } else {
        content.chunks(&scratch.record.basis.content)?
    };
    let len = scratch.len()?;
    let mut chunks = Vec::new();
    for index in 0..len.div_ceil(CHUNK_SIZE) {
        let start = index * CHUNK_SIZE;
        let count = (len - start).min(CHUNK_SIZE);
        if keeps_base_chunk(scratch, index, len, old.len() as u64) {
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

/// `name (recovered).ext`, or with the first free number if `taken` has that, by [`name_key`]. It
/// numbers as the app numbers a name that is taken: a dot that starts or ends the name starts no
/// extension. A long stem gives way, as the new name must fit where the old one did.
pub(crate) fn recovered_name(name: &str, taken: &BTreeSet<String>) -> Result<String> {
    const LONGEST: usize = 255;
    const LONGEST_EXTENSION: usize = 64;
    let (stem, extension) = match name.rfind('.') {
        Some(dot) if dot > 0 && dot < name.len() - 1 && name.len() - dot <= LONGEST_EXTENSION => {
            name.split_at(dot)
        }
        _ => (name, ""),
    };
    let units = |text: &str| text.encode_utf16().count();
    for number in 0u64.. {
        let mark = match number {
            0 => " (recovered)".to_owned(),
            number => format!(" (recovered) ({number})"),
        };
        let room = LONGEST.saturating_sub(units(&mark) + units(extension));
        let mut used = 0;
        let kept: String = stem
            .chars()
            .take_while(|c| {
                used += c.len_utf16();
                used <= room
            })
            .collect();
        let candidate = format!("{kept}{mark}{extension}");
        if !taken.contains(&name_key(&candidate)?) {
            return Ok(candidate);
        }
    }
    unreachable!("a finite directory has a free number")
}

/// Publishes a draft as a new file beside the one it was to replace, which stays as it was, with
/// the receipt in the same transaction: a kill after this finds the receipt and nothing to do.
/// The new file is made as any other is, by a create and a content event, so it syncs like one and
/// counts as waiting to send. Beside the file as it is now: if its directory has gone since, the new
/// one goes at the top of the folder, as a folder has no other place to show it.
fn seal_beside(
    store: &Mutex<Store>,
    layer: &WorkingLayer,
    file: &Arc<Mutex<Scratch>>,
) -> Result<String> {
    let _sealing = layer.seal_guard();
    let mut scratch = lock(file);
    let mut stats = SealStats::default();
    let reference = prepare(&mut scratch, layer, &mut stats)?;
    let _pin = layer.content().pin(&reference)?;
    layer.content().packs.flush()?;
    let stamp = modified_ns(scratch.file()?)?;
    let branch = scratch.record.basis.branch.clone();
    let mut metadata = lock(store);
    let original = match metadata.entry(&scratch.record.basis.entity) {
        Ok(entry) => entry,
        Err(_) => {
            metadata
                .version(&scratch.record.basis.version)?
                .context("BASE_VERSION_MISSING")?
                .state
        }
    };
    let parent = metadata
        .projection()
        .entries
        .get(&original.parent)
        .filter(|entry| !entry.deleted)
        .map_or_else(|| ROOT.to_owned(), |entry| entry.id.clone());
    let mut taken = BTreeSet::new();
    for child in metadata.projection().children(&parent) {
        taken.insert(name_key(&child.name)?);
    }
    let mut state = Entry {
        id: crate::core::id(),
        parent,
        name: recovered_name(&original.name, &taken)?,
        content: Some(metadata.empty_content()),
        times: Times::default(),
        deleted: false,
        working_len: None,
        ..original
    };
    let entity = state.id.clone();
    let created = metadata.event(&branch, state.clone(), Vec::new(), Operation::Create);
    state.content = Some(reference);
    state.times.modified = scratch.modified_time.unwrap_or(state.times.modified);
    let content = metadata.event(&branch, state, vec![created.id.clone()], Operation::Content);
    let receipt = (scratch.record.operation.clone(), content.id.clone(), stamp);
    metadata.apply_batch(&[created, content], &[receipt])?;
    Ok(entity)
}

/// What became of a draft found at a start.
enum Recovery {
    /// It is published, under this entity, and gone.
    Sealed(String),
    /// There is nothing to publish: it matched what is published, or went to quarantine.
    Gone,
    /// It stays on disk: these objects are not here yet.
    Missing(Vec<(Digest, Kind)>),
    /// It stays on disk: its seal failed.
    SealFailed(anyhow::Error),
}

/// Recovery uses the last committed receipt, not wall-clock ordering of peers.
/// Only materialized chunks override the immutable base. A draft whose seal needs
/// what is not here must not keep the store from opening, since nothing can fetch
/// until it has: it stays on disk for [`retry_waiting`], which the sealer calls.
pub fn recover(store: &Mutex<Store>, layer: &WorkingLayer) -> Result<usize> {
    let mut recovered = 0;
    for (path, record) in layer.records()? {
        match recover_draft(store, layer, &path, record)? {
            Recovery::Sealed(_) => recovered += 1,
            Recovery::Gone => {}
            Recovery::Missing(objects) => {
                event!(warn, DRAFT_WAITS_FOR_OBJECTS, {"draft": path.display().to_string(), "objects": objects.len()});
                layer.wait_for_retry(path);
            }
            Recovery::SealFailed(error) => {
                event!(warn, DRAFT_SEAL_FAILED, {"draft": path.display().to_string(), "error": format!("{error:#}")});
                layer.wait_for_retry(path);
            }
        }
    }
    Ok(recovered)
}

/// Tries again what [`recover`] left on disk, asking for what it still needs. Nothing here waits
/// for a fetch, so it can run on the sealer's thread. Returns the entities it published, and a
/// failure that is not just a wait, for the caller to show; every draft stays until it seals.
pub fn retry_waiting(
    store: &Mutex<Store>,
    layer: &WorkingLayer,
) -> (Vec<String>, Option<anyhow::Error>) {
    let mut sealed = Vec::new();
    let mut failure = None;
    for path in layer.take_waiting() {
        let outcome = layer
            .record_of(&path)
            .and_then(|record| recover_draft(store, layer, &path, record));
        match outcome {
            Ok(Recovery::Sealed(entity)) => {
                event!(info, DRAFT_RECOVERED, {"draft": path.display().to_string()});
                sealed.push(entity);
            }
            Ok(Recovery::Gone) => {}
            Ok(Recovery::Missing(objects)) => {
                layer.content().hint(&objects);
                layer.wait_for_retry(path);
            }
            Ok(Recovery::SealFailed(error)) | Err(error) => {
                failure = Some(error);
                layer.wait_for_retry(path);
            }
        }
    }
    (sealed, failure)
}

fn recover_draft(
    store: &Mutex<Store>,
    layer: &WorkingLayer,
    path: &Path,
    mut record: ScratchRecord,
) -> Result<Recovery> {
    if !path.exists() {
        return Ok(Recovery::Gone);
    }
    // Namespace transactions may still be inside the short write-back
    // window. A missing metadata-only successor can rebase to its content
    // anchor; an uncommitted new entity is preserved for manual recovery.
    if lock(store).version(&record.basis.version)?.is_none() {
        if lock(store).version(&record.basis.anchor)?.is_some() {
            record.basis.version = record.basis.anchor.clone();
        } else {
            layer.quarantine(path)?;
            return Ok(Recovery::Gone);
        }
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    let stamp = modified_ns(&file)?;
    let file_len = file.metadata()?.len();
    // Capture the original overlay before rebasing to a committed receipt.
    // Bytes appended beyond that old base are materialized host bytes, not
    // missing ranges to refill from the receipt's older content. A file shorter
    // than its boundary was cut before the record could say so.
    let mut base_visible_len = record
        .base_visible_len
        .unwrap_or(record.basis.content.total_len)
        .min(file_len);
    record.base_visible_len = Some(base_visible_len);
    for index in record.basis.content.total_len.div_ceil(CHUNK_SIZE)..file_len.div_ceil(CHUNK_SIZE)
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
            remove_draft(path)?;
            return Ok(Recovery::Gone);
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
        remove_draft(path)?;
        return Ok(Recovery::Gone);
    }
    // Half of a replacement that had got no further than its cut: the file stands, and there is
    // nothing to put beside it.
    if record.replacing && file_len == 0 {
        drop(file);
        remove_draft(path)?;
        return Ok(Recovery::Gone);
    }
    let beside = record.replacing;
    // Only what the draft holds overrides its base. The rest keeps the chunks it had,
    // which need not be here.
    let mut dirty = crate::content::working::DirtyBitmap::default();
    for index in &record.hydrated {
        dirty.mark(index * CHUNK_SIZE, CHUNK_SIZE);
    }
    dirty.mark(base_visible_len, file_len.saturating_sub(base_visible_len));
    let entity = record.basis.entity.clone();
    let content = layer.content();
    let hydrated = record.hydrated.clone();
    let reference = record.basis.content.clone();
    let scratch = Arc::new(Mutex::new(Scratch {
        modified_time: Some(file_write_time(&file)?),
        explicit_modified_time: false,
        automatic_time_current: false,
        length: file_len,
        base_visible_len,
        hydrated,
        hydration_pending: false,
        torn: Default::default(),
        replacement_open: false,
        record,
        path: path.to_owned(),
        file: Some(file),
        dirty,
        changed: true,
        revision: 0,
        writers: 0,
        handles: 0,
        closed_at: None,
        idle_at: std::time::Instant::now(),
        // Nothing to keep yet: this asks for nothing it would have to wait for.
        pin: content.pin_visible(&reference, 0)?,
    }));
    match objects_a_seal_needs(&lock(&scratch), content) {
        Ok(missing) if missing.is_empty() => {}
        Ok(missing) => return Ok(Recovery::Missing(missing)),
        Err(error) => return Ok(Recovery::SealFailed(error)),
    }
    let sealed = content
        .pin_visible(&reference, base_visible_len)
        .and_then(|pin| {
            lock(&scratch).pin = pin;
            if beside {
                seal_beside(store, layer, &scratch)
            } else {
                seal_batch(store, layer, std::slice::from_ref(&scratch)).map(|_| entity)
            }
        });
    let entity = match sealed {
        Ok(entity) => entity,
        Err(error) => return Ok(Recovery::SealFailed(error)),
    };
    drop(scratch);
    remove_draft(path)?;
    Ok(Recovery::Sealed(entity))
}
fn remove_draft(path: &Path) -> Result<()> {
    std::fs::remove_file(path)?;
    std::fs::remove_file(path.with_extension("json"))?;
    Ok(())
}
fn scratch_matches(
    file: &std::fs::File,
    record: &ScratchRecord,
    reference: &FileContentRef,
    layer: &WorkingLayer,
) -> Result<bool> {
    if file.metadata()?.len() != reference.total_len {
        return Ok(false);
    }
    // Bytes that cannot be compared are not discarded.
    let Ok(chunks) = layer.content().chunks(reference) else {
        return Ok(false);
    };
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
