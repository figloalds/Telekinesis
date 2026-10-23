//! Native existing-file namespace ordering, outside Engine during durability.
use super::*;
use crate::core::content_saves::NamespaceTarget;

pub(super) type NameSlot = (String, String, String);
pub(super) struct NameReservation {
    writes: Arc<NativeWrites>,
    slot: NameSlot,
    owner: uuid::Uuid,
}
impl Drop for NameReservation {
    fn drop(&mut self) {
        let mut names = crate::util::lock::lock(&self.writes.names);
        if names.get(&self.slot) == Some(&self.owner) {
            names.remove(&self.slot);
        }
    }
}
impl NativeWrites {
    // Only short map operations use this lock; no I/O or entity wait occurs.
    pub(crate) fn ensure_name_available(
        &self,
        branch: &str,
        parent: &str,
        name: &str,
    ) -> Result<()> {
        ensure!(
            !crate::util::lock::lock(&self.names).contains_key(&(
                branch.to_owned(),
                parent.to_owned(),
                name.to_owned()
            )),
            "CONTENT_RESOURCE_LIMIT: namespace destination is reserved"
        );
        Ok(())
    }
    pub(super) fn reserve_name(self: &Arc<Self>, slot: NameSlot) -> Result<NameReservation> {
        let mut names = crate::util::lock::lock(&self.names);
        ensure!(
            names.len() < limits::SAVE_QUEUE && !names.contains_key(&slot),
            "CONTENT_RESOURCE_LIMIT"
        );
        let owner = uuid::Uuid::new_v4();
        names.insert(slot.clone(), owner);
        Ok(NameReservation {
            writes: self.clone(),
            slot,
            owner,
        })
    }
}

pub fn rename(
    shared: &Shared,
    entity: &str,
    target: &str,
    replace: bool,
    wait: ContentWait,
) -> Result<bool> {
    ensure!(target.len() <= 32_768, "CONTENT_SAVE_CAPTURE_LIMIT");
    crate::core::split_path(target)?;
    let context = super::super::content_reads::request_context(shared, wait)?;
    let (writes, existing, retained) = {
        let mut e = engine(shared, &context)?;
        let Some(writes) = e.native_writes.clone() else {
            return Ok(false);
        };
        let existing = e.sessions.get(entity).and_then(|s| s.bytes.cow_file());
        let retained = existing
            .as_ref()
            .map(|file| e.retain_native_file_session(entity, file))
            .transpose()?;
        (writes, existing, retained)
    };
    let retained = retained.map(|handle| TemporaryHandle {
        shared: shared.clone(),
        handle,
    });
    let _adapter_permit = match wait.slots {
        Some(slots) => Some(slots.take().context("CONTENT_RESOURCE_LIMIT")?),
        None => None,
    };
    // A rename can arrive after the writer closed, or through a readonly
    // handle. Attach the current metadata owner without reading file bytes.
    let _source = if let Some(retained) = retained {
        Some(retained)
    } else {
        let mut e = engine(shared, &context)?;
        let mut entry = e.store.entry(entity)?;
        if entry.kind != "file" || entry.content_ref.is_none() {
            return Ok(false);
        }
        ensure!(entry.alive, "FILE_NOT_FOUND");
        let mut names = Vec::new();
        let mut length = 0usize;
        for _ in 0..4096 {
            if entry.id == crate::core::ROOT {
                break;
            }
            length += entry.name.len() + 1;
            ensure!(length <= 32_768, "CONTENT_SAVE_CAPTURE_LIMIT");
            names.push(entry.name);
            entry = if entry.parent == crate::core::ROOT {
                e.store.lookup("")?
            } else {
                e.store.entry(&entry.parent)?
            };
        }
        ensure!(entry.id == crate::core::ROOT, "INVALID_NAMESPACE_CYCLE");
        names.reverse();
        let path = names.join("/");
        ensure!(
            e.lookup_visible(&path)?.id == entity,
            "STALE_CONTENT_DESTINATION"
        );
        let handle = e.open(&path, None, false)?.0;
        drop(e);
        let temporary = TemporaryHandle {
            shared: shared.clone(),
            handle,
        };
        super::super::content_reads::prepare_open(shared, handle, wait)?;
        prepare_handle(shared, handle, true, &context)?;
        Some(temporary)
    };
    let file = match existing {
        Some(file) => file,
        None => engine(shared, &context)?
            .sessions
            .get(entity)
            .and_then(|s| s.bytes.cow_file())
            .context("STALE_CONTENT_VIEW")?,
    };
    let (destination, observed_target, _name_reservation) = {
        let e = engine(shared, &context)?;
        let (parent, name) = crate::core::split_path(target)?;
        let directory = e.store.lookup(&parent)?;
        ensure!(directory.kind == "directory", "NOT_DIRECTORY");
        let destination = match e.lookup_visible(target) {
            Ok(target) if target.id != entity => {
                ensure!(replace, "NAME_COLLISION");
                Some(target)
            }
            Ok(_) => None,
            Err(error) if crate::model::errors::code_of(&error) == "FILE_NOT_FOUND" => None,
            Err(error) => return Err(error),
        };
        let slot = (
            e.store.active.clone(),
            directory.id.clone(),
            crate::core::name_key(&name)?,
        );
        let reservation = writes.reserve_name(slot)?;
        (
            destination,
            NamespaceTarget {
                path: target.to_owned(),
                parent: directory.id,
            },
            reservation,
        )
    };
    // Promotion takes its single entity key itself. Keep the temporary handle
    // until replacement/observation finish, including a clean unopened target.
    let temporary = if let Some(destination) = &destination {
        let mut e = engine(shared, &context)?;
        ensure!(destination.kind == "file", "NAME_COLLISION");
        if let Some(file) = e
            .sessions
            .get(&destination.id)
            .and_then(|s| s.bytes.cow_file())
        {
            let handle = e.retain_native_file_session(&destination.id, &file)?;
            drop(e);
            Some(TemporaryHandle {
                shared: shared.clone(),
                handle,
            })
        } else {
            ensure!(
                destination.content_ref.is_some(),
                "UNSUPPORTED_FS_OPERATION: legacy destination promotion requires native create integration"
            );
            ensure!(
                e.lookup_visible(target)?.id == destination.id,
                "STALE_CONTENT_DESTINATION"
            );
            let handle = e.open(target, None, false)?.0;
            drop(e);
            let temporary = TemporaryHandle {
                shared: shared.clone(),
                handle,
            };
            super::super::content_reads::prepare_open(shared, handle, wait)?;
            prepare_handle(shared, handle, true, &context)?;
            Some(temporary)
        }
    } else {
        None
    };
    crate::util::fault::trigger("content.namespace.before_entity_gates")?;
    let mut entities = vec![entity.to_owned()];
    if let Some(destination) = &destination {
        entities.push(destination.id.clone());
    }
    entities.sort();
    entities.dedup();
    let keys = entities
        .iter()
        .map(|id| writes.key(id, &context))
        .collect::<Result<Vec<_>>>()?;
    let _keys = keys
        .iter()
        .map(|key| lock(key, &context))
        .collect::<Result<Vec<_>>>()?;
    let destination_file = {
        let e = engine(shared, &context)?;
        destination
            .as_ref()
            .map(|destination| {
                ensure!(
                    e.lookup_visible(target)?.id == destination.id,
                    "STALE_CONTENT_DESTINATION"
                );
                e.sessions
                    .get(&destination.id)
                    .and_then(|s| s.bytes.cow_file())
                    .context("STALE_CONTENT_VIEW")
            })
            .transpose()?
    };
    let mut files = vec![(entity, file.clone())];
    if let (Some(destination), Some(file)) = (&destination, &destination_file) {
        files.push((&destination.id, file.clone()));
    }
    files.sort_by(|a, b| a.0.cmp(b.0));
    let _mutations = files
        .iter()
        .map(|(_, file)| lock(&file.adapter_mutation, &context))
        .collect::<Result<Vec<_>>>()?;
    metadata::reconcile_metadata(shared, &file, &context)?;
    {
        let e = engine(shared, &context)?;
        ensure!(
            e.sessions
                .get(entity)
                .and_then(|s| s.bytes.cow_file())
                .is_some_and(|current| Arc::ptr_eq(&current, &file)),
            "STALE_CONTENT_VIEW"
        );
        if let (Some(destination), Some(file)) = (&destination, &destination_file) {
            ensure!(
                e.sessions
                    .get(&destination.id)
                    .and_then(|s| s.bytes.cow_file())
                    .is_some_and(|f| Arc::ptr_eq(&f, file)),
                "STALE_CONTENT_VIEW"
            );
        }
    }
    // A destination appearing after the observation must cause collision,
    // never unreviewed replacement of a newly opened/live native entity.
    writes
        .executor
        .rename_file(file.clone(), &observed_target, destination_file, &context)?;
    drop(temporary);
    Ok(true)
}

/// Remove exactly the observed entity/name, preserving existing native handles.
/// A retired handle never removes a replacement that reused its former name.
pub fn unlink(shared: &Shared, entity: &str, path: &str, wait: ContentWait) -> Result<bool> {
    unlink_inner(shared, entity, path, false, wait)
}
pub(super) fn unlink_inner(
    shared: &Shared,
    entity: &str,
    path: &str,
    durable: bool,
    wait: ContentWait,
) -> Result<bool> {
    ensure!(path.len() <= 32_768, "CONTENT_SAVE_CAPTURE_LIMIT");
    let context = super::super::content_reads::request_context(shared, wait)?;
    let (writes, entry, existing) = {
        let e = engine(shared, &context)?;
        let Some(writes) = e.native_writes.clone() else {
            return Ok(false);
        };
        let entry = e.store.entry(entity)?;
        if entry.kind != "file" || entry.content_ref.is_none() {
            return Ok(false);
        }
        let existing = e.sessions.get(entity).and_then(|s| s.bytes.cow_file());
        (writes, entry, existing)
    };
    let _permit = match wait.slots {
        Some(slots) => Some(slots.take().context("CONTENT_RESOURCE_LIMIT")?),
        None => None,
    };
    if !entry.alive {
        let file = existing.context("FILE_NOT_FOUND")?;
        let key = writes.key(entity, &context)?;
        let _key = lock(&key, &context)?;
        let _mutation = lock(&file.adapter_mutation, &context)?;
        metadata::reconcile_metadata(shared, &file, &context)?;
        ensure!(
            writes.executor.retired_file(&file, &context)?,
            "FILE_NOT_FOUND"
        );
        return Ok(true);
    }
    let (target, _name) = {
        let e = engine(shared, &context)?;
        let current = e.lookup_visible(path)?;
        ensure!(current.id == entity, "STALE_CONTENT_DESTINATION");
        let reservation = writes.reserve_name((
            e.store.active.clone(),
            current.parent.clone(),
            crate::core::name_key(&current.name)?,
        ))?;
        (
            NamespaceTarget {
                path: path.to_owned(),
                parent: current.parent,
            },
            reservation,
        )
    };
    let temporary = if existing.is_none() {
        let mut e = engine(shared, &context)?;
        ensure!(
            e.lookup_visible(path)?.id == entity,
            "STALE_CONTENT_DESTINATION"
        );
        let handle = e.open(path, None, false)?.0;
        drop(e);
        let temporary = TemporaryHandle {
            shared: shared.clone(),
            handle,
        };
        super::super::content_reads::prepare_open(shared, handle, wait)?;
        prepare_handle(shared, handle, true, &context)?;
        Some(temporary)
    } else {
        None
    };
    let key = writes.key(entity, &context)?;
    let _key = lock(&key, &context)?;
    let file = {
        let e = engine(shared, &context)?;
        ensure!(
            e.lookup_visible(path)?.id == entity,
            "STALE_CONTENT_DESTINATION"
        );
        e.sessions
            .get(entity)
            .and_then(|s| s.bytes.cow_file())
            .context("STALE_CONTENT_VIEW")?
    };
    let _mutation = lock(&file.adapter_mutation, &context)?;
    metadata::reconcile_metadata(shared, &file, &context)?;
    writes
        .executor
        .unlink_file(file.clone(), &target, durable, &context)?;
    drop(temporary);
    Ok(true)
}

struct TemporaryHandle {
    shared: Shared,
    handle: u64,
}
#[cfg(windows)]
pub(crate) fn retired_cleanup(shared: &Shared, handle: u64, context: &ReadContext) -> Result<bool> {
    let Some(file) = super::file(shared, handle, context)? else {
        return Ok(false);
    };
    let _mutation = lock(&file.adapter_mutation, context)?;
    metadata::reconcile_metadata(shared, &file, context)?;
    let writes = engine(shared, context)?
        .native_writes
        .clone()
        .context("PEER_NEEDS_UPGRADE")?;
    writes.executor.retired_file(&file, context)
}
impl Drop for TemporaryHandle {
    fn drop(&mut self) {
        let _ = super::super::lock_engine(&self.shared).release_handle(self.handle);
    }
}
