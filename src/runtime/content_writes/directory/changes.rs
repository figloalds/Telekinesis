//! Short metadata captures, entity ownership and FULL writes outside Engine.
use super::*;
use crate::core::{
    ROOT, Store,
    content_saves::{
        ContentBasicUpdate, DirectoryMutation, DirectoryMutationReceipt, DirectoryMutationRequest,
        DirectorySnapshot,
    },
};

pub(super) fn snapshot(
    shared: &Shared,
    writes: &Arc<NativeWrites>,
    store: &Store,
    entity: &str,
    basis: Option<&Entry>,
    context: &ReadContext,
) -> Result<DirectorySnapshot> {
    let mut snapshot = store.directory_snapshot(entity, context)?;
    if let Some(basis) = basis {
        ensure!(basis.id == entity, "STALE_CONTENT_DIRECTORY");
        if basis.revisions.get("alive") != snapshot.entry.revisions.get("alive") {
            let (request, receipt) = store
                .directory_handle_removal(basis, context)?
                .context("STALE_CONTENT_DIRECTORY")?;
            let removed = receipt
                .entries
                .iter()
                .find(|entry| entry.id == entity)
                .context("CORRUPT_CONTENT_SAVE")?;
            observe(shared, writes, &request, &receipt, context)?;
            snapshot = store.directory_snapshot(entity, context)?;
            ensure!(
                snapshot.entry.revisions.get("alive") == removed.revisions.get("alive"),
                "STALE_CONTENT_DIRECTORY"
            );
        }
    }
    Ok(snapshot)
}
pub(super) fn commit(
    store: &mut Store,
    q: &DirectoryMutationRequest,
    context: &ReadContext,
) -> Result<DirectoryMutationReceipt> {
    let receipt = match store.apply_directory_mutation(q, context) {
        Ok(receipt) => receipt,
        Err(error) => match store.directory_mutation_receipt(q, context)? {
            Some(receipt) => receipt,
            None => return Err(error),
        },
    };
    for entry in &receipt.entries {
        ensure!(
            store.directory_snapshot(&entry.id, context)?.entry == *entry,
            "STALE_CONTENT_DIRECTORY"
        );
    }
    Ok(receipt)
}
pub(super) fn observe(
    shared: &Shared,
    writes: &Arc<NativeWrites>,
    q: &DirectoryMutationRequest,
    receipt: &DirectoryMutationReceipt,
    context: &ReadContext,
) -> Result<()> {
    let mut e = engine(shared, context)?;
    ensure!(
        e.native_writes
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, writes)),
        "STALE_CONTENT_VIEW"
    );
    e.observe_directory_change(q, receipt)
}
pub(in crate::runtime::content_writes) fn modify_basic(
    shared: &Shared,
    handle: u64,
    wait: ContentWait,
    build: &impl Fn(u8) -> Result<(ContentBasicUpdate, u8)>,
) -> Result<bool> {
    let context = super::super::super::content_reads::request_context(shared, wait)?;
    let (writes, entity) = {
        let e = engine(shared, &context)?;
        let Some(writes) = e.native_writes.clone() else {
            return Ok(false);
        };
        let entry = e.handle_entry(handle)?;
        if entry.kind != "directory" || entry.id == ROOT {
            return Ok(false);
        }
        (writes, entry.id)
    };
    let context = writes.executor.namespace_context(&context)?;
    let _permit = match wait.slots {
        Some(slots) => Some(slots.take().context("CONTENT_RESOURCE_LIMIT")?),
        None => None,
    };
    let key = writes.key(&entity, &context)?;
    let _key = lock(&key, &context)?;
    let (basis, fields, disabled) = {
        let mut e = engine(shared, &context)?;
        let basis = e.handle_entry(handle)?;
        let h = e.handles.get(&handle).context("INVALID_HANDLE")?;
        ensure!(!h.cleaned, "INVALID_HANDLE");
        let (fields, disabled) = build(h.time_update_disabled)?;
        if fields.is_empty() {
            e.handles.get_mut(&handle).unwrap().time_update_disabled = disabled;
            return Ok(true);
        }
        (basis, fields, disabled)
    };
    let (q, receipt) = {
        let mut store = super::super::super::content_reads::retry_sql(&context, || {
            writes.stores.checkout_nowait()
        })?;
        let q = DirectoryMutationRequest {
            mutation: Uuid::new_v4(),
            before: snapshot(shared, &writes, &store, &entity, Some(&basis), &context)?,
            action: DirectoryMutation::Basic(fields),
        };
        let receipt = commit(&mut store, &q, &context)?;
        (q, receipt)
    };
    observe(shared, &writes, &q, &receipt, &context)?;
    let mut e = engine(shared, &context)?;
    ensure!(
        e.handle_entry(handle)?.revisions.get("alive") == receipt.entries[0].revisions.get("alive"),
        "STALE_CONTENT_DIRECTORY"
    );
    e.handles
        .get_mut(&handle)
        .context("INVALID_HANDLE")?
        .time_update_disabled = disabled;
    Ok(true)
}

/// A path-originated call supplies its observed source and target parent.
/// Windows instead supplies the exact retained source handle.
pub fn rename_directory(
    shared: &Shared,
    entity: &str,
    handle: Option<u64>,
    source: Option<&NamespaceTarget>,
    target: &NamespaceTarget,
    replace: bool,
    wait: ContentWait,
) -> Result<bool> {
    ensure!(
        target.path.len() <= 32_768 && source.is_none_or(|s| s.path.len() <= 32_768),
        "CONTENT_SAVE_CAPTURE_LIMIT"
    );
    crate::core::split_path(&target.path)?;
    let context = super::super::super::content_reads::request_context(shared, wait)?;
    let writes = {
        let e = engine(shared, &context)?;
        let Some(writes) = e.native_writes.clone() else {
            return Ok(false);
        };
        let entry = if let Some(handle) = handle {
            e.handle_entry(handle)?
        } else {
            e.store.entry(entity)?
        };
        ensure!(entry.id == entity, "INVALID_HANDLE");
        if entry.kind != "directory" || entry.id == ROOT {
            return Ok(false);
        }
        writes
    };
    let context = writes.executor.namespace_context(&context)?;
    let _permit = match wait.slots {
        Some(slots) => Some(slots.take().context("CONTENT_RESOURCE_LIMIT")?),
        None => None,
    };
    let (destination, _name) = {
        let e = engine(shared, &context)?;
        let parent = e.store.lookup(&crate::core::split_path(&target.path)?.0)?;
        ensure!(
            parent.id == target.parent && parent.kind == "directory",
            "STALE_CONTENT_DESTINATION"
        );
        let destination = match e.lookup_visible(&target.path) {
            Ok(entry) if entry.id == entity => None,
            Ok(entry) => {
                ensure!(replace, "NAME_COLLISION");
                ensure!(entry.kind == "directory", "NOT_DIRECTORY");
                Some(entry.id)
            }
            Err(error) if crate::model::errors::code_of(&error) == "FILE_NOT_FOUND" => None,
            Err(error) => return Err(error),
        };
        let name = writes.reserve_name((
            e.store.active.clone(),
            target.parent.clone(),
            crate::core::name_key(&crate::core::split_path(&target.path)?.1)?,
        ))?;
        (destination, name)
    };
    let mut entities = vec![entity.to_owned()];
    if let Some(d) = &destination {
        entities.push(d.clone())
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
    let basis = handle
        .map(|handle| engine(shared, &context)?.handle_entry(handle))
        .transpose()?;
    let (q, receipt) = {
        let mut store = super::super::super::content_reads::retry_sql(&context, || {
            writes.stores.checkout_nowait()
        })?;
        let before = snapshot(shared, &writes, &store, entity, basis.as_ref(), &context)?;
        let destination = destination
            .map(|id| snapshot(shared, &writes, &store, &id, None, &context).map(Box::new))
            .transpose()?;
        let q = DirectoryMutationRequest {
            mutation: Uuid::new_v4(),
            before,
            action: DirectoryMutation::Rename {
                source: source.cloned(),
                target: target.clone(),
                destination,
            },
        };
        let receipt = commit(&mut store, &q, &context)?;
        (q, receipt)
    };
    observe(shared, &writes, &q, &receipt, &context)?;
    Ok(true)
}

pub fn remove_directory(
    shared: &Shared,
    entity: &str,
    source: &NamespaceTarget,
    wait: ContentWait,
) -> Result<bool> {
    let context = super::super::super::content_reads::request_context(shared, wait)?;
    let writes = {
        let e = engine(shared, &context)?;
        let Some(writes) = e.native_writes.clone() else {
            return Ok(false);
        };
        if e.store.entry(entity)?.kind != "directory" || entity == ROOT {
            return Ok(false);
        }
        writes
    };
    let context = writes.executor.namespace_context(&context)?;
    let _permit = match wait.slots {
        Some(slots) => Some(slots.take().context("CONTENT_RESOURCE_LIMIT")?),
        None => None,
    };
    let _name = {
        let e = engine(shared, &context)?;
        writes.reserve_name((
            e.store.active.clone(),
            source.parent.clone(),
            crate::core::name_key(&crate::core::split_path(&source.path)?.1)?,
        ))?
    };
    let key = writes.key(entity, &context)?;
    let _key = lock(&key, &context)?;
    let (q, receipt) = {
        let mut store = super::super::super::content_reads::retry_sql(&context, || {
            writes.stores.checkout_nowait()
        })?;
        let q = DirectoryMutationRequest {
            mutation: Uuid::new_v4(),
            before: snapshot(shared, &writes, &store, entity, None, &context)?,
            action: DirectoryMutation::Remove {
                source: Some(source.clone()),
            },
        };
        let receipt = commit(&mut store, &q, &context)?;
        (q, receipt)
    };
    observe(shared, &writes, &q, &receipt, &context)?;
    Ok(true)
}
