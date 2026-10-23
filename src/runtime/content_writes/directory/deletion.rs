//! Durable directory dispositions never allocate a private file session.
use super::*;
use crate::core::{
    ROOT, Store,
    content_saves::{DirectoryMutation, DirectoryMutationRequest},
};

pub(in crate::runtime::content_writes) fn set_delete(
    shared: &Shared,
    handle: u64,
    delete: bool,
    wait: ContentWait,
) -> Result<bool> {
    let context = super::super::super::content_reads::request_context(shared, wait)?;
    let (writes, entity, request) = {
        let e = engine(shared, &context)?;
        let Some(writes) = e.native_writes.clone() else {
            return Ok(false);
        };
        let entry = e.handle_entry(handle)?;
        if entry.kind != "directory" || entry.id == ROOT {
            return Ok(false);
        }
        let h = e.handles.get(&handle).context("INVALID_HANDLE")?;
        ensure!(!h.cleaned, "INVALID_HANDLE");
        (writes, entry.id, h.delete_request)
    };
    let context = writes.executor.namespace_context(&context)?;
    let _permit = match wait.slots {
        Some(slots) => Some(slots.take().context("CONTENT_RESOURCE_LIMIT")?),
        None => None,
    };
    let key = writes.key(&entity, &context)?;
    let _key = lock(&key, &context)?;
    let basis = engine(shared, &context)?.handle_entry(handle)?;
    let mut store = super::super::super::content_reads::retry_sql(&context, || {
        writes.stores.checkout_nowait()
    })?;
    if !delete {
        if let Err(error) = store.cancel_directory_delete(request, &entity, &context)
            && store.directory_delete_intent(request, &context)?.is_some()
        {
            return Err(error);
        }
        return Ok(true);
    }
    let snapshot = changes::snapshot(shared, &writes, &store, &entity, Some(&basis), &context)?;
    if !snapshot.entry.alive {
        ensure!(
            store.directory_is_removed(&entity, &context)?,
            "FILE_NOT_FOUND"
        );
        return Ok(true);
    }
    if let Err(error) = store.stage_directory_delete(request, &snapshot, &context) {
        let Some(record) = store.directory_delete_intent(request, &context)? else {
            return Err(error);
        };
        ensure!(record.snapshot == snapshot, "STALE_CONTENT_DIRECTORY");
        store.validate_directory_delete(&record, &context)?;
    }
    Ok(true)
}

pub(in crate::runtime::content_writes) fn complete_delete(
    shared: &Shared,
    handle: u64,
    wait: ContentWait,
) -> Result<bool> {
    let context = super::super::super::content_reads::request_context(shared, wait)?;
    let (writes, entity, request) = {
        let e = engine(shared, &context)?;
        let Some(writes) = e.native_writes.clone() else {
            return Ok(false);
        };
        let entry = e.handle_entry(handle)?;
        if entry.kind != "directory" || entry.id == ROOT {
            return Ok(false);
        }
        (writes, entry.id, e.handles[&handle].delete_request)
    };
    let context = writes.executor.namespace_context(&context)?;
    let _permit = match wait.slots {
        Some(slots) => Some(slots.take().context("CONTENT_RESOURCE_LIMIT")?),
        None => None,
    };
    let key = writes.key(&entity, &context)?;
    let _key = lock(&key, &context)?;
    let basis = engine(shared, &context)?.handle_entry(handle)?;
    let outcome = {
        let mut store = super::super::super::content_reads::retry_sql(&context, || {
            writes.stores.checkout_nowait()
        })?;
        let current = changes::snapshot(shared, &writes, &store, &entity, Some(&basis), &context)?;
        if !current.entry.alive {
            ensure!(
                store.directory_is_removed(&entity, &context)?,
                "FILE_NOT_FOUND"
            );
            None
        } else {
            let row = store
                .directory_delete_intent(request, &context)?
                .context("CONTENT_METADATA_RECEIPT_LOST: unstaged directory cleanup")?;
            ensure!(row.snapshot == current, "STALE_CONTENT_DIRECTORY");
            store.validate_directory_delete(&row, &context)?;
            let q = DirectoryMutationRequest {
                mutation: request,
                before: row.snapshot,
                action: DirectoryMutation::Remove { source: None },
            };
            let receipt = changes::commit(&mut store, &q, &context)?;
            Some((q, receipt))
        }
    };
    if let Some((q, receipt)) = outcome {
        changes::observe(shared, &writes, &q, &receipt, &context)?;
    }
    Ok(true)
}
pub(in crate::runtime::content_writes) fn recover_pending(
    store: &mut Store,
    context: &ReadContext,
) -> Result<()> {
    for pass in 0..=limits::SAVE_QUEUE {
        let Some(row) = store
            .directory_delete_intents(None, 1, context)?
            .into_iter()
            .next()
        else {
            return Ok(());
        };
        ensure!(pass < limits::SAVE_QUEUE, "CONTENT_RESOURCE_LIMIT");
        store.validate_directory_delete(&row, context)?;
        let q = DirectoryMutationRequest {
            mutation: row.request,
            before: row.snapshot,
            action: DirectoryMutation::Remove { source: None },
        };
        changes::commit(store, &q, context)?;
    }
    anyhow::bail!("CONTENT_RESOURCE_LIMIT")
}
