//! Per-handle fallible disposition and restart of requests accepted by Windows.
use super::*;

/// Stage/cancel before SetDelete or a delete-on-close open is acknowledged.
/// It does not remove the name; Windows chooses the later Cleanup boundary.
pub fn set_delete_disposition(
    shared: &Shared,
    handle: u64,
    delete: bool,
    wait: ContentWait,
) -> Result<bool> {
    if directory::set_delete(shared, handle, delete, wait)? {
        return Ok(true);
    }
    let context = super::super::content_reads::request_context(shared, wait)?;
    let (writes, entity, request) = {
        let e = engine(shared, &context)?;
        let Some(writes) = e.native_writes.clone() else {
            return Ok(false);
        };
        let session = e.session(handle)?;
        if session.entry.kind != "file" || session.entry.content_ref.is_none() {
            return Ok(false);
        }
        ensure!(
            !e.handles.get(&handle).context("INVALID_HANDLE")?.cleaned,
            "INVALID_HANDLE"
        );
        (
            writes,
            session.entry.id.clone(),
            e.handles[&handle].delete_request,
        )
    };
    if !delete {
        let key = writes.key(&entity, &context)?;
        let _key = lock(&key, &context)?;
        let file = super::file(shared, handle, &context)?;
        let _mutation = file
            .as_ref()
            .map(|file| lock(&file.adapter_mutation, &context))
            .transpose()?;
        let mut store =
            super::super::content_reads::retry_sql(&context, || writes.stores.checkout_nowait())?;
        let Some(record) = store.content_delete_intent(request, &context)? else {
            return Ok(true);
        };
        ensure!(record.entity == entity, "INVALID_HANDLE");
        if let Err(error) = store.cancel_content_delete(request, record.operation, &context)
            && store.content_delete_intent(request, &context)?.is_some()
        {
            return Err(error);
        }
        return Ok(true);
    }
    super::super::content_reads::prepare_open(shared, handle, wait)?;
    prepare_handle(shared, handle, true, &context)?;
    let key = writes.key(&entity, &context)?;
    let _key = lock(&key, &context)?;
    let file = super::file(shared, handle, &context)?.context("CONTENT_SAVE_INTENT_MISMATCH")?;
    let _mutation = lock(&file.adapter_mutation, &context)?;
    metadata::reconcile_metadata(shared, &file, &context)?;
    while writes.executor.adopt_committed(file.clone(), &context)? {
        context.check()?;
    }
    let operation = file.operation();
    let mut store =
        super::super::content_reads::retry_sql(&context, || writes.stores.checkout_nowait())?;
    let record = match store.stage_content_delete(request, operation, &context) {
        Ok(record) => record,
        Err(error) => match store.content_delete_intent(request, &context)? {
            Some(record) => {
                store.validate_content_delete_stage(&record, &context)?;
                Some(record)
            }
            None => return Err(error),
        },
    };
    if let Some(record) = record {
        ensure!(
            record.operation == operation && record.entity == entity,
            "CONTENT_SAVE_INTENT_MISMATCH"
        );
    }
    Ok(true)
}
pub fn complete_delete_disposition(
    shared: &Shared,
    handle: u64,
    path: &str,
    wait: ContentWait,
) -> Result<bool> {
    if directory::complete_delete(shared, handle, wait)? {
        return Ok(true);
    }
    let context = super::super::content_reads::request_context(shared, wait)?;
    let entity = engine(shared, &context)?.session(handle)?.entry.id.clone();
    super::namespace::unlink_inner(shared, &entity, path, true, wait)
}

/// All old OS handles are gone at restart. Consume bounded pending dispositions
/// before normal Saved publication, so recovery cannot resurrect their names.
pub(super) fn recover_pending(
    store: &mut crate::core::Store,
    journal: &Arc<WriteJournal>,
    provider: Arc<dyn ContentProvider>,
    context: &ReadContext,
) -> Result<()> {
    let mut count = 0;
    loop {
        let Some(record) = store
            .content_delete_intents(None, 1, context)?
            .into_iter()
            .next()
        else {
            return Ok(());
        };
        ensure!(count < limits::SAVE_QUEUE, "CONTENT_RESOURCE_LIMIT");
        count += 1;
        let current = store.inspect_content_delete(&record, context)?;
        if !journal.has_saved_checkpoint(&current, context)? {
            let intent = store.validate_content_delete(&record, context)?;
            let target = store.content_delete_target(&record, context)?;
            // There are no promised private bytes or old handles to adopt.
            // Keep uncheckpointed Working intact; it has no publication grant.
            store.apply_content_checkpoint_unlink(
                crate::core::content_saves::NamespaceDestination {
                    intent,
                    checkpoint: None,
                },
                record.request,
                &target,
                context,
            )?;
            continue;
        }
        let mut session = journal.resume(record.operation, provider.clone(), context)?;
        // Recover exact committed byte parents before selecting the current
        // private checkpoint, including a crash between Core and journal adoption.
        for pass in 0..=limits::SAVE_QUEUE {
            let intent = session.intent(context)?;
            let Some(saved) = store.committed_content_checkpoint(&intent, context)? else {
                break;
            };
            ensure!(pass < limits::SAVE_QUEUE, "CONTENT_RESOURCE_LIMIT");
            let continuation = store.continue_content_save(&saved, context)?;
            session = session.adopt_continuation(&continuation, provider.clone(), context)?;
        }
        let intent = store.validate_content_delete(&record, context)?;
        ensure!(session.intent(context)? == intent, "STALE_CONTENT_SAVE");
        let target = store.content_delete_target(&record, context)?;
        let checkpoint = session.checkpoint(context)?;
        store.apply_content_checkpoint_unlink(
            crate::core::content_saves::NamespaceDestination {
                intent: intent.clone(),
                checkpoint,
            },
            record.request,
            &target,
            context,
        )?;
        let continuation = store
            .content_namespace_continuation(&intent, context)?
            .context("CONTENT_METADATA_CONTINUATION_LOST")?;
        session.adopt_namespace_continuation(&continuation, context)?;
    }
}
