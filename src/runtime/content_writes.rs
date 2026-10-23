//! Native mutable sessions initialized before the worker accepts requests.
use super::{
    EngineGuard, Shared,
    content::ContentWait,
    content_saves::{SaveExecutor, SaveFile},
    ctx::SyncStorePool,
};
use crate::{
    content::{ContentProvider, ReadContext, write::WriteJournal},
    core::content_saves::ContentSaveSelection,
    model::workspace::limits,
    staging::Staged,
};
use anyhow::{Context, Result, ensure};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, TryLockError, Weak},
    time::Duration,
};
mod create;
mod deletion;
mod directory;
mod metadata;
mod namespace;
pub use create::{CreateOptions, create};
#[cfg(windows)]
pub(crate) use deletion::complete_delete_disposition as unlink_on_cleanup;
pub use deletion::{complete_delete_disposition, set_delete_disposition};
pub use directory::{create_directory, remove_directory, rename_directory};
pub use metadata::{set_basic, update_basic};
pub use namespace::rename;
#[cfg(windows)]
pub(crate) use namespace::retired_cleanup;
pub use namespace::unlink;

pub struct NativeWrites {
    pub(crate) journal: Arc<WriteJournal>,
    executor: SaveExecutor,
    target: Arc<crate::content::LocalContent>,
    empty: crate::model::content::FileContentRef,
    _empty_pin: crate::content::PinnedContent,
    stores: SyncStorePool,
    keys: Mutex<BTreeMap<String, Weak<Mutex<()>>>>,
    names: Mutex<BTreeMap<(String, String, String), uuid::Uuid>>,
}
fn lock<'a, T>(mutex: &'a Mutex<T>, context: &ReadContext) -> Result<MutexGuard<'a, T>> {
    loop {
        context.check()?;
        match mutex.try_lock() {
            Ok(g) => return Ok(g),
            Err(TryLockError::Poisoned(_)) => anyhow::bail!("WRITE_JOURNAL_POISONED"),
            Err(TryLockError::WouldBlock) => {
                context.cancel.sleep(Duration::from_millis(1));
            }
        }
    }
}
fn engine<'a>(shared: &'a Shared, context: &ReadContext) -> Result<EngineGuard<'a>> {
    loop {
        context.check()?;
        if let Some(g) = super::try_lock_engine(shared) {
            ensure!(!g.switching, "STALE_CONTENT_VIEW");
            ensure!(
                !g.stopping.load(std::sync::atomic::Ordering::Acquire),
                "CONTENT_CANCELLED"
            );
            return Ok(g);
        }
        context.cancel.sleep(Duration::from_millis(1));
    }
}
impl NativeWrites {
    /// Initialize the current write service before mounting/opening handles.
    /// Current restarts recover the private journal before accepting requests.
    pub fn install(shared: &Shared, context: &ReadContext) -> Result<Arc<Self>> {
        let existing = engine(shared, context)?.native_writes.clone();
        if let Some(writes) = existing {
            writes.executor.namespace_context(context)?;
            return Ok(writes);
        }
        let (root, repo, device, branch, generation) = {
            let e = engine(shared, context)?;
            ensure!(
                !e.mounted
                    && e.sessions.is_empty()
                    && e.handles.is_empty()
                    && e.native_writes.is_none(),
                "CONTENT_RESOURCE_LIMIT"
            );
            (
                e.store.root.clone(),
                e.store.repo.clone(),
                e.store.device.clone(),
                e.store.active.clone(),
                e.store.generation,
            )
        };
        let stores = super::worker_of(shared).map_or_else(
            || SyncStorePool::new(&root, repo, device),
            |w| w.sync_stores.clone(),
        );
        let mut store = stores.checkout_nowait()?;
        // Validate private state before directory recovery can commit metadata.
        let journal = WriteJournal::open(&root)?;
        directory::recover_pending(&mut store, context)?;
        let target = Arc::new(crate::content::LocalContent::open(&root)?);
        let empty = target.import(&mut [].as_slice(), 0, context)?;
        let empty_pin = target.pin(&empty)?;
        let provider: Arc<dyn crate::content::ContentProvider> = target.clone();
        deletion::recover_pending(&mut store, &journal, provider.clone(), context)?;
        let mut recovered: Vec<(String, crate::core::Entry, Arc<SaveFile>, bool)> = Vec::new();
        let mut after = None;
        loop {
            let page = journal.pending(after, 64, context)?;
            if page.is_empty() {
                break;
            }
            after = page.last().map(|c| c.intent.operation);
            for checkpoint in page.into_iter().filter(|c| c.needs_publication()) {
                crate::startup::progress("recovering native saves");
                crate::util::fault::hang("content.write.recover_saved");
                ensure!(
                    recovered.len() < limits::SAVE_QUEUE,
                    "CONTENT_RESOURCE_LIMIT"
                );
                ensure!(
                    checkpoint.intent.branch == branch,
                    "RECOVERY_REQUIRED: native save belongs to another branch"
                );
                let entry = store.entry(&checkpoint.intent.base.entity)?;
                ensure!(
                    !recovered.iter().any(|(id, ..)| id == &entry.id),
                    "RECOVERY_REQUIRED: competing native operations"
                );
                let mut session =
                    journal.resume(checkpoint.intent.operation, provider.clone(), context)?;
                if let Some(handoff) =
                    store.content_namespace_continuation(&checkpoint.intent, context)?
                {
                    session = session.adopt_namespace_continuation(&handoff, context)?;
                }
                let retired = store.content_save_is_retired(&session.intent(context)?, context)?;
                ensure!(
                    entry.kind == "file" && (entry.alive || retired),
                    "RECOVERY_REQUIRED: native save namespace changed"
                );
                let file =
                    SaveFile::recovered(super::content_base::attach(shared, generation, session));
                recovered.push((entry.id.clone(), entry, file, retired));
            }
        }
        drop(store);
        let executor = SaveExecutor::start(shared, journal.clone(), context)?;
        let writes = Arc::new(Self {
            journal,
            executor,
            target,
            empty,
            _empty_pin: empty_pin,
            stores,
            keys: Mutex::new(BTreeMap::new()),
            names: Mutex::new(BTreeMap::new()),
        });
        let mut e = engine(shared, context)?;
        ensure!(
            e.store.root == root
                && e.store.active == branch
                && e.store.generation == generation
                && !e.mounted
                && e.sessions.is_empty()
                && e.handles.is_empty()
                && e.native_writes.is_none(),
            "STALE_CONTENT_VIEW"
        );
        e.vitals.install_native_journal(&writes.journal)?;
        e.native_writes = Some(writes.clone());
        let mut retired = Vec::new();
        for (id, mut entry, file, is_retired) in recovered {
            if is_retired {
                retired.push(id.clone());
            }
            entry.basic = Some(e.store.content_save_basic_info(
                file.operation(),
                file.basic_state(),
                context,
            )?);
            if let Some(times) = file.native_times() {
                entry.modified_ms = times.modified_ms;
            }
            e.sessions.insert(
                id,
                super::Session {
                    content_base: entry.revisions.get("content").cloned(),
                    alive_base: entry.revisions.get("alive").cloned(),
                    entry,
                    bytes: Staged::Cow(file),
                    dirty: true,
                    references: 0,
                    preserve_times: 0,
                    edits: 0,
                    saving: false,
                    queued: false,
                },
            );
        }
        e.publish_observation();
        drop(e);
        super::Engine::adopt_shared(shared);
        for entity in retired {
            super::engine::queue_recovered_native(shared, &entity);
        }
        Ok(writes)
    }
    pub fn shutdown(&self, context: &ReadContext) -> Result<()> {
        self.executor.shutdown(context)
    }
    fn key(&self, entity: &str, context: &ReadContext) -> Result<Arc<Mutex<()>>> {
        let mut keys = lock(&self.keys, context)?;
        keys.retain(|_, key| key.strong_count() > 0);
        if let Some(key) = keys.get(entity).and_then(Weak::upgrade) {
            return Ok(key);
        }
        ensure!(keys.len() < limits::SAVE_QUEUE, "CONTENT_RESOURCE_LIMIT");
        let key = Arc::new(Mutex::new(()));
        keys.insert(entity.into(), Arc::downgrade(&key));
        Ok(key)
    }
}
/// Stop admissions immediately after the worker has retained dirty state.
pub(crate) fn stop_worker(shared: &Shared) {
    let writes = super::lock_engine(shared).native_writes.clone();
    if let Some(writes) = writes {
        writes.executor.stop();
    }
}
/// Drain without holding Engine; the executor keeps its existing bounded wait.
pub(crate) fn drain_worker(shared: &Shared, context: &ReadContext) -> Result<()> {
    let writes = super::lock_engine(shared).native_writes.clone();
    if let Some(writes) = writes {
        writes.shutdown(context)?;
    }
    Ok(())
}
/// Called after the descriptor's immutable provider pin has been installed.
pub(crate) fn prepare_open(shared: &Shared, handle: u64, context: &ReadContext) -> Result<()> {
    prepare_handle(shared, handle, false, context)
}
fn prepare_handle(
    shared: &Shared,
    handle: u64,
    metadata: bool,
    context: &ReadContext,
) -> Result<()> {
    let (writes, entity) = {
        let e = engine(shared, context)?;
        let Some(writes) = e.native_writes.clone() else {
            return Ok(());
        };
        let h = e.handles.get(&handle).context("INVALID_HANDLE")?;
        if (!h.writable && !metadata) || e.session(handle)?.bytes.cow_file().is_some() {
            return Ok(());
        }
        (writes, h.entity.clone())
    };
    let key = writes.key(&entity, context)?;
    let _key = lock(&key, context)?;
    let (selected, selection, provider, root) = {
        let e = engine(shared, context)?;
        let session = e.session(handle)?;
        if session.bytes.cow_file().is_some() {
            return Ok(());
        }
        let Some(selection) = session.bytes.content_selection() else {
            return Ok(());
        };
        let binding = selection.binding().context("CONTENT_UNAVAILABLE")?;
        let selected = ContentSaveSelection {
            entity: entity.clone(),
            branch: e.store.active.clone(),
            generation: e.store.generation,
            content: selection.reference().clone(),
            content_parent: session.content_base.clone(),
            alive_parent: session.alive_base.clone(),
        };
        (selected, selection, binding.provider, e.store.root.clone())
    };
    let mut store = loop {
        context.check()?;
        match writes.stores.checkout_nowait() {
            Ok(s) => break s,
            Err(err) if crate::model::errors::code_of(&err) == "SQLITE_BUSY" => {
                context.cancel.sleep(Duration::from_millis(1));
            }
            Err(err) => return Err(err),
        }
    };
    crate::util::fault::trigger("content.native.before_capture")?;
    let intent = store.begin_selected_content_save(&selected, context)?;
    drop(store);
    let session = writes.journal.create(intent, provider, context)?;
    let file = SaveFile::new(super::content_base::attach(
        shared,
        selected.generation,
        session,
    ));
    let mut e = engine(shared, context)?;
    ensure!(
        e.store.root == root
            && e.store.active == selected.branch
            && e.store.generation == selected.generation,
        "STALE_CONTENT_VIEW"
    );
    ensure!(
        e.handles
            .get(&handle)
            .is_some_and(|h| (h.writable || metadata) && !h.cleaned),
        "INVALID_HANDLE"
    );
    let session = e.session_mut(handle)?;
    ensure!(
        session
            .bytes
            .content_selection()
            .is_some_and(|s| Arc::ptr_eq(&s, &selection)),
        "STALE_CONTENT_VIEW"
    );
    session.bytes = Staged::Cow(file);
    Ok(())
}
pub(crate) fn file(
    shared: &Shared,
    handle: u64,
    context: &ReadContext,
) -> Result<Option<Arc<SaveFile>>> {
    loop {
        if let Some(e) = super::try_lock_engine(shared) {
            let file = e.session(handle)?.bytes.cow_file();
            if file.is_some() {
                context.check()?;
                ensure!(!e.switching, "STALE_CONTENT_VIEW");
                ensure!(
                    !e.stopping.load(std::sync::atomic::Ordering::Acquire),
                    "CONTENT_CANCELLED"
                );
            }
            return Ok(file);
        }
        context.check()?;
        context.cancel.sleep(Duration::from_millis(1));
    }
}
struct Mutation {
    file: Arc<SaveFile>,
    target: Arc<crate::content::LocalContent>,
    time_update_disabled: u8,
    prior_times: [u64; 2],
    was_dirty: bool,
    edit: u64,
}
fn observe_mutation(shared: &Shared, handle: u64, mutation: &Mutation, before: u64) {
    let file = &mutation.file;
    let unchanged = file.visible_sequence() == before;
    let times = if unchanged { None } else { file.native_times() };
    // Mirror even a partially committed write that lost its reply. Only
    // memory is touched under Engine; no journal/provider access occurs.
    let mut e = super::lock_engine(shared);
    if let Ok(session) = e.session_mut(handle)
        && session
            .bytes
            .cow_file()
            .is_some_and(|current| Arc::ptr_eq(&current, file))
    {
        // The early marker fences checkout during off-lock preflight. Refusal
        // or a zero-byte write must release only this operation's marker;
        // prior dirty bytes and a newer session edit remain untouched.
        if unchanged {
            if session.edits == mutation.edit {
                session.dirty = mutation.was_dirty;
            }
            return;
        }
        let Some(times) = times else {
            return;
        };
        let mut basic = session.entry.basic_info();
        basic.write_time = times.write_time;
        basic.change_time = times.change_time;
        basic.attributes = (basic.attributes & !0x80) | 0x20;
        session.entry.basic = Some(basic);
        session.entry.modified_ms = times.modified_ms;
    }
}
fn mutate(shared: &Shared, handle: u64, context: &ReadContext) -> Result<Option<Mutation>> {
    let mut e = engine(shared, context)?;
    let h = e.handles.get(&handle).context("INVALID_HANDLE")?;
    ensure!(h.writable && !h.cleaned, "STALE_VIEW_OR_ACCESS_DENIED");
    let time_update_disabled = h.time_update_disabled & 6;
    let target = e
        .native_writes
        .as_ref()
        .context("PEER_NEEDS_UPGRADE")?
        .target
        .clone();
    let session = e.session_mut(handle)?;
    let Some(file) = session.bytes.cow_file() else {
        return Ok(None);
    };
    let basic = session.entry.basic_info();
    let prior_times = [basic.write_time, basic.change_time];
    let edit = session
        .edits
        .checked_add(1)
        .context("WRITE_GENERATION_EXHAUSTED")?;
    let was_dirty = session.dirty;
    session.dirty = true;
    session.edits = edit;
    Ok(Some(Mutation {
        file,
        target,
        time_update_disabled,
        prior_times,
        was_dirty,
        edit,
    }))
}
pub fn write(
    shared: &Shared,
    handle: u64,
    offset: u64,
    bytes: &[u8],
    append: bool,
    constrained: bool,
    wait: ContentWait,
) -> Result<Option<usize>> {
    let context = super::content_reads::request_context(shared, wait)?;
    let Some(selected) = file(shared, handle, &context)? else {
        return Ok(None);
    };
    let _adapter_permit = match wait.slots {
        Some(slots) => Some(slots.take().context("CONTENT_RESOURCE_LIMIT")?),
        None => None,
    };
    let _mutation = lock(&selected.adapter_mutation, &context)?;
    let _bytes = lock(&selected.byte_mutation, &context)?;
    metadata::reconcile_metadata(shared, &selected, &context)?;
    ensure!(!(wait.cancelled)(), "CONTENT_CANCELLED");
    let mutation = mutate(shared, handle, &context)?.context("STALE_CONTENT_VIEW")?;
    let before = mutation.file.visible_sequence();
    let result = (|| {
        ensure!(Arc::ptr_eq(&mutation.file, &selected), "STALE_CONTENT_VIEW");
        crate::util::fault::trigger("content.native.before_write")?;
        mutation.file.write_native(
            offset,
            bytes,
            append,
            constrained,
            crate::content::write::NativeTimePolicy {
                disabled: mutation.time_update_disabled,
                prior: mutation.prior_times,
            },
            crate::content::write::PreflightContext {
                target: &mutation.target,
                limits: Some(crate::content::write::BaseWaitLimits {
                    stall: wait.stall,
                    cancelled: wait.cancelled,
                }),
                context: &context,
            },
        )
    })();
    observe_mutation(shared, handle, &mutation, before);
    let count = result?;
    ensure!(!(wait.cancelled)(), "CONTENT_CANCELLED");
    Ok(Some(count))
}
pub fn truncate(
    shared: &Shared,
    handle: u64,
    length: u64,
    allocation: bool,
    wait: ContentWait,
) -> Result<bool> {
    let context = super::content_reads::request_context(shared, wait)?;
    let Some(file) = file(shared, handle, &context)? else {
        return Ok(false);
    };
    let _adapter_permit = match wait.slots {
        Some(slots) => Some(slots.take().context("CONTENT_RESOURCE_LIMIT")?),
        None => None,
    };
    let _mutation = lock(&file.adapter_mutation, &context)?;
    let _bytes = lock(&file.byte_mutation, &context)?;
    metadata::reconcile_metadata(shared, &file, &context)?;
    ensure!(!(wait.cancelled)(), "CONTENT_CANCELLED");
    if !allocation || length < file.visible_len() {
        let mutation = mutate(shared, handle, &context)?.context("STALE_CONTENT_VIEW")?;
        let before = mutation.file.visible_sequence();
        let result = (|| {
            ensure!(Arc::ptr_eq(&mutation.file, &file), "STALE_CONTENT_VIEW");
            mutation.file.set_len_native(
                length,
                crate::content::write::NativeTimePolicy {
                    disabled: mutation.time_update_disabled,
                    prior: mutation.prior_times,
                },
                crate::content::write::PreflightContext {
                    target: &mutation.target,
                    limits: Some(crate::content::write::BaseWaitLimits {
                        stall: wait.stall,
                        cancelled: wait.cancelled,
                    }),
                    context: &context,
                },
            )
        })();
        observe_mutation(shared, handle, &mutation, before);
        result?;
    }
    ensure!(!(wait.cancelled)(), "CONTENT_CANCELLED");
    Ok(true)
}
pub(crate) fn read(
    shared: &Shared,
    handle: u64,
    offset: u64,
    out: &mut [u8],
    context: &ReadContext,
    wait: Option<ContentWait>,
) -> Result<Option<super::content_reads::ReadOutcome>> {
    let Some(file) = file(shared, handle, context)? else {
        return Ok(None);
    };
    // Native mutable callbacks reserve their adapter capacity before byte work,
    // including a wait on an unavailable immutable base. Drop on every exit.
    let _adapter_permit = match wait.and_then(|wait| wait.slots) {
        Some(slots) => Some(slots.take().context("CONTENT_RESOURCE_LIMIT")?),
        None => None,
    };
    let _mutation = lock(&file.byte_mutation, context)?;
    let limits = wait.map(|wait| crate::content::write::BaseWaitLimits {
        stall: wait.stall,
        cancelled: wait.cancelled,
    });
    let mut read = || -> Result<(u64, usize)> {
        let length = file.len(context)?;
        let count = out
            .len()
            .min(usize::try_from(length.saturating_sub(offset)).unwrap_or(usize::MAX));
        for (index, part) in out[..count].chunks_mut(limits::READ_BYTES).enumerate() {
            file.read_range_waiting(
                offset + (index * crate::model::workspace::limits::READ_BYTES) as u64,
                part,
                limits,
                context,
            )?;
        }
        Ok((length, count))
    };
    let (length, count) = match read() {
        Err(error) if crate::model::errors::code_of(&error) == "STALE_WRITE_SESSION" => {
            let writes = engine(shared, context)?
                .native_writes
                .clone()
                .context("PEER_NEEDS_UPGRADE")?;
            writes.executor.reconcile_retirement(&file, context)?;
            read()?
        }
        result => result?,
    };
    ensure!(
        engine(shared, context)?
            .session(handle)?
            .bytes
            .cow_file()
            .is_some_and(|f| Arc::ptr_eq(&f, &file)),
        "STALE_CONTENT_VIEW"
    );
    Ok(Some(super::content_reads::ReadOutcome { count, length }))
}
pub(crate) fn save_entity(shared: &Shared, entity: &str, context: &ReadContext) -> Result<bool> {
    save_entity_inner(shared, entity, false, context)
}
/// Read-only cleanup may complete an already acknowledged Saved revision, but
/// must not checkpoint another writer's buffered bytes. Decide under its gate.
pub(crate) fn save_recoverable_entity(
    shared: &Shared,
    entity: &str,
    context: &ReadContext,
) -> Result<bool> {
    save_entity_inner(shared, entity, true, context)
}
fn save_entity_inner(
    shared: &Shared,
    entity: &str,
    only_recoverable: bool,
    context: &ReadContext,
) -> Result<bool> {
    let file = {
        let e = super::lock_engine(shared);
        let Some(file) = e.sessions.get(entity).and_then(|s| s.bytes.cow_file()) else {
            return Ok(false);
        };
        file
    };
    let _mutation = lock(&file.adapter_mutation, context)?;
    if only_recoverable && !file.recoverable() {
        return Ok(true);
    }
    if only_recoverable {
        let e = engine(shared, context)?;
        if e.handles
            .values()
            .any(|handle| handle.entity == entity && handle.writable && !handle.cleaned)
        {
            return Ok(true);
        }
    }
    metadata::reconcile_metadata(shared, &file, context)?;
    let (writes, edits) = {
        let mut e = engine(shared, context)?;
        let Some(session) = e.sessions.get_mut(entity) else {
            return Ok(false);
        };
        ensure!(
            session
                .bytes
                .cow_file()
                .is_some_and(|f| Arc::ptr_eq(&f, &file)),
            "STALE_CONTENT_VIEW"
        );
        session.saving = true;
        let edits = session.edits;
        (
            e.native_writes.clone().context("PEER_NEEDS_UPGRADE")?,
            edits,
        )
    };
    let saved = writes.executor.flush(file.clone(), context);
    // Completion must clear saving even after a deadline/stop. No byte I/O.
    let mut e = super::lock_engine(shared);
    if let Some(session) = e.sessions.get_mut(entity)
        && session
            .bytes
            .cow_file()
            .is_some_and(|current| Arc::ptr_eq(&current, &file))
    {
        session.saving = false;
        session.queued = false;
        if saved.is_ok() && session.edits == edits {
            session.dirty = false;
        }
    }
    if saved.is_ok() {
        if let Ok(entry) = e.store.entry(entity)
            && let Some(session) = e.sessions.get_mut(entity)
        {
            session.content_base = entry.revisions.get("content").cloned();
            session.alive_base = entry.revisions.get("alive").cloned();
            session.entry = entry;
        }
        if e.sessions
            .get(entity)
            .is_some_and(|s| !s.dirty && s.references == 0)
        {
            e.sessions.remove(entity);
        }
    }
    e.save_signal().bump();
    e.publish_observation();
    saved?;
    Ok(true)
}
pub(crate) fn default_context(shared: &Shared) -> Result<ReadContext> {
    super::content_reads::request_context(shared, ContentWait::fixed(Duration::from_secs(240)))
}
/// Forced detach retains native bytes/intents before legacy engine bookkeeping.
/// This context intentionally works after the adapter/worker fence is raised.
pub fn retain_dirty(shared: &Shared, context: &ReadContext) -> Result<usize> {
    let files: Vec<_> = {
        let e = super::lock_engine(shared);
        e.sessions
            .values()
            .filter(|s| s.dirty)
            .filter_map(|s| s.bytes.cow_file())
            .collect()
    };
    for file in &files {
        let _mutation = lock(&file.adapter_mutation, context)?;
        file.retain(context)?;
    }
    Ok(files.len())
}
pub fn flush(shared: &Shared, handle: u64, wait: ContentWait) -> Result<bool> {
    let context = super::content_reads::request_context(shared, wait)?;
    let entity = {
        let e = engine(shared, &context)?;
        e.session(handle)?.entry.id.clone()
    };
    save_entity(shared, &entity, &context)
}
