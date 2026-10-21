//! Native mutable sessions. Installation is explicit until capability/migration
//! qualification; ordinary user workspaces keep the legacy adapter path.
use super::{
    EngineGuard, Shared,
    content::ContentWait,
    content_saves::{SaveExecutor, SaveFile},
    ctx::SyncStorePool,
};
use crate::{
    content::{ReadContext, write::WriteJournal},
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

pub struct NativeWrites {
    pub(crate) journal: Arc<WriteJournal>,
    executor: SaveExecutor,
    target: Arc<crate::content::LocalContent>,
    stores: SyncStorePool,
    keys: Mutex<BTreeMap<String, Weak<Mutex<()>>>>,
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
    /// Explicit isolated-fixture seam, before mounting or opening handles.
    /// This is not repository activation or an upgrade/migration command.
    pub fn install(shared: &Shared, context: &ReadContext) -> Result<Arc<Self>> {
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
        let store = stores.checkout_nowait()?;
        ensure!(
            store.db.query_row(
                "SELECT EXISTS(SELECT 1 FROM config WHERE key='fixture:content_v1' AND value='1')",
                [],
                |r| r.get::<_, bool>(0)
            )?,
            "PEER_NEEDS_UPGRADE"
        );
        let journal = WriteJournal::open(&root)?;
        let target = Arc::new(crate::content::LocalContent::open(&root)?);
        let provider: Arc<dyn crate::content::ContentProvider> = target.clone();
        let mut recovered = Vec::new();
        let mut after = None;
        loop {
            let page = journal.pending(after, 64, context)?;
            if page.is_empty() {
                break;
            }
            after = page.last().map(|c| c.intent.operation);
            for checkpoint in page.into_iter().filter(|c| c.needs_publication()) {
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
                    entry.alive && entry.kind == "file",
                    "RECOVERY_REQUIRED: native save namespace changed"
                );
                ensure!(
                    !recovered.iter().any(
                        |(id, _, _): &(String, crate::core::Entry, Arc<SaveFile>)| id == &entry.id
                    ),
                    "RECOVERY_REQUIRED: competing native operations"
                );
                let session =
                    journal.resume(checkpoint.intent.operation, provider.clone(), context)?;
                let file =
                    SaveFile::recovered(super::content_base::attach(shared, generation, session));
                recovered.push((entry.id.clone(), entry, file));
            }
        }
        drop(store);
        let executor = SaveExecutor::start(shared, journal.clone(), context)?;
        let writes = Arc::new(Self {
            journal,
            executor,
            target,
            stores,
            keys: Mutex::new(BTreeMap::new()),
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
        e.native_writes = Some(writes.clone());
        for (id, entry, file) in recovered {
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
/// Called after the descriptor's immutable provider pin has been installed.
pub(crate) fn prepare_open(shared: &Shared, handle: u64, context: &ReadContext) -> Result<()> {
    let (writes, entity) = {
        let e = engine(shared, context)?;
        let Some(writes) = e.native_writes.clone() else {
            return Ok(());
        };
        let h = e.handles.get(&handle).context("INVALID_HANDLE")?;
        if !h.writable || e.session(handle)?.bytes.cow_file().is_some() {
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
            .is_some_and(|h| h.writable && !h.cleaned),
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
fn mutate(
    shared: &Shared,
    handle: u64,
    context: &ReadContext,
) -> Result<Option<(Arc<SaveFile>, Arc<crate::content::LocalContent>)>> {
    let mut e = engine(shared, context)?;
    let h = e.handles.get(&handle).context("INVALID_HANDLE")?;
    ensure!(h.writable && !h.cleaned, "STALE_VIEW_OR_ACCESS_DENIED");
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
    session.dirty = true;
    session.edits = session
        .edits
        .checked_add(1)
        .context("WRITE_GENERATION_EXHAUSTED")?;
    Ok(Some((file, target)))
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
    ensure!(!(wait.cancelled)(), "CONTENT_CANCELLED");
    let (file, target) = mutate(shared, handle, &context)?.context("STALE_CONTENT_VIEW")?;
    ensure!(Arc::ptr_eq(&file, &selected), "STALE_CONTENT_VIEW");
    crate::util::fault::trigger("content.native.before_write")?;
    let count = file.write_native(
        offset,
        bytes,
        append,
        constrained,
        crate::content::write::PreflightContext {
            target: &target,
            limits: Some(crate::content::write::BaseWaitLimits {
                stall: wait.stall,
                cancelled: wait.cancelled,
            }),
            context: &context,
        },
    )?;
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
    ensure!(!(wait.cancelled)(), "CONTENT_CANCELLED");
    if !allocation || length < file.visible_len() {
        let (selected, target) = mutate(shared, handle, &context)?.context("STALE_CONTENT_VIEW")?;
        ensure!(Arc::ptr_eq(&selected, &file), "STALE_CONTENT_VIEW");
        selected.set_len_native(
            length,
            crate::content::write::PreflightContext {
                target: &target,
                limits: Some(crate::content::write::BaseWaitLimits {
                    stall: wait.stall,
                    cancelled: wait.cancelled,
                }),
                context: &context,
            },
        )?;
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
    let _mutation = lock(&file.adapter_mutation, context)?;
    let limits = wait.map(|wait| crate::content::write::BaseWaitLimits {
        stall: wait.stall,
        cancelled: wait.cancelled,
    });
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
    let file = {
        let e = super::lock_engine(shared);
        let Some(file) = e.sessions.get(entity).and_then(|s| s.bytes.cow_file()) else {
            return Ok(false);
        };
        file
    };
    let _mutation = lock(&file.adapter_mutation, context)?;
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
