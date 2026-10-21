//! Provider-backed descriptor reads for both filesystem adapters. Metadata
//! capture/revalidation is short; provider pin and byte I/O occur off-lock.
use super::{EngineGuard, Shared, content::ContentWait};
use crate::{
    content::{ContentProvider, LocalContent, ReadContext},
    model::workspace::{ByteRange, Demand, DemandPriority, FileRevision, limits},
    staging::{ContentBinding, ContentSelection},
    util::Cancel,
};
use anyhow::{Context, Result, ensure};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use uuid::Uuid;

// Bound secondary connections/wait state before opening or allocating them.
// Adapter-specific slots additionally reserve their own dispatch capacity.
pub(crate) static WAITERS: super::content::WaitSlots =
    super::content::WaitSlots::new(limits::DEMAND_WAITERS);

pub(crate) fn retry_sql<T>(
    context: &ReadContext,
    mut operation: impl FnMut() -> Result<T>,
) -> Result<T> {
    loop {
        context.check()?;
        match operation() {
            Err(error) if crate::model::errors::code_of(&error) == "SQLITE_BUSY" => {
                context.cancel.sleep(Duration::from_millis(5));
            }
            result => return result,
        }
    }
}

struct DemandSelection {
    event: String,
    entity: String,
    branch: String,
    generation: u64,
}
impl DemandSelection {
    fn capture(guard: &super::Engine, handle: u64) -> Result<Self> {
        let session = guard.session(handle)?;
        Ok(Self {
            event: session
                .content_base
                .clone()
                .context("MISSING_CONTENT_DESCRIPTOR")?,
            entity: session.entry.id.clone(),
            branch: guard.store.active.clone(),
            generation: guard.store.generation,
        })
    }
    fn check(
        &self,
        guard: &super::Engine,
        handle: u64,
        selection: &Arc<ContentSelection>,
    ) -> Result<()> {
        still_selected(guard, handle, selection)?;
        ensure!(
            guard.store.generation == self.generation
                && guard.store.active == self.branch
                && guard.session(handle)?.content_base.as_ref() == Some(&self.event),
            "STALE_VIEW"
        );
        Ok(())
    }
}

struct WaitingRange<'a> {
    shared: &'a Shared,
    handle: u64,
    selection: &'a Arc<ContentSelection>,
    binding: &'a ContentBinding,
    context: &'a ReadContext,
    wait: Option<ContentWait>,
}
impl WaitingRange<'_> {
    fn retry_sql<T>(
        &self,
        captured: &DemandSelection,
        mut operation: impl FnMut() -> Result<T>,
    ) -> Result<T> {
        retry_sql(self.context, || {
            self.check(captured)?;
            operation()
        })
    }
    fn check(&self, captured: &DemandSelection) -> Result<()> {
        self.context.check()?;
        ensure!(
            self.wait.is_none_or(|w| !(w.cancelled)()),
            "CONTENT_CANCELLED"
        );
        captured.check(
            &*engine_wait(self.shared, self.context, self.wait.map(|w| w.cancelled))?,
            self.handle,
            self.selection,
        )
    }
    fn read(&self, offset: u64, out: &mut [u8], initial: anyhow::Error) -> Result<()> {
        let Some(mut key) = LocalContent::unavailable_key(&initial) else {
            return Err(initial);
        };
        let Some(worker) = super::worker_of(self.shared) else {
            return Err(initial);
        };
        let _permit = WAITERS.take().context("CONTENT_RESOURCE_LIMIT")?;
        let _adapter_permit = match self.wait.and_then(|w| w.slots) {
            Some(slots) => Some(slots.take().context("CONTENT_RESOURCE_LIMIT")?),
            None => None,
        };
        let captured = {
            let guard = engine_wait(self.shared, self.context, self.wait.map(|w| w.cancelled))?;
            ensure!(guard.store.root == worker.root, "STALE_VIEW");
            DemandSelection::capture(&guard, self.handle)?
        };
        let mut store = self.retry_sql(&captured, || worker.sync_stores.checkout_nowait())?;
        let state = self.retry_sql(&captured, || {
            super::content_rpc::state_for_read(&worker, &store)
        })?;
        // Provider identities are opaque. Fetch only for the same native owner;
        // an independent provider keeps its own availability/failure semantics.
        if self.binding.pin.provider() != state.provider.identity() {
            return Err(initial);
        }
        let reference = crate::core::admission::reference_id(&captured.event, &captured.entity);
        let (generation, root, granted) =
            self.retry_sql(&captured, || store.content_root(&reference))?;
        ensure!(
            granted && root == *self.selection.reference(),
            "INVALID_DEMAND_SCOPE"
        );
        if root.kind == crate::model::content::ContentKind::Legacy
            || !self.retry_sql(&captured, || {
                store.content_reference_authorized(&reference, generation)
            })?
        {
            self.retry_sql(&captured, || store.observe_content_unavailable(key))?;
            return Err(initial);
        }
        let workspace = self.retry_sql(&captured, || store.content_workspace_id(self.context))?;
        let mut demand = Demand {
            waiter: Uuid::new_v4(),
            reference,
            revision: FileRevision {
                workspace,
                entity: captured.entity.clone(),
                generation: captured.generation,
                content: root,
            },
            range: ByteRange {
                offset,
                len: out.len() as u64,
            },
            priority: DemandPriority::Foreground,
            eligible_peers: vec![],
        };
        let mut lease = None;
        let mut observed = None;
        let mut progress = Instant::now();
        loop {
            self.check(&captured)?;
            self.retry_sql(&captured, || {
                let (current, _, granted) = store.content_root(&demand.reference)?;
                ensure!(
                    granted
                        && current == generation
                        && store.content_reference_authorized(&demand.reference, generation)?,
                    "STALE_ADMISSION_GENERATION"
                );
                store.validate_content_demand(&demand)
            })?;
            let indexed = self.retry_sql(&captured, || store.content_object_verified(key))?;
            if observed != Some(key) || indexed {
                self.retry_sql(&captured, || store.observe_content_unavailable(key))?;
                drop(lease.take());
                demand.waiter = Uuid::new_v4();
                lease = Some(state.scheduler.demand_on(
                    state.provider.identity(),
                    demand.clone(),
                    self.context,
                )?);
                if observed.is_some_and(|old| old != key) {
                    progress = Instant::now();
                }
                observed = Some(key);
            }
            self.context.cancel.sleep(Duration::from_millis(10));
            self.check(&captured)?;
            match self
                .binding
                .provider
                .read_range(&self.binding.pin, offset, out, self.context)
            {
                Ok(()) => {
                    self.check(&captured)?;
                    let (current, _, granted) =
                        self.retry_sql(&captured, || store.content_root(&demand.reference))?;
                    ensure!(
                        granted
                            && current == generation
                            && self.retry_sql(&captured, || store
                                .content_reference_authorized(&demand.reference, generation))?,
                        "STALE_ADMISSION_GENERATION"
                    );
                    return Ok(());
                }
                Err(error) => {
                    key = LocalContent::unavailable_key(&error).ok_or(error)?;
                    ensure!(
                        self.wait.is_none_or(|w| progress.elapsed() < w.stall),
                        "CONTENT_UNAVAILABLE"
                    );
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
struct ReadOptions {
    limit: usize,
    wait: Option<ContentWait>,
}

fn engine_wait<'a>(
    shared: &'a Shared,
    context: &ReadContext,
    cancelled: Option<fn() -> bool>,
) -> Result<EngineGuard<'a>> {
    loop {
        ensure!(cancelled.is_none_or(|check| !check()), "CONTENT_CANCELLED");
        context.check()?;
        if let Some(guard) = super::try_lock_engine(shared) {
            ensure!(!guard.switching, "STALE_VIEW");
            ensure!(
                !guard.stopping.load(std::sync::atomic::Ordering::Acquire),
                "CONTENT_CANCELLED"
            );
            return Ok(guard);
        }
        context.cancel.sleep(Duration::from_millis(5));
    }
}
pub fn request_context(shared: &Shared, wait: ContentWait) -> Result<ReadContext> {
    ensure!(!(wait.cancelled)(), "CONTENT_CANCELLED");
    let cancel = super::worker_of(shared).map_or_else(Cancel::new, |worker| {
        crate::util::lock::lock(&worker.inflight_objects)
            .legacy_cancel
            .child()
    });
    Ok(ReadContext {
        deadline: Instant::now()
            .checked_add(wait.bound)
            .context("CONTENT_DEADLINE")?,
        cancel,
    })
}
fn descriptor(
    shared: &Shared,
    handle: u64,
    context: &ReadContext,
    cancelled: Option<fn() -> bool>,
) -> Result<bool> {
    loop {
        // An immediately available legacy selection needs no byte-work wait,
        // including ContentWait::fixed(0). Contention still obeys the deadline.
        if let Some(guard) = super::try_lock_engine(shared) {
            return Ok(guard.session(handle)?.bytes.content_selection().is_some());
        }
        ensure!(cancelled.is_none_or(|check| !check()), "CONTENT_CANCELLED");
        context.check()?;
        context.cancel.sleep(Duration::from_millis(5));
    }
}
fn still_selected(
    guard: &super::Engine,
    handle: u64,
    selection: &Arc<ContentSelection>,
) -> Result<()> {
    ensure!(
        guard
            .session(handle)?
            .bytes
            .content_selection()
            .is_some_and(|current| Arc::ptr_eq(&current, selection)),
        "STALE_VIEW"
    );
    Ok(())
}
fn prepare(
    shared: &Shared,
    handle: u64,
    context: &ReadContext,
    cancelled: Option<fn() -> bool>,
) -> Result<Option<Arc<ContentSelection>>> {
    let (selection, root, provider) = {
        let guard = engine_wait(shared, context, cancelled)?;
        let Some(selection) = guard.session(handle)?.bytes.content_selection() else {
            return Ok(None);
        };
        (
            selection,
            guard.store.root.clone(),
            guard.content_provider.clone(),
        )
    };
    if selection.binding().is_none() {
        let provider: Arc<dyn ContentProvider> = match provider {
            Some(provider) => provider,
            None => Arc::new(LocalContent::open(&root)?),
        };
        context.check()?;
        let pin = provider.pin(selection.reference())?;
        context.check()?;
        ensure!(
            pin.reference() == selection.reference(),
            "CONTENT_PROVIDER_MISMATCH"
        );
        let binding = ContentBinding {
            provider: provider.clone(),
            pin,
        };
        let mut guard = engine_wait(shared, context, cancelled)?;
        ensure!(guard.store.root == root, "STALE_VIEW");
        still_selected(&guard, handle, &selection)?;
        if guard.content_provider.is_none() {
            guard.content_provider = Some(provider);
        }
        let unused = selection.bind(binding);
        drop(guard);
        // A losing concurrent pin may have an implementation-specific release;
        // do not run its destructor under the metadata mutex either.
        drop(unused);
    }
    still_selected(
        &*engine_wait(shared, context, cancelled)?,
        handle,
        &selection,
    )?;
    context.check()?;
    Ok(Some(selection))
}
/// Called after creating a handle and before acknowledging open. A descriptor
/// may be unavailable, but its lifetime pin must already exist on success.
pub fn prepare_open(shared: &Shared, handle: u64, wait: ContentWait) -> Result<()> {
    let context = request_context(shared, wait)?;
    if !descriptor(shared, handle, &context, Some(wait.cancelled))? {
        return Ok(());
    }
    prepare(shared, handle, &context, Some(wait.cancelled))?;
    super::content_writes::prepare_open(shared, handle, &context)?;
    ensure!(!(wait.cancelled)(), "CONTENT_CANCELLED");
    context.check()
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadOutcome {
    pub count: usize,
    pub length: u64,
}
/// None selects the legacy staging path. Native reads short-read at 1 MiB;
/// callers discard all output on error, including stale handle/view selection.
pub fn read_content_range(
    shared: &Shared,
    handle: u64,
    offset: u64,
    out: &mut [u8],
    context: &ReadContext,
) -> Result<Option<ReadOutcome>> {
    let cancel = super::worker_of(shared).map(|worker| {
        crate::util::lock::lock(&worker.inflight_objects)
            .legacy_cancel
            .clone()
    });
    let context = ReadContext {
        deadline: context.deadline,
        cancel: cancel.map_or_else(
            || context.cancel.clone(),
            |stop| context.cancel.linked(&stop),
        ),
    };
    let context = &context;
    let native_limit = out.len().min(limits::READ_BYTES);
    if let Some(result) = super::content_writes::read(
        shared,
        handle,
        offset,
        &mut out[..native_limit],
        context,
        None,
    )? {
        return Ok(Some(result));
    }
    let Some(selection) = prepare(shared, handle, context, None)? else {
        return Ok(None);
    };
    read_selected(
        shared,
        handle,
        offset,
        out,
        context,
        selection,
        ReadOptions {
            limit: limits::READ_BYTES,
            wait: None,
        },
    )
    .map(Some)
}
fn read_selected(
    shared: &Shared,
    handle: u64,
    offset: u64,
    out: &mut [u8],
    context: &ReadContext,
    selection: Arc<ContentSelection>,
    options: ReadOptions,
) -> Result<ReadOutcome> {
    let binding = selection.binding().context("CONTENT_UNAVAILABLE")?;
    let length = selection.reference().total_len;
    let count = out
        .len()
        .min(options.limit)
        .min(usize::try_from(length.saturating_sub(offset)).unwrap_or(usize::MAX));
    let mut done = 0;
    while done < count {
        context.check()?;
        let end = count.min(done + limits::READ_BYTES);
        if let Err(error) = binding.provider.read_range(
            &binding.pin,
            offset + done as u64,
            &mut out[done..end],
            context,
        ) {
            WaitingRange {
                shared,
                handle,
                selection: &selection,
                binding: &binding,
                context,
                wait: options.wait,
            }
            .read(offset + done as u64, &mut out[done..end], error)?;
        }
        done = end;
    }
    still_selected(
        &*engine_wait(shared, context, options.wait.map(|w| w.cancelled))?,
        handle,
        &selection,
    )?;
    context.check()?;
    Ok(ReadOutcome { count, length })
}
pub fn read_mount_range(
    shared: &Shared,
    handle: u64,
    offset: u64,
    out: &mut [u8],
    wait: ContentWait,
) -> Result<Option<ReadOutcome>> {
    let context = request_context(shared, wait)?;
    if let Some(result) =
        super::content_writes::read(shared, handle, offset, out, &context, Some(wait))?
    {
        ensure!(!(wait.cancelled)(), "CONTENT_CANCELLED");
        return Ok(Some(result));
    }
    if !descriptor(shared, handle, &context, Some(wait.cancelled))? {
        return Ok(None);
    }
    let Some(selection) = prepare(shared, handle, &context, Some(wait.cancelled))? else {
        return Ok(None);
    };
    // The WinFsp ABI supplies the destination; fulfill its entire requested
    // range (or real EOF) using bounded provider calls and one selection fence.
    let limit = out.len();
    let result = read_selected(
        shared,
        handle,
        offset,
        out,
        &context,
        selection,
        ReadOptions {
            limit,
            wait: Some(wait),
        },
    )?;
    ensure!(!(wait.cancelled)(), "CONTENT_CANCELLED");
    Ok(Some(result))
}
/// FUSE owns the reply buffer. Allocate only after selecting a descriptor, and
/// use the same 1 MiB request bound as the provider/legacy adapter.
pub fn read_mount_vec(
    shared: &Shared,
    handle: u64,
    offset: u64,
    requested: usize,
    wait: ContentWait,
) -> Result<Option<Vec<u8>>> {
    let context = request_context(shared, wait)?;
    if let Some(file) = super::content_writes::file(shared, handle, &context)? {
        let count = requested
            .min(limits::READ_BYTES)
            .min(usize::try_from(file.visible_len().saturating_sub(offset)).unwrap_or(usize::MAX));
        let mut out = vec![0; count];
        let result =
            super::content_writes::read(shared, handle, offset, &mut out, &context, Some(wait))?
                .context("STALE_CONTENT_VIEW")?;
        out.truncate(result.count);
        ensure!(!(wait.cancelled)(), "CONTENT_CANCELLED");
        return Ok(Some(out));
    }
    if !descriptor(shared, handle, &context, Some(wait.cancelled))? {
        return Ok(None);
    }
    let Some(selection) = prepare(shared, handle, &context, Some(wait.cancelled))? else {
        return Ok(None);
    };
    let count = requested.min(limits::READ_BYTES).min(
        usize::try_from(selection.reference().total_len.saturating_sub(offset))
            .unwrap_or(usize::MAX),
    );
    let mut out = vec![0; count];
    let result = read_selected(
        shared,
        handle,
        offset,
        &mut out,
        &context,
        selection,
        ReadOptions {
            limit: limits::READ_BYTES,
            wait: Some(wait),
        },
    )?;
    ensure!(!(wait.cancelled)(), "CONTENT_CANCELLED");
    out.truncate(result.count);
    Ok(Some(out))
}
