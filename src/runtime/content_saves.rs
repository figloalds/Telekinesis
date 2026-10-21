//! Bounded coordinator for headless and explicitly installed native saves.
//! Ordinary repositories retain the legacy path until capability activation.
//! Checkpoint/construction/metadata writer/adoption run outside Engine.
use super::{
    EngineGuard, Shared,
    ctx::{PooledStore, SyncStorePool},
};
use crate::{
    content::{
        ContentProvider, LocalContent, ReadContext,
        write::{SaveCheckpoint, WriteJournal, WriteSession},
    },
    core::content_saves::SaveReceipt,
    model::workspace::limits,
    util::Cancel,
};
use anyhow::{Result, ensure};
use std::{
    collections::{BTreeMap, VecDeque},
    panic::{AssertUnwindSafe, catch_unwind},
    path::PathBuf,
    sync::{
        Arc, Condvar, Mutex, MutexGuard, TryLockError, Weak,
        atomic::{AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
use uuid::Uuid;

const POLL: Duration = Duration::from_millis(limits::CANCEL_POLL_MS);

fn lock<'a, T>(mutex: &'a Mutex<T>, context: &ReadContext) -> Result<MutexGuard<'a, T>> {
    loop {
        context.check()?;
        match mutex.try_lock() {
            Ok(guard) => return Ok(guard),
            // File state is replaced only after a successful adoption. A panic
            // retains the pending checkpoint and the last installed session.
            Err(TryLockError::Poisoned(error)) => return Ok(error.into_inner()),
            Err(TryLockError::WouldBlock) => {
                context.cancel.sleep(Duration::from_millis(5));
            }
        }
    }
}
fn engine<'a>(shared: &'a Shared, context: &ReadContext) -> Result<EngineGuard<'a>> {
    loop {
        context.check()?;
        if let Some(guard) = super::try_lock_engine(shared) {
            ensure!(
                !guard.store.content_save_connection_failed(),
                "CONTENT_SAVE_CONNECTION_FAILED"
            );
            ensure!(
                guard.store.db.is_autocommit(),
                "CONTENT_SAVE_REQUIRES_OUTER_TRANSACTION"
            );
            ensure!(
                guard.native_writes.is_some()
                    || (!guard.mounted && guard.sessions.is_empty() && guard.handles.is_empty()),
                "PEER_NEEDS_UPGRADE: content save callbacks are not enabled"
            );
            ensure!(
                !guard.stopping.load(std::sync::atomic::Ordering::Acquire),
                "CONTENT_CANCELLED"
            );
            ensure!(!guard.switching, "STALE_CONTENT_VIEW");
            return Ok(guard);
        }
        context.cancel.sleep(Duration::from_millis(5));
    }
}

struct FileState {
    session: Arc<WriteSession>,
    pending: Option<SaveCheckpoint>,
    needs_preflight: bool,
}
/// Stable local file identity across successful metadata/private generations.
/// The adapter must retain this handle, not a predecessor WriteSession clone.
pub struct SaveFile {
    buffered_epoch: AtomicU64,
    checkpointed_epoch: AtomicU64,
    pub(crate) adapter_mutation: Mutex<()>,
    operation: Uuid,
    visible_len: AtomicU64,
    state: Mutex<FileState>,
}
impl SaveFile {
    pub fn new(session: WriteSession) -> Arc<Self> {
        Self::new_with_preflight(session, false)
    }
    pub(crate) fn recovered(session: WriteSession) -> Arc<Self> {
        Self::new_with_preflight(session, true)
    }
    fn new_with_preflight(session: WriteSession, needs_preflight: bool) -> Arc<Self> {
        Arc::new(Self {
            buffered_epoch: AtomicU64::new(0),
            checkpointed_epoch: AtomicU64::new(0),
            adapter_mutation: Mutex::new(()),
            operation: session.operation(),
            visible_len: AtomicU64::new(session.visible_len()),
            state: Mutex::new(FileState {
                session: Arc::new(session),
                pending: None,
                needs_preflight,
            }),
        })
    }
    pub fn operation(&self) -> Uuid {
        self.operation
    }
    pub fn write_at(&self, offset: u64, bytes: &[u8], context: &ReadContext) -> Result<()> {
        let mut state = lock(&self.state, context)?;
        state.needs_preflight = true;
        self.buffered()?;
        let result = state.session.write_at(offset, bytes, context);
        self.visible_len
            .store(state.session.visible_len(), Ordering::Release);
        result
    }
    pub fn set_len(&self, length: u64, context: &ReadContext) -> Result<()> {
        let mut state = lock(&self.state, context)?;
        state.needs_preflight = true;
        self.buffered()?;
        let result = state.session.set_len(length, context);
        self.visible_len
            .store(state.session.visible_len(), Ordering::Release);
        result
    }
    pub(crate) fn visible_len(&self) -> u64 {
        self.visible_len.load(Ordering::Acquire)
    }
    fn buffered(&self) -> Result<()> {
        self.buffered_epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map_err(|_| anyhow::anyhow!("WRITE_GENERATION_EXHAUSTED"))?;
        Ok(())
    }
    fn checkpointed(&self) {
        self.checkpointed_epoch.store(
            self.buffered_epoch.load(Ordering::Acquire),
            Ordering::Release,
        );
    }
    pub(crate) fn recoverable(&self) -> bool {
        self.buffered_epoch.load(Ordering::Acquire)
            == self.checkpointed_epoch.load(Ordering::Acquire)
    }
    pub(crate) fn retain(&self, context: &ReadContext) -> Result<()> {
        let state = lock(&self.state, context)?;
        state.session.checkpoint(context)?;
        self.checkpointed();
        Ok(())
    }
    /// Append/constrained offsets and bounded pieces share file ownership.
    pub(crate) fn write_native(
        &self,
        offset: u64,
        bytes: &[u8],
        append: bool,
        constrained: bool,
        preflight: crate::content::write::PreflightContext<'_>,
    ) -> Result<usize> {
        let context = preflight.context;
        let (session, length, all_changes) = {
            let state = lock(&self.state, context)?;
            (
                state.session.clone(),
                state.session.len(context)?,
                state.needs_preflight,
            )
        };
        let offset = if append { length } else { offset };
        let count = if constrained {
            bytes
                .len()
                .min(usize::try_from(length.saturating_sub(offset)).unwrap_or(usize::MAX))
        } else {
            bytes.len()
        };
        if count == 0 {
            return Ok(0);
        }
        let selection = session.preflight_mutation(
            crate::model::workspace::ByteRange {
                offset,
                len: count as u64,
            },
            None,
            all_changes,
            &preflight,
        )?;
        let mut state = lock(&self.state, context)?;
        ensure!(Arc::ptr_eq(&state.session, &session), "STALE_WRITE_SESSION");
        session.validate_mutation(&selection, context)?;
        self.buffered()?;
        state.needs_preflight = true;
        let result = (|| {
            for (index, part) in bytes[..count].chunks(limits::READ_BYTES).enumerate() {
                selection.check(context)?;
                state.session.write_at(
                    offset
                        .checked_add((index * limits::READ_BYTES) as u64)
                        .ok_or_else(|| anyhow::anyhow!("RANGE_OVERFLOW"))?,
                    part,
                    context,
                )?;
            }
            Ok(count)
        })();
        self.visible_len
            .store(state.session.visible_len(), Ordering::Release);
        if result.is_ok() {
            state.needs_preflight = false;
        }
        result
    }
    pub(crate) fn set_len_native(
        &self,
        length: u64,
        preflight: crate::content::write::PreflightContext<'_>,
    ) -> Result<()> {
        let context = preflight.context;
        let (session, all_changes) = {
            let state = lock(&self.state, context)?;
            (state.session.clone(), state.needs_preflight)
        };
        let selection = session.preflight_mutation(
            crate::model::workspace::ByteRange { offset: 0, len: 0 },
            Some(length),
            all_changes,
            &preflight,
        )?;
        let mut state = lock(&self.state, context)?;
        ensure!(Arc::ptr_eq(&state.session, &session), "STALE_WRITE_SESSION");
        session.validate_mutation(&selection, context)?;
        self.buffered()?;
        state.needs_preflight = true;
        let result = session.set_len(length, context);
        self.visible_len
            .store(session.visible_len(), Ordering::Release);
        if result.is_ok() {
            state.needs_preflight = false;
        }
        result
    }
    pub fn len(&self, context: &ReadContext) -> Result<u64> {
        lock(&self.state, context)?.session.len(context)
    }
    pub fn is_empty(&self, context: &ReadContext) -> Result<bool> {
        Ok(self.len(context)? == 0)
    }
    pub fn read_range(&self, offset: u64, bytes: &mut [u8], context: &ReadContext) -> Result<()> {
        let session = lock(&self.state, context)?.session.clone();
        session.read_range(offset, bytes, context)
    }
    pub(crate) fn read_range_waiting(
        &self,
        offset: u64,
        bytes: &mut [u8],
        limits: Option<crate::content::write::BaseWaitLimits>,
        context: &ReadContext,
    ) -> Result<()> {
        let session = lock(&self.state, context)?.session.clone();
        session.read_range_waiting(offset, bytes, limits, context)
    }
}

type Completed = std::result::Result<SaveReceipt, String>;
#[derive(Default)]
struct Completion {
    result: Mutex<Option<Completed>>,
    changed: Condvar,
}
#[derive(Clone)]
pub struct SaveTicket(Arc<Completion>);
impl SaveTicket {
    /// Cancelling a wait does not cancel the accepted save. The enqueue context
    /// owns that operation; close callers may wait with a shorter request token.
    pub fn wait(&self, context: &ReadContext) -> Result<SaveReceipt> {
        loop {
            context.check()?;
            let guard = crate::util::lock::lock(&self.0.result);
            if let Some(result) = &*guard {
                return result.clone().map_err(anyhow::Error::msg);
            }
            drop(
                self.0
                    .changed
                    .wait_timeout(guard, POLL)
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct SaveStats {
    pub outstanding: usize,
    pub queued: usize,
    pub active: usize,
    pub workers: usize,
    pub accepting: bool,
}
struct State {
    jobs: VecDeque<Job>,
    outstanding: BTreeMap<Uuid, Uuid>,
    active: usize,
    workers: usize,
    accepting: bool,
}
pub(super) struct SavePool {
    journal: Arc<WriteJournal>,
    engine: Weak<Mutex<super::Engine>>,
    root: PathBuf,
    stores: SyncStorePool,
    target: Arc<LocalContent>,
    stop: Cancel,
    state: Mutex<State>,
    changed: Condvar,
}

struct MetadataView {
    branch: String,
    generation: u64,
    before: Option<crate::core::Projection>,
}
fn connection(
    pool: &SavePool,
    branch: &str,
    context: &ReadContext,
) -> Result<(PooledStore, MetadataView)> {
    let shared = pool
        .engine
        .upgrade()
        .ok_or_else(|| anyhow::anyhow!("CONTENT_CANCELLED"))?;
    loop {
        context.check()?;
        let view = {
            let guard = engine(&shared, context)?;
            ensure!(guard.store.root == pool.root, "STALE_CONTENT_VIEW");
            ensure!(guard.store.active == branch, "STALE_CONTENT_VIEW");
            MetadataView {
                branch: guard.store.active.clone(),
                generation: guard.store.generation,
                before: guard.capture_before(),
            }
        };
        match pool.stores.checkout_nowait() {
            Ok(store) => {
                ensure!(
                    store.active == view.branch && store.generation == view.generation,
                    "STALE_CONTENT_VIEW"
                );
                context.check()?;
                return Ok((store, view));
            }
            Err(error) if crate::model::errors::code_of(&error) == "SQLITE_BUSY" => {
                context.cancel.sleep(Duration::from_millis(5));
            }
            Err(error) => return Err(error),
        }
    }
}
fn observe(
    pool: &SavePool,
    receipt: &SaveReceipt,
    view: MetadataView,
    context: &ReadContext,
) -> Result<()> {
    let shared = pool
        .engine
        .upgrade()
        .ok_or_else(|| anyhow::anyhow!("CONTENT_CANCELLED"))?;
    let mut guard = engine(&shared, context)?;
    ensure!(
        guard.store.root == pool.root
            && guard.store.active == view.branch
            && guard.store.generation == view.generation,
        "STALE_CONTENT_VIEW"
    );
    guard.note_secondary_commit();
    let delta = guard.activation_delta(&[receipt.event().to_owned()], view.before)?;
    guard.after_activation(&delta);
    guard.publish_observation();
    Ok(())
}
struct Permit {
    pool: Arc<SavePool>,
    operation: Uuid,
    id: Uuid,
    active: bool,
}
impl Drop for Permit {
    fn drop(&mut self) {
        let mut state = crate::util::lock::lock(&self.pool.state);
        if state.outstanding.get(&self.operation) == Some(&self.id) {
            state.outstanding.remove(&self.operation);
            if self.active {
                state.active -= 1;
            }
        }
        self.pool.changed.notify_all();
    }
}
struct Job {
    file: Arc<SaveFile>,
    session: Arc<WriteSession>,
    checkpoint: SaveCheckpoint,
    context: ReadContext,
    completion: Arc<Completion>,
    _permit: Permit,
}
/// Exactly four fixed workers, at most 64 outstanding jobs including active
/// work and producers checkpointing before queue insertion. No inline fallback.
pub struct SaveExecutor {
    pool: Arc<SavePool>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}
impl SaveExecutor {
    pub fn start(
        shared: &Shared,
        journal: Arc<WriteJournal>,
        context: &ReadContext,
    ) -> Result<Self> {
        let (root, repo, device) = {
            let guard = engine(shared, context)?;
            (
                guard.store.root.clone(),
                guard.store.repo.clone(),
                guard.store.device.clone(),
            )
        };
        let stores = super::worker_of(shared).map_or_else(
            || SyncStorePool::new(&root, repo, device),
            |worker| worker.sync_stores.clone(),
        );
        ensure!(
            journal.store_root() == root.canonicalize()?,
            "CONTENT_SAVE_WRONG_STORE"
        );
        let target = Arc::new(LocalContent::open(&root)?);
        let mut guard = engine(shared, context)?;
        ensure!(
            guard.content_saver.upgrade().is_none(),
            "CONTENT_RESOURCE_LIMIT"
        );
        ensure!(guard.store.root == root, "STALE_CONTENT_VIEW");
        let pool = Arc::new(SavePool {
            journal,
            engine: Arc::downgrade(shared),
            root: root.clone(),
            stores,
            target,
            stop: Cancel::new(),
            state: Mutex::new(State {
                jobs: VecDeque::new(),
                outstanding: BTreeMap::new(),
                active: 0,
                workers: 0,
                accepting: true,
            }),
            changed: Condvar::new(),
        });
        guard.content_saver = Arc::downgrade(&pool);
        drop(guard);
        let executor = Self {
            pool,
            threads: Mutex::new(Vec::new()),
        };
        for index in 0..limits::SAVE_WORKERS {
            crate::util::lock::lock(&executor.pool.state).workers += 1;
            let pool = executor.pool.clone();
            match std::thread::Builder::new()
                .name(format!("content-save-{index}"))
                .spawn(move || worker(pool))
            {
                Ok(thread) => crate::util::lock::lock(&executor.threads).push(thread),
                Err(error) => {
                    crate::util::lock::lock(&executor.pool.state).workers -= 1;
                    executor.stop();
                    return Err(error.into());
                }
            }
        }
        Ok(executor)
    }
    pub fn stats(&self) -> SaveStats {
        let state = crate::util::lock::lock(&self.pool.state);
        SaveStats {
            outstanding: state.outstanding.len(),
            queued: state.jobs.len(),
            active: state.active,
            workers: state.workers,
            accepting: state.accepting,
        }
    }
    /// Returns only after a FULL private checkpoint exists. None means this
    /// generation is clean. The ticket acknowledges metadata commit AND private
    /// adoption; it never waits for a peer. Any failure retains recovery data.
    pub fn enqueue(
        &self,
        file: Arc<SaveFile>,
        context: &ReadContext,
    ) -> Result<Option<SaveTicket>> {
        context.check()?;
        let id = Uuid::new_v4();
        {
            let mut state = crate::util::lock::lock(&self.pool.state);
            ensure!(state.accepting, "CONTENT_CANCELLED");
            ensure!(
                state.outstanding.len() < limits::SAVE_QUEUE
                    && !state.outstanding.contains_key(&file.operation),
                "CONTENT_RESOURCE_LIMIT"
            );
            state.outstanding.insert(file.operation, id);
        }
        let permit = Permit {
            pool: self.pool.clone(),
            operation: file.operation,
            id,
            active: false,
        };
        ensure!(
            lock(&file.state, context)?
                .session
                .uses_journal(&self.pool.journal),
            "CONTENT_SAVE_WRONG_STORE"
        );
        let context = ReadContext {
            deadline: context.deadline,
            cancel: context.cancel.linked(&self.pool.stop),
        };
        let (session, pending, saved) = {
            let state = lock(&file.state, &context)?;
            let saved = match state.session.checkpoint(&context) {
                Ok(saved) => {
                    file.checkpointed();
                    saved
                }
                Err(error)
                    if state.pending.is_some()
                        && crate::model::errors::ErrorWire::from_error(&error).code
                            == "STALE_WRITE_SESSION" =>
                {
                    None
                }
                Err(error) => return Err(error),
            };
            (state.session.clone(), state.pending.clone(), saved)
        };
        let checkpoint = {
            let intent = match pending {
                Some(checkpoint) => checkpoint.intent,
                None => session.intent(&context)?,
            };
            // A committed older Saved may need adoption before the latest
            // Saved can select the successor generation, including restart.
            let committed = connection(&self.pool, &intent.branch, &context)?
                .0
                .committed_content_checkpoint(&intent, &context)?;
            let mut state = lock(&file.state, &context)?;
            let checkpoint = match committed {
                Some(checkpoint) => Some(checkpoint),
                None => saved,
            };
            let Some(checkpoint) = checkpoint else {
                return Ok(None);
            };
            state.pending = Some(checkpoint.clone());
            checkpoint
        };
        let completion = Arc::new(Completion::default());
        let ticket = SaveTicket(completion.clone());
        let mut state = crate::util::lock::lock(&self.pool.state);
        context.check()?;
        ensure!(state.accepting, "CONTENT_CANCELLED");
        state.jobs.push_back(Job {
            file,
            session,
            checkpoint,
            context,
            completion,
            _permit: permit,
        });
        self.pool.changed.notify_one();
        Ok(Some(ticket))
    }
    /// Explicit flush drains the captured generation and any later Saved work
    /// carried by adoption. New writes may keep it busy until its deadline.
    pub fn flush(&self, file: Arc<SaveFile>, context: &ReadContext) -> Result<()> {
        loop {
            context.check()?;
            match self.enqueue(file.clone(), context) {
                Ok(Some(ticket)) => {
                    ticket.wait(context)?;
                }
                Ok(None) => return Ok(()),
                Err(error)
                    if crate::model::errors::ErrorWire::from_error(&error).code
                        == "CONTENT_RESOURCE_LIMIT" =>
                {
                    context.cancel.sleep(POLL);
                }
                Err(error) => return Err(error),
            }
        }
    }
    /// Checkpoint up to 64 compatible files in one FULL private transaction,
    /// then schedule their independent publication/adoption on the fixed pool.
    /// Results are ordered by operation UUID. A pending earlier pipeline must
    /// be drained first; no partial queue insertion occurs on a batch error.
    pub fn enqueue_batch(
        &self,
        files: &[Arc<SaveFile>],
        context: &ReadContext,
    ) -> Result<Vec<(Uuid, Option<SaveTicket>)>> {
        context.check()?;
        ensure!(files.len() <= limits::SAVE_QUEUE, "CONTENT_RESOURCE_LIMIT");
        if files.is_empty() {
            return Ok(Vec::new());
        }
        ensure!(
            files.iter().enumerate().all(|(index, file)| files[..index]
                .iter()
                .all(|other| other.operation != file.operation)),
            "CONTENT_RESOURCE_LIMIT"
        );
        let mut permits = Vec::new();
        {
            let mut state = crate::util::lock::lock(&self.pool.state);
            ensure!(state.accepting, "CONTENT_CANCELLED");
            ensure!(
                state.outstanding.len() + files.len() <= limits::SAVE_QUEUE
                    && files
                        .iter()
                        .all(|file| !state.outstanding.contains_key(&file.operation)),
                "CONTENT_RESOURCE_LIMIT"
            );
            for file in files {
                let id = Uuid::new_v4();
                state.outstanding.insert(file.operation, id);
                permits.push(Permit {
                    pool: self.pool.clone(),
                    operation: file.operation,
                    id,
                    active: false,
                });
            }
        }
        // Reserve the full group before allocating the sorted file/guard/job
        // vectors. Permits stay outside the pool lock's scope for unwind.
        let mut ordered = files.to_vec();
        ordered.sort_unstable_by_key(|file| file.operation);
        permits.sort_unstable_by_key(|permit| permit.operation);
        let context = ReadContext {
            deadline: context.deadline,
            cancel: context.cancel.linked(&self.pool.stop),
        };
        // All entity locks use the same order. None is acquired while holding
        // the metadata engine or the journal writer mutex.
        let mut states = ordered
            .iter()
            .map(|file| lock(&file.state, &context))
            .collect::<Result<Vec<_>>>()?;
        for state in &states {
            ensure!(
                state.session.uses_journal(&self.pool.journal),
                "CONTENT_SAVE_WRONG_STORE"
            );
            ensure!(state.pending.is_none(), "CONTENT_RESOURCE_LIMIT");
        }
        let sessions: Vec<_> = states.iter().map(|state| state.session.as_ref()).collect();
        let saved = self.pool.journal.checkpoint_group(&sessions, &context)?;
        for file in &ordered {
            file.checkpointed();
        }
        drop(sessions);
        let mut selected = Vec::with_capacity(ordered.len());
        for (state, saved) in states.iter().zip(saved) {
            let intent = state.session.intent(&context)?;
            selected.push(
                connection(&self.pool, &intent.branch, &context)?
                    .0
                    .committed_content_checkpoint(&intent, &context)?
                    .or(saved),
            );
        }
        let mut jobs = Vec::with_capacity(ordered.len());
        let mut tickets = Vec::with_capacity(ordered.len());
        for (((file, state), checkpoint), permit) in ordered
            .iter()
            .zip(states.iter_mut())
            .zip(selected)
            .zip(permits)
        {
            if let Some(checkpoint) = checkpoint {
                state.pending = Some(checkpoint.clone());
                let completion = Arc::new(Completion::default());
                tickets.push((file.operation, Some(SaveTicket(completion.clone()))));
                jobs.push(Job {
                    file: file.clone(),
                    session: state.session.clone(),
                    checkpoint,
                    context: ReadContext {
                        deadline: context.deadline,
                        cancel: context.cancel.clone(),
                    },
                    completion,
                    _permit: permit,
                });
            } else {
                tickets.push((file.operation, None));
            }
        }
        drop(states);
        let mut state = crate::util::lock::lock(&self.pool.state);
        context.check()?;
        ensure!(state.accepting, "CONTENT_CANCELLED");
        state.jobs.extend(jobs);
        self.pool.changed.notify_all();
        Ok(tickets)
    }
    /// A bounded volume-flush slice. Callers enumerate larger sets in bounded
    /// pages; namespace/startup enumeration is not supplied by this API.
    pub fn flush_files(&self, files: &[Arc<SaveFile>], context: &ReadContext) -> Result<()> {
        context.check()?;
        ensure!(files.len() <= limits::SAVE_QUEUE, "CONTENT_RESOURCE_LIMIT");
        ensure!(
            files.iter().enumerate().all(|(index, file)| files[..index]
                .iter()
                .all(|other| other.operation != file.operation)),
            "CONTENT_RESOURCE_LIMIT"
        );
        loop {
            context.check()?;
            // Recover earlier publication/adoption before admitting a fresh
            // compatible group, including lost private adoption replies.
            for file in files {
                let pending = lock(&file.state, context)?.pending.is_some();
                if pending {
                    self.flush(file.clone(), context)?;
                }
            }
            match self.enqueue_batch(files, context) {
                Ok(tickets) => {
                    let mut any = false;
                    let mut failure = None;
                    for (_, ticket) in tickets {
                        if let Some(ticket) = ticket {
                            any = true;
                            if let Err(error) = ticket.wait(context) {
                                failure.get_or_insert(error);
                            }
                        }
                    }
                    if let Some(error) = failure {
                        return Err(error);
                    }
                    if !any {
                        return Ok(());
                    }
                }
                Err(error)
                    if crate::model::errors::ErrorWire::from_error(&error).code
                        == "CONTENT_RESOURCE_LIMIT" =>
                {
                    context.cancel.sleep(POLL);
                }
                Err(error) => return Err(error),
            }
        }
    }
    pub fn stop(&self) {
        crate::util::lock::lock(&self.pool.state).accepting = false;
        self.pool.stop.cancel();
        self.pool.changed.notify_all();
    }
    /// A bounded wait, not a claim that uninterruptible OS fsync can be killed.
    /// On timeout, this executor remains registered until its workers finish;
    /// another pool cannot multiply outstanding blocked workers for the engine.
    pub fn shutdown(&self, context: &ReadContext) -> Result<()> {
        self.stop();
        let context = ReadContext {
            deadline: context
                .deadline
                .min(Instant::now() + Duration::from_millis(limits::SHUTDOWN_DRAIN_MS)),
            cancel: context.cancel.clone(),
        };
        loop {
            context.check()?;
            let state = crate::util::lock::lock(&self.pool.state);
            if state.workers == 0 && state.outstanding.is_empty() {
                break;
            }
            drop(
                self.pool
                    .changed
                    .wait_timeout(state, POLL)
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
        }
        for thread in crate::util::lock::lock(&self.threads).drain(..) {
            ensure!(
                thread.join().is_ok(),
                "UNKNOWN_ERROR: content save worker panicked"
            );
        }
        Ok(())
    }
}
impl Drop for SaveExecutor {
    fn drop(&mut self) {
        self.stop();
    }
}
struct WorkerExit(Arc<SavePool>);
impl Drop for WorkerExit {
    fn drop(&mut self) {
        crate::util::lock::lock(&self.0.state).workers -= 1;
        self.0.changed.notify_all();
    }
}
fn worker(pool: Arc<SavePool>) {
    let _exit = WorkerExit(pool.clone());
    loop {
        let mut job = {
            let mut state = crate::util::lock::lock(&pool.state);
            loop {
                if let Some(mut job) = state.jobs.pop_front() {
                    state.active += 1;
                    job._permit.active = true;
                    break job;
                }
                if !state.accepting {
                    return;
                }
                state = pool
                    .changed
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        };
        let result = catch_unwind(AssertUnwindSafe(|| process(&pool, &mut job)))
            .unwrap_or_else(|_| {
                Err(anyhow::anyhow!(
                    "UNKNOWN_ERROR: content save worker panicked"
                ))
            })
            .map_err(|error| format!("{error:#}").chars().take(2048).collect());
        let completion = job.completion.clone();
        drop(job);
        *crate::util::lock::lock(&completion.result) = Some(result);
        completion.changed.notify_all();
    }
}
fn process(pool: &SavePool, job: &mut Job) -> Result<SaveReceipt> {
    let context = &job.context;
    context.check()?;
    let (committed, retry_view) = {
        let (store, view) = connection(pool, &job.checkpoint.intent.branch, context)?;
        (
            store.content_checkpoint_receipt(&job.checkpoint, context)?,
            view,
        )
    };
    let (receipt, _publication_pin) = if let Some(receipt) = committed {
        let pin = pool.target.pin(receipt.content())?;
        observe(pool, &receipt, retry_view, context)?;
        (receipt, pin)
    } else {
        connection(pool, &job.checkpoint.intent.branch, context)?
            .0
            .select_content_checkpoint(&job.checkpoint, context)?;
        crate::util::fault::trigger("content.executor.before_prepare")?;
        let prepared = job
            .session
            .prepare(&pool.target, &job.checkpoint, context)?;
        // Keep newly installed content pinned across metadata publication and
        // the following acquisition/adoption of its continuation base pin.
        let pin = prepared.pin().clone();
        job.session.validate_prepared(&prepared, context)?;
        let (receipt, view) = {
            let (mut store, view) = connection(pool, &job.checkpoint.intent.branch, context)?;
            (store.publish_content_save(&prepared, context)?, view)
        };
        observe(pool, &receipt, view, context)?;
        (receipt, pin)
    };
    crate::util::fault::trigger("content.executor.published")?;
    let handoff = connection(pool, &job.checkpoint.intent.branch, context)?
        .0
        .continue_content_save(&job.checkpoint, context)?;
    crate::util::fault::trigger("content.executor.before_adoption")?;
    // Only this file is excluded during final journal adoption. Preparation
    // above permits new writes; adoption incorporates them into its new roots.
    let mut state = lock(&job.file.state, context)?;
    let next = state
        .session
        .adopt_continuation(&handoff, pool.target.clone(), context)?;
    state.session = Arc::new(next);
    job.file
        .visible_len
        .store(state.session.visible_len(), Ordering::Release);
    state.pending = None;
    context.check()?;
    Ok(receipt)
}
