//! Runtime worker bounds and the actual journal/metadata/adoption pipeline.
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tkfs::{
    content::{ContentProvider, LocalContent, ReadContext, write::WriteJournal},
    core::{Store, id},
    model::workspace::limits,
    runtime::{
        Engine, Shared,
        content_saves::{SaveExecutor, SaveFile},
    },
    util::{
        Cancel,
        fault::{self, FaultAction, FaultConfig},
    },
};
fn context() -> ReadContext {
    ReadContext {
        deadline: Instant::now() + Duration::from_secs(60),
        cancel: Cancel::new(),
    }
}
fn short(ms: u64) -> ReadContext {
    ReadContext {
        deadline: Instant::now() + Duration::from_millis(ms),
        cancel: Cancel::new(),
    }
}
fn error<T>(result: anyhow::Result<T>, code: &str) {
    let err = result.err().expect("expected error");
    assert!(format!("{err:#}").contains(code), "{err:#}");
}
struct FaultReset;
impl Drop for FaultReset {
    fn drop(&mut self) {
        fault::configure(None);
    }
}
fn arm(point: &str, action: FaultAction, ms: u64, count: u64) -> FaultReset {
    let mut config = FaultConfig::new(point, action);
    config.ms = Some(ms);
    config.count = Some(count);
    fault::configure(Some(config));
    FaultReset
}
fn fired(count: u64) {
    let until = Instant::now() + Duration::from_secs(3);
    while fault::fired() < count && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(fault::fired(), count);
}
struct Fixture {
    shared: Shared,
    journal: Arc<WriteJournal>,
    native: Arc<LocalContent>,
    dir: tempfile::TempDir,
}
impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::initialize(dir.path(), "content-executor", &id()).unwrap();
        store
            .db
            .execute("INSERT INTO config VALUES('fixture:content_v1','1')", [])
            .unwrap();
        Self {
            shared: Arc::new(Mutex::new(Engine::new(store))),
            journal: WriteJournal::open(dir.path()).unwrap(),
            native: Arc::new(LocalContent::open(dir.path()).unwrap()),
            dir,
        }
    }
    fn file(&self, name: &str) -> Arc<SaveFile> {
        let intent = {
            let mut e = tkfs::runtime::lock_engine(&self.shared);
            let entry = e.store.create(name, "file").unwrap();
            e.store.begin_content_save(&entry.id, &context()).unwrap()
        };
        SaveFile::new(
            self.journal
                .create(intent, self.native.clone(), &context())
                .unwrap(),
        )
    }
    fn bytes(&self, file: &SaveFile, len: usize) -> Vec<u8> {
        let mut bytes = vec![0; len];
        file.read_range(0, &mut bytes, &context()).unwrap();
        bytes
    }
    fn publications(&self) -> usize {
        tkfs::runtime::lock_engine(&self.shared)
            .store
            .db
            .query_row("SELECT count(*) FROM content_save_history", [], |r| {
                r.get(0)
            })
            .unwrap()
    }
    fn oracle(&self) {
        assert!(
            tkfs::runtime::lock_engine(&self.shared)
                .store
                .verify_derived_state()
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn real_pipeline_reuses_file_identity_and_flushes_edits_made_during_preparation() {
    let f = Fixture::new();
    let file = f.file("file");
    let pool = SaveExecutor::start(&f.shared, f.journal.clone(), &context()).unwrap();
    file.write_at(0, b"initial", &context()).unwrap();
    let reset = arm("content.executor.before_prepare", FaultAction::Hang, 300, 1);
    let ticket = pool.enqueue(file.clone(), &context()).unwrap().unwrap();
    fired(1);
    file.write_at(0, b"updated", &context()).unwrap();
    let receipt = ticket.wait(&context()).unwrap();
    drop(reset);
    assert_eq!(f.bytes(&file, 7), b"updated");
    let pin = f.native.pin(receipt.content()).unwrap();
    let mut bytes = [0; 7];
    f.native
        .read_range(&pin, 0, &mut bytes, &context())
        .unwrap();
    assert_eq!(&bytes, b"initial");
    pool.flush(file.clone(), &context()).unwrap();
    assert_eq!(f.publications(), 2);
    assert!(pool.enqueue(file, &context()).unwrap().is_none());
    f.oracle();
    pool.shutdown(&context()).unwrap();
    assert_eq!(pool.stats().outstanding, 0);
}

#[test]
fn sixty_four_files_share_one_private_checkpoint_before_independent_publications() {
    let f = Fixture::new();
    let files: Vec<_> = (0..64)
        .map(|i| {
            let file = f.file(&format!("batch{i}"));
            file.write_at(0, &(i as u64).to_le_bytes(), &context())
                .unwrap();
            file
        })
        .collect();
    let pool = SaveExecutor::start(&f.shared, f.journal.clone(), &context()).unwrap();
    let before = f.journal.checkpoint_work();
    let tickets = pool.enqueue_batch(&files, &context()).unwrap();
    let after = f.journal.checkpoint_work();
    assert_eq!(after.commits - before.commits, 1);
    assert_eq!(after.sessions - before.sessions, 64);
    assert!(tickets.windows(2).all(|pair| pair[0].0 < pair[1].0));
    for (_, ticket) in tickets {
        ticket.unwrap().wait(&context()).unwrap();
    }
    assert_eq!(f.publications(), 64);
    for (i, file) in files.iter().enumerate() {
        assert_eq!(f.bytes(file, 8), (i as u64).to_le_bytes());
    }
    pool.flush_files(&files, &context()).unwrap();
    f.oracle();
    pool.shutdown(&context()).unwrap();
    println!(
        "WAVE6_CHECKPOINT_BATCH {}",
        serde_json::json!({"files":64,"private_full_checkpoint_transactions":1,"dirty_sessions_advanced":64,"metadata_publications":64,"metadata_batched":false})
    );
}

#[test]
fn a_failed_group_retains_working_and_releases_every_reservation_before_retry() {
    let f = Fixture::new();
    let files: Vec<_> = (0..3)
        .map(|i| {
            let file = f.file(&format!("file{i}"));
            file.write_at(0, b"new", &context()).unwrap();
            file
        })
        .collect();
    let pool = SaveExecutor::start(&f.shared, f.journal.clone(), &context()).unwrap();
    let reset = arm(
        "content.write.before_checkpoint_commit",
        FaultAction::Fail,
        0,
        1,
    );
    error(pool.enqueue_batch(&files, &context()), "FAULT_INJECTED");
    drop(reset);
    assert_eq!(pool.stats().outstanding, 0);
    assert_eq!(f.journal.checkpoint_work().commits, 0);
    assert!(f.journal.pending(None, 256, &context()).unwrap().is_empty());
    for file in &files {
        assert_eq!(f.bytes(file, 3), b"new");
    }
    pool.flush_files(&files, &context()).unwrap();
    assert_eq!(f.publications(), 3);
    f.oracle();
    pool.shutdown(&context()).unwrap();
}

#[test]
fn batch_limits_duplicates_foreign_files_and_pending_pipelines_do_not_partially_queue() {
    let f = Fixture::new();
    let file = f.file("file");
    file.write_at(0, b"new", &context()).unwrap();
    let foreign = Fixture::new();
    let other = foreign.file("other");
    let pool = SaveExecutor::start(&f.shared, f.journal.clone(), &context()).unwrap();
    error(
        pool.enqueue_batch(&[file.clone(), file.clone()], &context()),
        "CONTENT_RESOURCE_LIMIT",
    );
    error(
        pool.flush_files(&[file.clone(), file.clone()], &context()),
        "CONTENT_RESOURCE_LIMIT",
    );
    error(
        pool.enqueue_batch(&vec![file.clone(); 65], &context()),
        "CONTENT_RESOURCE_LIMIT",
    );
    error(
        pool.enqueue_batch(&[file.clone(), other], &context()),
        "CONTENT_SAVE_WRONG_STORE",
    );
    assert_eq!(f.journal.checkpoint_work().commits, 0);
    assert_eq!(pool.stats().outstanding, 0);
    assert!(pool.enqueue_batch(&[], &context()).unwrap().is_empty());
    let reset = arm("content.executor.before_prepare", FaultAction::Fail, 0, 1);
    error(
        pool.enqueue(file.clone(), &context())
            .unwrap()
            .unwrap()
            .wait(&context()),
        "FAULT_INJECTED",
    );
    drop(reset);
    error(
        pool.enqueue_batch(std::slice::from_ref(&file), &context()),
        "CONTENT_RESOURCE_LIMIT",
    );
    assert_eq!(pool.stats().outstanding, 0);
    pool.flush_files(&[file], &context()).unwrap();
    assert_eq!(f.publications(), 1);
    pool.shutdown(&context()).unwrap();
}

#[test]
fn lost_replies_at_publication_and_adoption_replay_exactly_once() {
    for point in [
        "content.save.committed",
        "content.executor.published",
        "content.executor.before_adoption",
        "content.write.adopted",
    ] {
        let f = Fixture::new();
        let file = f.file("file");
        let pool = SaveExecutor::start(&f.shared, f.journal.clone(), &context()).unwrap();
        file.write_at(0, b"durable", &context()).unwrap();
        let reset = arm(point, FaultAction::Fail, 0, 1);
        let ticket = pool.enqueue(file.clone(), &context()).unwrap().unwrap();
        error(ticket.wait(&context()), "FAULT_INJECTED");
        drop(reset);
        assert_eq!(f.publications(), 1);
        pool.flush(file.clone(), &context()).unwrap();
        assert_eq!(f.publications(), 1);
        assert_eq!(f.bytes(&file, 7), b"durable");
        file.write_at(0, b"next!!!", &context()).unwrap();
        pool.flush(file.clone(), &context()).unwrap();
        assert_eq!(f.publications(), 2);
        f.oracle();
        pool.shutdown(&context()).unwrap();
    }
}

#[test]
fn a_superseded_unpublished_checkpoint_can_retry_the_latest_saved_bytes() {
    let f = Fixture::new();
    let file = f.file("file");
    let pool = SaveExecutor::start(&f.shared, f.journal.clone(), &context()).unwrap();
    file.write_at(0, b"old", &context()).unwrap();
    let reset = arm("content.executor.before_prepare", FaultAction::Fail, 0, 1);
    error(
        pool.enqueue(file.clone(), &context())
            .unwrap()
            .unwrap()
            .wait(&context()),
        "FAULT_INJECTED",
    );
    drop(reset);
    file.write_at(0, b"new", &context()).unwrap();
    pool.flush(file.clone(), &context()).unwrap();
    assert_eq!(f.publications(), 1);
    assert_eq!(f.bytes(&file, 3), b"new");
    f.oracle();
    pool.shutdown(&context()).unwrap();
}

#[test]
fn restart_adopts_a_committed_parent_before_publishing_a_newer_saved_checkpoint() {
    let f = Fixture::new();
    let file = f.file("file");
    let pool = SaveExecutor::start(&f.shared, f.journal.clone(), &context()).unwrap();
    file.write_at(0, b"parent", &context()).unwrap();
    let reset = arm("content.executor.before_adoption", FaultAction::Fail, 0, 2);
    error(
        pool.enqueue(file.clone(), &context())
            .unwrap()
            .unwrap()
            .wait(&context()),
        "FAULT_INJECTED",
    );
    file.write_at(0, b"newest", &context()).unwrap();
    // Enqueue checkpoints these later bytes even while the prior publication
    // still needs adoption. Its retry must keep the committed parent identity.
    error(
        pool.enqueue(file.clone(), &context())
            .unwrap()
            .unwrap()
            .wait(&context()),
        "FAULT_INJECTED",
    );
    drop(reset);
    assert_eq!(f.publications(), 1);
    let operation = file.operation();
    pool.shutdown(&context()).unwrap();
    drop(pool);
    drop(file);
    let Fixture {
        shared,
        journal,
        native,
        dir,
    } = f;
    drop(shared);
    drop(journal);
    drop(native);
    let shared = Arc::new(Mutex::new(Engine::new(
        Store::open_existing(dir.path()).unwrap(),
    )));
    let journal = WriteJournal::open(dir.path()).unwrap();
    let native = Arc::new(LocalContent::open(dir.path()).unwrap());
    let file = SaveFile::new(journal.resume(operation, native, &context()).unwrap());
    let pool = SaveExecutor::start(&shared, journal.clone(), &context()).unwrap();
    pool.flush(file.clone(), &context()).unwrap();
    let mut bytes = [0; 6];
    file.read_range(0, &mut bytes, &context()).unwrap();
    assert_eq!(&bytes, b"newest");
    let mut e = tkfs::runtime::lock_engine(&shared);
    assert_eq!(
        e.store
            .db
            .query_row("SELECT count(*) FROM content_save_history", [], |r| r
                .get::<_, usize>(0))
            .unwrap(),
        2
    );
    assert!(e.store.verify_derived_state().unwrap().is_empty());
    drop(e);
    pool.shutdown(&context()).unwrap();
}

#[test]
fn panic_releases_its_job_and_the_fixed_worker_can_process_a_retry() {
    let f = Fixture::new();
    let file = f.file("file");
    let pool = SaveExecutor::start(&f.shared, f.journal.clone(), &context()).unwrap();
    file.write_at(0, b"safe", &context()).unwrap();
    let reset = arm("content.executor.before_prepare", FaultAction::Panic, 0, 1);
    error(
        pool.enqueue(file.clone(), &context())
            .unwrap()
            .unwrap()
            .wait(&context()),
        "UNKNOWN_ERROR",
    );
    drop(reset);
    assert_eq!(pool.stats().outstanding, 0);
    assert_eq!(pool.stats().workers, 4);
    pool.flush(file, &context()).unwrap();
    assert_eq!(f.publications(), 1);
    pool.shutdown(&context()).unwrap();
}

#[test]
fn full_queue_has_four_workers_no_inline_fallback_and_a_bounded_shutdown_wait() {
    let f = Fixture::new();
    let files: Vec<_> = (0..65)
        .map(|i| {
            let file = f.file(&format!("file{i}"));
            file.write_at(0, b"safe", &context()).unwrap();
            file
        })
        .collect();
    let pool = SaveExecutor::start(&f.shared, f.journal.clone(), &context()).unwrap();
    let reset = arm(
        "content.executor.before_prepare",
        FaultAction::Hang,
        4000,
        4,
    );
    let mut tickets = Vec::new();
    for file in &files[..4] {
        tickets.push(pool.enqueue(file.clone(), &context()).unwrap().unwrap());
    }
    fired(4);
    for file in &files[4..64] {
        tickets.push(pool.enqueue(file.clone(), &context()).unwrap().unwrap());
    }
    let stats = pool.stats();
    assert_eq!(stats.outstanding, 64);
    assert_eq!(stats.active, 4);
    assert_eq!(stats.queued, 60);
    assert_eq!(stats.workers, 4);
    let started = Instant::now();
    error(
        pool.enqueue(files[64].clone(), &context()),
        "CONTENT_RESOURCE_LIMIT",
    );
    assert!(started.elapsed() < Duration::from_millis(300));
    error(
        SaveExecutor::start(&f.shared, f.journal.clone(), &context()),
        "CONTENT_RESOURCE_LIMIT",
    );
    // Native metadata work remains possible while all preparation workers stall.
    let started = Instant::now();
    tkfs::runtime::lock_engine(&f.shared)
        .store
        .create("independent", "directory")
        .unwrap();
    assert!(started.elapsed() < Duration::from_millis(300));
    let started = Instant::now();
    error(pool.shutdown(&short(75)), "CONTENT_DEADLINE");
    assert!(started.elapsed() < Duration::from_millis(300));
    error(
        SaveExecutor::start(&f.shared, f.journal.clone(), &context()),
        "CONTENT_RESOURCE_LIMIT",
    );
    pool.shutdown(&context()).unwrap();
    drop(reset);
    for ticket in tickets {
        error(ticket.wait(&context()), "CONTENT_CANCELLED");
    }
    assert_eq!(f.publications(), 0);
    assert_eq!(f.journal.pending(None, 256, &context()).unwrap().len(), 64);
    assert_eq!(pool.stats().outstanding, 0);
    assert_eq!(pool.stats().workers, 0);
    println!(
        "WAVE6_EXECUTOR {}",
        serde_json::json!({"outstanding_limit":limits::SAVE_QUEUE,"workers":limits::SAVE_WORKERS,"queued_peak":60,"pending_after_cancel":64,"uncooperative_fault_ms":4000})
    );
}

#[test]
fn cancelled_ticket_wait_does_not_cancel_a_durable_accepted_save() {
    let f = Fixture::new();
    let file = f.file("file");
    file.write_at(0, b"saved", &context()).unwrap();
    let pool = SaveExecutor::start(&f.shared, f.journal.clone(), &context()).unwrap();
    let reset = arm("content.executor.before_prepare", FaultAction::Hang, 200, 1);
    let ticket = pool.enqueue(file, &context()).unwrap().unwrap();
    fired(1);
    let wait = context();
    wait.cancel.cancel();
    error(ticket.wait(&wait), "CONTENT_CANCELLED");
    ticket.wait(&context()).unwrap();
    drop(reset);
    assert_eq!(f.publications(), 1);
    pool.shutdown(&context()).unwrap();
}

#[test]
fn engine_lock_wait_obeys_deadline_without_leaking_a_queue_reservation() {
    let f = Fixture::new();
    let file = f.file("file");
    file.write_at(0, b"pending", &context()).unwrap();
    let pool = SaveExecutor::start(&f.shared, f.journal.clone(), &context()).unwrap();
    let guard = tkfs::runtime::lock_engine(&f.shared);
    error(pool.enqueue(file, &short(50)), "CONTENT_DEADLINE");
    assert_eq!(pool.stats().outstanding, 0);
    drop(guard);
    assert_eq!(f.journal.pending(None, 256, &context()).unwrap().len(), 1);
    pool.shutdown(&context()).unwrap();
}

fn independent_engine(shared: &Shared) -> tkfs::runtime::EngineGuard<'_> {
    let began = Instant::now();
    loop {
        if let Some(guard) = tkfs::runtime::try_lock_engine(shared) {
            return guard;
        }
        assert!(
            began.elapsed() < Duration::from_millis(300),
            "save metadata I/O held Engine"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn a_blocked_metadata_publication_leaves_engine_reads_and_other_private_writes_live() {
    let f = Fixture::new();
    let file = f.file("file");
    let other = f.file("other");
    let pool = SaveExecutor::start(&f.shared, f.journal.clone(), &context()).unwrap();
    file.write_at(0, b"durable", &context()).unwrap();
    let revision = independent_engine(&f.shared).vitals.store_revision();
    let reset = arm("content.save.event_written", FaultAction::Hang, 1000, 1);
    let ticket = pool.enqueue(file.clone(), &context()).unwrap().unwrap();
    fired(1);
    let began = Instant::now();
    {
        let engine = independent_engine(&f.shared);
        assert!(engine.store.lookup("file").unwrap().content_ref.is_none());
        assert_eq!(
            engine
                .store
                .db
                .query_row("SELECT count(*) FROM content_save_history", [], |r| r
                    .get::<_, usize>(0))
                .unwrap(),
            0
        );
    }
    let engine_ms = began.elapsed().as_secs_f64() * 1000.0;
    other.write_at(0, b"working", &context()).unwrap();
    assert_eq!(f.bytes(&other, 7), b"working");
    let queued = pool.enqueue(other.clone(), &context()).unwrap().unwrap();
    assert!(began.elapsed() < Duration::from_millis(500));
    ticket.wait(&context()).unwrap();
    queued.wait(&context()).unwrap();
    drop(reset);
    assert_eq!(f.publications(), 2);
    assert!(independent_engine(&f.shared).vitals.store_revision() > revision);
    assert_eq!(f.bytes(&file, 7), b"durable");
    f.oracle();
    pool.shutdown(&context()).unwrap();
    println!(
        "WAVE6_SAVE_DB {}",
        serde_json::json!({"scenario":"metadata_writer_held_off_engine","writer_stall_ms":1000,"engine_read_ms":engine_ms,"private_other_write":true,"publications":2})
    );
}

#[test]
fn a_metadata_writer_wait_obeys_job_deadline_and_retains_saved_without_engine_ownership() {
    let f = Fixture::new();
    let file = f.file("file");
    file.write_at(0, b"pending", &context()).unwrap();
    let pool = SaveExecutor::start(&f.shared, f.journal.clone(), &context()).unwrap();
    let writer = rusqlite::Connection::open(f.dir.path().join("metadata.sqlite")).unwrap();
    writer.execute_batch("BEGIN IMMEDIATE").unwrap();
    let began = Instant::now();
    let ticket = pool.enqueue(file.clone(), &short(200)).unwrap().unwrap();
    let until = Instant::now() + Duration::from_secs(1);
    while pool.stats().active == 0 {
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(1));
    }
    drop(independent_engine(&f.shared));
    error(ticket.wait(&context()), "CONTENT_DEADLINE");
    assert!(began.elapsed() < Duration::from_millis(750));
    assert_eq!(pool.stats().outstanding, 0);
    assert_eq!(f.publications(), 0);
    assert_eq!(f.journal.pending(None, 256, &context()).unwrap().len(), 1);
    writer.execute_batch("ROLLBACK").unwrap();
    pool.flush(file.clone(), &context()).unwrap();
    assert_eq!(f.bytes(&file, 7), b"pending");
    assert_eq!(f.publications(), 1);
    f.oracle();
    pool.shutdown(&context()).unwrap();
}

#[test]
fn a_publication_panic_rolls_back_its_secondary_without_poisoning_engine() {
    let f = Fixture::new();
    let file = f.file("file");
    file.write_at(0, b"retained", &context()).unwrap();
    let pool = SaveExecutor::start(&f.shared, f.journal.clone(), &context()).unwrap();
    let reset = arm("content.save.event_written", FaultAction::Panic, 0, 1);
    error(
        pool.enqueue(file.clone(), &context())
            .unwrap()
            .unwrap()
            .wait(&context()),
        "UNKNOWN_ERROR",
    );
    drop(reset);
    assert!(!f.shared.is_poisoned());
    assert_eq!(f.publications(), 0);
    independent_engine(&f.shared)
        .store
        .create("independent", "file")
        .unwrap();
    pool.flush(file.clone(), &context()).unwrap();
    assert_eq!(f.bytes(&file, 8), b"retained");
    assert_eq!(f.publications(), 1);
    f.oracle();
    pool.shutdown(&context()).unwrap();
}

#[test]
fn a_failed_secondary_rollback_is_quarantined_and_its_shared_pool_can_retry() {
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization, TransactionOperation};
    let f = Fixture::new();
    let file = f.file("file");
    file.write_at(0, b"recover", &context()).unwrap();
    let worker = tkfs::worker::build_ctx(f.shared.clone(), f.dir.path(), false, None);
    tkfs::runtime::register_worker(&worker);
    let seeded = worker.sync_stores.checkout().unwrap();
    seeded.db.authorizer(Some(|ctx: AuthContext<'_>| {
        if matches!(
            ctx.action,
            AuthAction::Transaction {
                operation: TransactionOperation::Rollback
            }
        ) {
            Authorization::Deny
        } else {
            Authorization::Allow
        }
    }));
    drop(seeded);
    assert_eq!(worker.sync_stores.idle(), 1);
    let pool = SaveExecutor::start(&f.shared, f.journal.clone(), &context()).unwrap();
    let reset = arm("content.save.event_written", FaultAction::Panic, 0, 1);
    error(
        pool.enqueue(file.clone(), &context())
            .unwrap()
            .unwrap()
            .wait(&context()),
        "UNKNOWN_ERROR",
    );
    drop(reset);
    assert!(!f.shared.is_poisoned());
    assert_eq!(
        worker.sync_stores.idle(),
        0,
        "quarantined connection was returned to its pool"
    );
    assert_eq!(f.publications(), 0);
    independent_engine(&f.shared)
        .store
        .create("unblocked", "file")
        .unwrap();
    pool.flush(file.clone(), &context()).unwrap();
    assert_eq!(f.bytes(&file, 7), b"recover");
    assert_eq!(f.publications(), 1);
    assert!(worker.sync_stores.idle() > 0);
    f.oracle();
    pool.shutdown(&context()).unwrap();
}

#[test]
fn an_old_committed_file_cannot_refresh_or_adopt_into_a_different_active_branch() {
    let f = Fixture::new();
    let file = f.file("file");
    file.write_at(0, b"old branch", &context()).unwrap();
    let pool = SaveExecutor::start(&f.shared, f.journal.clone(), &context()).unwrap();
    let reset = arm("content.executor.published", FaultAction::Fail, 0, 1);
    error(
        pool.enqueue(file.clone(), &context())
            .unwrap()
            .unwrap()
            .wait(&context()),
        "FAULT_INJECTED",
    );
    drop(reset);
    let before = f.publications();
    let branch = {
        let mut e = independent_engine(&f.shared);
        let branch = e.store.fork("other", true).unwrap();
        e.store.checkout(&branch.id).unwrap();
        branch
    };
    error(pool.enqueue(file.clone(), &context()), "STALE_CONTENT_VIEW");
    assert_eq!(pool.stats().outstanding, 0);
    assert_eq!(f.publications(), before);
    assert_eq!(independent_engine(&f.shared).store.active, branch.id);
    assert!(!f.journal.pending(None, 256, &context()).unwrap().is_empty());
    pool.shutdown(&context()).unwrap();
}

#[test]
fn the_coordinator_refuses_a_primary_caller_transaction_instead_of_escaping_it() {
    let f = Fixture::new();
    let file = f.file("file");
    file.write_at(0, b"saved", &context()).unwrap();
    let pool = SaveExecutor::start(&f.shared, f.journal.clone(), &context()).unwrap();
    independent_engine(&f.shared)
        .store
        .db
        .execute_batch("BEGIN IMMEDIATE")
        .unwrap();
    error(
        pool.enqueue(file.clone(), &context()),
        "CONTENT_SAVE_REQUIRES_OUTER_TRANSACTION",
    );
    assert_eq!(pool.stats().outstanding, 0);
    assert_eq!(f.journal.pending(None, 256, &context()).unwrap().len(), 1);
    independent_engine(&f.shared)
        .store
        .db
        .execute_batch("ROLLBACK")
        .unwrap();
    pool.flush(file.clone(), &context()).unwrap();
    assert_eq!(f.bytes(&file, 5), b"saved");
    assert_eq!(f.publications(), 1);
    f.oracle();
    pool.shutdown(&context()).unwrap();
}

#[test]
fn an_executor_cannot_join_a_foreign_or_independently_opened_journal() {
    let f = Fixture::new();
    let foreign = Fixture::new();
    error(
        SaveExecutor::start(&f.shared, foreign.journal.clone(), &context()),
        "CONTENT_SAVE_WRONG_STORE",
    );
    let pool = SaveExecutor::start(&f.shared, f.journal.clone(), &context()).unwrap();
    let file = foreign.file("foreign");
    file.write_at(0, b"private", &context()).unwrap();
    error(pool.enqueue(file, &context()), "CONTENT_SAVE_WRONG_STORE");
    let intent = {
        let mut e = tkfs::runtime::lock_engine(&f.shared);
        let entry = e.store.create("other", "file").unwrap();
        e.store.begin_content_save(&entry.id, &context()).unwrap()
    };
    let separate = WriteJournal::open(f.dir.path()).unwrap();
    let file = SaveFile::new(
        separate
            .create(intent, f.native.clone(), &context())
            .unwrap(),
    );
    error(pool.enqueue(file, &context()), "CONTENT_SAVE_WRONG_STORE");
    assert_eq!(pool.stats().outstanding, 0);
    assert_eq!(f.publications(), 0);
    pool.shutdown(&context()).unwrap();
}

#[test]
fn unintegrated_mounted_callbacks_are_refused_without_publishing_a_revision() {
    let f = Fixture::new();
    tkfs::runtime::lock_engine(&f.shared).mounted = true;
    error(
        SaveExecutor::start(&f.shared, f.journal.clone(), &context()),
        "PEER_NEEDS_UPGRADE",
    );
    tkfs::runtime::lock_engine(&f.shared).mounted = false;
    let file = f.file("file");
    file.write_at(0, b"pending", &context()).unwrap();
    let pool = SaveExecutor::start(&f.shared, f.journal.clone(), &context()).unwrap();
    tkfs::runtime::lock_engine(&f.shared).mounted = true;
    error(pool.enqueue(file, &context()), "PEER_NEEDS_UPGRADE");
    tkfs::runtime::lock_engine(&f.shared).mounted = false;
    assert_eq!(pool.stats().outstanding, 0);
    assert_eq!(f.publications(), 0);
    assert_eq!(f.journal.pending(None, 256, &context()).unwrap().len(), 1);
    pool.shutdown(&context()).unwrap();
}

#[test]
#[ignore = "parent process invokes this actual crash helper"]
fn content_executor_crash_child() {
    let Some(root) = std::env::var_os("TKFS_EXECUTOR_CHILD_ROOT") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let shared = Arc::new(Mutex::new(Engine::new(
        Store::open_existing(&root).unwrap(),
    )));
    let journal = WriteJournal::open(&root).unwrap();
    let native = Arc::new(LocalContent::open(&root).unwrap());
    let op = journal
        .pending(None, 256, &context())
        .unwrap()
        .remove(0)
        .intent
        .operation;
    let file = SaveFile::new(journal.resume(op, native, &context()).unwrap());
    let pool = SaveExecutor::start(&shared, journal.clone(), &context()).unwrap();
    pool.flush(file, &context()).unwrap();
    panic!("crash point did not terminate child");
}

#[test]
fn real_process_exits_recover_publication_and_private_adoption_without_duplicate_events() {
    for point in [
        "content.executor.before_prepare",
        "content.executor.published",
        "content.executor.before_adoption",
        "content.write.adopted",
    ] {
        let f = Fixture::new();
        let file = f.file("file");
        file.write_at(0, b"recover", &context()).unwrap();
        // Durable close checkpoint, then cancel only the queued preparation.
        let pool = SaveExecutor::start(&f.shared, f.journal.clone(), &context()).unwrap();
        let reset = arm("content.executor.before_prepare", FaultAction::Fail, 0, 1);
        error(
            pool.enqueue(file.clone(), &context())
                .unwrap()
                .unwrap()
                .wait(&context()),
            "FAULT_INJECTED",
        );
        drop(reset);
        pool.shutdown(&context()).unwrap();
        drop(pool);
        let operation = file.operation();
        drop(file);
        let Fixture {
            shared,
            journal,
            native,
            dir,
        } = f;
        drop(shared);
        drop(journal);
        drop(native);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "content_executor_crash_child",
                "--ignored",
                "--nocapture",
            ])
            .env("TKFS_EXECUTOR_CHILD_ROOT", dir.path())
            .env("TKFS_FAULT_POINT", point)
            .env("TKFS_FAULT_ACTION", "exit")
            .env("TKFS_FAULT_COUNT", "1")
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86));
        let shared = Arc::new(Mutex::new(Engine::new(
            Store::open_existing(dir.path()).unwrap(),
        )));
        let journal = WriteJournal::open(dir.path()).unwrap();
        let native = Arc::new(LocalContent::open(dir.path()).unwrap());
        let file = SaveFile::new(journal.resume(operation, native, &context()).unwrap());
        let pool = SaveExecutor::start(&shared, journal.clone(), &context()).unwrap();
        pool.flush(file.clone(), &context()).unwrap();
        let mut bytes = [0; 7];
        file.read_range(0, &mut bytes, &context()).unwrap();
        assert_eq!(&bytes, b"recover");
        let mut e = tkfs::runtime::lock_engine(&shared);
        assert_eq!(
            e.store
                .db
                .query_row("SELECT count(*) FROM content_save_history", [], |r| r
                    .get::<_, usize>(0))
                .unwrap(),
            1
        );
        assert!(e.store.verify_derived_state().unwrap().is_empty());
        drop(e);
        pool.shutdown(&context()).unwrap();
    }
}
