//! One fatal-exit owner checkpoints native bytes without holding Engine.
use super::{Engine, Shared};
use crate::content::ReadContext;
use anyhow::{Context, Result};
use std::{
    ops::DerefMut,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        MutexGuard, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

static EXITING: AtomicBool = AtomicBool::new(false);

pub(super) fn started() -> bool {
    EXITING.load(Ordering::Acquire)
}
pub(super) fn begin() -> bool {
    EXITING
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
}
pub(super) fn wait_if_exiting() {
    if started() {
        wait();
    }
}
pub(super) fn wait() -> ! {
    loop {
        std::thread::park();
    }
}
pub(super) fn context() -> ReadContext {
    ReadContext {
        deadline: Instant::now() + Duration::from_secs(2),
        cancel: crate::util::Cancel::new(),
    }
}

pub(super) struct Retained<'a> {
    // Keep the final guard until process::exit, including on retention failure.
    pub engine: Option<MutexGuard<'a, Engine>>,
    pub outcome: std::thread::Result<Result<usize>>,
}

pub(super) fn retain<'a>(
    shared: &'a Shared,
    mut initial: impl DerefMut<Target = Engine>,
    context: &ReadContext,
) -> Retained<'a> {
    let mut engine = None;
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        initial.switching = true;
        let prepared = initial.prepare_after_poison();
        let files: Vec<_> = initial
            .sessions
            .values()
            .filter(|session| session.dirty)
            .filter_map(|session| session.bytes.cow_file())
            .collect();
        drop(initial);

        // Do not take adapter_mutation here: another actor can own that gate
        // while waiting for Engine. SaveFile::retain serializes the file state
        // itself, so edits are checkpointed atomically without that deadlock.
        let mut native = Ok(());
        for file in files {
            if let Err(error) = file.retain(context)
                && native.is_ok()
            {
                native = Err(error);
            }
        }
        engine = Some(loop {
            match shared.try_lock() {
                Ok(guard) => break guard,
                Err(TryLockError::Poisoned(error)) => break error.into_inner(),
                Err(TryLockError::WouldBlock) => {
                    context.check()?;
                    context.cancel.sleep(Duration::from_millis(1));
                }
            }
        });
        // Never put retained metadata into an interrupted transaction. Native
        // private checkpoints were still attempted if metadata repair failed.
        let failed_rollback = prepared?;
        let retained = engine.as_mut().unwrap().retain_all_dirty();
        let result = native.and(retained);
        match failed_rollback {
            Some(rollback) => {
                result.with_context(|| format!("after a failed rollback ({rollback:#})"))
            }
            None => result,
        }
    }));
    Retained { engine, outcome }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        content::{LocalContent, write::WriteJournal},
        core::{Store, id},
        runtime::{
            content::ContentWait,
            content_writes::{self, NativeWrites},
        },
        util::fault::{self, FaultAction, FaultConfig},
    };
    use std::sync::{Arc, Mutex};

    struct ResetFault;
    impl Drop for ResetFault {
        fn drop(&mut self) {
            fault::configure(None);
        }
    }

    #[test]
    fn fatal_retention_rolls_back_and_checkpoints_outside_engine_and_adapter_gate() {
        let dir = tempfile::tempdir().unwrap();
        let shared = Arc::new(Mutex::new(Engine::new(
            Store::initialize(dir.path(), "fatal-retain", &id()).unwrap(),
        )));
        let writes = NativeWrites::install(&shared, &context()).unwrap();
        let wait = || ContentWait::fixed(Duration::from_secs(2));
        let (handle, entry) = content_writes::create(
            &shared,
            "file",
            content_writes::CreateOptions {
                writable: true,
                ..Default::default()
            },
            wait(),
        )
        .unwrap()
        .unwrap();
        content_writes::write(
            &shared,
            handle,
            0,
            b"native crash bytes",
            false,
            false,
            wait(),
        )
        .unwrap();
        let file = shared.lock().unwrap().sessions[&entry.id]
            .bytes
            .cow_file()
            .unwrap();
        assert!(!file.recoverable());
        // This gate can belong to an actor waiting for the poisoned Engine.
        let adapter = file.adapter_mutation.lock().unwrap();
        let mut fault = FaultConfig::new("content.write.checkpointed", FaultAction::Hang);
        fault.ms = Some(400);
        fault.count = Some(1);
        fault::configure(Some(fault));
        let _reset = ResetFault;
        let other = shared.clone();
        let worker = std::thread::spawn(move || {
            let initial = other.lock().unwrap();
            initial
                .store
                .db
                .execute_batch("BEGIN IMMEDIATE; CREATE TABLE half_done(x)")
                .unwrap();
            let retained = retain(&other, initial, &context());
            let count = retained.outcome.unwrap().unwrap();
            assert!(retained.engine.is_some());
            count
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        while fault::fired() == 0 {
            assert!(Instant::now() < deadline, "checkpoint did not start");
            std::thread::sleep(Duration::from_millis(1));
        }
        // The checkpoint is deliberately stalled; metadata is already rolled
        // back and its mutex remains available throughout private-journal I/O.
        let engine = shared.try_lock().expect("Engine held during checkpoint");
        assert!(engine.store.db.is_autocommit());
        assert!(
            !engine
                .store
                .db
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='half_done')",
                    [],
                    |row| row.get::<_, bool>(0)
                )
                .unwrap()
        );
        drop(engine);
        assert_eq!(worker.join().unwrap(), 1);
        drop(adapter);
        assert!(file.recoverable());
        writes.shutdown(&context()).unwrap();
        let journal = WriteJournal::open(dir.path()).unwrap();
        let saved = journal.pending(None, 64, &context()).unwrap();
        assert_eq!(
            saved.iter().filter(|save| save.needs_publication()).count(),
            1
        );
        let resumed = journal
            .resume(
                file.operation(),
                Arc::new(LocalContent::open(dir.path()).unwrap()),
                &context(),
            )
            .unwrap();
        let mut bytes = [0; 18];
        resumed.read_range(0, &mut bytes, &context()).unwrap();
        assert_eq!(&bytes, b"native crash bytes");
        assert_eq!(
            shared
                .lock()
                .unwrap()
                .store
                .lookup("file")
                .unwrap()
                .content_ref
                .unwrap()
                .total_len,
            0
        );
    }

    #[test]
    fn poisoned_transaction_is_rolled_back_or_replaced_before_current_saved_recovery() {
        for failed_rollback in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let shared = Arc::new(Mutex::new(Engine::new(
                Store::initialize(dir.path(), "poison-current", &id()).unwrap(),
            )));
            let writes = NativeWrites::install(&shared, &context()).unwrap();
            let wait = || ContentWait::fixed(Duration::from_secs(2));
            let mut operations = Vec::new();
            for (path, bytes) in [
                ("first", b"dirty".as_slice()),
                ("second", b"second".as_slice()),
            ] {
                let (handle, entry) = content_writes::create(
                    &shared,
                    path,
                    content_writes::CreateOptions {
                        writable: true,
                        ..Default::default()
                    },
                    wait(),
                )
                .unwrap()
                .unwrap();
                content_writes::write(&shared, handle, 0, bytes, false, false, wait()).unwrap();
                operations.push((entry.id, bytes));
            }
            let initial = shared.lock().unwrap();
            assert_eq!(initial.vitals.pending_saves(), 0);
            initial.store.db.execute_batch(
                "PRAGMA cache_size=1234; BEGIN IMMEDIATE; SAVEPOINT control_request;
                 CREATE TABLE half_done(x); INSERT INTO half_done VALUES(1); SAVEPOINT staged_save;",
            ).unwrap();
            if failed_rollback {
                fault::configure(Some(FaultConfig {
                    count: Some(1),
                    ..FaultConfig::new("engine.poison_rollback", FaultAction::Fail)
                }));
            }
            let _reset = ResetFault;
            let Retained { engine, outcome } = retain(&shared, initial, &context());
            assert_eq!(outcome.unwrap().unwrap(), 2);
            let engine = engine.unwrap();
            assert!(engine.store.db.is_autocommit());
            assert_eq!(engine.vitals.pending_saves(), 2);
            if failed_rollback {
                assert_eq!(fault::fired(), 1);
                assert_ne!(
                    engine
                        .store
                        .db
                        .query_row("PRAGMA cache_size", [], |r| r.get::<_, i64>(0))
                        .unwrap(),
                    1234
                );
            }
            let observer = rusqlite::Connection::open(dir.path().join("metadata.sqlite")).unwrap();
            assert!(
                !observer
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='half_done')",
                        [],
                        |r| r.get::<_, bool>(0)
                    )
                    .unwrap()
            );
            for (entity, _) in &operations {
                assert!(engine.sessions[entity].dirty);
                assert_eq!(
                    engine
                        .store
                        .entry(entity)
                        .unwrap()
                        .content_ref
                        .unwrap()
                        .total_len,
                    0
                );
            }
            drop(engine);
            fault::configure(None);
            let Retained { engine, outcome } = retain(&shared, shared.lock().unwrap(), &context());
            assert_eq!(
                outcome.unwrap().unwrap(),
                2,
                "retaining Saved is idempotent"
            );
            drop(engine);
            writes.shutdown(&context()).unwrap();
            drop(writes);
            drop(shared);

            let journal = WriteJournal::open(dir.path()).unwrap();
            let saved = journal.pending(None, 64, &context()).unwrap();
            assert_eq!(saved.len(), 2);
            assert_eq!(journal.pending_count(), 2);
            let provider = Arc::new(LocalContent::open(dir.path()).unwrap());
            for save in saved {
                let expected = operations
                    .iter()
                    .find(|(entity, _)| entity == &save.intent.base.entity)
                    .unwrap()
                    .1;
                let session = journal
                    .resume(save.intent.operation, provider.clone(), &context())
                    .unwrap();
                let mut bytes = vec![0; expected.len()];
                session.read_range(0, &mut bytes, &context()).unwrap();
                assert_eq!(bytes, expected);
            }
        }
    }
}
