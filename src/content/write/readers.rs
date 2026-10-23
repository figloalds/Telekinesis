//! Bounded, short WAL snapshots. Never retain a reader across provider I/O.
use super::*;

const CONNECTIONS: usize = 4;
#[derive(Default)]
pub(super) struct Readers {
    allocated: AtomicUsize,
    idle: Mutex<Vec<Connection>>,
}
struct Lease<'a> {
    readers: &'a Readers,
    connection: Option<Connection>,
}
impl Drop for Lease<'_> {
    fn drop(&mut self) {
        let Some(db) = self.connection.take() else {
            return;
        };
        db.progress_handler(0, None::<fn() -> bool>);
        if !db.is_autocommit() {
            // A failed read rollback cannot contaminate a later snapshot.
            // Discard this connection without changing writer authority.
            self.readers.allocated.fetch_sub(1, Ordering::AcqRel);
            drop(db);
        } else {
            crate::util::lock::lock(&self.readers.idle).push(db);
        }
    }
}
impl Readers {
    fn lease<'a>(&'a self, journal: &WriteJournal, context: &ReadContext) -> Result<Lease<'a>> {
        loop {
            context.check()?;
            ensure!(
                !journal.connection_failed(),
                "WRITE_JOURNAL_CONNECTION_FAILED"
            );
            if let Some(db) = crate::util::lock::lock(&self.idle).pop() {
                return Ok(Lease {
                    readers: self,
                    connection: Some(db),
                });
            }
            if self
                .allocated
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                    (n < CONNECTIONS).then_some(n + 1)
                })
                .is_ok()
            {
                let opened = (|| {
                    let db = Connection::open_with_flags(
                        journal.root.join("write-sessions-v1.sqlite"),
                        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
                    )?;
                    db.busy_timeout(Duration::ZERO)?;
                    db.execute_batch(
                        "PRAGMA query_only=ON; PRAGMA cache_size=-512; PRAGMA temp_store=FILE;",
                    )?;
                    format::current(&db)?;
                    context.check()?;
                    Ok::<_, anyhow::Error>(db)
                })();
                match opened {
                    Ok(db) => {
                        return Ok(Lease {
                            readers: self,
                            connection: Some(db),
                        });
                    }
                    Err(error) => {
                        self.allocated.fetch_sub(1, Ordering::AcqRel);
                        return Err(error);
                    }
                }
            }
            context.cancel.sleep(Duration::from_millis(1));
        }
    }
}
impl WriteJournal {
    /// The closure must only inspect this private journal. Its SQLite snapshot
    /// and permit end before any base/provider/network operation.
    pub(super) fn read_snapshot<T>(
        &self,
        context: &ReadContext,
        read: impl FnOnce(&Connection) -> Result<T>,
    ) -> Result<T> {
        let mut lease = self.readers.lease(self, context)?;
        let db = lease.connection.as_mut().unwrap();
        let cancel = context.cancel.clone();
        let deadline = context.deadline;
        db.progress_handler(
            1000,
            Some(move || cancel.is_cancelled() || std::time::Instant::now() >= deadline),
        );
        let tx = db.transaction()?;
        let value = read(&tx)?;
        context.check()?;
        ensure!(!self.connection_failed(), "WRITE_JOURNAL_CONNECTION_FAILED");
        tx.commit()?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;
    fn context(ms: u64) -> ReadContext {
        ReadContext {
            deadline: Instant::now() + Duration::from_millis(ms),
            cancel: crate::util::Cancel::new(),
        }
    }
    #[test]
    fn snapshot_capacity_waits_honor_deadline_and_cancel_then_reuse_the_permit() {
        let dir = tempfile::tempdir().unwrap();
        let journal = WriteJournal::open(dir.path()).unwrap();
        let mut held = Vec::new();
        for _ in 0..CONNECTIONS {
            held.push(journal.readers.lease(&journal, &context(1000)).unwrap());
        }
        let began = Instant::now();
        let result = journal.read_snapshot(&context(40), |_| Ok(()));
        assert!(format!("{:#}", result.unwrap_err()).contains("CONTENT_DEADLINE"));
        assert!(began.elapsed() < Duration::from_millis(300));
        let cancelled = context(1000);
        cancelled.cancel.cancel();
        let result = journal.read_snapshot(&cancelled, |_| Ok(()));
        assert!(format!("{:#}", result.unwrap_err()).contains("CONTENT_CANCELLED"));
        held.pop();
        let value = journal
            .read_snapshot(&context(1000), |db| {
                Ok(db.query_row("SELECT 17", [], |r| r.get::<_, i64>(0))?)
            })
            .unwrap();
        assert_eq!(value, 17);
        assert_eq!(
            journal.readers.allocated.load(Ordering::Acquire),
            CONNECTIONS
        );
        assert!(!journal.connection_failed());
    }
    #[test]
    fn failed_and_panicking_read_snapshots_release_transactions_without_poisoning_the_writer() {
        let dir = tempfile::tempdir().unwrap();
        let journal = WriteJournal::open(dir.path()).unwrap();
        assert!(
            journal
                .read_snapshot(&context(1000), |db| {
                    db.execute("DELETE FROM inventory", [])?;
                    Ok(())
                })
                .is_err(),
            "read connections must not mutate the journal"
        );
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _: Result<()> = journal.read_snapshot(&context(1000), |db| {
                let _: i64 = db.query_row("SELECT count(*) FROM inventory", [], |r| r.get(0))?;
                panic!("read snapshot panic");
            });
        }));
        assert!(panic.is_err());
        assert!(!journal.connection_failed());
        journal
            .read_snapshot(&context(1000), |db| {
                inventory(db)?;
                Ok(())
            })
            .unwrap();
        assert!(journal.lock(&context(1000)).unwrap().is_autocommit());
        assert_eq!(journal.readers.allocated.load(Ordering::Acquire), 1);
    }
}
