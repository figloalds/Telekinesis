//! Regular metadata WAL maintenance without holding the foreground Store lock.
use super::cost::Counter;
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, ErrorCode};
use std::{
    path::Path,
    sync::{
        Arc, Condvar, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

static PROBE: Counter = Counter::new();
static PASSIVE: Counter = Counter::new();
static ERRORS: AtomicU64 = AtomicU64::new(0);
static BUSY: AtomicU64 = AtomicU64::new(0);

pub(crate) fn stats() -> Vec<serde_json::Value> {
    vec![
        PROBE.json("sqlite_checkpoint_probe"),
        PASSIVE.json("sqlite_background_checkpoint"),
        serde_json::json!({"stage":"sqlite_background_checkpoint_errors","calls":ERRORS.load(Ordering::Relaxed),"ms":0.0}),
        serde_json::json!({"stage":"sqlite_background_checkpoint_busy","calls":BUSY.load(Ordering::Relaxed),"ms":0.0}),
    ]
}

#[derive(Default)]
struct State {
    stopping: bool,
    error: Option<String>,
}
struct Shared {
    state: Mutex<State>,
    wake: Condvar,
    // SQLite FULL checkpoints do not wait for another active checkpointer.
    // Serialize the worker with explicit durability barriers on the writer.
    gate: Mutex<()>,
}
pub(crate) struct CheckpointWorker {
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
}
impl CheckpointWorker {
    pub fn start(path: &Path) -> Result<Self> {
        ensure!(
            rusqlite::version_number() >= 3_051_003,
            "SQLITE_WAL_RESET_FIX_REQUIRED"
        );
        Self::with_connection(Connection::open(path)?)
    }
    fn with_connection(db: Connection) -> Result<Self> {
        db.busy_timeout(Duration::ZERO)?;
        db.execute_batch("PRAGMA synchronous=NORMAL;")?;
        let mode: String = db.query_row("PRAGMA journal_mode", [], |r| r.get(0))?;
        ensure!(mode == "wal", "METADATA_WAL_REQUIRED");
        // Open the schema/WAL before moving this connection to its thread.
        db.query_row("SELECT count(*) FROM sqlite_schema", [], |r| {
            r.get::<_, i64>(0)
        })?;
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            wake: Condvar::new(),
            gate: Mutex::new(()),
        });
        let run = shared.clone();
        let thread = std::thread::Builder::new()
            .name("tkfs-checkpoint".into())
            .spawn(move || {
                let mut next_probe = Instant::now() + Duration::from_millis(50);
                loop {
                    let mut state = run.state.lock().unwrap();
                    while !state.stopping {
                        let pause = next_probe.saturating_duration_since(Instant::now());
                        if pause.is_zero() {
                            break;
                        }
                        state = run.wake.wait_timeout(state, pause).unwrap().0;
                    }
                    if state.stopping {
                        break;
                    }
                    drop(state);
                    let _gate = run.gate.lock().unwrap();
                    if let Err(error) = maintain(&db) {
                        let mut state = run.state.lock().unwrap();
                        if state.error.is_none() {
                            ERRORS.fetch_add(1, Ordering::Relaxed);
                            state.error = Some(format!("METADATA_CHECKPOINT_FAILED: {error:#}"));
                        }
                    }
                    // A stream of commits must not turn background maintenance
                    // into a checkpoint per commit while the writer keeps the
                    // WAL above the threshold. Coalesce work for another 50 ms.
                    next_probe = Instant::now() + Duration::from_millis(50);
                }
            })
            .context("START_METADATA_CHECKPOINT_WORKER")?;
        Ok(Self {
            shared,
            thread: Some(thread),
        })
    }
    /// Disable the writer's inline automatic checkpoint only after a working
    /// replacement thread/connection exists. The worker probes every 50 ms even
    /// without new commits, so retained readers and direct catalog commits do
    /// not leave a missed maintenance request indefinitely.
    pub fn replace_inline(&self, db: &Connection) -> Result<()> {
        self.check_error()?;
        db.execute_batch("PRAGMA wal_autocheckpoint=0;")?;
        self.request();
        Ok(())
    }
    pub fn request(&self) {
        self.shared.wake.notify_one();
    }
    pub fn barrier(&self) -> Result<MutexGuard<'_, ()>> {
        self.shared
            .gate
            .lock()
            .map_err(|_| anyhow::anyhow!("METADATA_CHECKPOINT_GATE_POISONED"))
    }
    pub fn check_error(&self) -> Result<()> {
        ensure!(
            self.thread
                .as_ref()
                .is_some_and(|thread| !thread.is_finished()),
            "METADATA_CHECKPOINT_WORKER_STOPPED"
        );
        if let Some(error) = &self.shared.state.lock().unwrap().error {
            anyhow::bail!("{error}");
        }
        Ok(())
    }
    #[cfg(test)]
    pub fn inject_error(&self, error: &str) {
        self.shared.state.lock().unwrap().error = Some(error.into());
    }
}
impl Drop for CheckpointWorker {
    fn drop(&mut self) {
        self.shared.state.lock().unwrap().stopping = true;
        self.shared.wake.notify_one();
        if let Some(thread) = self.thread.take() {
            // The owning Store outlives all foreground operations. All handles
            // here are to native backing storage, never the served namespace.
            if thread.join().is_err() {
                ERRORS.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}
fn maintain(db: &Connection) -> Result<()> {
    let (busy, frames, synced) = {
        let _cost = PROBE.time();
        match db.query_row("PRAGMA wal_checkpoint(NOOP)", [], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
            ))
        }) {
            Ok(state) => state,
            Err(error) if busy_error(&error) => {
                BUSY.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        }
    };
    if busy != 0 {
        BUSY.fetch_add(1, Ordering::Relaxed);
        return Ok(());
    }
    if frames < 1000 || frames == synced {
        return Ok(());
    }
    let _cost = PASSIVE.time();
    match db.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |r| r.get::<_, i64>(0)) {
        Ok(0) => Ok(()),
        Ok(_) => {
            BUSY.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        Err(error) if busy_error(&error) => {
            BUSY.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}
fn busy_error(error: &rusqlite::Error) -> bool {
    matches!(
        error.sqlite_error_code(),
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn database() -> Result<(tempfile::TempDir, Connection)> {
        let home = tempfile::tempdir()?;
        let db = Connection::open(home.path().join("metadata.sqlite"))?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; CREATE TABLE data(bytes BLOB); PRAGMA wal_checkpoint(TRUNCATE);")?;
        Ok((home, db))
    }
    fn state(db: &Connection) -> Result<(i64, i64)> {
        Ok(db.query_row("PRAGMA wal_checkpoint(NOOP)", [], |r| {
            Ok((r.get(1)?, r.get(2)?))
        })?)
    }
    fn wait_for(
        db: &Connection,
        worker: &CheckpointWorker,
        predicate: impl Fn((i64, i64)) -> bool,
    ) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(3);
        while !predicate(state(db)?) {
            worker.check_error()?;
            ensure!(
                Instant::now() < deadline,
                "checkpoint did not make progress"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        worker.check_error()
    }
    #[test]
    fn periodic_checkpoint_retries_after_reader_release_without_another_commit() -> Result<()> {
        let (home, db) = database()?;
        let worker = CheckpointWorker::start(&home.path().join("metadata.sqlite"))?;
        worker.replace_inline(&db)?;
        db.execute("INSERT INTO data VALUES(zeroblob(2097152))", [])?;
        std::thread::sleep(Duration::from_millis(120));
        let (frames, done) = state(&db)?;
        assert!(frames > 0 && frames < 1000);
        assert_eq!(done, 0, "preserve the 1000-frame maintenance threshold");
        let reader = Connection::open(home.path().join("metadata.sqlite"))?;
        reader.execute_batch("BEGIN;")?;
        assert_eq!(
            reader.query_row("SELECT count(*) FROM data", [], |r| r.get::<_, i64>(0))?,
            1
        );
        db.execute("INSERT INTO data VALUES(zeroblob(2097152))", [])?;
        wait_for(&db, &worker, |(frames, done)| {
            frames >= 1000 && done > 0 && done < frames
        })?;
        reader.execute_batch("ROLLBACK;")?;
        wait_for(&db, &worker, |(frames, done)| {
            frames >= 1000 && frames == done
        })?;
        assert_eq!(
            reader.query_row("SELECT sum(length(bytes)) FROM data", [], |r| r
                .get::<_, i64>(0))?,
            4194304
        );
        Ok(())
    }
    #[test]
    fn foreground_commit_does_not_wait_for_checkpoint_gate() -> Result<()> {
        let (home, db) = database()?;
        let worker = CheckpointWorker::start(&home.path().join("metadata.sqlite"))?;
        worker.replace_inline(&db)?;
        let gate = worker.barrier()?;
        let (send, receive) = std::sync::mpsc::channel();
        let writer = std::thread::spawn(move || {
            let result = db.execute("INSERT INTO data VALUES(zeroblob(4194304))", []);
            send.send((db, result)).unwrap();
        });
        let result = receive.recv_timeout(Duration::from_secs(2));
        drop(gate);
        writer.join().unwrap();
        let (db, result) = result.context("foreground commit waited for checkpoint gate")?;
        result?;
        worker.request();
        wait_for(&db, &worker, |(frames, done)| {
            frames >= 1000 && frames == done
        })?;
        Ok(())
    }
    #[test]
    fn real_checkpoint_write_failure_is_latched_and_reported() -> Result<()> {
        let (home, db) = database()?;
        db.execute_batch("PRAGMA wal_autocheckpoint=0;")?;
        db.execute("INSERT INTO data VALUES(zeroblob(4194304))", [])?;
        let read_only = Connection::open_with_flags(
            home.path().join("metadata.sqlite"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let worker = CheckpointWorker::with_connection(read_only)?;
        worker.request();
        let deadline = Instant::now() + Duration::from_secs(3);
        while worker.check_error().is_ok() {
            ensure!(
                Instant::now() < deadline,
                "read-only checkpoint failure was not reported"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            worker
                .check_error()
                .unwrap_err()
                .to_string()
                .contains("METADATA_CHECKPOINT_FAILED")
        );
        assert_eq!(
            db.query_row("SELECT sum(length(bytes)) FROM data", [], |r| r
                .get::<_, i64>(0))?,
            4194304
        );
        Ok(())
    }
    #[test]
    fn already_synced_large_wal_does_not_attempt_another_checkpoint() -> Result<()> {
        let (home, db) = database()?;
        db.execute_batch("PRAGMA wal_autocheckpoint=0;")?;
        db.execute("INSERT INTO data VALUES(zeroblob(4194304))", [])?;
        db.execute_batch("PRAGMA wal_checkpoint(PASSIVE);")?;
        let (frames, done) = state(&db)?;
        assert!(frames >= 1000 && frames == done);
        let read_only = Connection::open_with_flags(
            home.path().join("metadata.sqlite"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        read_only.query_row("SELECT count(*) FROM sqlite_schema", [], |r| {
            r.get::<_, i64>(0)
        })?;
        // A redundant checkpoint would attempt to write through this read-only
        // connection. A complete WAL needs only the non-mutating state probe.
        maintain(&read_only)?;
        Ok(())
    }
}
