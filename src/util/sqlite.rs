//! Reuse compiled statements on the metadata and pack-index hot paths.
use super::cost::Counter;
use rusqlite::{Connection, Params, Result, Row};
use std::sync::atomic::{AtomicU64, Ordering};

static EXECUTE: Counter = Counter::new();
static QUERY: Counter = Counter::new();
static BATCH: Counter = Counter::new();
static BATCH_PARTS: [Counter; 7] = [const { Counter::new() }; 7];
static COMMIT_MODES: [Counter; 3] = [const { Counter::new() }; 3];
static ROWS: Counter = Counter::new();
static AUTO_CHECKPOINT: Counter = Counter::new();
static AUTO_CHECKPOINT_ERRORS: AtomicU64 = AtomicU64::new(0);

/// Time the existing 1,000-frame PASSIVE automatic checkpoint, rather than
/// attributing all NORMAL COMMIT time to it. This mirrors the bundled SQLite's
/// sqlite3WalDefaultHook, including its non-failing COMMIT result on checkpoint
/// error. Explicit durability barriers still check their own checkpoint result.
pub(crate) fn profile_default_auto_checkpoint(db: &Connection) -> Result<()> {
    let threshold: i32 = db.query_row("PRAGMA wal_autocheckpoint", [], |r| r.get(0))?;
    if threshold != 1000 {
        // Do not silently replace a differently configured checkpoint policy.
        return Err(rusqlite::Error::InvalidQuery);
    }
    db.wal_hook(Some(profiled_auto_checkpoint));
    Ok(())
}

fn profiled_auto_checkpoint(wal: &rusqlite::hooks::Wal, frames: i32) -> Result<()> {
    if frames >= 1000 {
        let _cost = AUTO_CHECKPOINT.time();
        if wal.checkpoint().is_err() {
            AUTO_CHECKPOINT_ERRORS.fetch_add(1, Ordering::Relaxed);
        }
    }
    Ok(())
}
pub(crate) fn metadata_rows<T>(read: impl FnOnce() -> T) -> T {
    let _cost = ROWS.time();
    read()
}
pub(crate) fn metadata_stats() -> Vec<serde_json::Value> {
    let mut rows = vec![
        EXECUTE.json("sqlite_execute"),
        QUERY.json("sqlite_query"),
        BATCH.json("sqlite_batch"),
        ROWS.json("sqlite_rows"),
        AUTO_CHECKPOINT.json("sqlite_auto_checkpoint"),
        serde_json::json!({"stage":"sqlite_auto_checkpoint_errors",
            "calls":AUTO_CHECKPOINT_ERRORS.load(Ordering::Relaxed),"ms":0.0}),
    ];
    for (counter, name) in BATCH_PARTS.iter().zip([
        "sqlite_commit",
        "sqlite_begin",
        "sqlite_savepoint",
        "sqlite_release",
        "sqlite_sync_mode",
        "sqlite_rollback",
        "sqlite_other_batch",
    ]) {
        rows.push(counter.json(name));
    }
    for (counter, name) in COMMIT_MODES.iter().zip([
        "sqlite_commit_normal",
        "sqlite_commit_full",
        "sqlite_commit_other_mode",
    ]) {
        rows.push(counter.json(name));
    }
    rows.extend(super::checkpoint::stats());
    rows
}

pub(crate) trait CachedConnection {
    fn execute_cached<P: Params>(&self, sql: &str, params: P) -> Result<usize>;
    fn query_cached<T, P, F>(&self, sql: &str, params: P, map: F) -> Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> Result<T>;
    fn execute_profiled<P: Params>(&self, sql: &str, params: P) -> Result<usize> {
        let _cost = EXECUTE.time();
        self.execute_cached(sql, params)
    }
    fn query_profiled<T, P, F>(&self, sql: &str, params: P, map: F) -> Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> Result<T>,
    {
        let _cost = QUERY.time();
        self.query_cached(sql, params, map)
    }
    fn batch_profiled(&self, sql: &str) -> Result<()>;
}

impl CachedConnection for Connection {
    fn batch_profiled(&self, sql: &str) -> Result<()> {
        let _cost = BATCH.time();
        // Nested in sqlite_batch. Distinguish COMMIT/checkpoint latency from
        // savepoint setup and durability-mode changes before changing policy.
        let part = match sql.trim_start() {
            "COMMIT;" => 0,
            "BEGIN IMMEDIATE;" => 1,
            "SAVEPOINT apply_events;" => 2,
            "RELEASE apply_events;" => 3,
            s if s.starts_with("PRAGMA synchronous=") => 4,
            s if s.starts_with("ROLLBACK") => 5,
            _ => 6,
        };
        let _part = BATCH_PARTS[part].time();
        let _mode = (part == 0).then(|| {
            // A read-only connection setting query; never change synchronous
            // policy for profiling. Failure is visible as other/unknown mode
            // and must not suppress the real COMMIT or its durability error.
            let mode = match self.query_cached("PRAGMA synchronous", [], |r| r.get::<_, i32>(0)) {
                Ok(1) => 0,
                Ok(2) => 1,
                _ => 2,
            };
            COMMIT_MODES[mode].time()
        });
        self.execute_batch(sql)
    }
    fn execute_cached<P: Params>(&self, sql: &str, params: P) -> Result<usize> {
        self.prepare_cached(sql)?.execute(params)
    }

    fn query_cached<T, P, F>(&self, sql: &str, params: P, map: F) -> Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> Result<T>,
    {
        self.prepare_cached(sql)?.query_row(params, map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profiled_checkpoint_preserves_default_threshold_and_retained_reader() -> anyhow::Result<()> {
        for profiled in [false, true] {
            let home = tempfile::tempdir()?;
            let path = home.path().join("metadata.sqlite");
            let db = Connection::open(&path)?;
            db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; CREATE TABLE data(bytes BLOB); PRAGMA wal_checkpoint(TRUNCATE);")?;
            if profiled {
                profile_default_auto_checkpoint(&db)?;
            }
            let state = || -> Result<(i64, i64)> {
                db.query_row("PRAGMA wal_checkpoint(NOOP)", [], |r| {
                    Ok((r.get(1)?, r.get(2)?))
                })
            };
            db.execute("INSERT INTO data VALUES(zeroblob(2097152))", [])?;
            let (frames, done) = state()?;
            assert!(frames > 0 && frames < 1000);
            assert_eq!(done, 0, "no early checkpoint: profiled={profiled}");
            let reader = Connection::open(&path)?;
            reader.execute_batch("BEGIN;")?;
            assert_eq!(
                reader.query_row("SELECT count(*) FROM data", [], |r| r.get::<_, i64>(0))?,
                1
            );
            db.execute("INSERT INTO data VALUES(zeroblob(2097152))", [])?;
            let (frames, done) = state()?;
            assert!(
                frames >= 1000 && done < frames,
                "reader limits checkpoint: profiled={profiled}"
            );
            reader.execute_batch("ROLLBACK;")?;
            db.execute("INSERT INTO data VALUES(zeroblob(1))", [])?;
            let (frames, done) = state()?;
            assert_eq!(
                done, frames,
                "next commit completes checkpoint: profiled={profiled}"
            );
            assert_eq!(
                reader.query_row("SELECT sum(length(bytes)) FROM data", [], |r| r
                    .get::<_, i64>(0))?,
                4194305
            );
        }
        Ok(())
    }

    #[test]
    fn checkpoint_profiler_refuses_a_different_policy() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("PRAGMA wal_autocheckpoint=7;")?;
        assert!(profile_default_auto_checkpoint(&db).is_err());
        assert_eq!(
            db.query_row("PRAGMA wal_autocheckpoint", [], |r| r.get::<_, i32>(0))?,
            7
        );
        Ok(())
    }
}
