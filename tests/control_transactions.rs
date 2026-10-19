//! WP0.INT (CCR-WP0.INT-2): a journaled control request (`Engine::control`)
//! takes SQLite's writer slot before it reads.
//!
//! Before the fix the request ran in a deferred `SAVEPOINT`: it read first and
//! upgraded to a writer on its first write. When another connection to the
//! same WAL database (the bulk transfer view, a pooled secondary store) held
//! the writer slot or had committed since that read, the upgrade failed at
//! once with `SQLITE_BUSY` (5) or `SQLITE_BUSY_SNAPSHOT` (517) and the busy
//! handler never ran: a user's `import`/`mkdir` failed spuriously during bulk
//! transfer (the `three_orchestrators_*` flake in `tests/orchestrator.rs`).
use rusqlite::Connection;
use serde_json::{Value, json};
use std::{
    path::Path,
    thread,
    time::{Duration, Instant},
};
use tkfs::{core::Store, runtime::Engine};

fn engine(root: &Path) -> Engine {
    Engine::new(Store::open(root, Some("control-transactions")).unwrap())
}

fn import(path: &str, bytes: &[u8]) -> Value {
    json!({"op":"import","path":path,"hex":hex::encode(bytes)})
}

fn content(e: &Engine, path: &str) -> Vec<u8> {
    let entry = e.store.lookup(path).unwrap();
    e.store.read_object(&entry.content.unwrap()).unwrap()
}

/// Another connection holds the writer slot while a request that reads before
/// it writes (`import` of an existing path: `lookup`, then the revision) runs.
/// The request now waits for the slot (`busy_timeout`) instead of failing.
#[test]
fn control_request_waits_for_another_writer_instead_of_failing() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = engine(dir.path());
    e.control("first", &import("code", b"one")).unwrap();
    let other = Connection::open(dir.path().join("metadata.sqlite")).unwrap();
    other
        .execute_batch("CREATE TABLE wp0_int_probe(x INTEGER NOT NULL)")
        .unwrap();
    other
        .execute_batch("BEGIN IMMEDIATE; INSERT INTO wp0_int_probe VALUES(1)")
        .unwrap();
    let hold = Duration::from_millis(300);
    let writer = thread::spawn(move || {
        thread::sleep(hold);
        other.execute_batch("COMMIT").unwrap();
        other
    });
    let started = Instant::now();
    let result = e.control("second", &import("code", b"two"));
    let waited = started.elapsed();
    let other = writer.join().unwrap();
    let result = result.expect("the request waits for the writer slot");
    assert!(result["revision"].is_string(), "{result}");
    assert!(
        waited >= hold / 2,
        "the writer slot was contended: {waited:?}"
    );
    assert_eq!(content(&e, "code"), b"two");
    assert!(e.store.db.is_autocommit());
    // The other connection's commit is intact, and the request is durable.
    let probes: i64 = other
        .query_row("SELECT count(*) FROM wp0_int_probe", [], |r| r.get(0))
        .unwrap();
    assert_eq!(probes, 1);
    drop(e);
    let e = engine(dir.path());
    assert_eq!(content(&e, "code"), b"two");
    assert!(
        e.store
            .receipt("second", &import("code", b"two"))
            .unwrap()
            .is_some()
    );
}

/// A commit by another connection between the request's reads and its writes
/// is not a failure either: the request reads only after it holds the slot.
#[test]
fn control_requests_interleaved_with_foreign_commits_never_fail_busy() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = engine(dir.path());
    e.control("seed", &import("code", b"seed")).unwrap();
    let other = Connection::open(dir.path().join("metadata.sqlite")).unwrap();
    other.busy_timeout(Duration::from_secs(5)).unwrap();
    other
        .execute_batch("CREATE TABLE wp0_int_probe(x INTEGER NOT NULL)")
        .unwrap();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let stop = stop.clone();
        thread::spawn(move || {
            let mut commits = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                other
                    .execute("INSERT INTO wp0_int_probe VALUES(?)", [commits as i64])
                    .unwrap();
                commits += 1;
                // Leave the slot free most of the time, as bulk bookkeeping does.
                thread::sleep(Duration::from_millis(2));
            }
            commits
        })
    };
    let mut failures = vec![];
    for n in 0..60 {
        let request = format!("r{n}");
        let payload = if n % 2 == 0 {
            import("code", format!("v{n}").as_bytes())
        } else {
            json!({"op":"mkdir","path":format!("dir{n}")})
        };
        if let Err(error) = e.control(&request, &payload) {
            failures.push(format!("{request}: {error:#}"));
        }
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let commits = writer.join().unwrap();
    assert!(failures.is_empty(), "{failures:#?}");
    assert!(commits > 0);
    assert_eq!(content(&e, "code"), b"v58");
    assert!(e.store.db.is_autocommit());
}

/// Inside a caller's transaction the request opens only its savepoint, so the
/// caller still decides: rolling back discards the mutation and its receipt.
#[test]
fn control_request_inside_a_caller_transaction_leaves_it_to_the_caller() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = engine(dir.path());
    e.store.db.execute_batch("BEGIN").unwrap();
    let payload = json!({"op":"mkdir","path":"nested"});
    e.control("nested", &payload).unwrap();
    assert!(
        !e.store.db.is_autocommit(),
        "the caller's transaction is open"
    );
    assert!(e.store.lookup("nested").is_ok());
    e.store.db.execute_batch("ROLLBACK").unwrap();
    assert!(e.store.lookup("nested").is_err());
    assert!(e.store.receipt("nested", &payload).unwrap().is_none());
}

/// A failed request leaves no transaction open and writes no receipt; the next
/// request on the same engine succeeds.
#[test]
fn failed_control_request_rolls_back_and_releases_the_writer_slot() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = engine(dir.path());
    let error = e.control("bad", &json!({"op":"no-such-op"})).unwrap_err();
    assert!(error.to_string().contains("UNKNOWN_OPERATION"), "{error:#}");
    assert!(e.store.db.is_autocommit());
    assert!(
        e.store
            .receipt("bad", &json!({"op":"no-such-op"}))
            .unwrap()
            .is_none()
    );
    // Renaming a missing path fails inside the request, after it began.
    let missing = json!({"op":"rename","path":"missing","target":"elsewhere"});
    assert!(e.control("missing", &missing).is_err());
    assert!(e.store.db.is_autocommit());
    // Another connection can take the writer slot at once.
    let other = Connection::open(dir.path().join("metadata.sqlite")).unwrap();
    other.busy_timeout(Duration::ZERO).unwrap();
    other.execute_batch("BEGIN IMMEDIATE; ROLLBACK").unwrap();
    e.control("good", &json!({"op":"mkdir","path":"after"}))
        .unwrap();
    assert!(e.store.lookup("after").is_ok());
}
