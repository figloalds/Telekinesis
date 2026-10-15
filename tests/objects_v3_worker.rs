//! WP3.2: objects v3, worker side (REVAMP_PLAN §6.4, §14 WP3.2).
//!
//! The store API (`want_objects`, `read_object_range`, `write_object_items`,
//! `report_corrupt`), the worker ops with leases (`objects_rpc::dispatch` on an
//! in-process `WorkerCtx`), loopback sessions on the worker listener, and the
//! crash windows of invariants 1–3 on real worker processes (`TKFS_FAULT`
//! exits the worker at a named point; a kill -9 mid-object).
//!
//! Invariants tested literally:
//! - 1, a whole-object SHA-256 before install: `corrupt_spool_never_installed`,
//!   `tampered_spool_never_installed`;
//! - 2, durable offsets advertised only after fsync and commit:
//!   `crash_mid_object_resumes_from_durable`,
//!   `kill_mid_object_resumes_from_durable`, `startup_truncates_spools_to_durable`;
//! - 3, a crash between install and registration is repaired by re-transfer:
//!   `crash_between_install_and_register_retransfers`;
//! - 13, never serve private bytes: `hash_only_request_not_permitted`,
//!   `staged_only_objects_are_wanted_received_but_not_served`;
//! - 19, one owner per object per session: `duplicate_sessions_transfer_once`,
//!   `mutual_pairing_transfers_once_and_leases_expire`, `want_lists_shard_by_lane`.
mod support;

use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    fs,
    path::Path,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant, UNIX_EPOCH},
};
use support::*;
use tkfs::{
    core::{
        Bundle, ObjectLocks, Store,
        inbox::StagedEvent,
        objects::{AVAILABLE, CORRUPT, STAMP_SECS},
        transfer::{ObjectOutcome, ReportAction, WantPriority, WriteItem, WriteResult},
    },
    model::protocol::{LaneSpec, objects::PULL_BUDGET_BYTES},
    runtime::{self, Engine, objects_rpc},
    session::{self, Conn, ConnOptions, Priority},
};

const MIB: u64 = 1024 * 1024;

fn lane() -> LaneSpec {
    LaneSpec::new(0, 1)
}

fn no_leases(_: &str) -> bool {
    false
}

/// A file at `path` with `bytes` (one create and one write event).
fn file(store: &mut Store, path: &str, bytes: &[u8]) -> String {
    let entry = store.create(path, "file").unwrap();
    store
        .write_revision(&entry.id, entry.revisions.get("content").cloned(), bytes)
        .unwrap();
    tkfs::core::hash(bytes)
}

/// Overwrite `path` with `bytes` (its previous content becomes history).
fn overwrite(store: &mut Store, path: &str, bytes: &[u8]) -> String {
    let entry = store.lookup(path).unwrap();
    store
        .write_revision(&entry.id, entry.revisions.get("content").cloned(), bytes)
        .unwrap();
    tkfs::core::hash(bytes)
}

/// A (author, holding every object) and B (every event committed, no object
/// bytes), as `support::metadata_replica` builds them.
struct Pair {
    dir: tempfile::TempDir,
    a: Store,
    b: Store,
}

fn pair(author: impl FnOnce(&mut Store)) -> Pair {
    let dir = tempfile::tempdir().unwrap();
    let repo = tkfs::core::id();
    let mut a = Store::initialize(&dir.path().join("a"), &repo, &tkfs::core::id()).unwrap();
    author(&mut a);
    let (a, b) = metadata_replica(a, &dir.path().join("b")).unwrap();
    // Reopening the copy without its objects queued each one as missing
    // (WP1.8's startup check); a receiver of the metadata alone has no demand.
    b.db.execute("DELETE FROM object_requests", []).unwrap();
    Pair { dir, a, b }
}

fn wants(store: &Store) -> Vec<(String, WantPriority, u64)> {
    store
        .want_objects(lane(), 256, PULL_BUDGET_BYTES, &no_leases)
        .unwrap()
        .into_iter()
        .map(|item| (item.hash, item.priority, item.offset))
        .collect()
}

fn item(hash: &str, offset: u64, total: u64, len: u64) -> WriteItem {
    WriteItem {
        hash: hash.to_owned(),
        offset,
        total,
        len,
        last: offset + len == total,
    }
}

/// Copy `hash` from `from` to `to` in `chunk`-byte writes, from `to`'s want
/// offset; returns the last result and the bytes read from `from`.
fn transfer(
    from: &Store,
    to: &Store,
    locks: &ObjectLocks,
    hash: &str,
    chunk: u64,
) -> (WriteResult, u64) {
    let mut offset = to
        .want_objects(lane(), 256, u64::MAX, &no_leases)
        .unwrap()
        .into_iter()
        .find(|item| item.hash == hash)
        .map_or(0, |item| item.offset);
    let mut read = 0;
    loop {
        let (total, bytes) = from
            .read_object_range(hash, offset, chunk as usize)
            .unwrap();
        read += bytes.len() as u64;
        let result = to
            .write_object_items(
                &[item(hash, offset, total, bytes.len() as u64)],
                &bytes,
                locks,
            )
            .unwrap()
            .remove(0);
        match result.outcome {
            ObjectOutcome::Progressed { durable_offset } => offset = durable_offset,
            _ => return (result, read),
        }
    }
}

fn received_shared(store: &Store, hash: &str) -> bool {
    store
        .db
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM received_shared_objects WHERE hash=?)",
            [hash],
            |r| r.get(0),
        )
        .unwrap()
}

fn availability(store: &Store, hash: &str) -> Option<i64> {
    store
        .db
        .query_row(
            "SELECT state FROM object_availability WHERE hash=?",
            [hash],
            |r| r.get(0),
        )
        .ok()
}

fn spool_len(root: &Path, hash: &str) -> Option<u64> {
    fs::metadata(root.join("incoming-objects").join(hash))
        .ok()
        .map(|m| m.len())
}

/// The committed durable offset of `hash`'s spool, read from the database
/// file directly (works on a stopped or crashed worker's state).
fn durable_row(root: &Path, hash: &str) -> Option<u64> {
    let db = rusqlite::Connection::open(root.join("metadata.sqlite")).unwrap();
    db.query_row(
        "SELECT durable FROM spool_state WHERE hash=?",
        [hash],
        |r| r.get::<_, i64>(0),
    )
    .ok()
    .map(|d| d as u64)
}

// ---------------------------------------------------------------------------
// Want lists

#[test]
fn want_ordering_is_demand_then_current_then_history_smaller_first() {
    let mut history = BTreeSet::new();
    let mut current = BTreeSet::new();
    let p = pair(|a| {
        // History: the first versions of two files (overwritten below).
        history.insert(file(a, "h-small", &[1; 10]));
        history.insert(file(a, "h-big", &[2; 3000]));
        history.insert(file(a, "demanded", &[3; 4000]));
        current.insert(overwrite(a, "h-small", &[4; 5]));
        current.insert(overwrite(a, "h-big", &[5; 7]));
        current.insert(overwrite(a, "demanded", &[6; 9]));
        current.insert(file(a, "c-big", &[7; 5000]));
        current.insert(file(a, "c-small", &[8; 100]));
    });
    let hash = |bytes: &[u8]| tkfs::core::hash(bytes);
    // Demand: a reader asked for two history objects; the newest first.
    p.b.request_object(&hash(&[2; 3000])).unwrap();
    thread::sleep(Duration::from_millis(20));
    p.b.request_object(&hash(&[3; 4000])).unwrap();
    let list = wants(&p.b);
    let order: Vec<_> = list.iter().map(|(h, prio, _)| (h.clone(), *prio)).collect();
    assert_eq!(
        order,
        vec![
            (hash(&[3; 4000]), WantPriority::Demand),
            (hash(&[2; 3000]), WantPriority::Demand),
            (hash(&[4; 5]), WantPriority::Current),
            (hash(&[5; 7]), WantPriority::Current),
            (hash(&[6; 9]), WantPriority::Current),
            (hash(&[8; 100]), WantPriority::Current),
            (hash(&[7; 5000]), WantPriority::Current),
            (hash(&[1; 10]), WantPriority::History),
        ],
        "history {history:?} current {current:?}"
    );
    // Sizes come from the committed descriptors; nothing is spooled yet.
    let items = p.b.want_objects(lane(), 256, u64::MAX, &no_leases).unwrap();
    assert!(items.iter().all(|i| i.offset == 0));
    assert_eq!(
        items
            .iter()
            .find(|i| i.hash == hash(&[7; 5000]))
            .unwrap()
            .size,
        5000
    );
    // A list is bounded by items and by bytes (the crossing item ends it).
    let two = p.b.want_objects(lane(), 2, u64::MAX, &no_leases).unwrap();
    assert_eq!(two.len(), 2);
    let small = p.b.want_objects(lane(), 256, 5000, &no_leases).unwrap();
    assert_eq!(
        small.iter().map(|i| i.size).collect::<Vec<_>>(),
        vec![4000, 3000],
        "4000 bytes, then the item that crosses 5000 ends the list"
    );
    // Objects in a transfer backoff wait; a local quarantine never delays the
    // download that repairs it.
    let backed = hash(&[8; 100]);
    p.b.record_object_failure(
        &backed,
        &tkfs::model::errors::ErrorWire::coded("OBJECT_IO_ERROR", "disk"),
    )
    .unwrap();
    assert!(!wants(&p.b).iter().any(|(h, _, _)| *h == backed));
    p.b.db
        .execute(
            "UPDATE object_error_state SET code='CORRUPT_OBJECT' WHERE hash=?",
            [&backed],
        )
        .unwrap();
    assert!(wants(&p.b).iter().any(|(h, _, _)| *h == backed));
    // Objects B holds and may share are never wanted.
    let locks = ObjectLocks::default();
    let (done, _) = transfer(&p.a, &p.b, &locks, &hash(&[8; 100]), MIB);
    assert_eq!(done.outcome, ObjectOutcome::Completed);
    assert!(!wants(&p.b).iter().any(|(h, _, _)| *h == backed));
    drop(p.dir);
}

#[test]
fn want_lists_interleave_a_large_object_fairly() {
    let p = pair(|a| {
        for i in 0..20u8 {
            file(a, &format!("tiny-{i}"), &[i; 64]);
        }
        file(a, "large", &vec![9; 200_000]);
    });
    let large = tkfs::core::hash(&vec![9; 200_000]);
    // Five slots: four tiny objects, then the fair slot carries the large one.
    let list = p.b.want_objects(lane(), 5, u64::MAX, &no_leases).unwrap();
    assert_eq!(list.len(), 5);
    assert!(list[..4].iter().all(|i| i.size == 64), "{list:?}");
    assert_eq!(list[4].hash, large, "{list:?}");
    // With a byte budget: tiny objects up to three quarters, then the large one.
    let list = p.b.want_objects(lane(), 256, 1000, &no_leases).unwrap();
    assert_eq!(list.last().unwrap().hash, large, "{list:?}");
    assert_eq!(
        list.len(),
        13,
        "12 × 64 B ≥ 750 B, then the fair slot: {list:?}"
    );
    // A single slot is never the fair one: smaller first.
    let one = p.b.want_objects(lane(), 1, u64::MAX, &no_leases).unwrap();
    assert_eq!(one[0].size, 64);
}

#[test]
fn want_lists_shard_by_lane() {
    let p = pair(|a| {
        small_objects_in(a, "s", 64, 32, 5).unwrap();
    });
    let all: BTreeSet<String> =
        p.b.want_objects(LaneSpec::new(0, 1), 256, u64::MAX, &no_leases)
            .unwrap()
            .into_iter()
            .map(|i| i.hash)
            .collect();
    assert_eq!(all.len(), 64);
    let mut union = BTreeSet::new();
    for index in 0..3 {
        let lane = LaneSpec::new(index, 3);
        for item in p.b.want_objects(lane, 256, u64::MAX, &no_leases).unwrap() {
            assert!(lane.owns(&item.hash));
            assert!(union.insert(item.hash), "two lanes own one object");
        }
    }
    assert_eq!(union, all);
    // An invalid lane wants nothing.
    assert!(
        p.b.want_objects(LaneSpec::new(3, 3), 256, u64::MAX, &no_leases)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn staged_only_objects_are_wanted_received_but_not_served() {
    let dir = tempfile::tempdir().unwrap();
    let repo = tkfs::core::id();
    let mut a = Store::initialize(&dir.path().join("a"), &repo, &tkfs::core::id()).unwrap();
    let staged = file(&mut a, "staged.bin", &vec![42; 300_000]);
    let b = Store::initialize(&dir.path().join("b"), &repo, &tkfs::core::id()).unwrap();
    // B stages A's events; nothing activates them (activation paused).
    let mut events = a.events().unwrap();
    causal_sort(&mut events);
    let allowed = BTreeSet::from([a.device.clone()]);
    let outcome = b
        .stage_incoming(
            events
                .into_iter()
                .map(|event| StagedEvent {
                    event,
                    source: None,
                })
                .collect(),
            &allowed,
        )
        .unwrap();
    assert!(!outcome.staged.is_empty());
    assert!(b.lookup("staged.bin").is_err(), "not activated");
    // Wanted with the size the staged event claims, before any activation.
    let list = b.want_objects(lane(), 256, u64::MAX, &no_leases).unwrap();
    let want = list
        .iter()
        .find(|i| i.hash == staged)
        .expect("staged object wanted");
    assert_eq!(want.size, 300_000);
    assert_eq!(want.priority, WantPriority::History);
    // A claim other than the staged size is refused.
    let locks = ObjectLocks::default();
    let (_, head) = a.read_object_range(&staged, 0, 1000).unwrap();
    let bad = b
        .write_object_items(&[item(&staged, 0, 299_999, 1000)], &head, &locks)
        .unwrap();
    assert_eq!(bad[0].outcome, ObjectOutcome::SizeMismatch, "{bad:?}");
    assert_eq!(spool_len(&b.root, &staged), None);
    let (done, _) = transfer(&a, &b, &locks, &staged, 128 * 1024);
    assert_eq!(done.outcome, ObjectOutcome::Completed);
    assert!(received_shared(&b, &staged));
    assert!(
        !b.want_objects(lane(), 256, u64::MAX, &no_leases)
            .unwrap()
            .iter()
            .any(|i| i.hash == staged)
    );
    // Received, but not served: no committed shared event references it yet.
    let error = b.read_object_range(&staged, 0, 16).unwrap_err();
    assert_eq!(error.code, "NOT_PERMITTED");
}

// ---------------------------------------------------------------------------
// Authorization (invariant 13)

#[test]
fn hash_only_request_not_permitted() {
    let dir = tempfile::tempdir().unwrap();
    let mut s =
        Store::initialize(&dir.path().join("s"), &tkfs::core::id(), &tkfs::core::id()).unwrap();
    let private = s.fork("private", false).unwrap();
    s.checkout(&private.id).unwrap();
    let secret = file(&mut s, "secret", b"PRIVATE-KNOWN-HASH-CANARY");
    s.checkout("main").unwrap();
    let shared = file(&mut s, "public", b"shared bytes");
    // A paired peer commits a shared event that names the private hash.
    let main = s.branch("main").unwrap();
    let change = |field: &str, value: Value| tkfs::core::Change {
        entity: "attack".into(),
        field: field.into(),
        parents: vec![],
        value,
    };
    let mut event = s.event(
        &main,
        vec![
            change("kind", json!("file")),
            change("alive", json!(true)),
            change(
                "location",
                json!({"parent": tkfs::core::ROOT, "name": "stolen"}),
            ),
            change("content", json!(secret)),
        ],
    );
    event.device = "paired-peer".into();
    s.receive(
        Bundle {
            repo: s.repo.clone(),
            branches: vec![main],
            events: vec![event],
            objects: Default::default(),
        },
        &BTreeSet::from(["paired-peer".into()]),
    )
    .unwrap();
    assert!(s.lookup("stolen").is_ok());
    // Referenced by a committed shared event and physically present, but
    // private: a hash alone grants nothing.
    let error = s.read_object_range(&secret, 0, 1024).unwrap_err();
    assert_eq!(error.code, "NOT_PERMITTED", "{error}");
    // Unknown hashes, upper-case aliases and junk are refused too.
    let unknown = tkfs::core::hash(b"never stored");
    assert_eq!(
        s.read_object_range(&unknown, 0, 1).unwrap_err().code,
        "NOT_PERMITTED"
    );
    assert_eq!(
        s.read_object_range(&shared.to_uppercase(), 0, 1)
            .unwrap_err()
            .code,
        "INVALID_OBJECT_ID"
    );
    assert_eq!(
        s.read_object_range("../metadata.sqlite", 0, 1)
            .unwrap_err()
            .code,
        "INVALID_OBJECT_ID"
    );
    // A report cannot make us hash (or quarantine) a private object either.
    assert_eq!(
        tkfs::model::errors::code_of(&s.report_corrupt(&secret).unwrap_err()),
        "NOT_PERMITTED"
    );
    // The shared object is served.
    let (total, bytes) = s.read_object_range(&shared, 0, 1024).unwrap();
    assert_eq!((total, bytes.as_slice()), (12, &b"shared bytes"[..]));
    // A private copy is wanted (only a verified transfer makes it shareable, I4).
    assert!(wants(&s).iter().any(|(h, _, _)| *h == secret));

    // The same refusals through a worker's loopback session, which stays usable
    // after each (invariant 17).
    let engine: runtime::Shared = Arc::new(Mutex::new(Engine::new(s)));
    let info = runtime::start_rpc(engine.clone(), &dir.path().join("s"), None).unwrap();
    let (repo, device) = {
        let e = engine.lock().unwrap();
        (e.store.repo.clone(), e.store.device.clone())
    };
    let conn = session::dial_loopback(&info.address, None, ConnOptions::default()).unwrap();
    let route = json!({"peer": "p", "repo": repo, "local_replica": device, "remote_replica": "r"});
    for hash in [&secret, &unknown] {
        let error = conn
            .call(
                "objects.read",
                json!({"route": route, "hash": hash, "offset": 0, "len": 1024}),
                vec![],
                Priority::Control,
                Duration::from_secs(10),
            )
            .unwrap_err();
        assert_eq!(error.code(), "NOT_PERMITTED", "{error:?}");
    }
    let reply = conn
        .call(
            "objects.read",
            json!({"route": route, "hash": shared, "offset": 7, "len": 1024}),
            vec![],
            Priority::Control,
            Duration::from_secs(10),
        )
        .unwrap();
    assert_eq!(reply.header["total"], 12);
    assert_eq!(reply.body, b"bytes");
    engine
        .lock()
        .unwrap()
        .stopping
        .store(true, std::sync::atomic::Ordering::Relaxed);
}

#[test]
fn unsolicited_write_rejected() {
    let p = pair(|a| {
        file(a, "f", b"referenced");
    });
    let locks = ObjectLocks::default();
    // Bytes that hash correctly, but no shared event references them.
    let bytes = b"not referenced by anything".to_vec();
    let hash = tkfs::core::hash(&bytes);
    let results =
        p.b.write_object_items(
            &[item(&hash, 0, bytes.len() as u64, bytes.len() as u64)],
            &bytes,
            &locks,
        )
        .unwrap();
    assert_eq!(results[0].outcome, ObjectOutcome::Unsolicited);
    assert!(!p.b.root.join("objects").join(&hash).exists());
    assert_eq!(spool_len(&p.b.root, &hash), None);
    assert!(!received_shared(&p.b, &hash));
    // Malformed items fail alone; a body that disagrees with the items fails
    // the call.
    let wanted = tkfs::core::hash(b"referenced");
    let mixed =
        p.b.write_object_items(
            &[
                WriteItem {
                    hash: wanted.clone(),
                    offset: 0,
                    total: 10,
                    len: 10,
                    last: false,
                },
                item(&wanted, 0, 10, 10),
            ],
            b"referencedreferenced",
            &locks,
        )
        .unwrap();
    assert!(
        matches!(&mixed[0].outcome, ObjectOutcome::Failed(e) if e.code == "INVALID_OBJECT_PART")
    );
    assert_eq!(mixed[1].outcome, ObjectOutcome::Completed);
    assert!(
        p.b.write_object_items(&[item(&wanted, 0, 10, 10)], b"short", &locks)
            .is_err()
    );
    // Already shareable and available: skipped, nothing rewritten.
    let again =
        p.b.write_object_items(&[item(&wanted, 0, 10, 10)], b"referenced", &locks)
            .unwrap();
    assert_eq!(again[0].outcome, ObjectOutcome::Skipped);
}

#[test]
fn private_present_object_is_retransferred_verified_then_registered() {
    let shared_bytes = b"bytes both replicas know".to_vec();
    let mut p = pair(|a| {
        file(a, "shared", &shared_bytes);
    });
    let hash = tkfs::core::hash(&shared_bytes);
    // B holds the same bytes privately (authored on a private branch).
    let private = p.b.fork("mine", false).unwrap();
    p.b.checkout(&private.id).unwrap();
    file(&mut p.b, "copy", &shared_bytes);
    p.b.checkout("main").unwrap();
    assert!(p.b.object_available(&hash));
    assert_eq!(
        p.b.read_object_range(&hash, 0, 8).unwrap_err().code,
        "NOT_PERMITTED"
    );
    assert!(
        wants(&p.b).iter().any(|(h, _, _)| *h == hash),
        "wanted (I4)"
    );
    let locks = ObjectLocks::default();
    // Wrong bytes never replace or register the private copy.
    let wrong = vec![0u8; shared_bytes.len()];
    let bad =
        p.b.write_object_items(
            &[item(&hash, 0, wrong.len() as u64, wrong.len() as u64)],
            &wrong,
            &locks,
        )
        .unwrap();
    assert_eq!(bad[0].outcome, ObjectOutcome::CorruptAtSource);
    assert_eq!(
        fs::read(p.b.root.join("objects").join(&hash)).unwrap(),
        shared_bytes
    );
    assert!(!received_shared(&p.b, &hash));
    // A full verified transfer registers it as received_shared.
    let (done, _) = transfer(&p.a, &p.b, &locks, &hash, MIB);
    assert_eq!(done.outcome, ObjectOutcome::Completed);
    assert!(received_shared(&p.b, &hash));
    assert_eq!(p.b.read_object_range(&hash, 0, 64).unwrap().1, shared_bytes);
}

// ---------------------------------------------------------------------------
// Durability and verification (invariants 1–3), in process

#[test]
fn corrupt_spool_never_installed() {
    let p = pair(|a| {
        large_object_in(a, "big.bin", 3 * MIB + 17, 3).unwrap();
    });
    let hash = p.a.lookup("big.bin").unwrap().content.unwrap();
    let locks = ObjectLocks::default();
    let total = 3 * MIB + 17;
    let mut offset = 0;
    let mut last = None;
    while offset < total {
        let (_, mut bytes) = p.a.read_object_range(&hash, offset, MIB as usize).unwrap();
        if offset + bytes.len() as u64 == total {
            bytes[0] ^= 0xff; // the source sends one wrong byte in its last part
        }
        let result =
            p.b.write_object_items(
                &[item(&hash, offset, total, bytes.len() as u64)],
                &bytes,
                &locks,
            )
            .unwrap()
            .remove(0);
        match result.outcome {
            ObjectOutcome::Progressed { durable_offset } => offset = durable_offset,
            _ => {
                last = Some(result);
                break;
            }
        }
    }
    let last = last.unwrap();
    assert_eq!(last.outcome, ObjectOutcome::CorruptAtSource);
    assert_eq!(last.durable_offset, 0);
    // Never installed, never registered; the spool and its row are gone.
    assert!(!p.b.root.join("objects").join(&hash).exists());
    assert!(!received_shared(&p.b, &hash));
    assert_ne!(availability(&p.b, &hash), Some(AVAILABLE));
    assert_eq!(spool_len(&p.b.root, &hash), None);
    assert_eq!(p.b.spool_state(&hash).unwrap(), None);
    // Still wanted, from the start.
    assert!(wants(&p.b).iter().any(|(h, _, o)| *h == hash && *o == 0));
    // A good transfer then completes.
    let (done, _) = transfer(&p.a, &p.b, &locks, &hash, MIB);
    assert_eq!(done.outcome, ObjectOutcome::Completed);
    assert!(received_shared(&p.b, &hash));
}

#[test]
fn tampered_spool_never_installed() {
    let p = pair(|a| {
        large_object_in(a, "big.bin", 2 * MIB + 5, 4).unwrap();
    });
    let hash = p.a.lookup("big.bin").unwrap().content.unwrap();
    let locks = ObjectLocks::default();
    let (total, first) = p.a.read_object_range(&hash, 0, MIB as usize).unwrap();
    let r =
        p.b.write_object_items(&[item(&hash, 0, total, MIB)], &first, &locks)
            .unwrap();
    assert_eq!(
        r[0].outcome,
        ObjectOutcome::Progressed {
            durable_offset: MIB
        }
    );
    // Bytes inside the durable prefix change behind the store's back.
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut spool = fs::OpenOptions::new()
            .write(true)
            .open(p.b.root.join("incoming-objects").join(&hash))
            .unwrap();
        spool.seek(SeekFrom::Start(10)).unwrap();
        spool.write_all(b"X").unwrap();
    }
    let (_, rest) =
        p.a.read_object_range(&hash, MIB, (2 * MIB) as usize)
            .unwrap();
    let r =
        p.b.write_object_items(&[item(&hash, MIB, total, rest.len() as u64)], &rest, &locks)
            .unwrap();
    assert_eq!(r[0].outcome, ObjectOutcome::CorruptAtSource, "{r:?}");
    assert!(!p.b.root.join("objects").join(&hash).exists());
    assert!(!received_shared(&p.b, &hash));
    // An overlapping resend that differs from the spool resets it.
    let (_, first) = p.a.read_object_range(&hash, 0, MIB as usize).unwrap();
    p.b.write_object_items(&[item(&hash, 0, total, MIB)], &first, &locks)
        .unwrap();
    let mut changed = first.clone();
    changed[100] ^= 1;
    let r =
        p.b.write_object_items(&[item(&hash, 0, total, MIB)], &changed, &locks)
            .unwrap();
    assert!(
        matches!(&r[0].outcome, ObjectOutcome::Failed(e) if e.code == "OBJECT_PART_CHANGED"),
        "{r:?}"
    );
    assert_eq!(r[0].durable_offset, 0);
    assert_eq!(p.b.spool_state(&hash).unwrap(), None);
    // A part past the durable offset is refused with the offset to resume from.
    let r =
        p.b.write_object_items(&[item(&hash, MIB, total, 10)], &rest[..10], &locks)
            .unwrap();
    assert!(
        matches!(&r[0].outcome, ObjectOutcome::Failed(e) if e.code == "RESUME_OFFSET_AHEAD"),
        "{r:?}"
    );
    assert_eq!(r[0].durable_offset, 0);
}

#[test]
fn overlapping_parts_are_verified_and_resume_where_durable() {
    let p = pair(|a| {
        large_object_in(a, "obj.bin", 3 * MIB, 6).unwrap();
    });
    let hash = p.a.lookup("obj.bin").unwrap().content.unwrap();
    let locks = ObjectLocks::default();
    let (total, head) =
        p.a.read_object_range(&hash, 0, (MIB + MIB / 2) as usize)
            .unwrap();
    let r =
        p.b.write_object_items(&[item(&hash, 0, total, head.len() as u64)], &head, &locks)
            .unwrap();
    assert_eq!(r[0].durable_offset, MIB + MIB / 2);
    assert_eq!(
        p.b.spool_state(&hash).unwrap().unwrap().durable,
        MIB + MIB / 2
    );
    // The want offset is the committed durable offset.
    assert!(
        wants(&p.b)
            .iter()
            .any(|(h, _, o)| *h == hash && *o == MIB + MIB / 2)
    );
    // A part that starts inside the durable prefix (other chunk boundaries):
    // the overlap is compared, the suffix appended.
    let (_, part) = p.a.read_object_range(&hash, MIB, MIB as usize).unwrap();
    let r =
        p.b.write_object_items(&[item(&hash, MIB, total, MIB)], &part, &locks)
            .unwrap();
    assert_eq!(
        r[0].outcome,
        ObjectOutcome::Progressed {
            durable_offset: 2 * MIB
        }
    );
    // Several parts of one object in one call, then completion.
    let (_, tail) = p.a.read_object_range(&hash, 2 * MIB, MIB as usize).unwrap();
    let half = tail.len() / 2;
    let r =
        p.b.write_object_items(
            &[
                item(&hash, 2 * MIB, total, half as u64),
                item(
                    &hash,
                    2 * MIB + half as u64,
                    total,
                    (tail.len() - half) as u64,
                ),
            ],
            &tail,
            &locks,
        )
        .unwrap();
    assert_eq!(r[1].outcome, ObjectOutcome::Completed, "{r:?}");
    assert_eq!(r[0].outcome, ObjectOutcome::Completed, "{r:?}");
    assert_eq!(p.b.spool_state(&hash).unwrap(), None);
    assert!(received_shared(&p.b, &hash));
    assert_eq!(availability(&p.b, &hash), Some(AVAILABLE));
}

#[test]
fn startup_truncates_spools_to_durable() {
    let p = pair(|a| {
        large_object_in(a, "obj.bin", 2 * MIB, 8).unwrap();
        file(a, "other", b"other object");
    });
    let hash = p.a.lookup("obj.bin").unwrap().content.unwrap();
    let other = tkfs::core::hash(b"other object");
    let locks = ObjectLocks::default();
    let (total, head) = p.a.read_object_range(&hash, 0, MIB as usize).unwrap();
    p.b.write_object_items(&[item(&hash, 0, total, MIB)], &head, &locks)
        .unwrap();
    let root = p.b.root.clone();
    let repo = p.b.repo.clone();
    drop(p.b);
    // Bytes past the recorded offset (written, maybe fsynced, never
    // advertised), and a spool with no row at all.
    {
        use std::io::Write;
        let mut spool = fs::OpenOptions::new()
            .append(true)
            .open(root.join("incoming-objects").join(&hash))
            .unwrap();
        spool.write_all(&[7; 4096]).unwrap();
        fs::write(root.join("incoming-objects").join(&other), b"other").unwrap();
    }
    assert_eq!(spool_len(&root, &hash), Some(MIB + 4096));
    let b = Store::open(&root, Some(&repo)).unwrap();
    assert_eq!(spool_len(&root, &hash), Some(MIB));
    assert_eq!(b.spool_state(&hash).unwrap().unwrap().durable, MIB);
    assert_eq!(
        spool_len(&root, &other),
        None,
        "a spool without a row has no durable bytes"
    );
    assert!(wants(&b).iter().any(|(h, _, o)| *h == hash && *o == MIB));
    let (done, read) = transfer(&p.a, &b, &locks, &hash, MIB);
    assert_eq!(done.outcome, ObjectOutcome::Completed);
    assert_eq!(read, MIB, "resumed from the durable offset");
}

#[test]
fn legacy_spool_is_adopted_once() {
    let legacy = copy_legacy_worker();
    let partial = &legacy.manifest["partial"];
    let hash = partial["hash"].as_str().unwrap().to_owned();
    let offset = partial["offset"].as_u64().unwrap();
    let total = partial["total"].as_u64().unwrap();
    assert_eq!(spool_len(&legacy.state, &hash), Some(offset));
    let store = Store::open_existing(&legacy.state).unwrap();
    // The legacy code fsynced every append: its file length is durable.
    assert_eq!(
        store.spool_state(&hash).unwrap(),
        Some(tkfs::core::transfer::SpoolState {
            durable: offset,
            total: Some(total)
        })
    );
    assert_eq!(spool_len(&legacy.state, &hash), Some(offset));
    // The partial object is wanted, from the adopted offset.
    let item = store
        .want_objects(LaneSpec::new(0, 1), 256, u64::MAX, &no_leases)
        .unwrap()
        .into_iter()
        .find(|i| i.hash == hash)
        .expect("the fixture's partial object is wanted");
    assert_eq!((item.offset, item.size), (offset, total));
    drop(store);
    // Once: a later row-less spool is not adopted.
    let other = "ab".repeat(32);
    fs::write(legacy.state.join("incoming-objects").join(&other), b"junk").unwrap();
    let store = Store::open_existing(&legacy.state).unwrap();
    assert_eq!(spool_len(&legacy.state, &other), None);
    assert_eq!(store.spool_state(&hash).unwrap().unwrap().durable, offset);
}

#[test]
fn legacy_receive_path_keeps_spool_state() {
    let p = pair(|a| {
        large_object_in(a, "obj.bin", 3 * MIB, 9).unwrap();
    });
    let hash = p.a.lookup("obj.bin").unwrap().content.unwrap();
    let (total, bytes) = p.a.read_object_range(&hash, 0, MIB as usize).unwrap();
    let parts = std::collections::BTreeMap::from([(
        hash.clone(),
        tkfs::core::ObjectPart {
            offset: 0,
            total,
            hex: hex_encode(&bytes),
        },
    )]);
    let outcomes =
        p.b.receive_parts_outcomes(parts, &[], &BTreeSet::new(), None)
            .unwrap();
    assert!(
        matches!(outcomes[0], tkfs::core::PartOutcome::Appended { offset, .. } if offset == MIB)
    );
    assert_eq!(p.b.spool_state(&hash).unwrap().unwrap().durable, MIB);
    // The v3 path resumes the legacy spool where it is durable.
    let (done, read) = transfer(&p.a, &p.b, &ObjectLocks::default(), &hash, MIB);
    assert_eq!(done.outcome, ObjectOutcome::Completed);
    assert_eq!(read, 2 * MIB);
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn tiny_objects_one_data_fsync_per_object_and_one_registration_per_batch() {
    let count = std::env::var("TKFS_WP32_TINY")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(10_000usize);
    let p = pair(|a| {
        small_objects_in(a, "tiny", count, 64, 21).unwrap();
    });
    let locks = ObjectLocks::default();
    let (syncs, registrations, hashes) = (
        p.b.spool_syncs(),
        p.b.object_registrations(),
        p.b.object_hashes(),
    );
    let started = Instant::now();
    let mut calls = 0u64;
    let mut objects = 0u64;
    loop {
        let list =
            p.b.want_objects(lane(), 256, PULL_BUDGET_BYTES, &no_leases)
                .unwrap();
        if list.is_empty() {
            break;
        }
        let mut items = vec![];
        let mut body = vec![];
        for want in &list {
            let (total, bytes) = p.a.read_object_range(&want.hash, 0, MIB as usize).unwrap();
            items.push(item(&want.hash, 0, total, bytes.len() as u64));
            body.extend_from_slice(&bytes);
        }
        let results = p.b.write_object_items(&items, &body, &locks).unwrap();
        assert!(
            results
                .iter()
                .all(|r| r.outcome == ObjectOutcome::Completed),
            "{results:?}"
        );
        calls += 1;
        objects += results.len() as u64;
    }
    // The empty object of the fixture is present on both sides already.
    assert!(objects >= count as u64 - 1, "{objects} of {count}");
    assert_eq!(
        p.b.spool_syncs() - syncs,
        objects,
        "one data fsync per object"
    );
    assert_eq!(
        p.b.object_registrations() - registrations,
        calls,
        "one registration per batch"
    );
    assert_eq!(
        p.b.object_hashes() - hashes,
        objects,
        "one whole-object hash per object"
    );
    let counters = p.b.content_counters().unwrap();
    assert!(counters.complete, "{counters:?}");
    eprintln!(
        "{objects} tiny objects in {calls} writes, {:.1} s ({:.0} objects/s, debug)",
        started.elapsed().as_secs_f64(),
        objects as f64 / started.elapsed().as_secs_f64()
    );
}

// ---------------------------------------------------------------------------
// Report and heal

#[test]
fn report_quarantines_the_sender_copy_and_a_restored_file_heals_on_read() {
    let content = vec![5u8; 200_000];
    let p = pair(|a| {
        file(a, "f.bin", &content);
    });
    let hash = tkfs::core::hash(&content);
    let path = p.a.root.join("objects").join(&hash);
    // A's bytes rot in place: same size, and its mtime marker still matches,
    // so the read path cannot tell and serves them.
    let mut rotten = content.clone();
    rotten[1000] = 6;
    fs::write(&path, &rotten).unwrap();
    fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_modified(UNIX_EPOCH + Duration::from_secs(STAMP_SECS))
        .unwrap();
    let locks = ObjectLocks::default();
    let (result, _) = transfer(&p.a, &p.b, &locks, &hash, 64 * 1024);
    assert_eq!(result.outcome, ObjectOutcome::CorruptAtSource);
    assert!(!received_shared(&p.b, &hash));
    // The receiver reports it; the sender re-verifies and quarantines its copy.
    assert_eq!(
        p.a.report_corrupt(&hash).unwrap(),
        ReportAction::Quarantined
    );
    assert!(!path.exists());
    let aside: Vec<_> = fs::read_dir(p.a.root.join("objects"))
        .unwrap()
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with(&format!("{hash}.corrupt-"))
        })
        .collect();
    assert_eq!(aside.len(), 1, "renamed aside");
    assert_eq!(availability(&p.a, &hash), Some(CORRUPT));
    assert!(
        p.a.object_backoff(&hash).unwrap().is_some(),
        "error state with backoff"
    );
    assert_eq!(
        p.a.read_object_range(&hash, 0, 10).unwrap_err().code,
        "NOT_AVAILABLE"
    );
    // A good file restored by the user is verified on its first read and served.
    fs::write(&path, &content).unwrap();
    let (total, bytes) = p.a.read_object_range(&hash, 0, 10).unwrap();
    assert_eq!((total, bytes), (200_000, content[..10].to_vec()));
    assert_eq!(availability(&p.a, &hash), Some(AVAILABLE));
    // A good copy reported again is kept.
    thread::sleep(Duration::from_millis(10));
    // Deleted behind the store's back: not available; restored: healed.
    fs::remove_file(&path).unwrap();
    assert_eq!(
        p.a.read_object_range(&hash, 0, 10).unwrap_err().code,
        "NOT_AVAILABLE"
    );
    assert_eq!(
        availability(&p.a, &hash),
        Some(tkfs::core::objects::MISSING)
    );
    fs::write(&path, &content).unwrap();
    assert_eq!(
        p.a.read_object_range(&hash, 0, 10).unwrap().1,
        content[..10].to_vec()
    );
    // The receiver now completes from the healed sender.
    let (done, _) = transfer(&p.a, &p.b, &locks, &hash, 64 * 1024);
    assert_eq!(done.outcome, ObjectOutcome::Completed);
}

#[test]
fn report_of_a_good_copy_keeps_it() {
    let p = pair(|a| {
        file(a, "f", b"good bytes");
    });
    let hash = tkfs::core::hash(b"good bytes");
    assert_eq!(p.a.report_corrupt(&hash).unwrap(), ReportAction::VerifiedOk);
    assert_eq!(
        p.a.read_object_range(&hash, 0, 64).unwrap().1,
        b"good bytes"
    );
}

// ---------------------------------------------------------------------------
// Worker ops with leases (invariant 19)

struct Worker {
    _dir: tempfile::TempDir,
    a: Store,
    ctx: Arc<tkfs::runtime::ctx::WorkerCtx>,
    route_b: Value,
}

/// B as an in-process worker context (its engine owns B's store); A stays a
/// plain store (the source).
fn worker(author: impl FnOnce(&mut Store)) -> Worker {
    let p = pair(author);
    let route_b = json!({"peer": "peer-a", "repo": p.b.repo, "local_replica": p.b.device,
        "remote_replica": p.a.device});
    let root = p.b.root.clone();
    let engine: runtime::Shared = Arc::new(Mutex::new(Engine::new(p.b)));
    let ctx = tkfs::worker::build_ctx(engine, &root, false, None);
    Worker {
        _dir: p.dir,
        a: p.a,
        ctx,
        route_b,
    }
}

fn op(
    ctx: &tkfs::runtime::ctx::WorkerCtx,
    name: &str,
    header: Value,
    body: &[u8],
) -> Result<(Value, Vec<u8>), tkfs::model::errors::ErrorWire> {
    objects_rpc::dispatch(ctx, name, &header, body)
}

fn want_op(w: &Worker, route: &Value, session: &str) -> Vec<Value> {
    let (reply, _) = op(
        &w.ctx,
        "objects.want",
        json!({"route": route, "lane": {"index": 0, "count": 1}, "max_items": 256,
            "max_bytes": PULL_BUDGET_BYTES, "session": session}),
        &[],
    )
    .unwrap();
    reply["items"].as_array().unwrap().clone()
}

fn write_op(
    w: &Worker,
    route: &Value,
    session: &str,
    hash: &str,
    offset: u64,
    total: u64,
    bytes: &[u8],
) -> Value {
    let (reply, _) = op(
        &w.ctx,
        "objects.write",
        json!({"route": route, "session": session,
            "items": [{"hash": hash, "offset": offset, "total": total, "len": bytes.len(), "last": offset + bytes.len() as u64 == total}]}),
        bytes,
    )
    .unwrap();
    reply["results"][0].clone()
}

#[test]
fn duplicate_sessions_transfer_once() {
    let w = worker(|a| {
        file(a, "x", &vec![1; 100_000]);
    });
    let hash = tkfs::core::hash(&vec![1; 100_000]);
    let first = want_op(&w, &w.route_b, "session-1");
    assert!(first.iter().any(|i| i["hash"] == hash.as_str()));
    // Another session (another lane, peer or connection) is not offered it.
    let second = want_op(&w, &w.route_b, "session-2");
    assert!(
        !second.iter().any(|i| i["hash"] == hash.as_str()),
        "{second:?}"
    );
    let (_, bytes) = w.a.read_object_range(&hash, 0, MIB as usize).unwrap();
    // A write by the other session touches nothing.
    let refused = write_op(&w, &w.route_b, "session-2", &hash, 0, 100_000, &bytes);
    assert_eq!(refused["outcome"], "in_flight_elsewhere", "{refused}");
    assert_eq!(spool_len(&w.ctx.root, &hash), None);
    // The lease holder transfers it once.
    let done = write_op(&w, &w.route_b, "session-1", &hash, 0, 100_000, &bytes);
    assert_eq!(done["outcome"], "completed", "{done}");
    assert!(
        w.ctx.inflight_objects.lock().unwrap().get(&hash).is_none(),
        "lease released"
    );
    assert!(want_op(&w, &w.route_b, "session-2").is_empty());
    let again = write_op(&w, &w.route_b, "session-2", &hash, 0, 100_000, &bytes);
    assert_eq!(again["outcome"], "skipped", "{again}");
}

#[test]
fn mutual_pairing_transfers_once_and_leases_expire() {
    let w = worker(|a| {
        file(a, "y", &vec![2; 50_000]);
    });
    let hash = tkfs::core::hash(&vec![2; 50_000]);
    // The same object from two peers (mutual pairing: an inbound and an
    // outbound session for one peer, or two peers holding it).
    let route_c = json!({"peer": "peer-c", "repo": w.route_b["repo"], "local_replica": w.route_b["local_replica"],
        "remote_replica": "replica-c"});
    let from_a = want_op(&w, &w.route_b, "dialer-session");
    let from_c = want_op(&w, &route_c, "acceptor-session");
    assert_eq!(from_a.len(), 1);
    assert!(from_c.is_empty(), "leased to the first session");
    // A lease that lapses (no progress for its TTL) frees the object.
    w.ctx
        .inflight_objects
        .lock()
        .unwrap()
        .get_mut(&hash)
        .unwrap()
        .expires_ms = 1;
    let from_c = want_op(&w, &route_c, "acceptor-session");
    assert_eq!(from_c.len(), 1);
    let (_, bytes) = w.a.read_object_range(&hash, 0, MIB as usize).unwrap();
    let late = write_op(&w, &w.route_b, "dialer-session", &hash, 0, 50_000, &bytes);
    assert_eq!(late["outcome"], "in_flight_elsewhere");
    let done = write_op(&w, &route_c, "acceptor-session", &hash, 0, 50_000, &bytes);
    assert_eq!(done["outcome"], "completed");
}

#[test]
fn out_of_order_writes_wait_for_their_turn() {
    let w = worker(|a| {
        large_object_in(a, "o.bin", 2 * MIB, 12).unwrap();
    });
    let hash = w.a.lookup("o.bin").unwrap().content.unwrap();
    assert_eq!(want_op(&w, &w.route_b, "s").len(), 1);
    let (_, first) = w.a.read_object_range(&hash, 0, MIB as usize).unwrap();
    let (_, second) = w.a.read_object_range(&hash, MIB, MIB as usize).unwrap();
    // The second part arrives first: it waits for the first.
    let ctx = w.ctx.clone();
    let route = w.route_b.clone();
    let hash2 = hash.clone();
    let late = thread::spawn(move || {
        let (reply, _) = objects_rpc::dispatch(
            &ctx,
            "objects.write",
            &json!({"route": route, "session": "s",
                "items": [{"hash": hash2, "offset": MIB, "total": 2 * MIB, "len": MIB, "last": true}]}),
            &second,
        )
        .unwrap();
        reply["results"][0].clone()
    });
    thread::sleep(Duration::from_millis(100));
    let early = write_op(&w, &w.route_b, "s", &hash, 0, 2 * MIB, &first);
    assert_eq!(
        early["outcome"]["progressed"]["durable_offset"], MIB,
        "{early}"
    );
    let late = late.join().unwrap();
    assert_eq!(late["outcome"], "completed", "{late}");
}

#[test]
fn a_corrupt_source_is_backed_off_for_that_source_only() {
    let w = worker(|a| {
        file(a, "z", &vec![3; 1000]);
    });
    let hash = tkfs::core::hash(&vec![3; 1000]);
    assert_eq!(want_op(&w, &w.route_b, "s").len(), 1);
    let wrong = vec![4; 1000];
    let result = write_op(&w, &w.route_b, "s", &hash, 0, 1000, &wrong);
    assert_eq!(result["outcome"], "corrupt_at_source", "{result}");
    assert!(
        objects_rpc::source_retry_at(&w.ctx, w.route_b["remote_replica"].as_str().unwrap(), &hash)
            .is_some()
    );
    // Not wanted from that source while it is backed off; wanted from another.
    assert!(want_op(&w, &w.route_b, "s").is_empty());
    let route_c = json!({"peer": "peer-c", "repo": w.route_b["repo"], "local_replica": w.route_b["local_replica"],
        "remote_replica": "replica-c"});
    assert_eq!(want_op(&w, &route_c, "s2").len(), 1);
}

/// The lease of `hash` (it must exist).
fn lease(w: &Worker, hash: &str) -> tkfs::runtime::ctx::ObjectLease {
    w.ctx
        .inflight_objects
        .lock()
        .unwrap()
        .get(hash)
        .cloned()
        .unwrap_or_else(|| panic!("no lease on {hash}"))
}

/// The want-list item for `hash`, if offered.
fn offered<'a>(items: &'a [Value], hash: &str) -> Option<&'a Value> {
    items.iter().find(|item| item["hash"] == hash)
}

/// Another source of the same repository (`replica-c`).
fn route_c(w: &Worker) -> Value {
    json!({"peer": "peer-c", "repo": w.route_b["repo"], "local_replica": w.route_b["local_replica"],
        "remote_replica": "replica-c"})
}

/// Moves the expiry of the lease on `hash` back by one `LEASE_TTL`: the
/// state one TTL later, since nothing but progress moves an expiry (the
/// test below asserts that).
fn age_one_ttl(w: &Worker, hash: &str) {
    let ttl = tkfs::model::protocol::objects::LEASE_TTL.as_millis() as u64;
    let mut leases = w.ctx.inflight_objects.lock().unwrap();
    let lease = leases.get_mut(hash).unwrap();
    lease.expires_ms = lease.expires_ms.saturating_sub(ttl);
}

#[test]
fn want_never_renews_a_lease_only_progress_does() {
    let content = vec![6u8; 300_000];
    let w = worker(|a| {
        file(a, "r.bin", &content);
    });
    let hash = tkfs::core::hash(&content);
    let route_c = route_c(&w);
    // S1 is offered the object and leased it.
    assert!(offered(&want_op(&w, &w.route_b, "s1"), &hash).is_some());
    let granted = lease(&w, &hash);
    assert_eq!(granted.session, "s1");
    // S1 keeps asking without writing: offered again (its own lease: a
    // reconnect resumes it), but the expiry never moves; another source's
    // session is never offered it meanwhile.
    for _ in 0..3 {
        thread::sleep(Duration::from_millis(20));
        let again = want_op(&w, &w.route_b, "s1");
        assert_eq!(offered(&again, &hash).unwrap()["offset"], 0);
        assert_eq!(
            lease(&w, &hash).expires_ms,
            granted.expires_ms,
            "objects.want renewed the lease"
        );
        assert!(offered(&want_op(&w, &route_c, "c1"), &hash).is_none());
    }
    // Progress renews it.
    thread::sleep(Duration::from_millis(20));
    let progressed = write_op(&w, &w.route_b, "s1", &hash, 0, 300_000, &content[..100_000]);
    assert_eq!(
        progressed["outcome"]["progressed"]["durable_offset"], 100_000,
        "{progressed}"
    );
    let renewed = lease(&w, &hash);
    assert!(renewed.expires_ms > granted.expires_ms, "{renewed:?}");
    assert_eq!(renewed.offset, 100_000);
    // A failed write does not: bytes that differ from the durable spool reset
    // it (the writer resumes at 0, the lease stays S1's).
    thread::sleep(Duration::from_millis(20));
    let changed = write_op(&w, &w.route_b, "s1", &hash, 0, 300_000, &vec![7u8; 100_000]);
    assert_eq!(
        changed["outcome"]["failed"]["code"], "OBJECT_PART_CHANGED",
        "{changed}"
    );
    assert_eq!(changed["durable_offset"], 0);
    let after = lease(&w, &hash);
    assert_eq!(
        (after.session.as_str(), after.expires_ms, after.offset),
        ("s1", renewed.expires_ms, 0)
    );
    // A want list never moves a held lease's offset back: the store's scan
    // can predate a write that has since advanced it.
    let progressed = write_op(&w, &w.route_b, "s1", &hash, 0, 300_000, &content[..100_000]);
    assert_eq!(
        progressed["outcome"]["progressed"]["durable_offset"],
        100_000
    );
    w.ctx
        .inflight_objects
        .lock()
        .unwrap()
        .get_mut(&hash)
        .unwrap()
        .offset = 200_000;
    let again = want_op(&w, &w.route_b, "s1");
    assert_eq!(offered(&again, &hash).unwrap()["offset"], 100_000);
    assert_eq!(lease(&w, &hash).offset, 200_000);
}

#[test]
fn a_stalled_lease_moves_the_object_to_another_source() {
    let (x, y) = (vec![8u8; 200_000], vec![9u8; 150_000]);
    let w = worker(|a| {
        file(a, "x.bin", &x);
        file(a, "y.bin", &y);
    });
    let (hx, hy) = (tkfs::core::hash(&x), tkfs::core::hash(&y));
    let source_a = w.route_b["remote_replica"].as_str().unwrap().to_owned();
    let route_c = route_c(&w);
    // A's session S1 holds both and makes progress on X only, then stalls:
    // it keeps asking (a source that cannot serve them, say) but never
    // writes again.
    let first = want_op(&w, &w.route_b, "s1");
    assert!(offered(&first, &hx).is_some() && offered(&first, &hy).is_some());
    let done = write_op(&w, &w.route_b, "s1", &hx, 0, 200_000, &x[..50_000]);
    assert_eq!(done["outcome"]["progressed"]["durable_offset"], 50_000);
    for _ in 0..3 {
        let again = want_op(&w, &w.route_b, "s1");
        assert!(offered(&again, &hx).is_some() && offered(&again, &hy).is_some());
        assert!(want_op(&w, &route_c, "c1").is_empty(), "held by S1");
    }
    // One TTL without progress later, both leases have lapsed. S1 asking
    // again does not keep them: each object is backed off for source A (any
    // of A's sessions), and C's session is offered them, from the durable
    // offsets.
    age_one_ttl(&w, &hx);
    age_one_ttl(&w, &hy);
    let again = want_op(&w, &w.route_b, "s1");
    assert!(offered(&again, &hx).is_none() && offered(&again, &hy).is_none());
    assert!(want_op(&w, &w.route_b, "s1-other").is_empty());
    for hash in [&hx, &hy] {
        let retry_at = objects_rpc::source_retry_at(&w.ctx, &source_a, hash).unwrap();
        let stall = objects_rpc::STALL_BACKOFF.as_millis() as u64;
        assert!(retry_at <= tkfs::util::time::now_ms() + stall, "{retry_at}");
    }
    let from_c = want_op(&w, &route_c, "c1");
    assert_eq!(offered(&from_c, &hx).unwrap()["offset"], 50_000);
    assert_eq!(offered(&from_c, &hy).unwrap()["offset"], 0);
    assert_eq!(lease(&w, &hx).session, "c1");
    let finished = write_op(&w, &route_c, "c1", &hx, 50_000, 200_000, &x[50_000..]);
    assert_eq!(finished["outcome"], "completed", "{finished}");
    // A write from A's session for Y while C holds it touches nothing.
    let late = write_op(&w, &w.route_b, "s1", &hy, 0, 150_000, &y);
    assert_eq!(late["outcome"], "in_flight_elsewhere");
    // C stalls on Y too: after its TTL Y is backed off for C as well, and
    // free. A write from A then takes the lease (a slow transfer continues)
    // and, while it holds it, A's session is offered Y despite A's backoff
    // (it resumes); C's is not.
    age_one_ttl(&w, &hy);
    assert!(want_op(&w, &route_c, "c1").is_empty());
    assert!(want_op(&w, &w.route_b, "s1").is_empty());
    let resumed = write_op(&w, &w.route_b, "s1", &hy, 0, 150_000, &y[..10_000]);
    assert_eq!(resumed["outcome"]["progressed"]["durable_offset"], 10_000);
    assert_eq!(
        offered(&want_op(&w, &w.route_b, "s1"), &hy).unwrap()["offset"],
        10_000
    );
    assert!(want_op(&w, &route_c, "c2").is_empty());
}

#[test]
fn failed_writes_back_their_object_off() {
    let (m, l) = (vec![10u8; 4000], vec![11u8; 5000]);
    let w = worker(|a| {
        file(a, "m.bin", &m);
        file(a, "l.bin", &l);
    });
    let (hm, hl) = (tkfs::core::hash(&m), tkfs::core::hash(&l));
    let source_a = w.route_b["remote_replica"].as_str().unwrap().to_owned();
    let route_c = route_c(&w);
    let wanted = want_op(&w, &w.route_b, "s");
    assert!(offered(&wanted, &hm).is_some() && offered(&wanted, &hl).is_some());
    // A size the source claims and nothing here agrees with (two parts of it
    // in one call): backed off for that source, one attempt per object
    // (escalating, like a corrupt copy: 30 s first), the lease released.
    let (reply, _) = op(
        &w.ctx,
        "objects.write",
        json!({"route": w.route_b, "session": "s", "items": [
            {"hash": hm, "offset": 0, "total": 4001, "len": 2000, "last": false},
            {"hash": hm, "offset": 2000, "total": 4001, "len": 2001, "last": true}]}),
        &[10u8; 4001],
    )
    .unwrap();
    for result in reply["results"].as_array().unwrap() {
        assert_eq!(result["outcome"], "size_mismatch", "{reply}");
    }
    let retry_at = objects_rpc::source_retry_at(&w.ctx, &source_a, &hm).unwrap();
    assert!(
        retry_at <= tkfs::util::time::now_ms() + 30_000,
        "one attempt for the object: {retry_at}"
    );
    assert!(w.ctx.inflight_objects.lock().unwrap().get(&hm).is_none());
    assert!(offered(&want_op(&w, &w.route_b, "s"), &hm).is_none());
    assert!(offered(&want_op(&w, &route_c, "c"), &hm).is_some());
    // A local failure (the spool cannot be created): the object backoff
    // (`object_error_state`), honored for every source; the lease released.
    fs::create_dir_all(w.ctx.root.join("incoming-objects").join(&hl)).unwrap();
    let local = write_op(&w, &w.route_b, "s", &hl, 0, 5000, &l);
    assert!(local["outcome"]["failed"]["code"].is_string(), "{local}");
    assert_ne!(local["outcome"]["failed"]["code"], "RESUME_OFFSET_AHEAD");
    assert!(w.ctx.inflight_objects.lock().unwrap().get(&hl).is_none());
    assert!(objects_rpc::source_retry_at(&w.ctx, &source_a, &hl).is_none());
    let store = w.ctx.sync_stores.checkout().unwrap();
    assert!(store.object_backoff(&hl).unwrap().is_some());
    drop(store);
    assert!(offered(&want_op(&w, &w.route_b, "s"), &hl).is_none());
    assert!(offered(&want_op(&w, &route_c, "c"), &hl).is_none());
}

#[test]
fn json_ops_over_a_session_fire_their_fault_point_once() {
    use tkfs::util::fault::{self, FaultAction, FaultConfig};
    let p = pair(|a| {
        file(a, "f", b"f");
    });
    let root = p.a.root.clone();
    let engine: runtime::Shared = Arc::new(Mutex::new(Engine::new(p.a)));
    let info = runtime::start_rpc(engine.clone(), &root, None).unwrap();
    let conn = session::dial_loopback(&info.address, None, ConnOptions::default()).unwrap();
    // Two hangs of 1.5 s on `worker.rpc.mount-status` (an op no other test
    // of this binary sends): one per call, as over the JSON RPC. A
    // session that fired the point twice per call would spend both hangs
    // on the first call and none on the second.
    let hang = Duration::from_millis(1500);
    fault::configure(Some(FaultConfig {
        point: "worker.rpc.mount-status".into(),
        scope: None,
        action: FaultAction::Hang,
        ms: Some(hang.as_millis() as u64),
        count: Some(2),
    }));
    let mut took = vec![];
    for _ in 0..2 {
        let started = Instant::now();
        let status = conn.call(
            "mount-status",
            json!({}),
            vec![],
            Priority::Control,
            Duration::from_secs(20),
        );
        took.push(started.elapsed());
        assert!(status.is_ok(), "{status:?}");
    }
    fault::configure(None);
    assert!(took.iter().all(|t| *t >= hang), "{took:?}");
    engine
        .lock()
        .unwrap()
        .stopping
        .store(true, std::sync::atomic::Ordering::Relaxed);
}

#[test]
fn worker_ops_check_routes_and_requests() {
    let w = worker(|a| {
        file(a, "f", b"f");
    });
    let mut wrong = w.route_b.clone();
    wrong["repo"] = json!("another-repository");
    let error = op(
        &w.ctx,
        "objects.want",
        json!({"route": wrong, "lane": {"index": 0, "count": 1},
        "max_items": 1, "max_bytes": 1, "session": "s"}),
        &[],
    )
    .unwrap_err();
    assert_eq!(error.code, "REPOSITORY_MISMATCH");
    let mut wrong = w.route_b.clone();
    wrong["local_replica"] = json!("someone-else");
    let error = op(
        &w.ctx,
        "objects.read",
        json!({"route": wrong, "hash": "00", "offset": 0, "len": 1}),
        &[],
    )
    .unwrap_err();
    assert_eq!(error.code, "LOCAL_REPLICA_BINDING_MISMATCH");
    let error = op(&w.ctx, "objects.want", json!({"route": w.route_b}), &[]).unwrap_err();
    assert_eq!(error.code, "INVALID_REQUEST");
    let error = op(
        &w.ctx,
        "objects.want",
        json!({"route": w.route_b, "lane": {"index": 2, "count": 2},
        "max_items": 1, "max_bytes": 1, "session": "s"}),
        &[],
    )
    .unwrap_err();
    assert_eq!(error.code, "INVALID_BULK_LANE");
    let error = op(&w.ctx, "objects.write", json!({"route": w.route_b, "session": "s",
        "items": [{"hash": tkfs::core::hash(b"f"), "offset": 0, "total": 1, "len": 1, "last": true}]}), b"ff").unwrap_err();
    assert_eq!(error.code, "INVALID_OBJECT_PART");
    let error = op(
        &w.ctx,
        "objects.report",
        json!({"route": w.route_b, "hash": tkfs::core::hash(b"f"),
        "code": "SOMETHING_ELSE"}),
        &[],
    )
    .unwrap_err();
    assert_eq!(error.code, "INVALID_REQUEST");
    let error = op(&w.ctx, "objects.unknown", json!({}), &[]).unwrap_err();
    assert_eq!(error.code, "UNSUPPORTED_OPERATION");
}

// ---------------------------------------------------------------------------
// Loopback sessions on the worker listener

#[test]
fn loopback_sessions_serve_objects_and_json_ops_next_to_the_json_rpc() {
    let content = vec![8u8; 3 * MIB as usize / 2];
    let p = pair(|a| {
        file(a, "f.bin", &content);
    });
    let hash = tkfs::core::hash(&content);
    let (a_root, a_repo, a_device, b_device) = (
        p.a.root.clone(),
        p.a.repo.clone(),
        p.a.device.clone(),
        p.b.device.clone(),
    );
    let engine: runtime::Shared = Arc::new(Mutex::new(Engine::new(p.a)));
    let info = runtime::start_rpc(engine.clone(), &a_root, None).unwrap();
    let conn = session::dial_loopback(&info.address, None, ConnOptions::default()).unwrap();
    let call = |conn: &Conn, op: &str, header: Value, body: Vec<u8>| {
        conn.call(op, header, body, Priority::Control, Duration::from_secs(20))
    };
    // JSON ops over the session: the header is the payload.
    let hello = call(&conn, "hello", json!({}), vec![]).unwrap();
    assert_eq!(hello.header["device"], a_device.as_str());
    let status = call(&conn, "status", json!({}), vec![]).unwrap();
    let status: Value = if status.header["in_body"] == true {
        serde_json::from_slice(&status.body).unwrap()
    } else {
        status.header
    };
    assert_eq!(status["repo"], a_repo.as_str());
    let error = call(&conn, "stop", json!({}), vec![]).unwrap_err();
    assert_eq!(error.code(), "UNSUPPORTED_OPERATION");
    // Binary reads: at most 1 MiB per call.
    let route =
        json!({"peer": "p", "repo": a_repo, "local_replica": a_device, "remote_replica": b_device});
    let read = call(
        &conn,
        "objects.read",
        json!({"route": route, "hash": hash, "offset": 0, "len": 4 * MIB}),
        vec![],
    )
    .unwrap();
    assert_eq!(read.header["total"], content.len());
    assert_eq!(read.body.len(), MIB as usize);
    let read = call(
        &conn,
        "objects.read",
        json!({"route": route, "hash": hash, "offset": MIB, "len": MIB}),
        vec![],
    )
    .unwrap();
    assert_eq!(read.body, content[MIB as usize..]);
    // The JSON RPC on the same listener is unchanged; binary replies need a session.
    let status = runtime::rpc(&info, &tkfs::core::id(), json!({"op": "status"})).unwrap();
    assert_eq!(status["repo"], a_repo.as_str());
    let error = runtime::rpc(
        &info,
        &tkfs::core::id(),
        json!({"op": "objects.read", "route": route,
        "hash": hash, "offset": 0, "len": 16}),
    )
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("BINARY_OPS_REQUIRE_SESSION"),
        "{error:#}"
    );
    let report = runtime::rpc(
        &info,
        &tkfs::core::id(),
        json!({"op": "objects.report", "route": route,
        "hash": hash, "code": "CORRUPT_AT_SOURCE"}),
    )
    .unwrap();
    assert_eq!(report["action"], "verified_ok");
    engine
        .lock()
        .unwrap()
        .stopping
        .store(true, std::sync::atomic::Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// Crash windows on real worker processes

struct Procs {
    _dir: tempfile::TempDir,
    a_state: std::path::PathBuf,
    b_state: std::path::PathBuf,
    route_a: Value,
    route_b: Value,
}

fn procs(author: impl FnOnce(&mut Store)) -> Procs {
    let p = pair(author);
    let route_b = json!({"peer": "peer-a", "repo": p.b.repo, "local_replica": p.b.device, "remote_replica": p.a.device});
    let route_a = json!({"peer": "peer-b", "repo": p.a.repo, "local_replica": p.a.device, "remote_replica": p.b.device});
    let (a_state, b_state) = (p.a.root.clone(), p.b.root.clone());
    drop(p.a);
    drop(p.b);
    Procs {
        _dir: p.dir,
        a_state,
        b_state,
        route_a,
        route_b,
    }
}

fn spawn(state: &Path, env: &[(&str, &str)]) -> WorkerProc {
    let mut worker = WorkerProc::spawn(state, &[], env).unwrap();
    worker.wait_ready(Duration::from_secs(120)).unwrap();
    worker
}

fn dial(state: &Path) -> Conn {
    let address = discovery_at(state).unwrap().address;
    session::dial_loopback(&address, None, ConnOptions::default()).unwrap()
}

fn rpc_call(
    conn: &Conn,
    op: &str,
    header: Value,
    body: Vec<u8>,
) -> Result<session::Reply, tkfs::model::orchestrator::CallError> {
    conn.call(op, header, body, Priority::Control, Duration::from_secs(30))
}

fn want_items(conn: &Conn, route: &Value, session: &str) -> Vec<Value> {
    let reply = rpc_call(
        conn,
        "objects.want",
        json!({"route": route, "lane": {"index": 0, "count": 1}, "max_items": 256,
            "max_bytes": PULL_BUDGET_BYTES, "session": session}),
        vec![],
    )
    .unwrap();
    reply.header["items"].as_array().unwrap().clone()
}

/// Read `[offset, offset + len)` of `hash` from A and write it to B.
fn chunk(
    a: &Conn,
    b: &Conn,
    p: &Procs,
    session: &str,
    hash: &str,
    offset: u64,
    len: u64,
) -> Result<Value, tkfs::model::orchestrator::CallError> {
    let read = rpc_call(
        a,
        "objects.read",
        json!({"route": p.route_a, "hash": hash, "offset": offset, "len": len}),
        vec![],
    )
    .unwrap();
    let total = read.header["total"].as_u64().unwrap();
    let n = read.body.len() as u64;
    let reply = rpc_call(
        b,
        "objects.write",
        json!({"route": p.route_b, "session": session,
            "items": [{"hash": hash, "offset": offset, "total": total, "len": n, "last": offset + n == total}]}),
        read.body,
    )?;
    Ok(reply.header["results"][0].clone())
}

/// Drive `hash` from B's want offset to completion; returns bytes read from A.
fn finish(a: &Conn, b: &Conn, p: &Procs, session: &str, hash: &str) -> u64 {
    let item = want_items(b, &p.route_b, session)
        .into_iter()
        .find(|i| i["hash"] == hash)
        .expect("wanted");
    let mut offset = item["offset"].as_u64().unwrap();
    let mut read = 0;
    loop {
        let result = chunk(a, b, p, session, hash, offset, MIB).unwrap();
        read += MIB.min(item["size"].as_u64().unwrap() - offset);
        if result["outcome"] == "completed" {
            return read;
        }
        offset = result["outcome"]["progressed"]["durable_offset"]
            .as_u64()
            .unwrap_or_else(|| panic!("{result}"));
    }
}

#[test]
fn crash_mid_object_resumes_from_durable() {
    let size = 8 * MIB + 333;
    let mut hash = String::new();
    let p = procs(|a| {
        hash = large_object_in(a, "big.bin", size, 31).unwrap();
    });
    let _a = spawn(&p.a_state, &[]);
    let a = dial(&p.a_state);
    let mut b = spawn(&p.b_state, &[]);
    let conn = dial(&p.b_state);
    assert!(
        want_items(&conn, &p.route_b, "s")
            .iter()
            .any(|i| i["hash"] == hash.as_str())
    );
    for k in 0..3 {
        let result = chunk(&a, &conn, &p, "s", &hash, k * MIB, MIB).unwrap();
        assert_eq!(
            result["outcome"]["progressed"]["durable_offset"],
            (k + 1) * MIB
        );
    }
    b.stop().unwrap();
    assert_eq!(durable_row(&p.b_state, &hash), Some(3 * MIB));
    // The fault: the worker exits after the spool fsync, before the commit.
    let mut b = spawn(&p.b_state, &[("TKFS_FAULT", "object_spool_synced")]);
    let conn = dial(&p.b_state);
    let lost = chunk(&a, &conn, &p, "s", &hash, 3 * MIB, MIB);
    assert!(lost.is_err(), "{lost:?}");
    let status = b
        .wait_exit(Duration::from_secs(30))
        .expect("exited at the fault");
    assert_eq!(status.code(), Some(86));
    // Fsynced on disk, but not advertised: the committed offset is unchanged.
    assert_eq!(spool_len(&p.b_state, &hash), Some(4 * MIB));
    assert_eq!(durable_row(&p.b_state, &hash), Some(3 * MIB));
    // The restart cuts the spool back to the durable offset and wants it from there.
    let _b = spawn(&p.b_state, &[]);
    assert_eq!(spool_len(&p.b_state, &hash), Some(3 * MIB));
    let conn = dial(&p.b_state);
    let item = want_items(&conn, &p.route_b, "s2")
        .into_iter()
        .find(|i| i["hash"] == hash.as_str())
        .unwrap();
    assert_eq!(item["offset"], 3 * MIB);
    let read = finish(&a, &conn, &p, "s2", &hash);
    assert_eq!(read, size - 3 * MIB, "no blind replay");
    let served = rpc_call(
        &conn,
        "objects.read",
        json!({"route": p.route_b, "hash": hash, "offset": size - 10, "len": 10}),
        vec![],
    )
    .unwrap();
    assert_eq!(served.body.len(), 10, "registered and shareable");
}

#[test]
fn kill_mid_object_resumes_from_durable() {
    let size = 24 * MIB;
    let mut hash = String::new();
    let p = procs(|a| {
        hash = large_object_in(a, "big.bin", size, 32).unwrap();
    });
    let _a = spawn(&p.a_state, &[]);
    let a = dial(&p.a_state);
    let mut b = spawn(&p.b_state, &[]);
    let conn = dial(&p.b_state);
    want_items(&conn, &p.route_b, "s");
    // Stream writes from another thread and kill the worker mid-object.
    let advertised = Arc::new(Mutex::new(0u64));
    let streamer = {
        let (a, conn, advertised) = (a.clone(), conn.clone(), advertised.clone());
        let p = (p.route_a.clone(), p.route_b.clone());
        let hash = hash.clone();
        thread::spawn(move || {
            let mut offset = 0;
            loop {
                let Ok(read) = a.call(
                    "objects.read",
                    json!({"route": p.0, "hash": hash, "offset": offset, "len": MIB}),
                    vec![],
                    Priority::Control,
                    Duration::from_secs(30),
                ) else {
                    return;
                };
                let n = read.body.len() as u64;
                let Ok(reply) = conn.call("objects.write", json!({"route": p.1, "session": "s",
                    "items": [{"hash": hash, "offset": offset, "total": size, "len": n, "last": offset + n == size}]}),
                    read.body, Priority::Data, Duration::from_secs(30)) else { return };
                let Some(durable) =
                    reply.header["results"][0]["outcome"]["progressed"]["durable_offset"].as_u64()
                else {
                    return;
                };
                *advertised.lock().unwrap() = durable;
                offset = durable;
            }
        })
    };
    assert!(wait_until(Duration::from_secs(60), || *advertised
        .lock()
        .unwrap()
        >= 6 * MIB));
    b.kill();
    streamer.join().unwrap();
    let last_advertised = *advertised.lock().unwrap();
    assert!(
        last_advertised < size,
        "killed mid-object ({last_advertised} of {size})"
    );
    // Nothing advertised was lost: the committed offset covers every
    // advertised one, and the spool holds at least that much.
    let durable = durable_row(&p.b_state, &hash).unwrap();
    assert!(
        durable >= last_advertised,
        "durable {durable} < advertised {last_advertised}"
    );
    assert!(spool_len(&p.b_state, &hash).unwrap() >= durable);
    let _b = spawn(&p.b_state, &[]);
    assert_eq!(spool_len(&p.b_state, &hash), Some(durable));
    let conn = dial(&p.b_state);
    let read = finish(&a, &conn, &p, "s2", &hash);
    assert_eq!(read, size - durable, "resumed from the durable offset");
}

#[test]
fn crash_between_install_and_register_retransfers() {
    let content = vec![9u8; 300_000];
    let p = procs(|a| {
        file(a, "x.bin", &content);
    });
    let hash = tkfs::core::hash(&content);
    let _a = spawn(&p.a_state, &[]);
    let a = dial(&p.a_state);
    let mut b = spawn(&p.b_state, &[("TKFS_FAULT", "object_installed")]);
    let conn = dial(&p.b_state);
    assert_eq!(want_items(&conn, &p.route_b, "s").len(), 1);
    let lost = chunk(&a, &conn, &p, "s", &hash, 0, MIB);
    assert!(lost.is_err(), "{lost:?}");
    assert_eq!(
        b.wait_exit(Duration::from_secs(30)).unwrap().code(),
        Some(86)
    );
    // Installed (verified bytes in the CAS), but not registered.
    assert_eq!(
        fs::read(p.b_state.join("objects").join(&hash)).unwrap(),
        content
    );
    let shared = |state: &Path| {
        let db = rusqlite::Connection::open(state.join("metadata.sqlite")).unwrap();
        db.query_row(
            "SELECT EXISTS(SELECT 1 FROM received_shared_objects WHERE hash=?)",
            [&hash],
            |r| r.get::<_, bool>(0),
        )
        .unwrap()
    };
    assert!(!shared(&p.b_state));
    // No startup registration (I4): still unshareable, still wanted.
    let _b = spawn(&p.b_state, &[]);
    assert!(!shared(&p.b_state));
    let conn = dial(&p.b_state);
    let peer_route = json!({"peer": "c", "repo": p.route_b["repo"], "local_replica": p.route_b["local_replica"], "remote_replica": "replica-c"});
    let refused = rpc_call(
        &conn,
        "objects.read",
        json!({"route": peer_route, "hash": hash, "offset": 0, "len": 10}),
        vec![],
    )
    .unwrap_err();
    assert_eq!(refused.code(), "NOT_PERMITTED");
    let item = want_items(&conn, &p.route_b, "s2");
    assert_eq!(item.len(), 1);
    assert_eq!(item[0]["offset"], 0);
    // The safe re-transfer verifies and registers it.
    let read = finish(&a, &conn, &p, "s2", &hash);
    assert_eq!(read, content.len() as u64);
    assert!(shared(&p.b_state));
    let served = rpc_call(
        &conn,
        "objects.read",
        json!({"route": peer_route, "hash": hash, "offset": 0, "len": 10}),
        vec![],
    )
    .unwrap();
    assert_eq!(served.body, content[..10].to_vec());
}
