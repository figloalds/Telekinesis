//! WP1.8 object indexes, startup markers and per-object outcomes
//! (REVAMP_PLAN §5.3, §14 WP1.8 acceptance).
mod support;

use rand::{Rng, SeedableRng, rngs::StdRng};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use support::*;
use tkfs::{
    core::{
        objects::{AVAILABLE, CORRUPT, MISSING},
        transfer::{MAX_PAGE_PARTS, PartOutcome},
        *,
    },
    runtime::{self, Engine},
};

// ---------------------------------------------------------------- legacy oracles

/// The 6298d0b availability rule: a regular file whose length equals the known
/// size (`object_sizes`, else the file itself).
fn legacy_available(store: &Store, object: &str) -> bool {
    object.len() == 64
        && object.bytes().all(|b| b.is_ascii_hexdigit())
        && fs::metadata(store.root.join("objects").join(object)).is_ok_and(|m| {
            m.is_file()
                && store
                    .db
                    .query_row(
                        "SELECT size FROM object_sizes WHERE hash=?",
                        [object],
                        |r| r.get::<_, u64>(0),
                    )
                    .map_or(true, |size| size == m.len())
        })
}
fn legacy_shared_inventory(store: &Store) -> BTreeSet<String> {
    hashes(
        store,
        "SELECT hash FROM published_objects UNION SELECT hash FROM received_shared_objects",
    )
    .into_iter()
    .filter(|h| legacy_available(store, h))
    .collect()
}
/// The legacy wanted list, with the WP1.8 24 h demand expiry.
fn legacy_wanted(store: &Store) -> BTreeSet<String> {
    let since = time().saturating_sub(24 * 60 * 60 * 1000);
    hashes(
        store,
        &format!("SELECT hash FROM object_requests WHERE requested_ms>={since} ORDER BY requested_ms LIMIT 1024"),
    )
    .into_iter()
    .filter(|h| !legacy_available(store, h))
    .collect()
}
/// `(hash, event, shared)` of every committed event, from the journal.
fn legacy_event_objects(store: &Store) -> BTreeSet<(String, String, bool)> {
    store
        .events()
        .unwrap()
        .into_iter()
        .flat_map(|e| {
            e.objects()
                .into_iter()
                .map(move |h| (h, e.id.clone(), e.branch.shared))
        })
        .collect()
}
fn legacy_staged_objects(store: &Store) -> BTreeSet<(String, String)> {
    let payloads: Vec<String> = store
        .db
        .prepare("SELECT payload FROM incoming_events")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    payloads
        .iter()
        .map(|p| serde_json::from_str::<Event>(p).unwrap())
        .flat_map(|e| e.objects().into_iter().map(move |h| (h, e.id.clone())))
        .collect()
}
/// The 6298d0b `content_status` counters and item paths (stat per object).
fn legacy_content(store: &Store) -> Value {
    let objects: BTreeSet<_> = store
        .events()
        .unwrap()
        .iter()
        .filter(|e| e.branch.shared)
        .flat_map(Event::objects)
        .collect();
    let available = legacy_shared_inventory(store);
    let projection = store.projection(&store.active).unwrap();
    let current: BTreeSet<_> = projection
        .entries
        .values()
        .filter(|e| e.alive)
        .filter_map(|e| e.content.clone())
        .collect();
    let items: Vec<Value> = projection
        .entries
        .values()
        .filter(|e| e.alive)
        .filter_map(|e| e.content.as_ref().map(|object| (e, object)))
        .filter(|(_, h)| !legacy_available(store, h))
        .take(20)
        .map(|(entry, object)| {
            let mut path = vec![entry.name.clone()];
            let mut parent = &entry.parent;
            while parent != ROOT {
                let Some(e) = projection.entries.get(parent) else {
                    break;
                };
                path.push(e.name.clone());
                parent = &e.parent;
            }
            path.reverse();
            json!({"object": object, "path": path.join("/")})
        })
        .collect();
    let present = current
        .iter()
        .filter(|h| legacy_available(store, h))
        .count();
    json!({"items": items, "current_objects": current.len(), "current_available": present,
        "current_missing": current.len() - present, "shared_objects": objects.len(),
        "shared_available": objects.intersection(&available).count(),
        "complete": objects.is_subset(&available), "requested": legacy_wanted(store).len()})
}
fn hashes(store: &Store, sql: &str) -> BTreeSet<String> {
    store
        .db
        .prepare(sql)
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}
fn indexed_event_objects(store: &Store) -> BTreeSet<(String, String, bool)> {
    store
        .db
        .prepare("SELECT hash,event,shared FROM event_objects")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}
fn indexed_staged_objects(store: &Store) -> BTreeSet<(String, String)> {
    store
        .db
        .prepare("SELECT hash,event FROM staged_objects")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}
fn availability_state(store: &Store, object: &str) -> Option<i64> {
    store
        .db
        .query_row(
            "SELECT state FROM object_availability WHERE hash=?",
            [object],
            |r| r.get(0),
        )
        .ok()
}
fn requested(store: &Store, object: &str) -> bool {
    store
        .db
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM object_requests WHERE hash=?)",
            [object],
            |r| r.get(0),
        )
        .unwrap()
}

/// Every index equals its legacy scan on `store`.
fn assert_indexes_match(store: &Store, label: &str) {
    assert_eq!(
        indexed_event_objects(store),
        legacy_event_objects(store),
        "{label}: event_objects"
    );
    assert_eq!(
        indexed_staged_objects(store),
        legacy_staged_objects(store),
        "{label}: staged_objects"
    );
    assert_eq!(
        store.shared_object_inventory().unwrap(),
        legacy_shared_inventory(store),
        "{label}: shared inventory"
    );
    assert_eq!(
        store.wanted_objects().unwrap(),
        legacy_wanted(store),
        "{label}: wanted"
    );
    let status = store.content_status().unwrap();
    let legacy = legacy_content(store);
    for key in [
        "current_objects",
        "current_available",
        "current_missing",
        "shared_objects",
        "shared_available",
        "complete",
        "requested",
    ] {
        assert_eq!(status[key], legacy[key], "{label}: content {key}");
    }
    let items: Vec<Value> = status["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| json!({"object": item["object"], "path": item["path"]}))
        .collect();
    assert_eq!(json!(items), legacy["items"], "{label}: content items");
    let counters = store.content_counters().unwrap();
    for (key, value) in [
        ("current_objects", counters.current_objects),
        ("current_available", counters.current_available),
        ("current_missing", counters.current_missing),
        ("shared_objects", counters.shared_objects),
        ("shared_available", counters.shared_available),
        ("downloading", counters.downloading),
        ("partial_bytes", counters.partial_bytes),
        ("requested", counters.requested),
    ] {
        assert_eq!(status[key], value, "{label}: content_counters {key}");
    }
    assert_eq!(status["complete"], counters.complete, "{label}: complete");
    let committed: BTreeSet<_> = legacy_event_objects(store)
        .into_iter()
        .filter(|(_, _, shared)| *shared)
        .map(|(h, _, _)| h)
        .collect();
    let staged_valid: BTreeSet<String> = {
        let valid: BTreeSet<String> =
            hashes(store, "SELECT id FROM incoming_events WHERE error IS NULL");
        legacy_staged_objects(store)
            .into_iter()
            .filter(|(_, e)| valid.contains(e))
            .map(|(h, _)| h)
            .collect()
    };
    for (object, _, _) in legacy_event_objects(store) {
        assert_eq!(
            store.objects_referenced_committed(&object).unwrap(),
            committed.contains(&object),
            "{label}: referenced_committed {object}"
        );
    }
    for (object, _) in legacy_staged_objects(store) {
        assert_eq!(
            store.objects_referenced_staged(&object).unwrap(),
            staged_valid.contains(&object),
            "{label}: referenced_staged {object}"
        );
    }
}

/// After an open, every referenced object has a row and the point lookup
/// equals the legacy rule.
fn assert_point_lookups_match(store: &Store, label: &str) {
    for (object, _, _) in legacy_event_objects(store) {
        if object.len() != 64 {
            continue;
        }
        assert!(
            availability_state(store, &object).is_some(),
            "{label}: no availability row for {object}"
        );
        assert_eq!(
            store.object_available(&object),
            legacy_available(store, &object),
            "{label}: object_available {object}"
        );
    }
}

// ---------------------------------------------------------------- property test

fn write_file(store: &mut Store, name: &str, bytes: &[u8]) {
    match store.lookup(name) {
        Ok(entry) => {
            store
                .write_revision(&entry.id, entry.revisions.get("content").cloned(), bytes)
                .unwrap();
        }
        Err(_) => {
            let entry = store.create(name, "file").unwrap();
            store
                .write_revision(&entry.id, entry.revisions.get("content").cloned(), bytes)
                .unwrap();
        }
    }
}
fn random_bytes(rng: &mut StdRng) -> Vec<u8> {
    let len = rng.gen_range(0..48);
    (0..len).map(|_| rng.r#gen()).collect()
}
/// Object files, except the empty object: every `create` re-verifies that
/// one, so damaging it would only make later creates fail (as in 6298d0b).
fn object_files(store: &Store) -> Vec<String> {
    let empty = hash(b"");
    let mut files: Vec<String> = fs::read_dir(store.root.join("objects"))
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.len() == 64 && *n != empty)
        .collect();
    files.sort();
    files
}
/// A remote event that creates `name` with `content` but an invalid kind: it
/// is staged, then quarantined (`INVALID_KIND`).
fn quarantined_event(author: &Store, name: &str, content: &str) -> Event {
    let branch = author.branch(&author.active.clone()).unwrap();
    let entity = id();
    let change = |field: &str, value: Value| Change {
        entity: entity.clone(),
        field: field.into(),
        parents: vec![],
        value,
    };
    let mut event = author.event(
        &branch,
        vec![
            change("kind", json!("symlink")),
            change("location", json!({"parent": ROOT, "name": name})),
            change("alive", json!(true)),
            change("content", json!(content)),
        ],
    );
    event.dependencies.clear();
    event
}

fn run_seed(seed: u64) {
    let mut rng = StdRng::seed_from_u64(seed);
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = Store::open(&dir.path().join("a"), Some(&repo)).unwrap();
    let mut b = Store::open(&dir.path().join("b"), Some(&repo)).unwrap();
    let allowed = devices([&a, &b]);
    let mut private: Option<String> = None;
    let mut files = 0;
    for _ in 0..rng.gen_range(6..14) {
        match rng.gen_range(0..12) {
            0 | 1 => {
                files += 1;
                let bytes = random_bytes(&mut rng);
                write_file(&mut a, &format!("f{files}"), &bytes);
            }
            2 => {
                let bytes = random_bytes(&mut rng);
                write_file(&mut b, &format!("b{}", rng.gen_range(0..3)), &bytes);
            }
            3 => {
                // Private bytes on A.
                let branch = match private.clone() {
                    Some(branch) => a.branch(&branch).unwrap(),
                    None => a.fork("private", false).unwrap(),
                };
                private = Some(branch.id.clone());
                let main = a.active.clone();
                a.checkout(&branch.id).unwrap();
                let bytes = random_bytes(&mut rng);
                write_file(&mut a, &format!("p{}", rng.gen_range(0..3)), &bytes);
                a.checkout(&main).unwrap();
            }
            4 => {
                let _ = b.receive(metadata_bundle(&a, &b).unwrap(), &allowed);
            }
            5 => {
                let _ = sync_into(&a, &mut b, &allowed);
            }
            6 => {
                let _ = sync_into(&b, &mut a, &allowed);
            }
            7 | 8 => {
                // Out-of-band delete (7) or size change (8) on either replica.
                let store = if rng.r#gen() { &a } else { &b };
                let objects = object_files(store);
                if !objects.is_empty() {
                    let object = &objects[rng.gen_range(0..objects.len())];
                    let path = store.root.join("objects").join(object);
                    if rng.gen_bool(0.5) {
                        fs::remove_file(&path).unwrap();
                    } else {
                        // Replaced with a different length (same-length
                        // rewrites are covered by the startup-marker tests).
                        // The store never rewrites a CAS file in place; an
                        // in-place rewrite by another program is seen by the
                        // next listing, read, scrub or startup (notes).
                        let len = fs::metadata(&path).unwrap().len() as usize;
                        fs::remove_file(&path).unwrap();
                        fs::write(&path, vec![0u8; len + 1]).unwrap();
                    }
                }
            }
            9 => {
                let referenced: Vec<String> = legacy_event_objects(&b)
                    .into_iter()
                    .map(|(h, _, _)| h)
                    .collect();
                if !referenced.is_empty() {
                    let object = &referenced[rng.gen_range(0..referenced.len())];
                    b.request_object(object).unwrap();
                }
            }
            10 => {
                // Object parts from A to B (possibly unauthorized ones).
                let empty = Bundle {
                    repo: repo.clone(),
                    branches: vec![],
                    events: vec![],
                    objects: BTreeMap::new(),
                };
                let parts = a
                    .shared_parts(
                        &empty,
                        &b.shared_object_inventory().unwrap(),
                        &b.partial_objects().unwrap(),
                        rng.gen_range(2_000..64_000),
                    )
                    .unwrap();
                let _ = b.receive_parts(parts, &[], &allowed);
            }
            _ => {
                // Staged rows on B: one waiting for missing parents and one
                // quarantined event referencing content.
                let missing: Vec<Event> = metadata_bundle(&a, &b).unwrap().events;
                if missing.len() > 1 {
                    let last = missing.last().unwrap().clone();
                    let _ = b.receive(
                        Bundle {
                            repo: repo.clone(),
                            branches: vec![],
                            events: vec![last],
                            objects: BTreeMap::new(),
                        },
                        &allowed,
                    );
                }
                let content = hash(&random_bytes(&mut rng));
                let bad = quarantined_event(&a, &format!("bad{}", rng.r#gen::<u32>()), &content);
                let _ = b.receive(
                    Bundle {
                        repo: repo.clone(),
                        branches: vec![],
                        events: vec![bad],
                        objects: BTreeMap::new(),
                    },
                    &allowed,
                );
            }
        }
    }
    assert_indexes_match(&a, &format!("seed {seed} a"));
    assert_indexes_match(&b, &format!("seed {seed} b"));
    if seed.is_multiple_of(4) {
        drop(a);
        drop(b);
        let a = Store::open_existing(&dir.path().join("a")).unwrap();
        let b = Store::open_existing(&dir.path().join("b")).unwrap();
        for (store, label) in [(&a, "a"), (&b, "b")] {
            let label = format!("seed {seed} reopened {label}");
            assert_indexes_match(store, &label);
            assert_point_lookups_match(store, &label);
        }
    }
}

fn run_seeds(range: std::ops::Range<u64>) {
    for seed in range {
        run_seed(seed);
    }
}

// Indexes equal the legacy scans on random two-replica histories (200 seeds
// in eight parallel tests): shared, private, staged and quarantined
// references, metadata-only replicas, out-of-band deletes and size changes,
// demand and partial transfers.
#[test]
fn indexes_equal_legacy_scans_seeds_000_024() {
    run_seeds(0..25);
}
#[test]
fn indexes_equal_legacy_scans_seeds_025_049() {
    run_seeds(25..50);
}
#[test]
fn indexes_equal_legacy_scans_seeds_050_074() {
    run_seeds(50..75);
}
#[test]
fn indexes_equal_legacy_scans_seeds_075_099() {
    run_seeds(75..100);
}
#[test]
fn indexes_equal_legacy_scans_seeds_100_124() {
    run_seeds(100..125);
}
#[test]
fn indexes_equal_legacy_scans_seeds_125_149() {
    run_seeds(125..150);
}
#[test]
fn indexes_equal_legacy_scans_seeds_150_174() {
    run_seeds(150..175);
}
#[test]
fn indexes_equal_legacy_scans_seeds_175_199() {
    run_seeds(175..200);
}

// ---------------------------------------------------------------- startup markers

/// `count` objects written the way the store installs them (with the
/// install mtime stamp, `objects::STAMP_SECS`) but without the per-object
/// fsync and index row, referenced by one create event each.
fn stamped_objects(store: &mut Store, count: usize) -> Vec<String> {
    let parent = store.create("objs", "directory").unwrap().id;
    let stamp = std::time::UNIX_EPOCH + Duration::from_secs(tkfs::core::objects::STAMP_SECS);
    let mut files = vec![];
    let mut hashes = vec![];
    for i in 0..count {
        let bytes = format!("object {i} of {count}").into_bytes();
        let object = hash(&bytes);
        let file = fs::File::create(store.root.join("objects").join(&object)).unwrap();
        std::io::Write::write_all(&mut &file, &bytes).unwrap();
        file.set_modified(stamp).unwrap();
        files.push(NewFile {
            parent: parent.clone(),
            name: format!("o{i:05}"),
            content: object.clone(),
            size: bytes.len() as u64,
        });
        hashes.push(object);
    }
    direct_creates(store, &files).unwrap();
    // One real commit seals the startup snapshot.
    store.create("sealed", "directory").unwrap();
    hashes
}

/// Startup with 2K objects performs zero hashes, and detects deleted,
/// size-changed and rewritten objects (hashing only the changed ones).
#[test]
fn startup_with_2k_objects_hashes_nothing_and_detects_changes() {
    let dir = tempfile::tempdir().unwrap();
    let hashes = {
        let mut store = Store::open(dir.path(), Some("markers")).unwrap();
        stamped_objects(&mut store, 2000)
    };
    // The first open indexes (hashes) objects it has never seen, once.
    let store = Store::open_existing(dir.path()).unwrap();
    assert_eq!(store.object_hashes(), 2000);
    drop(store);
    let store = Store::open_existing(dir.path()).unwrap();
    assert_eq!(store.object_hashes(), 0, "clean restart re-hashed objects");
    assert!(hashes.iter().all(|h| store.object_available(h)));
    assert_eq!(store.content_counters().unwrap().current_missing, 0);
    drop(store);

    let objects = dir.path().join("objects");
    let (deleted, resized, rewritten) = (&hashes[10], &hashes[20], &hashes[30]);
    fs::remove_file(objects.join(deleted)).unwrap();
    fs::write(objects.join(resized), b"a different length").unwrap();
    let len = fs::metadata(objects.join(rewritten)).unwrap().len() as usize;
    fs::write(objects.join(rewritten), vec![b'x'; len]).unwrap();

    let store = Store::open_existing(dir.path()).unwrap();
    assert_eq!(
        store.object_hashes(),
        2,
        "only the two changed files are hashed"
    );
    // Deleted: missing on open, and its download is queued.
    assert_eq!(availability_state(&store, deleted), Some(MISSING));
    assert!(!store.object_available(deleted));
    assert!(requested(&store, deleted));
    assert!(store.wanted_objects().unwrap().contains(deleted));
    // Changed bytes: quarantined (renamed aside), corrupt, queued again.
    for object in [resized, rewritten] {
        assert_eq!(availability_state(&store, object), Some(CORRUPT));
        assert!(!objects.join(object).exists());
        assert!(requested(&store, object));
        assert!(store.object_backoff(object).unwrap().is_some());
    }
    let quarantined = fs::read_dir(&objects)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().contains(".corrupt-"))
        .count();
    assert_eq!(quarantined, 2);
    let counters = store.content_counters().unwrap();
    assert_eq!(counters.current_missing, 3);
    assert!(!counters.complete);
    drop(store);

    // The next clean restart hashes nothing again.
    let store = Store::open_existing(dir.path()).unwrap();
    assert_eq!(store.object_hashes(), 0);
    drop(store);
    // `Full` hashes every present object; `Lazy` hashes nothing.
    let full = Store::open_existing_with(
        dir.path(),
        OpenOptions {
            defer_activation: false,
            verify: VerifyMode::Full,
        },
    )
    .unwrap();
    assert!(full.object_hashes() >= 1997, "{}", full.object_hashes());
    drop(full);
    let lazy = Store::open_existing_with(
        dir.path(),
        OpenOptions {
            defer_activation: false,
            verify: VerifyMode::Lazy,
        },
    )
    .unwrap();
    assert_eq!(lazy.object_hashes(), 0);
}

/// The one-time pass after an upgrade: a legacy (6298d0b) state gets every
/// index and marker, then restarts without hashing.
#[test]
fn legacy_state_upgrades_once_then_restarts_without_hashing() {
    let legacy = copy_legacy_worker();
    let store = Store::open_existing(&legacy.state).unwrap();
    for marker in ["availability_v1", "availability_v2", "event_objects_v1"] {
        let value: Option<String> = store
            .db
            .query_row("SELECT value FROM config WHERE key=?", [marker], |r| {
                r.get(0)
            })
            .ok();
        assert_eq!(value.as_deref(), Some("1"), "{marker}");
    }
    assert_indexes_match(&store, "legacy");
    assert_point_lookups_match(&store, "legacy");
    drop(store);
    // Rows recorded during the upgrade can be racy (files written moments
    // before); after one more verification they are trusted.
    std::thread::sleep(Duration::from_millis(3100));
    drop(Store::open_existing(&legacy.state).unwrap());
    let store = Store::open_existing(&legacy.state).unwrap();
    assert_eq!(store.object_hashes(), 0);
}

/// An interrupted `event_objects_v1` backfill resumes from its cursor.
#[test]
fn event_objects_backfill_is_chunked_and_resumable() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), Some("backfill")).unwrap();
    for i in 0..30 {
        write_file(&mut store, &format!("f{i}"), format!("v{i}").as_bytes());
    }
    let expected = legacy_event_objects(&store);
    // Simulate an older state: no index rows, no marker, and a cursor half-way.
    let middle: i64 = store
        .db
        .query_row(
            "SELECT rowid FROM events ORDER BY rowid LIMIT 1 OFFSET 20",
            [],
            |r| r.get(0),
        )
        .unwrap();
    store
        .db
        .execute_batch(&format!(
            "DELETE FROM event_objects WHERE event IN (SELECT id FROM events WHERE rowid>{middle});
             DELETE FROM config WHERE key='event_objects_v1';
             INSERT INTO config VALUES('event_objects_v1:events','{middle}');"
        ))
        .unwrap();
    drop(store);
    let store = Store::open_existing(dir.path()).unwrap();
    assert_eq!(indexed_event_objects(&store), expected);
    let cursor: Option<String> = store
        .db
        .query_row(
            "SELECT value FROM config WHERE key='event_objects_v1:events'",
            [],
            |r| r.get(0),
        )
        .ok();
    assert_eq!(cursor, None, "cursor removed once complete");
}

// ---------------------------------------------------------------- per-object outcomes

/// Author `count` shared objects on A, give B the metadata only.
fn author_and_receiver(dir: &Path, count: usize) -> (Store, Store, Vec<(String, Vec<u8>)>) {
    let repo = id();
    let mut a = Store::open(&dir.join("a"), Some(&repo)).unwrap();
    let mut b = Store::open(&dir.join("b"), Some(&repo)).unwrap();
    let mut objects = vec![];
    for i in 0..count {
        let bytes = seeded_bytes(200 + i as u64, 32 * 1024);
        write_file(&mut a, &format!("part-{i}.bin"), &bytes);
        objects.push((hash(&bytes), bytes));
    }
    let allowed = devices([&a, &b]);
    b.receive(metadata_bundle(&a, &b).unwrap(), &allowed)
        .unwrap();
    objects.sort();
    (a, b, objects)
}
fn whole(object: &str, bytes: &[u8]) -> (String, ObjectPart) {
    (
        object.to_owned(),
        ObjectPart {
            offset: 0,
            total: bytes.len() as u64,
            hex: hex::encode(bytes),
        },
    )
}

/// A corrupt part mid-page keeps the earlier (and later) completed objects
/// registered; only the bad object fails and backs off.
#[test]
fn corrupt_part_mid_page_keeps_completed_objects() {
    let dir = tempfile::tempdir().unwrap();
    let (a, mut b, objects) = author_and_receiver(dir.path(), 3);
    let allowed = devices([&a, &b]);
    let mut parts: BTreeMap<_, _> = objects.iter().map(|(h, b)| whole(h, b)).collect();
    let mut bad = objects[1].1.clone();
    bad[1000] ^= 0xff;
    parts.insert(objects[1].0.clone(), whole(&objects[1].0, &bad).1);
    let outcomes = b
        .receive_parts_outcomes(parts.clone(), &[], &allowed, None)
        .unwrap();
    assert!(
        matches!(&outcomes[0], PartOutcome::Completed { object, .. } if *object == objects[0].0)
    );
    assert!(
        matches!(&outcomes[1], PartOutcome::Failed { object, error } if *object == objects[1].0 && error.code == "CORRUPT_OBJECT")
    );
    assert!(
        matches!(&outcomes[2], PartOutcome::Completed { object, .. } if *object == objects[2].0)
    );
    // The legacy entry point: registers both completed objects, then fails.
    let error = b.receive_parts(parts, &[], &allowed).unwrap_err();
    assert!(error.to_string().contains("CORRUPT_OBJECT"), "{error:#}");
    let shared = b.shared_object_inventory().unwrap();
    assert!(shared.contains(&objects[0].0));
    assert!(shared.contains(&objects[2].0));
    assert!(!shared.contains(&objects[1].0));
    assert!(!b.object_available(&objects[1].0));
    assert!(b.object_backoff(&objects[1].0).unwrap().is_some());
    assert!(b.object_backoff(&objects[0].0).unwrap().is_none());
    // A good copy later completes and clears the backoff.
    b.receive_parts(
        BTreeMap::from([whole(&objects[1].0, &objects[1].1)]),
        &[],
        &allowed,
    )
    .unwrap();
    assert!(b.shared_object_inventory().unwrap().contains(&objects[1].0));
    assert!(b.object_backoff(&objects[1].0).unwrap().is_none());
}

/// A duplicate part for a shareable, available object is skipped without a
/// re-hash; a part beyond the spool never creates one.
#[test]
fn duplicate_part_is_not_rehashed_and_gaps_create_no_spool() {
    let dir = tempfile::tempdir().unwrap();
    let (a, mut b, objects) = author_and_receiver(dir.path(), 2);
    let allowed = devices([&a, &b]);
    let (object, bytes) = &objects[0];
    b.receive_parts(BTreeMap::from([whole(object, bytes)]), &[], &allowed)
        .unwrap();
    let before = b.object_hashes();
    let outcomes = b
        .receive_parts_outcomes(BTreeMap::from([whole(object, bytes)]), &[], &allowed, None)
        .unwrap();
    assert_eq!(
        outcomes,
        vec![PartOutcome::Skipped {
            object: object.clone()
        }]
    );
    assert_eq!(b.object_hashes(), before, "duplicate part re-hashed");
    // Offset > 0 with no spool: skipped, and no spool appears.
    let (other, other_bytes) = &objects[1];
    let part = ObjectPart {
        offset: 1000,
        total: other_bytes.len() as u64,
        hex: hex::encode(&other_bytes[1000..2000]),
    };
    let outcomes = b
        .receive_parts_outcomes(BTreeMap::from([(other.clone(), part)]), &[], &allowed, None)
        .unwrap();
    assert!(matches!(&outcomes[0], PartOutcome::Skipped { .. }));
    assert!(!b.root.join("incoming-objects").join(other).exists());
    assert!(b.partial_objects().unwrap().is_empty());
    // Offset 0 appends; the next part completes.
    let first = ObjectPart {
        offset: 0,
        total: other_bytes.len() as u64,
        hex: hex::encode(&other_bytes[..1000]),
    };
    let outcomes = b
        .receive_parts_outcomes(
            BTreeMap::from([(other.clone(), first)]),
            &[],
            &allowed,
            None,
        )
        .unwrap();
    assert_eq!(
        outcomes,
        vec![PartOutcome::Appended {
            object: other.clone(),
            offset: 1000
        }]
    );
}

/// A physically present but private object is transferred again, verified,
/// and only then registered as shareable (I4).
#[test]
fn private_present_object_is_retransferred_verified_then_registered() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b, objects) = author_and_receiver(dir.path(), 1);
    let allowed = devices([&a, &b]);
    let (object, bytes) = &objects[0];
    // B holds the same bytes privately (local CAS, no grant).
    assert_eq!(&b.put_object(bytes).unwrap(), object);
    assert!(b.object_available(object));
    assert!(!b.shared_object_inventory().unwrap().contains(object));
    let before = b.object_hashes();
    let outcomes = b
        .receive_parts_outcomes(BTreeMap::from([whole(object, bytes)]), &[], &allowed, None)
        .unwrap();
    assert!(
        matches!(&outcomes[0], PartOutcome::Completed { .. }),
        "{outcomes:?}"
    );
    assert!(b.object_hashes() > before, "transfer not verified");
    assert!(!b.shared_object_inventory().unwrap().contains(object));
    b.register_received(&[(object.clone(), bytes.len() as u64)])
        .unwrap();
    assert!(b.shared_object_inventory().unwrap().contains(object));
}

/// Object failures back off: the sender skips an object inside its backoff,
/// and an unreadable object never fails the page.
#[test]
fn sender_skips_objects_in_backoff_and_unreadable_objects() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b, objects) = author_and_receiver(dir.path(), 3);
    let empty = Bundle {
        repo: a.repo.clone(),
        branches: vec![],
        events: vec![],
        objects: BTreeMap::new(),
    };
    let send = |a: &Store| {
        a.shared_parts(
            &empty,
            &b.shared_object_inventory().unwrap(),
            &BTreeMap::new(),
            4 * 1024 * 1024,
        )
        .unwrap()
    };
    // The three objects (and the empty object of each create).
    let sent = |a: &Store| -> BTreeSet<String> {
        send(a)
            .into_keys()
            .filter(|h| objects.iter().any(|(o, _)| o == h))
            .collect()
    };
    let all: BTreeSet<String> = objects.iter().map(|(h, _)| h.clone()).collect();
    assert_eq!(sent(&a), all);
    let wire = tkfs::model::errors::ErrorWire::coded("OBJECT_IO_ERROR", "test");
    let retry = a.record_object_failure(&objects[0].0, &wire).unwrap();
    assert!(retry >= time() + 20_000, "objects back off 30 s first");
    assert_eq!(a.object_backoff(&objects[0].0).unwrap(), Some(retry));
    assert_eq!(
        sent(&a),
        BTreeSet::from([objects[1].0.clone(), objects[2].0.clone()])
    );
    // An object deleted behind the store's back is no longer offered.
    fs::remove_file(a.root.join("objects").join(&objects[1].0)).unwrap();
    assert_eq!(sent(&a), BTreeSet::from([objects[2].0.clone()]));
    assert!(!a.shared_object_inventory().unwrap().contains(&objects[1].0));
}

/// The page cliff: thousands of tiny pending objects produce pages of at most
/// `MAX_PAGE_PARTS` parts, below the receivers' 10,000-part bound.
#[test]
fn tiny_pending_objects_produce_valid_pages() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = Engine::new(Store::initialize(&dir.path().join("a"), &repo, &id()).unwrap());
    let receiver = id();
    // Enough tiny objects for the cap to bind (the 12K end-to-end run is
    // `qual_wp1_8_objects::tiny_pending_objects_12k`).
    let count = MAX_PAGE_PARTS + 100;
    small_objects_in(&mut a.store, "tiny", count, 40, 5).unwrap();
    // B has committed every event (as after a metadata round) and holds no bytes.
    a.store
        .db
        .execute(
            "INSERT INTO peer_known SELECT ?,id FROM events",
            [&receiver],
        )
        .unwrap();
    let a_root = a.store.root.clone();
    let a_device = a.store.device.clone();
    let a = Arc::new(Mutex::new(a));
    let info = runtime::start_rpc(a.clone(), &a_root, None).unwrap();
    let route =
        json!({"peer": id(), "repo": repo, "local_replica": a_device, "remote_replica": receiver});
    let lane = json!({"index": 0, "count": 1});
    let inventory = json!({"format": 1, "repo": repo, "sender": receiver, "receiver": a_device,
        "request": id(), "reply_to": null, "lane": lane, "known_objects": [], "partial_objects": {},
        "wanted_objects": [], "object_parts": {}});
    let page = runtime::rpc(
        &info,
        &id(),
        json!({"op":"published-bulk-offer","route":route,"lane":lane,"inventory":inventory}),
    )
    .unwrap();
    let parts = page["object_parts"].as_object().unwrap();
    assert_eq!(parts.len(), MAX_PAGE_PARTS);
    let raw: usize = parts
        .values()
        .map(|p| p["hex"].as_str().unwrap().len() / 2)
        .sum();
    assert!(raw <= 2 * 1024 * 1024);
    let page: tkfs::bulk_sync::Page = serde_json::from_value(page).unwrap();
    assert!(serde_json::to_vec(&page.object_parts).unwrap().len() < 4 * 1024 * 1024);
    a.lock()
        .unwrap()
        .stopping
        .store(true, std::sync::atomic::Ordering::Relaxed);
}

// ---------------------------------------------------------------- read-path heal and scrub

/// Bytes corrupted in place are found by the scrubber, quarantined, and queued.
#[test]
fn scrub_step_verifies_and_quarantines() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), Some("scrub")).unwrap();
    for i in 0..4 {
        write_file(
            &mut store,
            &format!("f{i}"),
            format!("scrubbed {i}").as_bytes(),
        );
    }
    let victim = store.lookup("f2").unwrap().content.unwrap();
    let path = store.root.join("objects").join(&victim);
    let len = fs::metadata(&path).unwrap().len() as usize;
    fs::write(&path, vec![b'z'; len]).unwrap();
    let outcome = store.scrub_step(u64::MAX).unwrap();
    assert_eq!(outcome.corrupt, 1, "{outcome:?}");
    assert!(outcome.verified >= 4, "{outcome:?}");
    assert_eq!(availability_state(&store, &victim), Some(CORRUPT));
    assert!(!path.exists());
    assert!(store.wanted_objects().unwrap().contains(&victim));
    assert_eq!(store.content_counters().unwrap().current_missing, 1);
    // A zero budget does nothing.
    assert_eq!(store.scrub_step(0).unwrap().bytes, 0);
}

/// `object_available_checked` heals the index to what the filesystem shows.
#[test]
fn checked_lookup_heals_the_index() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), Some("heal")).unwrap();
    write_file(&mut store, "f", b"healing");
    let object = hash(b"healing");
    assert_eq!(availability_state(&store, &object), Some(AVAILABLE));
    let path = store.root.join("objects").join(&object);
    fs::remove_file(&path).unwrap();
    assert!(
        store.object_available(&object),
        "point lookup trusts the index"
    );
    assert!(!store.object_available_checked(&object));
    assert_eq!(availability_state(&store, &object), Some(MISSING));
    assert!(!store.object_available(&object));
    fs::write(&path, b"healing").unwrap();
    assert!(store.object_available_checked(&object));
    assert!(store.object_available(&object));
}

// ---------------------------------------------------------------- crash between install and register

/// A worker that crashes after installing an object but before registering it
/// (`TKFS_FAULT=object_installed`) leaves the object unshareable; it is safely
/// transferred again, verified and registered.
#[test]
fn crash_between_install_and_register_retransfers() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b, objects) = author_and_receiver(dir.path(), 1);
    let (object, bytes) = objects[0].clone();
    let (repo, a_device, b_device) = (a.repo.clone(), a.device.clone(), b.device.clone());
    let b_state = b.root.clone();
    drop(a);
    drop(b);
    let route =
        json!({"peer": id(), "repo": repo, "local_replica": b_device, "remote_replica": a_device});
    let lane = json!({"index": 0, "count": 1});
    let page = json!({"format": 1, "repo": repo, "sender": a_device, "receiver": b_device,
        "request": id(), "reply_to": null, "lane": lane, "known_objects": [], "partial_objects": {},
        "wanted_objects": [], "object_parts": {object.clone(): {"offset": 0, "total": bytes.len(),
        "hex": hex::encode(&bytes)}}});
    let payload = json!({"op":"published-bulk-exchange","route":route,"lane":lane,"page":page});

    let mut worker =
        WorkerProc::spawn(&b_state, &[], &[("TKFS_FAULT", "object_installed")]).unwrap();
    worker.wait_ready(Duration::from_secs(60)).unwrap();
    assert!(
        worker.rpc(payload.clone()).is_err(),
        "the worker must crash"
    );
    let status = worker
        .wait_exit(Duration::from_secs(30))
        .expect("worker exit");
    assert_eq!(status.code(), Some(86));
    drop(worker);
    assert!(b_state.join("objects").join(&object).is_file(), "installed");

    {
        let b = Store::open_existing(&b_state).unwrap();
        // Installed bytes, but no registration: not shareable, still wanted by the
        // sender's offer (it is not in B's known objects).
        assert!(!b.shared_object_inventory().unwrap().contains(&object));
        assert!(b.object_available(&object));
    }

    let mut worker = WorkerProc::spawn(&b_state, &[], &[]).unwrap();
    worker.wait_ready(Duration::from_secs(60)).unwrap();
    let mut page = page;
    page["request"] = json!(id());
    let reply = worker
        .rpc(json!({"op":"published-bulk-exchange","route":route,"lane":lane,"page":page}))
        .unwrap();
    assert!(
        reply["known_objects"]
            .as_array()
            .unwrap()
            .iter()
            .any(|h| h == &json!(object)),
        "{reply}"
    );
    worker.stop().unwrap();
    let b = Store::open_existing(&b_state).unwrap();
    assert!(b.shared_object_inventory().unwrap().contains(&object));
    assert_eq!(b.read_object(&object).unwrap(), bytes);
}

/// Once a page's receive budget is spent, the remaining parts are skipped
/// untouched (the sender offers them again), and at least one part is received.
#[test]
fn receive_budget_skips_remaining_parts() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b, objects) = author_and_receiver(dir.path(), 3);
    let allowed = devices([&a, &b]);
    let parts: BTreeMap<_, _> = objects.iter().map(|(h, b)| whole(h, b)).collect();
    let spent = std::time::Instant::now();
    let outcomes = b
        .receive_parts_until(parts, &[], &allowed, None, Some(spent))
        .unwrap();
    assert!(
        matches!(&outcomes[0], PartOutcome::Completed { .. }),
        "{outcomes:?}"
    );
    assert!(
        outcomes[1..]
            .iter()
            .all(|o| matches!(o, PartOutcome::Skipped { .. })),
        "{outcomes:?}"
    );
    for (object, _) in &objects[1..] {
        assert!(!b.root.join("incoming-objects").join(object).exists());
        assert!(!b.root.join("objects").join(object).exists());
    }
}

/// Through the worker's bulk lane (`published-bulk-exchange`): a corrupt part
/// mid-page still fails the page, but the completed objects are registered and
/// the corrupt one backs off.
#[test]
fn bulk_lane_registers_completed_objects_when_a_part_fails() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b, objects) = author_and_receiver(dir.path(), 3);
    let (repo, a_device, b_device) = (a.repo.clone(), a.device.clone(), b.device.clone());
    let b_root = b.root.clone();
    let b = Arc::new(Mutex::new(Engine::new(b)));
    let info = runtime::start_rpc(b.clone(), &b_root, None).unwrap();
    let mut parts = serde_json::Map::new();
    for (i, (object, bytes)) in objects.iter().enumerate() {
        let mut bytes = bytes.clone();
        if i == 1 {
            bytes[7] ^= 0x55;
        }
        parts.insert(
            object.clone(),
            json!({"offset": 0, "total": bytes.len(), "hex": hex::encode(&bytes)}),
        );
    }
    let lane = json!({"index": 0, "count": 1});
    let route =
        json!({"peer": id(), "repo": repo, "local_replica": b_device, "remote_replica": a_device});
    let page = json!({"format": 1, "repo": repo, "sender": a_device, "receiver": b_device,
        "request": id(), "reply_to": null, "lane": lane, "known_objects": [], "partial_objects": {},
        "wanted_objects": [], "object_parts": parts});
    let error = runtime::rpc(
        &info,
        &id(),
        json!({"op":"published-bulk-exchange","route":route,"lane":lane,"page":page}),
    )
    .unwrap_err();
    assert!(error.to_string().contains("CORRUPT_OBJECT"), "{error:#}");
    let engine = b.lock().unwrap();
    let shared = engine.store.shared_object_inventory().unwrap();
    assert!(shared.contains(&objects[0].0));
    assert!(shared.contains(&objects[2].0));
    assert!(!shared.contains(&objects[1].0));
    assert!(
        engine
            .store
            .object_backoff(&objects[1].0)
            .unwrap()
            .is_some()
    );
    engine
        .stopping
        .store(true, std::sync::atomic::Ordering::Relaxed);
}

/// WP4.INT (CCR-WP4.2-5): `want_objects` keeps its scan per connection while
/// the database is unchanged as that connection sees it, and still sees every
/// change: one made through another connection (a transfer backoff
/// recorded by the objects RPC's pooled store) and one of its own.
#[test]
fn the_cached_want_scan_follows_every_change() {
    use tkfs::model::{errors::ErrorWire, protocol::LaneSpec};
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = Store::initialize(&dir.path().join("a"), &repo, &id()).unwrap();
    let mut b = Store::initialize(&dir.path().join("b"), &repo, &id()).unwrap();
    for i in 0..3 {
        write_file(
            &mut a,
            &format!("f{i}.txt"),
            format!("content {i}").as_bytes(),
        );
    }
    let allowed: BTreeSet<String> = [a.device.clone(), b.device.clone()].into();
    let mut bundle = a
        .shared_bundle(&b.shared_event_inventory().unwrap())
        .unwrap();
    causal_sort(&mut bundle.events);
    bundle.objects.clear();
    b.receive(bundle, &allowed).unwrap();
    let lane = LaneSpec { index: 0, count: 1 };
    let wanted = |s: &Store| -> BTreeSet<String> {
        s.want_objects(lane, 256, u64::MAX, &|_| false)
            .unwrap()
            .into_iter()
            .map(|item| item.hash)
            .collect()
    };
    let first = wanted(&b);
    // The three contents and the empty object every create references.
    assert_eq!(first.len(), 4, "{first:?}");
    assert_eq!(wanted(&b), first, "an unchanged store answers the same");
    // Another connection records a transfer failure: that object backs off.
    let mut objects = first.iter().cloned();
    let (backed, own) = (objects.next().unwrap(), objects.next().unwrap());
    let other = Store::open_secondary(&b.root, &b.repo, &b.device).unwrap();
    other
        .record_object_failure(&backed, &ErrorWire::coded("OBJECT_IO_ERROR", "test"))
        .unwrap();
    let after = wanted(&b);
    assert!(!after.contains(&backed), "{after:?}");
    assert_eq!(after.len(), 3);
    // This connection's own change is seen too.
    b.record_object_failure(&own, &ErrorWire::coded("OBJECT_IO_ERROR", "test"))
        .unwrap();
    let last = wanted(&b);
    assert!(!last.contains(&own) && !last.contains(&backed), "{last:?}");
    assert_eq!(last.len(), 2);
}

/// WP5.INT (CCR-WP5.2-2, and the WP5.2 review's minor on renewed demand):
/// installing or deduplicating an object and registering demand write only
/// object bookkeeping, so the handle's view stays fresh across them; before,
/// each one moved the connection's `total_changes` and the next lookup
/// reloaded the whole projection (on the engine's connection: under the
/// engine lock, once per renewed demand of a waiting read). The view still
/// follows a real change of the projection.
#[test]
fn object_bookkeeping_keeps_the_handles_view() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = Store::open(dir.path(), None).unwrap();
    for i in 0..20 {
        write_file(
            &mut s,
            &format!("f{i:02}.txt"),
            format!("bytes {i}").as_bytes(),
        );
    }
    s.lookup("f00.txt").unwrap();
    let loads = s.view_loads();
    let object = s.put_object(b"a new object").unwrap();
    s.put_object(b"a new object").unwrap();
    let mut stream: &[u8] = b"a streamed object";
    s.put_stream(&mut stream).unwrap();
    let referenced = s.lookup("f01.txt").unwrap().content.unwrap();
    s.request_object(&referenced).unwrap();
    s.request_object(&referenced).unwrap();
    s.lookup("f02.txt").unwrap();
    assert_eq!(
        s.view_loads(),
        loads,
        "an install, a dedupe or a demand reloaded the view"
    );
    assert!(requested(&s, &referenced));
    assert_eq!(availability_state(&s, &object), Some(AVAILABLE));
    // A real change is still seen.
    write_file(&mut s, "f03.txt", b"changed");
    let entry = s.lookup("f03.txt").unwrap();
    assert_eq!(
        s.read_object(entry.content.as_ref().unwrap()).unwrap(),
        b"changed"
    );
    assert!(s.lookup("f99.txt").is_err());
}
