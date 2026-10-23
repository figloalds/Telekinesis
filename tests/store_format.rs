//! Fresh selected metadata stores, nonmutating version refusal and current repair.
use rusqlite::{Connection, types::Value};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    path::Path,
    process::Command,
    sync::Arc,
    time::{Duration, Instant},
};
use tkfs::{
    content::{LocalContent, ReadContext, write::WriteJournal},
    core::{Change, Store},
    util::Cancel,
};

fn context() -> ReadContext {
    ReadContext {
        deadline: Instant::now() + Duration::from_secs(120),
        cancel: Cancel::new(),
    }
}
fn code<T>(result: anyhow::Result<T>, expected: &str) {
    let error = result.err().expect("unexpected success");
    assert_eq!(tkfs::model::errors::code_of(&error), expected, "{error:#}");
}
fn files(root: &Path) -> BTreeMap<String, String> {
    fn walk(root: &Path, path: &Path, out: &mut BTreeMap<String, String>) {
        for entry in fs::read_dir(path).unwrap() {
            let p = entry.unwrap().path();
            let name = p
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            if p.is_dir() {
                out.insert(format!("{name}/"), String::new());
                walk(root, &p, out);
            } else if !name.ends_with("-shm") {
                out.insert(name, hex::encode(Sha256::digest(fs::read(p).unwrap())));
            }
        }
    }
    let mut result = BTreeMap::new();
    walk(root, root, &mut result);
    result
}
fn rows(db: &Connection, table: &str) -> Vec<Vec<Value>> {
    let mut q = db
        .prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))
        .unwrap();
    let columns = q.column_count();
    q.query_map([], |r| (0..columns).map(|i| r.get(i)).collect())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}
fn authority(s: &Store) -> BTreeMap<String, Vec<Vec<Value>>> {
    [
        "events",
        "branches",
        "journal_sequence",
        "outbox",
        "receipts",
        "peer_known",
        "checkpoints",
        "published_objects",
        "received_shared_objects",
        "content_root_metadata",
        "content_root_outbox",
        "content_save_entities",
        "content_save_operations",
        "content_save_history",
        "content_delete_intents",
        "directory_delete_intents",
    ]
    .into_iter()
    .map(|t| (t.to_owned(), rows(&s.db, t)))
    .collect()
}
fn seed(root: &Path) -> Store {
    let mut s = Store::initialize(root, "current-repo", "current-device").unwrap();
    for i in 0..12 {
        let e = s.create(&format!("file-{i}"), "file").unwrap();
        s.write_revision(
            &e.id,
            e.revisions.get("content").cloned(),
            format!("bytes-{i}").as_bytes(),
        )
        .unwrap();
    }
    let main = s.active.clone();
    let private = s.fork("private", false).unwrap();
    s.checkout(&private.id).unwrap();
    s.create("private-file", "file").unwrap();
    s.checkout(&main).unwrap();
    s
}

#[test]
fn fresh_schema_matches_independent_golden_without_upgrade_markers_or_build_cursors() {
    let golden: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/store_v3/schema.json")).unwrap();
    assert_eq!(
        hex::encode(Sha256::digest(include_bytes!("../src/core/schema.sql"))),
        golden["schema_sql_sha256"]
    );
    let dir = tempfile::tempdir().unwrap();
    let s = Store::initialize(dir.path(), "repo", "device").unwrap();
    assert_eq!(s.repo, "repo");
    assert_eq!(s.device, "device");
    let app: i64 =
        s.db.query_row("PRAGMA application_id", [], |r| r.get(0))
            .unwrap();
    let version: i64 =
        s.db.query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
    assert_eq!(app, golden["application_id"].as_i64().unwrap());
    assert_eq!(version, 3);
    assert_eq!(version, golden["version"].as_i64().unwrap());
    let mut q=s.db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY type,name").unwrap();
    let actual:Vec<serde_json::Value>=q.query_map([],|r| Ok(serde_json::json!({"type":r.get::<_,String>(0)?,"name":r.get::<_,String>(1)?,"table":r.get::<_,String>(2)?,"sql":r.get::<_,String>(3)?}))).unwrap().collect::<Result<_,_>>().unwrap();
    assert_eq!(actual, golden["objects"].as_array().unwrap().as_slice());
    for group in ["objects", "feed", "journal"] {
        let ready: String =
            s.db.query_row(
                "SELECT value FROM config WHERE key=?",
                [format!("repair:{group}:ready")],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(ready, "1");
    }
    let old:usize=s.db.query_row("SELECT count(*) FROM config WHERE key LIKE 'schema:%' OR key LIKE 'availability_v%' OR key LIKE 'event_objects_v%' OR key LIKE 'feed_journal_v%' OR key LIKE 'snapshot_journal_branch_v%' OR key='spools_v1' OR key LIKE 'repair:%:cursor%'",[],|r|r.get(0)).unwrap();
    assert_eq!(old, 0);
    assert!(rows(&s.db, "pending_saves").is_empty());
}

#[test]
fn concurrent_fresh_initializers_share_one_committed_identity_and_complete_schema() {
    for _ in 0..4 {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("current");
        let start = Arc::new(std::sync::Barrier::new(8));
        let jobs: Vec<_> = (0..8)
            .map(|_| {
                let root = root.clone();
                let start = start.clone();
                std::thread::spawn(move || -> anyhow::Result<_> {
                    start.wait();
                    let s = Store::initialize(&root, "repo", "device")?;
                    Ok((s.repo, s.device, s.active, s.generation))
                })
            })
            .collect();
        // Join every writer before asserting or dropping the directory. SQLite
        // contention is allowed; retry it after the competing attempts have ended.
        let results: Vec<_> = jobs.into_iter().map(|job| job.join()).collect();
        let mut identities = Vec::new();
        let mut busy = 0;
        for result in results {
            match result.unwrap() {
                Ok(identity) => identities.push(identity),
                Err(error) => {
                    assert_eq!(
                        tkfs::model::errors::code_of(&error),
                        "SQLITE_BUSY",
                        "{error:#}"
                    );
                    busy += 1;
                }
            }
        }
        for _ in 0..busy {
            let s = Store::initialize(&root, "repo", "device").unwrap();
            identities.push((s.repo, s.device, s.active, s.generation));
        }
        assert_eq!(identities.len(), 8);
        assert!(identities.iter().all(|identity| identity == &identities[0]));
        println!("current initializers: 8 matching identities, {busy} bounded busy retries");
        let s = Store::open_existing(&root).unwrap();
        assert_eq!(s.repo, "repo");
        assert_eq!(s.device, "device");
        let version: i64 =
            s.db.query_row("PRAGMA user_version", [], |r| r.get(0))
                .unwrap();
        assert_eq!(version, 3);
    }
}

#[test]
fn current_restart_preserves_identity_authority_and_owned_typed_save() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = seed(dir.path());
    let provider = Arc::new(LocalContent::open(dir.path()).unwrap());
    let root = provider
        .import(&mut b"typed current bytes".as_slice(), 19, &context())
        .unwrap();
    let e = s.lookup("file-0").unwrap();
    s.db.execute("INSERT INTO config VALUES('fixture:content_v1','1')", [])
        .unwrap();
    let event = s.event(
        &s.branch(&s.active).unwrap(),
        vec![Change {
            entity: e.id.clone(),
            field: "content".into(),
            parents: e.revisions.get("content").cloned().into_iter().collect(),
            value: serde_json::json!(root),
        }],
    );
    s.commit(event, true).unwrap();
    let intent = s.begin_content_save(&e.id, &context()).unwrap();
    let journal = WriteJournal::open(dir.path()).unwrap();
    let session = journal
        .create(intent.clone(), provider.clone(), &context())
        .unwrap();
    session.write_at(0, b"saved", &context()).unwrap();
    let saved = session.checkpoint(&context()).unwrap().unwrap();
    let before = authority(&s);
    let epoch = s.feed_epoch().unwrap();
    let active = s.active.clone();
    let generation = s.generation;
    drop(session);
    drop(journal);
    drop(s);
    let s = Store::open_existing(dir.path()).unwrap();
    assert_eq!(authority(&s), before);
    assert_eq!(s.feed_epoch().unwrap(), epoch);
    assert_eq!(s.active, active);
    assert_eq!(s.generation, generation);
    let journal = WriteJournal::open(dir.path()).unwrap();
    assert_eq!(journal.pending(None, 64, &context()).unwrap(), vec![saved]);
    let recovered = journal
        .resume(intent.operation, provider, &context())
        .unwrap();
    let mut out = [0; 5];
    recovered.read_range(0, &mut out, &context()).unwrap();
    assert_eq!(&out, b"saved");
    let before = files(dir.path());
    code(
        Store::initialize(dir.path(), "wrong-repo", "current-device"),
        "STORE_IDENTITY_MISMATCH",
    );
    code(
        Store::initialize(dir.path(), "current-repo", "wrong-device"),
        "STORE_IDENTITY_MISMATCH",
    );
    assert_eq!(files(dir.path()), before);
}

#[test]
fn unsupported_metadata_headers_refuse_every_open_route_without_changing_data() {
    let headers = [
        (0, 0),
        (0x544b4d53, 0),
        (0x544b4d53, 1),
        (0x544b4d53, 2),
        (0x544b4d53, -1),
        (0x544b4d53, i32::MAX),
        (0, 1),
        (1, 1),
        (0x544b5753, 7),
    ];
    for (app, version) in headers {
        for mode in ["DELETE", "WAL"] {
            for closed in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                fs::create_dir(dir.path().join("objects")).unwrap();
                fs::create_dir(dir.path().join("incoming-objects")).unwrap();
                fs::write(
                    dir.path().join("objects").join("aa".repeat(32)),
                    b"preserved unknown object",
                )
                .unwrap();
                fs::write(
                    dir.path().join("incoming-objects").join("bb".repeat(32)),
                    b"unadvertised bytes",
                )
                .unwrap();
                let db = Connection::open(dir.path().join("metadata.sqlite")).unwrap();
                db.pragma_update(None, "journal_mode", mode).unwrap();
                db.execute_batch(
                    "CREATE TABLE preserved(data BLOB); INSERT INTO preserved VALUES(X'010203')",
                )
                .unwrap();
                db.pragma_update(None, "application_id", app).unwrap();
                db.pragma_update(None, "user_version", version).unwrap();
                let _writer = if closed {
                    drop(db);
                    None
                } else {
                    Some(db)
                };
                let before = files(dir.path());
                code(Store::open(dir.path(), None), "UNSUPPORTED_STORE_FORMAT");
                assert_eq!(files(dir.path()), before);
                code(Store::open_existing(dir.path()), "UNSUPPORTED_STORE_FORMAT");
                assert_eq!(files(dir.path()), before);
                code(
                    Store::initialize(dir.path(), "repo", "device"),
                    "UNSUPPORTED_STORE_FORMAT",
                );
                assert_eq!(files(dir.path()), before);
                code(
                    Store::open_secondary(dir.path(), "repo", "device"),
                    "UNSUPPORTED_STORE_FORMAT",
                );
                assert_eq!(files(dir.path()), before);
            }
        }
    }
    // Refusal must not invent the objects directory before inspecting old input.
    let dir = tempfile::tempdir().unwrap();
    let db = Connection::open(dir.path().join("metadata.sqlite")).unwrap();
    db.execute_batch("CREATE TABLE previous(data); INSERT INTO previous VALUES('keep')")
        .unwrap();
    let before = files(dir.path());
    code(Store::open(dir.path(), None), "UNSUPPORTED_STORE_FORMAT");
    assert_eq!(files(dir.path()), before);
    assert!(!dir.path().join("objects").exists());
    println!(
        "WAVE6_STORE_FORMAT {}",
        serde_json::json!({"unsupported_header_mode_lifetime_cases":32,"open_routes_per_case":4,"persistent_bytes_preserved":true,"no_unknown_object_or_spool_processing":true})
    );
}

#[test]
fn damaged_current_authority_is_not_reinitialized_or_recreated() {
    for key in ["repo", "device", "active", "generation", "journal_epoch"] {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::initialize(dir.path(), "repo", "device").unwrap();
        s.db.execute("DELETE FROM config WHERE key=?", [key])
            .unwrap();
        let before = files(dir.path());
        code(Store::open(dir.path(), None), "INVALID_STORE_METADATA");
        assert_eq!(files(dir.path()), before);
    }
    for table in [
        "events",
        "receipts",
        "journal_sequence",
        "content_save_operations",
        "content_delete_intents",
        "directory_delete_intents",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::initialize(dir.path(), "repo", "device").unwrap();
        s.db.execute_batch(&format!("DROP TABLE {table}")).unwrap();
        let before = files(dir.path());
        code(Store::open_existing(dir.path()), "INVALID_STORE_METADATA");
        assert_eq!(files(dir.path()), before);
        let missing: bool =
            s.db.query_row(
                "SELECT NOT EXISTS(SELECT 1 FROM sqlite_schema WHERE name=?)",
                [table],
                |r| r.get(0),
            )
            .unwrap();
        assert!(missing);
    }
}

#[test]
fn current_derived_index_repair_preserves_journal_epoch_and_acknowledgements_after_exit() {
    for (group, table, point) in [
        ("objects", "event_objects", "store.index.objects.page"),
        ("feed", "feed_journal", "store.index.feed.page"),
        ("journal", "journal_branch", "store.index.journal.page"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let s = seed(dir.path());
        s.db.execute("UPDATE outbox SET acknowledged=1 WHERE rowid%2=0", [])
            .unwrap();
        let before = authority(&s);
        let epoch = s.feed_epoch().unwrap();
        let indexed = rows(&s.db, table);
        s.db.execute_batch(&format!("DROP TABLE {table}")).unwrap();
        drop(s);
        let result = child(dir.path(), "open", point);
        assert_eq!(
            result.status.code(),
            Some(86),
            "{point}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        let db = Connection::open(dir.path().join("metadata.sqlite")).unwrap();
        let cursor: bool = db
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM config WHERE key LIKE ?)",
                [format!("repair:{group}:cursor%")],
                |r| r.get(0),
            )
            .unwrap();
        assert!(cursor, "a committed repair page has a durable cursor");
        drop(db);
        let s = Store::open_existing(dir.path()).unwrap();
        assert_eq!(authority(&s), before);
        assert_eq!(s.feed_epoch().unwrap(), epoch);
        assert_eq!(rows(&s.db, table), indexed);
        let cursors: usize =
            s.db.query_row(
                "SELECT count(*) FROM config WHERE key LIKE ?",
                [format!("repair:{group}:cursor%")],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(cursors, 0);
        let secondary = Store::open_secondary(dir.path(), &s.repo, &s.device).unwrap();
        assert_eq!(authority(&secondary), before);
    }
    println!(
        "WAVE6_STORE_FORMAT {}",
        serde_json::json!({"current_index_exit_86_children":3,"accepted_sequence_epoch_ack_and_permissions_preserved":true})
    );
}

fn child(root: &Path, mode: &str, point: &str) -> std::process::Output {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_current_store", "--ignored", "--nocapture"])
        .env("TKFS_STORE_FORMAT_ROOT", root)
        .env("TKFS_STORE_FORMAT_MODE", mode)
        .env("TKFS_FAULT_POINT", point)
        .env("TKFS_FAULT_ACTION", "exit")
        .env("TKFS_FAULT_COUNT", "1")
        .output()
        .unwrap()
}

#[test]
fn process_exit_during_fresh_schema_creation_leaves_empty_or_complete_current_store() {
    for point in [
        "store.schema.before_initialize",
        "store.schema.initialized",
        "store.schema.committed",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let result = child(dir.path(), "initialize", point);
        assert_eq!(
            result.status.code(),
            Some(86),
            "{point}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        let db = Connection::open_with_flags(
            dir.path().join("metadata.sqlite"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        let version: i64 = db
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        let count: usize = db
            .query_row("SELECT count(*) FROM sqlite_schema", [], |r| r.get(0))
            .unwrap();
        if point == "store.schema.committed" {
            assert_eq!(version, 3);
            assert!(count > 0);
        } else {
            assert_eq!(version, 0);
            assert_eq!(count, 0);
        }
        drop(db);
        let s = Store::initialize(dir.path(), "repo", "device").unwrap();
        assert_eq!(s.repo, "repo");
        assert_eq!(s.device, "device");
        drop(s);
        let s = Store::open_existing(dir.path()).unwrap();
        assert_eq!(s.repo, "repo");
    }
    println!(
        "WAVE6_STORE_FORMAT {}",
        serde_json::json!({"fresh_schema_exit_86_children":3,"schema_identity_and_format_atomic":true})
    );
}

#[test]
#[ignore = "subprocess helper for current-store creation and index recovery"]
fn child_current_store() {
    let root = std::env::var_os("TKFS_STORE_FORMAT_ROOT").unwrap();
    if std::env::var("TKFS_STORE_FORMAT_MODE").unwrap() == "initialize" {
        Store::initialize(Path::new(&root), "repo", "device").unwrap();
    } else {
        Store::open_existing(Path::new(&root)).unwrap();
    }
    panic!("requested crash boundary did not exit");
}
