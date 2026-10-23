//! Current-format initialization/recovery and refusal of every other layout.
use super::*;
use std::{collections::BTreeMap, path::Path};

fn persistent_files(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        // SQLite's shared-memory index is disposable reader coordination.
        // The database, WAL, rollback journal and any other files are compared.
        .filter(|path| path.is_file() && !path.to_string_lossy().ends_with("-shm"))
        .map(|path| {
            (
                path.file_name().unwrap().to_str().unwrap().to_owned(),
                fs::read(path).unwrap(),
            )
        })
        .collect()
}

#[test]
fn fresh_current_journal_restarts_without_rewriting_saved_records_or_clocks() {
    for native in [false, true] {
        let (dir, journal, base, session) = setup(0, false);
        if native {
            session
                .write_at_with_times(0, b"current saved", 6, [101, 202], &context())
                .unwrap();
        } else {
            session.write_at(0, b"current saved", &context()).unwrap();
        }
        let saved = session.checkpoint(&context()).unwrap().unwrap();
        drop(session);
        drop(journal);
        let db = rusqlite::Connection::open(dir.path().join("write-sessions-v1.sqlite")).unwrap();
        let records = || {
            db.query_row("SELECT working,saved,seal FROM sessions", [], |r| {
                Ok((
                    r.get::<_, Vec<u8>>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                ))
            })
            .unwrap()
        };
        let before = records();
        let reopened = WriteJournal::open(dir.path()).unwrap();
        assert_eq!(records(), before);
        assert_eq!(
            reopened.pending(None, 64, &context()).unwrap(),
            vec![saved.clone()]
        );
        let recovered = reopened
            .resume(saved.intent.operation, base, &context())
            .unwrap();
        assert_eq!(read(&recovered, 13), b"current saved");
        assert_eq!(
            recovered.checkpoint(&context()).unwrap(),
            Some(saved.clone())
        );
        if native {
            let clocks = saved.native_times().unwrap();
            assert_eq!([clocks.write_time, clocks.change_time], [101, 202]);
        } else {
            assert!(saved.native_times().is_none());
        }
        let resumed = records();
        assert_eq!(resumed.1, before.1, "Saved bytes must remain exact");
        let old: serde_json::Value = serde_json::from_slice(&before.0).unwrap();
        let new: serde_json::Value = serde_json::from_slice(&resumed.0).unwrap();
        assert_ne!(old["value"]["owner"], new["value"]["owner"]);
        assert!(
            new["value"]["sequence"].as_u64().unwrap() > old["value"]["sequence"].as_u64().unwrap()
        );
        let header: (i64, i64) = db.query_row(
            "SELECT (SELECT application_id FROM pragma_application_id), (SELECT user_version FROM pragma_user_version)",
            [], |r| Ok((r.get(0)?, r.get(1)?)),
        ).unwrap();
        assert_eq!(header, (0x544b5753, 7));
    }
}

#[test]
fn unsupported_journal_headers_preserve_all_persistent_bytes_without_upgrade_or_wal_switch() {
    let current_app = 0x544b5753;
    let headers = (-1..=8)
        .filter(|version| *version != 7)
        .map(|version| (current_app, version))
        .chain([
            (current_app, i32::MAX),
            (0, 7),
            (1, 7),
            (current_app + 1, 7),
        ]);
    for (app, version) in headers {
        for mode in ["DELETE", "WAL"] {
            for native in [false, true] {
                for closed in [false, true] {
                    let (dir, journal, _base, session) = setup(0, false);
                    if native {
                        session
                            .write_at_with_times(0, b"retained", 6, [101, 202], &context())
                            .unwrap();
                    } else {
                        session.write_at(0, b"retained", &context()).unwrap();
                    }
                    let saved = session.checkpoint(&context()).unwrap().unwrap();
                    drop(session);
                    drop(journal);
                    let db =
                        rusqlite::Connection::open(dir.path().join("write-sessions-v1.sqlite"))
                            .unwrap();
                    db.pragma_update(None, "journal_mode", mode).unwrap();
                    db.pragma_update(None, "application_id", app).unwrap();
                    db.pragma_update(None, "user_version", version).unwrap();
                    let records: Vec<u8> = db
                        .query_row("SELECT saved FROM sessions", [], |r| r.get(0))
                        .unwrap();
                    // Cover both live committed WAL headers and closed WAL-mode
                    // databases without sidecars. Read-only SQLite alone creates
                    // WAL/SHM files on refusal in the latter case.
                    let db = if closed {
                        drop(db);
                        None
                    } else {
                        Some(db)
                    };
                    let names = || {
                        fs::read_dir(dir.path())
                            .unwrap()
                            .map(|e| e.unwrap().file_name())
                            .collect::<std::collections::BTreeSet<_>>()
                    };
                    let before_names = names();
                    let before = persistent_files(dir.path());
                    let error = WriteJournal::open(dir.path())
                        .err()
                        .expect("unsupported format was accepted");
                    assert_eq!(
                        tkfs::model::errors::code_of(&error),
                        "UNSUPPORTED_WRITE_JOURNAL",
                        "{error:#}"
                    );
                    assert!(format!("{error:#}").contains("use a fresh store directory"));
                    assert_eq!(
                        persistent_files(dir.path()),
                        before,
                        "{app}/{version}/{mode}/{native}/{closed}"
                    );
                    assert_eq!(names(), before_names, "refusal created SQLite sidecars");
                    let db = db.unwrap_or_else(|| {
                        rusqlite::Connection::open_with_flags(
                            dir.path().join("write-sessions-v1.sqlite"),
                            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                        )
                        .unwrap()
                    });
                    assert_eq!(
                        db.query_row("PRAGMA user_version", [], |r| r.get::<_, i32>(0))
                            .unwrap(),
                        version
                    );
                    assert_eq!(
                        db.query_row("PRAGMA application_id", [], |r| r.get::<_, i32>(0))
                            .unwrap(),
                        app
                    );
                    assert_eq!(
                        db.query_row("PRAGMA journal_mode", [], |r| r.get::<_, String>(0))
                            .unwrap(),
                        mode.to_ascii_lowercase()
                    );
                    assert_eq!(
                        db.query_row("SELECT saved FROM sessions", [], |r| r.get::<_, Vec<u8>>(0))
                            .unwrap(),
                        records
                    );
                    let record: serde_json::Value = serde_json::from_slice(&records).unwrap();
                    assert_eq!(
                        serde_json::from_value::<tkfs::content::write::SaveCheckpoint>(
                            record["value"].clone()
                        )
                        .unwrap(),
                        saved
                    );
                }
            }
        }
    }
    println!(
        "WAVE6_JOURNAL_FORMAT {}",
        serde_json::json!({"headers":13,"modes":["delete","wal"],"native_and_headless":true,"open_and_closed":true,"cases":104,"persistent_bytes_and_directory_entries_unchanged":true})
    );
}

#[test]
fn standalone_header_guard_preserves_malformed_and_previously_used_unmarked_files() {
    for bytes in [
        vec![1],
        vec![0; 99],
        vec![0; 100],
        b"SQLite format 3\0".to_vec(),
    ] {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("write-sessions-v1.sqlite"), bytes).unwrap();
        let before = persistent_files(dir.path());
        let error = WriteJournal::open(dir.path())
            .err()
            .expect("invalid header was accepted");
        assert_eq!(
            tkfs::model::errors::code_of(&error),
            "UNSUPPORTED_WRITE_JOURNAL"
        );
        assert_eq!(persistent_files(dir.path()), before);
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }
    for mode in ["DELETE", "WAL"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("write-sessions-v1.sqlite");
        let db = rusqlite::Connection::open(path).unwrap();
        db.pragma_update(None, "journal_mode", mode).unwrap();
        db.execute_batch("CREATE TABLE previous(data); DROP TABLE previous")
            .unwrap();
        assert_eq!(
            db.query_row("SELECT count(*) FROM sqlite_schema", [], |r| r
                .get::<_, u64>(0))
                .unwrap(),
            0
        );
        drop(db);
        let before = persistent_files(dir.path());
        let error = WriteJournal::open(dir.path())
            .err()
            .expect("previously used state was initialized");
        assert_eq!(
            tkfs::model::errors::code_of(&error),
            "UNSUPPORTED_WRITE_JOURNAL"
        );
        assert_eq!(persistent_files(dir.path()), before);
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }
    // A genuinely empty existing file is fresh, including a locator that must
    // not be interpreted as a URI or require an ASCII-only directory.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("journal#% Ω");
    fs::create_dir(&root).unwrap();
    fs::write(root.join("write-sessions-v1.sqlite"), []).unwrap();
    let journal = WriteJournal::open(&root).unwrap();
    let session = journal
        .create(intent(0), Base::new(false), &context())
        .unwrap();
    session.write_at(0, b"fresh", &context()).unwrap();
    session.checkpoint(&context()).unwrap().unwrap();
}

#[test]
fn unmarked_nonempty_database_is_not_initialized_even_when_it_has_no_tables() {
    for sql in [
        "CREATE TABLE unrelated(data BLOB); INSERT INTO unrelated VALUES(X'010203')",
        "CREATE VIEW unrelated AS SELECT 'preserve me' AS data",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("write-sessions-v1.sqlite");
        let db = rusqlite::Connection::open(path).unwrap();
        db.execute_batch(sql).unwrap();
        let before = persistent_files(dir.path());
        let error = WriteJournal::open(dir.path())
            .err()
            .expect("unmarked state was accepted");
        assert_eq!(
            tkfs::model::errors::code_of(&error),
            "UNSUPPORTED_WRITE_JOURNAL"
        );
        assert_eq!(persistent_files(dir.path()), before);
        assert_eq!(
            db.query_row("SELECT data FROM unrelated", [], |r| r
                .get::<_, rusqlite::types::Value>(
                0
            ))
            .unwrap(),
            if sql.contains("VIEW") {
                rusqlite::types::Value::Text("preserve me".into())
            } else {
                rusqlite::types::Value::Blob(vec![1, 2, 3])
            }
        );
    }
}

#[test]
fn process_exit_during_fresh_initialization_leaves_an_empty_or_complete_current_layout() {
    for point in [
        "content.write.before_initialize",
        "content.write.schema_initialized",
        "content.write.initialized",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "formats::child_initialize",
                "--ignored",
                "--nocapture",
            ])
            .env("TKFS_JOURNAL_INIT_ROOT", dir.path())
            .env("TKFS_FAULT_POINT", point)
            .env("TKFS_FAULT_ACTION", "exit")
            .env("TKFS_FAULT_COUNT", "1")
            .output()
            .unwrap();
        assert_eq!(
            child.status.code(),
            Some(86),
            "{point}: {}",
            String::from_utf8_lossy(&child.stderr)
        );
        let db = rusqlite::Connection::open_with_flags(
            dir.path().join("write-sessions-v1.sqlite"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        let version = db
            .query_row("PRAGMA user_version", [], |r| r.get::<_, i32>(0))
            .unwrap();
        let objects = db
            .query_row("SELECT count(*) FROM sqlite_schema", [], |r| {
                r.get::<_, u64>(0)
            })
            .unwrap();
        if point == "content.write.initialized" {
            assert_eq!(version, 7);
            assert!(objects > 0);
        } else {
            assert_eq!(version, 0);
            assert_eq!(objects, 0);
        }
        drop(db);
        let journal = WriteJournal::open(dir.path()).unwrap();
        assert!(journal.pending(None, 64, &context()).unwrap().is_empty());
        let session = journal
            .create(intent(0), Base::new(false), &context())
            .unwrap();
        session.write_at(0, b"initialized", &context()).unwrap();
        let saved = session.checkpoint(&context()).unwrap().unwrap();
        let resumed = journal
            .resume(saved.intent.operation, Base::new(false), &context())
            .unwrap();
        assert_eq!(read(&resumed, 11), b"initialized");
    }
    println!(
        "WAVE6_JOURNAL_FORMAT {}",
        serde_json::json!({"initialization_exit_86_children":3,"partial_schema_never_accepted":true,"current_saved_recovered":true})
    );
}

#[test]
#[ignore = "fresh-initialization crash helper; invoked by the parent"]
fn child_initialize() {
    let path = std::env::var_os("TKFS_JOURNAL_INIT_ROOT").unwrap();
    WriteJournal::open(Path::new(&path)).unwrap();
    panic!("initialization crash point did not exit");
}
