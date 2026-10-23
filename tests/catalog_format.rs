//! Selected catalog initialization, nonmutating refusal and exact current restart.
use rusqlite::{Connection, types::Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs, path::Path, process::Command};
use tkfs::{
    model::{
        errors::{ErrorClass, ErrorWire, code_of},
        orchestrator::{CatalogApi, CatalogCmd, CredentialSide, FolderRecord, GateRecord},
    },
    orchestrator::{Config, Network, catalog::Catalog},
    pairing_code::Verifier,
};

fn config(root: &Path) -> Config {
    Config {
        format_version: 2,
        data_directory: root.join("data"),
        default_mount_directory: root.join("mounts"),
        maximum_running: 8,
        network: Network::default(),
    }
}
fn files(root: &Path) -> BTreeMap<String, String> {
    fn walk(root: &Path, path: &Path, result: &mut BTreeMap<String, String>) {
        for entry in fs::read_dir(path).unwrap() {
            let p = entry.unwrap().path();
            let name = p
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            if p.is_dir() {
                result.insert(format!("{name}/"), String::new());
                walk(root, &p, result);
            } else {
                let hash = if name.ends_with("-shm") {
                    "volatile".to_owned()
                } else {
                    hex::encode(Sha256::digest(fs::read(&p).unwrap()))
                };
                result.insert(name, hash);
            }
        }
    }
    let mut result = BTreeMap::new();
    walk(root, root, &mut result);
    result
}
fn refused(config: &Config, expected: &str) {
    let error = Catalog::open(config).unwrap_err();
    assert_eq!(code_of(&error), expected, "{error:#}");
    if expected == "UNSUPPORTED_CATALOG_FORMAT" {
        assert_eq!(
            ErrorWire::from_error(&error).class,
            ErrorClass::Incompatible
        );
    }
}
fn rows(db: &Connection) -> BTreeMap<String, Vec<Vec<Value>>> {
    [
        "settings",
        "projects",
        "peers",
        "folder_intents",
        "mount_gates",
        "peer_meta",
        "peer_credentials",
    ]
    .into_iter()
    .map(|table| {
        let query = if table == "settings" {
            "SELECT * FROM settings WHERE key!='objects_incarnation' ORDER BY rowid".to_owned()
        } else {
            format!("SELECT * FROM {table} ORDER BY rowid")
        };
        let mut q = db.prepare(&query).unwrap();
        let columns = q.column_count();
        let rows = q
            .query_map([], |r| (0..columns).map(|i| r.get(i)).collect())
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        (table.to_owned(), rows)
    })
    .collect()
}
fn seed(cfg: &Config) -> std::sync::Arc<Catalog> {
    let cat = Catalog::open(cfg).unwrap();
    cat.apply(CatalogCmd::UpsertPeer {
        record: tkfs::model::orchestrator::PeerRecord {
            endpoint: "ws://source:1".into(),
            code: "test-code".into(),
            installation: "peer".into(),
            connections: 3,
            revision: "revision".into(),
        },
    })
    .unwrap();
    cat.apply(CatalogCmd::CreateFolder {
        record: FolderRecord {
            state: "folder".into(),
            label: "Current".into(),
            repo: "repo".into(),
            device: "device".into(),
            directory: cfg.data_directory.join("states/folder"),
            mount: Some(cfg.default_mount_directory.join("Current")),
            desired_running: false,
            sync_enabled: false,
            mount_enabled: true,
        },
        gate: Some(GateRecord {
            source_endpoint: "ws://source:1".into(),
            ..GateRecord::default()
        }),
    })
    .unwrap();
    cat.apply(CatalogCmd::SetVerifier {
        verifier: Verifier::generate().1,
    })
    .unwrap();
    cat.enroll("peer", CredentialSide::Inbound, [7; 32])
        .unwrap();
    cat.enroll("peer", CredentialSide::Outbound, [8; 32])
        .unwrap();
    cat
}

#[test]
fn fresh_catalog_matches_independent_schema_and_has_one_gate_source() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let cat = Catalog::open(&cfg).unwrap();
    assert_eq!(cat.lease_incarnation(), 1);
    assert!(!cat.snapshot().installation.is_empty());
    let golden: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/catalog_v1/schema.json")).unwrap();
    assert_eq!(
        hex::encode(Sha256::digest(
            fs::read(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/orchestrator/catalog/schema.sql"
            ))
            .unwrap()
        )),
        golden["schema_sql_sha256"].as_str().unwrap()
    );
    let db = Connection::open(cfg.data_directory.join("folders.sqlite")).unwrap();
    let mut q=db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY type,name").unwrap();
    let objects=q.query_map([],|r|Ok(serde_json::json!({"type":r.get::<_,String>(0)?,"name":r.get::<_,String>(1)?,"table":r.get::<_,String>(2)?,"sql":r.get::<_,String>(3)?}))).unwrap().collect::<rusqlite::Result<Vec<_>>>().unwrap();
    assert_eq!(serde_json::json!(objects), golden["objects"]);
    for (pragma, key) in [
        ("PRAGMA application_id", "application_id"),
        ("PRAGMA user_version", "version"),
    ] {
        let actual: i64 = db.query_row(pragma, [], |r| r.get(0)).unwrap();
        assert_eq!(actual, golden[key].as_i64().unwrap());
    }
    let obsolete:usize=db.query_row("SELECT count(*) FROM settings WHERE key IN ('catalog_schema','credentials_generation')",[],|r|r.get(0)).unwrap();
    assert_eq!(obsolete, 0);
    assert_eq!(objects.len(), 7);
}

#[test]
fn current_restart_keeps_every_authoritative_row_and_advances_only_lease_incarnation() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let cat = seed(&cfg);
    let snapshot = cat.snapshot();
    let keys = cat.credentials();
    let incarnation = cat.lease_incarnation();
    assert!(cat.close());
    drop(cat);
    let before = rows(&Connection::open(cfg.data_directory.join("folders.sqlite")).unwrap());
    let cat = Catalog::open(&cfg).unwrap();
    assert_eq!(cat.snapshot().installation, snapshot.installation);
    assert_eq!(cat.snapshot().folders, snapshot.folders);
    assert_eq!(cat.snapshot().gates, snapshot.gates);
    assert_eq!(cat.snapshot().verifier, snapshot.verifier);
    assert_eq!(
        cat.snapshot().verifier_generation,
        snapshot.verifier_generation
    );
    assert_eq!(*cat.credentials(), *keys);
    assert_eq!(cat.lease_incarnation(), incarnation + 1);
    assert_eq!(
        rows(&Connection::open(cfg.data_directory.join("folders.sqlite")).unwrap()),
        before
    );
}

#[test]
fn unsupported_headers_preserve_files_permissions_and_do_not_create_locks_or_mounts() {
    for (app, version) in [
        (0, 0),
        (0, 1),
        (1, 1),
        (0x544b4353, 0),
        (0x544b4353, 2),
        (0x544b4353, -1),
        (0x544b4353, i32::MAX),
        (0x544b4d53, 1),
    ] {
        for mode in ["DELETE", "WAL"] {
            for closed in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let cfg = config(dir.path());
                fs::create_dir(&cfg.data_directory).unwrap();
                let path = cfg.data_directory.join("folders.sqlite");
                let db = Connection::open(&path).unwrap();
                db.pragma_update(None, "journal_mode", mode).unwrap();
                db.execute_batch(
                    "CREATE TABLE preserved(data); INSERT INTO preserved VALUES('keep')",
                )
                .unwrap();
                db.pragma_update(None, "application_id", app).unwrap();
                db.pragma_update(None, "user_version", version).unwrap();
                let _db = if closed {
                    drop(db);
                    None
                } else {
                    Some(db)
                };
                let before = files(dir.path());
                let permissions = fs::metadata(&path).unwrap().permissions();
                refused(&cfg, "UNSUPPORTED_CATALOG_FORMAT");
                assert_eq!(files(dir.path()), before);
                assert_eq!(fs::metadata(path).unwrap().permissions(), permissions);
                assert!(!cfg.default_mount_directory.exists());
                assert!(!cfg.data_directory.join("orchestrator.lock").exists());
            }
        }
    }
}

#[test]
fn damaged_current_authority_is_refused_without_recreation_or_credential_reset() {
    for key in [
        "installation",
        "objects_incarnation",
        "verifier_generation",
        "verifier_salt",
        "pairing",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let cat = seed(&cfg);
        assert!(cat.close());
        drop(cat);
        let db = Connection::open(cfg.data_directory.join("folders.sqlite")).unwrap();
        db.execute("DELETE FROM settings WHERE key=?", [key])
            .unwrap();
        let before = files(dir.path());
        refused(&cfg, "INVALID_CATALOG");
        assert_eq!(files(dir.path()), before);
    }
    for table in [
        "projects",
        "folder_intents",
        "mount_gates",
        "peer_credentials",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let cat = seed(&cfg);
        assert!(cat.close());
        drop(cat);
        let db = Connection::open(cfg.data_directory.join("folders.sqlite")).unwrap();
        db.execute_batch(&format!("DROP TABLE {table}")).unwrap();
        let before = files(dir.path());
        refused(&cfg, "INVALID_CATALOG");
        assert_eq!(files(dir.path()), before);
    }
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let cat = seed(&cfg);
    assert!(cat.close());
    drop(cat);
    let db = Connection::open(cfg.data_directory.join("folders.sqlite")).unwrap();
    db.execute("DELETE FROM folder_intents", []).unwrap();
    let before = files(dir.path());
    refused(&cfg, "INVALID_CATALOG");
    assert_eq!(files(dir.path()), before);
}

fn child(root: &Path, mode: &str, point: &str) -> std::process::Output {
    Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "child_current_catalog",
            "--ignored",
            "--nocapture",
        ])
        .env("TKFS_CATALOG_ROOT", root)
        .env("TKFS_CATALOG_MODE", mode)
        .env("TKFS_FAULT_POINT", point)
        .env("TKFS_FAULT_ACTION", "exit")
        .env("TKFS_FAULT_COUNT", "1")
        .output()
        .unwrap()
}
#[test]
fn initialization_exit_leaves_empty_or_complete_schema_identity_and_header() {
    for point in [
        "catalog.schema.before_initialize",
        "catalog.schema.initialized",
        "catalog.schema.committed",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let out = child(dir.path(), "open", point);
        assert_eq!(
            out.status.code(),
            Some(86),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let db = Connection::open(cfg.data_directory.join("folders.sqlite")).unwrap();
        let version: i64 = db
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        let count: usize = db
            .query_row("SELECT count(*) FROM sqlite_schema", [], |r| r.get(0))
            .unwrap();
        if point == "catalog.schema.committed" {
            assert_eq!(version, 1);
            assert!(count >= 7);
        } else {
            assert_eq!(version, 0);
            assert_eq!(count, 0);
        }
        drop(db);
        let cat = Catalog::open(&cfg).unwrap();
        assert_eq!(cat.lease_incarnation(), 1);
        assert!(!cat.snapshot().installation.is_empty());
    }
}
#[test]
fn exit_during_verifier_rotation_cannot_publish_new_code_or_keep_half_revoked_keys() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let cat = seed(&cfg);
    let incarnation = cat.lease_incarnation();
    assert!(cat.close());
    drop(cat);
    let before = rows(&Connection::open(cfg.data_directory.join("folders.sqlite")).unwrap());
    let out = child(dir.path(), "rotate", "catalog.slow_write");
    assert_eq!(out.status.code(), Some(86));
    let cat = Catalog::open(&cfg).unwrap();
    assert_eq!(
        rows(&Connection::open(cfg.data_directory.join("folders.sqlite")).unwrap()),
        before
    );
    assert_eq!(
        cat.credentials().active("peer", CredentialSide::Inbound),
        Some([7; 32])
    );
    assert_eq!(cat.lease_incarnation(), incarnation + 2);
}
#[test]
fn exhausted_generations_refuse_without_counter_wrap_or_partial_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let cat = seed(&cfg);
    assert!(cat.close());
    drop(cat);
    let db = Connection::open(cfg.data_directory.join("folders.sqlite")).unwrap();
    db.execute(
        "UPDATE settings SET value=? WHERE key='verifier_generation'",
        [u64::MAX.to_string()],
    )
    .unwrap();
    drop(db);
    let cat = Catalog::open(&cfg).unwrap();
    let before = cat.snapshot();
    let error = cat
        .apply(CatalogCmd::SetVerifier {
            verifier: Verifier::generate().1,
        })
        .unwrap_err();
    assert_eq!(error.code, "CATALOG_UNAVAILABLE");
    assert_eq!(cat.snapshot().verifier, before.verifier);
    assert_eq!(cat.snapshot().verifier_generation, u64::MAX);
    assert_eq!(
        cat.credentials().active("peer", CredentialSide::Inbound),
        Some([7; 32])
    );
    assert!(cat.close());
    drop(cat);
    let db = Connection::open(cfg.data_directory.join("folders.sqlite")).unwrap();
    db.execute(
        "UPDATE settings SET value=? WHERE key='objects_incarnation'",
        [u64::MAX.to_string()],
    )
    .unwrap();
    let before = files(dir.path());
    refused(&cfg, "CATALOG_UNAVAILABLE");
    assert_eq!(files(dir.path()), before);
}
#[test]
#[ignore = "actual process-exit helper for catalog initialization and verifier rotation"]
fn child_current_catalog() {
    let root = std::env::var_os("TKFS_CATALOG_ROOT").unwrap();
    let cat = Catalog::open(&config(Path::new(&root))).unwrap();
    if std::env::var("TKFS_CATALOG_MODE").unwrap() == "rotate" {
        cat.apply(CatalogCmd::SetVerifier {
            verifier: Verifier::generate().1,
        })
        .unwrap();
    }
    panic!("requested crash point did not exit");
}
