#![cfg(any(windows, target_os = "linux"))]
use std::fs;
use tempfile::tempdir;
use tkfs::{
    core::{Store, id},
    orchestrator::{Action, Config, Request, Supervisor},
};

fn config(root: &std::path::Path, extra: &str) -> std::path::PathBuf {
    let path = root.join("defaults.toml");
    let transport = if cfg!(windows) {
        "named-pipe"
    } else {
        "unix-socket"
    };
    fs::write(&path,format!("format_version=1\ndata_directory='data'\n{extra}\n[control]\ntransport='{transport}'\nname='test-{}'\n",id())).unwrap();
    path
}
#[test]
fn strict_open_does_not_recreate_missing_metadata_or_identity() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("state");
    fs::create_dir(&root).unwrap();
    assert!(Store::open_existing(&root).is_err());
    assert!(!root.join("metadata.sqlite").exists());
    let repo = id();
    let device = id();
    let store = Store::initialize(&root, &repo, &device).unwrap();
    assert_eq!(store.device, device);
    drop(store);
    let mut store = Store::open_existing(&root).unwrap();
    store.create("kept", "file").unwrap();
    drop(store);
    let store = Store::initialize(&root, &repo, &device).unwrap();
    assert!(store.lookup("kept").is_ok());
    assert!(Store::initialize(&root, &repo, &id()).is_err());
    store
        .db
        .execute("DELETE FROM config WHERE key='device'", [])
        .unwrap();
    drop(store);
    assert!(Store::open_existing(&root).is_err());
    let db = rusqlite::Connection::open(root.join("metadata.sqlite")).unwrap();
    assert_eq!(
        db.query_row("SELECT count(*) FROM config WHERE key='device'", [], |r| {
            r.get::<_, u32>(0)
        })
        .unwrap(),
        0
    );
}
#[test]
fn defaults_reject_unknown_version_fields_and_network_and_resolve_relative_paths() {
    let temp = tempdir().unwrap();
    let path = config(temp.path(), "");
    let loaded = Config::load(&path).unwrap();
    assert!(loaded.data_directory.is_absolute());
    assert_eq!(loaded.data_directory.file_name().unwrap(), "data");
    let original = fs::read_to_string(&path).unwrap();
    for bad in [
        original.replace("format_version=1", "format_version=2"),
        original.replace("format_version=1", "format_version=1\nunknown=true"),
        format!("{original}\n[network]\nenabled=true\n"),
        original.replace(
            if cfg!(windows) {
                "transport='named-pipe'"
            } else {
                "transport='unix-socket'"
            },
            "transport='tcp'",
        ),
    ] {
        fs::write(&path, bad).unwrap();
        assert!(Config::load(&path).is_err());
        assert!(!temp.path().join("data").exists());
    }
}
#[test]
fn supervisor_lock_and_terminal_receipt_replay_before_generation_check() {
    let temp = tempdir().unwrap();
    let path = config(temp.path(), "");
    let mut supervisor = Supervisor::open(Config::load(&path).unwrap()).unwrap();
    assert!(Supervisor::open(Config::load(&path).unwrap()).is_err());
    let request = Request {
        version: 1,
        target_installation: None,
        operation_id: Some(id()),
        expected_generation: Some(99),
        action: Action::Create {
            label: "example".into(),
            mount: None,
        },
    };
    let rejected = supervisor.handle(request.clone()).unwrap();
    assert_eq!(rejected["ok"], false);
    assert_eq!(rejected["retryable"], false);
    let replay = supervisor.handle(request.clone()).unwrap();
    assert_eq!(rejected, replay);
    let mut changed = request;
    changed.expected_generation = Some(0);
    assert!(supervisor.handle(changed).is_err());
    let result = supervisor
        .handle(Request {
            version: 1,
            target_installation: None,
            operation_id: None,
            expected_generation: None,
            action: Action::List,
        })
        .unwrap();
    assert_eq!(result["result"]["catalog_generation"], 0);
    assert_eq!(result["result"]["states"].as_array().unwrap().len(), 0);
}

#[test]
fn management_identity_is_bound_before_any_action() {
    let temp = tempdir().unwrap();
    let installation = id();
    let path = config(temp.path(), &format!("installation_id='{installation}'"));
    let mut supervisor = Supervisor::open(Config::load(&path).unwrap()).unwrap();
    let mut request = Request {
        version: 1,
        target_installation: Some(installation.clone()),
        operation_id: None,
        expected_generation: None,
        action: Action::Hello,
    };
    let hello = supervisor.handle(request.clone()).unwrap();
    assert_eq!(hello["result"]["installation_id"], installation);
    assert_eq!(hello["result"]["api_version"], 1);
    request.target_installation = Some(id());
    assert!(
        supervisor
            .handle(request.clone())
            .unwrap_err()
            .to_string()
            .contains("INSTALLATION_ID_MISMATCH")
    );
    request.target_installation = Some(installation);
    request.version = 2;
    assert!(
        supervisor
            .handle(request)
            .unwrap_err()
            .to_string()
            .contains("UNSUPPORTED_API_VERSION")
    );
}

#[test]
fn interrupted_bootstrap_recovers_and_future_registry_or_wrong_owner_refuses() {
    let temp = tempdir().unwrap();
    let path = config(temp.path(), "");
    let data = temp.path().join("data");
    fs::create_dir(&data).unwrap();
    fs::write(data.join("orchestrator.lock"), "").unwrap();
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&data, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(
            data.join("orchestrator.lock"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
    }
    drop(Supervisor::open(Config::load(&path).unwrap()).unwrap());
    let db = rusqlite::Connection::open(data.join("orchestrator.sqlite")).unwrap();
    db.execute_batch("PRAGMA user_version=2").unwrap();
    assert!(
        Supervisor::open(Config::load(&path).unwrap())
            .err()
            .unwrap()
            .to_string()
            .contains("UNSUPPORTED_REGISTRY_VERSION")
    );
    db.execute_batch("PRAGMA user_version=1; UPDATE installation SET owner='another-user'")
        .unwrap();
    assert!(
        Supervisor::open(Config::load(&path).unwrap())
            .err()
            .unwrap()
            .to_string()
            .contains("REGISTRY_OWNER_MISMATCH")
    );
}

#[test]
fn established_invalid_catalog_is_never_reinitialized() {
    for invalid in ["empty", "missing", "truncated", "version-zero-schema"] {
        let temp = tempdir().unwrap();
        let path = config(temp.path(), "");
        drop(Supervisor::open(Config::load(&path).unwrap()).unwrap());
        let root = temp.path().join("data");
        let retained = root.join("states").join("retained-project");
        fs::create_dir(&retained).unwrap();
        fs::write(
            retained.join("user-data"),
            b"preserve literal fixture bytes",
        )
        .unwrap();
        let database = root.join("orchestrator.sqlite");
        fs::remove_file(&database).unwrap();
        match invalid {
            "empty" => fs::write(&database, b"").unwrap(),
            "truncated" => fs::write(&database, b"SQLite format 3\0truncated").unwrap(),
            "version-zero-schema" => {
                let db = rusqlite::Connection::open(&database).unwrap();
                db.execute_batch("CREATE TABLE unrelated(precious TEXT); INSERT INTO unrelated VALUES('keep'); PRAGMA user_version=0;").unwrap();
            }
            _ => {}
        }
        let before = fs::read(&database).ok();
        assert!(
            Supervisor::open(Config::load(&path).unwrap()).is_err(),
            "{invalid} catalog was silently reinitialized"
        );
        assert_eq!(
            fs::read(&database).ok(),
            before,
            "{invalid} database bytes changed"
        );
        assert_eq!(
            fs::read(retained.join("user-data")).unwrap(),
            b"preserve literal fixture bytes"
        );
    }
}

#[test]
fn bootstrap_proof_cannot_reinitialize_existing_schema_or_state_directories() {
    for invalid in ["schema", "state-directory", "wrong-identity"] {
        let temp = tempdir().unwrap();
        let installation = id();
        let path = config(temp.path(), &format!("installation_id='{installation}'"));
        let root = temp.path().join("data");
        fs::create_dir(&root).unwrap();
        let marker = serde_json::json!({
            "version":1,
            "installation_id":if invalid == "wrong-identity" { id() } else { installation },
            "owner":tkfs::local_ipc::owner_sid().unwrap(),
            "root":fs::canonicalize(&root).unwrap()
        });
        let marker_path = root.join(".orchestrator-bootstrap.json");
        fs::write(&marker_path, serde_json::to_vec(&marker).unwrap()).unwrap();
        let database = root.join("orchestrator.sqlite");
        if invalid == "schema" {
            let db = rusqlite::Connection::open(&database).unwrap();
            db.execute_batch("CREATE VIEW preserved AS SELECT 'literal fixture';")
                .unwrap();
        } else {
            fs::write(&database, b"").unwrap();
        }
        if invalid == "state-directory" {
            fs::create_dir(root.join("states")).unwrap();
            fs::write(root.join("states/kept"), b"literal fixture").unwrap();
        }
        let before = fs::read(&database).unwrap();
        assert!(
            Supervisor::open(Config::load(&path).unwrap()).is_err(),
            "{invalid}"
        );
        assert_eq!(fs::read(&database).unwrap(), before);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&fs::read(marker_path).unwrap()).unwrap(),
            marker
        );
    }
}
