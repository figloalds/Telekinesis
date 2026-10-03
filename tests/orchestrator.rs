#![cfg(windows)]
use std::fs;
use tempfile::tempdir;
use tkfs::{
    core::{Store, id},
    orchestrator::{Action, Config, Request, Supervisor},
};

fn config(root: &std::path::Path, extra: &str) -> std::path::PathBuf {
    let path = root.join("defaults.toml");
    fs::write(&path,format!("format_version=1\ndata_directory='data'\n{extra}\n[control]\ntransport='named-pipe'\nname='test-{}'\n",id())).unwrap();
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
        original.replace("transport='named-pipe'", "transport='tcp'"),
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
            operation_id: None,
            expected_generation: None,
            action: Action::List,
        })
        .unwrap();
    assert_eq!(result["result"]["catalog_generation"], 0);
    assert_eq!(result["result"]["states"].as_array().unwrap().len(), 0);
}

#[test]
fn interrupted_bootstrap_recovers_and_future_registry_or_wrong_owner_refuses() {
    let temp = tempdir().unwrap();
    let path = config(temp.path(), "");
    let data = temp.path().join("data");
    fs::create_dir(&data).unwrap();
    fs::write(data.join("orchestrator.lock"), "").unwrap();
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
