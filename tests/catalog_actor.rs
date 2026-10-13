//! WP1.1 acceptance: the catalog actor (`orchestrator::catalog`) and the
//! published-catalog cache (`orchestrator::published`), REVAMP_PLAN §3.2 and
//! §14 WP1.1.
//!
//! Fault injection is process-wide (`util::fault`), so every test that opens a
//! catalog takes the `FAULTS` lock: shared by default, exclusive while a test
//! has a catalog fault point armed. The wake-up latency tests also take it
//! exclusively, so no catalog test competes with them for the CPU.
mod support;

use rusqlite::{Connection, OptionalExtension};
use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard,
        mpsc::{self, Receiver, Sender},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use support::{Node, NodeConfig, copy_legacy_datadir, legacy};
use tkfs::{
    core::id,
    model::{
        errors::{ErrorClass, ErrorWire, code_of},
        orchestrator::{
            CatalogApi, CatalogCmd, CatalogSnapshot, EntryState, FeedTip, FolderRecord, GateRecord,
            PeerRecord, PublishedCatalogApi, PublishedEntry,
        },
    },
    orchestrator::{
        self, Action, Config, Network, Project, Supervisor,
        catalog::{self, Catalog, validate_mount_path},
        published::PublishedCatalog,
    },
    pairing_code::Verifier,
    util::{
        fault::{self, FaultAction, FaultConfig},
        time::now_ms,
    },
};

// ---------------------------------------------------------------------------
// Harness

static FAULTS: RwLock<()> = RwLock::new(());

/// Taken by every test that runs catalog commands without arming a fault.
fn shared() -> RwLockReadGuard<'static, ()> {
    FAULTS.read().unwrap_or_else(PoisonError::into_inner)
}

/// Exclusive use of the catalog fault points for the guard's lifetime; they
/// are disarmed when it drops.
struct Exclusive(#[allow(dead_code)] RwLockWriteGuard<'static, ()>);

fn exclusive() -> Exclusive {
    Exclusive(FAULTS.write().unwrap_or_else(PoisonError::into_inner))
}

impl Exclusive {
    fn arm(&self, point: &str, action: FaultAction, ms: Option<u64>, count: Option<u64>) {
        fault::configure(Some(FaultConfig {
            point: point.into(),
            scope: None,
            action,
            ms,
            count,
        }));
    }

    fn disarm(&self) {
        fault::configure(None);
    }
}

impl Drop for Exclusive {
    fn drop(&mut self) {
        fault::configure(None);
    }
}

struct Env {
    root: tempfile::TempDir,
    config: Config,
}

fn env() -> Env {
    let root = tempfile::tempdir().unwrap();
    let config = Config {
        format_version: 2,
        data_directory: root.path().join("data"),
        default_mount_directory: root.path().join("mounts"),
        maximum_running: 8,
        network: Network::default(),
    };
    Env { root, config }
}

fn db_path(config: &Config) -> PathBuf {
    config.data_directory.join("folders.sqlite")
}

/// A new folder record laid out like the legacy `create()` does.
fn folder(config: &Config, label: &str) -> FolderRecord {
    let state = id();
    FolderRecord {
        directory: config.data_directory.join("states").join(&state),
        state,
        label: label.into(),
        repo: id(),
        device: id(),
        mount: None,
        desired_running: true,
        sync_enabled: true,
        mount_enabled: true,
    }
}

fn peer(endpoint: &str, installation: &str) -> PeerRecord {
    PeerRecord {
        endpoint: endpoint.into(),
        code: format!("code-for-{installation}"),
        installation: installation.into(),
        connections: 3,
        revision: String::new(),
    }
}

fn gate_from(endpoint: &str) -> GateRecord {
    GateRecord {
        source_endpoint: endpoint.into(),
        ..GateRecord::default()
    }
}

fn create(record: &FolderRecord, gate: Option<GateRecord>) -> CatalogCmd {
    CatalogCmd::CreateFolder {
        record: record.clone(),
        gate,
    }
}

fn set_desired(state: &str, desired: bool) -> CatalogCmd {
    CatalogCmd::SetDesired {
        state: state.into(),
        desired,
    }
}

/// Everything but the generation.
fn assert_same_content(a: &CatalogSnapshot, b: &CatalogSnapshot) {
    assert_eq!(a.installation, b.installation);
    assert_eq!(a.folders, b.folders);
    assert_eq!(a.peers, b.peers);
    assert_eq!(a.verifier, b.verifier);
    assert_eq!(a.verifier_generation, b.verifier_generation);
    assert_eq!(a.gates, b.gates);
    assert_eq!(a.peer_meta, b.peer_meta);
}

/// A read-only look at the file, as the legacy code (or a test) sees it.
fn raw(config: &Config) -> Connection {
    Connection::open(db_path(config)).unwrap()
}

fn raw_initial_sync(config: &Config) -> Vec<(String, String)> {
    raw(config)
        .prepare("SELECT state,endpoint FROM initial_sync ORDER BY state")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

fn raw_setting(config: &Config, key: &str) -> Option<String> {
    raw(config)
        .query_row("SELECT value FROM settings WHERE key=?", [key], |r| {
            r.get(0)
        })
        .optional()
        .unwrap()
}

fn raw_count(config: &Config, table: &str, state: &str) -> i64 {
    raw(config)
        .query_row(
            &format!("SELECT COUNT(*) FROM {table} WHERE state=?"),
            [state],
            |r| r.get(0),
        )
        .unwrap()
}

/// The legacy `Supervisor` view of a data directory: `projects()`, and the
/// List and Peers replies of the legacy `manage()` (no workers are started).
fn legacy_view(config: &Config) -> (Vec<Project>, Value, Value) {
    let supervisor = Supervisor::open(config.clone()).unwrap();
    let projects = supervisor.projects().unwrap();
    let shared = Arc::new(Mutex::new(supervisor));
    let list = orchestrator::manage(&shared, Action::List).unwrap();
    let peers = orchestrator::manage(&shared, Action::Peers).unwrap();
    (projects, list, peers)
}

/// Applies commands and checks that the generation moves by exactly one per
/// visible change, and not at all for no-ops and failures.
struct Steps<'a> {
    catalog: &'a Catalog,
    generation: u64,
}

impl Steps<'_> {
    #[track_caller]
    fn changed(&mut self, cmd: CatalogCmd) -> Arc<CatalogSnapshot> {
        let snap = self
            .catalog
            .apply(cmd.clone())
            .unwrap_or_else(|e| panic!("{cmd:?}: {e}"));
        self.generation += 1;
        assert_eq!(snap.generation, self.generation, "{cmd:?}");
        assert_eq!(self.catalog.snapshot().generation, self.generation);
        snap
    }

    #[track_caller]
    fn unchanged(&mut self, cmd: CatalogCmd) -> Arc<CatalogSnapshot> {
        let snap = self
            .catalog
            .apply(cmd.clone())
            .unwrap_or_else(|e| panic!("{cmd:?}: {e}"));
        assert_eq!(snap.generation, self.generation, "{cmd:?} is a no-op");
        assert_eq!(self.catalog.snapshot().generation, self.generation);
        snap
    }

    #[track_caller]
    fn fails(&mut self, cmd: CatalogCmd, code: &str) -> ErrorWire {
        let error = self.catalog.apply(cmd.clone()).unwrap_err();
        assert_eq!(error.code, code, "{cmd:?}: {error}");
        assert_eq!(self.catalog.snapshot().generation, self.generation);
        error
    }
}

/// `p95 <= limit` and nothing pathological (the criterion of
/// `tests/contracts.rs`), over 20 rounds of `measure`. The machine is shared
/// with parallel builds and suites, so a window spoiled by a load burst is
/// measured again (at most `WINDOWS` times); every window must meet the whole
/// bound on its own, and the failure lists every window.
fn assert_fast(name: &str, limit: Duration, mut measure: impl FnMut() -> Duration) {
    const WINDOWS: usize = 5;
    let mut windows = Vec::new();
    for _ in 0..WINDOWS {
        let mut latencies: Vec<Duration> = (0..20).map(|_| measure()).collect();
        latencies.sort();
        let p95 = latencies[latencies.len() * 95 / 100 - 1];
        if p95 <= limit && *latencies.last().unwrap() <= Duration::from_millis(250) {
            return;
        }
        windows.push(latencies);
    }
    panic!("{name}: no window had p95 <= {limit:?} and max <= 250ms: {windows:?}");
}

// ---------------------------------------------------------------------------
// Legacy data directory and downgrade safety

#[test]
fn legacy_datadir_opens_equal_to_the_legacy_supervisor_view() {
    let _faults = shared();
    let fx = copy_legacy_datadir();
    let config = Config::load(&fx.config).unwrap();
    let (projects, list, peers) = legacy_view(&config);
    let initial_sync = raw_initial_sync(&config);
    let pairing = raw_setting(&config, "pairing").expect("fixture has a verifier");
    let raw_peers: Vec<(String, String, String)> = raw(&config)
        .prepare("SELECT endpoint,code,revision FROM peers ORDER BY endpoint")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        raw_setting(&config, "catalog_schema"),
        None,
        "fresh fixture"
    );

    let catalog = Catalog::open(&config).unwrap();
    let snap = catalog.snapshot();
    assert_eq!(snap.generation, 1);
    assert_eq!(snap.installation, list["installation"].as_str().unwrap());
    assert_eq!(
        snap.installation,
        fx.manifest["installation"].as_str().unwrap()
    );

    // projects: same rows, same order (label, state), default intents.
    assert_eq!(snap.folders.len(), projects.len());
    assert_eq!(snap.folders.len(), 3);
    for (record, project) in snap.folders.iter().zip(&projects) {
        assert_eq!(record.state, project.state);
        assert_eq!(record.label, project.label);
        assert_eq!(record.repo, project.repo);
        assert_eq!(record.device, project.device);
        assert_eq!(record.directory, project.directory);
        assert_eq!(record.mount, project.mount);
        assert_eq!(record.desired_running, project.desired_running);
        assert!(record.sync_enabled && record.mount_enabled, "{record:?}");
    }

    // initial_sync: one gate per pending folder, backfilled into mount_gates
    // with the source installation resolved through peers.
    let pending: Vec<&str> = list["folders"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|f| f["initial_sync_pending"] == true)
        .map(|f| f["state"].as_str().unwrap())
        .collect();
    assert_eq!(pending.len(), 1);
    assert_eq!(
        snap.gates.keys().map(String::as_str).collect::<Vec<_>>(),
        pending
    );
    assert_eq!(initial_sync.len(), 1);
    for (state, endpoint) in &initial_sync {
        let gate = &snap.gates[state];
        assert_eq!(&gate.state, state);
        assert_eq!(&gate.source_endpoint, endpoint);
        assert_eq!(gate.source_endpoint, legacy::DEAD_PEER_ENDPOINT);
        assert_eq!(
            gate.source_installation.as_deref(),
            fx.manifest["peers"][0]["installation"].as_str()
        );
        assert_eq!(gate.override_ms, None);
        assert!(gate.created_ms > 0);
    }

    // peers: the legacy Peers reply plus the stored code and revision.
    let peers = peers.as_array().unwrap();
    assert_eq!(snap.peers.len(), peers.len());
    for ((record, shown), (endpoint, code, revision)) in
        snap.peers.iter().zip(peers).zip(&raw_peers)
    {
        assert_eq!(record.endpoint, shown["endpoint"].as_str().unwrap());
        assert_eq!(record.installation, shown["installation"].as_str().unwrap());
        assert_eq!(
            u64::from(record.connections),
            shown["configured_connections"].as_u64().unwrap()
        );
        assert_eq!(&record.endpoint, endpoint);
        assert_eq!(&record.code, code);
        assert_eq!(&record.revision, revision);
    }

    // verifier: the legacy `settings.pairing` value, first generation.
    let verifier: Verifier = serde_json::from_str(&pairing).unwrap();
    assert_eq!(snap.verifier.as_ref(), Some(&verifier));
    assert_eq!(snap.verifier_generation, 1);
    assert!(snap.peer_meta.is_empty());
    assert_eq!(raw_setting(&config, "catalog_schema").as_deref(), Some("1"));
    assert_eq!(
        raw_setting(&config, "verifier_generation").as_deref(),
        Some("1")
    );

    // Downgrade: after the migration the legacy view is unchanged.
    drop(catalog);
    let (projects_after, list_after, peers_after) = legacy_view(&config);
    assert_eq!(list_after, list);
    assert_eq!(peers_after, Value::Array(peers.clone()));
    assert_eq!(
        serde_json::to_value(&projects_after).unwrap(),
        serde_json::to_value(&projects).unwrap()
    );

    // A second open is a no-op migration with the same content.
    let catalog = Catalog::open(&config).unwrap();
    assert_same_content(&catalog.snapshot(), &snap);
}

#[test]
fn downgrade_legacy_daemon_creates_pairs_and_rotates_on_a_migrated_catalog() {
    let _faults = shared();
    let fx = copy_legacy_datadir();
    let config = Config::load(&fx.config).unwrap();
    let legacy_peer = fx.manifest["peers"][0]["installation"]
        .as_str()
        .unwrap()
        .to_owned();

    // The new code writes every side table.
    let catalog = Catalog::open(&config).unwrap();
    let mut gated = folder(&config, "Catalog gated");
    gated.desired_running = false;
    gated.sync_enabled = false;
    gated.mount = Some(
        validate_mount_path(
            &catalog.snapshot(),
            &config.data_directory,
            &gated.directory,
            &config.default_mount_directory.join("Catalog gated"),
        )
        .unwrap(),
    );
    catalog
        .apply(create(&gated, Some(gate_from(legacy::DEAD_PEER_ENDPOINT))))
        .unwrap();
    catalog
        .apply(CatalogCmd::OverrideGate {
            state: gated.state.clone(),
        })
        .unwrap();
    catalog
        .apply(CatalogCmd::SetVerifier {
            verifier: Verifier::generate().1,
        })
        .unwrap();
    catalog
        .apply(CatalogCmd::RecordPeerSeen {
            installation: legacy_peer.clone(),
            at_ms: 5,
            display_name: Some("legacy peer".into()),
            incompatible: None,
        })
        .unwrap();
    let before = catalog.snapshot();
    assert_eq!(before.verifier_generation, 2);
    assert_eq!(raw_initial_sync(&config).len(), 2);
    drop(catalog);

    // A downgraded daemon (the legacy Supervisor of this build) runs on it.
    let mut a = Node::adopt_root(fx.root, NodeConfig::default());
    let list = a.list();
    let row = list["folders"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["state"] == gated.state.as_str())
        .unwrap_or_else(|| panic!("legacy List misses the new folder: {list}"));
    assert_eq!(row["initial_sync_pending"], true, "{row}");
    assert_eq!(row["desired_running"], false, "{row}");
    assert_eq!(list["folders"].as_array().unwrap().len(), 4);
    // Legacy create(): the positional `projects` INSERT.
    let created = a.create("Downgrade local", true);
    let created_state = created["state"].as_str().unwrap().to_owned();
    // Legacy Pair: the positional `peers` upsert.
    let b = Node::new();
    let code = b.code();
    let paired = a.pair(&b, &code);
    assert_eq!(paired["installation"], b.installation().as_str());
    // Legacy ResetPairingCode: the `settings.pairing` upsert.
    a.code();
    a.stop();

    // Upgrade again: everything the legacy daemon wrote is in the snapshot.
    let catalog = Catalog::open(&config).unwrap();
    let snap = catalog.snapshot();
    let local = snap.folder(&created_state).expect("legacy-created folder");
    assert_eq!(local.label, "Downgrade local");
    assert!(local.desired_running && local.sync_enabled && local.mount_enabled);
    assert_eq!(local.mount, None);
    assert!(!snap.gates.contains_key(&created_state));
    let kept = snap.folder(&gated.state).unwrap();
    assert_eq!(kept, &gated);
    assert_eq!(snap.gates[&gated.state], before.gates[&gated.state]);
    let paired = snap.peer_by_endpoint(&b.endpoint()).expect("legacy pair");
    assert_eq!(paired.installation, b.installation());
    assert_eq!(paired.code, code);
    assert_eq!(paired.connections, 3);
    assert!(!paired.revision.is_empty());
    assert_eq!(snap.peers.len(), 2);
    // The legacy rotation is a new verifier generation.
    assert_ne!(snap.verifier, before.verifier);
    assert_eq!(snap.verifier_generation, before.verifier_generation + 1);
    assert_eq!(
        snap.peer_meta[&legacy_peer].display_name.as_deref(),
        Some("legacy peer")
    );
}

#[test]
fn gate_changes_made_while_downgraded_are_reconciled_on_open() {
    let _faults = shared();
    let env = env();
    let config = &env.config;
    let catalog = Catalog::open(config).unwrap();
    catalog
        .apply(CatalogCmd::UpsertPeer {
            record: peer("ws://source:1", "inst-source"),
        })
        .unwrap();
    let mounts = fs::canonicalize(&config.default_mount_directory).unwrap();
    let mut satisfied = folder(config, "Satisfied");
    satisfied.mount = Some(mounts.join("Satisfied"));
    let mut received = folder(config, "Received");
    received.mount = Some(mounts.join("Received"));
    catalog
        .apply(create(&satisfied, Some(gate_from("ws://source:1"))))
        .unwrap();
    catalog.apply(create(&received, None)).unwrap();
    catalog
        .apply(CatalogCmd::SetVerifier {
            verifier: Verifier::generate().1,
        })
        .unwrap();
    let before = catalog.snapshot();
    assert_eq!(before.verifier_generation, 1);
    assert!(before.gates.contains_key(&satisfied.state));
    drop(catalog);

    // What a downgraded binary does, with its own SQL: its gate rule is
    // satisfied (`delete_initial_sync`), it receives a mounted folder
    // (`insert_initial_sync_tx`), and the user rotates the code.
    let db = raw(config);
    db.execute("DELETE FROM initial_sync WHERE state=?", [&satisfied.state])
        .unwrap();
    db.execute(
        "INSERT INTO initial_sync VALUES(?,?)",
        [&received.state, "ws://source:1"],
    )
    .unwrap();
    let rotated = Verifier::generate().1;
    db.execute(
        "INSERT INTO settings VALUES('pairing',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        [serde_json::to_string(&rotated).unwrap()],
    )
    .unwrap();
    drop(db);

    let catalog = Catalog::open(config).unwrap();
    let snap = catalog.snapshot();
    assert!(!snap.gates.contains_key(&satisfied.state), "pruned");
    let gate = &snap.gates[&received.state];
    assert_eq!(gate.source_endpoint, "ws://source:1");
    assert_eq!(gate.source_installation.as_deref(), Some("inst-source"));
    assert_eq!(snap.verifier.as_ref(), Some(&rotated));
    assert_eq!(snap.verifier_generation, 2);
    assert_eq!(raw_count(config, "mount_gates", &satisfied.state), 0);
    // Reopening again changes nothing.
    drop(catalog);
    let catalog = Catalog::open(config).unwrap();
    assert_same_content(&catalog.snapshot(), &snap);
}

// ---------------------------------------------------------------------------
// Commands and generations

#[test]
fn every_command_bumps_the_generation_exactly_once_per_change() {
    let _faults = shared();
    let env = env();
    let config = &env.config;
    let catalog = Catalog::open(config).unwrap();
    let own = catalog.snapshot().installation.clone();
    assert!(!own.is_empty());
    let mut steps = Steps {
        catalog: &catalog,
        generation: 1,
    };
    assert_eq!(catalog.snapshot().generation, 1);

    // Peers.
    let snap = steps.changed(CatalogCmd::UpsertPeer {
        record: peer("ws://a:1", "inst-a"),
    });
    let a = snap.peer_by_endpoint("ws://a:1").unwrap().clone();
    assert_eq!(a.code, "code-for-inst-a");
    assert!(!a.revision.is_empty(), "an empty revision gets a fresh id");
    let snap = steps.changed(CatalogCmd::UpsertPeer {
        record: PeerRecord {
            revision: "r2".into(),
            ..peer("ws://a:1", "inst-a")
        },
    });
    assert_eq!(snap.peer_by_installation("inst-a").unwrap().revision, "r2");
    steps.unchanged(CatalogCmd::UpsertPeer {
        record: PeerRecord {
            revision: "r2".into(),
            ..peer("ws://a:1", "inst-a")
        },
    });
    steps.changed(CatalogCmd::UpsertPeer {
        record: peer("ws://b:2", "inst-b"),
    });
    steps.fails(
        CatalogCmd::UpsertPeer {
            record: peer("ws://self:3", &own),
        },
        "CANNOT_PAIR_WITH_SELF",
    );
    steps.fails(
        CatalogCmd::UpsertPeer {
            record: PeerRecord {
                connections: 0,
                ..peer("ws://c:3", "inst-c")
            },
        },
        "INVALID_CONNECTION_COUNT",
    );
    steps.fails(
        CatalogCmd::UpsertPeer {
            record: peer("", "inst-c"),
        },
        "INVALID_REQUEST",
    );

    // Folders.
    let mounts = fs::canonicalize(&config.default_mount_directory).unwrap();
    let plain = folder(config, "Plain");
    let snap = steps.changed(create(&plain, None));
    assert_eq!(snap.folder(&plain.state), Some(&plain));
    assert_eq!(snap.folder_by_repo(&plain.repo), Some(&plain));
    let mut gated = folder(config, "Gated");
    gated.mount = Some(mounts.join("Gated"));
    let snap = steps.changed(create(&gated, Some(gate_from("ws://a:1"))));
    let gate = snap.gates[&gated.state].clone();
    assert_eq!(gate.state, gated.state);
    assert_eq!(gate.source_installation.as_deref(), Some("inst-a"));
    assert_eq!(gate.source_endpoint, "ws://a:1");
    assert!(gate.created_ms > 0 && gate.override_ms.is_none());
    assert_eq!(
        raw_initial_sync(config),
        vec![(gated.state.clone(), "ws://a:1".to_owned())]
    );
    let mut headless = folder(config, "Headless");
    headless.desired_running = false;
    headless.sync_enabled = false;
    headless.mount_enabled = false;
    let snap = steps.changed(create(&headless, Some(gate_from("ws://a:1"))));
    assert_eq!(snap.folder(&headless.state), Some(&headless));
    assert!(
        !snap.gates.contains_key(&headless.state),
        "no gate without a mount (legacy create rule)"
    );
    // Sorted as the legacy query: label, then state.
    let labels: Vec<_> = snap.folders.iter().map(|f| f.label.as_str()).collect();
    assert_eq!(labels, ["Gated", "Headless", "Plain"]);

    let duplicate_repo = FolderRecord {
        repo: plain.repo.clone(),
        ..folder(config, "Other")
    };
    steps.fails(create(&duplicate_repo, None), "REPOSITORY_ALREADY_MANAGED");
    let duplicate_state = FolderRecord {
        state: plain.state.clone(),
        ..folder(config, "Other")
    };
    steps.fails(create(&duplicate_state, None), "INVALID_REQUEST");
    steps.fails(create(&folder(config, "  "), None), "INVALID_FOLDER_LABEL");
    steps.fails(
        create(&folder(config, &"x".repeat(201)), None),
        "INVALID_FOLDER_LABEL",
    );
    let mut nested = folder(config, "Nested");
    nested.mount = Some(mounts.join("Gated").join("inner"));
    steps.fails(create(&nested, None), "MOUNT_OVERLAPS_FOLDER");

    // SetDesired.
    let snap = steps.changed(set_desired(&plain.state, false));
    assert!(!snap.folder(&plain.state).unwrap().desired_running);
    steps.unchanged(set_desired(&plain.state, false));
    steps.changed(set_desired(&plain.state, true));
    steps.fails(set_desired("missing", true), "FOLDER_NOT_FOUND");

    // SetMount.
    let snap = steps.changed(CatalogCmd::SetMount {
        state: plain.state.clone(),
        mount: Some(mounts.join("Plain")),
    });
    assert_eq!(
        snap.folder(&plain.state).unwrap().mount,
        Some(mounts.join("Plain"))
    );
    steps.fails(
        CatalogCmd::SetMount {
            state: plain.state.clone(),
            mount: Some(mounts.join("Gated").join("x")),
        },
        "MOUNT_OVERLAPS_FOLDER",
    );
    steps.unchanged(CatalogCmd::SetMount {
        state: plain.state.clone(),
        mount: Some(mounts.join("Plain")),
    });
    steps.changed(CatalogCmd::SetMount {
        state: plain.state.clone(),
        mount: None,
    });
    steps.fails(
        CatalogCmd::SetMount {
            state: "missing".into(),
            mount: None,
        },
        "FOLDER_NOT_FOUND",
    );

    // SetIntents.
    let snap = steps.changed(CatalogCmd::SetIntents {
        state: plain.state.clone(),
        sync_enabled: Some(false),
        mount_enabled: None,
    });
    let record = snap.folder(&plain.state).unwrap();
    assert!(!record.sync_enabled && record.mount_enabled);
    steps.unchanged(CatalogCmd::SetIntents {
        state: plain.state.clone(),
        sync_enabled: Some(false),
        mount_enabled: None,
    });
    steps.unchanged(CatalogCmd::SetIntents {
        state: plain.state.clone(),
        sync_enabled: None,
        mount_enabled: None,
    });
    let snap = steps.changed(CatalogCmd::SetIntents {
        state: plain.state.clone(),
        sync_enabled: Some(true),
        mount_enabled: Some(false),
    });
    let record = snap.folder(&plain.state).unwrap();
    assert!(record.sync_enabled && !record.mount_enabled);
    steps.fails(
        CatalogCmd::SetIntents {
            state: "missing".into(),
            sync_enabled: Some(true),
            mount_enabled: None,
        },
        "FOLDER_NOT_FOUND",
    );

    // OverrideGate keeps the first override.
    let snap = steps.changed(CatalogCmd::OverrideGate {
        state: gated.state.clone(),
    });
    let overridden = snap.gates[&gated.state].override_ms.expect("override_ms");
    let snap = steps.unchanged(CatalogCmd::OverrideGate {
        state: gated.state.clone(),
    });
    assert_eq!(snap.gates[&gated.state].override_ms, Some(overridden));
    steps.unchanged(CatalogCmd::OverrideGate {
        state: plain.state.clone(),
    });
    steps.fails(
        CatalogCmd::OverrideGate {
            state: "missing".into(),
        },
        "FOLDER_NOT_FOUND",
    );

    // RetargetGate moves both markers.
    let snap = steps.changed(CatalogCmd::RetargetGate {
        state: gated.state.clone(),
        source_installation: None,
        source_endpoint: "ws://b:2".into(),
    });
    let gate = &snap.gates[&gated.state];
    assert_eq!(gate.source_installation.as_deref(), Some("inst-b"));
    assert_eq!(gate.source_endpoint, "ws://b:2");
    assert_eq!(gate.override_ms, Some(overridden));
    assert_eq!(
        raw_initial_sync(config),
        vec![(gated.state.clone(), "ws://b:2".to_owned())]
    );
    steps.fails(
        CatalogCmd::RetargetGate {
            state: gated.state.clone(),
            source_installation: None,
            source_endpoint: String::new(),
        },
        "INVALID_REQUEST",
    );

    // ClearGate deletes both markers.
    let snap = steps.changed(CatalogCmd::ClearGate {
        state: gated.state.clone(),
    });
    assert!(snap.gates.is_empty());
    assert_eq!(raw_count(config, "initial_sync", &gated.state), 0);
    assert_eq!(raw_count(config, "mount_gates", &gated.state), 0);
    steps.unchanged(CatalogCmd::ClearGate {
        state: gated.state.clone(),
    });
    steps.fails(
        CatalogCmd::ClearGate {
            state: "missing".into(),
        },
        "FOLDER_NOT_FOUND",
    );

    // RecordPeerSeen: last_seen never goes back, a missing name is kept, the
    // incompatibility is replaced.
    let snap = steps.changed(CatalogCmd::RecordPeerSeen {
        installation: "inst-a".into(),
        at_ms: 10,
        display_name: Some("A".into()),
        incompatible: None,
    });
    assert_eq!(snap.peer_meta["inst-a"].last_seen_ms, Some(10));
    let snap = steps.changed(CatalogCmd::RecordPeerSeen {
        installation: "inst-a".into(),
        at_ms: 5,
        display_name: None,
        incompatible: Some("PEER_NEEDS_UPGRADE".into()),
    });
    let meta = &snap.peer_meta["inst-a"];
    assert_eq!(meta.installation, "inst-a");
    assert_eq!(meta.last_seen_ms, Some(10));
    assert_eq!(meta.display_name.as_deref(), Some("A"));
    assert_eq!(meta.incompatible.as_deref(), Some("PEER_NEEDS_UPGRADE"));
    assert_eq!(meta.revoked_ms, None);
    steps.unchanged(CatalogCmd::RecordPeerSeen {
        installation: "inst-a".into(),
        at_ms: 7,
        display_name: None,
        incompatible: Some("PEER_NEEDS_UPGRADE".into()),
    });
    // An unpaired (inbound-only) peer has metadata too.
    steps.changed(CatalogCmd::RecordPeerSeen {
        installation: "inst-inbound".into(),
        at_ms: 11,
        display_name: None,
        incompatible: None,
    });
    steps.fails(
        CatalogCmd::RecordPeerSeen {
            installation: String::new(),
            at_ms: 1,
            display_name: None,
            incompatible: None,
        },
        "INVALID_REQUEST",
    );

    // SetVerifier bumps the durable verifier generation.
    let (code, first) = Verifier::generate();
    let snap = steps.changed(CatalogCmd::SetVerifier {
        verifier: first.clone(),
    });
    assert_eq!(snap.verifier.as_ref(), Some(&first));
    assert!(snap.verifier.as_ref().unwrap().verify(&code));
    assert_eq!(snap.verifier_generation, 1);
    let second = Verifier::generate().1;
    let snap = steps.changed(CatalogCmd::SetVerifier {
        verifier: second.clone(),
    });
    assert_eq!(snap.verifier_generation, 2);
    assert_eq!(
        raw_setting(config, "verifier_generation").as_deref(),
        Some("2")
    );

    // RemovePeer (legacy Unpair: removing a missing peer is not an error).
    let snap = steps.changed(CatalogCmd::RemovePeer {
        endpoint: "ws://b:2".into(),
    });
    assert!(snap.peer_by_endpoint("ws://b:2").is_none());
    assert!(
        snap.peer_meta.contains_key("inst-a"),
        "metadata outlives a pair"
    );
    steps.unchanged(CatalogCmd::RemovePeer {
        endpoint: "ws://b:2".into(),
    });

    // Secrets never reach Debug output (invariant 21).
    let debug = format!("{:?} {:?}", catalog.snapshot(), catalog);
    assert!(!debug.contains(&second.hash) && !debug.contains("code-for-inst-a"));

    // Everything is durable; a reopened catalog starts at generation 1.
    let last = catalog.snapshot();
    assert_eq!(last.generation, steps.generation);
    drop(catalog);
    let catalog = Catalog::open(config).unwrap();
    let reopened = catalog.snapshot();
    assert_eq!(reopened.generation, 1);
    assert_same_content(&reopened, &last);
}

#[test]
fn concurrent_commands_are_serialized_and_readers_never_wait() {
    let _faults = shared();
    let env = env();
    let catalog = Catalog::open(&env.config).unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader = {
        let catalog = catalog.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            let mut last = 0;
            let mut reads = 0u64;
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                let snap = catalog.snapshot();
                assert!(snap.generation >= last, "generations never go back");
                assert_eq!(snap.peer_meta.len() as u64 + 1, snap.generation);
                last = snap.generation;
                reads += 1;
                thread::yield_now();
            }
            reads
        })
    };
    let writers: Vec<_> = (0..8)
        .map(|w| {
            let catalog = catalog.clone();
            thread::spawn(move || {
                for i in 0..10 {
                    let snap = catalog
                        .apply(CatalogCmd::RecordPeerSeen {
                            installation: format!("inst-{w}-{i}"),
                            at_ms: 1 + i,
                            display_name: None,
                            incompatible: None,
                        })
                        .unwrap();
                    assert!(snap.peer_meta.contains_key(&format!("inst-{w}-{i}")));
                }
            })
        })
        .collect();
    for writer in writers {
        writer.join().unwrap();
    }
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(reader.join().unwrap() > 0);
    let snap = catalog.snapshot();
    assert_eq!(snap.generation, 81);
    assert_eq!(snap.peer_meta.len(), 80);
}

#[test]
fn creates_validated_against_one_snapshot_cannot_both_take_a_mount() {
    let _faults = shared();
    let env = env();
    let config = &env.config;
    let catalog = Catalog::open(config).unwrap();
    let snap = catalog.snapshot();
    let mut first = folder(config, "First");
    let mut second = folder(config, "Second");
    let requested = config.default_mount_directory.join("Shared");
    first.mount = Some(
        validate_mount_path(&snap, &config.data_directory, &first.directory, &requested).unwrap(),
    );
    second.mount = Some(
        validate_mount_path(
            &snap,
            &config.data_directory,
            &second.directory,
            &requested.join("inside"),
        )
        .unwrap(),
    );
    catalog.apply(create(&first, None)).unwrap();
    let error = catalog.apply(create(&second, None)).unwrap_err();
    assert_eq!(error.code, "MOUNT_OVERLAPS_FOLDER");
    assert_eq!(error.class, ErrorClass::NeedsUserAction);
    assert_eq!(catalog.snapshot().folders.len(), 1);
}

// ---------------------------------------------------------------------------
// Failure handling

#[test]
fn snapshot_p99_under_1ms_while_a_command_is_held() {
    let faults = exclusive();
    let env = env();
    let catalog = Catalog::open(&env.config).unwrap();
    let record = folder(&env.config, "Held");
    catalog.apply(create(&record, None)).unwrap();
    let before = catalog.snapshot().generation;
    faults.arm(
        catalog::faults::SLOW_WRITE,
        FaultAction::Hang,
        Some(500),
        Some(1),
    );
    let writer = {
        let catalog = catalog.clone();
        let state = record.state.clone();
        thread::spawn(move || {
            let started = Instant::now();
            let result = catalog.apply(set_desired(&state, false));
            (result, started.elapsed())
        })
    };
    // Sample for 400 ms of the 500 ms hold. No yield between samples: under
    // a fully loaded machine `yield_now` hands the core away for a quantum.
    let window = Instant::now();
    let mut samples = Vec::with_capacity(100_000);
    while window.elapsed() < Duration::from_millis(400) && samples.len() < 100_000 {
        let started = Instant::now();
        let snap = catalog.snapshot();
        samples.push(started.elapsed());
        assert_eq!(snap.generation, before, "the held command is not visible");
        for _ in 0..64 {
            std::hint::spin_loop();
        }
    }
    let (result, held) = writer.join().unwrap();
    let snap = result.unwrap();
    assert!(held >= Duration::from_millis(500), "held {held:?}");
    assert_eq!(snap.generation, before + 1);
    assert!(!snap.folder(&record.state).unwrap().desired_running);
    samples.sort();
    let p99 = samples[samples.len() * 99 / 100];
    assert!(samples.len() >= 100, "{} samples", samples.len());
    assert!(
        p99 < Duration::from_millis(1),
        "snapshot() p99 {p99:?} over {} samples (max {:?})",
        samples.len(),
        samples.last().unwrap()
    );
}

#[test]
fn a_panicking_command_errors_and_the_next_one_succeeds() {
    let faults = exclusive();
    let env = env();
    let config = &env.config;
    let catalog = Catalog::open(config).unwrap();
    let record = folder(config, "Panics");
    catalog.apply(create(&record, None)).unwrap();
    let before = catalog.snapshot();
    {
        faults.arm(
            catalog::faults::SLOW_WRITE,
            FaultAction::Panic,
            None,
            Some(1),
        );
        let error = catalog
            .apply(set_desired(&record.state, false))
            .unwrap_err();
        assert_eq!(error.code, "CATALOG_UNAVAILABLE", "{error}");
        assert_eq!(error.class, ErrorClass::Transient);
        assert!(error.message.contains("panicked"), "{error}");
        assert!(catalog.is_alive());
        let snap = catalog.snapshot();
        assert_eq!(snap.generation, before.generation);
        assert!(snap.folder(&record.state).unwrap().desired_running);
        // The next command succeeds and the panicked one left nothing behind.
        let snap = catalog.apply(set_desired(&record.state, false)).unwrap();
        assert_eq!(snap.generation, before.generation + 1);
        assert!(!snap.folder(&record.state).unwrap().desired_running);
        faults.disarm();
    }

    // A failing command (fault `fail`) is rolled back as a whole.
    let mut gated = folder(config, "Fails");
    gated.mount = Some(
        fs::canonicalize(&config.default_mount_directory)
            .unwrap()
            .join("Fails"),
    );
    let generation = catalog.snapshot().generation;
    {
        faults.arm(
            catalog::faults::SLOW_WRITE,
            FaultAction::Fail,
            None,
            Some(1),
        );
        let error = catalog
            .apply(create(&gated, Some(gate_from("ws://somewhere:1"))))
            .unwrap_err();
        assert_eq!(error.code, "FAULT_INJECTED");
        assert_eq!(code_of(&error.clone().into_anyhow()), "FAULT_INJECTED");
        assert_eq!(catalog.snapshot().generation, generation);
        for table in ["projects", "folder_intents", "initial_sync", "mount_gates"] {
            assert_eq!(raw_count(config, table, &gated.state), 0, "{table}");
        }
        let snap = catalog
            .apply(create(&gated, Some(gate_from("ws://somewhere:1"))))
            .unwrap();
        assert_eq!(snap.generation, generation + 1);
        assert!(snap.gates.contains_key(&gated.state));
    }
}

#[test]
fn a_dead_catalog_thread_answers_unavailable_and_keeps_the_last_snapshot() {
    let faults = exclusive();
    let env = env();
    let catalog = Catalog::open(&env.config).unwrap();
    let record = folder(&env.config, "Survivor");
    let last = catalog.apply(create(&record, None)).unwrap();
    faults.arm(catalog::faults::ACTOR, FaultAction::Panic, None, Some(1));
    let error = catalog
        .apply(set_desired(&record.state, false))
        .unwrap_err();
    assert_eq!(error.code, "CATALOG_UNAVAILABLE", "{error}");
    faults.disarm();
    let deadline = Instant::now() + Duration::from_secs(5);
    while catalog.is_alive() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    assert!(!catalog.is_alive());
    for _ in 0..3 {
        let error = catalog
            .apply(set_desired(&record.state, false))
            .unwrap_err();
        assert_eq!(error.code, "CATALOG_UNAVAILABLE", "{error}");
        assert_eq!(error.class, ErrorClass::Transient);
    }
    let snap = catalog.snapshot();
    assert!(Arc::ptr_eq(&snap, &last), "the last good snapshot is kept");
    let started = Instant::now();
    let waited = catalog.wait_newer(snap.generation, Duration::from_millis(50));
    assert!(started.elapsed() >= Duration::from_millis(50));
    assert!(Arc::ptr_eq(&waited, &last));
    // The dead thread released orchestrator.lock; the data is intact.
    drop(catalog);
    let catalog = Catalog::open(&env.config).unwrap();
    assert_same_content(&catalog.snapshot(), &last);
}

/// Another connection holds the SQLite writer slot until released (or for
/// `hold`). Returns once it holds it.
fn hold_writer(path: &Path, hold: Duration) -> (Sender<()>, JoinHandle<()>) {
    let (release, released) = mpsc::channel::<()>();
    let (held_tx, held): (Sender<()>, Receiver<()>) = mpsc::channel();
    let path = path.to_path_buf();
    let thread = thread::spawn(move || {
        let db = Connection::open(path).unwrap();
        db.execute_batch("BEGIN IMMEDIATE").unwrap();
        held_tx.send(()).unwrap();
        let _ = released.recv_timeout(hold);
        db.execute_batch("COMMIT").unwrap();
    });
    held.recv().unwrap();
    (release, thread)
}

#[test]
fn sqlite_busy_is_retried_then_reported() {
    let _faults = shared();
    let env = env();
    let config = &env.config;
    let catalog = Catalog::open(config).unwrap();
    let record = folder(config, "Busy");
    catalog.apply(create(&record, None)).unwrap();
    let generation = catalog.snapshot().generation;

    // Busy for 1.5 s: longer than one busy-handler budget, so the command
    // only succeeds through a retry.
    let (_release, holder) = hold_writer(&db_path(config), Duration::from_millis(1500));
    let started = Instant::now();
    let snap = catalog.apply(set_desired(&record.state, false)).unwrap();
    assert!(started.elapsed() >= Duration::from_millis(1000));
    assert_eq!(snap.generation, generation + 1);
    holder.join().unwrap();

    // Busy for longer than every retry: SQLITE_BUSY (Transient), nothing
    // published, and the catalog keeps working afterwards.
    let (release, holder) = hold_writer(&db_path(config), Duration::from_secs(60));
    let started = Instant::now();
    let error = catalog.apply(set_desired(&record.state, true)).unwrap_err();
    let waited = started.elapsed();
    release.send(()).unwrap();
    holder.join().unwrap();
    assert_eq!(error.code, "SQLITE_BUSY", "{error}");
    assert_eq!(error.class, ErrorClass::Transient);
    let budget = catalog::BUSY_TIMEOUT * (catalog::BUSY_RETRIES + 1);
    assert!(waited >= budget * 3 / 4, "gave up after {waited:?}");
    assert!(waited < budget * 3, "gave up after {waited:?}");
    let snap = catalog.snapshot();
    assert_eq!(snap.generation, generation + 1);
    assert!(!snap.folder(&record.state).unwrap().desired_running);
    let snap = catalog.apply(set_desired(&record.state, true)).unwrap();
    assert_eq!(snap.generation, generation + 2);
}

#[test]
fn one_catalog_owns_a_data_directory() {
    let _faults = shared();
    let env = env();
    let catalog = Catalog::open(&env.config).unwrap();
    assert!(catalog.is_alive());
    assert_eq!(
        catalog.data_directory(),
        env.config.data_directory.as_path()
    );
    assert!(env.config.default_mount_directory.is_dir());
    let error = Catalog::open(&env.config).unwrap_err();
    assert!(code_of(&error).starts_with("ALREADY_RUNNING"), "{error:#}");
    let Err(error) = Supervisor::open(env.config.clone()) else {
        panic!("the legacy supervisor opened a data directory the catalog owns");
    };
    assert!(code_of(&error).starts_with("ALREADY_RUNNING"), "{error:#}");

    // close() releases the lock; snapshot() still answers, apply does not.
    let record = folder(&env.config, "Closed");
    let last = catalog.apply(create(&record, None)).unwrap();
    assert!(catalog.close());
    assert!(!catalog.is_alive());
    let error = catalog
        .apply(set_desired(&record.state, false))
        .unwrap_err();
    assert_eq!(error.code, "CATALOG_UNAVAILABLE");
    assert!(Arc::ptr_eq(&catalog.snapshot(), &last));
    let supervisor = Supervisor::open(env.config.clone()).unwrap();
    assert_eq!(supervisor.projects().unwrap().len(), 1);
    drop(supervisor);
    drop(catalog);
    let catalog = Catalog::open(&env.config).unwrap();
    assert_same_content(&catalog.snapshot(), &last);
}

// ---------------------------------------------------------------------------
// Mount paths

struct Layout {
    _root: tempfile::TempDir,
    root: PathBuf,
    config: Config,
    target: FolderRecord,
    snapshot: Arc<CatalogSnapshot>,
}

/// A data directory with folders A (`mounts/A`), B (`mounts/y/B`, parent not
/// created) and the target T (`mounts/T` when `own_mount`), plus an existing
/// directory and file under `mounts`. The catalog is closed afterwards so the
/// legacy supervisor can open the same directory.
fn layout(own_mount: bool) -> Layout {
    let Env { root, config } = env();
    let catalog = Catalog::open(&config).unwrap();
    let mounts = fs::canonicalize(&config.default_mount_directory).unwrap();
    let mut a = folder(&config, "A");
    a.mount = Some(mounts.join("A"));
    let mut b = folder(&config, "B");
    b.mount = Some(mounts.join("y").join("B"));
    let mut target = folder(&config, "T");
    if own_mount {
        target.mount = Some(mounts.join("T"));
    }
    for record in [&a, &b, &target] {
        catalog.apply(create(record, None)).unwrap();
    }
    fs::create_dir(mounts.join("existing-dir")).unwrap();
    fs::write(mounts.join("existing-file"), b"x").unwrap();
    let snapshot = catalog.snapshot();
    drop(catalog);
    Layout {
        root: root.path().to_path_buf(),
        _root: root,
        config,
        target,
        snapshot,
    }
}

/// The legacy rule, through the legacy `ConfigureMount` (which calls
/// `Supervisor::mount_path` for a changed mount): the stored path or the code.
fn legacy_mount_path(layout: &Layout, requested: &Path) -> Result<PathBuf, String> {
    let supervisor = Supervisor::open(layout.config.clone()).unwrap();
    let shared = Arc::new(Mutex::new(supervisor));
    orchestrator::manage(
        &shared,
        Action::ConfigureMount {
            state: layout.target.state.clone(),
            mount: Some(requested.to_path_buf()),
        },
    )
    .map(|inspect| PathBuf::from(inspect["mount"].as_str().unwrap()))
    .map_err(|error| code_of(&error))
}

type Requested = fn(&Layout) -> PathBuf;

/// One fixture case: the requested path, whether the target folder has its
/// own mount, and the expected code (`None`: accepted).
fn case(
    name: &'static str,
    own_mount: bool,
    requested: Requested,
    expected: Option<&'static str>,
) -> (&'static str, bool, Requested, Option<&'static str>) {
    (name, own_mount, requested, expected)
}

#[test]
fn validate_mount_path_matches_the_legacy_mount_path() {
    let _faults = shared();
    let cases = vec![
        case(
            "a new name in the default mount directory",
            false,
            |l| l.config.default_mount_directory.join("fresh"),
            None,
        ),
        case(
            "missing parents are created",
            false,
            |l| l.root.join("deep").join("a").join("b").join("mnt"),
            None,
        ),
        case(
            "an existing directory",
            false,
            |l| l.config.default_mount_directory.join("existing-dir"),
            Some("MOUNT_PATH_MUST_NOT_EXIST"),
        ),
        case(
            "an existing file",
            false,
            |l| l.config.default_mount_directory.join("existing-file"),
            Some("MOUNT_PATH_MUST_NOT_EXIST"),
        ),
        case(
            "inside the data directory",
            false,
            |l| l.config.data_directory.join("inside"),
            Some("MOUNT_OVERLAPS_DATA"),
        ),
        case(
            "the directory above the data directory",
            false,
            |l| l.root.clone(),
            Some("MOUNT_OVERLAPS_DATA"),
        ),
        case(
            "inside the folder's state directory",
            false,
            |l| l.target.directory.join("mnt"),
            Some("MOUNT_OVERLAPS_DATA"),
        ),
        case(
            "inside another folder's mount",
            false,
            |l| l.config.default_mount_directory.join("A").join("sub"),
            Some("MOUNT_OVERLAPS_FOLDER"),
        ),
        case(
            "above another folder's mount",
            false,
            |l| l.config.default_mount_directory.join("y"),
            Some("MOUNT_OVERLAPS_FOLDER"),
        ),
        case(
            "another folder's mount itself",
            false,
            |l| l.config.default_mount_directory.join("A"),
            Some("MOUNT_OVERLAPS_FOLDER"),
        ),
        case(
            "inside the folder's own current mount",
            true,
            |l| l.config.default_mount_directory.join("T").join("inner"),
            Some("MOUNT_OVERLAPS_FOLDER"),
        ),
        case(
            "a filesystem root",
            false,
            |_| PathBuf::from(std::path::MAIN_SEPARATOR_STR),
            Some("MOUNT_PARENT_REQUIRED"),
        ),
        case(
            "a path ending in ..",
            false,
            |l| l.config.default_mount_directory.join("z").join(".."),
            // Windows resolves `..` lexically (the mount directory exists);
            // Unix keeps it, so the path names no folder.
            Some(if cfg!(windows) {
                "MOUNT_PATH_MUST_NOT_EXIST"
            } else {
                "FOLDER_MOUNT_REQUIRED"
            }),
        ),
        case(
            "a .. component inside the path",
            false,
            |l| {
                l.config
                    .default_mount_directory
                    .join("w")
                    .join("..")
                    .join("dotted")
            },
            None,
        ),
        case(
            "an empty path",
            false,
            |_| PathBuf::new(),
            Some("UNKNOWN_ERROR"),
        ),
    ];
    assert!(cases.len() >= 12);
    for (name, own_mount, requested, expected) in cases {
        let layout = layout(own_mount);
        let requested = requested(&layout);
        let ours = validate_mount_path(
            &layout.snapshot,
            &layout.config.data_directory,
            &layout.target.directory,
            &requested,
        );
        if let Ok(path) = &ours {
            assert!(path.parent().unwrap().is_dir(), "{name}: parent created");
            assert!(!path.exists(), "{name}");
            assert!(path.is_absolute(), "{name}");
        }
        let ours = ours.map_err(|error| {
            assert_eq!(
                error.class,
                tkfs::model::errors::classify(&error.code),
                "{name}"
            );
            error.code
        });
        let legacy = legacy_mount_path(&layout, &requested);
        assert_eq!(ours, legacy, "{name}: {requested:?}");
        match expected {
            None => assert!(ours.is_ok(), "{name}: {ours:?}"),
            Some(code) => assert_eq!(ours.as_ref().unwrap_err(), code, "{name}"),
        }
    }
}

#[test]
fn validate_mount_path_keeps_mounts_out_of_an_external_state_directory() {
    let _faults = shared();
    let env = env();
    let config = &env.config;
    fs::create_dir_all(&config.data_directory).unwrap();
    let snap = CatalogSnapshot::default();
    // A state directory outside the data directory (not a legacy layout).
    let state = env.root.path().join("elsewhere").join("state");
    fs::create_dir_all(&state).unwrap();
    for requested in [state.join("mnt"), env.root.path().join("elsewhere")] {
        let error =
            validate_mount_path(&snap, &config.data_directory, &state, &requested).unwrap_err();
        assert_eq!(error.code, "MOUNT_OVERLAPS_STATE", "{requested:?}");
        assert_eq!(error.class, ErrorClass::NeedsUserAction);
    }
    // A state directory that does not exist yet (Create validates before
    // `Store::initialize`) is recognised through its nearest existing parent.
    let pending = env.root.path().join("elsewhere").join("pending");
    let state = pending.join("state");
    let error = validate_mount_path(&snap, &config.data_directory, &state, &pending).unwrap_err();
    assert_eq!(error.code, "MOUNT_OVERLAPS_STATE");
    assert!(!state.exists());
    // An unrelated path next to it is accepted.
    let ok = validate_mount_path(
        &snap,
        &config.data_directory,
        &state,
        &env.root.path().join("elsewhere").join("mnt"),
    )
    .unwrap();
    assert!(ok.is_absolute() && !ok.exists());
}

// ---------------------------------------------------------------------------
// Waiters

#[test]
fn catalog_wait_newer_wakes_within_10ms_of_the_change() {
    // Exclusive: no other catalog test (some spin on `snapshot()`) competes
    // for the CPU while wake-up latency is measured.
    let _quiet = exclusive();
    let env = env();
    let catalog = Catalog::open(&env.config).unwrap();
    let record = folder(&env.config, "Waits");
    catalog.apply(create(&record, None)).unwrap();
    let mut round = 0u32;
    assert_fast("Catalog::wait_newer", Duration::from_millis(10), || {
        round += 1;
        let generation = catalog.snapshot().generation;
        let (tx, rx) = mpsc::channel();
        let waiter = {
            let catalog = catalog.clone();
            thread::spawn(move || {
                let snap = catalog.wait_newer(generation, Duration::from_secs(5));
                tx.send((Instant::now(), snap.generation)).unwrap();
            })
        };
        thread::sleep(Duration::from_millis(20));
        catalog
            .apply(set_desired(&record.state, round.is_multiple_of(2)))
            .unwrap();
        let returned = Instant::now();
        let (woke, seen) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        waiter.join().unwrap();
        assert_eq!(seen, generation + 1);
        // Published before the reply: the waiter is never later than the caller.
        woke.saturating_duration_since(returned)
    });
    let generation = catalog.snapshot().generation;
    let started = Instant::now();
    let snap = catalog.wait_newer(generation, Duration::from_millis(20));
    assert_eq!(snap.generation, generation);
    assert!(started.elapsed() >= Duration::from_millis(20));
    assert_eq!(
        catalog.wait_newer(0, Duration::from_secs(5)).generation,
        generation
    );
}

// ---------------------------------------------------------------------------
// Published catalog

fn entry(repo: &str, label: &str) -> PublishedEntry {
    PublishedEntry {
        repo: repo.into(),
        replica: format!("replica-{repo}"),
        label: label.into(),
        state: EntryState::Running,
        tip: Some(FeedTip {
            epoch: "e".into(),
            seq: 1,
        }),
        sync_generation: 1,
        updated_at_ms: 7,
        ..PublishedEntry::default()
    }
}

#[test]
fn published_list_is_cached_and_counts_changes() {
    let published = PublishedCatalog::with_base(100);
    let api: Arc<dyn PublishedCatalogApi> = published.clone();
    let (generation, list) = api.list();
    assert_eq!((generation, list.len()), (100, 0));

    api.update("s2", Some(entry("r2", "Two")));
    api.update("s1", Some(entry("r1", "One")));
    let (generation, list) = api.list();
    assert_eq!(generation, 102);
    let repos: Vec<_> = list.iter().map(|e| e.repo.as_str()).collect();
    assert_eq!(repos, ["r1", "r2"], "ordered by folder state");
    // O(1): the same cached list until something changes.
    let (_, again) = api.list();
    assert!(Arc::ptr_eq(&list, &again));
    api.update("s1", Some(entry("r1", "One")));
    let (generation, again) = api.list();
    assert_eq!(generation, 102, "an identical entry is not a change");
    assert!(Arc::ptr_eq(&list, &again));
    api.update("missing", None);
    assert_eq!(
        api.list().0,
        102,
        "removing a missing entry is not a change"
    );

    let mut paused = entry("r1", "One");
    paused.state = EntryState::Paused;
    api.update("s1", Some(paused.clone()));
    assert_eq!(api.list().0, 103);
    assert_eq!(published.entry("s1"), Some(paused.clone()));
    api.update("s2", None);
    let (generation, list) = api.list();
    assert_eq!(generation, 104);
    assert_eq!(list.as_slice(), &[paused]);
    assert_eq!(published.generation(), 104);
    assert_eq!(published.entry("s2"), None);
    assert!(format!("{published:?}").contains("generation: 104"));

    // wait_newer: immediate when already newer, the current one on timeout.
    assert_eq!(api.wait_newer(103, Duration::from_secs(5)), 104);
    assert_eq!(api.wait_newer(0, Duration::from_secs(5)), 104);
    let started = Instant::now();
    assert_eq!(api.wait_newer(104, Duration::from_millis(20)), 104);
    assert!(started.elapsed() >= Duration::from_millis(20));
    assert_eq!(api.wait_newer(200, Duration::from_millis(1)), 104);
}

#[test]
fn published_generation_starts_at_the_clock_and_never_repeats_across_restarts() {
    let before = now_ms();
    let first = PublishedCatalog::new();
    assert!(first.generation() >= before);
    for i in 0..3 {
        first.update("s", Some(entry(&format!("r{i}"), "Changing")));
    }
    let last = first.generation();
    assert_eq!(last, first.list().0);
    // A restarted daemon (at least one millisecond per change later) starts
    // above every generation the previous process served.
    thread::sleep(Duration::from_millis(10));
    let restarted = PublishedCatalog::new();
    assert!(restarted.generation() > last);
}

#[test]
fn published_wait_newer_wakes_within_10ms() {
    let _quiet = exclusive();
    assert_fast(
        "PublishedCatalog::wait_newer",
        Duration::from_millis(10),
        || {
            let published = PublishedCatalog::with_base(0);
            let generation = published.generation();
            let (tx, rx) = mpsc::channel();
            let waiter = {
                let published = published.clone();
                thread::spawn(move || {
                    let seen = published.wait_newer(generation, Duration::from_secs(5));
                    tx.send((Instant::now(), seen)).unwrap();
                })
            };
            thread::sleep(Duration::from_millis(20));
            let at = Instant::now();
            published.update("s", Some(entry("r", "Wake")));
            let (woke, seen) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
            waiter.join().unwrap();
            assert_eq!(seen, generation + 1);
            woke.saturating_duration_since(at)
        },
    );
}

#[test]
fn published_updates_from_many_threads_count_every_change() {
    let published = PublishedCatalog::with_base(1_000);
    let writers: Vec<_> = (0..8)
        .map(|w| {
            let published = published.clone();
            thread::spawn(move || {
                for i in 0..50 {
                    let mut e = entry(&format!("repo-{w}"), "Busy");
                    e.sync_generation = i + 1;
                    published.update(&format!("state-{w}"), Some(e));
                    let (_, list) = published.list();
                    assert!(list.windows(2).all(|p| p[0].repo < p[1].repo));
                }
            })
        })
        .collect();
    for writer in writers {
        writer.join().unwrap();
    }
    let (generation, list) = published.list();
    assert_eq!(generation, 1_000 + 8 * 50);
    assert_eq!(list.len(), 8);
    assert!(list.iter().all(|e| e.sync_generation == 50));
}
