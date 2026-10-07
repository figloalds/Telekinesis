//! Portable bounded O1: single-owner local supervisor. No machine sharing or adoption.
use crate::{
    core::{Store, hash, id},
    local_ipc,
    runtime::{self, Engine},
};
use anyhow::{Context, Result, bail, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    format_version: u32,
    pub installation_id: Option<String>,
    pub default_mount_directory: Option<PathBuf>,
    pub data_directory: PathBuf,
    pub control: Control,
    #[serde(default)]
    network: Network,
    #[serde(default)]
    workers: Workers,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Control {
    transport: String,
    pub name: String,
}
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Network {
    #[serde(default)]
    enabled: bool,
    listen: Option<String>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Workers {
    restart_policy: String,
    maximum_running: usize,
}
impl Default for Workers {
    fn default() -> Self {
        Self {
            restart_policy: "on-failure".into(),
            maximum_running: 8,
        }
    }
}
impl Config {
    pub fn control_endpoint(&self) -> String {
        #[cfg(windows)]
        {
            self.control.name.clone()
        }
        #[cfg(target_os = "linux")]
        {
            local_ipc::endpoint(&format!(
                "management:{}:{}",
                fs::canonicalize(&self.data_directory)
                    .unwrap_or_else(|_| self.data_directory.clone())
                    .display(),
                self.control.name
            ))
        }
    }
    pub fn load(path: &Path) -> Result<Self> {
        let path = fs::canonicalize(path).with_context(|| {
            format!("ORCHESTRATOR_CONFIG_NOT_FOUND: {}; run in the directory containing orchestrator.toml or select --defaults-file / -f", path.display())
        })?;
        let mut config: Self = toml::from_str(&fs::read_to_string(&path)?)
            .with_context(|| format!("INVALID_ORCHESTRATOR_CONFIG: {}", path.display()))?;
        ensure!(config.format_version == 1, "UNSUPPORTED_CONFIG_VERSION");
        if let Some(identity) = &config.installation_id {
            uuid::Uuid::parse_str(identity).context("INVALID_INSTALLATION_ID")?;
        }
        if let Some(directory) = &mut config.default_mount_directory {
            if directory.is_relative() {
                *directory = path.parent().unwrap().join(&*directory);
            }
            *directory = std::path::absolute(&*directory)?;
        }
        ensure!(
            config.control.transport
                == if cfg!(windows) {
                    "named-pipe"
                } else {
                    "unix-socket"
                },
            "UNSUPPORTED_CONTROL_TRANSPORT"
        );
        ensure!(
            !config.control.name.is_empty()
                && config.control.name.len() <= 100
                && config
                    .control
                    .name
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'),
            "INVALID_PIPE_NAME"
        );
        ensure!(!config.network.enabled, "O1_NETWORK_DISABLED");
        if let Some(address) = &config.network.listen {
            ensure!(
                address.parse::<std::net::SocketAddr>().is_ok(),
                "INVALID_LISTEN_ADDRESS"
            );
        }
        ensure!(
            config.workers.restart_policy == "on-failure"
                && (1..=64).contains(&config.workers.maximum_running),
            "INVALID_WORKER_POLICY"
        );
        ensure!(
            !config.data_directory.as_os_str().is_empty(),
            "INVALID_DATA_DIRECTORY"
        );
        if config.data_directory.is_relative() {
            config.data_directory = path.parent().unwrap().join(&config.data_directory);
        }
        config.data_directory = std::path::absolute(&config.data_directory)?;
        Ok(config)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Action {
    Hello,
    List,
    Inspect {
        state: String,
    },
    Operation {
        operation: String,
    },
    Create {
        label: String,
        mount: Option<PathBuf>,
    },
    Start {
        state: String,
    },
    Stop {
        state: String,
    },
    Shutdown,
}
impl Action {
    pub fn mutates(&self) -> bool {
        matches!(
            self,
            Self::Create { .. } | Self::Start { .. } | Self::Stop { .. } | Self::Shutdown
        )
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_installation: Option<String>,
    pub operation_id: Option<String>,
    pub expected_generation: Option<u64>,
    pub action: Action,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Marker {
    version: u32,
    state: String,
    repo: String,
    device: String,
    operation: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerRecord {
    version: u32,
    marker: Marker,
    root: PathBuf,
    mount: Option<PathBuf>,
    instance: String,
    pipe: String,
    token: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerRequest {
    version: u32,
    instance: String,
    token: String,
    op: String,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BootstrapMarker {
    version: u32,
    installation_id: String,
    owner: String,
    root: PathBuf,
}
#[derive(Clone)]
struct State {
    marker: Marker,
    label: String,
    mount: Option<PathBuf>,
    desired: bool,
    ready: bool,
    generation: u64,
}
struct Observation {
    value: Value,
    next: Instant,
    failures: u32,
}
pub struct Supervisor {
    config: Config,
    db: Connection,
    _lock: File,
    caller: String,
    observations: BTreeMap<String, Observation>,
    children: BTreeMap<String, Child>,
    exit_requested: bool,
    instance: String,
}
pub(crate) fn lock(path: &Path) -> Result<File> {
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .share_mode(0)
            .open(path)
            .context("ALREADY_OWNED")
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::{
            fd::AsRawFd,
            unix::fs::{MetadataExt, OpenOptionsExt},
        };
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        let metadata = file.metadata()?;
        ensure!(
            metadata.is_file()
                && metadata.uid() == unsafe { libc::geteuid() }
                && metadata.nlink() == 1
                && metadata.mode() & 0o077 == 0,
            "UNSAFE_OWNERSHIP_LOCK"
        );
        ensure!(
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
            "ALREADY_OWNED"
        );
        let current = fs::symlink_metadata(path)?;
        ensure!(
            current.ino() == metadata.ino() && current.dev() == metadata.dev(),
            "OWNERSHIP_LOCK_REPLACED"
        );
        Ok(file)
    }
}
/// Standalone daemons and managed workers share this exact ownership inode.
pub fn lock_state(state: &Path) -> Result<File> {
    #[cfg(target_os = "linux")]
    crate::local_ipc::protect_directory(state)?;
    lock(&state.join("owner.lock")).context("STATE_ALREADY_OWNED_OR_UNSAFE")
}
fn no_reparse(path: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        ensure!(
            fs::symlink_metadata(path)?.file_attributes() & 0x400 == 0,
            "REPARSE_POINT_REFUSED: {}",
            path.display()
        );
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = fs::symlink_metadata(path)?;
        ensure!(
            !metadata.file_type().is_symlink()
                && metadata.uid() == unsafe { libc::geteuid() }
                && metadata.mode() & 0o022 == 0,
            "UNSAFE_NATIVE_PATH: {}",
            path.display()
        );
    }
    Ok(())
}
pub(crate) fn atomic_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let temp = path.with_extension(format!("{}.tmp", id()));
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temp)?;
    file.write_all(&serde_json::to_vec(value)?)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temp, path)?;
    #[cfg(target_os = "linux")]
    File::open(path.parent().context("PARENT_REQUIRED")?)?.sync_all()?;
    Ok(())
}
fn load_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    no_reparse(path)?;
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
fn ok(value: Value) -> Value {
    json!({"ok":true,"result":value})
}
fn error(message: impl ToString, retryable: bool, operation: Option<&str>) -> Value {
    json!({"ok":false,"error":message.to_string(),"retryable":retryable,"operation_id":operation})
}
fn retryable(error: &anyhow::Error) -> bool {
    // Preserve typed OS errors: sharing/lock violations are availability
    // failures, unlike invalid identities/markers or unsupported formats.
    if error.chain().any(|cause| {
        cause.downcast_ref::<std::io::Error>().is_some_and(|io| {
            #[cfg(windows)]
            {
                matches!(io.raw_os_error(), Some(32 | 33 | 170))
            }
            #[cfg(target_os = "linux")]
            {
                matches!(
                    io.raw_os_error(),
                    Some(libc::EBUSY | libc::EAGAIN | libc::EINTR)
                )
            }
        })
    }) {
        return true;
    }
    let message = format!("{error:#}");
    [
        "BUSY_VIEW",
        "UNAVAILABLE",
        "ALREADY_OWNED",
        "LOCAL_IPC",
        "WORKER_LIMIT",
        "WORKER_START",
        "WORKER_EXIT",
        "SHUTDOWN_PENDING",
    ]
    .iter()
    .any(|code| message.contains(code))
}

/// Some(marker) permits only initial creation/recovery; None is a strictly
/// recognized existing catalog. An arbitrary version-zero file is never fresh.
fn catalog_preflight(
    root: &Path,
    config: &Config,
    caller: &str,
) -> Result<Option<BootstrapMarker>> {
    let database = root.join("orchestrator.sqlite");
    let marker_path = root.join(".orchestrator-bootstrap.json");
    let marker: Option<BootstrapMarker> = if marker_path.try_exists()? {
        let marker: BootstrapMarker =
            load_json(&marker_path).context("INVALID_BOOTSTRAP_MARKER")?;
        ensure!(
            marker.version == 1 && marker.owner == caller && marker.root == fs::canonicalize(root)?,
            "BOOTSTRAP_IDENTITY_MISMATCH"
        );
        uuid::Uuid::parse_str(&marker.installation_id).context("INVALID_BOOTSTRAP_IDENTITY")?;
        if let Some(expected) = &config.installation_id {
            ensure!(
                expected == &marker.installation_id,
                "INSTALLATION_ID_MISMATCH"
            );
        }
        Some(marker)
    } else {
        None
    };
    let entries = fs::read_dir(root)?
        .map(|entry| Ok(entry?.file_name()))
        .collect::<Result<Vec<_>>>()?;
    let bootstrap_layout = || {
        entries.iter().all(|entry| {
            [
                "orchestrator.lock",
                ".orchestrator-bootstrap.json",
                "orchestrator.sqlite",
                "orchestrator.sqlite-wal",
                "orchestrator.sqlite-shm",
                "orchestrator.sqlite-journal",
            ]
            .iter()
            .any(|name| entry == name)
        })
    };
    if database.try_exists()? {
        no_reparse(&database)?;
        let db = Connection::open_with_flags(&database, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .context("INVALID_REGISTRY")?;
        let version: i64 = db
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .context("INVALID_REGISTRY")?;
        ensure!((0..=1).contains(&version), "UNSUPPORTED_REGISTRY_VERSION");
        if version == 0 {
            ensure!(
                marker.is_some() && bootstrap_layout(),
                "REGISTRY_UNINITIALIZED: existing empty/unrecognized catalog has no valid fresh-bootstrap proof; restore the original catalog"
            );
            let tables: i64 = db
                .query_row("SELECT count(*) FROM sqlite_schema", [], |r| r.get(0))
                .context("INVALID_REGISTRY")?;
            ensure!(
                tables == 0,
                "INVALID_BOOTSTRAP_SCHEMA: version-zero catalog contains existing schema"
            );
            return Ok(marker);
        }
        let count: i64 = db
            .query_row("SELECT count(*) FROM installation", [], |r| r.get(0))
            .context("INVALID_REGISTRY_SCHEMA")?;
        ensure!(count == 1, "INVALID_REGISTRY_IDENTITY");
        let (identity, owner, generation): (String, String, i64) = db
            .query_row("SELECT id,owner,generation FROM installation", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .context("INVALID_REGISTRY_SCHEMA")?;
        uuid::Uuid::parse_str(&identity).context("INVALID_REGISTRY_IDENTITY")?;
        ensure!(generation >= 0, "INVALID_REGISTRY_GENERATION");
        ensure!(owner == caller, "REGISTRY_OWNER_MISMATCH");
        if let Some(expected) = &config.installation_id {
            ensure!(expected == &identity, "INSTALLATION_ID_MISMATCH");
        }
        if let Some(marker) = &marker {
            ensure!(
                marker.installation_id == identity,
                "BOOTSTRAP_IDENTITY_MISMATCH"
            );
        }
        db.prepare("SELECT id,repo,device,operation,label,mount,desired,ready,generation FROM states LIMIT 0").context("INVALID_REGISTRY_SCHEMA")?;
        db.prepare("SELECT caller,id,payload_hash,request,status,response FROM operations LIMIT 0")
            .context("INVALID_REGISTRY_SCHEMA")?;
        return Ok(None);
    }
    if let Some(marker) = marker {
        ensure!(
            bootstrap_layout(),
            "INVALID_BOOTSTRAP_LAYOUT: state directories exist without their catalog"
        );
        return Ok(Some(marker));
    }
    ensure!(
        entries.iter().all(|entry| entry == "orchestrator.lock"),
        "DATA_DIRECTORY_NOT_EMPTY: missing catalog; existing data will not be recreated"
    );
    Ok(Some(BootstrapMarker {
        version: 1,
        installation_id: config.installation_id.clone().unwrap_or_else(id),
        owner: caller.into(),
        root: fs::canonicalize(root)?,
    }))
}

impl Supervisor {
    pub fn validate_catalog(config: &Config) -> Result<()> {
        if config.data_directory.exists() {
            no_reparse(&config.data_directory)?;
            catalog_preflight(&config.data_directory, config, &local_ipc::owner_sid()?)?;
        }
        Ok(())
    }
    pub fn open(mut config: Config) -> Result<Self> {
        let caller = local_ipc::owner_sid()?;
        let root = &config.data_directory;
        if root.exists() {
            no_reparse(root)?;
            // Validate read-only before changing ACLs or opening a writable catalog.
            catalog_preflight(root, &config, &caller)?;
        } else {
            #[cfg(windows)]
            fs::create_dir_all(root)?;
            #[cfg(target_os = "linux")]
            crate::private_storage::Directory::open(root)?;
        }
        local_ipc::protect_directory(root)?;
        config.data_directory = fs::canonicalize(root)?;
        let root = &config.data_directory;
        let guard = lock(&root.join("orchestrator.lock"))?;
        // Repeat after locking; a concurrent first launcher may have initialized it.
        let bootstrap = catalog_preflight(root, &config, &caller)?;
        let marker_path = root.join(".orchestrator-bootstrap.json");
        if let Some(marker) = &bootstrap {
            if !marker_path.try_exists()? {
                atomic_json(&marker_path, marker)?;
            }
            crate::core::fault("orchestrator_bootstrap_marked");
        }
        let mut db = if bootstrap.is_some() {
            Connection::open(root.join("orchestrator.sqlite"))?
        } else {
            Connection::open_with_flags(
                root.join("orchestrator.sqlite"),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            )?
        };
        if bootstrap.is_some() {
            crate::core::fault("orchestrator_catalog_opened");
        }
        db.busy_timeout(Duration::from_secs(5))?;
        db.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;",
        )?;
        let version: i64 = db.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        ensure!(version <= 1, "UNSUPPORTED_REGISTRY_VERSION");
        if version == 0 {
            let marker = bootstrap
                .as_ref()
                .context("REGISTRY_UNINITIALIZED: no proven fresh bootstrap")?;
            let tx = db.transaction()?;
            tx.execute_batch("CREATE TABLE installation(id TEXT PRIMARY KEY,owner TEXT NOT NULL,generation INTEGER NOT NULL);
                CREATE TABLE states(id TEXT PRIMARY KEY,repo TEXT NOT NULL,device TEXT NOT NULL,operation TEXT NOT NULL,label TEXT NOT NULL,mount TEXT,desired INTEGER NOT NULL,ready INTEGER NOT NULL,generation INTEGER NOT NULL);
                CREATE TABLE operations(caller TEXT NOT NULL,id TEXT NOT NULL,payload_hash TEXT NOT NULL,request TEXT NOT NULL,status TEXT NOT NULL,response TEXT,PRIMARY KEY(caller,id)); PRAGMA user_version=1;")?;
            tx.execute(
                "INSERT INTO installation VALUES(?,?,0)",
                params![marker.installation_id, caller],
            )?;
            tx.commit()?;
            crate::core::fault("orchestrator_catalog_committed");
        }
        let owner: String = db.query_row("SELECT owner FROM installation", [], |r| r.get(0))?;
        ensure!(caller == owner, "REGISTRY_OWNER_MISMATCH");
        // A valid catalog replaces the transient bootstrap proof BEFORE state
        // directories or workers can exist. A stale marker never permits reset.
        if marker_path.try_exists()? {
            fs::remove_file(&marker_path)?;
        }
        // Commit bootstrap metadata before other directories so an interrupted
        // first startup remains distinguishable from unrelated user contents.
        for name in ["states", "staging", "logs"] {
            let path = root.join(name);
            fs::create_dir_all(&path)?;
            no_reparse(&path)?;
        }
        Ok(Self {
            config,
            db,
            _lock: guard,
            caller,
            observations: BTreeMap::new(),
            children: BTreeMap::new(),
            exit_requested: false,
            instance: id(),
        })
    }
    fn state(&self, state: &str) -> Result<State> {
        self.db.query_row("SELECT id,repo,device,operation,label,mount,desired,ready,generation FROM states WHERE id=?",[state],|r| {
            let mount:Option<String>=r.get(5)?;
            Ok(State { marker:Marker{version:1,state:r.get(0)?,repo:r.get(1)?,device:r.get(2)?,operation:r.get(3)?},label:r.get(4)?,mount:mount.map(PathBuf::from),desired:r.get(6)?,ready:r.get(7)?,generation:r.get(8)? })
        }).context("STATE_NOT_FOUND")
    }
    fn states(&self) -> Result<Vec<State>> {
        let ids = self
            .db
            .prepare("SELECT id FROM states ORDER BY id")?
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        ids.iter().map(|state| self.state(state)).collect()
    }
    pub fn generation(&self, action: &Action) -> Result<u64> {
        match action {
            Action::Start { state } | Action::Stop { state } => Ok(self.state(state)?.generation),
            _ => Ok(self
                .db
                .query_row("SELECT generation FROM installation", [], |r| r.get(0))?),
        }
    }
    fn root(&self, state: &State) -> PathBuf {
        self.config
            .data_directory
            .join("states")
            .join(&state.marker.state)
    }
    fn verify(&self, state: &State) -> Result<()> {
        let root = self.root(state);
        no_reparse(&root).context("STATE_UNAVAILABLE")?;
        ensure!(
            load_json::<Marker>(&root.join("state.json"))? == state.marker,
            "STATE_MARKER_MISMATCH"
        );
        no_reparse(&root.join("metadata.sqlite"))?;
        no_reparse(&root.join("objects"))?;
        let db = Connection::open_with_flags(
            root.join("metadata.sqlite"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        for (key, expected) in [
            ("repo", &state.marker.repo),
            ("device", &state.marker.device),
        ] {
            let actual: String =
                db.query_row("SELECT value FROM config WHERE key=?", [key], |r| r.get(0))?;
            ensure!(&actual == expected, "STORE_IDENTITY_MISMATCH");
        }
        Ok(())
    }
    fn validate_mount(&self, mount: &Path) -> Result<PathBuf> {
        ensure!(mount.is_absolute(), "MOUNT_MUST_BE_ABSOLUTE");
        ensure!(!mount.exists(), "MOUNT_PATH_OCCUPIED");
        let parent = fs::canonicalize(mount.parent().context("MOUNT_REQUIRES_PARENT")?)?;
        let target = parent.join(mount.file_name().context("FOLDER_MOUNT_REQUIRED")?);
        let normalized = |p: &Path| {
            let value = p.to_string_lossy();
            if cfg!(windows) {
                value
                    .trim_end_matches(['\\', '/'])
                    .replace('/', "\\")
                    .to_lowercase()
            } else {
                value.trim_end_matches('/').to_owned()
            }
        };
        let target_key = normalized(&target);
        let data_key = normalized(&self.config.data_directory);
        let overlap = |a: &str, b: &str| {
            let separator = if cfg!(windows) { "\\" } else { "/" };
            a == b
                || a.starts_with(&format!("{b}{separator}"))
                || b.starts_with(&format!("{a}{separator}"))
        };
        ensure!(!overlap(&target_key, &data_key), "MOUNT_DATA_OVERLAP");
        for state in self.states()? {
            if let Some(other) = state.mount {
                ensure!(
                    !overlap(&target_key, &normalized(&other)),
                    "MOUNT_RESERVATION_OVERLAP"
                );
            }
        }
        Ok(target)
    }
    fn observed(&self, state: &State) -> Value {
        let observation = self
            .observations
            .get(&state.marker.state)
            .map(|o| o.value.clone())
            .unwrap_or(json!({"status":"pending"}));
        json!({"state_id":state.marker.state,"repo_id":state.marker.repo,"device_id":state.marker.device,"label":state.label,"mount":state.mount,"desired_running":state.desired,"management_generation":state.generation,"observation":observation})
    }
    fn create_native(&mut self, state: &State) -> Result<()> {
        if state.ready {
            return self.verify(state);
        }
        let staging = self
            .config
            .data_directory
            .join("staging")
            .join(&state.marker.state);
        let root = self.root(state);
        if root.exists() {
            self.verify(state)?;
        } else {
            if staging.exists() {
                no_reparse(&staging)?;
                if !staging.join("state.json").exists() {
                    // A crash between mkdir and marker installation leaves an
                    // operation-owned empty directory (or verified marker temp).
                    for entry in fs::read_dir(&staging)? {
                        let entry = entry?;
                        ensure!(
                            entry.file_name().to_string_lossy().ends_with(".tmp")
                                && load_json::<Marker>(&entry.path())? == state.marker,
                            "STAGING_MARKER_MISSING"
                        );
                    }
                    atomic_json(&staging.join("state.json"), &state.marker)?;
                }
                ensure!(
                    load_json::<Marker>(&staging.join("state.json"))? == state.marker,
                    "STAGING_MARKER_MISMATCH"
                );
            } else {
                #[cfg(windows)]
                fs::create_dir(&staging)?;
                #[cfg(target_os = "linux")]
                crate::private_storage::Directory::open(&staging)?;
                crate::core::fault("orchestrator_staging_created");
                atomic_json(&staging.join("state.json"), &state.marker)?;
            }
            no_reparse(&staging)?;
            let guard = lock(&staging.join("owner.lock"))?;
            let store = Store::initialize(&staging, &state.marker.repo, &state.marker.device)?;
            ensure!(
                store.repo == state.marker.repo && store.device == state.marker.device,
                "STORE_IDENTITY_MISMATCH"
            );
            store.db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
            drop(store);
            drop(guard);
            crate::core::fault("orchestrator_store_initialized");
            fs::rename(&staging, &root)?;
            #[cfg(target_os = "linux")]
            {
                File::open(staging.parent().unwrap())?.sync_all()?;
                File::open(root.parent().unwrap())?.sync_all()?;
            }
            crate::core::fault("orchestrator_store_installed");
            self.verify(state)?;
        }
        self.db.execute(
            "UPDATE states SET ready=1 WHERE id=?",
            [&state.marker.state],
        )?;
        Ok(())
    }
    fn record(&self, state: &State) -> Result<WorkerRecord> {
        let record: WorkerRecord = load_json(&self.root(state).join("worker.json"))?;
        ensure!(
            record.version == 1
                && record.marker == state.marker
                && record.root == self.root(state)
                && record.mount == state.mount,
            "WORKER_RECORD_MISMATCH"
        );
        uuid::Uuid::parse_str(&record.instance)?;
        Ok(record)
    }
    fn worker_call(&self, record: &WorkerRecord, op: &str) -> Result<Value> {
        let request = WorkerRequest {
            version: 1,
            instance: record.instance.clone(),
            token: record.token.clone(),
            op: op.into(),
        };
        let reply: Value = local_ipc::call_timeout(&record.pipe, &request, 3000)?;
        ensure!(
            reply["ok"] == true,
            "WORKER_UNAVAILABLE: {}",
            reply["error"]
        );
        let result = &reply["result"];
        ensure!(
            result["instance"] == record.instance
                && result["state_id"] == record.marker.state
                && result["repo_id"] == record.marker.repo
                && result["device_id"] == record.marker.device
                && result["mount"] == json!(record.mount),
            "WORKER_INSTANCE_MISMATCH"
        );
        Ok(result.clone())
    }
    fn reconcile_state(&mut self, state: &State) -> Result<Value> {
        if let Ok(record) = self.record(state)
            && let Ok(info) = self.worker_call(&record, "health")
        {
            if state.desired {
                self.verify(state).context("STATE_UNAVAILABLE")?;
                ensure!(
                    info["mounted"] == json!(state.mount.is_some()),
                    "WORKER_UNAVAILABLE: view not mounted"
                );
                if !info["health"].is_null() {
                    let mut degraded = info;
                    degraded["status"] = json!("degraded");
                    return Ok(degraded);
                }
                return Ok(info);
            }
            let pid = info["pid"].as_u64().context("INVALID_WORKER_PID")? as u32;
            let process = Process::open(pid)?;
            self.worker_call(&record, "stop")?;
            process.wait()?;
            if let Some(mut child) = self.children.remove(&state.marker.state) {
                let _ = child.wait();
            }
            return Ok(json!({"status":"stopped"}));
        }
        if !state.desired && !self.root(state).exists() {
            ensure!(
                !self.children.contains_key(&state.marker.state),
                "WORKER_UNAVAILABLE: missing live owner record"
            );
            #[cfg(target_os = "linux")]
            if let Some(mount) = &state.mount {
                ensure!(
                    fs::symlink_metadata(mount)
                        .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound),
                    "WORKER_UNAVAILABLE: missing state cannot authorize mount cleanup"
                );
            }
            return Ok(json!({"status":"stopped"}));
        }
        // Never use a stored PID for recovery. A free ownership lock is required
        // before creating a replacement, even when the old endpoint is unreachable.
        let guard = lock(&self.root(state).join("owner.lock"))
            .context("WORKER_UNAVAILABLE: unverified owner")?;
        if !state.desired {
            #[cfg(target_os = "linux")]
            if let Some(mount) = &state.mount {
                let absent = fs::symlink_metadata(mount)
                    .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound);
                if !absent {
                    self.verify(state)
                        .context("WORKER_UNAVAILABLE: cleanup identity")?;
                    let record = self
                        .record(state)
                        .context("WORKER_UNAVAILABLE: cleanup record")?;
                    // Keep exclusive ownership through normal unmount. A busy or
                    // unrelated mount leaves stop/shutdown pending, never success.
                    crate::mount::recover_managed(mount, &record.instance)?;
                }
            }
            return Ok(json!({"status":"stopped"}));
        }
        drop(guard);
        ensure!(state.ready, "STATE_UNAVAILABLE: initialization pending");
        self.verify(state).context("STATE_UNAVAILABLE")?;
        let running = self
            .states()?
            .iter()
            .filter(|s| {
                s.marker.state != state.marker.state
                    && s.ready
                    && self.root(s).is_dir()
                    // Count unverified live owners too. Losing an IPC handshake
                    // must not make their worker slot available for overcommit.
                    && lock(&self.root(s).join("owner.lock")).is_err()
            })
            .count();
        ensure!(
            running < self.config.workers.maximum_running,
            "WORKER_LIMIT"
        );
        if let Some(mut old) = self.children.remove(&state.marker.state) {
            ensure!(
                old.try_wait()?.is_some(),
                "WORKER_UNAVAILABLE: launch still alive"
            );
        }
        if let Some(mount) = &state.mount {
            #[cfg(target_os = "linux")]
            if let Ok(record) = self.record(state) {
                crate::mount::recover_managed(mount, &record.instance)?;
            }
            ensure!(!mount.exists(), "WORKER_START: mount occupied");
            let canonical = fs::canonicalize(mount.parent().context("MOUNT_REQUIRES_PARENT")?)?
                .join(mount.file_name().context("FOLDER_MOUNT_REQUIRED")?);
            ensure!(&canonical == mount, "WORKER_START: mount parent changed");
        }
        let record = WorkerRecord {
            version: 1,
            marker: state.marker.clone(),
            root: self.root(state),
            mount: state.mount.clone(),
            instance: id(),
            pipe: {
                #[cfg(windows)]
                {
                    format!("tkfs-worker-{}", id())
                }
                #[cfg(target_os = "linux")]
                {
                    local_ipc::endpoint(&format!("worker:{}:{}", self.root(state).display(), id()))
                }
            },
            token: format!("{}{}", id(), id()),
        };
        atomic_json(&self.root(state).join("worker.json"), &record)?;
        let logs = self.config.data_directory.join("logs");
        let out = File::create(logs.join(format!("{}-stdout.log", state.marker.state)))?;
        let err = File::create(logs.join(format!("{}-stderr.log", state.marker.state)))?;
        let mut command = Command::new(std::env::current_exe()?);
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        command
            .arg("managed-worker")
            .stdin(Stdio::piped())
            .stdout(out)
            .stderr(err);
        // Managed O1 must not inherit PoC peer configuration or fault injection.
        command.env_remove("TKFS_PEER_KEY").env_remove("TKFS_FAULT");
        let mut child = command.spawn().context("WORKER_START")?;
        let input = serde_json::to_vec(&record)?;
        let write_result = child
            .stdin
            .take()
            .context("WORKER_STDIN")
            .and_then(|mut stdin| Ok(stdin.write_all(&input)?));
        self.children.insert(state.marker.state.clone(), child);
        write_result?;
        crate::core::fault("orchestrator_worker_spawned");
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(info) = self.worker_call(&record, "health") {
                return Ok(info);
            }
            if self
                .children
                .get_mut(&state.marker.state)
                .unwrap()
                .try_wait()?
                .is_some()
            {
                bail!("WORKER_START: child exited; inspect bounded worker log");
            }
            ensure!(Instant::now() < deadline, "WORKER_START: readiness timeout");
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    fn reconcile_one(&mut self, state: &State, force: bool) -> Result<Value> {
        if !force
            && self
                .observations
                .get(&state.marker.state)
                .is_some_and(|o| Instant::now() < o.next)
        {
            return Ok(self.observations[&state.marker.state].value.clone());
        }
        let result = self.reconcile_state(state);
        let failures = if result.is_ok() {
            0
        } else {
            self.observations
                .get(&state.marker.state)
                .map_or(1, |o| o.failures.saturating_add(1))
        };
        let value = match &result {
            Ok(value) => value.clone(),
            Err(e) => {
                let message = format!("{e:#}");
                json!({"status":if message.contains("BUSY_VIEW") {"stop-pending"} else {"unavailable"},"error":message})
            }
        };
        let seconds = if failures == 0 {
            1
        } else {
            1u64 << failures.min(5)
        };
        self.observations.insert(
            state.marker.state.clone(),
            Observation {
                value: value.clone(),
                next: Instant::now() + Duration::from_secs(seconds),
                failures,
            },
        );
        result
    }
    fn complete(&mut self, operation: &str, response: &Value, terminal: bool) -> Result<()> {
        self.db.execute(
            "UPDATE operations SET status=?,response=? WHERE caller=? AND id=?",
            params![
                if terminal { "completed" } else { "pending" },
                serde_json::to_string(response)?,
                self.caller,
                operation
            ],
        )?;
        Ok(())
    }
    fn execute(&mut self, request: &Request) -> Result<Value> {
        let operation = request
            .operation_id
            .as_deref()
            .context("OPERATION_ID_REQUIRED")?;
        let result = (|| -> Result<Value> {
            if matches!(request.action, Action::Create { .. } | Action::Start { .. }) {
                ensure!(
                    !self.shutdown_pending()?,
                    "SHUTDOWN_PENDING: retry shutdown before resuming worker operations"
                );
            }
            match &request.action {
                Action::Create { .. } => {
                    let state_id: String = self.db.query_row(
                        "SELECT id FROM states WHERE operation=?",
                        [operation],
                        |r| r.get(0),
                    )?;
                    let state = self.state(&state_id)?;
                    self.create_native(&state)?;
                    let state = self.state(&state_id)?;
                    self.reconcile_one(&state, true)?;
                    Ok(self.observed(&state))
                }
                Action::Start { state } | Action::Stop { state } => {
                    let state = self.state(state)?;
                    self.reconcile_one(&state, true)?;
                    Ok(self.observed(&state))
                }
                Action::Shutdown => {
                    // Preflight ALL workers before fencing/stopping any. A racing
                    // new open can still make stop retryable; intent remains durable.
                    for state in self.states()? {
                        if let Ok(record) = self.record(&state)
                            && let Ok(info) = self.worker_call(&record, "health")
                        {
                            ensure!(info["open_handles"] == 0, "BUSY_VIEW");
                        }
                    }
                    for mut state in self.states()? {
                        state.desired = false;
                        self.reconcile_one(&state, true)?;
                    }
                    self.exit_requested = true;
                    Ok(json!({"status":"stopped"}))
                }
                _ => bail!("NOT_A_MUTATION"),
            }
        })();
        let response = match result {
            Ok(value) => ok(value),
            Err(e) => {
                let message = format!("{e:#}");
                error(&message, retryable(&e), Some(operation))
            }
        };
        self.complete(operation, &response, response["retryable"] != true)?;
        if response["retryable"] == true {
            let state_id = match &request.action {
                Action::Create { .. } => self
                    .db
                    .query_row(
                        "SELECT id FROM states WHERE operation=?",
                        [operation],
                        |r| r.get::<_, String>(0),
                    )
                    .optional()?,
                Action::Start { state } | Action::Stop { state } => Some(state.clone()),
                _ => None,
            };
            if let Some(state_id) = state_id {
                let observation = self.observations.entry(state_id).or_insert(Observation {
                    value: json!({"status":"unavailable","error":response["error"]}),
                    next: Instant::now(),
                    failures: 1,
                });
                observation.next =
                    Instant::now() + Duration::from_secs(1u64 << observation.failures.min(5));
            }
        }
        Ok(response)
    }
    pub fn handle(&mut self, request: Request) -> Result<Value> {
        ensure!(request.version == 1, "UNSUPPORTED_API_VERSION");
        let identity: String = self
            .db
            .query_row("SELECT id FROM installation", [], |r| r.get(0))?;
        if let Some(expected) = &request.target_installation {
            ensure!(expected == &identity, "INSTALLATION_ID_MISMATCH");
        }
        if !request.action.mutates() {
            return match request.action {
                Action::Hello => Ok(ok(json!({"api_version":1,"installation_id":identity,
                    "instance_id":self.instance,"data_directory":self.config.data_directory,
                    "build_version":env!("CARGO_PKG_VERSION"),"registry_version":1,
                    "capabilities":["local-management-v1","target-installation-v1","runtime-rpc-v1"],
                    "network_enabled":false,"shutdown_pending":self.shutdown_pending()?}))),
                Action::List => Ok(ok(
                    json!({"api_version":1,"catalog_generation":self.generation(&Action::List)?,"states":self.states()?.iter().map(|s|self.observed(s)).collect::<Vec<_>>()}),
                )),
                Action::Inspect { state } => Ok(ok(self.observed(&self.state(&state)?))),
                Action::Operation { operation } => {
                    let (status, response): (String, Option<String>) = self
                        .db
                        .query_row(
                            "SELECT status,response FROM operations WHERE caller=? AND id=?",
                            params![self.caller, operation],
                            |r| Ok((r.get(0)?, r.get(1)?)),
                        )
                        .context("OPERATION_NOT_FOUND")?;
                    Ok(ok(
                        json!({"operation_id":operation,"status":status,"response":response.map(|r|serde_json::from_str::<Value>(&r)).transpose()?}),
                    ))
                }
                _ => unreachable!(),
            };
        }
        let operation = request
            .operation_id
            .as_deref()
            .context("OPERATION_ID_REQUIRED")?;
        uuid::Uuid::parse_str(operation).context("INVALID_OPERATION_ID")?;
        let payload = serde_json::to_string(&request)?;
        let payload_hash = hash(payload.as_bytes());
        let receipt: Option<(String, String, Option<String>)> = self
            .db
            .query_row(
                "SELECT payload_hash,status,response FROM operations WHERE caller=? AND id=?",
                params![self.caller, operation],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        if let Some((old_hash, status, response)) = receipt {
            ensure!(old_hash == payload_hash, "OPERATION_PAYLOAD_MISMATCH");
            if status == "completed" {
                return Ok(serde_json::from_str(&response.context("INVALID_RECEIPT")?)?);
            }
            return self.execute(&request);
        }
        let prepared = (|| -> Result<Option<PathBuf>> {
            let pending = self
                .db
                .prepare("SELECT request FROM operations WHERE caller=? AND status='pending'")?
                .query_map([&self.caller], |r| r.get::<_, String>(0))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            for payload in pending {
                let pending: Request = serde_json::from_str(&payload)?;
                ensure!(
                    !matches!(pending.action, Action::Shutdown),
                    "SHUTDOWN_PENDING: retry the original operation"
                );
                if let Action::Start { state } | Action::Stop { state } = &request.action {
                    let pending_state = match &pending.action {
                        Action::Create { .. } => self
                            .db
                            .query_row(
                                "SELECT id FROM states WHERE operation=?",
                                [pending.operation_id.as_deref().unwrap()],
                                |r| r.get::<_, String>(0),
                            )
                            .optional()?,
                        Action::Start { state } | Action::Stop { state } => Some(state.clone()),
                        _ => None,
                    };
                    ensure!(
                        pending_state.as_deref() != Some(state),
                        "STATE_OPERATION_PENDING: retry the original operation"
                    );
                }
            }
            ensure!(
                request.expected_generation == Some(self.generation(&request.action)?),
                "STALE_MANAGEMENT_GENERATION"
            );
            if let Action::Create { label, mount } = &request.action {
                ensure!(
                    !label.trim().is_empty() && label.len() <= 256,
                    "INVALID_STATE_LABEL"
                );
                return mount.as_ref().map(|p| self.validate_mount(p)).transpose();
            }
            Ok(None)
        })();
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT INTO operations VALUES(?,?,?,?,'pending',NULL)",
            params![self.caller, operation, payload_hash, payload],
        )?;
        match prepared {
            Err(e) => {
                let mut response = error(format!("{e:#}"), false, Some(operation));
                // This receipt and the absence of a state intent are committed
                // in the same transaction. Clients may safely replace this
                // rejected convenience request, never a pending/executed create.
                response["intent_committed"] = json!(false);
                tx.execute(
                    "UPDATE operations SET status='completed',response=? WHERE caller=? AND id=?",
                    params![serde_json::to_string(&response)?, self.caller, operation],
                )?;
                tx.commit()?;
                return Ok(response);
            }
            Ok(mount) => match &request.action {
                Action::Create { label, .. } => {
                    tx.execute(
                        "INSERT INTO states VALUES(?,?,?,?,?,?,1,0,0)",
                        params![
                            id(),
                            id(),
                            id(),
                            operation,
                            label,
                            mount.map(|p| p.to_string_lossy().into_owned())
                        ],
                    )?;
                    tx.execute("UPDATE installation SET generation=generation+1", [])?;
                }
                Action::Start { state } | Action::Stop { state } => {
                    tx.execute(
                        "UPDATE states SET desired=?,generation=generation+1 WHERE id=?",
                        params![matches!(request.action, Action::Start { .. }), state],
                    )?;
                }
                Action::Shutdown => {
                    tx.execute("UPDATE installation SET generation=generation+1", [])?;
                }
                _ => unreachable!(),
            },
        }
        tx.commit()?;
        crate::core::fault("orchestrator_intent_committed");
        self.execute(&request)
    }
    fn shutdown_pending(&self) -> Result<bool> {
        Ok(self.db.query_row("SELECT EXISTS(SELECT 1 FROM operations WHERE caller=? AND status='pending' AND json_extract(request,'$.action.op')='shutdown')",[&self.caller],|r|r.get(0))?)
    }
    fn recover(&mut self) -> Result<()> {
        // This guard applies to the first startup recovery pass too, before
        // tick() and before any intent can install a store or launch a worker.
        if self.shutdown_pending()? {
            return Ok(());
        }
        let pending = self
            .db
            .prepare(
                "SELECT request FROM operations WHERE caller=? AND status='pending' ORDER BY rowid",
            )?
            .query_map([&self.caller], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for payload in pending {
            let request: Request = serde_json::from_str(&payload)?;
            // Shutdown intent is completed by the next explicit exact retry,
            // rather than repeatedly shutting down every fresh supervisor.
            let state_id = match &request.action {
                Action::Create { .. } => self
                    .db
                    .query_row(
                        "SELECT id FROM states WHERE operation=?",
                        [request.operation_id.as_deref().unwrap()],
                        |r| r.get::<_, String>(0),
                    )
                    .optional()?,
                Action::Start { state } | Action::Stop { state } => Some(state.clone()),
                _ => None,
            };
            let waiting = state_id.as_ref().is_some_and(|state| {
                self.observations
                    .get(state)
                    .is_some_and(|o| Instant::now() < o.next)
            });
            if !matches!(request.action, Action::Shutdown) && !waiting {
                let _ = self.execute(&request)?;
            }
        }
        Ok(())
    }
    fn tick(&mut self) -> Result<()> {
        if self.shutdown_pending()? {
            return Ok(());
        }
        self.recover()?;
        for state in self.states()? {
            if state.ready {
                let _ = self.reconcile_one(&state, false);
            }
        }
        Ok(())
    }
    pub fn run(mut self) -> Result<()> {
        let mut listener = local_ipc::Listener::bind(&self.config.control_endpoint())?;
        self.recover()?;
        self.tick()?;
        println!(
            "{}",
            json!({"ready":true,"api_version":1,"control":self.config.control.name,"network_enabled":false})
        );
        loop {
            match listener.receive::<Request>() {
                Ok(Some(request)) => {
                    let response = self
                        .handle(request)
                        .unwrap_or_else(|e| error(format!("{e:#}"), false, None));
                    let _ = listener.reply(&response);
                    // Replaying a completed shutdown from a previous instance
                    // returns its receipt; it must not stop this new supervisor.
                    if self.exit_requested && response["ok"] == true {
                        break;
                    }
                }
                Ok(None) => {
                    #[cfg(target_os = "linux")]
                    std::thread::sleep(Duration::from_millis(25));
                }
                Err(e) => {
                    let _ = listener.reply(&error(format!("{e:#}"), false, None));
                }
            }
            self.tick()?;
        }
        Ok(())
    }
}

#[cfg(windows)]
unsafe extern "system" {
    fn OpenProcess(access: u32, inherit: i32, pid: u32) -> *mut std::ffi::c_void;
    fn WaitForSingleObject(handle: *mut std::ffi::c_void, milliseconds: u32) -> u32;
}
struct Process(File);
impl Process {
    #[cfg(windows)]
    fn open(pid: u32) -> Result<Self> {
        use std::os::windows::io::FromRawHandle;
        let handle = unsafe { OpenProcess(0x00100000, 0, pid) };
        ensure!(
            !handle.is_null(),
            "WORKER_EXIT: {}",
            std::io::Error::last_os_error()
        );
        Ok(Self(unsafe { File::from_raw_handle(handle) }))
    }
    #[cfg(windows)]
    fn wait(&self) -> Result<()> {
        use std::os::windows::io::AsRawHandle;
        ensure!(
            unsafe { WaitForSingleObject(self.0.as_raw_handle(), 5000) } == 0,
            "WORKER_EXIT: process did not exit"
        );
        Ok(())
    }
    #[cfg(target_os = "linux")]
    fn open(pid: u32) -> Result<Self> {
        use std::os::fd::FromRawFd;
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0) } as i32;
        ensure!(fd >= 0, "WORKER_EXIT: {}", std::io::Error::last_os_error());
        Ok(Self(unsafe { File::from_raw_fd(fd) }))
    }
    #[cfg(target_os = "linux")]
    fn wait(&self) -> Result<()> {
        use std::os::fd::AsRawFd;
        let mut descriptor = libc::pollfd {
            fd: self.0.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        ensure!(
            unsafe { libc::poll(&mut descriptor, 1, 5000) } == 1
                && descriptor.revents & libc::POLLIN != 0,
            "WORKER_EXIT: process did not exit"
        );
        Ok(())
    }
}

pub fn worker() -> Result<()> {
    let mut input = String::new();
    std::io::stdin().take(16384).read_to_string(&mut input)?;
    let record: WorkerRecord = serde_json::from_str(&input)?;
    ensure!(
        record.version == 1 && record.marker.version == 1,
        "UNSUPPORTED_WORKER_VERSION"
    );
    uuid::Uuid::parse_str(&record.instance)?;
    no_reparse(&record.root)?;
    ensure!(
        load_json::<Marker>(&record.root.join("state.json"))? == record.marker,
        "STATE_MARKER_MISMATCH"
    );
    let _guard = lock_state(&record.root)?;
    let store = Store::open_existing(&record.root)?;
    ensure!(
        store.repo == record.marker.repo && store.device == record.marker.device,
        "STORE_IDENTITY_MISMATCH"
    );
    let engine = Arc::new(Mutex::new(Engine::new(store)));
    runtime::start_rpc(
        engine.clone(),
        &record.root,
        record
            .mount
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned()),
    )?;
    if let Some(mount) = &record.mount {
        #[cfg(windows)]
        crate::mount::start(engine.clone(), mount)?;
        #[cfg(target_os = "linux")]
        crate::mount::start_managed(engine.clone(), mount, &record.instance)?;
    }
    // Private lifecycle readiness is published AFTER mount installation.
    let mut listener = local_ipc::Listener::bind(&record.pipe)?;
    let observation = engine.lock().unwrap().observation.clone();
    loop {
        let request = match listener.receive::<WorkerRequest>() {
            Ok(Some(r)) => r,
            Ok(None) => {
                #[cfg(target_os = "linux")]
                std::thread::sleep(Duration::from_millis(25));
                continue;
            }
            Err(e) => {
                let _ = listener.reply(&error(format!("{e:#}"), false, None));
                continue;
            }
        };
        let result = (|| -> Result<Value> {
            ensure!(
                request.version == 1
                    && request.instance == record.instance
                    && request.token == record.token,
                "WORKER_AUTHORIZATION_DENIED"
            );
            match request.op.as_str() {
                "health" => {}
                "stop" => crate::mount::stop(&engine)?,
                _ => bail!("UNKNOWN_WORKER_OPERATION"),
            }
            let (sample, busy) = if let Ok(e) = engine.try_lock() {
                e.publish_observation();
                (observation.lock().unwrap().clone(), false)
            } else {
                (observation.lock().unwrap().clone(), true)
            };
            Ok(
                json!({"status":"running","instance":record.instance,"state_id":record.marker.state,"repo_id":record.marker.repo,"device_id":record.marker.device,"mount":record.mount,"pid":std::process::id(),"open_handles":sample["open_handles"],"unflushed_files":sample["unflushed_files"],"mounted":sample["mounted"],"health":sample["health"],"busy":busy,"stale":busy,"sampled_at_ms":sample["sampled_at_ms"]}),
            )
        })();
        let stopping = request.op == "stop" && result.is_ok();
        let reply = match result {
            Ok(value) => ok(value),
            Err(e) => error(format!("{e:#}"), true, None),
        };
        let _ = listener.reply(&reply);
        if stopping {
            break;
        }
    }
    Ok(())
}

pub fn verify_hello(config: &Config, expected: Option<&str>, hello: &Value) -> Result<()> {
    ensure!(
        hello["api_version"] == 1 && hello["registry_version"] == 1,
        "DESKTOP_API_VERSION_MISMATCH"
    );
    ensure!(
        hello["build_version"] == env!("CARGO_PKG_VERSION"),
        "DESKTOP_BUILD_VERSION_MISMATCH: close clients and perform an orderly upgrade"
    );
    let identity = hello["installation_id"]
        .as_str()
        .context("HELLO_IDENTITY_MISSING")?;
    uuid::Uuid::parse_str(identity)?;
    if let Some(expected) = expected.or(config.installation_id.as_deref()) {
        ensure!(
            identity == expected,
            "INSTALLATION_ID_MISMATCH: this pipe belongs to another installation"
        );
    }
    let root =
        fs::canonicalize(&config.data_directory).context("CONFIGURED_DATA_FOLDER_MISSING")?;
    let actual = PathBuf::from(
        hello["data_directory"]
            .as_str()
            .context("HELLO_DATA_DIRECTORY_MISSING")?,
    );
    ensure!(
        root == actual,
        "DATA_DIRECTORY_MISMATCH: this pipe belongs to another installation"
    );
    let capabilities = hello["capabilities"]
        .as_array()
        .context("HELLO_CAPABILITIES_MISSING")?;
    ensure!(
        [
            "local-management-v1",
            "target-installation-v1",
            "runtime-rpc-v1"
        ]
        .iter()
        .all(|cap| capabilities.contains(&json!(cap))),
        "DESKTOP_CAPABILITY_MISMATCH"
    );
    Ok(())
}
pub fn client(config: &Config, request: &Request) -> Result<Value> {
    let hello_request = Request {
        version: 1,
        target_installation: request
            .target_installation
            .clone()
            .or(config.installation_id.clone()),
        operation_id: None,
        expected_generation: None,
        action: Action::Hello,
    };
    let hello: Value = local_ipc::call(&config.control_endpoint(), &hello_request)?;
    ensure!(
        hello["ok"] == true,
        "CONNECTION_REJECTED: {}",
        hello["error"]
    );
    verify_hello(
        config,
        hello_request.target_installation.as_deref(),
        &hello["result"],
    )?;
    if matches!(request.action, Action::Hello) {
        return Ok(hello);
    }
    let mut request = request.clone();
    request.target_installation = Some(
        hello["result"]["installation_id"]
            .as_str()
            .context("HELLO_IDENTITY_MISSING")?
            .into(),
    );
    local_ipc::call(&config.control_endpoint(), &request)
}

#[cfg(test)]
mod regressions {
    use super::*;
    #[cfg(windows)]
    use std::os::windows::fs::OpenOptionsExt;

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_owner_lock_rejects_aliases_replacement_and_inheritance() {
        use std::os::fd::AsRawFd;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("owner.lock");
        let guard = lock(&path).unwrap();
        assert!(lock(&path).is_err());
        assert_ne!(
            unsafe { libc::fcntl(guard.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
        let alias = root.path().join("alias");
        std::os::unix::fs::symlink(&path, &alias).unwrap();
        assert!(lock(&alias).is_err());
        fs::remove_file(&alias).unwrap();
        fs::hard_link(&path, &alias).unwrap();
        assert!(lock(&alias).is_err());
        drop(guard);
        assert!(lock(&path).is_err());
    }

    fn fixture() -> (tempfile::TempDir, Supervisor) {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("defaults.toml");
        let transport = if cfg!(windows) {
            "named-pipe"
        } else {
            "unix-socket"
        };
        fs::write(&path,format!("format_version=1\ndata_directory='data'\n[control]\ntransport='{transport}'\nname='regression-{}'",id())).unwrap();
        let supervisor = Supervisor::open(Config::load(&path).unwrap()).unwrap();
        (temp, supervisor)
    }
    fn pending_create(supervisor: &mut Supervisor) -> (Request, State) {
        let request = Request {
            version: 1,
            target_installation: None,
            operation_id: Some(id()),
            expected_generation: Some(0),
            action: Action::Create {
                label: "recover".into(),
                mount: None,
            },
        };
        let state = State {
            marker: Marker {
                version: 1,
                state: id(),
                repo: id(),
                device: id(),
                operation: request.operation_id.clone().unwrap(),
            },
            label: "recover".into(),
            mount: None,
            desired: true,
            ready: false,
            generation: 0,
        };
        let payload = serde_json::to_string(&request).unwrap();
        let tx = supervisor.db.transaction().unwrap();
        tx.execute(
            "INSERT INTO operations VALUES(?,?,?,?,'pending',NULL)",
            params![
                supervisor.caller,
                request.operation_id,
                hash(payload.as_bytes()),
                payload
            ],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO states VALUES(?,?,?,?,?,NULL,1,0,0)",
            params![
                state.marker.state,
                state.marker.repo,
                state.marker.device,
                state.marker.operation,
                state.label
            ],
        )
        .unwrap();
        tx.execute("UPDATE installation SET generation=1", [])
            .unwrap();
        tx.commit().unwrap();
        (request, state)
    }
    #[test]
    fn startup_recovery_respects_pending_shutdown_before_create() {
        let (_temp, mut supervisor) = fixture();
        let (request, state) = pending_create(&mut supervisor);
        let shutdown = Request {
            version: 1,
            target_installation: None,
            operation_id: Some(id()),
            expected_generation: Some(1),
            action: Action::Shutdown,
        };
        let payload = serde_json::to_string(&shutdown).unwrap();
        supervisor
            .db
            .execute(
                "INSERT INTO operations VALUES(?,?,?,?,'pending',NULL)",
                params![
                    supervisor.caller,
                    shutdown.operation_id,
                    hash(payload.as_bytes()),
                    payload
                ],
            )
            .unwrap();
        supervisor.recover().unwrap();
        assert!(
            !supervisor.root(&state).exists(),
            "startup recovery created a state despite shutdown fence"
        );
        let response = supervisor.handle(request).unwrap();
        assert_eq!(response["retryable"], true);
        assert!(
            response["error"]
                .as_str()
                .unwrap()
                .contains("SHUTDOWN_PENDING")
        );
        assert!(!supervisor.root(&state).exists());
    }
    #[test]
    #[cfg(windows)]
    fn installation_sharing_violation_keeps_create_pending_and_same_identities() {
        let (_temp, mut supervisor) = fixture();
        let (request, state) = pending_create(&mut supervisor);
        let staging = supervisor
            .config
            .data_directory
            .join("staging")
            .join(&state.marker.state);
        fs::create_dir(&staging).unwrap();
        atomic_json(&staging.join("state.json"), &state.marker).unwrap();
        drop(Store::initialize(&staging, &state.marker.repo, &state.marker.device).unwrap());
        let held = OpenOptions::new()
            .read(true)
            .share_mode(3)
            .custom_flags(0x02000000)
            .open(&staging)
            .unwrap();
        let response = supervisor.handle(request.clone()).unwrap();
        assert_eq!(
            response["retryable"], true,
            "sharing violation became a terminal receipt: {response}"
        );
        let status: String = supervisor
            .db
            .query_row(
                "SELECT status FROM operations WHERE id=?",
                [request.operation_id.as_ref().unwrap()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(status, "pending");
        drop(held);
        supervisor.create_native(&state).unwrap();
        assert_eq!(
            supervisor.state(&state.marker.state).unwrap().marker,
            state.marker
        );
        assert!(!staging.exists());
        assert!(supervisor.root(&state).exists());
    }
    #[test]
    fn recoverable_io_classification_does_not_retry_invalid_permanent_failures() {
        #[cfg(windows)]
        let temporary = [32, 33, 170];
        #[cfg(target_os = "linux")]
        let temporary = [libc::EBUSY, libc::EAGAIN, libc::EINTR];
        for code in temporary {
            assert!(retryable(&anyhow::Error::from(
                std::io::Error::from_raw_os_error(code)
            )));
        }
        #[cfg(windows)]
        let permanent = [2, 3, 5, 87, 123];
        #[cfg(target_os = "linux")]
        let permanent = [
            libc::ENOENT,
            libc::EACCES,
            libc::EINVAL,
            libc::ENOTDIR,
            libc::ELOOP,
        ];
        for code in permanent {
            assert!(!retryable(&anyhow::Error::from(
                std::io::Error::from_raw_os_error(code)
            )));
        }
        for message in [
            "STAGING_MARKER_MISMATCH",
            "STORE_IDENTITY_MISMATCH",
            "UNSUPPORTED_WORKER_VERSION",
        ] {
            assert!(!retryable(&anyhow::anyhow!(message)));
        }
    }
}
