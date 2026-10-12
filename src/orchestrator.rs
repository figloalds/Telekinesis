//! Folder lifecycle and networking belong to one daemon. Workers never dial peers.
use crate::{
    core::{Store, id},
    paired_sync::{PublishedState, SyncPage, SyncRoute},
    pairing_code::Verifier,
    runtime::{self, Discovery},
    ws,
};
use anyhow::{Context, Result, bail, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver},
    },
    time::{Duration, Instant},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub format_version: u32,
    pub data_directory: PathBuf,
    pub default_mount_directory: PathBuf,
    #[serde(default = "default_limit")]
    pub maximum_running: usize,
    #[serde(default)]
    pub network: Network,
}
fn default_limit() -> usize {
    8
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Network {
    pub listen: Option<String>,
    pub connections: usize,
    pub sync_interval_ms: u64,
}
impl Default for Network {
    fn default() -> Self {
        Self {
            listen: Some("127.0.0.1:43180".into()),
            connections: 4,
            sync_interval_ms: 500,
        }
    }
}
impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let path = fs::canonicalize(path).context("CONFIG_NOT_FOUND: run tkfs init -f CONFIG")?;
        let mut config: Self = toml::from_str(&fs::read_to_string(&path)?).context(
            "INVALID_CONFIG: this build requires the folder-first format_version=2 configuration",
        )?;
        ensure!(config.format_version == 2, "CONFIG_VERSION_2_REQUIRED");
        ensure!(
            (1..=64).contains(&config.maximum_running),
            "INVALID_WORKER_LIMIT"
        );
        ensure!(
            (1..=16).contains(&config.network.connections),
            "INVALID_CONNECTION_COUNT"
        );
        ensure!(
            (50..=60000).contains(&config.network.sync_interval_ms),
            "INVALID_SYNC_INTERVAL"
        );
        for directory in [
            &mut config.data_directory,
            &mut config.default_mount_directory,
        ] {
            ensure!(!directory.as_os_str().is_empty(), "EMPTY_DIRECTORY");
            if directory.is_relative() {
                *directory = path.parent().unwrap().join(&*directory);
            }
            *directory = std::path::absolute(&*directory)?;
        }
        if config.network.listen.as_deref() == Some("") {
            config.network.listen = None;
        }
        Ok(config)
    }
    pub fn initialize(path: &Path) -> Result<Self> {
        let path = std::path::absolute(path)?;
        fs::create_dir_all(path.parent().context("CONFIG_PARENT_REQUIRED")?)?;
        let config = Self {
            format_version: 2,
            data_directory: "data".into(),
            default_mount_directory: "mounts".into(),
            maximum_running: 8,
            network: Network::default(),
        };
        use std::io::Write;
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?
            .write_all(toml::to_string_pretty(&config)?.as_bytes())?;
        Self::load(&path)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Action {
    List,
    Inspect {
        state: String,
    },
    Create {
        label: String,
        mount: Option<PathBuf>,
        #[serde(default)]
        headless: bool,
    },
    ConfigureMount {
        state: String,
        mount: Option<PathBuf>,
    },
    Start {
        state: String,
    },
    Stop {
        state: String,
    },
    Shutdown,
    ResetPairingCode,
    Pair {
        endpoint: String,
        code: String,
        connections: Option<usize>,
    },
    Unpair {
        endpoint: String,
    },
    Peers,
    RemoteList {
        endpoint: String,
    },
    Receive {
        endpoint: String,
        repo: String,
        label: String,
        mount: Option<PathBuf>,
        #[serde(default)]
        headless: bool,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Project {
    pub state: String,
    pub label: String,
    pub repo: String,
    pub device: String,
    pub directory: PathBuf,
    pub mount: Option<PathBuf>,
    pub desired_running: bool,
}
#[derive(Clone)]
struct Peer {
    endpoint: String,
    code: String,
    installation: String,
    connections: usize,
    revision: String,
}
impl Peer {
    fn pool_size(&self) -> usize {
        self.connections.max(2)
    }

    fn bulk_lane(&self, repo: &str) -> usize {
        1 + repo
            .bytes()
            .fold(0usize, |n, b| n.wrapping_mul(31).wrapping_add(b as usize))
            % (self.pool_size() - 1)
    }
}

fn lane_role(lane: usize) -> &'static str {
    if lane == 0 { "control" } else { "bulk" }
}
#[derive(Default, Clone, Serialize)]
struct LinkStatus {
    connected: bool,
    exchanges: u64,
    last_success_ms: Option<u64>,
    error: Option<String>,
    repositories: BTreeMap<String, RepositoryStatus>,
}
#[derive(Default, Clone, Serialize)]
struct RepositoryStatus {
    last_success_ms: Option<u64>,
    caught_up: bool,
    error: Option<String>,
    consecutive_failures: u32,
    retry_at_ms: Option<u64>,
    #[serde(skip)]
    retry_at: Option<Instant>,
}
impl RepositoryStatus {
    fn failed(&mut self, error: &anyhow::Error) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        let delay = retry_delay(self.consecutive_failures);
        self.caught_up = false;
        self.error = Some(format!("{error:#}"));
        self.retry_at_ms = Some(crate::core::time() + delay.as_millis() as u64);
        self.retry_at = Some(Instant::now() + delay);
    }

    fn succeeded(&mut self, result: &Value) {
        self.last_success_ms = Some(crate::core::time());
        self.caught_up = result["caught_up"] == true;
        self.error = None;
        self.consecutive_failures = 0;
        self.retry_at_ms = None;
        self.retry_at = None;
    }
}

fn retry_delay(failures: u32) -> Duration {
    Duration::from_secs((1u64 << failures.saturating_sub(1).min(5)).min(30))
}
struct Worker {
    child: Child,
    messages: Receiver<Value>,
    startup: Option<crate::startup::Watchdog>,
}
pub struct Supervisor {
    pub config: Config,
    db: Connection,
    installation: String,
    workers: BTreeMap<String, Worker>,
    failures: BTreeMap<String, String>,
    publication_errors: BTreeMap<String, String>,
    links: BTreeMap<String, LinkStatus>,
    remote_catalogs: BTreeMap<String, Vec<PublishedState>>,
    stopping: Arc<AtomicBool>,
    _lock: File,
}
pub type Shared = Arc<Mutex<Supervisor>>;

pub fn lock(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.share_mode(0);
    }
    let file = options
        .open(path)
        .context("ALREADY_RUNNING_OR_PATH_UNAVAILABLE")?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        ensure!(
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
            "ALREADY_RUNNING"
        );
    }
    Ok(file)
}
pub fn lock_state(path: &Path) -> Result<File> {
    lock(&path.join("owner.lock"))
}

impl Supervisor {
    pub fn open(config: Config) -> Result<Self> {
        fs::create_dir_all(&config.data_directory)?;
        fs::create_dir_all(&config.default_mount_directory)?;
        let lock = lock(&config.data_directory.join("orchestrator.lock"))?;
        // A new catalog name deliberately avoids reinterpreting the old O1 registry.
        let db = Connection::open(config.data_directory.join("folders.sqlite"))?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS projects(state TEXT PRIMARY KEY,label TEXT NOT NULL,repo TEXT NOT NULL UNIQUE,device TEXT NOT NULL,directory TEXT NOT NULL,mount TEXT,desired INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS peers(endpoint TEXT PRIMARY KEY,code TEXT NOT NULL,installation TEXT NOT NULL,connections INTEGER NOT NULL,revision TEXT NOT NULL);")?;
        db.execute(
            "INSERT OR IGNORE INTO settings VALUES('installation',?)",
            [id()],
        )?;
        let installation = db.query_row(
            "SELECT value FROM settings WHERE key='installation'",
            [],
            |r| r.get(0),
        )?;
        Ok(Self {
            config,
            db,
            installation,
            workers: BTreeMap::new(),
            failures: BTreeMap::new(),
            publication_errors: BTreeMap::new(),
            links: BTreeMap::new(),
            remote_catalogs: BTreeMap::new(),
            stopping: Arc::new(AtomicBool::new(false)),
            _lock: lock,
        })
    }
    pub fn projects(&self) -> Result<Vec<Project>> {
        Ok(self.db.prepare("SELECT state,label,repo,device,directory,mount,desired FROM projects ORDER BY label,state")?
            .query_map([], |r| Ok(Project { state: r.get(0)?, label: r.get(1)?, repo: r.get(2)?, device: r.get(3)?, directory: PathBuf::from(r.get::<_,String>(4)?), mount: r.get::<_, Option<String>>(5)?.map(PathBuf::from), desired_running: r.get(6)? }))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }
    fn project(&self, state: &str) -> Result<Project> {
        self.projects()?
            .into_iter()
            .find(|p| p.state == state)
            .context("FOLDER_NOT_FOUND")
    }
    fn peers(&self) -> Result<Vec<Peer>> {
        Ok(self.db.prepare("SELECT endpoint,code,installation,connections,revision FROM peers ORDER BY endpoint")?
            .query_map([], |r| Ok(Peer { endpoint:r.get(0)?, code:r.get(1)?, installation:r.get(2)?, connections:r.get(3)?, revision:r.get(4)? }))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }
    fn verifier(&self) -> Result<Verifier> {
        let value: Option<String> = self
            .db
            .query_row("SELECT value FROM settings WHERE key='pairing'", [], |r| {
                r.get(0)
            })
            .optional()?;
        serde_json::from_str(&value.context("PAIRING_CODE_NOT_SET: reset-pairing-code first")?)
            .map_err(Into::into)
    }
    fn create(
        &mut self,
        label: String,
        repo: Option<String>,
        mount: Option<PathBuf>,
        headless: bool,
    ) -> Result<Value> {
        ensure!(
            !label.trim().is_empty() && label.len() <= 200,
            "INVALID_FOLDER_LABEL"
        );
        let state = id();
        let repo = repo.unwrap_or_else(id);
        ensure!(
            !self.projects()?.iter().any(|p| p.repo == repo),
            "REPOSITORY_ALREADY_MANAGED"
        );
        ensure!(
            self.workers.len() < self.config.maximum_running,
            "WORKER_LIMIT"
        );
        let directory = self.config.data_directory.join("states").join(&state);
        let mount = if headless {
            ensure!(mount.is_none(), "HEADLESS_MOUNT_CONFLICT");
            None
        } else {
            if mount.is_none() {
                crate::core::name_key(&label).context("INVALID_MOUNT_FOLDER_NAME")?;
            }
            Some(mount.unwrap_or_else(|| self.config.default_mount_directory.join(&label)))
        };
        let mount = mount.map(|p| self.mount_path(&directory, &p)).transpose()?;
        let device = id();
        let _store = Store::initialize(&directory, &repo, &device)?;
        drop(_store);
        self.db.execute(
            "INSERT INTO projects VALUES(?,?,?,?,?,?,1)",
            params![
                state,
                label,
                repo,
                device,
                directory.to_string_lossy(),
                mount.as_ref().map(|p| p.to_string_lossy().into_owned())
            ],
        )?;
        // Keep the durable record if the mount fails; list/inspect reports the error
        // and configure-mount/start can repair it without losing the state.
        if let Err(error) = self.start(&state) {
            self.failures.insert(state.clone(), format!("{error:#}"));
        }
        self.inspect(&state)
    }
    fn mount_path(&self, directory: &Path, requested: &Path) -> Result<PathBuf> {
        let path = std::path::absolute(requested)?;
        let parent = path.parent().context("MOUNT_PARENT_REQUIRED")?;
        fs::create_dir_all(parent)?;
        let path =
            fs::canonicalize(parent)?.join(path.file_name().context("FOLDER_MOUNT_REQUIRED")?);
        let data = fs::canonicalize(&self.config.data_directory)?;
        ensure!(
            !path.starts_with(&data) && !data.starts_with(&path) && !directory.starts_with(&path),
            "MOUNT_OVERLAPS_DATA"
        );
        ensure!(!path.exists(), "MOUNT_PATH_MUST_NOT_EXIST");
        for p in self.projects()? {
            if let Some(other) = p.mount {
                ensure!(
                    !path.starts_with(&other) && !other.starts_with(&path),
                    "MOUNT_OVERLAPS_FOLDER"
                );
            }
        }
        Ok(path)
    }
    fn discovery(&self, state: &str) -> Result<Discovery> {
        let p = self.project(state)?;
        ensure!(
            self.workers.get(state).is_some_and(|w| w.startup.is_none()),
            "WORKER_NOT_RUNNING"
        );
        let info: Discovery = serde_json::from_slice(&fs::read(p.directory.join("runtime.json"))?)?;
        ensure!(
            info.repo == p.repo && info.device == p.device,
            "WORKER_STATE_MISMATCH"
        );
        Ok(info)
    }
    fn start(&mut self, state: &str) -> Result<()> {
        if self.workers.contains_key(state) {
            return Ok(());
        }
        ensure!(
            self.workers.len() < self.config.maximum_running,
            "WORKER_LIMIT"
        );
        let p = self.project(state)?;
        // Discover an existing worker after daemon restart; do not start a duplicate.
        if let Ok(bytes) = fs::read(p.directory.join("runtime.json"))
            && let Ok(info) = serde_json::from_slice::<Discovery>(&bytes)
            && info.repo == p.repo
            && info.device == p.device
            && let Ok(status) = runtime::rpc(&info, &id(), json!({"op":"status"}))
            && status["repo"] == p.repo
            && status["device"] == p.device
        {
            runtime::rpc(&info, &id(), json!({"op":"stop"}))?;
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if let Ok(lock) = lock_state(&p.directory) {
                    drop(lock);
                    break;
                }
                ensure!(Instant::now() < deadline, "PREVIOUS_WORKER_STOP_TIMEOUT");
                std::thread::sleep(Duration::from_millis(25));
            }
        }
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(p.directory.join("worker.log"))?;
        let mut command = Command::new(std::env::current_exe()?);
        command.arg("daemon").arg("--state").arg(&p.directory);
        if let Some(mount) = &p.mount {
            command.arg("--mount").arg(mount);
        }
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(log.try_clone()?);
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        let mut child = command.spawn()?;
        let stdout = child.stdout.take().expect("piped worker stdout");
        let (sender, messages) = mpsc::channel();
        std::thread::spawn(move || {
            let mut log = log;
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if let Ok(value) = serde_json::from_str::<Value>(&line) {
                    let _ = sender.send(value);
                }
                let _ = writeln!(log, "{line}");
            }
        });
        self.workers.insert(
            state.into(),
            Worker {
                child,
                messages,
                startup: Some(crate::startup::Watchdog::new(Instant::now())),
            },
        );
        Ok(())
    }
    fn poll_worker(&mut self, state: &str) -> Result<()> {
        let p = self.project(state)?;
        let worker = self.workers.get_mut(state).context("WORKER_NOT_RUNNING")?;
        if let Some(status) = worker.child.try_wait()? {
            bail!(
                "WORKER_EXITED: {status}; see {}",
                p.directory.join("worker.log").display()
            );
        }
        // Only this child's stdout can renew its lease. Old runtime.json files
        // and a responsive side thread cannot keep stalled recovery alive.
        for message in worker.messages.try_iter() {
            if let Some(startup) = &mut worker.startup {
                if let Some(sequence) = message["startup_progress"].as_u64() {
                    startup.observe(
                        sequence,
                        message["phase"].as_str().unwrap_or("recovering"),
                        Instant::now(),
                    );
                }
                if message["ready"] == true {
                    ensure!(
                        message["repo"] == p.repo
                            && message["device"] == p.device
                            && message["mounted"] == p.mount.is_some(),
                        "WORKER_STATE_MISMATCH"
                    );
                    worker.startup = None;
                }
            }
        }
        if worker.startup.is_none() {
            self.failures.remove(state);
        }
        if let Some(startup) = &worker.startup {
            ensure!(
                !startup.expired(Instant::now()),
                "WORKER_START_TIMEOUT: no recovery progress for 30s during {}; see {}",
                startup.phase,
                p.directory.join("worker.log").display()
            );
        }
        Ok(())
    }
    fn stop(&mut self, state: &str) -> Result<()> {
        if !self.workers.contains_key(state) {
            return Ok(());
        }
        // Drain readiness before deciding whether the worker can stop via RPC.
        let _ = self.poll_worker(state);
        if self.workers[state].startup.is_some() {
            let worker = self.workers.get_mut(state).unwrap();
            if worker.child.try_wait()?.is_none() {
                worker.child.kill()?;
            }
            worker.child.wait()?;
            self.workers.remove(state);
            return Ok(());
        }
        let info = self.discovery(state)?;
        runtime::rpc(&info, &id(), json!({"op":"stop"}))?;
        let deadline = Instant::now() + Duration::from_secs(5);
        while self
            .workers
            .get_mut(state)
            .unwrap()
            .child
            .try_wait()?
            .is_none()
        {
            ensure!(Instant::now() < deadline, "WORKER_STOP_TIMEOUT");
            std::thread::sleep(Duration::from_millis(25));
        }
        self.workers.remove(state);
        Ok(())
    }
    fn inspect(&self, state: &str) -> Result<Value> {
        let mut value = json!(self.project(state)?);
        let worker = self.workers.get(state);
        value["running"] = json!(worker.is_some_and(|w| w.startup.is_none()));
        value["starting"] = json!(worker.is_some_and(|w| w.startup.is_some()));
        value["startup_phase"] = json!(worker.and_then(|w| w.startup.as_ref()).map(|s| &s.phase));
        value["error"] = json!(self.failures.get(state));
        value["publication_error"] = json!(self.publication_errors.get(state));
        Ok(value)
    }
    fn reconcile(&mut self) -> Result<()> {
        let states: Vec<_> = self.workers.keys().cloned().collect();
        for state in states {
            if let Err(error) = self.poll_worker(&state) {
                let mut worker = self.workers.remove(&state).unwrap();
                let _ = worker.child.kill();
                let _ = worker.child.wait();
                self.failures.insert(state, format!("{error:#}"));
            }
        }
        for p in self.projects()? {
            if p.desired_running
                && !self.workers.contains_key(&p.state)
                && let Err(error) = self.start(&p.state)
            {
                self.failures.insert(p.state, format!("{error:#}"));
            }
        }
        Ok(())
    }
}

pub fn client(config: &Config, action: &Action) -> Result<Value> {
    let mut action = action.clone();
    match &mut action {
        Action::Create { mount, .. }
        | Action::Receive { mount, .. }
        | Action::ConfigureMount { mount, .. } => {
            if let Some(path) = mount {
                *path = std::path::absolute(&*path)?;
            }
        }
        _ => {}
    }

    let discovery: Value = serde_json::from_slice(
        &fs::read(config.data_directory.join("orchestrator.json"))
            .context("ORCHESTRATOR_NOT_RUNNING")?,
    )?;
    let mut socket = ws::connect(
        discovery["control"]
            .as_str()
            .context("INVALID_ORCHESTRATOR_DISCOVERY")?,
    )?;
    ws::call(&mut socket, &action)
}

pub fn manage(shared: &Shared, action: Action) -> Result<Value> {
    // Remote I/O runs without holding the lifecycle/catalog lock.
    match action {
        Action::Pair {
            endpoint,
            code,
            connections,
        } => {
            let (installation, count) = {
                let s = shared.lock().unwrap();
                (
                    s.installation.clone(),
                    connections.unwrap_or(s.config.network.connections),
                )
            };
            ensure!((1..=16).contains(&count), "INVALID_CONNECTION_COUNT");
            let (_, remote) = dial(&endpoint, &code, &installation)?;
            ensure!(remote != installation, "CANNOT_PAIR_WITH_SELF");
            let s = shared.lock().unwrap();
            s.db.execute("INSERT INTO peers VALUES(?,?,?,?,?) ON CONFLICT(endpoint) DO UPDATE SET code=excluded.code,installation=excluded.installation,connections=excluded.connections,revision=excluded.revision", params![endpoint,code,remote,count,id()])?;
            return Ok(
                json!({"endpoint":endpoint,"installation":remote,"connections":count.max(2),"configured_connections":count,"reserved_connections":1}),
            );
        }
        Action::RemoteList { endpoint } => return Ok(json!(remote_list(shared, &endpoint)?)),
        Action::Receive {
            endpoint,
            repo,
            label,
            mount,
            headless,
        } => {
            ensure!(
                remote_list(shared, &endpoint)?
                    .iter()
                    .any(|p| p.repo == repo),
                "REMOTE_FOLDER_NOT_FOUND"
            );
            return shared
                .lock()
                .unwrap()
                .create(label, Some(repo), mount, headless);
        }
        _ => {}
    }
    let mut s = shared.lock().unwrap();
    ensure!(!s.stopping.load(Ordering::Relaxed), "ORCHESTRATOR_STOPPING");
    match action {
        Action::List => Ok(json!({"installation":s.installation,"folders":s.projects()?.iter().map(|p|s.inspect(&p.state)).collect::<Result<Vec<_>>>()?})),
        Action::Inspect { state } => s.inspect(&state),
        Action::Create { label, mount, headless } => s.create(label, None, mount, headless),
        Action::ConfigureMount { state, mount } => {
            ensure!(!s.workers.contains_key(&state), "STOP_FOLDER_BEFORE_CONFIGURING");
            let p = s.project(&state)?;
            let mount = if mount == p.mount { mount } else { mount.map(|p2|s.mount_path(&p.directory,&p2)).transpose()? };
            s.db.execute("UPDATE projects SET mount=? WHERE state=?", params![mount.map(|p|p.to_string_lossy().into_owned()),state])?;
            s.inspect(&state)
        }
        Action::Start { state } => {
            s.project(&state)?;
            s.db.execute("UPDATE projects SET desired=1 WHERE state=?", [&state])?;
            s.start(&state)?;
            s.inspect(&state)
        }
        Action::Stop { state } => {
            s.project(&state)?;
            s.stop(&state)?;
            s.db.execute("UPDATE projects SET desired=0 WHERE state=?", [&state])?;
            s.inspect(&state)
        }
        Action::Shutdown => {
            let states: Vec<_> = s.workers.keys().cloned().collect();
            for state in states { s.stop(&state)?; }
            s.stopping.store(true, Ordering::Relaxed);
            Ok(json!({"stopped":true}))
        }
        Action::ResetPairingCode => {
            let (code, verifier) = Verifier::generate();
            s.db.execute("INSERT INTO settings VALUES('pairing',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [serde_json::to_string(&verifier)?])?;
            Ok(json!({"code":code}))
        }
        Action::Unpair { endpoint } => {
            s.db.execute("DELETE FROM peers WHERE endpoint=?", [&endpoint])?;
            Ok(json!({"removed":endpoint}))
        }
        Action::Peers => Ok(json!(s.peers()?.iter().map(|p| json!({"endpoint":p.endpoint,"installation":p.installation,"connections":p.pool_size(),"configured_connections":p.connections,"reserved_connections":1,"lanes":(0..p.pool_size()).map(|lane| {
            let mut status = json!(s.links.get(&link_key(p,lane)).cloned().unwrap_or_default());
            status["role"] = json!(lane_role(lane));
            status
        }).collect::<Vec<_>>()})).collect::<Vec<_>>())),
        _ => unreachable!(),
    }
}

fn dial(endpoint: &str, code: &str, installation: &str) -> Result<(ws::Socket, String)> {
    dial_role(endpoint, code, installation, Some("control"))
}
fn dial_role(
    endpoint: &str,
    code: &str,
    installation: &str,
    role: Option<&str>,
) -> Result<(ws::Socket, String)> {
    let mut socket = ws::connect(endpoint)?;
    let hello = ws::call(
        &mut socket,
        &json!({"op":"pair","version":2,"code":code,"installation":installation,"lane":role}),
    )?;
    if let Some(role) = role {
        ensure!(
            hello["lane"] == role,
            "RESERVED_LANES_UNSUPPORTED: upgrade the remote orchestrator"
        );
    }
    Ok((
        socket,
        hello["installation"]
            .as_str()
            .context("INVALID_PAIR_REPLY")?
            .into(),
    ))
}
fn remote_list(shared: &Shared, endpoint: &str) -> Result<Vec<PublishedState>> {
    let (p, installation) = {
        let s = shared.lock().unwrap();
        (
            s.peers()?
                .into_iter()
                .find(|p| p.endpoint == endpoint)
                .context("PEER_NOT_PAIRED")?,
            s.installation.clone(),
        )
    };
    let (mut socket, remote) = dial(&p.endpoint, &p.code, &installation)?;
    ensure!(remote == p.installation, "PEER_INSTALLATION_CHANGED");
    Ok(serde_json::from_value(ws::call(
        &mut socket,
        &json!({"op":"list"}),
    )?)?)
}
fn published(shared: &Shared) -> Result<Vec<PublishedState>> {
    let workers = {
        let s = shared.lock().unwrap();
        s.projects()?
            .into_iter()
            .filter(|p| s.workers.get(&p.state).is_some_and(|w| w.startup.is_none()))
            .map(|p| {
                let info = s.discovery(&p.state);
                (p, info)
            })
            .collect::<Vec<_>>()
    };
    let mut published = Vec::new();
    let mut errors = BTreeMap::new();
    for (p, info) in workers {
        let result = (|| -> Result<PublishedState> {
            let state: PublishedState = serde_json::from_value(runtime::rpc(
                &info?,
                &id(),
                json!({"op":"published-summary"}),
            )?)?;
            ensure!(
                state.repo == p.repo && state.replica == p.device,
                "WORKER_STATE_MISMATCH"
            );
            Ok(state)
        })();
        match result {
            Ok(mut state) => {
                state.label = p.label;
                published.push(state);
            }
            Err(error) => {
                errors.insert(p.state, format!("PUBLISHED_SUMMARY_FAILED: {error:#}"));
            }
        }
    }
    shared.lock().unwrap().publication_errors = errors;
    Ok(published)
}
fn route(info: &Discovery, peer: &str, replica: &str) -> SyncRoute {
    SyncRoute {
        peer: peer.into(),
        repo: info.repo.clone(),
        local_replica: info.device.clone(),
        remote_replica: replica.into(),
    }
}
fn serve_peer(shared: Shared, stream: std::net::TcpStream) -> Result<()> {
    let mut socket = ws::accept(stream)?;
    let hello: Value = ws::receive(&mut socket)?;
    let role = hello["lane"].as_str();
    let verified = (|| -> Result<(Verifier, String, String)> {
        ensure!(
            hello["op"] == "pair" && hello["version"] == 2,
            "INCOMPATIBLE_ORCHESTRATOR_PROTOCOL"
        );
        ensure!(
            hello["lane"].is_null() || matches!(role, Some("control" | "bulk")),
            "INVALID_LANE_ROLE"
        );
        let peer = hello["installation"]
            .as_str()
            .context("INSTALLATION_REQUIRED")?
            .to_owned();
        let s = shared.lock().unwrap();
        let verifier = s.verifier()?;
        ensure!(
            verifier.verify(hello["code"].as_str().context("PAIRING_CODE_REQUIRED")?),
            "INVALID_PAIRING_CODE"
        );
        ensure!(peer != s.installation, "CANNOT_PAIR_WITH_SELF");
        Ok((verifier, peer, s.installation.clone()))
    })();
    let (verifier, peer, installation) = match verified {
        Ok(v) => v,
        Err(error) => {
            ws::reply(&mut socket, Err(error))?;
            return Ok(());
        }
    };
    ws::reply(
        &mut socket,
        Ok(json!({"installation":installation,"version":2,"lane":role})),
    )?;
    loop {
        let request: Value = ws::receive(&mut socket)?;
        let result = (|| -> Result<Value> {
            {
                let s = shared.lock().unwrap();
                ensure!(!s.stopping.load(Ordering::Relaxed), "ORCHESTRATOR_STOPPING");
                ensure!(s.verifier()?.hash == verifier.hash, "PAIRING_CODE_CHANGED");
            }
            match request["op"].as_str() {
                Some("ping") => Ok(json!({"alive":true})),
                Some("list") => Ok(json!(published(&shared)?)),
                Some("exchange") => {
                    let page: SyncPage = serde_json::from_value(request["page"].clone())?;
                    let info = {
                        let s = shared.lock().unwrap();
                        let p = s
                            .projects()?
                            .into_iter()
                            .find(|p| p.repo == page.repo)
                            .context("REPOSITORY_NOT_MANAGED")?;
                        s.discovery(&p.state)?
                    };
                    let binding = route(&info, &peer, &page.sender);
                    runtime::rpc(
                        &info,
                        &id(),
                        json!({"op":"published-exchange","route":binding,"page":page,"max_frame":4*1024*1024,"metadata_only":role == Some("control")}),
                    )
                }
                _ => bail!("UNKNOWN_PEER_OPERATION"),
            }
        })();
        ws::reply(&mut socket, result)?;
    }
}
fn link_key(peer: &Peer, lane: usize) -> String {
    format!("{}:{}:{lane}", peer.endpoint, peer.revision)
}
fn lane_active(shared: &Shared, peer: &Peer) -> bool {
    let s = shared.lock().unwrap();
    !s.stopping.load(Ordering::Relaxed)
        && s.peers().is_ok_and(|peers| {
            peers
                .iter()
                .any(|p| p.endpoint == peer.endpoint && p.revision == peer.revision)
        })
}

fn lane_wait(shared: &Shared, peer: &Peer, delay: Duration) {
    let until = Instant::now() + delay;
    while Instant::now() < until && lane_active(shared, peer) {
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn run_lane(shared: Shared, peer: Peer, lane: usize) {
    let key = link_key(&peer, lane);
    let catalog_key = link_key(&peer, 0);
    let metadata_only = lane == 0;
    let mut connection_failures = 0u32;
    while lane_active(&shared, &peer) {
        let result = (|| -> Result<()> {
            let installation = shared.lock().unwrap().installation.clone();
            let (mut socket, remote) = dial_role(
                &peer.endpoint,
                &peer.code,
                &installation,
                Some(lane_role(lane)),
            )?;
            ensure!(remote == peer.installation, "PEER_INSTALLATION_CHANGED");
            shared
                .lock()
                .unwrap()
                .links
                .entry(key.clone())
                .or_default()
                .connected = true;
            while lane_active(&shared, &peer) {
                let remote: Vec<PublishedState> = if metadata_only {
                    let catalog: Vec<PublishedState> =
                        serde_json::from_value(ws::call(&mut socket, &json!({"op":"list"}))?)?;
                    shared
                        .lock()
                        .unwrap()
                        .remote_catalogs
                        .insert(catalog_key.clone(), catalog.clone());
                    catalog
                } else {
                    // Keep idle bulk sockets alive without repeating discovery
                    // or putting control work behind content transfers.
                    ws::call(&mut socket, &json!({"op":"ping"}))?;
                    shared
                        .lock()
                        .unwrap()
                        .remote_catalogs
                        .get(&catalog_key)
                        .cloned()
                        .unwrap_or_default()
                };
                let (local, interval) = {
                    let s = shared.lock().unwrap();
                    (s.projects()?, s.config.network.sync_interval_ms)
                };
                let mut active_repositories = BTreeSet::new();
                for remote in remote {
                    // Events visit every repository on the reserved lane;
                    // object cursors have exactly one serial bulk owner.
                    if !metadata_only && peer.bulk_lane(&remote.repo) != lane {
                        continue;
                    }
                    let Some(local) = local
                        .iter()
                        .find(|p| p.repo == remote.repo && p.desired_running)
                    else {
                        continue;
                    };
                    active_repositories.insert(remote.repo.clone());
                    if !lane_active(&shared, &peer) {
                        break;
                    }
                    {
                        let mut s = shared.lock().unwrap();
                        let status = s
                            .links
                            .entry(key.clone())
                            .or_default()
                            .repositories
                            .entry(remote.repo.clone())
                            .or_default();
                        if status.retry_at.is_some_and(|until| Instant::now() < until) {
                            continue;
                        }
                    }
                    let mut connection_failed = false;
                    let exchange = (|| -> Result<Value> {
                        let info = shared
                            .lock()
                            .unwrap()
                            .discovery(&local.state)
                            .context("LOCAL_WORKER_UNAVAILABLE")?;
                        let binding = route(&info, &peer.installation, &remote.replica);
                        let page: SyncPage = serde_json::from_value(runtime::rpc(
                            &info,
                            &id(),
                            json!({"op":"published-offer","route":binding,"max_frame":4*1024*1024,"metadata_only":metadata_only}),
                        ).context("LOCAL_SYNC_OFFER_FAILED")?)?;
                        let sent: BTreeSet<_> =
                            page.bundle.events.iter().map(|e| e.id.clone()).collect();
                        let response = ws::call(&mut socket, &json!({"op":"exchange","page":page}))
                            .map_err(|error| {
                                connection_failed =
                                    error.downcast_ref::<ws::RemoteError>().is_none();
                                error.context("REMOTE_SYNC_EXCHANGE_FAILED")
                            })?;
                        runtime::rpc(
                            &info,
                            &id(),
                            json!({"op":"published-apply","route":binding,"page":response,"request":page.request,"sent":sent,"max_frame":4*1024*1024,"metadata_only":metadata_only}),
                        ).context("LOCAL_SYNC_APPLY_FAILED")
                    })();
                    if connection_failed {
                        return exchange.map(|_| ());
                    }
                    let mut s = shared.lock().unwrap();
                    let link = s.links.entry(key.clone()).or_default();
                    let status = link.repositories.entry(remote.repo).or_default();
                    match exchange {
                        Ok(result) => {
                            status.succeeded(&result);
                            link.exchanges += 1;
                        }
                        Err(error) => status.failed(&error),
                    }
                }
                {
                    let mut s = shared.lock().unwrap();
                    let status = s.links.entry(key.clone()).or_default();
                    status.last_success_ms = Some(crate::core::time());
                    status.error = None;
                    status
                        .repositories
                        .retain(|repo, _| active_repositories.contains(repo));
                }
                connection_failures = 0;
                lane_wait(&shared, &peer, Duration::from_millis(interval));
            }
            let _ = socket.close(None);
            Ok(())
        })();
        {
            let mut s = shared.lock().unwrap();
            let status = s.links.entry(key.clone()).or_default();
            status.connected = false;
            status.error = result.err().map(|e| format!("{e:#}"));
            for repository in status.repositories.values_mut() {
                repository.caught_up = false;
            }
        }
        if lane_active(&shared, &peer) {
            connection_failures = connection_failures.saturating_add(1);
            lane_wait(&shared, &peer, retry_delay(connection_failures));
        }
    }
    shared.lock().unwrap().links.remove(&key);
    if metadata_only {
        shared.lock().unwrap().remote_catalogs.remove(&catalog_key);
    }
}

pub fn run(config: Config) -> Result<()> {
    let supervisor = Supervisor::open(config)?;
    let stop = supervisor.stopping.clone();
    let control = TcpListener::bind("127.0.0.1:0")?;
    control.set_nonblocking(true)?;
    let network = supervisor
        .config
        .network
        .listen
        .as_ref()
        .map(TcpListener::bind)
        .transpose()?;
    if let Some(listener) = &network {
        listener.set_nonblocking(true)?;
    }
    let discovery = json!({"format":2,"installation":supervisor.installation,"control":format!("ws://{}",control.local_addr()?),"network":network.as_ref().map(|l|l.local_addr().map(|a|format!("ws://{a}"))).transpose()?});
    let discovery_path = supervisor.config.data_directory.join("orchestrator.json");
    fs::write(&discovery_path, serde_json::to_vec_pretty(&discovery)?)?;
    let shared = Arc::new(Mutex::new(supervisor));
    println!("{discovery}");
    let mut lanes = BTreeMap::<String, std::thread::JoinHandle<()>>::new();
    let mut controls = Vec::new();
    let mut reconcile = Instant::now();
    while !stop.load(Ordering::Relaxed) {
        if let Ok((stream, _)) = control.accept() {
            let shared = shared.clone();
            controls.push(std::thread::spawn(move || {
                let _ = (|| -> Result<()> {
                    let mut socket = ws::accept(stream)?;
                    let action = ws::receive(&mut socket)?;
                    ws::reply(&mut socket, manage(&shared, action))
                })();
            }));
        }
        if let Some(listener) = &network
            && let Ok((stream, _)) = listener.accept()
        {
            let shared = shared.clone();
            std::thread::spawn(move || {
                let _ = serve_peer(shared, stream);
            });
        }
        if Instant::now() >= reconcile {
            if let Err(error) = shared.lock().unwrap().reconcile() {
                eprintln!("worker reconcile: {error:#}");
            }
            reconcile = Instant::now() + Duration::from_secs(2);
        }
        let peers = shared.lock().unwrap().peers()?;
        for peer in peers {
            for lane in 0..peer.pool_size() {
                let key = link_key(&peer, lane);
                lanes.entry(key).or_insert_with(|| {
                    let shared = shared.clone();
                    let peer = peer.clone();
                    std::thread::spawn(move || run_lane(shared, peer, lane))
                });
            }
        }
        lanes.retain(|_, handle| !handle.is_finished());
        controls.retain(|handle| !handle.is_finished());
        std::thread::sleep(Duration::from_millis(10));
    }
    // Finish the shutdown response before the executable exits.
    for handle in controls {
        let _ = handle.join();
    }
    let _ = fs::remove_file(discovery_path);
    Ok(())
}
