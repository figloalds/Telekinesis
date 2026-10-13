//! Real daemons and workers driven through dynamic JSON (generalized from
//! `tests/orchestrator.rs:16-160`). Nothing here names an `Action` variant, so
//! qualification files compile before new variants exist.
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::{
    fs,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::mpsc::{self, Receiver},
    thread,
    time::{Duration, Instant},
};
use tkfs::{core::id, runtime, ws};

/// The `tkfs` binary under test (the baseline binary in WP0.5's worktree).
pub fn bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_tkfs"))
}

/// Poll `predicate` until it holds or `timeout` passes. Returns whether it held.
pub fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if predicate() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(50));
    }
}

/// Like `wait_until`, but panics with `what` on timeout.
#[track_caller]
pub fn wait_for(what: &str, timeout: Duration, predicate: impl FnMut() -> bool) {
    assert!(wait_until(timeout, predicate), "timeout waiting for {what}");
}

/// Last `max` bytes of a text file (lossy), or an empty string.
pub fn tail(path: &Path, max: usize) -> String {
    let Ok(bytes) = fs::read(path) else {
        return String::new();
    };
    let start = bytes.len().saturating_sub(max);
    String::from_utf8_lossy(&bytes[start..]).into_owned()
}

#[derive(Clone, Debug)]
pub struct NodeConfig {
    /// `network.sync_interval_ms` (50..=60000).
    pub interval: u64,
    /// `network.connections` (1..=16).
    pub connections: usize,
    /// `network.listen`; `None` disables the peer listener.
    pub listen: Option<String>,
    /// Extra environment for the daemon (inherited by its workers).
    pub env: Vec<(String, String)>,
    pub maximum_running: usize,
}
impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            interval: 50,
            connections: 3,
            listen: Some("127.0.0.1:0".into()),
            env: vec![],
            maximum_running: 8,
        }
    }
}
impl NodeConfig {
    pub fn interval(mut self, ms: u64) -> Self {
        self.interval = ms;
        self
    }
    pub fn connections(mut self, n: usize) -> Self {
        self.connections = n;
        self
    }
    pub fn env(mut self, key: &str, value: &str) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }
    pub fn toml(&self) -> String {
        let listen = self.listen.as_deref().unwrap_or("");
        format!(
            "format_version=2\ndata_directory='data'\ndefault_mount_directory='mounts'\nmaximum_running={}\n[network]\nlisten='{listen}'\nconnections={}\nsync_interval_ms={}\n",
            self.maximum_running, self.connections, self.interval
        )
    }
}

/// One `tkfs orchestrator` process with its own temporary root:
/// `ROOT/orchestrator.toml`, `ROOT/data`, `ROOT/mounts`, `ROOT/daemon.log`.
pub struct Node {
    pub root: tempfile::TempDir,
    pub cfg: NodeConfig,
    child: Option<Child>,
}
impl Default for Node {
    fn default() -> Self {
        Self::new()
    }
}
impl Node {
    pub fn new() -> Self {
        Self::start(NodeConfig::default())
    }
    pub fn with_interval(interval: u64) -> Self {
        Self::start(NodeConfig::default().interval(interval))
    }
    /// Create a fresh root, write the configuration and start the daemon.
    pub fn start(cfg: NodeConfig) -> Self {
        let root = tempfile::tempdir().unwrap();
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(root.path(), fs::Permissions::from_mode(0o777)).unwrap();
        }
        fs::write(root.path().join("orchestrator.toml"), cfg.toml()).unwrap();
        let mut node = Self {
            root,
            cfg,
            child: None,
        };
        node.spawn();
        node
    }
    /// Start a daemon on an existing root (for example a legacy fixture copy)
    /// whose `orchestrator.toml` already exists. `cfg.env` is applied.
    pub fn adopt_root(root: tempfile::TempDir, cfg: NodeConfig) -> Self {
        let mut node = Self {
            root,
            cfg,
            child: None,
        };
        node.spawn();
        node
    }
    pub fn path(&self) -> &Path {
        self.root.path()
    }
    pub fn config_path(&self) -> PathBuf {
        self.root.path().join("orchestrator.toml")
    }
    pub fn data_dir(&self) -> PathBuf {
        self.root.path().join("data")
    }
    pub fn log_path(&self) -> PathBuf {
        self.root.path().join("daemon.log")
    }
    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(Child::id)
    }
    pub fn running(&mut self) -> bool {
        self.child
            .as_mut()
            .is_some_and(|c| c.try_wait().ok().flatten().is_none())
    }
    /// Spawn the daemon and wait until it answers `list` (20 s).
    pub fn spawn(&mut self) {
        assert!(self.child.is_none(), "daemon already running");
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.log_path())
            .unwrap();
        let mut command = Command::new(bin());
        command
            .arg("-f")
            .arg(self.config_path())
            .arg("orchestrator")
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log);
        for (k, v) in &self.cfg.env {
            command.env(k, v);
        }
        self.child = Some(command.spawn().unwrap());
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if self.raw_call(json!({"op":"list"})).is_ok() {
                break;
            }
            if let Some(status) = self.child.as_mut().unwrap().try_wait().unwrap() {
                self.child = None;
                panic!("daemon exited {status}:\n{}", self.logs());
            }
            assert!(
                Instant::now() < deadline,
                "daemon startup timeout:\n{}",
                self.logs()
            );
            thread::sleep(Duration::from_millis(25));
        }
    }
    /// Kill the daemon process abruptly (workers keep running).
    pub fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
    /// Send `shutdown` and wait up to `timeout` for the process to exit.
    /// Returns the reply (or the error text) and the exit status.
    pub fn shutdown_within(&mut self, timeout: Duration) -> (Result<Value>, Option<ExitStatus>) {
        let reply = self.raw_call(json!({"op":"shutdown"}));
        let Some(mut child) = self.child.take() else {
            return (reply, None);
        };
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                return (reply, Some(status));
            }
            if Instant::now() >= deadline {
                self.child = Some(child);
                return (reply, None);
            }
            thread::sleep(Duration::from_millis(25));
        }
    }
    /// Graceful shutdown that must succeed within 10 s with exit code 0.
    pub fn stop(&mut self) -> Value {
        let (reply, status) = self.shutdown_within(Duration::from_secs(10));
        let status = status.unwrap_or_else(|| panic!("daemon did not exit:\n{}", self.logs()));
        assert!(status.success(), "daemon exit {status}:\n{}", self.logs());
        reply.unwrap()
    }
    /// Graceful stop (if running) followed by a fresh spawn.
    pub fn restart(&mut self) {
        if self.child.is_some() {
            self.stop();
        }
        self.spawn();
    }
    /// Restart with a replaced environment (for fault injection).
    pub fn restart_with_env(&mut self, env: Vec<(String, String)>) {
        if self.child.is_some() {
            self.stop();
        }
        self.cfg.env = env;
        self.spawn();
    }
    /// The daemon's `orchestrator.json`.
    pub fn discovery(&self) -> Result<Value> {
        let bytes = fs::read(self.data_dir().join("orchestrator.json"))
            .context("ORCHESTRATOR_NOT_RUNNING")?;
        Ok(serde_json::from_slice(&bytes)?)
    }
    /// Send arbitrary action JSON (`{"op": "...", ...}`) to the control endpoint
    /// over `ws::connect` + `ws::call`. A remote error is returned as `Err`.
    pub fn raw_call(&self, action: Value) -> Result<Value> {
        let discovery = self.discovery()?;
        let control = discovery["control"]
            .as_str()
            .context("INVALID_ORCHESTRATOR_DISCOVERY")?;
        let token = self.data_dir().join("orchestrator.token");
        if token.is_file() {
            // WP4.1 local tokens: the same JSON wire with an X-Tkfs-Token upgrade header.
            return token_call(control, fs::read_to_string(token)?.trim(), &action);
        }
        let mut socket = ws::connect(control)?;
        ws::call(&mut socket, &action)
    }
    /// `raw_call` that panics with the daemon and worker logs on error.
    #[track_caller]
    pub fn call(&self, action: Value) -> Value {
        match self.raw_call(action.clone()) {
            Ok(v) => v,
            Err(e) => panic!("{action} failed: {e:#}\n{}", self.logs()),
        }
    }
    /// `call`, then, if the reply says `starting`, wait (30 s) until `inspect`
    /// reports `running` and return that inspect (as `tests/orchestrator.rs` does).
    #[track_caller]
    pub fn call_ready(&self, action: Value) -> Value {
        let result = self.call(action);
        if result["starting"] == true {
            let state = result["state"].as_str().unwrap().to_owned();
            self.wait_for("folder running", Duration::from_secs(30), || {
                self.raw_call(json!({"op":"inspect","state":state}))
                    .is_ok_and(|p| p["running"] == true)
            });
            return self.call(json!({"op":"inspect","state":state}));
        }
        result
    }
    pub fn inspect(&self, state: &str) -> Value {
        self.call(json!({"op":"inspect","state":state}))
    }
    pub fn try_inspect(&self, state: &str) -> Result<Value> {
        self.raw_call(json!({"op":"inspect","state":state}))
    }
    pub fn list(&self) -> Value {
        self.call(json!({"op":"list"}))
    }
    pub fn peers(&self) -> Value {
        self.call(json!({"op":"peers"}))
    }
    pub fn endpoint(&self) -> String {
        self.discovery().unwrap()["network"]
            .as_str()
            .expect("network listener")
            .into()
    }
    pub fn installation(&self) -> String {
        self.discovery().unwrap()["installation"]
            .as_str()
            .unwrap()
            .into()
    }
    /// `reset-pairing-code` and return the new code.
    pub fn code(&self) -> String {
        self.call(json!({"op":"reset-pairing-code"}))["code"]
            .as_str()
            .unwrap()
            .into()
    }
    /// Create a headless (or default-mounted) folder and wait until it runs.
    pub fn create(&self, label: &str, headless: bool) -> Value {
        let p =
            self.call_ready(json!({"op":"create","label":label,"mount":null,"headless":headless}));
        assert_eq!(p["running"], true, "{p}");
        p
    }
    /// Create a folder mounted at `mount` (must not exist) and wait until it runs.
    pub fn create_mounted(&self, label: &str, mount: &Path) -> Value {
        let p =
            self.call_ready(json!({"op":"create","label":label,"mount":mount,"headless":false}));
        assert_eq!(p["running"], true, "{p}");
        p
    }
    pub fn pair(&self, other: &Node, code: &str) -> Value {
        self.pair_endpoint(&other.endpoint(), code, Some(self.cfg.connections))
    }
    pub fn pair_endpoint(&self, endpoint: &str, code: &str, connections: Option<usize>) -> Value {
        self.call(json!({"op":"pair","endpoint":endpoint,"code":code,"connections":connections}))
    }
    /// Receive `folder` (a create/inspect reply from `other`) headless and wait until running.
    pub fn receive(&self, other: &Node, folder: &Value) -> Value {
        self.receive_from(&other.endpoint(), folder)
    }
    pub fn receive_from(&self, endpoint: &str, folder: &Value) -> Value {
        let p = self.call_ready(json!({"op":"receive","endpoint":endpoint,
            "repo":folder["repo"],"label":folder["label"],"mount":null,"headless":true}));
        assert_eq!(p["running"], true, "{p}");
        p
    }
    pub fn start_folder(&self, state: &str) -> Value {
        self.call(json!({"op":"start","state":state}))
    }
    pub fn stop_folder(&self, state: &str) -> Value {
        self.call(json!({"op":"stop","state":state}))
    }
    /// Poll `predicate`; on timeout panic with `what` and the logs.
    #[track_caller]
    pub fn wait_for(&self, what: &str, timeout: Duration, predicate: impl FnMut() -> bool) {
        if !wait_until(timeout, predicate) {
            panic!("timeout waiting for {what}\n{}", self.logs());
        }
    }
    /// The daemon log plus the tail of every worker log (for failure reports).
    pub fn logs(&self) -> String {
        let mut out = format!(
            "--- {} ---\n{}",
            self.log_path().display(),
            tail(&self.log_path(), 16 * 1024)
        );
        if let Ok(states) = fs::read_dir(self.data_dir().join("states")) {
            for state in states.flatten() {
                let log = state.path().join("worker.log");
                if log.is_file() {
                    out.push_str(&format!(
                        "\n--- {} ---\n{}",
                        log.display(),
                        tail(&log, 8 * 1024)
                    ));
                }
            }
        }
        out
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = self.raw_call(json!({"op":"shutdown"}));
            let mut exited = false;
            for _ in 0..80 {
                if child.try_wait().ok().flatten().is_some() {
                    exited = true;
                    break;
                }
                thread::sleep(Duration::from_millis(25));
            }
            if !exited {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        // Workers outlive a killed (or adopting) daemon: never leave one running
        // in a deleted temp root. A clean worker exit removes its runtime.json.
        let live = fs::read_dir(self.data_dir().join("states"))
            .into_iter()
            .flatten()
            .flatten()
            .any(|state| state.path().join("runtime.json").exists());
        if live {
            for pid in super::metrics::find_pids(&self.root.path().to_string_lossy()) {
                kill_pid(pid);
            }
        }
    }
}

/// Forcefully terminate a process by id (test cleanup only).
pub fn kill_pid(pid: u32) {
    #[cfg(windows)]
    {
        let _ = Command::new("taskkill")
            .args(["/F", "/PID", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
    #[cfg(unix)]
    unsafe {
        libc::kill(pid as i32, libc::SIGKILL);
    }
}

fn token_call(control: &str, token: &str, action: &Value) -> Result<Value> {
    use tungstenite::{Message, client::IntoClientRequest};
    let mut request = control.into_client_request()?;
    request.headers_mut().insert("X-Tkfs-Token", token.parse()?);
    let (mut socket, _) = tungstenite::connect(request)?;
    socket.send(Message::Text(serde_json::to_string(action)?.into()))?;
    loop {
        let reply: Value = match socket.read()? {
            Message::Text(text) => serde_json::from_str(&text)?,
            Message::Binary(bytes) => serde_json::from_slice(&bytes)?,
            Message::Close(_) => bail!("WS_CLOSED"),
            _ => continue,
        };
        return match reply["ok"].as_bool() {
            Some(true) => Ok(reply["result"].clone()),
            Some(false) => {
                Err(ws::RemoteError(reply["error"].as_str().unwrap_or("").into()).into())
            }
            None => bail!("INVALID_WS_REPLY"),
        };
    }
}

/// The `runtime.json` discovery of a folder state directory.
pub fn discovery_at(state_dir: &Path) -> Result<runtime::Discovery> {
    Ok(serde_json::from_slice(&fs::read(
        state_dir.join("runtime.json"),
    )?)?)
}

/// A folder's state directory from a create/inspect/receive reply.
pub fn state_dir(folder: &Value) -> PathBuf {
    PathBuf::from(folder["directory"].as_str().expect("folder directory"))
}

pub fn state_id(folder: &Value) -> String {
    folder["state"].as_str().expect("folder state").to_owned()
}

/// Worker JSON RPC on a folder (create/inspect reply) through `runtime::rpc`.
pub fn try_rpc(folder: &Value, payload: Value) -> Result<Value> {
    rpc_at(&state_dir(folder), payload)
}

/// Worker JSON RPC on a state directory through `runtime::rpc`.
pub fn rpc_at(state_dir: &Path, payload: Value) -> Result<Value> {
    runtime::rpc(&discovery_at(state_dir)?, &id(), payload)
}

#[track_caller]
pub fn rpc(folder: &Value, payload: Value) -> Value {
    match try_rpc(folder, payload.clone()) {
        Ok(v) => v,
        Err(e) => panic!("worker rpc {payload} failed: {e:#}"),
    }
}

/// Import `bytes` as `name` through the worker's `import` op.
pub fn write_file(folder: &Value, name: &str, bytes: &[u8]) -> Value {
    rpc(
        folder,
        json!({"op":"import","path":name,"hex":hex::encode(bytes)}),
    )
}

/// Whether the folder's worker shows a live entry `name` with content `bytes`.
pub fn has_file(folder: &Value, name: &str, bytes: &[u8]) -> bool {
    has_hash(folder, name, &tkfs::core::hash(bytes))
}

pub fn has_hash(folder: &Value, name: &str, content: &str) -> bool {
    let Ok(state) = try_rpc(folder, json!({"op":"state"})) else {
        return false;
    };
    state["entries"].as_object().is_some_and(|entries| {
        entries
            .values()
            .any(|e| e["name"] == name && e["alive"] == true && e["content"] == content)
    })
}

/// Content counters from the worker `status` op (`content.complete` etc.).
pub fn content_status(folder: &Value) -> Value {
    try_rpc(folder, json!({"op":"status"}))
        .map(|s| s["content"].clone())
        .unwrap_or(Value::Null)
}

/// Whether mounts can work on this machine (WinFsp on Windows, `/dev/fuse` on Linux).
pub fn mount_supported() -> bool {
    #[cfg(windows)]
    {
        true
    }
    #[cfg(target_os = "linux")]
    {
        Path::new("/dev/fuse").exists()
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        false
    }
}

/// One `tkfs daemon --state S [args]` worker process with its stdout lines
/// (`startup_progress`, `ready`, `fatal`) collected by a reader thread.
pub struct WorkerProc {
    pub state: PathBuf,
    child: Option<Child>,
    lines: Receiver<String>,
    pub seen: Vec<Value>,
    pub spawned_at: Instant,
}
impl WorkerProc {
    /// Spawn with extra CLI args (for example `["--mount", path]`) and environment.
    pub fn spawn(state: &Path, args: &[&str], env: &[(&str, &str)]) -> Result<Self> {
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(state.join("worker.log"))?;
        let mut command = Command::new(bin());
        command
            .arg("daemon")
            .arg("--state")
            .arg(state)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(log);
        for (k, v) in env {
            command.env(k, v);
        }
        let spawned_at = Instant::now();
        let mut child = command.spawn()?;
        let stdout = child.stdout.take().unwrap();
        let (tx, lines) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        Ok(Self {
            state: state.to_owned(),
            child: Some(child),
            lines,
            seen: vec![],
            spawned_at,
        })
    }
    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(Child::id)
    }
    /// Wait for the `ready` line. Errors with the worker log tail if the
    /// process exits or prints `fatal` first, or on timeout.
    pub fn wait_ready(&mut self, timeout: Duration) -> Result<Value> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.lines.recv_timeout(Duration::from_millis(50)) {
                Ok(line) => {
                    if let Ok(value) = serde_json::from_str::<Value>(&line) {
                        self.seen.push(value.clone());
                        if value["ready"] == true {
                            return Ok(value);
                        }
                        if value["fatal"] == true {
                            bail!("WORKER_FATAL: {value}\n{}", self.log_tail());
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    let status = self.wait_exit(Duration::from_secs(5));
                    bail!("WORKER_EXITED: {status:?}\n{}", self.log_tail());
                }
            }
            if let Some(status) = self.try_exit() {
                // Drain lines printed just before exit.
                while let Ok(line) = self.lines.try_recv() {
                    if let Ok(value) = serde_json::from_str::<Value>(&line) {
                        self.seen.push(value.clone());
                        if value["ready"] == true {
                            return Ok(value);
                        }
                    }
                }
                bail!("WORKER_EXITED: {status}\n{}", self.log_tail());
            }
            if Instant::now() >= deadline {
                bail!("WORKER_READY_TIMEOUT\n{}", self.log_tail());
            }
        }
    }
    pub fn try_exit(&mut self) -> Option<ExitStatus> {
        self.child.as_mut()?.try_wait().ok().flatten()
    }
    pub fn wait_exit(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.try_exit() {
                return Some(status);
            }
            if Instant::now() >= deadline {
                return None;
            }
            thread::sleep(Duration::from_millis(20));
        }
    }
    pub fn rpc(&self, payload: Value) -> Result<Value> {
        rpc_at(&self.state, payload)
    }
    /// Graceful `stop` RPC, then wait (10 s) for exit; kill on timeout.
    pub fn stop(&mut self) -> Result<Value> {
        let reply = self.rpc(json!({"op":"stop"}));
        if self.wait_exit(Duration::from_secs(10)).is_none() {
            self.kill();
        }
        reply
    }
    pub fn kill(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
    pub fn log_tail(&self) -> String {
        tail(&self.state.join("worker.log"), 8 * 1024)
    }
}
impl Drop for WorkerProc {
    fn drop(&mut self) {
        if self.try_exit().is_none() {
            let _ = self.rpc(json!({"op":"stop"}));
            if self.wait_exit(Duration::from_secs(3)).is_none() {
                self.kill();
            }
        }
    }
}

impl Node {
    /// Persist the current listener address in `orchestrator.toml`, so restarts
    /// keep the same peer endpoint (as `tests/orchestrator.rs` does).
    pub fn pin_endpoint(&mut self) {
        let endpoint = self.endpoint();
        self.cfg.listen = Some(endpoint.trim_start_matches("ws://").to_owned());
        fs::write(self.config_path(), self.cfg.toml()).unwrap();
    }
    /// Wait for the daemon process to exit on its own (signal, shutdown).
    pub fn wait_exit(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            let status = self.child.as_mut()?.try_wait().ok().flatten();
            if let Some(status) = status {
                self.child = None;
                return Some(status);
            }
            if Instant::now() >= deadline {
                return None;
            }
            thread::sleep(Duration::from_millis(25));
        }
    }
    /// Stop `folder`, run `f` on its store in-process, then start it again and
    /// wait (`timeout`) until it runs. Returns `f`'s result.
    pub fn with_stopped_folder<T>(
        &self,
        folder: &Value,
        timeout: Duration,
        f: impl FnOnce(&mut tkfs::core::Store) -> Result<T>,
    ) -> Result<T> {
        let state = state_id(folder);
        self.raw_call(json!({"op":"stop","state":state}))?;
        let value = {
            let mut store = tkfs::core::Store::open_existing(&state_dir(folder))?;
            let value = f(&mut store)?;
            store.db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
            value
        };
        self.raw_call(json!({"op":"start","state":state}))?;
        if !wait_until(timeout, || {
            self.raw_call(json!({"op":"inspect","state":state}))
                .is_ok_and(|p| p["running"] == true)
        }) {
            bail!("FOLDER_NOT_RUNNING_AFTER_EDIT\n{}", self.logs());
        }
        Ok(value)
    }
    /// The worker process ids of `folder` (command line contains its state dir).
    pub fn worker_pids(&self, folder: &Value) -> Vec<u32> {
        super::metrics::find_pids(&state_dir(folder).to_string_lossy())
    }
}

/// Two already-synchronized replicas of a large history without replaying it:
/// `a` creates a folder (empty), `b` pairs through `endpoint` and receives it,
/// both folders stop, `build` writes the history into A's store in-process, B's
/// state becomes a copy of A's (keeping B's device id), and both start again.
/// Returns the (A, B) folder replies.
pub fn synced_replicas(
    a: &Node,
    b: &Node,
    endpoint: &str,
    label: &str,
    build: impl FnOnce(&mut tkfs::core::Store) -> Result<()>,
) -> Result<(Value, Value)> {
    let pa = a.create(label, true);
    b.pair_endpoint(endpoint, &a.code(), Some(b.cfg.connections));
    let pb = b.receive_from(endpoint, &pa);
    b.stop_folder(&state_id(&pb));
    a.stop_folder(&state_id(&pa));
    {
        let mut store = tkfs::core::Store::open_existing(&state_dir(&pa))?;
        build(&mut store)?;
        store.db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    }
    super::fixtures::clone_into(
        &state_dir(&pa),
        &state_dir(&pb),
        pb["device"].as_str().context("device")?,
    )?;
    for (node, folder) in [(a, &pa), (b, &pb)] {
        let state = state_id(folder);
        node.start_folder(&state);
        if !wait_until(Duration::from_secs(600), || {
            node.raw_call(json!({"op":"inspect","state":state}))
                .is_ok_and(|p| p["running"] == true)
        }) {
            bail!(
                "REPLICA_NOT_RUNNING
{}",
                node.logs()
            );
        }
    }
    Ok((pa, pb))
}

/// The worker address in `STATE/runtime.json`, if any.
pub fn worker_address(folder: &Value) -> Option<String> {
    discovery_at(&state_dir(folder)).ok().map(|d| d.address)
}

/// Retry `f` while it fails with a Contention code (`WORKER_BUSY`, `BUSY`).
pub fn retry_busy<T>(timeout: Duration, mut f: impl FnMut() -> Result<T>) -> Result<T> {
    let deadline = Instant::now() + timeout;
    loop {
        match f() {
            Err(e) if format!("{e:#}").contains("BUSY") && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(50));
            }
            other => return other,
        }
    }
}

/// Whether an error says the operation does not exist in this build.
pub fn is_unsupported(e: &anyhow::Error) -> bool {
    let text = format!("{e:#}");
    [
        "UNKNOWN_OPERATION",
        "UNSUPPORTED_OPERATION",
        "UNKNOWN_PUBLISHED_OPERATION",
        "NOT_IMPLEMENTED",
        "unknown variant",
    ]
    .iter()
    .any(|code| text.contains(code))
}

/// A dialer/acceptor pair of worker processes for one repository, driven the
/// way an orchestrator drives them (legacy v6 pages, or the format 7 feed).
pub struct WorkerLink<'a> {
    pub dialer: &'a WorkerProc,
    pub acceptor: &'a WorkerProc,
    pub route_d: Value,
    pub route_a: Value,
}
impl<'a> WorkerLink<'a> {
    pub fn new(
        dialer: &'a WorkerProc,
        acceptor: &'a WorkerProc,
        repo: &str,
        dialer_device: &str,
        acceptor_device: &str,
    ) -> Self {
        let route_d = json!({"peer": id(), "repo": repo, "local_replica": dialer_device,
            "remote_replica": acceptor_device});
        let route_a = json!({"peer": id(), "repo": repo, "local_replica": acceptor_device,
            "remote_replica": dialer_device});
        Self {
            dialer,
            acceptor,
            route_d,
            route_a,
        }
    }
    fn call(worker: &WorkerProc, payload: Value) -> Result<Value> {
        retry_busy(Duration::from_secs(30), || worker.rpc(payload.clone()))
    }
    /// One legacy control round (`published-offer` → `published-exchange` →
    /// `published-apply`, metadata only). Returns the apply result and the JSON
    /// bytes of the page plus the response.
    pub fn metadata_round(&self) -> Result<(Value, usize)> {
        let page = Self::call(
            self.dialer,
            json!({"op":"published-offer","route":self.route_d,"max_frame":4194304,"metadata_only":true}),
        )?;
        let sent: Vec<Value> = page["bundle"]["events"]
            .as_array()
            .map(|events| events.iter().map(|e| e["id"].clone()).collect())
            .unwrap_or_default();
        let response = Self::call(
            self.acceptor,
            json!({"op":"published-exchange","route":self.route_a,"page":page,
                "max_frame":4194304,"metadata_only":true}),
        )?;
        let bytes = page.to_string().len() + response.to_string().len();
        let result = Self::call(
            self.dialer,
            json!({"op":"published-apply","route":self.route_d,"page":response,
                "request":page["request"],"sent":sent,"max_frame":4194304,"metadata_only":true}),
        )?;
        Ok((result, bytes))
    }
    /// One legacy bulk round on `lane` (`{index, count}`): `published-bulk-offer`
    /// → `published-bulk-exchange` → `published-bulk-apply`. `inventory` carries
    /// the acceptor's last inventory between rounds (start with `null`).
    /// Returns (object bytes received by the dialer, object bytes sent, JSON bytes).
    pub fn bulk_round(&self, lane: &Value, inventory: &mut Value) -> Result<(u64, u64, usize)> {
        let page = Self::call(
            self.dialer,
            json!({"op":"published-bulk-offer","route":self.route_d,"lane":lane,"inventory":inventory}),
        )?;
        let response = Self::call(
            self.acceptor,
            json!({"op":"published-bulk-exchange","route":self.route_a,"lane":lane,"page":page}),
        )?;
        let bytes = page.to_string().len() + response.to_string().len();
        Self::call(
            self.dialer,
            json!({"op":"published-bulk-apply","route":self.route_d,"lane":lane,
                "page":response,"request":page["request"]}),
        )?;
        let parts = |v: &Value| -> u64 {
            v["object_parts"]
                .as_object()
                .map(|parts| {
                    parts
                        .values()
                        .map(|p| p["hex"].as_str().map_or(0, |h| h.len() as u64 / 2))
                        .sum()
                })
                .unwrap_or(0)
        };
        let received = parts(&response);
        let sent = parts(&page);
        let mut next = response;
        next["object_parts"] = json!({});
        *inventory = next;
        Ok((received, sent, bytes))
    }
    /// One format 7 feed round (`feed-offer` → `feed-exchange` → `feed-apply`,
    /// §6.3.5). Returns `(apply result, exchange reply, JSON bytes)`.
    pub fn feed_round(&self) -> Result<(Value, Value, usize)> {
        let request = Self::call(self.dialer, json!({"op":"feed-offer","route":self.route_d}))?;
        let exchanged = Self::call(
            self.acceptor,
            json!({"op":"feed-exchange","route":self.route_a,"request":request}),
        )?;
        let bytes = request.to_string().len() + exchanged["reply"].to_string().len();
        let applied = Self::call(
            self.dialer,
            json!({"op":"feed-apply","route":self.route_d,"request_id":request["request"],
                "reply":exchanged["reply"]}),
        )?;
        Ok((applied, exchanged, bytes))
    }
}
