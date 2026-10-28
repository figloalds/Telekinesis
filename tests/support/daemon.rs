//! `TestDaemon`: a real `tkfs start` orchestrator in a temporary home, driven
//! through `tkfs manage --json`. Every wait is bounded. A daemon still alive
//! when the harness lets go of it is killed and reported as
//! `STRANDED_PROCESS`; so is a worker left running in the data directory
//! once the home is dropped. Headless only: never passes `--mount`.
use anyhow::{Context, Result, anyhow, bail};
use serde_json::Value;
use std::{
    fs,
    io::Read,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::{Duration, Instant},
};
use tempfile::TempDir;

/// How long `start` waits for `DATA/orchestrator.json`.
pub const START_BOUND: Duration = Duration::from_secs(30);
/// How long one `tkfs` command (`manage`, `rpc`, a second `start`) may run.
pub const COMMAND_BOUND: Duration = Duration::from_secs(60);
/// How long a killed process gets to go before it is reported stranded.
const KILL_BOUND: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(200);

pub struct TestDaemon {
    home: Option<TempDir>,
    pub config: PathBuf,
    child: Option<Child>,
    pub log: PathBuf,
}

/// The output of one bounded `tkfs` command.
pub struct Output {
    pub code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

/// `tkfs ARGS…` with no stdin, bounded by `bound`: killed (and an error)
/// when it outlives it.
pub fn run_tkfs(args: &[&str], bound: Duration) -> Result<Output> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_tkfs"))
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove("TKFS_FAKE_NOW_MS")
        .spawn()
        .context("spawn tkfs")?;
    let pipe = |mut reader: Box<dyn Read + Send>| {
        thread::spawn(move || {
            let mut text = String::new();
            let _ = reader.read_to_string(&mut text);
            text
        })
    };
    let stdout = pipe(Box::new(child.stdout.take().unwrap()));
    let stderr = pipe(Box::new(child.stderr.take().unwrap()));
    let end = Instant::now() + bound;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= end {
            let _ = child.kill();
            let gone = wait_child(&mut child, KILL_BOUND);
            bail!(
                "tkfs {args:?} did not exit within {bound:?}{}",
                if gone {
                    String::new()
                } else {
                    format!("; STRANDED_PROCESS: pid {}", child.id())
                }
            );
        }
        thread::sleep(Duration::from_millis(20));
    };
    Ok(Output {
        code: status.code(),
        stdout: stdout.join().unwrap_or_default(),
        stderr: stderr.join().unwrap_or_default(),
    })
}

/// Waits at most `bound` for `child` to exit; whether it did.
fn wait_child(child: &mut Child, bound: Duration) -> bool {
    let end = Instant::now() + bound;
    while Instant::now() < end {
        if !matches!(child.try_wait(), Ok(None)) {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    !matches!(child.try_wait(), Ok(None))
}

impl TestDaemon {
    /// A daemon on a fresh home whose `orchestrator.toml` listens on
    /// `127.0.0.1:0`.
    pub fn start() -> Result<Self> {
        Self::start_with("")
    }

    /// [`Self::start`] with extra `[network]` lines.
    pub fn start_with(network_toml: &str) -> Result<Self> {
        let home = tempfile::tempdir()?;
        fs::write(
            home.path().join("orchestrator.toml"),
            format!(
                "format_version = 2\ndata_directory = \"data\"\n\
                 default_mount_directory = \"mounts\"\n\n[network]\n\
                 listen = \"127.0.0.1:0\"\n{network_toml}\n"
            ),
        )?;
        Self::start_in(home)
    }

    /// A daemon on `home` as it is: with no `orchestrator.toml` the daemon
    /// creates one (auto-init); an existing one (and catalog) is reused.
    pub fn start_in(home: TempDir) -> Result<Self> {
        static LOGS: AtomicUsize = AtomicUsize::new(0);
        let config = home.path().join("orchestrator.toml");
        let log = home.path().join(format!(
            "daemon-{}.log",
            LOGS.fetch_add(1, Ordering::Relaxed)
        ));
        let file = fs::File::create(&log)?;
        let child = Command::new(env!("CARGO_BIN_EXE_tkfs"))
            .arg("-f")
            .arg(&config)
            .arg("start")
            .stdin(Stdio::null())
            .stdout(file.try_clone()?)
            .stderr(file)
            .env_remove("TKFS_FAKE_NOW_MS")
            .env_remove("TKFS_UNSUPERVISED_EXIT_S")
            .spawn()
            .context("spawn tkfs start")?;
        let mut daemon = Self {
            home: Some(home),
            config,
            child: Some(child),
            log,
        };
        let discovery = daemon.data().join("orchestrator.json");
        let end = Instant::now() + START_BOUND;
        loop {
            if discovery.exists() {
                return Ok(daemon);
            }
            if let Some(status) = daemon.child.as_mut().unwrap().try_wait()? {
                bail!(
                    "the daemon exited ({status}) before writing orchestrator.json; log:\n{}",
                    daemon.log_text()
                );
            }
            if Instant::now() >= end {
                bail!(
                    "no orchestrator.json within {START_BOUND:?}; log:\n{}",
                    daemon.log_text()
                );
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    pub fn home(&self) -> &Path {
        self.home.as_ref().unwrap().path()
    }

    /// `DATA` (`data_directory = "data"`).
    pub fn data(&self) -> PathBuf {
        self.home().join("data")
    }

    pub fn log_text(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    pub fn pid(&self) -> u32 {
        self.child.as_ref().unwrap().id()
    }

    /// `tkfs -f CONFIG manage --json ARGS…`: its reply, or `Err(stderr)` on a
    /// non-zero exit.
    pub fn manage(&self, args: &[&str]) -> Result<Value> {
        let config = self.config.to_str().unwrap();
        let mut full = vec!["-f", config, "manage", "--json"];
        full.extend_from_slice(args);
        let output = run_tkfs(&full, COMMAND_BOUND)?;
        if output.code != Some(0) {
            return Err(anyhow!("{}", output.stderr.trim().to_owned()));
        }
        serde_json::from_str(&output.stdout)
            .with_context(|| format!("manage {args:?} printed {:?}", output.stdout))
    }

    /// Polls `manage inspect SELECTOR` until its phase is `phase`.
    pub fn wait_phase(&self, selector: &str, phase: &str, timeout: Duration) -> Result<Value> {
        let end = Instant::now() + timeout;
        loop {
            let last = match self.manage(&["inspect", selector]) {
                Ok(inspected) if inspected["phase"]["phase"] == phase => return Ok(inspected),
                Ok(inspected) => inspected,
                Err(error) => Value::String(format!("{error:#}")),
            };
            if Instant::now() >= end {
                bail!(
                    "{selector} not {phase} within {timeout:?}; last inspect: {last}\nlog:\n{}",
                    self.log_text()
                );
            }
            thread::sleep(POLL);
        }
    }

    /// `ws://` + `DATA/orchestrator.json["listen"]` (Task 11).
    pub fn listen_endpoint(&self) -> Result<String> {
        let discovery: Value =
            serde_json::from_slice(&fs::read(self.data().join("orchestrator.json"))?)?;
        let listen = discovery["listen"]
            .as_str()
            .context("orchestrator.json has no listen address")?;
        Ok(format!("ws://{listen}"))
    }

    /// Polls `manage peers` until the peer at `endpoint` has `count` lanes in
    /// `state` (Task 11).
    pub fn wait_lanes(
        &self,
        endpoint: &str,
        count: usize,
        state: &str,
        timeout: Duration,
    ) -> Result<Value> {
        let end = Instant::now() + timeout;
        let mut last = Value::Null;
        loop {
            if let Ok(peers) = self.manage(&["peers"]) {
                let lanes = peers
                    .as_array()
                    .into_iter()
                    .flatten()
                    .find(|peer| peer["endpoint"] == endpoint)
                    .map(|peer| {
                        peer["lanes"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter(|lane| lane["state"] == state)
                            .count()
                    })
                    .unwrap_or(0);
                if lanes >= count {
                    return Ok(peers);
                }
                last = peers;
            }
            if Instant::now() >= end {
                bail!("{count} lanes {state} to {endpoint} not within {timeout:?}; last: {last}");
            }
            thread::sleep(POLL);
        }
    }

    /// The folder's state directory, from `manage inspect`.
    pub fn state_dir(&self, selector: &str) -> Result<PathBuf> {
        let inspected = self.manage(&["inspect", selector])?;
        Ok(PathBuf::from(
            inspected["record"]["state"]
                .as_str()
                .context("inspect has no record.state")?,
        ))
    }

    /// SIGTERM on Unix; `manage shutdown` (in the background) on Windows.
    pub fn signal_term(&self) {
        #[cfg(unix)]
        {
            // SAFETY: kill(2) on the pid of a child this harness owns.
            let sent = unsafe { libc::kill(self.pid() as libc::pid_t, libc::SIGTERM) };
            assert_eq!(
                sent,
                0,
                "kill(SIGTERM): {}",
                std::io::Error::last_os_error()
            );
        }
        #[cfg(not(unix))]
        {
            let config = self.config.to_str().unwrap().to_owned();
            // Bounded by COMMAND_BOUND; its result is the daemon's exit.
            thread::spawn(move || {
                let _ = run_tkfs(&["-f", &config, "manage", "shutdown"], COMMAND_BOUND);
            });
        }
    }

    /// Waits at most `timeout` for the daemon's exit code; kills it and
    /// fails with `STRANDED_PROCESS` otherwise.
    pub fn wait_exit(&mut self, timeout: Duration) -> Result<i32> {
        let child = self
            .child
            .as_mut()
            .context("the daemon was already reaped")?;
        let end = Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait()? {
                self.child = None;
                return status
                    .code()
                    .with_context(|| format!("the daemon ended without an exit code: {status}"));
            }
            if Instant::now() >= end {
                let _ = child.kill();
                let gone = wait_child(child, KILL_BOUND);
                let pid = child.id();
                if gone {
                    self.child = None;
                }
                bail!(
                    "STRANDED_PROCESS: daemon pid {pid} did not exit within {timeout:?} (killed: {gone}); log:\n{}",
                    self.log_text()
                );
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    /// `manage shutdown`, then exit 0 within 60 s with `orchestrator.json`
    /// removed.
    pub fn shutdown(self) -> Result<()> {
        self.shutdown_keep_home().map(drop)
    }

    /// [`Self::shutdown`], handing the home over for another daemon.
    pub fn shutdown_keep_home(mut self) -> Result<TempDir> {
        let reply = self.manage(&["shutdown"])?;
        if reply["stopped"] != true {
            bail!("shutdown reply {reply}");
        }
        let code = self.wait_exit(COMMAND_BOUND)?;
        if code != 0 {
            bail!("daemon exit code {code}; log:\n{}", self.log_text());
        }
        if self.data().join("orchestrator.json").exists() {
            bail!("orchestrator.json left behind after shutdown");
        }
        Ok(self.home.take().unwrap())
    }

    /// The home of a daemon that has exited (see [`Self::wait_exit`]).
    pub fn into_home(mut self) -> Result<TempDir> {
        if self.child.is_some() {
            bail!("the daemon is still running");
        }
        Ok(self.home.take().unwrap())
    }
}

impl Drop for TestDaemon {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut()
            && matches!(child.try_wait(), Ok(None))
        {
            let _ = child.kill();
            if !wait_child(child, KILL_BOUND) {
                eprintln!("STRANDED_PROCESS: daemon pid {}", child.id());
            }
        }
        // Workers outlive their daemon by design: kill the ones this home's
        // data directory still names, and report them.
        let Some(home) = &self.home else {
            return;
        };
        let Ok(folders) = fs::read_dir(home.path().join("data")) else {
            return;
        };
        for folder in folders.flatten() {
            let record = folder.path().join("worker.json");
            let Ok(bytes) = fs::read(&record) else {
                continue;
            };
            let Ok(record) =
                serde_json::from_slice::<tkfs::orchestrator::worker::WorkerRecord>(&bytes)
            else {
                continue;
            };
            if let Ok(true) = tkfs::orchestrator::process::kill_verified(&record) {
                eprintln!(
                    "STRANDED_PROCESS: worker pid {} of {} was still running; killed",
                    record.pid,
                    folder.path().display()
                );
            }
        }
    }
}
