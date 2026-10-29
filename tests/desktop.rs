//! The desktop's view of the orchestrator: `orchestrator::launcher` attaches
//! to a running one or starts one in the background, bounded. Real
//! orchestrators on temporary homes; headless only, never a mount. Every
//! orchestrator a test starts is shut down by an [`Orchestrator`] guard and a
//! process that outlives it is killed and reported as `STRANDED_PROCESS`.
mod support;

use anyhow::{Context, Result, bail};
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};
use tkfs::desktop::Client;
use tkfs::orchestrator::{
    config::Config,
    endpoint::{ControlClient, DISCOVERY_FILE},
    hub::SHUTDOWN_BOUND,
    launcher::{self, START_BOUND, Started},
    process::ObservedProcess,
};

/// How long a process gets to exit after its discovery file is gone (or
/// after a kill) before it is reported stranded.
const EXIT_BOUND: Duration = Duration::from_secs(10);

fn exe() -> &'static Path {
    Path::new(env!("CARGO_BIN_EXE_tkfs"))
}

/// A home whose `orchestrator.toml` listens on `listen`.
fn home_listening(listen: &str) -> Result<(tempfile::TempDir, PathBuf)> {
    let home = tempfile::tempdir()?;
    let config = home.path().join("orchestrator.toml");
    fs::write(
        &config,
        format!(
            "format_version = 2\ndata_directory = \"data\"\n\
             default_mount_directory = \"mounts\"\n\n[network]\n\
             listen = \"{listen}\"\n"
        ),
    )?;
    Ok((home, config))
}

/// Shuts an orchestrator down (on [`Self::stop`] or drop): `shutdown`
/// through a fresh `ControlClient`, then at most `SHUTDOWN_BOUND` for
/// `DATA/orchestrator.json` to be gone and `EXIT_BOUND` for the process.
struct Orchestrator {
    config: Config,
    process: Option<ObservedProcess>,
    stopped: bool,
}

impl Orchestrator {
    fn new(config: &Config, pid: u32) -> Self {
        Self {
            config: config.clone(),
            // Held from here, so the pid cannot be recycled before the kill.
            process: ObservedProcess::open(pid).ok(),
            stopped: false,
        }
    }

    fn discovery(&self) -> PathBuf {
        self.config.data_directory.join(DISCOVERY_FILE)
    }

    /// After a `shutdown` the test itself sent: waits (bounded) for the
    /// process to go, killing and reporting it as stranded otherwise.
    fn exited(&mut self) -> Result<()> {
        self.stopped = true;
        if let Some(process) = &self.process
            && !process.wait(EXIT_BOUND)?
        {
            kill(process.pid);
            let gone = process.wait(EXIT_BOUND)?;
            bail!(
                "STRANDED_PROCESS: orchestrator pid {} outlived its shutdown (killed: {gone})",
                process.pid
            );
        }
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        self.stopped = true;
        let shutdown = ControlClient::connect(&self.config)
            .and_then(|mut client| client.request("shutdown", json!({})));
        let end = Instant::now() + SHUTDOWN_BOUND;
        while self.discovery().exists() && Instant::now() < end {
            thread::sleep(Duration::from_millis(50));
        }
        let discovery_left = self.discovery().exists();
        if let Some(process) = &self.process
            && !process.wait(EXIT_BOUND)?
        {
            kill(process.pid);
            let gone = process.wait(EXIT_BOUND)?;
            bail!(
                "STRANDED_PROCESS: orchestrator pid {} outlived its shutdown ({shutdown:?}; killed: {gone})",
                process.pid
            );
        }
        if discovery_left {
            bail!("{DISCOVERY_FILE} left behind after shutdown ({shutdown:?})");
        }
        shutdown.map(drop)
    }
}

impl Drop for Orchestrator {
    fn drop(&mut self) {
        if self.stopped {
            return;
        }
        if let Err(error) = self.stop() {
            if thread::panicking() {
                eprintln!("orchestrator cleanup: {error:#}");
            } else {
                panic!("orchestrator cleanup: {error:#}");
            }
        }
    }
}

/// Kills `pid` (held open by an `ObservedProcess`, so it is still ours).
fn kill(pid: u32) {
    #[cfg(windows)]
    let _ = std::process::Command::new("taskkill")
        .args(["/F", "/PID", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
    #[cfg(unix)]
    // SAFETY: kill(2) on a pid this test observes.
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGKILL);
    }
}

fn status_pid(client: &mut ControlClient) -> Result<u64> {
    let status = client.request_read("status", json!({}))?;
    status["daemon"]["pid"]
        .as_u64()
        .with_context(|| format!("status has no daemon.pid: {status}"))
}

#[test]
fn spawns_then_attaches_then_starts_again_after_shutdown() -> Result<()> {
    let (_home, config_path) = home_listening("127.0.0.1:0")?;

    let (config, client, started) = launcher::ensure_running(&config_path, exe(), START_BOUND)?;
    drop(client);
    let Started::Spawned { pid } = started else {
        bail!("the first ensure_running was {started:?}");
    };
    let mut first = Orchestrator::new(&config, pid);

    let (_, mut client, started) = launcher::ensure_running(&config_path, exe(), START_BOUND)?;
    assert_eq!(started, Started::Attached);
    assert_eq!(status_pid(&mut client)?, u64::from(pid));
    drop(client);

    first.stop()?;
    assert!(!config.data_directory.join(DISCOVERY_FILE).exists());
    // The spawned orchestrator's streams are its log: each event is in
    // `orchestrator.log` once, and no second log file exists.
    let logs = config.data_directory.join("logs");
    let log = fs::read_to_string(logs.join("orchestrator.log"))?;
    for code in ["ORCHESTRATOR_STARTED", "ORCHESTRATOR_STOPPED"] {
        assert_eq!(log.matches(code).count(), 1, "{code} in:\n{log}");
    }
    let files: Vec<_> = fs::read_dir(&logs)?
        .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
        .collect::<std::io::Result<_>>()?;
    assert!(
        files
            .iter()
            .all(|name| name.starts_with("orchestrator.log")),
        "{files:?}"
    );

    let (config, client, started) = launcher::ensure_running(&config_path, exe(), START_BOUND)?;
    drop(client);
    let Started::Spawned { pid: pid2 } = started else {
        bail!("the third ensure_running was {started:?}");
    };
    let mut second = Orchestrator::new(&config, pid2);
    assert_ne!(pid2, pid);
    second.stop()
}

#[test]
fn attaches_to_an_orchestrator_started_elsewhere() -> Result<()> {
    let (home, config_path) = home_listening("127.0.0.1:0")?;
    let d = support::daemon::TestDaemon::start_in(home)?;
    assert_eq!(d.config, config_path);

    let (_, mut client, started) = launcher::ensure_running(&config_path, exe(), START_BOUND)?;
    assert_eq!(started, Started::Attached);
    assert_eq!(status_pid(&mut client)?, u64::from(d.pid()));
    drop(client);
    d.shutdown()
}

#[test]
fn a_failing_orchestrator_is_reported_with_its_log() -> Result<()> {
    // Config::load does not validate `listen`; the orchestrator itself
    // fails with LISTEN_FAILED (exit 1, not 75).
    let (_home, config_path) = home_listening("256.0.0.1:1")?;
    let began = Instant::now();
    let result = launcher::ensure_running(&config_path, exe(), START_BOUND);
    let elapsed = began.elapsed();
    let error = match result {
        Ok((config, _client, started)) => {
            if let Started::Spawned { pid } = started {
                drop(Orchestrator::new(&config, pid));
            }
            bail!("an orchestrator listening on 256.0.0.1:1 started: {started:?}");
        }
        Err(error) => format!("{error:#}"),
    };
    assert!(error.starts_with("ORCHESTRATOR_START_FAILED"), "{error}");
    assert!(error.contains("orchestrator.log"), "{error}");
    assert!(elapsed < START_BOUND, "reported after {elapsed:?}");
    let config = Config::load(&config_path)?;
    let log = fs::read_to_string(config.data_directory.join("logs").join("orchestrator.log"))?;
    assert!(log.contains("LISTEN_FAILED"), "orchestrator.log:\n{log}");
    Ok(())
}

/// A loopback listener that accepts connections and never answers them,
/// until dropped.
struct Silent {
    address: String,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Silent {
    fn bind() -> Result<Self> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?.to_string();
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopping = stop.clone();
        let thread = thread::spawn(move || {
            let mut held = Vec::new();
            while !stopping.load(std::sync::atomic::Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => held.push(stream),
                    Err(_) => thread::sleep(Duration::from_millis(10)),
                }
            }
        });
        Ok(Self {
            address,
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for Silent {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[test]
fn an_endpoint_that_never_replies_does_not_stretch_the_bound() -> Result<()> {
    // DATA names an endpoint that accepts and never answers, and the
    // orchestrator lock is held, so the spawned child exits 75 and every
    // probe meets the silent endpoint until the bound.
    let (_home, config_path) = home_listening("127.0.0.1:0")?;
    let silent = Silent::bind()?;
    let data = Config::load(&config_path)?.data_directory;
    fs::create_dir_all(&data)?;
    fs::write(
        data.join(DISCOVERY_FILE),
        json!({"control": silent.address, "pid": 1}).to_string(),
    )?;
    fs::write(data.join("orchestrator.token"), "silent-token")?;
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(data.join("orchestrator.lock"))?;
    lock.try_lock()
        .map_err(|error| anyhow::anyhow!("lock: {error}"))?;

    let bound = Duration::from_secs(3);
    let began = Instant::now();
    let result = launcher::ensure_running(&config_path, exe(), bound);
    let elapsed = began.elapsed();
    let error = match result {
        Ok((config, _client, started)) => {
            if let Started::Spawned { pid } = started {
                drop(Orchestrator::new(&config, pid));
            }
            bail!("ensure_running against a silent endpoint succeeded: {started:?}");
        }
        Err(error) => format!("{error:#}"),
    };
    assert!(error.starts_with("ORCHESTRATOR_START_TIMEOUT"), "{error}");
    assert!(!error.contains("STRANDED_PROCESS"), "{error}");
    // No child to kill (it exited 75): the bound alone, plus scheduling.
    assert!(
        elapsed < bound + Duration::from_secs(1),
        "reported after {elapsed:?}: {error}"
    );
    let log = fs::read_to_string(data.join("logs").join("orchestrator.log"))?;
    assert!(log.contains("ALREADY_RUNNING"), "orchestrator.log:\n{log}");
    drop(lock);
    Ok(())
}

#[test]
fn an_unwritable_config_location_is_named_in_the_error() -> Result<()> {
    let home = tempfile::tempdir()?;
    let file = home.path().join("file");
    fs::write(&file, "not a directory")?;
    let config_path = file.join("orchestrator.toml");

    let error = match launcher::ensure_running(&config_path, exe(), START_BOUND) {
        Ok((config, _client, started)) => {
            if let Started::Spawned { pid } = started {
                drop(Orchestrator::new(&config, pid));
            }
            bail!("ensure_running under a regular file succeeded: {started:?}");
        }
        Err(error) => format!("{error:#}"),
    };
    assert!(error.starts_with("CONFIG_WRITE_FAILED"), "{error}");
    assert!(
        error.contains(&config_path.display().to_string()),
        "{error}"
    );
    assert!(error.contains("pass -f PATH or set TKFS_CONFIG"), "{error}");
    let entries: Vec<_> = fs::read_dir(home.path())?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<std::io::Result<_>>()?;
    assert_eq!(entries, vec![std::ffi::OsString::from("file")]);
    assert_eq!(fs::read_to_string(&file)?, "not a directory");
    Ok(())
}

/// A client on a fresh home, with the orchestrator it spawned guarded.
fn open_client() -> Result<(tempfile::TempDir, Client, Orchestrator)> {
    let (home, config_path) = home_listening("127.0.0.1:0")?;
    let (client, started) = Client::open(&config_path, exe())?;
    let Started::Spawned { pid } = started else {
        bail!("Client::open on a fresh home was {started:?}");
    };
    let guard = Orchestrator::new(&client.config, pid);
    Ok((home, client, guard))
}

#[test]
fn the_client_creates_a_headless_folder_and_runs_folder_ops() -> Result<()> {
    let (_home, mut client, mut guard) = open_client()?;
    let created = client.act("create", json!({"label": "Project", "mount": null}))?;
    let state = created["id"].as_str().context("created.id")?.to_owned();

    let end = Instant::now() + Duration::from_secs(30);
    loop {
        let status = client.status()?;
        if status["folders"][0]["phase"]["phase"] == "Running" {
            break;
        }
        if Instant::now() >= end {
            bail!("the folder was not Running within 30 s: {status}");
        }
        thread::sleep(Duration::from_millis(100));
    }

    let checkpoint = client.folder_op(&state, "checkpoint", json!({}))?;
    assert!(checkpoint["id"].is_string(), "{checkpoint}");
    drop(client);
    guard.stop()
}

// Windows only: elsewhere an existing directory is a valid mount point.
#[cfg(windows)]
#[test]
fn a_mount_request_reaches_the_orchestrator_and_an_occupied_point_is_refused() -> Result<()> {
    let (home, mut client, mut guard) = open_client()?;
    // An existing directory: refused before anything is created, so no
    // worker starts and nothing mounts (mounted runs belong in the VM).
    let occupied = home.path().join("m");
    fs::create_dir(&occupied)?;
    let error = client
        .act("create", json!({"label": "M", "mount": occupied}))
        .unwrap_err();
    assert!(
        error.to_string().starts_with("MOUNT_POINT_EXISTS"),
        "{error:#}"
    );
    let status = client.status()?;
    assert_eq!(status["folders"], json!([]), "{status}");
    drop(client);
    guard.stop()
}

#[test]
fn status_after_shutdown_reports_not_running() -> Result<()> {
    let (_home, mut client, mut guard) = open_client()?;
    let discovery = guard.discovery();
    // A second client, taken while the orchestrator is up, works.
    let mut second = client.connect_again()?;
    assert!(second.status()?["daemon"]["pid"].is_u64());
    drop(second);

    client.act("shutdown", json!({}))?;
    let end = Instant::now() + SHUTDOWN_BOUND;
    while discovery.exists() && Instant::now() < end {
        thread::sleep(Duration::from_millis(50));
    }
    assert!(!discovery.exists(), "{DISCOVERY_FILE} left behind");
    guard.exited()?;

    let error = client
        .connect_again()
        .err()
        .context("connect_again succeeded")?;
    assert!(
        error.to_string().contains("ORCHESTRATOR_NOT_RUNNING"),
        "{error:#}"
    );
    assert!(client.status().is_err());
    Ok(())
}
