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
use tkfs::orchestrator::{
    config::Config,
    endpoint::{ControlClient, DISCOVERY_FILE},
    hub::SHUTDOWN_BOUND,
    launcher::{self, CONSOLE_LOG, START_BOUND, Started},
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
    assert!(error.contains(CONSOLE_LOG), "{error}");
    assert!(elapsed < START_BOUND, "reported after {elapsed:?}");
    let config = Config::load(&config_path)?;
    let console = fs::read_to_string(config.data_directory.join("logs").join(CONSOLE_LOG))?;
    assert!(console.contains("LISTEN_FAILED"), "console log:\n{console}");
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
    let entries: Vec<_> = fs::read_dir(home.path())?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<std::io::Result<_>>()?;
    assert_eq!(entries, vec![std::ffi::OsString::from("file")]);
    assert_eq!(fs::read_to_string(&file)?, "not a directory");
    Ok(())
}
