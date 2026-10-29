//! Start or attach to the orchestrator of a configuration, bounded: what the
//! desktop does on startup.
//!
//! [`ensure_running`] loads the configuration (creating it when missing),
//! attaches when its orchestrator answers `status`, and otherwise spawns
//! `EXE -f CONFIG orchestrator` in the background and polls its control
//! endpoint until it answers or the bound elapses. Every probe fits what is
//! left of the bound, so an endpoint that accepts but never replies cannot
//! stretch it. The spawned orchestrator is not tied to this process: stdin
//! is null, stdout and stderr are appended to `DATA/logs/orchestrator.log`
//! (the orchestrator's own log: it recognizes the file as its stderr,
//! captures both streams into it as events and rotates it, so nothing is
//! written twice and the file stays bounded), its working directory is the
//! configuration's, and it outlives the caller.
use super::{
    config::Config,
    endpoint::{ControlClient, REPLY_TIMEOUT},
    hub::EXIT_ALREADY_RUNNING,
};
use crate::util::log::daemon_log_path;
use anyhow::{Context, Result, bail};
use serde_json::json;
use std::{
    fs::{self, OpenOptions},
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

/// How long the desktop waits for a spawned orchestrator's control endpoint.
pub const START_BOUND: Duration = Duration::from_secs(30);
/// How long a timed-out orchestrator gets to go after its kill before it is
/// reported as `STRANDED_PROCESS`.
pub const KILL_BOUND: Duration = Duration::from_secs(5);
/// Interval between control-endpoint polls.
const POLL: Duration = Duration::from_millis(50);
/// The longest one step of a probe (TCP connect, upgrade, `status` reply)
/// waits; less when less of the bound is left.
const PROBE_STEP: Duration = Duration::from_secs(2);
/// `CREATE_NO_WINDOW`: the orchestrator gets no console window (and none of
/// the caller's console's Ctrl+C).
#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

/// How [`ensure_running`] found its orchestrator.
#[derive(Debug, PartialEq)]
pub enum Started {
    /// One was already running (or another starter won the race).
    Attached,
    /// This call spawned it.
    Spawned { pid: u32 },
}

/// The configuration at `config_path` (created when missing), a client of
/// its orchestrator, and whether that orchestrator was already running or
/// spawned from `exe` here, within `bound` of the call (plus [`KILL_BOUND`]
/// when a timed-out child has to be killed). The returned client waits the
/// usual [`REPLY_TIMEOUT`] for replies.
///
/// Errors: `CONFIG_WRITE_FAILED: {path}: …; pass -f PATH or set TKFS_CONFIG` when a missing configuration
/// cannot be created (`Config::load` errors otherwise);
/// `ORCHESTRATOR_START_FAILED` when the spawned orchestrator exits with a
/// code other than [`EXIT_ALREADY_RUNNING`]; `ORCHESTRATOR_START_TIMEOUT`
/// (with `STRANDED_PROCESS` when its kill did not take within
/// [`KILL_BOUND`]) when no endpoint answers within `bound`.
pub fn ensure_running(
    config_path: &Path,
    exe: &Path,
    bound: Duration,
) -> Result<(Config, ControlClient, Started)> {
    let end = Instant::now() + bound;
    let config_path = std::path::absolute(config_path)?;
    let config = Config::load_or_init(&config_path)?;
    if let Some((client, _)) = answering(&config, end) {
        return Ok((config, client, Started::Attached));
    }
    let log = daemon_log_path(&config.data_directory);
    let mut child = Some(spawn(&config_path, exe, &log)?);
    let pid = child.as_ref().map_or(0, Child::id);
    loop {
        if let Some(running) = child.as_mut() {
            match running.try_wait() {
                Ok(None) => {}
                Ok(Some(status)) if status.code() == Some(EXIT_ALREADY_RUNNING) => {
                    // Another orchestrator holds the data directory: attach to it.
                    child = None;
                }
                Ok(Some(status)) => {
                    let code = status
                        .code()
                        .map_or_else(|| status.to_string(), |code| code.to_string());
                    bail!(
                        "ORCHESTRATOR_START_FAILED: exited with {code}; see {}",
                        log.display()
                    );
                }
                Err(error) => {
                    // It may well be alive: never returned from unkilled.
                    let stranded = !kill(running);
                    bail!(
                        "ORCHESTRATOR_START_FAILED: cannot wait for pid {pid}: {error}; see {}{}",
                        log.display(),
                        stranded_note(stranded, pid)
                    );
                }
            }
        }
        if let Some((client, serving)) = answering(&config, end) {
            // The child may have lost the lock race after the last check:
            // only its own pid in `status` makes it ours.
            let started = if child.is_some() && serving == Some(u64::from(pid)) {
                Started::Spawned { pid }
            } else {
                Started::Attached
            };
            // Dropped without waiting: the orchestrator outlives the caller.
            drop(child);
            return Ok((config, client, started));
        }
        if Instant::now() >= end {
            let stranded = child.as_mut().is_some_and(|child| !kill(child));
            bail!(
                "ORCHESTRATOR_START_TIMEOUT: no control endpoint within {bound:?}; see {}{}",
                log.display(),
                stranded_note(stranded, pid)
            );
        }
        thread::sleep(POLL.min(end.saturating_duration_since(Instant::now())));
    }
}

/// A client of `config`'s orchestrator and the pid its `status` reports,
/// when it answers before `end`. Its three waits (TCP connect, upgrade,
/// reply; no retry) share what is left until `end`.
fn answering(config: &Config, end: Instant) -> Option<(ControlClient, Option<u64>)> {
    let step = PROBE_STEP.min(end.saturating_duration_since(Instant::now()) / 3);
    if step < Duration::from_millis(1) {
        return None;
    }
    let mut client = ControlClient::connect_within(config, step).ok()?;
    let status = client.request("status", json!({})).ok()?;
    client.set_reply_timeout(REPLY_TIMEOUT);
    let pid = status["daemon"]["pid"].as_u64();
    Some((client, pid))
}

/// `exe -f config_path orchestrator`, detached from this process's stdio:
/// stdout and stderr append to `log`.
fn spawn(config_path: &Path, exe: &Path, log: &Path) -> Result<Child> {
    let logs = log.parent().context("LOG_UNAVAILABLE")?;
    fs::create_dir_all(logs).with_context(|| format!("LOG_UNAVAILABLE: {}", logs.display()))?;
    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
        .with_context(|| format!("LOG_UNAVAILABLE: {}", log.display()))?;
    let stderr = stdout.try_clone()?;
    let mut command = Command::new(exe);
    // Never the caller's: a live process pins its working directory.
    if let Some(directory) = config_path.parent() {
        command.current_dir(directory);
    }
    command
        .arg("-f")
        .arg(config_path)
        .arg("orchestrator")
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(CREATE_NO_WINDOW);
    }
    command
        .spawn()
        .with_context(|| format!("ORCHESTRATOR_SPAWN_FAILED: {}", exe.display()))
}

/// `; STRANDED_PROCESS: pid N` when `stranded`, else nothing.
fn stranded_note(stranded: bool, pid: u32) -> String {
    if stranded {
        format!("; STRANDED_PROCESS: pid {pid}")
    } else {
        String::new()
    }
}

/// Kills `child` and waits at most [`KILL_BOUND`] for it; whether it is
/// confirmed gone (a `try_wait` error is not confirmation).
fn kill(child: &mut Child) -> bool {
    let _ = child.kill();
    let end = Instant::now() + KILL_BOUND;
    loop {
        if matches!(child.try_wait(), Ok(Some(_))) {
            return true;
        }
        if Instant::now() >= end {
            return false;
        }
        thread::sleep(Duration::from_millis(20));
    }
}
