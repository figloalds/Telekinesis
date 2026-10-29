//! The orchestrator daemon end to end: `tkfs start`, `tkfs manage`, real
//! workers. Headless only: never passes `--mount`. Every wait is bounded
//! (see `support::daemon`).
mod support;

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    sync::mpsc,
    time::{Duration, Instant},
};
use support::daemon::{COMMAND_BOUND, TestDaemon, run_tkfs};
use tkfs::orchestrator::{config::Config, endpoint::ControlClient, hub::SHUTDOWN_BOUND, process};

const RUNNING: Duration = Duration::from_secs(30);

/// Waits at most `bound` for `path` to be gone.
fn wait_gone(path: &Path, bound: Duration) -> Result<()> {
    let end = Instant::now() + bound;
    while path.exists() {
        ensure!(
            Instant::now() < end,
            "{} still exists after {bound:?}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}

/// `STATE/worker.json` names no live worker: absent, or adoption refuses it.
fn worker_gone(state: &Path) -> Result<bool> {
    if !state.join("worker.json").exists() {
        return Ok(true);
    }
    let (sink, _events) = mpsc::channel();
    Ok(process::adopt(state, sink)?.is_none())
}

fn error_of(result: Result<Value>) -> String {
    match result {
        Ok(value) => panic!("expected an error, got {value}"),
        Err(error) => format!("{error:#}"),
    }
}

#[test]
fn start_auto_inits_config_creates_headless_folders_and_shuts_down_cleanly() -> Result<()> {
    // The empty home gets an auto-created orchestrator.toml (checked by the
    // harness when the default port is taken and the file is rewritten).
    let d = TestDaemon::start_empty_home()?;
    let written = fs::read_to_string(&d.config)?;
    assert!(
        written.contains("format_version = 2"),
        "auto-init wrote:\n{written}"
    );
    let discovery: Value = serde_json::from_slice(&fs::read(d.data().join("orchestrator.json"))?)?;
    for key in [
        "control",
        "installation",
        "pid",
        "version",
        "listen",
        "advertise",
        "started_ms",
        "data_directory",
    ] {
        assert!(
            discovery.get(key).is_some(),
            "orchestrator.json lacks {key}"
        );
    }
    assert_eq!(discovery["pid"], d.pid());
    assert!(d.data().join("orchestrator.token").is_file());

    let created = d.manage(&["create", "Project", "--headless"])?;
    assert!(created["mount"].is_null(), "{created}");
    assert_eq!(created["label"], "Project");
    assert_eq!(created["desired_running"], true);
    let state = Path::new(created["state"].as_str().context("record.state")?).to_owned();
    assert!(state.starts_with(d.data()));
    d.wait_phase("Project", "Running", RUNNING)?;

    let list = d.manage(&["list"])?;
    let list = list.as_array().context("list is an array")?;
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["id"], created["id"]);

    // `--mount` is refused before anything is written (no host mount under
    // a force-kill supervisor until W6-P0 is closed).
    let mount = d.home().join("never-mounted");
    let mount = mount.to_str().context("mount path")?;
    let refused = error_of(d.manage(&["create", "Mounted", "--mount", mount]));
    assert!(refused.contains("MOUNT_UNSUPPORTED"), "{refused}");
    let refused = error_of(d.manage(&["configure-mount", "Project", "--mount", mount]));
    assert!(refused.contains("MOUNT_UNSUPPORTED"), "{refused}");
    let unchanged = d.manage(&["list"])?;
    assert_eq!(unchanged.as_array().map(Vec::len), Some(1), "{unchanged}");
    assert!(!Path::new(mount).exists());

    let status = d.manage(&["status"])?;
    assert_eq!(status["folders"][0]["phase"]["phase"], "Running");
    assert_eq!(status["folders"][0]["record"]["label"], "Project");
    assert_eq!(status["daemon"]["pid"], d.pid());
    assert_eq!(status["daemon"]["stopping"], false);
    assert!(status["peers"].as_array().is_some_and(Vec::is_empty));

    let inspected = d.manage(&["inspect", "Project"])?;
    assert!(
        inspected["worker"]["entries"].is_number(),
        "inspect carries the worker's status: {inspected}"
    );
    assert_eq!(inspected["record"]["id"], created["id"]);
    assert!(inspected.get("sync").is_some());
    assert!(inspected["heartbeat"].is_object() || inspected["heartbeat"].is_null());

    // The `folder` passthrough reaches the worker's control lane; other ops
    // are not forwarded.
    let mut client = ControlClient::connect(&Config::load(&d.config)?)?;
    let worker_status = client.request_read(
        "folder",
        json!({"state": "Project", "op": "status", "payload": {}}),
    )?;
    assert_eq!(worker_status["repo"], created["repo"], "{worker_status}");
    let tree = client.request_read(
        "folder",
        json!({"state": "Project", "op": "tree", "payload": {}}),
    )?;
    assert!(tree.is_object(), "{tree}");
    let checkpoint = client.request(
        "folder",
        json!({"state": "Project", "op": "checkpoint", "payload": {}}),
    )?;
    assert!(checkpoint["id"].is_string(), "{checkpoint}");
    client.request(
        "folder",
        json!({"state": "Project", "op": "fork", "payload": {"name": "side"}}),
    )?;
    client.request(
        "folder",
        json!({"state": "Project", "op": "checkout", "payload": {"branch": "side"}}),
    )?;
    let collected = client.request(
        "folder",
        json!({"state": "Project", "op": "gc", "payload": {}}),
    )?;
    assert!(collected["removed"].is_number(), "{collected}");
    let refused = format!(
        "{:#}",
        client
            .request(
                "folder",
                json!({"state": "Project", "op": "shutdown", "payload": {}})
            )
            .unwrap_err()
    );
    assert!(refused.contains("OP_NOT_ALLOWED"), "{refused}");
    drop(client);

    // Text mode prints the formatted status.
    let text = run_tkfs(
        &["-f", d.config.to_str().unwrap(), "manage", "status"],
        COMMAND_BOUND,
    )?;
    assert_eq!(text.code, Some(0), "{}", text.stderr);
    assert!(text.stdout.contains("Project"), "{}", text.stdout);
    assert!(text.stdout.contains("Running"), "{}", text.stdout);
    assert!(text.stdout.contains("headless"), "{}", text.stdout);

    let discovery_path = d.data().join("orchestrator.json");
    d.shutdown()?;
    assert!(!discovery_path.exists());
    wait_gone(&state.join("runtime.json"), Duration::from_secs(10))?;
    assert!(worker_gone(&state)?);
    Ok(())
}

#[test]
fn stop_start_retry_pause_resume_and_selectors() -> Result<()> {
    let d = TestDaemon::start()?;
    let created = d.manage(&["create", "Project", "--headless"])?;
    let id = created["id"].as_str().context("id")?.to_owned();
    d.wait_phase("Project", "Running", RUNNING)?;

    // An 8-character id prefix selects the folder.
    let stopped = d.manage(&["stop", &id[..8]])?;
    assert_eq!(stopped["outcome"], "stopped", "{stopped}");
    let inspected = d.manage(&["inspect", &id])?;
    assert_eq!(inspected["phase"]["phase"], "Stopped");
    assert_eq!(inspected["record"]["desired_running"], false);

    // desired_running=false survives a restart: the folder stays Stopped.
    let home = d.shutdown_keep_home()?;
    let d = TestDaemon::start_in(home)?;
    std::thread::sleep(Duration::from_secs(2));
    let inspected = d.manage(&["inspect", "Project"])?;
    assert_eq!(inspected["phase"]["phase"], "Stopped", "{inspected}");
    assert_eq!(inspected["record"]["desired_running"], false);
    assert!(inspected["worker"].is_null());

    // A folder passthrough needs a running worker.
    let mut client = ControlClient::connect(&Config::load(&d.config)?)?;
    let refused = format!(
        "{:#}",
        client
            .request_read(
                "folder",
                json!({"state": "Project", "op": "status", "payload": {}})
            )
            .unwrap_err()
    );
    assert!(refused.contains("FOLDER_NOT_RUNNING"), "{refused}");
    drop(client);

    d.manage(&["start", "Project"])?;
    let running = d.wait_phase("Project", "Running", RUNNING)?;
    assert_eq!(running["record"]["desired_running"], true);

    let paused = d.manage(&["pause", "Project"])?;
    assert_eq!(paused["paused"], true, "{paused}");
    assert_eq!(d.manage(&["inspect", "Project"])?["record"]["paused"], true);
    d.manage(&["resume", "Project"])?;
    assert_eq!(
        d.manage(&["inspect", "Project"])?["record"]["paused"],
        false
    );

    // configure-mount is for stopped folders only.
    let refused = error_of(d.manage(&["configure-mount", "Project"]));
    assert!(refused.contains("FOLDER_RUNNING"), "{refused}");

    // retry starts a stopped folder.
    let stopped = d.manage(&["stop", "Project"])?;
    assert_eq!(stopped["outcome"], "stopped", "{stopped}");
    d.manage(&["retry", "Project"])?;
    d.wait_phase("Project", "Running", RUNNING)?;

    // Without waiting, the orchestrator stops it in the background.
    let stopping = d.manage(&["stop", "Project", "--no-wait"])?;
    assert_eq!(stopping["outcome"], "stopping", "{stopping}");
    let stopped = d.wait_phase("Project", "Stopped", RUNNING)?;
    assert_eq!(stopped["record"]["desired_running"], false);

    // Below four characters an id prefix selects nothing.
    let refused = error_of(d.manage(&["stop", &id[..3]]));
    assert!(refused.contains("FOLDER_NOT_FOUND"), "{refused}");
    let refused = error_of(d.manage(&["inspect", "Missing"]));
    assert!(refused.contains("FOLDER_NOT_FOUND"), "{refused}");
    // A taken label is refused.
    let refused = error_of(d.manage(&["create", "Project", "--headless"]));
    assert!(refused.contains("FOLDER_LABEL_TAKEN"), "{refused}");

    d.shutdown()
}

#[test]
fn worker_limit_refuses_starts_and_a_starting_folder_stops_once_it_runs() -> Result<()> {
    let home = tempfile::tempdir()?;
    fs::write(
        home.path().join("orchestrator.toml"),
        "format_version = 2
data_directory = \"data\"
default_mount_directory = \"mounts\"
         maximum_running = 1

[network]
listen = \"127.0.0.1:0\"
",
    )?;
    let d = TestDaemon::start_in(home)?;
    d.manage(&["create", "One", "--headless"])?;
    let refused = error_of(d.manage(&["create", "Two", "--headless"]));
    assert!(refused.contains("WORKER_LIMIT"), "{refused}");
    assert_eq!(d.manage(&["list"])?.as_array().map(Vec::len), Some(1));

    // A stop that arrives while the worker is still starting waits for it to
    // run, then stops it.
    let stopped = d.manage(&["stop", "One"])?;
    assert_eq!(stopped["outcome"], "stopped", "{stopped}");
    assert_eq!(stopped["phase"]["phase"], "Stopped");

    let created = d.manage(&["create", "Two", "--headless"])?;
    assert_eq!(created["label"], "Two");
    d.wait_phase("Two", "Running", RUNNING)?;
    let refused = error_of(d.manage(&["start", "One"]));
    assert!(refused.contains("WORKER_LIMIT"), "{refused}");
    d.shutdown()
}

#[test]
fn second_orchestrator_on_same_data_directory_exits_75_and_changes_nothing() -> Result<()> {
    let d = TestDaemon::start()?;
    let discovery = d.data().join("orchestrator.json");
    let token = d.data().join("orchestrator.token");
    let (before_json, before_token) = (fs::read(&discovery)?, fs::read(&token)?);
    let config = d.config.to_str().unwrap();
    let second = run_tkfs(&["-f", config, "start"], COMMAND_BOUND)?;
    assert_eq!(second.code, Some(75), "stderr: {}", second.stderr);
    assert!(
        second.stderr.contains("ALREADY_RUNNING"),
        "stderr: {}",
        second.stderr
    );
    assert_eq!(
        fs::read(&discovery)?,
        before_json,
        "orchestrator.json changed"
    );
    assert_eq!(
        fs::read(&token)?,
        before_token,
        "orchestrator.token changed"
    );
    let status = d.manage(&["status"])?;
    assert_eq!(status["daemon"]["pid"], d.pid());
    d.shutdown()
}

#[test]
fn sigterm_runs_the_shutdown_sequence_and_forces_a_hung_worker_within_the_bound() -> Result<()> {
    let mut d = TestDaemon::start()?;
    d.manage(&["create", "Project", "--headless"])?;
    d.wait_phase("Project", "Running", RUNNING)?;
    let state = d.state_dir("Project")?;
    let state_arg = state.to_str().unwrap();
    let fault = run_tkfs(
        &[
            "--state",
            state_arg,
            "rpc",
            "inject-fault",
            r#"{"name":"shutdown.hang"}"#,
        ],
        COMMAND_BOUND,
    )?;
    assert_eq!(fault.code, Some(0), "inject-fault: {}", fault.stderr);

    let started = Instant::now();
    d.signal_term();
    let code = d.wait_exit(Duration::from_secs(45))?;
    let elapsed = started.elapsed();
    assert_eq!(code, 0, "log:\n{}", d.log_text());
    assert!(
        elapsed <= SHUTDOWN_BOUND + Duration::from_secs(2),
        "shutdown took {elapsed:?}"
    );
    let log = d.log_text();
    assert!(log.contains("FORCED_STOP"), "log:\n{log}");
    assert!(!d.data().join("orchestrator.json").exists());
    assert!(worker_gone(&state)?, "the hung worker survived its daemon");

    // The next daemon recovers the store and runs the folder again.
    let d = TestDaemon::start_in(d.into_home()?)?;
    let running = d.wait_phase("Project", "Running", RUNNING)?;
    assert!(running["worker"]["entries"].is_number(), "{running}");
    d.shutdown()
}

#[test]
fn a_worker_that_ignores_even_a_forced_shutdown_is_killed_at_the_bound() -> Result<()> {
    let mut d = TestDaemon::start()?;
    d.manage(&["create", "Project", "--headless"])?;
    d.wait_phase("Project", "Running", RUNNING)?;
    let state = d.state_dir("Project")?;
    // Every `shutdown`, the forced one included, hangs for 60 s.
    let fault = run_tkfs(
        &[
            "--state",
            state.to_str().unwrap(),
            "rpc",
            "inject-fault",
            r#"{"name":"shutdown.hang-persistent"}"#,
        ],
        COMMAND_BOUND,
    )?;
    assert_eq!(fault.code, Some(0), "inject-fault: {}", fault.stderr);

    let started = Instant::now();
    let reply = d.manage(&["shutdown"])?;
    let elapsed = started.elapsed();
    assert!(elapsed <= SHUTDOWN_BOUND, "shutdown took {elapsed:?}");
    assert_eq!(reply["stopped"], true);
    let folders = reply["folders"].as_array().context("folders")?;
    assert_eq!(folders.len(), 1, "{reply}");
    assert_eq!(folders[0]["outcome"], "killed", "{reply}");
    assert_eq!(folders[0]["label"], "Project");
    assert_eq!(d.wait_exit(Duration::from_secs(15))?, 0);
    let log = d.log_text();
    assert!(
        log.lines()
            .any(|line| line.contains("FORCED_STOP") && line.contains("killed")),
        "log:
{log}"
    );
    assert!(worker_gone(&state)?, "the worker survived the kill");
    let d = TestDaemon::start_in(d.into_home()?)?;
    d.wait_phase("Project", "Running", RUNNING)?;
    d.shutdown()
}

#[test]
fn crash_loop_trips_breaker_and_retry_clears_it() -> Result<()> {
    let d = TestDaemon::start()?;
    d.manage(&["create", "Project", "--headless"])?;
    d.wait_phase("Project", "Running", RUNNING)?;
    let state = d.state_dir("Project")?;
    let stopped = d.manage(&["stop", "Project"])?;
    assert_eq!(stopped["outcome"], "stopped", "{stopped}");
    wait_gone(&state.join("runtime.json"), Duration::from_secs(10))?;

    // A store that is not a database: every worker exits 73 (STORE).
    let aside = d.home().join("aside");
    fs::create_dir_all(&aside)?;
    for name in [
        "metadata.sqlite",
        "metadata.sqlite-wal",
        "metadata.sqlite-shm",
    ] {
        if state.join(name).exists() {
            fs::copy(state.join(name), aside.join(name))?;
            fs::remove_file(state.join(name))?;
        }
    }
    fs::write(state.join("metadata.sqlite"), vec![0u8; 1024])?;

    d.manage(&["start", "Project"])?;
    let backoff = d.wait_phase("Project", "Backoff", Duration::from_secs(10))?;
    assert!(
        backoff["phase"]["error"]
            .as_str()
            .is_some_and(|error| !error.is_empty()),
        "{backoff}"
    );
    let failed = d.wait_phase("Project", "Failed", Duration::from_secs(60))?;
    assert_eq!(failed["phase"]["code"], "CRASH_LOOP", "{failed}");
    // The breaker holds: no restart on its own.
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(
        d.manage(&["inspect", "Project"])?["phase"]["phase"],
        "Failed"
    );

    fs::remove_file(state.join("metadata.sqlite"))?;
    for name in [
        "metadata.sqlite",
        "metadata.sqlite-wal",
        "metadata.sqlite-shm",
    ] {
        if aside.join(name).exists() {
            fs::copy(aside.join(name), state.join(name))?;
        }
    }
    d.manage(&["retry", "Project"])?;
    d.wait_phase("Project", "Running", RUNNING)?;
    d.shutdown()
}
