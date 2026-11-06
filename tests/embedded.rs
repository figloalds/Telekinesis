//! The embedded host (the Android app's mode): the orchestrator and a folder's
//! worker on threads of this process, with a headless desktop orchestrator as
//! the PC. Never passes `--mount`. Every wait is bounded.
mod support;

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use support::daemon::{TestDaemon, run_tkfs};
use tempfile::TempDir;
use tkfs::{model::errors::code_of, orchestrator::embedded::Host};

const FOLDER_COMMAND: Duration = Duration::from_secs(60);
const REPLICATED: Duration = Duration::from_secs(60);

/// A host on a fresh home: no listener, a passive dialer.
fn host() -> Result<(TempDir, Arc<Host>)> {
    let home = tempfile::tempdir()?;
    let config = home.path().join("orchestrator.toml");
    fs::write(
        &config,
        "format_version = 2\ndata_directory = \"data\"\n\
         default_mount_directory = \"mounts\"\n\n[network]\n\
         listen = \"\"\nsync_profile = \"passive\"\n",
    )?;
    Ok((home, Host::start(config)))
}

/// `len` bytes of pseudo-random lowercase text: no two 64 KiB chunks alike.
fn text(len: usize, seed: u64) -> String {
    let mut state = seed | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(b'a' + (state % 26) as u8)
        })
        .collect()
}

fn folder(state: &Path, args: &[&str]) -> Result<()> {
    let state = state.to_str().context("state path is not UTF-8")?;
    let mut full = vec!["--state", state];
    full.extend_from_slice(args);
    let output = run_tkfs(&full, FOLDER_COMMAND)?;
    ensure!(
        output.code == Some(0),
        "tkfs {args:?} exited {:?}: {}",
        output.code,
        output.stderr.trim()
    );
    Ok(())
}

#[test]
fn embedded_host_pairs_receives_and_reads_through_the_engine() -> Result<()> {
    let content = text(200 * 1024, 11);
    let pc = TestDaemon::start()?;
    let shared = pc.manage(&["create", "Shared", "--headless"])?;
    let repo = shared["repo"].as_str().context("no repo")?.to_owned();
    pc.wait_phase("Shared", "Running", REPLICATED)?;
    let source: PathBuf = pc.home().join("source-a.txt");
    fs::write(&source, &content)?;
    folder(
        &pc.state_dir("Shared")?,
        &["import", "a.txt", source.to_str().unwrap()],
    )?;
    let code = pc.manage(&["reset-pairing-code"])?["code"]
        .as_str()
        .context("no code")?
        .to_owned();
    let endpoint = pc.listen_endpoint()?;

    let (_home, host) = host()?;
    let daemon = host.daemon()?;
    daemon.execute(
        "pair",
        &json!({"endpoint": endpoint, "code": code, "connections": 2}),
    )?;
    let received = daemon.execute(
        "receive",
        &json!({"endpoint": endpoint, "repo": repo, "label": "Shared", "mount": null}),
    )?;
    let id = received["id"].as_str().context("no folder id")?.to_owned();
    let engine = host.engine(&id)?;
    let synced = daemon.execute("sync-now", &json!({"state": id}))?;
    ensure!(synced["folders"][0]["error"].is_null(), "{synced}");

    let end = Instant::now() + REPLICATED;
    let token = loop {
        match engine.open_file("/a.txt", false, false, false) {
            Ok(token) => break token,
            Err(error) => {
                ensure!(Instant::now() < end, "a.txt never appeared: {error:#}");
                std::thread::sleep(Duration::from_millis(250));
            }
        }
    };
    let mut bytes = Vec::new();
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let count = engine.read(token, bytes.len() as u64, &mut buffer)?;
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
    engine.close(token)?;
    ensure!(bytes == content.as_bytes(), "{} bytes read", bytes.len());

    let began = Instant::now();
    host.stop()?;
    ensure!(
        began.elapsed() < Duration::from_secs(40),
        "stop took {:?}",
        began.elapsed()
    );
    pc.shutdown()
}

/// The provider keeps a token across a worker restart and calls whatever engine
/// serves the folder by then. That token must fail, so that the provider reopens,
/// and must not name another open file on the new engine.
#[test]
fn a_token_kept_across_a_worker_restart_reaches_no_other_file() -> Result<()> {
    let (_home, host) = host()?;
    let daemon = host.daemon()?;
    let created = daemon.execute("create", &json!({"label": "Local"}))?;
    let id = created["id"].as_str().context("no folder id")?.to_owned();
    let engine = host.engine(&id)?;
    for (path, bytes) in [("/a.txt", b"bytes of a"), ("/b.txt", b"bytes of b")] {
        let token = engine.open_file(path, true, false, true)?;
        engine.write(token, 0, bytes, false, false)?;
        engine.flush(Some(token))?;
        engine.close(token)?;
    }
    let stale = engine.open_file("/a.txt", false, false, false)?;
    // The worker's store lock goes with the last reference to its engine.
    drop(engine);

    daemon.execute("force-stop", &json!({"state": id}))?;
    daemon.execute("start", &json!({"state": id}))?;
    let engine = host.engine(&id)?;
    let reopened = (0..8)
        .map(|_| engine.open_file("/b.txt", false, false, false))
        .collect::<Result<Vec<u64>>>()?;
    ensure!(
        !reopened.contains(&stale),
        "the new engine issued {stale} again: {reopened:?}"
    );

    let mut bytes = [0u8; 16];
    let refused = engine.read(stale, 0, &mut bytes).unwrap_err();
    ensure!(code_of(&refused) == "INVALID_HANDLE", "{refused:#}");
    let refused = engine.close(stale).unwrap_err();
    ensure!(code_of(&refused) == "INVALID_HANDLE", "{refused:#}");
    for token in reopened {
        let read = engine.read(token, 0, &mut bytes)?;
        ensure!(&bytes[..read] == b"bytes of b", "{:?}", &bytes[..read]);
        engine.close(token)?;
    }
    drop(engine);
    host.stop()
}

#[test]
fn an_embedded_orchestrator_serves_no_control_endpoint() -> Result<()> {
    let (home, host) = host()?;
    let status: Value = host.daemon()?.execute("status", &json!({}))?;
    ensure!(status["daemon"]["control"].is_null(), "{status}");
    let data = home.path().join("data");
    ensure!(data.is_dir(), "no data directory");
    for name in ["orchestrator.json", "orchestrator.token"] {
        ensure!(!data.join(name).exists(), "{name} exists");
    }
    host.stop()
}
