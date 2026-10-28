//! Two real orchestrators on loopback: pairing, peer lanes, pairing-code
//! resets, remote listings, `receive` with metadata and object sync, pause,
//! clock offsets and an invalid clock. Headless only: never passes `--mount`. Every
//! wait is bounded (see `support::daemon`).
mod support;

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use support::daemon::{TestDaemon, run_tkfs};

const LANES_READY: Duration = Duration::from_secs(30);
const LANES_REVOKED: Duration = Duration::from_secs(15);

fn error_of(result: Result<Value>) -> String {
    match result {
        Ok(value) => panic!("expected an error, got {value}"),
        Err(error) => format!("{error:#}"),
    }
}

/// `peers` entry for `endpoint`.
fn peer_at<'a>(peers: &'a Value, endpoint: &str) -> Result<&'a Value> {
    peers
        .as_array()
        .into_iter()
        .flatten()
        .find(|peer| peer["endpoint"] == endpoint)
        .with_context(|| format!("no peer at {endpoint} in {peers}"))
}

fn installation(daemon: &TestDaemon) -> Result<String> {
    Ok(daemon.manage(&["status"])?["daemon"]["installation"]
        .as_str()
        .context("status has no installation")?
        .to_owned())
}

#[test]
fn pair_opens_control_and_bulk_lanes_and_reset_code_revokes() -> Result<()> {
    let a = TestDaemon::start()?;
    let b = TestDaemon::start()?;
    let code = b.manage(&["reset-pairing-code"])?["code"]
        .as_str()
        .context("reset-pairing-code gave no code")?
        .to_string();
    assert_eq!(code.len(), 64, "{code}");
    assert!(code.chars().all(|c| c.is_ascii_hexdigit()), "{code}");
    let endpoint_b = b.listen_endpoint()?;

    // No code on stdin and no --code: refused before dialing.
    let missing = error_of(a.manage(&["pair", &endpoint_b]));
    assert!(missing.contains("PAIRING_CODE_REQUIRED"), "{missing}");

    let paired = a.manage(&["pair", &endpoint_b, "--code", &code, "--connections", "2"])?;
    assert_eq!(paired["installation"], installation(&b)?, "{paired}");

    // Pool 2: lane 0 control, lane 1 bulk, both Ready.
    let peers = a.wait_lanes(&endpoint_b, 2, "Ready", LANES_READY)?;
    let peer = peer_at(&peers, &endpoint_b)?;
    assert_eq!(peer["installation"], paired["installation"], "{peer}");
    let lanes = peer["lanes"].as_array().context("no lanes")?;
    assert_eq!(lanes.len(), 2, "{peer}");
    assert_eq!(lanes[0]["role"], "control", "{peer}");
    assert_eq!(lanes[1]["role"], "bulk", "{peer}");
    assert!(
        lanes[0]["health"]["since_ms"].as_u64().unwrap_or(0) > 0,
        "{peer}"
    );
    assert_eq!(
        a.manage(&["status"])?["peers"].as_array().map(Vec::len),
        Some(1)
    );

    // B sees A as paired inbound, with its live sessions.
    let peers_b = b.manage(&["peers"])?;
    let inbound = peers_b.as_array().context("peers is not a list")?;
    assert_eq!(inbound.len(), 1, "{peers_b}");
    assert_eq!(inbound[0]["installation"], installation(&a)?, "{peers_b}");
    assert!(inbound[0]["endpoint"].is_null(), "{peers_b}");

    // A reset on B revokes A: its lanes fall to Unauthorized and stay there.
    let rotated = b.manage(&["reset-pairing-code"])?;
    assert_ne!(rotated["code"], code.as_str());
    a.wait_lanes(&endpoint_b, 2, "Unauthorized", LANES_REVOKED)?;
    let peers_b = b.manage(&["peers"])?;
    assert_eq!(peers_b.as_array().map(Vec::len), Some(0), "{peers_b}");
    std::thread::sleep(Duration::from_secs(2));
    let peers = a.manage(&["peers"])?;
    ensure!(
        all_lanes(&peers, "Unauthorized"),
        "a lane left Unauthorized: {peers}"
    );

    // Re-pairing with the new code (A still holds B's old key) enrolls a
    // fresh credential and brings the lanes back.
    let new_code = rotated["code"].as_str().context("no rotated code")?;
    a.manage(&[
        "pair",
        &endpoint_b,
        "--code",
        new_code,
        "--connections",
        "2",
    ])?;
    a.wait_lanes(&endpoint_b, 2, "Ready", LANES_READY)?;

    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

#[test]
fn wrong_code_is_refused_and_nothing_is_saved() -> Result<()> {
    let a = TestDaemon::start()?;
    let b = TestDaemon::start()?;
    let endpoint_b = b.listen_endpoint()?;
    let zeros = "0".repeat(64);
    let refused = error_of(a.manage(&["pair", &endpoint_b, "--code", &zeros]));
    assert!(
        refused.contains("ACCESS_DENIED") || refused.contains("PAIRING_FAILED"),
        "{refused}"
    );
    assert_eq!(a.manage(&["peers"])?.as_array().map(Vec::len), Some(0));
    assert_eq!(b.manage(&["peers"])?.as_array().map(Vec::len), Some(0));
    // Nothing remembered: a remote listing needs a pairing.
    let unpaired = error_of(a.manage(&["remote-list", &endpoint_b]));
    assert!(unpaired.contains("PAIRING_CODE_REQUIRED"), "{unpaired}");
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

#[test]
fn remote_list_shows_published_folders() -> Result<()> {
    let a = TestDaemon::start()?;
    let b = TestDaemon::start()?;
    let shared = b.manage(&["create", "Shared", "--headless"])?;
    let repo = shared["repo"].as_str().context("create gave no repo")?;
    let code = b.manage(&["reset-pairing-code"])?["code"]
        .as_str()
        .context("no code")?
        .to_string();
    let endpoint_b = b.listen_endpoint()?;
    a.manage(&["pair", &endpoint_b, "--code", &code])?;
    let listed = a.manage(&["remote-list", &endpoint_b])?;
    let folders = listed.as_array().context("remote-list is not a list")?;
    assert_eq!(folders.len(), 1, "{listed}");
    assert_eq!(folders[0]["repo"], repo, "{listed}");
    assert_eq!(folders[0]["label"], "Shared", "{listed}");

    // Unpair forgets B and closes the lanes; B forgets A too (peer.revoke).
    let installation_b = installation(&b)?;
    a.manage(&["unpair", &installation_b])?;
    assert_eq!(a.manage(&["peers"])?.as_array().map(Vec::len), Some(0));
    assert_eq!(b.manage(&["peers"])?.as_array().map(Vec::len), Some(0));
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

/// The network's `PEER_NETWORK_STOPPED` line in a daemon log.
fn network_stopped(log: &str) -> Result<Value> {
    log.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|line| line["code"] == "PEER_NETWORK_STOPPED")
        .with_context(|| {
            format!(
                "no PEER_NETWORK_STOPPED in the log:
{log}"
            )
        })
}

#[test]
fn shutdown_closes_peer_lanes_and_sessions_first_and_promptly() -> Result<()> {
    let a = TestDaemon::start()?;
    let b = TestDaemon::start()?;
    let code = b.manage(&["reset-pairing-code"])?["code"]
        .as_str()
        .context("no code")?
        .to_string();
    let endpoint_b = b.listen_endpoint()?;
    // Default pool: `connections = 4` → 1 control + 3 bulk lanes.
    a.manage(&["pair", &endpoint_b, "--code", &code])?;
    let peers = a.wait_lanes(&endpoint_b, 4, "Ready", LANES_READY)?;
    assert_eq!(
        peer_at(&peers, &endpoint_b)?["lanes"]
            .as_array()
            .map(Vec::len),
        Some(4)
    );
    assert_eq!(b.manage(&["peers"])?[0]["inbound"], 4);

    // B stops with four inbound sessions open.
    let log_b = b.log.clone();
    let began = Instant::now();
    let home_b = b.shutdown_keep_home()?;
    let took = began.elapsed();
    let stopped = network_stopped(&fs::read_to_string(&log_b)?)?;
    drop(home_b);
    assert_eq!(stopped["inbound"], 4, "{stopped}");
    assert!(
        stopped["elapsed_ms"].as_u64().unwrap_or(u64::MAX) <= 2000,
        "{stopped}"
    );
    assert!(
        took < Duration::from_secs(15),
        "B took {took:?} to shut down"
    );

    // A notices and backs off; its own shutdown stops its lane threads.
    a.wait_lanes(&endpoint_b, 4, "Backoff", LANES_REVOKED)?;
    let log_a = a.log.clone();
    let home_a = a.shutdown_keep_home()?;
    let stopped = network_stopped(&fs::read_to_string(&log_a)?)?;
    drop(home_a);
    assert_eq!(stopped["peers"], 1, "{stopped}");
    assert_eq!(stopped["lane_threads_left"], 0, "{stopped}");
    Ok(())
}

/// Every lane of every peer entry in `peers` is in `state`.
fn all_lanes(peers: &Value, state: &str) -> bool {
    peers.as_array().into_iter().flatten().all(|peer| {
        peer["lanes"]
            .as_array()
            .into_iter()
            .flatten()
            .all(|lane| lane["state"] == state)
    })
}

/// A loopback address with a port that was free a moment ago.
fn free_address() -> Result<String> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.to_string())
}

/// `daemon`'s `peers`: exactly one entry, for `other` at `endpoint`, with two
/// lanes all Ready and two inbound sessions from it (its lanes to us).
fn check_mutual(daemon: &TestDaemon, other: &str, endpoint: &str) -> Result<()> {
    let peers = daemon.manage(&["peers"])?;
    let list = peers.as_array().context("peers is not a list")?;
    ensure!(list.len() == 1, "{peers}");
    ensure!(list[0]["installation"] == other, "{peers}");
    ensure!(list[0]["endpoint"] == endpoint, "{peers}");
    ensure!(
        list[0]["lanes"].as_array().map(Vec::len) == Some(2),
        "{peers}"
    );
    ensure!(all_lanes(&peers, "Ready"), "a lane left Ready: {peers}");
    ensure!(list[0]["inbound"] == 2, "{peers}");
    Ok(())
}

#[test]
fn mutual_pairing_keeps_both_directions_ready() -> Result<()> {
    let a = TestDaemon::start()?;
    // B on a fixed port, so A's lanes find it again after B restarts.
    let b = TestDaemon::start_listening(&free_address()?, "")?;
    let code_a = a.manage(&["reset-pairing-code"])?["code"]
        .as_str()
        .context("no code")?
        .to_string();
    let code_b = b.manage(&["reset-pairing-code"])?["code"]
        .as_str()
        .context("no code")?
        .to_string();
    let (endpoint_a, endpoint_b) = (a.listen_endpoint()?, b.listen_endpoint()?);
    let (installation_a, installation_b) = (installation(&a)?, installation(&b)?);

    a.manage(&["pair", &endpoint_b, "--code", &code_b, "--connections", "2"])?;
    a.wait_lanes(&endpoint_b, 2, "Ready", LANES_READY)?;
    b.manage(&["pair", &endpoint_a, "--code", &code_a, "--connections", "2"])?;
    b.wait_lanes(&endpoint_a, 2, "Ready", LANES_READY)?;
    a.wait_lanes(&endpoint_b, 2, "Ready", LANES_READY)?;

    // Both directions stay up.
    std::thread::sleep(Duration::from_secs(3));
    check_mutual(&a, &installation_b, &endpoint_b)?;
    check_mutual(&b, &installation_a, &endpoint_a)?;

    // B restarts: A's lanes redial with the credential they hold, B's lanes
    // come back from its catalog. Both directions are Ready again.
    let b = TestDaemon::start_in(b.shutdown_keep_home()?)?;
    let end = Instant::now() + LANES_READY;
    while let Err(error) = check_mutual(&a, &installation_b, &endpoint_b)
        .and_then(|()| check_mutual(&b, &installation_a, &endpoint_a))
    {
        ensure!(
            Instant::now() < end,
            "not mutually Ready again within {LANES_READY:?}: {error:#}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

/// How long a replicated file may take to appear (brief: 60 s).
const REPLICATED: Duration = Duration::from_secs(60);
/// Bound of one folder CLI command against a worker.
const FOLDER_COMMAND: Duration = Duration::from_secs(30);

/// `tkfs --state STATE ARGS…`: its stdout, or an error with its stderr.
fn folder(state: &Path, args: &[&str]) -> Result<String> {
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
    Ok(output.stdout)
}

/// `len` bytes of pseudo-random lowercase text: no two 64 KiB chunks alike,
/// and the raw output of `cat` stays valid UTF-8.
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

/// Polls `tkfs --state STATE cat PATH` until it prints `expected`.
fn wait_file(state: &Path, path: &str, expected: &str, bound: Duration) -> Result<()> {
    let end = Instant::now() + bound;
    loop {
        let last = match folder(state, &["cat", path]) {
            Ok(bytes) if bytes == expected => return Ok(()),
            Ok(bytes) => format!("{} bytes, not the {} expected", bytes.len(), expected.len()),
            Err(error) => format!("{error:#}"),
        };
        ensure!(
            Instant::now() < end,
            "{path} not replicated within {bound:?}; last cat: {last}"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Two daemons sharing a folder (see [`shared_pair`]).
struct Shared {
    a: TestDaemon,
    b: TestDaemon,
    endpoint_b: String,
    state_a: PathBuf,
    state_b: PathBuf,
}

/// B with a running headless folder "Shared" holding `files`, and A paired
/// with B (default pool, lanes Ready) and receiving it, Running.
fn shared_pair(files: &[(&str, &str)]) -> Result<Shared> {
    let a = TestDaemon::start()?;
    let b = TestDaemon::start()?;
    let shared = b.manage(&["create", "Shared", "--headless"])?;
    let repo = shared["repo"]
        .as_str()
        .context("create gave no repo")?
        .to_owned();
    b.wait_phase("Shared", "Running", REPLICATED)?;
    let state_b = b.state_dir("Shared")?;
    for (path, content) in files {
        import(&b, &state_b, path, content)?;
    }
    let code = b.manage(&["reset-pairing-code"])?["code"]
        .as_str()
        .context("no code")?
        .to_string();
    let endpoint_b = b.listen_endpoint()?;
    a.manage(&["pair", &endpoint_b, "--code", &code])?;
    a.wait_lanes(&endpoint_b, 4, "Ready", LANES_READY)?;
    let received = a.manage(&["receive", &endpoint_b, &repo, "Shared", "--headless"])?;
    ensure!(received["repo"] == repo.as_str(), "{received}");
    a.wait_phase("Shared", "Running", REPLICATED)?;
    let state_a = a.state_dir("Shared")?;
    Ok(Shared {
        a,
        b,
        endpoint_b,
        state_a,
        state_b,
    })
}

/// Imports `content` as `path` into the running folder at `state`.
fn import(daemon: &TestDaemon, state: &Path, path: &str, content: &str) -> Result<()> {
    let source = daemon.home().join(format!("source-{path}"));
    fs::write(&source, content)?;
    folder(state, &["import", path, source.to_str().unwrap()]).map(drop)
}

#[test]
fn receive_replicates_metadata_and_objects_across_two_orchestrators() -> Result<()> {
    // 2.5 MiB: a manifest and 40 chunks, more than one object batch.
    let notes = text(5 * 512 * 1024, 7);
    let Shared {
        a,
        b,
        endpoint_b,
        state_a,
        ..
    } = shared_pair(&[("notes.txt", &notes)])?;
    // Metadata first, then the object pump pulls every object unasked: no
    // read on A creates demand before B is gone.
    wait_until(REPLICATED, "notes.txt in A's tree", || {
        Ok(folder(&state_a, &["tree"])?.contains("notes.txt"))
    })?;
    wait_until(REPLICATED, "A's packs to hold notes.txt", || {
        Ok(pack_bytes(&state_a) >= notes.len() as u64)
    })?;

    let inspected = a.manage(&["inspect", "Shared"])?;
    let sync = &inspected["sync"][endpoint_b.as_str()];
    ensure!(
        sync["received"].as_u64().unwrap_or(0) >= 2,
        "sync[{endpoint_b}] = {sync}; inspect: {inspected}"
    );
    ensure!(sync["last_exchange_ms"].as_u64().unwrap_or(0) > 0, "{sync}");
    ensure!(sync["error"].is_null(), "{sync}");
    // The objects went over bulk lanes: the control lane carried metadata.
    let peers = a.manage(&["peers"])?;
    let control = &peer_at(&peers, &endpoint_b)?["lanes"][0];
    ensure!(control["role"] == "control", "{peers}");
    let down = control["health"]["bytes_down"]
        .as_u64()
        .context("no bytes_down")?;
    ensure!(down < 1024 * 1024, "control lane received {down} bytes");

    // A repo B does not publish is refused.
    let refused = error_of(a.manage(&[
        "receive",
        &endpoint_b,
        "no-such-repo",
        "Other",
        "--headless",
    ]));
    assert!(refused.contains("REPO_NOT_PUBLISHED"), "{refused}");

    // With B gone, A serves the file from its own packs.
    b.shutdown()?;
    let bytes = folder(&state_a, &["cat", "notes.txt"])?;
    ensure!(
        bytes == notes,
        "A's notes.txt differs from the source ({} of {} bytes)",
        bytes.len(),
        notes.len()
    );
    a.shutdown()?;
    Ok(())
}

/// Polls `check` until it holds, at most `bound`.
fn wait_until(bound: Duration, what: &str, mut check: impl FnMut() -> Result<bool>) -> Result<()> {
    let end = Instant::now() + bound;
    loop {
        let last = match check() {
            Ok(true) => return Ok(()),
            Ok(false) => "not yet".to_owned(),
            Err(error) => format!("{error:#}"),
        };
        ensure!(Instant::now() < end, "no {what} within {bound:?}: {last}");
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// Bytes in the pack files under `state` (every `packs` directory).
fn pack_bytes(state: &Path) -> u64 {
    fn walk(dir: &Path, packs: bool) -> u64 {
        let Ok(entries) = fs::read_dir(dir) else {
            return 0;
        };
        entries
            .flatten()
            .map(|entry| {
                let path = entry.path();
                match entry.file_type() {
                    Ok(kind) if kind.is_dir() => walk(&path, packs || entry.file_name() == "packs"),
                    Ok(_) if packs => entry.metadata().map_or(0, |m| m.len()),
                    _ => 0,
                }
            })
            .sum()
    }
    walk(state, false)
}

#[test]
fn paused_folder_stops_exchanging_and_resume_catches_up() -> Result<()> {
    let first = text(1000, 3);
    let Shared {
        a,
        b,
        state_a,
        state_b,
        ..
    } = shared_pair(&[("first.txt", &first)])?;
    wait_file(&state_a, "first.txt", &first, REPLICATED)?;

    let paused = a.manage(&["pause", "Shared"])?;
    ensure!(paused["paused"] == true, "{paused}");
    let before = folder(&state_a, &["tree"])?;
    let second = text(2000, 5);
    import(&b, &state_b, "second.txt", &second)?;
    std::thread::sleep(Duration::from_secs(5));
    let after = folder(&state_a, &["tree"])?;
    ensure!(
        before == after,
        "a paused folder changed:\n{before}\n→\n{after}"
    );
    ensure!(
        folder(&state_a, &["cat", "second.txt"]).is_err(),
        "second.txt reached a paused folder"
    );

    a.manage(&["resume", "Shared"])?;
    wait_file(&state_a, "second.txt", &second, Duration::from_secs(30))?;
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

#[test]
fn clock_offset_is_measured_and_stored() -> Result<()> {
    let a = TestDaemon::start()?;
    let b = TestDaemon::start()?;
    let code = b.manage(&["reset-pairing-code"])?["code"]
        .as_str()
        .context("no code")?
        .to_string();
    let endpoint_b = b.listen_endpoint()?;
    a.manage(&["pair", &endpoint_b, "--code", &code, "--connections", "2"])?;
    a.wait_lanes(&endpoint_b, 2, "Ready", LANES_READY)?;
    // Probed at Ready: within a few scheduler rounds.
    let end = Instant::now() + Duration::from_secs(15);
    let clock = loop {
        let peers = a.manage(&["peers"])?;
        let clock = peer_at(&peers, &endpoint_b)?["clock"].clone();
        if !clock.is_null() {
            break clock;
        }
        ensure!(Instant::now() < end, "no clock measured: {peers}");
        std::thread::sleep(Duration::from_millis(200));
    };
    let offset = clock["offset_ms"].as_i64().context("no offset_ms")?;
    let rtt = clock["rtt_ms"].as_u64().context("no rtt_ms")?;
    let measured_at = clock["measured_at"].as_u64().context("no measured_at")?;
    ensure!(offset.abs() <= 2000, "{clock}");
    ensure!(rtt < 1000, "{clock}");
    ensure!(measured_at >= tkfs::clock::FLOOR_MS, "{clock}");
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

/// The daemon side of an invalid clock: `tkfs start` refuses (exit 78,
/// `CLOCK_INVALID`) before it listens or writes `orchestrator.json`. The dial
/// side cannot run end to end (a daemon with that clock never starts), so
/// the lane's pre-dial check is the unit test
/// `network::tests::a_lane_with_an_invalid_clock_backs_off_without_dialing`.
#[test]
#[cfg_attr(
    not(debug_assertions),
    ignore = "TKFS_FAKE_NOW_MS is only honoured by debug builds"
)]
fn peer_with_invalid_clock_is_refused() -> Result<()> {
    use std::process::{Command, Stdio};
    let home = tempfile::tempdir()?;
    let listen = free_address()?;
    let config = home.path().join("orchestrator.toml");
    fs::write(
        &config,
        format!(
            "format_version = 2\ndata_directory = \"data\"\n\
             default_mount_directory = \"mounts\"\n\n[network]\nlisten = \"{listen}\"\n"
        ),
    )?;
    let log = home.path().join("daemon.log");
    let file = fs::File::create(&log)?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_tkfs"))
        .arg("-f")
        .arg(&config)
        .arg("start")
        .stdin(Stdio::null())
        .stdout(file.try_clone()?)
        .stderr(file)
        .env("TKFS_FAKE_NOW_MS", "1000000000000")
        .env_remove("TKFS_UNSUPERVISED_EXIT_S")
        .spawn()?;
    let end = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= end {
            let _ = child.kill();
            let _ = child.wait();
            anyhow::bail!("STRANDED_PROCESS: the daemon with an invalid clock did not exit");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let output = fs::read_to_string(&log)?;
    assert_eq!(status.code(), Some(78), "{status}: {output}");
    assert!(output.contains("CLOCK_INVALID"), "{output}");
    assert!(!home.path().join("data").join("orchestrator.json").exists());
    ensure!(
        std::net::TcpStream::connect(&listen).is_err(),
        "{listen} accepted a connection"
    );
    Ok(())
}
