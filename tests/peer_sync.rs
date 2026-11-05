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
use tkfs::{
    model::protocol::LaneRole,
    orchestrator::sync::MissingPage,
    session::{ConnOptions, Priority},
    util::time::now_ms,
};

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
fn a_dial_only_host_pairs_and_its_peer_keeps_no_endpoint_for_it() -> Result<()> {
    // A has no listener and no advertise address: it can only dial.
    let a = TestDaemon::start_listening("", "")?;
    let b = TestDaemon::start()?;
    let discovery: Value = serde_json::from_slice(&fs::read(a.data().join("orchestrator.json"))?)?;
    assert!(discovery["listen"].is_null(), "{discovery}");
    let code = b.manage(&["reset-pairing-code"])?["code"]
        .as_str()
        .context("no code")?
        .to_string();
    let endpoint_b = b.listen_endpoint()?;
    let paired = a.manage(&["pair", &endpoint_b, "--code", &code, "--connections", "2"])?;
    assert_eq!(paired["installation"], installation(&b)?, "{paired}");
    // A's lanes to B come up.
    a.wait_lanes(&endpoint_b, 2, "Ready", LANES_READY)?;
    // B keeps A paired inbound only, with no endpoint to dial.
    let peers_b = b.manage(&["peers"])?;
    let inbound = peers_b.as_array().context("peers is not a list")?;
    assert_eq!(inbound.len(), 1, "{peers_b}");
    assert_eq!(inbound[0]["installation"], installation(&a)?, "{peers_b}");
    assert!(inbound[0]["endpoint"].is_null(), "{peers_b}");
    assert_eq!(inbound[0]["direction"], "inbound", "{peers_b}");
    assert_eq!(inbound[0]["paired"], true, "{peers_b}");
    // B logged the enrollment with no endpoint.
    let log = b.log_text();
    let enrolled = log
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|line| line["code"] == "PEER_ENROLLED")
        .with_context(|| format!("no PEER_ENROLLED in B's log:\n{log}"))?;
    assert!(
        enrolled.get("endpoint").is_some_and(Value::is_null),
        "{enrolled}"
    );
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
    shared_pair_of(TestDaemon::start()?, TestDaemon::start()?, files)
}

/// [`shared_pair`] on daemons the test started itself.
fn shared_pair_of(a: TestDaemon, b: TestDaemon, files: &[(&str, &str)]) -> Result<Shared> {
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
    // One folder per repo: a second `receive` of it is refused, nothing added.
    let listed = a.manage(&["list"])?;
    let repo = inspected["record"]["repo"]
        .as_str()
        .context("no repo")?
        .to_owned();
    let twice = error_of(a.manage(&["receive", &endpoint_b, &repo, "Again", "--headless"]));
    assert!(twice.contains("REPO_ALREADY_PRESENT"), "{twice}");
    assert_eq!(a.manage(&["list"])?, listed);

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

// ---- content moving from dialer to listener ------------------------------

/// How long a 64 MiB file may take to push (debug builds encode every object
/// as JSON twice).
const LARGE_PUSH: Duration = Duration::from_secs(600);
/// Bound of importing a 64 MiB file.
const LARGE_IMPORT: Duration = Duration::from_secs(180);
const LARGE: usize = 64 * 1024 * 1024;
/// A push counts as under way once this many of its bytes reached the packs.
const UNDER_WAY: u64 = 1024 * 1024;

#[test]
fn a_file_created_on_the_dialer_becomes_readable_on_the_listener() -> Result<()> {
    let Shared {
        a,
        b,
        state_a,
        state_b,
        ..
    } = shared_pair(&[])?;
    // 2.5 MiB: a manifest and 40 chunks, more than one object batch.
    let from_a = text(5 * 512 * 1024, 11);
    import(&a, &state_a, "from-a.txt", &from_a)?;
    wait_file(&state_b, "from-a.txt", &from_a, REPLICATED)?;
    // The other direction still works over the same pairing.
    let from_b = text(1000, 13);
    import(&b, &state_b, "from-b.txt", &from_b)?;
    wait_file(&state_a, "from-b.txt", &from_b, REPLICATED)?;
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

/// Whether the worker of the folder at `state` has every object it needs: its
/// `objects.missing` scan, asked as the orchestrator asks it, finds none.
fn nothing_missing(state: &Path) -> Result<bool> {
    let runtime: Value = serde_json::from_slice(&fs::read(
        state.join(tkfs::orchestrator::worker::RUNTIME_FILE),
    )?)?;
    let token = fs::read_to_string(state.join(tkfs::orchestrator::worker::TOKEN_FILE))?;
    let conn = tkfs::session::dial_loopback_as(
        runtime["address"].as_str().context("no address")?,
        Some(token.trim()),
        LaneRole::Bulk,
        ConnOptions::for_role(LaneRole::Bulk),
    )
    .map_err(|error| anyhow::anyhow!("{error}"))?;
    let reply = conn.call(
        "objects.missing",
        serde_json::json!({"limit": 1}),
        Vec::new(),
        Priority::Data,
        FOLDER_COMMAND,
    );
    conn.close();
    let reply = reply.map_err(|error| anyhow::anyhow!("{error}"))?;
    let page: MissingPage = serde_json::from_slice(&reply.body)?;
    Ok(page.missing.is_empty())
}

/// Waits until the tree at `state` has `path` with content of `len` bytes.
/// Its name alone is not enough: an import creates the entry and then sets its
/// content, in two events, and an exchange can fall between them.
fn wait_entry(state: &Path, path: &str, len: usize, bound: Duration) -> Result<()> {
    wait_until(bound, &format!("{path} of {len} bytes in the tree"), || {
        let tree: Value = serde_json::from_str(&folder(state, &["tree"])?)?;
        let entries: Vec<&Value> = match &tree {
            Value::Object(entries) => entries.values().collect(),
            Value::Array(entries) => entries.iter().collect(),
            _ => anyhow::bail!("unexpected tree: {tree}"),
        };
        Ok(entries
            .iter()
            .any(|entry| entry["name"] == path && entry["content"]["total_len"] == len))
    })
}

/// Waits until the folder at `state` knows `path` and lacks no object.
fn wait_settled(state: &Path, path: &str, bound: Duration) -> Result<()> {
    wait_until(bound, &format!("{path} settled"), || {
        Ok(folder(state, &["tree"])?.contains(path) && nothing_missing(state)?)
    })
}

/// The two heads of the one conflict in `state`, once it lists them, as
/// (entity, version ids).
fn wait_conflict(state: &Path) -> Result<(String, Vec<String>)> {
    let mut found = None;
    wait_until(REPLICATED, "a conflict with two heads", || {
        let listed: Value = serde_json::from_str(&folder(state, &["conflicts"])?)?;
        let Some(conflict) = listed.as_array().and_then(|list| list.first()) else {
            return Ok(false);
        };
        let heads: Vec<String> = conflict["heads"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|head| head["id"].as_str().map(str::to_owned))
            .collect();
        if heads.len() != 2 {
            return Ok(false);
        }
        let entity = conflict["entity"].as_str().context("no entity")?.to_owned();
        found = Some((entity, heads));
        Ok(true)
    })?;
    found.context("the wait ended without a conflict")
}

/// `preview-version` of `version` in the folder at `state`.
fn preview(state: &Path, version: &str) -> Result<String> {
    let reply: Value = serde_json::from_str(&folder(
        state,
        &[
            "rpc",
            "preview-version",
            &serde_json::json!({ "version": version }).to_string(),
        ],
    )?)?;
    Ok(reply["preview"].as_str().context("no preview")?.to_owned())
}

#[test]
fn edits_converge_and_conflict_versions_survive_their_writer_leaving() -> Result<()> {
    let Shared {
        a,
        b,
        state_a,
        state_b,
        ..
    } = shared_pair(&[])?;
    // Edits on either side reach the other.
    let first = text(1000, 17);
    import(&a, &state_a, "doc.txt", &first)?;
    wait_file(&state_b, "doc.txt", &first, REPLICATED)?;
    let second = text(1000, 19);
    import(&b, &state_b, "doc.txt", &second)?;
    wait_file(&state_a, "doc.txt", &second, REPLICATED)?;

    // Concurrent edits, one on each side, of more than a chunk each.
    a.manage(&["pause", "Shared"])?;
    b.manage(&["pause", "Shared"])?;
    let (ours, theirs) = (text(200 * 1024, 23), text(200 * 1024, 29));
    import(&a, &state_a, "doc.txt", &ours)?;
    import(&b, &state_b, "doc.txt", &theirs)?;
    a.manage(&["resume", "Shared"])?;
    b.manage(&["resume", "Shared"])?;
    let (entity, heads) = wait_conflict(&state_b)?;

    // Background transfer settles: nothing has read A's version on B.
    wait_settled(&state_b, "doc.txt", REPLICATED)?;

    // With A gone, only B's packs can answer.
    a.shutdown()?;
    let mut version_a = None;
    let mut version_b = None;
    for head in &heads {
        let shown = preview(&state_b, head)?;
        if shown.contains(&ours[..1024]) {
            version_a = Some(head.clone());
        } else if shown.contains(&theirs[..1024]) {
            version_b = Some(head.clone());
        }
    }
    let version_a = version_a.context("A's version was not among the previews")?;
    version_b.context("B's version was not among the previews")?;
    folder(&state_b, &["resolve", &entity, &version_a])?;
    let bytes = folder(&state_b, &["cat", "doc.txt"])?;
    ensure!(bytes == ours, "doc.txt on B is not A's version");
    b.shutdown()?;
    Ok(())
}

#[test]
fn a_listener_read_fails_cleanly_while_the_dialer_is_away_then_succeeds() -> Result<()> {
    // A behaves as an older peer, so metadata goes over and bytes do not.
    let a = TestDaemon::start_env(
        "",
        &[(
            "TKFS_FAULT_POINT",
            tkfs::model::protocol::features::FAULT_NONE,
        )],
    )?;
    let Shared {
        a,
        b,
        state_a,
        state_b,
        ..
    } = shared_pair_of(a, TestDaemon::start()?, &[])?;
    let content = text(100 * 1024, 31);
    import(&a, &state_a, "away.txt", &content)?;
    wait_entry(&state_b, "away.txt", content.len(), REPLICATED)?;
    let home_a = a.shutdown_keep_home()?;

    let began = Instant::now();
    let refused = match folder(&state_b, &["cat", "away.txt"]) {
        Ok(bytes) => anyhow::bail!("read {} bytes with A away", bytes.len()),
        Err(error) => format!("{error:#}"),
    };
    ensure!(refused.contains("OBJECT_MISSING"), "{refused}");
    let took = began.elapsed();
    ensure!(took < Duration::from_secs(25), "the read took {took:?}");

    // A returns without the fault: the read succeeds.
    let a = TestDaemon::start_in(home_a)?;
    wait_file(&state_b, "away.txt", &content, REPLICATED)?;
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

/// Imports `content` as `path`, within [`LARGE_IMPORT`].
fn import_large(daemon: &TestDaemon, state: &Path, path: &str, content: &str) -> Result<()> {
    let source = daemon.home().join(format!("source-{path}"));
    fs::write(&source, content)?;
    let state = state.to_str().context("state path is not UTF-8")?;
    let source = source.to_str().context("source path is not UTF-8")?;
    let output = run_tkfs(&["--state", state, "import", path, source], LARGE_IMPORT)?;
    ensure!(
        output.code == Some(0),
        "import {path} exited {:?}: {}",
        output.code,
        output.stderr.trim()
    );
    Ok(())
}

/// Waits until a push of `len` bytes is under way into the packs at `state`,
/// which held `base` bytes before it, and has not finished yet.
fn wait_push_under_way(state: &Path, base: u64, len: usize) -> Result<()> {
    wait_until(LARGE_PUSH, "a push under way", || {
        Ok(pack_bytes(state) >= base + UNDER_WAY)
    })?;
    let at = pack_bytes(state);
    ensure!(
        at < base + len as u64,
        "the push had already finished ({at} bytes in the packs)"
    );
    Ok(())
}

/// `path` as a store on disk holds it, with no worker and nobody to fetch from:
/// every byte must be in its packs.
fn stored_file(state: &Path, path: &str) -> Result<Vec<u8>> {
    let engine = tkfs::runtime::Engine::open(state, None)?;
    let handle = engine.open_file(path, false, false, false)?;
    let (_, len) = engine.info(handle)?;
    let mut bytes = vec![0; len as usize];
    let mut filled = 0;
    while filled < bytes.len() {
        let read = engine.read(handle, filled as u64, &mut bytes[filled..])?;
        ensure!(read > 0, "{path} ended at {filled} of {len} bytes");
        filled += read;
    }
    engine.close(handle)?;
    engine.shutdown()?;
    Ok(bytes)
}

#[test]
fn an_interrupted_push_completes_after_restart() -> Result<()> {
    // B on a fixed port, so A's lanes find it again after it restarts.
    let b = TestDaemon::start_listening(&free_address()?, "")?;
    let Shared {
        a,
        b,
        state_a,
        state_b,
        ..
    } = shared_pair_of(TestDaemon::start()?, b, &[])?;

    // A, the side pushing, is killed mid-push and comes back.
    let first = text(LARGE, 37);
    let base = pack_bytes(&state_b);
    import_large(&a, &state_a, "first.txt", &first)?;
    wait_push_under_way(&state_b, base, LARGE)?;
    let a = TestDaemon::start_in(a.kill_keep_home()?)?;
    wait_settled(&state_b, "first.txt", LARGE_PUSH)?;

    // B, the side stored into, is killed mid-push and comes back.
    let second = text(LARGE, 41);
    let base = pack_bytes(&state_b);
    import_large(&a, &state_a, "second.txt", &second)?;
    wait_push_under_way(&state_b, base, LARGE)?;
    let b = TestDaemon::start_in(b.kill_keep_home()?)?;
    wait_settled(&state_b, "second.txt", LARGE_PUSH)?;

    // Stopped, B's store holds both files whole.
    let home_b = b.shutdown_keep_home()?;
    a.shutdown()?;
    for (path, content) in [("first.txt", &first), ("second.txt", &second)] {
        let bytes = stored_file(&state_b, path)?;
        ensure!(
            bytes == content.as_bytes(),
            "{path} differs on B ({} of {} bytes)",
            bytes.len(),
            content.len()
        );
    }
    drop(home_b);
    Ok(())
}

#[test]
fn a_zero_byte_file_reaches_the_listener() -> Result<()> {
    let Shared {
        a,
        b,
        state_a,
        state_b,
        ..
    } = shared_pair(&[])?;
    import(&a, &state_a, "empty.txt", "")?;
    wait_file(&state_b, "empty.txt", "", REPLICATED)?;
    // The pump is not stuck on it: what comes next still arrives.
    let next = text(1000, 47);
    import(&a, &state_a, "next.txt", &next)?;
    wait_file(&state_b, "next.txt", &next, REPLICATED)?;
    let log = a.log_text();
    ensure!(!log.contains("SYNC_PUSH_FAILED"), "A's log:\n{log}");
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

#[test]
fn a_paused_listener_causes_backoff_and_resumes() -> Result<()> {
    let Shared {
        a,
        b,
        state_a,
        state_b,
        ..
    } = shared_pair(&[])?;
    b.manage(&["pause", "Shared"])?;
    let content = text(2000, 43);
    import(&a, &state_a, "paused.txt", &content)?;
    std::thread::sleep(Duration::from_secs(5));
    let log = a.log_text();
    let failures = log
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|line| {
            line["code"] == "SYNC_PUSH_FAILED"
                && line["error"]
                    .as_str()
                    .is_some_and(|error| error.contains("REPO_NOT_PUBLISHED"))
        })
        .count();
    // Once: it did fail, and it did not flood the log.
    ensure!(failures == 1, "{failures} failures in A's log:\n{log}");
    b.manage(&["resume", "Shared"])?;
    wait_file(&state_b, "paused.txt", &content, REPLICATED)?;
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

// ---- active sync: notify and the backup poll -----------------------------

/// Polls so long that only a notify or a local change can explain a quick
/// arrival.
const NOTIFY_ONLY: &str = "sync_interval_ms = 60000\nbackup_poll_ms = 60000\n";
/// How long a notified change may take, against a poll of 60 s.
const NOTIFIED: Duration = Duration::from_secs(10);
/// The reader's own wait (`Demand::FETCH_WAIT`).
const FETCH_WAIT: Duration = Duration::from_secs(10);
/// How long a blocked reader may wait behind a background transfer.
const READER_BOUND: Duration = Duration::from_secs(5);

/// How many `PEER_NOTIFY_DROPPED` lines of `reason` are in `daemon`'s log.
fn notifies_dropped(daemon: &TestDaemon, reason: &str) -> usize {
    daemon
        .log_text()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|line| line["code"] == "PEER_NOTIFY_DROPPED" && line["reason"] == reason)
        .count()
}

/// Waits until `path` is readable at `state`, and fails past `bound` even if
/// a read that began in time ended late.
fn arrives_within(state: &Path, path: &str, expected: &str, bound: Duration) -> Result<()> {
    let began = Instant::now();
    wait_file(state, path, expected, bound)?;
    let took = began.elapsed();
    ensure!(took < bound, "{path} took {took:?}, past {bound:?}");
    Ok(())
}

#[test]
fn a_change_on_either_side_arrives_by_notify_well_inside_the_poll() -> Result<()> {
    let Shared {
        a,
        b,
        endpoint_b,
        state_a,
        state_b,
    } = shared_pair_of(
        TestDaemon::start_with(NOTIFY_ONLY)?,
        TestDaemon::start_with(NOTIFY_ONLY)?,
        &[],
    )?;
    // The dialer's own change.
    let from_a = text(1000, 53);
    import(&a, &state_a, "from-a.txt", &from_a)?;
    arrives_within(&state_b, "from-a.txt", &from_a, NOTIFIED)?;
    // The listener's change, which only a notify can announce.
    let from_b = text(1000, 59);
    import(&b, &state_b, "from-b.txt", &from_b)?;
    arrives_within(&state_a, "from-b.txt", &from_b, NOTIFIED)?;

    // Once those settle, heartbeats alone start no exchange on the dialer. The
    // worker's `seq` moves every second, so a quiet window spans several of them,
    // and a pair that exchanged on each one would never see such a window.
    let last_exchange = || -> Result<u64> {
        let inspected = a.manage(&["inspect", "Shared"])?;
        inspected["sync"][endpoint_b.as_str()]["last_exchange_ms"]
            .as_u64()
            .with_context(|| format!("no last_exchange_ms: {inspected}"))
    };
    wait_until(Duration::from_secs(30), "a quiet idle window", || {
        let before = last_exchange()?;
        std::thread::sleep(Duration::from_secs(4));
        Ok(last_exchange()? == before)
    })?;
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

#[test]
fn the_backup_poll_delivers_a_change_when_notify_is_suppressed() -> Result<()> {
    const POLLED: &str = "sync_interval_ms = 60000\nbackup_poll_ms = 2000\n";
    let a = TestDaemon::start_with(POLLED)?;
    let b = TestDaemon::start_env(POLLED, &[("TKFS_FAULT_POINT", "peer.notify.drop")])?;
    let Shared {
        a,
        b,
        state_a,
        state_b,
        ..
    } = shared_pair_of(a, b, &[])?;
    let from_b = text(1000, 61);
    import(&b, &state_b, "from-b.txt", &from_b)?;
    // Well before the 60 s interval, and with every notify of B dropped.
    arrives_within(&state_a, "from-b.txt", &from_b, Duration::from_secs(20))?;
    // The poll may beat B's watch to the change, so the drop can come just after.
    wait_until(NOTIFIED, "a suppressed notify in B's log", || {
        Ok(notifies_dropped(&b, "metadata") > 0)
    })?;
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

#[test]
fn a_repeated_demand_notify_saves_a_blocked_reader() -> Result<()> {
    // A pushes only what is asked for, so the bytes stay on A until B's reader
    // asks; B loses its first demand notify and no other.
    let a = TestDaemon::start_env(
        NOTIFY_ONLY,
        &[("TKFS_FAULT_POINT", "sync.push.background.skip")],
    )?;
    let b = TestDaemon::start_env(
        NOTIFY_ONLY,
        &[
            ("TKFS_FAULT_POINT", "peer.notify.drop.demand"),
            ("TKFS_FAULT_COUNT", "1"),
        ],
    )?;
    let Shared {
        a,
        b,
        state_a,
        state_b,
        ..
    } = shared_pair_of(a, b, &[])?;
    let content = text(1000, 67);
    import(&a, &state_a, "only-a.txt", &content)?;
    wait_entry(&state_b, "only-a.txt", content.len(), NOTIFIED)?;
    ensure!(
        pack_bytes(&state_b) < content.len() as u64,
        "the bytes reached B before its reader asked"
    );

    let began = Instant::now();
    let bytes = folder(&state_b, &["cat", "only-a.txt"])?;
    let took = began.elapsed();
    ensure!(bytes == content, "only-a.txt differs on B");
    ensure!(took < FETCH_WAIT, "the read took {took:?}");
    ensure!(
        notifies_dropped(&b, "demand") == 1,
        "the first demand notify was not the one dropped:\n{}",
        b.log_text()
    );
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

/// A small file imported on A reaches a reader on B within [`READER_BOUND`],
/// while a transfer into the packs at `busy` (which held `base` bytes before
/// it) keeps running.
fn reader_goes_first(
    (a, state_a): (&TestDaemon, &Path),
    state_b: &Path,
    (busy, base): (&Path, u64),
    (path, seed): (&str, u64),
) -> Result<()> {
    let content = text(1000, seed);
    import(a, state_a, path, &content)?;
    wait_entry(state_b, path, content.len(), NOTIFIED)?;
    let began = Instant::now();
    let bytes = folder(state_b, &["cat", path])?;
    let took = began.elapsed();
    ensure!(bytes == content, "{path} differs on B");
    ensure!(took < READER_BOUND, "the read of {path} took {took:?}");
    let at = pack_bytes(busy);
    ensure!(
        at < base + LARGE as u64,
        "the transfer had finished by then ({at} bytes in the packs)"
    );
    Ok(())
}

#[test]
fn a_waiting_reader_goes_ahead_of_background_transfer() -> Result<()> {
    let Shared {
        a,
        b,
        state_a,
        state_b,
        ..
    } = shared_pair_of(
        TestDaemon::start_with(NOTIFY_ONLY)?,
        TestDaemon::start_with(NOTIFY_ONLY)?,
        &[],
    )?;
    // From A to B: A's pump is pushing.
    let base = pack_bytes(&state_b);
    import_large(&a, &state_a, "big-a.txt", &text(LARGE, 71))?;
    wait_push_under_way(&state_b, base, LARGE)?;
    reader_goes_first(
        (&a, &state_a),
        &state_b,
        (&state_b, base),
        ("small-1.txt", 73),
    )?;
    wait_settled(&state_b, "big-a.txt", LARGE_PUSH)?;
    wait_settled(&state_a, "big-a.txt", LARGE_PUSH)?;

    // From B to A: A's pump is pulling.
    let base = pack_bytes(&state_a);
    import_large(&b, &state_b, "big-b.txt", &text(LARGE, 79))?;
    wait_push_under_way(&state_a, base, LARGE)?;
    reader_goes_first(
        (&a, &state_a),
        &state_b,
        (&state_a, base),
        ("small-2.txt", 83),
    )?;
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

/// In the test above a background page also lists demand, so the reader is
/// served either way. Here A pushes only what is asked for, so only the demand
/// notify can get it the file, and it has to cut into A's running pull.
#[test]
fn a_demand_notify_cuts_into_a_background_pull() -> Result<()> {
    let a = TestDaemon::start_env(
        NOTIFY_ONLY,
        &[("TKFS_FAULT_POINT", "sync.push.background.skip")],
    )?;
    let Shared {
        a,
        b,
        state_a,
        state_b,
        ..
    } = shared_pair_of(a, TestDaemon::start_with(NOTIFY_ONLY)?, &[])?;
    let base = pack_bytes(&state_a);
    import_large(&b, &state_b, "big-b.txt", &text(LARGE, 101))?;
    wait_push_under_way(&state_a, base, LARGE)?;
    reader_goes_first(
        (&a, &state_a),
        &state_b,
        (&state_a, base),
        ("small.txt", 103),
    )?;
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

#[test]
fn an_edit_made_while_lanes_are_down_arrives_when_they_return() -> Result<()> {
    // B on a fixed port, so A's lanes find it again after it restarts.
    const LONG_POLL: &str = "backup_poll_ms = 60000\n";
    let a = TestDaemon::start_with(LONG_POLL)?;
    let b = TestDaemon::start_listening(&free_address()?, LONG_POLL)?;
    let Shared {
        a,
        b,
        state_a,
        state_b,
        ..
    } = shared_pair_of(a, b, &[])?;
    let first = text(1000, 89);
    import(&a, &state_a, "first.txt", &first)?;
    arrives_within(&state_b, "first.txt", &first, NOTIFIED)?;

    let home_b = b.shutdown_keep_home()?;
    let offline = text(1000, 97);
    import(&a, &state_a, "offline.txt", &offline)?;
    let b = TestDaemon::start_in(home_b)?;
    b.wait_phase("Shared", "Running", REPLICATED)?;
    arrives_within(&state_b, "offline.txt", &offline, Duration::from_secs(20))?;
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

// ---- passive dialers and sync-now -----------------------------------------

const PASSIVE: &str = "sync_profile = \"passive\"\n";
/// `sync_driver::SYNC_NOW_BOUND`: `endpoint::REPLY_TIMEOUT` less 5 s.
const SYNC_NOW_BOUND: Duration = Duration::from_secs(175);
/// How long a passive dialer is watched doing nothing.
const IDLE_WINDOW: Duration = Duration::from_secs(15);

/// [`shared_pair`] with a passive A.
fn passive_pair(files: &[(&str, &str)]) -> Result<Shared> {
    shared_pair_of(
        TestDaemon::start_with(PASSIVE)?,
        TestDaemon::start()?,
        files,
    )
}

/// `manage sync-now`, within its bound: the reply's one folder, and how long
/// it took.
fn sync_now(daemon: &TestDaemon) -> Result<(Value, Duration)> {
    let began = Instant::now();
    let reply = daemon.manage(&["sync-now"])?;
    let took = began.elapsed();
    ensure!(took < SYNC_NOW_BOUND, "sync-now took {took:?}");
    let folders = reply["folders"].as_array().context("no folders")?;
    ensure!(folders.len() == 1, "{reply}");
    Ok((folders[0].clone(), took))
}

/// [`sync_now`] that reports no error.
fn sync_now_clean(daemon: &TestDaemon) -> Result<Value> {
    let (folder, _) = sync_now(daemon)?;
    ensure!(
        folder["error"].is_null(),
        "{folder}\nlog:\n{}",
        daemon.log_text()
    );
    Ok(folder)
}

/// `daemon`'s `peers` row for the installation of `peer`.
fn row_of(daemon: &TestDaemon, peer: &TestDaemon) -> Result<Value> {
    let installation = installation(peer)?;
    let peers = daemon.manage(&["peers"])?;
    peers
        .as_array()
        .into_iter()
        .flatten()
        .find(|row| row["installation"] == installation.as_str())
        .cloned()
        .with_context(|| format!("no row for {installation} in {peers}"))
}

#[test]
fn a_passive_dialer_makes_no_sync_requests_while_idle() -> Result<()> {
    let Shared { a, b, state_b, .. } = passive_pair(&[("seed.txt", "seed")])?;
    sync_now_clean(&a)?;
    let before = row_of(&b, &a)?;
    let served = before["requests_served"]
        .as_u64()
        .context("no requests_served")?;
    ensure!(served > 0, "the burst served nothing: {before}");
    // A change on the listener's side is no reason for it to reach A either.
    import(&b, &state_b, "while-idle.txt", "idle")?;
    std::thread::sleep(IDLE_WINDOW);
    let after = row_of(&b, &a)?;
    ensure!(after["requests_served"] == served, "{before} -> {after}");
    ensure!(after["notifies_sent"] == 0, "{after}");
    // The lanes stayed open meanwhile.
    ensure!(
        before["inbound"] == after["inbound"] && after["inbound"].as_u64() > Some(0),
        "{before} -> {after}"
    );
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

#[test]
fn sync_now_moves_metadata_and_pushes_but_does_not_download_unread_files() -> Result<()> {
    // Both unread on A. `cat` stops at 3 MiB, so the read is of the smaller one.
    let big = text(8 * 1024 * 1024, 107);
    let small = text(2 * 1024 * 1024, 113);
    let Shared {
        a,
        b,
        state_a,
        state_b,
        ..
    } = passive_pair(&[("big.bin", &big), ("small.bin", &small)])?;
    let new = text(1000, 109);
    import(&a, &state_a, "new.txt", &new)?;
    let reply = sync_now_clean(&a)?;
    ensure!(reply["remaining"] == 0, "{reply}");
    ensure!(reply["exchanged"].as_u64() > Some(0), "{reply}");
    ensure!(reply["pushed"].as_u64() > Some(0), "{reply}");
    // B cannot ask A for anything, so the burst must have put the bytes there.
    arrives_within(&state_b, "new.txt", &new, NOTIFIED)?;
    // A knows of both files and fetched neither.
    let tree = folder(&state_a, &["tree"])?;
    ensure!(
        tree.contains("big.bin") && tree.contains("small.bin"),
        "{tree}"
    );
    std::thread::sleep(Duration::from_secs(2));
    let held = pack_bytes(&state_a);
    ensure!(
        held < small.len() as u64 / 4,
        "A holds {held} bytes of packs against small.bin's {}",
        small.len()
    );
    // Its reader still gets one, and only that one comes.
    let bytes = folder(&state_a, &["cat", "small.bin"])?;
    ensure!(bytes == small, "small.bin differs on A");
    std::thread::sleep(Duration::from_secs(2));
    let held = pack_bytes(&state_a);
    ensure!(
        held < big.len() as u64 / 2,
        "A holds {held} bytes of packs against big.bin's {}",
        big.len()
    );
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

#[test]
fn a_passive_dialer_learns_of_a_remote_change_only_when_asked() -> Result<()> {
    let Shared {
        a,
        b,
        state_a,
        state_b,
        ..
    } = passive_pair(&[])?;
    import(&b, &state_b, "from-b.txt", "from b")?;
    std::thread::sleep(Duration::from_secs(5));
    let tree = folder(&state_a, &["tree"])?;
    ensure!(!tree.contains("from-b.txt"), "unasked: {tree}");
    // By its selector: the one folder.
    let reply = a.manage(&["sync-now", "Shared"])?;
    ensure!(reply["folders"][0]["error"].is_null(), "{reply}");
    let tree = folder(&state_a, &["tree"])?;
    ensure!(tree.contains("from-b.txt"), "asked: {tree}");
    let unknown = error_of(a.manage(&["sync-now", "no-such-folder"]));
    ensure!(unknown.contains("FOLDER_NOT_FOUND"), "{unknown}");
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

/// Once every lane to `endpoint` is in Backoff with its end over `away` off:
/// the soonest of those ends.
fn all_in_backoff(daemon: &TestDaemon, endpoint: &str, away: Duration) -> Result<u64> {
    let mut until = 0;
    wait_until(
        Duration::from_secs(120),
        "a long backoff on every lane",
        || {
            let peers = daemon.manage(&["peers"])?;
            let lanes = peer_at(&peers, endpoint)?["lanes"]
                .as_array()
                .context("no lanes")?
                .clone();
            until = lanes
                .iter()
                .map(|lane| lane["until_ms"].as_u64().unwrap_or(0))
                .min()
                .context("no lane")?;
            Ok(all_lanes(&peers, "Backoff") && until > now_ms() + away.as_millis() as u64)
        },
    )?;
    Ok(until)
}

#[test]
fn sync_now_redials_a_lane_waiting_in_backoff() -> Result<()> {
    let a = TestDaemon::start_with(PASSIVE)?;
    let b = TestDaemon::start_listening(&free_address()?, "")?;
    let Shared {
        a, b, endpoint_b, ..
    } = shared_pair_of(a, b, &[])?;
    let home_b = b.shutdown_keep_home()?;
    let until = all_in_backoff(&a, &endpoint_b, Duration::from_secs(20))?;
    let b = TestDaemon::start_in(home_b)?;
    b.wait_phase("Shared", "Running", REPLICATED)?;
    sync_now_clean(&a)?;
    a.wait_lanes(&endpoint_b, 4, "Ready", LANES_READY)?;
    let now = now_ms();
    ensure!(
        now < until,
        "the lanes came back at {now}, not before their backoff ended at {until}"
    );
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

#[test]
fn sync_now_leaves_a_refused_credential_on_hold() -> Result<()> {
    let Shared {
        a, b, endpoint_b, ..
    } = passive_pair(&[])?;
    b.manage(&["reset-pairing-code"])?;
    let held = |daemon: &TestDaemon| -> Result<Option<Vec<Value>>> {
        let peers = daemon.manage(&["peers"])?;
        let lanes = peer_at(&peers, &endpoint_b)?["lanes"]
            .as_array()
            .context("no lanes")?;
        let ends = lanes.iter().map(|lane| lane["until_ms"].clone()).collect();
        Ok(all_lanes(&peers, "Unauthorized").then_some(ends))
    };
    wait_until(LANES_REVOKED, "lanes on hold", || Ok(held(&a)?.is_some()))?;
    let ends = held(&a)?.context("the hold ended")?;
    let (reply, took) = sync_now(&a)?;
    ensure!(reply["error"].is_string(), "{reply}");
    ensure!(took < Duration::from_secs(30), "it took {took:?}: {reply}");
    // The same holds: no lane redialed.
    ensure!(held(&a)? == Some(ends), "{}", a.manage(&["peers"])?);
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

#[test]
fn sync_now_reports_an_unreachable_peer_within_its_bound() -> Result<()> {
    let Shared { a, b, .. } = passive_pair(&[])?;
    b.shutdown()?;
    let (reply, took) = sync_now(&a)?;
    ensure!(reply["error"].is_string(), "{reply}");
    // No listing came back, so nothing is known of what is left.
    ensure!(reply["remaining"].is_null(), "{reply}");
    // The lane wait, not the whole bound.
    ensure!(took < Duration::from_secs(30), "it took {took:?}: {reply}");
    a.shutdown()?;
    Ok(())
}

/// A peer that never held a repo is told apart at once. So B's folder, which A
/// exchanged with already, gets a moment when it stops answering.
const NOT_PUBLISHED_WAIT: Duration = Duration::from_secs(10);

#[test]
fn sync_now_outlasts_a_listener_folder_that_stops_for_a_moment() -> Result<()> {
    let Shared { a, b, .. } = passive_pair(&[])?;
    sync_now_clean(&a)?;
    b.manage(&["pause", "Shared"])?;
    // It comes back while A is asking.
    let reply = std::thread::scope(|scope| {
        let asking = scope.spawn(|| sync_now(&a));
        std::thread::sleep(Duration::from_secs(3));
        let resumed = b.manage(&["resume", "Shared"]);
        let reply = asking.join().expect("the sync-now thread panicked");
        resumed.and(reply)
    })?;
    ensure!(reply.0["error"].is_null(), "{}", reply.0);
    ensure!(reply.1 >= Duration::from_secs(2), "it took {:?}", reply.1);
    // It stays away: A says so, and soon.
    b.manage(&["pause", "Shared"])?;
    let (reply, took) = sync_now(&a)?;
    let error = reply["error"].as_str().unwrap_or_default();
    ensure!(error.contains("REPO_NOT_PUBLISHED"), "{reply}");
    ensure!(took >= NOT_PUBLISHED_WAIT, "it took {took:?}");
    ensure!(took < Duration::from_secs(30), "it took {took:?}");
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

#[test]
fn a_peer_that_does_not_hold_a_folder_is_no_error_and_does_not_hold_it_up() -> Result<()> {
    let Shared { a, b, .. } = passive_pair(&[])?;
    let c = TestDaemon::start()?;
    let code = c.manage(&["reset-pairing-code"])?["code"]
        .as_str()
        .context("no code")?
        .to_string();
    let endpoint_c = c.listen_endpoint()?;
    a.manage(&["pair", &endpoint_c, "--code", &code])?;
    a.wait_lanes(&endpoint_c, 4, "Ready", LANES_READY)?;
    let (reply, took) = sync_now(&a)?;
    ensure!(reply["error"].is_null(), "{reply}");
    ensure!(reply["exchanged"].as_u64() > Some(0), "{reply}");
    ensure!(reply["remaining"] == 0, "{reply}");
    ensure!(took < NOT_PUBLISHED_WAIT, "it took {took:?}");
    // A folder neither of them holds is reported, just as soon.
    a.manage(&["create", "Local", "--headless"])?;
    a.wait_phase("Local", "Running", REPLICATED)?;
    let began = Instant::now();
    let reply = a.manage(&["sync-now", "Local"])?;
    let took = began.elapsed();
    let error = reply["folders"][0]["error"].as_str().unwrap_or_default();
    ensure!(error.contains("REPO_NOT_PUBLISHED"), "{reply}");
    ensure!(took < NOT_PUBLISHED_WAIT, "it took {took:?}");
    a.shutdown()?;
    b.shutdown()?;
    c.shutdown()?;
    Ok(())
}

#[test]
fn sync_now_says_when_a_listener_takes_no_pushes() -> Result<()> {
    let Shared {
        a,
        b,
        state_a,
        state_b,
        ..
    } = shared_pair_of(
        TestDaemon::start_with(PASSIVE)?,
        TestDaemon::start_env("", &[("TKFS_FAULT_POINT", "session.features.none")])?,
        &[],
    )?;
    import(&a, &state_a, "new.txt", "new")?;
    import(&b, &state_b, "from-b.txt", "from b")?;
    let (reply, _) = sync_now(&a)?;
    let error = reply["error"].as_str().unwrap_or_default();
    ensure!(error.contains("PEER_PUSH_UNAVAILABLE"), "{reply}");
    ensure!(reply["remaining"].is_null(), "{reply}");
    ensure!(reply["exchanged"].as_u64() > Some(0), "{reply}");
    // The metadata moved all the same.
    let tree = folder(&state_a, &["tree"])?;
    ensure!(tree.contains("from-b.txt"), "{tree}");
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

#[test]
fn sync_now_works_on_an_active_dialer() -> Result<()> {
    // Polls too long to matter, and every notify of B lost: only asking moves it.
    let Shared {
        a,
        b,
        state_a,
        state_b,
        ..
    } = shared_pair_of(
        TestDaemon::start_with(NOTIFY_ONLY)?,
        TestDaemon::start_env(NOTIFY_ONLY, &[("TKFS_FAULT_POINT", "peer.notify.drop")])?,
        &[],
    )?;
    let from_b = text(1000, 127);
    import(&b, &state_b, "from-b.txt", &from_b)?;
    std::thread::sleep(Duration::from_secs(3));
    let tree = folder(&state_a, &["tree"])?;
    ensure!(!tree.contains("from-b.txt"), "unasked: {tree}");
    let from_a = text(1000, 131);
    import(&a, &state_a, "from-a.txt", &from_a)?;
    let reply = sync_now_clean(&a)?;
    ensure!(reply["exchanged"].as_u64() > Some(0), "{reply}");
    ensure!(reply["remaining"] == 0, "{reply}");
    let tree = folder(&state_a, &["tree"])?;
    ensure!(tree.contains("from-b.txt"), "asked: {tree}");
    // Its bytes come as an active dialer's do, and A's went across.
    arrives_within(&state_a, "from-b.txt", &from_b, NOTIFIED)?;
    arrives_within(&state_b, "from-a.txt", &from_a, NOTIFIED)?;
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

const OLDER: (&str, &str) = ("TKFS_FAULT_POINT", "session.features.none");

#[test]
fn an_older_listener_still_syncs_pull_only_and_peers_says_so() -> Result<()> {
    let seed = text(1000, 211);
    let Shared {
        a,
        b,
        endpoint_b,
        state_a,
        state_b,
    } = shared_pair_of(
        TestDaemon::start()?,
        TestDaemon::start_env("", &[OLDER])?,
        &[("seed.txt", &seed)],
    )?;
    arrives_within(&state_a, "seed.txt", &seed, REPLICATED)?;
    let held = pack_bytes(&state_b);
    import(&a, &state_a, "from-a.txt", &text(1000, 213))?;
    std::thread::sleep(Duration::from_secs(5));
    ensure!(
        pack_bytes(&state_b) == held,
        "the older listener took bytes it cannot accept"
    );
    let row = row_of(&a, &b)?;
    ensure!(row["two_way_content"] == false, "{row}");
    ensure!(row["features"] == serde_json::json!([]), "{row}");
    ensure!(row["endpoint"] == endpoint_b.as_str(), "{row}");
    let failed = a
        .log_text()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|line| line["code"] == "SYNC_PUSH_FAILED")
        .count();
    ensure!(failed == 0, "{failed} SYNC_PUSH_FAILED in A's log");
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

#[test]
fn an_older_dialer_keeps_the_short_poll() -> Result<()> {
    const SHORT: &str = "sync_interval_ms = 1000\nbackup_poll_ms = 60000\n";
    let Shared {
        a,
        b,
        state_a,
        state_b,
        ..
    } = shared_pair_of(
        TestDaemon::start_env(SHORT, &[OLDER])?,
        TestDaemon::start_with(SHORT)?,
        &[],
    )?;
    let from_b = text(1000, 217);
    import(&b, &state_b, "from-b.txt", &from_b)?;
    arrives_within(&state_a, "from-b.txt", &from_b, Duration::from_secs(15))?;
    let row = row_of(&b, &a)?;
    ensure!(row["notifies_sent"] == 0, "{row}");
    ensure!(row["two_way_content"] == false, "{row}");
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}

#[test]
fn peers_and_sync_views_report_features_profile_and_pushes() -> Result<()> {
    let Shared {
        a,
        b,
        endpoint_b,
        state_a,
        state_b,
    } = passive_pair(&[("seed.txt", "seed")])?;
    let from_a = text(1000, 219);
    import(&a, &state_a, "from-a.txt", &from_a)?;
    sync_now_clean(&a)?;
    arrives_within(&state_b, "from-a.txt", &from_a, NOTIFIED)?;
    let seen_by_b = row_of(&b, &a)?;
    ensure!(seen_by_b["profile"] == "passive", "{seen_by_b}");
    ensure!(
        seen_by_b["features"] == serde_json::json!(["profile-passive"]),
        "{seen_by_b}"
    );
    ensure!(seen_by_b["two_way_content"] == true, "{seen_by_b}");
    let seen_by_a = row_of(&a, &b)?;
    ensure!(seen_by_a["two_way_content"] == true, "{seen_by_a}");
    let features = seen_by_a["features"].as_array().context("no features")?;
    ensure!(features.iter().any(|f| f == "content-push"), "{seen_by_a}");
    let view = a.manage(&["inspect", "Shared"])?;
    let sync = &view["sync"][endpoint_b.as_str()];
    ensure!(sync["pushed"].as_u64() > Some(0), "{view}");
    ensure!(sync["last_push_error"].is_null(), "{view}");
    a.shutdown()?;
    b.shutdown()?;
    Ok(())
}
