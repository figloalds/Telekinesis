//! Two real orchestrators on loopback: pairing, peer lanes, pairing-code
//! resets and remote listings. Headless only: never passes `--mount`. Every
//! wait is bounded (see `support::daemon`).
mod support;

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::{
    fs,
    time::{Duration, Instant},
};
use support::daemon::TestDaemon;

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
