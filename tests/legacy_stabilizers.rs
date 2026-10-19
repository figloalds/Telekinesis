//! WP1.12: interim stabilizers on the legacy lanes and supervisor
//! (REVAMP_PLAN §14 WP1.12). Real daemons through `tests/support`; the
//! release-mode scenarios with floors live in `qual_wp1_12_legacy.rs`.
//!
//! WP3.1 deleted the legacy lanes and their supervisor; what is left here
//! holds on the v3 stack too:
//!
//! * `idle_lanes_survive_an_interval_longer_than_the_idle_timeout`: v3
//!   sessions heartbeat every 5 s, so idle lanes never reconnect;
//! * `mount_failure_keeps_the_folder_syncing_headless`.
//!
//! Removed by WP3.1 (each tested a mechanism the cutover deleted):
//! `hung_remote_worker_is_charged_while_the_healthy_repository_syncs`,
//! `published_list_is_bounded_by_one_worker_deadline`,
//! `crash_looping_worker_backs_off_and_list_stays_responsive` and
//! `shutdown_continues_past_a_folder_that_cannot_stop` (ignored since
//! WP2.INT, CCR-WP2.1-2; replaced by `orchestrator_runtime`'s tests), and
//! `lanes_are_connected_only_after_a_request_succeeds` (a v2 lane rule
//! against a v2 acceptor, which now fails closed:
//! `orchestrator::legacy_peer_fails_closed_with_needs_upgrade`; a v3 lane is
//! connected only once its session is authenticated, `peer_supervisor`).
mod support;

use serde_json::json;
use std::{fs, thread, time::Duration};
use support::*;

fn lanes_connected(node: &Node) -> bool {
    node.raw_call(json!({"op":"peers"})).is_ok_and(|p| {
        p[0]["lanes"]
            .as_array()
            .is_some_and(|l| !l.is_empty() && l.iter().all(|l| l["connected"] == true))
    })
}

/// With `sync_interval_ms` above the legacy acceptor's 15 s idle drop, idle
/// lanes never reconnect (since WP3.1 the lanes are v3 sessions, which
/// heartbeat after 5 s of send idleness and are dead only after 20 s with no
/// frame received).
#[test]
fn idle_lanes_survive_an_interval_longer_than_the_idle_timeout() {
    let a = Node::new();
    let b = Node::start(NodeConfig::default().interval(20_000).connections(2));
    let proxy = FaultProxy::start(&a.endpoint());
    let pa = a.create("Idle source", true);
    b.pair_endpoint(&proxy.endpoint(), &a.code(), Some(2));
    let _pb = b.receive_from(&proxy.endpoint(), &pa);
    b.wait_for("lanes connected", Duration::from_secs(60), || {
        lanes_connected(&b)
    });
    thread::sleep(Duration::from_secs(2));
    let before = proxy.connections();
    thread::sleep(Duration::from_secs(26));
    assert_eq!(
        proxy.connections() - before,
        0,
        "idle lanes reconnected\n{}",
        b.logs()
    );
    assert!(lanes_connected(&b));
}

/// A requested mount that fails no longer kills the worker: it reports Ready
/// with `mounted:false` and `mount_error` (WP1.4), the supervisor keeps it
/// running headless, reports the error and keeps syncing.
#[test]
fn mount_failure_keeps_the_folder_syncing_headless() {
    if !mount_supported() {
        return;
    }
    let a = Node::new();
    let b = Node::new();
    let mount = a.path().join("blocked-mount");
    let p = a.create_mounted("Blocked mount", &mount);
    a.stop_folder_exited(&p, STOP_EXIT_TIMEOUT).unwrap();
    // A pre-existing, non-empty mount path: the attach must fail.
    fs::create_dir_all(&mount).unwrap();
    fs::write(mount.join("squatter.txt"), b"not ours").unwrap();
    let state = state_id(&p);
    a.call(json!({"op":"start","state":state}));
    a.wait_for("running headless", Duration::from_secs(30), || {
        a.try_inspect(&state)
            .is_ok_and(|i| i["running"] == true && i["mounted"] == false)
    });
    let inspected = a.inspect(&state);
    assert!(
        inspected["error"]
            .as_str()
            .is_some_and(|e| e.contains("MOUNT_PATH_MUST_NOT_EXIST")),
        "{inspected}"
    );
    let pids = a.worker_pids(&p);
    b.pair(&a, &a.code());
    let pb = b.receive(&a, &p);
    write_file(&p, "while-unmounted.txt", b"still syncing");
    b.wait_for("sync without the mount", Duration::from_secs(60), || {
        has_file(&pb, "while-unmounted.txt", b"still syncing")
    });
    assert_eq!(a.worker_pids(&p), pids, "the worker was never restarted");
    assert_eq!(fs::read(mount.join("squatter.txt")).unwrap(), b"not ours");
}
