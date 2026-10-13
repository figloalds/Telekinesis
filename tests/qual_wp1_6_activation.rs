//! Qualification scenarios for WP1.6 (inbox state machine, remote fast paths,
//! classified quarantine). REVAMP_PLAN §14.6; floors in §15 and
//! `tests/qualification-floors.json`.
//!
//! Created by WP0.5 against the 6298d0b baseline, `#[ignore = "WP1.6"]`.
//! WP1.6 un-ignores and extends them. Run in release:
//! `cargo test --release --test qual_wp1_6_activation -- --ignored --test-threads=1 --nocapture`
mod support;

use serde_json::json;
use std::{collections::BTreeSet, time::Duration};
use support::*;
use tkfs::core::{Store, id};

const FILE: &str = "qual_wp1_6_activation";

/// A authors `n` linear creates; B (another replica) activates them as remote events.
fn remote_linear_create(name: &str, n: usize) {
    let mut s = Scenario::new(FILE, name);
    let n = scaled(n);
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = Store::open(&dir.path().join("a"), Some(&repo)).unwrap();
    let mut b = Store::open(&dir.path().join("b"), Some(&repo)).unwrap();
    let (_, setup_ms) = time_ms(|| linear_history(&mut a, n).unwrap());
    s.note(format!("authored {n} local creates in {setup_ms:.0} ms"));
    let allowed = devices([&a, &b]);
    let bundle = missing_bundle(&a, &b).unwrap();
    s.measure_from_now();
    let delivery = deliver_paced(&mut b, &bundle, &allowed, s.deadline()).unwrap();
    s.metric_with(
        "per_event_ms",
        delivery.mean_per_event_ms(),
        "ms",
        json!({"events": n, "activated": delivery.delivered,
            "last_per_event_ms": delivery.per_event_ms.last()}),
    );
    if delivery.timed_out {
        s.timeout(format!("{} of {n} events activated", delivery.delivered));
    }
    s.check(
        b.lookup(&format!("lin-{:06}.txt", n - 1)).is_ok() || delivery.timed_out,
        "last create visible on B",
    );
    s.finish();
}

#[test]
#[ignore = "WP1.6"]
fn remote_linear_create_2k() {
    remote_linear_create("remote_linear_create_2k", 2000);
}

#[test]
#[ignore = "WP1.6"]
fn remote_linear_create_10k() {
    remote_linear_create("remote_linear_create_10k", 10_000);
}

/// A quarantined event must not count as pending: the lane reports
/// `metadata_caught_up` and unrelated history keeps activating.
#[test]
#[ignore = "WP1.6"]
fn quarantine_not_pending() {
    let mut s = Scenario::new(FILE, "quarantine_not_pending");
    let a = Node::new();
    let b = Node::new();
    let pa = a.create("Quarantine source", true);
    write_file(&pa, "one.txt", b"one");
    b.pair(&a, &a.code());
    let pb = b.receive(&a, &pa);
    b.wait_for("first file", Duration::from_secs(60), || {
        has_file(&pb, "one.txt", b"one")
    });
    let repo = pa["repo"].as_str().unwrap();
    let a_device = pa["device"].as_str().unwrap();
    let b_device = pb["device"].as_str().unwrap();
    // An invalid event authored as A's replica, delivered straight to B's worker.
    let author_dir = tempfile::tempdir().unwrap();
    let author = Store::open(author_dir.path(), Some(repo)).unwrap();
    let bad = foreign_event(&author, a_device, invalid_kind_changes("bad.txt")).unwrap();
    let known: BTreeSet<String> = rpc(&pb, json!({"op":"events"}))
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["branch"]["shared"] == true)
        .map(|e| e["id"].as_str().unwrap().to_owned())
        .collect();
    let route = legacy_route(&id(), repo, b_device, a_device);
    let page = legacy_page(repo, a_device, b_device, &[bad], &known);
    rpc(
        &pb,
        json!({"op":"published-exchange","route":route,"page":page,"metadata_only":true}),
    );
    let status = rpc(&pb, json!({"op":"status"}));
    s.check(
        status["incoming_quarantined"] == 1,
        format!("one quarantined row: {}", status["incoming_quarantined"]),
    );
    s.measure_from_now();
    write_file(&pa, "two.txt", b"two");
    let unrelated = wait_until(s.remaining(), || has_file(&pb, "two.txt", b"two"));
    s.check(unrelated, "unrelated history activates");
    let watch = Stopwatch::start();
    let caught_up = wait_until(s.remaining(), || {
        b.raw_call(json!({"op":"peers"})).is_ok_and(|peers| {
            peers[0]["lanes"][0]["repositories"][repo]["metadata_caught_up"] == true
        })
    });
    s.metric("caught_up_ms", watch.ms(), "ms");
    if !caught_up {
        s.timeout("metadata_caught_up never true with one quarantined event");
    }
    s.finish();
}

/// An injected `SQLITE_BUSY` at commit is retried and committed, never quarantined.
/// The fault point (`core.commit.busy`, util::fault from WP0.4) is wired by WP1.6;
/// on builds without it no fault fires and the scenario only checks the commit.
#[test]
#[ignore = "WP1.6"]
fn transient_retry() {
    let mut s = Scenario::new(FILE, "transient_retry");
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = Store::open(&dir.path().join("a"), Some(&repo)).unwrap();
    let mut b = Store::open(&dir.path().join("b"), Some(&repo)).unwrap();
    let allowed = devices([&a, &b]);
    a.create("busy.txt", "file").unwrap();
    let bundle = missing_bundle(&a, &b).unwrap();
    let ids: Vec<String> = bundle.events.iter().map(|e| e.id.clone()).collect();
    // SAFETY: qualification scenarios run with --test-threads=1.
    unsafe {
        std::env::set_var("TKFS_FAULT_POINT", "core.commit.busy");
        std::env::set_var("TKFS_FAULT_ACTION", "fail");
        std::env::set_var("TKFS_FAULT_COUNT", "1");
    }
    let first = b.receive(bundle, &allowed);
    unsafe {
        std::env::remove_var("TKFS_FAULT_POINT");
        std::env::remove_var("TKFS_FAULT_ACTION");
        std::env::remove_var("TKFS_FAULT_COUNT");
    }
    s.note(format!(
        "first receive: {:?}",
        first.as_ref().map(|v| v.len())
    ));
    let quarantined: i64 =
        b.db.query_row(
            "SELECT count(*) FROM incoming_events WHERE error IS NOT NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    s.check(quarantined == 0, "a transient error is never quarantined");
    // The retry: reopening drains, as the activator's retry would.
    let root = b.root.clone();
    drop(b);
    let b = Store::open_existing(&root).unwrap();
    let committed = b.shared_event_inventory().unwrap();
    s.check(
        ids.iter().all(|id| committed.contains(id)),
        "event committed after the retry",
    );
    s.metric("quarantined", quarantined as f64, "count");
    s.finish();
}

/// A local create on a branch whose only conflict is resolved takes the fast path.
#[test]
#[ignore = "WP1.6"]
fn local_create_after_resolved_conflict() {
    let mut s = Scenario::new(FILE, "local_create_after_resolved_conflict");
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = Store::open(&dir.path().join("a"), Some(&repo)).unwrap();
    linear_history(&mut a, scaled(2000)).unwrap();
    let file = a.create("conflicted.txt", "file").unwrap();
    let (mut a, mut b) = clone_replica(a, &dir.path().join("b")).unwrap();
    let allowed = devices([&a, &b]);
    let base = file.revisions.get("content").cloned();
    a.write_revision(&file.id, base.clone(), b"from A").unwrap();
    b.write_revision(&file.id, base, b"from B").unwrap();
    sync_into(&b, &mut a, &allowed).unwrap();
    let conflict = a
        .conflicts()
        .unwrap()
        .into_iter()
        .find(|c| c.resolved_by.is_none())
        .expect("a content conflict");
    let winner = conflict.alternatives[0].revision.clone();
    a.resolve(&[conflict.id], &winner).unwrap();
    s.check(
        a.conflicts()
            .unwrap()
            .iter()
            .all(|c| c.resolved_by.is_some()),
        "only resolved conflicts remain",
    );
    s.measure_from_now();
    let mut samples = vec![];
    for i in 0..20 {
        if s.over_budget() {
            s.timeout(format!("{i} of 20 creates in budget"));
            break;
        }
        let (r, ms) = time_ms(|| a.create(&format!("after-resolve-{i:02}.txt"), "file"));
        r.unwrap();
        samples.push(ms);
    }
    let summary = summarize(&samples);
    s.metric_with("p90_ms", summary.p90, "ms", summary.json());
    s.metric("p50_ms", summary.p50, "ms");
    s.finish();
}
