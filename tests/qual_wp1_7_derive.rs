//! Qualification scenarios for WP1.7 (append-mode conflict derivation).
//! REVAMP_PLAN §14.6; floors in §15 and `tests/qualification-floors.json`.
//!
//! Created by WP0.5 against the 6298d0b baseline, `#[ignore = "WP1.7"]`.
//! WP1.7 un-ignores and extends them (also run with `TKFS_CONFLICT_MODE=verify`).
//! `cargo test --release --test qual_wp1_7_derive -- --ignored --test-threads=1 --nocapture`
mod support;

use serde_json::json;
use support::*;
use tkfs::core::{Store, id};

const FILE: &str = "qual_wp1_7_derive";

/// A streams `n` creates while B has one unseen local edit: every event is a
/// two-way (slow path) activation on B.
fn two_way_backlog_at(name: &str, n: usize) {
    let mut s = Scenario::new(FILE, name);
    let n = scaled(n);
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = Store::open(&dir.path().join("a"), Some(&repo)).unwrap();
    let mut b = Store::open(&dir.path().join("b"), Some(&repo)).unwrap();
    let backlog = two_way_backlog(&mut a, &mut b, n).unwrap();
    s.measure_from_now();
    let delivery = deliver_paced(&mut b, &backlog.bundle, &backlog.allowed, s.deadline()).unwrap();
    s.metric_with(
        "per_event_ms",
        delivery.mean_per_event_ms(),
        "ms",
        json!({"events": n, "activated": delivery.delivered,
            "last_per_event_ms": delivery.per_event_ms.last()}),
    );
    if delivery.timed_out {
        s.timeout(format!("{} of {n} events activated", delivery.delivered));
    } else {
        s.check(
            b.lookup(&format!("backlog-{:06}.txt", n - 1)).is_ok(),
            "backlog visible on B",
        );
        s.check(
            b.events()
                .unwrap()
                .iter()
                .any(|e| e.id == backlog.local_edit),
            "B's local edit kept",
        );
        s.check(
            b.conflicts()
                .unwrap()
                .iter()
                .all(|c| c.resolved_by.is_some()),
            "no spurious conflicts",
        );
    }
    s.finish();
}

#[test]
#[ignore = "WP1.7"]
fn two_way_backlog_2k() {
    two_way_backlog_at("two_way_backlog_2k", 2000);
}

#[test]
#[ignore = "WP1.7"]
fn two_way_backlog_6400() {
    two_way_backlog_at("two_way_backlog_6400", 6400);
}

/// 50 namespace operations from A, concurrent with one local namespace
/// operation on B, each activated on B over a 5K-event history.
#[test]
#[ignore = "WP1.7"]
fn concurrent_namespace_ops_5k() {
    let mut s = Scenario::new(FILE, "concurrent_namespace_ops_5k");
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = Store::open(&dir.path().join("a"), Some(&repo)).unwrap();
    linear_history(&mut a, scaled(5000)).unwrap();
    let (mut a, mut b) = clone_replica(a, &dir.path().join("b")).unwrap();
    let allowed = devices([&a, &b]);
    b.create("b-local-dir", "directory").unwrap();
    let before = b.shared_event_inventory().unwrap();
    batched(&mut a, |a| {
        for i in 0..25 {
            a.create(&format!("ns-dir-{i:02}"), "directory")?;
            let file = a.lookup(&format!("lin-{i:06}.txt"))?;
            a.rename(&file.id, &format!("ns-dir-{i:02}/moved-{i:02}.txt"), false)?;
        }
        Ok(())
    })
    .unwrap();
    let mut bundle = a.shared_bundle(&before).unwrap();
    causal_sort(&mut bundle.events);
    s.check(bundle.events.len() == 50, "50 namespace events");
    s.measure_from_now();
    let mut samples = vec![];
    for event in bundle.events.clone() {
        if s.over_budget() {
            s.timeout(format!("{} of 50 operations in budget", samples.len()));
            break;
        }
        let page = tkfs::core::Bundle {
            repo: bundle.repo.clone(),
            branches: vec![],
            events: vec![event],
            objects: Default::default(),
        };
        let (r, ms) = time_ms(|| b.receive(page, &allowed));
        r.unwrap();
        samples.push(ms);
    }
    let summary = summarize(&samples);
    s.metric_with("max_op_ms", summary.max, "ms", summary.json());
    s.metric("p50_op_ms", summary.p50, "ms");
    if samples.len() == 50 {
        s.check(b.lookup("ns-dir-24/moved-24.txt").is_ok(), "moves visible");
        s.check(b.lookup("b-local-dir").is_ok(), "local op kept");
    }
    s.finish();
}
