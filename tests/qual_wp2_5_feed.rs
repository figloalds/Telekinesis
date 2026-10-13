//! Qualification scenarios for WP2.5 (metadata feed, format 7, worker side).
//! REVAMP_PLAN §14.6, §6.3.
//!
//! Created by WP0.5 against the 6298d0b baseline, `#[ignore = "WP2.5"]`.
//! Two real worker processes are driven like a dialer and an acceptor through
//! the worker JSON ops `feed-offer` → `feed-exchange` → `feed-apply` (§6.1,
//! §6.3.5). The baseline has no feed ops: where §15 lists a baseline number the
//! scenario also measures the legacy v6 path (`published-*`) for the record.
//! `cargo test --release --test qual_wp2_5_feed -- --ignored --test-threads=1 --nocapture`
mod support;

use serde_json::{Value, json};
use std::{path::Path, time::Duration};
use support::*;
use tkfs::core::{Store, id};

const FILE: &str = "qual_wp2_5_feed";

/// The legacy path's error: its code when it has one, else the first line.
fn legacy_error(e: &anyhow::Error) -> String {
    let text = format!("{e:#}");
    match error_code(&text).as_str() {
        "TEXT" => text
            .lines()
            .next()
            .unwrap_or("")
            .chars()
            .take(200)
            .collect(),
        code => code.to_owned(),
    }
}

/// Activation pause for the receiving worker (§4.4 activator; name owned by WP2.3).
const ACTIVATOR_PAUSE: [(&str, &str); 2] = [
    ("TKFS_FAULT_POINT", "worker.activator.slice"),
    ("TKFS_FAULT_ACTION", "hang"),
];

struct Pair {
    _dir: tempfile::TempDir,
    a: WorkerProc,
    b: WorkerProc,
    repo: String,
    a_device: String,
    b_device: String,
}
impl Pair {
    /// B dials A.
    fn link(&self) -> WorkerLink<'_> {
        WorkerLink::new(&self.b, &self.a, &self.repo, &self.b_device, &self.a_device)
    }
}

/// A with `events` linear creates; B empty (`clone` = false) or a clone of A.
fn pair(events: usize, clone: bool, b_env: &[(&str, &str)]) -> Pair {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let a_state = dir.path().join("a");
    let b_state = dir.path().join("b");
    let mut a = Store::initialize(&a_state, &repo, &id()).unwrap();
    for start in (0..events).step_by(10_000) {
        let n = (events - start).min(10_000);
        linear_history_named(&mut a, &format!("feed{start:06}"), n).unwrap();
    }
    let (a_device, b_device) = if clone {
        let (a, b) = clone_replica(a, &b_state).unwrap();
        (a.device.clone(), b.device.clone())
    } else {
        let b = Store::initialize(&b_state, &repo, &id()).unwrap();
        (a.device.clone(), b.device.clone())
    };
    spawn_pair(dir, &a_state, &b_state, repo, a_device, b_device, b_env)
}

fn spawn_pair(
    dir: tempfile::TempDir,
    a_state: &Path,
    b_state: &Path,
    repo: String,
    a_device: String,
    b_device: String,
    b_env: &[(&str, &str)],
) -> Pair {
    let mut a = WorkerProc::spawn(a_state, &[], &[]).unwrap();
    let mut b = WorkerProc::spawn(b_state, &[], b_env).unwrap();
    a.wait_ready(Duration::from_secs(600)).unwrap();
    b.wait_ready(Duration::from_secs(600)).unwrap();
    Pair {
        _dir: dir,
        a,
        b,
        repo,
        a_device,
        b_device,
    }
}

/// Run feed rounds until B's cursor reaches A's tip (or the deadline). Returns
/// rounds, or the error that stopped them.
fn feed_until_caught_up(link: &WorkerLink, s: &Scenario) -> anyhow::Result<(usize, Value)> {
    let mut rounds = 0;
    loop {
        let (applied, _, _) = link.feed_round()?;
        rounds += 1;
        let summary = &applied["summary"];
        let at_tip = summary["cursor_in"].as_i64().is_some()
            && summary["cursor_in"] == summary["remote_tip"]
            && summary["received_events"] == 0;
        if at_tip || s.over_budget() {
            return Ok((rounds, applied));
        }
    }
}

/// A 150K-event feed has no caps (no `INVENTORY_TOO_LARGE`).
#[test]
#[ignore = "WP2.5"]
fn feed_150k_events() {
    let mut s = Scenario::new(FILE, "feed_150k_events");
    let n = scaled(150_000);
    let p = pair(n, false, &[]);
    s.measure_from_now();
    let link = p.link();
    match feed_until_caught_up(&link, &s) {
        Ok((rounds, applied)) => {
            let done = applied["summary"]["cursor_in"].as_i64() == Some(n as i64);
            s.metric_with(
                "completed",
                if done { 1.0 } else { 0.0 },
                "bool",
                json!({"rounds": rounds, "summary": applied["summary"]}),
            );
            s.metric(
                "events_per_s",
                applied["summary"]["cursor_in"].as_f64().unwrap_or(0.0)
                    / s.started.elapsed().as_secs_f64(),
                "events/s",
            );
            if !done {
                s.timeout("feed not at tip");
            }
        }
        Err(e) if is_unsupported(&e) => {
            let legacy = link.metadata_round();
            s.metric_with(
                "completed",
                0.0,
                "bool",
                json!({"legacy_path": legacy.as_ref().map(|_| "ok".to_string())
                    .unwrap_or_else(legacy_error)}),
            );
            s.unsupported(format!("feed ops: {}", error_code(&format!("{e:#}"))));
        }
        Err(e) => {
            s.metric("completed", 0.0, "bool");
            s.check(false, format!("feed error: {e:#}"));
        }
    }
    s.finish();
}

/// An idle exchange between replicas with 50K events costs ≤ 10 ms CPU per side
/// and ≤ 4 KiB.
#[test]
#[ignore = "WP2.5"]
fn idle_exchange_50k() {
    let mut s = Scenario::new(FILE, "idle_exchange_50k");
    let p = pair(scaled(50_000), true, &[]);
    let link = p.link();
    let cpu = |w: &WorkerProc| w.pid().and_then(process_cpu_ms).unwrap_or(f64::NAN);
    let feed = match feed_until_caught_up(&link, &s) {
        Ok(_) => true,
        Err(e) if is_unsupported(&e) => {
            s.unsupported(format!("feed ops: {}", error_code(&format!("{e:#}"))));
            // Baseline record: the legacy idle round (§15 baseline 1.13 s, 251 KB @6.4K).
            for _ in 0..3 {
                if let Err(e) = link.metadata_round() {
                    s.check(false, format!("legacy warm-up: {e:#}"));
                    break;
                }
            }
            false
        }
        Err(e) => {
            s.check(false, format!("feed error: {e:#}"));
            return s.finish();
        }
    };
    let path = if feed { "feed" } else { "legacy" };
    let round = || -> anyhow::Result<usize> {
        if feed {
            link.feed_round().map(|(_, _, bytes)| bytes)
        } else {
            link.metadata_round().map(|(_, bytes)| bytes)
        }
    };
    s.measure_from_now();
    let rounds = 10;
    let (a0, b0) = (cpu(&p.a), cpu(&p.b));
    let mut bytes = vec![];
    for _ in 0..rounds {
        match round() {
            Ok(n) => bytes.push(n as f64),
            Err(e) => {
                s.check(false, format!("idle round: {e:#}"));
                break;
            }
        }
        if s.over_budget() {
            break;
        }
    }
    let (a1, b1) = (cpu(&p.a), cpu(&p.b));
    let per_side = ((a1 - a0) + (b1 - b0)) / 2.0 / bytes.len().max(1) as f64;
    let summary = summarize(&bytes);
    s.metric_with(
        "cpu_ms_per_side",
        per_side,
        "ms",
        json!({"path": path, "rounds": bytes.len(), "acceptor_cpu_ms": a1 - a0,
            "dialer_cpu_ms": b1 - b0}),
    );
    s.metric_with("bytes", summary.p50, "bytes", json!({"path": path}));
    s.finish();
}

/// Staged (not yet activated) events are never sent again: the second exchange
/// carries no payload entries.
#[test]
#[ignore = "WP2.5"]
fn staged_events_not_resent() {
    let mut s = Scenario::new(FILE, "staged_events_not_resent");
    let p = pair(scaled(300), false, &ACTIVATOR_PAUSE);
    let link = p.link();
    let first = link.feed_round();
    let Ok((first, _, _)) = first else {
        let e = first.err().unwrap();
        s.unsupported(format!("feed ops: {}", error_code(&format!("{e:#}"))));
        return s.finish();
    };
    s.note(format!("first round: {}", first["summary"]));
    let (_, exchanged, bytes) = link.feed_round().unwrap();
    let resent = exchanged["reply"]["page"]["entries"]
        .as_array()
        .map(|entries| entries.iter().filter(|e| !e["event"].is_null()).count())
        .unwrap_or(0);
    s.metric_with(
        "resent_entries",
        resent as f64,
        "entries",
        json!({"bytes": bytes}),
    );
    s.check(resent == 0, format!("{resent} staged events re-sent"));
    s.finish();
}

/// Restoring B from an older snapshot makes A re-send the withdrawn events.
#[test]
#[ignore = "WP2.5"]
fn peer_restore() {
    let mut s = Scenario::new(FILE, "peer_restore");
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let (a_state, b_state, snapshot) = (
        dir.path().join("a"),
        dir.path().join("b"),
        dir.path().join("b-snapshot"),
    );
    let (a_device, b_device) = {
        let mut a = Store::initialize(&a_state, &repo, &id()).unwrap();
        linear_history(&mut a, 100).unwrap();
        let b = Store::initialize(&b_state, &repo, &id()).unwrap();
        (a.device.clone(), b.device.clone())
    };
    let mut wa = WorkerProc::spawn(&a_state, &[], &[]).unwrap();
    let mut wb = WorkerProc::spawn(&b_state, &[], &[]).unwrap();
    wa.wait_ready(Duration::from_secs(120)).unwrap();
    wb.wait_ready(Duration::from_secs(120)).unwrap();
    {
        let link = WorkerLink::new(&wb, &wa, &repo, &b_device, &a_device);
        if let Err(e) = feed_until_caught_up(&link, &s) {
            s.unsupported(format!("feed ops: {}", error_code(&format!("{e:#}"))));
            return s.finish();
        }
    }
    wb.stop().unwrap();
    copy_dir(&b_state, &snapshot).unwrap();
    let mut wb = WorkerProc::spawn(&b_state, &[], &[]).unwrap();
    wb.wait_ready(Duration::from_secs(120)).unwrap();
    for i in 0..50 {
        wa.rpc(json!({"op":"mkdir","path":format!("after-snapshot-{i:02}")}))
            .unwrap();
    }
    {
        let link = WorkerLink::new(&wb, &wa, &repo, &b_device, &a_device);
        feed_until_caught_up(&link, &s).unwrap();
    }
    wb.stop().unwrap();
    std::fs::remove_dir_all(&b_state).unwrap();
    copy_dir(&snapshot, &b_state).unwrap();
    let wb = {
        let mut w = WorkerProc::spawn(&b_state, &[], &[]).unwrap();
        w.wait_ready(Duration::from_secs(120)).unwrap();
        w
    };
    s.measure_from_now();
    let link = WorkerLink::new(&wb, &wa, &repo, &b_device, &a_device);
    let result = feed_until_caught_up(&link, &s);
    s.check(
        result.is_ok(),
        format!("exchange after restore: {result:?}"),
    );
    let restored = wait_until(s.remaining(), || {
        wb.rpc(json!({"op":"state"})).is_ok_and(|st| {
            st["entries"]
                .as_object()
                .is_some_and(|e| e.values().any(|e| e["name"] == "after-snapshot-49"))
        })
    });
    s.metric(
        "restore_ms",
        s.started.elapsed().as_secs_f64() * 1000.0,
        "ms",
    );
    s.check(restored, "withdrawn events re-sent after the restore");
    s.finish();
}

/// A multi-MiB event (above the legacy 349,525-byte cliff) syncs through `need`.
#[test]
#[ignore = "WP2.5"]
fn large_event_8mib() {
    let mut s = Scenario::new(FILE, "large_event_8mib");
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let (a_state, b_state) = (dir.path().join("a"), dir.path().join("b"));
    let (a_device, b_device, size) = {
        let mut a = Store::initialize(&a_state, &repo, &id()).unwrap();
        let event = large_event(&mut a, "big", 2400, 200).unwrap();
        let size: i64 =
            a.db.query_row(
                "SELECT length(payload) FROM events WHERE id=?",
                [&event],
                |r| r.get(0),
            )
            .unwrap();
        let b = Store::initialize(&b_state, &repo, &id()).unwrap();
        (a.device.clone(), b.device.clone(), size)
    };
    s.note(format!("event payload {size} bytes"));
    let p = spawn_pair(dir, &a_state, &b_state, repo, a_device, b_device, &[]);
    s.measure_from_now();
    let link = p.link();
    match feed_until_caught_up(&link, &s) {
        Ok(_) => {
            let arrived = wait_until(s.remaining(), || {
                p.b.rpc(json!({"op":"state"}))
                    .is_ok_and(|st| st["entries"].as_object().is_some_and(|e| e.len() > 2000))
            });
            s.metric_with(
                "completed",
                if arrived { 1.0 } else { 0.0 },
                "bool",
                json!({"event_bytes": size}),
            );
        }
        Err(e) if is_unsupported(&e) => {
            let legacy = link.metadata_round();
            s.metric_with(
                "completed",
                0.0,
                "bool",
                json!({"event_bytes": size, "legacy_path": legacy.as_ref().map(|_| "ok".to_string())
                    .unwrap_or_else(legacy_error)}),
            );
            s.unsupported(format!("feed ops: {}", error_code(&format!("{e:#}"))));
        }
        Err(e) => {
            s.metric("completed", 0.0, "bool");
            s.check(false, format!("feed error: {e:#}"));
        }
    }
    s.finish();
}
