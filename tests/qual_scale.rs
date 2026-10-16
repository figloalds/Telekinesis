//! Scale qualification (WP4.2 list, REVAMP_PLAN §14 WP4.2 and §15): every
//! floor re-checked at daemon level in release builds.
//!
//! Created by WP0.5 against the 6298d0b baseline, `#[ignore = "WP4.2"]`;
//! owned and run by WP4.2 (the scenarios stay `#[ignore]`: they are release
//! qualification runs, `scripts/qualify.ps1 -All`).
//! `cargo test --release --test qual_scale -- --ignored --test-threads=1 --nocapture`
//!
//! The 150K feed reads its progress from the worker's status cell, not from
//! the whole `state` (see `feed_150k`); `TKFS_QUAL_BUDGET_S` must cover it
//! (`qualify.ps1 -All` passes `-BudgetS 2400`).
mod support;

use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use support::*;

const FILE: &str = "qual_scale";
const SETUP: Duration = Duration::from_secs(1800);

fn empty() -> String {
    tkfs::core::hash(b"")
}

/// Committed event count on a folder's worker (`events` op), retried while the
/// worker answers busy. An unreadable count is an error, never 0: a 0 would
/// skew the activation base (and wrap the unsigned arithmetic).
fn event_count(folder: &Value) -> anyhow::Result<usize> {
    retry_busy(Duration::from_secs(60), || {
        try_rpc(folder, json!({"op":"events"}))?
            .as_array()
            .map(Vec::len)
            .ok_or_else(|| anyhow::anyhow!("INVALID_EVENTS_REPLY"))
    })
}

/// Events activated on the receiver since `base`, for `per_event_ms`. A count
/// that cannot be read fails the scenario (and reports 1, so the metric stays
/// an upper bound instead of a fabricated rate).
fn activated_since(s: &mut Scenario, folder: &Value, base: usize) -> usize {
    match event_count(folder) {
        Ok(count) => count.saturating_sub(base).max(1),
        Err(e) => {
            s.check(false, format!("receiver event count unreadable: {e:#}"));
            1
        }
    }
}

/// Remote linear activation between daemons: A's folder holds `n` creates; B receives.
fn remote_linear(name: &str, n: usize) {
    let mut s = Scenario::new(FILE, name);
    let n = scaled(n);
    let a = Node::new();
    let b = Node::new();
    let pa = a.create("Linear", true);
    a.with_stopped_folder(&pa, SETUP, |st| linear_history(st, n).map(|_| ()))
        .unwrap();
    // Events of the folder before the history (its own setup), which the
    // receiver also activates; `checked_sub` guards the unsigned arithmetic.
    let total = event_count(&pa).expect("source event count");
    let Some(base) = total.checked_sub(n) else {
        panic!("source holds {total} events, fewer than the {n} written");
    };
    b.pair(&a, &a.code());
    s.measure_from_now();
    let pb = b.receive(&a, &pa);
    let last = format!("lin-{:06}.txt", n - 1);
    let done = wait_until(s.remaining(), || has_hash(&pb, &last, &empty()));
    let activated = activated_since(&mut s, &pb, base);
    s.metric_with(
        "per_event_ms",
        s.started.elapsed().as_secs_f64() * 1000.0 / activated as f64,
        "ms",
        json!({"events": n, "activated": activated}),
    );
    if !done {
        s.timeout(format!("{activated} of {n} events activated"));
    }
    s.finish();
}

#[test]
#[ignore = "WP4.2"]
fn remote_linear_activation_2k() {
    remote_linear("remote_linear_activation_2k", 2000);
}

#[test]
#[ignore = "WP4.2"]
fn remote_linear_activation_6400() {
    remote_linear("remote_linear_activation_6400", 6400);
}

/// Two-way backlog between daemons: 2K remote creates while B has one unseen edit.
#[test]
#[ignore = "WP4.2"]
fn two_way_backlog() {
    let mut s = Scenario::new(FILE, "two_way_backlog");
    let n = scaled(2000);
    let a = Node::new();
    let b = Node::new();
    let pa = a.create("Backlog", true);
    write_file(&pa, "shared.txt", b"seed");
    b.pair(&a, &a.code());
    let pb = b.receive(&a, &pa);
    b.wait_for("seed", Duration::from_secs(60), || {
        has_file(&pb, "shared.txt", b"seed")
    });
    b.with_stopped_folder(&pb, SETUP, |st| {
        let e = st.lookup("shared.txt")?;
        st.write_revision(&e.id, e.revisions.get("content").cloned(), b"edit on B")?;
        Ok(())
    })
    .unwrap();
    let base = event_count(&pb).expect("receiver event count before the backlog");
    a.with_stopped_folder(&pa, SETUP, |st| {
        linear_history_named(st, "backlog", n).map(|_| ())
    })
    .unwrap();
    s.measure_from_now();
    let last = format!("backlog-{:06}.txt", n - 1);
    let done = wait_until(s.remaining(), || has_hash(&pb, &last, &empty()));
    let activated = activated_since(&mut s, &pb, base);
    s.metric_with(
        "per_event_ms",
        s.started.elapsed().as_secs_f64() * 1000.0 / activated as f64,
        "ms",
        json!({"events": n, "activated": activated}),
    );
    if !done {
        s.timeout(format!("{activated} of {n} events activated"));
    }
    s.finish();
}

/// A 150K-event folder synchronizes between daemons.
#[test]
#[ignore = "WP4.2"]
fn feed_150k() {
    let mut s = Scenario::new(FILE, "feed_150k");
    let n = scaled(150_000);
    let a = Node::new();
    let b = Node::new();
    let pa = a.create("Feed 150K", true);
    a.with_stopped_folder(&pa, SETUP, |st| {
        for start in (0..n).step_by(10_000) {
            linear_history_named(st, &format!("f{start:06}"), (n - start).min(10_000))?;
        }
        Ok(())
    })
    .unwrap();
    // Every event of A's folder (its own setup events included).
    let total = event_count(&pa).expect("source event count") as u64;
    b.pair(&a, &a.code());
    s.measure_from_now();
    let pb = b.receive(&a, &pa);
    let last_start = (n - 1) / 10_000 * 10_000;
    let last = format!("f{last_start:06}-{:06}.txt", n - 1 - last_start);
    // WP4.2 (as WP3.INT's `feed_150k_daemons`): progress is read from B's
    // worker `status` (its status cell; no projection is read). `has_hash`
    // asks for the whole `state`: about 70 MB of JSON at 150K entries, over
    // the RPC's 64 MiB message limit, so it never held, and every poll made
    // the worker load the whole projection, which slowed what it measured.
    let mut seen = 0;
    let activated = wait_until(s.remaining(), || {
        seen = try_rpc(&pb, json!({"op":"status"}))
            .ok()
            .and_then(|status| status["journal"]["events"].as_u64())
            .unwrap_or(seen);
        thread::sleep(Duration::from_millis(200));
        seen >= total
    });
    let seconds = s.started.elapsed().as_secs_f64();
    // Then the last event's entry, read from B's store (folder stopped): it
    // must be a live file with the (empty) content.
    let entry = activated.then(|| {
        b.with_stopped_folder(&pb, SETUP, |st| Ok(st.lookup(&last).ok()))
            .unwrap_or_else(|error| panic!("cannot read B's store: {error:#}"))
    });
    let present = entry.as_ref().is_some_and(|entry| {
        entry
            .as_ref()
            .is_some_and(|e| e.alive && e.content.as_deref() == Some(empty().as_str()))
    });
    let done = activated && present;
    s.metric_with(
        "completed",
        if done { 1.0 } else { 0.0 },
        "bool",
        json!({"events": n, "total": total, "activated": seen, "seconds": seconds}),
    );
    s.metric("events_per_s", seen as f64 / seconds, "events/s");
    if !activated {
        s.timeout(format!("{seen} of {total} events activated"));
    } else {
        s.check(
            present,
            format!("{last} is not an empty live file in B's store"),
        );
    }
    s.finish();
}

/// 10K × 4 KiB files between daemons ≥ 300 files/s.
#[test]
#[ignore = "WP4.2"]
fn tiny_files_10k() {
    let mut s = Scenario::new(FILE, "tiny_files_10k");
    let count = scaled(10_000);
    let a = Node::new();
    let b = Node::new();
    let pa = a.create("Tiny", true);
    a.with_stopped_folder(&pa, SETUP, |st| {
        small_objects_in(st, "tiny", count, 4096, 71)
    })
    .unwrap();
    b.pair(&a, &a.code());
    s.measure_from_now();
    let pb = b.receive(&a, &pa);
    let done = wait_until(s.remaining(), || content_complete(&pb, count as u64));
    let available = content_status(&pb)["shared_available"]
        .as_f64()
        .unwrap_or(0.0);
    s.metric_with(
        "files_per_s",
        available / s.started.elapsed().as_secs_f64(),
        "files/s",
        json!({"files": count, "available": available}),
    );
    if !done {
        s.timeout("files missing");
    }
    s.finish();
}

/// A 1 GiB object between daemons on loopback ≥ 40 MiB/s.
#[test]
#[ignore = "WP4.2"]
fn one_gib_object() {
    let mut s = Scenario::new(FILE, "one_gib_object");
    let size = scaled(1024) as u64 * 1024 * 1024;
    let a = Node::new();
    let b = Node::new();
    let pa = a.create("Gigabyte", true);
    let hash = a
        .with_stopped_folder(&pa, SETUP, |st| large_object_in(st, "gib.bin", size, 72))
        .unwrap();
    b.pair(&a, &a.code());
    s.measure_from_now();
    let pb = b.receive(&a, &pa);
    let done = wait_until(s.remaining(), || {
        has_hash(&pb, "gib.bin", &hash) && content_complete(&pb, 1)
    });
    let secs = s.started.elapsed().as_secs_f64();
    let moved = if done {
        size as f64
    } else {
        content_status(&pb)["partial_bytes"].as_f64().unwrap_or(0.0)
    };
    s.metric_with(
        "mib_per_s",
        moved / 1048576.0 / secs,
        "MiB/s",
        json!({"bytes": size, "seconds": secs}),
    );
    if !done {
        s.timeout("object not transferred");
    }
    s.finish();
}

/// Restart to Ready (`running`) with 50K stored objects < 10 s.
#[test]
#[ignore = "WP4.2"]
fn startup_ready_50k_objects() {
    let mut s = Scenario::new(FILE, "startup_ready_50k_objects");
    let a = Node::new();
    let pa = a.create("Fifty thousand", true);
    a.with_stopped_folder(&pa, SETUP, |st| {
        small_objects_in(st, "objects", scaled(50_000), 512, 73).map(|_| ())
    })
    .unwrap();
    let state = state_id(&pa);
    a.stop_folder_exited(&pa, STOP_EXIT_TIMEOUT).unwrap();
    s.measure_from_now();
    let _ = a.raw_call(json!({"op":"start","state":state}));
    let ready = wait_until(s.remaining(), || {
        a.try_inspect(&state).is_ok_and(|p| p["running"] == true)
    });
    s.metric(
        "ready_ms",
        if ready {
            s.started.elapsed().as_secs_f64() * 1000.0
        } else {
            f64::NAN
        },
        "ms",
    );
    if !ready {
        s.timeout("folder not running");
    }
    s.finish();
}

/// Management API p99 < 100 ms while six folders churn through Start/Stop.
#[test]
#[ignore = "WP4.2"]
fn management_api_p99_under_churn() {
    let mut s = Scenario::new(FILE, "management_api_p99_under_churn");
    let a = Node::new();
    let folders: Vec<String> = (0..6)
        .map(|i| state_id(&a.create(&format!("Churn {i}"), true)))
        .collect();
    let stop = Arc::new(AtomicBool::new(false));
    let churners: Vec<_> = folders
        .into_iter()
        .map(|state| {
            let (stop, control) = (stop.clone(), a.control());
            let counts = Arc::new(CallCounts::default());
            let thread_counts = counts.clone();
            let handle = thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    for op in ["stop", "start"] {
                        thread_counts.record(&control.call(&json!({"op":op,"state":state})));
                    }
                }
            });
            (handle, counts)
        })
        .collect();
    s.measure_from_now();
    let mut samples = vec![];
    for _ in 0..300 {
        if s.over_budget() {
            break;
        }
        let action = if samples.len() % 2 == 0 {
            json!({"op":"list"})
        } else {
            json!({"op":"peers"})
        };
        let (r, ms) = time_ms(|| a.raw_call(action));
        s.check(r.is_ok(), "read action ok");
        samples.push(ms);
        thread::sleep(Duration::from_millis(20));
    }
    stop.store(true, Ordering::Relaxed);
    let mut churn = vec![];
    for (i, (handle, counts)) in churners.into_iter().enumerate() {
        handle.join().unwrap();
        // A churner whose calls never reach the daemon (for example rejected
        // upgrades) would let the floor pass without any churn.
        let (ok, _, _) = counts.get();
        s.check(
            ok >= 2,
            format!("churner {i} made progress: {}", counts.json()),
        );
        churn.push(counts.json());
    }
    let summary = summarize(&samples);
    let mut extra = summary.json();
    extra["churn"] = json!(churn);
    s.metric_with("list_p99_ms", summary.p99, "ms", extra);
    s.finish();
}

/// Idle worker CPU ≤ 1 % with 5K events and 10K objects, synchronized and idle.
#[test]
#[ignore = "WP4.2"]
fn idle_worker_cpu() {
    let mut s = Scenario::new(FILE, "idle_worker_cpu");
    // The production default anti-entropy interval (the harness default is 50 ms).
    let a = Node::start(NodeConfig::default().interval(500));
    let b = Node::start(NodeConfig::default().interval(500));
    let (pa, pb) = synced_replicas(&a, &b, &a.endpoint(), "Idle", |st| {
        linear_history(st, scaled(5000))?;
        small_objects_in(st, "objects", scaled(10_000), 256, 74).map(|_| ())
    })
    .unwrap();
    let repo = pa["repo"].as_str().unwrap().to_owned();
    let settled = wait_until(SETUP, || {
        b.raw_call(json!({"op":"peers"}))
            .is_ok_and(|p| p[0]["lanes"][0]["repositories"][&repo]["caught_up"] == true)
    });
    s.check(settled, "replicas idle and caught up");
    thread::sleep(Duration::from_secs(5));
    let pids: Vec<u32> = a
        .worker_pids(&pa)
        .into_iter()
        .chain(b.worker_pids(&pb))
        .collect();
    s.check(pids.len() >= 2, format!("worker pids: {pids:?}"));
    s.measure_from_now();
    let before: Vec<Option<f64>> = pids.iter().map(|p| process_cpu_ms(*p)).collect();
    let window = Duration::from_secs(30).min(s.budget);
    thread::sleep(window);
    let after: Vec<Option<f64>> = pids.iter().map(|p| process_cpu_ms(*p)).collect();
    // Every worker must be measured at both ends: a pid that exited, restarted
    // or denied access is a failed measurement (NaN fails the floor), never a
    // silently dropped sample (f64::max ignores NaN).
    let mut percent = if pids.is_empty() { f64::NAN } else { 0.0 };
    for (pid, (b, a)) in pids.iter().zip(before.iter().zip(&after)) {
        match (b, a) {
            (Some(b), Some(a)) => {
                percent = percent.max((a - b) / (window.as_secs_f64() * 1000.0) * 100.0);
            }
            _ => {
                s.check(
                    false,
                    format!("worker {pid} CPU unreadable: {b:?} -> {a:?}"),
                );
                percent = f64::NAN;
                break;
            }
        }
    }
    s.metric_with(
        "cpu_percent",
        percent,
        "%",
        json!({"pids": pids, "window_s": window.as_secs()}),
    );
    s.finish();
}
