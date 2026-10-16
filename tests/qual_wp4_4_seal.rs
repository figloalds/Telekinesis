//! Qualification scenarios for WP4.4 (seal path and inbox staging cost),
//! REVAMP_PLAN §14.5a. Release only (`#[ignore]`, the qualify convention):
//! `cargo test --release --test qual_wp4_4_seal -- --ignored --test-threads=1 --nocapture`
//!
//! - `catch_up_150k_seals`: a 150K-event remote catch-up (linear creates,
//!   staged in 1,000-event pages as the feed stages them, activated in
//!   slices as the background activator does: `seal_forecast`, 64 events /
//!   25 ms, the sealing commit alone). Acceptance: the bytes its seals write
//!   are at least 10 times fewer than WP3.INT's seals would write
//!   (`wp3int_model_bytes`, below) and every seal's own time under the
//!   engine lock is under 50 ms (`max_seal_ms`: the time the store measures
//!   inside the committing transaction, from reading the previous seal to
//!   writing the anchor). The sealing slice's whole hold, which adds the
//!   commit's `COMMIT` (WAL append and fsync) that every committing slice
//!   pays, sealing or not, is reported (`sealing_hold_p99_ms`) without a
//!   floor. The store then restarts from its snapshot.
//! - `catch_up_20k_inline_vs_rows`: the WP3.INT model checked against a
//!   measurement: the same 20K catch-up sealed inline (every seal the whole
//!   projection at the scaled interval: WP3.INT's seals) and with this
//!   package's layout.
//! - `seal_after_other_branch_gap`: `main` (1,000 files) sealed, then a
//!   150K-event history on another branch, then one save on `main`: its
//!   seal hashes `main`'s own journal rows since its previous seal
//!   (`journal_branch`), not the 150K rows in between (floored: under
//!   50 ms, and only `main`'s rows hashed).
//! - `restart_50k_rows_vs_inline`: restarts at the tip of a 50K-entry
//!   history sealed as rows and of the same history sealed inline (WP3.INT's
//!   layout and restore: the whole projection parsed and `current_state`
//!   rewritten at every open), 5 of each, alternately (floored: the rows
//!   restart's median is at most 1.15 times the inline one's).
//! - `staging_cost_at_large_inboxes` (CCR-WP2.5-6): a 1,000-event page
//!   staged, inside a write transaction as the feed stages, into an empty
//!   inbox and into one of 40K (floored: at most 4 times the empty inbox's
//!   cost per event) and 100K staged rows (reported), alternately (the
//!   machine's load affects both alike); the commit's own cost (the WAL
//!   frames of the index pages the page dirtied) is reported apart.
//!
//! **WP3.INT model.** WP3.INT sealed the whole projection (anchor of the
//! projection, conflicts and frontier) every `seal_interval(previous
//! payload)` events. For a linear catch-up the sealed size grows linearly
//! with the entries, so with `per_entry` the full payload's size per entry
//! (measured here at the end: the rows' bytes plus the anchor, divided by
//! the entries) the model replays that cadence and adds each seal's full
//! payload. `catch_up_20k_inline_vs_rows` measures how close the model is.
mod support;

use serde_json::json;
use std::{
    collections::BTreeSet,
    path::Path,
    sync::{Arc, Mutex},
    time::Instant,
};
use support::*;
use tkfs::{
    core::{
        commit::set_thread_inline_limit,
        inbox::{SliceBudget, StagedEvent},
        *,
    },
    worker::activator::{seal_budget, seal_forecast},
};

const FILE: &str = "qual_wp4_4_seal";
const SOURCE: &str = "device-a";

/// `n` remote linear creates on the shared main branch of `repo`, authored
/// by [`SOURCE`], numbered from `from`, each depending on the previous one
/// (the first on `after`): a remote catch-up.
fn chain(repo: &str, n: usize, from: usize, after: Option<String>) -> Vec<Event> {
    let main = Branch {
        id: hash(format!("tkfs-v1/{repo}/main").as_bytes()),
        name: "main".into(),
        shared: true,
    };
    let empty = hash(b"");
    let mut previous: Vec<String> = after.into_iter().collect();
    let mut out = Vec::with_capacity(n);
    for i in from..from + n {
        let entity = id();
        let event_id = id();
        let change = |field: &str, value: serde_json::Value| Change {
            entity: entity.clone(),
            field: field.into(),
            parents: vec![],
            value,
        };
        out.push(Event {
            object_sizes: [(empty.clone(), 0u64)].into_iter().collect(),
            id: event_id.clone(),
            repo: repo.into(),
            branch: main.clone(),
            device: SOURCE.into(),
            created_ms: 1_790_000_000_000 + i as u64,
            changes: vec![
                change("kind", json!("file")),
                change(
                    "location",
                    json!({"parent": ROOT, "name": format!("catch-up-{i:07}.txt")}),
                ),
                change("alive", json!(true)),
                change("content", json!(empty)),
            ],
            dependencies: previous.clone(),
            reviewed: vec![],
        });
        previous = vec![event_id];
    }
    out
}

fn staged(events: &[Event]) -> Vec<StagedEvent> {
    events
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, event)| StagedEvent {
            event,
            source: Some((SOURCE.into(), i as i64 + 1)),
        })
        .collect()
}

/// What a catch-up measured.
#[derive(Debug, Default)]
struct CatchUp {
    events: usize,
    seconds: f64,
    seal: tkfs::core::commit::SealStats,
    /// Each seal's own time (ms), measured by the store inside its commit.
    seal_ms: Vec<f64>,
    /// Each seal: its time (ms), layout, events activated before it,
    /// entity rows written, and the part of its time spent reading the
    /// previous seal and extending the journal binding (ms).
    seals: Vec<(f64, &'static str, usize, u64, f64)>,
    /// The engine lock's hold of each slice that sealed (the seal, its
    /// commit's fsync), and of every other slice (ms).
    sealing_holds: Vec<f64>,
    other_holds: Vec<f64>,
    /// The full (inline) payload's bytes per entry at the end.
    per_entry: f64,
    /// Fixed bytes of a full payload (the anchor without entries).
    fixed: f64,
    entries: usize,
}

/// Catches a new store at `dir` up with `n` remote creates: pages of 1,000
/// events staged on a secondary handle, each followed by the background
/// activator's slices under a mutex (the worker's engine lock) until the
/// page is activated. The engine connection is tuned as the activator tunes
/// it while a backlog drains (`wal_autocheckpoint` 16,384 pages, a 32 MiB
/// page cache, `temp_store=MEMORY`) and the WAL is checkpointed off-lock after each seal and
/// every 16 committing slices (`maintenance::checkpoint_db`), as the
/// activator does (§4.4, WP2.3): otherwise SQLite's own checkpoint runs
/// inside a commit, under the lock, whatever seals cost.
fn catch_up(dir: &Path, repo: &str, n: usize, inline: bool) -> CatchUp {
    let previous = set_thread_inline_limit(inline.then_some(usize::MAX));
    let store = Store::initialize(dir, repo, "device-b").unwrap();
    store
        .db
        .execute_batch("PRAGMA wal_autocheckpoint=16384; PRAGMA cache_size=-32768;")
        .unwrap();
    store.set_temp_store_memory(true).unwrap();
    let stager = Store::open_secondary(dir, repo, "device-b").unwrap();
    let engine = Arc::new(Mutex::new(store));
    let allowed: BTreeSet<String> = [SOURCE.to_string()].into();
    let mut out = CatchUp {
        events: n,
        ..CatchUp::default()
    };
    let started = Instant::now();
    let page = 1000;
    let mut done = 0;
    let mut last: Option<String> = None;
    let mut since_checkpoint = 0;
    while done < n {
        let count = page.min(n - done);
        let events = chain(repo, count, done, last.take());
        last = events.last().map(|event| event.id.clone());
        let outcome = stager.stage_incoming(staged(&events), &allowed).unwrap();
        assert!(outcome.rejected.is_empty(), "{outcome:?}");
        loop {
            let mut store = engine.lock().unwrap();
            let held = Instant::now();
            let before = store.seal_stats();
            let forecast = seal_forecast(&store, 64);
            let budget = seal_budget(
                SliceBudget {
                    max_events: 64,
                    max_ms: 25,
                    inline_slow: false,
                },
                forecast.and_then(|f| f.at),
            );
            let slice = store.activate_slice(budget).unwrap();
            let hold = held.elapsed().as_secs_f64() * 1000.0;
            let after = store.seal_stats();
            drop(store);
            assert!(slice.quarantined.is_empty(), "{:?}", slice.quarantined);
            assert!(slice.slow_ticket.is_none(), "a linear catch-up is all fast");
            let committed = slice.fast + slice.slow > 0;
            if after.seals > before.seals {
                let ms = (after.total_micros - before.total_micros) as f64 / 1000.0;
                let mode = if after.rows_full > before.rows_full {
                    "rows_full"
                } else if after.inline > before.inline {
                    "inline"
                } else {
                    "rows_incremental"
                };
                out.sealing_holds.push(hold);
                out.seal_ms.push(ms);
                let journal_ms = (after.journal_micros - before.journal_micros) as f64 / 1000.0;
                out.seals.push((
                    ms,
                    mode,
                    done,
                    after.entity_rows - before.entity_rows,
                    journal_ms,
                ));
            } else if committed {
                out.other_holds.push(hold);
            }
            if committed {
                since_checkpoint += 1;
            }
            if after.seals > before.seals || since_checkpoint >= 16 {
                tkfs::worker::maintenance::checkpoint_db(&stager.db);
                since_checkpoint = 0;
            }
            if !slice.more {
                break;
            }
        }
        done += count;
        if done % 25_000 == 0 {
            eprintln!("{done} events in {:.0} s", started.elapsed().as_secs_f64());
        }
    }
    out.seconds = started.elapsed().as_secs_f64();
    let store = engine.lock().unwrap();
    out.seal = store.seal_stats();
    let branch = store.active.clone();
    out.entries = store.projection(&branch).unwrap().entries.len();
    // The full payload's size: the rows' bytes (as a full anchor holds the
    // same JSON) plus the anchor around them.
    let (rows_bytes, anchor_bytes): (i64, i64) = store
        .db
        .query_row(
            "SELECT (SELECT coalesce(sum(length(entry)+length(heads)),0) FROM snapshot_entries WHERE branch=?1),(SELECT octet_length(payload) FROM startup_snapshots WHERE branch=?1)",
            [&branch],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    let full = if inline {
        anchor_bytes as f64
    } else {
        rows_bytes as f64 + anchor_bytes as f64
    };
    out.fixed = 2048.0;
    out.per_entry = (full - out.fixed).max(0.0) / out.entries.max(1) as f64;
    drop(store);
    set_thread_inline_limit(previous);
    out
}

/// The five slowest seals: time, layout, events activated before them,
/// entity rows written and the journal part of their time.
fn slowest(run: &CatchUp) -> serde_json::Value {
    let mut seals = run.seals.clone();
    seals.sort_by(|a, b| b.0.total_cmp(&a.0));
    seals.truncate(5);
    json!(
        seals
            .iter()
            .map(|(ms, mode, at, rows, journal_ms)| json!({"ms": ms, "journal_ms": journal_ms, "mode": mode, "at_events": at, "entity_rows": rows}))
            .collect::<Vec<_>>()
    )
}

/// `max` of `samples` (0 when empty).
fn max(samples: &[f64]) -> f64 {
    samples.iter().copied().fold(0.0, f64::max)
}

/// The QUAL extra fields of a catch-up's seal measurements.
fn seal_summary(run: &CatchUp) -> serde_json::Value {
    json!({
        "seals": run.seal.seals,
        "inline": run.seal.inline,
        "rows_full": run.seal.rows_full,
        "rows_incremental": run.seal.rows_incremental,
        "entity_rows": run.seal.entity_rows,
        "journal_rows": run.seal.journal_rows,
        "seal_ms_p50": percentile(&run.seal_ms, 50.0),
        "seal_ms_p99": percentile(&run.seal_ms, 99.0),
        "seal_ms_max": max(&run.seal_ms),
        "sealing_hold_ms_p50": percentile(&run.sealing_holds, 50.0),
        "sealing_hold_ms_p99": percentile(&run.sealing_holds, 99.0),
        "sealing_hold_ms_max": max(&run.sealing_holds),
        "other_hold_ms_p50": percentile(&run.other_holds, 50.0),
        "other_hold_ms_p99": percentile(&run.other_holds, 99.0),
        "other_hold_ms_max": max(&run.other_holds),
        "events": run.events,
        "entries": run.entries,
        "seconds": run.seconds,
        "slowest_seals": slowest(run),
    })
}

/// WP3.INT's seal bytes for a linear catch-up of `n` creates whose full
/// payload is `fixed + per_entry * entries` (module docs).
fn wp3int_model_bytes(n: usize, per_entry: f64, fixed: f64) -> f64 {
    let size = |entries: f64| fixed + per_entry * entries;
    // The first seal (an empty store) at cursor 0.
    let mut at = 0f64;
    let mut payload = size(0.0);
    let mut total = payload;
    loop {
        let interval = seal_interval(Some(payload as i64)) as f64;
        at += interval;
        if at > n as f64 {
            return total;
        }
        payload = size(at);
        total += payload;
    }
}

#[test]
#[ignore = "WP4.4 release scenario"]
fn catch_up_150k_seals() {
    let mut s = Scenario::with_budget(FILE, "catch_up_150k_seals", budget_or(1800));
    let n = scaled(150_000);
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    s.measure_from_now();
    let run = catch_up(dir.path(), &repo, n, false);
    let model = wp3int_model_bytes(n, run.per_entry, run.fixed);
    let ratio = model / run.seal.bytes.max(1) as f64;
    s.metric_with(
        "seal_bytes",
        run.seal.bytes as f64,
        "bytes",
        seal_summary(&run),
    );
    s.metric_with(
        "wp3int_model_bytes",
        model,
        "bytes",
        json!({"per_entry": run.per_entry, "fixed": run.fixed}),
    );
    s.metric("seal_bytes_reduction", ratio, "x");
    let seal_max = max(&run.seal_ms);
    s.metric("max_seal_ms", seal_max, "ms");
    let hold_p99 = percentile(&run.sealing_holds, 99.0);
    s.metric("sealing_hold_p99_ms", hold_p99, "ms");
    s.metric("events_per_s", run.events as f64 / run.seconds, "events/s");
    s.check(
        ratio >= 10.0,
        format!("seal bytes reduced {ratio:.1}x (floor 10x)"),
    );
    s.check(
        seal_max < 50.0,
        format!("the longest seal's own time under the engine lock {seal_max:.1} ms (floor 50 ms)"),
    );
    // The restart resumes the rows snapshot.
    let started = Instant::now();
    let store = Store::open(dir.path(), Some(&repo)).unwrap();
    let open_ms = started.elapsed().as_secs_f64() * 1000.0;
    s.metric("restart_open_ms", open_ms, "ms");
    s.check(
        store.startup_resumes() == 1,
        "the restart resumed the snapshot",
    );
    s.check(
        store.projection(&store.active).unwrap().entries.len() == run.entries,
        "every entry after the restart",
    );
    s.finish();
}

#[test]
#[ignore = "WP4.4 release scenario"]
fn catch_up_20k_inline_vs_rows() {
    let mut s = Scenario::with_budget(FILE, "catch_up_20k_inline_vs_rows", budget_or(900));
    let n = scaled(20_000);
    let repo = id();
    s.measure_from_now();
    let inline_dir = tempfile::tempdir().unwrap();
    let inline = catch_up(inline_dir.path(), &repo, n, true);
    let rows_dir = tempfile::tempdir().unwrap();
    let rows = catch_up(rows_dir.path(), &repo, n, false);
    let model = wp3int_model_bytes(n, rows.per_entry, rows.fixed);
    s.metric_with(
        "inline_seal_bytes",
        inline.seal.bytes as f64,
        "bytes",
        seal_summary(&inline),
    );
    s.metric_with(
        "rows_seal_bytes",
        rows.seal.bytes as f64,
        "bytes",
        seal_summary(&rows),
    );
    s.metric("wp3int_model_bytes", model, "bytes");
    let model_error = (model - inline.seal.bytes as f64).abs() / inline.seal.bytes.max(1) as f64;
    s.metric("model_error", model_error, "ratio");
    s.metric(
        "seal_bytes_reduction",
        inline.seal.bytes as f64 / rows.seal.bytes.max(1) as f64,
        "x",
    );
    s.check(
        model_error < 0.25,
        format!("the WP3.INT model is within 25% of the inline measurement ({model_error:.2})"),
    );
    s.check(
        inline.seal.bytes > rows.seal.bytes * 3,
        "rows seal fewer bytes than inline seals",
    );
    s.finish();
}

#[test]
#[ignore = "WP4.4 release scenario"]
fn seal_after_other_branch_gap() {
    let mut s = Scenario::with_budget(FILE, "seal_after_other_branch_gap", budget_or(900));
    let n = scaled(150_000);
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut store = Store::initialize(dir.path(), &repo, "device-b").unwrap();
    linear_history(&mut store, 1000).unwrap();
    let main = store.active.clone();
    drop(store);
    // `main` sealed at its tip.
    let mut store = Store::open(dir.path(), Some(&repo)).unwrap();
    let side = store.fork("side", false).unwrap();
    store.checkout(&side.id).unwrap();
    linear_history_named(&mut store, "gap", n).unwrap();
    store.checkout(&main).unwrap();
    let gap: i64 = store
        .db
        .query_row(
            "SELECT count(*) FROM journal_sequence WHERE seq>(SELECT cursor FROM startup_snapshots WHERE branch=?)",
            [&main],
            |r| r.get(0),
        )
        .unwrap();
    s.measure_from_now();
    let before = store.seal_stats();
    let file = store.lookup("lin-000000.txt").unwrap();
    store
        .write_revision(
            &file.id,
            file.revisions.get("content").cloned(),
            b"after the gap",
        )
        .unwrap();
    let after = store.seal_stats();
    let seal_ms = (after.total_micros - before.total_micros) as f64 / 1000.0;
    let hashed = after.journal_rows - before.journal_rows;
    s.metric_with(
        "seal_ms",
        seal_ms,
        "ms",
        json!({"gap_journal_rows": gap, "journal_rows_hashed": hashed,
            "seals": after.seals - before.seals}),
    );
    s.check(after.seals == before.seals + 1, "the save sealed main");
    s.check(
        gap as usize >= n,
        format!("{gap} journal rows of another branch since main's seal"),
    );
    s.check(
        hashed <= 2,
        format!("the seal hashed {hashed} journal rows (main's own)"),
    );
    s.check(
        seal_ms < 50.0,
        format!("the seal after the gap took {seal_ms:.1} ms (floor 50 ms)"),
    );
    s.finish();
}

#[test]
#[ignore = "WP4.4 release scenario"]
fn restart_50k_rows_vs_inline() {
    let mut s = Scenario::with_budget(FILE, "restart_50k_rows_vs_inline", budget_or(1800));
    let n = scaled(50_000);
    let repo = id();
    // The same history built twice: sealed as rows (the default layout) and
    // sealed inline (the whole projection in the anchor). Not a copy whose
    // snapshot is dropped: reconstructing 50K events is quadratic.
    let rows_dir = tempfile::tempdir().unwrap();
    let inline_dir = tempfile::tempdir().unwrap();
    let inline_root = inline_dir.path().to_path_buf();
    for (root, inline) in [(rows_dir.path(), false), (inline_dir.path(), true)] {
        let previous = set_thread_inline_limit(inline.then_some(usize::MAX));
        let mut store = Store::initialize(root, &repo, "device-b").unwrap();
        linear_history(&mut store, n).unwrap();
        drop(store);
        set_thread_inline_limit(previous);
    }
    let open = |root: &Path, inline: bool| -> (f64, bool, usize) {
        let previous = set_thread_inline_limit(inline.then_some(usize::MAX));
        let started = Instant::now();
        let store = Store::open(root, Some(&repo)).unwrap();
        let ms = started.elapsed().as_secs_f64() * 1000.0;
        let resumed = store.startup_resumes() == 1;
        let rows = store
            .check_startup_snapshot(&store.active)
            .map(|check| usize::from(check.rows))
            .unwrap_or(2);
        drop(store);
        set_thread_inline_limit(previous);
        (ms, resumed, rows)
    };
    // The first opens (each stores the restored projection in its layout's
    // form), not measured.
    let (first_inline_ms, _, inline_layout) = open(&inline_root, true);
    let (first_rows_ms, _, rows_layout) = open(rows_dir.path(), false);
    s.check(
        inline_layout == 0 && rows_layout == 1,
        format!(
            "layouts: inline store {inline_layout}, rows store {rows_layout} (0 inline, 1 rows)"
        ),
    );
    s.measure_from_now();
    let (mut rows_ms, mut inline_ms) = (vec![], vec![]);
    let mut resumed = true;
    for _ in 0..5 {
        let (ms, ok, _) = open(rows_dir.path(), false);
        rows_ms.push(ms);
        resumed &= ok;
        let (ms, ok, _) = open(&inline_root, true);
        inline_ms.push(ms);
        resumed &= ok;
    }
    let rows = percentile(&rows_ms, 50.0);
    let inline = percentile(&inline_ms, 50.0);
    s.metric_with(
        "restart_rows_ms",
        rows,
        "ms",
        json!({"samples": rows_ms, "entries": n, "first_open_ms": first_rows_ms}),
    );
    s.metric_with(
        "restart_inline_ms",
        inline,
        "ms",
        json!({"samples": inline_ms, "first_open_ms": first_inline_ms}),
    );
    s.metric("restart_rows_vs_inline", rows / inline, "ratio");
    s.check(resumed, "every restart resumed its snapshot");
    // Alternating medians on a shared machine: within 15% is "not slower".
    s.check(
        rows <= inline * 1.15,
        format!(
            "a rows restart ({rows:.0} ms) is not slower than an inline one ({inline:.0} ms; floor 1.15x)"
        ),
    );
    s.finish();
}

#[test]
#[ignore = "WP4.4 release scenario (CCR-WP2.5-6)"]
fn staging_cost_at_large_inboxes() {
    let mut s = Scenario::with_budget(FILE, "staging_cost_at_large_inboxes", budget_or(900));
    let repo = id();
    let allowed: BTreeSet<String> = [SOURCE.to_string()].into();
    let large = scaled(100_000);
    let events = chain(&repo, large + 10_000, 0, None);
    let big_dir = tempfile::tempdir().unwrap();
    drop(Store::initialize(big_dir.path(), &repo, "device-b").unwrap());
    // A secondary handle stages, as the feed's pooled connections do.
    let big = Store::open_secondary(big_dir.path(), &repo, "device-b").unwrap();
    let mut staged_rows = 0;
    let mut page_at = large;
    // Measured at 40K staged rows (floored) and at `large` (reported).
    for (target, floor) in [(40_000.min(large), Some(4.0)), (large, None)] {
        let setup = Instant::now();
        for page in events[staged_rows..target].chunks(1000) {
            big.stage_incoming(staged(page), &allowed).unwrap();
        }
        let setup_us = setup.elapsed().as_secs_f64() * 1e6 / (target - staged_rows).max(1) as f64;
        staged_rows = target;
        let (mut empty_us, mut large_us) = (vec![], vec![]);
        let (mut empty_commit, mut large_commit) = (vec![], vec![]);
        for _ in 0..5 {
            let small_dir = tempfile::tempdir().unwrap();
            drop(Store::initialize(small_dir.path(), &repo, "device-b").unwrap());
            let small = Store::open_secondary(small_dir.path(), &repo, "device-b").unwrap();
            let page = &events[page_at..page_at + 1000];
            page_at += 1000;
            for (store, out, commit) in [
                (&small, &mut empty_us, &mut empty_commit),
                (&big, &mut large_us, &mut large_commit),
            ] {
                // As the feed stages: inside its write transaction
                // (`feed_write`); the commit's own cost (the WAL frames of
                // the index pages the page dirtied, and the fsync) apart.
                store.db.execute_batch("BEGIN IMMEDIATE").unwrap();
                let started = Instant::now();
                let outcome = store.stage_incoming(staged(page), &allowed).unwrap();
                assert_eq!(outcome.staged.len(), page.len());
                out.push(started.elapsed().as_secs_f64() * 1e6 / page.len() as f64);
                let started = Instant::now();
                store.db.execute_batch("COMMIT").unwrap();
                commit.push(started.elapsed().as_secs_f64() * 1000.0);
            }
            staged_rows += 1000;
        }
        let empty = percentile(&empty_us, 50.0);
        let at = percentile(&large_us, 50.0);
        let ratio = at / empty;
        s.metric_with(
            &format!("staging_us_per_event_at_{target}"),
            at,
            "us",
            json!({"empty_inbox_us": empty, "ratio": ratio, "setup_us_per_event": setup_us,
                "commit_ms": percentile(&large_commit, 50.0),
                "empty_inbox_commit_ms": percentile(&empty_commit, 50.0)}),
        );
        if let Some(floor) = floor {
            s.check(
                ratio <= floor,
                format!("staging at {target} staged rows costs {ratio:.1}x an empty inbox's (floor {floor}x)"),
            );
        }
    }
    s.finish();
}
