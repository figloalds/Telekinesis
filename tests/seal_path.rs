//! WP4.4: the seal path (REVAMP_PLAN §14.5a): incremental startup-snapshot
//! seals (format 2: a small anchor plus per-entity rows for a large
//! projection), the conflict list as a projection delta (CCR-WP2.INT-5), the
//! store's seal cadence (`Store::seal_due_in`), crash safety and fallbacks.
//!
//! - Seals of a large projection write the rows that changed since the
//!   previous seal, not the projection (`large_projections_seal_only_what_changed`,
//!   `prepared_slow_commits_seal_only_their_change`); a startup replays its
//!   tail and reseals incrementally.
//! - Crash points at every seal step (each write, inside the commit, at
//!   startup), in both layouts, for fast, slow inline, slow prepared and
//!   startup seals and for the switch from inline to rows: the restart
//!   resumes the previous snapshot, the derived state is exact (strict
//!   oracle) and the snapshot is valid.
//! - Random two-replica workloads with deliveries (legacy receive and the
//!   prepared off-lock path), reviews, restarts and crashes, in both layouts:
//!   the strict oracle is empty and every snapshot valid after every step.
//! - Damaged rows, an older snapshot format and a stale header reconstruct;
//!   a damaged column (non-UTF-8 or BLOB text, a non-integer cursor) never
//!   blocks an open or a commit.
//! - A seal hashes only its own branch's journal rows since its previous
//!   seal (`journal_branch`); a restart writes only the projection rows that
//!   differ from the snapshot.
//!
//! The thread's inline limit (`set_thread_inline_limit`) picks the layout
//! in-process; a crash child reads `TKFS_SNAPSHOT_INLINE_LIMIT`.
mod support;

use rand::{Rng, SeedableRng, rngs::StdRng, seq::SliceRandom};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    process::Command,
};
use support::{clone_replica, copy_dir, devices, linear_history, sync_into};
use tkfs::core::{
    commit::{FOLD_MAX_ENTRIES, SealStats, set_thread_inline_limit},
    inbox::{CommitOutcome, Prepared, SliceBudget, StagedEvent},
    *,
};

const REPO: &str = "seal-path";

/// The calling thread's inline limit while the guard lives.
struct Layout(Option<usize>);
impl Layout {
    /// Every projection sealed as rows.
    fn rows() -> Self {
        Self(set_thread_inline_limit(Some(0)))
    }
    /// Every projection sealed inline (the format-1 / WP3.INT layout).
    fn inline() -> Self {
        Self(set_thread_inline_limit(Some(usize::MAX)))
    }
    fn limit(bytes: usize) -> Self {
        Self(set_thread_inline_limit(Some(bytes)))
    }
    fn of(rows: bool) -> Self {
        if rows { Self::rows() } else { Self::inline() }
    }
}
impl Drop for Layout {
    fn drop(&mut self) {
        set_thread_inline_limit(self.0);
    }
}

fn open(path: &Path) -> Store {
    Store::open(path, Some(REPO)).unwrap()
}

fn open_deferred(path: &Path) -> Store {
    Store::open_existing_with(
        path,
        OpenOptions {
            defer_activation: true,
            ..OpenOptions::default()
        },
    )
    .unwrap()
}

fn save(s: &mut Store, path: &str, bytes: &[u8]) {
    let entry = s.lookup(path).unwrap();
    s.write_revision(&entry.id, entry.revisions.get("content").cloned(), bytes)
        .unwrap();
}

/// `branch`'s committed events with their journal sequences, in journal order.
fn journal(s: &Store, branch: &str) -> Vec<(i64, Event)> {
    s.db.prepare("SELECT s.seq,e.payload FROM journal_sequence s JOIN events e ON e.id=s.event WHERE e.branch=? ORDER BY s.seq")
        .unwrap()
        .query_map([branch], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
        .unwrap()
        .map(|row| {
            let (seq, payload) = row.unwrap();
            (seq, serde_json::from_str(&payload).unwrap())
        })
        .collect()
}

/// The stored conflict rows of `branch` as a snapshot check reports them.
fn stored_conflicts(s: &Store, branch: &str) -> BTreeMap<String, (Conflict, Option<String>)> {
    s.db.prepare("SELECT id,payload,resolved_by FROM conflicts WHERE branch=?")
        .unwrap()
        .query_map([branch], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
            ))
        })
        .unwrap()
        .map(|row| {
            let (id, payload, resolved) = row.unwrap();
            let mut conflict: Conflict = serde_json::from_str(&payload).unwrap();
            conflict.resolved_by = None;
            (id, (conflict, resolved))
        })
        .collect()
}

fn frontier(s: &Store, branch: &str) -> Vec<String> {
    s.db.prepare("SELECT event FROM event_frontier WHERE branch=? ORDER BY event")
        .unwrap()
        .query_map([branch], |r| r.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

/// Every branch's startup snapshot is valid as a startup checks it, and it
/// seals exactly the replay of the journal up to its cursor; at the tip it
/// is the stored state (projection, conflict rows, frontier). Returns how
/// many branches are sealed as rows.
#[track_caller]
fn assert_snapshots(s: &Store, context: &str) -> usize {
    let mut rows = 0;
    for branch in s.branches().unwrap() {
        let check = s
            .check_startup_snapshot(&branch.id)
            .unwrap_or_else(|error| panic!("{context}: snapshot of {}: {error:#}", branch.id));
        rows += usize::from(check.rows);
        let events = journal(s, &branch.id);
        let prefix: Vec<Event> = events
            .iter()
            .filter(|(seq, _)| *seq <= check.cursor)
            .map(|(_, event)| event.clone())
            .collect();
        assert_eq!(
            check.projection,
            project(&branch.id, &prefix).unwrap(),
            "{context}: the sealed projection replays the journal up to the cursor"
        );
        let tip = events.last().map_or(0, |(seq, _)| *seq);
        if check.cursor == tip {
            assert_eq!(
                check.projection,
                s.projection(&branch.id).unwrap(),
                "{context}: sealed projection at the tip"
            );
            assert_eq!(
                check.conflicts,
                stored_conflicts(s, &branch.id),
                "{context}: sealed conflict rows at the tip"
            );
            assert_eq!(
                check.frontier,
                frontier(s, &branch.id),
                "{context}: sealed frontier"
            );
        }
    }
    rows
}

#[track_caller]
fn assert_oracle(s: &mut Store, context: &str) {
    let diff = s.verify_derived_state_strict().unwrap();
    assert!(diff.is_empty(), "{context}: oracle: {diff:?}");
    for branch in s.branches().unwrap() {
        let events: Vec<Event> = journal(s, &branch.id)
            .into_iter()
            .map(|(_, event)| event)
            .collect();
        assert_eq!(
            s.projection(&branch.id).unwrap(),
            project(&branch.id, &events).unwrap(),
            "{context}: projection of {}",
            branch.id
        );
    }
}

/// The anchor rows (`branch, cursor, payload, checksum`), read on a plain
/// connection.
fn anchors(root: &Path) -> Vec<(String, i64, String, String)> {
    let db = Connection::open(root.join("metadata.sqlite")).unwrap();
    db.prepare("SELECT branch,cursor,payload,checksum FROM startup_snapshots ORDER BY branch")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

fn delta(after: SealStats, before: SealStats) -> SealStats {
    SealStats {
        seals: after.seals - before.seals,
        inline: after.inline - before.inline,
        rows_full: after.rows_full - before.rows_full,
        rows_incremental: after.rows_incremental - before.rows_incremental,
        bytes: after.bytes - before.bytes,
        entity_rows: after.entity_rows - before.entity_rows,
        conflict_rows: after.conflict_rows - before.conflict_rows,
        journal_rows: after.journal_rows - before.journal_rows,
        max_micros: after.max_micros,
        total_micros: after.total_micros - before.total_micros,
        journal_micros: after.journal_micros - before.journal_micros,
    }
}

/// Stages `events` on `b` and activates them as the background activator
/// does: slices, every slow event prepared off-lock on `read` and committed
/// with `commit_prepared`. Returns the prepared commits made.
fn activate_prepared(
    b: &mut Store,
    read: &Store,
    events: Vec<Event>,
    allowed: &BTreeSet<String>,
) -> usize {
    let outcome = b
        .stage_incoming(
            events
                .into_iter()
                .map(|event| StagedEvent {
                    event,
                    source: None,
                })
                .collect(),
            allowed,
        )
        .unwrap();
    assert!(outcome.rejected.is_empty(), "{outcome:?}");
    let mut prepared = 0;
    loop {
        let slice = b.activate_slice(SliceBudget::ACTIVATOR).unwrap();
        if let Some(ticket) = slice.slow_ticket {
            match b
                .commit_prepared(Store::prepare_slow(read, &ticket).unwrap())
                .unwrap()
            {
                CommitOutcome::Committed { .. } => prepared += 1,
                CommitOutcome::Stale => {
                    b.commit_prepared(Prepared::Inline(ticket)).unwrap();
                }
                CommitOutcome::Failed(error) => panic!("prepared commit failed: {error:?}"),
            }
            continue;
        }
        if !slice.more {
            return prepared;
        }
    }
}

/// The events of `from` that `to` lacks, in causal order.
fn missing(from: &Store, to: &Store) -> Vec<Event> {
    let mut events = from
        .shared_bundle(&to.shared_event_inventory().unwrap())
        .unwrap()
        .events;
    support::causal_sort(&mut events);
    events
}

// ---------------------------------------------------------------------------
// Incremental seals

/// The same workload sealed as rows and inline: every incremental rows seal
/// writes only the entities changed since the previous seal (a few here, of
/// 600), so the rows layout writes a small fraction of the inline layout's
/// bytes; both resume after a restart.
#[test]
fn large_projections_seal_only_what_changed() {
    let mut written = BTreeMap::new();
    for rows in [true, false] {
        let _layout = Layout::of(rows);
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path());
        for i in 0..600 {
            s.create(&format!("f{i:04}"), "file").unwrap();
        }
        let start = s.seal_stats();
        let mut rng = StdRng::seed_from_u64(4);
        let mut previous = start;
        let mut sealed = 0;
        for i in 0..1000 {
            let file = format!("f{:04}", rng.gen_range(0..8));
            save(&mut s, &file, format!("v{i}").as_bytes());
            let now = s.seal_stats();
            if now.seals > previous.seals && rows {
                let seal = delta(now, previous);
                assert_eq!(seal.rows_incremental, 1, "{seal:?}");
                // The first also writes the creates made since the last
                // seal of the create phase.
                let changed = if sealed == 0 { 8 + 127 } else { 8 };
                assert!(
                    seal.entity_rows <= changed,
                    "a seal writes the changed entities only: {seal:?}"
                );
                sealed += 1;
            }
            previous = now;
        }
        let saves = delta(s.seal_stats(), start);
        assert!(saves.seals >= 5, "{saves:?}");
        written.insert(rows, saves.bytes);
        assert_eq!(assert_snapshots(&s, "after the saves"), usize::from(rows));
        let expected = s.projection(&s.active).unwrap();
        drop(s);
        let mut s = open(dir.path());
        assert_eq!(s.startup_resumes(), 1, "resumed from the snapshot");
        assert_eq!(s.projection(&s.active).unwrap(), expected);
        assert_snapshots(&s, "after a restart");
        assert_oracle(&mut s, "after a restart");
    }
    eprintln!(
        "seal bytes: rows {} inline {}",
        written[&true], written[&false]
    );
    assert!(
        written[&true] * 10 < written[&false],
        "rows {} vs inline {}",
        written[&true],
        written[&false]
    );
}

/// A rows snapshot resumes, replays its tail and reseals only the replayed
/// entities; a second restart resumes at the tip.
#[test]
fn rows_snapshots_resume_replay_and_reseal_incrementally() {
    let _layout = Layout::rows();
    let dir = tempfile::tempdir().unwrap();
    let mut s = open(dir.path());
    for i in 0..200 {
        s.create(&format!("f{i:04}"), "file").unwrap();
    }
    drop(s);
    let mut s = open(dir.path());
    let sealed = anchors(dir.path());
    for i in 0..20 {
        save(&mut s, &format!("f{i:04}"), b"tail");
    }
    assert_eq!(anchors(dir.path()), sealed, "the tail is not sealed yet");
    let expected = s.projection(&s.active).unwrap();
    drop(s);
    let s = open(dir.path());
    assert_eq!(s.startup_resumes(), 1);
    assert_eq!(s.projection(&s.active).unwrap(), expected);
    let stats = s.seal_stats();
    assert_eq!(stats.rows_incremental, 1, "{stats:?}");
    assert_eq!(stats.entity_rows, 20, "the replayed entities: {stats:?}");
    assert_eq!(assert_snapshots(&s, "after the replay"), 1);
    drop(s);
    let mut s = open(dir.path());
    assert_eq!(s.startup_resumes(), 1);
    assert_eq!(s.seal_stats().seals, 0, "an empty tail seals nothing");
    assert_eq!(s.projection(&s.active).unwrap(), expected);
    assert_oracle(&mut s, "after two restarts");
}

/// A store sealed inline switches to rows when its anchor would pass the
/// inline limit; every seal falls `seal_interval(previous anchor)` journal
/// sequences after the previous one, in both layouts, and the store resumes.
#[test]
fn the_inline_layout_switches_to_rows_past_the_limit() {
    let _layout = Layout::limit(256 * 1024);
    let dir = tempfile::tempdir().unwrap();
    let mut s = open(dir.path());
    let sealed = |s: &Store| -> (i64, i64) {
        s.db.query_row(
            "SELECT cursor,octet_length(payload) FROM startup_snapshots WHERE branch=?",
            [&s.active],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
    };
    let mut seals = vec![sealed(&s)];
    let mut layouts = vec![];
    for i in 0..700 {
        s.create(&format!("{}-{i:04}", "n".repeat(60)), "file")
            .unwrap();
        let now = sealed(&s);
        if now.0 != seals.last().unwrap().0 {
            seals.push(now);
            layouts.push(s.check_startup_snapshot(&s.active).unwrap().rows);
        }
    }
    assert!(
        layouts.contains(&false) && layouts.contains(&true),
        "{layouts:?}"
    );
    assert!(
        layouts.windows(2).all(|pair| pair[0] <= pair[1]),
        "inline, then rows: {layouts:?}"
    );
    for pair in seals.windows(2) {
        assert_eq!(
            pair[1].0 - pair[0].0,
            seal_interval(Some(pair[0].1)),
            "{seals:?}"
        );
    }
    assert_snapshots(&s, "after the switch");
    let expected = s.projection(&s.active).unwrap();
    drop(s);
    let mut s = open(dir.path());
    assert_eq!(s.startup_resumes(), 1);
    assert_eq!(s.projection(&s.active).unwrap(), expected);
    assert_oracle(&mut s, "after the switch and a restart");
}

/// `Store::seal_due_in` is the store's cadence: the commit it names seals,
/// none before it does (both layouts).
#[test]
fn seal_due_in_names_the_sealing_commit() {
    for rows in [false, true] {
        let _layout = Layout::of(rows);
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path());
        s.create("file", "file").unwrap();
        drop(s);
        let mut s = open(dir.path());
        let branch = s.active.clone();
        for _round in 0..2 {
            let due = s.seal_due_in(&branch).unwrap();
            assert!((1..=MIN_SEAL_INTERVAL).contains(&due), "{due}");
            let before = anchors(&s.root);
            for _ in 1..due {
                save(&mut s, "file", b"x");
                assert_eq!(anchors(&s.root), before, "no seal before the due commit");
            }
            save(&mut s, "file", b"sealed");
            assert_ne!(anchors(&s.root), before, "the due commit seals");
            assert_eq!(s.seal_due_in(&branch).unwrap(), MIN_SEAL_INTERVAL);
        }
        assert_snapshots(&s, "after two periodic seals");
    }
}

/// Prepared slow commits on a rows-sealed branch: no projection JSON is
/// serialized off-lock for a delta, and each forced seal writes the
/// entities that changed, not the projection.
#[test]
fn prepared_slow_commits_seal_only_their_change() {
    let _layout = Layout::rows();
    let dir = tempfile::tempdir().unwrap();
    let mut a = Store::open(&dir.path().join("a"), Some(REPO)).unwrap();
    linear_history(&mut a, 300).unwrap();
    let (mut a, mut b) = clone_replica(a, &dir.path().join("b")).unwrap();
    let allowed = devices([&a, &b]);
    // B's unseen local edit: every event from A is concurrent with it.
    b.create("b-local", "directory").unwrap();
    for i in 0..10 {
        a.create(&format!("ns-{i:02}"), "directory").unwrap();
        let file = a.lookup(&format!("lin-{i:06}.txt")).unwrap();
        a.rename(&file.id, &format!("ns-{i:02}/moved.txt"), false)
            .unwrap();
    }
    let read = Store::open_secondary(&b.root, &b.repo, &b.device).unwrap();
    let (stats, json_seals) = (b.seal_stats(), b.prepared_seals());
    let events = missing(&a, &b);
    let prepared = activate_prepared(&mut b, &read, events, &allowed);
    assert_eq!(prepared, 20, "every event of A is a slow commit on B");
    let seals = delta(b.seal_stats(), stats);
    assert!(seals.rows_incremental >= 20, "{seals:?}");
    assert_eq!(seals.rows_full, 0, "{seals:?}");
    assert!(
        seals.entity_rows <= 2 * 20 + 1,
        "each forced seal writes its changed entities: {seals:?}"
    );
    assert_eq!(
        b.prepared_seals(),
        json_seals,
        "no projection JSON for a rows seal"
    );
    assert_eq!(assert_snapshots(&b, "after prepared commits"), 1);
    assert_oracle(&mut b, "after prepared commits");
}

// ---------------------------------------------------------------------------
// Stored projection header (CCR-WP2.INT-5) and the fold bound

fn state_payload(s: &Store) -> String {
    s.db.query_row(
        "SELECT payload FROM current_state WHERE branch=?",
        [&s.active],
        |r| r.get(0),
    )
    .unwrap()
}

fn header(s: &Store) -> Option<Value> {
    s.db.query_row(
        "SELECT payload FROM current_meta WHERE branch=?",
        [&s.active],
        |r| r.get::<_, String>(0),
    )
    .ok()
    .map(|payload| serde_json::from_str(&payload).unwrap())
}

/// A slow commit that adds a conflict (a concurrent edit) and the review
/// that resolves it store their projection as a delta: the changed entity's
/// row and the conflict list in `current_meta`, never a rewrite of
/// `current_state`. A restart restores `current_state` with the list and
/// drops the header row. Both layouts.
#[test]
fn a_conflict_change_is_stored_as_a_projection_delta() {
    for rows in [false, true] {
        let _layout = Layout::of(rows);
        let dir = tempfile::tempdir().unwrap();
        let mut a = Store::open(&dir.path().join("a"), Some(REPO)).unwrap();
        a.create("file", "file").unwrap();
        linear_history(&mut a, 40).unwrap();
        let (mut a, b) = clone_replica(a, &dir.path().join("b")).unwrap();
        let allowed = devices([&a, &b]);
        // A restart with the snapshot at the tip stores the projection as
        // one `current_state` row (the clone's open replayed its tail into
        // parts, which the first slow commit would fold back).
        let root = b.root.clone();
        drop(b);
        let mut b = open(&root);
        assert_eq!(
            b.db.query_row("SELECT count(*) FROM current_parts", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        save(&mut a, "file", b"from a");
        save(&mut b, "file", b"from b");
        let state = state_payload(&b);
        assert_eq!(header(&b), None);
        let slow = b.commit_path_counters().slow;
        sync_into(&a, &mut b, &allowed).unwrap();
        assert_eq!(b.commit_path_counters().slow, slow + 1, "a slow commit");
        let conflicts = b.projection(&b.active).unwrap().conflicts;
        assert_eq!(conflicts.len(), 1, "the concurrent edit conflicts");
        assert_eq!(state_payload(&b), state, "no rewrite of current_state");
        assert_eq!(
            header(&b).unwrap()["conflicts"],
            serde_json::to_value(&conflicts).unwrap()
        );
        assert_oracle(&mut b, "after the conflict");
        assert_snapshots(&b, "after the conflict");
        let selected = conflicts[0].alternatives[0].revision.clone();
        b.resolve(&[conflicts[0].id.clone()], &selected).unwrap();
        assert!(b.projection(&b.active).unwrap().conflicts.is_empty());
        assert_eq!(state_payload(&b), state, "the review is a delta too");
        assert_eq!(header(&b).unwrap()["conflicts"], serde_json::json!([]));
        assert_oracle(&mut b, "after the review");
        assert_snapshots(&b, "after the review");
        let expected = b.projection(&b.active).unwrap();
        drop(b);
        let mut b = open(&root);
        assert_eq!(b.startup_resumes(), 1);
        assert_eq!(header(&b), None, "a restore drops the header row");
        assert_eq!(b.projection(&b.active).unwrap(), expected);
        assert_oracle(&mut b, "after a restart");
    }
}

/// A projection larger than `FOLD_MAX_ENTRIES` is never rewritten by a slow
/// commit: its parts grow instead (one row per changed entity), and the
/// stored projection stays exact.
#[test]
fn a_slow_commit_never_rewrites_a_large_projection() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = Store::open(&dir.path().join("a"), Some(REPO)).unwrap();
    linear_history(&mut a, FOLD_MAX_ENTRIES + 50).unwrap();
    let (mut a, mut b) = clone_replica(a, &dir.path().join("b")).unwrap();
    let allowed = devices([&a, &b]);
    b.create("b-local", "directory").unwrap();
    for i in 0..15 {
        a.create(&format!("ns-{i:02}"), "directory").unwrap();
        let file = a.lookup(&format!("lin-{i:06}.txt")).unwrap();
        a.rename(&file.id, &format!("ns-{i:02}/moved.txt"), false)
            .unwrap();
    }
    let state = state_payload(&b);
    let parts = |s: &Store| -> i64 {
        s.db.query_row("SELECT count(*) FROM current_parts", [], |r| r.get(0))
            .unwrap()
    };
    let before = parts(&b);
    let slow = b.commit_path_counters().slow;
    for event in missing(&a, &b) {
        b.receive(
            Bundle {
                repo: b.repo.clone(),
                branches: vec![],
                events: vec![event],
                objects: Default::default(),
            },
            &allowed,
        )
        .unwrap();
        assert_eq!(state_payload(&b), state, "no rewrite of a large projection");
    }
    assert_eq!(b.commit_path_counters().slow, slow + 30);
    assert!(parts(&b) > before);
    assert_oracle(&mut b, "after slow commits on a large projection");
}

// ---------------------------------------------------------------------------
// Crash safety

const CHILD: &str = "TKFS_SEAL_CHILD";
const STEP: &str = "TKFS_SEAL_STEP";

/// Child of the crash tests: `TKFS_SEAL_CHILD=<state>` and `TKFS_SEAL_STEP`
/// choose the work, `TKFS_FAULT` ends the process (exit 86) at that point,
/// `TKFS_SNAPSHOT_INLINE_LIMIT` the layout. A no-op unless set.
#[test]
fn seal_crash_child() {
    let Ok(root) = std::env::var(CHILD) else {
        return;
    };
    let root = PathBuf::from(root);
    match std::env::var(STEP).unwrap().as_str() {
        // Periodic seals of fast commits (content saves of a file without
        // a conflict).
        "fast" => {
            let mut s = open(&root);
            for i in 0..140 {
                save(&mut s, "f000", format!("child {i}").as_bytes());
            }
        }
        // Creates (slow commits: the projection has a conflict), each with
        // a forced seal, until the projection passes a small inline limit.
        "create" => {
            let mut s = open(&root);
            for i in 0..200 {
                s.create(&format!("{}-{i:04}", "c".repeat(60)), "file")
                    .unwrap();
            }
        }
        // A staged slow event committed inline (forced seal).
        "slow-inline" => {
            let mut s = open_deferred(&root);
            let slice = s.activate_slice(SliceBudget::UNBOUNDED).unwrap();
            assert_eq!(slice.slow, 1, "{slice:?}");
        }
        // A staged slow event prepared off-lock and committed.
        "slow-prepared" => {
            let mut s = open_deferred(&root);
            let read = Store::open_secondary(&s.root, &s.repo, &s.device).unwrap();
            let ticket = s
                .activate_slice(SliceBudget::ACTIVATOR)
                .unwrap()
                .slow_ticket
                .unwrap();
            let prepared = Store::prepare_slow(&read, &ticket).unwrap();
            s.commit_prepared(prepared).unwrap();
        }
        // A startup that replays a tail and reseals.
        "open" => drop(open(&root)),
        // Saves of a file of its own (the random workload). A new file has
        // no conflict: saves of a file with an unresolved content conflict
        // (each based on the winning revision only) would each add
        // conflict rows, and the inline layout seals every one of them
        // (hundreds of MB after a few crash steps).
        "touch" => {
            let mut s = open(&root);
            let file = s.create(&format!("touch-{}", id()), "file").unwrap().id;
            for i in 0..140 {
                let entry = s.entry(&file).unwrap();
                s.write_revision(
                    &file,
                    entry.revisions.get("content").cloned(),
                    format!("touch {i}").as_bytes(),
                )
                .unwrap();
            }
        }
        step => panic!("unknown step {step}"),
    }
}

/// Runs the crash child on `root` until `point`; it must end there.
fn crash(root: &Path, step: &str, point: &str, inline_limit: Option<usize>) {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "seal_crash_child",
            "--exact",
            "--nocapture",
            "--test-threads",
            "1",
        ])
        .env(CHILD, root)
        .env(STEP, step)
        .env("TKFS_FAULT", point);
    match inline_limit {
        Some(limit) => command.env("TKFS_SNAPSHOT_INLINE_LIMIT", limit.to_string()),
        None => command.env_remove("TKFS_SNAPSHOT_INLINE_LIMIT"),
    };
    let output = command.output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(86),
        "{step} {point}: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The state a crash scenario starts from (`root`), sealed at its tip
/// unless the scenario needs a tail.
fn crash_scenario(dir: &Path, step: &str) -> PathBuf {
    match step {
        "fast" | "create" | "open" => {
            let root = dir.join("b");
            let mut s = open(&root);
            s.create("file", "file").unwrap();
            for i in 0..60 {
                s.create(&format!("f{i:03}"), "file").unwrap();
            }
            // A conflict row, so rows seals write conflict rows too.
            let entry = s.lookup("file").unwrap();
            let (content, alive) = (
                entry.revisions.get("content").cloned(),
                entry.revisions.get("alive").cloned(),
            );
            s.write_with_base(&entry.id, content.clone(), alive.clone(), b"one")
                .unwrap();
            s.write_with_base(&entry.id, content, alive, b"two")
                .unwrap();
            assert!(!s.conflicts().unwrap().is_empty());
            drop(s);
            let mut s = open(&root);
            if step == "open" {
                for i in 0..10 {
                    save(&mut s, &format!("f{i:03}"), b"tail");
                }
            }
            root
        }
        "slow-inline" | "slow-prepared" => {
            let mut a = Store::open(&dir.join("a"), Some(REPO)).unwrap();
            a.create("file", "file").unwrap();
            for i in 0..60 {
                a.create(&format!("f{i:03}"), "file").unwrap();
            }
            let (mut a, mut b) = clone_replica(a, &dir.join("b")).unwrap();
            let allowed = devices([&a, &b]);
            save(&mut a, "file", b"from a");
            save(&mut b, "file", b"from b");
            let root = b.root.clone();
            drop(b);
            let b = open(&root);
            let events = missing(&a, &b);
            assert_eq!(events.len(), 1);
            b.stage_incoming(
                events
                    .into_iter()
                    .map(|event| StagedEvent {
                        event,
                        source: None,
                    })
                    .collect(),
                &allowed,
            )
            .unwrap();
            root
        }
        step => panic!("unknown step {step}"),
    }
}

/// What a crash must have left of the snapshot sealed before it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Left {
    /// Exactly that snapshot: the crash was inside the first commit (or the
    /// startup) after it.
    Unchanged,
    /// A committed one (the crash came after a durable commit).
    Committed,
    /// An inline snapshot: the switch to rows did not commit.
    Inline,
}

/// After a crash: no uncommitted seal escaped (`left`), the restart resumes
/// the snapshot, the derived state is exact, every snapshot valid, and the
/// store keeps committing and resuming.
fn after_crash(root: &Path, before: &[(String, i64, String, String)], context: &str, left: Left) {
    let now = anchors(root);
    match left {
        Left::Unchanged => assert_eq!(
            now, before,
            "{context}: an uncommitted seal must not escape"
        ),
        Left::Committed => {}
        Left::Inline => assert!(
            now.iter().all(|(_, _, payload, _)| {
                serde_json::from_str::<Value>(payload).unwrap()["rows"].is_null()
            }),
            "{context}: the uncommitted rows seal must not escape"
        ),
    }
    let integrity: String = Connection::open(root.join("metadata.sqlite"))
        .unwrap()
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(integrity, "ok", "{context}");
    let mut s = open(root);
    assert_eq!(
        s.startup_resumes(),
        s.branches().unwrap().len() as u64,
        "{context}: the restart resumes the snapshot"
    );
    assert_eq!(
        s.db.query_row("SELECT count(*) FROM incoming_events", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0,
        "{context}: a staged event commits at the restart"
    );
    assert_oracle(&mut s, context);
    assert_snapshots(&s, context);
    for i in 0..3 {
        s.create(&format!("after-{i}"), "file").unwrap();
    }
    let expected = s.projection(&s.active).unwrap();
    drop(s);
    let mut s = open(root);
    assert_eq!(
        s.startup_resumes(),
        s.branches().unwrap().len() as u64,
        "{context}"
    );
    assert_eq!(s.projection(&s.active).unwrap(), expected, "{context}");
    assert_snapshots(&s, context);
    assert_oracle(&mut s, context);
}

/// A crash at every step of every seal (each write, before and inside the
/// commit, at startup), in both layouts, for periodic seals of fast
/// commits, forced seals of inline and prepared slow commits, the startup
/// replay's seal and the switch from inline to rows: the restart resumes the
/// previous snapshot (or, after a durable commit, the new one) and replays,
/// and the derived state is exactly a rebuild's.
#[test]
fn crashes_at_every_seal_step_resume_with_exact_state() {
    const SEAL: [&str; 2] = ["before_startup_snapshot", "startup_snapshot_written"];
    const ROWS: [&str; 3] = [
        "seal_entries_written",
        "seal_conflicts_written",
        "seal_marks_cleared",
    ];
    let mut runs = 0;
    for rows in [false, true] {
        let limit = if rows { 0 } else { usize::MAX };
        let mut points: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        let mut seal_points: Vec<&str> = SEAL.to_vec();
        if rows {
            seal_points.extend(ROWS);
        }
        let mut commit = vec!["before_sql", "sql_committed"];
        commit.extend(&seal_points);
        points.insert("fast", commit.clone());
        points.insert("slow-inline", commit.clone());
        points.insert("slow-prepared", commit);
        let mut startup = vec!["startup_snapshot_restored", "startup_suffix_applied"];
        startup.extend(&seal_points);
        points.insert("open", startup);
        for (step, list) in &points {
            for point in list {
                let _layout = Layout::of(rows);
                let dir = tempfile::tempdir().unwrap();
                let root = crash_scenario(dir.path(), step);
                let before = anchors(&root);
                crash(&root, step, point, Some(limit));
                let context = format!("{} {step} {point}", if rows { "rows" } else { "inline" });
                let left = if *point == "sql_committed" {
                    Left::Committed
                } else {
                    Left::Unchanged
                };
                after_crash(&root, &before, &context, left);
                runs += 1;
            }
        }
    }
    // The switch from inline to rows (a full rows seal) under a small limit.
    for point in [
        "seal_entries_written",
        "seal_conflicts_written",
        "seal_marks_cleared",
        "startup_snapshot_written",
    ] {
        let limit = 96 * 1024;
        let _layout = Layout::limit(limit);
        let dir = tempfile::tempdir().unwrap();
        let root = crash_scenario(dir.path(), "create");
        let before = anchors(&root);
        let s = open(&root);
        assert!(
            !s.check_startup_snapshot(&s.active).unwrap().rows,
            "inline before"
        );
        drop(s);
        crash(&root, "create", point, Some(limit));
        after_crash(&root, &before, &format!("switch {point}"), Left::Inline);
        runs += 1;
    }
    assert!(runs >= 30, "{runs}");
}

// ---------------------------------------------------------------------------
// Random workloads

/// Paths of the alive entries of `s`'s active projection.
fn alive(s: &Store) -> Vec<(String, Entry)> {
    let p = s.projection(&s.active).unwrap();
    let mut out = vec![];
    for entry in p.entries.values().filter(|e| e.alive) {
        let mut path = vec![entry.name.clone()];
        let mut parent = entry.parent.clone();
        let mut ok = true;
        while parent != ROOT {
            match p.entries.get(&parent).filter(|e| e.alive) {
                Some(dir) => {
                    path.push(dir.name.clone());
                    parent = dir.parent.clone();
                }
                None => {
                    ok = false;
                    break;
                }
            }
            if path.len() > 16 {
                ok = false;
                break;
            }
        }
        if ok {
            path.reverse();
            out.push((path.join("/"), entry.clone()));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// One random operation on `s` (errors such as name collisions are part of
/// the workload and ignored).
fn random_op(s: &mut Store, rng: &mut StdRng) {
    const NAMES: [&str; 6] = ["n0", "n1", "N1", "n2", "n3.txt", "N3.TXT"];
    let entries = alive(s);
    let dirs: Vec<String> = std::iter::once(String::new())
        .chain(
            entries
                .iter()
                .filter(|(_, e)| e.kind == "directory")
                .map(|(path, _)| path.clone()),
        )
        .collect();
    let target = |rng: &mut StdRng| {
        let dir = dirs.choose(rng).unwrap();
        let name = NAMES.choose(rng).unwrap();
        if dir.is_empty() {
            name.to_string()
        } else {
            format!("{dir}/{name}")
        }
    };
    match rng.gen_range(0..9) {
        0 | 1 => {
            let kind = if rng.gen_bool(0.7) {
                "file"
            } else {
                "directory"
            };
            let _ = s.create(&target(rng), kind);
        }
        2..=4 => {
            let files: Vec<&Entry> = entries
                .iter()
                .map(|(_, e)| e)
                .filter(|e| e.kind == "file")
                .collect();
            if let Some(file) = files.choose(rng) {
                let stale = rng.gen_bool(0.2);
                let base = if stale {
                    None
                } else {
                    file.revisions.get("content").cloned()
                };
                let bytes = format!("{}", rng.r#gen::<u32>());
                let _ = s.write_revision(&file.id, base, bytes.as_bytes());
            }
        }
        5 => {
            if let Some((_, entry)) = entries.choose(rng) {
                let _ = s.rename(&entry.id, &target(rng), rng.gen_bool(0.3));
            }
        }
        6 => {
            if let Some((_, entry)) = entries.choose(rng) {
                let _ = s.delete(&entry.id);
            }
        }
        _ => {
            let open: Vec<Conflict> = s
                .conflicts()
                .unwrap()
                .into_iter()
                .filter(|c| c.resolved_by.is_none() && c.field != "namespace")
                .collect();
            if let Some(conflict) = open.choose(rng) {
                let selected = conflict.alternatives.choose(rng).unwrap().revision.clone();
                let _ = s.resolve(std::slice::from_ref(&conflict.id), &selected);
            }
        }
    }
}

/// A two-replica random workload in one layout: random operations on both
/// replicas, deliveries in both directions (legacy receive, or staging plus
/// the prepared off-lock path), restarts and crashes of B at random seal
/// steps. After every step B's derived state is exactly a rebuild's (strict
/// oracle) and every snapshot is valid.
fn random_workload(seed: u64, rows: bool, steps: usize) {
    let started = std::time::Instant::now();
    let _layout = Layout::of(rows);
    let limit = if rows { 0 } else { usize::MAX };
    let dir = tempfile::tempdir().unwrap();
    let mut a = Store::open(&dir.path().join("a"), Some(REPO)).unwrap();
    a.create("d0", "directory").unwrap();
    a.create("f0", "file").unwrap();
    let (mut a, b) = clone_replica(a, &dir.path().join("b")).unwrap();
    let allowed = devices([&a, &b]);
    let root = b.root.clone();
    let mut b = Some(b);
    let mut rng = StdRng::seed_from_u64(seed);
    const CRASH_POINTS: [&str; 5] = [
        "seal_entries_written",
        "seal_marks_cleared",
        "before_startup_snapshot",
        "startup_snapshot_written",
        "before_sql",
    ];
    for step in 0..steps {
        let context = format!(
            "seed {seed} {} step {step}",
            if rows { "rows" } else { "inline" }
        );
        match rng.gen_range(0..20) {
            0..=6 => random_op(&mut a, &mut rng),
            7..=12 => random_op(b.as_mut().unwrap(), &mut rng),
            13 | 14 => {
                let to = b.as_mut().unwrap();
                sync_into(&a, to, &allowed).unwrap();
            }
            15 | 16 => {
                let to = b.as_mut().unwrap();
                let read = Store::open_secondary(&to.root, &to.repo, &to.device).unwrap();
                let events = missing(&a, to);
                if !events.is_empty() {
                    activate_prepared(to, &read, events, &allowed);
                }
            }
            17 => {
                let from = b.as_ref().unwrap();
                let _ = sync_into(from, &mut a, &allowed);
            }
            18 => {
                drop(b.take());
                b = Some(open(&root));
            }
            _ => {
                drop(b.take());
                let point = CRASH_POINTS.choose(&mut rng).unwrap();
                let mut command = Command::new(std::env::current_exe().unwrap());
                command
                    .args([
                        "seal_crash_child",
                        "--exact",
                        "--nocapture",
                        "--test-threads",
                        "1",
                    ])
                    .env(CHILD, &root)
                    .env(STEP, "touch")
                    .env("TKFS_FAULT", point)
                    .env("TKFS_SNAPSHOT_INLINE_LIMIT", limit.to_string());
                let output = command.output().unwrap();
                // Inline seals have no row steps: the child then finishes.
                assert!(
                    matches!(output.status.code(), Some(86) | Some(0)),
                    "{context}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
                b = Some(open(&root));
                assert_eq!(
                    b.as_ref().unwrap().startup_resumes(),
                    b.as_ref().unwrap().branches().unwrap().len() as u64,
                    "{context}: resumed after a crash at {point}"
                );
            }
        }
        let s = b.as_mut().unwrap();
        assert_oracle(s, &context);
        assert_snapshots(s, &context);
    }
    // Converge, then both replicas agree.
    let s = b.as_mut().unwrap();
    sync_into(&a, s, &allowed).unwrap();
    sync_into(s, &mut a, &allowed).unwrap();
    assert_eq!(
        s.projection(&s.active).unwrap(),
        a.projection(&a.active).unwrap(),
        "seed {seed}: converged"
    );
    assert_oracle(s, "converged");
    assert_oracle(&mut a, "converged A");
    let events = journal(s, &s.active.clone()).len();
    eprintln!(
        "workload seed {seed} {}: {steps} steps, {events} events, {:.1} s",
        if rows { "rows" } else { "inline" },
        started.elapsed().as_secs_f64()
    );
}

#[test]
fn random_workloads_and_crashes_keep_derived_state_exact() {
    for seed in 0..3 {
        random_workload(seed, true, 70);
        random_workload(seed + 100, false, 50);
    }
}

/// More seeds and steps (release): seeds `TKFS_SEAL_SEED_START` (default 0)
/// to `+ TKFS_SEAL_SEEDS` (default 20), each in both layouts (40 workloads
/// by default; shards run in parallel with disjoint ranges).
#[test]
#[ignore = "release: cargo test --release --test seal_path -- --ignored"]
fn random_workloads_and_crashes_release() {
    let env = |key: &str, default: u64| {
        std::env::var(key)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    let start = env("TKFS_SEAL_SEED_START", 0);
    for seed in start..start + env("TKFS_SEAL_SEEDS", 20) {
        random_workload(seed, true, 200);
        random_workload(seed + 1000, false, 150);
    }
}

// ---------------------------------------------------------------------------
// Fallbacks

/// Damaged sealed rows, a damaged journal state, an older (format 1) anchor
/// and stale side tables are never trusted: the open reconstructs, drops a
/// stale header row and reseals a valid snapshot. A stale header row next to
/// a valid snapshot is dropped by the restore.
#[test]
fn damaged_rows_and_older_snapshots_are_reconstructed() {
    let _layout = Layout::rows();
    let dir = tempfile::tempdir().unwrap();
    let mut a = Store::open(&dir.path().join("a"), Some(REPO)).unwrap();
    a.create("file", "file").unwrap();
    linear_history(&mut a, 30).unwrap();
    let (mut a, mut b) = clone_replica(a, &dir.path().join("b")).unwrap();
    let allowed = devices([&a, &b]);
    save(&mut a, "file", b"from a");
    save(&mut b, "file", b"from b");
    sync_into(&a, &mut b, &allowed).unwrap();
    let root = b.root.clone();
    drop(b);
    let b = open(&root);
    assert_eq!(b.startup_resumes(), 1);
    let expected = b.projection(&b.active).unwrap();
    let conflicts = stored_conflicts(&b, &b.active);
    assert_eq!(conflicts.len(), 1);
    assert_eq!(assert_snapshots(&b, "sealed"), 1);
    let branch = b.active.clone();
    drop(b);
    let damages: [(&str, bool); 10] = [
        ("entry", false),
        ("missing_entry", false),
        ("extra_entry", false),
        ("conflict_resolved", false),
        ("missing_conflict", false),
        ("rows_digest", false),
        ("journal_state", false),
        ("format_1", false),
        ("format_1_stale_header", false),
        ("stale_header", true),
    ];
    for (damage, resumes) in damages {
        let copy = tempfile::tempdir().unwrap();
        let state = copy.path().join("b");
        copy_dir(&root, &state).unwrap();
        let db = Connection::open(state.join("metadata.sqlite")).unwrap();
        let rewrite_anchor = |db: &Connection, f: &dyn Fn(&mut Value)| {
            let payload: String = db
                .query_row(
                    "SELECT payload FROM startup_snapshots WHERE branch=?",
                    [&branch],
                    |r| r.get(0),
                )
                .unwrap();
            let mut value: Value = serde_json::from_str(&payload).unwrap();
            f(&mut value);
            let payload = serde_json::to_string(&value).unwrap();
            db.execute(
                "UPDATE startup_snapshots SET payload=?,checksum=? WHERE branch=?",
                params![payload, hash(payload.as_bytes()), branch],
            )
            .unwrap();
        };
        let stale_header = |db: &Connection| {
            db.execute(
                "INSERT INTO current_meta(branch,payload) VALUES(?,'{\"conflicts\":[],\"pending\":[\"bogus\"]}')",
                [&branch],
            )
            .unwrap();
        };
        match damage {
            "entry" => {
                db.execute(
                    "UPDATE snapshot_entries SET entry=replace(entry,'\"alive\":true','\"alive\":false') WHERE rowid=(SELECT min(rowid) FROM snapshot_entries)",
                    [],
                )
                .unwrap();
            }
            "missing_entry" => {
                db.execute(
                    "DELETE FROM snapshot_entries WHERE rowid=(SELECT max(rowid) FROM snapshot_entries)",
                    [],
                )
                .unwrap();
            }
            "extra_entry" => {
                db.execute(
                    "INSERT INTO snapshot_entries SELECT branch,'extra',entry,heads FROM snapshot_entries LIMIT 1",
                    [],
                )
                .unwrap();
            }
            "conflict_resolved" => {
                db.execute("UPDATE snapshot_conflicts SET resolved_by='forged'", [])
                    .unwrap();
            }
            "missing_conflict" => {
                db.execute("DELETE FROM snapshot_conflicts", []).unwrap();
            }
            "rows_digest" => rewrite_anchor(&db, &|value| {
                value["rows"]["digest"] = Value::String("00".repeat(32));
            }),
            "journal_state" => rewrite_anchor(&db, &|value| {
                let mut hash = JournalStateForge::from(value["journal_state"].as_str().unwrap());
                hash.flip();
                value["journal_state"] = Value::String(hash.0);
            }),
            "format_1" | "format_1_stale_header" => {
                // What an older binary would find, or leave after reconstructing:
                // format 1 without the format-2 fields.
                rewrite_anchor(&db, &|value| {
                    value["format"] = Value::from(1);
                    let object = value.as_object_mut().unwrap();
                    object.remove("journal_state");
                    object.remove("rows");
                });
                if damage == "format_1_stale_header" {
                    stale_header(&db);
                }
            }
            "stale_header" => stale_header(&db),
            _ => unreachable!(),
        }
        drop(db);
        let mut s = open(&state);
        assert_eq!(s.startup_resumes(), u64::from(resumes), "{damage}");
        assert_eq!(s.projection(&s.active).unwrap(), expected, "{damage}");
        assert_eq!(stored_conflicts(&s, &s.active), conflicts, "{damage}");
        assert_eq!(
            header(&s),
            None,
            "{damage}: no stale header survives an open"
        );
        assert_snapshots(&s, damage);
        assert_oracle(&mut s, damage);
    }
}

/// A sealed store with a conflict row, at its tip (rows layout).
fn sealed_with_conflict(root: &Path) {
    let mut s = open(root);
    s.create("file", "file").unwrap();
    for i in 0..60 {
        s.create(&format!("f{i:03}"), "file").unwrap();
    }
    let entry = s.lookup("file").unwrap();
    let (content, alive) = (
        entry.revisions.get("content").cloned(),
        entry.revisions.get("alive").cloned(),
    );
    s.write_with_base(&entry.id, content.clone(), alive.clone(), b"one")
        .unwrap();
    s.write_with_base(&entry.id, content, alive, b"two")
        .unwrap();
    assert_eq!(s.conflicts().unwrap().len(), 1);
    drop(s);
    let s = open(root);
    assert_eq!(s.startup_resumes(), 1);
    assert_eq!(assert_snapshots(&s, "sealed"), 1);
}

/// Damages of the snapshot tables' columns: a non-UTF-8 text or a BLOB
/// where a seal writes UTF-8 text, a cursor that is not an integer. `{e}`
/// is the entity whose row is damaged.
const COLUMN_DAMAGES: [(&str, &str); 10] = [
    (
        "anchor_payload_not_utf8",
        "UPDATE startup_snapshots SET payload=CAST(X'FF' AS TEXT)||payload",
    ),
    (
        "anchor_payload_blob",
        "UPDATE startup_snapshots SET payload=CAST(payload AS BLOB)",
    ),
    (
        "anchor_checksum_blob",
        "UPDATE startup_snapshots SET checksum=CAST(checksum AS BLOB)",
    ),
    (
        "anchor_cursor_text",
        "UPDATE startup_snapshots SET cursor='damaged'",
    ),
    (
        "entry_not_utf8",
        "UPDATE snapshot_entries SET entry=CAST(X'FF' AS TEXT)||entry WHERE entity='{e}'",
    ),
    (
        "heads_blob",
        "UPDATE snapshot_entries SET heads=CAST(heads AS BLOB) WHERE entity='{e}'",
    ),
    (
        "conflict_payload_blob",
        "UPDATE snapshot_conflicts SET payload=CAST(payload AS BLOB)",
    ),
    (
        "conflict_resolved_blob",
        "UPDATE snapshot_conflicts SET resolved_by=X'FF'",
    ),
    (
        "entity_mark_blob",
        "INSERT INTO snapshot_dirty(branch,kind,key) SELECT branch,0,X'FF' FROM startup_snapshots",
    ),
    (
        "conflict_mark_not_utf8",
        "INSERT INTO snapshot_dirty(branch,kind,key) SELECT branch,1,CAST(X'FFFE' AS TEXT) FROM startup_snapshots",
    ),
];

/// Commits after a damage: the conflict's review (a slow commit and its
/// forced seal, which reads the conflict's sealed row), then saves of
/// `f000` (whose sealed row the entity damages hit) through two periodic
/// seals. Every commit must succeed, with `full` a seal must have been full
/// (it replaced the damage), and every snapshot is valid afterwards.
fn commits_keep_sealing(s: &mut Store, context: &str, full: bool) {
    let before = s.seal_stats();
    let branch = s.active.clone();
    let open: Vec<Conflict> = s
        .conflicts()
        .unwrap()
        .into_iter()
        .filter(|c| c.resolved_by.is_none())
        .collect();
    if let Some(conflict) = open.first() {
        let selected = conflict.alternatives[0].revision.clone();
        s.resolve(std::slice::from_ref(&conflict.id), &selected)
            .unwrap_or_else(|error| panic!("{context}: the review: {error:#}"));
    }
    let saves = s.seal_due_in(&branch).unwrap().max(1) + MIN_SEAL_INTERVAL;
    for i in 0..saves {
        let entry = s.lookup("f000").unwrap();
        s.write_revision(
            &entry.id,
            entry.revisions.get("content").cloned(),
            format!("{context} {i}").as_bytes(),
        )
        .unwrap_or_else(|error| panic!("{context}: save {i}: {error:#}"));
    }
    let seals = delta(s.seal_stats(), before);
    assert!(seals.seals >= 2, "{context}: {seals:?}");
    assert!(
        !full || seals.rows_full + seals.inline >= 1,
        "{context}: a full seal replaced the damage: {seals:?}"
    );
    assert_snapshots(s, context);
    assert_oracle(s, context);
}

/// Invariant 5 (snapshots are disposable), whatever the snapshot tables
/// hold: a non-UTF-8 or BLOB anchor, checksum, sealed row or change mark,
/// or a cursor that is not an integer, never blocks an open or a commit.
/// Found by an open, the store reconstructs (marks are only cleared by the
/// restore, which resumes); found by a commit's seal (the store open when
/// the damage lands), the seal is full and replaces it. Either way the
/// store keeps committing and sealing, and the next open resumes.
#[test]
fn damaged_snapshot_columns_never_block_opens_or_commits() {
    let _layout = Layout::rows();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("b");
    sealed_with_conflict(&root);
    let (expected, conflicts, f000) = {
        let s = open(&root);
        (
            s.projection(&s.active).unwrap(),
            stored_conflicts(&s, &s.active),
            s.lookup("f000").unwrap().id,
        )
    };
    for (damage, sql) in COLUMN_DAMAGES {
        let sql = sql.replace("{e}", &f000);
        let marks_only = damage.ends_with("_mark_blob") || damage.ends_with("_mark_not_utf8");
        // Found by an open.
        let copy = tempfile::tempdir().unwrap();
        let state = copy.path().join("b");
        copy_dir(&root, &state).unwrap();
        let db = Connection::open(state.join("metadata.sqlite")).unwrap();
        assert_eq!(db.execute(&sql, []).unwrap(), 1, "{damage}");
        drop(db);
        let context = format!("{damage} at open");
        let mut s = Store::open(&state, Some(REPO))
            .unwrap_or_else(|error| panic!("{context}: the open failed: {error:#}"));
        assert_eq!(
            s.startup_resumes(),
            u64::from(marks_only),
            "{context}: reconstructed unless only marks were damaged"
        );
        assert_eq!(s.projection(&s.active).unwrap(), expected, "{context}");
        assert_eq!(stored_conflicts(&s, &s.active), conflicts, "{context}");
        assert_snapshots(&s, &context);
        commits_keep_sealing(&mut s, &context, false);
        drop(s);
        // Found by a commit's seal: the damage lands while the store is
        // open (another connection), after its open resumed.
        let copy = tempfile::tempdir().unwrap();
        let state = copy.path().join("b");
        copy_dir(&root, &state).unwrap();
        let context = format!("{damage} at a commit");
        let mut s = open(&state);
        assert_eq!(s.startup_resumes(), 1, "{context}");
        let db = Connection::open(state.join("metadata.sqlite")).unwrap();
        assert_eq!(db.execute(&sql, []).unwrap(), 1, "{context}");
        drop(db);
        commits_keep_sealing(&mut s, &context, true);
        let expected = s.projection(&s.active).unwrap();
        drop(s);
        let mut s = open(&state);
        assert_eq!(s.startup_resumes(), 1, "{context}: the next open resumes");
        assert_eq!(s.projection(&s.active).unwrap(), expected, "{context}");
        assert_oracle(&mut s, &context);
    }
}

/// Flips one hex digit of a journal state (it then finalizes to another
/// digest).
struct JournalStateForge(String);
impl JournalStateForge {
    fn from(text: &str) -> Self {
        Self(text.to_owned())
    }
    fn flip(&mut self) {
        let mut chars: Vec<char> = self.0.chars().collect();
        chars[3] = if chars[3] == '0' { '1' } else { '0' };
        self.0 = chars.into_iter().collect();
    }
}

/// The startup snapshots of every branch, inline and rows, stay valid
/// across forks and checkouts (a branch sealed as rows while another is
/// sealed inline); each branch reseals from its own marks.
#[test]
fn branches_seal_from_their_own_marks() {
    let _layout = Layout::limit(64 * 1024);
    let dir = tempfile::tempdir().unwrap();
    let mut s = open(dir.path());
    s.create("small", "file").unwrap();
    let main = s.active.clone();
    let side = s.fork("side", true).unwrap();
    s.checkout(&side.id).unwrap();
    for i in 0..300 {
        s.create(&format!("{}-{i:04}", "s".repeat(60)), "file")
            .unwrap();
    }
    // A restart while `side` is checked out seals its tail (an inactive
    // branch's namespace tail is reconstructed, not replayed).
    drop(s);
    let mut s = open(dir.path());
    s.checkout(&main).unwrap();
    for i in 0..150 {
        save(&mut s, "small", format!("{i}").as_bytes());
    }
    let side_rows = s.check_startup_snapshot(&side.id).unwrap().rows;
    let main_rows = s.check_startup_snapshot(&main).unwrap().rows;
    assert!(
        side_rows && !main_rows,
        "side {side_rows}, main {main_rows}"
    );
    assert_snapshots(&s, "two layouts");
    let expected: BTreeMap<String, Projection> = s
        .branches()
        .unwrap()
        .into_iter()
        .map(|b| (b.id.clone(), s.projection(&b.id).unwrap()))
        .collect();
    drop(s);
    let mut s = open(dir.path());
    assert_eq!(s.startup_resumes(), 2);
    for (branch, projection) in expected {
        assert_eq!(s.projection(&branch).unwrap(), projection);
    }
    assert_oracle(&mut s, "two layouts after a restart");
}

// ---------------------------------------------------------------------------
// Per-branch journal index and restore cost

/// Every journal row's branch, as the journal itself names it.
fn journal_branches(db: &Connection) -> Vec<(i64, String)> {
    db.prepare("SELECT s.seq,e.branch FROM journal_sequence s JOIN events e ON e.id=s.event ORDER BY s.seq")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

/// The per-branch journal index (`journal_branch`).
fn indexed_branches(db: &Connection) -> Vec<(i64, String)> {
    db.prepare("SELECT seq,branch FROM journal_branch ORDER BY seq")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

/// The sealed cursor of `branch`.
fn sealed_cursor(s: &Store, branch: &str) -> i64 {
    s.db.query_row(
        "SELECT cursor FROM startup_snapshots WHERE branch=?",
        [branch],
        |r| r.get(0),
    )
    .unwrap()
}

/// A seal hashes only its own branch's journal rows since its previous
/// seal, however many rows other branches added in between: `main`'s first
/// seal after 300 commits on a side branch hashes `main`'s one new row, not
/// the 300 (both layouts; the index is `journal_branch`).
#[test]
fn a_seal_hashes_only_its_own_branchs_journal_rows() {
    for rows in [true, false] {
        let _layout = Layout::of(rows);
        let dir = tempfile::tempdir().unwrap();
        let mut s = open(dir.path());
        s.create("small", "file").unwrap();
        let main = s.active.clone();
        drop(s);
        let mut s = open(dir.path());
        let side = s.fork("side", true).unwrap();
        s.checkout(&side.id).unwrap();
        for i in 0..300 {
            s.create(&format!("side-{i:04}"), "file").unwrap();
        }
        s.checkout(&main).unwrap();
        assert!(
            s.seal_due_in(&main).unwrap() <= 1,
            "main's next commit seals"
        );
        let previous = sealed_cursor(&s, &main);
        let before = s.seal_stats();
        save(&mut s, "small", b"after the gap");
        let seal = delta(s.seal_stats(), before);
        assert_eq!(seal.seals, 1, "{seal:?}");
        let now = sealed_cursor(&s, &main);
        let own = journal(&s, &main)
            .iter()
            .filter(|(seq, _)| *seq > previous && *seq <= now)
            .count() as u64;
        let all: i64 =
            s.db.query_row(
                "SELECT count(*) FROM journal_sequence WHERE seq>? AND seq<=?",
                [previous, now],
                |r| r.get(0),
            )
            .unwrap();
        assert!(all > 300, "the gap: {all} journal rows");
        assert_eq!(own, 1, "main's own rows since its seal");
        assert_eq!(
            seal.journal_rows, own,
            "the seal hashed main's rows only: {seal:?}"
        );
        assert_eq!(indexed_branches(&s.db), journal_branches(&s.db));
        assert_snapshots(&s, "after the gap");
        drop(s);
        let mut s = open(dir.path());
        assert!(s.startup_resumes() >= 1);
        assert_oracle(&mut s, "after the gap and a restart");
    }
}

/// A store without the per-branch journal index (written by a binary
/// before it) gets it built from the journal at its next open, row for row
/// as the triggers keep it (inserts, a changed event, a deleted event), and
/// seals incrementally from it.
#[test]
fn the_journal_index_is_built_for_an_older_store() {
    let _layout = Layout::rows();
    let dir = tempfile::tempdir().unwrap();
    let mut s = open(dir.path());
    for i in 0..40 {
        s.create(&format!("f{i:03}"), "file").unwrap();
    }
    let main = s.active.clone();
    let side = s.fork("side", true).unwrap();
    s.checkout(&side.id).unwrap();
    for i in 0..40 {
        s.create(&format!("s{i:03}"), "file").unwrap();
    }
    s.checkout(&main).unwrap();
    assert_eq!(indexed_branches(&s.db), journal_branches(&s.db));
    drop(s);
    let db = Connection::open(dir.path().join("metadata.sqlite")).unwrap();
    db.execute_batch(
        "DROP TRIGGER journal_branch_insert; DROP TRIGGER journal_branch_update;
        DROP TRIGGER journal_branch_delete; DROP TABLE journal_branch;
        DELETE FROM config WHERE key LIKE 'snapshot_journal_branch%';",
    )
    .unwrap();
    drop(db);
    let mut s = open(dir.path());
    assert_eq!(
        indexed_branches(&s.db),
        journal_branches(&s.db),
        "built from the journal"
    );
    let config = |s: &Store, key: &str| -> Option<String> {
        s.db.query_row("SELECT value FROM config WHERE key=?", [key], |r| r.get(0))
            .optional()
            .unwrap()
    };
    assert_eq!(
        config(&s, "snapshot_journal_branch_v1").as_deref(),
        Some("1")
    );
    assert_eq!(config(&s, "snapshot_journal_branch_v1_cursor"), None);
    let before = s.seal_stats();
    for i in 0..MIN_SEAL_INTERVAL {
        save(&mut s, "f000", format!("{i}").as_bytes());
    }
    let seals = delta(s.seal_stats(), before);
    assert_eq!(seals.rows_incremental, 1, "{seals:?}");
    assert_eq!(indexed_branches(&s.db), journal_branches(&s.db));
    assert_snapshots(&s, "after the build");
    assert_oracle(&mut s, "after the build");
    drop(s);
    // Its triggers follow a changed and a deleted journal row (repair tools,
    // older binaries) as `journal_sequence` does.
    let db = Connection::open(dir.path().join("metadata.sqlite")).unwrap();
    let (first, last): (String, String) = db
        .query_row(
            "SELECT (SELECT event FROM journal_sequence ORDER BY seq LIMIT 1),(SELECT event FROM journal_sequence ORDER BY seq DESC LIMIT 1)",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    db.execute(
        "UPDATE events SET branch=? WHERE id=?",
        params![side.id, first],
    )
    .unwrap();
    db.execute("DELETE FROM events WHERE id=?", [last]).unwrap();
    assert_eq!(indexed_branches(&db), journal_branches(&db));
}

/// A restart at the tip of a large rows snapshot writes no projection row
/// (`projection_version` is unchanged), and one with a tail writes only the
/// tail's entities, not every entity (WP3.INT rewrote `current_state` whole
/// at every open; this package's first restore copied every row).
#[test]
fn a_restart_writes_only_the_rows_that_differ_from_the_snapshot() {
    let _layout = Layout::rows();
    let dir = tempfile::tempdir().unwrap();
    let mut s = open(dir.path());
    linear_history(&mut s, FOLD_MAX_ENTRIES + 50).unwrap();
    drop(s);
    let read = |sql: &str| -> i64 {
        Connection::open(dir.path().join("metadata.sqlite"))
            .unwrap()
            .query_row(sql, [], |r| r.get(0))
            .unwrap()
    };
    let version = || read("SELECT version FROM projection_version WHERE id=1");
    let s = open(dir.path());
    assert_eq!(s.startup_resumes(), 1);
    let expected = s.projection(&s.active).unwrap();
    assert!(expected.entries.len() > FOLD_MAX_ENTRIES);
    drop(s);
    assert_eq!(
        read("SELECT count(*) FROM current_parts"),
        expected.entries.len() as i64,
        "a large projection is stored as one row per entity"
    );
    let before = version();
    let s = open(dir.path());
    assert_eq!(s.startup_resumes(), 1);
    assert_eq!(s.projection(&s.active).unwrap(), expected);
    drop(s);
    assert_eq!(
        version(),
        before,
        "an open at the tip writes no projection row"
    );
    // A tail of 20 saves.
    let mut s = open(dir.path());
    for i in 0..20 {
        save(&mut s, &format!("lin-{i:06}.txt"), b"tail");
    }
    let expected = s.projection(&s.active).unwrap();
    drop(s);
    let before = version();
    let mut s = open(dir.path());
    let writes = version() - before;
    assert_eq!(s.startup_resumes(), 1);
    assert_eq!(s.projection(&s.active).unwrap(), expected);
    // The 20 rows the restore resets (a delete and an insert each) and the
    // replay writes again.
    assert!(
        writes <= 3 * 20 + 4,
        "the open wrote {writes} projection rows for a 20-event tail"
    );
    assert_eq!(assert_snapshots(&s, "after the tail's replay"), 1);
    assert_oracle(&mut s, "after the tail's replay");
}
