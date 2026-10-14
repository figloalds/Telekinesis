//! WP1.6: the inbox state machine (ready / waiting / retry / quarantined /
//! blocked), bounded activation slices, classified quarantine and the remote
//! fast paths (REVAMP_PLAN §4.4, §5.1).
//!
//! Three scenarios need process-wide settings (`util::fault` points and the
//! once-read `TKFS_REMOTE_FASTPATH`), so they run in a child process: the
//! parent test re-runs this binary with `--exact <child>` and the child
//! test is a no-op unless its `TKFS_ACTIVATION_CHILD_*` variable is set.
mod support;

use rand::{Rng, SeedableRng, rngs::StdRng, seq::SliceRandom};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    process::Command,
    thread,
    time::{Duration, Instant},
};
use support::{causal_sort, copy_legacy_worker, devices, invalid_kind_changes, staged_inbox};
use tkfs::{
    core::{
        Bundle, Change, Entry, Event, Projection, ROOT, Store,
        commit::remote_fast_paths_enabled,
        id,
        inbox::{CommitOutcome, Prepared, SliceBudget, StagedEvent},
    },
    paired_sync::{self, SyncRoute},
    runtime::Engine,
    util::fault::{FaultAction, FaultConfig},
};

const CHILD_BUSY: &str = "TKFS_ACTIVATION_CHILD_BUSY";
const CHILD_WINDOW: &str = "TKFS_ACTIVATION_CHILD_WINDOW";
const CHILD_REPLAY: &str = "TKFS_ACTIVATION_CHILD_REPLAY";

fn store(path: &Path, repo: &str) -> Store {
    Store::initialize(path, repo, &id()).unwrap()
}

#[track_caller]
fn assert_oracle_empty(s: &mut Store, step: &str) {
    let diff = s.verify_derived_state().unwrap();
    assert!(diff.is_empty(), "{step}: {diff:#?}");
}

fn page(s: &Store, events: Vec<Event>, objects: &BTreeMap<String, String>) -> Bundle {
    let wanted: BTreeSet<String> = events.iter().flat_map(Event::objects).collect();
    Bundle {
        repo: s.repo.clone(),
        branches: vec![],
        objects: objects
            .iter()
            .filter(|(h, _)| wanted.contains(*h))
            .map(|(h, v)| (h.clone(), v.clone()))
            .collect(),
        events,
    }
}

/// Shared events of `from` that `to` lacks, causally sorted, with objects.
fn missing(from: &Store, to: &Store) -> Bundle {
    let mut bundle = from
        .shared_bundle(&to.shared_event_inventory().unwrap())
        .unwrap();
    causal_sort(&mut bundle.events);
    bundle
}

fn state_of(s: &Store, id: &str) -> (Option<String>, Option<String>, Option<String>) {
    s.db.query_row(
        "SELECT state,error,blocked_on FROM incoming_events WHERE id=?",
        [id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )
    .unwrap()
}

fn write(s: &mut Store, path: &str, bytes: &[u8]) -> String {
    let entry = s.lookup(path).unwrap();
    s.write_revision(&entry.id, entry.revisions.get("content").cloned(), bytes)
        .unwrap()
}

/// Run this test binary's `child` test in a fresh process with `envs`.
fn run_child(child: &str, envs: &[(&str, String)]) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args([child, "--exact", "--nocapture", "--test-threads", "1"])
        .envs(envs.iter().map(|(k, v)| (*k, v.as_str())))
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success() && stdout.contains("1 passed"),
        "child {child} failed:\n{stdout}\n{stderr}"
    );
}

/// Children delivered before their parents wait (durably, with their missing
/// parents listed) and activate exactly when the parents commit.
#[test]
fn children_before_parents_wait_and_activate_with_their_parents() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = store(&dir.path().join("a"), &repo);
    let mut b = store(&dir.path().join("b"), &repo);
    let allowed = devices([&a, &b]);
    a.create("d", "directory").unwrap();
    a.create("d/f.txt", "file").unwrap();
    write(&mut a, "d/f.txt", b"one");
    write(&mut a, "d/f.txt", b"two");
    let bundle = missing(&a, &b);
    assert_eq!(bundle.events.len(), 4);
    let ids: Vec<String> = bundle.events.iter().map(|e| e.id.clone()).collect();
    let before = b.commit_path_counters();
    // Deliver the three children newest first: each waits.
    for (k, event) in bundle.events.iter().rev().take(3).enumerate() {
        let accepted = b
            .receive(page(&b, vec![event.clone()], &bundle.objects), &allowed)
            .unwrap();
        assert!(accepted.is_empty(), "a child never activates early");
        let counts = b.inbox_counts().unwrap();
        assert_eq!((counts.waiting, counts.ready), (k as u64 + 1, 0));
        assert_eq!(counts.pending_active(), k as u64 + 1);
        // Only the next (undelivered) parent is missing; staged ones are not.
        let parent = &ids[2 - k];
        assert_eq!(b.missing_parents(10).unwrap(), vec![parent.clone()]);
        assert_eq!(state_of(&b, &event.id).0.as_deref(), Some("waiting"));
    }
    assert!(b.shared_event_inventory().unwrap().is_empty());
    // The root arrives: the whole chain activates in the same call.
    let accepted = b
        .receive(
            page(&b, vec![bundle.events[0].clone()], &bundle.objects),
            &allowed,
        )
        .unwrap();
    assert_eq!(
        accepted.iter().collect::<BTreeSet<_>>(),
        ids.iter().collect::<BTreeSet<_>>()
    );
    assert_eq!(b.inbox_counts().unwrap().total(), 0);
    let wait_rows: i64 =
        b.db.query_row("SELECT count(*) FROM incoming_waits", [], |r| r.get(0))
            .unwrap();
    assert_eq!(wait_rows, 0);
    assert_eq!(
        b.read_object(&b.lookup("d/f.txt").unwrap().content.unwrap())
            .unwrap(),
        b"two"
    );
    assert_eq!(
        b.projection(&b.active).unwrap(),
        a.projection(&a.active).unwrap()
    );
    if remote_fast_paths_enabled() {
        let after = b.commit_path_counters();
        assert_eq!((after.fast - before.fast, after.slow - before.slow), (4, 0));
    }
    assert_oracle_empty(&mut b, "after the chain");
}

/// A quarantined event is not pending, unrelated history activates, its
/// descendants are blocked with a reason, and `revalidate` re-queues them.
#[test]
fn quarantine_is_not_pending_blocks_descendants_and_revalidate_requeues() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = store(&dir.path().join("a"), &repo);
    let mut b = store(&dir.path().join("b"), &repo);
    let allowed = devices([&a, &b]);
    a.create("ok.txt", "file").unwrap();
    b.receive(missing(&a, &b), &allowed).unwrap();
    let branch = a.branch(&a.active).unwrap();
    let bad = a.event(&branch, invalid_kind_changes("bad.txt"));
    let mut child = a.event(
        &branch,
        vec![Change {
            entity: id(),
            field: "kind".into(),
            parents: vec![],
            value: json!("directory"),
        }],
    );
    child.dependencies = vec![bad.id.clone()];
    let mut grandchild = child.clone();
    grandchild.id = id();
    grandchild.dependencies = vec![child.id.clone()];
    a.create("unrelated.txt", "file").unwrap();
    let unrelated = missing(&a, &b);
    let mut events = vec![grandchild.clone(), bad.clone(), child.clone()];
    events.extend(unrelated.events.clone());
    let accepted = b
        .receive(page(&b, events, &unrelated.objects), &allowed)
        .unwrap();
    assert_eq!(accepted, vec![unrelated.events[0].id.clone()]);
    assert!(b.lookup("unrelated.txt").is_ok());
    let (state, error, _) = state_of(&b, &bad.id);
    assert_eq!(state.as_deref(), Some("quarantined"));
    assert!(error.unwrap().contains("INVALID_KIND"));
    for descendant in [&child, &grandchild] {
        let (state, error, blocked_on) = state_of(&b, &descendant.id);
        assert_eq!(state.as_deref(), Some("blocked"));
        assert_eq!(
            error.as_deref(),
            Some(format!("QUARANTINED_CAUSAL_ANCESTOR: {}", bad.id).as_str())
        );
        assert_eq!(blocked_on.as_deref(), Some(bad.id.as_str()));
    }
    let counts = b.inbox_counts().unwrap();
    assert_eq!((counts.quarantined, counts.blocked), (1, 2));
    assert_eq!(counts.pending_active(), 0, "quarantine is never pending");
    let listed = b.quarantined(10).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(
        (listed[0].0.as_str(), listed[0].1.as_str()),
        (bad.id.as_str(), "INVALID_KIND")
    );
    // The legacy `error` column still marks quarantined and blocked rows.
    assert_eq!(b.status().unwrap()["incoming_quarantined"], 3);
    // Revalidating the root re-queues it and everything blocked on it.
    assert_eq!(
        b.revalidate(Some(std::slice::from_ref(&bad.id))).unwrap(),
        3
    );
    assert_eq!(b.inbox_counts().unwrap().unclassified, 3);
    let slice = b.activate_slice(SliceBudget::UNBOUNDED).unwrap();
    assert_eq!(slice.quarantined.len(), 1);
    assert_eq!(slice.quarantined[0].0, bad.id);
    assert_eq!(slice.quarantined[0].1.code, "INVALID_KIND");
    let counts = b.inbox_counts().unwrap();
    assert_eq!(
        (counts.quarantined, counts.blocked, counts.pending_active()),
        (1, 2, 0)
    );
    assert_eq!(b.revalidate(None).unwrap(), 3);
    assert_oracle_empty(&mut b, "after quarantine");
}

fn engines() -> (tempfile::TempDir, Engine, Engine, SyncRoute, SyncRoute) {
    let temp = tempfile::tempdir().unwrap();
    let repo = id();
    let (da, db) = (id(), id());
    let a = Engine::new(Store::initialize(&temp.path().join("a"), &repo, &da).unwrap());
    let b = Engine::new(Store::initialize(&temp.path().join("b"), &repo, &db).unwrap());
    let ga = SyncRoute {
        peer: id(),
        repo: repo.clone(),
        local_replica: da.clone(),
        remote_replica: db.clone(),
    };
    let gb = SyncRoute {
        peer: id(),
        repo,
        local_replica: db,
        remote_replica: da,
    };
    (temp, a, b, ga, gb)
}

fn roundtrip(a: &mut Engine, b: &mut Engine, ga: &SyncRoute, gb: &SyncRoute) -> Value {
    let page = paired_sync::offer(a, ga, None).unwrap();
    let sent: BTreeSet<_> = page.bundle.events.iter().map(|e| e.id.clone()).collect();
    let response = paired_sync::worker_control(
        b,
        &json!({"op":"published-exchange","route":gb,"page":page}),
    )
    .unwrap();
    paired_sync::worker_control(
        a,
        &json!({"op":"published-apply","route":ga,"page":response,"request":page.request,"sent":sent}),
    )
    .unwrap()
}

/// The legacy lanes report `metadata_caught_up` with a quarantined row on
/// either side, and their results carry inbox counters instead of the
/// O(repository) `content_status`.
#[test]
fn paired_sync_caught_up_excludes_quarantined_rows() {
    let (_temp, mut a, mut b, ga, gb) = engines();
    let file = a.store.create("one.txt", "file").unwrap();
    a.store
        .write_revision(&file.id, file.revisions.get("content").cloned(), b"one")
        .unwrap();
    roundtrip(&mut a, &mut b, &ga, &gb);
    assert!(b.store.lookup("one.txt").is_ok());
    // An invalid event authored as A, delivered straight to B.
    let branch = a.store.branch(&a.store.active).unwrap();
    let mut bad = a.store.event(&branch, invalid_kind_changes("bad.txt"));
    bad.dependencies.clear();
    let known: BTreeSet<String> = b.store.shared_event_inventory().unwrap();
    let page = support::legacy_page(
        &b.store.repo,
        &ga.local_replica,
        &gb.local_replica,
        &[bad],
        &known,
    );
    paired_sync::worker_control(
        &mut b,
        &json!({"op":"published-exchange","route":gb,"page":page,"metadata_only":true}),
    )
    .unwrap();
    assert_eq!(b.store.inbox_counts().unwrap().quarantined, 1);
    let offer = paired_sync::offer(&b, &gb, None).unwrap();
    assert_eq!(
        offer.pending_events, 0,
        "quarantine is not advertised as pending"
    );
    let from_a = roundtrip(&mut a, &mut b, &ga, &gb);
    assert_eq!(from_a["metadata_caught_up"], true, "{from_a}");
    let from_b = roundtrip(&mut b, &mut a, &gb, &ga);
    assert_eq!(from_b["metadata_caught_up"], true, "{from_b}");
    assert_eq!(from_b["caught_up"], true, "{from_b}");
    assert_eq!(from_b["content"]["incoming"]["quarantined"], 1);
    assert_eq!(from_b["content"]["outgoing_unacknowledged"], 0);
    assert!(
        from_b["content"].get("items").is_none(),
        "no content_status scan"
    );
    assert_eq!(from_b["metadata_progress"]["local_pending"], 0);
    assert_eq!(from_b["metadata_progress"]["local_quarantined"], 1);
    let observation = &b.peer_sync[&gb.remote_replica];
    assert_eq!(observation["metadata_caught_up"], true);
    assert_eq!(observation["content"]["incoming"]["quarantined"], 1);
}

/// Remote create, edit, mkdir, rename, move and delete chains activate on the
/// creation, content and namespace successors; the oracle agrees.
#[test]
fn remote_linear_history_takes_the_fast_paths() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = store(&dir.path().join("a"), &repo);
    let mut b = store(&dir.path().join("b"), &repo);
    let allowed = devices([&a, &b]);
    for i in 0..12 {
        a.create(&format!("dir-{i}"), "directory").unwrap();
        a.create(&format!("dir-{i}/file.txt"), "file").unwrap();
        write(
            &mut a,
            &format!("dir-{i}/file.txt"),
            format!("v{i}").as_bytes(),
        );
        let file = a.lookup(&format!("dir-{i}/file.txt")).unwrap();
        a.rename(&file.id, &format!("dir-{i}/renamed.txt"), false)
            .unwrap();
        if i > 0 {
            // Move the previous directory into this one, then delete a file.
            let previous = a.lookup(&format!("dir-{}", i - 1)).unwrap();
            a.rename(&previous.id, &format!("dir-{i}/nested"), false)
                .unwrap();
        }
        a.create(&format!("dir-{i}/gone.txt"), "file").unwrap();
        let gone = a.lookup(&format!("dir-{i}/gone.txt")).unwrap();
        a.delete(&gone.id).unwrap();
    }
    // Temp-file replacement (one location + tombstone event).
    a.create("dir-11/renamed.txt.tmp", "file").unwrap();
    let temp = a.lookup("dir-11/renamed.txt.tmp").unwrap();
    a.rename(&temp.id, "dir-11/renamed.txt", true).unwrap();
    let bundle = missing(&a, &b);
    let total = bundle.events.len() as u64;
    let before = b.commit_path_counters();
    for chunk in bundle.events.chunks(16) {
        b.receive(page(&b, chunk.to_vec(), &bundle.objects), &allowed)
            .unwrap();
        assert_oracle_empty(&mut b, "remote linear chunk");
    }
    let after = b.commit_path_counters();
    assert_eq!(after.fast + after.slow - before.fast - before.slow, total);
    if remote_fast_paths_enabled() {
        assert_eq!(
            after.slow - before.slow,
            0,
            "every remote event took a successor"
        );
    }
    assert_eq!(
        b.projection(&b.active).unwrap(),
        a.projection(&a.active).unwrap()
    );
    assert!(b.lookup("dir-11/nested/nested/renamed.txt").is_ok());
}

/// Conflict rows that were all resolved no longer force the slow path, for
/// local and for remote events.
#[test]
fn creates_after_only_resolved_conflicts_take_the_fast_path() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = store(&dir.path().join("a"), &repo);
    let mut b = store(&dir.path().join("b"), &repo);
    let allowed = devices([&a, &b]);
    a.create("shared.txt", "file").unwrap();
    b.receive(missing(&a, &b), &allowed).unwrap();
    write(&mut a, "shared.txt", b"from a");
    write(&mut b, "shared.txt", b"from b");
    b.receive(missing(&a, &b), &allowed).unwrap();
    a.receive(missing(&b, &a), &allowed).unwrap();
    let conflict = a
        .conflicts()
        .unwrap()
        .into_iter()
        .find(|c| c.resolved_by.is_none())
        .expect("a content conflict");
    a.resolve(&[conflict.id], &conflict.alternatives[0].revision.clone())
        .unwrap();
    b.receive(missing(&a, &b), &allowed).unwrap();
    for s in [&a, &b] {
        assert!(!s.conflicts().unwrap().is_empty());
        assert!(!s.branch_has_unresolved_conflicts(&s.active).unwrap());
    }
    let local = b.commit_path_counters();
    b.create("after-resolve.txt", "file").unwrap();
    let remote = a.commit_path_counters();
    a.receive(missing(&b, &a), &allowed).unwrap();
    if remote_fast_paths_enabled() {
        assert_eq!(
            b.commit_path_counters().fast,
            local.fast + 1,
            "local create"
        );
        assert_eq!(b.commit_path_counters().slow, local.slow);
        assert_eq!(
            a.commit_path_counters().fast,
            remote.fast + 1,
            "remote create"
        );
        assert_eq!(a.commit_path_counters().slow, remote.slow);
    }
    assert!(a.lookup("after-resolve.txt").is_ok());
    assert_oracle_empty(&mut a, "a");
    assert_oracle_empty(&mut b, "b");
}

/// `stage_incoming` reports every event and never activates; sources are
/// recorded and cleaned up with their row; staging works on a secondary
/// connection.
#[test]
fn stage_incoming_reports_each_event_and_never_activates() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = store(&dir.path().join("a"), &repo);
    let mut b = store(&dir.path().join("b"), &repo);
    let allowed = devices([&a, &b]);
    a.create("first.txt", "file").unwrap();
    b.receive(missing(&a, &b), &allowed).unwrap();
    let first = b.events().unwrap().remove(0);
    a.create("second.txt", "file").unwrap();
    let second = missing(&a, &b).events.remove(0);
    let staged_event = |event: &Event, source: Option<(&str, i64)>| StagedEvent {
        event: event.clone(),
        source: source.map(|(peer, seq)| (peer.to_owned(), seq)),
    };
    // On a secondary connection, as the feed will stage.
    let secondary = Store::open_secondary(&b.root, &b.repo, &b.device).unwrap();
    let outcome = secondary
        .stage_incoming(vec![staged_event(&second, Some(("peer-x", 7)))], &allowed)
        .unwrap();
    assert_eq!(outcome.staged, vec![second.id.clone()]);
    let mut unauthorized = second.clone();
    unauthorized.id = id();
    unauthorized.device = "stranger".into();
    let mut private = second.clone();
    private.id = id();
    private.branch.shared = false;
    let mut reused = second.clone();
    reused.created_ms += 1;
    let mut foreign = second.clone();
    foreign.id = id();
    foreign.repo = id();
    let outcome = b
        .stage_incoming(
            vec![
                staged_event(&first, None),
                staged_event(&second, Some(("peer-y", 3))),
                staged_event(&unauthorized, None),
                staged_event(&private, None),
                staged_event(&reused, None),
                staged_event(&foreign, None),
            ],
            &allowed,
        )
        .unwrap();
    assert_eq!(outcome.already_committed, vec![first.id.clone()]);
    assert_eq!(outcome.already_staged, vec![second.id.clone()]);
    assert!(outcome.staged.is_empty());
    let codes: Vec<(&str, &str)> = outcome
        .rejected
        .iter()
        .map(|(id, e)| (id.as_str(), e.code.as_str()))
        .collect();
    assert_eq!(
        codes,
        vec![
            (unauthorized.id.as_str(), "UNAUTHORIZED_EVENT"),
            (private.id.as_str(), "PRIVATE_EVENT_REJECTED"),
            (reused.id.as_str(), "EVENT_ID_REUSED"),
            (foreign.id.as_str(), "REPOSITORY_MISMATCH"),
        ]
    );
    // Staging never activates.
    assert!(b.lookup("second.txt").is_err());
    assert_eq!(b.inbox_counts().unwrap().ready, 1);
    let sources: Vec<(String, i64)> =
        b.db.prepare("SELECT peer,seq FROM incoming_sources WHERE event=? ORDER BY peer")
            .unwrap()
            .query_map([&second.id], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
    assert_eq!(sources, vec![("peer-x".into(), 7), ("peer-y".into(), 3)]);
    let slice = b.activate_slice(SliceBudget::ACTIVATOR).unwrap();
    assert_eq!(slice.activated, vec![second.id.clone()]);
    assert!(!slice.more);
    let left: i64 =
        b.db.query_row("SELECT count(*) FROM incoming_sources", [], |r| r.get(0))
            .unwrap();
    assert_eq!(left, 0, "the cleanup trigger drops sources with their row");
    assert!(b.lookup("second.txt").is_ok());
}

/// A slow event yields a `SlowTicket` (its row stays ready) unless the slice
/// may commit it inline; `commit_prepared` commits it under the caller's
/// lock; `requeue` and `Stale` behave as the activator expects.
#[test]
fn slow_events_yield_tickets_and_commit_prepared_commits_them() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = store(&dir.path().join("a"), &repo);
    let mut b = store(&dir.path().join("b"), &repo);
    let allowed = devices([&a, &b]);
    a.create("shared.txt", "file").unwrap();
    b.receive(missing(&a, &b), &allowed).unwrap();
    write(&mut a, "shared.txt", b"from a");
    write(&mut b, "shared.txt", b"from b");
    let concurrent = missing(&a, &b);
    let event = concurrent.events[0].clone();
    for (h, bytes) in &concurrent.objects {
        assert_eq!(b.put_object(&hex::decode(bytes).unwrap()).unwrap(), *h);
    }
    b.stage_incoming(
        vec![StagedEvent {
            event: event.clone(),
            source: None,
        }],
        &allowed,
    )
    .unwrap();
    let slice = b.activate_slice(SliceBudget::ACTIVATOR).unwrap();
    assert!(slice.activated.is_empty());
    assert!(slice.more);
    let ticket = slice
        .slow_ticket
        .expect("a two-way edit needs the slow path");
    assert_eq!(ticket.id, event.id);
    assert_eq!(ticket.branch, b.active);
    assert!(ticket.base_seq > 0);
    assert_eq!(state_of(&b, &event.id).0.as_deref(), Some("ready"));
    let read = Store::open_secondary(&b.root, &b.repo, &b.device).unwrap();
    let prepared = Store::prepare_slow(&read, &ticket).unwrap();
    // WP2.4 (CCR-WP2.4-1): the slow path is computed off-lock; this pinned
    // the Wave 1 stub's `Prepared::Inline`.
    assert!(matches!(&prepared, Prepared::Computed(p) if p.is_computed()));
    assert_eq!(
        b.commit_prepared(prepared).unwrap(),
        CommitOutcome::Committed {
            id: event.id.clone(),
            slow: true
        }
    );
    assert!(
        b.conflicts()
            .unwrap()
            .iter()
            .any(|c| c.resolved_by.is_none())
    );
    // The same ticket again: the event is already committed.
    assert_eq!(
        b.commit_prepared(Prepared::Inline(ticket.clone())).unwrap(),
        CommitOutcome::Committed {
            id: event.id.clone(),
            slow: false
        }
    );
    // `requeue` defers a ready row; it is not due, so a slice skips it.
    write(&mut a, "shared.txt", b"from a again");
    let next = missing(&a, &b).events[0].clone();
    b.stage_incoming(
        vec![StagedEvent {
            event: next.clone(),
            source: None,
        }],
        &allowed,
    )
    .unwrap();
    b.requeue(&next.id, 60_000).unwrap();
    assert_eq!(b.inbox_counts().unwrap().retry, 1);
    let slice = b.activate_slice(SliceBudget::UNBOUNDED).unwrap();
    assert!(slice.activated.is_empty() && !slice.more);
    // A ticket whose row now waits for a parent is stale.
    let mut orphan = next.clone();
    orphan.id = id();
    orphan.dependencies = vec!["missing-parent".into()];
    b.stage_incoming(
        vec![StagedEvent {
            event: orphan.clone(),
            source: None,
        }],
        &allowed,
    )
    .unwrap();
    let stale = Prepared::Inline(tkfs::core::inbox::SlowTicket {
        id: orphan.id.clone(),
        branch: b.active.clone(),
        base_seq: 0,
    });
    assert_eq!(b.commit_prepared(stale).unwrap(), CommitOutcome::Stale);
    // `revalidate` makes the retry row due now; it activates inline.
    assert_eq!(
        b.revalidate(Some(std::slice::from_ref(&next.id))).unwrap(),
        1
    );
    let slice = b.activate_slice(SliceBudget::UNBOUNDED).unwrap();
    assert_eq!(slice.activated, vec![next.id.clone()]);
    assert_eq!(slice.slow, 1);
    assert_oracle_empty(&mut b, "after slow commits");
}

/// Causal cycles are quarantined (`CAUSAL_CYCLE`), never pending forever.
#[test]
fn causal_cycles_are_quarantined_not_pending() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let a = store(&dir.path().join("a"), &repo);
    let mut b = store(&dir.path().join("b"), &repo);
    let allowed = devices([&a, &b]);
    let branch = a.branch(&a.active).unwrap();
    let create = |name: &str| {
        let entity = id();
        vec![
            Change {
                entity: entity.clone(),
                field: "kind".into(),
                parents: vec![],
                value: json!("directory"),
            },
            Change {
                entity: entity.clone(),
                field: "location".into(),
                parents: vec![],
                value: json!({"parent": ROOT, "name": name}),
            },
            Change {
                entity,
                field: "alive".into(),
                parents: vec![],
                value: json!(true),
            },
        ]
    };
    let mut x = a.event(&branch, create("x"));
    let mut y = a.event(&branch, create("y"));
    x.dependencies = vec![y.id.clone()];
    y.dependencies = vec![x.id.clone()];
    let mut z = a.event(&branch, create("z"));
    z.dependencies = vec![z.id.clone()];
    b.receive(page(&b, vec![x.clone()], &BTreeMap::new()), &allowed)
        .unwrap();
    assert_eq!(state_of(&b, &x.id).0.as_deref(), Some("waiting"));
    b.receive(
        page(&b, vec![y.clone(), z.clone()], &BTreeMap::new()),
        &allowed,
    )
    .unwrap();
    let (state, error, _) = state_of(&b, &y.id);
    assert_eq!(state.as_deref(), Some("quarantined"));
    assert!(error.unwrap().starts_with("CAUSAL_CYCLE"));
    assert_eq!(state_of(&b, &x.id).0.as_deref(), Some("blocked"));
    assert_eq!(state_of(&b, &z.id).0.as_deref(), Some("quarantined"));
    let counts = b.inbox_counts().unwrap();
    assert_eq!(
        (counts.quarantined, counts.blocked, counts.pending_active()),
        (2, 1, 0)
    );
    assert!(b.events().unwrap().is_empty());
}

/// Rows written by an older binary (`state` NULL) are classified on open and
/// drained; a legacy quarantine of a transient failure is retried.
#[test]
fn legacy_rows_are_classified_and_drained() {
    let fx = copy_legacy_worker();
    let m = fx.manifest.clone();
    let s = Store::open_existing(&fx.state).unwrap();
    let counts = s.inbox_counts().unwrap();
    assert_eq!(counts.unclassified, 0);
    assert_eq!(counts.total(), m["incoming"]["total"].as_u64().unwrap());
    assert_eq!(counts.waiting, m["incoming"]["waiting"].as_u64().unwrap());
    assert_eq!(
        counts.quarantined + counts.blocked,
        m["incoming"]["quarantined"].as_u64().unwrap()
    );
    assert_eq!(counts.pending_active(), counts.waiting);
    assert_eq!(
        s.status().unwrap()["incoming_quarantined"],
        m["incoming"]["quarantined"]
    );
    assert!(!s.missing_parents(10).unwrap().is_empty());
    assert!(!s.quarantined(10).unwrap().is_empty());
    drop(s);
    // NULL rows inserted while the store is open drain on the next slice.
    let dir = tempfile::tempdir().unwrap();
    let mut s = Store::open(dir.path(), Some(&id())).unwrap();
    s.create("seed.txt", "file").unwrap();
    let ids = staged_inbox(&mut s, 20).unwrap();
    assert_eq!(s.inbox_counts().unwrap().unclassified, 20);
    let mut activated = vec![];
    loop {
        let slice = s.activate_slice(SliceBudget::UNBOUNDED).unwrap();
        activated.extend(slice.activated);
        if !slice.more {
            break;
        }
    }
    assert_eq!(activated, ids);
    assert_eq!(s.inbox_counts().unwrap().total(), 0);
    // A legacy row quarantined for a transient failure is retried on open.
    let author = tempfile::tempdir().unwrap();
    let mut a = Store::open(author.path(), Some(&s.repo)).unwrap();
    let mut allowed: BTreeSet<String> = s.events().unwrap().into_iter().map(|e| e.device).collect();
    allowed.insert(a.device.clone());
    support::sync_into(&s, &mut a, &allowed).unwrap();
    a.create("transient.txt", "file").unwrap();
    let event = missing(&a, &s).events.remove(0);
    s.db.execute(
        "INSERT INTO incoming_events(id,payload,error) VALUES(?,?,?)",
        rusqlite::params![
            event.id,
            serde_json::to_string(&event).unwrap(),
            "database is locked"
        ],
    )
    .unwrap();
    let root = s.root.clone();
    drop(s);
    let mut s = Store::open_existing(&root).unwrap();
    assert!(s.lookup("transient.txt").is_ok());
    assert_eq!(s.inbox_counts().unwrap().total(), 0);
    assert_oracle_empty(&mut s, "after legacy drain");
}

/// A receiver `b` and the causally sorted remote creates of the nested
/// directories `d`, `d/x`, `d/x/y`, … (`depth` of them), each depending on
/// the previous one.
fn nested_chain(depth: usize) -> (tempfile::TempDir, Store, Vec<Event>) {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = store(&dir.path().join("a"), &repo);
    let b = store(&dir.path().join("b"), &repo);
    let mut path = String::from("d");
    for _ in 0..depth {
        a.create(&path, "directory").unwrap();
        path.push_str("/x");
    }
    let bundle = missing(&a, &b);
    assert_eq!(bundle.events.len(), depth);
    for pair in bundle.events.windows(2) {
        assert!(pair[1].dependencies.contains(&pair[0].id));
    }
    (dir, b, bundle.events)
}

/// Insert an inbox row as an older binary or build left it.
fn insert_row(
    s: &Store,
    event: &Event,
    state: Option<&str>,
    error: Option<&str>,
    blocked_on: Option<&str>,
) {
    s.db.execute(
        "INSERT INTO incoming_events(id,payload,error,state,blocked_on) VALUES(?,?,?,?,?)",
        rusqlite::params![
            event.id,
            serde_json::to_string(event).unwrap(),
            error,
            state,
            blocked_on
        ],
    )
    .unwrap();
}

/// Activate until no row is ready.
fn drain(s: &mut Store) {
    while s.activate_slice(SliceBudget::UNBOUNDED).unwrap().more {}
}

fn ancestor(id: &str) -> String {
    format!("QUARANTINED_CAUSAL_ANCESTOR: {id}")
}

/// Fix round 1 (blocker): a legacy quarantine of a transient failure (RC11)
/// is retried on open, and its descendants follow it in every rowid order,
/// whether the old binary marked them `QUARANTINED_CAUSAL_ANCESTOR: <root>`
/// or never tried them (`error` NULL).
#[test]
fn legacy_transient_roots_release_their_descendants_in_any_order() {
    let blocked = |root: &Event| Some(ancestor(&root.id));
    for (case, child_first, child_error) in [
        ("root first, blocked child", false, true),
        ("child first, blocked child", true, true),
        ("root first, untried child", false, false),
        ("child first, untried child", true, false),
    ] {
        let (_dir, b, events) = nested_chain(2);
        let (root, child) = (&events[0], &events[1]);
        let child_error = if child_error { blocked(root) } else { None };
        let insert_root = || insert_row(&b, root, None, Some("database is locked"), None);
        let insert_child = || insert_row(&b, child, None, child_error.as_deref(), None);
        if child_first {
            insert_child();
            insert_root();
        } else {
            insert_root();
            insert_child();
        }
        let path = b.root.clone();
        drop(b);
        let mut b = Store::open_existing(&path).unwrap();
        assert!(b.lookup("d").is_ok(), "{case}: root");
        assert!(
            b.lookup("d/x").is_ok(),
            "{case}: child stays {:?}",
            state_of(&b, &child.id)
        );
        assert_eq!(b.inbox_counts().unwrap().total(), 0, "{case}");
        assert_oracle_empty(&mut b, case);
    }
}

/// Fix round 1 (blocker): an older binary named the *direct* parent in
/// `QUARANTINED_CAUSAL_ANCESTOR`. Classification resolves every row to the
/// root whatever the rowid order, so `revalidate(root)` re-queues the whole
/// chain and it activates once the root commits.
#[test]
fn legacy_ancestor_chains_resolve_to_the_root() {
    let (_dir, b, events) = nested_chain(3);
    let (r, c, g) = (&events[0], &events[1], &events[2]);
    insert_row(&b, g, None, Some(&ancestor(&c.id)), None);
    insert_row(&b, c, None, Some(&ancestor(&r.id)), None);
    insert_row(&b, r, None, Some("INVALID_KIND: legacy verdict"), None);
    let path = b.root.clone();
    drop(b);
    let mut b = Store::open_existing(&path).unwrap();
    assert_eq!(state_of(&b, &r.id).0.as_deref(), Some("quarantined"));
    for row in [c, g] {
        let (state, error, blocked_on) = state_of(&b, &row.id);
        assert_eq!(state.as_deref(), Some("blocked"));
        assert!(error.unwrap().starts_with("QUARANTINED_CAUSAL_ANCESTOR"));
        assert_eq!(blocked_on.as_deref(), Some(r.id.as_str()), "the root");
    }
    let counts = b.inbox_counts().unwrap();
    assert_eq!(
        (counts.quarantined, counts.blocked, counts.pending_active()),
        (1, 2, 0)
    );
    assert_eq!(b.quarantined(10).unwrap().len(), 1);
    assert_eq!(b.revalidate(Some(std::slice::from_ref(&r.id))).unwrap(), 3);
    drain(&mut b);
    for path in ["d", "d/x", "d/x/x"] {
        assert!(b.lookup(path).is_ok(), "{path}");
    }
    assert_eq!(b.inbox_counts().unwrap().total(), 0);
    assert_oracle_empty(&mut b, "after the legacy chain");
}

/// Fix round 1 (blocker): rows that an older build left blocked on a row that
/// can commit, or on an intermediate blocked row, are not stuck. At runtime
/// the commit of the row they name releases them; `revalidate(root)` follows
/// `blocked_on` transitively; on open, a blocked row whose `blocked_on` is no
/// longer a quarantined root is re-classified.
#[test]
fn rows_blocked_on_a_row_that_can_commit_are_released() {
    // Runtime: the root is `ready`, its descendants are blocked on it.
    let (_dir, mut b, events) = nested_chain(3);
    let (r, c, g) = (&events[0], &events[1], &events[2]);
    insert_row(&b, g, Some("blocked"), Some(&ancestor(&r.id)), Some(&r.id));
    insert_row(&b, c, Some("blocked"), Some(&ancestor(&r.id)), Some(&r.id));
    insert_row(&b, r, Some("ready"), None, None);
    let slice = b.activate_slice(SliceBudget::UNBOUNDED).unwrap();
    assert!(slice.activated.contains(&r.id));
    drain(&mut b);
    assert!(b.lookup("d/x/x").is_ok(), "{:?}", state_of(&b, &g.id));
    assert_eq!(b.inbox_counts().unwrap().total(), 0);
    assert_oracle_empty(&mut b, "released at runtime");

    // `revalidate` follows `blocked_on` through an intermediate blocked row.
    let (_dir, mut b, events) = nested_chain(3);
    let (r, c, g) = (&events[0], &events[1], &events[2]);
    insert_row(&b, g, Some("blocked"), Some(&ancestor(&c.id)), Some(&c.id));
    insert_row(&b, c, Some("blocked"), Some(&ancestor(&r.id)), Some(&r.id));
    insert_row(&b, r, Some("quarantined"), Some("INVALID_KIND: x"), None);
    assert_eq!(b.revalidate(Some(std::slice::from_ref(&r.id))).unwrap(), 3);
    drain(&mut b);
    assert!(b.lookup("d/x/x").is_ok(), "{:?}", state_of(&b, &g.id));
    assert_oracle_empty(&mut b, "revalidated transitively");

    // On open: blocked on a committed event, and blocked on an intermediate
    // row whose root is quarantined (re-pointed to the root).
    let (_dir, mut b, events) = nested_chain(3);
    let (r, c, g) = (&events[0], &events[1], &events[2]);
    let allowed: BTreeSet<String> = events.iter().map(|e| e.device.clone()).collect();
    b.receive(page(&b, vec![r.clone()], &BTreeMap::new()), &allowed)
        .unwrap();
    insert_row(&b, c, Some("blocked"), Some(&ancestor(&r.id)), Some(&r.id));
    insert_row(&b, g, Some("blocked"), Some(&ancestor(&c.id)), Some(&c.id));
    let path = b.root.clone();
    drop(b);
    let mut b = Store::open_existing(&path).unwrap();
    assert!(b.lookup("d/x/x").is_ok(), "{:?}", state_of(&b, &g.id));
    assert_eq!(b.inbox_counts().unwrap().total(), 0);
    assert_oracle_empty(&mut b, "released on open");

    let (_dir, b, events) = nested_chain(3);
    let (r, c, g) = (&events[0], &events[1], &events[2]);
    insert_row(&b, r, Some("quarantined"), Some("INVALID_KIND: x"), None);
    insert_row(&b, c, Some("blocked"), Some(&ancestor(&r.id)), Some(&r.id));
    insert_row(&b, g, Some("blocked"), Some(&ancestor(&c.id)), Some(&c.id));
    let path = b.root.clone();
    drop(b);
    let b = Store::open_existing(&path).unwrap();
    assert_eq!(state_of(&b, &g.id).2.as_deref(), Some(r.id.as_str()));
    let counts = b.inbox_counts().unwrap();
    assert_eq!((counts.quarantined, counts.blocked), (1, 2));

    // A child staged under a stale blocked row is blocked with it (never
    // waiting on it): when the row they name is quarantined, nothing of the
    // chain is left pending.
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let a = store(&dir.path().join("a"), &repo);
    let mut b = store(&dir.path().join("b"), &repo);
    let branch = a.branch(&a.active).unwrap();
    let bad = a.event(&branch, invalid_kind_changes("bad.txt"));
    let mut child = a.event(&branch, invalid_kind_changes("child.txt"));
    child.dependencies = vec![bad.id.clone()];
    let mut grandchild = a.event(&branch, invalid_kind_changes("grandchild.txt"));
    grandchild.dependencies = vec![child.id.clone()];
    insert_row(&b, &bad, Some("ready"), None, None);
    insert_row(
        &b,
        &child,
        Some("blocked"),
        Some(&ancestor(&bad.id)),
        Some(&bad.id),
    );
    let staged = b
        .stage_incoming(
            vec![StagedEvent {
                event: grandchild.clone(),
                source: None,
            }],
            &devices([&a, &b]),
        )
        .unwrap();
    assert_eq!(staged.staged, vec![grandchild.id.clone()]);
    assert_eq!(
        state_of(&b, &grandchild.id).2.as_deref(),
        Some(bad.id.as_str())
    );
    drain(&mut b);
    let counts = b.inbox_counts().unwrap();
    assert_eq!(
        (counts.quarantined, counts.blocked, counts.pending_active()),
        (1, 2, 0)
    );
    for row in [&child, &grandchild] {
        assert_eq!(state_of(&b, &row.id).2.as_deref(), Some(bad.id.as_str()));
    }
}

/// Fix round 1: the commit path counters count durable commits only. A
/// commit inside a caller's transaction counts once that transaction
/// commits, and never if it rolls back.
#[test]
fn commit_path_counters_count_only_durable_commits() {
    let (_dir, mut b, events) = nested_chain(2);
    let allowed: BTreeSet<String> = events.iter().map(|e| e.device.clone()).collect();
    let total = |b: &Store| {
        let counters = b.commit_path_counters();
        counters.fast + counters.slow
    };
    let before = total(&b);
    let first = page(&b, vec![events[0].clone()], &BTreeMap::new());
    b.db.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert_eq!(b.receive(first.clone(), &allowed).unwrap().len(), 1);
    assert_eq!(total(&b), before, "not durable inside the transaction");
    b.db.execute_batch("ROLLBACK").unwrap();
    assert_eq!(total(&b), before, "rolled back: never counted");
    assert!(b.lookup("d").is_err());
    b.db.execute_batch("BEGIN IMMEDIATE").unwrap();
    b.receive(first, &allowed).unwrap();
    b.db.execute_batch("COMMIT").unwrap();
    assert_eq!(
        total(&b),
        before + 1,
        "counted once its transaction committed"
    );
    b.receive(
        page(&b, vec![events[1].clone()], &BTreeMap::new()),
        &allowed,
    )
    .unwrap();
    assert_eq!(total(&b), before + 2, "an outermost commit counts at once");
    assert!(b.lookup("d/x").is_ok());
}

/// CCR-WP0.INT-5 item 2: activation takes the writer slot before it reads
/// (`BEGIN IMMEDIATE`), so another writer makes it wait, not fail.
#[test]
fn activation_waits_for_another_writer_instead_of_failing() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = store(&dir.path().join("a"), &repo);
    let mut b = store(&dir.path().join("b"), &repo);
    let allowed = devices([&a, &b]);
    a.create("one.txt", "file").unwrap();
    let other = Connection::open(b.root.join("metadata.sqlite")).unwrap();
    other
        .execute_batch("CREATE TABLE wp1_6_probe(x INTEGER NOT NULL)")
        .unwrap();
    other
        .execute_batch("BEGIN IMMEDIATE; INSERT INTO wp1_6_probe VALUES(1)")
        .unwrap();
    let hold = Duration::from_millis(300);
    let writer = thread::spawn(move || {
        thread::sleep(hold);
        other.execute_batch("COMMIT").unwrap();
        other
    });
    let started = Instant::now();
    let result = b.receive(missing(&a, &b), &allowed);
    let waited = started.elapsed();
    writer.join().unwrap();
    assert_eq!(
        result.expect("activation waits for the writer slot").len(),
        1
    );
    assert!(
        waited >= hold / 2,
        "the writer slot was contended: {waited:?}"
    );
    assert!(b.db.is_autocommit());
    assert!(b.lookup("one.txt").is_ok());
}

/// CCR-WP0.INT-5 item 2: commits by another connection between staging,
/// reads and writes never fail an activation with `SQLITE_BUSY` (5) or
/// `SQLITE_BUSY_SNAPSHOT` (517).
#[test]
fn activation_interleaved_with_foreign_commits_never_fails_busy() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = store(&dir.path().join("a"), &repo);
    let mut b = store(&dir.path().join("b"), &repo);
    let allowed = devices([&a, &b]);
    for i in 0..60 {
        a.create(&format!("f-{i:02}.txt"), "file").unwrap();
    }
    let bundle = missing(&a, &b);
    let other = Connection::open(b.root.join("metadata.sqlite")).unwrap();
    other.busy_timeout(Duration::from_secs(5)).unwrap();
    other
        .execute_batch("CREATE TABLE wp1_6_probe(x INTEGER NOT NULL)")
        .unwrap();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = stop.clone();
    let writer = thread::spawn(move || {
        let mut commits = 0;
        while !flag.load(std::sync::atomic::Ordering::Relaxed) {
            other
                .execute("INSERT INTO wp1_6_probe VALUES(1)", [])
                .unwrap();
            commits += 1;
            thread::sleep(Duration::from_millis(2));
        }
        commits
    });
    let mut failures = vec![];
    for event in &bundle.events {
        if let Err(error) = b.receive(page(&b, vec![event.clone()], &bundle.objects), &allowed) {
            failures.push(format!("{error:#}"));
        }
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let commits = writer.join().unwrap();
    assert!(commits > 0);
    assert!(failures.is_empty(), "{failures:#?}");
    assert_eq!(b.shared_event_inventory().unwrap().len(), 60);
    assert_eq!(b.inbox_counts().unwrap().total(), 0);
}

/// Fault point `core.commit.busy` (`TKFS_FAULT_POINT`): an injected
/// `SQLITE_BUSY` at commit is retried (1 s backoff) and committed, never
/// quarantined. The child arms the point after authoring its fixture.
#[test]
fn injected_busy_is_retried_and_committed_never_quarantined() {
    let marker = tempfile::tempdir().unwrap();
    run_child(
        "busy_fault_child",
        &[(CHILD_BUSY, marker.path().display().to_string())],
    );
    assert!(
        marker.path().join("busy.done").is_file(),
        "the child ran the scenario"
    );
}

#[test]
fn busy_fault_child() {
    let Ok(marker) = std::env::var(CHILD_BUSY) else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = store(&dir.path().join("a"), &repo);
    let mut b = store(&dir.path().join("b"), &repo);
    let allowed = devices([&a, &b]);
    a.create("busy.txt", "file").unwrap();
    let bundle = missing(&a, &b);
    let event = bundle.events[0].id.clone();
    // Armed now (this child process only): the next commit hits SQLITE_BUSY once.
    let mut fault = FaultConfig::new("core.commit.busy", FaultAction::Fail);
    fault.count = Some(1);
    tkfs::util::fault::configure(Some(fault));
    let accepted = b.receive(bundle, &allowed).unwrap();
    assert!(accepted.is_empty(), "the busy commit was not activated");
    let (state, error, _) = state_of(&b, &event);
    assert_eq!(state.as_deref(), Some("retry"));
    assert_eq!(
        error, None,
        "a transient error never sets the quarantine column"
    );
    let (code, retry_at): (String, i64) =
        b.db.query_row(
            "SELECT code,retry_at_ms FROM incoming_events WHERE id=?",
            [&event],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(code, "SQLITE_BUSY");
    let delay = retry_at - tkfs::core::time() as i64;
    assert!((0..=1000).contains(&delay), "first backoff is 1 s: {delay}");
    let counts = b.inbox_counts().unwrap();
    assert_eq!(
        (counts.retry, counts.quarantined, counts.pending_active()),
        (1, 0, 1)
    );
    // Not due yet.
    assert!(
        b.activate_slice(SliceBudget::UNBOUNDED)
            .unwrap()
            .activated
            .is_empty()
    );
    thread::sleep(Duration::from_millis(1100));
    let slice = b.activate_slice(SliceBudget::UNBOUNDED).unwrap();
    assert_eq!(slice.activated, vec![event]);
    assert!(b.lookup("busy.txt").is_ok());
    assert_eq!(b.inbox_counts().unwrap().total(), 0);
    assert_oracle_empty(&mut b, "after the retry");
    std::fs::write(Path::new(&marker).join("busy.done"), b"ok").unwrap();
}

/// The slice budget starts after the window load: with a 1.5 s load and a
/// 1 s budget a slice of cheap events still activates `min(N, 32)`.
#[test]
fn slice_budget_excludes_the_window_load() {
    let marker = tempfile::tempdir().unwrap();
    run_child(
        "window_load_child",
        &[
            (CHILD_WINDOW, marker.path().display().to_string()),
            ("TKFS_FAULT_POINT", "core.inbox.window".into()),
            ("TKFS_FAULT_ACTION", "hang".into()),
            ("TKFS_FAULT_MS", "1500".into()),
        ],
    );
    assert!(
        marker.path().join("window.done").is_file(),
        "the child ran the scenario"
    );
}

#[test]
fn window_load_child() {
    let Ok(marker) = std::env::var(CHILD_WINDOW) else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = store(&dir.path().join("a"), &repo);
    let mut b = store(&dir.path().join("b"), &repo);
    let allowed = devices([&a, &b]);
    a.create("log.txt", "file").unwrap();
    b.receive(missing(&a, &b), &allowed).unwrap();
    for i in 0..40 {
        write(&mut a, "log.txt", format!("line {i}").as_bytes());
    }
    let bundle = missing(&a, &b);
    for (h, bytes) in &bundle.objects {
        b.put_object(&hex::decode(bytes).unwrap()).unwrap();
        assert!(b.object_available(h));
    }
    b.stage_incoming(
        bundle
            .events
            .iter()
            .map(|event| StagedEvent {
                event: event.clone(),
                source: None,
            })
            .collect(),
        &allowed,
    )
    .unwrap();
    let budget = SliceBudget {
        max_events: 32,
        max_ms: 1000,
        inline_slow: true,
    };
    let started = Instant::now();
    let first = b.activate_slice(budget).unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(1500),
        "the load hung"
    );
    assert_eq!(first.activated.len(), 32, "N=40: min(N, 32)");
    assert!(first.more);
    let second = b.activate_slice(budget).unwrap();
    assert_eq!(second.activated.len(), 8, "N=8: min(N, 32)");
    assert!(!second.more);
    assert_eq!(
        b.read_object(&b.lookup("log.txt").unwrap().content.unwrap())
            .unwrap(),
        b"line 39"
    );
    std::fs::write(Path::new(&marker).join("window.done"), b"ok").unwrap();
}

// ---------------------------------------------------------------------------
// Seeded two-replica scripts: the oracle after every activation, and
// `TKFS_REMOTE_FASTPATH=0` replaying the same events to identical state.

const SEEDS: u64 = 100;
const STEPS: usize = 28;
const NAMES: [&str; 5] = ["a", "b", "c", "a.txt", "dir"];

/// One replica's history: its local commits and its deliveries, in order.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum Step {
    Local {
        event: Event,
        objects: BTreeMap<String, String>,
    },
    Receive(Bundle),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Trace {
    seed: u64,
    repo: String,
    devices: [String; 2],
    steps: [Vec<Step>; 2],
    digest: [Value; 2],
    remote_fast: u64,
    remote_total: u64,
}

fn path_of(p: &Projection, entry: &Entry) -> String {
    let mut parts = vec![entry.name.clone()];
    let mut parent = entry.parent.clone();
    let mut guard = 0;
    while parent != ROOT && guard < 64 {
        let Some(e) = p.entries.get(&parent) else {
            break;
        };
        parts.push(e.name.clone());
        parent = e.parent.clone();
        guard += 1;
    }
    parts.reverse();
    parts.join("/")
}

/// Live entries by path (ties by id), so choices do not depend on UUIDs.
fn live(s: &Store) -> Vec<(String, Entry)> {
    let p = s.projection(&s.active).unwrap();
    let mut entries: Vec<_> = p
        .entries
        .values()
        .filter(|e| e.alive)
        .map(|e| (path_of(&p, e), e.clone()))
        .collect();
    entries.sort_by(|a, b| (&a.0, &a.1.id).cmp(&(&b.0, &b.1.id)));
    entries
}

fn pick_dir(s: &Store, rng: &mut StdRng) -> String {
    let dirs: Vec<String> = live(s)
        .into_iter()
        .filter(|(_, e)| e.kind == "directory")
        .map(|(path, _)| path)
        .collect();
    if dirs.is_empty() || rng.gen_bool(0.4) {
        String::new()
    } else {
        dirs[rng.gen_range(0..dirs.len())].clone()
    }
}

fn join(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_owned()
    } else {
        format!("{dir}/{name}")
    }
}

/// One random local operation; failures (collisions, cycles, non-empty
/// directories, stale reviews) are part of the script and simply skipped.
fn local_op(s: &mut Store, rng: &mut StdRng, step: usize) {
    let entries = live(s);
    let any = |rng: &mut StdRng| -> Option<Entry> {
        (!entries.is_empty()).then(|| entries[rng.gen_range(0..entries.len())].1.clone())
    };
    let name = NAMES[rng.gen_range(0..NAMES.len())];
    match rng.gen_range(0..100) {
        0..=29 => {
            let kind = if rng.gen_bool(0.35) {
                "directory"
            } else {
                "file"
            };
            let _ = s.create(&join(&pick_dir(s, rng), name), kind);
        }
        30..=49 => {
            if let Some(e) = any(rng) {
                let target = join(&pick_dir(s, rng), name);
                let _ = s.rename(&e.id, &target, rng.gen_bool(0.3));
            }
        }
        50..=59 => {
            if let Some(e) = any(rng) {
                let _ = s.delete(&e.id);
            }
        }
        60..=84 => {
            let files: Vec<Entry> = entries
                .iter()
                .filter(|(_, e)| e.kind == "file")
                .map(|(_, e)| e.clone())
                .collect();
            if !files.is_empty() {
                let e = &files[rng.gen_range(0..files.len())];
                let _ = s.write_revision(
                    &e.id,
                    e.revisions.get("content").cloned(),
                    format!("step {step} {}", rng.r#gen::<u32>()).as_bytes(),
                );
            }
        }
        _ => {
            let open: Vec<_> = s
                .conflicts()
                .unwrap()
                .into_iter()
                .filter(|c| c.resolved_by.is_none())
                .collect();
            if open.is_empty() {
                return;
            }
            let c = &open[rng.gen_range(0..open.len())];
            if c.field == "namespace" {
                let entity = c.entity.split(',').next().unwrap().to_owned();
                let target = format!("recovered-{step}");
                let _ = s.recover(&entity, &target, std::slice::from_ref(&c.id));
            } else {
                let choice = &c.alternatives[rng.gen_range(0..c.alternatives.len())];
                let _ = s.resolve(std::slice::from_ref(&c.id), &choice.revision.clone());
            }
        }
    }
}

fn event_ids(s: &Store) -> BTreeSet<String> {
    s.events().unwrap().into_iter().map(|e| e.id).collect()
}

/// Record the local events `op` committed on replica `r`.
fn record_local(s: &mut Store, steps: &mut Vec<Step>, op: impl FnOnce(&mut Store)) {
    let before = event_ids(s);
    op(s);
    for event in s.events().unwrap() {
        if before.contains(&event.id) {
            continue;
        }
        let objects = event
            .objects()
            .into_iter()
            .filter_map(|h| s.read_object(&h).ok().map(|bytes| (h, hex::encode(bytes))))
            .collect();
        steps.push(Step::Local { event, objects });
    }
}

/// Deliver what `from` has and `to` lacks in pages of 1–4 events, in a
/// random order mode, each page either through `receive` (oracle after the
/// page) or staged and activated one event per slice (oracle after every
/// activation). Returns (fast, total) remote commits.
fn deliver(
    from: &Store,
    to: &mut Store,
    steps: &mut Vec<Step>,
    rng: &mut StdRng,
    allowed: &BTreeSet<String>,
    context: &str,
) -> (u64, u64) {
    let bundle = missing(from, to);
    if bundle.events.is_empty() {
        return (0, 0);
    }
    let mut events = bundle.events.clone();
    match rng.gen_range(0..4) {
        0 => {}
        1 => events.reverse(),
        2 => events.shuffle(rng),
        _ => {
            // Causal order with duplicates of earlier events.
            for _ in 0..2 {
                let i = rng.gen_range(0..events.len());
                events.push(events[i].clone());
            }
        }
    }
    let before = to.commit_path_counters();
    let mut index = 0;
    while index < events.len() {
        let size = rng.gen_range(1..=4).min(events.len() - index);
        let mut chunk = page(to, events[index..index + size].to_vec(), &bundle.objects);
        chunk.branches = bundle.branches.clone();
        steps.push(Step::Receive(chunk.clone()));
        if rng.gen_bool(0.5) {
            // The legacy path: one `receive` (stage, then activate everything).
            to.receive(chunk, allowed)
                .unwrap_or_else(|e| panic!("{context}: receive failed: {e:#}"));
            assert_oracle_empty(to, context);
        } else {
            // The staged path, one activation per slice, the oracle after each.
            for bytes in chunk.objects.values() {
                to.put_object(&hex::decode(bytes).unwrap()).unwrap();
            }
            let staged = to
                .stage_incoming(
                    chunk
                        .events
                        .into_iter()
                        .map(|event| StagedEvent {
                            event,
                            source: Some(("fuzz".into(), index as i64)),
                        })
                        .collect(),
                    allowed,
                )
                .unwrap_or_else(|e| panic!("{context}: stage failed: {e:#}"));
            assert!(staged.rejected.is_empty(), "{context}: {staged:?}");
            loop {
                let slice = to
                    .activate_slice(SliceBudget {
                        max_events: 1,
                        max_ms: u64::MAX,
                        inline_slow: true,
                    })
                    .unwrap_or_else(|e| panic!("{context}: slice failed: {e:#}"));
                assert!(slice.activated.len() <= 1);
                assert_oracle_empty(to, context);
                if !slice.more {
                    break;
                }
            }
        }
        index += size;
    }
    let after = to.commit_path_counters();
    (
        after.fast - before.fast,
        after.fast + after.slow - before.fast - before.slow,
    )
}

/// The derived state two runs must agree on.
fn digest(s: &Store) -> Value {
    let rows = |sql: &str| -> Vec<Vec<Value>> {
        let mut statement = s.db.prepare(sql).unwrap();
        let columns = statement.column_count();
        statement
            .query_map([], |r| {
                (0..columns)
                    .map(|i| {
                        Ok(match r.get::<_, Option<String>>(i)? {
                            Some(text) => {
                                serde_json::from_str(&text).unwrap_or(Value::String(text))
                            }
                            None => Value::Null,
                        })
                    })
                    .collect()
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    };
    let projections: BTreeMap<String, Value> = s
        .branches()
        .unwrap()
        .into_iter()
        .map(|b| (b.id.clone(), json!(s.projection(&b.id).unwrap())))
        .collect();
    // A snapshot resume stores each conflict payload with its `resolved_by`
    // (the column is the record); compare payloads without it.
    let mut conflicts =
        rows("SELECT branch,id,payload,resolved_by FROM conflicts ORDER BY branch,id");
    for row in &mut conflicts {
        if let Some(payload) = row[2].as_object_mut() {
            payload.remove("resolved_by");
        }
    }
    json!({
        "events": event_ids(s),
        "projections": projections,
        "conflicts": conflicts,
        "frontier": rows("SELECT branch,event FROM event_frontier ORDER BY branch,event"),
        "inbox": rows("SELECT id,state,error,blocked_on FROM incoming_events ORDER BY id"),
    })
}

fn run_script(seed: u64) -> Trace {
    let dir = tempfile::tempdir().unwrap();
    let repo = format!("wp1-6-fuzz-{seed}");
    let names = [format!("replica-a-{seed}"), format!("replica-b-{seed}")];
    let mut stores = [
        Store::initialize(&dir.path().join("a"), &repo, &names[0]).unwrap(),
        Store::initialize(&dir.path().join("b"), &repo, &names[1]).unwrap(),
    ];
    let allowed: BTreeSet<String> = names.iter().cloned().collect();
    let mut steps: [Vec<Step>; 2] = [vec![], vec![]];
    let mut rng = StdRng::seed_from_u64(seed);
    let (mut remote_fast, mut remote_total) = (0, 0);
    for step in 0..STEPS {
        let r = rng.gen_range(0..2);
        let context = format!("seed {seed} step {step} replica {r}");
        if rng.gen_range(0..100) < 30 {
            let [a, b] = &mut stores;
            let (from, to) = if r == 0 { (&*b, a) } else { (&*a, b) };
            let (fast, total) = deliver(from, to, &mut steps[r], &mut rng, &allowed, &context);
            remote_fast += fast;
            remote_total += total;
        } else {
            record_local(&mut stores[r], &mut steps[r], |s| {
                local_op(s, &mut rng, step)
            });
        }
    }
    // Converge both ways and check both replicas agree.
    for round in 0..3 {
        for r in [0, 1] {
            let context = format!("seed {seed} converge {round} replica {r}");
            let [a, b] = &mut stores;
            let (from, to) = if r == 0 { (&*b, a) } else { (&*a, b) };
            let (fast, total) = deliver(from, to, &mut steps[r], &mut rng, &allowed, &context);
            remote_fast += fast;
            remote_total += total;
        }
    }
    let [a, b] = &stores;
    assert_eq!(
        event_ids(a),
        event_ids(b),
        "seed {seed}: converged journals"
    );
    assert_eq!(
        a.projection(&a.active).unwrap(),
        b.projection(&b.active).unwrap(),
        "seed {seed}: converged projections"
    );
    let open = |s: &Store| -> Vec<_> {
        s.conflicts()
            .unwrap()
            .into_iter()
            .map(|c| (c.id, c.resolved_by))
            .collect()
    };
    assert_eq!(open(a), open(b), "seed {seed}: converged conflicts");
    for s in &stores {
        let counts = s.inbox_counts().unwrap();
        assert_eq!(
            counts.total(),
            0,
            "seed {seed}: nothing left staged: {counts:?}"
        );
    }
    let digests = [digest(&stores[0]), digest(&stores[1])];
    // A restart resumes from the startup snapshot and replays its suffix
    // (remote events now through the successors too): same derived state.
    let roots = [stores[0].root.clone(), stores[1].root.clone()];
    drop(stores);
    for (r, root) in roots.iter().enumerate() {
        let mut s = Store::open_existing(root).unwrap();
        assert_eq!(digest(&s), digests[r], "seed {seed} replica {r}: restart");
        assert_oracle_empty(&mut s, &format!("seed {seed} replica {r}: restart"));
    }
    Trace {
        seed,
        repo,
        devices: names,
        digest: digests,
        steps,
        remote_fast,
        remote_total,
    }
}

/// The oracle is empty after every activation across 100 seeded two-replica
/// scripts (create, mkdir, rename, move, delete, edit, resolve, recover;
/// in-order, reversed, shuffled and duplicated delivery), with remote fast
/// paths on; a child process with `TKFS_REMOTE_FASTPATH=0` replays the same
/// events and reaches byte-identical derived state.
#[test]
fn seeded_scripts_keep_the_oracle_empty_and_fastpath_off_is_identical() {
    let started = Instant::now();
    let workers = thread::available_parallelism().map_or(2, |n| n.get().clamp(2, 6)) as u64;
    let traces: Vec<Trace> = thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|w| {
                scope.spawn(move || {
                    (0..SEEDS)
                        .filter(|seed| seed % workers == w)
                        .map(run_script)
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut traces: Vec<Trace> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        traces.sort_by_key(|t| t.seed);
        traces
    });
    let fast: u64 = traces.iter().map(|t| t.remote_fast).sum();
    let total: u64 = traces.iter().map(|t| t.remote_total).sum();
    println!(
        "remote activations: {total}, through a successor: {fast}; scripts in {:?}",
        started.elapsed()
    );
    assert!(total > 500, "the scripts deliver remote events: {total}");
    if remote_fast_paths_enabled() {
        assert!(
            fast * 4 > total,
            "the remote fast paths are exercised: {fast} of {total}"
        );
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("traces.json");
    std::fs::write(&path, serde_json::to_vec(&traces).unwrap()).unwrap();
    run_child(
        "fastpath_off_replay_child",
        &[
            (CHILD_REPLAY, path.display().to_string()),
            ("TKFS_REMOTE_FASTPATH", "0".into()),
        ],
    );
    println!(
        "replay with TKFS_REMOTE_FASTPATH=0 done at {:?}",
        started.elapsed()
    );
    let replayed: Vec<[Value; 2]> =
        serde_json::from_slice(&std::fs::read(path.with_extension("digest.json")).unwrap())
            .unwrap();
    assert_eq!(replayed.len(), traces.len());
    for (trace, digest) in traces.iter().zip(&replayed) {
        for (r, (fast, off)) in trace.digest.iter().zip(digest).enumerate() {
            assert_eq!(
                fast, off,
                "seed {} replica {r}: TKFS_REMOTE_FASTPATH=0 diverged",
                trace.seed
            );
        }
    }
}

#[test]
fn fastpath_off_replay_child() {
    let Ok(path) = std::env::var(CHILD_REPLAY) else {
        return;
    };
    assert!(
        !remote_fast_paths_enabled(),
        "the child runs with the kill switch"
    );
    let traces: Vec<Trace> = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let replay = |trace: &Trace, r: usize| -> Value {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Store::initialize(dir.path(), &trace.repo, &trace.devices[r]).unwrap();
        let allowed: BTreeSet<String> = trace.devices.iter().cloned().collect();
        for step in &trace.steps[r] {
            match step {
                Step::Local { event, objects } => {
                    for bytes in objects.values() {
                        s.put_object(&hex::decode(bytes).unwrap()).unwrap();
                    }
                    s.commit(event.clone(), true).unwrap();
                }
                Step::Receive(bundle) => {
                    s.receive(bundle.clone(), &allowed).unwrap();
                }
            }
        }
        assert_oracle_empty(&mut s, &format!("replay seed {} replica {r}", trace.seed));
        digest(&s)
    };
    let workers = thread::available_parallelism().map_or(2, |n| n.get().clamp(2, 6));
    let digests: Vec<[Value; 2]> = thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|w| {
                let traces = &traces;
                scope.spawn(move || {
                    traces
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| i % workers == w)
                        .map(|(i, trace)| (i, [replay(trace, 0), replay(trace, 1)]))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut digests: Vec<_> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        digests.sort_by_key(|(i, _)| *i);
        digests.into_iter().map(|(_, d)| d).collect()
    });
    std::fs::write(
        Path::new(&path).with_extension("digest.json"),
        serde_json::to_vec(&digests).unwrap(),
    )
    .unwrap();
}
