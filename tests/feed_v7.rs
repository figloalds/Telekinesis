//! Metadata feed, format 7 (REVAMP_PLAN §6.3, WP2.5), worker side.
//!
//! In-process stores exchange through the three feed functions, exactly as
//! two workers do through `feed-offer` → `feed-exchange` → `feed-apply`
//! (§6.3.5): the dialer `build_exchange_request`s, the acceptor
//! `handle_exchange_request`s, and the dialer `apply_exchange_reply`s.
//! Activation runs as `activate_slice` loops (the background activator is
//! WP2.3's). The last tests drive `runtime::sync_rpc::dispatch` on worker
//! contexts.
mod support;

use rusqlite::params;
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use support::*;
use tkfs::{
    core::{Branch, Change, Event, Store, id, inbox::SliceBudget},
    model::{
        errors::{ErrorClass, code_of},
        ipc::WorkerHeartbeat,
        protocol::feed::{
            self, Cursor, FeedApplyResult, LARGE_EVENT_BYTES, MAX_NEED, MAX_NEED_BYTES,
            MAX_PAGE_BYTES, MAX_PAGE_ENTRIES, MetaExchangeReply, MetaExchangeRequest, PageStatus,
        },
    },
    paired_sync::SyncRoute,
    runtime::{
        Engine, Shared,
        ctx::{EngineGaugeCell, SyncStorePool, WorkerCtx},
        sync_rpc,
    },
    util::{notify::Notify, snapshot::Snapshot},
    worker::mount_ctl::MountController,
};

// ---------------------------------------------------------------------------
// Harness

fn create(root: &Path, repo: &str) -> Store {
    Store::initialize(root, repo, &id()).unwrap()
}

fn reopen(root: &Path, repo: &str) -> Store {
    Store::open(root, Some(repo)).unwrap()
}

/// `from`'s route to `to` (keyed by `to`'s replica, as orchestrators bind it).
fn route(from: &Store, to: &Store) -> SyncRoute {
    SyncRoute {
        peer: format!("installation-of-{}", to.device),
        repo: from.repo.clone(),
        local_replica: from.device.clone(),
        remote_replica: to.device.clone(),
    }
}

struct Round {
    request: MetaExchangeRequest,
    reply: MetaExchangeReply,
    /// The acceptor's own view (`feed-exchange`'s `local`).
    local: FeedApplyResult,
    /// The dialer's result (`feed-apply`).
    applied: FeedApplyResult,
}

impl Round {
    /// Payload-bearing entries of the acceptor's reply page.
    fn reply_payloads(&self) -> usize {
        self.reply
            .page
            .entries
            .iter()
            .filter(|e| e.event.is_some())
            .count()
    }
    fn request_payloads(&self) -> usize {
        self.request
            .page
            .entries
            .iter()
            .filter(|e| e.event.is_some())
            .count()
    }
}

/// One §6.3.5 exchange: `d` dials `a`.
fn exchange(d: &Store, a: &Store) -> Round {
    exchange_with(d, a, |_| {})
}

/// `exchange`, with `tamper` editing the reply before the dialer applies it.
fn exchange_with(d: &Store, a: &Store, tamper: impl FnOnce(&mut MetaExchangeReply)) -> Round {
    let request = d.build_exchange_request(&route(d, a)).unwrap();
    let (mut reply, local) = a.handle_exchange_request(&route(a, d), &request).unwrap();
    tamper(&mut reply);
    let applied = d
        .apply_exchange_reply(&route(d, a), &request.request, &reply)
        .unwrap();
    Round {
        request,
        reply,
        local,
        applied,
    }
}

/// Activate every ready row; returns how many committed.
fn activate(s: &mut Store) -> usize {
    let mut n = 0;
    loop {
        let slice = s.activate_slice(SliceBudget::UNBOUNDED).unwrap();
        n += slice.activated.len();
        if !slice.more {
            return n;
        }
    }
}

/// Exchange (`d` dials `a`) and activate both until a round moves nothing
/// and leaves nothing to send or receive. Returns the rounds it took.
fn settle(d: &mut Store, a: &mut Store) -> usize {
    for round in 1..=400 {
        let r = exchange(d, a);
        let activated = activate(d) + activate(a);
        let idle = |s: &tkfs::model::protocol::feed::ExchangeSummary| {
            !s.metadata_advanced && s.to_send == 0 && s.to_receive == 0
        };
        if idle(&r.applied.summary) && idle(&r.local.summary) && activated == 0 {
            return round;
        }
    }
    panic!("the feed did not settle in 400 rounds");
}

fn outgoing(s: &Store) -> i64 {
    s.outgoing_unacknowledged().unwrap()
}

fn shared(s: &Store) -> BTreeSet<String> {
    s.shared_event_inventory().unwrap()
}

fn peer_known(s: &Store, peer: &Store) -> BTreeSet<String> {
    s.known_by_peer(&peer.device).unwrap()
}

/// Same shared journal and projection, nothing staged, every outbox row acked.
fn assert_converged(stores: &[&Store]) {
    let first = stores[0];
    for s in stores {
        assert_eq!(shared(s), shared(first), "shared journals differ");
        assert_eq!(
            s.projection(&s.active).unwrap().entries,
            first.projection(&first.active).unwrap().entries,
            "projections differ"
        );
        assert_eq!(s.inbox_counts().unwrap().total(), 0, "rows left staged");
        assert_eq!(outgoing(s), 0, "unacknowledged outbox rows");
    }
}

fn main_branch(s: &Store) -> Branch {
    s.branch(&s.active.clone()).unwrap()
}

/// A new directory at the root, as its own (uncommitted) event of `s`.
fn mkdir_event(s: &Store, name: &str) -> Event {
    let entity = id();
    let change = |field: &str, value: Value| Change {
        entity: entity.clone(),
        field: field.into(),
        parents: vec![],
        value,
    };
    s.event(
        &main_branch(s),
        vec![
            change("kind", json!("directory")),
            change("location", json!({"parent": "root", "name": name})),
            change("alive", json!(true)),
        ],
    )
}

/// An event every activation quarantines (an unknown entry kind).
fn invalid_event(s: &Store) -> Event {
    s.event(
        &main_branch(s),
        vec![Change {
            entity: id(),
            field: "kind".into(),
            parents: vec![],
            value: json!("bad-kind"),
        }],
    )
}

/// Append `event` to `s`'s journal as committed, without validation (a
/// misbehaving peer's journal), with its outbox row.
fn inject(s: &Store, event: &Event) {
    s.db.execute(
        "INSERT OR IGNORE INTO branches VALUES(?,?,?)",
        params![event.branch.id, event.branch.name, event.branch.shared],
    )
    .unwrap();
    s.db.execute(
        "INSERT INTO events VALUES(?,?,?)",
        params![
            event.id,
            event.branch.id,
            serde_json::to_string(event).unwrap()
        ],
    )
    .unwrap();
    if event.branch.shared {
        s.db.execute("INSERT INTO outbox(event) VALUES(?)", [&event.id])
            .unwrap();
    }
}

fn acked(s: &Store, event: &str) -> Option<bool> {
    use rusqlite::OptionalExtension;
    s.db.query_row(
        "SELECT acknowledged FROM outbox WHERE event=?",
        [event],
        |r| r.get(0),
    )
    .optional()
    .unwrap()
}

fn rejected_codes(s: &Store, peer: &Store) -> Vec<(String, String)> {
    s.feed_rejections(&peer.device)
        .unwrap()
        .into_iter()
        .map(|r| (r.id, r.code))
        .collect()
}

/// Compact JSON bytes of a request page's entries, as `feed-offer` sends them.
fn entry_bytes(request: &MetaExchangeRequest) -> usize {
    let value = sync_rpc::wire(request).unwrap();
    value["page"]["entries"].to_string().len()
}

// ---------------------------------------------------------------------------
// Feed identity

#[test]
fn epoch_tip_and_cursors_cover_committed_shared_events_only() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let epoch = a.feed_epoch().unwrap();
    assert_eq!(epoch.len(), 32);
    assert!(epoch.bytes().all(|b| b.is_ascii_hexdigit()));
    assert_eq!(a.feed_tip().unwrap(), 0);
    let version: i64 =
        a.db.query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
    assert_eq!(version, 3);

    a.create("one", "file").unwrap();
    let tip = a.feed_tip().unwrap();
    assert!(tip > 0);
    let first = a.journal_id_at(tip).unwrap().expect("shared event at tip");
    let at = |seq: i64, id: &str| Cursor {
        epoch: epoch.clone(),
        seq,
        id: id.into(),
    };
    assert!(a.validate_cursor(&at(tip, &first)).unwrap());
    assert!(a.validate_cursor(&Cursor::start(&epoch)).unwrap());
    assert!(!a.validate_cursor(&at(tip, "another")).unwrap());
    assert!(!a.validate_cursor(&at(tip + 1, &first)).unwrap());
    assert!(!a.validate_cursor(&at(-1, "")).unwrap());
    assert!(
        !a.validate_cursor(&Cursor {
            epoch: "0".repeat(32),
            seq: tip,
            id: first.clone()
        })
        .unwrap()
    );

    // Private events hold journal seqs but never move the tip, never appear
    // at a seq, and never make a cursor valid.
    let main = a.active.clone();
    let private = a.fork("private", false).unwrap();
    a.checkout(&private.id).unwrap();
    a.create("secret", "file").unwrap();
    a.checkout(&main).unwrap();
    let (max_seq, private_id): (i64, String) =
        a.db.query_row(
            "SELECT seq,event FROM journal_sequence ORDER BY seq DESC LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert!(max_seq > tip);
    assert_eq!(a.feed_tip().unwrap(), tip);
    assert_eq!(a.journal_id_at(max_seq).unwrap(), None);
    assert!(!a.validate_cursor(&at(max_seq, &private_id)).unwrap());
    let page = a.feed_page(Some(&Cursor::start(&epoch)), "peer", 100, MAX_PAGE_BYTES);
    let page = page.unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.through, at(tip, &first));
    assert!(!page.more);

    // A journal that loses or rewrites a row is a new feed.
    a.db.execute("UPDATE events SET payload=payload WHERE id=?", [&first])
        .unwrap();
    let rewritten = a.feed_epoch().unwrap();
    assert_ne!(rewritten, epoch);
    a.db.execute("DELETE FROM events WHERE id=?", [&private_id])
        .unwrap();
    let deleted = a.feed_epoch().unwrap();
    assert_ne!(deleted, rewritten);
    assert!(!a.validate_cursor(&at(tip, &first)).unwrap());
    // ... and the same epoch survives a reopen.
    drop(a);
    let a = reopen(&dir.path().join("a"), &repo);
    assert_eq!(a.feed_epoch().unwrap(), deleted);
}

#[test]
fn pages_are_bounded_and_contiguous() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    linear_history(&mut a, MAX_PAGE_ENTRIES + 100).unwrap();
    let epoch = a.feed_epoch().unwrap();
    let tip = a.feed_tip().unwrap();
    // Payloads: the 1 MiB budget cuts the page.
    let first = a
        .feed_page(
            Some(&Cursor::start(&epoch)),
            "p",
            MAX_PAGE_ENTRIES,
            MAX_PAGE_BYTES,
        )
        .unwrap();
    assert!(!first.reset);
    assert!(first.entries.len() < MAX_PAGE_ENTRIES);
    assert!(first.entries.iter().all(|e| e.event.is_some()));
    assert!(first.more);
    assert_eq!(first.through.seq, first.entries.last().unwrap().seq);
    let payload: usize = first
        .entries
        .iter()
        .filter_map(|e| e.event.as_ref())
        .map(|e| serde_json::to_string(e).unwrap().len())
        .sum();
    assert!(payload <= MAX_PAGE_BYTES);
    let next = a
        .feed_page(Some(&first.through), "p", MAX_PAGE_ENTRIES, MAX_PAGE_BYTES)
        .unwrap();
    assert_eq!(next.after.as_ref(), Some(&first.through));
    assert!(next.entries[0].seq > first.through.seq);
    // A page always carries its first payload.
    let small = a
        .feed_page(Some(&Cursor::start(&epoch)), "p", MAX_PAGE_ENTRIES, 1)
        .unwrap();
    assert_eq!(small.entries.len(), 1);
    assert!(small.more);
    // Elided entries (a peer known to hold every event): the entry cap cuts.
    a.db.execute("INSERT INTO peer_known SELECT 'q',id FROM events", [])
        .unwrap();
    let elided = a
        .feed_page(
            Some(&Cursor::start(&epoch)),
            "q",
            MAX_PAGE_ENTRIES,
            MAX_PAGE_BYTES,
        )
        .unwrap();
    assert_eq!(elided.entries.len(), MAX_PAGE_ENTRIES);
    assert!(
        elided
            .entries
            .iter()
            .all(|e| e.event.is_none() && e.large.is_none())
    );
    assert!(elided.more);
    let second = a
        .feed_page(Some(&elided.through), "q", MAX_PAGE_ENTRIES, MAX_PAGE_BYTES)
        .unwrap();
    assert_eq!(second.entries.len(), 100);
    assert_eq!(second.through.seq, tip);
    assert!(!second.more);
    // A probe carries nothing; an invalid cursor resets from the start.
    let probe = a
        .feed_page(None, "p", MAX_PAGE_ENTRIES, MAX_PAGE_BYTES)
        .unwrap();
    assert!(probe.after.is_none() && probe.entries.is_empty() && probe.more);
    let bogus = Cursor {
        epoch: epoch.clone(),
        seq: 3,
        id: "not-at-3".into(),
    };
    let reset = a
        .feed_page(Some(&bogus), "p", MAX_PAGE_ENTRIES, MAX_PAGE_BYTES)
        .unwrap();
    assert!(reset.reset);
    assert_eq!(reset.after, Some(Cursor::start(&epoch)));
    assert_eq!(reset.entries[0].seq, first.entries[0].seq);
}

// ---------------------------------------------------------------------------
// Convergence

#[test]
fn two_replicas_converge_on_5k_events() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    linear_history(&mut a, 5000).unwrap();
    let rounds = settle(&mut b, &mut a);
    assert!(rounds > 2, "a 5K catch-up takes several pages ({rounds})");
    assert_converged(&[&a, &b]);
    // Then B's own history flows back (concurrent histories would only test
    // the slow activation path, WP1.7's, at 5K in a debug build).
    linear_history_named(&mut b, "from-b", 20).unwrap();
    settle(&mut b, &mut a);
    assert_converged(&[&a, &b]);
    assert_eq!(shared(&a).len(), 5020);
    // Evidence is per id and complete in both directions.
    assert_eq!(peer_known(&a, &b), shared(&a));
    assert_eq!(peer_known(&b, &a), shared(&b));
    let counters = b.peer_feed_counters(&a.device).unwrap();
    assert!(counters.caught_up);
    assert_eq!(counters.cursor_seq, counters.remote_tip);
    assert_eq!((counters.to_send, counters.to_receive), (0, 0));
    assert_eq!(counters.rejected, 0);
    assert!(b.feed_gate(&a.device).unwrap().caught_up);
    assert!(a.feed_gate(&b.device).unwrap().caught_up);
}

#[test]
fn three_node_relay_converges() {
    // A <-> B <-> C; A and C never exchange.
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    let mut c = create(&dir.path().join("c"), &repo);
    linear_history_named(&mut a, "a", 40).unwrap();
    linear_history_named(&mut c, "c", 30).unwrap();
    b.create("from-b", "directory").unwrap();
    for _ in 0..20 {
        let ab = exchange(&b, &a);
        let cb = exchange(&c, &b);
        let activated = activate(&mut a) + activate(&mut b) + activate(&mut c);
        if activated == 0
            && [ab.applied, ab.local, cb.applied, cb.local]
                .iter()
                .all(|r| !r.summary.metadata_advanced)
        {
            break;
        }
    }
    assert_converged(&[&a, &b, &c]);
    assert_eq!(shared(&a).len(), 71);
    // A learned C's events from B's feed; it has no evidence about C at all.
    assert!(a.known_by_peer(&c.device).unwrap().is_empty());
    assert_eq!(peer_known(&a, &b), shared(&a));
}

#[test]
fn staged_events_not_resent() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let b = create(&dir.path().join("b"), &repo);
    linear_history(&mut a, 300).unwrap();
    // Activation paused: B only stages.
    let first = exchange(&b, &a);
    assert_eq!(first.reply_payloads(), 300);
    assert_eq!(first.applied.summary.staged, 300);
    assert_eq!(b.inbox_counts().unwrap().total(), 300);
    for _ in 0..3 {
        let again = exchange(&b, &a);
        assert_eq!(again.reply_payloads(), 0, "staged events were re-sent");
        assert!(again.reply.page.entries.is_empty());
        assert_eq!(again.applied.summary.received_events, 0);
        assert_eq!(again.applied.summary.cursor_in, a.feed_tip().unwrap());
    }
}

#[test]
fn idle_exchange_reads_no_payloads_and_echo_is_small() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    let n = 2000;
    linear_history(&mut a, n).unwrap();
    // A's pages reach B; B activates; then B's feed echoes A's events back,
    // elided (B learned from A's feed that A holds them), once.
    let mut echo = vec![];
    for _ in 0..50 {
        let round = exchange(&b, &a);
        if !round.request.page.entries.is_empty() {
            assert_eq!(round.request_payloads(), 0, "the echo carries no payload");
            echo.push((
                round.request.page.entries.len(),
                entry_bytes(&round.request),
            ));
        }
        activate(&mut b);
        if !round.applied.summary.metadata_advanced && !round.local.summary.metadata_advanced {
            break;
        }
    }
    let (entries, bytes) = echo
        .iter()
        .fold((0, 0), |(e, b), (entries, bytes)| (e + entries, b + bytes));
    assert_eq!(entries, n, "every event is echoed exactly once");
    assert!(
        bytes / entries < 100,
        "echo costs {} B/event",
        bytes / entries
    );
    assert_converged(&[&a, &b]);
    // Idle: no payload row read on either side, and fast (the best of 20
    // rounds, so a busy machine does not decide).
    let mut best = Duration::MAX;
    for _ in 0..20 {
        let reads = (a.feed_payload_reads(), b.feed_payload_reads());
        let started = Instant::now();
        let idle = exchange(&b, &a);
        best = best.min(started.elapsed());
        assert_eq!((a.feed_payload_reads(), b.feed_payload_reads()), reads);
        assert!(idle.request.page.entries.is_empty() && idle.reply.page.entries.is_empty());
        assert!(!idle.applied.summary.metadata_advanced);
        assert!(!idle.local.summary.metadata_advanced);
        assert!(idle.applied.summary.caught_up && idle.local.summary.caught_up);
    }
    assert!(
        best < Duration::from_millis(5),
        "idle exchange took {best:?}"
    );
}

/// Fix round 2: an idle exchange stays O(1) when private events follow the
/// last shared one (the tip, cursor checks and counts never visit private
/// rows), in both directions.
#[test]
fn idle_exchange_is_o1_with_a_private_tail() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    linear_history(&mut a, 50).unwrap();
    settle(&mut b, &mut a);
    settle(&mut a, &mut b);
    let tip = a.feed_tip().unwrap();
    // 3,000 private events on A after its last shared one.
    let main = a.active.clone();
    let private = a.fork("private", false).unwrap();
    a.checkout(&private.id).unwrap();
    linear_history_named(&mut a, "private", 3_000).unwrap();
    a.checkout(&main).unwrap();
    let private_rows: i64 =
        a.db.query_row(
            "SELECT count(*) FROM journal_sequence WHERE seq>?",
            [tip],
            |r| r.get(0),
        )
        .unwrap();
    assert!(private_rows >= 3_000);
    assert_eq!(
        a.feed_tip().unwrap(),
        tip,
        "private events never move the tip"
    );
    // One round notices nothing new; then idle, both ways.
    exchange(&b, &a);
    exchange(&a, &b);
    let mut best = Duration::MAX;
    for _ in 0..20 {
        let reads = (a.feed_payload_reads(), b.feed_payload_reads());
        let started = Instant::now();
        let dialed = exchange(&b, &a);
        let dialing = exchange(&a, &b);
        best = best.min(started.elapsed() / 2);
        assert_eq!((a.feed_payload_reads(), b.feed_payload_reads()), reads);
        for round in [&dialed, &dialing] {
            assert!(round.request.page.entries.is_empty() && round.reply.page.entries.is_empty());
            assert!(round.request.need.is_empty());
            for summary in [&round.applied.summary, &round.local.summary] {
                assert!(!summary.metadata_advanced, "{summary:?}");
                assert!(summary.caught_up && summary.to_send == 0 && summary.to_receive == 0);
            }
        }
        assert_eq!(dialed.local.summary.local_tip, tip);
        assert_eq!(dialing.applied.summary.local_tip, tip);
    }
    assert!(
        best < Duration::from_millis(5),
        "idle exchange with a private tail took {best:?}"
    );
}

/// `feed_journal` (the feed as a table) follows every journal change and
/// is built when it is missing (a store that a build without it opened).
#[test]
fn feed_journal_follows_the_journal() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let root = dir.path().join("a");
    let mut a = create(&root, &repo);
    let feed = |s: &Store| -> Vec<(i64, String)> {
        s.db.prepare("SELECT seq,event FROM feed_journal ORDER BY seq")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    };
    // What `feed_journal` must hold: the shared rows of `journal_sequence`.
    let expected = |s: &Store| -> Vec<(i64, String)> {
        s.db.prepare(
            "SELECT s.seq,s.event FROM journal_sequence s JOIN events e ON e.id=s.event
            JOIN branches b ON b.id=e.branch WHERE b.shared=1 ORDER BY s.seq",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
    };
    linear_history(&mut a, 5).unwrap();
    let main = a.active.clone();
    let private = a.fork("private", false).unwrap();
    a.checkout(&private.id).unwrap();
    a.create("secret", "file").unwrap();
    a.checkout(&main).unwrap();
    a.create("after", "file").unwrap();
    let shared_rows = feed(&a);
    assert_eq!(shared_rows, expected(&a));
    assert_eq!(shared_rows.len(), shared(&a).len());
    assert_eq!(a.feed_tip().unwrap(), shared_rows.last().unwrap().0);
    // A lost current derived feed index is rebuilt from the accepted sequence.
    a.db.execute_batch(
        "DROP TRIGGER feed_journal_insert; DROP TRIGGER feed_journal_update;
        DROP TRIGGER feed_journal_delete; DROP TRIGGER feed_journal_branch; DROP TABLE feed_journal;",
    )
    .unwrap();
    drop(a);
    let mut a = reopen(&root, &repo);
    assert_eq!(feed(&a), shared_rows);
    assert_eq!(a.feed_tip().unwrap(), shared_rows.last().unwrap().0);
    a.create("reopened", "file").unwrap();
    assert_eq!(feed(&a).len(), shared_rows.len() + 1);
    assert_eq!(feed(&a), expected(&a));
    // A rewritten row; a deleted one (the newest shared event).
    let (first_seq, first) = shared_rows[0].clone();
    a.db.execute("UPDATE events SET payload=payload WHERE id=?", [&first])
        .unwrap();
    assert_eq!(feed(&a), expected(&a));
    assert!(feed(&a).iter().any(|(seq, _)| *seq == first_seq));
    let (last_seq, last) = feed(&a).last().unwrap().clone();
    a.db.execute("DELETE FROM events WHERE id=?", [&last])
        .unwrap();
    assert!(!feed(&a).iter().any(|(seq, _)| *seq == last_seq));
    assert_eq!(feed(&a), expected(&a));
    assert!(a.feed_tip().unwrap() < last_seq);
    // A branch that stops (or starts) being shared leaves (or joins) the feed.
    a.db.execute("UPDATE branches SET shared=0 WHERE id=?", [&main])
        .unwrap();
    assert!(feed(&a).is_empty());
    assert_eq!(a.feed_tip().unwrap(), 0);
    a.db.execute("UPDATE branches SET shared=1 WHERE id=?", [&main])
        .unwrap();
    assert!(!feed(&a).is_empty());
    assert_eq!(feed(&a), expected(&a));
}

// ---------------------------------------------------------------------------
// Acknowledgements (invariant 10)

#[test]
fn staged_never_acks() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    linear_history(&mut a, 25).unwrap();
    let authored = shared(&a);
    for _ in 0..4 {
        let round = exchange(&b, &a);
        assert_eq!(round.applied.summary.acked, 0);
        assert_eq!(round.local.summary.acked, 0);
    }
    assert_eq!(b.inbox_counts().unwrap().total(), 25);
    assert_eq!(outgoing(&a), 25, "a staged event is not an ack");
    assert!(peer_known(&a, &b).is_empty());
    // B commits; its feed is the evidence, and the acks arrive with it.
    assert_eq!(activate(&mut b), 25);
    let round = exchange(&b, &a);
    assert_eq!(round.local.summary.acked, 25);
    assert_eq!(outgoing(&a), 0);
    assert_eq!(peer_known(&a, &b), authored);
    // B's received events were acked as they committed: A's feed had listed
    // them (the `feed_outbox_known` trigger).
    assert_eq!(outgoing(&b), 0);
}

#[test]
fn a_later_commit_is_acked_by_evidence_already_held() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    a.create("dir", "directory").unwrap();
    exchange(&b, &a);
    let ids = shared(&a);
    // Staged at B: evidence that A committed it, but no outbox row yet.
    assert_eq!(peer_known(&b, &a), ids);
    assert!(ids.iter().all(|id| acked(&b, id).is_none()));
    activate(&mut b);
    assert!(ids.iter().all(|id| acked(&b, id) == Some(true)));
    assert_eq!(outgoing(&b), 0);
}

#[test]
fn rejected_never_acks() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    linear_history(&mut a, 5).unwrap();
    settle(&mut b, &mut a);
    assert_converged(&[&a, &b]);
    // An unauthorized event in A's journal: B's own replica id as its author
    // (a forged origin). B refuses it at staging.
    let mut forged = mkdir_event(&a, "forged");
    forged.device = b.device.clone();
    inject(&a, &forged);
    // 300 events every activation quarantines.
    let invalid: Vec<Event> = (0..300).map(|_| invalid_event(&a)).collect();
    for event in &invalid {
        inject(&a, event);
    }
    for _ in 0..4 {
        exchange(&b, &a);
        activate(&mut b);
    }
    assert_eq!(
        rejected_codes(&b, &a),
        vec![(forged.id.clone(), "FORGED_EVENT_ORIGIN".to_owned())]
    );
    let inbox = b.inbox_counts().unwrap();
    assert_eq!(inbox.quarantined, 300);
    assert_eq!(inbox.pending_active(), 0);
    // None of them is in B's feed, so none is ever acked at A.
    assert_eq!(acked(&a, &forged.id), Some(false));
    assert!(invalid.iter().all(|e| acked(&a, &e.id) == Some(false)));
    assert_eq!(outgoing(&a), 301);
    let known = peer_known(&a, &b);
    assert!(!known.contains(&forged.id));
    assert!(invalid.iter().all(|e| !known.contains(&e.id)));
    assert!(!peer_known(&b, &a).contains(&forged.id));
    assert_eq!(outgoing(&b), 0);
    // The rejection is durable and visible: B's gate on A is blocked.
    let gate = b.feed_gate(&a.device).unwrap();
    assert!(gate.blocked && !gate.caught_up);
    assert!(
        gate.waiting_on
            .iter()
            .any(|w| w == "300 events quarantined")
    );
    assert!(gate.waiting_on.iter().any(|w| w == "1 event rejected"));
    let counters = b.peer_feed_counters(&a.device).unwrap();
    assert_eq!((counters.quarantined, counters.rejected), (300, 1));
    drop(b);
    let b = reopen(&dir.path().join("b"), &repo);
    let rejections = b.feed_rejections(&a.device).unwrap();
    assert_eq!(rejections.len(), 1);
    assert_eq!(rejections[0].class, ErrorClass::Fatal);
}

#[test]
fn third_peer_staged_not_acked() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    let mut c = create(&dir.path().join("c"), &repo);
    a.create("x", "directory").unwrap();
    let x = shared(&a);
    // B stages A's event but does not activate it.
    exchange(&b, &a);
    assert_eq!(b.inbox_counts().unwrap().total(), x.len() as u64);
    // C (a third peer) exchanges with B: B's feed has nothing of it.
    for _ in 0..3 {
        let round = exchange(&c, &b);
        assert!(round.reply.page.entries.is_empty());
        assert!(round.reply.need_served.is_empty());
    }
    assert_eq!(c.inbox_counts().unwrap().total(), 0);
    assert!(shared(&c).is_empty());
    exchange(&b, &a);
    assert_eq!(
        outgoing(&a),
        x.len() as i64,
        "staged at a third peer is not an ack"
    );
    // Committed at B: B's feed carries it to C, and its evidence to A.
    activate(&mut b);
    exchange(&c, &b);
    activate(&mut c);
    assert_eq!(shared(&c), x);
    exchange(&b, &a);
    assert_eq!(outgoing(&a), 0);
    // C never exchanged with A: A has no evidence about C.
    assert!(a.known_by_peer(&c.device).unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// Validation and privacy (invariants 6, 12, 14)

#[test]
fn forged_origin_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    a.create("base", "directory").unwrap();
    settle(&mut b, &mut a);
    // A's journal carries two events in B's name that B never committed: a
    // new one, and a rewrite of one B did commit.
    let mut invented = mkdir_event(&a, "invented");
    invented.device = b.device.clone();
    let real = mkdir_event(&b, "real");
    b.commit(real.clone(), true).unwrap();
    let mut rewritten = real.clone();
    rewritten.created_ms += 1;
    inject(&a, &invented);
    inject(&a, &rewritten);
    let round = exchange(&b, &a);
    let mut codes = rejected_codes(&b, &a);
    codes.sort();
    let mut expected = vec![
        (invented.id.clone(), "FORGED_EVENT_ORIGIN".to_owned()),
        (real.id.clone(), "FORGED_EVENT_ORIGIN".to_owned()),
    ];
    expected.sort();
    assert_eq!(codes, expected);
    assert_eq!(round.applied.summary.rejected, 2);
    assert_eq!(round.applied.summary.staged, 0);
    // The cursor moves past refused entries.
    assert_eq!(round.applied.summary.cursor_in, a.feed_tip().unwrap());
    assert_eq!(b.inbox_counts().unwrap().total(), 0);
    assert_eq!(
        b.events().unwrap().into_iter().find(|e| e.id == real.id),
        Some(real.clone()),
        "B's own event is never rewritten"
    );
    // A refuses B's real event as a reuse of the id it holds; no ack either way.
    exchange(&b, &a);
    assert_eq!(
        rejected_codes(&a, &b),
        vec![(real.id.clone(), "EVENT_ID_REUSED".to_owned())]
    );
    assert_eq!(acked(&a, &invented.id), Some(false));
    assert_eq!(acked(&a, &real.id), Some(false));
    assert_eq!(acked(&b, &real.id), Some(false));
    assert!(!peer_known(&b, &a).contains(&real.id));
}

#[test]
fn event_id_reuse_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    // First contact (B probes A's cursor of its feed).
    exchange(&b, &a);
    // One id, two payloads: B committed its own, A's journal has another.
    let mine = mkdir_event(&b, "mine");
    b.commit(mine.clone(), true).unwrap();
    let mut theirs = mkdir_event(&a, "theirs");
    theirs.id = mine.id.clone();
    inject(&a, &theirs);
    let round = exchange(&b, &a);
    assert_eq!(
        rejected_codes(&b, &a),
        vec![(mine.id.clone(), "EVENT_ID_REUSED".to_owned())]
    );
    assert_eq!(
        rejected_codes(&a, &b),
        vec![(mine.id.clone(), "EVENT_ID_REUSED".to_owned())]
    );
    assert_eq!(round.applied.summary.cursor_in, a.feed_tip().unwrap());
    assert_eq!(round.local.summary.cursor_in, b.feed_tip().unwrap());
    assert_eq!(
        (acked(&a, &mine.id), acked(&b, &mine.id)),
        (Some(false), Some(false))
    );
    assert!(peer_known(&a, &b).is_empty() && peer_known(&b, &a).is_empty());
    let rejected = &round.reply.rejected;
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].code, "EVENT_ID_REUSED");
    assert_eq!(rejected[0].class, ErrorClass::Fatal);
    // Both journals are unchanged.
    assert_eq!(
        b.events().unwrap().into_iter().find(|e| e.id == mine.id),
        Some(mine)
    );
}

#[test]
fn private_never_served() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    let main = a.active.clone();
    let file = a.create("code", "file").unwrap();
    a.write_revision(&file.id, file.revisions.get("content").cloned(), b"shared")
        .unwrap();
    let private = a.fork("private", false).unwrap();
    a.checkout(&private.id).unwrap();
    let canary = a.create("canary", "file").unwrap();
    a.write_revision(
        &canary.id,
        canary.revisions.get("content").cloned(),
        b"PRIVATE-FEED-CANARY",
    )
    .unwrap();
    a.checkout(&main).unwrap();
    let canary_hash = tkfs::core::hash(b"PRIVATE-FEED-CANARY");
    let private_ids: Vec<String> = a
        .events()
        .unwrap()
        .into_iter()
        .filter(|e| !e.branch.shared)
        .map(|e| e.id)
        .collect();
    assert!(!private_ids.is_empty());
    let mut wire_text = String::new();
    for _ in 0..3 {
        for (d, x) in [(&b, &a), (&a, &b)] {
            let round = exchange(d, x);
            wire_text.push_str(&sync_rpc::wire(&round.request).unwrap().to_string());
            wire_text.push_str(&sync_rpc::wire(&round.reply).unwrap().to_string());
        }
        activate(&mut b);
        activate(&mut a);
    }
    for secret in private_ids.iter().chain([&private.id, &canary_hash]) {
        assert!(
            !wire_text.contains(secret.as_str()),
            "{secret} left the replica"
        );
    }
    assert!(b.branch(&private.id).is_err());
    assert!(b.read_object(&canary_hash).is_err());
    assert_eq!(shared(&b), shared(&a));
    // Asked for by id, a private event is unknown.
    let mut request = b.build_exchange_request(&route(&b, &a)).unwrap();
    request.need = private_ids.clone();
    let (reply, _) = a.handle_exchange_request(&route(&a, &b), &request).unwrap();
    assert!(reply.need_served.is_empty());
    assert_eq!(
        reply.unknown.iter().collect::<BTreeSet<_>>(),
        private_ids.iter().collect::<BTreeSet<_>>()
    );
    let (served, unknown) = a.events_by_ids(&private_ids, usize::MAX).unwrap();
    assert!(served.is_empty());
    assert_eq!(unknown.len(), private_ids.len());
}

#[test]
fn private_branch_payloads_are_refused_at_staging() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let b = create(&dir.path().join("b"), &repo);
    a.create("base", "directory").unwrap();
    // A misbehaving sender puts a private-branch event in its page.
    let mut private = mkdir_event(&a, "leak");
    private.branch = Branch {
        id: id(),
        name: "private".into(),
        shared: false,
    };
    let leaked = private.clone();
    let round = exchange_with(&b, &a, move |reply| {
        let mut entry = reply.page.entries[0].clone();
        entry.id = leaked.id.clone();
        entry.digest =
            tkfs::model::protocol::feed::digest(&serde_json::to_string(&leaked).unwrap());
        entry.event = Some(leaked);
        reply.page.entries[0] = entry;
    });
    assert_eq!(
        rejected_codes(&b, &a),
        vec![(private.id.clone(), "PRIVATE_BRANCH_REJECTED".to_owned())]
    );
    assert_eq!(round.applied.summary.staged, 0);
    assert!(b.branch(&private.branch.id).is_err());
    assert!(!peer_known(&b, &a).contains(&private.id));
}

#[test]
fn digest_mismatch_no_ack() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    b.create("from-b", "directory").unwrap();
    settle(&mut b, &mut a);
    assert_converged(&[&a, &b]);
    // A payload whose digest does not match: refused, nothing staged.
    a.create("z", "directory").unwrap();
    let z: String = a.journal_id_at(a.feed_tip().unwrap()).unwrap().unwrap();
    let round = exchange_with(&b, &a, |reply| {
        for entry in &mut reply.page.entries {
            entry.digest = "0".repeat(16);
        }
    });
    assert_eq!(round.applied.summary.rejected, 1);
    assert_eq!(
        rejected_codes(&b, &a),
        vec![(z.clone(), "INVALID_EVENT".to_owned())]
    );
    assert_eq!(b.inbox_counts().unwrap().total(), 0);
    assert!(!peer_known(&b, &a).contains(&z));
    // An elided entry whose digest differs from what B holds: no evidence,
    // so B's own event stays unacknowledged.
    let w = mkdir_event(&b, "w");
    b.commit(w.clone(), true).unwrap();
    exchange(&b, &a);
    activate(&mut a);
    let round = exchange_with(&b, &a, |reply| {
        for entry in &mut reply.page.entries {
            assert!(entry.event.is_none(), "B is known to hold its own event");
            entry.digest = "f".repeat(16);
        }
    });
    assert_eq!(round.reply.page.entries.len(), 1);
    assert!(rejected_codes(&b, &a).contains(&(w.id.clone(), "EVENT_ID_REUSED".to_owned())));
    assert_eq!(acked(&b, &w.id), Some(false));
    assert!(!peer_known(&b, &a).contains(&w.id));
}

/// Rewrite the stored payload of `event` as equivalent JSON that is not
/// `serde_json`'s serialization of the `Event` (sorted keys, the empty
/// `dependencies` and `reviewed` left out): what an older build or another
/// tool may have written.
fn store_non_canonical(s: &Store, event: &str) {
    let payload: String =
        s.db.query_row("SELECT payload FROM events WHERE id=?", [event], |r| {
            r.get(0)
        })
        .unwrap();
    let mut value: Value = serde_json::from_str(&payload).unwrap();
    let object = value.as_object_mut().unwrap();
    for key in ["dependencies", "reviewed"] {
        if object[key].as_array().is_some_and(Vec::is_empty) {
            object.remove(key);
        }
    }
    let text = value.to_string();
    assert_ne!(text, payload);
    assert_eq!(
        serde_json::from_str::<Event>(&text).unwrap(),
        serde_json::from_str::<Event>(&payload).unwrap()
    );
    s.db.execute(
        "UPDATE events SET payload=? WHERE id=?",
        params![text, event],
    )
    .unwrap();
}

#[test]
fn non_canonical_stored_payloads_travel_and_are_acked() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    a.create("plain", "directory").unwrap();
    let plain = a.journal_id_at(a.feed_tip().unwrap()).unwrap().unwrap();
    let big = big_event(&a, "-", 400);
    a.commit(big.clone(), true).unwrap();
    for event in [&plain, &big.id] {
        store_non_canonical(&a, event);
    }
    // Entries, payload-bearing, large or elided, carry the digest of the
    // canonical serialization, which is what receivers check.
    let canonical = |event: &str| {
        let event = a
            .events()
            .unwrap()
            .into_iter()
            .find(|e| e.id == event)
            .unwrap();
        feed::digest(&serde_json::to_string(&event).unwrap())
    };
    let start = Cursor::start(&a.feed_epoch().unwrap());
    a.db.execute("INSERT INTO peer_known SELECT 'q',id FROM events", [])
        .unwrap();
    for peer in ["p", "q"] {
        let page = a
            .feed_page(Some(&start), peer, 100, MAX_PAGE_BYTES)
            .unwrap();
        assert_eq!(page.entries.len(), 2);
        for entry in &page.entries {
            assert_eq!(entry.digest, canonical(&entry.id), "{peer}: {}", entry.id);
        }
    }
    // The plain event travels in a page, the large one through `need`; both
    // stage, activate and are acked from B's (canonical) echo.
    settle(&mut b, &mut a);
    assert!(
        rejected_codes(&b, &a).is_empty(),
        "{:?}",
        rejected_codes(&b, &a)
    );
    assert!(rejected_codes(&a, &b).is_empty());
    assert_converged(&[&a, &b]);
    assert!(shared(&b).contains(&plain) && shared(&b).contains(&big.id));
    assert_eq!(peer_known(&a, &b), shared(&a));
    assert!(b.feed_gate(&a.device).unwrap().caught_up);
}

// ---------------------------------------------------------------------------
// Need

#[test]
fn need_delivers_missing_parents() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    a.create("parent", "directory").unwrap();
    let parent = a.journal_id_at(a.feed_tip().unwrap()).unwrap().unwrap();
    a.create("parent/child", "directory").unwrap();
    // The page that carried the parent is lost; the child arrives orphaned.
    let lost = parent.clone();
    let round = exchange_with(&b, &a, move |reply| {
        reply.page.entries.retain(|entry| entry.id != lost);
    });
    assert_eq!(round.applied.summary.staged, 1);
    assert_eq!(round.applied.summary.cursor_in, a.feed_tip().unwrap());
    assert_eq!(activate(&mut b), 0);
    assert_eq!(b.missing_parents(10).unwrap(), vec![parent.clone()]);
    let gate = b.feed_gate(&a.device).unwrap();
    assert!(!gate.caught_up);
    assert!(
        gate.waiting_on
            .iter()
            .any(|w| w == "1 event waiting for parents")
    );
    // The next request asks for it; A serves it; both activate.
    let round = exchange(&b, &a);
    assert_eq!(round.request.need, vec![parent.clone()]);
    assert_eq!(round.reply.need_served.len(), 1);
    assert_eq!(round.reply.need_served[0].id, parent);
    assert!(round.reply.unknown.is_empty());
    assert_eq!(round.applied.summary.staged, 1);
    assert_eq!(activate(&mut b), 2);
    settle(&mut b, &mut a);
    assert_converged(&[&a, &b]);
}

#[test]
fn unknown_parent_reported() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    a.create("base", "directory").unwrap();
    settle(&mut b, &mut a);
    // A's journal holds an event whose dependency nobody has.
    let mut orphan = mkdir_event(&a, "orphan");
    orphan.dependencies.push("phantom-parent".into());
    inject(&a, &orphan);
    exchange(&b, &a);
    assert_eq!(
        b.missing_parents(10).unwrap(),
        vec!["phantom-parent".to_owned()]
    );
    let round = exchange(&b, &a);
    assert_eq!(round.request.need, vec!["phantom-parent".to_owned()]);
    assert_eq!(round.reply.unknown, vec!["phantom-parent".to_owned()]);
    assert!(round.reply.need_served.is_empty());
    // Not asked again for a while; the gate says why it waits.
    let round = exchange(&b, &a);
    assert!(round.request.need.is_empty());
    assert!(!round.applied.summary.caught_up);
    let gate = b.feed_gate(&a.device).unwrap();
    assert!(!gate.caught_up && !gate.blocked);
    assert!(
        gate.waiting_on
            .iter()
            .any(|w| w == "1 event waiting for parents")
    );
    assert_eq!(b.inbox_counts().unwrap().waiting, 1);
}

/// Fix round 2: a parent refused at staging is never asked for again. A's
/// journal holds a forged event F (in B's name) and A's own child C of it. B
/// refuses F and stages C, which waits on F for good; asking A for F would
/// only bring the same refusal, every exchange, and keep the feed busy.
#[test]
fn a_refused_parent_is_not_requested_again() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    a.create("base", "directory").unwrap();
    settle(&mut b, &mut a);
    let mut forged = mkdir_event(&a, "forged");
    forged.device = b.device.clone();
    inject(&a, &forged);
    let mut child = mkdir_event(&a, "child");
    child.dependencies = vec![forged.id.clone()];
    inject(&a, &child);
    let round = exchange(&b, &a);
    assert_eq!(
        rejected_codes(&b, &a),
        vec![(forged.id.clone(), "FORGED_EVENT_ORIGIN".to_owned())]
    );
    assert_eq!(round.applied.summary.staged, 1);
    activate(&mut b);
    assert_eq!(b.inbox_counts().unwrap().waiting, 1);
    assert_eq!(b.missing_parents(10).unwrap(), vec![forged.id.clone()]);
    let seq = b.feed_rejections(&a.device).unwrap()[0].seq;
    assert!(seq > 0, "the refusal records the entry's feed seq");
    // At most one more round settles the echo; then every exchange is idle
    // on both sides: no need, nothing served or refused, no payload read.
    exchange(&b, &a);
    for _ in 0..6 {
        let reads = (a.feed_payload_reads(), b.feed_payload_reads());
        let round = exchange(&b, &a);
        assert!(round.request.need.is_empty(), "{:?}", round.request.need);
        assert!(round.reply.need_served.is_empty() && round.reply.unknown.is_empty());
        assert_eq!(round.applied.summary.rejected, 0);
        assert!(!round.applied.summary.metadata_advanced);
        assert!(!round.local.summary.metadata_advanced);
        assert_eq!((a.feed_payload_reads(), b.feed_payload_reads()), reads);
        // The reverse direction asks for nothing either.
        let reverse = exchange(&a, &b);
        assert!(reverse.request.need.is_empty() && reverse.reply.need_served.is_empty());
        assert!(!reverse.applied.summary.metadata_advanced);
        assert!(!reverse.local.summary.metadata_advanced);
    }
    // The gate on A says why: the refusal, and the row that waits on it.
    let gate = b.feed_gate(&a.device).unwrap();
    assert!(gate.blocked && !gate.caught_up);
    assert!(gate.waiting_on.iter().any(|w| w == "1 event rejected"));
    assert!(
        gate.waiting_on
            .iter()
            .any(|w| w == "1 event waiting for parents")
    );
    // A peer that serves the refused event unasked has it refused again; the
    // durable row keeps the feed seq of the first refusal.
    let pushed = forged.clone();
    let round = exchange_with(&b, &a, move |reply| reply.need_served.push(pushed));
    assert_eq!(round.applied.summary.rejected, 1);
    let rejections = b.feed_rejections(&a.device).unwrap();
    assert_eq!(rejections.len(), 1);
    assert_eq!(
        (rejections[0].id.as_str(), rejections[0].seq),
        (forged.id.as_str(), seq)
    );
    assert_eq!(acked(&a, &forged.id), Some(false));
    assert_eq!(acked(&a, &child.id), Some(false));
}

/// Fix round 2: the look for missing parents covers every waiting row in
/// turn. Rows that wait for good (on parents refused here) or that are
/// blocked (they keep their waits) never hide a newer row's missing parent,
/// however many there are.
#[test]
fn missing_parents_are_found_past_blocked_and_settled_waits() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    a.create("base", "directory").unwrap();
    settle(&mut b, &mut a);
    // B's inbox, older than anything A sends next: 4,100 blocked rows (each
    // still waiting on a missing parent) and 4,100 waiting rows whose parents
    // B refused from A. More of each than one look at waiting rows covers.
    let rows = 4_100;
    b.db.execute_batch("BEGIN").unwrap();
    for i in 0..rows {
        for (state, parent) in [
            ("blocked", format!("gone-{i:05}")),
            ("waiting", format!("refused-{i:05}")),
        ] {
            let row = id();
            b.db.execute(
                "INSERT INTO incoming_events(id,payload,state,received_ms,attempts) VALUES(?,'{}',?,1,0)",
                params![row, state],
            )
            .unwrap();
            b.db.execute(
                "INSERT INTO incoming_waits(child,parent) VALUES(?,?)",
                params![row, parent],
            )
            .unwrap();
            if state == "waiting" {
                b.db.execute(
                    "INSERT INTO feed_rejected(peer,event,seq,code,message,payload,at_ms) VALUES(?,?,0,'INVALID_EVENT','',NULL,1)",
                    params![a.device, parent],
                )
                .unwrap();
            }
        }
    }
    b.db.execute_batch("COMMIT").unwrap();
    // A new orphan: the page that carried its parent is lost.
    a.create("parent", "directory").unwrap();
    let parent = a.journal_id_at(a.feed_tip().unwrap()).unwrap().unwrap();
    a.create("parent/child", "directory").unwrap();
    let lost = parent.clone();
    let round = exchange_with(&b, &a, move |reply| {
        reply.page.entries.retain(|entry| entry.id != lost);
    });
    assert_eq!(round.applied.summary.staged, 1);
    assert_eq!(activate(&mut b), 0);
    let mut asked = None;
    for round_no in 1..=3 {
        let round = exchange(&b, &a);
        assert!(
            round.request.need.iter().all(|id| id == &parent),
            "only the obtainable parent is asked: {:?}",
            &round.request.need[..round.request.need.len().min(3)]
        );
        if !round.request.need.is_empty() {
            assert_eq!(round.reply.need_served.len(), 1);
            asked = Some(round_no);
            break;
        }
    }
    assert!(asked.is_some(), "the missing parent was never asked for");
    assert_eq!(activate(&mut b), 2, "the parent and its child activate");
    assert!(shared(&b).contains(&parent));
}

/// `s`'s `feed_needs` rows for `peer`: `(event, reason, reported unknown)`.
fn needs_of(s: &Store, peer: &Store) -> Vec<(String, String, bool)> {
    s.db.prepare("SELECT event,reason,missing_at FROM feed_needs WHERE peer=? ORDER BY event")
        .unwrap()
        .query_map([&peer.device], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get::<_, Option<i64>>(2)?.is_some()))
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

#[test]
fn a_listed_need_the_source_lost_does_not_wedge_the_gate() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let (a_root, snapshot) = (dir.path().join("a"), dir.path().join("a-snapshot"));
    let mut a = create(&a_root, &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    a.create("base", "directory").unwrap();
    settle(&mut b, &mut a);
    assert_converged(&[&a, &b]);
    drop(a);
    copy_dir(&a_root, &snapshot).unwrap();
    let mut a = reopen(&a_root, &repo);
    // A commits a large event; B's next exchange lists it (a need).
    let big = big_event(&a, "-", 400);
    a.commit(big.clone(), true).unwrap();
    let round = exchange(&b, &a);
    assert!(
        round
            .reply
            .page
            .entries
            .iter()
            .any(|e| e.id == big.id && e.large.is_some())
    );
    assert_eq!(
        needs_of(&b, &a),
        vec![(big.id.clone(), "large".to_owned(), false)]
    );
    assert_eq!(b.peer_feed_counters(&a.device).unwrap().to_receive, 1);
    // A is restored from before it: A's feed no longer has it. The reset of
    // A's feed drops what the old feed listed; B catches up with A again.
    drop(a);
    std::fs::remove_dir_all(&a_root).unwrap();
    copy_dir(&snapshot, &a_root).unwrap();
    let mut a = reopen(&a_root, &repo);
    let mut resets = 0;
    for _ in 0..12 {
        let round = exchange(&b, &a);
        resets += usize::from(round.reply.page.reset);
        activate(&mut a);
        activate(&mut b);
    }
    assert_eq!(resets, 1);
    assert!(needs_of(&b, &a).is_empty(), "{:?}", needs_of(&b, &a));
    let gate = b.feed_gate(&a.device).unwrap();
    assert!(gate.caught_up && gate.waiting_on.is_empty(), "{gate:?}");
    let counters = b.peer_feed_counters(&a.device).unwrap();
    assert!(
        counters.caught_up && counters.to_receive == 0,
        "{counters:?}"
    );
    assert_converged(&[&a, &b]);
    assert!(!shared(&b).contains(&big.id));
}

#[test]
fn a_listed_need_reported_unknown_is_not_outstanding() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    a.create("one", "directory").unwrap();
    a.create("two", "directory").unwrap();
    // A's page lists a large event A cannot serve (here: a tampered entry).
    let phantom = "phantom-large".to_owned();
    let listed = phantom.clone();
    let round = exchange_with(&b, &a, move |reply| {
        let entry = reply.page.entries.last_mut().unwrap();
        entry.id = listed;
        entry.digest = "0".repeat(16);
        entry.event = None;
        entry.large = Some(300_000);
    });
    assert_eq!(round.applied.summary.to_receive, 1);
    assert!(!round.applied.summary.caught_up);
    let round = exchange(&b, &a);
    assert_eq!(round.request.need, vec![phantom.clone()]);
    assert_eq!(round.reply.unknown, vec![phantom.clone()]);
    activate(&mut b);
    // A has nothing more to give: B's view of A is caught up.
    let counters = b.peer_feed_counters(&a.device).unwrap();
    assert_eq!(counters.to_receive, 0, "{counters:?}");
    assert!(counters.caught_up);
    let round = exchange(&b, &a);
    assert!(round.request.need.is_empty(), "asked again only later");
    assert!(round.applied.summary.caught_up && round.applied.gate.caught_up);
    assert_eq!(needs_of(&b, &a), vec![(phantom, "large".to_owned(), true)]);
}

/// One event creating `dirs` directories with long multi-byte names (about
/// 975 bytes of payload each).
fn big_event(s: &Store, prefix: &str, dirs: usize) -> Event {
    let stem = "€".repeat(240);
    let mut changes = vec![];
    for i in 0..dirs {
        let entity = id();
        let change = |field: &str, value: Value| Change {
            entity: entity.clone(),
            field: field.into(),
            parents: vec![],
            value,
        };
        changes.extend([
            change("kind", json!("directory")),
            change(
                "location",
                json!({"parent": "root", "name": format!("{stem}{prefix}{i:05}")}),
            ),
            change("alive", json!(true)),
        ]);
    }
    s.event(&main_branch(s), changes)
}

#[test]
fn a_3mib_event_travels_through_need() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    let big = big_event(&a, "-", 3300);
    let size = serde_json::to_string(&big).unwrap().len();
    assert!(size > 3 * 1024 * 1024, "{size} bytes");
    a.commit(big.clone(), true).unwrap();
    // The page lists it as large, without its payload.
    let round = exchange(&b, &a);
    let entry = &round.reply.page.entries[0];
    assert_eq!(entry.id, big.id);
    assert!(entry.event.is_none());
    assert_eq!(entry.large, Some(size as u64));
    assert!(!round.applied.summary.caught_up);
    assert_eq!(round.applied.summary.to_receive, 1);
    assert_eq!(
        b.peer_feed_counters(&a.device).unwrap().to_receive,
        1,
        "a listed event we lack is still to receive"
    );
    // The next request asks for it, and it is served whole.
    let round = exchange(&b, &a);
    assert_eq!(round.request.need, vec![big.id.clone()]);
    assert_eq!(round.reply.need_served, vec![big.clone()]);
    assert_eq!(round.applied.summary.staged, 1);
    assert_eq!(activate(&mut b), 1);
    assert_eq!(b.projection(&b.active).unwrap().entries.len(), 3300);
    settle(&mut b, &mut a);
    assert_converged(&[&a, &b]);
}

/// CCR-WP2.5-2 (applied by WP3.1; until then a request page carried a large
/// event inline, which this test pinned): a dialer's large event is flagged
/// in its request page like in a reply page, the acceptor asks for it in its
/// reply (`need`), and the dialer's next request serves it (`need_served`).
#[test]
fn a_dialer_large_event_travels_through_the_acceptors_need() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    // The first request is a probe (B has no report of A's cursor yet).
    exchange(&b, &a);
    linear_history_named(&mut b, "small", 3).unwrap();
    let big = big_event(&b, "+", 400);
    let size = serde_json::to_string(&big).unwrap().len();
    assert!(size > LARGE_EVENT_BYTES);
    b.commit(big.clone(), true).unwrap();
    linear_history_named(&mut b, "after", 3).unwrap();
    // B dials A (one-way): the page lists the big event without its payload.
    let round = exchange(&b, &a);
    let entry = round
        .request
        .page
        .entries
        .iter()
        .find(|e| e.id == big.id)
        .expect("the page lists the big event");
    assert_eq!(entry.event, None);
    assert_eq!(entry.large, Some(size as u64));
    // A asks for it; the events after it are staged already.
    assert_eq!(round.reply.need, vec![big.id.clone()]);
    assert!(round.request.need_served.is_empty());
    activate(&mut a);
    // B's next request serves it.
    let round = exchange(&b, &a);
    assert_eq!(round.request.need_served, vec![big.clone()]);
    assert!(round.request.unknown.is_empty());
    assert!(round.reply.need.is_empty());
    assert_eq!(round.local.summary.staged, 1);
    activate(&mut a);
    activate(&mut b);
    settle(&mut b, &mut a);
    assert_converged(&[&a, &b]);
}

/// CCR-WP2.5-2: under one-way pairing the acceptor gets a missing parent from
/// the dialer (its reply's `need`, the dialer's next `need_served`), and an
/// id the dialer does not have comes back `unknown` (asked again only after
/// the retry delay).
#[test]
fn an_acceptor_gets_a_missing_parent_from_the_dialer() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut d = create(&dir.path().join("d"), &repo);
    // The first request is a probe (D has no report of A's cursor yet).
    exchange(&d, &a);
    d.create("parent", "directory").unwrap();
    let parent = d.journal_id_at(d.feed_tip().unwrap()).unwrap().unwrap();
    d.create("parent/child", "directory").unwrap();
    // The request page that carried the parent loses it: the child arrives
    // at A orphaned (A never dials D).
    let mut request = d.build_exchange_request(&route(&d, &a)).unwrap();
    request.page.entries.retain(|entry| entry.id != parent);
    let (reply, local) = a.handle_exchange_request(&route(&a, &d), &request).unwrap();
    assert_eq!(local.summary.staged, 1);
    d.apply_exchange_reply(&route(&d, &a), &request.request, &reply)
        .unwrap();
    let mut asked = reply.need == vec![parent.clone()];
    assert_eq!(activate(&mut a), 0);
    assert_eq!(a.missing_parents(10).unwrap(), vec![parent.clone()]);
    // A asks D for it in a reply; D serves it in its next request.
    let mut served = None;
    for _ in 0..3 {
        let round = exchange(&d, &a);
        if !round.request.need_served.is_empty() {
            served = Some(round);
            break;
        }
        asked |= round.reply.need == vec![parent.clone()];
    }
    assert!(asked, "A never asked D for the parent");
    let round = served.expect("D never served the parent");
    assert_eq!(round.request.need_served.len(), 1);
    assert_eq!(round.request.need_served[0].id, parent);
    assert_eq!(round.local.summary.staged, 1);
    assert_eq!(activate(&mut a), 2);
    settle(&mut d, &mut a);
    assert_converged(&[&a, &d]);
    assert!(a.missing_parents(10).unwrap().is_empty());

    // A parent the dialer does not have either: reported unknown, and not
    // asked again within the retry delay.
    let mut orphan = mkdir_event(&d, "orphan");
    orphan.dependencies.push("phantom-parent".into());
    inject(&d, &orphan);
    let phantom = vec!["phantom-parent".to_owned()];
    let round = exchange(&d, &a);
    let mut asked = round.reply.need == phantom;
    activate(&mut a);
    assert_eq!(a.missing_parents(10).unwrap(), phantom);
    let mut reported = false;
    for _ in 0..3 {
        let round = exchange(&d, &a);
        if round.request.unknown == phantom {
            assert!(round.request.need_served.is_empty());
            assert!(round.reply.need.is_empty(), "asked again at once");
            reported = true;
            break;
        }
        asked |= round.reply.need == phantom;
    }
    assert!(
        asked && reported,
        "asked {asked}, reported unknown {reported}"
    );
    let round = exchange(&d, &a);
    assert!(
        round.reply.need.is_empty(),
        "asked again within the retry delay"
    );
    assert!(round.request.unknown.is_empty());
}

/// CCR-WP2.5-2, WP3.1 fix round 2: what an acceptor asks of one dialer is
/// served by that dialer only. Two replicas of one repository in one process
/// dial the same acceptor: the other replica's next request neither serves
/// nor reports `unknown` the first one's need, and the first one's next
/// request still serves it.
#[test]
fn a_need_is_served_by_the_replica_it_was_asked_of() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    let c = create(&dir.path().join("c"), &repo);
    // The first requests are probes (no report of A's cursor yet).
    exchange(&b, &a);
    exchange(&c, &a);
    let big = big_event(&b, "+", 400);
    assert!(serde_json::to_string(&big).unwrap().len() > LARGE_EVENT_BYTES);
    b.commit(big.clone(), true).unwrap();
    // B dials A: A asks B for the big event.
    let round = exchange(&b, &a);
    assert_eq!(round.reply.need, vec![big.id.clone()]);
    // C's next request to A carries nothing of B's need.
    let round = exchange(&c, &a);
    assert!(
        round.request.need_served.is_empty() && round.request.unknown.is_empty(),
        "C answered B's need: served {:?}, unknown {:?}",
        round.request.need_served,
        round.request.unknown
    );
    // B's next request serves it.
    let round = exchange(&b, &a);
    assert_eq!(round.request.need_served, vec![big.clone()]);
    assert!(round.request.unknown.is_empty());
    assert_eq!(round.local.summary.staged, 1);
    activate(&mut a);
    activate(&mut b);
    settle(&mut b, &mut a);
    assert!(shared(&a).contains(&big.id));
}

/// CCR-WP2.5-4 (applied by WP3.1): a page reports the sender's exact count of
/// its feed entries after `through`, so `to_receive` is exact when the
/// sender's journal interleaves private events (before, it was the upper
/// bound `tip - cursor`, which counts every private seq).
#[test]
fn to_receive_is_exact_with_private_events_in_the_senders_journal() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let b = create(&dir.path().join("b"), &repo);
    // B's first request is a probe; A's first page starts at B's cursor.
    exchange(&b, &a);
    let main = a.active.clone();
    let private = a.fork("private", false).unwrap();
    // 10 shared events, each followed by 3 private events.
    for i in 0..10 {
        a.checkout(&main).unwrap();
        linear_history_named(&mut a, &format!("shared-{i}"), 1).unwrap();
        a.checkout(&private.id).unwrap();
        linear_history_named(&mut a, &format!("private-{i}"), 3).unwrap();
    }
    a.checkout(&main).unwrap();
    let shared_total: u64 =
        a.db.query_row("SELECT count(*) FROM feed_journal", [], |r| r.get(0))
            .unwrap();
    // A short page of A's feed (3 entries), as A builds it for B, in place
    // of the full one.
    let peer = b.device.clone();
    let round = exchange_with(&b, &a, |reply| {
        reply.page = a
            .feed_page(reply.page.after.as_ref(), &peer, 3, MAX_PAGE_BYTES)
            .unwrap();
    });
    let page = &round.reply.page;
    assert!(page.more);
    assert_eq!(page.entries.len(), 3);
    let after: u64 =
        a.db.query_row(
            "SELECT count(*) FROM feed_journal WHERE seq>?",
            [page.through.seq],
            |r| r.get(0),
        )
        .unwrap();
    assert!(after > 0 && after < shared_total);
    assert_eq!(page.remaining, Some(after));
    let summary = &round.applied.summary;
    assert_eq!(summary.to_receive, after);
    let bound = (summary.remote_tip - summary.cursor_in) as u64;
    assert!(
        summary.to_receive < bound,
        "{} is the bound {bound}, not the exact count",
        summary.to_receive
    );
    // The heartbeat's counters agree, and an exhausted feed reports 0.
    assert_eq!(b.peer_feed_counters(&a.device).unwrap().to_receive, after);
    let round = exchange(&b, &a);
    assert!(!round.reply.page.more);
    assert_eq!(round.reply.page.remaining, Some(0));
    assert_eq!(round.applied.summary.to_receive, 0);
}

// ---------------------------------------------------------------------------
// Regressions, resets and cursor recovery (I2)

#[test]
fn peer_restore_requeues_withdrawn_events() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let (a_root, b_root, snapshot) = (
        dir.path().join("a"),
        dir.path().join("b"),
        dir.path().join("b-snapshot"),
    );
    let mut a = create(&a_root, &repo);
    let mut b = create(&b_root, &repo);
    linear_history(&mut a, 20).unwrap();
    settle(&mut b, &mut a);
    assert_converged(&[&a, &b]);
    drop(b);
    copy_dir(&b_root, &snapshot).unwrap();
    let mut b = reopen(&b_root, &repo);
    let before = shared(&a);
    linear_history_named(&mut a, "late", 10).unwrap();
    let late: BTreeSet<String> = shared(&a).difference(&before).cloned().collect();
    settle(&mut b, &mut a);
    assert_converged(&[&a, &b]);
    // Restore B: it loses the late events and its record of them.
    drop(b);
    std::fs::remove_dir_all(&b_root).unwrap();
    copy_dir(&snapshot, &b_root).unwrap();
    let mut b = reopen(&b_root, &repo);
    assert!(late.iter().all(|id| !shared(&b).contains(id)));
    let round = exchange(&b, &a);
    // A saw B's cursor of A's feed go back: B regressed. A forgets what it
    // knew about B (its outbox is queued again) and re-sends the withdrawn
    // events with their payloads.
    assert!(peer_known(&a, &b).is_empty());
    assert_eq!(outgoing(&a), 30);
    let resent: BTreeSet<String> = round
        .reply
        .page
        .entries
        .iter()
        .filter(|e| e.event.is_some())
        .map(|e| e.id.clone())
        .collect();
    assert_eq!(resent, late);
    activate(&mut b);
    settle(&mut b, &mut a);
    assert_converged(&[&a, &b]);
    assert_eq!(peer_known(&a, &b), shared(&a));
}

/// Post-integration re-review (WP2.INT): a reply's cursor is authoritative
/// across channels. D's request channel holds a report from when A used to
/// dial (seq 5); A is then restored from a backup taken before, and only D
/// dials. A's `CursorMismatch.expected` (seq 0) is lower than that report:
/// A regressed, D forgets it and pages from where A really is. Before the
/// fix D kept paging after the stale report: every round answered
/// `CursorMismatch` and A never received any of D's events.
#[test]
fn a_restored_acceptor_is_found_through_the_other_channel() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let (a_root, snapshot) = (dir.path().join("a"), dir.path().join("a-backup"));
    let mut d = create(&dir.path().join("d"), &repo);
    let a = create(&a_root, &repo);
    linear_history(&mut d, 5).unwrap();
    // D dials A once (a probe): D's reply channel has A's cursor at 0.
    exchange(&d, &a);
    drop(a);
    copy_dir(&a_root, &snapshot).unwrap();
    let mut a = reopen(&a_root, &repo);
    // A dials D twice: D's request channel now has A's cursor at 5.
    exchange(&a, &d);
    exchange(&a, &d);
    activate(&mut a);
    // A is restored from the backup; D commits 3 more events.
    drop(a);
    std::fs::remove_dir_all(&a_root).unwrap();
    copy_dir(&snapshot, &a_root).unwrap();
    let mut a = reopen(&a_root, &repo);
    linear_history_named(&mut d, "late", 3).unwrap();
    assert_eq!(shared(&d).len(), 8);
    // Only D dials from now on.
    let mut statuses = vec![];
    for _ in 0..20 {
        let round = exchange(&d, &a);
        statuses.push(round.reply.page_status.clone());
        activate(&mut a);
        activate(&mut d);
    }
    assert!(
        shared(&d).is_subset(&shared(&a)),
        "A received {} of D's 8 events; page statuses: {statuses:?}",
        shared(&d).intersection(&shared(&a)).count()
    );
    let counters = a.peer_feed_counters(&d.device).unwrap();
    assert!(counters.caught_up, "{counters:?}");
    assert_eq!(peer_known(&d, &a), shared(&d));
}

/// Post-integration re-review (WP2.INT): an event whose change parent is an
/// id no replica can hold (over 128 bytes) is refused at staging
/// (`INVALID_EVENT` in `feed_rejected`), so it never waits and its parent
/// never goes into a need list; a waiting row staged with such a parent by
/// another path (the legacy one) does not put it there either. Before the
/// fix the dialer staged it waiting and sent the malformed id in every
/// request to every peer, which every acceptor refused whole
/// (`PROTOCOL_VIOLATION: malformed need id`): no metadata moved again.
#[test]
fn a_malformed_parent_never_reaches_a_need_list() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut d = create(&dir.path().join("d"), &repo);
    let mut a = create(&dir.path().join("a"), &repo);
    let m = create(&dir.path().join("m"), &repo);
    settle(&mut d, &mut a);
    let malformed = |s: &Store| {
        let mut event = mkdir_event(s, "orphan");
        event.changes[0].parents = vec!["p".repeat(200)];
        event
    };
    let bad = malformed(&m);
    inject(&m, &bad);
    let round = exchange(&d, &m);
    assert_eq!(round.applied.summary.staged, 0);
    assert_eq!(
        rejected_codes(&d, &m),
        vec![(bad.id.clone(), "INVALID_EVENT".to_owned())]
    );
    assert_eq!(d.inbox_counts().unwrap().waiting, 0);
    // The same shape staged by another path waits, but is never asked for.
    let legacy = malformed(&m);
    let allowed: BTreeSet<String> = [m.device.clone(), d.device.clone()].into();
    d.stage_incoming(
        vec![tkfs::core::inbox::StagedEvent {
            event: legacy,
            source: None,
        }],
        &allowed,
    )
    .unwrap();
    activate(&mut d);
    assert_eq!(d.inbox_counts().unwrap().waiting, 1);
    // D's exchanges with an honest peer keep working.
    a.create("a2", "directory").unwrap();
    for _ in 0..3 {
        let request = d.build_exchange_request(&route(&d, &a)).unwrap();
        assert!(
            request
                .need
                .iter()
                .all(|id| !id.is_empty() && id.len() <= 128),
            "{:?}",
            request.need
        );
        let (reply, _) = a
            .handle_exchange_request(&route(&a, &d), &request)
            .expect("the acceptor refuses no request");
        d.apply_exchange_reply(&route(&d, &a), &request.request, &reply)
            .unwrap();
        activate(&mut d);
        activate(&mut a);
    }
    assert!(d.lookup("a2").is_ok(), "A's new event reached D");
    // And with M itself.
    let request = d.build_exchange_request(&route(&d, &m)).unwrap();
    assert!(request.need.iter().all(|id| id.len() <= 128));
    m.handle_exchange_request(&route(&m, &d), &request)
        .expect("M refuses no request either");
}

/// An event of about `bytes` bytes (one padded change), not validated.
fn padded_event(s: &Store, bytes: usize) -> Event {
    s.event(
        &main_branch(s),
        vec![Change {
            entity: id(),
            field: "basic".into(),
            parents: vec![],
            value: json!({ "pad": "x".repeat(bytes) }),
        }],
    )
}

/// Post-integration re-review (WP2.INT): need serving reads a payload only
/// when it fits the reply's budget (its stored length is read first). Before
/// the fix every requested payload was read, served or not, so draining N
/// large needs read about N²/2 payloads.
#[test]
fn need_serving_reads_only_the_payloads_it_serves() {
    let dir = tempfile::tempdir().unwrap();
    let a = create(&dir.path().join("a"), &id());
    let ids: Vec<String> = (0..24)
        .map(|_| {
            let event = padded_event(&a, 774_000);
            inject(&a, &event);
            event.id
        })
        .collect();
    let mut left = ids.clone();
    let mut rounds = 0;
    let before = a.feed_payload_reads();
    while !left.is_empty() {
        let reads = a.feed_payload_reads();
        let (served, unknown) = a.events_by_ids(&left, MAX_NEED_BYTES).unwrap();
        assert!(unknown.is_empty());
        assert!(!served.is_empty());
        assert_eq!(
            (a.feed_payload_reads() - reads) as usize,
            served.len(),
            "payloads read beyond those served"
        );
        let served: BTreeSet<String> = served.into_iter().map(|e| e.id).collect();
        left.retain(|id| !served.contains(id));
        rounds += 1;
    }
    assert_eq!(rounds, 2, "16 fit the 12 MiB budget, then 8");
    assert_eq!((a.feed_payload_reads() - before) as usize, ids.len());
}

/// Post-integration re-review (WP2.INT): a page reads at most
/// `PAGE_READ_BUDGET` of stored payloads for its entries, `large` and elided
/// ones included, and ends there (`more`; the next page continues
/// contiguously). Before the fix a page listed up to 4,096 such entries and
/// read every payload (738 MiB for 1,000 large events in the review), past
/// the exchange deadline.
#[test]
fn a_page_reads_its_entries_within_a_budget() {
    let dir = tempfile::tempdir().unwrap();
    let a = create(&dir.path().join("a"), &id());
    let size = 774_000;
    for _ in 0..40 {
        inject(&a, &padded_event(&a, size));
    }
    let per_page = tkfs::core::sharing::PAGE_READ_BUDGET as usize / size + 1;
    let mut after = Cursor::start(&a.feed_epoch().unwrap());
    let mut listed = vec![];
    let mut pages = 0;
    loop {
        let reads = a.feed_payload_reads();
        let page = a
            .feed_page(Some(&after), "some-peer", MAX_PAGE_ENTRIES, MAX_PAGE_BYTES)
            .unwrap();
        assert!(!page.entries.is_empty());
        assert!(
            page.entries.len() <= per_page,
            "{} entries",
            page.entries.len()
        );
        assert_eq!(
            (a.feed_payload_reads() - reads) as usize,
            page.entries.len()
        );
        assert!(
            page.entries
                .iter()
                .all(|e| e.event.is_none() && e.large.is_some())
        );
        assert_eq!(page.through.seq, page.entries.last().unwrap().seq);
        listed.extend(page.entries.iter().map(|e| e.seq));
        after = page.through.clone();
        pages += 1;
        if !page.more {
            break;
        }
    }
    assert_eq!(listed.len(), 40);
    assert!(listed.windows(2).all(|w| w[0] < w[1]));
    assert!(pages > 1);
}

#[test]
fn evacuation_resets_once() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let b_root = dir.path().join("b");
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&b_root, &repo);
    linear_history(&mut a, 10).unwrap();
    b.create("from-b", "directory").unwrap();
    settle(&mut b, &mut a);
    assert_converged(&[&a, &b]);
    let epoch = b.feed_epoch().unwrap();
    // An older build left an unvalidated event in B's journal; the next open
    // evacuates it (deletes the row), which makes B's feed a new one.
    let entity = b.lookup("from-b").unwrap().id;
    let malformed = b.event(
        &main_branch(&b),
        vec![Change {
            entity,
            field: "alive".into(),
            parents: vec!["future-parent".into()],
            value: json!(true),
        }],
    );
    inject(&b, &malformed);
    drop(b);
    let mut b = reopen(&b_root, &repo);
    assert!(b.events().unwrap().iter().all(|e| e.id != malformed.id));
    assert_ne!(b.feed_epoch().unwrap(), epoch);
    let mut resets = vec![];
    for _ in 0..5 {
        let round = exchange(&b, &a);
        resets.push((
            round.request.page.reset,
            round.reply.page.reset,
            round.reply.page_status.clone(),
        ));
        activate(&mut a);
        activate(&mut b);
    }
    let count = resets.iter().filter(|(r, s, _)| *r || *s).count();
    assert_eq!(count, 1, "{resets:?}");
    assert!(resets[0].0, "B's own pages restart its new feed");
    assert_eq!(resets[0].2, PageStatus::Reset);
    assert!(
        resets[1..]
            .iter()
            .all(|(_, _, s)| *s == PageStatus::Accepted)
    );
    assert_eq!(shared(&a), shared(&b));
    assert_eq!(outgoing(&a), 0);
    assert_eq!(outgoing(&b), 0);
    assert_eq!(peer_known(&a, &b), shared(&a));
}

#[test]
fn cursor_mismatch_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    linear_history(&mut a, 10).unwrap();
    settle(&mut b, &mut a);
    // B's next page reaches A, but A's reply is lost (never applied).
    linear_history_named(&mut b, "b", 5).unwrap();
    linear_history_named(&mut a, "a", 5).unwrap();
    let lost = b.build_exchange_request(&route(&b, &a)).unwrap();
    assert_eq!(lost.page.entries.len(), 5);
    let (lost_reply, _) = a.handle_exchange_request(&route(&a, &b), &lost).unwrap();
    assert_eq!(lost_reply.page_status, PageStatus::Accepted);
    // B pages again from where it last knew A was: A expects more.
    let round = exchange(&b, &a);
    let PageStatus::CursorMismatch { expected } = &round.reply.page_status else {
        panic!("expected a cursor mismatch: {:?}", round.reply.page_status);
    };
    assert_eq!(expected, &lost.page.through);
    assert_eq!(round.local.summary.staged, 0);
    // B adopts A's cursor (it is valid in B's feed, and ahead: no regression)
    // and receives A's page, which A built from B's own cursor.
    assert_eq!(round.applied.summary.cursor_out, lost.page.through.seq);
    assert_eq!(round.reply_payloads(), 5);
    assert!(!round.request.page.reset && !round.reply.page.reset);
    activate(&mut a);
    activate(&mut b);
    let round = exchange(&b, &a);
    assert_eq!(round.reply.page_status, PageStatus::Accepted);
    settle(&mut b, &mut a);
    assert_converged(&[&a, &b]);
}

// ---------------------------------------------------------------------------
// Gate view (§6.3.6)

#[test]
fn gate_waits_for_activation_rejection_and_the_remote_pending() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    linear_history(&mut a, 10).unwrap();
    assert!(
        !b.feed_gate(&a.device).unwrap().caught_up,
        "no exchange yet"
    );
    exchange(&b, &a);
    exchange(&b, &a);
    // B's cursor is at A's tip, but nothing activated yet.
    let gate = b.feed_gate(&a.device).unwrap();
    assert!(!gate.caught_up && !gate.blocked);
    assert!(
        gate.waiting_on
            .iter()
            .any(|w| w.contains("not yet activated"))
    );
    // A's view of B as a source: B still has 10 unactivated events.
    let round = exchange(&b, &a);
    assert!(!round.local.gate.caught_up);
    assert!(
        round.local.summary.caught_up,
        "A's own metadata is caught up"
    );
    assert_eq!(round.local.summary.remote_pending, 10);
    assert!(
        round
            .local
            .gate
            .waiting_on
            .contains(&"source has 10 unactivated events".to_owned())
    );
    activate(&mut b);
    exchange(&b, &a);
    let round = exchange(&b, &a);
    assert!(round.applied.gate.caught_up && round.local.gate.caught_up);
    assert!(round.applied.gate.waiting_on.is_empty());
    // A rejected event keeps the gate closed (and blocked).
    let mut forged = mkdir_event(&a, "forged");
    forged.device = b.device.clone();
    inject(&a, &forged);
    let round = exchange(&b, &a);
    assert!(
        round.applied.summary.caught_up,
        "metadata itself is at the tip"
    );
    assert!(!round.applied.gate.caught_up && round.applied.gate.blocked);
}

// ---------------------------------------------------------------------------
// Binding and bounded input (invariants 10, 15, 22)

#[test]
fn replies_bind_to_their_request_and_routes_to_their_replicas() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let a = create(&dir.path().join("a"), &repo);
    let b = create(&dir.path().join("b"), &repo);
    let c = create(&dir.path().join("c"), &id());
    let request = b.build_exchange_request(&route(&b, &a)).unwrap();
    let (reply, _) = a.handle_exchange_request(&route(&a, &b), &request).unwrap();
    let code = |e: anyhow::Error| code_of(&e);
    let other = id();
    assert_eq!(
        code(
            b.apply_exchange_reply(&route(&b, &a), &other, &reply)
                .unwrap_err()
        ),
        "UNBOUND_PEER_RESPONSE"
    );
    let mut foreign = route(&b, &a);
    foreign.repo = c.repo.clone();
    assert_eq!(
        code(b.build_exchange_request(&foreign).unwrap_err()),
        "REPOSITORY_MISMATCH"
    );
    assert_eq!(
        code(b.build_exchange_request(&route(&a, &b)).unwrap_err()),
        "LOCAL_REPLICA_BINDING_MISMATCH"
    );
    let mut itself = route(&b, &a);
    itself.remote_replica = b.device.clone();
    assert_eq!(
        code(b.build_exchange_request(&itself).unwrap_err()),
        "DUPLICATE_REPLICA_IDENTITY"
    );
    // The request names its sender: a page relayed for another route is refused.
    let mut misrouted = request.clone();
    misrouted.sender = c.device.clone();
    assert_eq!(
        code(
            a.handle_exchange_request(&route(&a, &b), &misrouted)
                .unwrap_err()
        ),
        "REMOTE_REPLICA_BINDING_MISMATCH"
    );
    let mut old = request.clone();
    old.format = 6;
    assert_eq!(
        code(a.handle_exchange_request(&route(&a, &b), &old).unwrap_err()),
        "INCOMPATIBLE_PEER_PROTOCOL"
    );
    let mut misbound = reply.clone();
    misbound.sender = c.device.clone();
    assert_eq!(
        code(
            b.apply_exchange_reply(&route(&b, &a), &request.request, &misbound)
                .unwrap_err()
        ),
        "REMOTE_REPLICA_BINDING_MISMATCH"
    );
    // The genuine reply still applies.
    b.apply_exchange_reply(&route(&b, &a), &request.request, &reply)
        .unwrap();
}

#[test]
fn oversized_or_malformed_messages_are_refused_whole() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let b = create(&dir.path().join("b"), &repo);
    linear_history(&mut a, 3).unwrap();
    let code = |e: anyhow::Error| code_of(&e);
    let fresh = || b.build_exchange_request(&route(&b, &a)).unwrap();
    let handle = |request: &MetaExchangeRequest| {
        a.handle_exchange_request(&route(&a, &b), request)
            .map(|_| ())
    };
    let mut need = fresh();
    need.need = (0..=MAX_NEED).map(|_| id()).collect();
    assert_eq!(code(handle(&need).unwrap_err()), "INVENTORY_TOO_LARGE");
    // A reply page with too many entries, and one out of order.
    let request = fresh();
    let (reply, _) = a.handle_exchange_request(&route(&a, &b), &request).unwrap();
    let apply = |reply: &MetaExchangeReply| {
        b.apply_exchange_reply(&route(&b, &a), &request.request, reply)
            .map(|_| ())
    };
    let mut flood = reply.clone();
    let entry = flood.page.entries[0].clone();
    flood.page.entries = vec![entry; MAX_PAGE_ENTRIES + 1];
    assert_eq!(code(apply(&flood).unwrap_err()), "INVENTORY_TOO_LARGE");
    let mut shuffled = reply.clone();
    shuffled.page.entries.reverse();
    assert_eq!(code(apply(&shuffled).unwrap_err()), "PROTOCOL_VIOLATION");
    let mut beyond = reply.clone();
    beyond.page.through.seq = beyond.page.tip + 1;
    assert_eq!(code(apply(&beyond).unwrap_err()), "PROTOCOL_VIOLATION");
    // Nothing of a refused message was kept.
    assert_eq!(b.inbox_counts().unwrap().total(), 0);
    assert!(
        b.peer_feed_counters(&a.device)
            .unwrap()
            .last_exchange_ms
            .is_none()
    );
    apply(&reply).unwrap();
    assert_eq!(b.inbox_counts().unwrap().total(), 3);
}

#[test]
fn peer_supplied_values_are_bounded_before_they_are_kept() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    let c = create(&dir.path().join("c"), &repo);
    linear_history(&mut a, 3).unwrap();
    settle(&mut b, &mut a);
    let code = |e: anyhow::Error| code_of(&e);
    let handle = |from: &Store, request: &MetaExchangeRequest| {
        a.handle_exchange_request(&route(&a, from), request)
            .map(|_| ())
    };
    let huge = "z".repeat(1 << 20);
    // A probe with a negative tip (stored, then subtracted from our cursor).
    let mut probe = b.build_exchange_request(&route(&b, &a)).unwrap();
    probe.page.after = None;
    probe.page.entries.clear();
    probe.page.reset = false;
    probe.page.tip = i64::MIN;
    assert_eq!(code(handle(&b, &probe).unwrap_err()), "PROTOCOL_VIOLATION");
    let counters = a.peer_feed_counters(&b.device).unwrap();
    assert!(counters.cursor_seq > 0 && counters.remote_tip >= 0);
    // A first contact naming an epoch that is not one (it would be stored).
    let mut first = c.build_exchange_request(&route(&c, &a)).unwrap();
    assert!(first.page.after.is_none());
    first.page.epoch = huge.clone();
    first.page.through.epoch = huge.clone();
    assert_eq!(code(handle(&c, &first).unwrap_err()), "PROTOCOL_VIOLATION");
    assert!(a.feed_peers().unwrap().iter().all(|peer| *peer != c.device));
    // Oversized cursor ids, in a request (a seq-0 cursor is valid whatever
    // its id) and in a reply.
    let mut request = b.build_exchange_request(&route(&b, &a)).unwrap();
    let mut cursor = Cursor::start(&a.feed_epoch().unwrap());
    cursor.id = huge.clone();
    request.cursor = Some(cursor);
    assert_eq!(
        code(handle(&b, &request).unwrap_err()),
        "PROTOCOL_VIOLATION"
    );
    let request = b.build_exchange_request(&route(&b, &a)).unwrap();
    let (reply, _) = a.handle_exchange_request(&route(&a, &b), &request).unwrap();
    let apply = |reply: &MetaExchangeReply| {
        b.apply_exchange_reply(&route(&b, &a), &request.request, reply)
            .map(|_| ())
    };
    let mut long_id = reply.clone();
    long_id.cursor.id = huge.clone();
    assert_eq!(code(apply(&long_id).unwrap_err()), "PROTOCOL_VIOLATION");
    let mut long_epoch = reply.clone();
    long_epoch.page_status = PageStatus::CursorMismatch {
        expected: Cursor {
            epoch: huge.clone(),
            seq: 1,
            id: "x".into(),
        },
    };
    assert_eq!(code(apply(&long_epoch).unwrap_err()), "PROTOCOL_VIOLATION");
    // `unknown` ids we never asked for are not recorded.
    let mut unasked = reply.clone();
    unasked.unknown = (0..MAX_NEED).map(|_| id()).collect();
    apply(&unasked).unwrap();
    assert!(needs_of(&b, &a).is_empty());
    // A missing parent of a waiting row is; an expired mark is dropped.
    let mut orphan = mkdir_event(&a, "orphan");
    orphan.dependencies.push("phantom-parent".into());
    inject(&a, &orphan);
    exchange(&b, &a);
    b.db.execute(
        "INSERT INTO feed_needs(peer,event,reason,requested_ms,missing_at) VALUES(?,'expired','parent',0,0)",
        [&a.device],
    )
    .unwrap();
    let round = exchange_with(&b, &a, |reply| {
        reply.unknown.extend((0..100).map(|_| id()));
    });
    assert_eq!(round.request.need, vec!["phantom-parent".to_owned()]);
    assert_eq!(
        needs_of(&b, &a),
        vec![("phantom-parent".to_owned(), "parent".to_owned(), true)]
    );
}

// ---------------------------------------------------------------------------
// The worker RPC seam (`runtime::sync_rpc`)

fn worker(root: &Path, repo: &str) -> WorkerCtx {
    let store = create(root, repo);
    let (device, repo) = (store.device.clone(), store.repo.clone());
    let engine: Shared = Arc::new(Mutex::new(Engine::new(store)));
    let (locks, stopping) = {
        let e = engine.lock().unwrap();
        (e.transfers.clone(), e.stopping.clone())
    };
    WorkerCtx {
        mount: MountController::new(engine.clone(), None),
        engine,
        root: root.to_owned(),
        state: root.to_owned(),
        repo: repo.clone(),
        device: device.clone(),
        instance: id(),
        started_at_ms: 1,
        supervised: false,
        version: "test".into(),
        previous: None,
        locks,
        activation: Arc::new(Notify::new()),
        activator_running: AtomicBool::new(false),
        status_generation: AtomicU64::new(0),
        heartbeat: Snapshot::new(WorkerHeartbeat::default()),
        status_cell: Snapshot::new(Value::Null),
        sync_obs: Mutex::new(Default::default()),
        sync_stores: SyncStorePool::new(root, repo, device),
        inflight_objects: Mutex::new(Default::default()),
        stopping,
        last_contact_ms: AtomicU64::new(0),
        gauges: EngineGaugeCell::new(),
    }
}

fn rpc(ctx: &WorkerCtx, payload: Value) -> anyhow::Result<Value> {
    let op = payload["op"].as_str().unwrap().to_owned();
    sync_rpc::dispatch(ctx, &op, &payload)
}

fn route_json(from: &WorkerCtx, to: &WorkerCtx) -> Value {
    json!({"peer": format!("installation-of-{}", to.device), "repo": from.repo,
        "local_replica": from.device, "remote_replica": to.device})
}

/// One `feed-offer` → `feed-exchange` → `feed-apply` round, as an
/// orchestrator drives it. Returns `(request, exchange answer, apply result)`.
fn rpc_round(d: &WorkerCtx, a: &WorkerCtx) -> (Value, Value, Value) {
    let request = rpc(d, json!({"op":"feed-offer","route":route_json(d, a)})).unwrap();
    let exchanged = rpc(
        a,
        json!({"op":"feed-exchange","route":route_json(a, d),"request":request}),
    )
    .unwrap();
    let applied = rpc(
        d,
        json!({"op":"feed-apply","route":route_json(d, a),"request_id":request["request"],
            "reply":exchanged["reply"]}),
    )
    .unwrap();
    (request, exchanged, applied)
}

fn names(ctx: &WorkerCtx) -> BTreeSet<String> {
    let engine = ctx.engine.lock().unwrap();
    engine
        .store
        .projection(&engine.store.active)
        .unwrap()
        .entries
        .values()
        .filter(|e| e.alive)
        .map(|e| e.name.clone())
        .collect()
}

/// WP4.INT (CCR-WP4.2-5): a feed exchange that moves nothing (no event
/// sent, received, staged, rejected or acknowledged, the peer's counters
/// unchanged) does not move `status_generation`, so an idle replica's
/// exchanges every sync interval do not move `generations.sync` and so do
/// not trigger a download round on every bulk lane. The counters keep their
/// time stamp; an exchange that moves something still bumps both sides.
#[test]
fn an_idle_feed_exchange_moves_no_generation() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let a = worker(&dir.path().join("a"), &repo);
    let b = worker(&dir.path().join("b"), &repo);
    a.engine
        .lock()
        .unwrap()
        .store
        .create("one", "directory")
        .unwrap();
    for _ in 0..6 {
        rpc_round(&b, &a);
    }
    assert!(names(&b).contains("one"));
    let generations = |w: &WorkerCtx| w.status_generation.load(Ordering::SeqCst);
    let (gb, ga) = (generations(&b), generations(&a));
    let stamped = b.sync_obs.lock().unwrap()[&a.device].last_exchange_ms;
    std::thread::sleep(Duration::from_millis(5));
    for _ in 0..3 {
        rpc_round(&b, &a);
    }
    assert_eq!(
        (generations(&b), generations(&a)),
        (gb, ga),
        "idle exchanges"
    );
    assert!(b.sync_obs.lock().unwrap()[&a.device].last_exchange_ms > stamped);
    a.engine
        .lock()
        .unwrap()
        .store
        .create("two", "directory")
        .unwrap();
    rpc_round(&b, &a);
    assert!(generations(&b) > gb, "the dialer received an event");
    assert!(generations(&a) > ga, "the acceptor sent one");
}

#[test]
fn feed_ops_run_through_the_worker_rpc_seam() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let a = worker(&dir.path().join("a"), &repo);
    let b = worker(&dir.path().join("b"), &repo);
    for name in ["one", "two", "three"] {
        a.engine
            .lock()
            .unwrap()
            .store
            .create(name, "directory")
            .unwrap();
    }
    // First round: B probes A's cursor of its feed, A pages from the start.
    let (request, exchanged, applied) = rpc_round(&b, &a);
    assert!(request["page"]["after"].is_null());
    let entries = exchanged["reply"]["page"]["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 3);
    assert!(
        entries
            .iter()
            .all(|e| e["event"].is_object() && e.get("large").is_none())
    );
    assert_eq!(exchanged["local"]["summary"]["sent_events"], 3);
    assert_eq!(applied["summary"]["staged"], 3);
    // No activator runs here: the exchange activated what it staged, in its
    // stand-in pass. That pass is bounded (`sync_rpc::STAND_IN_BUDGET`,
    // 100 ms), so on a loaded machine it may end before the batch does
    // (CCR-WP4.1-11, applied by WP4.INT): a non-empty part of it inline, the
    // rest as the next pass would.
    let expected = BTreeSet::from(["one", "two", "three"].map(String::from));
    let inline = names(&b);
    assert!(
        !inline.is_empty() && inline.is_subset(&expected),
        "inline: {inline:?}"
    );
    {
        let mut engine = b.engine.lock().unwrap();
        while engine
            .store
            .activate_slice(SliceBudget::UNBOUNDED)
            .unwrap()
            .more
        {}
    }
    assert_eq!(names(&b), expected);
    assert!(b.activation.count() > 0 && a.activation.count() > 0);
    assert!(b.status_generation.load(Ordering::SeqCst) > 0);
    let counters = b.sync_obs.lock().unwrap()[&a.device].clone();
    assert_eq!(counters.replica, a.device);
    assert!(counters.last_exchange_ms.is_some());
    // The echo: B's page lists A's events back, elided, without null members.
    let (request, _, _) = rpc_round(&b, &a);
    let echo = request["page"]["entries"].as_array().unwrap();
    assert_eq!(echo.len(), 3);
    assert!(
        echo.iter().all(|e| e.as_object().unwrap().len() == 3),
        "{echo:?}"
    );
    let (_, _, applied) = rpc_round(&b, &a);
    assert_eq!(applied["summary"]["caught_up"], true);
    assert_eq!(applied["gate"]["caught_up"], true);
    assert!(a.sync_obs.lock().unwrap()[&b.device].caught_up);
    // With a background activator running, a feed RPC only stages.
    b.activator_running.store(true, Ordering::SeqCst);
    a.engine
        .lock()
        .unwrap()
        .store
        .create("four", "directory")
        .unwrap();
    rpc_round(&b, &a);
    assert!(!names(&b).contains("four"));
    assert_eq!(
        b.engine
            .lock()
            .unwrap()
            .store
            .inbox_counts()
            .unwrap()
            .total(),
        1
    );
}

#[test]
fn feed_rpc_errors() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let a = worker(&dir.path().join("a"), &repo);
    let b = worker(&dir.path().join("b"), &repo);
    let error = |payload: Value| format!("{:#}", rpc(&a, payload).unwrap_err());
    assert!(error(json!({"op":"feed-unknown"})).starts_with("UNSUPPORTED_OPERATION"));
    // A malformed request (CCR-WP2.5-1, applied by WP2.INT: it answered the
    // Wave 0 stub's `UNSUPPORTED_OPERATION` before).
    assert!(error(json!({"op":"feed-offer","route":{}})).starts_with("INVALID_REQUEST"));
    let mut foreign = route_json(&a, &b);
    foreign["repo"] = json!(id());
    assert!(error(json!({"op":"feed-offer","route":foreign})).starts_with("REPOSITORY_MISMATCH"));
    let backwards = route_json(&b, &a);
    assert!(
        error(json!({"op":"feed-offer","route":backwards}))
            .starts_with("LOCAL_REPLICA_BINDING_MISMATCH")
    );
    let request = rpc(&b, json!({"op":"feed-offer","route":route_json(&b, &a)})).unwrap();
    let exchanged = rpc(
        &a,
        json!({"op":"feed-exchange","route":route_json(&a, &b),"request":request}),
    )
    .unwrap();
    let unbound = rpc(
        &b,
        json!({"op":"feed-apply","route":route_json(&b, &a),"request_id":id(),
            "reply":exchanged["reply"]}),
    )
    .unwrap_err();
    assert_eq!(code_of(&unbound), "UNBOUND_PEER_RESPONSE");
    // Only that route-less `feed-offer` keeps the Wave 0 answer: a peer's
    // malformed message is a protocol violation, and the orchestrator's own
    // malformed members are an invalid request (fix round 2).
    let code = |ctx: &WorkerCtx, payload: Value| code_of(&rpc(ctx, payload).unwrap_err());
    let mut bad_request = request.clone();
    bad_request["page"] = json!("not a page");
    assert_eq!(
        code(
            &a,
            json!({"op":"feed-exchange","route":route_json(&a, &b),"request":bad_request})
        ),
        "PROTOCOL_VIOLATION"
    );
    assert_eq!(
        code(
            &a,
            json!({"op":"feed-exchange","route":route_json(&a, &b),"request":{"format":7}})
        ),
        "PROTOCOL_VIOLATION"
    );
    assert_eq!(
        code(
            &a,
            json!({"op":"feed-exchange","route":{},"request":request})
        ),
        "INVALID_REQUEST"
    );
    assert_eq!(
        code(&a, json!({"op":"feed-exchange","route":route_json(&a, &b)})),
        "INVALID_REQUEST"
    );
    let mut bad_reply = exchanged["reply"].clone();
    bad_reply["page_status"] = json!("Sideways");
    assert_eq!(
        code(
            &b,
            json!({"op":"feed-apply","route":route_json(&b, &a),"request_id":request["request"],
                "reply":bad_reply})
        ),
        "PROTOCOL_VIOLATION"
    );
    assert_eq!(
        code(
            &b,
            json!({"op":"feed-apply","route":route_json(&b, &a),"reply":exchanged["reply"]})
        ),
        "INVALID_REQUEST"
    );
    assert_eq!(
        code(
            &b,
            json!({"op":"feed-apply","route":{"repo":repo},"request_id":request["request"],
                "reply":bad_reply})
        ),
        "INVALID_REQUEST"
    );
}

#[test]
fn mutual_dialing_converges_without_false_regressions() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    linear_history_named(&mut a, "a", 12).unwrap();
    settle(&mut b, &mut a);
    settle(&mut a, &mut b);
    assert_converged(&[&a, &b]);
    // B reads its cursor of A's feed for a request; before that request
    // reaches A, A dials B and B takes A's newer page. The request then
    // reports a lower cursor than B's reply just did: stale, not a restore.
    linear_history_named(&mut a, "later", 5).unwrap();
    let stale = b.build_exchange_request(&route(&b, &a)).unwrap();
    let ahead = exchange(&a, &b);
    assert_eq!(ahead.request_payloads(), 5);
    assert!(ahead.reply.cursor.seq > stale.cursor.as_ref().unwrap().seq);
    let known = peer_known(&a, &b);
    let unacked = outgoing(&a);
    let (reply, _) = a.handle_exchange_request(&route(&a, &b), &stale).unwrap();
    assert_eq!(peer_known(&a, &b), known, "a stale report is no regression");
    assert_eq!(outgoing(&a), unacked);
    assert!(a.peer_feed_counters(&b.device).unwrap().cursor_seq > 0);
    b.apply_exchange_reply(&route(&b, &a), &stale.request, &reply)
        .unwrap();
    // Both keep dialing each other until nothing moves.
    linear_history_named(&mut b, "b", 7).unwrap();
    for _ in 0..10 {
        exchange(&b, &a);
        exchange(&a, &b);
        activate(&mut a);
        activate(&mut b);
    }
    assert_converged(&[&a, &b]);
    assert_eq!(shared(&a).len(), 24);
}

#[test]
fn the_dialer_summary_counts_what_its_request_sent() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    linear_history_named(&mut b, "b", 4).unwrap();
    // The first request is a probe; the second carries B's four payloads.
    let probe = exchange(&b, &a);
    assert_eq!(probe.applied.summary.sent_events, 0);
    let round = exchange(&b, &a);
    assert_eq!(round.request_payloads(), 4);
    assert_eq!(round.local.summary.received_events, 4);
    let sent = &round.applied.summary;
    assert_eq!(sent.sent_events, 4);
    let payload: usize = round
        .request
        .page
        .entries
        .iter()
        .filter_map(|e| e.event.as_ref())
        .map(|e| serde_json::to_string(e).unwrap().len())
        .sum();
    assert!(sent.bytes_up > payload as u64, "{} bytes up", sent.bytes_up);
    assert!(sent.metadata_advanced);
    // Idle rounds send nothing.
    activate(&mut a);
    settle(&mut b, &mut a);
    let idle = exchange(&b, &a);
    assert_eq!(idle.applied.summary.sent_events, 0);
    assert!(!idle.applied.summary.metadata_advanced);
    // A reply to a request this process did not build (a restarted worker)
    // counts nothing sent.
    linear_history_named(&mut b, "late", 2).unwrap();
    let mut request = b.build_exchange_request(&route(&b, &a)).unwrap();
    assert_eq!(
        request
            .page
            .entries
            .iter()
            .filter(|e| e.event.is_some())
            .count(),
        2
    );
    request.request = id();
    let (reply, _) = a.handle_exchange_request(&route(&a, &b), &request).unwrap();
    let summary = b
        .apply_exchange_reply(&route(&b, &a), &request.request, &reply)
        .unwrap()
        .summary;
    assert_eq!(summary.sent_events, 0);
    // Through the worker RPC (offer and apply on pooled stores).
    let a = worker(&dir.path().join("wa"), &repo);
    let b = worker(&dir.path().join("wb"), &repo);
    for name in ["x", "y"] {
        b.engine
            .lock()
            .unwrap()
            .store
            .create(name, "directory")
            .unwrap();
    }
    rpc_round(&b, &a);
    let (request, _, applied) = rpc_round(&b, &a);
    assert_eq!(request["page"]["entries"].as_array().unwrap().len(), 2);
    assert_eq!(applied["summary"]["sent_events"], 2);
    assert!(applied["summary"]["bytes_up"].as_u64().unwrap() > 0);
}
