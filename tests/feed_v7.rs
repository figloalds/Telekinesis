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
            Cursor, FeedApplyResult, LARGE_EVENT_BYTES, MAX_NEED, MAX_PAGE_BYTES, MAX_PAGE_ENTRIES,
            MetaExchangeReply, MetaExchangeRequest, PageStatus,
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
    let stamp: String =
        a.db.query_row(
            "SELECT value FROM config WHERE key='schema:sharing'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stamp, "1");

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

#[test]
fn a_dialer_large_event_travels_inline() {
    // The acceptor cannot ask the dialer for anything (a reply has no need):
    // a request page carries a large event inline, as its last payload.
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let mut a = create(&dir.path().join("a"), &repo);
    let mut b = create(&dir.path().join("b"), &repo);
    linear_history_named(&mut b, "small", 3).unwrap();
    let big = big_event(&b, "+", 400);
    let size = serde_json::to_string(&big).unwrap().len();
    assert!(size > LARGE_EVENT_BYTES);
    b.commit(big.clone(), true).unwrap();
    linear_history_named(&mut b, "after", 3).unwrap();
    let mut inline = 0;
    for _ in 0..6 {
        let round = exchange(&b, &a);
        let page = &round.request.page;
        if let Some(position) = page.entries.iter().position(|e| e.id == big.id) {
            assert_eq!(page.entries[position].event.as_ref(), Some(&big));
            assert_eq!(position, page.entries.len() - 1, "the page stops after it");
            inline += 1;
        }
        activate(&mut a);
        activate(&mut b);
    }
    assert_eq!(inline, 1);
    assert_converged(&[&a, &b]);
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
    // No activator runs here: the exchange activated what it staged.
    assert_eq!(
        names(&b),
        BTreeSet::from(["one", "two", "three"].map(String::from))
    );
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
    // The Wave 0 answer to a malformed request is kept (CCR-WP2.5-1).
    assert!(error(json!({"op":"feed-offer","route":{}})).starts_with("UNSUPPORTED_OPERATION"));
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
