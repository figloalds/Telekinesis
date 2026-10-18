//! WP5.4 (CCR-WP4.INT-1): the worker `status` reply's top-level
//! `caught_up` and `metadata_caught_up` follow the v3 metadata feed's
//! observations (`ctx.sync_obs`, the reply's `feed_peers`), with a
//! freshness rule measured against each peer's exchange cadence
//! (`worker::status_cell::FeedCadence`). Before WP5.4 they read only the
//! legacy lanes' observations, so on the v3 path they stayed `false` (the
//! two-machine lab read them `false` throughout) and `upload_state` said
//! `peer-not-configured`.
//!
//! The legacy rule (`paired_sync`'s observations) is unchanged; its tests
//! are in `worker_concurrency`.
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use tkfs::{
    core::{Store, id, inbox::SliceBudget},
    model::health::PeerFeedCounters,
    runtime::{EngineVitals, Shared, ctx::WorkerCtx, engine::Engine, sync_rpc},
    util::time::now_ms,
    worker::{
        self,
        status_cell::{
            self, FEED_CADENCE_MAX, FEED_FRESH_MAX, FEED_FRESH_MIN, FeedCadence,
            feed_peers_caught_up,
        },
    },
};

struct Worker {
    ctx: Arc<WorkerCtx>,
    vitals: Arc<EngineVitals>,
}

/// A worker context over a new store (no RPC server, no maintenance
/// thread: [`Worker::status`] refreshes the cell as maintenance would).
fn worker(root: &Path, repo: &str) -> Worker {
    let store = Store::initialize(root, repo, &id()).unwrap();
    let engine: Shared = Arc::new(Mutex::new(Engine::new(store)));
    let vitals = engine.lock().unwrap().vitals.clone();
    let ctx = worker::build_ctx(engine, root, false, None);
    Worker { ctx, vitals }
}

impl Worker {
    fn rpc(&self, payload: Value) -> Value {
        let op = payload["op"].as_str().unwrap().to_owned();
        sync_rpc::dispatch(&self.ctx, &op, &payload).unwrap()
    }

    /// The `status` reply after a maintenance-like full refresh.
    fn status(&self) -> Value {
        status_cell::refresh_full(&self.ctx).unwrap();
        status_cell::reply(&self.ctx, &self.vitals).unwrap()
    }

    /// The `status` reply only (the cell refreshes what the reply needs).
    fn reply(&self) -> Value {
        status_cell::reply(&self.ctx, &self.vitals).unwrap()
    }

    fn activate(&self) {
        let mut engine = self.ctx.engine.lock().unwrap();
        while engine
            .store
            .activate_slice(SliceBudget::UNBOUNDED)
            .unwrap()
            .more
        {}
    }

    fn create(&self, name: &str) {
        self.ctx
            .engine
            .lock()
            .unwrap()
            .store
            .create(name, "directory")
            .unwrap();
    }

    /// Sets the feed counters of `replica`, as a feed exchange would.
    fn observe(&self, replica: &str, caught_up: bool, at_ms: u64) {
        self.ctx.sync_obs.lock().unwrap().insert(
            replica.to_owned(),
            PeerFeedCounters {
                replica: replica.to_owned(),
                caught_up,
                last_exchange_ms: Some(at_ms),
                ..PeerFeedCounters::default()
            },
        );
    }
}

fn route(from: &Worker, to: &Worker) -> Value {
    json!({"peer": format!("installation-of-{}", to.ctx.device), "repo": from.ctx.repo,
        "local_replica": from.ctx.device, "remote_replica": to.ctx.device})
}

/// One `feed-offer` → `feed-exchange` → `feed-apply` round, dialer `d`,
/// acceptor `a`, as an orchestrator drives it; then both activate what
/// they staged.
fn round(d: &Worker, a: &Worker) {
    let request = d.rpc(json!({"op":"feed-offer","route":route(d, a)}));
    let exchanged = a.rpc(json!({"op":"feed-exchange","route":route(a, d),"request":request}));
    d.rpc(
        json!({"op":"feed-apply","route":route(d, a),"request_id":request["request"],
        "reply":exchanged["reply"]}),
    );
    d.activate();
    a.activate();
}

/// Two replicas that sync only through the feed reach `caught_up` on both
/// sides (the dialer and the acceptor), with `metadata_caught_up` equal to
/// it and `upload_state` naming the peer; a new local event makes the side
/// that wrote it `queued` and not caught up until the peer acknowledges it.
#[test]
fn feed_peers_make_both_workers_caught_up() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let a = worker(&dir.path().join("a"), &repo);
    let b = worker(&dir.path().join("b"), &repo);
    for name in ["one", "two", "three"] {
        a.create(name);
    }
    // No exchange yet: no peer to be caught up with.
    let before = a.status();
    assert_eq!(before["caught_up"], false, "{before}");
    assert_eq!(before["upload_state"], "peer-not-configured", "{before}");

    let caught_up = |w: &Worker| w.status()["caught_up"] == true;
    let mut rounds = 0;
    while !(caught_up(&a) && caught_up(&b)) {
        rounds += 1;
        assert!(
            rounds <= 10,
            "not caught up after 10 rounds:\nA {}\nB {}",
            a.status(),
            b.status()
        );
        round(&b, &a);
    }
    for (side, w, peer) in [("A", &a, &b), ("B", &b, &a)] {
        let status = w.status();
        assert_eq!(status["caught_up"], true, "{side} {status}");
        assert_eq!(status["metadata_caught_up"], true, "{side} {status}");
        assert_eq!(
            status["upload_state"], "peer-acknowledged",
            "{side} {status}"
        );
        assert_eq!(status["outgoing_unacknowledged"], 0, "{side} {status}");
        let feed = status["feed_peers"].as_array().unwrap();
        assert_eq!(feed.len(), 1, "{side} {status}");
        assert_eq!(feed[0]["replica"], json!(peer.ctx.device), "{side}");
        assert_eq!(feed[0]["caught_up"], true, "{side} {status}");
    }

    // A writes again: queued and behind until B has it and A sees the ack.
    a.create("four");
    let behind = a.status();
    assert_eq!(behind["caught_up"], false, "{behind}");
    assert_eq!(behind["upload_state"], "queued", "{behind}");
    let mut rounds = 0;
    while !(caught_up(&a) && caught_up(&b)) {
        rounds += 1;
        assert!(rounds <= 10, "not caught up again after 10 rounds");
        round(&b, &a);
    }
    assert_eq!(a.status()["upload_state"], "peer-acknowledged");
}

/// WP5.INT (CCR-WP5.4-4): the worker op `folder-details` embeds the same
/// status claims as the `status` op (`caught_up`, `metadata_caught_up`,
/// `upload_state`), so a v3 folder is caught up there too; before, it
/// embedded the engine's own `status`, which counts only the legacy lanes
/// (`false`, `peer-not-configured`).
#[test]
fn folder_details_tells_the_status_claims_of_the_feed() {
    let dir = tempfile::tempdir().unwrap();
    let repo = id();
    let a = worker(&dir.path().join("a"), &repo);
    let b = worker(&dir.path().join("b"), &repo);
    a.create("one");
    let caught_up = |w: &Worker| w.status()["caught_up"] == true;
    let mut rounds = 0;
    while !(caught_up(&a) && caught_up(&b)) {
        rounds += 1;
        assert!(rounds <= 10, "not caught up after 10 rounds");
        round(&b, &a);
    }
    let server = tkfs::runtime::server::start_rpc_server(a.ctx.clone()).unwrap();
    let info = server.publish(None).unwrap();
    let details = tkfs::runtime::rpc(&info, &id(), json!({"op": "folder-details"})).unwrap();
    let status = tkfs::runtime::rpc(&info, &id(), json!({"op": "status"})).unwrap();
    for member in ["caught_up", "metadata_caught_up", "upload_state"] {
        assert_eq!(
            details["status"][member], status[member],
            "{member}: {details}"
        );
    }
    assert_eq!(details["status"]["caught_up"], true, "{details}");
    assert_eq!(
        details["status"]["upload_state"], "peer-acknowledged",
        "{details}"
    );
    assert!(details["conflicts"].is_array(), "{details}");
}

/// The freshness rule on a worker with nothing to do but one feed peer
/// (its counters set as an exchange would set them): the last exchange
/// counts for three of the peer's exchange intervals, at least 15 s and at
/// most 180 s, so a folder synced every 60 s stays caught up between its
/// exchanges, an old observation does not, and a peer whose last exchange
/// said it was behind never is.
#[test]
fn a_feed_observation_counts_for_three_exchange_intervals() {
    let dir = tempfile::tempdir().unwrap();
    let peer = "replica-of-the-peer";
    // One exchange: the interval is not known yet, so 15 s holds.
    let w = worker(&dir.path().join("w0"), &id());
    w.observe(peer, true, now_ms() - 20_000);
    assert_eq!(w.reply()["caught_up"], false, "20 s old, first exchange");
    let w = worker(&dir.path().join("w1"), &id());
    w.observe(peer, true, now_ms());
    assert_eq!(w.reply()["caught_up"], true, "{}", w.reply());

    let now = now_ms();
    let w = worker(&dir.path().join("w2"), &id());
    let caught_up = |at_s: u64| {
        w.observe(peer, true, now - at_s * 1000);
        w.reply()["caught_up"] == true
    };
    // Exchanges every 60 s: each counts for 180 s.
    assert!(!caught_up(400));
    assert!(!caught_up(340));
    assert!(!caught_up(280));
    assert!(!caught_up(220), "220 s old, more than three intervals");
    assert!(caught_up(160), "160 s old, within three intervals of 60 s");
    assert!(caught_up(100));
    // A burst of exchanges (a moved tip) does not shorten the window.
    assert!(caught_up(99));
    assert!(caught_up(98));
    assert!(caught_up(40));
    // The last exchange said the peer was behind: not caught up, fresh or not.
    w.observe(peer, false, now_ms());
    assert_eq!(w.reply()["caught_up"], false);
    w.observe(peer, true, now_ms());
    assert_eq!(w.reply()["caught_up"], true);
    // A second peer must be caught up and fresh too.
    w.observe("another-replica", true, now_ms() - 30_000);
    assert_eq!(w.reply()["caught_up"], false, "30 s, first exchange");
    assert_eq!(w.reply()["upload_state"], "peer-acknowledged");

    // An outage is not a cadence: gaps longer than 90 s are left out.
    let w = worker(&dir.path().join("w3"), &id());
    let caught_up = |at_s: u64| {
        w.observe(peer, true, now - at_s * 1000);
        w.reply()["caught_up"] == true
    };
    assert!(!caught_up(300));
    assert!(!caught_up(100), "a 200 s gap is an outage: 15 s holds");
}

fn counters(caught_up: bool, at_ms: u64) -> PeerFeedCounters {
    PeerFeedCounters {
        replica: "peer".into(),
        caught_up,
        last_exchange_ms: Some(at_ms),
        ..PeerFeedCounters::default()
    }
}

fn cadence_of(stamps: &[u64]) -> FeedCadence {
    let mut cadence = FeedCadence::default();
    for &at in stamps {
        cadence.observe(at);
    }
    cadence
}

/// `FeedCadence` and `feed_peers_caught_up` as pure functions.
#[test]
fn the_cadence_is_the_longest_recent_gap_short_of_an_outage() {
    let s = 1_000u64;
    let t0 = 1_800_000_000_000u64;
    // Unknown: the floor.
    assert_eq!(FeedCadence::default().fresh_for(), FEED_FRESH_MIN);
    assert_eq!(cadence_of(&[t0]).cadence_ms(), 0);
    assert_eq!(cadence_of(&[t0]).fresh_for(), FEED_FRESH_MIN);
    // A 500 ms interval: the 15 s floor.
    let fast: Vec<u64> = (0..200).map(|i| t0 + i * 500).collect();
    assert_eq!(cadence_of(&fast).cadence_ms(), 500);
    assert_eq!(cadence_of(&fast).fresh_for(), FEED_FRESH_MIN);
    // 20 s: 60 s; 60 s: 180 s, the ceiling.
    let every = |gap: u64, n: u64| -> Vec<u64> { (0..n).map(|i| t0 + i * gap).collect() };
    assert_eq!(
        cadence_of(&every(20 * s, 20)).fresh_for(),
        Duration::from_secs(60)
    );
    assert_eq!(cadence_of(&every(60 * s, 20)).fresh_for(), FEED_FRESH_MAX);
    // Repeated and older stamps change nothing.
    let mut cadence = cadence_of(&every(60 * s, 3));
    let kept = cadence.clone();
    assert!(!cadence.observe(t0 + 120 * s));
    assert!(!cadence.observe(t0));
    assert_eq!(cadence, kept);
    // A burst after a 60 s cadence keeps the 60 s.
    let mut stamps = every(60 * s, 5);
    stamps.extend((1..=20).map(|i| t0 + 240 * s + i * 50));
    assert_eq!(cadence_of(&stamps).cadence_ms(), 60 * s);
    // Mutual pairing: two schedules interleaved (10 s and 50 s gaps).
    let mut stamps = vec![];
    for i in 0..10 {
        stamps.push(t0 + i * 60 * s);
        stamps.push(t0 + i * 60 * s + 10 * s);
    }
    assert_eq!(cadence_of(&stamps).cadence_ms(), 50 * s);
    // An outage (a gap above FEED_CADENCE_MAX) is left out...
    let max = u64::try_from(FEED_CADENCE_MAX.as_millis()).unwrap();
    let outage = cadence_of(&[t0, t0 + s, t0 + 2 * s, t0 + 2 * s + max + 1]);
    assert_eq!(outage.cadence_ms(), s);
    // ...and a gap of exactly FEED_CADENCE_MAX counts.
    assert_eq!(cadence_of(&[t0, t0 + max]).cadence_ms(), max);
    // A shorter interval takes over within two windows.
    let mut stamps = every(60 * s, 5);
    let start = *stamps.last().unwrap();
    stamps.extend((1..=400).map(|i| start + i * 500));
    let cadence = cadence_of(&stamps);
    assert_eq!(cadence.cadence_ms(), 500, "{cadence:?}");

    // feed_peers_caught_up: each peer caught up and younger than its window.
    let now = t0 + 1_000 * s;
    let slow: BTreeMap<String, FeedCadence> =
        BTreeMap::from([("peer".to_owned(), cadence_of(&every(60 * s, 5)))]);
    let feed = |c: PeerFeedCounters| BTreeMap::from([("peer".to_owned(), c)]);
    assert!(feed_peers_caught_up(&BTreeMap::new(), &slow, now));
    assert!(feed_peers_caught_up(
        &feed(counters(true, now - 179 * s)),
        &slow,
        now
    ));
    assert!(!feed_peers_caught_up(
        &feed(counters(true, now - 180 * s)),
        &slow,
        now
    ));
    assert!(!feed_peers_caught_up(
        &feed(counters(false, now)),
        &slow,
        now
    ));
    assert!(feed_peers_caught_up(
        &feed(counters(true, now - 14 * s)),
        &BTreeMap::new(),
        now
    ));
    assert!(!feed_peers_caught_up(
        &feed(counters(true, now - 15 * s)),
        &BTreeMap::new(),
        now
    ));
    let never = PeerFeedCounters {
        last_exchange_ms: None,
        ..counters(true, 0)
    };
    assert!(!feed_peers_caught_up(&feed(never), &slow, now));
}
