//! The orchestrator's sync driver (spec §7.3–§7.5): the metadata scheduler,
//! the object pump and the peer clock probe. It only originates requests on
//! this side's outbound lanes ([`Lanes`], the [`Network`]); serving peers is
//! `PeerService`'s.
//!
//! Threads (all end once the driver's [`Cancel`] fires; [`Driver::stop`]):
//! * **scheduler** — looks at each paired peer with a Ready control lane × each
//!   Running, unpaused folder when poked and every [`DUE_CHECK`] at most, and
//!   dispatches an exchange for a (peer, repo) that is due, to a pool of
//!   [`MAX_EXCHANGES`] exchange threads (at most that many in flight overall).
//!   A pair is due when it is marked (a local change, a peer's metadata notify),
//!   when its control session or worker is new, or when its poll elapsed:
//!   `backup_poll_ms` on a session that gets notified, else `sync_interval_ms`.
//!   A failed exchange polls again within the shorter of the two. An exchange is
//!   `feed.outgoing {after: sent}` on the worker → `meta.exchange {repo}`
//!   (`FeedRequest {after: received, events}`) on the peer's control lane →
//!   `feed.apply` of the reply's events on the worker; then both cursors
//!   advance, and it repeats at once while either side has `more`. Cursors
//!   live in memory per (peer, repo) ([`SyncJob`]) and start over when the
//!   folder's worker or the peer's control session changes. A passive driver's
//!   go on instead: see below.
//! * **object pump** — one thread for every Running, unpaused folder. Each
//!   round has two passes (spec §7.8: demand never waits behind background
//!   traffic). Pass 1 supplies every folder's pending `watch` demand, then at
//!   most one batch of what its readers hinted, so that a read that begins
//!   waiting meanwhile, or a handle that closes, counts after one batch and not
//!   after a whole read-ahead window; pass 2
//!   prefetches `objects.missing {limit: OBJECT_BATCH, exclude, after}` per
//!   folder — a resumable scan: a page not wholly stored moves the folder's
//!   cursor past it, so every missing object is eventually listed — and is
//!   abandoned before its next call as soon as a demand wake-up (the
//!   wake [`Seq`] the folders' watch threads bump) arrives, so the next
//!   round's pass 1 waits at most for one call in flight (≤
//!   [`FETCH_DEADLINE_MAX`], and a wake cancels a background one). Objects are
//!   asked of each peer in turn, per
//!   owning Ready bulk lane: manifests (size unknown) in batches of ≤
//!   [`MANIFEST_BATCH`], then chunks in batches of ≤ [`OBJECT_BATCH`], each
//!   within a deadline that grows with what it asks ([`fetch_deadline`]),
//!   and written to the worker. A fetch that timed out halves that folder's
//!   batch for that peer (down to 1); each answered one grows it back by a
//!   quarter. Of a reply the peer cut to its byte bounds (`truncated_at`),
//!   the rest is asked again at once. A digest every asked
//!   peer omitted is excluded for a backoff (5 s doubling to 5 min), which
//!   spares re-asking it when the scan comes around again. A round that stored nothing sleeps
//!   `sync_interval_ms` unless woken. A peer whose fetch timed out or lost its
//!   lane sits out the next round, and a coded failure rests only that (folder,
//!   peer), as a push failure does. A reader's fetch gives a peer [`DEMAND_TRY`]
//!   while another could answer, with a peer known not to answer asked last, and
//!   the last peer asked gets its whole deadline, even at rest: a phone's one PC
//!   must not be locked out by its own slow link. The
//!   pump never blocks the scheduler. After each pass it also
//!   pushes ([`push`]), so a round runs: this side's demand, the listeners'
//!   demand, background pull, background push. The background push stops
//!   before its next call on a demand wake-up, as pass 2 does. Only a reader
//!   waiting is such a wake-up: its own side's demand, or a demand notify for a
//!   repo synced here. What marks work for later (a local change, an exchange
//!   that moved events) only rouses a sleeping pump ([`Seq::nudge`]), so it does
//!   not stop background work to do nothing new. The push steps
//!   run only for a pair that is due, so an idle pair asks its listener nothing:
//!   the demand step on a peer's demand notify, the background step on a local
//!   change or an exchange that moved events, and both on the pair's poll. A
//!   background step stays due until a whole sweep of the listener's scan stored
//!   nothing ([`push`]), and a failed one waits out a rest that grows.
//! * **clock probes** — a short thread per probe, at most one per peer: at
//!   every new control session (a Ready transition) and every
//!   [`CLOCK_PROBE_EVERY`] scheduler rounds. A round is one `sync_interval_ms`,
//!   however often the scheduler is poked. The smoothed offset goes to the
//!   catalog (`Catalog::set_clock`); one past `WARN_OFFSET_MS` logs
//!   `CLOCK_OFFSET_LARGE`.
//!
//! **Passive.** A passive driver asks peers nothing on its own: no exchange, no
//! clock probe, no push and no background pull, and a notify changes nothing.
//! Pass 1 still runs, so a local reader gets what it waits for from a peer whose
//! lanes are up, and that needs no poll: a folder's watch bumps the wake. Only a
//! burst ([`Driver::sync_now`], which works in either profile) opens the rest,
//! for the pairs it selected: an exchange until neither side has more, then
//! pushes until a whole sweep of the listener's scan, from its start, stored
//! nothing. A passive burst still does no background pull.
//!
//! A burst outlasts what may pass. A lost lane or a timeout is asked again until
//! the budget ends, which reports the last failure, at the pace of the push
//! pass's rests; its own recovery (a halved batch, a listing asked afresh) does
//! the rest. What a peer refused ends that pair's sweep and is reported by its
//! code, with what stays unsent in `remaining`. A peer that does not
//! hold a folder ends that pair: at once if it never answered for the repo, else
//! after [`REPO_WAIT`], since its folder may only be restarting. Any other coded
//! failure would repeat itself, so it ends the pair. A peer without a folder is
//! an error only when no peer took it.
//!
//! **Saved cursors.** A freeze on a phone ends the session, and a replay on every
//! session would make each background check read the whole log. So a passive
//! driver saves its cursors per (peer installation, repo) ([`CursorStore`]) and
//! starts a new session or worker where they stand. That replay is also what hides
//! events a crash lost, so a saved cursor passes only what is durable on both
//! sides: the exchange asks for `durable` on `meta.exchange` and `feed.apply` and
//! saves after both replied, page by page. Each page names the journal and the top
//! of its side; a journal that is not the cursor's, or a top below it, means the
//! cursor stands on nothing, and the exchange starts over from zero. A side that
//! names no journal is an older one: nothing is saved of it and nothing resumes.
//! What this cannot detect, such as a host restored from a backup that has since
//! grown past the cursor, is for `manage reset-cursors`. An active driver neither
//! loads nor saves nor asks for anything durable.
use super::{
    catalog::{Catalog, SavedCursor},
    config::SyncProfile,
    demand::Seq,
    endpoint::REPLY_TIMEOUT,
    network::{Network, NotifySink, ReadyPeer},
    push::{self, PushMode, PushState},
    sync::{FeedCursor, FeedPage, FeedReply, FeedRequest, Fetched, MAX_EXCLUDE},
    worker_client::{CALL_DEADLINE, WorkerLink},
};
use crate::{
    clock::{self, Offset, Sample, WARN_OFFSET_MS},
    content::Kind,
    model::{
        content::Digest,
        orchestrator::CallError,
        protocol::frame::{Notify, NotifyReason},
    },
    session::{Conn, Priority},
    util::{cancel::Cancel, fault, lock::lock, log::event, time::now_ms},
};
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver, Sender},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// Exchanges in flight at once, over every (peer, repo).
pub const MAX_EXCHANGES: usize = 4;
/// Objects per `objects.fetch` / `objects.write` (the worker refuses more).
pub const OBJECT_BATCH: usize = super::sync::OBJECT_BATCH;
/// A peer's clock is probed again every this many scheduler rounds.
pub const CLOCK_PROBE_EVERY: u64 = 60;
/// Deadline of one request on a peer lane (an `objects.fetch` gets more:
/// [`fetch_deadline`]).
pub const PEER_DEADLINE: Duration = Duration::from_secs(10);
/// Objects of unknown size (manifests) per `objects.fetch`.
pub const MANIFEST_BATCH: usize = 4;
/// The longest `objects.fetch` deadline.
pub const FETCH_DEADLINE_MAX: Duration = Duration::from_secs(120);
/// What a fetch deadline counts for a requested chunk (a full one)…
const CHUNK_GUESS: u64 = 64 * 1024;
/// …and for an object of unknown size (a manifest).
const UNKNOWN_GUESS: u64 = 1024 * 1024;
/// A fetch deadline grows by 1 s per this many requested bytes.
const DEADLINE_STEP_BYTES: u64 = 256 * 1024;
/// A digest no asked peer had is excluded this long at first…
pub const UNAVAILABLE_FIRST: Duration = Duration::from_secs(5);
/// …doubling each time to at most this.
pub const UNAVAILABLE_MAX: Duration = Duration::from_secs(300);
/// The scheduler looks for what came due at least this often, and so does the
/// pump while a push step stays due: a new control session and a failed step
/// send no poke.
const DUE_CHECK: Duration = Duration::from_millis(500);
/// How long a reader's `objects.fetch` waits for a peer when another could answer,
/// so that one that hangs does not use up the reader's own wait
/// (`Demand::FETCH_WAIT`).
const DEMAND_TRY: Duration = Duration::from_secs(3);
/// After a sweep that stored nothing, the pair's next poll is at least this many
/// times as long as the sweep took. A short one changes nothing. A long one is
/// many requests for a listener to scan, listing what neither side holds, and a
/// poll is no reason to start it over at once. What says something changed still
/// does.
const SWEEP_PACE: u32 = 10;
/// That wait is at most this: the longest backup poll there is.
const SWEEP_PACE_MAX: Duration = Duration::from_secs(3600);
/// How long [`Driver::sync_now`] may take: its reply beats the control client's
/// timeout, so the client never takes a slow burst for a lost connection.
pub const SYNC_NOW_BOUND: Duration = Duration::from_secs(REPLY_TIMEOUT.as_secs() - 5);
/// How long a burst waits for a peer's lanes before it reports that peer.
const LANE_WAIT: Duration = Duration::from_secs(10);
/// How long a burst keeps asking a peer that answered for a repo before and now
/// says it does not publish it: its folder may be restarting.
const REPO_WAIT: Duration = LANE_WAIT;
/// How long a burst waits between those asks.
const REPO_ASK_EVERY: Duration = Duration::from_secs(1);
/// How long a burst waits for a selected folder to be Running and unpaused. A
/// folder whose controller is only busy is back well within this.
const FOLDER_WAIT: Duration = Duration::from_secs(2);
/// How often a burst looks at how it is doing.
const BURST_LOOK: Duration = Duration::from_millis(50);
/// How long a passive driver's threads sleep with no burst open. Only a safety
/// net: a burst and a waiting reader wake them.
const PASSIVE_IDLE: Duration = Duration::from_secs(60);
/// Fault point: the background push steps do not run, so a test can hold bytes
/// on this side until a reader on the other side asks for them.
pub const FAULT_BACKGROUND_SKIP: &str = "sync.push.background.skip";
/// A demanded digest just written is not fetched again for this long (the
/// worker's demand list catches up at its next `watch`).
const DEMAND_SUPPLIED: Duration = super::worker_client::WATCH_DEADLINE;

pub(super) type Wanted = Vec<(Digest, Kind)>;

/// How this orchestrator keeps its folders in sync.
#[derive(Clone)]
pub struct SyncPolicy {
    pub profile: SyncProfile,
    /// The poll of a session that gets no notify, and the length of a round.
    pub interval: Duration,
    /// The poll of a session that gets notified.
    pub backup_poll: Duration,
    /// Where a passive driver keeps its feed cursors.
    pub cursors: Arc<dyn CursorStore>,
}

/// Where a passive driver's feed cursors survive sessions and restarts, per (peer
/// installation, repo). A failure is the store's to log and goes no further: all it
/// costs is a replay.
pub trait CursorStore: Send + Sync {
    fn load(&self, installation: &str, repo: &str) -> Option<SavedCursor>;
    fn save(&self, installation: &str, repo: &str, cursor: &SavedCursor);
    fn clear(&self);
}

impl CursorStore for Catalog {
    fn load(&self, installation: &str, repo: &str) -> Option<SavedCursor> {
        self.feed_cursor(installation, repo)
            .inspect_err(|error| {
                event!(warn, FEED_CURSOR_LOAD_FAILED, {
                    "installation": installation,
                    "repo": repo,
                    "error": format!("{error:#}"),
                });
            })
            .ok()
            .flatten()
    }
    fn save(&self, installation: &str, repo: &str, cursor: &SavedCursor) {
        if let Err(error) = self.save_feed_cursor(installation, repo, cursor) {
            event!(warn, FEED_CURSOR_SAVE_FAILED, {
                "installation": installation,
                "repo": repo,
                "error": format!("{error:#}"),
            });
        }
    }
    fn clear(&self) {
        if let Err(error) = self.clear_feed_cursors() {
            event!(warn, FEED_CURSOR_CLEAR_FAILED, { "error": format!("{error:#}") });
        }
    }
}

impl SyncPolicy {
    /// The poll of a session.
    fn poll(&self, notify: bool) -> Duration {
        if notify {
            self.backup_poll
        } else {
            self.interval
        }
    }

    /// The shorter poll: nothing comes due by time sooner than this.
    fn shortest(&self) -> Duration {
        self.interval.min(self.backup_poll)
    }
}

/// What a `sync-now` moves.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BurstMode {
    /// Metadata first, then the objects it names.
    #[default]
    Full,
    /// Metadata alone: no object is asked for or pushed, so a metered link
    /// carries a file list and no file.
    MetadataOnly,
}

/// (endpoint, repo).
type Pair = (String, String);

/// What is due before its poll says so.
#[derive(Default)]
struct Due {
    exchange: BTreeSet<Pair>,
    /// The demand push step.
    demand: BTreeSet<Pair>,
    /// The background push step.
    background: BTreeSet<Pair>,
    /// The open `sync-now`s. In here because what they count and what is marked
    /// have to change together.
    bursts: BTreeMap<u64, Burst>,
    next_burst: u64,
}

impl Due {
    /// Whether an open burst still wants `pair` synced.
    fn in_burst(&self, pair: &Pair) -> bool {
        in_burst(&self.bursts, pair)
    }

    /// Whether the push steps of `pair` are held back: bursts want it, and each
    /// is metadata-only.
    fn push_held(&self, pair: &Pair) -> bool {
        let now = Instant::now();
        let mut wanting = self
            .bursts
            .values()
            .filter(|burst| burst.wants(pair, now))
            .peekable();
        wanting.peek().is_some() && wanting.all(|burst| burst.mode == BurstMode::MetadataOnly)
    }

    /// What every open burst counts for `pair`.
    fn burst_pairs<'a>(&'a mut self, pair: &'a Pair) -> impl Iterator<Item = &'a mut BurstPair> {
        self.bursts
            .values_mut()
            .filter_map(move |burst| burst.pairs.get_mut(pair))
    }

    /// What every open burst that takes pushes counts for `pair`: a
    /// metadata-only burst records no push result.
    fn push_pairs<'a>(&'a mut self, pair: &'a Pair) -> impl Iterator<Item = &'a mut BurstPair> {
        self.burst_pairs(pair).filter(|state| !state.metadata_only)
    }

    /// `pair` joins `burst`, with everything due.
    /// A metadata-only burst marks the exchange alone.
    fn join_burst(&mut self, burst: u64, pair: Pair, error: Option<String>) {
        if let Some(burst) = self.bursts.get_mut(&burst) {
            let metadata_only = burst.mode == BurstMode::MetadataOnly;
            let state = BurstPair {
                rescan: true,
                error,
                metadata_only,
                ..BurstPair::default()
            };
            burst.pairs.insert(pair.clone(), state);
            self.exchange.insert(pair.clone());
            if !metadata_only {
                self.demand.insert(pair.clone());
                self.background.insert(pair);
            }
        }
    }
}

fn in_burst(bursts: &BTreeMap<u64, Burst>, pair: &Pair) -> bool {
    let now = Instant::now();
    bursts.values().any(|burst| burst.wants(pair, now))
}

/// One open `sync-now`.
struct Burst {
    until: Instant,
    mode: BurstMode,
    pairs: BTreeMap<Pair, BurstPair>,
}

impl Burst {
    /// Whether this burst is open and still wants `pair` synced.
    fn wants(&self, pair: &Pair, now: Instant) -> bool {
        self.until > now
            && self
                .pairs
                .get(pair)
                .is_some_and(|state| state.error.is_none() && state.not_published.is_none())
    }
}

/// What one `sync-now` counted for one pair.
#[derive(Default)]
struct BurstPair {
    /// Exchanges that finished.
    exchanged: u64,
    /// Objects the listener named durable.
    pushed: u64,
    /// What the last page listed and its pass did not store: an estimate of the
    /// work left. `None` until a page came back.
    remaining: Option<usize>,
    /// A background push, asked from the start of the scan, found nothing missing
    /// since this mark was last taken.
    swept: bool,
    /// The peer does not take pushes, so no sweep will come.
    no_push: bool,
    /// The burst moves metadata alone: no push step or sweep is to come, and
    /// the exchange is all it waits for.
    metadata_only: bool,
    /// The listener's scan must start over: what the exchange just told it may
    /// lie behind where the scan stood.
    rescan: bool,
    /// It failed in a way waiting will not cure, so this pair is finished.
    error: Option<String>,
    /// What this side holds did not all get across: the peer refused it, or no
    /// reply can carry it.
    refusal: Option<String>,
    /// The peer does not publish the repo, so this pair is finished: an error
    /// only if no other peer took the folder.
    not_published: Option<String>,
    /// The last failure that is being asked again. What a budget that ran out
    /// reports.
    failure: Option<String>,
    /// The peer answered for this repo before and now says it does not publish
    /// it: since when, and when to ask again.
    absent: Option<(Instant, Instant)>,
}

impl BurstPair {
    /// The peer says it does not publish the repo, and may be restarting it.
    fn note_absent(&mut self, error: String, now: Instant) {
        let since = self.absent.map_or(now, |(since, _)| since);
        self.absent = Some((since, now + REPO_ASK_EVERY));
        self.failure = Some(error);
    }

    /// What was failing is past.
    fn answered(&mut self) {
        self.failure = None;
        self.absent = None;
    }

    /// Why this pair is reported as failed, if it is.
    fn reason(&self) -> Option<&str> {
        self.error
            .as_deref()
            .or(self
                .no_push
                .then_some("PEER_PUSH_UNAVAILABLE: this peer does not take pushes"))
            .or(self.refusal.as_deref())
    }
}

/// A folder `sync-now` was asked about.
struct BurstFolder {
    id: String,
    /// Known once the folder was seen syncing.
    repo: Option<String>,
    /// When it was last seen syncing.
    seen: Instant,
    error: Option<String>,
}

/// A peer a `sync-now` is waiting on.
struct BurstPeer {
    /// When its control lane was last seen Ready.
    seen: Instant,
    /// Why it was given up on.
    error: Option<String>,
}

/// The peer lanes the driver uses: the [`Network`] (tests stub it).
pub trait Lanes: Send + Sync {
    /// Every peer with a Ready control lane.
    fn ready_peers(&self) -> Vec<ReadyPeer>;
    /// The endpoints with at least one Ready bulk lane.
    fn bulk_endpoints(&self) -> Vec<String>;
    /// The Ready bulk lane to `endpoint` that owns `digest`.
    fn bulk_lane(&self, endpoint: &str, digest: &Digest) -> Option<Conn>;
    /// Any Ready bulk lane to `endpoint`.
    fn any_bulk_lane(&self, endpoint: &str) -> Option<Conn>;
    /// Every endpoint lanes are kept to, whatever state they are in.
    fn dialed_endpoints(&self) -> Vec<String> {
        self.ready_peers()
            .into_iter()
            .map(|peer| peer.endpoint)
            .collect()
    }
    /// Why the control lane to `endpoint` last failed.
    fn lane_error(&self, _endpoint: &str) -> Option<String> {
        None
    }
    /// Lanes waiting out a transient backoff redial at once.
    fn nudge_lanes(&self) {}
}

impl Lanes for Network {
    fn ready_peers(&self) -> Vec<ReadyPeer> {
        Network::ready_peers(self)
    }
    fn bulk_endpoints(&self) -> Vec<String> {
        Network::bulk_endpoints(self)
    }
    fn bulk_lane(&self, endpoint: &str, digest: &Digest) -> Option<Conn> {
        Network::bulk_lane(self, endpoint, digest)
    }
    fn any_bulk_lane(&self, endpoint: &str) -> Option<Conn> {
        Network::any_bulk_lane(self, endpoint)
    }
    fn dialed_endpoints(&self) -> Vec<String> {
        Network::dialed_endpoints(self)
    }
    fn lane_error(&self, endpoint: &str) -> Option<String> {
        Network::lane_error(self, endpoint)
    }
    fn nudge_lanes(&self) {
        Network::nudge_lanes(self)
    }
}

/// A folder the driver syncs: Running, unpaused, with its worker link.
pub struct SyncFolder {
    pub id: String,
    pub repo: String,
    pub link: Arc<WorkerLink>,
    /// The objects the worker's readers wait for (its last `watch`).
    pub demand: Vec<(Digest, Kind)>,
    /// The objects they are expected to want next, nearest first.
    pub hints: Vec<(Digest, Kind)>,
}

/// The folders to sync (implemented over the daemon's folder slots).
pub trait SyncFolders: Send + Sync {
    /// Every Running, unpaused folder with a worker link.
    fn syncing(&self) -> Vec<SyncFolder>;
    /// Whether `repo` is a folder of this daemon, whatever it is doing now. A mark
    /// is kept only for those, so what a paired peer can make this side remember
    /// is bounded by its own folders and peers. Runs on a lane's I/O thread: no
    /// database, and no waiting for a busy controller.
    fn knows(&self, repo: &str) -> bool;
}

/// The metadata exchange state of one (peer, repo).
pub struct SyncJob {
    /// The peer's endpoint.
    pub peer: String,
    pub repo: String,
    pub cursor: FeedCursor,
    pub last_exchange_ms: u64,
    pub error: Option<String>,
}

/// A job with what its cursors belong to.
struct Slot {
    job: SyncJob,
    /// The worker link the cursors were taken on.
    link: Weak<WorkerLink>,
    /// The peer control session the cursors were taken on.
    session: String,
    /// The journals a passive driver's cursor counts in.
    journals: Journals,
    running: bool,
    /// When its poll comes due.
    next_poll: Option<Instant>,
}

/// One dispatched exchange.
struct Task {
    key: (String, String),
    /// The peer's installation, which its saved cursor goes by.
    installation: String,
    link: Arc<WorkerLink>,
    conn: Conn,
    session: String,
    cursor: FeedCursor,
    journals: Journals,
}

/// The journals a cursor counts in: without them its numbers mean nothing. Empty
/// where that side has named none.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Journals {
    local: String,
    peer: String,
}

/// What a passive driver's exchange has beyond its cursor.
struct Keeping<'a> {
    journals: &'a mut Journals,
    /// Takes a page's cursor once what it passes is durable on both sides.
    save: &'a dyn Fn(&SavedCursor),
}

impl Keeping<'_> {
    /// `cursor` no longer stands: from zero, in no journal. The row is overwritten
    /// by the first page that can be saved.
    fn start_over(&mut self, cursor: &mut FeedCursor, repo: &str, side: &str) {
        event!(info, FEED_CURSOR_STARTED_OVER, {
            "repo": repo,
            "side": side,
            "sent": cursor.sent,
            "received": cursor.received,
        });
        *cursor = FeedCursor::default();
        *self.journals = Journals::default();
    }

    /// A page both sides took: `cursor` passes it, from `before`, and these are the
    /// journals both of them count in.
    fn page_taken(&mut self, before: FeedCursor, cursor: FeedCursor, journals: Journals) {
        let changed = cursor != before || journals != *self.journals;
        if changed && !journals.local.is_empty() && !journals.peer.is_empty() {
            (self.save)(&SavedCursor {
                sent: cursor.sent,
                received: cursor.received,
                local_journal: journals.local.clone(),
                peer_journal: journals.peer.clone(),
            });
        }
        *self.journals = journals;
    }
}

/// Whether a cursor at `at` in `journal` still stands on a side that names `named`
/// as its journal and `top` as its latest sequence. One that names none is an older
/// one, and its top says nothing.
fn cursor_stands(journal: &str, at: u64, named: &str, top: u64) -> bool {
    journal == named && (named.is_empty() || top >= at)
}

#[derive(Default)]
struct ProbeState {
    /// The control session last probed.
    session: String,
    /// The scheduler round of that probe.
    round: u64,
    running: bool,
}

/// The object pump's memory across rounds.
#[derive(Default)]
struct PumpState {
    round: u64,
    /// Peers sitting out: endpoint → the first round they are asked again.
    resting: BTreeMap<String, u64>,
    /// The same of one folder only: (folder id, endpoint), after a failure that
    /// says nothing of the peer's other folders.
    folder_resting: BTreeMap<(String, String), u64>,
    /// The last error logged per (peer endpoint, folder) of a fetch, or per
    /// folder id of `objects.missing`: logged again only when it changes.
    logged: BTreeMap<String, String>,
    /// Per folder id: the digests no asked peer had, and until when they are
    /// excluded.
    unavailable: BTreeMap<String, BTreeMap<Digest, Unavailable>>,
    /// Per folder id: where its `objects.missing` scan resumes (`None`: from
    /// the start).
    scan: BTreeMap<String, Option<String>>,
    /// (folder id, digest) written lately: not fetched again for a stale
    /// demand.
    supplied: BTreeMap<(String, Digest), Instant>,
    /// (folder id, endpoint) → the objects asked per `objects.fetch`, while
    /// below [`OBJECT_BATCH`]: halved by a timeout, grown back by replies.
    batch: BTreeMap<(String, String), usize>,
    /// The push steps' memory.
    push: PushState,
    /// When each pair's push poll comes due; a pair not here is due.
    polls: BTreeMap<Pair, Instant>,
}

/// A batch of `asked` objects timed out: half that, at least 1.
pub(super) fn halved_batch(size: usize, asked: usize) -> usize {
    (size.min(asked) / 2).max(1)
}

/// A batch was answered: a quarter more (at least 1), up to [`OBJECT_BATCH`].
pub(super) fn grown_batch(size: usize) -> usize {
    (size + (size / 4).max(1)).min(OBJECT_BATCH)
}

impl PumpState {
    /// Of `endpoints`, the ones `id` can ask now.
    fn askable(&self, endpoints: &[String], id: &str) -> Vec<String> {
        endpoints
            .iter()
            .filter(|endpoint| {
                let folder_peer = (id.to_owned(), (*endpoint).clone());
                !self.folder_resting.contains_key(&folder_peer)
            })
            .cloned()
            .collect()
    }

    /// The objects asked of `endpoint` per fetch for folder `id`.
    fn batch_size(&self, id: &str, endpoint: &str) -> usize {
        self.batch
            .get(&(id.to_owned(), endpoint.to_owned()))
            .copied()
            .unwrap_or(OBJECT_BATCH)
    }

    /// A fetch of `asked` objects timed out: half that, at least 1.
    fn shrink(&mut self, id: &str, endpoint: &str, asked: usize) {
        let size = halved_batch(self.batch_size(id, endpoint), asked);
        self.batch
            .insert((id.to_owned(), endpoint.to_owned()), size);
        event!(info, SYNC_BATCH_SHRUNK, {
            "peer": endpoint,
            "folder": id,
            "batch": size,
        });
    }

    /// A fetch was answered: a quarter more (at least 1), up to
    /// [`OBJECT_BATCH`].
    fn grow(&mut self, id: &str, endpoint: &str) {
        let key = (id.to_owned(), endpoint.to_owned());
        if let Some(size) = self.batch.get(&key).copied() {
            let size = grown_batch(size);
            if size >= OBJECT_BATCH {
                self.batch.remove(&key);
            } else {
                self.batch.insert(key, size);
            }
        }
    }
}

pub(super) struct Unavailable {
    pub(super) until: Instant,
    pub(super) backoff: Duration,
}

/// What one [`Driver::supply`] did.
#[derive(Default)]
struct Supplied {
    stored: usize,
    /// It stopped early (a wake-up, cancel or a failed write).
    interrupted: bool,
}

/// Why an exchange or a fetch did not complete.
#[derive(Debug, PartialEq, Eq)]
pub enum Failure {
    /// The peer lane went away (closed, GOAWAY, cancelled): not a repo error.
    LaneLost,
    /// No reply in time. Uncertain, so nothing is replayed, and a slow link may
    /// well answer the next request.
    TimedOut(String),
    /// The peer has no running, unpaused folder of the repo.
    NotPublished(String),
    Error(String),
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Failure::LaneLost => f.write_str("LANE_LOST: the peer lane went away"),
            Failure::TimedOut(error) | Failure::NotPublished(error) | Failure::Error(error) => {
                f.write_str(error)
            }
        }
    }
}

pub(super) fn peer_failure(op: &str, error: CallError) -> Failure {
    match error {
        CallError::ConnectionLost(_) | CallError::Unavailable(_) | CallError::Cancelled => {
            Failure::LaneLost
        }
        CallError::Timeout { .. } => Failure::TimedOut(format!("{op}: {error}")),
        CallError::Remote(ref wire) if wire.code == "REPO_NOT_PUBLISHED" => {
            Failure::NotPublished(format!("{op}: {error}"))
        }
        error => Failure::Error(format!("{op}: {error}")),
    }
}

pub(super) fn worker_failure(op: &str, error: impl std::fmt::Display) -> Failure {
    Failure::Error(format!("{op}: {error}"))
}

pub struct Driver {
    lanes: Arc<dyn Lanes>,
    catalog: Arc<Catalog>,
    folders: Arc<dyn SyncFolders>,
    /// Bumped when a reader waits: by folder watch threads when demand changes
    /// or lasts, and by a demand notify for a repo synced here. A bump stops the
    /// background passes. What only marks work for later nudges it instead.
    wake: Arc<Seq>,
    /// Bumped by what marks an exchange due; wakes the scheduler.
    poke: Arc<Seq>,
    due: Mutex<Due>,
    policy: SyncPolicy,
    cancel: Cancel,
    jobs: Mutex<BTreeMap<(String, String), Slot>>,
    /// What the pump's pushes counted, per (endpoint, repo), for [`Driver::view`].
    pushes: Mutex<BTreeMap<Pair, (u64, Option<String>)>>,
    busy: AtomicUsize,
    /// Shared with probe threads, which clear their `running` flag.
    probes: Arc<Mutex<BTreeMap<String, ProbeState>>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    /// The deadline of an `objects.fetch` ([`fetch_deadline`]; tests
    /// shorten it).
    fetch_deadline: fn(&[(Digest, Kind)]) -> Duration,
}

/// Starts the scheduler, its exchange pool and the pump. `cancel` (a child
/// of the daemon's stopping token) stops them.
pub fn start(
    lanes: Arc<dyn Lanes>,
    catalog: Arc<Catalog>,
    folders: Arc<dyn SyncFolders>,
    wake: Arc<Seq>,
    policy: SyncPolicy,
    cancel: Cancel,
) -> Result<Arc<Driver>> {
    let driver = Driver::with_policy(lanes, catalog, folders, wake, policy, cancel);
    let (tasks, queue) = mpsc::channel::<Task>();
    let queue = Arc::new(Mutex::new(queue));
    let mut threads = Vec::new();
    let mut spawn = |name: String, body: Box<dyn FnOnce() + Send>| -> Result<()> {
        match thread::Builder::new().name(name).spawn(body) {
            Ok(handle) => {
                threads.push(handle);
                Ok(())
            }
            Err(error) => {
                // The threads already running end with the driver.
                driver.cancel.cancel();
                Err(error).context("SYNC_THREAD_FAILED")
            }
        }
    };
    for index in 0..MAX_EXCHANGES {
        let (exchanges, queue) = (driver.clone(), queue.clone());
        spawn(
            format!("tkfs-sync-exchange-{index}"),
            Box::new(move || exchanges.exchanges(&queue)),
        )?;
    }
    let scheduler = driver.clone();
    spawn(
        "tkfs-sync-scheduler".into(),
        Box::new(move || scheduler.schedule(tasks)),
    )?;
    let pump = driver.clone();
    spawn("tkfs-sync-pump".into(), Box::new(move || pump.pump()))?;
    *lock(&driver.threads) = threads;
    Ok(driver)
}

impl Driver {
    /// An active driver with no threads yet, polling every `interval`.
    #[cfg(test)]
    fn new(
        lanes: Arc<dyn Lanes>,
        catalog: Arc<Catalog>,
        folders: Arc<dyn SyncFolders>,
        wake: Arc<Seq>,
        interval: Duration,
        cancel: Cancel,
    ) -> Arc<Self> {
        let policy = SyncPolicy {
            profile: SyncProfile::Active,
            interval,
            backup_poll: interval,
            cursors: catalog.clone(),
        };
        Self::with_policy(lanes, catalog, folders, wake, policy, cancel)
    }

    /// A driver with no threads yet (see [`start`]).
    fn with_policy(
        lanes: Arc<dyn Lanes>,
        catalog: Arc<Catalog>,
        folders: Arc<dyn SyncFolders>,
        wake: Arc<Seq>,
        policy: SyncPolicy,
        cancel: Cancel,
    ) -> Arc<Self> {
        Arc::new(Driver {
            lanes,
            catalog,
            folders,
            wake,
            poke: Seq::new(),
            due: Mutex::new(Due::default()),
            policy,
            cancel,
            jobs: Mutex::new(BTreeMap::new()),
            pushes: Mutex::new(BTreeMap::new()),
            busy: AtomicUsize::new(0),
            probes: Arc::new(Mutex::new(BTreeMap::new())),
            threads: Mutex::new(Vec::new()),
            fetch_deadline,
        })
    }

    /// Stops every thread: cancels, wakes the pump and waits at most
    /// `budget` for them (a thread still in a bounded worker call is left to
    /// end on its own and reported).
    pub fn stop(&self, budget: Duration) {
        self.cancel.cancel();
        self.wake.bump();
        self.poke.bump();
        let end = Instant::now() + budget;
        let threads = std::mem::take(&mut *lock(&self.threads));
        while Instant::now() < end && !threads.iter().all(JoinHandle::is_finished) {
            thread::sleep(Duration::from_millis(10));
        }
        let left = threads.iter().filter(|t| !t.is_finished()).count();
        event!(info, SYNC_DRIVER_STOPPED, { "threads_left": left });
    }

    /// `{endpoint: {sent, received, last_exchange_ms, error, pushed,
    /// last_push_error}}` of `repo`.
    pub fn view(&self, repo: &str) -> Value {
        let jobs = lock(&self.jobs);
        let pushes = lock(&self.pushes);
        let map: serde_json::Map<String, Value> = jobs
            .values()
            .filter(|slot| slot.job.repo == repo)
            .map(|slot| {
                let job = &slot.job;
                let (pushed, push_error) = pushes
                    .get(&(job.peer.clone(), repo.to_owned()))
                    .cloned()
                    .unwrap_or_default();
                (
                    job.peer.clone(),
                    json!({
                        "sent": job.cursor.sent,
                        "received": job.cursor.received,
                        "last_exchange_ms": job.last_exchange_ms,
                        "error": job.error,
                        "pushed": pushed,
                        "last_push_error": push_error,
                    }),
                )
            })
            .collect();
        Value::Object(map)
    }

    fn passive(&self) -> bool {
        matches!(self.policy.profile, SyncProfile::Passive)
    }

    /// Whether a passive driver has nothing to do but serve readers.
    fn at_rest(&self) -> bool {
        self.passive() && lock(&self.due).bursts.is_empty()
    }

    /// `repo` changed on this side: every peer's pair of it is due. A passive
    /// driver tells nobody until it is asked, and a burst marks what it needs.
    pub fn local_change(&self, repo: &str) {
        if self.at_rest() {
            return;
        }
        let peers = self.lanes.ready_peers();
        {
            let mut due = lock(&self.due);
            for peer in &peers {
                let pair = (peer.endpoint.clone(), repo.to_owned());
                due.exchange.insert(pair.clone());
                due.background.insert(pair);
            }
        }
        self.poke.bump();
        self.wake.nudge();
    }

    /// Forgets every feed cursor, saved and in memory, so that the next exchange of
    /// each pair starts over. One under way saves nothing more and its results are
    /// dropped ([`Self::save_cursor`], [`Self::record_finish`]): its session is no
    /// longer the slot's, which is also what makes the pair due again.
    pub fn reset_cursors(&self) {
        let mut jobs = lock(&self.jobs);
        self.policy.cursors.clear();
        for slot in jobs.values_mut() {
            slot.job.cursor = FeedCursor::default();
            slot.journals = Journals::default();
            slot.session.clear();
        }
    }

    /// Saves `cursor` for `task`'s pair, unless [`Self::reset_cursors`] came since
    /// it started: under the lock that takes, so nothing it cleared comes back.
    fn save_cursor(&self, task: &Task, cursor: &SavedCursor) {
        let jobs = lock(&self.jobs);
        let current = jobs
            .get(&task.key)
            .is_some_and(|slot| slot.session == task.session);
        if current {
            self.policy
                .cursors
                .save(&task.installation, &task.key.1, cursor);
        }
    }

    /// Where a new session or worker starts: from zero, or for a passive driver
    /// where its saved cursor stands.
    fn starting_point(&self, installation: &str, repo: &str) -> (FeedCursor, Journals) {
        let saved = self
            .passive()
            .then(|| self.policy.cursors.load(installation, repo))
            .flatten();
        saved.map_or_else(Default::default, |saved| {
            let cursor = FeedCursor {
                sent: saved.sent,
                received: saved.received,
            };
            let journals = Journals {
                local: saved.local_journal,
                peer: saved.peer_journal,
            };
            (cursor, journals)
        })
    }

    // ---- sync-now --------------------------------------------------------

    /// Syncs `folders` (ids) with every peer now, in either profile, and reports
    /// `{"folders": [{"id", "exchanged", "pushed", "remaining", "error"}]}`.
    /// Lanes in a transient backoff redial at once. It returns when every pair
    /// is done or given up on, and within `budget` either way. `remaining` is an
    /// estimate from the last `objects.wanted` page, `null` when none came back.
    /// A budget that runs out reports the last failure being retried.
    /// A peer with no Ready control lane or no Ready bulk lane for [`LANE_WAIT`]
    /// is reported with the control lane's failure, so it costs that wait and not
    /// the budget, and a daemon that is stopping ends this at once.
    /// A [`BurstMode::MetadataOnly`] burst takes no push step and counts no
    /// `pushed`; it is done when its exchange is.
    pub fn sync_now(&self, folders: &[String], budget: Duration, mode: BurstMode) -> Value {
        let began = Instant::now();
        let end = began + budget;
        self.lanes.nudge_lanes();
        let mut peers: BTreeMap<String, BurstPeer> = self
            .lanes
            .dialed_endpoints()
            .into_iter()
            .map(|endpoint| {
                let peer = BurstPeer {
                    seen: began,
                    error: None,
                };
                (endpoint, peer)
            })
            .collect();
        let mut folders: Vec<BurstFolder> = folders
            .iter()
            .map(|id| BurstFolder {
                id: id.clone(),
                repo: None,
                seen: began,
                error: None,
            })
            .collect();
        let burst = {
            let mut due = lock(&self.due);
            let burst = due.next_burst;
            due.next_burst += 1;
            let pairs = BTreeMap::new();
            due.bursts.insert(
                burst,
                Burst {
                    until: end,
                    mode,
                    pairs,
                },
            );
            burst
        };
        let failure = loop {
            self.burst_look(burst, &mut folders, &mut peers);
            if self.cancel.is_cancelled() {
                break Some("ORCHESTRATOR_STOPPING: the orchestrator is shutting down".to_owned());
            }
            let probing = lock(&self.probes).values().any(|probe| probe.running);
            let settled = {
                let jobs = lock(&self.jobs);
                let due = lock(&self.due);
                folders
                    .iter()
                    .all(|folder| burst_folder_settled(&jobs, &due, burst, folder))
            };
            if settled && !probing {
                break None;
            }
            let left = end.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break Some(format!("SYNC_NOW_TIMEOUT: not finished within {budget:?}"));
            }
            self.cancel.sleep(BURST_LOOK.min(left));
        };
        self.close_burst(burst, &folders, failure)
    }

    /// Takes in the folders that became known and gives up on the folders and
    /// peers gone for too long.
    fn burst_look(
        &self,
        burst: u64,
        folders: &mut [BurstFolder],
        peers: &mut BTreeMap<String, BurstPeer>,
    ) {
        let now = Instant::now();
        let syncing = self.folders.syncing();
        let ready = self.lanes.ready_peers();
        let bulk = self.lanes.bulk_endpoints();
        let mut failed = Vec::new();
        for (endpoint, peer) in peers.iter_mut() {
            // A push needs a bulk lane as an exchange needs the control lane.
            if bulk.contains(endpoint) && ready.iter().any(|ready| ready.endpoint == *endpoint) {
                peer.seen = now;
            } else if peer.error.is_none() && now.duration_since(peer.seen) >= LANE_WAIT {
                let error = self.lanes.lane_error(endpoint).unwrap_or_else(|| {
                    format!("SYNC_NOW_TIMEOUT: no Ready lane within {LANE_WAIT:?}")
                });
                peer.error = Some(error.clone());
                failed.push((endpoint.clone(), error));
            }
        }
        let mut joined = false;
        let mut due = lock(&self.due);
        for folder in folders.iter_mut().filter(|folder| folder.error.is_none()) {
            match syncing.iter().find(|syncing| syncing.id == folder.id) {
                Some(syncing) => {
                    folder.seen = now;
                    if folder.repo.is_none() {
                        folder.repo = Some(syncing.repo.clone());
                        for (endpoint, peer) in peers.iter() {
                            let pair = (endpoint.clone(), syncing.repo.clone());
                            due.join_burst(burst, pair, peer.error.clone());
                            joined = true;
                        }
                    }
                }
                None if now.duration_since(folder.seen) >= FOLDER_WAIT => {
                    folder.error = Some(format!(
                        "WORKER_NOT_RUNNING: folder {} is not running, or is paused",
                        folder.id
                    ));
                }
                None => {}
            }
        }
        let Due {
            bursts, exchange, ..
        } = &mut *due;
        if let Some(burst) = bursts.get_mut(&burst) {
            for (endpoint, error) in failed {
                for ((peer, _), state) in &mut burst.pairs {
                    if *peer == endpoint {
                        state.error.get_or_insert_with(|| error.clone());
                    }
                }
            }
            for (pair, state) in &mut burst.pairs {
                let Some((since, ask)) = state.absent else {
                    continue;
                };
                if now.duration_since(since) >= REPO_WAIT {
                    state.not_published = state.failure.take();
                    state.absent = None;
                } else if ask <= now {
                    state.absent = Some((since, now + REPO_ASK_EVERY));
                    exchange.insert(pair.clone());
                    joined = true;
                }
            }
        }
        drop(due);
        if joined {
            self.poke.bump();
            self.wake.nudge();
        }
    }

    /// Ends `burst` and reports it. `failure` is why it stopped, for the folders
    /// it did not finish.
    fn close_burst(&self, burst: u64, folders: &[BurstFolder], failure: Option<String>) -> Value {
        // A daemon that is stopping says so: what was being retried did not end
        // this, and time did not run out.
        let stopping = self.cancel.is_cancelled();
        let jobs = lock(&self.jobs);
        let mut due = lock(&self.due);
        let reports: Vec<Value> = folders
            .iter()
            .map(|folder| {
                let settled = burst_folder_settled(&jobs, &due, burst, folder);
                let unfinished = failure.as_ref().filter(|_| !settled).map(|failure| {
                    if stopping {
                        failure.clone()
                    } else {
                        last_failure(&jobs, &due, burst, folder, failure)
                    }
                });
                burst_report(due.bursts.get(&burst), folder, unfinished)
            })
            .collect();
        due.bursts.remove(&burst);
        json!({ "folders": reports })
    }

    // ---- scheduler -------------------------------------------------------

    fn schedule(&self, tasks: Sender<Task>) {
        let look = self.policy.shortest().min(DUE_CHECK);
        let (mut round, mut turn): (u64, u64) = (1, 0);
        let mut next_round = Instant::now() + self.policy.interval;
        while !self.cancel.is_cancelled() {
            let seen = self.poke.current();
            let now = Instant::now();
            if now >= next_round {
                round += 1;
                next_round = now + self.policy.interval;
            }
            turn += 1;
            if self.at_rest() {
                self.poke.wait_past(seen, PASSIVE_IDLE);
                continue;
            }
            let peers = self.lanes.ready_peers();
            self.probe_clocks(round, &peers);
            self.dispatch(turn, &peers, &tasks);
            self.poke.wait_past(seen, look);
        }
        // Dropping `tasks` ends the exchange threads once they are idle.
    }

    /// Dispatches an exchange for every idle (peer, repo) that is due, within
    /// [`MAX_EXCHANGES`]; the pairs are rotated each `turn` so none starves.
    fn dispatch(&self, turn: u64, peers: &[ReadyPeer], tasks: &Sender<Task>) {
        let folders = self.folders.syncing();
        let mut pairs: Vec<(&ReadyPeer, &SyncFolder)> = peers
            .iter()
            .flat_map(|peer| folders.iter().map(move |folder| (peer, folder)))
            .collect();
        if pairs.is_empty() {
            return;
        }
        let turn = (turn as usize) % pairs.len();
        pairs.rotate_left(turn);
        let now = Instant::now();
        let mut jobs = lock(&self.jobs);
        for (peer, folder) in pairs {
            if self.busy.load(Ordering::Acquire) >= MAX_EXCHANGES {
                return;
            }
            let key = (peer.endpoint.clone(), folder.repo.clone());
            if self.passive() && !lock(&self.due).in_burst(&key) {
                continue;
            }
            let slot = jobs.entry(key.clone()).or_insert_with(|| Slot {
                job: SyncJob {
                    peer: peer.endpoint.clone(),
                    repo: folder.repo.clone(),
                    cursor: FeedCursor::default(),
                    last_exchange_ms: 0,
                    error: None,
                },
                link: Weak::new(),
                session: String::new(),
                journals: Journals::default(),
                running: false,
                next_poll: None,
            });
            if slot.running {
                continue;
            }
            // A restarted worker or a new control session: start over, or for a
            // passive driver where its saved cursor stands.
            let new = !Weak::ptr_eq(&slot.link, &Arc::downgrade(&folder.link))
                || slot.session != peer.session;
            if new {
                slot.link = Arc::downgrade(&folder.link);
                slot.session = peer.session.clone();
                (slot.job.cursor, slot.journals) =
                    self.starting_point(&peer.installation, &folder.repo);
            }
            // No clock moves a passive driver, even during a burst.
            let polled = !self.passive() && slot.next_poll.is_none_or(|at| at <= now);
            let marked = lock(&self.due).exchange.remove(&key);
            if !(new || polled || marked) {
                continue;
            }
            slot.next_poll = Some(now + self.policy.poll(peer.notify));
            let task = Task {
                key,
                installation: peer.installation.clone(),
                link: folder.link.clone(),
                conn: peer.control.clone(),
                session: peer.session.clone(),
                cursor: slot.job.cursor,
                journals: slot.journals.clone(),
            };
            slot.running = true;
            self.busy.fetch_add(1, Ordering::AcqRel);
            if tasks.send(task).is_err() {
                slot.running = false;
                self.busy.fetch_sub(1, Ordering::AcqRel);
                return;
            }
        }
    }

    /// One exchange thread: runs tasks until the scheduler is gone.
    fn exchanges(&self, queue: &Mutex<Receiver<Task>>) {
        loop {
            let task = match lock(queue).recv() {
                Ok(task) => task,
                Err(_) => return,
            };
            self.run_exchange(task);
        }
    }

    /// One exchange, start to finish.
    fn run_exchange(&self, mut task: Task) {
        let mut cursor = task.cursor;
        let mut journals = std::mem::take(&mut task.journals);
        let save = |saved: &SavedCursor| self.save_cursor(&task, saved);
        let keeping = self.passive().then_some(Keeping {
            journals: &mut journals,
            save: &save,
        });
        let result = exchange(
            &task.link,
            &task.conn,
            &task.key.1,
            &mut cursor,
            keeping,
            &self.cancel,
        );
        task.journals = journals;
        self.finish(&task, cursor, result);
    }

    /// Records an exchange's outcome on its job, unless the job started over
    /// meanwhile.
    fn finish(&self, task: &Task, cursor: FeedCursor, result: Result<(), Failure>) {
        self.record_finish(task, cursor, result);
        // What came due while it ran, or waited for a free exchange.
        self.poke.bump();
    }

    fn record_finish(&self, task: &Task, cursor: FeedCursor, result: Result<(), Failure>) {
        let mut jobs = lock(&self.jobs);
        self.busy.fetch_sub(1, Ordering::AcqRel);
        let Some(slot) = jobs.get_mut(&task.key) else {
            return;
        };
        slot.running = false;
        let current =
            slot.session == task.session && Weak::ptr_eq(&slot.link, &Arc::downgrade(&task.link));
        if !current {
            return;
        }
        // Pages the peer and the worker both took stay taken.
        slot.job.cursor = cursor;
        slot.journals = task.journals.clone();
        // Events moved: the objects they name may now be wanted, either way.
        let moved = cursor != task.cursor;
        if moved {
            let mut due = lock(&self.due);
            if !due.push_held(&task.key) {
                due.background.insert(task.key.clone());
            }
            drop(due);
            self.wake.nudge();
        }
        let answered = slot.job.last_exchange_ms > 0;
        self.burst_exchanged(&task.key, moved, answered, &result);
        // What this exchange carried is not across yet: no waiting for the
        // backup poll.
        if result.is_err() {
            let retry = Instant::now() + self.policy.shortest();
            slot.next_poll = Some(slot.next_poll.map_or(retry, |at| at.min(retry)));
        }
        match result {
            Ok(()) => {
                slot.job.last_exchange_ms = now_ms();
                slot.job.error = None;
            }
            Err(Failure::LaneLost) => {}
            Err(failure) => {
                let error = failure.to_string();
                if slot.job.error.as_deref() != Some(error.as_str()) {
                    event!(warn, SYNC_EXCHANGE_FAILED, {
                        "peer": task.key.0.clone(),
                        "repo": task.key.1.clone(),
                        "error": error.clone(),
                    });
                }
                slot.job.error = Some(error);
            }
        }
    }

    /// Tells the open bursts of `key` how its exchange went. A lost lane or a
    /// timeout is asked again at once, by the next session if it takes that. A
    /// peer that says it lacks the repo ends the pair, unless it `answered` for
    /// that repo before: then [`Self::burst_look`] asks again for a while. Any
    /// other failure would only repeat itself, so it ends the pair.
    fn burst_exchanged(
        &self,
        key: &Pair,
        moved: bool,
        answered: bool,
        result: &Result<(), Failure>,
    ) {
        let now = Instant::now();
        let mut due = lock(&self.due);
        let wanted = due.in_burst(key);
        for state in due.burst_pairs(key) {
            state.rescan |= moved;
            match result {
                Ok(()) => {
                    state.exchanged += 1;
                    state.answered();
                }
                Err(Failure::NotPublished(error)) if answered => {
                    state.note_absent(error.clone(), now);
                }
                Err(Failure::NotPublished(error)) => {
                    state.not_published.get_or_insert_with(|| error.clone());
                }
                Err(Failure::Error(error)) => {
                    state.error.get_or_insert_with(|| error.clone());
                }
                Err(failure) => state.failure = Some(failure.to_string()),
            }
        }
        if wanted && matches!(result, Err(Failure::LaneLost | Failure::TimedOut(_))) {
            due.exchange.insert(key.clone());
        }
    }

    // ---- clock -----------------------------------------------------------

    /// Starts a probe of every peer whose control session is new or whose
    /// last probe is [`CLOCK_PROBE_EVERY`] rounds old.
    fn probe_clocks(&self, round: u64, peers: &[ReadyPeer]) {
        let mut probes = lock(&self.probes);
        for peer in peers {
            let state = probes.entry(peer.endpoint.clone()).or_default();
            let due = state.session != peer.session
                || round.saturating_sub(state.round) >= CLOCK_PROBE_EVERY;
            if state.running || !due {
                continue;
            }
            state.session = peer.session.clone();
            state.round = round;
            state.running = true;
            let (catalog, endpoint) = (self.catalog.clone(), peer.endpoint.clone());
            let (installation, conn) = (peer.installation.clone(), peer.control.clone());
            let cancel = self.cancel.clone();
            let done = Arc::downgrade(&self.probes);
            let spawned = thread::Builder::new()
                .name("tkfs-clock-probe".into())
                .spawn(move || {
                    if let Err(error) = probe(&catalog, &installation, &conn, &cancel) {
                        event!(info, CLOCK_PROBE_FAILED, {
                            "peer": endpoint.clone(),
                            "error": format!("{error:#}"),
                        });
                    }
                    if let Some(done) = done.upgrade()
                        && let Some(state) = lock(&done).get_mut(&endpoint)
                    {
                        state.running = false;
                    }
                });
            if spawned.is_err() {
                state.running = false;
            }
        }
    }

    // ---- pump ------------------------------------------------------------

    fn pump(&self) {
        let mut state = PumpState::default();
        while !self.cancel.is_cancelled() {
            let since = self.wake.mark();
            let stored = self.pump_round(&mut state, since.value);
            if self.cancel.is_cancelled() {
                break;
            }
            if stored == 0 {
                // Returns at once when something came up meanwhile.
                self.wake.wait_moved(since, self.pump_wait());
            }
        }
    }

    /// How long a round that stored nothing rests: until a poll may come due,
    /// or [`DUE_CHECK`] while a push step of a syncing folder stays due. Marks
    /// of a repo not synced here wait for it and do not keep the pump awake,
    /// and nothing comes due by time on a passive driver.
    fn pump_wait(&self) -> Duration {
        if self.at_rest() {
            return PASSIVE_IDLE;
        }
        let repos: BTreeSet<String> = self
            .folders
            .syncing()
            .into_iter()
            .map(|folder| folder.repo)
            .collect();
        let due = lock(&self.due);
        let pending = due
            .demand
            .iter()
            .chain(&due.background)
            .any(|pair| repos.contains(&pair.1) && (!self.passive() || due.in_burst(pair)));
        if pending {
            self.policy.shortest().min(DUE_CHECK)
        } else {
            self.policy.shortest()
        }
    }

    /// Marks both push steps due for the pairs whose poll came up, and drops
    /// the marks of a peer with no Ready lane left, or of a folder this daemon no
    /// longer has. When the peer returns, its new control session starts an
    /// exchange that moves events again. What a burst marked stays, since it waits
    /// for that peer. A passive driver has no polls.
    fn mark_polls(
        &self,
        state: &mut PumpState,
        endpoints: &[String],
        peers: &[ReadyPeer],
        folders: &[SyncFolder],
    ) {
        state
            .polls
            .retain(|(endpoint, _), _| endpoints.contains(endpoint));
        let notified = |endpoint: &str| {
            peers
                .iter()
                .any(|peer| peer.notify && peer.endpoint == endpoint)
        };
        let now = Instant::now();
        let mut due = lock(&self.due);
        let lanes = |endpoint: &String| {
            endpoints.contains(endpoint) || peers.iter().any(|peer| peer.endpoint == *endpoint)
        };
        {
            let Due {
                exchange,
                demand,
                background,
                bursts,
                ..
            } = &mut *due;
            let keep = |pair: &Pair| {
                (lanes(&pair.0) && self.folders.knows(&pair.1)) || in_burst(bursts, pair)
            };
            exchange.retain(keep);
            demand.retain(keep);
            background.retain(keep);
        }
        if self.passive() {
            return;
        }
        for folder in folders {
            for endpoint in endpoints {
                let pair = (endpoint.clone(), folder.repo.clone());
                if state.push.rests(&folder.id, endpoint, now)
                    || state.polls.get(&pair).is_some_and(|at| *at > now)
                {
                    continue;
                }
                let poll = self.policy.poll(notified(endpoint));
                state.polls.insert(pair.clone(), now + poll);
                due.demand.insert(pair.clone());
                due.background.insert(pair);
            }
        }
    }

    /// Whether `folder` to `endpoint` has its step in `marks` due and can run
    /// it now. A pair at rest keeps its mark, and on a passive driver so does a
    /// pair no burst wants.
    fn take_due(
        &self,
        state: &PumpState,
        folder: &SyncFolder,
        endpoint: &str,
        marks: impl Fn(&mut Due) -> &mut BTreeSet<Pair>,
    ) -> bool {
        let pair = (endpoint.to_owned(), folder.repo.clone());
        if state.push.rests(&folder.id, endpoint, Instant::now()) {
            return false;
        }
        let mut due = lock(&self.due);
        (!self.passive() || due.in_burst(&pair))
            && !due.push_held(&pair)
            && marks(&mut due).remove(&pair)
    }

    /// [`Self::take_due`] of the background step. For a burst it also says the
    /// last sweep no longer counts, in the same breath so the burst never sees
    /// neither a mark nor a sweep under way, and starts the scan over if an
    /// exchange moved events since.
    fn take_background(&self, state: &mut PumpState, folder: &SyncFolder, endpoint: &str) -> bool {
        let pair = (endpoint.to_owned(), folder.repo.clone());
        if state.push.rests(&folder.id, endpoint, Instant::now()) {
            return false;
        }
        let mut due = lock(&self.due);
        if (self.passive() && !due.in_burst(&pair))
            || due.push_held(&pair)
            || !due.background.remove(&pair)
        {
            return false;
        }
        let mut rescan = false;
        for burst in due.push_pairs(&pair) {
            burst.swept = false;
            rescan |= std::mem::take(&mut burst.rescan);
        }
        drop(due);
        if rescan {
            state.push.restart_scan(&folder.id, endpoint);
        }
        true
    }

    /// A background step ended: what stays due, and what the bursts learn of it.
    /// The pass says whether the pair has more to do ([`push`]). Otherwise it
    /// waits for its next trigger: after a sweep that changed nothing, or a
    /// listing that failed, so a hung listener does not hold this pump again at
    /// once.
    ///
    /// A burst is such a trigger, and it believes only what a pass saw. A pass
    /// that listed nothing says nothing of what is left, and only a sweep that
    /// ran from the top of the scan to its end says it is done: what a page before
    /// it left behind is behind the cursor. Then `remaining` is what this side
    /// holds and knows did not get across, since what it lacks is not its to send.
    /// Short of that it is at least the last page's, and what the sweep has left so
    /// far. The rests of a failed pass
    /// pace asking again, and the wait for a next page of a sweep is cut short.
    fn background_done(&self, pair: &Pair, outcome: &push::PushOutcome) {
        let now = Instant::now();
        let clean = outcome.listed && outcome.error.is_none() && !outcome.interrupted;
        let mut due = lock(&self.due);
        if outcome.interrupted || outcome.again {
            due.background.insert(pair.clone());
        }
        let wanted = due.in_burst(pair);
        let retry = match &outcome.error {
            Some(Failure::Error(_)) => false,
            Some(_) => true,
            None => matches!(
                outcome.unlisted,
                Some(push::Unlisted::NoLane | push::Unlisted::Resting)
            ),
        };
        let next_page = clean && outcome.again;
        let refusal = outcome
            .refusal
            .as_ref()
            .filter(|_| outcome.unsent > 0)
            .map(|code| {
                let unsent = outcome.unsent;
                format!("{code}: {unsent} object(s) this host holds did not get across")
            });
        for state in due.push_pairs(pair) {
            state.pushed += outcome.pushed as u64;
            if outcome.swept {
                state.remaining = Some(outcome.unsent);
            } else if outcome.remaining > 0 || clean {
                let known = outcome.unsent.max(outcome.left);
                state.remaining = Some(outcome.remaining.max(known));
            }
            if outcome.listed {
                state.refusal = refusal.clone();
            }
            state.no_push |= outcome.unlisted == Some(push::Unlisted::NoPush);
            state.swept = outcome.swept;
            if clean {
                state.answered();
            }
            match &outcome.error {
                None => {}
                Some(Failure::NotPublished(error)) => state.note_absent(error.clone(), now),
                Some(Failure::Error(error)) => {
                    state.error.get_or_insert_with(|| error.clone());
                }
                Some(failure) => state.failure = Some(failure.to_string()),
            }
        }
        if wanted && retry {
            due.background.insert(pair.clone());
        }
        drop(due);
        if wanted && next_page {
            self.wake.nudge();
        }
    }

    /// One round, in a fixed order. Pass 1 supplies every folder's pending
    /// demand, then the push pass serves the listeners' demand; pass 2 is the
    /// background `objects.missing` work, then the background push. The push
    /// steps run only for the pairs that are due. Both background passes are
    /// abandoned before their next call as soon as the wake moves past `seen`
    /// (the next round starts with pass 1). Pull's rests hold back only the
    /// pull passes: a failure of one folder's fetch says nothing of another's
    /// push, and what says the lane is unusable rests the push steps by itself
    /// ([`Self::supply`]). The number of objects stored, here or on a listener.
    fn pump_round(&self, state: &mut PumpState, seen: u64) -> usize {
        state.round += 1;
        let round = state.round;
        state.resting.retain(|_, at| *at > round);
        let mut all = self.lanes.bulk_endpoints();
        let peers = self.lanes.ready_peers();
        if all.is_empty() {
            self.mark_polls(state, &all, &peers, &[]);
            return 0;
        }
        let turn = (round as usize) % all.len();
        all.rotate_left(turn);
        let mut endpoints = all.clone();
        endpoints.retain(|endpoint| !state.resting.contains_key(endpoint));
        let folders = self.folders.syncing();
        let syncing = |id: &String| folders.iter().any(|folder| folder.id == *id);
        state.unavailable.retain(|id, _| syncing(id));
        state.scan.retain(|id, _| syncing(id));
        state.batch.retain(|(id, _), _| syncing(id));
        state.folder_resting.retain(|_, at| *at > round);
        self.mark_polls(state, &all, &peers, &folders);
        let now = Instant::now();
        state
            .supplied
            .retain(|_, at| now.saturating_duration_since(*at) < DEMAND_SUPPLIED);
        let mut stored = 0;
        // Pass 1: what readers wait for, then one batch of what they hinted: the
        // next round lists afresh. A digest just written may still be listed until
        // the worker's next `watch`: it is not fetched again.
        for folder in &folders {
            let unsupplied = |listed: &[(Digest, Kind)]| -> Vec<(Digest, Kind)> {
                listed
                    .iter()
                    .filter(|(digest, _)| {
                        !state.supplied.contains_key(&(folder.id.clone(), *digest))
                    })
                    .copied()
                    .collect()
            };
            let demand = unsupplied(&folder.demand);
            let mut hints = unsupplied(&folder.hints);
            hints.truncate(OBJECT_BATCH);
            if (demand.is_empty() && hints.is_empty())
                || endpoints.is_empty()
                || self.cancel.is_cancelled()
            {
                continue;
            }
            // A peer known not to answer is asked last: it may be the only
            // one that holds this, so it is not passed over.
            let mut asked = state.askable(&endpoints, &folder.id);
            asked.sort_by_key(|endpoint| state.push.endpoint_rests(endpoint, now));
            for wanted in [demand, hints] {
                if !wanted.is_empty() {
                    stored += self.supply(state, &asked, folder, wanted, None).stored;
                }
            }
        }
        // What the listeners' readers wait for. A failure needs no mark: their
        // notify repeats while they wait.
        for folder in &folders {
            for endpoint in &all {
                if self.cancel.is_cancelled() {
                    break;
                }
                if self.take_due(state, folder, endpoint, |due| &mut due.demand) {
                    let pushed = self
                        .push(state, endpoint, folder, PushMode::Demand, None)
                        .pushed;
                    stored += pushed;
                    let pair = (endpoint.clone(), folder.repo.clone());
                    for burst in lock(&self.due).push_pairs(&pair) {
                        burst.pushed += pushed as u64;
                    }
                }
            }
        }
        // Pass 2: background prefetch. With every endpoint resting a page would
        // be passed over unasked. A passive driver downloads only what is read.
        for folder in &folders {
            if self.passive()
                || self.cancel.is_cancelled()
                || self.wake.current() != seen
                || endpoints.is_empty()
            {
                break;
            }
            // A peer that rests is not asked for what no reader waits for. With
            // nobody to ask for this folder, its page is not to be passed over.
            let now = Instant::now();
            let mut asked = state.askable(&endpoints, &folder.id);
            asked.retain(|endpoint| !state.push.endpoint_rests(endpoint, now));
            if asked.is_empty() {
                continue;
            }
            let Some((wanted, cursor)) = self.background(state, folder) else {
                continue;
            };
            let supplied = if wanted.is_empty() {
                Supplied::default()
            } else {
                self.supply(state, &asked, folder, wanted, Some(seen))
            };
            stored += supplied.stored;
            if supplied.interrupted {
                break;
            }
            // A page wholly stored is listed afresh from the same position
            // (what was stored is no longer missing); any other moves on, and
            // a scan at its end starts over.
            if supplied.stored < OBJECT_BATCH {
                state.scan.insert(folder.id.clone(), cursor);
            }
        }
        // What the listeners lack.
        if fault::hit(FAULT_BACKGROUND_SKIP).is_some() {
            return stored;
        }
        for folder in &folders {
            for endpoint in &all {
                if self.cancel.is_cancelled() || self.wake.current() != seen {
                    return stored;
                }
                if !self.take_background(state, folder, endpoint) {
                    continue;
                }
                let outcome = self.push(state, endpoint, folder, PushMode::Background, Some(seen));
                stored += outcome.pushed;
                let pair = (endpoint.clone(), folder.repo.clone());
                if outcome.swept {
                    let at = Instant::now() + (outcome.took * SWEEP_PACE).min(SWEEP_PACE_MAX);
                    let poll = state.polls.entry(pair.clone()).or_insert(at);
                    *poll = (*poll).max(at);
                }
                self.background_done(&pair, &outcome);
                if outcome.interrupted {
                    return stored;
                }
            }
        }
        stored
    }

    /// A push step of `folder` to `endpoint`.
    fn push(
        &self,
        state: &mut PumpState,
        endpoint: &String,
        folder: &SyncFolder,
        mode: PushMode,
        interrupt: Option<u64>,
    ) -> push::PushOutcome {
        let outcome = push::run(
            self.lanes.as_ref(),
            &mut state.push,
            std::slice::from_ref(endpoint),
            folder,
            mode,
            interrupt.map(|seen| (self.wake.as_ref(), seen)),
            &self.cancel,
        );
        if let Some(counters) = state.push.counters(&folder.id, endpoint) {
            lock(&self.pushes).insert(
                (endpoint.clone(), folder.repo.clone()),
                (counters.pushed, counters.last_error.clone()),
            );
        }
        outcome
    }

    /// The folder's background work: the next `objects.missing` page of its
    /// scan without the digests no peer had lately (passed as `exclude`, and
    /// filtered here too), with the cursor after that page. `None` when the
    /// worker failed.
    fn background(
        &self,
        state: &mut PumpState,
        folder: &SyncFolder,
    ) -> Option<(Wanted, Option<String>)> {
        let now = Instant::now();
        let unavailable = state.unavailable.entry(folder.id.clone()).or_default();
        let exclude: Vec<Digest> = unavailable
            .iter()
            .filter(|(_, entry)| entry.until > now)
            .map(|(digest, _)| *digest)
            .take(MAX_EXCLUDE)
            .collect();
        let after = state.scan.get(&folder.id).cloned().flatten();
        let (missing, cursor) = match folder
            .link
            .missing(OBJECT_BATCH, &exclude, after.as_deref())
        {
            Ok(page) => {
                state.logged.remove(&folder.id);
                (page.missing, page.cursor)
            }
            Err(error) => {
                let error = format!("objects.missing: {error}");
                if state.logged.get(&folder.id) != Some(&error) {
                    event!(warn, SYNC_MISSING_FAILED, {
                        "folder": folder.id.clone(),
                        "error": error.clone(),
                    });
                    state.logged.insert(folder.id.clone(), error);
                }
                return None;
            }
        };
        // An expired backoff is kept while its object is still missing (the
        // next one doubles); one no longer missing is forgotten.
        let listed: BTreeSet<Digest> = missing.iter().map(|(digest, _)| *digest).collect();
        unavailable.retain(|digest, entry| entry.until > now || listed.contains(digest));
        let wanted = missing
            .into_iter()
            .filter(|(digest, _)| {
                unavailable
                    .get(digest)
                    .is_none_or(|entry| entry.until <= now)
            })
            .collect();
        Some((wanted, cursor))
    }

    /// Fetches `wanted` for `folder`: each endpoint in turn is asked, per
    /// bulk lane and at most [`OBJECT_BATCH`] per call, for what is still
    /// missing, and what arrives is written to the worker. A digest that
    /// every asked peer omitted (none failing) is unavailable for a backoff
    /// (5 s doubling to 5 min). A peer whose lane is unusable sits out the next
    /// round, and one that gave a coded failure does so for this folder alone.
    /// With `interrupt`, stops once the wake moved past it, cancelling the call in
    /// flight: no failure, and what it asked stays missing and is listed again.
    /// Without it a reader is waiting or about to, so a peer gets [`DEMAND_TRY`]
    /// while another could answer, and the last one its whole deadline.
    fn supply(
        &self,
        state: &mut PumpState,
        endpoints: &[String],
        folder: &SyncFolder,
        wanted: Vec<(Digest, Kind)>,
        interrupt: Option<u64>,
    ) -> Supplied {
        let mut remaining = wanted;
        let mut asked = BTreeSet::new();
        let mut failed = BTreeSet::new();
        let mut result = Supplied::default();
        'peers: for (index, endpoint) in endpoints.iter().enumerate() {
            if remaining.is_empty() {
                break;
            }
            let folder_peer = (folder.id.clone(), endpoint.clone());
            if state.resting.contains_key(endpoint)
                || state.folder_resting.contains_key(&folder_peer)
            {
                continue;
            }
            let logged = format!("{endpoint} {}", folder.id);
            let hurried = interrupt.is_none() && index + 1 < endpoints.len();
            for (conn, batch) in lanes_for(self.lanes.as_ref(), endpoint, &remaining) {
                // Objects of unknown size (manifests) first, in their own
                // small batches; then the chunks.
                let mut queue: VecDeque<(Digest, Kind)> = batch
                    .iter()
                    .filter(|(_, kind)| *kind != Kind::Chunk)
                    .chain(batch.iter().filter(|(_, kind)| *kind == Kind::Chunk))
                    .copied()
                    .collect();
                let stop = || {
                    self.cancel.is_cancelled()
                        || interrupt.is_some_and(|seen| self.wake.current() != seen)
                };
                while let Some(&(_, kind)) = queue.front() {
                    if stop() {
                        result.interrupted = true;
                        break 'peers;
                    }
                    let chunks = kind == Kind::Chunk;
                    let size = state.batch_size(&folder.id, endpoint);
                    let limit = size.min(if chunks { OBJECT_BATCH } else { MANIFEST_BATCH });
                    let mut chunk = Vec::with_capacity(limit);
                    while chunk.len() < limit
                        && queue
                            .front()
                            .is_some_and(|(_, next)| (*next == Kind::Chunk) == chunks)
                    {
                        chunk.extend(queue.pop_front());
                    }
                    let mut deadline = (self.fetch_deadline)(&chunk);
                    if hurried {
                        deadline = deadline.min(DEMAND_TRY);
                    }
                    let fetched = match fetch(&conn, &folder.repo, &chunk, deadline, &stop) {
                        Ok(fetched) => {
                            state.grow(&folder.id, endpoint);
                            state.push.answered(endpoint);
                            fetched
                        }
                        Err(failure) if failure.abandoned => {
                            result.interrupted = true;
                            break 'peers;
                        }
                        Err(FetchFailure {
                            failure, timed_out, ..
                        }) => {
                            failed.extend(chunk.iter().map(|(digest, _)| *digest));
                            if timed_out {
                                state.shrink(&folder.id, endpoint, chunk.len());
                            }
                            // The lane is unusable, for the other folders and for
                            // pushes too. A coded failure is this folder's alone.
                            if timed_out || failure == Failure::LaneLost {
                                state.resting.insert(endpoint.clone(), state.round + 2);
                                state.push.rest_endpoint(endpoint);
                            } else {
                                state
                                    .folder_resting
                                    .insert(folder_peer.clone(), state.round + 2);
                            }
                            let error = failure.to_string();
                            if state.logged.get(&logged) != Some(&error) {
                                event!(warn, SYNC_FETCH_FAILED, {
                                    "peer": endpoint.clone(),
                                    "repo": folder.repo.clone(),
                                    "error": error.clone(),
                                    "batch": chunk.len(),
                                });
                                state.logged.insert(logged.clone(), error);
                            }
                            continue 'peers;
                        }
                    };
                    // The peer cut its reply to its byte bounds: what it did
                    // not look at is asked again next. It looked at one object
                    // at least, so the queue always shrinks.
                    let looked = fetched
                        .truncated_at
                        .map_or(chunk.len(), |at| at.clamp(1, chunk.len()));
                    for object in chunk[looked..].iter().rev() {
                        queue.push_front(*object);
                    }
                    let chunk = &chunk[..looked];
                    asked.extend(chunk.iter().map(|(digest, _)| *digest));
                    // Never sent (no reply can hold it): logged once, then
                    // treated as absent like any object the peer lacks.
                    for digest in &fetched.oversized {
                        let key = format!("{}/{digest}", folder.id);
                        state.logged.entry(key).or_insert_with(|| {
                            event!(warn, SYNC_OBJECT_TOO_LARGE, {
                                "peer": endpoint.clone(),
                                "repo": folder.repo.clone(),
                                "digest": digest.to_string(),
                            });
                            String::new()
                        });
                    }
                    // Only what was asked, as what it was asked as: a paired peer
                    // does not get to store a wrong kind here, and the worker takes
                    // ≤ OBJECT_BATCH.
                    let mut objects = fetched.objects;
                    objects.retain(|object| {
                        chunk
                            .iter()
                            .any(|(d, kind)| *d == object.digest && *kind == object.kind)
                    });
                    if objects.is_empty() {
                        continue;
                    }
                    let received: BTreeSet<Digest> = objects.iter().map(|o| o.digest).collect();
                    match folder.link.write(objects) {
                        Ok(count) => {
                            result.stored += count;
                            state.logged.remove(&logged);
                            let now = Instant::now();
                            for digest in &received {
                                state.supplied.insert((folder.id.clone(), *digest), now);
                            }
                            if let Some(unavailable) = state.unavailable.get_mut(&folder.id) {
                                unavailable.retain(|digest, _| !received.contains(digest));
                            }
                            remaining.retain(|(digest, _)| !received.contains(digest));
                        }
                        Err(error) => {
                            event!(warn, SYNC_WRITE_FAILED, {
                                "folder": folder.id.clone(),
                                "error": error.to_string(),
                            });
                            result.interrupted = true;
                            break 'peers;
                        }
                    }
                }
            }
        }
        if !result.interrupted {
            let now = Instant::now();
            let unavailable = state.unavailable.entry(folder.id.clone()).or_default();
            for (digest, _) in &remaining {
                if asked.contains(digest) && !failed.contains(digest) {
                    let backoff = unavailable
                        .get(digest)
                        .map_or(UNAVAILABLE_FIRST, |entry| entry.backoff * 2)
                        .min(UNAVAILABLE_MAX);
                    unavailable.insert(
                        *digest,
                        Unavailable {
                            until: now + backoff,
                            backoff,
                        },
                    );
                }
            }
        }
        result
    }
}

impl NotifySink for Driver {
    /// Runs on the lane's I/O thread: only marks and bumps. A peer picks the name,
    /// so one of no folder here is ignored and costs nothing. A demand notify
    /// repeats while its reader waits: only a new mark looks any further, to stop
    /// background work, and a repeat rouses the pump, in case it sleeps on a mark
    /// it could not take. A passive driver did not ask for hints, and the other
    /// side does not get to wake it.
    fn notified(&self, endpoint: &str, notify: &Notify) {
        if self.passive() || !self.folders.knows(&notify.repo) {
            return;
        }
        let pair = (endpoint.to_owned(), notify.repo.clone());
        match notify.reason {
            NotifyReason::Metadata => {
                lock(&self.due).exchange.insert(pair);
                self.poke.bump();
            }
            NotifyReason::Demand => {
                let new = lock(&self.due).demand.insert(pair);
                let here = new
                    && self
                        .folders
                        .syncing()
                        .iter()
                        .any(|folder| folder.repo == notify.repo);
                if here {
                    self.wake.bump();
                } else {
                    // A repeat, or not syncing now, which may be a slot that is
                    // only busy: the pump looks again, and nothing is interrupted.
                    self.wake.nudge();
                }
            }
        }
    }
}

/// Whether nothing more will happen for `state` of `pair`: it is given up on, or
/// it has nothing running or due and its sweep found nothing missing, or its peer
/// takes no pushes.
fn burst_pair_settled(
    jobs: &BTreeMap<Pair, Slot>,
    due: &Due,
    pair: &Pair,
    state: &BurstPair,
) -> bool {
    state.error.is_some()
        || state.not_published.is_some()
        || (state.metadata_only
            && state.exchanged > 0
            && state.absent.is_none()
            && !due.exchange.contains(pair)
            && !jobs.get(pair).is_some_and(|slot| slot.running))
        || (!state.metadata_only
            && (state.swept || state.no_push)
            && state.absent.is_none()
            && !due.exchange.contains(pair)
            && !due.demand.contains(pair)
            && !due.background.contains(pair)
            && !jobs.get(pair).is_some_and(|slot| slot.running))
}

/// What to report of a burst stopped by `failure` for `folder`: the last failure
/// of a pair that did not finish, if one was being retried.
fn last_failure(
    jobs: &BTreeMap<Pair, Slot>,
    due: &Due,
    burst: u64,
    folder: &BurstFolder,
    failure: &str,
) -> String {
    due.bursts
        .get(&burst)
        .into_iter()
        .flat_map(|burst| &burst.pairs)
        .filter(|((_, repo), _)| Some(repo) == folder.repo.as_ref())
        .filter(|(pair, state)| !burst_pair_settled(jobs, due, pair, state))
        .find_map(|((endpoint, _), state)| {
            let failure = state.failure.as_ref()?;
            Some(format!("{failure} (peer {endpoint})"))
        })
        .unwrap_or_else(|| failure.to_owned())
}

/// Whether nothing more will happen for `folder` in `burst`: it is given up on,
/// or each of its pairs is settled. Take `jobs` before `due`, as `dispatch` does,
/// so a job that just took its mark shows as running.
fn burst_folder_settled(
    jobs: &BTreeMap<Pair, Slot>,
    due: &Due,
    burst: u64,
    folder: &BurstFolder,
) -> bool {
    if folder.error.is_some() {
        return true;
    }
    let (Some(repo), Some(burst)) = (&folder.repo, due.bursts.get(&burst)) else {
        return false;
    };
    burst
        .pairs
        .iter()
        .filter(|((_, of), _)| of == repo)
        .all(|(pair, state)| burst_pair_settled(jobs, due, pair, state))
}

/// `{id, exchanged, pushed, remaining, error}` of `folder`, over its pairs in
/// `burst`. `unfinished` is the error of a burst stopped before it was done.
fn burst_report(burst: Option<&Burst>, folder: &BurstFolder, unfinished: Option<String>) -> Value {
    let pairs: Vec<(&Pair, &BurstPair)> = burst
        .iter()
        .flat_map(|burst| &burst.pairs)
        .filter(|((_, repo), _)| Some(repo) == folder.repo.as_ref())
        .collect();
    let with_peer = |endpoint: &String, error: &str| format!("{error} (peer {endpoint})");
    let (lacking, took): (Vec<_>, Vec<_>) = pairs
        .iter()
        .partition(|(_, state)| state.not_published.is_some());
    let error = folder.error.clone().or_else(|| {
        took.iter()
            .find_map(|((endpoint, _), state)| Some(with_peer(endpoint, state.reason()?)))
            .or(unfinished)
            // A peer without the folder matters only if no peer took it.
            .or_else(|| {
                took.is_empty()
                    .then(|| {
                        lacking.first().and_then(|((endpoint, _), state)| {
                            Some(with_peer(endpoint, state.not_published.as_deref()?))
                        })
                    })
                    .flatten()
            })
    });
    // Unknown only if some peer was asked and none answered.
    let remaining = pairs
        .iter()
        .filter_map(|(_, state)| state.remaining)
        .reduce(|sum, left| sum + left)
        .or(pairs.is_empty().then_some(0));
    json!({
        "id": folder.id,
        "exchanged": pairs.iter().map(|(_, state)| state.exchanged).sum::<u64>(),
        "pushed": pairs.iter().map(|(_, state)| state.pushed).sum::<u64>(),
        "remaining": remaining,
        "error": error,
    })
}

/// `wanted` grouped by the Ready bulk lane of `endpoint` that owns each
/// digest (digests no Ready lane owns are left out).
pub(super) fn lanes_for(
    lanes: &dyn Lanes,
    endpoint: &str,
    wanted: &[(Digest, Kind)],
) -> Vec<(Conn, Wanted)> {
    let mut owners: BTreeMap<String, (Conn, Wanted)> = BTreeMap::new();
    for object in wanted {
        if let Some(conn) = lanes.bulk_lane(endpoint, &object.0) {
            owners
                .entry(conn.peer().session.clone())
                .or_insert_with(|| (conn, Vec::new()))
                .1
                .push(*object);
        }
    }
    owners.into_values().collect()
}

/// One exchange of `repo` with the peer behind `conn`, repeated while either
/// side has more. `cursor` advances only past pages both sides took. With
/// `keeping`, a passive dialer's, it may be a saved one: it starts over if a side's
/// journal or top says it no longer stands, and each page taken is durable on both
/// sides before it is saved.
fn exchange(
    link: &WorkerLink,
    conn: &Conn,
    repo: &str,
    cursor: &mut FeedCursor,
    mut keeping: Option<Keeping<'_>>,
    cancel: &Cancel,
) -> Result<(), Failure> {
    let (exchange_header, apply_header) = if keeping.is_some() {
        (
            json!({"repo": repo, "durable": true}),
            json!({"durable": true}),
        )
    } else {
        (json!({"repo": repo}), json!({}))
    };
    loop {
        if cancel.is_cancelled() || conn.closed().is_some() {
            return Err(Failure::LaneLost);
        }
        let page: FeedPage = link
            .call_json("feed.outgoing", json!({"after": cursor.sent}))
            .map_err(|error| worker_failure("feed.outgoing", error))
            .and_then(|value| {
                serde_json::from_value(value).map_err(|e| worker_failure("feed.outgoing", e))
            })?;
        if let Some(keeping) = &mut keeping
            && *cursor != FeedCursor::default()
            && !cursor_stands(
                &keeping.journals.local,
                cursor.sent,
                &page.journal,
                page.top,
            )
        {
            keeping.start_over(cursor, repo, "local");
            continue;
        }
        let more = page.more;
        let request = FeedRequest {
            after: cursor.received,
            events: page.events,
        };
        let body =
            serde_json::to_vec(&request).map_err(|error| worker_failure("meta.exchange", error))?;
        let reply = conn
            .call_cancellable(
                "meta.exchange",
                exchange_header.clone(),
                body,
                Priority::Control,
                PEER_DEADLINE,
                &|| cancel.is_cancelled(),
            )
            .map_err(|error| peer_failure("meta.exchange", error))?;
        let reply: FeedReply = serde_json::from_slice(&reply.body)
            .map_err(|error| Failure::Error(format!("meta.exchange: malformed reply: {error}")))?;
        if let Some(keeping) = &mut keeping
            && *cursor != FeedCursor::default()
            && !cursor_stands(
                &keeping.journals.peer,
                cursor.received,
                &reply.journal,
                reply.top,
            )
        {
            keeping.start_over(cursor, repo, "peer");
            continue;
        }
        // A saved cursor passes this page's own events as well: `sent` does, and so
        // does `received` where the peer left out their echo. Applied again they
        // change nothing, but a durable apply puts them on disk here too.
        let to_apply = if reply.events.is_empty() && keeping.is_some() {
            &request.events
        } else {
            &reply.events
        };
        if !to_apply.is_empty() {
            let events = serde_json::to_vec(to_apply)
                .map_err(|error| worker_failure("feed.apply", error))?;
            link.call("feed.apply", apply_header.clone(), events, CALL_DEADLINE)
                .map_err(|error| worker_failure("feed.apply", error))?;
        }
        let before = *cursor;
        cursor.sent = page.cursor;
        cursor.received = reply.cursor;
        if let Some(keeping) = &mut keeping {
            let journals = Journals {
                local: page.journal,
                peer: reply.journal,
            };
            keeping.page_taken(before, *cursor, journals);
        }
        if !(more || reply.more) {
            return Ok(());
        }
    }
}

/// Why a fetch failed, and whether its deadline passed or its caller gave it up.
pub(super) struct FetchFailure {
    pub(super) failure: Failure,
    pub(super) timed_out: bool,
    /// A stop cancelled it: not a failure of the peer. Nothing rests or shrinks.
    pub(super) abandoned: bool,
}

impl From<Failure> for FetchFailure {
    fn from(failure: Failure) -> Self {
        Self {
            failure,
            timed_out: false,
            abandoned: false,
        }
    }
}

/// The deadline of an `objects.fetch` of `wanted`: [`PEER_DEADLINE`] plus
/// 1 s per 256 KiB requested (a chunk counts 64 KiB, an object of unknown
/// size such as a manifest 1 MiB), at most [`FETCH_DEADLINE_MAX`].
pub fn fetch_deadline(wanted: &[(Digest, Kind)]) -> Duration {
    let requested: u64 = wanted
        .iter()
        .map(|(_, kind)| match kind {
            Kind::Chunk => CHUNK_GUESS,
            _ => UNKNOWN_GUESS,
        })
        .sum();
    let extra = Duration::from_millis(requested.saturating_mul(1000) / DEADLINE_STEP_BYTES);
    (PEER_DEADLINE + extra).min(FETCH_DEADLINE_MAX)
}

/// `objects.fetch` of ≤ [`OBJECT_BATCH`] objects on a peer bulk lane, within
/// `deadline`, until `stop`.
fn fetch(
    conn: &Conn,
    repo: &str,
    wanted: &[(Digest, Kind)],
    deadline: Duration,
    stop: &dyn Fn() -> bool,
) -> Result<Fetched, FetchFailure> {
    let body = serde_json::to_vec(wanted).map_err(|e| worker_failure("objects.fetch", e))?;
    let reply = conn
        .call_cancellable(
            "objects.fetch",
            json!({"repo": repo}),
            body,
            Priority::Data,
            deadline,
            stop,
        )
        .map_err(|error| FetchFailure {
            timed_out: matches!(error, CallError::Timeout { .. }),
            abandoned: matches!(error, CallError::Cancelled),
            failure: peer_failure("objects.fetch", error),
        })?;
    Fetched::from_reply(&reply.header, &reply.body).map_err(|error| {
        Failure::Error(format!("objects.fetch: malformed reply: {error:#}")).into()
    })
}

/// One clock probe on a control lane: stores the smoothed offset of
/// `installation` and warns past [`WARN_OFFSET_MS`].
fn probe(catalog: &Catalog, installation: &str, conn: &Conn, cancel: &Cancel) -> Result<Offset> {
    let t1 = now_ms();
    let reply = conn
        .call_cancellable(
            "clock.probe",
            json!({"t1": t1}),
            Vec::new(),
            Priority::Control,
            PEER_DEADLINE,
            &|| cancel.is_cancelled(),
        )
        .map_err(|error| anyhow::anyhow!("clock.probe: {error}"))?;
    let t4 = now_ms();
    let times: Value = serde_json::from_slice(&reply.body).context("clock.probe reply")?;
    let time = |key: &str| {
        times[key]
            .as_u64()
            .with_context(|| format!("clock.probe: no {key}"))
    };
    let sample = Sample {
        t1,
        t2: time("t2")?,
        t3: time("t3")?,
        t4,
    }
    .offset();
    let previous = catalog
        .clock(installation)?
        .map(|(offset, _)| offset.offset_ms);
    let offset = Offset {
        offset_ms: clock::smooth(previous, sample.offset_ms),
        rtt_ms: sample.rtt_ms,
    };
    catalog.set_clock(installation, offset, t4)?;
    if offset.offset_ms.abs() > WARN_OFFSET_MS {
        event!(warn, CLOCK_OFFSET_LARGE, {
            "installation": installation,
            "offset_ms": offset.offset_ms,
            "rtt_ms": offset.rtt_ms,
        });
    }
    Ok(offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::{
        network::{Refused, StoreReply},
        sync::{FEED_PAGE, MissingPage, Object, Service, WantedPage},
    };
    use crate::{
        core::EntryKind,
        model::{
            errors::ErrorWire,
            protocol::{LaneRole, features},
        },
        runtime::Engine,
        session::{ConnOptions, Handler, Incoming, PeerInfo, Side},
    };
    use std::{
        net::{TcpListener, TcpStream},
        sync::OnceLock,
    };
    use tungstenite::stream::MaybeTlsStream;

    #[test]
    fn the_driver_bounds_match_the_worker_and_the_brief() {
        assert_eq!(MAX_EXCHANGES, 4);
        assert_eq!(OBJECT_BATCH, 32);
        assert_eq!(CLOCK_PROBE_EVERY, 60);
    }

    #[test]
    fn a_lost_lane_is_not_a_repo_error() {
        for error in [
            CallError::ConnectionLost("x".into()),
            CallError::Unavailable("x".into()),
            CallError::Cancelled,
        ] {
            assert_eq!(peer_failure("meta.exchange", error), Failure::LaneLost);
        }
        let remote = peer_failure(
            "meta.exchange",
            CallError::Remote(ErrorWire::coded("REPO_NOT_PUBLISHED", "paused")),
        );
        assert!(
            matches!(&remote, Failure::NotPublished(e) if e.contains("REPO_NOT_PUBLISHED")),
            "{remote:?}"
        );
        assert!(matches!(
            peer_failure("objects.fetch", CallError::Timeout { after_ms: 1 }),
            Failure::TimedOut(_)
        ));
        assert!(matches!(
            peer_failure(
                "objects.fetch",
                CallError::Remote(ErrorWire::coded("OP_NOT_ALLOWED", "no"))
            ),
            Failure::Error(_)
        ));
    }

    /// Answers `clock.probe` as a peer whose clock runs `skew_ms` ahead.
    struct SkewedPeer {
        skew_ms: Mutex<i64>,
    }

    impl Handler for SkewedPeer {
        fn handle(&self, req: Incoming) {
            let t1 = req.header["t1"].as_u64().unwrap();
            let peer_now = (now_ms() as i64 + *lock(&self.skew_ms)) as u64;
            let body = json!({"t1": t1, "t2": peer_now, "t3": peer_now + 1});
            req.responder
                .reply(json!({}), serde_json::to_vec(&body).unwrap());
        }
    }

    fn info_as(role: LaneRole) -> PeerInfo {
        PeerInfo {
            installation: "peer-b".into(),
            role,
            lane: None,
            session: "session".into(),
            verifier_generation: 1,
            features: Vec::new(),
        }
    }

    /// A client `Conn` whose server side is served by `handler`.
    fn served(handler: Arc<dyn Handler>) -> (Conn, Conn) {
        served_as(LaneRole::Control, handler)
    }

    /// [`served`] as a session of `role`.
    fn served_as(role: LaneRole, handler: Arc<dyn Handler>) -> (Conn, Conn) {
        served_info(info_as(role), handler)
    }

    /// [`served`] with the client seeing `info`.
    fn served_info(info: PeerInfo, handler: Arc<dyn Handler>) -> (Conn, Conn) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            tungstenite::accept(MaybeTlsStream::Plain(stream)).unwrap()
        });
        let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        let client = tungstenite::client(
            format!("ws://127.0.0.1:{port}/"),
            MaybeTlsStream::Plain(stream),
        )
        .unwrap()
        .0;
        let role = info.role;
        let client =
            Conn::from_established(client, Side::Client, info, ConnOptions::default()).unwrap();
        let server = Conn::from_established(
            server.join().unwrap(),
            Side::Server,
            info_as(role),
            ConnOptions::default(),
        )
        .unwrap();
        server.serve(handler);
        (client, server)
    }

    #[test]
    fn a_probe_stores_the_smoothed_offset_and_the_round_trip() {
        let root = tempfile::tempdir().unwrap();
        let catalog = Catalog::open(root.path()).unwrap();
        let peer = Arc::new(SkewedPeer {
            skew_ms: Mutex::new(8000),
        });
        let (client, server) = served(peer.clone());
        let cancel = Cancel::new();
        let before = now_ms();
        // The first sample is taken as it is (and is past WARN_OFFSET_MS).
        let first = probe(&catalog, "peer-b", &client, &cancel).unwrap();
        assert!((first.offset_ms - 8000).abs() <= 1000, "{first:?}");
        assert!(first.rtt_ms < 1000, "{first:?}");
        let (stored, measured_at) = catalog.clock("peer-b").unwrap().unwrap();
        assert_eq!(stored, first);
        assert!(measured_at >= before);
        // The next moves a quarter of the way toward the new sample.
        *lock(&peer.skew_ms) = 0;
        let second = probe(&catalog, "peer-b", &client, &cancel).unwrap();
        assert!((second.offset_ms - 6000).abs() <= 1000, "{second:?}");
        assert_eq!(catalog.clock("peer-b").unwrap().unwrap().0, second);
        client.close();
        server.close();
    }

    // ---- pump ------------------------------------------------------------

    const TOKEN: &str = "pump-test-token";

    struct FnHandler<F>(F);

    impl<F: Fn(Incoming) + Send + Sync> Handler for FnHandler<F> {
        fn handle(&self, req: Incoming) {
            (self.0)(req)
        }
    }

    /// `count` distinct objects (bytes and digest).
    fn objects(tag: &str, count: usize) -> Vec<Object> {
        (0..count)
            .map(|i| {
                let bytes = format!("{tag}-{i}").into_bytes();
                Object {
                    digest: Digest::bytes(&bytes),
                    kind: Kind::Chunk,
                    bytes,
                }
            })
            .collect()
    }

    fn wanted_of(objects: &[Object]) -> Vec<(Digest, Kind)> {
        objects.iter().map(|o| (o.digest, o.kind)).collect()
    }

    /// A worker standing in for one folder: `objects.missing {limit,
    /// exclude}` lists `missing` in order minus what was written and what is
    /// excluded; `objects.write` records each digest with its time.
    #[derive(Default)]
    struct FakeFolder {
        missing: Vec<(Digest, Kind)>,
        written: Mutex<Vec<(Digest, Instant)>>,
        excludes: Mutex<Vec<usize>>,
        /// What `objects.fetch` reads, as the worker does: in request order,
        /// absent ones left out, with the kind asked.
        held: Vec<Object>,
        /// `objects.fetch` stops after this many objects (`truncated_at`).
        read_cut: Option<usize>,
        /// Bumped by every `objects.fetch`, as a reader's demand arriving
        /// during the read.
        wake_on_read: Option<Arc<Seq>>,
        /// Runs during every `objects.fetch`.
        during_read: Option<Hook>,
        /// What `objects.fetch` leaves out as too large for any reply.
        oversized: Vec<Digest>,
    }

    /// Something that happens while a request is being served.
    type Hook = Box<dyn Fn() + Send + Sync>;

    impl FakeFolder {
        fn written(&self) -> Vec<Digest> {
            lock(&self.written)
                .iter()
                .map(|(digest, _)| *digest)
                .collect()
        }

        fn written_at(&self, digest: &Digest) -> Option<Instant> {
            lock(&self.written)
                .iter()
                .find(|(d, _)| d == digest)
                .map(|(_, at)| *at)
        }

        fn serve(&self, req: Incoming) {
            let reply = match req.op.as_str() {
                "objects.missing" => {
                    let limit = req.header["limit"].as_u64().unwrap() as usize;
                    let exclude: BTreeSet<Digest> =
                        serde_json::from_value(req.header["exclude"].clone()).unwrap();
                    lock(&self.excludes).push(exclude.len());
                    let start: usize = req.header["after"]
                        .as_str()
                        .map_or(0, |after| after.parse().unwrap());
                    let written = self.written();
                    let mut page = MissingPage::default();
                    for (index, object) in self.missing.iter().enumerate().skip(start) {
                        if !exclude.contains(&object.0) && !written.contains(&object.0) {
                            page.missing.push(*object);
                        }
                        if page.missing.len() == limit {
                            page.cursor = Some((index + 1).to_string());
                            break;
                        }
                    }
                    serde_json::to_vec(&page).unwrap()
                }
                "objects.write" => {
                    let objects: Vec<Object> = serde_json::from_slice(&req.body).unwrap();
                    let now = Instant::now();
                    lock(&self.written).extend(objects.iter().map(|o| (o.digest, now)));
                    serde_json::to_vec(&json!({"stored": objects.len()})).unwrap()
                }
                "objects.fetch" => {
                    if let Some(wake) = &self.wake_on_read {
                        wake.bump();
                    }
                    if let Some(hook) = &self.during_read {
                        hook();
                    }
                    let wanted: Vec<(Digest, Kind)> = serde_json::from_slice(&req.body).unwrap();
                    let mut fetched = Fetched::default();
                    for (index, (digest, kind)) in wanted.into_iter().enumerate() {
                        if self.oversized.contains(&digest) {
                            fetched.oversized.push(digest);
                            continue;
                        }
                        let Some(object) = self.held.iter().find(|o| o.digest == digest) else {
                            continue;
                        };
                        if self.read_cut == Some(fetched.objects.len()) {
                            fetched.truncated_at = Some(index);
                            break;
                        }
                        fetched.objects.push(Object {
                            kind,
                            ..object.clone()
                        });
                    }
                    let body = serde_json::to_vec(&fetched.objects).unwrap();
                    req.responder.reply(fetched.header(), body);
                    return;
                }
                op => {
                    req.responder
                        .error(ErrorWire::coded("UNSUPPORTED_OPERATION", op));
                    return;
                }
            };
            req.responder.reply(json!({}), reply);
        }
    }

    /// A loopback worker serving `folder`, and its link.
    fn fake_worker(folder: Arc<FakeFolder>) -> Arc<WorkerLink> {
        worker_serving(Arc::new(FnHandler(move |req: Incoming| folder.serve(req))))
    }

    /// A loopback worker whose every request goes to `handler`, and its link.
    fn worker_serving(handler: Arc<dyn Handler>) -> Arc<WorkerLink> {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("ws://{}", listener.local_addr().unwrap());
        thread::spawn(move || {
            let mut conns = Vec::new();
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                if let Ok(conn) = crate::session::accept_loopback(
                    stream,
                    Some(TOKEN),
                    ConnOptions::for_role(LaneRole::Control),
                ) {
                    conn.serve(handler.clone());
                    conns.push(conn);
                }
            }
        });
        let ready = super::super::worker::Ready {
            ready: true,
            repo: "repo-1".into(),
            device: "device-1".into(),
            instance: "instance-1".into(),
            pid: 1,
            address,
            token: TOKEN.into(),
            version: "test".into(),
            ipc: 1,
            mounts: Vec::new(),
        };
        WorkerLink::connect(&ready, 2).unwrap()
    }

    /// A peer bulk lane that holds `held`; a fetch naming one of `slow`
    /// takes `delay`, and so does one of more than `stall_above` objects.
    /// With `cut`, a reply carries at most that many objects and names the
    /// first digest not looked at (`truncated_at`), as a peer at its byte
    /// bounds does. Counts the fetches and records each request's kinds.
    struct Peer {
        held: BTreeMap<Digest, Object>,
        slow: BTreeSet<Digest>,
        delay: Duration,
        stall_above: Option<usize>,
        cut: Option<usize>,
        fetches: Mutex<u32>,
        requests: Mutex<Vec<Vec<Kind>>>,
    }

    impl Peer {
        fn new(held: &[Object], slow: &[Object], delay: Duration) -> Arc<Self> {
            Arc::new(Self::plain(held, slow, delay))
        }

        fn plain(held: &[Object], slow: &[Object], delay: Duration) -> Self {
            Self {
                held: held.iter().map(|o| (o.digest, o.clone())).collect(),
                slow: slow.iter().map(|o| o.digest).collect(),
                delay,
                stall_above: None,
                cut: None,
                fetches: Mutex::new(0),
                requests: Mutex::new(Vec::new()),
            }
        }

        fn lane(self: &Arc<Self>) -> Conn {
            let peer = self.clone();
            let (client, server) = served_as(
                LaneRole::Bulk,
                Arc::new(FnHandler(move |req: Incoming| {
                    *lock(&peer.fetches) += 1;
                    let wanted: Vec<(Digest, Kind)> = serde_json::from_slice(&req.body).unwrap();
                    lock(&peer.requests).push(wanted.iter().map(|(_, kind)| *kind).collect());
                    if wanted.iter().any(|(d, _)| peer.slow.contains(d))
                        || peer.stall_above.is_some_and(|most| wanted.len() > most)
                    {
                        thread::sleep(peer.delay);
                    }
                    let mut header = json!({});
                    let mut objects: Vec<&Object> = Vec::new();
                    for (index, (digest, _)) in wanted.iter().enumerate() {
                        let Some(object) = peer.held.get(digest) else {
                            continue;
                        };
                        if peer.cut.is_some_and(|most| objects.len() == most) {
                            header = json!({"truncated_at": index});
                            break;
                        }
                        objects.push(object);
                    }
                    req.responder
                        .reply(header, serde_json::to_vec(&objects).unwrap());
                })),
            );
            std::mem::forget(server);
            client
        }
    }

    /// One peer endpoint whose every digest goes to `conn`.
    struct OneLane(Conn);

    impl Lanes for OneLane {
        fn ready_peers(&self) -> Vec<ReadyPeer> {
            Vec::new()
        }
        fn bulk_endpoints(&self) -> Vec<String> {
            vec!["ws://peer".into()]
        }
        fn bulk_lane(&self, _: &str, _: &Digest) -> Option<Conn> {
            Some(self.0.clone())
        }
        fn any_bulk_lane(&self, _: &str) -> Option<Conn> {
            Some(self.0.clone())
        }
    }

    /// Running folders with settable demand.
    struct Folders(Vec<(String, Arc<WorkerLink>, Mutex<Wanted>)>);

    impl SyncFolders for Folders {
        fn syncing(&self) -> Vec<SyncFolder> {
            self.0
                .iter()
                .map(|(id, link, demand)| SyncFolder {
                    id: id.clone(),
                    repo: "repo-1".into(),
                    link: link.clone(),
                    demand: lock(demand).clone(),
                    hints: Vec::new(),
                })
                .collect()
        }
        fn knows(&self, repo: &str) -> bool {
            repo == "repo-1"
        }
    }

    fn driver(
        lanes: OneLane,
        folders: Arc<dyn SyncFolders>,
        wake: Arc<Seq>,
    ) -> (Arc<Driver>, tempfile::TempDir) {
        let root = tempfile::tempdir().unwrap();
        let catalog = Catalog::open(root.path()).unwrap();
        let driver = Driver::new(
            Arc::new(lanes),
            catalog,
            folders,
            wake,
            Duration::from_millis(50),
            Cancel::new(),
        );
        (driver, root)
    }

    #[test]
    fn the_pump_moves_past_objects_no_peer_has_within_two_rounds() {
        // 40 missing; the peer has none of the first 32 (the whole first
        // `objects.missing` page) and all of the last 8.
        let all = objects("object", 40);
        let folder = Arc::new(FakeFolder {
            missing: wanted_of(&all),
            ..FakeFolder::default()
        });
        let link = fake_worker(folder.clone());
        let peer = Peer::new(&all[32..], &[], Duration::ZERO);
        let folders = Arc::new(Folders(vec![("f1".into(), link, Mutex::new(Vec::new()))]));
        let (driver, _root) = driver(OneLane(peer.lane()), folders, Seq::new());
        let mut state = PumpState::default();
        let seen = driver.wake.current();
        assert_eq!(driver.pump_round(&mut state, seen), 0);
        let unavailable = &state.unavailable["f1"];
        assert_eq!(unavailable.len(), 32);
        assert!(unavailable.values().all(|u| u.backoff == UNAVAILABLE_FIRST));
        assert_eq!(driver.pump_round(&mut state, seen), 8);
        let expected: Vec<Digest> = all[32..].iter().map(|o| o.digest).collect();
        assert_eq!(folder.written(), expected);
        // The second `objects.missing` excluded the 32 no peer had.
        assert_eq!(lock(&folder.excludes).as_slice(), &[0, 32]);
        // An object asked again after its backoff and still absent doubles it.
        for entry in state.unavailable.get_mut("f1").unwrap().values_mut() {
            entry.until = Instant::now();
        }
        assert_eq!(driver.pump_round(&mut state, seen), 0);
        assert!(
            state.unavailable["f1"]
                .values()
                .all(|u| u.backoff == UNAVAILABLE_FIRST * 2 && u.until > Instant::now())
        );
    }

    #[test]
    fn the_scan_pages_past_a_long_run_no_peer_has() {
        // 300 head digests no peer has (more than MAX_EXCLUDE + one page),
        // then one the peer has.
        let all = objects("head", 301);
        let folder = Arc::new(FakeFolder {
            missing: wanted_of(&all),
            ..FakeFolder::default()
        });
        let link = fake_worker(folder.clone());
        let peer = Peer::new(&all[300..], &[], Duration::ZERO);
        let folders = Arc::new(Folders(vec![("f1".into(), link, Mutex::new(Vec::new()))]));
        let (driver, _root) = driver(OneLane(peer.lane()), folders, Seq::new());
        let mut state = PumpState::default();
        let seen = driver.wake.current();
        let bound = 300_usize.div_ceil(OBJECT_BATCH) + 2;
        let mut rounds = 0;
        while folder.written().is_empty() {
            rounds += 1;
            assert!(
                rounds <= bound,
                "the 301st object not fetched in {bound} rounds"
            );
            driver.pump_round(&mut state, seen);
        }
        assert_eq!(folder.written(), vec![all[300].digest]);
        assert!(
            rounds <= 300_usize.div_ceil(OBJECT_BATCH) + 1,
            "{rounds} rounds"
        );
    }

    #[test]
    fn demand_is_served_before_background_batches_of_other_folders() {
        // Folder 1: 64 objects behind slow fetches (two background rounds);
        // folder 3: 32 more; folder 2: nothing missing, one demanded object.
        let first = objects("first", 64);
        let third = objects("third", 32);
        let demanded = objects("demanded", 1);
        let delay = Duration::from_millis(500);
        let held: Vec<Object> = first
            .iter()
            .chain(&third)
            .chain(&demanded)
            .cloned()
            .collect();
        let slow: Vec<Object> = first.iter().chain(&third).cloned().collect();
        let peer = Peer::new(&held, &slow, delay);
        let fakes: Vec<Arc<FakeFolder>> = [wanted_of(&first), Vec::new(), wanted_of(&third)]
            .into_iter()
            .map(|missing| {
                Arc::new(FakeFolder {
                    missing,
                    ..FakeFolder::default()
                })
            })
            .collect();
        let folders = Arc::new(Folders(
            fakes
                .iter()
                .enumerate()
                .map(|(i, fake)| {
                    (
                        format!("f{}", i + 1),
                        fake_worker(fake.clone()),
                        Mutex::new(Vec::new()),
                    )
                })
                .collect(),
        ));
        let wake = Seq::new();
        let root = tempfile::tempdir().unwrap();
        let driver = start(
            Arc::new(OneLane(peer.lane())),
            Catalog::open(root.path()).unwrap(),
            folders.clone(),
            wake.clone(),
            active(Duration::from_millis(50), Duration::from_millis(50)),
            Cancel::new(),
        )
        .unwrap();
        // Folder 1's first background batch is in flight: a reader of
        // folder 2 starts waiting.
        let end = Instant::now() + Duration::from_secs(10);
        while *lock(&peer.fetches) == 0 {
            assert!(Instant::now() < end, "no background fetch started");
            thread::sleep(Duration::from_millis(5));
        }
        *lock(&folders.0[1].2) = wanted_of(&demanded);
        wake.bump();
        while fakes[0].written().len() < 64 || fakes[2].written().len() < 32 {
            assert!(
                Instant::now() < end,
                "not everything written: {} / {} / {}",
                fakes[0].written().len(),
                fakes[1].written().len(),
                fakes[2].written().len()
            );
            thread::sleep(Duration::from_millis(10));
        }
        driver.stop(Duration::from_secs(2));
        let served = fakes[1]
            .written_at(&demanded[0].digest)
            .expect("the demanded object was never written");
        let last_first = fakes[0].written_at(&first[63].digest).unwrap();
        let first_third = fakes[2].written_at(&third[0].digest).unwrap();
        assert!(
            served < last_first,
            "demand waited for folder 1's second background batch"
        );
        assert!(
            served < first_third,
            "demand waited for folder 3's background batch"
        );
    }

    #[test]
    fn the_fetch_deadline_grows_with_what_is_asked_and_is_capped() {
        let chunk = |n: u8| (Digest([n; 32]), Kind::Chunk);
        let manifest = |n: u8| (Digest([n; 32]), Kind::Manifest);
        assert_eq!(fetch_deadline(&[]), PEER_DEADLINE);
        // 1 s per 256 KiB: a 64 KiB chunk adds 250 ms…
        assert_eq!(
            fetch_deadline(&[chunk(1)]),
            PEER_DEADLINE + Duration::from_millis(250)
        );
        // …a full chunk batch (2 MiB) 8 s…
        let chunks: Vec<_> = (0..OBJECT_BATCH as u8).map(chunk).collect();
        assert_eq!(
            fetch_deadline(&chunks),
            PEER_DEADLINE + Duration::from_secs(8)
        );
        // …a manifest (size unknown, taken as 1 MiB) 4 s.
        let manifests: Vec<_> = (0..MANIFEST_BATCH as u8).map(manifest).collect();
        assert_eq!(
            fetch_deadline(&manifests),
            PEER_DEADLINE + Duration::from_secs(16)
        );
        // At most FETCH_DEADLINE_MAX.
        let many: Vec<_> = (0..=255).map(manifest).collect();
        assert_eq!(fetch_deadline(&many), FETCH_DEADLINE_MAX);
        assert_eq!(FETCH_DEADLINE_MAX, Duration::from_secs(120));
    }

    /// `count` distinct objects of kind Manifest.
    fn manifests(tag: &str, count: usize) -> Vec<Object> {
        objects(tag, count)
            .into_iter()
            .map(|object| Object {
                kind: Kind::Manifest,
                ..object
            })
            .collect()
    }

    /// Pump rounds until `folder` has `count` objects written, at most
    /// `bound` of time.
    fn pump_until(
        driver: &Driver,
        state: &mut PumpState,
        folder: &FakeFolder,
        count: usize,
        bound: Duration,
    ) -> u32 {
        let end = Instant::now() + bound;
        let seen = driver.wake.current();
        let mut rounds = 0;
        while folder.written().len() < count {
            assert!(
                Instant::now() < end,
                "{} of {count} written after {rounds} rounds",
                folder.written().len()
            );
            driver.pump_round(state, seen);
            rounds += 1;
        }
        rounds
    }

    #[test]
    fn a_timed_out_batch_is_halved_and_the_pump_completes() {
        // A slow link: a fetch of more than 16 objects outlasts the deadline,
        // one of 16 or fewer is answered at once.
        let all = objects("slow-link", OBJECT_BATCH);
        let folder = Arc::new(FakeFolder {
            missing: wanted_of(&all),
            ..FakeFolder::default()
        });
        let link = fake_worker(folder.clone());
        let mut peer = Peer::plain(&all, &[], Duration::from_millis(900));
        peer.stall_above = Some(16);
        let peer = Arc::new(peer);
        let folders = Arc::new(Folders(vec![("f1".into(), link, Mutex::new(Vec::new()))]));
        let (mut driver, _root) = driver(OneLane(peer.lane()), folders, Seq::new());
        Arc::get_mut(&mut driver).unwrap().fetch_deadline = |_| Duration::from_millis(300);
        let mut state = PumpState::default();
        let seen = driver.wake.current();
        // The full batch times out: nothing stored, the batch halves, and
        // the timed-out digests are not taken as missing on the peer.
        assert_eq!(driver.pump_round(&mut state, seen), 0);
        assert_eq!(state.batch_size("f1", "ws://peer"), OBJECT_BATCH / 2);
        assert!(state.unavailable["f1"].is_empty());
        assert_eq!(lock(&peer.requests)[0].len(), OBJECT_BATCH);
        pump_until(
            &driver,
            &mut state,
            &folder,
            OBJECT_BATCH,
            Duration::from_secs(30),
        );
        let mut written = folder.written();
        written.sort();
        let mut expected: Vec<Digest> = all.iter().map(|o| o.digest).collect();
        expected.sort();
        assert_eq!(written, expected);
        // Every batch after the timeout was within the halved size, and the
        // answered ones grew it back.
        let requests = lock(&peer.requests).clone();
        assert!(
            requests[1..].iter().all(|kinds| kinds.len() <= 16),
            "{:?}",
            requests.iter().map(Vec::len).collect::<Vec<_>>()
        );
        assert!(state.batch_size("f1", "ws://peer") > OBJECT_BATCH / 2);
    }

    #[test]
    fn a_batch_shrinks_to_one_and_grows_back_to_the_full_batch() {
        let mut state = PumpState::default();
        for expected in [16, 8, 4, 2, 1, 1] {
            let asked = state.batch_size("f", "p");
            state.shrink("f", "p", asked);
            assert_eq!(state.batch_size("f", "p"), expected);
        }
        let mut sizes = Vec::new();
        while state.batch_size("f", "p") < OBJECT_BATCH {
            state.grow("f", "p");
            sizes.push(state.batch_size("f", "p"));
            assert!(sizes.len() < 32, "{sizes:?}");
        }
        assert_eq!(sizes[..4], [2, 3, 4, 5]);
        assert!(state.batch.is_empty());
        // Growing a full batch keeps it full.
        state.grow("f", "p");
        assert_eq!(state.batch_size("f", "p"), OBJECT_BATCH);
    }

    #[test]
    fn manifests_are_asked_in_their_own_batches_of_four_before_chunks() {
        let manifests = manifests("manifest", 6);
        let chunks = objects("chunk", 10);
        // Listed mixed, as an `objects.missing` page may be.
        let all: Vec<Object> = chunks[..5]
            .iter()
            .chain(&manifests)
            .chain(&chunks[5..])
            .cloned()
            .collect();
        let folder = Arc::new(FakeFolder {
            missing: wanted_of(&all),
            ..FakeFolder::default()
        });
        let link = fake_worker(folder.clone());
        let peer = Peer::new(&all, &[], Duration::ZERO);
        let folders = Arc::new(Folders(vec![("f1".into(), link, Mutex::new(Vec::new()))]));
        let (driver, _root) = driver(OneLane(peer.lane()), folders, Seq::new());
        let mut state = PumpState::default();
        assert_eq!(driver.pump_round(&mut state, driver.wake.current()), 16);
        let requests = lock(&peer.requests).clone();
        assert_eq!(
            requests,
            vec![
                vec![Kind::Manifest; 4],
                vec![Kind::Manifest; 2],
                vec![Kind::Chunk; 10],
            ]
        );
    }

    #[test]
    fn a_reply_cut_to_the_byte_bounds_has_the_rest_asked_again() {
        // The peer answers at most 3 objects per reply and says where it
        // stopped; the 2 objects it does not hold are still found absent.
        let all = objects("cut", 12);
        let held: Vec<Object> = all[..10].to_vec();
        let folder = Arc::new(FakeFolder {
            missing: wanted_of(&all),
            ..FakeFolder::default()
        });
        let link = fake_worker(folder.clone());
        let mut peer = Peer::plain(&held, &[], Duration::ZERO);
        peer.cut = Some(3);
        let peer = Arc::new(peer);
        let folders = Arc::new(Folders(vec![("f1".into(), link, Mutex::new(Vec::new()))]));
        let (driver, _root) = driver(OneLane(peer.lane()), folders, Seq::new());
        let mut state = PumpState::default();
        // One round stores everything the peer holds.
        assert_eq!(driver.pump_round(&mut state, driver.wake.current()), 10);
        let expected: Vec<Digest> = held.iter().map(|o| o.digest).collect();
        assert_eq!(folder.written(), expected);
        // 12, then what was not looked at: 9 (from the 4th), 6, 3 (two
        // absent, held objects first).
        let sizes: Vec<usize> = lock(&peer.requests).iter().map(Vec::len).collect();
        assert_eq!(sizes, [12, 9, 6, 3]);
        // Only the two the peer lacks are unavailable; nothing failed.
        let unavailable: BTreeSet<Digest> = state.unavailable["f1"].keys().copied().collect();
        let absent: BTreeSet<Digest> = all[10..].iter().map(|o| o.digest).collect();
        assert_eq!(unavailable, absent);
        assert!(state.resting.is_empty());
    }

    // ---- push ------------------------------------------------------------

    /// A listener's bulk lane: lists `demand` and pages `missing` as the
    /// worker does, less what was stored and what is excluded. A store of more
    /// than `stall_above` objects outlasts its deadline and stores nothing. A
    /// digest in `refuse` is refused. With `unpublished`, that repo answers
    /// `REPO_NOT_PUBLISHED`. Also serves `objects.fetch` from `held`, for the
    /// pull passes. `calls` records every request in order.
    #[derive(Default)]
    struct Listener {
        demand: Wanted,
        missing: Wanted,
        held: Vec<Object>,
        refuse: BTreeSet<Digest>,
        stall_above: Option<usize>,
        /// An `objects.fetch` outlasts its deadline.
        stall_fetch: bool,
        unpublished: Option<String>,
        /// Bumped by every store, as a reader's demand arriving meanwhile.
        wake_on_store: Option<Arc<Seq>>,
        /// Runs during every `objects.fetch`.
        during_fetch: Option<Hook>,
        /// No request is ever answered, while the lane stays up.
        hang: bool,
        /// An `objects.fetch` is never answered.
        hang_fetch: bool,
        /// An `objects.store` naming one of these is never answered.
        hang_store_of: BTreeSet<Digest>,
        /// An `objects.store` fails with this code.
        fail_store: Mutex<Option<String>>,
        /// A reply to a store also names these as stored.
        claims: Vec<Digest>,
        /// Readers that began waiting later.
        late_demand: Mutex<Wanted>,
        stored: Mutex<Vec<(Digest, Kind)>>,
        calls: Mutex<Vec<String>>,
        store_sizes: Mutex<Vec<usize>>,
        excludes: Mutex<Vec<Vec<Digest>>>,
    }

    impl Listener {
        fn calls(&self) -> Vec<String> {
            lock(&self.calls).clone()
        }

        fn stored(&self) -> Vec<Digest> {
            lock(&self.stored)
                .iter()
                .map(|(digest, _)| *digest)
                .collect()
        }

        fn serve(&self, req: Incoming) {
            if self.hang {
                lock(&self.calls).push(req.op.clone());
                std::mem::forget(req);
                return;
            }
            if self.unpublished.as_deref() == req.header["repo"].as_str() {
                lock(&self.calls).push(req.op.clone());
                req.responder
                    .error(ErrorWire::coded("REPO_NOT_PUBLISHED", "paused"));
                return;
            }
            match req.op.as_str() {
                "objects.wanted" => self.wanted(req),
                "objects.store" => self.store(req),
                "objects.fetch" => {
                    lock(&self.calls).push("fetch".into());
                    if let Some(hook) = &self.during_fetch {
                        hook();
                    }
                    if self.hang_fetch {
                        std::mem::forget(req);
                        return;
                    }
                    if self.stall_fetch {
                        thread::sleep(Duration::from_millis(900));
                        return;
                    }
                    let wanted: Vec<(Digest, Kind)> = serde_json::from_slice(&req.body).unwrap();
                    let objects: Vec<&Object> = wanted
                        .iter()
                        .filter_map(|(digest, _)| self.held.iter().find(|o| o.digest == *digest))
                        .collect();
                    req.responder
                        .reply(json!({}), serde_json::to_vec(&objects).unwrap());
                }
                op => {
                    let error = ErrorWire::coded("UNSUPPORTED_OPERATION", op);
                    req.responder.error(error)
                }
            }
        }

        fn wanted(&self, req: Incoming) {
            let limit = req.header["limit"].as_u64().unwrap() as usize;
            lock(&self.calls).push(format!("wanted/{limit}"));
            let exclude: Vec<Digest> =
                serde_json::from_value(req.header["exclude"].clone()).unwrap();
            let start: usize = req.header["after"]
                .as_str()
                .map_or(0, |after| after.parse().unwrap());
            let stored = self.stored();
            let late = lock(&self.late_demand).clone();
            let mut page = WantedPage {
                demand: self
                    .demand
                    .iter()
                    .chain(&late)
                    .filter(|(digest, _)| !stored.contains(digest))
                    .copied()
                    .collect(),
                ..WantedPage::default()
            };
            if limit > 0 {
                for (index, object) in self.missing.iter().enumerate().skip(start) {
                    if !exclude.contains(&object.0) && !stored.contains(&object.0) {
                        page.missing.push(*object);
                    }
                    if page.missing.len() == limit {
                        page.cursor = Some((index + 1).to_string());
                        break;
                    }
                }
            }
            lock(&self.excludes).push(exclude);
            req.responder
                .reply(json!({}), serde_json::to_vec(&page).unwrap());
        }

        fn store(&self, req: Incoming) {
            lock(&self.calls).push("store".into());
            let objects: Vec<Object> = serde_json::from_slice(&req.body).unwrap();
            lock(&self.store_sizes).push(objects.len());
            if objects
                .iter()
                .any(|object| self.hang_store_of.contains(&object.digest))
            {
                std::mem::forget(req);
                return;
            }
            if let Some(code) = lock(&self.fail_store).clone() {
                req.responder.error(ErrorWire::coded(&code, "failing"));
                return;
            }
            if self.stall_above.is_some_and(|most| objects.len() > most) {
                thread::sleep(Duration::from_millis(900));
                return;
            }
            let mut reply = StoreReply {
                stored: Vec::new(),
                refused: Vec::new(),
            };
            for object in objects {
                if self.refuse.contains(&object.digest) {
                    reply.refused.push(Refused {
                        digest: object.digest,
                        code: "OBJECT_NOT_WANTED".into(),
                    });
                } else {
                    lock(&self.stored).push((object.digest, object.kind));
                    reply.stored.push(object.digest);
                }
            }
            reply.stored.extend(self.claims.iter().copied());
            if let Some(wake) = &self.wake_on_store {
                wake.bump();
                // The caller sees the wake before the reply, every time.
                thread::sleep(Duration::from_millis(100));
            }
            req.responder
                .reply(json!({}), serde_json::to_vec(&reply).unwrap());
        }

        /// A bulk lane to this listener whose peer advertises `features`.
        fn lane_with(self: &Arc<Self>, session: &str, features: &[&str]) -> Conn {
            let listener = self.clone();
            let info = PeerInfo {
                session: session.into(),
                features: features.iter().map(|f| f.to_string()).collect(),
                ..info_as(LaneRole::Bulk)
            };
            let (client, server) = served_info(
                info,
                Arc::new(FnHandler(move |req: Incoming| listener.serve(req))),
            );
            std::mem::forget(server);
            client
        }

        /// A bulk lane to this listener, which takes pushes.
        fn lane(self: &Arc<Self>) -> OneLane {
            OneLane(self.lane_with("session", &[features::CONTENT_PUSH]))
        }
    }

    /// A folder of `repo` whose worker holds `held`.
    fn pushing_folder(repo: &str, held: &[Object]) -> SyncFolder {
        let fake = Arc::new(FakeFolder {
            held: held.to_vec(),
            ..FakeFolder::default()
        });
        SyncFolder {
            id: format!("folder-{repo}"),
            repo: repo.into(),
            link: fake_worker(fake),
            demand: Vec::new(),
            hints: Vec::new(),
        }
    }

    const ENDPOINT: &str = "ws://peer";

    fn push_once(
        lanes: &dyn Lanes,
        state: &mut PushState,
        folder: &SyncFolder,
        mode: PushMode,
    ) -> push::PushOutcome {
        let endpoints = [ENDPOINT.to_owned()];
        push::run(lanes, state, &endpoints, folder, mode, None, &Cancel::new())
    }

    fn push_peer<'a>(state: &'a mut PushState, folder: &SyncFolder) -> &'a mut push::PushPeer {
        state
            .peers
            .get_mut(&(folder.id.clone(), ENDPOINT.to_owned()))
            .unwrap()
    }

    #[test]
    fn the_pump_runs_demand_pull_then_demand_push_then_background() {
        let wanted_here = objects("wanted-here", 1);
        let missing_here = objects("missing-here", 1);
        let wanted_there = objects("wanted-there", 1);
        let missing_there = objects("missing-there", 1);
        let folder = Arc::new(FakeFolder {
            missing: wanted_of(&missing_here),
            held: [wanted_there.clone(), missing_there.clone()].concat(),
            ..FakeFolder::default()
        });
        let listener = Arc::new(Listener {
            demand: wanted_of(&wanted_there),
            missing: wanted_of(&missing_there),
            held: [wanted_here.clone(), missing_here.clone()].concat(),
            ..Listener::default()
        });
        let link = fake_worker(folder.clone());
        let folders = Arc::new(Folders(vec![(
            "f1".into(),
            link,
            Mutex::new(wanted_of(&wanted_here)),
        )]));
        let (driver, _root) = driver(listener.lane(), folders, Seq::new());
        let mut state = PumpState::default();
        // Two pulled, two pushed.
        assert_eq!(driver.pump_round(&mut state, driver.wake.current()), 4);
        assert_eq!(
            listener.calls(),
            ["fetch", "wanted/0", "store", "fetch", "wanted/32", "store"]
        );
        assert_eq!(
            listener.stored(),
            [wanted_there[0].digest, missing_there[0].digest]
        );
    }

    #[test]
    fn a_push_round_stops_before_its_next_call_when_demand_arrives() {
        // Two stores' worth, and demand here arrives during the first.
        let all = objects("object", 2 * OBJECT_BATCH);
        let wake = Seq::new();
        let listener = Arc::new(Listener {
            demand: wanted_of(&all[..OBJECT_BATCH]),
            missing: wanted_of(&all[OBJECT_BATCH..]),
            wake_on_store: Some(wake.clone()),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let folder = pushing_folder("repo-1", &all);
        let mut state = PushState::default();
        let endpoints = [ENDPOINT.to_owned()];
        let cancel = Cancel::new();
        let seen = wake.current();
        let outcome = push::run(
            &lanes,
            &mut state,
            &endpoints,
            &folder,
            PushMode::Background,
            Some((wake.as_ref(), seen)),
            &cancel,
        );
        assert!(outcome.interrupted);
        assert!(outcome.error.is_none());
        // Cancelled, so uncertain, and not counted: the listener may well hold it.
        assert_eq!(outcome.pushed, 0);
        assert_eq!(outcome.remaining, 2 * OBJECT_BATCH);
        assert_eq!(listener.calls(), ["wanted/32", "store"]);
        assert_eq!(listener.stored().len(), OBJECT_BATCH);
        // The page was not finished: the scan stays on it.
        assert_eq!(push_peer(&mut state, &folder).scan, None);
        // A wake that already moved stops it before any call.
        let outcome = push::run(
            &lanes,
            &mut state,
            &endpoints,
            &folder,
            PushMode::Background,
            Some((wake.as_ref(), seen)),
            &cancel,
        );
        assert!(outcome.interrupted);
        assert_eq!(listener.calls().len(), 2);
        // The next round finishes the job.
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, OBJECT_BATCH);
        assert_eq!(listener.stored().len(), 2 * OBJECT_BATCH);
    }

    #[test]
    fn a_listener_without_content_push_is_never_pushed_to() {
        let all = objects("object", 2);
        let listener = Arc::new(Listener {
            demand: wanted_of(&all[..1]),
            missing: wanted_of(&all[1..]),
            ..Listener::default()
        });
        let lanes = OneLane(listener.lane_with("session", &[features::NOTIFY]));
        let folder = pushing_folder("repo-1", &all);
        let mut state = PushState::default();
        for mode in [PushMode::Demand, PushMode::Background] {
            let outcome = push_once(&lanes, &mut state, &folder, mode);
            assert_eq!(outcome.pushed, 0);
            assert!(outcome.error.is_none());
        }
        assert!(listener.calls().is_empty(), "{:?}", listener.calls());
    }

    #[test]
    fn a_digest_this_side_lacks_is_backed_off_and_not_an_error() {
        let all = objects("object", 3);
        let listener = Arc::new(Listener {
            demand: wanted_of(&all[..1]),
            missing: wanted_of(&all[1..]),
            ..Listener::default()
        });
        let lanes = listener.lane();
        // This side holds only the middle one: part of what is wanted.
        let folder = pushing_folder("repo-1", &all[1..2]);
        let mut state = PushState::default();
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, 1);
        assert_eq!(outcome.remaining, 2);
        assert!(outcome.error.is_none());
        assert_eq!(listener.stored(), [all[1].digest]);
        let peer = push_peer(&mut state, &folder);
        assert!(peer.counters.last_error.is_none());
        let lacks: BTreeSet<Digest> = peer.lacks.keys().copied().collect();
        let expected: BTreeSet<Digest> = [all[0].digest, all[2].digest].into();
        assert_eq!(lacks, expected);
        assert!(peer.lacks.values().all(|u| u.backoff == UNAVAILABLE_FIRST));

        // Meanwhile they are excluded from the listing, and not read: demand
        // is not excluded by the listener, so it is skipped here.
        let calls = listener.calls().len();
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, 0);
        assert!(outcome.error.is_none());
        assert_eq!(lock(&listener.excludes).last().unwrap().len(), 2);
        assert_eq!(listener.calls().len(), calls + 1, "{:?}", listener.calls());

        // Lacked again after the backoff: it doubles.
        let peer = push_peer(&mut state, &folder);
        for entry in peer.lacks.values_mut() {
            entry.until = Instant::now();
        }
        push_once(&lanes, &mut state, &folder, PushMode::Background);
        let peer = push_peer(&mut state, &folder);
        assert_eq!(peer.lacks.len(), 2);
        assert!(
            peer.lacks
                .values()
                .all(|u| u.backoff == UNAVAILABLE_FIRST * 2)
        );
        assert!(peer.lacks.values().all(|u| u.until > Instant::now()));
        // Doubling stops at the bound.
        for entry in peer.lacks.values_mut() {
            entry.until = Instant::now();
            entry.backoff = UNAVAILABLE_MAX;
        }
        push_once(&lanes, &mut state, &folder, PushMode::Background);
        let peer = push_peer(&mut state, &folder);
        assert!(peer.lacks.values().all(|u| u.backoff == UNAVAILABLE_MAX));
    }

    #[test]
    fn a_timed_out_store_halves_the_batch_and_answers_grow_it() {
        let all = objects("slow-link", OBJECT_BATCH);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            stall_above: Some(16),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let folder = pushing_folder("repo-1", &all);
        let mut state = PushState {
            deadline: |_| Duration::from_millis(300),
            ..PushState::default()
        };
        // The full batch times out: nothing is stored, the batch halves, and
        // the whole endpoint rests (the lane is not answering).
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, 0);
        assert_eq!(outcome.remaining, OBJECT_BATCH);
        assert!(outcome.error.is_some());
        assert_eq!(push_peer(&mut state, &folder).batch, OBJECT_BATCH / 2);
        assert!(push_peer(&mut state, &folder).lacks.is_empty());
        assert!(state.resting.contains_key(ENDPOINT));
        // While it rests, nothing is asked. Uncertain: not replayed.
        let calls = listener.calls().len();
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert!(outcome.error.is_none());
        assert_eq!(listener.calls().len(), calls);

        let end = Instant::now() + Duration::from_secs(30);
        while listener.stored().len() < OBJECT_BATCH {
            assert!(Instant::now() < end, "{:?}", listener.calls());
            state.resting.clear();
            let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
            assert!(outcome.error.is_none(), "{:?}", outcome.error);
        }
        // Every store after the timeout was within the halved size, and the
        // answered ones grew it back.
        let sizes = lock(&listener.store_sizes).clone();
        assert_eq!(sizes[0], OBJECT_BATCH);
        assert_eq!(sizes[1..], [16, 16], "{sizes:?}");
        assert_eq!(push_peer(&mut state, &folder).batch, 16 + 4 + 5);
    }

    #[test]
    fn a_read_cut_to_its_bounds_is_one_store_and_the_rest_is_read_next() {
        let all = objects("cut", 10);
        let fake = Arc::new(FakeFolder {
            held: all[..8].to_vec(),
            read_cut: Some(3),
            ..FakeFolder::default()
        });
        let folder = SyncFolder {
            id: "f1".into(),
            repo: "repo-1".into(),
            link: fake_worker(fake),
            demand: Vec::new(),
            hints: Vec::new(),
        };
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let mut state = PushState::default();
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, 8);
        assert_eq!(*lock(&listener.store_sizes), [3, 3, 2]);
        // Only what was looked at and found absent is lacked.
        let lacks: BTreeSet<Digest> = push_peer(&mut state, &folder)
            .lacks
            .keys()
            .copied()
            .collect();
        let absent: BTreeSet<Digest> = all[8..].iter().map(|o| o.digest).collect();
        assert_eq!(lacks, absent);
    }

    #[test]
    fn a_refused_digest_is_dropped_from_the_round_and_backed_off() {
        let all = objects("object", 3);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            refuse: [all[1].digest].into(),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let folder = pushing_folder("repo-1", &all);
        let mut state = PushState::default();
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, 2);
        assert_eq!(outcome.remaining, 1);
        assert!(outcome.error.is_none());
        assert_eq!(*lock(&listener.store_sizes), [3]);
        assert_eq!(outcome.unsent, 1);
        assert_eq!(outcome.refusal.as_deref(), Some("OBJECT_NOT_WANTED"));
        let peer = push_peer(&mut state, &folder);
        assert_eq!(peer.lacks.keys().collect::<Vec<_>>(), [&all[1].digest]);
        assert_eq!(peer.counters.pushed, 2);
        assert_eq!(peer.counters.refused, 1);
        // Meanwhile it is excluded, and still known to be unsent. A demand pass,
        // for a reader, does not offer it again either.
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, 0);
        assert_eq!(outcome.remaining, 0);
        assert_eq!(outcome.unsent, 1);
        assert!(outcome.swept);
        assert_eq!(*lock(&listener.store_sizes), [3]);
        // Past its backoff it is offered again, and refused again it doubles.
        let peer = push_peer(&mut state, &folder);
        peer.lacks
            .values_mut()
            .for_each(|u| u.until = Instant::now());
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.remaining, 1);
        assert_eq!(*lock(&listener.store_sizes), [3, 1]);
        let peer = push_peer(&mut state, &folder);
        let backoff = peer.lacks[&all[1].digest].backoff;
        assert_eq!(backoff, UNAVAILABLE_FIRST * 2);
    }

    /// Digests go to the lane their first byte's parity names.
    struct TwoLanes(Conn, Conn);

    impl Lanes for TwoLanes {
        fn ready_peers(&self) -> Vec<ReadyPeer> {
            Vec::new()
        }
        fn bulk_endpoints(&self) -> Vec<String> {
            vec![ENDPOINT.into()]
        }
        fn bulk_lane(&self, _: &str, digest: &Digest) -> Option<Conn> {
            Some(
                if digest.0[0].is_multiple_of(2) {
                    &self.0
                } else {
                    &self.1
                }
                .clone(),
            )
        }
        fn any_bulk_lane(&self, _: &str) -> Option<Conn> {
            Some(self.0.clone())
        }
    }

    #[test]
    fn a_push_stores_each_digest_on_its_lane_with_the_listed_kind_and_demand_first() {
        let mut all = objects("object", 12);
        all[0].kind = Kind::Manifest;
        let owned = |lane: u8, of: &[Object]| -> Vec<Digest> {
            of.iter()
                .map(|o| o.digest)
                .filter(|digest| digest.0[0] % 2 == lane)
                .collect()
        };
        let lists = Arc::new(Listener {
            demand: wanted_of(&all[..4]),
            missing: wanted_of(&all[4..]),
            ..Listener::default()
        });
        // Each lane records its own stores; only the first is asked to list.
        let other = Arc::new(Listener::default());
        let lanes = TwoLanes(
            lists.lane_with("lane-0", &[features::CONTENT_PUSH]),
            other.lane_with("lane-1", &[features::CONTENT_PUSH]),
        );
        let folder = pushing_folder("repo-1", &all);
        let mut state = PushState::default();
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, 12);
        // Demand went first on each lane, in listed order.
        let expected = |lane| [owned(lane, &all[..4]), owned(lane, &all[4..])].concat();
        assert_eq!(lists.stored(), expected(0));
        assert_eq!(other.stored(), expected(1));
        // The kind the listing gave, not one assumed from the read.
        let kinds: BTreeMap<Digest, Kind> = lock(&lists.stored)
            .iter()
            .chain(lock(&other.stored).iter())
            .copied()
            .collect();
        assert_eq!(kinds[&all[0].digest], Kind::Manifest);
        assert_eq!(kinds[&all[1].digest], Kind::Chunk);
    }

    #[test]
    fn a_repo_the_listener_does_not_publish_rests_alone_and_is_logged_once() {
        let all = objects("object", 1);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            unpublished: Some("paused-repo".into()),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let paused = pushing_folder("paused-repo", &all);
        let open = pushing_folder("repo-1", &all);
        let mut state = PushState::default();
        for mode in [PushMode::Demand, PushMode::Background] {
            let outcome = push_once(&lanes, &mut state, &paused, mode);
            // The first call rests it; the second is not even sent.
            assert_eq!(outcome.error.is_some(), mode == PushMode::Demand);
        }
        assert_eq!(listener.calls(), ["objects.wanted"]);
        let peer = push_peer(&mut state, &paused);
        let logged = peer.counters.last_error.clone().unwrap();
        assert!(logged.contains("REPO_NOT_PUBLISHED"), "{logged}");
        assert!(!state.resting.contains_key(ENDPOINT));

        // The other folder of that endpoint is not kept from pushing.
        let outcome = push_once(&lanes, &mut state, &open, PushMode::Background);
        assert_eq!(outcome.pushed, 1);

        // Its rest ended, it fails alike: the error stays as logged.
        push_peer(&mut state, &paused).rest_until = None;
        let outcome = push_once(&lanes, &mut state, &paused, PushMode::Demand);
        assert_eq!(outcome.error.map(|e| e.to_string()).as_ref(), Some(&logged));
    }

    #[test]
    fn a_failing_store_is_not_cleared_by_the_clean_listing_of_a_later_round() {
        let all = objects("object", 1);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            stall_above: Some(0),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let folder = pushing_folder("repo-1", &all);
        let mut state = PushState {
            deadline: |_| Duration::from_millis(100),
            ..PushState::default()
        };
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        let logged = outcome.error.unwrap().to_string();
        state.resting.clear();
        // A Demand pass with nothing to do lists cleanly.
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Demand);
        assert!(outcome.error.is_none());
        let peer = push_peer(&mut state, &folder);
        assert_eq!(peer.counters.last_error.as_ref(), Some(&logged));
    }

    #[test]
    fn what_a_push_remembers_outlasts_a_busy_round_and_not_long_idleness() {
        let all = objects("object", 1);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let lacking = pushing_folder("lacking", &[]);
        let other = pushing_folder("other", &all);
        let mut state = PushState::default();
        push_once(&lanes, &mut state, &lacking, PushMode::Background);
        // Another folder's pass, with this one absent, keeps its backoff.
        push_once(&lanes, &mut state, &other, PushMode::Background);
        assert_eq!(push_peer(&mut state, &lacking).lacks.len(), 1);
        // Unused for long, it goes.
        let long_ago = Instant::now()
            .checked_sub(UNAVAILABLE_MAX * 2 + Duration::from_secs(1))
            .expect("the monotonic clock starts early enough");
        push_peer(&mut state, &lacking).touched = long_ago;
        push_once(&lanes, &mut state, &other, PushMode::Background);
        assert!(state.counters(&lacking.id, ENDPOINT).is_none());
        assert!(state.counters(&other.id, ENDPOINT).is_some());
    }

    #[test]
    fn a_wake_during_the_read_stops_a_background_push_before_its_store() {
        let all = objects("object", 2);
        let wake = Seq::new();
        let fake = Arc::new(FakeFolder {
            held: all.clone(),
            wake_on_read: Some(wake.clone()),
            ..FakeFolder::default()
        });
        let folder = SyncFolder {
            id: "f1".into(),
            repo: "repo-1".into(),
            link: fake_worker(fake),
            demand: Vec::new(),
            hints: Vec::new(),
        };
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let mut state = PushState::default();
        let endpoints = [ENDPOINT.to_owned()];
        let seen = wake.current();
        let outcome = push::run(
            &lanes,
            &mut state,
            &endpoints,
            &folder,
            PushMode::Background,
            Some((wake.as_ref(), seen)),
            &Cancel::new(),
        );
        assert!(outcome.interrupted);
        assert!(outcome.error.is_none());
        assert_eq!(outcome.pushed, 0);
        assert_eq!(outcome.remaining, 2);
        assert_eq!(listener.calls(), ["wanted/32"]);
        // This side does hold them, and they stay missing: listed again.
        let peer = push_peer(&mut state, &folder);
        assert!(peer.lacks.is_empty());
        assert_eq!(peer.scan, None);
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, 2);
    }

    /// Running folders, each of its own repo (its id here), with set demand.
    struct RepoFolders(Vec<(String, Arc<WorkerLink>, Wanted)>);

    impl SyncFolders for RepoFolders {
        fn syncing(&self) -> Vec<SyncFolder> {
            self.0
                .iter()
                .map(|(repo, link, demand)| SyncFolder {
                    id: repo.clone(),
                    repo: repo.clone(),
                    link: link.clone(),
                    demand: demand.clone(),
                    hints: Vec::new(),
                })
                .collect()
        }
        fn knows(&self, repo: &str) -> bool {
            self.0.iter().any(|(known, _, _)| known == repo)
        }
    }

    const UNPUBLISHED: &str = "unpublished-repo";

    /// A folder of [`UNPUBLISHED`] that lists `missing` and whose readers wait
    /// for `demand`, then one of `repo-1` that holds `held`.
    fn unpublished_then_open(
        missing: &[Object],
        demand: &[Object],
        held: &[Object],
    ) -> Arc<RepoFolders> {
        let unpublished = Arc::new(FakeFolder {
            missing: wanted_of(missing),
            ..FakeFolder::default()
        });
        let open = Arc::new(FakeFolder {
            held: held.to_vec(),
            ..FakeFolder::default()
        });
        Arc::new(RepoFolders(vec![
            (
                UNPUBLISHED.into(),
                fake_worker(unpublished),
                wanted_of(demand),
            ),
            ("repo-1".into(), fake_worker(open), Vec::new()),
        ]))
    }

    #[test]
    fn a_coded_pull_failure_of_one_folder_does_not_starve_the_pushes_of_the_others() {
        // More than a page to push, so a second round has work.
        let there = objects("there", OBJECT_BATCH + 8);
        let listener = Arc::new(Listener {
            missing: wanted_of(&there),
            unpublished: Some(UNPUBLISHED.into()),
            ..Listener::default()
        });
        let folders = unpublished_then_open(&objects("here", 1), &[], &there);
        let (driver, _root) = driver(listener.lane(), folders, Seq::new());
        let mut state = PumpState::default();
        let seen = driver.wake.current();
        // The pull of the first folder fails: the pull passes rest, the second
        // folder's background push goes on in this very round.
        assert_eq!(driver.pump_round(&mut state, seen), OBJECT_BATCH);
        assert!(!state.resting.contains_key(ENDPOINT));
        let rested = (UNPUBLISHED.to_owned(), ENDPOINT.to_owned());
        assert!(state.folder_resting.contains_key(&rested));
        assert!(!state.push.resting.contains_key(ENDPOINT));
        // The round pull sits out holds no call of it, and still pushes.
        assert_eq!(driver.pump_round(&mut state, seen), 8);
        assert_eq!(listener.stored().len(), there.len());
        let fetches = listener
            .calls()
            .iter()
            .filter(|call| *call == "objects.fetch")
            .count();
        assert_eq!(fetches, 1, "{:?}", listener.calls());
    }

    #[test]
    fn a_blocked_reader_of_one_folder_does_not_skip_the_demand_push_of_the_others() {
        let wanted_there = objects("wanted-there", 1);
        let listener = Arc::new(Listener {
            demand: wanted_of(&wanted_there),
            unpublished: Some(UNPUBLISHED.into()),
            ..Listener::default()
        });
        let folders = unpublished_then_open(&[], &objects("here", 1), &wanted_there);
        let (driver, _root) = driver(listener.lane(), folders, Seq::new());
        let mut state = PumpState::default();
        assert_eq!(driver.pump_round(&mut state, driver.wake.current()), 1);
        assert_eq!(listener.stored(), [wanted_there[0].digest]);
        assert_eq!(
            listener.calls(),
            [
                "objects.fetch",
                "objects.wanted",
                "wanted/0",
                "store",
                "wanted/32"
            ]
        );
    }

    #[test]
    fn a_pull_that_timed_out_rests_the_push_steps_too() {
        let here = objects("here", 1);
        let there = objects("there", 1);
        let listener = Arc::new(Listener {
            missing: wanted_of(&there),
            stall_fetch: true,
            ..Listener::default()
        });
        let fake = Arc::new(FakeFolder {
            missing: wanted_of(&here),
            held: there.clone(),
            ..FakeFolder::default()
        });
        let folders = Arc::new(RepoFolders(vec![(
            "repo-1".into(),
            fake_worker(fake),
            Vec::new(),
        )]));
        let (mut driver, _root) = driver(listener.lane(), folders, Seq::new());
        Arc::get_mut(&mut driver).unwrap().fetch_deadline = |_| Duration::from_millis(100);
        let mut state = PumpState::default();
        let seen = driver.wake.current();
        // The lane is not answering: no more calls to it in this round, nor in
        // the next, whichever step would make them.
        assert_eq!(driver.pump_round(&mut state, seen), 0);
        assert_eq!(listener.calls(), ["wanted/0", "fetch"]);
        assert!(state.push.resting.contains_key(ENDPOINT));
        assert_eq!(driver.pump_round(&mut state, seen), 0);
        assert_eq!(listener.calls(), ["wanted/0", "fetch"]);
    }

    #[test]
    fn the_push_scan_stays_on_a_stored_page_and_moves_past_any_other() {
        let all = objects("object", OBJECT_BATCH + 2);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            ..Listener::default()
        });
        let lanes = listener.lane();
        // The first page is held whole.
        let folder = pushing_folder("repo-1", &all[..OBJECT_BATCH]);
        let mut state = PushState::default();
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, OBJECT_BATCH);
        assert_eq!(push_peer(&mut state, &folder).scan, None);
        // The next page is short and lacked: the scan reached its end.
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, 0);
        assert_eq!(outcome.remaining, 2);
        assert_eq!(push_peer(&mut state, &folder).scan, None);
        assert_eq!(push_peer(&mut state, &folder).lacks.len(), 2);

        // A full page not wholly stored moves the scan on.
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let folder = pushing_folder("repo-1", &all[1..OBJECT_BATCH]);
        let mut state = PushState::default();
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, OBJECT_BATCH - 1);
        assert_eq!(
            push_peer(&mut state, &folder).scan.as_deref(),
            Some(OBJECT_BATCH.to_string().as_str())
        );
    }

    // ---- what is due -----------------------------------------------------

    const HOUR: Duration = Duration::from_secs(3600);
    /// Longer than the [`SHORT`] interval, by a margin no scheduler hiccup in
    /// these tests needs to beat: nothing here waits for an exact moment.
    const PAST_SHORT: Duration = Duration::from_millis(40);
    const SHORT: Duration = Duration::from_millis(10);

    fn active(interval: Duration, backup_poll: Duration) -> SyncPolicy {
        SyncPolicy {
            profile: SyncProfile::Active,
            interval,
            backup_poll,
            cursors: Arc::new(FakeCursors::default()),
        }
    }

    /// A control lane no test sends anything on.
    fn idle_control() -> Conn {
        let (client, server) = served(Arc::new(SkewedPeer {
            skew_ms: Mutex::new(0),
        }));
        std::mem::forget(server);
        client
    }

    fn ready_peer(endpoint: &str, session: &str, notify: bool, control: &Conn) -> ReadyPeer {
        ReadyPeer {
            endpoint: endpoint.into(),
            installation: "peer-b".into(),
            session: session.into(),
            control: control.clone(),
            notify,
        }
    }

    /// The (endpoint, session, notify) peers with a Ready control lane, and one
    /// bulk lane all of them use.
    struct ReadyLanes {
        peers: Vec<(String, String, bool)>,
        control: Conn,
        bulk: Option<Conn>,
    }

    impl ReadyLanes {
        fn one(notify: bool, bulk: Conn) -> Self {
            Self {
                peers: vec![(ENDPOINT.into(), "control".into(), notify)],
                control: idle_control(),
                bulk: Some(bulk),
            }
        }
    }

    impl Lanes for ReadyLanes {
        fn ready_peers(&self) -> Vec<ReadyPeer> {
            self.peers
                .iter()
                .map(|(endpoint, session, notify)| {
                    ready_peer(endpoint, session, *notify, &self.control)
                })
                .collect()
        }
        fn bulk_endpoints(&self) -> Vec<String> {
            self.bulk
                .iter()
                .flat_map(|_| self.peers.iter().map(|(endpoint, _, _)| endpoint.clone()))
                .collect()
        }
        fn bulk_lane(&self, _: &str, _: &Digest) -> Option<Conn> {
            self.bulk.clone()
        }
        fn any_bulk_lane(&self, _: &str) -> Option<Conn> {
            self.bulk.clone()
        }
    }

    fn policy_driver(
        lanes: impl Lanes + 'static,
        repos: &[&str],
        policy: SyncPolicy,
    ) -> (Arc<Driver>, tempfile::TempDir) {
        let folders = repos
            .iter()
            .map(|repo| {
                let fake = Arc::new(FakeFolder::default());
                ((*repo).to_owned(), fake_worker(fake), Vec::new())
            })
            .collect();
        let root = tempfile::tempdir().unwrap();
        let driver = Driver::with_policy(
            Arc::new(lanes),
            Catalog::open(root.path()).unwrap(),
            Arc::new(RepoFolders(folders)),
            Seq::new(),
            policy,
            Cancel::new(),
        );
        (driver, root)
    }

    /// One dispatch: the pairs it started, each finished with nothing moved.
    fn dispatched(driver: &Driver, peers: &[ReadyPeer]) -> Vec<Pair> {
        let (tasks, queue) = mpsc::channel();
        driver.dispatch(1, peers, &tasks);
        let mut pairs: Vec<Pair> = queue
            .try_iter()
            .map(|task| {
                driver.finish(&task, task.cursor, Ok(()));
                task.key
            })
            .collect();
        pairs.sort();
        pairs
    }

    fn pair(endpoint: &str, repo: &str) -> Pair {
        (endpoint.to_owned(), repo.to_owned())
    }

    fn metadata(repo: &str) -> Notify {
        Notify {
            repo: repo.into(),
            reason: NotifyReason::Metadata,
        }
    }

    fn demand_of(repo: &str) -> Notify {
        Notify {
            repo: repo.into(),
            reason: NotifyReason::Demand,
        }
    }

    /// A driver of one repo with no bulk lane, for what `dispatch` does.
    fn dispatching(policy: SyncPolicy) -> (Arc<Driver>, tempfile::TempDir, Conn) {
        let control = idle_control();
        let lanes = ReadyLanes {
            peers: Vec::new(),
            control: control.clone(),
            bulk: None,
        };
        let (driver, root) = policy_driver(lanes, &["repo-1"], policy);
        (driver, root, control)
    }

    #[test]
    fn a_notify_session_waits_for_the_backup_poll_when_nothing_changed() {
        let (driver, _root, control) = dispatching(active(SHORT, HOUR));
        let peers = [ready_peer(ENDPOINT, "control", true, &control)];
        // New: due once.
        assert_eq!(dispatched(&driver, &peers), [pair(ENDPOINT, "repo-1")]);
        assert!(dispatched(&driver, &peers).is_empty());
        // The sync interval passing is not its poll.
        thread::sleep(PAST_SHORT);
        assert!(dispatched(&driver, &peers).is_empty());
        // The backup poll is.
        let key = pair(ENDPOINT, "repo-1");
        lock(&driver.jobs).get_mut(&key).unwrap().next_poll = Some(Instant::now());
        assert_eq!(dispatched(&driver, &peers), [pair(ENDPOINT, "repo-1")]);
        assert!(dispatched(&driver, &peers).is_empty());
    }

    #[test]
    fn a_session_without_notify_keeps_the_sync_interval() {
        let (driver, _root, control) = dispatching(active(SHORT, HOUR));
        let peers = [ready_peer(ENDPOINT, "control", false, &control)];
        assert_eq!(dispatched(&driver, &peers).len(), 1);
        thread::sleep(PAST_SHORT);
        assert_eq!(dispatched(&driver, &peers).len(), 1);
        thread::sleep(PAST_SHORT);
        assert_eq!(dispatched(&driver, &peers).len(), 1);
    }

    #[test]
    fn a_notify_marks_only_its_peer_and_repo_due() {
        let control = idle_control();
        let lanes = ReadyLanes {
            peers: Vec::new(),
            control: control.clone(),
            bulk: None,
        };
        let (driver, _root) = policy_driver(lanes, &["repo-1", "repo-2"], active(HOUR, HOUR));
        let peers = [
            ready_peer("ws://one", "control-1", true, &control),
            ready_peer("ws://two", "control-2", true, &control),
        ];
        assert_eq!(dispatched(&driver, &peers).len(), 4);
        assert!(dispatched(&driver, &peers).is_empty());

        let (poke, wake) = (driver.poke.current(), driver.wake.current());
        driver.notified("ws://one", &metadata("repo-2"));
        assert!(driver.poke.current() > poke);
        assert_eq!(driver.wake.current(), wake);
        assert_eq!(dispatched(&driver, &peers), [pair("ws://one", "repo-2")]);
        assert!(dispatched(&driver, &peers).is_empty());

        // Demand is the pump's business, not the scheduler's.
        driver.notified("ws://two", &demand_of("repo-1"));
        assert!(driver.wake.current() > wake);
        assert!(dispatched(&driver, &peers).is_empty());
        let due = lock(&driver.due);
        assert_eq!(
            due.demand.iter().cloned().collect::<Vec<_>>(),
            [pair("ws://two", "repo-1")]
        );
        assert!(due.background.is_empty());
    }

    #[test]
    fn a_new_control_session_is_always_due() {
        let (driver, _root, control) = dispatching(active(HOUR, HOUR));
        let first = [ready_peer(ENDPOINT, "control", true, &control)];
        assert_eq!(dispatched(&driver, &first).len(), 1);
        let key = pair(ENDPOINT, "repo-1");
        lock(&driver.jobs).get_mut(&key).unwrap().job.cursor = FeedCursor {
            sent: 5,
            received: 7,
        };
        assert!(dispatched(&driver, &first).is_empty());
        // The lanes came back: due, from the start.
        let second = [ready_peer(ENDPOINT, "control-again", true, &control)];
        let (tasks, queue) = mpsc::channel();
        driver.dispatch(1, &second, &tasks);
        let task = queue.try_recv().expect("the new session was not due");
        assert_eq!(task.cursor, FeedCursor::default());
        driver.finish(&task, task.cursor, Ok(()));
        assert!(dispatched(&driver, &second).is_empty());
    }

    #[test]
    fn a_mark_that_finds_the_pair_busy_or_away_waits_for_it() {
        let (driver, _root, control) = dispatching(active(HOUR, HOUR));
        let peers = [ready_peer(ENDPOINT, "control", true, &control)];
        let (tasks, queue) = mpsc::channel();
        driver.dispatch(1, &peers, &tasks);
        let task = queue.try_recv().unwrap();
        // It arrives during the exchange: the exchange may be past it already.
        driver.notified(ENDPOINT, &metadata("repo-1"));
        driver.dispatch(1, &peers, &tasks);
        assert!(queue.try_recv().is_err());
        let poke = driver.poke.current();
        driver.finish(&task, task.cursor, Ok(()));
        assert!(driver.poke.current() > poke);
        assert_eq!(dispatched(&driver, &peers).len(), 1);
        // It arrives with no Ready peer: the next one to show up is told.
        driver.notified(ENDPOINT, &metadata("repo-1"));
        assert!(dispatched(&driver, &[]).is_empty());
        assert_eq!(dispatched(&driver, &peers).len(), 1);
    }

    #[test]
    fn a_failed_exchange_does_not_wait_for_the_backup_poll() {
        let (driver, _root, control) = dispatching(active(SHORT, HOUR));
        let peers = [ready_peer(ENDPOINT, "control", true, &control)];
        let (tasks, queue) = mpsc::channel();
        driver.dispatch(1, &peers, &tasks);
        let task = queue.try_recv().unwrap();
        driver.finish(&task, task.cursor, Err(Failure::LaneLost));
        thread::sleep(PAST_SHORT);
        assert_eq!(dispatched(&driver, &peers).len(), 1);
        // Once it works the backup poll applies again.
        thread::sleep(PAST_SHORT);
        assert!(dispatched(&driver, &peers).is_empty());
    }

    #[test]
    fn a_local_change_marks_that_repo_for_every_peer() {
        let control = idle_control();
        let lanes = ReadyLanes {
            peers: vec![
                ("ws://one".into(), "control-1".into(), true),
                ("ws://two".into(), "control-2".into(), true),
            ],
            control: control.clone(),
            bulk: None,
        };
        let peers = lanes.ready_peers();
        let (driver, _root) = policy_driver(lanes, &["repo-1", "repo-2"], active(HOUR, HOUR));
        assert_eq!(dispatched(&driver, &peers).len(), 4);
        let (poke, wake) = (driver.poke.current(), driver.wake.mark());
        driver.local_change("repo-1");
        assert!(driver.poke.current() > poke);
        // It rouses the pump and stops no background work.
        assert_ne!(driver.wake.mark(), wake);
        assert_eq!(driver.wake.current(), wake.value);
        let expected = [pair("ws://one", "repo-1"), pair("ws://two", "repo-1")];
        assert_eq!(dispatched(&driver, &peers), expected);
        assert_eq!(
            lock(&driver.due)
                .background
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            expected
        );
    }

    // ---- passive and sync-now ---------------------------------------------

    fn passive(interval: Duration) -> SyncPolicy {
        SyncPolicy {
            profile: SyncProfile::Passive,
            interval,
            backup_poll: interval,
            cursors: Arc::new(FakeCursors::default()),
        }
    }

    /// Opens a burst of `pairs`, the way `sync_now` does, ending far off.
    fn open_burst(driver: &Driver, pairs: &[Pair]) -> u64 {
        open_burst_of(driver, pairs, BurstMode::Full)
    }

    fn open_burst_of(driver: &Driver, pairs: &[Pair], mode: BurstMode) -> u64 {
        let mut due = lock(&driver.due);
        let burst = due.next_burst;
        due.next_burst += 1;
        let until = Instant::now() + HOUR;
        let map = BTreeMap::new();
        due.bursts.insert(
            burst,
            Burst {
                until,
                mode,
                pairs: map,
            },
        );
        for pair in pairs {
            due.join_burst(burst, pair.clone(), None);
        }
        burst
    }

    #[test]
    fn the_sync_now_bound_beats_the_reply_timeout() {
        assert_eq!(REPLY_TIMEOUT - SYNC_NOW_BOUND, Duration::from_secs(5));
    }

    #[test]
    fn a_passive_driver_dispatches_only_for_a_pair_in_a_burst() {
        let control = idle_control();
        let lanes = ReadyLanes {
            peers: Vec::new(),
            control: control.clone(),
            bulk: None,
        };
        let (driver, _root) = policy_driver(lanes, &["repo-1", "repo-2"], passive(SHORT));
        let peers = [ready_peer(ENDPOINT, "control", false, &control)];
        // New, past its poll, and with a mark: none of it moves a passive driver.
        assert!(dispatched(&driver, &peers).is_empty());
        thread::sleep(PAST_SHORT);
        driver.notified(ENDPOINT, &metadata("repo-1"));
        driver.local_change("repo-1");
        assert!(dispatched(&driver, &peers).is_empty());
        assert!(lock(&driver.jobs).is_empty());
        // Only what the burst selected, once.
        let key = pair(ENDPOINT, "repo-1");
        open_burst(&driver, std::slice::from_ref(&key));
        assert_eq!(dispatched(&driver, &peers), [key]);
        thread::sleep(PAST_SHORT);
        assert!(dispatched(&driver, &peers).is_empty());
    }

    #[test]
    fn a_passive_driver_is_not_marked_by_what_a_peer_says() {
        let (driver, _root, _control) = dispatching(passive(SHORT));
        let (poke, wake) = (driver.poke.current(), driver.wake.mark());
        driver.notified(ENDPOINT, &metadata("repo-1"));
        driver.notified(ENDPOINT, &demand_of("repo-1"));
        assert_eq!(driver.poke.current(), poke);
        assert_eq!(driver.wake.mark(), wake);
        let due = lock(&driver.due);
        assert!(due.exchange.is_empty() && due.demand.is_empty());
    }

    #[test]
    fn a_passive_pump_asks_a_listener_nothing_outside_a_burst() {
        let held = objects("held", 1);
        let folder = Arc::new(FakeFolder {
            held: held.clone(),
            missing: wanted_of(&objects("there", 1)),
            ..FakeFolder::default()
        });
        let listener = Arc::new(Listener {
            demand: wanted_of(&held),
            ..Listener::default()
        });
        let bulk = listener.lane_with("session", &[features::CONTENT_PUSH]);
        let (driver, _root) = {
            let root = tempfile::tempdir().unwrap();
            let driver = Driver::with_policy(
                Arc::new(ReadyLanes::one(false, bulk)),
                Catalog::open(root.path()).unwrap(),
                Arc::new(RepoFolders(vec![(
                    "repo-1".to_owned(),
                    fake_worker(folder),
                    Vec::new(),
                )])),
                Seq::new(),
                passive(SHORT),
                Cancel::new(),
            );
            (driver, root)
        };
        let mut state = PumpState::default();
        for _ in 0..3 {
            assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
            thread::sleep(PAST_SHORT);
        }
        assert_eq!(driver.pump_wait(), PASSIVE_IDLE);
        // A burst pushes, demand first, and pulls nothing unasked.
        let key = pair(ENDPOINT, "repo-1");
        open_burst(&driver, std::slice::from_ref(&key));
        let calls = calls_of_round(&driver, &mut state, &listener);
        assert_eq!(calls[..3], ["wanted/0", "store", "wanted/32"], "{calls:?}");
        assert_eq!(calls_named(&calls, "fetch"), 0, "{calls:?}");
        // Over: nothing again, even with a mark left behind.
        lock(&driver.due).bursts.clear();
        lock(&driver.due).background.insert(key);
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
        assert_eq!(driver.pump_wait(), PASSIVE_IDLE);
    }

    #[test]
    fn a_passive_pump_still_pulls_what_a_reader_waits_for() {
        let wanted = objects("wanted", 1);
        let background = objects("background", 1);
        let folder = Arc::new(FakeFolder {
            missing: wanted_of(&background),
            ..FakeFolder::default()
        });
        let listener = Arc::new(Listener {
            held: [wanted.clone(), background].concat(),
            ..Listener::default()
        });
        let bulk = listener.lane_with("session", &[features::CONTENT_PUSH]);
        let root = tempfile::tempdir().unwrap();
        let driver = Driver::with_policy(
            Arc::new(ReadyLanes::one(false, bulk)),
            Catalog::open(root.path()).unwrap(),
            Arc::new(RepoFolders(vec![(
                "repo-1".to_owned(),
                fake_worker(folder.clone()),
                wanted_of(&wanted),
            )])),
            Seq::new(),
            passive(SHORT),
            Cancel::new(),
        );
        let mut state = PumpState::default();
        assert_eq!(calls_of_round(&driver, &mut state, &listener), ["fetch"]);
        assert_eq!(folder.written(), [wanted[0].digest]);
    }

    /// A passive driver of `repo-1`, running, whose peer's control lane is
    /// `ready` (or down, while it is `dialed` all the same).
    struct DownLanes {
        ready: ReadyLanes,
        dialed: Vec<String>,
        error: Option<String>,
    }

    impl Lanes for DownLanes {
        fn ready_peers(&self) -> Vec<ReadyPeer> {
            self.ready.ready_peers()
        }
        fn bulk_endpoints(&self) -> Vec<String> {
            self.ready.bulk_endpoints()
        }
        fn bulk_lane(&self, endpoint: &str, digest: &Digest) -> Option<Conn> {
            self.ready.bulk_lane(endpoint, digest)
        }
        fn any_bulk_lane(&self, endpoint: &str) -> Option<Conn> {
            self.ready.any_bulk_lane(endpoint)
        }
        fn dialed_endpoints(&self) -> Vec<String> {
            self.dialed.clone()
        }
        fn lane_error(&self, _: &str) -> Option<String> {
            self.error.clone()
        }
    }

    fn down_lanes(error: Option<&str>) -> DownLanes {
        DownLanes {
            ready: ReadyLanes {
                peers: Vec::new(),
                control: idle_control(),
                bulk: None,
            },
            dialed: vec![ENDPOINT.to_owned()],
            error: error.map(str::to_owned),
        }
    }

    fn folder_of(reply: &Value) -> &Value {
        let folders = reply["folders"].as_array().unwrap();
        assert_eq!(folders.len(), 1, "{reply}");
        &folders[0]
    }

    #[test]
    fn sync_now_with_no_peer_has_nothing_to_do() {
        let lanes = ReadyLanes {
            peers: Vec::new(),
            control: idle_control(),
            bulk: None,
        };
        let (driver, _root) = policy_driver(lanes, &["repo-1"], passive(SHORT));
        let reply = driver.sync_now(
            &["repo-1".to_owned()],
            Duration::from_secs(5),
            BurstMode::Full,
        );
        let folder = folder_of(&reply);
        assert_eq!(folder["id"], "repo-1");
        assert_eq!(folder["remaining"], 0, "{reply}");
        assert!(folder["error"].is_null(), "{reply}");
        assert!(lock(&driver.due).bursts.is_empty());
    }

    #[test]
    fn sync_now_runs_out_its_budget_as_sync_now_timeout() {
        let (driver, _root) = policy_driver(down_lanes(None), &["repo-1"], passive(SHORT));
        let began = Instant::now();
        let reply = driver.sync_now(
            &["repo-1".to_owned()],
            Duration::from_millis(200),
            BurstMode::Full,
        );
        assert!(began.elapsed() < Duration::from_secs(5));
        let folder = folder_of(&reply);
        let error = folder["error"].as_str().unwrap();
        assert!(error.starts_with("SYNC_NOW_TIMEOUT"), "{reply}");
        assert!(folder["remaining"].is_null(), "{reply}");
        // Its gate closed with it.
        assert!(!lock(&driver.due).in_burst(&pair(ENDPOINT, "repo-1")));
    }

    #[test]
    fn sync_now_gives_up_on_a_lane_that_stays_down_with_its_failure() {
        let (driver, _root) = policy_driver(
            down_lanes(Some("WS_CONNECT_FAILED: refused")),
            &["repo-1"],
            passive(SHORT),
        );
        let began = Instant::now();
        let reply = driver.sync_now(&["repo-1".to_owned()], SYNC_NOW_BOUND, BurstMode::Full);
        let took = began.elapsed();
        assert!(took >= LANE_WAIT, "{took:?}");
        assert!(took < LANE_WAIT + Duration::from_secs(5), "{took:?}");
        let error = folder_of(&reply)["error"].as_str().unwrap();
        assert!(error.starts_with("WS_CONNECT_FAILED"), "{reply}");
        assert!(error.contains(ENDPOINT), "{reply}");
    }

    #[test]
    fn sync_now_stops_when_the_daemon_stops() {
        let (driver, _root) = policy_driver(down_lanes(None), &["repo-1"], passive(SHORT));
        let cancelling = driver.cancel.clone();
        let stopper = thread::spawn(move || {
            thread::sleep(PAST_SHORT);
            cancelling.cancel();
        });
        let began = Instant::now();
        let reply = driver.sync_now(&["repo-1".to_owned()], SYNC_NOW_BOUND, BurstMode::Full);
        assert!(began.elapsed() < Duration::from_secs(5));
        stopper.join().unwrap();
        let error = folder_of(&reply)["error"].as_str().unwrap();
        assert!(error.starts_with("ORCHESTRATOR_STOPPING"), "{reply}");
    }

    #[test]
    fn sync_now_reports_a_folder_that_is_not_syncing() {
        let (driver, _root) = policy_driver(down_lanes(None), &["repo-1"], passive(SHORT));
        let began = Instant::now();
        let reply = driver.sync_now(&["paused".to_owned()], SYNC_NOW_BOUND, BurstMode::Full);
        assert!(began.elapsed() < LANE_WAIT);
        let folder = folder_of(&reply);
        assert_eq!(folder["id"], "paused");
        let error = folder["error"].as_str().unwrap();
        assert!(error.starts_with("WORKER_NOT_RUNNING"), "{reply}");
    }

    #[test]
    fn a_failed_exchange_ends_its_pair_and_a_lost_lane_is_asked_again() {
        let (driver, _root, control) = dispatching(passive(SHORT));
        let peers = [ready_peer(ENDPOINT, "control", false, &control)];
        let key = pair(ENDPOINT, "repo-1");
        let burst = open_burst(&driver, std::slice::from_ref(&key));
        let (tasks, queue) = mpsc::channel();
        driver.dispatch(1, &peers, &tasks);
        let task = queue.try_recv().unwrap();
        driver.finish(&task, task.cursor, Err(Failure::LaneLost));
        assert!(lock(&driver.due).in_burst(&key));
        driver.dispatch(1, &peers, &tasks);
        let task = queue.try_recv().expect("the lost lane was not asked again");
        let error = "feed.apply: FATAL_STORE_ERROR: broken";
        driver.finish(&task, task.cursor, Err(Failure::Error(error.into())));
        // No more for this pair, and no waiting for the budget to say so.
        let due = lock(&driver.due);
        assert!(!due.in_burst(&key));
        let state = &due.bursts[&burst].pairs[&key];
        assert_eq!(state.error.as_deref(), Some(error));
        assert_eq!(state.exchanged, 0);
    }

    #[test]
    fn a_burst_is_settled_only_when_its_last_page_came_back_empty() {
        let (driver, _root, _control) = dispatching(passive(SHORT));
        let key = pair(ENDPOINT, "repo-1");
        let burst = open_burst(&driver, std::slice::from_ref(&key));
        let folder = BurstFolder {
            id: "repo-1".into(),
            repo: Some("repo-1".into()),
            seen: Instant::now(),
            error: None,
        };
        let settled = |driver: &Driver| {
            let jobs = lock(&driver.jobs);
            let due = lock(&driver.due);
            burst_folder_settled(&jobs, &due, burst, &folder)
        };
        assert!(!settled(&driver));
        {
            let mut due = lock(&driver.due);
            due.exchange.clear();
            due.demand.clear();
        }
        let outcome = |pushed, remaining, again, swept| push::PushOutcome {
            pushed,
            remaining,
            again,
            swept,
            listed: true,
            ..push::PushOutcome::default()
        };
        // Stored a page: there may be more, so it is due again.
        let mut state = PumpState::default();
        let fake = Arc::new(FakeFolder::default());
        let syncing = SyncFolder {
            id: "repo-1".into(),
            repo: "repo-1".into(),
            link: fake_worker(fake),
            demand: Vec::new(),
            hints: Vec::new(),
        };
        assert!(driver.take_background(&mut state, &syncing, ENDPOINT));
        assert!(!settled(&driver), "a sweep is under way");
        driver.background_done(&key, &outcome(3, 0, true, false));
        assert!(!settled(&driver), "a stored page is not the last");
        // A page that stored nothing, with more of the scan to go.
        assert!(driver.take_background(&mut state, &syncing, ENDPOINT));
        driver.background_done(&key, &outcome(0, 2, true, false));
        assert!(!settled(&driver), "the sweep is not over");
        // The end of a whole sweep that stored nothing.
        assert!(driver.take_background(&mut state, &syncing, ENDPOINT));
        driver.background_done(&key, &outcome(0, 2, false, true));
        assert!(settled(&driver));
        let reply = burst_report(lock(&driver.due).bursts.get(&burst), &folder, None);
        assert_eq!(reply["pushed"], 3, "{reply}");
        assert_eq!(reply["remaining"], 0, "{reply}");
    }

    #[test]
    fn an_exchange_that_moved_events_makes_the_burst_sweep_again_from_the_start() {
        let (driver, _root, control) = dispatching(passive(SHORT));
        let peers = [ready_peer(ENDPOINT, "control", false, &control)];
        let key = pair(ENDPOINT, "repo-1");
        open_burst(&driver, std::slice::from_ref(&key));
        let mut state = PumpState::default();
        let syncing = SyncFolder {
            id: "repo-1".into(),
            repo: "repo-1".into(),
            link: fake_worker(Arc::new(FakeFolder::default())),
            demand: Vec::new(),
            hints: Vec::new(),
        };
        let at = (syncing.id.clone(), ENDPOINT.to_owned());
        state.push.peers.entry(at.clone()).or_default().scan = Some("7".into());
        assert!(driver.take_background(&mut state, &syncing, ENDPOINT));
        assert_eq!(
            state.push.peers[&at].scan, None,
            "a burst starts from the top"
        );
        state.push.peers.entry(at.clone()).or_default().scan = Some("9".into());
        driver.background_done(
            &key,
            &push::PushOutcome {
                pushed: 0,
                ..push::PushOutcome::default()
            },
        );
        let (tasks, queue) = mpsc::channel();
        driver.dispatch(1, &peers, &tasks);
        let task = queue.try_recv().unwrap();
        let moved = FeedCursor {
            sent: 5,
            received: 7,
        };
        driver.finish(&task, moved, Ok(()));
        assert!(driver.take_background(&mut state, &syncing, ENDPOINT));
        assert_eq!(state.push.peers[&at].scan, None);
    }

    #[test]
    fn a_running_driver_ends_sync_now_at_a_coded_failure_with_time_to_spare() {
        // The stub worker refuses `feed.outgoing`.
        let listener = Arc::new(Listener::default());
        let bulk = listener.lane_with("session", &[features::CONTENT_PUSH]);
        let lanes = ReadyLanes::one(false, bulk);
        let root = tempfile::tempdir().unwrap();
        let driver = start(
            Arc::new(lanes),
            Catalog::open(root.path()).unwrap(),
            Arc::new(RepoFolders(vec![(
                "repo-1".to_owned(),
                fake_worker(Arc::new(FakeFolder::default())),
                Vec::new(),
            )])),
            Seq::new(),
            passive(SHORT),
            Cancel::new(),
        )
        .unwrap();
        let began = Instant::now();
        let reply = driver.sync_now(&["repo-1".to_owned()], SYNC_NOW_BOUND, BurstMode::Full);
        assert!(began.elapsed() < LANE_WAIT, "{reply}");
        let error = folder_of(&reply)["error"].as_str().unwrap();
        assert!(error.contains("UNSUPPORTED_OPERATION"), "{reply}");
        assert!(error.contains(ENDPOINT), "{reply}");
        driver.stop(Duration::from_secs(5));
    }

    // ---- what a sync-now reply can claim -----------------------------------

    fn repo_folder() -> BurstFolder {
        BurstFolder {
            id: "repo-1".into(),
            repo: Some("repo-1".into()),
            seen: Instant::now(),
            error: None,
        }
    }

    fn settled_now(driver: &Driver, burst: u64) -> bool {
        let jobs = lock(&driver.jobs);
        let due = lock(&driver.due);
        burst_folder_settled(&jobs, &due, burst, &repo_folder())
    }

    fn report_now(driver: &Driver, burst: u64) -> Value {
        burst_report(lock(&driver.due).bursts.get(&burst), &repo_folder(), None)
    }

    /// The burst's exchanges went through: only the push steps are left.
    fn exchange_done(driver: &Driver) {
        lock(&driver.due).exchange.clear();
    }

    /// A passive driver of `repo-1`, not running, whose worker holds `held`.
    fn burst_driver(
        lanes: impl Lanes + 'static,
        held: &[Object],
    ) -> (Arc<Driver>, tempfile::TempDir) {
        let fake = Arc::new(FakeFolder {
            held: held.to_vec(),
            ..FakeFolder::default()
        });
        let root = tempfile::tempdir().unwrap();
        let driver = Driver::with_policy(
            Arc::new(lanes),
            Catalog::open(root.path()).unwrap(),
            Arc::new(RepoFolders(vec![(
                "repo-1".to_owned(),
                fake_worker(fake),
                Vec::new(),
            )])),
            Seq::new(),
            passive(HOUR),
            Cancel::new(),
        );
        (driver, root)
    }

    #[test]
    fn a_peer_that_takes_no_pushes_is_reported_and_settles_on_no_count() {
        let listener = Arc::new(Listener {
            missing: wanted_of(&objects("there", 1)),
            ..Listener::default()
        });
        let bulk = listener.lane_with("session", &[]);
        let (driver, _root) = burst_driver(ReadyLanes::one(false, bulk), &[]);
        let burst = open_burst(&driver, &[pair(ENDPOINT, "repo-1")]);
        let mut state = PumpState::default();
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
        // Its metadata still moves: that is not over yet.
        assert!(lock(&driver.due).in_burst(&pair(ENDPOINT, "repo-1")));
        assert!(!settled_now(&driver, burst));
        exchange_done(&driver);
        assert!(settled_now(&driver, burst));
        let report = report_now(&driver, burst);
        assert!(report["remaining"].is_null(), "{report}");
        let error = report["error"].as_str().unwrap_or_default();
        assert!(error.starts_with("PEER_PUSH_UNAVAILABLE"), "{report}");
        assert!(error.contains(ENDPOINT), "{report}");
    }

    #[test]
    fn a_metadata_only_burst_takes_no_push_step() {
        let listener = Arc::new(Listener {
            missing: wanted_of(&objects("there", 1)),
            demand: wanted_of(&objects("read", 1)),
            ..Listener::default()
        });
        let bulk = listener.lane_with("session", &[features::CONTENT_PUSH]);
        let (driver, _root) = burst_driver(ReadyLanes::one(false, bulk), &[]);
        let key = pair(ENDPOINT, "repo-1");
        let burst = open_burst_of(&driver, std::slice::from_ref(&key), BurstMode::MetadataOnly);
        {
            let due = lock(&driver.due);
            assert!(due.exchange.contains(&key));
            assert!(!due.demand.contains(&key) && !due.background.contains(&key));
            assert!(due.push_held(&key));
        }
        // A mark the burst did not make, as a local change or a notify makes it.
        driver.local_change("repo-1");
        lock(&driver.due).demand.insert(key.clone());
        let mut state = PumpState::default();
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
        assert!(!settled_now(&driver, burst), "its exchange has not run");
        driver.burst_exchanged(&key, true, true, &Ok(()));
        exchange_done(&driver);
        assert!(settled_now(&driver, burst));
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
        let report = report_now(&driver, burst);
        assert!(report["error"].is_null(), "{report}");
        assert_eq!(report["pushed"], 0, "{report}");
        // A full burst beside it lifts the hold.
        open_burst(&driver, std::slice::from_ref(&key));
        assert!(!lock(&driver.due).push_held(&key));
    }

    #[test]
    fn a_full_burst_beside_a_metadata_only_one_leaves_its_report_alone() {
        let listener = Arc::new(Listener::default());
        let bulk = listener.lane_with("session", &[features::CONTENT_PUSH]);
        let (driver, _root) = burst_driver(ReadyLanes::one(false, bulk), &[]);
        let key = pair(ENDPOINT, "repo-1");
        let meta = open_burst_of(&driver, std::slice::from_ref(&key), BurstMode::MetadataOnly);
        let full = open_burst(&driver, std::slice::from_ref(&key));
        let outcome = push::PushOutcome {
            pushed: 3,
            remaining: 2,
            unsent: 2,
            left: 2,
            listed: true,
            swept: true,
            error: Some(Failure::Error("FATAL_STORE_ERROR: boom".to_owned())),
            unlisted: Some(push::Unlisted::NoPush),
            ..push::PushOutcome::default()
        };
        driver.background_done(&key, &outcome);
        for burst in lock(&driver.due).bursts.values_mut() {
            for state in burst.pairs.values_mut() {
                state.exchanged = 1;
            }
        }
        exchange_done(&driver);
        let report = report_now(&driver, meta);
        assert_eq!(report["pushed"], 0, "{report}");
        assert!(report["remaining"].is_null(), "{report}");
        assert!(report["error"].is_null(), "{report}");
        assert!(settled_now(&driver, meta));
        let report = report_now(&driver, full);
        assert_eq!(report["pushed"], 3, "{report}");
        assert_eq!(report["remaining"], 2, "{report}");
        assert!(report["error"].is_string(), "{report}");
    }

    /// Bulk lanes that look Ready when a round starts and are gone when asked.
    struct GoneLanes;

    impl Lanes for GoneLanes {
        fn ready_peers(&self) -> Vec<ReadyPeer> {
            Vec::new()
        }
        fn bulk_endpoints(&self) -> Vec<String> {
            vec![ENDPOINT.to_owned()]
        }
        fn bulk_lane(&self, _: &str, _: &Digest) -> Option<Conn> {
            None
        }
        fn any_bulk_lane(&self, _: &str) -> Option<Conn> {
            None
        }
    }

    #[test]
    fn a_lane_gone_during_a_round_leaves_the_pair_due_and_uncounted() {
        let (driver, _root) = burst_driver(GoneLanes, &[]);
        let key = pair(ENDPOINT, "repo-1");
        let burst = open_burst(&driver, std::slice::from_ref(&key));
        exchange_done(&driver);
        let mut state = PumpState::default();
        driver.pump_round(&mut state, driver.wake.current());
        assert!(!settled_now(&driver, burst));
        assert!(lock(&driver.due).background.contains(&key));
        let report = report_now(&driver, burst);
        assert!(report["remaining"].is_null(), "{report}");
        assert!(report["error"].is_null(), "{report}");
    }

    /// One bulk lane that does not own `skip` while it is set.
    struct SkippingLanes {
        lane: Conn,
        skip: Arc<Mutex<Option<Digest>>>,
    }

    impl Lanes for SkippingLanes {
        fn ready_peers(&self) -> Vec<ReadyPeer> {
            Vec::new()
        }
        fn bulk_endpoints(&self) -> Vec<String> {
            vec![ENDPOINT.to_owned()]
        }
        fn bulk_lane(&self, _: &str, digest: &Digest) -> Option<Conn> {
            (*lock(&self.skip) != Some(*digest)).then(|| self.lane.clone())
        }
        fn any_bulk_lane(&self, _: &str) -> Option<Conn> {
            Some(self.lane.clone())
        }
    }

    #[test]
    fn an_empty_page_past_the_top_of_the_scan_does_not_settle_a_burst() {
        let all = objects("object", OBJECT_BATCH);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            ..Listener::default()
        });
        let skip = Arc::new(Mutex::new(Some(all[0].digest)));
        let lanes = SkippingLanes {
            lane: listener.lane_with("session", &[features::CONTENT_PUSH]),
            skip: skip.clone(),
        };
        let (driver, _root) = burst_driver(lanes, &all);
        let burst = open_burst(&driver, &[pair(ENDPOINT, "repo-1")]);
        exchange_done(&driver);
        let mut state = PumpState::default();
        // The page goes but for the digest whose lane is not Ready, and the scan
        // moves past it.
        let stored = driver.pump_round(&mut state, driver.wake.current());
        assert_eq!(stored, OBJECT_BATCH - 1);
        // So the next page, past that one, is empty. That says nothing of it.
        driver.pump_round(&mut state, driver.wake.current());
        assert!(
            !settled_now(&driver, burst),
            "{}",
            report_now(&driver, burst)
        );
        // Nor does the reply take it for all sent, should time run out here.
        let report = report_now(&driver, burst);
        assert_eq!(report["remaining"], 1, "{report}");
        // The lane is back: the sweep from the top finds it.
        *lock(&skip) = None;
        for _ in 0..4 {
            driver.pump_round(&mut state, driver.wake.current());
        }
        assert_eq!(listener.stored().len(), OBJECT_BATCH);
        assert!(settled_now(&driver, burst));
        let report = report_now(&driver, burst);
        assert_eq!(report["remaining"], 0, "{report}");
        assert_eq!(report["pushed"], OBJECT_BATCH, "{report}");
        assert!(report["error"].is_null(), "{report}");
    }

    #[test]
    fn a_timed_out_store_stays_in_the_burst_and_goes_through_at_half_the_batch() {
        let all = objects("slow-link", OBJECT_BATCH);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            stall_above: Some(16),
            ..Listener::default()
        });
        let bulk = listener.lane_with("session", &[features::CONTENT_PUSH]);
        let (driver, _root) = burst_driver(ReadyLanes::one(false, bulk), &all);
        let key = pair(ENDPOINT, "repo-1");
        let burst = open_burst(&driver, std::slice::from_ref(&key));
        exchange_done(&driver);
        let mut state = PumpState {
            push: PushState {
                deadline: |_| Duration::from_millis(300),
                ..PushState::default()
            },
            ..PumpState::default()
        };
        driver.pump_round(&mut state, driver.wake.current());
        assert!(listener.stored().is_empty());
        assert!(lock(&driver.due).in_burst(&key), "one timeout ended it");
        assert!(!settled_now(&driver, burst));
        let end = Instant::now() + Duration::from_secs(30);
        while !settled_now(&driver, burst) {
            assert!(Instant::now() < end, "{:?}", listener.calls());
            state.push.resting.clear();
            driver.pump_round(&mut state, driver.wake.current());
        }
        assert_eq!(listener.stored().len(), OBJECT_BATCH);
        // What it failed on is past: nothing to report.
        let report = report_now(&driver, burst);
        assert!(report["error"].is_null(), "{report}");
        assert_eq!(report["remaining"], 0, "{report}");
    }

    #[test]
    fn a_timed_out_exchange_is_asked_again_and_the_budget_reports_the_last_failure() {
        let (driver, _root, control) = dispatching(passive(SHORT));
        let peers = [ready_peer(ENDPOINT, "control", false, &control)];
        let key = pair(ENDPOINT, "repo-1");
        let burst = open_burst(&driver, std::slice::from_ref(&key));
        let timed_out = || peer_failure("meta.exchange", CallError::Timeout { after_ms: 7 });
        let (tasks, queue) = mpsc::channel();
        for _ in 0..2 {
            driver.dispatch(1, &peers, &tasks);
            let task = queue.try_recv().expect("it was not asked again");
            driver.finish(&task, task.cursor, Err(timed_out()));
            assert!(lock(&driver.due).in_burst(&key));
            assert!(!settled_now(&driver, burst));
        }
        let reply = driver.close_burst(
            burst,
            &[repo_folder()],
            Some("SYNC_NOW_TIMEOUT: not finished".to_owned()),
        );
        let error = folder_of(&reply)["error"].as_str().unwrap();
        assert!(error.contains("REQUEST_TIMEOUT"), "{reply}");
        assert!(error.contains(ENDPOINT), "{reply}");
    }

    /// A burst over `ws://one` and `ws://two`, with `ws://one` wholly synced and
    /// `ws://two` given `not_there`'s answer: what `ws://two` makes of the folder.
    fn with_a_peer_that_lacks_the_repo(one_took_it: bool) -> Value {
        let (driver, _root, control) = dispatching(passive(SHORT));
        let (one, two) = (pair("ws://one", "repo-1"), pair("ws://two", "repo-1"));
        let burst = open_burst(&driver, &[one.clone(), two.clone()]);
        let peers = [
            ready_peer("ws://one", "control-1", false, &control),
            ready_peer("ws://two", "control-2", false, &control),
        ];
        let (tasks, queue) = mpsc::channel();
        driver.dispatch(1, &peers, &tasks);
        let not_there = || {
            peer_failure(
                "meta.exchange",
                CallError::Remote(ErrorWire::coded("REPO_NOT_PUBLISHED", "no such folder")),
            )
        };
        for task in queue.try_iter() {
            if task.key == two || !one_took_it {
                driver.finish(&task, task.cursor, Err(not_there()));
            } else {
                driver.finish(&task, task.cursor, Ok(()));
            }
        }
        if one_took_it {
            let mut due = lock(&driver.due);
            due.demand.remove(&one);
            due.background.remove(&one);
            let state = due
                .bursts
                .get_mut(&burst)
                .unwrap()
                .pairs
                .get_mut(&one)
                .unwrap();
            state.swept = true;
            state.remaining = Some(0);
        }
        assert!(settled_now(&driver, burst), "it holds the burst open");
        report_now(&driver, burst)
    }

    #[test]
    fn a_peer_that_answered_before_gets_a_while_to_publish_the_repo_again() {
        let (driver, _root, control) = dispatching(passive(SHORT));
        let peers = [ready_peer(ENDPOINT, "control", false, &control)];
        let key = pair(ENDPOINT, "repo-1");
        let burst = open_burst(&driver, std::slice::from_ref(&key));
        let not_there = || {
            Err(peer_failure(
                "meta.exchange",
                CallError::Remote(ErrorWire::coded("REPO_NOT_PUBLISHED", "restarting")),
            ))
        };
        let (tasks, queue) = mpsc::channel();
        driver.dispatch(1, &peers, &tasks);
        let task = queue.try_recv().unwrap();
        driver.finish(&task, task.cursor, Ok(()));
        exchange_done(&driver);
        lock(&driver.due).exchange.insert(key.clone());
        driver.dispatch(1, &peers, &tasks);
        let task = queue.try_recv().unwrap();
        driver.finish(&task, task.cursor, not_there());
        // Still wanted, not due until its next ask, and not settled meanwhile.
        assert!(lock(&driver.due).in_burst(&key));
        assert!(!lock(&driver.due).exchange.contains(&key));
        {
            let mut due = lock(&driver.due);
            due.demand.clear();
            due.background.clear();
            let state = due
                .bursts
                .get_mut(&burst)
                .unwrap()
                .pairs
                .get_mut(&key)
                .unwrap();
            state.swept = true;
        }
        assert!(!settled_now(&driver, burst));
        let (mut folders, mut seen) = (Vec::new(), BTreeMap::new());
        driver.burst_look(burst, &mut folders, &mut seen);
        assert!(!lock(&driver.due).exchange.contains(&key));
        // Its ask comes up: asked again.
        let set_absent = |since, ask| {
            let mut due = lock(&driver.due);
            let state = due
                .bursts
                .get_mut(&burst)
                .unwrap()
                .pairs
                .get_mut(&key)
                .unwrap();
            assert!(state.absent.is_some());
            state.absent = Some((since, ask));
        };
        let now = Instant::now();
        set_absent(now, now);
        driver.burst_look(burst, &mut folders, &mut seen);
        driver.dispatch(1, &peers, &tasks);
        let task = queue.try_recv().expect("it was not asked again");
        driver.finish(&task, task.cursor, not_there());
        // Too long: it is given up on, and reported as the only peer.
        set_absent(now.checked_sub(REPO_WAIT).unwrap(), now + HOUR);
        driver.burst_look(burst, &mut folders, &mut seen);
        assert!(!lock(&driver.due).in_burst(&key));
        assert!(settled_now(&driver, burst));
        let report = report_now(&driver, burst);
        let error = report["error"].as_str().unwrap();
        assert!(error.contains("REPO_NOT_PUBLISHED"), "{report}");
        // An answer in time would have cleared it.
        let mut state = BurstPair::default();
        state.note_absent("x".into(), now);
        state.answered();
        assert!(state.absent.is_none() && state.failure.is_none());
    }

    #[test]
    fn a_push_that_finds_the_folder_gone_is_asked_again_and_not_settled() {
        let all = objects("object", 1);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            unpublished: Some("repo-1".into()),
            ..Listener::default()
        });
        let bulk = listener.lane_with("session", &[features::CONTENT_PUSH]);
        let (driver, _root) = burst_driver(ReadyLanes::one(false, bulk), &all);
        let key = pair(ENDPOINT, "repo-1");
        let burst = open_burst(&driver, std::slice::from_ref(&key));
        exchange_done(&driver);
        let mut state = PumpState::default();
        driver.pump_round(&mut state, driver.wake.current());
        let due = lock(&driver.due);
        assert!(due.in_burst(&key));
        assert!(due.background.contains(&key));
        drop(due);
        assert!(!settled_now(&driver, burst));
    }

    #[test]
    fn a_peer_without_the_repo_is_no_error_when_another_peer_took_it() {
        let report = with_a_peer_that_lacks_the_repo(true);
        assert!(report["error"].is_null(), "{report}");
        assert_eq!(report["remaining"], 0, "{report}");
        assert_eq!(report["exchanged"], 1, "{report}");
    }

    #[test]
    fn a_repo_no_peer_took_is_reported() {
        let report = with_a_peer_that_lacks_the_repo(false);
        let error = report["error"].as_str().unwrap();
        assert!(error.contains("REPO_NOT_PUBLISHED"), "{report}");
        assert!(report["remaining"].is_null(), "{report}");
    }

    /// One pump round, and the listener's calls it made.
    fn calls_of_round(driver: &Driver, state: &mut PumpState, listener: &Listener) -> Vec<String> {
        let before = listener.calls().len();
        driver.pump_round(state, driver.wake.current());
        listener.calls()[before..].to_vec()
    }

    /// A driver of one repo over a bulk lane to `listener`.
    fn pushing_driver(
        listener: &Arc<Listener>,
        notify: bool,
        policy: SyncPolicy,
    ) -> (Arc<Driver>, tempfile::TempDir) {
        let bulk = listener.lane_with("session", &[features::CONTENT_PUSH]);
        policy_driver(ReadyLanes::one(notify, bulk), &["repo-1"], policy)
    }

    #[test]
    fn an_idle_pair_asks_nothing_until_something_marks_it_due() {
        let listener = Arc::new(Listener::default());
        let (driver, _root) = pushing_driver(&listener, true, active(SHORT, HOUR));
        let mut state = PumpState::default();
        // New: its first poll.
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["wanted/0", "wanted/32"]
        );
        // Idle, past the sync interval: nothing.
        for _ in 0..3 {
            thread::sleep(PAST_SHORT);
            assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
        }
        // A metadata notify is for the scheduler.
        driver.notified(ENDPOINT, &metadata("repo-1"));
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
        // A demand notify runs only the demand step.
        driver.notified(ENDPOINT, &demand_of("repo-1"));
        assert_eq!(calls_of_round(&driver, &mut state, &listener), ["wanted/0"]);
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
        // A local change runs only the background step.
        driver.local_change("repo-1");
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["wanted/32"]
        );
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
        // So does an exchange, once it moved events.
        let peers = driver.lanes.ready_peers();
        lock(&driver.due).exchange.insert(pair(ENDPOINT, "repo-1"));
        let (tasks, queue) = mpsc::channel();
        driver.dispatch(1, &peers, &tasks);
        let task = queue.try_recv().unwrap();
        driver.finish(&task, task.cursor, Ok(()));
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
        lock(&driver.due).exchange.insert(pair(ENDPOINT, "repo-1"));
        driver.dispatch(1, &peers, &tasks);
        let task = queue.try_recv().unwrap();
        let wake = driver.wake.mark();
        let moved = FeedCursor {
            sent: task.cursor.sent + 1,
            ..task.cursor
        };
        driver.finish(&task, moved, Ok(()));
        assert_ne!(driver.wake.mark(), wake);
        assert_eq!(driver.wake.current(), wake.value);
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["wanted/32"]
        );
        // And its poll runs both.
        state.polls.insert(pair(ENDPOINT, "repo-1"), Instant::now());
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["wanted/0", "wanted/32"]
        );
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
    }

    #[test]
    fn a_session_without_notify_keeps_the_sync_interval_for_its_pushes_too() {
        let listener = Arc::new(Listener::default());
        let (driver, _root) = pushing_driver(&listener, false, active(SHORT, HOUR));
        let mut state = PumpState::default();
        assert_eq!(calls_of_round(&driver, &mut state, &listener).len(), 2);
        thread::sleep(PAST_SHORT);
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["wanted/0", "wanted/32"]
        );
    }

    #[test]
    fn a_pair_stays_due_until_a_page_lists_nothing() {
        // This side lacks all of a page and of what follows it.
        let lacked = objects("lacked", OBJECT_BATCH + 1);
        let listener = Arc::new(Listener {
            missing: wanted_of(&lacked),
            ..Listener::default()
        });
        let (driver, _root) = pushing_driver(&listener, true, active(HOUR, HOUR));
        let mut state = PumpState::default();
        assert_eq!(calls_of_round(&driver, &mut state, &listener).len(), 2);
        assert!(background_due(&driver), "the scan is not over");
        assert_eq!(driver.pump_wait(), DUE_CHECK);
        // Its last page: the whole sweep stored nothing, and that is the end.
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["wanted/32"]
        );
        assert!(!background_due(&driver));
        assert_eq!(driver.pump_wait(), HOUR);
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
    }

    #[test]
    fn a_sweep_that_stored_something_goes_over_the_scan_again() {
        // The last of two pages holds only what this side lacks.
        let all = objects("object", OBJECT_BATCH + 1);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            ..Listener::default()
        });
        let fake = FakeFolder {
            held: all[1..OBJECT_BATCH].to_vec(),
            ..FakeFolder::default()
        };
        let (driver, _root) = holding_driver(&listener, fake, Seq::new());
        let mut state = PumpState::default();
        // Stored most of its page, and moved past what it lacks.
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener)[..2],
            ["wanted/0", "wanted/32"]
        );
        assert!(background_due(&driver));
        // The end of the scan, having stored something: from the top again.
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["wanted/32"]
        );
        assert!(background_due(&driver));
        // A whole sweep that stores nothing, and it is over.
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["wanted/32"]
        );
        assert!(!background_due(&driver));
    }

    #[test]
    fn a_listing_that_failed_waits_for_the_next_trigger() {
        let listener = Arc::new(Listener {
            unpublished: Some("repo-1".into()),
            ..Listener::default()
        });
        let (driver, _root) = pushing_driver(&listener, true, active(HOUR, HOUR));
        let mut state = PumpState::default();
        let key = ("repo-1".to_owned(), ENDPOINT.to_owned());
        // The demand step fails and rests the pair: the background step keeps
        // its mark, and nothing is asked until the rest ends.
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["objects.wanted"]
        );
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
        assert!(!lock(&driver.due).background.is_empty());
        // Then it fails too, and that ends it: no page, nothing left to do.
        state.push.peers.get_mut(&key).unwrap().rest_until = None;
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["objects.wanted"]
        );
        assert!(lock(&driver.due).background.is_empty());
        state.push.peers.get_mut(&key).unwrap().rest_until = None;
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
        // Until something marks it due again.
        driver.local_change("repo-1");
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["objects.wanted"]
        );
    }

    /// A driver of one repo whose worker is `fake`, over a bulk lane to
    /// `listener` of a notified session, that never polls again.
    fn holding_driver(
        listener: &Arc<Listener>,
        fake: FakeFolder,
        wake: Arc<Seq>,
    ) -> (Arc<Driver>, tempfile::TempDir) {
        let root = tempfile::tempdir().unwrap();
        let bulk = listener.lane_with("session", &[features::CONTENT_PUSH]);
        let driver = Driver::with_policy(
            Arc::new(ReadyLanes::one(true, bulk)),
            Catalog::open(root.path()).unwrap(),
            Arc::new(RepoFolders(vec![(
                "repo-1".into(),
                fake_worker(Arc::new(fake)),
                Vec::new(),
            )])),
            wake,
            active(HOUR, HOUR),
            Cancel::new(),
        );
        (driver, root)
    }

    fn background_due(driver: &Driver) -> bool {
        lock(&driver.due)
            .background
            .contains(&pair(ENDPOINT, "repo-1"))
    }

    #[test]
    fn a_store_that_failed_stays_due_and_waits_out_its_rest() {
        let all = objects("object", 1);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            stall_above: Some(0),
            ..Listener::default()
        });
        let fake = FakeFolder {
            held: all,
            ..FakeFolder::default()
        };
        let (driver, _root) = holding_driver(&listener, fake, Seq::new());
        let mut state = PumpState::default();
        state.push.deadline = |_| Duration::from_millis(100);
        // The store outlasts its deadline: what it held stays to do.
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["wanted/0", "wanted/32", "store"]
        );
        assert!(background_due(&driver));
        // While the endpoint rests the mark waits.
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
        assert!(background_due(&driver));
        // A wake that already moved stops the step before it takes its mark.
        state.push.resting.clear();
        let before = listener.calls().len();
        driver.pump_round(&mut state, driver.wake.current().wrapping_add(1));
        assert_eq!(listener.calls().len(), before);
        assert!(background_due(&driver));
    }

    #[test]
    fn a_pass_a_wake_stopped_stays_due() {
        let all = objects("object", 1);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            ..Listener::default()
        });
        let wake = Seq::new();
        let fake = FakeFolder {
            held: all,
            wake_on_read: Some(wake.clone()),
            ..FakeFolder::default()
        };
        let (driver, _root) = holding_driver(&listener, fake, wake);
        let mut state = PumpState::default();
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["wanted/0", "wanted/32"]
        );
        assert!(background_due(&driver));
    }

    #[test]
    fn a_mark_of_a_repo_not_here_does_not_keep_the_pump_awake() {
        let listener = Arc::new(Listener::default());
        let (driver, _root) = pushing_driver(&listener, true, active(HOUR, HOUR));
        let wake = driver.wake.mark();
        driver.notified(ENDPOINT, &demand_of("repo-elsewhere"));
        assert_eq!(driver.pump_wait(), HOUR);
        // No folder here: it is not even kept, and rouses nothing.
        assert!(lock(&driver.due).demand.is_empty());
        assert_eq!(driver.wake.mark(), wake);
        driver.notified(ENDPOINT, &demand_of("repo-1"));
        assert_eq!(driver.pump_wait(), DUE_CHECK);
        assert!(driver.wake.current() > wake.value);
    }

    /// A folder this daemon has and whose slot is busy now, then one running.
    struct BusySlot(RepoFolders);

    impl SyncFolders for BusySlot {
        fn syncing(&self) -> Vec<SyncFolder> {
            self.0.syncing()
        }
        fn knows(&self, repo: &str) -> bool {
            repo == "repo-busy" || self.0.knows(repo)
        }
    }

    #[test]
    fn a_mark_for_a_busy_folder_waits_for_it_and_interrupts_nothing() {
        let listener = Arc::new(Listener::default());
        let bulk = listener.lane_with("session", &[features::CONTENT_PUSH]);
        let root = tempfile::tempdir().unwrap();
        let folders = RepoFolders(vec![(
            "repo-1".into(),
            fake_worker(Arc::new(FakeFolder::default())),
            Vec::new(),
        )]);
        let driver = Driver::with_policy(
            Arc::new(ReadyLanes::one(true, bulk)),
            Catalog::open(root.path()).unwrap(),
            Arc::new(BusySlot(folders)),
            Seq::new(),
            active(HOUR, HOUR),
            Cancel::new(),
        );
        let wake = driver.wake.mark();
        driver.notified(ENDPOINT, &demand_of("repo-busy"));
        driver.notified(ENDPOINT, &metadata("repo-busy"));
        // Kept, and the pump neither stays awake for it nor stops what it does.
        let key = pair(ENDPOINT, "repo-busy");
        assert!(lock(&driver.due).demand.contains(&key));
        assert!(lock(&driver.due).exchange.contains(&key));
        assert_eq!(driver.pump_wait(), HOUR);
        assert_ne!(driver.wake.mark(), wake);
        assert_eq!(driver.wake.current(), wake.value);
        // A round that cannot run it leaves it.
        driver.pump_round(&mut PumpState::default(), wake.value);
        assert!(lock(&driver.due).demand.contains(&key));
        // Its repeat rouses a pump asleep on it, and so finds the slot free.
        let held = driver.wake.mark();
        driver.notified(ENDPOINT, &demand_of("repo-busy"));
        assert_ne!(driver.wake.mark(), held);
        assert_eq!(driver.wake.current(), wake.value);
    }

    /// The driver a hook acts on, set once the driver exists.
    type LateDriver = Arc<OnceLock<Weak<Driver>>>;

    fn on_driver(late: &LateDriver, act: impl Fn(&Driver) + Send + Sync + 'static) -> Hook {
        let late = late.clone();
        Box::new(move || {
            if let Some(driver) = late.get().and_then(Weak::upgrade) {
                act(&driver);
            }
        })
    }

    /// Exchanges that moved events finish, for every pair the dispatch starts.
    fn exchanges_move_events(driver: &Driver) {
        lock(&driver.due).exchange.insert(pair(ENDPOINT, "repo-1"));
        let (tasks, queue) = mpsc::channel();
        driver.dispatch(1, &driver.lanes.ready_peers(), &tasks);
        for task in queue.try_iter() {
            let moved = FeedCursor {
                sent: task.cursor.sent + 1,
                ..task.cursor
            };
            driver.finish(&task, moved, Ok(()));
        }
    }

    /// Two folders that each have a page to pull from a listener, which also
    /// wants a page pushed; `during_fetch` runs during every pull.
    struct PullAndPush {
        listener: Arc<Listener>,
        driver: Arc<Driver>,
        _root: tempfile::TempDir,
    }

    fn pull_and_push(act: impl Fn(&Driver) + Send + Sync + 'static) -> PullAndPush {
        let (first, second, there) = (objects("first", 1), objects("second", 1), objects("x", 1));
        let late = LateDriver::default();
        let listener = Arc::new(Listener {
            held: [first.clone(), second.clone()].concat(),
            missing: wanted_of(&there),
            during_fetch: Some(on_driver(&late, act)),
            ..Listener::default()
        });
        let folder = |missing: &[Object], repo: &str| {
            let fake = Arc::new(FakeFolder {
                missing: wanted_of(missing),
                held: there.clone(),
                ..FakeFolder::default()
            });
            (repo.to_owned(), fake_worker(fake), Vec::new())
        };
        let root = tempfile::tempdir().unwrap();
        let driver = Driver::with_policy(
            Arc::new(ReadyLanes::one(
                true,
                listener.lane_with("session", &[features::CONTENT_PUSH]),
            )),
            Catalog::open(root.path()).unwrap(),
            Arc::new(RepoFolders(vec![
                folder(&first, "repo-1"),
                folder(&second, "repo-2"),
            ])),
            Seq::new(),
            active(HOUR, HOUR),
            Cancel::new(),
        );
        late.set(Arc::downgrade(&driver)).unwrap();
        PullAndPush {
            listener,
            driver,
            _root: root,
        }
    }

    fn calls_named(calls: &[String], name: &str) -> usize {
        calls.iter().filter(|call| *call == name).count()
    }

    #[test]
    fn a_local_change_during_a_background_page_does_not_interrupt_the_pass() {
        let PullAndPush {
            listener, driver, ..
        } = pull_and_push(|driver| driver.local_change("repo-1"));
        let mut state = PumpState::default();
        let calls = calls_of_round(&driver, &mut state, &listener);
        assert_eq!(calls_named(&calls, "fetch"), 2, "{calls:?}");
        assert_eq!(calls_named(&calls, "store"), 1, "{calls:?}");
    }

    #[test]
    fn an_exchange_that_moved_events_during_a_background_page_does_not_interrupt_the_pass() {
        let PullAndPush {
            listener, driver, ..
        } = pull_and_push(exchanges_move_events);
        let mut state = PumpState::default();
        let calls = calls_of_round(&driver, &mut state, &listener);
        assert_eq!(calls_named(&calls, "fetch"), 2, "{calls:?}");
        assert_eq!(calls_named(&calls, "store"), 1, "{calls:?}");
    }

    #[test]
    fn a_demand_notify_for_a_repo_not_here_does_not_interrupt_the_pass() {
        let PullAndPush {
            listener, driver, ..
        } = pull_and_push(|driver| driver.notified(ENDPOINT, &demand_of("repo-elsewhere")));
        let mut state = PumpState::default();
        let calls = calls_of_round(&driver, &mut state, &listener);
        assert_eq!(calls_named(&calls, "fetch"), 2, "{calls:?}");
        assert_eq!(calls_named(&calls, "store"), 1, "{calls:?}");
    }

    #[test]
    fn a_demand_notify_for_a_repo_here_does_interrupt_the_pass() {
        let PullAndPush {
            listener, driver, ..
        } = pull_and_push(|driver| driver.notified(ENDPOINT, &demand_of("repo-2")));
        let mut state = PumpState::default();
        let calls = calls_of_round(&driver, &mut state, &listener);
        // The page in flight finishes, and nothing after it starts.
        assert_eq!(calls_named(&calls, "fetch"), 1, "{calls:?}");
        assert_eq!(calls_named(&calls, "store"), 0, "{calls:?}");
        // The next round begins with what the reader waits for.
        let calls = calls_of_round(&driver, &mut state, &listener);
        assert_eq!(calls[0], "wanted/0", "{calls:?}");
    }

    #[test]
    fn a_local_change_during_a_read_does_not_discard_it_before_its_store() {
        let all = objects("object", 1);
        let late = LateDriver::default();
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            ..Listener::default()
        });
        let fake = FakeFolder {
            held: all,
            during_read: Some(on_driver(&late, |driver| driver.local_change("repo-1"))),
            ..FakeFolder::default()
        };
        let (driver, _root) = holding_driver(&listener, fake, Seq::new());
        late.set(Arc::downgrade(&driver)).unwrap();
        let mut state = PumpState::default();
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["wanted/0", "wanted/32", "store"]
        );
    }

    #[test]
    fn a_local_change_rouses_a_sleeping_pump() {
        let listener = Arc::new(Listener::default());
        let bulk = listener.lane_with("session", &[features::CONTENT_PUSH]);
        let fake = Arc::new(FakeFolder::default());
        let root = tempfile::tempdir().unwrap();
        let driver = start(
            Arc::new(ReadyLanes::one(true, bulk)),
            Catalog::open(root.path()).unwrap(),
            Arc::new(RepoFolders(vec![(
                "repo-1".into(),
                fake_worker(fake),
                Vec::new(),
            )])),
            Seq::new(),
            active(HOUR, HOUR),
            Cancel::new(),
        )
        .unwrap();
        let backgrounds = || calls_named(&listener.calls(), "wanted/32");
        let wait_for = |count: usize| {
            let end = Instant::now() + Duration::from_secs(5);
            while backgrounds() < count {
                assert!(Instant::now() < end, "{:?}", listener.calls());
                thread::sleep(Duration::from_millis(5));
            }
        };
        // Its first poll, then nothing is due for an hour.
        wait_for(1);
        thread::sleep(Duration::from_millis(100));
        assert_eq!(backgrounds(), 1);
        driver.local_change("repo-1");
        wait_for(2);
        driver.stop(Duration::from_secs(2));
    }

    #[test]
    fn a_notify_for_a_folder_not_here_leaves_no_mark() {
        let (driver, _root, _control) = dispatching(active(HOUR, HOUR));
        let (poke, wake) = (driver.poke.current(), driver.wake.mark());
        for index in 0..100 {
            driver.notified(ENDPOINT, &metadata(&format!("made-up-{index}")));
            driver.notified(ENDPOINT, &demand_of(&format!("made-up-{index}")));
        }
        let due = lock(&driver.due);
        assert!(due.exchange.is_empty() && due.demand.is_empty());
        assert_eq!(driver.poke.current(), poke);
        assert_eq!(driver.wake.mark(), wake);
    }

    #[test]
    fn the_marks_of_a_peer_with_no_lane_left_are_dropped() {
        let listener = Arc::new(Listener::default());
        let (driver, _root) = pushing_driver(&listener, true, active(HOUR, HOUR));
        let mut state = PumpState::default();
        driver.pump_round(&mut state, driver.wake.current());
        driver.notified("ws://gone", &demand_of("repo-1"));
        lock(&driver.due)
            .background
            .insert(pair("ws://gone", "repo-1"));
        driver.pump_round(&mut state, driver.wake.current());
        let due = lock(&driver.due);
        assert!(due.demand.is_empty());
        assert!(due.background.is_empty());
    }

    // ---- marks: what a peer can make this side remember --------------------

    #[test]
    fn a_flood_of_made_up_repos_does_not_stop_the_marks_of_a_real_one() {
        let wanted = objects("wanted", 1);
        let listener = Arc::new(Listener {
            demand: wanted_of(&wanted),
            ..Listener::default()
        });
        let fake = FakeFolder {
            held: wanted,
            ..FakeFolder::default()
        };
        let (driver, _root) = holding_driver(&listener, fake, Seq::new());
        let key = pair(ENDPOINT, "repo-1");
        let mut state = PumpState::default();
        state.polls.insert(key.clone(), Instant::now() + HOUR);
        for index in 0..5000 {
            let made_up = format!("made-up-{index}");
            driver.notified(ENDPOINT, &metadata(&made_up));
            driver.notified(ENDPOINT, &demand_of(&made_up));
        }
        // A waiting reader is still served, not left for the poll.
        driver.notified(ENDPOINT, &demand_of("repo-1"));
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["wanted/0", "store"]
        );
        // And a local change still marks its exchange and its push.
        driver.local_change("repo-1");
        assert!(lock(&driver.due).exchange.contains(&key));
        assert_eq!(
            calls_of_round(&driver, &mut state, &listener),
            ["wanted/32"]
        );
    }

    #[test]
    fn the_pump_purges_the_marks_of_a_repo_or_a_peer_that_is_gone() {
        let listener = Arc::new(Listener::default());
        let (driver, _root) = pushing_driver(&listener, true, active(HOUR, HOUR));
        {
            let mut due = lock(&driver.due);
            for gone in [pair(ENDPOINT, "repo-gone"), pair("ws://gone", "repo-1")] {
                due.exchange.insert(gone.clone());
                due.demand.insert(gone.clone());
                due.background.insert(gone);
            }
        }
        let mut state = PumpState::default();
        driver.pump_round(&mut state, driver.wake.current());
        let due = lock(&driver.due);
        assert!(due.exchange.is_empty(), "{:?}", due.exchange);
        assert!(due.demand.is_empty(), "{:?}", due.demand);
        assert!(due.background.is_empty(), "{:?}", due.background);
    }

    #[test]
    fn a_repeated_demand_notify_rouses_a_pump_whose_mark_is_held() {
        let listener = Arc::new(Listener::default());
        let (driver, _root) = pushing_driver(&listener, true, active(HOUR, HOUR));
        driver.notified(ENDPOINT, &demand_of("repo-1"));
        let held = driver.wake.mark();
        driver.notified(ENDPOINT, &demand_of("repo-1"));
        assert_ne!(driver.wake.mark(), held, "the repeat was swallowed");
        // It only rouses: the bump of the first one already stopped what it had to.
        assert_eq!(driver.wake.current(), held.value);
    }

    // ---- passes that store nothing -----------------------------------------

    #[test]
    fn a_permanently_refused_digest_goes_idle_on_an_active_host() {
        let all = objects("object", 1);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            refuse: [all[0].digest].into(),
            ..Listener::default()
        });
        let fake = FakeFolder {
            held: all,
            ..FakeFolder::default()
        };
        let (driver, _root) = holding_driver(&listener, fake, Seq::new());
        let mut state = PumpState::default();
        for _ in 0..6 {
            driver.pump_round(&mut state, driver.wake.current());
        }
        let calls = listener.calls();
        assert_eq!(calls_named(&calls, "store"), 1, "{calls:?}");
        assert!(!background_due(&driver));
        assert_eq!(driver.pump_wait(), HOUR);
    }

    #[test]
    fn more_lacked_digests_than_an_exclude_holds_go_idle_on_an_active_host() {
        let lacked = objects("lacked", MAX_EXCLUDE + 44);
        let listener = Arc::new(Listener {
            missing: wanted_of(&lacked),
            ..Listener::default()
        });
        let (driver, _root) = holding_driver(&listener, FakeFolder::default(), Seq::new());
        let mut state = PumpState::default();
        for _ in 0..20 {
            driver.pump_round(&mut state, driver.wake.current());
            if !background_due(&driver) {
                break;
            }
        }
        assert!(!background_due(&driver), "{:?}", listener.calls());
        assert_eq!(driver.pump_wait(), HOUR);
        assert!(calls_of_round(&driver, &mut state, &listener).is_empty());
    }

    #[test]
    fn more_lacked_digests_than_an_exclude_holds_end_a_burst_early() {
        let lacked = objects("lacked", MAX_EXCLUDE + 44);
        let listener = Arc::new(Listener {
            missing: wanted_of(&lacked),
            ..Listener::default()
        });
        let bulk = listener.lane_with("session", &[features::CONTENT_PUSH]);
        let (driver, _root) = burst_driver(ReadyLanes::one(false, bulk), &[]);
        let burst = open_burst(&driver, &[pair(ENDPOINT, "repo-1")]);
        exchange_done(&driver);
        let mut state = PumpState::default();
        for _ in 0..20 {
            driver.pump_round(&mut state, driver.wake.current());
            if settled_now(&driver, burst) {
                break;
            }
        }
        let report = report_now(&driver, burst);
        assert!(settled_now(&driver, burst), "{report}");
        // None of it is this side's to send.
        assert_eq!(report["remaining"], 0, "{report}");
        assert!(report["error"].is_null(), "{report}");
    }

    #[test]
    fn a_burst_reports_a_refusal_by_its_code_and_does_not_call_it_done() {
        let all = objects("object", OBJECT_BATCH + 8);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            refuse: [all[3].digest].into(),
            ..Listener::default()
        });
        let bulk = listener.lane_with("session", &[features::CONTENT_PUSH]);
        let (driver, _root) = burst_driver(ReadyLanes::one(false, bulk), &all);
        let burst = open_burst(&driver, &[pair(ENDPOINT, "repo-1")]);
        exchange_done(&driver);
        let mut state = PumpState::default();
        for _ in 0..20 {
            driver.pump_round(&mut state, driver.wake.current());
            if settled_now(&driver, burst) {
                break;
            }
        }
        let report = report_now(&driver, burst);
        assert!(settled_now(&driver, burst), "{report}");
        assert_eq!(report["pushed"], all.len() - 1, "{report}");
        // Excluded from the listing by now, and still known to be unsent.
        assert_eq!(report["remaining"], 1, "{report}");
        let error = report["error"].as_str().unwrap_or_default();
        assert!(error.starts_with("OBJECT_NOT_WANTED"), "{report}");
        assert!(error.contains(ENDPOINT), "{report}");
        let calls = listener.calls();
        assert!(calls_named(&calls, "store") <= 3, "{calls:?}");
    }

    /// `until` is about `expected` from now, to within what a test takes to look.
    fn close_to(until: Instant, expected: Duration) {
        let left = until.saturating_duration_since(Instant::now());
        assert!(
            left <= expected && left + Duration::from_secs(1) > expected,
            "{left:?} left, expected about {expected:?}"
        );
    }

    #[test]
    fn a_failing_store_rests_ever_longer_and_success_starts_over() {
        let all = objects("object", 2);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all[..1]),
            fail_store: Mutex::new(Some("FATAL_STORE_ERROR".into())),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let folder = pushing_folder("repo-1", &all);
        let mut state = PushState::default();
        for seconds in [1, 2, 4, 8, 16, 32, 60, 60] {
            let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
            assert!(outcome.error.is_some());
            let peer = push_peer(&mut state, &folder);
            close_to(peer.rest_until.unwrap(), Duration::from_secs(seconds));
            peer.rest_until = Some(Instant::now());
        }
        // It works again: the next failure is a first one.
        *lock(&listener.fail_store) = None;
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, 1);
        let failing = Arc::new(Listener {
            missing: wanted_of(&all[1..]),
            fail_store: Mutex::new(Some("FATAL_STORE_ERROR".into())),
            ..Listener::default()
        });
        let lanes = failing.lane();
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert!(outcome.error.is_some());
        let peer = push_peer(&mut state, &folder);
        close_to(peer.rest_until.unwrap(), Duration::from_secs(1));
    }

    #[test]
    fn a_store_that_times_out_rests_its_endpoint_ever_longer() {
        let all = objects("object", 1);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            stall_above: Some(0),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let folder = pushing_folder("repo-1", &all);
        let mut state = PushState {
            deadline: |_| Duration::from_millis(50),
            ..PushState::default()
        };
        for seconds in [1, 2, 4] {
            push_once(&lanes, &mut state, &folder, PushMode::Background);
            close_to(state.resting[ENDPOINT], Duration::from_secs(seconds));
            state.resting.clear();
        }
    }

    #[test]
    fn a_burst_on_a_timing_out_link_runs_to_its_budget_at_a_growing_pace() {
        let all = objects("object", 1);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            stall_above: Some(0),
            ..Listener::default()
        });
        let bulk = listener.lane_with("session", &[features::CONTENT_PUSH]);
        let (driver, _root) = burst_driver(ReadyLanes::one(false, bulk), &all);
        let burst = open_burst(&driver, &[pair(ENDPOINT, "repo-1")]);
        exchange_done(&driver);
        let mut state = PumpState::default();
        state.push.deadline = |_| Duration::from_millis(100);
        // Rests of 1, 2 and 4 s: stores at about 0, 1.1 and 3.2 s, and no more
        // before the budget ends at 5 s. A fixed rest would make it 5. A slow
        // machine may get only 2.
        let end = Instant::now() + Duration::from_secs(5);
        while Instant::now() < end {
            driver.pump_round(&mut state, driver.wake.current());
            thread::sleep(Duration::from_millis(20));
        }
        let calls = listener.calls();
        let stores = calls_named(&calls, "store");
        assert!((2..=3).contains(&stores), "{calls:?}");
        assert!(!settled_now(&driver, burst));
        let reply = driver.close_burst(
            burst,
            &[repo_folder()],
            Some("SYNC_NOW_TIMEOUT: not finished".to_owned()),
        );
        let folder = folder_of(&reply);
        let error = folder["error"].as_str().unwrap();
        assert!(error.contains("REQUEST_TIMEOUT"), "{reply}");
        assert_eq!(folder["remaining"], 1, "{reply}");
    }

    // ---- a listener that hangs ---------------------------------------------

    /// Bulk lanes to several endpoints, in this order. A pump's first round
    /// starts with the second.
    struct Endpoints(Vec<(String, Conn)>);

    impl Lanes for Endpoints {
        fn ready_peers(&self) -> Vec<ReadyPeer> {
            Vec::new()
        }
        fn bulk_endpoints(&self) -> Vec<String> {
            self.0
                .iter()
                .map(|(endpoint, _)| endpoint.clone())
                .collect()
        }
        fn bulk_lane(&self, endpoint: &str, _: &Digest) -> Option<Conn> {
            self.any_bulk_lane(endpoint)
        }
        fn any_bulk_lane(&self, endpoint: &str) -> Option<Conn> {
            self.0
                .iter()
                .find(|(name, _)| name == endpoint)
                .map(|(_, conn)| conn.clone())
        }
    }

    /// How long a reader may take with a hung listener in the way: well inside
    /// its 10 s (`Demand::FETCH_WAIT`).
    const PAST_A_HUNG_LISTENER: Duration = Duration::from_secs(6);

    /// Waits for `done`, bounded, and says how long it took.
    fn time_until(mut done: impl FnMut() -> bool) -> Duration {
        let began = Instant::now();
        while !done() {
            assert!(began.elapsed() < Duration::from_secs(20), "never happened");
            thread::sleep(Duration::from_millis(10));
        }
        began.elapsed()
    }

    /// A running driver of `repo-1` whose first round asks a hung listener first
    /// and `healthy` next. Readers on this side wait for `demand`.
    fn started_past_a_hung_listener(
        healthy: &Arc<Listener>,
        fake: Arc<FakeFolder>,
        demand: Wanted,
    ) -> (Arc<Driver>, tempfile::TempDir) {
        let hung = Arc::new(Listener {
            hang: true,
            ..Listener::default()
        });
        let lane = |listener: &Arc<Listener>, session: &str| {
            listener.lane_with(session, &[features::CONTENT_PUSH])
        };
        let root = tempfile::tempdir().unwrap();
        let driver = start(
            Arc::new(Endpoints(vec![
                ("ws://healthy".into(), lane(healthy, "healthy")),
                ("ws://hung".into(), lane(&hung, "hung")),
            ])),
            Catalog::open(root.path()).unwrap(),
            Arc::new(RepoFolders(vec![(
                "repo-1".into(),
                fake_worker(fake),
                demand,
            )])),
            Seq::new(),
            active(HOUR, HOUR),
            Cancel::new(),
        )
        .unwrap();
        (driver, root)
    }

    #[test]
    fn a_reader_on_a_healthy_peer_is_served_past_a_hung_listener() {
        let wanted = objects("wanted", 1);
        let healthy = Arc::new(Listener {
            demand: wanted_of(&wanted),
            ..Listener::default()
        });
        let fake = Arc::new(FakeFolder {
            held: wanted,
            ..FakeFolder::default()
        });
        let (driver, _root) = started_past_a_hung_listener(&healthy, fake, Vec::new());
        let took = time_until(|| !healthy.stored().is_empty());
        driver.stop(Duration::from_secs(2));
        assert!(took < PAST_A_HUNG_LISTENER, "{took:?}");
    }

    #[test]
    fn a_reader_here_is_served_by_a_healthy_peer_past_a_hung_listener() {
        let wanted = objects("wanted", 1);
        let healthy = Arc::new(Listener {
            held: wanted.clone(),
            ..Listener::default()
        });
        let fake = Arc::new(FakeFolder::default());
        let (driver, _root) =
            started_past_a_hung_listener(&healthy, fake.clone(), wanted_of(&wanted));
        let took = time_until(|| !fake.written().is_empty());
        driver.stop(Duration::from_secs(2));
        assert!(took < PAST_A_HUNG_LISTENER, "{took:?}");
    }

    // ---- a failing folder keeps no other from pulling -----------------------

    #[test]
    fn a_paused_folder_does_not_starve_another_folder_of_its_pulls() {
        let (paused, open) = (objects("paused", 1), objects("open", 1));
        let listener = Arc::new(Listener {
            held: open.clone(),
            unpublished: Some(UNPUBLISHED.into()),
            ..Listener::default()
        });
        let fake_of = |missing: &[Object]| {
            Arc::new(FakeFolder {
                missing: wanted_of(missing),
                ..FakeFolder::default()
            })
        };
        let (first, second) = (fake_of(&paused), fake_of(&open));
        let folders = Arc::new(RepoFolders(vec![
            (UNPUBLISHED.into(), fake_worker(first), Vec::new()),
            ("repo-1".into(), fake_worker(second.clone()), Vec::new()),
        ]));
        let (driver, _root) = driver(listener.lane(), folders, Seq::new());
        let mut state = PumpState::default();
        for _ in 0..6 {
            driver.pump_round(&mut state, driver.wake.current());
        }
        assert_eq!(second.written(), [open[0].digest], "{:?}", listener.calls());
        // What the other folder's success says does not make this one log again.
        assert_eq!(state.logged.len(), 1, "{:?}", state.logged);
    }

    // ---- a wake cancels what is in flight -----------------------------------

    /// Bumps `wake` in a moment, as a reader starting to wait does.
    fn bump_soon(wake: &Arc<Seq>) -> JoinHandle<()> {
        let wake = wake.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(200));
            wake.bump();
        })
    }

    #[test]
    fn a_wake_during_a_background_store_cancels_it_with_no_failure_to_count() {
        let all = objects("object", 2);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            hang_store_of: [all[0].digest].into(),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let folder = pushing_folder("repo-1", &all);
        let mut state = PushState::default();
        let wake = Seq::new();
        let seen = wake.current();
        let bumper = bump_soon(&wake);
        let began = Instant::now();
        let outcome = push::run(
            &lanes,
            &mut state,
            &[ENDPOINT.to_owned()],
            &folder,
            PushMode::Background,
            Some((wake.as_ref(), seen)),
            &Cancel::new(),
        );
        let took = began.elapsed();
        bumper.join().unwrap();
        assert!(took < Duration::from_secs(2), "{took:?}");
        assert!(outcome.interrupted);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        assert!(state.resting.is_empty());
        let peer = push_peer(&mut state, &folder);
        assert!(peer.rest_until.is_none());
        assert!(peer.counters.last_error.is_none());
        assert_eq!(peer.batch, OBJECT_BATCH);
        // Uncertain, so not replayed: only a later listing brings it back.
        assert_eq!(peer.scan, None);
    }

    #[test]
    fn a_wake_during_a_background_fetch_cancels_it_with_no_failure_to_count() {
        let here = objects("here", 1);
        let listener = Arc::new(Listener {
            held: here.clone(),
            hang_fetch: true,
            ..Listener::default()
        });
        let fake = Arc::new(FakeFolder {
            missing: wanted_of(&here),
            ..FakeFolder::default()
        });
        let wake = Seq::new();
        let folders = Arc::new(RepoFolders(vec![(
            "repo-1".into(),
            fake_worker(fake),
            Vec::new(),
        )]));
        let (driver, _root) = driver(listener.lane(), folders, wake.clone());
        let mut state = PumpState::default();
        let seen = wake.current();
        let bumper = bump_soon(&wake);
        let began = Instant::now();
        driver.pump_round(&mut state, seen);
        let took = began.elapsed();
        bumper.join().unwrap();
        assert!(took < Duration::from_secs(2), "{took:?}");
        assert!(state.resting.is_empty());
        assert!(state.push.resting.is_empty());
        assert!(state.batch.is_empty());
    }

    #[test]
    fn a_reader_is_served_at_once_while_a_background_store_hangs() {
        let (background, wanted) = (objects("background", 1), objects("wanted", 1));
        let listener = Arc::new(Listener {
            missing: wanted_of(&background),
            hang_store_of: [background[0].digest].into(),
            ..Listener::default()
        });
        let fake = FakeFolder {
            held: [background, wanted.clone()].concat(),
            ..FakeFolder::default()
        };
        let bulk = listener.lane_with("session", &[features::CONTENT_PUSH]);
        let root = tempfile::tempdir().unwrap();
        let driver = start(
            Arc::new(ReadyLanes::one(true, bulk)),
            Catalog::open(root.path()).unwrap(),
            Arc::new(RepoFolders(vec![(
                "repo-1".into(),
                fake_worker(Arc::new(fake)),
                Vec::new(),
            )])),
            Seq::new(),
            active(HOUR, HOUR),
            Cancel::new(),
        )
        .unwrap();
        time_until(|| calls_named(&listener.calls(), "store") > 0);
        *lock(&listener.late_demand) = wanted_of(&wanted);
        driver.notified(ENDPOINT, &demand_of("repo-1"));
        let took = time_until(|| listener.stored().contains(&wanted[0].digest));
        driver.stop(Duration::from_secs(2));
        assert!(took < Duration::from_secs(2), "{took:?}");
    }

    // ---- what a peer says is checked ----------------------------------------

    #[test]
    fn a_store_reply_counts_only_what_was_sent() {
        let (all, other) = (objects("object", 1), objects("other", 1));
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            claims: vec![all[0].digest, other[0].digest],
            ..Listener::default()
        });
        let lanes = listener.lane();
        let folder = pushing_folder("repo-1", &all);
        let mut state = PushState::default();
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert_eq!(outcome.pushed, 1);
        assert_eq!(push_peer(&mut state, &folder).counters.pushed, 1);
    }

    #[test]
    fn a_pulled_object_of_another_kind_than_asked_is_not_written() {
        let asked = objects("object", 1);
        let answered: Vec<Object> = asked
            .iter()
            .map(|object| Object {
                kind: Kind::Manifest,
                ..object.clone()
            })
            .collect();
        let folder = Arc::new(FakeFolder {
            missing: wanted_of(&asked),
            ..FakeFolder::default()
        });
        let link = fake_worker(folder.clone());
        let peer = Peer::new(&answered, &[], Duration::ZERO);
        let folders = Arc::new(Folders(vec![("f1".into(), link, Mutex::new(Vec::new()))]));
        let (driver, _root) = driver(OneLane(peer.lane()), folders, Seq::new());
        let mut state = PumpState::default();
        driver.pump_round(&mut state, driver.wake.current());
        assert!(folder.written().is_empty());
    }

    #[test]
    fn a_demand_push_reads_again_what_this_side_lately_lacked() {
        let all = objects("object", 1);
        let listener = Arc::new(Listener {
            demand: wanted_of(&all),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let lacking = pushing_folder("repo-1", &[]);
        let mut state = PushState::default();
        push_once(&lanes, &mut state, &lacking, PushMode::Demand);
        assert_eq!(push_peer(&mut state, &lacking).lacks.len(), 1);
        // This side gets it by some other way, well within the backoff.
        let holding = pushing_folder("repo-1", &all);
        let outcome = push_once(&lanes, &mut state, &holding, PushMode::Demand);
        assert_eq!(outcome.pushed, 1);
        assert_eq!(listener.stored(), [all[0].digest]);
    }

    #[test]
    fn a_daemon_stopping_is_not_reported_as_the_last_failure() {
        let (driver, _root, control) = dispatching(passive(SHORT));
        let peers = [ready_peer(ENDPOINT, "control", false, &control)];
        let key = pair(ENDPOINT, "repo-1");
        let burst = open_burst(&driver, std::slice::from_ref(&key));
        let (tasks, queue) = mpsc::channel();
        driver.dispatch(1, &peers, &tasks);
        let task = queue.try_recv().unwrap();
        let timed_out = peer_failure("meta.exchange", CallError::Timeout { after_ms: 7 });
        driver.finish(&task, task.cursor, Err(timed_out));
        driver.cancel.cancel();
        let reply = driver.close_burst(
            burst,
            &[repo_folder()],
            Some("ORCHESTRATOR_STOPPING: the orchestrator is shutting down".to_owned()),
        );
        let error = folder_of(&reply)["error"].as_str().unwrap();
        assert!(error.starts_with("ORCHESTRATOR_STOPPING"), "{reply}");
    }

    #[test]
    fn a_failing_store_of_a_full_page_rests_ever_longer_too() {
        // A full page has a cursor, so a clean empty page follows each failure.
        let all = objects("object", OBJECT_BATCH);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            fail_store: Mutex::new(Some("FATAL_STORE_ERROR".into())),
            ..Listener::default()
        });
        let lanes = listener.lane();
        let folder = pushing_folder("repo-1", &all);
        let mut state = PushState::default();
        for seconds in [1, 2, 4, 8] {
            let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
            assert!(outcome.error.is_some());
            let peer = push_peer(&mut state, &folder);
            close_to(peer.rest_until.unwrap(), Duration::from_secs(seconds));
            peer.rest_until = None;
            let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
            assert!(outcome.error.is_none());
            assert!(outcome.again, "the failed sweep is not the last");
        }
    }

    #[test]
    fn a_long_sweep_that_stored_nothing_is_not_started_over_by_the_next_poll() {
        // Most pages list only what neither side holds, past what an exclude
        // can name, so each is another request.
        let lacked = objects("lacked", MAX_EXCLUDE + 44);
        let listener = Arc::new(Listener {
            missing: wanted_of(&lacked),
            ..Listener::default()
        });
        let (driver, _root) = pushing_driver(&listener, true, active(HOUR, SHORT));
        let mut state = PumpState::default();
        let mut pages = 0;
        loop {
            driver.pump_round(&mut state, driver.wake.current());
            thread::sleep(Duration::from_millis(30));
            pages += 1;
            assert!(pages < 30, "{:?}", listener.calls());
            if !background_due(&driver) {
                break;
            }
        }
        // Its poll comes up in moments. The listener is left alone a good while.
        let calls = listener.calls().len();
        for _ in 0..5 {
            thread::sleep(PAST_SHORT);
            driver.pump_round(&mut state, driver.wake.current());
        }
        assert_eq!(
            listener.calls().len(),
            calls,
            "{:?}",
            &listener.calls()[calls..]
        );
        // A trigger that says something changed still starts one at once.
        driver.local_change("repo-1");
        assert!(calls_of_round(&driver, &mut state, &listener).contains(&"wanted/32".to_owned()));
    }

    #[test]
    fn a_listing_that_timed_out_gets_longer_next_time_and_a_demand_listing_does_not() {
        let listener = Arc::new(Listener {
            hang: true,
            ..Listener::default()
        });
        let lanes = listener.lane();
        let folder = pushing_folder("repo-1", &[]);
        let mut state = PushState::default();
        state
            .peers
            .entry((folder.id.clone(), ENDPOINT.to_owned()))
            .or_default()
            .listing = Duration::from_millis(50);
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Background);
        assert!(matches!(outcome.error, Some(Failure::TimedOut(_))));
        assert!(!outcome.again, "a hung listener is not asked again at once");
        assert_eq!(
            push_peer(&mut state, &folder).listing,
            Duration::from_millis(100)
        );
        // A demand listing does not scan, so it keeps its short deadline.
        state.resting.clear();
        let began = Instant::now();
        let outcome = push_once(&lanes, &mut state, &folder, PushMode::Demand);
        assert!(matches!(outcome.error, Some(Failure::TimedOut(_))));
        let took = began.elapsed();
        assert!(took >= push::WANTED_DEADLINE, "{took:?}");
        assert!(took < PEER_DEADLINE, "{took:?}");
        assert_eq!(
            push_peer(&mut state, &folder).listing,
            Duration::from_millis(100)
        );
    }

    #[test]
    fn what_no_reply_can_carry_is_reported_and_stays_in_remaining() {
        let all = objects("object", 1);
        let listener = Arc::new(Listener {
            missing: wanted_of(&all),
            ..Listener::default()
        });
        let bulk = listener.lane_with("session", &[features::CONTENT_PUSH]);
        let root = tempfile::tempdir().unwrap();
        let fake = FakeFolder {
            oversized: vec![all[0].digest],
            ..FakeFolder::default()
        };
        let driver = Driver::with_policy(
            Arc::new(ReadyLanes::one(false, bulk)),
            Catalog::open(root.path()).unwrap(),
            Arc::new(RepoFolders(vec![(
                "repo-1".to_owned(),
                fake_worker(Arc::new(fake)),
                Vec::new(),
            )])),
            Seq::new(),
            passive(HOUR),
            Cancel::new(),
        );
        let burst = open_burst(&driver, &[pair(ENDPOINT, "repo-1")]);
        exchange_done(&driver);
        let mut state = PumpState::default();
        for _ in 0..3 {
            driver.pump_round(&mut state, driver.wake.current());
        }
        assert!(settled_now(&driver, burst));
        let report = report_now(&driver, burst);
        assert_eq!(report["remaining"], 1, "{report}");
        let error = report["error"].as_str().unwrap_or_default();
        assert!(error.starts_with("SYNC_OBJECT_TOO_LARGE"), "{report}");
    }

    #[test]
    fn a_listener_known_not_to_answer_is_asked_last_for_a_reader() {
        let wanted = objects("wanted", 1);
        let healthy = Arc::new(Listener {
            held: wanted.clone(),
            ..Listener::default()
        });
        let hung = Arc::new(Listener {
            hang: true,
            ..Listener::default()
        });
        let fake = Arc::new(FakeFolder::default());
        let (driver, _root) = {
            let root = tempfile::tempdir().unwrap();
            let lanes = Endpoints(vec![
                ("ws://healthy".into(), healthy.lane_with("h", &[])),
                ("ws://hung".into(), hung.lane_with("x", &[])),
            ]);
            let driver = Driver::with_policy(
                Arc::new(lanes),
                Catalog::open(root.path()).unwrap(),
                Arc::new(RepoFolders(vec![(
                    "repo-1".into(),
                    fake_worker(fake.clone()),
                    wanted_of(&wanted),
                )])),
                Seq::new(),
                active(HOUR, HOUR),
                Cancel::new(),
            );
            (driver, root)
        };
        let mut state = PumpState::default();
        state.push.rest_endpoint("ws://hung");
        // Its first round would start with it.
        driver.pump_round(&mut state, driver.wake.current());
        assert_eq!(fake.written(), [wanted[0].digest]);
        assert!(hung.calls().is_empty(), "{:?}", hung.calls());
    }

    #[test]
    fn a_single_peer_at_rest_keeps_its_full_deadline_for_a_reader() {
        // It answers, later than a peer that another could stand in for is given.
        let wanted = objects("wanted", 1);
        let listener = Arc::new(Listener {
            held: wanted.clone(),
            during_fetch: Some(Box::new(|| {
                thread::sleep(DEMAND_TRY + Duration::from_millis(300))
            })),
            ..Listener::default()
        });
        let fake = Arc::new(FakeFolder::default());
        let folders = Arc::new(RepoFolders(vec![(
            "repo-1".into(),
            fake_worker(fake.clone()),
            wanted_of(&wanted),
        )]));
        let (driver, _root) = driver(listener.lane(), folders, Seq::new());
        let mut state = PumpState::default();
        state.push.rest_endpoint(ENDPOINT);
        driver.pump_round(&mut state, driver.wake.current());
        assert_eq!(fake.written(), [wanted[0].digest]);
        // So the next read finds it again, and the rest it was at is over.
        assert!(!state.resting.contains_key(ENDPOINT));
        assert!(!state.push.endpoint_rests(ENDPOINT, Instant::now()));
    }

    /// One folder whose readers wait for `reads` and hint `hints`.
    struct HintingFolder {
        link: Arc<WorkerLink>,
        reads: Mutex<Wanted>,
        hints: Mutex<Wanted>,
    }

    impl SyncFolders for HintingFolder {
        fn syncing(&self) -> Vec<SyncFolder> {
            vec![SyncFolder {
                id: "f1".into(),
                repo: "repo-1".into(),
                link: self.link.clone(),
                demand: lock(&self.reads).clone(),
                hints: lock(&self.hints).clone(),
            }]
        }
        fn knows(&self, repo: &str) -> bool {
            repo == "repo-1"
        }
    }

    #[test]
    fn read_ahead_is_asked_a_batch_a_round_and_a_waiting_read_goes_first() {
        // More than a batch of hints, and a read that is not among them.
        let all = objects("hint", OBJECT_BATCH + 8);
        let read = objects("read", 1);
        let listener = Arc::new(Listener {
            held: [all.clone(), read.clone()].concat(),
            ..Listener::default()
        });
        let fake = Arc::new(FakeFolder::default());
        let folders = Arc::new(HintingFolder {
            link: fake_worker(fake.clone()),
            reads: Mutex::new(Vec::new()),
            hints: Mutex::new(wanted_of(&all)),
        });
        let (driver, _root) = driver(listener.lane(), folders.clone(), Seq::new());
        let digests = |objects: &[Object]| objects.iter().map(|o| o.digest).collect::<Vec<_>>();
        let mut state = PumpState::default();

        let stored = driver.pump_round(&mut state, driver.wake.current());
        assert_eq!(stored, OBJECT_BATCH, "a long read-ahead holds nothing up");
        assert_eq!(
            fake.written(),
            digests(&all[..OBJECT_BATCH]),
            "nearest first"
        );

        // A reader starts waiting. It is asked for by itself and before the rest,
        // so whatever is slow about the rest does not count against it.
        *lock(&folders.reads) = wanted_of(&read);
        let fetches = calls_named(&listener.calls(), "fetch");
        assert_eq!(driver.pump_round(&mut state, driver.wake.current()), 9);
        assert_eq!(calls_named(&listener.calls(), "fetch"), fetches + 2);
        assert_eq!(fake.written()[OBJECT_BATCH], read[0].digest);
        assert_eq!(
            fake.written()[OBJECT_BATCH + 1..],
            digests(&all[OBJECT_BATCH..])
        );
    }

    #[test]
    fn read_ahead_a_handle_took_back_is_asked_no_further() {
        let all = objects("hint", 2 * OBJECT_BATCH);
        let listener = Arc::new(Listener {
            held: all.clone(),
            ..Listener::default()
        });
        let fake = Arc::new(FakeFolder::default());
        let folders = Arc::new(HintingFolder {
            link: fake_worker(fake.clone()),
            reads: Mutex::new(Vec::new()),
            hints: Mutex::new(wanted_of(&all)),
        });
        let (driver, _root) = driver(listener.lane(), folders.clone(), Seq::new());
        let mut state = PumpState::default();
        driver.pump_round(&mut state, driver.wake.current());
        assert_eq!(fake.written().len(), OBJECT_BATCH);
        lock(&folders.hints).clear();
        assert_eq!(driver.pump_round(&mut state, driver.wake.current()), 0);
        assert_eq!(fake.written().len(), OBJECT_BATCH);
    }

    #[test]
    fn a_background_pull_leaves_a_listener_alone_while_it_rests() {
        let here = objects("here", 1);
        let listener = Arc::new(Listener {
            held: here.clone(),
            ..Listener::default()
        });
        let fake = Arc::new(FakeFolder {
            missing: wanted_of(&here),
            ..FakeFolder::default()
        });
        let folders = Arc::new(RepoFolders(vec![(
            "repo-1".into(),
            fake_worker(fake.clone()),
            Vec::new(),
        )]));
        let (driver, _root) = driver(listener.lane(), folders, Seq::new());
        let mut state = PumpState::default();
        state.push.rest_endpoint(ENDPOINT);
        driver.pump_round(&mut state, driver.wake.current());
        assert_eq!(calls_named(&listener.calls(), "fetch"), 0);
        // The rest over, nothing was passed over.
        state.push.resting.clear();
        driver.pump_round(&mut state, driver.wake.current());
        assert_eq!(fake.written(), [here[0].digest], "{:?}", listener.calls());
    }

    #[test]
    fn an_answered_fetch_starts_the_rest_of_its_endpoint_over() {
        let here = objects("here", 1);
        let listener = Arc::new(Listener {
            held: here.clone(),
            ..Listener::default()
        });
        let fake = Arc::new(FakeFolder {
            missing: wanted_of(&here),
            ..FakeFolder::default()
        });
        let folders = Arc::new(RepoFolders(vec![(
            "repo-1".into(),
            fake_worker(fake),
            Vec::new(),
        )]));
        let (driver, _root) = driver(OneLane(listener.lane_with("s", &[])), folders, Seq::new());
        let mut state = PumpState::default();
        state.push.rest_endpoint(ENDPOINT);
        state.push.rest_endpoint(ENDPOINT);
        state.push.rest_endpoint(ENDPOINT);
        state.push.resting.clear();
        // This listener takes no pushes: only its answers to pulls can say so.
        driver.pump_round(&mut state, driver.wake.current());
        state.push.rest_endpoint(ENDPOINT);
        close_to(state.push.resting[ENDPOINT], Duration::from_secs(1));
    }

    // ---- feed cursors that survive ---------------------------------------

    /// The saved cursors of a test, in memory.
    #[derive(Default)]
    struct FakeCursors {
        rows: Mutex<BTreeMap<(String, String), SavedCursor>>,
        loads: Mutex<usize>,
        saves: Mutex<usize>,
        /// Runs at every save, before the row changes.
        on_save: Mutex<Option<Hook>>,
    }

    impl FakeCursors {
        /// The row of the one (installation, repo) these tests use.
        fn row(&self) -> Option<SavedCursor> {
            lock(&self.rows)
                .get(&("peer-b".to_owned(), "repo-1".to_owned()))
                .cloned()
        }

        fn set_row(&self, cursor: SavedCursor) {
            let key = ("peer-b".to_owned(), "repo-1".to_owned());
            lock(&self.rows).insert(key, cursor);
        }

        fn loads(&self) -> usize {
            *lock(&self.loads)
        }

        fn saves(&self) -> usize {
            *lock(&self.saves)
        }
    }

    impl CursorStore for FakeCursors {
        fn load(&self, installation: &str, repo: &str) -> Option<SavedCursor> {
            *lock(&self.loads) += 1;
            let key = (installation.to_owned(), repo.to_owned());
            lock(&self.rows).get(&key).cloned()
        }
        fn save(&self, installation: &str, repo: &str, cursor: &SavedCursor) {
            *lock(&self.saves) += 1;
            if let Some(hook) = &*lock(&self.on_save) {
                hook();
            }
            let key = (installation.to_owned(), repo.to_owned());
            lock(&self.rows).insert(key, cursor.clone());
        }
        fn clear(&self) {
            lock(&self.rows).clear();
        }
    }

    fn policy_with(cursors: Arc<dyn CursorStore>, policy: SyncPolicy) -> SyncPolicy {
        SyncPolicy { cursors, ..policy }
    }

    #[test]
    fn a_passive_driver_resumes_a_saved_cursor_on_a_new_session() {
        let cursors = Arc::new(FakeCursors::default());
        let saved = |sent, received| SavedCursor {
            sent,
            received,
            local_journal: "here".into(),
            peer_journal: "there".into(),
        };
        cursors.set_row(saved(3, 4));
        let (driver, _root, control) = dispatching(policy_with(cursors.clone(), passive(SHORT)));
        let key = pair(ENDPOINT, "repo-1");
        open_burst(&driver, std::slice::from_ref(&key));
        let (tasks, queue) = mpsc::channel();
        let first = [ready_peer(ENDPOINT, "control", false, &control)];
        driver.dispatch(1, &first, &tasks);
        let task = queue.try_recv().unwrap();
        assert_eq!(
            task.cursor,
            FeedCursor {
                sent: 3,
                received: 4
            }
        );
        assert_eq!(task.journals.local, "here");
        assert_eq!(task.journals.peer, "there");
        let ahead = FeedCursor {
            sent: 30,
            received: 40,
        };
        driver.finish(&task, ahead, Ok(()));
        // A next exchange of this session goes on from memory.
        lock(&driver.due).exchange.insert(key.clone());
        driver.dispatch(1, &first, &tasks);
        let task = queue.try_recv().unwrap();
        assert_eq!(task.cursor, ahead);
        assert_eq!(cursors.loads(), 1);
        driver.finish(&task, ahead, Ok(()));
        // A new session, or a new worker, goes back to what was saved: memory can
        // hold what no save made it to the row.
        cursors.set_row(saved(5, 6));
        let second = [ready_peer(ENDPOINT, "control-again", false, &control)];
        driver.dispatch(1, &second, &tasks);
        let task = queue.try_recv().expect("the new session was not due");
        assert_eq!(
            task.cursor,
            FeedCursor {
                sent: 5,
                received: 6
            }
        );
        driver.finish(&task, task.cursor, Ok(()));
        // No row: from the start, as on every session today.
        lock(&cursors.rows).clear();
        let third = [ready_peer(ENDPOINT, "control-third", false, &control)];
        driver.dispatch(1, &third, &tasks);
        let task = queue.try_recv().expect("the new session was not due");
        assert_eq!(task.cursor, FeedCursor::default());
        assert_eq!(task.journals, Journals::default());
    }

    /// What a listener saw of each `meta.exchange`.
    #[derive(Debug, PartialEq, Eq)]
    struct Asked {
        after: u64,
        events: usize,
        durable: bool,
    }

    /// Requests after which an exchange is taken to be looping.
    const TOO_MANY_PAGES: usize = 64;

    /// A real listener: the sync service of a real folder, recording what it is
    /// asked. It holds the service weakly, so the folder closes with its test.
    struct RealListener {
        service: Weak<Service>,
        asked: Mutex<Vec<Asked>>,
        /// It answers as a listener from before journals did.
        older: bool,
        /// `meta.exchange` fails after this many requests.
        fail_after: Option<usize>,
        /// Cancelled at [`TOO_MANY_PAGES`], so that a loop fails the test.
        loop_guard: Cancel,
    }

    impl Handler for RealListener {
        fn handle(&self, req: Incoming) {
            let asked = {
                let mut asked = lock(&self.asked);
                let request: FeedRequest = serde_json::from_slice(&req.body).unwrap();
                asked.push(Asked {
                    after: request.after,
                    events: request.events.len(),
                    durable: req.header["durable"] == true,
                });
                asked.len()
            };
            if asked >= TOO_MANY_PAGES {
                self.loop_guard.cancel();
            }
            if self.fail_after.is_some_and(|most| asked > most) {
                req.responder
                    .error(ErrorWire::coded("BROKEN", "on purpose"));
                return;
            }
            let service = self.service.upgrade().expect("the folder is gone");
            let (header, body) = service
                .handle_reply(&req.op, &req.header, &req.body)
                .unwrap();
            let body = if self.older {
                let mut reply: Value = serde_json::from_slice(&body).unwrap();
                let fields = reply.as_object_mut().unwrap();
                fields.remove("journal");
                fields.remove("top");
                serde_json::to_vec(&reply).unwrap()
            } else {
                body
            };
            req.responder.reply(header, body);
        }
    }

    fn real_service(engine: &Arc<Engine>) -> Arc<Service> {
        Arc::new(Service {
            engine: engine.clone(),
            demand: crate::orchestrator::demand::Demand::new(
                Seq::new(),
                engine.working.content().packs.clone(),
            ),
        })
    }

    /// A driver whose one folder (`here`) has a real worker, and a real listener
    /// (`there`) of the same repo. Nothing runs on its own: a test exchanges.
    struct RealSides {
        driver: Arc<Driver>,
        cursors: Arc<FakeCursors>,
        here: Arc<Engine>,
        there: Arc<Engine>,
        listener: Arc<RealListener>,
        control: Conn,
        server: Conn,
        guard: Cancel,
        _services: [Arc<Service>; 2],
        _homes: [tempfile::TempDir; 3],
    }

    impl RealSides {
        fn passive(cursors: &Arc<FakeCursors>) -> Self {
            let policy = policy_with(cursors.clone(), passive(SHORT));
            Self::new(policy, cursors.clone(), false, None)
        }

        fn new(
            policy: SyncPolicy,
            cursors: Arc<FakeCursors>,
            older: bool,
            fail_after: Option<usize>,
        ) -> Self {
            let homes = [
                tempfile::tempdir().unwrap(),
                tempfile::tempdir().unwrap(),
                tempfile::tempdir().unwrap(),
            ];
            let here = Engine::open(homes[0].path(), Some("repo-1")).unwrap();
            let there = Engine::open(homes[1].path(), Some("repo-1")).unwrap();
            let (worker, peer) = (real_service(&here), real_service(&there));
            let serving = Arc::downgrade(&worker);
            let link = worker_serving(Arc::new(FnHandler(move |req: Incoming| {
                serving.upgrade().expect("the folder is gone").handle(req);
            })));
            let guard = Cancel::new();
            let listener = Arc::new(RealListener {
                service: Arc::downgrade(&peer),
                asked: Mutex::new(Vec::new()),
                older,
                fail_after,
                loop_guard: guard.clone(),
            });
            let (control, server) = served(listener.clone());
            let lanes = ReadyLanes {
                peers: Vec::new(),
                control: control.clone(),
                bulk: None,
            };
            let driver = Driver::with_policy(
                Arc::new(lanes),
                Catalog::open(homes[2].path()).unwrap(),
                Arc::new(RepoFolders(vec![("repo-1".into(), link, Vec::new())])),
                Seq::new(),
                policy,
                guard.clone(),
            );
            Self {
                driver,
                cursors,
                here,
                there,
                listener,
                control,
                server,
                guard,
                _services: [worker, peer],
                _homes: homes,
            }
        }

        /// One exchange on this control `session`, start to finish, on this thread.
        fn exchange_on(&self, session: &str) {
            let key = pair(ENDPOINT, "repo-1");
            open_burst(&self.driver, std::slice::from_ref(&key));
            let peers = [ready_peer(ENDPOINT, session, false, &self.control)];
            let (tasks, queue) = mpsc::channel();
            self.driver.dispatch(1, &peers, &tasks);
            let task = queue.try_recv().expect("the pair was not due");
            self.driver.run_exchange(task);
            assert!(!self.guard.is_cancelled(), "the exchange did not end");
        }

        fn cursor(&self) -> FeedCursor {
            lock(&self.driver.jobs)[&pair(ENDPOINT, "repo-1")]
                .job
                .cursor
        }

        fn error(&self) -> Option<String> {
            let jobs = lock(&self.driver.jobs);
            jobs[&pair(ENDPOINT, "repo-1")].job.error.clone()
        }

        /// What the listener was asked since this was last called.
        fn asked(&self) -> Vec<Asked> {
            std::mem::take(&mut *lock(&self.listener.asked))
        }

        /// A saved cursor at the top of both sides, in both their journals. A
        /// test that means something else says what.
        fn caught_up(&self) -> SavedCursor {
            let (here, there) = (lock(&self.here.store), lock(&self.there.store));
            SavedCursor {
                sent: here.feed_position().unwrap(),
                received: there.feed_position().unwrap(),
                local_journal: here.device.clone(),
                peer_journal: there.device.clone(),
            }
        }

        fn add_here(&self, name: &str) {
            let mut store = lock(&self.here.store);
            store.create(name, EntryKind::File).unwrap();
        }

        fn add_there(&self, name: &str) {
            let mut store = lock(&self.there.store);
            store.create(name, EntryKind::File).unwrap();
        }

        /// A file on each side that the other does not have.
        fn one_file_each(&self) {
            self.add_here("from-here");
            self.add_there("from-there");
        }

        fn each_has_the_others_file(&self) -> bool {
            lock(&self.here.store).lookup("from-there").is_ok()
                && lock(&self.there.store).lookup("from-here").is_ok()
        }

        /// After [`Self::one_file_each`] and a row that claims neither file is
        /// news: only an exchange from zero moves them, and the row is rewritten.
        fn assert_started_over(&self) {
            assert_eq!(self.error(), None);
            assert!(self.each_has_the_others_file());
            assert_eq!(self.asked().last().unwrap().after, 0);
            let (row, truth) = (self.cursors.row().unwrap(), self.caught_up());
            assert_eq!(row.received, truth.received);
            assert_eq!(row.local_journal, truth.local_journal);
            assert_eq!(row.peer_journal, truth.peer_journal);
        }
    }

    impl Drop for RealSides {
        fn drop(&mut self) {
            self.control.close();
            self.server.close();
            let _ = self.here.shutdown();
            let _ = self.there.shutdown();
        }
    }

    #[test]
    fn a_resumed_exchange_asks_only_for_what_is_new() {
        let cursors = Arc::new(FakeCursors::default());
        let sides = RealSides::passive(&cursors);
        sides.one_file_each();
        // The second sends back what the first brought: all there is to send.
        sides.exchange_on("session-1");
        sides.exchange_on("session-2");
        assert_eq!(sides.error(), None);
        let saved = cursors.row().expect("nothing was saved");
        assert_eq!(saved, sides.caught_up());
        let asked = sides.asked();
        assert!(asked.iter().all(|asked| asked.durable), "{asked:?}");
        // The lanes came back: not one event goes again, either way, and an
        // unchanged folder does not even write its row.
        let saves = cursors.saves();
        sides.exchange_on("session-3");
        assert_eq!(
            sides.asked(),
            [Asked {
                after: saved.received,
                events: 0,
                durable: true
            }]
        );
        assert_eq!(cursors.saves(), saves);
        // Something new goes, and only that.
        sides.add_here("new");
        sides.exchange_on("session-4");
        assert_eq!(
            sides.asked(),
            [Asked {
                after: saved.received,
                events: 1,
                durable: true
            }]
        );
        assert!(lock(&sides.there.store).lookup("new").is_ok());
    }

    #[test]
    fn a_resumed_exchange_starts_over_when_the_peer_journal_changed() {
        let cursors = Arc::new(FakeCursors::default());
        let sides = RealSides::passive(&cursors);
        sides.one_file_each();
        cursors.set_row(SavedCursor {
            peer_journal: "a-journal-since-replaced".into(),
            ..sides.caught_up()
        });
        sides.exchange_on("session-1");
        sides.assert_started_over();
    }

    #[test]
    fn a_resumed_exchange_starts_over_when_the_peer_top_is_below_the_cursor() {
        let cursors = Arc::new(FakeCursors::default());
        let sides = RealSides::passive(&cursors);
        sides.one_file_each();
        let caught_up = sides.caught_up();
        cursors.set_row(SavedCursor {
            received: caught_up.received + 1,
            ..caught_up
        });
        sides.exchange_on("session-1");
        sides.assert_started_over();
    }

    #[test]
    fn a_resumed_exchange_starts_over_when_the_local_journal_changed() {
        let cursors = Arc::new(FakeCursors::default());
        let sides = RealSides::passive(&cursors);
        sides.one_file_each();
        cursors.set_row(SavedCursor {
            local_journal: "a-journal-since-replaced".into(),
            ..sides.caught_up()
        });
        sides.exchange_on("session-1");
        sides.assert_started_over();
    }

    #[test]
    fn a_resumed_exchange_starts_over_when_the_local_top_is_below_the_cursor() {
        let cursors = Arc::new(FakeCursors::default());
        let sides = RealSides::passive(&cursors);
        sides.one_file_each();
        let caught_up = sides.caught_up();
        cursors.set_row(SavedCursor {
            sent: caught_up.sent + 1,
            ..caught_up
        });
        sides.exchange_on("session-1");
        sides.assert_started_over();
    }

    #[test]
    fn a_listener_that_lost_events_cannot_hide_it_behind_this_exchange() {
        // Its top is one short of the cursor, and what this side has to send would
        // lift it past: the usual case, a phone with something new.
        let cursors = Arc::new(FakeCursors::default());
        let sides = RealSides::passive(&cursors);
        sides.one_file_each();
        let caught_up = sides.caught_up();
        sides.add_here("new");
        cursors.set_row(SavedCursor {
            received: caught_up.received + 1,
            ..caught_up
        });
        sides.exchange_on("session-1");
        sides.assert_started_over();
    }

    #[test]
    fn an_active_driver_never_saves_a_cursor() {
        let cursors = Arc::new(FakeCursors::default());
        let policy = policy_with(cursors.clone(), active(HOUR, HOUR));
        let sides = RealSides::new(policy, cursors.clone(), false, None);
        let row = SavedCursor {
            sent: 7,
            received: 7,
            local_journal: "here".into(),
            peer_journal: "there".into(),
        };
        cursors.set_row(row.clone());
        sides.one_file_each();
        sides.exchange_on("session-1");
        sides.exchange_on("session-2");
        assert_eq!(sides.error(), None);
        assert_eq!((cursors.loads(), cursors.saves()), (0, 0));
        assert_eq!(cursors.row(), Some(row));
        // Every session starts from zero, and it asks nothing durable of anybody.
        let asked = sides.asked();
        assert_eq!(asked.last().unwrap().after, 0);
        assert!(asked.iter().all(|asked| !asked.durable), "{asked:?}");
        assert!(!lock(&sides.there.store).metadata_is_durable());
        assert!(!lock(&sides.here.store).metadata_is_durable());
    }

    #[test]
    fn a_reply_with_no_journal_saves_nothing() {
        let cursors = Arc::new(FakeCursors::default());
        let policy = policy_with(cursors.clone(), passive(SHORT));
        let sides = RealSides::new(policy, cursors.clone(), true, None);
        // More than a page: an older listener's reply says nothing of its top, so
        // its second page must not look like a listener that lost events.
        for n in 0..=FEED_PAGE {
            sides.add_there(&format!("file-{n}"));
        }
        sides.exchange_on("session-1");
        assert_eq!(sides.error(), None);
        assert_eq!(cursors.saves(), 0);
        assert_eq!(cursors.row(), None);
        assert_eq!(sides.cursor().received, FEED_PAGE as u64 + 1);
        sides.asked();
        // Within the session it goes on from there, as it does today.
        sides.exchange_on("session-1");
        assert_eq!(sides.asked()[0].after, FEED_PAGE as u64 + 1);
        // A new session starts over: nothing resumes.
        sides.exchange_on("session-2");
        assert_eq!(sides.asked()[0].after, 0);
        assert_eq!(cursors.saves(), 0);
    }

    #[test]
    fn a_row_saved_with_a_listener_now_of_an_older_version_does_not_resume() {
        let cursors = Arc::new(FakeCursors::default());
        let policy = policy_with(cursors.clone(), passive(SHORT));
        let sides = RealSides::new(policy, cursors.clone(), true, None);
        sides.one_file_each();
        let row = sides.caught_up();
        cursors.set_row(row.clone());
        sides.exchange_on("session-1");
        assert_eq!(sides.error(), None);
        assert!(sides.each_has_the_others_file());
        assert_eq!(cursors.saves(), 0);
        assert_eq!(cursors.row(), Some(row));
    }

    #[test]
    fn a_saved_cursor_passes_nothing_that_is_not_durable_on_both_sides() {
        let check = |set_up: &dyn Fn(&RealSides)| {
            let cursors = Arc::new(FakeCursors::default());
            let sides = RealSides::passive(&cursors);
            set_up(&sides);
            let (here, there) = (sides.here.clone(), sides.there.clone());
            *lock(&cursors.on_save) = Some(Box::new(move || {
                assert!(lock(&here.store).metadata_is_durable(), "here");
                assert!(lock(&there.store).metadata_is_durable(), "there");
            }));
            sides.exchange_on("session-1");
            *lock(&cursors.on_save) = None;
            assert_eq!(sides.error(), None);
            assert!(cursors.saves() > 0);
        };
        // Only this side has events: the reply leaves out their echoes and carries
        // nothing to apply, while `sent` and `received` both pass them.
        check(&|sides| sides.add_here("from-here"));
        // Only the listener has them: the request carries nothing.
        check(&|sides| sides.add_there("from-there"));
        check(&RealSides::one_file_each);
    }

    #[test]
    fn a_long_first_sync_keeps_the_pages_it_finished() {
        let cursors = Arc::new(FakeCursors::default());
        let policy = policy_with(cursors.clone(), passive(SHORT));
        let sides = RealSides::new(policy, cursors.clone(), false, Some(1));
        for n in 0..=FEED_PAGE {
            sides.add_there(&format!("file-{n}"));
        }
        sides.exchange_on("session-1");
        assert!(sides.error().is_some());
        let row = cursors.row().expect("the finished page was not saved");
        assert_eq!((row.sent, row.received), (0, FEED_PAGE as u64));
        assert_eq!(sides.asked().len(), 2);
        // It resumes there.
        sides.exchange_on("session-2");
        assert_eq!(sides.asked()[0].after, FEED_PAGE as u64);
    }

    #[test]
    fn a_cursor_that_cannot_be_saved_costs_only_a_replay() {
        let root = tempfile::tempdir().unwrap();
        let catalog = Catalog::open(root.path()).unwrap();
        rusqlite::Connection::open(root.path().join("folders.sqlite"))
            .unwrap()
            .execute_batch("DROP TABLE peer_cursors;")
            .unwrap();
        let policy = policy_with(catalog, passive(SHORT));
        let sides = RealSides::new(policy, Arc::default(), false, None);
        sides.one_file_each();
        sides.exchange_on("session-1");
        assert_eq!(sides.error(), None);
        assert!(sides.each_has_the_others_file());
        assert_ne!(sides.cursor(), FeedCursor::default());
        // And one that cannot be read is no cursor.
        sides.exchange_on("session-2");
        assert_eq!(sides.error(), None);
        assert_eq!(sides.asked().last().unwrap().after, 0);
    }

    #[test]
    fn reset_cursors_forgets_the_saved_rows_and_every_slot() {
        let cursors = Arc::new(FakeCursors::default());
        let sides = RealSides::passive(&cursors);
        sides.one_file_each();
        sides.exchange_on("session-1");
        assert!(cursors.row().is_some());
        assert_ne!(sides.cursor(), FeedCursor::default());
        sides.asked();
        sides.driver.reset_cursors();
        assert_eq!(cursors.row(), None);
        assert_eq!(sides.cursor(), FeedCursor::default());
        // The same session: the next exchange starts over, and saves again.
        sides.exchange_on("session-1");
        assert_eq!(sides.asked()[0].after, 0);
        assert_eq!(cursors.row(), Some(sides.caught_up()));
    }

    #[test]
    fn an_exchange_under_way_does_not_undo_a_reset() {
        let cursors = Arc::new(FakeCursors::default());
        let sides = RealSides::passive(&cursors);
        sides.one_file_each();
        let key = pair(ENDPOINT, "repo-1");
        open_burst(&sides.driver, std::slice::from_ref(&key));
        let peers = [ready_peer(ENDPOINT, "session-1", false, &sides.control)];
        let (tasks, queue) = mpsc::channel();
        sides.driver.dispatch(1, &peers, &tasks);
        let task = queue.try_recv().unwrap();
        // Reset with it on its way: what it learns is dropped, row and memory.
        sides.driver.reset_cursors();
        sides.driver.run_exchange(task);
        assert_eq!(cursors.row(), None);
        assert_eq!(sides.cursor(), FeedCursor::default());
        // The burst is not stranded: the pair is due again, from the start.
        sides.driver.dispatch(1, &peers, &tasks);
        let task = queue.try_recv().expect("the pair was not due again");
        assert_eq!(task.cursor, FeedCursor::default());
        sides.driver.run_exchange(task);
        assert_eq!(cursors.row(), Some(sides.caught_up()));
    }
}
