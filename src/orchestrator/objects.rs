//! Owned by WP2.7 (interim, Wave 2) and WP3.3 (objects v3, Wave 3).
//!
//! One bulk lane of a peer (pool lanes 1..N-1): objects v3 (REVAMP_PLAN
//! §6.4), the dialer side. The acceptor side is
//! [`super::inbound_objects`]; both share the data plane in this file
//! ([`Sink`], the worker reads of [`worker_client`], the allotment rule).
//!
//! # Scheduling
//!
//! A lane runs two schedulers of the shared engine ([`super::scheduler`]),
//! one per direction, each with its own per-(peer, repo, op) keys, slots
//! (`REPOS_PER_LANE` = 2 repositories at once), backoff by class, and claims:
//!
//! * **download** (`objects.pull`, scope `objects.pull#<lane>`): D pulls from A;
//! * **upload** (`objects.push`, scope `objects.push#<lane>`): D pushes to A.
//!
//! So a stalled push never holds a pull of the same lane, and a failing
//! direction backs off alone. At most `REPOS_PER_LANE` (2) repositories have
//! rounds on the lane at once, both directions of one counting once
//! ([`LaneOptions::repos`]; waiting repositories enter in order). Both
//! directions run on the lane's one bulk session; the peer runs at most
//! `PEER_CALLS` (2, its per-session exchange slots, `limits::BULK_HANDLERS`)
//! of our `objects.*` calls at once, so the lane admits at most that many
//! ([`LaneOptions::peer_calls`]): pulls, pushes and reports wait their turn
//! in order, and pushes (with their `objects.wants`) hold at most
//! `peer_calls - 1` slots at once, so a push the peer stalls never keeps a
//! pull or a report waiting (CCR-WP3.3-1; WP3.INT re-review). A key's round
//! ends after [`ROUND_CAP`] (10 s) so other repositories get their turn; one
//! that moved data runs again after
//! [`CONTENT_YIELD`]. A round that finds nothing to pull starts no writer
//! threads.
//!
//! Triggers: the local feed tip, the remote tip and content counters, and
//! the local `generations.sync` (it moves when the metadata exchange stages
//! events: their objects are wanted **before activation**, because the
//! worker wants objects referenced by validly staged events too). The upload
//! direction ignores its own echo (`absorb`).
//!
//! # Download round (one key)
//!
//! 1. Worker `objects.want{route, lane, max_items 256, max_bytes 16 MiB,
//!    session}`: wanted objects owned by the lane, with their durable spool
//!    offsets. The lease session is one of two ids per lane and repository
//!    ([`pull_lease`]), both stable across reconnects. A want issued while a
//!    pull is in flight uses the id the previous want did not, so it never
//!    returns that pull's objects again (the worker hides objects leased to
//!    another session). A want with no pull in flight asks **both** ids
//!    (the second only when the first finds nothing to move): an object a
//!    round or a run left unfinished (the round cap, a connection loss) may
//!    be leased to either, so the next round, or the first round after a
//!    reconnect, finds it at once, with its durable offset, and resumes there
//!    (never a blind replay, never an idle interval). An id known to hold
//!    nothing (its last want returned nothing, and no object of it is
//!    unfinished) is not asked twice. Objects owned by another lane and
//!    objects already begun in this round are left out; an object in its
//!    per-object backoff for this source is *parked* (see "Lease handoff").
//!    Before the want, the round takes what it continues or takes over from
//!    the handoff table.
//! 2. Peer `objects.pull{repo, sender, receiver, lane, items, budget}`
//!    (`sender` is our replica, `receiver` the peer's, as in
//!    `meta.exchange`): the lane's **window** (16 MiB, shared by its keys)
//!    bounds what is in flight: a pull reserves its budget (at most its
//!    key's fair share of the window, `window / REPOS_PER_LANE`, so a pull
//!    whose source stalls never takes the window from another repository;
//!    WP3.INT re-review), and the budget comes back as its bytes are written
//!    durably (or dropped) and, at its end, whatever it did not deliver.
//!    Within its share a key pipelines freely; a next pull that would take
//!    it past its share is issued once the one in flight received half its
//!    budget ("re-pull at half"), and at most 2 are in
//!    flight: when the link is the bottleneck the next pull starts while one
//!    still streams. A large object is split across consecutive pulls; their
//!    chunks may interleave on the wire, and the sink orders them.
//! 3. Each CHUNK `{hash, offset, total, last}` goes to the [`Sink`]: at most
//!    `INFLIGHT_WRITES` (4) worker `objects.write` calls in flight, one per
//!    object at a time, each at the object's durable offset, ≤ 1 MiB, tiny
//!    objects batched into one call. A write's `durable_offset` is only
//!    trusted once it is returned (invariant 2).
//! 4. Per-object results: a worker hash mismatch (`CorruptAtSource`) is
//!    reported to the peer (`objects.report{CORRUPT_AT_SOURCE}`), and that
//!    object backs off for that source (30 s → 1 h) and is parked for
//!    another source; one bad object never stops the others, of this source
//!    or of another. `ObjectError` per failed object; an object installed
//!    here resolves the errors every source had for it.
//!
//! When the round ends, what arrived is written (also when the session
//! ended: it is valid and contiguous, so the next session resumes after it),
//! unless the lane is told to stop.
//!
//! # Upload round (one key)
//!
//! Peer `objects.wants` (the peer's worker leases what it wants to this lane)
//! → our worker `objects.read` (≤ 4 in flight, authorization is the worker's)
//! → peer `objects.push` (CHUNK stream, ≤ 16 MiB per push) → per-object
//! results. A `CorruptAtSource` result makes our worker re-verify its copy
//! (`objects.report`).
//!
//! # Lease handoff (multi-source liveness)
//!
//! Every want leases what it returns to its session, and a session's next
//! want renews those leases: a source that cannot serve an object (it lacks
//! it: a relay with the metadata only; it failed it) would keep it leased,
//! and so hidden from every other source of the daemon, for as long as its
//! lane runs. All the sessions holding leases at a worker are this daemon's
//! (our pull lanes, and the acceptor's push leases of the dialers it
//! serves), so the daemon hands such objects on itself, within the frozen
//! contract (a lease is a session id; a write with that id continues it):
//!
//! * an object a source cannot serve is **parked** in the handoff table
//!   ([`handoff_park`], [`handoff_wanted`]): its lease, the worker's durable
//!   offset, no claim. The source's own wants keep returning it (and
//!   renewing its lease) but leave it alone while the source is in its
//!   backoff for it;
//! * any other transfer of the daemon (a pull lane to another peer, or the
//!   acceptor offering it to another dialer) **takes it over**
//!   ([`handoff_take`]) if its lane owns it and it is not in its own
//!   backoff, and moves it **under the parked lease**: the table records
//!   that transfer's **claim**, and the source's wants leave the object to
//!   it while the claim stands;
//! * a claim stands for [`CLAIM_TTL`] (30 s) and **only its holder renews
//!   it** ([`handoff_renew`] while it works, [`handoff_take`] of its own
//!   claims, [`handoff_progress`] after a round), never the source's wants.
//!   A holder continues the object in its next rounds, releases it once
//!   installed or another session's ([`handoff_release`]), and **gives the
//!   claim back** ([`handoff_unclaim`]: parked again, at the durable offset
//!   it reached) when it cannot serve it either, when its round or push
//!   fails or is cancelled, and, for all its claims at once
//!   ([`handoff_unclaim_all`]), when its lane run ends or its request fails.
//!   A claim whose holder vanished without that lapses. Either way the
//!   object is parked again: another transfer takes it over, or its source
//!   moves it itself once it can (its backoff over). So a transfer that
//!   took an object over and went away never pins it.
//!
//! A pull lane run claims under its own name ([`pull_holder`] and a run
//! number), the acceptor under the dialer lane's push lease and its bulk
//! session, so one run or session gives back only its own claims. Entries
//! lapse with their lease (60 s without renewal). The worker remains the
//! arbiter: a write under a lease another session now holds is
//! `InFlightElsewhere`, and every append starts at the durable offset. The
//! acceptor learns that a dialer did not serve an object from its own offer
//! ([`super::inbound_objects`]). CCR-WP3.3-4 asks for the worker-side form
//! (an exclusion list on `objects.want` that also drops this session's
//! leases on what it excludes), which would make the table unnecessary.
//!
//! # Wire rules (both sides)
//!
//! * Requests are the JSON header (`ObjectsPull`, `ObjectsWantsRequest`,
//!   `ObjectsPush`, `ObjectsReport`); replies are a JSON body, or the header
//!   when there is no body ([`super::meta_exchange`]'s `body_of`).
//! * **Allotment** ([`allot`]): the items of a pull or push cover, in order,
//!   `[offset, offset + min(size − offset, budget left))`; an item's share is
//!   fixed by the list whether or not it is served. A zero-size object is one
//!   empty chunk. So both sides know exactly which bytes a request covers:
//!   a receiver refuses (`PROTOCOL_VIOLATION`) a chunk outside its object's
//!   share or not contiguous with its last one (invariant 22), and an object
//!   with a share that got no bytes in a stream that ended normally is one
//!   the sender could not (or would not) serve.
//!
//! # Status
//!
//! `ExchangeStarted`/`ExchangeFinished{op: objects.pull|objects.push}` per
//! pull or push on status lane `index + 1`, with the real duration (from the
//! request to the last write of its bytes), the bytes moved, and the objects
//! **completed** (installed here for a pull, `Completed` in the peer's
//! results for a push); `ObjectError`/`Resolved` per object.
//!
//! # Interim fallback (§6.5)
//!
//! A local worker without loopback sessions or objects ops, or a peer that
//! answers `UNSUPPORTED_OPERATION` (a Wave 2 build), puts the key on the
//! interim path: worker `published-bulk-offer` → peer `objects.exchange` →
//! worker `published-bulk-apply` (legacy `bulk_sync::Page`, hex parts), run
//! by the download scheduler for both directions (op `objects.exchange`);
//! v3 is tried again when the key resets (new worker instance or remote
//! replica) or after [`INTERIM_RECHECK`]. [`run_bulk_lane_with`] runs the
//! interim path alone, as Wave 2 did. WP3.INT can delete the fallback once
//! every worker serves objects v3 (CCR-WP3.3-2).
use super::{
    Config,
    meta_exchange::{body_of, invalid, parse},
    scheduler::{
        DriveOptions, ExchangeError, Finished, Job, Next, Report, Signals, Target, check_session,
        drive, refuse, short, sync_interval,
    },
    worker_client::{self, ReadFlow, ReadRange, SessionWorkerClient},
};
use crate::{
    bulk_sync::{self, Page},
    model::{
        errors::{ErrorClass, ErrorWire, Scope},
        orchestrator::{CallError, Ctx, FolderView, PeerContext, PublishedEntry, limits},
        protocol::{
            LaneRole, LaneSpec, deadlines,
            frame::ChunkHeader,
            objects::{
                INFLIGHT_PULLS, INFLIGHT_READS, INFLIGHT_WRITES, LEASE_TTL, MAX_READ_LEN,
                MAX_WANT_ITEMS, ObjectOutcome, ObjectsPull, ObjectsPush, ObjectsReport,
                ObjectsResult, ObjectsWantsReply, ObjectsWantsRequest, PULL_BUDGET_BYTES,
                REPOS_PER_LANE, ReadHeader, WantHeader, WantItem, WantPriority, WriteItem,
                WriteResult,
            },
            ops,
        },
        status::{Direction, ObjectSource, StatusEvent},
    },
    paired_sync::SyncRoute,
    session::{self, Priority},
    util::{backoff::Backoff, cancel::Cancel, lock::lock, log::event},
};
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::{
        Arc, Condvar, Mutex, OnceLock, PoisonError,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

/// Worker ops of the interim path (served by `bulk_sync::worker_control`).
pub mod worker_ops {
    pub const OFFER: &str = "published-bulk-offer";
    pub const APPLY: &str = "published-bulk-apply";
    /// The acceptor's op (`inbound_objects`, WP2.8).
    pub const EXCHANGE: &str = "published-bulk-exchange";
}

/// Local `published-bulk-offer` and `published-bulk-apply` deadline.
pub const WORKER_DEADLINE: Duration = Duration::from_secs(30);
/// A key whose exchange moved object bytes runs again after this.
pub const CONTENT_YIELD: Duration = Duration::from_millis(10);
/// The download window of a lane: object bytes requested from the peer and
/// not yet written durably (or dropped) by the local worker.
pub const WINDOW_BYTES: u64 = PULL_BUDGET_BYTES;
/// Object bytes one push carries at most.
pub const PUSH_BUDGET_BYTES: u64 = PULL_BUDGET_BYTES;
/// `objects.*` calls a lane runs on the peer at once: the acceptor's
/// exchange slots per bulk session (`limits::BULK_HANDLERS`; a third call
/// would be refused `BUSY`). See CCR-WP3.3-1.
pub const PEER_CALLS: usize = limits::BULK_HANDLERS;
/// A key's round ends after this, so the lane's other repositories get a
/// turn (it runs again after [`CONTENT_YIELD`] when it moved data).
pub const ROUND_CAP: Duration = Duration::from_secs(10);
/// A key on the interim path tries objects v3 again after this (or at once
/// when the key resets).
pub const INTERIM_RECHECK: Duration = Duration::from_secs(600);
/// An acceptor stops reading a push while this much is waiting to be
/// written (the session then holds the dialer back).
pub const PUSH_BUFFERED_BYTES: u64 = 8 * 1024 * 1024;
/// An exchange served for the peer sends a PROGRESS this often while it
/// waits on its worker, so the dialer's idle deadline never runs out.
pub const KEEPALIVE: Duration = Duration::from_secs(5);
/// Bytes per worker `objects.write` call (the op's body bound).
const MAX_WRITE_BYTES: usize = MAX_READ_LEN as usize;
/// Objects per worker `objects.write` call.
const MAX_WRITE_ITEMS: usize = MAX_WANT_ITEMS;
/// Longest wait of a round's event loop before it re-checks its state.
const EVENT_WAIT: Duration = Duration::from_millis(100);
/// Per-object backoffs (object and source) remembered at most. Inserting
/// past it prunes once: expired entries first, then the earliest-expiring
/// ones down to [`BACKOFF_KEEP`] (so the pruning cost is amortized over the
/// quarter of the table it frees; WP3.INT re-review).
const BACKOFF_MAX: usize = 16 * 1024;
const BACKOFF_KEEP: usize = BACKOFF_MAX / 4 * 3;
/// The SHA-256 of no bytes: the only object whose size is 0. A want item of
/// another object with size 0 has an unknown size (an event without a size
/// descriptor; WP3.INT re-review), and its sender's total defines it.
pub const EMPTY_OBJECT: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

// ---------------------------------------------------------------------------
// Options and statistics

/// Options of a v3 bulk lane.
#[derive(Clone, Debug)]
pub struct LaneOptions {
    /// The download scheduler (`objects.pull` keys).
    pub download: DriveOptions,
    /// The upload scheduler (`objects.push` keys).
    pub upload: DriveOptions,
    /// See [`WINDOW_BYTES`].
    pub window_bytes: u64,
    /// Pulls in flight per lane (2).
    pub pulls: usize,
    /// See [`PEER_CALLS`].
    pub peer_calls: usize,
    /// Repositories with rounds on the lane at once, both directions of one
    /// repository counting once (`REPOS_PER_LANE`, 2).
    pub repos: usize,
    /// Worker `objects.write` calls in flight per round (4).
    pub writes: usize,
    /// Worker `objects.read` calls in flight per push (4).
    pub reads: usize,
    /// Peer `objects.pull`/`objects.push` idle deadline (30 s).
    pub idle: Duration,
    /// See [`ROUND_CAP`].
    pub round: Duration,
    /// The interim path (fallback).
    pub interim: BulkOptions,
    /// Where to count what the lane does (tests and diagnostics).
    pub stats: Option<Arc<LaneStats>>,
}

impl LaneOptions {
    /// The frozen limits of §6.4 and the configured interval.
    pub fn for_config(config: &Config) -> Self {
        let interval = sync_interval(config);
        Self {
            download: DriveOptions::new(REPOS_PER_LANE, interval),
            upload: DriveOptions::new(REPOS_PER_LANE, interval),
            window_bytes: WINDOW_BYTES,
            pulls: INFLIGHT_PULLS,
            peer_calls: PEER_CALLS,
            repos: REPOS_PER_LANE,
            writes: INFLIGHT_WRITES,
            reads: INFLIGHT_READS,
            idle: deadlines::OBJECTS_IDLE,
            round: ROUND_CAP,
            interim: BulkOptions::for_config(config),
            stats: None,
        }
    }
}

/// What a lane did (counters only grow; `*_peak` are maxima).
#[derive(Debug, Default)]
pub struct LaneStats {
    /// Download window bytes reserved now / at most.
    pub window_used: AtomicU64,
    pub window_peak: AtomicU64,
    /// Peer `objects.*` calls in flight now / at most.
    pub peer_calls: AtomicU64,
    pub peer_calls_peak: AtomicU64,
    /// Pulls in flight now / at most, and issued.
    pub pulls_in_flight: AtomicU64,
    pub pulls_peak: AtomicU64,
    pub pulls: AtomicU64,
    pub pushes: AtomicU64,
    /// Object bytes received by pulls, written durably here, sent by pushes.
    pub bytes_pulled: AtomicU64,
    pub bytes_written: AtomicU64,
    pub bytes_pushed: AtomicU64,
    /// Objects installed here / completed at the peer.
    pub objects_down: AtomicU64,
    pub objects_up: AtomicU64,
    /// Peer `objects.report` calls sent.
    pub reports: AtomicU64,
    /// Rounds run on objects v3, and interim exchanges.
    pub v3_rounds: AtomicU64,
    pub interim_exchanges: AtomicU64,
    /// Repositories with a round on the lane now / at most.
    pub repos: AtomicU64,
    pub repos_peak: AtomicU64,
}

fn bump(counter: &AtomicU64, by: u64) -> u64 {
    counter.fetch_add(by, Ordering::Relaxed) + by
}

fn peak(counter: &AtomicU64, value: u64) {
    counter.fetch_max(value, Ordering::Relaxed);
}

impl LaneStats {
    pub fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Wire helpers shared with `inbound_objects`

/// The byte range `[start, end)` each item of a pull or push covers (module
/// docs, "Allotment"): items in order, each from its offset, until `budget`
/// is spent; an item's share does not depend on whether it is served.
pub fn allot(items: &[WantItem], budget: u64) -> Vec<(u64, u64)> {
    let mut left = budget;
    items
        .iter()
        .map(|item| {
            let take = item.size.saturating_sub(item.offset).min(left);
            left -= take;
            (item.offset, item.offset + take)
        })
        .collect()
}

/// The worker reads of `items` under `spans` ([`allot`]): ≤ 1 MiB each, in
/// order; one empty read for a zero-size object, none for an item whose span
/// is empty otherwise.
pub fn read_ranges(items: &[WantItem], spans: &[(u64, u64)]) -> Vec<ReadRange> {
    let mut ranges = Vec::new();
    for (item, &(start, end)) in items.iter().zip(spans) {
        if item.size == 0 && start == 0 {
            ranges.push(ReadRange {
                hash: item.hash.clone(),
                offset: 0,
                len: 0,
            });
            continue;
        }
        let mut offset = start;
        while offset < end {
            let len = (end - offset).min(MAX_READ_LEN);
            ranges.push(ReadRange {
                hash: item.hash.clone(),
                offset,
                len,
            });
            offset += len;
        }
    }
    ranges
}

/// The largest CHUNK frame of object bytes on a bulk session: a worker read
/// (up to 1 MiB) goes out in pieces of at most this, so a control frame
/// queued behind object data (a pull's request, a cancel, a heartbeat)
/// waits for at most one piece in the session's own buffer (WP3.INT
/// re-review: 1 MiB frames on a slow uplink held them for seconds).
pub const WIRE_CHUNK_BYTES: usize = 256 * 1024;

/// The CHUNK frames of `bytes` read at `offset` of an object of `total`
/// bytes: `(header, piece)`, contiguous, at most [`WIRE_CHUNK_BYTES`] each,
/// one empty piece for no bytes.
pub fn wire_pieces<'a>(
    hash: &str,
    offset: u64,
    total: u64,
    bytes: &'a [u8],
) -> Vec<(ChunkHeader, &'a [u8])> {
    let piece = |at: u64, part: &'a [u8]| {
        let end = at + part.len() as u64;
        (
            ChunkHeader {
                id: 0,
                hash: Some(hash.to_owned()),
                offset: at,
                total,
                last: end == total,
            },
            part,
        )
    };
    if bytes.is_empty() {
        return vec![piece(offset, bytes)];
    }
    let mut at = offset;
    bytes
        .chunks(WIRE_CHUNK_BYTES)
        .map(|part| {
            let pair = piece(at, part);
            at += part.len() as u64;
            pair
        })
        .collect()
}

/// Whether an item covers a byte or is a zero-size object (it is sent).
pub fn covered(item: &WantItem, span: (u64, u64)) -> bool {
    span.1 > span.0 || (item.size == 0 && item.offset == 0)
}

/// Whether a want item's size is unknown to its receiver: size 0 for an
/// object that is not the empty one (an event without a size descriptor,
/// which the core keeps for legacy journals). Such an item is sent like a
/// zero-size object, one empty chunk, which carries the sender's total; the
/// receiver then wants it with that size (WP3.INT re-review).
pub fn size_unknown(item: &WantItem) -> bool {
    item.size == 0 && item.offset == 0 && item.hash != EMPTY_OBJECT
}

/// The lease session of a dialer's pulls on `lane` of `peer` (`slot` 0 or
/// 1): stable across reconnects, so a resumed transfer finds its own lease.
pub fn pull_lease(peer: &str, lane: LaneSpec, slot: u64) -> String {
    format!(
        "pull:{peer}:{}/{}:{}",
        lane.index,
        lane.count,
        if slot.is_multiple_of(2) { 'a' } else { 'b' }
    )
}

/// The handoff name of a dialer's pulls on `lane` of `peer`: the prefix of
/// both lease sessions of the lane; a run of the lane claims handed-off
/// objects as `<this>#<run number>` (module docs, "Lease handoff").
pub fn pull_holder(peer: &str, lane: LaneSpec) -> String {
    format!("pull:{peer}:{}/{}", lane.index, lane.count)
}

/// The lease session of the pushes an acceptor serves for `peer` on `lane`
/// (its `objects.wants` takes the leases, its writes use them). The
/// acceptor claims handed-off objects for that lane as `<this>@<bulk
/// session>` (module docs, "Lease handoff").
pub fn push_lease(peer: &str, lane: LaneSpec) -> String {
    format!("push:{peer}:{}/{}", lane.index, lane.count)
}

/// Whether an error says "objects v3 is not served here" (a Wave 2 worker or
/// peer): the interim path takes over.
pub fn is_unsupported(error: &CallError) -> bool {
    error.code() == "UNSUPPORTED_OPERATION"
}

/// The outcome a failed worker read stands for.
pub fn read_outcome(error: &CallError) -> ObjectOutcome {
    match error.code().as_str() {
        "NOT_PERMITTED" | "UNAUTHORIZED_OBJECT" => ObjectOutcome::NotPermitted,
        "NOT_AVAILABLE" | "OFFLINE_OBJECT_MISSING" | "CONTENT_UNAVAILABLE" => {
            ObjectOutcome::NotAvailable
        }
        "CORRUPT_AT_SOURCE" => ObjectOutcome::CorruptAtSource,
        "SIZE_MISMATCH" => ObjectOutcome::SizeMismatch,
        _ => ObjectOutcome::Failed(error.to_wire()),
    }
}

/// Whether a failed write's code is a resume point of the worker
/// (`RESUME_OFFSET_AHEAD`, `OBJECT_PART_CHANGED`): the object continues from
/// the durable offset the worker answered (CCR-WP3.2-5).
pub fn is_resume_point(code: &str) -> bool {
    matches!(code, "RESUME_OFFSET_AHEAD" | "OBJECT_PART_CHANGED")
}

/// Whether a failed item's (or write call's) error is contention: the §8
/// Contention class, or a busy database (`SQLITE_BUSY`, Transient in the §8
/// table). The worker that answers it records nothing for the object
/// (`objects_rpc::failure_kind`), and neither does a lane: no backoff, no
/// `ObjectError`; the object continues in the next round (WP3.INT
/// re-review: one busy write batch of 256 tiny objects backed them off for
/// 30 s).
pub fn is_contention(error: &ErrorWire) -> bool {
    error.class == ErrorClass::Contention || error.code == "SQLITE_BUSY"
}

/// Whether an outcome is a per-object failure worth a backoff and an
/// `ObjectError` (not success, not contention, see [`is_contention`]).
/// `Unsolicited` is none: the receiving worker no longer wants the object
/// (its event was quarantined, or it never wanted it), which is nobody's
/// failure; the object settles (WP3.INT re-review).
pub fn is_failure(outcome: &ObjectOutcome) -> bool {
    match outcome {
        ObjectOutcome::Completed
        | ObjectOutcome::Progressed { .. }
        | ObjectOutcome::Skipped
        | ObjectOutcome::InFlightElsewhere
        | ObjectOutcome::Unsolicited => false,
        ObjectOutcome::Failed(error) => !is_contention(error),
        _ => true,
    }
}

/// The wire error of a failed outcome.
pub fn outcome_error(hash: &str, outcome: &ObjectOutcome, detail: &str) -> Option<ErrorWire> {
    match outcome {
        ObjectOutcome::Failed(error) => Some(error.clone()),
        other => other
            .code()
            .map(|code| ErrorWire::coded(code, format!("object {hash}: {detail}"))),
    }
}

// ---------------------------------------------------------------------------
// Per-object backoff (§8 objects: 30 s → 1 h), per source

struct Held {
    backoff: Backoff,
    until: Instant,
    code: String,
}

/// Per object (local installation, repository, direction, hash), per source
/// peer; `entries` counts the (object, peer) pairs, bounded by
/// [`BACKOFF_MAX`].
#[derive(Default)]
struct HeldTable {
    map: HashMap<String, HashMap<String, Held>>,
    entries: usize,
}

impl HeldTable {
    fn get(&self, key: &ObjectKey) -> Option<&Held> {
        self.map.get(&key.object)?.get(&key.peer)
    }

    /// Room for one more entry: expired entries go first, then the
    /// earliest-expiring ones, down to [`BACKOFF_KEEP`]. An entry dropped
    /// early only loses its escalation (and the code it would resolve).
    fn make_room(&mut self, now: Instant) {
        if self.entries < BACKOFF_MAX {
            return;
        }
        self.map.retain(|_, peers| {
            peers.retain(|_, held| now < held.until);
            !peers.is_empty()
        });
        self.entries = self.map.values().map(HashMap::len).sum();
        if self.entries <= BACKOFF_KEEP {
            return;
        }
        let mut until: Vec<Instant> = self
            .map
            .values()
            .flat_map(|peers| peers.values().map(|held| held.until))
            .collect();
        let drop = self.entries - BACKOFF_KEEP;
        until.select_nth_unstable(drop - 1);
        let cut = until[drop - 1];
        let mut left = drop;
        self.map.retain(|_, peers| {
            peers.retain(|_, held| {
                if left > 0 && held.until <= cut {
                    left -= 1;
                    false
                } else {
                    true
                }
            });
            !peers.is_empty()
        });
        self.entries = self.map.values().map(HashMap::len).sum();
    }

    /// The entry of `key`; a new one backs off on `schedule`.
    fn entry(&mut self, key: &ObjectKey, now: Instant, schedule: fn() -> Backoff) -> &mut Held {
        let known = self
            .map
            .get(&key.object)
            .is_some_and(|peers| peers.contains_key(&key.peer));
        if !known {
            self.make_room(now);
            self.entries += 1;
        }
        self.map
            .entry(key.object.clone())
            .or_default()
            .entry(key.peer.clone())
            .or_insert_with(|| Held {
                backoff: schedule(),
                until: now,
                code: String::new(),
            })
    }

    fn remove(&mut self, key: &ObjectKey) -> Option<Held> {
        let peers = self.map.get_mut(&key.object)?;
        let held = peers.remove(&key.peer);
        if peers.is_empty() {
            self.map.remove(&key.object);
        }
        if held.is_some() {
            self.entries -= 1;
        }
        held
    }

    fn remove_object(&mut self, object: &str) -> Vec<Held> {
        let held: Vec<Held> = self
            .map
            .remove(object)
            .into_iter()
            .flat_map(HashMap::into_values)
            .collect();
        self.entries -= held.len();
        held
    }
}

fn held() -> &'static Mutex<HeldTable> {
    static HELD: OnceLock<Mutex<HeldTable>> = OnceLock::new();
    HELD.get_or_init(|| Mutex::new(HeldTable::default()))
}

/// Per-object backoffs remembered now (diagnostics and tests).
pub fn object_backoffs() -> usize {
    lock(held()).entries
}

/// A per-object backoff key: the object (local installation, repository,
/// direction, hash) and the peer it moves from or to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ObjectKey {
    object: String,
    peer: String,
}

pub(crate) fn object_key(local: &str, peer: &str, repo: &str, up: bool, hash: &str) -> ObjectKey {
    let direction = if up { "up" } else { "down" };
    ObjectKey {
        object: format!("{local}\u{1f}{repo}\u{1f}{direction}\u{1f}{hash}"),
        peer: peer.to_owned(),
    }
}

/// Whether `key` is inside its backoff.
pub(crate) fn object_blocked(key: &ObjectKey) -> bool {
    lock(held())
        .get(key)
        .is_some_and(|held| Instant::now() < held.until)
}

/// Records a failure of `key`; returns the delay before it is tried again.
/// An empty `code` is a backoff without an error in the hub (a source that
/// did not serve an object, see "Lease handoff"); it keeps the code of an
/// earlier failure.
pub(crate) fn object_failed(key: &ObjectKey, code: &str) -> Duration {
    let now = Instant::now();
    let mut table = lock(held());
    let held = table.entry(key, now, Backoff::objects);
    let delay = held.backoff.failure(now);
    held.until = now + delay;
    if !code.is_empty() {
        held.code = code.to_owned();
    }
    delay
}

/// The backoff of an object a peer did not send although we offered it to
/// that peer (it lacks the object, or may not serve it): no failure, and
/// often over soon (a relay that receives it a moment later), so a short
/// schedule, 5 s → 5 min, instead of the failures' 30 s → 1 h (WP3.INT
/// re-review: a relay delivered such an object 28 s late).
fn unserved_backoff() -> Backoff {
    Backoff::new(
        Duration::from_secs(5),
        Duration::from_secs(300),
        0.2,
        Duration::ZERO,
    )
}

/// Records that `key`'s peer did not serve its object (see
/// [`unserved_backoff`]); an object already backing off for a failure keeps
/// its schedule and its code. Returns the delay.
pub(crate) fn object_unserved(key: &ObjectKey) -> Duration {
    let now = Instant::now();
    let mut table = lock(held());
    let held = table.entry(key, now, unserved_backoff);
    let delay = held.backoff.failure(now);
    held.until = now + delay;
    delay
}

/// The objects of `repo` moving in direction `up` between `local` and `peer`
/// may be tried again at once, with their escalation reset (their codes stay,
/// to be resolved when they complete): the peer now holds objects it lacked
/// (its published content counters grew), so what it could not serve may now
/// be served (WP3.INT re-review). Returns how many were in their backoff.
pub(crate) fn object_backoffs_reset(local: &str, peer: &str, repo: &str, up: bool) -> usize {
    let direction = if up { "up" } else { "down" };
    let prefix = format!("{local}\u{1f}{repo}\u{1f}{direction}\u{1f}");
    let now = Instant::now();
    let mut reset = 0;
    let mut table = lock(held());
    for (object, peers) in table.map.iter_mut() {
        if !object.starts_with(&prefix) {
            continue;
        }
        if let Some(held) = peers.get_mut(peer)
            && now < held.until
        {
            held.until = now;
            held.backoff = Backoff::objects();
            reset += 1;
        }
    }
    reset
}

/// `key` completed (with its peer): forgets its failures; returns the code
/// of the failure it had, if any (to resolve in the hub).
pub(crate) fn object_completed(key: &ObjectKey) -> Option<String> {
    lock(held())
        .remove(key)
        .map(|held| held.code)
        .filter(|code| !code.is_empty())
}

/// `key`'s object is installed here (from whichever source): forgets the
/// failures of every source of it; returns their codes, each once (to
/// resolve in the hub).
pub(crate) fn object_installed(key: &ObjectKey) -> Vec<String> {
    let mut codes: Vec<String> = lock(held())
        .remove_object(&key.object)
        .into_iter()
        .map(|held| held.code)
        .filter(|code| !code.is_empty())
        .collect();
    codes.sort();
    codes.dedup();
    codes
}

// ---------------------------------------------------------------------------
// Lease handoff between this daemon's transfers (module docs)

/// How long a transfer's claim on a handed-off object stands unless that
/// transfer renews it (it does while it works: module docs, "Lease
/// handoff"). Only the claiming transfer renews it, never the source whose
/// lease the object is under.
pub const CLAIM_TTL: Duration = Duration::from_secs(30);
/// A working holder renews its claims this often.
const CLAIM_RENEW: Duration = Duration::from_secs(1);

/// A transfer moving a handed-off object, until when (renewed by it alone).
#[derive(Clone, Debug)]
struct Claim {
    holder: String,
    until: Instant,
}

/// An object leased at one of our workers to a session that does not move
/// it (its source cannot serve it: *parked*), or that a transfer of another
/// source continues under that session's lease (*claimed*).
#[derive(Clone, Debug)]
struct Handoff {
    /// Size, the worker's durable offset as it last reported it, priority.
    item: WantItem,
    /// The worker session that holds the lease.
    lease: String,
    /// The transfer moving it. None, or lapsed: parked (another transfer
    /// may take it, and its own source moves it again once it can).
    claim: Option<Claim>,
    /// When the lease lapses unless something renews it.
    until: Instant,
}

impl Handoff {
    /// The transfer whose claim stands at `now`.
    fn holder(&self, now: Instant) -> Option<&str> {
        self.claim
            .as_ref()
            .filter(|claim| now < claim.until)
            .map(|claim| claim.holder.as_str())
    }

    /// Whether `holder` claimed it and nobody took it since (its claim may
    /// have lapsed: it is still the last to have moved it).
    fn claimed_by(&self, holder: &str) -> bool {
        self.claim
            .as_ref()
            .is_some_and(|claim| claim.holder == holder)
    }
}

/// Handoffs per worker instance (a worker's leases are its own).
fn handoffs() -> &'static Mutex<HashMap<String, HashMap<String, Handoff>>> {
    static HANDOFFS: OnceLock<Mutex<HashMap<String, HashMap<String, Handoff>>>> = OnceLock::new();
    HANDOFFS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Handoffs remembered per worker instance (expired ones are dropped first;
/// past this, nothing more is parked: those leases simply lapse).
const MAX_HANDOFFS: usize = 64 * 1024;

fn handoff_put(map: &mut HashMap<String, Handoff>, hash: &str, entry: Handoff, now: Instant) {
    if map.len() >= MAX_HANDOFFS && !map.contains_key(hash) {
        map.retain(|_, handoff| now < handoff.until);
        if map.len() >= MAX_HANDOFFS {
            return;
        }
    }
    map.insert(hash.to_owned(), entry);
}

/// A worker want of session `lease` returned `item` (so `lease` holds it,
/// renewed) for transfer `holder`; returns whether `holder` moves it. Not
/// when another transfer's claim on it under that lease stands, nor when
/// `blocked` (its source cannot serve it now): it is then parked, so another
/// source of this daemon can continue it at once, under the same lease. The
/// want renews the lease, never another transfer's claim: a claim whose
/// holder went away lapses, and the object is parked again (or moved by its
/// own source once that can serve it).
pub(crate) fn handoff_wanted(
    instance: &str,
    item: &WantItem,
    lease: &str,
    holder: &str,
    blocked: bool,
) -> bool {
    let now = Instant::now();
    let mut all = lock(handoffs());
    if let Some(map) = all.get_mut(instance) {
        if let Some(entry) = map.get_mut(&item.hash)
            && entry.lease == lease
            && now < entry.until
        {
            // The want renewed the lease, and its offset is our worker's
            // newest word.
            entry.until = now + LEASE_TTL;
            entry.item = item.clone();
            if entry.holder(now).is_some_and(|other| other != holder) {
                // Another transfer continues it under this lease.
                return false;
            }
            if blocked {
                entry.claim = None;
                return false;
            }
        }
        if !blocked {
            // This lease's own transfer moves it: nothing to hand off.
            map.remove(&item.hash);
            if map.is_empty() {
                all.remove(instance);
            }
            return true;
        }
    }
    if !blocked {
        return true;
    }
    let entry = Handoff {
        item: item.clone(),
        lease: lease.to_owned(),
        claim: None,
        until: now + LEASE_TTL,
    };
    let map = all.entry(instance.to_owned()).or_default();
    handoff_put(map, &item.hash, entry, now);
    false
}

/// Parks `item` (its offset: the durable offset as the worker reported it),
/// leased to `lease`, for that lease's own transfer: its source cannot
/// serve it; another may continue it.
pub(crate) fn handoff_park(instance: &str, item: &WantItem, lease: &str) {
    let now = Instant::now();
    let mut all = lock(handoffs());
    let map = all.entry(instance.to_owned()).or_default();
    let entry = Handoff {
        item: item.clone(),
        lease: lease.to_owned(),
        claim: None,
        until: now + LEASE_TTL,
    };
    handoff_put(map, &item.hash, entry, now);
}

/// `holder` gives its claim on `hash` (under `lease`) back: it cannot serve
/// it, or it stopped (its round failed, its session ended). The object is
/// parked again (`item`: where our worker stands, when known). Nothing
/// changes when another transfer took it since, or the lease changed.
pub(crate) fn handoff_unclaim(
    instance: &str,
    hash: &str,
    lease: &str,
    holder: &str,
    item: Option<&WantItem>,
) {
    let mut all = lock(handoffs());
    if let Some(entry) = all.get_mut(instance).and_then(|map| map.get_mut(hash))
        && entry.lease == lease
        && entry.claimed_by(holder)
    {
        entry.claim = None;
        if let Some(item) = item {
            entry.item = item.clone();
        }
    }
}

/// `holder` stopped (its lane run or session ended, its request failed):
/// every claim it still has is given back (`instance`: on that worker only;
/// `None`: on every worker).
pub(crate) fn handoff_unclaim_all(instance: Option<&str>, holder: &str) {
    let mut all = lock(handoffs());
    for (name, map) in all.iter_mut() {
        if instance.is_some_and(|instance| instance != name) {
            continue;
        }
        for entry in map.values_mut() {
            if entry.claimed_by(holder) {
                entry.claim = None;
            }
        }
    }
}

/// `holder` is still working: its claims at `instance` stand for another
/// [`CLAIM_TTL`].
pub(crate) fn handoff_renew(instance: &str, holder: &str) {
    let until = Instant::now() + CLAIM_TTL;
    let mut all = lock(handoffs());
    if let Some(map) = all.get_mut(instance) {
        for entry in map.values_mut() {
            if let Some(claim) = entry.claim.as_mut()
                && claim.holder == holder
            {
                claim.until = until;
            }
        }
    }
}

/// What `holder` moves next under leases its own wants do not return: its
/// continuations, then parked objects (none, or a lapsed claim) that
/// `accept` allows, at most `max_items` and about `max_bytes`. Each comes
/// with the lease to write it under; `holder` claims each for [`CLAIM_TTL`].
pub(crate) fn handoff_take(
    instance: &str,
    holder: &str,
    max_items: usize,
    max_bytes: u64,
    accept: &dyn Fn(&str) -> bool,
) -> Vec<(WantItem, String)> {
    let now = Instant::now();
    let mut all = lock(handoffs());
    let Some(map) = all.get_mut(instance) else {
        return Vec::new();
    };
    map.retain(|_, handoff| now < handoff.until);
    if map.is_empty() {
        all.remove(instance);
        return Vec::new();
    }
    let mut candidates: Vec<(bool, WantPriority, u64, String)> = map
        .iter()
        .filter_map(|(hash, handoff)| {
            let mine = handoff.claimed_by(holder);
            (mine || (handoff.holder(now).is_none() && accept(hash))).then(|| {
                let remaining = handoff.item.size.saturating_sub(handoff.item.offset);
                (!mine, handoff.item.priority, remaining, hash.clone())
            })
        })
        .collect();
    candidates.sort();
    let mut taken = Vec::new();
    let mut bytes = 0u64;
    for (_, _, remaining, hash) in candidates {
        if taken.len() >= max_items || (bytes >= max_bytes && !taken.is_empty()) {
            break;
        }
        if let Some(handoff) = map.get_mut(&hash) {
            handoff.claim = Some(Claim {
                holder: holder.to_owned(),
                until: now + CLAIM_TTL,
            });
            taken.push((handoff.item.clone(), handoff.lease.clone()));
            bytes = bytes.saturating_add(remaining);
        }
    }
    taken
}

/// `holder` continues `item` (its offset: the worker's durable offset after
/// its last write) under `lease`, which that write renewed: its claim stands
/// for another [`CLAIM_TTL`]. Nothing changes when the object is no longer
/// in the table (its source moves it again, or it is installed), is under
/// another lease, or another transfer's claim on it stands.
pub(crate) fn handoff_progress(instance: &str, item: &WantItem, lease: &str, holder: &str) {
    let now = Instant::now();
    let mut all = lock(handoffs());
    if let Some(entry) = all
        .get_mut(instance)
        .and_then(|map| map.get_mut(&item.hash))
        && entry.lease == lease
        && entry.holder(now).is_none_or(|other| other == holder)
    {
        entry.item = item.clone();
        entry.claim = Some(Claim {
            holder: holder.to_owned(),
            until: now + CLAIM_TTL,
        });
        entry.until = entry.until.max(now + LEASE_TTL);
    }
}

/// `hash` no longer moves under `lease` here (installed, failed without a
/// known offset, or the lease is another session's now).
pub(crate) fn handoff_release(instance: &str, hash: &str, lease: &str) {
    let mut all = lock(handoffs());
    if let Some(map) = all.get_mut(instance)
        && map.get(hash).is_some_and(|handoff| handoff.lease == lease)
    {
        map.remove(hash);
        if map.is_empty() {
            all.remove(instance);
        }
    }
}

/// Renews a working holder's claims at one worker, at most every
/// [`CLAIM_RENEW`] (and only once it claimed something).
pub(crate) struct Renewal {
    instance: String,
    holder: String,
    active: std::cell::Cell<bool>,
    last: std::cell::Cell<Instant>,
}

impl Renewal {
    pub(crate) fn new(instance: &str, holder: &str) -> Self {
        Self {
            instance: instance.to_owned(),
            holder: holder.to_owned(),
            active: std::cell::Cell::new(false),
            last: std::cell::Cell::new(Instant::now()),
        }
    }

    /// The holder claimed something: renew from now on.
    pub(crate) fn activate(&self) {
        self.active.set(true);
    }

    /// Renews if due (`now`: whether or not it is).
    pub(crate) fn tick(&self, now: bool) {
        if self.active.get() && (now || self.last.get().elapsed() >= CLAIM_RENEW) {
            self.last.set(Instant::now());
            handoff_renew(&self.instance, &self.holder);
        }
    }
}

// ---------------------------------------------------------------------------
// Window and peer-call slots of a lane

/// Waiters served in arrival order (a releasing thread cannot jump the
/// queue by re-acquiring at once).
#[derive(Default)]
struct Queue {
    next: u64,
    waiting: VecDeque<u64>,
}

impl Queue {
    fn join(&mut self) -> u64 {
        let ticket = self.next;
        self.next += 1;
        self.waiting.push_back(ticket);
        ticket
    }

    fn first(&self, ticket: u64) -> bool {
        self.waiting.front() == Some(&ticket)
    }

    fn leave(&mut self, ticket: u64) {
        self.waiting.retain(|waiting| *waiting != ticket);
    }
}

/// A byte budget shared by a lane's pulls (module docs).
pub(crate) struct Window {
    capacity: u64,
    state: Mutex<WindowState>,
    changed: Condvar,
    stats: Option<Arc<LaneStats>>,
}

#[derive(Default)]
struct WindowState {
    used: u64,
    queue: Queue,
}

impl Window {
    pub(crate) fn new(capacity: u64, stats: Option<Arc<LaneStats>>) -> Self {
        Self {
            capacity: capacity.max(1),
            state: Mutex::new(WindowState::default()),
            changed: Condvar::new(),
            stats,
        }
    }

    pub(crate) fn capacity(&self) -> u64 {
        self.capacity
    }

    fn take(&self, state: &mut WindowState, min: u64, max: u64) -> Option<u64> {
        let free = self.capacity - state.used.min(self.capacity);
        let min = min.min(self.capacity);
        if free < min {
            return None;
        }
        let take = free.min(max.max(min));
        state.used += take;
        if let Some(stats) = &self.stats {
            stats.window_used.store(state.used, Ordering::Relaxed);
            peak(&stats.window_peak, state.used);
        }
        Some(take)
    }

    /// Waits (in turn with other waiters) until `min` bytes are free and
    /// takes up to `max`; `None` once `stop` holds.
    pub(crate) fn reserve(&self, min: u64, max: u64, stop: &dyn Fn() -> bool) -> Option<u64> {
        let mut state = lock(&self.state);
        let ticket = state.queue.join();
        let taken = loop {
            if stop() {
                break None;
            }
            if state.queue.first(ticket)
                && let Some(taken) = self.take(&mut state, min, max)
            {
                break Some(taken);
            }
            state = self
                .changed
                .wait_timeout(state, EVENT_WAIT)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        };
        state.queue.leave(ticket);
        drop(state);
        self.changed.notify_all();
        taken
    }

    /// Takes up to `max` if `min` bytes are free and nobody waits.
    pub(crate) fn try_reserve(&self, min: u64, max: u64) -> Option<u64> {
        let mut state = lock(&self.state);
        if !state.queue.waiting.is_empty() {
            return None;
        }
        self.take(&mut state, min, max)
    }

    pub(crate) fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut state = lock(&self.state);
        state.used = state.used.saturating_sub(bytes);
        if let Some(stats) = &self.stats {
            stats.window_used.store(state.used, Ordering::Relaxed);
        }
        drop(state);
        self.changed.notify_all();
    }
}

/// The peer `objects.*` calls a lane may have in flight (module docs).
pub(crate) struct Slots {
    capacity: usize,
    state: Mutex<SlotsState>,
    changed: Condvar,
    stats: Option<Arc<LaneStats>>,
}

#[derive(Default)]
struct SlotsState {
    used: usize,
    queue: Queue,
}

/// One peer call slot, given back when dropped.
pub(crate) struct Slot(Arc<Slots>);

impl Drop for Slot {
    fn drop(&mut self) {
        let mut state = lock(&self.0.state);
        state.used = state.used.saturating_sub(1);
        if let Some(stats) = &self.0.stats {
            stats.peer_calls.store(state.used as u64, Ordering::Relaxed);
        }
        drop(state);
        self.0.changed.notify_all();
    }
}

impl Slots {
    pub(crate) fn new(capacity: usize, stats: Option<Arc<LaneStats>>) -> Arc<Self> {
        Arc::new(Self {
            capacity: capacity.max(1),
            state: Mutex::new(SlotsState::default()),
            changed: Condvar::new(),
            stats,
        })
    }

    fn grant(self: &Arc<Self>, state: &mut SlotsState) -> Slot {
        state.used += 1;
        if let Some(stats) = &self.stats {
            stats.peer_calls.store(state.used as u64, Ordering::Relaxed);
            peak(&stats.peer_calls_peak, state.used as u64);
        }
        Slot(self.clone())
    }

    /// Waits for a slot in turn; `None` once `stop` holds.
    pub(crate) fn acquire(self: &Arc<Self>, stop: &dyn Fn() -> bool) -> Option<Slot> {
        let mut state = lock(&self.state);
        let ticket = state.queue.join();
        let slot = loop {
            if stop() {
                break None;
            }
            if state.queue.first(ticket) && state.used < self.capacity {
                break Some(self.grant(&mut state));
            }
            state = self
                .changed
                .wait_timeout(state, EVENT_WAIT)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        };
        state.queue.leave(ticket);
        drop(state);
        self.changed.notify_all();
        slot
    }
}

/// At most `capacity` repositories have rounds on a lane at once (both
/// directions of a repository count once); waiting repositories enter in
/// order, before a repository that just left comes back.
pub(crate) struct RepoGate {
    capacity: usize,
    state: Mutex<RepoGateState>,
    changed: Condvar,
    stats: Option<Arc<LaneStats>>,
}

#[derive(Default)]
struct RepoGateState {
    active: HashMap<String, usize>,
    waiting: VecDeque<String>,
}

/// A repository's turn on a lane, given back when dropped.
pub(crate) struct RepoTurn<'a> {
    gate: &'a RepoGate,
    repo: String,
}

impl RepoGate {
    pub(crate) fn new(capacity: usize, stats: Option<Arc<LaneStats>>) -> Self {
        Self {
            capacity: capacity.max(1),
            state: Mutex::new(RepoGateState::default()),
            changed: Condvar::new(),
            stats,
        }
    }

    /// Waits for `repo`'s turn; `None` once `stop` holds.
    pub(crate) fn enter(&self, repo: &str, stop: &dyn Fn() -> bool) -> Option<RepoTurn<'_>> {
        let mut state = lock(&self.state);
        let mut queued = false;
        let entered = loop {
            if stop() {
                break false;
            }
            let first = state.waiting.front().is_none_or(|front| front == repo);
            if state.active.contains_key(repo) || (state.active.len() < self.capacity && first) {
                break true;
            }
            if !queued {
                state.waiting.push_back(repo.to_owned());
                queued = true;
            }
            state = self
                .changed
                .wait_timeout(state, EVENT_WAIT)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        };
        if queued && let Some(index) = state.waiting.iter().position(|name| name == repo) {
            state.waiting.remove(index);
        }
        if !entered {
            drop(state);
            self.changed.notify_all();
            return None;
        }
        *state.active.entry(repo.to_owned()).or_default() += 1;
        if let Some(stats) = &self.stats {
            let count = state.active.len() as u64;
            stats.repos.store(count, Ordering::Relaxed);
            peak(&stats.repos_peak, count);
        }
        drop(state);
        self.changed.notify_all();
        Some(RepoTurn {
            gate: self,
            repo: repo.to_owned(),
        })
    }
}

impl Drop for RepoTurn<'_> {
    fn drop(&mut self) {
        let mut state = lock(&self.gate.state);
        if let Some(count) = state.active.get_mut(&self.repo) {
            *count -= 1;
            if *count == 0 {
                state.active.remove(&self.repo);
            }
        }
        if let Some(stats) = &self.gate.stats {
            stats
                .repos
                .store(state.active.len() as u64, Ordering::Relaxed);
        }
        drop(state);
        self.gate.changed.notify_all();
    }
}

// ---------------------------------------------------------------------------
// Sink: received object streams → local worker writes

/// What a [`Sink`] reports, from the thread that settled it (never under
/// the sink's lock).
#[derive(Clone, Debug)]
pub(crate) enum SinkEvent {
    /// `bytes` offered under `tag` left the sink: written durably
    /// (`written`), or dropped.
    Released { tag: u64, bytes: u64, written: bool },
    /// `hash` reached its final state in this sink. `call` is set when the
    /// whole write call failed (not an outcome of this object).
    Settled {
        hash: String,
        tag: u64,
        outcome: ObjectOutcome,
        call: bool,
    },
}

/// Receives what one sink step settled, in order and together (never
/// empty): a write's releases and the objects it settled arrive at once.
pub(crate) type SinkHook = Box<dyn Fn(Vec<SinkEvent>) + Send + Sync>;

/// How a [`Sink`] writes.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SinkOptions {
    /// Write calls in flight (4).
    pub writers: usize,
    /// Bytes per write call (1 MiB).
    pub max_write: usize,
    /// Objects per write call (256).
    pub max_items: usize,
    pub deadline: Duration,
}

impl Default for SinkOptions {
    fn default() -> Self {
        Self {
            writers: INFLIGHT_WRITES,
            max_write: MAX_WRITE_BYTES,
            max_items: MAX_WRITE_ITEMS,
            deadline: worker_client::deadlines::WRITE,
        }
    }
}

/// The final state of one object in a finished [`Sink`].
#[derive(Clone, Debug)]
pub(crate) struct SinkResult {
    /// The settled outcome (`None`: never settled; see `durable`).
    pub outcome: Option<ObjectOutcome>,
    /// The worker's durable offset as last reported (or the start offset).
    /// Not known when `call` is set.
    pub durable: u64,
    /// Bytes written durably through this sink.
    pub written: u64,
    /// A whole write call with this object failed (timed out, lost, refused,
    /// the worker unreachable or erring for the call): a failure of the call,
    /// not of the object, and what the worker kept is not known.
    pub call: Option<CallError>,
}

/// Received object bytes on their way into the local worker (`objects.write`
/// with the lease of the session that wanted them): ordered per object (out
/// of order ranges wait in `pending`), at most `writers` calls in flight and
/// one per object, tiny objects batched. Every byte offered is released
/// exactly once ([`SinkEvent::Released`]).
pub(crate) struct Sink {
    inner: Arc<SinkInner>,
    writers: Vec<thread::JoinHandle<()>>,
}

struct SinkInner {
    client: Arc<SessionWorkerClient>,
    route: SyncRoute,
    options: SinkOptions,
    state: Mutex<SinkState>,
    changed: Condvar,
    hook: SinkHook,
}

#[derive(Default)]
struct SinkState {
    objects: HashMap<String, SinkObject>,
    ready: VecDeque<String>,
    writing: usize,
    buffered: u64,
    closing: bool,
    fatal: Option<CallError>,
}

struct SinkObject {
    lease: String,
    total: u64,
    /// Next byte expected in `buf` (the end of the contiguous data).
    next: u64,
    /// Nothing at or after this is accepted (a pull ended short).
    limit: u64,
    durable: u64,
    written: u64,
    /// Contiguous data `[next − buf_len, next)` as received pieces (tag,
    /// bytes), in order: a write takes whole pieces, so nothing is moved.
    buf: VecDeque<(u64, Vec<u8>)>,
    buf_len: usize,
    /// Ranges ahead of `next`: offset → (tag, bytes).
    pending: BTreeMap<u64, (u64, Vec<u8>)>,
    /// The empty write of a zero-size object is due.
    empty_due: bool,
    writing: bool,
    queued: bool,
    flush: bool,
    settled: Option<ObjectOutcome>,
    /// See [`SinkResult::call`].
    call: Option<CallError>,
    last_tag: u64,
}

/// One write call being made.
struct Batch {
    lease: String,
    items: Vec<WriteItem>,
    body: Vec<u8>,
    /// Per item: its tags (tag, bytes) in order.
    tags: Vec<Vec<(u64, u64)>>,
}

impl SinkState {
    fn release(&mut self, tag: u64, bytes: u64, written: bool, events: &mut Vec<SinkEvent>) {
        self.buffered = self.buffered.saturating_sub(bytes);
        if bytes > 0 {
            events.push(SinkEvent::Released {
                tag,
                bytes,
                written,
            });
        }
    }

    /// Drops whatever `hash` still holds (buffered and pending data).
    fn drop_data(&mut self, hash: &str, events: &mut Vec<SinkEvent>) {
        let Some(object) = self.objects.get_mut(hash) else {
            return;
        };
        let tags: Vec<(u64, u64)> = object
            .buf
            .drain(..)
            .map(|(tag, bytes)| (tag, bytes.len() as u64))
            .collect();
        object.buf_len = 0;
        object.empty_due = false;
        let pending: Vec<(u64, u64)> = std::mem::take(&mut object.pending)
            .into_values()
            .map(|(tag, bytes)| (tag, bytes.len() as u64))
            .collect();
        for (tag, bytes) in tags.into_iter().chain(pending) {
            self.release(tag, bytes, false, events);
        }
    }

    /// `call`: the whole write call failed with this error (see
    /// [`SinkResult::call`]).
    fn settle(
        &mut self,
        hash: &str,
        outcome: ObjectOutcome,
        call: Option<CallError>,
        events: &mut Vec<SinkEvent>,
    ) {
        self.drop_data(hash, events);
        let Some(object) = self.objects.get_mut(hash) else {
            return;
        };
        if object.settled.is_some() {
            return;
        }
        object.settled = Some(outcome.clone());
        events.push(SinkEvent::Settled {
            hash: hash.to_owned(),
            tag: object.last_tag,
            outcome,
            call: call.is_some(),
        });
        object.call = call;
    }

    fn dispatchable(&self, object: &SinkObject, max_write: usize) -> bool {
        if object.writing || object.settled.is_some() {
            return false;
        }
        object.empty_due
            || (object.buf_len > 0
                && (object.buf_len >= max_write
                    || object.next == object.total
                    || object.flush
                    || self.closing))
    }

    fn enqueue(&mut self, hash: &str, max_write: usize) -> bool {
        let Some(object) = self.objects.get(hash) else {
            return false;
        };
        if object.queued || !self.dispatchable(object, max_write) {
            return false;
        }
        if let Some(object) = self.objects.get_mut(hash) {
            object.queued = true;
        }
        self.ready.push_back(hash.to_owned());
        true
    }

    /// Appends `bytes` at `offset == next`, then whatever pending ranges
    /// became contiguous (overlapping ones are dropped).
    fn append(&mut self, hash: &str, tag: u64, bytes: Vec<u8>, events: &mut Vec<SinkEvent>) {
        let mut dropped = Vec::new();
        if let Some(object) = self.objects.get_mut(hash) {
            let mut piece = Some((tag, bytes));
            while let Some((tag, bytes)) = piece.take() {
                let len = bytes.len() as u64;
                object.next += len;
                object.last_tag = tag;
                if len > 0 {
                    object.buf_len += bytes.len();
                    object.buf.push_back((tag, bytes));
                }
                while let Some(entry) = object.pending.first_entry() {
                    let offset = *entry.key();
                    if offset > object.next {
                        break;
                    }
                    let (tag, bytes) = entry.remove();
                    if offset == object.next {
                        piece = Some((tag, bytes));
                        break;
                    }
                    dropped.push((tag, bytes.len() as u64));
                }
            }
        }
        for (tag, bytes) in dropped {
            self.release(tag, bytes, false, events);
        }
    }

    /// The next write call: ready objects of one lease, ≤ `max_write`
    /// bytes and ≤ `max_items` objects.
    fn take_batch(&mut self, options: &SinkOptions) -> Option<Batch> {
        let mut lease: Option<String> = None;
        let mut batch = Batch {
            lease: String::new(),
            items: Vec::new(),
            body: Vec::new(),
            tags: Vec::new(),
        };
        let mut later = Vec::new();
        while let Some(hash) = self.ready.pop_front() {
            let max_write = options.max_write;
            let Some(object) = self.objects.get(&hash) else {
                continue;
            };
            if !self.dispatchable(object, max_write) {
                if let Some(object) = self.objects.get_mut(&hash) {
                    object.queued = false;
                }
                continue;
            }
            if lease.as_ref().is_some_and(|lease| *lease != object.lease) {
                later.push(hash);
                continue;
            }
            let room = max_write.saturating_sub(batch.body.len());
            if room == 0 || (room < object.buf_len && !batch.items.is_empty()) {
                later.push(hash);
                break;
            }
            let Some(object) = self.objects.get_mut(&hash) else {
                continue;
            };
            object.queued = false;
            object.writing = true;
            object.flush = false;
            let take = object.buf_len.min(room);
            let offset = object.next - object.buf_len as u64;
            object.empty_due = false;
            // Whole pieces; only the last one taken may be split.
            let mut left = take;
            let mut tags = Vec::new();
            while left > 0 {
                let Some((tag, mut bytes)) = object.buf.pop_front() else {
                    break;
                };
                if bytes.len() > left {
                    let rest = bytes.split_off(left);
                    object.buf.push_front((tag, rest));
                }
                left -= bytes.len();
                object.buf_len -= bytes.len();
                tags.push((tag, bytes.len() as u64));
                if batch.body.is_empty() {
                    batch.body = bytes;
                } else {
                    batch.body.extend_from_slice(&bytes);
                }
            }
            if tags.is_empty() {
                tags.push((object.last_tag, 0));
            }
            batch.items.push(WriteItem {
                hash: hash.clone(),
                offset,
                total: object.total,
                len: take as u64,
                last: offset + take as u64 == object.total,
            });
            batch.tags.push(tags);
            lease.get_or_insert_with(|| object.lease.clone());
            if batch.items.len() >= options.max_items || batch.body.len() >= max_write {
                break;
            }
        }
        for hash in later.into_iter().rev() {
            self.ready.push_front(hash);
        }
        if batch.items.is_empty() {
            return None;
        }
        batch.lease = lease.unwrap_or_default();
        self.writing += 1;
        Some(batch)
    }

    /// Applies the outcome of a write call.
    fn apply(
        &mut self,
        batch: Batch,
        result: Result<Vec<WriteResult>, CallError>,
        options: &SinkOptions,
        events: &mut Vec<SinkEvent>,
    ) {
        self.writing = self.writing.saturating_sub(1);
        let call_error = result.as_ref().err().cloned();
        if let Some(CallError::Unavailable(_)) = &call_error {
            self.fatal = call_error.clone();
        }
        let results: HashMap<String, WriteResult> = result
            .unwrap_or_default()
            .into_iter()
            .map(|result| (result.hash.clone(), result))
            .collect();
        for (item, tags) in batch.items.iter().zip(batch.tags) {
            let hash = &item.hash;
            if let Some(object) = self.objects.get_mut(hash) {
                object.writing = false;
            }
            let end = item.offset + item.len;
            let (written, outcome, call) = match (&call_error, results.get(hash)) {
                (Some(error), _) => (
                    false,
                    Some(ObjectOutcome::Failed(error.to_wire())),
                    Some(error.clone()),
                ),
                (None, None) => {
                    let error = ErrorWire::coded(
                        "INVALID_REPLY",
                        format!("objects.write returned no result for {hash}"),
                    );
                    (
                        false,
                        Some(ObjectOutcome::Failed(error.clone())),
                        Some(CallError::Remote(error)),
                    )
                }
                (None, Some(result)) => match &result.outcome {
                    ObjectOutcome::Completed => (true, Some(ObjectOutcome::Completed), None),
                    ObjectOutcome::Progressed { durable_offset } if *durable_offset == end => {
                        (true, None, None)
                    }
                    // The worker kept less (or more) than this call wrote:
                    // its durable offset is the truth; resynchronize.
                    ObjectOutcome::Progressed { durable_offset } => (
                        false,
                        Some(ObjectOutcome::Progressed {
                            durable_offset: *durable_offset,
                        }),
                        None,
                    ),
                    // A resume point, not a failure of the object (WP3.2's
                    // contract, CCR-WP3.2-5; WP3.INT): the write did not
                    // start at the worker's durable offset, or the bytes
                    // before it changed and the spool was reset. Resynchronize
                    // from the offset the worker answered, as above: no
                    // per-object backoff, no `ObjectError`.
                    ObjectOutcome::Failed(error) if is_resume_point(&error.code) => (
                        false,
                        Some(ObjectOutcome::Progressed {
                            durable_offset: result.durable_offset,
                        }),
                        None,
                    ),
                    other => (false, Some(other.clone()), None),
                },
            };
            if let Some(object) = self.objects.get_mut(hash) {
                if let Some(result) = results.get(hash) {
                    object.durable = match &result.outcome {
                        ObjectOutcome::Completed => object.total,
                        _ => result.durable_offset,
                    };
                }
                if written {
                    object.written += item.len;
                }
            }
            for (tag, bytes) in tags {
                self.release(tag, bytes, written, events);
            }
            match outcome {
                Some(outcome) => self.settle(hash, outcome, call, events),
                None => {
                    self.enqueue(hash, options.max_write);
                }
            }
        }
        if let Some(fatal) = self.fatal.clone() {
            let open: Vec<String> = self
                .objects
                .iter()
                .filter(|(_, object)| object.settled.is_none() && !object.writing)
                .map(|(hash, _)| hash.clone())
                .collect();
            for hash in open {
                self.settle(
                    &hash,
                    ObjectOutcome::Failed(fatal.to_wire()),
                    Some(fatal.clone()),
                    events,
                );
            }
        }
    }
}

impl Sink {
    pub(crate) fn start(
        client: Arc<SessionWorkerClient>,
        route: SyncRoute,
        options: SinkOptions,
        hook: SinkHook,
    ) -> Sink {
        let inner = Arc::new(SinkInner {
            client,
            route,
            options,
            state: Mutex::new(SinkState::default()),
            changed: Condvar::new(),
            hook,
        });
        let writers = (0..options.writers.max(1))
            .filter_map(|index| {
                let inner = inner.clone();
                thread::Builder::new()
                    .name(format!("objects-write-{index}"))
                    .spawn(move || write_loop(&inner))
                    .ok()
            })
            .collect();
        Sink { inner, writers }
    }

    fn with_state<T>(&self, f: impl FnOnce(&mut SinkState, &mut Vec<SinkEvent>) -> T) -> T {
        let mut events = Vec::new();
        let value = {
            let mut state = lock(&self.inner.state);
            f(&mut state, &mut events)
        };
        self.inner.changed.notify_all();
        if !events.is_empty() {
            (self.inner.hook)(events);
        }
        value
    }

    /// Declares that `hash` (`total` bytes) will arrive from `offset`, under
    /// `lease`. An object already declared keeps its state (a later pull
    /// continues it).
    pub(crate) fn expect(&self, hash: &str, lease: &str, total: u64, offset: u64, tag: u64) {
        self.with_state(|state, _| {
            state
                .objects
                .entry(hash.to_owned())
                .or_insert_with(|| SinkObject {
                    lease: lease.to_owned(),
                    total,
                    next: offset,
                    limit: total,
                    durable: offset,
                    written: 0,
                    buf: VecDeque::new(),
                    buf_len: 0,
                    pending: BTreeMap::new(),
                    empty_due: false,
                    writing: false,
                    queued: false,
                    flush: false,
                    settled: None,
                    call: None,
                    last_tag: tag,
                });
        });
    }

    /// Offers one received part. `Err` (the part was dropped and released at
    /// once): the object is unknown (`Unsolicited`), settled, cut, the part
    /// overlaps data already received (`Skipped`), or it contradicts the
    /// object's size (the object settles `SizeMismatch`).
    pub(crate) fn offer(
        &self,
        hash: &str,
        offset: u64,
        total: u64,
        bytes: &[u8],
        tag: u64,
    ) -> Result<(), ObjectOutcome> {
        let len = bytes.len() as u64;
        let max_write = self.inner.options.max_write;
        self.with_state(|state, events| {
            let verdict = match state.objects.get(hash) {
                _ if state.fatal.is_some() => Err(ObjectOutcome::Failed(
                    state
                        .fatal
                        .as_ref()
                        .map(CallError::to_wire)
                        .unwrap_or_else(|| ErrorWire::coded("LOCAL_WORKER_UNAVAILABLE", hash)),
                )),
                None => Err(ObjectOutcome::Unsolicited),
                Some(object) if object.settled.is_some() => {
                    Err(object.settled.clone().unwrap_or(ObjectOutcome::Skipped))
                }
                Some(object) if total != object.total || offset.saturating_add(len) > total => {
                    Err(ObjectOutcome::SizeMismatch)
                }
                Some(object)
                    if offset.saturating_add(len) > object.limit || offset < object.next =>
                {
                    Err(ObjectOutcome::Skipped)
                }
                Some(object) if offset > object.next && object.pending.contains_key(&offset) => {
                    Err(ObjectOutcome::Skipped)
                }
                Some(_) => Ok(()),
            };
            if let Err(outcome) = &verdict {
                if len > 0 {
                    events.push(SinkEvent::Released {
                        tag,
                        bytes: len,
                        written: false,
                    });
                }
                if *outcome == ObjectOutcome::SizeMismatch {
                    state.settle(hash, ObjectOutcome::SizeMismatch, None, events);
                }
                return verdict;
            }
            state.buffered += len;
            let next = state.objects.get(hash).map_or(offset, |object| object.next);
            if offset == next {
                state.append(hash, tag, bytes.to_vec(), events);
                if let Some(object) = state.objects.get_mut(hash)
                    && object.total == 0
                    && object.written == 0
                {
                    object.empty_due = true;
                }
            } else if let Some(object) = state.objects.get_mut(hash) {
                object.pending.insert(offset, (tag, bytes.to_vec()));
            }
            state.enqueue(hash, max_write);
            Ok(())
        })
    }

    /// Nothing of `hash` at or after `at` will be accepted (a pull ended
    /// short there): later ranges already waiting are dropped, and what is
    /// contiguous is written.
    pub(crate) fn cut(&self, hash: &str, at: u64) {
        let max_write = self.inner.options.max_write;
        self.with_state(|state, events| {
            let mut dropped = Vec::new();
            if let Some(object) = state.objects.get_mut(hash) {
                object.limit = object.limit.min(at.max(object.next));
                let limit = object.limit;
                let tail: Vec<u64> = object.pending.range(limit..).map(|(k, _)| *k).collect();
                for offset in tail {
                    if let Some((tag, bytes)) = object.pending.remove(&offset) {
                        dropped.push((tag, bytes.len() as u64));
                    }
                }
                object.flush = true;
            }
            for (tag, bytes) in dropped {
                state.release(tag, bytes, false, events);
            }
            state.enqueue(hash, max_write);
        });
    }

    /// `hash`, declared with size 0 because its size was unknown
    /// ([`size_unknown`]), is `total` bytes: it takes that size if nothing of
    /// it was received or settled yet (returns whether it did).
    pub(crate) fn resize(&self, hash: &str, total: u64) -> bool {
        self.with_state(|state, _| match state.objects.get_mut(hash) {
            Some(object)
                if object.total == 0
                    && object.next == 0
                    && object.written == 0
                    && object.settled.is_none()
                    && !object.writing
                    && !object.empty_due =>
            {
                object.total = total;
                object.limit = total;
                true
            }
            _ => false,
        })
    }

    /// Whether `hash` reached its final state in this sink (nothing more of
    /// it is accepted).
    pub(crate) fn settled(&self, hash: &str) -> bool {
        lock(&self.inner.state)
            .objects
            .get(hash)
            .is_some_and(|object| object.settled.is_some())
    }

    /// Writes the partial data of `hashes` without waiting for a full call.
    pub(crate) fn flush(&self, hashes: &[String]) {
        let max_write = self.inner.options.max_write;
        self.with_state(|state, _| {
            for hash in hashes {
                if let Some(object) = state.objects.get_mut(hash) {
                    object.flush = true;
                }
                state.enqueue(hash, max_write);
            }
        });
    }

    /// Waits until fewer than `limit` bytes wait to be written that the
    /// writers will take without more input, or `stop` holds. Only those
    /// count: ranges ahead of a gap, and a partial tail that is not yet a
    /// whole write (its object neither ends there nor is flushed), drain only
    /// when the sender sends more, so waiting for them could never end. (The
    /// caller bounds what it offers: a push's shares, a pull's budget.)
    pub(crate) fn wait_below(&self, limit: u64, stop: &dyn Fn() -> bool) {
        let mut state = lock(&self.inner.state);
        let max_write = self.inner.options.max_write;
        let held = |state: &SinkState| -> u64 {
            state
                .objects
                .values()
                .filter(|object| object.writing || state.dispatchable(object, max_write))
                .map(|object| object.buf_len as u64)
                .sum()
        };
        while held(&state) >= limit && state.fatal.is_none() && !stop() {
            state = self
                .inner
                .changed
                .wait_timeout(state, EVENT_WAIT)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    /// The local worker cannot be reached: the sink writes nothing more.
    pub(crate) fn fatal(&self) -> Option<CallError> {
        lock(&self.inner.state).fatal.clone()
    }

    /// Ends the sink: writes what is held (`flush`) or drops it, waits for
    /// the calls in flight, releases whatever is left, and returns every
    /// object's final state.
    pub(crate) fn finish(mut self, flush: bool) -> HashMap<String, SinkResult> {
        let max_write = self.inner.options.max_write;
        self.with_state(|state, events| {
            state.closing = true;
            let hashes: Vec<String> = state.objects.keys().cloned().collect();
            for hash in hashes {
                if flush {
                    if let Some(object) = state.objects.get_mut(&hash) {
                        object.flush = true;
                    }
                    state.enqueue(&hash, max_write);
                } else if state
                    .objects
                    .get(&hash)
                    .is_some_and(|object| !object.writing)
                {
                    state.drop_data(&hash, events);
                }
            }
        });
        for writer in std::mem::take(&mut self.writers) {
            let _ = writer.join();
        }
        self.with_state(|state, events| {
            let hashes: Vec<String> = state.objects.keys().cloned().collect();
            for hash in &hashes {
                state.drop_data(hash, events);
            }
            state
                .objects
                .iter()
                .map(|(hash, object)| {
                    (
                        hash.clone(),
                        SinkResult {
                            outcome: object.settled.clone(),
                            durable: object.durable,
                            written: object.written,
                            call: object.call.clone(),
                        },
                    )
                })
                .collect()
        })
    }
}

impl Drop for Sink {
    fn drop(&mut self) {
        // Not finished (a panic): the writers stop after their current call.
        if !self.writers.is_empty() {
            lock(&self.inner.state).closing = true;
            self.inner.changed.notify_all();
        }
    }
}

/// One writer of a [`Sink`]: one `objects.write` call at a time.
fn write_loop(inner: &SinkInner) {
    loop {
        let batch = {
            let mut state = lock(&inner.state);
            loop {
                if let Some(batch) = state.take_batch(&inner.options) {
                    break Some(batch);
                }
                if state.closing && state.ready.is_empty() {
                    break None;
                }
                state = inner
                    .changed
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        };
        let Some(mut batch) = batch else {
            inner.changed.notify_all();
            return;
        };
        let items = std::mem::take(&mut batch.items);
        let body = std::mem::take(&mut batch.body);
        // A write that completes an object includes its whole-object hash.
        // Totals may be a peer's (a push): saturating, never an overflow
        // (`with_verify` caps the time anyway; WP3.INT, WP3.3 review minor).
        let verified: u64 = items
            .iter()
            .filter(|item| item.last)
            .fold(0u64, |sum, item| sum.saturating_add(item.total));
        let result = inner.client.write_batch(
            &inner.route,
            &batch.lease,
            items.clone(),
            body,
            worker_client::deadlines::with_verify(inner.options.deadline, verified),
        );
        batch.items = items;
        let mut events = Vec::new();
        {
            let mut state = lock(&inner.state);
            state.apply(batch, result, &inner.options, &mut events);
        }
        inner.changed.notify_all();
        if !events.is_empty() {
            (inner.hook)(events);
        }
    }
}

// ---------------------------------------------------------------------------
// The v3 lane

/// One bulk lane: objects v3 with the interim fallback (module docs). Runs
/// until `cancel` fires or the session closes or goes away.
pub fn run_bulk_lane(
    ctx: Arc<Ctx>,
    peer: Arc<PeerContext>,
    lane: LaneSpec,
    conn: session::Conn,
    cancel: Cancel,
) {
    let options = LaneOptions::for_config(&ctx.config);
    run_lane_with(ctx, peer, lane, conn, cancel, options);
}

/// [`run_bulk_lane`] with explicit options.
pub fn run_lane_with(
    ctx: Arc<Ctx>,
    peer: Arc<PeerContext>,
    lane: LaneSpec,
    conn: session::Conn,
    cancel: Cancel,
    options: LaneOptions,
) {
    let status = status_lane(lane);
    let name = format!("bulk-{}-{}", short(&peer.installation), status);
    if let Err(error) = lane_check(&peer, &conn, lane) {
        refuse(
            &ctx,
            &peer,
            &conn,
            &cancel,
            status,
            error,
            &options.download,
        );
        return;
    }
    // Either direction's end (cancel, session gone) ends the other.
    let token = cancel.child();
    let stats = options.stats.clone().unwrap_or_default();
    static RUNS: AtomicU64 = AtomicU64::new(0);
    let run = Arc::new(LaneRun {
        ctx: ctx.clone(),
        peer: peer.clone(),
        conn: conn.clone(),
        spec: lane,
        status,
        window: Arc::new(Window::new(options.window_bytes, Some(stats.clone()))),
        slots: Slots::new(options.peer_calls, Some(stats.clone())),
        // Pushes leave at least one peer slot to pulls and reports (WP3.INT
        // re-review): a push held by the peer's stalled worker keeps its slot
        // until the peer's write deadline.
        pushes: Slots::new(options.peer_calls.saturating_sub(1).max(1), None),
        repos: RepoGate::new(options.repos, Some(stats.clone())),
        stats,
        cancel: token.clone(),
        outer: cancel.clone(),
        modes: Mutex::new(HashMap::new()),
        interim: InterimState::default(),
        turns: Mutex::new(HashMap::new()),
        remote_content: Mutex::new(HashMap::new()),
        claimant: format!(
            "{}#{}",
            pull_holder(&peer.installation, lane),
            RUNS.fetch_add(1, Ordering::Relaxed)
        ),
        options: options.clone(),
    });
    let up = Arc::new(UploadJob(run.clone()));
    let uploads = {
        let (ctx, peer, conn, token, options, name) = (
            ctx.clone(),
            peer.clone(),
            conn.clone(),
            token.clone(),
            options.upload.clone(),
            format!("{name}-up"),
        );
        thread::Builder::new().name(name.clone()).spawn(move || {
            drive(&ctx, &peer, &conn, &token, up, &options, status, &name);
            token.cancel();
        })
    };
    let down = Arc::new(DownloadJob(run));
    drive(
        &ctx,
        &peer,
        &conn,
        &token,
        down,
        &options.download,
        status,
        &name,
    );
    token.cancel();
    if let Ok(handle) = uploads {
        let _ = handle.join();
    }
}

fn lane_check(peer: &PeerContext, conn: &session::Conn, lane: LaneSpec) -> Result<(), ErrorWire> {
    if lane.is_valid() {
        check_session(peer, conn, LaneRole::Bulk, Some(lane))
    } else {
        Err(ErrorWire::coded(
            "INVALID_BULK_LANE",
            format!(
                "lane {}/{} is not a valid bulk lane",
                lane.index, lane.count
            ),
        ))
    }
}

/// What both directions of a lane share.
struct LaneRun {
    ctx: Arc<Ctx>,
    peer: Arc<PeerContext>,
    conn: session::Conn,
    spec: LaneSpec,
    status: u32,
    window: Arc<Window>,
    slots: Arc<Slots>,
    /// Pushes at once: one fewer than `slots` (when there are two or more),
    /// so a stalled push never takes the last slot from pulls and reports.
    pushes: Arc<Slots>,
    repos: RepoGate,
    stats: Arc<LaneStats>,
    /// Ends both directions (the lane's cancel, the session's end).
    cancel: Cancel,
    /// The lane's own cancel (the lane is told to stop).
    outer: Cancel,
    /// Repositories on the interim path, until when.
    modes: Mutex<HashMap<String, Instant>>,
    interim: InterimState,
    /// The pull lease ids, per repository (each repository is one worker,
    /// with its own leases).
    turns: Mutex<HashMap<String, Turns>>,
    /// Per repository: the peer's published content counters (shared and
    /// current objects available) as the last download round saw them.
    remote_content: Mutex<HashMap<String, (u64, u64)>>,
    /// This lane run's name in the handoff table (module docs, "Lease
    /// handoff"): the lane's holder name and a number of its own, so a run
    /// gives back only its own claims.
    claimant: String,
    options: LaneOptions,
}

impl Drop for LaneRun {
    /// Both directions ended: whatever this run still claims in the handoff
    /// table goes back to the table, for another source (or its next run).
    fn drop(&mut self) {
        handoff_unclaim_all(None, &self.claimant);
    }
}

/// The two pull lease ids of one repository's wants (module docs, step 1).
#[derive(Default)]
struct Turns {
    /// The id of the next want made while a pull is in flight.
    next: usize,
    /// Per id: until when it may hold leases at our worker (`None`: not
    /// known, as in a new lane run, after a reconnect).
    holds: [Option<Instant>; 2],
}

impl Turns {
    fn may_hold(&self, slot: usize, now: Instant) -> bool {
        self.holds[slot].is_none_or(|until| now < until)
    }

    /// The ids to ask, in order. While a pull is in flight, one: the id the
    /// last want did not use, so the pull's own objects are not returned
    /// again. Otherwise both (what a round resumes may be leased to either),
    /// except one known to hold nothing; the one that may hold first.
    fn order(&self, idle: bool, now: Instant) -> Vec<usize> {
        let (first, second) = (self.next % 2, 1 - self.next % 2);
        if !idle {
            return vec![first];
        }
        let (first, second) = if !self.may_hold(first, now) && self.may_hold(second, now) {
            (second, first)
        } else {
            (first, second)
        };
        if self.may_hold(second, now) {
            vec![first, second]
        } else {
            vec![first]
        }
    }

    /// A want with `slot` returned `items` objects or none.
    fn asked(&mut self, slot: usize, items: bool, now: Instant) {
        self.next = 1 - slot;
        // A want leases (and renews) what it returns; one that returns
        // nothing shows the id holds nothing.
        self.holds[slot] = Some(if items { now + LEASE_TTL } else { now });
    }

    /// An object leased to `slot` is unfinished: the id may hold it.
    fn holding(&mut self, slot: usize, now: Instant) {
        let until = now + LEASE_TTL;
        self.holds[slot] = Some(self.holds[slot].map_or(until, |held| held.max(until)));
    }
}

/// How a v3 round ended without an error.
enum RoundEnd {
    Done(Next),
    /// Objects v3 is not served (here or at the peer): take the interim path.
    Interim,
}

impl LaneRun {
    fn stopped(&self) -> bool {
        self.cancel.is_cancelled()
            || self.conn.closed().is_some()
            || self.ctx.stopping.load(Ordering::Relaxed)
    }

    /// The lane (or the daemon) is told to stop: what arrived is dropped
    /// rather than written. A session that merely ended is not that.
    fn cancelled(&self) -> bool {
        self.outer.is_cancelled() || self.ctx.stopping.load(Ordering::Relaxed)
    }

    /// The lease ids (`slot`s of [`pull_lease`]) `repo`'s next want asks
    /// with, in order ([`Turns::order`]).
    fn want_order(&self, repo: &str, idle: bool) -> Vec<usize> {
        lock(&self.turns)
            .entry(repo.to_owned())
            .or_default()
            .order(idle, Instant::now())
    }

    /// A want of `repo` with `slot` returned `items` objects or none.
    fn asked(&self, repo: &str, slot: usize, items: bool) {
        lock(&self.turns)
            .entry(repo.to_owned())
            .or_default()
            .asked(slot, items, Instant::now());
    }

    /// An object of `repo` leased to `slot` is unfinished.
    fn holding(&self, repo: &str, slot: usize) {
        lock(&self.turns)
            .entry(repo.to_owned())
            .or_default()
            .holding(slot, Instant::now());
    }

    fn lease(&self, slot: usize) -> String {
        pull_lease(&self.peer.installation, self.spec, slot as u64)
    }

    fn stop_error(&self) -> ExchangeError {
        let detail = match self.conn.closed() {
            Some(error) => format!("WS_CLOSED: the bulk session closed: {error}"),
            None => "WS_CLOSED: the bulk lane is stopping".to_owned(),
        };
        ExchangeError::peer(CallError::Unavailable(detail))
    }

    fn on_interim(&self, repo: &str) -> bool {
        let mut modes = lock(&self.modes);
        match modes.get(repo) {
            Some(until) if Instant::now() < *until => true,
            Some(_) => {
                modes.remove(repo);
                false
            }
            None => false,
        }
    }

    fn set_interim(&self, repo: &str, why: &CallError) {
        let first = lock(&self.modes)
            .insert(repo.to_owned(), Instant::now() + INTERIM_RECHECK)
            .is_none();
        if first {
            event!(info, UNSUPPORTED_OPERATION, {
                "peer": self.peer.installation,
                "repo": repo,
                "lane": self.status,
                "detail": format!("objects v3 is not served; using the interim path: {why}"),
            });
        }
    }

    fn forget(&self, repo: &str) {
        lock(&self.modes).remove(repo);
        self.interim.forget(repo);
    }

    fn route(&self, target: &Target) -> SyncRoute {
        SyncRoute {
            peer: self.peer.installation.clone(),
            repo: target.folder.record.repo.clone(),
            local_replica: target.folder.record.device.clone(),
            remote_replica: target.remote.replica.clone(),
        }
    }

    /// The local worker's loopback session client; `None` when it serves no
    /// objects v3 (the interim path).
    fn local(&self, target: &Target) -> Result<Option<Arc<SessionWorkerClient>>, ExchangeError> {
        let Some(endpoint) = target.folder.worker.as_ref() else {
            return Err(ExchangeError::local(CallError::Unavailable(format!(
                "LOCAL_WORKER_UNAVAILABLE: folder {} has no running worker",
                target.folder.record.state
            ))));
        };
        match worker_client::session_for(endpoint) {
            Ok(client) => Ok(Some(client)),
            Err(error) if is_unsupported(&error) => {
                self.set_interim(&target.repo, &error);
                Ok(None)
            }
            Err(error) => Err(ExchangeError::local(error)),
        }
    }

    fn started(&self, repo: &str, op: &str) {
        self.ctx.status.record(StatusEvent::ExchangeStarted {
            installation: self.peer.installation.clone(),
            repo: repo.to_owned(),
            lane: self.status,
            op: op.to_owned(),
            at_ms: self.ctx.clock.now_ms(),
            direction: Direction::Outbound,
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn finished(
        &self,
        repo: &str,
        op: &str,
        error: Option<ErrorWire>,
        bytes_up: u64,
        bytes_down: u64,
        objects_up: u64,
        objects_down: u64,
        started: Instant,
    ) {
        self.ctx.status.record(StatusEvent::ExchangeFinished {
            installation: self.peer.installation.clone(),
            repo: repo.to_owned(),
            lane: self.status,
            op: op.to_owned(),
            direction: Direction::Outbound,
            ok: error.is_none(),
            error,
            summary: None,
            bytes_up,
            bytes_down,
            objects_up,
            objects_down,
            duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        });
    }

    fn key(&self, repo: &str, up: bool, hash: &str) -> ObjectKey {
        object_key(
            &self.ctx.installation,
            &self.peer.installation,
            repo,
            up,
            hash,
        )
    }

    /// A per-object failure: backoff for this source, `ObjectError`.
    fn object_failed(&self, target: &Target, up: bool, hash: &str, outcome: &ObjectOutcome) {
        if !is_failure(outcome) {
            return;
        }
        let detail = if up {
            format!("pushing to peer {}", self.peer.installation)
        } else {
            format!("pulling from peer {}", self.peer.installation)
        };
        let Some(mut error) = outcome_error(hash, outcome, &detail) else {
            return;
        };
        let delay = object_failed(&self.key(&target.repo, up, hash), &error.code);
        // The record's next retry (§7.1) is this object's backoff here.
        error.retry_after_ms = Some(u64::try_from(delay.as_millis()).unwrap_or(u64::MAX));
        event!(debug, (error.code.as_str()), {
            "peer": self.peer.installation,
            "repo": target.repo,
            "object": hash,
            "retry_ms": u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
            "message": error.message,
        });
        self.ctx.status.record(StatusEvent::ObjectError {
            state: target.folder.record.state.clone(),
            hash: hash.to_owned(),
            error,
            source: Some(ObjectSource {
                installation: self.peer.installation.clone(),
                lane: self.status,
                direction: Direction::Outbound,
            }),
        });
    }

    /// The peer's published content grew since the last download round of
    /// `target` (it holds objects it lacked: a relay that got them, a copy
    /// repaired): what it could not serve us is tried again at once instead of
    /// after its backoff, up to an hour (WP3.INT re-review).
    fn remote_content_grew(&self, target: &Target) {
        // No counters published (yet) counts as nothing available.
        let now = target.remote.content.map_or((0, 0), |content| {
            (content.shared_available, content.current_available)
        });
        let before = lock(&self.remote_content).insert(target.repo.clone(), now);
        if before.is_some_and(|(shared, current)| now.0 > shared || now.1 > current) {
            object_backoffs_reset(
                &self.ctx.installation,
                &self.peer.installation,
                &target.repo,
                false,
            );
        }
    }

    /// An object completed: clears its backoff and resolves its errors. One
    /// installed here (`up` false) resolves those of every source of it (a
    /// source that could not serve it may have handed it to this one).
    fn object_done(&self, target: &Target, up: bool, hash: &str) {
        let key = self.key(&target.repo, up, hash);
        let codes = if up {
            object_completed(&key).into_iter().collect()
        } else {
            object_installed(&key)
        };
        for code in codes {
            self.ctx.status.record(StatusEvent::Resolved {
                scope: Scope::Object {
                    state: target.folder.record.state.clone(),
                    hash: hash.to_owned(),
                },
                code,
            });
        }
    }

    /// The interim exchange of `target` (both directions).
    fn interim_exchange(
        &self,
        target: &Target,
        retry_after: &mut dyn FnMut(&ExchangeError) -> Option<Duration>,
    ) -> Result<Next, ExchangeError> {
        bump(&self.stats.interim_exchanges, 1);
        self.interim.run(
            &self.ctx,
            &self.conn,
            &self.peer,
            self.spec,
            &self.options.interim,
            target,
            retry_after,
        )
    }
}

/// Signals both directions react to (module docs).
fn objects_triggered(before: &Signals, after: &Signals, absorbing: bool) -> bool {
    before.tip != after.tip
        || before.remote_tip != after.remote_tip
        || (before.remote_content != after.remote_content && !absorbing)
        || (before.content != after.content && !absorbing)
}

/// The download direction of a lane.
struct DownloadJob(Arc<LaneRun>);

impl Job for DownloadJob {
    fn op(&self) -> &str {
        ops::OBJECTS_PULL
    }

    fn scope(&self) -> String {
        format!("{}#{}", ops::OBJECTS_PULL, self.0.spec.index)
    }

    fn class(&self) -> &'static str {
        "objects"
    }

    fn triggered(&self, before: &Signals, after: &Signals, absorbing: bool) -> bool {
        // Staging moves `sync`: objects of staged events are wanted at once.
        objects_triggered(before, after, absorbing) || before.sync != after.sync
    }

    fn run(
        &self,
        target: &Target,
        retry_after: &mut dyn FnMut(&ExchangeError) -> Option<Duration>,
    ) -> Result<Next, ExchangeError> {
        let lane = &*self.0;
        let stop = || lane.stopped();
        let Some(_turn) = lane.repos.enter(&target.repo, &stop) else {
            return Err(lane.stop_error());
        };
        if lane.on_interim(&target.repo) {
            return lane.interim_exchange(target, retry_after);
        }
        let Some(client) = lane.local(target)? else {
            return lane.interim_exchange(target, retry_after);
        };
        lane.remote_content_grew(target);
        bump(&lane.stats.v3_rounds, 1);
        match download_round(lane, target, &client)? {
            RoundEnd::Done(next) => Ok(next),
            RoundEnd::Interim => lane.interim_exchange(target, retry_after),
        }
    }

    fn forget(&self, repo: &str) {
        self.0.forget(repo);
    }
}

/// The upload direction of a lane.
struct UploadJob(Arc<LaneRun>);

impl Job for UploadJob {
    fn op(&self) -> &str {
        ops::OBJECTS_PUSH
    }

    fn scope(&self) -> String {
        format!("{}#{}", ops::OBJECTS_PUSH, self.0.spec.index)
    }

    fn class(&self) -> &'static str {
        "objects"
    }

    fn triggered(&self, before: &Signals, after: &Signals, absorbing: bool) -> bool {
        objects_triggered(before, after, absorbing) || (before.sync != after.sync && !absorbing)
    }

    fn run(
        &self,
        target: &Target,
        _retry_after: &mut dyn FnMut(&ExchangeError) -> Option<Duration>,
    ) -> Result<Next, ExchangeError> {
        let lane = &*self.0;
        // On the interim path the download scheduler moves both directions.
        let idle = Next {
            after: None,
            progress: false,
        };
        if lane.on_interim(&target.repo) {
            return Ok(idle);
        }
        let stop = || lane.stopped();
        let Some(_turn) = lane.repos.enter(&target.repo, &stop) else {
            return Err(lane.stop_error());
        };
        let Some(client) = lane.local(target)? else {
            return Ok(idle);
        };
        match upload_round(lane, target, &client)? {
            RoundEnd::Done(next) => Ok(next),
            RoundEnd::Interim => Ok(idle),
        }
    }

    fn forget(&self, repo: &str) {
        self.0.forget(repo);
    }
}

// ---------------------------------------------------------------------------
// Download round

/// One wanted object, planned for a pull.
#[derive(Clone, Debug)]
struct Planned {
    item: WantItem,
    lease: String,
    /// The lane's lease id it is under (`None`: taken from the handoff
    /// table, `lease` is another source's).
    slot: Option<usize>,
}

impl Planned {
    fn adopted(&self) -> bool {
        self.slot.is_none()
    }
}

/// One pull of a round.
struct Pull {
    started: Instant,
    /// `(hash, start, end)` this pull covers.
    spans: Vec<(String, u64, u64)>,
    budget: u64,
    /// Bytes received so far, counted as they stream (the next pull waits
    /// for half the budget: "re-pull at half").
    streamed: Arc<AtomicU64>,
    received: u64,
    released: u64,
    completed: u64,
    ended: Option<Result<ObjectsResult, CallError>>,
    reported: bool,
}

enum RoundEvent {
    /// What one sink step settled, together (a write's releases and the
    /// objects it completed arrive at once, so a pull is never reported
    /// between them).
    Sink(Vec<SinkEvent>),
    PullEnded {
        tag: u64,
        result: Result<ObjectsResult, CallError>,
        received: u64,
        ends: HashMap<String, u64>,
        /// Objects whose size we did not know, with the total their
        /// sender's empty chunk gave ([`size_unknown`]).
        sized: HashMap<String, u64>,
    },
}

/// What a download round has seen so far.
#[derive(Default)]
struct RoundState {
    plan: VecDeque<Planned>,
    begun: HashSet<String>,
    /// Every object planned in this round, as first planned (offset: where
    /// the worker said it stood), with its lease and whether it was taken
    /// from the handoff table (module docs, "Lease handoff").
    planned: HashMap<String, Planned>,
    /// Objects that failed for this source in this round (now in backoff).
    failed: HashSet<String>,
    pulls: BTreeMap<u64, Pull>,
    next_tag: u64,
    want_dry: bool,
    written: u64,
    completed: u64,
    corrupt: Vec<String>,
    leftovers: bool,
    interim: Option<CallError>,
    session_lost: Option<CallError>,
    peer_error: Option<CallError>,
    local_error: Option<CallError>,
}

impl RoundState {
    fn plan(&mut self, planned: Planned) {
        self.planned
            .entry(planned.item.hash.clone())
            .or_insert_with(|| planned.clone());
        self.plan.push_back(planned);
    }

    fn planned_or_begun(&self, hash: &str) -> bool {
        self.begun.contains(hash) || self.plan.iter().any(|planned| planned.item.hash == hash)
    }

    fn in_flight(&self) -> usize {
        self.pulls
            .values()
            .filter(|pull| pull.ended.is_none())
            .count()
    }

    fn plan_bytes(&self) -> u64 {
        self.plan
            .iter()
            .map(|planned| planned.item.size.saturating_sub(planned.item.offset))
            .sum()
    }

    fn failed(&self) -> bool {
        self.interim.is_some()
            || self.session_lost.is_some()
            || self.peer_error.is_some()
            || self.local_error.is_some()
    }
}

fn download_round(
    lane: &LaneRun,
    target: &Target,
    client: &Arc<SessionWorkerClient>,
) -> Result<RoundEnd, ExchangeError> {
    let route = lane.route(target);
    let (tx, rx) = mpsc::channel::<RoundEvent>();
    // Started with the round's first pull: a round that finds nothing to
    // pull (most anti-entropy rounds) starts no writer threads.
    let sink = LazySink {
        cell: OnceLock::new(),
        make: Box::new({
            let (tx, route, client) = (Mutex::new(tx.clone()), route.clone(), client.clone());
            // Credit comes back on the writer threads: the round's own
            // thread may be waiting for it (the next first pull).
            let window = lane.window.clone();
            let writers = lane.options.writes;
            move || {
                let (tx, window) = (lock(&tx).clone(), window.clone());
                Sink::start(
                    client.clone(),
                    route.clone(),
                    SinkOptions {
                        writers,
                        ..SinkOptions::default()
                    },
                    Box::new(move |events: Vec<SinkEvent>| {
                        let released: u64 = events
                            .iter()
                            .map(|event| match event {
                                SinkEvent::Released { bytes, .. } => *bytes,
                                SinkEvent::Settled { .. } => 0,
                            })
                            .sum();
                        window.release(released);
                        let _ = tx.send(RoundEvent::Sink(events));
                    }),
                )
            }
        }),
    };
    let abort = AtomicBool::new(false);
    let started = Instant::now();
    let mut state = RoundState::default();
    // What this round takes over from the handoff table stays claimed while
    // it works (module docs, "Lease handoff").
    let renewal = Renewal::new(&client.endpoint().instance, &lane.claimant);
    thread::scope(|scope| {
        loop {
            while let Ok(event) = rx.try_recv() {
                round_event(lane, target, &sink, &mut state, event);
            }
            report_pulls(lane, target, &mut state);
            renewal.tick(false);
            if lane.stopped() {
                break;
            }
            if let Some(error) = sink.cell.get().and_then(Sink::fatal) {
                state.local_error.get_or_insert(error);
            }
            if state.failed() {
                // The round fails: pulls in flight stop at once rather than
                // stream into a round whose bytes may be dropped.
                abort.store(true, Ordering::Relaxed);
                if state.in_flight() == 0 {
                    break;
                }
            } else {
                let over = started.elapsed() >= lane.options.round;
                let in_flight = state.in_flight();
                if !over
                    && state.plan.is_empty()
                    && !state.want_dry
                    && in_flight < lane.options.pulls.max(1)
                {
                    want_more(lane, target, client, &route, &renewal, &mut state);
                    continue;
                }
                if !over && !state.plan.is_empty() && in_flight < lane.options.pulls.max(1) {
                    let issued = issue_pull(
                        lane, target, &route, &sink, &tx, &abort, &renewal, &mut state, scope,
                    );
                    if issued {
                        continue;
                    }
                    if in_flight == 0 {
                        // Stopped while waiting for the window or a slot.
                        break;
                    }
                }
                if in_flight == 0 && (over || (state.plan.is_empty() && state.want_dry)) {
                    break;
                }
            }
            if let Ok(event) = rx.recv_timeout(EVENT_WAIT) {
                round_event(lane, target, &sink, &mut state, event);
            }
        }
        // Wind down: no new pulls; pulls in flight end (at once when aborted).
        if lane.stopped() || state.failed() {
            abort.store(true, Ordering::Relaxed);
        }
        while state.in_flight() > 0 {
            renewal.tick(false);
            if let Ok(event) = rx.recv_timeout(EVENT_WAIT) {
                round_event(lane, target, &sink, &mut state, event);
            }
        }
    });
    // The pulls are over: write what arrived (valid and contiguous: also
    // when the session ended, so the next one resumes after it; not when
    // the lane is told to stop), then account every byte.
    renewal.tick(true);
    let results = sink
        .cell
        .into_inner()
        .map(|sink| sink.finish(!lane.cancelled()))
        .unwrap_or_default();
    drop(tx);
    while let Ok(event) = rx.try_recv() {
        round_event(lane, target, &SinkGone, &mut state, event);
    }
    if results.values().any(|result| result.outcome.is_none()) {
        state.leftovers = true;
    }
    let failed = state.failed() || lane.stopped();
    settle_handoffs(lane, target, client, &state, &results, failed);
    for pull in state.pulls.values_mut() {
        pull.released = pull.released.max(pull.received);
    }
    report_pulls(lane, target, &mut state);
    send_reports(lane, target, &route, &mut state);

    let moved = state.written > 0 || state.completed > 0;
    let progress = |after: Option<Duration>| {
        Ok(RoundEnd::Done(Next {
            after,
            progress: moved,
        }))
    };
    if let Some(error) = state.interim.take() {
        lane.set_interim(&target.repo, &error);
        return Ok(RoundEnd::Interim);
    }
    if let Some(error) = state.session_lost.take() {
        return Err(ExchangeError::peer(error));
    }
    if lane.stopped() {
        return Err(lane.stop_error());
    }
    if let Some(error) = state.local_error.take()
        && (!moved || matches!(error, CallError::Unavailable(_)))
    {
        return Err(ExchangeError::local(error));
    }
    if let Some(error) = state.peer_error.take()
        && !moved
    {
        return Err(ExchangeError::peer(error));
    }
    let over = started.elapsed() >= lane.options.round;
    let again = moved && (over || state.leftovers || !state.want_dry);
    progress(again.then_some(CONTENT_YIELD))
}

/// The sink of a download round, started on first use.
struct LazySink {
    cell: OnceLock<Sink>,
    make: Box<dyn Fn() -> Sink + Send + Sync>,
}

impl LazySink {
    fn get(&self) -> &Sink {
        self.cell.get_or_init(|| (self.make)())
    }
}

/// Stand-in sink for events drained after the round's sink finished.
struct SinkGone;

/// What `round_event` needs of a sink.
trait SinkOps {
    fn cut(&self, hash: &str, at: u64);
    fn flush(&self, hashes: &[String]);
    fn resize(&self, hash: &str, total: u64) -> bool;
}

impl SinkOps for Sink {
    fn cut(&self, hash: &str, at: u64) {
        Sink::cut(self, hash, at);
    }
    fn flush(&self, hashes: &[String]) {
        Sink::flush(self, hashes);
    }
    fn resize(&self, hash: &str, total: u64) -> bool {
        Sink::resize(self, hash, total)
    }
}

impl SinkOps for LazySink {
    fn cut(&self, hash: &str, at: u64) {
        if let Some(sink) = self.cell.get() {
            sink.cut(hash, at);
        }
    }
    fn flush(&self, hashes: &[String]) {
        if let Some(sink) = self.cell.get() {
            sink.flush(hashes);
        }
    }
    fn resize(&self, hash: &str, total: u64) -> bool {
        self.cell.get().is_some_and(|sink| sink.resize(hash, total))
    }
}

impl SinkOps for SinkGone {
    fn cut(&self, _: &str, _: u64) {}
    fn flush(&self, _: &[String]) {}
    fn resize(&self, _: &str, _: u64) -> bool {
        false
    }
}

fn round_event(
    lane: &LaneRun,
    target: &Target,
    sink: &dyn SinkOps,
    state: &mut RoundState,
    event: RoundEvent,
) {
    match event {
        RoundEvent::Sink(events) => {
            for event in events {
                sink_event(lane, target, state, event);
            }
        }
        RoundEvent::PullEnded {
            tag,
            result,
            received,
            ends,
            sized,
        } => {
            state.want_dry = false;
            // Objects whose size we just learned are pulled again, whole,
            // under the lease they were wanted under.
            for (hash, total) in sized {
                let Some(planned) = state.planned.get(&hash).cloned() else {
                    continue;
                };
                if sink.resize(&hash, total) {
                    state.plan.push_front(Planned {
                        item: WantItem {
                            size: total,
                            offset: 0,
                            ..planned.item
                        },
                        ..planned
                    });
                }
            }
            let Some(pull) = state.pulls.get_mut(&tag) else {
                return;
            };
            pull.received = received;
            lane.window.release(pull.budget.saturating_sub(received));
            bump(&lane.stats.bytes_pulled, received);
            let mut hashes = Vec::new();
            let mut short_ended = Vec::new();
            for (hash, start, end) in &pull.spans {
                let got = ends.get(hash).copied().unwrap_or(*start);
                if got < *end {
                    short_ended.push((hash.clone(), got));
                }
                hashes.push(hash.clone());
            }
            for (hash, at) in &short_ended {
                sink.cut(hash, *at);
                state.plan.retain(|planned| planned.item.hash != *hash);
                state.leftovers = true;
            }
            sink.flush(&hashes);
            match &result {
                Ok(results) => {
                    // Only the pull's own items, each once (invariant 22;
                    // WP3.INT, WP3.3 review major): a peer's reply cannot make
                    // us back off, or record errors for, objects we did not
                    // ask it for.
                    let mut listed: HashSet<&str> = hashes.iter().map(String::as_str).collect();
                    for result in &results.results {
                        if !listed.remove(result.hash.as_str()) {
                            continue;
                        }
                        if is_failure(&result.outcome) {
                            lane.object_failed(target, false, &result.hash, &result.outcome);
                            state.failed.insert(result.hash.clone());
                        }
                    }
                }
                Err(error) if is_unsupported(error) => {
                    state.interim.get_or_insert(error.clone());
                }
                Err(error @ (CallError::ConnectionLost(_) | CallError::Unavailable(_))) => {
                    state.session_lost.get_or_insert(error.clone());
                }
                Err(CallError::Remote(error)) if error.code == "CANCELLED" => {}
                Err(error) => {
                    state.peer_error.get_or_insert(error.clone());
                }
            }
            pull.ended = Some(result);
            lane.stats.pulls_in_flight.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

fn sink_event(lane: &LaneRun, target: &Target, state: &mut RoundState, event: SinkEvent) {
    match event {
        SinkEvent::Released {
            tag,
            bytes,
            written,
        } => {
            // (The window got this credit back on the writer thread.)
            if let Some(pull) = state.pulls.get_mut(&tag) {
                pull.released += bytes;
            }
            if written {
                state.written += bytes;
                bump(&lane.stats.bytes_written, bytes);
            }
        }
        SinkEvent::Settled {
            hash,
            tag,
            outcome,
            call,
        } => {
            state.want_dry = false;
            // Settled in this round's sink: whatever of it is still planned
            // would be read at the source, sent and dropped here (WP3.INT
            // re-review). It continues, if at all, from the durable offset
            // our worker gives in a later round.
            state.plan.retain(|planned| planned.item.hash != hash);
            match &outcome {
                ObjectOutcome::Completed => {
                    state.completed += 1;
                    bump(&lane.stats.objects_down, 1);
                    if let Some(pull) = state.pulls.get_mut(&tag) {
                        pull.completed += 1;
                    }
                    lane.object_done(target, false, &hash);
                }
                ObjectOutcome::CorruptAtSource => {
                    state.corrupt.push(hash.clone());
                    lane.object_failed(target, false, &hash, &outcome);
                    state.failed.insert(hash);
                }
                ObjectOutcome::Failed(error) if call => {
                    // The whole write call failed: the round's error, not
                    // the object's (it resumes from its durable offset).
                    state.leftovers = true;
                    if !is_contention(error) {
                        state
                            .local_error
                            .get_or_insert_with(|| CallError::Remote(error.clone()));
                    }
                }
                ObjectOutcome::Progressed { .. } => state.leftovers = true,
                other => {
                    if is_failure(other) {
                        lane.object_failed(target, false, &hash, other);
                        state.failed.insert(hash);
                    } else if matches!(other, ObjectOutcome::Failed(_)) {
                        // Contention in our worker: it continues next round.
                        state.leftovers = true;
                    }
                }
            }
        }
    }
}

/// Emits `ExchangeFinished` for every pull that ended and whose bytes have
/// all left the sink.
fn report_pulls(lane: &LaneRun, target: &Target, state: &mut RoundState) {
    for pull in state.pulls.values_mut() {
        if pull.reported || pull.released < pull.received {
            continue;
        }
        let Some(result) = &pull.ended else {
            continue;
        };
        pull.reported = true;
        let error = match result {
            Ok(_) => None,
            Err(CallError::Remote(error)) if error.code == "CANCELLED" => None,
            Err(error) => Some(error.to_wire()),
        };
        lane.finished(
            &target.repo,
            ops::OBJECTS_PULL,
            error,
            0,
            pull.received,
            0,
            pull.completed,
            pull.started,
        );
    }
}

/// Updates the handoff table after a download round (module docs, "Lease
/// handoff"): an object its source failed here is parked under its lease,
/// so another source of ours can continue it at once; one this lane took
/// over is continued by its next round when the round went well, given
/// back (parked) when the round failed, and forgotten once installed or
/// another session's. Objects under the lane's own lease ids that are not
/// finished keep their id marked as holding (its next rounds ask it).
fn settle_handoffs(
    lane: &LaneRun,
    target: &Target,
    client: &SessionWorkerClient,
    state: &RoundState,
    results: &HashMap<String, SinkResult>,
    failed: bool,
) {
    let instance = client.endpoint().instance.as_str();
    let holder = lane.claimant.as_str();
    for (hash, planned) in &state.planned {
        let result = results.get(hash);
        // Where the worker stands, as it last said (not known after a write
        // call that failed as a whole).
        let known = match result {
            Some(result) if result.call.is_some() => None,
            Some(result) => Some(result.durable),
            None => Some(planned.item.offset),
        };
        let at = |offset: u64| WantItem {
            offset,
            ..planned.item.clone()
        };
        let gone = matches!(
            result.and_then(|result| result.outcome.as_ref()),
            Some(
                ObjectOutcome::Completed
                    | ObjectOutcome::Skipped
                    | ObjectOutcome::InFlightElsewhere
                    | ObjectOutcome::Unsolicited
            )
        );
        if let Some(slot) = planned.slot
            && !gone
        {
            lane.holding(&target.repo, slot);
        }
        let lease = planned.lease.as_str();
        match known {
            _ if gone => {
                if planned.adopted() {
                    handoff_release(instance, hash, lease);
                }
            }
            Some(offset) if state.failed.contains(hash) => {
                if planned.adopted() {
                    handoff_unclaim(instance, hash, lease, holder, Some(&at(offset)));
                } else {
                    handoff_park(instance, &at(offset), lease);
                }
            }
            Some(offset) if planned.adopted() && !failed => {
                handoff_progress(instance, &at(offset), lease, holder);
            }
            Some(offset) if planned.adopted() => {
                handoff_unclaim(instance, hash, lease, holder, Some(&at(offset)));
            }
            None if planned.adopted() => handoff_release(instance, hash, lease),
            _ => {}
        }
    }
}

/// Asks the local worker for more wanted objects (module docs, step 1).
fn want_more(
    lane: &LaneRun,
    target: &Target,
    client: &SessionWorkerClient,
    route: &SyncRoute,
    renewal: &Renewal,
    state: &mut RoundState,
) {
    let instance = client.endpoint().instance.as_str();
    let holder = lane.claimant.as_str();
    let blocked = |hash: &str| object_blocked(&lane.key(&target.repo, false, hash));
    // First what this lane continues, or takes over from a source of ours
    // that cannot serve it, under the lease that holds it (module docs,
    // "Lease handoff").
    let taken = handoff_take(
        instance,
        holder,
        MAX_WANT_ITEMS,
        lane.options.window_bytes,
        &|hash| lane.spec.owns(hash) && !blocked(hash) && !state.planned_or_begun(hash),
    );
    let mut fresh = 0;
    for (item, lease) in taken {
        if state.planned_or_begun(&item.hash) {
            continue;
        }
        fresh += 1;
        renewal.activate();
        state.plan(Planned {
            item,
            lease,
            slot: None,
        });
    }
    let idle = state.in_flight() == 0;
    for (index, slot) in lane.want_order(&target.repo, idle).into_iter().enumerate() {
        // The second id only when the first found nothing to move.
        if index > 0 && fresh > 0 {
            break;
        }
        let lease = lane.lease(slot);
        let header = WantHeader {
            route: route.clone(),
            lane: lane.spec,
            max_items: MAX_WANT_ITEMS as u32,
            max_bytes: lane.options.window_bytes,
            session: lease.clone(),
        };
        let items = match client.want(&header, worker_client::deadlines::WANT) {
            Ok(items) => items,
            Err(error) if is_unsupported(&error) => {
                state.interim = Some(error);
                return;
            }
            Err(error) => {
                state.local_error.get_or_insert(error);
                return;
            }
        };
        lane.asked(&target.repo, slot, !items.is_empty());
        for item in items {
            if !lane.spec.owns(&item.hash) || state.planned_or_begun(&item.hash) {
                if state.begun.contains(&item.hash) {
                    state.leftovers = true;
                }
                continue;
            }
            // An object in its backoff for this source is parked under this
            // lease (another source may take it); one another source
            // continues under this lease is left to it.
            if !handoff_wanted(instance, &item, &lease, holder, blocked(&item.hash)) {
                continue;
            }
            fresh += 1;
            state.plan(Planned {
                item,
                lease: lease.clone(),
                slot: Some(slot),
            });
        }
    }
    if fresh == 0 {
        state.want_dry = true;
    }
}

/// Issues the next pull when the window and a peer slot allow (module docs,
/// step 2). The first pull of a round waits its turn; a second one goes when
/// its budget is free and nobody waits, and, if it would take the key past
/// its share of the window, only once the pull in flight received half its
/// budget ("re-pull at half").
///
/// A pull reserves at most its key's fair share of the lane window
/// (`window / repos`, 8 MiB): the window is shared by the lane's keys, and a
/// pull whose source stalls (a first-serve verify, a slow disk) holds its
/// reservation until it ends, so a whole-window pull would stop every other
/// repository's downloads on the lane (§2.3; WP3.INT re-review). One key
/// still uses the whole window with its two pulls once data flows.
#[allow(clippy::too_many_arguments)]
fn issue_pull<'scope, 'env>(
    lane: &'env LaneRun,
    target: &'env Target,
    route: &'env SyncRoute,
    sink: &'env LazySink,
    tx: &mpsc::Sender<RoundEvent>,
    abort: &'env AtomicBool,
    renewal: &Renewal,
    state: &mut RoundState,
    scope: &'scope thread::Scope<'scope, 'env>,
) -> bool {
    let first = state.in_flight() == 0;
    // A pull's budget never exceeds what the acceptor honours, nor the key's
    // fair share of the window: within its share a key pipelines freely (a
    // round of small objects keeps two pulls in flight), beyond it only once
    // the pull in flight received half its budget, so a key whose source
    // stalls holds at most its share.
    let window = lane.window.capacity().min(PULL_BUDGET_BYTES);
    let share = (window / lane.options.repos.max(1) as u64).max(1);
    let in_flight: Vec<&Pull> = state
        .pulls
        .values()
        .filter(|pull| pull.ended.is_none())
        .collect();
    let streaming = in_flight
        .iter()
        .all(|pull| pull.streamed.load(Ordering::Relaxed) >= pull.budget / 2);
    let reserved: u64 = in_flight.iter().map(|pull| pull.budget).sum();
    let cap = if first || streaming {
        share
    } else {
        share.saturating_sub(reserved)
    };
    if cap == 0 {
        return false;
    }
    let plan_bytes = state.plan_bytes();
    let min = plan_bytes.min(share / 2).min(cap);
    let max = plan_bytes.min(cap);
    // (Claims stay renewed while the round waits its turn.)
    let stop = || {
        renewal.tick(false);
        lane.stopped()
    };
    let budget = if first {
        lane.window.reserve(min, max, &stop)
    } else {
        lane.window.try_reserve(min, max)
    };
    let Some(budget) = budget else {
        return false;
    };
    // A pipelined pull waits its turn for a peer slot too (behind a push or
    // a wants call that holds the other one), but not past the moment its
    // round could use it: it gives up after `EVENT_WAIT` × 10 and is tried
    // again.
    let until = Instant::now() + EVENT_WAIT * 10;
    let pipelined = || {
        renewal.tick(false);
        lane.stopped() || abort.load(Ordering::Relaxed) || Instant::now() >= until
    };
    let slot = if first {
        lane.slots.acquire(&stop)
    } else {
        lane.slots.acquire(&pipelined)
    };
    let Some(slot) = slot else {
        lane.window.release(budget);
        return false;
    };
    // Take items in order, at most the `MAX_WANT_ITEMS` an acceptor takes in
    // one pull (§6.4; a plan can hold more: what the lane takes over from a
    // source of ours that cannot serve it, plus its own want list; WP3.INT
    // re-review); the last one may be split (its rest stays first in the
    // plan for the next pull).
    let mut items = Vec::new();
    let mut leases = Vec::new();
    let mut left = budget;
    while let Some(front) = state.plan.front_mut() {
        let remaining = front.item.size.saturating_sub(front.item.offset);
        if items.len() >= MAX_WANT_ITEMS || (remaining > 0 && left == 0) {
            break;
        }
        let take = remaining.min(left);
        items.push(front.item.clone());
        leases.push(front.lease.clone());
        left -= take;
        if take == remaining {
            state.plan.pop_front();
        } else {
            front.item.offset += take;
            break;
        }
    }
    let budget = budget - left;
    lane.window.release(left);
    let spans = allot(&items, budget);
    let tag = state.next_tag;
    state.next_tag += 1;
    let sink = sink.get();
    for ((item, lease), (start, _)) in items.iter().zip(&leases).zip(&spans) {
        sink.expect(&item.hash, lease, item.size, *start, tag);
        state.begun.insert(item.hash.clone());
    }
    // Per item: its span, and whether its size is unknown (its sender's one
    // empty chunk then gives the size).
    let allowed: HashMap<String, (u64, u64, bool)> = items
        .iter()
        .zip(&spans)
        .map(|(item, span)| (item.hash.clone(), (span.0, span.1, size_unknown(item))))
        .collect();
    let streamed = Arc::new(AtomicU64::new(0));
    state.pulls.insert(
        tag,
        Pull {
            started: Instant::now(),
            spans: items
                .iter()
                .zip(&spans)
                .map(|(item, (start, end))| (item.hash.clone(), *start, *end))
                .collect(),
            budget,
            streamed: streamed.clone(),
            received: 0,
            released: 0,
            completed: 0,
            ended: None,
            reported: false,
        },
    );
    bump(&lane.stats.pulls, 1);
    let in_flight = bump(&lane.stats.pulls_in_flight, 1);
    peak(&lane.stats.pulls_peak, in_flight);
    lane.started(&target.repo, ops::OBJECTS_PULL);
    let request = ObjectsPull {
        repo: route.repo.clone(),
        sender: route.local_replica.clone(),
        receiver: route.remote_replica.clone(),
        lane: lane.spec,
        items,
        budget_bytes: budget,
    };
    let tx = tx.clone();
    scope.spawn(move || {
        let _slot = slot;
        let mut received = 0u64;
        let mut ends: HashMap<String, u64> = HashMap::new();
        let mut sized: HashMap<String, u64> = HashMap::new();
        let header = serde_json::to_value(&request).unwrap_or_default();
        let result = lane
            .conn
            .call_streaming(
                ops::OBJECTS_PULL,
                header,
                Vec::new(),
                Priority::Control,
                None,
                lane.options.idle,
                &mut |chunk: ChunkHeader, bytes: &[u8]| {
                    if abort.load(Ordering::Relaxed) || lane.stopped() {
                        return Err(ErrorWire::coded("CANCELLED", "the objects round ended"));
                    }
                    let len = bytes.len() as u64;
                    let Some(hash) = chunk.hash.as_deref() else {
                        return Err(ErrorWire::coded(
                            "PROTOCOL_VIOLATION",
                            "an objects.pull stream carried a chunk without an object",
                        ));
                    };
                    let Some(&(start, end, unknown)) = allowed.get(hash) else {
                        return Err(ErrorWire::coded(
                            "PROTOCOL_VIOLATION",
                            format!("objects.pull streamed {hash}, which it was not asked for"),
                        ));
                    };
                    // In order from the start of its span, inside it; an
                    // empty chunk only as the one chunk of an empty span (a
                    // zero-size object, or one whose size we do not know):
                    // empty chunks carry no bytes but reset the call's idle
                    // deadline, so a peer could hold the pull, its slot and
                    // the window with them forever (invariant 22; WP3.INT
                    // re-review).
                    let chunk_end = chunk.offset.saturating_add(len);
                    let first_chunk = !ends.contains_key(hash);
                    let next = ends.get(hash).copied().unwrap_or(start);
                    let sized_ok = len > 0 || (first_chunk && start == end);
                    if chunk.offset != next
                        || chunk_end > end
                        || !sized_ok
                        || received.saturating_add(len) > budget
                    {
                        return Err(ErrorWire::coded(
                            "PROTOCOL_VIOLATION",
                            format!(
                                "objects.pull streamed {hash} [{}, {chunk_end}); its span [{start}, \
                                 {end}) continues at {next}",
                                chunk.offset
                            ),
                        ));
                    }
                    received += len;
                    streamed.fetch_add(len, Ordering::Relaxed);
                    ends.insert(hash.to_owned(), chunk_end);
                    if unknown && chunk.total > 0 {
                        // The sender's total of an object whose size we did
                        // not know: it is pulled again with that size.
                        sized.insert(hash.to_owned(), chunk.total);
                        return Ok(());
                    }
                    if sink.offer(hash, chunk.offset, chunk.total, bytes, tag).is_err()
                        && allowed.iter().all(|(hash, (_, end, _))| {
                            ends.get(hash).is_some_and(|got| got >= end) || sink.settled(hash)
                        })
                    {
                        // Every object of this pull settled here (a resume
                        // point, a failed write): the rest of the stream
                        // would only be dropped (WP3.INT re-review).
                        return Err(ErrorWire::coded(
                            "CANCELLED",
                            "every object of the pull settled",
                        ));
                    }
                    Ok(())
                },
            )
            .and_then(|reply| body_of::<ObjectsResult>(&reply, ops::OBJECTS_PULL));
        let _ = tx.send(RoundEvent::PullEnded {
            tag,
            result,
            received,
            ends,
            sized,
        });
    });
    true
}

/// Reports objects whose bytes failed our worker's hash check to the peer
/// (`objects.report{CORRUPT_AT_SOURCE}`); the peer re-verifies its copy.
fn send_reports(lane: &LaneRun, target: &Target, route: &SyncRoute, state: &mut RoundState) {
    for hash in std::mem::take(&mut state.corrupt) {
        let stop = || lane.stopped();
        let Some(_slot) = lane.slots.acquire(&stop) else {
            return;
        };
        let report = ObjectsReport {
            repo: route.repo.clone(),
            sender: route.local_replica.clone(),
            hash: hash.clone(),
            code: "CORRUPT_AT_SOURCE".into(),
        };
        let header = serde_json::to_value(&report).unwrap_or_default();
        bump(&lane.stats.reports, 1);
        if let Err(error) = lane.conn.call(
            ops::OBJECTS_REPORT,
            header,
            Vec::new(),
            Priority::Control,
            deadlines::OBJECTS_REPORT,
        ) {
            event!(warn, (error.code().as_str()), {
                "peer": lane.peer.installation,
                "repo": target.repo,
                "object": hash,
                "detail": format!("objects.report failed: {error}"),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Upload round

fn upload_round(
    lane: &LaneRun,
    target: &Target,
    client: &Arc<SessionWorkerClient>,
) -> Result<RoundEnd, ExchangeError> {
    let route = lane.route(target);
    let started = Instant::now();
    let mut moved = false;
    loop {
        if lane.stopped() {
            return Err(lane.stop_error());
        }
        if started.elapsed() >= lane.options.round {
            return Ok(RoundEnd::Done(Next {
                after: moved.then_some(CONTENT_YIELD),
                progress: moved,
            }));
        }
        let stop = || lane.stopped();
        // The push gate first, then a peer slot: at most `peer_calls - 1`
        // pushes (with their wants) hold slots, so pulls and reports always
        // find one (WP3.INT re-review).
        let Some(gate) = lane.pushes.acquire(&stop) else {
            return Err(lane.stop_error());
        };
        let Some(slot) = lane.slots.acquire(&stop) else {
            return Err(lane.stop_error());
        };
        let request = ObjectsWantsRequest {
            repo: route.repo.clone(),
            sender: route.local_replica.clone(),
            receiver: route.remote_replica.clone(),
            lane: lane.spec,
            max_items: MAX_WANT_ITEMS as u32,
            max_bytes: PUSH_BUDGET_BYTES,
        };
        let wants = lane
            .conn
            .call(
                ops::OBJECTS_WANTS,
                serde_json::to_value(&request).unwrap_or_default(),
                Vec::new(),
                Priority::Control,
                deadlines::OBJECTS_WANTS,
            )
            .and_then(|reply| body_of::<ObjectsWantsReply>(&reply, ops::OBJECTS_WANTS));
        let wanted = match wants {
            Ok(reply) => reply.items,
            Err(error) if is_unsupported(&error) => {
                lane.set_interim(&target.repo, &error);
                return Ok(RoundEnd::Interim);
            }
            Err(error) => return idle_or(moved, ExchangeError::peer(error)),
        };
        let mut seen = HashSet::new();
        // At most the items we asked for (invariant 22; WP3.INT).
        let items: Vec<WantItem> = wanted
            .into_iter()
            .take(MAX_WANT_ITEMS)
            .filter(|item| {
                lane.spec.owns(&item.hash)
                    && seen.insert(item.hash.clone())
                    && !object_blocked(&lane.key(&target.repo, true, &item.hash))
            })
            .collect();
        // Objects whose size the peer does not know (an event without a size
        // descriptor) are pushed with the size our worker reads: the receiver
        // accepts any total when it knows none (WP3.INT re-review). One our
        // worker refuses to read is left out (nothing of ours to send).
        let items = sized_for_push(lane, client, &route, items);
        let spans = allot(&items, PUSH_BUDGET_BYTES);
        let (items, spans): (Vec<WantItem>, Vec<(u64, u64)>) = items
            .into_iter()
            .zip(spans)
            .filter(|(item, span)| covered(item, *span))
            .unzip();
        if items.is_empty() {
            return Ok(RoundEnd::Done(Next {
                after: None,
                progress: moved,
            }));
        }
        let pushed = push(lane, target, client, &route, items, &spans, slot);
        drop(gate);
        match pushed {
            Ok(push) => {
                moved |= push.sent > 0 || push.completed > 0;
                if push.sent == 0 && push.completed == 0 {
                    return Ok(RoundEnd::Done(Next {
                        after: None,
                        progress: moved,
                    }));
                }
            }
            Err(PushError::Interim(error)) => {
                lane.set_interim(&target.repo, &error);
                return Ok(RoundEnd::Interim);
            }
            Err(PushError::Failed(error)) => return idle_or(moved, error),
        }
    }
}

/// The items of a push, each with its size: an item whose size the peer
/// does not know ([`size_unknown`]) gets the total our worker reads for it,
/// with a zero-length read; one that read fails is dropped (our worker
/// cannot serve it: the peer learns that from the push, which does not send
/// it).
fn sized_for_push(
    lane: &LaneRun,
    client: &SessionWorkerClient,
    route: &SyncRoute,
    items: Vec<WantItem>,
) -> Vec<WantItem> {
    if !items.iter().any(size_unknown) {
        return items;
    }
    items
        .into_iter()
        .filter_map(|mut item| {
            if !size_unknown(&item) {
                return Some(item);
            }
            if lane.stopped() {
                return None;
            }
            let header = ReadHeader {
                route: route.clone(),
                hash: item.hash.clone(),
                offset: 0,
                len: 0,
            };
            let (total, _) = client.read(&header, worker_client::deadlines::READ).ok()?;
            item.size = total;
            Some(item)
        })
        .collect()
}

/// A failure that moved nothing is the key's; one after progress is left
/// for the next round (it repeats there if it persists).
fn idle_or(moved: bool, error: ExchangeError) -> Result<RoundEnd, ExchangeError> {
    if moved && !error.session_lost() && !matches!(error.error, CallError::Unavailable(_)) {
        Ok(RoundEnd::Done(Next {
            after: Some(CONTENT_YIELD),
            progress: true,
        }))
    } else {
        Err(error)
    }
}

struct Pushed {
    sent: u64,
    completed: u64,
}

enum PushError {
    Interim(CallError),
    Failed(ExchangeError),
}

/// One push: our worker's reads streamed to the peer (module docs).
fn push(
    lane: &LaneRun,
    target: &Target,
    client: &SessionWorkerClient,
    route: &SyncRoute,
    items: Vec<WantItem>,
    spans: &[(u64, u64)],
    slot: Slot,
) -> Result<Pushed, PushError> {
    let began = Instant::now();
    lane.started(&target.repo, ops::OBJECTS_PUSH);
    bump(&lane.stats.pushes, 1);
    let sizes: HashMap<String, u64> = items
        .iter()
        .map(|item| (item.hash.clone(), item.size))
        .collect();
    let ranges = read_ranges(&items, spans);
    let request = ObjectsPush {
        repo: route.repo.clone(),
        sender: route.local_replica.clone(),
        receiver: route.remote_replica.clone(),
        lane: lane.spec,
        items,
    };
    let finish = |error: Option<&CallError>, sent: u64, completed: u64| {
        lane.finished(
            &target.repo,
            ops::OBJECTS_PUSH,
            error.map(CallError::to_wire),
            sent,
            0,
            completed,
            0,
            began,
        );
    };
    let mut upload = match lane.conn.upload(
        ops::OBJECTS_PUSH,
        serde_json::to_value(&request).unwrap_or_default(),
        Priority::Control,
        lane.options.idle,
    ) {
        Ok(upload) => upload,
        Err(error) => {
            finish(Some(&error), 0, 0);
            drop(slot);
            return Err(PushError::Failed(ExchangeError::peer(error)));
        }
    };
    let mut sent = 0u64;
    let mut local: HashMap<String, ObjectOutcome> = HashMap::new();
    let mut failure: Option<ExchangeError> = None;
    // Objects whose last byte this push sent: only those can be corrupt at
    // their source here (WP3.INT, WP3.3 review major).
    let mut through_end: HashSet<String> = HashSet::new();
    // A first read of an object may verify it whole (verify-on-first-serve).
    let largest = sizes.values().copied().max().unwrap_or(0);
    client.read_stream(
        route,
        &ranges,
        lane.options.reads,
        worker_client::deadlines::with_verify(worker_client::deadlines::READ, largest),
        &mut |range, result| {
            if lane.stopped() {
                failure = Some(lane.stop_error());
                return ReadFlow::Stop;
            }
            match result {
                Ok((total, bytes)) => {
                    let size = sizes.get(&range.hash).copied().unwrap_or(total);
                    if total != size || bytes.len() as u64 != range.len {
                        local.insert(range.hash.clone(), ObjectOutcome::SizeMismatch);
                        return ReadFlow::SkipObject;
                    }
                    let last = range.offset + range.len == total;
                    for (header, piece) in wire_pieces(&range.hash, range.offset, total, &bytes) {
                        if let Err(error) = upload.chunk(header, piece) {
                            failure = Some(ExchangeError::peer(error));
                            return ReadFlow::Stop;
                        }
                        sent += piece.len() as u64;
                    }
                    if last {
                        through_end.insert(range.hash.clone());
                    }
                    ReadFlow::Continue
                }
                Err(error @ (CallError::Unavailable(_) | CallError::ConnectionLost(_))) => {
                    failure = Some(ExchangeError::local(error));
                    ReadFlow::Stop
                }
                Err(error) => {
                    local.insert(range.hash.clone(), read_outcome(&error));
                    ReadFlow::SkipObject
                }
            }
        },
    );
    bump(&lane.stats.bytes_pushed, sent);
    let result = match failure {
        Some(error) => {
            drop(upload);
            Err(error)
        }
        None => upload
            .finish()
            .and_then(|reply| body_of::<ObjectsResult>(&reply, ops::OBJECTS_PUSH))
            .map_err(ExchangeError::peer),
    };
    drop(slot);
    for (hash, outcome) in &local {
        if matches!(
            outcome,
            ObjectOutcome::NotPermitted | ObjectOutcome::NotAvailable
        ) {
            // The peer wants an object we do not hold shareable (it never
            // was ours to send, or we never received it): not serving it is
            // no error of ours, as on the acceptor's side (WP3.INT). Neither
            // an `ObjectError` nor a backoff here (WP3.INT re-review): a
            // peer's wants could name any hash, and the peer backs off what
            // we did not send it (its offer).
            continue;
        }
        lane.object_failed(target, true, hash, outcome);
    }
    match result {
        Ok(results) => {
            let mut completed = 0;
            // Only the push's own items, each once (invariant 22; WP3.INT,
            // WP3.3 review major): a peer's reply cannot inflate what we
            // count as uploaded, back off or report objects we did not push.
            let mut listed: HashSet<&str> = sizes.keys().map(String::as_str).collect();
            for result in &results.results {
                if !listed.remove(result.hash.as_str()) {
                    continue;
                }
                match &result.outcome {
                    ObjectOutcome::Completed => {
                        completed += 1;
                        lane.object_done(target, true, &result.hash);
                    }
                    ObjectOutcome::CorruptAtSource if !through_end.contains(&result.hash) => {
                        // Not sent through its end: the peer cannot have
                        // checked the whole object. A protocol error of the
                        // peer's, kept to this object (no report to our
                        // worker, which would re-read it whole).
                        lane.object_failed(target, true, &result.hash, &result.outcome);
                    }
                    ObjectOutcome::CorruptAtSource => {
                        // The peer's hash check failed on our bytes: our
                        // worker re-verifies (and quarantines) its copy.
                        lane.object_failed(target, true, &result.hash, &result.outcome);
                        if let Err(error) = client.report(
                            route,
                            &result.hash,
                            "CORRUPT_AT_SOURCE",
                            worker_client::deadlines::REPORT,
                        ) {
                            event!(warn, (error.code().as_str()), {
                                "peer": lane.peer.installation,
                                "repo": target.repo,
                                "object": result.hash,
                                "detail": format!("local objects.report failed: {error}"),
                            });
                        }
                    }
                    other if !local.contains_key(&result.hash) => {
                        lane.object_failed(target, true, &result.hash, other);
                    }
                    _ => {}
                }
            }
            bump(&lane.stats.objects_up, completed);
            finish(None, sent, completed);
            Ok(Pushed { sent, completed })
        }
        Err(error) => {
            finish(Some(&error.error), sent, 0);
            if is_unsupported(&error.error) {
                return Err(PushError::Interim(error.error));
            }
            Err(PushError::Failed(error))
        }
    }
}

// ---------------------------------------------------------------------------
// Interim path (§6.5)

/// Options of an interim bulk lane.
#[derive(Clone, Debug)]
pub struct BulkOptions {
    pub drive: DriveOptions,
    /// Worker `published-bulk-offer`.
    pub offer: Duration,
    /// Peer `objects.exchange` (60 s).
    pub exchange: Duration,
    /// Worker `published-bulk-apply`.
    pub apply: Duration,
    /// See [`CONTENT_YIELD`].
    pub content_yield: Duration,
}

impl BulkOptions {
    /// 2 repositories at once, the configured interval, the frozen deadlines.
    pub fn for_config(config: &Config) -> Self {
        Self {
            drive: DriveOptions::new(REPOS_PER_LANE, sync_interval(config)),
            offer: WORKER_DEADLINE,
            exchange: deadlines::OBJECTS_EXCHANGE,
            apply: WORKER_DEADLINE,
            content_yield: CONTENT_YIELD,
        }
    }
}

/// The status lane number of a bulk lane (`index + 1`; lane 0 is control).
pub fn status_lane(lane: LaneSpec) -> u32 {
    lane.index.saturating_add(1)
}

/// The legacy page lane of a [`LaneSpec`].
pub fn page_lane(lane: LaneSpec) -> bulk_sync::Lane {
    bulk_sync::Lane {
        index: lane.index as usize,
        count: lane.count as usize,
    }
}

/// A completed interim exchange.
#[derive(Clone, Debug)]
pub struct BulkOutcome {
    /// Our page's request id.
    pub request: String,
    /// Object bytes sent and received (decoded parts).
    pub sent_bytes: u64,
    pub received_bytes: u64,
    /// Objects we sent in full that the peer now lists as available.
    pub objects_up: u64,
    /// Objects the reply completed here.
    pub objects_down: u64,
    /// The peer's inventory (its reply without parts), for the next offer.
    pub inventory: Page,
    pub duration_ms: u64,
}

impl BulkOutcome {
    /// Object bytes moved in either direction.
    pub fn moved(&self) -> bool {
        self.sent_bytes > 0 || self.received_bytes > 0
    }
}

/// One bulk lane on the interim path only (Wave 2's lane): runs until
/// `cancel` fires or the session closes or goes away.
pub fn run_bulk_lane_with(
    ctx: Arc<Ctx>,
    peer: Arc<PeerContext>,
    lane: LaneSpec,
    conn: session::Conn,
    cancel: Cancel,
    options: BulkOptions,
) {
    let status = status_lane(lane);
    let name = format!("bulk-{}-{}", short(&peer.installation), status);
    if let Err(error) = lane_check(&peer, &conn, lane) {
        refuse(&ctx, &peer, &conn, &cancel, status, error, &options.drive);
        return;
    }
    let job = Arc::new(BulkJob {
        ctx: ctx.clone(),
        peer: peer.clone(),
        conn: conn.clone(),
        lane,
        options: options.clone(),
        state: InterimState::default(),
    });
    drive(
        &ctx,
        &peer,
        &conn,
        &cancel,
        job,
        &options.drive,
        status,
        &name,
    );
}

struct BulkJob {
    ctx: Arc<Ctx>,
    peer: Arc<PeerContext>,
    conn: session::Conn,
    lane: LaneSpec,
    options: BulkOptions,
    state: InterimState,
}

/// The peer's last inventory per repository (the legacy lane's cache): the
/// next offer sends the parts it lacks. Dropped after a failure of the key,
/// and when the key is reset (new worker, new remote replica, readiness
/// regained), removed or created.
#[derive(Default)]
struct Inventories {
    pages: HashMap<String, Page>,
    /// Bumped by every [`Job::forget`] of a repository. An exchange that was
    /// running when its key was reset or removed keeps the epoch it started
    /// with, and does not put its (now stale) inventory back.
    epochs: HashMap<String, u64>,
}

impl Inventories {
    fn epoch(&self, repo: &str) -> u64 {
        self.epochs.get(repo).copied().unwrap_or(0)
    }
}

/// The interim path's state of one lane.
#[derive(Default)]
struct InterimState {
    inventories: Mutex<Inventories>,
}

impl InterimState {
    #[allow(clippy::too_many_arguments)]
    fn run(
        &self,
        ctx: &Ctx,
        conn: &session::Conn,
        peer: &PeerContext,
        lane: LaneSpec,
        options: &BulkOptions,
        target: &Target,
        retry_after: &mut dyn FnMut(&ExchangeError) -> Option<Duration>,
    ) -> Result<Next, ExchangeError> {
        let (epoch, inventory) = {
            let cache = lock(&self.inventories);
            (
                cache.epoch(&target.repo),
                cache.pages.get(&target.repo).cloned(),
            )
        };
        let result = bulk_exchange(
            ctx,
            conn,
            peer,
            lane,
            &target.folder,
            &target.remote,
            inventory.as_ref(),
            options,
            retry_after,
        );
        let mut cache = lock(&self.inventories);
        match result {
            Ok(outcome) => {
                let moved = outcome.moved();
                // Not if the key was reset or removed meanwhile: this is the
                // inventory of the old route (or of a key that is gone).
                if cache.epoch(&target.repo) == epoch {
                    cache.pages.insert(target.repo.clone(), outcome.inventory);
                }
                Ok(Next {
                    after: moved.then_some(options.content_yield),
                    progress: moved,
                })
            }
            Err(error) => {
                // A failed exchange may leave the cached inventory stale (a
                // replica change, say): the next offer starts without it.
                cache.pages.remove(&target.repo);
                Err(error)
            }
        }
    }

    fn forget(&self, repo: &str) {
        let mut cache = lock(&self.inventories);
        cache.pages.remove(repo);
        let epoch = cache.epochs.entry(repo.to_owned()).or_default();
        *epoch = epoch.wrapping_add(1);
    }
}

impl Job for BulkJob {
    fn op(&self) -> &str {
        ops::OBJECTS_EXCHANGE
    }

    fn scope(&self) -> String {
        format!("{}#{}", ops::OBJECTS_EXCHANGE, self.lane.index)
    }

    fn class(&self) -> &'static str {
        "objects"
    }

    fn triggered(&self, before: &Signals, after: &Signals, absorbing: bool) -> bool {
        before.tip != after.tip
            || before.remote_tip != after.remote_tip
            || before.remote_content != after.remote_content
            || (before.content != after.content && !absorbing)
    }

    fn run(
        &self,
        target: &Target,
        retry_after: &mut dyn FnMut(&ExchangeError) -> Option<Duration>,
    ) -> Result<Next, ExchangeError> {
        self.state.run(
            &self.ctx,
            &self.conn,
            &self.peer,
            self.lane,
            &self.options,
            target,
            retry_after,
        )
    }

    fn forget(&self, repo: &str) {
        self.state.forget(repo);
    }
}

/// One interim object exchange of `folder` with `peer`'s `remote` entry on
/// bulk lane `lane`. `inventory` is the peer's inventory from the previous
/// exchange of this repository on this lane, if any. It is used only if it
/// fits this exchange's route and lane ([`fits`]); any other is left out.
#[allow(clippy::too_many_arguments)]
pub fn bulk_exchange(
    ctx: &Ctx,
    conn: &session::Conn,
    peer: &PeerContext,
    lane: LaneSpec,
    folder: &FolderView,
    remote: &PublishedEntry,
    inventory: Option<&Page>,
    options: &BulkOptions,
    retry_after: &mut dyn FnMut(&ExchangeError) -> Option<Duration>,
) -> Result<BulkOutcome, ExchangeError> {
    let repo = folder.record.repo.clone();
    let report = Report::start(
        ctx,
        &peer.installation,
        &repo,
        status_lane(lane),
        ops::OBJECTS_EXCHANGE,
    );
    let mut finished = Finished::default();
    let result = run(
        ctx,
        conn,
        peer,
        lane,
        folder,
        remote,
        inventory,
        options,
        &mut finished,
    );
    let duration_ms = report.finish(result.as_ref().map(|_| &finished), retry_after);
    let mut outcome = result?;
    outcome.duration_ms = duration_ms;
    Ok(outcome)
}

#[allow(clippy::too_many_arguments)]
fn run(
    ctx: &Ctx,
    conn: &session::Conn,
    peer: &PeerContext,
    lane: LaneSpec,
    folder: &FolderView,
    remote: &PublishedEntry,
    inventory: Option<&Page>,
    options: &BulkOptions,
    finished: &mut Finished,
) -> Result<BulkOutcome, ExchangeError> {
    let local = ExchangeError::local;
    let route = SyncRoute {
        peer: peer.installation.clone(),
        repo: folder.record.repo.clone(),
        local_replica: folder.record.device.clone(),
        remote_replica: remote.replica.clone(),
    };
    if remote.repo != route.repo {
        return Err(local(invalid(
            "REPOSITORY_MISMATCH",
            format!("the peer entry is for {}, not {}", remote.repo, route.repo),
        )));
    }
    let page_lane = page_lane(lane);
    // The worker refuses an inventory of another route (`BULK_ROUTE_MISMATCH`,
    // Fatal): a stale one is dropped here rather than park the key.
    let inventory = inventory.filter(|page| fits(page, &route, page_lane));
    let client = ctx.folders.client(&folder.record.state).ok_or_else(|| {
        local(CallError::Unavailable(format!(
            "LOCAL_WORKER_UNAVAILABLE: folder {} has no running worker",
            folder.record.state
        )))
    })?;

    // 1. The local offer (with the peer's last inventory: the parts it lacks).
    let page = client
        .call(
            worker_ops::OFFER,
            json!({"route": route, "lane": page_lane, "inventory": inventory}),
            options.offer,
        )
        .map_err(local)?;
    let page: Page = parse(page, worker_ops::OFFER).map_err(local)?;
    if page.lane != page_lane || page.repo != route.repo {
        return Err(local(invalid(
            "INCOMPATIBLE_BULK_LANE",
            format!(
                "published-bulk-offer returned a page of lane {}/{} for {}",
                page.lane.index, page.lane.count, page.repo
            ),
        )));
    }

    // 2. The peer.
    let body =
        serde_json::to_vec(&page).map_err(|error| local(invalid("INVALID_SYNC_REQUEST", error)))?;
    let header = json!({"repo": route.repo, "lane": lane});
    // The reply is a plain body (≤ the op's body limit), or streamed as
    // CHUNK frames before the RESPONSE (any size up to `MAX_REPLY`).
    let mut streamed = Vec::new();
    let reply = conn
        .call_streaming(
            ops::OBJECTS_EXCHANGE,
            header,
            body,
            Priority::Data,
            Some(options.exchange),
            options.exchange,
            &mut |chunk, bytes| collect(&mut streamed, &chunk, bytes),
        )
        .map_err(ExchangeError::peer)?;
    let reply = if streamed.is_empty() {
        reply
    } else {
        session::Reply {
            header: reply.header,
            body: streamed,
        }
    };
    let reply: Page = body_of(&reply, ops::OBJECTS_EXCHANGE).map_err(ExchangeError::peer)?;
    if reply.reply_to.as_deref() != Some(page.request.as_str()) {
        return Err(ExchangeError::peer(invalid(
            "UNBOUND_PEER_RESPONSE",
            format!(
                "the objects.exchange reply answers {:?}, not {}",
                reply.reply_to, page.request
            ),
        )));
    }
    let sent_bytes = page.object_bytes() as u64;
    let received_bytes = reply.object_bytes() as u64;
    let objects_up = completed(&page)
        .filter(|hash| reply.known_objects.contains(*hash))
        .count() as u64;
    let objects_down = completed(&reply).count() as u64;

    // 3. The local apply (the worker checks `reply_to` against the request).
    let applied = client
        .call(
            worker_ops::APPLY,
            json!({"route": route, "lane": page_lane, "page": reply, "request": page.request}),
            options.apply,
        )
        .map_err(local)?;
    let _: serde_json::Value = parse(applied, worker_ops::APPLY).map_err(local)?;
    *finished = Finished {
        summary: None,
        bytes_up: sent_bytes,
        bytes_down: received_bytes,
        objects_up,
        objects_down,
    };
    Ok(BulkOutcome {
        request: page.request,
        sent_bytes,
        received_bytes,
        objects_up,
        objects_down,
        inventory: reply.inventory(),
        duration_ms: 0,
    })
}

/// Largest streamed `objects.exchange` reply (the legacy wire's message bound).
pub const MAX_REPLY: usize = crate::ws::MAX_MESSAGE;

/// Appends one streamed reply chunk: in order, and within [`MAX_REPLY`].
fn collect(body: &mut Vec<u8>, chunk: &ChunkHeader, bytes: &[u8]) -> Result<(), ErrorWire> {
    if chunk.hash.is_some() || chunk.offset != body.len() as u64 {
        return Err(ErrorWire::coded(
            "PROTOCOL_VIOLATION",
            format!(
                "objects.exchange reply chunk at {} (hash {:?}) after {} bytes",
                chunk.offset,
                chunk.hash,
                body.len()
            ),
        ));
    }
    if body.len() + bytes.len() > MAX_REPLY {
        return Err(ErrorWire::coded(
            "WS_MESSAGE_TOO_LARGE",
            format!("the objects.exchange reply exceeds {MAX_REPLY} bytes"),
        ));
    }
    body.extend_from_slice(bytes);
    Ok(())
}

/// Whether `page`, a page of the peer, is one the worker accepts for `route`
/// and `lane` (the route half of `bulk_sync`'s page validation): our bulk
/// protocol and lane, our repository, from the remote replica to us.
pub fn fits(page: &Page, route: &SyncRoute, lane: bulk_sync::Lane) -> bool {
    page.format == bulk_sync::PROTOCOL
        && page.lane == lane
        && page.repo == route.repo
        && page.sender == route.remote_replica
        && page.receiver == route.local_replica
}

/// Objects whose part in `page` ends at their total size (the part that
/// completes them).
fn completed(page: &Page) -> impl Iterator<Item = &String> {
    page.object_parts.iter().filter_map(|(hash, part)| {
        let len = (part.hex.len() / 2) as u64;
        (part.offset.checked_add(len) == Some(part.total)).then_some(hash)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The per-object backoff table stays bounded (WP3.INT re-review): a
    /// peer can make us back off as many objects as it names, and the table
    /// pruned only entries an hour past their expiry, with a full scan on
    /// every insert once it was large.
    #[test]
    fn the_object_backoff_table_is_bounded() {
        let local = format!("bounded-{}", uuid::Uuid::new_v4());
        let began = Instant::now();
        for index in 0..(BACKOFF_MAX + BACKOFF_MAX / 2) {
            let key = object_key(&local, "peer", "repo", false, &format!("{index:064x}"));
            object_failed(&key, "NOT_AVAILABLE");
        }
        let entries = lock(held()).entries;
        assert!(entries <= BACKOFF_MAX, "{entries} entries");
        let counted: usize = lock(held()).map.values().map(HashMap::len).sum();
        assert_eq!(counted, entries);
        // The most recent failures are kept (the earliest-expiring go).
        let last = object_key(
            &local,
            "peer",
            "repo",
            false,
            &format!("{:064x}", BACKOFF_MAX + BACKOFF_MAX / 2 - 1),
        );
        assert!(object_blocked(&last));
        assert!(
            began.elapsed() < Duration::from_secs(20),
            "{:?}",
            began.elapsed()
        );
        // An installed object forgets every source's entry, and the count
        // follows.
        let before = lock(held()).entries;
        assert_eq!(object_installed(&last), vec!["NOT_AVAILABLE".to_owned()]);
        assert_eq!(lock(held()).entries, before - 1);
    }
}
