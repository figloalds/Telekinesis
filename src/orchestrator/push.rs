//! The dialer's push pass: content moving from a dialer to its listener over
//! the lanes the dialer opened (only dialers originate requests).
//!
//! For one folder and each endpoint whose bulk lane advertises
//! [`features::CONTENT_PUSH`], a pass asks `objects.wanted`: in
//! [`PushMode::Demand`] for what the listener's readers are blocked on, in
//! [`PushMode::Background`] also for the next page of its `objects.missing`
//! scan. It reads what it holds from this side's worker (`fetch_reply`) and
//! calls `objects.store` on the bulk lane that owns each digest, one request
//! per read: a read is cut to the byte bounds that also fit a store body.
//!
//! The rules are the pull path's. What this side lacks is no error: it is
//! backed off ([`UNAVAILABLE_FIRST`] doubling to [`UNAVAILABLE_MAX`]) and
//! excluded from the listing meanwhile. A store that timed out halves the
//! batch of that (folder, endpoint), and each answered one grows it back by a
//! quarter. A failure logs `SYNC_PUSH_FAILED` once per distinct error and
//! rests, for [`REST`], that (folder, endpoint), so one folder the listener
//! does not publish keeps no other from pushing, or the whole endpoint when
//! the failure says the lane is gone or not answering. Nothing uncertain is
//! replayed: its digests stay missing on the listener and are listed again.
//! So are the digests the listener refused, which are dropped from the round.
use super::{
    demand::Seq,
    network::StoreReply,
    sync::{MAX_EXCLUDE, OBJECT_BATCH, Object, WantedPage},
    sync_driver::{
        Failure, FetchFailure, Lanes, PEER_DEADLINE, SyncFolder, UNAVAILABLE_FIRST,
        UNAVAILABLE_MAX, Unavailable, fetch_deadline, grown_batch, halved_batch, lanes_for,
        peer_failure, worker_failure,
    },
};
use crate::{
    content::Kind,
    model::{content::Digest, orchestrator::CallError, protocol::features},
    session::{Conn, Priority},
    util::{cancel::Cancel, log::event},
};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::{Duration, Instant},
};

/// How long a failed push leaves what it failed on alone.
pub const REST: Duration = Duration::from_secs(1);
/// What a pass remembers of a (folder, endpoint) lasts this long unused: past
/// any backoff, so none is cut short.
const FORGET_AFTER: Duration = Duration::from_secs(2 * UNAVAILABLE_MAX.as_secs());

/// What one [`run`] pushes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushMode {
    /// What the listener's readers are blocked on.
    Demand,
    /// That, then a page of what the listener's scan finds missing.
    Background,
}

/// What one [`run`] did, over every endpoint.
#[derive(Debug, Default)]
pub struct PushOutcome {
    /// Objects the listeners named durable.
    pub pushed: usize,
    /// Objects listed by this run and not stored by it. An estimate of the
    /// work left: a scan's later pages are not counted.
    pub remaining: usize,
    /// It stopped before a next call (a wake-up or cancel).
    pub interrupted: bool,
    /// The last failure, if any.
    pub error: Option<String>,
}

/// What the pass counted for one (folder, endpoint).
#[derive(Clone, Debug, Default)]
pub struct PushCounters {
    /// Objects the listener named durable.
    pub pushed: u64,
    /// Objects the listener refused.
    pub refused: u64,
    /// The failure last logged; cleared once pushing works again.
    pub last_error: Option<String>,
    /// The refusal code last logged.
    pub last_refusal: Option<String>,
}

/// The memory of one (folder, endpoint).
pub(super) struct PushPeer {
    /// Where the listener's scan resumes (`None`: from the start).
    pub(super) scan: Option<String>,
    /// The objects read and stored per request: halved by a timeout, grown
    /// back by answers.
    pub(super) batch: usize,
    /// What this side lacks, and until when it is not tried again.
    pub(super) lacks: BTreeMap<Digest, Unavailable>,
    /// The digests already logged as too large to send.
    too_large: BTreeSet<Digest>,
    /// Until when this (folder, endpoint) sits out after a failure.
    pub(super) rest_until: Option<Instant>,
    /// The last pass that got this far.
    pub(super) touched: Instant,
    pub(super) counters: PushCounters,
}

impl Default for PushPeer {
    fn default() -> Self {
        Self {
            scan: None,
            batch: OBJECT_BATCH,
            lacks: BTreeMap::new(),
            too_large: BTreeSet::new(),
            rest_until: None,
            touched: Instant::now(),
            counters: PushCounters::default(),
        }
    }
}

/// The push pass's memory across rounds.
pub struct PushState {
    /// Per (folder id, endpoint).
    pub(super) peers: BTreeMap<(String, String), PushPeer>,
    /// Endpoints whose lanes are gone or not answering, and until when no
    /// folder pushes to them.
    pub(super) resting: BTreeMap<String, Instant>,
    /// The deadline of an `objects.store` ([`fetch_deadline`]; tests shorten
    /// it).
    pub(super) deadline: fn(&[(Digest, Kind)]) -> Duration,
}

impl Default for PushState {
    fn default() -> Self {
        Self {
            peers: BTreeMap::new(),
            resting: BTreeMap::new(),
            deadline: fetch_deadline,
        }
    }
}

impl PushState {
    /// What the pass counted for `folder` to `endpoint`, once it tried.
    pub fn counters(&self, folder: &str, endpoint: &str) -> Option<&PushCounters> {
        self.peers
            .get(&(folder.to_owned(), endpoint.to_owned()))
            .map(|peer| &peer.counters)
    }

    /// Forgets the (folder, endpoint) pairs not pushed to for [`FORGET_AFTER`]
    /// and the rests that ended. By age, not by the folders syncing now: a
    /// folder whose slot is busy sits out a round, and that must not cost it
    /// its backoffs or what it logged.
    fn forget_idle(&mut self, now: Instant) {
        self.peers
            .retain(|_, peer| now.saturating_duration_since(peer.touched) < FORGET_AFTER);
        self.resting.retain(|_, until| *until > now);
    }
}

/// Pushes `folder` to each of `endpoints` that takes pushes, as `mode` says.
/// With `interrupt` (the wake and the value last seen), stops before its next
/// call to a peer once the wake moves past it.
pub fn run(
    lanes: &dyn Lanes,
    state: &mut PushState,
    endpoints: &[String],
    folder: &SyncFolder,
    mode: PushMode,
    interrupt: Option<(&Seq, u64)>,
    cancel: &Cancel,
) -> PushOutcome {
    let mut outcome = PushOutcome::default();
    state.forget_idle(Instant::now());
    for endpoint in endpoints {
        if stopped(interrupt, cancel) {
            outcome.interrupted = true;
            break;
        }
        let Some(lane) = lanes.any_bulk_lane(endpoint) else {
            continue;
        };
        if !lane.peer().has_feature(features::CONTENT_PUSH) {
            continue;
        }
        let now = Instant::now();
        if state.resting.get(endpoint).is_some_and(|at| *at > now) {
            continue;
        }
        let deadline = state.deadline;
        let peer = state
            .peers
            .entry((folder.id.clone(), endpoint.clone()))
            .or_default();
        peer.touched = now;
        if peer.rest_until.is_some_and(|at| at > now) {
            continue;
        }
        let result = Pass {
            lanes,
            peer: &mut *peer,
            endpoint,
            folder,
            interrupt,
            cancel,
            deadline,
            outcome: &mut outcome,
            stored: BTreeSet::new(),
        }
        .run(&lane, mode);
        let Err(FetchFailure { failure, timed_out }) = result else {
            // A clean Demand pass says nothing about what a Background one
            // failed on: only that pass, or an answered store, clears it.
            if mode == PushMode::Background && !outcome.interrupted {
                peer.counters.last_error = None;
            }
            continue;
        };
        if cancel.is_cancelled() {
            outcome.interrupted = true;
            break;
        }
        let rest = Instant::now() + REST;
        if timed_out || failure == Failure::LaneLost {
            state.resting.insert(endpoint.clone(), rest);
        } else {
            peer.rest_until = Some(rest);
        }
        let error = failure.to_string();
        if peer.counters.last_error.as_ref() != Some(&error) {
            event!(warn, SYNC_PUSH_FAILED, {
                "peer": endpoint.clone(),
                "folder": folder.id.clone(),
                "repo": folder.repo.clone(),
                "error": error.clone(),
            });
            peer.counters.last_error = Some(error.clone());
        }
        outcome.error = Some(error);
    }
    outcome
}

/// Whether a pass must stop before its next call.
fn stopped(interrupt: Option<(&Seq, u64)>, cancel: &Cancel) -> bool {
    cancel.is_cancelled() || interrupt.is_some_and(|(wake, seen)| wake.current() != seen)
}

/// One pass of one folder to one endpoint.
struct Pass<'a> {
    lanes: &'a dyn Lanes,
    peer: &'a mut PushPeer,
    endpoint: &'a str,
    folder: &'a SyncFolder,
    interrupt: Option<(&'a Seq, u64)>,
    cancel: &'a Cancel,
    deadline: fn(&[(Digest, Kind)]) -> Duration,
    outcome: &'a mut PushOutcome,
    /// What the listener named durable in this pass.
    stored: BTreeSet<Digest>,
}

impl Pass<'_> {
    fn run(&mut self, lane: &Conn, mode: PushMode) -> Result<(), FetchFailure> {
        let now = Instant::now();
        let (limit, exclude, after) = match mode {
            PushMode::Demand => (0, Vec::new(), None),
            PushMode::Background => (
                OBJECT_BATCH,
                self.peer
                    .lacks
                    .iter()
                    .filter(|(_, entry)| entry.until > now)
                    .map(|(digest, _)| *digest)
                    .take(MAX_EXCLUDE)
                    .collect(),
                self.peer.scan.clone(),
            ),
        };
        let page = wanted(
            lane,
            &self.folder.repo,
            limit,
            &exclude,
            after.as_deref(),
            self.cancel,
        )?;
        if mode == PushMode::Background {
            // An expired backoff is kept while its object is still listed (the
            // next one doubles); one no longer listed is forgotten. A Demand
            // page lists too little to tell.
            let listed: BTreeSet<Digest> = page
                .demand
                .iter()
                .chain(&page.missing)
                .map(|(digest, _)| *digest)
                .collect();
            self.peer
                .lacks
                .retain(|digest, entry| entry.until > now || listed.contains(digest));
        }
        // A digest both in demand and missing goes once, as demand.
        let mut seen = BTreeSet::new();
        let mut first = |list: &[(Digest, Kind)]| -> Vec<(Digest, Kind)> {
            list.iter()
                .filter(|(digest, _)| seen.insert(*digest))
                .copied()
                .collect()
        };
        let demand = first(&page.demand);
        let missing = first(&page.missing);
        let listed = demand.len() + missing.len();
        let result = self
            .store_listed(&demand)
            .and_then(|()| self.store_listed(&missing));
        self.outcome.remaining += listed.saturating_sub(self.stored.len());
        // A page wholly stored is listed afresh from the same position (what
        // was stored is no longer missing); any other moves on, and a scan at
        // its end starts over. Interrupted, its rest is listed again.
        let wholly_stored = page.missing.len() >= OBJECT_BATCH
            && page.missing.iter().all(|(d, _)| self.stored.contains(d));
        if mode == PushMode::Background && !self.outcome.interrupted && !wholly_stored {
            self.peer.scan = page.cursor;
        }
        result
    }

    /// Reads and stores `listed`, per owning lane, skipping what this side
    /// lately lacked.
    fn store_listed(&mut self, listed: &[(Digest, Kind)]) -> Result<(), FetchFailure> {
        let now = Instant::now();
        let due: Vec<(Digest, Kind)> = listed
            .iter()
            .filter(|(digest, _)| {
                self.peer
                    .lacks
                    .get(digest)
                    .is_none_or(|entry| entry.until <= now)
            })
            .copied()
            .collect();
        for (conn, batch) in lanes_for(self.lanes, self.endpoint, &due) {
            let mut queue: VecDeque<(Digest, Kind)> = batch.into();
            while !queue.is_empty() {
                if stopped(self.interrupt, self.cancel) {
                    self.outcome.interrupted = true;
                    return Ok(());
                }
                let limit = self.peer.batch.min(queue.len());
                let asked: Vec<(Digest, Kind)> = queue.drain(..limit).collect();
                let fetched = self
                    .folder
                    .link
                    .fetch_reply(&asked)
                    .map_err(|error| worker_failure("objects.fetch", error))?;
                // The worker cut its reply to its byte bounds: what it did not
                // look at is read next. It looked at one object at least, so
                // the queue always shrinks.
                let looked = fetched
                    .truncated_at
                    .map_or(asked.len(), |at| at.clamp(1, asked.len()));
                for object in asked[looked..].iter().rev() {
                    queue.push_front(*object);
                }
                for digest in &fetched.oversized {
                    if self.peer.too_large.insert(*digest) {
                        event!(warn, SYNC_OBJECT_TOO_LARGE, {
                            "peer": self.endpoint,
                            "repo": self.folder.repo.clone(),
                            "digest": digest.to_string(),
                        });
                    }
                }
                self.note_lacks(&asked[..looked], &fetched.objects);
                if !fetched.objects.is_empty() {
                    self.store(&conn, fetched.objects)?;
                }
            }
        }
        Ok(())
    }

    /// Of `looked`, what `read` does not hold is backed off; the rest is not.
    fn note_lacks(&mut self, looked: &[(Digest, Kind)], read: &[Object]) {
        let now = Instant::now();
        for (digest, _) in looked {
            if read.iter().any(|object| object.digest == *digest) {
                self.peer.lacks.remove(digest);
                continue;
            }
            let backoff = self
                .peer
                .lacks
                .get(digest)
                .map_or(UNAVAILABLE_FIRST, |entry| entry.backoff * 2)
                .min(UNAVAILABLE_MAX);
            self.peer.lacks.insert(
                *digest,
                Unavailable {
                    until: now + backoff,
                    backoff,
                },
            );
        }
    }

    /// One `objects.store` of one read.
    fn store(&mut self, conn: &Conn, objects: Vec<Object>) -> Result<(), FetchFailure> {
        let sent: Vec<(Digest, Kind)> = objects.iter().map(|o| (o.digest, o.kind)).collect();
        let body = serde_json::to_vec(&objects).map_err(|e| worker_failure("objects.store", e))?;
        drop(objects);
        let deadline = (self.deadline)(&sent);
        let reply = call(
            conn,
            "objects.store",
            json!({"repo": self.folder.repo}),
            body,
            deadline,
            self.cancel,
        )
        .inspect_err(|failure| {
            if failure.timed_out {
                self.shrink(sent.len());
            }
        })?;
        let reply: StoreReply = serde_json::from_slice(&reply)
            .map_err(|error| Failure::Error(format!("objects.store: malformed reply: {error}")))?;
        self.peer.batch = grown_batch(self.peer.batch);
        let counters = &mut self.peer.counters;
        counters.last_error = None;
        counters.pushed += reply.stored.len() as u64;
        counters.refused += reply.refused.len() as u64;
        self.outcome.pushed += reply.stored.len();
        self.stored.extend(reply.stored);
        for refused in reply.refused {
            if counters.last_refusal.as_ref() != Some(&refused.code) {
                event!(warn, SYNC_PUSH_REFUSED, {
                    "peer": self.endpoint,
                    "repo": self.folder.repo.clone(),
                    "digest": refused.digest.to_string(),
                    "code": refused.code.clone(),
                });
                counters.last_refusal = Some(refused.code);
            }
        }
        Ok(())
    }

    /// A store of `asked` objects timed out: half that, at least 1.
    fn shrink(&mut self, asked: usize) {
        self.peer.batch = halved_batch(self.peer.batch, asked);
        event!(info, SYNC_BATCH_SHRUNK, {
            "peer": self.endpoint,
            "folder": self.folder.id.clone(),
            "batch": self.peer.batch,
        });
    }
}

/// `objects.wanted` on a bulk lane of the listener.
fn wanted(
    lane: &Conn,
    repo: &str,
    limit: usize,
    exclude: &[Digest],
    after: Option<&str>,
    cancel: &Cancel,
) -> Result<WantedPage, FetchFailure> {
    let body = call(
        lane,
        "objects.wanted",
        json!({"repo": repo, "limit": limit, "exclude": exclude, "after": after}),
        Vec::new(),
        PEER_DEADLINE,
        cancel,
    )?;
    serde_json::from_slice(&body)
        .map_err(|error| Failure::Error(format!("objects.wanted: malformed reply: {error}")).into())
}

/// One data-priority request on a peer lane: the reply body.
fn call(
    conn: &Conn,
    op: &str,
    header: serde_json::Value,
    body: Vec<u8>,
    deadline: Duration,
    cancel: &Cancel,
) -> Result<Vec<u8>, FetchFailure> {
    conn.call_cancellable(op, header, body, Priority::Data, deadline, &|| {
        cancel.is_cancelled()
    })
    .map(|reply| reply.body)
    .map_err(|error| FetchFailure {
        timed_out: matches!(error, CallError::Timeout { .. }),
        failure: peer_failure(op, error),
    })
}
