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
//! excluded from the listing meanwhile. A demand pass does not wait that out,
//! since its read is local and a reader is waiting. What the listener refused
//! is backed off alike, by every pass, and stays reported as unsent. A store
//! that timed out halves the batch of that (folder, endpoint), and each answered
//! one grows it back by a quarter. A failure logs `SYNC_PUSH_FAILED` once per
//! distinct error and rests that (folder, endpoint), so one folder the listener
//! does not publish keeps no other from pushing, or the whole endpoint when the
//! failure says the lane is gone or not answering. A rest grows from [`REST`] to
//! [`REST_MAX`] with each failure in a row. Nothing uncertain is replayed: its
//! digests stay missing on the listener and are listed again.
//!
//! **Sweeps.** Background passes walk the listener's scan page by page. A sweep
//! runs from its top to its end, and a pass says whether the pair has more to do
//! ([`PushOutcome::again`]): it stored something, its page has a cursor, or its
//! store failed. A whole sweep that stored nothing and failed nothing is the end
//! of it ([`PushOutcome::swept`]), whatever its pages listed, and the pair waits
//! for its next trigger. So what neither side holds costs one sweep per trigger,
//! even past what an `exclude` can name.
//!
//! **Interrupted.** A wake cancels a background call in flight. That is no
//! failure: nothing rests or shrinks, and the request is abandoned as uncertain
//! and never replayed. Its digests return only through a later listing, and
//! storing one twice is harmless.
use super::{
    demand::Seq,
    network::StoreReply,
    sync::{MAX_EXCLUDE, OBJECT_BATCH, Object, WantedPage, objects_body},
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

/// How long a failed push leaves what it failed on alone…
pub const REST: Duration = Duration::from_secs(1);
/// …doubling with each failure in a row to at most this. An answered store or a
/// clean background pass starts it over.
pub const REST_MAX: Duration = Duration::from_secs(60);
/// The deadline of an `objects.wanted`. Nothing is sent or scanned by a demand
/// listing, so a listener that does not answer this soon is not to hold up a
/// reader that another peer can serve.
pub const WANTED_DEADLINE: Duration = Duration::from_secs(3);
/// Why what this side holds did not get across: it is more than any one body can
/// carry, the more so in JSON to a peer from before binary bodies. A listener says
/// the same of what a worker it adopted from before them could not take.
pub(super) const TOO_LARGE: &str = "SYNC_OBJECT_TOO_LARGE";
/// What a pass remembers of a (folder, endpoint) lasts this long unused: past
/// any backoff, so none is cut short.
const FORGET_AFTER: Duration = Duration::from_secs(2 * UNAVAILABLE_MAX.as_secs());

/// Why [`run`] asked `objects.wanted` nothing of an endpoint, with no failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unlisted {
    /// It has no Ready bulk lane now.
    NoLane,
    /// It does not advertise [`features::CONTENT_PUSH`].
    NoPush,
    /// It sits out an earlier failure.
    Resting,
}

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
    /// It stopped before a next call, or cancelled one (a wake-up or cancel).
    pub interrupted: bool,
    /// The last failure, if any.
    pub error: Option<Failure>,
    /// A listing came back, so `remaining` says something.
    pub listed: bool,
    /// The pair has more to do with no new trigger. Not set by a listing that
    /// failed, so a hung listener does not hold the pump again at once.
    pub again: bool,
    /// It ended a sweep that ran from the top of the scan to its end, stored
    /// nothing and failed nothing: there is nothing more to do.
    pub swept: bool,
    /// How long that sweep took.
    pub took: Duration,
    /// Objects this side holds and known not to have got across, for as long as
    /// the listener may still want them. Excluded from a listing, they are not in
    /// `remaining`.
    pub unsent: usize,
    /// Why, for one of those.
    pub refusal: Option<String>,
    /// Objects the pages of this sweep listed so far and did not store, and which
    /// this side does not know it lacks. With what `remaining` leaves out of it:
    /// an empty page at the end of a scan does not make them sent.
    pub left: usize,
    /// Why nothing was listed, when nothing failed. With several endpoints, of
    /// the last that was skipped.
    pub unlisted: Option<Unlisted>,
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

/// What a sweep of the listener's scan has seen so far.
#[derive(Default)]
struct Sweep {
    /// When it began at the top of the scan, if it did: what it did not see then
    /// cannot lie before it.
    began: Option<Instant>,
    /// It stored something, which may have changed what is missing.
    stored: bool,
    /// Something in it failed or could not be tried, so it says nothing of what
    /// is left.
    failed: bool,
    /// The digests of `PushPeer::unsent` it listed.
    unsent: BTreeSet<Digest>,
    /// The objects its pages listed and did not store, less what this side lacks.
    left: usize,
}

/// The memory of one (folder, endpoint).
pub(super) struct PushPeer {
    /// Where the listener's scan resumes (`None`: from the start).
    pub(super) scan: Option<String>,
    /// The sweep under way.
    sweep: Sweep,
    /// The objects read and stored per request: halved by a timeout, grown
    /// back by answers.
    pub(super) batch: usize,
    /// How long a background listing may take: a page of a large repo can take
    /// the listener a while to scan. Grown by a timeout, up to [`PEER_DEADLINE`].
    /// A wake cancels it, so it never holds up a reader.
    pub(super) listing: Duration,
    /// What is not tried again until when: what this side lacks, and what it
    /// holds and the listener refused.
    pub(super) lacks: BTreeMap<Digest, Unavailable>,
    /// Of what `lacks` holds, what this side does hold, with the code of why it
    /// did not get across.
    unsent: BTreeMap<Digest, String>,
    /// The digests already logged as too large to send.
    too_large: BTreeSet<Digest>,
    /// Until when this (folder, endpoint) sits out after a failure.
    pub(super) rest_until: Option<Instant>,
    /// The failures in a row of this (folder, endpoint), which set its rest.
    failures: u32,
    /// The last pass that got this far.
    pub(super) touched: Instant,
    pub(super) counters: PushCounters,
}

impl Default for PushPeer {
    fn default() -> Self {
        Self {
            scan: None,
            sweep: Sweep::default(),
            batch: OBJECT_BATCH,
            listing: WANTED_DEADLINE,
            lacks: BTreeMap::new(),
            unsent: BTreeMap::new(),
            too_large: BTreeSet::new(),
            rest_until: None,
            failures: 0,
            touched: Instant::now(),
            counters: PushCounters::default(),
        }
    }
}

impl PushPeer {
    /// `digest` is not tried again for a while, longer each time.
    fn back_off(&mut self, digest: Digest, now: Instant) {
        let backoff = self
            .lacks
            .get(&digest)
            .map_or(UNAVAILABLE_FIRST, |entry| entry.backoff * 2)
            .min(UNAVAILABLE_MAX);
        self.lacks.insert(
            digest,
            Unavailable {
                until: now + backoff,
                backoff,
            },
        );
    }

    /// Another failure in a row: this (folder, endpoint) sits out for longer.
    fn rest(&mut self) {
        self.failures = self.failures.saturating_add(1);
        self.rest_until = Some(Instant::now() + rest_after(self.failures));
    }
}

/// The rest after `failures` failures in a row: [`REST`], doubling to
/// [`REST_MAX`].
fn rest_after(failures: u32) -> Duration {
    let doublings = failures.saturating_sub(1).min(16);
    REST.saturating_mul(1 << doublings).min(REST_MAX)
}

/// The push pass's memory across rounds.
pub struct PushState {
    /// Per (folder id, endpoint).
    pub(super) peers: BTreeMap<(String, String), PushPeer>,
    /// Endpoints whose lanes are gone or not answering, and until when no
    /// folder pushes to them.
    pub(super) resting: BTreeMap<String, Instant>,
    /// Per endpoint, its failures in a row, which set its rest, and when the last
    /// one was.
    pub(super) failures: BTreeMap<String, (u32, Instant)>,
    /// The deadline of an `objects.store` ([`fetch_deadline`]; tests shorten
    /// it).
    pub(super) deadline: fn(&[(Digest, Kind)]) -> Duration,
}

impl Default for PushState {
    fn default() -> Self {
        Self {
            peers: BTreeMap::new(),
            resting: BTreeMap::new(),
            failures: BTreeMap::new(),
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

    /// `endpoint` answered: its next failure is a first one.
    pub(super) fn answered(&mut self, endpoint: &str) {
        self.failures.remove(endpoint);
    }

    /// No folder pushes to `endpoint` for a while, longer with each failure in a
    /// row: its lanes are gone or not answering.
    pub(super) fn rest_endpoint(&mut self, endpoint: &str) {
        let now = Instant::now();
        let failures = self
            .failures
            .get(endpoint)
            .map_or(0, |(failures, _)| *failures)
            .saturating_add(1);
        self.failures.insert(endpoint.to_owned(), (failures, now));
        self.resting
            .insert(endpoint.to_owned(), now + rest_after(failures));
    }

    /// Whether `endpoint` as a whole sits out a failure at `now`.
    pub(super) fn endpoint_rests(&self, endpoint: &str, now: Instant) -> bool {
        self.resting.get(endpoint).is_some_and(|at| *at > now)
    }

    /// Whether `folder` to `endpoint` sits out a failure at `now`: a pass would
    /// ask nothing.
    pub(super) fn rests(&self, folder: &str, endpoint: &str, now: Instant) -> bool {
        self.endpoint_rests(endpoint, now)
            || self
                .peers
                .get(&(folder.to_owned(), endpoint.to_owned()))
                .and_then(|peer| peer.rest_until)
                .is_some_and(|at| at > now)
    }

    /// The next pass of `folder` to `endpoint` lists the listener's scan from
    /// its start.
    pub(super) fn restart_scan(&mut self, folder: &str, endpoint: &str) {
        if let Some(peer) = self
            .peers
            .get_mut(&(folder.to_owned(), endpoint.to_owned()))
        {
            peer.scan = None;
        }
    }

    /// Forgets the (folder, endpoint) pairs not pushed to for [`FORGET_AFTER`]
    /// and the rests that ended. By age, not by the folders syncing now: a
    /// folder whose slot is busy sits out a round, and that must not cost it
    /// its backoffs or what it logged.
    fn forget_idle(&mut self, now: Instant) {
        self.peers
            .retain(|_, peer| now.saturating_duration_since(peer.touched) < FORGET_AFTER);
        self.resting.retain(|_, until| *until > now);
        self.failures
            .retain(|_, (_, last)| now.saturating_duration_since(*last) < FORGET_AFTER);
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
            outcome.unlisted = Some(Unlisted::NoLane);
            continue;
        };
        if !lane.peer().has_feature(features::CONTENT_PUSH) {
            outcome.unlisted = Some(Unlisted::NoPush);
            continue;
        }
        let now = Instant::now();
        if state.endpoint_rests(endpoint, now) {
            outcome.unlisted = Some(Unlisted::Resting);
            continue;
        }
        let deadline = state.deadline;
        let peer = state
            .peers
            .entry((folder.id.clone(), endpoint.clone()))
            .or_default();
        peer.touched = now;
        if peer.rest_until.is_some_and(|at| at > now) {
            outcome.unlisted = Some(Unlisted::Resting);
            continue;
        }
        let mut pass = Pass {
            lanes,
            peer: &mut *peer,
            endpoint,
            folder,
            interrupt,
            cancel,
            deadline,
            outcome: &mut outcome,
            stored: BTreeSet::new(),
            answered: false,
            no_lane: false,
            sound: false,
        };
        let result = pass.run(&lane, mode);
        let (answered, sound, no_lane, stored) = (
            pass.answered,
            pass.sound,
            pass.no_lane,
            !pass.stored.is_empty(),
        );
        outcome.unsent += peer.unsent.len();
        outcome.refusal = peer.unsent.values().next().cloned().or(outcome.refusal);
        // Past a store that failed, a page with nothing to store proves nothing:
        // only the sweep it is part of can.
        let clean = sound && result.is_ok() && !outcome.interrupted;
        if answered || clean {
            peer.failures = 0;
            state.failures.remove(endpoint);
        }
        let Err(FetchFailure {
            failure, timed_out, ..
        }) = result
        else {
            // A clean Demand pass says nothing about what a Background one
            // failed on: only that pass, or an answered store, clears it.
            if clean {
                peer.counters.last_error = None;
            }
            // A lane that owns some of what was listed is down, and nothing else
            // moved: no need to ask again until it may be back.
            if no_lane && !stored {
                peer.rest();
            }
            continue;
        };
        if cancel.is_cancelled() {
            outcome.interrupted = true;
            break;
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
        if timed_out || failure == Failure::LaneLost {
            state.rest_endpoint(endpoint);
        } else {
            peer.rest();
        }
        outcome.error = Some(failure);
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
    /// The listener answered a store, so that much of its lanes works.
    answered: bool,
    /// Something listed has no Ready lane here.
    no_lane: bool,
    /// It is a background pass, in a sweep where nothing failed so far.
    sound: bool,
}

impl Pass<'_> {
    fn run(&mut self, lane: &Conn, mode: PushMode) -> Result<(), FetchFailure> {
        let now = Instant::now();
        let background = mode == PushMode::Background;
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
        if background && after.is_none() {
            self.peer.sweep = Sweep {
                began: Some(now),
                ..Sweep::default()
            };
        }
        let deadline = if background {
            self.peer.listing
        } else {
            WANTED_DEADLINE
        };
        let page = wanted(
            lane,
            &self.folder.repo,
            limit,
            &exclude,
            after.as_deref(),
            deadline,
            self.interrupt,
            self.cancel,
        );
        let page = match page {
            Ok(page) => page,
            Err(failure) if failure.abandoned => {
                self.outcome.interrupted = true;
                return Ok(());
            }
            Err(failure) => {
                if background {
                    self.peer.sweep.failed = true;
                    if failure.timed_out {
                        self.peer.listing = (self.peer.listing * 2).min(PEER_DEADLINE);
                    }
                }
                return Err(failure);
            }
        };
        self.outcome.listed = true;
        if background {
            // An expired backoff is kept while its object is still listed (the
            // next one doubles), and while it counts as unsent, which a sweep's
            // end settles; one no longer listed is forgotten. A Demand page lists
            // too little to tell.
            let listed: BTreeSet<Digest> = page
                .demand
                .iter()
                .chain(&page.missing)
                .map(|(digest, _)| *digest)
                .collect();
            let unsent = &self.peer.unsent;
            self.peer.lacks.retain(|digest, entry| {
                entry.until > now || listed.contains(digest) || unsent.contains_key(digest)
            });
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
            .store_listed(&demand, mode)
            .and_then(|()| self.store_listed(&missing, mode));
        self.outcome.remaining += listed.saturating_sub(self.stored.len());
        if background {
            let seen: Vec<Digest> = demand.iter().chain(&missing).map(|(d, _)| *d).collect();
            self.page_done(&seen, &page, result.is_err());
        }
        result
    }

    /// A background page was listed and worked through: where the scan and the
    /// sweep stand, and whether the pair has more to do.
    fn page_done(&mut self, listed: &[Digest], page: &WantedPage, failed: bool) {
        let stored = !self.stored.is_empty();
        let peer = &mut *self.peer;
        peer.sweep.stored |= stored;
        peer.sweep.failed |= failed || self.no_lane;
        self.sound = !peer.sweep.failed;
        let unsent = listed.iter().filter(|d| peer.unsent.contains_key(*d));
        peer.sweep.unsent.extend(unsent);
        // Interrupted, its rest is listed again.
        if self.outcome.interrupted {
            return;
        }
        peer.sweep.left += listed
            .iter()
            .filter(|digest| {
                !self.stored.contains(*digest)
                    && (!peer.lacks.contains_key(*digest) || peer.unsent.contains_key(*digest))
            })
            .count();
        self.outcome.left = peer.sweep.left;
        // A page wholly stored is listed afresh from the same position (what
        // was stored is no longer missing); any other moves on, and a scan at
        // its end starts over.
        let wholly_stored = page.missing.len() >= OBJECT_BATCH
            && page.missing.iter().all(|(d, _)| self.stored.contains(d));
        if !wholly_stored {
            peer.scan = page.cursor.clone();
        }
        self.outcome.again |= stored || failed || page.cursor.is_some() || wholly_stored;
        if page.cursor.is_some() || wholly_stored {
            return;
        }
        // The end of the scan. Only a whole sweep that changed nothing is the end
        // of the work; any other goes over it again, from the top.
        let sweep = std::mem::take(&mut peer.sweep);
        let Some(began) = sweep.began.filter(|_| !sweep.stored && !sweep.failed) else {
            self.outcome.again = true;
            return;
        };
        self.outcome.swept = true;
        self.outcome.took = began.elapsed();
        // Unsent still means the listener wants it: it was listed, or its backoff
        // keeps it from being listed.
        let now = Instant::now();
        let lacks = &peer.lacks;
        peer.unsent.retain(|digest, _| {
            sweep.unsent.contains(digest)
                || lacks.get(digest).is_some_and(|entry| entry.until > now)
        });
    }

    /// Reads and stores `listed`, per owning lane, skipping what is backed off.
    /// What this side lately lacked it may hold by now, and the read is local, so
    /// a [`PushMode::Demand`] pass, with a reader waiting, reads it again. What
    /// the listener refused it would only refuse again.
    fn store_listed(
        &mut self,
        listed: &[(Digest, Kind)],
        mode: PushMode,
    ) -> Result<(), FetchFailure> {
        let now = Instant::now();
        let due: Vec<(Digest, Kind)> = listed
            .iter()
            .filter(|(digest, _)| {
                let backed_off = self
                    .peer
                    .lacks
                    .get(digest)
                    .is_some_and(|entry| entry.until > now);
                !backed_off || (mode == PushMode::Demand && !self.peer.unsent.contains_key(digest))
            })
            .copied()
            .collect();
        let lanes = lanes_for(self.lanes, self.endpoint, &due);
        let owned: usize = lanes.iter().map(|(_, batch)| batch.len()).sum();
        self.no_lane |= owned < due.len();
        for (conn, batch) in lanes {
            let mut queue: VecDeque<(Digest, Kind)> = batch.into();
            while !queue.is_empty() {
                if stopped(self.interrupt, self.cancel) {
                    self.outcome.interrupted = true;
                    return Ok(());
                }
                let limit = self.peer.batch.min(queue.len());
                let asked: Vec<(Digest, Kind)> = queue.drain(..limit).collect();
                // Cut to what the body it becomes can hold.
                let binary = conn.peer().has_feature(features::CONTENT_BINARY);
                let fetched = self
                    .folder
                    .link
                    .fetch_reply(&asked, binary)
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
                    self.peer.unsent.insert(*digest, TOO_LARGE.to_owned());
                    if self.peer.too_large.insert(*digest) {
                        event!(warn, SYNC_OBJECT_TOO_LARGE, {
                            "peer": self.endpoint,
                            "repo": self.folder.repo.clone(),
                            "digest": digest.to_string(),
                        });
                    }
                }
                self.note_lacks(&asked[..looked], &fetched.objects);
                if fetched.objects.is_empty() {
                    continue;
                }
                // A wake during the read: what was read stays missing there
                // and is listed again.
                if stopped(self.interrupt, self.cancel) {
                    self.outcome.interrupted = true;
                    return Ok(());
                }
                self.store(&conn, fetched.objects)?;
            }
        }
        Ok(())
    }

    /// Of `looked`, what `read` does not hold is backed off, unless it still is:
    /// a Demand pass reading it early does not lengthen that. What was read is
    /// not, unless the listener refused it, whose backoff its next answer settles.
    fn note_lacks(&mut self, looked: &[(Digest, Kind)], read: &[Object]) {
        let now = Instant::now();
        for (digest, _) in looked {
            if read.iter().any(|object| object.digest == *digest) {
                if !self.peer.unsent.contains_key(digest) {
                    self.peer.lacks.remove(digest);
                }
                continue;
            }
            let waiting = self.peer.lacks.get(digest);
            if waiting.is_none_or(|entry| entry.until <= now) {
                self.peer.back_off(*digest, now);
            }
        }
    }

    /// One `objects.store` of one read. A wake cancels it: then what it held
    /// stays missing there and is listed again.
    fn store(&mut self, conn: &Conn, objects: Vec<Object>) -> Result<(), FetchFailure> {
        let sent: Vec<(Digest, Kind)> = objects.iter().map(|o| (o.digest, o.kind)).collect();
        let binary = conn.peer().has_feature(features::CONTENT_BINARY);
        let body =
            objects_body(&objects, binary).map_err(|e| worker_failure("objects.store", e))?;
        drop(objects);
        let deadline = (self.deadline)(&sent);
        let reply = call(
            conn,
            "objects.store",
            json!({"repo": self.folder.repo}),
            body,
            deadline,
            self.interrupt,
            self.cancel,
        );
        let reply = match reply {
            Ok(reply) => reply,
            Err(failure) if failure.abandoned => {
                self.outcome.interrupted = true;
                return Ok(());
            }
            Err(failure) => {
                if failure.timed_out {
                    self.shrink(sent.len());
                }
                return Err(failure);
            }
        };
        let reply: StoreReply = serde_json::from_slice(&reply)
            .map_err(|error| Failure::Error(format!("objects.store: malformed reply: {error}")))?;
        self.answered = true;
        self.peer.batch = grown_batch(self.peer.batch);
        // A listener speaks only for what it was sent: goal 5 lets a later design
        // free what is named here.
        let was_sent = |digest: &Digest| sent.iter().any(|(sent, _)| sent == digest);
        let stored: BTreeSet<Digest> = reply.stored.into_iter().filter(was_sent).collect();
        let refused: BTreeMap<Digest, String> = reply
            .refused
            .into_iter()
            .filter(|refused| was_sent(&refused.digest) && !stored.contains(&refused.digest))
            .map(|refused| (refused.digest, refused.code))
            .collect();
        let now = Instant::now();
        let peer = &mut *self.peer;
        peer.counters.last_error = None;
        peer.counters.pushed += stored.len() as u64;
        peer.counters.refused += refused.len() as u64;
        self.outcome.pushed += stored.len();
        for digest in &stored {
            peer.lacks.remove(digest);
            peer.unsent.remove(digest);
        }
        self.stored.extend(stored);
        for (digest, code) in refused {
            if peer.counters.last_refusal.as_ref() != Some(&code) {
                event!(warn, SYNC_PUSH_REFUSED, {
                    "peer": self.endpoint,
                    "repo": self.folder.repo.clone(),
                    "digest": digest.to_string(),
                    "code": code.clone(),
                });
                peer.counters.last_refusal = Some(code.clone());
            }
            peer.unsent.insert(digest, code);
            peer.back_off(digest, now);
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
#[allow(clippy::too_many_arguments)]
fn wanted(
    lane: &Conn,
    repo: &str,
    limit: usize,
    exclude: &[Digest],
    after: Option<&str>,
    deadline: Duration,
    interrupt: Option<(&Seq, u64)>,
    cancel: &Cancel,
) -> Result<WantedPage, FetchFailure> {
    let body = call(
        lane,
        "objects.wanted",
        json!({"repo": repo, "limit": limit, "exclude": exclude, "after": after}),
        Vec::new(),
        deadline,
        interrupt,
        cancel,
    )?;
    serde_json::from_slice(&body)
        .map_err(|error| Failure::Error(format!("objects.wanted: malformed reply: {error}")).into())
}

/// One data-priority request on a peer lane: the reply body. A stop abandons it.
fn call(
    conn: &Conn,
    op: &str,
    header: serde_json::Value,
    body: Vec<u8>,
    deadline: Duration,
    interrupt: Option<(&Seq, u64)>,
    cancel: &Cancel,
) -> Result<Vec<u8>, FetchFailure> {
    conn.call_cancellable(op, header, body, Priority::Data, deadline, &|| {
        stopped(interrupt, cancel)
    })
    .map(|reply| reply.body)
    .map_err(|error| FetchFailure {
        timed_out: matches!(error, CallError::Timeout { .. }),
        abandoned: matches!(error, CallError::Cancelled),
        failure: peer_failure(op, error),
    })
}
