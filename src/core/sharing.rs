//! What is shared with peers: event inventories, peer knowledge, outgoing
//! metadata pages, outbox acknowledgements and the metadata feed (format 7,
//! REVAMP_PLAN §6.3, WP2.5).
//!
//! # The feed
//!
//! The feed of a replica is its committed **shared** events in local
//! acceptance order (`journal_sequence.seq`). A peer reads it through a
//! cursor `{epoch, seq, id}` that the feed's owner validates against its own
//! journal on every exchange; `journal_epoch` changes whenever an event row is
//! deleted (or rewritten), so a cursor never survives a journal that shrank.
//!
//! Per peer `P` (keyed by P's replica id, like `peer_known`):
//!
//! | table | meaning |
//! |---|---|
//! | `feed_in(P)` | our cursor of P's feed, plus P's last reported tip and inbox state |
//! | `feed_out(P, via)` | P's last reported cursor of our feed, per channel: carried by P's requests (`request`) or by P's replies to ours (`reply`) |
//! | `feed_needs(P, id)` | ids P's feed listed (elided or `large`) that we lack, and missing parents we asked P for |
//! | `feed_rejected(P, id)` | events of P's feed we refused, durably (never acked) |
//!
//! **Acks** (invariant 10): `peer_known(P, id)` is written only for an entry
//! of P's committed feed (or an event P served from its committed journal
//! through `need`) whose digest equals our own committed or staged payload,
//! and `outbox.acknowledged` is set only from `peer_known`, for events we have
//! committed. The cursor never acks; a staged or rejected event never acks;
//! an outbox row inserted later (a staged event that commits) is acked by the
//! `feed_outbox_known` trigger when the evidence is already there.
//!
//! **Regressions**: a peer reporting a lower cursor of our (unchanged) feed
//! than it reported before lost state (a restore). We then forget everything
//! we know about it (`peer_known`, recomputing outbox acks: withdrawn events
//! are queued again; its `feed_needs`) and re-read its feed from the start,
//! which rebuilds the evidence and the needs that are still true. A reset
//! page of its feed forgets it the same way. While we re-read, we report *no
//! cursor* (`None` in a request, an empty-epoch cursor in a reply), which the
//! peer treats as invalid (it resets us) and never as a regression of ours.
//!
//! **Digests** are of an event's canonical serialization (`serde_json` of the
//! parsed `Event`): what a receiver re-serializes a payload-bearing entry to,
//! and what every held-payload check accepts. It is the stored text itself
//! for everything `commit` and staging write; a payload stored otherwise (an
//! older build, another tool) still travels and is acked.
//!
//! **Bounds** (invariant 22): every peer-supplied value that is compared or
//! stored is checked first (`check_page`, `well_formed_cursor`); ids a peer
//! reports `unknown` only touch needs we hold or missing parents of waiting
//! rows, and expired `parent` marks are dropped.
//!
//! A report is compared only with the previous report of the same channel.
//! Each channel is sequential (one exchange per peer and repository in each
//! direction), so its reports only grow while the peer keeps its state; but
//! under mutual pairing a request can carry a cursor read before the peer
//! applied our latest reply, which would look lower than the reply-borne one.
//! Resets restart both channels: a reset we send (or accept) is followed by
//! low reports by design.
use super::derive::causal_order;
use super::inbox::{StageOutcome, StagedEvent};
use super::*;
use crate::model::errors::{ErrorWire, classify};
use crate::model::health::{InboxCounts, PeerFeedCounters};
use crate::model::protocol::feed::{
    self, COUNT_CAP, Cursor, DIGEST_HEX_LEN, ExchangeSummary, FEED_FORMAT, FeedApplyResult,
    FeedEntry, FeedPage, FeedState, GateView, LARGE_EVENT_BYTES, MAX_NEED, MAX_NEED_BYTES,
    MAX_PAGE_BYTES, MAX_PAGE_ENTRIES, MAX_REJECTED_SHOWN, MetaExchangeReply, MetaExchangeRequest,
    PageStatus, RejectedEvent,
};
use crate::paired_sync::SyncRoute;
use crate::util::lock::lock;
use std::cell::Cell;
use std::collections::VecDeque;
use std::sync::Mutex;

/// Sharing cache: counters of the feed paths (derived, disposable).
#[derive(Default)]
pub(crate) struct Cache {
    /// Event payload rows read to build feed pages or serve needs.
    payload_reads: Cell<u64>,
}

/// `schema:sharing` stamp.
const SCHEMA_VERSION: i64 = 1;
/// An id a peer reported unknown is asked for again after this long.
const NEED_RETRY_MS: i64 = 60_000;
/// `feed_needs.reason` of ids listed by the peer's feed (elided entries we do
/// not hold, `large` entries); these keep the peer's metadata not caught up
/// until they arrive or the peer reports them `unknown`.
const NEED_LISTED: [&str; 2] = ["elided", "large"];
/// `feed_needs.reason` of missing parents of waiting rows.
const NEED_PARENT: &str = "parent";
/// `feed_out.via`: a cursor carried by the peer's request.
const VIA_REQUEST: &str = "request";
/// `feed_out.via`: a cursor carried by the peer's reply to our request.
const VIA_REPLY: &str = "reply";
/// Hex characters of a feed epoch (`journal_epoch`).
const EPOCH_HEX_LEN: usize = 32;
/// Longest event, replica or cursor id accepted from a peer.
const MAX_ID_BYTES: usize = 128;
/// Offers whose sent counts wait for their `feed-apply` (oldest dropped).
const OFFERS_KEPT: usize = 1024;

/// What one `build_exchange_request` sent, until `apply_exchange_reply`
/// folds it into the dialer's summary. Process-wide: the offer and the apply
/// of one exchange may run on different pooled stores of the worker.
struct Offer {
    request: String,
    repo: String,
    peer: String,
    sent: Sent,
}

static OFFERS: Mutex<VecDeque<Offer>> = Mutex::new(VecDeque::new());

fn remember_offer(offer: Offer) {
    let mut offers = lock(&OFFERS);
    while offers.len() >= OFFERS_KEPT {
        offers.pop_front();
    }
    offers.push_back(offer);
}

/// What the request `request` of `repo` to `peer` sent (nothing when this
/// process did not build it, e.g. across a worker restart).
fn take_offer(request: &str, repo: &str, peer: &str) -> Sent {
    let mut offers = lock(&OFFERS);
    let offer = offers
        .iter()
        .position(|offer| offer.request == request)
        .and_then(|position| offers.remove(position));
    match offer {
        Some(offer) if offer.repo == repo && offer.peer == peer => offer.sent,
        _ => Sent::default(),
    }
}

/// `sharing::ensure_schema` (`schema:sharing=1`, REVAMP_PLAN §9, WP2.5 task 1):
/// the `journal_epoch` config row and the triggers that regenerate it when the
/// journal loses or rewrites a row; the per-peer feed tables; indexes on
/// `incoming_sources(peer)` (per-peer inbox counts) and `feed_needs(event)`
/// (an event that arrives clears every peer's need of it); and the
/// `feed_outbox_known` trigger, which acks an outbox row as it is inserted
/// when a feed peer is already known to hold the event (point lookups per
/// feed peer: no index on `peer_known(event)` to maintain per received
/// event). Only new objects: no table changes shape. Idempotent; runs in its
/// own savepoint.
pub(super) fn ensure_schema(store: &mut Store) -> Result<()> {
    let db = &store.db;
    db.execute_batch("SAVEPOINT sharing_schema")?;
    let result = (|| -> Result<()> {
        db.execute_batch(
            "INSERT OR IGNORE INTO config(key,value) VALUES('journal_epoch',lower(hex(randomblob(16))));
            CREATE TRIGGER IF NOT EXISTS feed_epoch_delete AFTER DELETE ON events BEGIN
                INSERT OR REPLACE INTO config(key,value) VALUES('journal_epoch',lower(hex(randomblob(16))));
            END;
            CREATE TRIGGER IF NOT EXISTS feed_epoch_update AFTER UPDATE ON events BEGIN
                INSERT OR REPLACE INTO config(key,value) VALUES('journal_epoch',lower(hex(randomblob(16))));
            END;
            CREATE TABLE IF NOT EXISTS feed_in(peer TEXT PRIMARY KEY,epoch TEXT NOT NULL,seq INTEGER NOT NULL,id TEXT NOT NULL,
                tip INTEGER NOT NULL,updated_ms INTEGER NOT NULL,pending_active INTEGER NOT NULL DEFAULT 0,
                blocked INTEGER NOT NULL DEFAULT 0,quarantined INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS feed_out(peer TEXT NOT NULL,via TEXT NOT NULL,epoch TEXT NOT NULL,seq INTEGER NOT NULL,
                id TEXT NOT NULL,reported_ms INTEGER NOT NULL,PRIMARY KEY(peer,via));
            CREATE TABLE IF NOT EXISTS feed_needs(peer TEXT NOT NULL,event TEXT NOT NULL,reason TEXT NOT NULL,
                requested_ms INTEGER NOT NULL,missing_at INTEGER,seq INTEGER NOT NULL DEFAULT 0,digest TEXT,
                PRIMARY KEY(peer,event));
            CREATE TABLE IF NOT EXISTS feed_rejected(peer TEXT NOT NULL,event TEXT NOT NULL,seq INTEGER NOT NULL,
                code TEXT NOT NULL,message TEXT NOT NULL,payload TEXT,at_ms INTEGER NOT NULL,PRIMARY KEY(peer,event));
            CREATE INDEX IF NOT EXISTS feed_needs_event ON feed_needs(event);
            CREATE INDEX IF NOT EXISTS incoming_sources_peer ON incoming_sources(peer);
            CREATE TRIGGER IF NOT EXISTS feed_outbox_known AFTER INSERT ON outbox
                WHEN NEW.acknowledged=0 AND EXISTS(SELECT 1 FROM peer_known k WHERE k.peer IN (SELECT peer FROM feed_in) AND k.event=NEW.event) BEGIN
                UPDATE outbox SET acknowledged=1 WHERE event=NEW.event;
            END;",
        )?;
        db.execute(
            "INSERT INTO config(key,value) VALUES('schema:sharing',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value WHERE CAST(config.value AS INTEGER)<?1",
            [SCHEMA_VERSION],
        )?;
        Ok(())
    })();
    match result {
        Ok(()) => db.execute_batch("RELEASE sharing_schema")?,
        Err(error) => {
            let _ = db.execute_batch("ROLLBACK TO sharing_schema; RELEASE sharing_schema");
            return Err(error);
        }
    }
    Ok(())
}

impl Store {
    pub fn known_by_peer(&self, peer: &str) -> Result<BTreeSet<String>> {
        Ok(self
            .db
            .prepare("SELECT event FROM peer_known WHERE peer=?")?
            .query_map([peer], |r| r.get(0))?
            .collect::<std::result::Result<_, _>>()?)
    }
    pub fn shared_event_inventory(&self) -> Result<BTreeSet<String>> {
        Ok(self
            .db
            .prepare("SELECT e.id FROM events e JOIN branches b ON b.id=e.branch WHERE b.shared=1")?
            .query_map([], |r| r.get(0))?
            .collect::<std::result::Result<_, _>>()?)
    }
    /// The configured authenticated peer advertises accepted, durable events,
    /// not staged manifests or object possession. Record this current receipt
    /// snapshot and reconcile the two-device outbox in the same transaction.
    /// A withdrawn event is queued again (for example after a peer restore).
    pub fn remember_peer_inventory(&mut self, peer: &str, known: &BTreeSet<String>) -> Result<()> {
        let tx = self.db.savepoint()?;
        tx.execute("DELETE FROM peer_known WHERE peer=?", [peer])?;
        for event in known {
            tx.execute("INSERT INTO peer_known VALUES(?,?)", params![peer, event])?;
        }
        tx.execute("UPDATE outbox SET acknowledged=EXISTS(SELECT 1 FROM peer_known p WHERE p.peer=? AND p.event=outbox.event)
            WHERE event IN (SELECT e.id FROM events e JOIN branches b ON b.id=e.branch WHERE b.shared=1)", [peer])?;
        tx.commit()?;
        Ok(())
    }
    pub fn shared_bundle(&self, known: &BTreeSet<String>) -> Result<Bundle> {
        let events: Vec<_> = self
            .events()?
            .into_iter()
            .filter(|e| e.branch.shared && !known.contains(&e.id))
            .collect();
        // Authorization starts with shared semantic events, never the object dir.
        let hashes: BTreeSet<_> = events.iter().flat_map(Event::objects).collect();
        let available = self.shared_object_inventory()?;
        let objects = hashes
            .into_iter()
            .filter(|h| available.contains(h))
            .map(|h| Ok((h.clone(), hex::encode(self.read_object(&h)?))))
            .collect::<Result<_>>()?;
        Ok(Bundle {
            repo: self.repo.clone(),
            branches: self.branches()?.into_iter().filter(|b| b.shared).collect(),
            events,
            objects,
        })
    }
    pub fn known_objects_by_peer(&self, peer: &str) -> Result<BTreeSet<String>> {
        Ok(self
            .db
            .prepare("SELECT hash FROM peer_objects WHERE peer=?")?
            .query_map([peer], |r| r.get(0))?
            .collect::<std::result::Result<_, _>>()?)
    }
    pub fn remember_peer_objects(&mut self, peer: &str, objects: &BTreeSet<String>) -> Result<()> {
        let tx = self.db.savepoint()?;
        tx.execute("DELETE FROM peer_objects WHERE peer=?", [peer])?;
        for h in objects {
            tx.execute("INSERT INTO peer_objects VALUES(?,?)", params![peer, h])?;
        }
        tx.commit()?;
        Ok(())
    }
    /// Manifests and objects can cross separately. A large atomic root remains
    /// visible after causal validation; content availability is tracked separately.
    pub fn shared_page(
        &self,
        known: &BTreeSet<String>,
        known_objects: &BTreeSet<String>,
        budget: usize,
    ) -> Result<Bundle> {
        self.shared_page_content(known, known_objects, budget, true)
    }
    /// Event manifests without reading CAS content. Reserved network lanes must
    /// never build bulk payloads only to discard them afterwards.
    pub fn shared_metadata_page(&self, known: &BTreeSet<String>, budget: usize) -> Result<Bundle> {
        self.shared_page_content(known, &BTreeSet::new(), budget, false)
    }
    fn shared_page_content(
        &self,
        known: &BTreeSet<String>,
        known_objects: &BTreeSet<String>,
        budget: usize,
        include_objects: bool,
    ) -> Result<Bundle> {
        let branches: Vec<_> = self.branches()?.into_iter().filter(|b| b.shared).collect();
        let mut used = serde_json::to_vec(&branches)?.len() + 1024;
        let mut events = vec![];
        // Inventory acknowledges committed events only. Sending a UUID-sorted
        // prefix can fill every page with children whose parents never fit, so
        // the receiver cannot ACK and the same prefix repeats forever.
        let journal = self.events()?;
        for e in causal_order(&journal)
            .into_iter()
            .filter(|e| e.branch.shared && !known.contains(&e.id))
        {
            let size = serde_json::to_vec(&e)?.len() + 1;
            ensure!(size < budget / 3, "EVENT_METADATA_TOO_LARGE");
            if used + size > budget / 3 {
                break;
            }
            used += size;
            events.push(e.clone());
        }
        let available = if include_objects {
            self.shared_object_inventory()?
        } else {
            BTreeSet::new()
        };
        let hashes: BTreeSet<_> = events.iter().flat_map(Event::objects).collect();
        let mut objects = BTreeMap::new();
        for h in hashes
            .into_iter()
            .filter(|h| include_objects && available.contains(h) && !known_objects.contains(h))
        {
            let length = self.object_size(&h)?;
            if length > MAX_FILE as u64 || used + length as usize * 2 + h.len() + 8 > budget {
                continue;
            }
            let bytes = self.read_object(&h)?;
            let size = bytes.len() * 2 + h.len() + 8;
            if used + size > budget {
                continue;
            }
            used += size;
            objects.insert(h, hex::encode(bytes));
        }
        let bundle = Bundle {
            repo: self.repo.clone(),
            branches,
            events,
            objects,
        };
        ensure!(
            serde_json::to_vec(&bundle)?.len() <= budget,
            "BUNDLE_TOO_LARGE"
        );
        Ok(bundle)
    }
    pub fn acknowledge(&self, events: &[String]) -> Result<()> {
        for rev in events {
            self.db.execute(
                "UPDATE outbox SET acknowledged=1 WHERE event=? AND acknowledged=0 AND event IN
                    (SELECT e.id FROM events e JOIN branches b ON b.id=e.branch WHERE b.shared=1)",
                [rev],
            )?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Metadata feed, format 7 (REVAMP_PLAN §6.3; WP2.5)

/// The shared feed in seq order. `CROSS JOIN` pins `journal_sequence` as the
/// outer loop, so a page is a rowid range scan with point lookups.
const PAGE_SQL: &str =
    "SELECT s.seq,e.id,e.payload FROM journal_sequence s CROSS JOIN events e ON e.id=s.event
    CROSS JOIN branches b ON b.id=e.branch WHERE s.seq>?1 AND b.shared=1 ORDER BY s.seq LIMIT ?2";
/// The feed tip: a reverse rowid scan that stops at the first shared event.
const TIP_SQL: &str = "SELECT s.seq FROM journal_sequence s CROSS JOIN events e ON e.id=s.event
    CROSS JOIN branches b ON b.id=e.branch WHERE b.shared=1 ORDER BY s.seq DESC LIMIT 1";
const ID_AT_SQL: &str = "SELECT e.id FROM journal_sequence s CROSS JOIN events e ON e.id=s.event
    CROSS JOIN branches b ON b.id=e.branch WHERE s.seq=?1 AND b.shared=1";
/// Shared entries after `?1`, capped at `?2`.
const SHARED_AFTER_SQL: &str =
    "SELECT count(*) FROM (SELECT 1 FROM journal_sequence s CROSS JOIN events e ON e.id=s.event
    CROSS JOIN branches b ON b.id=e.branch WHERE s.seq>?1 AND b.shared=1 LIMIT ?2)";
/// Journal rows after `?1`, capped at `?2`: a rowid range count (every row
/// is a shared entry when the replica has no private branch).
const ROWS_AFTER_SQL: &str =
    "SELECT count(*) FROM (SELECT 1 FROM journal_sequence WHERE seq>?1 LIMIT ?2)";
/// Missing parents among the oldest `?1` waits (rowid order), at most `?2`.
/// A wait on a missing parent never resolves, so it ages to the front; waits
/// on staged parents resolve as activation proceeds. Bounded per call.
const MISSING_PARENTS_SQL: &str = "SELECT DISTINCT w.parent FROM (SELECT child,parent FROM incoming_waits ORDER BY rowid LIMIT ?1) w
    JOIN incoming_events c ON c.id=w.child AND c.state='waiting'
    WHERE NOT EXISTS(SELECT 1 FROM incoming_events i WHERE i.id=w.parent)
    AND NOT EXISTS(SELECT 1 FROM events e WHERE e.id=w.parent) ORDER BY w.parent LIMIT ?2";
/// Waits one need list looks at for missing parents.
const MISSING_SCAN: usize = 4096;
/// Approximate JSON bytes of one entry without its payload (seq, id, digest).
const ENTRY_OVERHEAD: u64 = 96;
/// Approximate JSON bytes of an exchange message besides its entries.
const MESSAGE_OVERHEAD: u64 = 512;

fn now_ms() -> i64 {
    i64::try_from(time()).unwrap_or(i64::MAX)
}

fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= MAX_ID_BYTES
}

/// `len` lowercase hex characters.
fn lower_hex(text: &str, len: usize) -> bool {
    text.len() == len
        && text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn valid_digest(digest: &str) -> bool {
    lower_hex(digest, DIGEST_HEX_LEN)
}

fn valid_epoch(epoch: &str) -> bool {
    lower_hex(epoch, EPOCH_HEX_LEN)
}

/// A peer-supplied cursor is bounded before it is compared or stored
/// (invariant 22): a feed epoch, or empty (the peer has no record, or is
/// re-reading our feed); an id of at most `MAX_ID_BYTES` (empty at the
/// start); a seq that is not negative.
fn well_formed_cursor(cursor: &Cursor) -> bool {
    (cursor.epoch.is_empty() || valid_epoch(&cursor.epoch))
        && cursor.id.len() <= MAX_ID_BYTES
        && cursor.seq >= 0
}

/// A stored payload as a feed entry carries it: the parsed event (`None`
/// when the text does not parse), the digest of its canonical serialization
/// and that serialization's length (what the receiver re-serializes and
/// checks). For text `commit` or staging wrote, this is the text itself.
fn canonical(payload: &str) -> Result<(Option<Event>, String, usize)> {
    let Ok(event) = serde_json::from_str::<Event>(payload) else {
        return Ok((None, feed::digest(payload), payload.len()));
    };
    let text = serde_json::to_string(&event)?;
    Ok((Some(event), feed::digest(&text), text.len()))
}

fn capped(n: i64) -> u64 {
    u64::try_from(n.max(0)).unwrap_or(0).min(COUNT_CAP)
}

/// Whether a stored payload has `digest`: of its text (how `commit` and
/// staging store an event), or else of its canonical re-serialization (a
/// payload written by another tool).
fn payload_matches(stored: &str, digest: &str) -> bool {
    feed::digest(stored) == digest
        || serde_json::from_str::<Event>(stored)
            .ok()
            .and_then(|event| serde_json::to_string(&event).ok())
            .is_some_and(|text| feed::digest(&text) == digest)
}

/// One `feed_in` row: our cursor of a peer's feed plus what the peer last
/// reported about itself (its tip and inbox state).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct InRow {
    cursor: Cursor,
    tip: i64,
    pending_active: u64,
    blocked: u64,
    quarantined: u64,
    updated_ms: u64,
}

impl InRow {
    fn same_as(&self, other: &InRow) -> bool {
        (
            &self.cursor,
            self.tip,
            self.pending_active,
            self.blocked,
            self.quarantined,
        ) == (
            &other.cursor,
            other.tip,
            other.pending_active,
            other.blocked,
            other.quarantined,
        )
    }
}

/// A payload we hold: committed, or staged in `incoming_events`.
struct Held {
    payload: String,
    committed: bool,
}

/// One feed entry (or one event a peer served through `need`) to judge.
struct Item<'a> {
    id: String,
    seq: i64,
    /// The digest the peer's feed claims; `None` for a served event that no
    /// entry listed (a missing parent): its own payload is then the claim.
    digest: Option<String>,
    event: Option<&'a Event>,
    large: Option<u64>,
}

/// What to do with one item.
enum Verdict {
    /// We hold it with an equal digest.
    Known {
        committed: bool,
    },
    Stage {
        event: Event,
        text: String,
    },
    /// We lack it: ask the peer through `need`.
    Need(&'static str),
    Reject(ErrorWire, Option<String>),
}

/// Lookups repeated across the events of one message (object sizes stat
/// the CAS; consecutive events mostly share their objects and branch).
#[derive(Default)]
struct Lookups {
    sizes: BTreeMap<String, Option<u64>>,
    branches: BTreeMap<String, Option<Branch>>,
}

/// What receiving a page (and served events) did.
#[derive(Debug, Default)]
struct Received {
    /// Payload-bearing entries and served events received.
    events: u32,
    staged: u32,
    acked: u32,
    rejected: Vec<RejectedEvent>,
    /// Approximate JSON bytes received.
    bytes: u64,
}

/// What we send in this exchange (for the summary).
#[derive(Debug, Default)]
struct Sent {
    events: u32,
    bytes: u64,
}

/// Our view of one peer's feed, for summaries, counters and the gate.
struct PeerView {
    row: Option<InRow>,
    out: Option<Cursor>,
    /// Inbox rows staged from this peer, by state.
    source: InboxCounts,
    rejected: u64,
    /// Ids this peer's feed listed that we still lack, except those the peer
    /// since reported `unknown` (it no longer has them to give; they are
    /// asked again after `NEED_RETRY_MS`).
    needs: u64,
    /// Entries of our feed after the peer's cursor of it: what the peer has
    /// still to read, elided or not (exact, capped at `COUNT_CAP`).
    to_send: u64,
    local_tip: i64,
}

impl PeerView {
    fn cursor_in(&self) -> i64 {
        self.row.as_ref().map_or(0, |row| row.cursor.seq)
    }
    fn remote_tip(&self) -> i64 {
        self.row.as_ref().map_or(0, |row| row.tip)
    }
    fn remote_pending(&self) -> u64 {
        self.row.as_ref().map_or(0, |row| row.pending_active)
    }
    /// Entries of the peer's feed after our cursor (an upper bound when the
    /// peer's feed has gaps: private events and deleted rows hold seqs).
    fn remaining(&self) -> u64 {
        capped(self.remote_tip().saturating_sub(self.cursor_in()))
    }
    fn to_receive(&self) -> u64 {
        self.remaining().saturating_add(self.needs).min(COUNT_CAP)
    }
    /// §6.3.6 `metadata.caught_up`: our cursor is at the peer's tip, no inbox
    /// row sourced from the peer is still pending, and nothing its feed
    /// listed is still missing here (and still to be had from it).
    fn metadata_caught_up(&self) -> bool {
        self.row.is_some()
            && self.remaining() == 0
            && self.source.pending_active() == 0
            && self.needs == 0
    }
    /// §6.3.6 gate view: metadata caught up, nothing from the peer blocked,
    /// quarantined or rejected, and the peer has nothing left to activate.
    fn gate(&self) -> GateView {
        let blocked = self.source.blocked > 0 || self.source.quarantined > 0 || self.rejected > 0;
        let plural =
            |n: u64, one: &str, many: &str| format!("{n} {}", if n == 1 { one } else { many });
        let events = |n: u64| plural(n, "event", "events");
        let unactivated = self
            .source
            .ready
            .saturating_add(self.source.retry)
            .saturating_add(self.source.unclassified);
        let mut waiting_on = vec![];
        if self.row.is_none() {
            waiting_on.push("no metadata exchange with the source yet".to_owned());
        }
        for (count, text) in [
            (self.remaining(), "remaining at source"),
            (self.needs, "requested from source"),
            (self.source.waiting, "waiting for parents"),
            (unactivated, "not yet activated"),
        ] {
            if count > 0 {
                waiting_on.push(format!("{} {text}", events(count)));
            }
        }
        if self.remote_pending() > 0 {
            waiting_on.push(format!(
                "source has {}",
                plural(
                    self.remote_pending(),
                    "unactivated event",
                    "unactivated events"
                )
            ));
        }
        for (count, text) in [
            (self.source.quarantined, "quarantined"),
            (self.source.blocked, "blocked by a quarantined ancestor"),
            (self.rejected, "rejected"),
        ] {
            if count > 0 {
                waiting_on.push(format!("{} {text}", events(count)));
            }
        }
        GateView {
            caught_up: self.metadata_caught_up() && !blocked && self.remote_pending() == 0,
            waiting_on,
            blocked,
        }
    }
}

impl Store {
    /// The feed epoch (`config.journal_epoch`, 32 hex characters).
    pub fn feed_epoch(&self) -> Result<String> {
        self.db
            .prepare_cached("SELECT value FROM config WHERE key='journal_epoch'")?
            .query_row([], |r| r.get(0))
            .optional()?
            .context("INVALID_STORE_METADATA: journal_epoch missing")
    }
    /// The feed tip: the `seq` of the newest committed shared event (0 when
    /// there is none). Private events never move it.
    pub fn feed_tip(&self) -> Result<i64> {
        Ok(self
            .db
            .prepare_cached(TIP_SQL)?
            .query_row([], |r| r.get(0))
            .optional()?
            .unwrap_or(0))
    }
    /// The id of the shared event at `seq` (`None` for a private event, a
    /// deleted row or a seq past the tip).
    pub fn journal_id_at(&self, seq: i64) -> Result<Option<String>> {
        Ok(self
            .db
            .prepare_cached(ID_AT_SQL)?
            .query_row([seq], |r| r.get(0))
            .optional()?)
    }
    /// §6.3.1: `epoch == ours && seq ≤ tip && (seq == 0 || journal_id_at(seq) == id)`.
    pub fn validate_cursor(&self, c: &Cursor) -> Result<bool> {
        if c.epoch != self.feed_epoch()? || c.seq < 0 {
            return Ok(false);
        }
        if c.seq == 0 {
            return Ok(true);
        }
        if c.seq > self.feed_tip()? {
            return Ok(false);
        }
        Ok(self.journal_id_at(c.seq)?.as_deref() == Some(c.id.as_str()))
    }
    /// Our feed identity and inbox state, as sent in every exchange.
    pub fn feed_state(&self) -> Result<FeedState> {
        let counts = self.inbox_counts()?;
        Ok(FeedState {
            epoch: self.feed_epoch()?,
            tip: self.feed_tip()?,
            pending_active: counts.pending_active(),
            blocked: counts.blocked,
            quarantined: counts.quarantined,
        })
    }
    /// A page of our feed for `for_peer` (§6.3.3): `after: None` is a probe
    /// (no entries); an invalid cursor gives a reset page from the start.
    /// Entries `for_peer` is known to hold are elided; events over
    /// `LARGE_EVENT_BYTES` are flagged `large` (served through `need`). At
    /// most `max_entries` entries and `max_bytes` of payload, but a page
    /// always carries its first payload.
    pub fn feed_page(
        &self,
        after: Option<&Cursor>,
        for_peer: &str,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<FeedPage> {
        self.feed_read(|| self.page_after(after, for_peer, max_entries, max_bytes, false))
    }
    /// `feed-offer` (§6.3.5 step 1): our page from `feed_out(peer)`, our
    /// cursor of the peer's feed, the ids we need from it and our state.
    ///
    /// A request page carries a large event inline, as its last payload: the
    /// acceptor can never ask the dialer for an event, since a reply has no
    /// `need` (CCR-WP2.5-2).
    pub fn build_exchange_request(&self, route: &SyncRoute) -> Result<MetaExchangeRequest> {
        self.check_route(route)?;
        let peer = route.remote_replica.as_str();
        let request = self.feed_read(|| {
            let out = self.out_row(peer)?;
            let page =
                self.page_after(out.as_ref(), peer, MAX_PAGE_ENTRIES, MAX_PAGE_BYTES, true)?;
            Ok(MetaExchangeRequest {
                format: FEED_FORMAT,
                request: id(),
                sender: self.device.clone(),
                receiver: peer.to_owned(),
                repo: self.repo.clone(),
                page,
                cursor: self.in_row(peer)?.map(|row| row.cursor),
                need: self.need_list(peer)?,
                state: self.feed_state()?,
            })
        })?;
        // The dialer's `feed-apply` summary reports what this request sent.
        let mut sent = page_sent(&request.page);
        sent.bytes = request.need.iter().fold(sent.bytes, |bytes, id| {
            bytes.saturating_add(id.len() as u64 + 3)
        });
        remember_offer(Offer {
            request: request.request.clone(),
            repo: self.repo.clone(),
            peer: peer.to_owned(),
            sent,
        });
        Ok(request)
    }
    /// `feed-exchange` (§6.3.5 step 2), on the acceptor: stage the request
    /// page (§6.3.4), record the dialer's cursor of our feed (regression
    /// check), then reply with our page after that cursor, the served needs
    /// and our own view of the dialer (inbound gate evidence).
    pub fn handle_exchange_request(
        &self,
        route: &SyncRoute,
        req: &MetaExchangeRequest,
    ) -> Result<(MetaExchangeReply, FeedApplyResult)> {
        self.check_route(route)?;
        ensure!(
            req.format == FEED_FORMAT,
            "INCOMPATIBLE_PEER_PROTOCOL: feed format {}",
            req.format
        );
        ensure!(req.repo == route.repo, "REPOSITORY_MISMATCH");
        ensure!(
            req.sender == route.remote_replica && req.receiver == route.local_replica,
            "REMOTE_REPLICA_BINDING_MISMATCH"
        );
        uuid::Uuid::parse_str(&req.request).context("INVALID_SYNC_REQUEST_ID")?;
        ensure!(
            req.need.len() <= MAX_NEED,
            "INVENTORY_TOO_LARGE: {} needed ids",
            req.need.len()
        );
        ensure!(
            req.need.iter().all(|id| valid_id(id)),
            "PROTOCOL_VIOLATION: malformed need id"
        );
        ensure!(
            req.cursor.as_ref().is_none_or(well_formed_cursor),
            "PROTOCOL_VIOLATION: malformed feed cursor"
        );
        let peer = route.remote_replica.as_str();
        let before = self.feed_read(|| self.marks(peer))?;
        let (status, mut received) = self.feed_write(|| {
            let (status, received) = self.receive_page(peer, &req.page, &req.state)?;
            // Our reply page resets when the dialer's cursor of our feed is
            // invalid: it restarts from the start, which is all we know of it.
            let reset = match &req.cursor {
                Some(cursor) => !self.validate_cursor(cursor)?,
                None => false,
            };
            let report = if reset {
                Some(Cursor::start(&self.feed_epoch()?))
            } else {
                req.cursor.clone()
            };
            self.observe_report(peer, VIA_REQUEST, report.as_ref(), reset)?;
            Ok((status, received))
        })?;
        received.bytes = received
            .bytes
            .saturating_add(ENTRY_OVERHEAD * req.page.entries.len() as u64 + MESSAGE_OVERHEAD);
        self.feed_read(|| {
            let epoch = self.feed_epoch()?;
            let after = req.cursor.clone().unwrap_or_else(|| Cursor::start(&epoch));
            let page =
                self.page_after(Some(&after), peer, MAX_PAGE_ENTRIES, MAX_PAGE_BYTES, false)?;
            let (need_served, unknown) = self.events_by_ids(&req.need, MAX_NEED_BYTES)?;
            let cursor = self.in_row(peer)?.map(|row| row.cursor).unwrap_or_default();
            let mut sent = page_sent(&page);
            for event in &need_served {
                sent.events += 1;
                sent.bytes += serde_json::to_vec(event)?.len() as u64;
            }
            let local = self.apply_result(peer, &received, &sent, before)?;
            let page_status = match status {
                // Our cursor of the dialer's feed as recorded now (a
                // regression found in this exchange deletes it).
                PageStatus::CursorMismatch { .. } => PageStatus::CursorMismatch {
                    expected: cursor.clone(),
                },
                status => status,
            };
            let reply = MetaExchangeReply {
                format: FEED_FORMAT,
                reply_to: req.request.clone(),
                sender: self.device.clone(),
                receiver: peer.to_owned(),
                repo: self.repo.clone(),
                page_status,
                cursor,
                page,
                staged: received.staged,
                rejected: received
                    .rejected
                    .iter()
                    .take(MAX_REJECTED_SHOWN)
                    .cloned()
                    .collect(),
                need_served,
                unknown,
                state: self.feed_state()?,
            };
            Ok((reply, local))
        })
    }
    /// `feed-apply` (§6.3.5 step 3), on the dialer: the reply must answer
    /// `request_id` (`UNBOUND_PEER_RESPONSE`); stage its page and served
    /// needs, mark unknown ids, and record the acceptor's cursor of our feed
    /// (regression check; an invalid cursor makes the next page a reset).
    pub fn apply_exchange_reply(
        &self,
        route: &SyncRoute,
        request_id: &str,
        reply: &MetaExchangeReply,
    ) -> Result<FeedApplyResult> {
        self.check_route(route)?;
        uuid::Uuid::parse_str(request_id).context("INVALID_SYNC_REQUEST_ID")?;
        ensure!(reply.reply_to == request_id, "UNBOUND_PEER_RESPONSE");
        ensure!(
            reply.format == FEED_FORMAT,
            "INCOMPATIBLE_PEER_PROTOCOL: feed format {}",
            reply.format
        );
        ensure!(reply.repo == route.repo, "REPOSITORY_MISMATCH");
        ensure!(
            reply.sender == route.remote_replica && reply.receiver == route.local_replica,
            "REMOTE_REPLICA_BINDING_MISMATCH"
        );
        ensure!(
            reply.need_served.len() <= MAX_NEED
                && reply.unknown.len() <= MAX_NEED
                && reply.rejected.len() <= MAX_REJECTED_SHOWN,
            "INVENTORY_TOO_LARGE: feed reply lists"
        );
        ensure!(
            reply.unknown.iter().all(|id| valid_id(id)),
            "PROTOCOL_VIOLATION: malformed unknown id"
        );
        let expected = match &reply.page_status {
            PageStatus::CursorMismatch { expected } => Some(expected),
            _ => None,
        };
        ensure!(
            well_formed_cursor(&reply.cursor) && expected.is_none_or(well_formed_cursor),
            "PROTOCOL_VIOLATION: malformed feed cursor"
        );
        let peer = route.remote_replica.as_str();
        let before = self.feed_read(|| self.marks(peer))?;
        let mut received = self.feed_write(|| {
            let (_, mut received) = self.receive_page(peer, &reply.page, &reply.state)?;
            self.receive_served(peer, &reply.need_served, &mut received)?;
            self.mark_unknown(peer, &reply.unknown)?;
            let report = match &reply.page_status {
                PageStatus::CursorMismatch { expected } => expected,
                _ => &reply.cursor,
            };
            // `Reset`: the acceptor took our reset page, so it restarts low.
            let restart = reply.page_status == PageStatus::Reset;
            self.observe_report(peer, VIA_REPLY, Some(report), restart)?;
            Ok(received)
        })?;
        received.bytes = received
            .bytes
            .saturating_add(ENTRY_OVERHEAD * reply.page.entries.len() as u64 + MESSAGE_OVERHEAD);
        // What our request sent (recorded by `build_exchange_request`).
        let sent = take_offer(request_id, &self.repo, peer);
        self.feed_read(|| self.apply_result(peer, &received, &sent, before))
    }
    /// Per-peer feed counters for the heartbeat (`PeerFeedCounters`).
    /// `last_exchange_ms` is when our record of the peer last changed.
    pub fn peer_feed_counters(&self, peer: &str) -> Result<PeerFeedCounters> {
        self.feed_read(|| {
            let view = self.peer_view(peer)?;
            Ok(PeerFeedCounters {
                replica: peer.to_owned(),
                cursor_seq: view.cursor_in(),
                remote_tip: view.remote_tip(),
                to_receive: view.to_receive(),
                to_send: view.to_send,
                staged_pending: view.source.pending_active(),
                blocked: view.source.blocked,
                quarantined: view.source.quarantined,
                rejected: view.rejected,
                caught_up: view.metadata_caught_up(),
                last_exchange_ms: view.row.as_ref().map(|row| row.updated_ms),
            })
        })
    }
    /// `PeerFeedCounters` from an exchange result (what `peer_feed_counters`
    /// would compute right after it, without counting again).
    pub fn feed_counters_after(
        &self,
        peer: &str,
        result: &FeedApplyResult,
    ) -> Result<PeerFeedCounters> {
        let summary = &result.summary;
        let rejected: i64 = self
            .db
            .prepare_cached("SELECT count(*) FROM feed_rejected WHERE peer=?")?
            .query_row([peer], |r| r.get(0))?;
        Ok(PeerFeedCounters {
            replica: peer.to_owned(),
            cursor_seq: summary.cursor_in,
            remote_tip: summary.remote_tip,
            to_receive: summary.to_receive,
            to_send: summary.to_send,
            staged_pending: summary.local_pending,
            blocked: summary.blocked,
            quarantined: summary.quarantined,
            rejected: capped(rejected),
            caught_up: summary.caught_up,
            last_exchange_ms: self.in_row(peer)?.map(|row| row.updated_ms),
        })
    }
    /// The gate view of one peer as a source (§6.3.6).
    pub fn feed_gate(&self, peer: &str) -> Result<GateView> {
        self.feed_read(|| Ok(self.peer_view(peer)?.gate()))
    }
    /// Committed shared events by id (need serving): served in order while
    /// their payloads fit `max_bytes` (the rest are asked again), and
    /// `unknown` for ids that are not committed shared events here (a
    /// private event is unknown: it never leaves the replica).
    pub fn events_by_ids(
        &self,
        ids: &[String],
        max_bytes: usize,
    ) -> Result<(Vec<Event>, Vec<String>)> {
        let mut served = vec![];
        let mut unknown = vec![];
        let mut used = 0usize;
        let mut seen = BTreeSet::new();
        for id in ids {
            if !seen.insert(id.as_str()) {
                continue;
            }
            let payload: Option<String> = self
                .db
                .prepare_cached(
                    "SELECT e.payload FROM events e JOIN branches b ON b.id=e.branch WHERE e.id=? AND b.shared=1",
                )?
                .query_row([id], |r| r.get(0))
                .optional()?;
            let Some(payload) = payload else {
                unknown.push(id.clone());
                continue;
            };
            self.count_payload_read();
            if used.saturating_add(payload.len()) > max_bytes {
                continue;
            }
            used += payload.len();
            served.push(serde_json::from_str(&payload)?);
        }
        Ok((served, unknown))
    }
    /// Peers with a feed record, sorted.
    pub fn feed_peers(&self) -> Result<Vec<String>> {
        Ok(self
            .db
            .prepare_cached("SELECT peer FROM feed_in UNION SELECT peer FROM feed_out ORDER BY 1")?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }
    /// The durable rejections of `peer`'s feed, oldest first.
    pub fn feed_rejections(&self, peer: &str) -> Result<Vec<RejectedEvent>> {
        let rows: Vec<(String, i64, String)> = self
            .db
            .prepare_cached(
                "SELECT event,seq,code FROM feed_rejected WHERE peer=? ORDER BY at_ms,event",
            )?
            .query_map([peer], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(rows
            .into_iter()
            .map(|(id, seq, code)| RejectedEvent {
                id,
                seq,
                class: classify(&code),
                code,
            })
            .collect())
    }
    /// Event payload rows this handle read to build feed pages or serve
    /// needs (an idle exchange reads none).
    pub fn feed_payload_reads(&self) -> u64 {
        self.caches.sharing.payload_reads.get()
    }

    fn count_payload_read(&self) {
        let reads = &self.caches.sharing.payload_reads;
        reads.set(reads.get().saturating_add(1));
    }
    fn check_route(&self, route: &SyncRoute) -> Result<()> {
        ensure!(route.repo == self.repo, "REPOSITORY_MISMATCH");
        ensure!(
            route.local_replica == self.device,
            "LOCAL_REPLICA_BINDING_MISMATCH"
        );
        ensure!(
            route.local_replica != route.remote_replica,
            "DUPLICATE_REPLICA_IDENTITY"
        );
        ensure!(
            valid_id(&route.remote_replica),
            "REMOTE_REPLICA_BINDING_MISMATCH"
        );
        Ok(())
    }
    /// Run `f` in one read transaction (one snapshot), unless the caller
    /// already holds a transaction.
    fn feed_read<T>(&self, f: impl FnOnce() -> Result<T>) -> Result<T> {
        if !self.db.is_autocommit() {
            return f();
        }
        self.db.execute_batch("BEGIN")?;
        let result = f();
        let end = self
            .db
            .execute_batch(if result.is_ok() { "COMMIT" } else { "ROLLBACK" });
        let value = result?;
        end?;
        Ok(value)
    }
    /// Run `f` in a write transaction: `BEGIN IMMEDIATE` from autocommit
    /// (never a deferred read-then-write upgrade, CCR-WP0.INT-5), else a
    /// savepoint inside the caller's transaction. A transaction that changed
    /// nothing commits without writing (an idle exchange costs no fsync).
    fn feed_write<T>(&self, f: impl FnOnce() -> Result<T>) -> Result<T> {
        let outer = self.db.is_autocommit();
        if outer {
            self.db.execute_batch("BEGIN IMMEDIATE")?;
        }
        if let Err(error) = self.db.execute_batch("SAVEPOINT feed_write") {
            if outer {
                let _ = self.db.execute_batch("ROLLBACK");
            }
            return Err(error.into());
        }
        let result = f().and_then(|value| {
            self.db.execute_batch("RELEASE feed_write")?;
            if outer {
                self.db.execute_batch("COMMIT")?;
            }
            Ok(value)
        });
        if result.is_err() {
            if !outer {
                let _ = self
                    .db
                    .execute_batch("ROLLBACK TO feed_write; RELEASE feed_write");
            } else if !self.db.is_autocommit() {
                let _ = self.db.execute_batch("ROLLBACK");
            }
        }
        result
    }
    /// A page after `after` (see `feed_page`). `inline_large`: a large event
    /// travels inline as the page's last payload (it may exceed the budget).
    fn page_after(
        &self,
        after: Option<&Cursor>,
        peer: &str,
        max_entries: usize,
        max_bytes: usize,
        inline_large: bool,
    ) -> Result<FeedPage> {
        let epoch = self.feed_epoch()?;
        let tip = self.feed_tip()?;
        let Some(after) = after else {
            return Ok(FeedPage {
                through: Cursor::start(&epoch),
                epoch,
                after: None,
                reset: false,
                entries: vec![],
                tip,
                more: tip > 0,
            });
        };
        let reset = !self.validate_cursor(after)?;
        let base = if reset {
            Cursor::start(&epoch)
        } else {
            after.clone()
        };
        let mut entries = vec![];
        let mut through = base.clone();
        let mut used = 0usize;
        let mut statement = self.db.prepare_cached(PAGE_SQL)?;
        let mut rows = statement.query(params![base.seq, max_entries.max(1) as i64])?;
        while let Some(row) = rows.next()? {
            let seq: i64 = row.get(0)?;
            let id: String = row.get(1)?;
            let payload: String = row.get(2)?;
            self.count_payload_read();
            // The digest (and size) of the canonical serialization, which
            // receivers check (module docs).
            let (event, digest, len) = canonical(&payload)?;
            let mut entry = FeedEntry {
                seq,
                digest,
                id,
                event: None,
                large: None,
            };
            let mut last = false;
            if !self.peer_knows(peer, &entry.id)? {
                if len > LARGE_EVENT_BYTES && !inline_large {
                    entry.large = Some(len as u64);
                } else {
                    // A page always carries its first payload.
                    if used > 0 && (used.saturating_add(len) > max_bytes || len > LARGE_EVENT_BYTES)
                    {
                        break;
                    }
                    used += len;
                    last = len > LARGE_EVENT_BYTES;
                    entry.event = Some(event.with_context(|| {
                        format!("INVALID_STORE_METADATA: unreadable payload of {}", entry.id)
                    })?);
                }
            }
            through = Cursor {
                epoch: epoch.clone(),
                seq: entry.seq,
                id: entry.id.clone(),
            };
            entries.push(entry);
            if last {
                break;
            }
        }
        Ok(FeedPage {
            more: through.seq < tip,
            after: Some(base),
            reset,
            entries,
            through,
            tip,
            epoch,
        })
    }
    fn peer_knows(&self, peer: &str, id: &str) -> Result<bool> {
        Ok(self
            .db
            .prepare_cached("SELECT EXISTS(SELECT 1 FROM peer_known WHERE peer=? AND event=?)")?
            .query_row(params![peer, id], |r| r.get(0))?)
    }
    fn in_row(&self, peer: &str) -> Result<Option<InRow>> {
        let count = |n: i64| u64::try_from(n).unwrap_or(0);
        Ok(self
            .db
            .prepare_cached(
                "SELECT epoch,seq,id,tip,pending_active,blocked,quarantined,updated_ms FROM feed_in WHERE peer=?",
            )?
            .query_row([peer], |r| {
                Ok(InRow {
                    cursor: Cursor {
                        epoch: r.get(0)?,
                        seq: r.get(1)?,
                        id: r.get(2)?,
                    },
                    tip: r.get(3)?,
                    pending_active: count(r.get(4)?),
                    blocked: count(r.get(5)?),
                    quarantined: count(r.get(6)?),
                    updated_ms: count(r.get(7)?),
                })
            })
            .optional()?)
    }
    /// Store `row` unless it equals `previous` (no write when nothing moved).
    fn put_in_row(&self, peer: &str, previous: Option<&InRow>, row: &InRow) -> Result<()> {
        if previous.is_some_and(|previous| previous.same_as(row)) {
            return Ok(());
        }
        let count = |n: u64| i64::try_from(n).unwrap_or(i64::MAX);
        self.db.execute(
            "INSERT INTO feed_in(peer,epoch,seq,id,tip,updated_ms,pending_active,blocked,quarantined) VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(peer) DO UPDATE SET epoch=excluded.epoch,seq=excluded.seq,id=excluded.id,tip=excluded.tip,
            updated_ms=excluded.updated_ms,pending_active=excluded.pending_active,blocked=excluded.blocked,quarantined=excluded.quarantined",
            params![
                peer,
                row.cursor.epoch,
                row.cursor.seq,
                row.cursor.id,
                row.tip,
                now_ms(),
                count(row.pending_active),
                count(row.blocked),
                count(row.quarantined)
            ],
        )?;
        Ok(())
    }
    /// The peer's last report on one channel (`request` or `reply`).
    fn out_report(&self, peer: &str, via: &str) -> Result<Option<Cursor>> {
        Ok(self
            .db
            .prepare_cached("SELECT epoch,seq,id FROM feed_out WHERE peer=? AND via=?")?
            .query_row(params![peer, via], |r| {
                Ok(Cursor {
                    epoch: r.get(0)?,
                    seq: r.get(1)?,
                    id: r.get(2)?,
                })
            })
            .optional()?)
    }
    /// The cursor our next page for `peer` continues from: the furthest
    /// valid report of either channel; else an invalid one (the page then
    /// resets); `None` when the peer reported nothing (a probe).
    fn out_row(&self, peer: &str) -> Result<Option<Cursor>> {
        let mut best: Option<Cursor> = None;
        let mut invalid = None;
        for via in [VIA_REQUEST, VIA_REPLY] {
            let Some(report) = self.out_report(peer, via)? else {
                continue;
            };
            if !self.validate_cursor(&report)? {
                invalid = Some(report);
            } else if best.as_ref().is_none_or(|best| report.seq > best.seq) {
                best = Some(report);
            }
        }
        Ok(best.or(invalid))
    }
    /// `(cursor_in, cursor_out)` seqs, to tell whether an exchange moved them.
    fn marks(&self, peer: &str) -> Result<(i64, i64)> {
        Ok((
            self.in_row(peer)?.map_or(0, |row| row.cursor.seq),
            self.out_row(peer)?.map_or(0, |cursor| cursor.seq),
        ))
    }
    /// Stage a page of `peer`'s feed (§6.3.4) and advance our cursor of it,
    /// contiguously, in the caller's write transaction. A probe (no `after`)
    /// stages nothing; a page that does not continue from our cursor stages
    /// nothing and answers `CursorMismatch`; a reset page first forgets the
    /// peer (`peer_known`, outbox acks recomputed).
    fn receive_page(
        &self,
        peer: &str,
        page: &FeedPage,
        state: &FeedState,
    ) -> Result<(PageStatus, Received)> {
        check_page(page)?;
        let previous = self.in_row(peer)?;
        let mut row = previous.clone().unwrap_or_else(|| InRow {
            cursor: Cursor::start(&page.epoch),
            ..InRow::default()
        });
        row.tip = page.tip;
        row.pending_active = state.pending_active;
        row.blocked = state.blocked;
        row.quarantined = state.quarantined;
        let mut received = Received::default();
        let Some(after) = &page.after else {
            self.put_in_row(peer, previous.as_ref(), &row)?;
            return Ok((PageStatus::Accepted, received));
        };
        let status = if page.reset {
            self.forget_peer(peer)?;
            PageStatus::Reset
        } else {
            let accepted = match &previous {
                Some(previous) => previous.cursor == *after,
                None => after.is_start(),
            };
            if !accepted {
                if previous.is_some() {
                    self.put_in_row(peer, previous.as_ref(), &row)?;
                }
                let expected = previous.map(|row| row.cursor).unwrap_or_default();
                return Ok((PageStatus::CursorMismatch { expected }, received));
            }
            PageStatus::Accepted
        };
        let items = page
            .entries
            .iter()
            .map(|entry| Item {
                id: entry.id.clone(),
                seq: entry.seq,
                digest: Some(entry.digest.clone()),
                event: entry.event.as_ref(),
                large: entry.large,
            })
            .collect();
        self.receive_items(
            peer,
            items,
            MAX_PAGE_BYTES.saturating_add(MAX_EVENT),
            &mut received,
        )?;
        row.cursor = page.through.clone();
        self.put_in_row(peer, previous.as_ref(), &row)?;
        Ok((status, received))
    }
    /// Stage events `peer` served through `need` (its committed shared
    /// events), checked against the digest its feed listed, if any.
    fn receive_served(&self, peer: &str, events: &[Event], received: &mut Received) -> Result<()> {
        let mut items = vec![];
        for event in events {
            ensure!(
                valid_id(&event.id),
                "PROTOCOL_VIOLATION: malformed served event id"
            );
            let listed: Option<(i64, Option<String>)> = self
                .db
                .prepare_cached("SELECT seq,digest FROM feed_needs WHERE peer=? AND event=?")?
                .query_row(params![peer, event.id], |r| Ok((r.get(0)?, r.get(1)?)))
                .optional()?;
            let (seq, digest) = listed.unwrap_or((0, None));
            items.push(Item {
                id: event.id.clone(),
                seq,
                digest,
                event: Some(event),
                large: None,
            });
        }
        self.receive_items(
            peer,
            items,
            MAX_NEED_BYTES.saturating_add(MAX_EVENT),
            received,
        )
    }
    /// Judge, stage and record items (entries or served events) of `peer`.
    /// Rejections become durable `feed_rejected` rows; held payloads with an
    /// equal digest become `peer_known` evidence (and acks, when committed
    /// here); missing ones become `feed_needs`.
    fn receive_items(
        &self,
        peer: &str,
        items: Vec<Item<'_>>,
        max_bytes: usize,
        received: &mut Received,
    ) -> Result<()> {
        let mut allowed: BTreeSet<String> = items
            .iter()
            .filter_map(|item| item.event.map(|event| event.device.clone()))
            .collect();
        allowed.insert(self.device.clone());
        allowed.insert(peer.to_owned());
        let mut bytes = 0usize;
        let mut stage = vec![];
        let mut texts: BTreeMap<String, (i64, String)> = BTreeMap::new();
        let mut known = vec![];
        let mut rejected = vec![];
        let mut needs = vec![];
        let mut lookups = Lookups::default();
        for item in &items {
            if item.event.is_some() {
                received.events += 1;
            }
            match self.verdict(item, &mut bytes, &mut lookups)? {
                Verdict::Known { committed } => known.push((item.id.clone(), committed)),
                Verdict::Stage { event, text } => {
                    texts.insert(item.id.clone(), (item.seq, text));
                    stage.push(StagedEvent {
                        event,
                        source: Some((peer.to_owned(), item.seq)),
                    });
                }
                Verdict::Need(reason) => {
                    needs.push((item.id.clone(), item.seq, reason, item.digest.clone()))
                }
                Verdict::Reject(error, payload) => {
                    rejected.push((item.id.clone(), item.seq, error, payload))
                }
            }
        }
        ensure!(
            bytes <= max_bytes,
            "INVENTORY_TOO_LARGE: {bytes} payload bytes in one feed message"
        );
        received.bytes = received.bytes.saturating_add(bytes as u64);
        let outcome = if stage.is_empty() {
            StageOutcome::default()
        } else {
            self.stage_incoming(stage, &allowed)?
        };
        received.staged += outcome.staged.len() as u32;
        known.extend(outcome.staged.into_iter().map(|id| (id, false)));
        known.extend(outcome.already_staged.into_iter().map(|id| (id, false)));
        known.extend(outcome.already_committed.into_iter().map(|id| (id, true)));
        for (id, error) in outcome.rejected {
            let (seq, text) = texts.remove(&id).unwrap_or_default();
            rejected.push((id, seq, error, Some(text)));
        }
        let needed: bool = self
            .db
            .prepare_cached("SELECT EXISTS(SELECT 1 FROM feed_needs)")?
            .query_row([], |r| r.get(0))?;
        for (id, committed) in known {
            // Per-id evidence that `peer` committed `id` (§6.3.4).
            self.db
                .prepare_cached("INSERT OR IGNORE INTO peer_known(peer,event) VALUES(?,?)")?
                .execute(params![peer, id])?;
            if committed {
                let acked = self
                    .db
                    .prepare_cached(
                        "UPDATE outbox SET acknowledged=1 WHERE event=? AND acknowledged=0",
                    )?
                    .execute([&id])?;
                received.acked += acked as u32;
            }
            if needed {
                // Nobody needs to serve an event this replica holds.
                self.db
                    .prepare_cached("DELETE FROM feed_needs WHERE event=?")?
                    .execute([&id])?;
            }
        }
        for (id, seq, reason, digest) in needs {
            self.db.execute(
                "INSERT INTO feed_needs(peer,event,reason,requested_ms,missing_at,seq,digest) VALUES(?,?,?,?,NULL,?,?)
                ON CONFLICT(peer,event) DO UPDATE SET reason=excluded.reason,seq=excluded.seq,digest=excluded.digest,missing_at=NULL",
                params![peer, id, reason, now_ms(), seq, digest],
            )?;
        }
        for (id, seq, error, payload) in rejected {
            self.db.execute(
                "INSERT INTO feed_rejected(peer,event,seq,code,message,payload,at_ms) VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(peer,event) DO UPDATE SET seq=excluded.seq,code=excluded.code,message=excluded.message,
                payload=coalesce(excluded.payload,feed_rejected.payload),at_ms=excluded.at_ms",
                params![peer, id, seq, error.code, error.message, payload, now_ms()],
            )?;
            self.db.execute(
                "DELETE FROM feed_needs WHERE peer=? AND event=?",
                params![peer, id],
            )?;
            received.rejected.push(RejectedEvent {
                id,
                seq,
                code: error.code,
                class: error.class,
            });
        }
        Ok(())
    }
    /// What to do with one item (§6.3.4 receiver rules).
    fn verdict(
        &self,
        item: &Item<'_>,
        bytes: &mut usize,
        lookups: &mut Lookups,
    ) -> Result<Verdict> {
        let held = self.held(&item.id)?;
        let Some(event) = item.event else {
            // Elided or large: the entry is only an id and a digest.
            let digest = item.digest.as_deref().unwrap_or_default();
            return Ok(match held {
                Some(held) if payload_matches(&held.payload, digest) => Verdict::Known {
                    committed: held.committed,
                },
                Some(_) => Verdict::Reject(ErrorWire::coded("EVENT_ID_REUSED", &item.id), None),
                None if item.large.is_some_and(|size| size > MAX_EVENT as u64) => {
                    Verdict::Reject(ErrorWire::coded("EVENT_METADATA_TOO_LARGE", &item.id), None)
                }
                None if item.large.is_some() => Verdict::Need("large"),
                None => Verdict::Need("elided"),
            });
        };
        let text = serde_json::to_string(event)?;
        *bytes = bytes.saturating_add(text.len());
        let digest = feed::digest(&text);
        if event.id != item.id {
            let error = ErrorWire::coded("INVALID_EVENT", "feed entry id differs from its payload");
            return Ok(Verdict::Reject(error, Some(text)));
        }
        if item
            .digest
            .as_ref()
            .is_some_and(|claimed| *claimed != digest)
        {
            let error = ErrorWire::coded("INVALID_EVENT", "payload digest mismatch");
            return Ok(Verdict::Reject(error, Some(text)));
        }
        Ok(match held {
            Some(held) if held.payload == text || payload_matches(&held.payload, &digest) => {
                Verdict::Known {
                    committed: held.committed,
                }
            }
            // A relayed page cannot rewrite this replica's own events (invariant 14).
            Some(_) if event.device == self.device => Verdict::Reject(
                ErrorWire::coded("FORGED_EVENT_ORIGIN", &event.id),
                Some(text),
            ),
            Some(_) => Verdict::Reject(ErrorWire::coded("EVENT_ID_REUSED", &event.id), Some(text)),
            None => match self.precheck(event, &text, lookups)? {
                Some(error) => Verdict::Reject(error, Some(text)),
                None => Verdict::Stage {
                    event: event.clone(),
                    text,
                },
            },
        })
    }
    /// The payload we hold for `id`: committed, else staged (any state).
    fn held(&self, id: &str) -> Result<Option<Held>> {
        let committed: Option<String> = self
            .db
            .prepare_cached("SELECT payload FROM events WHERE id=?")?
            .query_row([id], |r| r.get(0))
            .optional()?;
        if let Some(payload) = committed {
            return Ok(Some(Held {
                payload,
                committed: true,
            }));
        }
        let staged: Option<String> = self
            .db
            .prepare_cached("SELECT payload FROM incoming_events WHERE id=?")?
            .query_row([id], |r| r.get(0))
            .optional()?;
        Ok(staged.map(|payload| Held {
            payload,
            committed: false,
        }))
    }
    /// Stage-time validation of a payload we do not hold (§6.3.4):
    /// repository, shared branch, bounds, origin, immutable object sizes and
    /// branch descriptor. Activation validates the rest (causal links,
    /// values, conflicts) and quarantines what fails.
    fn precheck(
        &self,
        event: &Event,
        text: &str,
        lookups: &mut Lookups,
    ) -> Result<Option<ErrorWire>> {
        let error = |code: &str, detail: &str| Ok(Some(ErrorWire::coded(code, detail)));
        if event.repo != self.repo {
            return error("REPOSITORY_MISMATCH", &event.id);
        }
        if !event.branch.shared {
            return error("PRIVATE_BRANCH_REJECTED", &event.id);
        }
        if text.len() > MAX_EVENT {
            return error("EVENT_METADATA_TOO_LARGE", &event.id);
        }
        if !valid_id(&event.id)
            || !valid_id(&event.device)
            || !valid_id(&event.branch.id)
            || event.changes.is_empty()
            || event.changes.len() > 10000
        {
            return error("INVALID_EVENT", &event.id);
        }
        if event.device == self.device {
            // Not held: this replica never committed it.
            return error("FORGED_EVENT_ORIGIN", &event.id);
        }
        let objects = event.objects();
        if !event.object_sizes.keys().all(|h| objects.contains(h)) {
            return error("UNREFERENCED_OBJECT_SIZE", &event.id);
        }
        if objects.iter().any(|object| valid_object(object).is_err()) {
            return error("INVALID_EVENT", "invalid object id");
        }
        for (object, size) in &event.object_sizes {
            let known = match lookups.sizes.get(object) {
                Some(known) => *known,
                None => {
                    let known = self.object_size_if_known(object)?;
                    lookups.sizes.insert(object.clone(), known);
                    known
                }
            };
            if known.is_some_and(|known| known != *size) {
                return error("OBJECT_SIZE_CHANGED", object);
            }
        }
        if !lookups.branches.contains_key(&event.branch.id) {
            let branch: Option<Branch> = self
                .db
                .prepare_cached("SELECT id,name,shared FROM branches WHERE id=?")?
                .query_row([&event.branch.id], |r| {
                    Ok(Branch {
                        id: r.get(0)?,
                        name: r.get(1)?,
                        shared: r.get(2)?,
                    })
                })
                .optional()?;
            lookups.branches.insert(event.branch.id.clone(), branch);
        }
        if lookups.branches[&event.branch.id]
            .as_ref()
            .is_some_and(|branch| *branch != event.branch)
        {
            return error("BRANCH_DESCRIPTOR_CHANGED", &event.branch.id);
        }
        Ok(None)
    }
    /// Forget what `peer` is known to hold, when its feed restarts (a reset
    /// page) or it regressed: delete its `peer_known` rows and recompute
    /// every outbox ack from the evidence left (withdrawn events are queued
    /// again; the port of `remember_peer_inventory`'s re-queue), and delete
    /// its `feed_needs`: what its old feed listed (it may have lost those
    /// events, which would keep its metadata from ever catching up) and its
    /// `unknown` answers. The re-read of its feed lists again what it still
    /// has.
    fn forget_peer(&self, peer: &str) -> Result<()> {
        self.db
            .prepare_cached("DELETE FROM feed_needs WHERE peer=?")?
            .execute([peer])?;
        let removed = self
            .db
            .execute("DELETE FROM peer_known WHERE peer=?", [peer])?;
        if removed > 0 {
            self.db.execute(
                "WITH peers(peer) AS (SELECT DISTINCT peer FROM peer_known)
                UPDATE outbox SET acknowledged=EXISTS(SELECT 1 FROM peer_known k
                    WHERE k.peer IN peers AND k.event=outbox.event)
                WHERE acknowledged<>EXISTS(SELECT 1 FROM peer_known k
                    WHERE k.peer IN peers AND k.event=outbox.event)",
                [],
            )?;
        }
        Ok(())
    }
    /// Record `peer`'s cursor of our feed as reported on channel `via`. A
    /// lower cursor of the same epoch than the valid one the same channel
    /// reported before means the peer regressed (§6.3.4): forget it and
    /// re-read its feed from the start (by deleting our cursor of it), which
    /// rebuilds the evidence that still holds. No cursor (`None`, or an
    /// empty epoch: a peer re-reading our feed) and `restart` (a reset was
    /// sent or accepted) drop both channels' reports first, so the low
    /// reports that follow are never taken for a regression.
    fn observe_report(
        &self,
        peer: &str,
        via: &str,
        report: Option<&Cursor>,
        restart: bool,
    ) -> Result<bool> {
        let report = report.filter(|report| !report.epoch.is_empty());
        if restart || report.is_none() {
            self.db
                .execute("DELETE FROM feed_out WHERE peer=?", [peer])?;
        }
        let Some(report) = report else {
            return Ok(false);
        };
        let previous = self.out_report(peer, via)?;
        let regressed = match &previous {
            Some(previous) => {
                previous.epoch == report.epoch
                    && report.seq < previous.seq
                    && self.validate_cursor(previous)?
            }
            None => false,
        };
        if regressed {
            self.forget_peer(peer)?;
            self.db
                .execute("DELETE FROM feed_in WHERE peer=?", [peer])?;
            self.db
                .execute("DELETE FROM feed_out WHERE peer=?", [peer])?;
        }
        if regressed || previous.as_ref() != Some(report) {
            self.db.execute(
                "INSERT INTO feed_out(peer,via,epoch,seq,id,reported_ms) VALUES(?,?,?,?,?,?)
                ON CONFLICT(peer,via) DO UPDATE SET epoch=excluded.epoch,seq=excluded.seq,id=excluded.id,reported_ms=excluded.reported_ms",
                params![peer, via, report.epoch, report.seq, report.id, now_ms()],
            )?;
        }
        Ok(regressed)
    }
    /// Ids `peer` reported unknown: not asked again for `NEED_RETRY_MS`, and
    /// a listed one no longer counts as outstanding (`PeerView::needs`).
    /// Only ids we could have asked for are recorded (invariant 22): a need
    /// of `peer` we hold, or a missing parent of a waiting row (a `parent`
    /// mark). Marks that expired, which suppress nothing, are dropped first.
    fn mark_unknown(&self, peer: &str, ids: &[String]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let now = now_ms();
        self.db
            .prepare_cached("DELETE FROM feed_needs WHERE peer=? AND reason=? AND missing_at<=?")?
            .execute(params![
                peer,
                NEED_PARENT,
                now.saturating_sub(NEED_RETRY_MS)
            ])?;
        for id in ids {
            let marked = self
                .db
                .prepare_cached("UPDATE feed_needs SET missing_at=? WHERE peer=? AND event=?")?
                .execute(params![now, peer, id])?;
            if marked > 0 {
                continue;
            }
            let waited_on: bool = self
                .db
                .prepare_cached("SELECT EXISTS(SELECT 1 FROM incoming_waits WHERE parent=?)")?
                .query_row([id], |r| r.get(0))?;
            if waited_on {
                self.db
                    .prepare_cached(
                        "INSERT OR IGNORE INTO feed_needs(peer,event,reason,requested_ms,missing_at,seq,digest)
                        VALUES(?,?,?,?,?,0,NULL)",
                    )?
                    .execute(params![peer, id, NEED_PARENT, now, now])?;
            }
        }
        Ok(())
    }
    /// The `need` list for `peer` (≤ `MAX_NEED`): ids its feed listed that
    /// we lack, then missing parents of waiting rows. Parents are looked for
    /// only once our cursor is at the peer's tip (while its feed streams, a
    /// feed in acceptance order brings parents before children), and only
    /// among the oldest `MISSING_SCAN` waits (`Store::missing_parents` scans
    /// every wait, O(staged) per offer during a large catch-up). Ids the peer
    /// reported unknown are skipped for `NEED_RETRY_MS`.
    fn need_list(&self, peer: &str) -> Result<Vec<String>> {
        let retry_before = now_ms().saturating_sub(NEED_RETRY_MS);
        let mut need: Vec<String> = self
            .db
            .prepare_cached(
                "SELECT event FROM feed_needs WHERE peer=?1 AND reason<>?2 AND (missing_at IS NULL OR missing_at<=?3)
                ORDER BY requested_ms,event LIMIT ?4",
            )?
            .query_map(
                params![peer, NEED_PARENT, retry_before, MAX_NEED as i64],
                |r| r.get(0),
            )?
            .collect::<rusqlite::Result<_>>()?;
        let streaming = self
            .in_row(peer)?
            .is_some_and(|row| row.cursor.seq < row.tip);
        if need.len() < MAX_NEED && !streaming {
            let mut listed: BTreeSet<String> = need.iter().cloned().collect();
            let parents: Vec<String> = self
                .db
                .prepare_cached(MISSING_PARENTS_SQL)?
                .query_map(params![MISSING_SCAN as i64, MAX_NEED as i64], |r| r.get(0))?
                .collect::<rusqlite::Result<_>>()?;
            for parent in parents {
                if need.len() >= MAX_NEED {
                    break;
                }
                let recently_missing: bool = self
                    .db
                    .prepare_cached(
                        "SELECT EXISTS(SELECT 1 FROM feed_needs WHERE peer=? AND event=? AND missing_at>?)",
                    )?
                    .query_row(params![peer, parent, retry_before], |r| r.get(0))?;
                if !recently_missing && listed.insert(parent.clone()) {
                    need.push(parent);
                }
            }
        }
        Ok(need)
    }
    /// Inbox rows staged from `peer` (`incoming_sources`), by state. When
    /// every staged row came (also) from `peer`, these are the inbox counts
    /// (an index-only count) instead of a join over every row.
    fn source_counts(&self, peer: &str) -> Result<InboxCounts> {
        let from_peer: i64 = self
            .db
            .prepare_cached("SELECT count(*) FROM incoming_sources WHERE peer=?")?
            .query_row([peer], |r| r.get(0))?;
        if from_peer == 0 {
            return Ok(InboxCounts::default());
        }
        let staged: i64 = self
            .db
            .prepare_cached("SELECT count(*) FROM incoming_events")?
            .query_row([], |r| r.get(0))?;
        if staged == from_peer {
            return self.inbox_counts();
        }
        let mut counts = InboxCounts::default();
        let mut statement = self.db.prepare_cached(
            "SELECT i.state,count(*) FROM incoming_sources s JOIN incoming_events i ON i.id=s.event WHERE s.peer=? GROUP BY i.state",
        )?;
        let rows = statement.query_map([peer], |r| {
            Ok((r.get::<_, Option<String>>(0)?, r.get::<_, i64>(1)?))
        })?;
        for row in rows {
            let (state, count) = row?;
            let count = u64::try_from(count).unwrap_or(0);
            match state.as_deref() {
                Some("ready") => counts.ready += count,
                Some("waiting") => counts.waiting += count,
                Some("retry") => counts.retry += count,
                Some("blocked") => counts.blocked += count,
                Some("quarantined") => counts.quarantined += count,
                _ => counts.unclassified += count,
            }
        }
        Ok(counts)
    }
    fn peer_view(&self, peer: &str) -> Result<PeerView> {
        let out = self.out_row(peer)?;
        let base = match &out {
            Some(cursor) if self.validate_cursor(cursor)? => cursor.seq,
            _ => 0,
        };
        let private: bool = self
            .db
            .prepare_cached("SELECT EXISTS(SELECT 1 FROM branches WHERE shared=0)")?
            .query_row([], |r| r.get(0))?;
        let count_sql = if private {
            SHARED_AFTER_SQL
        } else {
            ROWS_AFTER_SQL
        };
        let to_send: i64 = self
            .db
            .prepare_cached(count_sql)?
            .query_row(params![base, COUNT_CAP as i64], |r| r.get(0))?;
        let rejected: i64 = self
            .db
            .prepare_cached("SELECT count(*) FROM feed_rejected WHERE peer=?")?
            .query_row([peer], |r| r.get(0))?;
        // Listed ids the peer reported unknown are not outstanding: it lost
        // them, or they were never its to give.
        let needs: i64 = self
            .db
            .prepare_cached(
                "SELECT count(*) FROM feed_needs WHERE peer=? AND reason IN (?,?) AND missing_at IS NULL",
            )?
            .query_row(params![peer, NEED_LISTED[0], NEED_LISTED[1]], |r| r.get(0))?;
        Ok(PeerView {
            row: self.in_row(peer)?,
            out,
            source: self.source_counts(peer)?,
            rejected: capped(rejected),
            needs: capped(needs),
            to_send: capped(to_send),
            local_tip: self.feed_tip()?,
        })
    }
    fn apply_result(
        &self,
        peer: &str,
        received: &Received,
        sent: &Sent,
        before: (i64, i64),
    ) -> Result<FeedApplyResult> {
        let view = self.peer_view(peer)?;
        let cursor_out = view.out.as_ref().map_or(0, |cursor| cursor.seq);
        let summary = ExchangeSummary {
            sent_events: sent.events,
            received_events: received.events,
            staged: received.staged,
            rejected: received.rejected.len() as u32,
            acked: received.acked,
            local_tip: view.local_tip,
            remote_tip: view.remote_tip(),
            cursor_in: view.cursor_in(),
            cursor_out,
            to_send: view.to_send,
            to_receive: view.to_receive(),
            local_pending: view.source.pending_active(),
            remote_pending: view.remote_pending(),
            blocked: view.source.blocked,
            quarantined: view.source.quarantined,
            caught_up: view.metadata_caught_up(),
            metadata_advanced: received.staged > 0
                || received.acked > 0
                || !received.rejected.is_empty()
                || sent.events > 0
                || before != (view.cursor_in(), cursor_out),
            bytes_up: sent.bytes,
            bytes_down: received.bytes,
        };
        Ok(FeedApplyResult {
            gate: view.gate(),
            summary,
        })
    }
}

/// Payload entries and approximate JSON bytes of a page we send.
fn page_sent(page: &FeedPage) -> Sent {
    let mut sent = Sent {
        events: 0,
        bytes: MESSAGE_OVERHEAD + ENTRY_OVERHEAD * page.entries.len() as u64,
    };
    for event in page.entries.iter().filter_map(|entry| entry.event.as_ref()) {
        sent.events += 1;
        sent.bytes += serde_json::to_vec(event).map_or(0, |bytes| bytes.len() as u64);
    }
    sent
}

/// Structural checks of a received page: bounded (invariant 22), one epoch,
/// an ordered range after its cursor, a tip that is not negative (probes
/// included: the tip is stored and subtracted from).
fn check_page(page: &FeedPage) -> Result<()> {
    ensure!(
        page.entries.len() <= MAX_PAGE_ENTRIES,
        "INVENTORY_TOO_LARGE: {} feed entries",
        page.entries.len()
    );
    ensure!(
        valid_epoch(&page.epoch),
        "PROTOCOL_VIOLATION: a feed page names its epoch ({EPOCH_HEX_LEN} hex characters)"
    );
    ensure!(
        page.tip >= 0,
        "PROTOCOL_VIOLATION: negative feed tip {}",
        page.tip
    );
    ensure!(
        well_formed_cursor(&page.through) && page.after.as_ref().is_none_or(well_formed_cursor),
        "PROTOCOL_VIOLATION: malformed feed page cursor"
    );
    let Some(after) = &page.after else {
        ensure!(
            page.entries.is_empty() && !page.reset,
            "PROTOCOL_VIOLATION: a probe page carries no entries"
        );
        return Ok(());
    };
    ensure!(
        after.epoch == page.epoch && page.through.epoch == page.epoch,
        "PROTOCOL_VIOLATION: feed page epochs differ"
    );
    let base = if page.reset { 0 } else { after.seq };
    ensure!(
        base >= 0 && page.through.seq >= base && page.through.seq <= page.tip,
        "PROTOCOL_VIOLATION: feed page range {}..{} (tip {})",
        base,
        page.through.seq,
        page.tip
    );
    let mut last = base;
    for entry in &page.entries {
        ensure!(
            entry.seq > last && entry.seq <= page.through.seq,
            "PROTOCOL_VIOLATION: feed entries out of order"
        );
        ensure!(
            valid_id(&entry.id) && valid_digest(&entry.digest),
            "PROTOCOL_VIOLATION: malformed feed entry"
        );
        last = entry.seq;
    }
    Ok(())
}
