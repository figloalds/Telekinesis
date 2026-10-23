//! Status and content summaries of one store.
//!
//! Content summaries follow current direct/manifest descriptors and verified
//! graph indexes, with cached listings of `objects/` and `manifests-v1/`.
//! They never read or hash file bytes. Physical presence without verified
//! adjacency is reported as indexing, and received shared roots require public
//! receipts independently of locally readable deduplicated bytes. The cache
//! follows database changes, branch and both directory listing generations.
//!
//! The legacy status (`Store::status`, WP2.3 task 9) is built from three
//! parts that the worker's status cell (`worker::status_cell`) also reads
//! separately, on a secondary connection and without the engine lock:
//!
//! * [`Store::status_counters`]: inbox and outbox counters, the journal tip
//!   and the unresolved-conflict count. Point lookups and index scans only:
//!   no projection, no content scan (cheap enough for every mutating path);
//! * [`Store::content_report`]: the content counters and the legacy
//!   `content` member, from one content scan;
//! * [`Store::pending_causal_events`]: the active projection's pending list,
//!   read from the cached view (no clone of the projection).
//!
//! Compatibility keys keep their meaning: `incoming_pending` counts every
//! staged row (any state) and `incoming_quarantined` the rows with a legacy
//! `error` (quarantined, blocked, or a legacy row still to be classified). The
//! inbox state machine's own accounting (§4.4) is added beside them:
//! `incoming_active` (`pending_active`: ready + waiting + retry +
//! unclassified), `incoming_blocked`, and the typed `inbox` counts.
mod content;
use super::objects::Presence;
use super::*;
use crate::model::health::{ContentCounters, InboxCounts, JournalCounters};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

/// The cheap part of a store's status: no projection and no content scan.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusCounters {
    /// Inbox rows by state (§4.4).
    pub inbox: InboxCounts,
    /// Legacy `incoming_pending`: every staged row, in any state.
    pub incoming_pending: u64,
    /// Legacy `incoming_quarantined`: rows with an `error` (quarantined,
    /// blocked, or a legacy row with an error still to be classified).
    pub incoming_quarantined: u64,
    pub outgoing_unacknowledged: u64,
    /// Unresolved conflicts of the active branch.
    pub unresolved_conflicts: u64,
    /// `journal_sequence` tip (0 for an empty journal).
    pub tip: i64,
}

impl StatusCounters {
    /// The legacy counter keys, as `Store::status` reports them, plus the
    /// state machine's accounting (`incoming_active`, `incoming_blocked`,
    /// `inbox`).
    pub fn legacy_json(&self) -> Value {
        json!({"outgoing_unacknowledged":self.outgoing_unacknowledged,
            "incoming_pending":self.incoming_pending,"incoming_quarantined":self.incoming_quarantined,
            "incoming_active":self.inbox.pending_active(),"incoming_blocked":self.inbox.blocked,
            "inbox":self.inbox,"unresolved_conflicts":self.unresolved_conflicts})
    }
}

/// Whether a peer's advertised inventory (`peer_known`) equals the shared
/// committed inventory, per peer (the worker status `caught_up` check).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryMatch {
    /// Shared committed events.
    pub shared_events: u64,
    /// Peers with at least one `peer_known` row: whether their known set is
    /// exactly the shared committed inventory. A peer without rows matches
    /// only an empty inventory.
    pub peers: BTreeMap<String, bool>,
}

impl InventoryMatch {
    pub fn matches(&self, peer: &str) -> bool {
        self.peers
            .get(peer)
            .copied()
            .unwrap_or(self.shared_events == 0)
    }
}

/// The status cache of one connection (`StoreCaches::status`): the journal
/// counters and the per-peer inventory match, each with the change key it
/// was computed under. An idle store's status refresh therefore reads them
/// back instead of scanning the journal and `peer_known` again (WP2.3's
/// maintenance did both at about 1 Hz forever: about a fifth of a core per
/// idle folder of 20K+ events; post-integration re-review, WP2.INT).
#[derive(Default)]
pub(crate) struct Cache {
    journal: std::cell::RefCell<Option<(ChangeKey, JournalCounters)>>,
    inventory: std::cell::RefCell<Option<(ChangeKey, InventoryMatch)>>,
    /// Computations of either (a counter for tests).
    computed: std::cell::Cell<u64>,
}

/// Whether the database changed for this connection: its own writes
/// (`total_changes`), other connections' commits (`PRAGMA data_version`), an
/// open transaction (whose reads may roll back), and the active branch.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ChangeKey {
    changes: u64,
    data_version: i64,
    transactional: bool,
    branch: String,
}

/// Most missing-content items in `content_status`.
pub const CONTENT_ITEMS: usize = 20;
/// A cached scan is recomputed at least this often. Its key already holds
/// every input (the database as this connection sees it, the active branch,
/// both content listings, re-taken when their directories change), so
/// this is only a safety net: 10 minutes (it was 60 s, which made every idle
/// folder rescan all its objects each minute, about 0.3 s of CPU at 10K
/// objects; CCR-WP4.2-5, WP4.INT).
const CONTENT_CACHE_TTL: Duration = Duration::from_secs(600);

/// The cached part of a content scan (see the module docs).
pub(crate) struct ContentCache {
    key: ContentKey,
    at: Instant,
    counters: ContentCounters,
    /// Typed details of the first `CONTENT_ITEMS` unavailable current entries.
    missing: Vec<Value>,
}
#[derive(PartialEq, Eq)]
struct ContentKey {
    changes: u64,
    data_version: i64,
    transactional: bool,
    branch: String,
    presence: u64,
    manifests: u64,
}

impl Store {
    /// The content claim (invariant 30) as typed counters.
    pub fn content_counters(&self) -> Result<ContentCounters> {
        Ok(self.content_scan(0)?.0)
    }
    /// The compatibility shape: counters plus up to `CONTENT_ITEMS` missing
    /// current objects with their state and path.
    pub fn content_status(&self) -> Result<Value> {
        Ok(self.content_report()?.1)
    }
    /// The typed counters and the compatibility `content` member, from one
    /// content scan.
    pub fn content_report(&self) -> Result<(ContentCounters, Value)> {
        let (counters, items) = self.content_scan(CONTENT_ITEMS)?;
        let legacy = json!({"items":items,"current_objects":counters.current_objects,"current_available":counters.current_available,
            "current_missing":counters.current_missing,"current_missing_bytes":counters.current_missing_bytes,
            "shared_missing_bytes":counters.shared_missing_bytes,"shared_objects":counters.shared_objects,
            "shared_available":counters.shared_available,"downloading":counters.downloading,
            "partial_bytes":counters.partial_bytes,"complete":counters.complete,
            "requested":counters.requested});
        Ok((counters, legacy))
    }
    /// The cheap status counters (module docs): index scans and point
    /// lookups, no projection.
    pub fn status_counters(&self) -> Result<StatusCounters> {
        let inbox = self.inbox_counts()?;
        // Rows with a legacy `error`: quarantined and blocked rows always
        // carry one (§4.4), other classified rows never do; a legacy row may
        // carry one until it is classified. Through the state index, never a
        // scan of the staged payloads.
        let legacy_errors: i64 = self
            .db
            .prepare_cached(
                "SELECT count(*) FROM incoming_events WHERE state IS NULL AND error IS NOT NULL",
            )?
            .query_row([], |r| r.get(0))?;
        let unresolved: i64 = self
            .db
            .prepare_cached(
                "SELECT count(*) FROM conflicts WHERE branch=? AND resolved_by IS NULL",
            )?
            .query_row([&self.active], |r| r.get(0))?;
        let tip: i64 = self
            .db
            .prepare_cached("SELECT coalesce(max(seq),0) FROM journal_sequence")?
            .query_row([], |r| r.get(0))?;
        Ok(StatusCounters {
            incoming_pending: inbox.total(),
            incoming_quarantined: inbox
                .quarantined
                .saturating_add(inbox.blocked)
                .saturating_add(legacy_errors.max(0) as u64),
            inbox,
            outgoing_unacknowledged: self.outgoing_unacknowledged()?.max(0) as u64,
            unresolved_conflicts: unresolved.max(0) as u64,
            tip,
        })
    }
    /// The active projection's entries only (no heads, conflicts or pending
    /// list), from the cached view: what a notification diff reads, at a
    /// fraction of the cost of cloning the whole projection.
    pub fn entries_snapshot(&self) -> Result<Projection> {
        Ok(Projection {
            entries: self.view(&self.active)?.projection.entries.clone(),
            ..Projection::default()
        })
    }
    /// The active projection's pending causal events, from the cached view
    /// (loaded only if it is stale; never cloned whole).
    pub fn pending_causal_events(&self) -> Result<Vec<String>> {
        Ok(self.view(&self.active)?.projection.pending.clone())
    }
    /// The change key the status cache is valid under (see [`Cache`]).
    fn change_key(&self) -> Result<ChangeKey> {
        Ok(ChangeKey {
            changes: self.db.total_changes(),
            data_version: self.data_version()?,
            transactional: !self.db.is_autocommit(),
            branch: self.active.clone(),
        })
    }
    /// How many times this handle computed the journal counters or the
    /// inventory match (not counting cached answers).
    pub fn status_computations(&self) -> u64 {
        self.caches.status.computed.get()
    }
    fn count_status_computation(&self) {
        let computed = &self.caches.status.computed;
        computed.set(computed.get().saturating_add(1));
    }
    /// Journal counters (§4.5); `epoch` is the feed epoch (`journal_epoch`,
    /// WP2.5; wired by WP2.INT, CCR-WP2.5-5). Cached until the database
    /// changes ([`Cache`]).
    pub fn journal_counters(&self) -> Result<JournalCounters> {
        let key = self.change_key()?;
        if let Some((cached, counters)) = self.caches.status.journal.borrow().as_ref()
            && *cached == key
        {
            return Ok(counters.clone());
        }
        let counters = self.compute_journal_counters()?;
        self.count_status_computation();
        *self.caches.status.journal.borrow_mut() = Some((key, counters.clone()));
        Ok(counters)
    }
    fn compute_journal_counters(&self) -> Result<JournalCounters> {
        let counters = self.status_counters()?;
        let count = |sql: &str| -> Result<u64> {
            let n: i64 = self.db.prepare_cached(sql)?.query_row([], |r| r.get(0))?;
            Ok(n.max(0) as u64)
        };
        Ok(JournalCounters {
            events: count("SELECT count(*) FROM events")?,
            shared_events: count(
                "SELECT count(*) FROM events e JOIN branches b ON b.id=e.branch WHERE b.shared=1",
            )?,
            branches: count("SELECT count(*) FROM branches")?,
            tip: counters.tip,
            epoch: Some(self.feed_epoch()?),
            outgoing_unacknowledged: counters.outgoing_unacknowledged,
            unresolved_conflicts: counters.unresolved_conflicts,
        })
    }
    /// Per peer with `peer_known` rows: whether its known set equals the
    /// shared committed inventory (`shared_event_inventory() ==
    /// known_by_peer(peer)`), by counts: `peer_known` rows are unique per
    /// `(peer, event)`, so equal counts of the shared inventory, of the
    /// peer's rows and of their intersection mean equal sets. Cached until
    /// the database changes ([`Cache`]).
    pub fn inventory_match(&self) -> Result<InventoryMatch> {
        let key = self.change_key()?;
        if let Some((cached, inventory)) = self.caches.status.inventory.borrow().as_ref()
            && *cached == key
        {
            return Ok(inventory.clone());
        }
        let inventory = self.compute_inventory_match()?;
        self.count_status_computation();
        *self.caches.status.inventory.borrow_mut() = Some((key, inventory.clone()));
        Ok(inventory)
    }
    /// [`inventory_match`](Self::inventory_match), computed. Once the feed
    /// index (`feed_journal`, the shared committed events) is built, the
    /// counts are index-only: the feed's row count, each peer's
    /// `peer_known` range, and per feed row a lookup of the peer's
    /// `(peer, event)` key; the wide `events` rows are never read (the join
    /// through them was about 4× slower than the legacy set comparison at
    /// 50K events; post-integration re-review, WP2.INT).
    fn compute_inventory_match(&self) -> Result<InventoryMatch> {
        let indexed: bool = self
            .db
            .prepare_cached("SELECT EXISTS(SELECT 1 FROM config WHERE key=?1)")?
            .query_row([super::sharing::FEED_JOURNAL_MARKER], |r| r.get(0))?;
        if !indexed {
            return self.inventory_match_by_join();
        }
        let shared: i64 = self
            .db
            .prepare_cached("SELECT count(*) FROM feed_journal")?
            .query_row([], |r| r.get(0))?;
        let peers: Vec<(String, i64)> = self
            .db
            .prepare_cached("SELECT peer,count(*) FROM peer_known GROUP BY peer")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        let mut matches = BTreeMap::new();
        for (peer, known) in peers {
            let both: i64 = if known == shared {
                self.db
                    .prepare_cached(
                        "SELECT count(*) FROM feed_journal f WHERE EXISTS(SELECT 1 FROM peer_known k WHERE k.peer=?1 AND k.event=f.event)",
                    )?
                    .query_row([&peer], |r| r.get(0))?
            } else {
                -1
            };
            matches.insert(peer, known == shared && both == shared);
        }
        Ok(InventoryMatch {
            shared_events: shared.max(0) as u64,
            peers: matches,
        })
    }
    /// The inventory match through `events` (a store whose feed index is
    /// still being built).
    fn inventory_match_by_join(&self) -> Result<InventoryMatch> {
        let shared: i64 = self
            .db
            .prepare_cached(
                "SELECT count(*) FROM events e JOIN branches b ON b.id=e.branch WHERE b.shared=1",
            )?
            .query_row([], |r| r.get(0))?;
        let rows: Vec<(String, i64, i64)> = self
            .db
            .prepare_cached(
                "SELECT p.peer,count(*),count(b.id) FROM peer_known p
                 LEFT JOIN events e ON e.id=p.event LEFT JOIN branches b ON b.id=e.branch AND b.shared=1
                 GROUP BY p.peer",
            )?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(InventoryMatch {
            shared_events: shared.max(0) as u64,
            peers: rows
                .into_iter()
                .map(|(peer, known, both)| (peer, known == shared && both == shared))
                .collect(),
        })
    }
    fn content_scan(&self, items: usize) -> Result<(ContentCounters, Vec<Value>)> {
        let presence = self.presence()?;
        let manifests = self.content_presence("manifests-v1")?;
        let key = ContentKey {
            changes: self.db.total_changes(),
            data_version: self.data_version()?,
            transactional: !self.db.is_autocommit(),
            branch: self.active.clone(),
            presence: presence.generation,
            manifests: manifests.generation,
        };
        let cached = self
            .caches
            .objects
            .content
            .borrow()
            .as_ref()
            .filter(|cache| {
                !key.transactional && cache.key == key && cache.at.elapsed() < CONTENT_CACHE_TTL
            })
            .map(|cache| (cache.counters, cache.missing.clone()));
        let (counters, missing) = match cached {
            Some(cached) => cached,
            None => {
                let (counters, missing) = self.scan_content(&presence, &manifests)?;
                if !key.transactional {
                    *self.caches.objects.content.borrow_mut() = Some(ContentCache {
                        key,
                        at: Instant::now(),
                        counters,
                        missing: missing.clone(),
                    });
                }
                (counters, missing)
            }
        };
        Ok((counters, missing.into_iter().take(items).collect()))
    }
    pub fn outgoing_unacknowledged(&self) -> Result<i64> {
        Ok(self.db.query_row(
            "SELECT count(*) FROM outbox WHERE acknowledged=0",
            [],
            |r| r.get(0),
        )?)
    }
    /// The legacy status JSON of the store (module docs), built from
    /// [`Store::status_counters`], [`Store::content_report`] and
    /// [`Store::pending_causal_events`].
    pub fn status(&self) -> Result<Value> {
        let counters = self.status_counters()?;
        let (_, content) = self.content_report()?;
        let pending = self.pending_causal_events()?;
        let mut status = self.status_identity()?;
        merge_object(&mut status, counters.legacy_json());
        merge_object(
            &mut status,
            json!({"content":content,"caught_up":"unknown-until-peer-roundtrip","pending_causal_events":pending}),
        );
        Ok(status)
    }
    /// The identity and constant members of the legacy status: `repo`,
    /// `device`, `branch`, `generation`, `local_durability`, `peer_protocol`.
    pub fn status_identity(&self) -> Result<Value> {
        Ok(
            json!({"repo":self.repo,"device":self.device,"branch":self.branch(&self.active)?,"generation":self.generation,
            "local_durability":"metadata:sqlite-full; content:per-object","peer_protocol":PEER_FORMAT}),
        )
    }
}

/// Copies every member of the object `from` into the object `into`.
pub fn merge_object(into: &mut Value, from: Value) {
    if let (Value::Object(into), Value::Object(from)) = (into, from) {
        into.extend(from);
    }
}
