//! Status and content summaries of one store.
//!
//! `content_counters` is one SQL pass over the object indexes plus one walk of
//! the active projection. Physical presence comes from one cached listing of
//! `objects/` (`Store::presence`), never from a `stat` per object, so objects
//! deleted behind the store's back still show as missing. The result is cached
//! until the database (`total_changes`, `PRAGMA data_version`), the active
//! branch or the listing changes; spools, demand and item details are read
//! fresh on every call.
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
use super::objects::{CORRUPT, Presence};
use super::*;
use crate::model::health::{ContentCounters, InboxCounts, JournalCounters};
use std::{
    collections::{HashMap, HashSet},
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

/// Most missing-content items in `content_status`.
pub const CONTENT_ITEMS: usize = 20;
/// A cached scan is recomputed at least this often.
const CONTENT_CACHE_TTL: Duration = Duration::from_secs(60);

/// The cached part of a content scan (see the module docs).
pub(crate) struct ContentCache {
    key: ContentKey,
    at: Instant,
    counters: ContentCounters,
    /// `(object, path)` of the first `CONTENT_ITEMS` missing current entries.
    missing: Vec<(String, String)>,
}
#[derive(PartialEq, Eq)]
struct ContentKey {
    changes: u64,
    data_version: i64,
    transactional: bool,
    branch: String,
    presence: u64,
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
            "current_missing":counters.current_missing,"shared_objects":counters.shared_objects,
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
    /// Journal counters (§4.5): `epoch` stays `None` (the feed epoch is
    /// WP2.5's).
    pub fn journal_counters(&self) -> Result<JournalCounters> {
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
            epoch: None,
            outgoing_unacknowledged: counters.outgoing_unacknowledged,
            unresolved_conflicts: counters.unresolved_conflicts,
        })
    }
    /// Per peer with `peer_known` rows: whether its known set equals the
    /// shared committed inventory (`shared_event_inventory() ==
    /// known_by_peer(peer)`), by counts: `peer_known` rows are unique per
    /// `(peer, event)`, so equal counts of the shared inventory, of the
    /// peer's rows and of their intersection mean equal sets.
    pub fn inventory_match(&self) -> Result<InventoryMatch> {
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
        let key = ContentKey {
            changes: self.db.total_changes(),
            data_version: self.data_version()?,
            transactional: !self.db.is_autocommit(),
            branch: self.active.clone(),
            presence: presence.generation,
        };
        let cached = self
            .caches
            .objects
            .content
            .borrow()
            .as_ref()
            .filter(|cache| cache.key == key && cache.at.elapsed() < CONTENT_CACHE_TTL)
            .map(|cache| (cache.counters, cache.missing.clone()));
        let (mut counters, missing) = match cached {
            Some(cached) => cached,
            None => {
                let (counters, missing) = self.scan_content(&presence)?;
                *self.caches.objects.content.borrow_mut() = Some(ContentCache {
                    key,
                    at: Instant::now(),
                    counters,
                    missing: missing.clone(),
                });
                (counters, missing)
            }
        };
        let partial = self.partial_objects()?;
        counters.downloading = partial.len() as u64;
        counters.partial_bytes = partial.values().sum();
        counters.requested = self.wanted_objects()?.len() as u64;
        let mut listed = Vec::with_capacity(missing.len().min(items));
        for (object, path) in missing.into_iter().take(items) {
            let mut state = self.object_state(&object)?;
            state["path"] = json!(path);
            listed.push(state);
        }
        Ok((counters, listed))
    }
    /// The uncached scan: shared and current counters, and the first missing
    /// current entries. Available means installed with a consistent size and
    /// not known corrupt.
    fn scan_content(
        &self,
        presence: &Presence,
    ) -> Result<(ContentCounters, Vec<(String, String)>)> {
        let mut counters = ContentCounters::default();
        let view = self.view(&self.active)?;
        let current: HashSet<&str> = view
            .projection
            .entries
            .values()
            .filter(|e| e.alive)
            .filter_map(|e| e.content.as_deref())
            .collect();
        // `(available, size)` of every current object, filled from the pass below.
        let mut current_state: HashMap<&str, (bool, Option<i64>)> =
            HashMap::with_capacity(current.len());
        let mut statement = self.db.prepare_cached(
            "SELECT e.hash,e.shared,z.size,a.state,
                EXISTS(SELECT 1 FROM published_objects p WHERE p.hash=e.hash)
                OR EXISTS(SELECT 1 FROM received_shared_objects r WHERE r.hash=e.hash)
             FROM (SELECT hash,max(shared) AS shared FROM event_objects GROUP BY hash) e
             LEFT JOIN object_sizes z ON z.hash=e.hash
             LEFT JOIN object_availability a ON a.hash=e.hash",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let object = row.get_ref(0)?.as_str()?;
            let shared: bool = row.get(1)?;
            let size: Option<i64> = row.get(2)?;
            let corrupt = row.get::<_, Option<i64>>(3)? == Some(CORRUPT);
            let available = !corrupt && presence.has(object, size);
            if shared {
                counters.shared_objects += 1;
                if available && row.get::<_, bool>(4)? {
                    counters.shared_available += 1;
                } else {
                    counters.shared_missing_bytes += size.map_or(0, |s| s.max(0) as u64);
                }
            }
            if let Some(object) = current.get(object) {
                current_state.insert(object, (available, size));
            }
        }
        drop(rows);
        drop(statement);
        counters.complete = counters.shared_available == counters.shared_objects;
        let mut available = HashSet::with_capacity(current.len());
        for object in &current {
            let (present, size) = match current_state.get(object) {
                Some(&state) => state,
                // Not referenced by any committed event (never expected).
                None => (self.object_available(object), None),
            };
            if present {
                available.insert(*object);
            } else {
                counters.current_missing_bytes += size.map_or(0, |s| s.max(0) as u64);
            }
        }
        counters.current_objects = current.len() as u64;
        counters.current_available = available.len() as u64;
        counters.current_missing = counters.current_objects - counters.current_available;
        let missing = view
            .projection
            .entries
            .values()
            .filter(|e| e.alive)
            .filter_map(|e| e.content.as_deref().map(|object| (e, object)))
            .filter(|(_, object)| !available.contains(object))
            .take(CONTENT_ITEMS)
            .map(|(entry, object)| {
                let mut path = vec![entry.name.clone()];
                let mut parent = &entry.parent;
                let mut seen = BTreeSet::new();
                while parent != ROOT && seen.insert(parent.clone()) {
                    let Some(e) = view.projection.entries.get(parent) else {
                        break;
                    };
                    path.push(e.name.clone());
                    parent = &e.parent;
                }
                path.reverse();
                (object.to_owned(), path.join("/"))
            })
            .collect();
        Ok((counters, missing))
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
