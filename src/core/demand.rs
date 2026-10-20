//! Object demand: locally queued requests and what each peer wants from us.
//! Every check is an index lookup (`event_objects`, `object_availability`);
//! nothing here parses the journal.
//!
//! Objects v3 (WP3.2, §6.4): `want_objects` builds the receiver-driven want
//! list of one bulk lane (demand > current projection > history).
use super::objects::CORRUPT;
use super::*;
use crate::model::protocol::{
    LaneSpec,
    objects::{MAX_WANT_ITEMS, WantItem, WantPriority},
};
use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

/// Demand older than this is no longer advertised (`requested_ms` expiry).
pub const DEMAND_TTL_MS: u64 = 24 * 60 * 60 * 1000;
/// Most wanted objects advertised per page.
pub const MAX_WANTED: usize = 1024;

impl Store {
    pub fn request_object(&self, object: &str) -> Result<()> {
        valid_object(object)?;
        ensure!(
            self.db
                .prepare_cached("SELECT EXISTS(SELECT 1 FROM event_objects WHERE hash=?)")?
                .query_row([object], |r| r.get::<_, bool>(0))?,
            "UNREFERENCED_OBJECT"
        );
        // Demand is bookkeeping, not projection state: the handle's view stays
        // fresh across it (WP5.INT, the WP5.2 review's minor: a read waiting
        // for a download renews its demand on the engine's connection every
        // 10 s, and each renewal made the next callback reload the whole
        // projection under the engine lock).
        self.keeping_view(|| -> Result<()> {
            self.db.execute("INSERT INTO object_requests VALUES(?,?) ON CONFLICT(hash) DO UPDATE SET requested_ms=excluded.requested_ms", params![object, time()])?;
            // A request means a reader found no usable bytes: reconcile the index.
            self.object_available_for_demand(object);
            Ok(())
        })
    }
    /// Objects this replica wants, newest demand first (at most `MAX_WANTED`,
    /// requests younger than `DEMAND_TTL_MS`), excluding available ones (the
    /// cached `objects/` listing, never one `stat` per object).
    pub fn wanted_objects(&self) -> Result<BTreeSet<String>> {
        let since = time().saturating_sub(DEMAND_TTL_MS);
        let presence = self.presence()?;
        let mut statement = self.db.prepare_cached(
            "SELECT r.hash,a.state,z.size FROM object_requests r
             LEFT JOIN object_availability a ON a.hash=r.hash LEFT JOIN object_sizes z ON z.hash=r.hash
             WHERE r.requested_ms>=? ORDER BY r.requested_ms DESC,r.hash",
        )?;
        let mut rows = statement.query([since])?;
        let mut wanted = BTreeSet::new();
        while wanted.len() < MAX_WANTED
            && let Some(row) = rows.next()?
        {
            let object = row.get_ref(0)?.as_str()?;
            let available =
                row.get::<_, Option<i64>>(1)? != Some(CORRUPT) && presence.has(object, row.get(2)?);
            if !available {
                wanted.insert(object.to_owned());
            }
        }
        Ok(wanted)
    }
    /// The objects with a waiting reader (WP5.3, `objects.demand`): demand
    /// younger than `DEMAND_TTL_MS` whose object is not available here,
    /// newest first, at most `limit`, with when it was requested. Two index
    /// lookups per demand row (`object_requests` is small: registration
    /// deletes a row), so a lane busy with other objects can ask often.
    /// Availability is the index's word: an object it shows available that
    /// is not is still listed by the want scan, and one listed here that
    /// arrived meanwhile is simply not wanted.
    pub fn demanded_objects(&self, limit: usize) -> Result<Vec<(String, i64)>> {
        let since = time().saturating_sub(DEMAND_TTL_MS) as i64;
        let mut statement = self.db.prepare_cached(
            "SELECT q.hash, q.requested_ms FROM object_requests q
             LEFT JOIN object_availability a ON a.hash = q.hash
             WHERE q.requested_ms >= ?1 AND (a.state IS NULL OR a.state != ?2)
             ORDER BY q.requested_ms DESC, q.hash LIMIT ?3",
        )?;
        let rows = statement
            .query_map(
                params![
                    since,
                    super::objects::AVAILABLE,
                    limit.min(MAX_WANTED) as i64
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Foreground selection is one indexed identity lookup, independent of
    /// the background want cache or current-tree/history cardinality.
    pub fn demanded_object(&self, hash: &str, lane: LaneSpec) -> Result<Option<WantItem>> {
        valid_object(hash)?;
        if !lane.is_valid() || !lane.owns(hash) {
            return Ok(None);
        }
        let row = self
            .db
            .prepare_cached(
                "SELECT z.size,a.state,
            EXISTS(SELECT 1 FROM published_objects p WHERE p.hash=q.hash)
              OR EXISTS(SELECT 1 FROM received_shared_objects r WHERE r.hash=q.hash),
            sp.durable,sp.total,e.retry_at_ms,e.code
            FROM object_requests q LEFT JOIN object_sizes z ON z.hash=q.hash
            LEFT JOIN object_availability a ON a.hash=q.hash
            LEFT JOIN spool_state sp ON sp.hash=q.hash
            LEFT JOIN object_error_state e ON e.hash=q.hash
            WHERE q.hash=?1 AND q.requested_ms>=?2 AND (
              EXISTS(SELECT 1 FROM event_objects o WHERE o.hash=q.hash AND o.shared=1)
              OR EXISTS(SELECT 1 FROM staged_objects s JOIN incoming_events i ON i.id=s.event
                WHERE s.hash=q.hash AND i.error IS NULL))",
            )?
            .query_row(
                params![hash, time().saturating_sub(DEMAND_TTL_MS) as i64],
                |r| {
                    Ok((
                        r.get::<_, Option<u64>>(0)?,
                        r.get::<_, Option<i64>>(1)?,
                        r.get::<_, bool>(2)?,
                        r.get::<_, Option<u64>>(3)?,
                        r.get::<_, Option<u64>>(4)?,
                        r.get::<_, Option<i64>>(5)?,
                        r.get::<_, Option<String>>(6)?,
                    ))
                },
            )
            .optional()?;
        let Some((size, state, shareable, durable, total, retry, code)) = row else {
            return Ok(None);
        };
        if (shareable && state == Some(super::objects::AVAILABLE))
            || (retry.is_some_and(|at| at > time() as i64)
                && code.as_deref() != Some("CORRUPT_OBJECT"))
        {
            return Ok(None);
        }
        let size = match size {
            Some(n) => n,
            None => self
                .expected_object_sizes(hash)?
                .first()
                .copied()
                .unwrap_or(total.unwrap_or(0)),
        };
        let durable = durable
            .filter(|d| *d <= size && total.is_none_or(|n| n == size))
            .unwrap_or(0);
        Ok(Some(WantItem {
            hash: hash.to_owned(),
            size,
            offset: if durable == size && size > 0 {
                size - 1
            } else {
                durable
            },
            priority: WantPriority::Demand,
        }))
    }
    pub fn peer_requests(&self, peer: &str) -> Result<BTreeSet<String>> {
        Ok(self
            .db
            .prepare("SELECT hash FROM peer_requests WHERE peer=?")?
            .query_map([peer], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }
    /// Record what `peer` wants (shared, committed references only) and relay
    /// the demand for objects we lack. One transaction, no journal parse.
    pub fn remember_requests(&self, peer: &str, wanted: &BTreeSet<String>) -> Result<()> {
        let presence = self.presence()?;
        self.in_savepoint("remember_requests", || {
            self.db
                .execute("DELETE FROM peer_requests WHERE peer=?", [peer])?;
            for object in wanted {
                if !self.objects_referenced_committed(object)? {
                    continue;
                }
                self.db.execute(
                    "INSERT OR IGNORE INTO peer_requests VALUES(?,?)",
                    params![peer, object],
                )?;
                if !self.available_in(&presence, object)? {
                    self.db.execute("INSERT INTO object_requests VALUES(?,?) ON CONFLICT(hash) DO UPDATE SET requested_ms=excluded.requested_ms", params![object, time()])?;
                }
            }
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// Objects v3 want lists (WP3.2, §6.4)

/// The current-projection object set is recomputed at most this often while
/// commits keep changing it: priority is a hint, correctness never depends on
/// it, and one recomputation is O(projection).
const CURRENT_REFRESH: Duration = Duration::from_secs(5);
/// Largest share of a want list's byte budget that the smallest-first order
/// fills before the fair slot (below) goes to the class's largest item.
const FAIR_SHARE_NUM: u64 = 3;
const FAIR_SHARE_DEN: u64 = 4;

/// The objects of the active branch's current projection, per store root:
/// `(active branch, journal rowid high-water mark)` it was computed at.
struct CurrentSet {
    key: (String, i64),
    at: Instant,
    objects: Arc<HashSet<String>>,
}
static CURRENT: Mutex<BTreeMap<PathBuf, CurrentSet>> = Mutex::new(BTreeMap::new());
const MAX_CURRENT_ROOTS: usize = 64;

/// One want candidate.
struct Candidate {
    item: WantItem,
    /// Bytes still to transfer.
    remaining: u64,
    /// `object_requests.requested_ms` (demand only).
    requested_ms: i64,
}

/// One row of the want scan (`want_objects`): an object referenced by a
/// committed shared or a validly staged event that is not shareable and
/// available here, with what does not depend on the lane, the caller or the
/// time already resolved (its size).
#[derive(Clone, Debug)]
struct WantRow {
    hash: String,
    size: u64,
    /// `spool_state.durable` and `.total`.
    durable: Option<i64>,
    spool_total: Option<u64>,
    /// `object_requests.requested_ms`.
    requested_ms: Option<i64>,
    /// `object_error_state.retry_at_ms` and `.code`.
    retry_at: Option<i64>,
    code: Option<String>,
}

/// Want scans of at most this many rows are kept (CCR-WP4.2-5): a store
/// that misses more is in a catch-up, whose commits change the database
/// between almost every two wants anyway.
const WANT_CACHE_ROWS: usize = 4096;

/// The last want scan of one connection, kept while the database is
/// unchanged as this connection sees it (CCR-WP4.2-5, applied by WP4.INT):
/// `PRAGMA data_version` (other connections' commits) and this connection's
/// `total_changes`, outside a transaction. Every bulk lane asks its worker's
/// wants each sync interval in both directions; with 10K objects each scan
/// joined six tables per object (the idle worker CPU floor measured 18 %).
/// An idle replica's scan is empty, and is answered from here.
pub(crate) struct WantCache {
    data_version: i64,
    changes: u64,
    rows: Arc<Vec<WantRow>>,
}

impl Store {
    /// The objects the active branch's current projection references (alive
    /// entries with content), read from the stored projection
    /// (`current_state` overlaid with `current_parts`) without loading it,
    /// and cached per store root (see [`CURRENT_REFRESH`]).
    pub fn current_objects(&self) -> Result<Arc<HashSet<String>>> {
        let key: (String, i64) = self.db.query_row(
            "SELECT (SELECT value FROM config WHERE key='active'),
                    coalesce((SELECT max(rowid) FROM events),0)",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        {
            let cache = CURRENT.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(cached) = cache.get(&self.root)
                && cached.key.0 == key.0
                && (cached.key.1 == key.1 || cached.at.elapsed() < CURRENT_REFRESH)
            {
                return Ok(cached.objects.clone());
            }
        }
        let objects: HashSet<String> = self
            .db
            .prepare_cached(
                "SELECT json_extract(e.value,'$.content') FROM current_state s,
                     json_each(CASE WHEN json_valid(s.payload) THEN s.payload ELSE '{}' END,'$.entries') e
                 WHERE s.branch=?1 AND json_extract(e.value,'$.alive')
                   AND json_type(e.value,'$.content')='text'
                   AND NOT EXISTS(SELECT 1 FROM current_parts p WHERE p.branch=?1 AND p.entity=e.key)
                 UNION
                 SELECT json_extract(p.entry,'$.content') FROM current_parts p
                 WHERE p.branch=?1 AND json_valid(p.entry) AND json_extract(p.entry,'$.alive')
                   AND json_type(p.entry,'$.content')='text'",
            )?
            .query_map([&key.0], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        let objects = Arc::new(objects);
        let mut cache = CURRENT.lock().unwrap_or_else(|p| p.into_inner());
        if cache.len() >= MAX_CURRENT_ROOTS && !cache.contains_key(&self.root) {
            cache.pop_first();
        }
        cache.insert(
            self.root.clone(),
            CurrentSet {
                key,
                at: Instant::now(),
                objects: objects.clone(),
            },
        );
        Ok(objects)
    }

    /// The want scan (`want_objects`), from this connection's cache while the
    /// database is unchanged ([`WantCache`]).
    fn want_rows(&self) -> Result<Arc<Vec<WantRow>>> {
        let cacheable = self.db.is_autocommit();
        let data_version: i64 = self
            .db
            .prepare_cached("PRAGMA data_version")?
            .query_row([], |r| r.get(0))?;
        let changes = self.db.total_changes();
        if cacheable
            && let Some(cached) = self.caches.objects.wants.borrow().as_ref()
            && cached.data_version == data_version
            && cached.changes == changes
        {
            return Ok(cached.rows.clone());
        }
        let mut statement = self.db.prepare_cached(
            "WITH c(hash) AS (
                 SELECT hash FROM event_objects WHERE shared=1
                 UNION
                 SELECT s.hash FROM staged_objects s JOIN incoming_events i ON i.id=s.event
                 WHERE i.error IS NULL)
             SELECT c.hash, z.size, a.state,
                 EXISTS(SELECT 1 FROM published_objects p WHERE p.hash=c.hash)
                     OR EXISTS(SELECT 1 FROM received_shared_objects r WHERE r.hash=c.hash),
                 sp.durable, sp.total, q.requested_ms, e.retry_at_ms, e.code
             FROM c
             LEFT JOIN object_sizes z ON z.hash=c.hash
             LEFT JOIN object_availability a ON a.hash=c.hash
             LEFT JOIN spool_state sp ON sp.hash=c.hash
             LEFT JOIN object_requests q ON q.hash=c.hash
             LEFT JOIN object_error_state e ON e.hash=c.hash",
        )?;
        let mut rows = statement.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let object = row.get_ref(0)?.as_str()?;
            if !super::objects::canonical_object(object) {
                continue;
            }
            let shareable: bool = row.get(3)?;
            if shareable && row.get::<_, Option<i64>>(2)? == Some(super::objects::AVAILABLE) {
                continue;
            }
            let size: Option<u64> = row
                .get::<_, Option<i64>>(1)?
                .and_then(|s| u64::try_from(s).ok());
            let spool_total: Option<u64> = row
                .get::<_, Option<i64>>(5)?
                .and_then(|t| u64::try_from(t).ok());
            let size = match size {
                Some(size) => size,
                None => match self.expected_object_sizes(object)?.first() {
                    Some(size) => *size,
                    None => spool_total.unwrap_or(0),
                },
            };
            out.push(WantRow {
                hash: object.to_owned(),
                size,
                durable: row.get(4)?,
                spool_total,
                requested_ms: row.get(6)?,
                retry_at: row.get(7)?,
                code: row.get(8)?,
            });
        }
        drop(rows);
        drop(statement);
        let rows = Arc::new(out);
        // The scan and the size lookups only read: the key still holds.
        *self.caches.objects.wants.borrow_mut() = (cacheable && rows.len() <= WANT_CACHE_ROWS)
            .then(|| WantCache {
                data_version,
                changes,
                rows: rows.clone(),
            });
        Ok(rows)
    }

    /// Objects v3 `objects.want` (§6.4): what this replica wants on `lane`,
    /// at most `max_items` items (≤ `MAX_WANT_ITEMS`) and about `max_bytes`
    /// still to transfer (the item that crosses the budget ends the list).
    ///
    /// **Candidates:** objects referenced by committed shared events or by
    /// validly staged ones (`event_objects` ∪ `staged_objects`, so objects
    /// flow before their events activate) that are not already shareable and
    /// available (a private copy is wanted, I4), owned by `lane`
    /// (`LaneSpec::owns`, invariant 19), not `leased_elsewhere` (another
    /// session's lease, or the caller's per-source backoff), and not inside
    /// their transfer backoff (`object_error_state`; a local quarantine,
    /// `CORRUPT_OBJECT`, never delays the download that repairs it).
    ///
    /// **Order:** demand (`object_requests` younger than 24 h, newest first: a
    /// reader is waiting), then the active branch's current projection, then
    /// history. Within current and history, smaller remaining bytes first,
    /// with fair interleaving: once the list is about to fill (its last item,
    /// or three quarters of its byte budget), its last slot goes to the
    /// class's *largest* pending item, so a stream of small objects never
    /// starves a large one (it gets the rest of every pull's budget).
    ///
    /// **Offsets** are the committed durable spool offsets (invariant 2). A
    /// complete but unfinished spool (a crash before its install) is offered
    /// one byte short, so the sender's last byte, checked against the spool,
    /// finalizes it.
    pub fn want_objects(
        &self,
        lane: LaneSpec,
        max_items: usize,
        max_bytes: u64,
        leased_elsewhere: &dyn Fn(&str) -> bool,
    ) -> Result<Vec<WantItem>> {
        let max_items = max_items.min(MAX_WANT_ITEMS);
        let max_bytes = max_bytes.max(1);
        if max_items == 0 || !lane.is_valid() {
            return Ok(vec![]);
        }
        let now = time();
        let demand_since = now.saturating_sub(DEMAND_TTL_MS) as i64;
        let rows = self.want_rows()?;
        let mut classes: [Vec<Candidate>; 3] = [vec![], vec![], vec![]];
        let mut undecided = vec![];
        for row in rows.iter() {
            let object = row.hash.as_str();
            if !lane.owns(object) {
                continue;
            }
            if row.retry_at.is_some_and(|at| at > now as i64)
                && row.code.as_deref() != Some("CORRUPT_OBJECT")
            {
                continue;
            }
            if leased_elsewhere(object) {
                continue;
            }
            let size = row.size;
            let durable = row
                .durable
                .and_then(|d| u64::try_from(d).ok())
                .filter(|d| *d <= size && row.spool_total.is_none_or(|t| t == size))
                .unwrap_or(0);
            let offset = if durable == size && size > 0 {
                size - 1
            } else {
                durable
            };
            let demand = row.requested_ms.is_some_and(|at| at >= demand_since);
            let candidate = Candidate {
                remaining: size - offset,
                requested_ms: row.requested_ms.unwrap_or(0),
                item: WantItem {
                    hash: object.to_owned(),
                    size,
                    offset,
                    priority: if demand {
                        WantPriority::Demand
                    } else {
                        WantPriority::History
                    },
                },
            };
            if demand {
                classes[0].push(candidate);
            } else {
                undecided.push(candidate);
            }
        }
        if !undecided.is_empty() {
            let current = self.current_objects()?;
            for mut candidate in undecided {
                if current.contains(&candidate.item.hash) {
                    candidate.item.priority = WantPriority::Current;
                    classes[1].push(candidate);
                } else {
                    classes[2].push(candidate);
                }
            }
        }
        classes[0].sort_by(|a, b| {
            (b.requested_ms, a.remaining, &a.item.hash).cmp(&(
                a.requested_ms,
                b.remaining,
                &b.item.hash,
            ))
        });
        for class in &mut classes[1..] {
            class.sort_by(|a, b| (a.remaining, &a.item.hash).cmp(&(b.remaining, &b.item.hash)));
        }
        let fair_budget = max_bytes / FAIR_SHARE_DEN * FAIR_SHARE_NUM;
        let mut list = Vec::new();
        let mut bytes = 0u64;
        for (index, class) in classes.into_iter().enumerate() {
            let fair = index > 0;
            let mut pending: VecDeque<Candidate> = class.into();
            while let Some(front) = pending.front() {
                if list.len() >= max_items || bytes >= max_bytes {
                    return Ok(list);
                }
                // The fair slot: never the list's first item, and only while
                // the class has more than the one item it would take.
                let fair_slot = fair
                    && pending.len() > 1
                    && !list.is_empty()
                    && (list.len() + 1 == max_items
                        || bytes.saturating_add(front.remaining) >= max_bytes
                        || bytes >= fair_budget);
                let take = if fair_slot {
                    pending.pop_back()
                } else {
                    pending.pop_front()
                };
                let Some(candidate) = take else { break };
                bytes = bytes.saturating_add(candidate.remaining);
                list.push(candidate.item);
                if fair_slot {
                    // The fair slot ends the list.
                    return Ok(list);
                }
            }
        }
        Ok(list)
    }
}
