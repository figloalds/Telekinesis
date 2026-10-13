//! Status and content summaries of one store.
//!
//! `content_counters` is one SQL pass over the object indexes plus one walk of
//! the active projection. Physical presence comes from one cached listing of
//! `objects/` (`Store::presence`), never from a `stat` per object, so objects
//! deleted behind the store's back still show as missing. The result is cached
//! until the database (`total_changes`, `PRAGMA data_version`), the active
//! branch or the listing changes; spools, demand and item details are read
//! fresh on every call.
use super::objects::{CORRUPT, Presence};
use super::*;
use crate::model::health::ContentCounters;
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

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
        let (counters, items) = self.content_scan(CONTENT_ITEMS)?;
        Ok(
            json!({"items":items,"current_objects":counters.current_objects,"current_available":counters.current_available,
            "current_missing":counters.current_missing,"shared_objects":counters.shared_objects,
            "shared_available":counters.shared_available,"downloading":counters.downloading,
            "partial_bytes":counters.partial_bytes,"complete":counters.complete,
            "requested":counters.requested}),
        )
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
    pub fn status(&self) -> Result<Value> {
        let p = self.projection(&self.active)?;
        let outgoing = self.outgoing_unacknowledged()?;
        Ok(
            json!({"repo":self.repo,"device":self.device,"branch":self.branch(&self.active)?,"generation":self.generation,
            "content":self.content_status()?,"local_durability":"metadata:sqlite-full; content:per-object","outgoing_unacknowledged":outgoing,"incoming_pending":self.db.query_row("SELECT count(*) FROM incoming_events",[],|r|r.get::<_,i64>(0))?,"incoming_quarantined":self.db.query_row("SELECT count(*) FROM incoming_events WHERE error IS NOT NULL",[],|r|r.get::<_,i64>(0))?,"peer_protocol":PEER_FORMAT,
            "caught_up":"unknown-until-peer-roundtrip","pending_causal_events":p.pending,"unresolved_conflicts":self.conflicts()?.iter().filter(|c|c.resolved_by.is_none()).count()}),
        )
    }
}
