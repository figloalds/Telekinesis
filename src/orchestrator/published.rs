//! Owned by WP1.1 — the published-catalog cache (REVAMP_PLAN §3.2, §3.3,
//! §3.8; RC8).
//!
//! FolderControllers write one [`PublishedEntry`] per folder (after
//! `published-summary` and on heartbeats); the inbound server answers
//! `catalog.list` from [`PublishedCatalogApi::list`] in O(1): the reply list
//! is rebuilt once per change and shared as an `Arc<Vec>`, so serving it never
//! calls a worker and never waits for one (the legacy `list` fan-out called
//! every worker in turn inside one request).
//!
//! **Generation.** It increases by exactly one per update that changes the
//! list (setting an identical entry or removing a missing one is not a
//! change). It starts at the wall-clock milliseconds of [`PublishedCatalog::new`]
//! rather than at 0, so it keeps increasing across daemon restarts: a peer
//! that still holds `since` from the previous process gets the new list
//! instead of a false `unchanged:true` (`catalog.list{since}`, §3.8).
use crate::{
    model::orchestrator::{PublishedCatalogApi, PublishedEntry},
    util::{lock::Tracked, snapshot::Snapshot, time::now_ms},
};
use std::{collections::BTreeMap, fmt, sync::Arc, time::Duration};

/// The published-folder cache (implements [`PublishedCatalogApi`]).
pub struct PublishedCatalog {
    /// Generation of the empty list; each change adds one.
    base: u64,
    /// Entries keyed by folder state; the only writers' critical section.
    entries: Tracked<BTreeMap<String, PublishedEntry>>,
    /// The cached list in state order. Its own generation counts changes.
    list: Snapshot<Vec<PublishedEntry>>,
}

impl PublishedCatalog {
    /// An empty cache whose generation starts at the current wall-clock
    /// milliseconds (see the module docs).
    pub fn new() -> Arc<Self> {
        Self::with_base(now_ms())
    }

    /// An empty cache whose generation starts at `base` (tests and callers
    /// that need a deterministic generation).
    pub fn with_base(base: u64) -> Arc<Self> {
        Arc::new(Self {
            base,
            entries: Tracked::new("published.entries", BTreeMap::new()),
            list: Snapshot::new(Vec::new()),
        })
    }

    /// The current generation (the one `list()` reports).
    pub fn generation(&self) -> u64 {
        self.base.saturating_add(self.list.generation())
    }

    /// The entry of folder `state`, if published.
    pub fn entry(&self, state: &str) -> Option<PublishedEntry> {
        self.entries.lock().get(state).cloned()
    }
}

impl PublishedCatalogApi for PublishedCatalog {
    fn update(&self, state: &str, entry: Option<PublishedEntry>) {
        let mut entries = self.entries.lock();
        let changed = match entry {
            Some(entry) => {
                if entries.get(state) == Some(&entry) {
                    false
                } else {
                    entries.insert(state.to_owned(), entry);
                    true
                }
            }
            None => entries.remove(state).is_some(),
        };
        if changed {
            // Published while the entries are still held, so concurrent
            // updates publish their lists in the order they changed the map.
            let list: Vec<PublishedEntry> = entries.values().cloned().collect();
            self.list.store(list);
        }
    }

    fn list(&self) -> (u64, Arc<Vec<PublishedEntry>>) {
        let (list, generation) = self.list.load_versioned();
        (self.base.saturating_add(generation), list)
    }

    fn wait_newer(&self, generation: u64, timeout: Duration) -> u64 {
        let current = self.generation();
        if current > generation {
            return current;
        }
        // Here `generation >= current >= base`.
        let seen = generation - self.base;
        self.base
            .saturating_add(self.list.wait_newer(seen, timeout))
    }
}

impl fmt::Debug for PublishedCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (generation, list) = self.list();
        f.debug_struct("PublishedCatalog")
            .field("generation", &generation)
            .field("entries", &list.len())
            .finish()
    }
}
