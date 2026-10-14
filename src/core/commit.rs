//! The commit path: validation, the incremental successors (fast paths) and
//! the full-replay slow path with its forced snapshot seal.
//!
//! WP1.6 (REVAMP_PLAN §5.1): the creation and namespace successors also serve
//! remote events (kill switch `TKFS_REMOTE_FASTPATH=0`), and their conflict
//! gate reads only the unresolved conflicts of the event's own branch.
//!
//! Why a successor is a restriction of full replay: it requires
//! `dependencies == frontier` (sorted list equality), so every committed event
//! of the branch is a causal ancestor of the new event E. The only antichain
//! that contains E is `{E}`, whose cut is the whole history plus E: the
//! current projection plus E's changes. The successor checks reject every way
//! that cut could hold a new conflict (an occupied slot, a missing or dead
//! parent directory, a directory cycle, live children of a deleted entry, a
//! register with competing heads), and E is not a review. Location and
//! register pairs that include E are ancestor-related, so neither the pair
//! derivation nor `namespace_history` can add a record, and full mode's
//! DELETE of non-rederived namespace rows is a no-op. The device that
//! authored E plays no part in this argument; the old `device == self`
//! restriction only kept the remote case behind the slow path. The oracle
//! (`verify_derived_state`) is the executable check (`tests/activation_inbox.rs`).
//!
//! **The `NAMESPACE_HISTORY_LIMIT` count (CCR-WP1.6-5, WP2.4).** No record
//! changes, but the competing-cut count over *old* events can grow: a
//! location change to a parent or a `(parent, name)` slot the entity never
//! used joins cones (`NamespaceCones` unions historical parents and slot
//! sharers), and a first tombstone makes the entity's `alive = true` saves
//! eligible as TRUE-presence witnesses. Full replay refuses such an event
//! before its ACK once a cone exceeds `MAX_NAMESPACE_CUTS`. Since WP2.4 a
//! namespace successor counts too: `derive::successor_may_add_cuts` finds the
//! events that can change a count (a scan of the branch's cached events, no
//! derivation), and for those `derive::check_successor_cuts` counts every
//! cone that contains a touched entity exactly as full mode does and refuses
//! with full mode's error. The creation successor needs no count (a fresh
//! entity joins one existing cone), nor does the content successor (it adds
//! no relevant event and only a TRUE version that descends from all).
//!
//! # Off-lock slow path (WP2.4, REVAMP_PLAN §4.4, §14 WP2.4)
//!
//! A slow commit is split into a computation and a write. The computation
//! (`compute_slow`) reads the branch's events from the event cache, projects
//! them, derives the conflict records (`derive::derive_conflicts`, pure) and
//! works out how the projection is stored (the `current_parts` delta of
//! CCR-WP1.INT-4, or the precomputed `current_state` JSON of a full
//! rewrite). It needs no lock and no writer, so `inbox::prepare_slow` runs
//! it on a read store. The write (`write_commit`) is the same for the inline
//! and the prepared path: the journal rows, the stored projection, the
//! records (`derive::write_projection`), the frontier and the forced
//! snapshot seal of the projection the commit holds, in one savepoint.
//!
//! # Event cache (`Cache::events`)
//!
//! Per branch (the [`EVENT_CACHE_BRANCHES`] most recently used), the
//! branch's events, parsed and in `id` order (exactly `Store::events()`
//! filtered by branch, the slow path's historical input). Each entry is
//! keyed by the journal: `upto` (the global `max(seq)` it covers) and
//! `total` (the global journal row count then). A use reads both (one
//! aggregate over `journal_sequence`), loads only the rows with a larger
//! `seq` (a range scan; sequences grow and are never reused once committed)
//! and checks that the row count grew by exactly those rows; any deletion
//! breaks that equation and reloads the branch in full.
//!
//! Rows read inside a write transaction may still roll back, and a
//! rolled-back sequence number is reused by the next insert (AUTOINCREMENT
//! state rolls back with the transaction), which neither `upto` nor `total`
//! can see. So every journal row an entry covers that is not known
//! committed is kept as *tentative*: the exact `(seq, id)` list of the
//! journal range `(after, upto]`, **every** row of it whatever its branch
//! (a rolled-back row of another branch frees a sequence number that a row
//! of this branch can take: WP2.4 fix round 1). Before every use the range
//! is read again (one range scan) and must be exactly that list; once a use
//! in autocommit (or `settle_event_cache`, after `with_write`'s transaction)
//! finds it intact, it is committed and dropped. A full load inside a write
//! transaction is kept the same way: every row above the newest journal
//! sequence this connection last read in a committed state
//! (`committed_tip`) is tentative (rows at or below it were committed then
//! and stay so). Events are immutable (invariant 6), and a staged or
//! committed id never carries another payload (`EVENT_ID_REUSED`), so a row
//! known by `(seq, id)` never changes its payload.
//!
//! # Rolled-back commits and the view (WP2.4 fix round 2)
//!
//! A commit patches (or seeds) the connection's cached view, and the view's
//! freshness key (`total_changes`, `data_version`, an open transaction)
//! cannot see a rollback by a caller the store never hears of (a runtime
//! request's `ROLLBACK`, or its `ROLLBACK TO` inside a caller's
//! transaction): the key matches again as soon as a transaction is open,
//! and the view still holds the rolled-back commit. The commit path must not
//! build on such a view: the successor checks and deltas, the
//! stored-projection delta of a slow commit and the periodic seal written
//! from the patched view (CCR-WP1.6-1 adapter) all read it. So every commit
//! also bumps a counter in a TEMP table of its connection, inside its
//! savepoint (`temp.tkfs_commit_epoch`: a rollback of the commit, at any
//! nesting, undoes the bump), and `Cache::epoch` holds the value the
//! connection's own commits left. Before the commit path reads the view
//! (`drop_rolled_back_view`), a different stored value means a commit was
//! rolled back since: the view is dropped (its next use loads the stored
//! projection) and the counter is resynchronized. A handle that never
//! committed (a read store) never reads the table. Readers outside the
//! commit path (`Store::view`) keep the view until the next commit or write
//! transaction drops it (CCR-WP2.4-6).
use super::derive::{self, DeriveInput, DeriveOutput, derive_conflicts};
use super::*;
use std::{
    cell::{Cell, RefCell},
    sync::OnceLock,
};

/// The view slots (`(parent, name key)` → entity) of the alive entries of
/// `projection`; `None` if a name cannot be keyed, which a committed
/// projection never has.
fn view_slots(projection: &Projection) -> Option<BTreeMap<(String, String), String>> {
    projection
        .entries
        .values()
        .filter(|e| e.alive)
        .map(|e| Ok(((e.parent.clone(), name_key(&e.name)?), e.id.clone())))
        .collect::<Result<_>>()
        .ok()
}

/// The view of `branch` after a slow commit stored `projection` (`None` if a
/// name cannot be keyed, which a committed projection never has). `slots`
/// are its view slots when already computed (off-lock).
fn slow_commit_view(
    branch: &str,
    projection: Projection,
    slots: Option<BTreeMap<(String, String), String>>,
    changes: u64,
    data_version: i64,
    transactional: bool,
) -> Option<ViewCache> {
    let slots = match slots {
        Some(slots) => slots,
        None => view_slots(&projection)?,
    };
    Some(ViewCache {
        branch: branch.into(),
        changes,
        data_version,
        transactional,
        projection,
        slots,
    })
}

/// The head registers a `current_parts` row stores for its entity.
const PART_FIELDS: [&str; 5] = ["kind", "location", "alive", "content", "basic"];

/// The journal distance between periodic seals of a fast commit; mirrors
/// `snapshot::INTERVAL`, which decides. Used only to know beforehand that a
/// seal is due, so the seal can serialize the projection the commit holds
/// instead of reading it back (CCR-WP1.6-1 adapter); a mismatch would only
/// cost that read.
const SEAL_INTERVAL: i64 = 128;

/// Upserts the `current_parts` row of `entity` (its entry and its head
/// registers) from projection `p`, which must hold the entry.
fn write_part(tx: &Connection, branch: &str, entity: &str, p: &Projection) -> Result<()> {
    let heads: BTreeMap<_, _> = PART_FIELDS
        .into_iter()
        .filter_map(|field| {
            let key = register(entity, field);
            p.heads.get(&key).map(|v| (key, v))
        })
        .collect();
    tx.execute("INSERT INTO current_parts VALUES(?,?,?,?) ON CONFLICT(branch,entity) DO UPDATE SET entry=excluded.entry,heads=excluded.heads",
        params![branch, entity, serde_json::to_string(&p.entries[entity])?, serde_json::to_string(&heads)?])?;
    Ok(())
}

/// Commit cache: fast/slow commit counters and the event cache (module docs).
#[derive(Default)]
pub(crate) struct Cache {
    fast: Cell<u64>,
    slow: Cell<u64>,
    /// Commits made inside an enclosing transaction (event id → fast), not
    /// counted until that transaction is known to have committed them.
    pending: RefCell<BTreeMap<String, bool>>,
    /// Per-branch events, least recently used first (module docs, "Event
    /// cache").
    events: RefCell<Vec<EventCache>>,
    /// The newest journal sequence this connection read in a committed
    /// state (autocommit, or a read transaction of its own): every row at
    /// or below it was committed then, and so stays (module docs).
    committed_tip: Cell<Option<i64>>,
    /// Full loads of a branch into the event cache (tests).
    event_loads: Cell<u64>,
    /// Namespace successor commits whose competing cuts were counted
    /// (CCR-WP1.6-5).
    successor_checks: Cell<u64>,
    /// Periodic seals of fast commits that serialized the patched view
    /// (CCR-WP1.6-1 adapter).
    known_seals: Cell<u64>,
    /// Forced seals of prepared slow commits written from their off-lock
    /// parts (`seal_prepared`; tests).
    prepared_seals: Cell<u64>,
    /// The `temp.tkfs_commit_epoch` value this connection's own commits left
    /// (0: none yet; module docs, "Rolled-back commits and the view").
    epoch: Cell<i64>,
    /// Views dropped because a commit was rolled back around the store
    /// (tests).
    rolled_back_views: Cell<u64>,
}

/// Bumps the connection's commit counter inside the commit's savepoint and
/// returns the new value (module docs, "Rolled-back commits and the view").
/// `held`: the value the connection's commits left before this one; the
/// TEMP table is created by the first commit (or again, after a rollback
/// undid its creation).
fn bump_commit_epoch(tx: &Connection, held: i64) -> Result<i64> {
    if held == 0 {
        tx.execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS tkfs_commit_epoch(id INTEGER PRIMARY KEY CHECK(id=0), n INTEGER NOT NULL)",
        )?;
    }
    Ok(tx
        .prepare_cached(
            "INSERT INTO temp.tkfs_commit_epoch VALUES(0,1) ON CONFLICT(id) DO UPDATE SET n=n+1 RETURNING n",
        )?
        .query_row([], |r| r.get(0))?)
}

/// How many branches the event cache keeps (the least recently used is
/// dropped first). The activator alternates between a few branches at most
/// (shared branches with remote events, the active one with local
/// operations); each entry holds its branch's parsed events.
const EVENT_CACHE_BRANCHES: usize = 4;

/// The cached events of one branch (module docs, "Event cache").
struct EventCache {
    branch: String,
    /// In `id` order.
    events: Vec<Event>,
    /// The global journal `max(seq)` the cache covers.
    upto: i64,
    /// The global journal row count when `upto` was read.
    total: i64,
    /// The rows the cache covers that are not known committed (`None`: all
    /// are).
    tentative: Option<Tentative>,
    /// The startup-snapshot journal hash state over the branch's rows (in
    /// sequence order) and the last sequence number it covers (0: none),
    /// kept with the rows: the off-lock part of a prepared commit's seal.
    journal: (Sha256, i64),
}

/// Journal rows an event cache entry covers that may still roll back.
struct Tentative {
    /// Every row with a sequence number at or below `after` is known
    /// committed.
    after: i64,
    /// Every journal row in `(after, upto]`, all branches, in `seq` order,
    /// as read: `(seq, event id)`.
    rows: Vec<(i64, String)>,
}

/// How many commits on this handle took an incremental successor (`fast`)
/// or the full-replay slow path (`slow`) since it was opened. Only durable
/// commits count: a commit made inside an enclosing transaction (an
/// activation slice, a control request) counts once the connection is back
/// in autocommit with the event in the journal, so a commit whose enclosing
/// transaction rolled back is never counted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitPathCounters {
    pub fast: u64,
    pub slow: u64,
}

/// `TKFS_REMOTE_FASTPATH` (read once per process): any value but `0` lets
/// remote events use the creation and namespace successors. `0` restores the
/// previous gate exactly: those successors serve only this device's events,
/// and any conflict row of the branch (resolved or not) disables them.
pub fn remote_fast_paths_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("TKFS_REMOTE_FASTPATH").as_deref() != Ok("0"))
}

/// Fault point `core.commit.busy` (`util::fault`): when it fires, the commit
/// fails with a genuine `SQLITE_BUSY` before writing anything.
const BUSY_FAULT: &str = "core.commit.busy";

/// The validated parts of an event that `commit_checked` needs.
pub(super) struct Checked {
    payload: String,
    objects: BTreeSet<String>,
}

/// Result of `check_commit`.
pub(super) enum Check {
    /// The identical event is already in the journal (idempotent commit).
    Committed,
    Ready(Checked),
}

/// A slow commit, computed (`Store::compute_slow`): everything its write
/// needs, so the write takes no derivation (WP2.4).
pub(super) struct SlowPlan {
    /// The branch's projection with the event.
    pub(super) projection: Projection,
    /// Its conflict records (`derive::derive_conflicts`).
    pub(super) derived: DeriveOutput,
    /// CCR-WP1.INT-4: the entities whose `current_parts` rows make the
    /// stored projection read `projection`, or `None` for the full rewrite.
    delta: Option<BTreeSet<String>>,
    /// The projection's JSON, serialized off-lock: the `current_state`
    /// payload of a full rewrite and the sealed snapshot's `projection`.
    json: Option<String>,
    /// The startup-snapshot journal hash over the branch's rows up to the
    /// computation's state, and the last of those sequence numbers (from the
    /// event cache), extended by the event under the lock (`seal_prepared`).
    journal: Option<(Sha256, i64)>,
    /// The view slots of `projection`, computed off-lock.
    slots: Option<BTreeMap<(String, String), String>>,
}

impl std::fmt::Debug for SlowPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlowPlan")
            .field("entries", &self.projection.entries.len())
            .field("conflicts", &self.derived.conflicts().len())
            .field("delta", &self.delta.as_ref().map(BTreeSet::len))
            .field("json_bytes", &self.json.as_ref().map(String::len))
            .field(
                "journal_through",
                &self.journal.as_ref().map(|(_, seq)| seq),
            )
            .finish()
    }
}

/// How a commit stores its projection.
pub(super) enum Plan {
    /// An incremental successor's delta (the changed entities' entries and
    /// head registers).
    Fast(Projection),
    /// Full replay, computed.
    Slow(Box<SlowPlan>),
}

impl Store {
    pub fn commit(&mut self, event: Event, local: bool) -> Result<()> {
        if local {
            return self.commit_ready(event, true);
        }
        // Trusted core callers supply the actual local bytes explicitly. The
        // network path uses receive directly and never promotes global CAS hashes.
        let objects = event
            .objects()
            .into_iter()
            .map(|h| Ok((h.clone(), hex::encode(self.read_object(&h)?))))
            .collect::<Result<_>>()?;
        let revision = event.id.clone();
        let device = event.device.clone();
        self.receive(
            Bundle {
                repo: self.repo.clone(),
                branches: vec![],
                events: vec![event],
                objects,
            },
            &BTreeSet::from([device]),
        )?;
        let row: Option<(Option<String>, Option<String>, Option<String>)> = self
            .db
            .query_row(
                "SELECT error,state,code FROM incoming_events WHERE id=?",
                [&revision],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        match row {
            Some((Some(error), _, _)) => bail!("{error}"),
            // A transient failure is retried later; the event is not committed.
            Some((None, Some(state), code)) if state == "retry" => bail!(
                "{}: transient activation failure; retry scheduled",
                code.as_deref().unwrap_or("SQLITE_BUSY")
            ),
            _ => Ok(()),
        }
    }
    pub(super) fn commit_ready(&mut self, event: Event, local: bool) -> Result<()> {
        let Check::Ready(checked) = self.check_commit(&event, local)? else {
            return Ok(());
        };
        let fast = self.fast_successor(&event, local)?;
        self.commit_checked(event, local, checked, fast)
    }
    /// Every validation of the commit path that needs no projection.
    pub(super) fn check_commit(&self, event: &Event, local: bool) -> Result<Check> {
        ensure!(event.repo == self.repo, "REPOSITORY_MISMATCH");
        ensure!(
            event.id.len() <= 128 && !event.id.is_empty() && event.device.len() <= 128,
            "INVALID_EVENT"
        );
        ensure!(
            !event.changes.is_empty() && event.changes.len() <= 10000,
            "INVALID_EVENT"
        );
        ensure!(local || event.branch.shared, "PRIVATE_EVENT_REJECTED");
        let payload = serde_json::to_string(event)?;
        ensure!(payload.len() <= MAX_EVENT, "EVENT_METADATA_TOO_LARGE");
        name_key(&event.branch.name)?;
        ensure!(
            !event.branch.id.is_empty() && event.branch.id.len() <= 128,
            "INVALID_BRANCH_ID"
        );
        let existing: Option<String> = self
            .db
            .query_row("SELECT payload FROM events WHERE id=?", [&event.id], |r| {
                r.get(0)
            })
            .optional()?;
        if let Some(old) = existing {
            ensure!(old == payload, "EVENT_ID_REUSED");
            return Ok(Check::Committed);
        }
        let objects = event.objects();
        ensure!(
            event.object_sizes.keys().all(|h| objects.contains(h)),
            "UNREFERENCED_OBJECT_SIZE"
        );
        for object in &objects {
            valid_object(object)?;
            if let Some(size) = event.object_sizes.get(object) {
                ensure!(*size <= MAX_CONTENT, "FILE_TOO_LARGE");
                if let Ok(old) = self.object_size(object) {
                    ensure!(old == *size, "OBJECT_SIZE_CHANGED");
                }
            }
            // Newly authored bytes must exist; metadata decisions may refer to
            // previously accepted, currently unavailable revisions.
            if local
                && !self.db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM object_references WHERE hash=?)",
                    [object],
                    |r| r.get::<_, bool>(0),
                )?
            {
                self.verify_object(object)?;
            }
        }
        let mut seen = BTreeSet::new();
        for change in &event.changes {
            ensure!(
                seen.insert(register(&change.entity, &change.field)),
                "DUPLICATE_REGISTER"
            );
            ensure!(
                change.entity != ROOT && !change.entity.is_empty() && change.entity.len() <= 128,
                "INVALID_ENTITY"
            );
            ensure!(
                ["kind", "location", "content", "alive", "basic"].contains(&change.field.as_str()),
                "INVALID_FIELD"
            );
            ensure!(!change.parents.contains(&event.id), "CAUSAL_CYCLE");
            match change.field.as_str() {
                "location" => {
                    name_key(change.value["name"].as_str().context("INVALID_LOCATION")?)?;
                    ensure!(
                        change.value["parent"].as_str().is_some(),
                        "INVALID_LOCATION"
                    );
                }
                "kind" => ensure!(
                    [json!("file"), json!("directory")].contains(&change.value),
                    "INVALID_KIND"
                ),
                "alive" => ensure!(change.value.is_boolean(), "INVALID_ALIVE"),
                "content" => ensure!(change.value.is_string(), "INVALID_CONTENT"),
                "basic" => serde_json::from_value::<BasicInfo>(change.value.clone())?.validate()?,
                _ => {}
            }
        }
        if let Ok(b) = self.branch(&event.branch.id) {
            ensure!(b == event.branch, "BRANCH_DESCRIPTOR_CHANGED");
        }
        Ok(Check::Ready(Checked { payload, objects }))
    }
    /// The incremental projection delta for `event`, if a successor proves it
    /// equal to full replay; `None` means the slow path. A namespace
    /// successor first counts the competing cuts full replay would count
    /// (CCR-WP1.6-5, module docs) and refuses as it would.
    pub(super) fn fast_successor(&self, event: &Event, local: bool) -> Result<Option<Projection>> {
        self.fast_successor_in(event, local, false)
    }
    /// `fast_successor`; `committed_view`: the connection reads committed
    /// state only (a read transaction of its own), so the event cache may
    /// keep what it loads.
    pub(super) fn fast_successor_in(
        &self,
        event: &Event,
        local: bool,
        committed_view: bool,
    ) -> Result<Option<Projection>> {
        self.drop_rolled_back_view();
        let mut fast = self.content_successor(event)?;
        if fast.is_none() && (local || remote_fast_paths_enabled()) {
            fast = self.creation_successor(event)?;
            if fast.is_none() {
                fast = self.namespace_successor(event)?;
                if fast.is_some() {
                    self.check_namespace_successor(event, committed_view)?;
                }
            }
        }
        Ok(fast)
    }
    /// CCR-WP1.6-5: refuse a namespace successor commit exactly when full
    /// replay would refuse it with `NAMESPACE_HISTORY_LIMIT` (module docs).
    fn check_namespace_successor(&self, event: &Event, committed_view: bool) -> Result<()> {
        let branch = event.branch.id.as_str();
        self.with_branch_events(branch, Some(event), committed_view, |events, _| {
            let new = events.len() - 1;
            if !derive::successor_may_add_cuts(events, new) {
                return Ok(());
            }
            let counted = &self.caches.commit.successor_checks;
            counted.set(counted.get().saturating_add(1));
            derive::check_successor_cuts(branch, events, new)
        })?
    }
    /// How many namespace successor commits had their competing cuts counted
    /// on this handle (CCR-WP1.6-5; telemetry and tests).
    #[doc(hidden)]
    pub fn successor_cut_checks(&self) -> u64 {
        self.caches.commit.successor_checks.get()
    }
    /// How many periodic seals of fast commits on this handle serialized the
    /// patched view instead of reading the projection back (CCR-WP1.6-1
    /// adapter; tests). A seal whose transaction rolled back counts too.
    #[doc(hidden)]
    pub fn known_projection_seals(&self) -> u64 {
        self.caches.commit.known_seals.get()
    }
    /// How many times this handle dropped its view because a commit it had
    /// made was rolled back around the store (tests).
    #[doc(hidden)]
    pub fn rolled_back_views(&self) -> u64 {
        self.caches.commit.rolled_back_views.get()
    }
    /// Drops the cached view if a commit of this connection was rolled back
    /// since its last commit (module docs, "Rolled-back commits and the
    /// view"). Call before the commit path reads the view. No query on a
    /// handle that never committed.
    pub(super) fn drop_rolled_back_view(&self) {
        let cache = &self.caches.commit;
        let held = cache.epoch.get();
        if held == 0 {
            return;
        }
        // No table (its creation rolled back, or a replaced connection): 0.
        let stored: i64 = self
            .db
            .prepare_cached("SELECT n FROM temp.tkfs_commit_epoch WHERE id=0")
            .and_then(|mut statement| statement.query_row([], |r| r.get(0)).optional())
            .ok()
            .flatten()
            .unwrap_or(0);
        if stored != held {
            // `Store::view` calls this too (CCR-WP2.4-6, applied by WP2.INT),
            // possibly while an outer reader still holds the view: then the
            // drop waits for the next call (the epoch is left unsynced).
            let Ok(mut view) = self.view_cache.try_borrow_mut() else {
                return;
            };
            view.take();
            cache.epoch.set(stored);
            let dropped = &cache.rolled_back_views;
            dropped.set(dropped.get().saturating_add(1));
        }
    }
    /// How a slow-path commit stores its projection `p` on `branch`
    /// (WP1.INT fix round 1, CCR-WP1.INT-4): `Some(entities)` when writing
    /// the `current_parts` rows of those entities makes the stored projection
    /// (`snapshot::load_projection`: `current_state` overlaid with
    /// `current_parts`) read exactly `p`; `None` for the full rewrite of
    /// `current_state` (`persist_projection`).
    ///
    /// The full rewrite stores the whole projection (about 7 MB at 5K
    /// entries) on every slow commit, as much again as the forced snapshot
    /// seal, and that I/O made slow commits spike past the
    /// `concurrent_namespace_ops_5k` floor. A delta is possible when the
    /// stored projection (the connection's view, fresh by its own contract,
    /// else loaded) differs from `p` only in entries and their head
    /// registers, with no key removed, every changed register belonging to an
    /// entry of `p` and to one of the [`PART_FIELDS`], and the same
    /// `conflicts` and `pending`. Otherwise (no stored projection yet, any
    /// other difference, or a read error), and whenever the branch's parts
    /// would outnumber half of its entries, the full rewrite runs, which also
    /// folds the parts back into `current_state` (`load_projection` cost
    /// stays bounded; the rewrites happen as the entries double, so their
    /// cost is amortized). The derived-state oracle (`verify_derived_state`)
    /// compares exactly this stored projection with a rebuild.
    ///
    /// WP2.4: a prepared commit computes this on the read store, against the
    /// stored projection of the branch state it derived from; the write
    /// happens only if the branch is still in that state (`commit_prepared`).
    fn projection_delta(&self, branch: &str, p: &Projection) -> Option<BTreeSet<String>> {
        self.drop_rolled_back_view();
        let view = self.view(branch).ok()?;
        let stored = &view.projection;
        if stored.conflicts != p.conflicts
            || stored.pending != p.pending
            || !stored.entries.keys().all(|id| p.entries.contains_key(id))
            || !stored.heads.keys().all(|key| p.heads.contains_key(key))
        {
            return None;
        }
        let mut changed: BTreeSet<String> = p
            .entries
            .iter()
            .filter(|(id, entry)| stored.entries.get(*id) != Some(*entry))
            .map(|(id, _)| id.clone())
            .collect();
        for (key, versions) in &p.heads {
            if stored.heads.get(key) == Some(versions) {
                continue;
            }
            let (entity, field) = key.rsplit_once('/')?;
            if !PART_FIELDS.contains(&field) || !p.entries.contains_key(entity) {
                return None;
            }
            changed.insert(entity.to_owned());
        }
        drop(view);
        let parts: i64 = self
            .db
            .query_row(
                "SELECT count(*) FROM current_parts WHERE branch=?",
                [branch],
                |r| r.get(0),
            )
            .ok()?;
        let parts = usize::try_from(parts).ok()?;
        (parts.saturating_add(changed.len()) <= p.entries.len() / 2).then_some(changed)
    }
    /// The slow path's computation for `event` (WP2.4): the branch's events
    /// (the event cache) plus `event`, projected; `None` when the projection
    /// is pending (a prerequisite outside the branch's journal); then the
    /// stored rows `event` reviews, the derived records and how the
    /// projection is stored. Reads only; with `precompute` also serializes a
    /// full rewrite's `current_state` and computes the view slots, so that
    /// the write does no O(projection) work. Errors are the slow path's
    /// refusals, as the historical inline path raised them.
    pub(super) fn compute_slow(
        &self,
        event: &Event,
        committed_view: bool,
        precompute: bool,
    ) -> Result<Option<Box<SlowPlan>>> {
        let branch = event.branch.id.as_str();
        let computed =
            self.with_branch_events(branch, Some(event), committed_view, |events, journal| {
                let projection = project(branch, events)?;
                if !projection.pending.is_empty() {
                    return Ok(None);
                }
                let reviewed = derive::read_reviewed(&self.db, branch, event)?;
                let derived = derive_conflicts(&DeriveInput {
                    branch,
                    projection: &projection,
                    events,
                    new_event: Some(&event.id),
                    reviewed: &reviewed,
                })?;
                let journal = precompute.then(|| journal.clone());
                Ok::<_, anyhow::Error>(Some((projection, derived, journal)))
            })??;
        let Some((projection, derived, journal)) = computed else {
            return Ok(None);
        };
        let delta = self.projection_delta(branch, &projection);
        let (json, slots) = if precompute {
            (
                Some(serde_json::to_string(&projection)?),
                view_slots(&projection),
            )
        } else {
            (None, None)
        };
        Ok(Some(Box::new(SlowPlan {
            projection,
            derived,
            delta,
            json,
            journal,
            slots,
        })))
    }
    /// Writes a checked event: through `fast` (an incremental delta), or by
    /// full replay of the branch when `fast` is `None` (computed here, under
    /// the caller's lock: the inline slow path).
    pub(super) fn commit_checked(
        &mut self,
        event: Event,
        local: bool,
        checked: Checked,
        fast: Option<Projection>,
    ) -> Result<()> {
        let (plan, view_base) = match fast {
            Some(delta) => (Plan::Fast(delta), None),
            None => {
                // Another connection's commit after this point bumps it (see
                // the view seeding in `write_commit`).
                let base = self.data_version().ok();
                match self.compute_slow(&event, false, false)? {
                    Some(plan) => (Plan::Slow(plan), base),
                    None => return Err(CausalPrerequisitesPending.into()),
                }
            }
        };
        self.write_commit(event, local, checked, plan, view_base)
    }
    /// The write of a commit (WP2.4: shared by the inline and the prepared
    /// slow path; no derivation here). `view_base`: for a slow commit, the
    /// `data_version` under which its projection was computed from this
    /// connection's state; the projection becomes the connection's view if
    /// no other connection committed since.
    pub(super) fn write_commit(
        &mut self,
        event: Event,
        local: bool,
        checked: Checked,
        plan: Plan,
        view_base: Option<i64>,
    ) -> Result<()> {
        if crate::util::fault::hit(BUSY_FAULT).is_some() {
            return Err(injected_busy().into());
        }
        let result = self.write_commit_inner(event, local, checked, plan, view_base);
        if result.is_err() {
            // The view may have been patched for the seal.
            self.view_cache.borrow_mut().take();
        }
        result
    }
    fn write_commit_inner(
        &mut self,
        event: Event,
        local: bool,
        checked: Checked,
        plan: Plan,
        view_base: Option<i64>,
    ) -> Result<()> {
        let Checked { payload, objects } = checked;
        let incremental = matches!(plan, Plan::Fast(_));
        let branch = event.branch.id.clone();
        // Never patch (or seal) a view that holds a rolled-back commit.
        self.drop_rolled_back_view();
        // A fast commit patches the view if no other connection committed
        // since it was loaded (I6).
        let patchable = incremental && {
            let data_version = self.data_version().ok();
            self.view_cache
                .borrow()
                .as_ref()
                .is_some_and(|v| v.branch == branch && Some(v.data_version) == data_version)
        };
        fault("before_sql");
        let tx = self.db.savepoint()?;
        for object in &objects {
            tx.execute(
                "INSERT OR IGNORE INTO object_references VALUES(?)",
                [object],
            )?;
        }
        for (object, size) in &event.object_sizes {
            tx.execute(
                "INSERT OR IGNORE INTO object_sizes VALUES(?,?)",
                params![object, size],
            )?;
        }
        // Explicit content authorship/publication grants sharing. A resolution
        // of an inbound hash alone must not expose private-only local bytes.
        if local && event.branch.shared && event.reviewed.is_empty() {
            for object in &objects {
                if self.root.join("objects").join(object).is_file() {
                    tx.execute(
                        "INSERT OR IGNORE INTO published_objects VALUES(?)",
                        [object],
                    )?;
                }
            }
        }
        tx.execute(
            "INSERT OR IGNORE INTO branches VALUES(?,?,?)",
            params![event.branch.id, event.branch.name, event.branch.shared],
        )?;
        // A prepared seal checks the branch's journal against the one its
        // hash covers (`seal_prepared`): the branch's newest sequence before
        // this commit.
        let prior = match &plan {
            Plan::Slow(slow) if slow.json.is_some() && slow.journal.is_some() => {
                Some(frontier_seq(&tx, &branch)?)
            }
            _ => None,
        };
        tx.execute(
            "INSERT INTO events VALUES(?,?,?)",
            params![event.id, event.branch.id, payload],
        )?;
        if event.branch.shared {
            tx.execute("INSERT INTO outbox(event) VALUES(?)", [&event.id])?;
        }
        match &plan {
            Plan::Fast(delta) => {
                for entity in event
                    .changes
                    .iter()
                    .map(|c| &c.entity)
                    .collect::<BTreeSet<_>>()
                {
                    write_part(&tx, &branch, entity, delta)?;
                }
            }
            Plan::Slow(slow) => {
                if let Some(changed) = &slow.delta {
                    // The stored projection becomes `projection` through the
                    // rows of the entities that changed (CCR-WP1.INT-4).
                    for entity in changed {
                        write_part(&tx, &branch, entity, &slow.projection)?;
                    }
                } else {
                    let state = match &slow.json {
                        Some(state) => std::borrow::Cow::Borrowed(state.as_str()),
                        None => std::borrow::Cow::Owned(serde_json::to_string(&slow.projection)?),
                    };
                    tx.execute("DELETE FROM current_parts WHERE branch=?", [&branch])?;
                    tx.execute("INSERT INTO current_state VALUES(?,?) ON CONFLICT(branch) DO UPDATE SET payload=excluded.payload", params![branch, state])?;
                }
                derive::write_projection(&tx, &branch, &slow.derived)?;
            }
        }
        for parent in event
            .dependencies
            .iter()
            .chain(event.changes.iter().flat_map(|c| c.parents.iter()))
        {
            tx.execute(
                "DELETE FROM event_frontier WHERE branch=? AND event=?",
                params![branch, parent],
            )?;
        }
        tx.execute(
            "INSERT INTO event_frontier VALUES(?,?)",
            params![branch, event.id],
        )?;
        let mut patched = false;
        let plan = match plan {
            Plan::Fast(delta) => {
                // A periodic seal serializes the projection the commit holds
                // (the view, patched now) instead of reading it back and
                // parsing it (CCR-WP1.6-1 adapter; the payload is the same).
                let mut delta = Some(delta);
                if patchable && seal_due(&tx, &branch)? {
                    let mut cache = self.view_cache.borrow_mut();
                    if let (Some(view), Some(delta)) = (cache.as_mut(), delta.take()) {
                        if patch_view(view, &event, delta).is_ok() {
                            patched = true;
                            let seals = &self.caches.commit.known_seals;
                            seals.set(seals.get().saturating_add(1));
                        } else {
                            *cache = None;
                        }
                    }
                }
                let cache = self.view_cache.borrow();
                let known = cache
                    .as_ref()
                    .filter(|_| patched)
                    .map(|view| &view.projection);
                snapshot::maybe_persist(&tx, &branch, false, known)?;
                drop(cache);
                Plan::Fast(delta.unwrap_or_default())
            }
            Plan::Slow(slow) => {
                // The forced seal after a slow-path commit is kept (REVAMP_PLAN
                // P4). It seals the projection this commit holds (and has just
                // stored); a prepared commit brings its payload's expensive
                // parts computed off-lock.
                let sealed = match (&slow.json, &slow.journal, prior) {
                    (Some(json), Some(journal), Some(prior)) => {
                        seal_prepared(&tx, &branch, &event, &payload, json, journal, prior)?
                    }
                    _ => false,
                };
                if sealed {
                    let seals = &self.caches.commit.prepared_seals;
                    seals.set(seals.get().saturating_add(1));
                }
                if !sealed {
                    snapshot::maybe_persist(&tx, &branch, true, Some(&slow.projection))?;
                }
                Plan::Slow(slow)
            }
        };
        let epoch = bump_commit_epoch(&tx, self.caches.commit.epoch.get())?;
        tx.commit()?;
        self.caches.commit.epoch.set(epoch);
        self.count_commit(&event.id, incremental);
        // Patch the view only if no other connection committed since it was
        // loaded (I6); otherwise drop it. Never fail after the commit.
        let data_version = self.data_version().ok();
        let mut cache = self.view_cache.borrow_mut();
        match plan {
            Plan::Fast(_) if patched => match cache.as_mut() {
                Some(view) if Some(view.data_version) == data_version => {
                    view.changes = self.db.total_changes();
                    view.transactional = !self.db.is_autocommit();
                }
                _ => *cache = None,
            },
            Plan::Fast(delta)
                if cache.as_ref().is_some_and(|v| {
                    v.branch == branch && Some(v.data_version) == data_version
                }) =>
            {
                let view = cache.as_mut().unwrap();
                if patch_view(view, &event, delta).is_err() {
                    *cache = None;
                } else {
                    view.changes = self.db.total_changes();
                    view.transactional = !self.db.is_autocommit();
                }
            }
            Plan::Slow(slow) => {
                *cache = match view_base.filter(|base| Some(*base) == data_version) {
                    // A slow commit stored exactly `projection`, and no other
                    // connection committed since its history was read: it is
                    // the fresh view, so the next reader does not load and
                    // parse the whole projection again (WP1.INT fix round 1).
                    Some(version) => {
                        let SlowPlan {
                            projection, slots, ..
                        } = *slow;
                        slow_commit_view(
                            &branch,
                            projection,
                            slots,
                            self.db.total_changes(),
                            version,
                            !self.db.is_autocommit(),
                        )
                    }
                    None => None,
                };
            }
            Plan::Fast(_) => *cache = None,
        }
        drop(cache);
        fault("sql_committed");
        Ok(())
    }
    /// Commits on this handle by path (see [`CommitPathCounters`]).
    pub fn commit_path_counters(&self) -> CommitPathCounters {
        self.settle_commit_counters();
        CommitPathCounters {
            fast: self.caches.commit.fast.get(),
            slow: self.caches.commit.slow.get(),
        }
    }
    /// Count the commit of `id` once it is durable: now when its savepoint
    /// was the outermost transaction, else when it settles.
    fn count_commit(&self, id: &str, fast: bool) {
        let cache = &self.caches.commit;
        // An entry for the same event is an attempt whose enclosing
        // transaction rolled back: `check_commit` found it absent from the
        // journal before this commit.
        cache.pending.borrow_mut().remove(id);
        if self.db.is_autocommit() {
            self.settle_commit_counters();
            self.bump_commit_counter(fast);
        } else {
            cache.pending.borrow_mut().insert(id.to_owned(), fast);
        }
    }
    /// Count the pending commits that became durable; drop the rolled-back
    /// ones. Only in autocommit, when no enclosing transaction is open (any
    /// pending commit's transaction has then ended). A lookup failure keeps
    /// the entry for a later settle.
    pub(super) fn settle_commit_counters(&self) {
        if !self.db.is_autocommit() {
            return;
        }
        let pending = std::mem::take(&mut *self.caches.commit.pending.borrow_mut());
        for (id, fast) in pending {
            let durable = self
                .db
                .prepare_cached("SELECT EXISTS(SELECT 1 FROM events WHERE id=?)")
                .and_then(|mut statement| statement.query_row([&id], |r| r.get::<_, bool>(0)));
            match durable {
                Ok(true) => self.bump_commit_counter(fast),
                Ok(false) => {}
                Err(_) => {
                    self.caches.commit.pending.borrow_mut().insert(id, fast);
                }
            }
        }
    }
    fn bump_commit_counter(&self, fast: bool) {
        let counter = if fast {
            &self.caches.commit.fast
        } else {
            &self.caches.commit.slow
        };
        counter.set(counter.get().saturating_add(1));
    }
    /// Runs `f` over `branch`'s events in `id` order (exactly
    /// `Store::events()` filtered by branch), with `extra` appended last
    /// when given, and the journal hash state over those rows (without
    /// `extra`), through the event cache (module docs). `committed_view`:
    /// the connection reads committed state only (a read transaction of its
    /// own, as `prepare_slow` runs), so nothing it reads can roll back; in
    /// autocommit that holds anyway.
    pub(super) fn with_branch_events<T>(
        &self,
        branch: &str,
        extra: Option<&Event>,
        committed_view: bool,
        f: impl FnOnce(&[Event], &(Sha256, i64)) -> T,
    ) -> Result<T> {
        let committed = committed_view || self.db.is_autocommit();
        let (tip, total): (i64, i64) = self
            .db
            .prepare_cached("SELECT coalesce(max(seq),0),count(*) FROM journal_sequence")?
            .query_row([], |r| Ok((r.get(0)?, r.get(1)?)))?;
        let cached = {
            let mut caches = self.caches.commit.events.borrow_mut();
            caches
                .iter()
                .position(|cache| cache.branch == branch)
                .map(|at| caches.remove(at))
        };
        let refreshed = match cached {
            Some(cache) if cache.upto <= tip => {
                self.extend_event_cache(cache, tip, total, committed)?
            }
            _ => None,
        };
        let (mut cache, keep) = match refreshed {
            Some(cache) => (cache, true),
            None => self.load_event_cache(branch, tip, total, committed)?,
        };
        if committed {
            self.note_committed_tip(tip);
        }
        if let Some(extra) = extra {
            cache.events.push(extra.clone());
        }
        let result = f(&cache.events, &cache.journal);
        if extra.is_some() {
            cache.events.pop();
        }
        if keep {
            let mut caches = self.caches.commit.events.borrow_mut();
            caches.push(cache);
            if caches.len() > EVENT_CACHE_BRANCHES {
                caches.remove(0);
            }
        }
        Ok(result)
    }
    /// `branch`'s events loaded in full at `(tip, total)`, and whether the
    /// entry may be kept: always in a committed state; inside a write
    /// transaction only with every row above the last committed tip this
    /// connection read recorded as tentative (module docs). Without such a
    /// tip (nothing read in a committed state yet) it is not kept.
    fn load_event_cache(
        &self,
        branch: &str,
        tip: i64,
        total: i64,
        committed: bool,
    ) -> Result<(EventCache, bool)> {
        let loads = &self.caches.commit.event_loads;
        loads.set(loads.get().saturating_add(1));
        let mut events: Vec<Event> = vec![];
        let mut journal = (Sha256::new(), 0);
        let mut statement = self.db.prepare_cached(
            "SELECT s.seq,e.id,e.payload FROM events e JOIN journal_sequence s ON s.event=e.id WHERE e.branch=? ORDER BY s.seq",
        )?;
        let mut rows = statement.query([branch])?;
        while let Some(row) = rows.next()? {
            let seq: i64 = row.get(0)?;
            let id: String = row.get(1)?;
            let payload: String = row.get(2)?;
            journal_row(&mut journal.0, seq, &id, &payload);
            journal.1 = seq;
            events.push(serde_json::from_str(&payload)?);
        }
        drop(rows);
        // `Store::events()` order.
        events.sort_by(|a, b| a.id.cmp(&b.id));
        let mut cache = EventCache {
            branch: branch.to_owned(),
            events,
            upto: tip,
            total,
            tentative: None,
            journal,
        };
        if committed {
            return Ok((cache, true));
        }
        let Some(after) = self.caches.commit.committed_tip.get() else {
            return Ok((cache, false));
        };
        let after = after.min(tip);
        let rows = self.journal_range(after, tip)?;
        cache.tentative = Some(Tentative { after, rows });
        Ok((cache, true))
    }
    /// Every journal row in `(after, upto]`, in `seq` order.
    fn journal_range(&self, after: i64, upto: i64) -> Result<Vec<(i64, String)>> {
        Ok(self
            .db
            .prepare_cached(
                "SELECT seq,event FROM journal_sequence WHERE seq>? AND seq<=? ORDER BY seq",
            )?
            .query_map(params![after, upto], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?)
    }
    /// Record a journal tip read in a committed state (module docs).
    fn note_committed_tip(&self, tip: i64) {
        let known = &self.caches.commit.committed_tip;
        known.set(Some(known.get().map_or(tip, |known| known.max(tip))));
    }
    /// The cache brought up to `tip`: its tentative rows still exactly the
    /// journal's, the rows above `upto` loaded and merged, the journal count
    /// grown by exactly those rows. Outside a committed state every row read
    /// (of any branch) becomes tentative. `None` when any check fails (the
    /// caller reloads).
    fn extend_event_cache(
        &self,
        mut cache: EventCache,
        tip: i64,
        total: i64,
        committed: bool,
    ) -> Result<Option<EventCache>> {
        if !self.tentative_rows_hold(&cache)? {
            return Ok(None);
        }
        if committed {
            cache.tentative = None;
        }
        if tip == cache.upto {
            return Ok((total == cache.total).then_some(cache));
        }
        let mut statement = self.db.prepare_cached(
            "SELECT s.seq,e.id,e.branch=?1,CASE WHEN e.branch=?1 THEN e.payload END FROM journal_sequence s CROSS JOIN events e ON e.id=s.event WHERE s.seq>?2 ORDER BY s.seq",
        )?;
        let mut rows = statement.query(params![cache.branch, cache.upto])?;
        let mut count = 0i64;
        let mut added: Vec<Event> = vec![];
        let mut read: Vec<(i64, String)> = vec![];
        while let Some(row) = rows.next()? {
            count += 1;
            let seq: i64 = row.get(0)?;
            let id: String = row.get(1)?;
            if row.get::<_, bool>(2)? {
                let payload: String = row.get(3)?;
                added.push(serde_json::from_str(&payload)?);
                journal_row(&mut cache.journal.0, seq, &id, &payload);
                cache.journal.1 = seq;
            }
            // Any row read here may roll back, and its sequence number then
            // go to a row of this branch: all of them are tentative.
            if !committed {
                read.push((seq, id));
            }
        }
        drop(rows);
        if cache.total + count != total {
            return Ok(None);
        }
        if !committed {
            let after = cache.upto;
            cache
                .tentative
                .get_or_insert_with(|| Tentative {
                    after,
                    rows: vec![],
                })
                .rows
                .extend(read);
        }
        if !added.is_empty() {
            added.sort_by(|a, b| a.id.cmp(&b.id));
            let old = std::mem::take(&mut cache.events);
            let mut merged = Vec::with_capacity(old.len() + added.len());
            let mut old = old.into_iter().peekable();
            let mut added = added.into_iter().peekable();
            loop {
                let next = match (old.peek(), added.peek()) {
                    (Some(a), Some(b)) if a.id == b.id => return Ok(None),
                    (Some(a), Some(b)) if a.id < b.id => old.next(),
                    (Some(_), Some(_)) => added.next(),
                    (Some(_), None) => old.next(),
                    (None, Some(_)) => added.next(),
                    (None, None) => break,
                };
                merged.extend(next);
            }
            cache.events = merged;
        }
        cache.upto = tip;
        cache.total = total;
        Ok(Some(cache))
    }
    /// Whether the journal range of `cache`'s tentative rows still holds
    /// exactly those rows (same sequence numbers, same events, none more):
    /// one range scan. A rollback since they were read removed some, or
    /// removed them and gave their sequence numbers to other rows.
    fn tentative_rows_hold(&self, cache: &EventCache) -> Result<bool> {
        let Some(tentative) = &cache.tentative else {
            return Ok(true);
        };
        let mut statement = self.db.prepare_cached(
            "SELECT seq,event FROM journal_sequence WHERE seq>? AND seq<=? ORDER BY seq",
        )?;
        let mut rows = statement.query(params![tentative.after, cache.upto])?;
        let mut expected = tentative.rows.iter();
        while let Some(row) = rows.next()? {
            let Some((seq, id)) = expected.next() else {
                return Ok(false);
            };
            if row.get::<_, i64>(0)? != *seq || row.get_ref(1)?.as_str()? != id {
                return Ok(false);
            }
        }
        Ok(expected.next().is_none())
    }
    /// After a write transaction (in autocommit): an entry whose tentative
    /// rows are all still in the journal had them committed (they become
    /// known committed); any other entry is dropped. Then record the
    /// committed journal tip, so a full load inside the next transaction can
    /// be kept. Never fails; a failure only drops entries.
    pub(super) fn settle_event_cache(&self) {
        if !self.db.is_autocommit() {
            return;
        }
        let caches = std::mem::take(&mut *self.caches.commit.events.borrow_mut());
        let mut kept = Vec::with_capacity(caches.len());
        for mut cache in caches {
            if self.tentative_rows_hold(&cache).unwrap_or(false) {
                cache.tentative = None;
                kept.push(cache);
            }
        }
        *self.caches.commit.events.borrow_mut() = kept;
        let tip = self
            .db
            .prepare_cached("SELECT coalesce(max(seq),0) FROM journal_sequence")
            .and_then(|mut statement| statement.query_row([], |r| r.get::<_, i64>(0)));
        if let Ok(tip) = tip {
            self.note_committed_tip(tip);
        }
    }
    /// The branch's events through the event cache (tests: the cache must
    /// equal `events()` filtered by branch, in the same order).
    #[doc(hidden)]
    pub fn cached_branch_events(&self, branch: &str) -> Result<Vec<Event>> {
        self.with_branch_events(branch, None, false, |events, _| events.to_vec())
    }
    /// How many times this handle loaded a branch's events in full into
    /// the event cache (tests).
    #[doc(hidden)]
    pub fn event_cache_loads(&self) -> u64 {
        self.caches.commit.event_loads.get()
    }
    /// How many forced seals of prepared slow commits on this handle were
    /// written from their off-lock parts rather than by `snapshot.rs` under
    /// the lock (tests). A seal whose transaction rolled back counts too.
    #[doc(hidden)]
    pub fn prepared_seals(&self) -> u64 {
        self.caches.commit.prepared_seals.get()
    }
}

/// `snapshot::FORMAT`, which `seal_prepared` writes (a unit test pins the
/// payload to `snapshot::persist`'s, byte for byte).
const SEAL_FORMAT: u32 = 1;

/// The newest journal sequence among `branch`'s frontier events (0: none):
/// two index lookups per frontier event (`event_frontier`'s key, the
/// journal's `event` index), whatever the journal's size or the distance
/// from its tip to the branch's rows.
///
/// It is the branch's newest journal sequence: `write_commit` (the only
/// journal insert) adds every event to its branch's frontier and removes
/// only that branch's frontier entries the event names as parents, which
/// committed before it, so the newest row of a branch stays in its
/// frontier. (Only an open-time `rebuild_frontiers`, which drops any event
/// another branch's event names, can leave it lower.) As a staleness token
/// it is exact either way: every commit to the branch adds an event whose
/// sequence is the journal's largest, so the value grows with each commit
/// and changes with nothing else.
pub(super) fn frontier_seq(db: &Connection, branch: &str) -> Result<i64> {
    Ok(db
        .prepare_cached(
            "SELECT coalesce(max(s.seq),0) FROM event_frontier f JOIN journal_sequence s ON s.event=f.event WHERE f.branch=?",
        )?
        .query_row([branch], |r| r.get(0))?)
}

/// One journal row in the startup-snapshot journal hash.
fn journal_row(digest: &mut Sha256, seq: i64, id: &str, payload: &str) {
    digest.update(seq.to_le_bytes());
    for value in [id, payload] {
        digest.update((value.len() as u64).to_le_bytes());
        digest.update(value.as_bytes());
    }
}

/// The payload `snapshot::persist` seals for `branch` at `cursor`, with the
/// projection given as its JSON and the journal hash given: the same bytes
/// (`Snapshot`'s fields in order, compact JSON), the rest read from `db`.
fn seal_payload(
    db: &Connection,
    branch: &str,
    cursor: i64,
    journal_hash: &str,
    projection_json: &str,
) -> Result<String> {
    let mut conflicts = Vec::new();
    let mut statement =
        db.prepare_cached("SELECT payload,resolved_by FROM conflicts WHERE branch=? ORDER BY id")?;
    let mut rows = statement.query([branch])?;
    while let Some(row) = rows.next()? {
        let mut conflict: Conflict = serde_json::from_str(&row.get::<_, String>(0)?)?;
        conflict.resolved_by = row.get(1)?;
        conflicts.push(conflict);
    }
    let repo: String = db.query_row("SELECT value FROM config WHERE key='repo'", [], |r| {
        r.get(0)
    })?;
    let device: String = db.query_row("SELECT value FROM config WHERE key='device'", [], |r| {
        r.get(0)
    })?;
    let descriptor = db.query_row(
        "SELECT id,name,shared FROM branches WHERE id=?",
        [branch],
        |r| {
            Ok(Branch {
                id: r.get(0)?,
                name: r.get(1)?,
                shared: r.get(2)?,
            })
        },
    )?;
    let frontier: Vec<String> = db
        .prepare_cached("SELECT event FROM event_frontier WHERE branch=? ORDER BY event")?
        .query_map([branch], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    let mut payload = String::with_capacity(projection_json.len() + 1024);
    payload.push_str(&format!("{{\"format\":{SEAL_FORMAT},\"repo\":"));
    payload.push_str(&serde_json::to_string(&repo)?);
    payload.push_str(",\"device\":");
    payload.push_str(&serde_json::to_string(&device)?);
    payload.push_str(",\"branch\":");
    payload.push_str(&serde_json::to_string(&descriptor)?);
    payload.push_str(&format!(",\"cursor\":{cursor},\"journal_hash\":"));
    payload.push_str(&serde_json::to_string(journal_hash)?);
    payload.push_str(",\"projection\":");
    payload.push_str(projection_json);
    payload.push_str(",\"conflicts\":");
    payload.push_str(&serde_json::to_string(&conflicts)?);
    payload.push_str(",\"frontier\":");
    payload.push_str(&serde_json::to_string(&frontier)?);
    payload.push('}');
    Ok(payload)
}

/// The forced seal of a prepared slow commit (WP2.4): what
/// `snapshot::maybe_persist(.., true, ..)` writes, but with the projection's
/// JSON and the journal hash over the branch's earlier rows computed
/// off-lock; under the lock only the event's row is hashed, the small parts
/// read and the row written. `false` (nothing written; the caller seals
/// normally) unless the branch's journal is exactly that prefix plus this
/// event. Same fault points as `snapshot::persist`.
///
/// `prior` is [`frontier_seq`] of the branch just before this commit's
/// journal row. The branch's journal is that prefix plus the event when the
/// event's row is the journal's newest (so its branch's newest) and `prior`
/// equals `through`, the newest branch row the hash covers: a branch row
/// committed after the computation read the branch (above `through`) is a
/// frontier event when it commits and leaves the frontier only for a newer
/// branch row, so `prior` would be at least its sequence; and no row can
/// appear at or below `through` afterwards (sequences above a committed tip
/// only). A few index lookups, whatever the distance from the journal's tip
/// to the branch's previous row.
fn seal_prepared(
    tx: &Connection,
    branch: &str,
    event: &Event,
    payload: &str,
    projection_json: &str,
    (prefix, through): &(Sha256, i64),
    prior: i64,
) -> Result<bool> {
    let seq: i64 = tx.query_row(
        "SELECT seq FROM journal_sequence WHERE event=?",
        [&event.id],
        |r| r.get(0),
    )?;
    let tip: i64 = tx.query_row(
        "SELECT coalesce(max(seq),0) FROM journal_sequence",
        [],
        |r| r.get(0),
    )?;
    if tip != seq || prior != *through {
        return Ok(false);
    }
    let mut digest = prefix.clone();
    journal_row(&mut digest, seq, &event.id, payload);
    let journal_hash = hex::encode(digest.finalize());
    let sealed = seal_payload(tx, branch, seq, &journal_hash, projection_json)?;
    fault("before_startup_snapshot");
    tx.execute("INSERT INTO startup_snapshots VALUES(?,?,?,?) ON CONFLICT(branch) DO UPDATE SET cursor=excluded.cursor,payload=excluded.payload,checksum=excluded.checksum", params![branch, seq, sealed, hash(sealed.as_bytes())])?;
    fault("startup_snapshot_written");
    Ok(true)
}

/// Whether a fast commit's periodic seal is due (mirrors `snapshot::maybe_persist`
/// with [`SEAL_INTERVAL`], inside the commit's savepoint after its journal row).
fn seal_due(tx: &Connection, branch: &str) -> Result<bool> {
    let saved: Option<i64> = tx
        .query_row(
            "SELECT cursor FROM startup_snapshots WHERE branch=?",
            [branch],
            |r| r.get(0),
        )
        .optional()?;
    let tip: i64 = tx.query_row(
        "SELECT coalesce(max(seq),0) FROM journal_sequence",
        [],
        |r| r.get(0),
    )?;
    Ok(saved.is_none_or(|saved| tip - saved >= SEAL_INTERVAL))
}

/// Patch `view` with a fast commit's `delta` (the changed entities' entries
/// and head registers) and their slots. On an error the view is partly
/// patched and must be dropped.
fn patch_view(view: &mut ViewCache, event: &Event, delta: Projection) -> Result<()> {
    let entities: BTreeSet<_> = event.changes.iter().map(|c| &c.entity).collect();
    // A replacement moves one entry into another's old slot. Remove all old
    // slots first, independent of entity ID ordering.
    for entity in &entities {
        if let Some(old) = view.projection.entries.get(*entity).filter(|e| e.alive) {
            view.slots
                .remove(&(old.parent.clone(), name_key(&old.name)?));
        }
    }
    for entity in entities {
        let Some(entry) = delta.entries.get(entity) else {
            bail!("the fast commit's delta lacks the entry of {entity}");
        };
        if entry.alive {
            view.slots.insert(
                (entry.parent.clone(), name_key(&entry.name)?),
                entity.clone(),
            );
        }
    }
    view.projection.entries.extend(delta.entries);
    view.projection.heads.extend(delta.heads);
    Ok(())
}

impl Store {
    /// Whether `branch` has a conflict row that no review resolved.
    pub fn branch_has_unresolved_conflicts(&self, branch: &str) -> Result<bool> {
        Ok(self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM conflicts WHERE branch=? AND resolved_by IS NULL)",
            [branch],
            |r| r.get(0),
        )?)
    }
    /// The conflict gate of the creation and namespace successors. With
    /// remote fast paths, only unresolved conflicts of the event's own branch
    /// disable them; with `TKFS_REMOTE_FASTPATH=0`, any conflict row of that
    /// branch does (the previous gate, also the oracle fallback of §5.1).
    fn successor_conflict_gate(&self, branch: &str) -> Result<bool> {
        if remote_fast_paths_enabled() {
            return self.branch_has_unresolved_conflicts(branch);
        }
        Ok(self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM conflicts WHERE branch=?)",
            [branch],
            |r| r.get(0),
        )?)
    }
    /// Whether the successors may handle an event of `device`.
    fn successor_device_allowed(&self, device: &str) -> bool {
        device == self.device || remote_fast_paths_enabled()
    }
    /// The sorted causal frontier of `branch`.
    fn frontier(&self, branch: &str) -> Result<Vec<String>> {
        Ok(self
            .db
            .prepare_cached("SELECT event FROM event_frontier WHERE branch=? ORDER BY event")?
            .query_map([branch], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?)
    }
    pub(super) fn content_successor(&self, event: &Event) -> Result<Option<Projection>> {
        if !event.reviewed.is_empty()
            || !event
                .changes
                .iter()
                .any(|c| ["content", "basic"].contains(&c.field.as_str()))
            || event.changes.iter().any(|c| {
                !["content", "alive", "basic"].contains(&c.field.as_str())
                    || (c.field == "alive" && c.value != true)
            })
        {
            return Ok(None);
        }
        let Ok(view) = self.view(&event.branch.id) else {
            return Ok(None);
        };
        let current = &view.projection;
        if !current.pending.is_empty() {
            return Ok(None);
        }
        for change in &event.changes {
            let Some(entry) = current.entries.get(&change.entity) else {
                return Ok(None);
            };
            let Some(heads) = current.heads.get(&register(&change.entity, &change.field)) else {
                if change.field == "basic" && change.parents.is_empty() && entry.alive {
                    continue;
                }
                return Ok(None);
            };
            if (entry.kind != "file" && change.field != "basic")
                || !entry.alive
                || heads.len() != 1
                || change.parents != vec![heads[0].revision.clone()]
            {
                return Ok(None);
            }
        }
        // Frontier dependencies are checked against accepted journal rows only.
        // A missing or cross-branch prerequisite falls back to full validation.
        for dependency in &event.dependencies {
            let branch: Option<String> = self
                .db
                .query_row("SELECT branch FROM events WHERE id=?", [dependency], |r| {
                    r.get(0)
                })
                .optional()?;
            if branch.as_deref() != Some(&event.branch.id) {
                return Ok(None);
            }
        }
        let mut p = Projection::default();
        for entity in event
            .changes
            .iter()
            .map(|c| &c.entity)
            .collect::<BTreeSet<_>>()
        {
            p.entries
                .insert(entity.clone(), current.entries[entity].clone());
            for field in ["kind", "location", "alive", "content", "basic"] {
                let key = register(entity, field);
                if let Some(heads) = current.heads.get(&key) {
                    p.heads.insert(key, heads.clone());
                }
            }
        }
        for change in &event.changes {
            p.heads.insert(
                register(&change.entity, &change.field),
                vec![Version {
                    revision: event.id.clone(),
                    device: event.device.clone(),
                    created_ms: event.created_ms,
                    parents: change.parents.clone(),
                    value: change.value.clone(),
                }],
            );
            let entry = p.entries.get_mut(&change.entity).unwrap();
            entry
                .revisions
                .insert(change.field.clone(), event.id.clone());
            if change.field == "content" {
                entry.content = change.value.as_str().map(str::to_owned);
            } else if change.field == "basic" {
                entry.basic = Some(serde_json::from_value(change.value.clone())?);
            }
        }
        for entity in event
            .changes
            .iter()
            .map(|c| &c.entity)
            .collect::<BTreeSet<_>>()
        {
            let modified = modified_ms(&p.heads, entity);
            p.entries.get_mut(entity).unwrap().modified_ms = modified;
        }
        // Each changed register has one head and this event parents it. All old
        // versions are ancestors, so no new historical pair/namespace conflict
        // is possible. Existing durable records remain untouched.
        Ok(Some(p))
    }
    /// A fresh entry ordered after the entire accepted frontier cannot
    /// introduce a concurrent namespace cut. All other placements use replay.
    pub(super) fn creation_successor(&self, event: &Event) -> Result<Option<Projection>> {
        if !self.successor_device_allowed(&event.device)
            || !event.reviewed.is_empty()
            || event.changes.is_empty()
        {
            return Ok(None);
        }
        let entity = &event.changes[0].entity;
        if event
            .changes
            .iter()
            .any(|c| &c.entity != entity || !c.parents.is_empty())
        {
            return Ok(None);
        }
        let fields: BTreeMap<_, _> = event
            .changes
            .iter()
            .map(|c| (c.field.as_str(), &c.value))
            .collect();
        if !fields.contains_key("kind")
            || !fields.contains_key("location")
            || fields.get("alive") != Some(&&json!(true))
        {
            return Ok(None);
        }
        let kind = fields["kind"].as_str().context("INVALID_KIND")?;
        if (kind == "file" && !fields.contains_key("content"))
            || (kind == "directory" && fields.contains_key("content"))
        {
            return Ok(None);
        }
        if event.dependencies != self.frontier(&event.branch.id)? {
            return Ok(None);
        }
        let Ok(view) = self.view(&event.branch.id) else {
            return Ok(None);
        };
        let current = &view.projection;
        if current.entries.contains_key(entity)
            || !current.pending.is_empty()
            || !current.conflicts.is_empty()
            || self.successor_conflict_gate(&event.branch.id)?
        {
            return Ok(None);
        }
        let parent = fields["location"]["parent"]
            .as_str()
            .context("INVALID_LOCATION")?;
        let name = fields["location"]["name"]
            .as_str()
            .context("INVALID_LOCATION")?;
        if parent != ROOT
            && !current
                .entries
                .get(parent)
                .is_some_and(|e| e.alive && e.kind == "directory")
        {
            return Ok(None);
        }
        let key = name_key(name)?;
        if view.slots.contains_key(&(parent.into(), key)) {
            return Ok(None);
        }
        let mut p = Projection::default();
        let mut entry = Entry {
            id: entity.clone(),
            kind: kind.into(),
            parent: parent.into(),
            name: name.into(),
            alive: true,
            created_ms: event.created_ms,
            modified_ms: event.created_ms,
            ..Default::default()
        };
        for change in &event.changes {
            entry
                .revisions
                .insert(change.field.clone(), event.id.clone());
            if change.field == "content" {
                entry.content = change.value.as_str().map(str::to_owned);
            }
            if change.field == "basic" {
                entry.basic = Some(serde_json::from_value(change.value.clone())?);
            }
            p.heads.insert(
                register(entity, &change.field),
                vec![Version {
                    revision: event.id.clone(),
                    device: event.device.clone(),
                    created_ms: event.created_ms,
                    parents: vec![],
                    value: change.value.clone(),
                }],
            );
        }
        p.entries.insert(entity.clone(), entry);
        Ok(Some(p))
    }
    /// A placement/deletion after the complete frontier adds only the new cut.
    /// If it preserves a valid namespace, no historical conflict can be added.
    /// Directory moves preserve descendant entity IDs; empty directory deletes
    /// are equally safe. Replaying every historical cut here can hold the mounted
    /// filesystem's engine and WinFsp namespace locks for minutes on IDE cleanup.
    pub(super) fn namespace_successor(&self, event: &Event) -> Result<Option<Projection>> {
        if !self.successor_device_allowed(&event.device)
            || !event.reviewed.is_empty()
            || !(1..=2).contains(&event.changes.len())
        {
            return Ok(None);
        }
        let replacement = if event.changes.len() == 2 {
            let location = event.changes.iter().find(|c| c.field == "location");
            let removed = event
                .changes
                .iter()
                .find(|c| c.field == "alive" && c.value == false);
            match (location, removed) {
                (Some(a), Some(b)) if a.entity != b.entity => Some((&a.entity, &b.entity)),
                _ => return Ok(None),
            }
        } else {
            None
        };
        if event.changes.iter().any(|change| {
            change.field != "location" && !(change.field == "alive" && change.value == false)
        }) {
            return Ok(None);
        }
        if event.dependencies != self.frontier(&event.branch.id)? {
            return Ok(None);
        }
        let Ok(view) = self.view(&event.branch.id) else {
            return Ok(None);
        };
        let current = &view.projection;
        if !current.pending.is_empty()
            || !current.conflicts.is_empty()
            || self.successor_conflict_gate(&event.branch.id)?
        {
            return Ok(None);
        }
        let mut delta = Projection::default();
        for change in &event.changes {
            let Some(entry) = current.entries.get(&change.entity).filter(|e| e.alive) else {
                return Ok(None);
            };
            let key = register(&change.entity, &change.field);
            let Some(heads) = current.heads.get(&key) else {
                return Ok(None);
            };
            if heads.len() != 1 || change.parents != vec![heads[0].revision.clone()] {
                return Ok(None);
            }
            let mut entry = entry.clone();
            if change.field == "location" {
                let parent = change.value["parent"]
                    .as_str()
                    .context("INVALID_LOCATION")?;
                let name = change.value["name"].as_str().context("INVALID_LOCATION")?;
                if parent != ROOT
                    && !current
                        .entries
                        .get(parent)
                        .is_some_and(|e| e.alive && e.kind == "directory")
                {
                    return Ok(None);
                }
                let occupying = view.slots.get(&(parent.into(), name_key(name)?));
                if let Some((source, removed)) = replacement {
                    if source != &entry.id
                        || occupying != Some(removed)
                        || entry.kind != "file"
                        || current.entries[removed].kind != "file"
                    {
                        return Ok(None);
                    }
                } else if occupying.is_some_and(|e| e != &entry.id) {
                    return Ok(None);
                }
                // Validate here as well as in rename(): snapshot replay and
                // direct commits also use this path. Cycles need the reducer.
                if entry.kind == "directory" {
                    let mut ancestor = parent;
                    let mut seen = BTreeSet::new();
                    while ancestor != ROOT {
                        if ancestor == entry.id || !seen.insert(ancestor) {
                            return Ok(None);
                        }
                        let Some(dir) = current
                            .entries
                            .get(ancestor)
                            .filter(|e| e.alive && e.kind == "directory")
                        else {
                            return Ok(None);
                        };
                        ancestor = &dir.parent;
                    }
                }
                entry.parent = parent.into();
                entry.name = name.into();
            } else {
                // Deleting a parent with live children can suppress their
                // namespace and create conflicts, even on a sequential event.
                // The slot index holds exactly the live entries, keyed by parent.
                if view
                    .slots
                    .range((entry.id.clone(), String::new())..)
                    .next()
                    .is_some_and(|((parent, _), _)| parent == &entry.id)
                {
                    return Ok(None);
                }
                entry.alive = false;
            }
            entry
                .revisions
                .insert(change.field.clone(), event.id.clone());
            for field in ["kind", "location", "alive", "content", "basic"] {
                let key = register(&entry.id, field);
                if let Some(heads) = current.heads.get(&key) {
                    delta.heads.insert(key, heads.clone());
                }
            }
            delta.heads.insert(
                key,
                vec![Version {
                    revision: event.id.clone(),
                    device: event.device.clone(),
                    created_ms: event.created_ms,
                    parents: change.parents.clone(),
                    value: change.value.clone(),
                }],
            );
            // Full replay derives `modified_ms` from the current winners, so
            // a remote event with an older clock can lower it (exactly as
            // `project` does); `max(old, created)` would diverge.
            entry.modified_ms = modified_ms(&delta.heads, &entry.id);
            delta.entries.insert(entry.id.clone(), entry);
        }
        Ok(Some(delta))
    }
}

/// `project`'s `modified_ms` of `entity`: the latest `created_ms` among the
/// winners (the last head) of its non-`basic` registers.
fn modified_ms(heads: &BTreeMap<String, Vec<Version>>, entity: &str) -> u64 {
    ["kind", "location", "alive", "content"]
        .iter()
        .filter_map(|field| {
            heads
                .get(&register(entity, field))
                .and_then(|heads| heads.last())
        })
        .map(|v| v.created_ms)
        .max()
        .unwrap_or(0)
}

/// The error of the `core.commit.busy` fault point: a genuine `SQLITE_BUSY`,
/// so classification sees exactly what a contended writer would produce.
fn injected_busy() -> rusqlite::Error {
    rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
        Some(format!("database is locked (fault point {BUSY_FAULT})")),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `seal_payload` (with `journal_prefix`) writes exactly the payload of
    /// `snapshot::persist`: empty and populated branches, with register
    /// conflicts, a resolved one and a deleted entry.
    #[test]
    fn seal_payload_is_snapshot_persist_byte_for_byte() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Store::open(dir.path(), None).unwrap();
        let check = |s: &Store| {
            let branch = s.active.clone();
            let (prefix, through) = s
                .with_branch_events(&branch, None, true, |_, journal| journal.clone())
                .unwrap();
            let projection = snapshot::load_projection(&s.db, &branch).unwrap();
            let json = serde_json::to_string(&projection).unwrap();
            snapshot::persist(&s.db, &branch).unwrap();
            let (cursor, expected): (i64, String) =
                s.db.query_row(
                    "SELECT cursor,payload FROM startup_snapshots WHERE branch=?",
                    [&branch],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();
            assert_eq!(cursor, through);
            let journal_hash = hex::encode(prefix.finalize());
            let mine = seal_payload(&s.db, &branch, cursor, &journal_hash, &json).unwrap();
            assert_eq!(mine, expected);
        };
        check(&s);
        let file = s.create("f.txt", "file").unwrap();
        s.create("dir", "directory").unwrap();
        check(&s);
        let entry = s.entry(&file.id).unwrap();
        let (content, alive) = (
            entry.revisions.get("content").cloned(),
            entry.revisions.get("alive").cloned(),
        );
        s.write_with_base(&file.id, content.clone(), alive.clone(), b"one")
            .unwrap();
        s.write_with_base(&file.id, content, alive, b"two").unwrap();
        let open: Vec<String> = s.conflicts().unwrap().into_iter().map(|c| c.id).collect();
        assert!(!open.is_empty(), "concurrent saves conflict");
        check(&s);
        let selected = s.conflicts().unwrap()[0].alternatives[0].revision.clone();
        s.resolve(&open[..1], &selected).unwrap();
        let other = s.create("gone.txt", "file").unwrap();
        s.delete(&other.id).unwrap();
        check(&s);
    }
}
