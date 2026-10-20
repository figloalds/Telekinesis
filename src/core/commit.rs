//! The commit path: validation, the incremental successors (fast paths) and
//! the full-replay slow path with its forced snapshot seal.
//!
//! WP1.6 (REVAMP_PLAN §5.1): the creation and namespace successors also serve
//! remote events (kill switch `TKFS_REMOTE_FASTPATH=0`), and their conflict
//! gate reads only the unresolved conflicts of the event's own branch.
//!
//! **The conflict gate is scoped to the event's cone (WP4.INT re-review,
//! RC1).** One unresolved conflict anywhere on the branch used to send every
//! creation, rename and delete to the slow path, local filesystem operations
//! included (inline under the engine lock, O(history) each: about one
//! namespace operation per second on a 30K-event folder). With fast paths on,
//! the successors now refuse only when a conflict of the current projection
//! touches what the event changes ([`conflicts_touch`]): the conflict names
//! an entity the event changes or a parent directory it leaves or enters, or
//! one of its alternatives is located inside a changed entity or in a slot
//! the event leaves or takes. Why
//! that is exact: full replay of `N = O ∪ {E}` stores its projection's
//! records and those of every antichain cut; with `dependencies == frontier`
//! the old antichains keep their cuts (their records are already stored) and
//! `{E}`'s cut is the new projection, so exactness needs only `project(N)` =
//! the delta applied to `project(O)`, conflicts included (then full mode's
//! DELETE of rows no longer derived removes nothing). The successor checks
//! give that for the entries (a free slot under a live parent, which, being
//! live, has a valid chain, so every register-alive member of a slot under
//! it is live or a slot loser beside a live winner the view's slots hold; a
//! single head parented by the event; no live child of a deleted entry; no
//! cycle); a conflict that names none of the touched entities, parents and
//! slots is derived from registers and slots the event leaves alone (its
//! pairs, its suppressed entries and its invalid-parent records keep their
//! inputs: a moved directory is live, so the validity of everything below
//! it is decided below it), so it is the same record before and after. A
//! stored conflict row that is no longer in the projection is a historical
//! record, re-derived from an unchanged cut. The oracle (`derive_oracle`,
//! every commit checked against a rebuild) is the executable check; with
//! `TKFS_REMOTE_FASTPATH=0` the previous gate (no conflict in the projection
//! and no conflict row on the branch) applies.
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
//! entity joins one existing cone). The content successor counts when its
//! event saves an entity that has a tombstone (CCR-WP4.4-5, WP4.INT): it
//! needs one head per register it changes, not the whole frontier, so its
//! event may be concurrent with other events of the branch, and every save
//! asserts `alive = true`, which is a TRUE-presence witness (a relevant
//! event) of an entity with a tombstone: such a commit can tip a cone over
//! `MAX_NAMESPACE_CUTS` (found by the stress fuzz, `derive_oracle`). Without
//! a tombstone a TRUE version is never relevant, so ordinary saves never
//! count (`derive::content_successor_may_add_cuts`).
//!
//! # Off-lock slow path (WP2.4, REVAMP_PLAN §4.4, §14 WP2.4)
//!
//! A slow commit is split into a computation and a write. The computation
//! (`compute_slow`) reads the branch's events from the event cache, projects
//! them, derives the conflict records (`derive::derive_conflicts`, pure) and
//! works out how the projection is stored (the `current_parts` delta of
//! CCR-WP1.INT-4, with the header row of CCR-WP2.INT-5 when only the
//! conflict or pending list changed besides, or the precomputed
//! `current_state` JSON of a full rewrite). It needs no lock and no writer,
//! so `inbox::prepare_slow` runs it on a read store. The write
//! (`write_commit`) is the same for the inline and the prepared path: the
//! journal rows, the stored projection, the records
//! (`derive::write_projection`), the frontier and the forced snapshot seal
//! of the projection the commit holds, in one savepoint.
//!
//! # Seals (WP4.4)
//!
//! Every seal is written by `snapshot::seal` (one writer of the payload; the
//! WP2.4 mirror of it here is gone, CCR-WP2.4-2). A fast commit seals when
//! the branch's interval is due (`snapshot::seal_due`), a slow commit always
//! (REVAMP_PLAN P4), passing the entities its projection changed, so a
//! large projection's seal writes only the rows that changed since the
//! previous seal (see `snapshot`'s module docs). The journal hash continues
//! from the previous seal's stored state, so no seal hashes the history.
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

/// Objects this process verified (WP5.1 task 5): `(state root, hash)` →
/// the file's size and mtime when it was verified, and when. Process-wide,
/// bounded; `check_commit` skips the re-hash of an object recorded here
/// within [`FRESH_MS`](verified::FRESH_MS) whose file still has that size
/// and mtime. A verification is recorded only when it is not racy: observed
/// at least [`RACY_NS`](verified::RACY_NS) after the file's mtime, so that a
/// same-size rewrite within one timestamp tick cannot hide (the rule of the
/// availability rows' `trusted`).
pub mod verified {
    use std::{
        collections::HashMap,
        path::{Path, PathBuf},
        sync::{
            Mutex, OnceLock, PoisonError,
            atomic::{AtomicU64, Ordering},
        },
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };

    /// How long a verification counts as "moments ago".
    pub const FRESH_MS: u64 = 300_000;
    const FRESH: Duration = Duration::from_millis(FRESH_MS);
    /// A verification observed within this long of the file's mtime is racy
    /// and not recorded: the availability rows' `objects::RACY_NS` (larger
    /// than any common mtime granularity; FAT: 2 s).
    pub const RACY_NS: i64 = 3_000_000_000;
    /// Entries kept (the oldest are dropped past it).
    const CAPACITY: usize = 4096;

    type Entries = HashMap<(PathBuf, String), (u64, i64, Instant)>;

    fn entries() -> &'static Mutex<Entries> {
        static ENTRIES: OnceLock<Mutex<Entries>> = OnceLock::new();
        ENTRIES.get_or_init(|| Mutex::new(HashMap::new()))
    }

    static SKIPPED: AtomicU64 = AtomicU64::new(0);

    /// The wall clock, in ns since the epoch (`observed_ns` of [`note`]).
    pub fn now_ns() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
    }

    /// When this process started trusting availability rows, in ms since the
    /// epoch: fixed by the first call, which `Store::open` makes (through
    /// `snapshot::initialize`) before the process installs anything.
    pub fn epoch_ms() -> u64 {
        static EPOCH: OnceLock<u64> = OnceLock::new();
        *EPOCH.get_or_init(super::time)
    }

    /// Whether an observation at `observed_ns` of a file with mtime
    /// `mtime_ns` is not racy.
    pub fn settled(mtime_ns: i64, observed_ns: i64) -> bool {
        i128::from(observed_ns) - i128::from(mtime_ns) >= i128::from(RACY_NS)
    }

    /// Whether an availability row verified at `verified_ms` stands for a
    /// verification by this process moments ago (at `now_ms`): not before
    /// [`epoch_ms`] (a previous process's row, until CCR-WP5.1-1 lets the
    /// installers record their verifications here) and within
    /// [`FRESH_MS`].
    pub fn fresh_row(verified_ms: i64, now_ms: u64) -> bool {
        u64::try_from(verified_ms)
            .is_ok_and(|at| at >= epoch_ms() && now_ms.saturating_sub(at) <= FRESH_MS)
    }

    /// Record that this process verified `hash` under `root` while its file
    /// had `len` bytes and mtime `mtime_ns`, as `stat` read them no earlier
    /// than `observed_ns` (wall clock, ns since the epoch). Nothing is
    /// recorded when that observation is racy ([`settled`]).
    pub fn note(root: &Path, hash: &str, len: u64, mtime_ns: i64, observed_ns: i64) {
        if !settled(mtime_ns, observed_ns) {
            return;
        }
        let mut entries = entries().lock().unwrap_or_else(PoisonError::into_inner);
        if entries.len() >= CAPACITY {
            entries.retain(|_, (_, _, at)| at.elapsed() < FRESH);
            if entries.len() >= CAPACITY {
                entries.clear();
            }
        }
        entries.insert(
            (root.to_owned(), hash.to_owned()),
            (len, mtime_ns, Instant::now()),
        );
    }

    /// Drops what is recorded of `hash` under `root`: a later verification
    /// found its bytes corrupt or missing, or they were quarantined
    /// (WP5.INT, the WP5.1 review's minor; `objects::forget_served` calls
    /// it, so the set never outlives what the index says).
    pub fn forget(root: &Path, hash: &str) {
        entries()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&(root.to_owned(), hash.to_owned()));
    }

    /// Whether `hash` under `root` was verified within [`FRESH_MS`] with the
    /// file at `len` bytes and mtime `mtime_ns`.
    pub fn recent(root: &Path, hash: &str, len: u64, mtime_ns: i64) -> bool {
        entries()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(root.to_owned(), hash.to_owned()))
            .is_some_and(|&(l, m, at)| l == len && m == mtime_ns && at.elapsed() < FRESH)
    }

    pub(super) fn count_skip() {
        SKIPPED.fetch_add(1, Ordering::Relaxed);
    }

    /// Re-verifications `check_commit` skipped in this process (tests).
    #[doc(hidden)]
    pub fn skipped() -> u64 {
        SKIPPED.load(Ordering::Relaxed)
    }
}

/// A view's slots: (parent, name key) to its visible entry.
type ViewSlots = BTreeMap<(String, String), String>;

/// The view slots (`(parent, name key)` → entity) of the alive entries of
/// `projection`; `None` if a name cannot be keyed, which a committed
/// projection never has.
fn view_slots(projection: &Projection) -> Option<ViewSlots> {
    projection
        .entries
        .values()
        .filter(|e| e.alive)
        .map(|e| Ok(((e.parent.clone(), name_key(&e.name)?), e.id.clone())))
        .collect::<Result<_>>()
        .ok()
}

/// `view_slots` of a projection `derive::project_append` made from a view
/// whose slots are `slots` (WP5.1): only the entities that changed can move,
/// and each slot has at most one visible entry. `placements`: the changed
/// entities with their visible placement in the base (`Appended`).
fn patched_slots(
    mut slots: BTreeMap<(String, String), String>,
    placements: &[(String, Option<(String, String)>)],
    projection: &Projection,
) -> Option<BTreeMap<(String, String), String>> {
    for (id, before) in placements {
        if let Some((parent, name)) = before {
            let key = (parent.clone(), name_key(name).ok()?);
            if slots.get(&key) == Some(id) {
                slots.remove(&key);
            }
        }
    }
    for (id, _) in placements {
        if let Some(e) = projection.entries.get(id)
            && e.alive
        {
            slots.insert((e.parent.clone(), name_key(&e.name).ok()?), id.clone());
        }
    }
    Some(slots)
}

/// The view of `branch` after a slow commit stored `projection` (`None` if a
/// name cannot be keyed, which a committed projection never has). `slots`
/// are its view slots when already computed (off-lock).
pub(super) fn slow_commit_view(
    branch: &str,
    projection: Projection,
    slots: Option<BTreeMap<(String, String), String>>,
    changes: u64,
    data_version: i64,
    transactional: bool,
    projection_version: Option<i64>,
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
        projection_version,
        projection,
        slots,
    })
}

use super::snapshot::PART_FIELDS;
/// The snapshot seal API (WP4.4), reachable through this public module
/// (`core::snapshot` is private to `core`).
pub use super::snapshot::{
    INLINE_LIMIT, SealMode, SealStats, SnapshotCheck, inline_limit, set_thread_inline_limit,
};

/// The largest projection (in entries) a slow commit folds back into
/// `current_state` with a full rewrite (`Store::projection_delta`): such a
/// rewrite costs about 1.4 MB under the lock at this size; a larger
/// projection keeps its parts instead (WP4.4).
pub const FOLD_MAX_ENTRIES: usize = 1024;

/// Upserts the `current_parts` row of `entity` (its entry and its head
/// registers) from projection `p`, which must hold the entry.
fn write_part(tx: &Connection, branch: &str, entity: &str, p: &Projection) -> Result<()> {
    let (entry, heads) = snapshot::part_texts(p, entity)?;
    tx.prepare_cached("INSERT INTO current_parts VALUES(?,?,?,?) ON CONFLICT(branch,entity) DO UPDATE SET entry=excluded.entry,heads=excluded.heads")?
        .execute(params![branch, entity, entry, heads])?;
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
    /// parts (the projection's JSON; tests).
    prepared_seals: Cell<u64>,
    /// Every seal written through this handle (WP4.4; `Store::seal_stats`).
    seal_stats: Cell<SealStats>,
    /// Branches this handle's open resumed from their startup snapshot
    /// (the others were reconstructed; tests).
    resumed: Cell<u64>,
    /// The `temp.tkfs_commit_epoch` value this connection's own commits left
    /// (0: none yet; module docs, "Rolled-back commits and the view").
    epoch: Cell<i64>,
    /// Views dropped because a commit was rolled back around the store
    /// (tests).
    rolled_back_views: Cell<u64>,
    /// The anchors this handle's seals wrote (WP5.1, `snapshot::AnchorCache`).
    pub(super) anchors: snapshot::AnchorCache,
    /// Full loads of the view from the stored projection (tests; WP3.INT).
    view_loads: Cell<u64>,
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
    /// WP5.1: the derivation index of `events` (`derive::BranchIndex`),
    /// built by the first slow commit that needs it and extended with the
    /// events that join the cache; dropped with the entry.
    index: derive::IndexCell,
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
    /// Journal generation from which the projection will be prepared.
    base_seq: i64,
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
    /// How the stored projection becomes `projection`.
    stored: Stored,
    /// The projection's JSON, serialized off-lock: the `current_state`
    /// payload of a full rewrite and an inline seal's `projection`. Only
    /// computed when one of them needs it (a rows seal of a delta does not).
    json: Option<String>,
    /// The view slots of `projection`, computed off-lock.
    slots: Option<BTreeMap<(String, String), String>>,
}

/// How a slow commit stores its projection (`Store::projection_delta`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Stored {
    /// CCR-WP1.INT-4: the `current_parts` rows of `changed` make the stored
    /// projection read the new one; `header`: the conflict or pending list
    /// changed too, stored as the `current_meta` row (CCR-WP2.INT-5).
    Delta {
        changed: BTreeSet<String>,
        header: bool,
    },
    /// The full `current_state` rewrite that folds the parts back; the
    /// projection differs from the stored one in `changed` (and possibly
    /// its lists).
    Fold { changed: BTreeSet<String> },
    /// The full rewrite, with what changed not known.
    Rewrite,
}

impl Stored {
    fn is_delta(&self) -> bool {
        matches!(self, Stored::Delta { .. })
    }
}

impl std::fmt::Debug for SlowPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlowPlan")
            .field("entries", &self.projection.entries.len())
            .field("conflicts", &self.derived.conflicts().len())
            .field(
                "stored",
                &match &self.stored {
                    Stored::Delta { changed, header } => {
                        format!("delta {} {header}", changed.len())
                    }
                    Stored::Fold { changed } => format!("fold {}", changed.len()),
                    Stored::Rewrite => "rewrite".into(),
                },
            )
            .field("json_bytes", &self.json.as_ref().map(String::len))
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
        self.check_content_capability(event)?;
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
                self.verify_new_object(object)?;
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
                "content" => {
                    if !change.value.is_string() {
                        let reference: crate::model::content::FileContentRef =
                            serde_json::from_value(change.value.clone())
                                .context("INVALID_CONTENT")?;
                        reference.validate()?;
                    }
                }
                "basic" => serde_json::from_value::<BasicInfo>(change.value.clone())?.validate()?,
                _ => {}
            }
        }
        if let Ok(b) = self.branch(&event.branch.id) {
            ensure!(b == event.branch, "BRANCH_DESCRIPTOR_CHANGED");
        }
        Ok(Check::Ready(Checked {
            payload,
            objects,
            base_seq: frontier_seq(&self.db, &event.branch.id)?,
        }))
    }
    /// `check_commit`'s verification of newly authored bytes (WP5.1 task 5,
    /// CCR-WP4.INT-5's re-hash): skipped when this process verified the
    /// object moments ago and the file still has the size and mtime it had
    /// then ([`verified`]), else a full `verify_object`, recorded on success
    /// unless racy. The bytes were hashed by this process (or installed from
    /// bytes it hashed: the trusted availability marker `put_object` /
    /// `put_stream` leave, written since this process started and read within
    /// [`verified::FRESH_MS`]) and `stat` shows them unchanged, with an mtime
    /// at least [`verified::RACY_NS`] older than the verification: the rule
    /// startup trusts markers by (`VerifyMode::Changed`), narrowed to this
    /// process and to moments ago, so invariant 1 (a verified, durable
    /// install) holds.
    fn verify_new_object(&self, object: &str) -> Result<()> {
        let path = self.root.join("objects").join(object);
        // No later than the `stat` below (`verified::note`).
        let observed = verified::now_ns();
        let before = fs::metadata(&path).ok();
        if let Some(metadata) = &before {
            let (len, mtime) = (metadata.len(), objects::mtime_ns(metadata));
            // The index first (WP5.INT, the WP5.1 review's minor): a row
            // that a later verification left `CORRUPT` or `MISSING` wins over
            // the in-memory set, as it does over the served cache.
            let row = self.availability_row(object)?;
            if row.is_none_or(|row| row.state == objects::AVAILABLE)
                && verified::recent(&self.root, object, len, mtime)
            {
                verified::count_skip();
                return Ok(());
            }
            // An install marker of this process (`put_object`, `put_stream`,
            // and the engine's saves record their verifications in the set
            // too, CCR-WP5.1-1); `trusted` is not racy.
            let row = row.filter(|row| {
                row.trusted(len, mtime) && verified::fresh_row(row.verified_ms, time())
            });
            if let Some(row) = row {
                let verified_ns = row.verified_ms.saturating_mul(1_000_000);
                verified::note(&self.root, object, len, mtime, verified_ns);
                verified::count_skip();
                return Ok(());
            }
        }
        self.verify_object(object)?;
        if let Some(metadata) = before {
            verified::note(
                &self.root,
                object,
                metadata.len(),
                objects::mtime_ns(&metadata),
                observed,
            );
        }
        Ok(())
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
        if fast.is_some() {
            self.check_content_successor(event, committed_view)?;
        }
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
        self.with_branch_index(branch, Some(event), committed_view, |events, index| {
            let new = events.len() - 1;
            if !derive::successor_may_add_cuts(events, new) {
                return Ok(());
            }
            let counted = &self.caches.commit.successor_checks;
            counted.set(counted.get().saturating_add(1));
            if let Some(cell) = index {
                derive::ensure_index(cell, &events[..new]);
            }
            derive::check_successor_cuts(branch, events, new, index)
        })?
    }
    /// CCR-WP4.4-5: refuse a content successor commit exactly when full
    /// replay would refuse it with `NAMESPACE_HISTORY_LIMIT` (module docs):
    /// only an event that saves an entity with a tombstone and is concurrent
    /// with another event of the branch can change a count, and only those
    /// are counted. A save that follows the whole frontier (every local save,
    /// a linear remote history) is in no antichain of two or more events and
    /// can only displace an older TRUE witness from the relevant set, which
    /// never adds a cut; it is not even scanned.
    fn check_content_successor(&self, event: &Event, committed_view: bool) -> Result<()> {
        if !event.changes.iter().any(|c| c.field == "alive") {
            return Ok(());
        }
        let mut dependencies = event.dependencies.clone();
        dependencies.sort();
        if dependencies == self.frontier(&event.branch.id)? {
            return Ok(());
        }
        let branch = event.branch.id.as_str();
        self.with_branch_index(branch, Some(event), committed_view, |events, index| {
            let new = events.len() - 1;
            if !derive::content_successor_may_add_cuts(events, new) {
                return Ok(());
            }
            let counted = &self.caches.commit.successor_checks;
            counted.set(counted.get().saturating_add(1));
            if let Some(cell) = index {
                derive::ensure_index(cell, &events[..new]);
            }
            derive::check_successor_cuts(branch, events, new, index)
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
    /// How many times this handle loaded its view in full from the stored
    /// projection (tests; WP3.INT).
    #[doc(hidden)]
    pub fn view_loads(&self) -> u64 {
        self.caches.commit.view_loads.get()
    }
    /// Sets the connection's `temp_store` to `MEMORY` (`memory`) or back to
    /// the default, outside a transaction (SQLite refuses it inside one). On
    /// Windows the worker's engine store keeps its commits' savepoint
    /// sub-journals in memory this way from its open on
    /// (`inbox::startup_drain`, WP4.4). The
    /// change drops the connection's TEMP
    /// objects, among them the commit-epoch table (module docs,
    /// "Rolled-back commits and the view"): its stored value is carried over
    /// unchanged, so the change neither drops the view (an O(projection)
    /// reload at the next commit) nor hides a rolled-back commit.
    #[doc(hidden)]
    pub fn set_temp_store_memory(&self, memory: bool) -> Result<()> {
        let changes = self.db.total_changes();
        let stored: Option<i64> = self
            .db
            .prepare("SELECT n FROM temp.tkfs_commit_epoch WHERE id=0")
            .and_then(|mut statement| statement.query_row([], |r| r.get(0)).optional())
            .ok()
            .flatten();
        self.db.execute_batch(if memory {
            "PRAGMA temp_store=MEMORY"
        } else {
            "PRAGMA temp_store=DEFAULT"
        })?;
        if let Some(n) = stored {
            self.db.execute_batch(
                "CREATE TEMP TABLE IF NOT EXISTS tkfs_commit_epoch(id INTEGER PRIMARY KEY CHECK(id=0), n INTEGER NOT NULL)",
            )?;
            self.db.execute(
                "INSERT INTO temp.tkfs_commit_epoch VALUES(0,?1) ON CONFLICT(id) DO UPDATE SET n=excluded.n",
                [n],
            )?;
        }
        // That row counts in `total_changes`, the view's freshness key: a
        // view fresh before it is fresh after it (no projection row changed).
        if let Ok(mut cache) = self.view_cache.try_borrow_mut()
            && let Some(view) = cache.as_mut().filter(|view| view.changes == changes)
        {
            view.changes = self.db.total_changes();
        }
        Ok(())
    }
    pub(super) fn count_view_load(&self) {
        let loads = &self.caches.commit.view_loads;
        loads.set(loads.get().saturating_add(1));
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
    /// (WP1.INT fix round 1, CCR-WP1.INT-4; CCR-WP2.INT-5 in WP4.4):
    /// `Stored::Delta` when writing the `current_parts` rows of the changed
    /// entities (and, when the conflict or pending list changed, the
    /// `current_meta` header row) makes the stored projection
    /// (`snapshot::load_projection`: `current_state`, its lists replaced by
    /// `current_meta`'s, overlaid with `current_parts`) read exactly `p`;
    /// otherwise a full rewrite of `current_state` (`persist_projection`).
    ///
    /// The full rewrite stores the whole projection (about 7 MB at 5K
    /// entries) on every slow commit, and that I/O made slow commits spike
    /// past the `concurrent_namespace_ops_5k` floor. A delta is possible when
    /// the stored projection (the connection's view, fresh by its own
    /// contract, else loaded) differs from `p` only in entries and their head
    /// registers and in its two lists, with no key removed and every changed
    /// register belonging to an entry of `p` and to one of the
    /// [`PART_FIELDS`]. A conflict-producing event (a concurrent edit) is a
    /// delta too since WP4.4: its new conflict list goes to the header row
    /// instead of a rewrite of the whole projection (CCR-WP2.INT-5).
    /// Otherwise (no stored projection yet, a removed key, or a read error)
    /// the full rewrite runs (`Stored::Rewrite`). A small projection (at
    /// most [`FOLD_MAX_ENTRIES`] entries) whose parts would outnumber half
    /// of its entries is also rewritten in full, folding the parts back into
    /// `current_state` (`Stored::Fold`, the entities that changed known); a
    /// larger one keeps its parts (at most one row per entity), so no slow
    /// commit rewrites a large projection under the lock. The derived-state
    /// oracle (`verify_derived_state`) compares exactly this stored
    /// projection with a rebuild.
    ///
    /// WP2.4: a prepared commit computes this on the read store, against the
    /// stored projection of the branch state it derived from; the write
    /// happens only if the branch is still in that state (`commit_prepared`).
    fn projection_delta(&self, branch: &str, p: &Projection) -> Stored {
        self.drop_rolled_back_view();
        let Ok(view) = self.view(branch) else {
            return Stored::Rewrite;
        };
        let stored = &view.projection;
        if !stored.entries.keys().all(|id| p.entries.contains_key(id))
            || !stored.heads.keys().all(|key| p.heads.contains_key(key))
        {
            return Stored::Rewrite;
        }
        let header = stored.conflicts != p.conflicts || stored.pending != p.pending;
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
            let Some((entity, field)) = key.rsplit_once('/') else {
                return Stored::Rewrite;
            };
            if !PART_FIELDS.contains(&field) || !p.entries.contains_key(entity) {
                return Stored::Rewrite;
            }
            changed.insert(entity.to_owned());
        }
        drop(view);
        if p.entries.len() > FOLD_MAX_ENTRIES {
            return Stored::Delta { changed, header };
        }
        let parts: Option<usize> = self
            .db
            .query_row(
                "SELECT count(*) FROM current_parts WHERE branch=?",
                [branch],
                |r| r.get::<_, i64>(0),
            )
            .ok()
            .and_then(|parts| usize::try_from(parts).ok());
        match parts {
            None => Stored::Rewrite,
            Some(parts) if parts.saturating_add(changed.len()) <= p.entries.len() / 2 => {
                Stored::Delta { changed, header }
            }
            Some(_) => Stored::Fold { changed },
        }
    }
    /// The branch's projection as this handle's fresh view (loaded if
    /// needed) and its slots, taken out of the view cache; `None` when it
    /// cannot be read.
    fn take_view(&self, branch: &str) -> Option<ViewCache> {
        self.drop_rolled_back_view();
        if self.view(branch).is_err() {
            return None;
        }
        self.view_cache
            .try_borrow_mut()
            .ok()?
            .take()
            .filter(|view| view.branch == branch)
    }
    /// Puts back a view [`Self::take_view`] took and nothing used (WP5.INT,
    /// the WP5.1 review's minor: a declined `project_append` left the next
    /// reader an O(entries) reload). Its freshness key is the one it was
    /// taken with, which the next `view` checks as usual; a view cached
    /// meanwhile wins.
    fn restore_view(&self, view: ViewCache) {
        if let Ok(mut cache) = self.view_cache.try_borrow_mut()
            && cache.is_none()
        {
            *cache = Some(view);
        }
    }
    /// `projection_delta` for a projection `derive::project_append` made from
    /// the stored one: it knows the entities that changed and whether the
    /// lists did, so nothing is compared (WP5.1). The same layout decision:
    /// a delta, or a fold of a small projection whose parts would outnumber
    /// half of its entries.
    fn appended_delta(
        &self,
        branch: &str,
        p: &Projection,
        changed: BTreeSet<String>,
        header: bool,
    ) -> Stored {
        if p.entries.len() > FOLD_MAX_ENTRIES {
            return Stored::Delta { changed, header };
        }
        let parts: Option<usize> = self
            .db
            .query_row(
                "SELECT count(*) FROM current_parts WHERE branch=?",
                [branch],
                |r| r.get::<_, i64>(0),
            )
            .ok()
            .and_then(|parts| usize::try_from(parts).ok());
        match parts {
            None => Stored::Rewrite,
            Some(parts) if parts.saturating_add(changed.len()) <= p.entries.len() / 2 => {
                Stored::Delta { changed, header }
            }
            Some(_) => Stored::Fold { changed },
        }
    }
    /// The slow path's computation for `event` (WP2.4): the branch's events
    /// (the event cache) plus `event`, projected; `None` when the projection
    /// is pending (a prerequisite outside the branch's journal); then the
    /// stored rows `event` reviews, the derived records and how the
    /// projection is stored. Reads only; with `precompute` also serializes
    /// the projection when its write or its seal needs the JSON (a full
    /// rewrite, or a branch sealed inline) and computes the view slots, so
    /// that the write does no O(projection) work. Errors are the slow path's
    /// refusals, as the historical inline path raised them.
    pub(super) fn compute_slow(
        &self,
        event: &Event,
        committed_view: bool,
        precompute: bool,
    ) -> Result<Option<Box<SlowPlan>>> {
        let branch = event.branch.id.as_str();
        // Full mode stays the reference: it projects the branch in full, as
        // it derives it in full, so it leaves the view where it is.
        let full_mode = derive::conflict_mode() == derive::ConflictMode::Full;
        // The stored projection of the branch, as this handle's view: the
        // base of `derive::project_append` (WP5.1). It is consumed; a
        // committing slow commit leaves its own projection as the view, and
        // a declined append hands it back to the view cache (WP5.INT).
        let mut view = if full_mode {
            None
        } else {
            self.take_view(branch)
        };
        let base = view.as_mut().map(|v| std::mem::take(&mut v.projection));
        let computed =
            self.with_branch_index(branch, Some(event), committed_view, |events, index| {
                let new = events.len() - 1;
                // The index of the cached events, built once per cache entry.
                if let Some(cell) = index {
                    derive::ensure_index(cell, &events[..new]);
                }
                let (appended, returned) = match base {
                    Some(base) => {
                        let index = index.map(|cell| cell.borrow());
                        let index = index.as_deref().and_then(Option::as_ref);
                        match derive::project_append_or_base(branch, base, events, new, index)? {
                            Ok(appended) => (Some(appended), None),
                            Err(base) => (None, Some(base)),
                        }
                    }
                    None => (None, None),
                };
                let (projection, delta, fresh) = match appended {
                    Some(appended) => (
                        appended.projection,
                        Some((appended.changed, appended.lists, appended.placements)),
                        Some(appended.fresh),
                    ),
                    None => (
                        project(branch, events)?,
                        None,
                        None,
                    ),
                };
                if delta.is_some() && derive::checks_projections() {
                    // The commit checks (and verify mode) compare it with
                    // `project` itself.
                    let full = project(branch, events)?;
                    ensure!(
                        full == projection,
                        "CONFLICT_MODE_MISMATCH: the appended projection of {} differs from project",
                        event.id
                    );
                }
                if !projection.pending.is_empty() {
                    return Ok((None, returned));
                }
                let reviewed = derive::read_reviewed(&self.db, branch, event)?;
                let derived = derive_conflicts(&DeriveInput {
                        branch,
                        projection: &projection,
                        events,
                        new_event: Some(&event.id),
                        reviewed: &reviewed,
                        fresh: fresh.as_ref(),
                        index,
                    })?;
                Ok::<_, anyhow::Error>((Some((projection, derived, delta)), returned))
            })??;
        let (computed, returned) = computed;
        if let Some(base) = returned
            && let Some(mut taken) = view.take()
        {
            // Declined: the base is the view it was, for `projection_delta`
            // below and the next reader.
            taken.projection = base;
            self.restore_view(taken);
        }
        let base_slots = view.map(|v| v.slots);
        let Some((projection, derived, delta)) = computed else {
            return Ok(None);
        };
        let (stored, patched) = match delta {
            Some((changed, lists, placements)) => {
                let patched =
                    base_slots.and_then(|slots| patched_slots(slots, &placements, &projection));
                if patched.is_some() && derive::checks_projections() {
                    ensure!(
                        patched == view_slots(&projection),
                        "CONFLICT_MODE_MISMATCH: the patched view slots of {} differ",
                        event.id
                    );
                }
                let stored = self.appended_delta(branch, &projection, changed, lists);
                (stored, patched)
            }
            None => (self.projection_delta(branch, &projection), None),
        };
        let (json, slots) = if precompute {
            let json = (!stored.is_delta() || !snapshot::sealed_as_rows(&self.db, branch))
                .then(|| serde_json::to_string(&projection))
                .transpose()?;
            (json, patched.or_else(|| view_slots(&projection)))
        } else {
            (None, patched)
        };
        Ok(Some(Box::new(SlowPlan {
            projection,
            derived,
            stored,
            json,
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
        _view_base: Option<i64>,
    ) -> Result<()> {
        let Checked {
            payload,
            objects,
            base_seq,
        } = checked;
        let incremental = matches!(plan, Plan::Fast(_));
        let branch = event.branch.id.clone();
        // Never patch (or seal) a view that holds a rolled-back commit.
        self.drop_rolled_back_view();
        // Other connections' commits that changed no projection row keep the
        // view (WP3.INT).
        self.adopt_foreign_commits();
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
        // The first write acquired SQLite's writer. No other connection can
        // now change this branch before publication. Reject stale preparation
        // rather than overwrite a projection prepared from an older history.
        ensure!(
            frontier_seq(&tx, &branch)? == base_seq,
            "STALE_METADATA_PREPARATION"
        );
        let projection_base = snapshot::projection_version(&tx);
        let patchable = incremental
            && projection_base.is_some()
            && self.view_cache.borrow().as_ref().is_some_and(|view| {
                view.branch == branch && view.projection_version == projection_base
            });
        tx.execute(
            "INSERT INTO events VALUES(?,?,?)",
            params![event.id, event.branch.id, payload],
        )?;
        admission::append_event(&tx, &event, local)?;
        // The event's journal row (the `journal_sequence` trigger) is the
        // journal's newest, so its sequence is the branch's new tip: the
        // cursor of a seal in this commit.
        let seq: i64 = tx
            .prepare_cached("SELECT seq FROM journal_sequence WHERE event=?")?
            .query_row([&event.id], |r| r.get(0))?;
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
                if let Stored::Delta { changed, header } = &slow.stored {
                    // The stored projection becomes `projection` through the
                    // rows of the entities that changed (CCR-WP1.INT-4) and,
                    // when its lists changed, its header row (CCR-WP2.INT-5).
                    for entity in changed {
                        write_part(&tx, &branch, entity, &slow.projection)?;
                    }
                    if *header {
                        snapshot::write_header(
                            &tx,
                            &branch,
                            &slow.projection,
                            Some(&self.caches.commit.anchors),
                        )?;
                    }
                } else {
                    let state = match &slow.json {
                        Some(state) => std::borrow::Cow::Borrowed(state.as_str()),
                        None => std::borrow::Cow::Owned(serde_json::to_string(&slow.projection)?),
                    };
                    tx.execute("DELETE FROM current_parts WHERE branch=?", [&branch])?;
                    tx.execute("INSERT INTO current_state VALUES(?,?) ON CONFLICT(branch) DO UPDATE SET payload=excluded.payload", params![branch, state])?;
                    snapshot::clear_header(&tx, &branch)?;
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
                // A periodic seal (when the branch's interval is due) seals
                // the projection the commit holds (the view, patched now)
                // instead of reading it back (CCR-WP1.6-1 adapter): an inline
                // seal serializes it, a rows seal the marked entities of it.
                let mut delta = Some(delta);
                if snapshot::seal_due(&tx, &branch, seq)? {
                    if patchable {
                        let mut cache = self.view_cache.borrow_mut();
                        if let (Some(view), Some(delta)) = (cache.as_mut(), delta.take()) {
                            if patch_view(view, &event, delta).is_ok() {
                                patched = true;
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
                    let report = snapshot::seal(
                        &tx,
                        &branch,
                        snapshot::Seal {
                            cursor: Some(seq),
                            projection: known,
                            json: None,
                            changed: snapshot::Changed::Marked,
                            anchors: Some(&self.caches.commit.anchors),
                            lists_unchanged: true,
                        },
                    )?;
                    drop(cache);
                    record_seal(&self.caches.commit, &report);
                    if patched {
                        let seals = &self.caches.commit.known_seals;
                        seals.set(seals.get().saturating_add(1));
                    }
                }
                Plan::Fast(delta.unwrap_or_default())
            }
            Plan::Slow(slow) => {
                // The forced seal after a slow-path commit is kept (REVAMP_PLAN
                // P4). It seals the projection this commit holds (and has just
                // stored): the entities it changed besides the marked ones
                // (WP4.4); an inline seal uses the JSON a prepared commit
                // serialized off-lock.
                let changed = match &slow.stored {
                    Stored::Delta { changed, .. } | Stored::Fold { changed } => {
                        snapshot::Changed::Plus(changed)
                    }
                    Stored::Rewrite => snapshot::Changed::All,
                };
                let lists_unchanged = matches!(slow.stored, Stored::Delta { header: false, .. });
                let report = snapshot::seal(
                    &tx,
                    &branch,
                    snapshot::Seal {
                        cursor: Some(seq),
                        projection: Some(&slow.projection),
                        json: slow.json.as_deref(),
                        changed,
                        anchors: Some(&self.caches.commit.anchors),
                        lists_unchanged,
                    },
                )?;
                record_seal(&self.caches.commit, &report);
                if report.used_json {
                    let seals = &self.caches.commit.prepared_seals;
                    seals.set(seals.get().saturating_add(1));
                }
                Plan::Slow(slow)
            }
        };
        let epoch = bump_commit_epoch(&tx, self.caches.commit.epoch.get())?;
        // The projection counter this commit leaves, read inside it: no other
        // connection's commit is in it (WP3.INT, see `adopt_foreign_commits`).
        let projection_version = snapshot::projection_version(&tx);
        tx.commit()?;
        self.caches.commit.epoch.set(epoch);
        self.count_commit(&event.id, incremental);
        #[cfg(test)]
        tests::after_commit();
        // Read data_version before the projection counter: a later metadata
        // commit either changes the counter below or invalidates this token on
        // the next lookup. Object bookkeeping alone need not reload the view.
        let data_version = self.data_version().ok();
        let unchanged = projection_version.is_some()
            && snapshot::projection_version(&self.db) == projection_version;
        let mut cache = self.view_cache.borrow_mut();
        match plan {
            Plan::Fast(_) if patched => match cache.as_mut() {
                Some(view) if data_version.is_some() && unchanged => {
                    view.data_version = data_version.unwrap();
                    view.changes = self.db.total_changes();
                    view.transactional = !self.db.is_autocommit();
                    view.projection_version = projection_version;
                }
                _ => *cache = None,
            },
            Plan::Fast(delta)
                if cache.as_ref().is_some_and(|v| {
                    v.branch == branch && patchable && data_version.is_some() && unchanged
                }) =>
            {
                let view = cache.as_mut().unwrap();
                if patch_view(view, &event, delta).is_err() {
                    *cache = None;
                } else {
                    view.changes = self.db.total_changes();
                    view.transactional = !self.db.is_autocommit();
                    view.projection_version = projection_version;
                    view.data_version = data_version.unwrap();
                }
            }
            Plan::Slow(slow) => {
                *cache = match data_version.filter(|_| unchanged) {
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
                            projection_version,
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
    /// when given, through the event cache (module docs). `committed_view`:
    /// the connection reads committed state only (a read transaction of its
    /// own, as `prepare_slow` runs), so nothing it reads can roll back; in
    /// autocommit that holds anyway.
    pub(super) fn with_branch_events<T>(
        &self,
        branch: &str,
        extra: Option<&Event>,
        committed_view: bool,
        f: impl FnOnce(&[Event]) -> T,
    ) -> Result<T> {
        self.with_branch_index(branch, extra, committed_view, |events, _| f(events))
    }
    /// `with_branch_events`, with the cache entry's derivation index (WP5.1)
    /// when the entry is kept: it indexes the cached events, `extra` aside.
    pub(super) fn with_branch_index<T>(
        &self,
        branch: &str,
        extra: Option<&Event>,
        committed_view: bool,
        f: impl FnOnce(&[Event], Option<&derive::IndexCell>) -> T,
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
        let result = f(&cache.events, keep.then_some(&cache.index));
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
        let mut statement = self
            .db
            .prepare_cached("SELECT payload FROM events WHERE branch=?")?;
        let mut rows = statement.query([branch])?;
        while let Some(row) = rows.next()? {
            events.push(serde_json::from_str(row.get_ref(0)?.as_str()?)?);
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
            index: Default::default(),
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
                added.push(serde_json::from_str(row.get_ref(3)?.as_str()?)?);
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
            let mut positions = Vec::with_capacity(added.len());
            let mut old = old.into_iter().peekable();
            let mut added = added.into_iter().peekable();
            loop {
                let next = match (old.peek(), added.peek()) {
                    (Some(a), Some(b)) if a.id == b.id => return Ok(None),
                    (Some(a), Some(b)) if a.id < b.id => old.next(),
                    (Some(_), Some(_)) | (None, Some(_)) => {
                        positions.push(merged.len());
                        added.next()
                    }
                    (Some(_), None) => old.next(),
                    (None, None) => break,
                };
                merged.extend(next);
            }
            cache.events = merged;
            // The index follows the cache, or is dropped (rebuilt on demand).
            let follows = match cache.index.get_mut() {
                Some(index) => index.extend(&cache.events, &positions)?,
                None => true,
            };
            if !follows {
                *cache.index.get_mut() = None;
            }
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
        self.with_branch_events(branch, None, false, |events| events.to_vec())
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
    /// Every seal written through this handle (WP4.4): how many of each
    /// layout, their bytes (anchors plus rows) and durations. A seal whose
    /// transaction rolled back counts too. A rebuild's seal (`core.rs`,
    /// `snapshot::persist`) is not counted.
    #[doc(hidden)]
    pub fn seal_stats(&self) -> SealStats {
        self.caches.commit.seal_stats.get()
    }
    pub(super) fn record_seal(&self, report: &snapshot::SealReport) {
        record_seal(&self.caches.commit, report);
    }
    /// How many branches this handle's open resumed from their startup
    /// snapshot instead of reconstructing them (tests).
    #[doc(hidden)]
    pub fn startup_resumes(&self) -> u64 {
        self.caches.commit.resumed.get()
    }
    pub(super) fn count_resume(&self) {
        let resumed = &self.caches.commit.resumed;
        resumed.set(resumed.get().saturating_add(1));
    }
}

/// Adds `report` to `cache`'s seal counters (a free function: the commit
/// path holds the store's connection mutably while it seals).
fn record_seal(cache: &Cache, report: &snapshot::SealReport) {
    let mut stats = cache.seal_stats.get();
    stats.add(report);
    cache.seal_stats.set(stats);
}

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

/// Patch `view` with a fast commit's `delta` (the changed entities' entries
/// and head registers) and their slots. On an error the view is partly
/// patched and must be dropped.
pub(super) fn patch_view(view: &mut ViewCache, event: &Event, delta: Projection) -> Result<()> {
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
    /// The conflict gate of the creation and namespace successors: `true`
    /// refuses the fast path. With remote fast paths, a conflict of the
    /// current projection that touches what the event changes
    /// ([`conflicts_touch`], module docs; WP4.INT re-review: it was any
    /// unresolved conflict of the branch); with `TKFS_REMOTE_FASTPATH=0`, any
    /// conflict of the projection or any conflict row of the branch (the
    /// previous gate, also the oracle fallback of §5.1).
    fn successor_conflict_gate(
        &self,
        branch: &str,
        current: &Projection,
        touched: &Touched<'_>,
    ) -> Result<bool> {
        if remote_fast_paths_enabled() {
            return Ok(conflicts_touch(current, touched));
        }
        Ok(!current.conflicts.is_empty()
            || self.db.query_row(
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
                entry.set_content_value(&change.value)?;
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
        let parent = fields["location"]["parent"]
            .as_str()
            .context("INVALID_LOCATION")?;
        let name = fields["location"]["name"]
            .as_str()
            .context("INVALID_LOCATION")?;
        let touched = Touched {
            entities: BTreeSet::from([entity.as_str()]),
            parents: BTreeSet::from([parent]),
            slots: BTreeSet::from([(parent.to_owned(), name_key(name)?)]),
        };
        if current.entries.contains_key(entity)
            || !current.pending.is_empty()
            || self.successor_conflict_gate(&event.branch.id, current, &touched)?
        {
            return Ok(None);
        }
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
                entry.set_content_value(&change.value)?;
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
        let mut touched = Touched::default();
        for change in &event.changes {
            let Some(entry) = current.entries.get(&change.entity) else {
                return Ok(None);
            };
            touched.entities.insert(&change.entity);
            touched.parents.insert(&entry.parent);
            touched
                .slots
                .insert((entry.parent.clone(), name_key(&entry.name)?));
            if change.field == "location" {
                let parent = change.value["parent"]
                    .as_str()
                    .context("INVALID_LOCATION")?;
                let name = change.value["name"].as_str().context("INVALID_LOCATION")?;
                touched.parents.insert(parent);
                touched.slots.insert((parent.to_owned(), name_key(name)?));
            }
        }
        if !current.pending.is_empty()
            || self.successor_conflict_gate(&event.branch.id, current, &touched)?
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

/// What a creation or namespace successor touches: the entities it changes,
/// the parent directories they leave or enter, and the `(parent, name key)`
/// slots they leave or take.
#[derive(Default)]
struct Touched<'a> {
    entities: BTreeSet<&'a str>,
    parents: BTreeSet<&'a str>,
    slots: BTreeSet<(String, String)>,
}

/// Whether a conflict of `current` touches what a successor changes (module
/// docs): the conflict names a touched entity or parent (a pair `a,b` by
/// either side), or one of its alternatives or bases is a location inside a
/// touched entity (a created entity that a conflicted entry names as its
/// missing parent, a moved directory) or in a touched slot. A location in a
/// touched parent's other slots does not count: that parent is a live
/// directory (the successors check it), so no entry under it has an invalid
/// chain, and its collisions are per slot.
fn conflicts_touch(current: &Projection, touched: &Touched<'_>) -> bool {
    current.conflicts.iter().any(|conflict| {
        conflict
            .entity
            .split(',')
            .any(|id| touched.entities.contains(id) || touched.parents.contains(id))
            || conflict
                .alternatives
                .iter()
                .chain(&conflict.bases)
                .any(|version| {
                    let Some(parent) = version.value.get("parent").and_then(Value::as_str) else {
                        return false;
                    };
                    touched.entities.contains(parent)
                        || version
                            .value
                            .get("name")
                            .and_then(Value::as_str)
                            .and_then(|name| name_key(name).ok())
                            .is_some_and(|key| touched.slots.contains(&(parent.to_owned(), key)))
                })
    })
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

    thread_local! {
        static AFTER_COMMIT: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
    }

    pub(super) fn after_commit() {
        let hook = AFTER_COMMIT.with(|slot| slot.borrow_mut().take());
        if let Some(hook) = hook {
            hook();
        }
    }

    fn content_event(store: &Store, entity: &str, object: &str) -> Event {
        store.event(
            &store.branch(&store.active).unwrap(),
            vec![
                store
                    .current_change(entity, "content", json!(object))
                    .unwrap(),
            ],
        )
    }

    #[test]
    fn an_overlapping_object_commit_keeps_the_patched_view() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path(), None).unwrap();
        let file = store.create("file", "file").unwrap();
        let object = store.put_object(b"new content").unwrap();
        let event = content_event(&store, &file.id, &object);
        let root = store.root.clone();
        let repo = store.repo.clone();
        let device = store.device.clone();
        let key = object.clone();
        AFTER_COMMIT.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                let other = Store::open_secondary(&root, &repo, &device).unwrap();
                other
                    .db
                    .execute(
                        "INSERT OR REPLACE INTO object_requests VALUES(?,?)",
                        params![key, time()],
                    )
                    .unwrap();
            }))
        });
        let loads = store.view_loads();
        store.commit(event, true).unwrap();
        assert_eq!(store.lookup("file").unwrap().content, Some(object));
        assert_eq!(
            store.view_loads(),
            loads,
            "bookkeeping forced a projection reload"
        );
        assert!(store.verify_derived_state().unwrap().is_empty());
    }

    #[test]
    fn an_overlapping_metadata_commit_is_never_hidden_by_a_new_freshness_token() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path(), None).unwrap();
        let a = store.create("a", "file").unwrap();
        let b = store.create("b", "file").unwrap();
        let local = store.put_object(b"local").unwrap();
        let foreign = store.put_object(b"foreign").unwrap();
        let event = content_event(&store, &a.id, &local);
        let second = content_event(&store, &b.id, &foreign);
        let root = store.root.clone();
        let repo = store.repo.clone();
        let device = store.device.clone();
        AFTER_COMMIT.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                let mut other = Store::open_secondary(&root, &repo, &device).unwrap();
                other.commit(second, true).unwrap();
            }))
        });
        store.commit(event, true).unwrap();
        assert_eq!(store.lookup("a").unwrap().content, Some(local));
        assert_eq!(store.lookup("b").unwrap().content, Some(foreign));
        assert!(store.verify_derived_state().unwrap().is_empty());
    }

    #[test]
    fn a_foreign_commit_after_preparation_rejects_the_stale_delta() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path(), None).unwrap();
        let a = store.create("a", "file").unwrap();
        let b = store.create("b", "file").unwrap();
        let local = store.put_object(b"local").unwrap();
        let foreign = store.put_object(b"foreign").unwrap();
        let event = content_event(&store, &a.id, &local);
        let Check::Ready(checked) = store.check_commit(&event, true).unwrap() else {
            panic!()
        };
        let fast = store.fast_successor(&event, true).unwrap();
        let second = content_event(&store, &b.id, &foreign);
        let mut other = Store::open_secondary(&store.root, &store.repo, &store.device).unwrap();
        other.commit(second, true).unwrap();
        let error = store
            .commit_checked(event.clone(), true, checked, fast)
            .unwrap_err();
        assert!(error.to_string().contains("STALE_METADATA_PREPARATION"));
        assert!(!store.events().unwrap().iter().any(|old| old.id == event.id));
        store.commit(event, true).unwrap();
        assert_eq!(store.lookup("a").unwrap().content, Some(local));
        assert_eq!(store.lookup("b").unwrap().content, Some(foreign));
        assert!(store.verify_derived_state().unwrap().is_empty());
    }

    /// WP5.1 task 5: a local commit of bytes this process has just stored
    /// (`put_object`, `put_stream`) does not hash them again; bytes that
    /// changed on disk since (another size or mtime) are verified, and
    /// corrupt ones are refused as before.
    #[test]
    fn a_local_commit_skips_the_rehash_of_bytes_just_verified() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Store::open(dir.path(), None).unwrap();
        let file = s.create("f.txt", "file").unwrap();
        let skipped = verified::skipped();
        let hashes = s.object_hashes();
        let content = s.entry(&file.id).unwrap().revisions.get("content").cloned();
        s.write_revision(&file.id, content, b"fresh bytes").unwrap();
        assert_eq!(
            s.object_hashes(),
            hashes,
            "the fresh object was hashed again"
        );
        assert!(verified::skipped() > skipped);
        let mut stream: &[u8] = b"streamed bytes";
        let content = s.entry(&file.id).unwrap().revisions.get("content").cloned();
        s.write_stream_with_base_and_times(&file.id, content, None, &mut stream, 0)
            .unwrap();
        assert_eq!(
            s.object_hashes(),
            hashes,
            "the streamed object was hashed again"
        );
        // Changed behind the store's back (same size): verified, refused.
        let object = s.put_object(b"other bytes").unwrap();
        fs::write(s.root.join("objects").join(&object), b"Other bytes").unwrap();
        let content = s.entry(&file.id).unwrap().revisions.get("content").cloned();
        let refused = s.write_object_with_base_and_times(&file.id, content, None, object, 0);
        assert!(
            format!("{:#}", refused.unwrap_err()).contains("CORRUPT_OBJECT"),
            "corrupt bytes accepted"
        );
        assert_eq!(s.object_hashes(), hashes + 1);
    }

    /// WP5.1 review (round 1): a verification observed within `RACY_NS` of
    /// the file's mtime is not recorded, so a same-size rewrite in that tick
    /// is hashed again and refused; a verification well after the mtime is
    /// recorded and skips the next re-hash. Availability rows count only when
    /// written since this process started, and within `FRESH_MS`.
    #[test]
    fn a_racy_or_earlier_verification_is_not_trusted() {
        // The set's rule.
        let root = Path::new("racy-root");
        let mtime = 1_000_000_000_000_000_000;
        verified::note(root, "h1", 5, mtime, mtime + verified::RACY_NS - 1);
        assert!(!verified::recent(root, "h1", 5, mtime));
        verified::note(root, "h2", 5, mtime, mtime + verified::RACY_NS);
        assert!(verified::recent(root, "h2", 5, mtime));
        assert!(!verified::recent(root, "h2", 6, mtime));
        assert!(!verified::recent(root, "h2", 5, mtime + 1));
        // Availability rows.
        let now = time();
        let epoch = verified::epoch_ms();
        assert!(epoch <= now);
        assert!(verified::fresh_row(now as i64, now));
        assert!(!verified::fresh_row(epoch as i64 - 1, now));
        assert!(!verified::fresh_row(0, now));
        assert!(!verified::fresh_row(
            now as i64,
            now + verified::FRESH_MS + 1
        ));
        // Bytes installed behind the store's back with a racy mtime: hashed,
        // not recorded; rewritten (same size) and given that mtime again,
        // they are hashed again and refused.
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(dir.path(), None).unwrap();
        let bytes = b"racy bytes";
        let object = hash(bytes);
        let path = s.root.join("objects").join(&object);
        fs::write(&path, bytes).unwrap();
        // In the future: racy however long the machine stalls before the
        // verification reads it (a minute).
        let stamp = std::time::SystemTime::now() + std::time::Duration::from_secs(60);
        fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(stamp)
            .unwrap();
        let hashes = s.object_hashes();
        s.verify_new_object(&object).unwrap();
        assert_eq!(s.object_hashes(), hashes + 1);
        fs::write(&path, b"Racy bytes").unwrap();
        fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(stamp)
            .unwrap();
        let refused = s.verify_new_object(&object).unwrap_err();
        assert!(
            format!("{refused:#}").contains("CORRUPT_OBJECT"),
            "{refused:#}"
        );
        assert_eq!(s.object_hashes(), hashes + 2);
        // Bytes whose mtime is well before the verification: recorded.
        let bytes = b"settled bytes";
        let object = hash(bytes);
        let path = s.root.join("objects").join(&object);
        fs::write(&path, bytes).unwrap();
        fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(std::time::SystemTime::now() - std::time::Duration::from_secs(60))
            .unwrap();
        s.verify_new_object(&object).unwrap();
        assert_eq!(s.object_hashes(), hashes + 3);
        let skipped = verified::skipped();
        s.verify_new_object(&object).unwrap();
        assert_eq!(s.object_hashes(), hashes + 3);
        assert!(verified::skipped() > skipped);
    }

    /// WP5.INT (the WP5.1 review's minor, CCR-WP5.1-1): `put_object` records
    /// the bytes it installs in the verified set, and a later verification
    /// that finds them corrupt wins over it: the same process's (`forget`)
    /// and one whose `CORRUPT` row is all this handle sees (another process:
    /// the row is read first). The rewrite keeps the size and the stamped
    /// mtime, so only the index can tell.
    #[test]
    fn a_later_corrupt_verdict_wins_over_the_verified_set() {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(dir.path(), None).unwrap();
        let rewrite = |s: &Store, object: &str, bytes: &[u8]| {
            let path = s.root.join("objects").join(object);
            let mtime = fs::metadata(&path).unwrap().modified().unwrap();
            fs::write(&path, bytes).unwrap();
            fs::File::options()
                .write(true)
                .open(&path)
                .unwrap()
                .set_modified(mtime)
                .unwrap();
        };
        // This process's verification: the installer recorded the bytes.
        let object = s.put_object(b"original bytes").unwrap();
        let hashes = s.object_hashes();
        s.verify_new_object(&object).unwrap();
        assert_eq!(s.object_hashes(), hashes, "the install was recorded");
        rewrite(&s, &object, b"Original bytes");
        assert!(s.verify_object(&object).is_err());
        let refused = s.verify_new_object(&object).unwrap_err();
        assert!(
            format!("{refused:#}").contains("CORRUPT_OBJECT"),
            "{refused:#}"
        );
        // Another process's verdict: only its row.
        let object = s.put_object(b"second bytes").unwrap();
        s.verify_new_object(&object).unwrap();
        rewrite(&s, &object, b"Second bytes");
        s.db.execute(
            "UPDATE object_availability SET state=?1,verified_ms=0 WHERE hash=?2",
            params![objects::CORRUPT, object],
        )
        .unwrap();
        let path = s.root.join("objects").join(&object);
        let metadata = fs::metadata(&path).unwrap();
        assert!(
            verified::recent(
                &s.root,
                &object,
                metadata.len(),
                objects::mtime_ns(&metadata)
            ),
            "the set still holds the install"
        );
        let refused = s.verify_new_object(&object).unwrap_err();
        assert!(
            format!("{refused:#}").contains("CORRUPT_OBJECT"),
            "{refused:#}"
        );
    }

    /// WP4.4 (CCR-WP2.4-2): every seal is written by `snapshot::seal`, so a
    /// prepared commit's seal from the JSON it serialized off-lock is the
    /// seal of the projection itself, byte for byte (it replaces WP2.4's
    /// pin of the payload mirror this module no longer has), on empty and
    /// populated branches with register conflicts, a resolved one and a
    /// deleted entry; and the seal is a valid snapshot.
    #[test]
    fn a_seal_from_prepared_json_is_the_seal_of_the_projection() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Store::open(dir.path(), None).unwrap();
        let check = |s: &Store| {
            let branch = s.active.clone();
            let projection = snapshot::load_projection(&s.db, &branch).unwrap();
            let json = serde_json::to_string(&projection).unwrap();
            let anchor = |s: &Store| -> (i64, String, String) {
                s.db.query_row(
                    "SELECT cursor,payload,checksum FROM startup_snapshots WHERE branch=?",
                    [&branch],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .unwrap()
            };
            let seal = |json: Option<&str>| {
                snapshot::seal(
                    &s.db,
                    &branch,
                    snapshot::Seal {
                        cursor: None,
                        projection: Some(&projection),
                        json,
                        changed: snapshot::Changed::All,
                        anchors: None,
                        lists_unchanged: false,
                    },
                )
                .unwrap()
            };
            s.db.execute_batch("SAVEPOINT a").unwrap();
            assert!(!seal(None).used_json);
            let expected = anchor(s);
            s.db.execute_batch("ROLLBACK TO a").unwrap();
            assert!(seal(Some(&json)).used_json);
            assert_eq!(anchor(s), expected);
            s.db.execute_batch("RELEASE a").unwrap();
            let checked = s.check_startup_snapshot(&branch).unwrap();
            assert_eq!(checked.projection, projection);
            assert!(!checked.rows, "a small projection is sealed inline");
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
