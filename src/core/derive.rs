//! Deterministic derivation from the journal: projection, register and
//! namespace conflicts, namespace-history cuts and causal ordering.
//!
//! # Conflict derivation modes (WP1.7, REVAMP_PLAN §5.2)
//!
//! `persist_projection(tx, branch, p, events, Some(E))` records the conflicts
//! of one newly accepted event `E`. `TKFS_CONFLICT_MODE` picks how:
//!
//! - `full`: the reference. Every historical location pair, every reviewed
//!   cut and every antichain of every namespace cone is re-derived; namespace
//!   rows that are no longer derived are deleted.
//! - `append` (default): derives only what `E` can add, under an exactness
//!   precondition checked on every commit, and runs `full` whenever the
//!   precondition does not hold.
//! - `verify`: runs both in separate savepoints, keeps the full result and
//!   fails the commit with `CONFLICT_MODE_MISMATCH` on any difference in
//!   conflict rows or `resolved_by` (used by the fuzzer and qualification).
//!
//! A rebuild (`new_event == None`) always derives in full.
//!
//! ## Pure derivation, cheap write (WP2.4)
//!
//! `persist_projection` is `derive_conflicts` (pure: it reads no table, so a
//! slow commit derives off-lock on a read handle) followed by
//! `write_projection` (the DELETE of namespace rows no longer derived when
//! the result asks for it, the upsert, the review receipts; in verify mode
//! and under the commit checks, the comparisons, which need the stored rows).
//! The only stored rows a derivation used to read, those the new event
//! reviews, are read first into `DeriveInput::reviewed`. Reviews are
//! validated during derivation (`INVALID_CONFLICT_REVIEW` is a derivation
//! error); the write only applies their receipts. Every refusal therefore
//! surfaces where it did before, with the same text.
//!
//! ## Successor counting (CCR-WP1.6-5)
//!
//! `successor_may_add_cuts` and `check_successor_cuts` give the commit path's
//! namespace successor full replay's `NAMESPACE_HISTORY_LIMIT` count without
//! deriving any record (see `commit.rs`). `check_successor_cuts` counts with
//! the same relevance, compression and enumeration as `cone_conflicts`
//! (`count_cuts`).
//!
//! ## Why append mode equals full mode (proof obligation)
//!
//! Notation: `O` = the branch's committed events, `N = O ∪ {E}`. `S` = the
//! stored conflict rows before the commit. Full mode stores
//! `S \ {namespace rows it no longer derives} ∪ D_full`, where `D_full` is
//! `p.conflicts`, the stored rows `E` reviews, all location pairs, all
//! reviewed cuts, `namespace_history(N)` and the register pairs of `E`
//! (deduplicated by id).
//!
//! **Induction hypothesis (H):** `S` is what a rebuild of `O` from no rows
//! derives (`Store::verify_derived_state_strict` is exactly this check; the
//! plain oracle misses excess rows). In particular the rebuild of `O`
//! succeeds, so every computation over `O` alone (every antichain cut's
//! projection included) succeeds and stays within `MAX_NAMESPACE_CUTS`.
//!
//! **Facts used:**
//! 1. `E` is a sink of the causal DAG: every committed event's parents were
//!    present when it committed, and `E` is new (checked: no event of `O`
//!    names `E` as a parent, and the projection has no pending event). So the
//!    ancestry among events of `O` is the same in `O` and in `N`, and `E` is
//!    in no causal cut of an event of `O`.
//! 2. `project` is independent of event order, and every cut it is given
//!    is downward closed.
//! 3. Under the ambiguity guards (no entity id contains `,`; `E` relocates at
//!    most one entity; no event places two entities in one slot; no two
//!    events relocate the same two entities), a conflict id determines its
//!    payload (up to the `resolved_by` field the column overrides), whichever
//!    source derives it. Then "last one wins" deduplication cannot differ.
//!
//! **Components:**
//! - *Location pairs.* A pair of events of `O` has the same ancestry in `N`
//!   (fact 1), so it derives the same record, already in `S` (H). Append
//!   derives the pairs that include `E`.
//! - *Reviewed cuts.* The cut of a review in `O` excludes `E` (fact 1), so it
//!   is unchanged. Append derives `E`'s own cut when `E` is a review.
//! - *Register pairs* already derive only new/old pairs (unchanged code).
//! - *`p.conflicts` and the rows `E` reviews* are the same input in both.
//! - *Namespace history.* Cones only grow when events are added (parent and
//!   slot edges are only added), so every old cone `cone_O(x)` is a subset of
//!   `cone_N(x)`. For each cone `C` of `N`:
//!   - `C` is an old cone and contains no entity whose kind, location or
//!     alive register `E` changes: its relevant set (relevance plus the TRUE
//!     presence compression) is unchanged, because `E` adds no relevant
//!     change and no alive version for its members, and the signatures of old
//!     TRUE versions do not change (fact 1). Same antichains, same cuts, same
//!     records: all in `S`. Skipped.
//!   - `C` is an old cone that `E` touches: checked
//!     `relevant_O(C) ⊆ relevant_N(C)` (§5.2 asks for equality up to `E`;
//!     a superset is exact too: a first tombstone turns on the compression
//!     and brings old presence witnesses in). Antichain-ness depends only on
//!     ancestry among the events, so the antichains of `relevant_N(C)` are
//!     the old ones, with the old cuts and records (in `S`), plus those
//!     containing a newly relevant event (`E` or a new witness), which are
//!     projected. All antichains are counted, as in full mode. A witness that
//!     stops being relevant (displaced by `E`'s rank) fails the check.
//!   - `C` is a new cone: derived (and counted) in full.
//!   - An old cone that is no longer a cone of `N` (`cone_O(x)` grew to
//!     `cone_N(x)`): if `cone_N(x)` is a new cone, checked
//!     `relevant_O(cone_O(x)) ⊆ relevant_N(cone_N(x))`; if it is an old cone,
//!     checked `relevant_O(cone_O(x)) ⊆ relevant_O(cone_N(x)) ⊆
//!     relevant_N(cone_N(x))`. Every old antichain of the grown cone is then
//!     an antichain of the cover, with the same cut, and the cover's member
//!     filter is a superset, so its old records are derived again (new
//!     cover) or stored and still derived by full mode (old cover).
//!
//!   Hence `namespace_history(O)`'s records are a subset of
//!   `namespace_history(N)`'s, and full mode's refusal
//!   (`NAMESPACE_HISTORY_LIMIT`) happens in append mode at the same cone: the
//!   untouched old cones' counts are unchanged and within the limit (H);
//!   every other cone is counted in full.
//! - *The old projection's records (checked).* `S` also holds the namespace
//!   records of the old current projection (`p_O.conflicts`, a source of the
//!   rebuild of `O`). These are not always records of a namespace-history
//!   cut: TRUE presence compression keeps each group's rank extrema, not the
//!   head of a presence chain, so the whole history can hold an alive state
//!   that no antichain cut has (`tests::current_record_outside_history`:
//!   a stale-base save on one entity and a presence chain across devices on
//!   its slot competitor). Such a row stays only while the projection holds
//!   it; when `E` takes it out, full mode's DELETE removes it. So:
//!   - an entity outside the cones that contain an entity `E` touches keeps
//!     its records (`p_O`'s equal `p_N`'s): a cone's records read only its
//!     members' kind, location and alive registers, and `E` changes none;
//!   - for the members of those cones (`AppendPlan::members`), every record
//!     of `project(O)` that append mode does not derive (`p_N`'s records are
//!     part of `D_append`) must be a record of its old cone's maximal
//!     antichain cut, which full mode re-derives (previous step); otherwise
//!     append falls back (`NsContext::current_records_survive`). Both are
//!     computed from the members' registers (`namespace_records`), exact
//!     because `O` and each antichain cut of `O` project (H).
//!
//!   Hence every namespace row of `S` is derived by full mode again, and its
//!   DELETE removes nothing.
//! - *Reviews of `E`* are validated against the derived list; a reviewed row
//!   is either stored (and loaded in both modes) or newly derived (derived in
//!   both modes, since everything full mode derives beyond append's list is
//!   already stored, H).
//!
//! So `S ∪ D_append` equals full mode's result (`S` minus nothing, plus
//! `D_full`) row for row, with the same `resolved_by` updates, and the result
//! again satisfies (H) for `N`. When a
//! check fails, append mode derives in full instead, which keeps (H) by
//! definition. The executable check of (H) is the oracle (with every row
//! re-derived from no rows: `Store::verify_derived_state_strict`); `verify`
//! mode, `set_thread_commit_checks` and `tests/derive_oracle.rs` check the
//! equality itself, and the unit test
//! `append_commit_equals_full_commit_with_delete_on_every_prefix` checks it as
//! a pure function, the DELETE included.
//!
//! ## Memoization
//!
//! Within one derivation (both modes) the causal closures of events are
//! bitsets computed once (up to `CLOSURE_MEMO_BYTES`), the per-entity
//! alive-register analysis is computed once per entity, and each distinct
//! antichain's cut is projected once (up to `CUT_MEMO_BYTES` of retained
//! records; past it, a recurring cut is projected again).
//!
//! ## Append-mode shortcuts (exact; full mode, the oracle's reference, keeps
//! the plain computation)
//!
//! - Cones are computed only for the entities that reach an entity `E`
//!   touches (`ConeGraph::reaching`), from one graph that records which
//!   edges only `E` contributes (`append_plan` explains why no other cone
//!   matters).
//! - A cut is projected from the cone members' own kind, location and alive
//!   registers (`NsContext::namespace_records`) when no cut of the branch can
//!   fail to project (`projection_cannot_fail`); otherwise the whole cut is
//!   projected. The only influence other entities have on a cone's records
//!   is such a failure.
//! - Slot keys (`name_key`) are memoized process-wide (`slot_key`), up to
//!   `SLOT_KEY_MEMO_BYTES`.
use super::*;
use std::{
    cell::Cell,
    collections::HashMap,
    rc::Rc,
    sync::{
        Arc, Mutex, OnceLock, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
};

/// Derivation cache (owned by WP1.7). Derivation runs inside
/// `persist_projection`, which has no `Store`, so all memoization lives in a
/// per-call context (`NsContext`) and this per-store cache stays empty.
#[derive(Default)]
pub(crate) struct Cache {}

/// How a commit derives its conflict records (`TKFS_CONFLICT_MODE`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConflictMode {
    /// Derive only what the new event adds, under the exactness precondition;
    /// otherwise derive in full.
    #[default]
    Append,
    /// Re-derive the whole branch history (the reference).
    Full,
    /// Run both and fail the commit with `CONFLICT_MODE_MISMATCH` on any
    /// difference; the full result is kept.
    Verify,
}
impl ConflictMode {
    /// `append`, `full` or `verify` (case-insensitive).
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "append" => Some(Self::Append),
            "full" => Some(Self::Full),
            "verify" => Some(Self::Verify),
            _ => None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Full => "full",
            Self::Verify => "verify",
        }
    }
}

static ENV_MODE: OnceLock<ConflictMode> = OnceLock::new();
thread_local! {
    static THREAD_MODE: Cell<Option<ConflictMode>> = const { Cell::new(None) };
    static THREAD_COUNTERS: Cell<DeriveCounters> = const { Cell::new(DeriveCounters::ZERO) };
    static LAST_FALLBACK: Cell<Option<&'static str>> = const { Cell::new(None) };
    static FALLBACK_REASONS: RefCell<BTreeMap<&'static str, u64>> = const { RefCell::new(BTreeMap::new()) };
    static COMMIT_CHECKS: Cell<bool> = const { Cell::new(false) };
}

/// Check every commit on the calling thread (any mode) against the
/// reference, inside the commit's transaction (rolled back): the branch's
/// conflict rows must equal a derivation from no rows (what the oracle's
/// rebuild derives), and a refused commit must be refused by full mode with
/// the same error. A failed check fails the commit with
/// `CONFLICT_MODE_MISMATCH` (the §8 code for a derivation that differs from
/// the reference). Returns the previous setting. For fuzzers: a delivery can
/// commit several events, and the store-level oracle only runs between
/// deliveries.
#[doc(hidden)]
pub fn set_thread_commit_checks(enabled: bool) -> bool {
    COMMIT_CHECKS.with(|cell| cell.replace(enabled))
}

/// The conflict derivation mode: a thread override (tests), else
/// `TKFS_CONFLICT_MODE` read once per process. Unset or empty means `append`.
/// An unrecognised value selects `full`, the conservative reference.
pub fn conflict_mode() -> ConflictMode {
    if let Some(mode) = THREAD_MODE.with(Cell::get) {
        return mode;
    }
    *ENV_MODE.get_or_init(|| match std::env::var("TKFS_CONFLICT_MODE") {
        Err(_) => ConflictMode::Append,
        Ok(value) if value.trim().is_empty() => ConflictMode::Append,
        Ok(value) => ConflictMode::parse(&value).unwrap_or_else(|| {
            eprintln!(
                "TKFS_CONFLICT_MODE={value:?} is not append, full or verify; using full derivation"
            );
            ConflictMode::Full
        }),
    })
}

/// Override the mode for commits on the calling thread (`None` restores the
/// process setting). Returns the previous override. For tests and fuzzers,
/// which drive several modes in one process.
#[doc(hidden)]
pub fn set_thread_conflict_mode(mode: Option<ConflictMode>) -> Option<ConflictMode> {
    THREAD_MODE.with(|cell| cell.replace(mode))
}

/// How many derivations ran, by kind. `derive_counters()` is process-wide;
/// `thread_derive_counters()` counts the calling thread only.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeriveCounters {
    /// Rebuilds (startup repair, oracle, evacuation): always full.
    pub rebuilds: u64,
    /// Commits derived in full because the mode is `full`.
    pub full: u64,
    /// Commits derived in append mode.
    pub append: u64,
    /// Append or verify commits whose exactness precondition failed, so they
    /// were derived in full.
    pub fallbacks: u64,
    /// Verify-mode commits whose append and full results matched.
    pub verified: u64,
    /// Append derivations that projected cuts from the cones' own registers
    /// (`projection_cannot_fail`) rather than from the whole cut.
    pub restricted: u64,
    /// Time spent deriving conflict records for commits (all modes; not
    /// rebuilds, not the commit checks), in microseconds.
    pub commit_micros: u64,
    /// Commits checked against the reference (`set_thread_commit_checks`).
    pub checked: u64,
}
impl DeriveCounters {
    const ZERO: Self = Self {
        rebuilds: 0,
        full: 0,
        append: 0,
        fallbacks: 0,
        verified: 0,
        restricted: 0,
        commit_micros: 0,
        checked: 0,
    };
}

#[derive(Clone, Copy)]
enum Count {
    Rebuild,
    Full,
    Append,
    Fallback,
    Verified,
    Restricted,
    CommitMicros,
    Checked,
}
static COUNTERS: [AtomicU64; 8] = [const { AtomicU64::new(0) }; 8];

fn count(kind: Count) {
    add(kind, 1);
}

fn add(kind: Count, amount: u64) {
    COUNTERS[kind as usize].fetch_add(amount, Ordering::Relaxed);
    THREAD_COUNTERS.with(|cell| {
        let mut c = cell.get();
        match kind {
            Count::Rebuild => c.rebuilds += amount,
            Count::Full => c.full += amount,
            Count::Append => c.append += amount,
            Count::Fallback => c.fallbacks += amount,
            Count::Verified => c.verified += amount,
            Count::Restricted => c.restricted += amount,
            Count::CommitMicros => c.commit_micros += amount,
            Count::Checked => c.checked += amount,
        }
        cell.set(c);
    });
}

fn fallback(reason: &'static str) {
    count(Count::Fallback);
    LAST_FALLBACK.with(|cell| cell.set(Some(reason)));
    FALLBACK_REASONS.with(|cell| *cell.borrow_mut().entry(reason).or_default() += 1);
}

/// Process-wide derivation counters (telemetry).
pub fn derive_counters() -> DeriveCounters {
    let get = |kind: Count| COUNTERS[kind as usize].load(Ordering::Relaxed);
    DeriveCounters {
        rebuilds: get(Count::Rebuild),
        full: get(Count::Full),
        append: get(Count::Append),
        fallbacks: get(Count::Fallback),
        verified: get(Count::Verified),
        restricted: get(Count::Restricted),
        commit_micros: get(Count::CommitMicros),
        checked: get(Count::Checked),
    }
}

/// Derivation counters of the calling thread.
#[doc(hidden)]
pub fn thread_derive_counters() -> DeriveCounters {
    THREAD_COUNTERS.with(Cell::get)
}

/// Why the calling thread's most recent append attempt derived in full.
#[doc(hidden)]
pub fn thread_last_fallback() -> Option<&'static str> {
    LAST_FALLBACK.with(Cell::get)
}

/// How often each precondition sent the calling thread's commits to full mode.
#[doc(hidden)]
pub fn thread_fallback_reasons() -> BTreeMap<&'static str, u64> {
    FALLBACK_REASONS.with(|cell| cell.borrow().clone())
}

/// Rewrites `branch`'s stored projection as `p` (`current_state`, with its
/// `current_parts` folded in) and derives and writes its conflict records:
/// every source in full for a rebuild (`new_event == None`), else the records
/// of the newly accepted event in the configured mode. `derive_conflicts`
/// plus `write_projection`; only the stored rows the new event reviews are
/// read first.
pub(super) fn persist_projection(
    tx: &Connection,
    branch: &str,
    p: &Projection,
    events: &[Event],
    new_event: Option<&str>,
) -> Result<()> {
    tx.execute("DELETE FROM current_parts WHERE branch=?", [branch])?;
    tx.execute("INSERT INTO current_state VALUES(?,?) ON CONFLICT(branch) DO UPDATE SET payload=excluded.payload",params![branch,serde_json::to_string(p)?])?;
    let reviewed = reviewed_rows(tx, branch, events, new_event)?;
    let out = derive_conflicts(&DeriveInput {
        branch,
        projection: p,
        events,
        new_event,
        reviewed: &reviewed,
    })?;
    write_projection(tx, branch, &out)
}

/// Everything a derivation reads (WP2.4, REVAMP_PLAN §14 WP2.4 task 1).
/// Derivation itself touches no table, so it runs off-lock on any handle:
/// the only stored rows it needs, those the new event reviews, are read
/// before (`read_reviewed`).
#[derive(Debug, Clone, Copy)]
pub struct DeriveInput<'a> {
    pub branch: &'a str,
    /// The projection of `events` (with the new event).
    pub projection: &'a Projection,
    /// The branch's events, the new one included. The historical commit
    /// path passes them in `id` order with the new event last; the order
    /// decides only which of two records with one id is kept, so the same
    /// order gives the same rows.
    pub events: &'a [Event],
    /// The newly accepted event (a commit), or `None` (a rebuild: every
    /// source in full, whatever the mode).
    pub new_event: Option<&'a str>,
    /// The stored conflict rows (payloads) that the new event reviews, in
    /// its `reviewed` order; missing ones are skipped. Empty for a rebuild.
    pub reviewed: &'a [Conflict],
}

/// A derived record set ready to write: rows deduplicated by id (the last
/// record of an id wins), the receipts of the validated reviews in order
/// (`(review event, reviewed record)`), and whether full mode's DELETE of
/// the namespace rows it no longer derives runs first.
#[derive(Debug, Clone, PartialEq)]
struct Rows {
    rows: Vec<Conflict>,
    receipts: Vec<(String, String)>,
    delete_stale: bool,
}

#[derive(Debug, Clone, PartialEq)]
enum Derived {
    /// One record set (append, full, a fallback or a rebuild).
    Rows(Rows),
    /// Verify mode: both results, compared at write time in savepoints;
    /// the full one is kept.
    Verify { append: Rows, full: Rows },
}

/// The commit check of `set_thread_commit_checks`: after the commit's own
/// rows are written, the branch's rows must equal a derivation from no rows
/// (the oracle's rebuild), computed off-lock with the derivation.
#[derive(Debug, Clone, PartialEq)]
struct CommitCheck {
    mode: &'static str,
    /// A derivation from no rows, or its error text.
    from_empty: std::result::Result<Rows, String>,
}

/// The output of `derive_conflicts`: what `write_projection` writes. It is
/// opaque; only `derive_conflicts` makes one.
#[derive(Debug, Clone, PartialEq)]
pub struct DeriveOutput {
    new_event: Option<String>,
    derived: Derived,
    check: Option<CommitCheck>,
}

impl DeriveOutput {
    /// The records this output writes (the kept ones in verify mode),
    /// deduplicated, in id order.
    pub fn conflicts(&self) -> &[Conflict] {
        match &self.derived {
            Derived::Rows(rows) | Derived::Verify { full: rows, .. } => &rows.rows,
        }
    }
    /// Whether the write runs full mode's DELETE of namespace rows that are
    /// no longer derived (full mode, a fallback, a rebuild).
    pub fn deletes_stale_namespace_rows(&self) -> bool {
        match &self.derived {
            Derived::Rows(rows) | Derived::Verify { full: rows, .. } => rows.delete_stale,
        }
    }
}

/// The pure half of `persist_projection` (WP2.4): the conflict records of a
/// commit of `input.new_event` in the configured mode (`conflict_mode`), or
/// of a rebuild, with the reviews validated. Reads no table and writes
/// nothing, so a slow commit can derive off-lock; `write_projection` writes
/// the result. A refusal (`NAMESPACE_HISTORY_LIMIT`, `INVALID_CONFLICT_REVIEW`,
/// a projection failure of a cut, `CONFLICT_MODE_MISMATCH` in verify mode or
/// under the commit checks) is an error here, exactly where the historical
/// `persist_projection` raised it. Counts the derivation (`derive_counters`).
pub fn derive_conflicts(input: &DeriveInput) -> Result<DeriveOutput> {
    let Some(new_id) = input.new_event else {
        count(Count::Rebuild);
        return Ok(DeriveOutput {
            new_event: None,
            derived: Derived::Rows(full_rows(input, None)?),
            check: None,
        });
    };
    // The derivation's own time (not the checks').
    let timed = || {
        let started = std::time::Instant::now();
        let result = derive_in_mode(input, new_id);
        add(Count::CommitMicros, started.elapsed().as_micros() as u64);
        result
    };
    if !COMMIT_CHECKS.with(Cell::get) {
        return Ok(DeriveOutput {
            new_event: Some(new_id.to_owned()),
            derived: timed()?,
            check: None,
        });
    }
    count(Count::Checked);
    let mode = conflict_mode().as_str();
    let derived = match timed() {
        Ok(derived) => derived,
        Err(refused) => {
            // A refusal must be full mode's, with the same error.
            return match full_rows(input, Some(new_id)) {
                Ok(_) => bail!(
                    "CONFLICT_MODE_MISMATCH: {mode} refused {new_id} ({refused:#}), full accepted it"
                ),
                Err(full) if format!("{full:#}") != format!("{refused:#}") => bail!(
                    "CONFLICT_MODE_MISMATCH: {mode} refused {new_id} ({refused:#}), full refused it with {full:#}"
                ),
                Err(_) => Err(refused),
            };
        }
    };
    // The rebuild from no rows reads no stored review either.
    let from_empty = full_rows(
        &DeriveInput {
            new_event: None,
            reviewed: &[],
            ..*input
        },
        None,
    )
    .map_err(|error| format!("{error:#}"));
    Ok(DeriveOutput {
        new_event: Some(new_id.to_owned()),
        derived,
        check: Some(CommitCheck { mode, from_empty }),
    })
}

/// The cheap half of `persist_projection` (WP2.4): writes `out` to
/// `branch`'s conflict rows inside the caller's transaction. Full mode's
/// DELETE of namespace rows no longer derived (when `out` asks for it), the
/// upsert, then the review receipts (`resolved_by`). In verify mode both
/// results are written in savepoints and compared (`CONFLICT_MODE_MISMATCH`
/// on any difference; the full one is kept), and under the commit checks
/// the rows are compared with a derivation from no rows. The stored
/// projection itself (`current_state`, `current_parts`) is the caller's.
pub fn write_projection(tx: &Connection, branch: &str, out: &DeriveOutput) -> Result<()> {
    let new_id = out.new_event.as_deref().unwrap_or_default();
    match &out.derived {
        Derived::Rows(rows) => write_rows(tx, branch, rows)?,
        Derived::Verify { append, full } => {
            let append = in_savepoint(tx, "tkfs_derive_append", false, || {
                write_rows(tx, branch, append)?;
                capture_conflicts(tx, branch)
            });
            let full = in_savepoint(tx, "tkfs_derive_full", true, || {
                write_rows(tx, branch, full)?;
                capture_conflicts(tx, branch)
            });
            match (append, full) {
                (Ok(append), Ok(full)) => {
                    let differences = row_differences(&append, &full, ["append", "full"]);
                    ensure!(
                        differences.is_empty(),
                        "CONFLICT_MODE_MISMATCH: append and full derivation differ for {new_id}: {}",
                        differences.join("; ")
                    );
                    count(Count::Verified);
                }
                (Err(_), Err(full)) => return Err(full),
                (Ok(_), Err(full)) => bail!(
                    "CONFLICT_MODE_MISMATCH: append accepted {new_id}, full refused it: {full:#}"
                ),
                (Err(append), Ok(_)) => bail!(
                    "CONFLICT_MODE_MISMATCH: append refused {new_id} ({append:#}), full accepted it"
                ),
            }
        }
    }
    let Some(check) = &out.check else {
        return Ok(());
    };
    let mode = check.mode;
    let (stored, rebuilt) = in_savepoint(tx, "tkfs_derive_check", false, || {
        let stored = capture_conflicts(tx, branch)?;
        tx.execute("DELETE FROM conflicts WHERE branch=?", [branch])?;
        let rows = check
            .from_empty
            .as_ref()
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        write_rows(tx, branch, rows)?;
        Ok((stored, capture_conflicts(tx, branch)?))
    })
    .map_err(|error| {
        anyhow::anyhow!(
            "CONFLICT_MODE_MISMATCH: {mode} committed {new_id}, a derivation from no rows fails: {error:#}"
        )
    })?;
    let differences = row_differences(&stored, &rebuilt, ["stored", "rebuilt"]);
    ensure!(
        differences.is_empty(),
        "CONFLICT_MODE_MISMATCH: {mode} commit of {new_id} differs from a derivation from no rows: {}",
        differences.join("; ")
    );
    Ok(())
}

/// A commit's records, in the configured mode.
fn derive_in_mode(input: &DeriveInput, new_id: &str) -> Result<Derived> {
    match conflict_mode() {
        ConflictMode::Full => {
            count(Count::Full);
            Ok(Derived::Rows(full_rows(input, Some(new_id))?))
        }
        ConflictMode::Append => match derive_append_with(input, new_id)? {
            Plan::Derived(conflicts) => {
                count(Count::Append);
                Ok(Derived::Rows(finish(
                    input,
                    Some(new_id),
                    conflicts,
                    false,
                )?))
            }
            Plan::Fallback(reason) => {
                fallback(reason);
                Ok(Derived::Rows(full_rows(input, Some(new_id))?))
            }
        },
        ConflictMode::Verify => derive_verified(input, new_id),
    }
}

/// Full derivation: every source over the whole branch history, written
/// after the DELETE of namespace rows that are no longer derived.
fn full_rows(input: &DeriveInput, new_event: Option<&str>) -> Result<Rows> {
    let conflicts = derive_full_with(input, new_event)?;
    finish(input, new_event, conflicts, true)
}

/// Deduplicate `conflicts` (the last record of an id wins) and validate the
/// new event's reviews (every event's, on rebuild) against them.
fn finish(
    input: &DeriveInput,
    new_event: Option<&str>,
    conflicts: Vec<Conflict>,
    delete_stale: bool,
) -> Result<Rows> {
    let rows: Vec<_> = conflicts
        .into_iter()
        .map(|c| (c.id.clone(), c))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect();
    let receipts = review_receipts(input.projection, input.events, new_event, &rows)?;
    Ok(Rows {
        rows,
        receipts,
        delete_stale,
    })
}

/// The stored rows `event` reviews (payloads), in its `reviewed` order;
/// missing ones are skipped. What `DeriveInput::reviewed` holds for a
/// commit of `event`.
pub(super) fn read_reviewed(db: &Connection, branch: &str, event: &Event) -> Result<Vec<Conflict>> {
    let mut rows = vec![];
    for reviewed in &event.reviewed {
        let payload: Option<String> = db
            .query_row(
                "SELECT payload FROM conflicts WHERE id=? AND branch=?",
                params![reviewed, branch],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(payload) = payload {
            rows.push(serde_json::from_str(&payload)?);
        }
    }
    Ok(rows)
}

/// `derive_full_with` reading the stored rows the new event reviews from
/// `tx` (the historical signature, for the unit tests).
#[cfg(test)]
fn derive_full(
    tx: &Connection,
    branch: &str,
    p: &Projection,
    events: &[Event],
    new_event: Option<&str>,
) -> Result<Vec<Conflict>> {
    let reviewed = reviewed_rows(tx, branch, events, new_event)?;
    derive_full_with(
        &DeriveInput {
            branch,
            projection: p,
            events,
            new_event,
            reviewed: &reviewed,
        },
        new_event,
    )
}

/// Every full-mode source, in the historical order (the deduplication keeps
/// the last record of each id). The stored rows the new event reviews come
/// from `input.reviewed` (none for a rebuild).
fn derive_full_with(input: &DeriveInput, new_event: Option<&str>) -> Result<Vec<Conflict>> {
    let DeriveInput {
        branch,
        projection: p,
        events,
        ..
    } = *input;
    let mut conflicts = p.conflicts.clone();
    if new_event.is_some() {
        conflicts.extend(input.reviewed.iter().cloned());
    }
    // Namespace collisions also have a deterministic historical basis. A later
    // rename arriving first must not erase evidence of concurrent same-name
    // placement. Sequential temp replacement is ordered by event context.
    let activated: Vec<_> = events
        .iter()
        .filter(|e| !p.pending.contains(&e.id))
        .collect();
    let mut locations = vec![];
    for e in &activated {
        for c in &e.changes {
            if c.field == "location" {
                locations.push((*e, c));
            }
        }
    }
    // Each change's slot name key is computed once, in the order the pairwise
    // comparison first needs it (so a failure surfaces at the same pair).
    let mut keys: Vec<Option<String>> = vec![None; locations.len()];
    for (i, &(a, ac)) in locations.iter().enumerate() {
        crate::startup::progress("rebuilding namespace conflicts");
        for (j, &(b, bc)) in locations.iter().enumerate().skip(i + 1) {
            if ac.entity == bc.entity || ac.value["parent"] != bc.value["parent"] {
                continue;
            }
            for k in [i, j] {
                if keys[k].is_none() {
                    keys[k] = Some(name_key(locations[k].1.value["name"].as_str().unwrap())?);
                }
            }
            if keys[i] != keys[j] {
                continue;
            }
            if let Some(c) = concurrent_placement(branch, events, (a, ac), (b, bc)) {
                conflicts.push(c);
            }
        }
    }
    // Explicit reviews carry their observed cut. Recover namespace records from
    // it even when resolution was delivered before its prerequisites.
    for e in &activated {
        if !e.reviewed.is_empty() {
            conflicts.extend(reviewed_cut(branch, events, e)?);
        }
    }
    // Every namespace causal cut is represented by an antichain of maximal
    // namespace-changing events. Include arbitrary-width concurrency, not just
    // pairs; content-only events add no new namespace projection. The explicit
    // budget refuses a commit before acknowledgement rather than losing history.
    conflicts.extend(NsContext::new(branch, events).full()?);
    conflicts.extend(register_pairs(branch, p, events, new_event));
    Ok(conflicts)
}

/// The stored rows the new event reviews: they stay in the derived list, so
/// their `resolved_by` survives and the review can be validated.
fn reviewed_rows(
    tx: &Connection,
    branch: &str,
    events: &[Event],
    new_event: Option<&str>,
) -> Result<Vec<Conflict>> {
    match events.iter().find(|e| Some(e.id.as_str()) == new_event) {
        Some(event) => read_reviewed(tx, branch, event),
        None => Ok(vec![]),
    }
}

/// The record of two placements of different entities into one slot, unless
/// one event is an ancestor of the other.
fn concurrent_placement(
    branch: &str,
    events: &[Event],
    (a, ac): (&Event, &Change),
    (b, bc): (&Event, &Change),
) -> Option<Conflict> {
    if event_ancestors(a, events).contains(&b.id) || event_ancestors(b, events).contains(&a.id) {
        return None;
    }
    let mut entities = [ac.entity.clone(), bc.entity.clone()];
    entities.sort();
    let alternatives = [(a, ac), (b, bc)]
        .into_iter()
        .map(|(e, c)| Version {
            revision: e.id.clone(),
            device: e.device.clone(),
            created_ms: e.created_ms,
            parents: c.parents.clone(),
            value: c.value.clone(),
        })
        .collect();
    Some(make_conflict(
        branch,
        &entities.join(","),
        "namespace",
        "same name / file-directory collision",
        alternatives,
        vec![],
    ))
}

/// Every record of the projection of a review's observed (ancestor) cut.
fn reviewed_cut(branch: &str, events: &[Event], e: &Event) -> Result<Vec<Conflict>> {
    let ancestors = event_ancestors(e, events);
    let cut: Vec<&Event> = events
        .iter()
        .filter(|prior| ancestors.contains(&prior.id))
        .collect();
    Ok(project_refs(branch, &cut)?.conflicts)
}

/// Historical register pairs: on append, the new event's version against every
/// other version of each register it changes; on rebuild, every pair.
fn register_pairs(
    branch: &str,
    p: &Projection,
    events: &[Event],
    new_event: Option<&str>,
) -> Vec<Conflict> {
    // On append only the new event's registers can yield pairs (any other
    // register has no candidate version and is skipped below).
    let wanted: Option<BTreeSet<String>> = new_event.map(|id| {
        events
            .iter()
            .filter(|e| e.id == id && !p.pending.contains(&e.id))
            .flat_map(|e| e.changes.iter().map(|c| register(&c.entity, &c.field)))
            .collect()
    });
    // Derive historical concurrent pairs from the full activated DAG too, so a
    // fast-forward delivered before a competitor cannot hide the earlier pair.
    let mut history: BTreeMap<String, Vec<Version>> = BTreeMap::new();
    for e in events.iter().filter(|e| !p.pending.contains(&e.id)) {
        for c in &e.changes {
            let key = register(&c.entity, &c.field);
            if wanted.as_ref().is_some_and(|wanted| !wanted.contains(&key)) {
                continue;
            }
            history.entry(key).or_default().push(Version {
                revision: e.id.clone(),
                device: e.device.clone(),
                created_ms: e.created_ms,
                parents: c.parents.clone(),
                value: c.value.clone(),
            });
        }
    }
    let mut conflicts = vec![];
    for (key, versions) in &history {
        crate::startup::progress("rebuilding register conflicts");
        let candidate = new_event.and_then(|id| versions.iter().position(|v| v.revision == id));
        if new_event.is_some() && candidate.is_none() {
            continue;
        }
        // The overwhelmingly common single-parent chain has no historical
        // concurrency. Detect it in O(revisions), without enumerating pairs.
        if linear_versions(versions) {
            continue;
        }
        let reach = VersionReach::new(versions);
        let (entity, field) = key.rsplit_once('/').unwrap();
        // Appending a validated event cannot change old/old register ancestry.
        // Keep their durable records and derive only new/old pairs on append.
        // Startup/repair still derives the entire historical set from the DAG.
        let pairs: Box<dyn Iterator<Item = (usize, usize)>> = if let Some(i) = candidate {
            Box::new(
                (0..versions.len())
                    .filter(move |&j| j != i)
                    .map(move |j| (i, j)),
            )
        } else {
            Box::new(
                (0..versions.len()).flat_map(|i| ((i + 1)..versions.len()).map(move |j| (i, j))),
            )
        };
        for (i, j) in pairs {
            crate::startup::progress("rebuilding register conflicts");
            let a = &versions[i];
            let b = &versions[j];
            if a.value != b.value && !reach.has(i, j) && !reach.has(j, i) {
                let mut bases: Vec<_> = versions
                    .iter()
                    .enumerate()
                    .filter(|(k, _)| reach.has(i, *k) && reach.has(j, *k))
                    .map(|(_, v)| v)
                    .cloned()
                    .collect();
                bases.sort_by(|a, b| a.revision.cmp(&b.revision));
                conflicts.push(make_conflict(
                    branch,
                    entity,
                    field,
                    "concurrent revisions",
                    vec![a.clone(), b.clone()],
                    bases,
                ));
            }
        }
    }
    conflicts
}

/// Writes `rows`: full mode's DELETE of the namespace rows they lack (when
/// asked), the upsert, then the receipts of the validated reviews.
fn write_rows(tx: &Connection, branch: &str, rows: &Rows) -> Result<()> {
    if rows.delete_stale {
        let namespace_ids: BTreeSet<_> = rows
            .rows
            .iter()
            .filter(|c| c.field == "namespace")
            .map(|c| c.id.clone())
            .collect();
        let old: Vec<(String, String)> = tx
            .prepare("SELECT id,payload FROM conflicts WHERE branch=? AND json_extract(payload,'$.field')='namespace'")?
            .query_map([branch], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<std::result::Result<_, _>>()?;
        for (id, payload) in old {
            let c: Conflict = serde_json::from_str(&payload)?;
            if c.field == "namespace" && !namespace_ids.contains(&id) {
                tx.execute("DELETE FROM conflicts WHERE id=?", [id])?;
            }
        }
    }
    {
        let mut insert=tx.prepare_cached("INSERT INTO conflicts(id,branch,payload) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload WHERE conflicts.payload<>excluded.payload")?;
        for c in &rows.rows {
            insert.execute(params![c.id, branch, serde_json::to_string(c)?])?;
        }
    }
    for (event, reviewed) in &rows.receipts {
        // Concurrent review events converge on a stable receipt as well.
        tx.execute("UPDATE conflicts SET resolved_by=CASE WHEN resolved_by IS NULL OR resolved_by < ? THEN ? ELSE resolved_by END WHERE id=? AND branch=?",params![event,event,reviewed,branch])?;
    }
    Ok(())
}

/// Validate the new event's reviews (every event's, on rebuild) against the
/// deduplicated `rows`: the receipts to record, in order, or
/// `INVALID_CONFLICT_REVIEW`.
fn review_receipts(
    p: &Projection,
    events: &[Event],
    new_event: Option<&str>,
    rows: &[Conflict],
) -> Result<Vec<(String, String)>> {
    let mut receipts = vec![];
    for e in events {
        if !p.pending.contains(&e.id) && new_event.is_none_or(|id| id == e.id) {
            for reviewed in &e.reviewed {
                let c = rows
                    .iter()
                    .find(|c| &c.id == reviewed)
                    .context("INVALID_CONFLICT_REVIEW")?;
                let valid_review = if c.field == "namespace" {
                    e.changes.iter().any(|change| {
                        change.field == "location"
                            && c.entity.split(',').any(|entity| entity == change.entity)
                            && c.alternatives
                                .iter()
                                .any(|v| change.parents.contains(&v.revision))
                            && c.alternatives
                                .iter()
                                .filter(|v| {
                                    events
                                        .iter()
                                        .find(|prior| prior.id == v.revision)
                                        .is_some_and(|prior| {
                                            prior.changes.iter().any(|ch| {
                                                ch.entity == change.entity
                                                    && ch.field == "location"
                                                    && ch.value == v.value
                                            })
                                        })
                                })
                                .all(|v| change.parents.contains(&v.revision))
                    })
                } else {
                    e.changes.iter().any(|change| {
                        change.entity == c.entity
                            && change.field == c.field
                            && c.alternatives
                                .iter()
                                .all(|v| change.parents.contains(&v.revision))
                    })
                };
                ensure!(valid_review, "INVALID_CONFLICT_REVIEW");
                receipts.push((e.id.clone(), reviewed.clone()));
            }
        }
    }
    Ok(receipts)
}

/// The outcome of the append-mode precondition.
enum Plan {
    /// The records append mode derives (written without the namespace DELETE).
    Derived(Vec<Conflict>),
    /// The precondition does not hold: derive in full.
    Fallback(&'static str),
}

/// Bound on the process-wide slot-key memo (`slot_key`), in estimated bytes.
const SLOT_KEY_MEMO_BYTES: usize = 16 * 1024 * 1024;

/// A slot key, or the text of the error `name_key` raises for the name.
type SlotKey = std::result::Result<Arc<str>, Arc<str>>;

/// A memo of slot keys bounded by an estimate of the bytes it holds, in two
/// generations of at most half the budget each: a hit in the older one moves
/// the entry to the current one, and a current generation that would outgrow
/// its half becomes the older one (the previous older one is dropped). Names
/// used on every commit stay; a branch with more distinct names than the
/// budget holds recomputes some keys instead of growing the memo.
struct SlotKeys {
    current: HashMap<String, SlotKey>,
    older: HashMap<String, SlotKey>,
    current_bytes: usize,
    older_bytes: usize,
    budget: usize,
}
impl SlotKeys {
    fn new(budget: usize) -> Self {
        Self {
            current: HashMap::new(),
            older: HashMap::new(),
            current_bytes: 0,
            older_bytes: 0,
            budget,
        }
    }
    /// The heap an entry holds: the name, the key or error text, the
    /// `String` and `Arc` headers and the table slot.
    fn entry_bytes(name: &str, key: &SlotKey) -> usize {
        let text = match key {
            Ok(key) | Err(key) => key.len(),
        };
        name.len() + text + 96
    }
    fn get(&mut self, name: &str) -> Option<SlotKey> {
        if let Some(key) = self.current.get(name) {
            return Some(key.clone());
        }
        let key = self.older.remove(name)?;
        self.older_bytes -= Self::entry_bytes(name, &key);
        self.insert(name, key.clone());
        Some(key)
    }
    fn insert(&mut self, name: &str, key: SlotKey) {
        let bytes = Self::entry_bytes(name, &key);
        if bytes > self.budget / 2 {
            return;
        }
        if self.current_bytes + bytes > self.budget / 2 {
            self.older = std::mem::take(&mut self.current);
            self.older_bytes = self.current_bytes;
            self.current_bytes = 0;
        }
        // Another thread may have inserted it since this one looked.
        if let Some(previous) = self.current.insert(name.to_owned(), key) {
            self.current_bytes -= Self::entry_bytes(name, &previous);
        }
        self.current_bytes += bytes;
    }
    /// Estimated bytes held (at most the budget).
    #[cfg(test)]
    fn bytes(&self) -> usize {
        self.current_bytes + self.older_bytes
    }
}

/// `name_key`, memoized process-wide (bounded, see `SlotKeys`): a pure
/// function of the name, needed for every location change of the branch on
/// every append. Errors keep their text.
fn slot_key(name: &str) -> Result<Arc<str>> {
    static KEYS: OnceLock<Mutex<SlotKeys>> = OnceLock::new();
    let keys = KEYS.get_or_init(|| Mutex::new(SlotKeys::new(SLOT_KEY_MEMO_BYTES)));
    let found = keys
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(name);
    let key = match found {
        Some(key) => key,
        None => {
            let key: SlotKey = match name_key(name) {
                Ok(key) => Ok(Arc::from(key)),
                Err(error) => Err(Arc::from(format!("{error:#}"))),
            };
            keys.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(name, key.clone());
            key
        }
    };
    key.map_err(|error| anyhow::anyhow!("{error}"))
}

/// One location change of the branch with its slot key (or the error its
/// name gives).
struct Location<'a> {
    event: usize,
    change: &'a Change,
    parent: &'a str,
    key: std::result::Result<Arc<str>, String>,
}

/// Every location change of the branch, in event then change order (the
/// order in which full mode first evaluates their names).
struct Locations<'a> {
    rows: Vec<Location<'a>>,
}
impl<'a> Locations<'a> {
    fn new(events: &'a [Event]) -> Self {
        let mut rows = vec![];
        for (event, e) in events.iter().enumerate() {
            for change in e.changes.iter().filter(|c| c.field == "location") {
                rows.push(Location {
                    event,
                    change,
                    parent: change.value["parent"].as_str().unwrap(),
                    key: slot_key(change.value["name"].as_str().unwrap())
                        .map_err(|error| format!("{error:#}")),
                });
            }
        }
        Self { rows }
    }
    /// Row `k`'s key, or its name's error (as `name_key` raises it).
    fn key(&self, k: usize) -> Result<&str> {
        match &self.rows[k].key {
            Ok(key) => Ok(key),
            Err(error) => Err(anyhow::anyhow!("{error}")),
        }
    }
    /// The first name error, where full mode's cone analysis raises it.
    fn first_error(&self) -> Result<()> {
        for k in 0..self.rows.len() {
            self.key(k)?;
        }
        Ok(())
    }
}

/// Whether `project` succeeds on every downward-closed cut of `events` (it
/// succeeded on the whole branch): every name is valid, every kind is a
/// string and every presence a boolean, every metadata value parses, and
/// every presence version has a placement of its entity in its own event or
/// causally before it (through its presence parents). Then no visible entry
/// of any cut lacks a valid name, so a cone's records can be projected from
/// its own registers (`NsContext::namespace_records`).
fn projection_cannot_fail(
    events: &[Event],
    locations: &Locations,
    alive: &BTreeMap<&str, Vec<(usize, &Change)>>,
) -> bool {
    if locations.rows.iter().any(|row| row.key.is_err()) {
        return false;
    }
    for c in events.iter().flat_map(|e| &e.changes) {
        let valid = match c.field.as_str() {
            "kind" => c.value.is_string(),
            "alive" => c.value.is_boolean(),
            "basic" => BasicInfo::deserialize(&c.value).is_ok(),
            _ => true,
        };
        if !valid {
            return false;
        }
    }
    for (&entity, versions) in alive {
        let index: BTreeMap<&str, usize> = versions
            .iter()
            .enumerate()
            .map(|(k, (i, _))| (events[*i].id.as_str(), k))
            .collect();
        // placed[k]: version k's event places the entity, or a presence
        // parent of it is placed (resolved in topological order).
        let parents: Vec<Vec<usize>> = versions
            .iter()
            .map(|(_, c)| {
                c.parents
                    .iter()
                    .filter_map(|p| index.get(p.as_str()).copied())
                    .collect()
            })
            .collect();
        let children = children_of(&parents);
        let mut degree: Vec<usize> = parents.iter().map(Vec::len).collect();
        let mut ready: Vec<usize> = (0..versions.len()).filter(|&k| degree[k] == 0).collect();
        let mut placed = vec![false; versions.len()];
        let mut seen = 0;
        while let Some(k) = ready.pop() {
            seen += 1;
            placed[k] = events[versions[k].0]
                .changes
                .iter()
                .any(|c| c.entity == entity && c.field == "location")
                || parents[k].iter().any(|&p| placed[p]);
            for &child in &children[k] {
                degree[child] -= 1;
                if degree[child] == 0 {
                    ready.push(child);
                }
            }
        }
        if seen < versions.len() || placed.contains(&false) {
            return false;
        }
    }
    true
}

/// Per edge of the namespace dependency graph: how many location changes of
/// the branch contribute it, and whether the new event is one of them.
#[derive(Clone, Copy, Default)]
struct Contributions {
    all: usize,
    new: usize,
}
impl Contributions {
    fn add(&mut self, new: bool) {
        self.all += 1;
        self.new += usize::from(new);
    }
    /// Present before the new event (`before`) or with it.
    fn present(self, before: bool) -> bool {
        if before {
            self.all > self.new
        } else {
            self.all > 0
        }
    }
}

/// `NamespaceCones` of the whole branch, built once, with per-edge
/// contributions so the cones without the new event come from the same
/// graph (the new event adds at most one placement).
struct ConeGraph<'a> {
    nodes: BTreeMap<&'a str, Contributions>,
    parents: BTreeMap<&'a str, BTreeMap<&'a str, Contributions>>,
    children: BTreeMap<&'a str, BTreeSet<&'a str>>,
    entity_slots: BTreeMap<&'a str, BTreeSet<(&'a str, &'a str)>>,
    slots: BTreeMap<(&'a str, &'a str), BTreeMap<&'a str, Contributions>>,
}
impl<'a> ConeGraph<'a> {
    fn new(events: &'a [Event], locations: &'a Locations<'a>, new: usize) -> Self {
        let mut graph = Self {
            nodes: BTreeMap::new(),
            parents: BTreeMap::new(),
            children: BTreeMap::new(),
            entity_slots: BTreeMap::new(),
            slots: BTreeMap::new(),
        };
        for (i, e) in events.iter().enumerate() {
            for c in &e.changes {
                graph.nodes.entry(&c.entity).or_default().add(i == new);
            }
        }
        for row in &locations.rows {
            let is_new = row.event == new;
            let entity = row.change.entity.as_str();
            if row.parent != ROOT {
                graph
                    .parents
                    .entry(entity)
                    .or_default()
                    .entry(row.parent)
                    .or_default()
                    .add(is_new);
                graph.children.entry(row.parent).or_default().insert(entity);
                graph.nodes.entry(row.parent).or_default().add(is_new);
            }
            // Callers check `Locations::first_error` first.
            let key: &str = row.key.as_deref().unwrap_or_default();
            let slot = (row.parent, key);
            graph.entity_slots.entry(entity).or_default().insert(slot);
            graph
                .slots
                .entry(slot)
                .or_default()
                .entry(entity)
                .or_default()
                .add(is_new);
        }
        graph
    }
    /// Whether `entity` is a node of the history without the new event.
    fn existed(&self, entity: &str) -> bool {
        self.nodes.get(entity).is_some_and(|c| c.present(true))
    }
    /// The cone of `entity` (`NamespaceCones::cone`), with or without the new
    /// event's contributions.
    fn cone(&self, entity: &'a str, before: bool) -> BTreeSet<&'a str> {
        crate::startup::progress("analyzing namespace dependencies");
        let mut members = BTreeSet::new();
        let mut visited_slots = BTreeSet::new();
        let mut pending = vec![entity];
        while let Some(next) = pending.pop() {
            if !members.insert(next) {
                continue;
            }
            for (&parent, c) in self.parents.get(next).into_iter().flatten() {
                if c.present(before) {
                    pending.push(parent);
                }
            }
            for &slot in self.entity_slots.get(next).into_iter().flatten() {
                let occupants = &self.slots[&slot];
                if occupants[next].present(before) && visited_slots.insert(slot) {
                    pending.extend(
                        occupants
                            .iter()
                            .filter(|(_, c)| c.present(before))
                            .map(|(&m, _)| m),
                    );
                }
            }
        }
        members
    }
    /// The entities whose cone (with the new event) contains one of
    /// `targets`: reverse reachability over parent and slot edges.
    fn reaching(&self, targets: &BTreeSet<&str>) -> BTreeSet<&'a str> {
        let mut found = BTreeSet::new();
        let mut pending: Vec<&'a str> = targets
            .iter()
            .filter_map(|t| self.nodes.get_key_value(*t).map(|(&k, _)| k))
            .collect();
        while let Some(next) = pending.pop() {
            if !found.insert(next) {
                continue;
            }
            pending.extend(self.children.get(next).into_iter().flatten().copied());
            for slot in self.entity_slots.get(next).into_iter().flatten() {
                pending.extend(self.slots[slot].keys().copied());
            }
        }
        found
    }
}

/// `derive_append_with` reading the stored rows the new event reviews from
/// `tx` (the historical signature, for the unit tests).
#[cfg(test)]
fn derive_append(
    tx: &Connection,
    branch: &str,
    p: &Projection,
    events: &[Event],
    new_id: &str,
) -> Result<Plan> {
    let reviewed = reviewed_rows(tx, branch, events, Some(new_id))?;
    derive_append_with(
        &DeriveInput {
            branch,
            projection: p,
            events,
            new_event: Some(new_id),
            reviewed: &reviewed,
        },
        new_id,
    )
}

/// Append mode (§5.2): the records the new event adds, or a fallback reason.
/// Errors are refusals, raised exactly where full mode raises them (module
/// docs): nothing here fails on history that a rebuild accepts.
fn derive_append_with(input: &DeriveInput, new_id: &str) -> Result<Plan> {
    let DeriveInput {
        branch,
        projection: p,
        events,
        ..
    } = *input;
    let mut positions = events
        .iter()
        .enumerate()
        .filter(|(_, e)| e.id == new_id)
        .map(|(i, _)| i);
    let (Some(new), None) = (positions.next(), positions.next()) else {
        return Ok(Plan::Fallback("new event not exactly once in the branch"));
    };
    if let Some(reason) = append_guard(p, events, new) {
        return Ok(Plan::Fallback(reason));
    }
    let e = &events[new];
    let locations = Locations::new(events);
    // Same source order as full mode, so a refusal surfaces with the same error.
    let mut conflicts = p.conflicts.clone();
    conflicts.extend(input.reviewed.iter().cloned());
    conflicts.extend(location_pairs_with(branch, events, &locations, new)?);
    if !e.reviewed.is_empty() {
        conflicts.extend(reviewed_cut(branch, events, e)?);
    }
    // Full mode's cone analysis reads every name next.
    locations.first_error()?;
    let ns = NsContext::new(branch, events);
    let plan = match ns.append_plan(new, &locations)? {
        Ok(plan) => plan,
        Err(reason) => return Ok(Plan::Fallback(reason)),
    };
    let restricted =
        !plan.cones.is_empty() && projection_cannot_fail(events, &locations, &ns.alive);
    if restricted {
        count(Count::Restricted);
    }
    conflicts.extend(ns.derive_planned(&plan, restricted)?);
    conflicts.extend(register_pairs(branch, p, events, Some(new_id)));
    // Nothing above can fail on accepted history; a failure here is left to
    // full mode, which raises or derives whatever the reference does.
    match ns.current_records_survive(new, &plan, &conflicts) {
        Ok(true) => Ok(Plan::Derived(conflicts)),
        Ok(false) => Ok(Plan::Fallback(
            "a current namespace record is not derived from history",
        )),
        Err(_) => Ok(Plan::Fallback(
            "the current namespace records failed to project",
        )),
    }
}

/// Structural preconditions of append mode (facts 1 and 3 of the module
/// docs). `None` when append mode may run.
fn append_guard(p: &Projection, events: &[Event], new: usize) -> Option<&'static str> {
    if !p.pending.is_empty() {
        return Some("the branch has pending events");
    }
    let id = &events[new].id;
    let mut multi = vec![];
    for (i, e) in events.iter().enumerate() {
        if i != new
            && (e.dependencies.contains(id) || e.changes.iter().any(|c| c.parents.contains(id)))
        {
            return Some("the new event is a parent of a committed event");
        }
        if e.changes.iter().any(|c| c.entity.contains(',')) {
            return Some("an entity id contains a comma");
        }
        let locations: Vec<&Change> = e.changes.iter().filter(|c| c.field == "location").collect();
        if locations.len() > 1 {
            if i == new {
                return Some("the new event relocates several entities");
            }
            multi.push(locations);
        }
    }
    // Several location changes in one event are a fork or restore root (or
    // crafted). They keep ids unambiguous unless one event places two entities
    // in one slot, or two events both relocate the same two entities.
    for locations in &multi {
        let mut slots = BTreeSet::new();
        for c in locations {
            let (Some(parent), Some(name)) = (c.value["parent"].as_str(), c.value["name"].as_str())
            else {
                return Some("a location change without parent or name");
            };
            let key = if name.is_ascii() {
                name.to_ascii_lowercase()
            } else {
                match name_key(name) {
                    Ok(key) => key,
                    Err(_) => return Some("a location change with an invalid name"),
                }
            };
            if !slots.insert((parent, key)) {
                return Some("one event places two entities in one slot");
            }
        }
    }
    if multi.len() > 1 {
        if multi.len() > 16 {
            return Some("too many events relocate several entities");
        }
        let sets: Vec<BTreeSet<&str>> = multi
            .iter()
            .map(|locations| locations.iter().map(|c| c.entity.as_str()).collect())
            .collect();
        for (i, a) in sets.iter().enumerate() {
            for b in &sets[i + 1..] {
                if a.intersection(b).nth(1).is_some() {
                    return Some("two events relocate the same entities");
                }
            }
        }
    }
    None
}

/// CCR-WP1.6-5: whether committing `events[new]` through the namespace
/// successor can change the competing-cut count of a dependency cone, so
/// that `check_successor_cuts` must count. A successor's event follows the
/// whole branch (`dependencies == frontier`), so it is in no antichain of two
/// or more events and adds no competing cut itself; it is also a descendant
/// of every version of the registers it changes (one head, parented by it),
/// so it changes no presence-witness group of an existing tombstone. A cone
/// can only gain competing cuts when the event
///
/// - places an existing entity under a parent it never had (its cone takes
///   in the parent's), or into a slot it never used that another entity
///   used (it takes in the slot competitors' cones), or
/// - writes the first tombstone of an entity that has `alive = true`
///   versions outside its placements (TRUE presence compression starts, and
///   the witnesses join the relevant set).
///
/// A fresh entity (a create) only joins the cone of its parent or of its
/// slot's past occupants, which already contains that parent: never two
/// cones. Conservative: anything unexpected answers `true`.
pub(super) fn successor_may_add_cuts(events: &[Event], new: usize) -> bool {
    let others = || {
        events
            .iter()
            .enumerate()
            .filter(move |(i, _)| *i != new)
            .map(|(_, e)| e)
    };
    for c in &events[new].changes {
        let x = c.entity.as_str();
        match c.field.as_str() {
            "location" => {
                let (Some(parent), Some(name)) =
                    (c.value["parent"].as_str(), c.value["name"].as_str())
                else {
                    return true;
                };
                let Ok(key) = slot_key(name) else {
                    return true;
                };
                let mut fresh = true;
                let mut new_parent = parent != ROOT;
                let mut new_slot = true;
                let mut occupied = false;
                for prior in others() {
                    for pc in &prior.changes {
                        if pc.entity == x {
                            fresh = false;
                        }
                        if pc.field != "location" || pc.value["parent"].as_str() != Some(parent) {
                            continue;
                        }
                        if pc.entity == x {
                            new_parent = false;
                        }
                        let Some(other) = pc.value["name"].as_str() else {
                            return true;
                        };
                        // ASCII names share a slot exactly when they are equal
                        // ignoring ASCII case (`name_key` is NFC plus lower
                        // case; validity is the same for both spellings).
                        let same_slot = if name.is_ascii() && other.is_ascii() {
                            name.eq_ignore_ascii_case(other)
                        } else {
                            match slot_key(other) {
                                Ok(k) => k == key,
                                Err(_) => return true,
                            }
                        };
                        if same_slot {
                            if pc.entity == x {
                                new_slot = false;
                            } else {
                                occupied = true;
                            }
                        }
                    }
                }
                if !fresh && (new_parent || (new_slot && occupied)) {
                    return true;
                }
            }
            "alive" if c.value == false => {
                let mut tombstone = false;
                let mut witness = false;
                for prior in others() {
                    let places = prior
                        .changes
                        .iter()
                        .any(|pc| pc.entity == x && (pc.field == "location" || pc.field == "kind"));
                    for pc in prior
                        .changes
                        .iter()
                        .filter(|pc| pc.entity == x && pc.field == "alive")
                    {
                        if pc.value == false {
                            tombstone = true;
                        } else if !places {
                            witness = true;
                        }
                    }
                }
                if !tombstone && witness {
                    return true;
                }
            }
            "kind" if others().any(|prior| prior.changes.iter().any(|pc| pc.entity == x)) => {
                return true;
            }
            _ => {}
        }
    }
    false
}

/// Whether every cone `check_successor_cuts` would count is certainly within
/// `MAX_NAMESPACE_CUTS`, by a cheap sound bound: a cone with `r` relevant
/// events has at most `2^r - 1 - r` competing cuts (its antichains of two or
/// more events), which is 4,083 for `r = 12`. The cones that contain an
/// entity the event touches are the cones of the entities that reach one
/// (`ConeGraph::reaching`: the closure over children and slot sharers), and
/// each lies in the closure of those over parents and slot sharers
/// (`NamespaceCones::cone`); every relevant event of such a cone changes a
/// member's kind, location or presence. So at most 12 such events among the
/// members of that closure means no count can exceed the limit. Gives up
/// (`false`) as soon as the closure grows past a small size.
fn successor_cones_are_small<'a>(events: &'a [Event], new: usize) -> bool {
    const SMALL: usize = 12;
    const MEMBERS: usize = 64;
    let rows: Vec<(&'a str, &'a str, &'a str)> = events
        .iter()
        .flat_map(|e| e.changes.iter().filter(|c| c.field == "location"))
        .filter_map(|c| {
            Some((
                c.entity.as_str(),
                c.value["parent"].as_str()?,
                c.value["name"].as_str()?,
            ))
        })
        .collect();
    // Two names share a slot (conservatively `true` for a name that cannot
    // be keyed).
    let same_slot = |a: &str, b: &str| {
        if a.is_ascii() && b.is_ascii() {
            a.eq_ignore_ascii_case(b)
        } else {
            match (slot_key(a), slot_key(b)) {
                (Ok(a), Ok(b)) => a == b,
                _ => true,
            }
        }
    };
    // Close `set` over slot sharers and, `up`, parents (else children).
    let close = |set: &mut BTreeSet<&'a str>, up: bool| -> bool {
        loop {
            if set.len() > MEMBERS {
                return false;
            }
            let slots: Vec<(&str, &str)> = rows
                .iter()
                .filter(|(e, _, _)| set.contains(e))
                .map(|&(_, p, n)| (p, n))
                .collect();
            let mut grew = false;
            for &(entity, parent, name) in &rows {
                if set.contains(entity) {
                    if up && parent != ROOT && set.insert(parent) {
                        grew = true;
                    }
                    continue;
                }
                let joins = (!up && set.contains(parent))
                    || slots
                        .iter()
                        .any(|&(p, n)| p == parent && same_slot(n, name));
                if joins {
                    set.insert(entity);
                    grew = true;
                }
            }
            if !grew {
                return true;
            }
        }
    };
    let mut members: BTreeSet<&'a str> = events[new]
        .changes
        .iter()
        .filter(|c| ["kind", "location", "alive"].contains(&c.field.as_str()))
        .map(|c| c.entity.as_str())
        .collect();
    if !close(&mut members, false) || !close(&mut members, true) {
        return false;
    }
    let candidates = events
        .iter()
        .filter(|e| {
            e.changes.iter().any(|c| {
                ["kind", "location", "alive"].contains(&c.field.as_str())
                    && members.contains(c.entity.as_str())
            })
        })
        .count();
    candidates <= SMALL
}

/// CCR-WP1.6-5: the `NAMESPACE_HISTORY_LIMIT` count that full replay makes
/// when `events[new]` is committed, for a successor commit (which derives
/// no record and so never counted before). Counts the competing cuts of
/// every dependency cone (with the new event) that contains an entity the
/// event touches, exactly as full mode enumerates them (`cone_conflicts`),
/// and refuses with full mode's error when one exceeds `MAX_NAMESPACE_CUTS`.
/// Every other cone of the branch is a cone of the history without the
/// event, with the same relevant events (`successor_may_add_cuts`), whose
/// count the accepted history already bounds. Pure; only the counting, no
/// cut is projected, and nothing at all when a bound shows every count is
/// within the limit (`successor_cones_are_small`).
pub(super) fn check_successor_cuts(branch: &str, events: &[Event], new: usize) -> Result<()> {
    if successor_cones_are_small(events, new) {
        return Ok(());
    }
    let locations = Locations::new(events);
    locations.first_error()?;
    let graph = ConeGraph::new(events, &locations, new);
    let touched: BTreeSet<&str> = events[new]
        .changes
        .iter()
        .filter(|c| ["kind", "location", "alive"].contains(&c.field.as_str()))
        .map(|c| c.entity.as_str())
        .collect();
    let cones: BTreeSet<BTreeSet<&str>> = graph
        .reaching(&touched)
        .into_iter()
        .map(|entity| graph.cone(entity, false))
        .filter(|cone| cone.iter().any(|m| touched.contains(m)))
        .collect();
    let ns = NsContext::new(branch, events);
    for members in &cones {
        let relevant = ns.relevant(members, None);
        ns.count_cuts(&relevant)?;
    }
    Ok(())
}

/// The location pairs that include the new event's location change, in the
/// order full mode evaluates them (pairs of two older changes are stored).
fn location_pairs_with(
    branch: &str,
    events: &[Event],
    locations: &Locations,
    new: usize,
) -> Result<Vec<Conflict>> {
    let rows = &locations.rows;
    let mut conflicts = vec![];
    for q in (0..rows.len()).filter(|&q| rows[q].event == new) {
        for k in (0..rows.len()).filter(|&k| k != q) {
            crate::startup::progress("rebuilding namespace conflicts");
            let (i, j) = if k < q { (k, q) } else { (q, k) };
            let (a, b) = (&rows[i], &rows[j]);
            if a.change.entity == b.change.entity
                || a.change.value["parent"] != b.change.value["parent"]
            {
                continue;
            }
            // Keys are read in full mode's order, so a bad name fails alike.
            if locations.key(i)? != locations.key(j)? {
                continue;
            }
            if let Some(c) = concurrent_placement(
                branch,
                events,
                (&events[a.event], a.change),
                (&events[b.event], b.change),
            ) {
                conflicts.push(c);
            }
        }
    }
    Ok(conflicts)
}

/// Verify mode: both derivations; `write_projection` writes each in its own
/// savepoint, compares them and keeps the full result. Both refusing is a
/// refusal (full mode's error); any other difference is
/// `CONFLICT_MODE_MISMATCH`.
fn derive_verified(input: &DeriveInput, new_id: &str) -> Result<Derived> {
    let append = match derive_append_with(input, new_id) {
        Ok(Plan::Fallback(reason)) => {
            fallback(reason);
            return Ok(Derived::Rows(full_rows(input, Some(new_id))?));
        }
        Ok(Plan::Derived(conflicts)) => finish(input, Some(new_id), conflicts, false),
        Err(error) => Err(error),
    };
    let full = full_rows(input, Some(new_id));
    match (append, full) {
        (Ok(append), Ok(full)) => Ok(Derived::Verify { append, full }),
        (Err(_), Err(full)) => Err(full),
        (Ok(_), Err(full)) => {
            bail!("CONFLICT_MODE_MISMATCH: append accepted {new_id}, full refused it: {full:#}")
        }
        (Err(append), Ok(_)) => {
            bail!("CONFLICT_MODE_MISMATCH: append refused {new_id} ({append:#}), full accepted it")
        }
    }
}

/// Run `f` inside `SAVEPOINT name`, released if `keep` and `f` succeeded,
/// otherwise rolled back. `f`'s own error takes precedence over a failure to
/// close the savepoint.
fn in_savepoint<T>(
    tx: &Connection,
    name: &str,
    keep: bool,
    f: impl FnOnce() -> Result<T>,
) -> Result<T> {
    tx.execute_batch(&format!("SAVEPOINT {name}"))?;
    let result = f();
    let close = if keep && result.is_ok() {
        format!("RELEASE {name}")
    } else {
        format!("ROLLBACK TO {name}; RELEASE {name}")
    };
    let closed = tx.execute_batch(&close);
    let value = result?;
    closed?;
    Ok(value)
}

/// One branch's conflict rows: id -> (payload with `resolved_by` cleared,
/// `resolved_by` column). The payload's own `resolved_by` field is ignored by
/// every reader (the column wins), as in the oracle.
type ConflictRows = BTreeMap<String, (Conflict, Option<String>)>;

fn capture_conflicts(tx: &Connection, branch: &str) -> Result<ConflictRows> {
    let mut rows = BTreeMap::new();
    let mut statement =
        tx.prepare("SELECT id,payload,resolved_by FROM conflicts WHERE branch=?")?;
    let mut query = statement.query([branch])?;
    while let Some(row) = query.next()? {
        let mut conflict: Conflict = serde_json::from_str(&row.get::<_, String>(1)?)?;
        conflict.resolved_by = None;
        rows.insert(row.get(0)?, (conflict, row.get(2)?));
    }
    Ok(rows)
}

/// The differences between two captures, named `[left, right]` in the text.
fn row_differences(left: &ConflictRows, right: &ConflictRows, names: [&str; 2]) -> Vec<String> {
    let [l, r] = names;
    let mut out = vec![];
    for (id, (conflict, resolved)) in left {
        match right.get(id) {
            None => out.push(format!("{id}: only in {l}")),
            Some((other, other_resolved)) => {
                if conflict != other {
                    out.push(format!("{id}: payload differs"));
                }
                if resolved != other_resolved {
                    out.push(format!(
                        "{id}: resolved_by {l} {resolved:?} {r} {other_resolved:?}"
                    ));
                }
            }
        }
    }
    for id in right.keys().filter(|id| !left.contains_key(*id)) {
        out.push(format!("{id}: only in {r}"));
    }
    if out.len() > 8 {
        let more = out.len() - 8;
        out.truncate(8);
        out.push(format!("{more} more"));
    }
    out
}

/// Every branch's conflict rows: (branch, id) -> (payload with `resolved_by`
/// cleared, `resolved_by` column).
type AllConflictRows = BTreeMap<(String, String), (Conflict, Option<String>)>;

fn capture_all_conflicts(db: &Connection) -> Result<AllConflictRows> {
    let mut rows = BTreeMap::new();
    let mut statement = db.prepare("SELECT branch,id,payload,resolved_by FROM conflicts")?;
    let mut query = statement.query([])?;
    while let Some(row) = query.next()? {
        let mut conflict: Conflict = serde_json::from_str(&row.get::<_, String>(2)?)?;
        conflict.resolved_by = None;
        rows.insert((row.get(0)?, row.get(1)?), (conflict, row.get(3)?));
    }
    Ok(rows)
}

impl Store {
    /// `verify_derived_state` with every conflict row re-derived from no rows
    /// (CCR-WP1.7-1 asks for this in `verify_derived_state` itself; until the
    /// integrator applies it, this adapter adds the check on WP1.7's side).
    ///
    /// The plain oracle rebuilds over the stored rows, and a rebuild's upsert
    /// keeps rows it does not derive (it deletes only stale namespace rows)
    /// and keeps a stored `resolved_by` larger than any review's. Here the
    /// rows are also compared with a rebuild that starts from an empty
    /// conflict table, so excess derived state is reported too: a fabricated
    /// or stale row is `conflicts_extra`, an inflated or invented
    /// `resolved_by` is `resolved_by_mismatch`. Same text as the plain
    /// oracle; a row both report is listed once. No side effects: the
    /// rebuild is rolled back (the whole transaction outside a caller's,
    /// taken with `BEGIN IMMEDIATE` so a concurrent writer cannot break it
    /// between its reads and writes; a savepoint inside one).
    #[doc(hidden)]
    pub fn verify_derived_state_strict(&mut self) -> Result<DerivedDiff> {
        let mut diff = self.verify_derived_state()?;
        let branches = self.branches()?;
        let outer = self.db.is_autocommit();
        self.db.execute_batch(if outer {
            "BEGIN IMMEDIATE"
        } else {
            "SAVEPOINT tkfs_derive_oracle"
        })?;
        let result = (|| -> Result<(AllConflictRows, AllConflictRows)> {
            let stored = capture_all_conflicts(&self.db)?;
            self.db.execute("DELETE FROM conflicts", [])?;
            self.rebuild_frontiers()?;
            for branch in &branches {
                self.rebuild(&branch.id)?;
            }
            Ok((stored, capture_all_conflicts(&self.db)?))
        })();
        let rollback = self.db.execute_batch(if outer {
            "ROLLBACK"
        } else {
            "ROLLBACK TO tkfs_derive_oracle; RELEASE tkfs_derive_oracle"
        });
        self.view_cache.borrow_mut().take();
        // The rebuild's own failure is the finding; a failed rollback after
        // it must not hide it.
        let (stored, rebuilt) = result?;
        rollback?;
        let add = |list: &mut Vec<String>, key: &str, text: String| {
            let listed = list
                .iter()
                .any(|item| item == key || item.starts_with(&format!("{key}: ")));
            if !listed {
                list.push(text);
            }
        };
        for ((branch, id), (conflict, resolved)) in &stored {
            let key = format!("{branch}/{id}");
            match rebuilt.get(&(branch.clone(), id.clone())) {
                None => add(&mut diff.conflicts_extra, &key, key.clone()),
                Some((derived, derived_resolved)) => {
                    if conflict != derived {
                        add(
                            &mut diff.conflicts_extra,
                            &key,
                            format!("{key}: payload differs"),
                        );
                    }
                    if resolved != derived_resolved {
                        add(
                            &mut diff.resolved_by_mismatch,
                            &key,
                            format!("{key}: stored {resolved:?} rebuilt {derived_resolved:?}"),
                        );
                    }
                }
            }
        }
        for (branch, id) in rebuilt.keys() {
            if !stored.contains_key(&(branch.clone(), id.clone())) {
                let key = format!("{branch}/{id}");
                add(&mut diff.conflicts_missing, &key, key.clone());
            }
        }
        Ok(diff)
    }
}

fn ancestors(version: &Version, versions: &[Version]) -> BTreeSet<String> {
    let index: BTreeMap<_, _> = versions.iter().map(|v| (v.revision.as_str(), v)).collect();
    let mut found = BTreeSet::new();
    let mut todo = version.parents.clone();
    while let Some(revision) = todo.pop() {
        if found.insert(revision.clone())
            && let Some(v) = index.get(revision.as_str())
        {
            todo.extend(v.parents.iter().cloned());
        }
    }
    found
}
fn event_ancestors(event: &Event, events: &[Event]) -> BTreeSet<String> {
    let index: BTreeMap<_, _> = events.iter().map(|e| (e.id.as_str(), e)).collect();
    let mut seen = BTreeSet::new();
    let mut todo = event.dependencies.clone();
    todo.extend(event.changes.iter().flat_map(|c| c.parents.iter().cloned()));
    while let Some(revision) = todo.pop() {
        if seen.insert(revision.clone())
            && let Some(e) = index.get(revision.as_str())
        {
            todo.extend(e.dependencies.iter().cloned());
            todo.extend(e.changes.iter().flat_map(|c| c.parents.iter().cloned()));
        }
    }
    seen
}
fn linear_versions(versions: &[Version]) -> bool {
    let mut children = BTreeSet::new();
    let mut roots = 0;
    for v in versions {
        if v.parents.is_empty() {
            roots += 1;
        }
        if v.parents.len() > 1 || v.parents.iter().any(|p| !children.insert(p)) {
            return false;
        }
    }
    roots == 1
}
struct VersionReach {
    rows: Vec<Vec<u64>>,
}
impl VersionReach {
    fn new(versions: &[Version]) -> Self {
        let index: BTreeMap<_, _> = versions
            .iter()
            .enumerate()
            .map(|(i, v)| (v.revision.as_str(), i))
            .collect();
        let mut children = vec![vec![]; versions.len()];
        let mut degree = vec![0; versions.len()];
        let mut parents = vec![vec![]; versions.len()];
        for (i, v) in versions.iter().enumerate() {
            for parent in &v.parents {
                if let Some(&j) = index.get(parent.as_str()) {
                    parents[i].push(j);
                    children[j].push(i);
                    degree[i] += 1;
                }
            }
        }
        let mut ready: Vec<_> = (0..versions.len()).filter(|&i| degree[i] == 0).collect();
        let mut rows = vec![vec![0; versions.len().div_ceil(64)]; versions.len()];
        while let Some(i) = ready.pop() {
            for &parent in &parents[i] {
                for word in 0..rows[i].len() {
                    rows[i][word] |= rows[parent][word];
                }
                rows[i][parent / 64] |= 1 << (parent % 64);
            }
            for &child in &children[i] {
                degree[child] -= 1;
                if degree[child] == 0 {
                    ready.push(child);
                }
            }
        }
        Self { rows }
    }
    fn has(&self, child: usize, parent: usize) -> bool {
        self.rows[child][parent / 64] & (1 << (parent % 64)) != 0
    }
}
pub(super) fn activated_ids(events: &[Event]) -> BTreeSet<String> {
    causal_order(events)
        .into_iter()
        .map(|e| e.id.clone())
        .collect()
}
pub(super) fn causal_order(events: &[Event]) -> Vec<&Event> {
    causal_order_refs(&events.iter().collect::<Vec<_>>())
}
fn causal_order_refs<'a>(events: &[&'a Event]) -> Vec<&'a Event> {
    let by_id: BTreeMap<_, _> = events.iter().map(|e| (e.id.as_str(), *e)).collect();
    let mut degrees = BTreeMap::new();
    let mut children: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    let mut ready = BTreeSet::new();
    for e in events {
        let parents: BTreeSet<_> = e
            .dependencies
            .iter()
            .chain(e.changes.iter().flat_map(|c| c.parents.iter()))
            .collect();
        degrees.insert(e.id.as_str(), parents.len());
        if parents.is_empty() {
            ready.insert(e.id.as_str());
        }
        for parent in parents {
            children
                .entry(parent.as_str())
                .or_default()
                .push(e.id.as_str());
        }
    }
    let mut active = Vec::new();
    while let Some(id) = ready.pop_first() {
        crate::startup::progress("validating causal history");
        active.push(by_id[id]);
        if let Some(children) = children.get(id) {
            for child in children {
                let degree = degrees.get_mut(child).unwrap();
                *degree -= 1;
                if *degree == 0 {
                    ready.insert(*child);
                }
            }
        }
    }
    active
}

/// A fixed-size bitset over event (or version) indices.
#[derive(Clone)]
struct Bits {
    words: Vec<u64>,
}
impl Bits {
    fn new(len: usize) -> Self {
        Self {
            words: vec![0; len.div_ceil(64)],
        }
    }
    fn contains(&self, i: usize) -> bool {
        self.words
            .get(i / 64)
            .is_some_and(|word| word & (1 << (i % 64)) != 0)
    }
    /// Whether `i` was newly inserted.
    fn insert(&mut self, i: usize) -> bool {
        let word = &mut self.words[i / 64];
        let bit = 1 << (i % 64);
        let new = *word & bit == 0;
        *word |= bit;
        new
    }
    fn union_with(&mut self, other: &Bits) {
        for (word, other) in self.words.iter_mut().zip(&other.words) {
            *word |= other;
        }
    }
    fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.words.iter().enumerate().flat_map(|(w, &word)| {
            let mut word = word;
            std::iter::from_fn(move || {
                (word != 0).then(|| {
                    let bit = word.trailing_zeros() as usize;
                    word &= word - 1;
                    w * 64 + bit
                })
            })
        })
    }
    fn bytes(&self) -> usize {
        self.words.len() * 8
    }
}

/// Everything reachable from `from` along `edges` (excluding `from` itself
/// unless it lies on a cycle): the historical `FactGraph::closure`.
fn reach(edges: &[Vec<usize>], from: usize) -> Bits {
    let mut found = Bits::new(edges.len());
    let mut todo = edges[from].clone();
    while let Some(i) = todo.pop() {
        if found.insert(i) {
            todo.extend(&edges[i]);
        }
    }
    found
}

fn children_of(parents: &[Vec<usize>]) -> Vec<Vec<usize>> {
    let mut children = vec![vec![]; parents.len()];
    for (i, ps) in parents.iter().enumerate() {
        for &p in ps {
            children[p].push(i);
        }
    }
    children
}

/// Memoized closures are bounded so a very long history cannot exhaust
/// memory; past the budget, closures are recomputed on demand.
const CLOSURE_MEMO_BYTES: usize = 64 * 1024 * 1024;
/// Bound on memoized cut projections, in estimated bytes (`conflict_bytes`).
/// A cut's namespace records are all of the cut's, not only the cone's, so
/// with many unresolved collisions they grow with the cut: unbounded, the
/// memo would hold (cuts × records per cut). Past the budget, cuts are
/// projected again when they recur.
const CUT_MEMO_BYTES: usize = 16 * 1024 * 1024;

/// The estimated heap a JSON value holds (map nodes at 64 bytes per entry).
fn value_bytes(value: &Value) -> usize {
    std::mem::size_of::<Value>()
        + match value {
            Value::String(s) => s.len(),
            Value::Array(items) => items.iter().map(value_bytes).sum(),
            Value::Object(map) => map.iter().map(|(k, v)| 64 + k.len() + value_bytes(v)).sum(),
            _ => 0,
        }
}

/// The estimated heap a conflict record holds (strings, versions, values).
fn conflict_bytes(c: &Conflict) -> usize {
    let version = |v: &Version| {
        std::mem::size_of::<Version>()
            + v.revision.len()
            + v.device.len()
            + v.parents
                .iter()
                .map(|p| std::mem::size_of::<String>() + p.len())
                .sum::<usize>()
            + value_bytes(&v.value)
    };
    std::mem::size_of::<Conflict>()
        + c.id.len()
        + c.branch.len()
        + c.entity.len()
        + c.field.len()
        + c.reason.len()
        + c.resolved_by.as_ref().map_or(0, String::len)
        + c.alternatives
            .iter()
            .chain(&c.bases)
            .map(version)
            .sum::<usize>()
}

/// One alive register analysed for TRUE presence compression: its versions,
/// the causal context of each tombstone and the unbranched segments, all over
/// the transitively reduced register graph (indices into `versions`).
struct RegisterFacts<'a> {
    versions: Vec<(usize, &'a Change)>,
    false_context: Vec<(Bits, Bits)>,
    segments: Vec<usize>,
}

#[derive(Default)]
struct Memo<'a> {
    ancestors: Vec<Option<Rc<Bits>>>,
    descendants: Vec<Option<Rc<Bits>>>,
    closure_bytes: usize,
    registers: RegisterMemo<'a>,
    cuts: BTreeMap<Vec<usize>, Rc<Vec<Conflict>>>,
    /// Estimated bytes `cuts` holds, and its budget (`CUT_MEMO_BYTES`).
    cut_bytes: usize,
    cut_budget: usize,
}

/// Namespace-history derivation over one branch's events (both modes), with
/// per-call memoization. Event indices are positions in `events`.
struct NsContext<'a> {
    branch: &'a str,
    events: &'a [Event],
    /// The causal DAG: dependencies and change parents present in `events`.
    parents: Vec<Vec<usize>>,
    children: Vec<Vec<usize>>,
    index: BTreeMap<&'a str, usize>,
    /// Alive versions per entity, in event order.
    alive: BTreeMap<&'a str, Vec<(usize, &'a Change)>>,
    /// Events with a kind, location or alive change, per entity.
    touching: BTreeMap<&'a str, Vec<usize>>,
    memo: RefCell<Memo<'a>>,
}

/// One cone append mode derives.
struct PlannedCone<'a> {
    members: BTreeSet<&'a str>,
    relevant: Rc<Vec<usize>>,
    /// An old cone: project only the antichains containing one of these
    /// newly relevant events (sorted). `None`, a new cone: every antichain.
    only: Option<Vec<usize>>,
}

/// The cones append mode derives.
struct AppendPlan<'a> {
    cones: Vec<PlannedCone<'a>>,
    /// Old cones that grew into a covering cone (checked, not derived).
    #[cfg_attr(not(test), allow(dead_code))]
    covered: usize,
    /// The members of every cone (with the new event) that contains an
    /// entity the new event touches: the only entities whose records in the
    /// current projection the new event can change. Closed under parents
    /// and slot competitors (a union of cones).
    members: BTreeSet<&'a str>,
    /// The old cone (without the new event) of every entity that reaches a
    /// touched entity and existed before.
    before: BTreeMap<&'a str, BTreeSet<&'a str>>,
}

/// Memoized alive-register analyses, by (entity, excluded event).
type RegisterMemo<'a> = BTreeMap<(&'a str, Option<usize>), Option<Rc<RegisterFacts<'a>>>>;

impl<'a> NsContext<'a> {
    fn new(branch: &'a str, events: &'a [Event]) -> Self {
        let index: BTreeMap<&str, usize> = events
            .iter()
            .enumerate()
            .map(|(i, e)| (e.id.as_str(), i))
            .collect();
        let parents: Vec<Vec<usize>> = events
            .iter()
            .map(|e| {
                e.dependencies
                    .iter()
                    .chain(e.changes.iter().flat_map(|c| c.parents.iter()))
                    .filter_map(|p| index.get(p.as_str()).copied())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect()
            })
            .collect();
        let children = children_of(&parents);
        let mut alive: BTreeMap<&str, Vec<(usize, &Change)>> = BTreeMap::new();
        let mut touching: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
        for (i, e) in events.iter().enumerate() {
            for c in &e.changes {
                if c.field == "alive" {
                    alive.entry(c.entity.as_str()).or_default().push((i, c));
                }
                if ["kind", "location", "alive"].contains(&c.field.as_str()) {
                    let list = touching.entry(c.entity.as_str()).or_default();
                    if list.last() != Some(&i) {
                        list.push(i);
                    }
                }
            }
        }
        let memo = Memo {
            ancestors: vec![None; events.len()],
            descendants: vec![None; events.len()],
            cut_budget: CUT_MEMO_BYTES,
            ..Default::default()
        };
        Self {
            branch,
            events,
            parents,
            children,
            index,
            alive,
            touching,
            memo: RefCell::new(memo),
        }
    }

    /// Ancestors (`forward == false`) or descendants of event `i`.
    fn closure(&self, i: usize, forward: bool) -> Rc<Bits> {
        {
            let memo = self.memo.borrow();
            let slot = if forward {
                &memo.descendants[i]
            } else {
                &memo.ancestors[i]
            };
            if let Some(found) = slot {
                return found.clone();
            }
        }
        let edges = if forward {
            &self.children
        } else {
            &self.parents
        };
        let found = Rc::new(reach(edges, i));
        let mut memo = self.memo.borrow_mut();
        if memo.closure_bytes + found.bytes() <= CLOSURE_MEMO_BYTES {
            memo.closure_bytes += found.bytes();
            let slot = if forward {
                &mut memo.descendants[i]
            } else {
                &mut memo.ancestors[i]
            };
            *slot = Some(found.clone());
        }
        found
    }

    /// Whether `c` (a change of a cone member) makes its event relevant to
    /// the cone's namespace: kind, location, a tombstone, or presence
    /// re-asserted over a tombstone.
    fn primary_change(&self, c: &Change) -> bool {
        ["kind", "location"].contains(&c.field.as_str())
            || (c.field == "alive"
                && (c.value == false
                    || c.parents.iter().any(|parent| {
                        self.index.get(parent.as_str()).is_some_and(|&j| {
                            self.events[j].changes.iter().any(|p| {
                                p.entity == c.entity && p.field == "alive" && p.value == false
                            })
                        })
                    })))
    }

    /// The relevant events of a cone (relevance plus TRUE presence
    /// compression), as if event `exclude` did not exist. Sorted.
    fn relevant(&self, members: &BTreeSet<&str>, exclude: Option<usize>) -> Vec<usize> {
        let mut relevant = BTreeSet::new();
        for &member in members {
            for &i in self.touching.get(member).into_iter().flatten() {
                if Some(i) != exclude
                    && self.events[i]
                        .changes
                        .iter()
                        .any(|c| c.entity == member && self.primary_change(c))
                {
                    relevant.insert(i);
                }
            }
        }
        let primary: Vec<_> = relevant
            .iter()
            .map(|&i| (self.closure(i, false), self.closure(i, true)))
            .collect();
        // Pure TRUE presence assertions cannot change namespace when there is
        // no tombstone. If tombstones exist, retain rank extrema per unbranched
        // TRUE segment and topology/false-revision causal relationship signature.
        // This preserves possible boolean winners without counting every save.
        for &entity in members {
            let Some((&entity, _)) = self.alive.get_key_value(entity) else {
                continue;
            };
            let Some(facts) = self.register_facts(entity, exclude) else {
                continue;
            };
            let mut groups: BTreeMap<(usize, Vec<u8>), (usize, usize)> = BTreeMap::new();
            for (k, &(i, c)) in facts.versions.iter().enumerate() {
                if c.value != true || relevant.contains(&i) {
                    continue;
                }
                let signature = primary
                    .iter()
                    .map(|(anc, desc)| {
                        if anc.contains(i) {
                            1
                        } else if desc.contains(i) {
                            2
                        } else {
                            0
                        }
                    })
                    .chain(facts.false_context.iter().map(|(anc, desc)| {
                        if anc.contains(k) {
                            1
                        } else if desc.contains(k) {
                            2
                        } else {
                            0
                        }
                    }))
                    .collect();
                let range = groups
                    .entry((facts.segments[k], signature))
                    .or_insert((i, i));
                let rank = |j: usize| (&self.events[j].device, &self.events[j].id);
                if rank(i) < rank(range.0) {
                    range.0 = i;
                }
                if rank(i) > rank(range.1) {
                    range.1 = i;
                }
            }
            for (min, max) in groups.values() {
                relevant.extend([*min, *max]);
            }
        }
        relevant.into_iter().collect()
    }

    /// The alive register of `entity` (without event `exclude`), analysed;
    /// `None` without a tombstone (no compression applies).
    fn register_facts(
        &self,
        entity: &'a str,
        exclude: Option<usize>,
    ) -> Option<Rc<RegisterFacts<'a>>> {
        if let Some(found) = self.memo.borrow().registers.get(&(entity, exclude)) {
            return found.clone();
        }
        let facts = self.analyse_register(entity, exclude).map(Rc::new);
        self.memo
            .borrow_mut()
            .registers
            .insert((entity, exclude), facts.clone());
        facts
    }

    /// The historical `FactGraph::register` analysis, on a compact graph whose
    /// nodes are the register's versions in event order (the other events are
    /// isolated nodes there and never change closures, ranks or segments).
    fn analyse_register(&self, entity: &str, exclude: Option<usize>) -> Option<RegisterFacts<'a>> {
        let versions: Vec<(usize, &'a Change)> = self
            .alive
            .get(entity)?
            .iter()
            .copied()
            .filter(|(i, _)| Some(*i) != exclude)
            .collect();
        let tombstones: Vec<usize> = versions
            .iter()
            .enumerate()
            .filter(|(_, (_, c))| c.value == false)
            .map(|(k, _)| k)
            .collect();
        if tombstones.is_empty() {
            return None;
        }
        let index: BTreeMap<&str, usize> = versions
            .iter()
            .enumerate()
            .map(|(k, (i, _))| (self.events[*i].id.as_str(), k))
            .collect();
        let parents: Vec<Vec<usize>> = versions
            .iter()
            .map(|(_, c)| {
                c.parents
                    .iter()
                    .filter_map(|p| index.get(p.as_str()).copied())
                    .collect()
            })
            .collect();
        let children = children_of(&parents);
        // Explicit review can list many ancestors as parents. Such redundant
        // edges are evidence, not new branches in an unchanged-TRUE segment.
        // Reduce only this analysis graph; immutable event parents stay intact.
        let mut degree: Vec<_> = parents.iter().map(Vec::len).collect();
        let mut ready: Vec<_> = (0..degree.len()).filter(|&k| degree[k] == 0).collect();
        let mut rank = vec![0; degree.len()];
        let mut sequence = 0;
        while let Some(k) = ready.pop() {
            rank[k] = sequence;
            sequence += 1;
            for &child in &children[k] {
                degree[child] -= 1;
                if degree[child] == 0 {
                    ready.push(child);
                }
            }
        }
        let mut reduced = parents.clone();
        for ps in &mut reduced {
            if ps.len() < 2 {
                continue;
            }
            ps.sort_by_key(|&p| std::cmp::Reverse(rank[p]));
            let mut covered = Bits::new(parents.len());
            ps.retain(|&p| {
                if covered.contains(p) {
                    false
                } else {
                    covered.union_with(&reach(&parents, p));
                    true
                }
            });
        }
        let reduced_children = children_of(&reduced);
        let false_context = tombstones
            .iter()
            .map(|&k| (reach(&reduced, k), reach(&reduced_children, k)))
            .collect();
        let mut degree: Vec<_> = reduced.iter().map(Vec::len).collect();
        let mut ready: Vec<_> = (0..degree.len()).filter(|&k| degree[k] == 0).collect();
        let mut segments: Vec<_> = (0..degree.len()).collect();
        while let Some(k) = ready.pop() {
            if reduced[k].len() == 1 && reduced_children[reduced[k][0]].len() == 1 {
                segments[k] = segments[reduced[k][0]];
            }
            for &child in &reduced_children[k] {
                degree[child] -= 1;
                if degree[child] == 0 {
                    ready.push(child);
                }
            }
        }
        Some(RegisterFacts {
            versions,
            false_context,
            segments,
        })
    }

    /// The namespace records of one antichain's cut (memoized per antichain,
    /// within `Memo::cut_budget`: an antichain recurs across the cones that
    /// share its events, such as the cones of a directory's entries).
    fn cut_conflicts(
        &self,
        selected: &[usize],
        relevant: &[usize],
        ancestors: &[Rc<Bits>],
    ) -> Result<Rc<Vec<Conflict>>> {
        let key: Vec<usize> = selected.iter().map(|&j| relevant[j]).collect();
        if let Some(found) = self.memo.borrow().cuts.get(&key) {
            return Ok(found.clone());
        }
        let mut ids = Bits::new(self.events.len());
        for &j in selected {
            ids.union_with(&ancestors[j]);
            ids.insert(relevant[j]);
        }
        let cut: Vec<&Event> = ids.iter().map(|i| &self.events[i]).collect();
        let conflicts: Rc<Vec<Conflict>> = Rc::new(
            project_refs(self.branch, &cut)?
                .conflicts
                .into_iter()
                .filter(|c| c.field == "namespace")
                .collect(),
        );
        let bytes = 64
            + key.len() * std::mem::size_of::<usize>()
            + conflicts.iter().map(conflict_bytes).sum::<usize>();
        let mut memo = self.memo.borrow_mut();
        if memo.cut_bytes + bytes <= memo.cut_budget {
            memo.cut_bytes += bytes;
            memo.cuts.insert(key, conflicts.clone());
        }
        Ok(conflicts)
    }

    /// Enumerate every antichain of a cone's relevant events (counting them
    /// against `MAX_NAMESPACE_CUTS`) and record the cone's namespace records
    /// of each antichain's cut; with `only`, only of the antichains containing
    /// one of those events (sorted). `restricted` projects a cut from the
    /// members' registers alone (`namespace_records`; only when no cut can
    /// fail to project, see `projection_cannot_fail`).
    fn cone_conflicts(
        &self,
        members: &BTreeSet<&str>,
        relevant: &[usize],
        only: Option<&[usize]>,
        restricted: bool,
        out: &mut BTreeMap<String, Conflict>,
    ) -> Result<()> {
        let ancestors: Vec<_> = relevant.iter().map(|&i| self.closure(i, false)).collect();
        let mut antichains: Vec<Vec<usize>> = vec![vec![]];
        let mut competing_cuts = 0;
        for (i, &event) in relevant.iter().enumerate() {
            let before = antichains.len();
            for index in 0..before {
                crate::startup::progress("replaying namespace cuts");
                if !antichains[index]
                    .iter()
                    .all(|&j| !ancestors[i].contains(relevant[j]) && !ancestors[j].contains(event))
                {
                    continue;
                }
                let mut selected = antichains[index].clone();
                selected.push(i);
                if selected.len() > 1 {
                    competing_cuts += 1;
                    ensure!(
                        competing_cuts <= MAX_NAMESPACE_CUTS,
                        "NAMESPACE_HISTORY_LIMIT: more than {MAX_NAMESPACE_CUTS} competing topology cuts in one dependency cone; commit refused without discarding conflicts"
                    );
                }
                if only.is_none_or(|only| {
                    selected
                        .iter()
                        .any(|&j| only.binary_search(&relevant[j]).is_ok())
                }) {
                    if restricted {
                        let mut cut = Bits::new(self.events.len());
                        for &j in &selected {
                            cut.union_with(&ancestors[j]);
                            cut.insert(relevant[j]);
                        }
                        for c in self.namespace_records(members, &cut)? {
                            out.insert(c.id.clone(), c);
                        }
                    } else {
                        for c in self
                            .cut_conflicts(&selected, relevant, &ancestors)?
                            .iter()
                            .filter(|c| c.entity.split(',').any(|e| members.contains(e)))
                        {
                            out.insert(c.id.clone(), c.clone());
                        }
                    }
                }
                antichains.push(selected);
            }
        }
        Ok(())
    }

    /// The competing cuts (antichains of two or more events) of a cone's
    /// relevant events, enumerated and refused exactly as `cone_conflicts`
    /// does, without projecting any cut (CCR-WP1.6-5).
    fn count_cuts(&self, relevant: &[usize]) -> Result<usize> {
        let ancestors: Vec<_> = relevant.iter().map(|&i| self.closure(i, false)).collect();
        let mut antichains: Vec<Vec<usize>> = vec![vec![]];
        let mut competing_cuts = 0;
        for (i, &event) in relevant.iter().enumerate() {
            let before = antichains.len();
            for index in 0..before {
                if !antichains[index]
                    .iter()
                    .all(|&j| !ancestors[i].contains(relevant[j]) && !ancestors[j].contains(event))
                {
                    continue;
                }
                let mut selected = antichains[index].clone();
                selected.push(i);
                if selected.len() > 1 {
                    competing_cuts += 1;
                    ensure!(
                        competing_cuts <= MAX_NAMESPACE_CUTS,
                        "NAMESPACE_HISTORY_LIMIT: more than {MAX_NAMESPACE_CUTS} competing topology cuts in one dependency cone; commit refused without discarding conflicts"
                    );
                }
                antichains.push(selected);
            }
        }
        Ok(competing_cuts)
    }

    /// Full mode: every antichain of every cone (the historical
    /// `namespace_history`).
    fn full(&self) -> Result<Vec<Conflict>> {
        let cones = NamespaceCones::new(self.events)?;
        let mut conflicts = BTreeMap::new();
        for members in cones.members() {
            crate::startup::progress("rebuilding namespace history");
            let members: BTreeSet<&str> = members.iter().map(String::as_str).collect();
            let relevant = self.relevant(&members, None);
            self.cone_conflicts(&members, &relevant, None, false, &mut conflicts)?;
        }
        Ok(conflicts.into_values().collect())
    }

    /// The §5.2 exactness precondition (module docs) for appending event
    /// `new`: the cones to derive, or why append mode cannot run.
    ///
    /// Only cones that contain an entity `E` touches can differ, and only the
    /// entities that reach a touched entity (`ConeGraph::reaching`) can have
    /// such a cone, before or after: every other cone of the old history is a
    /// cone of the new one with the same relevant set (module docs), and
    /// every other cone of the new history is either such a cone or the cone
    /// of an entity `E` only mentions, which has no relevant event. A cone
    /// equal to one of these is itself the cone of an entity that reaches a
    /// touched entity (it contains the entity whose cone it equals), so the
    /// comparisons below between old and new cones are exact.
    fn append_plan(
        &self,
        new: usize,
        locations: &'a Locations<'a>,
    ) -> Result<std::result::Result<AppendPlan<'a>, &'static str>> {
        let graph = ConeGraph::new(self.events, locations, new);
        let touched: BTreeSet<&str> = self.events[new]
            .changes
            .iter()
            .filter(|c| ["kind", "location", "alive"].contains(&c.field.as_str()))
            .map(|c| c.entity.as_str())
            .collect();
        let reaching = graph.reaching(&touched);
        let after: BTreeMap<&str, BTreeSet<&str>> = reaching
            .iter()
            .map(|&entity| (entity, graph.cone(entity, false)))
            .collect();
        let before: BTreeMap<&str, BTreeSet<&str>> = reaching
            .iter()
            .filter(|&&entity| graph.existed(entity))
            .map(|&entity| (entity, graph.cone(entity, true)))
            .collect();
        let after_set: BTreeSet<&BTreeSet<&str>> = after.values().collect();
        let before_set: BTreeSet<&BTreeSet<&str>> = before.values().collect();
        let mut cache: BTreeMap<BTreeSet<&str>, Rc<Vec<usize>>> = BTreeMap::new();
        let mut relevant_now = |members: &BTreeSet<&'a str>| -> Rc<Vec<usize>> {
            if let Some(found) = cache.get(members) {
                return found.clone();
            }
            let found = Rc::new(self.relevant(members, None));
            cache.insert(members.clone(), found.clone());
            found
        };
        let relevant_then = |members: &BTreeSet<&str>| self.relevant(members, Some(new));
        // Sorted-slice subset test.
        let subset =
            |small: &[usize], large: &[usize]| small.iter().all(|i| large.binary_search(i).is_ok());
        let mut cones = vec![];
        for &members in &after_set {
            if before_set.contains(members) {
                if !members.iter().any(|m| touched.contains(m)) {
                    continue;
                }
                // Every old antichain must survive (its records are stored and
                // full mode would re-derive them); the antichains that contain
                // a newly relevant event (the new event, or a presence witness
                // its tombstone brings in) are new.
                let now = relevant_now(members);
                let then = relevant_then(members);
                if !subset(&then, &now) {
                    return Ok(Err("a touched cone lost a relevant event"));
                }
                let added: Vec<usize> = now
                    .iter()
                    .copied()
                    .filter(|i| then.binary_search(i).is_err())
                    .collect();
                if !added.is_empty() {
                    cones.push(PlannedCone {
                        members: members.clone(),
                        relevant: now,
                        only: Some(added),
                    });
                }
            } else {
                cones.push(PlannedCone {
                    members: members.clone(),
                    relevant: relevant_now(members),
                    only: None,
                });
            }
        }
        let mut seen = BTreeSet::new();
        for (entity, old) in &before {
            if after_set.contains(old) || !seen.insert(old) {
                continue;
            }
            let cover = &after[entity];
            if !old.is_subset(cover) {
                return Ok(Err("a cone shrank"));
            }
            let was = relevant_then(old);
            if before_set.contains(cover) {
                // An old cover keeps its old antichains (a superset of this
                // cone's) only if it lost no relevant event.
                let cover_then = relevant_then(cover);
                if !subset(&was, &cover_then) || !subset(&cover_then, &relevant_now(cover)) {
                    return Ok(Err("a grown cone is not covered by an old cone"));
                }
            } else if !subset(&was, &relevant_now(cover)) {
                return Ok(Err("a grown cone lost a relevant event"));
            }
        }
        let covered = seen.len();
        let members = after
            .values()
            .filter(|cone| cone.iter().any(|m| touched.contains(m)))
            .flatten()
            .copied()
            .collect();
        Ok(Ok(AppendPlan {
            cones,
            covered,
            members,
            before,
        }))
    }

    /// The step the namespace history alone does not give (module docs,
    /// "current records"): every namespace record of the old current
    /// projection that the new event takes out of the projection must still
    /// be derived, or full mode's DELETE would remove it. `derived` is
    /// everything append mode derives (the new projection's records
    /// included). A record that is neither derived nor a record of its old
    /// cone's maximal cut (an antichain cut of the old namespace history,
    /// which full mode re-derives) fails the check.
    ///
    /// Records of entities outside `plan.members` are unchanged (their cones
    /// contain no entity the new event touches). Both projections over the
    /// members are computed from the members' registers: the old history and
    /// every antichain cut of it project (the rebuild succeeded), so the
    /// restricted records equal the projection's (`namespace_records`).
    fn current_records_survive(
        &self,
        new: usize,
        plan: &AppendPlan<'a>,
        derived: &[Conflict],
    ) -> Result<bool> {
        if plan.members.is_empty() {
            return Ok(true);
        }
        let mut old = Bits::new(self.events.len());
        for i in (0..self.events.len()).filter(|&i| i != new) {
            old.insert(i);
        }
        let derived: BTreeSet<&str> = derived.iter().map(|c| c.id.as_str()).collect();
        let mut maximal: BTreeMap<&BTreeSet<&str>, BTreeSet<String>> = BTreeMap::new();
        for record in self.namespace_records(&plan.members, &old)? {
            if derived.contains(record.id.as_str()) {
                continue;
            }
            let entity = record.entity.split(',').next().unwrap_or_default();
            let Some(cone) = plan.before.get(entity) else {
                return Ok(false);
            };
            if !maximal.contains_key(cone) {
                let mut cut = Bits::new(self.events.len());
                for i in self.relevant(cone, Some(new)) {
                    cut.union_with(&self.closure(i, false));
                    cut.insert(i);
                }
                let ids = self
                    .namespace_records(cone, &cut)?
                    .into_iter()
                    .map(|c| c.id)
                    .collect();
                maximal.insert(cone, ids);
            }
            if !maximal[cone].contains(&record.id) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Append mode: the planned cones' records.
    fn derive_planned(&self, plan: &AppendPlan, restricted: bool) -> Result<Vec<Conflict>> {
        let mut conflicts = BTreeMap::new();
        for cone in &plan.cones {
            crate::startup::progress("rebuilding namespace history");
            self.cone_conflicts(
                &cone.members,
                &cone.relevant,
                cone.only.as_deref(),
                restricted,
                &mut conflicts,
            )?;
        }
        Ok(conflicts.into_values().collect())
    }

    /// The namespace records of `project(cut)` that mention a cone member,
    /// computed from the members' kind, location and alive registers alone.
    ///
    /// Exact because a member's slot competitors and every directory it was
    /// ever placed in are members (the cone is closed under both, over the
    /// whole branch), so the namespace verdicts on members never read another
    /// entity's registers; and an entity without these registers behaves as
    /// an absent one (never alive, never a directory). The only other effect
    /// of the other entities, a failure of `project` on them, is ruled out by
    /// the caller (`projection_cannot_fail`).
    fn namespace_records(&self, members: &BTreeSet<&str>, cut: &Bits) -> Result<Vec<Conflict>> {
        let mut registers: BTreeMap<String, Vec<Version>> = BTreeMap::new();
        for &member in members {
            for &i in self.touching.get(member).into_iter().flatten() {
                if !cut.contains(i) {
                    continue;
                }
                let e = &self.events[i];
                for c in e.changes.iter().filter(|c| {
                    c.entity == member && ["kind", "location", "alive"].contains(&c.field.as_str())
                }) {
                    registers
                        .entry(register(&c.entity, &c.field))
                        .or_default()
                        .push(Version {
                            revision: e.id.clone(),
                            device: e.device.clone(),
                            created_ms: e.created_ms,
                            parents: c.parents.clone(),
                            value: c.value.clone(),
                        });
                }
            }
        }
        // As in `project`: each register's winner among its heads.
        #[derive(Default)]
        struct Placement {
            kind: String,
            parent: String,
            name: String,
            alive: bool,
        }
        let mut entries: BTreeMap<String, Placement> = BTreeMap::new();
        let mut locations: BTreeMap<String, Vec<Version>> = BTreeMap::new();
        for (key, versions) in registers {
            let dominated: BTreeSet<&str> = versions
                .iter()
                .flat_map(|v| v.parents.iter().map(String::as_str))
                .collect();
            let mut heads: Vec<Version> = versions
                .iter()
                .filter(|v| !dominated.contains(v.revision.as_str()))
                .cloned()
                .collect();
            heads.sort_by(|a, b| (&a.device, &a.revision).cmp(&(&b.device, &b.revision)));
            let winner = heads.last().unwrap();
            let (entity, field) = key.rsplit_once('/').unwrap();
            let entry = entries.entry(entity.into()).or_default();
            match field {
                "kind" => entry.kind = winner.value.as_str().unwrap().into(),
                "location" => {
                    entry.parent = winner.value["parent"].as_str().unwrap().into();
                    entry.name = winner.value["name"].as_str().unwrap().into();
                }
                "alive" => entry.alive = winner.value.as_bool().unwrap(),
                _ => {}
            }
            if field == "location" {
                locations.insert(entity.into(), heads);
            }
        }
        // As in `project`: slot collisions, then missing parents and cycles.
        let mut conflicts = vec![];
        let ids: Vec<_> = entries.keys().cloned().collect();
        let mut slots: BTreeMap<(String, Arc<str>), Vec<String>> = BTreeMap::new();
        for entity in &ids {
            let e = &entries[entity];
            if e.alive {
                slots
                    .entry((e.parent.clone(), slot_key(&e.name)?))
                    .or_default()
                    .push(entity.clone());
            }
        }
        for members in slots.values().filter(|m| m.len() > 1) {
            let winner = members.iter().max().unwrap();
            for entity in members {
                if entity != winner {
                    entries.get_mut(entity).unwrap().alive = false;
                }
            }
            for (i, a) in members.iter().enumerate() {
                for b in &members[i + 1..] {
                    let pair = [locations[a].clone(), locations[b].clone()].concat();
                    conflicts.push(make_conflict(
                        self.branch,
                        &format!("{a},{b}"),
                        "namespace",
                        "same name / file-directory collision",
                        pair,
                        vec![],
                    ));
                }
            }
        }
        for entity in &ids {
            let mut cursor = entity.clone();
            let mut visited = BTreeSet::new();
            let mut invalid = false;
            while cursor != ROOT {
                if !visited.insert(cursor.clone()) {
                    invalid = true;
                    break;
                }
                if let Some(e) = entries.get(&cursor) {
                    if !e.alive || (cursor != *entity && e.kind != "directory") {
                        invalid = true;
                        break;
                    }
                    cursor = e.parent.clone();
                } else {
                    invalid = true;
                    break;
                }
            }
            if invalid && entries[entity].alive {
                entries.get_mut(entity).unwrap().alive = false;
                conflicts.push(make_conflict(
                    self.branch,
                    entity,
                    "namespace",
                    "deleted/missing parent or directory cycle",
                    locations.get(entity).cloned().unwrap_or_default(),
                    vec![],
                ));
            }
        }
        conflicts.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(conflicts)
    }
}

struct NamespaceCones {
    parents: BTreeMap<String, BTreeSet<String>>,
    entity_slots: BTreeMap<String, BTreeSet<(String, String)>>,
    slots: BTreeMap<(String, String), BTreeSet<String>>,
}
impl NamespaceCones {
    fn new<'e>(events: impl IntoIterator<Item = &'e Event>) -> Result<Self> {
        let mut result = Self {
            parents: BTreeMap::new(),
            entity_slots: BTreeMap::new(),
            slots: BTreeMap::new(),
        };
        for e in events {
            for c in &e.changes {
                result.parents.entry(c.entity.clone()).or_default();
                if c.field == "location" {
                    let parent = c.value["parent"].as_str().unwrap();
                    if parent != ROOT {
                        result
                            .parents
                            .entry(c.entity.clone())
                            .or_default()
                            .insert(parent.to_owned());
                        result.parents.entry(parent.to_owned()).or_default();
                    }
                    let key = (
                        parent.to_owned(),
                        name_key(c.value["name"].as_str().unwrap())?,
                    );
                    result
                        .entity_slots
                        .entry(c.entity.clone())
                        .or_default()
                        .insert(key.clone());
                    result
                        .slots
                        .entry(key)
                        .or_default()
                        .insert(c.entity.clone());
                }
            }
        }
        Ok(result)
    }
    /// The dependency cone of `entity`: itself, its parents and slot
    /// competitors, transitively.
    fn cone(&self, entity: &str) -> BTreeSet<String> {
        crate::startup::progress("analyzing namespace dependencies");
        let mut members = BTreeSet::new();
        let mut visited_slots = BTreeSet::new();
        let mut pending = vec![entity.to_owned()];
        while let Some(next) = pending.pop() {
            if !members.insert(next.clone()) {
                continue;
            }
            pending.extend(self.parents[&next].iter().cloned());
            if let Some(slots) = self.entity_slots.get(&next) {
                for slot in slots {
                    if visited_slots.insert(slot.clone()) {
                        pending.extend(self.slots[slot].iter().cloned());
                    }
                }
            }
        }
        // Ancestors and slot competitors can affect this entry's visibility.
        // Sharing an ancestor alone does not make sibling histories interact.
        members
    }
    /// The distinct cones.
    fn members(&self) -> BTreeSet<BTreeSet<String>> {
        self.parents
            .keys()
            .map(|entity| self.cone(entity))
            .collect()
    }
}

/// Complete topological activation followed by per-register ancestry. Unknown
/// parents keep the entire atomic event pending; child-before-parent is durable.
pub fn project(branch: &str, events: &[Event]) -> Result<Projection> {
    project_refs(branch, &events.iter().collect::<Vec<_>>())
}
/// `project` over borrowed events (cuts are projected without cloning them).
fn project_refs(branch: &str, events: &[&Event]) -> Result<Projection> {
    let by_id: BTreeMap<_, _> = events.iter().map(|e| (e.id.clone(), *e)).collect();
    for e in events {
        for c in &e.changes {
            for parent in &c.parents {
                if let Some(parent) = by_id.get(parent) {
                    ensure!(
                        parent
                            .changes
                            .iter()
                            .any(|pc| pc.entity == c.entity && pc.field == c.field),
                        "INVALID_CAUSAL_PARENT"
                    );
                }
            }
        }
    }
    let active: BTreeSet<String> = causal_order_refs(events)
        .into_iter()
        .map(|e| e.id.clone())
        .collect();
    // A known closed dependency cycle is malformed, rather than permanently pending.
    let pending: Vec<_> = by_id
        .keys()
        .filter(|e| !active.contains(*e))
        .cloned()
        .collect();
    if !pending.is_empty() {
        let has_missing = events.iter().any(|e| {
            e.dependencies.iter().any(|p| !by_id.contains_key(p))
                || e.changes
                    .iter()
                    .any(|c| c.parents.iter().any(|p| !by_id.contains_key(p)))
        });
        ensure!(has_missing, "CAUSAL_CYCLE");
    }
    let mut registers: BTreeMap<String, Vec<Version>> = BTreeMap::new();
    for e in events.iter().filter(|e| active.contains(&e.id)) {
        for c in &e.changes {
            registers
                .entry(register(&c.entity, &c.field))
                .or_default()
                .push(Version {
                    revision: e.id.clone(),
                    device: e.device.clone(),
                    created_ms: e.created_ms,
                    parents: c.parents.clone(),
                    value: c.value.clone(),
                });
        }
    }
    let mut p = Projection {
        pending,
        ..Default::default()
    };
    for (key, mut versions) in registers {
        crate::startup::progress("projecting registers");
        // All ancestors are dominated before the stable concurrent rank is used.
        let dominated: BTreeSet<_> = versions
            .iter()
            .flat_map(|v| v.parents.iter().cloned())
            .collect();
        let mut heads: Vec<_> = versions
            .iter()
            .filter(|v| !dominated.contains(&v.revision))
            .cloned()
            .collect();
        heads.sort_by(|a, b| (&a.device, &a.revision).cmp(&(&b.device, &b.revision)));
        let winner = heads.last().unwrap();
        let (entity, field) = key.rsplit_once('/').unwrap();
        let entry = p.entries.entry(entity.into()).or_insert_with(|| Entry {
            id: entity.into(),
            ..Default::default()
        });
        entry
            .revisions
            .insert(field.into(), winner.revision.clone());
        if field != "basic" {
            entry.modified_ms = entry.modified_ms.max(winner.created_ms);
        }
        match field {
            "kind" => {
                entry.kind = winner.value.as_str().unwrap().into();
                entry.created_ms = winner.created_ms;
            }
            "location" => {
                entry.parent = winner.value["parent"].as_str().unwrap().into();
                entry.name = winner.value["name"].as_str().unwrap().into();
            }
            "alive" => entry.alive = winner.value.as_bool().unwrap(),
            "content" => entry.content = winner.value.as_str().map(str::to_owned),
            "basic" => entry.basic = Some(serde_json::from_value(winner.value.clone())?),
            _ => {}
        }
        if heads.len() > 1
            && heads
                .iter()
                .map(|v| serde_json::to_string(&v.value).unwrap())
                .collect::<BTreeSet<_>>()
                .len()
                > 1
        {
            versions.sort_by(|a, b| a.revision.cmp(&b.revision));
            // Pair records have stable identity even when a third competitor is
            // learned later. No delivery-dependent subset record is persisted.
            for (i, a) in heads.iter().enumerate() {
                for b in &heads[i + 1..] {
                    if a.value != b.value {
                        let aa = ancestors(a, &versions);
                        let ba = ancestors(b, &versions);
                        let bases = versions
                            .iter()
                            .filter(|v| aa.contains(&v.revision) && ba.contains(&v.revision))
                            .cloned()
                            .collect();
                        p.conflicts.push(make_conflict(
                            branch,
                            entity,
                            field,
                            "concurrent revisions",
                            vec![a.clone(), b.clone()],
                            bases,
                        ));
                    }
                }
            }
        }
        p.heads.insert(key, heads);
    }
    // Make a deterministic, acyclic visible namespace, retaining suppressed
    // entities and their complete registers in conflicts/current-state.
    let ids: Vec<_> = p.entries.keys().cloned().collect();
    let mut slots: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    for entity in &ids {
        let e = &p.entries[entity];
        if e.alive {
            slots
                .entry((e.parent.clone(), name_key(&e.name)?))
                .or_default()
                .push(entity.clone());
        }
    }
    for members in slots.values().filter(|m| m.len() > 1) {
        let mut alternatives = vec![];
        for entity in members {
            alternatives.extend(p.heads[&register(entity, "location")].clone());
        }
        let winner = members.iter().max().unwrap();
        for entity in members {
            if entity != winner {
                p.entries.get_mut(entity).unwrap().alive = false;
            }
        }
        for (i, a) in members.iter().enumerate() {
            for b in &members[i + 1..] {
                let pair = [
                    p.heads[&register(a, "location")].clone(),
                    p.heads[&register(b, "location")].clone(),
                ]
                .concat();
                p.conflicts.push(make_conflict(
                    branch,
                    &format!("{a},{b}"),
                    "namespace",
                    "same name / file-directory collision",
                    pair,
                    vec![],
                ));
            }
        }
    }
    for entity in &ids {
        let mut cursor = entity.clone();
        let mut visited = BTreeSet::new();
        let mut invalid = false;
        while cursor != ROOT {
            if !visited.insert(cursor.clone()) {
                invalid = true;
                break;
            }
            if let Some(e) = p.entries.get(&cursor) {
                if !e.alive || (cursor != *entity && e.kind != "directory") {
                    invalid = true;
                    break;
                }
                cursor = e.parent.clone();
            } else {
                invalid = true;
                break;
            }
        }
        if invalid && p.entries[entity].alive {
            p.entries.get_mut(entity).unwrap().alive = false;
            let alternatives = p
                .heads
                .get(&register(entity, "location"))
                .cloned()
                .unwrap_or_default();
            p.conflicts.push(make_conflict(
                branch,
                entity,
                "namespace",
                "deleted/missing parent or directory cycle",
                alternatives,
                vec![],
            ));
        }
    }
    p.conflicts.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(p)
}
fn make_conflict(
    branch: &str,
    entity: &str,
    field: &str,
    reason: &str,
    mut alternatives: Vec<Version>,
    bases: Vec<Version>,
) -> Conflict {
    alternatives.sort_by(|a, b| a.revision.cmp(&b.revision));
    let revs: Vec<_> = alternatives.iter().map(|v| &v.revision).collect();
    let conflict_id = hash(&serde_json::to_vec(&(branch, entity, field, reason, revs)).unwrap());
    Conflict {
        id: conflict_id,
        branch: branch.into(),
        entity: entity.into(),
        field: field.into(),
        reason: reason.into(),
        alternatives,
        bases,
        resolved_by: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng, rngs::StdRng, seq::SliceRandom};

    /// The Wave 0 derivation, verbatim: the reference the memoized
    /// implementation must reproduce exactly.
    mod legacy {
        use crate::core::*;
        use anyhow::{Result, ensure};
        use rusqlite::{OptionalExtension, params};
        use std::collections::{BTreeMap, BTreeSet};

        fn ancestors(version: &Version, versions: &[Version]) -> BTreeSet<String> {
            let index: BTreeMap<_, _> = versions.iter().map(|v| (v.revision.as_str(), v)).collect();
            let mut found = BTreeSet::new();
            let mut todo = version.parents.clone();
            while let Some(revision) = todo.pop() {
                if found.insert(revision.clone())
                    && let Some(v) = index.get(revision.as_str())
                {
                    todo.extend(v.parents.iter().cloned());
                }
            }
            found
        }
        fn activated_ids(events: &[Event]) -> BTreeSet<String> {
            causal_order(events)
                .into_iter()
                .map(|e| e.id.clone())
                .collect()
        }
        fn causal_order(events: &[Event]) -> Vec<&Event> {
            let by_id: BTreeMap<_, _> = events.iter().map(|e| (e.id.as_str(), e)).collect();
            let mut degrees = BTreeMap::new();
            let mut children: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
            let mut ready = BTreeSet::new();
            for e in events {
                let parents: BTreeSet<_> = e
                    .dependencies
                    .iter()
                    .chain(e.changes.iter().flat_map(|c| c.parents.iter()))
                    .collect();
                degrees.insert(e.id.as_str(), parents.len());
                if parents.is_empty() {
                    ready.insert(e.id.as_str());
                }
                for parent in parents {
                    children
                        .entry(parent.as_str())
                        .or_default()
                        .push(e.id.as_str());
                }
            }
            let mut active = Vec::new();
            while let Some(id) = ready.pop_first() {
                crate::startup::progress("validating causal history");
                active.push(by_id[id]);
                if let Some(children) = children.get(id) {
                    for child in children {
                        let degree = degrees.get_mut(child).unwrap();
                        *degree -= 1;
                        if *degree == 0 {
                            ready.insert(*child);
                        }
                    }
                }
            }
            active
        }
        pub(super) fn namespace_history(branch: &str, events: &[Event]) -> Result<Vec<Conflict>> {
            let graph = FactGraph::events(events);
            let cones = NamespaceCones::new(events)?;
            let mut alive: BTreeMap<&str, Vec<(usize, &Change)>> = BTreeMap::new();
            for (i, e) in events.iter().enumerate() {
                for c in &e.changes {
                    if c.field == "alive" {
                        alive.entry(&c.entity).or_default().push((i, c));
                    }
                }
            }
            let mut conflicts = BTreeMap::new();
            for members in cones.members() {
                crate::startup::progress("rebuilding namespace history");
                let mut relevant = BTreeSet::new();
                for (i, e) in events.iter().enumerate() {
                    if e.changes.iter().any(|c| {
                        members.contains(&c.entity)
                            && (["kind", "location"].contains(&c.field.as_str())
                                || (c.field == "alive"
                                    && (c.value == false
                                        || c.parents.iter().any(|parent| {
                                            graph.index.get(parent.as_str()).is_some_and(|&j| {
                                                events[j].changes.iter().any(|p| {
                                                    p.entity == c.entity
                                                        && p.field == "alive"
                                                        && p.value == false
                                                })
                                            })
                                        }))))
                    }) {
                        relevant.insert(i);
                    }
                }
                let primary: Vec<_> = relevant
                    .iter()
                    .map(|&i| (graph.closure(i, false), graph.closure(i, true)))
                    .collect();
                // Pure TRUE presence assertions cannot change namespace when there is
                // no tombstone. If tombstones exist, retain rank extrema per unbranched
                // TRUE segment and topology/false-revision causal relationship signature.
                // This preserves possible boolean winners without counting every save.
                for entity in &members {
                    let Some(versions) = alive.get(entity.as_str()) else {
                        continue;
                    };
                    let false_ids: Vec<_> = versions
                        .iter()
                        .filter(|(_, c)| c.value == false)
                        .map(|(i, _)| *i)
                        .collect();
                    if false_ids.is_empty() {
                        continue;
                    }
                    let register = FactGraph::register(events, versions);
                    let false_context: Vec<_> = false_ids
                        .iter()
                        .map(|&i| (register.closure(i, false), register.closure(i, true)))
                        .collect();
                    let segments = register.segments();
                    let mut groups: BTreeMap<(usize, Vec<u8>), (usize, usize)> = BTreeMap::new();
                    for &(i, c) in versions {
                        if c.value != true || relevant.contains(&i) {
                            continue;
                        }
                        let signature = primary
                            .iter()
                            .chain(&false_context)
                            .map(|(anc, desc)| {
                                if anc.contains(&i) {
                                    1
                                } else if desc.contains(&i) {
                                    2
                                } else {
                                    0
                                }
                            })
                            .collect();
                        let range = groups.entry((segments[i], signature)).or_insert((i, i));
                        let rank = |j: usize| (&events[j].device, &events[j].id);
                        if rank(i) < rank(range.0) {
                            range.0 = i;
                        }
                        if rank(i) > rank(range.1) {
                            range.1 = i;
                        }
                    }
                    for (min, max) in groups.values() {
                        relevant.extend([*min, *max]);
                    }
                }
                let relevant: Vec<_> = relevant.into_iter().collect();
                let ancestors: Vec<_> = relevant.iter().map(|&i| graph.closure(i, false)).collect();
                let mut antichains: Vec<Vec<usize>> = vec![vec![]];
                let mut competing_cuts = 0;
                for (i, &event) in relevant.iter().enumerate() {
                    let before = antichains.len();
                    for index in 0..before {
                        crate::startup::progress("replaying namespace cuts");
                        if !antichains[index].iter().all(|&j| {
                            !ancestors[i].contains(&relevant[j]) && !ancestors[j].contains(&event)
                        }) {
                            continue;
                        }
                        let mut selected = antichains[index].clone();
                        selected.push(i);
                        if selected.len() > 1 {
                            competing_cuts += 1;
                            ensure!(
                                competing_cuts <= MAX_NAMESPACE_CUTS,
                                "NAMESPACE_HISTORY_LIMIT: more than {MAX_NAMESPACE_CUTS} competing topology cuts in one dependency cone; commit refused without discarding conflicts"
                            );
                        }
                        let mut ids = BTreeSet::new();
                        for &j in &selected {
                            ids.extend(&ancestors[j]);
                            ids.insert(relevant[j]);
                        }
                        let cut: Vec<_> = ids.into_iter().map(|j| events[j].clone()).collect();
                        for c in project(branch, &cut)?.conflicts.into_iter().filter(|c| {
                            c.field == "namespace"
                                && c.entity.split(',').any(|e| members.contains(e))
                        }) {
                            conflicts.insert(c.id.clone(), c);
                        }
                        antichains.push(selected);
                    }
                }
            }
            Ok(conflicts.into_values().collect())
        }
        struct FactGraph {
            index: BTreeMap<String, usize>,
            parents: Vec<Vec<usize>>,
            children: Vec<Vec<usize>>,
        }
        impl FactGraph {
            fn events(events: &[Event]) -> Self {
                let index: BTreeMap<_, _> = events
                    .iter()
                    .enumerate()
                    .map(|(i, e)| (e.id.clone(), i))
                    .collect();
                let parents: Vec<_> = events
                    .iter()
                    .map(|e| {
                        e.dependencies
                            .iter()
                            .chain(e.changes.iter().flat_map(|c| c.parents.iter()))
                            .filter_map(|p| index.get(p).copied())
                            .collect::<BTreeSet<_>>()
                            .into_iter()
                            .collect()
                    })
                    .collect();
                Self::from_parents(index, parents)
            }
            fn register(events: &[Event], versions: &[(usize, &Change)]) -> Self {
                let index: BTreeMap<_, _> = versions
                    .iter()
                    .map(|(i, _)| (events[*i].id.clone(), *i))
                    .collect();
                let mut parents = vec![vec![]; events.len()];
                for &(i, c) in versions {
                    parents[i] = c
                        .parents
                        .iter()
                        .filter_map(|p| index.get(p).copied())
                        .collect();
                }
                let graph = Self::from_parents(index.clone(), parents);
                // Explicit review can list many ancestors as parents. Such redundant
                // edges are evidence, not new branches in an unchanged-TRUE segment.
                // Reduce only this analysis graph; immutable event parents stay intact.
                let mut degree: Vec<_> = graph.parents.iter().map(Vec::len).collect();
                let mut ready: Vec<_> = (0..degree.len()).filter(|&i| degree[i] == 0).collect();
                let mut rank = vec![0; degree.len()];
                let mut sequence = 0;
                while let Some(i) = ready.pop() {
                    rank[i] = sequence;
                    sequence += 1;
                    for &child in &graph.children[i] {
                        degree[child] -= 1;
                        if degree[child] == 0 {
                            ready.push(child);
                        }
                    }
                }
                let mut reduced = graph.parents.clone();
                for ps in &mut reduced {
                    if ps.len() < 2 {
                        continue;
                    }
                    ps.sort_by_key(|&p| std::cmp::Reverse(rank[p]));
                    let mut covered = BTreeSet::new();
                    ps.retain(|&p| {
                        if covered.contains(&p) {
                            false
                        } else {
                            covered.extend(graph.closure(p, false));
                            true
                        }
                    });
                }
                Self::from_parents(index, reduced)
            }
            fn from_parents(index: BTreeMap<String, usize>, parents: Vec<Vec<usize>>) -> Self {
                let mut children = vec![vec![]; parents.len()];
                for (i, ps) in parents.iter().enumerate() {
                    for &p in ps {
                        children[p].push(i);
                    }
                }
                Self {
                    index,
                    parents,
                    children,
                }
            }
            fn closure(&self, id: usize, forward: bool) -> BTreeSet<usize> {
                let edges = if forward {
                    &self.children
                } else {
                    &self.parents
                };
                let mut found = BTreeSet::new();
                let mut todo = edges[id].clone();
                while let Some(i) = todo.pop() {
                    if found.insert(i) {
                        todo.extend(&edges[i]);
                    }
                }
                found
            }
            fn segments(&self) -> Vec<usize> {
                let mut degree: Vec<_> = self.parents.iter().map(Vec::len).collect();
                let mut ready: Vec<_> = (0..degree.len()).filter(|&i| degree[i] == 0).collect();
                let mut segment: Vec<_> = (0..degree.len()).collect();
                while let Some(i) = ready.pop() {
                    if self.parents[i].len() == 1 && self.children[self.parents[i][0]].len() == 1 {
                        segment[i] = segment[self.parents[i][0]];
                    }
                    for &child in &self.children[i] {
                        degree[child] -= 1;
                        if degree[child] == 0 {
                            ready.push(child);
                        }
                    }
                }
                segment
            }
        }
        struct NamespaceCones {
            parents: BTreeMap<String, BTreeSet<String>>,
            entity_slots: BTreeMap<String, BTreeSet<(String, String)>>,
            slots: BTreeMap<(String, String), BTreeSet<String>>,
        }
        impl NamespaceCones {
            fn new(events: &[Event]) -> Result<Self> {
                let mut result = Self {
                    parents: BTreeMap::new(),
                    entity_slots: BTreeMap::new(),
                    slots: BTreeMap::new(),
                };
                for e in events {
                    for c in &e.changes {
                        result.parents.entry(c.entity.clone()).or_default();
                        if c.field == "location" {
                            let parent = c.value["parent"].as_str().unwrap();
                            if parent != ROOT {
                                result
                                    .parents
                                    .entry(c.entity.clone())
                                    .or_default()
                                    .insert(parent.to_owned());
                                result.parents.entry(parent.to_owned()).or_default();
                            }
                            let key = (
                                parent.to_owned(),
                                name_key(c.value["name"].as_str().unwrap())?,
                            );
                            result
                                .entity_slots
                                .entry(c.entity.clone())
                                .or_default()
                                .insert(key.clone());
                            result
                                .slots
                                .entry(key)
                                .or_default()
                                .insert(c.entity.clone());
                        }
                    }
                }
                Ok(result)
            }
            fn members(&self) -> BTreeSet<BTreeSet<String>> {
                let mut cones = BTreeSet::new();
                for entity in self.parents.keys() {
                    crate::startup::progress("analyzing namespace dependencies");
                    let mut members = BTreeSet::new();
                    let mut visited_slots = BTreeSet::new();
                    let mut pending = vec![entity.clone()];
                    while let Some(next) = pending.pop() {
                        if !members.insert(next.clone()) {
                            continue;
                        }
                        pending.extend(self.parents[&next].iter().cloned());
                        if let Some(slots) = self.entity_slots.get(&next) {
                            for slot in slots {
                                if visited_slots.insert(slot.clone()) {
                                    pending.extend(self.slots[slot].iter().cloned());
                                }
                            }
                        }
                    }
                    // Ancestors and slot competitors can affect this entry's visibility.
                    // Sharing an ancestor alone does not make sibling histories interact.
                    cones.insert(members);
                }
                cones
            }
        }

        /// Complete topological activation followed by per-register ancestry. Unknown
        /// parents keep the entire atomic event pending; child-before-parent is durable.
        pub(super) fn project(branch: &str, events: &[Event]) -> Result<Projection> {
            let by_id: BTreeMap<_, _> = events.iter().map(|e| (e.id.clone(), e)).collect();
            for e in events {
                for c in &e.changes {
                    for parent in &c.parents {
                        if let Some(parent) = by_id.get(parent) {
                            ensure!(
                                parent
                                    .changes
                                    .iter()
                                    .any(|pc| pc.entity == c.entity && pc.field == c.field),
                                "INVALID_CAUSAL_PARENT"
                            );
                        }
                    }
                }
            }
            let active = activated_ids(events);
            // A known closed dependency cycle is malformed, rather than permanently pending.
            let pending: Vec<_> = by_id
                .keys()
                .filter(|e| !active.contains(*e))
                .cloned()
                .collect();
            if !pending.is_empty() {
                let has_missing = events.iter().any(|e| {
                    e.dependencies.iter().any(|p| !by_id.contains_key(p))
                        || e.changes
                            .iter()
                            .any(|c| c.parents.iter().any(|p| !by_id.contains_key(p)))
                });
                ensure!(has_missing, "CAUSAL_CYCLE");
            }
            let mut registers: BTreeMap<String, Vec<Version>> = BTreeMap::new();
            for e in events.iter().filter(|e| active.contains(&e.id)) {
                for c in &e.changes {
                    registers
                        .entry(register(&c.entity, &c.field))
                        .or_default()
                        .push(Version {
                            revision: e.id.clone(),
                            device: e.device.clone(),
                            created_ms: e.created_ms,
                            parents: c.parents.clone(),
                            value: c.value.clone(),
                        });
                }
            }
            let mut p = Projection {
                pending,
                ..Default::default()
            };
            for (key, mut versions) in registers {
                crate::startup::progress("projecting registers");
                // All ancestors are dominated before the stable concurrent rank is used.
                let dominated: BTreeSet<_> = versions
                    .iter()
                    .flat_map(|v| v.parents.iter().cloned())
                    .collect();
                let mut heads: Vec<_> = versions
                    .iter()
                    .filter(|v| !dominated.contains(&v.revision))
                    .cloned()
                    .collect();
                heads.sort_by(|a, b| (&a.device, &a.revision).cmp(&(&b.device, &b.revision)));
                let winner = heads.last().unwrap();
                let (entity, field) = key.rsplit_once('/').unwrap();
                let entry = p.entries.entry(entity.into()).or_insert_with(|| Entry {
                    id: entity.into(),
                    ..Default::default()
                });
                entry
                    .revisions
                    .insert(field.into(), winner.revision.clone());
                if field != "basic" {
                    entry.modified_ms = entry.modified_ms.max(winner.created_ms);
                }
                match field {
                    "kind" => {
                        entry.kind = winner.value.as_str().unwrap().into();
                        entry.created_ms = winner.created_ms;
                    }
                    "location" => {
                        entry.parent = winner.value["parent"].as_str().unwrap().into();
                        entry.name = winner.value["name"].as_str().unwrap().into();
                    }
                    "alive" => entry.alive = winner.value.as_bool().unwrap(),
                    "content" => entry.content = winner.value.as_str().map(str::to_owned),
                    "basic" => entry.basic = Some(serde_json::from_value(winner.value.clone())?),
                    _ => {}
                }
                if heads.len() > 1
                    && heads
                        .iter()
                        .map(|v| serde_json::to_string(&v.value).unwrap())
                        .collect::<BTreeSet<_>>()
                        .len()
                        > 1
                {
                    versions.sort_by(|a, b| a.revision.cmp(&b.revision));
                    // Pair records have stable identity even when a third competitor is
                    // learned later. No delivery-dependent subset record is persisted.
                    for (i, a) in heads.iter().enumerate() {
                        for b in &heads[i + 1..] {
                            if a.value != b.value {
                                let aa = ancestors(a, &versions);
                                let ba = ancestors(b, &versions);
                                let bases = versions
                                    .iter()
                                    .filter(|v| {
                                        aa.contains(&v.revision) && ba.contains(&v.revision)
                                    })
                                    .cloned()
                                    .collect();
                                p.conflicts.push(make_conflict(
                                    branch,
                                    entity,
                                    field,
                                    "concurrent revisions",
                                    vec![a.clone(), b.clone()],
                                    bases,
                                ));
                            }
                        }
                    }
                }
                p.heads.insert(key, heads);
            }
            // Make a deterministic, acyclic visible namespace, retaining suppressed
            // entities and their complete registers in conflicts/current-state.
            let ids: Vec<_> = p.entries.keys().cloned().collect();
            let mut slots: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
            for entity in &ids {
                let e = &p.entries[entity];
                if e.alive {
                    slots
                        .entry((e.parent.clone(), name_key(&e.name)?))
                        .or_default()
                        .push(entity.clone());
                }
            }
            for members in slots.values().filter(|m| m.len() > 1) {
                let mut alternatives = vec![];
                for entity in members {
                    alternatives.extend(p.heads[&register(entity, "location")].clone());
                }
                let winner = members.iter().max().unwrap();
                for entity in members {
                    if entity != winner {
                        p.entries.get_mut(entity).unwrap().alive = false;
                    }
                }
                for (i, a) in members.iter().enumerate() {
                    for b in &members[i + 1..] {
                        let pair = [
                            p.heads[&register(a, "location")].clone(),
                            p.heads[&register(b, "location")].clone(),
                        ]
                        .concat();
                        p.conflicts.push(make_conflict(
                            branch,
                            &format!("{a},{b}"),
                            "namespace",
                            "same name / file-directory collision",
                            pair,
                            vec![],
                        ));
                    }
                }
            }
            for entity in &ids {
                let mut cursor = entity.clone();
                let mut visited = BTreeSet::new();
                let mut invalid = false;
                while cursor != ROOT {
                    if !visited.insert(cursor.clone()) {
                        invalid = true;
                        break;
                    }
                    if let Some(e) = p.entries.get(&cursor) {
                        if !e.alive || (cursor != *entity && e.kind != "directory") {
                            invalid = true;
                            break;
                        }
                        cursor = e.parent.clone();
                    } else {
                        invalid = true;
                        break;
                    }
                }
                if invalid && p.entries[entity].alive {
                    p.entries.get_mut(entity).unwrap().alive = false;
                    let alternatives = p
                        .heads
                        .get(&register(entity, "location"))
                        .cloned()
                        .unwrap_or_default();
                    p.conflicts.push(make_conflict(
                        branch,
                        entity,
                        "namespace",
                        "deleted/missing parent or directory cycle",
                        alternatives,
                        vec![],
                    ));
                }
            }
            p.conflicts.sort_by(|a, b| a.id.cmp(&b.id));
            Ok(p)
        }
        fn make_conflict(
            branch: &str,
            entity: &str,
            field: &str,
            reason: &str,
            mut alternatives: Vec<Version>,
            bases: Vec<Version>,
        ) -> Conflict {
            alternatives.sort_by(|a, b| a.revision.cmp(&b.revision));
            let revs: Vec<_> = alternatives.iter().map(|v| &v.revision).collect();
            let conflict_id =
                hash(&serde_json::to_vec(&(branch, entity, field, reason, revs)).unwrap());
            Conflict {
                id: conflict_id,
                branch: branch.into(),
                entity: entity.into(),
                field: field.into(),
                reason: reason.into(),
                alternatives,
                bases,
                resolved_by: None,
            }
        }
        /// The Wave 0 `persist_projection` source list (everything before its
        /// deduplication and writes), verbatim.
        pub(super) fn derive_list(
            tx: &rusqlite::Connection,
            branch: &str,
            p: &Projection,
            events: &[Event],
            new_event: Option<&str>,
        ) -> Result<Vec<Conflict>> {
            // Derive historical concurrent pairs from the full activated DAG too, so a
            // fast-forward delivered before a competitor cannot hide the earlier pair.
            let mut history: BTreeMap<String, Vec<Version>> = BTreeMap::new();
            for e in events.iter().filter(|e| !p.pending.contains(&e.id)) {
                for c in &e.changes {
                    history
                        .entry(register(&c.entity, &c.field))
                        .or_default()
                        .push(Version {
                            revision: e.id.clone(),
                            device: e.device.clone(),
                            created_ms: e.created_ms,
                            parents: c.parents.clone(),
                            value: c.value.clone(),
                        });
                }
            }
            let mut conflicts = p.conflicts.clone();
            if let Some(event) = events.iter().find(|e| Some(e.id.as_str()) == new_event) {
                for reviewed in &event.reviewed {
                    let payload: Option<String> = tx
                        .query_row(
                            "SELECT payload FROM conflicts WHERE id=? AND branch=?",
                            params![reviewed, branch],
                            |r| r.get(0),
                        )
                        .optional()?;
                    if let Some(payload) = payload {
                        conflicts.push(serde_json::from_str(&payload)?);
                    }
                }
            }
            // Namespace collisions also have a deterministic historical basis. A later
            // rename arriving first must not erase evidence of concurrent same-name
            // placement. Sequential temp replacement is ordered by event context.
            let activated: Vec<_> = events
                .iter()
                .filter(|e| !p.pending.contains(&e.id))
                .collect();
            let mut locations = vec![];
            for e in &activated {
                for c in &e.changes {
                    if c.field == "location" {
                        locations.push((*e, c));
                    }
                }
            }
            for (i, (a, ac)) in locations.iter().enumerate() {
                crate::startup::progress("rebuilding namespace conflicts");
                for (b, bc) in &locations[i + 1..] {
                    if ac.entity == bc.entity
                        || ac.value["parent"] != bc.value["parent"]
                        || name_key(ac.value["name"].as_str().unwrap())?
                            != name_key(bc.value["name"].as_str().unwrap())?
                    {
                        continue;
                    }
                    if event_ancestors(a, events).contains(&b.id)
                        || event_ancestors(b, events).contains(&a.id)
                    {
                        continue;
                    }
                    let mut entities = [ac.entity.clone(), bc.entity.clone()];
                    entities.sort();
                    let alternatives = [(*a, *ac), (*b, *bc)]
                        .into_iter()
                        .map(|(e, c)| Version {
                            revision: e.id.clone(),
                            device: e.device.clone(),
                            created_ms: e.created_ms,
                            parents: c.parents.clone(),
                            value: c.value.clone(),
                        })
                        .collect();
                    conflicts.push(make_conflict(
                        branch,
                        &entities.join(","),
                        "namespace",
                        "same name / file-directory collision",
                        alternatives,
                        vec![],
                    ));
                }
            }
            // Explicit reviews carry their observed cut. Recover namespace records from
            // it even when resolution was delivered before its prerequisites.
            for e in &activated {
                if !e.reviewed.is_empty() {
                    let ancestors = event_ancestors(e, events);
                    let cut: Vec<_> = events
                        .iter()
                        .filter(|prior| ancestors.contains(&prior.id))
                        .cloned()
                        .collect();
                    conflicts.extend(project(branch, &cut)?.conflicts);
                }
            }
            // Every namespace causal cut is represented by an antichain of maximal
            // namespace-changing events. Include arbitrary-width concurrency, not just
            // pairs; content-only events add no new namespace projection. The explicit
            // budget refuses a commit before acknowledgement rather than losing history.
            conflicts.extend(namespace_history(branch, events)?);
            for (key, versions) in &history {
                crate::startup::progress("rebuilding register conflicts");
                let candidate =
                    new_event.and_then(|id| versions.iter().position(|v| v.revision == id));
                if new_event.is_some() && candidate.is_none() {
                    continue;
                }
                // The overwhelmingly common single-parent chain has no historical
                // concurrency. Detect it in O(revisions), without enumerating pairs.
                if linear_versions(versions) {
                    continue;
                }
                let reach = VersionReach::new(versions);
                let (entity, field) = key.rsplit_once('/').unwrap();
                // Appending a validated event cannot change old/old register ancestry.
                // Keep their durable records and derive only new/old pairs on append.
                // Startup/repair still derives the entire historical set from the DAG.
                let pairs: Box<dyn Iterator<Item = (usize, usize)>> = if let Some(i) = candidate {
                    Box::new(
                        (0..versions.len())
                            .filter(move |&j| j != i)
                            .map(move |j| (i, j)),
                    )
                } else {
                    Box::new(
                        (0..versions.len())
                            .flat_map(|i| ((i + 1)..versions.len()).map(move |j| (i, j))),
                    )
                };
                for (i, j) in pairs {
                    crate::startup::progress("rebuilding register conflicts");
                    let a = &versions[i];
                    let b = &versions[j];
                    if a.value != b.value && !reach.has(i, j) && !reach.has(j, i) {
                        let mut bases: Vec<_> = versions
                            .iter()
                            .enumerate()
                            .filter(|(k, _)| reach.has(i, *k) && reach.has(j, *k))
                            .map(|(_, v)| v)
                            .cloned()
                            .collect();
                        bases.sort_by(|a, b| a.revision.cmp(&b.revision));
                        conflicts.push(make_conflict(
                            branch,
                            entity,
                            field,
                            "concurrent revisions",
                            vec![a.clone(), b.clone()],
                            bases,
                        ));
                    }
                }
            }
            Ok(conflicts)
        }
        fn event_ancestors(event: &Event, events: &[Event]) -> BTreeSet<String> {
            let index: BTreeMap<_, _> = events.iter().map(|e| (e.id.as_str(), e)).collect();
            let mut seen = BTreeSet::new();
            let mut todo = event.dependencies.clone();
            todo.extend(event.changes.iter().flat_map(|c| c.parents.iter().cloned()));
            while let Some(revision) = todo.pop() {
                if seen.insert(revision.clone())
                    && let Some(e) = index.get(revision.as_str())
                {
                    todo.extend(e.dependencies.iter().cloned());
                    todo.extend(e.changes.iter().flat_map(|c| c.parents.iter().cloned()));
                }
            }
            seen
        }
        fn linear_versions(versions: &[Version]) -> bool {
            let mut children = BTreeSet::new();
            let mut roots = 0;
            for v in versions {
                if v.parents.is_empty() {
                    roots += 1;
                }
                if v.parents.len() > 1 || v.parents.iter().any(|p| !children.insert(p)) {
                    return false;
                }
            }
            roots == 1
        }
        struct VersionReach {
            rows: Vec<Vec<u64>>,
        }
        impl VersionReach {
            fn new(versions: &[Version]) -> Self {
                let index: BTreeMap<_, _> = versions
                    .iter()
                    .enumerate()
                    .map(|(i, v)| (v.revision.as_str(), i))
                    .collect();
                let mut children = vec![vec![]; versions.len()];
                let mut degree = vec![0; versions.len()];
                let mut parents = vec![vec![]; versions.len()];
                for (i, v) in versions.iter().enumerate() {
                    for parent in &v.parents {
                        if let Some(&j) = index.get(parent.as_str()) {
                            parents[i].push(j);
                            children[j].push(i);
                            degree[i] += 1;
                        }
                    }
                }
                let mut ready: Vec<_> = (0..versions.len()).filter(|&i| degree[i] == 0).collect();
                let mut rows = vec![vec![0; versions.len().div_ceil(64)]; versions.len()];
                while let Some(i) = ready.pop() {
                    for &parent in &parents[i] {
                        for word in 0..rows[i].len() {
                            rows[i][word] |= rows[parent][word];
                        }
                        rows[i][parent / 64] |= 1 << (parent % 64);
                    }
                    for &child in &children[i] {
                        degree[child] -= 1;
                        if degree[child] == 0 {
                            ready.push(child);
                        }
                    }
                }
                Self { rows }
            }
            fn has(&self, child: usize, parent: usize) -> bool {
                self.rows[child][parent / 64] & (1 << (parent % 64)) != 0
            }
        }
    }

    const BRANCH: &str = "fuzz-branch";

    /// A random causal history over a small entity pool. Every event depends
    /// on random earlier events and changes one or two registers, parenting
    /// random earlier versions of the same register (so concurrency, merges,
    /// tombstones, resurrections, slot collisions and directory cycles all
    /// occur). At most one location change per event.
    fn history(rng: &mut StdRng, len: usize) -> Vec<Event> {
        let entities = ["d1", "d2", "f1", "f2", "f3"];
        let mut events: Vec<Event> = vec![];
        let mut versions: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
        for n in 0..len {
            let id = format!("{:04x}-{n:02}", rng.gen_range(0..0x10000));
            let mut dependencies = BTreeSet::new();
            for _ in 0..rng.gen_range(0..=2) {
                if !events.is_empty() {
                    // Mostly recent parents: moderate concurrency.
                    let back = rng.gen_range(1..=events.len().min(4));
                    dependencies.insert(events[events.len() - back].id.clone());
                }
            }
            let mut changes: Vec<Change> = vec![];
            let mut location = false;
            let fresh: Vec<&str> = entities
                .iter()
                .copied()
                .filter(|e| !versions.contains_key(&(e.to_string(), "kind".to_string())))
                .collect();
            if !fresh.is_empty() && rng.gen_bool(0.5) {
                // A creation: kind, location and presence, unparented.
                let entity = fresh[rng.gen_range(0..fresh.len())];
                let parent = [ROOT, "d1", "d2"][rng.gen_range(0..3)];
                let name = ["a", "A", "b", "c"][rng.gen_range(0..4)];
                let kind = if entity.starts_with('d') {
                    "directory"
                } else {
                    "file"
                };
                for (field, value) in [
                    ("kind", json!(kind)),
                    ("location", json!({"parent": parent, "name": name})),
                    ("alive", json!(true)),
                ] {
                    changes.push(Change {
                        entity: entity.into(),
                        field: field.into(),
                        parents: vec![],
                        value,
                    });
                }
                location = true;
            }
            for _ in 0..rng.gen_range(usize::from(location)..=2) {
                let entity = entities[rng.gen_range(0..entities.len())];
                if !versions.contains_key(&(entity.to_string(), "kind".to_string())) {
                    continue;
                }
                let field = match rng.gen_range(0..20) {
                    0..=7 if !location => "location",
                    0..=14 => "alive",
                    15..=17 => "kind",
                    _ => "content",
                };
                if changes
                    .iter()
                    .any(|c| c.entity == entity && c.field == field)
                {
                    continue;
                }
                location |= field == "location";
                let known = versions
                    .get(&(entity.to_owned(), field.to_owned()))
                    .cloned()
                    .unwrap_or_default();
                // Edits parent at least one earlier version (rarely none: a
                // concurrent re-creation of the register).
                let mut parents = BTreeSet::new();
                let wanted = if rng.gen_bool(0.05) {
                    0
                } else {
                    rng.gen_range(1..=2)
                };
                for _ in 0..wanted {
                    if !known.is_empty() {
                        parents.insert(known[rng.gen_range(0..known.len())].clone());
                    }
                }
                let parent = [ROOT, "d1", "d2"][rng.gen_range(0..3)];
                let name = ["a", "A", "b"][rng.gen_range(0..3)];
                let value = match field {
                    "location" => json!({"parent": parent, "name": name}),
                    "alive" => json!(rng.gen_bool(0.7)),
                    "kind" if entity.starts_with('d') => json!("directory"),
                    "kind" => json!(if rng.gen_bool(0.8) {
                        "file"
                    } else {
                        "directory"
                    }),
                    _ => json!(hash(&rng.r#gen::<u64>().to_le_bytes())),
                };
                changes.push(Change {
                    entity: entity.into(),
                    field: field.into(),
                    parents: parents.into_iter().collect(),
                    value,
                });
            }
            if changes.is_empty() {
                continue;
            }
            for c in &changes {
                versions
                    .entry((c.entity.clone(), c.field.clone()))
                    .or_default()
                    .push(id.clone());
            }
            events.push(Event {
                id,
                repo: "repo".into(),
                branch: Branch {
                    id: BRANCH.into(),
                    name: "main".into(),
                    shared: true,
                },
                device: ["dev-a", "dev-b", "dev-c"][rng.gen_range(0..3)].into(),
                created_ms: rng.gen_range(0..1000),
                changes,
                dependencies: dependencies.into_iter().collect(),
                reviewed: vec![],
                object_sizes: BTreeMap::new(),
            });
        }
        events
    }

    /// A random history as replicas make it: each event depends on its
    /// device's frontier and parents the heads of every register it changes,
    /// as that device sees them; devices sync now and then. So devices build
    /// long TRUE presence chains (saves) concurrent with tombstones, edit
    /// deleted directories, resurrect, and rename into occupied slots.
    fn device_history(rng: &mut StdRng, len: usize) -> Vec<Event> {
        device_history_with(rng, len, false)
    }

    /// `focused`: five devices and two files competing for one slot, mostly
    /// saves and deletes: TRUE chains that span devices around concurrent
    /// tombstones (compressed presence with rank orders that differ from
    /// chain order).
    fn device_history_with(rng: &mut StdRng, len: usize, focused: bool) -> Vec<Event> {
        let devices: &[&str] = if focused {
            &["dev-a", "dev-b", "dev-c", "dev-d", "dev-e"]
        } else {
            &["dev-a", "dev-b", "dev-c"]
        };
        let entities: &[(&str, &str)] = if focused {
            &[("f1", "file"), ("f2", "file"), ("d1", "directory")]
        } else {
            &[
                ("d1", "directory"),
                ("d2", "directory"),
                ("f1", "file"),
                ("f2", "file"),
                ("f3", "file"),
            ]
        };
        let mut events: Vec<Event> = vec![];
        let mut seen: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); devices.len()];
        let mut serial = 0;
        while events.len() < len && serial < len * 4 {
            serial += 1;
            let d = rng.gen_range(0..devices.len());
            if rng.gen_bool(if focused { 0.35 } else { 0.2 }) {
                let other = seen[rng.gen_range(0..devices.len())].clone();
                seen[d].extend(other);
            }
            let view = &seen[d];
            let heads = |entity: &str, field: &str| -> Vec<String> {
                let versions: Vec<&Event> = view
                    .iter()
                    .map(|&i| &events[i])
                    .filter(|e| {
                        e.changes
                            .iter()
                            .any(|c| c.entity == entity && c.field == field)
                    })
                    .collect();
                let dominated: BTreeSet<&str> = versions
                    .iter()
                    .flat_map(|e| &e.changes)
                    .filter(|c| c.entity == entity && c.field == field)
                    .flat_map(|c| c.parents.iter().map(String::as_str))
                    .collect();
                versions
                    .iter()
                    .filter(|e| !dominated.contains(e.id.as_str()))
                    .map(|e| e.id.clone())
                    .collect()
            };
            let change = |entity: &str, field: &str, value: Value| Change {
                entity: entity.into(),
                field: field.into(),
                parents: heads(entity, field),
                value,
            };
            let (entity, kind) = entities[rng.gen_range(0..entities.len())];
            let parent = if kind == "file" && rng.gen_bool(0.5) {
                ["d1", "d2"][rng.gen_range(0..2)]
            } else {
                ROOT
            };
            let name = if focused && rng.gen_bool(0.8) {
                "a"
            } else {
                ["a", "A", "b"][rng.gen_range(0..3)]
            };
            let location = json!({"parent": parent, "name": name});
            let content = json!(hash(&rng.r#gen::<u64>().to_le_bytes()));
            let changes = if heads(entity, "kind").is_empty() {
                if !rng.gen_bool(0.6) {
                    continue;
                }
                vec![
                    change(entity, "kind", json!(kind)),
                    change(entity, "location", location),
                    change(entity, "alive", json!(true)),
                ]
            } else {
                match rng.gen_range(0..10) {
                    // A save (resurrects over a tombstone).
                    n if n <= 4 || (focused && n <= 6) => {
                        let mut changes = vec![change(entity, "alive", json!(true))];
                        if kind == "file" {
                            changes.push(change(entity, "content", content));
                        }
                        changes
                    }
                    n if n <= 6 || (focused && n <= 8) => {
                        vec![change(entity, "alive", json!(false))]
                    }
                    _ => vec![change(entity, "location", location)],
                }
            };
            let referenced: BTreeSet<&str> = view
                .iter()
                .flat_map(|&i| {
                    events[i]
                        .dependencies
                        .iter()
                        .chain(events[i].changes.iter().flat_map(|c| c.parents.iter()))
                        .map(String::as_str)
                })
                .collect();
            let dependencies: Vec<String> = view
                .iter()
                .map(|&i| events[i].id.clone())
                .filter(|id| !referenced.contains(id.as_str()))
                .collect();
            let id = format!("{:04x}-{:03}", rng.gen_range(0..0x10000), events.len());
            seen[d].insert(events.len());
            events.push(Event {
                id,
                repo: "repo".into(),
                branch: Branch {
                    id: BRANCH.into(),
                    name: "main".into(),
                    shared: true,
                },
                device: devices[d].into(),
                created_ms: serial as u64,
                changes,
                dependencies,
                reviewed: vec![],
                object_sizes: BTreeMap::new(),
            });
        }
        events
    }

    /// The rows a commit leaves: `stored` upserted with `derived` (the last
    /// record of an id wins), after full mode's DELETE of the namespace rows
    /// `derived` lacks when `delete`.
    fn committed(
        stored: &BTreeMap<String, Conflict>,
        derived: &[Conflict],
        delete: bool,
    ) -> BTreeMap<String, Conflict> {
        let derived = by_id(derived);
        let mut rows = stored.clone();
        if delete {
            rows.retain(|id, c| c.field != "namespace" || derived.contains_key(id));
        }
        rows.extend(derived);
        rows
    }

    /// The whole commit, not only the namespace history: with the stored rows
    /// of a rebuild of `events[..k]` from no rows (location pairs, reviewed
    /// cuts, the current projection's records, namespace history, register
    /// pairs), appending `events[k]` leaves exactly the rows of a full-mode
    /// commit, whose namespace DELETE is applied; refusals happen in both or
    /// neither, with the same error. Every prefix of random histories from
    /// three generators (`TKFS_DERIVE_PREFIX_SEEDS`, default 90; release runs
    /// take more), and the crafted `current_record_outside_history`, which
    /// must fall back.
    #[test]
    fn append_commit_equals_full_commit_with_delete_on_every_prefix() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE conflicts(id TEXT PRIMARY KEY, branch TEXT NOT NULL, payload TEXT NOT NULL, resolved_by TEXT);")
            .unwrap();
        let mut appended = 0;
        let mut fallbacks: BTreeMap<&str, usize> = BTreeMap::new();
        let seeds: u64 = std::env::var("TKFS_DERIVE_PREFIX_SEEDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(90);
        let histories = (0..seeds)
            .map(|seed| {
                let mut rng = StdRng::seed_from_u64(7000 + seed);
                let events = match seed % 3 {
                    0 => {
                        let len = rng.gen_range(4..=16);
                        history(&mut rng, len)
                    }
                    1 => {
                        let len = rng.gen_range(8..=24);
                        device_history(&mut rng, len)
                    }
                    _ => {
                        let len = rng.gen_range(8..=20);
                        device_history_with(&mut rng, len, true)
                    }
                };
                (format!("seed {seed}"), events)
            })
            .chain([("crafted".to_string(), current_record_outside_history())]);
        for (name, events) in histories {
            for k in 1..events.len() {
                let (old, new) = (&events[..k], &events[..=k]);
                let (Ok(p_old), Ok(p_new)) = (project(BRANCH, old), project(BRANCH, new)) else {
                    continue;
                };
                if !p_new.pending.is_empty() {
                    continue;
                }
                // Only accepted (rebuildable) histories have stored rows.
                let Ok(stored) = derive_full(&db, BRANCH, &p_old, old, None) else {
                    continue;
                };
                let stored = committed(&BTreeMap::new(), &stored, true);
                let id = new[k].id.as_str();
                let full = derive_full(&db, BRANCH, &p_new, new, Some(id));
                match (full, derive_append(&db, BRANCH, &p_new, new, id)) {
                    (_, Ok(Plan::Fallback(reason))) => {
                        *fallbacks.entry(reason).or_default() += 1;
                    }
                    (Ok(full), Ok(Plan::Derived(append))) => {
                        assert_eq!(
                            committed(&stored, &append, false),
                            committed(&stored, &full, true),
                            "{name} k {k}"
                        );
                        appended += 1;
                    }
                    (Err(full), Err(append)) => {
                        assert_eq!(format!("{full:#}"), format!("{append:#}"), "{name} k {k}");
                    }
                    (full, append) => panic!(
                        "{name} k {k}: refusal parity: full {:?} append {:?}",
                        full.err(),
                        append.err()
                    ),
                }
            }
        }
        println!("{appended} appended, fallbacks {fallbacks:?}");
        assert!(
            appended > 400,
            "{appended} appended, fallbacks {fallbacks:?}"
        );
        assert!(
            fallbacks.contains_key("a current namespace record is not derived from history"),
            "{fallbacks:?}"
        );
    }

    fn same<T: PartialEq + std::fmt::Debug>(a: &Result<T>, b: &Result<T>) -> bool {
        match (a, b) {
            (Ok(a), Ok(b)) => a == b,
            (Err(a), Err(b)) => format!("{a:#}") == format!("{b:#}"),
            _ => false,
        }
    }

    fn by_id(conflicts: &[Conflict]) -> BTreeMap<String, Conflict> {
        conflicts
            .iter()
            .map(|c| (c.id.clone(), c.clone()))
            .collect()
    }

    /// Memoized closures, the per-entity register analysis on a compact graph,
    /// the entity index and memoized cut projections reproduce the Wave 0
    /// `namespace_history` and `project` exactly, in every event order.
    #[test]
    fn memoized_derivation_matches_the_legacy_implementation() {
        let mut compared = 0;
        let mut refusals = 0;
        for seed in 0..300 {
            let mut rng = StdRng::seed_from_u64(seed);
            let len = rng.gen_range(4..=16);
            let events = history(&mut rng, len);
            let mut shuffled = events.clone();
            shuffled.shuffle(&mut rng);
            let mut reversed = events.clone();
            reversed.reverse();
            for order in [&events, &shuffled, &reversed] {
                let legacy = legacy::namespace_history(BRANCH, order);
                let current = NsContext::new(BRANCH, order).full();
                assert!(
                    same(&legacy, &current),
                    "seed {seed}: namespace history differs\nlegacy {legacy:#?}\ncurrent {current:#?}"
                );
                refusals += usize::from(legacy.is_err());
                assert!(
                    same(&legacy::project(BRANCH, order), &project(BRANCH, order)),
                    "seed {seed}: projection differs"
                );
                compared += 1;
            }
            // Downward-closed cuts (every prefix of the creation order).
            for k in 1..events.len() {
                assert!(
                    same(
                        &legacy::project(BRANCH, &events[..k]),
                        &project(BRANCH, &events[..k])
                    ),
                    "seed {seed}: projection of prefix {k} differs"
                );
            }
        }
        println!("{compared} orders compared, {refusals} refused");
        assert!(
            compared == 900 && refusals < compared / 4,
            "{refusals} refusals"
        );
    }

    /// The refactored full derivation (memoized slot keys, borrowed cuts, the
    /// register pairs of only the new event's registers, the memoized
    /// namespace history) builds exactly the Wave 0 source list, in order.
    #[test]
    fn full_derivation_list_matches_the_legacy_implementation() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE conflicts(id TEXT PRIMARY KEY, branch TEXT NOT NULL, payload TEXT NOT NULL, resolved_by TEXT);")
            .unwrap();
        let mut compared = 0;
        for seed in 0..200 {
            let mut rng = StdRng::seed_from_u64(5000 + seed);
            let len = rng.gen_range(4..=14);
            let mut events = history(&mut rng, len);
            for e in &mut events {
                if rng.gen_bool(0.15) {
                    // A review: its observed cut is projected too.
                    e.reviewed = vec!["unknown-record".into()];
                }
            }
            let Ok(p) = project(BRANCH, &events) else {
                continue;
            };
            let Some(last) = events.last().map(|e| e.id.clone()) else {
                continue;
            };
            for new_event in [None, Some(last.as_str())] {
                let legacy = legacy::derive_list(&db, BRANCH, &p, &events, new_event);
                let current = derive_full(&db, BRANCH, &p, &events, new_event);
                assert!(
                    same(&legacy, &current),
                    "seed {seed} {new_event:?}
legacy {legacy:#?}
current {current:#?}"
                );
                compared += 1;
            }
        }
        assert!(compared > 300, "{compared}");
    }

    /// The §5.2 theorem as a pure function: whenever the exactness
    /// precondition holds for appending event `k` to `events[..k]`,
    /// `namespace_history(old) ∪ append-derived == namespace_history(new)`,
    /// and a refusal happens in both or neither.
    #[test]
    fn append_plan_reproduces_full_history_on_every_prefix() {
        let mut appended = 0;
        let mut restricted_runs = 0;
        let mut covered = 0;
        let mut partial = 0;
        let mut fallbacks: BTreeMap<&str, usize> = BTreeMap::new();
        for seed in 0..400 {
            let mut rng = StdRng::seed_from_u64(1000 + seed);
            let len = rng.gen_range(4..=16);
            let events = history(&mut rng, len);
            for k in 1..events.len() {
                let old = &events[..k];
                let new = &events[..=k];
                let Ok(before) = NsContext::new(BRANCH, old).full() else {
                    continue;
                };
                if let Some(reason) = append_guard(&Projection::default(), new, k) {
                    *fallbacks.entry(reason).or_insert(0) += 1;
                    continue;
                }
                let after = NsContext::new(BRANCH, new).full();
                let locations = Locations::new(new);
                if let Err(error) = locations.first_error() {
                    assert!(after.is_err(), "seed {seed} k {k}: {error:#}");
                    continue;
                }
                let ctx = NsContext::new(BRANCH, new);
                let plan = match ctx.append_plan(k, &locations) {
                    Ok(Ok(plan)) => plan,
                    Ok(Err(reason)) => {
                        *fallbacks.entry(reason).or_insert(0) += 1;
                        continue;
                    }
                    Err(error) => {
                        assert!(after.is_err(), "seed {seed} k {k}: plan refused: {error:#}");
                        continue;
                    }
                };
                covered += usize::from(plan.covered > 0);
                partial += usize::from(plan.cones.iter().any(|c| c.only.is_some()));
                // Full cut projections always; restricted ones when allowed.
                let mut variants = vec![false];
                if projection_cannot_fail(new, &locations, &ctx.alive) {
                    variants.push(true);
                    restricted_runs += 1;
                }
                for restricted in variants {
                    let derived = ctx.derive_planned(&plan, restricted);
                    match (&after, &derived) {
                        (Ok(after), Ok(derived)) => {
                            let mut union = by_id(&before);
                            union.extend(by_id(derived));
                            assert_eq!(
                                union,
                                by_id(after),
                                "seed {seed} k {k} restricted {restricted}"
                            );
                        }
                        (Err(full), Err(append)) => assert_eq!(
                            format!("{full:#}"),
                            format!("{append:#}"),
                            "seed {seed} k {k} restricted {restricted}"
                        ),
                        _ => panic!(
                            "seed {seed} k {k} restricted {restricted}: refusal parity: full {:?} append {:?}",
                            after.as_ref().err(),
                            derived.as_ref().err()
                        ),
                    }
                }
                appended += 1;
            }
        }
        println!(
            "{appended} appended ({restricted_runs} also restricted, {covered} with grown cones, {partial} with partial old cones), fallbacks {fallbacks:?}"
        );
        // Every path is exercised (fallbacks are rare in random histories;
        // `witness_displacement_falls_back` pins one).
        assert!(
            appended > 1000 && restricted_runs > 500 && covered > 50 && partial > 100,
            "{appended} appended ({restricted_runs} restricted, {covered} grown, {partial} partial), fallbacks {fallbacks:?}"
        );
    }

    /// A cone's records projected from its own registers equal the filtered
    /// records of the full projection, on random downward-closed cuts of
    /// histories where no cut can fail to project; such histories are
    /// recognised (and the others refused) by `projection_cannot_fail`.
    #[test]
    fn restricted_projection_matches_project() {
        let mut compared = 0;
        let mut unsafe_histories = 0;
        for seed in 0..300 {
            let mut rng = StdRng::seed_from_u64(9000 + seed);
            let len = rng.gen_range(4..=16);
            let events = history(&mut rng, len);
            let locations = Locations::new(&events);
            let ctx = NsContext::new(BRANCH, &events);
            if project(BRANCH, &events).is_err()
                || !projection_cannot_fail(&events, &locations, &ctx.alive)
            {
                unsafe_histories += 1;
                continue;
            }
            let cones = NamespaceCones::new(&events).unwrap().members();
            for _ in 0..6 {
                let mut cut = Bits::new(events.len());
                for i in 0..events.len() {
                    if rng.gen_bool(0.3) {
                        cut.union_with(&ctx.closure(i, false));
                        cut.insert(i);
                    }
                }
                let refs: Vec<&Event> = cut.iter().map(|i| &events[i]).collect();
                let full = project_refs(BRANCH, &refs).expect("a cut projects");
                for members in &cones {
                    let members: BTreeSet<&str> = members.iter().map(String::as_str).collect();
                    let expected: Vec<Conflict> = full
                        .conflicts
                        .iter()
                        .filter(|c| {
                            c.field == "namespace"
                                && c.entity.split(',').any(|e| members.contains(e))
                        })
                        .cloned()
                        .collect();
                    let actual = ctx.namespace_records(&members, &cut).unwrap();
                    assert_eq!(actual, expected, "seed {seed} cone {members:?}");
                    compared += 1;
                }
            }
        }
        println!("{compared} cone cuts compared, {unsafe_histories} histories refused");
        assert!(
            compared > 2000 && unsafe_histories > 0,
            "{compared} {unsafe_histories}"
        );
    }

    /// A history whose current projection holds a namespace record that no
    /// cut of the namespace history derives (TRUE presence compression keeps
    /// rank extrema, not the chain's head), and a last event that takes the
    /// record out of the projection: full mode's DELETE removes it.
    ///
    /// `f` and `y` compete for slot `x`. `f`'s presence chain `e1 -> a5 ->
    /// c1` (saves on three devices, concurrent with `f`'s tombstone `b1`) is
    /// one compressed group whose witnesses are the rank extrema `a5` and
    /// `e1`, so its head `c1` is in no cut. `y` was deleted (`a3`) and moved
    /// into `x` (`a4`); `d1` saves `y` on a stale base (concurrent with `a3`)
    /// after seeing `a5` and `b1`. In the whole history both are alive (`c1`
    /// beats `b1`, `d1` beats `a3`): record `f,y`. In every cut where `y` is
    /// alive in `x` (it needs `d1`), `f`'s head is `a5`, which loses to `b1`.
    /// The last event (`c2`, from `c1`'s device) moves `y` out of `x`.
    fn current_record_outside_history() -> Vec<Event> {
        let event = |id: &str, deps: &[&str], changes: Vec<Change>| Event {
            id: id.into(),
            repo: "repo".into(),
            branch: Branch {
                id: BRANCH.into(),
                name: "main".into(),
                shared: true,
            },
            device: id[..1].into(),
            created_ms: 1,
            changes,
            dependencies: deps.iter().map(|d| d.to_string()).collect(),
            reviewed: vec![],
            object_sizes: BTreeMap::new(),
        };
        let change = |entity: &str, field: &str, parents: &[&str], value: Value| Change {
            entity: entity.into(),
            field: field.into(),
            parents: parents.iter().map(|p| p.to_string()).collect(),
            value,
        };
        let place = |name: &str| json!({"parent": ROOT, "name": name});
        vec![
            event(
                "a1",
                &[],
                vec![
                    change("f", "kind", &[], json!("file")),
                    change("f", "location", &[], place("x")),
                    change("f", "alive", &[], json!(true)),
                ],
            ),
            event(
                "a2",
                &["a1"],
                vec![
                    change("y", "kind", &[], json!("file")),
                    change("y", "location", &[], place("z")),
                    change("y", "alive", &[], json!(true)),
                ],
            ),
            event(
                "a3",
                &["a2"],
                vec![change("y", "alive", &["a2"], json!(false))],
            ),
            event(
                "a4",
                &["a3"],
                vec![change("y", "location", &["a2"], place("x"))],
            ),
            event(
                "e1",
                &["a4"],
                vec![change("f", "alive", &["a1"], json!(true))],
            ),
            event(
                "a5",
                &["e1"],
                vec![change("f", "alive", &["e1"], json!(true))],
            ),
            event(
                "b1",
                &["a4"],
                vec![change("f", "alive", &["a1"], json!(false))],
            ),
            event(
                "c1",
                &["a5"],
                vec![change("f", "alive", &["a5"], json!(true))],
            ),
            event(
                "d1",
                &["a5", "b1"],
                vec![change("y", "alive", &["a2"], json!(true))],
            ),
            event(
                "c2",
                &["c1"],
                vec![change("y", "location", &["a4"], place("w"))],
            ),
        ]
    }

    /// Full mode's DELETE removes the record when `c2` moves `y` out of `x`;
    /// append mode falls back instead of keeping it.
    #[test]
    fn current_record_outside_history_falls_back() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE conflicts(id TEXT PRIMARY KEY, branch TEXT NOT NULL, payload TEXT NOT NULL, resolved_by TEXT);")
            .unwrap();
        let events = current_record_outside_history();
        let (old, new) = (&events[..events.len() - 1], &events[..]);
        let (p_old, p_new) = (project(BRANCH, old).unwrap(), project(BRANCH, new).unwrap());
        let record = p_old
            .conflicts
            .iter()
            .find(|c| c.entity == "f,y")
            .expect("f and y collide in the current projection")
            .clone();
        let mut history_only = p_old.clone();
        history_only.conflicts.clear();
        let history = by_id(&derive_full(&db, BRANCH, &history_only, old, None).unwrap());
        assert!(
            !history.contains_key(&record.id),
            "the record is in no history cut"
        );
        let stored = committed(
            &BTreeMap::new(),
            &derive_full(&db, BRANCH, &p_old, old, None).unwrap(),
            true,
        );
        let full = derive_full(&db, BRANCH, &p_new, new, Some("c2")).unwrap();
        let full_rows = committed(&stored, &full, true);
        assert!(stored.contains_key(&record.id) && !full_rows.contains_key(&record.id));
        assert!(!p_new.conflicts.iter().any(|c| c.id == record.id));
        // The cone precondition holds (every old antichain survives); only
        // the current-records check sees that append mode, which never
        // deletes, would keep the row.
        let locations = Locations::new(new);
        let ctx = NsContext::new(BRANCH, new);
        let plan = match ctx.append_plan(new.len() - 1, &locations) {
            Ok(Ok(plan)) => plan,
            _ => panic!("the cone precondition holds"),
        };
        assert!(plan.members.contains("f") && plan.members.contains("y"));
        assert!(matches!(
            derive_append(&db, BRANCH, &p_new, new, "c2"),
            Ok(Plan::Fallback(
                "a current namespace record is not derived from history"
            ))
        ));
    }

    /// Two devices that never sync each create the same `n` names: `n`
    /// unresolved collisions, and the records of a cut grow with the cut, so
    /// an unbounded cut memo would retain about n²/2 records.
    fn collisions(n: usize) -> Vec<Event> {
        let mut events = vec![];
        for (d, device) in ["dev-a", "dev-b"].into_iter().enumerate() {
            let mut previous: Option<String> = None;
            for i in 0..n {
                let id = format!("{d}-{i:04}");
                let entity = format!("e{d}-{i:04}");
                let change = |field: &str, value: Value| Change {
                    entity: entity.clone(),
                    field: field.into(),
                    parents: vec![],
                    value,
                };
                events.push(Event {
                    id: id.clone(),
                    repo: "repo".into(),
                    branch: Branch {
                        id: BRANCH.into(),
                        name: "main".into(),
                        shared: true,
                    },
                    device: device.into(),
                    created_ms: i as u64,
                    changes: vec![
                        change("kind", json!("file")),
                        change(
                            "location",
                            json!({"parent": ROOT, "name": format!("n{i:04}")}),
                        ),
                        change("alive", json!(true)),
                    ],
                    dependencies: previous.into_iter().collect(),
                    reviewed: vec![],
                    object_sizes: BTreeMap::new(),
                });
                previous = Some(id);
            }
        }
        events
    }

    /// The cut memo stops at its byte budget (the estimate covers at least
    /// the retained records' JSON size), and the derivation is unchanged:
    /// equal to the Wave 0 code and to an unbounded memo.
    #[test]
    fn cut_memo_is_bounded_by_retained_bytes() {
        let events = collisions(150);
        let reference = by_id(&legacy::namespace_history(BRANCH, &events).unwrap());
        let unbounded = NsContext::new(BRANCH, &events);
        unbounded.memo.borrow_mut().cut_budget = usize::MAX;
        assert_eq!(by_id(&unbounded.full().unwrap()), reference);
        let budget = 256 * 1024;
        let bounded = NsContext::new(BRANCH, &events);
        bounded.memo.borrow_mut().cut_budget = budget;
        assert_eq!(by_id(&bounded.full().unwrap()), reference);
        let (all, kept) = (unbounded.memo.borrow(), bounded.memo.borrow());
        let records = |memo: &Memo| memo.cuts.values().map(|c| c.len()).sum::<usize>();
        let json = |memo: &Memo| {
            memo.cuts
                .values()
                .flat_map(|c| c.iter())
                .map(|c| serde_json::to_vec(c).unwrap().len())
                .sum::<usize>()
        };
        println!(
            "unbounded: {} records, {} estimated bytes, {} JSON bytes; bounded: {} records, {} bytes",
            records(&all),
            all.cut_bytes,
            json(&all),
            records(&kept),
            kept.cut_bytes
        );
        // About n²/2 records without a bound (each pair's cut holds all the
        // collisions below it).
        assert!(records(&all) > 150 * 150 / 2, "{}", records(&all));
        assert!(all.cut_bytes > 20 * budget && json(&all) <= all.cut_bytes);
        assert!(kept.cut_bytes <= budget && kept.cut_bytes > budget / 2);
        assert!(json(&kept) <= kept.cut_bytes);
    }

    /// Probe (ignored): one full derivation over `TKFS_DERIVE_PROBE_N`
    /// (default 1,200) collisions, or the Wave 0 code with
    /// `TKFS_DERIVE_PROBE_LEGACY=1`; run under `/usr/bin/time -v` for the
    /// peak resident set. The memo never exceeds its budget.
    #[test]
    #[ignore = "WP1.7 memory probe"]
    fn cut_memo_probe_n_collisions() {
        let n = std::env::var("TKFS_DERIVE_PROBE_N")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1200);
        let events = collisions(n);
        let started = std::time::Instant::now();
        if std::env::var("TKFS_DERIVE_PROBE_LEGACY").is_ok_and(|v| v == "1") {
            let records = legacy::namespace_history(BRANCH, &events).unwrap().len();
            println!("legacy: {records} records in {:?}", started.elapsed());
            return;
        }
        let ctx = NsContext::new(BRANCH, &events);
        let records = ctx.full().unwrap().len();
        let memo = ctx.memo.borrow();
        println!(
            "memoized: {records} records in {:?}; memo {} cuts, {} retained records, {} estimated bytes",
            started.elapsed(),
            memo.cuts.len(),
            memo.cuts.values().map(|c| c.len()).sum::<usize>(),
            memo.cut_bytes
        );
        assert!(memo.cut_bytes <= CUT_MEMO_BYTES);
    }

    /// The slot-key memo stays within its byte budget whatever the names,
    /// keeps a name used on every pass, and returns `name_key`'s keys and
    /// error texts.
    #[test]
    fn slot_key_memo_is_bounded_by_bytes() {
        let budget = 64 * 1024;
        let mut keys = SlotKeys::new(budget);
        let computed = Cell::new(0);
        let lookup = |keys: &mut SlotKeys, name: &str| -> SlotKey {
            if let Some(key) = keys.get(name) {
                return key;
            }
            computed.set(computed.get() + 1);
            let key: SlotKey = name_key(name)
                .map(Arc::from)
                .map_err(|error| Arc::from(format!("{error:#}")));
            keys.insert(name, key.clone());
            key
        };
        // Maximum-length names, 1,000 per pass: far more than the budget.
        let long = |i: usize| format!("{i:06}{}", "é".repeat(124));
        for _ in 0..3 {
            for i in 0..1000 {
                let name = long(i);
                let key = lookup(&mut keys, &name);
                assert_eq!(key.as_deref().ok(), name_key(&name).ok().as_deref());
                assert!(keys.bytes() <= budget, "{}", keys.bytes());
                // A hot name, used between every cold one, stays memoized.
                assert_eq!(
                    lookup(&mut keys, "Hot.TXT").as_deref().ok(),
                    Some("hot.txt")
                );
            }
        }
        // 3,000 cold lookups plus the hot name's first one.
        assert_eq!(computed.get(), 3001);
        let error = lookup(&mut keys, "a/b").unwrap_err();
        assert_eq!(&*error, format!("{:#}", name_key("a/b").unwrap_err()));
        assert_eq!(lookup(&mut keys, "a/b").unwrap_err(), error);
    }

    /// A TRUE presence assertion extending a compressed segment past its rank
    /// extreme displaces an old witness: the precondition must refuse it.
    #[test]
    fn witness_displacement_falls_back() {
        let event = |id: &str, device: &str, deps: &[&str], changes: Vec<Change>| Event {
            id: id.into(),
            repo: "repo".into(),
            branch: Branch {
                id: BRANCH.into(),
                name: "main".into(),
                shared: true,
            },
            device: device.into(),
            created_ms: 1,
            changes,
            dependencies: deps.iter().map(|d| d.to_string()).collect(),
            reviewed: vec![],
            object_sizes: BTreeMap::new(),
        };
        let alive = |parents: &[&str], value: bool| Change {
            entity: "f".into(),
            field: "alive".into(),
            parents: parents.iter().map(|p| p.to_string()).collect(),
            value: json!(value),
        };
        let mut seed = vec![
            Change {
                entity: "f".into(),
                field: "kind".into(),
                parents: vec![],
                value: json!("file"),
            },
            Change {
                entity: "f".into(),
                field: "location".into(),
                parents: vec![],
                value: json!({"parent": ROOT, "name": "f"}),
            },
        ];
        seed.push(alive(&[], true));
        let events = vec![
            event("seed", "a", &[], seed),
            event("t", "a", &["seed"], vec![alive(&["seed"], false)]),
            event("w1", "b", &["seed"], vec![alive(&["seed"], true)]),
            event("w2", "b", &["w1"], vec![alive(&["w1"], true)]),
            event("w3", "b", &["w2"], vec![alive(&["w2"], true)]),
        ];
        // w1 and w2 extend the witnesses; w3 displaces w2 (the rank maximum).
        for k in 2..=3 {
            let locations = Locations::new(&events[..=k]);
            let ctx = NsContext::new(BRANCH, &events[..=k]);
            assert!(matches!(ctx.append_plan(k, &locations), Ok(Ok(_))), "{k}");
        }
        let locations = Locations::new(&events);
        let ctx = NsContext::new(BRANCH, &events);
        assert!(matches!(
            ctx.append_plan(4, &locations),
            Ok(Err("a touched cone lost a relevant event"))
        ));
    }

    /// `e` as a successor would carry it after `old`: depending on the whole
    /// frontier of `old` and parenting every head of each register it
    /// changes (so it descends from every event and every version).
    fn successor_shaped(old: &[Event], e: &Event) -> Event {
        let referenced: BTreeSet<&str> = old
            .iter()
            .flat_map(|o| {
                o.dependencies
                    .iter()
                    .chain(o.changes.iter().flat_map(|c| c.parents.iter()))
                    .map(String::as_str)
            })
            .collect();
        let mut dependencies: Vec<String> = old
            .iter()
            .map(|o| o.id.clone())
            .filter(|id| !referenced.contains(id.as_str()))
            .collect();
        dependencies.sort();
        let changes = e
            .changes
            .iter()
            .map(|c| {
                let versions: Vec<(&Event, &Change)> = old
                    .iter()
                    .flat_map(|o| {
                        o.changes
                            .iter()
                            .filter(|oc| oc.entity == c.entity && oc.field == c.field)
                            .map(move |oc| (o, oc))
                    })
                    .collect();
                let dominated: BTreeSet<&str> = versions
                    .iter()
                    .flat_map(|(_, oc)| oc.parents.iter().map(String::as_str))
                    .collect();
                Change {
                    parents: versions
                        .iter()
                        .map(|(o, _)| o.id.clone())
                        .filter(|id| !dominated.contains(id.as_str()))
                        .collect(),
                    ..c.clone()
                }
            })
            .collect();
        Event {
            id: format!("succ-{}", e.id),
            dependencies,
            changes,
            ..e.clone()
        }
    }

    /// CCR-WP1.6-5, as pure functions over random histories (three
    /// generators): for an event shaped like a successor's (after the whole
    /// history, parenting every head), `check_successor_cuts` refuses exactly
    /// when full replay refuses with `NAMESPACE_HISTORY_LIMIT` (same text),
    /// and whenever `successor_may_add_cuts` says it cannot, every cone that
    /// contains a touched entity has no more competing cuts than the cone of
    /// the same entities had before the event.
    #[test]
    fn successor_cut_count_matches_full_replay_and_the_flag_is_sound() {
        let mut compared = 0;
        let mut flagged = 0;
        let mut unflagged_cones = 0;
        let mut refusals = 0;
        let mut small = 0;
        for seed in 0..160u64 {
            let mut rng = StdRng::seed_from_u64(9000 + seed);
            let events = match seed % 3 {
                0 => {
                    let len = rng.gen_range(4..=16);
                    history(&mut rng, len)
                }
                1 => {
                    let len = rng.gen_range(8..=24);
                    device_history(&mut rng, len)
                }
                _ => {
                    let len = rng.gen_range(8..=20);
                    device_history_with(&mut rng, len, true)
                }
            };
            for k in 1..events.len() {
                let old = &events[..k];
                if NsContext::new(BRANCH, old).full().is_err() {
                    continue;
                }
                let mut new = old.to_vec();
                new.push(successor_shaped(old, &events[k]));
                let full = NsContext::new(BRANCH, &new).full();
                let check = check_successor_cuts(BRANCH, &new, k);
                if successor_cones_are_small(&new, k) {
                    small += 1;
                    assert!(
                        !full
                            .as_ref()
                            .is_err_and(|e| format!("{e:#}").contains("NAMESPACE_HISTORY_LIMIT")),
                        "seed {seed} k {k}: a small cone was refused"
                    );
                }
                let limit =
                    |e: &anyhow::Error| format!("{e:#}").contains("NAMESPACE_HISTORY_LIMIT");
                match (&full, &check) {
                    (Err(f), _) if !limit(f) => continue,
                    (Ok(_), Ok(())) => {}
                    (Err(f), Err(c)) => {
                        assert_eq!(format!("{f:#}"), format!("{c:#}"), "seed {seed} k {k}");
                        refusals += 1;
                    }
                    (full, check) => panic!(
                        "seed {seed} k {k}: full {:?} check {:?}",
                        full.as_ref().err(),
                        check.as_ref().err()
                    ),
                }
                compared += 1;
                if successor_may_add_cuts(&new, k) {
                    flagged += 1;
                    continue;
                }
                // Not flagged: no touched cone gains a competing cut.
                assert!(full.is_ok(), "seed {seed} k {k}: unflagged refusal");
                let locations = Locations::new(&new);
                if locations.first_error().is_err() {
                    continue;
                }
                let graph = ConeGraph::new(&new, &locations, k);
                let touched: BTreeSet<&str> = new[k]
                    .changes
                    .iter()
                    .filter(|c| ["kind", "location", "alive"].contains(&c.field.as_str()))
                    .map(|c| c.entity.as_str())
                    .collect();
                let existing: BTreeSet<&str> = old
                    .iter()
                    .flat_map(|e| e.changes.iter().map(|c| c.entity.as_str()))
                    .collect();
                let (ns_new, ns_old) = (NsContext::new(BRANCH, &new), NsContext::new(BRANCH, old));
                for entity in graph.reaching(&touched) {
                    let members = graph.cone(entity, false);
                    let now = ns_new.count_cuts(&ns_new.relevant(&members, None)).unwrap();
                    let before: BTreeSet<&str> = members
                        .iter()
                        .copied()
                        .filter(|m| existing.contains(m))
                        .collect();
                    let then = ns_old.count_cuts(&ns_old.relevant(&before, None)).unwrap();
                    assert!(
                        now <= then,
                        "seed {seed} k {k}: unflagged cone {members:?} grew from {then} to {now} competing cuts"
                    );
                    unflagged_cones += 1;
                }
            }
        }
        println!(
            "{compared} compared, {flagged} flagged, {unflagged_cones} unflagged cones, {refusals} refusals, {small} small"
        );
        assert!(
            compared > 1000 && flagged > 200 && unflagged_cones > 1000,
            "{compared} {flagged} {unflagged_cones}"
        );
    }

    /// `n` devices that never sync each create an entity in one slot: one
    /// cone of `n` concurrent relevant events, 2^n - 1 - n competing cuts.
    fn same_slot(n: usize) -> Vec<Event> {
        (0..n)
            .map(|d| {
                let entity = format!("e{d:02}");
                let change = |field: &str, value: Value| Change {
                    entity: entity.clone(),
                    field: field.into(),
                    parents: vec![],
                    value,
                };
                Event {
                    id: format!("{d:02}"),
                    repo: "repo".into(),
                    branch: Branch {
                        id: BRANCH.into(),
                        name: "main".into(),
                        shared: true,
                    },
                    device: format!("dev-{d:02}"),
                    created_ms: d as u64,
                    changes: vec![
                        change("kind", json!("file")),
                        change("location", json!({"parent": ROOT, "name": "a"})),
                        change("alive", json!(true)),
                    ],
                    dependencies: vec![],
                    reviewed: vec![],
                    object_sizes: BTreeMap::new(),
                }
            })
            .collect()
    }

    /// `count_cuts` counts and refuses exactly as `cone_conflicts` does, on
    /// both sides of the 4,096-cut boundary (12 events: 4,083; 13: 8,178).
    #[test]
    fn count_cuts_matches_the_enumeration_of_cone_conflicts() {
        let mut refused = 0;
        let histories = [1usize, 2, 8]
            .into_iter()
            .map(collisions)
            .chain([3usize, 12, 13].into_iter().map(same_slot));
        for events in histories {
            let n = events.len();
            let ns = NsContext::new(BRANCH, &events);
            let cones = NamespaceCones::new(&events).unwrap();
            for members in cones.members() {
                let members: BTreeSet<&str> = members.iter().map(String::as_str).collect();
                let relevant = ns.relevant(&members, None);
                let mut out = BTreeMap::new();
                let enumerated = ns.cone_conflicts(&members, &relevant, None, false, &mut out);
                let counted = ns.count_cuts(&relevant);
                match (enumerated, counted) {
                    (Ok(()), Ok(count)) => {
                        if members.len() == 12 {
                            assert_eq!(count, 4083);
                        }
                    }
                    (Err(a), Err(b)) => {
                        assert_eq!(format!("{a:#}"), format!("{b:#}"));
                        refused += 1;
                    }
                    (a, b) => panic!("n {n}: {:?} vs {:?}", a.err(), b.err()),
                }
            }
        }
        assert!(refused > 0, "the boundary was crossed");
    }
}
