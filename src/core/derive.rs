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
//! ## Why append mode equals full mode (proof obligation)
//!
//! Notation: `O` = the branch's committed events, `N = O ∪ {E}`. `S` = the
//! stored conflict rows before the commit. Full mode stores
//! `S \ {namespace rows it no longer derives} ∪ D_full`, where `D_full` is
//! `p.conflicts`, the stored rows `E` reviews, all location pairs, all
//! reviewed cuts, `namespace_history(N)` and the register pairs of `E`
//! (deduplicated by id).
//!
//! **Induction hypothesis (H):** `S` is what a rebuild of `O` derives (the
//! oracle, `Store::verify_derived_state`, is exactly this check). In
//! particular the rebuild of `O` succeeds, so every computation over `O`
//! alone succeeds and stays within `MAX_NAMESPACE_CUTS`.
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
//!   `namespace_history(N)`'s, full mode's DELETE removes nothing, and full
//!   mode's refusal (`NAMESPACE_HISTORY_LIMIT`) happens in append mode at the
//!   same cone: the untouched old cones' counts are unchanged and within the
//!   limit (H); every other cone is counted in full.
//! - *Reviews of `E`* are validated against the derived list; a reviewed row
//!   is either stored (and loaded in both modes) or newly derived (derived in
//!   both modes, since everything full mode derives beyond append's list is
//!   already stored, H).
//!
//! So `S ∪ D_append` equals full mode's result (`S` minus nothing, plus
//! `D_full`) row for row, with the same `resolved_by` updates, and the result
//! again satisfies (H) for `N`. When a
//! check fails, append mode derives in full instead, which keeps (H) by
//! definition. The executable check of (H) is the oracle; `verify` mode and
//! `tests/derive_oracle.rs` check the equality itself.
//!
//! ## Memoization
//!
//! Within one derivation (both modes) the causal closures of events are
//! bitsets computed once, the per-entity alive-register analysis is computed
//! once per entity, and each distinct antichain's cut is projected once.
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
//! - Slot keys (`name_key`) are memoized process-wide (`slot_key`).
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
    /// rebuilds), in microseconds.
    pub commit_micros: u64,
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
}
static COUNTERS: [AtomicU64; 7] = [const { AtomicU64::new(0) }; 7];

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

pub(super) fn persist_projection(
    tx: &Connection,
    branch: &str,
    p: &Projection,
    events: &[Event],
    new_event: Option<&str>,
) -> Result<()> {
    tx.execute("DELETE FROM current_parts WHERE branch=?", [branch])?;
    tx.execute("INSERT INTO current_state VALUES(?,?) ON CONFLICT(branch) DO UPDATE SET payload=excluded.payload",params![branch,serde_json::to_string(p)?])?;
    let Some(new_id) = new_event else {
        count(Count::Rebuild);
        return persist_full(tx, branch, p, events, None);
    };
    let started = std::time::Instant::now();
    let result = persist_commit(tx, branch, p, events, new_id);
    add(Count::CommitMicros, started.elapsed().as_micros() as u64);
    result
}

/// A commit's records, in the configured mode.
fn persist_commit(
    tx: &Connection,
    branch: &str,
    p: &Projection,
    events: &[Event],
    new_id: &str,
) -> Result<()> {
    match conflict_mode() {
        ConflictMode::Full => {
            count(Count::Full);
            persist_full(tx, branch, p, events, Some(new_id))
        }
        ConflictMode::Append => match derive_append(tx, branch, p, events, new_id)? {
            Plan::Derived(conflicts) => {
                count(Count::Append);
                write_conflicts(tx, branch, p, events, Some(new_id), conflicts, false)
            }
            Plan::Fallback(reason) => {
                fallback(reason);
                persist_full(tx, branch, p, events, Some(new_id))
            }
        },
        ConflictMode::Verify => persist_verified(tx, branch, p, events, new_id),
    }
}

/// Full derivation: every source over the whole branch history, then the
/// DELETE of namespace rows that are no longer derived.
fn persist_full(
    tx: &Connection,
    branch: &str,
    p: &Projection,
    events: &[Event],
    new_event: Option<&str>,
) -> Result<()> {
    let conflicts = derive_full(tx, branch, p, events, new_event)?;
    write_conflicts(tx, branch, p, events, new_event, conflicts, true)
}

/// Every full-mode source, in the historical order (the deduplication keeps
/// the last record of each id).
fn derive_full(
    tx: &Connection,
    branch: &str,
    p: &Projection,
    events: &[Event],
    new_event: Option<&str>,
) -> Result<Vec<Conflict>> {
    let mut conflicts = p.conflicts.clone();
    conflicts.extend(reviewed_rows(tx, branch, events, new_event)?);
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
    let mut rows = vec![];
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
                rows.push(serde_json::from_str(&payload)?);
            }
        }
    }
    Ok(rows)
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

/// Deduplicate (the last record of an id wins), optionally delete namespace
/// rows that are no longer derived (full mode), upsert, then validate the new
/// event's reviews (every event's, on rebuild) and record their receipts.
fn write_conflicts(
    tx: &Connection,
    branch: &str,
    p: &Projection,
    events: &[Event],
    new_event: Option<&str>,
    conflicts: Vec<Conflict>,
    delete_stale_namespace: bool,
) -> Result<()> {
    let conflicts: Vec<_> = conflicts
        .into_iter()
        .map(|c| (c.id.clone(), c))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect();
    if delete_stale_namespace {
        let namespace_ids: BTreeSet<_> = conflicts
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
        for c in &conflicts {
            insert.execute(params![c.id, branch, serde_json::to_string(c)?])?;
        }
    }
    for e in events {
        if !p.pending.contains(&e.id) && new_event.is_none_or(|id| id == e.id) {
            for reviewed in &e.reviewed {
                let c = conflicts
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
                // Concurrent review events converge on a stable receipt as well.
                tx.execute("UPDATE conflicts SET resolved_by=CASE WHEN resolved_by IS NULL OR resolved_by < ? THEN ? ELSE resolved_by END WHERE id=? AND branch=?",params![e.id,e.id,reviewed,branch])?;
            }
        }
    }
    Ok(())
}

/// The outcome of the append-mode precondition.
enum Plan {
    /// The records append mode derives (written without the namespace DELETE).
    Derived(Vec<Conflict>),
    /// The precondition does not hold: derive in full.
    Fallback(&'static str),
}

/// `name_key`, memoized process-wide: a pure function of the name, needed for
/// every location change of the branch on every append. Errors keep their
/// text.
fn slot_key(name: &str) -> Result<Arc<str>> {
    type Keys = HashMap<String, std::result::Result<Arc<str>, Arc<str>>>;
    static KEYS: OnceLock<Mutex<Keys>> = OnceLock::new();
    let keys = KEYS.get_or_init(Default::default);
    let found = keys
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(name)
        .cloned();
    let key = match found {
        Some(key) => key,
        None => {
            let key = match name_key(name) {
                Ok(key) => Ok(Arc::from(key)),
                Err(error) => Err(Arc::from(format!("{error:#}"))),
            };
            let mut keys = keys.lock().unwrap_or_else(PoisonError::into_inner);
            if keys.len() >= 1 << 18 {
                keys.clear();
            }
            keys.insert(name.to_owned(), key.clone());
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

/// Append mode (§5.2): the records the new event adds, or a fallback reason.
/// Errors are refusals, raised exactly where full mode raises them (module
/// docs): nothing here fails on history that a rebuild accepts.
fn derive_append(
    tx: &Connection,
    branch: &str,
    p: &Projection,
    events: &[Event],
    new_id: &str,
) -> Result<Plan> {
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
    conflicts.extend(reviewed_rows(tx, branch, events, Some(new_id))?);
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
    Ok(Plan::Derived(conflicts))
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

/// Verify mode: append and full in separate savepoints; the full result is
/// kept. Both refusing is a refusal (full mode's error); any other difference
/// is `CONFLICT_MODE_MISMATCH`.
fn persist_verified(
    tx: &Connection,
    branch: &str,
    p: &Projection,
    events: &[Event],
    new_id: &str,
) -> Result<()> {
    let derived = match derive_append(tx, branch, p, events, new_id) {
        Ok(Plan::Fallback(reason)) => {
            fallback(reason);
            return persist_full(tx, branch, p, events, Some(new_id));
        }
        Ok(Plan::Derived(conflicts)) => Ok(conflicts),
        Err(error) => Err(error),
    };
    let append = derived.and_then(|conflicts| {
        in_savepoint(tx, "tkfs_derive_append", false, || {
            write_conflicts(tx, branch, p, events, Some(new_id), conflicts, false)?;
            capture_conflicts(tx, branch)
        })
    });
    let full = in_savepoint(tx, "tkfs_derive_full", true, || {
        persist_full(tx, branch, p, events, Some(new_id))?;
        capture_conflicts(tx, branch)
    });
    match (append, full) {
        (Ok(append), Ok(full)) => {
            let differences = conflict_differences(&append, &full);
            ensure!(
                differences.is_empty(),
                "CONFLICT_MODE_MISMATCH: append and full derivation differ for {new_id}: {}",
                differences.join("; ")
            );
            count(Count::Verified);
            Ok(())
        }
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

fn conflict_differences(append: &ConflictRows, full: &ConflictRows) -> Vec<String> {
    let mut out = vec![];
    for (id, (conflict, resolved)) in append {
        match full.get(id) {
            None => out.push(format!("{id}: only in append")),
            Some((other, other_resolved)) => {
                if conflict != other {
                    out.push(format!("{id}: payload differs"));
                }
                if resolved != other_resolved {
                    out.push(format!(
                        "{id}: resolved_by append {resolved:?} full {other_resolved:?}"
                    ));
                }
            }
        }
    }
    for id in full.keys().filter(|id| !append.contains_key(*id)) {
        out.push(format!("{id}: only in full"));
    }
    if out.len() > 8 {
        let more = out.len() - 8;
        out.truncate(8);
        out.push(format!("{more} more"));
    }
    out
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
/// Bound on memoized cut projections (namespace records only, usually few).
const CUT_MEMO_ENTRIES: usize = 65_536;

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

    /// The namespace records of one antichain's cut (memoized per antichain).
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
        let mut memo = self.memo.borrow_mut();
        if memo.cuts.len() < CUT_MEMO_ENTRIES {
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
        Ok(Ok(AppendPlan {
            cones,
            covered: seen.len(),
        }))
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
}
