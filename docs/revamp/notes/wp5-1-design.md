# WP5.1 design: concurrent-edit activation

Package: WP5.1 (REVAMP_PLAN §14.5b). Inputs: CCR-WP4.INT-3 and CCR-WP4.INT-4
(`docs/revamp/notes/wp4-int.md`), the coordinator's CPU profile of the
WP4.INT soak workers (`prepare_slow → derive_conflicts → ConeGraph::cone →
project_refs`, dominated by `String` clones into `BTreeSet`s).

This note is written before the implementation; *Measurements after* is
filled in at the end (the final numbers are in `wp5-1.md`).

## 1. Where the time goes today

### Method

The baseline is `c6948c9` (the wave-5 base, WP4.INT's final code). Two
instruments:

- **A store-level conflict storm** (`tests/qual_wp5_1_concurrent.rs`,
  `storm::Storm`): two replicas of one folder in one process, each making
  the soak's operation mix (65 % saves or creates, 8 % renames, 8 %
  deletes, 3 % directories) on a shared name pool (`N` names in each of 4
  directories), exchanging with probability `p` per round of one operation
  per side. `N = 24, p = 0.15` reproduces the soak storm's shape (conflicts
  grow with the history: 2.2K unresolved at 4K events, 3.2K at 4.8K; the
  WP4.INT two-hour storm had 9.4K at 9K events). Every receive is inline
  (`Store::receive`), so the CPU of a delivery is the slow path's.
- **Phase timers** (temporary, `derive::timed`): the thread's microseconds
  per phase of `compute_slow` (`s.*`), `derive_append_with` (`a.*`, `p.*`,
  `e.*`), full mode (`f.*`) and the commit's write (`w.*`).

The daemon-level workloads (scenarios (a) and (b)) were run on the baseline
too; see §5.

### Results (baseline, release, Windows, Ryzen 9 7950X3D)

A window of 250 events between 4.5K and 4.8K events, 2.9K–3.2K unresolved
conflicts, 178 slow-path commits, no fallback in the window:

| Phase | ms per slow commit | What |
|---|---|---|
| `s.project` (`project` of the whole branch) | 42 | `project_refs`: every event's id cloned into a `BTreeMap` and a `BTreeSet`, every version of every register cloned (`Version`: id, device, parents, JSON value) |
| `s.derive` (append mode) | 38 | `NsContext::new` 9.7 (index maps keyed by `&str`, per-event parent lookups), `append_plan` 6.8 (`ConeGraph::new`, one `BTreeSet<&str>` per reaching entity, `relevant` twice), location pairs 6.2 (`event_ancestors`: a `BTreeSet<String>` of every ancestor, twice per pair of placements in the new event's slot), the planned cones' cut records 5.5, register pairs 3.4, the guard 2.2 |
| `s.delta` (`projection_delta`) | 12 | the stored projection compared entry by entry, the view slots |
| `w.slow` (under the engine lock) | 31 | the conflict rows: every row of the current projection re-upserted (thousands; serialized, looked up, compared), the `current_meta` header and the rows anchor (each holds the projection's whole conflict list), the forced seal, the fsync |
| total | ~124 | |

Plus the **fallbacks**: 40 of 2,800 append commits (1.4 %) fell back to
full mode, every one with *a touched cone lost a relevant event* (the
event, a save of an entity with a tombstone, displaces an older TRUE
presence witness of the same group from the relevant set). One fallback
cost **53 s** at 4.5K events (13 s at 3K, 22 s at 4K): `f.ns` (full mode's
namespace history) projected every antichain cut of every cone with the
whole `project_refs` (6.5K cuts × 16 ms), plus 6 s of location pairs
(`event_ancestors` per pair). Amortized over the slow commits that is
~0.75 s per slow commit: **the fallbacks dominate**, then `project`, the
derivation and the write.

The window's CPU per received event (all paths) grew from 7 ms at 500 events
to 64–257 ms at 4.5K (more in windows with a fallback). The oracle (a full
rebuild) of the same store costs the same as a fallback, once per branch.

What grows with what:

| Cost | Grows with |
|---|---|
| fallback (full derivation) | antichains of every cone × history (each cut projected in full) |
| `project`, `NsContext::new`, `ConeGraph::new`, `append_guard`, register pairs, `projection_cannot_fail` | history (O(H) with large constants: string clones) |
| location pairs of the new event | placements in its slot × history |
| `append_plan`'s cones | reaching entities × cone size (one `BTreeSet<&str>` each) |
| the counting in touched cones | relevant events × antichains (every antichain re-checked for every event) |
| conflict rows, header, anchor | open conflicts (every slow commit rewrites them) |

The coordinator's profile (`ConeGraph::cone → project_refs`, `String`
clones into `BTreeSet`s) is the fallback's full replay and `project`: both
clone every id of the history into ordered sets.

## 2. Approaches

### A. Faster primitives, same algorithms

Intern entity and event ids as integers (entity numbers in the ids' sort
order, so every order-dependent choice is unchanged), keep causal closures
as bitsets computed in one topological sweep, project cuts from the cone's
registers without cloning (`namespace_records` on integer registers, with
each record built once and memoized), enumerate antichains through
concurrent sets instead of re-checking every antichain for every event,
and make `project` allocation-light. Same functions, same results: every
function is a re-implementation of one that exists, checked against the
Wave 0 reference (`tests::legacy`) and the current one.

- Gains: constant factors (10–100× on the hot paths), full rebuild and
  fallbacks become seconds instead of minutes.
- Leaves: O(H) per commit, fallbacks still re-derive the whole branch,
  every commit still rewrites the open conflicts.

### B. Scoped fallback

When the precondition fails, re-derive in full only the cones the event
touches, and run full mode's DELETE only on the rows they can make stale.

- The difficulty: cones overlap (a directory is in the cone of every entry
  under it), so a stale candidate that mentions a directory can still be
  derived by an untouched cone; proving a candidate stale needs every cone
  that contains its entities, which can be the whole subtree. A correct
  scoped DELETE needs a global view of which cone derives which record.

### C. A memoized model of full mode's namespace history (chosen, on A)

Full mode's namespace history is a union over cones: `D_ns(N) =
∪_C ∪_{A ∈ antichains(rel(C))} records(cut(A), C)`. Keep, per branch, an
in-memory model of that union for the branch's current event set: per
distinct cone its members, relevant events, competing-cut count and, per
antichain, the ids of its records; a reference count per record id; and
the other sources full mode reads (the location-pair records, the records
of reviewed cuts). A commit updates only the cones of the entities that
reach an entity the event touches (the only cones that can change, as
append mode already argues), reusing an antichain's records when its cut
and the cone's members are unchanged. The result is full mode's row set
`D_full(N)` exactly, so the commit writes the new rows and deletes exactly
the rows full mode's DELETE removes: `D_full_ns(O) \ D_full_ns(N)`, read
off the model, no precondition, no fallback.

- Gains: cost follows the touched cones in every case (displacement, grown
  cones, current records out of history); refusal parity by construction
  (exact per-cone counts: CCR-WP4.INT-4); the write knows which rows
  changed.
- Cost: memory (records per antichain, one model per branch, a few
  versions kept for rolled-back or stale commits) and one full derivation
  to build the model when none matches (process start, a new handle after
  other commits, a history the model does not know).

### D. Clock-based incremental derivation

Rejected: a scope cut (§12) and new derived state.

## 3. The chosen design (C on A)

### 3.1 Interned index

`Ix` (per derivation, from `DeriveInput::events`): event positions as now
(the slice order), `HashMap<&str, u32>` for ids, causal parents as `u32`,
entities numbered in sorted order (`ROOT` excluded), per entity its
touching events and alive versions, per register its versions with
version parents as positions, location rows with interned parent entity
and slot key (`slot_key` memo, unchanged). Closures (`anc`, `desc`) are
bitsets, computed lazily per event or in one sweep for a cone, within
`CLOSURE_MEMO_BYTES`.

### 3.2 Exactness of the primitives (against the reference)

- **Entity order.** Entity numbers follow the ids' byte order, so every
  `BTreeSet<&str>` / `BTreeMap<&str, _>` iteration of the reference is the
  same order on numbers; pair records name `a,b` in id order as before.
- **Antichain enumeration order.** The reference builds the antichain
  list `L` by, for each relevant event `i` in order, appending
  `A ∪ {i}` for every earlier antichain `A` compatible with `i`. The
  antichains compatible with `i` are exactly those inside
  `conc_<(i)` (relevant events before `i` that are neither ancestors nor
  descendants of `i`), and in `L` they appear in the order `L(S)` defined
  by `L(S) = [∅] ++ concat_{m ∈ S ascending} [A ∪ {m} : A ∈ L(S ∩ conc_<(m))]`
  (induction on `i`). Enumerating `L(conc_<(i))` recursively gives the
  same list in the same order, visiting only antichains, never the whole
  list per event. The refusal (`competing_cuts > MAX_NAMESPACE_CUTS`) is
  raised at the same antichain.
- **Restricted records in full mode.** `NsContext::namespace_records`
  (append mode's restricted projection) is exact whenever
  `projection_cannot_fail` holds (the existing argument; unit test
  `restricted_projection_matches_project`). Full mode now uses it under the
  same condition, and keeps the whole `project` of each cut otherwise.
- **Location pairs.** `concurrent_placement`'s ancestry test reads the
  event closures instead of `event_ancestors`' string sets; pairs are
  evaluated in the reference order, so a bad name fails at the same pair.
- **`project`.** Same registers, heads, winners, `modified_ms`, conflicts
  (pairs of heads with distinct values, bases from the common ancestors),
  slot and chain passes, built from references: only heads and records are
  cloned.

Every one is compared with `tests::legacy` (Wave 0) on random histories in
every event order, and on storm-shaped histories (new generator: two
devices, a shared name pool, renames and deletes, tombstones and
resurrections).

### 3.3 The model and why it equals full mode (invariant 11)

Notation as in `derive.rs`'s module docs: `O` the branch's committed events,
`N = O ∪ {E}`, `S` the stored rows, (H) `S = rebuild(O)` (the strict
oracle).

Full mode stores `S \ stale ∪ D_full(N)`, with `D_full(N) = p_N.conflicts ∪
reviewed(E) ∪ LP(N) ∪ RC(N) ∪ D_ns(N) ∪ RP(E)` (location pairs, reviewed
cuts, namespace history, register pairs of `E`) and `stale = S_ns \
D_full(N)`. By (H), `S_ns = D_full_ns(O)`, so `stale = D_full_ns(O) \
D_full_ns(N)`: a set difference of two model states.

The model of `O` holds `D_ns(O)` as cone data and reference counts, `LP(O)`
and `RC(O)` (namespace records only; non-namespace rows are never
deleted). Applying `E`:

1. `LP(N) = LP(O) ∪ pairs(E)`: a pair of two older placements has the same
   ancestry in `N` (fact 1, `E` is a sink), so `LP(O) ⊆ LP(N)` and only
   pairs with `E` are new.
2. `RC(N) = RC(O) ∪ cut(E)'s records` when `E` is a review: an older
   review's cut excludes `E`.
3. `D_ns(N)`: a cone of `N` that is the cone of an entity that does not
   reach an entity `E` touches is a cone of `O` with the same members and
   the same relevant set (the argument of `append_plan`: its members' kind,
   location and alive registers are unchanged, and their TRUE-presence
   signatures read only events of `O`, whose ancestry is unchanged), so its
   antichains, cuts and records are the model's. Every other cone of `N` is
   the cone of an entity in `reaching(touched)` (computed on `N`); the model
   recomputes exactly those: members, relevant set (with the compression,
   on `N`), antichains in the reference order, and per antichain its
   records; an antichain whose event set and cone members equal an old
   antichain of the same cone reuses that antichain's records (same cut:
   `E` is in no cut of older events; same filter). Cones of `O` that are no
   longer cones of `N` (an entity's cone grew) leave the union: their
   records' reference counts drop. A record is in `D_ns(N)` iff its count is
   positive.
4. Counting: every recomputed cone is counted in full (its antichains are
   enumerated), every other cone keeps its count, which (H) bounds. The
   first cone over `MAX_NAMESPACE_CUTS` refuses with full mode's error, so a
   refusal happens in both or neither.

So the model's `D_full(N)` is full mode's, row for row, and the write
(upsert of `D_full(N)`'s rows that `S` lacks or holds differently, the
receipts, the DELETE of `stale`) leaves full mode's table. The model is a
pure function of the event set: it is found by a fingerprint of the
branch's event ids (count and a 128-bit hash), per store root, so a commit
that rolled back or went stale never corrupts it (the previous version is
kept), and a mismatch builds a fresh model with one full derivation.
Restricted records are a precondition: when `projection_cannot_fail` does
not hold for `N`, the commit derives in full (the reference) instead.

**When it falls back to full mode:** the structural guards of
`append_guard` (pending events, `E` a parent of a committed event, a comma
in an entity id, several relocations in one event, two events relocating
the same entities: the ambiguity guards of fact 3, under which a record id
determines its payload), a branch on which a projection can fail
(`projection_cannot_fail`), and `TKFS_CONFLICT_MODE=full`. `verify` runs the
model and full mode and compares (`CONFLICT_MODE_MISMATCH`), as today; the
fuzz checks every commit against a derivation from no rows.

### 3.4 Per-commit write

Rows already in `S` with the same payload are not written again (an upsert
whose `WHERE payload<>excluded.payload` would skip them anyway, after
serializing and looking each up): the derivation knows them (the old
projection's records, the model's old set). The `current_meta` header and
the rows anchor's header hold the projection's conflict list; with
thousands of open conflicts each slow commit rewrote megabytes. (Kept in
the format if the measurements show it is small after the rest; otherwise
moved to per-row form; see `wp5-1.md`.)

### 3.5 Activator

- One preparation per batch: the read handle keeps its event cache and the
  model across tickets, so consecutive slow events cost their own cones.
- A prepared commit whose branch moved only by commits of other entities is
  committed instead of discarded: under the lock, when every event
  committed since the preparation's base touches no entity of the
  prepared event's cones (and is not touched by them), the prepared result
  is rebased (see `wp5-1.md` for what is re-checked); otherwise `Stale` as
  today.

### 3.6 Other tasks

- `check_commit`: an in-memory verified set (hash → size, mtime, process
  instant) records objects this process verified; a local commit skips the
  re-hash when the file still has the recorded size and mtime (invariant 1:
  the bytes were hashed by this process and are unchanged by `stat`, as the
  startup marker rule trusts).
- `backfill_journal_branch`: the cursor is upserted, so the progress row
  exists from the first chunk.

## 4. Risks and checks

- The 1,000-seed `derive_oracle` fuzz (every commit checked against a
  derivation from no rows; verify mode every third seed), the stress seeds,
  the unit comparisons with `tests::legacy`, `TKFS_CONFLICT_MODE=full` and
  `verify` suites.
- The model's memory is bounded (records per cut are memoized ids; an LRU of
  model versions per branch; a cap past which the model is dropped and the
  commit derives in full).

## 5. Baseline numbers of the scenarios (before)

Filled in by the runs (see `wp5-1.md`).
