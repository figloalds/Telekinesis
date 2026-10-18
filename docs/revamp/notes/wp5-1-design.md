# WP5.1 design: concurrent-edit activation

Package: WP5.1 (REVAMP_PLAN §14.5b). Inputs: CCR-WP4.INT-3 and CCR-WP4.INT-4
(`docs/revamp/notes/wp4-int.md`), the coordinator's CPU profile of the
WP4.INT soak workers (`prepare_slow → derive_conflicts → ConeGraph::cone →
project_refs`, dominated by `String` clones into `BTreeSet`s).

Sections 1 and 2 were written before the implementation; section 3
describes what was built (revised after it: the first choice, C, was not
needed, see §3.7). The numbers before and after are in `wp5-1.md`.

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

### C. A memoized model of full mode's namespace history (first choice, on A; not built)

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

## 3. The design as built

The note's first version chose C on A. What was built is A, plus three
incremental pieces that turned out to remove the O(history) work of a slow
commit without a model of full mode: an appended projection, a per-branch
derivation index kept beside the event cache, and a write that skips what is
stored already. C was not built (§3.7 says why); fallbacks still derive in
full, now on A's engine.

### 3.1 Interned engine (A)

`derive.rs`: entities are numbers in their ids' sort order, events are
positions, registers are pre-indexed per entity and field with their version
parents resolved, causal relations among a cone's events come from one
topological sweep (`Relations`), antichains are enumerated over concurrent
sets in the historical order, cuts are projected from the cone's registers
(`namespace_records`, each record built once per derivation), and `project`
borrows instead of cloning. Exactness of each primitive:

- **Entity order.** Entity numbers follow the ids' byte order, so every
  `BTreeSet<&str>` / `BTreeMap<&str, _>` iteration of the reference is the
  same order on numbers; pair records name `a,b` in id order as before.
- **Antichain enumeration order.** The reference builds the antichain list
  `L` by, for each relevant event `i` in order, appending `A ∪ {i}` for
  every earlier antichain `A` compatible with `i`. Those are exactly the
  antichains inside `conc_<(i)` (relevant events before `i` that are neither
  ancestors nor descendants of `i`), and in `L` they appear in the order
  `L(S) = [∅] ++ concat_{m ∈ S ascending} [A ∪ {m} : A ∈ L(S ∩ conc_<(m))]`
  (induction on `i`). Enumerating `L(conc_<(i))` recursively gives the same
  list in the same order, visiting only antichains. The refusal
  (`competing_cuts > MAX_NAMESPACE_CUTS`) is raised at the same antichain.
- **Restricted records in full mode.** `namespace_records` is exact whenever
  `projection_cannot_fail` holds (append mode's existing argument; unit test
  `restricted_projection_matches_project`). Full mode uses it under the same
  condition and projects each cut in full otherwise.
- **Location pairs** read the memoized closures and are evaluated in the
  reference order, so a bad name fails at the same pair.
- **`project`**: same registers, heads, winners, conflicts and namespace
  passes, built from references.
- **Competing-cut counts** (CCR-WP4.INT-4) are memoized process-wide by a
  SHA-256 digest of the branch, the relevant events' ids and the ancestry
  among them (`cut_counts`): the count is a function of exactly those.

Checked against the Wave 0 reference (`tests::legacy`) and against a
verbatim copy of the Wave 4 derivation (`tests::wave4`,
`interned_derivation_matches_wave4`: projections, full namespace history,
append plans, successor counts, on four generators including two-device
storms).

### 3.2 The appended projection

`project_append(base, events, new)`: `project(events)` from the stored
projection of the events without `E` (the handle's view), instead of
projecting the branch. The argument is in its doc comment; in short: with
no pending event and no event naming `E`, `E` is the only addition to the
active set; a register `E` does not change keeps its heads, winner and
records; one it changes has `E`'s parents removed from its heads and `E`
added, and its records recomputed from its whole version list; only `E`'s
entities' entries are rebuilt; the namespace passes are re-evaluated where
they can differ (`namespace_delta`): the slots `E`'s entities leave or take,
and the chains through an entity whose slot verdict, kind or parent changed
(and their descendants). It also returns what changed (entities, whether the
lists did, the records carried unchanged, the changed entities' old visible
placements), so the stored-projection delta, the view slots and the derived
rows need no comparison of whole projections. Refusals: `E`'s own
`INVALID_CAUSAL_PARENT` is checked as `project` checks it; anything else it
does not handle declines (`None`) and `project` runs. Under the commit
checks every appended projection is compared with `project`, and the
patched view slots with `view_slots`.

### 3.3 The branch index

Everything append mode used to compute from every event on every commit is
a function of the branch's events alone: event positions and causal
parents/children, entity numbers, the per-entity alive, touching and
namespace-register versions (`NsBase`), the cone graph with per-edge
contributions and the location changes per slot (`GraphBase`), and the
precondition's facts (comma ids, malformed locations, ids named but absent,
events with several relocations, names without a key, ill-typed values,
entities with an unplaced presence version: `Facts`). `BranchIndex` keeps
them per branch beside the event cache (`commit.rs`'s `EventCache`), built
by the first slow commit that needs them and **extended** when events join
the cache (`extend`: a monotone move of every stored position, then the new
events' contributions inserted in order; an added event that an indexed
event names, or a repeated id, drops the index instead, and the next commit
rebuilds it). Entities have stable numbers (first appearance) and a rank
table gives the id order, so a new entity changes two small tables, not
every list.

A slow commit **pushes** its event onto the index (it is the last event, so
everything it adds goes to the end of a row and is logged), derives, and
**pops** it (`undo`), also when the derivation panics (`Pushed`'s drop).
`derive_append_indexed` is the scan derivation (`derive_append_scan`, the
WP1.7 code on the engine of §3.1) with these reads replaced: the guard's
scans (no event names `E` and no id has a comma, read from the facts; the
relocation conditions over the indexed events with several relocations),
the context (`NsContext` borrows the index's `NsBase`), the cone graph, the
location pairs (the rows of `E`'s slot), the register versions of `E`'s
registers, and `projection_cannot_fail` (the facts plus `E`'s entities
rechecked). `project_append` and the successor counts read it too. A
history the index does not cover takes the scan: the crafted ones the guard
and `Locations::first_error` exist for.

Why it is exact: every part of the index is defined as a function of an
event list, and `BranchIndex::same_as` compares two indexes as such
functions (up to the stable numbering). Under the commit checks (every fuzz
seed, and verify mode) each extension is compared with a fresh build, each
indexed derivation and successor count with the scan's, and the index after
`undo` with the index before; the unit tests
`branch_index_extends_and_pushes_like_a_fresh_build` (random batches at
their id positions, as the cache merges them) and
`indexed_append_equals_the_scan_on_every_prefix` check the same on 3,000
seeds in release.

### 3.4 The write

- Rows the derivation knows are stored with the same payload (the base
  projection's records that `project_append` carried) are not written again
  (`projection_sources`): the upsert would leave the same table. The
  survival check still sees their ids.
- A seal trusts the handle's own last anchor (`snapshot::AnchorCache`: the
  bytes it wrote, compared by value with the stored row) instead of reading
  and hashing it, and reuses its header when the lists did not change.
- The view's slots are patched for the changed entities instead of
  recomputed (`commit::patched_slots`).

### 3.5 When it falls back to full mode

Unchanged from WP1.7: the structural guards (pending events, `E` named by a
committed event, a comma in an entity id, several relocations in `E`, one
event placing two entities in one slot, two events relocating the same
entities, more than 16 multi-relocation events), an irregular history
(repeated ids or registers), and the cone checks of `append_plan` (a touched
cone lost a relevant event: a TRUE presence witness displaced by `E`'s rank;
a cone shrank; a grown cone not covered), and a current namespace record not
derived from history. `TKFS_CONFLICT_MODE=full` always derives in full;
`verify` runs both and compares.

### 3.6 What a fallback and a rebuild cost now

Full mode runs on §3.1's engine: the 53 s fallback of §1 (4.5K events, 3K
conflicts) takes about 0.3 s; a full rebuild of a store with 9K open
conflicts is in `wp5-1.md` (scenario (d)).

### 3.7 Why not the model (C)

With §3.2–3.4, a slow commit that keeps the precondition costs its touched
cones plus a few O(history) integer passes (the index's position move, the
event cache merge), and the fallbacks are rare (§1: 1.4 % of append commits,
all witness displacements) and now cost a fraction of a second. The model
would make those fallbacks incremental too, at the price of a second
derivation of full mode's namespace history to keep exact (reference counts
per record across overlapping cones, a scoped DELETE) and its memory. The
measurements did not need it; it stays the way forward if displacement
fallbacks ever dominate (they are counted per reason,
`thread_fallback_reasons`).

### 3.8 Activator (CCR-WP4.INT-3, the other items)

A slow ticket is still prepared on a pooled read store and committed under
the lock, and a ticket whose branch moved is still `Stale` and re-queued.
What changed is the price: the read store keeps its event cache and branch
index across tickets (both are extended from the journal, not reloaded; its
view is reloaded when the engine's commits changed the stored projection),
so a preparation costs the event's cones plus that reload, and a stale one
is re-prepared in milliseconds, not minutes. Batching several tickets into one preparation, or
rebasing a prepared result over unrelated commits, would save that
re-preparation only; neither was built (the scenarios' stale counts and
backlogs are in `wp5-1.md`).

### 3.9 Other tasks

- `check_commit`: a process-wide verified set (`commit::verified`, keyed by
  store root and hash, holding size and mtime) records objects this process
  hashed; a local commit skips the re-hash of a new object recorded within
  five minutes with the same size and mtime, or with a fresh trusted
  availability row (invariant 1: the bytes were hashed by this process and
  `stat` shows them unchanged, the rule the startup marker already trusts).
- `backfill_journal_branch`: the cursor row is upserted, so a backfill that
  crashed before its first chunk resumes instead of matching no row.

## 4. Risks and checks

- The 1,000-seed release `derive_oracle` fuzz (every commit checked against
  a derivation from no rows, verify mode every third seed) and the stress
  seeds, which now also run every extension, indexed derivation and
  successor count against the scan; the Wave 0 and Wave 4 differential unit
  tests; `TKFS_CONFLICT_MODE=full` and `verify` spot checks.
- Memory: one index per cached branch (`EVENT_CACHE_BRANCHES`), dropped with
  its cache entry; it is about the size of the parsed events.

## 5. Numbers

Before and after, for every scenario, are in `wp5-1.md`.
