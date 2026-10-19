# Wave 5 closure and continuation handoff

2026-10-19. The user requested completing Wave 5, skipping long tests,
integrating the revamp into `master`, and retaining the knowledge needed to
arbitrate a continuation. This closes the planned integration with a reduced
verification run; it does not implement the proposed Wave 6 designs.

## Integrated work

The four package branches were already combined in `revamp/wp5-int` at
`90ad052a1dc72e5a872bab20f399506886c64ae8`. The current closure merged that
branch into `revamp/production-sync` with `ef9bb23`, preserving the separate
package commits and the investigation commit `d78ea59`. It also imports the
integration worktree's unfinished notes and completed evidence. The final
production branch is merged into local `master` after focused verification.

The integration fixes the outstanding package review blockers rather than
carrying them as accepted bugs: the deferred-create name collision is fixed
by `9e84933`, and the uncommitted WP5.4 comment fix was already applied in
`05796d8`. Package-level approval labels in historical notes describe those
reviews at the time; the integrator's fixes and tests follow them.

Wave 5 includes:

- Causal/conflict preparation scoped to affected changes, interned derivation,
  incremental projection/index work, and reconstruction improvements.
- Saves whose copying, hashing and installation run outside the engine lock;
  new-object verification reuse; fewer bookkeeping-triggered view reloads;
  bounded background save work and corrected deferred-create recovery.
- Missing-content reads with bounded progress-aware waits; faster reconnect,
  session-loss lease release, and demand promotion within a repository round.
- Feed-derived worker status and per-peer credentials through the catalog
  contract, plus qualification wiring and operator documentation.

## Evidence and verification policy

The completed long-run records were recovered from the integration worktree,
not rerun. `test-evidence/qualification-wave-5.json` combines their floor
results and records the current focused checks. The preserved all-run has
102 passing scenarios and one skipped SIGTERM scenario on Windows. Its 76
passing floors plus the two heavy records' three floors cover **79 distinct
floor IDs**, with no failing floor.

Historical full Windows suite, WSL subset, oracles and default-soak outcomes
remain in [WP5.INT's notes](wp5-int.md) and [VALIDATION](../../VALIDATION.md).
These reports must not be presented as fresh tests of the final merge commit.
The source code is the integration branch's code; closure changes evidence
and documentation. Fresh closure checks passed:

| Check | Result |
|---|---|
| `cargo fmt --check`, `git diff --check` | clean |
| Five focused binaries: `local_fs_throughput`, `availability_index`, `worker_feed_status`, `contracts`, `harness_selftest` | 124 passed, 0 failed, 2 historical fixture-capture tests ignored |
| Exact network regressions: sibling reconnect wake, next-pull demand, dead-session offer lease release | 3 passed |
| Windows debug CLI and selected test binaries | rebuilt successfully; initial compilation 1 min 30 s |
| Rebuilt `tkfs.exe --help` | exit 0 |
| Evidence JSON and 13 local handoff links | validated |

Total fresh regression checks: **127 passed, zero failed**. The summary JSON
records commands and per-binary counts. No production source changes were
needed beyond the existing Wave 5 integration fixes.

Skipped now by user direction: full suite, `qualify -All`, multi-hour soak,
heavy concurrent-edit campaigns, large oracle fuzz/stress, and the two-machine
lab. Desktop/manual checks, Docker upgrade rehearsal and full Linux
qualification are not newly claimed. Existing timing-sensitive failures and
successful isolated reruns remain documented in WP5.INT's notes.

## What the live-build investigation does and does not describe

The installation observed during the slow Visual Studio build still ran
`0.1.0+b14d7bd79566`. The initial source study used the Wave 5 base `c6948c9`.
Neither is the merged Wave 5 code. Preserve those observations as evidence
about that installation, not a benchmark or regression verdict on Wave 5.

| Earlier finding/proposal | State after Wave 5 integration |
|---|---|
| Hash/copy/install under the engine mutex | Ordinary save phases moved off lock. Re-measure the build with the integrated version before attributing the old latency to current code. |
| Immediately hashing a newly saved object again at commit | Verified-set/install proof reuse is integrated, including a later corrupt-verdict override. |
| Object bookkeeping/demand invalidates the local metadata view | Same-connection view freshness fixes are integrated; overlapping foreign commits still have a documented reload issue. |
| Missing-content read fails immediately | Progress-aware waits integrated; unavailable content still has finite failure deadlines. |
| Foreground demand should get a transfer slot | Promotion exists inside a repository round; cross-repository admission still needs work. |
| Separate engines, durable admission, scoped failures, cache coherency, invariant-scoped coordination | Future design proposals; not implemented by closing Wave 5. |
| Availability-index failure could cause a redundant want | Hypothesis remains unproven in TKFS. The isolated SQLite model proves only the underlying snapshot-upgrade mechanism. |
| Engine waits of 230–339 ms during the live build | Observed on the older running worker; SQLite's contribution and the lock-holder operation were not identified. |

Closing this branch does not itself deploy or restart the installation.

## Retained limitations and decisions for continuation

1. **Durability semantics need explicit product arbitration.** During a burst,
   close can return with background saves outstanding (up to 72 queued/in-flight
   files in the documented common case; see the additional same-file race
   qualification in WP5.2's notes). A hard crash can lose unsaved closed-file
   changes. Explicit fsync/FlushFileBuffers remains a durability point. New
   WinFsp files enter the journal at their first save. Do not erase this
   tradeoff when comparing performance.
2. **Conflict-list header writes:** CCR-WP5.1-2, O(open conflicts), snapshot
   format redesign deferred. Some O(history) passes and worker CPU growth with
   history remain; profile rather than assume activation is the only cost.
3. **Projection freshness on overlapping commits:** CCR-WP5.2-3, deferred for
   correctness review. Separate ordinary lock waits from SQLite writer waits,
   transaction/fsync latency and checkpoint stalls before blaming the engine.
4. **Demand across repositories:** CCR-WP5.INT-1; a demand can wait behind
   other repository rounds, up to the 10 s round cap. Correct slot admission
   and fair resource budgets remain a priority.
5. **Old-run lease release versus a new run:** CCR-WP5.INT-2; add appropriate
   run generation/ownership to prevent stale release of a newer runner's lease.
6. **Stale peer status:** CCR-WP5.4-5; permanently departed feed peers persist
   until restart. **Staging cost:** CCR-WP4.4-1 payload-parsing trigger remains.
7. **User design constraints:** keep offline local writes and causal
   reconciliation; give metadata and objects independent bounded engines;
   prioritize metadata on peer and loopback surfaces; prioritize missing
   foreground content; coordinate only affected invariants/entities. SQLite
   has one writer per database, but that does not justify a global I/O lock.
8. **Runtime/storage choices remain open.** Tokio requires measured comparison;
   chunked CAS requires format/protocol/recovery design; SQLite replacement
   requires evidence that it is the bottleneck after unnecessary work and
   serialization are removed. No continuation is preapproved by these notes.

## Knowledge index

- [WP5.INT](wp5-int.md): package integration, review fixes, carried CCRs,
  historical tests and qualifications.
- [WP5.1](wp5-1.md), [design details](wp5-1-design.md), [WP5.2](wp5-2.md),
  [WP5.3](wp5-3.md), [WP5.4](wp5-4.md): measurements, design decisions,
  deviations, failed experiments and limits from each package.
- [Build incident and JuiceFS study](wp6-build-sync-juicefs-study.md): source
  study, live samples, admission hypothesis and object/local-I/O proposals.
- [TigerBeetle study](wp6-tigerbeetle-metadata-study.md): offline authority,
  batching, bounded repair, keyed contention and deterministic simulation.
- [Normalized build incident evidence](../../../test-evidence/build-sync-20261019/README.md):
  observations and isolated SQLite model, with explicit confidence limits.
- [Archived catch-up probe](../../../test-evidence/wave-5-handoff/README.md):
  exploratory source kept outside the normal test suite.

Next arbitration starts from the integrated `master` tree and this index:
measure the actual build on the integrated code, identify its dominant costs,
then select a bounded continuation scope with explicit latency and durability
requirements.
