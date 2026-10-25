# Stable metadata reads without the Store mutex

The `bc04000b...` full-gate trace identifies Git's 50,000-lstat preload as the
dominant status stage, not index decoding or content hashing. A measured
repetition performs 51,096 Opens; its inclusive metadata stage totals 9.099 s,
including 6.551 s waiting for the Store mutex. These are concurrent cumulative
scopes, not independent wall-time costs. The underlying reports and trace are
in the [full-gate evidence](../../../test-evidence/wave-6/background-full-sandbox-20261024.json).

The follow-up caches successful namespace entry values for read-only opens and
lookup. A shared atomic epoch invalidates all pathname aliases before a
projection change and after checkout. Cache hits check the epoch twice;
misses use the authoritative Store lookup. The cache holds owned entry,
branch, version and anchor values, not projection-map clones, object pins or
namespace handles. It retains at most 65,536 entries and a 64 MiB estimated
owned-value/pathname budget, evicting entries in insertion order. Raw pathname
keys may miss on aliases; validation and normalization still run in the
authoritative miss path. Failed lookups are not cached.

Creates and writable opens retain their existing Store path. File size and
automatic/explicit timestamps still consult the working layer, so a cached
immutable descriptor cannot hide an unsealed local extension. Directory checks,
read/write access checks, retained-handle retargeting, cache purging and explicit
durability points remain in place. `mount-stats` exposes namespace hit/miss
counts alongside the existing open-stage counters.

The candidate also coalesces background checkpoint work: commit notifications
cannot bypass a minimum 50 ms interval after the preceding maintenance pass,
and an already fully checkpointed WAL does not need another PASSIVE call. Busy
reader retries continue periodically, while explicit FULL barriers remain
unthrottled and coordinated with the worker. This addresses the earlier
3,380 background calls rather than removing checkpoint work.

Four regressions protect cached read behavior: a stable metadata open completes
while the Store lock is held, without cloning projection maps; aliases follow
rename/delete/recreate; unsealed growth and explicit times remain visible; and
checkout invalidates cached entries when both branch views already exist.
Checkpoint reader, failure, recovery and FULL-barrier tests also pass.

Candidate SHA-256:
`0fb0beaed905fd4e8a86ea696318e1827c739688e6065f6115c069563719d7b8`.
All 185 host tests, strict Clippy, formatting and release build pass. All 44
isolated lifecycle cases and six timestamp laps pass (forced exit maximum
0.013 s; ordinary graceful maximum 0.171 s). The diagnostic daemon exits
normally in 2.543 s. The three-sample copy/Git medians remain 8.511 s/9.436 s
(6.48x/11.99x native); see the
[retained diagnostic](../../../test-evidence/wave-6/namespace-cache-sandbox-20261024.json).

The copy workload makes 462 background checkpoint calls costing 21.607 s,
versus 3,380/49.375 s in the preceding targeted worker diagnostic. NORMAL/FULL
commit costs are 3.614/2.859 s. Git's measured Open cost falls from 11.606 s to
2.601 s in a representative repetition, but wall time barely changes. Normal
Git status still incurs about 51,000 cache misses and two projection updates
per repetition; its optional index lock invalidates the global epoch.

A separate [Git cache probe](../../../test-evidence/wave-6/git-cache-probe-sandbox-20261024.json)
suppresses only Git's optional locks for diagnosis. It then records 102,180
hits and 34 misses per warmed repetition, zero projection updates, and a
9.514-second median. That variant is not the benchmark gate. Fully warm
user-mode metadata caching therefore does not remove this wall-time deficit.
The daemon exits normally in 0.595 s; both exports are verified. P0 remains open.

The next experiment compares explicit read-only volumes with cleanup purge
enabled and disabled, including a verified 50,000-file data-cache primer. The
test-only build setting `TKFS_NATIVE_CACHE_PROBE=purge|standby` changes only
those diagnostic builds. They identify themselves in the daemon log and reject
creates, writable/delete opens and mutating native callbacks. The default
build retains cleanup purging and its existing access/durability behavior.
These probes cannot qualify writer, fsync, recovery or Close publication policy.

An initial attempt only advertised `ReadOnlyVolume`; a client write was
accepted and the validation stopped it before measurement. That
[setup failure](../../../test-evidence/wave-6/readonly-cache-advertise-only-failed-20261024.json)
is retained, with its clean 0.426-second stop and verified exports. The v2
probe adds explicit native mutation rejection and completed successfully. Its default
build passes 185 host tests, Clippy, formatting/release and the five inert
import-binding fixtures. No result from the failed setup is accepted.
The [cleanup/cache isolation note](cleanup-cache-cost.md) records both the
purge/standby comparison and the subsequent modified-only Cleanup experiment.

## Canonical Git reference and scoped invalidation

A fresh isolated guest compares five measured canonical Git status runs on
NTFS, the pinned upstream WinFsp memory filesystem, and Telekinesis `23b4e6aa...`,
followed by another NTFS control. All three fixtures have the same Git tree;
all 50,000 files are byte-verified. Optional locks remain enabled, fsmonitor
and untracked cache are disabled, and `core.fscache` is unset/false.

| Backend | Median Git status |
|---|---:|
| NTFS before | 270.8 ms |
| Upstream memory filesystem | 3,127.3 ms |
| Telekinesis | 3,246.7 ms |
| NTFS after | 275.9 ms |

Both mounted backends issue 51,096 Open callbacks per measured run. Their
normal stops take 0.112/0.334 s, with no kill or stranded process. The 174.3 MB
export passes CRC, SHA-256 and timestamp-size checks; the runner's final process
handle is missing. See the
[retained reference](../../../test-evidence/wave-6/memfs-reference-sandbox-20261025.json).
The memory backend lacks durable receipts, synchronization and conflict logic;
it retains upstream operation guards and ACL support. Its result is neither a
theoretical lower bound nor a replacement for this project's acceptance.

The stopped Telekinesis database independently confirms 12 `index.lock`
creation/deletion events across warmup and measurement. Each measured run
records 51,062 namespace misses and 51,166 hits. The global namespace epoch
turns a change confined to `.git` into a cache miss for every tracked file.

The scoped candidate records changed entity IDs and, for membership changes,
old/new parent IDs. Cached entries retain ancestor IDs, so invalidation covers
case/path aliases, moved descendants and name-collision winners without relying
on pathname prefixes. Each publication starts as global; only successful
projection updates narrow it. Checkout, failed updates, unknown ancestry,
oversized scopes and history gaps require authoritative lookup. History is
bounded to 1,024 records and a 1 MiB accounting budget; the entry cache keeps
its existing 65,536-entry/64 MiB limits, including ancestry storage.

The new regression fails before the change and passes afterward: a cached
`sources/file` lookup completes while Store is held after creating/deleting
`.git/index.lock`. Ancestor moves, remote collision/deletion, aliases, unsealed
metadata and checkout are covered. All 193 host tests, formatting, strict
Clippy, release build and five inert binding fixtures pass; see the
[host record](../../../test-evidence/wave-6/namespace-scoped-host-20261025.json).

Candidate `de1c25b7...` passes all 44 isolated lifecycle cases (forced exit
maximum 0.004 s; ordinary graceful maximum 0.100 s) and six timestamp laps.
The subsequent five-sample Git A–B–B–A comparison uses independent copies of
the same stopped corpus and verifies every fixture byte before each phase:

| Phase | Code | Median Git | Namespace misses per measured run |
|---|---|---:|---:|
| A1 | UPSERT reference | 3,293.3 ms | 51,062 |
| B1 | Scoped invalidation | 3,369.6 ms | 54 |
| B2 | Scoped invalidation | 3,261.7 ms | 54 |
| A2 | UPSERT reference | 3,309.8 ms | 51,062 |

The candidate has 102,174 cache hits per measured run. The miss reduction does
not produce a consistent wall-time improvement. Open/security callback counts
remain 51,096/51,132, and every Git trace still records 50,000 index checks.
Matched native before/after medians are 320.1/289.6 ms, so Git still fails.
All four mounted daemons exit normally in 0.308–0.359 s; the runner's final
handle is missing. All four stopped databases pass integrity and show only
the expected 12 lock events. The controller retained native read-only backing
connections, so raw WAL/SHM are preserved and included when verifying private
database copies. The 563.1 MB export passes CRC, SHA-256 and timestamp-size
verification; see the
[retained qualification](../../../test-evidence/wave-6/namespace-scoped-sandbox-20261025.json).
This qualifies scoped cache coherence and lifecycle behavior, not the full
workload/build/idle-peer gate or the failed Git target. P0 remains open.

## Complete five-sample gate on the scoped candidate

The same `de1c25b7...` binary subsequently completes the native, mounted and
idle-peer workloads in the isolated guest. All **54 build observations** are
correct, with no workload I/O errors. These are new matched measurements;
absolute differences from older guests do not establish a code speedup.

| Metric | Native | Mounted | With idle peer | Mounted/native |
|---|---:|---:|---:|---:|
| Copy small files | 568.3 ms | 3,490.7 ms | 4,991.2 ms | 6.14x |
| Sequential write | 501.6 ms | 1,409.7 ms | 2,258.8 ms | 2.81x |
| File fsync | 402.5 ms | 669.6 ms | 1,492.1 ms | 1.66x |
| Sequential read | 512.3 ms | 758.6 ms | 771.1 ms | 1.48x |
| Random overwrite | 42.3 ms | 1,341.2 ms | 1,731.6 ms | 31.70x |
| Git status | 297.1 ms | 3,358.2 ms | 3,558.6 ms | 11.30x |
| Clean build | 4,506.4 ms | 4,886.5 ms | 5,015.9 ms | 1.08x |
| No-op build | 1,051.7 ms | 1,194.9 ms | 1,196.4 ms | 1.14x |
| Incremental build | 1,966.9 ms | 2,332.0 ms | 2,163.4 ms | 1.19x |

The five copy/write/read/random/Git targets still fail, both with and without
the peer. Clean/no-op targets pass. Fsync and incremental times are retained
without inventing separate numeric targets. Initial peer catch-up completes
in 70.900 s within its 300-second bound. Remote visibility passes at a 34.831 ms
median. Idle-peer overhead fails for copy (+43.0%), sequential write (+60.2%)
and random overwrite (+29.1%); read (+1.6%), Git (+6.0%) and clean/no-op builds
(+2.6%/+0.1%) pass that overhead limit.

Both daemons stop normally in 0.380/0.086 s, with no kill or stranded process.
After shutdown an independent verifier reconstructs every byte of the final
1 GiB, including six seeded 10,000-overwrite laps and 53,560 changed pages.
All 16,385 objects verify; actual/expected SHA-256 agree:
`16e78db98a9732f51955188d6b69de7749375dbfc9d79d7b934bc09f1afee0ba`.
Metadata catch-up does not imply complete peer payload hydration. Cache,
durability and access policy, the canonical Git command and all original
acceptance targets remain unchanged. The
[full-gate record](../../../test-evidence/wave-6/namespace-full-sandbox-20261025.json)
retains reports, callback observations, traces, exits and the verified export.
P0 remains open on performance and the purge-cost decision.
