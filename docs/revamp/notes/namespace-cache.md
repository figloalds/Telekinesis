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
Clippy, release build and five inert binding fixtures pass. Mounted lifecycle
and paired performance qualification are in progress; P0 remains open.
