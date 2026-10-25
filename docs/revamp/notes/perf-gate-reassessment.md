# Performance gate reassessment: floor, purge policy and target rewrite

Written 2026-10-25 against the latest complete gate
([`prefetch-barrier-sandbox-20261025.json`](../../../test-evidence/wave-6/prefetch-barrier-sandbox-20261025.json))
and the retained diagnostics linked from
[W6-P0-MOUNT-LIFECYCLE](../WAVE_6_MOUNT_LIFECYCLE_BLOCKER.md). It changes no
code, no policy and no acceptance ratio. It proposes one candidate lap with
its predicted counters, one measurement lap, and a rewrite of section 8 of
[the replacement plan](../WAVE_6_REPLACEMENT.md) for the user to approve.

## 1. The five failures have two causes

Medians from the latest gate, in ms:

| Workload | Native | Mounted | Ratio | Target | Where the mounted time goes |
|---|---:|---:|---:|---:|---|
| Random 4 KiB overwrite, 10,000 ops | 43 | 1,348 | 31.4x | 1.5x | The close inside the timed window flushes every dirty page: about 9,400 Write callbacks per repetition ([marker note](hydration-marker-cost.md)) |
| Sequential 1 GiB write | 635 | 2,075 | 3.27x | 1.2x | The close inside the timed window flushes 1 GiB through the driver into the scratch file |
| Sequential 1 GiB read | 477 | 762 | 1.60x | 1.2x | Cleanup purged the just-written pages, so the read goes to the server; native reads its own page cache |
| `git status`, 50,000 files | 305 | 3,271 | 10.7x | 1.3x | 51,096 Open callbacks; the upstream memory filesystem measures 3,127 ms on the same guest ([reference](namespace-cache.md#canonical-git-reference-and-scoped-invalidation)) |
| Copy 2,000 small files | 559 | 3,796 | 6.8x | 1.3x | Per-file Create/Write/Cleanup/Close round-trips plus the metadata commit; the memory-filesystem floor for this workload has not been measured |

The native baseline defers its I/O. Ten thousand cached 4 KiB writes in 43 ms
is the page cache; a 1 GiB read in 477 ms is memory bandwidth. `bench.py`
times `random_4k` and `seq_write_1g` up to and including the handle close and
measures fsync separately, so native never pays writeback inside a timed
window.

The mount pays it every time. [`native/mount.c`](../../../native/mount.c)
sets `FlushAndPurgeOnCleanup=1`, so the driver flushes the file's dirty pages
as Write callbacks before the last Cleanup and then purges the cache. Three
of the five failures (random overwrite, sequential write, sequential read)
are the direct cost of that flag, not of anything on the server's byte path.

The other two (Git, copy) are metadata workloads whose cost is the
kernel-to-user transition per operation. The Git measurement already has
its floor: the upstream memory filesystem, which is entirely in RAM with no
persistence, receipts or sync, is within 4% of Telekinesis.

## 2. Why another in-memory cache cannot move the metadata workloads

- The server already has a bounded namespace entry cache (65,536 entries,
  64 MiB) with scoped invalidation. The scoped candidate reduced misses from
  51,062 to 54 per Git run; wall time stayed at about 3.3 s and the Open and
  security callback counts were unchanged
  ([evidence](namespace-cache.md#canonical-git-reference-and-scoped-invalidation)).
- A no-optional-locks diagnostic achieved over 99.9% user-mode cache hits
  and still took 9.5 s for status.
- Sampled guest CPU attributes 48.5 to 50.0% of server CPU weight to WinFsp's
  pending/processing queue spin-locks with 16 workers; the worker reduction
  that removes that contention regresses the data workloads and was rejected
  ([worker evidence](dispatcher-workers.md)).
- Two transport experiments (batch transport, split receive/completion
  queues) on the memory reference itself showed no consistent improvement.

Every one of these is a cost outside the server's lookup path. A cache
inside the process cannot beat a filesystem that does no work at all, and it
would add exactly the retained-state lifecycle risk this demand exists to
remove. Do not spend a lap on it.

## 3. Candidate lap: purge off, sealer-requested delivery

This is the fallback already written into section 3a of the replacement
plan ("seal at Close after the Cache Manager's own lazy writes, never a
volume flush from the server"). It has not been qualified because
[cleanup-cache-cost.md](cleanup-cache-cost.md) concluded that neither
`NotifyEnd` returning nor dispatcher shutdown proves that all dirty bytes
reached Rust, so a writable standby policy had no delivery proof.

The proof is not needed. The Write callbacks are the delivery. The design:

- Set `FlushAndPurgeOnCleanup=0`. Keep `FileInfoTimeout=INFINITE`,
  `PostCleanupWhenModifiedOnly=1`, access checks, receipts, recovery markers
  and the no-self-handle rule exactly as they are.
- The last writable Cleanup still marks the entity pending seal. The sealer
  thread, after the existing write-back delay, issues the existing
  per-path notify for that entity. In the pinned Beta5 driver,
  `FspFileNodeInvalidateCachesAndNotifyChangeByName` flushes and purges the
  named file's data section before applying the notification filter
  (source observation already recorded in the cleanup note). That is the
  server asking the driver to deliver one file's bytes, with no handle to
  the served namespace.
- The sealer then waits until the entity's Write callbacks go quiet (last
  Write timestamp older than a short settle interval, for example 100 ms)
  and seals. A bounded overall wait (for example 10 s) that does not go quiet
  is reported as a durability failure for that entity and the scratch stays
  on disk for the startup re-seal, which is the existing failure contract.
  A Write that lands after the seal produces a successor seal, which the
  `changed` recheck already handles.
- Explicit fsync is unchanged: the driver flushes the file's cache before it
  posts Flush regardless of this flag, so Flush remains a durability point.
- The crash contract is unchanged in kind: section 4 already states that a
  crash loses at most the write-back window of closed but unsealed files.
  The window now also covers bytes still in the kernel cache after close.
  A daemon kill inside that window loses them. Record the measured window in
  the lap.
- Host engine tests to add before the lap: pending seal with late Write
  arrival produces one seal with the final bytes; notify-then-quiet with an
  injected Write after the settle interval produces a successor seal; the
  bounded wait reports failure and retains the scratch; fsync during the
  pending window seals immediately with the flushed bytes.

Predicted counter movement, from `mount-stats`, per measured repetition:

| Counter | Latest gate | Predicted | Reason |
|---|---:|---:|---|
| `random_4k`: Write callbacks inside the timed window | ~9,400 | near 0 | No flush at Cleanup; pages stay dirty in the kernel cache, as native |
| `random_4k`: Write callbacks after the window | ~0 | ~9,400 or fewer (lazy-writer coalescing) | Delivered by the sealer's notify after the write-back delay |
| `seq_read_1g`: Read callbacks | ~16,384 (1 GiB in driver-sized reads) | near 0 | No purge at Cleanup; the read is served from the pages the write left in the cache |
| `seq_write_1g`: Write callbacks inside the window | ~1 GiB worth | only the portion the Cache Manager throttles during the write | Dirty-page throttling still forces some writeback, as it does for native NTFS |
| `copy_small`, `git_status_50k`: Open/Create/Close callbacks | 51,096 Opens for Git | unchanged | This lap does not touch metadata round-trips |
| Daemon forced/graceful exit | 0.004 s / 1.852 s | unchanged | No new retained state; the matrix must still pass all 44 cases |

Predicted outcome against the current targets: random overwrite and
sequential read pass; sequential write moves from 3.27x to roughly 1.5x
because the 1 GiB scratch write leaves the window but throttled writeback
and the per-callback transfer cost remain; Git and copy do not move. If the
Write counter inside the `random_4k` window does not fall, the driver did
not honour the flag as understood and the lap is a different question; do
not stack the next candidate on it.

What this lap must also prove, in the isolated VM with the 44-case matrix
and six timestamp laps: cached bytes reach the server after notify with no
self-open refusal logged; a reader opened before a writer on the same path
sees the final bytes; a forced kill during the write-back window loses only
that window and the restart re-seal recovers everything on disk; every byte
of the final 1 GiB verifies against the stopped backing packs after six
overwrite laps.

## 4. Measurement lap: the memory-filesystem floor for every workload

The pinned upstream memory filesystem fixture already exists in the guest
tooling. Run `bench.py` against it for copy, delete, sequential write,
fsync, read, random overwrite, Git and the three builds in the same guest and
the same session as the NTFS baseline, with one warm-up and five samples.
Retain the report next to the candidate's. This costs one lap and converts
the target debate from opinion into a measured floor per workload.

Expected: Git near 3.1 s (already known); copy well above 1.3x native
because each file costs at least four round-trips before any metadata work;
sequential write and read at some fixed fraction above native from the
driver's transfer copying; random overwrite near native once the flag is
off on that fixture too (the memory filesystem does not set it).

## 5. Proposed rewrite of the section 8 targets

Keep one script and one table. Add a second reference column and split the
targets by what bounds them.

| Workload | Current target | Proposed target | Why |
|---|---|---|---|
| Random 4 KiB overwrite | <= 1.5x native | <= 1.5x native, and <= one new chunk per op at seal | Achievable once the purge flag is off; this is a policy fix |
| 1 GiB sequential write | <= 1.2x native | <= 1.2x of the memory-filesystem floor, reported with the native ratio | The driver's transfer copying is outside the server; the floor lap sets the real bound |
| 1 GiB sequential read | <= 1.2x native | <= 1.2x native | Achievable from the kernel cache once purge is off |
| Copy 2,000 files | <= 1.3x native | <= 1.3x memory-filesystem floor | Per-file round-trips bound it; native ratio stays informational |
| `git status` 50,000 files | <= 1.3x native, `core.fscache` unset | <= 1.3x memory-filesystem floor with `core.fscache=true`, and native ratio reported | Git for Windows has shipped `core.fscache` enabled by default since 2.8.0; the guest's Portable Git without it is the unrepresentative client. With it on the mount measures 2.05x native with 610 Opens ([client-cache note](git-client-cache.md)) and the remaining cost is about 52,000 directory-entry callbacks, which the server can work on |
| Clean / no-op build | <= 1.3x / <= 1.2x native | unchanged | Already passing |
| Remote visibility | <= 250 ms | unchanged | Already passing |
| Idle-peer overhead | <= 20% per metric | unchanged | Copy still fails at +48.6%; separate cause, see the prefetch notes |

The native ratios stay in the report for every workload so a regression
against NTFS is still visible. Only the pass/fail bound moves for the
workloads whose cost is the driver boundary.

What this does not do: it does not relax durability, cache flushing, access
checks, fsync barriers, receipts or recovery comparison, and it does not
authorize cutover. The lifecycle matrix remains the P0 gate.

## 6. The structural option, for a separate decision

The targets as literally written (1.2x to 1.3x NTFS on metadata and
streaming workloads) are reachable only by a design where ordinary I/O does
not cross into user mode at all. On Windows that is the Projected File
System (ProjFS), the in-box driver behind VFS for Git:

- A placeholder becomes a real NTFS file on first write. After that, lstat,
  read, write and mmap are NTFS; the provider is not called.
- The provider answers enumeration, placeholder info and get-file-data for
  unhydrated items (served from packs, the existing lazy immutable-prefix
  model) and receives notifications. `FILE_HANDLE_CLOSED_FILE_MODIFIED` is
  seal-on-close; rename and delete notifications are the metadata events.
- The kernel does not depend on the provider for materialized files, and the
  driver was built for a provider that gets killed routinely. The NTFS USN
  journal records every change under the root while the provider is dead,
  which replaces hydration markers and scratch recovery for local edits.

Costs and unknowns, all of which need VM proof before this is more than an
option: directory root only (no drive letter), NTFS only, Windows 10 1809+
with the `Client-ProjFS` optional feature enabled by an administrator,
reparse-point placeholders visible to some tools, a rewrite of the working
layer where NTFS replaces scratch files, and a new adapter beside
`native/mount.c` and `src/mount.rs`. Public sources only partially confirm
that the provider's own I/O is exempt from callbacks and do not document
pending-callback cancellation on provider exit. This is a wave-sized
decision, not a P0 candidate; it is recorded here so the P0 record states
why the WinFsp floor exists and what would remove it.

## 7. Order

1. Host engine tests for section 3, then the purge-off lap with the
   predicted counters above and the full 44-case matrix.
2. The memory-filesystem floor lap (section 4), in the same guest session.
3. With both reports retained, the user decides the section 8 rewrite
   (section 5) and whether to open the ProjFS question (section 6).
