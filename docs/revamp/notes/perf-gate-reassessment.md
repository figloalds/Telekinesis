# Performance gate reassessment: floor, purge policy and target rewrite

Written 2026-10-25 against the then-latest complete gate
([`prefetch-barrier-sandbox-20261025.json`](../../../test-evidence/wave-6/prefetch-barrier-sandbox-20261025.json))
and the retained diagnostics linked from
[W6-P0-MOUNT-LIFECYCLE](../WAVE_6_MOUNT_LIFECYCLE_BLOCKER.md). It changes no
code, no policy and no acceptance ratio. It proposes one candidate lap with
its predicted counters, one measurement lap, and a rewrite of section 8 of
[the replacement plan](../WAVE_6_REPLACEMENT.md) for the user to approve.

## 1. The five failures have two leading cost explanations

Medians from the preceding prefetch-barrier gate, in ms:

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
measures fsync separately. Native does not require completion of all writeback
at close; cache pressure can still force writes or throttle the caller.

The mount pays it every time. [`native/mount.c`](../../../native/mount.c)
sets `FlushAndPurgeOnCleanup=1`, so the driver flushes the file's dirty pages
as Write callbacks before the last Cleanup and then purges the cache. The flag
places extra delivery/purge work inside or between these windows.
The callbacks also execute the server byte path, including its scratch I/O and
hydration markers. The existing gate does not isolate how much each contributes;
a matched purge-on/off reference measurement is needed before assigning the
whole gap to policy.

Git and copy include per-operation kernel/user transitions; copy also performs
file data and metadata commits. The upstream memory filesystem is a measured
reference for Git, within 4% of Telekinesis in the cited run. It omits
persistence, receipts and sync, but still performs access checks, allocations
and filesystem callbacks. That observation is not a theoretical lower bound.

## 2. Why another lookup cache is not the next measured lead

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

These results do not support another lookup-cache experiment as the next
step. They also do not prove that this memory implementation is unbeatable.
Ordinary bounded in-memory state does not itself recreate the retained
served-namespace-handle defect; any future candidate still needs measured
benefit and correct invalidation.

## 3. Proposed candidate: purge off, sealer-requested delivery

This is the fallback already written into section 3a of the replacement
plan ("seal at Close after the Cache Manager's own lazy writes, never a
volume flush from the server"). It has not been qualified because
[cleanup-cache-cost.md](cleanup-cache-cost.md) concluded that neither
`NotifyEnd` returning nor dispatcher shutdown proves that all dirty bytes
reached Rust, so a writable standby policy had no delivery proof.

The proposed design treats observed Write callbacks as delivery. The review
below identifies why a quiet interval alone cannot establish complete delivery;
this proposal is not yet safe to promote as a writable candidate. Its steps:

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
  and would seal. A bounded overall wait (for example 10 s) that does not go quiet
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
driver's transfer copying; random overwrite closer to native with the flag off. The existing matched
Git reference explicitly enabled purge; the new measurement therefore needs
both modes rather than assuming its prior configuration was purge-off.

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

The existing WinFsp measurements miss the literal NTFS targets. They do not
prove those targets impossible for every WinFsp configuration. One structural
alternative is a design where materialized-file I/O stays in the native
filesystem. Projected File System (ProjFS), the in-box driver behind VFS for
Git, is an option to investigate:

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

A source review of the pinned Beta5 driver
([winfsp-source-review.md](winfsp-source-review.md)) maps each failing ratio
to the driver's request model, weighs keeping, forking or bypassing WinFsp,
and adds the Cloud Files API beside ProjFS as the second in-box option.

## 7. Order

The follow-up review changes the order:

1. Measure both reference cache policies between native controls (section 4),
   retaining incomplete attempts and reference implementation adjustments.
2. Establish complete delivery/error accounting for any writable cache-policy
   candidate, including held readers and bounded shutdown. A quiet interval
   alone is insufficient; host late-write tests do not prove kernel completion.
3. Qualify a candidate that preserves that contract with all 44 lifecycle cases,
   six timestamp laps and the complete original performance/build/peer gate.
4. With the measurements retained, the user decides any section 8 target
   rewrite (section 5) and whether to open the ProjFS question (section 6).

## 8. Follow-up review (2026-10-26)

The current exact candidate's [complete result](../../../test-evidence/wave-6/prefetch-roots-sandbox-20261025.json)
now passes lifecycle, timestamps, all 54 build observations and final stopped-
backing byte verification. It still fails copy, write, random overwrite, Git
and peer copy overhead. No acceptance target has changed.

Section 3 has a completion gap. The pinned Beta5 `FspVolumeNotify` enqueues a
`DelayedWorkQueue` item and returns success before `FspVolumeNotifyWork` runs.
The worker can begin after a chosen quiet interval or block acquiring the file
resource before its first Write. A gap after some delivered pages also does
not establish that the remainder arrived. A later successor seal only repairs
this if the dispatcher stays alive long enough to receive those later writes.
It cannot recover kernel-only bytes after successful-looking shutdown stops
that dispatcher. This affects graceful durability and the bounded write-back
window, even while the original forced-kill contract allows unsealed losses.

There is also an error channel gap: `FspFileNodeInvalidateCachesAndNotifyChangeByName`
receives `IoStatus` from `FspCcFlushCache`, skips purge on a failed flush, then
continues invalidation/notification without returning that status to user mode.
Neither silence nor notification enqueue success can distinguish successful
no-op delivery from delayed or failed delivery. A 10-second timeout only helps
when the implementation can recognize that completion has not happened.

The [source review and counterexample](../../../test-evidence/wave-6/notify-quiet-review-20261026.json)
retain exact Git blob hashes and a permissible ordering with worker startup
after the quiet interval. This is a source/protocol argument, not an observed
Windows scheduler delay. Before disabling the production purge flag, establish
a completion/error acknowledgement or an equivalent proven Close-based
invariant, track pending kernel delivery independently of scratch writes, and
exercise delayed-first-write, partial-flush gaps, no-op completion, flush
failure, fsync, rename/delete and bounded shutdown. Host late-write tests alone
cannot establish the kernel ordering. Production keeps the qualified policy.

The independent section 4 measurement proceeds first: one executable compares
purge-on and standby on the pinned upstream memory backend, between two native
NTFS controls. It keeps the benchmark script, dimensions, canonical Git
configuration and five measured samples unchanged. This measures a reference,
not a theoretical lower bound or Telekinesis writable durability. A target
rewrite remains a separate user decision after the reports are reviewable.

The first full-size reference attempt is retained as
[intentionally incomplete](../../../test-evidence/wave-6/memfs-floor-aborted-sandbox-20261026.json).
The existing Git fixture capped individual files at 64 MiB. Raising that cap
for the 1 GiB workload exposed the upstream backend's repeated `HeapReAlloc`
growth: completed warmup/two sequential writes took 218.4/235.2/232.8 seconds.
This does not establish an ideal memory-backend streaming floor. The benchmark
client was intentionally terminated through a verified process handle; the
server exited normally in 0.058 s, and the controller's final handle is missing.
The native export passes CRC, SHA-256 and timestamp-size verification. No
standby or native return-control measurement completed in that attempt.

A separate test-reference variant grows private buffer capacity geometrically,
retaining capacity until free while preserving logical file/allocation sizes
and the upstream operation/cache/access handlers. Inert host checks cover
prefix preservation, shrink/grow, size limits, injected allocation failures,
concurrent independent allocations and free; normal and AddressSanitizer runs
pass. Its [host record](../../../test-evidence/wave-6/memfs-floor-amortized-host-20261026.json)
retains exact adapter/build sources. Its isolated paired-policy/full-workload
measurement is complete and retained below. This adapter changes the reference implementation;
it is neither an unmodified upstream measurement nor a production change.

## 9. Complete paired-policy reference measurement (2026-10-26)

The [complete reference record](../../../test-evidence/wave-6/memfs-floor-amortized-sandbox-20261026.json)
retains executable `d2437f6e...`, exact upstream/adapter sources, the unchanged
benchmark scripts, counters and traces. Each phase has one warmup and five
measured samples: 2,000 copy files, 1 GiB sequential I/O, 10,000 random writes,
50,000 tracked Git files, and the original clean/no-op/incremental build sizes.
Canonical Git remains unchanged, including `core.fscache` unset/false.
Medians in milliseconds:

| Workload | NTFS before | Memory, purge | Memory, standby | NTFS after |
|---|---:|---:|---:|---:|
| Copy 2,000 files | 655.7 | 1,816.6 | 1,745.0 | 578.3 |
| Delete those files | 183.0 | 659.2 | 652.9 | 161.9 |
| Sequential 1 GiB write | 495.9 | 2,015.0 | 1,990.9 | 481.2 |
| Separate file fsync | 536.4 | 0.5 | 210.6 | 345.1 |
| Sequential 1 GiB read | 570.0 | 406.7 | 593.7 | 548.6 |
| Random overwrite | 44.7 | 592.5 | 49.3 | 44.6 |
| Canonical Git status | 347.9 | 3,390.5 | 1,692.8 | 316.0 |
| Clean build | 4,854.5 | 4,965.5 | 4,760.7 | 4,756.4 |
| No-op build | 1,200.5 | 1,246.9 | 1,189.4 | 1,119.2 |
| Incremental build | 2,150.6 | 2,251.6 | 2,141.1 | 2,062.9 |

All 72 build observations validate. Every byte of the final 1 GiB matches the
expected overwrite result in each of the four phases; this is client-visible
verification, not an independent durable backing-store test of the memory
backend. All four 50,000-file Git corpora also match their expected bytes.
The eight small lifecycle cases plus both large-corpus normal stops pass.
Forced exit takes at most 0.003 s; normal exit at most 0.577 s. No process is
stranded. The controller's final process handle is missing. The 2,183,338,729-
byte, 104,814-member export passes CRC, SHA-256 and timestamp-size checks.

The random-write counter prediction holds on this reference: each measured
purge repetition delivers 9,413–9,456 Write callbacks, versus zero within each
standby repetition's bracket. Delivery is deferred: standby Git observations
include 953–2,903 background Write callbacks. Those calls need not belong to
Git itself. Both modes still make 51,096 Open and 51,137 security callbacks per
Git repetition, despite the halved Git time. Sequential observations report
zero Read callbacks in both modes; this comparison does not show the predicted
16,384-to-zero transition. The sequential bracket combines write, fsync and
read, so its Write totals cannot isolate the three timers.

This narrows the hypotheses in sections 1–4. Purge off substantially improves
random overwrite and Git in this memory implementation, but leaves sequential
write essentially unchanged and does not improve its read median. Copy gains
only about 4%. The measured Git improvement with the same Open/security counts
also means the earlier 3.1-second purge reference was not a policy-independent
floor. Native return controls vary, particularly copy, Git and fsync; this is
one ordered guest comparison, not a repeated A–B–B–A causal experiment.

These results do not qualify a writable Telekinesis standby policy. Memory
fsync has no pack/receipt/WAL persistence, and its timings are not durability
bounds. The reference also contains the explicitly recorded capacity adapter.
The delivery/error gap in section 8 must be resolved before changing production
cache policy. The target table in section 5 remains a proposal: the configured
client-cache variant is not measured here, and no original target changes.
P0 remains open on the actual Telekinesis full-gate and peer-copy failures.

## 10. Further checks without changing the acceptance target

The [known-event echo experiment](metadata-echo-cost.md) reproduces a changed
SQLite counter/durability stamp, but both versions write zero bytes in a
stable-echo native stress and have essentially unchanged wall time. Its
conditional-clock candidate is rejected and the qualified source restored.
This is not evidence that peer copy overhead is solved.

The [payload-readiness diagnostic](peer-payload-readiness.md) subsequently
confirms that metadata catch-up can leave almost all current objects missing.
Its fully imported copy comparisons are lower but include a +20.9% result
against the return control. The harness now waits for both metadata and
committed current-object inventory within one cumulative deadline; the original
targets and prior full-gate failures remain visible. This corrects the initial
idle boundary; it neither qualifies a new cache policy nor proves P0 complete.
The complete corrected-boundary matrix subsequently validates all 54 builds,
the final 1 GiB and normal stops, but still fails peer copy at +21.2% and all
five original native data/metadata ratios. The measurement gap was real;
initial hydration was not the whole remaining overhead.

The [cached-presence experiments](cached-prefetch-presence-cost.md) also reject
two local prefetch optimizations: a grouped lock trades lower isolated scan
time for worse writer contention, while a source-only fast path does not keep
its apparent gain in a reversed comparison. The qualified source is restored.
This is further evidence to measure foreground contention and actual feed/fetch
activity rather than equating a lower background microbenchmark with a copy fix.

A [read-only guest inventory](../../../test-evidence/wave-6/driver-options-sandbox-20261026.json)
checks feasibility of the structural options. This Sandbox reports UEFI,
Secure Boot off and administrator access, but cannot open its system boot
configuration store. A custom test-signing/reboot route is therefore not
established. `Client-ProjFS` is `DisabledWithPayloadRemoved`, with no
`ProjectedFSLib.dll` present. No boot setting, feature, driver, certificate or
registry value was changed. These facts identify prerequisites; they neither
qualify a new driver/provider nor prove other isolated VM routes impossible.
