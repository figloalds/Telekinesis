# W6-P0-MOUNT-LIFECYCLE: kernel cleanup hang

Status: **OPEN, P0 — fixed-driver lifecycle/build checks pass; performance acceptance fails**
(2026-10-25). Resolve this before
other Wave 6 feature work, runtime cutover or mounted performance qualification.
Documentation and diagnosis do not close this demand. The fix below is
implemented in this checkout ([note](notes/mount-lifecycle-fix.md)); the demand
closes only when all acceptance checks, including the purge-cost decision, pass.

## Current work and executable acceptance

The earlier full gate (`e8ee0871...`, 2026-10-24) **stranded the daemon and .NET**
on WinFsp 2.1.25156. This supersedes the earlier short lifecycle passes as a
safety conclusion. A full guest kernel dump proves the notification/rename
resource cycle fixed upstream in [WinFsp PR #669](https://github.com/winfsp/winfsp/pull/669),
with volume deletion subsequently blocked on outstanding notification work.
This is distinct from the historical retained-root-handle defect below.
See the [driver dependency diagnosis](notes/notification-driver-lifecycle.md)
and [retained evidence](../../test-evidence/wave-6/lazy-notify-hang-20261024.json).
No further mounts ran in the stranded guest; native stores, exact timestamps,
process state and kernel/user dumps were exported before disposal.

The new native preflight refuses unrecognized dependencies before creating a
volume. Its current exact allowlist is official WinFsp **2.2.26286.0 (2026 Beta5)**,
including both DLL/SYS hashes and selected loaded-driver identity. This is a
prerelease dependency and still needs this project's complete acceptance.
The expanded matrix's **42 cases pass**, including concurrent notification/rename
stops and legacy receipt recovery. Maximum forced exit is **0.010 s** and
ordinary graceful exit **0.199 s**; the
[report](../../test-evidence/wave-6/mount-lifecycle-fixed-driver-sandbox-20261024.json)
records the exact candidate `5fce4ee6...`. Host checks pass (165 tests, one
opt-in benchmark ignored). Six repeated timestamp laps and the immutable-open
diagnostic also pass ([evidence](../../test-evidence/wave-6/fixed-driver-sandbox-20261024.json));
the full gate completed correct clean/no-op/incremental builds and the daemon
stopped gracefully in **0.768 s**, without a kill or stranded process. Its
five other target ratios still fail: copy **10.85x**, sequential write/read
**7.77x/3.23x**, random overwrite **26.89x**, Git status **12.98x** native.
The old harness stopped before the peer phase on those metric-only failures.
This is not a purge-cost isolation experiment; host compilation also overlapped
part of the mounted run, so a controlled final performance lap remains required.
The complete export (7,148 files) and timestamps were verified before further
work in the clean guest. Cache flushing,
notifications, durability and access checks remain enabled. The earlier short
matrix also passed before a full-gate hang, so this does not close P0.

The next candidate also rejects mounts overlapping backing scratch/object
storage before any volume exists, including existing reparse aliases. Its
44-case matrix adds two preflight negatives. All 44 cases pass on
`eff2afbc...` ([report](../../test-evidence/wave-6/mount-lifecycle-backing-guard-sandbox-20261024.json)).
It has 166 passing host tests (one opt-in benchmark ignored), strict Clippy
and a release build. Its targeted transaction/peer diagnostic completed:
`COMMIT` consumed **25.309 s** of **25.499 s** in `sqlite_batch`; savepoints,
release and mode changes were negligible, and no projection maps were copied.
Peer initial catch-up completed in 15.833 s before measurement. Remote
visibility median was **87.39 ms**; both daemons exited gracefully (0.697 s
and 0.269 s). Copy still failed at 8.95x native and increased another 27.9%
with the connected peer, so this is not performance acceptance. The
[diagnostic](../../test-evidence/wave-6/transaction-split-sandbox-20261024.json)
retains counters, reports, exit records and a verified native archive.
An instrumentation-only follow-up separates actual NORMAL/FULL commit timing
with a read-only connection-setting query. Its host checks pass; the targeted
VM copy diagnostic measures **15.056 s NORMAL** and **8.291 s FULL** commit
time, with no unknown-mode commits and a graceful 0.586-second stop
([report](../../test-evidence/wave-6/commit-modes-sandbox-20261024.json)).
No durability policy changes.
The benchmark now continues into peer measurements
after complete, correct workloads that miss only performance ratios, while
retaining the failed gate. Missing measurements, build/I/O failures and cleanup
timeouts still stop progression.

A native import-binding audit found one further preflight gap: the shim's
basename delay-load descriptor could select a different already-loaded DLL
from the absolute module that was verified. The implemented fix pins/verifies
every actual delay-IAT binding and refuses a changed runtime selection within
one process. Five bounded, inert-DLL fixtures pass without driver/service or
mount calls: the default-helper baseline demonstrably chooses preloaded A
despite an absolute B load; the corrected hook chooses B, rejects prebound or
mixed imports, and freezes selection across attaches. Run
`python scripts/check_winfsp_binding.py` for these explicit child fixtures.
The corrected binding and SQLite candidate `805b8634...` passes all 44 VM
lifecycle cases and six repeated timestamp laps. Maximum forced exit is
0.018 s and ordinary graceful exit 0.159 s; its copy diagnostic exits normally
in 0.540 s. See the
[qualification record](../../test-evidence/wave-6/iat-sqlite-sandbox-20261024.json).
Earlier binaries above lack this binding correction. This remains targeted
qualification, not the full performance gate.

The bundled SQLite was also updated before any concurrent checkpoint work:
the preceding `libsqlite3-sys 0.35.0` contained **SQLite 3.50.2**, which predates
the [WAL-reset race fix](https://sqlite.org/wal.html#walreset). That race concerns
concurrent commit/checkpoint connections. The NORMAL commit cost is a reason
to investigate automatic checkpoint work, not authorization to disable
checkpointing, weaken fsync or introduce that known corruption path.
`rusqlite 0.40.2` now bundles **SQLite 3.53.2**, with statement caching and the
previous fallible unsigned conversions explicitly preserved. A version-floor
regression protects this prerequisite. No checkpoint policy changed yet.
The upgraded engine still spends 17.076 s in 4,763 NORMAL commits and 7.664 s
in 544 FULL commits in the three-run copy diagnostic. The upgrade fixes a
correctness prerequisite; these counters do not establish checkpoint cost.

The next candidate removes whole-base hydration from truncation. A shrink now
retains lazy immutable-prefix reads and durably clips their visibility before
any later extension; marker failures block re-extension until retried.
Three new recovery/failure regressions pass, bringing the host suite to 170
passing tests. Candidate `b9908f33...` passes all 44 isolated lifecycle cases
and six timestamp laps (forced exit max 0.015 s, ordinary graceful max 0.169 s).
Its three-sample sequential comparison reduces write median from 11.413 s to
5.647 s and cumulative operation 6 time from 22.972 s to 1.513 s, with clean
0.104/0.112-second daemon exits. Native write median is 1.887 s; read does not
improve. See the [truncate contract](notes/lazy-truncate.md) and
[retained diagnostic](../../test-evidence/wave-6/lazy-truncate-sandbox-20261024.json).
The full gate and purge-cost decision remain open.

The next diagnostic (`33c3d36e...`) times the same 1,000-frame synchronous
PASSIVE automatic checkpoint through an equivalent WAL hook. It has 172
passing host tests, including default-versus-profiled checkpoint behavior
with a retained reader, plus Clippy/format/release checks. Its targeted VM
copy measurement attributes 18.155 s of 22.844 s COMMIT time (79.5%) to 167
automatic checkpoints, with no checkpoint errors and a graceful 0.485-second
exit. The [retained diagnostic](../../test-evidence/wave-6/checkpoint-profile-sandbox-20261024.json)
establishes the next foreground cost; no asynchronous checkpoint policy exists
in that measured binary.

The follow-up `bc04000b...` implements regular background PASSIVE checkpoints
on a separate backing-store connection, with a shared gate for explicit FULL
barriers and latched maintenance errors. It preserves the metadata commit
window and file-fsync ordering, starts the replacement before disabling the
inline hook, and retries after reader release without requiring another
commit. It also decodes only requested manifest digests on range reads while
retaining full framing/object validation. All 180 host tests, strict Clippy,
formatting and release build pass. All 44 isolated cases and six timestamp
laps pass (forced exit max 0.015 s; ordinary graceful max 0.156 s). Its targeted
copy diagnostic reduces NORMAL/FULL COMMIT time to 3.718/2.661 s, with no
worker errors and graceful daemon exit in 0.332 s. Background checkpoint work
still costs 49.375 s across 3,380 calls, overlapping the foreground; copy and
sequential write/read remain 7.26x and 3.10x/1.60x native. See the
[worker contract](notes/background-checkpoint.md) and
[retained diagnostic](../../test-evidence/wave-6/background-checkpoint-sandbox-20261024.json).
The five-sample full-gate attempt completes correct native and mounted builds
(18 observations each) and both daemons stop gracefully (0.946 s/1.399 s),
without kills or stranded processes. It still fails copy 6.89x, sequential
write/read 3.04x/1.61x, random overwrite 97.48x, Git status 12.02x and no-op
build 1.21x native. The peer initial import exceeds its 120-second bound before
idle-peer measurements begin: after clean shutdown its valid database contains
92,672 of 143,194 source events, with no waiting events. The
[full-gate evidence](../../test-evidence/wave-6/background-full-sandbox-20261024.json)
includes callback observations, Git trace2, clean exits and a verified export.
Git spends about 8.9 s in 50,000 `lstat` calls; metadata lock contention is
measured, while purge cost remains unisolated. A bounded peer-resume diagnostic
is investigating the incomplete initial import; it does not qualify the
missing full-gate phase. It completes the remaining 50,522 events in 68.779 s
and passes five remote-visibility samples (102.425 ms median), with normal
0.170/0.177-second exits and verified exports
([resume record](../../test-evidence/wave-6/background-peer-resume-sandbox-20261024.json)).
The harness also rejects
missing incremental measurements and idle-peer overhead above 20%, verified
against real earlier reports without a host mount
([checks](../../test-evidence/wave-6/benchmark-acceptance-host-20261024.json)).

Candidate `0fb0beae...` adds a bounded, epoch-invalidated namespace entry cache
for stable reads, without cloning projection maps or retaining namespace
handles. Creates/writable opens and working-layer visibility remain on their
existing paths. It also coalesces background checkpoint requests with a minimum
50 ms interval, preserving periodic retries and explicit FULL barriers. All
185 host tests, Clippy, formatting and release build pass. All 44 isolated cases
and six timestamp laps pass (forced exit max 0.013 s; ordinary graceful max
0.171 s), with normal 2.543-second stop after the diagnostic. Copy/Git still
miss at 6.48x/11.99x native. The metadata Open improvement does not translate
to sufficient wall-time improvement. A separate no-optional-locks diagnostic
gets over 99.9% user-mode cache hits but still takes 9.514 s for status. See the
[contract and retained diagnostics](notes/namespace-cache.md).
An explicit read-only purge/standby comparison, after verifying all 50,000
corpus files, measures 10.364 s with purge and 5.170 s with standby retention.
Both reject writes and stop normally (0.794/1.979 s); even standby still misses
the native target. The [cost isolation](notes/cleanup-cache-cost.md) retains
binary/source identities, callbacks and verified exports. Read-only variants
that omit unmodified Cleanup callbacks complete at 7.995 s with purge and
5.258 s with standby, with verified bytes/write rejection and normal
0.955/2.120-second exits. Writer/seal semantics remain unqualified in those
diagnostic builds. Writable `6df02c15...` omits only unmodified Cleanup,
retaining purge, access and durability policy. Its 185 host tests, 44 isolated
lifecycle cases and six timestamp laps pass (forced exit max 0.012 s;
ordinary graceful max 0.158 s). Copy/Git remain 6.45x/9.34x native, with a
normal 2.194-second stop and verified export
([qualification](../../test-evidence/wave-6/modified-cleanup-sandbox-20261024.json)).
The complete performance/build/idle-peer gate remains open.

The next candidate `cad2200f...` removes a whole-hydrated-set search from each
small write, retaining the same marker durability and failed-marker retries.
All 186 host tests, strict Clippy and 44 isolated lifecycle cases pass, as do
six timestamp laps (forced exit max 0.012 s; ordinary graceful max 0.177 s).
Its targeted overwrite median is 2.708 s (12.54x matched native), still failing;
copy and sequential write/read also miss their targets. It stops normally in
2.052 s and its 2.234 GB native export is verified. See the
[marker contract and measurements](notes/hydration-marker-cost.md). The full
five-sample gate now completes all native/mounted/idle-peer workloads with
54 correct build observations. Mounted/native copy, sequential write/read,
random overwrite and Git still fail at 5.13x, 3.51x/2.00x, 14.57x and 9.59x;
clean/no-op builds pass at 1.03x/1.05x. Initial peer import reaches event
143,175 in 201.498 s within the explicit 300-second bound. Remote visibility
passes (37.658 ms median), but idle-peer overhead fails the 20% limit in those
five metrics (+29.3%, +35.0%/+32.3%, +61.5%, +26.2%). Both daemons exit
normally in 0.343/0.256 s without kills or stranded processes. The 5.634 GB
export is verified, and an independent read-only check after shutdown confirms
every byte of the final 1 GiB, including all six overwrite laps. See the
[full-gate record](../../test-evidence/wave-6/hydration-full-sandbox-20261024.json).
No host compilation overlaps timed phases. None of the acceptance ratios,
cache flushing, access checks or explicit-fsync requirements have been relaxed.

A same-binary read-only dispatcher sweep in a fresh isolated guest measures
5.850 s with two workers, versus 7.401/7.782 s in repeated 16-worker controls.
All five volumes reject writes and stop normally; namespace head and verified
exports remain intact. Writable candidate `5cd5005d...` passes 186 host tests,
the binding fixtures, 44 isolated lifecycle cases, six timestamp laps and 36
correct native/reference/candidate build observations. Its paired three-sample
comparison improves Git by 20.8%, but sequential write/read regress 37.0%/25.8%
and random overwrite 13.7%. Both daemons stop normally in 0.944/1.127 s, and
independent backing verification confirms every byte of both final 1 GiB files.
The 2.528 GB export is verified. The global two-worker change is rejected and
the WinFsp default restored exactly. See the
[worker-count evidence](notes/dispatcher-workers.md). This is a diagnostic,
not the full five-sample/peer gate; P0 remains open.
Bounded guest CPU sampling subsequently locates a large contention cost in
WinFsp's pending/processing queue locks: spin-lock waits account for 48.5–50.0%
of sampled server CPU weight in repeated 16-worker controls, versus 5.6% with
two workers. All six profiled daemons stop normally, both exports are verified,
and all traces report zero lost events/buffers. These sampled CPU weights are
not wall-time acceptance results and do not justify reinstating the rejected
global worker reduction. The same note retains exact traces, symbols and
workload interval analysis.
A read-only metadata-only quota then keeps 16 dispatcher workers while limiting
concurrent attribute opens to 2/4/8. Its counters confirm the limits, but warmed
Git medians of 7.955/7.360/7.387 s do not consistently improve repeated
7.514/7.120-second no-quota controls. Ten targeted lifecycle cases and two
active-metadata forced terminations pass, without stranded processes; writes
remain rejected and both exports are verified. The experiment is removed and
the complete prior bridge source restored, with a successful default release
build. The same note retains the failed optimization as evidence; the measured
queue contention still requires a better remedy.

A separate client-cache isolation keeps the filesystem binary, 16 workers and
purge policy fixed. The guest's Portable Git has `core.fscache` unset/disabled.
Enabling that option reduces actual mounted Open callbacks from 51,090 to 610
while Git still reports 50,000 logical index checks. Warmed Git falls from
7.484 s to 0.381 s, but the matched native value also falls from 0.725 s to
0.186 s: even that configuration misses at 2.05x. Repeated default/explicit-off
controls still miss at about 10x native. The daemon stops normally in 0.387 s,
the namespace head is unchanged and the complete export is verified. See the
[client-cache diagnostic](notes/git-client-cache.md). The original acceptance
command and targets remain unchanged; an optional client setting does not
close P0 or qualify the remaining workloads.

Normal/default `1d1ea8c6...` subsequently passes all 44 isolated lifecycle cases
(forced exit max 0.016 s; ordinary graceful max 0.162 s). Sampling-only writable
copy/sequential/random diagnostics retain matching private symbols. Sequential
and random commands spend 10.35%/19.21% of sampled server CPU weight in SHA-256,
while queue spin-lock weights are about 0.1%; the Git contention result must
not be generalized to these workloads. The daemon stops normally in 0.486 s,
the complete final 1 GiB passes independent backing-byte verification, and the
1.615 GB export is verified. The [data CPU diagnosis](notes/data-cpu-cost.md)
records the profiling scope and a source-confirmed duplicate hash/location
lookup in seal preparation. The captured times are not acceptance results.
Candidate `33ba68d0...` removes that duplicate hash/lookup while retaining
validated insertion and pack durability. It passes 187 host tests, strict
Clippy, formatting, release and inert binding checks. The optimized native
512 MiB seal diagnostic falls from 599.4 to 374.3 ms with identical logical
chunk/new-object counts and validated byte probes. Its 44 isolated lifecycle
cases and six timestamp laps pass (forced exit max 0.011 s; ordinary graceful
max 0.152 s), as do 36 native/reference/candidate build observations. Sequential
fsync improves from 1.601 to 1.248 s in the three-sample comparison, but copy,
sequential write/read and random overwrite still fail at 8.68x, 3.13x/1.89x and
33.15x matched native. The complete paired timings, including slower metrics,
are retained in the same note. Both daemons stop normally in 0.930/1.056 s;
all bytes of both final 1 GiB files verify independently, and the 2.638 GB
export is verified. The full five-sample/idle-peer gate remains open.

A subsequent native-only page-cache isolation uses independent copies of the
125,276-event corpus. Raising only the diagnostic writer's cache from 2,000 to
65,536 KiB cuts cache misses, but does not materially reduce create WAL writes;
neither setting spills dirty pages. Three-sample create/receipt and delete
medians improve 6.3%/1.4%, with substantial timing variation. All six copies pass
receipt/deletion/reopen/integrity verification. The cache increase is not adopted
in production. The [data CPU note](notes/data-cpu-cost.md#native-metadata-page-cache-isolation)
retains this rejected hypothesis; it does not change the failed mounted gate.

Candidate `23b4e6aa...` then replaces `current` table delete/reinsert updates
with an UPSERT on the same logical key. Cache, schema, driver and durability
policy remain unchanged. Three native metadata samples reduce create/receipt
and delete medians 20.4%/29.3%, with 14.6%/24.3% fewer WAL page writes;
a return control and all reopen/receipt/integrity checks support the result.
Its 187 host tests, strict Clippy, formatting, release and inert binding fixtures
pass. All 44 isolated lifecycle cases and six timestamp laps pass (forced exit
max 0.012 s; ordinary graceful max 0.192 s), with 36 correct build observations.
The mounted comparison improves copy/delete 18.3%/19.2%, but sequential read
is 17.1% slower. Both daemons stop normally in 0.952/1.166 s, and independent
verification confirms both final 1 GiB files, including all overwritten pages.
Copy, sequential write/read and random overwrite still fail at 7.04x,
4.05x/1.49x and 15.37x matched native. See the
[SQL write diagnosis](notes/data-cpu-cost.md#update-current-rows-without-replacement)
for every paired metric and the unresolved read variation. The full five-sample
and idle-peer gate remains open.

Read-only follow-ups isolate cache behavior without changing production policy.
An A–B–B–A read control after reopening identical verified content does not
reproduce a persistent UPSERT slowdown, but its warmed reads use the kernel
cache and do not recreate the canonical write/fsync state. The original slower
read remains recorded. A separate cached-attribute experiment removes only
nine of roughly 51,000 Git Open callbacks, even when a test client holds all
file nodes present. A 32-file positive control drops Open/security callbacks to
zero, while source audit finds a fixed 100-item driver security-cache capacity.
See the [read control](notes/data-cpu-cost.md#read-control-after-reopening-identical-content)
and [cached-query isolation](notes/cleanup-cache-cost.md#cached-attribute-query-isolation).
All test clients/servers exit cleanly and exports verify. The experimental
build/bridge changes are removed; no production helper, access bypass or cache
policy change is adopted. This does not close any failed performance target.

The next five-sample canonical Git comparison uses a fresh guest and a pinned
upstream memory-filesystem reference. NTFS before/after measures 270.8/275.9 ms,
the memory backend 3,127.3 ms, and Telekinesis 3,246.7 ms. The memory reference
does not implement this project's persistence/sync semantics or establish a
theoretical lower bound. Stopped metadata confirms that `index.lock` changes
invalidate the entire Telekinesis namespace cache. Candidate `de1c25b7...`
scopes invalidation by entity/ancestor IDs, with bounded history and global
fallback on failure, checkout or incomplete history. It passes 193 host tests,
the inert binding fixtures, 44 isolated lifecycle cases and six timestamp laps
(forced exit max 0.004 s; ordinary graceful max 0.100 s). A five-sample A–B–B–A
comparison reduces cache misses from 51,062 to 54 per Git, but wall time stays
around 3.3 s and the same Open/security callbacks remain. All four servers stop
normally and both complete exports verify. See the
[scoped-cache evidence](notes/namespace-cache.md#canonical-git-reference-and-scoped-invalidation).
Git performance and the full workload/idle-peer gate remain open.

The no-self-handle implementation was qualified for lifecycle correctness in **Windows Sandbox**
(a separate Windows kernel under the Microsoft hypervisor). All stores and
mounts are on the guest's local disk; only inputs and exported evidence cross
the guest boundary. All **32 lifecycle cases passed** on Windows build 26100
with WinFsp 2.1.25156. Maximum measured forced exit was **0.015 s**; maximum
ordinary graceful exit was **0.157 s**. Injected flush failures exited with
code 1 without a kill, released the mount and recovered the saved scratch on
reopen. Full records, binary/source hashes and host checks are in
[`mount-lifecycle-sandbox-20261023.json`](../../test-evidence/wave-6/mount-lifecycle-sandbox-20261023.json).
The full-gate attempt failed; this lifecycle result alone does not authorize
Wave 6 cutover or mounted performance qualification on the host.

Sandbox is suitable for these lifecycle checks. Use a
persistent Windows VM when retained kernel dumps or repeatable benchmark
configuration are needed. Closing Sandbox discards its local evidence.

`scripts/mount_acceptance.py --isolated-vm --binary <tkfs.exe> --home <new-directory>`
is the lifecycle matrix. It exercises real drive-letter and directory mounts,
graceful/forced stops at idle and under cached writes/fsync/delete-recreate,
save/restart, persistent flush failure with scratch recovery, self-open refusal
before Rust while holding the store lock, and failures after
volume creation, mount-point creation and dispatcher startup. Startup failures
must contain the injected-stage marker; an unavailable driver cannot pass them.
All namespace probes run in child processes with bounded waits. The incremental
`acceptance.json` retains PIDs, exit records and the first failure. `--report`
optionally mirrors that report to an evidence export folder. Stop the matrix
at the first failure; do not reuse a guest after a cleanup timeout.

The follow-up audit also fixes these gaps in the original implementation:

- A failed control flush is remembered before publishing `stopping`. A later
  successful flush cannot turn the daemon's failed shutdown into exit code 0.
- Orchestrator startup and runtime errors now use the same shutdown/detach
  path as control stop, including bind/catalog failures after mount attachment.
- VM cached smoke exposed a second correctness issue: paging writes through a
  retained reader after delete/recreate updated the old entity's scratch while
  file fsync sealed 23 zero bytes from the replacement's scratch. Native paging
  writes and per-file flushes now resolve the visible entity consistently with
  cache reads. Live renamed entities keep their identity if the old name is
  reused; existing local edits keep their original conflict parent. Regression
  tests and both VM cached smokes pass without dropping the durability check.

The harnesses now share the 20-second graceful deadline between the stop RPC
and process exit. A fallback kill fails graceful qualification even when the
kill succeeds. Kill failures still produce an exit record. Benchmark stores
remain under `test-runs/bench` after failure instead of being deleted by a
temporary-directory context.

The historical diagnosis below describes the implementation before the
no-self-handle fix.

## Performance acceptance: failed, demand remains open

The isolated Sandbox full-gate attempt on intermediate binary `85b168b9...`
failed. Five-sample medians versus its matched native baseline were:

| Workload | Mounted / native | Target |
|---|---:|---:|
| Copy 2,000 small files | 12.48x | <= 1.3x |
| Sequential 1 GiB write | 3.33x | <= 1.2x |
| Sequential 1 GiB read | 2.91x | <= 1.2x |
| Random 4 KiB overwrite | 8.98x | <= 1.5x |

During preparation for the 50,000-file Git workload, `git add -A` exceeded
600 s and its process (PID 3208) survived the 10 s post-kill deadline. The
harness reported a stranded command and failed the run. The filesystem daemon
(PID 13612) then exited **gracefully in 0.998 s**; neither Git nor TKFS was
present at the subsequent process check. No further mounts were started in
that Sandbox, and its reports, binary and recoverable stores were exported
before disposal.

Git status and .NET build measurements were **not reached**; the paired idle
peer phase was not started. This run neither qualifies those workloads nor
isolates purge cost from the rest of the filesystem overhead. It is not
evidence of another deleting daemon or a proven recurrence of the original
root-cleanup dependency. The final early self-open guard and private-draft
fsync follow-up are not included in this intermediate benchmark binary.

See the [cost/timeout evidence](../../test-evidence/wave-6/mount-lifecycle-cost-sandbox-20261023.json)
and its linked native/mounted reports. Performance acceptance remains open;
do not disable caching or cleanup flushing to turn this failure into a pass.
Any Close-based fallback must first prove cached-byte delivery and preserve
file-fsync and recovery semantics in the VM, including retained readers.

The [fsync cost follow-up](notes/fsync-cost.md) removes a whole-working-tree
scan per file Flush and a redundant checkpoint after a fully synchronized seal.
It preserves cleanup flushing and caching. Reduced VM measurements still miss
the targets; they do not close this demand or isolate purge cost. The follow-up
also makes an incomplete SQLite checkpoint a reported durability failure.
The follow-up release `36cf9467...` passes all 32 isolated lifecycle cases
([report](../../test-evidence/wave-6/mount-lifecycle-fsync-wal-sandbox-20261024.json));
forced exit max is 0.016 s and ordinary graceful exit max is 0.158 s. Its full
performance gate also failed on 2026-10-24: copy 11.65x native, sequential
write/read 3.73x/3.15x and random overwrite 11.44x. Git add again exceeded 600 s;
Git status, mounted builds and the peer phase were not reached. The daemon
exited gracefully in 1.658 s; the later process query found neither Git nor the
daemon. All exports completed before the guest was disposed. See the
[follow-up evidence](../../test-evidence/wave-6/fsync-wal-cost-sandbox-20261024.json).
The timeout runner now terminates Windows launcher/child trees within the
existing kill deadline, covered by a non-mounted regression. The old runner's
stranded-pipe report alone cannot distinguish a live child from a deleting
process. No further mounts were started after that cleanup timeout. This does
not authorize cutover or host mounted qualification.

The subsequent metadata-cost release `3f3be093...` failed the last recovery
case (31/32 passed). A deterministic VM replay proved that recovery could
discard 110,592 valid bytes when their timestamp equaled the clean marker.
The [recovery fix and before-fix evidence](notes/recovery-timestamps.md) retain
that failure. Release `cf54ca85...` compares materialized bytes before discard;
its expanded matrix adds equal-timestamp recovery for both mount types (34
cases). All 144 host Rust tests and all 34 VM cases pass for that release
([report](../../test-evidence/wave-6/mount-lifecycle-recovery-sandbox-20261024.json)).
Its copy median still fails at 11.59x native. A subsequent
[create-path optimization](notes/create-pack-flush.md) removes shared pack
flushes from metadata creation; it passes 145 host tests and awaits its own VM
qualification. Cache and cleanup flushing remain enabled.

## Performance triage: read the counters before the next VM lap

The [local cache-coherence follow-up](notes/cached-write-times.md), release
`e8ee0871...`, passes all 36 expanded lifecycle cases and six repeated timestamp
laps. Maximum forced exit is 0.015 s; ordinary graceful exit maximum is 0.124 s.
Its [matrix](../../test-evidence/wave-6/mount-lifecycle-lazy-coherence-sandbox-20261024.json)
and [additional evidence](../../test-evidence/wave-6/lazy-coherence-sandbox-20261024.json)
are retained. Its full performance gate failed and stranded the daemon as
described above; the short matrix did not establish driver lifecycle safety.

The next [seal snapshot/recovery candidate](notes/seal-snapshot-recovery.md)
passes host regressions but is not included in that binary. In particular, a
new replay found stale-sidecar receipt recovery replacing `new bytes` with
`old bytes`; recovery now marks extensions before rebasing. Draft revisions
and a persisted immutable visibility boundary protect writes and truncate/
re-extend operations while seal locks are released for durability. This needs
its own complete VM matrix and measurements. Metadata split counters and actual
map-copy counts are now available in `mount-stats` for the next diagnostic.

The next [lazy-draft candidate](notes/lazy-draft-open.md) removes content copying
and host scratch creation from Open/Read. A paired VM diagnostic confirmed the
predicted movement: 501 immutable files created 501 host scratches before the
change and zero after; cold open-stage scratch time fell from 658 ms to 14 ms,
and round wall time from 1,640 ms to 965 ms. This is a diagnostic, not the gate.
Its expanded lifecycle lap stopped at 19/20 passing cases when the directory
timestamp case detected a 1 ms change across reopen/seal. Earlier binaries also
failed that new check; the previous 34-case matrices did not include it.
All daemons exited within their bounds. The [diagnostic and failure](../../test-evidence/wave-6/lazy-open-diagnostic-sandbox-20261024.json)
are retained, with complete native stores and timestamps exported locally under
`test-runs/mount-sandbox/cache-output/lazy-open-v5`.
The follow-up synchronizes automatic times published by Write with Cleanup;
its regression failed before the correction. Its later successful VM
qualification is recorded above. No performance acceptance or cutover is
authorized by this diagnostic lap.

The historical metadata-open release `51e70e72...` also completed a full-gate
attempt: copy 7.38x, sequential write/read 3.47x/1.36x, random overwrite 9.96x
and Git status 8.14x native. Clean build was 0.82x and no-op 1.21x, but the
incremental build returned the old answer (65024 instead of 65025), so build
correctness failed and the peer phase was not started. The daemon stopped
gracefully in 1.015 s. Its [34-case lifecycle report](../../test-evidence/wave-6/mount-lifecycle-metadata-open-sandbox-20261024.json)
and [cost/correctness evidence](../../test-evidence/wave-6/metadata-open-cost-sandbox-20261024.json)
are retained; neither qualifies the newer timestamp or lazy-draft changes.

Added 2026-10-24. Each candidate so far took a full VM lap and came back with a
whole-workload ratio that cannot say which cost moved. The daemon already
counts calls and time per callback and per open stage (`rpc mount-stats`). Two
VM runs retained them:
[Git/build diagnostic](../../test-evidence/wave-6/callback-stats-git-build-sandbox-20261024.json)
and [fsync-after gate](../../test-evidence/wave-6/callback-stats-fsync-after-sandbox-20261023.json).
Operation numbers follow `tk_call`: 1 security-by-name, 2 create, 3 open,
4 read, 5 write, 7 flush, 9 cleanup, 10 close. Mean user-mode cost per call:

| Callback | Git/build VM | fsync-after VM | Reading |
|---|---:|---:|---|
| Open existing file | 25.9 ms | 0.25 ms | Dominant in Git/build: 509 s over 19,691 opens |
| Create | 4.4 ms | 2.5 ms | Dominant in copy |
| Open stage `scratch` | 8.1 ms | 0.48 ms | Host scratch, sidecar and hydration |
| Open stage `metadata_total` | 2.3 ms | 0.57 ms | Store lock held: lookup, event, SQLite, projection |
| Open stage `metadata_wait` | 0.013 ms | 0.002 ms | The store lock itself is not contended at open |
| Close | 2.2 ms | 0.39 ms | Waits for the scratch mutex |
| Flush | n/a | 18 ms | Three fsyncs per file fsync |
| Cleanup | 0.011 ms | 0.030 ms | Purge cost is kernel time, invisible here |

What this establishes, and what it does not:

- **Opening an existing file copies it.** `WorkingLayer::open_impl` creates a
  scratch, sets its length and writes every locally present chunk through
  `packs.get` before the open returns, plus a sidecar record. A read-only open
  of a 1 GiB file copies 1 GiB. `packs.get` also takes the pack state mutex
  that `flush_locked` holds across the segment fsync and the FULL-sync
  `index.sqlite` commit, so opens stall behind seals. This is the largest
  measured cost and the first thing to change.
- **Metadata is real but second.** In the copy-heavy run the metadata stage is
  about half of create. It is not yet split between SQLite statements, the
  per-event `content_anchor`/`version` queries, JSON encoding and projection
  updates. `Projection::index` uses `Arc::make_mut`; any live clone of a map,
  such as the `anchors` clone `Engine::children` holds during a listing, turns
  the next create into a copy of the whole map. Replacing SQLite is not
  justified until a split counter shows statements dominate.
- **Close waits on sealing.** `seal_batch` holds every batched scratch guard
  through the pack fsync and the FULL metadata commit, and Close needs that
  guard to release its handle.
- **Flush pays three fsyncs**: pack segment, `index.sqlite` at FULL and the
  metadata receipt commit at FULL. Folding the object index into the metadata
  database (section 7 of the replacement plan) leaves two.
- **Purge cost is not isolated by these counters.** Cleanup is microseconds in
  user mode, but callbacks overlap across dispatcher threads and background
  work. Their summed times cannot be subtracted from wall time to attribute a
  residual to the kernel or purge. Use matched flag-isolation diagnostics and
  kernel/CPU stacks; read-only isolation does not qualify a writable policy.

Candidate order, each with its expected counter movement:

1. Open without copying: serve reads of an unmodified file from the pack and
   LRU, and create or hydrate the scratch on the first write. This includes
   `ConstrainedIo` paging writes on read-only handles (section 3a of the
   plan), which must still land in the entity's shared scratch. Expect open
   `scratch` and open callback time to fall to near the `handle` stage.
2. Release scratch guards before durability work in `seal_batch`: snapshot
   each prepared reference, drop the guards, then flush and commit. Recheck
   `changed` afterwards so a concurrent write produces a successor seal and is
   not cleared. Expect close to fall to microseconds.
3. Split `metadata_total` into SQLite execution, version queries and
   projection update, and count map copies in `index`. Act on whichever
   dominates.
4. One database for events and the object index; one fewer fsync per flush.
5. Only then size purge cost with matched flag-isolation experiments, retaining
   callback observations and kernel/CPU traces rather than subtracting
   overlapping callback totals from wall time.

Lap discipline, so a VM lap answers a question:

- Every lifecycle and benchmark lap exports `mount-stats` next to its report,
  and that file is copied into `test-evidence/wave-6/`. A lap without
  counters is not a performance diagnostic.
- Write down the counter each candidate is expected to move before the lap.
  A whole-workload ratio that moves without the predicted counter is noise or
  a different cause; do not stack the next candidate on it.
- Iterate on candidates 1 to 3 through engine-level tests on the host, which
  call `Engine` directly with no WinFsp mount. Use the VM only to confirm the
  predicted counter change and rerun the lifecycle matrix. Mounted runs stay
  in the VM until this demand closes.
- These changes keep caching, cleanup flushing, fsync barriers, receipts,
  recovery comparison and the no-self-handle rule intact.

## Observed failure

The replacement smoke test's peer B, PID 49692, was serving `Y:` with
`--state test-runs/replacement/1792775748686240700/b daemon --mount Y:`.
Here `--state` is a directory option, not a status operation. Windows has already
set exit code 1 and `IsProcessDeleting`; its last thread (58708) has pending I/O
and the process remains unsignaled. Opening it with terminate access succeeds.

The administrator-captured kernel dump shows:

```text
PspExitLastThread -> PspRundownSingleProcess -> ObKillProcess
  -> ExSweepHandleTable -> ObCloseHandleTableEntry
  -> IopCloseFile+0x15d -> KeWaitForSingleObject
```

The pending request is `IRP_MJ_CLEANUP` (0x12), returned as `STATUS_PENDING`,
with an infinite completion-event wait. Its file object has name `\` (a root
directory). The IRP is in a cancel-safe queue; its completion chain includes
`FLTMGR!FltpPassThroughCompletion`.

The full live kernel dump confirms that both the root file's device and the
pending request's device belong to `winfsp-x64.sys` (driver object
`\FileSystem\WinFsp+20261002T205313Z`). Filter Manager is above it.
The root has WriteAccess=true and SharedRead/SharedWrite/SharedDelete=true,
matching `tk_cache_open()`'s GENERIC_WRITE root handle.

A fresh PowerShell also failed `InitializeDefaultDrives`. The earlier Visual
Studio stall is a reported symptom, not a proven instance of this same wait.

## Unsafe application and test sequence to remove

1. [`tk_cache_open`](../../native/mount.c) opens the served root with
   `CreateFileW`; [`attach`](../../src/mount.rs) retains that handle in `MOUNT`
   for `FlushFileBuffers`. The filesystem server consequently depends on I/O
   against its own mounted namespace.
2. Normal `detach()` closes that root and then stops the dispatcher. Forced
   process termination bypasses this Rust cleanup and kills the callback workers.
3. [The smoke harness](../../scripts/replacement_smoke.py) falls back from a
   failed/timed-out graceful stop to `proc.kill(); proc.wait()`, with no timeout
   on the final wait. Windows can then wait for root cleanup which needs the
   filesystem service it just terminated, stranding the daemon and the harness.

In addition, [control::execute](../../src/orchestrator/control.rs) pre-flushes
`shutdown` with `flush_cached(true)?` before dispatching the operation, and
`detach()` starts with the same fallible flush before releasing mount resources.
A flush failure therefore skips shutdown/detachment work. These error paths must
become lifecycle-safe while retaining recoverable data and reporting durability
failure. The captured root FILE_OBJECT's FinalStatus is `0xc0000022`
(ACCESS_DENIED), consistent with the write failures; this does not prove which
operation first caused the earlier freeze.

The dump proves the termination-time root cleanup hang. The self-mount handle
is the source-level lifetime dependency to eliminate; reordering graceful
cleanup or adding a `Drop` handler alone does not cover forced termination.
A folder mount still uses `WinFsp.Disk` and these callbacks; replacing `Y:`
with a folder is not an accepted fix. Drive-letter enumeration can expose the
hung mount to other applications.

Peer B also logged two `mount I/O operation 5: ACCESS_DENIED` errors. Audit
[`Engine::write`](../../src/runtime/engine.rs)'s per-handle writable check
against WinFsp cached/paging writes. These errors are a separate lead; the
post-termination dump does not establish why graceful operation first failed.

## Why the kernel cannot recover

Process termination runs `ExSweepHandleTable` on the dying process: every
handle is closed in table order, one at a time, synchronously. Closing the
retained `Y:\` root handle issues `IRP_MJ_CLEANUP`, which `winfsp-x64.sys`
queues for the user-mode file system. That file system is the process being
swept; its dispatcher threads were already terminated, so nobody will ever
transact the request. The dump shows the volume device still alive, so the
volume (fsctl) handle had not been swept when the sweep blocked on the root.
Handle-table order is not under user-mode control (indices are reused), so no
cleanup ordering, `Drop` handler or "close the root first" change can make a
retained self-handle safe. The mount passes `IrpTimeout = 60 000 ms`
([`attach`](../../src/mount.rs)); the pending cleanup was not timed out, so
driver-side expiry must not be relied on either. The sweep thread belongs to
the deleting process: it holds the process alive, drive-letter enumeration
still reaches the hung volume, and anything that touches that volume
(Explorer, a fresh PowerShell's `InitializeDefaultDrives`, Visual Studio)
blocks behind it. The only acceptable fix is that the server process never
holds a handle to, or issues I/O against, its own served namespace.

This is the common exit path, not a harness quirk. Task Manager, `taskkill /F`,
a console close (5 s grace, then termination), a panic that aborts the process,
and an operating-system shutdown while the daemon runs all take the same route.
With the retained root handle, an OS shutdown with a mounted daemon is expected
to hang as well.

## Probable trigger: paging writes on a read-only handle

Peer B's two `mount I/O operation 5: ACCESS_DENIED` errors are most likely the
first cause of the chain, not a separate lead. With `FileInfoTimeout = INFINITE`
the Windows cache is shared per pathname, and the Cache Manager issues its
paging writes on the file object it initialised caching with. That can be a
handle opened read-only before the writer opened. [`Engine::write`](../../src/runtime/engine.rs)
rejects any write on a handle whose `writable` flag is false, so the lazy
writer's write fails, the dirty pages stay dirty forever, the root
`FlushFileBuffers` fails with ACCESS_DENIED, every `flush`/`shutdown`/`detach`
returns before releasing anything, the harness times out, kills the daemon, and
the sweep hangs. The smoke test's paired delete/recreate step opens a reader
(`remote.open("rb")`) before the writer on the same path, exactly this pattern.
The root FILE_OBJECT's FinalStatus of `0xc0000022` is consistent with it.

WinFsp already enforces access at Create; the user-mode file system must accept
a write marked `ConstrainedIo` (paging I/O, which must not extend the file) on
any handle of the entity, regardless of that handle's own access. The entity's
scratch file is shared by all handles, so the bytes land in the right place.

## Further call sites and gaps

- [`orchestrator::run`](../../src/orchestrator/mod.rs) calls
  `engine.shutdown()?` before `detach()?`. A seal or checkpoint failure there
  also skips the detach, and `main` then returns with the dispatcher running and
  the root handle open; `ExitProcess` kills the dispatcher threads and the sweep
  hangs the same way.
- [`scripts/bench.py`](../../scripts/bench.py) has the same unbounded
  `proc.kill(); proc.wait()` fallback as the smoke harness.
- Nothing prevents the server from opening its own namespace again. WinFsp
  reports the originating process of Create/Open requests
  (`FspFileSystemOperationProcessId`); the shim must refuse opens from its own
  PID so a regression fails loudly instead of re-introducing the hang.
- The volume drain that needed the root handle is documented only in
  [the step 7 note](notes/replacement-step-7.md), not in the plan. The plan's
  "seal on close" (section 4) silently assumed cached bytes reach the server
  before Cleanup; they do not unless the driver flushes them. The plan now
  carries the invariants in its Windows cache and lifecycle section.

## Required fix and acceptance

Design decision (2026-10-23): no helper process and no self-handle. The server
process holds no handle to, and issues no I/O against, the mounted namespace.

- Delete `tk_cache_open`/`tk_cache_flush`/`tk_cache_close`, the retained root
  handle in `MOUNT`, the cache-drain thread and the pending-close bookkeeping.
  Cached bytes reach the server through the driver instead: set
  `FlushAndPurgeOnCleanup = 1`, so the FSD flushes (and purges) a file's cached
  pages as Write callbacks before it delivers Cleanup. Seal on close then works
  as the plan describes, with the existing write-back delay. Explicit fsync
  stays a durability point because WinFsp flushes that file's cache before
  posting Flush. Dirty cached pages of handles still open at a forced kill are
  lost, which is the plan's stated crash contract.
- Measure the purge cost against the benchmark gate in the VM. If `git status`
  or the build workloads regress past their targets, the fallback is to seal at
  Close (after the Cache Manager's lazy writes) instead of at Cleanup, keeping
  the flag off. Either way the first rule above holds; the choice only moves
  seal latency.
- Accept `ConstrainedIo` writes on any handle in `Engine::write`; keep the
  access check for ordinary writes.
- Refuse Create/Open requests whose originating process is the server itself
  (STATUS_ACCESS_DENIED, logged once) in the native shim.
- Reject unqualified WinFsp dependencies before volume creation. Verify the
  exact selected DLL/SYS files and the selected loaded driver's identity;
  a DLL version, configured service filename or graceful teardown alone cannot
  exclude the kernel notification deadlocks. Retain negative preflight evidence.
- Make every shutdown path release mount resources regardless of seal or flush
  failure: the control `shutdown` sets `stopping` before reporting the flush
  error, `detach()` stops the dispatcher before returning the final seal error,
  and `orchestrator::run` runs both `engine.shutdown()` and `detach()` and
  reports the first error afterwards. A failed flush is reported as a durability
  failure; recoverable scratch files stay on disk for the startup re-seal.
- Bound graceful stop, diagnostic capture and the post-kill wait in every mount
  harness (`replacement_smoke.py`, `bench.py`). On timeout, retain PID, mount,
  log and exit state, write them next to the daemon log, and fail the run; never
  wait forever, report success or start further mounts over a stranded process.
- Inject seal/flush failure and partial-attach failure. Shutdown must report
  the durability failure and retain recoverable data while releasing mount
  resources within a bounded deadline; it must not skip cleanup because a flush
  failed or claim the failed flush succeeded.
- In an isolated Windows VM, prove graceful exit within the existing 20-second
  smoke deadline and forced exit within 10 seconds at idle, during cached writes
  and flushing, and during paired cached delete/recreate with retained readers.
  No deleting daemon, pending root cleanup or stale usable mount may remain.
- Repeat graceful and forced stops while cached writes/fsync and independent
  renames generate local notifications concurrently. The driver must also
  complete the full Git/build gate; the 2.1 notification/rename hang escaped
  the earlier short matrices.
- Repeat with drive-letter and directory mounts. Check fresh PowerShell drive
  initialization and unrelated native filesystem operations stay responsive.
  Re-run the small existing save/restart and cached-update checks, including a
  reader opened before a writer on the same path with no ACCESS_DENIED logged.

Until these pass, mounted repro/qualification runs belong in an isolated VM,
not the development host. No mount was created on the development host; the
later acceptance and benchmark runs above used isolated Windows Sandbox guests.

## Evidence retained

Local dump/analysis directory: `%TEMP%/tkfs-hang-49692/`. The targeted kernel
dump was captured at 15:18:50 America/Sao_Paulo on 2026-10-23. Its concrete
objects are ETHREAD `ffffd209c8988080`, IRP `ffffd209a227eca0` and
FILE_OBJECT `ffffd209f6b12e60`. `engine-kernel.log`, `engine-follow.log`,
`engine-object.log`, `engine-resolve.log`, `exit-state.json` and
`findings.txt` preserve the queries, stack and source references. The full
Task Manager dump is under `%LOCALAPPDATA%/Microsoft/Windows/TaskManager/LiveKernelDumps/livedump.DMP`;
`engine-live.log` and `live-resolved-objects.json` retain its resolution.
Memory dumps stay local; the confirmed stack and actionable gate are recorded
here so the demand does not depend on temporary files surviving.
