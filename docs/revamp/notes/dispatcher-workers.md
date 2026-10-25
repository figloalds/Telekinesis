# Dispatcher worker count isolation

WinFsp Beta5's [dispatcher startup](https://github.com/winfsp/winfsp/blob/v2.2B5/src/dll/fs.c)
selects its default from the process affinity and clamps it to 4–16 workers;
explicit counts have a minimum of two. A fresh isolated guest reports 32
affinity CPUs. The comparison varies only the requested dispatcher count in
one mutation-rejecting read-only binary. Purge, modified-only Cleanup, access
checks, sealed corpus, Git arguments and user-mode cache remain the same.

Three warmed samples per configuration:

| Requested workers | Git status median |
|---|---:|
| 16, first | 7,401.2 ms |
| 2 | 5,849.8 ms |
| 4 | 6,116.9 ms |
| 8 | 6,876.3 ms |
| 16, repeated | 7,781.9 ms |

Two workers improve this read-only measurement by 21–25% against the two
16-worker controls. This identifies a dispatcher-concurrency cost; it does
not establish which kernel/user synchronization mechanism accounts for it.
It also does not meet the native target or qualify writable workloads.

Every volume rejects the client write probe. All five daemons stop normally
in 0.364–0.437 s, without kill or stranded process, and the namespace journal
head is unchanged. Invalid counts `0`, `32` and `4x` are rejected before volume
creation. Both controller/store exports pass CRC, SHA-256 and timestamp size
checks. See the [retained comparison](../../../test-evidence/wave-6/readonly-dispatch-sandbox-20261024.json).

The previous guest no longer existed when work resumed (`wsb list` was empty).
The new guest restores the earlier verified corpus export with exact mtimes,
installs the exact official Beta5 hashes, and starts its selected SxS driver
before any mounts. The sweep compares configurations inside this new guest;
its absolute times are not a matched cross-guest benchmark comparison.

The runtime override is compiled only into the read-only diagnostic. Both
that hook and its setting name are absent from the default binary. The host
suite passes 186 tests, Clippy/format/release and five inert binding fixtures
([host checks](../../../test-evidence/wave-6/readonly-dispatch-host-20261024.json)).

Writable candidate `5cd5005d...` selects two workers when the caller requests
zero. Relative to the last full-gate reference `cad2200f...`, only the native
bridge source changes; cache, purge, access and durability policy stay intact.
It passes 186 host tests and the binding checks
([record](../../../test-evidence/wave-6/two-worker-host-20261024.json)), all 44
isolated lifecycle cases and six timestamp laps. The paired writable comparison
completes 36 correct build observations across native/reference/candidate and
both daemons stop normally in 0.944/1.127 s without a kill or stranded process.

Three warmed samples in the same guest give:

| Workload | Reference, 16 workers | Candidate, 2 workers | Change |
|---|---:|---:|---:|
| Git status | 7,508.7 ms | 5,947.0 ms | -20.8% |
| Sequential write | 3,176.2 ms | 4,349.8 ms | +37.0% |
| Sequential read | 1,474.1 ms | 1,854.7 ms | +25.8% |
| Random overwrite | 3,215.6 ms | 3,656.4 ms | +13.7% |
| Copy small files | 8,720.3 ms | 8,973.8 ms | +2.9% |

This diagnostic uses one reference-then-candidate order, rather than the full
five-sample/peer gate. It has no same-guest native Git baseline. The data
regressions are sufficient to reject a global two-worker default despite the
Git gain. The WinFsp default is restored; the bridge source is byte-identical
to the source before that candidate. The diagnostic-only read-only override
remains compiled out of normal builds.

After shutdown, independent verification reconstructs every byte of each final
1 GiB file from the stopped native backing packs, including all four overwrite
laps. Both match the same expected SHA-256. The 2.528 GB export passes CRC,
SHA-256 and timestamp size checks. See the
[retained writable comparison](../../../test-evidence/wave-6/two-worker-sandbox-20261024.json).
P0 remains open; the concurrency mechanism needs further diagnosis before a
different scheduling policy can be justified.

## CPU sampling follow-up

Three bounded WPR captures in the same isolated guest retain context-switch
and CPU stacks. A second set uses a custom
[SystemProvider](https://learn.microsoft.com/en-us/windows-hardware/test/wpt/systemprovider)
with CPU samples and image/process identity only, without context-switch or
ReadyThread tracing. This reduces instrumentation overhead. All six volumes
reject writes, leave the namespace head unchanged and stop normally, without
kill or stranded process. Both exports pass CRC, SHA-256 and timestamp size
checks. All six ETLs report zero lost events and buffers. See the
[complete captures](../../../test-evidence/wave-6/dispatch-cpu-profile-sandbox-20261024.json)
and [sampling-only captures](../../../test-evidence/wave-6/dispatch-cpu-sampling-sandbox-20261024.json).

Offline xperf analysis resolves matching Windows and WinFsp PDB identities.
The sampling-only analysis crops each trace to the controller's UTC Git status
interval. It finds:

| Workers | Profiled wall time | Server sampled CPU weight | Spin-lock wait weight | Share of server weight |
|---|---:|---:|---:|---:|
| 16, first | 8.382 s | 26.058 CPU s | 12.646 CPU s | 48.5% |
| 2 | 6.719 s | 8.060 CPU s | 0.451 CPU s | 5.6% |
| 16, repeated | 8.301 s | 26.718 CPU s | 13.371 CPU s | 50.0% |

Weights are sampled CPU estimates across concurrent threads, so they can exceed
wall time. They are not acceptance timings. Inclusive stacks place the cost
under `FspVolumeFastTransact`, including `FspIoqPendingAcquireLock`,
`FspIoqProcessAcquireLock` and `FspIoqRetriedIrpCount`. Their inclusive counts
overlap and must not be added. The exact Beta5
[queue source](https://github.com/winfsp/winfsp/blob/14d6e0280ae15e2550c44d6dbc476be51649727f/src/sys/ioq.c)
defines the pending/processing queue locks. This establishes a kernel queue
contention cost beyond the Rust callback timers; it does not establish a safe
replacement scheduling policy or account for all remaining overhead.

The first online symbol-resolution attempt exceeded its 240-second limit; its
host-only xperf child was terminated successfully and the failed record is
retained. Analysis then completed using locally cached, matching symbols.
This was an offline analysis timeout, with no mounted process involved.
No production scheduling, cache, durability or access policy changed.

## Rejected metadata-only quota

Read-only diagnostic `2d3604bf...` keeps 16 dispatcher workers and adds a
semaphore only around metadata-only existing-file open operations. It excludes
data/execute/write/delete access, retains the library's access checks and
refuses self-opens before any semaphore wait. The hypothesis was that fewer
active metadata operations could reduce queue contention while leaving data
I/O workers available. The setting and implementation are absent from normal
builds. Its 186 host tests, format, strict Clippy, release builds and five inert
binding fixtures pass
([host record](../../../test-evidence/wave-6/readonly-metadata-gate-host-20261024.json)).

Ten read-only-compatible lifecycle checks pass across drive-letter/directory
mounts: self-open while holding Core, three attach-failure stages and idle
forced termination. Two additional forced terminations during active Git
metadata operations complete in 0.037/0.036 s; both clients exit with code 128
within their ten-second bound. No process is stranded.

| Metadata quota, 16 dispatcher workers | Warmed Git median |
|---|---:|
| None, first control | 7,513.9 ms |
| 2 | 7,955.3 ms |
| 4 | 7,360.1 ms |
| 8 | 7,386.8 ms |
| None, repeated control | 7,119.9 ms |

Each quota handles 202,352 matching requests, reaches its configured maximum
of 2/4/8 and drains to zero. Every volume rejects writes and stops normally
in 0.377–0.450 s. The namespace head is unchanged and both exports pass CRC,
SHA-256 and timestamp size checks. See the
[retained comparison](../../../test-evidence/wave-6/readonly-metadata-gate-sandbox-20261024.json).
The quota has no consistent wall-time gain against repeated controls; it was
removed and the complete preceding bridge source restored byte-for-byte.
Its proposed CPU-spin reduction was not sampled, and must not be claimed from
these wall times. This does not qualify writable scheduling or close P0.

## Public batch transport isolation

The next diagnostic keeps 16 active operation workers and changes the transport
to the public `FspFsctlTransact(..., Batch=TRUE)` API. It delegates to the same
SDK operation handlers and guards, preserving request kind/hint, origin PID,
response alignment and `STATUS_PENDING` behavior. The SDK still owns the
`STOP0 -> join -> STOP` sequence. A separate waiting controller joins the 16
workers; no served-namespace handle or cache-drain helper is added.

Nine host fixtures exercise protocol buffers, PID context, exactly-once
responses, partial thread creation and shutdown, with unchanged handle counts.
Their executable imports no WinFsp functions and cannot load the real DLL or
driver. The [host record](../../../test-evidence/wave-6/batch-transport-host-20261025.json)
includes the source audit at commit `14d6e028...` and the fixtures.

Two failed setups are retained. A pre-created directory mount point is refused
with `STATUS_OBJECT_NAME_COLLISION`, with a clean process exit
([record](../../../test-evidence/wave-6/memfs-batch-setup-failure-20261025.json)).
The next prototype omits the SDK's `Transact0` readiness handshake: its directory
self probe fails with `ERROR_OPERATION_ABORTED` before any callback. It also
exits cleanly, and no later mount runs in that attempt
([record](../../../test-evidence/wave-6/memfs-batch-readiness-failure-20261025.json)).
The corrected implementation sends readiness from startup and every receiver.
The same inert regression fails with the old implementation and passes after
the fix. Pinned SDK/kernel source enables early-IRP rejection and marks volume
readiness on the empty transaction. Readiness is an explicit protocol
requirement, not something to infer from a thread existing.

One executable `3a4a9f5d...` then compares stock and batch dispatch on the
upstream memory filesystem in the isolated guest. Eight small lifecycle cases
pass: normal/forced stops, drive-letter/directory mounts, 32 byte-verified
files with flush/rename/delete, and active metadata clients during forced
termination. All 12 started servers refuse the test-only self-origin request.
Maximum forced exit is 0.003 s; all normal stops, including the large corpus,
complete within 0.135 s. No process is stranded.

Five measured canonical Git samples after warmup, with every one of 50,000
fixture files byte-verified before each mounted phase:

| Phase | Dispatcher | Median Git |
|---|---|---:|
| A1 | Stock | 3,375.6 ms |
| B1 | Batch | 3,198.2 ms |
| B2 | Batch | 3,206.1 ms |
| A2 | Stock | 3,139.9 ms |

Native before/after medians are 291.9/287.1 ms. Every measured mounted run makes
51,096 Open and 51,136 security callbacks, no Write callbacks, and 50,000 logical
Git index checks. The batches do not beat both stock controls. The original
performance target still fails, so this dispatcher is not promoted.

Across each entire batch phase, including fixture creation/verification and
Git, 1,475,503 requests produce only 143,595–144,424 requests in multi-request
batches. There are 354,787–355,228 empty receives and 1,753,763–1,754,649
transactions. Those totals do not isolate timed Git or account for its wall
time. They show that enabling batches while retaining 16 independent receivers
does not substantially coalesce this workload. Further transport work needs
to distinguish receivers from callback processing capacity; the rejected
global worker reduction remains rejected.

The [retained comparison](../../../test-evidence/wave-6/memfs-batch-sandbox-20261025.json)
contains full snapshots, traces, source/binary identities and stop records.
Its 119-member export passes CRC, SHA-256 and timestamp-size verification, and
the final runner handle is missing. The memory backend has no Telekinesis
receipts, persistence, synchronization or conflict logic; this is transport
isolation, not writable or full acceptance. Production source hashes still
match the qualified scoped-cache candidate. No host compilation or heavy host
I/O overlaps timing. P0 remains open.

## Split receive, operation and completion queues

The next memory-backend diagnostic separates two receive threads from the same
16 operation workers and two completion threads. Both user queues are bounded
at 64 jobs. The public SDK handlers, operation guards, ACL checks, cache flags,
readiness handshake and `STOP0 -> join -> STOP` ownership remain unchanged.
The owner wakes the bounded queues before SDK shutdown; no queue lock spans a
callback or transport call. A transport failure stops reception and joins every
active callback before releasing its buffers. This is a test-only transport,
with one filesystem per process, and adds no served-namespace handle.

The first split executable `d2b44c42...` passes eight lifecycle cases and four
large-corpus stops in the isolated guest. All 12 self-origin probes are refused;
forced exit takes at most 0.003 s and normal exit at most 0.131 s. No process is
stranded. Its five-sample Git sequence is slower than both stock controls:

| Dispatcher | First phase | Repeated phase |
|---|---:|---:|
| Stock | 3,153.0 ms | 3,148.4 ms |
| Split queues | 3,997.4 ms | 4,008.4 ms |

Native before/after medians are 296.7/292.3 ms. Every mounted phase verifies all
50,000 files before timing. Each measured Git run makes 51,096 Open and 51,136
security callbacks, no Write callbacks, and 50,000 logical index checks.

Whole-phase counters, including preparation and byte verification, record
1,475,502–1,475,514 received/processed/responded requests, 806,986–808,444
receive calls and 912,383–912,886 completion calls. Empty receives fall to
12,430–12,531, but the combined transaction count remains about 1.72 million.
Both queues reach their bounds. Fewer empty receives do not establish lower
CPU spin or better wall time. The first split transport is rejected.

The [retained comparison](../../../test-evidence/wave-6/memfs-pool-v1-sandbox-20261025.json)
contains sources, counters, traces and all stop records. Its 120-member native
export passes CRC, SHA-256 and timestamp-size checks; the final runner handle
is missing. These results isolate transport behavior on a memory backend,
without qualifying Telekinesis persistence or the full performance gate.

The same-executable follow-up `56d1978c...` isolates allocation: dynamic jobs
versus a fixed arena of 256 reusable request/response slots. Both queues remain
bounded at 64; all SDK operation, access, cache and shutdown policies match.
The [host fixtures](../../../test-evidence/wave-6/split-transport-host-20261025.json)
retain 41 normal plus 41 ASAN cases for the first split version, then 82 normal
plus 82 ASAN cases covering dynamic/reusable allocation. They include every
child-thread creation failure, callback lifetime during stop, transport failure
and full work/response queues with both senders blocked. All handles/jobs are
reclaimed; ASAN does not establish absence of data races.

Eight initial lifecycle cases and six large-corpus stops pass in the guest,
with 14 refused self-origin probes, forced exit at most 0.003 s and normal exit
at most 0.173 s. All 50,000 files are verified before each mounted phase:

| Phase | Dispatcher | Median Git |
|---|---|---:|
| A1 | Stock | 3,224.5 ms |
| B1 | Split, dynamic allocation | 4,050.5 ms |
| C1 | Split, reusable arena | 4,421.4 ms |
| C2 | Split, reusable arena | 4,019.3 ms |
| B2 | Split, dynamic allocation | 3,947.6 ms |
| A2 | Stock | 3,158.5 ms |

Native medians are 295.8/305.1 ms. Every measured mounted run still makes
51,096 Open and 51,136 security callbacks, no Write callbacks and 50,000 logical
Git index checks. The arena reaches its 256-slot bound during each full phase
and drains to zero. It provides no consistent benefit over dynamic allocation;
both split variants lose to both stock controls. Neither is promoted.

The [allocation comparison](../../../test-evidence/wave-6/memfs-pool-v2-sandbox-20261025.json)
retains all samples and counters. Its 136-member export passes CRC, SHA-256 and
timestamp-size checks, and the runner handle is missing. Production source
hashes matched the scoped-cache candidate at completion of these experiments;
the transports remain test-only. Original performance targets remain unchanged.
