# Writable data CPU diagnosis

Normal/default build `1d1ea8c6...` retains purge, access checks, automatic
timestamps, delayed sealing and explicit durability barriers. Its native
bridge is byte-identical to the preceding restored source; read-only diagnostic
settings are absent. All 44 lifecycle cases pass in the isolated guest
(forced exit max 0.016 s; ordinary graceful max 0.162 s).

The guest then records sampling-only WPR traces for copy/delete, sequential
write/fsync/read and random overwrite, with one warmup and one measured
repetition per workload. The matching private PDB is retained alongside exact
binary/source identities. The [capture and analysis record](../../../test-evidence/wave-6/data-cpu-profile-sandbox-20261024.json)
contains per-repetition callbacks, original reports and raw trace identities.

Offline xperf analysis crops traces to each complete benchmark command. These
intervals include setup, warmup, measured work and counter RPCs; they do not
isolate an individual timed sample. Sampled server CPU estimates are:

| Command | Server CPU weight | SHA-256 compression | Kernel spin-lock wait | `memcpy` symbols |
|---|---:|---:|---:|---:|
| Copy/delete | 30.701 CPU s | 0.34% | 5.07% | 4.10% |
| Sequential write/fsync/read | 17.795 CPU s | 10.35% | 0.11% | 22.28% |
| Random overwrite | 2.488 CPU s | 19.21% | 0.08% | 15.53% |

Percentages are exclusive sampled CPU weight, not elapsed wall-time fractions.
All traces report zero lost events and buffers. Optimized/public symbol names
do not give exact inlined-function or source-line attribution. In particular,
a sealing helper's instructions may be grouped under a neighboring public
symbol; the traces alone do not prove that an atomic replacement occurred.
These data workloads have a different cost distribution from the read-only
Git queue-contention trace. A global worker reduction remains rejected.

After profiling, the daemon stops normally in 0.486 s without kill or stranded
process. An independent read-only verifier reconstructs all final 1 GiB from
the stopped native backing packs, including both 10,000-overwrite laps and
19,239 changed pages. All 16,385 objects verify, and actual/expected SHA-256
match `c6a4670d53b65c670e8a8785025f25dd6d4b68ae5894ff6493fdb372a38bf71a`.
The 1.615 GB export passes CRC, SHA-256 and timestamp size checks. No host
compilation overlaps measured/profiling phases. This is not the five-sample,
build or idle-peer acceptance gate.

Source inspection identifies a concrete redundant cost: seal preparation
hashes each dirty chunk for its `new_chunks` statistic, calls `contains`, and
then `PackStore::put` hashes the same immutable byte slice and looks up its
location again. Candidate `33ba68d0...` has pack insertion return its computed
digest and whether it inserted a new object under the same deduplication lock.
Seal uses that result instead of its duplicate hash and lookup. Callers never
supply an unchecked digest. Size/kind/hash validation, pack flush ordering,
on-disk formats and recovery remain unchanged. Expected movement is one
hash/location lookup per prepared chunk, with the same logical
`hashed_chunks` and accurate `new_chunks` counts.

All 187 host tests, format, strict Clippy, release and the five inert binding
fixtures pass. A concurrent insertion regression verifies one new object
across eight callers, including persisted deduplication, kind collisions,
oversized chunks and verified readback. An optimized native-backing-only
diagnostic seals 512 MiB of repeated chunks in three laps per version:
median **599.4 ms before / 374.3 ms after**, a 37.6% reduction. Both retain
8,192 logical hashed chunks, one new chunk and validated byte probes. See the
[host evidence](../../../test-evidence/wave-6/seal-single-hash-host-20261024.json).
This microbenchmark does not predict the same whole-workload improvement.

One initial parallel-suite WSS control-latency assertion failed; its original
log is retained. The isolated test and an unchanged full-suite rerun pass,
without changing source or timing thresholds. The candidate is absent from
the preceding captured binary.

## Single-hash candidate qualification

Candidate `33ba68d0...` passes all 44 isolated lifecycle cases (forced exit max
0.011 s; ordinary graceful max 0.152 s) and six additional timestamp laps.
The paired native/reference/candidate diagnostic completes 36 correct build
observations. Both mounted daemons stop normally in 0.930/1.056 s, without
kill or stranded process. Independent offline verification confirms every
byte of each final 1 GiB after four 10,000-overwrite laps; both match
`25612c6dc623d6e1f3cafcdaaffdb974d3fc5be0ef0ac16ae1b80108182264ab`.
The 2.638 GB export passes CRC, SHA-256 and timestamp size checks. See the
[retained comparison](../../../test-evidence/wave-6/seal-single-hash-sandbox-20261024.json).

Three warmed measured samples, with the same 16-worker default and unchanged
cache/access/durability policy:

| Workload | Reference | Single hash | Change |
|---|---:|---:|---:|
| Copy small files | 10,913.5 ms | 10,884.9 ms | -0.3% |
| Delete small files | 3,891.0 ms | 3,380.5 ms | -13.1% |
| Sequential write | 4,827.8 ms | 4,974.1 ms | +3.0% |
| Sequential fsync | 1,600.6 ms | 1,247.6 ms | -22.1% |
| Sequential read | 1,871.6 ms | 1,885.8 ms | +0.8% |
| Random overwrite | 3,438.4 ms | 3,496.9 ms | +1.7% |
| Clean build | 10,093.3 ms | 10,969.7 ms | +8.7% |
| No-op build | 2,690.4 ms | 2,805.7 ms | +4.3% |
| Incremental build | 4,704.5 ms | 4,684.6 ms | -0.4% |
| Git status | 7,494.0 ms | 7,735.5 ms | +3.2% |

The change removes demonstrated duplicate work and improves this fsync
measurement, but does not make the filesystem generally faster in this
comparison. One native/reference/candidate order and three samples cannot
separate all scheduling/cache variation from code effects. The matched native
baseline still rejects copy, sequential write/read and random overwrite at
8.68x, 3.13x/1.89x and 33.15x. Clean/no-op builds pass at 0.91x/1.09x with
correct incremental output. Git here has paired measurements only, without a
contemporaneous same-command native baseline. This is not the five-sample or
idle-peer gate, and P0 remains open. No host compilation overlaps timed phases.

## Native metadata page-cache isolation

The copy command's inclusive stacks place 42.55% of sampled server CPU weight
inside SQLite, with overlapping maintenance work. This does not identify a
specific SQLite routine's cost: optimized static functions can resolve to a
neighboring public symbol. A native-only Store diagnostic therefore measures
pager counters on independent copies of the retained 125,276-event corpus.
It creates 2,000 empty files with real FULL receipt transactions, deletes them,
and includes the final metadata sync in each timed phase. Reopen checks verify
all receipts/deletions, the expected 131,277-event head and database integrity.
No driver loads or mounts occur.

Three samples per setting use the order 2/64/64/2/2/64 MiB. The
[retained evidence](../../../test-evidence/wave-6/metadata-page-cache-host-20261024.json)
records exact source/database hashes and all samples. Medians:

| Writer cache limit | Create + receipt | Delete | Create misses / WAL pages | Delete misses / WAL pages |
|---|---:|---:|---:|---:|
| 2,000 KiB | 8,027.0 ms | 1,641.8 ms | 12,747 / 55,278 | 12,066 / 12,113 |
| 65,536 KiB | 7,525.2 ms | 1,619.6 ms | 4,572 / 55,255 | 103 / 11,488 |

Neither setting spills dirty pages during transactions. SQLite's
[counter definitions](https://www.sqlite.org/c3ref/c_dbstatus_options.html)
distinguish cache misses, WAL page writes and mid-transaction spills. Its
[cache-size documentation](https://www.sqlite.org/pragma.html#pragma_cache_size)
describes a per-connection, session-only memory suggestion, allocated as needed.
The larger cache actually reaches about 29 MiB here. All counters above are
phase deltas, excluding Store loading. This changes only the test connection;
checkpoint, receipt synchronization and production settings remain unchanged.

Creation improves 6.3% and deletion 1.4% by medians, with considerable sample
variation (default creation 7.15–10.96 s; larger cache 7.50–8.28 s). Nearly
eliminating delete misses barely changes its median, and create WAL writes
remain almost identical. This does not establish page-cache pressure as the
dominant cost or justify a production cache increase. The change is not adopted.
The diagnostic excludes scratch, hashing, clients and the driver; it cannot
qualify mounted performance. An initial test-only type error stopped compilation
before any workload; its log is retained separately from the six passing samples.

## Update current rows without replacement

Source inspection identifies another avoidable write: advancing an entity uses
`INSERT OR REPLACE` on `current`, deleting/reinserting its unchanged primary key.
Candidate `23b4e6aa...` instead uses `ON CONFLICT(branch,entity) DO UPDATE` for
the state and visible-version columns. New entities still insert normally.
The schema has no triggers or foreign keys involving this table; the logical
key/state/visible values, savepoint rollback and publication order stay the same.
There is no schema migration or change to cache, synchronization or driver policy.
SQLite documents the respective
[replacement](https://www.sqlite.org/lang_conflict.html) and
[UPSERT](https://www.sqlite.org/lang_upsert.html) behavior.

The same copied native Store diagnostic, at the unchanged 2,000 KiB cache:

| Code | Samples | Create + receipt | Create WAL pages | Delete | Delete WAL pages |
|---|---:|---:|---:|---:|---:|
| REPLACE before | 3 | 8,027.0 ms | 55,278 | 1,641.8 ms | 12,113 |
| UPSERT | 3 | 6,391.2 ms | 47,199 | 1,160.0 ms | 9,171 |
| REPLACE return control | 1 | 7,691.7 ms | 55,426 | 1,669.9 ms | 11,970 |

All seven independent copies pass receipt/deletion/reopen/integrity verification.
The UPSERT medians improve 20.4%/29.3% against the initial controls, with
14.6%/24.3% fewer WAL page writes. The return control supports the direction,
but this is not a fully interleaved comparison and UUIDs differ between copies.
It isolates metadata work, excluding scratch, clients and driver cost.
The [host record](../../../test-evidence/wave-6/metadata-current-upsert-host-20261024.json)
retains all samples, the trial source hash and the return control. Final source
adds only a comment to the measured SQL change. All 187 host tests (five opt-in
diagnostics ignored), strict Clippy, formatting, release and five inert binding
fixtures pass.

The isolated candidate passes all 44 lifecycle cases (forced exit max 0.012 s;
ordinary graceful max 0.192 s), six additional timestamp laps and 36 correct
native/reference/candidate build observations. Both daemons exit normally in
0.952/1.166 s, without kill or stranded process. Independent stopped-backing
verification checks every byte of both final 1 GiB files after four 10,000-write
laps, including 37,030 changed pages and all 16,385 objects. Both SHA-256 values
match `25612c6dc623d6e1f3cafcdaaffdb974d3fc5be0ef0ac16ae1b80108182264ab`.
The 2.639 GB export passes CRC, SHA-256 and timestamp size checks. See the
[mounted record](../../../test-evidence/wave-6/metadata-current-upsert-sandbox-20261024.json).

Three warmed measured samples, one native/reference/candidate order:

| Workload | Reference | UPSERT | Change |
|---|---:|---:|---:|
| Copy small files | 11,045.7 ms | 9,019.3 ms | -18.3% |
| Delete small files | 3,606.9 ms | 2,913.6 ms | -19.2% |
| Sequential write | 5,004.5 ms | 4,710.2 ms | -5.9% |
| Sequential fsync | 1,030.3 ms | 1,111.0 ms | +7.8% |
| Sequential read | 1,849.3 ms | 2,166.2 ms | +17.1% |
| Random overwrite | 3,374.1 ms | 3,307.9 ms | -2.0% |
| Clean build | 10,648.4 ms | 10,450.9 ms | -1.9% |
| No-op build | 2,962.0 ms | 2,693.4 ms | -9.1% |
| Incremental build | 4,692.9 ms | 4,639.2 ms | -1.1% |
| Git status | 7,627.2 ms | 7,744.2 ms | +1.5% |

Copy/delete improve, while read and fsync are slower in this comparison. The
single ordering and three samples do not separate all cache/scheduling variation
from code effects; the read increase needs a controlled follow-up before a
general performance claim. The current matched native baseline still rejects
copy, sequential write/read and random overwrite at 7.04x, 4.05x/1.49x and
15.37x. Clean/no-op builds pass at 0.86x/1.05x with correct incremental output.
Git has paired results only, without a same-command native baseline. No host
compilation overlaps timed phases; lightweight process-handle observations are
retained separately. This is not the full five-sample or idle-peer gate. P0 stays
open, with unchanged targets and cache/durability/access policy.

## Read control after reopening identical content

An A–B–B–A control then mounts the same verified candidate backing state with
reference `33ba68d0...` and candidate `23b4e6aa...`, using normal writable-volume
settings and read-only clients. Each phase has two warmups and five measured
1 GiB reads in the canonical 1 MiB loop, followed by a complete SHA-256 check
outside timing. Metadata counts, event head, visible version and large-file
state remain unchanged. No Write callbacks occur. All four daemons stop
normally (0.532–0.703 s); the runner's final process handle is missing.

| Phase | Code | Median read |
|---|---|---:|
| A1 | Reference | 1,022.4 ms |
| B1 | UPSERT | 816.6 ms |
| B2 | UPSERT | 981.8 ms |
| A2 | Reference | 1,018.8 ms |

The native before/after controls measure 1,285.9/1,284.1 ms. All hashes match
the independently verified final-file digest above. The 20.8 MB export passes
CRC, SHA-256 and timestamp-size checks; see the
[retained control](../../../test-evidence/wave-6/upsert-read-control-sandbox-20261024.json).

Each mounted phase issues only 1,024 server Read callbacks across seven reads
plus the hash pass: later reads use the kernel data cache. The control does
not reproduce a persistent candidate slowdown after reopen, but it does not
recreate the preceding write/fsync cache state. It neither erases the original
17.1% slower result nor qualifies canonical sequential performance. Original
combined sequence counters also differ: reference Read callbacks are 1,024 in
each measured lap, versus 593/612/1,024 for the candidate. Callback times overlap
and must not be subtracted from wall time. The source already coalesces adjacent
scratch/base read ranges, so another chunk-read coalescing change is unwarranted.
