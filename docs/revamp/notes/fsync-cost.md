# File fsync cost follow-up

Date: 2026-10-24. Part of the open
[mount lifecycle/performance blocker](../WAVE_6_MOUNT_LIFECYCLE_BLOCKER.md).

Per-file Flush used to scan and lock every scratch in the working tree. A weak
index by branch/entity now selects every draft of the affected entity, including
old local edits and drafts retargeted by atomic replacement. A retained Windows
reader still flushes the visible entity and its own private draft. Volume flush
and shutdown still visit all materialized dirty files. The index holds no pins.

Flush also checkpointed SQLite after sealing had already committed the content
event and receipt with `synchronous=FULL`. A full WAL commit is already durable
([SQLite documentation](https://sqlite.org/pragma.html#pragma_synchronous)).
The store now remembers that barrier using the connection's row-change count
and schema cookie. Later namespace, catalog or schema writes still require a
barrier; reopening starts conservatively. An incomplete checkpoint result now
reports `METADATA_SYNC_INCOMPLETE` rather than claiming durability. Cache,
cleanup flushing, access checks and the no-self-handle rule are unchanged.

Host validation passes: 65 library tests, 59 session tests, 19 handshake tests,
the two existing ignores, format, Clippy and the bounded lifecycle helper's
self-test. Regressions cover unrelated locked scratches, replacement through a
retained reader, and a WAL reader that permits a durable seal but blocks a later
namespace/catalog/schema barrier.

A native selection diagnostic (100 samples) measured 90.1 us for the old scan
and 0.6 us for the index with 10,000 scratches. This is not a mounted benchmark.
Reduced Sandbox runs (200 copy files, 64 MiB, 2,000 Git files, one measured
sample) still miss all targets; separate native baselines also vary. They do
not isolate purge cost or qualify performance. Their daemons exited gracefully
in 0.616/0.632 s. All 32 lifecycle cases passed for the index-only binary
`a8fd7291...`; the [report](../../../test-evidence/wave-6/mount-lifecycle-fsync-index-sandbox-20261023.json)
retains its exact hash and exit records.

The WAL follow-up binary `36cf9467...` also passes all 32 isolated lifecycle
cases ([report](../../../test-evidence/wave-6/mount-lifecycle-fsync-wal-sandbox-20261024.json)).
Its full five-sample gate failed: copy 11.65x native, sequential write/read
3.73x/3.15x, random overwrite 11.44x. Git add exceeded 600 s and the old runner
reported a stranded command; Git status, mounted builds and the peer phase
were not reached. The daemon exited gracefully in 1.658 s; neither it nor Git
remained at the later process check. Reports and recoverable stores were
exported before guest disposal ([evidence](../../../test-evidence/wave-6/fsync-wal-cost-sandbox-20261024.json)).
The blocker remains OPEN; this does not isolate purge cost.

The next candidate reuses compiled SQLite statements for namespace and pack
operations and skips host length/seek/pack-read work for a newly created empty
scratch. Those source changes are not included in `36cf9467...` and await their
own performance qualification. Release `3f3be093...` passed 31/32 VM cases but
exposed a [timestamp-based recovery data-loss hole](recovery-timestamps.md).
The fixed release `cf54ca85...` passes 144 Rust tests and all 34 VM cases, but
copy still costs 11.59x native. The
[next create-path optimization](create-pack-flush.md) awaits VM qualification.

The timeout runner now kills Windows command process trees within its existing
10-second post-timeout budget. A non-mounted launcher/child regression first
reproduced the inherited-pipe STRANDED error, then passed after this fix. A
timeout still fails qualification. This does not establish that inherited pipes
caused the Git timeout, or that another deleting-daemon hang occurred.
