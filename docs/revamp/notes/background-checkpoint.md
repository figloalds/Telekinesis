# Metadata checkpoint maintenance outside the Store lock

The [checkpoint diagnostic](sqlite-commit-cost.md) measured 18.155 s in 167
automatic checkpoints inside 22.844 s of foreground COMMIT time. The new
candidate uses a separate native metadata connection for regular PASSIVE
checkpoint maintenance. SQLite 3.53.2 is bundled; startup also enforces the
3.51.3 version floor for the concurrent WAL-reset fix and NOOP probes.

The writer's inline automatic hook is replaced only after the worker connection
has opened and its thread has started. The worker probes committed WAL state
after commit requests and with a 50 ms idle wait. At 1,000 or more frames it runs
a PASSIVE checkpoint. It retries periodically after busy/incomplete work,
including after a retained reader releases its snapshot without another
writer commit. The foreground five-millisecond metadata commit window remains
unchanged. This is a working replacement for inline automatic checkpoints,
not checkpoint removal.

An explicit `sync_metadata` FULL checkpoint takes the same gate as background
maintenance. SQLite does not wait for another active checkpointer in this
case; the shared gate prevents an avoidable BUSY result from the worker.
The explicit barrier still requires a non-busy result and all frames synced.
Fully synchronized seal receipts retain their existing WAL durability point;
pack and index durability ordering, caching, cleanup flushing and access checks
are unchanged.

Busy/locked background work is retried. Other SQLite errors are latched, counted
and returned by metadata commits, maintenance ticks and explicit durability
requests, including a request whose cached durability stamp already matches.
An unexpectedly stopped worker is also a failure. Shutdown reports that
failure and retains unsealed scratch for recovery. Worker handles refer only
to native backing files; the backing-overlap preflight still runs before a
mount can cover them. Store field order closes the writer before the worker;
the worker is woken and joined when its owner drops. Mounted harnesses retain
their external stop/kill deadlines and halt after a cleanup timeout.

The six new checkpoint regressions cover:

- Progress after retained-reader release without another commit, while keeping
  the 1,000-frame threshold.
- A foreground commit while the checkpoint gate is held.
- A real failed checkpoint on a read-only SQLite connection, latched and
  reported while committed bytes remain readable.
- Error reporting despite a previously satisfied durability stamp.
- Failed seal/shutdown with successful recovery of the unsealed successor bytes.
- Repeated FULL barriers while background maintenance is active, checking all
  WAL frames are synchronized on return.

The same candidate avoids decoding an entire flat manifest on every range
read. Pack verification and complete manifest framing/length validation remain;
only the requested digest range is decoded. Cross-chunk, partial EOF, empty
read and malformed-unrequested-tail regressions pass. The expected movement
is less operation 4 time, independently of the checkpoint's expected reduction
in foreground COMMIT/Create time.

Candidate `bc04000b73dfa144c1276380ea9b93e2fe0ab94c40361e0077ba43e92399f3f7`
passes 180 host tests, strict Clippy, formatting and release build. All 44
isolated lifecycle cases and six timestamp laps pass (forced exit maximum
0.015 s; ordinary graceful maximum 0.156 s). The diagnostic daemon exits
gracefully in 0.332 s, with zero background checkpoint errors.

The three-run copy diagnostic reduces NORMAL/FULL COMMIT cost from
16.406/6.363 s to 3.718/2.661 s. Background maintenance costs 49.375 s across
3,380 calls, overlapping the foreground; this is a new cost to investigate,
not eliminated work. Copy median is 9.662 s (7.26x its fresh native baseline),
sequential write/read medians 5.167/1.881 s (3.10x/1.60x native). The preceding
truncate candidate read median was 2.551 s; this is a targeted comparison,
not a controlled full-gate conclusion.

The [retained diagnostic](../../../test-evidence/wave-6/background-checkpoint-sandbox-20261024.json)
contains separate workload counters, reports, exit records and a CRC-verified
complete native archive (812 members), whose sizes match its exact-timestamp
manifest. The five-sample full-gate attempt completed native and mounted phases
with all 18 build observations correct in each. Both daemons exited gracefully
(0.946 s source, 1.399 s peer), without forced cleanup or stranded processes.
The idle-peer measurements were not reached: initial import of the source cut
at event 143,194 exceeded the 120-second bound. Offline checks of the preserved
databases find 92,672 peer events, zero waiting events and `quick_check=ok` on
both stores. A bounded resume diagnostic is investigating progress separately;
it cannot retroactively qualify the missing full-gate phase.

That [resume diagnostic](../../../test-evidence/wave-6/background-peer-resume-sandbox-20261024.json)
then advances from 92,672 to 143,194 events in 68.779 s within a separate
300-second bound. Its timeline records continued import progress. Five remote
visibility samples pass (102.425 ms median, 111.171 ms maximum); the two daemons
exit normally in 0.170/0.177 s. Both controller/store exports pass CRC and
timestamp-manifest size verification. This confirms slow bulk initialization
for that attempt, not a completed idle-peer performance gate.

The harness now accepts `--peer-catchup-timeout` for the bounded initial import
and records the selected limit with the elapsed time. Its default stays 120 s;
the next complete bulk run can explicitly use 300 s based on this measured
initialization cost. This changes no native ratio, remote-visibility target or
20% idle-peer overhead limit. Nonpositive/nonfinite bounds are rejected before
starting work; delayed delivery, a missing source cut and earlier real gate
reports are checked without a mount
([host evidence](../../../test-evidence/wave-6/peer-import-bound-host-20261024/result.json)).

The mounted gate still fails copy (6.89x), sequential write/read (3.04x/1.61x),
random overwrite (97.48x), Git status (12.02x) and no-op build (1.21x) native.
Clean/incremental build ratios are 0.96x/1.14x. The random-overwrite ratio must
be read alongside its absolute 10.631 s median and the new 0.109 s native
median; these are not interchangeable with an earlier baseline.

Git trace2 identifies about 8.9 s in the 50,000-lstat preload, not index
decoding (about 26 ms) or content refresh (zero scans after warm-up). Per-run
callback observations show 51,096 Opens per measured repetition. One repetition
attributes 6.551 s to metadata lock waiting inside the inclusive 9.099 s
metadata stage, and 2.200 s to scratch lookup. Concurrent callback sums exceed
wall time and cannot be treated as an isolated purge measurement.

The [full-gate record](../../../test-evidence/wave-6/background-full-sandbox-20261024.json)
retains per-repetition observations, trace2, clean exits, both workload reports
and the CRC/size-verified complete export. No host compilation overlapped the
timed phases. Performance, the idle-peer gate and purge-cost acceptance remain
open. Coalescing background checkpoints and the measured metadata-read
contention are separate follow-ups; the driver flag must not simply be turned
off to hide these failures.
