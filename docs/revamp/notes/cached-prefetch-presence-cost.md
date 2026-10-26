# Cached prefetch presence experiments, 2026-10-26

The corrected initial-import boundary still leaves peer copy overhead at
+21.208% in the [complete gate](peer-payload-readiness.md). This experiment
tests repeated all-local presence work without changing cache, durability,
access or acceptance policy. **Both candidates were rejected and the qualified
production sources restored.** No mount, network request or real WinFsp DLL
load occurred in these host-native probes.

The [retained record](../../../test-evidence/wave-6/cached-prefetch-rejected-host-20261026.json)
contains every sample, candidate sources/patches, probe code, build identities
and restoration checks. The fixture has 50,000 direct eight-byte objects plus
a valid 1 GiB manifest with 16,384 distinct full-size chunks. All source bytes
were verified at creation. Read-only scans share that unchanged fixture;
writer-contention phases use separate full copies. Scan timing excludes Store
projection cloning, direct-root caller deduplication, metadata exchange and
networking, so it is not a mounted copy result.

## Grouped presence: isolated gain, writer regression

The candidate checks each existing 32-object fetch group under one PackStore
lock and avoids preparing sources when no object is missing. It retains the
normal foreground/background pack barriers and passes a pending-arrival,
GC-relocation and reopen regression, all 210 host tests and strict Clippy.

The isolated A–B–B–A scan comparison initially looks helpful:

| Phase | Baseline | Candidate | Candidate | Baseline |
|---|---:|---:|---:|---:|
| Median ms per complete cached scan | 8.275 | 7.339 | 7.616 | 8.169 |

Each process discards one warmup and retains five samples of 100 scans. The
relevant question is also whether the larger lock region delays writes. A
second probe writes 5,000 new objects per sample, with a barrier every 32,
while the other thread scans the cached roots/manifest. Keeping the real
worker's 100 ms pause between complete passes produces:

| Phase | Baseline | Candidate | Candidate | Baseline |
|---|---:|---:|---:|---:|
| Median writer sample, ms | 501.076 | 545.928 | 531.803 | 515.103 |
| Median per-sample put p99, microseconds | 28.1 | 268.6 | 365.5 | 29.5 |
| Background passes across six samples | 1 | 23 | 21 | 1 |

The candidate gives the background thread more progress but costs the writer
about 3–9% relative to the bounding controls and substantially worsens its
tail. Without the 100 ms pause, an explicitly harsher stress shows an even
larger regression. These are native contention models, not actual mounted
copy measurements, but they contradict promoting the lock change on the
isolated scan result alone. The grouped method and its test were removed
from production sources; their exact tested versions remain retained.

## Source-only fast path: initial gain does not repeat

A separate candidate keeps all original per-object locks and only avoids
preparing the source vector when nothing is missing. Its first A–B–B–A scan
medians are 8.323 / 7.777 / 7.698 / 7.795 ms. The return control moves too,
so a B–A–A–B repetition checks that apparent advantage:

| Phase | Candidate | Baseline | Baseline | Candidate |
|---|---:|---:|---:|---:|
| Median wall ms per scan | 8.192 | 8.214 | 8.100 | 8.135 |
| Median thread CPU ms per scan | 8.125 | 8.281 | 7.969 | 8.125 |

The two label averages have equal CPU time and essentially equal wall time
in this reversal. Its cadenced writer comparison also stays near the controls
(499.897/486.744 ms versus 496.971/499.505 ms), without the grouped lock's tail
regression. This does not demonstrate a useful workload improvement. The
source-only candidate was rejected too, rather than promoted as a copy fix.

## Restored state and implication

All compiled Rust/native sources match the qualified `49af917f...` state.
The library rebuild matches the exact pre-experiment SHA-256
`7f97af7e26e124774eda3559c930389992acd7349a60ef9c12c3155afde431b1`;
the qualified executable is unchanged. Restored format, 209 tests, strict
Clippy and release-library checks pass. The intentional earlier
`scripts/bench.py` readiness correction remains, with its separate harness
manifest and completed isolated qualification.

No scheduler change or candidate mount follows these measurements. They do
not identify the full remaining peer overhead, exclude projection/feed costs,
or solve the five original native performance failures. The next diagnosis
needs to distinguish actual metadata/fetch activity and foreground contention
during copy; an isolated cached-loop improvement is insufficient. P0 remains
open with the original failed gate retained.
