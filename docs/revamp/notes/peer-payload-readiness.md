# Initial payload import is not an idle peer

The metadata-only wait in `scripts/bench.py` proves that the receiver has the
source's last committed event. It does not establish that the receiver has
downloaded the current file objects. The per-peer worker performs that download
after exchanging metadata. The original full-gate failures remain retained;
this note does not change an acceptance ratio or production policy.

## Isolated diagnostic, 2026-10-26

The same qualified `49af917f...` executable ran in the existing isolated guest
with the exact allowed WinFsp dependency. Its stopped, previously exported
source was reused; a fresh receiver imported its larger history. No host
compilation or heavy host I/O overlapped timing. Each copy series used the
unchanged 2,000-file workload, one warmup and five measured samples.

The source snapshot had 51,269 current live file references, eight manifests,
and 66,663 distinct required objects. Source manifest SHA/framing was checked
from native packs. At metadata arrival, **238.746 s** after pairing began,
the receiver's committed index contained **one** required object: **66,662
were missing**. All required objects were committed after **277.654 s**, within
the same cumulative 300-second bound. This inventory establishes membership
and kind, not independent byte verification of all receiver packs. This reused
history is not the original gate's fresh-state catch-up time.

| Phase | Copy median, ms | Delete median, ms |
|---|---:|---:|
| Native | 632.150 | 160.200 |
| Source alone, before | 4,056.369 | 1,501.542 |
| Connected peer after complete import | 4,769.274 | 1,810.118 |
| Connected peer, repeated | 4,521.032 | 1,723.722 |
| Source alone, return control | 3,943.553 | 1,520.019 |

Copy overhead relative to the first control is +17.6%/+11.5%; relative to the
return control it is +20.9%/+14.6%. The first connected series is therefore
borderline against 20%, depending on control. This is one ordered diagnostic,
not a complete performance qualification or proof that every earlier copy
failure came from initial hydration. There is no extra quiet interval after
readiness. The peer/source stop normally in 0.244/1.654 s, with no kills or
stranded processes; the controller's actual final process handle is missing.

The [retained diagnostic](../../../test-evidence/wave-6/payload-readiness-v2-sandbox-20261026.json)
includes the initial/terminal inventories, every sample and callback bracket,
exit records, and separate native source/diagnostic exports of 2,609,801,564
and 2,865,527,302 bytes. Both exports passed CRC, SHA-256 and timestamp-size
verification. The [first attempt](../../../test-evidence/wave-6/payload-readiness-sandbox-20261026.json)
stopped on an old fixture pairing before measuring the mount; its server
exited normally in 0.126 s. The corrected diagnostic records removal of that
fixture pairing before the source-only control. All original stores and the
final reused source state remain in verified exports.

## Harness correction

`scripts/peer_readiness.py` captures the source head and current active,
nondeleted file references in one read-only metadata transaction. It validates
source manifests, collects required roots/chunks and then checks the receiver's
committed native object index. The matrix now waits for both metadata and this
inventory before measuring an idle peer. The existing `wait_peer_metadata`
remains explicitly metadata-only for diagnostics.

One cumulative timeout includes source inventory, metadata arrival and payload
readiness; it is not restarted between phases. SQLite progress handlers bound
long scans. All native file/database handles close before each observation and
before the workload starts. Progress and final evidence retain metadata timing,
the initial missing-object inventory and complete readiness timing. Invalid
manifests, wrong kinds, count bounds and timeout fail the run. No daemon
self-I/O, retained served handle, cache-policy change, per-repetition delay or
target relaxation is introduced.

Ten native regression checks pass on both host Python and guest Python. They
cover metadata without payload, uncommitted objects, wrong kinds, corrupt or
malformed manifests, deleted/other-branch exclusion, a single shared deadline,
SQL interruption, an empty source and closed read handles.

The [host/guest check record](../../../test-evidence/wave-6/peer-readiness-host-20261026.json)
pins the harness sources. Of the executable's 60-file qualification manifest,
59 files remain identical; `scripts/bench.py` is the intentional changed entry.
The server executable and all compiled Rust/native sources are unchanged.

## Complete matrix with the corrected initial boundary

The [complete new matrix](../../../test-evidence/wave-6/peer-readiness-gate-sandbox-20261026.json)
uses the original workload dimensions, one warmup plus five samples and the
same qualified executable. All 54 build observations validate; every byte of
the final 1 GiB matches the independent stopped-backing reconstruction after
six overwrite laps. There are no workload I/O errors. Both servers exit
normally in 0.458/0.119 s, without kills or stranding. The prior 44 lifecycle,
six timestamp and four large paired checks still refer to this exact binary;
these unchanged checks were not repeated for the Python harness correction.

In this fresh-state matrix, metadata arrival takes **68.606 s**, with **66,534
of 66,663 objects still missing**. Complete inventory takes **105.736 s** within
the unchanged cumulative 300-second bound. The idle phase starts immediately
after readiness and the existing flush barrier, without an added quiet period.

| Metric | Native, ms | Mounted, ms | Peer, ms | Mounted/native | Peer overhead |
|---|---:|---:|---:|---:|---:|
| Copy | 582.884 | 3,907.128 | 4,735.750 | 6.70x | +21.2% |
| Sequential write | 430.238 | 2,199.365 | 2,180.532 | 5.11x | -0.9% |
| Sequential read | 562.833 | 794.724 | 780.050 | 1.41x | -1.8% |
| Random overwrite | 43.344 | 1,453.085 | 1,315.950 | 33.52x | -9.4% |
| Git status | 315.180 | 3,284.608 | 3,388.756 | 10.42x | +3.2% |
| Clean build | 4,653.457 | 4,841.119 | 4,827.710 | 1.04x | -0.3% |
| No-op build | 1,108.575 | 1,152.063 | 1,203.173 | 1.04x | +4.4% |

The gate still fails all five native data/metadata targets and peer copy
overhead (+21.208% against 20%). Clean/no-op builds and the other six gated
peer metrics pass. Remote visibility is 23.420 ms median. This demonstrates
that initial hydration did not explain all copy overhead. It is not a causal
comparison of 21.2% with the prior lap's 40.6%, whose baseline differs.

The native export is 8,090,796,266 bytes with 38,006 members, verified by CRC,
SHA-256 and timestamp-size checks. The controller's final actual process handle
is missing. P0 remains open under the original performance and peer targets;
the legacy metadata-only result is retained alongside this corrected matrix.
