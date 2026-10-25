# Keep idle prefetch out of foreground pack commits

`DigestFetch::run` previously ended every successful background presence check
with `PackStore::flush`, including when all requested objects were already
committed. A peer scanning existing content could consequently force an
unrelated foreground pack batch through file sync and a FULL index commit.

A native regression confirms this against the preceding code: commit a wanted
object, append an unrelated object, and prefetch the wanted object. An independent
read-only SQLite connection unexpectedly sees the unrelated object committed.
The regression fails before the change and passes afterwards.

Background prefetch now calls `PackStore::flush_if_pending` with its requested
digests. Under the existing pack mutex, the helper runs the unchanged complete
barrier only if a requested object is still pending. Foreground fetch retains
its unconditional barrier. File fsync, seals, receipts, metadata transactions,
cache draining and access policy are unchanged.

This cannot be based only on whether the current call downloaded bytes: a
previous failed partial download may already have installed a requested object
in the pending map. Such a retry still commits it. Newly downloaded requested
objects also remain persistent, and a failed requested barrier still returns
an error and retains pending data for retry.

Six focused regressions cover those cases, empty foreground requests, byte
verification and reopen. Candidate `db2a81cd...` passes 202 host tests,
strict Clippy, formatting, release and five inert binding fixtures.

A native model writes 2,000 distinct 2 KiB objects, explicitly flushing every
32 objects and once at the end. The prefetch phases interleave one presence-only
request for an already committed object after each put. Each phase has a warmup
and five measured repetitions; every object is verified after reopening.

| Code | First no-prefetch control | Prefetch | Repeated prefetch | Return control |
|---|---:|---:|---:|---:|
| Previous pack-location cache | 145.0 ms | 3,854.4 ms | 3,887.7 ms | 165.0 ms |
| Requested background barrier | 141.7 ms | 146.8 ms | 147.7 ms | 163.8 ms |

This model deliberately controls the interleaving. It does not measure actual
peer wake frequency or establish mounted performance improvement. Both binaries
use the same helper, and its process verifies that the real WinFsp DLL remains
unloaded. No mount or network transfer runs on the host.

The [host record](../../../test-evidence/wave-6/prefetch-barrier-host-20261025.json)
retains sources, the failing regression, samples, binary identities and checks.
No acceptance target is relaxed.

## Complete isolated qualification

The exact candidate passes all 44 lifecycle cases and six timestamp laps in
Windows Sandbox. Maximum forced exit is 0.004 s and maximum matrix graceful
exit is 0.101 s. The full five-sample gate validates 54 build observations;
all workloads complete without I/O errors. Both servers exit normally in
1.852/0.084 s, without a kill or stranded process.

| Workload | Native | Mounted | Connected peer |
|---|---:|---:|---:|
| Copy small files | 558.5 ms | 3,796.5 ms | 5,642.2 ms |
| Sequential write | 634.8 ms | 2,075.1 ms | 2,181.4 ms |
| File fsync | 484.7 ms | 633.9 ms | 758.4 ms |
| Sequential read | 476.5 ms | 761.5 ms | 815.0 ms |
| Random overwrite | 42.5 ms | 1,347.8 ms | 1,283.2 ms |
| Git status, 50,000 files | 305.3 ms | 3,270.7 ms | 3,423.4 ms |
| Clean build | 4,616.2 ms | 4,758.8 ms | 4,944.9 ms |
| No-op build | 1,069.3 ms | 1,209.4 ms | 1,184.3 ms |
| Incremental build | 1,980.5 ms | 2,155.4 ms | 2,161.1 ms |

Within this run, peer overhead passes for sequential write/read (+5.1%/+7.0%),
random overwrite (-4.8%), Git (+4.7%) and clean/no-op builds (+3.9%/-2.1%).
Copy still fails that limit at +48.6%. Initial metadata catch-up is 75.018 s
within its 300-second bound; remote visibility is 34.108 ms median. Metadata
catch-up does not establish complete peer payload hydration.

The original mounted/native performance targets still fail for copy,
sequential write/read, random overwrite and Git at 6.80x, 3.27x/1.60x, 31.71x
and 10.71x. Clean/no-op builds pass. This is one complete native/mounted/peer
ordering, without interleaved filesystem-binary controls. It does not prove
that the changed barriers explain every difference from the preceding run.

Independent verification after shutdown checks every byte of the final 1 GiB
after six 10,000-overwrite laps, including 53,560 changed pages and 16,385
objects. SHA-256 is
`16e78db98a9732f51955188d6b69de7749375dbfc9d79d7b934bc09f1afee0ba`.
All 18 Git runs, including warmups, report 50,000 logical index checks.

The [complete record](../../../test-evidence/wave-6/prefetch-barrier-sandbox-20261025.json)
retains traces, callback observations, lifecycle logs and stop records. Its
4,407,568,390-byte, 30,061-member export passes CRC, SHA-256 and timestamp
manifest size checks, and the final runner handle is missing. No host build
or heavy host I/O overlaps timed phases. P0 remains open on the original
performance gate and copy overhead; cache flushing, fsync, access checks and
the no-self-namespace-handle rule remain in place.
