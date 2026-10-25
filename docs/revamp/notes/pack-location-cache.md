# Bounded immutable-pack location cache

The peer prefetch loop repeatedly walks current content, including objects that
are already local. Each direct reference performs two `DigestFetch::prefetch`
calls; `PackStore::contains` previously queried SQLite for each committed object.
This is separate from WinFsp transport and the namespace entry cache.

`PackStore` now caches positive digest-to-location lookups under its existing
mutex. Pending writes stay authoritative, and missing objects are never cached.
The FIFO holds at most 131,072 fixed-size records. On Windows x64 its digest,
location and FIFO-key payload is at most 12 MiB, plus bounded container overhead.
The verified object-byte cache and all durability barriers remain unchanged.

Locations are immutable between collection operations. GC clears the location
cache before changing the index, including when a later operation fails. A
detected corrupt read also clears it before removing the bad index row. Actual
reads retain length checks and SHA-256 verification; a cached location is not a
verified byte buffer. The Engine's existing exclusive backing-store ownership
remains required; this adds no cross-process cache coherency protocol.

A native diagnostic creates 50,000 distinct eight-byte objects, commits them,
verifies every byte, and executes the existing prefetch scan without a mount or
network traffic. Each phase has one warmup and five measured scans. A counted
dummy source confirms zero peer fetches. The host process verifies that the
real WinFsp DLL is absent before and after execution.

| Code | Median complete scan |
|---|---:|
| Reference, before | 474.4 ms |
| Location cache | 17.4 ms |
| Same reference binary, return control | 484.9 ms |

This isolates repeated presence lookup work. It excludes Store projection
cloning, metadata exchange, real payload sizes and actual filesystem clients;
the 96.3% reduction is not a mounted or peer-overhead acceptance result.
Thread CPU measurements are retained with the Windows clock's coarse resolution.

Candidate `60a747c1...` passes 196 host tests, strict Clippy, formatting, release
and five inert DLL binding fixtures. Regressions cover capacity eviction, new
objects after misses, concurrent deduplication and kind checks, GC relocation,
corruption repair and reopen. A real Windows sharing restriction also forces
old-segment deletion to fail after the GC index commit: subsequent lookups must
follow the committed new locations and reject deleted objects.

The [host record](../../../test-evidence/wave-6/pack-location-cache-host-20261025.json)
retains exact sources, build identities, all samples and checks. Only
`src/content/local.rs` differs from the scoped-cache production candidate.
Original acceptance targets, cache flushing, access checks and receipt/durability
semantics are unchanged.

## Complete isolated qualification

The exact candidate passes all 44 lifecycle cases and six additional timestamp
laps in Windows Sandbox. Maximum forced exit is 0.004 s; maximum graceful exit
in the matrix is 0.094 s. All cases pass their own mount-removal, native I/O,
failure/recovery and responsiveness checks. No process is stranded.

The full five-sample gate completes all workloads with 54 validated clean,
no-op and incremental build observations. Both servers exit normally in
0.569/0.433 s without a kill. Initial peer metadata catch-up takes 70.610 s
within its 300-second bound; remote visibility median is 36.091 ms.

| Workload | Native | Mounted | Connected peer |
|---|---:|---:|---:|
| Copy small files | 570.4 ms | 3,781.7 ms | 5,148.6 ms |
| Sequential write | 623.2 ms | 1,927.8 ms | 2,757.6 ms |
| File fsync | 732.5 ms | 645.8 ms | 845.5 ms |
| Sequential read | 512.5 ms | 732.7 ms | 957.8 ms |
| Random overwrite | 44.6 ms | 1,480.9 ms | 1,328.0 ms |
| Git status, 50,000 files | 302.8 ms | 3,341.5 ms | 3,562.8 ms |
| Clean build | 5,609.6 ms | 4,754.8 ms | 4,972.2 ms |
| No-op build | 1,090.3 ms | 1,185.9 ms | 1,264.9 ms |
| Incremental build | 2,008.0 ms | 2,124.4 ms | 2,176.2 ms |

Copy, sequential write/read, random overwrite and Git still fail at
6.63x, 3.09x/1.43x, 33.22x and 11.04x native. Clean/no-op builds pass their
original targets. Peer overhead also fails for copy (+36.1%), sequential write
(+43.0%) and read (+30.7%); random overwrite, Git and builds pass that overhead
limit in this run. The native scan improvement does not establish acceptance
or explain the remaining peer cost. This is one native/mounted/peer ordering,
without an interleaved comparison against the preceding filesystem binary.

After both servers stop, independent backing verification confirms every byte
of the final 1 GiB after six 10,000-overwrite laps: 53,560 changed pages and
16,385 verified objects. SHA-256 is
`16e78db98a9732f51955188d6b69de7749375dbfc9d79d7b934bc09f1afee0ba`.
All 18 Git invocations, including warmups, report 50,000 logical index checks.

The [complete record](../../../test-evidence/wave-6/pack-location-cache-sandbox-20261025.json)
retains traces, callback observations, all lifecycle logs and stop records.
Its 6,956,912,035-byte, 39,701-member export passes CRC, SHA-256 and timestamp
manifest size checks. The final runner handle is missing. No host compilation
or heavy host I/O overlaps timed phases. P0 remains open on the original
performance and peer-overhead gates; this is not a qualified purge-off/Close
policy or authorization for other Wave 6 feature work.
