# Bounded prefetch of direct content roots

The peer loop previously prefetched each content root individually. Direct
files then repeated the same digest as their single chunk. Although the fetch
protocol already accepts batches, an initial download of 2,000 small files
therefore needed 2,000 source calls and requested-object pack barriers.

The prefetch pass now groups at most 32 direct roots. References are validated
and duplicate digests within a group are removed. A valid direct object is at
most 64 KiB, so a group has at most 2 MiB of payload. Its worst-case JSON byte
arrays occupy about 8 MiB plus metadata, below the unchanged default 16 MiB
reassembled response limit. Server request limits and frame/queue budgets do
not change. Manifest roots and chunk traversal retain their existing path.

A partial reply does not prevent independent progress: received requested
roots are committed even if another root is missing. This uses the existing
requested-object barrier without retrying each absent root. Foreground demand
and the stop flag are checked between bounded groups. No namespace handle,
helper process, new worker or cache-drain policy is introduced.

Before changing the caller, a same-library native model compares the existing
single-root and grouped APIs. It fetches 2,000 distinct 2 KiB objects from a
local pack source through the real JSON codec, with a warmup and five measured
repetitions in each phase. All objects are verified after reopening.

| Phase | Roots per request | Source calls | Median |
|---|---:|---:|---:|
| A1 | 1 | 2,000 | 3,914.4 ms |
| B1 | 32 | 63 | 229.6 ms |
| B2 | 32 | 63 | 225.3 ms |
| A2 | 1 | 2,000 | 3,943.7 ms |

This model has no network or mount and does not establish actual peer overhead.
A separate authenticated TCP test uses the production prefetch helper,
`SessionSource`, `Service` and default connection options. It transfers 33
distinct maximum-size direct objects, mostly byte value 255, in requests of
32 and 1. The first serialized response exceeds 8 MiB and fits the unchanged
16 MiB bound. Every byte is verified and the connection remains open.

Candidate `272a1194...` passes 207 host tests, strict Clippy, formatting,
release and five inert DLL binding cases. Coverage includes deduplication,
invalid references, a partial first group followed by successful later groups,
retry after an absent root arrives, foreground priority, stop between groups,
manifests and empty content. The
[host record](../../../test-evidence/wave-6/prefetch-roots-host-20261025.json)
retains exact sources, models, protocol coverage and checks. No real WinFsp DLL,
driver or mount is used on the host.

Isolated lifecycle/timestamp and full performance/build/peer qualification are
pending for this exact candidate. The preceding complete gate remains failed;
P0 stays open and all original acceptance targets remain unchanged.
