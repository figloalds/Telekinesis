# Bound maximum-chunk replies to the default session budget

The direct-root prefetch pass already groups 32 roots, but the general
`DigestFetch::run` path still requested 64 chunks. With 64 distinct 64 KiB
chunks containing mostly byte value 255, their JSON response is 16,782,839
bytes. This exceeds the unchanged default connection limit of 16,777,216 bytes.
A real authenticated `SessionSource`/`Service` test fails before the change with
`WS_MESSAGE_TOO_LARGE`. The previous random-payload gate did not expose this case.

General foreground/background fetch now groups 32 requested objects too. That
bounds a maximum-chunk group to 2 MiB raw, approximately 8 MiB plus metadata as
JSON. The server's 64-object request cap, 8 MiB raw reply cap, protocol limits
and default 16 MiB connection cap remain unchanged. Direct-root grouping,
foreground priority, retries, object validation and the final requested-object
or foreground pack barriers retain their existing paths. Driver/cache/access
settings and metadata/receipt durability are unchanged.

The shared authenticated transport fixture covers direct roots (`[32,1]`),
64 maximum-size foreground chunks (`[32,32]`) and a 4 MiB manifest-prefetch
pass (`[1,32,32]`). Every chunk byte and persisted index entry verifies; the
manifest reconstructs all expected bytes and both connections stay open.
This tests the chunk response bound; individual manifest/tree objects continue
to be subject to their existing object/request/connection limits.

Candidate `49af917f...` passes all 209 host tests, including 13 focused fetch
regressions, plus strict Clippy, formatting, release and five inert binding
fixtures. No real WinFsp DLL, driver or mount runs on the host. The
[host record](../../../test-evidence/wave-6/fetch-wire-bound-host-20261026.json)
retains exact sources, the failing-before transcript and checks.

The exact candidate passes the standard 44 isolated lifecycle cases and six
timestamp laps. Maximum forced exit is 0.004 s and ordinary matrix graceful
exit 0.099 s. Four additional paired reader-first cached delete/recreate/
write/fsync cases with a 4 MiB near-255 payload also pass, across letter and
directory mounts and graceful/forced target stops. Their target forced/graceful
maxima are 0.004/0.045 s; source cleanup also passes without stranding.

The unchanged complete five-sample gate validates all 54 build observations
and independently verifies every byte of the final 1 GiB directly from stopped
native backing packs after six overwrite laps. There are no workload I/O errors.
Both full-gate servers exit normally in 0.481/0.071 s without kills or stranded
processes. The controller's final process handle is missing.

Performance still fails: mounted/native ratios are copy 6.32x, sequential
write/read 4.63x/1.42x, random overwrite 32.49x and Git 11.25x. Clean/no-op builds
pass at 1.03x/1.05x. Idle-peer overhead fails only copy, at +40.6%; other gated
peer metrics stay within 20%. Initial metadata catch-up takes 77.831 s and
remote visibility is 36.991 ms median. Initial metadata catch-up does not prove
complete peer payload hydration. One new lap does not establish a causal
performance change against the preceding candidate's different native baseline.

The [complete qualification record](../../../test-evidence/wave-6/fetch-wire-bound-sandbox-20261026.json)
retains all reports, 321 additional lifecycle evidence files, exact identities
and an 8,403,411,073-byte, 37,699-member native export verified by CRC, SHA-256
and timestamp-size checks. This fixes the reproduced valid-data access failure;
P0 remains open under the original performance/peer targets.
