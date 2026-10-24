# Read-only cleanup/cache cost isolation

The `0fb0beae...` Git probe remains near 9.5 s even with 102,180 metadata cache
hits and 34 misses per repetition. The next experiment varies cleanup purge
on explicit read-only volumes, preserving the same sealed 50,000-file corpus,
WinFsp Beta5, metadata cache, Git arguments and directory timeout. Every corpus
byte is checked against the fixture generator during the data-cache primer.
Client creation/write rejection is verified before accepting any measurements.

Three warmed samples per cell, milliseconds:

| Diagnostic mode | Before data primer | After verified data primer |
|---|---:|---:|
| Cleanup purge enabled | 9,826.7 | 10,363.5 |
| Standby cache retained | 9,184.2 | 5,169.6 |

The primer generates 50,000 data opens and reads in both modes. Its following
snapshot records about 49,086 Close callbacks with purge, versus three with
standby retention. After priming, both modes still make about 51,090 Opens per
Git repetition and have over 99.9% user-mode namespace cache hits. Retaining
the data cache cuts this read-only wall time roughly in half; it does not
eliminate the open/cleanup roundtrips or meet the native target.

Both diagnostic volumes reject client writes. Their daemons stop normally in
0.794/1.979 s, with no kill or stranded process. The
[retained experiment](../../../test-evidence/wave-6/readonly-cache-probe-sandbox-20261024.json)
includes exact binary/source hashes, complete callback snapshots, byte checks,
write rejection, logs, traces and verified exports. No host compilation overlaps
timing. Git uses `--no-optional-locks` here to keep the corpus immutable; this is
not the acceptance gate and cannot qualify writer/fsync/recovery/Close policy.

The test-only `TKFS_NATIVE_CACHE_PROBE` build setting always advertises a
read-only volume and explicitly rejects native mutation. A prior advertise-only
attempt accepted a write; validation stopped it before measurement and its
[failure evidence](../../../test-evidence/wave-6/readonly-cache-advertise-only-failed-20261024.json)
remains preserved. The default build keeps the existing purge and access policy.

The next read-only variants also set `PostCleanupWhenModifiedOnly`. The WinFsp
maintainer [describes this option](https://github.com/orgs/winfsp/discussions/607)
as avoiding the user-mode Cleanup roundtrip for unmodified files, while Close
remains asynchronous and uncached Create still requires a roundtrip. These
variants completed with these three-sample medians:

| Modified-only Cleanup diagnostic | Before data primer | After verified data primer |
|---|---:|---:|
| Cleanup purge enabled | 7,685.9 | 7,995.1 |
| Standby cache retained | 7,675.9 | 5,258.1 |

All 50,000 primer files again match their expected bytes, both write probes
are rejected, and no unmodified Cleanup callbacks arrive. The purge/standby
daemons stop normally in 0.955/2.120 s. Both archives pass CRC, SHA-256 and
timestamp-manifest size verification. The
[retained modified-only experiment](../../../test-evidence/wave-6/readonly-cache-post-probe-sandbox-20261024.json)
preserves all samples. Omitting Cleanup improves the purge case but still
does not meet the native target; standby is similar to its prior result.

The next writable candidate keeps purge enabled and omits only unmodified
Cleanup callbacks. WinFsp's [Beta5 Cleanup implementation](https://github.com/winfsp/winfsp/blob/v2.2B5/src/sys/cleanup.c)
calls its cached-byte flush before deciding whether to post Cleanup, and still
posts deletion, allocation, archive/write/change-time work. `Engine::close`
already calls its idempotent cleanup fallback before releasing a handle.
Writable candidate `6df02c15...` passes all 44 isolated lifecycle cases and
six additional timestamp laps (forced exit max 0.012 s; ordinary graceful
max 0.158 s), including cached/paging writes, fsync and receipt recovery.
All 185 host tests, strict Clippy, formatting/release and five inert binding
fixtures pass. Its three-sample native/mounted comparison measures copy
8.444 s (6.45x native) and Git 7.790 s (9.34x native). The diagnostic daemon
stops normally in 2.194 s. Its complete export passes CRC, SHA-256 and
timestamp-manifest size verification. See the
[writable qualification](../../../test-evidence/wave-6/modified-cleanup-sandbox-20261024.json)
and [host checks](../../../test-evidence/wave-6/modified-cleanup-host-20261024.json).
The full performance/build/idle-peer gate remains open; no writable standby
or Close-based cache-retention policy has been qualified.

The existing notification calls are not a substitute durability barrier for
that fallback. In Beta5, [volume notification](https://github.com/winfsp/winfsp/blob/v2.2B5/src/sys/volume.c)
queues kernel work and returns before the cache invalidation runs; the
Begin/End session controls rename exclusion rather than acknowledging data
delivery. [Flush dispatch](https://github.com/winfsp/winfsp/blob/v2.2B5/src/sys/flush.c)
handles served-volume objects and rejects a flush on the protocol/control
device. Stopping the volume also stops its request queue. Consequently, neither
NotifyEnd returning nor dispatcher shutdown proves that all dirty cached bytes
reached Rust. A writable standby fallback needs that proof without opening
the served namespace. The present writable build keeps the qualified purge
path while other measured costs are addressed.

## Cached attribute query isolation

The pinned Beta5
[FastIoQueryOpen implementation](https://github.com/winfsp/winfsp/blob/14d6e0280ae15e2550c44d6dbc476be51649727f/src/sys/fileinfo.c)
requires `AllowOpenInKernelMode`, an absolute lookup and eligible cached file
information. Its
[by-name lookup](https://github.com/winfsp/winfsp/blob/14d6e0280ae15e2550c44d6dbc476be51649727f/src/sys/file.c)
still requires user-mode requests to ask only for `FILE_READ_ATTRIBUTES`, have
traverse privilege and pass `SeAccessCheck` against a cached descriptor.
The normal application leaves this option off.

A mutation-rejecting read-only diagnostic compares only that option, keeping
purge, modified-only Cleanup, the ordinary descriptor/access policy and 16
workers. The enabled candidate passes ten read-only-compatible lifecycle cases
across letter/directory mounts: self-open refusal before Core, three partial
attach failures and idle forced termination. Both variants reject write probes.
Host tests (187 passed, five ignored), strict Clippy, formatting, release builds
and five inert binding fixtures pass. This is not writable qualification.

Three warmed Git samples per cell use the same unchanged 50,000-file corpus.
A separate test client optionally keeps 50,000 metadata-only handles open;
the server owns none. This is a cache-lifetime control, not a production helper.

| Option | Test-client handles | Git median | Open callbacks per sample |
|---|---:|---:|---:|
| Off | None | 7,602.8 ms | 51,090 |
| On | None | 7,385.6 ms | 51,081 |
| Off | 50,000 | 4,131.8 ms | 51,090 |
| On | 50,000 | 4,379.6 ms | 51,081 |

Keeping nodes present changes their allocation/cleanup work, but enabling the
option removes only nine callbacks and gives no large-scan fast-path benefit.
Both clients close all 50,000 handles without error in 4.23/4.28 s, and both
servers stop normally in 0.544/0.492 s. The head is unchanged. The 20.9 MB export
passes CRC, SHA-256 and timestamp size checks; see the
[large-scan evidence](../../../test-evidence/wave-6/readonly-kernel-query-sandbox-20261024.json)
and [host record](../../../test-evidence/wave-6/readonly-kernel-query-host-20261024.json).

The driver builds its security cache with a fixed capacity of **100 items**
([capacity](https://github.com/winfsp/winfsp/blob/14d6e0280ae15e2550c44d6dbc476be51649727f/src/sys/driver.h),
[creation](https://github.com/winfsp/winfsp/blob/14d6e0280ae15e2550c44d6dbc476be51649727f/src/sys/device.c)).
Entries use individual indices and are capacity-evicted rather than descriptor
content-deduplicated
([cache](https://github.com/winfsp/winfsp/blob/14d6e0280ae15e2550c44d6dbc476be51649727f/src/sys/meta.c)).
This is a source-confirmed eligibility constraint, not a measured accounting of
every large-scan miss. The source copies and Git blob hashes are retained in the
[pinned audit](../../../test-evidence/wave-6/winfsp-beta5-cache-source-audit-20261024/sources.json).

A second control keeps only 32 metadata handles open. Each lap makes 32,000
`GetFileAttributesExW` queries; expected fixture sizes are checked outside
timing. With the option off, three-sample median is **9,292.1 ms**; enabled it
is **100.4 ms**. Across the warmup plus measured queries, Open/security callback
counts fall from 128,033/128,040 to **zero**, with no Write callbacks. Both servers
stop normally in 0.417/0.396 s, all handles close and the corpus head is unchanged.
The 17.4 MB export verifies; see the
[small positive control](../../../test-evidence/wave-6/kernel-query-small-sandbox-20261024.json).
The path works on a fitting live-node working set, but this does not establish
50,000-file Git acceptance, a safe node-retention policy or a production helper.

The option is not promoted. Both experimental source edits were removed and
the preceding `build.rs` and native bridge restored byte-for-byte against the
qualified UPSERT source manifest. Cache, durability and access policy stay
unchanged; the original failed Git target remains open.

Narrowing the notification filter is also insufficient to avoid data purge:
`FspFileNodeInvalidateCachesAndNotifyChangeByName` flushes/purges an existing
data section before applying the filter to the notification. All these source
observations concern the exact pinned Beta5 code, not an unqualified new driver.
