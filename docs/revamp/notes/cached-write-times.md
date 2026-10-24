# Keep cached write replies and Cleanup on one automatic clock boundary

Date: 2026-10-24. Follow-up to W6-P0 qualification.

The expanded VM matrix adds explicit pre-Unix timestamps, ordinary cached
writes and timestamp-preserving atomic replacement for both mount types. It
exposed a 1 ms drift in the lazy-draft release's directory case; older binaries
also failed the new check. The earlier 34-case matrices did not include it.

Write replies already carry FileInfo back to WinFsp, while the Cleanup callback
has no FileInfo output ([WinFsp interface](https://github.com/winfsp/winfsp/blob/master/inc/winfsp/winfsp.h)).
The engine previously assigned a fresh automatic time at Cleanup, after Write
had published a different time. A reopen could then expose the discrepancy.
A deterministic engine regression delayed Cleanup after a paging-write reply
and observed that timestamp change before the correction.

Paging writes now establish an automatic timestamp while a writer is active;
Cleanup reuses the published boundary. Late cache delivery after Cleanup keeps
that boundary. Explicit SetFileTime suppresses automatic paging changes, and a
new writer starts a new boundary once the preceding writers have cleaned up.
An fsync before Cleanup also retains the published time after its seal. Cleanup
records the timestamp in metadata if no dirty scratch remains to carry it.

The first clock correction (`6f1140f2...`) passes 159 host tests but still fails
the directory timestamp check. A 20-file VM probe separates the two layers:
the engine already reports a later time while Windows keeps the older cached
FileInfo, including after an immediate read. Nested paging writes do not update
that FileInfo ([driver write completion](https://github.com/winfsp/winfsp/blob/master/src/sys/write.c));
the driver can also retain it across opens while a cached file object survives
([open metadata update](https://github.com/winfsp/winfsp/blob/master/src/sys/file.c)).
Clock ordering alone is therefore insufficient.

The follow-up queues local metadata invalidation at Cleanup and dispatches it
outside the callback before background sealing. No served path is opened by
the daemon. The timestamp check now first requires kernel/engine agreement
within 500 ms, before the 1 s delayed-seal window; it records the initial stale
observation and convergence latency. Both the native and logical timestamp
must then stay unchanged across the seal delay, including explicit pre-Unix
times. This distinguishes a stale kernel observation from a changed event time.

Release `e8ee0871...` passes all 160 host tests, format and Clippy, all six repeated
timestamp laps (three per mount type) and all 36 lifecycle cases in the isolated
guest. Forced daemon exit max is 0.015 s and ordinary graceful exit max is
0.124 s. The [matrix](../../../test-evidence/wave-6/mount-lifecycle-lazy-coherence-sandbox-20261024.json)
and [repeated checks/open diagnostic](../../../test-evidence/wave-6/lazy-coherence-sandbox-20261024.json)
retain the records and counter snapshots. A subsequent harness tightening includes
the logical-time RPC in the 500 ms deadline; it requires a new run. Cache
settings, cleanup flushing, access checks and durability barriers remain enabled.
The native coherence failure is retained and does not qualify the earlier binary.

The [seal snapshot/recovery follow-up](seal-snapshot-recovery.md) is not included
in this release and needs its own qualification. Performance acceptance remains
OPEN while the full gate runs.

The preceding diagnostic release `3ad93ba2...` passed 19 of the 20 attempted
lifecycle cases before the timestamp failure stopped the matrix. Forced daemon
exit max was 0.012 s and ordinary graceful exit max was 0.133 s. Those partial
results do not qualify that binary. Reports, native scratch bytes, source hashes
and exact native timestamps are exported under
`test-runs/mount-sandbox/cache-output/lazy-open-v5/complete-evidence.zip` and its
timestamp manifest. The first zip export failed because Windows Python cannot
convert pre-Unix file timestamps; the repaired archive carries timestamp values
in JSON and uses a representable ZIP header date without changing source files.
