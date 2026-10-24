# Release draft locks before durability; preserve successor writes

Date: 2026-10-24. W6-P0 triage candidate 2, with recovery prerequisite.

An engine-level regression holds a real SQLite writer on the pack index while
the sealer reaches durability. The preceding implementation kept the scratch
mutex locked for that whole wait. The correction snapshots each prepared
reference and receipt independently, releases its scratch guard, then flushes
packs and commits the metadata batch. Prepared references stay pinned throughout.
Seal publication remains serialized. Explicit write-time changes coordinate with
that publication boundary; ordinary data writes and Close can proceed.

Each draft has a revision counter. If data changes after its snapshot, the
completed seal advances the immutable parent but retains the dirty bitmap and
schedules a successor seal. A concurrent shrink followed by re-extension keeps
its zero-filled ranges: the recovery sidecar now saves the immutable visibility
boundary, with a default for old records. The regression writes and closes while
durability is blocked, verifies the changed draft was not cleared, and restarts
to verify every successor byte, including the zero ranges.

This exposed an existing receipt-recovery error. Recovery rebased the sidecar to
a committed receipt before identifying physical ranges appended beyond the
original base. A stale sidecar for an originally empty file therefore recovered
`new bytes` as `old bytes`. The deterministic regression failed before the fix.
Recovery now captures materialized extensions and the original visibility
boundary before rebasing, including records in the old sidecar format.

The lock-release and receipt-extension regressions both pass after the fixes;
all 84 library tests passed at that point. The later full host suite, including
the driver preflight, passes 165 tests (one opt-in codec benchmark is ignored);
formatting, strict Clippy and release
build pass. Separate VM qualification is in progress. The earlier `e8ee0871...`
lifecycle and failed/stranded performance gate do not qualify this candidate.

Expected VM movement: Close callback time falls because it no longer waits for
pack fsync or the receipt commit. Metadata counters added alongside this work
split cached SQL queries/commands, transaction batches, row scans, version
decoding, JSON encoding, projection indexing and actual map copies. Scopes are
inclusive: `version_total` and `projection_delta_total` overlap their nested
SQL/JSON counters and must not be added to them. Map-copy rows count copied maps,
their entry counts and copy time. No map, database, cache or durability policy
has been changed by the instrumentation.

Cache settings, cleanup flushing, file fsync, receipt ordering, recovery byte
comparison and the no-self-handle rule remain enabled. P0 stays OPEN.
