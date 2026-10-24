# Step 7: runtime cutover and qualification

The replacement now occupies the canonical content/core/runtime/native paths.
The old write journal, overlay maps, save pipeline, admission engine, lease
models, cone projection and qualification test apparatus have been removed.
The pairing catalog shares the metadata database. The CLI and desktop conflicts
view use entity versions. Session framing/authentication/TLS suites are retained.

WinFsp and FUSE use scratch handles with kernel caching and remote notification.
The real Windows smoke passes create/read/write/fsync/rename and paired cached
invalidation. FUSE testing found a metadata-successor scratch-sharing bug; a
content-version anchor and exact regression tests now cover that case.

Qualification is **open**. The initial full matrix failed performance targets
and exposed stale unsealed sizes to Git. The current working projection now
supplies draft sizes; further full-gate runs are required. These are measured
failures, not passing qualification. See `test-evidence/bench/wave6-*.json` and
the benchmark command in `docs/VALIDATION.md`.
# Windows cache follow-up

**Historical design, superseded by
[W6-P0-MOUNT-LIFECYCLE](../WAVE_6_MOUNT_LIFECYCLE_BLOCKER.md).** The persistent
root handle and volume-drain thread described below caused a termination-time
kernel cleanup dependency and have been removed. The current adapter relies on
driver flush-before-Cleanup and per-file fsync; see
[the lifecycle fix note](mount-lifecycle-fix.md). Do not restore the old drain.

WinFsp now uses `FileInfoTimeout=INFINITE`, the setting required to enable
Windows data caching. Closed writable files queue a volume cache drain at the
one-second/64 MiB seal boundary. A persistent root handle permits a synchronous
`FlushFileBuffers` without raw-volume privileges; WinFsp drains cached pages
before one batched durable seal. Explicit file/volume/control flushes remain
immediate. System volume flushes can also checkpoint open writers. Drafts with
pending native pages cannot be sealed as sparse zero-filled data. Remote
notifications use normalized names; paging reads and metadata follow the
visible entity head after invalidation. Per the user's decision, existing
Windows readers may see remote updates. Their local edit parent still binds
conflict detection. A paired, cached-reader smoke test passed in 62 ms;
the full performance gate remains open.

Linux invalidation writes no longer serialize replies behind a blocking kernel
notification. File inodes distinguish content versions, so new opens get the
remote head while existing readers keep their original data cache. Lookup/open
references retire obsolete inode bindings. The paired real-mount smoke covers
this with a reader held open across the update, plus read-only automatic saves.

