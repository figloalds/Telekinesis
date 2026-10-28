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

## Correctness-first continuation (2026-10-28)

The user directed remaining implementation/correctness work to proceed and
performance gates to run last. Host mounts and cutover still require full P0
acceptance; all mounted checks remain in an isolated Windows VM.

Conflict resolution now seals pending local saves before selecting a head,
invalidates both names when the choice renames the file, and retains unselected
local bytes through restart. Delete-conflict notifications follow the projected
state. The desktop distinguishes current heads from retired history. Failed
delete cleanup retires writers and Close releases handles while reporting the
failure; import opens/validates its source before mutation, and a refused large
`cat` releases its reader. Seven focused regressions cover these failures.

Windows has 221 passing Rust tests; Linux has 218. Both pass strict Clippy and
formatting and build release runnables. The 28 Python harness tests, bounded
lifecycle-helper selftest and five inert WinFsp binding fixtures pass. The
expanded real-mount smoke adds offline conflict/rename resolution and pending
alternative retention. Final binary `24042550...` passes all 44 isolated
lifecycle cases and four extra timestamp cases, with no stranded process;
forced exit max is 0.004 s and ordinary matrix graceful exit max 0.113 s.
The [retained report](../../../test-evidence/wave-6/correctness-20261028.json)
includes exact source/binary identities, controller exit 0 and a verified
25.2 MB native evidence export. Performance acceptance remains deferred and open;
these correctness results do not close P0 or authorize host mounted cutover.

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

