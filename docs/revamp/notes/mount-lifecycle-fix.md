# W6-P0-MOUNT-LIFECYCLE: VM lifecycle passes, performance acceptance failed

Date: 2026-10-23. Demand: [WAVE_6_MOUNT_LIFECYCLE_BLOCKER.md](../WAVE_6_MOUNT_LIFECYCLE_BLOCKER.md).
Design rules: [WAVE_6_REPLACEMENT.md §3a](../WAVE_6_REPLACEMENT.md).

## What changed

- **No self-handle.** `tk_cache_open`/`tk_cache_flush`/`tk_cache_close`, the
  retained root handle in `MOUNT`, the `tkfs-cache-drain` thread and the
  pending-close bookkeeping (`cache_pending`, `cache_generation`,
  `CACHED_CLOSES`) are gone. The daemon process opens nothing under its mount.
- **Driver delivers cached bytes.** `FlushAndPurgeOnCleanup = 1` in
  `tk_mount_start`. The FSD flushes a file's dirty cached pages as Write
  callbacks before Cleanup, so the last Cleanup of a writable handle sees a
  complete scratch file and the existing sealer thread seals it after the
  write-back delay. Explicit flush/fsync is unchanged (WinFsp flushes the file's
  cache before posting Flush). A foreign process's volume flush still seals every
  changed scratch through `flush_native_volume`.
- **Paging writes on any handle.** `Engine::write` accepts `ConstrainedIo`
  writes regardless of the handle's own access; ordinary writes keep the check.
  This removes the reader-first `ACCESS_DENIED` chain that the blocker names as
  the probable trigger.
- **Paging identity after delete/recreate.** VM smoke found another failure:
  the retained reader's WinFsp cache context could receive paging writes for
  the recreated pathname. Writing its old scratch and flushing the new
  writer's scratch sealed 23 zero bytes after a successful file fsync.
  `write_native` now follows the same visible entity as cache reads, and
  `flush_native_file` seals that entity's dirty drafts. A dirty local edit
  keeps its original parent across a remote content update. Live renamed
  entities retain their identity when their old name is reused. Core/private
  handles and ordinary-write access checks retain their existing semantics.
- **Self-open guard.** `create_open` in `native/mount.c` refuses Create/Open
  requests whose originating PID is the server's (`STATUS_ACCESS_DENIED`,
  logged once); `mount-stats` reports `self_open_refusals`.
  The same guard now runs at `GetSecurityByName`, before its Rust lookup:
  WinFsp can request security before invoking Create/Open, and the requesting
  server thread might already hold the store lock. The explicit VM-only
  `TKFS_TEST_MOUNT_SELF_OPEN=1` negative test opens the root twice while holding
  that lock; both opens must be refused and only one refusal logged. This seam
  is unset in ordinary operation.
- **Failure-safe shutdown.** Control `shutdown` sets `stopping` before it
  reports a flush error and records the error in `status`. `detach()` stops the
  dispatcher first and returns the final seal error afterwards. The daemon loop
  runs both `engine.shutdown()` and `detach()` and reports
  `SHUTDOWN_INCOMPLETE` with every failure. The Windows pre-flush before control
  operations is gone with the root handle. Fault injection:
  `tkfs rpc inject-fault '{"name":"flush"}'` arms a one-shot seal failure.
  A failed control flush is remembered before `stopping` is published, so a
  successful final retry cannot erase it or return daemon exit code 0. Startup
  and loop failures, including a bind failure after attach, now run the same
  shutdown/detach path. `flush-persistent` also fails final retries, allowing
  the VM matrix to check that recoverable scratch survives and re-seals on
  reopen. Native partial-attach injection covers `after-create`, `after-mount`
  and `after-dispatcher`; the last case joins the dispatcher before deletion.
- **Bounded harness waits.** `scripts/mount_lifecycle.py` stops daemons with a
  20 s graceful deadline, an optional bounded procdump, a 10 s post-kill wait,
  and writes `<daemon>-exit-state.json` next to the log. The stop RPC and process
  exit share the graceful deadline; a failed RPC still receives the remaining
  exit time before a kill. A fallback kill fails graceful qualification even
  if it succeeds. Benchmark stores are retained for recovery after failure.
  A daemon that survives the kill fails the run; no further mounts are started. Used by
  `replacement_smoke.py` and `bench.py`. `replacement_smoke.py --forced-kill`
  kills peer A with a dirty cached write and a retained reader open, requires
  exit within 10 s, the mount gone, and a fresh PowerShell drive enumeration.
  The smoke also fails on any logged `mount I/O operation` error.

## Verified on the development host (no mount created)

- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`,
  `cargo test -- --test-threads=4` (62 lib, 59 + 19 integration tests passed;
  one pre-existing library ignore and one documentation ignore). One earlier
  default-thread session test timed out under concurrent load; its isolated
  retry and the final full suite passed.
- New invariants: `paging_writes_are_accepted_on_a_read_only_handle`,
  `a_paging_write_after_cleanup_is_sealed_again`,
  `a_failed_flush_still_stops_the_daemon_and_keeps_recoverable_scratch`
  (each watched failing before the change).
- `python scripts/mount_lifecycle.py` self-test (kill fallback, stranded
  detection).

## VM lifecycle acceptance: passed

All 32 cases of `scripts/mount_acceptance.py` passed in Windows Sandbox on
Windows build 26100 and WinFsp 2.1.25156. These cover drive-letter and directory
mounts, graceful and forced exit at idle and during cached writes, continuous
fsync and paired delete/recreate with retained readers; save/restart; all three
partial-attach fault stages; startup bind failure; self-open refusal under the
store lock (two denials, one log entry, for both mount types); and persistent flush failure
with exit code 1, mount release and scratch recovery. No stranded daemon or
stale usable mount remained; fresh PowerShell and unrelated native I/O probes
passed. Forced exit max: 0.015 s; ordinary graceful exit max: 0.157 s.

The release binary SHA-256 is
`5e5b3a9e3f746fbe8eac01143c224b37ae0f6d62d96497046cc60a10d920c6ed`.
The [versioned evidence](../../../test-evidence/wave-6/mount-lifecycle-sandbox-20261023.json)
retains each PID, exit record, binary/source hashes and validation facts. Local
logs and recoverable stores are exported under
`test-runs/mount-sandbox/final-output/acceptance-final`. Earlier guest runs that exposed
setup, harness and zero-byte-seal failures are retained beside that run.

## Performance acceptance: failed, demand remains open

The full-gate attempt on the intermediate `85b168b9...` binary failed:
copy was 12.48x native, sequential write/read 3.33x/2.91x and random overwrites
8.98x. `git add -A` exceeded 600 s during 50,000-file fixture preparation and
survived its 10 s post-kill wait. The daemon then stopped gracefully in 0.998 s;
a later process query found neither Git nor TKFS running. The run failed and
no further mounts were started in that guest. Reports, binary and recoverable
stores were exported before it was disposed. Final lifecycle acceptance used
a fresh Sandbox.

Git status, .NET builds and the paired phase were not measured. The
[cost/timeout evidence](../../../test-evidence/wave-6/mount-lifecycle-cost-sandbox-20261023.json)
links the complete native report and partial mounted report. These numbers do
not isolate cleanup purge cost from other filesystem overhead and do not
qualify the final binary's performance. The demand remains OPEN. A Close-based
fallback must preserve cached-byte delivery, file fsync and recovery with
retained readers; disabling cache or cleanup flushing without that proof is
not an accepted fix.
