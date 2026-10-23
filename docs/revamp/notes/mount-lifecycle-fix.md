# W6-P0-MOUNT-LIFECYCLE: fix implemented, VM acceptance pending

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
- **Self-open guard.** `create_open` in `native/mount.c` refuses Create/Open
  requests whose originating PID is the server's (`STATUS_ACCESS_DENIED`,
  logged once); `mount-stats` reports `self_open_refusals`.
- **Failure-safe shutdown.** Control `shutdown` sets `stopping` before it
  reports a flush error and records the error in `status`. `detach()` stops the
  dispatcher first and returns the final seal error afterwards. The daemon loop
  runs both `engine.shutdown()` and `detach()` and reports
  `SHUTDOWN_INCOMPLETE` with every failure. The Windows pre-flush before control
  operations is gone with the root handle. Fault injection:
  `tkfs rpc inject-fault '{"name":"flush"}'` arms a one-shot seal failure.
- **Bounded harness waits.** `scripts/mount_lifecycle.py` stops daemons with a
  20 s graceful deadline, an optional bounded procdump, a 10 s post-kill wait,
  and writes `<daemon>-exit-state.json` next to the log. A daemon that survives
  the kill fails the run; no further mounts are started. Used by
  `replacement_smoke.py` and `bench.py`. `replacement_smoke.py --forced-kill`
  kills peer A with a dirty cached write and a retained reader open, requires
  exit within 10 s, the mount gone, and a fresh PowerShell drive enumeration.
  The smoke also fails on any logged `mount I/O operation` error.

## Verified on the development host (no mount created)

- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`
  (56 lib, 59 + 19 integration tests, one pre-existing ignore).
- New invariants: `paging_writes_are_accepted_on_a_read_only_handle`,
  `a_paging_write_after_cleanup_is_sealed_again`,
  `a_failed_flush_still_stops_the_daemon_and_keeps_recoverable_scratch`
  (each watched failing before the change).
- `python scripts/mount_lifecycle.py` self-test (kill fallback, stranded
  detection).

## Still open: VM acceptance

Run in an isolated Windows VM, per the blocker's acceptance list:

```powershell
cargo build --release
python scripts/replacement_smoke.py --binary target/release/tkfs.exe --forced-kill
python scripts/bench.py --label wave6 --matrix-tkfs target/release/tkfs.exe
```

Also required: the same smoke against a directory mount, graceful and forced
exit during flushing and during paired delete/recreate, the fault-injected
shutdown (`inject-fault` then `stop`: the daemon must exit, release the mount
and report the failure), and the purge-cost comparison of the gate. If the
`git status` or build workloads regress past their targets because of
`FlushAndPurgeOnCleanup`, switch to sealing at Close as §3a describes; the
no-self-handle rule stays either way.
