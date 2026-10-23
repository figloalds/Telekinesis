# W6-P0-MOUNT-LIFECYCLE: kernel cleanup hang

Status: **OPEN, P0, immediate Wave 6 demand** (2026-10-23). Resolve this before
other Wave 6 feature work, runtime cutover or mounted performance qualification.
Documentation and diagnosis do not close this demand. The fix below is
implemented in this checkout ([note](notes/mount-lifecycle-fix.md)); the demand
closes only when the VM acceptance checks pass.

## Observed failure

The replacement smoke test's peer B, PID 49692, was serving `Y:` with
`--state test-runs/replacement/1792775748686240700/b daemon --mount Y:`.
Here `--state` is a directory option, not a status operation. Windows has already
set exit code 1 and `IsProcessDeleting`; its last thread (58708) has pending I/O
and the process remains unsignaled. Opening it with terminate access succeeds.

The administrator-captured kernel dump shows:

```text
PspExitLastThread -> PspRundownSingleProcess -> ObKillProcess
  -> ExSweepHandleTable -> ObCloseHandleTableEntry
  -> IopCloseFile+0x15d -> KeWaitForSingleObject
```

The pending request is `IRP_MJ_CLEANUP` (0x12), returned as `STATUS_PENDING`,
with an infinite completion-event wait. Its file object has name `\` (a root
directory). The IRP is in a cancel-safe queue; its completion chain includes
`FLTMGR!FltpPassThroughCompletion`.

The full live kernel dump confirms that both the root file's device and the
pending request's device belong to `winfsp-x64.sys` (driver object
`\FileSystem\WinFsp+20261002T205313Z`). Filter Manager is above it.
The root has WriteAccess=true and SharedRead/SharedWrite/SharedDelete=true,
matching `tk_cache_open()`'s GENERIC_WRITE root handle.

A fresh PowerShell also failed `InitializeDefaultDrives`. The earlier Visual
Studio stall is a reported symptom, not a proven instance of this same wait.

## Unsafe application and test sequence to remove

1. [`tk_cache_open`](../../native/mount.c) opens the served root with
   `CreateFileW`; [`attach`](../../src/mount.rs) retains that handle in `MOUNT`
   for `FlushFileBuffers`. The filesystem server consequently depends on I/O
   against its own mounted namespace.
2. Normal `detach()` closes that root and then stops the dispatcher. Forced
   process termination bypasses this Rust cleanup and kills the callback workers.
3. [The smoke harness](../../scripts/replacement_smoke.py) falls back from a
   failed/timed-out graceful stop to `proc.kill(); proc.wait()`, with no timeout
   on the final wait. Windows can then wait for root cleanup which needs the
   filesystem service it just terminated, stranding the daemon and the harness.

In addition, [control::execute](../../src/orchestrator/control.rs) pre-flushes
`shutdown` with `flush_cached(true)?` before dispatching the operation, and
`detach()` starts with the same fallible flush before releasing mount resources.
A flush failure therefore skips shutdown/detachment work. These error paths must
become lifecycle-safe while retaining recoverable data and reporting durability
failure. The captured root FILE_OBJECT's FinalStatus is `0xc0000022`
(ACCESS_DENIED), consistent with the write failures; this does not prove which
operation first caused the earlier freeze.

The dump proves the termination-time root cleanup hang. The self-mount handle
is the source-level lifetime dependency to eliminate; reordering graceful
cleanup or adding a `Drop` handler alone does not cover forced termination.
A folder mount still uses `WinFsp.Disk` and these callbacks; replacing `Y:`
with a folder is not an accepted fix. Drive-letter enumeration can expose the
hung mount to other applications.

Peer B also logged two `mount I/O operation 5: ACCESS_DENIED` errors. Audit
[`Engine::write`](../../src/runtime/engine.rs)'s per-handle writable check
against WinFsp cached/paging writes. These errors are a separate lead; the
post-termination dump does not establish why graceful operation first failed.

## Why the kernel cannot recover

Process termination runs `ExSweepHandleTable` on the dying process: every
handle is closed in table order, one at a time, synchronously. Closing the
retained `Y:\` root handle issues `IRP_MJ_CLEANUP`, which `winfsp-x64.sys`
queues for the user-mode file system. That file system is the process being
swept; its dispatcher threads were already terminated, so nobody will ever
transact the request. The dump shows the volume device still alive, so the
volume (fsctl) handle had not been swept when the sweep blocked on the root.
Handle-table order is not under user-mode control (indices are reused), so no
cleanup ordering, `Drop` handler or "close the root first" change can make a
retained self-handle safe. The mount passes `IrpTimeout = 60 000 ms`
([`attach`](../../src/mount.rs)); the pending cleanup was not timed out, so
driver-side expiry must not be relied on either. The sweep thread belongs to
the deleting process: it holds the process alive, drive-letter enumeration
still reaches the hung volume, and anything that touches that volume
(Explorer, a fresh PowerShell's `InitializeDefaultDrives`, Visual Studio)
blocks behind it. The only acceptable fix is that the server process never
holds a handle to, or issues I/O against, its own served namespace.

This is the common exit path, not a harness quirk. Task Manager, `taskkill /F`,
a console close (5 s grace, then termination), a panic that aborts the process,
and an operating-system shutdown while the daemon runs all take the same route.
With the retained root handle, an OS shutdown with a mounted daemon is expected
to hang as well.

## Probable trigger: paging writes on a read-only handle

Peer B's two `mount I/O operation 5: ACCESS_DENIED` errors are most likely the
first cause of the chain, not a separate lead. With `FileInfoTimeout = INFINITE`
the Windows cache is shared per pathname, and the Cache Manager issues its
paging writes on the file object it initialised caching with. That can be a
handle opened read-only before the writer opened. [`Engine::write`](../../src/runtime/engine.rs)
rejects any write on a handle whose `writable` flag is false, so the lazy
writer's write fails, the dirty pages stay dirty forever, the root
`FlushFileBuffers` fails with ACCESS_DENIED, every `flush`/`shutdown`/`detach`
returns before releasing anything, the harness times out, kills the daemon, and
the sweep hangs. The smoke test's paired delete/recreate step opens a reader
(`remote.open("rb")`) before the writer on the same path, exactly this pattern.
The root FILE_OBJECT's FinalStatus of `0xc0000022` is consistent with it.

WinFsp already enforces access at Create; the user-mode file system must accept
a write marked `ConstrainedIo` (paging I/O, which must not extend the file) on
any handle of the entity, regardless of that handle's own access. The entity's
scratch file is shared by all handles, so the bytes land in the right place.

## Further call sites and gaps

- [`orchestrator::run`](../../src/orchestrator/mod.rs) calls
  `engine.shutdown()?` before `detach()?`. A seal or checkpoint failure there
  also skips the detach, and `main` then returns with the dispatcher running and
  the root handle open; `ExitProcess` kills the dispatcher threads and the sweep
  hangs the same way.
- [`scripts/bench.py`](../../scripts/bench.py) has the same unbounded
  `proc.kill(); proc.wait()` fallback as the smoke harness.
- Nothing prevents the server from opening its own namespace again. WinFsp
  reports the originating process of Create/Open requests
  (`FspFileSystemOperationProcessId`); the shim must refuse opens from its own
  PID so a regression fails loudly instead of re-introducing the hang.
- The volume drain that needed the root handle is documented only in
  [the step 7 note](notes/replacement-step-7.md), not in the plan. The plan's
  "seal on close" (section 4) silently assumed cached bytes reach the server
  before Cleanup; they do not unless the driver flushes them. The plan now
  carries the invariants in its Windows cache and lifecycle section.

## Required fix and acceptance

Design decision (2026-10-23): no helper process and no self-handle. The server
process holds no handle to, and issues no I/O against, the mounted namespace.

- Delete `tk_cache_open`/`tk_cache_flush`/`tk_cache_close`, the retained root
  handle in `MOUNT`, the cache-drain thread and the pending-close bookkeeping.
  Cached bytes reach the server through the driver instead: set
  `FlushAndPurgeOnCleanup = 1`, so the FSD flushes (and purges) a file's cached
  pages as Write callbacks before it delivers Cleanup. Seal on close then works
  as the plan describes, with the existing write-back delay. Explicit fsync
  stays a durability point because WinFsp flushes that file's cache before
  posting Flush. Dirty cached pages of handles still open at a forced kill are
  lost, which is the plan's stated crash contract.
- Measure the purge cost against the benchmark gate in the VM. If `git status`
  or the build workloads regress past their targets, the fallback is to seal at
  Close (after the Cache Manager's lazy writes) instead of at Cleanup, keeping
  the flag off. Either way the first rule above holds; the choice only moves
  seal latency.
- Accept `ConstrainedIo` writes on any handle in `Engine::write`; keep the
  access check for ordinary writes.
- Refuse Create/Open requests whose originating process is the server itself
  (STATUS_ACCESS_DENIED, logged once) in the native shim.
- Make every shutdown path release mount resources regardless of seal or flush
  failure: the control `shutdown` sets `stopping` before reporting the flush
  error, `detach()` stops the dispatcher before returning the final seal error,
  and `orchestrator::run` runs both `engine.shutdown()` and `detach()` and
  reports the first error afterwards. A failed flush is reported as a durability
  failure; recoverable scratch files stay on disk for the startup re-seal.
- Bound graceful stop, diagnostic capture and the post-kill wait in every mount
  harness (`replacement_smoke.py`, `bench.py`). On timeout, retain PID, mount,
  log and exit state, write them next to the daemon log, and fail the run; never
  wait forever, report success or start further mounts over a stranded process.
- Inject seal/flush failure and partial-attach failure. Shutdown must report
  the durability failure and retain recoverable data while releasing mount
  resources within a bounded deadline; it must not skip cleanup because a flush
  failed or claim the failed flush succeeded.
- In an isolated Windows VM, prove graceful exit within the existing 20-second
  smoke deadline and forced exit within 10 seconds at idle, during cached writes
  and flushing, and during paired cached delete/recreate with retained readers.
  No deleting daemon, pending root cleanup or stale usable mount may remain.
- Repeat with drive-letter and directory mounts. Check fresh PowerShell drive
  initialization and unrelated native filesystem operations stay responsive.
  Re-run the small existing save/restart and cached-update checks, including a
  reader opened before a writer on the same path with no ACCESS_DENIED logged.

Until these pass, mounted repro/qualification runs belong in an isolated VM,
not the development host. No fresh mount was created for this investigation.

## Evidence retained

Local dump/analysis directory: `%TEMP%/tkfs-hang-49692/`. The targeted kernel
dump was captured at 15:18:50 America/Sao_Paulo on 2026-10-23. Its concrete
objects are ETHREAD `ffffd209c8988080`, IRP `ffffd209a227eca0` and
FILE_OBJECT `ffffd209f6b12e60`. `engine-kernel.log`, `engine-follow.log`,
`engine-object.log`, `engine-resolve.log`, `exit-state.json` and
`findings.txt` preserve the queries, stack and source references. The full
Task Manager dump is under `%LOCALAPPDATA%/Microsoft/Windows/TaskManager/LiveKernelDumps/livedump.DMP`;
`engine-live.log` and `live-resolved-objects.json` retain its resolution.
Memory dumps stay local; the confirmed stack and actionable gate are recorded
here so the demand does not depend on temporary files surviving.
