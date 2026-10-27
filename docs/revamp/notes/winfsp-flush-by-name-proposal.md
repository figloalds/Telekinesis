# Upstream proposal draft: flush one cached file by name, with status

Drafted 2026-10-26 after the user's
[decision](perf-gate-reassessment.md#11-decision-2026-10-26) to keep the
official WinFsp build and propose the one driver change upstream. **Nothing has
been submitted.** The change is unbuilt as a driver and unrun; see section 4.

| Item | Value |
|---|---|
| Upstream base | `winfsp/winfsp` `master` at `4425d480` (`v2.2B5-1`) |
| Local branch | `flush-by-name` in `..\winfsp`, uncommitted |
| Patch | [winfsp-flush-by-name.patch](winfsp-flush-by-name.patch), 8 files, 438 lines added |
| Patch SHA-256 | `d52bc02eaa80f60975db08aa5702ee89190acd7991ffdeb12ed50a31541f1151` |

## 1. Why Telekinesis needs it

Section 8 of the [reassessment](perf-gate-reassessment.md#8-follow-up-review-2026-10-26)
showed that the only way a writable mount can turn off
`FlushAndPurgeOnCleanup` is with proof that a closed file's cached bytes reached
the server. The existing per-path notify cannot give that proof. In the pinned
Beta5 source it queues a work item and returns before the flush runs
(`volume.c:1433`), and its worker drops the flush status and purges the cache
(`file.c:2395`). Purge-on-cleanup is the largest measured cost behind random
overwrite: the memory reference takes 592.5 ms with it and 49.3 ms without
([section 9](perf-gate-reassessment.md#9-complete-paired-policy-reference-measurement-2026-10-26)).
Line numbers here are upstream `4425d480`.

Three properties are required, and the existing calls give none of them:

- **Completion.** Return only after the Write requests for that file's dirty
  pages have been answered.
- **Status.** Report a failed or cancelled Write instead of discarding it.
- **No lifecycle dependency.** A server killed while it waits must still exit.
  The [blocker](../WAVE_6_MOUNT_LIFECYCLE_BLOCKER.md) forbids any
  termination dependency on I/O to the server's own namespace.

## 2. The change

New control code `FSP_FSCTL_FLUSH_BY_NAME` on the volume handle, with DLL
wrappers `FspFsctlFlushByName` and `FspFileSystemFlushByName(FileSystem,
FileName, Timeout)`.

- **Kernel dispatch** validates an absolute name and reserved flags. It takes a
  device reference and the volume delete resource shared, exactly as
  `FspVolumeNotify` does. It then queues a work item and waits for it.
- **The worker** looks the name up in the context table. If it finds a live
  file node with a data section, it acquires the node `Full` exclusive and calls
  `FspCcFlushCache`, then reports the status. This mirrors the per-file
  `FlushBuffers` path (`flush.c:114`) without posting a Flush request or
  purging. The lazy writer and the modified page writer take the same lock
  exclusive (`callbacks.c:242`, `callbacks.c:131`), so no other write is in
  flight while the flush runs.
- **The wait** copies the alertable kernel-mode loop of
  `FspQeventCancellableWait`, which the transact path already relies on inside
  `FsRtlEnterFileSystem`. It ends on completion, on timeout or when the calling
  thread is terminating. A terminating server therefore does not wait for its
  own paging I/O. The worker is released when `FspVolumeDelete` stops the
  queue, the same ordering that fixed issue #682 for notify.
- **Timeout** is in milliseconds or `INFINITE`. Expiry returns
  `STATUS_IO_TIMEOUT`, because `STATUS_TIMEOUT` counts as success.
- **The DLL refuses calls from dispatcher threads** with
  `STATUS_POSSIBLE_DEADLOCK`. The flush needs dispatcher threads to answer its
  Writes and may wait for a node that the calling request owns.
- **A name with no file node succeeds with no Writes.** A node leaves the
  context table only at its last `IRP_MJ_CLOSE`, after any data section is gone
  (`file.c:1087`), or when the file is deleted (`file.c:933`). On a
  case-insensitive volume the table compares without case (`device.c:760`).
- **An older driver** rejects the unknown code; the DLL then returns
  `STATUS_NOT_IMPLEMENTED`.

The test `flush_by_name_test` in `tst/winfsp-tests/flush-test.c` counts Write
requests with the same operation hook that `create_pid_test` uses. It checks
that names without cached data produce no Writes, and that bad names are
refused. It writes and closes a cached file, then flushes it twice. The second
flush must produce no further Writes, which fails if the first one returned
before its Writes completed. It finally reads the bytes back.

## 3. Draft pull request text

> **Title:** sys, dll: add FspFileSystemFlushByName to flush one cached file and report the status
>
> A file system that leaves `FlushAndPurgeOnCleanup` off keeps closed files'
> dirty pages in the cache. Those pages reach it later as Write requests, but it
> has no way to learn when one file's data has been delivered. `FspFileSystemNotify`
> flushes as a side effect, but it runs asynchronously, ignores the flush status
> and purges the cache. Volume `FlushFileBuffers` would need a handle to the
> file system's own volume, which a file system process must not hold.
>
> This adds `FSP_FSCTL_FLUSH_BY_NAME` and `FspFileSystemFlushByName(FileSystem,
> FileName, Timeout)`. The FSD looks the name up in the context table and
> flushes the file node's data section under `Full` exclusive, like the
> per-file FlushBuffers path, without purging or posting a Flush request. The
> calling thread waits on a system worker with the same alertable wait that
> `FspVolumeFastTransact` uses, so a file system that is terminated while it
> waits does not block on its own paging I/O. The worker holds the volume
> delete resource shared like the notify worker, and `FspVolumeDelete` stops the
> IOQ before acquiring it. The DLL refuses calls from dispatcher threads.
>
> Returns `STATUS_SUCCESS` once all data that was dirty when the flush started
> has been delivered by successful Write requests. A name with no file node, or
> a directory, succeeds without Writes. A timeout returns `STATUS_IO_TIMEOUT`
> and leaves the flush running. Flags are reserved and must be zero.
>
> Adds `flush_by_name_test` to winfsp-tests. Not included: FUSE and .NET
> bindings, the generated API document, a Changelog entry.

Before posting, open a GitHub discussion asking whether the maintainer wants
this API, since it adds a control code and a public function. The maintainer
has answered cache-policy questions there before
([discussion #607](https://github.com/orgs/winfsp/discussions/607)).

## 4. What is verified and what is not

| Check | Result |
|---|---|
| DLL sources (`dll/fsctl.c`, `dll/fs.c`) with MSVC `/Zs /W4` against the patched headers | Compile; only pre-existing warnings |
| `flush-test.c` with MSVC `/Zs /W4` | Compiles; only a pre-existing warning |
| Kernel sources (`sys/volume.c`, `sys/fsctl.c`) | **Not compiled.** The host has no WDK kernel headers |
| Driver build, signing, load | **Not done** |
| winfsp-tests, Driver Verifier, fscrash | **Not run** |
| Telekinesis use of the API | **Not written**; production keeps purge on |

The kernel half was checked only by reading it against the existing notify,
FlushBuffers and transact-wait code it copies.

## 5. Steps before submitting upstream

1. **Sign the WinFsp contributor agreement yourself.**
   [Contributors.asciidoc](../../../../winfsp/Contributors.asciidoc) requires
   every contributor to add their own name and email. Doing so is a binding
   legal agreement that assigns joint copyright. It has to be your own act, so
   this draft does not touch that file.
2. Build the driver with the WDK in Visual Studio and fix any compile errors.
3. In a test-signed VM, run `winfsp-tests` including `flush_by_name_test`
   under Driver Verifier, plus fscrash.
4. Add forced-termination coverage: kill a file system process while it waits
   in `FspFileSystemFlushByName` on a file whose Writes block in user mode. The
   process must exit and the volume must disappear. This is the property
   Telekinesis depends on, and it has no upstream test yet.
5. Open the discussion, then the pull request.

## 6. How Telekinesis would use it

Nothing changes until an official signed release contains this API and the
loader manifest pins that release. Until then production keeps
`FlushAndPurgeOnCleanup=1`.

With the API available, the purge-off candidate of reassessment section 3
becomes:

- The last writable Cleanup marks the entity pending seal, as today.
- After the write-back delay, the sealer thread calls
  `FspFileSystemFlushByName` for the entity's current name. It must not hold
  engine or store locks, because the resulting Writes take them.
- `STATUS_SUCCESS` means every cached byte reached `write_native`; the sealer
  seals. Any other status is a durability failure for that entity; the scratch
  stays for the startup re-seal.
- Graceful shutdown flushes every pending entity with a bounded timeout before
  it stops the dispatcher. A timeout is reported as a failure, never waited out.
- A deleted entity needs no flush. A renamed one is flushed by its new name.

This addresses the section 8 cases as follows:

| Section 8 case | Covered by |
|---|---|
| Worker starts after a quiet interval | The call waits for the worker; no quiet interval is used |
| Partial flush gaps | Success means the whole section was flushed under `Full` exclusive |
| No-op completion | No file node or no data section returns success with no Writes |
| Flush failure | The `FspCcFlushCache` status is returned |
| Bounded shutdown | Per-call timeout; termination ends the wait |
| fsync, rename/delete | Unchanged fsync path; flush by current name; deleted files skipped |

Two cases still need Telekinesis tests or VM proof. Data still dirty in a
memory-mapped view at flush time is covered only as far as `CcFlushCache`
covers it, which is the same as today's cleanup flush. A Write that arrives
after the flush, from a handle reopened meanwhile, still needs the existing
successor-seal path.
