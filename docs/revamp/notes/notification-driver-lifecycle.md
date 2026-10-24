# Notification/rename lifecycle dependency

The no-self-namespace-handle rule remains mandatory. It is insufficient with
the WinFsp 2.1.25156 driver once local metadata notifications are enabled.
Do not disable notifications, cache flushing or durability to avoid this fault.

## Observed in the isolated guest

Release `e8ee0871e22067fea960f8e6746bc15bef493a502c1d5a9c4ec0c87678ea18c1`
passed 36 short lifecycle cases and six timestamp laps, then hung during the
full .NET build gate. The build exceeded 600 seconds. The bounded runner
reported command PID 11088 stranded; daemon PID 6468 exceeded the shared
20-second graceful deadline and 10-second post-kill deadline. Later native
queries found both processes deleting with unsignaled process handles.

The targeted daemon dump resolves `FspVolumeDelete+0x38` waiting exclusively
on `VolumeDeleteResource` at `ffffaf065f1c8a30`. The full kernel dump resolves:

- Device extension `ffffaf065f1c8680`; its `FileRenameResource` is at
  `ffffaf065f1c8aa0`, owned shared by tagged pointer `ffffaf065f1c8cf3`.
  This is `&VolumeNotifyCount | 3`; the count is 4.
- .NET thread `ffffaf066f6d60c0` waits exclusively in
  `FspFsvolSetRenameInformation+0x112` on that rename resource.
- Eight System workers wait shared in `FspVolumeNotifyWork+0x74` on the
  same resource, behind the exclusive waiter. They retain shared ownership
  of the volume-delete resource. One retained work item has input length 2,
  the notification End sentinel; it cannot run to release the session.
- The loaded driver is exactly 2.1.25156.0, checksum `0x35c54`, image size
  `0x2d000`, timestamp `0x6841b212`, with matching private symbols.

These objects and stacks establish the same cycle as
[upstream PR #669](https://github.com/winfsp/winfsp/pull/669): its corrected
worker acquires shared access with starve-exclusive semantics so End can run.
The independent forced-termination/read-notification cycle in
[issue #682](https://github.com/winfsp/winfsp/issues/682) is another reason to
reject 2.1. It is a source-established dependency, not the cycle observed in
this dump. Its fix stops the IOQ before waiting on volume deletion.

## Dependency preflight

`native/winfsp_loader.c` performs all checks before `FspFileSystemCreate`:

1. Resolve the selected absolute DLL path; verify exact SHA256 and complete
   file version using `native/winfsp_driver_manifest.h`.
2. Load that file with restricted DLL search and verify its file identity.
3. Read `FspSxsIdent`, query the selected SxS service image path, verify the
   SYS hash/version and bind its final directory to the verified DLL.
4. Start the selected service without creating a filesystem volume.
5. Query `SystemModuleInformationEx` and require exactly one matching loaded
   path with the trusted SYS's image size/checksum. A nonzero kernel timestamp
   must also match. Unrelated older SxS modules do not invalidate selection.
6. Pin that DLL, direct the delay-load hook to it, eagerly resolve the shim's
   WinFsp imports and verify the complete IAT against its exports. Freeze module
   and SxS selection for the process lifetime; reject a previously wrong binding
   or later change. Keep a partially bound module pinned and require restart
   after an eager-binding failure. `/DELAY:NOBIND` prevents bound-IAT shortcuts.

File handles deny concurrent writes/replacement during verification and are
closed before return. Any unavailable or malformed identity fails closed with
a diagnostic. No served namespace is accessed. Kernel base addresses may be
redacted and timestamps may be zero on Windows build 26100; neither is used as
an unconditional prerequisite. Parsing regressions cover redaction, unrelated
SxS images, mismatches, duplicates, truncation and malformed offsets/paths.

This is operational loaded-image attestation, not a cryptographic hash of
resident code. The exact guest kernel disassembly of
`ExpConvertLdrEntryToModuleInfo` copies checksum from
`_KLDR_DATA_TABLE_ENTRY.CheckSum` (+0x78) to record +0x130. It obtains size and
path from the loaded entry as well; it does not reopen the configured file.
The full file hashes additionally bind the selected on-disk dependencies.

The [inert import fixtures](../../../test-evidence/wave-6/winfsp-import-binding-20261024.json)
exercise the actual test executable's 11 WinFsp delay imports in five bounded
children. Disabling the hook in the baseline child demonstrates the old default
helper choosing preloaded A after absolute B was loaded. The corrected path
chooses B, rejects prebound/mixed imports and refuses a later module change.
The stubs exit 88 if invoked; all cases exit normally without calling a WinFsp
export, service or mount. The integrated IAT/SQLite candidate
`805b8634...` passes 167 host tests and strict Clippy/release checks; the codec
benchmark is opt-in and the fixture test is exercised explicitly by
`python scripts/check_winfsp_binding.py`. All 44 VM lifecycle cases and six
timestamp laps pass on that candidate; forced exit is at most 0.018 s and
ordinary graceful exit at most 0.159 s. Its copy diagnostic exits gracefully
in 0.540 s, but COMMIT cost remains high. The
[retained record](../../../test-evidence/wave-6/iat-sqlite-sandbox-20261024.json)
does not replace the required complete performance gate.

Sandbox sets Windows' `ContainerType` flag. WinFsp's service-start helper
deliberately returns success without loading a driver in that environment;
the guest setup must explicitly start the verified SxS service. The first
Beta5 attempt correctly failed closed before creating a volume when no
driver was loaded. Its [failed report](../../../test-evidence/wave-6/mount-lifecycle-driver-not-started-20261024.json)
is retained; the corrected setup then began a new matrix in the same clean
guest. This was a preflight/setup failure with an already-exited process,
not a cleanup timeout.

The allowlist currently contains official
[WinFsp 2026 Beta5](https://github.com/winfsp/winfsp/releases/tag/v2.2B5),
full version 2.2.26286.0. Its MSI SHA256 is
`293ffb9bcc0b625c85079fe85fa77f8fe98170aba51a02ac9c83d58f6c3b0e2f`;
the header records hashes of the extracted x64 DLL/SYS. The MSI was read and
extracted without host installation. Only isolated guests install drivers.
Beta5 includes both notification fixes and cached-write memory-pressure fix
#677. It remains a prerelease; unknown builds are deliberately unqualified.

## Validation still required

The 42-case matrix adds four concurrent rename/fsync-write cases, graceful and
forced on drive-letter and directory mounts, plus two legacy receipt replays.
All 42 pass on candidate `5fce4ee6...`, along with six timestamp repeats and the
immutable-open diagnostic. Its full gate completes correct .NET builds and a
graceful 0.768-second stop, but five performance ratios fail; the prior harness
therefore did not start the peer phase. See the
[complete result](../../../test-evidence/wave-6/fixed-driver-sandbox-20261024.json).

A later host-checked candidate also rejects directory mounts that cover the
store or overlap its scratch/object storage, resolving existing reparse points
before attachment. Mounting over an empty scratch directory would otherwise
redirect the backend's next file open into its own dispatcher. Callback refusal
alone cannot protect the interval between that open and forced termination.
Ordinary separate mount directories, including a sibling directory under the
state root, remain valid. Two additional VM negatives (direct scratch and a
junction alias) require the refusal marker before the after-create fault seam;
they cannot accidentally attach a mount point when the guard regresses. This
extends the candidate's matrix to 44 cases. All 44 pass on `eff2afbc...`;
maximum forced exit is 0.011 s, ordinary graceful exit 0.148 s.

The observed full-gate metadata counters are dominated by `sqlite_batch`, with
no map copies in the sampled workload. The next candidate splits that inclusive
scope into commit, begin, savepoint, release, sync-mode, rollback and other SQL.
The [three-run diagnostic](../../../test-evidence/wave-6/transaction-split-sandbox-20261024.json)
attributes 25.309 of 25.499 batch seconds to COMMIT, with no map copies. The
next read-only diagnostic separates NORMAL/FULL commits before changing any
policy. After waiting for the receiver to reach the source's committed journal
cut, the paired diagnostic measures 87.39 ms remote visibility; both daemons
stop gracefully. Copy ratios and peer overhead still fail. These diagnostics
do not change transaction, caching or durability policy.

The full benchmark must also complete correct incremental builds and the peer
phase with clean bounded teardown. Passing short stress does not prove absence
of kernel races. The blocker remains open until every required check passes.

Large dumps and the complete store export remain local under
`test-runs/mount-sandbox/cache-output/lazy-full-gate`; the versioned
[failure report](../../../test-evidence/wave-6/lazy-notify-hang-20261024.json)
retains the resolved stacks, process states, workload reports and hashes.
