# WinFsp source review: keep, fork, or use the raw constructs

Written 2026-10-26 against the upstream clone at `..\winfsp`, commit
`4425d480` (`v2.2B5-1`, the tag the pinned Beta5 manifest in
[`native/winfsp_driver_manifest.h`](../../../native/winfsp_driver_manifest.h)
was built from), and against sections 1–9 of
[perf-gate-reassessment.md](perf-gate-reassessment.md). It changes no code,
policy or acceptance target. It answers one question: given the measured
memory-filesystem floor, should Telekinesis keep using WinFsp as shipped,
fork and embed a modified WinFsp, or bypass it for the underlying constructs.

## 1. What the driver does with each measured workload

WinFsp is a 22,769-line kernel file system driver (`src/sys`) plus a
10,126-line user-mode DLL (`src/dll`). Telekinesis uses the DLL through the
228-line shim [`native/mount.c`](../../../native/mount.c) and the delay-load
binder in [`native/winfsp_loader.c`](../../../native/winfsp_loader.c). The
design points below are the ones that bound the five failing ratios.

**One spin-lock per volume.** Every volume owns one `FSP_IOQ`
([`src/sys/ioq.c`](../../../../winfsp/src/sys/ioq.c)): a pending list, a
processing hash table and a retried list, all guarded by a single
`KSPIN_LOCK` (`driver.h:981`), with waiters on a queued event
(`driver.h:973`). Every `FSP_FSCTL_TRANSACT` call from a dispatcher thread
runs `FspVolumeFastTransact` (`volume.c:1047`), which under that one lock
completes the responses it carries, drains retried IRPs, then dequeues the
next pending IRP (one for `TRANSACT`, up to buffer-full for
`TRANSACT_BATCH`). The 48.5–50.0% spin-lock weight sampled at 16 workers in
[dispatcher-workers.md](dispatcher-workers.md) is this lock. The batch and
split-queue transports could not move it because every receive and every
completion still takes it; the two-worker result that lowered it to 5.6%
regressed data workloads because two threads also serialize the byte path.

**Every Create reaches user mode.** `FspFsvolCreateNoLock` builds a Create
request and returns `FSP_STATUS_IOQ_POST` (`create.c:740`) for every
`IRP_MJ_CREATE` the driver cannot reject outright. The DLL then runs an
access check through `GetSecurityByName` and the `Open` or `Create`
callback (`dll/fsop.c:431–506`), which is why each Git lstat produces one
Open and one security callback. The only kernel-side short cut is
`FspFastIoQueryOpen` (`fileinfo.c:2451`): it needs `AllowOpenInKernelMode`,
a non-relative open, a live file node with unexpired cached file
information, and a descriptor in the driver's 100-entry security cache
(`driver.h:1161`). File nodes are released at last close, so a 50,000-file
scan with no retained handles has nothing to hit; the 32-handle positive
control and the 50,000-file miss in
[cleanup-cache-cost.md](cleanup-cache-cost.md#cached-attribute-query-isolation)
are exactly this design. The 51,096 Opens per Git run are not a Telekinesis
cost and not a DLL cost.

**Cached data stays in the kernel; paging I/O is marshalled per transfer.**
Cached reads and writes are served by the Cache Manager through fast I/O
without a user-mode round trip (`read.c:60`, `write.c:61`). Paging I/O
(lazy writer, read-ahead, memory-mapped faults, non-cached handles) goes to
user mode with the IRP's MDL mapped into the file system process
(`FspMapLockedPagesInUserMode`, `util.c:670`), so the user-mode side copies
once but the driver pays a map and unmap per transfer. Transfer size is the
Cache Manager's own clustering; WinFsp sets no read-ahead granularity. This
is the sequential-write floor: the memory reference in section 9 writes
1 GiB in 2.0 s against 0.5 s native in both cache policies, with no
persistence at all. Nothing in user mode can change it.

**Cleanup flushes before it decides whether to post.** `FspFsvolCleanup`
(`cleanup.c`) calls `FspFileNodeCleanupFlush` (`file.c:838`), which with
`FlushAndPurgeOnCleanup` flushes and purges the data section on the last
handle, and only then checks `PostCleanupWhenModifiedOnly`. This is the
source of the ~9,400 Write callbacks inside the random-overwrite window and
of the purged pages behind the sequential-read miss (section 1). With the
flag off the same reference measures random overwrite at 1.1x native and
read at 1.04x, which is why those two targets are reachable without any
driver change once the delivery contract below is settled.

**Notify is asynchronous and reports nothing back.** `FSP_FSCTL_NOTIFY`
copies the payload, queues a `DelayedWorkQueue` work item and returns
success (`volume.c:1361–1433`). The worker looks each name up in the
context table, flushes the data section, purges only if the flush
succeeded, invalidates the metadata caches and raises the directory
notification (`file.c:2374–2395`). The flush `IoStatus` is dropped. A file
whose node is no longer in the context table is skipped. This confirms the
section 8 completion and error gaps as a protocol property of the pinned
build, not a scheduling accident.

**Process death tears the volume down.** The volume is a handle; closing it
runs `FspVolumeDelete` (`volume.c:422`), which stops the queue before
taking the delete resource (the issue #682 fix) and forces active sections
closed. That property is what the 44-case lifecycle matrix relies on, and
it is why the no-self-handle rule exists: the driver cannot complete a
request from a dispatcher that is in the process of dying.

## 2. The floor in those terms

Section 9 measured the pinned upstream memory file system, with the
capacity adapter, between two native controls. Against the section 8
targets:

| Workload | Memory floor, best policy | Target | Bound by |
|---|---:|---|---|
| Copy 2,000 files | 3.0x native | 1.3x | Four kernel/user transitions per file (Create, Write, Cleanup, Close) |
| Sequential 1 GiB write | 4.0x | 1.2x | Per-transfer mapping of paging writes |
| Sequential 1 GiB read | 1.04x | 1.2x | Reachable with purge off |
| Random 4 KiB overwrite | 1.1x | 1.5x | Reachable with purge off |
| `git status`, 50,000 files | 4.9x (2.05x with `core.fscache`) | 1.3x | One Create round trip per lstat |

The three that fail are bounded by the driver's request model, not by the
DLL, the shim or the Rust engine. The transport experiments in
[dispatcher-workers.md](dispatcher-workers.md) already showed that changing
the user-mode side of the protocol does not move Git. Any decision that keeps
WinFsp's request model keeps this floor; a decision that wants the literal
native-ratio targets for copy, write and Git has to move materialized file
I/O out of the user-mode request path altogether.

## 3. Option A: keep WinFsp as shipped

What it gives: random overwrite and sequential read inside target once the
section 8 delivery contract is resolved; Git at 2.05x with the client cache
on; copy, write and Git-without-fscache at the floor above. Choosing A is
choosing the section 5 target rewrite for those three workloads.

What it costs: nothing in kernel work. The dependency risk is upstream's
cadence. Commits per year are 12 (2024), 22 (2025) and 50 (2026); 2.2 is
still a beta, Beta5 is 13 days old, and its notes list seven vulnerability
fixes plus the three deadlocks this project hit (PR #669, issues #680,
#682, PR #677). Fixes arrived within weeks of the reports, and three of the
2026 changes are contributor pull requests (#614, #646, #648, #669), so
upstream accepts outside fixes. The hash pin and loader stay as they are.

"Embed" does not require "modify". The official installer already installs
into `SxS\sxs.<InstanceID>\bin` with a per-instance `WinFsp+<ident>` driver
service (`Changelog.md:154`, `installer/Product.wxs:62–82`), and the loader
already binds to one selected SxS service. Shipping a private copy of the
unmodified official build is permission 2 of the FLOSS exception in
`License.txt`; Telekinesis is GPL-3.0-or-later, so plain GPLv3 also covers
it. That would change the stance recorded in
[THIRD-PARTY-NOTICES.md](../../../THIRD-PARTY-NOTICES.md), which currently
says the app does not install WinFsp, and it would bring driver installation
(an elevated MSI) into the product's install path.

## 4. Option B: fork the driver, modify it, embed it

What a fork could change, against each measured cost:

- **Delivery acknowledgement (the section 8 gap).** A synchronous control
  code that flushes one named file's data section and returns the flush
  status is small: the lookup, flush and status already exist in
  `file.c:2374`, and the obstacle is only that `FspVolumeNotify` must be
  asynchronous because it can be called from inside a callback that holds
  the same locks (comment at `volume.c:1367`). A control code that the
  sealer thread calls outside any callback has no such constraint. This is
  the one change with a clear correctness payoff. It is also small enough to
  propose upstream as a pull request instead of carrying a fork.
- **Queue lock.** Splitting the pending and processing locks, or sharding
  the pending queue, is medium kernel work. The two-worker result bounds the
  Git gain at roughly 20%; data workloads showed about 0.1% lock weight, so
  they would not benefit. It leaves Git at about 4x.
- **Kernel-side open fast path.** Turning the 50,000 Opens into kernel hits
  means keeping file nodes and security descriptors alive after last close
  with name-keyed invalidation from notify: a new kernel cache subsystem.
  It reproduces, in kernel mode, what `core.fscache=true` already does in
  the client at 2.05x. Weeks of kernel work plus Driver Verifier, IfsTest
  and the upstream suites, for a workload the client can already fix.
- **Transfer granularity.** Read-ahead can be raised; lazy-writer clustering
  is the Cache Manager's and is not a file system choice. The 4x write floor
  does not move.
- **Per-file transitions (copy).** No driver change removes them; they are
  the request model.

What a fork costs, independent of what it changes:

- **Signing.** Official builds are attestation-signed through the Hardware
  Dev Center (`tools/make-release.ps1:334`, `testHarness = "attestation"`),
  which needs an organisation EV code-signing certificate, Partner Center
  enrolment and a submission per kernel build. Without that, the driver
  runs only on test-signed machines. The current isolated guest cannot
  even query its boot store
  ([driver-options evidence](../../../test-evidence/wave-6/driver-options-sandbox-20261026.json)),
  so a forked driver cannot be qualified in the existing harness at all; it
  needs a real VM with test signing before the first measurement.
- **Rebranding.** A fork must change both device class GUIDs and the
  installer GUIDs ([WinFsp-Rebranding.asciidoc](../../../../winfsp/doc/WinFsp-Rebranding.asciidoc),
  `build.version.props`) or it collides with official WinFsp on users'
  machines. GPLv3 permits the fork for a GPL project; the FLOSS exception
  no longer applies to a modified driver, and the WinFsp copyright notice
  requirement remains.
- **Maintenance.** The fork must track every upstream security and deadlock
  fix (seven vulnerability fixes in this one beta series) and reproduce the
  upstream regime for each kernel change: winfsp-tests, winfstest, HLK
  IfsTest, FSX, fscrash and Driver Verifier
  ([WinFsp-Testing.asciidoc](../../../../winfsp/doc/WinFsp-Testing.asciidoc)).
  None of that tooling exists in this repository today.

Net: B buys the delivery acknowledgement and perhaps 20% on Git, at the cost
of owning a signed kernel driver. It does not reach the copy, write or Git
targets.

## 5. Option C: "the raw underlying constructs" has three readings

**C1. Speak the driver's protocol directly and drop the DLL.** This is what
[`src/fuse_linux.rs`](../../../src/fuse_linux.rs) does on Linux with
`/dev/fuse`. The Windows equivalent is `FSP_FSCTL_TRANSACT` on the volume
handle; the wire format is public in `inc/winfsp/fsctl.h`, the kernel-mode
file system document describes the request kinds, and the batch and split
transports in [dispatcher-workers.md](dispatcher-workers.md) already drove
the raw transact call with a private dispatcher. Replacing the DLL means
reimplementing volume creation, the mount point, the access check
(`dll/security.c`), directory buffering and the operation dispatch, roughly
two to three thousand lines. It yields control over threading and removes
the delay-load binding machinery. It does not change the floor, because the
floor is on the kernel side of that call, and the transport experiments
already measured that. Worth doing only if the binding complexity itself
becomes the problem.

**C2. Write a file system driver from NT primitives.** Not viable. It is the
22,769 lines above plus ten years of fixes, NTFS-compatibility testing
against Microsoft's IfsTest, and every signing and maintenance cost of
option B, with none of the existing hardening. The upstream design document
puts the saving WinFsp provides at six to twelve months for an experienced
kernel developer; this project has none of the test infrastructure.

**C3. Use the in-box Microsoft primitives that keep file I/O on NTFS.** Two
exist, both already signed and shipped by Microsoft:

- *Projected File System* (`ProjectedFSLib`, section 6 of the reassessment).
  A placeholder becomes a real NTFS file on first write; afterwards open,
  stat, read, write, mmap and close are NTFS and the provider is not
  called. The provider answers enumeration, placeholder info and get-file-data
  for unhydrated items and receives notifications, including file closed
  with modification, which is seal-on-close. Needs the `Client-ProjFS`
  optional feature (absent on the development host: `ProjectedFSLib.dll` is
  not installed, and the guest reports it `DisabledWithPayloadRemoved`),
  an NTFS directory root, no drive letter, and case-insensitive names.
- *Cloud Files API* (`cfapi.h`, `cldapi.dll`, `cldflt.sys`), the filter
  behind OneDrive Files On-Demand. It is present on the development host
  without any feature switch (`cldflt.sys` 10.0.26100.9549). It is built for
  sync engines: placeholders on NTFS, full or partial hydration on demand,
  dehydration, pin and in-sync state, on-demand directory population, open
  and close completion, rename and delete notifications, and Explorer
  status integration. Materialized file I/O is NTFS, as with ProjFS.

For both, copy, sequential write, random overwrite, Git and builds run on
NTFS for materialized files, which is the only route to the literal
section 8 targets. The costs are the ones section 6 lists, plus: the working
layer is rewritten so NTFS files replace scratch files; the namespace is
physical (placeholders on disk), so snapshot fork isolation and the
projection model need a different mechanism; placeholders are reparse
points that some tools treat specially; and the provider-exit semantics,
callback cancellation and the exemption of the provider's own I/O still
need VM proof, exactly as the reassessment says. Both SDK headers are on the
development host (`Windows Kits\10\Include\10.0.26100.0\um`).

## 6. Recommendation

1. **Keep WinFsp as shipped for the P0 closure.** Do not fork. The floor is
   measured, it is the request model, and no fork within reach changes copy
   or sequential write. Accept the section 5 floor-relative targets for
   copy, write and Git, with native ratios still reported.
2. **Resolve the section 8 delivery gap without a kernel change if
   possible; otherwise propose the flush-by-name control code upstream** as
   a pull request against `volume.c`/`file.c`, so the project keeps running
   the official signed driver. Only if upstream declines does a private
   build become a question, and then it is a one-patch SxS build, not a
   product fork.
3. **Open the C3 question as its own wave-sized decision**, as section 6
   already frames it, with the Cloud Files API evaluated beside ProjFS
   because it needs no optional feature and matches the sync model. It is
   the only option that reaches native-ratio targets, and it is also the
   only one that removes the user-mode request path from the lifecycle
   blocker's failure surface.
4. **Do not pursue C1 or C2.** C1 is maintainability work with no measured
   performance return; C2 is not a realistic project.

What this note does not do: it does not qualify any cache-policy change,
does not relax any acceptance ratio, and does not establish ProjFS or Cloud
Files semantics beyond public documentation and host presence checks. The
lifecycle matrix remains the P0 gate.
