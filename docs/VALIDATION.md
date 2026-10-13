# Validation

## Live mount failure and sync visibility (2026-10-12)

The remote Docker worker repeatedly exited with `MOUNT_PATH_MUST_NOT_EXIST` while
all four outbound lanes remained connected with no repository exchanges. Read-only
SQLite snapshots found 1,593 local shared events versus 920 remote events, no
quarantines or missing prerequisites absent from both peers, and 102 missing
objects referenced by the remote's already accepted metadata. This was a different
failure from the earlier replica's missing history.

Stopped the failed worker's retry loop, verified that `/app/mounts/Projetos` was an
empty ordinary directory and not a mount or symlink, removed it with nonrecursive
`rmdir`, then started the same worker state. No backing objects or metadata were
removed. The mount succeeded and all three bulk lanes resumed. A later snapshot
contained 1,027 accepted events, 367 pending events and all 239 objects referenced
by accepted shared history. Metadata was still catching up; full convergence was
not claimed. Reports are under `test-evidence/live-sync-*.json` (local, ignored).

The desktop now separates synchronization details from conflict review, displays
current and shared-history availability, metadata backlogs, per-lane object
inventories, transfer totals/timings and observation ages. Folder rows distinguish
remote unavailability from network connection health. The API includes the last
advertised remote-folder availability and per-repository lane observations.
Four Windows GUI unit tests and all 15 orchestrator integration tests passed,
including a connected peer whose worker stops and disappears from discovery.
All-target checking, strict Clippy and formatting passed. These UI/API source
changes have not been deployed; physical desktop interaction was not validated.
The independent-mount design in `ARCHITECTURE.md` is a study, not an implemented
lifecycle refactor.

## Parallel object lanes and binary gzip transport (2026-10-12)

The full Windows Cargo suite passed 128 tests, with one opt-in codec benchmark
ignored by the normal suite. That benchmark was run separately in release mode
and passed. The Ubuntu/WSL release run passed 53 tests across the library,
`bulk_lanes`, `runtime_lanes`, `paired_sync`, `partial_content` and `orchestrator`
targets, with the same benchmark ignored. `/dev/fuse` was available for the real
mount cases. All-target checking, strict Clippy, formatting and diff whitespace
checks passed on Windows. Windows and Linux release binaries were built.

The new regressions verify concurrent transfer of different objects from one
repository, progress while another object's lock and the engine lock are held,
control-only metadata acknowledgements, bounded activation with durable pending
events, lane-pool changes and overlapping partial-file recovery. Codec tests cover
binary/gzip round trips, malformed and oversized frames, truncated streams, legacy
JSON fallback, and negotiated binary messages over WSS.

An isolated Windows release comparison transferred three 4 MiB objects through
two daemons and independently verified their bytes. The installed baseline took
10.311 seconds; the new build took 4.352 seconds (about 2.37 times faster). Each
of the three bulk lanes received exactly 4 MiB; the control lane received zero
object bytes. This is one local loopback comparison, not a WAN bandwidth or
sustained throughput qualification. Objects are partitioned across lanes; a
single large object is not striped across connections.

The release codec fixture reduced 1,110,000 bytes of repetitive text to a
7,843-byte binary/gzip message. A 1,048,576-byte seeded random fixture used a raw
binary message of 1,048,719 bytes, avoiding gzip expansion and hex wire overhead.
Full envelope encode/decode timings were respectively 5.69/3.76 ms for the text
fixture and 9.01/3.12 ms for random data. These include JSON/hex conversion and
allocation, not just gzip. Compression is adaptive and has measurable CPU cost;
the repetitive fixture does not predict compression ratios for project files.

Local ignored evidence is in `test-evidence/parallel-gzip-windows-tests.log`,
`parallel-gzip-linux-tests.log`, `parallel-gzip-codec-benchmark.log` and
`parallel-gzip-transport-benchmark.json`. Release artifacts are
`target/release/tkfs.exe` and `target/runnables/linux-x86_64/tkfs`. Neither the
installed local application nor the remote Docker service was replaced or
restarted by this validation. The missing-history findings below describe an
earlier replica; the user subsequently recreated the repositories, so those
counts must not be used to diagnose the current live state.

## Worker transport pacing (2026-10-12)

The full Windows suite passed 118 tests. After final connection/validation cleanup,
the 14 orchestrator tests and 19 paired-sync, partial-content and worker-lane tests
passed again. Coverage includes control delivery during a blocked bulk transfer,
persistent worker requests after application errors, no automatic replay after
an uncertain disconnect, and a multi-page transfer with a five-second idle poll
interval. Strict Clippy and release builds pass.

An isolated two-daemon release benchmark transferred and independently verified
an 8 MiB object at the default 500 ms polling interval: 8.08 seconds using the
previously installed Windows binary, 3.99 seconds with persistent worker lanes,
reduced repeated availability scans, and a 10 ms yield during active content.
This single loopback comparison is not a WAN throughput qualification. Evidence:
`test-evidence/transport-benchmark.json` (local, ignored).

Read-only inspection of the live Projetos peers found matching 844-event accepted
journals and projections, but 1,273 pending remote events requiring 1,018 event IDs
absent from both samples. Both replicas lacked the same 51 content objects.
Remote ping median was 11.86 ms; no object download was active. The receiving mount
remained unpublished because its initial metadata readiness condition was not met.
Increasing connections cannot recover missing history or content. Live binaries
were not replaced by this validation.

The folder-first rewrite was checked on Windows and Ubuntu/WSL on 2026-10-11.
The reboot results below describe commit `26ab12d`; the outbound WSS follow-up
has separate results at the end.

```sh
cargo check --locked --lib --bin tkfs
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
python scripts/orchestrator_e2e.py --exe target/debug/tkfs.exe
```

The full Windows suite passes all 74 tests, with no failures or ignored tests.
On Linux, select the Linux executable for the Python harness. The targeted Linux
run covers the library, `orchestrator` and `paired_sync` test targets (12 tests).
Strict Clippy checks pass on both platforms, including the older Rust 1.88 Linux
toolchain. The public CLI harness passes on Windows and Linux using four persistent
WS lanes and bidirectional sync between two headless orchestrators.

The six process integration tests exercise:

- Independent orchestrator/worker processes, two repositories, three persistent
  WS lanes, bidirectional sync with only one side dialing, and restart.
- Server-persisted salted verifier, code-only pairing, wrong-code refusal, code
  rotation invalidating existing sessions, re-pairing and local pool removal.
- A/B/C relay with events from three origins and private branches remaining local.
- Real WinFsp and FUSE mounts: filesystem write/read, cooperative stop, changed
  mount path, and remount with the original bytes intact. The FUSE test skips
  hosts without `/dev/fuse`; it ran with a real FUSE device in this validation.
- Version 2 configuration defaults and rejection of removed credential fields.
- A 6 MiB file transferred through bounded pages after a daemon crash, recovery
  of a surviving worker, and receiving-daemon restart with the same verifier.

Linux integration fixtures use ordinary group/world-writable parent directories,
confirming that the daemon does not impose private storage modes.

Retained tests cover causal conflicts, branches, snapshots, basic/POSIX metadata,
atomic save/recovery boundaries, acknowledgement correctness, object segmentation,
large files, bounded replication pages and 10,000 durable saves. Removed tests
were specific to encrypted transport, OPAQUE, credential stores and the previous
supervisor/desktop contracts. The new process and public CLI suites exercise the
replacement architecture.

The Windows Slint desktop compiles against the new management API. Physical UI
interaction, real separate-machine networking, operator reverse proxies and
unattended installed services are not qualified by these local tests. Test data
is disposable; no user data/configuration is migrated or deployed by validation.

## Outbound WSS follow-up

On 2026-10-11, `cargo test --locked` passed all 80 Windows tests with no failures
or ignored tests. Formatting and strict Clippy checks passed, and the public CLI
harness passed its four-lane bidirectional WS scenario.

The new transport tests cover WS/WSS endpoint validation, persistent WSS messages,
preserved paths/query strings, rejection of an untrusted server certificate and
rejection of a hostname mismatch with a trusted test CA. The test connector trusts
the fixture CA in memory only. These tests do not install certificates or restore
the retired enrollment, credential storage or ACL mechanisms.

The follow-up Ubuntu/WSL run (`cargo test --locked --lib --test orchestrator
--test paired_sync --target-dir target/linux-review`) stopped during the OpenSSL
dependency build because `pkg-config` was unavailable. It did not run the Linux
tests; install the build prerequisites in the [Linux guide](LINUX.md) before
repeating it. The earlier reboot's Linux results do not qualify the WSS change.

## Independent metadata and content availability

Validated on Windows and Ubuntu/WSL on 2026-10-12. The full Windows suite passed
99 tests during implementation. After adding two further regressions and the
final mount/cursor fixes, the Windows `orchestrator`, `paired_sync` and
`partial_content` targets passed all 27 tests. The final Linux `paired_sync`
and `partial_content` targets passed all 15 tests. An earlier Linux run passed
35 tests across the library and those three integration targets.

The new real-mount regression passed on both WinFsp and FUSE: a replica mounts
with absent content, exposes its namespace and file size, waits for a remote
read while creating an unrelated directory, and completes the read after the
source reconnects. Other coverage includes metadata acknowledgements without
bytes, three-peer relay, restart with missing/corrupt objects, bounded independent
content transfers, resumed partial cursors, read deadlines, control RPCs during
bulk installation, and stale conflict-review rejection.

Formatting, diff whitespace checks and strict all-target Clippy passed on both
platforms (Linux Rust 1.88). The Windows public CLI orchestrator harness passed
four-lane bidirectional WS synchronization. The Windows Slint desktop compiles;
physical interaction with its new conflict/status panel has not been qualified.

The additional Git compatibility harness stopped at its first `git status`
because Git rejected the mounted directory's Everyone ownership as dubious.
Its remaining scenarios therefore have no result from this run. No user/global
Git trust configuration was changed. These tests use disposable repositories
and do not update the separately installed application.

Performance improvements remove redundant passes and move bulk receipt/read
verification outside the engine mutex. No comparative throughput benchmark or
parallel hashing implementation is claimed by this validation.

## Startup snapshot validation

`cargo test --locked --test startup_snapshots` exercises suffix replay against the
full reducer, historical conflict reviews, damaged/incompatible snapshot fallback,
multiple branches, unavailable content, outer transaction rollback, monotonic
journal cursors and process crashes during replay or snapshot replacement. Existing
recovery/replication tests also cover staged saves, out-of-order incoming events and
legacy journal evacuation with startup snapshots enabled.

For an existing state, build with `cargo build --locked --release`, then run:

```powershell
python scripts/benchmark_startup.py --state STATE_DIRECTORY --exe target/release/tkfs.exe --baseline-exe OLD_TKFS_EXE
```

The harness reads the source through SQLite's backup API, copies content/staging
into an isolated fixture, and runs real mount workers without an orchestrator or
remote connection. It records baseline mount time, first-open migration, three
snapshot remounts, and a remount after writing a file to produce a real event suffix.
It compares canonical projections, historical conflicts, frontiers, events, outbox
and peer acknowledgements across opens. Timings include process launch through the
mounted worker's ready response; OS caches are not flushed. The JSON report includes
binary hashes and is written to `test-evidence/startup-benchmark.json`. Add
`--keep-artifacts` to retain the copied state and worker logs.

On 2026-10-12, a Windows release-build benchmark of a copied state with 4,798
events, 530 current objects and 554 stored objects measured 120.161 seconds for
the previously installed executable's mount. The final build's first open took
102.808 seconds to reconstruct and seed its snapshot. Three subsequent real
WinFsp mounts took 333.772, 333.716 and 308.837 ms (333.716 ms median). A remount
replaying two events added through the mounted filesystem took 409.978 ms. All
semantic and acknowledgement comparisons passed. These are local warm-cache
measurements, not cold-disk guarantees. Reports are in
`test-evidence/startup-benchmark.json` and `startup-benchmark-final.json`.

The full Windows Cargo suite passed, followed by the final targeted recovery,
replication and eight snapshot tests. Strict all-target Clippy and formatting
checks passed. No separately installed executable or original state was replaced.

## Visual Studio directory-cleanup stall

Investigated on Windows on 2026-10-12 using an isolated SQLite backup and object
copy of state `614d6b81-4ef5-4e56-bfbc-0c628849a4a7` (6,405 events and 2,154
entries). The source state and installed executable were not modified.

The installed binary passed 3,480 hash-verified mounted reads over the solution
files, in groups of 870 with 1, 8, 32 and 64 readers. A single empty-directory
deletion then stalled. Opening the copied EnroSistemas solution in Visual Studio
with the pre-fix release build reproduced the stall. Debugger stacks showed
`Engine::cleanup` / `Store::delete` inside history reconstruction, with another
WinFsp thread waiting in `FspFileSystemOpEnter` for its shared namespace lock.
The worker consumed about 95 seconds of CPU before this isolated reproduction
was terminated. This establishes a long computation holding the engine and
namespace locks; it does not establish a circular read deadlock.
This matches WinFsp's documented exclusive namespace guard for delete cleanup
([WinFsp operation guards](https://winfsp.dev/doc/WinFsp-API-winfsp.h/#fsp_file_system_operation_guard_strategy)).

The directory successor fix allows sequential, conflict-free directory moves
and empty-directory deletions after the complete frontier to update the current
projection without reconstructing historical cuts. It validates ancestry and
empty-directory constraints; cycles, deletion of parents with live children,
reviews and competing history still use the full reducer.

On the same copied state the fixed release build completed the directory rename
in 28.273 ms and deletion in 19.753 ms. Another 870 hash-verified reads with 64
readers completed in 0.819 seconds. Visual Studio subsequently opened the same
solution, and its DTE reported `Solution.IsOpen=true` and two projects. The worker
successfully committed deletion of `.vs/EnroSistemas/FileContentIndex/merges`,
reported complete content with no health error, and stayed responsive. Visual
Studio closed through DTE normally. These tests used local content without a
remote peer and did not validate build/debug operations in the IDE.
Two contexts remained open after the IDE exited, so cooperative worker stop
returned `BUSY_VIEW`. Only the isolated test worker was then terminated, after
confirming zero unflushed files and no health error.

`cargo test --locked --tests` passed all 112 tests. The three new
`directory_progress` regressions cover descendant identities and restart/full
recovery equivalence, conflict retention for unsafe direct directory commits,
and bounded directory cleanup with 2,000 unrelated historical events. Formatting
and diff whitespace checks passed. Local debugger traces and measurements are
under `test-runs/hang-investigation`; the corrected binary is
`target/release/tkfs.exe`, not the separately installed application.
