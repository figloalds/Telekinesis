# Validation

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
