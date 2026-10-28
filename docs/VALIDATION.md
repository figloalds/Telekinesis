# Validation

```powershell
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
python scripts/mount_lifecycle.py
python -m unittest discover -s scripts -p 'test_*.py'
```

Mounted runs belong in an isolated Windows VM until
[W6-P0-MOUNT-LIFECYCLE](revamp/WAVE_6_MOUNT_LIFECYCLE_BLOCKER.md) closes.
Windows Sandbox provides a separate kernel and is suitable for the lifecycle
matrix. Install the exact WinFsp DLL/SYS build allowed by the native preflight
described in the blocker, and copy the executable and scripts to
its local disk; the host installation is not inherited. Keep state, scratch,
mount points and benchmark trees off mapped host folders. Export reports/logs
before closing Sandbox, which deletes its local state. A persistent VM is
preferable for retained kernel dumps and stable benchmark configuration.

Inside the isolated guest, build/copy the current release binary, then run:

```powershell
python scripts/mount_acceptance.py --isolated-vm --binary target/release/tkfs.exe
```

The matrix checks drive-letter and directory mounts, idle/cached-write/fsync/
paired-delete-recreate graceful and forced exits, save/restart, startup unwind
at each native attach stage, and failed shutdown with recoverable scratch.
It probes mount removal, fresh PowerShell drive initialization and unrelated
native I/O in bounded children. It stops at the first failure and retains
`acceptance.json`, logs, stores and exit records under its `--home` directory.
`--report <path>` mirrors the incremental report to an export folder. Verify
the report's binary SHA-256 matches the release under qualification. No report
from a failed driver setup or a partial matrix establishes acceptance.

The matrix now has 44 cases: each mount kind also recovers dirty scratch with
its timestamp set exactly to the saved marker. Fault cases preserve the full
pre-recovery store before the offline reopen can consume scratch, and report
the recovered byte length/hash. Equal or backwards clocks cannot prove that
scratch bytes are unchanged; recovery checks the materialized content too.

`scripts/mount_lifecycle.py` bounds every daemon stop (20 s graceful, 10 s
after kill) and writes `*-exit-state.json` next to each daemon log; a daemon
that survives the kill fails the run. The stop RPC and exit wait share the
20-second deadline. A failed RPC still gets the remaining exit time before a
kill; a fallback kill or nonzero exit fails ordinary graceful qualification.
Benchmark stores survive failures under `test-runs/bench` for recovery.
Timed-out Windows commands terminate their launcher and descendant processes
within one shared 10-second kill budget. A timeout remains a failed run; a
launcher exiting while its child's output pipe stays open cannot count as
successful cleanup. The helper self-test includes this non-mounted regression.
`replacement_smoke.py --forced-kill`
adds the blocker's forced-exit check, and the smoke fails on any logged
`mount I/O operation` error. `tkfs rpc inject-fault '{"name":"flush"}'`
arms a one-shot seal failure. `flush-persistent` fails final retries as well,
so the lifecycle matrix verifies restart recovery after a failed seal.
`TKFS_TEST_MOUNT_FAIL=after-create|after-mount|after-dispatcher` exercises the
native unwind stages in the VM; these fault seams must be unset in normal runs.
`TKFS_TEST_MOUNT_SELF_OPEN=1` additionally checks that self-opens are refused
before any Rust lookup, even while the requesting thread holds the store lock.
Only the isolated VM matrix enables this negative test.

The trimmed suite retains transport framing/authentication/TLS tests and exact
filesystem invariants: receipt-based exactly-once sealing, recovery of unsealed
scratch files, changed-chunk reuse, snapshot fork isolation, concurrent entity
heads and explicit resolution, pinned pack GC, range hydration with missing
objects, and remote invalidation. Additional regressions cover local metadata
changes during an edit and temporary-file replacement of a stable target entity.
File-fsync regressions also cover entity-only draft selection, retargeted drafts
flushed through a retained reader, and a reader-blocked SQLite checkpoint after
later namespace, catalog or schema writes. A completed FULL-synchronous seal
remains durable in the WAL without requiring a second checkpoint.

The real-mount smoke checks both mounted bytes and committed bytes, then pairs
two peers and updates an already-cached file. It also stops both peers within
the shutdown bounds, makes offline content/rename edits, reconnects them, and
resolves the conflict through cached mounted names. The old name must disappear,
the selected bytes must be visible, and the pending local edit must remain in
history without reopening the conflict. Earlier logs and exit records are retained
across that restart. It runs on Windows and Linux;
use `--binary` and `--home` to select the executable and disposable test directory.
Use `--mount-a` and `--mount-b` for free drive letters or nonexistent directory
mount points. Explicit file `fsync` drains an open writer's Windows cache; a
control RPC can seal only bytes already delivered to the server's scratch.

## Performance gate

Per the user's 2026-10-28 instruction, complete correctness/implementation and
isolated lifecycle qualification first; run this performance gate last. P0 and
cutover stay open until the complete gate passes. The scheduling change does
not change thresholds or cache/durability/access policy.

Only `scripts/bench.py` defines the performance gate. It runs one warm-up and
five samples and reports medians for small-file copy, .NET clean/no-op builds,
1 GiB sequential write/read, 10,000 random 4 KiB overwrites, git status on a
50,000-file tree, and cached remote visibility after local apply.

```powershell
python scripts/bench.py --label native
python scripts/bench.py --label mounted --root X:/bench --compare test-evidence/bench/native.json
# After measuring the pinned memory filesystem in this same guest/session:
python scripts/bench.py --label wave6 --matrix-tkfs target/release/tkfs.exe --matrix-reference test-evidence/bench/memory-reference.json --matrix-cache-policy purge
```

The matrix uses disposable stores on X: and Y: by default. `--matrix-mount` and
`--matrix-peer-mount` choose other free letters. It measures native, mounted,
and mounted with one paired peer, using identical workload dimensions.
Reports are under `test-evidence/bench/`. An exceeded target, failed workload or
incompatible baseline fails the command. Partial `--only` runs are diagnostic;
they do not establish completion of the full gate.

Copy, sequential write and Git use the same-session pinned memory-filesystem
reference with matching cleanup policy; all native ratios are retained. Git
runs with `core.fscache=true` in native/reference/candidate phases. See
[section 8](revamp/WAVE_6_REPLACEMENT.md#8-the-benchmark-gate) for the complete
target table and reference requirements. A reference from an earlier guest or
client configuration cannot establish acceptance.

Before the idle-peer phase, the matrix waits for the source's captured metadata
head and all current active file roots/chunks in the receiver's committed
native object index. `--peer-catchup-timeout` bounds the whole initial import;
it is not restarted after metadata arrival. The report retains metadata time,
the inventory at metadata arrival and final readiness time. This checks object
membership/kind, not independent verification of every peer payload byte. All
native read handles close before workload timing. The
[readiness diagnostic](revamp/notes/peer-payload-readiness.md) explains why a
metadata-only wait did not establish an idle peer. The native regression suite
above checks this boundary without mounting or loading a driver.

Git status uses a packed native fixture copied identically into each target,
with the tracked count checked before timing. Object creation/fsync during
`git add` is outside this read workload. Mounted preparation finishes pending
seals before the warm-up. The fixture mode is recorded and mismatched older
baselines are rejected; rerun the native baseline after this setup change.

Qualification remains open until the full mounted matrix passes. Earlier
capsules and CCR qualification scripts are not part of the replacement gate.
