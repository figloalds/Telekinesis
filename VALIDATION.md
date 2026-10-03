# Implementation validation — 2026-10-03

All commands ran from `C:\Users\felyp\Desktop\Projetos\Telekinesis` with the
existing toolchain. WinFsp was discovered pre-existing; no driver was installed or
changed. Its DLL at `C:\Program Files (x86)\WinFsp\bin\winfsp-x64.dll` reports
`2.1.25156.ddca7bd` / WinFsp 2025. Rust/Cargo were `1.96.0`, with the available
MSVC x64 compiler/Windows SDK; the OS reports `10.0.26300.0`. Python 3.12 runs
the harness using only its standard library. No external service/bucket or
persistent peer secret was created.

## Implemented deliverables

| Files | Scope |
|---|---|
| `Cargo.toml`, `Cargo.lock`, `build.rs` | Rust runtime, cached pinned dependencies, installed WinFsp SDK/native linkage |
| `src/core.rs` | SQLite WAL/FULL journal/state/outbox/receipts, immutable CAS, causal projection, conflicts, branch IDs, checkpoint history/restore, namespace recovery |
| `src/runtime.rs` | Shared staging and flush boundaries, thin RPC, durable peer inventories/inbox, authenticated TCP paging, mount notifications, privacy filtering |
| `native/mount.c`, `src/mount.rs` | Actual WinFsp callbacks, caching/notification boundary, conservative same-path checkout/remount |
| `src/main.rs` | CLI launcher and runtime RPC client; command inventory/defaults in README |
| `src/backend.rs` | Verified immutable object-backend interface and explicit native local test-bucket adapter |
| `tests/*.rs`, `scripts/e2e.py`, `scripts/watch.ps1` | Driver-independent core/fault/permutation tests plus actual Windows mounts and FileSystemWatcher |
| `README.md`, `TKFS-PLAN.md` | Build/run/pairing instructions, agreed architecture, limits and remaining decisions |

## Final commands and results

```powershell
$env:PATH = 'C:\Program Files (x86)\WinFsp\bin;' + $env:PATH
cargo build --offline
cargo test --offline
cargo fmt --check
cargo clippy --offline --all-targets -- -D warnings
python scripts\e2e.py
```

| Command | Observed result |
|---|---|
| `cargo build --offline` | Passed; `target/debug/tkfs.exe` built |
| `cargo test --offline` | Passed: 17 core + 5 additional + 6 recovery + 9 replication regression + 5 scalability tests (42 total); zero failures (two are subprocess fault workers) |
| `cargo fmt --check` | Passed |
| `cargo clippy --offline --all-targets -- -D warnings` | Passed |
| `python scripts\e2e.py` | Passed all 17 real-mount/two-local-process checks |

The full run exposed a test that assumed UUID-sorted journal order was operation
order. The corrected replacement test identifies the added event and verifies its
move and tombstone are in that single event. The subsequent full run passed.
Cargo emits an environment warning about canonicalizing the user home path;
compilation, lint and test exit codes are successful.

## Core/fault coverage and review regressions

Tests cover causality before tie-break, three contenders/all 24 orders,
duplicate delivery/restarts, exact review and unseen contenders, child-before-parent,
rename/edit, delete/edit, directory cycles, deleted parents, same-name and
file/directory collisions, atomic temp replacement, missing/corrupt bytes,
historical conflicts when correction arrives first, private canaries and dedup,
private current-state publication, long-lived writer bases, conservative checkout,
AEAD tampering, object/SQL/receipt process crashes, namespace recovery, checkpoint
restore and native test-bucket verification/privacy. Process crashes do not qualify
physical power loss.

Dedicated tests in `tests/replication_regressions.rs` close the four P1 findings:

| Finding | Evidence |
|---|---|
| Backlog larger than one encrypted frame | One atomic publication with two 16 MiB objects transfers across two bounded pages; incomplete root invisible/unacknowledged; restart preserves partial bytes and completes |
| Private CAS hash promoted by inbound manifest | Known private hash without supplied shared bytes stays staged across restart; absent from shared inventory/export and visible events |
| Receipt-order-dependent deleted-parent conflicts | Seed/delete/child/correcting rename across all 24 permutations yields identical projection and complete conflict records |
| Branch label collisions block catchup | Private/shared overlapping labels and concurrent same-label shared branches retain stable IDs; ambiguous lookup fails clearly while unrelated main events progress |
| Invalid semantic event blocks useful work | Descriptor error quarantined while unrelated valid main event commits and acknowledges |

Targeted re-review reproduced two residual failures before fixing them: a
child with an absent mismatched parent was prematurely acknowledged, and an
ordinary correction erased three-way cycle records. Focused regressions now pass:

- `malformed_deferred_child_cannot_poison_parent_local_flush_or_other_branches`:
  no ack/journal promotion for unknown causal links; valid parent later commits;
  child/descendant quarantine; successful runtime flush/read/restart/replication
  and independent branch progress.
- `three_origin_cycle_survives_ordinary_correction_in_every_delivery_order`:
  all three cycle records persist after correction, identical projection/full
  records across all 24 delivery orders and restart.
- `restart_evacuates_legacy_unvalidated_journal_before_local_writes`:
  legacy incomplete journal/frontier/outbox entries move to durable inbox;
  valid parent/new writes progress, previous bytes remain inspectable locally.
- `namespace_history_budget_refuses_commit_without_erasing_records`:
  13 concurrent moves to the same name exceed the competing-witness cap,
  refusing a new commit before ack;
  journal and existing conflicts remain identical through restart. A simulated
  legacy journal over the budget also proves startup repair/rebuild failure
  rolls back journal, outbox, inbox, current state and existing conflict records
  atomically rather than deleting records before rebuild.

The large-object regression exercises the same page/receive functions used by
TCP, without a driver dependency. Mounted TCP checks exercise the protocol between
separate processes; no claim is made that the large fixture itself ran over two
physical computers.

## Ordinary-save scalability correction and benchmark

The previous global all-cut bound also counted unchanged TRUE assertions, so long
save chains and unrelated offline chains exhausted it. The correction partitions
dependency cones of historical ancestors and same-name competitors, closed
transitively without coupling unrelated siblings sharing a folder. It excludes
content-only/no-tombstone TRUE history
from topology analysis, and retains min/max-rank witnesses per unbranched TRUE
segment and causal relationship to topology/tombstone facts. Actual events,
parents and all competing byte/existence revisions are never pruned. An exhaustive
512-subset small-DAG oracle, explicit reviewed undelete and reversed delivery
validate the compressed presence witnesses. Existing cycle/rename/delete and
all-24-order regressions still pass.

The cap remains 4,096 **competing** witness cuts (antichain width >= 2) **per
namespace dependency cone**. Single-event/linear cuts and independent
histories do not share a history budget. Ordinary save count no longer hits this
cap. Genuine topology contention still fails before acknowledgement and preserves
existing journal/conflicts, including atomic startup-repair rollback.

Other costs exposed by the requested benchmark were corrected too: causal
dependencies use the accepted frontier; sole-head content successors update state
incrementally; full register analysis skips linear chains and uses indexed
ancestry for branching history; generic append derives new/old pairs while keeping
old/old records; inbox processing waits for committed prerequisites and batches
metadata durability behind one frame acknowledgement boundary. No automatic merge
or conflict-history truncation was introduced.

Exact benchmark command:

```powershell
cargo test --offline --release --test scalability -- --nocapture --test-threads=1
```

| Case | Release observation | Final debug full-suite observation |
|---|---|---|
| 10,000 durable 10-byte content saves | 69.409 s; first 1,000 6.481 s, last 1,000 7.383 s | 74.044 s; first 1,000 8.657 s, last 1,000 6.830 s |
| Subsequent delete/explicit undelete | 0.414 s | 1.525 s |
| Restart after long history | 0.815 s | 2.116 s |
| Two replicas, 100 offline 4-byte saves each | Saves 0.814 s; reconnect 3.350 s; both restarts 0.334 s | Saves 1.057 s; reconnect 9.068 s; both restarts 1.900 s |
| Two independent 65-rename chains in one folder plus convergence | 1.794 s | 9.648 s |

The release benchmark runs one case at a time to avoid competing benchmark I/O;
the debug measurements come from the normal full-suite run.

The first correct-but-unoptimized debug reconnect took 114.640 s; deriving only
new pairs and skipping unready activation reduced a standalone debug rerun to
6.318 s while retaining all 10,000 historical content pairs. Final tests also
verify every competing object, absence of quarantined backlog, restart equality,
fast/full projection equality and backward-clock provenance behavior.

Machine-readable benchmark: [SCALABILITY.json](SCALABILITY.json). Final stdout:
[test-runs/final-tests.log](test-runs/final-tests.log) and
[test-runs/final-release-scalability.log](test-runs/final-release-scalability.log).
These benchmark core/Engine
flushes to actual native Windows SQLite/CAS with FULL/write-through durability,
small files and independent replica stores in **one process**, without the mount.
The real mounted suite is separate below. These are single-run observations,
not production throughput guarantees. Startup still verifies retained objects;
genuine content contention can create quadratically many inspectable historical
pairs (100 x 100 here), and larger files/history cost more memory, storage and I/O.

## Mounted acceptance evidence

Machine-readable result: [VALIDATION.json](VALIDATION.json). Reproducible harness:
[scripts/e2e.py](scripts/e2e.py). Its result records the exact latest fixture
directory/device UUIDs; preserved `a-<index>-stdout.log`, `a-<index>-stderr.log`
and equivalent `b-*` files live there, alongside `watch.events`/watcher logs.
Each device retains its own SQLite database/CAS. The ephemeral peer key is not
logged. The harness terminates its runtime/watcher processes after testing.

Final fixture: `test-runs/e89b43d5-3878-4c31-afb1-fe240f99a82a/`.
Devices: `21076db3-4f43-46f9-8ab9-f3dded73f8a7` and
`e07f9f38-b46c-47bb-877d-692cd8fa2bb5`. Direct evidence:
[A initial stdout](test-runs/e89b43d5-3878-4c31-afb1-fe240f99a82a/a-0-stdout.log),
[B initial stdout](test-runs/e89b43d5-3878-4c31-afb1-fe240f99a82a/b-0-stdout.log),
[watcher events](test-runs/e89b43d5-3878-4c31-afb1-fe240f99a82a/watch.events),
[final mounted stdout](test-runs/final-mounted.log).
After the run, no TKFS runtime process remained and both mount paths were absent.

Verified actual mounted I/O includes ordinary Python save/fsync/read, PowerShell
mkdir/rename, atomic `os.replace`, automatic peer visibility, FileSystemWatcher
create/change/delete (renames emit old-path deletion/new-path creation), existing
read-only handle refresh, checkpoint history
and restoration to a mounted private branch, A-B-A checkout at one path,
open-writer/memory-map refusal, private isolation across restart, shared-only test
bucket export with mount-recursion refusal, explicit current-state publication,
offline conflicting durable saves/crash/restart/convergence, inspectable bytes/base/
provenance, exact reviewed resolution, namespace alternative recovery onto both
mounts, lost acknowledgement replay, and persistent retry-safe RPC identities.

PowerShell script-file execution was blocked by the host policy. The harness
executes its trusted local watcher script body through a normal inline `-Command`;
it does not change execution policy or security settings.

Transport is AES-256-GCM authenticated/encrypted TCP, protocol version 2, between
separate durable stores bound only to loopback. This is **two local processes with
real WinFsp mounts, not real two-computer validation**. No simulated filesystem,
central ref service or cloud bucket substitutes for the mount.

## Remaining setup and acceptance gaps

Real second-machine prerequisites/runbook are in
[README.md](README.md#two-explicitly-configured-devices): another x64 Windows
computer with WinFsp, distinct device database/UUID, common repository UUID,
authorized reachable endpoints and an ephemeral out-of-band pairing key. No
driver, firewall or network-security setup was performed. Both daemon peer
endpoints/peer identity must be configured together; the secret is environment
only. Pairing assumes trusted devices; no PKI, independent signing, forward
secrecy, revocation or multi-user permissions are implemented.

The object backend is a local native test directory, with no automatic uploader.
Real provider qualification needs a selected provider/endpoint, bucket/container,
prefix/region, temporary read/write credentials and approval for test writes to
that prefix. There is no cloud adapter or provisioned infrastructure yet.

Chosen GUI editor/build compatibility, arbitrary mapped-write coherence, every
remount/disk-full boundary and physical power-loss guarantees remain unqualified.
Buffered write success alone is not durable; explicit successful flush is the
acknowledged boundary. Close-publication failure sets health/logs because Windows
cleanup cannot return an error. User-space stale buffers can hide their actual
base. Read-only refresh requires no dirty staging/local writer; watchers/handles
can block conservative checkout. Notification retries are in memory and restart
uses remount/rescan. Automatic text merge is deferred; branch merging,
cross-device checkpoint labels/history and snapshot diff remain future work.

Bounds: 16 MiB objects/files, 8 MiB generated event payloads, 64 MiB frames,
4,096 competing topology witness cuts per namespace dependency cone (width
>= 2; ordinary content saves with unchanged namespace and unrelated histories do not
share/exhaust that budget; overflow refuses a commit rather than pruning history),
whole-file buffering and unbounded retained history with no GC. Only Windows
regular files/directories and the documented name normalization are supported.
The tested namespace permutations are evidence, not a proof of all graphs.

The original plan remains byte-identical in its archive, verified SHA-256
`dd00e62cd9e142a4b71e0715d9e77cff7476e6e72d680be85ce4aff1323d6fde`.
The original workspace had no Git repository; no commits/pushes/external publishing
or writes to other projects occurred. The tests use isolated workspace fixtures.
