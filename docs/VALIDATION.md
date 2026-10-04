# Implementation validation — 2026-10-03

The original 42-test/17-mount evidence below predates the acknowledgement correction
from the later physical run. The separate correction validation and physical scope
are recorded under "Physical findings and acknowledgement correction" below;
the original JSON reports and benchmark observations are preserved.
Paths under `test-runs/` refer to ignored local logs/fixtures and are not included
in a fresh checkout or portable package.

## Local orchestrator O1 and WinFsp loader — 2026-10-03

Implemented the accepted foreground, single-user, peer-network-disabled O1 slice.
The registry journals caller-scoped UUID requests with expected catalog/state
generations and payload hashes. Creation preallocates identities, stages a
versioned marker and initializes explicitly; existing workers open strictly.
Management uses owner-only local Windows named pipes, verified caller/server
SIDs, bounded frames/I/O and a separate transient worker instance secret passed
through stdin. Supervisor crashes reattach authenticated instances; an occupied
unverified store is unavailable and never killed/replaced. Cooperative shutdown
refuses handles/mappings and verifies process exit. Desired state survives
graceful supervisor shutdown; a completed shutdown receipt never exits a new
supervisor. Worker launch failures/backoff/limits are per-state.

The reported Windows startup error was reproduced as `0xC0000135`
(`STATUS_DLL_NOT_FOUND`) with the installed `winfsp-x64.dll` absent from PATH.
The new build delay-loads WinFsp; mount startup loads the DLL by absolute path
from an explicit `WINFSP_DIR`, otherwise the installer's registry or conventional
installation path. `dumpbin /dependents` confirmed WinFsp moved to delay imports;
clean-environment CLI, test binaries and managed real mounts succeeded. Bad
explicit overrides produce `WINFSP_RUNTIME_UNAVAILABLE` with the attempted path.
No system PATH, driver, firewall or existing executable/daemon was changed.
Older binaries still use their original startup loader behavior.

Build/test target: `target/o1`, keeping the default executable and original
daemons untouched. Executed:

```powershell
cargo build --offline --target-dir target/o1
cargo test --offline --target-dir target/o1
cargo test --offline --target-dir target/o1 --test orchestrator
cargo clippy --offline --all-targets --target-dir target/o1 -- -D warnings
cargo fmt --check
python scripts/orchestrator_e2e.py
python scripts/e2e.py --exe target/o1/debug/tkfs.exe --report test-evidence/ORCHESTRATOR-COMPATIBILITY.json
```

The final full suite passed all 54 tests, including the existing acknowledgement,
recovery, replication and 10,000-save regression tests. The focused O1 suite
contains four integration tests and three targeted lifecycle regressions, covering
strict missing/identity refusal, explicit initialization, defaults validation,
duplicate registry ownership, exact terminal receipt replay, interrupted
bootstrap, future schema refusal and wrong owner refusal, startup shutdown fencing,
recoverable staging installation and terminal-versus-retryable I/O classification.
Final Clippy with warnings denied and formatting passed.

[ORCHESTRATOR-VALIDATION.json](../test-evidence/ORCHESTRATOR-VALIDATION.json) records 22 local
acceptance checks: two independent real mounts, durable isolated content,
duplicate supervisor/store ownership refusal, exact replay/mismatch/stale
generation behavior, API version/anonymous caller rejection, worker token and
instance rejection, mounted bearer separation, crash reattachment, unverified
owner isolation, open handle/mapping refusal, occupied target/data overlap
refusal, graceful exit/restart and completed-shutdown replay, strict missing
metadata recovery, five creation crash boundaries, partial startup failure and
running-limit recovery, durable shutdown fencing across pending create/start and
crash/restart, and staging sharing-violation recovery without reallocation/data
loss. The harness removes WinFsp PATH and `WINFSP_DIR` from
its environment and retains its own fixture directories under `test-runs/`.
All successful-run fixture workers/mounts were cooperatively stopped.

[ORCHESTRATOR-COMPATIBILITY.json](../test-evidence/ORCHESTRATOR-COMPATIBILITY.json) records all
18 existing real mounted-filesystem/peer checks passing against the new binary,
including notifications, retained mappings, private/current-state publication,
offline conflict convergence and acknowledgement-loss replay. This remains
two local processes on one computer, not renewed physical-machine qualification.

Deferred: adoption; O2 mount reassignment/trash/restore/purge; O3/O4 persistent
machine trust/catalog/contracts; privileged services, logoff/reboot/session
visibility, installer/UI; power-loss and physical cross-machine qualification.
There is no claim of installation-level multi-user authorization or a reviewed
network identity protocol. Existing per-store mounted discovery RPC remains
loopback/bearer for compatibility; it grants no supervisor lifecycle capability.

Independent-review follow-up: both reported P2 issues were reproduced with failing
targeted tests before fixes. `recover()` now checks the durable shutdown fence
before its first startup pass; pending create/start exact retries also honor it.
Recoverable I/O classification retains the typed Windows sharing/lock/busy errors
(32/33/170) through the error chain instead of relying on localized message text.
Missing paths, access-denied errors outside existing worker availability handling,
invalid markers/identities and unsupported versions are not broadly converted to
endless retries. The real-mount harness also reproduced busy shutdown with both
pending create/start, crash/restart, and a non-delete-sharing staging handle across
retry/restart/release. Both recovered with the same allocated IDs and preserved
data. No deferred scope or machine configuration was changed.

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

Machine-readable benchmark: [SCALABILITY.json](../test-evidence/SCALABILITY.json). Final stdout:
`test-runs/final-tests.log` and
`test-runs/final-release-scalability.log`.
These benchmark core/Engine
flushes to actual native Windows SQLite/CAS with FULL/write-through durability,
small files and independent replica stores in **one process**, without the mount.
The real mounted suite is separate below. These are single-run observations,
not production throughput guarantees. Startup still verifies retained objects;
genuine content contention can create quadratically many inspectable historical
pairs (100 x 100 here), and larger files/history cost more memory, storage and I/O.

## Mounted acceptance evidence

Machine-readable result: [VALIDATION.json](../test-evidence/VALIDATION.json). Reproducible harness:
`scripts/e2e.py` (included in the portable package's source archive).
Its result records the exact latest fixture
directory/device UUIDs; preserved `a-<index>-stdout.log`, `a-<index>-stderr.log`
and equivalent `b-*` files live there, alongside `watch.events`/watcher logs.
Each device retains its own SQLite database/CAS. The ephemeral peer key is not
logged. The harness terminates its runtime/watcher processes after testing.

Final fixture: `test-runs/e89b43d5-3878-4c31-afb1-fe240f99a82a/`.
Devices: `21076db3-4f43-46f9-8ab9-f3dded73f8a7` and
`e07f9f38-b46c-47bb-877d-692cd8fa2bb5`. Direct evidence:
A initial stdout (`test-runs/e89b43d5-3878-4c31-afb1-fe240f99a82a/a-0-stdout.log`),
B initial stdout (`test-runs/e89b43d5-3878-4c31-afb1-fe240f99a82a/b-0-stdout.log`),
watcher events (`test-runs/e89b43d5-3878-4c31-afb1-fe240f99a82a/watch.events`),
final mounted stdout (`test-runs/final-mounted.log`).
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

## Physical findings and acknowledgement correction

The original physical report was retained at
`test-runs/two-machine-448ed3f6-948d-4d90-b8b7-9b1cdbd82744/TWO-MACHINE-VALIDATION.json`
(an ignored local artifact, not included in a fresh checkout). It
records ten live checks on FIGLOALDS/FELYPE at commit
`303fec87c02444d3c9790ac11268f9c396fb6527`: bidirectional mounted saves,
stable-ID renames/delete, private branch/canary event/object isolation and
bidirectional authentication after the user's firewall exception. Physical
offline conflict/reconnect qualification was not completed. Both nodes reported
caught-up while one/three received events remained queued despite durable copies.

Two new baseline regressions reproduced the stale queue and stale caught-up flag
before changes (failure log (`test-runs/ack-fix-before.log`)). The correction makes
authenticated accepted-event inventories authoritative receipts on both request
and response paths. Peer inventory and outbox reconciliation are atomic; events
withdrawn from the peer inventory are queued again. SQL acknowledgements apply
only to validated shared journal entries. Objects, private events, incomplete
manifests and quarantined data do not become event receipts. Explicit ack IDs must
be in the authenticated sender's accepted inventory. Protocol version remains 2.

`status.caught_up` now requires current matching shared inventories, empty queues,
no pending causal events/unflushed staging and healthy storage, in addition to a
successful exchange. Failed explicit and background syncs invalidate `last_sync`.
This is knowledge of the last peer exchange, not a guarantee about unseen remote
edits or a distributed barrier. Conflict rules and object publication scope are
unchanged.

Five focused tests cover received-event echoes, lost acknowledgements and restart,
repeated inventory replay, peer restore/withdrawal, staging/current status,
object-only receipts, private/quarantined data, real encrypted TCP reply loss and
retry without explicit acks, wrong device, unbound responses and inconsistent ack
claims. All passed, alongside the relevant existing suite:

```powershell
$env:PATH = 'C:\Program Files (x86)\WinFsp\bin;' + $env:PATH
cargo test --offline --target-dir target/ack-fix -- --nocapture --skip ten_thousand_durable_content_saves_and_later_delete_undelete --skip two_replicas_each_make_one_hundred_offline_saves_and_retain_all_pairs
cargo fmt -- --check
cargo clippy --offline --target-dir target/ack-fix --all-targets -- -D warnings
cargo build --offline --target-dir target/ack-fix
python scripts/e2e.py --exe target/ack-fix/debug/tkfs.exe --report test-evidence/ACK-FIX-VALIDATION.json
```

Results: **45 Rust tests passed**, zero failures; two heavy save benchmarks were
intentionally filtered because content projection/storage did not change. Their
original measurements remain in `SCALABILITY.json`. Formatting, strict Clippy,
offline build, diff whitespace and harness syntax checks passed. **18 real WinFsp
checks passed**, adding both outgoing queues reaching zero after repeated two-way
sync to the original mounted acceptance suite.

Machine-readable correction result: [ACK-FIX-VALIDATION.json](../test-evidence/ACK-FIX-VALIDATION.json).
Logs: focused tests (`test-runs/ack-fix-focused.log`),
relevant suite (`test-runs/ack-fix-tests.log`),
actual mounts (`test-runs/ack-fix-mounted.log`).
Correction fixture: `test-runs/b6102308-b998-461b-bf77-f1ca5db437fd/`; each device's native SQLite/CAS and
stdout/stderr logs are retained there. These are **two local processes**, not a
retest on physical computers. Both disposable mounts/processes were removed.
Original daemon PIDs `44164`/`66280`, the default executable, original physical/local
reports and plan archive remain untouched. Those daemons still run the prior
binary; no restart, second-machine operation, settings change or push occurred.
The corrected executable is `target/ack-fix/debug/tkfs.exe`. Physical testing of the
correction and offline/reconnect qualification await user-directed next steps.

## Remaining setup and acceptance gaps

The original build at `303fec87c02444d3c9790ac11268f9c396fb6527` subsequently passed
ten live checks on two physical computers. The acknowledgement correction is
validated locally only; physical offline/reconnect qualification remains pending.

Real second-machine prerequisites/runbook are in
[README.md](../README.md#two-explicitly-configured-devices): another x64 Windows
computer with WinFsp, distinct device database/UUID, common repository UUID,
authorized reachable endpoints and an ephemeral out-of-band pairing key. No
driver, firewall or network-security setup was performed by agents; the physical
report records the user's firewall exception. Both daemon peer
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
