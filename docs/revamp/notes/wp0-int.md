# WP0.INT — Wave 0 integration: merge and land every seam stub

Branch `revamp/wp0-int`, cut from `revamp/wave-0-base` (`4af40db`).
`revamp/production-sync` was not touched and `revamp/wave-1-base` was not
created: the coordinator does both after review (the fast-forward of §13
rule 6 and item 7 of rule 8).

## Summary

1. **Merges**, in the order of §14 WP0.INT, each as a `--no-ff` merge commit
   followed by the touched tests:

   | # | Branch (head) | Conflicts | Touched tests after the merge |
   |---|---|---|---|
   | 1 | `revamp/wp0-1-orchestrator-carveup` (`993967c`) | none | build; `orchestrator` 15/15 |
   | 2 | `revamp/wp0-2-runtime-carveup` (`0449160`) | none | `orchestrator` 15/15, `worker_seams` 3/3, `runtime_lanes` 3/3, `availability` 13/13, `partial_content` 7/8 (pre-existing race, see CCR-WP0.INT-1) |
   | 3 | `revamp/wp0-3-core-carveup` (`ac81374`) | none | clippy clean; `oracle_smoke`, `startup_snapshots`, `replication_regressions`, `core`, `recovery`, `availability`, `acknowledgements`, `paired_sync`, `partial_content`, `bulk_lanes`, `worker_seams`, `orchestrator`, `runtime_lanes`: all green |
   | 4 | `revamp/wp0-4-contracts` (`3cb4619`) | `src/lib.rs` auto-merged (WP0.2 and WP0.4 both add `mod` lines) | `contracts` 30/31: the code-literal registry found WP0.2's new `DIRTY_SESSION_RETAINED` (fixed under CCR-WP0.4-3) |
   | 5 | `revamp/wp0-5-harness` (`fe7ce83`) | none | `harness_selftest` 18/18 (+2 ignored capture routines): legacy fixtures open with the merged build, compat goldens re-capture identically, `qualify.ps1 -SelfTest` passes |

   The glob-import risk WP0.3 flagged (`use crate::core::*` now also brings
   module names such as `status` and `objects`) did not materialize: the
   merged tree built with no warnings, including after the new
   `orchestrator::{objects, ...}` seam modules were added.

2. **Seam stubs** (commit "land every wave 0 seam stub"), exactly the
   §14 WP0.INT table. Every new file starts with
   `//! Owned by WPx.y — stub landed by WP0.INT`.

   | File | Landed | Owner |
   |---|---|---|
   | `src/runtime/ctx.rs` | `WorkerCtx` (every field of the plan, same names and types), `SyncStorePool` + `PooledStore` (`Deref`/`DerefMut<Target = Store>`), `ObjectLease`, `EngineGaugeCell` (`record_wait`, `record_hold`, `snapshot() -> EngineGauges`) | frozen; WP1.4 constructs |
   | `src/runtime/sync_rpc.rs` | `pub fn dispatch(ctx: &WorkerCtx, op: &str, payload: &Value) -> anyhow::Result<Value>` → `bail!("UNSUPPORTED_OPERATION: {op}")` | WP2.5 |
   | `src/runtime/objects_rpc.rs` | `pub fn dispatch(ctx: &WorkerCtx, op: &str, header: &Value, body: &[u8]) -> Result<(Value, Vec<u8>), ErrorWire>` → `UNSUPPORTED_OPERATION` | WP3.2 |
   | `src/worker/mount_ctl.rs` | `MountController::new(engine: Shared, previous: Option<WorkerRecord>) -> Arc<Self>`; `impl MountControl` delegating to `mount::start` / `mount::stop` | WP1.5 |
   | `src/worker/{record, heartbeat}.rs` | empty | WP1.4 |
   | `src/worker/{activator, status_cell, maintenance}.rs` | empty | WP2.3 |
   | `src/orchestrator/{catalog, published}.rs` | empty | WP1.1 |
   | `src/orchestrator/{folder, registry}.rs` | empty | WP1.2 |
   | `src/orchestrator/{process, worker_client}.rs` | empty | WP1.3 |
   | `src/orchestrator/{hub, hub_rules}.rs` | empty | WP1.11 |
   | `src/orchestrator/{ctx, shutdown, legacy_bridge}.rs` | empty | WP2.1 |
   | `src/orchestrator/manage.rs` | `pub fn manage(ctx: &Arc<Ctx>, action: super::api::Action) -> anyhow::Result<Value>` → `bail!("NOT_IMPLEMENTED")` | WP2.1 |
   | `src/orchestrator/manage_v2.rs` | `pub fn handle(ctx: &Arc<Ctx>, action: super::api::Action) -> anyhow::Result<Value>` → `bail!("NOT_IMPLEMENTED")` | WP2.2 |
   | `src/orchestrator/api.rs` | the frozen `Action` variants (below); the legacy `manage()` answers them with `NOT_IMPLEMENTED` through explicit arms; its `_ => unreachable!()` became an explicit error arm | frozen |
   | `src/orchestrator/peer.rs` | empty | WP2.6 |
   | `src/orchestrator/scheduler.rs` | `pub fn run_control(ctx: Arc<Ctx>, peer: Arc<PeerContext>, conn: session::Conn, cancel: Cancel)` → `cancel.wait()` | WP2.7 |
   | `src/orchestrator/meta_exchange.rs` | empty | WP2.7 |
   | `src/orchestrator/objects.rs` | `pub fn run_bulk_lane(ctx: Arc<Ctx>, peer: Arc<PeerContext>, lane: LaneSpec, conn: session::Conn, cancel: Cancel)` → `cancel.wait()` | WP2.7 (interim), WP3.3 |
   | `src/orchestrator/inbound.rs` | empty | WP2.8 |
   | `src/orchestrator/inbound_objects.rs` | `pub fn handle(ctx: &Arc<Ctx>, peer: &session::AcceptedPeer, req: session::Incoming)` → `req.responder.error(UNSUPPORTED_OPERATION)` | WP2.8, WP3.3 |
   | `src/orchestrator/compat.rs` | empty | WP3.1 |
   | `src/orchestrator/diagnostics.rs` | `pub fn collect(ctx: &Arc<Ctx>) -> anyhow::Result<Value>` → `Ok(json!({"schema":1}))` | WP4.2 |
   | `src/ui_model.rs` | empty; `pub mod ui_model;` in `lib.rs` | WP3.4 |

   `Action` (serde `tag = "op"`, kebab-case, `deny_unknown_fields`) gained
   `Status`, `Subscribe{since: u64, #[serde(default)] wait_ms: Option<u32>}`,
   `Attach{state}`, `Detach{state, #[serde(default)] force: bool}`,
   `PauseSync{state}`, `ResumeSync{state}`, `OverrideGate{state}`,
   `ForceStop{state}`, `Retry{state}`,
   `Revalidate{state, #[serde(default)] ids: Option<Vec<String>>}`,
   `Diagnostics`, and `Stop{state, #[serde(default)] wait: Option<bool>}`.
   The ops are `status`, `subscribe`, `attach`, `detach`, `pause-sync`,
   `resume-sync`, `override-gate`, `force-stop`, `retry`, `revalidate`,
   `diagnostics` (the JSON the WP0.5 qual files already send). The legacy
   `manage()` answers each with `NOT_IMPLEMENTED: <op> is not served by the
   legacy orchestrator`, before it takes the lifecycle lock, and ignores
   `Stop.wait` (it always stops synchronously, as before).

   Registry lines (§13 rule 1): `pub mod ui_model;` in `src/lib.rs`;
   `pub mod {ctx, objects_rpc, sync_rpc};` in `src/runtime/mod.rs`;
   `pub mod {activator, heartbeat, maintenance, mount_ctl, record, status_cell};`
   in `src/worker/mod.rs`; 21 `pub mod` lines in `src/orchestrator/mod.rs`
   (a separate block after WP0.1's private legacy modules). All new seam
   modules are `pub` so their owners can test them through `tkfs::…` and so
   the stub functions are not dead code.

   Behaviour of the stateful seams:

   - **`SyncStorePool`** opens `Store::open_secondary(root, repo, device)`
     connections. `checkout()` reuses an idle connection or opens a new one,
     so it never blocks behind other checkouts (a long `objects.*` stream can
     hold one). On drop a connection goes back to the pool only while fewer
     than `MAX_IDLE = 8` are idle and it is in autocommit; otherwise it is
     closed (which rolls back anything left open). Pooled connections see the
     engine's commits through WP0.3's `data_version` view key (tested).
   - **`EngineGaugeCell`** keeps its counters in atomics, plus a
     1,024-sample ring of waits for `wait_p99_ms` (nearest rank) behind a
     short internal mutex (never the engine lock). `record_hold` counts the
     hold, updates `longest_hold_ms` and ends the hold in progress;
     `begin_hold` starts it, so `current_hold_ms` and `stalled`
     (> `timing::ENGINE_STALLED_AFTER`, 120 s) are computable without the
     engine lock; `begin_wait` returns a guard that counts a waiter.
   - **`MountController`** (today's semantics): `attach(path)` calls
     `mount::start`; on failure it records
     `AttachFailed{error: ErrorWire::from_error, attempt, next_retry_at_ms: None}`
     unless a mount is already attached (a refused second attach leaves the
     mount in place). `detach(force)` calls `mount::stop` only when the engine
     is mounted, exactly as today's `stop` op does, so detaching an unmounted
     worker never fences the engine. `force` has no separate path yet.
     `status()` reads a `Snapshot<MountStatus>` and never waits for an attach
     or detach. On WinFsp a second `attach` in one process still fails with
     `ONE_MOUNT_PER_RUNTIME` because `mount.rs` sets its `ENGINE` `OnceLock`
     before the path check (§1.2 M10); WP1.5 fixes that.

3. **CCRs applied** (commit "apply wave 0 CCRs"); see the CCR section.

## Deviations from the spec, and why

1. **Additive API on the frozen `ctx.rs` types.** The table lists only
   `checkout`, `record_wait`, `record_hold` and `snapshot`. Since nobody owns
   `ctx.rs` after Wave 0, WP0.INT also landed what WP1.4 needs to build and
   feed them: `SyncStorePool::{new, idle, MAX_IDLE}`, `DerefMut` on
   `PooledStore`, `EngineGaugeCell::{new, begin_wait, begin_hold, waiters,
   WAIT_SAMPLES}` and its `WaitGuard`, and `Clone, Debug, Default, PartialEq,
   Eq` on `ObjectLease`. `EngineGauges.waiters` and `current_hold_ms` cannot
   be derived from completed waits and holds alone.
2. **"max 8" is the idle limit, not a concurrency limit.** Blocking a 9th
   checkout would stall `status` behind long object streams (and could
   deadlock a thread that checks out twice); overflow connections are opened
   and closed instead.
3. **The legacy `manage()` answers the new variants before taking the
   lifecycle lock** (so an `ORCHESTRATOR_STOPPING` daemon also answers them
   `NOT_IMPLEMENTED`). The legacy `Supervisor` mutex is held across I/O
   (RC8), and a stub answer should not wait on it.
4. **`MountController::detach` on an unmounted worker is a no-op** (state
   `Detached`, intent `Detach`) rather than a call into `mount::stop`, which
   on WinFsp fences the engine for the rest of the process. This is what
   today's `stop` op does.
5. **Model types gained `PartialEq, Eq`** as the follow-through of
   CCR-WP0.4-1: `FeedOfferRequest`, `FeedExchangeRequest`, `FeedApplyRequest`
   (`model::ipc`) and `WantHeader`, `ReadHeader`, `WriteHeader`,
   `ReportHeader` (`model::protocol::objects`), and the "compare serialized
   forms" doc notes were removed. Purely additive; the golden fixtures and
   `tests/contracts.rs` are unchanged (they still compare JSON).
6. **`tests/seam_stubs.rs` pins only frozen behaviour**: signatures (as
   `fn` pointer types), the `WorkerCtx` field set, Send/Sync, the
   `Action` JSON, the legacy `manage()` answer, the unknown-op convention of
   the worker RPC seams (`feed-unknown`, `objects.unknown` →
   `UNSUPPORTED_OPERATION`), `SyncStorePool`, `EngineGaugeCell`, and the
   mount invariants that also hold for WP1.5 (an occupied path is a
   NeedsUserAction `AttachFailed` and never touches `Engine.health`). It does
   not pin stub answers that owners will replace (`manage::manage`,
   `manage_v2::handle`, `diagnostics::collect`, known feed/objects ops), so
   no Wave 1–4 package has to edit a file it does not own.
   `run_control`, `run_bulk_lane` and `inbound_objects::handle` are pinned by
   signature only: `session::Conn` and `session::Incoming` cannot be
   constructed until WP1.9.
7. **Edits to existing test files** (none changes an assertion):
   - `tests/orchestrator.rs`: the 9 `Action::Stop { state }` literals gain
     `wait: None` (required by the frozen `Stop{state, wait}`; compile fix
     only), and CCR-WP0.5-3 below.
   - `tests/partial_content.rs`: CCR-WP0.INT-1 below.
8. **CCR-WP0.5-3 applied in part.** The race is fixed at its root (the
   wait). The second half of the request (stop the folder's worker in
   `tests/orchestrator.rs`'s `Node::drop`) was not applied: it changes the
   teardown of all 15 tests, and the orphan only appeared after that race
   panicked. Left to WP3.1, which owns the file's migration.
9. **`C:/tkfs-build/suite-lock.ps1` was not changed** (WP0.5 deviation 13
   suggests moving it to `Global\`). It is outside the repository and shared
   by the other agents running now. The repo's `scripts/suite-lock.ps1`
   takes `Local\tkfs-full-suite` first and then `Global\tkfs-full-suite`, so
   the two exclude each other; VERIFY used the repo script.
10. **Behaviour-neutral Wave 0.** Pre-existing product defects found in
    review (CCR-WP0.INT-2, -3, -4) are filed, not fixed: none has a
    deterministic reproduction here, and the wave's contract is no
    behaviour change.

## CCRs

### Applied by WP0.INT (filed by the packages)

| CCR | Change | Where |
|---|---|---|
| CCR-WP0.4-1 | `paired_sync::SyncRoute` derives `PartialEq, Eq` (+ deviation 5) | `src/paired_sync.rs`, `src/model/{ipc,protocol}.rs` |
| CCR-WP0.4-2 | `pairing_code::Verifier`: hand-written `Debug` that prints `hash: "<redacted>"`, plus `PartialEq, Eq`; unit test `verifier_debug_redacts_the_hash`. WP0.4's adapters (redacting `Debug` on `CatalogSnapshot`/`CatalogCmd`) stay: they are still correct | `src/pairing_code.rs` |
| CCR-WP0.4-3 | Registered the one new code literal of the wave, `DIRTY_SESSION_RETAINED` (WP0.2's `retain_all_dirty` reason), as Transient-default. `PENDING_SAVE_PERSIST_FAILED` was already pre-registered. The stubs use only pre-registered codes (`NOT_IMPLEMENTED`, `UNSUPPORTED_OPERATION`) | `src/model/errors.rs` |
| CCR-WP0.5-1 | Root `.gitattributes`: `*.sh text eol=lf`; `scripts/qualify.sh` re-checked out with LF (`git ls-files --eol`: `i/lf w/lf`). The script's CR-stripping prologue stays as a second line of defence | `.gitattributes` |
| CCR-WP0.5-2 | `.gitignore`: `/test-evidence/*` + `!/test-evidence/qualification-*.json` (a directory pattern cannot be re-included, so `/test-evidence/` became `/test-evidence/*`), `!/tests/fixtures/**`, and SQLite side files (`*-wal`, `*-shm`, `*-journal`) under `tests/fixtures` stay ignored | `.gitignore` |
| CCR-WP0.5-3 | `real_partial_mount_fetches_on_read_without_blocking_unrelated_metadata` also waits for `content.current_available >= 1` before deleting the object (`complete` is vacuously true before the event commits). See deviation 8 | `tests/orchestrator.rs` |

### Filed by WP0.INT

- **CCR-WP0.INT-1 (applied; test-only).** Filed from the WP0.3 review
  finding on `tests/partial_content.rs:283-288`.
  `bulk_install_wait_does_not_block_control_rpc` raced with itself:
  `published_rpc` takes the engine with `try_lock` and answers
  `WORKER_BUSY` (before applying anything) when it collides with the test's
  5 ms polling loop, which holds the same mutex; the status assertion then saw
  `current_missing = 0`. It failed 1 in 20 runs of the binary after the WP0.2
  merge. Change: the install thread retries `WORKER_BUSY`, and the poll waits
  up to 10 s (was 0.5 s) for the metadata. No assertion changed. After the
  change: 0/40 sequential runs and 0/48 copies in 8 rounds of 6 parallel
  copies. The same parallel stress on the pre-change binary also passed
  48/48, so this run is not a comparison; the fix is justified by the failure
  mode itself. WP2.3's "retry `try_lock` ≤ 100 ms before `WORKER_BUSY`" fixes
  the product side.
- **CCR-WP0.INT-2 (not applied).** Requested change: `Engine::control`
  (`src/runtime/control.rs:169`) opens a deferred `SAVEPOINT control_request`,
  reads, then writes. When another connection to the same WAL database (the
  bulk transfer view, a pooled `SyncStore`) commits in between, the
  read-to-write upgrade fails with `SQLITE_BUSY_SNAPSHOT` (517) or
  `SQLITE_BUSY` (5) without running the busy handler, so the 5 s
  `busy_timeout` does not help. Fix: when the connection is in autocommit,
  start with `BEGIN IMMEDIATE; SAVEPOINT control_request`, and `COMMIT` after
  `RELEASE` (before `fault("receipt_committed")`) and after the rollback
  path; or retry the whole request on BUSY (the savepoint is rolled back and
  receipts make it idempotent). Reason: it is the pre-existing flake in
  `orchestrator::three_orchestrators_relay_origins_and_private_branches_stay_local`
  (WP0.2 notes and review). It failed once during this integration, in a
  targeted `--test orchestrator` run after the seam commit, panicking in the
  same `rpc(…).unwrap()` helper (`tests/orchestrator.rs:168`) as the WP0.2
  runs; the error text was not captured, and the test then passed 6/6 alone
  and 15/15 in the full suite. Owner:
  `control.rs` has no owner in Waves 1–4, so WP1.INT (or WP1.6 under this
  CCR, since it classifies BUSY as retry) should apply it. Adapter: none; the
  suite is re-run when it fails.
- **CCR-WP0.INT-3 (not applied).** Requested change: `Staged::retain`
  (`src/staging.rs`) creates `staging/<uuid>.stage` and fsyncs only the file;
  `retain_pending` then commits the `pending_saves` row with
  `synchronous=FULL`. After power loss on Linux the row can be durable while
  the directory entry is not, so `load_pending` fails and those bytes are lost
  (`RECOVERY_REQUIRED`). Fix: after creating the stage file, fsync
  `root/staging` on unix (as `core.rs` already does after installing an
  object). Reason: invariant 8 for WP0.2's `retain_all_dirty`, which WP1.4's
  poison path and WP1.5's force detach rely on. Owner: `staging.rs` has no
  owner in Waves 1–4; WP1.INT applies it, or WP1.4/WP1.5 under this CCR.
- **CCR-WP0.INT-4 (not applied; frozen-contract review findings for the
  owners named).** From the WP0.4 review, each a defect in a frozen file:
  1. `model::protocol::feed::MAX_NEED_BYTES` (16 MiB of payload) plus a reply
     page (`MAX_PAGE_BYTES`, 1 MiB) plus JSON overhead can exceed
     `frame::MAX_BODY_META` (16 MiB), the `meta.exchange` reassembly limit: a
     reply serving two 8 MiB events would be refused forever. Set the need
     budget to `MAX_BODY_META - MAX_PAGE_BYTES - margin` (for example 12 MiB).
     Needed by WP2.5 (Wave 2).
  2. `op_role("catalog.list")` returns `Some(Control)`, but §6.2 allows
     control **or** probe sessions. Add a frozen `op_allowed(op, role)` helper
     before WP2.8 enforces roles.
  3. `util::cancel`: `Inner::cancel` runs hooks before cancelling children,
     and `on_cancel` hooks cannot be deregistered (per-connection hooks on a
     long-lived token leak). Cancel children first; document registering
     per-connection killers on `child()` tokens.
  4. `util::lock::TrackedGuard::drop` reports (and in debug builds logs)
     while the tracked mutex is still held, and `util::log::emit` writes the
     sink under the global logger mutex (§2.2 rule 2). Release first, then
     report; format under the lock and write after it.
  5. `util::log` never prunes its per-code rate-limit map, so dynamic codes
     (peer-supplied `ErrorWire` codes) grow it without bound. Cap or prune it.
  6. `util::backoff::Backoff` sanitizes `jitter` only in `new`; a NaN or
     infinite `jitter` set through the pub field panics in `gen_range`. Clamp
     it in `sample`/`bounds`.
  7. `tests/contracts.rs`'s code-literal scan misses codes passed to `event!`
     as bare identifiers (`event!(fatal, LOCK_POISONED, …)`); scan those too.

  These are for the Wave 1 integrator to schedule (items 3–6 are used by
  Wave 1 packages; 1–2 by Wave 2). Adapter: none needed in Wave 0.

## Package status

| Package | Review | Merged | Status in the merged tree | Open issues (owner) |
|---|---|---|---|---|
| WP0.1 orchestrator carve-up | approved | yes (`c229825`) | green; public API unchanged | RC8 behaviour kept by design: global `Supervisor` mutex across I/O, 10 ms main loop, fatal `peers()` error (WP1.12, WP2.1) |
| WP0.2 runtime carve-up, worker entry, seams | approved | yes (`97a0213`) | green; `DIRTY_SESSION_RETAINED` registered (CCR-WP0.4-3) | `three_orchestrators_*` SQLite busy-snapshot flake (CCR-WP0.INT-2); staging directory fsync (CCR-WP0.INT-3); WP1.4's poison handler must roll back open savepoints before `retain_all_dirty`; WP1.5 must keep or reload sessions after a force detach; review minors: `worker_seams.rs:77` `quiet().is_err()` holds regardless of the retain (assert the message), and the failure path of `retain_all_dirty` (`k of n`) has no test (WP1.4/WP1.5) |
| WP0.3 core carve-up, oracle, view freshness | approved | yes (`ccec5db`) | green | oracle blind spots: inflated `resolved_by` is not reported (clear `resolved_by` inside the savepoint before rebuild) and fabricated register conflict rows are not reported; oracle error path checks `rollback?` before `result?` and reads before writing in a deferred savepoint (BUSY_SNAPSHOT on a live worker) (WP1.7 uses the oracle; `verify.rs` has no later owner: WP1.INT); `data_version` makes the engine reload its view after foreign commits such as bulk bookkeeping, O(projection) per reload (WP1.8/WP2.x qualification); no test pins `open_existing_with` W0 behaviour (WP1.8); `partial_content` race fixed here (CCR-WP0.INT-1) |
| WP0.4 contracts, utilities, session stubs | approved | yes (`6417df3`) | green; CCR-WP0.4-1/-2/-3 applied | review findings filed as CCR-WP0.INT-4 (need budget vs `MAX_BODY_META`; `catalog.list` probe role; `Cancel` hook ordering and deregistration; `Tracked` drop order and log I/O under the logger lock; unbounded log rate-limit map; `Backoff` pub jitter; `event!` identifiers not scanned); minors: `ProcessEvent::Exited` `Option` fields lack `#[serde(default)]` (no runtime effect); `objects.wants` reply is `{"items":[…]}` rather than the bare `[WantItem]` of §6.2, undocumented in notes/wp0-4.md (WP2.8/WP3.2 should follow the frozen type) |
| WP0.5 harness, goldens, fixtures | **not approved** | yes (`1ee843c`); nothing in it blocked integration | `harness_selftest` green; 77 qual scenarios compile and are ignored; CCR-WP0.5-1/-2/-3 applied | **major:** `qual_wp3_int_e2e::idle_exchange_daemons` computes `(bytes1-bytes0)/(ex1-ex0).max(1)` with unchecked `u64` subtraction and no proof an exchange happened, so the W3 floor `e2e_idle_exchange_bytes` can pass with nothing measured (WP3.INT, owner of the file; WP0.5 follow-up if reopened). **minor:** no daemon-level (W3) idle-exchange CPU floor; `qual_wp1_12_legacy::crash_backoff` and both `interval_20s_no_reconnects` can pass vacuously (no restart lower bound; no "lanes still connected" check) (WP1.12, WP3.1); `qual_wp2_4_offlock` lock-hold probe ignores RPC errors (WP2.4); `worker_busy_not_counted` passes with no contention observed (WP1.12); `qual_wp3_2_objects::crash_resume` never checks it was killed mid-object (WP3.2); `metrics::qual()` lets `extra` override reserved keys, and `qual_wp2_3_worker` passes `"status"` (WP2.3/WP0.5) |

## Tests added

- `tests/seam_stubs.rs` (7 tests, Windows and Linux):
  `stub_signatures_are_exactly_the_landed_ones`,
  `action_v2_variants_parse_the_frozen_json`,
  `legacy_manage_answers_v2_actions_not_implemented`,
  `worker_rpc_seams_answer_unknown_ops_unsupported`,
  `sync_store_pool_reuses_up_to_eight_idle_connections`,
  `engine_gauge_cell_reports_waits_holds_and_stalls`,
  `mount_failures_stay_mount_scoped`.
- `src/pairing_code.rs` unit test `verifier_debug_redacts_the_hash`
  (CCR-WP0.4-2).

## VERIFY

All on the final code (`1f16b00`; the commits after it only add these notes
and the evidence file), Windows `CARGO_TARGET_DIR=C:/tkfs-target/wp0-int`,
WSL `CARGO_TARGET_DIR=$HOME/tkfs-target/wp0-int`.

| Check | Result |
|---|---|
| `cargo fmt --check` | pass |
| `cargo build --all-targets` | pass, no warnings |
| `cargo clippy --all-targets -- -D warnings` | pass |
| `powershell -NoProfile -File scripts/suite-lock.ps1 cargo test --all-targets` (the repo's lock script works and was used, see deviation 9) | **pass, EXIT=0: 194 passed, 0 failed, 80 ignored** across 38 test binaries |
| WSL `cargo check --all-targets` | pass, no warnings |
| WSL `cargo clippy --all-targets -- -D warnings` (extra: covers the `cfg(unix)` code) | pass |
| WSL `cargo test --lib --test orchestrator --test seam_stubs --test worker_seams --test contracts --test harness_selftest --test partial_content --test oracle_smoke --test runtime_lanes --test availability --test paired_sync` | **pass: 124 passed, 0 failed** (lib 16 + 1 ignored, orchestrator 15, seam_stubs 7, worker_seams 3, contracts 31, harness_selftest 18 + 2 ignored, partial_content 8, oracle_smoke 4, runtime_lanes 3, availability 10, paired_sync 9). Of §13 rule 8's subset only `orchestrator` exists yet (`session_*`, `worker_lifecycle`, `mount_lifecycle` arrive in Wave 1); `/dev/fuse` is present, so the FUSE mount tests ran |
| Legacy fixtures (§13 rule 8.3) | `harness_selftest::legacy_worker_fixture_opens_with_current_build` and `legacy_datadir_fixture_opens_with_current_build` pass on Windows and WSL; the release qual scenario `legacy_fixture_upgrade_then_sync` passes |
| Compat goldens | `harness_selftest::compat_goldens_recapture_identically` passes on Windows and WSL |
| Qualify baseline reproduced | see below |

**Full-suite breakdown (Windows).** 194 = the 130 baseline tests (lib 13 +
1 ignored, bin 4, integration 113) + `worker_seams` 3 (WP0.2) +
`oracle_smoke` 4 (WP0.3) + `contracts` 31 (WP0.4) + `harness_selftest` 18
(WP0.5) + `seam_stubs` 7 + the `pairing_code` unit test (lib is now 14). The
80 ignored are the baseline's 1, WP0.5's 2 capture routines and the 77 qual
scenarios. Per binary: lib 14 (+1 ignored), main 4, acknowledgements 3,
additional 5, availability 13, basic_metadata 4, bulk_lanes 3, contracts 31,
core 16, directory_progress 3, harness_selftest 18 (+2), oracle_smoke 4,
orchestrator 15, paired_sync 9, partial_content 8, posix_metadata 3,
recovery 6, replication_regressions 9, runtime_lanes 3, scalability 5,
seam_stubs 7, startup_snapshots 8, worker_seams 3, and 15 qual binaries with
0 run and 77 ignored. The brief's "135" baseline does not match the tree:
like WP0.1–WP0.4, I count 131 listed (130 run + 1 ignored) on Windows at
`4af40db`.

**Flakes seen during the integration** (none in the final full suite):
`partial_content::bulk_install_wait_does_not_block_control_rpc` once
(fixed, CCR-WP0.INT-1) and
`orchestrator::three_orchestrators_relay_origins_and_private_branches_stay_local`
once in a targeted run (filed, CCR-WP0.INT-2).

**Qualification baseline reproduced** (`test-evidence/qualification-wave-0.json`,
committed now that CCR-WP0.5-2 un-ignores it). Command:
`powershell -NoProfile -File scripts/qualify.ps1 -Baseline -BudgetS 60 -FileTimeoutMin 60 -Evidence test-evidence/qualification-wave-0.json`,
the same settings as WP0.5's baseline, on a clean tree at `1f16b00`
(`dirty: false`), release, 4,278 s (baseline: 3,956 s), same machine
(Ryzen 9 7950X3D, rustc 1.96.0). `-Wave 0` does not exist (there are no
Wave 0 floors), so `-Baseline` with a separate evidence file is the
reproduction; `qualification-baseline.json` was not overwritten.

| | WP0.5 baseline (`9454104`) | merged tree (`1f16b00`) |
|---|---|---|
| Scenarios | 77: pass 7, fail 38, timeout 31, skipped 1 | 77: pass 7, fail 38, timeout 31, skipped 1 |
| Scenario statuses that differ | | **0** of 77 |
| Floors | 49: pass 1, fail 21, incomplete 25, missing 2 | 49: pass 1, fail 21, incomplete 25, missing 2 |
| Floor verdicts that differ | | **0** of 49 |
| Files | 15 × fail (exit 101) | 15 × fail (exit 101), same passed/failed count in every `test result:` line |

Measured values moved within run-to-run variation on a shared machine (other
agents were running suites), for example `api_p99_under_churn` 23.0 → 15.8 ms,
`scale_remote_linear_6400` 660 → 459 ms/event, `idle_exchange_50k_cpu`
5,598 → 7,988 ms (WP0.5 recorded 5.9 s and 8.2 s in earlier baseline runs),
`content_status_50k` 5,670 → 8,500 ms. The last one was checked by A/B:
alternating the release `qual_wp1_8_objects` binary of this tree with the one
WP0.5 built from the baseline sources (`C:/tkfs-target/wp0-5`, run, not
modified), `content_status_50k` p50 was 9,640 / 9,241 / 6,369 ms for the
baseline binary and 6,784 / 8,217 / 6,676 ms for this tree. It is the same
single-connection `Store::content_status` code, moved verbatim by WP0.3.

## Known limitations

- The seams are stubs: `session::Conn`/`Incoming` cannot be constructed until
  WP1.9, so `run_control`, `run_bulk_lane` and `inbound_objects::handle` are
  only signature-checked.
- `MountController` keeps today's limits (no re-attach on WinFsp, no force
  detach, no FUSE recovery with `previous`) until WP1.5.
- `EngineGaugeCell` is not fed by anything yet: WP1.4 routes the engine locks
  through `runtime::lock_engine` and reports into it.
- The open issues in the package table, and the CCRs filed above, are not
  addressed in Wave 0.
