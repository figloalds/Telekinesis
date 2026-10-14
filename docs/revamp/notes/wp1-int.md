# WP1.INT — Wave 1 integration

Branch `revamp/wp1-int`, cut from `revamp/wave-1-base` (`abc8234`).
`revamp/production-sync` was not touched and `revamp/wave-2-base` was not
created: the coordinator does both after review (§13 rule 6 and rule 8 item 7).

## Summary

1. **Merges.** All twelve package branches, each as a `--no-ff` merge commit,
   in the order the coordinator's brief gave (1.1 → 1.12; see deviation 1),
   each followed by its touched tests.

   | # | Branch (head) | Merge | Conflicts / breakages | Touched tests after the merge |
   |---|---|---|---|---|
   | 1 | `revamp/wp1-1-catalog-actor` (`37636b6`) | `a18c841` | none | `catalog_actor` 19, `contracts` 31, `seam_stubs` 8 |
   | 2 | `revamp/wp1-2-folder-controller` (`0482cf6`) | `47a18c5` | none | `folder_controller` 50, `contracts`, `seam_stubs` |
   | 3 | `revamp/wp1-3-process-launcher` (`e881d83`) | `6f782af` | none | `worker_process` 22, `orchestrator` 15, `seam_stubs` |
   | 4 | `revamp/wp1-4-worker-lifecycle` (`7b34840`) | `decaef8` | none (took WP1.4's `src/staging.rs` and `tests/worker_seams.rs`, as WP1.4 asked; CCR-WP0.INT-3 is not re-applied) | `worker_lifecycle` 25, `worker_seams` 4, `runtime_lanes` 3, `availability` 13, `partial_content` 8, `orchestrator` 15, `worker_process` 22, `contracts`, `seam_stubs` |
   | 5 | `revamp/wp1-5-mount-controller` (`635adc2`) | `4a5fe6e` | none | `mount_lifecycle` 11 (real WinFsp), `worker_lifecycle` 25, `worker_seams`, `orchestrator`, `contracts`, `seam_stubs` |
   | 6 | `revamp/wp1-6-inbox-fastpaths` (`de5fe1d`) | `226fcb7` | none | `activation_inbox` 21, `replication_regressions` 9, `acknowledgements` 3, `runtime_lanes`, `startup_snapshots` 8, `core` 16, `paired_sync` 9, `oracle_smoke` 4, `harness_selftest` 18, `orchestrator`, `contracts` |
   | 7 | `revamp/wp1-7-append-derive` (`de3dd09`) | `63846e5` + fix `801ca56` | **test breakage:** `derive_oracle::fallback_when_a_presence_witness_is_displaced` failed: with WP1.6's remote fast paths the crafted remote create commits through the creation successor and derives nothing, so the per-event derivation counters read 0. Fixed in the test (deviation 3) | lib, `derive_oracle` 11, `activation_inbox`, `replication_regressions`, `scalability` 5, `core`, `oracle_smoke`, `startup_snapshots`, `paired_sync` |
   | 8 | `revamp/wp1-8-object-indexes` (`dea7f2e`) | `c359811` (amended) | **compile error:** WP1.6 (`core/inbox.rs`) and WP1.8 (`core/objects.rs`) both added a `Store::quarantine`; the private inbox one is renamed `quarantine_event` in the merge commit | `availability_index` 21, `availability`, `partial_content`, `bulk_lanes` 3, `orchestrator`, `paired_sync`, `runtime_lanes`, `worker_lifecycle` (incl. `backfill_reports_progress`), `activation_inbox`, `harness_selftest`, `contracts` |
   | 9 | `revamp/wp1-9-session-core` (`436099d`) | `237a852` | none | lib, `session_core` 52, `contracts`, `seam_stubs` |
   | 10 | `revamp/wp1-10-session-handshake` (`5d419f8`) | `963d478` | `tests/contracts.rs` auto-merged (WP1.9 and WP1.10 edited different hunks of the same test, CCR-WP1.9-1 / CCR-WP1.10-1). **Warning:** `session::NOT_IMPLEMENTED`/`not_implemented` became dead code (clippy `-D warnings`); removed in `9b36508` | lib, `session_handshake` 21, `session_core` 52, `contracts`, `seam_stubs`, `orchestrator` |
   | 11 | `revamp/wp1-11-status-hub` (`7d3458c`) | `a818984` | none | `status_hub` 74, `contracts`, `seam_stubs` |
   | 12 | `revamp/wp1-12-legacy-stabilizers` (`676cbc7`) | `d7b10f3` + fix `9b36508` | **test breakage:** `legacy_stabilizers::crash_looping_worker_backs_off_and_list_stays_responsive` counted `STATE_NOT_INITIALIZED` matches in `worker.log`; with WP1.4 each failed start writes its `FatalLine` plus a fatal log event (4 matches), so 16 "starts" were seen for 4. Fixed in the test (deviation 3). `mount_failure_keeps_the_folder_syncing_headless` un-ignored as WP1.12 asked; it passes with real WinFsp | lib, `legacy_stabilizers` 7, `orchestrator`, `runtime_lanes`, `harness_selftest`, `contracts`, `seam_stubs` |

2. **Integration tasks (§14 WP1.INT).**
   - **Task 1, `revalidate`:** the `ops::REVALIDATE` arm in
     `src/runtime/server.rs` (`Core::revalidate`) parses `RevalidateRequest`,
     calls `Store::revalidate(ids)` under the engine lock, then activates
     what is ready in one bounded pass (≤ 32 events, ≤ 50 ms: the legacy
     lane receive budget, through `Engine::receive_peer_budget` with an empty
     bundle so the namespace notifications and session refresh run as for
     any receive), pokes `ctx.activation` (WP2.3's activator), and replies
     `RevalidateReply{requeued}`. The rows are re-queued durably before the
     activation, so an activation failure is logged (its own code,
     `site: worker.revalidate`) rather than returned. Without that pass,
     re-queued rows would only move on the next legacy receive.
   - **Task 2, `tests/w1_integration.rs`** (new, owned by WP1.INT):
     `real_launcher_registry_and_adoption_after_a_daemon_crash`,
     `session_dial_and_accept_run_a_real_conn`,
     `revalidate_requeues_quarantined_rows_and_reactivates_them`, and the
     ignored `interop_with_a_6298d0b_peer` (Tests added).
   - **Task 3:** qualification floors, interoperability and legacy
     fixtures: see VERIFY, Qualification and Interoperability.

3. **CCRs applied** (one commit per group, below). Wave 0's CCR-WP0.INT-4
   items 1–7 and the Wave 1 CCRs listed as applied. CCR-WP0.INT-3 was
   applied by WP1.4 and is not re-applied.

## CCRs

### Wave 0 (from `notes/wp0-int.md`)

| CCR | Status | Change |
|---|---|---|
| CCR-WP0.INT-3 (staging directory fsync) | applied by WP1.4 (`Staged::retain`), as WP0.INT allowed; not re-applied | — |
| CCR-WP0.INT-4 item 1 | applied (`d69a0ed`) | `model::protocol::feed::MAX_NEED_BYTES` 16 → **12 MiB**, so `need_served` + a page (≤ 1 MiB) + JSON overhead fits `frame::MAX_BODY_META` (16 MiB); a `const` assertion pins it; one 8 MiB event still fits. Nothing produces need pages yet (WP2.5) |
| CCR-WP0.INT-4 item 2 | applied | new frozen helper `model::protocol::op_allowed(op, role)`: `catalog.list` on control **or** probe sessions, `meta.exchange` on control, `objects.*` on bulk, role-less ops (`ping`, unknown) on any. `op_role`'s doc points at it |
| CCR-WP0.INT-4 item 3 | applied | `util::cancel`: cancelling a token cancels its children (running their hooks) **before** its own hooks; docs say to register per-connection killers on `child()` tokens (a parent keeps only weak references, so a dropped child frees its hooks) |
| CCR-WP0.INT-4 item 4 | applied | `util::lock::TrackedGuard` releases the mutex, then reports the hold (violation ring, debug log). `util::log` formats a line under the logger lock and writes it after releasing it, under the sink's own lock (the sink is now `Arc<Mutex<Box<dyn Write>>>`; `set_sink`'s signature is unchanged) |
| CCR-WP0.INT-4 item 5 | applied | `util::log` keeps at most `MAX_TRACKED_CODES` (1,024) rate windows. When a new code would exceed it, windows whose period ended are pruned (their pending `suppressed` summaries are written); if all are current, the code shares the `OTHER_CODES` (`"*"`) window. Each line still carries its own code. New `log::tracked_codes()` |
| CCR-WP0.INT-4 item 6 | applied | `util::backoff`: `jitter` is sanitized wherever it is used (`failure`, `bounds`): NaN or infinite → 0, else clamped to `[0, 1]` |
| CCR-WP0.INT-4 item 7 | applied | `tests/contracts.rs`'s code-literal scan also reads codes passed to `event!` as bare identifiers. It found one unregistered code: WP1.10's `WATCHDOG_SHUTDOWN_FAILED` (`ws.rs`), now in `TRANSIENT_DEFAULT_CODES` |

### Wave 1 (filed by the packages)

| CCR | Status | Change or reason |
|---|---|---|
| CCR-WP1.2-1, CCR-WP1.4-3, CCR-WP1.11-3 (`CARGO_PKG_VERSION` in the code scan) | applied (`d69a0ed`) | the scan skips `CARGO_*` names, like `TKFS_*`. The packages' `env!(stringify!(CARGO_PKG_VERSION))` adapters still work and were left alone |
| CCR-WP1.2-2 (one frozen worker version source) | resolved without a contract change | WP1.4 provides `tkfs::worker::version()` (`0.1.0+<build id>`), used by Ready, `hello` and `worker.json`; WP2.1 sets `LifecycleLimits::worker_version` from it. WP1.2's prefix comparison stays the fallback |
| CCR-WP1.2-3 (`WorkerProcess` drop never kills) | applied (`8841d4a`) | doc on the trait |
| CCR-WP1.2-4 (`Watchdog::expired_after`) | applied (`8841d4a`) | `startup::Watchdog::expired_after(now, silence)`; `expired(now)` = `expired_after(now, LEEWAY)`; the FolderController uses it instead of shifting `now` |
| CCR-WP1.3-1 (`WORKER_IDENTITY_MISMATCH`) | **declined for now** (optional) | `WORKER_NOT_RUNNING` is accurate (the recorded worker is not running), and the message says why. A dedicated code is best decided together with the WP1.3 review finding that "cannot inspect" (`Probe::Unknown`) is also reported as not running (package table); both are WP2.1's when it wires the launcher |
| CCR-WP1.3-2 | information | adopted workers' `stderr_tail` comes from `process::log_tail`; for WP2.1 |
| CCR-WP1.4-1 (engine locks in `runtime/{content,export,notify,published,control}.rs`) | applied (`77ce440`) | `lock_engine`/`try_lock_engine` everywhere (control.rs had none). A poisoned engine now follows the poison policy in `published-*` too (it answered `WORKER_BUSY` before) |
| CCR-WP1.4-2 (`lock_engine` returns `EngineGuard`) | applied (`77ce440`) | `lock_engine` = `lock_engine_tracked`; every caller only dereferences it. Filesystem callbacks and mount adapters now feed the hold gauges, `engine.stalled` and `EngineVitals` |
| CCR-WP1.4-4 (`build.rs` emits `TKFS_BUILD_ID`) | **deferred to WP4.2** (operations) | correct `rerun-if-changed` tracking of git state (and `-dirty`) is fiddly and would rebuild the crate on unrelated git operations; WP1.4's size/mtime digest already makes two builds compare unequal, which is what adoption needs |
| CCR-WP1.5-1 (`WorkerHeartbeat.mount_errors`) | applied (`e92bb2a`) | `#[serde(default)] mount_errors: u64`, filled from `mount::mount_errors()`. Four contract fixtures that embed a heartbeat were re-blessed (only the new member); `heartbeat_without_mount_errors_parses` pins backward parsing |
| CCR-WP1.5-2 (`MountControl::generation`) | applied (`e92bb2a`) | provided `fn generation(&self) -> u64 { 0 }`; `MountController` returns its status generation, which the heartbeat publishes as `generations.mount` |
| CCR-WP1.6-1 (snapshot seal cadence) | **deferred** (WP2.4, off-lock commit path) | performance only; the 10K floor holds (qualification). Changing the seal cadence also moves `try_resume_snapshot`'s replay bound, which needs its own crash tests |
| CCR-WP1.6-2 (`harness_selftest` compares parsed JSON) | **declined** | the string comparison in `fixtures_are_deterministic_per_seed` is deliberate: it detects a fallback rebuild ("would have re-serialized it in struct field order"). WP1.6's seal-after-drain adapter keeps it green |
| CCR-WP1.6-3 (worker `status.incoming_pending` = `pending_active`) | **deferred to WP2.3** (status cell) | it changes the legacy status semantics that `tests/{core,replication_regressions,orchestrator,qual_wp2_3_worker}` and the §7.3 projection read; WP2.3 owns the worker status in Wave 2 |
| CCR-WP1.6-4 (drop the bulk-lane `content_status()` override in `published.rs`) | **deferred to WP2.3** (owner of `published.rs` in Wave 2) | the `published-apply` result travels back to 6298d0b peers on the legacy wire; changing its `content` shape needs the compatibility review WP2.3 does |
| CCR-WP1.6-5 (append-mode cut counting for successors) | **open, for Wave 2** (WP2.4 with WP1.7's derive.rs) | a feature request (exact `NAMESPACE_HISTORY_LIMIT` counting on the fast paths); not reproduced, needs > 4,096 competing cuts; documented in `commit.rs` |
| CCR-WP1.7-1 (strict oracle in `verify.rs`) | applied (`98852df`) | WP1.7's original patch (`69b8b3b`'s `verify.rs`) verbatim: rebuild from no conflict rows, rebuild error before rollback error, `BEGIN IMMEDIATE` outside a transaction. `activation_inbox`, `derive_oracle`, `oracle_smoke`, `core`, `replication_regressions`, `startup_snapshots`, `harness_selftest`, `scalability` pass with it. WP1.7's `verify_derived_state_strict` adapter stays (test-only; deviation 5) |
| CCR-WP1.8-1 (`OpenOptions` doc) | applied (`8841d4a`) | |
| CCR-WP1.8-2 (re-export `PartOutcome`, `ScrubOutcome`) | applied (`8841d4a`) | |
| CCR-WP1.8-3 (`fetch_verified` quarantines through the store) | **deferred to WP3.2** (objects v3, worker side) | `Store::quarantine` records a per-object backoff; on the on-demand read path that could hold a reader's download behind the 30 s object backoff. The adapter (`request_object` right after the rename) already corrects the index row to missing |
| CCR-WP1.9-1 (`tests/contracts.rs` frame assertion) | ratified | merged as WP1.9 wrote it |
| CCR-WP1.9-2 (`Bytes` in `Incoming::chunks`) | **deferred to WP3.2/WP3.3** | a frozen signature break; performance only |
| CCR-WP1.9-3 (floor `control_queued_30mib_p99_ms < 50`) | applied (`8841d4a`) | floor `session_control_under_queued_data` (wave 1) |
| CCR-WP1.10-1 (`tests/contracts.rs` edit + rename) | applied | both halves merged; `session_stubs_fail_closed_with_not_implemented` renamed `session_entry_points_fail_closed` (`d69a0ed`) |
| CCR-WP1.10-2 (`PAIRING_CODE_CHANGED` in `GOAWAY_HANDSHAKE_CODES`) | applied (`d69a0ed`) | WP1.10's explicit match stays |
| CCR-WP1.10-3 (`session/mod.rs` doc, re-exports) | applied (`9b36508`) | doc updated; `client_hello`, `dial_expecting`, `connect_v3`, `accept_v3`, `client_handshake`, `server_handshake` re-exported; the dead `NOT_IMPLEMENTED` helper removed |
| CCR-WP1.11-1, CCR-WP1.11-2 (`PeerConfigured.display_name`, `LaneState.protocol`) | **deferred to WP2.INT** | the fields need the hub to consume them (label, `PeerStatus.protocol`), in WP1.11's hub, which the review has not approved (open issues); the producers arrive in Wave 2 (WP2.1, WP2.6). Adapters: `StatusHub::set_peer_label`; `protocol` stays `None` until WP3.1 |
| CCR-WP1.11-4 (`SNAPSHOT_MIN_INTERVAL` doc) | applied (`8841d4a`) | |
| CCR-WP1.12-1 (`RpcLane` deadline) | **declined** (optional) | `peering::DeadlineLane` works and the path is deleted in WP2.1/WP3.1. The review's extension (a deadline-aware `ws::connect`) is listed in open issues |
| CCR-WP1.12-2 (`api.rs` poison-tolerant locks) | applied (`8841d4a`) | `util::lock::lock(shared)` in the preamble, `Folder`, `Pair` and `Receive` |

### Filed by WP1.INT

- **CCR-WP1.INT-1 (for WP2.1; information, no contract change).** On
  Windows, `std::process::Command::spawn` uses `bInheritHandles = TRUE`, so
  a worker that `RealLauncher` spawns inherits every inheritable handle of
  the daemon, including standard handles the daemon itself inherited (for
  example a pipe from a shell or CI runner). A consumer reading that pipe
  to its end then waits for every worker (found by
  `w1_integration::real_launcher_registry_and_adoption_after_a_daemon_crash`,
  whose first version read the child daemon's stdout to EOF and waited for
  the worker's 600 s idle exit). Requested: WP2.1 starts the daemon with
  non-inheritable standard handles (or WP1.3's launcher passes an explicit
  `PROC_THREAD_ATTRIBUTE_HANDLE_LIST`). Adapter: the test reads lines until
  the child exits instead of to EOF. Linux is unaffected (`O_CLOEXEC`).

## Deviations from the spec, and why

1. **Merge order.** §14 WP1.INT lists 1.11, 1.1, 1.3, 1.2, 1.9, 1.10, 1.6,
   1.7, 1.8, 1.4, 1.5, 1.12; the coordinator's brief asked for 1.1 … 1.12.
   I followed the brief. The packages own disjoint files except
   `tests/contracts.rs` (1.9, 1.10) and `src/core.rs` `Store` methods (1.6,
   1.8), so the order changed only where the breakages showed up, not what
   had to be fixed.
2. **`revalidate` also activates** (task 1 says "wire … to
   `Store::revalidate`"): `Store::revalidate` only re-queues, and in Wave 1
   nothing activates staged rows except a legacy receive, so a re-queued row
   would sit unclassified until the next page arrived. The bounded pass
   uses the lane budget and the normal receive path.
3. **Existing test files changed** (each a merge consequence, no assertion
   weakened):
   - `tests/derive_oracle.rs` (`fallback_when_a_presence_witness_is_displaced`,
     WP1.7): commits counted as fast by WP1.6's `commit_path_counters` must
     run no derivation and are skipped by the per-mode counter assertion;
     the event under test (`w3`) must still take the slow path and fall
     back, as before.
   - `tests/legacy_stabilizers.rs` (WP1.12): the crash-loop test counts
     lines (skipping the `"level":` log event), not matches; the
     mount-failure test is un-ignored.
   - `tests/qual_wp1_12_legacy.rs` (WP1.12, `crash_backoff`): the same
     counting fix (`956c842`); found by the first wave 1 qualification run,
     which measured 20 "restarts" for 5.
   - `tests/worker_lifecycle.rs` (WP1.4,
     `ready_worker_record_and_hello_describe_the_same_process`): pinned the
     `revalidate` stub (`UNSUPPORTED_OPERATION`); it now expects
     `{"requeued":0}` (task 1 replaces the stub).
   - `tests/contracts.rs` (WP0.4): the CCR-WP0.INT-4 tests, the scan
     extension, the rename and `heartbeat_without_mount_errors_parses`;
     four fixtures under `tests/fixtures/contracts` re-blessed for
     CCR-WP1.5-1 (the only diff is `"mount_errors": 0`).
4. **`worker::heartbeat::beat` reads the mount status inside the snapshot
   update** (a WP1.4 review minor), because the CCR-WP1.5-2 change touched
   the same lines: concurrent beats now publish `seq` in the order they read
   the mount.
5. **`verify_derived_state_strict` kept.** CCR-WP1.7-1 says it can delegate
   or go; its tests assert its exact `payload differs` text, and it is
   test-only. With the strict plain oracle it now does the from-empty
   rebuild twice (`derive_oracle` 56 s → 102 s in debug).
6. **The interop test is `#[ignore]`d** and needs `TKFS_BASELINE_BIN`; the
   default suite has no 6298d0b binary. It was run on Windows (below).

## Tests added

`tests/w1_integration.rs` (new; 3 run, 1 ignored):

| Test | What it checks |
|---|---|
| `real_launcher_registry_and_adoption_after_a_daemon_crash` | A child process (the first "daemon") opens a real `Catalog` (WP1.1), creates a folder, and a `FolderRegistry` (WP1.2) with `RealLauncher` (WP1.3) runs the real `daemon --supervised` (WP1.4) to `Running`; the `StatusHub` (WP1.11) shows it `Running` with that pid; `worker.json` names the same pid/instance and `supervised`; `hello` answers. The child exits with no Shutdown (a crash). The worker survives; the test process opens the catalog again and a new registry **adopts** it (`Running{adopted:true}`, same pid and instance, hub `adopted`), `hello` and `FolderCmd::Revalidate` work through the adopted client, and Shutdown ends with `stopped` and the process gone |
| `session_dial_and_accept_run_a_real_conn` | `session::dial` ↔ `session::accept` (WP1.10) hand the authenticated socket to `Conn` (WP1.9) on both sides: peer info, generation, admission; paired requests; a 3 MiB body chunked and reassembled (and echoed reversed); a Remote error that keeps the connection; 64 concurrent calls correctly paired; a wrong code is `Unauthorized(INVALID_PAIRING_CODE)` and never reaches a `Conn` |
| `revalidate_requeues_quarantined_rows_and_reactivates_them` | On the 6298d0b worker fixture (real worker process): unknown ids re-queue 0; `revalidate` re-queues the 2 quarantined/blocked rows and activates them in the same request (still invalid → quarantined again, `incoming_quarantined` 2, not 0) |
| `interop_with_a_6298d0b_peer` (ignored) | With `TKFS_BASELINE_BIN`: this build pairs with a 6298d0b daemon and receives its folder; files flow both ways; then the 6298d0b daemon pairs with this build and receives a folder; files flow both ways; the old worker has no `hello` (it really is the old build) |

`tests/contracts.rs` (8 new): `event_identifier_codes_are_scanned`,
`need_budget_fits_the_meta_exchange_body`, `op_allowed_follows_the_op_table`,
`cancel_runs_children_before_the_parents_hooks`,
`tracked_reports_after_release_and_log_writes_after_unlock` (fails with the
old `TrackedGuard` drop order: checked), `log_rate_windows_are_bounded`
(child process; 1,524 distinct codes → 1,044 lines, 1,025 windows, one
`"*"` summary of 480), `backoff_sanitizes_a_jitter_set_through_the_field`,
`heartbeat_without_mount_errors_parses`.

## VERIFY

All on the merged tree after the last product-code change (`8841d4a`).
Later commits change only these notes, the evidence file and the restart
counting of the ignored `qual_wp1_12_legacy::crash_backoff` scenario
(`956c842`), after which fmt, build, clippy and the WSL check were run
again (same results). Windows
`CARGO_TARGET_DIR=C:/tkfs-target/wp1-int`, WSL
`CARGO_TARGET_DIR=$HOME/tkfs-target/wp1-int`. No other agent's cargo or
tkfs process was running on the machine when I checked before the runs.

| Check | Result |
|---|---|
| `cargo fmt --check` | pass (exit 0) |
| `cargo build --all-targets` | pass, no warnings |
| `cargo clippy --all-targets -- -D warnings` | pass (exit 0) |
| `CARGO_TARGET_DIR=C:/tkfs-target/wp1-int powershell -NoProfile -File C:/tkfs-build/suite-lock.ps1 cargo test --all-targets` | **pass, SUITE_EXIT=0: 583 passed, 0 failed, 84 ignored** across 52 test binaries; no failing test, no retry |
| WSL `cargo check --all-targets` | pass, no warnings |
| WSL `cargo clippy --all-targets -- -D warnings` (extra: `cfg(unix)` code, `fuse_linux.rs`) | pass |
| WSL `cargo test --all-targets --no-fail-fast` (the whole suite; it contains §13 rule 8's subset `orchestrator`, `session_core`, `session_handshake`, `worker_lifecycle`, `mount_lifecycle`) | **pass, exit 0: 586 passed, 0 failed, 85 ignored** across 52 binaries. `/dev/fuse` and `fusermount3` present, so every FUSE scenario ran (`mount_lifecycle` 17) |
| Legacy fixtures (§13 rule 8.3) | `harness_selftest::legacy_worker_fixture_opens_with_current_build`, `legacy_datadir_fixture_opens_with_current_build` and `compat_goldens_recapture_identically` pass on Windows and WSL; `catalog_actor::legacy_datadir_opens_equal_to_the_legacy_supervisor_view` passes; `w1_integration::revalidate_…` runs a real worker on the legacy worker fixture |
| Interoperability (§13 rule 8.4) | pass, see below |

**Per binary (Windows / WSL):** lib 52+2i / 56+2i, bin 4 / 0,
acknowledgements 3, activation_inbox 21, additional 5, availability 13 / 10,
availability_index 21, basic_metadata 4, bulk_lanes 3, catalog_actor 19,
contracts 39, control_transactions 4, core 16, derive_oracle 11+2i,
directory_progress 3, folder_controller 50, harness_selftest 18+2i,
legacy_stabilizers 7, mount_lifecycle 11 / 17, oracle_smoke 4,
orchestrator 15, paired_sync 9, partial_content 8, posix_metadata 3,
recovery 6, replication_regressions 9, runtime_lanes 3, scalability 5,
seam_stubs 8, session_core 52, session_handshake 21, startup_snapshots 8,
status_hub 74, w1_integration 3+1i, worker_lifecycle 25, worker_process 22,
worker_seams 4, and 15 qual binaries with 77 ignored scenarios on Windows
(78 in WSL: `qual_wp1_5_mount::stale_owned_mount_recovered_after_crash` is
Linux-only).

**Count reconciliation.** WP0.INT's final tree ran 199 passed and 80
ignored in 39 binaries. Wave 1 added 13 binaries and 384 passing tests:
catalog_actor 19, folder_controller 50, worker_process 22,
worker_lifecycle 25, worker_seams +1, mount_lifecycle 11 (Windows),
activation_inbox 21, derive_oracle 11, availability_index 21, session_core
52, session_handshake 21, status_hub 74, legacy_stabilizers 7 (6 + the
un-ignored mount-failure test), lib unit tests +38 (14 → 52), and WP1.INT's
contracts +8 (31 → 39) and w1_integration 3. 199 + 384 = 583. Ignored:
80 + derive_oracle 2 + lib 1 (WP1.7's memory probe) + w1_integration 1
(interop) = 84.

**Stability of the new tests.** `w1_integration` ran 5 times on Windows
(3 of them with the interop test), all green.

## Qualification (`test-evidence/qualification-wave-1.json`)

**Runs.** `CARGO_TARGET_DIR=C:/tkfs-target/wp1-int powershell -NoProfile -File
scripts/qualify.ps1 -Wave 1 -BudgetS 2400 -FileTimeoutMin 150` (release, all
seven wave 1 qual files, 2,121 s) on the clean tree `65543f0` (`dirty:
false`). Its `qual_wp1_12_legacy::crash_backoff` failed on an integration
breakage in the scenario (it counted `STATE_NOT_INITIALIZED` matches, 4 per
failed start since WP1.4; fixed in `956c842` like the `legacy_stabilizers`
test), so that file was re-run with `-Files qual_wp1_12_legacy -Merge` on
`fb7275f` (`dirty: false`); the evidence keeps the other six files of the
first run (`merged_from`). The budget is 2,400 s per scenario because the
6.4K backlog alone takes about 24 minutes (WP1.7 used 1,800 s). Hardware:
AMD Ryzen 9 7950X3D (16 cores, 32 threads), 127.7 GB, Windows 11 Pro
10.0.26300, rustc 1.96.0; I ran nothing else on the machine during either
run.

**Result: 29 scenarios, 28 pass, 1 fail; 16 floors evaluated, 15 pass, 1
fail (`concurrent_namespace_ops_5k`); `ok: false`.**

| Floor (wave 1) | Floor | Measured | Verdict | Wave 0 (`qualification-wave-0.json`, 60 s budget) |
|---|---|---|---|---|
| `remote_linear_create_2k` | ≤ 5 ms/event | 0.68 | pass | 175 (incomplete) |
| `remote_linear_create_10k` | ≤ 5 ms/event | 1.94 (10,000 fast, 0 slow) | pass | 166 (incomplete) |
| `local_create_after_resolved_conflict` | p90 ≤ 10 ms | 1.68 | pass | 27,841 (incomplete) |
| `two_way_backlog_2k` | ≤ 100 ms/event | 60.9 (2,000 append, 0 fallbacks; oracle empty) | pass | 197 (incomplete) |
| `two_way_backlog_6400` | ≤ 300 ms/event | 221.4 (6,400 append, 0 fallbacks) | pass | 187 (incomplete) |
| `concurrent_namespace_ops_5k` | max ≤ 500 ms | **721.2** (p50 311, p90 347; 50 append, 0 fallbacks; derivation 17.9 ms/commit) | **fail** | 200,294 (incomplete) |
| `restart_ready_20k_objects` | ≤ 3,000 ms | 1,004 (first open after build 3,534) | pass | 3,855 (fail) |
| `content_status_50k` | < 50 ms | 0.07 warm (cold 511) | pass | 8,499 (fail) |
| `tiny_pending_objects_12k` | completes | yes (302 objects/s) | pass | no (fail) |
| `watch_latency` | ≤ 1,500 ms | 1,000.3 | pass | missing |
| `session_slow_does_not_block_fast` | p99 < 50 ms | 0.18 | pass | 2,900 (fail) |
| `session_control_under_queued_data` (CCR-WP1.9-3, new) | p99 < 50 ms | 40.3 | pass | — |
| `session_loopback_throughput` | ≥ 300 MiB/s | 651 (WSS 508, upload 380) | pass | 26 (fail) |
| `legacy_hung_repo_healthy_progress` | ≤ 45,000 ms | 3,096 | pass | incomplete |
| `legacy_interval_20s_reconnects` | 0 | 0 | pass | 9 (fail) |
| `legacy_crash_backoff` | ≤ 5 restarts in 20 s | 4 (re-run); 20 in the first run = 5 restarts × 4 matches | pass | 11 (fail) |

Scenarios without a floor all pass: `qual_wp1_4_worker` 5/5 (accept loop,
mount failure, poison exit 70, RPC before mount), `qual_wp1_5_mount` 3/3 on
Windows (attach/detach/attach on one pid, force detach persists dirty data,
foreign mount untouched), `qual_wp1_6_activation::{quarantine_not_pending,
transient_retry}`, `qual_wp1_8_objects::corrupt_mid_page_keeps_completed`,
`qual_wp1_9_session::{dead_peer_detection (20.0 s), duplex_32mib, idle_60s}`,
`qual_wp1_12_legacy::worker_busy_not_counted` (138 busy observed, 0
counted). Extra, WSL release: `qual_wp1_5_mount` **4/4** including the
Linux-only `stale_owned_mount_recovered_after_crash` (WP1.5's hand-off),
and `qual_wp1_4_worker` 5/5.

**The failing floor.** `concurrent_namespace_ops_5k` is a per-operation
maximum. The merged tree is faster than WP1.7 measured alone (p50 311 ms
against 374 ms: WP1.6's inbox no longer parses the journal per event), but
1–3 of the 50 operations take 450–850 ms. A probe (the scenario's workload
three times, per-op samples, not committed) showed the slow operations at
random positions (not a warm-up): `[…, 451, …, 756, …, 564, …]`,
`[…, 734, …, 685, …, 623, …]`, `[…, 456, …, 843, …, 587, …]`. With SQLite's
WAL autocheckpoint disabled the median fell to ~250 ms but spikes of up to
1.1 s remained, so they are not checkpoints alone: each slow-path commit
rewrites `current_state` and, by design, force-seals the startup snapshot
(both O(5K entries), `synchronous=FULL`), and the outliers track I/O
latency. WP1.7's own review reproduced 546–579 ms before the merge. No
derive.rs change helps (derivation is 18 ms of the 311); this stays an open
acceptance item for the slow-path commit cost (CCR-WP1.6-1 seal cadence,
WP2.4's off-lock prepare, and the O(N) `current_state` rewrite of slow
commits).

## Interoperability with 6298d0b (§13 rule 8.4)

The baseline binary was built from `git archive 6298d0b` (no worktree was
added to the repository) in `C:/tkfs-build/wp1-int-baseline-src` with
`CARGO_TARGET_DIR=C:/tkfs-target/wp1-int-baseline` (debug, 1 min).

`TKFS_BASELINE_BIN=C:/tkfs-target/wp1-int-baseline/debug/tkfs.exe cargo
test --test w1_integration -- --include-ignored` (Windows, debug): **pass, 4
of 4 runs**, about 5.5 s each. The scenario
(`w1_integration::interop_with_a_6298d0b_peer`):

1. this build pairs with the 6298d0b daemon (3 connections) and receives a
   folder it created; a file written on the old side arrives on the new
   side, and a file written on the new side arrives on the old side;
2. the 6298d0b daemon pairs with this build and receives a folder this build
   created; files flow both ways again;
3. both daemons list each other as peers; a final round old → new.

The old folder's worker answers `hello` with an error, which proves the peer
really runs the old build. No wire-format change was needed: the legacy
lanes, `published-*`, `bulk_sync` protocol 1 and the pairing exchange are
unchanged on the wire (WP1.8 kept bulk protocol 1; WP1.12's additions are
local; v3 sessions are not used on any network path until WP3.1).
Interop was not run in WSL (it needs a Linux baseline build) or with
mounted folders.

## Package status

| Package | Review | Merged | Status in the merged tree | Open issues (owner) |
|---|---|---|---|---|
| WP1.1 catalog actor | approved | `a18c841` | green: `catalog_actor` 19; used by `w1_integration` (a real catalog across a daemon "crash") | minors: `snapshot_p99_under_1ms_while_a_command_is_held` does not wait for the writer to hold its transaction before sampling (test); `CreateFolder` silently drops a gate on a mount-less record and accepts an installation-only gate (empty `initial_sync.endpoint`); `OverrideGate`/`RetargetGate` on a gateless folder are silent no-ops (WP2.1 must check `snapshot.gates`); a dead catalog thread is not restarted (WP2.1) |
| WP1.2 folder controller | approved | `47a18c5` | green: `folder_controller` 50; with the real launcher, worker and catalog in `w1_integration` (Running, adoption, Revalidate, Shutdown) | minor: a controller can lose its `maximum_running` wake-up while `reconcile` spawns controllers before inserting their handles (fix in `registry.rs`, or a self-healing timer; WP2.1); a folder removed and re-added under the same state while its retired controller still stops the worker gets two controllers; `Failed{WORKER_STOP_TIMEOUT}` with an unkillable lock holder cycles to `CRASH_LOOP` |
| WP1.3 process launcher | approved | `6f782af` | green: `worker_process` 22; the real `daemon --supervised` launch, Ready validation and adoption of a real worker are now checked (`w1_integration`), which closes WP1.3's open item | minors: the adopted-worker probe and `kill_verified` treat "cannot inspect" (`Probe::Unknown`, e.g. EMFILE on `/proc`) as exited / `WORKER_NOT_RUNNING`; `JsonWorkerClient::call` panics on `Duration::MAX` (`Instant + deadline` overflow); a copied stdout line is two writes to `worker.log` (interleaving). CCR-WP1.INT-1 (Windows handle inheritance) |
| WP1.4 worker lifecycle | approved | `decaef8` | green: `worker_lifecycle` 25, `worker_seams` 4; `revalidate` wired (task 1); CCR-WP1.4-1/-2 applied; the concurrent-beat ordering finding fixed with CCR-WP1.5-2 | minors: the supervised idle exit measures with the wall clock (`now_ms`), which a clock step can trip (a WSL flake of `unsupervised_worker_exits_when_idle_and_contact_keeps_it_alive` was reported; it did not recur here); a retried Linux signal refused with `BUSY_VIEW` is dropped without a log line; the accept-error exit retains only best-effort; `qual_wp1_4_worker` stays `#[ignore]` by the qualify convention |
| WP1.5 mount controller | approved | `4a5fe6e` | green: `mount_lifecycle` 11 (WinFsp) / 17 (FUSE); qual run below; CCR-WP1.5-1/-2 applied | minors: the adoption branch of `Inner::attach` does not call `watch_lost()` (legacy `mount::start` path only); a FUSE checkout whose unmount is refused while the dispatch thread died leaves `Attached`; WinFsp `TK_HANDLE` contexts leak at detach (`native/mount.c`); a lazy external unmount with open files reads `Attached` until the next attach/detach |
| WP1.6 inbox and fast paths | approved | `226fcb7` | green: `activation_inbox` 21 (also under the strict oracle of CCR-WP1.7-1); remote fast paths on by default | CCR-WP1.6-5 (successors do not count `NAMESPACE_HISTORY_LIMIT` cuts; invariant 11's "refused before ACK" not guaranteed for > 4,096 competing cuts); the `TKFS_REMOTE_FASTPATH=0` doc says "exactly" but gates on the event's branch; `core.commit.busy` ignores the fault action; CCR-WP1.6-1/-3/-4 deferred (above) |
| WP1.7 append-mode derivation | approved | `63846e5` (+ `801ca56`) | green: `derive_oracle` 11; CCR-WP1.7-1 applied, which also resolves the review finding that the adapter's plain pass used a deferred savepoint; 2K and 6.4K backlog floors pass | **`concurrent_namespace_ops_5k` floor fails** (max 721 ms, p50 311; Qualification), owner: the slow-path commit cost (WP2.4, CCR-WP1.6-1); minors: verify mode returns full's refusal without comparing it with append's (the fuzzer's commit checks do compare); `append_guard` scans the whole branch and two of its guards depend on peer-controlled data (a `,` in an entity id, > 16 multi-relocation events), after which every slow commit of that branch runs full mode (a CCR on `commit.rs` validation to reject `,` is suggested; Wave 2) |
| WP1.8 object indexes | approved | `c359811` | green: `availability_index` 21, `availability`, `partial_content`, `bulk_lanes`; CCR-WP1.8-1/-2 applied | minors: on Linux `scan_objects` fails the whole call when an entry vanishes between readdir and stat (ENOENT; skip it); the cached `objects/` listing has no maximum age (filesystems without directory mtime updates); `put_object`/`put_stream` add a separate durable commit per new local object (13–20 % slower local writes in `scalability`); a cold `content_status` costs ~0.7 s at 50K objects during active sync (WP2.3 status cell); CCR-WP1.8-3 deferred |
| WP1.9 session core | **not approved** | `237a852` | green: `session_core` 52; a real `Conn` runs over the real handshake in `w1_integration`. Nothing blocked the merge, and no wire path uses v3 sessions until WP3.1 | **blockers (open):** (1) a zero-length streamed request part put in an upload's overflow is never delivered (`deliver_uploads`/`next_wake` key on `overflow_bytes`), so an upload ending with empty parts stalls to its idle timeout (0-byte files over `objects.push`); (2) receive-side flow control charges only body bytes, so empty or tiny parts with large headers grow memory without bound (invariant 22; the review measured +0.8–1.9 GiB). Minors: `control_call_is_fast_while_data_is_queued` checks every sample < 50 ms in release (1 in 12 runs fails; use p99); a residual cross-side idle case and up to 4 MiB uncounted per running stream (documented). **Needs a WP1.9 fix round before WP2.6/WP2.8 rely on the bounds** |
| WP1.10 session handshake | approved | `963d478` | green: `session_handshake` 21; CCR-WP1.10-1/-2/-3 applied; its "accept either outcome" tests now reach a real `Conn` | minors: the distinct-nonce check compares hex strings (an upper-case echo passes; no exploit: proofs use different labels); peer-chosen text (serde error detail, GOAWAY message) reaches error messages unescaped; `ws::connect_any` keeps only the last address's error (an HTTP 401/403 can be reported as `WS_CONNECT_FAILED`). Optional: switch the private pre-auth frame codec to `session::frame` |
| WP1.11 status hub | **not approved** | `a818984` | green: `status_hub` 74; used with a real registry in `w1_integration`. Not wired until WP2.1, so nothing blocked the merge | **major (open):** object transfer rates and ETA include metadata exchange bytes (the per-pair EWMAs add every op's bytes), so mixed traffic misreports object rate/ETA (the §14 "within 5 %" item) and an idle pair shows non-zero object rates. Minors: exchange activity entries coalesce only with the newest entry, so two concurrent transfers fill the 500-entry ring; inbound-incompatible notices whose code changes still fill the daemon history; CCR-WP1.11-1/-2 deferred to WP2.INT. **Needs a WP1.11 fix round before WP2.1 wires it** |
| WP1.12 legacy stabilizers | approved | `d7b10f3` (+ `9b36508`, `956c842`) | green: `legacy_stabilizers` 7 (the mount-failure test un-ignored and passing with real WinFsp); all four release scenarios pass their floors (crash backoff after the counting fix); CCR-WP1.12-2 applied | minors: `DeadlineLane`'s 3 s does not bound the WebSocket upgrade (a frozen worker costs ~15 s per `list` call; widen CCR-WP1.12-1 to a deadline-aware `ws::connect`); `LOCAL_WORKER_UNAVAILABLE` anywhere in a chain is Contention, which hides wrapped `WORKER_STATE_MISMATCH` and `runtime.json` errors (only `WORKER_NOT_RUNNING` should be); `WORKER_LIMIT` escalates the crash backoff (up to 60 s to start once a slot frees); `lane_active` retires lanes on a `peers()` error while `run.rs` keeps the list (lane churn); discovery runs under the supervisor mutex for backed-off repositories; the notes' VERIFY table names the wrong ignored `harness_selftest` tests |

## Known limitations

- WP1.9's two blockers and WP1.11's major finding are open (package
  table). They do not affect the release point "after WP1.INT" (§14),
  because neither v3 sessions nor the hub run in the daemon yet, but Wave 2
  packages that build on them (WP2.1, WP2.6, WP2.8) should wait for their
  fix rounds.
- The new components (catalog, registry, launcher, hub, sessions) are still
  standalone: the daemon runs the legacy `Supervisor` and lanes until
  WP2.1. `w1_integration` is the only place they run together.
- Interoperability was checked on Windows only, with headless folders and
  small files, in debug builds, one 6298d0b daemon against one Wave 1
  daemon.
- Deferred CCRs (above) stay with their named owners: CCR-WP1.4-4 (WP4.2),
  CCR-WP1.6-1 (WP2.4), CCR-WP1.6-3/-4 (WP2.3), CCR-WP1.6-5 (Wave 2),
  CCR-WP1.8-3 (WP3.2), CCR-WP1.9-2 (WP3.2/WP3.3), CCR-WP1.11-1/-2
  (WP2.INT); declined: CCR-WP1.3-1, CCR-WP1.6-2, CCR-WP1.12-1.
- `verify_derived_state_strict` now runs the from-empty rebuild twice
  (deviation 5); WP2.x can drop it once its tests call the plain oracle.
- Build artefacts outside the repository: the 6298d0b source export
  `C:/tkfs-build/wp1-int-baseline-src` and its target
  `C:/tkfs-target/wp1-int-baseline` (no git worktree was added).
